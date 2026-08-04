"""macro_ingestion worker — FRED regional macro series → macro_data + snapshot.

Standalone reimplementation of the monolith ``macro_ingestion`` worker
(reference: ``app/jobs/workers/macro_ingestion.py`` + ``quant_engine/
regional_macro_service.py`` + ``macro_snapshot_builder.py``). Fetches ~92 FRED
series across 4 regions (US, EUROPE, ASIA, EM) plus global and credit series,
upserts the raw observations into ``macro_data`` (with the two derived series
``YIELD_CURVE_10Y2Y`` and ``CPI_YOY``), and writes a version-1 regional macro
snapshot into ``macro_regional_snapshots``.

Faithful to the monolith:
  * series registry copied verbatim (REGION_SERIES / GLOBAL_SERIES / CREDIT_SERIES);
  * percentile-rank scoring (neutral 50.0 below 60 obs), staleness-decay weights,
    dimension weights, min-coverage 50%;
  * BIS credit-cycle 7th dimension + IMF WEO growth/inflation/fiscal blends,
    both degrading gracefully when ``bis_statistics``/``imf_weo_forecasts`` are
    absent or empty;
  * 10y lookback, no per-series watermark — the window is re-fetched and the
    upsert (ON CONFLICT (series_id, obs_date) DO UPDATE) keeps re-runs cheap;
  * batch deduped by PK in Python before INSERT, chunked 2000.

Differences (by design, README §Princípios): sync psycopg3 + httpx, in-process
token bucket (FRED 120 req/min) instead of Redis gates, advisory lock 900_320.

Beyond FRED it also ingests the BIS offshore-dollar stock (locational banking
statistics, SDMX over stats.bis.org) under ``source='bis'`` — full history each
run, fail-soft, reported as ``bis_rows``/``bis_error``. See the BIS section.

Contract:  run(dsn, *, calc_date=None, limit=None) -> {"fetched", "upserted",
"bis_rows", "bis_error", ...}
``limit`` caps the number of series fetched (smoke runs); ``calc_date`` is the
snapshot as-of date (defaults to today).

Env: FRED_API_KEY (required).
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import os
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from src.db import LOCK_MACRO_INGESTION, advisory_lock, connect

FRED_BASE_URL = "https://api.stlouisfed.org/fred"
LOOKBACK_YEARS = 10
UPSERT_CHUNK = 2000
_MISSING_VALUES = frozenset((".", "#N/A", "", "NaN", "nan", "null", "None"))

# Limit per frequency for 10yr lookback (observations requested from FRED).
# Sized so a full LOOKBACK_YEARS window round-trips even at maximum publication
# density (7-day daily series like DFF ≈ 3660 rows/10y; weekly ≈ 522). The old
# values (daily 2520 / weekly 520) silently truncated dense series, and with
# sort_order=asc FRED kept the OLDEST rows — freezing the newest observations
# out of macro_data entirely (T10YIE stuck at 2026-02-27, DFF at 2023-05-28).
FREQUENCY_LIMITS: dict[str, int] = {
    "daily": 4000,
    "weekly": 600,
    "monthly": 150,
    "quarterly": 50,
}

# Escape hatch from the LOOKBACK_YEARS window for specs marked ``full_history``:
# the whole series, every run. FRED's earliest accepted observation_start is
# 1776-07-04 and its documented maximum ``limit`` is 100000; no FRED series comes
# close, so this is "everything" without being a magic sentinel.
FULL_HISTORY_START = "1776-07-04"
FULL_HISTORY_LIMIT = 100_000

MIN_HISTORY_OBS = 60
IMF_MAX_STALE_DAYS = 366
IMF_FORECAST_WEIGHTS: dict[int, float] = {1: 0.40, 2: 0.20, 3: 0.10}

_DEFAULT_CONFIG: dict[str, Any] = {
    "lookback_years": LOOKBACK_YEARS,
    "dimension_weights": {
        "growth": 0.20,
        "inflation": 0.20,
        "monetary": 0.15,
        "financial_conditions": 0.20,
        "labor": 0.15,
        "sentiment": 0.10,
    },
    "min_coverage": 0.50,
    "staleness": {
        "daily": {"fresh_days": 3, "max_useful_days": 10, "floor": 0.30},
        "weekly": {"fresh_days": 10, "max_useful_days": 30, "floor": 0.40},
        "monthly": {"fresh_days": 45, "max_useful_days": 90, "floor": 0.50},
        "quarterly": {"fresh_days": 100, "max_useful_days": 180, "floor": 0.50},
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# FRED series registry (verbatim from the monolith regional_macro_service)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class SeriesSpec:
    series_id: str  # FRED series id — what we ASK FOR
    dimension: str
    label: str
    frequency: str  # daily | weekly | monthly | quarterly
    invert: bool = False  # True = higher raw value means worse conditions
    units: str = "lin"  # FRED transform: lin, pch, pc1, ...
    # macro_data.series_id — what we STORE AS. Defaults to series_id. Set it only
    # to ingest the same FRED series twice under different transforms: the FRED id
    # is not unique across the registry once `units` varies, but macro_data's PK is
    # (series_id, obs_date), so two transforms of one FRED series would otherwise
    # overwrite each other row-for-row. See INDPRO / INDPRO_IDX below.
    storage_id: str | None = None
    # Ignore the LOOKBACK_YEARS window and fetch the entire series every run. Only
    # for cheap, low-frequency series a downstream consumer needs full history for.
    full_history: bool = False

    @property
    def key(self) -> str:
        """The macro_data.series_id this spec writes to."""
        return self.storage_id or self.series_id


REGION_SERIES: dict[str, list[SeriesSpec]] = {
    "US": [
        SeriesSpec("A191RL1Q225SBEA", "growth", "Real GDP Growth", "quarterly"),
        SeriesSpec("INDPRO", "growth", "Industrial Production", "monthly", units="pc1"),
        SeriesSpec("PAYEMS", "growth", "Nonfarm Payrolls", "monthly"),
        SeriesSpec("CPIAUCSL", "inflation", "CPI All Urban", "monthly", invert=True),
        SeriesSpec("PCEPILFE", "inflation", "Core PCE", "monthly", invert=True, units="pc1"),
        SeriesSpec("DFF", "monetary", "Fed Funds Rate", "daily", invert=True),
        SeriesSpec("DGS10", "monetary", "10Y Treasury", "daily"),
        SeriesSpec("DGS2", "monetary", "2Y Treasury", "daily"),
        SeriesSpec("NFCI", "financial_conditions", "Chicago Fed Financial Conditions", "weekly", invert=True),
        SeriesSpec("VIXCLS", "financial_conditions", "VIX", "daily", invert=True),
        SeriesSpec("UNRATE", "labor", "Unemployment Rate", "monthly", invert=True),
        SeriesSpec("JTSJOL", "labor", "JOLTS Openings", "monthly"),
        SeriesSpec("SAHMREALTIME", "labor", "Sahm Rule", "monthly", invert=True),
        SeriesSpec("CFNAI", "growth", "Chicago Fed National Activity Index", "monthly"),
        SeriesSpec("UMCSENT", "sentiment", "Michigan Consumer Sentiment", "monthly"),
    ],
    "EUROPE": [
        SeriesSpec("CLVMNACSCAB1GQEA19", "growth", "Euro Area Real GDP", "quarterly", units="pc1"),
        SeriesSpec("CP0000EZ19M086NEST", "inflation", "Eurostat HICP EA19", "monthly", invert=True, units="pc1"),
        SeriesSpec("ECBDFR", "monetary", "ECB Deposit Facility Rate", "daily", invert=True),
        SeriesSpec("IRLTLT01DEM156N", "monetary", "German 10Y Bund", "monthly"),
        SeriesSpec("BAMLHE00EHYIEY", "financial_conditions", "Euro HY Effective Yield", "daily", invert=True),
        SeriesSpec("CSCICP02EZM460S", "sentiment", "Consumer Confidence EA19", "monthly"),
    ],
    "ASIA": [
        SeriesSpec("JPNRGDPEXP", "growth", "Japan Real GDP", "quarterly", units="pc1"),
        SeriesSpec("CHNLOLITOAASTSAM", "growth", "China CLI Amplitude-Adjusted", "monthly"),
        SeriesSpec("JPNLOLITOAASTSAM", "growth", "Japan CLI Amplitude-Adjusted", "monthly"),
        SeriesSpec("JPNCPIALLMINMEI", "inflation", "Japan CPI", "monthly", invert=True, units="pc1"),
        SeriesSpec("CHNCPIALLMINMEI", "inflation", "China CPI", "monthly", invert=True, units="pc1"),
        SeriesSpec("IRLTLT01JPM156N", "monetary", "10Y JGB Yield", "monthly"),
        SeriesSpec("BAMLEMRACRPIASIAOAS", "financial_conditions", "Asia EM Corp OAS", "daily", invert=True),
    ],
    "EM": [
        SeriesSpec("BRALOLITOAASTSAM", "growth", "Brazil CLI Amplitude-Adjusted", "monthly"),
        SeriesSpec("INDLOLITOAASTSAM", "growth", "India CLI Amplitude-Adjusted", "monthly"),
        SeriesSpec("MEXLOLITONOSTSAM", "growth", "Mexico CLI Normalized", "monthly"),
        SeriesSpec("BRACPIALLMINMEI", "inflation", "Brazil CPI", "monthly", invert=True, units="pc1"),
        SeriesSpec("INDCPIALLMINMEI", "inflation", "India CPI", "monthly", invert=True, units="pc1"),
        SeriesSpec("INTDSRBRM193N", "monetary", "Brazil SELIC", "monthly", invert=True),
        SeriesSpec("BAMLEMCBPIOAS", "financial_conditions", "EM Corp OAS", "daily", invert=True),
    ],
}

GLOBAL_SERIES: list[SeriesSpec] = [
    SeriesSpec("GPRH", "geopolitical", "Geopolitical Risk Index", "monthly", invert=True),
    SeriesSpec("USEPUINDXD", "geopolitical", "Economic Policy Uncertainty", "daily", invert=True),
    SeriesSpec("DCOILWTICO", "energy", "WTI Crude Oil", "daily", invert=True),
    SeriesSpec("DCOILBRENTEU", "energy", "Brent Crude Oil", "daily", invert=True),
    SeriesSpec("DHHNGSP", "energy", "Henry Hub Natural Gas", "daily", invert=True),
    SeriesSpec("WCSSTUS1", "reserves", "US Strategic Petroleum Reserve", "weekly"),
    SeriesSpec("WCESTUS1", "reserves", "US Crude Oil Inventories", "weekly"),
    SeriesSpec("PCOPPUSDM", "metals", "Global Copper Price", "monthly"),
    SeriesSpec("GOLDAMGBD228NLBM", "metals", "London Gold Price", "daily"),
    SeriesSpec("PFERTINDEXM", "agriculture", "Fertilizer Price Index", "monthly", invert=True),
    SeriesSpec("DTWEXBGS", "currency", "USD Trade-Weighted Index", "daily"),
]

CREDIT_SERIES: list[SeriesSpec] = [
    SeriesSpec("BAA10Y", "credit_spreads", "Baa Corporate Spread (Moody's)", "daily"),
    SeriesSpec("BAMLH0A0HYM2", "credit_spreads", "ICE BofA HY Spread (OAS)", "daily"),
    SeriesSpec("SOFR", "rates", "SOFR Overnight Rate", "daily"),
    SeriesSpec("USREC", "recession", "NBER Recession Indicator", "monthly"),
    SeriesSpec("CSUSHPINSA", "real_estate", "Case-Shiller National HPI (NSA)", "monthly"),
    SeriesSpec("MSPUS", "real_estate", "Median Sales Price of Houses Sold", "quarterly"),
    SeriesSpec("HOUST", "real_estate", "Housing Starts (Total, SAAR)", "monthly"),
    SeriesSpec("PERMIT", "real_estate", "Building Permits (Total, SAAR)", "monthly"),
    SeriesSpec("EXHOSLUSM495S", "real_estate", "Existing Home Sales", "monthly"),
    SeriesSpec("MSACSR", "real_estate", "Monthly Supply of Houses", "monthly"),
    SeriesSpec("MORTGAGE30US", "mortgage", "30-Year Fixed Mortgage Rate", "weekly"),
    SeriesSpec("MORTGAGE15US", "mortgage", "15-Year Fixed Mortgage Rate", "weekly"),
    SeriesSpec("OBMMIFHA30YF", "mortgage", "FHA 30-Year Fixed Mortgage Rate", "weekly"),
    SeriesSpec("DRCCLACBS", "delinquency", "Credit Card Delinquency Rate", "quarterly"),
    SeriesSpec("DRSFRMACBS", "delinquency", "Single-Family Mortgage Delinquency Rate", "quarterly"),
    SeriesSpec("DRHMACBS", "delinquency", "Home Equity Loan Delinquency Rate", "quarterly"),
    SeriesSpec("DRALACBN", "credit_quality", "Delinquency Rate — All Loans", "quarterly"),
    SeriesSpec("NETCIBAL", "credit_quality", "Net Charge-Off Rate — All Loans", "quarterly"),
    SeriesSpec("CCLACBW027SBOG", "credit_quality", "CRE Loans (commercial banks)", "weekly"),
    SeriesSpec("DRCILNFNQ", "credit_quality", "Delinquency Rate — C&I Loans", "quarterly"),
    SeriesSpec("ICSA", "labor", "Initial Jobless Claims", "weekly"),
    SeriesSpec("TOTBKCR", "credit_cycle", "Total Bank Credit, All Commercial Banks", "weekly"),
    SeriesSpec("TOTLL", "banking", "Total Loans & Leases", "weekly"),
    SeriesSpec("DPSACBW027SBOG", "banking", "Total Deposits", "weekly"),
    SeriesSpec("STLFSI4", "banking", "St. Louis Fed Financial Stress Index", "weekly"),
    SeriesSpec("WRMFSL", "banking", "Money Market Fund Assets (retail)", "weekly"),
    SeriesSpec("NYXRSA", "real_estate_regional", "Case-Shiller New York", "monthly"),
    SeriesSpec("LXXRSA", "real_estate_regional", "Case-Shiller Los Angeles", "monthly"),
    SeriesSpec("MFHXRSA", "real_estate_regional", "Case-Shiller Miami", "monthly"),
    SeriesSpec("CHXRSA", "real_estate_regional", "Case-Shiller Chicago", "monthly"),
    SeriesSpec("DAXRSA", "real_estate_regional", "Case-Shiller Dallas", "monthly"),
    SeriesSpec("HIOXRSA", "real_estate_regional", "Case-Shiller Houston", "monthly"),
    SeriesSpec("WDXRSA", "real_estate_regional", "Case-Shiller Washington DC", "monthly"),
    SeriesSpec("BOXRSA", "real_estate_regional", "Case-Shiller Boston", "monthly"),
    SeriesSpec("ATXRSA", "real_estate_regional", "Case-Shiller Atlanta", "monthly"),
    SeriesSpec("SEXRSA", "real_estate_regional", "Case-Shiller Seattle", "monthly"),
    SeriesSpec("PHXRSA", "real_estate_regional", "Case-Shiller Phoenix", "monthly"),
    SeriesSpec("DNXRSA", "real_estate_regional", "Case-Shiller Denver", "monthly"),
    SeriesSpec("SFXRSA", "real_estate_regional", "Case-Shiller San Francisco", "monthly"),
    SeriesSpec("TPXRSA", "real_estate_regional", "Case-Shiller Tampa", "monthly"),
    SeriesSpec("CRXRSA", "real_estate_regional", "Case-Shiller Charlotte", "monthly"),
    SeriesSpec("MNXRSA", "real_estate_regional", "Case-Shiller Minneapolis", "monthly"),
    SeriesSpec("POXRSA", "real_estate_regional", "Case-Shiller Portland", "monthly"),
    SeriesSpec("SDXRSA", "real_estate_regional", "Case-Shiller San Diego", "monthly"),
    SeriesSpec("DEXRSA", "real_estate_regional", "Case-Shiller Detroit", "monthly"),
    SeriesSpec("CLXRSA", "real_estate_regional", "Case-Shiller Cleveland", "monthly"),
]


# Raw-only ingest: fetched and upserted into macro_data for downstream consumers
# (the risk_metrics FI inflation-beta regression reads Δ T10YIE) but deliberately
# NOT part of any scored dimension list, so the regional regime snapshot is
# unchanged by their presence.
RAW_INGEST_SERIES: list[SeriesSpec] = [
    SeriesSpec("T10YIE", "inflation_expectations", "10Y Breakeven Inflation", "daily"),
    # open_macro v4.0-rev (M-COMP4). L1 reads the first two, L3 the last two. They
    # are raw-only for the same reason T10YIE is: scoring them would move the
    # regional regime snapshot, and the v4 engine reads macro_data directly.
    # NB `GDP` is the NOMINAL LEVEL series. The registry already carries
    # A191RL1Q225SBEA (real growth) under the US growth dimension; substituting one
    # for the other silently changes the deficit/GDP ratio's denominator.
    # NB this worker fetches a LOOKBACK_YEARS window; it keeps these four current,
    # it does not backfill the decades of history the v4 replay reads.
    SeriesSpec("MTSDS133FMS", "fiscal", "Federal Surplus/Deficit (MTS)", "monthly"),
    SeriesSpec("GDP", "fiscal", "Gross Domestic Product (nominal level)", "quarterly"),
    SeriesSpec("M2SL", "monetary", "M2 Money Stock", "monthly"),
    SeriesSpec("SUBLPDCILSLGNQ", "credit",
               "SLOOS: banks tightening C&I standards, large and middle-market firms",
               "quarterly"),
    # INDPRO, the INDEX LEVEL, stored under the alias INDPRO_IDX.
    #
    # ``macro_data.INDPRO`` is a chimera and has been for as long as this worker
    # has run. The US growth dimension scores INDPRO with units="pc1" (YoY %), so
    # every run rewrites the trailing LOOKBACK_YEARS window in percent — while the
    # head of the series, older than the first pc1 run, is still index level from
    # an earlier ingest. Prod: ~689 rows in the 22..104 range (level) ahead of the
    # splice, ~121 rows in −17..+17 (percent) behind it. Reading INDPRO across the
    # splice as one unit is meaningless.
    #
    # The regional scoring depends on the pc1 spec, so it is NOT touched here. The
    # divergence gauge needs the level with full history, so it gets its own
    # storage id fed by the same FRED series with units="lin". Two transforms, two
    # rows, no collision.
    #
    # OPERATOR NOTE (out of scope for this worker): the pre-splice head of
    # ``macro_data.INDPRO`` is a fossil in the wrong unit and is a candidate for a
    # hygiene re-sync to pc1. Nothing here reads it; nothing here repairs it.
    SeriesSpec("INDPRO", "growth", "Industrial Production (index level, 2017=100)",
               "monthly", storage_id="INDPRO_IDX", full_history=True),
]


def _all_specs() -> list[SeriesSpec]:
    """Every spec to fetch, deduped by STORAGE key (not by FRED id).

    Deduping by FRED id would silently drop an aliased spec whose FRED series is
    already scored somewhere — exactly the INDPRO/INDPRO_IDX case.
    """
    specs: list[SeriesSpec] = []
    for region_specs in REGION_SERIES.values():
        specs.extend(region_specs)
    specs.extend(GLOBAL_SERIES)
    existing = {s.key for s in specs}
    specs.extend(s for s in CREDIT_SERIES if s.key not in existing)
    existing |= {s.key for s in CREDIT_SERIES}
    specs.extend(s for s in RAW_INGEST_SERIES if s.key not in existing)
    return specs


def get_all_series_ids() -> list[str]:
    """The macro_data.series_id values this worker writes (storage keys)."""
    return [s.key for s in _all_specs()]


# ──────────────────────────────────────────────────────────────────────────────
# FRED fetch (rate-limited)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Obs:
    date: str  # ISO yyyy-mm-dd
    value: float | None


class TokenBucket:
    """Thread-safe token bucket — FRED allows 120 req/min (2 req/s sustained)."""

    def __init__(self, max_tokens: float = 10.0, refill_rate: float = 2.0) -> None:
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self._tokens = max_tokens
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self.max_tokens, self._tokens + (now - self._last) * self.refill_rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.refill_rate
            time.sleep(wait)


def parse_observations(payload: dict[str, Any]) -> list[Obs]:
    """FRED JSON → [Obs]; missing markers and unparseable values dropped."""
    out: list[Obs] = []
    for o in payload.get("observations", []):
        raw = o.get("value")
        s = str(raw).strip() if raw is not None else ""
        if s in _MISSING_VALUES:
            continue
        try:
            v = float(s)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(v):
            continue
        out.append(Obs(o["date"], v))
    return out


def _fetch_series(client, api_key: str, spec: SeriesSpec, observation_start: str,
                  bucket: TokenBucket) -> list[Obs]:
    limit = FREQUENCY_LIMITS.get(spec.frequency, 120)
    if spec.full_history:
        observation_start = FULL_HISTORY_START
        limit = FULL_HISTORY_LIMIT
    params = {
        "series_id": spec.series_id,
        "api_key": api_key,
        "file_type": "json",
        # desc: FRED applies `limit` AFTER sorting, so newest-first guarantees the
        # most recent observations survive even if a window ever exceeds the limit
        # (with asc, truncation silently froze the newest months out of macro_data).
        "sort_order": "desc",
        "limit": limit,
        "observation_start": observation_start,
    }
    if spec.units and spec.units != "lin":
        params["units"] = spec.units
    for attempt in range(3):
        bucket.acquire()
        resp = client.get(f"{FRED_BASE_URL}/series/observations", params=params)
        if resp.status_code in (429, 503) or resp.status_code >= 500:
            time.sleep(min(30.0, 2.0 * (2 ** attempt)))
            continue
        if resp.status_code == 400:  # bad/discontinued series: skip, don't fail run
            return []
        resp.raise_for_status()
        payload = resp.json()
        count = payload.get("count")
        if isinstance(count, int) and count > limit:
            print(
                f"WARN macro_ingestion fred_window_truncated series={spec.series_id} "
                f"count={count} limit={limit} (oldest rows dropped, newest kept)",
                flush=True,
            )
        obs = parse_observations(payload)
        # Downstream (derived series, snapshot percentile scoring) assumes ascending.
        obs.sort(key=lambda o: o.date)
        return obs
    return []


def fetch_all_series(api_key: str, observation_start: str,
                     limit: int | None = None) -> dict[str, list[Obs]]:
    """Fetch every registry series concurrently (5 threads, shared bucket).

    Keyed by STORAGE id (``SeriesSpec.key``), so an aliased spec lands in its own
    slot instead of overwriting the FRED-id-named one.
    """
    import concurrent.futures

    import httpx

    specs = _all_specs()
    if limit:
        specs = specs[:limit]
    bucket = TokenBucket()
    out: dict[str, list[Obs]] = {}
    with httpx.Client(timeout=30.0) as client:
        def one(spec: SeriesSpec) -> tuple[str, list[Obs]]:
            return spec.key, _fetch_series(client, api_key, spec, observation_start, bucket)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            for sid, obs in pool.map(one, specs):
                out[sid] = obs
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Rows + derived series (pure)
# ──────────────────────────────────────────────────────────────────────────────
def obs_to_rows(raw: dict[str, list[Obs]], *, source: str = "fred") -> list[dict[str, Any]]:
    """``{macro_data.series_id: [Obs]}`` → upsert row dicts.

    Keys are the STORAGE ids (``macro_data.series_id``), not the upstream
    provider's ids — see ``SeriesSpec.key``. ``source`` tags the provider so a
    non-FRED feed (BIS) can reuse the same row shape.
    """
    rows: list[dict[str, Any]] = []
    for sid, obs_list in raw.items():
        for o in obs_list:
            if o.value is None:
                continue
            rows.append({
                "series_id": sid,
                "obs_date": _dt.date.fromisoformat(o.date),
                "value": o.value,
                "source": source,
                "is_derived": False,
            })
    return rows


def compute_derived_series(raw: dict[str, list[Obs]]) -> list[dict[str, Any]]:
    """YIELD_CURVE_10Y2Y = DGS10 - DGS2; CPI_YOY = 12m % change of CPIAUCSL."""
    rows: list[dict[str, Any]] = []

    ten = {o.date: o.value for o in raw.get("DGS10", []) if o.value is not None}
    two = {o.date: o.value for o in raw.get("DGS2", []) if o.value is not None}
    for d in sorted(set(ten) & set(two)):
        rows.append({
            "series_id": "YIELD_CURVE_10Y2Y",
            "obs_date": _dt.date.fromisoformat(d),
            "value": round(ten[d] - two[d], 4),
            "source": "derived",
            "is_derived": True,
        })

    cpi = {o.date: o.value for o in raw.get("CPIAUCSL", []) if o.value is not None}
    for d, v in cpi.items():
        cur = _dt.date.fromisoformat(d)
        prior_key = f"{cur.year - 1:04d}-{cur.month:02d}-01"
        prior = cpi.get(prior_key)
        if prior is None or prior == 0:
            continue
        rows.append({
            "series_id": "CPI_YOY",
            "obs_date": cur,
            "value": round((v / prior - 1.0) * 100.0, 4),
            "source": "derived",
            "is_derived": True,
        })
    return rows


def dedup_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Dedup by (series_id, obs_date), keeping the last value seen — Postgres
    rejects two rows with the same conflict key in a single INSERT."""
    seen: dict[tuple[str, _dt.date], dict[str, Any]] = {}
    for r in rows:
        seen[(r["series_id"], r["obs_date"])] = r
    return list(seen.values())


# ──────────────────────────────────────────────────────────────────────────────
# Snapshot scoring (pure — verbatim monolith logic)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class DataFreshness:
    series_id: str
    last_date: _dt.date | None
    days_stale: int | None
    weight: float
    status: str  # fresh | decaying | stale


@dataclass(frozen=True)
class DimensionScore:
    dimension: str
    score: float
    n_indicators: int
    indicators: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class BisDataPoint:
    country_code: str
    indicator: str
    value: float
    period: _dt.date


@dataclass(frozen=True)
class ImfDataPoint:
    country_code: str
    indicator: str
    year: int
    value: float
    period: _dt.date | None = None
    edition: str | None = None


_COUNTRY_TO_REGION: dict[str, str] = {
    "US": "US",
    "GB": "EUROPE", "DE": "EUROPE", "FR": "EUROPE", "IT": "EUROPE",
    "ES": "EUROPE", "NL": "EUROPE", "CH": "EUROPE", "SE": "EUROPE",
    "NO": "EUROPE", "DK": "EUROPE", "AT": "EUROPE", "BE": "EUROPE",
    "FI": "EUROPE", "PT": "EUROPE", "IE": "EUROPE", "GR": "EUROPE",
    "PL": "EUROPE", "CZ": "EUROPE", "HU": "EUROPE",
    "JP": "ASIA", "CN": "ASIA", "KR": "ASIA", "AU": "ASIA",
    "SG": "ASIA", "HK": "ASIA", "TW": "ASIA",
    "BR": "EM", "IN": "EM", "MX": "EM", "TR": "EM", "ZA": "EM",
    "CL": "EM", "CO": "EM", "PE": "EM", "TH": "EM", "MY": "EM",
    "ID": "EM", "PH": "EM", "AR": "EM", "RU": "EM", "SA": "EM",
    "IL": "EM",
}

_REGION_IMF_AGGREGATE: dict[str, str] = {
    "US": "ADVEC", "EUROPE": "ADVEC", "ASIA": "EMDA", "EM": "EME",
}

_ISO3_TO_ISO2: dict[str, str] = {
    "USA": "US", "GBR": "GB", "DEU": "DE", "FRA": "FR", "JPN": "JP",
    "CHN": "CN", "BRA": "BR", "IND": "IN", "MEX": "MX", "KOR": "KR",
    "AUS": "AU", "CAN": "CA", "ITA": "IT", "ESP": "ES", "NLD": "NL",
    "CHE": "CH", "SWE": "SE", "NOR": "NO", "DNK": "DK", "AUT": "AT",
    "BEL": "BE", "FIN": "FI", "PRT": "PT", "IRL": "IE", "GRC": "GR",
    "POL": "PL", "CZE": "CZ", "HUN": "HU", "TUR": "TR", "ZAF": "ZA",
    "CHL": "CL", "COL": "CO", "PER": "PE", "THA": "TH", "MYS": "MY",
    "IDN": "ID", "PHL": "PH", "SGP": "SG", "HKG": "HK", "TWN": "TW",
    "ARG": "AR", "RUS": "RU", "SAU": "SA", "ISR": "IL",
}


def percentile_rank_score(current: float, history: np.ndarray, *, invert: bool = False) -> float:
    """0-100 percentile rank; neutral 50.0 below MIN_HISTORY_OBS observations."""
    if len(history) < MIN_HISTORY_OBS:
        return 50.0
    rank = float(np.sum(history <= current) / len(history) * 100)
    if invert:
        rank = 100.0 - rank
    return round(rank, 2)


def compute_staleness_weight(last_obs_date: _dt.date | None, as_of: _dt.date,
                             frequency: str, staleness_config: dict[str, Any]) -> DataFreshness:
    """Linear decay 1.0 → floor between fresh_days and max_useful_days; 0 beyond."""
    if last_obs_date is None:
        return DataFreshness("", None, None, 0.0, "stale")
    days_stale = (as_of - last_obs_date).days
    cfg = staleness_config.get(frequency, staleness_config.get(
        "monthly", {"fresh_days": 45, "max_useful_days": 90, "floor": 0.50}))
    fresh_days, max_useful, floor = cfg["fresh_days"], cfg["max_useful_days"], cfg["floor"]
    if days_stale <= fresh_days:
        weight, status = 1.0, "fresh"
    elif days_stale <= max_useful:
        progress = (days_stale - fresh_days) / (max_useful - fresh_days)
        weight, status = max(floor, 1.0 - progress * (1.0 - floor)), "decaying"
    else:
        weight, status = 0.0, "stale"
    return DataFreshness("", last_obs_date, days_stale, round(weight, 4), status)


def _extract_history(observations: list[Obs]) -> tuple[np.ndarray, _dt.date | None]:
    values: list[float] = []
    latest: _dt.date | None = None
    for o in observations:
        if o.value is None:
            continue
        values.append(o.value)
        d = _dt.date.fromisoformat(o.date)
        if latest is None or d > latest:
            latest = d
    return np.array(values, dtype=np.float64), latest


def score_region(region: str, raw: dict[str, list[Obs]], as_of: _dt.date) -> dict[str, Any]:
    """Composite macro score for one region (percentile-rank, staleness-weighted)."""
    specs = REGION_SERIES.get(region, [])
    dim_weights = _DEFAULT_CONFIG["dimension_weights"]
    staleness_cfg = _DEFAULT_CONFIG["staleness"]
    min_coverage = _DEFAULT_CONFIG["min_coverage"]

    indicator_scores: dict[str, float] = {}
    freshness: dict[str, DataFreshness] = {}
    for spec in specs:
        history, last_date = _extract_history(raw.get(spec.key, []))
        if len(history) == 0:
            freshness[spec.key] = DataFreshness(spec.key, None, None, 0.0, "stale")
            continue
        indicator_scores[spec.key] = percentile_rank_score(
            float(history[-1]), history, invert=spec.invert)
        f = compute_staleness_weight(last_date, as_of, spec.frequency, staleness_cfg)
        freshness[spec.key] = DataFreshness(
            spec.key, f.last_date, f.days_stale, f.weight, f.status)

    by_dim: dict[str, list[tuple[str, float, float]]] = {}
    for spec in specs:
        if spec.key not in indicator_scores:
            continue
        w = freshness[spec.key].weight
        if w <= 0:
            continue
        by_dim.setdefault(spec.dimension, []).append(
            (spec.key, indicator_scores[spec.key], w))

    dimensions: dict[str, DimensionScore] = {}
    for dim, indicators in by_dim.items():
        total_w = sum(w for _, _, w in indicators)
        if total_w <= 0:
            continue
        dimensions[dim] = DimensionScore(
            dim, round(sum(s * w for _, s, w in indicators) / total_w, 2),
            len(indicators), {sid: s for sid, s, _ in indicators})

    active_weight = sum(dim_weights.get(d, 0) for d in dimensions)
    total_possible = sum(dim_weights.values())
    coverage = active_weight / total_possible if total_possible > 0 else 0.0
    if coverage < min_coverage or active_weight <= 0:
        composite = 50.0
    else:
        composite = sum(dimensions[d].score * dim_weights.get(d, 0)
                        for d in dimensions) / active_weight
    return {
        "composite_score": round(composite, 2),
        "coverage": round(coverage, 4),
        "dimensions": dimensions,
        "data_freshness": freshness,
    }


def _score_credit_cycle(region: str, bis_data: list[BisDataPoint]) -> dict[str, Any] | None:
    region_countries = {cc for cc, r in _COUNTRY_TO_REGION.items() if r == region}
    latest: dict[tuple[str, str], tuple[_dt.date, float]] = {}
    for dp in bis_data:
        if dp.country_code not in region_countries:
            continue
        key = (dp.country_code, dp.indicator)
        if key not in latest or dp.period > latest[key][0]:
            latest[key] = (dp.period, dp.value)
    if not latest:
        return None
    by_ind: dict[str, list[float]] = {}
    for (_, ind), (_, v) in latest.items():
        by_ind.setdefault(ind, []).append(v)

    scores: list[float] = []
    weights: list[float] = []
    avg_gap = float(np.mean(by_ind["credit_to_gdp_gap"])) if by_ind.get("credit_to_gdp_gap") else None
    if avg_gap is not None:
        scores.append(max(0.0, min(100.0, 50.0 - avg_gap * 4.0)))
        weights.append(0.5)
    avg_dsr = float(np.mean(by_ind["debt_service_ratio"])) if by_ind.get("debt_service_ratio") else None
    if avg_dsr is not None:
        scores.append(max(0.0, min(100.0, 120.0 - avg_dsr * 4.0)))
        weights.append(0.3)
    avg_prop = float(np.mean(by_ind["property_prices"])) if by_ind.get("property_prices") else None
    if avg_prop is not None:
        prop_score = (50.0 + avg_prop * 3.0 if avg_prop <= 5.0
                      else max(20.0, 65.0 - (avg_prop - 5.0) * 2.5))
        scores.append(prop_score)
        weights.append(0.2)
    if not scores:
        return None
    total_w = sum(weights)
    n_countries = len({cc for (cc, _) in latest})
    return {
        "score": round(sum(s * w for s, w in zip(scores, weights)) / total_w, 2),
        "credit_gap": round(avg_gap, 4) if avg_gap is not None else None,
        "debt_service": round(avg_dsr, 4) if avg_dsr is not None else None,
        "property_prices": round(avg_prop, 4) if avg_prop is not None else None,
        "n_countries": n_countries,
    }


def _score_imf_indicator(indicator: str, value: float) -> float:
    if indicator == "NGDP_RPCH":
        return max(0.0, min(100.0, 35.0 + value * 7.5))
    if indicator == "PCPIPCH":
        return max(0.0, min(100.0, 100.0 - abs(value - 2.0) * 15.0))
    if indicator == "GGXCNL_NGDP":
        return max(0.0, min(100.0, 50.0 + value * 5.0))
    if indicator == "GGXWDG_NGDP":
        return max(0.0, min(100.0, 100.0 - value * 0.8))
    return 50.0


def _select_region_imf_points(region: str, imf_data: list[ImfDataPoint],
                              as_of: _dt.date) -> list[ImfDataPoint]:
    aggregate = _REGION_IMF_AGGREGATE.get(region)
    agg_points = [dp for dp in imf_data if dp.country_code == aggregate]
    country_points = [
        dp for dp in imf_data
        if _COUNTRY_TO_REGION.get(_ISO3_TO_ISO2.get(dp.country_code, dp.country_code)) == region
    ]
    if agg_points and _imf_points_fresh(agg_points, as_of):
        return agg_points
    if country_points and _imf_points_fresh(country_points, as_of):
        return country_points
    return agg_points or country_points


def _imf_points_fresh(points: list[ImfDataPoint], as_of: _dt.date) -> bool:
    periods = [dp.period for dp in points if dp.period is not None and dp.period <= as_of]
    return bool(periods) and (as_of - max(periods)).days <= IMF_MAX_STALE_DAYS


def _blend_imf_dimension(region: str, current_score: float, imf_data: list[ImfDataPoint],
                         as_of: _dt.date, indicators: tuple[str, ...]) -> float:
    points = _select_region_imf_points(region, imf_data, as_of)
    if not _imf_points_fresh(points, as_of):
        return current_score
    horizon_scores: dict[int, list[float]] = {}
    for dp in points:
        if dp.indicator not in indicators:
            continue
        horizon = dp.year - as_of.year
        if horizon not in IMF_FORECAST_WEIGHTS:
            continue
        horizon_scores.setdefault(horizon, []).append(_score_imf_indicator(dp.indicator, dp.value))
    if not horizon_scores:
        return current_score
    blended = 0.0
    total_fw = 0.0
    for horizon, weight in IMF_FORECAST_WEIGHTS.items():
        values = horizon_scores.get(horizon)
        if not values:
            continue
        blended += float(np.mean(values)) * weight
        total_fw += weight
    blended += current_score * max(0.0, 1.0 - total_fw)
    return round(max(0.0, min(100.0, blended)), 2)


def _enrich_region(result: dict[str, Any], region: str, as_of: _dt.date,
                   bis_data: list[BisDataPoint] | None,
                   imf_data: list[ImfDataPoint] | None) -> dict[str, Any]:
    """BIS credit_cycle 7th dimension + IMF growth/inflation/fiscal blends.
    No-op when BIS/IMF data is None/empty (graceful degradation)."""
    dimensions: dict[str, DimensionScore] = dict(result["dimensions"])
    changed = False

    if bis_data:
        cc = _score_credit_cycle(region, bis_data)
        if cc is not None and cc["n_countries"] > 0:
            dimensions["credit_cycle"] = DimensionScore(
                "credit_cycle", cc["score"], cc["n_countries"],
                {"credit_gap": cc["credit_gap"] or 0.0,
                 "debt_service": cc["debt_service"] or 0.0,
                 "property_prices": cc["property_prices"] or 0.0})
            changed = True

    if imf_data and "growth" in dimensions:
        orig = dimensions["growth"]
        blended = _blend_imf_dimension(region, orig.score, imf_data, as_of, ("NGDP_RPCH",))
        if blended != orig.score:
            dimensions["growth"] = DimensionScore("growth", blended, orig.n_indicators, orig.indicators)
            changed = True
    if imf_data and "inflation" in dimensions:
        orig = dimensions["inflation"]
        blended = _blend_imf_dimension(region, orig.score, imf_data, as_of, ("PCPIPCH",))
        if blended != orig.score:
            dimensions["inflation"] = DimensionScore("inflation", blended, orig.n_indicators, orig.indicators)
            changed = True
    if imf_data:
        fiscal = _blend_imf_dimension(region, 50.0, imf_data, as_of, ("GGXCNL_NGDP", "GGXWDG_NGDP"))
        if fiscal != 50.0:
            dimensions["fiscal"] = DimensionScore(
                "fiscal", fiscal, 2,
                {"fiscal_balance": fiscal, "government_debt": fiscal})
            changed = True

    if not changed:
        return result

    dim_weights = dict(_DEFAULT_CONFIG["dimension_weights"])
    if "credit_cycle" in dimensions:
        dim_weights["credit_cycle"] = 0.10
    if "fiscal" in dimensions:
        dim_weights["fiscal"] = 0.10
    active = sum(dim_weights.get(d, 0) for d in dimensions)
    composite = (sum(dimensions[d].score * dim_weights.get(d, 0) for d in dimensions) / active
                 if active > 0 else result["composite_score"])
    return {**result, "composite_score": round(composite, 2), "dimensions": dimensions}


def score_global_indicators(raw: dict[str, list[Obs]]) -> dict[str, float]:
    invert = {s.key: s.invert for s in GLOBAL_SERIES}

    def _avg(series_ids: list[str]) -> float:
        scores = []
        for sid in series_ids:
            history, _ = _extract_history(raw.get(sid, []))
            if len(history) == 0:
                continue
            scores.append(percentile_rank_score(
                float(history[-1]), history, invert=invert.get(sid, False)))
        return round(sum(scores) / len(scores), 2) if scores else 50.0

    geopolitical = _avg(["GPRH", "USEPUINDXD"])
    energy_price = _avg(["DCOILWTICO", "DCOILBRENTEU", "DHHNGSP"])
    energy_reserves = _avg(["WCSSTUS1", "WCESTUS1"])
    energy_stress = round((100.0 - energy_price) * 0.6 + (100.0 - energy_reserves) * 0.4, 2)
    commodity = _avg(["PCOPPUSDM", "GOLDAMGBD228NLBM", "PFERTINDEXM"])
    usd = _avg(["DTWEXBGS"])
    return {
        "geopolitical_risk_score": geopolitical,
        "energy_stress": energy_stress,
        "commodity_stress": commodity,
        "usd_strength": usd,
    }


def build_regional_snapshot(raw: dict[str, list[Obs]], *, as_of: _dt.date,
                            bis_data: list[BisDataPoint] | None = None,
                            imf_data: list[ImfDataPoint] | None = None) -> dict[str, Any]:
    """Version-1 snapshot dict for macro_regional_snapshots.data_json."""
    regions: dict[str, Any] = {}
    for region in ("US", "EUROPE", "ASIA", "EM"):
        result = score_region(region, raw, as_of)
        result = _enrich_region(result, region, as_of, bis_data, imf_data)
        regions[region] = {
            "composite_score": result["composite_score"],
            "coverage": result["coverage"],
            "dimensions": {
                dim: {"score": ds.score, "n_indicators": ds.n_indicators,
                      "indicators": ds.indicators}
                for dim, ds in result["dimensions"].items()
            },
            "data_freshness": {
                sid: {"last_date": f.last_date.isoformat() if f.last_date else None,
                      "days_stale": f.days_stale, "weight": f.weight, "status": f.status}
                for sid, f in result["data_freshness"].items()
            },
        }
    return {
        "version": 1,
        "as_of_date": as_of.isoformat(),
        "regions": regions,
        "global_indicators": score_global_indicators(raw),
    }


# ──────────────────────────────────────────────────────────────────────────────
# DB I/O
# ──────────────────────────────────────────────────────────────────────────────
def _fetch_bis(conn) -> list[BisDataPoint] | None:
    """Last 180d of bis_statistics for snapshot enrichment; None when absent."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT country_code, indicator, value, period FROM bis_statistics
                   WHERE period >= now() - interval '180 days' AND value IS NOT NULL""")
            return [BisDataPoint(r[0], r[1], float(r[2]),
                                 r[3].date() if isinstance(r[3], _dt.datetime) else r[3])
                    for r in cur.fetchall()] or None
    except Exception:
        conn.rollback()
        return None


def _fetch_imf(conn) -> list[ImfDataPoint] | None:
    """Recent imf_weo_forecasts for snapshot enrichment; None when absent."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT country_code, indicator, year, value, period, edition
                   FROM imf_weo_forecasts
                   WHERE year >= extract(year FROM now())::int - 1
                     AND period <= now() AND value IS NOT NULL""")
            return [ImfDataPoint(r[0], r[1], int(r[2]), float(r[3]),
                                 r[4].date() if isinstance(r[4], _dt.datetime) else r[4], r[5])
                    for r in cur.fetchall()] or None
    except Exception:
        conn.rollback()
        return None


def upsert_macro_data(conn, rows: list[dict[str, Any]]) -> int:
    """Chunked idempotent upsert into macro_data. Caller commits."""
    upserted = 0
    sql = """
        INSERT INTO macro_data (series_id, obs_date, value, source, is_derived)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date) DO UPDATE SET
            value = EXCLUDED.value,
            source = EXCLUDED.source,
            is_derived = EXCLUDED.is_derived,
            updated_at = now()
    """
    with conn.cursor() as cur:
        for i in range(0, len(rows), UPSERT_CHUNK):
            chunk = rows[i:i + UPSERT_CHUNK]
            cur.executemany(sql, [
                (r["series_id"], r["obs_date"], r["value"], r["source"], r["is_derived"])
                for r in chunk
            ])
            upserted += len(chunk)
    return upserted


def upsert_snapshot(conn, as_of: _dt.date, data_json: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO macro_regional_snapshots (as_of_date, data_json, created_by)
               VALUES (%s, %s, 'worker:macro_ingestion')
               ON CONFLICT (as_of_date) DO UPDATE SET
                   data_json = EXCLUDED.data_json,
                   updated_by = 'worker:macro_ingestion',
                   updated_at = now()""",
            (as_of, json.dumps(data_json)))


# ──────────────────────────────────────────────────────────────────────────────
# BIS offshore-dollar stock (SDMX v2 REST, stats.bis.org)
# ──────────────────────────────────────────────────────────────────────────────
# Input to the liquidity-divergence DIAGNOSTIC gauge (conviction P1b): the Light
# backend computes the gauge on-read from ``macro_data`` out of the M2 + offshore
# stock composite. It informs; it never allocates.
#
# Provenance is fixed by the P1b pre-registration and is NOT a free parameter:
# dataflow ``WS_LBS_D_PUB`` version 1.0, key ``Q.S.C.A.USD.A.5J.A.5A.A.5J.N``.
# Open download, no key, no published quota — but we stay polite anyway: 60s
# timeout, 3 attempts, exponential backoff.
BIS_BASE_URL = "https://stats.bis.org/api/v2/data/dataflow/BIS"
BIS_SOURCE = "bis"
BIS_TIMEOUT_SECONDS = 60.0
BIS_MAX_ATTEMPTS = 3
# The LBS publishes in USD MILLIONS (UNIT_MEASURE=USD, UNIT_MULT=6). macro_data
# stores this series in USD BILLIONS so it is dimensionally comparable with the
# FRED monetary aggregates (M2SL is $B). Both facts are asserted at parse time:
# a silent BIS rescale would move the gauge by 1000x without any error.
BIS_UNIT_MEASURE = "USD"
BIS_UNIT_MULT = "6"
BIS_MILLIONS_PER_BILLION = 1000.0

_BIS_QUARTER_RE = re.compile(r"^(\d{4})-?Q([1-4])$")


@dataclass(frozen=True)
class BisSeriesSpec:
    storage_id: str  # macro_data.series_id
    dataflow: str
    version: str
    sdmx_key: str
    label: str


BIS_SERIES: list[BisSeriesSpec] = [
    BisSeriesSpec(
        "BIS_LBS_XB_CLAIMS_USD", "WS_LBS_D_PUB", "1.0",
        "Q.S.C.A.USD.A.5J.A.5A.A.5J.N",
        # FREQ=Q, L_MEASURE=S (amounts outstanding), L_POSITION=C (total claims),
        # L_INSTR=A (all instruments), L_DENOM=USD, L_CURR_TYPE=A, L_PARENT_CTY=5J,
        # L_REP_BANK_TYPE=A, L_REP_CTY=5A, L_CP_SECTOR=A, L_CP_COUNTRY=5J,
        # L_POS_TYPE=N (cross-border). 1977-Q4 onward, ~194 quarterly observations.
        "BIS LBS cross-border claims, all instruments, USD-denominated ($B)",
    ),
]


def bis_series_url(spec: BisSeriesSpec) -> str:
    return f"{BIS_BASE_URL}/{spec.dataflow}/{spec.version}/{spec.sdmx_key}"


def _bis_period_to_date(period: str | None) -> _dt.date:
    """``'1977-Q4'`` → ``date(1977, 10, 1)``.

    FIRST day of the quarter, FRED-like: every other quarterly series in
    ``macro_data`` (GDP, SUBLPDCILSLGNQ, ...) is period-START stamped, and a
    consumer joining M2SL to this series must not have to special-case it.
    NB the P1b research script stamped quarter-END; the difference is a label,
    not a value, but the on-read gauge has to know which one it is reading.
    """
    match = _BIS_QUARTER_RE.match((period or "").strip())
    if not match:
        raise ValueError(f"unsupported BIS TIME_PERIOD {period!r} (expected 'YYYY-Qn')")
    year, quarter = int(match.group(1)), int(match.group(2))
    return _dt.date(year, 3 * (quarter - 1) + 1, 1)


def parse_bis_csv(text: str) -> list[Obs]:
    """SDMX-CSV → ascending [Obs] in USD BILLIONS, one row per quarter."""
    import csv
    import io

    reader = csv.DictReader(io.StringIO(text))
    missing = {"TIME_PERIOD", "OBS_VALUE", "UNIT_MEASURE", "UNIT_MULT"} - set(
        reader.fieldnames or ())
    if missing:
        raise ValueError(f"unexpected BIS CSV shape, missing columns {sorted(missing)}")

    by_date: dict[_dt.date, float] = {}
    for row in reader:
        unit = (row.get("UNIT_MEASURE") or "").strip()
        mult = (row.get("UNIT_MULT") or "").strip()
        if unit != BIS_UNIT_MEASURE or mult != BIS_UNIT_MULT:
            raise ValueError(
                f"unexpected BIS units UNIT_MEASURE={unit!r} UNIT_MULT={mult!r} "
                f"(expected {BIS_UNIT_MEASURE!r}/{BIS_UNIT_MULT!r}) — refusing to "
                "store a series whose scale is not the documented USD millions")
        raw_value = (row.get("OBS_VALUE") or "").strip()
        if raw_value in _MISSING_VALUES:
            continue
        try:
            millions = float(raw_value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(millions):
            continue
        by_date[_bis_period_to_date(row.get("TIME_PERIOD"))] = (
            millions / BIS_MILLIONS_PER_BILLION)
    return [Obs(day.isoformat(), value) for day, value in sorted(by_date.items())]


def _fetch_bis_with(client, spec: BisSeriesSpec) -> list[Obs]:
    url = bis_series_url(spec)
    last_error: Exception | None = None
    for attempt in range(BIS_MAX_ATTEMPTS):
        if attempt:
            time.sleep(min(30.0, 2.0 * (2 ** (attempt - 1))))
        try:
            resp = client.get(url, params={"format": "csv"})
        except Exception as exc:  # timeout / connection reset → retry
            last_error = exc
            continue
        status = resp.status_code
        if status == 429 or status >= 500:
            last_error = RuntimeError(f"BIS HTTP {status} for {spec.storage_id}")
            continue
        if status != 200:
            raise RuntimeError(f"BIS HTTP {status} for {spec.storage_id} ({url})")
        text = resp.text
        if text.lstrip().startswith("<"):
            raise RuntimeError(
                f"BIS returned an SDMX/XML error document for {spec.storage_id}")
        return parse_bis_csv(text)
    raise RuntimeError(
        f"BIS fetch failed after {BIS_MAX_ATTEMPTS} attempts for "
        f"{spec.storage_id}: {last_error}")


def fetch_bis_series(spec: BisSeriesSpec, *, client: Any | None = None) -> list[Obs]:
    """Download one BIS series, full history, as ascending [Obs] in $B.

    Raises on any failure; the fail-soft boundary is ``ingest_bis``. Pass
    ``client`` to inject a transport (tests); otherwise an httpx client is
    created and closed here.
    """
    if client is not None:
        return _fetch_bis_with(client, spec)
    import httpx

    with httpx.Client(timeout=BIS_TIMEOUT_SECONDS, follow_redirects=True) as owned:
        return _fetch_bis_with(owned, spec)


def ingest_bis(conn, *, specs: list[BisSeriesSpec] | None = None,
               client: Any | None = None) -> tuple[int, str | None]:
    """Fetch the BIS series and upsert them. Returns ``(rows, error_or_None)``.

    Fail-soft by contract: the BIS is a third-party open endpoint and its outage
    must not take the daily FRED ingest down with it. Every failure is swallowed,
    described in the returned string and logged. The DB work runs inside a
    SAVEPOINT (``conn.transaction()``) so a half-applied BIS upsert cannot poison
    the caller's open transaction and lose the FRED rows.

    Full history every run: 194 quarterly observations is cheap, and a window
    would leave the head of the series frozen at whatever the first run wrote.
    """
    specs = BIS_SERIES if specs is None else specs
    try:
        fetched = {spec.storage_id: fetch_bis_series(spec, client=client)
                   for spec in specs}
        rows = dedup_rows(obs_to_rows(fetched, source=BIS_SOURCE))
        if not rows:
            raise RuntimeError("BIS returned no observations")
        with conn.transaction():
            return upsert_macro_data(conn, rows), None
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        print(f"WARN macro_ingestion bis_ingest_failed error={error}", flush=True)
        return 0, error


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Ingest FRED macro series and write snapshot. Returns stats."""
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise RuntimeError("FRED_API_KEY not set")
    as_of = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()
    observation_start = (as_of - _dt.timedelta(days=LOOKBACK_YEARS * 365)).isoformat()

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_MACRO_INGESTION) as got:
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}

            raw = fetch_all_series(api_key, observation_start, limit)
            rows = dedup_rows(obs_to_rows(raw) + compute_derived_series(raw))
            upserted = upsert_macro_data(conn, rows)

            # BIS offshore-dollar stock, AFTER the FRED leg and fail-soft: a BIS
            # outage costs the diagnostic gauge its newest quarter, it does not
            # cost the fleet its daily FRED ingest. The outcome is reported in
            # the stats dict (bis_rows / bis_error), never raised.
            bis_rows, bis_error = ingest_bis(conn)

            snapshot_written = False
            if limit is None:  # partial fetches would skew the regional scores
                snapshot = build_regional_snapshot(
                    raw, as_of=as_of,
                    bis_data=_fetch_bis(conn), imf_data=_fetch_imf(conn))
                upsert_snapshot(conn, as_of, snapshot)
                snapshot_written = True
            conn.commit()

    return {
        "fetched": sum(len(v) for v in raw.values()),
        "series": len(raw),
        "upserted": upserted,
        "bis_rows": bis_rows,
        "bis_error": bis_error,
        "snapshot_written": snapshot_written,
        "as_of": as_of.isoformat(),
    }
