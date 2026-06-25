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

Contract:  run(dsn, *, calc_date=None, limit=None) -> {"fetched", "upserted", ...}
``limit`` caps the number of series fetched (smoke runs); ``calc_date`` is the
snapshot as-of date (defaults to today).

Env: FRED_API_KEY (required).
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import os
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
FREQUENCY_LIMITS: dict[str, int] = {
    "daily": 2520,
    "weekly": 520,
    "monthly": 120,
    "quarterly": 40,
}

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
    series_id: str
    dimension: str
    label: str
    frequency: str  # daily | weekly | monthly | quarterly
    invert: bool = False  # True = higher raw value means worse conditions
    units: str = "lin"  # FRED transform: lin, pch, pc1, ...


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
]


def _all_specs() -> list[SeriesSpec]:
    specs: list[SeriesSpec] = []
    for region_specs in REGION_SERIES.values():
        specs.extend(region_specs)
    specs.extend(GLOBAL_SERIES)
    existing = {s.series_id for s in specs}
    specs.extend(s for s in CREDIT_SERIES if s.series_id not in existing)
    existing |= {s.series_id for s in CREDIT_SERIES}
    specs.extend(s for s in RAW_INGEST_SERIES if s.series_id not in existing)
    return specs


def get_all_series_ids() -> list[str]:
    return [s.series_id for s in _all_specs()]


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
    params = {
        "series_id": spec.series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "asc",
        "limit": FREQUENCY_LIMITS.get(spec.frequency, 120),
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
        return parse_observations(resp.json())
    return []


def fetch_all_series(api_key: str, observation_start: str,
                     limit: int | None = None) -> dict[str, list[Obs]]:
    """Fetch every registry series concurrently (5 threads, shared bucket)."""
    import concurrent.futures

    import httpx

    specs = _all_specs()
    if limit:
        specs = specs[:limit]
    bucket = TokenBucket()
    out: dict[str, list[Obs]] = {}
    with httpx.Client(timeout=30.0) as client:
        def one(spec: SeriesSpec) -> tuple[str, list[Obs]]:
            return spec.series_id, _fetch_series(client, api_key, spec, observation_start, bucket)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            for sid, obs in pool.map(one, specs):
                out[sid] = obs
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Rows + derived series (pure)
# ──────────────────────────────────────────────────────────────────────────────
def obs_to_rows(raw: dict[str, list[Obs]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sid, obs_list in raw.items():
        for o in obs_list:
            if o.value is None:
                continue
            rows.append({
                "series_id": sid,
                "obs_date": _dt.date.fromisoformat(o.date),
                "value": o.value,
                "source": "fred",
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
        history, last_date = _extract_history(raw.get(spec.series_id, []))
        if len(history) == 0:
            freshness[spec.series_id] = DataFreshness(spec.series_id, None, None, 0.0, "stale")
            continue
        indicator_scores[spec.series_id] = percentile_rank_score(
            float(history[-1]), history, invert=spec.invert)
        f = compute_staleness_weight(last_date, as_of, spec.frequency, staleness_cfg)
        freshness[spec.series_id] = DataFreshness(
            spec.series_id, f.last_date, f.days_stale, f.weight, f.status)

    by_dim: dict[str, list[tuple[str, float, float]]] = {}
    for spec in specs:
        if spec.series_id not in indicator_scores:
            continue
        w = freshness[spec.series_id].weight
        if w <= 0:
            continue
        by_dim.setdefault(spec.dimension, []).append(
            (spec.series_id, indicator_scores[spec.series_id], w))

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
    invert = {s.series_id: s.invert for s in GLOBAL_SERIES}

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
        "snapshot_written": snapshot_written,
        "as_of": as_of.isoformat(),
    }
