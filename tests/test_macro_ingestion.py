"""Tests for the macro_ingestion worker (FRED → macro_data + macro_regional_snapshots).

Pure-helper tests (parsing, derived series, dedup, scoring) run anywhere.
The upsert/idempotency test runs against a throwaway schema in the DB-mãe
(localhost:5434) and self-skips if it is unreachable — same convention as
test_risk_metrics.py. No network calls anywhere in this file.
"""

from __future__ import annotations

import contextlib
import datetime as _dt

import psycopg
import pytest

from src.db import LOCK_MACRO_INGESTION, advisory_lock
from src.workers import macro_ingestion as mi

MAE_DSN = "host=localhost port=5434 dbname=investintell_alloc user=investintell password=investintell"


def _mae():
    try:
        return psycopg.connect(MAE_DSN, connect_timeout=5)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"DB-mãe unreachable: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Fake connection — enough psycopg3 surface for the pure-Python DB paths
# (upsert, snapshot write, SAVEPOINT via conn.transaction()). No server.
# ──────────────────────────────────────────────────────────────────────────────
class _FakeCursor:
    def __init__(self, conn: "_FakeConn"):
        self._conn = conn

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self._conn.executed.append((sql, params))

    def executemany(self, sql: str, rows) -> None:
        self._conn.rows.extend(rows)

    def fetchall(self) -> list:
        return []

    def fetchone(self):
        return None


class _FakeTransaction:
    """psycopg3 ``Connection.transaction()`` inside an open transaction is a
    SAVEPOINT: on exception it rolls back to the savepoint and re-raises,
    leaving the outer transaction usable."""

    def __init__(self, conn: "_FakeConn"):
        self._conn = conn
        self._mark = 0

    def __enter__(self) -> "_FakeTransaction":
        self._conn.savepoints += 1
        self._mark = len(self._conn.rows)
        return self

    def __exit__(self, exc_type, *_exc) -> bool:
        if exc_type is not None:
            del self._conn.rows[self._mark:]
            self._conn.savepoint_rollbacks += 1
        return False


class _FakeConn:
    def __init__(self) -> None:
        self.rows: list[tuple] = []
        self.executed: list[tuple] = []
        self.commits = 0
        self.savepoints = 0
        self.savepoint_rollbacks = 0
        self.rolled_back = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rolled_back = True

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *_exc) -> bool:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Parsing
# ──────────────────────────────────────────────────────────────────────────────
def test_parse_observations_filters_fred_missing_values():
    payload = {
        "observations": [
            {"date": "2026-06-01", "value": "4.33"},
            {"date": "2026-06-02", "value": "."},        # FRED missing marker
            {"date": "2026-06-03", "value": ""},
            {"date": "2026-06-04", "value": "NaN"},
            {"date": "2026-06-05", "value": "4.50"},
            {"date": "2026-06-06", "value": "garbage"},  # unparseable → dropped
        ]
    }
    obs = mi.parse_observations(payload)
    assert [(o.date, o.value) for o in obs] == [
        ("2026-06-01", 4.33),
        ("2026-06-05", 4.50),
    ]


def test_parse_observations_handles_fred_error_body():
    assert mi.parse_observations({"error_code": 400, "error_message": "bad"}) == []


# ──────────────────────────────────────────────────────────────────────────────
# Derived series
# ──────────────────────────────────────────────────────────────────────────────
def test_yield_curve_10y2y_only_on_common_dates():
    raw = {
        "DGS10": [mi.Obs("2026-06-01", 4.50), mi.Obs("2026-06-02", 4.60)],
        "DGS2": [mi.Obs("2026-06-01", 4.00)],  # no 06-02
    }
    derived = mi.compute_derived_series(raw)
    yc = [r for r in derived if r["series_id"] == "YIELD_CURVE_10Y2Y"]
    assert len(yc) == 1
    assert yc[0]["obs_date"] == _dt.date(2026, 6, 1)
    assert yc[0]["value"] == pytest.approx(0.5, abs=1e-9)
    assert yc[0]["source"] == "derived"
    assert yc[0]["is_derived"] is True


def test_cpi_yoy_needs_12m_prior():
    months = [
        mi.Obs(f"2025-{m:02d}-01", 300.0 + m) for m in range(1, 13)
    ] + [mi.Obs("2026-01-01", 313.0)]
    derived = mi.compute_derived_series({"CPIAUCSL": months})
    yoy = {r["obs_date"]: r["value"] for r in derived if r["series_id"] == "CPI_YOY"}
    # 2026-01 vs 2025-01: (313/301 - 1) * 100
    assert yoy[_dt.date(2026, 1, 1)] == pytest.approx((313.0 / 301.0 - 1) * 100, abs=1e-4)
    # 2025-01 has no 12m-prior point → not derived
    assert _dt.date(2025, 1, 1) not in yoy


# ──────────────────────────────────────────────────────────────────────────────
# Dedup
# ──────────────────────────────────────────────────────────────────────────────
def test_dedup_rows_by_pk_keeps_last():
    rows = [
        {"series_id": "DFF", "obs_date": _dt.date(2026, 6, 1), "value": 1.0},
        {"series_id": "DFF", "obs_date": _dt.date(2026, 6, 1), "value": 2.0},
        {"series_id": "DGS10", "obs_date": _dt.date(2026, 6, 1), "value": 3.0},
    ]
    out = mi.dedup_rows(rows)
    assert len(out) == 2
    assert [r["value"] for r in out if r["series_id"] == "DFF"] == [2.0]


# ──────────────────────────────────────────────────────────────────────────────
# Scoring (snapshot)
# ──────────────────────────────────────────────────────────────────────────────
def test_percentile_rank_score_neutral_below_min_history():
    import numpy as np
    assert mi.percentile_rank_score(5.0, np.arange(10, dtype=float)) == 50.0


def test_percentile_rank_score_invert_flips():
    import numpy as np
    hist = np.arange(100, dtype=float)
    hi = mi.percentile_rank_score(99.0, hist)
    hi_inv = mi.percentile_rank_score(99.0, hist, invert=True)
    assert hi == 100.0
    assert hi_inv == 0.0


def test_staleness_weight_decay():
    as_of = _dt.date(2026, 6, 11)
    cfg = mi._DEFAULT_CONFIG["staleness"]
    fresh = mi.compute_staleness_weight(as_of - _dt.timedelta(days=2), as_of, "daily", cfg)
    assert fresh.weight == 1.0 and fresh.status == "fresh"
    stale = mi.compute_staleness_weight(as_of - _dt.timedelta(days=30), as_of, "daily", cfg)
    assert stale.weight == 0.0 and stale.status == "stale"
    mid = mi.compute_staleness_weight(as_of - _dt.timedelta(days=7), as_of, "daily", cfg)
    assert 0.0 < mid.weight < 1.0 and mid.status == "decaying"


def test_snapshot_structure_v1():
    """Synthetic 70-point histories produce a well-formed version-1 snapshot."""
    as_of = _dt.date(2026, 6, 11)
    raw: dict[str, list[mi.Obs]] = {}
    start = as_of - _dt.timedelta(days=69)
    for spec in mi.REGION_SERIES["US"]:
        raw[spec.series_id] = [
            mi.Obs((start + _dt.timedelta(days=i)).isoformat(), float(i)) for i in range(70)
        ]
    snap = mi.build_regional_snapshot(raw, as_of=as_of)
    assert snap["version"] == 1
    assert snap["as_of_date"] == "2026-06-11"
    assert set(snap["regions"]) == {"US", "EUROPE", "ASIA", "EM"}
    us = snap["regions"]["US"]
    assert 0.0 <= us["composite_score"] <= 100.0
    assert us["coverage"] > 0.5
    assert "growth" in us["dimensions"]
    assert "DFF" in us["data_freshness"]
    # Regions with no data are neutral, fully covered structure intact.
    assert snap["regions"]["EM"]["composite_score"] == 50.0
    gi = snap["global_indicators"]
    assert set(gi) == {"geopolitical_risk_score", "energy_stress",
                       "commodity_stress", "usd_strength"}


def test_registry_covers_design_series():
    ids = mi.get_all_series_ids()
    assert len(ids) == len(set(ids))  # no duplicates
    for must in ("DFF", "DGS10", "DGS2", "CPIAUCSL", "VIXCLS", "BAMLH0A0HYM2", "NYXRSA"):
        assert must in ids
    assert len(ids) >= 90  # 35 regional + 11 global + ~46 credit


def test_open_macro_v4_inputs_are_ingested_raw_and_never_scored():
    """The four series the v4.0-rev regime engine reads out of ``macro_data``.

    Raw-only for the same reason T10YIE is: they are ingested for a downstream
    consumer, and scoring them would move the regional regime snapshot.

    The GDP assertion is the one that matters. ``A191RL1Q225SBEA`` (REAL GROWTH) is
    already in the US growth dimension; ``GDP`` is the NOMINAL LEVEL and is the
    denominator of deficit/GDP. Confusing them would rescale L1 silently, so both
    are asserted present and distinct.
    """
    raw_ids = [spec.series_id for spec in mi.RAW_INGEST_SERIES]
    for series_id in ("MTSDS133FMS", "GDP", "M2SL", "SUBLPDCILSLGNQ"):
        assert series_id in raw_ids
        assert series_id in mi.get_all_series_ids()

    by_id = {spec.series_id: spec for spec in mi.RAW_INGEST_SERIES}
    assert by_id["MTSDS133FMS"].frequency == "monthly"
    assert by_id["GDP"].frequency == "quarterly"
    assert by_id["M2SL"].frequency == "monthly"
    assert by_id["SUBLPDCILSLGNQ"].frequency == "quarterly"
    assert all(spec.frequency in mi.FREQUENCY_LIMITS for spec in mi.RAW_INGEST_SERIES)

    assert "A191RL1Q225SBEA" in mi.get_all_series_ids()
    assert "A191RL1Q225SBEA" not in raw_ids

    scored: set[str] = set()
    for region_specs in mi.REGION_SERIES.values():
        scored |= {spec.series_id for spec in region_specs}
    scored |= {spec.series_id for spec in mi.GLOBAL_SERIES}
    scored |= {spec.series_id for spec in mi.CREDIT_SERIES}
    assert not (set(raw_ids) & scored)


# ──────────────────────────────────────────────────────────────────────────────
# Upsert / idempotency (throwaway schema in the DB-mãe)
# ──────────────────────────────────────────────────────────────────────────────
def test_upsert_macro_data_idempotent():
    conn = _mae()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_macro CASCADE")
            cur.execute("CREATE SCHEMA _dlw_test_macro")
            cur.execute("SET search_path TO _dlw_test_macro")
            cur.execute(
                """CREATE TABLE macro_data (
                       series_id varchar(30) NOT NULL,
                       obs_date date NOT NULL,
                       value numeric(24,6) NOT NULL,
                       source varchar(30) DEFAULT 'fred',
                       is_derived boolean NOT NULL DEFAULT false,
                       created_at timestamptz NOT NULL DEFAULT now(),
                       updated_at timestamptz NOT NULL DEFAULT now(),
                       PRIMARY KEY (series_id, obs_date))"""
            )
        rows = [
            {"series_id": "DFF", "obs_date": _dt.date(2026, 6, 1), "value": 4.33,
             "source": "fred", "is_derived": False},
            {"series_id": "DFF", "obs_date": _dt.date(2026, 6, 2), "value": 4.33,
             "source": "fred", "is_derived": False},
        ]
        n1 = mi.upsert_macro_data(conn, rows)
        conn.commit()
        # Re-run with one revised value: still 2 rows total, value updated.
        rows[1]["value"] = 4.50
        n2 = mi.upsert_macro_data(conn, rows)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), max(value) FROM macro_data")
            count, mx = cur.fetchone()
        assert n1 == 2 and n2 == 2
        assert count == 2
        assert float(mx) == pytest.approx(4.50)
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_macro CASCADE")
        conn.commit()
        conn.close()


def test_advisory_lock_is_distinct():
    assert LOCK_MACRO_INGESTION == 900_320
    conn = _mae()
    try:
        with advisory_lock(conn, LOCK_MACRO_INGESTION) as got:
            assert got is True
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# FRED fetch window (sort_order/limit/observation_start semantics)
# ──────────────────────────────────────────────────────────────────────────────
class _FakeFredResponse:
    def __init__(self, payload: dict):
        self.status_code = 200
        self._payload = payload

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:  # pragma: no cover - status is always 200
        return None


class _FakeFredClient:
    """Simulates the FRED observations endpoint server-side semantics:
    filters by observation_start, sorts by sort_order, then applies limit.
    Missing-value rows ('.') are real rows and DO count toward limit."""

    def __init__(self, observations: list[tuple[str, str]]):
        self._observations = observations  # [(date, value-or-'.')]
        self.last_params: dict | None = None

    def get(self, url: str, params: dict) -> _FakeFredResponse:
        self.last_params = dict(params)
        rows = [o for o in self._observations if o[0] >= params["observation_start"]]
        count = len(rows)  # FRED reports total matching rows before limit
        rows.sort(key=lambda o: o[0], reverse=(params.get("sort_order") == "desc"))
        rows = rows[: int(params["limit"])]
        return _FakeFredResponse(
            {"count": count, "observations": [{"date": d, "value": v} for d, v in rows]}
        )


def _weekday_observations(start: _dt.date, end: _dt.date) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    day = start
    while day <= end:
        if day.weekday() < 5:
            # every 40th row is a '.' missing marker, as FRED emits for holidays
            value = "." if len(out) % 40 == 39 else f"{2.0 + (len(out) % 7) * 0.01:.2f}"
            out.append((day.isoformat(), value))
        day += _dt.timedelta(days=1)
    return out


def test_fetch_series_returns_newest_observations_when_window_exceeds_limit():
    """A full 10y daily window must round-trip with the newest observation
    present (regression: sort_order=asc + limit=2520 silently truncated the
    most recent months — T10YIE froze at 2026-02-27 in production)."""
    observations = _weekday_observations(_dt.date(2016, 1, 4), _dt.date(2026, 6, 30))
    client = _FakeFredClient(observations)
    spec = mi.SeriesSpec("T10YIE", "inflation_expectations", "10Y Breakeven Inflation", "daily")

    obs = mi._fetch_series(client, "test-key", spec, "2016-07-03", mi.TokenBucket())

    assert obs, "fetch returned nothing"
    newest_available = max(d for d, v in observations if v != ".")
    assert max(o.date for o in obs) == newest_available
    assert all(o1.date <= o2.date for o1, o2 in zip(obs, obs[1:])), "output must stay ascending"


def test_fetch_series_requests_newest_first_within_frequency_limit():
    """Locks the fetch contract: sort_order=desc (FRED applies limit server-side
    AFTER sorting, so newest-first survives truncation) with the per-frequency limit."""
    observations = _weekday_observations(_dt.date(2024, 1, 1), _dt.date(2026, 6, 30))
    client = _FakeFredClient(observations)
    spec = mi.SeriesSpec("DGS10", "monetary", "10Y Treasury", "daily")

    mi._fetch_series(client, "test-key", spec, "2024-01-01", mi.TokenBucket())

    assert client.last_params is not None
    assert client.last_params["sort_order"] == "desc"
    assert client.last_params["limit"] == mi.FREQUENCY_LIMITS["daily"]


def test_fetch_series_keeps_newest_under_forced_truncation(monkeypatch, capsys):
    """Even when the window exceeds the limit (forced small here), the newest
    observation must survive and a truncation warning must be emitted."""
    monkeypatch.setitem(mi.FREQUENCY_LIMITS, "daily", 100)
    observations = _weekday_observations(_dt.date(2025, 1, 1), _dt.date(2026, 6, 30))
    assert len(observations) > 100
    client = _FakeFredClient(observations)
    spec = mi.SeriesSpec("DFF", "monetary", "Fed Funds Rate", "daily", invert=True)

    obs = mi._fetch_series(client, "test-key", spec, "2025-01-01", mi.TokenBucket())

    newest_available = max(d for d, v in observations if v != ".")
    assert max(o.date for o in obs) == newest_available
    assert all(o1.date <= o2.date for o1, o2 in zip(obs, obs[1:]))
    assert "fred_window_truncated" in capsys.readouterr().out


def test_fetch_series_returns_all_rows_for_short_history_series():
    """Series younger than the window (BAML*/SOFR-like) must round-trip fully."""
    observations = _weekday_observations(_dt.date(2026, 1, 5), _dt.date(2026, 6, 30))
    client = _FakeFredClient(observations)
    spec = mi.SeriesSpec("SOFR", "monetary", "SOFR", "daily")

    obs = mi._fetch_series(client, "test-key", spec, "2016-07-03", mi.TokenBucket())

    expected = [d for d, v in observations if v != "."]
    assert [o.date for o in obs] == expected


def test_frequency_limits_cover_full_lookback_window_at_max_density():
    """Guards the sizing invariant: every frequency limit must fit LOOKBACK_YEARS
    of observations at the densest publication cadence (7-day daily like DFF,
    53 weeks/yr), so snapshot percentile scoring keeps its full history
    (regression: daily 2520 and weekly 520 truncated DFF/NFCI-class series)."""
    years = mi.LOOKBACK_YEARS
    assert mi.FREQUENCY_LIMITS["daily"] >= 366 * years + 10
    assert mi.FREQUENCY_LIMITS["weekly"] >= 53 * years + 5
    assert mi.FREQUENCY_LIMITS["monthly"] >= 12 * years + 3
    assert mi.FREQUENCY_LIMITS["quarterly"] >= 4 * years + 2


# ──────────────────────────────────────────────────────────────────────────────
# BIS offshore-dollar stock (SDMX-CSV parse, retry, upsert, fail-soft)
# ──────────────────────────────────────────────────────────────────────────────
# Verbatim excerpt of the real download (header + first two and last two rows) of
# https://stats.bis.org/api/v2/data/dataflow/BIS/WS_LBS_D_PUB/1.0/
# Q.S.C.A.USD.A.5J.A.5A.A.5J.N?format=csv
# Full response: 194 data rows, sha256
# 567098e78c1774f6eabca4af1d08fbe1bffc81b97348cf31b5af996b40b5d5b6
# (P1b provenance, 05_bis_provenance.json). Inlined rather than kept as a file:
# the repo .gitignore drops any directory named ``data/`` at any depth.
BIS_CSV_FIXTURE = (
    "FREQ,L_MEASURE,L_POSITION,L_INSTR,L_DENOM,L_CURR_TYPE,L_PARENT_CTY,"
    "L_REP_BANK_TYPE,L_REP_CTY,L_CP_SECTOR,L_CP_COUNTRY,L_POS_TYPE,DECIMALS,"
    "UNIT_MEASURE,UNIT_MULT,AVAILABILITY,TITLE_GRP,TIME_FORMAT,COLLECTION,"
    "ORG_VISIBILITY,TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK\n"
    "Q,S,C,A,USD,A,5J,A,5A,A,5J,N,3,USD,6,K,,,E,E,1977-Q4,379688.0,B,F,NaN\n"
    "Q,S,C,A,USD,A,5J,A,5A,A,5J,N,3,USD,6,K,,,E,E,1978-Q1,392687.0,A,F,\n"
    "Q,S,C,A,USD,A,5J,A,5A,A,5J,N,3,USD,6,K,,,E,E,2025-Q4,20837989.85,B,F,20838302.593\n"
    "Q,S,C,A,USD,A,5J,A,5A,A,5J,N,3,USD,6,K,,,E,E,2026-Q1,21796862.573,B,F,21794144.133\n"
)

BIS_SPEC = mi.BIS_SERIES[0]


class _FakeBisResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class _FakeBisClient:
    """Replays a scripted sequence of responses/exceptions, one per attempt."""

    def __init__(self, script: list):
        self._script = list(script)
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict) -> _FakeBisResponse:
        self.calls.append((url, dict(params)))
        item = self._script[min(len(self.calls) - 1, len(self._script) - 1)]
        if isinstance(item, Exception):
            raise item
        return item


def test_bis_registry_pins_the_preregistered_series():
    """The instrument is fixed by the P1b pre-registration, not by taste."""
    assert BIS_SPEC.storage_id == "BIS_LBS_XB_CLAIMS_USD"
    assert BIS_SPEC.dataflow == "WS_LBS_D_PUB"
    assert BIS_SPEC.version == "1.0"
    assert BIS_SPEC.sdmx_key == "Q.S.C.A.USD.A.5J.A.5A.A.5J.N"
    assert mi.bis_series_url(BIS_SPEC) == (
        "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_LBS_D_PUB/1.0/"
        "Q.S.C.A.USD.A.5J.A.5A.A.5J.N"
    )


def test_parse_bis_csv_stamps_quarter_start_and_converts_to_billions():
    obs = mi.parse_bis_csv(BIS_CSV_FIXTURE)
    assert [o.date for o in obs] == [
        "1977-10-01", "1978-01-01", "2025-10-01", "2026-01-01"]
    # USD millions / 1000 → $B. 379688.0 → 379.688
    assert obs[0].value == pytest.approx(379.688, abs=1e-9)
    assert obs[-1].value == pytest.approx(21796.862573, abs=1e-9)


def test_parse_bis_csv_rejects_a_silent_rescale():
    """UNIT_MULT is the whole ballgame: if the BIS ever republished this cut in
    units or thousands, storing it unchecked would move the gauge by 1000x."""
    tampered = BIS_CSV_FIXTURE.replace(",USD,6,K,", ",USD,3,K,")
    with pytest.raises(ValueError, match="unexpected BIS units"):
        mi.parse_bis_csv(tampered)


def test_parse_bis_csv_rejects_unknown_shape():
    with pytest.raises(ValueError, match="missing columns"):
        mi.parse_bis_csv("A,B\n1,2\n")


def test_parse_bis_csv_drops_missing_observations():
    rows = BIS_CSV_FIXTURE.splitlines()
    rows[1] = rows[1].replace(",1977-Q4,379688.0,", ",1977-Q4,NaN,")
    rows[2] = rows[2].replace(",1978-Q1,392687.0,", ",1978-Q1,,")
    obs = mi.parse_bis_csv("\n".join(rows) + "\n")
    assert [o.date for o in obs] == ["2025-10-01", "2026-01-01"]


def test_bis_period_parser_rejects_non_quarterly():
    for bad in ("2026-M01", "2026", "", None, "2026-Q5"):
        with pytest.raises(ValueError):
            mi._bis_period_to_date(bad)


def test_fetch_bis_series_retries_transient_failures_then_succeeds(monkeypatch):
    monkeypatch.setattr(mi.time, "sleep", lambda _s: None)
    client = _FakeBisClient([
        _FakeBisResponse(503),
        RuntimeError("connection reset"),
        _FakeBisResponse(200, BIS_CSV_FIXTURE),
    ])
    obs = mi.fetch_bis_series(BIS_SPEC, client=client)
    assert len(client.calls) == 3
    assert client.calls[0][1] == {"format": "csv"}
    assert len(obs) == 4


def test_fetch_bis_series_gives_up_after_three_attempts(monkeypatch):
    monkeypatch.setattr(mi.time, "sleep", lambda _s: None)
    client = _FakeBisClient([_FakeBisResponse(503)])
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        mi.fetch_bis_series(BIS_SPEC, client=client)
    assert len(client.calls) == mi.BIS_MAX_ATTEMPTS


def test_fetch_bis_series_does_not_retry_a_bad_key():
    """404/400 is a wrong SDMX key, not weather — retrying just wastes time."""
    client = _FakeBisClient([_FakeBisResponse(404)])
    with pytest.raises(RuntimeError, match="BIS HTTP 404"):
        mi.fetch_bis_series(BIS_SPEC, client=client)
    assert len(client.calls) == 1


def test_fetch_bis_series_rejects_an_sdmx_error_document():
    client = _FakeBisClient([_FakeBisResponse(200, '<?xml version="1.0"?><err/>')])
    with pytest.raises(RuntimeError, match="error document"):
        mi.fetch_bis_series(BIS_SPEC, client=client)


def test_ingest_bis_upserts_billions_under_the_bis_source():
    conn = _FakeConn()
    client = _FakeBisClient([_FakeBisResponse(200, BIS_CSV_FIXTURE)])
    rows, error = mi.ingest_bis(conn, client=client)
    assert (rows, error) == (4, None)
    assert conn.savepoints == 1
    stored = {r[1]: r for r in conn.rows}
    assert set(stored) == {
        _dt.date(1977, 10, 1), _dt.date(1978, 1, 1),
        _dt.date(2025, 10, 1), _dt.date(2026, 1, 1)}
    first = stored[_dt.date(1977, 10, 1)]
    assert first[0] == "BIS_LBS_XB_CLAIMS_USD"
    assert first[2] == pytest.approx(379.688, abs=1e-9)
    assert first[3] == "bis"
    assert first[4] is False


def test_ingest_bis_is_fail_soft_and_leaves_the_transaction_usable(monkeypatch):
    monkeypatch.setattr(mi.time, "sleep", lambda _s: None)
    conn = _FakeConn()
    client = _FakeBisClient([RuntimeError("stats.bis.org unreachable")])
    rows, error = mi.ingest_bis(conn, client=client)
    assert rows == 0
    assert "stats.bis.org unreachable" in error
    assert conn.rows == []          # nothing written
    assert conn.savepoints == 0     # never even opened the savepoint
    assert conn.rolled_back is False


def test_ingest_bis_rolls_back_to_savepoint_when_the_upsert_fails(monkeypatch):
    """A half-applied BIS upsert must not poison the caller's transaction — the
    FRED rows written before it are what would be lost."""
    conn = _FakeConn()
    fred_row = ("DFF", _dt.date(2026, 6, 1), 4.33, "fred", False)
    conn.rows.append(fred_row)

    def _half_applied_then_boom(target_conn, rows):
        target_conn.rows.append(rows[0])
        raise RuntimeError("boom")

    monkeypatch.setattr(mi, "upsert_macro_data", _half_applied_then_boom)
    client = _FakeBisClient([_FakeBisResponse(200, BIS_CSV_FIXTURE)])
    rows, error = mi.ingest_bis(conn, client=client)
    assert rows == 0 and "boom" in error
    assert conn.savepoints == 1 and conn.savepoint_rollbacks == 1
    assert conn.rows == [fred_row]  # the partial BIS write is gone, FRED survives


def test_ingest_bis_treats_an_empty_download_as_a_failure():
    conn = _FakeConn()
    header = BIS_CSV_FIXTURE.splitlines()[0] + "\n"
    client = _FakeBisClient([_FakeBisResponse(200, header)])
    rows, error = mi.ingest_bis(conn, client=client)
    assert rows == 0
    assert "no observations" in error


# ──────────────────────────────────────────────────────────────────────────────
# run() wiring — stats dict and the FRED-survives-a-BIS-outage contract
# ──────────────────────────────────────────────────────────────────────────────
@contextlib.contextmanager
def _lock_granted(_conn, _lock_id):
    yield True


def _patch_run_environment(monkeypatch, conn, *, bis) -> None:
    monkeypatch.setenv("FRED_API_KEY", "test-key")
    monkeypatch.setattr(mi, "connect", lambda _dsn: conn)
    monkeypatch.setattr(mi, "advisory_lock", _lock_granted)
    monkeypatch.setattr(mi, "fetch_all_series",
                        lambda *_a, **_k: {"DFF": [mi.Obs("2026-06-01", 4.33)]})
    monkeypatch.setattr(mi, "fetch_bis_series", bis)


def test_run_reports_the_bis_leg_in_the_stats_dict(monkeypatch):
    conn = _FakeConn()
    _patch_run_environment(monkeypatch, conn,
                           bis=lambda _spec, **_k: mi.parse_bis_csv(BIS_CSV_FIXTURE))
    stats = mi.run("dsn://ignored", calc_date="2026-06-11")
    assert stats["bis_rows"] == 4
    assert stats["bis_error"] is None
    assert stats["upserted"] == 1
    assert {r[3] for r in conn.rows} == {"fred", "bis"}
    assert conn.commits == 1


def test_run_survives_a_bis_outage_without_losing_the_fred_leg(monkeypatch):
    conn = _FakeConn()

    def _down(_spec, **_k):
        raise RuntimeError("stats.bis.org unreachable")

    _patch_run_environment(monkeypatch, conn, bis=_down)
    stats = mi.run("dsn://ignored", calc_date="2026-06-11")
    assert stats["bis_rows"] == 0
    assert "stats.bis.org unreachable" in stats["bis_error"]
    assert stats["upserted"] == 1
    assert stats["snapshot_written"] is True
    assert {r[3] for r in conn.rows} == {"fred"}
    assert conn.commits == 1
