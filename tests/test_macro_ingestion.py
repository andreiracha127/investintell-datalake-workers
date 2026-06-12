"""Tests for the macro_ingestion worker (FRED → macro_data + macro_regional_snapshots).

Pure-helper tests (parsing, derived series, dedup, scoring) run anywhere.
The upsert/idempotency test runs against a throwaway schema in the DB-mãe
(localhost:5434) and self-skips if it is unreachable — same convention as
test_risk_metrics.py. No network calls anywhere in this file.
"""

from __future__ import annotations

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
