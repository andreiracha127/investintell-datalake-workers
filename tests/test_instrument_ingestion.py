"""Tests for the instrument_ingestion worker (Tiingo NAV → nav_timeseries).

Pure-helper tests (stale-ticker selection, AUM prioritisation, row building)
run anywhere; the upsert/idempotency test uses a throwaway schema in the DB-mãe
(self-skips if unreachable). No network calls.
"""

from __future__ import annotations

import datetime as _dt
import math
import uuid

import psycopg
import pytest

from src.db import LOCK_INSTRUMENT_INGESTION, advisory_lock
from src.workers import instrument_ingestion as ii

MAE_DSN = "host=localhost port=5434 dbname=investintell_alloc user=investintell password=investintell"

AS_OF = _dt.date(2026, 6, 11)


def _mae():
    try:
        return psycopg.connect(MAE_DSN, connect_timeout=5)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"DB-mãe unreachable: {exc}")


def _inst(ticker: str, currency: str = "USD", aum: float | None = None):
    return {"instrument_id": uuid.uuid4(), "ticker": ticker,
            "currency": currency, "aum_usd": aum}


# ──────────────────────────────────────────────────────────────────────────────
# Stale-only, AUM-prioritised sweep (design §5.1a)
# ──────────────────────────────────────────────────────────────────────────────
def test_select_stale_tickers_skips_fresh_orders_by_aum_and_caps():
    a, b, c, d = (_inst("AAA", aum=1e9), _inst("BBB", aum=5e9),
                  _inst("CCC", aum=None), _inst("DDD", aum=2e9))
    universe = [a, b, c, d]
    watermarks = {
        "AAA": AS_OF - _dt.timedelta(days=10),   # stale
        "BBB": AS_OF - _dt.timedelta(days=30),   # stale, biggest AUM
        "DDD": AS_OF - _dt.timedelta(days=1),    # fresh → skipped
        # CCC: no history at all → stale (full backfill), no AUM → last
    }
    plan = ii.select_stale_tickers(universe, watermarks, AS_OF, cap=10)
    assert [p.ticker for p in plan] == ["BBB", "AAA", "CCC"]
    # Cap bounds the run (Tiingo ~130 req/h budget).
    assert [p.ticker for p in ii.select_stale_tickers(universe, watermarks, AS_OF, cap=1)] == ["BBB"]


def test_select_stale_tickers_dedups_shared_ticker_and_keeps_all_instruments():
    i1, i2 = _inst("SPY", "USD", 1e9), _inst("SPY", "EUR", 2e9)
    plan = ii.select_stale_tickers([i1, i2], {}, AS_OF, cap=10)
    assert len(plan) == 1
    p = plan[0]
    assert p.ticker == "SPY"
    assert len(p.instruments) == 2  # both instruments receive the fetched rows
    assert p.start_date == AS_OF - _dt.timedelta(days=ii.DEFAULT_LOOKBACK_DAYS)


def test_fetch_start_uses_watermark_minus_overlap():
    i1 = _inst("QQQ")
    wm = AS_OF - _dt.timedelta(days=9)
    plan = ii.select_stale_tickers([i1], {"QQQ": wm}, AS_OF, cap=10)
    assert plan[0].start_date == wm - _dt.timedelta(days=ii.WATERMARK_OVERLAP_DAYS)


# ──────────────────────────────────────────────────────────────────────────────
# Row building
# ──────────────────────────────────────────────────────────────────────────────
def test_build_rows_one_per_instrument_with_currency_and_log_returns():
    i1, i2 = _inst("SPY", "USD"), _inst("SPY", "EUR")
    series = [(_dt.date(2026, 6, 8), 100.0), (_dt.date(2026, 6, 9), 101.0),
              (_dt.date(2026, 6, 10), None)]
    rows = ii.build_rows(series, [(i1["instrument_id"], "USD"), (i2["instrument_id"], "EUR")])
    assert len(rows) == 4  # 2 valid bars × 2 instruments
    by_inst = [r for r in rows if r["instrument_id"] == i1["instrument_id"]]
    assert by_inst[0]["return_1d"] is None
    assert by_inst[1]["return_1d"] == pytest.approx(math.log(101.0 / 100.0), abs=1e-8)
    assert {r["currency"] for r in rows} == {"USD", "EUR"}
    assert all(r["return_type"] == "log" and r["source"] == "tiingo" for r in rows)


def test_build_rows_repairs_glitch_before_return():
    series = [
        (_dt.date(2020, 1, 1), 19.66),
        (_dt.date(2020, 1, 2), 0.02),    # glitch
        (_dt.date(2020, 1, 3), 19.68),
    ]
    rows = ii.build_rows(series, [("iid-1", "USD")])
    nav_by_date = {r["nav_date"]: r["nav"] for r in rows}
    assert 15.0 < nav_by_date[_dt.date(2020, 1, 2)] < 25.0   # repaired, not 0.02
    # no impossible log return remains
    rets = [r["return_1d"] for r in rows if r["return_1d"] is not None]
    assert all(abs(x) < 1.0 for x in rets)


# ──────────────────────────────────────────────────────────────────────────────
# Upsert / idempotency (throwaway schema in the DB-mãe)
# ──────────────────────────────────────────────────────────────────────────────
def test_upsert_nav_timeseries_idempotent():
    conn = _mae()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_nav CASCADE")
            cur.execute("CREATE SCHEMA _dlw_test_nav")
            cur.execute("SET search_path TO _dlw_test_nav")
            cur.execute(
                """CREATE TABLE nav_timeseries (
                       instrument_id uuid NOT NULL,
                       nav_date date NOT NULL,
                       nav numeric(18,6),
                       return_1d numeric(12,8),
                       aum_usd numeric(18,2),
                       currency varchar(3),
                       source varchar(30) DEFAULT 'tiingo',
                       return_type varchar(10) NOT NULL DEFAULT 'arithmetic',
                       PRIMARY KEY (instrument_id, nav_date))"""
            )
        iid = uuid.uuid4()
        rows = ii.build_rows(
            [(_dt.date(2026, 6, 8), 100.0), (_dt.date(2026, 6, 9), 101.0)],
            [(iid, "USD")],
        )
        n1 = ii.upsert_nav_timeseries(conn, rows)
        conn.commit()
        rows[1]["nav"] = 101.5  # revision on re-run
        n2 = ii.upsert_nav_timeseries(conn, rows)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), max(nav) FROM nav_timeseries")
            count, mx = cur.fetchone()
        assert n1 == n2 == 2
        assert count == 2 and float(mx) == pytest.approx(101.5)
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_nav CASCADE")
        conn.commit()
        conn.close()


def test_advisory_lock_is_distinct():
    assert LOCK_INSTRUMENT_INGESTION == 900_331
    conn = _mae()
    try:
        with advisory_lock(conn, LOCK_INSTRUMENT_INGESTION) as got:
            assert got is True
    finally:
        conn.close()
