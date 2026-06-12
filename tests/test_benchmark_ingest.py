"""Tests for the benchmark_ingest worker (Tiingo → benchmark_nav).

Pure-helper tests (bar parsing, NaN validation, block fan-out, log returns) run
anywhere; ticker-map and upsert/idempotency tests use a throwaway schema in the
DB-mãe (self-skip if unreachable). No network calls.
"""

from __future__ import annotations

import datetime as _dt

import psycopg
import pytest

from src.db import LOCK_BENCHMARK_INGEST, advisory_lock
from src.workers import benchmark_ingest as bi
from src.workers._tiingo import parse_price_bars

MAE_DSN = "host=localhost port=5434 dbname=investintell_alloc user=investintell password=investintell"


def _mae():
    try:
        return psycopg.connect(MAE_DSN, connect_timeout=5)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"DB-mãe unreachable: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Tiingo bar parsing (shared client helper)
# ──────────────────────────────────────────────────────────────────────────────
def test_parse_price_bars_prefers_adjclose_falls_back_to_close():
    bars = [
        {"date": "2026-06-05T00:00:00.000Z", "close": 100.0, "adjClose": 99.5},
        {"date": "2026-06-08T00:00:00.000Z", "close": 101.0},          # no adjClose
        {"date": "2026-06-09T00:00:00.000Z", "close": None, "adjClose": None},  # NaN bar
    ]
    parsed = parse_price_bars(bars)
    assert parsed == [
        (_dt.date(2026, 6, 5), 99.5),
        (_dt.date(2026, 6, 8), 101.0),
        (_dt.date(2026, 6, 9), None),
    ]


# ──────────────────────────────────────────────────────────────────────────────
# NaN-ratio validation (design: reject ticker when >5% NaN)
# ──────────────────────────────────────────────────────────────────────────────
def test_nan_ratio_validation():
    good = [(_dt.date(2026, 1, 1) + _dt.timedelta(days=i),
             100.0 if i % 30 else None) for i in range(100)]  # ~3% NaN
    bad = [(_dt.date(2026, 1, 1) + _dt.timedelta(days=i),
            100.0 if i % 10 else None) for i in range(100)]   # 10% NaN
    assert bi.nan_ratio_ok(good) is True
    assert bi.nan_ratio_ok(bad) is False
    assert bi.nan_ratio_ok([]) is False


# ──────────────────────────────────────────────────────────────────────────────
# Row building: one ticker fans out to its blocks; log return_1d
# ──────────────────────────────────────────────────────────────────────────────
def test_build_rows_fans_out_blocks_and_computes_log_returns():
    import math
    series = [
        (_dt.date(2026, 6, 5), 100.0),
        (_dt.date(2026, 6, 8), 102.0),
        (_dt.date(2026, 6, 9), None),   # NaN bar dropped
        (_dt.date(2026, 6, 10), 101.0),
    ]
    rows = bi.build_rows(series, ["na_equity_large", "9ec925d8"])
    assert len(rows) == 6  # 3 valid bars × 2 blocks
    first = [r for r in rows if r["block_id"] == "na_equity_large"]
    assert first[0]["return_1d"] is None
    # return_1d is rounded to 8 decimals (Numeric(12,8) in the table)
    assert first[1]["return_1d"] == pytest.approx(math.log(102.0 / 100.0), abs=1e-8)
    # return after a dropped NaN bar uses the previous *valid* close
    assert first[2]["return_1d"] == pytest.approx(math.log(101.0 / 102.0), abs=1e-8)
    assert all(r["return_type"] == "log" and r["source"] == "tiingo" for r in rows)


# ──────────────────────────────────────────────────────────────────────────────
# Ticker map: cloud allocation_blocks when present, embedded default otherwise
# ──────────────────────────────────────────────────────────────────────────────
def test_default_block_tickers_cover_known_blocks():
    m = bi.DEFAULT_BLOCK_TICKERS
    assert m["na_equity_large"] == "SPY"
    assert m["fi_us_treasury"] == "IEF"
    assert m["cash"] == "SHV"
    assert len(m) >= 30


def test_block_ticker_map_reads_table_and_falls_back():
    conn = _mae()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_bench CASCADE")
            cur.execute("CREATE SCHEMA _dlw_test_bench")
            cur.execute("SET search_path TO _dlw_test_bench")
        # Commit so block_ticker_map's internal rollback (failed SELECT) does
        # not undo the schema + search_path setup.
        conn.commit()
        # Table absent in this schema → embedded default map.
        assert bi.block_ticker_map(conn) == bi.DEFAULT_BLOCK_TICKERS
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE allocation_blocks (
                               block_id varchar(80) PRIMARY KEY,
                               benchmark_ticker varchar(20),
                               is_active boolean DEFAULT true)""")
            cur.execute("INSERT INTO allocation_blocks VALUES "
                        "('custom_block', 'VT', true), ('inactive', 'XXX', false), "
                        "('no_ticker', NULL, true)")
        assert bi.block_ticker_map(conn) == {"custom_block": "VT"}
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_bench CASCADE")
        conn.commit()
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Upsert / idempotency (throwaway schema in the DB-mãe)
# ──────────────────────────────────────────────────────────────────────────────
def test_upsert_benchmark_nav_idempotent():
    conn = _mae()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_bench2 CASCADE")
            cur.execute("CREATE SCHEMA _dlw_test_bench2")
            cur.execute("SET search_path TO _dlw_test_bench2")
            cur.execute(
                """CREATE TABLE benchmark_nav (
                       block_id varchar(80) NOT NULL,
                       nav_date date NOT NULL,
                       nav numeric(18,6) NOT NULL,
                       return_1d numeric(12,8),
                       return_type varchar(10) NOT NULL DEFAULT 'log',
                       source varchar(30) NOT NULL DEFAULT 'tiingo',
                       created_at timestamptz DEFAULT now(),
                       updated_at timestamptz DEFAULT now(),
                       PRIMARY KEY (block_id, nav_date))"""
            )
        rows = bi.build_rows(
            [(_dt.date(2026, 6, 5), 100.0), (_dt.date(2026, 6, 8), 102.0)],
            ["na_equity_large"],
        )
        n1 = bi.upsert_benchmark_nav(conn, rows)
        conn.commit()
        n2 = bi.upsert_benchmark_nav(conn, rows)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM benchmark_nav")
            count = cur.fetchone()[0]
        assert n1 == n2 == 2
        assert count == 2
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_bench2 CASCADE")
        conn.commit()
        conn.close()


def test_advisory_lock_is_distinct():
    assert LOCK_BENCHMARK_INGEST == 900_332
    conn = _mae()
    try:
        with advisory_lock(conn, LOCK_BENCHMARK_INGEST) as got:
            assert got is True
    finally:
        conn.close()
