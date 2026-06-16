"""Tests for the eod_prices_warmer worker (Tiingo → eod_prices).

Pure-helper tests (full-bar → row mapping, universe union, watermark filtering,
upsert SQL shape) run anywhere with no network and no DB. The idempotent-upsert
test uses a throwaway schema in a local DB and self-skips if unreachable.

This worker keeps the Investintell-Light API's ``eod_prices`` universe fresh so
the API can serve /stocks/* DB-first (Strategy B) without a synchronous Tiingo
fetch for stale tickers on the request path.
"""

from __future__ import annotations

import datetime as _dt

import psycopg
import pytest

from src.db import LOCK_EOD_PRICES_WARMER, advisory_lock
from src.workers import eod_prices_warmer as w
from src.workers._tiingo import TiingoClient, parse_price_bars

MAE_DSN = "host=localhost port=5434 dbname=investintell_alloc user=investintell password=investintell"


def _mae():
    try:
        return psycopg.connect(MAE_DSN, connect_timeout=5)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"local DB unreachable: {exc}")


_FULL_BAR = {
    "date": "2026-06-15T00:00:00.000Z",
    "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "volume": 1000,
    "adjOpen": 9.9, "adjHigh": 10.9, "adjLow": 9.4, "adjClose": 10.4,
    "adjVolume": 1000, "divCash": 0.0, "splitFactor": 1.0,
}


# ──────────────────────────────────────────────────────────────────────────────
# Full-bar → eod_prices row mapping
# ──────────────────────────────────────────────────────────────────────────────
def test_build_eod_rows_maps_all_fourteen_columns():
    rows = w.build_eod_rows("AAPL", [_FULL_BAR])
    assert rows == [
        ("AAPL", _dt.date(2026, 6, 15), 10.0, 11.0, 9.5, 10.5, 1000,
         9.9, 10.9, 9.4, 10.4, 1000, 0.0, 1.0)
    ]


def test_build_eod_rows_skips_bar_missing_any_required_field():
    """eod_prices columns are all NOT NULL → a bar missing/None on any field is dropped."""
    no_adjclose = {k: v for k, v in _FULL_BAR.items() if k != "adjClose"}
    null_volume = {**_FULL_BAR, "date": "2026-06-16T00:00:00.000Z", "volume": None}
    rows = w.build_eod_rows("AAPL", [_FULL_BAR, no_adjclose, null_volume])
    # only the complete bar survives
    assert [r[1] for r in rows] == [_dt.date(2026, 6, 15)]


def test_build_eod_rows_empty():
    assert w.build_eod_rows("AAPL", []) == []


# ──────────────────────────────────────────────────────────────────────────────
# Upsert SQL shape (DB-free contract check)
# ──────────────────────────────────────────────────────────────────────────────
def test_eod_upsert_sql_targets_ticker_date_and_updates_price_columns():
    sql = w.EOD_UPSERT_SQL
    assert "INSERT INTO eod_prices" in sql
    assert "ON CONFLICT (ticker, date) DO UPDATE" in sql
    for col in (
        "open", "high", "low", "close", "volume",
        "adj_open", "adj_high", "adj_low", "adj_close", "adj_volume",
        "div_cash", "split_factor",
    ):
        assert f"{col} = EXCLUDED.{col}" in sql
    # ticker/date are the conflict key — never in the SET clause.
    assert "ticker = EXCLUDED" not in sql
    assert "date = EXCLUDED" not in sql


# ──────────────────────────────────────────────────────────────────────────────
# Universe + watermarks (fake cursor — no DB)
# ──────────────────────────────────────────────────────────────────────────────
class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._sql = sql

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _FakeCursor(self._rows)


def test_warming_universe_unions_index_tickers_dedups_and_sorts():
    conn = _FakeConn([("MSFT",), ("AAPL",), ("SPY",)])
    universe = w.warming_universe(conn)
    # SPY (an index ticker) already present is not duplicated; QQQ/DIA/IWM added.
    assert universe == sorted(set(["MSFT", "AAPL", "SPY", *w.INDEX_TICKERS]))
    assert universe == sorted(universe)
    assert len(universe) == len(set(universe))


def test_ticker_watermarks_drops_null_max_date():
    conn = _FakeConn([("AAPL", _dt.date(2026, 6, 15)), ("ZZZ", None)])
    marks = w._ticker_watermarks(conn)
    assert marks == {"AAPL": _dt.date(2026, 6, 15)}


# ──────────────────────────────────────────────────────────────────────────────
# Tiingo client: raw full-bar fetch wiring (no network — monkeypatched _get_bars)
# ──────────────────────────────────────────────────────────────────────────────
def test_fetch_daily_bars_returns_raw_and_prices_stay_parsed():
    client = TiingoClient(key="test")
    try:
        sample = [_FULL_BAR]
        client._get_bars = lambda *a, **k: sample  # type: ignore[method-assign]
        # raw full bars for the warmer
        assert client.fetch_daily_bars("AAPL", _dt.date(2026, 6, 1)) is sample
        # NAV path unchanged: still date+adjClose tuples
        assert client.fetch_daily_prices("AAPL", _dt.date(2026, 6, 1)) == parse_price_bars(sample)
    finally:
        client.close()


# ──────────────────────────────────────────────────────────────────────────────
# Advisory lock id
# ──────────────────────────────────────────────────────────────────────────────
def test_advisory_lock_id_is_distinct():
    assert LOCK_EOD_PRICES_WARMER == 900_335


# ──────────────────────────────────────────────────────────────────────────────
# Upsert idempotency (throwaway schema; self-skips without a local DB)
# ──────────────────────────────────────────────────────────────────────────────
def test_upsert_eod_prices_idempotent():
    conn = _mae()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_eodw CASCADE")
            cur.execute("CREATE SCHEMA _dlw_test_eodw")
            cur.execute("SET search_path TO _dlw_test_eodw")
            cur.execute(
                """CREATE TABLE eod_prices (
                       ticker varchar(20) NOT NULL,
                       date date NOT NULL,
                       open double precision NOT NULL,
                       high double precision NOT NULL,
                       low double precision NOT NULL,
                       close double precision NOT NULL,
                       volume bigint NOT NULL,
                       adj_open double precision NOT NULL,
                       adj_high double precision NOT NULL,
                       adj_low double precision NOT NULL,
                       adj_close double precision NOT NULL,
                       adj_volume bigint NOT NULL,
                       div_cash double precision NOT NULL,
                       split_factor double precision NOT NULL,
                       PRIMARY KEY (ticker, date))"""
            )
        rows = w.build_eod_rows("AAPL", [_FULL_BAR])
        n1 = w.upsert_eod_prices(conn, rows)
        conn.commit()
        n2 = w.upsert_eod_prices(conn, rows)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM eod_prices")
            count = cur.fetchone()[0]
        assert n1 == n2 == 1
        assert count == 1
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_eodw CASCADE")
        conn.commit()
        conn.close()
