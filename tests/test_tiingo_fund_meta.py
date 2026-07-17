"""Tests for the tiingo_fund_meta worker (Tiingo meta → tiingo_fund_meta).

Everything here runs with no network and no DB: the Tiingo HTTP call is mocked by
monkeypatching ``TiingoClient.fetch_meta`` (and, for the client-level test, its
transport), and the DB is a fake cursor/conn. One idempotent-upsert test uses a
throwaway schema in a local DB and self-skips if unreachable — matching the
eod_prices_warmer test convention.

Covered: universe-query composition, happy-path upsert, 404 → not_found,
skip-when-fresh, content-change detection, and the run() orchestration end to end
against fakes.
"""

from __future__ import annotations

import datetime as _dt

import psycopg
import pytest

from src.db import LOCK_TIINGO_FUND_META
from src.workers import tiingo_fund_meta as w
from src.workers._tiingo import TiingoClient

MAE_DSN = "host=localhost port=5434 dbname=investintell_alloc user=investintell password=investintell"


def _mae():
    try:
        return psycopg.connect(MAE_DSN, connect_timeout=5)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"local DB unreachable: {exc}")


_META_PAYLOAD = {
    "ticker": "SPY",
    "name": "SPDR S&P 500 ETF Trust",
    "description": "The Trust seeks to track the S&P 500 index.",
    "exchangeCode": "NYSE ARCA",
    "startDate": "1993-01-29",
    "endDate": "2026-07-16",
}


# ──────────────────────────────────────────────────────────────────────────────
# Universe SQL composition
# ──────────────────────────────────────────────────────────────────────────────
def test_universe_sql_unions_all_catalog_sources_distinct_and_nonblank():
    sql = w.universe_sql()
    for table in ("sec_fund_classes", "sec_etfs", "sec_registered_funds"):
        assert f"FROM {table} " in sql
    # one UNION between each of the three sources
    assert sql.count("UNION") == len(w.CATALOG_TICKER_SOURCES) - 1
    assert sql.count("SELECT DISTINCT upper(ticker)") == len(w.CATALOG_TICKER_SOURCES)
    assert "ticker IS NOT NULL" in sql
    assert "btrim(ticker) <> ''" in sql
    assert sql.rstrip().endswith("ORDER BY ticker")


def test_universe_sql_is_easily_extensible_via_sources():
    sql = w.universe_sql(("sec_fund_classes", "sec_etfs", "sec_registered_funds", "my_new_table"))
    assert "FROM my_new_table " in sql
    assert sql.count("UNION") == 3


def test_universe_sql_rejects_empty_sources():
    with pytest.raises(ValueError):
        w.universe_sql(())


# ──────────────────────────────────────────────────────────────────────────────
# Pure row-building + parsing
# ──────────────────────────────────────────────────────────────────────────────
def test_build_meta_row_happy_path_parses_dates_and_marks_ok():
    row = w.build_meta_row("SPY", _META_PAYLOAD)
    assert row == (
        "SPY",
        "SPDR S&P 500 ETF Trust",
        "The Trust seeks to track the S&P 500 index.",
        "NYSE ARCA",
        _dt.date(1993, 1, 29),
        _dt.date(2026, 7, 16),
        "ok",
    )


def test_build_meta_row_none_payload_is_not_found_with_nulls():
    row = w.build_meta_row("ZZZZ", None)
    assert row == ("ZZZZ", None, None, None, None, None, "not_found")


def test_build_meta_row_tolerates_missing_and_blank_dates():
    payload = {"name": "X", "description": "d", "exchangeCode": "NYSE",
               "startDate": "", "endDate": None}
    row = w.build_meta_row("X", payload)
    assert row[4] is None and row[5] is None
    assert row[6] == "ok"


def test_parse_date_rejects_junk():
    assert w._parse_date("not-a-date") is None
    assert w._parse_date(None) is None
    assert w._parse_date("2020-05-01T00:00:00Z") == _dt.date(2020, 5, 1)


# ──────────────────────────────────────────────────────────────────────────────
# Freshness + content-change gates
# ──────────────────────────────────────────────────────────────────────────────
def _now():
    return _dt.datetime(2026, 7, 17, tzinfo=_dt.timezone.utc)


def test_is_fresh_true_when_within_window_false_when_stale_or_missing():
    fresh_row = {"fetched_at": _now() - _dt.timedelta(days=5)}
    stale_row = {"fetched_at": _now() - _dt.timedelta(days=40)}
    assert w.is_fresh(fresh_row, _now(), 30) is True
    assert w.is_fresh(stale_row, _now(), 30) is False
    assert w.is_fresh(None, _now(), 30) is False
    assert w.is_fresh({"fetched_at": None}, _now(), 30) is False


def test_content_changed_true_for_new_and_differing_false_for_identical():
    row = w.build_meta_row("SPY", _META_PAYLOAD)
    existing = {
        "name": _META_PAYLOAD["name"],
        "description": _META_PAYLOAD["description"],
        "exchange_code": _META_PAYLOAD["exchangeCode"],
        "start_date": _dt.date(1993, 1, 29),
        "end_date": _dt.date(2026, 7, 16),
        "source_status": "ok",
    }
    assert w.content_changed(row, None) is True          # brand-new ticker
    assert w.content_changed(row, existing) is False     # byte-identical
    drifted = {**existing, "description": "changed prose"}
    assert w.content_changed(row, drifted) is True       # description drift
    end_moved = {**existing, "end_date": _dt.date(2026, 7, 1)}
    assert w.content_changed(row, end_moved) is True      # endDate advanced


# ──────────────────────────────────────────────────────────────────────────────
# Upsert SQL shape (DB-free contract check)
# ──────────────────────────────────────────────────────────────────────────────
def test_upsert_sql_targets_ticker_and_updates_content_columns():
    sql = " ".join(w.UPSERT_SQL.split())  # normalize alignment whitespace
    assert "INSERT INTO tiingo_fund_meta" in sql
    assert "ON CONFLICT (ticker) DO UPDATE" in sql
    for col in ("name", "description", "exchange_code", "start_date",
                "end_date", "source_status"):
        assert f"{col} = EXCLUDED.{col}" in sql
    # fetched_at is refreshed to now() on every upsert, never carried from EXCLUDED.
    assert "fetched_at = now()" in sql
    # ticker is the conflict key — never in the SET clause.
    assert "ticker = EXCLUDED" not in sql


def test_advisory_lock_id_is_distinct():
    assert LOCK_TIINGO_FUND_META == 900_336


# ──────────────────────────────────────────────────────────────────────────────
# Client-level: fetch_meta wiring against a fake transport (no network)
# ──────────────────────────────────────────────────────────────────────────────
class _FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def test_fetch_meta_returns_dict_on_200():
    client = TiingoClient(key="test")
    try:
        client._client.get = lambda *a, **k: _FakeResponse(200, _META_PAYLOAD)  # type: ignore[assignment]
        assert client.fetch_meta("SPY") == _META_PAYLOAD
    finally:
        client.close()


def test_fetch_meta_returns_none_on_404():
    client = TiingoClient(key="test")
    try:
        client._client.get = lambda *a, **k: _FakeResponse(404, {"detail": "Not found."})  # type: ignore[assignment]
        assert client.fetch_meta("ZZZZ") is None
    finally:
        client.close()


def test_fetch_meta_returns_none_on_non_object_body():
    client = TiingoClient(key="test")
    try:
        client._client.get = lambda *a, **k: _FakeResponse(200, ["unexpected", "list"])  # type: ignore[assignment]
        assert client.fetch_meta("SPY") is None
    finally:
        client.close()


# ──────────────────────────────────────────────────────────────────────────────
# run() orchestration end-to-end against fakes (mock HTTP + mock DB)
# ──────────────────────────────────────────────────────────────────────────────
class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._result: list = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        if s.startswith("SELECT DISTINCT upper(ticker)"):
            self._result = [(t,) for t in self._conn.universe]
        elif s.startswith("SELECT ticker, name, description"):
            self._result = list(self._conn.existing_rows)
        elif "INSERT INTO tiingo_fund_meta" in s and "ON CONFLICT" in s:
            self._conn.upserts.append(params)
            self._result = []
        else:  # CREATE TABLE / CREATE INDEX (ensure_schema)
            self._result = []

    def fetchall(self):
        return self._result


class _FakeConn:
    """Minimal psycopg-shaped conn: records upserts, serves canned reads."""

    def __init__(self, universe, existing_rows):
        self.universe = universe
        self.existing_rows = existing_rows
        self.upserts: list = []
        self.commits = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    # context-manager surface used by advisory_lock + `with connect(...) as conn`
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _wire_run(monkeypatch, conn, meta_by_ticker):
    """Patch connect/resolve_dsn/advisory_lock/TiingoClient for a run() call."""
    monkeypatch.setattr(w, "resolve_dsn", lambda dsn=None: "fake-dsn")
    monkeypatch.setattr(w, "connect", lambda dsn: conn)

    import contextlib

    @contextlib.contextmanager
    def _lock(_conn, _lock_id):
        yield True

    monkeypatch.setattr(w, "advisory_lock", _lock)

    class _FakeTiingo:
        def __init__(self, *a, **k):
            self.fetched: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def fetch_meta(self, ticker):
            self.fetched.append(ticker)
            return meta_by_ticker.get(ticker)

    monkeypatch.setattr(w, "TiingoClient", _FakeTiingo)


def test_run_happy_path_upserts_new_and_notfound(monkeypatch):
    conn = _FakeConn(universe=["SPY", "ZZZZ"], existing_rows=[])
    _wire_run(monkeypatch, conn, {"SPY": _META_PAYLOAD})  # ZZZZ → None (404)

    stats = w.run("ignored")

    assert stats["universe"] == 2
    assert stats["fetched"] == 2
    assert stats["upserted"] == 2
    assert stats["not_found"] == 1
    assert stats["changed"] == 2
    assert stats["skipped_fresh"] == 0
    # both tickers were upserted; SPY is 'ok', ZZZZ is 'not_found'
    by_ticker = {p[0]: p for p in conn.upserts}
    assert by_ticker["SPY"][-1] == "ok"
    assert by_ticker["ZZZZ"][-1] == "not_found"
    assert by_ticker["SPY"][4] == _dt.date(1993, 1, 29)  # start_date parsed


def test_run_skips_fresh_rows(monkeypatch):
    recent = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=2)
    existing = [("SPY", "SPDR S&P 500 ETF Trust",
                 _META_PAYLOAD["description"], "NYSE ARCA",
                 _dt.date(1993, 1, 29), _dt.date(2026, 7, 16), "ok", recent)]
    conn = _FakeConn(universe=["SPY"], existing_rows=existing)
    _wire_run(monkeypatch, conn, {"SPY": _META_PAYLOAD})

    stats = w.run("ignored", refresh_days=30)

    assert stats["skipped_fresh"] == 1
    assert stats["fetched"] == 0
    assert stats["upserted"] == 0
    assert conn.upserts == []


def test_run_refetches_stale_rows(monkeypatch):
    old = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=90)
    existing = [("SPY", "SPDR S&P 500 ETF Trust",
                 _META_PAYLOAD["description"], "NYSE ARCA",
                 _dt.date(1993, 1, 29), _dt.date(2026, 7, 16), "ok", old)]
    conn = _FakeConn(universe=["SPY"], existing_rows=existing)
    _wire_run(monkeypatch, conn, {"SPY": _META_PAYLOAD})

    stats = w.run("ignored", refresh_days=30)

    assert stats["skipped_fresh"] == 0
    assert stats["fetched"] == 1
    assert stats["upserted"] == 1
    # content identical → refreshed fetched_at but not counted as a content change
    assert stats["changed"] == 0


def test_run_returns_lock_busy_when_lock_unavailable(monkeypatch):
    conn = _FakeConn(universe=["SPY"], existing_rows=[])
    _wire_run(monkeypatch, conn, {"SPY": _META_PAYLOAD})

    import contextlib

    @contextlib.contextmanager
    def _busy(_conn, _lock_id):
        yield False

    monkeypatch.setattr(w, "advisory_lock", _busy)

    assert w.run("ignored") == {"skipped": "lock_busy"}


def test_run_limit_caps_universe(monkeypatch):
    conn = _FakeConn(universe=["AAA", "BBB", "CCC"], existing_rows=[])
    _wire_run(monkeypatch, conn, {"AAA": _META_PAYLOAD})

    stats = w.run("ignored", limit=1)

    assert stats["universe"] == 1
    assert stats["fetched"] == 1


# ──────────────────────────────────────────────────────────────────────────────
# Upsert idempotency (throwaway schema; self-skips without a local DB)
# ──────────────────────────────────────────────────────────────────────────────
def test_upsert_meta_idempotent_and_updates_in_place():
    conn = _mae()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_tfm CASCADE")
            cur.execute("CREATE SCHEMA _dlw_test_tfm")
            cur.execute("SET search_path TO _dlw_test_tfm")
            cur.execute(
                """CREATE TABLE tiingo_fund_meta (
                       ticker text PRIMARY KEY,
                       name text, description text, exchange_code text,
                       start_date date, end_date date,
                       fetched_at timestamptz NOT NULL DEFAULT now(),
                       source_status text)"""
            )
        conn.commit()
        row = w.build_meta_row("SPY", _META_PAYLOAD)
        w.upsert_meta(conn, row)
        w.upsert_meta(conn, row)  # second upsert must not duplicate
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM tiingo_fund_meta")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT description FROM tiingo_fund_meta WHERE ticker = 'SPY'")
            assert cur.fetchone()[0] == _META_PAYLOAD["description"]
        # update-in-place: changed description lands on the same row
        changed = w.build_meta_row("SPY", {**_META_PAYLOAD, "description": "new prose"})
        w.upsert_meta(conn, changed)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM tiingo_fund_meta")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT description FROM tiingo_fund_meta WHERE ticker = 'SPY'")
            assert cur.fetchone()[0] == "new prose"
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_tfm CASCADE")
        conn.commit()
        conn.close()
