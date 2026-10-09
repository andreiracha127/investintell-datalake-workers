"""Tests for the tiingo_fund_meta worker (Tiingo meta → tiingo_fund_meta).

Everything here runs with no network and no DB: the Tiingo HTTP call is mocked by
monkeypatching ``TiingoClient.fetch_meta`` (and, for the client-level test, its
transport), and the DB is a fake cursor/conn. One idempotent-upsert test uses a
throwaway schema in a local DB and self-skips if unreachable — matching the
eod_prices_warmer test convention.

Covered: universe-query composition, happy-path upsert, 404 → not_found,
skip-when-fresh, content-change detection, and the run() orchestration end to end
against fakes. Coverage of priced funds outside the catalog: Tiingo's
supported_tickers listing typed by each ticker's current listing, a priced ETF in
no catalog (AGG) selected, stocks and unlisted tickers excluded, catalog rows still
refreshed, a capped backfill resuming across runs, and requests paced by the
shared token bucket.
"""

from __future__ import annotations

import csv
import datetime as _dt
import io
import zipfile

import psycopg
import pytest

from src.db import LOCK_TIINGO_FUND_META
from src.workers import _tiingo
from src.workers import tiingo_fund_meta as w
from src.workers._tiingo import (
    TIINGO_MAX_REQUESTS_PER_HOUR,
    TiingoClient,
    TokenBucket,
    parse_supported_tickers,
)

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
        elif s == " ".join(w.PRICED_TICKERS_SQL.split()):
            self._conn.priced_reads += 1
            self._result = [(t,) for t in self._conn.priced]
        elif s.startswith("SELECT ticker, name, description"):
            self._result = list(self._conn.existing_rows)
        elif "INSERT INTO tiingo_fund_meta" in s and "ON CONFLICT" in s:
            self._conn.upserts.append(params)
            # Apply it, so a second run() on this conn sees the fresh row.
            self._conn.existing_rows = [
                r for r in self._conn.existing_rows if r[0] != params[0]
            ] + [(*params, _dt.datetime.now(_dt.timezone.utc))]
            self._result = []
        else:  # CREATE TABLE / CREATE INDEX (ensure_schema)
            self._result = []

    def fetchall(self):
        return self._result


class _FakeConn:
    """Minimal psycopg-shaped conn: applies upserts, serves canned reads.

    ``universe`` is the catalog query's answer, ``priced`` the eod_prices ∪
    universe_constituents union's."""

    def __init__(self, universe, existing_rows, priced=()):
        self.universe = universe
        self.existing_rows = existing_rows
        self.priced = list(priced)
        self.priced_reads = 0
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


def _wire_run(monkeypatch, conn, meta_by_ticker, asset_types=None):
    """Patch connect/resolve_dsn/advisory_lock/TiingoClient for a run() call.

    ``asset_types`` is what Tiingo's supported_tickers listing says (default:
    an empty listing, i.e. no priced extension); an Exception instance makes the
    listing fetch raise it. Returns the list of fetch_meta tickers, in order."""
    monkeypatch.setattr(w, "resolve_dsn", lambda dsn=None: "fake-dsn")
    monkeypatch.setattr(w, "connect", lambda dsn: conn)

    import contextlib

    @contextlib.contextmanager
    def _lock(_conn, _lock_id):
        yield True

    monkeypatch.setattr(w, "advisory_lock", _lock)
    fetched: list[str] = []

    class _FakeTiingo:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def fetch_supported_asset_types(self):
            if isinstance(asset_types, Exception):
                raise asset_types
            return dict(asset_types or {})

        def fetch_meta(self, ticker):
            fetched.append(ticker)
            return meta_by_ticker.get(ticker)

    monkeypatch.setattr(w, "TiingoClient", _FakeTiingo)
    return fetched


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


def test_run_limit_caps_fetches_not_universe(monkeypatch):
    conn = _FakeConn(universe=["AAA", "BBB", "CCC"], existing_rows=[])
    _wire_run(monkeypatch, conn, {"AAA": _META_PAYLOAD})

    stats = w.run("ignored", limit=1)

    assert stats["universe"] == 3
    assert stats["due"] == 3
    assert stats["fetched"] == 1
    assert stats["deferred"] == 2


# ──────────────────────────────────────────────────────────────────────────────
# Tiingo's own asset type: supported_tickers.zip → current listing per ticker
# ──────────────────────────────────────────────────────────────────────────────
_SUPPORTED_HEADER = ("ticker", "exchange", "assetType", "priceCurrency", "startDate", "endDate")


def _supported_zip(rows, header=_SUPPORTED_HEADER) -> bytes:
    text = io.StringIO()
    writer = csv.writer(text, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr("supported_tickers.csv", text.getvalue())
    return out.getvalue()


def test_parse_supported_tickers_types_each_ticker_by_its_current_listing():
    content = _supported_zip([
        ("AGG", "NYSE", "ETF", "USD", "2003-09-26", "2026-10-08"),
        ("AAPL", "NASDAQ", "Stock", "USD", "1980-12-12", "2026-10-08"),
        ("VFIAX", "NMFQS", "Mutual Fund", "USD", "2000-11-13", "2026-10-07"),
        # Reused: a stock that delisted, then an ETF on the same ticker (open end).
        ("CA", "NASDAQ", "ETF", "USD", "2023-12-14", ""),
        ("CA", "NASDAQ", "Stock", "USD", "1984-09-07", "2018-11-06"),
        # Reused the other way: the ETF ended, the stock is the current security.
        ("GHI", "NYSE", "ETF", "USD", "1994-11-22", "2022-12-27"),
        ("GHI", "NYSE", "Stock", "USD", "1986-04-02", "2026-10-08"),
        ("spy", "NYSE", "ETF", "USD", "1993-01-29", "2026-10-08"),
    ])

    types = parse_supported_tickers(content)

    assert types == {
        "AGG": "ETF", "AAPL": "Stock", "VFIAX": "Mutual Fund",
        "CA": "ETF", "GHI": "Stock", "SPY": "ETF",
    }


def test_parse_supported_tickers_never_lets_an_undated_reservation_outrank_a_listing():
    content = _supported_zip([
        # A reservation for a future ETF on a ticker whose priced listing is a stock.
        ("OLDS", "NYSE", "ETF", "USD", "", ""),
        ("OLDS", "NYSE", "Stock", "USD", "1990-01-02", "2015-06-30"),
        # The reverse, with the reservation first and an open-ended ETF listing:
        # an empty endDate is open only on a row that has a startDate.
        ("OPEN", "NASDAQ", "Mutual Fund", "USD", "", ""),
        ("OPEN", "NASDAQ", "ETF", "USD", "2020-03-02", ""),
        # An endDate without a startDate is still not a dated listing.
        ("HALF", "NYSE", "ETF", "USD", "", "2026-10-08"),
        ("HALF", "NYSE", "Stock", "USD", "2001-05-01", "2019-12-31"),
        # Only a reservation: the ticker takes the reservation's type.
        ("NEWF", "NYSE", "ETF", "USD", "", ""),
    ])

    types = parse_supported_tickers(content)

    assert types == {"OLDS": "Stock", "OPEN": "ETF", "HALF": "Stock", "NEWF": "ETF"}


def test_parse_supported_tickers_rejects_an_unusable_file():
    with pytest.raises(ValueError, match="missing columns"):
        parse_supported_tickers(_supported_zip([("AGG", "ETF")], header=("ticker", "type")))
    with pytest.raises(ValueError, match="no rows"):
        parse_supported_tickers(_supported_zip([]))


def test_priced_tickers_sql_reads_eod_prices_and_universe_constituents():
    sql = " ".join(w.PRICED_TICKERS_SQL.split())
    assert "FROM eod_prices" in sql
    assert "FROM universe_constituents" in sql
    assert "UNION" in sql
    assert "status" not in sql  # every screener row, not only the active ones


def test_select_extension_keeps_etfs_and_funds_and_drops_stocks_and_unlisted():
    types = {"AGG": "ETF", "VFIAX": "Mutual Fund", "AAPL": "Stock", "GHI": "Stock"}
    priced = ["AGG", "agg", "VFIAX", "AAPL", "GHI", "ABR-PD", None, " "]

    assert w.select_extension(priced, types) == ["AGG", "VFIAX"]


def test_due_tickers_puts_missing_first_then_stalest_and_skips_fresh():
    now = _now()
    existing = {
        "FRESH": {"fetched_at": now - _dt.timedelta(days=1)},
        "STALE": {"fetched_at": now - _dt.timedelta(days=40)},
        "STALER": {"fetched_at": now - _dt.timedelta(days=90)},
        "NEVER": {"fetched_at": None},
    }
    universe = ["FRESH", "STALE", "STALER", "NEVER", "NEW_B", "NEW_A"]

    assert w.due_tickers(universe, existing, now, 30) == [
        "NEW_A", "NEW_B", "NEVER", "STALER", "STALE",
    ]


# ──────────────────────────────────────────────────────────────────────────────
# run(): priced ETFs/funds outside the catalog, stocks out, catalog unchanged
# ──────────────────────────────────────────────────────────────────────────────
def _stale_row(ticker, days_old=90):
    return (ticker, "n", "d", "NYSE", _dt.date(2000, 1, 3), None, "ok",
            _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=days_old))


def test_run_adds_a_priced_etf_missing_from_the_catalog_and_excludes_stocks(monkeypatch):
    # AGG: priced in eod_prices, resolved as a fund by Light, in no SEC catalog.
    conn = _FakeConn(universe=["VFIAX"], existing_rows=[],
                     priced=["AGG", "AAPL", "GHI", "ABR-PD", "VTSAX", "VFIAX"])
    fetched = _wire_run(
        monkeypatch, conn,
        {"AGG": {**_META_PAYLOAD, "ticker": "AGG"}, "VTSAX": _META_PAYLOAD,
         "VFIAX": _META_PAYLOAD},
        asset_types={"AGG": "ETF", "VTSAX": "Mutual Fund", "VFIAX": "Mutual Fund",
                     "AAPL": "Stock", "GHI": "Stock"},
    )

    stats = w.run("ignored")

    assert sorted(fetched) == ["AGG", "VFIAX", "VTSAX"]
    assert {p[0] for p in conn.upserts} == {"AGG", "VFIAX", "VTSAX"}
    assert stats["catalog"] == 1
    assert stats["extension"] == 2          # AGG + VTSAX; VFIAX is already catalog
    assert stats["universe"] == 3
    assert "aborted" not in stats


def test_run_still_refreshes_catalog_rows_whatever_tiingo_lists(monkeypatch):
    # Catalog tickers bypass the type filter: a stale one refreshes even when the
    # listing calls it a stock or omits it; a fresh one is still skipped.
    fresh = _stale_row("FRESHC", days_old=2)
    conn = _FakeConn(universe=["CATSTOCK", "CATUNLISTED", "FRESHC"],
                     existing_rows=[_stale_row("CATSTOCK"), _stale_row("CATUNLISTED"), fresh],
                     priced=[])
    fetched = _wire_run(monkeypatch, conn, {}, asset_types={"CATSTOCK": "Stock"})

    stats = w.run("ignored", refresh_days=30)

    assert fetched == ["CATSTOCK", "CATUNLISTED"]
    assert stats["skipped_fresh"] == 1
    assert stats["extension"] == 0


def test_run_capped_backfill_resumes_across_runs(monkeypatch):
    # The first production run is a backfill; WORKER_LIMIT splits it over runs
    # and each run picks up where the previous one stopped (fetched_at is the cursor).
    priced = ["E1", "E2", "E3", "E4", "E5"]
    conn = _FakeConn(universe=[], existing_rows=[], priced=priced)
    fetched = _wire_run(monkeypatch, conn, {}, asset_types={t: "ETF" for t in priced})

    first = w.run("ignored", limit=2)
    second = w.run("ignored", limit=2)
    third = w.run("ignored", limit=2)

    assert fetched == ["E1", "E2", "E3", "E4", "E5"]
    assert [first["deferred"], second["deferred"], third["deferred"]] == [3, 1, 0]
    assert third["skipped_fresh"] == 4


def test_run_capped_refresh_takes_new_tickers_before_the_stalest(monkeypatch):
    conn = _FakeConn(
        universe=["OLD", "OLDER", "FRESH"],
        existing_rows=[_stale_row("OLD", 40), _stale_row("OLDER", 200), _stale_row("FRESH", 1)],
        priced=["NEWETF"],
    )
    fetched = _wire_run(monkeypatch, conn, {}, asset_types={"NEWETF": "ETF"})

    stats = w.run("ignored", limit=2)

    assert fetched == ["NEWETF", "OLDER"]
    assert stats["due"] == 3 and stats["deferred"] == 1


def test_run_without_the_listing_refreshes_the_catalog_and_reports_aborted(monkeypatch):
    conn = _FakeConn(universe=["SPY"], existing_rows=[], priced=["AGG"])
    fetched = _wire_run(monkeypatch, conn, {"SPY": _META_PAYLOAD},
                        asset_types=ConnectionError("media host down"))

    stats = w.run("ignored")

    assert fetched == ["SPY"]
    assert conn.priced_reads == 0
    assert stats["extension"] == 0
    assert "supported_tickers unavailable" in stats["aborted"]  # run_worker exits 1


# ──────────────────────────────────────────────────────────────────────────────
# Budget: the backfill is paced under the shared account ceiling, not a burst
# ──────────────────────────────────────────────────────────────────────────────
def test_fetch_pacing_fits_the_shared_hourly_budget():
    # An hour of sweeping is at most one burst plus the refill rate.
    assert w.FETCH_BURST + w.FETCH_RATE_PER_S * 3600 <= TIINGO_MAX_REQUESTS_PER_HOUR
    TiingoClient(key="test", bucket=TokenBucket(
        max_tokens=w.FETCH_BURST, refill_rate=w.FETCH_RATE_PER_S)).close()


def test_run_backfill_requests_follow_the_token_bucket(monkeypatch):
    # Drive run() through the real TiingoClient + TokenBucket on a virtual clock:
    # N requests cannot complete before (N - burst) / rate seconds.
    clock = type("Clock", (), {"t": 0.0})()
    clock.monotonic = lambda: clock.t
    # A real sleep always moves time on; a float add of a sub-ulp wait would not,
    # and the bucket would spin. The nanosecond floor only lengthens the clock.
    clock.sleep = lambda s: setattr(clock, "t", clock.t + max(s, 1e-9))
    monkeypatch.setattr(_tiingo, "time", clock)

    tickers = [f"ETF{i:03d}" for i in range(60)]
    conn = _FakeConn(universe=[], existing_rows=[], priced=tickers)
    _wire_run(monkeypatch, conn, {})
    clients: list[TiingoClient] = []

    class _PacedTiingo(TiingoClient):
        def __init__(self, *a, **k):
            super().__init__(key="test", **k)
            self._client.get = lambda url, **kw: _FakeResponse(  # type: ignore[assignment]
                200, {**_META_PAYLOAD, "ticker": url.rsplit("/", 1)[-1]})
            clients.append(self)

        def fetch_supported_asset_types(self):
            return {t: "ETF" for t in tickers}

    monkeypatch.setattr(w, "TiingoClient", _PacedTiingo)

    stats = w.run("ignored")

    assert stats["fetched"] == 60
    assert clients[0].requests_made == 60
    assert clock.t >= (60 - w.FETCH_BURST) / w.FETCH_RATE_PER_S - 1e-9


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
