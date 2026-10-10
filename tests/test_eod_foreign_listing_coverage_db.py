"""TimescaleDB tests: W1c foreign listings get full ``eod_prices`` history.

Runs against a disposable ``timescale/timescaledb:2.27.2-pg18`` database named in
``EOD_COVERAGE_TEST_DSN`` (loopback, database name containing ``eod_coverage``).
The W1c schema is installed from ``schemas/`` into ``public``; the warmer's own
tables live in a per-test schema reached through ``search_path``. Tiingo is an
``httpx.MockTransport``: no network, no API key.

Covers: W1c source-set selection, instruments metadata seeding without
clobbering and the status table's reader grants, full-history cold start vs the
screener's 745-day cold start, status-based ring admission, truncated-history
backfill without rewriting rows, the review gate's reproductions (an interrupted
load, a 2:1 split after the stored rows, a raw-close conflict, an unusable bar,
an empty older window), the compressed-chunk insert path (production layout,
decompression limit forced to 1), Tiingo-unknown handling, and the per-run cap,
budget abort and resume.
"""

from __future__ import annotations

import collections
import contextlib
import datetime as dt
import json
import os
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import exchange_calendars as xcals
import httpx
import psycopg
import pytest
from psycopg import errors, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from test_eod_history_mutants import WARMER_MUTANTS, warmer_mutant

from src.workers import _tiingo
from src.workers import eod_history_validation as v
from src.workers import eod_prices_warmer as w

ROOT = Path(__file__).resolve().parents[1]
AS_OF = dt.date(2026, 10, 9)  # a Friday
D = dt.date


# ──────────────────────────────────────────────────────────────────────────────
# Database fixtures
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def base_dsn():
    dsn = os.environ.get("EOD_COVERAGE_TEST_DSN")
    if not dsn:
        pytest.skip("EOD_COVERAGE_TEST_DSN not set (disposable TimescaleDB only)")
    info = conninfo_to_dict(dsn)
    if info.get("host") not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail("EOD_COVERAGE_TEST_DSN must be a loopback database")
    if "eod_coverage" not in (info.get("dbname") or "") or info.get("user") == "mcp_ro":
        pytest.fail("EOD_COVERAGE_TEST_DSN must name a disposable eod_coverage database")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        version, ext = conn.execute(
            "SELECT current_setting('server_version_num'),"
            " (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb')"
        ).fetchone()
        assert version.startswith("18") and ext == "2.27.2", (version, ext)
        for name in ("sec_foreign_listing_evidence.sql", "sec_foreign_listing_evidence_v2.sql"):
            conn.execute((ROOT / "schemas" / name).read_text(encoding="utf-8"))
    return dsn


class Db:
    def __init__(self, base: str, schema: str):
        self.schema = schema
        self.dsn = make_conninfo(base, options=f"-c search_path={schema},public")
        self.conn = psycopg.connect(self.dsn, autocommit=True)

    def q(self, query, params=None):
        return self.conn.execute(query, params).fetchall()

    def one(self, query, params=None):
        return self.conn.execute(query, params).fetchone()


EOD_DDL = """
CREATE TABLE instruments (
    ticker varchar PRIMARY KEY, name varchar, exchange_code varchar,
    asset_type varchar, tiingo_start_date date, tiingo_end_date date,
    eod_last_fetched_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE eod_prices (
    ticker varchar NOT NULL REFERENCES instruments(ticker) ON DELETE CASCADE,
    date date NOT NULL,
    open double precision NOT NULL, high double precision NOT NULL,
    low double precision NOT NULL, close double precision NOT NULL,
    volume bigint NOT NULL,
    adj_open double precision NOT NULL, adj_high double precision NOT NULL,
    adj_low double precision NOT NULL, adj_close double precision NOT NULL,
    adj_volume bigint NOT NULL,
    div_cash double precision NOT NULL DEFAULT 0,
    split_factor double precision NOT NULL DEFAULT 1,
    CONSTRAINT pk_eod_prices PRIMARY KEY (ticker, date));
CREATE INDEX ix_eod_prices_date ON eod_prices (date);
CREATE TABLE universe_constituents (ticker text PRIMARY KEY, name text, status text NOT NULL);
"""


@pytest.fixture
def db(base_dsn):
    schema = f"eodcov_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        admin.execute("TRUNCATE public.sec_foreign_listing_evidence")
    handle = Db(base_dsn, schema)
    handle.conn.execute(EOD_DDL)
    # Production layout: monthly chunks, segmentby ticker, orderby date DESC.
    handle.conn.execute(
        "SELECT create_hypertable(%s, 'date', chunk_time_interval => INTERVAL '1 month')",
        (f"{schema}.eod_prices",),
    )
    handle.conn.execute(
        "ALTER TABLE eod_prices SET (timescaledb.compress,"
        " timescaledb.compress_segmentby = 'ticker', timescaledb.compress_orderby = 'date DESC')"
    )
    try:
        yield handle
    finally:
        handle.conn.close()
        with psycopg.connect(base_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            admin.execute("TRUNCATE public.sec_foreign_listing_evidence")


# ──────────────────────────────────────────────────────────────────────────────
# W1c evidence
# ──────────────────────────────────────────────────────────────────────────────
def add_evidence(db, cik, symbol, filed, *, listed_type="ads", kind="listed_type",
                 retired=None):
    filed_d = D.fromisoformat(filed)
    tomorrow = filed_d + dt.timedelta(days=1)
    row = dict(
        fact_hash=uuid.uuid4().hex, cik=cik, symbol=symbol, ordinary_candidate=True,
        adsh=f"{cik % 10**10:010d}-{filed_d.year % 100:02d}-{filed_d.timetuple().tm_yday:06d}",
        form="20-F" if kind == "listed_type" else "F-6", filed=filed_d,
        source_url="https://www.sec.gov/Archives/edgar/data/test", source_sha256="a" * 64,
        source_kind="cover_12b" if kind == "listed_type" else "f6",
        evidence_kind=kind,
        listed_type=listed_type if kind == "listed_type" else None,
        ratio_numerator=None if kind == "listed_type" else 2,
        ratio_denominator=None if kind == "listed_type" else 1,
        effective_from=tomorrow, evidence_text="synthetic", evidence_location="test",
        parser_version="test-v1", available_on=tomorrow, retired_on=retired,
        loaded_on=D(2026, 10, 9), source_package=uuid.uuid4().hex,
    )
    db.conn.execute(
        sql.SQL("INSERT INTO public.sec_foreign_listing_evidence ({}) VALUES ({})").format(
            sql.SQL(",").join(map(sql.Identifier, row)),
            sql.SQL(",").join(sql.Placeholder() for _ in row),
        ),
        list(row.values()),
    )


def resolved_lines(db, *symbols, cik=None):
    """One resolved cover per symbol, each on its own synthetic issuer."""
    for i, symbol in enumerate(symbols):
        add_evidence(db, (cik or 9_000_000) + i, symbol, "2019-03-01")


# ──────────────────────────────────────────────────────────────────────────────
# Tiingo mock
# ──────────────────────────────────────────────────────────────────────────────
_XNYS = xcals.get_calendar("XNYS", start="1900-01-01")


def bdays(a, b):
    """XNYS sessions in [a, b]: the provider's and the store's trading days."""
    if a > b:
        return []
    return [ts.date() for ts in _XNYS.sessions_in_range(a.isoformat(), b.isoformat())]


def raw_close(ticker, d):
    """Deterministic raw close per ticker and date (shared by stored rows)."""
    return round(20.0 + (d.toordinal() % 50) / 10 + sum(map(ord, ticker)) % 7, 4)


class FakeTiingo:
    """Tiingo meta + daily prices over a MockTransport, with a request log.

    A listing's ``adj_factor`` is Tiingo's CURRENT adjustment of every bar
    (adjClose = raw × factor); stored rows written with another factor model
    rows fetched before a corporate action. ``bad`` overrides fields of single
    bars; ``omit`` drops single bars from responses; ``price_start`` /
    ``price_end`` make price data start later or end earlier than meta says;
    ``raw_prices`` replaces a ticker's price body verbatim; ``meta_start`` /
    ``meta_end`` replace the metadata dates verbatim."""

    def __init__(self):
        self.listings: dict[str, dict] = {}
        self.unknown: set[str] = set()
        self.meta_status: dict[str, int] = {}
        self.price_status: dict[str, int] = {}
        self.bad: dict[tuple[str, dt.date], dict] = {}
        self.omit: set[tuple[str, dt.date]] = set()
        self.raw_prices: dict[str, object] = {}
        self.no_key = False
        self.hooks: dict[str, object] = {}   # ticker -> f(start, end) run during a price request
        self.requests: list[tuple] = []

    def listing(self, ticker):
        return self.listings.get(ticker) or {"start": D(1993, 1, 29), "end": AS_OF}

    def bar(self, ticker, d):
        px = raw_close(ticker, d)
        adj = px * self.listing(ticker).get("adj_factor", 1.0)
        bar = {
            "date": f"{d.isoformat()}T00:00:00.000Z", "open": px, "high": px, "low": px,
            "close": px, "volume": 1000, "adjOpen": adj, "adjHigh": adj, "adjLow": adj,
            "adjClose": adj, "adjVolume": 1000, "divCash": 0.0, "splitFactor": 1.0,
        }
        bar.update(self.bad.get((ticker, d), {}))
        return bar

    def handler(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.strip("/").split("/")
        ticker = parts[2]
        if len(parts) == 3:
            self.requests.append(("meta", ticker))
            status = self.meta_status.get(ticker)
            if status:
                return httpx.Response(status, json={"detail": "status"})
            if ticker in self.unknown:
                return httpx.Response(404, json={"detail": "Not found."})
            lst = self.listing(ticker)
            return httpx.Response(200, json={
                "ticker": ticker.lower(), "name": lst.get("name", f"{ticker} Holdings"),
                "exchangeCode": lst.get("exchange", "NYSE"), "description": "",
                "startDate": lst["meta_start"] if "meta_start" in lst else (
                    None if lst.get("no_start") else lst["start"].isoformat()),
                "endDate": lst["meta_end"] if "meta_end" in lst else (
                    None if lst.get("no_end") else lst["end"].isoformat()),
            })
        params = parse_qs(urlsplit(str(request.url)).query)
        start = D.fromisoformat(params["startDate"][0])
        end = D.fromisoformat(params["endDate"][0]) if "endDate" in params else AS_OF
        self.requests.append(("prices", ticker, start, end))
        if ticker in self.hooks:
            self.hooks[ticker](start, end)
        status = self.price_status.get(ticker)
        if status:
            return httpx.Response(status, json={"detail": "status"})
        if ticker in self.unknown:
            return httpx.Response(404, json={"detail": "Not found."})
        if ticker in self.raw_prices:
            return httpx.Response(200, json=self.raw_prices[ticker])
        lst = self.listing(ticker)
        first = max(start, lst.get("price_start", lst["start"]))
        end = min(end, lst.get("price_end", end))
        bars = [self.bar(ticker, d) for d in bdays(first, min(end, lst["end"]))
                if (ticker, d) not in self.omit]
        # Raw JSON text, as a server could send it: NaN / Infinity tokens included.
        return httpx.Response(200, content=json.dumps(bars).encode(),
                              headers={"content-type": "application/json"})

    def of(self, kind, ticker=None):
        return [r for r in self.requests
                if r[0] == kind and (ticker is None or r[1] == ticker)]

    def history_requests(self, ticker):
        """Meta calls and price windows that are not the ring's 5-day overlap."""
        return [r for r in self.requests if r[1] == ticker
                and (r[0] == "meta" or (r[3] - r[2]).days > w.WATERMARK_OVERLAP_DAYS)]


@pytest.fixture
def tiingo(monkeypatch):
    fake = FakeTiingo()

    def factory(*args, bucket=None, **kwargs):
        client = _tiingo.TiingoClient(key="test", bucket=bucket)
        client._client.close()
        client._client = httpx.Client(transport=httpx.MockTransport(fake.handler))
        if fake.no_key:
            client._key = ""
        return client

    monkeypatch.setattr(w, "TiingoClient", factory)
    monkeypatch.setattr(w, "FETCH_BURST", 100_000.0)
    monkeypatch.setattr(_tiingo.time, "sleep", lambda s: None)
    monkeypatch.delenv(w.HISTORY_LIMIT_ENV, raising=False)
    return fake


def run(db, **kwargs):
    kwargs.setdefault("history_limit", 250)
    return w.run(db.dsn, calc_date=AS_OF.isoformat(), **kwargs)


def history_of(db, ticker):
    return db.one("SELECT min(date), max(date), count(*) FROM eod_prices WHERE ticker = %s",
                  (ticker,))


def status_of(db, ticker):
    return db.one("SELECT status, detail, history_start FROM eod_warmer_ticker_status"
                  " WHERE ticker = %s", (ticker,))


def missing_dates(db, ticker, a, b):
    have = {r[0] for r in db.q(
        "SELECT date FROM eod_prices WHERE ticker = %s AND date BETWEEN %s AND %s", (ticker, a, b))}
    return [d for d in bdays(a, b) if d not in have]


def store_rows(db, ticker, a, b, *, adj_factor=1.0):
    """Rows as the ring or API stored them: raw closes, adjusted at ``adj_factor``."""
    rows = []
    for d in bdays(a, b):
        px = raw_close(ticker, d)
        adj = px * adj_factor
        rows.append((ticker, d, px, px, px, px, 1000, adj, adj, adj, adj, 1000, 0.0, 1.0))
    with db.conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO eod_prices VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            rows)


def snapshot(db, ticker, a, b):
    return db.one("SELECT count(*), sum(close), sum(adj_close), sum(volume) FROM eod_prices"
                  " WHERE ticker = %s AND date BETWEEN %s AND %s", (ticker, a, b))


def certify(conn, ticker, rows, start):
    """promote() a load verdict for ``rows`` judged on the current store."""
    stored = w._stored_rows(conn, ticker, through=AS_OF)
    verdict = v.Verdict(v.VERDICT_LOAD, "test", tuple(rows), v.stored_digest(stored))
    return w.promote(conn, ticker, verdict, through=AS_OF, history_start=start)


def _chunk_of(db, ticker, day):
    return db.one("SELECT tableoid::regclass::text FROM eod_prices WHERE ticker = %s AND date = %s",
                  (ticker, day))[0]


def compress_older_than(db, cutoff):
    for (chunk,) in db.q(
        "SELECT format('%%I.%%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks"
        " WHERE hypertable_schema = %s AND hypertable_name = 'eod_prices'"
        "   AND range_end <= %s AND NOT is_compressed", (db.schema, cutoff)):
        db.conn.execute("SELECT compress_chunk(%s::regclass)", (chunk,))


def instruments_row(db, ticker, *, name=None, exchange=None, start=None, end=None,
                    asset_type="stock", updated_at=None):
    db.conn.execute(
        "INSERT INTO instruments (ticker, name, exchange_code, asset_type, tiingo_start_date,"
        " tiingo_end_date, updated_at) VALUES (%s, %s, %s, %s, %s, %s, coalesce(%s, now()))",
        (ticker, name, exchange, asset_type, start, end, updated_at))


# ──────────────────────────────────────────────────────────────────────────────
# 1. Source set
# ──────────────────────────────────────────────────────────────────────────────
def test_source_set_is_listing_resolved_at_run_date_or_any_year_end(db):
    add_evidence(db, 1046179, "TSM", "2019-03-01")            # resolved 2020+, no ratio
    add_evidence(db, 2, "OLDA", "2012-04-01")                 # only until the 2017 cover
    add_evidence(db, 2, "NEWB", "2017-04-01")
    add_evidence(db, 3, "BRIEF", "2011-02-01")                # Feb-Jun 2011 only
    add_evidence(db, 3, "OTHR", "2011-06-01")
    add_evidence(db, 4, "AMBG", "2018-01-01", listed_type="ads")
    add_evidence(db, 4, "AMBG", "2018-01-01", listed_type="ordinary_direct")
    add_evidence(db, 5, "SANB11", "2018-01-01", listed_type="ordinary_direct")
    add_evidence(db, 5, "ABCD-B", "2018-01-01", listed_type="ordinary_direct")
    add_evidence(db, 6, "GONE", "2014-01-01", retired=D(2019, 6, 1))  # visible at 2015, not active
    add_evidence(db, 7, "RTIO", "2018-01-01", kind="ads_ratio")
    add_evidence(db, 8, "RCNT", "2026-02-01")                 # resolved only at the run date
    add_evidence(db, 9, "LATE", "2024-01-01")                 # public only from 2024

    # TSM proves listing_status, not the combined status, selects the line.
    assert db.one("SELECT status, listing_status FROM public.sec_foreign_listing_at("
                  "1046179, 'TSM', DATE '2025-12-31')") == ("none", "resolved")
    # GONE would resolve at 2015 if retired facts counted.
    assert db.one("SELECT listing_status FROM public.sec_foreign_listing_at("
                  "6, 'GONE', DATE '2015-12-31')") == ("resolved",)

    with psycopg.connect(db.dsn) as conn:
        jit_before = conn.execute("SHOW jit").fetchone()[0]
        conn.commit()
        assert w.foreign_listing_tickers(conn, AS_OF) == [
            "ABCD-B", "LATE", "NEWB", "OLDA", "OTHR", "RCNT", "TSM"]
        assert w.foreign_listing_tickers(conn, D(2026, 1, 15)) == [
            "ABCD-B", "LATE", "NEWB", "OLDA", "OTHR", "TSM"]
        # Codex 4237494731: a historical run probes no year-end after as_of, so
        # LATE (resolved at 2025-12-31) is not pulled into a 2020 run.
        assert w.foreign_listing_tickers(conn, D(2020, 6, 30)) == [
            "ABCD-B", "NEWB", "OLDA", "OTHR", "TSM"]
        # JIT is off only inside the selection transaction.
        assert conn.execute("SHOW jit").fetchone()[0] == jit_before


def test_source_is_absent_without_the_w1c_resolver(db):
    with psycopg.connect(db.dsn) as conn:
        conn.execute("ALTER FUNCTION public.sec_foreign_listing_at(bigint, text, date)"
                     " RENAME TO sec_foreign_listing_at_hidden")
        try:
            assert w.foreign_listing_tickers(conn, AS_OF) is None
        finally:
            conn.rollback()


# ──────────────────────────────────────────────────────────────────────────────
# 2. instruments seeding and the status table's readers
# ──────────────────────────────────────────────────────────────────────────────
def test_meta_seeds_new_instruments_and_fills_only_nulls(db, tiingo):
    resolved_lines(db, "KEEP", "FILL", "NEWT")
    frozen = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    instruments_row(db, "KEEP", name="Keep Co", exchange="NYSE", start=D(2001, 1, 2),
                    end=D(2026, 6, 30), updated_at=frozen)
    instruments_row(db, "FILL", name="Screener Name", asset_type=None, updated_at=frozen)
    store_rows(db, "FILL", D(2024, 9, 30), D(2026, 10, 2))  # the 745-day cohort
    tiingo.listings.update({
        "KEEP": {"start": D(2001, 1, 2), "end": AS_OF},
        "FILL": {"start": D(2005, 1, 3), "end": AS_OF, "name": "Fill Tiingo", "exchange": "NASDAQ"},
        "NEWT": {"start": D(1997, 10, 9), "end": AS_OF, "name": "New Listing Co", "exchange": "NYSE"},
    })

    stats = run(db)["foreign_history"]

    rows = {r[0]: r[1:] for r in db.q(
        "SELECT ticker, name, exchange_code, asset_type, tiingo_start_date, tiingo_end_date,"
        " updated_at FROM instruments WHERE ticker IN ('KEEP', 'FILL', 'NEWT')")}
    # Nothing to fill: the row is not touched at all.
    assert rows["KEEP"] == ("Keep Co", "NYSE", "stock", D(2001, 1, 2), D(2026, 6, 30), frozen)
    # NULLs filled from meta; the screener's name and NULL asset_type stay.
    assert rows["FILL"][:5] == ("Screener Name", "NASDAQ", None, D(2005, 1, 3), AS_OF)
    assert rows["FILL"][5] > frozen
    # New row: Tiingo metadata, asset_type stock.
    assert rows["NEWT"][:5] == ("New Listing Co", "NYSE", "stock", D(1997, 10, 9), AS_OF)
    assert (stats["instruments_inserted"], stats["instruments_filled"]) == (1, 1)
    assert history_of(db, "KEEP")[0] == D(2001, 1, 2)
    assert history_of(db, "NEWT")[0] == D(1997, 10, 9)
    assert history_of(db, "FILL")[0] == D(2005, 1, 3)


def test_status_table_is_readable_by_the_api_and_read_only_roles(db):
    with psycopg.connect(db.dsn, autocommit=True) as conn:
        conn.execute(
            """DO $$ DECLARE r text; BEGIN
                 FOREACH r IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
                   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
                     EXECUTE format('CREATE ROLE %I NOLOGIN', r);
                   END IF;
                 END LOOP; END $$""")
    with psycopg.connect(db.dsn) as conn:
        w.ensure_status_table(conn)
        w.ensure_status_table(conn)  # idempotent
    for role in ("app_runtime", "app_analytics_ro", "mcp_ro"):
        assert db.one("SELECT has_table_privilege(%s, %s, 'SELECT'),"
                      " has_table_privilege(%s, %s, 'INSERT')",
                      (role, f"{db.schema}.eod_warmer_ticker_status",
                       role, f"{db.schema}.eod_warmer_ticker_status")) == (True, False)


# ──────────────────────────────────────────────────────────────────────────────
# 3. full-history cold start vs the screener's 745 days; ring admission
# ──────────────────────────────────────────────────────────────────────────────
def test_covered_cold_start_is_full_history_and_screener_stays_745_days(db, tiingo):
    resolved_lines(db, "FRGN", "BOTH")
    db.conn.execute("INSERT INTO universe_constituents VALUES"
                    " ('SCRN', 'Screener Co', 'active'), ('BOTH', 'Both Co', 'active')")
    universe_before = db.q("SELECT * FROM universe_constituents ORDER BY ticker")
    tiingo.listings.update({
        "SCRN": {"start": D(1990, 1, 2), "end": AS_OF},
        "FRGN": {"start": D(1997, 10, 9), "end": AS_OF},
        "BOTH": {"start": D(2000, 5, 1), "end": AS_OF},
    })

    stats = run(db)

    screener_start = AS_OF - dt.timedelta(days=w.NEW_TICKER_LOOKBACK_DAYS)
    assert tiingo.of("prices", "SCRN") == [("prices", "SCRN", screener_start, AS_OF)]
    assert tiingo.of("meta", "SCRN") == []
    assert history_of(db, "SCRN")[0] >= screener_start
    # A covered line outside the screener: one Tiingo startDate -> as_of request.
    assert tiingo.of("prices", "FRGN") == [("prices", "FRGN", D(1997, 10, 9), AS_OF)]
    # A covered screener line without rows keeps its 745-day ring warming; the
    # history phase then extends it backward through the same verification.
    assert tiingo.of("prices", "BOTH") == [
        ("prices", "BOTH", screener_start, AS_OF), ("prices", "BOTH", D(2000, 5, 1), AS_OF)]
    assert history_of(db, "FRGN")[:2] == (D(1997, 10, 9), AS_OF)
    assert missing_dates(db, "FRGN", D(1997, 10, 9), AS_OF) == []
    assert history_of(db, "BOTH")[:2] == (D(2000, 5, 1), AS_OF)
    assert stats["foreign_history"]["completed"] == 2
    assert db.q("SELECT * FROM universe_constituents ORDER BY ticker") == universe_before

    # Next run: both are ordinary ring tickers (incremental), no history work.
    tiingo.requests.clear()
    stats = run(db)
    assert tiingo.of("meta") == []
    assert tiingo.of("prices", "FRGN") == [
        ("prices", "FRGN", AS_OF - dt.timedelta(days=w.WATERMARK_OVERLAP_DAYS), AS_OF)]
    assert stats["foreign_history"]["already_complete"] == 2
    assert stats["foreign_history"]["pending"] == 0


# ──────────────────────────────────────────────────────────────────────────────
# 4. truncated history, compressed chunks, end to end
# ──────────────────────────────────────────────────────────────────────────────
def test_truncated_history_is_backfilled_into_compressed_chunks_without_rewrites(db, tiingo):
    resolved_lines(db, "TRNC", "CMPL")
    instruments_row(db, "TRNC", name="Trunc Co")
    instruments_row(db, "CMPL", name="Complete Co", exchange="NYSE", start=D(2010, 1, 4),
                    end=AS_OF)
    store_rows(db, "TRNC", D(2024, 6, 3), D(2026, 10, 2))
    store_rows(db, "CMPL", D(2010, 1, 4), D(2026, 10, 2))
    compress_older_than(db, AS_OF - dt.timedelta(days=90))   # the columnstore policy
    assert db.one(
        "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = %s"
        " AND hypertable_name = 'eod_prices' AND is_compressed", (db.schema,))[0] > 20
    untouched = (D(2024, 6, 3), D(2026, 9, 26))   # before the ring's 5-day overlap
    existing = snapshot(db, "TRNC", *untouched)
    tiingo.listings["TRNC"] = {"start": D(2005, 1, 3), "end": AS_OF}
    tiingo.listings["CMPL"] = {"start": D(2010, 1, 4), "end": AS_OF}

    # Any decompression of an existing batch would now fail the run.
    strict = make_conninfo(
        db.dsn, options=f"-c search_path={db.schema},public"
        " -c timescaledb.max_tuples_decompressed_per_dml_transaction=1")
    stats = w.run(strict, calc_date=AS_OF.isoformat(), history_limit=250)

    assert "aborted" not in stats
    # Rows keep the ring refresh; the verification pass reads the full range.
    overlap_start = D(2026, 10, 2) - dt.timedelta(days=w.WATERMARK_OVERLAP_DAYS)
    assert tiingo.of("prices", "TRNC") == [
        ("prices", "TRNC", overlap_start, AS_OF), ("prices", "TRNC", D(2005, 1, 3), AS_OF)]
    assert history_of(db, "TRNC")[0] == D(2005, 1, 3)
    assert missing_dates(db, "TRNC", D(2005, 1, 3), AS_OF) == []
    assert snapshot(db, "TRNC", *untouched) == existing       # nothing rewritten
    prefix = len(list(bdays(D(2005, 1, 3), D(2024, 5, 31))))
    assert status_of(db, "TRNC") == (
        "history_complete", f"inserted {prefix} missing sessions; history_from=2005-01-03",
        D(2005, 1, 3))
    # Rows that reach the start still get the pass: verified, nothing inserted.
    assert tiingo.of("prices", "CMPL")[-1] == ("prices", "CMPL", D(2010, 1, 4), AS_OF)
    assert status_of(db, "CMPL")[0] == "history_complete"
    assert status_of(db, "CMPL")[1].endswith("0 missing; history_from=2010-01-04")
    assert stats["foreign_history"]["completed"] == 2
    assert stats["foreign_history"]["verified_without_insert"] == 1

    tiingo.requests.clear()
    run(db)
    assert [r for r in tiingo.requests if r[1] == "TRNC"] == [
        ("prices", "TRNC", AS_OF - dt.timedelta(days=w.WATERMARK_OVERLAP_DAYS), AS_OF)]


# ──────────────────────────────────────────────────────────────────────────────
# 5. Gate and re-gate reproductions
# ──────────────────────────────────────────────────────────────────────────────
def test_interrupted_load_commits_nothing_and_resume_refetches_everything(db, tiingo):
    """Gate repro: TRNC has 2024-2026 rows; ascending history from 2000 fails in
    its second 500-row batch. Batch one must not survive, the status must not
    say complete, and the resume must refetch and leave no missing date."""
    resolved_lines(db, "TRNC")
    instruments_row(db, "TRNC", name="Trunc Co")
    store_rows(db, "TRNC", D(2024, 1, 2), D(2026, 10, 2))
    tiingo.listings["TRNC"] = {"start": D(2000, 1, 3), "end": AS_OF}
    prefix_days = list(bdays(D(2000, 1, 3), D(2024, 1, 1)))
    poison = prefix_days[700]                     # inside the second batch
    # A valid bar the database refuses: the load dies mid-way. (An out-of-range
    # value no longer reaches the insert; the validator bounds it.)
    db.conn.execute(sql.SQL(
        "ALTER TABLE eod_prices ADD CONSTRAINT poison CHECK (NOT (ticker = 'TRNC' AND date = {}))"
    ).format(sql.Literal(poison)))

    stats = run(db)["foreign_history"]          # the phase survives the failure
    assert history_of(db, "TRNC")[0] == D(2024, 1, 2)       # batch one rolled back too
    assert status_of(db, "TRNC")[:2] == ("history_incomplete", "unexpected:CheckViolation")
    assert stats["error_tickers"] == {"TRNC": "unexpected:CheckViolation"}

    db.conn.execute("ALTER TABLE eod_prices DROP CONSTRAINT poison")
    db.conn.execute("UPDATE eod_warmer_ticker_status SET retry_after = now() - interval '1 second'")
    tiingo.requests.clear()
    stats = run(db)["foreign_history"]
    assert ("prices", "TRNC", D(2000, 1, 3), AS_OF) in tiingo.requests   # refetched in full
    assert stats["completed"] == 1
    assert status_of(db, "TRNC")[0] == "history_complete"
    assert missing_dates(db, "TRNC", D(2000, 1, 3), AS_OF) == []


def test_partial_history_from_an_older_writer_is_verified_not_assumed(db, tiingo):
    """Re-gate P1 #1: the first 500 of 1,200 ascending prefix rows were committed
    by a per-batch writer, which then died. min(date) already reaches Tiingo's
    startDate. The pass must fetch, find the 700 missing dates and fill them."""
    resolved_lines(db, "PART")
    instruments_row(db, "PART", name="Partial Co")
    start = D(2019, 1, 2)
    prefix = list(bdays(start, D(2024, 1, 1)))[:1200]
    store_rows(db, "PART", prefix[0], prefix[499])
    store_rows(db, "PART", prefix[1199] + dt.timedelta(days=1), D(2026, 10, 2))
    tiingo.listings["PART"] = {"start": start, "end": AS_OF}
    assert history_of(db, "PART")[0] == start
    assert len(missing_dates(db, "PART", start, D(2026, 10, 2))) == 700

    stats = run(db)["foreign_history"]
    assert stats["history_fetches"] == 1
    assert ("prices", "PART", start, AS_OF) in tiingo.requests
    assert status_of(db, "PART")[:2] == (
        "history_complete", "inserted 700 missing sessions; history_from=2019-01-02")
    assert missing_dates(db, "PART", start, AS_OF) == []


def test_a_response_omitting_stored_boundary_sessions_inserts_nothing(db, tiingo):
    """Re-gate P1 #2: stored Sep 21-Oct 7 still at factor 1.0, Oct 8-9 already
    refreshed at 0.5 after a split; the response carries the prefix at 0.5 but
    omits Sep 21-Oct 7. Inserting would put 0.5-basis Sep 18 next to 1.0-basis
    Sep 21 (+103% fabricated). It must fail closed."""
    resolved_lines(db, "SEAM")
    instruments_row(db, "SEAM", name="Seam Co")
    store_rows(db, "SEAM", D(2026, 9, 21), D(2026, 10, 7), adj_factor=1.0)
    store_rows(db, "SEAM", D(2026, 10, 8), AS_OF, adj_factor=0.5)
    tiingo.listings["SEAM"] = {"start": D(2026, 1, 2), "end": AS_OF, "adj_factor": 0.5}
    tiingo.omit |= {("SEAM", d) for d in bdays(D(2026, 9, 21), D(2026, 10, 7))}
    before = snapshot(db, "SEAM", D(2026, 1, 1), AS_OF)

    stats = run(db)["foreign_history"]
    assert status_of(db, "SEAM")[:2] == (
        "history_incomplete", "stored_sessions_missing=13 first=2026-09-21")
    assert stats["errors"] == 1
    assert history_of(db, "SEAM")[0] == D(2026, 9, 21)
    assert snapshot(db, "SEAM", D(2026, 1, 1), AS_OF) == before

    # With the full response the moved basis is visible: still nothing inserted.
    tiingo.omit.clear()
    db.conn.execute("UPDATE eod_warmer_ticker_status SET retry_after = now() - interval '1 second'")
    stats = run(db)["foreign_history"]
    assert status_of(db, "SEAM")[:2] == (
        # Sep 21-Oct 2 still at 1.0; the ring's overlap has re-based Oct 5-9.
        "adjustment_rebase_required", "adjusted_moved: ratio=0.500000 on 10/15 sessions")
    assert history_of(db, "SEAM")[0] == D(2026, 9, 21)


def test_a_truncated_response_cannot_insert_a_seam_before_the_stored_tail(db, tiingo):
    """Re-gate 2, P1 (i): meta starts Aug 3; the response holds only Aug 3-Sep 18
    at factor 0.5; stored Sep 21-Oct 9 keeps factor 1.0. No shared session: the
    old check passed vacuously, inserted 35 rows and a +103% seam."""
    resolved_lines(db, "TRUN")
    instruments_row(db, "TRUN", name="Trunc Response Co")
    store_rows(db, "TRUN", D(2026, 9, 21), AS_OF, adj_factor=1.0)
    tiingo.listings["TRUN"] = {"start": D(2026, 8, 3), "end": AS_OF, "adj_factor": 0.5,
                               "price_end": D(2026, 9, 18)}
    before = snapshot(db, "TRUN", D(2026, 1, 1), AS_OF)

    stats = run(db, history_limit=250)["foreign_history"]
    assert ("prices", "TRUN", D(2026, 8, 3), AS_OF) in tiingo.requests   # the request's interval
    assert status_of(db, "TRUN")[:2] == (
        "history_incomplete", "stored_sessions_missing=15 first=2026-09-21")
    assert stats["completed"] == 0 and stats["history_rows"] == 0
    assert history_of(db, "TRUN")[0] == D(2026, 9, 21)
    assert snapshot(db, "TRUN", D(2026, 1, 1), AS_OF) == before


def test_a_truncated_response_never_verifies_a_stored_series(db, tiingo):
    """Re-gate 2, P1 (ii): the full Aug 3-Oct 9 series is stored and the
    response stops at Sep 18. It must not report 'verified: 35 sessions'."""
    resolved_lines(db, "TAIL")
    instruments_row(db, "TAIL", name="Tail Co")
    store_rows(db, "TAIL", D(2026, 8, 3), AS_OF)
    tiingo.listings["TAIL"] = {"start": D(2026, 8, 3), "end": AS_OF, "price_end": D(2026, 9, 18)}

    stats = run(db, history_limit=250)["foreign_history"]
    status, detail, _ = status_of(db, "TAIL")
    assert status == "history_incomplete"
    assert detail == "stored_sessions_missing=15 first=2026-09-21"
    assert stats["completed"] == 0 and stats["verified_without_insert"] == 0


def test_stored_rows_before_an_advanced_start_date_fail_closed(db, tiingo):
    resolved_lines(db, "ADVS")
    instruments_row(db, "ADVS", name="Advanced Start Co")
    store_rows(db, "ADVS", D(2026, 7, 1), AS_OF)
    tiingo.listings["ADVS"] = {"start": D(2026, 8, 3), "end": AS_OF}

    stats = run(db, history_limit=250)["foreign_history"]
    status, detail, _ = status_of(db, "ADVS")
    assert status == "history_conflict"
    assert detail.startswith("stored_outside_provider_range: 22 stored sessions")  # July 3 is a holiday
    assert stats["fail_closed"] == 1


def test_malformed_bodies_back_off_and_never_stop_the_phase(db, tiingo):
    """Re-gate 2, P2: [null], ["bad"], a string body and a dict body each get a
    status row, growing backoff and an error entry; the healthy ticker behind
    them completes on the first run."""
    bodies = {"MNUL": [None], "MBAD": ["bad"], "MSTR": "oops", "MDIC": {"detail": "no"}}
    resolved_lines(db, *bodies, "MZOK")
    tiingo.raw_prices.update(bodies)

    for attempt, hours in ((1, 12), (2, 24), (3, 48)):
        stats = run(db, history_limit=250)["foreign_history"]
        assert set(stats["error_tickers"]) == set(bodies)
        assert all(v == "prices:invalid_payload" for v in stats["error_tickers"].values())
        for t in bodies:
            got = db.one("SELECT status, attempts, retry_after - checked_at"
                         " FROM eod_warmer_ticker_status WHERE ticker = %s", (t,))
            assert (got[0], got[1], round(got[2] / dt.timedelta(hours=1))) == (
                "history_incomplete", attempt, hours)
        assert status_of(db, "MZOK")[0] == "history_complete"
        db.conn.execute("UPDATE eod_warmer_ticker_status SET retry_after = now() - interval '1 second'"
                        " WHERE status = 'history_incomplete'")
    assert history_of(db, "MNUL") == (None, None, 0)


def test_an_unexpected_exception_is_contained_to_its_ticker(db, tiingo, monkeypatch):
    resolved_lines(db, "BOOM", "CALM")
    real = w.validate_series

    def flaky(ticker, *args, **kwargs):
        if ticker == "BOOM":
            raise ValueError("surprise")
        return real(ticker, *args, **kwargs)

    monkeypatch.setattr(w, "validate_series", flaky)
    stats = run(db, history_limit=250)["foreign_history"]
    assert status_of(db, "BOOM")[:2] == ("history_incomplete", "unexpected:ValueError")
    assert stats["error_tickers"] == {"BOOM": "unexpected:ValueError"}
    assert status_of(db, "CALM")[0] == "history_complete"


def test_split_after_the_stored_rows_fails_closed_without_a_seam(db, tiingo):
    """Gate repro: rows from 2024-06-11 were stored before a 2:1 split (adj =
    raw); Tiingo now adjusts every older bar by 0.5. A prefix on the new basis
    would fabricate +100% at 2024-06-10 -> 06-11, so nothing is inserted."""
    resolved_lines(db, "SPLT")
    instruments_row(db, "SPLT", name="Split Co")
    store_rows(db, "SPLT", D(2024, 6, 11), D(2026, 10, 2), adj_factor=1.0)
    tiingo.listings["SPLT"] = {"start": D(2005, 1, 3), "end": AS_OF, "adj_factor": 0.5}
    untouched = (D(2024, 6, 11), D(2026, 9, 26))
    stored = snapshot(db, "SPLT", *untouched)

    stats = run(db)["foreign_history"]
    status, detail, start = status_of(db, "SPLT")
    assert (status, start) == ("adjustment_rebase_required", D(2005, 1, 3))
    assert detail.startswith("adjusted_moved: ratio=0.500000 on ")
    assert stats["fail_closed"] == 1
    assert stats["fail_closed_tickers"] == {"SPLT": detail}
    assert history_of(db, "SPLT")[0] == D(2024, 6, 11)     # no prefix, no seam
    assert snapshot(db, "SPLT", *untouched) == stored

    # The existing series keeps its ring refresh; history waits for the recheck.
    tiingo.requests.clear()
    run(db)
    assert tiingo.of("prices", "SPLT") == [
        ("prices", "SPLT", AS_OF - dt.timedelta(days=w.WATERMARK_OVERLAP_DAYS), AS_OF)]
    assert tiingo.of("meta", "SPLT") == []


def test_raw_close_difference_is_a_conflict(db, tiingo):
    resolved_lines(db, "RAWD")
    instruments_row(db, "RAWD", name="Raw Co")
    store_rows(db, "RAWD", D(2024, 6, 11), D(2026, 10, 2))
    # A coherent stored bar on another raw basis (an incoherent one would be
    # refused earlier, as stored_bar_invalid).
    db.conn.execute("UPDATE eod_prices SET open = open * 2, high = high * 2, low = low * 2,"
                    " close = close * 2 WHERE ticker = 'RAWD' AND date = '2025-03-03'")
    tiingo.listings["RAWD"] = {"start": D(2005, 1, 3), "end": AS_OF}

    stats = run(db)["foreign_history"]
    status, detail, _ = status_of(db, "RAWD")
    assert status == "history_conflict"
    assert detail.startswith("raw_differs: ratio=0.500000 on 1/")
    assert stats["fail_closed"] == 1
    assert history_of(db, "RAWD")[0] == D(2024, 6, 11)


def test_an_unusable_bar_leaves_the_ticker_incomplete_and_retryable(db, tiingo):
    resolved_lines(db, "DROP")
    tiingo.listings["DROP"] = {"start": D(2000, 1, 3), "end": AS_OF}
    tiingo.bad[("DROP", D(2000, 1, 4))] = {"adjClose": None}

    stats = run(db)["foreign_history"]
    reason = "unusable_bar: non_numeric_adjClose at index 1"
    assert status_of(db, "DROP")[:2] == ("history_incomplete", reason)
    assert stats["errors"] == 1
    assert stats["error_tickers"] == {"DROP": reason}
    assert stats["completed"] == 0
    assert history_of(db, "DROP") == (None, None, 0)    # nothing partial

    # Not before retry_after; then the corrected response completes it.
    tiingo.requests.clear()
    stats = run(db)["foreign_history"]
    assert tiingo.of("meta", "DROP") == [] and tiingo.of("prices", "DROP") == []
    assert stats["waiting"] == {"history_incomplete": 1}
    db.conn.execute("UPDATE eod_warmer_ticker_status SET retry_after = now() - interval '1 second'")
    del tiingo.bad[("DROP", D(2000, 1, 4))]
    stats = run(db)["foreign_history"]
    assert stats["completed"] == 1
    assert missing_dates(db, "DROP", D(2000, 1, 3), AS_OF) == []


def test_a_response_starting_after_meta_start_is_incomplete(db, tiingo):
    resolved_lines(db, "EMPT")
    instruments_row(db, "EMPT", name="Empty Co")
    store_rows(db, "EMPT", D(2024, 6, 11), D(2026, 10, 2))
    # Meta claims history from 2005, but no bar exists before the stored rows.
    tiingo.listings["EMPT"] = {"start": D(2005, 1, 3), "end": AS_OF,
                               "price_start": D(2024, 6, 11)}

    stats = run(db)["foreign_history"]
    status, detail, _ = status_of(db, "EMPT")
    assert status == "history_incomplete"
    assert detail.startswith("sessions_missing=") and detail.endswith("first=2005-01-03")
    assert stats["errors"] == 1
    assert stats["completed"] == 0


def test_failing_tickers_rotate_so_a_healthy_one_completes(db, tiingo):
    """Re-gate P2 #3: 25 covered tickers with rows fail every time (503), one
    healthy ticker sorts after them, cap 25. The failures back off, so the
    healthy ticker completes within two runs instead of never."""
    failing = [f"F{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(25)]
    resolved_lines(db, *failing, "ZHEAL")
    for t in (*failing, "ZHEAL"):
        instruments_row(db, t, name=t)
        store_rows(db, t, D(2026, 9, 1), D(2026, 10, 2))
        tiingo.listings[t] = {"start": D(2026, 1, 2), "end": AS_OF}
    for t in failing:
        tiingo.meta_status[t] = 503

    first = run(db, history_limit=25)["foreign_history"]
    # Retries are not settled: all 26 stay deferred after the first run.
    assert first["errors"] == 25 and first["deferred"] == 26
    second = run(db, history_limit=25)["foreign_history"]
    assert second["waiting"] == {"history_incomplete": 25}
    assert status_of(db, "ZHEAL")[0] == "history_complete"
    assert missing_dates(db, "ZHEAL", D(2026, 1, 2), AS_OF) == []

    # Consecutive failures back off: 12 h, then 24 h.
    def backoff():
        attempts, delay = db.one(
            "SELECT attempts, retry_after - checked_at FROM eod_warmer_ticker_status"
            " WHERE ticker = %s", (failing[0],))
        return attempts, round(delay / dt.timedelta(hours=1))

    assert backoff() == (1, 12)
    db.conn.execute("UPDATE eod_warmer_ticker_status SET retry_after = now() - interval '1 second'"
                    " WHERE status = 'history_incomplete'")
    run(db, history_limit=25)
    assert backoff() == (2, 24)


def test_cap_zero_keeps_existing_covered_tickers_in_the_ring(db, tiingo):
    """Re-gate P2 #4: a dark rollout (cap 0) must leave established ring service
    alone: covered tickers with rows, no status, outside the screener, are still
    refreshed. A covered ticker without rows is not cold-started at 745 days."""
    resolved_lines(db, "OLDA", "OLDB", "COLD")
    for t in ("OLDA", "OLDB"):
        instruments_row(db, t, name=t)
        store_rows(db, t, D(2024, 6, 11), D(2026, 10, 2))

    stats = run(db, history_limit=0)
    overlap_start = D(2026, 10, 2) - dt.timedelta(days=w.WATERMARK_OVERLAP_DAYS)
    for t in ("OLDA", "OLDB"):
        assert tiingo.of("prices", t) == [("prices", t, overlap_start, AS_OF)]
        assert history_of(db, t)[1] == AS_OF
    assert [r for r in tiingo.requests if r[1] == "COLD"] == []
    assert tiingo.of("meta") == []
    assert stats["foreign_history"] == {"source_tickers": 3, "skipped": "cap_zero"}


def test_cap_zero_warms_a_zero_row_screener_line_through_the_ring(db, tiingo):
    """Codex 4237394675: with the history phase off, a W1c-covered screener
    constituent without rows must still get the ring's 745-day cold start."""
    resolved_lines(db, "SCOV")
    db.conn.execute("INSERT INTO universe_constituents VALUES ('SCOV', 'Screener Covered', 'active')")
    screener_start = AS_OF - dt.timedelta(days=w.NEW_TICKER_LOOKBACK_DAYS)

    run(db, history_limit=0)
    assert tiingo.of("prices", "SCOV") == [("prices", "SCOV", screener_start, AS_OF)]
    assert history_of(db, "SCOV")[0] >= screener_start
    assert tiingo.of("meta") == []


def test_gate3_finding7_a_cold_screener_line_is_warmed_even_when_the_ring_aborts(db, tiingo):
    """Gate 3 #7 / Codex 4237696273: nothing keeps a covered zero-row screener
    line out of the ring any more. The breaker aborts the ring (and so the
    history phase) after COLD was served, and COLD has its 745-day rows."""
    resolved_lines(db, "COLD")
    failing = [f"SF{chr(65 + i)}" for i in range(11)]
    db.conn.execute("INSERT INTO universe_constituents VALUES ('COLD', 'Cold', 'active')")
    for t in failing:
        db.conn.execute("INSERT INTO universe_constituents VALUES (%s, %s, 'active')", (t, t))
    for t in (*w.INDEX_TICKERS, *failing):
        tiingo.price_status[t] = 429

    stats = run(db)
    screener_start = AS_OF - dt.timedelta(days=w.NEW_TICKER_LOOKBACK_DAYS)
    assert stats["aborted"].startswith("30 consecutive 429s")
    assert stats["foreign_history"] == {"source_tickers": 1, "skipped": "aborted"}
    assert tiingo.of("prices", "COLD") == [("prices", "COLD", screener_start, AS_OF)]
    assert history_of(db, "COLD")[0] >= screener_start


def test_unsettled_tickers_stay_deferred_and_a_missing_key_records_nothing(db, tiingo):
    """Codex 4237394678: deferred counts tickers that did not reach a settled
    status; with no Tiingo key nothing is recorded and every ticker stays."""
    resolved_lines(db, "NKYA", "NKYB", "NKYC")
    tiingo.no_key = True
    stats = run(db, history_limit=2)["foreign_history"]
    assert stats["processed"] == 2 and stats["completed"] == 0
    assert stats["deferred"] == 3
    assert stats["error_tickers"] == {"NKYA": "meta:not_configured", "NKYB": "meta:not_configured"}
    assert db.one("SELECT count(*) FROM eod_warmer_ticker_status")[0] == 0
    assert tiingo.requests == []


def test_out_of_range_field_values_are_unusable_not_a_crash(db, tiingo):
    """Codex 4237556581: "N/A", NaN, inf, a numeric string, a bool or a
    negative price in a required field makes the response unusable: a status
    row with backoff and an error entry, never an exception."""
    bad = {"VNAS": {"close": "N/A"}, "VNAN": {"adjClose": float("nan")},
           "VINF": {"high": float("inf")}, "VSTR": {"open": "12.5"},
           "VBOO": {"volume": True}, "VNEG": {"low": -1.0}}
    resolved_lines(db, *bad, "VZOK")
    for t, fields in bad.items():
        tiingo.bad[(t, D(2000, 1, 4))] = fields
        tiingo.listings[t] = {"start": D(2000, 1, 3), "end": AS_OF}

    stats = run(db, history_limit=250)["foreign_history"]
    assert set(stats["error_tickers"]) == set(bad)
    for t in bad:
        status, detail, _ = status_of(db, t)
        assert status == "history_incomplete" and detail.startswith("unusable_bar: ")
        assert stats["error_tickers"][t] == detail
        assert history_of(db, t) == (None, None, 0)
    assert status_of(db, "VZOK")[0] == "history_complete"


def test_status_table_schema_is_migrated_and_verified(db):
    # The shape from the PR's first revision: two statuses, no retry columns.
    db.conn.execute(
        """CREATE TABLE eod_warmer_ticker_status (
               ticker text PRIMARY KEY, source text NOT NULL,
               status text NOT NULL CHECK (status IN ('history_complete', 'tiingo_unknown')),
               detail text, history_start date,
               checked_at timestamptz NOT NULL DEFAULT now())""")
    db.conn.execute("INSERT INTO eod_warmer_ticker_status (ticker, source, status)"
                    " VALUES ('OLDX', 'w1c_foreign_listing', 'tiingo_unknown')")
    with psycopg.connect(db.dsn) as conn:
        w.ensure_status_table(conn)
        w.record_ticker_status(conn, "NEWX", w.STATUS_INCOMPLETE, detail="x", attempts=1,
                               retry_after=dt.datetime(2026, 10, 11, tzinfo=dt.UTC))
    assert db.one("SELECT status, attempts, retry_after FROM eod_warmer_ticker_status"
                  " WHERE ticker = 'OLDX'") == ("tiingo_unknown", 0, None)
    assert db.one("SELECT status, attempts FROM eod_warmer_ticker_status"
                  " WHERE ticker = 'NEWX'") == ("history_incomplete", 1)
    with pytest.raises(errors.CheckViolation):
        db.conn.execute("INSERT INTO eod_warmer_ticker_status (ticker, source, status)"
                        " VALUES ('BADX', 's', 'not_a_status')")

    # A correctly named CHECK that also allows 'bogus' is replaced exactly.
    db.conn.execute("ALTER TABLE eod_warmer_ticker_status"
                    " DROP CONSTRAINT eod_warmer_ticker_status_status_check")
    db.conn.execute(
        "ALTER TABLE eod_warmer_ticker_status ADD CONSTRAINT eod_warmer_ticker_status_status_check"
        " CHECK (status IN ('history_complete', 'tiingo_unknown', 'history_incomplete',"
        " 'adjustment_rebase_required', 'history_conflict', 'bogus'))")
    with psycopg.connect(db.dsn) as conn:
        w.ensure_status_table(conn)
    with pytest.raises(errors.CheckViolation):
        db.conn.execute("INSERT INTO eod_warmer_ticker_status (ticker, source, status)"
                        " VALUES ('BOGX', 's', 'bogus')")
    assert db.one("SELECT count(*) FROM eod_warmer_ticker_status"
                  " WHERE ticker = '__status_probe__'")[0] == 0

    # Gate 3 #8: a nullable status column is restored to NOT NULL and verified.
    db.conn.execute("ALTER TABLE eod_warmer_ticker_status ALTER COLUMN status DROP NOT NULL")
    with psycopg.connect(db.dsn) as conn:
        w.ensure_status_table(conn)
    assert db.one("SELECT is_nullable FROM information_schema.columns WHERE table_schema = %s"
                  " AND table_name = 'eod_warmer_ticker_status' AND column_name = 'status'",
                  (db.schema,)) == ("NO",)
    with pytest.raises(errors.NotNullViolation):
        db.conn.execute("INSERT INTO eod_warmer_ticker_status (ticker, source, status)"
                        " VALUES ('NULX', 's', NULL)")

    # A column of another type cannot be migrated in place: fail loud.
    db.conn.execute("DROP TABLE eod_warmer_ticker_status")
    db.conn.execute(
        """CREATE TABLE eod_warmer_ticker_status (
               ticker text PRIMARY KEY, source text NOT NULL, status text NOT NULL,
               retry_after text)""")
    with psycopg.connect(db.dsn) as conn, pytest.raises(RuntimeError, match="retry_after"):
        w.ensure_status_table(conn)


# ──────────────────────────────────────────────────────────────────────────────
# 6. compressed-chunk insert path at production chunk size
# ──────────────────────────────────────────────────────────────────────────────
def test_history_insert_into_compressed_chunks_decompresses_no_batch(db):
    n = 5000  # ~110k rows per monthly chunk: above the 100k per-DML limit
    db.conn.execute(
        "INSERT INTO instruments (ticker) SELECT 'T' || lpad(i::text, 4, '0')"
        " FROM generate_series(1, %s) i", (n,))
    db.conn.execute("INSERT INTO instruments (ticker) VALUES ('MID'), ('NEW1'), ('GAPT')")
    db.conn.execute(
        """INSERT INTO eod_prices
           SELECT 'T' || lpad(i::text, 4, '0'), d::date, 10, 11, 9, 10, 1000, 10, 11, 9, 10, 1000, 0, 1
           FROM generate_series(1, %s) i,
                generate_series(DATE '2026-03-02', DATE '2026-04-30', INTERVAL '1 day') d
           WHERE extract(isodow FROM d) < 6""", (n,))
    store_rows(db, "MID", D(2026, 4, 15), D(2026, 4, 30))   # starts mid-chunk
    gap_day = D(2026, 3, 18)
    store_rows(db, "GAPT", D(2026, 3, 2), gap_day - dt.timedelta(days=1))
    store_rows(db, "GAPT", gap_day + dt.timedelta(days=1), D(2026, 4, 30))
    compress_older_than(db, D(2100, 1, 1))
    chunks = db.q(
        """SELECT format('%%I.%%I', ch.schema_name, ch.table_name),
                  format('%%I.%%I', cc.schema_name, cc.table_name)
           FROM _timescaledb_catalog.chunk ch
           JOIN _timescaledb_catalog.chunk cc ON cc.id = ch.compressed_chunk_id
           JOIN _timescaledb_catalog.hypertable h ON h.id = ch.hypertable_id
           WHERE h.schema_name = %s AND h.table_name = 'eod_prices'""", (db.schema,))
    assert max(db.one(f"SELECT count(*) FROM {c}")[0] for c, _ in chunks) > 100_000

    def batches():
        return {c: db.one(f"SELECT count(*) FROM {cc}")[0] for c, cc in chunks}

    batches_before = batches()
    existing = ("SELECT count(*), sum(hashtext(ticker || date::text || close::text))"
                " FROM eod_prices WHERE ticker LIKE 'T%' OR (ticker = 'MID' AND date >= '2026-04-15')"
                " OR (ticker = 'GAPT' AND date <> '2026-03-18')")
    checksum_before = db.one(existing)

    def rows(ticker, a, b):
        return [(ticker, d, 1.0, 2.0, 0.5, 1.5, 7, 1.0, 2.0, 0.5, 1.5, 7, 0.0, 1.0)
                for d in bdays(a, b)]

    new_rows = rows("NEW1", D(2020, 1, 2), D(2026, 4, 30))
    mid_rows = rows("MID", D(2019, 1, 2), D(2026, 4, 14))
    strict = make_conninfo(db.dsn, options=f"-c search_path={db.schema},public"
                           " -c timescaledb.max_tuples_decompressed_per_dml_transaction=1")
    with psycopg.connect(strict) as conn:
        w.ensure_status_table(conn)
        # One promotion per ticker (~1.7k and ~1.9k rows) at a limit of 1.
        assert certify(conn, "NEW1", new_rows, D(2020, 1, 2)) == len(new_rows)
        assert certify(conn, "MID", mid_rows, D(2019, 1, 2)) == len(mid_rows)
        # A stale verdict (the snapshot it judged is gone) writes nothing.
        stale = v.Verdict(v.VERDICT_LOAD, "stale", tuple(mid_rows), v.stored_digest({}))
        assert w.promote(conn, "MID", stale, through=AS_OF, history_start=D(2019, 1, 2)) is None
        # Positive control: the ring's DO UPDATE on one existing compressed key
        # must decompress its batch, so the limit trips. The measurement sees it.
        with pytest.raises(errors.ConfigurationLimitExceeded):
            w.upsert_eod_prices(conn, rows("T0001", D(2026, 3, 3), D(2026, 3, 3)))

    assert batches() == batches_before
    assert db.one(existing) == checksum_before
    # Only history keys sit in the compressed chunks' uncompressed heaps: a
    # decompressed batch would have moved existing rows there.
    assert sum(db.one(f"SELECT count(*) FROM ONLY {c}")[0] for c, _ in chunks) > 0
    for c, _ in chunks:
        assert db.one(
            f"SELECT count(*) FROM ONLY {c} WHERE ticker NOT IN ('NEW1', 'MID')"
            " OR (ticker = 'MID' AND date >= DATE '2026-04-15')")[0] == 0

    # An interior gap is inside GAPT's own compressed batch: at the production
    # limit only that batch is decompressed, bounded by GAPT's rows in the chunk.
    with psycopg.connect(db.dsn) as conn:
        assert certify(conn, "GAPT", rows("GAPT", gap_day, gap_day), D(2026, 3, 2)) == 1
    gap_chunk = _chunk_of(db, "GAPT", gap_day)
    heap_tickers = dict(db.q(f"SELECT ticker, count(*) FROM ONLY {gap_chunk} GROUP BY ticker"))
    gapt_in_chunk = db.one(f"SELECT count(*) FROM {gap_chunk} WHERE ticker = 'GAPT'")[0]
    assert set(heap_tickers) <= {"GAPT", "NEW1", "MID"}
    assert heap_tickers["GAPT"] <= gapt_in_chunk < 100
    # The columnstore policy's recompression absorbs the partial chunks.
    for c, _ in chunks:
        db.conn.execute("SELECT compress_chunk(%s::regclass)", (c,))
    assert sum(db.one(f"SELECT count(*) FROM ONLY {c}")[0] for c, _ in chunks) == 0
    assert db.one(existing) == checksum_before
    assert history_of(db, "MID")[:2] == (D(2019, 1, 2), D(2026, 4, 30))


# ──────────────────────────────────────────────────────────────────────────────
# 7. Tiingo-unknown tickers
# ──────────────────────────────────────────────────────────────────────────────
def test_tiingo_unknown_is_recorded_reported_and_rechecked_later(db, tiingo):
    resolved_lines(db, "UNKN", "NODT", "PNF", "OKAY")
    tiingo.unknown.add("UNKN")
    tiingo.listings["NODT"] = {"start": D(2000, 1, 3), "end": AS_OF, "no_start": True}
    tiingo.price_status["PNF"] = 404

    stats = run(db)
    history = stats["foreign_history"]
    assert "aborted" not in stats
    assert history["errors"] == 0
    assert history["tiingo_unknown"] == 3
    assert history["tiingo_unknown_tickers"] == ["NODT", "PNF", "UNKN"]
    assert status_of(db, "UNKN")[:2] == ("tiingo_unknown", "not_found")
    assert status_of(db, "NODT")[:2] == ("tiingo_unknown", "no_start_date")
    assert status_of(db, "PNF")[:2] == ("tiingo_unknown", "prices_not_found")
    assert db.one("SELECT count(*) FROM instruments WHERE ticker IN ('UNKN', 'NODT')")[0] == 0
    assert status_of(db, "OKAY")[0] == "history_complete"

    tiingo.requests.clear()
    stats = run(db)
    assert [r for r in tiingo.requests if r[1] in ("UNKN", "NODT", "PNF")] == []
    assert stats["foreign_history"]["waiting"] == {"tiingo_unknown": 3}

    db.conn.execute("UPDATE eod_warmer_ticker_status SET retry_after = now() - interval '1 second'"
                    " WHERE status = 'tiingo_unknown'")
    tiingo.requests.clear()
    run(db)
    assert sorted(r[1] for r in tiingo.of("meta")) == ["NODT", "PNF", "UNKN"]


# ──────────────────────────────────────────────────────────────────────────────
# 8. per-run cap, resume, budget
# ──────────────────────────────────────────────────────────────────────────────
def test_cap_bounds_each_run_and_the_backlog_resumes(db, tiingo):
    names = ["CAPA", "CAPB", "CAPC", "CAPD", "CAPE"]
    resolved_lines(db, *names)
    seen = []
    for expected_completed, expected_deferred in ((2, 3), (2, 1), (1, 0), (0, 0)):
        tiingo.requests.clear()
        stats = run(db, history_limit=2)["foreign_history"]
        history_requests = [r for t in names for r in tiingo.history_requests(t)]
        assert len(history_requests) <= 2 * 2
        assert (stats["completed"], stats["deferred"]) == (expected_completed, expected_deferred)
        seen += sorted({r[1] for r in tiingo.of("meta") if r[1] in names})
    assert seen == names
    assert all(missing_dates(db, t, D(1993, 1, 29), AS_OF) == [] for t in names)


def test_default_cap_is_the_small_rollout_value(db, tiingo, monkeypatch):
    names = [f"D{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(w.HISTORY_TICKERS_PER_RUN + 3)]
    resolved_lines(db, *names)
    tiingo.listings.update({t: {"start": D(2025, 1, 2), "end": AS_OF} for t in names})
    stats = w.run(db.dsn, calc_date=AS_OF.isoformat())["foreign_history"]
    assert (stats["processed"], stats["deferred"]) == (w.HISTORY_TICKERS_PER_RUN, 3)
    monkeypatch.setenv(w.HISTORY_LIMIT_ENV, "250")
    stats = w.run(db.dsn, calc_date=AS_OF.isoformat())["foreign_history"]
    assert (stats["completed"], stats["deferred"]) == (3, 0)


def test_budget_abort_in_the_history_phase_is_reported_and_resumes(db, tiingo):
    names = [f"BUD{chr(65 + i)}" for i in range(12)]
    resolved_lines(db, *names)
    for t in names:
        tiingo.meta_status[t] = 429

    stats = run(db)
    assert stats["aborted"].startswith("30 consecutive 429s")
    assert stats["foreign_history"]["aborted"] == stats["aborted"]
    assert stats["foreign_history"]["completed"] == 0
    assert db.one("SELECT count(*) FROM eod_warmer_ticker_status")[0] == 0

    tiingo.meta_status.clear()
    stats = run(db)
    assert "aborted" not in stats
    assert stats["foreign_history"]["completed"] == len(names)


def test_ring_budget_abort_skips_the_history_phase(db, tiingo):
    resolved_lines(db, "HIST")
    db.conn.execute("INSERT INTO universe_constituents SELECT 'S' || i, 'S', 'active'"
                    " FROM generate_series(1, 5) i")
    for t in (*w.INDEX_TICKERS, *(f"S{i}" for i in range(1, 6))):
        tiingo.price_status[t] = 429

    stats = run(db)
    assert stats["aborted"].startswith("30 consecutive 429s")
    assert stats["foreign_history"] == {"source_tickers": 1, "skipped": "aborted"}
    assert [r for r in tiingo.requests if r[1] == "HIST"] == []


# ──────────────────────────────────────────────────────────────────────────────
# Robustness battery: provider responses and state, through run()
# ──────────────────────────────────────────────────────────────────────────────
REPLAY = D(2020, 6, 30)


def completion_of(db, ticker):
    return db.one("SELECT status, complete_through FROM eod_warmer_ticker_status"
                  " WHERE ticker = %s", (ticker,))


def test_battery_historical_run_is_capped_at_as_of_and_reverified_later(db, tiingo):
    """Codex 4237640082: a 2020-06-30 replay whose metadata ends in 2026 inserts
    nothing after 2020-06-30 and completes only through it; a real-date run
    re-verifies the tail."""
    resolved_lines(db, "HREP")
    stats = w.run(db.dsn, calc_date=REPLAY.isoformat(), history_limit=250)
    assert ("prices", "HREP", D(1993, 1, 29), REPLAY) in tiingo.requests
    assert history_of(db, "HREP")[:2] == (D(1993, 1, 29), REPLAY)
    assert completion_of(db, "HREP") == ("history_complete", REPLAY)
    assert stats["foreign_history"]["completed"] == 1

    tiingo.requests.clear()
    stats = run(db)["foreign_history"]
    assert ("prices", "HREP", D(1993, 1, 29), AS_OF) in tiingo.requests
    assert completion_of(db, "HREP") == ("history_complete", None)
    assert missing_dates(db, "HREP", D(1993, 1, 29), AS_OF) == []


def test_battery_stored_rows_after_as_of_are_out_of_scope_and_untouched(db, tiingo):
    resolved_lines(db, "HPOS")
    instruments_row(db, "HPOS", name="Post As-of Co")
    store_rows(db, "HPOS", D(2010, 1, 4), D(2026, 10, 2))
    tiingo.listings["HPOS"] = {"start": D(2005, 1, 3), "end": AS_OF}
    after = snapshot(db, "HPOS", REPLAY + dt.timedelta(days=1), AS_OF)

    stats = w.run(db.dsn, calc_date=REPLAY.isoformat(), history_limit=250)
    assert "aborted" not in stats
    assert completion_of(db, "HPOS") == ("history_complete", REPLAY)
    assert missing_dates(db, "HPOS", D(2005, 1, 3), REPLAY) == []
    assert snapshot(db, "HPOS", REPLAY + dt.timedelta(days=1), AS_OF) == after


def test_battery_c_an_empty_list_while_meta_has_history(db, tiingo):
    resolved_lines(db, "EMLS")
    instruments_row(db, "EMLS", name="Empty List Co")
    store_rows(db, "EMLS", D(2026, 9, 1), D(2026, 10, 2))
    tiingo.raw_prices["EMLS"] = []
    before = snapshot(db, "EMLS", D(2000, 1, 1), AS_OF)

    run(db)
    assert status_of(db, "EMLS")[:2] == ("history_incomplete", "empty_window")
    assert snapshot(db, "EMLS", D(2000, 1, 1), AS_OF) == before


def test_battery_f_a_stored_row_tiingo_no_longer_serves(db, tiingo):
    resolved_lines(db, "GONE")
    instruments_row(db, "GONE", name="Gone Row Co")
    store_rows(db, "GONE", D(2026, 9, 1), D(2026, 10, 2))
    tiingo.listings["GONE"] = {"start": D(2026, 1, 2), "end": AS_OF}
    tiingo.omit.add(("GONE", D(2026, 9, 23)))

    run(db)
    assert status_of(db, "GONE")[:2] == (
        "history_incomplete", "stored_sessions_missing=1 first=2026-09-23")
    assert history_of(db, "GONE")[0] == D(2026, 9, 1)


def test_battery_h_stale_instruments_dates_do_not_bound_the_interval(db, tiingo):
    resolved_lines(db, "STAL")
    # Non-null but stale: the worker never overwrites it, and never trusts it.
    instruments_row(db, "STAL", name="Stale Co", exchange="NYSE", start=D(2015, 6, 1),
                    end=D(2020, 1, 2))
    tiingo.listings["STAL"] = {"start": D(2005, 1, 3), "end": AS_OF}
    run(db)
    assert ("prices", "STAL", D(2005, 1, 3), AS_OF) in tiingo.requests
    assert history_of(db, "STAL")[:2] == (D(2005, 1, 3), AS_OF)
    assert db.one("SELECT tiingo_start_date, tiingo_end_date FROM instruments"
                  " WHERE ticker = 'STAL'") == (D(2015, 6, 1), D(2020, 1, 2))


def test_battery_h_meta_without_an_end_date_is_retried(db, tiingo):
    resolved_lines(db, "NOEN")
    tiingo.listings["NOEN"] = {"start": D(2005, 1, 3), "end": AS_OF, "no_end": True}
    stats = run(db)["foreign_history"]
    assert status_of(db, "NOEN")[:2] == ("history_incomplete", "meta_without_end_date")
    assert stats["error_tickers"] == {"NOEN": "meta_without_end_date"}
    assert tiingo.of("prices", "NOEN") == []


def test_battery_i_a_concurrent_insert_between_verification_and_load(db, tiingo, monkeypatch):
    """Another writer adds a row after the verification read: the verdict no
    longer holds, so the load (rows and status) is not written; the next pass
    verifies the new state."""
    resolved_lines(db, "RACE")
    instruments_row(db, "RACE", name="Race Co")
    store_rows(db, "RACE", D(2026, 9, 1), D(2026, 10, 2))
    tiingo.listings["RACE"] = {"start": D(2026, 1, 2), "end": AS_OF}
    real = w.validate_series

    def racing(ticker, *args, **kwargs):
        verdict = real(ticker, *args, **kwargs)
        if ticker == "RACE":
            store_rows(db, "RACE", D(2026, 1, 2), D(2026, 1, 2))   # the other writer
        return verdict

    monkeypatch.setattr(w, "validate_series", racing)
    run(db)
    assert status_of(db, "RACE")[:2] == ("history_incomplete", "stored_rows_changed_during_pass")
    assert history_of(db, "RACE")[0] == D(2026, 1, 2)
    assert len(missing_dates(db, "RACE", D(2026, 1, 2), D(2026, 8, 31))) > 100  # ours rolled back

    monkeypatch.setattr(w, "validate_series", real)
    db.conn.execute("UPDATE eod_warmer_ticker_status SET retry_after = now() - interval '1 second'")
    run(db)
    assert status_of(db, "RACE")[0] == "history_complete"
    assert missing_dates(db, "RACE", D(2026, 1, 2), AS_OF) == []


def test_battery_j_an_aged_completion_is_reverified_against_a_moved_start(db, tiingo):
    resolved_lines(db, "AGED")
    tiingo.listings["AGED"] = {"start": D(2010, 1, 4), "end": AS_OF}
    run(db)
    assert completion_of(db, "AGED") == ("history_complete", None)

    # Tiingo now serves five more years; within REVERIFY_DAYS nothing happens...
    tiingo.listings["AGED"] = {"start": D(2005, 1, 3), "end": AS_OF}
    tiingo.requests.clear()
    run(db)
    assert tiingo.of("meta", "AGED") == []
    # ...after it, the pass re-verifies and loads the earlier, coherent prefix.
    db.conn.execute("UPDATE eod_warmer_ticker_status SET checked_at = now() - %s",
                    (dt.timedelta(days=w.REVERIFY_DAYS + 1),))
    run(db)
    assert completion_of(db, "AGED") == ("history_complete", None)
    assert history_of(db, "AGED")[0] == D(2005, 1, 3)
    assert missing_dates(db, "AGED", D(2005, 1, 3), AS_OF) == []


# ──────────────────────────────────────────────────────────────────────────────
# Gate 3: whole-ticker validation and the single promotion path, end to end
# ──────────────────────────────────────────────────────────────────────────────
G_START = D(2026, 8, 3)   # 49 XNYS sessions to AS_OF


def gate_line(db, tiingo, ticker, *, stored=False):
    resolved_lines(db, ticker)
    instruments_row(db, ticker, name=ticker)
    if stored:
        store_rows(db, ticker, G_START, AS_OF)
    tiingo.listings[ticker] = {"start": G_START, "end": AS_OF}


def test_gate3_finding1_endpoint_only_and_off_session_responses_write_nothing(db, tiingo):
    gate_line(db, tiingo, "ENDP")
    tiingo.raw_prices["ENDP"] = [tiingo.bar("ENDP", G_START), tiingo.bar("ENDP", AS_OF)]
    gate_line(db, tiingo, "SUND")
    tiingo.raw_prices["SUND"] = ([tiingo.bar("SUND", d) for d in bdays(G_START, AS_OF)]
                                 + [tiingo.bar("SUND", D(2026, 8, 9))])
    gate_line(db, tiingo, "HOLE")
    tiingo.omit.add(("HOLE", D(2026, 9, 15)))
    run(db)
    assert status_of(db, "ENDP")[:2] == ("history_incomplete", "sessions_missing=47 first=2026-08-04")
    assert status_of(db, "SUND")[:2] == ("history_incomplete", "off_session_bar: 1 first=2026-08-09")
    assert status_of(db, "HOLE")[:2] == ("history_incomplete", "sessions_missing=1 first=2026-09-15")
    for t in ("ENDP", "SUND", "HOLE"):
        assert history_of(db, t) == (None, None, 0)


def test_gate3_finding2_a_verified_store_that_changes_mid_pass_is_not_certified(db, tiingo):
    """All 49 sessions stored; while the price request is in flight another
    transaction doubles the last stored close. The provider series is the
    original one, so the verdict is complete_nothing_to_insert — and promote
    must still refuse it."""
    gate_line(db, tiingo, "MIDC", stored=True)

    def other_writer(start, end):
        if start == G_START:
            db.conn.execute("UPDATE eod_prices SET close = close * 2"
                            " WHERE ticker = 'MIDC' AND date = %s", (AS_OF,))

    tiingo.hooks["MIDC"] = other_writer
    stats = run(db)["foreign_history"]
    assert status_of(db, "MIDC")[:2] == ("history_incomplete", "stored_rows_changed_during_pass")
    assert stats["completed"] == 0 and stats["verified_without_insert"] == 0


def test_gate3_findings3_4_incoherent_bar_and_malformed_date_write_nothing(db, tiingo):
    gate_line(db, tiingo, "INCO")
    tiingo.bad[("INCO", D(2026, 8, 20))] = {"high": 1.0, "low": 100.0, "adjHigh": 1.0,
                                            "adjLow": 100.0}
    gate_line(db, tiingo, "GARB")
    tiingo.bad[("GARB", D(2026, 8, 10))] = {"date": "2026-08-10garbage"}
    stats = run(db)["foreign_history"]
    assert status_of(db, "INCO")[:2] == ("history_incomplete", "unusable_bar: ohlc_order at index 13")
    assert status_of(db, "GARB")[:2] == ("history_incomplete", "unusable_bar: malformed_date at index 5")
    assert stats["completed"] == 0
    assert history_of(db, "INCO") == history_of(db, "GARB") == (None, None, 0)


def test_gate3_finding5_a_mixed_adjusted_basis_is_never_complete(db, tiingo):
    gate_line(db, tiingo, "MIXB", stored=True)
    db.conn.execute("UPDATE eod_prices SET adj_open = adj_open / 2, adj_high = adj_high / 2,"
                    " adj_low = adj_low / 2, adj_close = adj_close / 2"
                    " WHERE ticker = 'MIXB' AND date = %s", (AS_OF - dt.timedelta(days=7),))
    tiingo.listings["MIXB"]["end"] = AS_OF - dt.timedelta(days=7)   # keep the ring off it
    run(db)
    status, detail, _ = status_of(db, "MIXB")
    assert status == "adjustment_rebase_required"
    assert detail.startswith("adjusted_moved: ratio=2.000000 on 1/")


@pytest.mark.parametrize("cap", [0, 25])
def test_gate3_finding6_the_entrypoint_migrates_an_older_status_table(db, tiingo, cap):
    db.conn.execute(
        """CREATE TABLE eod_warmer_ticker_status (
               ticker text PRIMARY KEY, source text NOT NULL, status text NOT NULL,
               detail text, history_start date, retry_after timestamptz,
               attempts integer NOT NULL DEFAULT 0,
               checked_at timestamptz NOT NULL DEFAULT now())""")
    resolved_lines(db, "MIGR")
    stats = run(db, history_limit=cap)
    assert "aborted" not in stats
    # Migrated by the entrypoint even with the history phase off (dark rollout).
    assert db.one("SELECT data_type, is_nullable FROM information_schema.columns"
                  " WHERE table_schema = %s AND table_name = 'eod_warmer_ticker_status'"
                  " AND column_name = 'complete_through'", (db.schema,)) == ("date", "YES")
    expected = ("history_complete", None) if cap else None
    assert db.one("SELECT status, complete_through FROM eod_warmer_ticker_status"
                  " WHERE ticker = 'MIGR'") == expected


def test_gate3_completion_and_history_rows_are_written_only_by_promote(db, tiingo, monkeypatch):
    """Dynamic half of the single-promotion-path rule: every statement the real
    entrypoint sends is inspected; the history insert and any history_complete
    status write must happen inside promote()."""
    gate_line(db, tiingo, "PLOD")                    # a load
    gate_line(db, tiingo, "PVER", stored=True)       # complete_nothing_to_insert
    gate_line(db, tiingo, "PBAD")                    # a refusal
    tiingo.omit.add(("PBAD", D(2026, 9, 15)))
    depth = {"promote": 0}
    seen = collections.Counter()
    real_promote = w.promote

    def tracked(*args, **kwargs):
        depth["promote"] += 1
        try:
            return real_promote(*args, **kwargs)
        finally:
            depth["promote"] -= 1

    def guard(query, params):
        text = query.as_string(None) if hasattr(query, "as_string") else str(query)
        if text.strip() == w.EOD_HISTORY_INSERT_SQL.strip():
            assert depth["promote"], "history insert outside promote()"
            seen["history_insert"] += 1
        if text.strip() == w.RECORD_STATUS_SQL.strip():
            rows = params if isinstance(params, list) else [params]
            for row in rows:
                if row and w.STATUS_COMPLETE in row:
                    assert depth["promote"], "history_complete written outside promote()"
                    seen["complete_write"] += 1

    real_execute, real_executemany = psycopg.Cursor.execute, psycopg.Cursor.executemany

    def execute(self, query, params=None, **kwargs):
        guard(query, params)
        return real_execute(self, query, params, **kwargs)

    def executemany(self, query, params_seq, **kwargs):
        params_seq = list(params_seq)
        guard(query, params_seq)
        return real_executemany(self, query, params_seq, **kwargs)

    monkeypatch.setattr(w, "promote", tracked)
    monkeypatch.setattr(psycopg.Cursor, "execute", execute)
    monkeypatch.setattr(psycopg.Cursor, "executemany", executemany)
    run(db)
    assert seen["history_insert"] >= 1 and seen["complete_write"] == 2
    assert status_of(db, "PLOD")[0] == status_of(db, "PVER")[0] == "history_complete"
    assert status_of(db, "PBAD")[0] == "history_incomplete"


# ──────────────────────────────────────────────────────────────────────────────
# Gate 4: the whole stored row, retained-bar validation, the calendar domain
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(("col", "value"), [
    ("volume", -1), ("adj_volume", -1), ("div_cash", 0.5), ("split_factor", 0)])
def test_gate4_promote_refuses_a_concurrent_change_to_any_stored_column(db, col, value):
    """Gate 4 repro: after the verification snapshot, another writer (Light's
    ingest order: the instruments upsert, then the price row, one transaction)
    changes a column outside OHLC and commits. promote() re-reads all twelve
    columns under its lock and refuses."""
    instruments_row(db, "SNAP", name="baseline")
    store_rows(db, "SNAP", AS_OF, AS_OF)
    with psycopg.connect(db.dsn) as conn, psycopg.connect(db.dsn) as writer:
        w.ensure_status_table(conn)
        conn.commit()
        snap = w._stored_rows(conn, "SNAP", through=AS_OF)
        provider = [{"date": AS_OF.isoformat(), **{key: snap[AS_OF][c] for c, key in v.EOD_FIELDS}}]
        verdict = v.validate_series("SNAP", (AS_OF, AS_OF), provider, snap, v.xnys_calendar())
        assert verdict.status == v.VERDICT_COMPLETE
        writer.execute("UPDATE instruments SET name = 'light metadata' WHERE ticker = 'SNAP'")
        writer.execute(sql.SQL("UPDATE eod_prices SET {} = %s WHERE ticker = 'SNAP'").format(
            sql.Identifier(col)), (value,))
        writer.commit()
        assert w.promote(conn, "SNAP", verdict, through=AS_OF, history_start=AS_OF) is None
    assert status_of(db, "SNAP") is None


def test_gate4_promote_refuses_a_concurrent_change_through_the_entrypoint(db, tiingo):
    """The same, end to end: the change lands while the price request is in
    flight; the ticker is recorded stored_rows_changed_during_pass."""
    gate_line(db, tiingo, "SNPE", stored=True)

    def other_writer(start, end):
        if start == G_START:
            db.conn.execute("UPDATE eod_prices SET div_cash = 0.5"
                            " WHERE ticker = 'SNPE' AND date = %s", (D(2026, 8, 20),))

    tiingo.hooks["SNPE"] = other_writer
    run(db)
    assert status_of(db, "SNPE")[:2] == ("history_incomplete", "stored_rows_changed_during_pass")


@pytest.mark.parametrize(("update", "problem"), [
    ("high = open * 0.9999995", "ohlc_order"),           # within 1e-6 of Tiingo, incoherent
    ("close = 'Infinity'", "non_numeric_close"),
    ("adj_close = 'Infinity'", "non_numeric_adjClose"),
    ("high = 'Infinity'", "non_numeric_high"),
    ("volume = -1", "negative"),
    ("split_factor = 0", "non_positive"),
])
def test_gate4_an_invalid_retained_stored_bar_is_a_conflict(db, tiingo, update, problem):
    gate_line(db, tiingo, "SINV", stored=True)
    day = D(2026, 8, 20)                                  # outside the ring's overlap
    db.conn.execute(f"UPDATE eod_prices SET {update} WHERE ticker = 'SINV' AND date = %s", (day,))
    stats = run(db)["foreign_history"]
    assert status_of(db, "SINV")[:2] == ("history_conflict", f"stored_bar_invalid: {problem} on {day}")
    assert stats["fail_closed"] == 1 and stats["completed"] == 0


def test_gate4_history_before_1970_is_not_requested_and_not_an_error(db, tiingo):
    """Tiingo's startDate is 1962-01-02: the pass requests from 1970-01-01 (the
    certified calendar domain), loads from the first session (1970-01-02),
    certifies and records the effective start and Tiingo's own.

    The history phase runs as of 1970-12-31 (``cover_foreign_history``, the
    function run() calls; run()'s W1c source set has no 1970 listings) so the
    promotion touches 12 monthly chunks: a 1970-to-date promotion locks ~690
    chunks in one transaction, which fits production's lock table (256 x 100
    connections) but not the CI service container's (128 x 25)."""
    instruments_row(db, "OLDT", name="Old Co")
    tiingo.listings["OLDT"] = {"start": D(1962, 1, 2), "end": AS_OF}
    as_of = D(1970, 12, 31)
    with psycopg.connect(db.dsn) as conn, w.TiingoClient() as client:
        w.ensure_status_table(conn)
        conn.commit()
        stats = w.cover_foreign_history(conn, client, ["OLDT"], as_of=as_of, cap=25)
    assert ("prices", "OLDT", D(1970, 1, 1), as_of) in tiingo.requests
    assert not [r for r in tiingo.of("prices", "OLDT") if r[2] < D(1970, 1, 1)]
    n = len(bdays(D(1970, 1, 1), as_of))
    assert history_of(db, "OLDT") == (D(1970, 1, 2), as_of, n)
    assert status_of(db, "OLDT") == (
        "history_complete",
        f"inserted {n} missing sessions; history_from=1970-01-01;"
        " tiingo_start=1962-01-02 (earlier history not certified)",
        D(1970, 1, 1))
    assert completion_of(db, "OLDT") == ("history_complete", as_of)
    assert stats["completed"] == 1 and "error_tickers" not in stats


# ──────────────────────────────────────────────────────────────────────────────
# Gate 5 (head 56f7047a): transaction scope, chunk footprint, discovery isolation
# ──────────────────────────────────────────────────────────────────────────────
IDLE = psycopg.pq.TransactionStatus.IDLE


def relation_locks(db, pid):
    return db.q("SELECT mode, count(*) FROM pg_locks WHERE pid = %s AND locktype = 'relation'"
                " GROUP BY mode ORDER BY mode", (pid,))


class LockProbe:
    """A Tiingo stand-in that records, on every call, the worker connection's
    transaction state and relation locks, and (once, during the first price
    request) tries to compress a historical chunk with a 100 ms lock timeout."""

    def __init__(self, db, conn, fake, *, prices="success_new"):
        self.db, self.conn, self.fake, self.prices = db, conn, fake, prices
        self.seen = []
        self.compressed = None

    def _look(self, call):
        self.seen.append((call, self.conn.info.transaction_status,
                          relation_locks(self.db, self.conn.info.backend_pid)))

    def fetch_meta_result(self, ticker):
        self._look("meta")
        lst = self.fake.listing(ticker)
        return "found", {"name": f"{ticker} Holdings", "exchangeCode": "NYSE",
                         "startDate": lst["start"].isoformat(), "endDate": lst["end"].isoformat()}

    def fetch_daily_bars_result(self, ticker, first, last):
        self._look("prices")
        if self.compressed is None:
            chunk = self.db.one(
                "SELECT format('%%I.%%I', chunk_schema, chunk_name)"
                " FROM timescaledb_information.chunks WHERE hypertable_schema = %s"
                " AND hypertable_name = 'eod_prices' AND NOT is_compressed"
                " ORDER BY range_start LIMIT 1", (self.db.schema,))[0]
            self.db.conn.execute("SET lock_timeout = '100ms'")
            try:
                self.db.conn.execute("SELECT compress_chunk(%s::regclass)", (chunk,))
                self.compressed = True
            except errors.LockNotAvailable:
                self.compressed = False
            finally:
                self.db.conn.execute("SET lock_timeout = 0")
        if self.prices != "success_new":
            return self.prices, []
        return "success_new", [self.fake.bar(ticker, d) for d in bdays(first, last)]


def hold_scenario(db, module, ticker, prices):
    """One ticker with twelve historical monthly chunks, verified by ``module``'s
    ``cover_foreign_history`` against a probe. Returns the problems found."""
    instruments_row(db, ticker)
    store_rows(db, ticker, D(2025, 10, 1), D(2026, 9, 30))
    fake = FakeTiingo()
    fake.listings[ticker] = {"start": D(2025, 10, 1), "end": AS_OF}
    chunks = db.one("SELECT count(*) FROM timescaledb_information.chunks"
                    " WHERE hypertable_schema = %s AND hypertable_name = 'eod_prices'", (db.schema,))[0]
    problems = [] if chunks >= 12 else [f"only {chunks} chunks"]
    with psycopg.connect(db.dsn) as conn:
        probe = LockProbe(db, conn, fake, prices=prices)
        stats = module.cover_foreign_history(conn, probe, [ticker], as_of=AS_OF, cap=5)
        after = (conn.info.transaction_status, relation_locks(db, conn.info.backend_pid))
    for call, status, locks in probe.seen:
        if status != IDLE or locks:
            problems.append(f"{call}: {status.name}, {locks}")
    if probe.compressed is not True:
        problems.append("a concurrent compress_chunk() was blocked")
    if after != (IDLE, []):
        problems.append(f"the phase returned {after[0].name} holding {after[1]}")
    return problems, stats


@pytest.mark.parametrize("prices", ["success_new", "rate_limited", "not_configured"])
def test_gate5_finding1_the_worker_holds_no_chunk_lock_during_any_request(db, prices):
    problems, stats = hold_scenario(db, w, "OLDT", prices)
    assert problems == []
    if prices == "success_new":
        assert stats["completed"] == 1 and stats["errors"] == 0
    else:
        assert stats["errors"] == 1 and stats["completed"] == 0


def test_gate5_finding1_the_held_snapshot_mutant_is_killed_by_the_lock_scenario(db):
    """Re-introducing the held snapshot (W01) leaves the worker INTRANS with
    its chunk locks across the price request and blocks the compression."""
    name, old, new, _ = next(m for m in WARMER_MUTANTS if m[0].startswith("W01_"))
    problems, _ = hold_scenario(db, warmer_mutant(name, old, new), "OLDT", "rate_limited")
    assert any("INTRANS" in p for p in problems), problems
    assert "a concurrent compress_chunk() was blocked" in problems
    assert any(p.startswith("the phase returned INTRANS") for p in problems), problems


def put(db, ticker, day):
    db.conn.execute("INSERT INTO eod_prices VALUES (%s, %s, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1)",
                    (ticker, day))


def chunk_span(db, day):
    """(first date, exclusive end date) of the chunk holding ``day``, from the catalog."""
    return db.one(
        "SELECT (range_start AT TIME ZONE 'UTC')::date, (range_end AT TIME ZONE 'UTC')::date"
        " FROM timescaledb_information.chunks WHERE hypertable_schema = %s"
        " AND hypertable_name = 'eod_prices'"
        " AND range_start <= (%s::date)::timestamp AT TIME ZONE 'UTC'"
        " AND range_end > (%s::date)::timestamp AT TIME ZONE 'UTC'", (db.schema, day, day))


def test_gate5_chunk_footprint_counts_only_the_chunks_of_the_read_range(db):
    instruments_row(db, "CHK")
    for day in (D(1962, 3, 5), D(1970, 1, 5), AS_OF, D(2026, 12, 7)):
        put(db, "CHK", day)
    first = chunk_span(db, D(1970, 1, 5))
    last_start, _ = chunk_span(db, AS_OF)
    assert first == (D(1970, 1, 1), D(1970, 1, 31)) and last_start > D(1970, 2, 1)
    for tz in ("UTC", "America/Los_Angeles", "Asia/Tokyo"):
        with psycopg.connect(db.dsn) as conn:
            conn.execute(sql.SQL("SET TIME ZONE {}").format(sql.Literal(tz)))
            count = lambda a, b: w.chunk_footprint(conn, a, b)       # noqa: E731
            assert count(D(1970, 1, 1), AS_OF) == 2, tz         # not 1962, not after as_of
            assert count(D(1961, 1, 1), D(2030, 1, 1)) == 4, tz
            assert count(D(1970, 1, 30), AS_OF) == 2, tz        # the last day of the first chunk
            assert count(D(1970, 1, 31), AS_OF) == 1, tz        # its end is exclusive
            assert count(D(1970, 1, 1), last_start - dt.timedelta(days=1)) == 1, tz
            assert count(D(1970, 1, 1), last_start) == 2, tz    # the first day of the last
            conn.rollback()


def test_gate5_chunk_footprint_is_zero_without_a_hypertable(db):
    with psycopg.connect(db.dsn) as conn:
        conn.execute("DROP TABLE eod_prices CASCADE")
        assert w.chunk_footprint(conn, D(1970, 1, 1), AS_OF) == 0       # no such table
        conn.execute("CREATE TABLE eod_prices (ticker text, date date)")
        assert w.chunk_footprint(conn, D(1970, 1, 1), AS_OF) == 0       # a plain table
        conn.rollback()


class NoCalls:
    def fetch_meta_result(self, ticker):
        raise AssertionError("a request was made")

    fetch_daily_bars_result = fetch_meta_result


def test_gate5_finding_chunk_footprint_ceiling_at_production_scale(db):
    """806 monthly chunks (production has 689 in the read range): one more than
    the ceiling's 800 refuses the pass before any request and any read."""
    instruments_row(db, "BIG")
    as_of = D(2036, 12, 31)
    try:
        # A few dozen chunks a statement: one transaction never needs more locks
        # than the smallest lock table (the CI container's) holds.
        for first in range(0, 806, 50):
            db.conn.execute(
                "INSERT INTO eod_prices SELECT 'BIG', d::date, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1"
                " FROM generate_series(date '1970-01-05' + 30 * %s, date '1970-01-05' + 30 * %s,"
                " interval '30 days') d", (first, min(first + 49, 805)))
        with psycopg.connect(db.dsn) as conn:
            assert w.chunk_footprint(conn, D(1970, 1, 1), as_of) == 806 > w.MAX_VERIFICATION_CHUNKS
            assert w.chunk_footprint(conn, D(1970, 1, 1), D(2010, 1, 1)) < w.MAX_VERIFICATION_CHUNKS
            stats = w.cover_foreign_history(conn, NoCalls(), ["BIG"], as_of=as_of, cap=5)
            assert relation_locks(db, conn.info.backend_pid) == []
            assert stats["chunk_footprint_exceeded"] == 1 and stats["errors"] == 1
            assert stats["meta_requests"] == 0 and stats["history_fetches"] == 0
            assert stats["error_tickers"]["BIG"].startswith("chunk_footprint_exceeded: 806 chunks")
            with pytest.raises(w.ChunkFootprintExceeded):
                w.promote(conn, "BIG", v.Verdict(v.VERDICT_COMPLETE, "t", (), v.stored_digest({})),
                          through=as_of, history_start=D(1970, 1, 1))
        assert status_of(db, "BIG")[0] == "history_incomplete"
        assert status_of(db, "BIG")[1].startswith("chunk_footprint_exceeded")
    finally:                  # the fixture's DROP SCHEMA could not lock 806 chunks at once
        for year in range(1980, 2041, 4):
            db.conn.execute("SELECT drop_chunks('eod_prices', older_than => %s::date)",
                            (D(year, 1, 1),))


def test_gate5_chunk_footprint_ceiling_through_the_entrypoint(db, tiingo, monkeypatch):
    resolved_lines(db, "WIDE")
    instruments_row(db, "WIDE")
    store_rows(db, "WIDE", D(2026, 5, 1), D(2026, 9, 30))
    tiingo.listings["WIDE"] = {"start": D(2026, 5, 1), "end": AS_OF}
    run(db, history_limit=0)                      # the ring first: it may add the newest chunk
    with psycopg.connect(db.dsn) as conn:
        n = w.chunk_footprint(conn, v.CALENDAR_SUPPORTED_FROM, AS_OF)
    assert n >= 5
    monkeypatch.setattr(w, "MAX_VERIFICATION_CHUNKS", n - 1)
    tiingo.requests.clear()
    stats = run(db)["foreign_history"]
    assert stats["chunk_footprint_exceeded"] == 1 and stats["errors"] == 1
    assert stats["completed"] == 0 and tiingo.history_requests("WIDE") == []
    assert status_of(db, "WIDE")[0] == "history_incomplete"
    assert status_of(db, "WIDE")[1] == f"chunk_footprint_exceeded: {n} chunks in 1970-01-01..{AS_OF}, limit {n - 1}"
    # at the ceiling the same ticker is verified and certified
    monkeypatch.setattr(w, "MAX_VERIFICATION_CHUNKS", n)
    db.conn.execute("UPDATE eod_warmer_ticker_status SET retry_after = NULL")
    stats = run(db)["foreign_history"]
    assert stats["completed"] == 1 and stats["chunk_footprint_exceeded"] == 0
    assert status_of(db, "WIDE")[0] == "history_complete"


def test_gate5_promote_refuses_above_the_ceiling_and_writes_nothing(db, monkeypatch):
    instruments_row(db, "PRM")
    store_rows(db, "PRM", D(2026, 8, 3), D(2026, 9, 30))
    before = snapshot(db, "PRM", D(2026, 8, 1), D(2026, 10, 1))
    rows = [("PRM", D(2026, 10, 1), *[1.0] * 4, 10, *[1.0] * 4, 10, 0.0, 1.0)]
    monkeypatch.setattr(w, "MAX_VERIFICATION_CHUNKS", 0)
    with psycopg.connect(db.dsn) as conn:
        w.ensure_status_table(conn)
        with pytest.raises(w.ChunkFootprintExceeded):
            certify(conn, "PRM", rows, D(2026, 8, 3))
    assert snapshot(db, "PRM", D(2026, 8, 1), D(2026, 10, 1)) == before
    assert db.one("SELECT count(*) FROM eod_warmer_ticker_status WHERE ticker = 'PRM'")[0] == 0


# --- discovery isolation (comment 4238137020) and an empty source (4238137015) ---
@pytest.fixture
def stub_resolver(db):
    """Replace the W1c resolver with a plpgsql body; the real one is restored."""
    @contextlib.contextmanager
    def install(body):
        with psycopg.connect(db.dsn, autocommit=True) as admin:
            admin.execute("ALTER FUNCTION public.sec_foreign_listing_at(bigint, text, date)"
                          " RENAME TO sec_foreign_listing_at_real")
            admin.execute(
                "CREATE FUNCTION public.sec_foreign_listing_at(bigint, text, date)"
                " RETURNS TABLE(listing_status text) LANGUAGE plpgsql AS $f$ BEGIN "
                + body + " END $f$")
        try:
            yield
        finally:
            with psycopg.connect(db.dsn, autocommit=True) as admin:
                admin.execute("DROP FUNCTION public.sec_foreign_listing_at(bigint, text, date)")
                admin.execute("ALTER FUNCTION public.sec_foreign_listing_at_real"
                              " RENAME TO sec_foreign_listing_at")
    return install


@pytest.mark.parametrize(("body", "reason"), [
    ("RAISE EXCEPTION 'resolver exploded';", "RaiseException"),
    ("RAISE EXCEPTION 'permission denied for table sec_foreign_listing_evidence'"
     " USING ERRCODE = '42501';", "InsufficientPrivilege"),
    ("RAISE EXCEPTION 'relation does not exist' USING ERRCODE = '42P01';", "UndefinedTable"),
])
def test_gate5_comment_4238137020_a_failing_discovery_leaves_the_ring_running(
        db, tiingo, stub_resolver, body, reason):
    resolved_lines(db, "BOOM")
    db.conn.execute("INSERT INTO universe_constituents VALUES ('RING', 'Ring Co', 'active')")
    with stub_resolver(body):
        stats = run(db)
    assert stats["foreign_history"] == {"source": "error", "reason": reason, "errors": 1}
    assert "aborted" not in stats                                  # run_worker exits zero
    assert history_of(db, "RING")[2] > 0                           # the ring warmed its ticker
    assert db.one("SELECT count(*) FROM eod_warmer_ticker_status")[0] == 0     # history skipped
    assert tiingo.history_requests("BOOM") == []


def test_gate5_comment_4238137020_a_slow_discovery_is_cancelled_by_its_own_timeout(
        db, tiingo, stub_resolver, monkeypatch):
    resolved_lines(db, "SLOW")
    db.conn.execute("INSERT INTO universe_constituents VALUES ('RING', 'Ring Co', 'active')")
    monkeypatch.setattr(w, "FOREIGN_DISCOVERY_TIMEOUT_MS", 300)
    started = time.monotonic()
    with stub_resolver("PERFORM pg_sleep(60); RETURN;"):
        stats = run(db)
    assert time.monotonic() - started < 30
    assert stats["foreign_history"] == {"source": "error", "reason": "QueryCanceled", "errors": 1}
    assert history_of(db, "RING")[2] > 0 and "aborted" not in stats


def test_gate5_comment_4238137020_the_statement_timeout_is_local_to_discovery(db):
    with psycopg.connect(db.dsn) as conn:           # idle, as run() calls it
        w.foreign_listing_tickers(conn, AS_OF)
        assert conn.execute("SHOW statement_timeout").fetchone()[0] == "0"
        conn.rollback()


def test_gate5_comment_4238137020_the_worker_entrypoint_exits_zero(
        db, tiingo, stub_resolver, monkeypatch, capsys):
    from src import run_worker

    resolved_lines(db, "BOOM")
    db.conn.execute("INSERT INTO universe_constituents VALUES ('RING', 'Ring Co', 'active')")
    monkeypatch.setenv("WORKER", "eod_prices_warmer")
    monkeypatch.setenv("WORKER_CALC_DATE", AS_OF.isoformat())
    monkeypatch.setattr(run_worker, "resolve_dsn", lambda: db.dsn)
    with stub_resolver("RAISE EXCEPTION 'resolver exploded';"):
        run_worker.main()                                   # SystemExit would fail the test
    printed = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert printed["worker"] == "eod_prices_warmer" and printed["fetched"] > 0
    assert printed["foreign_history"] == {"source": "error", "reason": "RaiseException", "errors": 1}


def test_gate5_comment_4238137015_an_empty_source_is_reported_not_omitted(db, tiingo):
    db.conn.execute("INSERT INTO universe_constituents VALUES ('RING', 'Ring Co', 'active')")
    stats = run(db)
    assert stats["foreign_history"] == {
        "source": "empty", "source_tickers": 0, "already_complete": 0, "pending": 0,
        "processed": 0, "completed": 0, "errors": 0, "deferred": 0}
    assert history_of(db, "RING")[2] > 0


# --- malformed metadata dates (comment 4238137011) ---
@pytest.mark.parametrize("field", ["meta_start", "meta_end"])
@pytest.mark.parametrize("bad", ["2020-01-01garbage", "2020-01-01T12:00:00Z", "20200101", " 2020-01-01"])
def test_gate5_comment_4238137011_a_malformed_metadata_date_is_retried_never_an_interval(
        db, tiingo, field, bad):
    resolved_lines(db, "MALF")
    tiingo.listings["MALF"] = {"start": D(1993, 1, 29), "end": AS_OF, field: bad}
    stats = run(db)["foreign_history"]
    assert stats["errors"] == 1 and stats["completed"] == 0
    name = "start" if field == "meta_start" else "end"
    assert status_of(db, "MALF")[:2] == ("history_incomplete", f"meta:malformed_{name}_date")
    assert tiingo.of("prices", "MALF") == []                       # no interval, no price request
    assert db.one("SELECT count(*) FROM instruments WHERE ticker = 'MALF'")[0] == 0   # nothing seeded
    assert db.one("SELECT attempts, retry_after IS NOT NULL FROM eod_warmer_ticker_status"
                  " WHERE ticker = 'MALF'") == (1, True)


def test_gate5_comment_4238137011_documented_timestamp_dates_are_accepted(db, tiingo):
    resolved_lines(db, "TSZ")
    tiingo.listings["TSZ"] = {"start": D(2026, 8, 3), "end": AS_OF,
                              "meta_start": "2026-08-03T00:00:00.000Z",
                              "meta_end": "2026-10-09T00:00:00Z"}
    stats = run(db)["foreign_history"]
    assert stats["completed"] == 1 and stats["errors"] == 0
    assert db.one("SELECT tiingo_start_date, tiingo_end_date FROM instruments WHERE ticker = 'TSZ'") \
        == (D(2026, 8, 3), AS_OF)
