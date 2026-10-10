"""TimescaleDB tests: W1c foreign listings get full ``eod_prices`` history.

Runs against a disposable ``timescale/timescaledb:2.27.2-pg18`` database named in
``EOD_COVERAGE_TEST_DSN`` (loopback, database name containing ``eod_coverage``).
The W1c schema is installed from ``schemas/`` into ``public``; the warmer's own
tables live in a per-test schema reached through ``search_path``. Tiingo is an
``httpx.MockTransport``: no network, no API key.

Covers: W1c source-set selection, instruments metadata seeding without
clobbering, full-history cold start vs the screener's 745-day cold start,
truncated-history backfill without rewriting rows, the compressed-chunk insert
path (production layout, decompression limit forced to 1), Tiingo-unknown
handling, and the per-run cap / budget abort / resume.
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import psycopg
import pytest
from psycopg import errors, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from src.workers import _tiingo
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
def bdays(a, b):
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += dt.timedelta(days=1)


class FakeTiingo:
    """Tiingo meta + daily prices over a MockTransport, with a request log."""

    def __init__(self):
        self.listings: dict[str, dict] = {}
        self.unknown: set[str] = set()
        self.meta_status: dict[str, int] = {}
        self.price_status: dict[str, int] = {}
        self.requests: list[tuple] = []

    def listing(self, ticker):
        return self.listings.get(ticker) or {"start": D(1993, 1, 29), "end": AS_OF}

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
            start = lst["start"]
            return httpx.Response(200, json={
                "ticker": ticker.lower(), "name": lst.get("name", f"{ticker} Holdings"),
                "exchangeCode": lst.get("exchange", "NYSE"), "description": "",
                "startDate": None if lst.get("no_start") else start.isoformat(),
                "endDate": lst["end"].isoformat(),
            })
        params = parse_qs(urlsplit(str(request.url)).query)
        start = D.fromisoformat(params["startDate"][0])
        end = D.fromisoformat(params["endDate"][0]) if "endDate" in params else AS_OF
        self.requests.append(("prices", ticker, start, end))
        status = self.price_status.get(ticker)
        if status:
            return httpx.Response(status, json={"detail": "status"})
        if ticker in self.unknown:
            return httpx.Response(404, json={"detail": "Not found."})
        lst = self.listing(ticker)
        bars = []
        for d in bdays(max(start, lst["start"]), min(end, lst["end"])):
            px = 20.0 + (d.toordinal() % 50) / 10
            bars.append({
                "date": f"{d.isoformat()}T00:00:00.000Z", "open": px, "high": px + 1,
                "low": px - 1, "close": px, "volume": 1000, "adjOpen": px,
                "adjHigh": px + 1, "adjLow": px - 1, "adjClose": px, "adjVolume": 1000,
                "divCash": 0.0, "splitFactor": 1.0,
            })
        return httpx.Response(200, json=bars)

    def of(self, kind, ticker=None):
        return [r for r in self.requests
                if r[0] == kind and (ticker is None or r[1] == ticker)]


@pytest.fixture
def tiingo(monkeypatch):
    fake = FakeTiingo()

    def factory(*args, bucket=None, **kwargs):
        client = _tiingo.TiingoClient(key="test", bucket=bucket)
        client._client.close()
        client._client = httpx.Client(transport=httpx.MockTransport(fake.handler))
        return client

    monkeypatch.setattr(w, "TiingoClient", factory)
    monkeypatch.setattr(w, "FETCH_BURST", 100_000.0)
    monkeypatch.setattr(_tiingo.time, "sleep", lambda s: None)
    monkeypatch.delenv(w.HISTORY_LIMIT_ENV, raising=False)
    return fake


def run(db, **kwargs):
    return w.run(db.dsn, calc_date=AS_OF.isoformat(), **kwargs)


def history_of(db, ticker):
    return db.one("SELECT min(date), max(date), count(*) FROM eod_prices WHERE ticker = %s",
                  (ticker,))


def status_of(db, ticker):
    return db.one("SELECT status, detail, history_start FROM eod_warmer_ticker_status"
                  " WHERE ticker = %s", (ticker,))


def insert_rows(db, ticker, a, b, close=777.0):
    db.conn.execute(
        """INSERT INTO eod_prices
           SELECT %s, d::date, %s, %s, %s, %s, 5, %s, %s, %s, %s, 5, 0, 1
           FROM generate_series(%s::date, %s::date, interval '1 day') d
           WHERE extract(isodow FROM d) < 6""",
        (ticker, close, close, close, close, close, close, close, close, a, b),
    )


def compress_older_than(db, cutoff):
    for (chunk,) in db.q(
        "SELECT format('%%I.%%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks"
        " WHERE hypertable_schema = %s AND hypertable_name = 'eod_prices'"
        "   AND range_end <= %s AND NOT is_compressed", (db.schema, cutoff)):
        db.conn.execute("SELECT compress_chunk(%s::regclass)", (chunk,))


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
            "ABCD-B", "NEWB", "OLDA", "OTHR", "RCNT", "TSM"]
        assert w.foreign_listing_tickers(conn, D(2026, 1, 15)) == [
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
# 2. instruments seeding
# ──────────────────────────────────────────────────────────────────────────────
def test_meta_seeds_new_instruments_and_fills_only_nulls(db, tiingo):
    resolved_lines(db, "KEEP", "FILL", "NEWT")
    frozen = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    db.conn.execute(
        "INSERT INTO instruments (ticker, name, exchange_code, asset_type, tiingo_start_date,"
        " tiingo_end_date, updated_at) VALUES"
        " ('KEEP', 'Keep Co', 'NYSE', 'stock', '2001-01-02', '2026-06-30', %s),"
        " ('FILL', 'Screener Name', NULL, NULL, NULL, NULL, %s)", (frozen, frozen))
    insert_rows(db, "FILL", D(2024, 9, 30), D(2026, 10, 2))  # the 745-day cohort
    tiingo.listings.update({
        "KEEP": {"start": D(2001, 1, 2), "end": AS_OF},
        "FILL": {"start": D(2005, 1, 3), "end": AS_OF, "name": "Fill Tiingo", "exchange": "NASDAQ"},
        "NEWT": {"start": D(1997, 10, 9), "end": AS_OF, "name": "New Listing Co", "exchange": "NYSE"},
    })

    stats = run(db)["foreign_history"]

    rows = {r[0]: r[1:] for r in db.q(
        "SELECT ticker, name, exchange_code, asset_type, tiingo_start_date, tiingo_end_date,"
        " updated_at FROM instruments WHERE ticker IN ('KEEP', 'FILL', 'NEWT')")}
    # Complete row: no meta request, not touched at all.
    assert tiingo.of("meta", "KEEP") == []
    assert rows["KEEP"] == ("Keep Co", "NYSE", "stock", D(2001, 1, 2), D(2026, 6, 30), frozen)
    # NULLs filled from meta; the screener's name and NULL asset_type stay.
    assert rows["FILL"][:5] == ("Screener Name", "NASDAQ", None, D(2005, 1, 3), AS_OF)
    assert rows["FILL"][5] > frozen
    # New row: Tiingo metadata, asset_type stock.
    assert rows["NEWT"][:5] == ("New Listing Co", "NYSE", "stock", D(1997, 10, 9), AS_OF)
    assert (stats["instruments_inserted"], stats["instruments_filled"]) == (1, 1)
    assert history_of(db, "KEEP")[0] == D(2001, 1, 2)
    assert history_of(db, "NEWT")[0] == D(1997, 10, 9)


# ──────────────────────────────────────────────────────────────────────────────
# 3. full-history cold start vs the screener's 745 days
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
    # Covered lines: Tiingo startDate -> as_of, one price request, never 745 days.
    assert tiingo.of("prices", "FRGN") == [("prices", "FRGN", D(1997, 10, 9), AS_OF)]
    assert tiingo.of("prices", "BOTH") == [("prices", "BOTH", D(2000, 5, 1), AS_OF)]
    assert history_of(db, "FRGN")[:2] == (D(1997, 10, 9), AS_OF)
    assert history_of(db, "BOTH")[:2] == (D(2000, 5, 1), AS_OF)
    # The ring-served covered line goes first.
    order = [r[1] for r in tiingo.requests if r[1] in ("FRGN", "BOTH")]
    assert order.index("BOTH") < order.index("FRGN")
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
    db.conn.execute(
        "INSERT INTO instruments (ticker, name, asset_type, tiingo_start_date, tiingo_end_date,"
        " exchange_code) VALUES ('TRNC', 'Trunc Co', 'stock', NULL, NULL, NULL),"
        " ('CMPL', 'Complete Co', 'stock', '2010-01-04', '2026-10-09', 'NYSE')")
    insert_rows(db, "TRNC", D(2024, 6, 3), D(2026, 10, 2))
    insert_rows(db, "CMPL", D(2010, 1, 4), D(2026, 10, 2))
    compress_older_than(db, AS_OF - dt.timedelta(days=90))   # the columnstore policy
    compressed = db.one(
        "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = %s"
        " AND hypertable_name = 'eod_prices' AND is_compressed", (db.schema,))[0]
    assert compressed > 20
    untouched = (D(2024, 6, 3), D(2026, 9, 26))  # before the ring's 5-day overlap
    before = db.one("SELECT count(*), sum(close), sum(volume) FROM eod_prices"
                    " WHERE ticker = 'TRNC' AND date BETWEEN %s AND %s", untouched)
    tiingo.listings["TRNC"] = {"start": D(2005, 1, 3), "end": AS_OF}

    # Any decompression of an existing batch would now fail the run.
    strict = make_conninfo(
        db.dsn, options=f"-c search_path={db.schema},public"
        " -c timescaledb.max_tuples_decompressed_per_dml_transaction=1")
    stats = w.run(strict, calc_date=AS_OF.isoformat())

    assert "aborted" not in stats
    assert tiingo.of("prices", "TRNC") == [
        ("prices", "TRNC", D(2026, 9, 27), AS_OF),               # ring overlap
        ("prices", "TRNC", D(2005, 1, 3), D(2024, 6, 2)),        # missing history only
    ]
    assert history_of(db, "TRNC")[0] == D(2005, 1, 3)
    assert db.one("SELECT count(*), sum(close), sum(volume) FROM eod_prices"
                  " WHERE ticker = 'TRNC' AND date BETWEEN %s AND %s", untouched) == before
    assert db.one("SELECT count(*) FROM eod_prices WHERE ticker = 'TRNC' AND date < %s",
                  (D(2024, 6, 3),))[0] == len(list(bdays(D(2005, 1, 3), D(2024, 6, 2))))
    assert status_of(db, "TRNC") == ("history_complete", None, D(2005, 1, 3))
    # Rows already reach the instruments startDate: recorded, no request at all.
    assert status_of(db, "CMPL") == ("history_complete", None, D(2010, 1, 4))
    assert [r for r in tiingo.requests if r[1] == "CMPL" and r[0] == "meta"] == []
    assert stats["foreign_history"]["completed"] == 2

    tiingo.requests.clear()
    run(db)
    assert [r for r in tiingo.requests if r[1] == "TRNC"] == [
        ("prices", "TRNC", AS_OF - dt.timedelta(days=w.WATERMARK_OVERLAP_DAYS), AS_OF)]


# ──────────────────────────────────────────────────────────────────────────────
# 5. compressed-chunk insert path at production chunk size
# ──────────────────────────────────────────────────────────────────────────────
def test_history_insert_into_compressed_chunks_decompresses_no_batch(db):
    n = 5000  # ~110k rows per monthly chunk: above the 100k per-DML limit
    db.conn.execute(
        "INSERT INTO instruments (ticker) SELECT 'T' || lpad(i::text, 4, '0')"
        " FROM generate_series(1, %s) i", (n,))
    db.conn.execute("INSERT INTO instruments (ticker) VALUES ('MID'), ('NEW1')")
    db.conn.execute(
        """INSERT INTO eod_prices
           SELECT 'T' || lpad(i::text, 4, '0'), d::date, 10, 11, 9, 10, 1000, 10, 11, 9, 10, 1000, 0, 1
           FROM generate_series(1, %s) i,
                generate_series(DATE '2026-03-02', DATE '2026-04-30', INTERVAL '1 day') d
           WHERE extract(isodow FROM d) < 6""", (n,))
    insert_rows(db, "MID", D(2026, 4, 15), D(2026, 4, 30))   # starts mid-chunk
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
                " FROM eod_prices WHERE ticker LIKE 'T%' OR (ticker = 'MID' AND date >= '2026-04-15')")
    checksum_before = db.one(existing)

    def rows(ticker, a, b):
        return [(ticker, d, 1.0, 2.0, 0.5, 1.5, 7, 1.0, 2.0, 0.5, 1.5, 7, 0.0, 1.0)
                for d in bdays(a, b)]

    new_rows = rows("NEW1", D(2020, 1, 2), D(2026, 4, 30))
    mid_rows = rows("MID", D(2019, 1, 2), D(2026, 4, 14))
    strict = make_conninfo(db.dsn, options=f"-c search_path={db.schema},public"
                           " -c timescaledb.max_tuples_decompressed_per_dml_transaction=1")
    with psycopg.connect(strict) as conn:
        assert w.insert_history_rows(conn, new_rows) == len(new_rows)
        assert w.insert_history_rows(conn, mid_rows) == len(mid_rows)
        # Replaying the same keys inserts nothing and still decompresses nothing.
        assert w.insert_history_rows(conn, mid_rows) == 0
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
    # The columnstore policy's recompression absorbs the partial chunks.
    for c, _ in chunks:
        db.conn.execute("SELECT compress_chunk(%s::regclass)", (c,))
    assert sum(db.one(f"SELECT count(*) FROM ONLY {c}")[0] for c, _ in chunks) == 0
    assert db.one(existing) == checksum_before
    assert history_of(db, "MID")[:2] == (D(2019, 1, 2), D(2026, 4, 30))


# ──────────────────────────────────────────────────────────────────────────────
# 6. Tiingo-unknown tickers
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
    assert stats["foreign_history"]["known_unknown"] == 3

    db.conn.execute("UPDATE eod_warmer_ticker_status SET checked_at = now() - %s"
                    " WHERE status = 'tiingo_unknown'",
                    (dt.timedelta(days=w.UNKNOWN_RECHECK_DAYS + 1),))
    tiingo.requests.clear()
    run(db)
    assert sorted(r[1] for r in tiingo.of("meta")) == ["NODT", "UNKN"]
    # PNF's metadata was seeded on the first run; its recheck is the price call.
    assert tiingo.of("prices", "PNF") == [("prices", "PNF", D(1993, 1, 29), AS_OF)]


# ──────────────────────────────────────────────────────────────────────────────
# 7. per-run cap, resume, budget
# ──────────────────────────────────────────────────────────────────────────────
def test_cap_bounds_each_run_and_the_backlog_resumes(db, tiingo):
    names = ["CAPA", "CAPB", "CAPC", "CAPD", "CAPE"]
    resolved_lines(db, *names)
    seen = []
    for expected_completed, expected_deferred in ((2, 3), (2, 1), (1, 0), (0, 0)):
        tiingo.requests.clear()
        stats = run(db, history_limit=2)["foreign_history"]
        # Completed lines join the ring (incremental); count history requests only.
        history_requests = [r for r in tiingo.requests if r[1] in names
                            and (r[0] == "meta" or r[2] == D(1993, 1, 29))]
        assert len(history_requests) <= 2 * 2
        assert (stats["completed"], stats["deferred"]) == (expected_completed, expected_deferred)
        seen += sorted({r[1] for r in tiingo.of("meta") if r[1] in names})
    assert seen == names
    assert all(history_of(db, t)[0] == D(1993, 1, 29) for t in seen)


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
