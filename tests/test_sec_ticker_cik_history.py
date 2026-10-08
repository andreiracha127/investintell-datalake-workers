"""SEC cover-page ticker -> (CIK, class) history: parser, loader and resolvers.

Unit tests need no database. The DB tests run against a disposable loopback
PostgreSQL named by ``SEC_TEST_DATABASE_URL`` (postgres:16 in CI), each inside
its own schema, and skip when the variable is unset. Expected values are
written out by hand from the documented interval rules, not computed by the
code under test.
"""

from __future__ import annotations

import datetime as dt
import gzip
import os
import zipfile
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from scripts import load_sec_ticker_cik_history as loader

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = (ROOT / "schemas" / "sec_ticker_cik_history_v1.sql").read_text(encoding="utf-8")
ROLLBACK_SQL = (ROOT / "schemas" / "sec_ticker_cik_history_v1.rollback.sql").read_text(
    encoding="utf-8"
)
LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}
READERS = ("app_runtime", "app_analytics_ro", "mcp_ro")
FUNCTIONS = (
    "sec_ticker_lines_at(text,date,integer)",
    "sec_ticker_issuer_at(text,date,integer)",
    "sec_issuer_line_at(bigint,text,date,integer)",
    "sec_cover_class_shares_at(bigint,text,date,integer)",
    "sec_ticker_price_span(text,bigint,text)",
)
TABLES = (
    "sec_ticker_cik_observations", "sec_cover_share_counts", "sec_registration_events",
    "sec_ticker_cik_packages", "sec_ticker_intervals",
)

SUB_HEADER = ("adsh", "cik", "name", "form", "period", "fy", "fp", "filed", "accepted")
TXT_HEADER = (
    "adsh", "tag", "version", "ddate", "qtrs", "iprx", "lang", "dcml", "durp", "datp",
    "dimh", "dimn", "coreg", "escaped", "srclen", "txtlen", "footnote", "footlen",
    "context", "value",
)
NUM_HEADER = (
    "adsh", "tag", "version", "ddate", "qtrs", "uom", "dimh", "iprx", "value", "footnote",
    "footlen", "dimn", "coreg", "durp", "datp", "dcml",
)
DIM_HEADER = ("dimhash", "segments", "segt")
CLASS_A = "ClassOfStock=CommonClassA;"
NONVOTING = "ClassOfStock=NonvotingCommonStock;"


# --------------------------------------------------------------------------- #
# Normalization and classification
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("raw", "tickers"),
    [
        ("AAPL", ["AAPL"]),
        ("kpay", ["KPAY"]),  # pre-2019 voluntary tags are often lowercase
        ("BRK.B", ["BRK-B"]),  # universe / eod_prices write class shares as BRK-B
        ("BRK B", ["BRK-B"]),
        ("MKC.V", ["MKC-V"]),
        ("BFB", ["BFB"]),  # no separator to recover; the resolver key bridges it
        ("USB PrA", ["USB-PA"]),  # preferred series in the Tiingo style
        ("PSAPrM", ["PSA-PM"]),
        ("CDRpB", ["CDR-PB"]),
        ("WFC.PRA", ["WFC-PA"]),
        ("JPM PR C", ["JPM-PC"]),
        ("COF PRI", ["COF-PI"]),
        ("GSPRA", ["GSPRA"]),  # uppercase marker without a separator stays literal
        ("SPY", ["SPY"]),  # a P followed by a letter is not a preferred marker
        ("TPR", ["TPR"]),
        ("USB/28", ["USB-28"]),  # listed notes keep their own symbol
        ("BAX (NYSE)", ["BAX"]),
        ("NYSE: KO", ["KO"]),
        ("GOOGL, GOOG", ["GOOGL", "GOOG"]),
        ("BIP; BIP UN", ["BIP", "BIP-UN"]),
    ],
)
def test_symbols_normalize_to_the_price_table_style(raw: str, tickers: list[str]) -> None:
    assert loader.normalize_symbols(raw) == (tickers, [])


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("None", "placeholder"),
        ("None.", "placeholder"),
        ("N/A", "placeholder"),
        ("Not Applicable", "placeholder"),
        ("true", "placeholder"),
        ("No Trading Symbol", "placeholder"),
        ("", "empty"),
        ("Common Stock par value", "too_long"),
        ("AB#C", "malformed"),
    ],
)
def test_placeholders_and_junk_are_rejected_with_a_reason(raw: str, reason: str) -> None:
    assert loader.normalize_symbols(raw) == ([], [reason])


def test_resolver_key_matches_every_class_spelling() -> None:
    assert {loader.ticker_key(t) for t in ("BRK-B", "BRK.B", "brk b", "BRKB")} == {"BRKB"}


@pytest.mark.parametrize(
    ("segments", "key"),
    [
        ("", ""),
        (CLASS_A, CLASS_A),
        ("EntityListingsExchange=NYSE;ClassOfStock=CommonClassA;", CLASS_A),
        ("ClassOfStock=CommonClassA;LegalEntity=Parent;", CLASS_A),
        ("EntityListingsExchange=NASDAQ;", ""),
        ("LongtermDebtType=Notes2029;", "LongtermDebtType=Notes2029;"),
    ],
)
def test_class_key_drops_listing_and_entity_axes(segments: str, key: str) -> None:
    assert loader.class_key(segments) == key


@pytest.mark.parametrize(
    ("title", "ticker", "segments", "kind"),
    [
        ("Common Stock, $0.01 par value per share", "AAPL", "", "equity"),
        ("Class A Common Stock", "BF-A", CLASS_A, "equity"),
        ("Common Stock and associated Preferred Stock Purchase Rights", "XYZ", "", "equity"),
        ("Common units representing limited partner interests", "EPD", "", "equity"),
        ("Ordinary Shares, nominal value $0.0001", "ABC", "", "equity"),
        ("1.375% Notes due 2029", "BF-28", "", "debt"),
        ("Floating Rate Senior Notes due 2026", "GS-26", "", "debt"),
        ("Depositary Shares, each representing a 1/1,000th interest in a share of 5.85% "
         "Series A Preferred Stock", "USB-PA", "", "preferred"),
        ("American Depositary Shares, each representing five ordinary shares", "TSM", "",
         "depositary"),
        ("Units, each consisting of one Class A ordinary share and one-half of one "
         "redeemable warrant", "ACAC-U", "", "unit"),
        ("Redeemable warrants, each whole warrant exercisable for one share", "ACAC-WS", "",
         "warrant"),
        ("Rights, each entitling the holder to receive one-tenth of one share", "ACAC-R", "",
         "right"),
        (None, "USB-PA", "", "preferred"),
        (None, "ACHR-WS", "", "warrant"),
        (None, "BRK27", "", "debt"),
        (None, "ZZZ", "LongtermDebtType=Notes2029;", "debt"),
        (None, "AAPL", "", "equity"),
    ],
)
def test_security_kind_reads_the_title_then_segments_then_symbol(
    title: str | None, ticker: str, segments: str, kind: str
) -> None:
    assert loader.security_kind(title, ticker, segments) == kind


def test_packages_sort_chronologically_across_naming_schemes() -> None:
    names = ["2025_10_notes.zip", "2010q1_notes_1.zip", "2025q3_notes.zip", "2009q4_notes.zip",
             "2026_01_notes.zip"]
    ordered = [p.name for p in sorted((Path(n) for n in names), key=loader.package_sort_key)]
    assert ordered == ["2009q4_notes.zip", "2010q1_notes_1.zip", "2025q3_notes.zip",
                       "2025_10_notes.zip", "2026_01_notes.zip"]
    with pytest.raises(ValueError):
        loader.package_sort_key(Path("financial.zip"))


def test_listing_links_are_absolute_and_deduplicated() -> None:
    html = (
        '<a href="/files/dera/data/financial-statement-notes-data-sets/2026_09_notes.zip">a</a>'
        '<a href="/files/dera/data/financial-statement-notes-data-sets/2026_09_notes.zip">b</a>'
        '<a href="/files/dera/data/financial-statement-notes-data-sets/2010q1_notes_1.zip">c</a>'
    )
    assert loader.listed_package_urls(html) == [
        "https://www.sec.gov/files/dera/data/financial-statement-notes-data-sets/2026_09_notes.zip",
        "https://www.sec.gov/files/dera/data/financial-statement-notes-data-sets/2010q1_notes_1.zip",
    ]


def test_index_quarters_start_with_the_first_package() -> None:
    quarters = loader.quarters_through(dt.date(2010, 5, 1))
    assert quarters == [(2009, 1), (2009, 2), (2009, 3), (2009, 4), (2010, 1), (2010, 2)]


# --------------------------------------------------------------------------- #
# Package and index parsing
# --------------------------------------------------------------------------- #
def _fact(adsh: str, tag: str, value: str, *, dimh: str = "0x00000000", coreg: str = "",
          version: str = "dei/2024", iprx: int = 0, ddate: str = "20200131") -> dict[str, str]:
    return {"adsh": adsh, "tag": tag, "version": version, "ddate": ddate, "iprx": str(iprx),
            "dimh": dimh, "coreg": coreg, "value": value}


def _shares(adsh: str, value: str, *, dimh: str = "0x00000000", ddate: str = "20240220",
            uom: str = "shares", coreg: str = "") -> dict[str, str]:
    return {"adsh": adsh, "tag": "EntityCommonStockSharesOutstanding", "version": "dei/2024",
            "ddate": ddate, "uom": uom, "dimh": dimh, "coreg": coreg, "value": value}


def _write_package(path: Path, submissions: list[dict[str, str]], facts: list[dict[str, str]],
                   shares: list[dict[str, str]] = (), dims: dict[str, str] | None = None, *,
                   compression: int = zipfile.ZIP_DEFLATED) -> Path:
    def tsv(header: tuple[str, ...], rows: list[dict[str, str]]) -> str:
        lines = ["\t".join(header)] + ["\t".join(row.get(c, "") for c in header) for row in rows]
        return "\n".join(lines) + "\n"

    dim_rows = [{"dimhash": "0x00000000", "segments": "", "segt": "0"}] + [
        {"dimhash": h, "segments": s, "segt": "0"} for h, s in (dims or {}).items()
    ]
    with zipfile.ZipFile(path, "w", compression=compression) as archive:
        archive.writestr("sub.tsv", tsv(SUB_HEADER, submissions))
        archive.writestr("txt.tsv", tsv(TXT_HEADER, facts))
        archive.writestr("num.tsv", tsv(NUM_HEADER, list(shares)))
        archive.writestr("dim.tsv", tsv(DIM_HEADER, dim_rows))
        archive.writestr("pre.tsv", "adsh\treport\n")  # never read
    return path


def _sub(adsh: str, cik: int, form: str, filed: str, accepted: str = "") -> dict[str, str]:
    return {"adsh": adsh, "cik": str(cik), "name": f"CIK {cik}", "form": form,
            "period": filed, "filed": filed, "accepted": accepted}


A1 = "0000000001-24-000001"
A2 = "0000000002-24-000001"
A3 = "0000000003-24-000001"
DIMS = {
    "0xaaa": CLASS_A,
    "0xbbb": "ClassOfStock=CommonClassB;",
    "0xbbx": "ClassOfStock=CommonClassB;EntityListingsExchange=NYSE;",
    "0xnot": "LongtermDebtType=Notes2034;",
    "0xccc": NONVOTING,
}


def _sample_package(tmp_path: Path) -> Path:
    return _write_package(
        tmp_path / "2024q1_notes.zip",
        [
            _sub(A1, 1067983, "10-K", "20240226", "2024-02-24 08:00:05.0"),
            _sub(A2, 14693, "8-K", "20240305", ""),
            _sub(A3, 99, "10-Q", "20240310", "2024-03-09 18:01:00.0"),
        ],
        [
            _fact(A1, "TradingSymbol", "BRK.A", dimh="0xaaa"),
            _fact(A1, "Security12bTitle", "Class A Common Stock", dimh="0xaaa"),
            _fact(A1, "SecurityExchangeName", "NYSE", dimh="0xaaa"),
            _fact(A1, "TradingSymbol", "BRK.B", dimh="0xbbx", ddate="20190101"),
            _fact(A1, "Security12bTitle", "Class B Common Stock", dimh="0xbbx"),
            _fact(A1, "SecurityExchangeName", "NYSE", dimh="0xbbx"),
            _fact(A1, "TradingSymbol", "BRK34", dimh="0xnot"),
            _fact(A1, "Security12bTitle", "2.000% Senior Notes due 2034", dimh="0xnot"),
            _fact(A2, "TradingSymbol", "BFB", dimh="0xccc"),
            _fact(A2, "TradingSymbol", "BFB", dimh="0xccc", iprx=1),  # same fact twice
            # Not the registrant's symbol, not a dei fact, a placeholder, no submission.
            _fact(A3, "TradingSymbol", "SUB", coreg="SubsidiaryMember"),
            _fact(A3, "TradingSymbol", "CUST", version="0000000003-24-000001"),
            _fact(A3, "TradingSymbol", "None"),
            _fact("0000000009-24-000009", "TradingSymbol", "GHOST"),
        ],
        [
            _shares(A1, "511820.0000", dimh="0xaaa", ddate="20240212"),
            _shares(A1, "1389605139.0000", dimh="0xbbb", ddate="20240212"),
            _shares(A2, "290262390", dimh="0xccc"),
            _shares(A2, "12", dimh="0xccc", uom="USD"),  # not a share count
            _shares(A3, "1000", coreg="SubsidiaryMember"),
        ],
        DIMS,
    )


def test_package_parse_keeps_registrant_lines_with_their_class(tmp_path: Path) -> None:
    result = loader.parse_package(_sample_package(tmp_path))
    rows = {(o.adsh, o.ticker): o for o in result.observations}
    assert set(rows) == {(A1, "BRK-A"), (A1, "BRK-B"), (A1, "BRK34"), (A2, "BFB")}
    class_b = rows[(A1, "BRK-B")]
    # The exchange axis is not part of the class: it joins the class B share count.
    assert (class_b.cik, class_b.class_key, class_b.segments, class_b.security_kind) == (
        1067983, "ClassOfStock=CommonClassB;",
        "ClassOfStock=CommonClassB;EntityListingsExchange=NYSE;", "equity",
    )
    assert (class_b.security_title, class_b.exchange, class_b.ticker_raw) == (
        "Class B Common Stock", "NYSE", "BRK.B",
    )
    # Knowledge comes from the submission; ddate is kept, never used to date it.
    assert (class_b.filed, class_b.accepted, class_b.ddate) == (
        dt.date(2024, 2, 26), dt.datetime(2024, 2, 24, 8, 0, 5), dt.date(2019, 1, 1),
    )
    assert rows[(A1, "BRK34")].security_kind == "debt"
    assert rows[(A2, "BFB")].accepted is None
    assert result.symbol_facts == 9
    assert dict(result.rejected) == {
        "coregistrant": 1, "non_dei_tag": 1, "placeholder": 1, "no_submission": 1,
        "duplicate_in_context": 1, "share_count_other_entity_or_unit": 2,
    }
    shares = {(s.adsh, s.class_key): s.shares for s in result.share_counts}
    assert shares == {
        (A1, CLASS_A): Decimal("511820.0000"),
        (A1, "ClassOfStock=CommonClassB;"): Decimal("1389605139.0000"),
        (A2, NONVOTING): Decimal("290262390"),
    }
    assert len(result.submissions) == 3
    assert len(result.sha256) == 64


def test_a_corrupt_package_member_fails_the_parse(tmp_path: Path) -> None:
    path = _sample_package(tmp_path)
    stored = _write_package(
        tmp_path / "2024q2_notes.zip",
        [_sub(A1, 1, "10-K", "20240226")],
        [_fact(A1, "TradingSymbol", "GHOSTLY")],
        compression=zipfile.ZIP_STORED,
    )
    data = bytearray(stored.read_bytes())
    data[data.index(b"GHOSTLY")] ^= 0x01  # same length, different bytes: CRC must catch it
    stored.write_bytes(bytes(data))
    assert path.exists()
    with pytest.raises(zipfile.BadZipFile):
        loader.parse_package(stored)


def test_form_index_keeps_only_deregistration_and_delisting_rows(tmp_path: Path) -> None:
    path = tmp_path / "2020QTR1.form.gz"
    rows = [
        "Form Type   Company Name      CIK   Date Filed  File Name",
        "-" * 80,
        "10-K             SOME CO 2000 INC                                  1234        "
        "2020-02-01  edgar/data/1234/0001234-20-000001.txt",
        "15-12G           ACQUIRED CORP                                     5907        "
        "2020-03-02  edgar/data/5907/0000005907-20-000001.txt          ",
        "25-NSE           ACHILLION PHARMACEUTICALS INC                     1070336     "
        "2020-01-28  edgar/data/1070336/0001354457-20-000034.txt         ",
        "25               ACHILLION PHARMACEUTICALS INC                     1070336     "
        "2020-01-29  edgar/data/1070336/0001070336-20-000003.txt",
        "SC 13D           HOLDER 25 LLC                                     777         "
        "2020-01-05  edgar/data/777/0000000777-20-000001.txt",
    ]
    path.write_bytes(gzip.compress(("\n".join(rows) + "\n").encode("latin-1")))
    events, sha256, size = loader.parse_form_index(path)
    assert [(e.form, e.cik, e.filed, e.adsh) for e in events] == [
        ("15-12G", 5907, dt.date(2020, 3, 2), "0000005907-20-000001"),
        ("25", 1070336, dt.date(2020, 1, 29), "0001070336-20-000003"),
        ("25-NSE", 1070336, dt.date(2020, 1, 28), "0001354457-20-000034"),
    ]
    assert len(sha256) == 64 and size == path.stat().st_size


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #
def _dsn() -> str:
    import psycopg

    dsn = os.getenv("SEC_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SEC_TEST_DATABASE_URL unset; DB tests not evaluated")
    if psycopg.conninfo.conninfo_to_dict(dsn).get("host") not in LOOPBACK_HOSTS:
        raise RuntimeError("SEC_TEST_DATABASE_URL must target a loopback database")
    return dsn


@pytest.fixture
def schema_dsn():
    """A fresh schema holding the applied DDL; yields (connection, schema dsn)."""
    import psycopg
    from psycopg import sql

    dsn = _dsn()
    schema = f"sec_ticker_{uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        try:
            conn.execute(SCHEMA_SQL)
            yield conn, psycopg.conninfo.make_conninfo(dsn, options=f"-csearch_path={schema}")
        finally:
            conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)))


_SEQ = iter(range(1, 10**6))


def _observe(conn, cik: int, ticker: str, filed: str, *, accepted: str | None = None,
             class_key: str = "", kind: str = "equity", adsh: str | None = None) -> str:
    adsh = adsh or f"{next(_SEQ):010d}-24-000001"
    conn.execute(
        "INSERT INTO sec_ticker_cik_observations (adsh, cik, dimh, segments, class_key, "
        "ticker, ticker_raw, security_kind, form, filed, accepted, source_package) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '10-Q', %s, %s, 'test')",
        (adsh, cik, class_key or "0x00000000", class_key, class_key, ticker, ticker, kind,
         filed, accepted),
    )
    return adsh


def _event(conn, cik: int, form: str, filed: str) -> None:
    conn.execute(
        "INSERT INTO sec_registration_events (adsh, cik, form, filed, source_package) "
        "VALUES (%s, %s, %s, %s, 'test')",
        (f"{next(_SEQ):010d}-24-000009", cik, form, filed),
    )


def _count(conn, cik: int, class_key: str, ddate: str, shares: int, filed: str,
           adsh: str | None = None) -> None:
    conn.execute(
        "INSERT INTO sec_cover_share_counts (adsh, cik, dimh, segments, class_key, ddate, "
        "shares, form, filed, source_package) VALUES (%s, %s, %s, %s, %s, %s, %s, '10-Q', %s, "
        "'test')",
        (adsh or f"{next(_SEQ):010d}-24-000002", cik, class_key or "0x00000000", class_key,
         class_key, ddate, shares, filed),
    )


def _issuer(conn, ticker: str, as_of: str) -> tuple:
    return conn.execute(
        "SELECT status, cik, class_key, observed_on, active_ciks "
        "FROM sec_ticker_issuer_at(%s, %s)", (ticker, as_of),
    ).fetchone()


def _line(conn, cik: int, class_key: str, as_of: str) -> tuple:
    return conn.execute(
        "SELECT status, class_key, tickers, statement_on, equity_lines "
        "FROM sec_issuer_line_at(%s, %s, %s)", (cik, class_key, as_of),
    ).fetchone()


def _class_shares(conn, cik: int, class_key: str, as_of: str) -> tuple:
    return conn.execute(
        "SELECT status, shares, shares_as_of FROM sec_cover_class_shares_at(%s, %s, %s)",
        (cik, class_key, as_of),
    ).fetchone()


def test_schema_reapplies_and_rolls_back_cleanly(schema_dsn) -> None:
    conn, _ = schema_dsn
    conn.execute(SCHEMA_SQL)  # idempotent
    _observe(conn, 732717, "T", "2024-01-10")
    assert conn.execute("SELECT ticker_key, available_on FROM sec_ticker_cik_observations"
                        ).fetchone() == ("T", dt.date(2024, 1, 11))
    conn.execute(ROLLBACK_SQL)
    leftovers = conn.execute(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = current_schema() "
        "UNION ALL SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = current_schema()"
    ).fetchall()
    assert leftovers == [(0,), (0,)]


def test_availability_uses_acceptance_date_else_the_day_after_filing(schema_dsn) -> None:
    conn, _ = schema_dsn
    _observe(conn, 1, "AAA", "2024-03-04", accepted="2024-03-01 18:30:00")  # after 17:30
    _observe(conn, 2, "BBB", "2024-03-04")  # no acceptance time
    assert _issuer(conn, "AAA", "2024-02-29")[0] == "missing"
    assert _issuer(conn, "AAA", "2024-03-01")[:2] == ("resolved", 1)  # public on acceptance
    assert _issuer(conn, "BBB", "2024-03-04")[0] == "missing"  # filing day: no time known
    assert _issuer(conn, "BBB", "2024-03-05") == ("resolved", 2, "", dt.date(2024, 3, 5), [2])


@pytest.mark.parametrize(("age_days", "status"), [(400, "resolved"), (401, "stale")])
def test_open_interval_goes_stale_after_400_days_without_confirmation(
    schema_dsn, age_days: int, status: str,
) -> None:
    conn, _ = schema_dsn
    _observe(conn, 7, "OLD", "2023-01-09", accepted="2023-01-10 10:00:00")
    as_of = dt.date(2023, 1, 10) + dt.timedelta(days=age_days)
    assert _issuer(conn, "OLD", as_of.isoformat()) == (
        status, 7 if status == "resolved" else None, "" if status == "resolved" else None,
        dt.date(2023, 1, 10), [7],
    )


def _att(conn, *, deregistered: bool) -> None:
    """AT&T Corp (CIK 5907) held "T"; AT&T Inc (CIK 732717) took it over."""
    _observe(conn, 5907, "T", "2009-11-05")
    _observe(conn, 5907, "T", "2010-02-25")
    if deregistered:
        _event(conn, 5907, "15-12G", "2010-03-15")
    _observe(conn, 732717, "T", "2010-05-07")
    _observe(conn, 732717, "T", "2010-08-06")


def test_reused_ticker_moves_to_the_new_issuer_after_deregistration(schema_dsn) -> None:
    conn, _ = schema_dsn
    _att(conn, deregistered=True)
    assert _issuer(conn, "T", "2010-03-01") == ("resolved", 5907, "", dt.date(2010, 2, 26), [5907])
    assert _issuer(conn, "T", "2010-03-16")[:2] == ("ended", None)  # 15-12G public 03-16
    assert _issuer(conn, "T", "2010-05-08") == (
        "resolved", 732717, "", dt.date(2010, 5, 8), [732717],
    )
    assert _issuer(conn, "T", "2011-01-03")[:2] == ("resolved", 732717)


def test_reused_ticker_without_an_end_event_overlaps_until_the_old_hold_is_stale(
    schema_dsn,
) -> None:
    conn, _ = schema_dsn
    _att(conn, deregistered=False)
    # AT&T Corp's interval stays open (no different symbol, no deregistration):
    # two issuers hold "T" until its last statement (2010-02-26) is 400 days old.
    assert _issuer(conn, "T", "2010-05-08") == (
        "ambiguous", None, None, dt.date(2010, 5, 8), [732717, 5907],
    )
    assert _issuer(conn, "T", "2011-04-02")[0] == "ambiguous"  # 2010-02-26 + 400
    assert _issuer(conn, "T", "2011-04-03")[:2] == ("resolved", 732717)


def test_a_class_specific_delisting_ends_only_a_single_symbol_issuer(schema_dsn) -> None:
    conn, _ = schema_dsn
    filing = _observe(conn, 10, "ONE", "2021-01-10")
    _observe(conn, 20, "TWO", "2021-01-10")
    _observe(conn, 20, "TWO-27", "2021-01-10", class_key="LongtermDebtType=Notes2027;",
             kind="debt", adsh=conn.execute(
                 "SELECT adsh FROM sec_ticker_cik_observations WHERE ticker = 'TWO'"
             ).fetchone()[0])
    _event(conn, 10, "25-NSE", "2021-02-01")
    _event(conn, 20, "25-NSE", "2021-02-01")  # could be the notes: does not end TWO
    assert filing
    assert _issuer(conn, "ONE", "2021-02-02")[:2] == ("ended", None)
    assert _issuer(conn, "TWO", "2021-02-02")[:2] == ("resolved", 20)
    _event(conn, 20, "15-15D", "2021-03-01")  # the registrant stops reporting
    assert _issuer(conn, "TWO", "2021-03-02")[:2] == ("ended", None)
    _observe(conn, 20, "TWO", "2021-04-01")  # a later cover filing reopens the hold
    assert _issuer(conn, "TWO", "2021-04-02")[:2] == ("resolved", 20)


def test_rename_ends_the_old_symbol_and_the_line_shows_what_it_traded_as(schema_dsn) -> None:
    conn, _ = schema_dsn
    _observe(conn, 1512673, "SQ", "2024-11-05")
    _observe(conn, 1512673, "XYZ", "2025-01-21", accepted="2025-01-21 08:01:00")
    assert _issuer(conn, "SQ", "2025-01-20")[:2] == ("resolved", 1512673)
    assert _issuer(conn, "SQ", "2025-01-21")[:2] == ("ended", None)
    assert _issuer(conn, "XYZ", "2024-12-31")[:2] == ("missing", None)
    assert _issuer(conn, "XYZ", "2025-01-21")[:2] == ("resolved", 1512673)
    # The current symbol's line asked earlier shows the symbol it traded under then.
    assert _line(conn, 1512673, "", "2024-12-31") == (
        "resolved", "", ["SQ"], dt.date(2024, 11, 6), 1,
    )
    # A later issuer reusing "SQ" holds it from its own first statement.
    _observe(conn, 4242, "SQ", "2026-03-02")
    assert _issuer(conn, "SQ", "2026-03-03")[:2] == ("resolved", 4242)
    assert _issuer(conn, "SQ", "2024-12-31")[:2] == ("resolved", 1512673)


def test_concurrent_holders_are_ambiguous(schema_dsn) -> None:
    conn, _ = schema_dsn
    _observe(conn, 30, "TIE", "2022-01-10")
    _observe(conn, 40, "TIE", "2022-01-10")
    assert _issuer(conn, "TIE", "2022-02-01") == (
        "ambiguous", None, None, dt.date(2022, 1, 11), [30, 40],
    )


def _two_class_issuer(conn) -> None:
    """Brown-Forman: class A (BFA) and nonvoting class B (BFB), plus listed notes."""
    adsh = _observe(conn, 14693, "BFA", "2024-03-05", class_key=CLASS_A)
    _observe(conn, 14693, "BFB", "2024-03-05", class_key=NONVOTING, adsh=adsh)
    _observe(conn, 14693, "BF-28", "2024-03-05", class_key="LongtermDebtType=Notes2028;",
             kind="debt", adsh=adsh)
    _count(conn, 14693, CLASS_A, "2024-02-28", 168_441_239, "2024-03-05", adsh=adsh)
    _count(conn, 14693, NONVOTING, "2024-02-28", 290_262_390, "2024-03-05", adsh=adsh)


def test_two_class_issuer_resolves_each_class_to_its_own_line_and_count(schema_dsn) -> None:
    conn, _ = schema_dsn
    _two_class_issuer(conn)
    assert _issuer(conn, "BF-B", "2024-03-06") == (
        "resolved", 14693, NONVOTING, dt.date(2024, 3, 6), [14693],
    )
    assert _issuer(conn, "BF.A", "2024-03-06")[:3] == ("resolved", 14693, CLASS_A)
    assert _line(conn, 14693, NONVOTING, "2024-03-06") == (
        "resolved", NONVOTING, ["BFB"], dt.date(2024, 3, 6), 2,
    )
    assert _class_shares(conn, 14693, NONVOTING, "2024-03-06") == (
        "resolved", Decimal(290_262_390), dt.date(2024, 2, 28),
    )
    assert _class_shares(conn, 14693, CLASS_A, "2024-03-05")[0] == "missing"  # filed that day
    assert _class_shares(conn, 14693, "", "2024-03-06")[0] == "missing"  # no total reported


def test_single_class_issuer_is_followed_through_a_renamed_member(schema_dsn) -> None:
    conn, _ = schema_dsn
    _observe(conn, 55, "OLDSYM", "2020-02-10")  # no dimension then
    _observe(conn, 55, "NEWSYM", "2021-02-10", class_key="ClassOfStock=CommonStock;")
    assert _line(conn, 55, "ClassOfStock=CommonStock;", "2020-06-01") == (
        "resolved", "", ["OLDSYM"], dt.date(2020, 2, 11), 1,
    )
    assert _line(conn, 77, "", "2020-06-01") == ("missing", None, [], None, 0)


def test_class_share_count_rules(schema_dsn) -> None:
    conn, _ = schema_dsn
    _count(conn, 9, CLASS_A, "2023-01-31", 100, "2023-02-10")
    assert _class_shares(conn, 9, CLASS_A, "2023-02-10")[0] == "missing"
    assert _class_shares(conn, 9, CLASS_A, "2023-02-11") == (
        "resolved", Decimal(100), dt.date(2023, 1, 31),
    )
    _count(conn, 9, CLASS_A, "2023-01-31", 120, "2023-03-01")  # amendment, same date
    assert _class_shares(conn, 9, CLASS_A, "2023-03-02")[:2] == ("resolved", Decimal(120))
    _count(conn, 9, CLASS_A, "2023-04-30", 130, "2023-05-10")
    _count(conn, 9, CLASS_A, "2023-04-30", 131, "2023-05-10")  # conflicting in one filing
    assert _class_shares(conn, 9, CLASS_A, "2023-05-11")[:2] == ("ambiguous", None)
    assert _class_shares(conn, 9, CLASS_A, "2024-06-03")[0] == "ambiguous"  # 2023-04-30 + 400
    assert _class_shares(conn, 9, CLASS_A, "2024-06-04")[0] == "stale"


def test_future_filings_never_change_an_earlier_answer(schema_dsn) -> None:
    conn, _ = schema_dsn
    _att(conn, deregistered=True)
    _two_class_issuer(conn)
    _observe(conn, 1512673, "SQ", "2024-11-05")
    as_ofs = ("2010-03-01", "2010-05-08", "2024-03-06", "2024-12-31")
    probes = [
        ("issuer", "SELECT * FROM sec_ticker_issuer_at(%s, %s)", t)
        for t in ("T", "BF-B", "BF-A", "SQ", "XYZ")
    ] + [
        ("line", "SELECT * FROM sec_issuer_line_at(%s, %s, %s)", (14693, NONVOTING)),
        ("line", "SELECT * FROM sec_issuer_line_at(%s, %s, %s)", (1512673, "")),
        ("shares", "SELECT * FROM sec_cover_class_shares_at(%s, %s, %s)", (14693, NONVOTING)),
    ]

    def answers() -> list:
        out = []
        for as_of in as_ofs:
            for _, query, args in probes:
                params = (args, as_of) if isinstance(args, str) else (*args, as_of)
                out.append(conn.execute(query, params).fetchall())
        return out

    before = answers()
    # Everything below becomes public after the last probe date.
    _observe(conn, 1512673, "XYZ", "2025-01-21")  # rename
    _observe(conn, 999, "T", "2025-02-01")  # a later claim on T
    _event(conn, 732717, "15-12B", "2025-03-01")
    _event(conn, 14693, "15-15D", "2025-03-01")
    _observe(conn, 14693, "BFB", "2025-03-05", class_key="ClassOfStock=CommonClassB;")
    _count(conn, 14693, NONVOTING, "2024-02-28", 1, "2025-04-01")  # late restatement
    _count(conn, 14693, NONVOTING, "2025-02-28", 2, "2025-03-05")
    assert answers() == before


def _span(conn, ticker: str, cik: int, class_key: str | None = None) -> list[tuple]:
    return conn.execute(
        "SELECT class_key, valid_from, valid_to, end_reason, last_confirmed_on, "
        "prior_holder_end, next_holder_start FROM sec_ticker_price_span(%s, %s, %s)",
        (ticker, cik, class_key),
    ).fetchall()


def test_price_span_of_a_reused_ticker_bounds_each_holder(schema_dsn) -> None:
    conn, _ = schema_dsn
    # AT&T Corp (5907) shows T, then T1; AT&T Inc (732717) then shows T.
    _observe(conn, 5907, "T", "2009-11-05")
    _observe(conn, 5907, "T", "2010-02-25")
    _observe(conn, 5907, "T1", "2010-04-01")
    _observe(conn, 732717, "T", "2010-05-07")
    _observe(conn, 732717, "T", "2010-08-06")
    d = dt.date
    assert _span(conn, "T", 732717) == [
        ("", d(2010, 5, 8), None, None, d(2010, 8, 7), d(2010, 4, 2), None),
    ]
    assert _span(conn, "T", 5907) == [
        ("", d(2009, 11, 6), d(2010, 4, 2), "other_symbol", d(2010, 2, 26), None,
         d(2010, 5, 8)),
    ]
    assert _span(conn, "T1", 5907) == [("", d(2010, 4, 2), None, None, d(2010, 4, 2), None, None)]
    assert _span(conn, "T", 999) == []


def test_price_span_ends_at_a_deregistration_and_reopens_on_a_later_statement(
    schema_dsn,
) -> None:
    conn, _ = schema_dsn
    d = dt.date
    _observe(conn, 5907, "T", "2009-11-05")
    _event(conn, 5907, "15-12G", "2010-03-15")
    _observe(conn, 732717, "T", "2010-05-07")
    assert _span(conn, "T", 5907) == [
        ("", d(2009, 11, 6), d(2010, 3, 16), "15-12G", d(2009, 11, 6), None, d(2010, 5, 8)),
    ]
    assert _span(conn, "T", 732717)[0][5] == d(2010, 3, 16)  # prior holder ended there
    # A class-specific delisting after a multi-symbol filing ends nothing.
    adsh = _observe(conn, 20, "TWO", "2021-01-10")
    _observe(conn, 20, "TWO-27", "2021-01-10", class_key="LongtermDebtType=Notes2027;",
             kind="debt", adsh=adsh)
    _event(conn, 20, "25-NSE", "2021-02-01")
    _event(conn, 20, "15-15D", "2021-03-01")
    _observe(conn, 20, "TWO", "2021-04-01")
    assert _span(conn, "TWO", 20) == [
        ("", d(2021, 1, 11), d(2021, 3, 2), "15-15D", d(2021, 1, 11), None, None),
        ("", d(2021, 4, 2), None, None, d(2021, 4, 2), None, None),
    ]


def test_price_span_follows_a_rename_and_a_later_reuse(schema_dsn) -> None:
    conn, _ = schema_dsn
    d = dt.date
    _observe(conn, 1512673, "SQ", "2024-11-05")
    _observe(conn, 1512673, "SQ", "2024-12-05")
    _observe(conn, 1512673, "XYZ", "2025-01-21")
    _observe(conn, 4242, "SQ", "2026-03-02")
    assert _span(conn, "SQ", 1512673) == [
        ("", d(2024, 11, 6), d(2025, 1, 22), "other_symbol", d(2024, 12, 6), None,
         d(2026, 3, 3)),
    ]
    assert _span(conn, "XYZ", 1512673) == [
        ("", d(2025, 1, 22), None, None, d(2025, 1, 22), None, None),
    ]
    assert _span(conn, "SQ", 4242) == [
        ("", d(2026, 3, 3), None, None, d(2026, 3, 3), d(2025, 1, 22), None),
    ]


def test_price_span_is_per_class_for_a_multi_class_issuer(schema_dsn) -> None:
    conn, _ = schema_dsn
    _two_class_issuer(conn)
    run = ("", dt.date(2024, 3, 6), None, None, dt.date(2024, 3, 6), None, None)
    assert _span(conn, "BF-B", 14693) == [(NONVOTING, *run[1:])]
    assert _span(conn, "BF-B", 14693, NONVOTING) == [(NONVOTING, *run[1:])]
    assert _span(conn, "BF-B", 14693, CLASS_A) == []
    assert _span(conn, "BF-A", 14693) == [(CLASS_A, *run[1:])]
    # A single-class filer relabelling its line is one security: both runs, no
    # other holder.
    _observe(conn, 55, "SOLO", "2020-02-10")
    _observe(conn, 55, "SOLO", "2021-02-10", class_key="ClassOfStock=CommonStock;")
    assert _span(conn, "SOLO", 55) == [
        ("", dt.date(2020, 2, 11), None, None, dt.date(2020, 2, 11), None, None),
        ("ClassOfStock=CommonStock;", dt.date(2021, 2, 11), None, None, dt.date(2021, 2, 11),
         None, None),
    ]


def test_notes_lines_tagged_with_the_common_symbol_never_decide(schema_dsn) -> None:
    conn, _ = schema_dsn
    d = dt.date
    adsh = _observe(conn, 1140859, "COR", "2025-06-05", class_key="ClassOfStock=CommonStock;")
    _observe(conn, 1140859, "COR", "2025-06-05", kind="debt", adsh=adsh,
             class_key="ClassOfStock=Sec2.875SeniorNotesDue2028;")
    _observe(conn, 1140859, "COR", "2025-06-05", kind="debt", adsh=adsh,
             class_key="ClassOfStock=AaaNotesDue2027;")  # sorts before the common line
    _observe(conn, 777, "COR", "2025-07-01", kind="debt")  # another issuer's notes line
    assert _issuer(conn, "COR", "2025-08-01") == (
        "resolved", 1140859, "ClassOfStock=CommonStock;", d(2025, 6, 6), [1140859],
    )
    assert _span(conn, "COR", 1140859) == [
        ("ClassOfStock=CommonStock;", d(2025, 6, 6), None, None, d(2025, 6, 6), None, None),
    ]
    # A ticker only ever shown on non-equity lines still resolves through them.
    _observe(conn, 36104, "USB-PA", "2025-06-05", kind="preferred")
    assert _issuer(conn, "USB-PA", "2025-06-06")[:2] == ("resolved", 36104)


def test_resolvers_inline_into_lateral_joins(schema_dsn) -> None:
    conn, _ = schema_dsn
    rows = conn.execute(
        "SELECT t, i.status FROM unnest(ARRAY['NOPE', 'NADA']) t, "
        "LATERAL sec_ticker_issuer_at(t, DATE '2024-01-01') i ORDER BY t"
    ).fetchall()
    assert rows == [("NADA", "missing"), ("NOPE", "missing")]
    for query in (
        "SELECT i.* FROM unnest(ARRAY['T']) t, LATERAL sec_ticker_issuer_at(t, DATE '2024-01-01') i",
        "SELECT i.* FROM unnest(ARRAY[1::bigint]) c, "
        "LATERAL sec_issuer_line_at(c, '', DATE '2024-01-01') i",
        "SELECT i.* FROM unnest(ARRAY[1::bigint]) c, "
        "LATERAL sec_cover_class_shares_at(c, '', DATE '2024-01-01') i",
    ):
        plan = "\n".join(r[0] for r in conn.execute("EXPLAIN " + query).fetchall())
        assert "Function Scan on sec_" not in plan, plan


def test_interval_view_lists_each_hold(schema_dsn) -> None:
    conn, _ = schema_dsn
    _observe(conn, 1512673, "SQ", "2024-11-05")
    _observe(conn, 1512673, "SQ", "2024-12-05")
    _observe(conn, 1512673, "XYZ", "2025-01-21")
    assert conn.execute(
        "SELECT ticker, valid_from, last_confirmed_on, valid_to, statements "
        "FROM sec_ticker_intervals ORDER BY valid_from"
    ).fetchall() == [
        ("SQ", dt.date(2024, 11, 6), dt.date(2024, 12, 6), dt.date(2025, 1, 22), 2),
        ("XYZ", dt.date(2025, 1, 22), dt.date(2025, 1, 22), None, 1),
    ]


def test_readers_get_select_and_execute_only(schema_dsn) -> None:
    from psycopg import sql

    conn, _ = schema_dsn
    schema = conn.execute("SELECT current_schema()").fetchone()[0]
    stranger = f"sec_ticker_stranger_{uuid4().hex[:8]}"
    created = [
        role for role in ("worker_writer", *READERS)
        if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
    ]
    for role in (*created, stranger):
        conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
    try:
        conn.execute(SCHEMA_SQL)
        for table in TABLES:
            relation = f"{schema}.{table}"
            assert conn.execute(
                "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = %s::regclass",
                (relation,),
            ).fetchone() == ("worker_writer",)
            for reader in READERS:
                assert conn.execute(
                    "SELECT has_table_privilege(%s, %s, 'SELECT'), "
                    "has_table_privilege(%s, %s, 'INSERT,UPDATE,DELETE,TRUNCATE')",
                    (reader, relation, reader, relation),
                ).fetchone() == (True, False), (reader, table)
            assert conn.execute(
                "SELECT has_table_privilege(%s, %s, 'SELECT')", (stranger, relation)
            ).fetchone() == (False,)
        for function in FUNCTIONS:
            routine = f"{schema}.{function}"
            assert conn.execute(
                "SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE oid = %s::regprocedure",
                (routine,),
            ).fetchone() == ("worker_writer",)
            for reader in READERS:
                assert conn.execute(
                    "SELECT has_function_privilege(%s, %s, 'EXECUTE')", (reader, routine)
                ).fetchone() == (True,)
            assert conn.execute(
                "SELECT has_function_privilege(%s, %s, 'EXECUTE')", (stranger, routine)
            ).fetchone() == (False,)
    finally:
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        for role in (*created, stranger):
            conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_loader_upserts_idempotently_and_converges_on_rerun(
    schema_dsn, tmp_path: Path
) -> None:
    conn, dsn = schema_dsn
    package = _sample_package(tmp_path)
    index = tmp_path / "2024QTR1.form.gz"
    index.write_bytes(gzip.compress(
        b"25-NSE           ACHILLION PHARMACEUTICALS INC      1070336     2024-01-28  "
        b"edgar/data/1070336/0001354457-24-000034.txt\n"
    ))
    first = loader.run([package], dsn=dsn, dry_run=False, form_indexes=[index])
    assert (first[0]["inserted"], first[0]["updated"], first[0]["removed"]) == (4, 0, 0)
    assert first[0]["shares_inserted"] == 3
    assert (first[1]["events"], first[1]["inserted"]) == (1, 1)
    rows = conn.execute(
        "SELECT adsh, ticker, ticker_key, cik, class_key, security_kind, available_on "
        "FROM sec_ticker_cik_observations ORDER BY adsh, ticker"
    ).fetchall()
    assert rows == [
        (A1, "BRK34", "BRK34", 1067983, "LongtermDebtType=Notes2034;", "debt",
         dt.date(2024, 2, 24)),
        (A1, "BRK-A", "BRKA", 1067983, CLASS_A, "equity", dt.date(2024, 2, 24)),
        (A1, "BRK-B", "BRKB", 1067983, "ClassOfStock=CommonClassB;", "equity",
         dt.date(2024, 2, 24)),
        (A2, "BFB", "BFB", 14693, NONVOTING, "equity", dt.date(2024, 3, 6)),
    ]
    # The resolver and the class count join on the same class.
    assert _issuer(conn, "BRK.B", "2024-02-24")[:3] == (
        "resolved", 1067983, "ClassOfStock=CommonClassB;",
    )
    assert _class_shares(conn, 1067983, "ClassOfStock=CommonClassB;", "2024-02-24") == (
        "resolved", Decimal("1389605139.0000"), dt.date(2024, 2, 12),
    )
    again = loader.run([package], dsn=dsn, dry_run=False, form_indexes=[index])
    assert (again[0]["inserted"], again[0]["updated"], again[0]["removed"]) == (0, 0, 0)
    assert (again[1]["inserted"], again[1]["updated"]) == (0, 0)

    # A re-published package without BRK.A: the stale rows of that filing go.
    package.unlink()
    _write_package(
        package,
        [_sub(A1, 1067983, "10-K", "20240226", "2024-02-24 08:00:05.0")],
        [_fact(A1, "TradingSymbol", "BRK.B", dimh="0xbbx")],
        [_shares(A1, "1389605139.0000", dimh="0xbbb", ddate="20240212")],
        DIMS,
    )
    third = loader.run([package], dsn=dsn, dry_run=False)
    assert (third[0]["inserted"], third[0]["updated"], third[0]["removed"]) == (0, 1, 2)
    assert third[0]["shares_removed"] == 1
    assert conn.execute(
        "SELECT adsh, ticker FROM sec_ticker_cik_observations ORDER BY adsh, ticker"
    ).fetchall() == [(A1, "BRK-B"), (A2, "BFB")]
    assert conn.execute(
        "SELECT source_package, submissions, symbol_facts, observations, share_counts, events "
        'FROM sec_ticker_cik_packages ORDER BY source_package COLLATE "C"'
    ).fetchall() == [("2024QTR1.form.gz", 0, 0, 0, 0, 1), ("2024q1_notes.zip", 1, 1, 1, 1, 0)]


def test_loader_refuses_a_database_without_the_governed_schema(tmp_path: Path) -> None:
    import psycopg
    from psycopg import sql

    dsn = _dsn()
    schema = f"sec_ticker_empty_{uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            with pytest.raises(RuntimeError, match="apply schemas/sec_ticker_cik_history_v1.sql"):
                loader.run(
                    [_sample_package(tmp_path)],
                    dsn=psycopg.conninfo.make_conninfo(dsn, options=f"-csearch_path={schema}"),
                    dry_run=False,
                )
        finally:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


# --------------------------------------------------------------------------- #
# Recurring worker
# --------------------------------------------------------------------------- #
FSN_BASE = "https://www.sec.gov/files/dera/data/financial-statement-notes-data-sets/"


def _fake_sec(tmp_path: Path, packages: dict[str, bytes], indexes: dict[str, bytes]):
    """An httpx client answering the SEC listing, package and index URLs."""
    import httpx

    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        calls.append((request.method, url))
        assert request.headers["User-Agent"] == loader.USER_AGENT
        if url == loader.LISTING_URL:
            links = "".join(
                f'<a href="/files/dera/data/financial-statement-notes-data-sets/{name}">x</a>'
                for name in packages
            )
            return httpx.Response(200, text=links)
        if url.startswith(FSN_BASE):
            body = packages[url[len(FSN_BASE):]]
            if request.method == "HEAD":
                return httpx.Response(200, headers={"content-length": str(len(body))})
            return httpx.Response(200, content=body)
        for key, body in indexes.items():
            if url.endswith(key):
                return httpx.Response(200, content=body)
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler),
                          headers={"User-Agent": loader.USER_AGENT})
    return client, calls


def _index_bytes(cik: int, filed: str) -> bytes:
    return gzip.compress(
        f"15-12G           SOME CO      {cik}     {filed}  "
        f"edgar/data/{cik}/{cik:010d}-24-000001.txt\n".encode()
    )


def test_worker_loads_new_packages_and_the_open_quarter_then_idles(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tempfile

    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    conn, dsn = schema_dsn
    build = tmp_path / "build"
    build.mkdir()
    first = _sample_package(build).read_bytes()
    newest = _write_package(
        build / "2024_10_notes.zip",
        [_sub("0000000005-24-000001", 732717, "8-K", "20241105")],
        [_fact("0000000005-24-000001", "TradingSymbol", "T")],
    ).read_bytes()
    packages = {"2024q1_notes.zip": first, "2024_10_notes.zip": newest}
    indexes = {"2024/QTR3/form.gz": _index_bytes(5907, "2024-08-01"),
               "2024/QTR4/form.gz": _index_bytes(5908, "2024-11-01")}

    client, calls = _fake_sec(tmp_path, packages, indexes)
    stats = worker.run(dsn, calc_date="2024-11-15", limit=1, client=client)
    assert stats["state"] == "ok" and stats["backlog"] == 2
    assert [p["package"] for p in stats["packages"]] == ["2024q1_notes.zip"]  # oldest first
    assert [i["events"] for i in stats["form_indexes"]] == [1, 1]

    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert [p["package"] for p in stats["packages"]] == ["2024_10_notes.zip"]
    assert _issuer(conn, "T", "2024-11-06")[:2] == ("resolved", 732717)

    calls.clear()
    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert stats["state"] == "noop" and stats["packages"] == [] and stats["backlog"] == 0
    assert ("GET", FSN_BASE + "2024_10_notes.zip") not in calls  # HEAD only

    # A republished newest month (different size) is reloaded.
    packages["2024_10_notes.zip"] = _write_package(
        build / "2024_10_notes.zip",
        [_sub("0000000005-24-000001", 732717, "8-K", "20241105")],
        [_fact("0000000005-24-000001", "TradingSymbol", "T"),
         _fact("0000000005-24-000001", "Security12bTitle", "Common Stock")],
    ).read_bytes()
    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert [(p["package"], p["updated"]) for p in stats["packages"]] == [("2024_10_notes.zip", 1)]
    assert stats["state"] == "ok"
    assert list(scratch.iterdir()) == []  # every run removed its downloads


def test_worker_reports_lock_busy_without_loading(schema_dsn, tmp_path: Path) -> None:
    import psycopg

    from src.db import LOCK_SEC_TICKER_CIK_HISTORY
    from src.workers import sec_ticker_cik_history as worker

    _, dsn = schema_dsn
    client, calls = _fake_sec(tmp_path, {}, {})
    with psycopg.connect(dsn, autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (LOCK_SEC_TICKER_CIK_HISTORY,))
        stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert stats["status"] == "lock_busy"
    assert calls == []
