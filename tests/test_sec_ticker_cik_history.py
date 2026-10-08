"""SEC cover-page ticker -> (CIK, class) history: parser, loader and resolvers.

Unit tests need no database. The DB tests run against a disposable loopback
PostgreSQL named by ``SEC_TEST_DATABASE_URL`` (postgres:16 in CI), each inside
its own schema, and skip when the variable is unset. Expected values are
written out by hand from the documented rules, not computed by the code under
test. Form 15/25 parsing is checked on real EDGAR filings
(tests/fixtures/sec_ticker_cik_history/filings, copied verbatim from sec.gov).
"""

from __future__ import annotations

import datetime as dt
import gzip
import itertools
import os
import random
import zipfile
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from scripts import load_sec_ticker_cik_history as loader

ROOT = Path(__file__).resolve().parents[1]
V1_SQL = (ROOT / "schemas" / "sec_ticker_cik_history_v1.sql").read_text(encoding="utf-8")
V2_SQL = (ROOT / "schemas" / "sec_ticker_cik_history_v2.sql").read_text(encoding="utf-8")
# A fresh installation applies v1, then v2 (production applied v1 on 2026-10-08).
SCHEMA_SQL = V1_SQL + V2_SQL
ROLLBACK_SQL = (ROOT / "schemas" / "sec_ticker_cik_history_v1.rollback.sql").read_text(
    encoding="utf-8"
)
V2_ROLLBACK_SQL = (ROOT / "schemas" / "sec_ticker_cik_history_v2.rollback.sql").read_text(
    encoding="utf-8"
)
FILINGS = ROOT / "tests" / "fixtures" / "sec_ticker_cik_history" / "filings"
# Cover rows of single filings carved verbatim from DERA's FSN packages (sub,
# txt cover tags, the share counts and their dim rows): fsn_<package>_<issuer>.
FSN_FIXTURES = ROOT / "tests" / "fixtures" / "sec_ticker_cik_history"
LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}
READERS = ("app_runtime", "app_analytics_ro", "mcp_ro")
FUNCTIONS = (
    "sec_observations_at(date,boolean)",
    "sec_share_counts_at(date,boolean)",
    "sec_registration_end_events(bigint,date,boolean)",
    "sec_registration_starts(bigint,date,boolean)",
    "sec_issuer_end_events(bigint,date,boolean)",
    "sec_ticker_holds(text,date,integer,boolean)",
    "sec_ticker_issuer_at(text,date,integer)",
    "sec_issuer_line_at(bigint,text,date,integer)",
    "sec_cover_class_shares_at(bigint,text,date,integer)",
    "sec_cover_ticker_shares_at(text,bigint,date,integer)",
    "sec_issuer_lines(bigint)",
    "sec_ticker_line_runs(text,integer)",
    "sec_line_alive_runs(bigint,text,integer)",
    "sec_line_price_evidence(text,bigint,text)",
    "sec_ticker_price_span(text,bigint,text)",
)
TABLES = (
    "sec_ticker_cik_observations", "sec_cover_share_counts", "sec_registration_events",
    "sec_ticker_cik_packages", "sec_ticker_cik_package_members",
    "sec_ticker_cik_package_facts", "sec_ticker_intervals",
)

SUB_HEADER = ("adsh", "cik", "name", "form", "period", "fy", "fp", "filed", "accepted",
              "nciks")
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
CLASS_B = "ClassOfStock=CommonClassB;"
NONVOTING = "ClassOfStock=NonvotingCommonStock;"
d = dt.date


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
        ("ACHR WS", ["ACHR-WS"]),  # a suffix qualifies the symbol before it
        ("BAX (NYSE)", ["BAX"]),
        ("NYSE: KO", ["KO"]),
        ("GOOGL, GOOG", ["GOOGL", "GOOG"]),
        ("BIP; BIP UN", ["BIP", "BIP-UN"]),
        # Spellings seen in SEC insider filings (W1B cross-check).
        ("(SIRI)", ["SIRI"]),
        ("[USG]", ["USG"]),
        ("(NYSE:FBC)", ["FBC"]),
        ('"""WM"""', ["WM"]),
        ("BF'B", ["BF-B"]),
        ("JWA/JWB", ["JWA", "JWB"]),
        ("CRDA CRDB", ["CRDA", "CRDB"]),
        ("Z AND ZG", ["Z", "ZG"]),
        ("NYSE/TRN", ["TRN"]),
        ("WELPP.OB", ["WELPP"]),
        ("ISCA, ISCB", ["ISCA", "ISCB"]),
        ("WSO; WSOB", ["WSO", "WSOB"]),
        ("CBS, CBS.A", ["CBS", "CBS-A"]),
        # Codex thread 4222376271: lowercase symbols joined by a slash are not prose.
        ("jwa/jwb", ["JWA", "JWB"]),
        ("belfa,belfb", ["BELFA", "BELFB"]),  # Bel Fuse, CIK 729580
        # Integrated Rail and Resources (CIK 1854795) writes its OTC Pink market.
        ("OTC Pink: IRRXU", ["IRRXU"]),
        ("BAX NYSE", ["BAX"]),  # an exchange qualifying a symbol is dropped
    ],
)
def test_symbols_normalize_to_the_price_table_style(raw: str, tickers: list[str]) -> None:
    assert loader.normalize_symbols(raw) == (tickers, [])


@pytest.mark.parametrize(
    ("raw", "ticker"),
    [
        # Codex thread 4222086476: TrueCar (CIK 1327318) tags TRUE, every one of
        # the 94 FSN facts written in capitals; "true" and "True" are booleans.
        ("TRUE", "TRUE"),
        # Codex thread 4223252982: Cboe Global Markets (CIK 1374310) tags CBOE.
        ("CBOE", "CBOE"),
        # Outbrain (CIK 1454938) tags OB; beside a symbol OB is the OTC suffix.
        ("OB", "OB"),
    ],
)
def test_real_symbols_that_look_like_placeholders_or_venues_are_kept(
    raw: str, ticker: str,
) -> None:
    assert loader.normalize_symbols(raw) == ([ticker], [])


@pytest.mark.parametrize(
    ("raw", "result"),
    [
        ("None", ([], ["placeholder"])),
        ("None.", ([], ["placeholder"])),
        ("N/A", ([], ["placeholder"])),
        ("Not Applicable", ([], ["placeholder"])),
        ("true", ([], ["placeholder"])),
        ("True", ([], ["placeholder"])),  # New York City REIT's boolean
        ("False", ([], ["placeholder"])),
        ("NA", ([], ["placeholder"])),  # TETRA, CenterPoint and others: not a symbol
        ("NONE", ([], ["placeholder"])),
        # A venue alone is no symbol (Box Ships' OTCQB, Natura's NYSE, Zion's OTCQX).
        ("OTCQB", ([], ["placeholder"])),
        ("NYSE", ([], ["placeholder"])),
        ("NYSE American", ([], ["placeholder"])),
        ("ABC, true", (["ABC"], ["placeholder"])),
        ("No Trading Symbol", ([], ["placeholder"])),
        ("XXXXXXXXXX", ([], ["placeholder"])),
        ("OTCBB", ([], ["placeholder"])),
        ("", ([], ["empty"])),
        ("Common Stock par value", ([], ["malformed"])),  # prose, not symbols
        ("1314152", ([], ["malformed"])),  # a CIK typed as the symbol
        ("AB#C", ([], ["malformed"])),
        ("EDLG, OB", (["EDLG"], ["placeholder"])),  # the OTC suffix is no symbol
    ],
)
def test_placeholders_and_junk_are_rejected_with_a_reason(
    raw: str, result: tuple[list[str], list[str]],
) -> None:
    assert loader.normalize_symbols(raw) == result


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


def test_class_key_keeps_a_legal_entity_member_used_as_a_class() -> None:
    segments = "EntityListingsExchange=NASDAQ;LegalEntity=AmericanDepositaryShares;"
    assert loader.class_key(segments, keep_entity=True) == "LegalEntity=AmericanDepositaryShares;"
    assert loader.legal_entity_member(segments) == "AmericanDepositaryShares"
    assert loader.legal_entity_member(CLASS_A) is None


@pytest.mark.parametrize(
    ("ddate", "datp", "stated"),
    [
        ("20260930", "14.0", d(2026, 9, 16)),  # Campbell 0000016732-26-000026
        ("20240131", "-13.0", d(2024, 2, 13)),  # Renalytix: after the month end
        ("20231231", "0.0", d(2023, 12, 31)),
        ("20231231", "", d(2023, 12, 31)),
    ],
)
def test_share_counts_are_dated_by_the_stated_day_not_the_rounded_month_end(
    ddate: str, datp: str, stated: dt.date,
) -> None:
    assert loader.stated_date(ddate, datp) == (stated, dt.datetime.strptime(ddate, "%Y%m%d").date())


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
        # Spelled "Depository" (14 CIKs' 8-K/10-Q/10-K covers, one at 2,000
        # ordinary shares per ADS), abbreviated "Pref", or an "Adr" member.
        ("American Depository Shares, each representing 2,000 Ordinary Shares", "XYZ", "",
         "depositary"),
        ("Depositary Shrs, each representing 1/40th intrst in a shr of 5.875% Non-Cum. Perp "
         "Pref Stock, Srs E", "XYZ-PE", "", "preferred"),
        (None, "FUTU", "ClassOfStock=Adr;", "depositary"),
        (None, "XYZ", "ClassOfStock=OtherAddress;", "equity"),
        (None, "AAPL", "", "equity"),
    ],
)
def test_security_kind_reads_the_title_then_segments_then_symbol(
    title: str | None, ticker: str, segments: str, kind: str
) -> None:
    assert loader.security_kind(title, ticker, segments) == kind


@pytest.mark.parametrize(
    ("title", "ticker", "segments", "kind"),
    [
        (None, "TSM", "", "unknown"),  # TSM's 2018 20-F: its ADS, untitled
        (None, "ABC", "ClassOfStock=OrdinarySharesMember;", "equity"),
        (None, "ABC", "ClassOfStock=CommonStockMember;", "equity"),
        (None, "ABC", "ClassOfStock=AmericanDepositarySharesMember;", "depositary"),
        (None, "ABC", "ClassOfStock=SeriesXMember;", "unknown"),
        (None, "ACHR-WS", "", "warrant"),
        ("AmericanDepositaryShares(evidencedbyAmericanDepositaryReceipts),each representing "
         "one common share", "VALE", "", "depositary"),
        ("Common Shares", "CNQ", "", "equity"),
    ],
)
def test_an_unidentified_line_on_a_foreign_issuers_form_is_unknown(
    title: str | None, ticker: str, segments: str, kind: str
) -> None:
    """Gate P1 (fc7224c): on a 20-F/40-F/6-K, an untitled symbol is an ADS as
    often as an ordinary share; without a title or a telling segment it is
    'unknown', never equity."""
    assert loader.security_kind(title, ticker, segments, foreign=True) == kind
    assert loader.is_foreign_form("20-F/A") and loader.is_foreign_form("6-K")
    assert not loader.is_foreign_form("10-K")


def test_periodic_forms_are_the_filers_own_reports() -> None:
    assert all(loader.is_periodic_form(f) for f in (
        "10-K", "10-Q", "8-K", "20-F", "40-F", "6-K", "10-KT", "10-QT", "10-K/A", "8-K/A"))
    assert not any(loader.is_periodic_form(f) for f in (
        "S-1", "S-3", "S-4", "S-8", "F-1", "F-4", "POS AM", "S-4/A", "DEF 14A", "424B3"))


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


def test_cached_indexes_of_open_quarters_are_refreshed(tmp_path: Path) -> None:
    """Codex thread 4221720847: an index downloaded before its quarter closed."""
    import os

    today = d(2026, 10, 8)

    def cached(name: str, fetched: dt.datetime) -> Path:
        path = tmp_path / name
        path.write_bytes(b"x")
        stamp = fetched.replace(tzinfo=dt.timezone.utc).timestamp()
        os.utime(path, (stamp, stamp))
        return path

    early = cached("2025QTR2.form.gz", dt.datetime(2025, 6, 20, 12))
    late = cached("2025QTR1.form.gz", dt.datetime(2025, 4, 3, 12))
    closing = cached("2024QTR4.form.gz", dt.datetime(2025, 1, 1, 6))
    assert loader.quarter_closed_on(2025, 2) == d(2025, 7, 1)
    assert loader.index_needs_refresh(early, 2025, 2, today)  # its quarter was open
    assert not loader.index_needs_refresh(late, 2025, 1, today)  # complete when fetched
    assert loader.index_needs_refresh(closing, 2024, 4, today)  # the day after it closed
    assert loader.index_needs_refresh(tmp_path / "2025QTR3.form.gz", 2025, 3, today)
    current = cached("2026QTR4.form.gz", dt.datetime(2026, 10, 8, 6))
    previous = cached("2026QTR3.form.gz", dt.datetime(2026, 10, 5, 6))
    assert loader.index_needs_refresh(current, 2026, 4, today)
    assert loader.index_needs_refresh(previous, 2026, 3, today)


def test_index_quarters_start_with_the_first_package() -> None:
    quarters = loader.quarters_through(d(2010, 5, 1))
    assert quarters == [(2009, 1), (2009, 2), (2009, 3), (2009, 4), (2010, 1), (2010, 2)]


# --------------------------------------------------------------------------- #
# Form 15 / Form 25 filings (real EDGAR documents)
# --------------------------------------------------------------------------- #
def _filing(adsh: str) -> str:
    return (FILINGS / f"{adsh}.txt").read_bytes().decode("latin-1")


@pytest.mark.parametrize(
    ("adsh", "form", "expected"),
    [
        # American Greetings: NYSE removes the listed Class A after the 2013 merger
        # (12d2-2(a)(3)); the issuer's Form 15 then covers both share classes.
        ("0000876661-13-000657", "25-NSE",
         ("Class A Common Shares", "equity", 1, "17 CFR 240.12d2-2(a)(3)", True,
          "NEW YORK STOCK EXCHANGE LLC", "primary", None)),
        ("0001193125-13-343607", "15-12B",
         ("Class A Common Shares, Par Value $1.00 Class B Common Shares, Par Value $1.00",
          "equity", 2, None, None, None, "unknown", None)),
        # PepsiCo leaves the NYSE for Nasdaq (its 8-A12B is filed the same day).
        ("0000950103-17-012553", "25",
         ("Common Stock, par value 1-2/3 cents per share", "equity", 1, None, None,
          "1-1183 PepsiCo, Inc. / New York Stock Exchange", "primary", None)),
        # IDEX withdraws a second listing on the Chicago Stock Exchange.
        ("0000832101-17-000055", "25",
         ("Common Stock, par value $0.01 per share", "equity", 1, None, None,
          "1-10235 IDEX CORPORATION; Exchange: The Chicago Stock Exchange, Inc",
          "secondary", None)),
        # Notes, preferred, a rights plan and a savings plan are other classes.
        ("0000876661-17-000048", "25-NSE",
         ("1.125% Notes due 2017", "other", 1, "17 CFR 240.12d2-2(a)(2)", True,
          "NEW YORK STOCK EXCHANGE LLC", "primary", None)),
        ("0000912593-18-000044", "15-12B",
         ("7.125% Series A Cumulative Redeemable Preferred Stock", "other", 1, None, None,
          None, "unknown", None)),
        ("0000876661-11-000305", "25-NSE",
         ("Common Stock Purchase Rights", "other", 1, "17 CFR 240.12d2-2(a)(4)", True,
          "NEW YORK STOCK EXCHANGE LLC", "primary", None)),
        ("0000746838-17-000043", "15-15D",
         ("Plan Interests under the Unisys Technical Services Savings Plan", "other", 1, None,
          None, None, "unknown", None)),
        # A small issuer terminating the registration of its common stock.
        ("0001078782-11-001558", "15-12G",
         ("Common Stock, $0.001 par value per share", "equity", 1, None, None, None,
          "unknown", None)),
        # Minim: Nasdaq's 25-NSE, then the 25-NSE/A saying it will not delist.
        ("0001354457-24-000828", "25-NSE",
         ("Common stock", "equity", 1, "17 CFR 240.12d2-2(b)", False,
          "Nasdaq Stock Market LLC", "primary", None)),
        ("0001354457-25-000301", "25-NSE/A",
         ("Common stock", "equity", 1, "17 CFR 240.12d2-2(b)", False,
          "Nasdaq Stock Market LLC", "primary", "cancels")),
        # Knight-Swift: a 25-NSE/A that repeats its original.
        ("0000876661-17-000516", "25-NSE/A",
         ("Class A Common Stock", "equity", 1, "17 CFR 240.12d2-2(a)(3)", True,
          "NEW YORK STOCK EXCHANGE LLC", "primary", "restates")),
        # PetroChina's Form 15F (Rule 12h-6) after its ADSs left the NYSE (Codex
        # thread 4222924635): the ADS and the H shares behind it.
        ("0000947871-24-000089", "15F-12B",
         ("ADSs (1) , each representing 100 Class H ordinary shares, par value RMB 1.00 per "
          "share. Class H Ordinary Shares of par value RMB1.00 per Share", "equity", 1, None,
          None, None, "unknown", None)),
        # Forms 8-A (Codex thread 4223111409): what a registration registers. PepsiCo
        # registers its common stock on Nasdaq the day its Form 25 leaves the NYSE
        # (2017), then its notes (2018); Statera's 8-A12G registers the Series B
        # Preferred Stock it paid as a dividend, the day Nasdaq delisted its common.
        ("0000950103-17-012545", "8-A12B",
         ("Common Stock, par value 1-2/3 cents per share The Nasdaq Stock Market LLC",
          "equity", 1, None, None, None, "unknown", None)),
        ("0000950103-18-014472", "8-A12B",
         ("2.500% Senior Notes due 2022 1.750% Senior Notes due 2021 2.625% Senior Notes due "
          "2026 0.875% Senior Notes due 2028 The Nasdaq Stock Market LLC The Nasdaq Stock "
          "Market LLC The Nasdaq Stock Market LLC The Nasdaq Stock Market LLC", "other", 1,
          None, None, None, "unknown", None)),
        ("0001437749-23-002250", "8-A12G",
         ("Series B Preferred Stock, par value $0.005 per share", "other", 1, None, None, None,
          "unknown", None)),
    ],
)
def test_end_filings_state_their_class_provision_and_exchange(
    adsh: str, form: str, expected: tuple,
) -> None:
    parsed = loader.parse_event_document(_filing(adsh), form)
    assert (parsed.class_description, parsed.class_kind, parsed.class_count, parsed.provision,
            parsed.extinguished, parsed.venue, parsed.venue_kind,
            parsed.amendment_effect) == expected


@pytest.mark.parametrize(
    ("description", "kind"),
    [
        ("Common Stock, par value $0.01 per share Preferred stock purchase rights", "equity"),
        ("Ordinary Shares, Units: consisting of 1 Ordinary Share and 1 Warrant", "equity"),
        ("Class X ordinary shares, par value $0.0000225 per share", "equity"),
        ("American Depositary Shares, each representing one ordinary share", "equity"),
        ("Common Units representing limited partner interests", "equity"),
        ("Warrants to purchase Common Stock", "other"),
        ("Common Share Purchase Warrants", "other"),
        ("Rights to Purchase Shares of Common Stock", "other"),
        ("Units, each consisting of one share of Class A Common Stock and one-third of one "
         "Warrant", "other"),
        ("Depositary Shares, each representing 1/40th interest in a share of 6.00% "
         "Non-Cumulative Perpetual Preferred Stock, Series B", "other"),
        ("6.625% Series I Cumulative Redeemable Preferred Shares of Beneficial Interest",
         "other"),
        ("5.75% Convertible Senior Notes", "other"),
        ("Plan interests in the XTO Energy Inc. Employees 401(k) Plan and Exxon Mobil "
         "Corporation Common Stock", "other"),
        ("", "unknown"),
        (None, "unknown"),
    ],
)
def test_class_descriptions_name_equity_or_other_classes(
    description: str | None, kind: str,
) -> None:
    assert loader.event_class_kind(description) == kind


@pytest.mark.parametrize(
    ("description", "count"),
    [
        # Real descriptions (American Greetings 15-12B, Google 25-NSE of 2015-10-02).
        ("Class A Common Shares, Par Value $1.00 Class B Common Shares, Par Value $1.00", 2),
        ("Class A Common Stock and Class C Capital Stock", 2),
        ("Class A Common Stock, $0.01 par value", 1),
        ("Common Stock", 1),
        # Enumerations (Codex thread 4221720837).
        ("Class A and B Common Stock", 2),
        ("Classes A, B and C Common Stock", 3),
        ("Class A/B Common Shares", 2),
        ("Class A or B Common Stock", 2),
        ("Class A & Class B common", 2),
        ("Class A, Inc. Common Stock", 1),
        # Series and non-letter classes (Codex thread 4221867175), real ones first
        # (0001104659-25-041383, 0001354457-13-000110).
        ("Series A common stock, par value $0.01 per share; Series B common stock, par "
         "value $0.01 per share", 2),
        ("Class A Common Stock, Class B Common Stock, Series C Common Stock", 3),
        ("Series A Common Stock and Series B Common Stock", 2),
        ("Series A and Series C Common Stock", 2),
        ("Series A Junior Participating Preferred Stock Purchase Rights", 1),
        ("Class A Common Stock; Series A Preferred Stock", 1),
        ("Class 1 Common Stock and Class 2 Common Stock", 2),
        ("Class I and Class II Common Shares", 2),
        # Classes named without a Class/Series label (Codex thread 4222376247),
        # real end filings read with sec_event_class_v3 as one class.
        ("Common Stock and Voting Common Stock", 2),
        ("Voting Common Stock, $0.01 Par Value Per Share Non-Voting Common Stock, $0.01 Par "
         "Value Per Share", 2),
        ("Common Stock, no par value; Class A Common Stock, no par value", 2),
        ("Class B-2 Common Stock, par value $0.01 per share Class B-3 Common Stock, par value "
         "$0.01 per share", 2),
        ("Series A Liberty Capital Common Stock, Series B Liberty Capital Common Stock, "
         "Liberty Starz Ser A Common Stock, Liberty Starz Ser B Common Stock", 4),
        ("Class A Common Stock - $.01 par value Class B Common Stock - $.01 par value Common "
         "Stock - $.10 par value", 3),
        # One class, however often or in whatever company it is named.
        ("Ordinary Shares (Common Stock), Representative's Unit Purchase Options, Warrants", 1),
        ('Common Stock, par value $0.001 per share (the "Common Stock") 5.625% Senior Notes '
         'due 2021', 1),
        ("Common Stock; American Depositary Shares, each representing five (5) shares of "
         "Common Stock", 1),
        ("American Depositary Shares (ADSs), each representing four shares of Common Stock "
         "Nasdaq Stock Market Common Stock, par value ARS 100.00 per share", 1),
        ("Ordinary Shares, nominal value US$0.11 3/7 American Depositary Shares each "
         "representing ten Ordinary Shares of Vodafone Group Plc", 1),
        ("Common Stock, par value $0.0001 per share; Warrants, exercisable for Common Stock at "
         "an exercise price of $12.00 per share; and Units, each consisting of one share", 1),
        ("Common Stock, $1.25 par value per share Common Stock Purchase Rights", 1),
        ("Purchase of Common Stock Warrants & Common Stock", 1),
    ],
)
def test_class_counts_read_enumerations(description: str, count: int) -> None:
    assert loader.class_count(description) == count


def test_a_filing_without_a_class_block_is_unknown_never_other() -> None:
    parsed = loader.parse_event_document("<DOCUMENT><TEXT>FORM 15</TEXT></DOCUMENT>", "15-12G")
    assert (parsed.class_kind, parsed.class_count, parsed.venue_kind) == ("unknown", 1, "unknown")


# --------------------------------------------------------------------------- #
# Package and index parsing
# --------------------------------------------------------------------------- #
def _fact(adsh: str, tag: str, value: str, *, dimh: str = "0x00000000", coreg: str = "",
          version: str = "dei/2024", iprx: int = 0, ddate: str = "20200131") -> dict[str, str]:
    return {"adsh": adsh, "tag": tag, "version": version, "ddate": ddate, "iprx": str(iprx),
            "dimh": dimh, "coreg": coreg, "value": value}


def _shares(adsh: str, value: str, *, dimh: str = "0x00000000", ddate: str = "20240229",
            datp: str = "0.0", uom: str = "shares", coreg: str = "") -> dict[str, str]:
    """A num.tsv cover count: DERA's rounded month-end ddate plus datp."""
    return {"adsh": adsh, "tag": "EntityCommonStockSharesOutstanding", "version": "dei/2024",
            "ddate": ddate, "datp": datp, "uom": uom, "dimh": dimh, "coreg": coreg,
            "value": value}


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


def _sub(adsh: str, cik: int, form: str, filed: str, accepted: str = "",
         nciks: int = 1) -> dict[str, str]:
    return {"adsh": adsh, "cik": str(cik), "name": f"CIK {cik}", "form": form,
            "period": filed, "filed": filed, "accepted": accepted, "nciks": str(nciks)}


A1 = "0000000001-24-000001"
A2 = "0000000002-24-000001"
A3 = "0000000003-24-000001"
A4 = "0000000004-24-000001"
DIMS = {
    "0xaaa": CLASS_A,
    "0xbbb": CLASS_B,
    "0xbbx": "ClassOfStock=CommonClassB;EntityListingsExchange=NYSE;",
    "0xnot": "LongtermDebtType=Notes2034;",
    "0xccc": NONVOTING,
    "0xsub": "LegalEntity=SubsidiaryMember;",
}


def _sample_package(tmp_path: Path) -> Path:
    return _write_package(
        tmp_path / "2024q1_notes.zip",
        [
            _sub(A1, 1067983, "10-K", "20240226", "2024-02-24 08:00:05.0"),
            _sub(A2, 14693, "8-K", "20240305", ""),
            _sub(A3, 99, "10-Q", "20240310", "2024-03-09 18:01:00.0"),
            # Medtronic plc's S-4 named MDT before it existed: not cover evidence.
            _sub(A4, 1613103, "S-4", "20240311", ""),
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
            _fact(A3, "TradingSymbol", "SUB", dimh="0xsub", coreg="SubsidiaryMember"),
            _fact(A3, "TradingSymbol", "CUST", version="0000000003-24-000001"),
            _fact(A3, "TradingSymbol", "None"),
            _fact("0000000009-24-000009", "TradingSymbol", "GHOST"),
            _fact(A4, "TradingSymbol", "MDT"),
        ],
        [
            # Stated 2024-02-12 and 2024-02-20; DERA rounds both to 2024-02-29.
            _shares(A1, "511820.0000", dimh="0xaaa", datp="17.0"),
            _shares(A1, "1389605139.0000", dimh="0xbbb", datp="17.0"),
            _shares(A2, "290262390", dimh="0xccc", datp="9.0"),
            _shares(A2, "12", dimh="0xccc", uom="USD"),  # not a share count
            _shares(A3, "1000", dimh="0xsub", coreg="SubsidiaryMember"),
            _shares(A4, "1000"),
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
        1067983, CLASS_B, "ClassOfStock=CommonClassB;EntityListingsExchange=NYSE;", "equity",
    )
    assert (class_b.security_title, class_b.exchange, class_b.ticker_raw) == (
        "Class B Common Stock", "NYSE", "BRK.B",
    )
    # Knowledge comes from the submission; ddate is kept, never used to date it.
    assert (class_b.filed, class_b.accepted, class_b.ddate) == (
        d(2024, 2, 26), dt.datetime(2024, 2, 24, 8, 0, 5), d(2019, 1, 1),
    )
    assert rows[(A1, "BRK34")].security_kind == "debt"
    assert rows[(A2, "BFB")].accepted is None
    # A1 (a 10-K) shows two equity classes and counts them: complete. A2 is an
    # 8-K: its count never makes it a complete inventory of classes.
    assert {key: (row.filing_equity_classes, row.filing_complete)
            for key, row in rows.items()} == {
        (A1, "BRK-A"): (2, True), (A1, "BRK-B"): (2, True), (A1, "BRK34"): (2, True),
        (A2, "BFB"): (1, False),
    }
    assert result.symbol_facts == 10
    assert dict(result.rejected) == {
        "coregistrant": 1, "non_dei_tag": 1, "placeholder": 1, "no_submission": 1,
        "duplicate_in_context": 1, "share_count_unit": 1, "share_count_coregistrant": 1,
        "non_periodic_form": 1, "share_count_non_periodic_form": 1,
    }
    shares = {(s.adsh, s.class_key): (s.shares, s.stated_on, s.ddate_rounded)
              for s in result.share_counts}
    rounded = d(2024, 2, 29)
    assert shares == {
        (A1, CLASS_A): (Decimal("511820.0000"), d(2024, 2, 12), rounded),
        (A1, CLASS_B): (Decimal("1389605139.0000"), d(2024, 2, 12), rounded),
        (A2, NONVOTING): (Decimal("290262390"), d(2024, 2, 20), rounded),
    }
    assert len(result.submissions) == 4
    assert len(result.sha256) == 64


@pytest.mark.parametrize(
    ("symbols", "others", "classes"),
    [
        ({"": {"AM"}}, set(), 1),  # one undimensioned symbol
        ({"": {"AM"}}, {CLASS_A}, 1),  # the symbol is the one counted class
        ({"": {"AM"}}, {CLASS_A, CLASS_B}, 2),  # American Greetings: AM beside A and B counts
        ({"": {"X"}, CLASS_B: {"XB"}}, {CLASS_A, CLASS_B}, 2),
        ({"": {"X"}, CLASS_B: {"XB"}}, {CLASS_B}, 2),  # the undimensioned symbol is another
        ({CLASS_A: {"AAA"}}, {CLASS_B}, 2),  # an unlisted class B ("N/A" symbol) still counts
        ({CLASS_A: {"AAA"}, CLASS_B: {"BBB"}}, {CLASS_A, CLASS_B}, 2),
        # Codex thread 4222086431: distinct symbols of one context are distinct
        # classes (JWA/JWB; Google Inc's "GOOG, GOOGL" beside its A, B and C counts).
        ({"": {"JWA", "JWB"}}, set(), 2),
        ({"": {"GOOG", "GOOGL"}}, {CLASS_A, CLASS_B, "ClassOfStock=CapitalClassC;"}, 3),
    ],
)
def test_filing_profile_counts_every_equity_class(
    symbols: dict[str, set[str]], others: set[str], classes: int,
) -> None:
    filing = (A1, 1)
    profiles = loader._filing_profiles({filing: symbols}, {filing: others}, {filing})
    assert profiles == {filing: (classes, True)}


def test_an_undimensioned_multi_symbol_fact_is_two_classes(tmp_path: Path) -> None:
    """Codex thread 4222086431: one undimensioned fact "JWA/JWB" beside the
    filing's total. The filing shows two classes, so the total is neither class's
    count."""
    path = _write_package(
        tmp_path / "2024q1_notes.zip",
        [_sub(A1, 61, "10-Q", "20240306", "2024-03-06 08:00:00.0")],
        [_fact(A1, "TradingSymbol", "JWA/JWB")],
        [_shares(A1, "55000000")],
    )
    result = loader.parse_package(path)
    assert [(o.ticker, o.class_key, o.filing_equity_classes, o.filing_complete)
            for o in result.observations] == [("JWA", "", 2, True), ("JWB", "", 2, True)]


def test_co_registrants_sharing_a_context_keep_their_own_counts(tmp_path: Path) -> None:
    """Codex thread 4222924612: the parent and a co-registrant state the same
    count (100 shares) in one context of a combined filing; each keeps its row."""
    adsh = "0000000012-24-000001"
    path = _write_package(
        tmp_path / "2024q1_notes.zip",
        [_sub(adsh, 1111, "10-K", "20240226", "2024-02-26 08:00:00.0", nciks=2)],
        [
            _fact(adsh, "TradingSymbol", "PARENT"),
            _fact(adsh, "EntityCentralIndexKey", "0000002222", dimh="0xsub",
                  coreg="SubsidiaryMember"),
        ],
        [
            _shares(adsh, "100", dimh="0xsub"),
            _shares(adsh, "100", dimh="0xsub", coreg="SubsidiaryMember"),
        ],
        DIMS,
    )
    result = loader.parse_package(path)
    assert sorted((s.cik, s.class_key, s.shares) for s in result.share_counts) == [
        (1111, "", Decimal("100")), (2222, "", Decimal("100")),
    ]


def test_an_unlisted_titled_class_counts_toward_the_filing_classes(tmp_path: Path) -> None:
    """A/AAA is listed; class B's symbol is 'N/A' but its title says equity."""
    path = _write_package(
        tmp_path / "2024q1_notes.zip",
        [_sub(A1, 61, "10-K", "20240226", "2024-02-26 08:00:00.0")],
        [
            _fact(A1, "TradingSymbol", "AAA", dimh="0xaaa"),
            _fact(A1, "Security12bTitle", "Class A Common Stock", dimh="0xaaa"),
            _fact(A1, "TradingSymbol", "N/A", dimh="0xbbb"),
            _fact(A1, "Security12bTitle", "Class B Common Stock", dimh="0xbbb"),
        ],
        [_shares(A1, "1000")],
        DIMS,
    )
    result = loader.parse_package(path)
    assert [(o.ticker, o.filing_equity_classes, o.filing_complete)
            for o in result.observations] == [("AAA", 2, True)]


RNLX = "0000950170-24-015276"
SRE = "0001032208-24-000010"


def _legal_entity_package(tmp_path: Path) -> Path:
    """Renalytix names its own ADS with a LegalEntity member; Sempra files with
    a genuine co-registrant (SDG&E) that carries its own CIK."""
    return _write_package(
        tmp_path / "2024q1_notes.zip",
        [
            _sub(RNLX, 1811115, "10-K", "20240214", "2024-02-14 16:30:00.0"),
            _sub(SRE, 1032208, "10-K", "20240227", "2024-02-27 16:10:00.0", nciks=2),
        ],
        [
            _fact(RNLX, "EntityCentralIndexKey", "0001811115"),
            _fact(RNLX, "Security12bTitle", "Ordinary shares, nominal value 0.0025 per share"),
            _fact(RNLX, "TradingSymbol", "RNLX", dimh="0xads",
                  coreg="AmericanDepositaryShares"),
            _fact(RNLX, "Security12bTitle", "American Depositary Shares, each representing "
                  "two ordinary shares", dimh="0xads", coreg="AmericanDepositaryShares"),
            _fact(RNLX, "SecurityExchangeName", "NASDAQ", dimh="0xads",
                  coreg="AmericanDepositaryShares"),
            _fact(SRE, "EntityCentralIndexKey", "0001032208"),
            _fact(SRE, "TradingSymbol", "SRE", dimh="0xsre"),
            _fact(SRE, "Security12bTitle", "Sempra Common Stock", dimh="0xsre"),
            _fact(SRE, "EntityCentralIndexKey", "0000086521", dimh="0xsdge",
                  coreg="SanDiegoGasAndElectricCompany"),
            _fact(SRE, "TradingSymbol", "SDGE-PB", dimh="0xsdgepb",
                  coreg="SanDiegoGasAndElectricCompany"),
            _fact(SRE, "Security12bTitle", "Series B Preferred Stock", dimh="0xsdgepb",
                  coreg="SanDiegoGasAndElectricCompany"),
            # A legal-entity context that names no CIK, in a multi-registrant filing.
            _fact(SRE, "TradingSymbol", "SCG", dimh="0xscg", coreg="SoCalGasMember"),
            _fact(SRE, "Security12bTitle", "SoCalGas Preferred", dimh="0xscg",
                  coreg="SoCalGasMember"),
        ],
        [
            _shares(RNLX, "99930156", ddate="20240131", datp="-13.0"),
            _shares(SRE, "631000000", dimh="0xsre", ddate="20240229", datp="9.0"),
            _shares(SRE, "116583358", dimh="0xsdge", coreg="SanDiegoGasAndElectricCompany",
                    ddate="20240229", datp="9.0"),
        ],
        {
            "0xads": "LegalEntity=AmericanDepositaryShares;",
            "0xsre": "ClassOfStock=CommonStock;",
            "0xsdge": "LegalEntity=SanDiegoGasAndElectricCompany;",
            "0xsdgepb": "ClassOfStock=SeriesBPreferredStock;"
                        "LegalEntity=SanDiegoGasAndElectricCompany;",
            "0xscg": "LegalEntity=SoCalGasMember;",
        },
    )


def test_legal_entity_contexts_resolve_to_the_entity_they_name(tmp_path: Path) -> None:
    result = loader.parse_package(_legal_entity_package(tmp_path))
    rows = {o.ticker: (o.cik, o.class_key, o.security_kind, o.filing_equity_classes)
            for o in result.observations}
    assert rows == {
        # Single registrant, no CIK on the member, a titled context: its own ADS class.
        "RNLX": (1811115, "LegalEntity=AmericanDepositaryShares;", "depositary", 1),
        "SRE": (1032208, "ClassOfStock=CommonStock;", "equity", 1),
        # The member carries SDG&E's own CIK: the line is SDG&E's, not Sempra's.
        "SDGE-PB": (86521, "ClassOfStock=SeriesBPreferredStock;", "preferred", 0),
    }
    assert result.rejected["coregistrant"] == 1  # SCG: a member without a CIK, 2 registrants
    assert result.rejected["attributed_to_coregistrant"] == 1
    shares = {(s.cik, s.class_key): (s.shares, s.stated_on) for s in result.share_counts}
    assert shares == {
        (1811115, ""): (Decimal("99930156"), d(2024, 2, 13)),
        (1032208, "ClassOfStock=CommonStock;"): (Decimal("631000000"), d(2024, 2, 20)),
        (86521, ""): (Decimal("116583358"), d(2024, 2, 20)),
    }


def test_two_registrants_sharing_the_undimensioned_context_stay_apart(
    tmp_path: Path,
) -> None:
    """Codex thread 4222086457: the parent's and a co-registrant's facts share
    dimh 0x00000000 in a combined filing; the co-registrant's row (listed last)
    must not take the parent's symbol with it."""
    adsh = "0000000011-24-000001"
    path = _write_package(
        tmp_path / "2024q1_notes.zip",
        [_sub(adsh, 1111, "10-K", "20240226", "2024-02-26 08:00:00.0", nciks=2)],
        [
            _fact(adsh, "TradingSymbol", "PARENT"),
            _fact(adsh, "Security12bTitle", "Common Stock"),
            _fact(adsh, "TradingSymbol", "SUBX", coreg="SubsidiaryMember"),
            _fact(adsh, "EntityCentralIndexKey", "0000002222", coreg="SubsidiaryMember"),
        ],
    )
    result = loader.parse_package(path)
    assert [(o.ticker, o.cik, o.security_title) for o in result.observations] == [
        ("PARENT", 1111, "Common Stock"),
    ]
    assert result.rejected["coregistrant"] == 1  # no legal-entity member to attribute it


def test_an_empty_package_listing_is_an_error() -> None:
    """Codex thread 4222086469."""
    httpx = pytest.importorskip("httpx")

    def answer(status: int, text: str):
        return httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(status, text=text)))

    with pytest.raises(RuntimeError, match="no package links"):
        loader.list_package_urls(answer(200, "<html>maintenance</html>"))
    with pytest.raises(httpx.HTTPStatusError):
        loader.list_package_urls(answer(503, ""))


def test_a_corrupt_package_member_fails_the_parse(tmp_path: Path) -> None:
    stored = _write_package(
        tmp_path / "2024q2_notes.zip",
        [_sub(A1, 1, "10-K", "20240226")],
        [_fact(A1, "TradingSymbol", "GHOSTLY")],
        compression=zipfile.ZIP_STORED,
    )
    data = bytearray(stored.read_bytes())
    data[data.index(b"GHOSTLY")] ^= 0x01  # same length, different bytes: CRC must catch it
    stored.write_bytes(bytes(data))
    with pytest.raises(zipfile.BadZipFile):
        loader.parse_package(stored)


def test_form_index_keeps_registration_end_and_start_rows(tmp_path: Path) -> None:
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
        "25-NSE/A         ACHILLION PHARMACEUTICALS INC                     1070336     "
        "2020-02-10  edgar/data/1070336/0001354457-20-000099.txt",
        "8-A12B           PEPSICO INC                                       77476       "
        "2020-02-11  edgar/data/77476/0000950103-20-000001.txt",
        "8-K12B           NEWCO INC                                         888         "
        "2020-02-12  edgar/data/888/0000000888-20-000001.txt",
        # A foreign private issuer's terminations (Codex thread 4222924635).
        "15F-12B          PETROCHINA CO LTD                                 1108329     "
        "2020-02-05  edgar/data/1108329/0000947871-20-000089.txt",
        "15F-15D/A        SOME FOREIGN PLC                                  999001      "
        "2020-03-05  edgar/data/999001/0000999001-20-000002.txt",
    ]
    path.write_bytes(gzip.compress(("\n".join(rows) + "\n").encode("latin-1")))
    events, sha256, size = loader.parse_form_index(path)
    assert [(e.form, e.cik, e.filed, e.adsh) for e in events] == [
        # A successor's 8-K12B registers the class it substitutes.
        ("8-K12B", 888, d(2020, 2, 12), "0000000888-20-000001"),
        ("15-12G", 5907, d(2020, 3, 2), "0000005907-20-000001"),
        ("15F-12B", 1108329, d(2020, 2, 5), "0000947871-20-000089"),
        ("8-A12B", 77476, d(2020, 2, 11), "0000950103-20-000001"),
        ("15F-15D/A", 999001, d(2020, 3, 5), "0000999001-20-000002"),
        ("25", 1070336, d(2020, 1, 29), "0001070336-20-000003"),
        ("25-NSE", 1070336, d(2020, 1, 28), "0001354457-20-000034"),
        ("25-NSE/A", 1070336, d(2020, 2, 10), "0001354457-20-000099"),
    ]
    assert all(e.class_kind is None and e.parser_version is None for e in events)
    assert len(sha256) == 64 and size == path.stat().st_size


class _Response:
    def __init__(self, status: int, content: bytes = b"", headers: dict | None = None):
        self.status_code = status
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        raise RuntimeError(self.status_code)


class _Client:
    """Answers EDGAR filing URLs from a script of responses."""

    def __init__(self, script: list[_Response]):
        self.script = list(script)
        self.urls: list[str] = []

    def get(self, url: str) -> _Response:
        self.urls.append(url)
        return self.script.pop(0)


def test_event_filings_are_fetched_politely_and_cached(tmp_path: Path) -> None:
    body = (FILINGS / "0000876661-13-000657.txt").read_bytes()
    client = _Client([_Response(503, headers={"retry-after": "0"}), _Response(200, body),
                      _Response(404), _Response(403)])
    documents = loader.EventDocuments(tmp_path / "docs", client, spacing=0)
    assert documents.text(5133, "0000876661-13-000657") == body.decode("latin-1")
    assert client.urls == [  # the submission text in its accession folder
        "https://www.sec.gov/Archives/edgar/data/5133/000087666113000657/"
        "0000876661-13-000657.txt"] * 2
    assert (tmp_path / "docs" / "0000876661-13-000657.txt").read_bytes() == body
    assert documents.text(5133, "0000876661-13-000657") == body.decode("latin-1")  # cached
    assert len(client.urls) == 2
    # Codex thread 4221400830: an HTTP error is "not fetched", never an empty document.
    assert documents.text(5133, "0000000000-13-000001") is None  # 404
    assert documents.text(5133, "0000000000-13-000003") is None  # 403
    assert (documents.fetched, documents.failed) == (1, 2)
    assert not (tmp_path / "docs" / "0000000000-13-000001.txt").exists()
    offline = loader.EventDocuments(tmp_path / "docs", None)
    assert offline.text(5133, "0000000000-13-000002") is None  # not cached, no client
    assert offline.failed == 0


def test_end_filings_of_cover_ciks_are_described(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    for adsh in ("0000876661-13-000657", "0001193125-13-343607"):
        (docs / f"{adsh}.txt").write_bytes((FILINGS / f"{adsh}.txt").read_bytes())
    events = [
        loader.RegistrationEvent("0000876661-13-000657", 5133, "25-NSE", d(2013, 8, 12), "q"),
        loader.RegistrationEvent("0001193125-13-343607", 5133, "15-12B", d(2013, 8, 22), "q"),
        loader.RegistrationEvent("0000000001-13-000001", 5133, "10-12B", d(2013, 9, 1), "q"),
        loader.RegistrationEvent("0000000002-13-000001", 4242, "15-12G", d(2013, 9, 1), "q"),
        loader.RegistrationEvent("0000000003-13-000001", 5133, "15-15D", d(2013, 9, 2), "q"),
    ]
    described, stats = loader.describe_events(
        events, loader.EventDocuments(docs, None), {5133})
    assert [(e.form, e.class_kind, e.class_count, e.extinguished, e.parser_version)
            for e in described] == [
        ("25-NSE", "equity", 1, True, loader.EVENT_PARSER_VERSION),
        ("15-12B", "equity", 2, None, loader.EVENT_PARSER_VERSION),
        ("10-12B", None, None, None, None),  # a Form 10 is not read
        ("15-12G", None, None, None, None),  # no cover data for CIK 4242
        ("15-15D", None, None, None, None),  # not cached and no client
    ]
    assert dict(stats) == {"class_equity": 2, "class_unread": 1, "filings_missing": 1}
    assert described[0].fact_hash != events[0].fact_hash  # the class is part of the fact


def test_an_event_read_by_this_parser_is_carried_without_a_fetch(tmp_path: Path) -> None:
    """Codex thread 4223252991: a worker without a persistent cache must not fetch
    every Form 15/25/8-A of both quarters again each week. An event already read
    by this parser version is carried; one read by another version is read again."""
    adsh = "0000876661-13-000657"
    event = loader.RegistrationEvent(adsh, 5133, "25-NSE", d(2013, 8, 12), "2013QTR3.form.gz")
    read = loader.RegistrationEvent(
        adsh, 5133, "25-NSE", d(2013, 8, 12), "old", class_kind="equity", class_count=1,
        extinguished=True, venue_kind="primary", parser_version=loader.EVENT_PARSER_VERSION)
    client = _Client([_Response(200, (FILINGS / f"{adsh}.txt").read_bytes())])
    documents = loader.EventDocuments(tmp_path / "docs", client, spacing=0)
    key = (adsh, 5133, "25-NSE", d(2013, 8, 12))
    described, stats = loader.describe_events([event], documents, {5133}, {key: read})
    assert described == [replace(read, source_package="2013QTR3.form.gz")]
    assert (dict(stats), client.urls) == ({"class_reused": 1}, [])
    older = replace(read, parser_version="sec_event_class_v3")
    described, stats = loader.describe_events([event], documents, {5133}, {key: older})
    assert described[0].parser_version == loader.EVENT_PARSER_VERSION
    assert (dict(stats), len(client.urls)) == ({"class_equity": 1}, 1)


def test_a_body_that_is_not_the_submission_is_never_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4222376284: a 200 maintenance page is no filing. It is not
    cached, counts as rejected, and the class derived before is carried."""
    monkeypatch.setattr(loader.time, "sleep", lambda seconds: None)
    adsh = "0000876661-13-000657"
    page = b"<html><body>EDGAR is undergoing maintenance.</body></html>"
    client = _Client([_Response(200, page)] * 6)
    documents = loader.EventDocuments(tmp_path / "docs", client, spacing=0)
    assert documents.text(5133, adsh) is None
    assert (documents.fetched, documents.failed, documents.rejected) == (0, 0, 1)
    assert len(client.urls) == 3  # tried three times
    assert not (tmp_path / "docs" / f"{adsh}.txt").exists()
    event = loader.RegistrationEvent(adsh, 5133, "25-NSE", d(2013, 8, 12), "q")
    prior = replace(event, class_kind="other", class_count=1,
                           parser_version="sec_event_class_v3")
    key = (adsh, 5133, "25-NSE", d(2013, 8, 12))
    described, stats = loader.describe_events([event], documents, {5133}, {key: prior})
    assert described == [prior] and stats["class_carried"] == 1
    # A non-filing body cached before this check is not read as a filing either.
    (tmp_path / "docs").mkdir(exist_ok=True)
    (tmp_path / "docs" / f"{adsh}.txt").write_bytes(page)
    offline = loader.EventDocuments(tmp_path / "docs", None)
    assert offline.text(5133, adsh) is None and offline.rejected == 1
    real = (FILINGS / f"{adsh}.txt").read_bytes()
    assert loader.is_submission(real.decode("latin-1"), adsh)
    assert not loader.is_submission(real.decode("latin-1"), "0000876661-13-000658")


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


_SEQ = itertools.count(1)


def _adsh(year: int = 24) -> str:
    return f"{next(_SEQ):010d}-{year:02d}-000001"


def _profile(conn, adsh: str, cik: int) -> None:
    """The loader's per-filing profile (tested above on parsed packages): equity
    classes from the filing's equity symbols and dimensioned counts, an
    undimensioned symbol beside an unnamed counted class being one of them; and
    whether the filing reports a count."""
    conn.execute(
        """
        WITH s AS (
            SELECT DISTINCT class_key FROM sec_ticker_cik_observations
            WHERE adsh = %(a)s AND cik = %(c)s AND retired_on IS NULL
              AND security_kind IN ('equity', 'depositary', 'unknown')
        ), k AS (
            SELECT DISTINCT class_key FROM sec_cover_share_counts
            WHERE adsh = %(a)s AND cik = %(c)s AND retired_on IS NULL AND class_key <> ''
        )
        UPDATE sec_ticker_cik_observations SET
            filing_equity_classes =
                (SELECT count(*) FROM (SELECT class_key FROM s UNION SELECT class_key FROM k) u)
                - CASE WHEN EXISTS (SELECT 1 FROM s WHERE class_key = '')
                            AND EXISTS (SELECT 1 FROM k WHERE class_key NOT IN (
                                SELECT class_key FROM s))
                       THEN 1 ELSE 0 END,
            filing_complete = EXISTS (
                SELECT 1 FROM sec_cover_share_counts
                WHERE adsh = %(a)s AND cik = %(c)s AND retired_on IS NULL)
        WHERE adsh = %(a)s AND cik = %(c)s AND retired_on IS NULL
        """,
        {"a": adsh, "c": cik},
    )


def _observe(conn, cik: int, ticker: str, filed: str, *, accepted: str | None = None,
             class_key: str = "", kind: str = "equity", adsh: str | None = None,
             title: str | None = None, available_on: str | None = None,
             retired_on: str | None = None) -> str:
    adsh = adsh or _adsh()
    conn.execute(
        "INSERT INTO sec_ticker_cik_observations (fact_hash, adsh, cik, dimh, segments, "
        "class_key, ticker, ticker_raw, security_title, security_kind, filing_equity_classes, "
        "filing_complete, form, filed, accepted, available_on, retired_on, loaded_on, "
        "source_package) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, false, '10-Q', "
        "%s, %s, COALESCE(%s::date, %s::timestamp::date, %s::date + 1), %s, "
        "CURRENT_DATE, 'test')",
        (uuid4().hex, adsh, cik, class_key or "0x00000000", class_key, class_key, ticker,
         ticker, title, kind, filed, accepted, available_on, accepted, filed, retired_on),
    )
    _profile(conn, adsh, cik)
    return adsh


def _count(conn, cik: int, class_key: str, stated: str, shares: int, filed: str,
           adsh: str | None = None, accepted: str | None = None) -> str:
    adsh = adsh or _adsh()
    conn.execute(
        "INSERT INTO sec_cover_share_counts (fact_hash, adsh, cik, dimh, segments, class_key, "
        "stated_on, ddate_rounded, shares, form, filed, accepted, available_on, loaded_on, "
        "source_package) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '10-Q', %s, %s, "
        "COALESCE(%s::timestamp::date, %s::date + 1), CURRENT_DATE, 'test')",
        (uuid4().hex, adsh, cik, class_key or "0x00000000", class_key, class_key, stated,
         stated, shares, filed, accepted, accepted, filed),
    )
    _profile(conn, adsh, cik)
    return adsh


def _event(conn, cik: int, form: str, filed: str, *, kind: str | None = None, count: int = 1,
           extinguished: bool | None = None, venue_kind: str | None = None,
           effect: str | None = None, adsh: str | None = None,
           available_on: str | None = None, description: str | None = None) -> str:
    """An index row; ``kind`` set means its filing was read (class_kind)."""
    adsh = adsh or _adsh()
    read = kind is not None
    conn.execute(
        "INSERT INTO sec_registration_events (fact_hash, adsh, cik, form, filed, class_kind, "
        "class_count, extinguished, venue_kind, amendment_effect, parser_version, "
        "class_description, available_on, loaded_on, source_package) VALUES (%s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s::date, %s::date + 1), CURRENT_DATE, "
        "'test')",
        (uuid4().hex, adsh, cik, form, filed, kind, count if read else None,
         extinguished, venue_kind, effect, "test" if read else None, description,
         available_on, filed),
    )
    return adsh


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


def _ticker_shares(conn, ticker: str, cik: int, as_of: str) -> tuple:
    return conn.execute(
        "SELECT status, shares, shares_as_of, basis "
        "FROM sec_cover_ticker_shares_at(%s, %s, %s)", (ticker, cik, as_of),
    ).fetchone()


def _span(conn, ticker: str, cik: int, class_key: str | None = None) -> list[tuple]:
    return conn.execute(
        "SELECT class_key, valid_from, valid_to, end_reason, last_confirmed_on, "
        "prior_holder_end, next_holder_start FROM sec_ticker_price_span(%s, %s, %s)",
        (ticker, cik, class_key),
    ).fetchall()


def _ends(conn, cik: int, as_of: str) -> list[tuple]:
    """(form, the date the end takes effect, definitive) of the ends at D."""
    return conn.execute(
        "SELECT form, effective_on, definitive FROM sec_issuer_end_events(%s, %s) "
        "ORDER BY effective_on, adsh", (cik, as_of),
    ).fetchall()


def test_schema_reapplies_and_rolls_back_cleanly(schema_dsn) -> None:
    conn, _ = schema_dsn
    conn.execute(V2_SQL)  # idempotent (v1 cannot replace the functions v2 reshaped)
    _observe(conn, 732717, "T", "2024-01-10")
    assert conn.execute("SELECT ticker_key, available_on FROM sec_ticker_cik_observations"
                        ).fetchone() == ("T", d(2024, 1, 11))
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
    assert _issuer(conn, "BBB", "2024-03-05") == ("resolved", 2, "", d(2024, 3, 5), [2])


@pytest.mark.parametrize(("age_days", "status"), [(400, "resolved"), (401, "stale")])
def test_open_interval_goes_stale_after_400_days_without_confirmation(
    schema_dsn, age_days: int, status: str,
) -> None:
    conn, _ = schema_dsn
    _observe(conn, 7, "OLD", "2023-01-09", accepted="2023-01-10 10:00:00")
    as_of = d(2023, 1, 10) + dt.timedelta(days=age_days)
    assert _issuer(conn, "OLD", as_of.isoformat()) == (
        status, 7 if status == "resolved" else None, "" if status == "resolved" else None,
        d(2023, 1, 10), [7],
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
    assert _issuer(conn, "T", "2010-03-01") == ("resolved", 5907, "", d(2010, 2, 26), [5907])
    assert _issuer(conn, "T", "2010-03-16")[:2] == ("ended", None)  # 15-12G public 03-16
    assert _issuer(conn, "T", "2010-05-08") == ("resolved", 732717, "", d(2010, 5, 8), [732717])
    assert _issuer(conn, "T", "2011-01-03")[:2] == ("resolved", 732717)


def test_reused_ticker_without_an_end_event_overlaps_until_the_old_hold_is_stale(
    schema_dsn,
) -> None:
    conn, _ = schema_dsn
    _att(conn, deregistered=False)
    # AT&T Corp's interval stays open (no different symbol, no deregistration):
    # two issuers hold "T" until its last statement (2010-02-26) is 400 days old.
    assert _issuer(conn, "T", "2010-05-08") == (
        "ambiguous", None, None, d(2010, 5, 8), [732717, 5907],
    )
    assert _issuer(conn, "T", "2011-04-02")[0] == "ambiguous"  # 2010-02-26 + 400
    assert _issuer(conn, "T", "2011-04-03")[:2] == ("resolved", 732717)


def test_an_unread_delisting_ends_only_a_single_symbol_issuer(schema_dsn) -> None:
    conn, _ = schema_dsn
    _observe(conn, 10, "ONE", "2021-01-10")
    adsh = _observe(conn, 20, "TWO", "2021-01-10")
    _observe(conn, 20, "TWO-27", "2021-01-10", class_key="LongtermDebtType=Notes2027;",
             kind="debt", adsh=adsh)
    _event(conn, 10, "25-NSE", "2021-02-01")
    _event(conn, 20, "25-NSE", "2021-02-01")  # could be the notes: does not end TWO
    assert _issuer(conn, "ONE", "2021-02-02")[:2] == ("ended", None)
    assert _issuer(conn, "TWO", "2021-02-02")[:2] == ("resolved", 20)
    # Nor does an unread 15-15D: it too could be the notes' (Codex thread
    # 4222924619); after a single-symbol filing it ends the hold.
    _event(conn, 20, "15-15D", "2021-03-01")
    assert _issuer(conn, "TWO", "2021-03-02")[:2] == ("resolved", 20)
    _event(conn, 10, "15-15D", "2021-03-01")
    _observe(conn, 10, "ONE", "2021-02-20")  # reopened after the 25-NSE, then
    assert _issuer(conn, "ONE", "2021-03-02")[:2] == ("ended", None)
    _observe(conn, 10, "ONE", "2021-04-01")  # not definitive: a later statement reopens
    assert _issuer(conn, "ONE", "2021-04-02")[:2] == ("resolved", 10)


def test_rename_ends_the_old_symbol_and_the_line_shows_what_it_traded_as(schema_dsn) -> None:
    conn, _ = schema_dsn
    q = _observe(conn, 1512673, "SQ", "2024-11-05")
    _count(conn, 1512673, "", "2024-10-31", 600, "2024-11-05", adsh=q)
    _observe(conn, 1512673, "XYZ", "2025-01-21", accepted="2025-01-21 08:01:00")
    assert _issuer(conn, "SQ", "2025-01-20")[:2] == ("resolved", 1512673)
    assert _issuer(conn, "SQ", "2025-01-21")[:2] == ("ended", None)
    assert _issuer(conn, "XYZ", "2024-12-31")[:2] == ("missing", None)
    assert _issuer(conn, "XYZ", "2025-01-21")[:2] == ("resolved", 1512673)
    # The current symbol's line asked earlier shows the symbol it traded under then.
    assert _line(conn, 1512673, "", "2024-12-31") == ("resolved", "", ["SQ"], d(2024, 11, 6), 1)
    # A later issuer reusing "SQ" holds it from its own first statement.
    _observe(conn, 4242, "SQ", "2026-03-02")
    assert _issuer(conn, "SQ", "2026-03-03")[:2] == ("resolved", 4242)
    assert _issuer(conn, "SQ", "2024-12-31")[:2] == ("resolved", 1512673)


def test_concurrent_holders_are_ambiguous(schema_dsn) -> None:
    conn, _ = schema_dsn
    _observe(conn, 30, "TIE", "2022-01-10")
    _observe(conn, 40, "TIE", "2022-01-10")
    assert _issuer(conn, "TIE", "2022-02-01") == (
        "ambiguous", None, None, d(2022, 1, 11), [30, 40],
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
        "resolved", 14693, NONVOTING, d(2024, 3, 6), [14693],
    )
    assert _issuer(conn, "BF.A", "2024-03-06")[:3] == ("resolved", 14693, CLASS_A)
    assert _line(conn, 14693, NONVOTING, "2024-03-06") == (
        "resolved", NONVOTING, ["BFB"], d(2024, 3, 6), 2,
    )
    assert _class_shares(conn, 14693, NONVOTING, "2024-03-06") == (
        "resolved", Decimal(290_262_390), d(2024, 2, 28),
    )
    assert _class_shares(conn, 14693, CLASS_A, "2024-03-05")[0] == "missing"  # filed that day
    assert _class_shares(conn, 14693, "", "2024-03-06")[0] == "missing"  # no total reported


def test_single_class_issuer_is_followed_through_a_renamed_member(schema_dsn) -> None:
    conn, _ = schema_dsn
    old = _observe(conn, 55, "OLDSYM", "2020-02-10")  # no dimension then
    _count(conn, 55, "", "2020-01-31", 100, "2020-02-10", adsh=old)
    new = _observe(conn, 55, "NEWSYM", "2021-02-10", class_key="ClassOfStock=CommonStock;")
    _count(conn, 55, "ClassOfStock=CommonStock;", "2021-01-31", 100, "2021-02-10", adsh=new)
    assert _line(conn, 55, "ClassOfStock=CommonStock;", "2020-06-01") == (
        "resolved", "", ["OLDSYM"], d(2020, 2, 11), 1,
    )
    assert _line(conn, 77, "", "2020-06-01") == ("missing", None, [], None, 0)


def test_the_line_of_a_class_is_point_in_time(schema_dsn) -> None:
    """Review 2, item 1: a later filing where A is the sole class must not change
    what class A traded as at an earlier date."""
    conn, _ = schema_dsn
    jan = _observe(conn, 61, "AAA", "2024-01-10", class_key=CLASS_A)
    _observe(conn, 61, "BBB", "2024-01-10", class_key=CLASS_B, adsh=jan)
    _count(conn, 61, CLASS_A, "2023-12-31", 10, "2024-01-10", adsh=jan)
    _count(conn, 61, CLASS_B, "2023-12-31", 20, "2024-01-10", adsh=jan)
    _observe(conn, 61, "BBB", "2024-02-10", class_key=CLASS_B)  # an 8-K listing only B
    before = _line(conn, 61, CLASS_A, "2024-03-01")
    assert before == ("resolved", CLASS_A, ["AAA"], d(2024, 1, 11), 2)
    apr = _observe(conn, 61, "BBB", "2024-04-10")  # A is gone; the sole class trades as BBB
    _count(conn, 61, "", "2024-03-31", 20, "2024-04-10", adsh=apr)
    assert _line(conn, 61, CLASS_A, "2024-03-01") == before
    assert _issuer(conn, "AAA", "2024-03-01")[:3] == ("resolved", 61, CLASS_A)


def test_class_share_count_rules(schema_dsn) -> None:
    conn, _ = schema_dsn
    _count(conn, 9, CLASS_A, "2023-01-31", 100, "2023-02-10")
    assert _class_shares(conn, 9, CLASS_A, "2023-02-10")[0] == "missing"
    assert _class_shares(conn, 9, CLASS_A, "2023-02-11") == (
        "resolved", Decimal(100), d(2023, 1, 31),
    )
    _count(conn, 9, CLASS_A, "2023-01-31", 120, "2023-03-01")  # amendment, same date
    assert _class_shares(conn, 9, CLASS_A, "2023-03-02")[:2] == ("resolved", Decimal(120))
    filing = _count(conn, 9, CLASS_A, "2023-04-30", 130, "2023-05-10")
    _count(conn, 9, CLASS_A, "2023-04-30", 131, "2023-05-10", adsh=filing)  # same filing
    assert _class_shares(conn, 9, CLASS_A, "2023-05-11")[:2] == ("ambiguous", None)
    assert _class_shares(conn, 9, CLASS_A, "2024-06-03")[0] == "ambiguous"  # 2023-04-30 + 400
    assert _class_shares(conn, 9, CLASS_A, "2024-06-04")[0] == "stale"


def test_a_same_day_amendment_replaces_the_original_count(schema_dsn) -> None:
    conn, _ = schema_dsn
    # Ordered by acceptance time, not accession: the amendment has the lower one.
    _count(conn, 9, CLASS_A, "2023-04-28", 100, "2023-05-10", accepted="2023-05-10 09:00:00",
           adsh="0000000009-23-000009")
    _count(conn, 9, CLASS_A, "2023-04-28", 120, "2023-05-10", accepted="2023-05-10 15:00:00",
           adsh="0000000001-23-000001")
    assert _class_shares(conn, 9, CLASS_A, "2023-05-10") == (
        "resolved", Decimal(120), d(2023, 4, 28),
    )


def test_same_day_statements_are_ordered_by_acceptance_time(schema_dsn) -> None:
    """Codex thread 4215648958: a rename filed the same day as an older-style report."""
    conn, _ = schema_dsn
    _observe(conn, 8, "OLD", "2024-01-11", accepted="2024-01-11 09:00:00",
             adsh="0000000009-24-000888")
    _observe(conn, 8, "NEW", "2024-01-11", accepted="2024-01-11 15:00:00",
             adsh="0000000001-24-000888")
    assert _issuer(conn, "OLD", "2024-01-11")[:2] == ("ended", None)
    assert _issuer(conn, "NEW", "2024-01-11")[:2] == ("resolved", 8)
    assert _line(conn, 8, "", "2024-01-11")[:3] == ("resolved", "", ["NEW"])
    assert _span(conn, "OLD", 8) == []  # superseded on the day it appeared


def test_a_count_is_found_from_its_stated_day_not_the_rounded_month_end(schema_dsn) -> None:
    conn, _ = schema_dsn
    # Campbell: stated 2026-09-16, accepted 2026-09-24, DERA ddate 2026-09-30.
    _count(conn, 16732, "", "2026-06-01", 298_000_000, "2026-06-10")
    _count(conn, 16732, "", "2026-09-16", 298_234_693, "2026-09-24",
           accepted="2026-09-24 16:05:00")
    for as_of in ("2026-09-24", "2026-09-29"):
        assert _class_shares(conn, 16732, "", as_of) == (
            "resolved", Decimal(298_234_693), d(2026, 9, 16),
        )


def test_future_filings_never_change_an_earlier_answer(schema_dsn) -> None:
    conn, _ = schema_dsn
    _att(conn, deregistered=True)
    _two_class_issuer(conn)
    _observe(conn, 1512673, "SQ", "2024-11-05")
    as_ofs = ("2010-03-01", "2010-05-08", "2024-03-06", "2024-12-31")
    probes = [
        ("SELECT * FROM sec_ticker_issuer_at(%s, %s)", t) for t in ("T", "BF-B", "BF-A", "SQ")
    ] + [
        ("SELECT * FROM sec_issuer_line_at(%s, %s, %s)", (14693, NONVOTING)),
        ("SELECT * FROM sec_issuer_line_at(%s, %s, %s)", (1512673, "")),
        ("SELECT * FROM sec_cover_class_shares_at(%s, %s, %s)", (14693, NONVOTING)),
    ]

    def answers() -> list:
        out = []
        for as_of in as_ofs:
            for query, args in probes:
                params = (args, as_of) if isinstance(args, str) else (*args, as_of)
                out.append(conn.execute(query, params).fetchall())
        return out

    before = answers()
    # Everything below becomes public after the last probe date.
    _observe(conn, 1512673, "XYZ", "2025-01-21")  # rename
    _observe(conn, 999, "T", "2025-02-01")  # a later claim on T
    _event(conn, 732717, "15-12B", "2025-03-01", kind="equity", count=1)
    _event(conn, 14693, "15-15D", "2025-03-01")
    _observe(conn, 14693, "BFB", "2025-03-05", class_key=CLASS_B)
    _count(conn, 14693, NONVOTING, "2024-02-28", 1, "2025-04-01")  # late restatement
    _count(conn, 14693, NONVOTING, "2025-02-28", 2, "2025-03-05")
    assert answers() == before


PIT_DAY = d(2022, 6, 30)


def _random_history(conn, rng: random.Random, start: dt.date, end: dt.date,
                    filings: int) -> list[tuple[str, int, dt.date]]:
    """Random filings of every kind public in [start, end]; returns accessions."""
    ciks = (101, 102, 103, 104)
    tickers = ("AAA", "BBB", "CCC", "DDD")
    classes = ("", CLASS_A, CLASS_B)
    made = []
    for _ in range(filings):
        filed = start + dt.timedelta(days=rng.randrange((end - start).days))
        cik = rng.choice(ciks)
        adsh = _adsh(22)
        for class_key in rng.sample(classes, rng.randint(1, 2)):
            _observe(conn, cik, rng.choice(tickers), filed.isoformat(), class_key=class_key,
                     adsh=adsh, kind=rng.choice(("equity", "equity", "equity", "debt")),
                     title=rng.choice((None, None, "Common Stock")))
            if rng.random() < 0.6:
                _count(conn, cik, class_key,
                       (filed - dt.timedelta(days=rng.randrange(40))).isoformat(),
                       rng.randrange(1, 10**6), filed.isoformat(), adsh=adsh)
        made.append((adsh, cik, filed))
        roll = rng.random()
        if roll < 0.25:
            form = rng.choice(loader.END_FORMS)
            kind = rng.choice((None, "equity", "other", "unknown"))
            on = filed + dt.timedelta(days=rng.randrange(1, 60))
            if on < end:
                _event(conn, cik, form, on.isoformat(), kind=kind, count=rng.randint(1, 2),
                       extinguished=rng.choice((None, True, False)) if kind else None,
                       venue_kind=rng.choice((None, "primary", "secondary")) if kind else None)
        elif roll < 0.35:
            on = filed + dt.timedelta(days=rng.randrange(1, 60))
            if on < end:
                _event(conn, cik, rng.choice(loader.REGISTRATION_FORMS), on.isoformat())
    return made


def _point_answers(conn, as_of: dt.date) -> list:
    out = []
    for ticker in ("AAA", "BBB", "CCC", "DDD"):
        out.append(conn.execute("SELECT * FROM sec_ticker_issuer_at(%s, %s)",
                                (ticker, as_of)).fetchall())
        out.append(conn.execute("SELECT * FROM sec_ticker_holds(%s, %s) ORDER BY cik",
                                (ticker, as_of)).fetchall())
        for cik in (101, 102, 103, 104):
            out.append(conn.execute("SELECT * FROM sec_cover_ticker_shares_at(%s, %s, %s)",
                                    (ticker, cik, as_of)).fetchall())
    for cik in (101, 102, 103, 104):
        out.append(conn.execute(
            "SELECT * FROM sec_issuer_end_events(%s, %s) ORDER BY available_on, adsh",
            (cik, as_of)).fetchall())
        out.append(conn.execute(
            "SELECT * FROM sec_registration_end_events(%s, %s) ORDER BY available_on, adsh",
            (cik, as_of)).fetchall())
        for class_key in ("", CLASS_A, CLASS_B):
            out.append(conn.execute("SELECT * FROM sec_issuer_line_at(%s, %s, %s)",
                                    (cik, class_key, as_of)).fetchall())
            out.append(conn.execute("SELECT * FROM sec_cover_class_shares_at(%s, %s, %s)",
                                    (cik, class_key, as_of)).fetchall())
    return out


@pytest.mark.parametrize("seed", range(5))
def test_point_answers_are_invariant_to_later_knowledge(schema_dsn, seed: int) -> None:
    """Review 2, item 1: append rows of every kind public after D (observations,
    counts, events, amendments) and reconciliations dated after D (retirements,
    corrections of known accessions): every point function's answer at D is
    unchanged."""
    conn, _ = schema_dsn
    rng = random.Random(seed)
    made = _random_history(conn, rng, d(2020, 1, 1), PIT_DAY, 40)
    before = _point_answers(conn, PIT_DAY)
    after = PIT_DAY + dt.timedelta(days=1)
    _random_history(conn, rng, after, d(2023, 12, 31), 25)
    for adsh, cik, filed in rng.sample(made, 10):
        later = (after + dt.timedelta(days=rng.randrange(200))).isoformat()
        # A reconciliation after D retires the accession's facts and corrects it.
        conn.execute("UPDATE sec_ticker_cik_observations SET retired_on = %s "
                     "WHERE adsh = %s AND retired_on IS NULL", (later, adsh))
        conn.execute("UPDATE sec_cover_share_counts SET retired_on = %s "
                     "WHERE adsh = %s AND retired_on IS NULL", (later, adsh))
        _observe(conn, cik, rng.choice(("AAA", "EEE")), filed.isoformat(), adsh=adsh,
                 available_on=later)
        # An amendment filed after D of an end filed before D.
        original = conn.execute(
            "SELECT form FROM sec_registration_events WHERE cik = %s AND filed <= %s "
            "AND form NOT LIKE '%%/A' AND form IN ('15-12B', '15-12G', '15-15D', '25', "
            "'25-NSE') LIMIT 1", (cik, PIT_DAY)).fetchone()
        if original:
            _event(conn, cik, original[0] + "/A", later, kind="equity",
                   effect=rng.choice(("cancels", "restates")))
        conn.execute("UPDATE sec_registration_events SET retired_on = %s "
                     "WHERE cik = %s AND filed <= %s AND retired_on IS NULL "
                     "AND form = '15-12G'", (later, cik, PIT_DAY))
    assert _point_answers(conn, PIT_DAY) == before


def test_price_span_of_a_reused_ticker_bounds_each_holder(schema_dsn) -> None:
    conn, _ = schema_dsn
    # AT&T Corp (5907) shows T, then T1; AT&T Inc (732717) then shows T.
    _observe(conn, 5907, "T", "2009-11-05")
    _observe(conn, 5907, "T", "2010-02-25")
    _observe(conn, 5907, "T1", "2010-04-01")
    _observe(conn, 732717, "T", "2010-05-07")
    _observe(conn, 732717, "T", "2010-08-06")
    assert _span(conn, "T", 732717) == [
        ("", d(2010, 5, 8), None, None, d(2010, 8, 7), d(2010, 4, 2), None),
    ]
    assert _span(conn, "T", 5907) == [
        ("", d(2009, 11, 6), d(2010, 4, 2), "other_symbol", d(2010, 2, 26), None,
         d(2010, 5, 8)),
    ]
    assert _span(conn, "T1", 5907) == [("", d(2010, 4, 2), None, None, d(2010, 4, 2), None, None)]
    assert _span(conn, "T", 999) == []


def test_an_open_prior_run_admits_nothing_before_the_next_holder(schema_dsn) -> None:
    """Light #223 thread: AT&T Corp shows no end; its last confirmation
    (2010-02-26) is not one, so AT&T Inc admits no row before its own start and
    AT&T Corp none from that start."""
    conn, _ = schema_dsn
    _att(conn, deregistered=False)
    assert _span(conn, "T", 732717) == [
        ("", d(2010, 5, 8), None, None, d(2010, 8, 7), d(2010, 5, 8), None),
    ]
    assert _span(conn, "T", 5907) == [
        ("", d(2009, 11, 6), None, None, d(2010, 2, 26), None, d(2010, 5, 8)),
    ]


def test_lineage_judges_an_end_with_everything_known_today(schema_dsn) -> None:
    """PepsiCo's 8-A12B could follow its Form 25 by days: point-in-time the 25
    ends PEP until the registration is public; lineage knows it was a transfer."""
    conn, _ = schema_dsn
    _observe(conn, 77476, "PEP", "2017-10-04")
    _event(conn, 77476, "25", "2017-12-19", kind="equity", venue_kind="primary")
    _event(conn, 77476, "8-A12B", "2017-12-27")
    assert _issuer(conn, "PEP", "2017-12-21")[0] == "ended"
    assert _issuer(conn, "PEP", "2017-12-28")[:2] == ("resolved", 77476)
    assert _span(conn, "PEP", 77476) == [
        ("", d(2017, 10, 5), None, None, d(2017, 10, 5), None, None),
    ]


def test_price_span_ends_at_a_deregistration_and_reopens_on_a_later_statement(
    schema_dsn,
) -> None:
    conn, _ = schema_dsn
    _observe(conn, 5907, "T", "2009-11-05")
    _event(conn, 5907, "15-12G", "2010-03-15")
    _observe(conn, 732717, "T", "2010-05-07")
    assert _span(conn, "T", 5907) == [
        ("", d(2009, 11, 6), d(2010, 3, 16), "15-12G", d(2009, 11, 6), None, d(2010, 5, 8)),
    ]
    assert _span(conn, "T", 732717)[0][5] == d(2010, 3, 16)  # prior holder ended there
    # An unread delisting or deregistration after a filing with two symbols ends
    # nothing (Codex thread 4222924619); after a single-symbol filing it does.
    adsh = _observe(conn, 20, "TWO", "2021-01-10")
    _observe(conn, 20, "TWO-27", "2021-01-10", class_key="LongtermDebtType=Notes2027;",
             kind="debt", adsh=adsh)
    _event(conn, 20, "25-NSE", "2021-02-01")
    _event(conn, 20, "15-15D", "2021-03-01")
    _observe(conn, 20, "TWO", "2021-04-01")
    assert _span(conn, "TWO", 20) == [
        ("", d(2021, 1, 11), None, None, d(2021, 4, 2), None, None),
    ]
    _observe(conn, 21, "ONE", "2021-01-10")
    _event(conn, 21, "15-15D", "2021-03-01")
    _observe(conn, 21, "ONE", "2021-04-01")
    assert _span(conn, "ONE", 21) == [
        ("", d(2021, 1, 11), d(2021, 3, 2), "15-15D", d(2021, 1, 11), None, None),
        ("", d(2021, 4, 2), None, None, d(2021, 4, 2), None, None),
    ]


def test_price_span_follows_a_rename_and_a_later_reuse(schema_dsn) -> None:
    conn, _ = schema_dsn
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
    run = ("", d(2024, 3, 6), None, None, d(2024, 3, 6), None, None)
    assert _span(conn, "BF-B", 14693) == [(NONVOTING, *run[1:])]
    assert _span(conn, "BF-B", 14693, NONVOTING) == [(NONVOTING, *run[1:])]
    assert _span(conn, "BF-B", 14693, CLASS_A) == []
    assert _span(conn, "BF-A", 14693) == [(CLASS_A, *run[1:])]
    # A single-class filer relabelling its class is one line: one run, no
    # other holder.
    _observe(conn, 55, "SOLO", "2020-02-10")
    _observe(conn, 55, "SOLO", "2021-02-10", class_key="ClassOfStock=CommonStock;")
    assert _span(conn, "SOLO", 55) == [
        ("ClassOfStock=CommonStock;", d(2020, 2, 11), None, None, d(2021, 2, 11), None, None),
    ]


def test_notes_lines_tagged_with_the_common_symbol_never_decide(schema_dsn) -> None:
    conn, _ = schema_dsn
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


def test_a_ticker_count_follows_the_filing_not_the_member_name(schema_dsn) -> None:
    conn, _ = schema_dsn
    # Berkshire's 10-Q tags BRK.B and counts it on 'CommonClassB'; its later 8-K
    # tags BRK.B on 'ClassBCommonStock' and counts nothing.
    q = _observe(conn, 1067983, "BRK-A", "2022-05-02", class_key=CLASS_A)
    _observe(conn, 1067983, "BRK-B", "2022-05-02", class_key=CLASS_B, adsh=q)
    _count(conn, 1067983, CLASS_A, "2022-04-20", 613_707, "2022-05-02", adsh=q)
    _count(conn, 1067983, CLASS_B, "2022-04-20", 1_285_751_332, "2022-05-02", adsh=q)
    k = _observe(conn, 1067983, "BRK-A", "2022-05-04", class_key="ClassOfStock=ClassACommonStock;")
    _observe(conn, 1067983, "BRK-B", "2022-05-04", class_key="ClassOfStock=ClassBCommonStock;",
             adsh=k)
    assert _issuer(conn, "BRK-B", "2022-06-30")[2] == "ClassOfStock=ClassBCommonStock;"
    assert _class_shares(conn, 1067983, "ClassOfStock=ClassBCommonStock;", "2022-06-30")[0] == (
        "missing"
    )
    assert _ticker_shares(conn, "BRK.B", 1067983, "2022-06-30") == (
        "resolved", Decimal(1_285_751_332), d(2022, 4, 20), "class",
    )
    # A single-class filer's total is its one class's count.
    a = _observe(conn, 320193, "AAPL", "2024-02-02")
    _count(conn, 320193, "", "2024-01-19", 15_441_881_000, "2024-02-02", adsh=a)
    assert _ticker_shares(conn, "AAPL", 320193, "2024-02-05") == (
        "resolved", Decimal(15_441_881_000), d(2024, 1, 19), "sole_class_total",
    )
    # Two classes in one filing never share a total.
    g = _observe(conn, 1652044, "GOOGL", "2024-02-01", class_key=CLASS_A)
    _observe(conn, 1652044, "GOOG", "2024-02-01", class_key="ClassOfStock=CapitalClassC;",
             adsh=g)
    _count(conn, 1652044, "", "2024-01-25", 12_000_000_000, "2024-02-01", adsh=g)
    assert _ticker_shares(conn, "GOOGL", 1652044, "2024-02-05")[0] == "missing"


def test_an_issuer_total_never_goes_to_a_depositary_or_one_of_several_classes(
    schema_dsn,
) -> None:
    """Review 2, items 3 and 4."""
    conn, _ = schema_dsn
    # An unsegmented ADS (five ordinary shares each) beside the unsegmented
    # ordinary total: the total counts ordinary shares, not ADSs.
    r = _observe(conn, 1811115, "ADSX", "2024-02-14", kind="depositary")
    _count(conn, 1811115, "", "2024-02-13", 1000, "2024-02-14", adsh=r)
    assert _ticker_shares(conn, "ADSX", 1811115, "2024-02-20")[0] == "missing"
    # An unsegmented class beside a segmented second class.
    u = _observe(conn, 71, "UNSEG", "2024-02-14")
    _observe(conn, 71, "SEGB", "2024-02-14", class_key=CLASS_B, adsh=u)
    _count(conn, 71, "", "2024-02-13", 1000, "2024-02-14", adsh=u)
    assert _ticker_shares(conn, "UNSEG", 71, "2024-02-20")[0] == "missing"
    # A/AAA listed, unlisted class B ('N/A' symbol, so no row) counted at 400,
    # issuer total 1,000: A's count is not the total.
    a = _observe(conn, 72, "AAA", "2024-02-14", class_key=CLASS_A)
    _count(conn, 72, CLASS_B, "2024-02-13", 400, "2024-02-14", adsh=a)
    _count(conn, 72, "", "2024-02-13", 1000, "2024-02-14", adsh=a)
    assert _ticker_shares(conn, "AAA", 72, "2024-02-20")[0] == "missing"
    older = _observe(conn, 72, "AAA", "2023-11-14", class_key=CLASS_A)
    _count(conn, 72, CLASS_A, "2023-11-10", 600, "2023-11-14", adsh=older)
    assert _ticker_shares(conn, "AAA", 72, "2024-02-20") == (
        "resolved", Decimal(600), d(2023, 11, 10), "class",
    )


def _fsn_fixture(name: str, target: Path) -> Path:
    """A package named ``target`` holding the carved real rows of ``name``."""
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for member in ("sub.tsv", "txt.tsv", "num.tsv", "dim.tsv"):
            archive.write(FSN_FIXTURES / name / member, member)
        archive.writestr("pre.tsv", "adsh\treport\n")  # never read
    return target


UNVERIFIED = "foreign_issuer_listing_unverified"


def _ticker_shares_row(conn, ticker: str, cik: int, as_of: str) -> tuple:
    return conn.execute(
        "SELECT status, shares, shares_as_of, adsh, basis, refusal "
        "FROM sec_cover_ticker_shares_at(%s, %s, %s)", (ticker, cik, as_of),
    ).fetchone()


def test_no_count_on_a_foreign_issuers_filing_sizes_its_listed_line(
    schema_dsn, tmp_path: Path,
) -> None:
    """Gate P1 (fc7224c) and its decision (C), through ingestion of real DERA rows.
    A 20-F counts the underlying shares, and its cover may tag the ADS symbol
    untitled (TSM 2018), on the underlying class titled as such (TSM 2024 "Common
    Shares", 5 per ADS; America Movil's AMOV/AMX on its A/L shares, 20 per ADS)
    or on the ordinary class beside the ADS class (FUTU, 8 per ADS), or title the
    underlying class's member as the ADS (AMX 2023, B shares). No such count
    sizes the line: it is refused, loudly, with the filing kept for audit."""
    conn, dsn = schema_dsn
    packages = [
        _fsn_fixture("fsn_2018q2_tsm", tmp_path / "2018q2_notes.zip"),
        _fsn_fixture("fsn_2021q2_amov", tmp_path / "2021q2_notes.zip"),
        _fsn_fixture("fsn_2022q1_futu", tmp_path / "2022q1_notes.zip"),
        _fsn_fixture("fsn_2023q2_amx", tmp_path / "2023q2_notes.zip"),
        _fsn_fixture("fsn_2024q2_tsm", tmp_path / "2024q2_notes.zip"),
    ]
    loader.run(packages, dsn=dsn, dry_run=False, reconciled_on=d(2024, 6, 1))
    tsm18, tsm24 = "0001193125-18-121866", "0001193125-24-099840"
    # TSM 2018: an untitled, undimensioned symbol on a 20-F is 'unknown'.
    assert conn.execute(
        "SELECT ticker, class_key, security_title, security_kind, filing_equity_classes, "
        "filing_complete FROM sec_ticker_cik_observations WHERE adsh = %s", (tsm18,),
    ).fetchall() == [("TSM", "", None, "unknown", 1, True)]
    assert conn.execute(
        "SELECT status, cik, class_key, security_kind FROM sec_ticker_issuer_at('TSM', %s)",
        ("2018-12-31",),
    ).fetchone() == ("resolved", 1046179, "", "unknown")
    assert _ticker_shares_row(conn, "TSM", 1046179, "2018-12-31") == (
        "refused", None, d(2017, 12, 31), tsm18, "sole_class_total", UNVERIFIED,
    )
    assert conn.execute(
        "SELECT status, shares, adsh, refusal FROM sec_cover_class_shares_at(%s, '', %s)",
        (1046179, "2018-12-31"),
    ).fetchone() == ("refused", None, tsm18, UNVERIFIED)
    # TSM 2024: titled "Common Shares" (the registered class behind the ADS).
    assert _ticker_shares_row(conn, "TSM", 1046179, "2024-12-31") == (
        "refused", None, d(2023, 12, 31), tsm24, "sole_class_total", UNVERIFIED,
    )
    # America Movil 2021: AMOV on its A shares, AMX on its L shares.
    amov = "0001193125-21-137773"
    for ticker in ("AMOV", "AMX"):
        assert _ticker_shares_row(conn, ticker, 1129137, "2021-12-31") == (
            "refused", None, d(2020, 12, 31), amov, "class", UNVERIFIED,
        )
    # FUTU 2022: tagged on its ADR class and on Class A (counted); the Class A
    # count is the class's own fact, never the listed line's.
    futu = "0001104659-22-035332"
    assert _ticker_shares_row(conn, "FUTU", 1754581, "2022-12-31") == (
        "refused", None, d(2021, 12, 31), futu, "class", UNVERIFIED,
    )
    assert _class_shares(conn, 1754581, "ClassOfStock=CommonClassA;", "2022-12-31") == (
        "resolved", Decimal("708482154"), d(2021, 12, 31),
    )
    # AMX 2023: a depositary line by its title, but on the B shares' member, whose
    # count is of B shares: not an explicit depositary member.
    amx = "0001193125-23-129777"
    assert conn.execute(
        "SELECT status, security_kind FROM sec_ticker_issuer_at('AMX', %s)", ("2023-06-30",),
    ).fetchone() == ("resolved", "depositary")
    assert _ticker_shares_row(conn, "AMX", 1129137, "2023-06-30") == (
        "refused", None, d(2023, 3, 31), amx, "class", UNVERIFIED,
    )


def test_a_foreign_issuers_explicit_depositary_class_keeps_its_count(schema_dsn) -> None:
    """Decision (C): a count dimensioned to an explicit depositary class is the
    line's own count and keeps the existing rules; a domestic total is unchanged."""
    conn, _ = schema_dsn

    def foreign(adsh: str) -> None:
        conn.execute("UPDATE sec_ticker_cik_observations SET form = '20-F' WHERE adsh = %s",
                     (adsh,))
        conn.execute("UPDATE sec_cover_share_counts SET form = '20-F/A' WHERE adsh = %s",
                     (adsh,))

    ads = "ClassOfStock=Adr;"
    a = _observe(conn, 91, "ADRX", "2024-03-01", class_key=ads, kind="depositary")
    _count(conn, 91, ads, "2024-02-29", 700, "2024-03-01", adsh=a)
    foreign(a)
    assert _ticker_shares_row(conn, "ADRX", 91, "2024-06-30") == (
        "resolved", Decimal(700), d(2024, 2, 29), a, "class", None,
    )
    # The same issuer's later 20-F tags the symbol on its ordinary class too: the
    # admissible ADS count of that filing wins over the refused ordinary one.
    b = _observe(conn, 91, "ADRX", "2025-03-01", class_key=ads, kind="depositary")
    _observe(conn, 91, "ADRX", "2025-03-01", class_key=CLASS_A, adsh=b)
    _count(conn, 91, ads, "2025-02-28", 710, "2025-03-01", adsh=b)
    _count(conn, 91, CLASS_A, "2025-02-28", 7100, "2025-03-01", adsh=b)
    foreign(b)
    assert _ticker_shares_row(conn, "ADRX", 91, "2025-06-30") == (
        "resolved", Decimal(710), d(2025, 2, 28), b, "class", None,
    )
    # A 10-K total is untouched.
    k = _observe(conn, 92, "DOMX", "2024-03-01")
    _count(conn, 92, "", "2024-02-29", 500, "2024-03-01", adsh=k)
    assert _ticker_shares_row(conn, "DOMX", 92, "2024-06-30") == (
        "resolved", Decimal(500), d(2024, 2, 29), k, "sole_class_total", None,
    )


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
        "SELECT i.* FROM unnest(ARRAY['T']) t, "
        "LATERAL sec_cover_ticker_shares_at(t, 1, DATE '2024-01-01') i",
    ):
        plan = "\n".join(r[0] for r in conn.execute("EXPLAIN " + query).fetchall())
        assert "Function Scan on sec_" not in plan, plan


def test_class_relabel_with_a_rename_closes_the_old_symbol_everywhere(schema_dsn) -> None:
    """A single-class issuer drops the dimension and renames in the same step."""
    conn, _ = schema_dsn
    old = _observe(conn, 55, "OLDSYM", "2024-02-10")  # available 2024-02-11, class ''
    _count(conn, 55, "", "2024-01-31", 100, "2024-02-10", adsh=old)
    new = _observe(conn, 55, "NEWSYM", "2024-04-10", class_key="ClassOfStock=CommonStock;")
    _count(conn, 55, "ClassOfStock=CommonStock;", "2024-03-31", 100, "2024-04-10", adsh=new)
    assert _issuer(conn, "OLDSYM", "2024-05-01")[:2] == ("ended", None)
    assert _issuer(conn, "NEWSYM", "2024-05-01")[:3] == (
        "resolved", 55, "ClassOfStock=CommonStock;",
    )
    assert _line(conn, 55, "ClassOfStock=CommonStock;", "2024-03-01") == (
        "resolved", "", ["OLDSYM"], d(2024, 2, 11), 1,
    )
    assert _span(conn, "OLDSYM", 55) == [
        ("", d(2024, 2, 11), d(2024, 4, 11), "other_symbol", d(2024, 2, 11), None, None),
    ]
    # Another issuer takes OLDSYM: it holds it alone, not jointly with CIK 55.
    _observe(conn, 66, "OLDSYM", "2024-06-03")
    assert _issuer(conn, "OLDSYM", "2024-06-05") == ("resolved", 66, "", d(2024, 6, 4), [66])
    assert _span(conn, "OLDSYM", 66)[0][5] == d(2024, 4, 11)  # prior holder's end


def test_sole_and_multi_class_transitions_end_the_old_symbol(schema_dsn) -> None:
    """Review 2, item 5, both directions."""
    conn, _ = schema_dsn
    # January: one class trading as OLD. February: it renames to NEW and a class
    # B appears; both are dimensioned now.
    jan = _observe(conn, 81, "OLD", "2024-01-10")
    _count(conn, 81, "", "2023-12-31", 100, "2024-01-10", adsh=jan)
    feb = _observe(conn, 81, "NEW", "2024-02-10", class_key=CLASS_A)
    _observe(conn, 81, "NEWB", "2024-02-10", class_key=CLASS_B, adsh=feb)
    _count(conn, 81, CLASS_A, "2024-01-31", 100, "2024-02-10", adsh=feb)
    _count(conn, 81, CLASS_B, "2024-01-31", 50, "2024-02-10", adsh=feb)
    assert _issuer(conn, "OLD", "2024-03-01")[:2] == ("ended", None)
    assert _issuer(conn, "NEW", "2024-03-01")[:3] == ("resolved", 81, CLASS_A)
    # Two classes collapse into one, which trades as ONE: neither old symbol is
    # still listed.
    jan = _observe(conn, 82, "AAA", "2024-01-10", class_key=CLASS_A)
    _observe(conn, 82, "BBB", "2024-01-10", class_key=CLASS_B, adsh=jan)
    _count(conn, 82, CLASS_A, "2023-12-31", 10, "2024-01-10", adsh=jan)
    _count(conn, 82, CLASS_B, "2023-12-31", 20, "2024-01-10", adsh=jan)
    feb = _observe(conn, 82, "ONE", "2024-02-10")
    _count(conn, 82, "", "2024-01-31", 30, "2024-02-10", adsh=feb)
    assert [_issuer(conn, t, "2024-03-01")[0] for t in ("AAA", "BBB", "ONE")] == [
        "ended", "ended", "resolved",
    ]


def test_registration_amendments_supersede_exactly_one_original(schema_dsn) -> None:
    """Review 2, item 7: two same-day originals, one amendment."""
    conn, _ = schema_dsn
    _observe(conn, 91, "TWIN", "2024-01-02")
    first = _event(conn, 91, "25-NSE", "2024-01-10", adsh="0000000091-24-000001")
    second = _event(conn, 91, "25-NSE", "2024-01-10", adsh="0000000091-24-000002")
    early = _event(conn, 91, "25-NSE/A", "2024-01-05", kind="equity", effect="cancels")
    assert early  # filed before the originals: it amends neither
    _event(conn, 91, "25-NSE/A", "2024-02-01", kind="equity", effect="cancels")
    assert [e[0] for e in conn.execute(
        "SELECT adsh FROM sec_registration_end_events(91, '2024-03-01') ORDER BY adsh"
    ).fetchall()] == [first]
    assert second > first
    assert _issuer(conn, "TWIN", "2024-03-01")[:2] == ("ended", None)


def test_a_delisting_amendment_withdraws_the_original_from_its_own_date(schema_dsn) -> None:
    """Minim: 25-NSE 2024-10-24, then 25-NSE/A 2025-04-09 (Nasdaq will not delist)."""
    conn, _ = schema_dsn
    _observe(conn, 1467761, "MINM", "2024-08-13")
    _event(conn, 1467761, "25-NSE", "2024-10-24", kind="equity", extinguished=False,
           venue_kind="primary")
    _event(conn, 1467761, "25-NSE/A", "2025-04-09", kind="equity", extinguished=False,
           venue_kind="primary", effect="cancels")
    assert _issuer(conn, "MINM", "2024-10-24")[:2] == ("resolved", 1467761)
    assert _issuer(conn, "MINM", "2024-10-25")[:2] == ("ended", None)  # original public
    assert _issuer(conn, "MINM", "2025-04-09")[:2] == ("ended", None)  # amendment not yet
    assert _issuer(conn, "MINM", "2025-04-10")[:2] == ("resolved", 1467761)
    assert _line(conn, 1467761, "", "2025-01-02")[0] == "ended"
    assert _line(conn, 1467761, "", "2025-04-10")[0] == "resolved"
    # Lineage uses today's knowledge: the withdrawn delisting ends nothing.
    assert _span(conn, "MINM", 1467761) == [
        ("", d(2024, 8, 14), None, None, d(2024, 8, 14), None, None),
    ]
    # A later original of the same form is not covered by the earlier amendment.
    _event(conn, 1467761, "25-NSE", "2025-09-01", kind="equity", venue_kind="primary")
    assert _issuer(conn, "MINM", "2025-09-02")[:2] == ("ended", None)


def test_unread_and_restating_amendments_leave_the_end_in_force(schema_dsn) -> None:
    conn, _ = schema_dsn
    _observe(conn, 92, "UNR", "2024-01-02")
    _event(conn, 92, "25-NSE", "2024-01-10")
    _event(conn, 92, "25-NSE/A", "2024-01-20")  # not read: no effect known
    assert _issuer(conn, "UNR", "2024-02-01")[:2] == ("ended", None)
    _observe(conn, 93, "RST", "2024-01-02")
    _event(conn, 93, "25-NSE", "2024-01-10", kind="equity", venue_kind="primary")
    _event(conn, 93, "25-NSE/A", "2024-01-20", kind="equity", venue_kind="primary",
           effect="restates")
    assert _issuer(conn, "RST", "2024-02-01")[:2] == ("ended", None)
    # A restatement that names another class withdraws the end from its date.
    _event(conn, 93, "25-NSE/A", "2024-03-01", kind="other", effect="restates")
    assert _issuer(conn, "RST", "2024-02-15")[:2] == ("ended", None)
    assert _issuer(conn, "RST", "2024-03-02")[:2] == ("resolved", 93)


def test_a_restatement_that_makes_an_end_apply_counts_from_the_amendment(
    schema_dsn,
) -> None:
    """Codex thread 4220893560: read as notes, then restated as the common stock;
    a cover statement filed in between must not outrank the restated end."""
    conn, _ = schema_dsn
    _observe(conn, 94, "RSA", "2024-01-02")
    _event(conn, 94, "25-NSE", "2024-01-10", kind="other", venue_kind="primary")
    _observe(conn, 94, "RSA", "2024-02-01")
    _event(conn, 94, "25-NSE/A", "2024-03-01", kind="equity", venue_kind="primary",
           effect="restates")
    assert _ends(conn, 94, "2024-02-15") == []
    assert _issuer(conn, "RSA", "2024-02-15")[:2] == ("resolved", 94)
    assert _ends(conn, 94, "2024-03-05") == [("25-NSE", d(2024, 3, 2), False)]
    assert _issuer(conn, "RSA", "2024-03-05")[:2] == ("ended", None)
    _observe(conn, 94, "RSA", "2024-04-01")  # not definitive: a later statement reopens
    assert _issuer(conn, "RSA", "2024-04-05")[:2] == ("resolved", 94)
    # An end that applied as filed keeps its own date when restated.
    _observe(conn, 96, "KEEPD", "2024-01-02")
    _event(conn, 96, "25-NSE", "2024-01-10", kind="equity", venue_kind="primary")
    _event(conn, 96, "25-NSE/A", "2024-03-01", kind="equity", count=1, venue_kind="primary",
           effect="restates")
    assert _ends(conn, 96, "2024-03-05") == [("25-NSE", d(2024, 1, 11), False)]


def _american_greetings(conn) -> None:
    """CIK 5133 listed AM (Class A; Class B unlisted, both counted). Merger: NYSE's
    25-NSE (12d2-2(a)(3)) on 2013-08-12 and the Form 15-12B naming both classes
    on 2013-08-22; it kept filing 10-Qs tagged AM until 2017 (100 shares, held by
    the buyer). Antero Midstream Partners (CIK 1598968) took AM in November 2014."""
    q = _observe(conn, 5133, "AM", "2013-07-10")
    _count(conn, 5133, CLASS_A, "2013-07-01", 29_294_198, "2013-07-10", adsh=q)
    _count(conn, 5133, CLASS_B, "2013-07-01", 2_912_167, "2013-07-10", adsh=q)
    _event(conn, 5133, "25-NSE", "2013-08-12", kind="equity", count=1, extinguished=True,
           venue_kind="primary")
    _event(conn, 5133, "15-12B", "2013-08-22", kind="equity", count=2, venue_kind="unknown")
    for filed in ("2013-10-10", "2014-01-09", "2014-10-10", "2015-01-09"):
        later = _observe(conn, 5133, "AM", filed)
        _count(conn, 5133, "", filed, 100, filed, adsh=later)
    _observe(conn, 1598968, "AM", "2014-11-13")
    _observe(conn, 1598968, "AM", "2015-02-26")


def test_statements_after_a_definitive_delisting_do_not_reopen_the_hold(schema_dsn) -> None:
    """Insider cross-check, item 9 (American Greetings)."""
    conn, _ = schema_dsn
    _american_greetings(conn)
    # The listed class is extinguished, but the issuer had two classes: the
    # 25-NSE alone is not definitive; with the Form 15 for both classes it is.
    assert _ends(conn, 5133, "2013-08-15") == [("25-NSE", d(2013, 8, 13), False)]
    assert _ends(conn, 5133, "2013-09-01") == [
        ("25-NSE", d(2013, 8, 13), False), ("15-12B", d(2013, 8, 23), True),
    ]
    assert _issuer(conn, "AM", "2013-08-13")[:2] == ("ended", None)
    for as_of in ("2013-11-01", "2014-01-15", "2014-10-15"):
        assert _issuer(conn, "AM", as_of)[:2] == ("ended", None), as_of
    assert _issuer(conn, "AM", "2014-11-20")[:2] == ("resolved", 1598968)
    assert _issuer(conn, "AM", "2015-03-01") == (
        "resolved", 1598968, "", d(2015, 2, 27), [1598968],
    )
    assert _line(conn, 5133, "", "2014-06-01")[0] == "ended"
    assert _span(conn, "AM", 1598968)[0][5] == d(2013, 8, 13)  # prior holder's end
    assert _span(conn, "AM", 5133) == [
        ("", d(2013, 7, 11), d(2013, 8, 13), "25-NSE", d(2013, 7, 11), None, d(2014, 11, 14)),
    ]


def test_only_a_relisting_reopens_a_definitive_end(schema_dsn) -> None:
    """Light #223 contract (probe case definitive_end_not_reopened): after a
    definitive end a later cover, even one listing the symbol under a 12(b)
    title, does not reopen the hold; a registration filed after the end does."""
    conn, _ = schema_dsn
    _american_greetings(conn)
    _observe(conn, 5133, "AM", "2014-03-10", title="Class A Common Shares")  # a 12(b) row
    assert _issuer(conn, "AM", "2014-04-01")[:2] == ("ended", None)
    assert _line(conn, 5133, "", "2014-04-01")[0] == "ended"
    # Another issuer whose common was delisted and deregistered, then relisted.
    _observe(conn, 95, "REL", "2016-01-10")
    _event(conn, 95, "25-NSE", "2016-02-01", kind="equity", extinguished=False,
           venue_kind="primary")
    _event(conn, 95, "15-12G", "2016-02-12", kind="equity")
    _observe(conn, 95, "REL", "2016-05-10")
    assert _issuer(conn, "REL", "2016-06-01")[:2] == ("ended", None)
    _event(conn, 95, "8-A12B", "2017-03-01")
    _observe(conn, 95, "REL", "2017-05-10")
    assert _issuer(conn, "REL", "2017-04-01")[:2] == ("ended", None)
    assert _issuer(conn, "REL", "2017-06-01")[:2] == ("resolved", 95)


def test_a_reorganization_that_keeps_the_shareholder_base_is_not_a_definitive_end(
    schema_dsn,
) -> None:
    """United Fire (101199), 2012: its class was delisted (12d2-2(a)(3)) and
    deregistered when a holding company took its place under the same CIK; the
    cover count stayed about 25.5 million shares, unlike American Greetings' 100."""
    conn, _ = schema_dsn
    q = _observe(conn, 101199, "UFCS", "2011-11-07")
    _count(conn, 101199, "", "2011-11-01", 25_502_667, "2011-11-07", adsh=q)
    _event(conn, 101199, "15-12B", "2012-02-01", kind="equity", count=1)
    _event(conn, 101199, "25-NSE", "2012-02-02", kind="equity", extinguished=True,
           venue_kind="primary")
    k = _observe(conn, 101199, "UFCS", "2012-03-15")
    _count(conn, 101199, "", "2012-03-01", 25_506_809, "2012-03-15", adsh=k)
    assert _ends(conn, 101199, "2012-04-01") == [
        ("15-12B", d(2012, 2, 2), False), ("25-NSE", d(2012, 2, 3), False),
    ]
    assert _issuer(conn, "UFCS", "2012-02-10")[0] == "ended"
    assert _issuer(conn, "UFCS", "2012-04-01")[:2] == ("resolved", 101199)


def test_a_definitive_pair_needs_a_delisting_of_every_listed_class(schema_dsn) -> None:
    """Codex thread 4221720827: two listed classes; only class B is delisted, while
    the Form 15 names both."""
    conn, _ = schema_dsn
    q = _observe(conn, 98, "TWA", "2019-01-10", class_key=CLASS_A)
    _observe(conn, 98, "TWB", "2019-01-10", class_key=CLASS_B, adsh=q)
    _event(conn, 98, "25-NSE", "2019-02-01", kind="equity", count=1, venue_kind="primary")
    _event(conn, 98, "15-12G", "2019-02-11", kind="equity", count=2)
    assert _ends(conn, 98, "2019-03-01") == [("15-12G", d(2019, 2, 12), False)]
    _event(conn, 98, "25-NSE", "2019-02-05", kind="equity", count=1, venue_kind="primary")
    _event(conn, 98, "25-NSE", "2019-02-05", kind="equity", count=2, venue_kind="primary")
    assert [e[2] for e in _ends(conn, 98, "2019-03-01")] == [True, True]


def test_a_new_symbol_of_the_same_cik_after_a_definitive_end_is_a_new_line(
    schema_dsn,
) -> None:
    """Knight-Swift: Swift's class (SWFT) was extinguished in the merger and the
    same CIK's class traded as KNX afterwards."""
    conn, _ = schema_dsn
    q = _observe(conn, 1492691, "SWFT", "2017-07-24")
    _count(conn, 1492691, "", "2017-07-20", 133_000_000, "2017-07-24", adsh=q)
    _event(conn, 1492691, "25-NSE", "2017-09-11", kind="equity", extinguished=True,
           venue_kind="primary")
    k = _observe(conn, 1492691, "KNX", "2017-11-09")
    _count(conn, 1492691, "", "2017-11-01", 178_000_000, "2017-11-09", adsh=k)
    assert _ends(conn, 1492691, "2017-12-01") == [("25-NSE", d(2017, 9, 12), True)]
    assert _issuer(conn, "SWFT", "2017-12-01")[:2] == ("ended", None)
    assert _issuer(conn, "KNX", "2017-12-01")[:2] == ("resolved", 1492691)
    assert _line(conn, 1492691, "", "2017-12-01")[:3] == ("resolved", "", ["KNX"])
    assert _line(conn, 1492691, "", "2017-08-01")[:3] == ("resolved", "", ["SWFT"])


@pytest.mark.parametrize(
    ("event", "status"),
    [
        # PepsiCo's NYSE -> Nasdaq move: its 8-A12B is filed the same day.
        ({"form": "25", "kind": "equity", "venue_kind": "primary", "transfer": True},
         "resolved"),
        # IDEX withdraws its second listing on the Chicago Stock Exchange.
        ({"form": "25", "kind": "equity", "venue_kind": "secondary"}, "resolved"),
        # Listed notes or preferred of a single-symbol issuer are removed.
        ({"form": "25-NSE", "kind": "other", "venue_kind": "primary", "extinguished": True},
         "resolved"),
        # A rights plan or a savings plan ends.
        ({"form": "15-15D", "kind": "other"}, "resolved"),
        # The common stock itself: delisted for non-compliance, or its
        # registration terminated (GMV Wireless's 15-12G).
        ({"form": "25-NSE", "kind": "equity", "venue_kind": "primary", "extinguished": False},
         "ended"),
        ({"form": "15-12G", "kind": "equity"}, "ended"),
        # A foreign private issuer's Form 15F (Codex thread 4222924635).
        ({"form": "15F-12B", "kind": "equity"}, "ended"),
        ({"form": "15F-15D", "kind": "equity"}, "ended"),
        # A filing that states no class is read as unknown: the old gate holds.
        ({"form": "25-NSE", "kind": "unknown"}, "ended"),
    ],
)
def test_an_end_filing_ends_the_hold_only_for_the_class_it_names(
    schema_dsn, event: dict, status: str,
) -> None:
    conn, _ = schema_dsn
    _observe(conn, 77476, "PEP", "2017-10-04")
    if event.get("transfer"):
        _event(conn, 77476, "8-A12B", "2017-12-19")
    _event(conn, 77476, event["form"], "2017-12-19", kind=event["kind"],
           venue_kind=event.get("venue_kind"), extinguished=event.get("extinguished"))
    assert _issuer(conn, "PEP", "2018-01-02")[0] == status
    _observe(conn, 77476, "PEP", "2018-02-13")  # not definitive: a later statement reopens
    assert _issuer(conn, "PEP", "2018-03-01")[:2] == ("resolved", 77476)


def test_a_foreign_issuers_form_15f_ends_its_hold_like_a_form_15(schema_dsn) -> None:
    """Codex thread 4222924635. PetroChina delisted its ADSs from the NYSE with a
    Form 25 (2022-08-29), kept filing 20-Fs, and terminated its registration with
    a 15F-12B (2024-02-05)."""
    conn, _ = schema_dsn
    _observe(conn, 1108329, "PTR", "2022-04-28")
    _event(conn, 1108329, "25", "2022-08-29", kind="equity", venue_kind="primary")
    _observe(conn, 1108329, "PTR", "2023-04-27")  # the delisting alone is not definitive
    assert _issuer(conn, "PTR", "2023-06-30")[:2] == ("resolved", 1108329)
    _event(conn, 1108329, "15F-12B", "2024-02-05", kind="equity")
    assert _ends(conn, 1108329, "2024-03-01") == [
        ("25", d(2022, 8, 30), False), ("15F-12B", d(2024, 2, 6), False),
    ]
    assert _issuer(conn, "PTR", "2024-03-01")[:2] == ("ended", None)
    # Within 120 days of the delisting a Form 15F makes the end definitive, as a
    # Form 15 does: a later untitled cover does not reopen the hold.
    _observe(conn, 1108330, "PTRX", "2022-04-28")
    _event(conn, 1108330, "25", "2022-08-29", kind="equity", venue_kind="primary")
    _event(conn, 1108330, "15F-15D", "2022-10-03", kind="equity")
    _observe(conn, 1108330, "PTRX", "2023-04-27")
    assert _ends(conn, 1108330, "2023-06-30") == [
        ("25", d(2022, 8, 30), True), ("15F-15D", d(2022, 10, 4), True),
    ]
    assert _issuer(conn, "PTRX", "2023-06-30")[:2] == ("ended", None)


def test_a_form_15_for_one_class_of_a_multi_class_issuer_ends_no_line(schema_dsn) -> None:
    """Codex thread 4222924619: a 15-12G or 15-15D naming fewer classes than the
    issuer listed needs the same whole-class coverage as other partial ends."""
    conn, _ = schema_dsn
    a = _observe(conn, 98, "MCA", "2024-02-10", class_key=CLASS_A)
    _observe(conn, 98, "MCB", "2024-02-10", class_key=CLASS_B, adsh=a)
    _event(conn, 98, "15-12G", "2024-03-01", kind="equity", count=1)
    _event(conn, 98, "15F-15D", "2024-03-05", kind="equity", count=1)
    assert _ends(conn, 98, "2024-04-01") == []
    assert _issuer(conn, "MCA", "2024-04-01")[:2] == ("resolved", 98)
    # Naming both classes, it ends both lines.
    _event(conn, 98, "15-15D", "2024-05-01", kind="equity", count=2)
    assert _ends(conn, 98, "2024-06-01") == [("15-15D", d(2024, 5, 2), False)]
    assert _issuer(conn, "MCA", "2024-06-01")[:2] == ("ended", None)
    assert _issuer(conn, "MCB", "2024-06-01")[:2] == ("ended", None)


@pytest.mark.parametrize("form", loader.END_FORMS)
@pytest.mark.parametrize("kind", ["equity", "unknown", None])
def test_no_partial_end_form_ends_a_line_of_a_two_symbol_issuer(
    schema_dsn, form: str, kind: str | None,
) -> None:
    """Codex thread 4222924619, re-checked for every end form: one that names
    one class (or none, or was not read) of an issuer listing two symbols ends
    neither line, as filed or as restated."""
    conn, _ = schema_dsn
    a = _observe(conn, 98, "MCA", "2024-02-10", class_key=CLASS_A)
    _observe(conn, 98, "MCB", "2024-02-10", class_key=CLASS_B, adsh=a)
    _event(conn, 98, form, "2024-03-01", kind=kind, count=1, venue_kind="primary")
    _event(conn, 98, f"{form}/A", "2024-03-20", kind="equity", count=1, venue_kind="primary",
           effect="restates")
    assert _ends(conn, 98, "2024-04-01") == []
    assert [_issuer(conn, t, "2024-04-01")[:2] for t in ("MCA", "MCB")] == [
        ("resolved", 98), ("resolved", 98)]


def test_a_form_15_for_an_unlisted_class_leaves_the_listed_class(schema_dsn) -> None:
    """W1 re-gate (P2): the cover lists class A (AAA) and counts an unlisted class
    B; a 15-12G or 15-15D naming one equity class may terminate B's registration
    and must not close A. A delisting (25, 25-NSE) or a 15-12B concerns a listed
    class, so one naming one class ends the one listed line."""
    conn, _ = schema_dsn
    q = _observe(conn, 72, "AAA", "2024-02-14", class_key=CLASS_A)
    _count(conn, 72, CLASS_A, "2024-02-13", 600, "2024-02-14", adsh=q)
    _count(conn, 72, CLASS_B, "2024-02-13", 400, "2024-02-14", adsh=q)
    _event(conn, 72, "15-12G", "2024-03-01", kind="equity", count=1)
    _event(conn, 72, "15F-15D", "2024-03-04", kind="equity", count=1)
    assert _ends(conn, 72, "2024-04-01") == []
    assert _issuer(conn, "AAA", "2024-04-01")[:2] == ("resolved", 72)
    _event(conn, 72, "15-15D", "2024-04-10", kind="equity", count=2)
    assert _ends(conn, 72, "2024-05-01") == [("15-15D", d(2024, 4, 11), False)]
    assert _issuer(conn, "AAA", "2024-05-01")[:2] == ("ended", None)
    q = _observe(conn, 73, "BBB", "2024-02-14", class_key=CLASS_A)
    _count(conn, 73, CLASS_A, "2024-02-13", 600, "2024-02-14", adsh=q)
    _count(conn, 73, CLASS_B, "2024-02-13", 400, "2024-02-14", adsh=q)
    _event(conn, 73, "25-NSE", "2024-03-01", kind="equity", count=1, venue_kind="primary")
    assert _issuer(conn, "BBB", "2024-04-01")[:2] == ("ended", None)


def test_a_count_stated_before_the_end_does_not_prove_the_base_continued(
    schema_dsn,
) -> None:
    """Codex thread 4222086445: American Greetings' class A was extinguished and
    both classes deregistered. A 10-Q filed after the end but stating its count as
    of before the merger is not the post-end shareholder base; the first count
    stated after the end (100 shares) is, so the end is definitive."""
    conn, _ = schema_dsn
    q = _observe(conn, 5133, "AM", "2013-07-10")
    _count(conn, 5133, CLASS_A, "2013-07-01", 29_294_198, "2013-07-10", adsh=q)
    _count(conn, 5133, CLASS_B, "2013-07-01", 2_912_167, "2013-07-10", adsh=q)
    _event(conn, 5133, "25-NSE", "2013-08-12", kind="equity", count=1, extinguished=True,
           venue_kind="primary")
    _event(conn, 5133, "15-12B", "2013-08-22", kind="equity", count=2, venue_kind="unknown")
    late = _observe(conn, 5133, "AM", "2013-09-05")  # the quarter before the merger
    _count(conn, 5133, CLASS_A, "2013-07-31", 29_294_200, "2013-09-05", adsh=late)
    _count(conn, 5133, CLASS_B, "2013-07-31", 2_912_167, "2013-09-05", adsh=late)
    after = _observe(conn, 5133, "AM", "2013-10-10")
    _count(conn, 5133, "", "2013-10-01", 100, "2013-10-10", adsh=after)
    assert _ends(conn, 5133, "2013-11-01") == [
        ("25-NSE", d(2013, 8, 13), False), ("15-12B", d(2013, 8, 23), True),
    ]
    assert _issuer(conn, "AM", "2013-11-01")[:2] == ("ended", None)


def test_a_registration_of_another_class_is_no_transfer(schema_dsn, tmp_path: Path) -> None:
    """Codex thread 4223111409, real Forms 8-A read by the loader. Statera
    (CIK 1318641) registered its Series B Preferred Stock (8-A12G) the day Nasdaq
    delisted its common stock: no transfer, the delisting ends the hold. PepsiCo's
    8-A12B of its common stock beside its Form 25 is a transfer; its 8-A12B of
    notes beside a Form 25 of the common is not."""
    conn, dsn = schema_dsn
    _observe(conn, 1318641, "STAB", "2022-11-14")
    _observe(conn, 77476, "PEP", "2017-10-04")
    _observe(conn, 77477, "PEPX", "2018-10-04")
    docs = tmp_path / "docs"
    docs.mkdir()
    statera, common, notes = ("0001437749-23-002250", "0000950103-17-012545",
                              "0000950103-18-014472")
    for adsh in (statera, common, notes):
        (docs / f"{adsh}.txt").write_bytes((FILINGS / f"{adsh}.txt").read_bytes())
    # The notes' 8-A, filed for a second CIK to pair it with a common stock's Form 25.
    index = _index(tmp_path / "2023QTR1.form.gz",
                   ("8-A12G", 1318641, "2023-02-01", statera),
                   ("8-A12B", 77476, "2017-12-19", common),
                   ("8-A12B", 77477, "2018-12-07", notes))
    loader.run([], dsn=dsn, dry_run=False, form_indexes=[index],
               documents=loader.EventDocuments(docs, None), reconciled_on=d(2023, 3, 1))
    assert conn.execute(
        "SELECT cik, form, class_kind FROM sec_registration_events ORDER BY cik"
    ).fetchall() == [(77476, "8-A12B", "equity"), (77477, "8-A12B", "other"),
                     (1318641, "8-A12G", "other")]
    _event(conn, 1318641, "25-NSE", "2023-02-01", kind="equity", extinguished=False,
           venue_kind="primary")
    _event(conn, 77476, "25", "2017-12-19", kind="equity", venue_kind="primary")
    _event(conn, 77477, "25", "2018-12-07", kind="equity", venue_kind="primary")
    assert _issuer(conn, "STAB", "2023-02-10")[:2] == ("ended", None)
    assert _issuer(conn, "PEP", "2018-01-02")[:2] == ("resolved", 77476)
    assert _issuer(conn, "PEPX", "2019-01-02")[:2] == ("ended", None)
    # A registration that was not read (a Form 10) still corroborates a transfer.
    _observe(conn, 4343, "TENX", "2021-01-10")
    _event(conn, 4343, "10-12B", "2021-02-01")
    _event(conn, 4343, "25", "2021-02-05", kind="equity", venue_kind="primary")
    assert _issuer(conn, "TENX", "2021-03-01")[:2] == ("resolved", 4343)


def test_a_reused_tickers_earlier_non_listed_holder_keeps_its_run(schema_dsn) -> None:
    """Codex thread 4223111418: an earlier holder tagged its symbol only on rows
    read as debt; years later another issuer lists it. The equity-first preference
    applies over time: the earlier holder's rows count until a listed row shows
    the symbol, so its lineage run survives the reuse."""
    conn, _ = schema_dsn
    for filed in ("2012-05-10", "2013-05-10", "2014-05-09"):
        _observe(conn, 501, "OLDT", filed, kind="debt", class_key="LongtermDebtType=Notes;")
    for filed in ("2016-03-01", "2016-06-01"):
        _observe(conn, 502, "OLDT", filed)
    notes = "LongtermDebtType=Notes;"
    assert _span(conn, "OLDT", 501) == [
        (notes, d(2012, 5, 11), None, None, d(2014, 5, 10), None, d(2016, 3, 2)),
    ]
    assert _span(conn, "OLDT", 502)[0][:2] == ("", d(2016, 3, 2))
    assert _issuer(conn, "OLDT", "2013-06-01")[:2] == ("resolved", 501)
    assert _issuer(conn, "OLDT", "2016-07-01")[:2] == ("resolved", 502)


def test_an_undimensioned_total_is_no_count_of_two_symbols(
    schema_dsn, tmp_path: Path,
) -> None:
    """Codex thread 4222086431: "JWA/JWB" on one undimensioned fact beside the
    filing's total; v1 read one class and sized either symbol with the total."""
    conn, dsn = schema_dsn
    path = _write_package(
        tmp_path / "2024q1_notes.zip",
        [_sub(A1, 61, "10-Q", "20240306", "2024-03-06 08:00:00.0")],
        [_fact(A1, "TradingSymbol", "JWA/JWB")],
        [_shares(A1, "55000000")],
    )
    loader.run([path], dsn=dsn, dry_run=False, reconciled_on=d(2024, 4, 1))
    assert [_ticker_shares(conn, t, 61, "2024-04-01")[0] for t in ("JWA", "JWB")] == [
        "missing", "missing"]


def test_a_successor_registration_under_the_same_cik_is_no_end(schema_dsn) -> None:
    """KKR & Co. (CIK 1404912) reorganized as a holding company in 2022 under the
    same CIK: its 8-K12B (2022-05-31), NYSE's 25-NSE of the old common stock
    (12d2-2(a)(3), 2022-06-01) and a 15-12B; its count rose from 592.6 to 859.8
    million shares, so the base check alone reads a definitive end. The successor
    registration says the class went on under the CIK: no end. ODP (2020) and
    ADTRAN (2022) did the same."""
    conn, _ = schema_dsn
    q = _observe(conn, 1404912, "KKR", "2022-05-06", title="Common Stock")
    _count(conn, 1404912, "", "2022-05-04", 592_607_251, "2022-05-06", adsh=q)
    _event(conn, 1404912, "8-K12B", "2022-05-31")
    _event(conn, 1404912, "25-NSE", "2022-06-01", kind="equity", extinguished=True,
           venue_kind="primary", description="Common Stock")
    _event(conn, 1404912, "15-12B", "2022-06-13", kind="equity",
           description="Common Stock, par value $0.01 per share, of KKR Group Co. Inc.")
    k = _observe(conn, 1404912, "KKR", "2022-08-05", title="Common Stock")
    _count(conn, 1404912, "", "2022-08-04", 859_833_444, "2022-08-05", adsh=k)
    assert _ends(conn, 1404912, "2024-12-31") == []
    for as_of in ("2022-06-20", "2022-12-31"):
        assert _issuer(conn, "KKR", as_of)[:2] == ("resolved", 1404912), as_of
    assert _alive(conn, "KKR", 1404912, "", d(2022, 6, 20))
    # Without the successor registration the same filings are a definitive end.
    q = _observe(conn, 1404913, "KKRX", "2022-05-06", title="Common Stock")
    _count(conn, 1404913, "", "2022-05-04", 592_607_251, "2022-05-06", adsh=q)
    _event(conn, 1404913, "25-NSE", "2022-06-01", kind="equity", extinguished=True,
           venue_kind="primary", description="Common Stock")
    k = _observe(conn, 1404913, "KKRX", "2022-08-05", title="Common Stock")
    _count(conn, 1404913, "", "2022-08-04", 859_833_444, "2022-08-05", adsh=k)
    assert _ends(conn, 1404913, "2024-12-31") == [("25-NSE", d(2022, 6, 2), True)]
    assert _issuer(conn, "KKRX", "2022-12-31")[:2] == ("ended", None)


def test_the_end_of_a_common_stock_leaves_its_listed_preferreds(schema_dsn) -> None:
    """Triton (CIK 1660734) was taken private in 2023: NYSE removed its common
    shares (25-NSE, 12d2-2(a)(3)) and it deregistered them, while its preferred
    shares stayed listed and kept their covers. An end of an equity class ends the
    listed equity lines, not the preferred ones."""
    conn, _ = schema_dsn
    q = _observe(conn, 1660734, "TRTN", "2023-07-27", class_key="ClassOfStock=CommonStock;",
                 title="Common shares")
    _observe(conn, 1660734, "TRTN-PA", "2023-07-27", kind="preferred", adsh=q,
             class_key="ClassOfStock=SeriesAPreferredStock;", title="8.50% Series A Preference")
    _count(conn, 1660734, "ClassOfStock=CommonStock;", "2023-07-20", 54_000_000, "2023-07-27",
           adsh=q)
    _event(conn, 1660734, "25-NSE", "2023-09-28", kind="equity", extinguished=True,
           venue_kind="primary", description="Common shares, par value $0.01 per share")
    _event(conn, 1660734, "15-12G", "2023-10-10", kind="equity",
           description="Common shares, par value $0.01 per share")
    for filed in ("2023-11-02", "2024-05-01"):
        _observe(conn, 1660734, "TRTN-PA", filed, kind="preferred",
                 class_key="ClassOfStock=SeriesAPreferredStock;", title="8.50% Series A Preference")
    assert _issuer(conn, "TRTN", "2024-06-01")[:2] == ("ended", None)
    assert _issuer(conn, "TRTN-PA", "2024-06-01")[:2] == ("resolved", 1660734)
    assert _span(conn, "TRTN-PA", 1660734)[0][2:4] == (None, None)


def test_a_stray_claim_inside_another_holders_claims_does_not_count(schema_dsn) -> None:
    """Insider cross-check, item 11: a misfiled 10-Q put ANDE under a shell CIK
    (1650205) while The Andersons (821026) reported it every quarter."""
    conn, _ = schema_dsn
    for filed in ("2015-11-05", "2016-02-25", "2016-05-05", "2016-08-04", "2016-11-03"):
        _observe(conn, 821026, "ANDE", filed)
    _observe(conn, 1650205, "ANDE", "2016-05-16")
    # Until The Andersons reports again, nothing known distinguishes the two.
    assert _issuer(conn, "ANDE", "2016-06-01")[0] == "ambiguous"
    assert _issuer(conn, "ANDE", "2016-08-10") == (
        "resolved", 821026, "", d(2016, 8, 5), [821026],
    )
    # Lineage: the stray run bounds nothing.
    assert _span(conn, "ANDE", 821026) == [
        ("", d(2015, 11, 6), None, None, d(2016, 11, 4), None, None),
    ]
    # A holder that keeps claiming after the incumbent stops is not a stray.
    _observe(conn, 999, "ANDE", "2017-03-01")
    assert _issuer(conn, "ANDE", "2017-03-10")[0] == "ambiguous"


CAPITAL_C = "ClassOfStock=CapitalClassC;"


def _evidence(conn, ticker: str, cik: int, class_key: str) -> list[tuple]:
    return conn.execute(
        "SELECT evidence, holder_cik, line_key, valid_from, valid_to, end_reason, symbols "
        "FROM sec_line_price_evidence(%s, %s, %s)", (ticker, cik, class_key),
    ).fetchall()

def _alive(conn, ticker: str, cik: int, class_key: str, on: dt.date) -> bool:
    """Whether the line of ``class_key`` is evidenced alive at ``on``."""
    return any(row[0] == "alive" and row[3] <= on and (row[4] is None or on < row[4])
               for row in _evidence(conn, ticker, cik, class_key))


def test_an_end_naming_one_class_ends_that_class_only(schema_dsn) -> None:
    """Light #223 contract (probe cases named_class_end and unaffected_class): a
    Form 15 of an A/B issuer naming "Class B common stock" ends class B's hold and
    line, and leaves class A's. The class is read from the cover before the end:
    its 12(b) title, else its member's letter."""
    conn, _ = schema_dsn
    a = _observe(conn, 100, "DUAL-A", "2024-02-01", class_key="CommonClassA",
                 title="Class A common stock")
    _observe(conn, 100, "DUAL-B", "2024-02-01", class_key="CommonClassB",
             title="Class B common stock", adsh=a)
    for class_key in ("CommonClassA", "CommonClassB"):
        _count(conn, 100, class_key, "2024-01-31", 1_000_000, "2024-02-01", adsh=a)
    _event(conn, 100, "15-12B", "2024-03-01", kind="equity", count=1,
           description="Class B common stock")
    on = d(2024, 3, 3)
    assert _issuer(conn, "DUAL-B", on.isoformat())[:2] == ("ended", None)
    assert _issuer(conn, "DUAL-A", on.isoformat())[:2] == ("resolved", 100)
    assert (_alive(conn, "DUAL-A", 100, "CommonClassA", on),
            _alive(conn, "DUAL-B", 100, "CommonClassB", on)) == (True, False)
    assert _line(conn, 100, "CommonClassB", on.isoformat())[0] == "ended"
    assert _line(conn, 100, "CommonClassA", on.isoformat())[0] == "resolved"
    assert conn.execute(
        "SELECT form, effective_on, definitive, class_keys "
        "FROM sec_issuer_end_events(100, '2024-03-03')"
    ).fetchall() == [("15-12B", d(2024, 3, 2), False, ["CommonClassB"])]
    # Untitled lines: the member names the class. A Form 15 naming class C, which
    # the issuer does not list, ends neither line.
    q = _observe(conn, 101, "UNA", "2024-02-01", class_key=CLASS_A)
    _observe(conn, 101, "UNB", "2024-02-01", class_key=CLASS_B, adsh=q)
    _event(conn, 101, "15-12G", "2024-03-01", kind="equity", count=1,
           description="Class C Common Stock, $0.01 par value")
    assert [_issuer(conn, t, "2024-03-03")[0] for t in ("UNA", "UNB")] == [
        "resolved", "resolved"]
    _event(conn, 101, "25-NSE", "2024-04-01", kind="equity", count=1, venue_kind="primary",
           description="Class A Common Stock, $0.01 par value")
    assert [_issuer(conn, t, "2024-04-03")[0] for t in ("UNA", "UNB")] == [
        "ended", "resolved"]


def test_an_end_naming_a_compound_class_ends_that_class_only(schema_dsn) -> None:
    """Codex thread 4224877377: classes named Class B-2 and Class B-3 are two
    classes; an end naming B-2 ends B-2's line only, from titles or members."""
    conn, _ = schema_dsn
    q = _observe(conn, 102, "BTWO", "2024-02-01", class_key="ClassOfStock=ClassB2CommonStock;",
                 title="Class B-2 Common Stock")
    _observe(conn, 102, "BTHREE", "2024-02-01", class_key="ClassOfStock=ClassB3CommonStock;",
             title="Class B-3 Common Stock", adsh=q)
    _event(conn, 102, "25-NSE", "2024-03-01", kind="equity", venue_kind="primary",
           description="Class B-2 Common Stock, par value $0.01 per share")
    assert [_issuer(conn, t, "2024-03-03")[0] for t in ("BTWO", "BTHREE")] == [
        "ended", "resolved"]
    q = _observe(conn, 103, "CTWO", "2024-02-01", class_key="ClassOfStock=ClassB2CommonStock;")
    _observe(conn, 103, "CTHREE", "2024-02-01", class_key="ClassOfStock=ClassB3CommonStock;",
             adsh=q)
    _event(conn, 103, "25-NSE", "2024-03-01", kind="equity", venue_kind="primary",
           description="Class B-3 Common Stock")
    assert [_issuer(conn, t, "2024-03-03")[0] for t in ("CTWO", "CTHREE")] == [
        "resolved", "ended"]


def test_a_definitive_end_is_not_reopened_by_a_titled_cover(schema_dsn) -> None:
    """Light #223 contract (probe case definitive_end_not_reopened): the 25-NSE
    extinguished the class and the next count is 100 shares; the later 10-Q that
    still lists the symbol with a title does not reopen the hold or the line."""
    conn, _ = schema_dsn
    q = _observe(conn, 200, "DEAD", "2024-02-01", title="Common stock")
    _count(conn, 200, "", "2024-01-31", 1_000_000, "2024-02-01", adsh=q)
    _event(conn, 200, "25-NSE", "2024-03-01", kind="equity", extinguished=True,
           venue_kind="primary", description="Common stock")
    k = _observe(conn, 200, "DEAD", "2024-04-01", title="Common stock")
    _count(conn, 200, "", "2024-03-31", 100, "2024-04-01", adsh=k)
    on = d(2024, 4, 3)
    assert _ends(conn, 200, on.isoformat()) == [("25-NSE", d(2024, 3, 2), True)]
    assert _issuer(conn, "DEAD", on.isoformat())[:2] == ("ended", None)
    assert not _alive(conn, "DEAD", 200, "", on)
    assert _span(conn, "DEAD", 200) == [
        ("", d(2024, 2, 2), d(2024, 3, 2), "25-NSE", d(2024, 2, 2), None, None),
    ]


def test_an_undimensioned_sole_class_is_not_merged_into_a_later_class(schema_dsn) -> None:
    """Light #223 contract (probe case undimensioned_class_switch): Google Inc's
    undimensioned "Common stock" GOOG (its sole class) in March 2014, then GOOGL on
    class A and GOOG on class C. GOOG moved to the new class C in the split; the
    class C line is not alive before it existed. Berkshire's relabelled members
    stay one line (test_relabelled_members_of_one_class_are_one_line)."""
    conn, _ = schema_dsn
    old = _observe(conn, 1288776, "GOOG", "2014-03-01", title="Common stock")
    conn.execute("UPDATE sec_ticker_cik_observations SET filing_complete = true "
                 "WHERE adsh = %s", (old,))
    q = _observe(conn, 1288776, "GOOGL", "2014-04-03", class_key="ClassA",
                 title="Class A common stock")
    _observe(conn, 1288776, "GOOG", "2014-04-03", class_key="ClassC",
             title="Class C common stock", adsh=q)
    conn.execute("UPDATE sec_ticker_cik_observations SET filing_complete = true "
                 "WHERE adsh = %s", (q,))
    assert sorted(conn.execute("SELECT class_key, line_key FROM sec_issuer_lines(1288776)"
                               ).fetchall()) == [("", ""), ("ClassA", "ClassA"),
                                                 ("ClassC", "ClassC")]
    on = d(2014, 3, 15)
    assert _issuer(conn, "GOOG", on.isoformat())[:2] == ("resolved", 1288776)
    assert not _alive(conn, "GOOG", 1288776, "ClassC", on)
    assert _alive(conn, "GOOG", 1288776, "ClassC", d(2014, 5, 1))
    # The old sole class's run ends at the split, so it does not hold GOOG beside
    # class C (a run left open would refuse class C's rows until it went stale).
    assert _evidence(conn, "GOOG", 1288776, "ClassC") == [
        ("alive", 1288776, "ClassC", d(2014, 4, 4), d(2015, 5, 10), "stale", ["GOOG"]),
        ("other_holder", 1288776, "", d(2014, 3, 2), d(2014, 4, 4), "other_symbol", ["GOOG"]),
    ]


def test_a_sole_class_that_starts_being_dimensioned_stays_one_line(schema_dsn) -> None:
    """The undimensioned sole class keeps its line when the filer dimensions it
    and counts an unlisted class beside it: one listed class, nothing to choose."""
    conn, _ = schema_dsn
    old = _observe(conn, 77, "XYZ", "2018-03-01")
    _count(conn, 77, "", "2018-02-28", 1000, "2018-03-01", adsh=old)
    new = _observe(conn, 77, "XYZ", "2019-08-01", class_key=CLASS_A)
    _count(conn, 77, CLASS_A, "2019-07-31", 900, "2019-08-01", adsh=new)
    _count(conn, 77, CLASS_B, "2019-07-31", 100, "2019-08-01", adsh=new)
    assert sorted(conn.execute("SELECT class_key, line_key FROM sec_issuer_lines(77)"
                               ).fetchall()) == [("", ""), (CLASS_A, "")]
    assert _span(conn, "XYZ", 77, CLASS_A) == [
        (CLASS_A, d(2018, 3, 2), None, None, d(2019, 8, 2), None, None),
    ]


def test_issuer_at_takes_class_and_kind_from_the_same_row(schema_dsn) -> None:
    """Light #223 thread: an older depositary row and the latest ordinary row."""
    conn, _ = schema_dsn
    _observe(conn, 97, "KND", "2020-02-10", kind="depositary",
             class_key="LegalEntity=AmericanDepositaryShares;")
    _observe(conn, 97, "KND", "2021-02-10", class_key="ClassOfStock=OrdinaryShares;")
    assert conn.execute(
        "SELECT status, class_key, security_kind FROM sec_ticker_issuer_at('KND', '2021-03-01')"
    ).fetchone() == ("resolved", "ClassOfStock=OrdinaryShares;", "equity")
    assert conn.execute(
        "SELECT status, class_key, security_kind FROM sec_ticker_issuer_at('KND', '2020-03-01')"
    ).fetchone() == ("resolved", "LegalEntity=AmericanDepositaryShares;", "depositary")


def _google_inc_to_alphabet(conn) -> None:
    """Google Inc (1288776) tagged GOOG and GOOGL undimensioned beside its class
    A, B and C counts. On 2015-10-02 Nasdaq removed classes A and C (12d2-2(a)(3))
    and Alphabet (1652044) filed its 8-K12B as successor; Google Inc's 10-Q of
    2015-10-29 still tagged both symbols, the day Alphabet's first 10-Q did."""
    q = _observe(conn, 1288776, "GOOG", "2015-07-23")
    _observe(conn, 1288776, "GOOGL", "2015-07-23", adsh=q)
    for class_key, shares in ((CLASS_A, 289), (CLASS_B, 52), (CAPITAL_C, 345)):
        _count(conn, 1288776, class_key, "2015-07-17", shares, "2015-07-23", adsh=q)
    _event(conn, 1288776, "25-NSE", "2015-10-02", kind="equity", count=2, extinguished=True,
           venue_kind="primary")
    _event(conn, 1288776, "15-12G", "2015-10-02", kind="equity", count=1)  # class B
    k = _observe(conn, 1652044, "GOOG", "2015-10-02")
    _observe(conn, 1652044, "GOOGL", "2015-10-02", adsh=k)
    for cik in (1288776, 1652044):
        late = _observe(conn, cik, "GOOG", "2015-10-29")
        _observe(conn, cik, "GOOGL", "2015-10-29", adsh=late)


def test_a_symbol_taken_by_a_successor_cik_is_not_reopened_by_stale_covers(
    schema_dsn,
) -> None:
    conn, _ = schema_dsn
    _google_inc_to_alphabet(conn)
    assert _issuer(conn, "GOOG", "2015-09-30")[:2] == ("resolved", 1288776)
    for as_of in ("2015-10-05", "2015-11-15"):
        assert _issuer(conn, "GOOG", as_of)[:2] == ("resolved", 1652044), as_of
    assert _issuer(conn, "GOOGL", "2015-11-15")[:2] == ("resolved", 1652044)
    assert _span(conn, "GOOG", 1652044) == [
        ("", d(2015, 10, 3), None, None, d(2015, 10, 30), d(2015, 10, 3), None),
    ]
    assert _span(conn, "GOOG", 1288776) == [
        # The 25-NSE of classes A and C ends it; the 15-12G of the unlisted class B
        # alone would not (Codex thread 4222924619).
        ("", d(2015, 7, 24), d(2015, 10, 3), "25-NSE", d(2015, 7, 24), None, d(2015, 10, 3)),
    ]


def _google_class_move(conn) -> None:
    """Class A traded as GOOG until April 2014; then class C took GOOG and class A
    became GOOGL (dimensioned covers; the class B count is unlisted)."""
    for filed, symbols in (("2013-10-24", {CLASS_A: "GOOG"}),
                           ("2014-02-11", {CLASS_A: "GOOG"}),
                           ("2014-04-24", {CLASS_A: "GOOGL", CAPITAL_C: "GOOG"}),
                           ("2014-07-24", {CLASS_A: "GOOGL", CAPITAL_C: "GOOG"})):
        adsh = None
        for class_key, ticker in symbols.items():
            adsh = _observe(conn, 1288776, ticker, filed, class_key=class_key, adsh=adsh)
        for class_key in (CLASS_A, CLASS_B, *([CAPITAL_C] if CAPITAL_C in symbols else [])):
            _count(conn, 1288776, class_key, filed, 100, filed, adsh=adsh)


def test_another_line_of_the_same_issuer_is_another_holder(schema_dsn) -> None:
    """GOOG moved from class A to class C of one CIK: the class C line admits no
    GOOG row from before its own start."""
    conn, _ = schema_dsn
    _google_class_move(conn)
    assert sorted(conn.execute("SELECT class_key, line_key FROM sec_issuer_lines(1288776)"
                               ).fetchall()) == [(CAPITAL_C, CAPITAL_C), (CLASS_A, CLASS_A)]
    assert _span(conn, "GOOG", 1288776, CAPITAL_C) == [
        (CAPITAL_C, d(2014, 4, 25), None, None, d(2014, 7, 25), d(2014, 4, 25), None),
    ]
    assert _span(conn, "GOOG", 1288776, CLASS_A) == [
        (CLASS_A, d(2013, 10, 25), d(2014, 4, 25), "other_symbol", d(2014, 2, 12), None,
         d(2014, 4, 25)),
    ]
    assert _span(conn, "GOOGL", 1288776, CLASS_A) == [
        (CLASS_A, d(2014, 4, 25), None, None, d(2014, 7, 25), None, None),
    ]
    assert _evidence(conn, "GOOG", 1288776, CAPITAL_C) == [
        ("alive", 1288776, CAPITAL_C, d(2014, 4, 25), d(2015, 8, 30), "stale", ["GOOG"]),
        ("other_holder", 1288776, CLASS_A, d(2013, 10, 25), d(2014, 4, 25), "other_symbol",
         ["GOOG"]),
    ]


def test_a_line_never_joins_classes_that_appear_together(schema_dsn) -> None:
    """Codex thread 4221720858: A and B share a symbol, B and C share another, and
    A and C appear in one filing. The earlier edge (A-B) wins; B-C is dropped."""
    conn, _ = schema_dsn
    a, b, c = (f"ClassOfStock={m};" for m in ("ShareA", "ShareB", "ShareC"))
    _observe(conn, 99, "XX", "2020-01-10", class_key=a)
    _observe(conn, 99, "XX", "2020-04-10", class_key=b)
    _observe(conn, 99, "YY", "2020-07-10", class_key=b)
    _observe(conn, 99, "YY", "2020-10-10", class_key=c)
    both = _observe(conn, 99, "XX", "2021-01-10", class_key=a)
    _observe(conn, 99, "ZZ", "2021-01-10", class_key=c, adsh=both)
    assert sorted(conn.execute("SELECT class_key, line_key FROM sec_issuer_lines(99)"
                               ).fetchall()) == [(a, a), (b, a), (c, c)]


def test_relabelled_members_of_one_class_are_one_line(schema_dsn) -> None:
    """Berkshire tags BRK.B on CommonClassB in 10-Qs and on ClassBCommonStock in
    8-Ks, for years."""
    conn, _ = schema_dsn
    for filed, (member_a, member_b), counted in (
        ("2022-05-02", ("CommonClassA", "CommonClassB"), True),
        ("2022-05-04", ("ClassACommonStock", "ClassBCommonStock"), False),
        ("2022-08-01", ("CommonClassA", "CommonClassB"), True),
        ("2022-08-08", ("ClassACommonStock", "ClassBCommonStock"), False),
    ):
        adsh = _observe(conn, 1067983, "BRK-A", filed, class_key=f"ClassOfStock={member_a};")
        _observe(conn, 1067983, "BRK-B", filed, class_key=f"ClassOfStock={member_b};", adsh=adsh)
        if counted:
            _count(conn, 1067983, f"ClassOfStock={member_a};", filed, 600_000, filed, adsh=adsh)
            _count(conn, 1067983, f"ClassOfStock={member_b};", filed, 1_300_000_000, filed,
                   adsh=adsh)
    assert sorted(conn.execute("SELECT class_key, line_key FROM sec_issuer_lines(1067983)"
                               ).fetchall()) == [
        ("ClassOfStock=ClassACommonStock;", "ClassOfStock=CommonClassA;"),
        ("ClassOfStock=ClassBCommonStock;", "ClassOfStock=CommonClassB;"),
        ("ClassOfStock=CommonClassA;", "ClassOfStock=CommonClassA;"),
        ("ClassOfStock=CommonClassB;", "ClassOfStock=CommonClassB;"),
    ]
    assert _span(conn, "BRK-B", 1067983, "ClassOfStock=ClassBCommonStock;") == [
        ("ClassOfStock=ClassBCommonStock;", d(2022, 5, 3), None, None, d(2022, 8, 9), None,
         None),
    ]
    assert _evidence(conn, "BRK-B", 1067983, "ClassOfStock=ClassBCommonStock;") == [
        ("alive", 1067983, "ClassOfStock=CommonClassB;", d(2022, 5, 3), d(2023, 9, 14),
         "stale", ["BRK-B"]),
    ]


def test_a_renamed_line_is_alive_under_its_old_symbol(schema_dsn) -> None:
    """Meta's line is alive through its FB years; another issuer briefly used META."""
    conn, _ = schema_dsn
    for filed in ("2014-07-24", "2015-07-30", "2016-07-28", "2017-07-27", "2018-07-26",
                  "2019-07-25", "2020-07-31", "2021-07-29"):
        _observe(conn, 1326801, "FB", filed)
    for filed in ("2021-10-26", "2022-04-28"):
        _observe(conn, 1326801, "META", filed)
    _observe(conn, 1630113, "META", "2015-07-14")
    _observe(conn, 1630113, "MMAT", "2015-11-29")
    assert _evidence(conn, "META", 1326801, "") == [
        ("alive", 1326801, "", d(2014, 7, 25), d(2023, 6, 4), "stale", ["FB", "META"]),
        ("other_holder", 1630113, "", d(2015, 7, 15), d(2015, 11, 30), "other_symbol",
         ["META"]),
    ]


def test_price_evidence_of_a_reused_ticker(schema_dsn) -> None:
    """AT&T Corp (5907) showed T then T1; AT&T Inc (732717) then showed T. Without
    an end, AT&T Corp's last confirmation holds T until it is stale."""
    conn, _ = schema_dsn
    _observe(conn, 5907, "T", "2009-11-05")
    _observe(conn, 5907, "T", "2010-02-25")
    _observe(conn, 5907, "T1", "2010-04-01")
    _observe(conn, 732717, "T", "2010-05-07")
    assert _evidence(conn, "T", 732717, "") == [
        ("alive", 732717, "", d(2010, 5, 8), d(2011, 6, 13), "stale", ["T"]),
        ("other_holder", 5907, "", d(2009, 11, 6), d(2010, 4, 2), "other_symbol", ["T"]),
    ]


def test_price_evidence_of_an_open_prior_holder_ends_at_its_stale_cutoff(schema_dsn) -> None:
    conn, _ = schema_dsn
    _att(conn, deregistered=False)
    assert _evidence(conn, "T", 732717, "") == [
        ("alive", 732717, "", d(2010, 5, 8), d(2011, 9, 12), "stale", ["T"]),
        ("other_holder", 5907, "", d(2009, 11, 6), d(2011, 4, 3), "stale", ["T"]),
    ]


def test_interval_view_lists_each_hold(schema_dsn) -> None:
    conn, _ = schema_dsn
    _observe(conn, 1512673, "SQ", "2024-11-05")
    _observe(conn, 1512673, "SQ", "2024-12-05")
    _observe(conn, 1512673, "XYZ", "2025-01-21")
    assert conn.execute(
        "SELECT ticker, valid_from, last_confirmed_on, valid_to, statements "
        "FROM sec_ticker_intervals ORDER BY valid_from"
    ).fetchall() == [
        ("SQ", d(2024, 11, 6), d(2024, 12, 6), d(2025, 1, 22), 2),
        ("XYZ", d(2025, 1, 22), d(2025, 1, 22), None, 1),
    ]


def test_interval_view_ends_every_symbol_of_a_run_at_the_next_run(schema_dsn) -> None:
    conn, _ = schema_dsn
    first = _observe(conn, 8, "OLD1", "2024-01-10")
    _observe(conn, 8, "OLD2", "2024-01-10", adsh=first)
    _observe(conn, 8, "NEW", "2024-05-10")
    assert conn.execute(
        "SELECT ticker, valid_from, valid_to FROM sec_ticker_intervals "
        "WHERE cik = 8 ORDER BY valid_from, ticker"
    ).fetchall() == [
        ("OLD1", d(2024, 1, 11), d(2024, 5, 11)),
        ("OLD2", d(2024, 1, 11), d(2024, 5, 11)),
        ("NEW", d(2024, 5, 11), None),
    ]


def test_interval_view_orders_same_day_statements_by_acceptance(schema_dsn) -> None:
    """Review 2, item 8: OLD then NEW, both public on 2024-01-11."""
    conn, _ = schema_dsn
    _observe(conn, 8, "NEW", "2024-01-11", accepted="2024-01-11 15:00:00",
             adsh="0000000001-24-000777")
    _observe(conn, 8, "OLD", "2024-01-11", accepted="2024-01-11 09:00:00",
             adsh="0000000009-24-000777")
    assert conn.execute(
        "SELECT ticker, valid_from, valid_to FROM sec_ticker_intervals WHERE cik = 8"
    ).fetchall() == [("NEW", d(2024, 1, 11), None)]


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
        # With the roles present: back to v1, v1 again (tables), then v2 (routines).
        conn.execute(V2_ROLLBACK_SQL + SCHEMA_SQL)
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


# --------------------------------------------------------------------------- #
# Loader (bitemporal reconciliation)
# --------------------------------------------------------------------------- #
def _index(path: Path, *rows: tuple[str, int, str, str]) -> Path:
    path.write_bytes(gzip.compress("".join(
        f"{form:<17}SOME CO      {cik}     {filed}  edgar/data/{cik}/{adsh}.txt\n"
        for form, cik, filed, adsh in rows
    ).encode()))
    return path


def test_loader_is_idempotent_and_retires_what_a_republication_drops(
    schema_dsn, tmp_path: Path
) -> None:
    """Review 2, item 2: nothing is deleted; answers before a correction keep."""
    conn, dsn = schema_dsn
    package = _sample_package(tmp_path)
    index = _index(tmp_path / "2024QTR1.form.gz",
                   ("25-NSE", 1070336, "2024-01-28", "0001354457-24-000034"))
    first = loader.run([package], dsn=dsn, dry_run=False, form_indexes=[index],
                       reconciled_on=d(2024, 3, 20))
    assert (first[0]["inserted"], first[0]["retired"]) == (4, 0)
    assert (first[0]["shares_inserted"], first[0]["shares_retired"]) == (3, 0)
    assert (first[1]["events"], first[1]["inserted"], first[1]["retired"]) == (1, 1, 0)
    rows = conn.execute(
        "SELECT adsh, ticker, ticker_key, cik, class_key, security_kind, available_on, "
        "loaded_on, retired_on FROM sec_ticker_cik_observations ORDER BY adsh, ticker"
    ).fetchall()
    loaded = d(2024, 3, 20)
    assert rows == [
        (A1, "BRK34", "BRK34", 1067983, "LongtermDebtType=Notes2034;", "debt",
         d(2024, 2, 24), loaded, None),
        (A1, "BRK-A", "BRKA", 1067983, CLASS_A, "equity", d(2024, 2, 24), loaded, None),
        (A1, "BRK-B", "BRKB", 1067983, CLASS_B, "equity", d(2024, 2, 24), loaded, None),
        (A2, "BFB", "BFB", 14693, NONVOTING, "equity", d(2024, 3, 6), loaded, None),
    ]
    # The resolver and the class count join on the same class.
    assert _issuer(conn, "BRK.B", "2024-02-24")[:3] == ("resolved", 1067983, CLASS_B)
    assert _class_shares(conn, 1067983, CLASS_B, "2024-02-24") == (
        "resolved", Decimal("1389605139.0000"), d(2024, 2, 12),
    )
    again = loader.run([package], dsn=dsn, dry_run=False, form_indexes=[index],
                       reconciled_on=d(2024, 4, 1))
    assert (again[0]["inserted"], again[0]["retired"]) == (0, 0)
    assert (again[1]["inserted"], again[1]["retired"]) == (0, 0)

    # A republished package: A1 without BRK.A and the notes, BRK.B retitled; A2
    # and A3 gone.
    package.unlink()
    _write_package(
        package,
        [_sub(A1, 1067983, "10-K", "20240226", "2024-02-24 08:00:05.0")],
        [_fact(A1, "TradingSymbol", "BRK.B", dimh="0xbbx"),
         _fact(A1, "Security12bTitle", "Class B Common Stock", dimh="0xbbx")],
        [_shares(A1, "1389605139.0000", dimh="0xbbb", datp="17.0")],
        DIMS,
    )
    third = loader.run([package], dsn=dsn, dry_run=False, reconciled_on=d(2024, 6, 1))
    # BRK-B's fact changed (one class in the filing now): a correction of a
    # known accession, knowable from the reconciliation date.
    assert (third[0]["inserted"], third[0]["retired"]) == (1, 4)
    assert (third[0]["shares_inserted"], third[0]["shares_retired"]) == (0, 2)
    assert conn.execute(
        "SELECT ticker, available_on, retired_on, filing_equity_classes "
        'FROM sec_ticker_cik_observations ORDER BY ticker COLLATE "C", available_on'
    ).fetchall() == [
        ("BFB", d(2024, 3, 6), d(2024, 6, 1), 1),
        ("BRK-A", d(2024, 2, 24), d(2024, 6, 1), 2),
        ("BRK-B", d(2024, 2, 24), d(2024, 6, 1), 2),
        ("BRK-B", d(2024, 6, 1), None, 1),
        ("BRK34", d(2024, 2, 24), d(2024, 6, 1), 2),
    ]
    assert _issuer(conn, "BRK-A", "2024-05-01")[:2] == ("resolved", 1067983)  # as known then
    assert _issuer(conn, "BRK-A", "2024-06-01")[0] == "missing"
    assert _issuer(conn, "BF-B", "2024-05-01")[:2] == ("resolved", 14693)
    assert conn.execute(
        "SELECT source_package, submissions, symbol_facts, observations, share_counts, events "
        'FROM sec_ticker_cik_packages ORDER BY source_package COLLATE "C"'
    ).fetchall() == [("2024QTR1.form.gz", 0, 0, 0, 0, 1), ("2024q1_notes.zip", 1, 1, 1, 1, 0)]


def test_a_fact_another_package_still_carries_is_not_retired(
    schema_dsn, tmp_path: Path
) -> None:
    """Review 2, item 6: P and Q both carry accession A2 with OLD and KEEP."""
    conn, dsn = schema_dsn

    def package(name: str, *symbols: str) -> Path:
        return _write_package(
            tmp_path / name,
            [_sub(A2, 14693, "10-Q", "20240305", "")],
            [_fact(A2, "TradingSymbol", s, dimh=h) for s, h in zip(symbols, ("0xccc", "0xaaa"))],
            [], DIMS,
        )

    p = package("2024q1_notes.zip", "KEEP", "OLD")
    q = package("2024q2_notes.zip", "KEEP", "OLD")
    loader.run([p, q], dsn=dsn, dry_run=False, reconciled_on=d(2024, 4, 1))
    p.unlink()
    package("2024q1_notes.zip", "KEEP")
    loader.run([p], dsn=dsn, dry_run=False, reconciled_on=d(2024, 5, 1))
    # P's new version of KEEP (one class in the filing now) and Q's old one are
    # both carried; OLD is still carried by Q.
    current = ("SELECT DISTINCT ticker FROM sec_ticker_cik_observations "
               "WHERE retired_on IS NULL ORDER BY 1")
    assert [r[0] for r in conn.execute(current).fetchall()] == ["KEEP", "OLD"]
    q.unlink()
    package("2024q2_notes.zip", "KEEP")
    loader.run([q], dsn=dsn, dry_run=False, reconciled_on=d(2024, 6, 1))
    assert [r[0] for r in conn.execute(current).fetchall()] == ["KEEP"]
    assert conn.execute(
        "SELECT retired_on FROM sec_ticker_cik_observations WHERE ticker = 'OLD'"
    ).fetchall() == [(d(2024, 6, 1),)]


def test_an_index_reload_retires_removed_and_reassigned_events(
    schema_dsn, tmp_path: Path
) -> None:
    """Review 2, item 2: a January delisting the October index drops keeps its
    January answer; a corrected CIK moves the event from the reconciliation on."""
    conn, dsn = schema_dsn
    _observe(conn, 10, "TEN", "2023-12-01")
    _observe(conn, 20, "TWENTY", "2023-12-01")
    _observe(conn, 21, "TWENTYONE", "2023-12-01")
    q1 = tmp_path / "2024QTR1.form.gz"
    gone, moved, kept = "0000000001-24-000101", "0000000001-24-000102", "0000000001-24-000103"
    _index(q1, ("25-NSE", 10, "2024-01-10", gone), ("15-12G", 20, "2024-01-11", moved),
           ("15-12B", 30, "2024-01-12", kept))
    q2 = _index(tmp_path / "2024QTR2.form.gz", ("15-12B", 30, "2024-01-12", kept))
    loader.run([], dsn=dsn, dry_run=False, form_indexes=[q1, q2], reconciled_on=d(2024, 4, 1))
    assert _issuer(conn, "TEN", "2024-01-15")[0] == "ended"
    # The rebuilt index drops one event and corrects the CIK of another.
    _index(q1, ("15-12G", 21, "2024-01-11", moved))
    stats = loader.run([], dsn=dsn, dry_run=False, form_indexes=[q1],
                       reconciled_on=d(2024, 10, 1))
    assert (stats[0]["retired"], stats[0]["inserted"]) == (2, 1)
    assert conn.execute(
        "SELECT adsh, cik, available_on, retired_on FROM sec_registration_events "
        "ORDER BY adsh, cik"
    ).fetchall() == [
        (gone, 10, d(2024, 1, 11), d(2024, 10, 1)),
        (moved, 20, d(2024, 1, 12), d(2024, 10, 1)),
        (moved, 21, d(2024, 10, 1), None),  # a correction: knowable from its reconciliation
        (kept, 30, d(2024, 1, 13), None),  # q2 still lists it
    ]
    assert _issuer(conn, "TEN", "2024-01-15")[0] == "ended"  # as known then
    assert _issuer(conn, "TEN", "2024-10-01")[0] == "resolved"
    assert _issuer(conn, "TWENTY", "2024-01-15")[0] == "ended"
    assert _issuer(conn, "TWENTY", "2024-10-01")[0] == "resolved"
    assert _issuer(conn, "TWENTYONE", "2024-01-15")[0] == "resolved"
    assert _issuer(conn, "TWENTYONE", "2024-10-01")[0] == "ended"


def test_a_parser_correction_restates_the_reading_from_the_filing_date(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coordinator decision (2026-10-08): point-in-time means public at D. The
    same package read again by a fixed parser restates our reading: a symbol the
    old parser rejected (Outbrain's OB) and a class count it got wrong (JWA/JWB)
    are knowable from their filings' public dates; the wrong rows are retired as
    parser corrections and visible at no date. A republished package (other
    bytes) stays a change of the public record, dated by its reconciliation."""
    conn, dsn = schema_dsn
    ob, jw = "0000000071-24-000001", "0000000072-24-000001"
    path = _write_package(
        tmp_path / "2024q1_notes.zip",
        [_sub(ob, 1454938, "10-Q", "20240208", "2024-02-08 16:05:00.0"),
         _sub(jw, 61, "10-Q", "20240306", "2024-03-06 08:00:00.0")],
        [_fact(ob, "TradingSymbol", "OB"), _fact(jw, "TradingSymbol", "JWA/JWB")],
        [_shares(jw, "55000000")],
    )
    with monkeypatch.context() as old_parser:  # the reading of an older parser
        old_parser.setattr(loader, "FSN_PARSER_VERSION", "sec_fsn_v1")
        old_parser.setattr(loader, "PLACEHOLDER_KEYS", loader.PLACEHOLDER_KEYS | {"OB"})
        old_parser.setattr(loader, "_filing_profiles", lambda symbols, others, counted: {
            f: (1, f in counted) for f in set(symbols) | set(others)})
        loader.run([path], dsn=dsn, dry_run=False, reconciled_on=d(2024, 4, 1))
    assert _issuer(conn, "OB", "2024-03-01")[0] == "missing"
    assert _ticker_shares(conn, "JWA", 61, "2024-03-15")[0] == "resolved"  # the wrong total
    stats = loader.run([path], dsn=dsn, dry_run=False, reconciled_on=d(2024, 9, 1))
    assert (stats[0]["reconciled_as"], stats[0]["inserted"], stats[0]["retired"]) == (
        "parser_correction", 3, 2)
    assert conn.execute(
        "SELECT ticker, filing_equity_classes, available_on, loaded_on, retired_on, "
        "retired_reason, parser_version FROM sec_ticker_cik_observations "
        "ORDER BY ticker, id"
    ).fetchall() == [
        ("JWA", 1, d(2024, 3, 6), d(2024, 4, 1), d(2024, 9, 1), "parser_correction",
         "sec_fsn_v1"),
        ("JWA", 2, d(2024, 3, 6), d(2024, 9, 1), None, None, loader.FSN_PARSER_VERSION),
        ("JWB", 1, d(2024, 3, 6), d(2024, 4, 1), d(2024, 9, 1), "parser_correction",
         "sec_fsn_v1"),
        ("JWB", 2, d(2024, 3, 6), d(2024, 9, 1), None, None, loader.FSN_PARSER_VERSION),
        ("OB", 1, d(2024, 2, 8), d(2024, 9, 1), None, None, loader.FSN_PARSER_VERSION),
    ]
    # Point-in-time before the re-derivation takes the corrected reading.
    assert _issuer(conn, "OB", "2024-03-01")[:2] == ("resolved", 1454938)
    assert _ticker_shares(conn, "JWA", 61, "2024-03-15")[0] == "missing"
    # A republication changes the public record: knowledge-dated, as before.
    path.unlink()
    _write_package(
        path, [_sub(ob, 1454938, "10-Q", "20240208", "2024-02-08 16:05:00.0")],
        [_fact(ob, "TradingSymbol", "OB")],
    )
    stats = loader.run([path], dsn=dsn, dry_run=False, reconciled_on=d(2024, 10, 1))
    assert (stats[0]["reconciled_as"], stats[0]["retired"]) == ("source", 2)
    assert conn.execute(
        "SELECT DISTINCT retired_reason FROM sec_ticker_cik_observations "
        "WHERE retired_on = '2024-10-01'").fetchall() == [("source",)]
    assert _issuer(conn, "JWB", "2024-09-15")[:2] == ("resolved", 61)  # as known then
    assert _issuer(conn, "JWB", "2024-10-01")[0] == "missing"


def test_v2_migrates_a_loaded_v1_schema_in_place_and_rolls_back() -> None:
    """The production path: v1 applied and loaded, then v2. v2 rewrites no table,
    keeps every row, applies twice; its rollback restores the v1 functions exactly
    (and keeps the rows and the new columns), after which the loader refuses the
    database until v2 is applied again."""
    import psycopg
    from psycopg import sql

    dsn = _dsn()
    schemas = [f"sec_ticker_{uuid4().hex}", f"sec_ticker_{uuid4().hex}"]
    definitions = ("SELECT p.proname, pg_get_function_identity_arguments(p.oid), "
                   "replace(pg_get_functiondef(p.oid), current_schema() || '.', '') "
                   "FROM pg_proc p WHERE p.pronamespace = current_schema()::regnamespace "
                   "ORDER BY 1, 2")
    relfilenodes = ("SELECT relname, relfilenode FROM pg_class WHERE relnamespace = "
                    "current_schema()::regnamespace AND relkind = 'r' ORDER BY 1")
    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            for schema in schemas:
                conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schemas[1])))
            conn.execute(V1_SQL)
            v1_functions = conn.execute(definitions).fetchall()
            conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schemas[0])))
            conn.execute(V1_SQL)
            _observe(conn, 732717, "T", "2024-01-10")
            _event(conn, 732717, "15-12G", "2024-02-01", kind="equity")
            tables = conn.execute(relfilenodes).fetchall()
            conn.execute(V2_SQL)
            conn.execute(V2_SQL)
            assert conn.execute(relfilenodes).fetchall() == tables  # no rewrite
            loader.require_schema(conn)
            assert _issuer(conn, "T", "2024-03-01")[:2] == ("ended", None)
            # A parser correction: the reading of 2024-01-10 restated on 2024-06-01.
            conn.execute("UPDATE sec_ticker_cik_observations SET retired_on = '2024-06-01', "
                         "retired_reason = 'parser_correction'")
            _observe(conn, 732717, "TT", "2024-01-10")
            conn.execute(V2_ROLLBACK_SQL)
            # Codex thread 4224877391: the four functions that gate point-in-time
            # rows keep hiding parser-corrected versions, so the two readings never
            # overlap; every other function is v1's.
            restored = {row[:2]: row[2] for row in conn.execute(definitions).fetchall()}
            original = {row[:2]: row[2] for row in v1_functions}
            assert restored.keys() == original.keys()
            assert sorted(k[0] for k in original if restored[k] != original[k]) == [
                "sec_observations_at", "sec_registration_end_events",
                "sec_registration_starts", "sec_share_counts_at"]
            for key in original:
                if restored[key] != original[key]:  # v1's text plus the one predicate
                    assert restored[key].count("parser_correction") == 1, key
                    assert "parser_correction" not in original[key], key
            assert conn.execute(
                "SELECT ticker FROM sec_observations_at('2024-03-01', false)").fetchall() == [
                ("TT",)]
            assert conn.execute(
                "SELECT count(*) FROM sec_ticker_cik_observations WHERE retired_reason IS NULL"
            ).fetchone() == (1,)
            with pytest.raises(RuntimeError, match="apply schemas/sec_ticker_cik_history_v2"):
                loader.require_schema(conn)
            conn.execute(V2_SQL)
            loader.require_schema(conn)
        finally:
            for schema in schemas:
                conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)))


def test_index_loads_read_the_end_filings_of_cover_ciks(schema_dsn, tmp_path: Path) -> None:
    conn, dsn = schema_dsn
    _observe(conn, 5133, "AM", "2013-07-10")
    docs = tmp_path / "docs"
    docs.mkdir()
    for adsh in ("0000876661-13-000657", "0001193125-13-343607"):
        (docs / f"{adsh}.txt").write_bytes((FILINGS / f"{adsh}.txt").read_bytes())
    index = _index(tmp_path / "2013QTR3.form.gz",
                   ("25-NSE", 5133, "2013-08-12", "0000876661-13-000657"),
                   ("15-12B", 5133, "2013-08-22", "0001193125-13-343607"),
                   ("15-12G", 4242, "2013-08-23", "0000004242-13-000001"))
    documents = loader.EventDocuments(docs, None)
    stats = loader.run([], dsn=dsn, dry_run=False, form_indexes=[index], documents=documents,
                       reconciled_on=d(2013, 10, 1))
    assert {k: stats[0][k] for k in loader.CLASS_STAT_KEYS} == {
        "class_equity": 2, "class_other": 0, "class_unknown": 0, "class_carried": 0,
        "class_reused": 0, "class_unread": 0, "filings_missing": 0,
    }
    assert stats[1] == {"package": "derive_event_classes", "derived": 0, "class_equity": 0,
                        "class_other": 0, "class_unknown": 0, "class_carried": 0,
                        "class_reused": 0, "class_unread": 0, "filings_missing": 0,
                        "filings_fetched": 0, "filings_failed": 0, "filings_rejected": 0}
    assert conn.execute(
        "SELECT cik, form, class_kind, class_count, extinguished, venue_kind, parser_version "
        "FROM sec_registration_events ORDER BY filed"
    ).fetchall() == [
        (5133, "25-NSE", "equity", 1, True, "primary", loader.EVENT_PARSER_VERSION),
        (5133, "15-12B", "equity", 2, None, "unknown", loader.EVENT_PARSER_VERSION),
        (4242, "15-12G", None, None, None, None, None),
    ]
    again = loader.run([], dsn=dsn, dry_run=False, form_indexes=[index], documents=documents,
                       reconciled_on=d(2013, 11, 1))
    assert (again[0]["inserted"], again[0]["retired"], again[0]["class_reused"]) == (0, 0, 2)


def test_end_filings_of_a_cik_whose_covers_were_all_retired_are_still_read(
    schema_dsn, tmp_path: Path,
) -> None:
    """Codex thread 4220893585: a CIK keeps its historical cover evidence after a
    reconciliation retires it, so its Form 15/25 filings are read too."""
    conn, dsn = schema_dsn
    _observe(conn, 5133, "AM", "2013-07-10", retired_on="2013-09-01")
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "0000876661-13-000657.txt").write_bytes(
        (FILINGS / "0000876661-13-000657.txt").read_bytes())
    index = _index(tmp_path / "2013QTR3.form.gz",
                   ("25-NSE", 5133, "2013-08-12", "0000876661-13-000657"))
    stats = loader.run([], dsn=dsn, dry_run=False, form_indexes=[index],
                       documents=loader.EventDocuments(docs, None),
                       reconciled_on=d(2013, 10, 1))
    assert stats[0]["class_equity"] == 1
    assert conn.execute("SELECT class_kind, available_on FROM sec_registration_events"
                        ).fetchall() == [("equity", d(2013, 8, 13))]


def test_a_parser_change_re_derives_events_as_corrections(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, dsn = schema_dsn
    _observe(conn, 5133, "AM", "2013-07-10")
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "0000876661-13-000657.txt").write_bytes(
        (FILINGS / "0000876661-13-000657.txt").read_bytes())
    index = _index(tmp_path / "2013QTR3.form.gz",
                   ("25-NSE", 5133, "2013-08-12", "0000876661-13-000657"))
    loader.run([], dsn=dsn, dry_run=False, form_indexes=[index], reconciled_on=d(2013, 9, 1))
    assert conn.execute("SELECT class_kind FROM sec_registration_events").fetchall() == [(None,)]
    documents = loader.EventDocuments(docs, None)
    assert loader.derive_event_classes(conn, documents, reconciled_on=d(2013, 10, 1)) == {
        "derived": 1, "class_equity": 1, "class_other": 0, "class_unknown": 0,
        "class_carried": 0, "class_reused": 0, "class_unread": 0, "filings_missing": 0,
    }
    current = loader.EVENT_PARSER_VERSION
    monkeypatch.setattr(loader, "EVENT_PARSER_VERSION", "sec_event_class_next")
    assert loader.derive_event_classes(conn, documents, reconciled_on=d(2013, 11, 1))[
        "derived"] == 1
    # A re-derivation restates our reading of a public filing: the new reading is
    # knowable from the filing's public date, and the old one, retired as a
    # parser correction, is visible at no date.
    assert conn.execute(
        "SELECT class_kind, parser_version, available_on, retired_on, retired_reason "
        "FROM sec_registration_events ORDER BY id"
    ).fetchall() == [
        (None, None, d(2013, 8, 13), d(2013, 10, 1), "parser_correction"),
        ("equity", current, d(2013, 8, 13), d(2013, 11, 1), "parser_correction"),
        ("equity", "sec_event_class_next", d(2013, 8, 13), None, None),
    ]
    # Point-in-time before either re-derivation, the corrected reading answers.
    assert conn.execute(
        "SELECT class_kind FROM sec_registration_end_events(5133, '2013-09-01')"
    ).fetchall() == [("equity",)]
    # The index that carried the first version now carries the current one: a
    # reload under the new parser changes nothing.
    stats = loader.run([], dsn=dsn, dry_run=False, form_indexes=[index], documents=documents,
                       reconciled_on=d(2013, 12, 1))
    assert (stats[0]["inserted"], stats[0]["retired"], stats[1]["derived"]) == (0, 0, 0)


def test_an_end_re_derived_years_later_takes_effect_at_its_filing(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4223111427: a parser change re-derives an old end, a
    restatement of our reading known from the filing's own date. It is still
    judged against the issuer's covers filed before it, not the class structure
    of years later. An end that the public record itself adds years later (a
    rebuilt index that moves it to this CIK) is known only from that correction,
    and takes effect at its filing all the same: covers filed after the end keep
    the hold they reopened (v1 dated such an end at its correction and read the
    holder as ended)."""
    conn, dsn = schema_dsn
    _observe(conn, 4242, "SOLO", "2016-01-10")
    docs = tmp_path / "docs"
    docs.mkdir()
    gmv = "0001078782-11-001558"  # a 15-12G of the common stock
    (docs / f"{gmv}.txt").write_bytes((FILINGS / f"{gmv}.txt").read_bytes())
    index = _index(tmp_path / "2016QTR1.form.gz", ("15-12G", 4242, "2016-03-01", gmv))
    documents = loader.EventDocuments(docs, None)
    loader.run([], dsn=dsn, dry_run=False, form_indexes=[index], documents=documents,
               reconciled_on=d(2016, 4, 1))
    # The issuer registers again, now with two listed classes.
    for filed in ("2018-05-01", "2019-09-02"):
        two = _observe(conn, 4242, "SOLO", filed, class_key=CLASS_A)
        _observe(conn, 4242, "DUO", filed, class_key=CLASS_B, adsh=two)
    assert _ends(conn, 4242, "2019-01-01") == [("15-12G", d(2016, 3, 2), False)]
    monkeypatch.setattr(loader, "EVENT_PARSER_VERSION", "sec_event_class_next")
    assert loader.derive_event_classes(conn, documents, reconciled_on=d(2020, 6, 1))[
        "derived"] == 1
    assert _ends(conn, 4242, "2019-01-01") == [("15-12G", d(2016, 3, 2), False)]
    assert _ends(conn, 4242, "2021-01-01") == [("15-12G", d(2016, 3, 2), False)]
    assert conn.execute(
        "SELECT available_on, effective_on FROM sec_issuer_end_events(4242, '2021-01-01')"
    ).fetchall() == [(d(2016, 3, 2), d(2016, 3, 2))]
    # A source correction: the 2016 index is rebuilt in 2020 and lists a 15-12G
    # of 2016 under CIK 4343, whose single class showed SOLE until 2016 and again
    # from 2018. Visible from the correction; in effect from its filing.
    _observe(conn, 4343, "SOLE", "2016-01-10")
    for filed in ("2018-05-01", "2019-09-02"):
        _observe(conn, 4343, "SOLE", filed)
    moved = "0000004343-16-000001"
    _index(index, ("15-12G", 4242, "2016-03-01", gmv), ("15-12G", 9999, "2016-03-01", moved))
    loader.run([], dsn=dsn, dry_run=False, form_indexes=[index], reconciled_on=d(2016, 4, 2))
    _index(index, ("15-12G", 4242, "2016-03-01", gmv), ("15-12G", 4343, "2016-03-01", moved))
    loader.run([], dsn=dsn, dry_run=False, form_indexes=[index], reconciled_on=d(2020, 6, 1))
    assert conn.execute(
        "SELECT available_on, effective_on FROM sec_issuer_end_events(4343, '2021-01-01')"
    ).fetchall() == [(d(2020, 6, 1), d(2016, 3, 2))]
    assert _ends(conn, 4343, "2020-05-31") == []
    for as_of in ("2020-05-31", "2020-07-01"):
        assert _issuer(conn, "SOLE", as_of)[:2] == ("resolved", 4343), as_of
    assert _issuer(conn, "SOLE", "2016-06-01")[:2] == ("resolved", 4343)  # as known then
    assert _line(conn, 4343, "", "2020-07-01")[0] == "resolved"


def test_a_fact_dropped_and_carried_again_is_known_again_from_its_return(
    schema_dsn, tmp_path: Path,
) -> None:
    """Codex thread 4221061354: a republication drops accession A2 whole, a later
    one restores it. The restored fact is knowable from its return only."""
    conn, dsn = schema_dsn
    path = tmp_path / "2024q1_notes.zip"

    def publish(with_a2: bool) -> None:
        path.unlink(missing_ok=True)
        _write_package(
            path,
            [_sub(A1, 1067983, "10-K", "20240226", "2024-02-24 08:00:05.0"),
             *([_sub(A2, 14693, "8-K", "20240305", "")] if with_a2 else [])],
            [_fact(A1, "TradingSymbol", "BRK.B", dimh="0xbbx"),
             *([_fact(A2, "TradingSymbol", "BFB", dimh="0xccc")] if with_a2 else [])],
            [], DIMS,
        )

    publish(True)
    loader.run([path], dsn=dsn, dry_run=False, reconciled_on=d(2024, 4, 1))
    publish(False)
    loader.run([path], dsn=dsn, dry_run=False, reconciled_on=d(2024, 5, 1))
    publish(True)
    stats = loader.run([path], dsn=dsn, dry_run=False, reconciled_on=d(2024, 6, 1))
    assert (stats[0]["inserted"], stats[0]["retired"]) == (1, 0)
    assert conn.execute(
        "SELECT available_on, retired_on FROM sec_ticker_cik_observations "
        "WHERE ticker = 'BFB' ORDER BY id"
    ).fetchall() == [(d(2024, 3, 6), d(2024, 5, 1)), (d(2024, 6, 1), None)]
    assert _issuer(conn, "BF-B", "2024-04-15")[:2] == ("resolved", 14693)
    assert _issuer(conn, "BF-B", "2024-05-15")[0] == "missing"  # known absent then
    assert _issuer(conn, "BF-B", "2024-06-01")[:2] == ("resolved", 14693)
    assert conn.execute(
        "SELECT fact_table, loaded_on, retired_on FROM sec_ticker_cik_package_members "
        "WHERE adsh = %s ORDER BY id", (A2,)
    ).fetchall() == [
        ("observation", d(2024, 4, 1), d(2024, 5, 1)), ("share_count", d(2024, 4, 1),
                                                         d(2024, 5, 1)),
        ("observation", d(2024, 6, 1), None), ("share_count", d(2024, 6, 1), None),
    ]


def test_an_unreadable_filing_carries_its_derived_class_forward(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4221061363: under --no-fetch with a cache miss the event keeps
    its class; only a parse replaces it. Read by this parser version, it is
    carried without a read (Codex thread 4223252991)."""
    conn, dsn = schema_dsn
    _observe(conn, 5133, "AM", "2013-07-10")
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "0000876661-13-000657.txt").write_bytes(
        (FILINGS / "0000876661-13-000657.txt").read_bytes())
    index = _index(tmp_path / "2013QTR3.form.gz",
                   ("25-NSE", 5133, "2013-08-12", "0000876661-13-000657"))
    loader.run([], dsn=dsn, dry_run=False, form_indexes=[index],
               documents=loader.EventDocuments(docs, None), reconciled_on=d(2013, 10, 1))
    read_by = loader.EVENT_PARSER_VERSION
    empty = tmp_path / "empty"
    stats = loader.run([], dsn=dsn, dry_run=False, form_indexes=[index],
                       documents=loader.EventDocuments(empty, None),
                       reconciled_on=d(2013, 11, 1))
    assert (stats[0]["inserted"], stats[0]["retired"], stats[0]["class_reused"]) == (0, 0, 1)
    assert (stats[0]["filings_missing"], stats[1]["derived"]) == (0, 0)
    # Under another parser version the filing must be read again; missing, the
    # class read before is carried and nothing changes.
    monkeypatch.setattr(loader, "EVENT_PARSER_VERSION", "sec_event_class_next")
    stats = loader.run([], dsn=dsn, dry_run=False, form_indexes=[index],
                       documents=loader.EventDocuments(empty, None),
                       reconciled_on=d(2013, 12, 1))
    assert (stats[0]["inserted"], stats[0]["retired"]) == (0, 0)
    assert (stats[0]["class_carried"], stats[0]["filings_missing"]) == (1, 1)
    assert (stats[1]["derived"], stats[1]["filings_missing"]) == (0, 1)
    assert conn.execute(
        "SELECT class_kind, parser_version, retired_on FROM sec_registration_events"
    ).fetchall() == [("equity", read_by, None)]


def _month(path: Path, filings: list[tuple[str, int, str, str]]) -> Path:
    path.unlink(missing_ok=True)
    return _write_package(
        path,
        [_sub(adsh, cik, "10-Q", filed) for adsh, cik, filed, _ in filings],
        [_fact(adsh, "TradingSymbol", symbol) for adsh, _, _, symbol in filings],
    )


B1, B2, B3, B4 = ("0000000061-25-000001", "0000000062-25-000001",
                  "0000000063-25-000001", "0000000064-25-000001")


def test_a_quarterly_package_supersedes_the_months_it_consolidates(
    schema_dsn, tmp_path: Path,
) -> None:
    """Codex thread 4221061372: DERA replaces 2025_10..2025_12 with 2025q4."""
    conn, dsn = schema_dsn
    m10 = _month(tmp_path / "2025_10_notes.zip",
                 [(B1, 61, "20251015", "AAA"), (B2, 62, "20251020", "BBB")])
    m11 = _month(tmp_path / "2025_11_notes.zip", [(B3, 63, "20251110", "CCC")])
    loader.run([m10, m11], dsn=dsn, dry_run=False, reconciled_on=d(2025, 12, 1))
    # The quarterly keeps AAA, drops BBB, corrects CCC to CCD and adds DDD.
    q4 = _month(tmp_path / "2025q4_notes.zip",
                [(B1, 61, "20251015", "AAA"), (B3, 63, "20251110", "CCD"),
                 (B4, 64, "20251215", "DDD")])
    stats = loader.run([q4], dsn=dsn, dry_run=False, reconciled_on=d(2026, 12, 1))
    assert (stats[0]["inserted"], stats[0]["superseded"], stats[0]["superseded_retired"]) == (
        2, ["2025_10_notes.zip", "2025_11_notes.zip"], 2)
    assert conn.execute(
        "SELECT ticker, available_on, retired_on FROM sec_ticker_cik_observations ORDER BY ticker"
    ).fetchall() == [
        ("AAA", d(2025, 10, 16), None),  # carried by the quarterly: untouched
        ("BBB", d(2025, 10, 21), d(2026, 12, 1)),
        ("CCC", d(2025, 11, 11), d(2026, 12, 1)),
        ("CCD", d(2026, 12, 1), None),  # a correction of a known accession
        ("DDD", d(2025, 12, 16), None),  # first seen
    ]
    assert conn.execute(
        "SELECT source_package, superseded_by, superseded_on FROM sec_ticker_cik_packages "
        'ORDER BY source_package COLLATE "C"'
    ).fetchall() == [
        ("2025_10_notes.zip", "2025q4_notes.zip", d(2026, 12, 1)),
        ("2025_11_notes.zip", "2025q4_notes.zip", d(2026, 12, 1)),
        ("2025q4_notes.zip", None, None),
    ]
    assert _issuer(conn, "BBB", "2026-06-01")[:2] == ("resolved", 62)  # as known then
    assert _issuer(conn, "BBB", "2026-12-01")[0] == "missing"
    # A monthly package of a consolidated quarter is not loaded again.
    again = loader.run([m11], dsn=dsn, dry_run=False, reconciled_on=d(2027, 1, 1))
    assert again == [{"package": "2025_11_notes.zip",
                      "skipped": "superseded by 2025q4_notes.zip"}]


def test_quarterly_packages_sort_after_their_months() -> None:
    names = ["2025q4_notes.zip", "2025_12_notes.zip", "2025_10_notes.zip", "2026_01_notes.zip",
             "2025q3_notes.zip"]
    assert [p.name for p in sorted((Path(n) for n in names), key=loader.package_sort_key)] == [
        "2025q3_notes.zip", "2025_10_notes.zip", "2025_12_notes.zip", "2025q4_notes.zip",
        "2026_01_notes.zip",
    ]
    assert loader.quarter_months("2025q4_notes.zip") == (2025, (10, 11, 12))
    assert loader.quarter_months("2025_10_notes.zip") is None
    assert loader.covering_quarter("2025_11_notes.zip") == "2025q4"


def test_a_registration_carried_by_a_dera_package_is_first_seen_by_its_index(
    schema_dsn, tmp_path: Path,
) -> None:
    """Codex thread 4221400851: an XBRL 10-12B carried by an FSN package is a
    first-seen event when the index lists it, dated filed + 1."""
    conn, dsn = schema_dsn
    reg = "0000000070-24-000010"
    package = _write_package(tmp_path / "2024q1_notes.zip",
                             [_sub(reg, 70, "10-12B", "20240212", "")],
                             [_fact(reg, "TradingSymbol", "NEWCO")])
    index = _index(tmp_path / "2024QTR1.form.gz", ("10-12B", 70, "2024-02-12", reg))
    loader.run([package], dsn=dsn, dry_run=False, form_indexes=[index],
               reconciled_on=d(2024, 6, 1))
    assert conn.execute("SELECT form, available_on FROM sec_registration_events"
                        ).fetchall() == [("10-12B", d(2024, 2, 13))]


def test_an_event_listed_again_keeps_the_class_read_before(
    schema_dsn, tmp_path: Path,
) -> None:
    """Codex thread 4221526158: an index drops an event and lists it again while
    its filing cannot be read; the class read before is carried."""
    conn, dsn = schema_dsn
    _observe(conn, 5133, "AM", "2013-07-10")
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "0000876661-13-000657.txt").write_bytes(
        (FILINGS / "0000876661-13-000657.txt").read_bytes())
    row = ("25-NSE", 5133, "2013-08-12", "0000876661-13-000657")
    other = ("15-12G", 4242, "2013-08-23", "0000004242-13-000001")
    index = _index(tmp_path / "2013QTR3.form.gz", row, other)
    loader.run([], dsn=dsn, dry_run=False, form_indexes=[index],
               documents=loader.EventDocuments(docs, None), reconciled_on=d(2013, 10, 1))
    _index(index, other)
    loader.run([], dsn=dsn, dry_run=False, form_indexes=[index], reconciled_on=d(2013, 11, 1))
    _index(index, row, other)
    stats = loader.run([], dsn=dsn, dry_run=False, form_indexes=[index],
                       documents=loader.EventDocuments(tmp_path / "empty", None),
                       reconciled_on=d(2013, 12, 1))
    assert (stats[0]["inserted"], stats[0]["class_reused"]) == (1, 1)
    assert conn.execute(
        "SELECT class_kind, available_on, retired_on FROM sec_registration_events "
        "WHERE cik = 5133 ORDER BY id"
    ).fetchall() == [("equity", d(2013, 8, 13), d(2013, 11, 1)),
                     ("equity", d(2013, 12, 1), None)]


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
FILING_BASE = "https://www.sec.gov/Archives/edgar/data/"


def _fake_sec(tmp_path: Path, packages: dict[str, bytes], indexes: dict[str, bytes],
              filings: dict[str, bytes] | None = None,
              last_modified: dict[str, str] | None = None):
    """An httpx client answering the SEC listing, package, index and filing URLs.
    Packages carry a Last-Modified header (as the SEC sends) from
    ``last_modified``, by default one that changes with the content; an empty
    string sends none."""
    import hashlib

    import httpx

    calls: list[tuple[str, str]] = []

    def validators(name: str, body: bytes) -> dict[str, str]:
        stamp = (last_modified or {}).get(name, hashlib.sha1(body).hexdigest()[:12])
        return {"last-modified": stamp} if stamp else {}

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
            name = url[len(FSN_BASE):]
            body = packages[name]
            if request.method == "HEAD":
                return httpx.Response(200, headers={"content-length": str(len(body)),
                                                    **validators(name, body)})
            return httpx.Response(200, content=body, headers=validators(name, body))
        if url.startswith(FILING_BASE):
            adsh = url.rsplit("/", 1)[1].removesuffix(".txt")
            if adsh in (filings or {}):
                return httpx.Response(200, content=filings[adsh])
            return httpx.Response(404)
        for key, body in indexes.items():
            if url.endswith(key):
                return httpx.Response(200, content=body)
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler),
                          headers={"User-Agent": loader.USER_AGENT})
    return client, calls


def _index_bytes(*rows: tuple[str, int, str, str]) -> bytes:
    return gzip.compress("".join(
        f"{form:<17}SOME CO      {cik}     {filed}  edgar/data/{cik}/{adsh}.txt\n"
        for form, cik, filed, adsh in rows
    ).encode())


def test_worker_loads_new_packages_and_the_open_quarter_then_idles(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tempfile

    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    monkeypatch.setattr(loader, "FILING_SPACING_S", 0)
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
    notes = "0000876661-17-000048"  # a notes delisting of a cover CIK
    indexes = {
        "2024/QTR3/form.gz": _index_bytes(("15-12G", 5907, "2024-08-01", "0000005907-24-000001")),
        "2024/QTR4/form.gz": _index_bytes(("15-12G", 5908, "2024-11-01", "0000005908-24-000001"),
                                          ("25-NSE", 732717, "2024-11-08", notes)),
    }
    filings = {notes: (FILINGS / f"{notes}.txt").read_bytes()}

    client, calls = _fake_sec(tmp_path, packages, indexes, filings)
    stats = worker.run(dsn, calc_date="2024-11-15", limit=1, client=client)
    assert stats["state"] == "ok" and stats["backlog"] == 2
    assert [p["package"] for p in stats["packages"]] == ["2024q1_notes.zip"]  # oldest first
    assert [i["events"] for i in stats["form_indexes"]] == [1, 2]
    assert stats["filings_fetched"] == 0  # CIK 732717 has no cover data yet

    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert [p["package"] for p in stats["packages"]] == ["2024_10_notes.zip"]
    assert stats["filings_fetched"] == 1
    assert ("GET", f"{FILING_BASE}732717/{notes.replace('-', '')}/{notes}.txt") in calls
    assert conn.execute(
        "SELECT class_kind FROM sec_registration_events WHERE retired_on IS NULL AND cik = 732717"
    ).fetchall() == [("other",)]
    # Read only now (a correction from today): today's lineage sees notes, no end.
    assert _span(conn, "T", 732717) == [("", d(2024, 11, 6), None, None, d(2024, 11, 6),
                                         None, None)]

    calls.clear()
    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert stats["state"] == "noop" and stats["packages"] == [] and stats["backlog"] == 0
    assert ("GET", FSN_BASE + "2024_10_notes.zip") not in calls  # HEAD only

    # A republished newest month (different size) is reconciled.
    packages["2024_10_notes.zip"] = _write_package(
        build / "2024_10_notes.zip",
        [_sub("0000000005-24-000001", 732717, "8-K", "20241105")],
        [_fact("0000000005-24-000001", "TradingSymbol", "T"),
         _fact("0000000005-24-000001", "Security12bTitle", "Common Stock")],
    ).read_bytes()
    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert [(p["package"], p["inserted"], p["retired"]) for p in stats["packages"]] == [
        ("2024_10_notes.zip", 1, 1),
    ]
    assert stats["state"] == "ok"
    assert list(scratch.iterdir()) == []  # every run removed its downloads


def test_worker_refetches_a_republished_package_over_its_cached_copy(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    conn, dsn = schema_dsn
    cache = tmp_path / "cache"
    build = tmp_path / "build"
    build.mkdir()

    def month(symbol: str) -> bytes:
        return _write_package(
            build / "2024_10_notes.zip",
            [_sub("0000000005-24-000001", 732717, "8-K", "20241105")],
            [_fact("0000000005-24-000001", "TradingSymbol", symbol)],
        ).read_bytes()

    packages = {"2024_10_notes.zip": month("T")}
    client, calls = _fake_sec(tmp_path, packages, {})
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    first = worker.run(dsn, calc_date="2024-11-15", client=client, cache_dir=cache)
    assert [p["package"] for p in first["packages"]] == ["2024_10_notes.zip"]
    assert (cache / "2024_10_notes.zip").exists()  # kept in the persistent cache

    packages["2024_10_notes.zip"] = month("TLONGER")  # republished: different size
    calls.clear()
    second = worker.run(dsn, calc_date="2024-11-15", client=client, cache_dir=cache)
    assert ("GET", FSN_BASE + "2024_10_notes.zip") in calls
    assert second["state"] == "ok"
    assert [(p["package"], p["inserted"], p["retired"]) for p in second["packages"]] == [
        ("2024_10_notes.zip", 1, 1),
    ]
    assert conn.execute(
        "SELECT ticker FROM sec_ticker_cik_observations WHERE retired_on IS NULL"
    ).fetchall() == [("TLONGER",)]
    assert (cache / "2024_10_notes.zip").read_bytes() == packages["2024_10_notes.zip"]


def test_worker_reports_a_share_count_only_change_as_ok(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4215648954: a republication that changes only a count wrote."""
    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    _, dsn = schema_dsn
    build = tmp_path / "build"
    build.mkdir()

    def month(shares: str) -> bytes:
        return _write_package(
            build / "2024_10_notes.zip",
            [_sub("0000000005-24-000001", 732717, "10-Q", "20241105")],
            [_fact("0000000005-24-000001", "TradingSymbol", "T")],
            [_shares("0000000005-24-000001", shares, ddate="20241031")],
        ).read_bytes()

    packages = {"2024_10_notes.zip": month("7000")}
    client, _ = _fake_sec(tmp_path, packages, {})
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    assert worker.run(dsn, calc_date="2024-11-15", client=client)["state"] == "ok"
    packages["2024_10_notes.zip"] = month("7170000000")  # republished: the count only
    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert [(p["inserted"], p["retired"], p["shares_inserted"], p["shares_retired"])
            for p in stats["packages"]] == [(0, 0, 1, 1)]
    assert stats["state"] == "ok"


def test_worker_supersedes_monthly_packages_when_their_quarter_is_listed(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The DERA listing replaces 2025_10 and 2025_11 with 2025q4."""
    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    conn, dsn = schema_dsn
    build = tmp_path / "build"
    build.mkdir()
    packages = {
        "2025_10_notes.zip": _month(build / "2025_10_notes.zip",
                                    [(B1, 61, "20251015", "AAA"),
                                     (B2, 62, "20251020", "BBB")]).read_bytes(),
        "2025_11_notes.zip": _month(build / "2025_11_notes.zip",
                                    [(B3, 63, "20251110", "CCC")]).read_bytes(),
    }
    client, _ = _fake_sec(tmp_path, packages, {})
    assert worker.run(dsn, calc_date="2025-12-01", client=client)["state"] == "ok"
    packages.clear()
    packages["2025q4_notes.zip"] = _month(build / "2025q4_notes.zip",
                                          [(B1, 61, "20251015", "AAA"),
                                           (B3, 63, "20251110", "CCC")]).read_bytes()
    stats = worker.run(dsn, calc_date="2026-12-01", client=client)
    assert stats["state"] == "ok"
    assert [(p["package"], p["inserted"], p["superseded"], p["superseded_retired"])
            for p in stats["packages"]] == [
        ("2025q4_notes.zip", 0, ["2025_10_notes.zip", "2025_11_notes.zip"], 1),
    ]
    assert conn.execute(
        "SELECT ticker FROM sec_ticker_cik_observations WHERE retired_on IS NULL ORDER BY 1"
    ).fetchall() == [("AAA",), ("CCC",)]


def test_superseded_monthly_packages_never_take_a_worker_slot(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4221400839: with WORKER_LIMIT 1, a listed monthly package of a
    consolidated quarter must not starve a new package."""
    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    conn, dsn = schema_dsn
    build = tmp_path / "build"
    build.mkdir()
    quarter = _month(build / "2025q4_notes.zip", [(B1, 61, "20251015", "AAA")])
    loader.run([quarter], dsn=dsn, dry_run=False, reconciled_on=d(2026, 1, 5))
    packages = {
        "2025_11_notes.zip": _month(build / "2025_11_notes.zip",
                                    [(B3, 63, "20251110", "CCC")]).read_bytes(),
        "2025q4_notes.zip": quarter.read_bytes(),
        "2026_01_notes.zip": _month(build / "2026_01_notes.zip",
                                    [(B4, 64, "20260115", "DDD")]).read_bytes(),
    }
    client, _ = _fake_sec(tmp_path, packages, {})
    stats = worker.run(dsn, calc_date="2026-02-01", limit=1, client=client)
    assert stats["backlog"] == 1
    assert [p["package"] for p in stats["packages"]] == ["2026_01_notes.zip"]


def test_worker_detects_a_same_size_republication_by_its_validator_or_digest(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4221526170: a republished month of the same byte length."""
    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    conn, dsn = schema_dsn
    build = tmp_path / "build"
    build.mkdir()

    def month(symbol: str) -> bytes:
        return _write_package(
            build / "2024_10_notes.zip",
            [_sub("0000000005-24-000001", 732717, "8-K", "20241105")],
            [_fact("0000000005-24-000001", "TradingSymbol", symbol)],
            compression=zipfile.ZIP_STORED,
        ).read_bytes()

    first, second = month("TT"), month("TU")
    assert len(first) == len(second) and first != second
    # The Last-Modified the SEC sends decides when both sides have one.
    packages, stamps = {"2024_10_notes.zip": first}, {"2024_10_notes.zip": "Mon, 07 Oct"}
    client, calls = _fake_sec(tmp_path, packages, {}, last_modified=stamps)
    worker.run(dsn, calc_date="2024-11-15", client=client)
    assert conn.execute("SELECT remote_last_modified FROM sec_ticker_cik_packages"
                        ).fetchall() == [("Mon, 07 Oct",)]
    calls.clear()
    assert worker.run(dsn, calc_date="2024-11-15", client=client)["state"] == "noop"
    assert ("GET", FSN_BASE + "2024_10_notes.zip") not in calls
    packages["2024_10_notes.zip"], stamps["2024_10_notes.zip"] = second, "Tue, 08 Oct"
    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert [(p["inserted"], p["retired"]) for p in stats["packages"]] == [(1, 1)]
    # Without validators, the newest package is downloaded and its SHA-256 compared.
    packages["2024_10_notes.zip"], stamps["2024_10_notes.zip"] = first, ""
    conn.execute("UPDATE sec_ticker_cik_packages SET remote_last_modified = NULL")
    calls.clear()
    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert [(p["inserted"], p["retired"]) for p in stats["packages"]] == [(1, 1)]
    assert calls.count(("GET", FSN_BASE + "2024_10_notes.zip")) == 1  # fetched once
    calls.clear()
    assert worker.run(dsn, calc_date="2024-11-15", client=client)["state"] == "noop"
    assert conn.execute(
        "SELECT ticker FROM sec_ticker_cik_observations WHERE retired_on IS NULL"
    ).fetchall() == [("TT",)]


def test_a_stopped_supersession_is_completed_by_the_next_run(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4221526183: the quarterly committed, its supersession did not."""
    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    conn, dsn = schema_dsn
    build = tmp_path / "build"
    build.mkdir()
    m10 = _month(build / "2025_10_notes.zip",
                 [(B1, 61, "20251015", "AAA"), (B2, 62, "20251020", "BBB")])
    loader.run([m10], dsn=dsn, dry_run=False, reconciled_on=d(2025, 12, 1))
    q4 = _month(build / "2025q4_notes.zip", [(B1, 61, "20251015", "AAA")])
    loader.load_package(conn, loader.parse_package(q4), reconciled_on=d(2026, 12, 1))
    # ... the run stops here, before supersede_monthly_packages.
    client, _ = _fake_sec(tmp_path, {"2025q4_notes.zip": q4.read_bytes()}, {})
    stats = worker.run(dsn, calc_date="2026-12-08", client=client)
    assert [(r["package"], r["superseded"], r["superseded_retired"])
            for r in stats["resumed_supersession"]] == [
        ("2025q4_notes.zip", ["2025_10_notes.zip"], 1),
    ]
    assert stats["state"] == "ok"
    assert conn.execute(
        "SELECT ticker, retired_on FROM sec_ticker_cik_observations ORDER BY ticker"
    ).fetchall() == [("AAA", None), ("BBB", dt.date.today())]
    again = worker.run(dsn, calc_date="2026-12-08", client=client)
    assert again["resumed_supersession"] == [] and again["state"] == "noop"


def test_worker_fails_on_an_empty_package_listing(schema_dsn, tmp_path: Path) -> None:
    """Codex thread 4222086469: no package link is an error, not "nothing new"."""
    from src.workers import sec_ticker_cik_history as worker

    _, dsn = schema_dsn
    client, _ = _fake_sec(tmp_path, {}, {})
    with pytest.raises(RuntimeError, match="no package links"):
        worker.run(dsn, calc_date="2024-11-15", client=client)


def test_a_workstation_cache_is_verified_before_the_initial_load(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4222197135 / --verify-cache: a cached zip the SEC republished
    (newer Last-Modified, or another size) is fetched again before the load, and
    only the fresh validators are recorded."""
    import os

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    conn, dsn = schema_dsn
    build, cache = tmp_path / "build", tmp_path / "cache"
    build.mkdir()
    cache.mkdir()

    def month(name: str, symbol: str) -> bytes:
        return _write_package(
            build / name, [_sub("0000000005-24-000001", 732717, "8-K", "20241105")],
            [_fact("0000000005-24-000001", "TradingSymbol", symbol)],
            compression=zipfile.ZIP_STORED,
        ).read_bytes()

    remote = {"2024_09_notes.zip": month("2024_09_notes.zip", "AA"),
              "2024_10_notes.zip": month("2024_10_notes.zip", "BB"),
              "2024_11_notes.zip": month("2024_11_notes.zip", "CC")}
    stamps = {"2024_09_notes.zip": "Tue, 01 Oct 2024 10:00:00 GMT",
              "2024_10_notes.zip": "Fri, 06 Dec 2024 10:00:00 GMT",
              "2024_11_notes.zip": "Fri, 06 Dec 2024 10:00:00 GMT"}
    (cache / "2024_09_notes.zip").write_bytes(remote["2024_09_notes.zip"])  # current
    (cache / "2024_10_notes.zip").write_bytes(month("old", "XX"))  # republished since
    for name in ("2024_09_notes.zip", "2024_10_notes.zip"):
        cached_at = dt.datetime(2024, 11, 20, tzinfo=dt.timezone.utc).timestamp()
        os.utime(cache / name, (cached_at, cached_at))
    client, calls = _fake_sec(tmp_path, remote, {}, last_modified=stamps)
    validators = loader.verify_package_cache(client, cache)
    gets = [url for method, url in calls if method == "GET" and url.startswith(FSN_BASE)]
    assert sorted(gets) == [FSN_BASE + "2024_10_notes.zip", FSN_BASE + "2024_11_notes.zip"]
    assert (cache / "2024_10_notes.zip").read_bytes() == remote["2024_10_notes.zip"]
    assert validators[(cache / "2024_10_notes.zip").resolve()] == (
        None, "Fri, 06 Dec 2024 10:00:00 GMT")
    loader.run(loader.discover_packages(cache), dsn=dsn, dry_run=False,
               validators=validators, reconciled_on=d(2024, 12, 10))
    assert conn.execute(
        "SELECT source_package, remote_last_modified FROM sec_ticker_cik_packages "
        "ORDER BY source_package"
    ).fetchall() == [(name, stamps[name]) for name in sorted(remote)]
    calls.clear()
    loader.verify_package_cache(client, cache)  # everything is current now
    assert not [url for method, url in calls if method == "GET" and url.startswith(FSN_BASE)]


def test_a_verified_load_reads_only_the_listed_packages(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Gate P2 / Codex thread 4222376260: with --verify-cache, a zip in the cache
    that the SEC no longer lists (or one passed by name) was not verified, so it
    is ignored, with a log line, instead of loaded."""
    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    conn, dsn = schema_dsn
    build, cache = tmp_path / "build", tmp_path / "cache"
    build.mkdir()
    cache.mkdir()
    listed = _write_package(
        build / "2024_10_notes.zip", [_sub("0000000005-24-000001", 732717, "8-K", "20241105")],
        [_fact("0000000005-24-000001", "TradingSymbol", "BB")], compression=zipfile.ZIP_STORED,
    ).read_bytes()
    (cache / "2024_10_notes.zip").write_bytes(listed)
    stray = _write_package(
        cache / "2024_09_notes.zip", [_sub("0000000006-24-000001", 732717, "8-K", "20241005")],
        [_fact("0000000006-24-000001", "TradingSymbol", "AA")],
    )
    monkeypatch.setattr(loader, "sec_client", lambda: _fake_sec(
        tmp_path, {"2024_10_notes.zip": listed}, {},
        last_modified={"2024_10_notes.zip": ""})[0])
    args = ["--verify-cache", "--no-events", "--packages-dir", str(cache), "--dsn", dsn,
            "--reconciled-on", "2024-12-10"]
    assert loader.main(args) == 0
    assert '{"ignored_unlisted_packages": ["2024_09_notes.zip"]}' in capsys.readouterr().out
    assert conn.execute("SELECT source_package FROM sec_ticker_cik_packages").fetchall() == [
        ("2024_10_notes.zip",)]
    assert conn.execute("SELECT DISTINCT ticker FROM sec_ticker_cik_observations"
                        ).fetchall() == [("BB",)]
    # Named on the command line, an unlisted package is ignored the same way.
    with pytest.raises(SystemExit):
        loader.main([str(stray), *args])
    assert "ignored_unlisted_packages" in capsys.readouterr().out
    assert conn.execute("SELECT count(*) FROM sec_ticker_cik_packages").fetchone() == (1,)


def _stored_month(build: Path, name: str, symbol: str, adsh: str = "0000000005-24-000001",
                  filed: str = "20241105") -> bytes:
    """A one-filing package, stored uncompressed so equal-length symbols give
    equal-length zips."""
    return _write_package(
        build / name, [_sub(adsh, 732717, "8-K", filed)], [_fact(adsh, "TradingSymbol", symbol)],
        compression=zipfile.ZIP_STORED,
    ).read_bytes()


def test_worker_reloads_an_older_package_the_sec_republished(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4221867196: the SEC republished 2010q1-2013q4 in 2024. Every
    loaded package still listed is checked, not only the newest."""
    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    monkeypatch.setattr(loader, "FILING_SPACING_S", 0)
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    conn, dsn = schema_dsn
    build = tmp_path / "build"
    build.mkdir()
    packages = {
        "2024q1_notes.zip": _stored_month(build, "2024q1_notes.zip", "OLD",
                                          "0000000004-24-000001", "20240305"),
        "2024_10_notes.zip": _stored_month(build, "2024_10_notes.zip", "TT"),
    }
    stamps = {"2024q1_notes.zip": "Mon, 01 Apr 2024 10:00:00 GMT",
              "2024_10_notes.zip": "Tue, 05 Nov 2024 10:00:00 GMT"}
    client, calls = _fake_sec(tmp_path, packages, {}, last_modified=stamps)
    worker.run(dsn, calc_date="2024-11-15", client=client)
    calls.clear()
    assert worker.run(dsn, calc_date="2024-11-15", client=client)["state"] == "noop"
    assert not [url for method, url in calls if method == "GET" and url.startswith(FSN_BASE)]
    # The older quarterly is republished: same size, a new Last-Modified.
    packages["2024q1_notes.zip"] = _stored_month(build, "2024q1_notes.zip", "NEW",
                                                 "0000000004-24-000001", "20240305")
    stamps["2024q1_notes.zip"] = "Wed, 25 Sep 2024 11:44:37 GMT"
    stats = worker.run(dsn, calc_date="2024-11-15", client=client)
    assert stats["republished"] == ["2024q1_notes.zip"]
    assert [(p["package"], p["inserted"], p["retired"]) for p in stats["packages"]] == [
        ("2024q1_notes.zip", 1, 1)]
    assert conn.execute(
        "SELECT remote_last_modified FROM sec_ticker_cik_packages "
        "WHERE source_package = '2024q1_notes.zip'").fetchone() == (stamps["2024q1_notes.zip"],)


def test_a_monthly_package_about_to_be_superseded_is_not_rechecked(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4224877371: a loaded month whose quarterly is now listed is
    neither checked nor reloaded, even with a new validator: with WORKER_LIMIT 1
    the quarterly loads (and supersedes it) first."""
    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    monkeypatch.setattr(loader, "FILING_SPACING_S", 0)
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    _, dsn = schema_dsn
    build = tmp_path / "build"
    build.mkdir()
    packages = {"2025_10_notes.zip": _stored_month(build, "2025_10_notes.zip", "AA",
                                                   "0000000061-25-000001", "20251015")}
    stamps = {"2025_10_notes.zip": "Mon, 03 Nov 2025 10:00:00 GMT",
              "2025q4_notes.zip": "Mon, 05 Oct 2026 10:00:00 GMT"}
    client, calls = _fake_sec(tmp_path, packages, {}, last_modified=stamps)
    worker.run(dsn, calc_date="2025-11-15", client=client)
    stamps["2025_10_notes.zip"] = "Fri, 02 Oct 2026 10:00:00 GMT"  # republished
    packages["2025q4_notes.zip"] = _stored_month(build, "2025q4_notes.zip", "AA",
                                                 "0000000061-25-000001", "20251015")
    calls.clear()
    stats = worker.run(dsn, calc_date="2026-10-08", limit=1, client=client)
    assert (stats["backlog"], stats["republished"]) == (1, [])
    assert [p["package"] for p in stats["packages"]] == ["2025q4_notes.zip"]
    assert ("HEAD", FSN_BASE + "2025_10_notes.zip") not in calls


def test_a_package_recorded_without_validators_is_downloaded_once(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4221867184: the validators recorded are those of the download
    that was loaded, in its transaction. A package recorded without them (a v1
    run whose HEAD after the load failed) is downloaded once, its digest matches,
    and its validators are recorded: the next run compares them, no download."""
    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    monkeypatch.setattr(loader, "FILING_SPACING_S", 0)
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    conn, dsn = schema_dsn
    build = tmp_path / "build"
    build.mkdir()
    packages = {"2024_10_notes.zip": _stored_month(build, "2024_10_notes.zip", "TT")}
    stamp = "Tue, 05 Nov 2024 10:00:00 GMT"
    client, calls = _fake_sec(tmp_path, packages, {},
                              last_modified={"2024_10_notes.zip": stamp})
    worker.run(dsn, calc_date="2024-11-15", client=client)
    assert ("HEAD", FSN_BASE + "2024_10_notes.zip") not in calls  # no HEAD after the load
    validators = "SELECT remote_last_modified FROM sec_ticker_cik_packages"
    assert conn.execute(validators).fetchall() == [(stamp,)]
    conn.execute("UPDATE sec_ticker_cik_packages SET remote_last_modified = NULL")
    gets = [("GET", FSN_BASE + "2024_10_notes.zip")]
    calls.clear()
    assert worker.run(dsn, calc_date="2024-11-15", client=client)["state"] == "noop"
    assert [c for c in calls if c[0] == "GET" and c[1].startswith(FSN_BASE)] == gets
    assert conn.execute(validators).fetchall() == [(stamp,)]
    calls.clear()
    assert worker.run(dsn, calc_date="2024-11-15", client=client)["state"] == "noop"
    assert not [c for c in calls if c[0] == "GET" and c[1].startswith(FSN_BASE)]


def test_a_cached_copy_of_an_unrecorded_package_is_never_loaded(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex thread 4222197135: the persistent cache holds an older revision of a
    package not loaded yet, of the same byte length. The worker loads the bytes it
    downloads, and records their digest with their validators."""
    import hashlib

    from src.workers import sec_ticker_cik_history as worker

    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    monkeypatch.setattr(worker, "_quarters", lambda as_of: [])
    conn, dsn = schema_dsn
    build, cache = tmp_path / "build", tmp_path / "cache"
    build.mkdir()
    cache.mkdir()
    stale = _stored_month(build, "stale.zip", "XX")
    remote = _stored_month(build, "2024_10_notes.zip", "YY")
    assert len(stale) == len(remote) and stale != remote
    (cache / "2024_10_notes.zip").write_bytes(stale)
    client, calls = _fake_sec(tmp_path, {"2024_10_notes.zip": remote}, {})
    worker.run(dsn, calc_date="2024-11-15", client=client, cache_dir=cache)
    assert ("GET", FSN_BASE + "2024_10_notes.zip") in calls
    assert conn.execute("SELECT DISTINCT ticker FROM sec_ticker_cik_observations"
                        ).fetchall() == [("YY",)]
    assert conn.execute("SELECT package_sha256 FROM sec_ticker_cik_packages").fetchone() == (
        hashlib.sha256(remote).hexdigest(),)
    assert (cache / "2024_10_notes.zip").read_bytes() == remote


def test_verify_cache_checks_the_package_file_named_on_the_command_line(
    schema_dsn, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Codex thread 4222924629: with --verify-cache, a package passed by path is
    the file verified (and fetched again when stale), not the file of the same name
    under --packages-dir."""
    monkeypatch.setattr(loader, "DOWNLOAD_SPACING_S", 0)
    conn, dsn = schema_dsn
    build, cache, other = tmp_path / "build", tmp_path / "cache", tmp_path / "other"
    for directory in (build, cache, other):
        directory.mkdir()
    remote = _stored_month(build, "2024_10_notes.zip", "BB")
    (cache / "2024_10_notes.zip").write_bytes(remote)  # current
    (other / "2024_10_notes.zip").write_bytes(_stored_month(build, "x.zip", "XXXX"))  # stale
    monkeypatch.setattr(loader, "sec_client", lambda: _fake_sec(
        tmp_path, {"2024_10_notes.zip": remote}, {},
        last_modified={"2024_10_notes.zip": ""})[0])
    assert loader.main([str(other / "2024_10_notes.zip"), "--verify-cache", "--no-events",
                        "--packages-dir", str(cache), "--dsn", dsn,
                        "--reconciled-on", "2024-12-10"]) == 0
    assert '"fetched_again": 1, "size": 1' in capsys.readouterr().out
    assert (other / "2024_10_notes.zip").read_bytes() == remote
    assert conn.execute("SELECT DISTINCT ticker FROM sec_ticker_cik_observations"
                        ).fetchall() == [("BB",)]


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
