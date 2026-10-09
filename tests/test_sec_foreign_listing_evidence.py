"""Real SEC excerpts plus dated resolver tests in disposable loopback Postgres.

Set SEC_TEST_DATABASE_URL to run SQL tests. Nothing contacts production or SEC.
Fixture provenance and exact excerpt hashes live beside the downloaded excerpts.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

from scripts.sec_foreign_listing_parser import parse_filing

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "sec_foreign_listing_evidence"
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text(encoding="utf-8"))


def parse_fixture(name):
    meta = next(item for item in MANIFEST if item["name"] == name)
    text = (FIXTURES / meta["fixture"]).read_text(encoding="utf-8")
    kwargs = {key: meta[key] for key in (
        "cik", "accession_number", "source_url", "filing_date", "form_type"
    )}
    if "document_role" in meta:
        kwargs["document_role"] = meta["document_role"]
    return parse_filing(text, symbols=[meta["symbol"]], **kwargs)


@pytest.mark.parametrize("name,expected", [
    ("tsm", "ads"), ("zim", "ordinary_direct"),
    ("qgen", "ordinary_direct"), ("cnq", "ordinary_direct"),
])
def test_real_cover_security_types(name, expected):
    rows = [row for row in parse_fixture(name) if row["evidence_kind"] == "listed_type"]
    assert len(rows) == 1
    row = rows[0]
    meta = next(item for item in MANIFEST if item["name"] == name)
    assert row["symbol"] == meta["symbol"]
    assert row["listed_type"] == expected
    tomorrow = (dt.date.fromisoformat(meta["filing_date"]) + dt.timedelta(days=1)).isoformat()
    assert row["effective_from"] == row["available_on"] == tomorrow
    assert row["evidence_text"] and row["evidence_location"]
    assert row["source_url"] == meta["source_url"]


def test_not_for_trading_footnote_overrides_tsm_common_share_tag():
    row = next(row for row in parse_fixture("tsm") if row["evidence_kind"] == "listed_type")
    assert row["listed_type"] == "ads"
    assert "not for trading" in row["evidence_text"].lower()


def test_real_cover_fixture_integrity():
    for meta in MANIFEST:
        payload = (FIXTURES / meta["fixture"]).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == meta["sha256"]
        assert b"\r\n" not in payload
        assert meta["source_url"].startswith("https://www.sec.gov/Archives/edgar/data/")


def test_real_tsm_securities_description_exhibit_is_ratio_corroboration_only():
    rows = parse_fixture("tsm_2024_securities_description")
    assert rows
    assert all(row["evidence_kind"] == "ads_ratio" for row in rows)
    assert all(row["source_kind"] == "securities_description" for row in rows)
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {(5, 1)}
    assert {row["symbol"] for row in rows} == {"TSM"}


def test_real_pt_item12d_does_not_treat_the_former_ratio_as_current():
    rows = parse_fixture("pt_2025_ratio_change")
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in ratios} == {(35, 1)}
    item12 = [row for row in ratios if row["source_kind"] == "item_12d"]
    assert item12
    assert {row["effective_from"] for row in item12} == {"2022-05-16"}
    assert {row["available_on"] for row in item12} == {"2025-04-18"}


def test_real_affirmative_ads_footnote_overrides_common_share_title():
    rows = parse_fixture("asx_2009_affirmative_ads")
    listings = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert listings and all(row["listed_type"] == "ads" for row in listings)
    assert "Traded" in listings[0]["evidence_text"]
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in ratios} == {(5, 1)}


def test_real_honda_table_enumerator_is_not_the_ads_denominator():
    rows = parse_fixture("hmc_2005_enumerated_cover")
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert ratios
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in ratios} == {(1, 2)}


@pytest.mark.parametrize("role", ["primary", "securities_description"])
def test_former_ratio_is_excluded_from_annual_and_attached_descriptions(role):
    content = """<h1>Description of securities registered under Section 12 of the Exchange Act</h1>
    <p>Securities registered pursuant to Section 12(b) of the Act:</p>
    <table><tr><th>Title of each class</th><th>Trading Symbol</th>
    <th>Name of each exchange on which registered</th></tr>
    <tr><td>American Depositary Shares, each representing 35 Class A ordinary shares</td>
    <td>PT</td><td>New York Stock Exchange</td></tr></table>
    <h1>TABLE OF CONTENTS</h1><h2>Item 12.D. American Depositary Shares</h2>
    <p>Effective May 16, 2022, we amended our ADS to Share ratio from one ADS
    representing seven (7) Class A ordinary shares to one ADS representing thirty-five (35) ordinary shares.</p>
    <h2>Item 13.</h2>"""
    rows = parse_filing(
        content, cik=1716338, form_type="20-F", accession_number="0001410578-25-000773",
        filing_date="2025-04-17", source_url="https://www.sec.gov/Archives/test",
        document_role=role,
    )
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert ratios
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in ratios} == {(35, 1)}


@pytest.mark.parametrize("name,numerator,denominator", [
    ("tsm_2007_f6", 5, 1),
    ("azn_2014_f6_ratio", 1, 1),
    ("azn_2015_f6_combined", 1, 2),
    ("azn_2015_6k_change", 1, 2),
    ("azn_2015_6k_announcement", 1, 2),
    ("azn_2017_20f_cover_combined", 1, 2),
])
def test_real_registered_and_corroborating_ratios(name, numerator, denominator):
    rows = [row for row in parse_fixture(name) if row["evidence_kind"] == "ads_ratio"]
    assert rows
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {(numerator, denominator)}
    # The legacy documents do not print a ticker beside the ratio. The caller's
    # candidate symbol is a search hint, not evidence for inventing a linkage.
    assert all(row["symbol"] is None for row in rows)


@pytest.mark.parametrize("name,available", [
    ("azn_2015_f6_combined", "2015-07-21"),
    ("azn_2015_6k_change", "2015-07-28"),
    ("azn_2015_6k_announcement", "2015-06-27"),
])
def test_real_azn_change_keeps_effective_and_public_dates_separate(name, available):
    rows = [row for row in parse_fixture(name) if row["evidence_kind"] == "ads_ratio"]
    assert rows
    assert {row["effective_from"] for row in rows} == {"2015-07-27"}
    assert {row["available_on"] for row in rows} == {available}


def test_candidate_ticker_words_in_titles_and_prose_are_not_symbol_evidence():
    html = """<p>Securities registered pursuant to Section 12(b) of the Act:</p>
    <table><tr><th>Title of each class</th><th>Trading Symbol(s)</th>
    <th>Name of each exchange on which registered</th></tr>
    <tr><td>Class A ordinary shares</td><td>XYZ</td>
    <td>New York Stock Exchange</td></tr></table>
    <p>ON and IT appear in this explanatory prose.</p>
    <p>Securities registered pursuant to Section 12(g): None.</p>"""
    rows = parse_filing(
        html, cik=123, form_type="20-F", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
        symbols=["A", "ON", "IT", "XYZ"],
    )
    listings = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert [(row["symbol"], row["listed_type"]) for row in listings] == [("XYZ", "ordinary_direct")]


def test_issuer_ratio_does_not_bind_to_candidate_ticker_used_as_ordinary_word():
    rows = parse_filing(
        "Each ADS represents five ordinary shares. ON BEHALF OF THE ISSUER.",
        cik=123, form_type="F-6", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
        symbols=["ON"],
    )
    assert rows and all(row["symbol"] is None for row in rows)


@pytest.mark.parametrize("quantity,numerator,denominator", [
    ("one and one-half", 3, 2),
    ("two and one quarter", 9, 4),
    ("one-half", 1, 2),
    ("two and half", 5, 2),
])
def test_mixed_number_ads_ratios_preserve_the_whole_and_fractional_parts(quantity, numerator, denominator):
    rows = parse_filing(
        f"Each ADS represents {quantity} ordinary shares.",
        cik=123, form_type="F-6", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
    )
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert len(ratios) == 1
    assert (ratios[0]["ratio_numerator"], ratios[0]["ratio_denominator"]) == (numerator, denominator)


def test_cover_dotted_symbol_is_normalized_without_rewriting_evidence():
    rows = parse_filing(
        """<p>Securities registered pursuant to Section 12(b) of the Act:</p>
        <table><tr><th>Title of each class</th><th>Trading Symbol</th>
        <th>Name of each exchange on which registered</th></tr>
        <tr><td>Common shares</td><td>AB.A</td><td>New York Stock Exchange</td></tr></table>""",
        cik=123, form_type="20-F", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AB-A"
    assert "AB.A" in rows[0]["evidence_text"]


def test_f6_dotted_trading_symbol_is_normalized_without_rewriting_evidence():
    rows = parse_filing(
        'Our ADSs trade on the NYSE under the symbol "AB.A". Each ADS represents five ordinary shares.',
        cik=123, form_type="F-6", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AB-A"
    assert "AB.A" in rows[0]["evidence_text"]


def test_f6_quoted_symbol_sentence_period_is_not_a_share_class_separator():
    rows = parse_filing(
        'Our ADSs trade on the NYSE under the symbol "LITB." Each ADS represents five ordinary shares.',
        cik=123, form_type="F-6", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "LITB"
    assert "LITB." in rows[0]["evidence_text"]


def test_cover_symbol_trailing_period_is_removed_without_losing_raw_evidence():
    rows = parse_filing(
        """<p>Securities registered pursuant to Section 12(b) of the Act:</p>
        <table><tr><th>Title of each class</th><th>Trading Symbol</th>
        <th>Name of each exchange on which registered</th></tr>
        <tr><td>Common shares</td><td>AHG.</td><td>New York Stock Exchange</td></tr></table>""",
        cik=123, form_type="20-F", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AHG"
    assert "AHG." in rows[0]["evidence_text"]


@pytest.mark.parametrize("name,expected", [
    ("aem_2005", "ordinary_direct"), ("ul_2005", "ads"),
    ("tlk_2008", "ads"), ("elp_2011", "ads"), ("eocc_2011", "ads"),
    ("mtu_2015", "ads"), ("cni_2016", "ordinary_direct"),
    ("iba_2017", "ads"),
])
def test_real_legacy_cover_layouts_retain_type_without_inventing_ticker(name, expected):
    rows = [row for row in parse_fixture(name) if row["evidence_kind"] == "listed_type"]
    assert rows
    assert {row["listed_type"] for row in rows} == {expected}
    assert all(row["symbol"] is None for row in rows)


@pytest.mark.parametrize("name,ratio", [
    ("ul_2005", (4, 1)), ("mtu_2015", (1, 1)),
    ("iba_2017", (12, 1)), ("kep_2023", (1, 2)),
])
def test_real_cover_ratio_wording_from_preflight(name, ratio):
    rows = [row for row in parse_fixture(name) if row["evidence_kind"] == "ads_ratio"]
    assert rows
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {ratio}


def test_ads_over_preferred_shares_is_ads_without_claiming_an_ordinary_ratio():
    rows = parse_fixture("elp_2011")
    assert any(row["listed_type"] == "ads" for row in rows)
    assert all(not row["ordinary_candidate"] for row in rows if row["evidence_kind"] == "listed_type")
    assert not any(row["evidence_kind"] == "ads_ratio" for row in rows)


def test_real_spaced_symbol_header_keeps_ads_and_non_equity_lines_separate():
    rows = parse_fixture("nwg_2022")
    listings = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert any(row["symbol"] == "RBS" and row["listed_type"] == "ads" for row in listings)
    assert all(row["listed_type"] == "unknown" for row in listings if row["symbol"] != "RBS")
    assert all(not row["ordinary_candidate"] for row in listings if row["symbol"] != "RBS")
    assert any(row["symbol"] == "RBSP1" for row in listings)
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert [(row["symbol"], row["ratio_numerator"], row["ratio_denominator"]) for row in ratios] == [("RBS", 2, 1)]


def test_real_global_depositary_shares_over_a_basket_are_not_an_ordinary_ratio():
    rows = parse_fixture("tv_2008")
    listings = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert len(listings) == 1
    assert listings[0]["listed_type"] == "ads"
    assert listings[0]["ordinary_candidate"] is False
    assert listings[0]["symbol"] is None
    assert not any(row["evidence_kind"] == "ads_ratio" for row in rows)


def test_real_abbreviated_voting_class_retains_unknown_audit_evidence():
    rows = parse_fixture("rci_2008")
    listings = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert len(listings) == 1
    assert listings[0]["listed_type"] == "unknown"
    assert listings[0]["ordinary_candidate"] is True
    assert listings[0]["underlying_class"] == "class_b"
    assert "Class B Non-Voting" in listings[0]["evidence_text"]


@pytest.mark.parametrize("title,expected_type,ordinary_candidate", [
    ("American Depositary Shares, each representing the right to receive five ordinary shares", "ads", True),
    ("Subscription Rights to ordinary shares", "unknown", False),
])
def test_receipt_right_to_receive_is_distinct_from_a_subscription_right(title, expected_type, ordinary_candidate):
    rows = parse_filing(
        f"""<p>Securities registered pursuant to Section 12(b) of the Act:</p>
        <table><tr><th>Title of each class</th><th>Trading Symbol</th>
        <th>Name of each exchange on which registered</th></tr>
        <tr><td>{title}</td><td>TEST</td><td>New York Stock Exchange</td></tr></table>""",
        cik=123, form_type="20-F", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
    )
    listing = next(row for row in rows if row["evidence_kind"] == "listed_type")
    assert listing["listed_type"] == expected_type
    assert listing["ordinary_candidate"] is ordinary_candidate


@pytest.fixture(scope="module")
def sql_database():
    dsn = os.environ.get("SEC_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SEC_TEST_DATABASE_URL not set (disposable local Postgres only)")
    psycopg = pytest.importorskip("psycopg")
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    if info.get("host") not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail("SEC_TEST_DATABASE_URL must use a disposable loopback database")
    if info.get("port") == "65432" or info.get("user") == "mcp_ro":
        pytest.fail("Refusing production connection for SQL tests")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute((ROOT / "schemas" / "sec_foreign_listing_evidence.sql").read_text(encoding="utf-8"))
        yield conn


@pytest.fixture
def db(sql_database):
    conn = sql_database
    conn.execute("BEGIN")
    conn.execute("TRUNCATE public.sec_foreign_listing_evidence")
    try:
        yield conn
    finally:
        conn.execute("ROLLBACK")


def add(db, *, kind="listed_type", listed_type="ads", source="cover_12b", ratio=None,
        filed="2020-01-01", effective=None, available=None, retired=None, until=None,
        symbol="TSM", underlying_class=None, ordinary_candidate=True):
    from psycopg import sql
    tomorrow = (dt.date.fromisoformat(filed) + dt.timedelta(days=1)).isoformat()
    row = dict(
        fact_hash=uuid4().hex, cik=1046179, symbol=symbol, underlying_class=underlying_class,
        ordinary_candidate=ordinary_candidate,
        adsh="0001193125-20-000001", form="20-F", filed=filed,
        source_url="https://www.sec.gov/Archives/edgar/data/test",
        source_sha256="a" * 64, source_kind=source, evidence_kind=kind,
        listed_type=listed_type if kind == "listed_type" else None,
        ratio_numerator=ratio[0] if ratio else None,
        ratio_denominator=ratio[1] if ratio else None,
        effective_from=effective or tomorrow, effective_to=until,
        evidence_text="Synthetic resolver assertion; parser fixtures are separate.",
        evidence_location="test", parser_version="test-v1",
        available_on=available or tomorrow, retired_on=retired,
        loaded_on="2026-10-09", source_package=uuid4().hex,
    )
    stmt = sql.SQL("INSERT INTO public.sec_foreign_listing_evidence ({}) VALUES ({}) RETURNING id").format(
        sql.SQL(",").join(map(sql.Identifier, row)),
        sql.SQL(",").join(sql.Placeholder() for _ in row),
    )
    return db.execute(stmt, list(row.values())).fetchone()[0]


def add_ratio(db, ratio=(5, 1), source="f6", **kwargs):
    return add(db, kind="ads_ratio", ratio=ratio, source=source, **kwargs)


def insert_real_parsed_rows(db, name):
    from psycopg import sql
    ids = []
    for parsed in parse_fixture(name):
        row = {**parsed, "fact_hash": uuid4().hex, "loaded_on": "2026-10-09", "source_package": name}
        stmt = sql.SQL("INSERT INTO public.sec_foreign_listing_evidence ({}) VALUES ({}) RETURNING id").format(
            sql.SQL(",").join(map(sql.Identifier, row)),
            sql.SQL(",").join(sql.Placeholder() for _ in row),
        )
        ids.append(db.execute(stmt, list(row.values())).fetchone()[0])
    return ids


def resolve(db, day):
    row = db.execute("SELECT * FROM public.sec_foreign_listing_at(1046179, 'TSM', %s::date)", [day]).fetchone()
    assert row is not None
    return row


def test_no_evidence_has_one_none_result(db):
    assert resolve(db, "2020-01-02") == ("none", None, None, None, "none", "none", [])


def test_real_tsm_cover_f6_and_attached_description_resolve_five_shares_per_ads(db):
    insert_real_parsed_rows(db, "tsm")
    insert_real_parsed_rows(db, "tsm_2007_f6")
    assert resolve(db, "2025-04-18")[0] == "none"
    insert_real_parsed_rows(db, "tsm_2024_securities_description")
    assert resolve(db, "2025-04-17")[0] == "none"
    assert resolve(db, "2025-04-18")[:6] == ("resolved", "ads", 5, 1, "resolved", "resolved")


def test_real_description_exhibit_alone_cannot_create_a_listed_line(db):
    insert_real_parsed_rows(db, "tsm_2024_securities_description")
    assert resolve(db, "2025-04-18")[:5] == ("none", None, None, None, "none")


def test_unbound_description_ratio_cannot_infer_its_symbol_from_later_cover(db):
    add(db)
    add_ratio(db, symbol=None)
    add_ratio(db, source="securities_description", symbol=None)
    assert resolve(db, "2020-01-02")[0] == "none"
    add_ratio(db, source="securities_description", symbol="TSM")
    assert resolve(db, "2020-01-02")[:4] == ("resolved", "ads", 5, 1)


def test_direct_listing_identity_and_filing_plus_one(db):
    add(db, listed_type="ordinary_direct")
    assert resolve(db, "2020-01-01")[0] == "none"
    assert resolve(db, "2020-01-02")[:6] == ("resolved", "ordinary_direct", 1, 1, "resolved", "resolved")


def test_ads_requires_f6_and_independent_cover_corroboration(db):
    add(db)
    add_ratio(db)
    assert resolve(db, "2020-01-02")[:6] == ("none", "ads", None, None, "resolved", "none")
    add_ratio(db, source="item_12d")
    assert resolve(db, "2020-01-02")[:6] == ("resolved", "ads", 5, 1, "resolved", "resolved")


def test_fractional_ratio_is_exact_and_equivalent_fractions_agree(db):
    add(db)
    add_ratio(db, (1, 2))
    add_ratio(db, (5, 10), source="cover_footnote")
    assert resolve(db, "2020-01-02")[:4] == ("resolved", "ads", 1, 2)


def test_conflicting_types_at_same_effective_date_never_pick_one(db):
    add(db)
    add(db, listed_type="ordinary_direct")
    assert resolve(db, "2020-01-02")[:5] == ("ambiguous", None, None, None, "ambiguous")


def test_conflicting_ratios_never_pick_source_priority(db):
    add(db)
    add_ratio(db)
    add_ratio(db, (1, 1), source="item_12d")
    assert resolve(db, "2020-01-02")[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")


def test_conflicting_same_date_f6_assertions_are_retained(db):
    add(db)
    add_ratio(db)
    add_ratio(db, (1, 1))
    add_ratio(db, source="item_12d")
    assert resolve(db, "2020-01-02")[0] == "ambiguous"


def test_late_correction_preserves_the_old_answer_before_retirement(db):
    old = add(db, listed_type="ordinary_direct", retired="2025-01-01")
    new = add(db, available="2025-01-01")
    assert resolve(db, "2024-12-31")[1] == "ordinary_direct"
    assert resolve(db, "2024-12-31")[-1] == [old]
    assert resolve(db, "2025-01-01")[1] == "ads"
    assert resolve(db, "2025-01-01")[-1] == [new]


def test_future_ratio_change_does_not_leak_then_supersedes_old_regime(db):
    add(db)
    add_ratio(db)
    add_ratio(db, source="item_12d")
    add_ratio(db, (1, 2), filed="2020-06-01", effective="2020-07-01")
    add_ratio(db, (1, 2), source="cover_footnote", filed="2020-06-01", effective="2020-07-01")
    add_ratio(db, (1, 2), source="ratio_change_6k", filed="2020-06-01", effective="2020-07-01")
    assert resolve(db, "2020-06-30")[:4] == ("resolved", "ads", 5, 1)
    assert resolve(db, "2020-07-01")[:4] == ("resolved", "ads", 1, 2)


def test_later_filing_retrospective_ratio_supersedes_older_filing_assertion(db):
    add(db)
    add_ratio(db, (7, 1), filed="2023-01-01")
    add_ratio(db, (7, 1), source="item_12d", filed="2024-01-01")
    add_ratio(db, (35, 1), source="item_12d", filed="2025-01-01", effective="2022-05-16")
    assert resolve(db, "2024-12-31")[:4] == ("resolved", "ads", 7, 1)
    assert resolve(db, "2025-01-02")[0] == "ambiguous"
    add_ratio(db, (35, 1), filed="2025-01-02", effective="2022-05-16")
    assert resolve(db, "2025-01-03")[:4] == ("resolved", "ads", 35, 1)


def test_later_retrospective_conflict_is_not_discarded_by_an_earlier_change_event(db):
    add(db)
    add_ratio(db, (5, 1), filed="2023-01-01")
    add_ratio(db, (5, 1), source="item_12d", filed="2023-01-01")
    add_ratio(db, (5, 1), source="ratio_change_6k", filed="2023-01-01")
    add_ratio(db, (35, 1), source="item_12d", filed="2025-01-01", effective="2022-05-16")
    assert resolve(db, "2024-12-31")[:4] == ("resolved", "ads", 5, 1)
    assert resolve(db, "2025-01-02")[0] == "ambiguous"


def test_announced_change_is_invisible_until_available(db):
    add(db)
    add_ratio(db)
    add_ratio(db, source="item_12d")
    add_ratio(db, (1, 2), source="ratio_change_6k", filed="2020-07-10", effective="2020-07-01")
    assert resolve(db, "2020-07-10")[:4] == ("resolved", "ads", 5, 1)
    assert resolve(db, "2020-07-11")[0] == "none"


def test_ratio_change_can_use_latest_matching_preregistration(db):
    add(db)
    add_ratio(db, (1, 2), filed="2020-06-01")
    add_ratio(db, (1, 2), source="item_12d", filed="2020-06-01")
    add_ratio(db, (1, 2), source="ratio_change_6k", filed="2020-06-15", effective="2020-07-01")
    assert resolve(db, "2020-07-01")[:4] == ("resolved", "ads", 1, 2)


def test_change_does_not_resurrect_superseded_matching_registration(db):
    add(db)
    add_ratio(db, (5, 1))
    add_ratio(db, (1, 1), filed="2020-02-01")
    add_ratio(db, (5, 1), source="item_12d", filed="2020-03-01")
    add_ratio(db, (5, 1), source="ratio_change_6k", filed="2020-03-01")
    assert resolve(db, "2020-03-02")[0] == "none"


def test_expired_latest_type_does_not_resurrect_older_open_type(db):
    add(db, listed_type="ordinary_direct")
    add(db, filed="2020-02-01", until="2020-03-01")
    assert resolve(db, "2020-02-29")[1] == "ads"
    assert resolve(db, "2020-03-01")[0] == "none"


def test_expired_latest_ratio_does_not_resurrect_older_open_ratio(db):
    add(db)
    add_ratio(db)
    add_ratio(db, (1, 2), filed="2020-02-01", until="2020-03-01")
    add_ratio(db, (5, 1), source="item_12d")
    assert resolve(db, "2020-03-01")[0] == "none"


def test_unknown_latest_type_supersedes_old_direct_type(db):
    add(db, listed_type="ordinary_direct")
    add(db, listed_type="unknown", filed="2020-02-01")
    assert resolve(db, "2020-02-02")[0] == "none"


def test_issuer_f6_ratio_binds_only_the_single_public_ads_line(db):
    add(db)
    add_ratio(db, symbol=None)
    add_ratio(db, source="item_12d")
    assert resolve(db, "2020-01-02")[:4] == ("resolved", "ads", 5, 1)


@pytest.mark.parametrize("other_type,other_symbol", [
    ("ads", "OTHER"), ("ads", None), ("unknown", "OTHER"),
])
def test_multiple_or_uncertain_ads_lines_prevent_issuer_ratio_binding(db, other_type, other_symbol):
    add(db)
    add(db, listed_type=other_type, symbol=other_symbol)
    add_ratio(db, symbol=None)
    add_ratio(db, source="item_12d")
    assert resolve(db, "2020-01-02")[0] == "none"


def test_future_cover_symbol_does_not_backfill_an_issuer_ratio(db):
    add(db, filed="2025-01-01")
    add_ratio(db, symbol=None)
    add_ratio(db, source="item_12d", filed="2025-01-01")
    assert resolve(db, "2024-12-31")[0] == "none"
    assert resolve(db, "2025-01-02")[:4] == ("resolved", "ads", 5, 1)


def test_symbol_omitted_by_a_new_complete_cover_is_not_carried_forward(db):
    add(db, listed_type="ordinary_direct")
    add(db, listed_type="ordinary_direct", symbol="NEW", filed="2025-01-01")
    assert resolve(db, "2024-12-31")[0] == "resolved"
    assert resolve(db, "2025-01-02")[0] == "none"


def test_symbol_aliases_preserve_all_competing_evidence(db):
    add(db, listed_type="ordinary_direct", symbol="BF-B")
    before = db.execute("SELECT status FROM public.sec_foreign_listing_at(1046179,'BF.B','2020-01-02')").fetchone()
    assert before == ("resolved",)
    add(db, listed_type="ads", symbol="BFB")
    after = db.execute("SELECT status FROM public.sec_foreign_listing_at(1046179,'BF.B','2020-01-02')").fetchone()
    assert after == ("ambiguous",)


def test_issuer_ratios_for_different_classes_do_not_become_false_corroboration(db):
    add(db, underlying_class="class_a")
    add_ratio(db, symbol=None, underlying_class="class_b")
    add_ratio(db, source="item_12d", underlying_class="class_a")
    assert resolve(db, "2020-01-02")[0] == "none"
    add_ratio(db, symbol=None, underlying_class="class_a")
    assert resolve(db, "2020-01-02")[:4] == ("resolved", "ads", 5, 1)


def test_unknown_cover_class_and_two_f6_programs_are_ambiguous_even_at_equal_ratio(db):
    add(db)
    add_ratio(db, symbol=None, underlying_class="class_a")
    add_ratio(db, symbol=None, underlying_class="class_b")
    add_ratio(db, source="item_12d")
    assert resolve(db, "2020-01-02")[0] == "ambiguous"


def test_explicit_symbol_class_conflict_is_ambiguous_even_at_equal_ratio(db):
    add(db, underlying_class="class_a")
    add_ratio(db, underlying_class="class_b")
    add_ratio(db, source="item_12d", underlying_class="class_a")
    assert resolve(db, "2020-01-02")[0] == "ambiguous"


def test_same_symbol_cover_with_conflicting_underlying_classes_is_ambiguous(db):
    add(db, underlying_class="class_a")
    add(db, underlying_class="class_b")
    assert resolve(db, "2020-01-02")[0] == "ambiguous"


def test_non_equity_notes_do_not_block_unique_ordinary_ads_program(db):
    add(db)
    add(db, listed_type="unknown", symbol="NOTE", ordinary_candidate=False)
    add_ratio(db, symbol=None)
    add_ratio(db, source="item_12d")
    assert resolve(db, "2020-01-02")[:4] == ("resolved", "ads", 5, 1)


def test_preferred_ads_do_not_block_a_separate_ordinary_ads_program(db):
    add(db)
    add(db, symbol="PREF", ordinary_candidate=False)
    add_ratio(db, symbol=None)
    add_ratio(db, source="item_12d")
    assert resolve(db, "2020-01-02")[:4] == ("resolved", "ads", 5, 1)


def test_preferred_ads_cannot_consume_an_ordinary_issuer_ratio(db):
    add(db, ordinary_candidate=False)
    add_ratio(db, symbol=None)
    add_ratio(db, source="item_12d")
    assert resolve(db, "2020-01-02")[0] == "none"
