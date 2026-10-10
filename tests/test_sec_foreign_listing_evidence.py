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


@pytest.mark.parametrize("form", ["20FR12G", "20FR12G/A"])
def test_discovered_20fr12g_securities_description_is_ratio_corroboration_only(form):
    from scripts.load_sec_foreign_listing_evidence import securities_description_exhibits

    url = "https://www.sec.gov/Archives/edgar/data/1046179/000119312524083790/exhibit2a1.htm"
    filing = {"formType": form, "documentFormatFiles": [{
        "type": "EX-2.A.1", "description": "Description of securities", "documentUrl": url,
    }]}
    attachment, = securities_description_exhibits(filing)
    # A real description can also include a securities table. Its role must
    # still prevent it from establishing an exchange-listed security itself.
    content = """<p>Securities registered pursuant to Section 12(b)</p>
    <table><tr><th>Title of each class</th><th>Trading symbol</th><th>Exchange</th></tr>
    <tr><td>American Depositary Shares, each representing five common shares</td>
    <td>TSM</td><td>New York Stock Exchange</td></tr></table>"""
    kwargs = {"cik": 1046179, "form_type": attachment["formType"],
              "accession_number": "0001193125-24-083790", "filing_date": "2024-03-28",
              "source_url": attachment["filingUrl"], "symbols": ["TSM"]}
    rows = parse_filing(content, document_role=attachment["document_role"], **kwargs)
    assert rows
    assert {row["evidence_kind"] for row in rows} == {"ads_ratio"}
    assert {row["source_kind"] for row in rows} == {"securities_description"}
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {(5, 1)}
    assert {row["form"] for row in rows} == {form}
    assert {row["available_on"] for row in rows} == {"2024-03-29"}
    assert all(row["evidence_location"].startswith("exhibit/securities-description;") for row in rows)
    # Primary 12(g) registration parsing keeps its existing scope.
    assert parse_filing(content, **kwargs) == []


def test_real_pt_item12d_does_not_treat_the_former_ratio_as_current():
    rows = parse_fixture("pt_2025_ratio_change")
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in ratios} == {(35, 1)}
    item12 = [row for row in ratios if row["source_kind"] == "item_12d"]
    assert item12
    assert {row["effective_from"] for row in item12} == {"2022-05-16"}
    assert {row["available_on"] for row in item12} == {"2025-04-18"}


def test_real_tour_cross_reference_does_not_relabel_item9_as_item12d():
    rows = parse_fixture("tour_2025_item12_crossreference")
    assert any(row["listed_type"] == "ads" for row in rows)
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert ratios
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in ratios} == {(3, 1)}
    assert {row["source_kind"] for row in ratios} == {"cover_footnote"}
    assert not any(row["source_kind"] == "item_12d" for row in rows)


def test_real_anpc_placeholder_amendment_is_distinguished_from_explicit_announcement():
    amendment = parse_fixture("anpc_2022_f6_undated_amendment_full")
    announcement = parse_fixture("anpc_2022_change_announcement")
    assert amendment and announcement
    assert all(row["effective_date_explicit"] is False for row in amendment)
    assert {row["effective_from"] for row in amendment} == {"2022-10-25"}
    assert all(row["effective_date_explicit"] is True for row in announcement)
    assert {row["effective_from"] for row in announcement} == {"2022-11-04"}
    assert all(row.get("ratio_effectiveness_pending") is True for row in announcement)
    assert all(row["ratio_effectiveness_conditions"] == ["unknown_condition"] for row in announcement)


def test_real_otly_market_open_amendment_has_an_explicit_later_effective_date():
    rows = parse_fixture("otly_2025_market_open_amendment_full")
    assert rows
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {(20, 1)}
    assert {row["effective_from"] for row in rows} == {"2025-02-18"}
    assert {row["available_on"] for row in rows} == {"2025-02-13"}
    assert all(row["effective_date_explicit"] is True for row in rows)


def test_real_otly_cover_and_listing_description_keep_the_type_conflict():
    rows = parse_fixture("otly_2024_cover_and_listing_description")
    types = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert any(row["listed_type"] == "ordinary_direct" and row["source_kind"] == "cover_12b" for row in types)
    descriptions = [row for row in types if row["source_kind"] == "listing_description"]
    assert descriptions and all(row["listed_type"] == "ads" and row["symbol"] == "OTLY" for row in descriptions)
    assert all(row["effective_from"] == "2024-03-23" for row in types)
    assert not any(row["evidence_kind"] == "ads_ratio" for row in rows)


def test_terminated_historical_ads_description_does_not_conflict_with_current_cover():
    rows = parse_filing(
        """<p>Securities registered pursuant to Section 12(b) of the Act:</p>
        <table><tr><th>Title of each class</th><th>Trading Symbol</th>
        <th>Name of each exchange on which registered</th></tr>
        <tr><td>Ordinary Shares</td><td>OTLY</td><td>Nasdaq</td></tr></table>
        <h2>Item 9. The Offer and Listing</h2><h3>C. Markets</h3>
        <p>Our ADSs previously traded on Nasdaq under the symbol "OTLY", but were delisted on May 20, 2021.
        That depositary program was terminated before our ordinary shares began trading.</p>
        <h2>Item 10. Additional Information</h2>""",
        cik=1843586, form_type="20-F", accession_number="0000950170-24-035039",
        filing_date="2024-03-22", source_url="https://www.sec.gov/Archives/test",
    )
    assert not any(row["source_kind"] == "listing_description" for row in rows)
    assert {row["listed_type"] for row in rows if row["evidence_kind"] == "listed_type"} == {"ordinary_direct"}


@pytest.mark.parametrize("effective_clause", [
    "effective as of the open of trading of the ADSs on The Nasdaq Global Select Market on February 18, 2025",
    "effective at the close of trading on February 18, 2025",
    "effective as of the beginning of trading on February 18, 2025",
    "effective at the open of business on February 18, 2025",
    "effective as of the close of business on February 18, 2025",
])
def test_market_session_effective_date_phrases_are_explicit(effective_clause):
    rows = parse_filing(
        f"Each ADS represents twenty ordinary shares, {effective_clause}.",
        cik=1843586, form_type="F-6 POS", accession_number="0001104659-25-012180",
        filing_date="2025-02-12", source_url="https://www.sec.gov/Archives/test",
    )
    assert rows
    assert {row["effective_from"] for row in rows} == {"2025-02-18"}
    assert all(row["effective_date_explicit"] is True for row in rows)


@pytest.mark.parametrize("date_text", [
    "On February 18, 2025, we announced the ratio change.",
    "We announced a change in the ADS ratio on February 18, 2025.",
    "The ADSs began trading on February 18, 2025.",
])
def test_announcement_and_trading_history_dates_are_not_ratio_effective_dates(date_text):
    rows = parse_filing(
        f"{date_text} Each ADS represents twenty ordinary shares.",
        cik=1843586, form_type="F-6 POS", accession_number="0001104659-25-012180",
        filing_date="2025-02-12", source_url="https://www.sec.gov/Archives/test",
    )
    assert rows
    assert {row["effective_from"] for row in rows} == {"2025-02-13"}
    assert all(row["effective_date_explicit"] is False for row in rows)


def test_conflicting_market_session_effective_dates_do_not_choose_one():
    rows = parse_filing(
        "Each ADS represents twenty ordinary shares, effective as of the open of trading on February 18, 2025. "
        "The ratio change is effective at the close of trading on February 19, 2025.",
        cik=1843586, form_type="F-6 POS", accession_number="0001104659-25-012180",
        filing_date="2025-02-12", source_url="https://www.sec.gov/Archives/test",
    )
    assert rows
    assert all(row["effective_date_explicit"] is True for row in rows)
    assert {row["effective_from"] for row in rows} == {"2025-02-18"}
    assert all(row["operative_date_conflict"] is True for row in rows)
    assert {tuple(row["operative_date_candidates"]) for row in rows} == {("2025-02-18", "2025-02-19")}
    assert all("February 18" in row["operative_date_conflict_text"]
               and "February 19" in row["operative_date_conflict_text"] for row in rows)


def test_real_affirmative_ads_footnote_overrides_common_share_title():
    rows = parse_fixture("asx_2009_affirmative_ads")
    listings = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert listings and all(row["listed_type"] == "ads" for row in listings)
    assert "Traded" in listings[0]["evidence_text"]
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in ratios} == {(5, 1)}


@pytest.mark.parametrize("name,symbol,numerator", [
    ("ec_2021_depository", "EC", 20),
    ("gsk_2024_america_depositary", "GSK", 2),
])
def test_real_depository_and_america_spellings_share_type_and_ratio_recognition(name, symbol, numerator):
    rows = parse_fixture(name)
    listings = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert any(row["symbol"] == symbol and row["listed_type"] == "ads" for row in listings)
    assert all(row["listed_type"] == "unknown" and not row["ordinary_candidate"]
               for row in listings if row["symbol"] != symbol)
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert [(row["symbol"], row["ratio_numerator"], row["ratio_denominator"]) for row in ratios] == [(symbol, numerator, 1)]


def test_real_exchange_cell_depositary_qualifier_overrides_ordinary_title():
    rows = parse_fixture("ixhl_2022_exchange_wrapper")
    listings = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert [(row["symbol"], row["listed_type"]) for row in listings] == [("IXHL", "ads")]
    assert "in connection with the listing for trading of American Depositary Shares" in listings[0]["evidence_text"]
    assert not any(row["evidence_kind"] == "ads_ratio" for row in rows)


def test_real_parenthesized_exchange_depositary_description_overrides_ordinary_title():
    rows = parse_fixture("immp_2014_parenthesized_exchange")
    listings = [row for row in rows if row["evidence_kind"] == "listed_type"]
    assert len(listings) == 1
    assert listings[0]["listed_type"] == "ads"
    assert listings[0]["symbol"] is None
    assert "(American Depositary Shares representing Ordinary Shares)" in listings[0]["evidence_text"]
    assert not any(row["evidence_kind"] == "ads_ratio" for row in rows)


def test_not_for_trading_footnote_without_depositary_evidence_stays_unknown():
    rows = parse_filing(
        """<p>Securities registered pursuant to Section 12(b) of the Act:</p>
        <table><tr><th>Title of each class</th><th>Trading Symbol</th>
        <th>Name of each exchange on which registered</th></tr>
        <tr><td>Class A ordinary shares*</td><td>ONE</td><td>New York Stock Exchange</td></tr>
        <tr><td>Class B ordinary shares</td><td>TWO</td><td>New York Stock Exchange</td></tr></table>
        <p>* Not for trading, but only for registration purposes.</p>""",
        cik=123, form_type="20-F", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
    )
    listings = {row["symbol"]: row["listed_type"] for row in rows if row["evidence_kind"] == "listed_type"}
    assert listings == {"ONE": "unknown", "TWO": "ordinary_direct"}


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


def test_full_azn_operative_amendment_does_not_activate_new_ratio_before_effective_date():
    rows = [row for row in parse_fixture("azn_2015_amendment_full") if row["evidence_kind"] == "ads_ratio"]
    new = [row for row in rows if (row["ratio_numerator"], row["ratio_denominator"]) == (1, 2)]
    assert new
    assert {row["effective_from"] for row in new} == {"2015-07-27"}
    assert {row["available_on"] for row in new} == {"2015-07-21"}
    # Deleted or quoted old terms may be retained only as a bounded old regime.
    old = [row for row in rows if (row["ratio_numerator"], row["ratio_denominator"]) == (1, 1)]
    assert all(row["effective_to"] == "2015-07-27" for row in old)
    assert not any(row["effective_from"] == "2015-07-27" for row in old)


def test_full_azn_legacy_agreement_is_old_ratio_evidence_not_the_amended_ratio():
    rows = [row for row in parse_fixture("azn_2015_legacy_agreement_full") if row["evidence_kind"] == "ads_ratio"]
    assert rows
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {(1, 1)}


def test_azn_parent_registration_has_no_ratio_to_substitute_for_its_exhibits():
    assert not parse_fixture("azn_2015_f6_parent_full")


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


@pytest.mark.parametrize("name,title,exchange", [
    ("fsv_2015_40fr12b", "Subordinate Voting Shares", "NASDAQ Stock Market"),
    ("stn_2005_40fr12b", "COMMON SHARES", "NEW YORK STOCK EXCHANGE"),
    ("qtrrf_2008_40fr12b", "Common Shares, no par value", "American Stock Exchange"),
])
def test_real_40fr12b_legacy_covers_retain_direct_type_without_current_symbol(name, title, exchange):
    meta = json.loads((FIXTURES / (name + ".json")).read_text(encoding="utf-8"))
    payload = (FIXTURES / meta["fixture"]).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == meta["sha256"] == meta["original_source_sha256"]
    assert b"\r\n" not in payload
    rows = parse_filing(
        payload.decode("utf-8"), symbols=[meta["symbol"]],
        **{key: meta[key] for key in ("cik", "form_type", "accession_number", "filing_date", "source_url")},
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["evidence_kind"] == "listed_type" and row["source_kind"] == "cover_12b"
    assert row["listed_type"] == "ordinary_direct" and row["ordinary_candidate"] is True
    assert row["symbol"] is None
    assert row["ratio_numerator"] is row["ratio_denominator"] is None
    assert title in row["evidence_text"] and exchange in row["evidence_text"]
    tomorrow = (dt.date.fromisoformat(meta["filing_date"]) + dt.timedelta(days=1)).isoformat()
    assert row["effective_from"] == row["available_on"] == tomorrow
    assert row["source_url"] == meta["source_url"] and row["source_sha256"] == meta["sha256"]


@pytest.mark.parametrize("body", [
    "Common Shares NEW YORK STOCK EXCHANGE Common Shares NEW YORK STOCK EXCHANGE",
    "Common Shares NEW YORK STOCK EXCHANGE Preferred Shares NEW YORK STOCK EXCHANGE",
    "Subscription Rights to Common Shares NEW YORK STOCK EXCHANGE",
    "Common Shares not for trading NEW YORK STOCK EXCHANGE",
    "Common Shares",
    "Class A Common Shares NEW YORK STOCK EXCHANGE Class B Common Shares NEW YORK STOCK EXCHANGE",
])
def test_positioned_ordinary_cover_requires_one_unqualified_equity_line(body):
    content = ("Securities registered pursuant to Section 12(b) of the Act: "
               "Title of each class Name of each exchange on which registered " + body +
               " Securities registered pursuant to Section 12(g): None")
    rows = parse_filing(
        content, cik=123, form_type="40FR12B", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test", symbols=["TODAY"],
    )
    assert not any(row["listed_type"] == "ordinary_direct" for row in rows)
    assert all(row["symbol"] is None for row in rows)


def test_positioned_common_share_prose_without_cover_column_headings_is_not_listing_evidence():
    rows = parse_filing(
        "Securities registered pursuant to Section 12(b): Common Shares NEW YORK STOCK EXCHANGE "
        "Securities registered pursuant to Section 12(g): None",
        cik=123, form_type="40FR12B", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test", symbols=["TODAY"],
    )
    assert not rows


def test_legacy_voting_cover_does_not_choose_between_two_unbound_equity_classes():
    rows = parse_filing(
        """<p>Securities registered pursuant to Section 12(b):</p>
        <table><tr><th>Title of each class</th><th>Name of each exchange on which registered</th></tr>
        <tr><td>Subordinate Voting Shares</td><td>NASDAQ Stock Market</td></tr>
        <tr><td>Variable Voting Shares</td><td>NASDAQ Stock Market</td></tr></table>
        <p>Securities registered pursuant to Section 12(g): None</p>""",
        cik=123, form_type="40FR12B", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test", symbols=["TODAY"],
    )
    assert not rows


def test_historical_american_exchange_does_not_make_a_preference_line_ordinary():
    rows = parse_filing(
        """<p>Securities registered pursuant to Section 12(b):</p>
        <table><tr><th>Title of each class</th><th>Trading Symbol</th>
        <th>Name of each exchange on which registered</th></tr>
        <tr><td>Series A Preferred Shares</td><td>PREF</td><td>American Stock Exchange</td></tr></table>""",
        cik=123, form_type="40FR12B", accession_number="0000000123-20-000001",
        filing_date="2020-01-01", source_url="https://www.sec.gov/Archives/test",
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "PREF" and rows[0]["listed_type"] == "unknown"
    assert rows[0]["ordinary_candidate"] is False


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
        symbol="TSM", underlying_class=None, ordinary_candidate=True, cik=1046179,
        publication_floor_on=None, effective_date_explicit=None,
        adsh="0001193125-20-000001", form="20-F",
        ratio_change_program_key=None, ratio_change_correction_kind=None,
        ratio_change_correction_text=None, ratio_effectiveness_pending=None,
        ratio_effectiveness_pending_text=None, operative_date_conflict=None,
        operative_date_candidates=None, operative_date_conflict_text=None,
        ratio_effectiveness_confirmed=None, ratio_effectiveness_confirmation_text=None,
        ratio_effectiveness_conditions=None, ratio_effectiveness_confirmed_conditions=None):
    from psycopg import sql
    tomorrow = (dt.date.fromisoformat(filed) + dt.timedelta(days=1)).isoformat()
    row = dict(
        fact_hash=uuid4().hex, cik=cik, symbol=symbol, underlying_class=underlying_class,
        ordinary_candidate=ordinary_candidate,
        publication_floor_on=publication_floor_on,
        effective_date_explicit=(effective is not None) if effective_date_explicit is None else effective_date_explicit,
        adsh=adsh, form=form, filed=filed,
        ratio_change_program_key=ratio_change_program_key,
        ratio_change_correction_kind=ratio_change_correction_kind,
        ratio_change_correction_text=ratio_change_correction_text,
        ratio_effectiveness_pending=ratio_effectiveness_pending,
        ratio_effectiveness_pending_text=ratio_effectiveness_pending_text,
        operative_date_conflict=operative_date_conflict,
        operative_date_candidates=operative_date_candidates,
        operative_date_conflict_text=operative_date_conflict_text,
        ratio_effectiveness_confirmed=ratio_effectiveness_confirmed,
        ratio_effectiveness_confirmation_text=ratio_effectiveness_confirmation_text,
        ratio_effectiveness_conditions=ratio_effectiveness_conditions,
        ratio_effectiveness_confirmed_conditions=ratio_effectiveness_confirmed_conditions,
        source_url="https://www.sec.gov/Archives/edgar/data/test",
        source_sha256="a" * 64, source_kind=source, evidence_kind=kind,
        listed_type=listed_type if kind == "listed_type" else None,
        ratio_numerator=ratio[0] if ratio else None,
        ratio_denominator=ratio[1] if ratio else None,
        effective_from=effective or tomorrow, effective_to=until,
        evidence_text="Synthetic resolver assertion; parser fixtures are separate.",
        evidence_location="test", parser_version="test-v1",
        available_on=available or max(tomorrow, publication_floor_on or tomorrow), retired_on=retired,
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


def resolve(db, day, *, cik=1046179, symbol="TSM"):
    row = db.execute("SELECT * FROM public.sec_foreign_listing_at(%s, %s, %s::date)", [cik, symbol, day]).fetchone()
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


def test_real_otly_contemporaneous_listing_sources_resolve_as_ambiguous(db):
    insert_real_parsed_rows(db, "otly_2024_cover_and_listing_description")
    result = resolve(db, "2024-03-23", cik=1843586, symbol="OTLY")
    assert result[0] == "ambiguous" and result[1] is None
    assert result[4] == "ambiguous"


def test_listing_description_is_not_an_accepted_ratio_source(db):
    import psycopg
    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction():
            add_ratio(db, source="listing_description")


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


@pytest.mark.parametrize("filed,floor,before", [
    ("2018-06-08", "2022-11-09", "2020-12-31"),
    ("2020-07-10", "2023-07-26", "2020-12-31"),
])
def test_backdated_replacement_cannot_be_seen_before_its_reported_publication(db, filed, floor, before):
    # Real VOD/Pemex date pairs; the listed security assertion is synthetic.
    add(db, listed_type="ordinary_direct", filed=filed, publication_floor_on=floor)
    assert resolve(db, before)[0] == "none"
    assert resolve(db, (dt.date.fromisoformat(floor) - dt.timedelta(days=1)).isoformat())[0] == "none"
    assert resolve(db, floor)[:4] == ("resolved", "ordinary_direct", 1, 1)
    source = db.execute("SELECT effective_from,source_available_on,available_on FROM public.sec_foreign_listing_evidence").fetchone()
    assert source == (dt.date.fromisoformat(filed) + dt.timedelta(days=1), dt.date.fromisoformat(floor), dt.date.fromisoformat(floor))


def test_publication_on_filing_plus_one_does_not_add_an_extra_day(db):
    add(db, listed_type="ordinary_direct", filed="2024-03-25", publication_floor_on="2024-03-26")
    assert resolve(db, "2024-03-25")[0] == "none"
    assert resolve(db, "2024-03-26")[0] == "resolved"


def test_republished_old_annual_does_not_displace_a_newer_legal_filing(db):
    add(db, filed="2021-01-01")
    add_ratio(db, (5, 1), filed="2021-01-01")
    add_ratio(db, (5, 1), source="item_12d", filed="2021-01-01")
    add_ratio(db, (1, 1), source="item_12d", filed="2018-06-08", publication_floor_on="2022-11-09")
    assert resolve(db, "2022-11-08")[:4] == ("resolved", "ads", 5, 1)
    assert resolve(db, "2022-11-09")[:4] == ("resolved", "ads", 5, 1)


def test_later_publication_of_pre_change_assertion_does_not_create_false_conflict(db):
    add(db, filed="2019-01-01")
    add_ratio(db, (5, 1), filed="2020-01-01")
    add_ratio(db, (5, 1), source="ratio_change_6k", filed="2020-01-01")
    add_ratio(db, (1, 1), source="item_12d", filed="2018-06-08", publication_floor_on="2022-11-09")
    assert resolve(db, "2022-11-09")[:4] == ("resolved", "ads", 5, 1)


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


def _dated_ratio_notice(db, ratio, *, filed="2022-11-17", effective="2022-11-28",
                        adsh="0001140361-22-042069", correction=False,
                        program="old_cusip:98417P105", **kwargs):
    return add_ratio(
        db, ratio, source="ratio_change_6k", filed=filed, effective=effective,
        adsh=adsh, form="6-K/A" if correction else "6-K", symbol=None,
        ratio_change_program_key=program,
        ratio_change_correction_kind="correcting_and_replacing" if correction else None,
        ratio_change_correction_text="CORRECTING and REPLACING ADR Ratio Change" if correction else None,
        **kwargs,
    )


def _pending_ratio_registration(db, ratio, *, filed="2022-11-15",
                                adsh="0001193805-22-001555", **kwargs):
    return add_ratio(
        db, ratio, filed=filed, adsh=adsh, symbol=None, form="F-6 POS",
        ratio_effectiveness_pending=True,
        ratio_effectiveness_pending_text=(
            "The ratio change amendments shall not become effective until the effective date "
            "for such ratio change as announced by the Depositary."
        ), **kwargs,
    )


def test_explicit_same_event_replacement_and_pending_contract_respect_xin_boundaries(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (2, 1), filed="2022-01-01")
    add_ratio(db, (2, 1), source="cover_footnote", filed="2022-01-01")
    _pending_ratio_registration(db, (20, 1))
    old = _dated_ratio_notice(db, (16, 1))
    corrected = _dated_ratio_notice(db, (20, 1), filed="2022-11-18",
                                    adsh="0001140361-22-042119", correction=True)
    for day in ("2022-11-15", "2022-11-16", "2022-11-18", "2022-11-19", "2022-11-27"):
        assert resolve(db, day)[:4] == ("resolved", "ads", 2, 1)
    answer = resolve(db, "2022-11-28")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert corrected in answer[-1] and old not in answer[-1]


def test_delayed_correction_preserves_the_old_effective_answer_until_publication(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (16, 1), filed="2022-01-01")
    add_ratio(db, (16, 1), source="cover_footnote", filed="2022-01-01")
    old = _dated_ratio_notice(db, (16, 1))
    _pending_ratio_registration(db, (20, 1))
    corrected = _dated_ratio_notice(db, (20, 1), filed="2022-11-18",
                                    publication_floor_on="2022-12-01",
                                    adsh="0001140361-22-042119", correction=True)
    before = resolve(db, "2022-11-30")
    assert before[:4] == ("resolved", "ads", 16, 1)
    assert old in before[-1] and corrected not in before[-1]
    after = resolve(db, "2022-12-01")
    assert after[:4] == ("resolved", "ads", 20, 1)
    assert corrected in after[-1] and old not in after[-1]


@pytest.mark.parametrize("correction,program", [
    (False, "old_cusip:98417P105"), (True, "old_cusip:98417P204"),
])
def test_unrelated_or_nonreplacement_same_date_notices_remain_ambiguous(db, correction, program):
    add(db, filed="2022-01-01")
    add_ratio(db, (20, 1), filed="2022-01-01")
    add_ratio(db, (20, 1), source="cover_footnote", filed="2022-01-01")
    old = _dated_ratio_notice(db, (16, 1))
    newer = _dated_ratio_notice(db, (20, 1), filed="2022-11-18", program=program,
                                adsh="0001140361-22-042119", correction=correction)
    answer = resolve(db, "2022-11-28")
    assert answer[0] == "ambiguous"
    assert old in answer[-1] and newer in answer[-1]


def test_correction_of_a_different_class_never_replaces_the_original_event(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (20, 1), filed="2022-01-01")
    add_ratio(db, (20, 1), source="cover_footnote", filed="2022-01-01")
    old = _dated_ratio_notice(db, (16, 1))
    _dated_ratio_notice(db, (20, 1), filed="2022-11-18", underlying_class="class_b",
                       adsh="0001140361-22-042119", correction=True)
    answer = resolve(db, "2022-11-28")
    assert answer[0] == "ambiguous" and old in answer[-1]


def test_same_day_correction_ties_never_choose_one_ratio(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (20, 1), filed="2022-01-01")
    add_ratio(db, (20, 1), source="cover_footnote", filed="2022-01-01")
    old = _dated_ratio_notice(db, (16, 1))
    first = _dated_ratio_notice(db, (20, 1), filed="2022-11-18",
                                adsh="0001140361-22-042119", correction=True)
    second = _dated_ratio_notice(db, (24, 1), filed="2022-11-18",
                                 adsh="0001140361-22-042120", correction=True)
    answer = resolve(db, "2022-11-28")
    assert answer[0] == "ambiguous"
    assert first in answer[-1] and second in answer[-1] and old not in answer[-1]


def test_later_republication_does_not_promote_a_legally_older_replacement(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (20, 1), filed="2022-01-01")
    add_ratio(db, (20, 1), source="cover_footnote", filed="2022-01-01")
    old = _dated_ratio_notice(db, (16, 1))
    _dated_ratio_notice(db, (20, 1), filed="2022-11-16", publication_floor_on="2022-11-20",
                       adsh="0001140361-22-042119", correction=True)
    answer = resolve(db, "2022-11-28")
    assert answer[0] == "ambiguous" and old in answer[-1]


def test_pending_f6_uses_later_public_date_knowledge_without_backdating(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (2, 1), filed="2022-01-01")
    add_ratio(db, (2, 1), source="cover_footnote", filed="2022-01-01")
    pending = _pending_ratio_registration(db, (20, 1))
    _dated_ratio_notice(db, (20, 1), filed="2022-11-18", available="2022-12-01",
                       adsh="0001140361-22-042119")
    for day in ("2022-11-16", "2022-11-28", "2022-11-30"):
        answer = resolve(db, day)
        assert answer[:4] == ("resolved", "ads", 2, 1) and pending not in answer[-1]
    assert resolve(db, "2022-12-01")[:4] == ("resolved", "ads", 20, 1)


def test_fee_table_in_same_registration_cannot_bypass_pending_contract(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (2, 1), filed="2022-01-01")
    add_ratio(db, (2, 1), source="cover_footnote", filed="2022-01-01")
    _pending_ratio_registration(db, (20, 1))
    fee = add_ratio(db, (20, 1), filed="2022-11-15", adsh="0001193805-22-001555",
                    form="F-6 POS", symbol=None)
    _dated_ratio_notice(db, (20, 1), filed="2022-11-18", adsh="0001140361-22-042119")
    for day in ("2022-11-16", "2022-11-19", "2022-11-27"):
        answer = resolve(db, day)
        assert answer[:4] == ("resolved", "ads", 2, 1) and fee not in answer[-1]
    answer = resolve(db, "2022-11-28")
    assert answer[:4] == ("resolved", "ads", 20, 1) and fee in answer[-1]


@pytest.mark.parametrize("adsh,ratio,underlying_class", [
    ("0001193805-22-001556", (20, 1), None),
    ("0001193805-22-001555", (16, 1), None),
    ("0001193805-22-001555", (20, 1), "class_b"),
])
def test_pending_contract_condition_does_not_govern_an_unrelated_registration_fact(
        db, adsh, ratio, underlying_class):
    add(db, filed="2022-01-01")
    add_ratio(db, (2, 1), filed="2022-01-01")
    add_ratio(db, (2, 1), source="cover_footnote", filed="2022-01-01")
    pending = _pending_ratio_registration(db, (20, 1))
    independent = add_ratio(db, ratio, filed="2022-11-15", adsh=adsh, symbol=None,
                            form="F-6 POS", underlying_class=underlying_class)
    answer = resolve(db, "2022-11-16")
    assert answer[0] == "ambiguous" and independent in answer[-1] and pending not in answer[-1]


@pytest.mark.parametrize("effective,ratio,underlying_class,symbol", [
    ("2020-07-01", (20, 1), None, None),
    ("2022-11-28", (16, 1), None, None),
    ("2022-11-28", (20, 1), "class_b", None),
    ("2022-11-28", (20, 1), None, "OTHER"),
])
def test_pending_f6_requires_its_own_dated_ratio_class_and_program(
        db, effective, ratio, underlying_class, symbol):
    add(db, filed="2022-01-01")
    add_ratio(db, (2, 1), filed="2022-01-01")
    add_ratio(db, (2, 1), source="cover_footnote", filed="2022-01-01")
    pending = _pending_ratio_registration(db, (20, 1))
    confirmed = ({"ratio_effectiveness_confirmed": True,
                  "ratio_effectiveness_confirmation_text": "A different change was completed on July 1, 2020.",
                  "ratio_effectiveness_confirmed_conditions": ["ratio_effective"]}
                 if effective == "2020-07-01" else {})
    add_ratio(db, ratio, source="ratio_change_6k", filed="2022-11-18", effective=effective,
              symbol=symbol, underlying_class=underlying_class, **confirmed)
    answer = resolve(db, "2022-11-28")
    assert pending not in answer[-1]


def test_same_day_f6_completion_cannot_borrow_a_different_known_program(db):
    add(db, filed="2022-01-01", underlying_class="class_a")
    add_ratio(db, (2, 1), filed="2022-01-01", underlying_class="class_a")
    add_ratio(db, (2, 1), source="cover_footnote", filed="2022-01-01", underlying_class="class_a")
    pending = _pending_ratio_registration(
        db, (20, 1), filed="2022-11-28", adsh="0001193805-22-001555",
        underlying_class="class_a",
    )
    add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-11-30",
        effective="2022-11-28", symbol="TSM", underlying_class="class_a",
        adsh="0001234567-22-000101", ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="This ratio change was completed on November 28.",
        ratio_effectiveness_confirmed_conditions=["ratio_effective"],
        ratio_change_program_key="old_cusip:111111111",
    )
    add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-11-30",
        effective="2022-11-28", symbol=None, underlying_class="class_a",
        adsh="0001234567-22-000102", ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="A different program completed its ratio change on November 28.",
        ratio_effectiveness_confirmed_conditions=["ratio_effective"],
        ratio_change_program_key="old_cusip:222222222",
    )
    answer = resolve(db, "2022-12-01")
    assert pending not in answer[-1]


def test_dated_ante_consolidation_ends_the_old_event_when_pending_f6_takes_effect(db):
    add(db, filed="2022-05-01")
    add_ratio(db, (10, 1), filed="2019-01-01")
    add_ratio(db, (10, 1), source="cover_footnote", filed="2022-05-01")
    old = add_ratio(db, (10, 1), source="ratio_change_6k", filed="2019-05-07", effective="2019-04-11")
    _pending_ratio_registration(db, (1, 1), filed="2022-11-21")
    new = add_ratio(db, (1, 1), source="ratio_change_6k", filed="2022-11-04", effective="2022-12-09")
    for day in ("2022-11-05", "2022-11-22", "2022-12-08"):
        assert resolve(db, day)[:4] == ("resolved", "ads", 10, 1)
    answer = resolve(db, "2022-12-09")
    assert answer[:4] == ("resolved", "ads", 1, 1)
    assert new in answer[-1] and old not in answer[-1]


def _insert_dated_ratio_source_fixture(db, name):
    from psycopg import sql
    meta = json.loads((FIXTURES / (name + ".json")).read_text(encoding="utf-8"))
    payload = (FIXTURES / meta["fixture"]).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == meta["sha256"] and b"\r" not in payload
    parsed = parse_filing(payload.decode("utf-8"), **{
        key: meta[key] for key in ("cik", "form_type", "accession_number", "filing_date", "source_url")
    })
    ids = []
    for fact in parsed:
        row = {**fact, "fact_hash": uuid4().hex, "loaded_on": "2026-10-09", "source_package": name}
        stmt = sql.SQL("INSERT INTO public.sec_foreign_listing_evidence ({}) VALUES ({}) RETURNING id").format(
            sql.SQL(",").join(map(sql.Identifier, row)),
            sql.SQL(",").join(sql.Placeholder() for _ in row),
        )
        ids.append(db.execute(stmt, list(row.values())).fetchone()[0])
    return ids


def test_real_xin_correcting_notice_and_pending_receipt_preserve_the_temporal_regime(db):
    add(db, cik=1398453, symbol="XIN", filed="2022-01-01")
    add_ratio(db, (2, 1), cik=1398453, symbol="XIN", filed="2022-01-01")
    add_ratio(db, (2, 1), cik=1398453, symbol="XIN", source="cover_footnote", filed="2022-01-01")
    _insert_dated_ratio_source_fixture(db, "xin_2022_pending_f6_ratio")
    old = _insert_dated_ratio_source_fixture(db, "xin_2022_original_ratio_notice")
    new = _insert_dated_ratio_source_fixture(db, "xin_2022_correcting_ratio_notice")
    assert old and new
    for day in ("2022-11-15", "2022-11-16", "2022-11-18", "2022-11-19", "2022-11-27"):
        assert resolve(db, day, cik=1398453, symbol="XIN")[:4] == ("resolved", "ads", 2, 1)
    answer = resolve(db, "2022-11-28", cik=1398453, symbol="XIN")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert set(new).intersection(answer[-1]) and not set(old).intersection(answer[-1])


def test_real_ante_consolidation_defers_the_pending_receipt_to_legal_effectiveness(db):
    add(db, cik=1413745, symbol="ANTE", filed="2022-05-01")
    add_ratio(db, (10, 1), cik=1413745, symbol="ANTE", filed="2019-01-01")
    add_ratio(db, (10, 1), cik=1413745, symbol="ANTE", source="cover_footnote", filed="2022-05-01")
    old = add_ratio(db, (10, 1), cik=1413745, symbol=None, source="ratio_change_6k",
                    filed="2019-05-07", effective="2019-04-11")
    _insert_dated_ratio_source_fixture(db, "ante_2022_pending_f6_ratio")
    new = []
    for name in ("ante_2022_consolidation_notice", "ante_2022_consolidation_meeting_notice",
                 "ante_2022_consolidation_confirmation"):
        new.extend(_insert_dated_ratio_source_fixture(db, name))
    assert new
    for day in ("2022-11-05", "2022-11-22", "2022-12-08"):
        assert resolve(db, day, cik=1413745, symbol="ANTE")[:4] == ("resolved", "ads", 10, 1)
    for day in ("2022-12-09", "2022-12-12", "2025-12-31"):
        answer = resolve(db, day, cik=1413745, symbol="ANTE")
        assert answer[:4] == ("resolved", "ads", 1, 1)
        assert old not in answer[-1] and set(new).intersection(answer[-1])


@pytest.mark.parametrize("symbol", ["TSM", None])
def test_nonordinary_registration_ratio_is_ineligible_even_with_an_explicit_symbol(db, symbol):
    add(db)
    add_ratio(db, (5, 1), source="cover_footnote")
    nonordinary = add_ratio(db, (5, 1), ordinary_candidate=False, symbol=symbol)
    answer = resolve(db, "2020-01-02")
    assert answer[:6] == ("none", "ads", None, None, "resolved", "none")
    assert nonordinary not in answer[-1]


@pytest.mark.parametrize("symbol", ["TSM", None])
def test_newer_nonordinary_program_does_not_replace_the_operative_ordinary_registration(db, symbol):
    add(db)
    ordinary = add_ratio(db, (5, 1))
    add_ratio(db, (5, 1), source="cover_footnote")
    preferred = add_ratio(db, (99, 1), filed="2020-06-01", ordinary_candidate=False, symbol=symbol)
    for day in ("2020-06-01", "2020-06-02", "2025-12-31"):
        answer = resolve(db, day)
        assert answer[:4] == ("resolved", "ads", 5, 1)
        assert ordinary in answer[-1] and preferred not in answer[-1]


def test_nonordinary_change_never_ends_or_competes_with_the_ordinary_regime(db):
    add(db)
    add_ratio(db, (5, 1))
    add_ratio(db, (5, 1), source="cover_footnote")
    preferred = add_ratio(db, (10, 1), source="ratio_change_6k", filed="2020-06-01",
                          effective="2020-07-01", ordinary_candidate=False)
    for day in ("2020-06-02", "2020-07-01", "2025-12-31"):
        answer = resolve(db, day)
        assert answer[:4] == ("resolved", "ads", 5, 1) and preferred not in answer[-1]


def test_nonordinary_notice_cannot_activate_an_ordinary_pending_registration(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (2, 1), filed="2022-01-01")
    add_ratio(db, (2, 1), source="cover_footnote", filed="2022-01-01")
    pending = _pending_ratio_registration(db, (20, 1))
    preferred = add_ratio(db, (20, 1), source="ratio_change_6k", filed="2022-11-18",
                          effective="2022-11-28", ordinary_candidate=False)
    for day in ("2022-11-19", "2022-11-28", "2025-12-31"):
        answer = resolve(db, day)
        assert answer[:4] == ("resolved", "ads", 2, 1)
        assert pending not in answer[-1] and preferred not in answer[-1]


def test_ignoring_nonordinary_program_does_not_resurrect_an_expired_ordinary_registration(db):
    add(db)
    add_ratio(db, (5, 1))
    ended = add_ratio(db, (2, 1), filed="2020-02-01", until="2020-03-01")
    add_ratio(db, (2, 1), source="cover_footnote", filed="2020-02-01")
    add_ratio(db, (2, 1), filed="2020-04-01", ordinary_candidate=False)
    assert resolve(db, "2020-02-29")[:4] == ("resolved", "ads", 2, 1)
    for day in ("2020-03-01", "2020-04-02", "2025-12-31"):
        answer = resolve(db, day)
        assert answer[:6] == ("none", "ads", None, None, "resolved", "none")
        assert ended not in answer[-1]


def test_nonordinary_same_event_correction_cannot_replace_an_ordinary_notice(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (16, 1), filed="2022-01-01")
    add_ratio(db, (16, 1), source="cover_footnote", filed="2022-01-01")
    old = _dated_ratio_notice(db, (16, 1))
    preferred = _dated_ratio_notice(db, (20, 1), filed="2022-11-18", ordinary_candidate=False,
                                    adsh="0001140361-22-042119", correction=True)
    answer = resolve(db, "2022-11-28")
    assert answer[:4] == ("resolved", "ads", 16, 1)
    assert old in answer[-1] and preferred not in answer[-1]


def test_nonordinary_later_corroboration_does_not_hide_a_genuine_ordinary_conflict(db):
    add(db)
    add_ratio(db, (5, 1))
    add_ratio(db, (2, 1), source="cover_footnote")
    add_ratio(db, (5, 1), source="cover_footnote", filed="2020-06-01", ordinary_candidate=False)
    assert resolve(db, "2020-06-02")[0] == "ambiguous"


def test_foreign_evidence_schema_replay_is_additive_and_idempotent(sql_database):
    before = sql_database.execute("SELECT count(*) FROM public.sec_foreign_listing_evidence").fetchone()[0]
    schema = (ROOT / "schemas" / "sec_foreign_listing_evidence.sql").read_text(encoding="utf-8")
    sql_database.execute(schema)
    sql_database.execute(schema)
    assert sql_database.execute("SELECT count(*) FROM public.sec_foreign_listing_evidence").fetchone()[0] == before
    constraints = sql_database.execute(
        "SELECT conname FROM pg_catalog.pg_constraint "
        "WHERE conrelid = 'public.sec_foreign_listing_evidence'::regclass "
        "AND conname IN ('sec_foreign_listing_ratio_program_ck', "
        "'sec_foreign_listing_ratio_correction_ck', 'sec_foreign_listing_ratio_pending_ck', "
        "'sec_foreign_listing_operative_date_conflict_ck', 'sec_foreign_listing_ratio_confirmation_ck')"
    ).fetchall()
    assert len(constraints) == 5
    columns = dict(sql_database.execute(
        "SELECT attname, format_type(atttypid, atttypmod) FROM pg_catalog.pg_attribute "
        "WHERE attrelid = 'public.sec_foreign_listing_evidence'::regclass AND NOT attisdropped "
        "AND attname IN ('operative_date_conflict', 'operative_date_candidates', "
        "'operative_date_conflict_text', 'ratio_effectiveness_confirmed', "
        "'ratio_effectiveness_confirmation_text', 'ratio_effectiveness_conditions', "
        "'ratio_effectiveness_confirmed_conditions')"
    ).fetchall())
    assert columns == {
        "operative_date_conflict": "boolean", "operative_date_candidates": "date[]",
        "operative_date_conflict_text": "text", "ratio_effectiveness_confirmed": "boolean",
        "ratio_effectiveness_confirmation_text": "text",
        "ratio_effectiveness_conditions": "text[]",
        "ratio_effectiveness_confirmed_conditions": "text[]",
    }


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


def test_dated_change_corroborates_new_f6_before_the_next_annual_filing(db):
    add(db, filed="2015-03-10")
    add_ratio(db, (1, 1), filed="2014-11-14")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2015-03-10")
    add_ratio(db, (1, 2), filed="2015-07-20", effective="2015-07-27")
    add_ratio(db, (1, 2), source="ratio_change_6k", filed="2015-06-26", effective="2015-07-27")
    assert resolve(db, "2015-07-20")[:4] == ("resolved", "ads", 1, 1)
    assert resolve(db, "2015-07-21")[:4] == ("resolved", "ads", 1, 1)
    assert resolve(db, "2015-07-26")[:4] == ("resolved", "ads", 1, 1)
    assert resolve(db, "2015-07-27")[:4] == ("resolved", "ads", 1, 2)
    add_ratio(db, (1, 1), source="cover_footnote", filed="2016-03-08")
    assert resolve(db, "2016-03-09")[0] == "ambiguous"


def test_full_real_azn_contracts_and_announcement_switch_on_effective_date(db):
    # Isolate ratio chronology using a synthetic, already-bound cover line.
    # Both complete F-6 contract documents and the dated 6-K are parsed from
    # their real SEC contents, including the companion's old unamended terms.
    add(db, cik=901832, symbol="AZN", filed="2015-03-10")
    add_ratio(db, (1, 1), cik=901832, symbol="AZN", source="cover_footnote", filed="2015-03-10")
    insert_real_parsed_rows(db, "azn_2014_f6_ratio")
    insert_real_parsed_rows(db, "azn_2015_amendment_full")
    insert_real_parsed_rows(db, "azn_2015_legacy_agreement_full")
    insert_real_parsed_rows(db, "azn_2015_6k_announcement")
    assert resolve(db, "2015-07-20", cik=901832, symbol="AZN")[:4] == ("resolved", "ads", 1, 1)
    assert resolve(db, "2015-07-21", cik=901832, symbol="AZN")[:4] == ("resolved", "ads", 1, 1)
    assert resolve(db, "2015-07-26", cik=901832, symbol="AZN")[:4] == ("resolved", "ads", 1, 1)
    assert resolve(db, "2015-07-27", cik=901832, symbol="AZN")[:4] == ("resolved", "ads", 1, 2)


def test_real_anpc_future_announcement_dates_placeholder_f6_ratio(db):
    for name in ("anpc_2022_cover", "anpc_2019_f6_full", "anpc_2022_f6_undated_amendment_full"):
        insert_real_parsed_rows(db, name)
    plan = _parse_independent_listing_fixture("anpc_2022_plan_expected_ratio")
    _insert_gate_parsed_rows(db, plan, "anpc-2022-plan-expected-ratio")
    for day in ("2022-10-24", "2022-10-25", "2022-11-03"):
        assert resolve(db, day, cik=1786511, symbol="ANPC")[:4] == ("resolved", "ads", 1, 1)
    for day in ("2022-11-04", "2022-12-16"):
        answer = resolve(db, day, cik=1786511, symbol="ANPC")
        assert answer[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
    completion = _parse_independent_listing_fixture("anpc_2022_ratio_completion")
    assert completion and all(row.get("ratio_effectiveness_confirmed") is True for row in completion)
    _insert_gate_parsed_rows(db, completion, "anpc-2022-ratio-completion")
    assert resolve(db, "2022-12-16", cik=1786511, symbol="ANPC")[0] == "ambiguous"
    assert resolve(db, "2022-12-17", cik=1786511, symbol="ANPC")[:4] == ("resolved", "ads", 20, 1)


def _future_ratio_program(db, *, explicit=False):
    add(db, filed="2022-05-16", underlying_class="class_a")
    add_ratio(db, (1, 1), filed="2019-11-07", underlying_class="class_a")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-05-16", underlying_class="class_a")
    add_ratio(db, (20, 1), filed="2022-10-24", underlying_class="class_a", effective_date_explicit=explicit)


def test_explicit_f6_date_is_not_overridden_by_future_change_announcement(db):
    _future_ratio_program(db, explicit=True)
    add_ratio(db, (20, 1), source="ratio_change_6k", filed="2022-10-18", effective="2022-11-04", underlying_class="class_a")
    assert resolve(db, "2022-10-25")[0] == "ambiguous"


@pytest.mark.parametrize("filed,floor", [("2022-10-30", None), ("2022-10-18", "2022-10-30")])
def test_future_announcement_must_already_be_public_when_f6_becomes_available(db, filed, floor):
    _future_ratio_program(db)
    add_ratio(db, (20, 1), source="ratio_change_6k", filed=filed, effective="2022-11-04",
              publication_floor_on=floor, underlying_class="class_a")
    assert resolve(db, "2022-10-25")[0] == "ambiguous"
    assert resolve(db, "2022-11-03")[0] == "ambiguous"


def test_corrected_announcement_learned_after_f6_cannot_retroactively_date_it(db):
    _future_ratio_program(db)
    add_ratio(db, (20, 1), source="ratio_change_6k", filed="2022-10-18", effective="2022-11-04",
              available="2022-10-30", underlying_class="class_a")
    assert resolve(db, "2022-11-03")[0] == "ambiguous"


def test_future_announcement_for_a_different_ratio_cannot_date_f6(db):
    _future_ratio_program(db)
    add_ratio(db, (40, 1), source="ratio_change_6k", filed="2022-10-18", effective="2022-11-04", underlying_class="class_a")
    assert resolve(db, "2022-10-25")[0] == "ambiguous"


def test_only_filing_plus_one_fallback_is_eligible_for_announcement_deferral(db):
    add(db, filed="2022-05-16", underlying_class="class_a")
    add_ratio(db, (1, 1), filed="2019-11-07", underlying_class="class_a")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-05-16", underlying_class="class_a")
    add_ratio(db, (20, 1), filed="2022-10-24", effective="2022-10-26", effective_date_explicit=False, underlying_class="class_a")
    add_ratio(db, (20, 1), source="ratio_change_6k", filed="2022-10-18", effective="2022-11-04", underlying_class="class_a")
    assert resolve(db, "2022-10-26")[0] == "ambiguous"


@pytest.mark.parametrize("symbol,underlying_class", [("OTHER", "class_a"), ("TSM", "class_b")])
def test_future_change_for_a_different_symbol_or_class_cannot_date_f6(db, symbol, underlying_class):
    _future_ratio_program(db)
    add_ratio(db, (20, 1), source="ratio_change_6k", filed="2022-10-18", effective="2022-11-04",
              symbol=symbol, underlying_class=underlying_class)
    assert resolve(db, "2022-10-25")[0] == "ambiguous"


def test_multiple_announced_future_dates_never_choose_a_deferral_date(db):
    _future_ratio_program(db)
    add_ratio(db, (20, 1), source="ratio_change_6k", filed="2022-10-18", effective="2022-11-04", underlying_class="class_a")
    add_ratio(db, (20, 1), source="ratio_change_6k", filed="2022-10-19", effective="2022-12-01", underlying_class="class_a")
    assert resolve(db, "2022-10-25")[0] == "ambiguous"
    # The later event must not cause re-deferral after the first event starts.
    assert resolve(db, "2022-11-05")[:4] == ("resolved", "ads", 20, 1)


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


def _parse_ratio_transition_fixture(name):
    metadata = json.loads((FIXTURES / "ratio_transition_regressions.json").read_text(encoding="utf-8"))
    meta = next(item for item in metadata if item["name"] == name)
    payload = (FIXTURES / meta["fixture"]).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == meta["sha256"]
    assert b"\r" not in payload
    kwargs = {key: meta[key] for key in ("cik", "form_type", "accession_number", "filing_date", "source_url")}
    return parse_filing(payload.decode("utf-8"), **kwargs)


def test_original_honda_2007_reciprocal_transitions_keep_new_regimes_and_their_own_dates():
    rows = _parse_ratio_transition_fixture("honda_2007_6k_ratio_transitions")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"]) for row in rows} == {
        (1, 1, "2006-07-01"), (1, 2, "2002-01-10"),
    }
    assert sum(row["effective_from"] == "2006-07-01" for row in rows) == 3
    assert {row["available_on"] for row in rows} == {"2007-03-14"}
    assert all(row["source_kind"] == "ratio_change_6k" for row in rows)


@pytest.mark.parametrize("name", ["pldt_2002_charter_not_ratio_change", "fms_2008_tax_not_ratio_date"])
def test_original_non_ads_effective_dates_do_not_date_undated_ads_ratios(name):
    assert _parse_ratio_transition_fixture(name) == []


def test_original_tal_transition_retains_explicit_class_a_share_antecedent():
    rows = _parse_ratio_transition_fixture("tal_2017_class_a_ratio_change")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"], row["underlying_class"])
            for row in rows} == {(1, 3, "2017-08-16", "class_a")}


def test_original_dated_current_ratio_is_kept_without_an_old_numeric_side():
    rows = _parse_ratio_transition_fixture("osn_2017_dated_current_ratio")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(3, 1, "2016-08-22")}


@pytest.mark.parametrize("name,ratio,effective", [
    ("mechel_2015_ordinary_adr_change", (2, 1), "2016-01-12"),
    ("james_hardie_2016_cufs_adr_change", (1, 1), "2015-09-18"),
])
def test_original_adr_transitions_retain_the_new_ratio_and_explicit_unit_direction(name, ratio, effective):
    rows = _parse_ratio_transition_fixture(name)
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(*ratio, effective)}
    assert all(row["ordinary_candidate"] for row in rows)


def test_explicitly_dated_current_ratio_label_is_not_a_former_ratio():
    rows = _parse_6k_transition_text(
        "The current ADS ratio is one ADS representing three ordinary shares, effective from July 1, 2020."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(3, 1, "2020-07-01")}


def _parse_6k_transition_text(text):
    return parse_filing(text, cik=715153, form_type="6-K", accession_number="0001193125-07-052771",
                        filing_date="2020-08-01", source_url="https://www.sec.gov/Archives/test")


@pytest.mark.parametrize("new_side", [
    "one ADS representing twenty ordinary shares",
    "one ADS, each representing twenty ordinary shares",
])
def test_forward_from_to_clause_is_not_misread_as_a_reciprocal_ratio(new_side):
    rows = _parse_6k_transition_text(
        "The ADS ratio changed from one ADS representing one ordinary share to "
        f"{new_side}, effective July 1, 2020."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {(20, 1)}


def test_reciprocal_american_share_receipt_parentheses_end_the_instrument():
    instrument = "American share (evidenced by depositary receipts)"
    rows = _parse_6k_transition_text(
        f"The depositary ratio changed from two ordinary shares to one {instrument} "
        f"to one ordinary share to one {instrument}, effective July 1, 2020."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(1, 1, "2020-07-01")}


def test_compact_ratio_without_share_to_depositary_direction_is_not_guessed():
    assert _parse_6k_transition_text(
        "The ADR ratio changed from a 5-to-1 ratio to a 1-to-1 ratio, effective July 1, 2020."
    ) == []


@pytest.mark.parametrize("later_date", [", effective July 1, 2020", ""])
def test_two_transitions_in_one_sentence_never_share_the_first_event_date(later_date):
    assert _parse_6k_transition_text(
        "The ADR ratio changed from one ADR representing one ordinary share to one ADR representing two "
        "ordinary shares, effective January 1, 2020, and from one ADR representing two ordinary shares "
        f"to one ADR representing five ordinary shares{later_date}."
    ) == []


def test_adjacent_undated_change_does_not_borrow_the_previous_transition_date():
    rows = _parse_6k_transition_text(
        "The ADR ratio changed from one ADR representing one ordinary share to one ADR representing two "
        "ordinary shares, effective January 1, 2020. Separately, the ratio will change from one ADR "
        "representing two ordinary shares to one ADR representing five ordinary shares."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(2, 1, "2020-01-01")}


def test_charter_effective_date_with_an_ads_mention_does_not_date_a_ratio_transition():
    assert _parse_6k_transition_text(
        "The ADS ratio will change from one ADS representing one ordinary share to one ADS representing five "
        "ordinary shares. The amended charter became effective January 1, 2020 while the ADSs continued trading."
    ) == []


def test_conflicting_dates_for_the_same_announced_change_remain_unresolved():
    assert _parse_6k_transition_text(
        "The ADS ratio will change from one ADS representing one ordinary share to one ADS representing five "
        "ordinary shares. The effective date of the ratio change is January 1, 2020. "
        "The effective date of the ratio change is July 1, 2020."
    ) == []


def test_ratio_transition_does_not_inherit_a_neighboring_preferred_program_class():
    rows = _parse_6k_transition_text(
        "The Class B preferred ADS program is separate. The ordinary ADS ratio changed from one ADS "
        "representing one ordinary share to one ADS representing four ordinary shares, effective July 1, 2020."
    )
    assert rows
    assert {row["underlying_class"] for row in rows} == {None}
    assert all(row["ordinary_candidate"] for row in rows)


def test_ratio_change_heading_connects_a_directly_dated_depositary_entitlement():
    rows = _parse_6k_transition_text(
        "Ratio change. Effective October 1, 2020, each American Depositary Share represents five ordinary shares."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(5, 1, "2020-10-01")}


def test_ratio_heading_does_not_connect_a_tax_date_to_an_undated_entitlement():
    assert _parse_6k_transition_text(
        "Ratio change. Effective October 1, 2020, the German tax reform applies while each ADS represents five ordinary shares."
    ) == []


@pytest.mark.parametrize("proof", [
    "",
    " Each CUFS represents two ordinary shares.",
    " One ADR is equivalent to one ordinary share/CUFS.",
])
def test_cufs_to_adr_counts_without_matching_ordinary_entitlement_are_not_share_ratios(proof):
    assert _parse_6k_transition_text(
        "The ADR ratio changed from a 5-to-1 CUFS-to-ADR ratio to a 10-to-1 ratio, effective July 1, 2020."
        + proof
    ) == []


def test_compact_ordinary_share_to_adr_direction_does_not_require_a_cufs_conversion():
    rows = _parse_6k_transition_text(
        "The ADR ratio changed from a 5-to-1 ordinary shares-to-ADR ratio to a 10-to-1 ratio, effective July 1, 2020."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(10, 1, "2020-07-01")}


def test_original_ccu_anaphoric_date_remains_attached_through_an_underlying_share_clarification():
    rows = _parse_ratio_transition_fixture("ccu_2017_anaphoric_ratio_date")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(2, 1, "2012-12-20")}


@pytest.mark.parametrize("intervening", [
    "The board amended the company charter.",
    "The company completed an unrelated acquisition.",
])
def test_anaphoric_effective_date_does_not_cross_an_unrelated_intervening_action(intervening):
    assert _parse_6k_transition_text(
        "The ADS ratio changed from one ADS representing five ordinary shares to one ADS representing two "
        "ordinary shares. There was no change to the underlying ordinary shares. "
        f"{intervening} This action was effective on December 20, 2012."
    ) == []


def _parse_independent_listing_fixture(name):
    metadata = json.loads((FIXTURES / (name + ".json")).read_text(encoding="utf-8"))
    payload = (FIXTURES / metadata["fixture"]).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == metadata["sha256"]
    assert b"\r" not in payload
    kwargs = {key: metadata[key] for key in ("cik", "form_type", "accession_number", "filing_date", "source_url")}
    return parse_filing(payload.decode("utf-8-sig", errors="replace"), **kwargs)


def test_original_drd_for_transition_does_not_attach_the_old_ratio_to_the_new_date():
    rows = _parse_independent_listing_fixture("drd_2007_ratio_for_transition")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(10, 1, "2007-07-23")}
    assert {row["available_on"] for row in rows} == {"2007-07-21"}


def test_explicit_old_new_ratio_table_retains_only_the_new_column():
    rows = _parse_6k_transition_text(
        "The ADS ratio change is effective July 23, 2020. "
        "<table><tr><th>OLD</th><th>NEW</th></tr><tr><td>Ratio: one ADS for one ordinary share</td>"
        "<td>one ADS for ten ordinary shares</td></tr></table>"
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {(10, 1)}


@pytest.mark.parametrize("history", ["one ADS formerly represented one ordinary share", "one ADS used to represent one ordinary share"])
def test_historical_former_ratio_is_not_assigned_the_new_event_date(history):
    rows = _parse_6k_transition_text(
        f"The ADS ratio change is effective July 23, 2020. The {history}. "
        "The new ratio is one ADS for ten ordinary shares, effective July 23, 2020."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {(10, 1)}


def test_original_xin_correction_carries_exact_program_and_literal_replacement_provenance():
    original = _parse_independent_listing_fixture("xin_2022_original_ratio_notice")
    corrected = _parse_independent_listing_fixture("xin_2022_correcting_ratio_notice")
    assert {(row["ratio_numerator"], row["effective_from"]) for row in original} == {(16, "2022-11-28")}
    assert {(row["ratio_numerator"], row["effective_from"]) for row in corrected} == {(20, "2022-11-28")}
    assert {row["ratio_change_program_key"] for row in original + corrected} == {"old_cusip:98417P105"}
    assert all(row["ratio_change_correction_kind"] is None for row in original)
    assert {row["ratio_change_correction_kind"] for row in corrected} == {"correcting_and_replacing"}
    assert all(row["ratio_change_correction_text"] ==
               "CORRECTING and REPLACING - Xinyuan Real Estate Co., Ltd. Announces ADR Ratio Change" for row in corrected)
    assert {row["available_on"] for row in original} == {"2022-11-18"}
    assert {row["available_on"] for row in corrected} == {"2022-11-19"}


def test_amendment_form_and_recency_alone_do_not_claim_a_ratio_correction():
    rows = parse_filing(
        "The new ADR ratio is one ADR for twenty ordinary shares, effective November 28, 2022. "
        "Old CUSIP: 98417P105.", cik=1398453, form_type="6-K/A", accession_number="0001140361-22-042119",
        filing_date="2022-11-18", source_url="https://www.sec.gov/Archives/test",
    )
    assert rows
    assert {row["ratio_change_program_key"] for row in rows} == {"old_cusip:98417P105"}
    assert all(row["ratio_change_correction_kind"] is None for row in rows)
    assert all(row["ratio_change_correction_text"] is None for row in rows)


@pytest.mark.parametrize("name,ratio", [
    ("xin_2022_pending_f6_ratio", 20), ("ante_2022_pending_f6_ratio", 1),
])
def test_original_operative_f6_conditions_are_explicit_pending_ratio_facts(name, ratio):
    rows = _parse_independent_listing_fixture(name)
    assert {(row["ratio_numerator"], row["ratio_denominator"]) for row in rows} == {(ratio, 1)}
    assert all(row["ratio_effectiveness_pending"] is True for row in rows)
    assert all("announced by the Depositary" in row["ratio_effectiveness_pending_text"] for row in rows)


def test_proposed_f6_filing_and_generic_holder_notice_are_not_pending_ratio_conditions():
    rows = parse_filing(
        "It is proposed that this filing become effective immediately. Each ADS represents five ordinary shares. "
        "Any amendment prejudicing holder rights shall not become effective until thirty days after notice.",
        cik=123, form_type="F-6 POS", accession_number="0001234567-20-000001", filing_date="2020-01-01",
        source_url="https://www.sec.gov/Archives/test",
    )
    assert rows
    assert all(row["ratio_effectiveness_pending"] is None for row in rows)


@pytest.mark.parametrize("name,available", [
    ("ante_2022_consolidation_notice", "2022-11-05"),
    ("ante_2022_consolidation_meeting_notice", "2022-11-05"),
    ("ante_2022_consolidation_confirmation", "2022-12-01"),
])
def test_original_ante_ratio_follows_the_named_consolidation_date_not_trading_price_date(name, available):
    rows = _parse_independent_listing_fixture(name)
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(1, 1, "2022-12-09")}
    assert {row["available_on"] for row in rows} == {available}
    assert all("effective-date-named-event=Share Consolidation" in row["evidence_location"] for row in rows)
    assert all("December 9, 2022" in row["evidence_text"] for row in rows)
    assert all(row["ordinary_candidate"] for row in rows)


def test_unlinked_consolidation_does_not_supply_a_ratio_effective_date():
    assert _parse_6k_transition_text(
        "The Share Consolidation will be effective at 5:00 P.M., on December 9, 2022. "
        "Separately, the Company proposes an ADS ratio change from one ADS for ten ordinary shares "
        "to one ADS for one ordinary share."
    ) == []


def test_conflicting_named_consolidation_dates_do_not_choose_one():
    rows = _parse_6k_transition_text(
        "The Share Consolidation will be effective at 5:00 P.M., on December 9, 2022. "
        "The Share Consolidation will be effective at 5:00 P.M., on December 10, 2022. "
        "Upon the Share Consolidation, the ADS ratio will change from one ADS for ten ordinary shares "
        "to one ADS for one ordinary share."
    )
    assert rows and all(row["operative_date_conflict"] is True for row in rows)
    assert {tuple(row["operative_date_candidates"]) for row in rows} == {("2022-12-09", "2022-12-10")}
    assert {(row["ratio_numerator"], row["effective_from"]) for row in rows} == {(1, "2022-12-09")}
    assert all(row["effective_date_explicit"] for row in rows)


@pytest.mark.parametrize("name,available", [
    ("nndm_2020_reverse_split_notice", "2020-06-17"),
    ("nndm_2020_reverse_split_confirmation", "2020-10-20"),
])
def test_original_nndm_concurrent_share_conversion_has_its_exact_ratio_date(name, available):
    rows = _parse_independent_listing_fixture(name)
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(1, 1, "2020-06-29")}
    assert {row["available_on"] for row in rows} == {available}
    assert all(row["ordinary_candidate"] for row in rows)


@pytest.mark.parametrize("name,available", [
    ("edu_2011_ratio_change_notice", "2011-07-20"),
    ("edu_2011_ratio_change_confirmation", "2011-10-19"),
])
def test_original_edu_completed_ratio_does_not_reuse_current_or_price_effect_side(name, available):
    rows = _parse_independent_listing_fixture(name)
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(1, 1, "2011-08-18")}
    assert {row["available_on"] for row in rows} == {available}


def test_original_dq_completed_event_retains_its_date_and_publication_floor():
    rows = _parse_independent_listing_fixture("dq_2020_completed_ratio_change")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(5, 1, "2020-11-17")}
    assert {row["available_on"] for row in rows} == {"2020-11-24"}


def test_original_dq_f6_prior_and_commencing_entitlements_have_disjoint_intervals():
    rows = _parse_independent_listing_fixture("dq_2020_prior_commencing_f6")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"],
             row.get("effective_to")) for row in rows} == {
        (25, 1, "2020-10-27", "2020-11-17"),
        (5, 1, "2020-11-17", None),
    }
    assert {row["available_on"] for row in rows} == {"2020-10-27"}
    assert all(row["ordinary_candidate"] for row in rows)


@pytest.mark.parametrize("name,available", [
    ("scisparc_2021_two_ratio_events_notice", "2021-08-17"),
    ("scisparc_2021_two_ratio_events_confirmation", "2021-08-27"),
])
def test_original_scisparc_preserves_both_events_without_assigning_the_old_sides(name, available):
    rows = _parse_independent_listing_fixture(name)
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(140, 1, "2020-10-16"), (1, 1, "2021-08-09")}
    assert {row["available_on"] for row in rows} == {available}


@pytest.mark.parametrize("transition", [
    "from the current ADS Ratio of one ADS to three ordinary shares "
    "to a new ADS Ratio of one ADS to thirty ordinary shares",
    "from its original ratio of one ADS representing three ordinary shares "
    "to a new ratio of one ADS representing thirty ordinary shares",
    "from the then-current ADS Ratio of one ADS to three ordinary shares "
    "to a new ADS Ratio of one ADS to thirty ordinary shares",
])
def test_explicit_old_new_ratio_wrappers_keep_only_the_entitlement_for_the_event(transition):
    rows = _parse_6k_transition_text(
        f"The Company will change its ADS ratio {transition}, effective July 23, 2020."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(30, 1, "2020-07-23")}


def test_ratio_of_old_has_been_changed_to_new_is_one_dated_event():
    rows = _parse_6k_transition_text(
        "The ratio of one ADS representing three ordinary shares has been changed "
        "to one ADS representing thirty ordinary shares, effective July 23, 2020."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(30, 1, "2020-07-23")}


def test_class_b_current_entitlement_survives_a_separate_class_a_ratio_transition():
    rows = _parse_6k_transition_text(
        "The Class A ADS ratio will change from one ADS representing three Class A ordinary shares "
        "to one ADS representing thirty Class A ordinary shares, effective July 23, 2020. "
        "The separate Class B program has a current ratio of one ADS representing three "
        "Class B ordinary shares, effective July 23, 2020."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["underlying_class"],
             row["effective_from"]) for row in rows} == {
        (30, 1, "class_a", "2020-07-23"), (3, 1, "class_b", "2020-07-23"),
    }


def test_hypothetical_first_trading_date_does_not_date_an_unlinked_ads_ratio_change():
    assert _parse_6k_transition_text(
        "The Company proposes a reverse split of its ordinary shares. "
        "The first date when the Company's ADSs will begin trading on the Nasdaq Capital Market "
        "after implementation of the reverse split will be September 15, 2020. "
        "Separately, the Company proposes to change the ADS ratio from one ADS representing "
        "one ordinary share to one ADS representing five ordinary shares."
    ) == []


def test_a_different_class_reverse_split_does_not_supply_the_ads_event_date():
    assert _parse_6k_transition_text(
        "The reverse split of the Company's Class B ordinary shares was effective July 23, 2020. "
        "Separately, the Company proposes a reverse split of its Class A ordinary shares. "
        "Concurrently with the Class A reverse split, the ADS ratio will change from one ADS "
        "representing one Class A ordinary share to one ADS representing five Class A ordinary shares."
    ) == []


def test_conflicting_reverse_split_effective_dates_do_not_pick_the_nearest_date():
    rows = _parse_6k_transition_text(
        "The reverse split of ordinary shares will be effective July 23, 2020. "
        "The same reverse split of ordinary shares will be effective July 24, 2020. "
        "Concurrently with the reverse split, the ADS ratio will change from one ADS "
        "representing one ordinary share to one ADS representing five ordinary shares."
    )
    assert rows and all(row["operative_date_conflict"] is True for row in rows)
    assert {tuple(row["operative_date_candidates"]) for row in rows} == {("2020-07-23", "2020-07-24")}
    assert {(row["ratio_numerator"], row["effective_from"]) for row in rows} == {(5, "2020-07-23")}
    assert all(row["effective_date_explicit"] for row in rows)


def test_completed_on_date_ratio_uses_the_event_date_instead_of_the_announcement_date():
    rows = _parse_6k_transition_text(
        "On July 31, 2020, the Company announced its quarterly results. "
        "On July 15, 2020, the Company effected a change of the ratio of its ADSs to ordinary "
        "shares from one ADS representing twenty-five ordinary shares to one ADS "
        "representing five ordinary shares."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(5, 1, "2020-07-15")}
    assert {row["available_on"] for row in rows} == {"2020-08-02"}


def test_announcement_on_date_alone_does_not_date_a_proposed_ratio_change():
    assert _parse_6k_transition_text(
        "On July 31, 2020, the Company announced a proposed change of the ratio of its ADSs "
        "to ordinary shares from one ADS representing twenty-five ordinary shares "
        "to one ADS representing five ordinary shares."
    ) == []


def _parse_f6_beneficial_ownership_entitlements(text, *, filing_date="2020-10-26"):
    return parse_filing(
        text, cik=1477641, form_type="F-6 POS", accession_number="0001193805-20-001310",
        filing_date=filing_date, source_url="https://www.sec.gov/Archives/test",
    )


def test_expired_f6_prior_entitlement_is_not_revived_at_later_publication():
    rows = _parse_f6_beneficial_ownership_entitlements(
        '"Shares" mean the ordinary shares of the Company. '
        'Prior to November 17, 2020 each "ADS" evidenced by an ADR represents the right to receive, '
        "and to exercise the beneficial ownership interests in, twenty five Shares that are on "
        "deposit with the Depositary and commencing on November 17, 2020 each ADS evidenced by "
        "an ADR represents the right to receive, and to exercise the beneficial ownership "
        "interests in, five Shares that are on deposit with the Depositary.",
        filing_date="2020-11-23",
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(5, 1, "2020-11-17")}
    assert {row["available_on"] for row in rows} == {"2020-11-24"}


def test_conflicting_f6_commencing_dates_do_not_fall_back_to_publication():
    rows = _parse_f6_beneficial_ownership_entitlements(_GATE_CONFLICTING_F6_INTERVALS)
    former = [row for row in rows if row["ratio_numerator"] == 25]
    assert {(row["effective_from"], row["effective_to"]) for row in former} == {
        ("2020-10-27", "2020-11-17"),
    }
    conflict = [row for row in rows if row["ratio_numerator"] == 5]
    assert conflict and all(row["operative_date_conflict"] is True for row in conflict)
    assert {tuple(row["operative_date_candidates"]) for row in conflict} == {("2020-11-17", "2020-11-18")}
    assert all(row["effective_from"] == "2020-11-17" and row["effective_date_explicit"] for row in conflict)
    late = _parse_f6_beneficial_ownership_entitlements(_GATE_CONFLICTING_F6_INTERVALS, filing_date="2020-11-23")
    assert late and all(row["ratio_numerator"] == 5 and row["operative_date_conflict"] for row in late)
    assert {row["available_on"] for row in late} == {"2020-11-24"}


@pytest.mark.parametrize("share_definition", [
    '"Shares" mean the preferred shares of the Company.',
    "\"Shares\" mean units of the Company's investment trust.",
])
def test_f6_beneficial_ownership_of_nonordinary_units_does_not_emit_an_ordinary_ratio(share_definition):
    rows = _parse_f6_beneficial_ownership_entitlements(
        share_definition + ' Prior to November 17, 2020 each "ADS" evidenced by an ADR represents '
        "the right to receive, and to exercise the beneficial ownership interests in, twenty five "
        "Shares that are on deposit with the Depositary and commencing on November 17, 2020 "
        "each ADS evidenced by an ADR represents the right to receive, and to exercise the "
        "beneficial ownership interests in, five Shares that are on deposit with the Depositary."
    )
    assert not [row for row in rows if row["evidence_kind"] == "ads_ratio" and row["ordinary_candidate"]]


@pytest.mark.parametrize("name", [
    "pldt_preferred_gds_deposit_definition",
    "televisa_cpo_gds_deposit_definition",
    "televisa_2007_cpo_gds_deposit_definition",
])
def test_original_nonordinary_f6_deposited_units_do_not_become_ordinary_ratios(name):
    assert _parse_independent_listing_fixture(name) == []


def test_original_tal_f6_generic_share_alias_retains_its_class_a_common_unit():
    rows = _parse_independent_listing_fixture("tal_class_a_f6_deposit_definition")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["underlying_class"],
             row["ordinary_candidate"], row["effective_from"]) for row in rows} == {
        (1, 3, "class_a", True, "2017-07-29"),
    }
    assert all("class a" in row["evidence_text"].lower() for row in rows)


@pytest.mark.parametrize("entitlement", [
    "Each ADS represents five Shares.",
    "Each ADS represents the right to receive five Shares.",
    "American Depositary Shares, each representing five Shares.",
    "Five Shares to one ADS.",
])
def test_f6_preferred_share_definition_governs_every_entitlement_pattern(entitlement):
    assert _parse_f6_beneficial_ownership_entitlements(
        'Section 1.20. "Shares" shall mean the preferred shares of the Company. ' + entitlement
    ) == []


def test_f6_ordinary_program_survives_unrelated_preferred_capital_and_conversion_terms():
    rows = _parse_f6_beneficial_ownership_entitlements(
        'The Company has Class C preferred shares in its capital structure. '
        'Section 1.20. "Shares" shall mean the Class A common shares of the Company. '
        'Each ADS represents five Shares. '
        'A separate conversion provision defines "Class C Shares" as preferred shares.'
    )
    assert {(row["ratio_numerator"], row["underlying_class"], row["ordinary_candidate"])
            for row in rows} == {(5, "class_a", True)}


def test_f6_qualified_class_a_definition_does_not_retype_the_class_c_preferred_receipt():
    assert _parse_f6_beneficial_ownership_entitlements(
        'Section 1.16. "Class A Shares" shall mean the Company\'s Class A ordinary shares. '
        'AMERICAN DEPOSITARY RECEIPT FOR AMERICAN DEPOSITARY SHARES representing '
        'DEPOSITED CLASS C-2 PREFERRED SHARES of the Company. Each ADS represents one Share.'
    ) == []


def test_source_local_new_ordinary_program_definition_replaces_earlier_preferred_program():
    rows = _parse_f6_beneficial_ownership_entitlements(
        'PREFERRED PROGRAM DEPOSIT AGREEMENT. "Shares" mean preferred shares of the Company. '
        'American Depositary Shares representing DEPOSITED PREFERRED SHARES of the Company. '
        'Each ADS represents one Share. '
        'ORDINARY PROGRAM DEPOSIT AGREEMENT. "Shares" mean Class A common shares of the Company. '
        'Each ADS represents five Shares.'
    )
    assert {(row["ratio_numerator"], row["underlying_class"], row["ordinary_candidate"])
            for row in rows} == {(5, "class_a", True)}


def test_preferred_global_receipt_does_not_remove_the_american_common_receipt_in_same_source():
    rows = _parse_f6_beneficial_ownership_entitlements(
        'Each Global Depositary Share represents one share of Series III Convertible Preferred Stock. '
        'Each American Depositary Share represents one common share.'
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["ordinary_candidate"])
            for row in rows} == {(1, 1, True)}
    assert len(rows) == 1


def test_primary_cpo_unit_does_not_borrow_its_nested_common_component_count():
    assert _parse_f6_beneficial_ownership_entitlements(
        'Each Global Depositary Share represents five CPOs, each representing twenty five '
        'Series A common shares and thirty five Series D preferred shares.'
    ) == []


def test_mixed_common_and_preferred_receipt_is_not_a_scalar_common_share_ratio():
    assert _parse_f6_beneficial_ownership_entitlements(
        'Each ADS represents fifty five common shares, no par value, '
        'and fifty preferred shares, no par value, of the Company.'
    ) == []


def test_named_preferred_class_definition_does_not_retype_a_separate_common_class():
    rows = _parse_f6_beneficial_ownership_entitlements(
        'The Company has Class B Preferred Shares and Class A Common Shares outstanding. '
        'Each ADS represents one thousand Class B Shares. '
        'Each ADS represents five Class A common shares.'
    )
    assert {(row["ratio_numerator"], row["underlying_class"], row["ordinary_candidate"])
            for row in rows} == {(5, "class_a", True)}


def test_item_12d_bare_class_uses_the_same_sources_governing_unit_definition():
    rows = parse_filing(
        'Securities registered pursuant to Section 12(b) of the Act: '
        'American Depositary Shares New York Stock Exchange. '
        'The outstanding capital includes Class B Preferred Shares. '
        'TABLE OF CONTENTS. PART I. Item 12. Description of Securities Other Than Equity Securities. '
        'D. American Depositary Shares. Each ADS represents one thousand Class B Shares. '
        'Item 13. Defaults, Dividend Arrearages and Delinquencies.',
        cik=1041792, form_type="20-F", accession_number="0001292814-07-002876",
        filing_date="2007-10-15", source_url="https://www.sec.gov/Archives/test",
    )
    assert not [row for row in rows if row["evidence_kind"] == "ads_ratio"]


def test_original_copel_bare_class_b_cover_uses_its_explicit_preferred_definition():
    rows = _parse_independent_listing_fixture("copel_2007_common_preferred_ads_cover")
    assert any(row["evidence_kind"] == "listed_type" for row in rows)
    assert not [row for row in rows if row["evidence_kind"] == "ads_ratio"]


def test_original_santander_mixed_receipt_does_not_turn_its_common_component_into_a_ratio():
    rows = _parse_independent_listing_fixture("santander_2012_basket_unit_ads_cover")
    assert not [row for row in rows if row["evidence_kind"] == "ads_ratio"]


def test_original_abbey_ordinary_ratio_retains_its_actual_12g_registration_scope():
    rows = _parse_independent_listing_fixture("abbey_2004_12g_ordinary_ratio")
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["ordinary_candidate"])
            for row in ratios} == {(2, 1, True)}
    assert all(row["evidence_location"].startswith("cover/section-12g;") for row in ratios)
    assert not [row for row in rows if row["evidence_kind"] == "listed_type" and row["ordinary_candidate"]]


def test_ratio_transition_accepts_equivalent_numeric_then_spelled_parenthetical():
    rows = _parse_6k_transition_text(
        "Effective November 7, 2008, the ratio of one (1) ADS representing one (1) ordinary "
        "share will change to one (1) ADS representing 20 (twenty) ordinary shares."
    )
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in rows} == {(20, 1, "2008-11-07")}


def test_ratio_transition_does_not_resolve_disagreeing_numeric_and_spelled_quantities():
    assert _parse_6k_transition_text(
        "Effective November 7, 2008, the ratio of one (1) ADS representing one (1) ordinary "
        "share will change to one (1) ADS representing 20 (thirty) ordinary shares."
    ) == []


def test_original_rbs_numeric_parenthetical_transition_retains_only_the_new_ordinary_ratio():
    rows = _parse_independent_listing_fixture("rbs_2008_numeric_parenthetical_ratio_change")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"],
             row["ordinary_candidate"]) for row in rows} == {(20, 1, "2008-11-07", True)}
    assert {row["available_on"] for row in rows} == {"2008-11-04"}


# Gate regressions exercise parsed sources through SQL rather than silently
# accepting a dropped entitlement or an invented filing-plus-one start date.
_GATE_CONFLICTING_F6_INTERVALS = (
    '"Shares" mean the ordinary shares of the Company. '
    'Prior to November 17, 2020 each "ADS" evidenced by an ADR represents the right to receive, '
    "and to exercise the beneficial ownership interests in, twenty five Shares that are on "
    "deposit with the Depositary and commencing on November 17, 2020 each ADS evidenced by "
    "an ADR represents the right to receive, and to exercise the beneficial ownership "
    "interests in, five Shares that are on deposit with the Depositary. "
    "Commencing on November 18, 2020 each ADS evidenced by an ADR represents the right to receive, "
    "and to exercise the beneficial ownership interests in, five Shares that are on deposit "
    "with the Depositary."
)
_GATE_CONDITIONAL_NOTICE = (
    "The Company will change the ratio of its American Depositary Shares to ordinary shares "
    "from one ADS representing one ordinary share to one ADS representing twenty ordinary shares, "
    "effective on November 4, 2022, subject to shareholder approval which has not yet been obtained."
)
_GATE_PENDING_F6 = (
    "Effective on the date announced by the Depositary, each ADS shall represent twenty ordinary shares."
)


def _parse_gate_source(text, *, form, filed, adsh="0001193125-22-000123"):
    return parse_filing(
        text, cik=1046179, form_type=form, accession_number=adsh,
        filing_date=filed, source_url="https://www.sec.gov/Archives/edgar/data/1046179/" + adsh.replace("-", "") + "/gate.htm",
        symbols=["TSM"],
    )


def _insert_gate_parsed_rows(db, rows, package):
    from psycopg import sql
    ids = []
    for parsed in rows:
        row = {**parsed, "fact_hash": uuid4().hex, "loaded_on": "2026-10-09", "source_package": package}
        statement = sql.SQL("INSERT INTO public.sec_foreign_listing_evidence ({}) VALUES ({}) RETURNING id").format(
            sql.SQL(",").join(map(sql.Identifier, row)),
            sql.SQL(",").join(sql.Placeholder() for _ in row),
        )
        ids.append(db.execute(statement, list(row.values())).fetchone()[0])
    return ids


def test_gate_contradictory_operative_dates_preserve_new_source_ambiguity(db):
    add(db, filed="2020-01-01")
    add_ratio(db, (25, 1), filed="2019-01-01")
    add_ratio(db, (25, 1), source="cover_footnote", filed="2020-01-01")
    rows = _parse_gate_source(_GATE_CONFLICTING_F6_INTERVALS, form="F-6 POS", filed="2020-11-23",
                              adsh="0001193125-20-000123")
    ids = _insert_gate_parsed_rows(db, rows, "conflicting-new-registration")
    assert resolve(db, "2020-11-23")[:4] == ("resolved", "ads", 25, 1)
    for day in ("2020-11-24", "2020-12-01", "2025-12-31"):
        answer = resolve(db, day)
        assert answer[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
        assert set(ids).intersection(answer[-1])
    conflict = [row for row in rows if row.get("operative_date_conflict")]
    assert conflict
    assert {tuple(row["operative_date_candidates"]) for row in conflict} == {("2020-11-17", "2020-11-18")}
    assert all(row["effective_date_explicit"] and row["effective_from"] == "2020-11-17" for row in conflict)


@pytest.mark.parametrize("new_annual_corroboration", [False, True])
def test_gate_conflicting_february_dates_never_activate_on_filing_plus_one(db, new_annual_corroboration):
    add(db, filed="2024-01-01")
    add_ratio(db, (1, 1), filed="2023-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2024-01-01")
    if new_annual_corroboration:
        add(db, filed="2025-02-12")
        add_ratio(db, (20, 1), source="cover_footnote", filed="2025-02-12")
    rows = _parse_gate_source(
        "Each ADS represents twenty ordinary shares, effective as of the open of trading on February 18, 2025. "
        "The ratio change is effective at the close of trading on February 19, 2025.",
        form="F-6 POS", filed="2025-02-12", adsh="0001193125-25-000123",
    )
    ids = _insert_gate_parsed_rows(db, rows, "conflicting-market-session-registration")
    assert resolve(db, "2025-02-12")[:4] == ("resolved", "ads", 1, 1)
    for day in ("2025-02-13", "2025-02-17"):
        answer = resolve(db, day)
        expected = (("ambiguous", "ads", None, None, "resolved", "ambiguous")
                    if new_annual_corroboration else ("resolved", "ads", 1, 1, "resolved", "resolved"))
        assert answer[:6] == expected
    for day in ("2025-02-18", "2025-02-19", "2025-02-20"):
        assert resolve(db, day)[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
    assert ids
    assert all(row.get("operative_date_conflict") is True for row in rows)
    assert {tuple(row["operative_date_candidates"]) for row in rows} == {("2025-02-18", "2025-02-19")}
    assert {row["effective_from"] for row in rows} == {"2025-02-18"}


def test_gate_conditional_notice_cannot_activate_pending_f6_without_public_confirmation(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    conditional_rows = _parse_gate_source(_GATE_CONDITIONAL_NOTICE, form="6-K", filed="2022-10-18")
    conditional = _insert_gate_parsed_rows(db, conditional_rows, "conditional-ratio-announcement")
    pending_rows = _parse_gate_source(_GATE_PENDING_F6, form="F-6 POS", filed="2022-10-24",
                                     adsh="0001193125-22-000124")
    pending = _insert_gate_parsed_rows(db, pending_rows, "pending-ratio-registration")
    assert conditional and pending
    for day in ("2022-10-19", "2022-10-25", "2022-11-03"):
        answer = resolve(db, day)
        assert answer[:4] == ("resolved", "ads", 1, 1)
        assert not set(pending).intersection(answer[-1])
    for day in ("2022-11-04", "2022-11-10"):
        answer = resolve(db, day)
        assert answer[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
        assert not set(pending).intersection(answer[-1])
    confirmation_rows = _parse_gate_source(
        "The shareholders approved the proposed ADS ratio change. On November 4, 2022, the Company "
        "completed the change of the ratio of its ADSs to ordinary shares from one ADS representing "
        "one ordinary share to one ADS representing twenty ordinary shares.",
        form="6-K", filed="2022-11-10", adsh="0001193125-22-000125",
    )
    confirmation = _insert_gate_parsed_rows(db, confirmation_rows, "public-ratio-confirmation")
    assert confirmation
    assert resolve(db, "2022-11-10")[0] == "ambiguous"
    answer = resolve(db, "2022-11-11")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert set(pending).intersection(answer[-1]) and set(confirmation).intersection(answer[-1])
    assert all(row.get("ratio_effectiveness_pending") is True for row in conditional_rows)
    assert all(row.get("ratio_effectiveness_confirmed") is True for row in confirmation_rows)


def test_gate_ante_conditional_meeting_notice_needs_confirmation_and_keeps_valid_event(db):
    add(db, cik=1413745, symbol="ANTE", filed="2022-05-01")
    add_ratio(db, (10, 1), cik=1413745, symbol="ANTE", filed="2019-01-01")
    add_ratio(db, (10, 1), cik=1413745, symbol="ANTE", source="cover_footnote", filed="2022-05-01")
    _insert_dated_ratio_source_fixture(db, "ante_2022_pending_f6_ratio")
    _insert_dated_ratio_source_fixture(db, "ante_2022_consolidation_notice")
    _insert_dated_ratio_source_fixture(db, "ante_2022_consolidation_meeting_notice")
    assert resolve(db, "2022-12-08", cik=1413745, symbol="ANTE")[:4] == ("resolved", "ads", 10, 1)
    assert resolve(db, "2022-12-09", cik=1413745, symbol="ANTE")[0] == "ambiguous"
    confirmation = _insert_dated_ratio_source_fixture(db, "ante_2022_consolidation_confirmation")
    assert resolve(db, "2022-11-30", cik=1413745, symbol="ANTE")[:4] == ("resolved", "ads", 10, 1)
    assert resolve(db, "2022-12-08", cik=1413745, symbol="ANTE")[:4] == ("resolved", "ads", 10, 1)
    for day in ("2022-12-09", "2022-12-12", "2025-12-31"):
        answer = resolve(db, day, cik=1413745, symbol="ANTE")
        assert answer[:4] == ("resolved", "ads", 1, 1)
        assert set(confirmation).intersection(answer[-1])


@pytest.mark.parametrize("unrelated_statement", [
    "The shareholders approved an acquisition of a subsidiary.",
    "The Board approved the Company's annual dividend.",
    "The ADS trading price is expected to change on November 4, 2022.",
])
def test_gate_unrelated_approval_or_trading_expectation_does_not_confirm_ratio(unrelated_statement):
    rows = _parse_gate_source(unrelated_statement + " " + _GATE_CONDITIONAL_NOTICE,
                              form="6-K", filed="2022-10-18")
    assert rows
    assert all(row.get("ratio_effectiveness_pending") is True for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


def test_gate_later_proposal_remains_conditional_instead_of_becoming_confirmation():
    rows = _parse_gate_source(
        "On November 10, 2022, the Company announced that it still proposes to change the ADS "
        "ratio from one ADS representing one ordinary share to one ADS representing twenty "
        "ordinary shares, effective November 4, 2022, subject to shareholder approval.",
        form="6-K", filed="2022-11-10",
    )
    assert rows and all(row.get("ratio_effectiveness_pending") is True for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


@pytest.mark.parametrize("field,value", [
    ("symbol", "OTHER"), ("underlying_class", "class_b"),
    ("ratio_change_program_key", "old_cusip:987654321"),
    ("effective", "2022-12-01"), ("ratio", (40, 1)),
])
def test_gate_unrelated_confirmation_cannot_clear_conditional_program(db, field, value):
    add(db, filed="2022-01-01", underlying_class="class_a")
    add_ratio(db, (1, 1), filed="2021-01-01", underlying_class="class_a")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01", underlying_class="class_a")
    pending = add_ratio(db, (20, 1), filed="2022-10-24", symbol=None, underlying_class="class_a",
                        ratio_effectiveness_pending=True,
                        ratio_effectiveness_pending_text="Effective on the date announced by the Depositary.")
    conditional = add_ratio(db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-10-18",
                            effective="2022-11-04", symbol="TSM", underlying_class="class_a",
                            ratio_change_program_key="old_cusip:123456789",
                            ratio_effectiveness_pending=True,
                            ratio_effectiveness_pending_text="Subject to shareholder approval.",
                            ratio_effectiveness_conditions=["shareholder_approval"])
    kwargs = {"ratio": (20, 1), "source": "ratio_change_6k", "form": "6-K", "filed": "2022-11-10",
              "effective": "2022-11-04", "symbol": "TSM", "underlying_class": "class_a",
              "ratio_change_program_key": "old_cusip:123456789",
              "adsh": "0001193125-22-000110", "ratio_effectiveness_confirmed": True,
              "ratio_effectiveness_confirmation_text": "The shareholders approved and completed this ADS ratio change.",
              "ratio_effectiveness_confirmed_conditions": ["ratio_effective"]}
    kwargs[field] = value
    add_ratio(db, **kwargs)
    answer = resolve(db, "2022-11-11")
    assert answer[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
    assert conditional in answer[-1] and pending not in answer[-1]


@pytest.mark.parametrize("metadata", [
    {"operative_date_candidates": ["2020-11-17"]},
    {"operative_date_candidates": ["2020-11-17", "2020-11-17"]},
    {"operative_date_candidates": ["2020-11-17", None]},
    {"operative_date_conflict_text": ""},
    {"effective_date_explicit": False},
    {"operative_date_conflict": False},
])
def test_gate_schema_rejects_incomplete_conflicting_date_metadata(db, metadata):
    import psycopg
    kwargs = {"operative_date_conflict": True,
              "operative_date_candidates": ["2020-11-17", "2020-11-18"],
              "operative_date_conflict_text": "Commencing November 17; commencing November 18.",
              "effective_date_explicit": True}
    kwargs.update(metadata)
    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction():
            add_ratio(db, (5, 1), effective="2020-11-17", **kwargs)


@pytest.mark.parametrize("metadata", [
    {"ratio_effectiveness_confirmation_text": ""},
    {"ratio_effectiveness_confirmed": False},
    {"effective_date_explicit": False},
    {"ratio_effectiveness_pending": True, "ratio_effectiveness_pending_text": "Subject to approval."},
    {"operative_date_conflict": True, "operative_date_candidates": ["2022-11-04", "2022-11-05"],
     "operative_date_conflict_text": "Effective November 4; effective November 5."},
])
def test_gate_schema_rejects_unproved_or_unsettled_confirmation_metadata(db, metadata):
    import psycopg
    kwargs = {"ratio_effectiveness_confirmed": True,
              "ratio_effectiveness_confirmation_text": "The ADS ratio change was completed on November 4.",
              "effective_date_explicit": True,
              "ratio_effectiveness_confirmed_conditions": ["ratio_effective"]}
    kwargs.update(metadata)
    with pytest.raises(psycopg.errors.CheckViolation):
        with db.transaction():
            add_ratio(db, (20, 1), source="ratio_change_6k", form="6-K", effective="2022-11-04", **kwargs)



def _gate_conflicted_f6_registration(db):
    return add_ratio(
        db, (20, 1), filed="2025-02-12", form="F-6 POS", effective="2025-02-18",
        symbol=None, adsh="0001193125-25-000123", operative_date_conflict=True,
        operative_date_candidates=["2025-02-18", "2025-02-19"],
        operative_date_conflict_text="This ADS ratio is effective February 18 and effective February 19.",
    )


def _gate_old_ratio_pair(db):
    add(db, filed="2024-01-01")
    add_ratio(db, (1, 1), filed="2023-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2024-01-01")


def test_gate_later_single_date_confirmation_recovers_only_conflicted_f6_entitlement(db):
    _gate_old_ratio_pair(db)
    conflict = _gate_conflicted_f6_registration(db)
    assert resolve(db, "2025-02-17")[:4] == ("resolved", "ads", 1, 1)
    assert resolve(db, "2025-02-18")[0] == "ambiguous"
    confirmation = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2025-02-21",
        effective="2025-02-19", symbol="TSM", adsh="0001193125-25-000456", ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="The Company completed this ADS ratio change on February 19.",
        ratio_effectiveness_confirmed_conditions=["ratio_effective"],
    )
    assert resolve(db, "2025-02-21")[0] == "ambiguous"
    answer = resolve(db, "2025-02-22")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert conflict in answer[-1] and confirmation in answer[-1]
    original = db.execute(
        "SELECT operative_date_conflict,operative_date_candidates,effective_from "
        "FROM public.sec_foreign_listing_evidence WHERE id=%s", [conflict],
    ).fetchone()
    assert original == (True, [dt.date(2025, 2, 18), dt.date(2025, 2, 19)], dt.date(2025, 2, 18))


def test_gate_definitive_future_confirmation_respects_publication_and_actual_date(db):
    _gate_old_ratio_pair(db)
    conflict = _gate_conflicted_f6_registration(db)
    confirmation = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2025-02-21",
        publication_floor_on="2025-02-25", effective="2025-03-01", symbol="TSM", adsh="0001193125-25-000789",
        ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="The Depositary definitively announced this ADS ratio effective March 1.",
        ratio_effectiveness_confirmed_conditions=["depositary_notice"],
    )
    for day in ("2025-02-18", "2025-02-22", "2025-02-24"):
        assert resolve(db, day)[0] == "ambiguous"
    for day in ("2025-02-25", "2025-02-28"):
        answer = resolve(db, day)
        assert answer[:4] == ("ambiguous", "ads", None, None)
        assert conflict in answer[-1] and confirmation in answer[-1]
    answer = resolve(db, "2025-03-01")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert conflict in answer[-1] and confirmation in answer[-1]



def test_gate_unflagged_fee_in_same_registration_cannot_bypass_date_conflict(db):
    _gate_old_ratio_pair(db)
    conflict = _gate_conflicted_f6_registration(db)
    fee = add_ratio(db, (20, 1), filed="2025-02-12", form="F-6 POS", symbol=None,
                    adsh="0001193125-25-000123")
    for day in ("2025-02-13", "2025-02-17"):
        answer = resolve(db, day)
        assert answer[:4] == ("resolved", "ads", 1, 1)
        assert fee not in answer[-1]
    for day in ("2025-02-18", "2025-02-19"):
        answer = resolve(db, day)
        assert answer[0] == "ambiguous" and conflict in answer[-1]


@pytest.mark.parametrize("end_field", ["until", "retired"])
def test_gate_expired_or_retired_confirmation_cannot_clear_conditional_notice(db, end_field):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    pending = add_ratio(db, (20, 1), filed="2022-10-24", symbol=None,
                        ratio_effectiveness_pending=True,
                        ratio_effectiveness_pending_text="Effective on the date announced by the Depositary.")
    conditional = add_ratio(db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-10-18",
                            effective="2022-11-04", ratio_effectiveness_pending=True,
                            ratio_effectiveness_pending_text="Subject to shareholder approval.",
                            ratio_effectiveness_conditions=["shareholder_approval"])
    confirmation = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-11-10", effective="2022-11-04",
        adsh="0001193125-22-000110", ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="This ADS ratio change was completed on November 4.",
        ratio_effectiveness_confirmed_conditions=["ratio_effective"],
        **{end_field: "2022-11-12"},
    )
    answer = resolve(db, "2022-11-13")
    assert answer[0] == "ambiguous" and conditional in answer[-1]
    assert pending not in answer[-1] and confirmation not in answer[-1]



def test_gate_shareholder_approval_cannot_clear_outstanding_regulatory_condition(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    pending = add_ratio(db, (20, 1), filed="2022-10-24", symbol=None,
                        ratio_effectiveness_pending=True,
                        ratio_effectiveness_pending_text="Effective on the date announced by the Depositary.",
                        ratio_effectiveness_conditions=["depositary_notice"])
    conditional = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-10-18", effective="2022-11-04",
        ratio_effectiveness_pending=True, ratio_effectiveness_pending_text="Subject to SEC approval.",
        ratio_effectiveness_conditions=["regulatory_approval"],
    )
    add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-11-10", effective="2022-11-04",
        adsh="0001193125-22-000110", ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="The shareholders approved the proposed ADS ratio change.",
        ratio_effectiveness_confirmed_conditions=["shareholder_approval"],
    )
    answer = resolve(db, "2022-11-11")
    assert answer[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
    assert conditional in answer[-1] and pending not in answer[-1]
    completed = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-11-12", effective="2022-11-04",
        adsh="0001193125-22-000112", ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="The Company completed this ADS ratio change on November 4.",
        ratio_effectiveness_confirmed_conditions=["ratio_effective"],
    )
    assert resolve(db, "2022-11-12")[0] == "ambiguous"
    answer = resolve(db, "2022-11-13")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert pending in answer[-1] and completed in answer[-1]



def test_gate_explicitly_bound_fee_cannot_bypass_issuer_scoped_same_registration_conflict(db):
    add(db, symbol="XYZ", filed="2024-01-01")
    add_ratio(db, (1, 1), symbol=None, filed="2023-01-01")
    add_ratio(db, (1, 1), symbol="XYZ", source="cover_footnote", filed="2024-01-01")
    add(db, symbol="XYZ", filed="2025-02-12")
    add_ratio(db, (20, 1), symbol="XYZ", source="cover_footnote", filed="2025-02-12")
    conflict = _gate_conflicted_f6_registration(db)
    fee = add_ratio(db, (20, 1), filed="2025-02-12", form="F-6 POS", symbol="XYZ",
                    adsh="0001193125-25-000123")
    assert resolve(db, "2025-02-12", symbol="XYZ")[:4] == ("resolved", "ads", 1, 1)
    for day in ("2025-02-13", "2025-02-17", "2025-02-18", "2025-02-19"):
        answer = resolve(db, day, symbol="XYZ")
        assert answer[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
        if day < "2025-02-18":
            assert fee not in answer[-1]
        else:
            assert conflict in answer[-1]


@pytest.mark.parametrize("same_accession", [False, True])
def test_gate_future_conditional_event_does_not_suppress_completed_same_ratio_regime(db, same_accession):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    pending = add_ratio(db, (20, 1), filed="2022-10-24", symbol=None,
                        ratio_effectiveness_pending=True,
                        ratio_effectiveness_pending_text="Effective on the date announced by the Depositary.")
    completed = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-11-10", effective="2022-11-04",
        adsh="0001193125-22-000111", ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="This ADS ratio change was completed on November 4.",
        ratio_effectiveness_confirmed_conditions=["ratio_effective"],
    )
    future = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-11-10" if same_accession else "2022-11-18",
        effective="2022-12-01", adsh="0001193125-22-000111" if same_accession else "0001193125-22-000222",
        ratio_effectiveness_pending=True, ratio_effectiveness_pending_text="The later ADS event remains subject to SEC approval.",
        ratio_effectiveness_conditions=["regulatory_approval"],
    )
    for day in ("2022-11-11", "2022-11-19", "2022-11-30"):
        answer = resolve(db, day)
        assert answer[:4] == ("resolved", "ads", 20, 1)
        assert pending in answer[-1] and completed in answer[-1] and future not in answer[-1]
    answer = resolve(db, "2022-12-01")
    assert answer[0] == "ambiguous" and future in answer[-1]



@pytest.mark.parametrize("source", ["f6", "ratio_change_6k"])
def test_gate_later_authoritative_earlier_date_settles_conflict_without_history_backfill(db, source):
    _gate_old_ratio_pair(db)
    conflict = _gate_conflicted_f6_registration(db)
    kwargs = {}
    if source == "f6":
        # Another registration settles its source clock but still needs an
        # independent matching annual/change stream to resolve the ratio.
        add(db, filed="2025-02-23")
        add_ratio(db, (20, 1), source="cover_footnote", filed="2025-02-23")
    if source == "ratio_change_6k":
        kwargs = {"ratio_effectiveness_confirmed": True,
                  "ratio_effectiveness_confirmation_text": "This ADS ratio change was actually completed on February 17.",
                  "ratio_effectiveness_confirmed_conditions": ["ratio_effective"]}
    settled = add_ratio(
        db, (20, 1), source=source, form="F-6 POS" if source == "f6" else "6-K",
        filed="2025-02-21", publication_floor_on="2025-02-24", effective="2025-02-17",
        adsh="0001193125-25-000456", symbol="TSM", **kwargs,
    )
    assert resolve(db, "2025-02-17")[:4] == ("resolved", "ads", 1, 1)
    for day in ("2025-02-18", "2025-02-22", "2025-02-23"):
        assert resolve(db, day)[0] == "ambiguous"
    answer = resolve(db, "2025-02-24")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert conflict in answer[-1] and settled in answer[-1]
    assert resolve(db, "2025-02-17")[:4] == ("resolved", "ads", 1, 1)
    original = db.execute("SELECT operative_date_candidates,effective_from FROM public.sec_foreign_listing_evidence WHERE id=%s", [conflict]).fetchone()
    assert original == ([dt.date(2025, 2, 18), dt.date(2025, 2, 19)], dt.date(2025, 2, 18))


def test_gate_current_cover_ratio_does_not_borrow_conflicting_historical_transition_dates():
    content = """<p>Securities registered pursuant to Section 12(b) of the Act:</p>
    <table><tr><th>Title of each class</th><th>Trading symbol</th><th>Exchange</th></tr>
    <tr><td>American Depositary Shares, each representing ten common shares</td>
    <td>EDU</td><td>New York Stock Exchange</td></tr></table>
    <p>Effective on August 18, 2011, the Company changed the ADS ratio from one ADS representing
    four common shares to one ADS representing one common share.</p>
    <p>Effective on April 8, 2022, the Company changed the ADS ratio from one ADS representing
    one common share to one ADS representing ten common shares.</p>"""
    rows = parse_filing(content, cik=1372920, form_type="20-F", accession_number="0001193125-23-000123",
                        filing_date="2023-09-29", source_url="https://www.sec.gov/Archives/test", symbols=["EDU"])
    ratios = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"])
            for row in ratios} == {(10, 1, "2023-09-30"), (1, 1, "2011-08-18"), (10, 1, "2022-04-08")}
    assert not any(row.get("operative_date_conflict") for row in ratios)



def test_gate_plain_later_registration_cannot_prove_conditional_announcement_approval(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    pending = add_ratio(db, (20, 1), filed="2022-10-24", symbol=None,
                        ratio_effectiveness_pending=True,
                        ratio_effectiveness_pending_text="Effective on the date announced by the Depositary.")
    conditional = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-10-18", effective="2022-11-04",
        ratio_effectiveness_pending=True, ratio_effectiveness_pending_text="Subject to SEC approval.",
        ratio_effectiveness_conditions=["regulatory_approval"],
    )
    add_ratio(db, (20, 1), filed="2022-11-10", effective="2022-11-04", form="F-6 POS",
              adsh="0001193125-22-000999")
    answer = resolve(db, "2022-11-11")
    assert answer[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
    assert conditional in answer[-1] and pending not in answer[-1]



@pytest.mark.parametrize("clause,category", [
    ("If approved,", "other_approval"),
    ("When shareholders approve the proposed change,", "shareholder_approval"),
    ("Once shareholder approval is obtained,", "shareholder_approval"),
])
def test_gate_future_approval_grammar_marks_ratio_pending(clause, category):
    rows = _parse_gate_source(
        clause + " the Company will change the ADS ratio from one ADS representing one ordinary "
        "share to one ADS representing twenty ordinary shares effective November 4, 2022.",
        form="6-K", filed="2022-10-18",
    )
    assert rows and all(row.get("ratio_effectiveness_pending") is True for row in rows)
    assert all(category in row["ratio_effectiveness_conditions"] for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


def test_gate_if_approved_announcement_stays_ambiguous_until_actual_public_completion(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    conditional_rows = _parse_gate_source(
        "If approved, the Company will change the ADS ratio from one ADS representing one "
        "ordinary share to one ADS representing twenty ordinary shares effective November 4, 2022.",
        form="6-K", filed="2022-10-18",
    )
    _insert_gate_parsed_rows(db, conditional_rows, "unknown-approval-notice")
    pending_rows = _parse_gate_source(_GATE_PENDING_F6, form="F-6 POS", filed="2022-10-24",
                                     adsh="0001193125-22-000124")
    pending = _insert_gate_parsed_rows(db, pending_rows, "unknown-approval-pending-receipt")
    assert conditional_rows and pending
    assert all(row.get("ratio_effectiveness_pending") is True for row in conditional_rows)
    assert resolve(db, "2022-11-03")[:4] == ("resolved", "ads", 1, 1)
    assert resolve(db, "2022-11-04")[0] == "ambiguous"
    completed_rows = _parse_gate_source(
        "The Company completed the change of the ADS ratio from one ADS representing one ordinary "
        "share to one ADS representing twenty ordinary shares on November 4, 2022.",
        form="6-K", filed="2022-11-10", adsh="0001193125-22-000125",
    )
    assert completed_rows and all(row.get("ratio_effectiveness_confirmed") is True for row in completed_rows)
    assert all("ratio_effective" in row["ratio_effectiveness_confirmed_conditions"] for row in completed_rows)
    completed = _insert_gate_parsed_rows(db, completed_rows, "unknown-approval-actual-completion")
    assert resolve(db, "2022-11-10")[0] == "ambiguous"
    answer = resolve(db, "2022-11-11")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert set(pending).intersection(answer[-1]) and set(completed).intersection(answer[-1])


def test_gate_actual_depositary_notice_fulfills_the_matching_f6_notice_condition():
    rows = _parse_gate_source(
        "Effective on the date announced by the Depositary, each ADS shall represent twenty ordinary shares. "
        "The Depositary has announced that this ratio change is effective on November 4, 2022.",
        form="F-6 POS", filed="2022-11-10",
    )
    assert rows
    assert all(row.get("ratio_effectiveness_confirmed") is True for row in rows)
    assert all("depositary_notice" in row["ratio_effectiveness_confirmed_conditions"] for row in rows)
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)
    assert {row["effective_from"] for row in rows} == {"2022-11-04"}


def test_gate_same_accession_cannot_self_clear_its_own_unmet_condition(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    conditional = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-10-18", effective="2022-11-04",
        adsh="0001193125-22-000123", ratio_effectiveness_pending=True,
        ratio_effectiveness_pending_text="This change remains subject to SEC approval.",
        ratio_effectiveness_conditions=["regulatory_approval"],
    )
    add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-11-10", effective="2022-11-04",
        adsh="0001193125-22-000123", ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="This change was completed on November 4.",
        ratio_effectiveness_confirmed_conditions=["ratio_effective"],
    )
    answer = resolve(db, "2022-11-11")
    assert answer[0] == "ambiguous" and conditional in answer[-1]



def test_gate_plain_dated_f6_does_not_claim_depositary_notice_or_approval_confirmation():
    rows = _parse_gate_source("Each ADS represents twenty ordinary shares, effective on November 4, 2022.",
                              form="F-6 POS", filed="2022-11-10")
    assert rows and not any(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed_conditions") for row in rows)


def test_gate_actual_shareholder_approval_result_is_not_a_future_approval_condition():
    rows = _parse_gate_source(
        "The shareholders have approved the proposed change of the ADS ratio from one ADS representing "
        "one ordinary share to one ADS representing twenty ordinary shares, effective November 4, 2022.",
        form="6-K", filed="2022-11-10",
    )
    assert rows and all(row.get("ratio_effectiveness_confirmed") is True for row in rows)
    assert all("shareholder_approval" in row["ratio_effectiveness_confirmed_conditions"] for row in rows)
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)



def test_gate_conditional_notice_bounds_fallback_registration_without_activating_it(db):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2022-10-18", effective="2022-11-04",
        adsh="0001193125-22-000018", ratio_effectiveness_pending=True,
        ratio_effectiveness_pending_text="This dated change is subject to shareholder approval.",
        ratio_effectiveness_conditions=["shareholder_approval"],
    )
    registration = add_ratio(db, (20, 1), filed="2022-10-24", form="F-6 POS", symbol=None,
                             adsh="0001193125-22-000024", effective_date_explicit=False)
    add(db, filed="2022-10-24")
    add_ratio(db, (20, 1), source="cover_footnote", filed="2022-10-24")
    assert resolve(db, "2022-10-24")[:4] == ("resolved", "ads", 1, 1)
    for day in ("2022-10-25", "2022-11-03"):
        answer = resolve(db, day)
        assert answer[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
        assert registration not in answer[-1]
    for day in ("2022-11-04", "2022-12-01"):
        assert resolve(db, day)[0] == "ambiguous"


@pytest.mark.parametrize("action", ["has not adjusted", "expects to adjust", "will adjust"])
def test_gate_negated_or_future_ratio_adjustment_is_not_actual_completion(action):
    rows = _parse_gate_source(
        f"Effective on November 4, 2022, the Company {action} the ratio of its ADSs to ordinary shares "
        "from one ADS representing one ordinary share to one ADS representing twenty ordinary shares.",
        form="6-K", filed="2022-11-10",
    )
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)



def test_gate_actual_tal_adjustment_confirms_ratio_only_at_later_publication():
    rows = _parse_independent_listing_fixture("tal_2017_completed_ratio_change")
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["underlying_class"],
             row["effective_from"], row["available_on"]) for row in rows} == {
        (1, 3, "class_a", "2017-08-16", "2017-10-28"),
    }
    assert all(row.get("ratio_effectiveness_confirmed") is True for row in rows)
    assert all("ratio_effective" in row["ratio_effectiveness_confirmed_conditions"] for row in rows)
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)
    assert all("adjusted the ratio" in row["ratio_effectiveness_confirmation_text"] for row in rows)


def test_gate_actual_tal_proposal_stays_uncertain_until_completed_ratio_is_public(db):
    add(db, cik=1499620, symbol="TAL", filed="2017-05-01", underlying_class="class_a")
    add_ratio(db, (2, 1), cik=1499620, symbol="TAL", filed="2016-01-01", underlying_class="class_a")
    add_ratio(db, (2, 1), cik=1499620, symbol="TAL", source="cover_footnote", filed="2017-05-01",
              underlying_class="class_a")
    _insert_gate_parsed_rows(db, _parse_independent_listing_fixture("tal_class_a_f6_deposit_definition"),
                             "tal-original-f6-registration")
    proposal_rows = _parse_independent_listing_fixture("tal_2017_class_a_ratio_change")
    assert proposal_rows and all(row.get("ratio_effectiveness_pending") for row in proposal_rows)
    _insert_gate_parsed_rows(db, proposal_rows, "tal-conditional-regulatory-proposal")
    assert resolve(db, "2017-08-15", cik=1499620, symbol="TAL")[:4] == ("resolved", "ads", 2, 1)
    for day in ("2017-08-16", "2017-10-27"):
        assert resolve(db, day, cik=1499620, symbol="TAL")[0] == "ambiguous"
    completed = _insert_gate_parsed_rows(db, _parse_independent_listing_fixture("tal_2017_completed_ratio_change"),
                                         "tal-actual-completed-ratio-publication")
    assert resolve(db, "2017-10-27", cik=1499620, symbol="TAL")[0] == "ambiguous"
    answer = resolve(db, "2017-10-28", cik=1499620, symbol="TAL")
    assert answer[:4] == ("resolved", "ads", 1, 3)
    assert set(completed).intersection(answer[-1])
    assert resolve(db, "2017-08-16", cik=1499620, symbol="TAL")[0] == "ambiguous"


@pytest.mark.parametrize("future_authority", ["f6", "ratio_change_6k"])
def test_gate_later_future_authority_does_not_undo_an_established_operative_clock(db, future_authority):
    _gate_old_ratio_pair(db)
    conflict = _gate_conflicted_f6_registration(db)
    completed = add_ratio(
        db, (20, 1), source="ratio_change_6k", form="6-K", filed="2025-02-21", effective="2025-02-19",
        symbol="TSM", adsh="0001193125-25-000456", ratio_effectiveness_confirmed=True,
        ratio_effectiveness_confirmation_text="This ADS ratio change was completed on February 19.",
        ratio_effectiveness_confirmed_conditions=["ratio_effective"],
    )
    kwargs = {}
    if future_authority == "ratio_change_6k":
        kwargs = {"ratio_effectiveness_confirmed": True,
                  "ratio_effectiveness_confirmation_text": "The Depositary announced this exact ratio effective March 1.",
                  "ratio_effectiveness_confirmed_conditions": ["depositary_notice"]}
    future = add_ratio(
        db, (20, 1), source=future_authority, form="F-6 POS" if future_authority == "f6" else "6-K",
        filed="2025-02-24", effective="2025-03-01", symbol="TSM", adsh="0001193125-25-000789", **kwargs,
    )
    for day in ("2025-02-22", "2025-02-24", "2025-02-25", "2025-02-28"):
        answer = resolve(db, day)
        assert answer[:4] == ("resolved", "ads", 20, 1)
        assert conflict in answer[-1] and completed in answer[-1]
        assert future not in answer[-1]
    answer = resolve(db, "2025-03-01")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert conflict in answer[-1] and future in answer[-1]


@pytest.mark.parametrize("fixture,ratio,underlying_class,effective,available,conditions,proof_phrase", [
    ("bidu_2021_subdivision_completed_ratio", (8, 1), "class_a", "2021-03-01", "2021-03-02",
     {"ratio_effective"}, "has taken effect"),
    ("tcom_2021_subdivision_completed_ratio", (1, 1), None, "2021-03-18", "2021-03-19",
     {"shareholder_approval", "consolidation"}, "proposed resolutions submitted for shareholder approval"),
])
def test_gate_actual_subdivision_results_confirm_the_linked_ratio(
    fixture, ratio, underlying_class, effective, available, conditions, proof_phrase,
):
    rows = _parse_independent_listing_fixture(fixture)
    ratio_rows = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert ratio_rows
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["underlying_class"],
             row["effective_from"], row["available_on"]) for row in ratio_rows} == {
        (*ratio, underlying_class, effective, available),
    }
    assert all(row.get("ratio_effectiveness_confirmed") is True for row in ratio_rows)
    assert all(conditions.issubset(set(row["ratio_effectiveness_confirmed_conditions"])) for row in ratio_rows)
    assert not any(row.get("ratio_effectiveness_pending") for row in ratio_rows)
    assert all(proof_phrase.lower() in row["ratio_effectiveness_confirmation_text"].lower() for row in ratio_rows)


@pytest.mark.parametrize("fixture,ratio,underlying_class,effective,available,pending_phrase", [
    ("anpc_2022_plan_expected_ratio", (20, 1), "class_a", "2022-11-04", "2022-10-19", "plans to change"),
    ("aktx_2023_plan_expected_ratio", (2000, 1), None, "2023-08-17", "2023-08-16", "expected to be effective"),
])
def test_gate_actual_expected_ratio_plans_remain_pending_until_completion(
    fixture, ratio, underlying_class, effective, available, pending_phrase,
):
    rows = _parse_independent_listing_fixture(fixture)
    ratio_rows = [row for row in rows if row["evidence_kind"] == "ads_ratio"]
    assert ratio_rows
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["underlying_class"],
             row["effective_from"], row["available_on"]) for row in ratio_rows} == {
        (*ratio, underlying_class, effective, available),
    }
    assert all(row.get("ratio_effectiveness_pending") is True for row in ratio_rows)
    assert all(row["ratio_effectiveness_conditions"] == ["unknown_condition"] for row in ratio_rows)
    assert all(pending_phrase in row["ratio_effectiveness_pending_text"].lower() for row in ratio_rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in ratio_rows)


def test_gate_aktx_actual_plan_stays_ambiguous_until_completion_is_public(db):
    add(db, cik=1541157, symbol="AKTX", filed="2023-05-01")
    add_ratio(db, (100, 1), cik=1541157, symbol="AKTX", filed="2022-12-29", form="F-6 POS")
    add_ratio(db, (100, 1), source="cover_footnote", cik=1541157, symbol="AKTX",
              filed="2023-05-01", underlying_class=None)
    add_ratio(db, (2000, 1), cik=1541157, symbol=None, filed="2023-08-17", form="F-6 POS",
              effective="2023-08-18", effective_date_explicit=False, underlying_class=None,
              adsh="0000000000-23-000001", ratio_effectiveness_pending=True,
              ratio_effectiveness_pending_text="Effective on the date announced by the Depositary.")
    plan = _parse_independent_listing_fixture("aktx_2023_plan_expected_ratio")
    assert plan and all(row.get("ratio_effectiveness_pending") is True for row in plan)
    _insert_gate_parsed_rows(db, plan, "aktx-2023-plan-expected-ratio")
    assert resolve(db, "2023-08-16", cik=1541157, symbol="AKTX")[:4] == ("resolved", "ads", 100, 1)
    for day in ("2023-08-17", "2023-09-29"):
        answer = resolve(db, day, cik=1541157, symbol="AKTX")
        assert answer[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
    completion = _parse_independent_listing_fixture("aktx_2023_ratio_completion")
    assert completion and all(row.get("ratio_effectiveness_confirmed") is True for row in completion)
    _insert_gate_parsed_rows(db, completion, "aktx-2023-ratio-completion")
    assert resolve(db, "2023-09-29", cik=1541157, symbol="AKTX")[0] == "ambiguous"
    assert resolve(db, "2023-09-30", cik=1541157, symbol="AKTX")[:4] == ("resolved", "ads", 2000, 1)


def test_gate_contract_boilerplate_is_not_an_outstanding_condition():
    rows = _parse_gate_source(
        "The Company will change the ratio of its American Depositary Shares to ordinary shares "
        "from one ADS representing one ordinary share to one ADS representing twenty ordinary shares, "
        "effective on November 4, 2022, subject to the terms and conditions of the Deposit Agreement.",
        form="6-K", filed="2022-10-18",
    )
    assert rows
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert {row["effective_from"] for row in rows} == {"2022-11-04"}
    assert all(row["effective_date_explicit"] for row in rows)


def test_gate_unknown_closing_condition_still_marks_pending():
    rows = _parse_gate_source(
        "The Company will change the ratio of its American Depositary Shares to ordinary shares "
        "from one ADS representing one ordinary share to one ADS representing twenty ordinary shares, "
        "effective on November 4, 2022, subject to customary closing conditions.",
        form="6-K", filed="2022-10-18",
    )
    assert rows and all(row.get("ratio_effectiveness_pending") is True for row in rows)
    assert all(row["ratio_effectiveness_conditions"] == ["unknown_condition"] for row in rows)


def test_gate_pro_forma_retroactive_assumption_does_not_defer_the_announced_change():
    rows = _parse_gate_source(
        "On November 30, 2022, the Company announced its plans to change the ADS ratio from one ADS "
        "to three ordinary shares to a new ADS ratio of one ADS to thirty ordinary shares. "
        "The change in the ADS Ratio became effective on December 12, 2022. For all the periods "
        "presented, basic and diluted loss per ADS have been revised assuming the change of ADS ratio "
        "from a ratio of one ADS to three ordinary shares to a new ratio of one ADS to thirty ordinary "
        "shares occurred at the beginning of the earliest period presented.",
        form="6-K", filed="2025-03-18",
    )
    assert rows
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)
    assert any(row.get("ratio_effectiveness_confirmed") is True for row in rows)
    assert {row["effective_from"] for row in rows} == {"2022-12-12"}


def test_gate_completed_change_date_does_not_conflict_with_its_effective_date():
    rows = _parse_gate_source(
        "As previously announced, on July 31, 2024, the Company changed its ratio of its American "
        "Depositary Shares to ordinary shares from one ADS representing twenty ordinary shares to one "
        "ADS representing two hundred ordinary shares (the ADS Ratio Change). The ADS Ratio Change "
        "became effective on August 5th, 2024 (the Effective Date).",
        form="6-K", filed="2024-08-08",
    )
    assert rows
    assert not any(row.get("operative_date_conflict") for row in rows)
    assert {row["effective_from"] for row in rows} == {"2024-08-05"}
    assert all(row["effective_date_explicit"] for row in rows)
    assert all(row.get("ratio_effectiveness_confirmed") is True for row in rows)


def test_gate_conflicting_operative_dates_still_conflict_when_completion_dates_exist():
    rows = _parse_gate_source(
        "On July 31, 2024, the Company changed the ratio of its ADSs to ordinary shares from one ADS "
        "representing twenty ordinary shares to one ADS representing two hundred ordinary shares. "
        "The ADS ratio change became effective on August 5th, 2024. "
        "The ADS ratio change became effective on August 6th, 2024.",
        form="6-K", filed="2024-08-08",
    )
    assert rows
    assert all(row.get("operative_date_conflict") is True for row in rows)
    assert {tuple(row["operative_date_candidates"]) for row in rows} == {("2024-08-05", "2024-08-06")}




# B1 re-gate scenarios deliberately use synthetic filings: the object and
# identity boundaries remain independent of the production artifact's answers.
@pytest.mark.parametrize("fixture", [
    "b1_preparations_completion", "b1_different_class_confirmation", "b1_budget_approval",
])
def test_b1_regate_invalid_completion_or_approval_cannot_confirm(fixture):
    rows = _parse_gate_source((FIXTURES / (fixture + ".html")).read_text(encoding="utf-8"),
                              form="6-K", filed="2022-11-10")
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)
    if fixture != "b1_preparations_completion":
        assert rows and all(row.get("ratio_effectiveness_pending") for row in rows)


def test_b1_regate_numeric_financial_table_never_confirms_but_narrative_footnote_does():
    rows = _parse_gate_source((FIXTURES / "b1_financial_table_confirmation.html").read_text(encoding="utf-8"),
                              form="6-K", filed="2022-11-10")
    assert len(rows) == 1
    confirmed = [row for row in rows if row.get("ratio_effectiveness_confirmed")]
    assert len(confirmed) == 1
    assert confirmed[0]["ratio_effectiveness_confirmation_text"].startswith("On November 4, 2022")
    assert "Weighted average" not in confirmed[0]["ratio_effectiveness_confirmation_text"]
    assert "loss per ADS" not in confirmed[0]["ratio_effectiveness_confirmation_text"]
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)


@pytest.mark.parametrize("object_text", [
    "preparations for the ADS ratio change", "filings for the ADS ratio change",
    "notices for the ADS ratio change", "a budget for the ADS ratio change",
    "plans for the ADS ratio change", "the ADS ratio change preparations",
    "the ADS ratio change administrative budget", "the change of the ratio filing",
])
def test_b1_completion_requires_the_ratio_change_as_its_completed_object(object_text):
    rows = _parse_gate_source(
        f"On November 4, 2022, the Company completed {object_text} for changing its ratio "
        "from one ADS representing one ordinary share to one ADS representing twenty ordinary shares.",
        form="6-K", filed="2022-11-10",
    )
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


@pytest.mark.parametrize("statement", [
    "The GDS ratio became effective on November 4, 2022.",
    "The ADS ratio for the Alpha program became effective on November 4, 2022.",
    "The ADS ratio for the alpha program became effective on November 4, 2022.",
    "The ADS ratio for the second program became effective on November 4, 2022.",
    "The ADS ratio for the Class A and Series A programs became effective on November 4, 2022.",
    "The ADS ratio of 10:1 became effective on November 4, 2022.",
    "The ADS ratio of 20:1 became effective on November 4, 2022.",
    "The ADS to ordinary share ratio of 20:1 became effective on November 4, 2022.",
    "The ratio of ADSs to ordinary shares of 20:1 became effective on November 4, 2022.",
    "The ADS ratio of 10/1 became effective on November 4, 2022.",
    "The ADS ratio of ten to one became effective on November 4, 2022.",
    "The ADS ratio of 1 ADS:10 ordinary shares became effective on November 4, 2022.",
    "The Depositary announced completion of preparations for the ADS ratio change effective on November 4, 2022.",
    "The Depositary confirmed the filings for the ADS ratio change effective on November 4, 2022.",
    "If the ADS ratio became effective on November 4, 2022, investors would receive the proposed entitlement.",
    "For financial statement presentation only, we assumed that the ADS ratio became effective on November 4, 2022.",
    "The ADS ratio became effective on November 4, 2022, following the implementation of the operational measures announced for investors and the registration processing instructions published by the Depositary, for the Class B program.",
])
def test_b1_confirmation_cannot_inherit_another_program_ratio_or_preparatory_notice(statement):
    rows = _parse_gate_source(
        "The Company plans to change the ratio of its American Depositary Shares to ordinary shares "
        "from one ADS representing one ordinary share to one ADS representing twenty ordinary shares, "
        "effective on November 4, 2022. " + statement,
        form="6-K", filed="2022-11-10",
    )
    assert rows and all(row.get("ratio_effectiveness_pending") for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


@pytest.mark.parametrize("approval_object", [
    "the administrative budget for the ADS ratio change", "the ADS ratio change administrative budget",
    "the filings for the ADS ratio change", "the Share Consolidation budget",
])
def test_b1_shareholder_approval_requires_ratio_change_or_consolidation_object(approval_object):
    rows = _parse_gate_source(
        _GATE_CONDITIONAL_NOTICE + f" The shareholders approved {approval_object}.",
        form="6-K", filed="2022-11-10",
    )
    assert rows and all(row.get("ratio_effectiveness_pending") for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


@pytest.mark.parametrize("underlying", ["ordinary shares", "Class A ordinary shares"])
def test_b1_differently_named_voting_class_does_not_erase_a_direct_condition(underlying):
    rows = _parse_gate_source(
        "The Company will change the ratio of its American Depositary Shares to " + underlying +
        " from one ADS representing one " + underlying.rstrip("s") +
        " to one ADS representing twenty " + underlying +
        ", effective on November 4, 2022, subject to approval of the Class B shareholders.",
        form="6-K", filed="2022-10-18",
    )
    assert rows and all(row.get("ratio_effectiveness_pending") for row in rows)
    assert all(row["ratio_effectiveness_conditions"] == ["shareholder_approval"] for row in rows)


@pytest.mark.parametrize("completion", [
    "On November 4, 2022, the Company completed the ADS ratio change from one ADS representing one ordinary share to one ADS representing twenty ordinary shares.",
    "On November 4, 2022, the Company effected the change of the ratio from one ADS representing one ordinary share to one ADS representing twenty ordinary shares.",
    "The Company changed the ratio from one ADS representing one ordinary share to one ADS representing twenty ordinary shares, effective on November 4, 2022.",
    "The Company plans to change the ADS ratio from one ADS representing one ordinary share to one ADS representing twenty ordinary shares, effective November 4, 2022. The ratio change became effective on November 4, 2022.",
])
def test_b1_genuine_direct_ratio_completion_is_still_confirmation(completion):
    rows = _parse_gate_source(completion, form="6-K", filed="2022-11-10")
    assert rows and all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all("ratio_effective" in row["ratio_effectiveness_confirmed_conditions"] for row in rows)
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)


def test_b1_narrative_completion_proof_does_not_include_an_unlabelled_numeric_table():
    text = "<table><tr><td>Net earnings</td><td>2023</td><td>2022</td></tr>" + (
        "<tr><td>100</td><td>200</td><td>300</td></tr>" * 15
    ) + "</table><p>The Company completed the ADS ratio change from one ADS representing one ordinary share to one ADS representing twenty ordinary shares on November 4, 2022.</p>"
    rows = _parse_gate_source(text, form="6-K", filed="2022-11-10")
    assert rows and all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all(row["ratio_effectiveness_confirmation_text"].startswith("The Company completed") for row in rows)


@pytest.mark.parametrize("fixture", [
    "b1_preparations_completion", "b1_different_class_confirmation", "b1_budget_approval",
])
def test_b1_regate_false_confirmation_keeps_conditional_ratio_ambiguous_in_sql(db, fixture):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    conditional = _parse_gate_source(_GATE_CONDITIONAL_NOTICE, form="6-K", filed="2022-10-18")
    _insert_gate_parsed_rows(db, conditional, "b1-conditional-ratio-announcement")
    pending = _parse_gate_source(_GATE_PENDING_F6, form="F-6 POS", filed="2022-10-24",
                                 adsh="0001193125-22-000124")
    _insert_gate_parsed_rows(db, pending, "b1-pending-registration")
    later = _parse_gate_source((FIXTURES / (fixture + ".html")).read_text(encoding="utf-8"),
                               form="6-K", filed="2022-11-10", adsh="0001193125-22-000125")
    _insert_gate_parsed_rows(db, later, "b1-invalid-later-confirmation")
    for day in ("2022-11-04", "2022-11-10", "2022-11-11", "2025-12-31"):
        assert resolve(db, day)[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")


def test_b1_genuine_anpc_completion_still_settles_after_publication(db):
    # Settlement needs the actual registered 20/1 entitlement as well as its
    # public completion. The completion cannot create an F-6 registration.
    for name in ("anpc_2022_cover", "anpc_2019_f6_full", "anpc_2022_f6_undated_amendment_full"):
        insert_real_parsed_rows(db, name)
    _insert_gate_parsed_rows(db, _parse_independent_listing_fixture("anpc_2022_plan_expected_ratio"),
                             "b1-anpc-announced-plan")
    assert resolve(db, "2022-12-16", cik=1786511, symbol="ANPC")[0] == "ambiguous"
    completed = _insert_gate_parsed_rows(db, _parse_independent_listing_fixture("anpc_2022_ratio_completion"),
                                         "b1-anpc-genuine-completion")
    assert resolve(db, "2022-12-16", cik=1786511, symbol="ANPC")[0] == "ambiguous"
    answer = resolve(db, "2022-12-17", cik=1786511, symbol="ANPC")
    assert answer[:4] == ("resolved", "ads", 20, 1)
    assert set(completed).intersection(answer[-1])


@pytest.mark.parametrize("suffix", [
    "but only hypothetically for pro forma presentation",
    "for financial statement presentation only",
])
def test_b1_hypothetical_accounting_completion_is_not_public_completion(suffix):
    rows = _parse_gate_source(
        "The Company completed the ADS ratio change from one ADS representing one ordinary share "
        "to one ADS representing twenty ordinary shares on November 4, 2022, " + suffix + ".",
        form="6-K", filed="2022-11-10",
    )
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all(row.get("ratio_effectiveness_pending") for row in rows)


@pytest.mark.parametrize("approval", [
    "The shareholders approved the Share Consolidation administrative budget.",
    "The shareholders resolved by ordinary resolution to consolidate the administrative budgets for the Share Consolidation.",
])
def test_b1_budget_approval_cannot_fulfil_named_consolidation(approval):
    rows = _parse_gate_source(
        approval + " The Share Consolidation will be effective on December 9, 2022. "
        "Upon the Share Consolidation, the ratio of its American Depositary Shares to ordinary shares "
        "will be amended from one ADS representing ten ordinary shares to one ADS representing one ordinary share.",
        form="6-K", filed="2022-11-30",
    )
    assert rows and not any(row.get("ratio_effectiveness_confirmed") for row in rows)


@pytest.mark.parametrize("object_text", ["regarding the administrative budget for", "unrelated to"])
def test_b1_adopted_resolutions_must_actually_approve_the_share_operation(object_text):
    rows = _parse_gate_source(
        "The proposed resolutions submitted for shareholder approval have been adopted " + object_text +
        " the Share Subdivision. The Share Subdivision will be effective on March 18, 2021. "
        "Concurrently with the effectiveness of the Share Subdivision, the Company will change the ratio "
        "of its ADSs to ordinary shares from one ADS representing two ordinary shares to one ADS "
        "representing one ordinary share.",
        form="6-K", filed="2021-03-18",
    )
    assert rows and not any(row.get("ratio_effectiveness_confirmed") for row in rows)



def test_b1_preparations_and_budget_effect_cannot_confirm_a_ratio_linked_to_subdivision():
    rows = _parse_gate_source(
        "The Share Subdivision became effective on November 4, 2022. Concurrently with the Share "
        "Subdivision, the Company plans to change its ADS ratio from one ADS representing one "
        "ordinary share to one ADS representing twenty ordinary shares, and the preparations for "
        "this ratio change have been approved while the ratio change administrative budget has "
        "taken effect concurrently on the same day.",
        form="6-K", filed="2022-11-10",
    )
    assert rows and not any(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all(row.get("ratio_effectiveness_pending") for row in rows)


@pytest.mark.parametrize("planned_class", [None, "Class A"])
def test_b1_following_named_class_confirmation_never_discharges_unknown_or_other_class_plan(planned_class):
    unit = ((planned_class + " ") if planned_class else "") + "ordinary"
    rows = _parse_gate_source(
        "The Company plans to change the ADS ratio from one ADS representing one " + unit +
        " share to one ADS representing twenty " + unit +
        " shares, effective on November 4, 2022. The ADS ratio for the Class B program became "
        "effective on November 4, 2022.", form="6-K", filed="2022-11-10",
    )
    assert rows and all(row.get("ratio_effectiveness_pending") for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


def test_b1_explicit_target_ratio_cannot_confirm_a_different_numerical_transition():
    rows = _parse_gate_source(
        "The Company plans to change the ADS ratio from one ADS representing one ordinary share "
        "to one ADS representing twenty ordinary shares, effective on November 4, 2022, while "
        "the ADS ratio of one ADS representing ten ordinary shares became effective on November 4, 2022.",
        form="6-K", filed="2022-11-10",
    )
    planned = [row for row in rows if (row["ratio_numerator"], row["ratio_denominator"]) == (20, 1)]
    assert planned and all(row.get("ratio_effectiveness_pending") for row in planned)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in planned)


@pytest.mark.parametrize("text,ratio,underlying_class,effective", [
    ("Effective April 22, 2026, the Company changed its ADS ratio from the previous ratio of one ADS representing three Class A ordinary shares to the current ratio of one ADS representing thirty Class A ordinary shares.", (30, 1), "class_a", "2026-04-22"),
    ("The Company changed the ratio of its ADSs to its Class A ordinary shares from the previous ADS ratio of one ADS to eight Class A ordinary shares to the current ADS Ratio of one ADS to forty-eight Class A ordinary shares, effective on May 23, 2022.", (48, 1), "class_a", "2022-05-23"),
    ("On January 9, 2023, we effected a ratio change so that each ADS now represents 600 ordinary shares (representing a 10-for-1 reverse split).", (600, 1), None, "2023-01-09"),
    ("On May 20, 2022, the Company effected a change to a new ratio of one ADS to five Class A ordinary shares, which had the same effect as a one-for-five reverse share split.", (5, 1), "class_a", "2022-05-20"),
    ("On February 20, 2024, we effected a change of the ADS to Class A ordinary share ratio from one ADS representing two Class A ordinary shares to one ADS representing twenty Class A ordinary shares.", (20, 1), "class_a", "2024-02-20"),
    ("On June 11, 2025, the Company effected the change in the ratio of each ADS to Ordinary Shares from one ADS representing one thousand two hundred Ordinary Shares, to one ADS representing three thousand six hundred Ordinary Shares.", (3600, 1), None, "2025-06-11"),
    ("The Company previously announced its plan to change the ratio of its ADSs to ordinary shares from one ADS representing one ordinary share to one ADS representing five ordinary shares. The ratio change on the ADS trading price on the Nasdaq Capital Market became effective as of the opening of trading on May 16, 2024.", (5, 1), None, "2024-05-16"),
    ("The Company effected the consolidation along with a change in the ADS ratio from one ADS representing twenty-five Ordinary Shares to the new ratio of one ADS representing five Ordinary Shares, this was effective from 27 March 2023.", (5, 1), None, "2023-03-27"),
    ("The reverse share split went effective on October 16, 2020. Concurrently with the reverse share split, a change to the ratio of its ADSs to its ordinary shares was effective pursuant to which each ADS representing forty ordinary shares changed to each ADS representing one hundred forty ordinary shares.", (140, 1), None, "2020-10-16"),
])
def test_b1_literal_completed_ratio_variants_preserve_only_the_new_target(text, ratio, underlying_class, effective):
    rows = _parse_gate_source(text, form="6-K", filed="2026-09-01")
    assert rows
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["underlying_class"], row["effective_from"]) for row in rows} == {(*ratio, underlying_class, effective)}
    assert all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)


@pytest.mark.parametrize("footnote,ratio,effective", [
    ("(1) Each ADS represents eight ordinary shares, which reflects the share subdivision and ADS ratio change that became effective on July 30, 2019.", (8, 1), "2019-07-30"),
    ("(1) On July 3, 2023, the Company plans to change its ADS ratio from one ADS representing one ordinary share to one ADS representing fifty ordinary shares. The change in the ADS ratio was effective on July 13, 2023.", (50, 1), "2023-07-13"),
])
def test_b1_genuine_narrative_footnote_survives_a_preceding_financial_table(footnote, ratio, effective):
    text = "<table><tr><td>Weighted average number of ordinary shares</td><td>60,000,000</td></tr><tr><td>Basic and diluted loss per ADS</td><td>(3.14)</td></tr></table><p>" + footnote + "</p>"
    rows = _parse_gate_source(text, form="6-K", filed="2025-03-01")
    assert rows and all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"]) for row in rows} == {(*ratio, effective)}
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)
    assert all("Weighted average" not in row["ratio_effectiveness_confirmation_text"] and "loss per ADS" not in row["ratio_effectiveness_confirmation_text"] for row in rows)


def test_b1_completion_clause_remains_valid_when_followed_by_its_eps_accounting_effect():
    rows = _parse_gate_source(
        "The Company completed the ADS ratio change from one ADS representing one ordinary share to one ADS representing twenty ordinary shares on November 4, 2022. Basic and diluted earnings per ADS have been revised retrospectively for all periods presented.",
        form="6-K", filed="2022-11-10",
    )
    assert rows and all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all("basic and diluted" not in row["ratio_effectiveness_confirmation_text"] for row in rows)


def test_b1_market_price_table_cells_do_not_enter_the_narrative_confirmation():
    rows = _parse_gate_source(
        "<table><tr><td>High Low 2014 First Quarter</td><td>$ 4.11</td><td>$ 3.24</td></tr></table><p>The table below sets forth the prices, adjusted to reflect the current ADS-to-ordinary share ratio of one ADS to three ordinary shares, which became effective on August 22, 2016, for all periods presented):</p><table><tr><td>High Low 2017 January</td><td>$ 2.14</td></tr></table>",
        form="6-K", filed="2017-08-02",
    )
    assert rows and all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all("High Low" not in row["ratio_effectiveness_confirmation_text"] for row in rows)
    assert all("$" not in row["ratio_effectiveness_confirmation_text"] for row in rows)



def test_b1_effected_consolidation_with_actual_coordinated_ratio_change_confirms_new_target():
    rows = _parse_gate_source(
        "Following notification that the Company was not in compliance with the Nasdaq minimum "
        "bid price requirement, the Company effected an Ordinary Share consolidation on a one for "
        "twenty basis along with a change in ADS ratio from one ADS representing twenty-five Ordinary "
        "Shares to the new ratio of one ADS representing five Ordinary Shares, this was effective "
        "from 27 March 2023.", form="6-K", filed="2023-04-28",
    )
    assert rows and all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"]) for row in rows} == {(5, 1, "2023-03-27")}
    assert all(row["ratio_effectiveness_confirmation_text"].lower().startswith("the company effected") for row in rows)
    assert not any(row.get("ratio_effectiveness_pending") for row in rows)


@pytest.mark.parametrize("ratio_object", [
    "preparations for a change in ADS ratio", "the budget for a change in ADS ratio",
    "a planned change in ADS ratio", "a proposed ADS ratio change",
])
def test_b1_effected_consolidation_does_not_complete_a_preparatory_or_planned_ratio_object(ratio_object):
    rows = _parse_gate_source(
        "The Company effected an Ordinary Share consolidation on a one for twenty basis along "
        "with " + ratio_object + " from one ADS representing twenty-five Ordinary Shares to the new "
        "ratio of one ADS representing five Ordinary Shares, this was effective from 27 March 2023, "
        "subject to Depositary confirmation of the ratio change.", form="6-K", filed="2023-04-28",
    )
    assert rows and not any(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all(row.get("ratio_effectiveness_pending") for row in rows)


def test_b1_completed_consolidation_alone_cannot_confirm_a_separate_ratio_plan():
    rows = _parse_gate_source(
        "The Company effected an Ordinary Share consolidation on March 27, 2023. It plans to "
        "change the ADS ratio from one ADS representing twenty-five Ordinary Shares to one ADS "
        "representing five Ordinary Shares, effective March 27, 2023.", form="6-K", filed="2023-04-28",
    )
    assert rows and all(row.get("ratio_effectiveness_pending") for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)



def test_b1_accounting_relative_clause_requires_a_separate_legal_completion():
    rows = _parse_gate_source(
        "The weighted average number of ADS and earnings per ADS have been retrospectively adjusted "
        "to reflect the ADS ratio change from one ADS representing one Class A ordinary share to "
        "one ADS representing five Class A ordinary shares, which became effective on December 23, 2021.",
        form="6-K", filed="2022-04-08",
    )
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


@pytest.mark.parametrize("premise", ["assuming", "as if", "on the assumption that"])
def test_b1_assumed_ratio_effective_relative_clause_never_confirms(premise):
    rows = _parse_gate_source(
        "The weighted average number of ADS and earnings per ADS have been retrospectively adjusted "
        + premise + " the ADS ratio change from one ADS representing one Class A ordinary share to "
        "one ADS representing five Class A ordinary shares, which became effective on December 23, "
        "2021, occurred at the beginning of the earliest period presented.",
        form="6-K", filed="2022-04-08",
    )
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


def test_b1_numeric_table_ratio_outside_an_actual_relative_clause_does_not_inherit_confirmation():
    rows = _parse_gate_source(
        "<table><tr><td>Weighted average ADS (1 ADS representing 5 Class A ordinary shares)</td><td>60,000,000</td></tr></table>"
        "<p>The weighted average ADS and earnings per ADS have been retrospectively adjusted to reflect "
        "the ADS ratio change from one ADS representing one Class A ordinary share to one ADS "
        "representing five Class A ordinary shares, which became effective on December 23, 2021.</p>",
        form="6-K", filed="2022-04-08",
    )
    confirmed = [row for row in rows if row.get("ratio_effectiveness_confirmed")]
    assert not confirmed


@pytest.mark.parametrize("identity", [
    "the Class B ADS program", "the GDS program", "the second ADS program",
])
def test_b1_actual_relative_clause_does_not_clear_another_class_or_program(identity):
    rows = _parse_gate_source(
        "The Company plans to change the ADS ratio from one ADS representing one Class A ordinary "
        "share to one ADS representing five Class A ordinary shares, effective December 23, 2021. "
        "The weighted average ADS and earnings per ADS have been revised to reflect the ratio change "
        "for " + identity + ", which became effective on December 23, 2021.",
        form="6-K", filed="2022-04-08",
    )
    assert rows and not any(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all(row.get("ratio_effectiveness_pending") for row in rows)


def test_b1_actual_relative_clause_with_another_target_ratio_cannot_confirm_the_plan():
    rows = _parse_gate_source(
        "The Company plans to change the ADS ratio from one ADS representing one Class A ordinary "
        "share to one ADS representing five Class A ordinary shares, effective December 23, 2021, "
        "and weighted average ADS have been revised to reflect the ratio change from one ADS "
        "representing one Class A ordinary share to one ADS representing ten Class A ordinary "
        "shares, which became effective on December 23, 2021.",
        form="6-K", filed="2022-04-08",
    )
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)



def test_b1_a_numeric_table_ratio_label_cannot_span_a_later_actual_ratio_subject():
    rows = _parse_gate_source(
        "<table><tr><td>Weighted average ADS ratio of one ADS representing five ordinary shares</td><td>60,000,000</td></tr></table>"
        "<p>EPS have been retrospectively adjusted to reflect the ADS ratio change from one ADS "
        "representing one ordinary share to one ADS representing five ordinary shares, which became "
        "effective on December 23, 2021.</p>", form="6-K", filed="2022-04-08",
    )
    confirmed = [row for row in rows if row.get("ratio_effectiveness_confirmed")]
    assert not confirmed



def test_b1_accounting_relative_clause_with_comma_requires_separate_legal_completion():
    rows = _parse_gate_source(
        "Net loss per ADS has been retrospectively adjusted for the ADS ratio change, from two ADS "
        "to five Class A ordinary shares to one ADS to twenty Class A ordinary shares, that became "
        "effective on October 30, 2020.", form="6-K", filed="2020-11-18",
    )
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


@pytest.mark.parametrize("predicate", ["would become", "might become", "could become", "will become"])
def test_b1_financial_prospective_relative_clause_is_not_an_unconditional_announcement(predicate):
    rows = _parse_gate_source(
        "The weighted average ADS have been adjusted to reflect the ADS ratio change from one ADS "
        "representing one ordinary share to one ADS representing five ordinary shares, which "
        + predicate + " effective on December 23, 2021.", form="6-K", filed="2021-12-01",
    )
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert not rows or all(row.get("ratio_effectiveness_pending") for row in rows)


@pytest.mark.parametrize("sentence", [
    "For purposes of EPS, it is assumed that the Company's ADS ratio of one ADS representing twenty ordinary shares was effective on November 4, 2022.",
    "The earnings per ADS figures have been retrospectively adjusted as though the ADS ratio change from one ADS representing one ordinary share to one ADS representing twenty ordinary shares was effective on November 4, 2022.",
    "Assuming the Company's ADS ratio of one ADS representing twenty ordinary shares was effective on November 4, 2022, earnings per ADS have been restated.",
    "The Company's ADS ratio of one ADS representing twenty ordinary shares had been effective on November 4, 2022 for purposes of EPS.",
])
def test_b1_round2_full_accounting_sentence_cannot_confirm(sentence):
    rows = _parse_gate_source(sentence, form="6-K", filed="2022-11-10")
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert not rows or all(row.get("ratio_effectiveness_pending") for row in rows)


@pytest.mark.parametrize("verb,object_text,predicate", [
    ("amended", "ADS ratio change announcement", "will represent"),
    ("adjusted", "ADS ratio estimates", "represents"),
    ("amended", "ADS ratio notice", "represents"),
    ("changed", "ADS ratio filing", "represents"),
    ("amended", "ADS ratio agreement", "represents"),
    ("adjusted", "ADS ratio", "will represent"),
    ("amended", "ADS ratio", "is expected to represent"),
])
def test_b1_round2_adjustment_requires_ratio_object_and_completed_predicate(verb, object_text, predicate):
    rows = _parse_gate_source(
        "On November 4, 2022, the Company " + verb + " its " + object_text +
        " to state that each ADS " + predicate + " twenty ordinary shares.",
        form="6-K", filed="2022-11-10",
    )
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


@pytest.mark.parametrize("former", ["former", "previous", "prior", "old", "existing", "current-before-change"])
def test_b1_round2_numberless_former_ratio_cannot_confirm_target(former):
    rows = _parse_gate_source(
        "The Company plans to change the ADS ratio from one ADS representing one ordinary share "
        "to one ADS representing twenty ordinary shares, effective on November 4, 2022. "
        "The " + former + " ADS ratio was effective on November 4, 2022.",
        form="6-K", filed="2022-11-10",
    )
    assert rows and all(row.get("ratio_effectiveness_pending") for row in rows)
    assert not any(row.get("ratio_effectiveness_confirmed") for row in rows)


def test_b1_round2_baidu_representing_class_a_keeps_completed_reciprocal_ratio():
    rows = _parse_gate_source(
        "Effective on May 12, 2010, Baidu adjusted the ratio of its American depositary shares "
        "('ADSs') representing Class A ordinary shares from one (1) ADS for one (1) share to "
        "ten (10) ADSs for one (1) share. All earnings per ADS figures in this announcement "
        "give effect to the foregoing ADS to share ratio change.",
        form="6-K", filed="2010-07-26", adsh="0000950123-10-067501",
    )
    assert rows
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["underlying_class"], row["effective_from"]) for row in rows} == {(1, 10, "class_a", "2010-05-12")}
    assert all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all("earnings per ADS" not in row["ratio_effectiveness_confirmation_text"] for row in rows)


@pytest.mark.parametrize("transition", [
    "from the previous one ADS for every twenty-five ordinary shares to one ADS for every five ordinary shares",
    "from the previous twenty-five ordinary shares for every one ADS to five ordinary shares for every one ADS",
])
def test_b1_round2_for_every_ratio_directions_keep_only_completed_target(transition):
    rows = _parse_gate_source(
        "Effective October 1, 2020, the Company changed the ratio of its American depositary "
        "shares ('ADSs'), representing ordinary shares, " + transition + ". The data "
        "throughout this announcement have been revised to reflect the ratio change as if it "
        "had occurred throughout the periods presented herein.",
        form="6-K", filed="2020-11-20",
    )
    assert rows
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["effective_from"]) for row in rows} == {(5, 1, "2020-10-01")}
    assert all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all("as if" not in row["ratio_effectiveness_confirmation_text"] for row in rows)


@pytest.mark.parametrize("verb", ["completed", "effected", "implemented", "amended", "adjusted", "changed"])
def test_b1_round2_legal_completion_and_separate_eps_sentence_still_confirm(verb):
    object_text = "the ADS ratio change" if verb in {"completed", "effected", "implemented"} else "the ADS ratio"
    rows = _parse_gate_source(
        "On November 4, 2022, the Company " + verb + " " + object_text +
        " from one ADS representing one Class A ordinary share to one ADS representing twenty "
        "Class A ordinary shares. For purposes of EPS, the figures have been retrospectively "
        "adjusted as though the ratio had been effective for all periods presented.",
        form="6-K", filed="2022-11-10",
    )
    assert rows and all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert all("EPS" not in row["ratio_effectiveness_confirmation_text"] for row in rows)


def test_b1_round2_ordinal_completion_footnote_does_not_inherit_financial_table_classes():
    rows = _parse_gate_source(
        "<table><tr><td>Loss per ADS - basic and diluted</td><td>(9.78)</td></tr>"
        "<tr><td>Weighted average Class A ordinary shares</td><td>2,773,677,803</td></tr>"
        "<tr><td>Weighted average Class B ordinary shares</td><td>65,387,845</td></tr></table>"
        "<p>* On March 26, 2025, LGHL implemented the second ADS Ratio change, from one (1) ADS "
        "representing fifty (50) Share to one (1) ADS representing two thousand and five hundred "
        "(2,500) shares.</p>", form="6-K", filed="2025-09-15",
    )
    assert rows and all(row.get("ratio_effectiveness_confirmed") for row in rows)
    assert {(row["ratio_numerator"], row["ratio_denominator"], row["underlying_class"], row["effective_from"]) for row in rows} == {(2500, 1, None, "2025-03-26")}
    assert all(row["ratio_effectiveness_confirmation_text"].startswith("On March 26, 2025") for row in rows)


_B1_ROUND2_MATRIX = json.loads((FIXTURES / "b1_round2_confirmation_matrix.json").read_text(encoding="utf-8"))["cases"]


@pytest.mark.parametrize("case", [case for case in _B1_ROUND2_MATRIX if not case["positive"]],
                         ids=lambda case: case["entry"] + "/" + case["category"])
def test_b1_round2_adversarial_confirmation_matrix(case):
    rows = _parse_gate_source(case["text"], form=case["form"], filed="2022-11-10")
    target = [row for row in rows if (row["ratio_numerator"], row["ratio_denominator"]) == (20, 1)]
    assert not any(row.get("ratio_effectiveness_confirmed") for row in target)
    # An unconfirmed unconditional row could independently activate a dated
    # announced change in SQL. Every surviving rejected target stays pending.
    assert all(row.get("ratio_effectiveness_pending") for row in target)


@pytest.mark.parametrize("case", [case for case in _B1_ROUND2_MATRIX if case["positive"]],
                         ids=lambda case: case["entry"] + "/" + case["category"])
def test_b1_round2_confirmation_matrix_genuine_controls(case):
    rows = _parse_gate_source(case["text"], form=case["form"], filed="2023-11-10")
    target = [row for row in rows if (row["ratio_numerator"], row["ratio_denominator"]) == (20, 1)]
    assert any(row.get("ratio_effectiveness_confirmed") for row in target)


@pytest.mark.parametrize("later_text", [
    "For purposes of EPS, it is assumed that the Company's ADS ratio of one ADS representing twenty ordinary shares was effective on November 4, 2022.",
    "On November 4, 2022, the Company amended its ADS ratio change announcement to state that each ADS will represent twenty ordinary shares.",
    "The Company plans to change the ADS ratio from one ADS representing one ordinary share to one ADS representing twenty ordinary shares, effective on November 4, 2022. The former ADS ratio was effective on November 4, 2022.",
    "The shareholders approved the Share Consolidation. For purposes of EPS, it is assumed that the Share Consolidation was effective on November 4, 2022. Upon the Share Consolidation, the ratio of its ADSs representing ordinary shares will be amended from one ADS representing one ordinary share to one ADS representing twenty ordinary shares.",
    "The Company did not change its ADS ratio from one ADS representing one ordinary share to one ADS representing twenty ordinary shares, effective on November 4, 2022.",
])
def test_b1_round2_rejected_later_authority_keeps_sql_ratio_ambiguous(db, later_text):
    add(db, filed="2022-01-01")
    add_ratio(db, (1, 1), filed="2021-01-01")
    add_ratio(db, (1, 1), source="cover_footnote", filed="2022-01-01")
    _insert_gate_parsed_rows(db, _parse_gate_source(_GATE_CONDITIONAL_NOTICE, form="6-K", filed="2022-10-18"),
                             "b1-round2-conditional-ratio-announcement")
    _insert_gate_parsed_rows(db, _parse_gate_source(_GATE_PENDING_F6, form="F-6 POS", filed="2022-10-24",
                                                  adsh="0001193125-22-000124"), "b1-round2-pending-registration")
    later = _parse_gate_source(later_text, form="6-K", filed="2022-11-10", adsh="0001193125-22-000125")
    _insert_gate_parsed_rows(db, later, "b1-round2-rejected-later-authority")
    for day in ("2022-11-04", "2022-11-10", "2022-11-11", "2025-12-31"):
        assert resolve(db, day)[:6] == ("ambiguous", "ads", None, None, "resolved", "ambiguous")
