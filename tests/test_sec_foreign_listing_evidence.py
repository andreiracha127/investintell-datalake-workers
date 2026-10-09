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
    assert all(row["effective_date_explicit"] is False for row in rows)
    assert not any(row["effective_from"] in {"2025-02-18", "2025-02-19"} for row in rows)


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
        ratio_effectiveness_pending_text=None):
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
    add_ratio(db, ratio, source="ratio_change_6k", filed="2022-11-18", effective=effective,
              symbol=symbol, underlying_class=underlying_class)
    answer = resolve(db, "2022-11-28")
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
        "'sec_foreign_listing_ratio_correction_ck', 'sec_foreign_listing_ratio_pending_ck')"
    ).fetchall()
    assert len(constraints) == 3


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
    for name in ("anpc_2022_cover", "anpc_2019_f6_full",
                 "anpc_2022_f6_undated_amendment_full", "anpc_2022_change_announcement"):
        insert_real_parsed_rows(db, name)
    for day in ("2022-10-24", "2022-10-25", "2022-11-03"):
        assert resolve(db, day, cik=1786511, symbol="ANPC")[:4] == ("resolved", "ads", 1, 1)
    assert resolve(db, "2022-11-04", cik=1786511, symbol="ANPC")[:4] == ("resolved", "ads", 20, 1)


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
    assert _parse_6k_transition_text(
        "The Share Consolidation will be effective at 5:00 P.M., on December 9, 2022. "
        "The Share Consolidation will be effective at 5:00 P.M., on December 10, 2022. "
        "Upon the Share Consolidation, the ADS ratio will change from one ADS for ten ordinary shares "
        "to one ADS for one ordinary share."
    ) == []
