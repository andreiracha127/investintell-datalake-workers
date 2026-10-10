"""Offline cover grammars and fail-closed count scope regression fixtures."""
from __future__ import annotations

from html import escape
import hashlib
import json
from pathlib import Path

import pytest

from scripts.sec_foreign_listing_parser import _Document
from scripts.sec_foreign_share_census_parser import normalize_class_key, parse_share_census

PROMPT = ("Indicate the number of outstanding shares of each of the issuer’s "
          "classes of capital or common stock as of the close of the period "
          "covered by the annual report: ")


def cover(response, header="For the fiscal year ended December 31, 2024"):
    return ("<p>" + header + "</p><p>" + PROMPT + response +
            "</p><p>Indicate by check mark whether the registrant...</p><p>PART I</p><p>ITEM 1. Identity of Directors</p>")


@pytest.mark.parametrize("name,key", [
    ("Class A ordinary shares", "class:a"), ("Class II common shares", "class:2"),
    ("Series IV preference shares", "series:4"), ("Class 2 shares", "class:2"),
    ("Class AA shares", "class:aa"), ("Class of common shares", None),
    ("ordinary shares", "ordinary"), ("Common Stock", "common"),
    ("A shares", "class:a"), ("Class_A", "class:a"), ("class:III", "class:3"),
    ("A ordinary shares", "class:a"), ("B common shares", "class:b"),
    ("II ordinary shares", "class:2"),
    ("A Common Stock", "class:a"),
])
def test_class_normalization(name, key):
    assert normalize_class_key(name) == key


@pytest.mark.parametrize("number", range(1, 40))
def test_all_w1_roman_class_and_series_keys(number):
    tens, remainder = divmod(number, 10)
    roman = "X" * tens + ("", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX")[remainder]
    assert normalize_class_key("Class " + roman + " ordinary shares") == f"class:{number}"
    assert normalize_class_key("Series " + roman + " preferred shares") == f"series:{number}"
    assert normalize_class_key(roman + " ordinary shares") == f"class:{number}"
    census = parse_share_census(cover("Class " + roman + " ordinary shares: 100"))
    assert census["complete"], census
    assert census["classes"][0]["class_key"] == f"class:{number}"


@pytest.mark.parametrize("response,counts,names", [
    ("2,239,234,372 Class A ordinary shares and 524,340,320 Class B ordinary shares, par value US$0.000000625 per share, as of December 31, 2024.",
     [2239234372, 524340320], ["Class A ordinary shares", "Class B ordinary shares"]),
    ("As of December 31, 2024, 25,932,733,242 Common Shares, par value NT$10 each were outstanding.",
     [25932733242], ["Common Shares"]),
    ("393,283,720 Ordinary Shares (nominal value €0.09 per share)", [393283720], ["Ordinary Shares"]),
    ("A shares, nominal value DKK 0.10 each: 1,074,872,000 B shares, nominal value DKK 0.10 each: 3,390,128,000",
     [1074872000, 3390128000], ["A shares", "B shares"]),
    ("3,167,959,016 ordinary shares, par value US$0.0001 per share.", [3167959016], ["ordinary shares"]),
    ("2,102,996,000 Common Shares outstanding as of December 31, 2024", [2102996000], ["Common Shares"]),
    ("Class 1 ordinary shares: 400 Class 2 ordinary shares: 100 Total: 500", [400, 100], ["Class 1 ordinary shares", "Class 2 ordinary shares"]),
])
def test_observed_named_and_numbered_cover_grammars(response, counts, names):
    census = parse_share_census(cover(response))
    assert census["complete"], census
    assert [row["shares"] for row in census["classes"]] == counts
    assert [row["class_name"] for row in census["classes"]] == names
    assert census["computed_total"] == sum(counts)
    assert census["shares_as_of"] == "2024-12-31"
    assert not census["numeric_residue"]


def test_qgen_custom_cover_response():
    text = ("<p>The number of outstanding Common Shares as of December 31, 2024 was 222,290,848.</p>"
            "<p>Indicate by check mark ...</p><p>PART I</p><p>ITEM 1. Identity of Directors</p>")
    census = parse_share_census(text)
    assert census["complete"]
    assert census["classes"] == [{"class_name": "Common Shares", "class_key": "common", "class_kind": "ordinary", "shares": 222290848}]
    assert census["date_explicit"]


def test_dlo_combined_ab_is_not_allocated():
    census = parse_share_census(cover("285,475,136 Class A and Class B common shares, as of December 31, 2024"))
    assert not census["complete"]
    assert [c["class_key"] for c in census["classes"]] == ["class:a", "class:b"]
    assert all(c["shares"] is None for c in census["classes"])
    assert census["stated_total"] == 285475136
    assert "combined_class_count" in census["reasons"]


def test_bare_capital_letters_with_ordinary_names_are_explicit_classes():
    census = parse_share_census(cover("A ordinary shares: 100 B ordinary shares: 25"))
    assert census["complete"], census
    assert [c["class_name"] for c in census["classes"]] == ["A ordinary shares", "B ordinary shares"]
    assert [c["class_key"] for c in census["classes"]] == ["class:a", "class:b"]
    assert [c["shares"] for c in census["classes"]] == [100, 25]
    assert parse_share_census(cover("a total of 100 ordinary shares"))["classes"][0]["class_key"] == "ordinary"


def test_zim_unnamed_count_must_not_inherit_12b_title():
    census = parse_share_census("<p>12(b): Ordinary shares ZIM NYSE</p>" + cover("120,423,333."))
    assert not census["complete"]
    assert census["classes"] == []
    assert census["numeric_residue"] == ["120,423,333"]


def test_preferred_deferred_and_nil_counts_are_preserved():
    census = parse_share_census(cover("Ordinary shares: 100 Preferred shares: nil Deferred shares: 4 Total: 104"))
    assert census["complete"], census
    assert [c["class_kind"] for c in census["classes"]] == ["ordinary", "preferred", "other"]
    assert [c["shares"] for c in census["classes"]] == [100, 0, 4]


def test_conflicting_stated_total_refuses_without_repair():
    census = parse_share_census(cover("Class A common shares: 100 Class B common shares: 30 Total: 140"))
    assert not census["complete"]
    assert census["computed_total"] == 130
    assert census["stated_total"] == 140
    assert "stated_total_mismatch" in census["reasons"]
    assert census["conflicting"]
    assert census["status"] == "conflicting"


@pytest.mark.parametrize("response", [
    "100 ordinary shares; 17 unidentified", "100 ordinary shares; Founder shares: TBD",
    "100 ordinary shares and Class B common shares", "approximately 100 ordinary shares",
    "100 ordinary shares including 5 treasury shares", "-100 ordinary shares",
    "100 ordinary shares, 10 ADSs", "100 ordinary shares and preferred stock: unknown",
    "Ordinary shares: 100; Deferred C capital: not disclosed",
    "fully paid registered ordinary shares: 100", "ordinary shares: 100; 1,20 unknown",
    "Ordinary shares: 100, par value $0.01, preferred shares 50 each",
    "Class A ordinary shares 100 Class B ordinary shares",
    "100 Class A ordinary shares Class B ordinary shares",
])
def test_unknown_or_qualified_response_fails_closed(response):
    assert not parse_share_census(cover(response))["complete"]


@pytest.mark.parametrize("response", [
    "Class A ordinary shares: 100 or Class B ordinary shares: 200",
    "100 ordinary shares or 200 common shares",
    "either 100 ordinary shares and 200 common shares",
    "100 ordinary shares; alternatively 200 common shares",
    "100 ordinary shares if 200 common shares",
    "100 ordinary shares unless 200 common shares",
])
def test_alternative_or_conditional_counts_are_not_an_exhaustive_census(response):
    census = parse_share_census(cover(response))
    assert not census["complete"]
    assert "alternative_or_conditional_statement" in census["reasons"]


@pytest.mark.parametrize("response", [
    "Total ordinary shares: 100", "Total common stock: 100",
    "The total number of ordinary shares: 100",
    "An aggregate of 100 ordinary shares",
    "100 common shares in aggregate", "Aggregate ordinary shares: 100",
    "In aggregate 100 ordinary shares",
])
def test_generic_aggregate_does_not_prove_single_class(response):
    census = parse_share_census(cover(response))
    assert not census["complete"]
    assert census["stated_total"] == 100
    assert "aggregate_only_statement" in census["reasons"]


def test_explicit_class_total_preserves_class_identity():
    census = parse_share_census(cover("Total Class A ordinary shares: 100"))
    assert census["complete"], census
    assert census["classes"][0]["class_key"] == "class:a"
    assert census["classes"][0]["shares"] == 100


def test_explicit_class_aggregate_preserves_class_identity():
    census = parse_share_census(cover("An aggregate of 100 Class A ordinary shares"))
    assert census["complete"], census
    assert census["classes"][0]["class_key"] == "class:a"
    assert census["classes"][0]["shares"] == 100


def test_multiline_html_table_joins_class_name_and_keeps_all_rows():
    response = "<table><tr><td>Class A<br>ordinary shares</td><td>1,200</td></tr><tr><td>Class B<br>ordinary shares</td><td>30</td></tr><tr><td>Total</td><td>1,230</td></tr></table>"
    census = parse_share_census(cover(response))
    assert census["complete"], census
    assert [row["shares"] for row in census["classes"]] == [1200, 30]


def test_pdf_extracted_cover_uses_same_document_text():
    from scripts.load_sec_foreign_listing_evidence import pdf_parser_input
    pages = [
        "For the fiscal year ended December 31, 2024\n" + PROMPT + "\nClass A ordinary shares   1,200\nClass B ordinary shares   nil\nTotal 1,200\nIndicate by check mark ...",
        "PART I\nITEM 1. Identity of Directors\nAnnual report financial statements 2024",
    ]
    html, normalized, ranges = pdf_parser_input(pages)
    census = parse_share_census(_Document(html), source_lines=[line for page in pages for line in page.splitlines()])
    assert census["complete"], census
    assert [row["shares"] for row in census["classes"]] == [1200, 0]
    assert census["source_text"] in normalized
    assert ranges[0][0] == 1


def test_period_end_is_same_filing_dei_not_filing_year_guess():
    html = '<ix:nonNumeric name="dei:DocumentPeriodEndDate">2024-09-30</ix:nonNumeric>' + cover("Ordinary shares: 100", header="Annual report filed March 2025")
    assert parse_share_census(html)["shares_as_of"] == "2024-09-30"
    missing = parse_share_census(cover("Ordinary shares: 100", header="Annual report filed March 2025"))
    assert not missing["complete"]
    assert missing["shares_as_of"] is None


def test_explicit_statement_date_and_period_end_remain_distinct():
    census = parse_share_census(cover("100 ordinary shares as of February 28, 2025"))
    assert census["complete"]
    assert census["shares_as_of"] == "2025-02-28"
    assert census["period_end"] == "2024-12-31"


def test_no_statement_returns_none_and_no_guessed_class():
    census = parse_share_census("<p>Common shares: 100 in financial statement</p>")
    assert census["status"] == "none"
    assert census["classes"] == []


def test_named_source_excerpt_fixtures_preserve_provenance_and_expected_scope():
    fixtures = json.loads((Path(__file__).parent / "fixtures" / "sec_foreign_share_census" / "named_cover_statements.json").read_text(encoding="utf-8"))
    expected = {"BIDU": [2239234372, 524340320], "NVO": [1074872000, 3390128000],
                "TSM": [25932733242], "ASML": [393283720], "QGEN": [222290848],
                "NTES": [3167959016], "CNQ": [2102996000],
                "AKO-A": [473289301, 473281303], "CGG": [151861932], "BUR": [218581877],
                "AQN": [767343863], "RCI": [112467648, 523231804], "DBVT": [24648828], "CIFS": [22114188],
                "APWC": [13819669], "JOBS": [66784688], "BSBR": [3850970714, 3712111703], "DIV": [11043027],
                "IMOS": [727240126], "SHELL-2009": [3454731900, 2667562105], "SEK": [2579394, 1410606],
                "BORR": [252582036], "AKAN": [1983546]}
    assert {f["symbol"] for f in fixtures} == set(expected) | {"DLO", "ZIM", "SAP", "ASX", "AHI", "REPCF", "GIL", "STN", "CIFS-HEREIN", "BNS"}
    for fixture in fixtures:
        assert hashlib.sha256(fixture["source_text"].encode()).hexdigest() == fixture["excerpt_sha256"]
        assert fixture["source_url"].startswith("https://www.sec.gov/Archives/edgar/")
        region = fixture.get("cover_region_text", fixture["source_text"])
        if "cover_region_text" in fixture:
            assert hashlib.sha256(region.encode()).hexdigest() == fixture["cover_region_sha256"]
            if "footnote_quote" in fixture:
                assert fixture["footnote_quote"] in region
        suffix = "" if "cover_section_header" in fixture else "<p>PART I</p><p>ITEM 1. Identity of Directors</p>"
        header = "<p>For the fiscal year ended " + fixture["period_end"] + "</p>" if fixture["period_end"] else ""
        html = fixture.get("cover_html", header + "<p>" + escape(region) + "</p>" + suffix)
        if "cover_html" in fixture:
            assert hashlib.sha256(html.encode()).hexdigest() == fixture["cover_html_sha256"]
        census = parse_share_census(html, period_end=fixture["period_end"])
        if fixture["symbol"] in expected:
            assert census["complete"], census
            assert [c["shares"] for c in census["classes"]] == expected[fixture["symbol"]]
        else:
            assert not census["complete"]
            if fixture["symbol"] in {"SAP", "ASX", "AHI", "REPCF", "GIL", "STN", "CIFS-HEREIN", "BNS"}:
                assert census["status"] == fixture["expected_status"]
                assert fixture["expected_reason"] in census["reasons"]
        if fixture["symbol"] == "NVO":
            assert [c["class_kind"] for c in census["classes"]] == ["other", "other"]
