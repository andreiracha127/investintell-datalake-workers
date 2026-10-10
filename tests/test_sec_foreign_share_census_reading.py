"""Independent gate reproductions and source-reading checks.

SEC_CENSUS_BASELINE_PARSER may point at an immutable git-show parser copy for
the gate-only failure baseline. Normal CI imports the working parser.
"""
from __future__ import annotations

from copy import deepcopy
from html import escape
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import re

import pytest

from scripts.load_sec_foreign_listing_evidence import pdf_parser_input
from scripts.sec_foreign_listing_parser import _Document


def parser_module():
    path = os.environ.get("SEC_CENSUS_BASELINE_PARSER")
    if not path:
        from scripts import sec_foreign_share_census_parser as current_parser
        return current_parser
    spec = importlib.util.spec_from_file_location("census_reading_baseline", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PARSER = parser_module()
PROMPT = ("Indicate the number of outstanding shares of each of the issuer's "
          "classes of capital or common stock as of the close of the period "
          "covered by the annual report: ")
CHECKMARK = "Indicate by check mark whether the registrant is a shell company."
HEADER = "For the fiscal year ended December 31, 2024"
SECTION = "PART I ITEM 1. Identity of Directors"
FIXTURES = Path(__file__).parent / "fixtures/sec_foreign_share_census/named_cover_statements.json"


def full_cover(response, *, note="", standard=True):
    response = PROMPT + response if standard else response
    return ("<p>" + HEADER + "</p><p>" + response + "</p><p>" + CHECKMARK +
            "</p><p>" + note + "</p><p>" + SECTION + "</p>")


def census(html, *, source_lines=None):
    options = {"period_end": "2024-12-31"}
    # The immutable pre-fix parser lacks PDF line transport. Running the same
    # adversarial source through its supported API reproduces its wrong count.
    if source_lines is not None and "source_lines" in inspect.signature(PARSER.parse_share_census).parameters:
        options["source_lines"] = source_lines
    return PARSER.parse_share_census(html, **options)


def assert_refusal(reading, reason):
    assert reading["complete"] is False, reading
    assert reading["status"] in {"incomplete", "conflicting"}, reading
    assert reason in reading["reasons"], reading


@pytest.mark.parametrize("symbol,note", [
    ("SAP", "Including 61,914,771 treasury shares."),
    ("ASX", "as of January 31, 2025, we had 4,416,485,537 Common Shares outstanding."),
])
def test_gate_real_cover_footnotes_survive_late_checkmark_boundary(symbol, note):
    fixture = next(row for row in json.loads(FIXTURES.read_text(encoding="utf-8")) if row["symbol"] == symbol)
    region = fixture["cover_region_text"]
    assert hashlib.sha256(region.encode()).hexdigest() == fixture["cover_region_sha256"]
    assert note in region and region.index(note) > region.index("Indicate by check mark")
    assert fixture["footnote_quote"] in region
    reading = census("<p>" + escape(HEADER + " " + region) + "</p><p>" + SECTION + "</p>")
    assert_refusal(reading, "footnoted_or_qualified_count")
    assert reading["classes"][0]["shares"] == (1228504232 if symbol == "SAP" else 4414930537)
    assert note in reading.get("cover_region_text", region)


@pytest.mark.parametrize("symbol,header,shares", [
    ("ASML", "STRATEGIC REPORT", 393283720),
    ("QGEN", "EXCHANGE RATES", 222290848),
    ("CNQ", "PRINCIPAL DOCUMENTS", 2102996000),
])
def test_actual_named_cover_section_headers_preserve_unqualified_positive_census(symbol, header, shares):
    fixture = next(row for row in json.loads(FIXTURES.read_text(encoding="utf-8")) if row["symbol"] == symbol)
    region = fixture["cover_region_text"]
    assert fixture["cover_section_header"] == header and region.endswith(header)
    assert hashlib.sha256(region.encode()).hexdigest() == fixture["cover_region_sha256"]
    assert len(region) < 60000
    reading = census("<p>" + escape(HEADER + " " + region) + "</p>")
    assert reading["complete"] and reading["status"] == "complete", reading
    assert reading["classes"][0]["shares"] == shares
    assert "cover_boundary_unresolved" not in reading["reasons"]


@pytest.mark.parametrize("symbol", ["AKO-A", "CGG", "BUR", "AQN", "RCI", "DBVT", "CIFS"])
def test_confirmed_source_cover_positives_keep_empty_cells_and_exclude_checklist_and_body(symbol):
    fixture = next(row for row in json.loads(FIXTURES.read_text(encoding="utf-8")) if row["symbol"] == symbol)
    html = fixture["cover_html"]
    assert hashlib.sha256(html.encode()).hexdigest() == fixture["cover_html_sha256"]
    assert fixture["cover_section_header"] in fixture["cover_region_text"]
    reading = PARSER.parse_share_census(html, period_end=fixture["period_end"])
    assert reading["complete"] and reading["status"] == "complete", reading
    assert reading["classes"] == fixture["expected_classes"]
    assert [entry["shares"] for entry in reading["classes"]] == fixture["expected_counts"]
    assert reading["shares_as_of"] == fixture["expected_shares_as_of"]
    if symbol == "AKO-A":
        assert hashlib.sha256(fixture["source_table_html"].encode()).hexdigest() == fixture["source_table_sha256"]
        assert fixture["source_table_rows"] == [["", "Series A Shares", "473,289,301"], ["", "Series B Shares", "473,281,303"]]
        assert [entry["class_kind"] for entry in reading["classes"]] == ["other", "other"]
    if symbol == "CGG":
        assert "files).**" in fixture["cover_region_text"]
        assert "** This requirement is not currently applicable to the registrant." in fixture["cover_region_text"]
        assert "footnoted_or_qualified_count" not in reading["reasons"]
    if symbol == "BUR":
        assert "Background on restatement of previously issued consolidated financial statements" in html
        assert "previously issued consolidated financial statements" not in reading["source_text"]


@pytest.mark.parametrize("symbol", ["AHI", "REPCF", "APWC"])
def test_report_global_share_units_refuse_but_financial_dollar_thousands_do_not(symbol):
    fixture = next(row for row in json.loads(FIXTURES.read_text(encoding="utf-8")) if row["symbol"] == symbol)
    quote = fixture["global_unit_quote"]
    assert hashlib.sha256(quote.encode()).hexdigest() == fixture["global_unit_quote_sha256"]
    assert hashlib.sha256(fixture["cover_html"].encode()).hexdigest() == fixture["cover_html_sha256"]
    reading = PARSER.parse_share_census(fixture["cover_html"], period_end=fixture["period_end"])
    assert reading["status"] == fixture["expected_status"], reading
    assert reading["classes"] == fixture["expected_classes"]
    assert reading["shares_as_of"] == fixture["expected_shares_as_of"]
    if symbol == "APWC":
        assert reading["complete"] and not reading.get("share_unit_notes"), reading
        assert "scaled_quantity" not in reading["reasons"]
        assert "footnoted_or_qualified_count" not in reading["reasons"]
    else:
        assert reading["complete"] is False
        assert fixture["expected_reason"] in reading["reasons"]
        literal_note = quote.rstrip(".")
        assert reading.get("share_unit_notes") and any(note["text"] == literal_note for note in reading["share_unit_notes"])
        assert literal_note in reading["cover_region_text"]


def test_actual_source_par_and_date_table_continuation_are_never_an_extra_share_count():
    fixture = next(row for row in json.loads(FIXTURES.read_text(encoding="utf-8")) if row["symbol"] == "JOBS")
    assert fixture["source_table_rows"] == [
        ["", ""], ["", "66,784,688 common shares, par value"],
        ["", "US$0.0001 per share, as of December 31, 2019."],
    ]
    assert hashlib.sha256(fixture["source_table_html"].encode()).hexdigest() == fixture["source_table_sha256"]
    assert hashlib.sha256(fixture["cover_html"].encode()).hexdigest() == fixture["cover_html_sha256"]
    reading = PARSER.parse_share_census(fixture["cover_html"], period_end=fixture["period_end"])
    assert reading["complete"] and reading["status"] == "complete", reading
    assert reading["classes"] == fixture["expected_classes"]
    assert reading["computed_total"] == 66784688 and reading["shares_as_of"] == "2019-12-31"
    assert not reading["numeric_residue"]


@pytest.mark.parametrize("symbol", ["BSBR", "GIL", "STN", "DIV"])
def test_actual_sources_preserve_padded_columns_and_strict_row_sign_and_unit_scope_rules(symbol):
    fixture = next(row for row in json.loads(FIXTURES.read_text(encoding="utf-8")) if row["symbol"] == symbol)
    html = fixture["cover_html"]
    assert hashlib.sha256(html.encode()).hexdigest() == fixture["cover_html_sha256"]
    if "source_table_html" in fixture:
        assert hashlib.sha256(fixture["source_table_html"].encode()).hexdigest() == fixture["source_table_sha256"]
    reading = PARSER.parse_share_census(html, period_end=fixture["period_end"])
    assert reading["status"] == fixture["expected_status"], reading
    assert reading["classes"] == fixture["expected_classes"]
    assert reading["shares_as_of"] == fixture["expected_shares_as_of"]
    if symbol == "BSBR":
        assert fixture["source_table_rows"] == [
            ["Title of Class", "", "Number of Shares Outstanding", ""],
            ["Common shares", "", "", "3,850,970,714", ""],
            ["Preferred shares", "", "", "3,712,111,703", ""],
        ]
        assert reading["complete"] and reading["computed_total"] == 7563082417
        assert [entry["class_kind"] for entry in reading["classes"]] == ["ordinary", "preferred"]
    elif symbol == "DIV":
        quote = fixture["financial_statements_unit_quote"]
        assert hashlib.sha256(quote.encode()).hexdigest() == fixture["financial_statements_unit_quote_sha256"]
        assert reading["complete"] and reading["computed_total"] == 11043027
        assert "financial statements" in quote.lower() and not reading.get("share_unit_notes")
    else:
        # These are explicit contract refusals, rather than numerical errors.
        assert reading["complete"] is False and fixture["expected_reason"] in reading["reasons"]
        if symbol == "GIL":
            assert fixture["source_table_rows"] == [["Common Shares:"], ["29,698,706"]]
            assert reading["classes"][0]["shares"] is None
        else:
            assert "2006 – 45,257,451" in fixture["source_text"]
            assert reading["classes"][0]["shares"] == 45257451


@pytest.mark.parametrize("symbol", ["IMOS", "SHELL-2009", "SEK", "CIFS-HEREIN"])
def test_source_qualifier_meaning_and_unrestricted_share_unit_scope_are_preserved(symbol):
    fixture = next(row for row in json.loads(FIXTURES.read_text(encoding="utf-8")) if row["symbol"] == symbol)
    html = fixture["cover_html"]
    assert hashlib.sha256(html.encode()).hexdigest() == fixture["cover_html_sha256"]
    reading = PARSER.parse_share_census(html, period_end=fixture["period_end"])
    assert reading["status"] == fixture["expected_status"], reading
    assert reading["classes"] == fixture["expected_classes"]
    assert reading["shares_as_of"] == fixture["expected_shares_as_of"]
    if symbol == "CIFS-HEREIN":
        quote = fixture["global_unit_quote"]
        assert "presented herein" in quote and "Annual Report" not in quote
        assert hashlib.sha256(quote.encode()).hexdigest() == fixture["global_unit_quote_sha256"]
        assert reading["complete"] is False and fixture["expected_reason"] in reading["reasons"]
        assert any(note["text"] == quote for note in reading["share_unit_notes"])
    else:
        quote = fixture["unrelated_qualifier_quote"]
        assert hashlib.sha256(quote.encode()).hexdigest() == fixture["unrelated_qualifier_quote_sha256"]
        assert reading["complete"] and "footnoted_or_qualified_count" not in reading["reasons"]
        if symbol == "IMOS":
            assert "each representing 20 common shares" in quote
        elif symbol == "SHELL-2009":
            assert "volumes of hydrocarbons" in quote and "royalty payments in kind" in quote
        else:
            assert "Medium-Term Notes, Series D" in quote and "Index" in quote and "Total Return" in quote


@pytest.mark.parametrize("symbol", ["BORR", "AKAN"])
def test_explicit_audited_financial_statement_and_mda_scope_does_not_qualify_cover_count(symbol):
    fixture = next(row for row in json.loads(FIXTURES.read_text(encoding="utf-8")) if row["symbol"] == symbol)
    quote = fixture["section_only_unit_quote"]
    assert hashlib.sha256(quote.encode()).hexdigest() == fixture["section_only_unit_quote_sha256"]
    assert hashlib.sha256(fixture["cover_html"].encode()).hexdigest() == fixture["cover_html_sha256"]
    reading = PARSER.parse_share_census(fixture["cover_html"], period_end=fixture["period_end"])
    assert reading["complete"] and reading["status"] == "complete", reading
    assert reading["classes"] == fixture["expected_classes"]
    assert reading["shares_as_of"] == fixture["expected_shares_as_of"]
    assert not reading.get("share_unit_notes")
    assert "footnoted_or_qualified_count" not in reading["reasons"]
    if symbol == "BORR":
        assert "in these Consolidated Audited Financial Statements" in quote
    else:
        assert "management’s discussion and analysis" in quote and "Audited Consolidated Financial Statements" in quote


def test_actual_bns_preferred_group_and_indented_series_refuse_unsupported_hierarchy():
    fixture = next(row for row in json.loads(FIXTURES.read_text(encoding="utf-8")) if row["symbol"] == "BNS")
    assert hashlib.sha256(fixture["source_table_html"].encode()).hexdigest() == fixture["source_table_sha256"]
    assert hashlib.sha256(fixture["cover_html"].encode()).hexdigest() == fixture["cover_html_sha256"]
    rows = fixture["source_table_rows"]
    assert any(any("Common" in cell for cell in row) for row in rows)
    assert any(any("Preferred" in cell for cell in row) and not any(re.search(r"\d", cell) for cell in row) for row in rows)
    assert any(any("Series 11" in cell for cell in row) for row in rows)
    assert any(any("Series 12" in cell for cell in row) for row in rows)
    reading = PARSER.parse_share_census(fixture["cover_html"], period_end=fixture["period_end"])
    # The literal arithmetic is not disputed. A group label above shifted
    # series rows does not establish the required same-row association, and
    # this reader provides no hierarchy or preferred-kind inheritance.
    assert reading["complete"] is False and reading["status"] == "incomplete", reading
    assert "class_count_unresolved" in reading["reasons"] or "count_class_row_mismatch" in reading["reasons"]
    assert any(entry["shares"] is None for entry in reading["classes"])


@pytest.mark.parametrize("marker,note", [
    ("*", "* Includes both Class A and Class B ordinary shares."),
    ("**", "** Including 20 treasury shares."),
    ("†", "† Count combines Class A and Class B shares."),
])
def test_gate_late_cover_footnote_invalidates_truncated_sole_class_statement(marker, note):
    reading = census(full_cover("Ordinary shares: 100" + marker, note=note))
    assert_refusal(reading, "footnoted_or_qualified_count")


@pytest.mark.parametrize("note", [
    "The count shown above represents issued share capital.",
    "Twenty ordinary shares are held by the issuer.",
    "Outstanding net of treasury shares is 80.",
    "These counts refer to ADSs.",
    "The aggregate ordinary capital includes Class A and Class B.",
    "Ordinary shares outstanding are stated net of shares held by subsidiaries.",
])
def test_late_cover_qualifications_without_markers_cannot_certify_source_supply(note):
    assert_refusal(census(full_cover("Ordinary shares: 100", note=note)), "footnoted_or_qualified_count")


@pytest.mark.parametrize("note", [
    "Class B ordinary shares: 25.",
    "There is also a Class B ordinary share class with 25 shares outstanding.",
    "The issuer has another class, Class B ordinary shares, numbering 25.",
])
def test_late_cover_positive_named_class_cannot_be_ignored_or_subtracted_from_initial_count(note):
    reading = census(full_cover("Ordinary shares: 100", note=note))
    assert reading["complete"] is False and reading["status"] == "incomplete", reading
    assert {"additional_cover_class", "footnoted_or_qualified_count"} & set(reading["reasons"]), reading
    assert reading["classes"] == [{"class_name": "Ordinary shares", "class_key": "ordinary", "class_kind": "ordinary", "shares": 100}]
    assert reading["computed_total"] == 100 and reading["stated_total"] is None


@pytest.mark.parametrize("connector", ["&", "and"])
def test_gate_bare_a_combined_with_b_cannot_disappear_as_article_glue(connector):
    reading = census(full_cover("A " + connector + " B ordinary shares: 100"))
    assert_refusal(reading, "unresolved_bare_class_token")


def test_gate_unparsed_bare_a_blank_table_class_prevents_single_class_proof():
    response = ("<table><tr><th>Class of shares</th><th>Number outstanding</th></tr>"
                "<tr><td>Ordinary shares</td><td>100</td></tr><tr><td>A</td><td></td></tr></table>")
    assert_refusal(census(full_cover(response)), "unresolved_bare_class_token")


@pytest.mark.parametrize("label", ["Α", "Д", "普通股"])
def test_gate_unparsed_unicode_class_label_in_blank_row_prevents_sole_class_proof(label):
    response = ("<table><tr><th>Class of shares</th><th>Number outstanding</th></tr>"
                "<tr><td>Ordinary shares</td><td>100</td></tr><tr><td>" + label + "</td><td></td></tr></table>")
    reading = census(full_cover(response))
    assert reading["complete"] is False and reading["status"] == "incomplete", reading
    assert {"unparsed_class_or_text_residue", "class_count_unresolved"} & set(reading["reasons"]), reading
    ordinary = [entry for entry in reading["classes"] if entry["class_key"] == "ordinary"]
    assert len(ordinary) == 1 and ordinary[0]["shares"] == 100


@pytest.mark.parametrize("response,reason", [
    ("<table><tr><td>Ordinary shares</td><td>100</td><td>200</td></tr></table>", "ambiguous_row_numbers"),
    ("<table><tr><td>Ordinary shares</td><td>100</td></tr><tr><td>200</td></tr></table>", "ambiguous_numeric_cells"),
    ("<table><tr><td>Ordinary shares</td></tr><tr><td>100</td></tr><tr><td>200</td></tr></table>", "ambiguous_numeric_cells"),
])
def test_gate_numeric_cells_never_form_an_invented_space_grouped_quantity(response, reason):
    reading = census(full_cover(response))
    assert_refusal(reading, reason)
    assert not any(entry["shares"] == 100200 for entry in reading["classes"])


def test_gate_blank_class_a_cell_cannot_borrow_next_rows_leading_count():
    response = ("<table><tr><td>Class A ordinary shares</td><td></td></tr>"
                "<tr><td>100</td><td>Class B ordinary shares</td><td>200</td></tr></table>")
    reading = census(full_cover(response))
    assert_refusal(reading, "class_count_unresolved")
    class_a = [entry for entry in reading["classes"] if entry["class_key"] == "class:a"]
    assert len(class_a) == 1 and class_a[0]["shares"] is None


def test_named_class_and_number_columns_prevent_switching_count_direction_per_row():
    response = ("<table><tr><th>Class of shares</th><th>Number outstanding</th></tr>"
                "<tr><td>A ordinary shares</td><td>100</td></tr>"
                "<tr><td>200</td><td>B ordinary shares</td></tr></table>")
    reading = census(full_cover(response))
    assert_refusal(reading, "count_class_row_mismatch")
    class_b = [entry for entry in reading["classes"] if entry["class_key"] == "class:b"]
    assert len(class_b) == 1 and class_b[0]["shares"] is None


def test_gate_pdf_unexplained_numeric_lines_refuse_instead_of_merging():
    page = HEADER + "\n" + PROMPT + "\nOrdinary shares\n100\n200\n" + CHECKMARK + "\n" + SECTION
    html, _, _ = pdf_parser_input([page])
    reading = census(_Document(html), source_lines=page.splitlines())
    assert_refusal(reading, "ambiguous_numeric_cells")
    assert not any(entry["shares"] == 100200 for entry in reading["classes"])


@pytest.mark.parametrize("quantity", [
    "–100", "—100", "‒100", "−100", "-100", "(100)",
    "－100", "﹣100", "⁻100", "₋100", "―100", "﹘100", "（100）",
])
def test_gate_unsupported_sign_notation_cannot_be_positive_supply(quantity):
    assert_refusal(census(full_cover("Ordinary shares: " + quantity)), "unsupported_sign_notation")


@pytest.mark.parametrize("response", [
    "1.5 million ordinary shares", "1 thousand ordinary shares", "1 billion ordinary shares",
    "Ordinary shares: 100 (in thousands)",
    "<table><tr><th>Class of shares</th><th>Number outstanding (in thousands)</th></tr>"
    "<tr><td>Ordinary shares</td><td>100</td></tr></table>",
])
def test_gate_scaled_quantities_remain_incomplete_even_with_exact_arithmetic(response):
    reading = census(full_cover(response))
    assert_refusal(reading, "scaled_quantity")
    if response == "1.5 million ordinary shares":
        assert PARSER._count("1.5 million") is None
        assert reading["classes"][0]["shares"] is None


@pytest.mark.parametrize("second_standard", [False, True])
def test_gate_all_custom_and_standard_declarations_participate_in_disagreement(second_standard):
    first = "The number of outstanding Ordinary shares as of December 31, 2024 was 100."
    second = (PROMPT + "Ordinary shares: 200 as of December 31, 2024." if second_standard else
              "The number of outstanding Ordinary shares as of December 31, 2024 was 200.")
    html = "<p>" + HEADER + "</p><p>" + first + "</p><p>" + CHECKMARK + "</p><p>" + second + "</p><p>" + SECTION + "</p>"
    reading = census(html)
    assert_refusal(reading, "disagreeing_cover_statements")
    assert "multiple_cover_statements" in reading["reasons"]


def test_standard_question_with_nested_custom_answer_remains_one_complete_census():
    answer = "The number of outstanding Ordinary shares as of December 31, 2024 was 100."
    reading = census(full_cover(answer))
    assert reading["complete"] and reading["status"] == "complete", reading
    assert reading["classes"] == [{"class_name": "Ordinary shares", "class_key": "ordinary", "class_kind": "ordinary", "shares": 100}]
    assert "disagreeing_cover_statements" not in reading["reasons"]


@pytest.mark.parametrize("first_kind,second_kind", [
    ("custom", "custom"), ("custom", "standard"),
    ("standard", "custom"), ("standard", "standard"),
])
def test_equal_custom_and_standard_cover_declarations_remain_complete(first_kind, second_kind):
    def declaration(kind):
        return ("The number of outstanding Ordinary shares as of December 31, 2024 was 100." if kind == "custom" else
                PROMPT + "Ordinary shares: 100 as of December 31, 2024.")
    html = ("<p>" + HEADER + "</p><p>" + declaration(first_kind) + "</p><p>" + CHECKMARK +
            "</p><p>" + declaration(second_kind) + "</p><p>" + SECTION + "</p>")
    reading = census(html)
    assert reading["complete"] and reading["status"] == "complete", reading
    assert reading["classes"][0]["shares"] == 100
    assert "disagreeing_cover_statements" not in reading["reasons"]


def test_gate_unresolved_cover_boundary_refuses_arbitrary_maximum_length_clipping():
    html = ("<p>" + HEADER + "</p><p>" + PROMPT + "Ordinary shares: 100</p><p>" + CHECKMARK +
            "</p><p>" + "Further cover text " * 5000 + "</p>")
    assert_refusal(census(html), "cover_boundary_unresolved")


def proposed_complete(*, class_name="Ordinary shares", class_key="ordinary", shares=100):
    return {"classes": [{"class_name": class_name, "class_key": class_key, "class_kind": "ordinary", "shares": shares}],
            "shares_as_of": "2024-12-31", "period_end": "2024-12-31", "date_explicit": True,
            "stated_total": None, "computed_total": shares, "complete": True,
            "conflicting": False, "status": "complete", "reasons": []}


def checked(span, region, candidate):
    status, reasons = PARSER.check_reading(span, region, candidate)
    assert status in {"complete", "incomplete", "conflicting", "none"}
    assert isinstance(reasons, list) and all(isinstance(reason, str) for reason in reasons)
    return status, reasons


@pytest.mark.parametrize("span,note,candidate,reason", [
    ("Ordinary shares: 100*", "* Includes Class A and Class B shares.", proposed_complete(), "footnoted_or_qualified_count"),
    ("A & B ordinary shares: 100", "", proposed_complete(class_name="B ordinary shares", class_key="class:b"), "unresolved_bare_class_token"),
    ("Ordinary shares\t100\t200", "", proposed_complete(shares=100200), "ambiguous_row_numbers"),
    ("Ordinary shares\n100\n200", "", proposed_complete(shares=100200), "ambiguous_numeric_cells"),
    ("Ordinary shares: –100", "", proposed_complete(), "unsupported_sign_notation"),
    ("1.5 million ordinary shares", "", proposed_complete(shares=1500000), "scaled_quantity"),
    ("Class A ordinary shares: 100 or Class B ordinary shares: 200", "", proposed_complete(class_name="Class A ordinary shares", class_key="class:a"), "alternative_or_conditional_statement"),
])
def test_check_reading_rejects_supplied_false_complete_candidate_from_literal_source(span, note, candidate, reason):
    span = PROMPT + span
    region = HEADER + "\n" + span + "\n" + CHECKMARK + "\n" + note + "\n" + SECTION
    status, reasons = checked(span, region, deepcopy(candidate))
    assert status in {"incomplete", "conflicting"} and reason in reasons, (status, reasons)


def test_check_reading_does_not_trust_candidate_completeness_or_reason_flags():
    span = PROMPT + "Ordinary shares: 100 as of December 31, 2024."
    region = HEADER + "\n" + span + "\n" + CHECKMARK + "\n" + SECTION
    candidate = proposed_complete()
    candidate.update(complete=False, status="incomplete", reasons=["pretend_refusal"])
    assert checked(span, region, candidate) == ("complete", [])


def test_check_reading_standard_question_with_nested_custom_answer_is_one_declaration():
    span = PROMPT + "The number of outstanding Ordinary shares as of December 31, 2024 was 100."
    region = HEADER + "\n" + span + "\n" + CHECKMARK + "\n" + SECTION
    assert checked(span, region, proposed_complete()) == ("complete", [])


def test_check_reading_finite_complete_cover_eof_is_an_actual_boundary():
    span = PROMPT + "Ordinary shares: 100 as of December 31, 2024."
    region = HEADER + "\n" + span + "\n" + CHECKMARK
    assert checked(span, region, proposed_complete()) == ("complete", [])


@pytest.mark.parametrize("field,value,reason", [
    ("shares", 101, "candidate_count_mismatch"),
    ("class_name", "Common shares", "candidate_class_mismatch"),
    ("class_key", "class:b", "candidate_class_mismatch"),
    ("class_kind", "preferred", "candidate_class_mismatch"),
])
def test_check_reading_matches_proposed_class_and_count_to_literal_source(field, value, reason):
    span = PROMPT + "Ordinary shares: 100 as of December 31, 2024."
    region = HEADER + "\n" + span + "\n" + CHECKMARK + "\n" + SECTION
    candidate = proposed_complete()
    candidate["classes"][0][field] = value
    if field == "shares":
        candidate["computed_total"] = value
    status, reasons = checked(span, region, candidate)
    assert status in {"incomplete", "conflicting"} and reason in reasons


def test_check_reading_forged_structured_span_cannot_override_actual_blank_cover_cell():
    response = ("<table><tr><td>Class A ordinary shares</td><td></td></tr>"
                "<tr><td>100</td><td>Class B ordinary shares</td><td>200</td></tr></table>")
    actual_cover = full_cover(response)
    forged_span = PROMPT + "\nClass A ordinary shares\t100\nClass B ordinary shares\t200"
    candidate = proposed_complete(class_name="Class A ordinary shares", class_key="class:a", shares=100)
    candidate["classes"].append({"class_name": "Class B ordinary shares", "class_key": "class:b", "class_kind": "ordinary", "shares": 200})
    candidate["computed_total"] = 300
    status, reasons = checked(forged_span, actual_cover, candidate)
    assert status in {"incomplete", "conflicting"}, (status, reasons)
    assert "class_count_unresolved" in reasons or "candidate_source_mismatch" in reasons, reasons


def test_check_reading_actual_named_columns_override_proposed_per_row_direction():
    response = ("<table><tr><th>Class of shares</th><th>Number outstanding</th></tr>"
                "<tr><td>A ordinary shares</td><td>100</td></tr>"
                "<tr><td>200</td><td>B ordinary shares</td></tr></table>")
    candidate = proposed_complete(class_name="A ordinary shares", class_key="class:a", shares=100)
    candidate["classes"].append({"class_name": "B ordinary shares", "class_key": "class:b", "class_kind": "ordinary", "shares": 200})
    candidate["computed_total"] = 300
    span = PROMPT + "\nClass of shares\tNumber outstanding\nA ordinary shares\t100\n200\tB ordinary shares"
    status, reasons = checked(span, full_cover(response), candidate)
    assert status in {"incomplete", "conflicting"} and "count_class_row_mismatch" in reasons


@pytest.mark.parametrize("actual,forged,shares,reason", [
    ("Ordinary shares: –100", "Ordinary shares: 100", 100, "unsupported_sign_notation"),
    ("Ordinary shares: (100)", "Ordinary shares: 100", 100, "unsupported_sign_notation"),
    ("1 thousand ordinary shares", "Ordinary shares: 1,000", 1000, "scaled_quantity"),
    ("approximately 100 ordinary shares", "Ordinary shares: 100", 100, "footnoted_or_qualified_count"),
    ("Total ordinary shares: 100", "Ordinary shares: 100", 100, "aggregate_only_statement"),
])
def test_check_reading_forged_span_cannot_remove_actual_source_qualification(actual, forged, shares, reason):
    actual_cover = full_cover(actual)
    status, reasons = checked(PROMPT + forged, actual_cover, proposed_complete(shares=shares))
    assert status in {"incomplete", "conflicting"} and reason in reasons, (status, reasons)


def test_check_reading_forged_span_cannot_remove_actual_or_between_classes():
    actual_cover = full_cover("Class A ordinary shares: 100 or Class B ordinary shares: 200")
    forged_span = PROMPT + "Class A ordinary shares: 100; Class B ordinary shares: 200"
    candidate = proposed_complete(class_name="Class A ordinary shares", class_key="class:a", shares=100)
    candidate["classes"].append({"class_name": "Class B ordinary shares", "class_key": "class:b", "class_kind": "ordinary", "shares": 200})
    candidate["computed_total"] = 300
    status, reasons = checked(forged_span, actual_cover, candidate)
    assert status in {"incomplete", "conflicting"} and "alternative_or_conditional_statement" in reasons


@pytest.mark.parametrize("note", [
    "Class B ordinary shares: 25.",
    "There is also a Class B ordinary share class with 25 shares outstanding.",
    "The issuer has another class, Class B ordinary shares, numbering 25.",
])
def test_check_reading_late_positive_named_class_invalidates_supplied_sole_class_scope(note):
    span = PROMPT + "Ordinary shares: 100"
    actual_cover = full_cover("Ordinary shares: 100", note=note)
    status, reasons = checked(span, actual_cover, proposed_complete())
    assert status in {"incomplete", "conflicting"}
    assert {"additional_cover_class", "footnoted_or_qualified_count"} & set(reasons), reasons


def test_check_reading_compares_a_supplied_custom_candidate_with_standard_alternative():
    span = "The number of outstanding Ordinary shares as of December 31, 2024 was 100."
    alternative = PROMPT + "Ordinary shares: 200 as of December 31, 2024."
    region = HEADER + "\n" + span + "\n" + CHECKMARK + "\n" + alternative + "\n" + SECTION
    status, reasons = checked(span, region, proposed_complete())
    assert status in {"incomplete", "conflicting"} and "disagreeing_cover_statements" in reasons


def test_reading_test_and_named_cover_fixture_use_lf_only():
    for path in (Path(__file__), FIXTURES):
        assert b"\r" not in path.read_bytes(), path.name
