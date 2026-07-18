from __future__ import annotations

from dataclasses import replace

import pytest


def test_non_finite_decimal_is_quarantined_without_losing_lexical_value() -> None:
    from src.nport.schema import load_nport_contract, parse_row

    table = load_nport_contract().table_for_filename("DEBT_SECURITY.tsv")
    values = tuple("NaN" if column.name == "ANNUALIZED_RATE" else "holding-1" for column in table.columns)

    parsed = parse_row(table.columns, values)

    issue = next(issue for issue in parsed.issues if issue.column_name == "ANNUALIZED_RATE")
    assert issue.code == "invalid_decimal"
    assert issue.raw_value == "NaN"
    assert parsed.lexical["ANNUALIZED_RATE"] == "NaN"


def test_text_max_length_is_enforced_with_lexical_evidence() -> None:
    from src.nport.schema import load_nport_contract, parse_row

    table = load_nport_contract().table_for_filename("SUBMISSION.tsv")
    accession = next(column for column in table.columns if column.name == "ACCESSION_NUMBER")
    column = replace(accession, required=False, datatype={"base": "string", "maxLength": 3})

    parsed = parse_row((column,), ("TOO-LONG",))

    assert parsed.parse_status == "quarantined"
    assert parsed.lexical["ACCESSION_NUMBER"] == "TOO-LONG"
    assert parsed.issues[0].code == "invalid_text"


@pytest.mark.parametrize("raw", ["1-Jan-2024", "01-jan-2024", "2024-1-01", "2024-01-1"])
def test_dates_require_exact_anchored_governed_grammar(raw: str) -> None:
    from src.nport.schema import load_nport_contract, parse_row

    table = load_nport_contract().table_for_filename("SUBMISSION.tsv")
    values = tuple(raw if column.name == "FILING_DATE" else "" for column in table.columns)
    parsed = parse_row(table.columns, values)
    issue = next(issue for issue in parsed.issues if issue.column_name == "FILING_DATE")
    assert (issue.code, issue.raw_value, issue.detail) == (
        "invalid_date", raw, "data inválida para o campo",
    )


@pytest.mark.parametrize(
    ("raw", "expected_code"),
    [
        ("1e131071", None),
        ("1e131072", "decimal_out_of_domain"),
        ("1e-16383", None),
        ("1e-16384", "decimal_out_of_domain"),
        ("1e200000", "decimal_out_of_domain"),
        ("NaN", "invalid_decimal"),
        ("Infinity", "invalid_decimal"),
    ],
)
def test_decimal_domain_is_bounded_before_materialization(raw: str, expected_code: str | None) -> None:
    from src.nport.schema import load_nport_contract, parse_row

    table = load_nport_contract().table_for_filename("FUND_REPORTED_HOLDING.tsv")
    values = tuple(raw if column.name == "BALANCE" else "" for column in table.columns)
    parsed = parse_row(table.columns, values)
    issue = next((item for item in parsed.issues if item.column_name == "BALANCE"), None)
    assert (issue.code if issue else None) == expected_code
