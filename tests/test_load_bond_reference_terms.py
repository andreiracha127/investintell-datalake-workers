"""The reference-terms flattener: what it emits and what it refuses to.

Pure, no DB, no network. The CSV this produces is \\copy'd straight into
``bond_reference_terms``, so a silent default here would become a published
term. Most of what is pinned below is therefore an ABSENCE.
"""

from __future__ import annotations

import json
from pathlib import Path

from scripts.load_bond_reference_terms import COLUMNS, build_rows, row_from_profile, write_csv

FULL = {
    "status": "ok", "cusip": "00033GAA3", "isin": "US00033GAA31",
    "profile": {
        "isin": "US00033GAA31", "coupon": 8.375, "maturityDate": "2029-01-15",
        "issueDate": "2024-01-22", "amountOutstanding": 525,
        "paymentFrequency": "Semi-Annual", "securityLevel": "Senior",
        "callable": True, "couponType": "Fixed", "dayCount": "",
        "debtType": "Senior Unsecured Note",
    },
}


def test_a_complete_profile_flattens_to_the_copy_columns() -> None:
    row = row_from_profile("00033GAA3", FULL, "2026-08-07")
    assert row["cusip9"] == "00033GAA3"
    assert row["coupon_rate"] == "8.375"
    assert row["maturity_date"] == "2029-01-15"
    assert row["seniority"] == "Senior"
    assert row["callable"] == "true"
    assert row["amount_outstanding_mm"] == "525.0"
    assert row["payment_frequency"] == "Semi-Annual"
    assert row["batch_label"] == "2026-08-07"


def test_a_blank_field_stays_absent_and_is_never_defaulted() -> None:
    """``dayCount`` is empty on ~90% of the cohort; an empty string is absence."""
    row = row_from_profile("00033GAA3", FULL, "b")
    assert row["day_count"] is None


def test_secured_needs_an_explicit_collateral_statement() -> None:
    """Seniority is not evidence about collateral, so it must not imply one."""
    assert row_from_profile("A00000001", {"profile": {"debtType": "Senior Unsecured Note"}}, "b")["secured"] == "unsecured"
    assert row_from_profile("A00000001", {"profile": {"debtType": "Senior Secured Bond"}}, "b")["secured"] == "secured"
    # A senior bond whose debt type says nothing about collateral: still absent.
    assert row_from_profile("A00000001", {"profile": {"securityLevel": "Senior"}}, "b")["secured"] is None
    assert row_from_profile("A00000001", {"profile": {"debtType": "Note"}}, "b")["secured"] is None


def test_no_issuer_name_can_ever_leave_this_flattener() -> None:
    """Names come from the reported filings and their consensus, never a vendor.

    Even if a profile body carried one, there is no column for it to land in.
    """
    assert not any("issuer" in column or "name" in column for column in COLUMNS)
    body = {"profile": {**FULL["profile"], "issuerName": "SOMEONE ELSE INC"}}
    assert "SOMEONE ELSE INC" not in json.dumps(row_from_profile("A00000001", body, "b"))


def test_a_non_finite_number_is_absence_not_a_value() -> None:
    row = row_from_profile("A00000001", {"profile": {"coupon": float("nan"),
                                                     "amountOutstanding": float("inf")}}, "b")
    assert row["coupon_rate"] is None
    assert row["amount_outstanding_mm"] is None


def test_malformed_and_out_of_scope_files_are_skipped_not_guessed(tmp_path: Path) -> None:
    (tmp_path / "00033GAA3.json").write_text(json.dumps(FULL), encoding="utf-8")
    (tmp_path / "BADCUSIP.json").write_text(json.dumps(FULL), encoding="utf-8")  # 8 chars
    (tmp_path / "A00000002.json").write_text("{not json", encoding="utf-8")
    (tmp_path / "A00000003.json").write_text('"a string, not a body"', encoding="utf-8")

    rows, skipped = build_rows(tmp_path, batch_label="b")
    assert [row["cusip9"] for row in rows] == ["00033GAA3"]
    assert sorted(skipped) == ["A00000002", "A00000003"]


def test_only_restricts_the_batch_to_the_declared_universe(tmp_path: Path) -> None:
    (tmp_path / "00033GAA3.json").write_text(json.dumps(FULL), encoding="utf-8")
    (tmp_path / "A00000001.json").write_text(json.dumps(FULL), encoding="utf-8")

    rows, _ = build_rows(tmp_path, batch_label="b", only={"00033GAA3"})
    assert [row["cusip9"] for row in rows] == ["00033GAA3"]


def test_the_csv_is_lf_only_because_copy_breaks_on_crlf(tmp_path: Path) -> None:
    out = tmp_path / "ref.csv"
    write_csv([row_from_profile("00033GAA3", FULL, "b")], out)
    raw = out.read_bytes()
    assert b"\r\n" not in raw
    assert raw.split(b"\n")[0].decode() == ",".join(COLUMNS)
    # An absent field is an EMPTY cell, which \copy reads as SQL NULL.
    assert b",,"  in raw
