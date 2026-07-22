"""Lossless CUSIP9 qualification — exact semantics ported from the pilot."""

from __future__ import annotations

from decimal import Decimal

import pytest

from src.bonds.identifiers import normalize_cusip9
from src.bonds.states import IdentifierState


@pytest.mark.parametrize(
    ("value", "state", "normalized", "transformation"),
    [
        ("  123abc789  ", IdentifierState.VALID_CUSIP9, "123ABC789", "trim_upper"),
        ("123ABC789", IdentifierState.VALID_CUSIP9, "123ABC789", "identity"),
        (None, IdentifierState.BLANK, None, "rejected"),
        (" ", IdentifierState.BLANK, None, "rejected"),
        ("000000000", IdentifierState.PLACEHOLDER, None, "rejected"),
        ("XXXXXXXXX", IdentifierState.PLACEHOLDER, None, "rejected"),
        ("N/A", IdentifierState.PLACEHOLDER, None, "rejected"),
        ("IS:123456", IdentifierState.SYNTHETIC, None, "rejected"),
        ("LE:123456", IdentifierState.SYNTHETIC, None, "rejected"),
        ("H:1234567", IdentifierState.SYNTHETIC, None, "rejected"),
        ("CIK:12345", IdentifierState.SYNTHETIC, None, "rejected"),
        ("123-45678", IdentifierState.INVALID_FORMAT, None, "rejected"),
        ("12345678", IdentifierState.INVALID_FORMAT, None, "rejected"),
        (123456789, IdentifierState.INVALID_FORMAT, None, "rejected"),
        (Decimal("123456789"), IdentifierState.INVALID_FORMAT, None, "rejected"),
    ],
)
def test_normalize_cusip9_is_reversible_and_explicit(
    value: object, state: IdentifierState, normalized: str | None, transformation: str
) -> None:
    result = normalize_cusip9(value)

    assert result.original_value == value
    assert result.state is state
    assert result.normalized_cusip9 == normalized
    assert result.transformation == transformation


def test_synthetic_prefix_case_insensitive_before_length_check() -> None:
    # Lower-case synthetic prefix is upper-cased then rejected as synthetic,
    # never repaired into a 9-char identifier.
    assert normalize_cusip9("is:abcdef").state is IdentifierState.SYNTHETIC


def test_valid_alphanumeric_uppercased_identity_vs_trim_upper() -> None:
    assert normalize_cusip9("ABCDE1234").transformation == "identity"
    assert normalize_cusip9(" abcde1234 ").transformation == "trim_upper"
    assert normalize_cusip9(" abcde1234 ").normalized_cusip9 == "ABCDE1234"
