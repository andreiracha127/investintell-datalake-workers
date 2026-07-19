"""Lossless identifier qualification for the observed bond panel."""

from __future__ import annotations

from dataclasses import dataclass

from .contracts import IdentifierState


_PLACEHOLDERS = frozenset({"000000000", "XXXXXXXXX", "NNNNNNNNN", "999999999", "N/A", "NA", "NONE", "NULL", "UNKNOWN"})
_SYNTHETIC_PREFIXES = ("IS:", "LE:", "H:", "CIK:")


@dataclass(frozen=True)
class NormalizedCusip:
    """A qualified CUSIP while retaining the exact source value."""

    original_value: object
    normalized_cusip9: str | None
    state: IdentifierState
    transformation: str


def normalize_cusip9(value: object) -> NormalizedCusip:
    """Normalize only outer whitespace and casing; never repair an identifier."""
    if value is None:
        return NormalizedCusip(value, None, IdentifierState.BLANK, "rejected")
    if not isinstance(value, str):
        return NormalizedCusip(value, None, IdentifierState.INVALID_FORMAT, "rejected")
    normalized = value.strip().upper()
    if not normalized:
        return NormalizedCusip(value, None, IdentifierState.BLANK, "rejected")
    if normalized in _PLACEHOLDERS:
        return NormalizedCusip(value, None, IdentifierState.PLACEHOLDER, "rejected")
    if normalized.startswith(_SYNTHETIC_PREFIXES):
        return NormalizedCusip(value, None, IdentifierState.SYNTHETIC, "rejected")
    if len(normalized) != 9 or not normalized.isascii() or not normalized.isalnum():
        return NormalizedCusip(value, None, IdentifierState.INVALID_FORMAT, "rejected")
    transformation = "identity" if value == normalized else "trim_upper"
    return NormalizedCusip(value, normalized, IdentifierState.VALID_CUSIP9, transformation)
