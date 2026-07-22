"""Typed vocabulary states for identity, fields, debt eligibility, and matching.

Ported verbatim (values unchanged) from the bond pilot so the downstream
observation/matching semantics carry the exact same string state values.
"""

from __future__ import annotations

from enum import StrEnum


class FieldState(StrEnum):
    PRESENT = "present"
    NULL = "null"
    INVALID = "invalid"
    NOT_IN_SCHEMA = "not_in_schema"


class IdentifierState(StrEnum):
    VALID_CUSIP9 = "valid_cusip9"
    BLANK = "blank"
    PLACEHOLDER = "placeholder"
    SYNTHETIC = "synthetic"
    INVALID_FORMAT = "invalid_format"


class DebtState(StrEnum):
    DEBT_LIKE_ELIGIBLE = "debt_like_eligible"
    INELIGIBLE_NON_DEBT = "ineligible_non_debt"
    AMBIGUOUS_CATEGORY = "ambiguous_category"
    MISSING_CATEGORY = "missing_category"


class MatchState(StrEnum):
    INELIGIBLE_NON_DEBT = "ineligible_non_debt"
    AMBIGUOUS_CATEGORY = "ambiguous_category"
    MISSING_CATEGORY = "missing_category"
    INVALID_IDENTIFIER = "invalid_identifier"
    OUTSIDE_WINDOW_BEFORE_SOURCE = "outside_window_before_source"
    OUTSIDE_WINDOW_AFTER_CUTOFF = "outside_window_after_cutoff"
    UNMATCHED_NO_CUSIP = "unmatched_no_cusip"
    UNMATCHED_NO_PRIOR_OBSERVATION = "unmatched_no_prior_observation"
    STALE = "stale"
    UNAVAILABLE_AMBIGUOUS = "unavailable_ambiguous"
    MATCHED = "matched"
