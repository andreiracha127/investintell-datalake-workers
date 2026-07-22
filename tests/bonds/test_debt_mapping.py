"""Exact composite-triple debt classification — no fuzzy match, no normalization."""

from __future__ import annotations

import pytest

from src.bonds.debt_mapping import DebtMapping
from src.bonds.errors import BondError
from src.bonds.states import DebtState


_FIXTURE_RULES = (
    ("fixture_debt", "fixture_asset", "fixture_structure", "eligible_debt"),
    ("fixture_non_debt", "fixture_asset", "fixture_structure", "non_debt_excluded"),
    ("synthetic_mbs", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
    ("synthetic_abs", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
    ("synthetic_clo", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
    ("synthetic_loan", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
    ("synthetic_repo", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
)


@pytest.fixture
def mapping() -> DebtMapping:
    return DebtMapping(rules=_FIXTURE_RULES)


def test_mapping_classifies_exact_composites(mapping: DebtMapping) -> None:
    assert mapping.classify("fixture_debt", "fixture_asset", "fixture_structure") is DebtState.DEBT_LIKE_ELIGIBLE
    assert mapping.classify("fixture_debt", "fixture_asset", "fixture_structure ") is DebtState.AMBIGUOUS_CATEGORY
    assert mapping.classify(None, "fixture_asset", "fixture_structure") is DebtState.MISSING_CATEGORY
    assert mapping.classify("fixture_debt", " ", "fixture_structure") is DebtState.AMBIGUOUS_CATEGORY
    assert mapping.classify("unseen", "fixture_asset", "fixture_structure") is DebtState.AMBIGUOUS_CATEGORY


@pytest.mark.parametrize("field", [0, 1, 2])
def test_missing_component_is_missing_category(mapping: DebtMapping, field: int) -> None:
    for value in (None, ""):
        components = ["fixture_debt", "fixture_asset", "fixture_structure"]
        components[field] = value
        assert mapping.classify(*components) is DebtState.MISSING_CATEGORY


def test_mapping_does_not_normalize_or_near_match(mapping: DebtMapping) -> None:
    for components in (
        ("fixture_debt ", "fixture_asset", "fixture_structure"),
        ("fixture_debt", "fixture-asset", "fixture_structure"),
        ("fixture_debt", "fixture_asset", "fixture structure"),
    ):
        assert mapping.classify(*components) is DebtState.AMBIGUOUS_CATEGORY


@pytest.mark.parametrize(
    "issuer",
    ["synthetic_mbs", "synthetic_abs", "synthetic_clo", "synthetic_loan", "synthetic_repo"],
)
def test_exclusion_families_require_explicit_decision_rows(mapping: DebtMapping, issuer: str) -> None:
    assert mapping.classify(issuer, "synthetic_asset", "synthetic_structure") is DebtState.INELIGIBLE_NON_DEBT


def test_non_string_component_is_ambiguous(mapping: DebtMapping) -> None:
    assert mapping.classify("fixture_debt", 7, "fixture_structure") is DebtState.AMBIGUOUS_CATEGORY


def test_mapping_accepts_dict_rules() -> None:
    mapping = DebtMapping(
        rules=[
            {
                "issuer_category": "corp",
                "asset_class": "debt",
                "instrument_structure": "bond",
                "decision": "eligible_debt",
            }
        ]
    )
    assert mapping.classify("corp", "debt", "bond") is DebtState.DEBT_LIKE_ELIGIBLE


def test_empty_rules_are_rejected() -> None:
    # Donor parity: an empty composite table is never a valid mapping.
    with pytest.raises(BondError, match="invalid_debt_mapping"):
        DebtMapping(rules=[])


def test_duplicate_composite_rules_are_rejected() -> None:
    with pytest.raises(BondError, match="duplicate_debt_rule"):
        DebtMapping(
            rules=[
                ("i", "a", "s", "eligible_debt"),
                ("i", "a", "s", "non_debt_excluded"),
            ]
        )


@pytest.mark.parametrize(
    "rules",
    [
        [("i", "a", "s", "unknown_decision")],
        [("i", "a", "")],
        [("i", "a", "", "eligible_debt")],
        [("i", "a", 7, "eligible_debt")],
    ],
)
def test_invalid_rule_shapes_are_rejected(rules: list[object]) -> None:
    with pytest.raises(BondError, match="invalid_debt_mapping"):
        DebtMapping(rules=rules)
