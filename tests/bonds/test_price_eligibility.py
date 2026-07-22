"""DB-free tests for the pure price-eligibility predicate (src.bonds.eligibility).

The SQL half (the bond_price_is_eligible function + bond_price_eligibility_v1 view)
is exercised in tests/test_bond_price_eligibility_view.py; these tests pin the pure
Python predicate that mirrors it exactly.
"""

from __future__ import annotations

from src.bonds.eligibility import (
    ELIGIBLE_PRICE_TYPES,
    KNOWN_ACCRUED_TREATMENTS,
    eligibility_reason,
    price_observation_is_eligible,
)


def _eligible_kwargs():
    return dict(
        price_type="evaluated",
        accrued_treatment="clean",
        identity_state="resolved",
        daily_key_state="unique_in_matching_cohort",
        price_state="present",
    )


def test_fully_qualified_observation_is_eligible():
    assert price_observation_is_eligible(**_eligible_kwargs()) is True
    assert eligibility_reason(**_eligible_kwargs()) is None


def test_declared_eligible_price_types():
    assert ELIGIBLE_PRICE_TYPES == frozenset({"trade", "evaluated"})
    assert KNOWN_ACCRUED_TREATMENTS == frozenset({"clean", "dirty"})


def test_model_and_not_reported_price_types_are_ineligible():
    for pt in ("model", "not_reported"):
        kw = _eligible_kwargs() | {"price_type": pt}
        assert price_observation_is_eligible(**kw) is False
        assert eligibility_reason(**kw) == "price_type_not_eligible"


def test_unknown_accrued_treatment_is_ineligible():
    kw = _eligible_kwargs() | {"accrued_treatment": "not_reported"}
    assert price_observation_is_eligible(**kw) is False
    assert eligibility_reason(**kw) == "accrued_treatment_unknown"


def test_unresolved_identity_is_ineligible():
    kw = _eligible_kwargs() | {"identity_state": "unresolved"}
    assert price_observation_is_eligible(**kw) is False
    assert eligibility_reason(**kw) == "identity_unresolved"


def test_ambiguous_daily_key_is_ineligible():
    kw = _eligible_kwargs() | {"daily_key_state": "duplicate_in_matching_cohort"}
    assert price_observation_is_eligible(**kw) is False
    assert eligibility_reason(**kw) == "identity_ambiguous"


def test_absent_price_is_ineligible():
    for ps in ("null", "invalid"):
        kw = _eligible_kwargs() | {"price_state": ps}
        assert price_observation_is_eligible(**kw) is False
        assert eligibility_reason(**kw) == "price_absent"
