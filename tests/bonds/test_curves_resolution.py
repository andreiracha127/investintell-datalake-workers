"""DB-free tests for the pure curve resolver (src.bonds.curves.resolve_curves)."""

from __future__ import annotations

from datetime import date

from src.bonds.curves import CurveObservationInput, curve_id_for, resolve_curves


def _obs(nodes, *, currency="USD", curve_type="spot", interpolation="linear", oid="o1"):
    return CurveObservationInput(
        observation_id=oid,
        curve_date=date(2026, 6, 30),
        currency=currency,
        curve_type=curve_type,
        interpolation=interpolation,
        nodes=tuple(nodes),
        source_lineage={"engine": "test"},
    )


def test_valid_curve_resolves_with_stable_identity_and_sorted_nodes():
    result = resolve_curves([_obs([(5.0, 0.04), (1.0, 0.03), (2.0, 0.035)])])
    assert not result.rejected
    (curve,) = result.curves
    # Identity is deterministic from (currency, curve_date, curve_type).
    assert curve.curve_id == curve_id_for("USD", date(2026, 6, 30), "spot")
    # Nodes are sorted strictly increasing in tenor.
    assert [n.tenor_years for n in curve.nodes] == [1.0, 2.0, 5.0]
    assert [n.rate for n in curve.nodes] == [0.03, 0.035, 0.04]


def test_too_few_nodes_is_typed_degenerate():
    result = resolve_curves([_obs([(1.0, 0.03)])])
    assert not result.curves
    (rej,) = result.rejected
    assert rej.curve_state == "degenerate"
    assert rej.reason_code == "too_few_nodes"


def test_non_increasing_tenor_is_typed_degenerate():
    result = resolve_curves([_obs([(1.0, 0.03), (1.0, 0.035)])])  # equal tenor
    (rej,) = result.rejected
    assert rej.reason_code == "tenor_not_increasing"


def test_non_finite_rate_is_typed_degenerate():
    result = resolve_curves([_obs([(1.0, 0.03), (2.0, float("inf"))])])
    (rej,) = result.rejected
    assert rej.reason_code == "non_finite_rate"


def test_non_positive_tenor_is_typed_degenerate():
    result = resolve_curves([_obs([(0.0, 0.03), (2.0, 0.035)])])
    (rej,) = result.rejected
    assert rej.reason_code == "non_positive_tenor"


def test_unsupported_interpolation_is_typed_degenerate():
    result = resolve_curves([_obs([(1.0, 0.03), (2.0, 0.035)], interpolation="cubic")])
    (rej,) = result.rejected
    assert rej.reason_code == "unsupported_interpolation"
