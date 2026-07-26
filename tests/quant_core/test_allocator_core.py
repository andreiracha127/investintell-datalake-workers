from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))

from investintell_quant_core.allocator import (  # noqa: E402
    AllocatorErrorCode,
    AllocatorPolicy,
    AllocatorUniverse,
    Instrument,
    LinearConstraint,
    SleeveBand,
    compile_problem,
    project_book,
    structural_preflight,
    verify_solution,
)


def _compiled():
    universe = AllocatorUniverse(
        instruments=(
            Instrument(
                instrument_id="fund:E1",
                label="E1",
                category_id="EQUITY/IVV",
                sleeve_id="equity",
                returns=np.array([0.010, -0.020, 0.005]),
            ),
            Instrument(
                instrument_id="fund:E2",
                label="E2",
                category_id="EQUITY/IVV",
                sleeve_id="equity",
                returns=np.array([0.008, -0.018, 0.004]),
            ),
            Instrument(
                instrument_id="fund:F1",
                label="F1",
                category_id="FIXED/GOVT",
                sleeve_id="fixed_income",
                returns=np.array([0.002, 0.001, -0.001]),
            ),
        ),
        return_dates=("2026-01-02", "2026-01-05", "2026-01-06"),
        sleeve_ids=("equity", "fixed_income"),
        mapping_version="test-map-v1",
    )
    policy = AllocatorPolicy(
        sleeve_bands=(
            SleeveBand("equity", 0.2, 0.8),
            SleeveBand("fixed_income", 0.2, 0.8),
        ),
        cvar_alpha=0.95,
        cvar_limit=0.10,
        instrument_cap=0.60,
        instrument_floor=0.05,
        risk_asset_sleeves=frozenset({"equity"}),
        risk_assets_cap=0.80,
        defensive_sleeves=frozenset({"fixed_income"}),
        defensive_floor=0.20,
        instrument_linear_constraints=(
            LinearConstraint(
                coef=np.array([1.1, 0.9, 0.2]),
                lo=None,
                hi=0.75,
                label="portfolio_beta_cap",
            ),
        ),
    )
    result = compile_problem(universe, policy)
    assert result.ok
    assert result.problem is not None
    return result.problem


def test_compile_problem_snapshots_s_m_constraints_and_signature() -> None:
    problem = _compiled()

    assert problem.category_ids == ("EQUITY/IVV", "FIXED/GOVT")
    assert problem.instrument_ids == ("fund:E1", "fund:E2", "fund:F1")
    np.testing.assert_array_equal(problem.S, np.eye(2))
    np.testing.assert_array_equal(
        problem.M,
        np.array(
            [
                [0.5, 0.0],
                [0.5, 0.0],
                [0.0, 1.0],
            ]
        ),
    )
    np.testing.assert_allclose(problem.category_returns, problem.daily_returns @ problem.M)
    assert [constraint.label for constraint in problem.linear_constraints] == [
        "instrument_cap:fund:E1",
        "instrument_cap:fund:E2",
        "instrument_cap:fund:F1",
        "instrument_floor:fund:E1",
        "instrument_floor:fund:E2",
        "instrument_floor:fund:F1",
        "risk_assets_cap",
        "defensive_floor",
        "portfolio_beta_cap",
    ]
    np.testing.assert_array_equal(problem.linear_constraints[-1].coef, [1.0, 0.2])
    assert problem.signature == (
        "32c9e8acbcd5752f494f3d282f20ab6190127ef01de5d4997a94b6c919b4617a"
    )


def test_structural_preflight_and_y_equals_mx_verification() -> None:
    problem = _compiled()
    assert structural_preflight(problem).ok

    x = np.array([0.6, 0.4])
    y = project_book(problem, x)
    np.testing.assert_allclose(y, [0.3, 0.3, 0.4])

    verified = verify_solution(problem, x, y)
    assert verified.ok
    assert verified.sleeve_weights == {"equity": 0.6, "fixed_income": 0.4}
    assert verified.realized_cvar is not None


def test_instrument_decision_mode_makes_final_holdings_independent_atoms() -> None:
    legacy = _compiled()
    universe = AllocatorUniverse(
        instruments=tuple(
            Instrument(
                instrument_id=instrument_id,
                label=label,
                category_id="EQUITY/IVV" if i < 2 else "FIXED/GOVT",
                sleeve_id="equity" if i < 2 else "fixed_income",
                returns=legacy.daily_returns[:, i],
            )
            for i, (instrument_id, label) in enumerate(
                zip(legacy.instrument_ids, legacy.instrument_labels, strict=True)
            )
        ),
        return_dates=legacy.return_dates,
        sleeve_ids=legacy.sleeve_ids,
        mapping_version="test-map-v2",
        decision_mode="instrument",
    )
    result = compile_problem(
        universe,
        AllocatorPolicy(
            sleeve_bands=(
                SleeveBand("equity", 0.2, 0.8),
                SleeveBand("fixed_income", 0.2, 0.8),
            ),
            cvar_alpha=0.95,
            cvar_limit=0.10,
            instrument_cap=0.60,
        ),
    )

    assert result.ok and result.problem is not None
    problem = result.problem
    np.testing.assert_array_equal(problem.M, np.eye(3))
    np.testing.assert_array_equal(
        problem.S,
        np.array(
            [
                [1.0, 1.0, 0.0],
                [0.0, 0.0, 1.0],
            ]
        ),
    )
    assert problem.category_ids == (
        "EQUITY/IVV::fund:E1",
        "EQUITY/IVV::fund:E2",
        "FIXED/GOVT::fund:F1",
    )
    assert problem.signature != legacy.signature


def test_verifier_reports_projection_and_constraint_failures_structurally() -> None:
    problem = _compiled()
    result = verify_solution(problem, np.array([0.9, 0.1]), np.array([0.6, 0.3, 0.1]))

    assert not result.ok
    assert {issue.code for issue in result.issues} == {
        AllocatorErrorCode.CONSTRAINT_VIOLATION
    }
    assert "y=M_t x" in {issue.constraint_label for issue in result.issues}
    assert "risk_assets_cap" in {issue.constraint_label for issue in result.issues}


def test_structural_preflight_rejects_invalid_m_matrix() -> None:
    problem = _compiled()
    invalid = dataclasses.replace(problem, M=np.zeros_like(problem.M))

    result = structural_preflight(invalid)

    assert not result.ok
    assert "M_t" in {issue.constraint_label for issue in result.issues}


def test_strict_missing_sleeve_has_stable_error_code() -> None:
    universe = AllocatorUniverse(
        instruments=(
            Instrument(
                instrument_id="fund:E1",
                label="E1",
                category_id="EQUITY/IVV",
                sleeve_id="equity",
                returns=np.array([0.01, -0.01]),
            ),
        ),
        return_dates=("2026-01-02", "2026-01-05"),
        sleeve_ids=("equity", "fixed_income"),
        mapping_version="test-map-v1",
    )
    policy = AllocatorPolicy(
        sleeve_bands=(
            SleeveBand("equity", 0.2, 0.8),
            SleeveBand("fixed_income", 0.2, 0.8),
        ),
        cvar_alpha=0.95,
        cvar_limit=0.10,
        strict_missing_sleeves=True,
    )

    result = compile_problem(universe, policy)

    assert not result.ok
    assert result.issues[0].code is AllocatorErrorCode.MISSING_REQUIRED_SLEEVES
    assert result.issues[0].context == {"sleeve_id": "fixed_income"}
