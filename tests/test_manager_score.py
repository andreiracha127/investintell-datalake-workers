"""Unit tests for the equity composite manager_score (pure, no DB)."""

from __future__ import annotations

import math

import pytest

from src.workers import manager_score as ms


# ── normalize ────────────────────────────────────────────────────────────────
def test_normalize_midpoint_value_scores_50():
    score, synth = ms.normalize_with_provenance(0.10, -0.20, 0.40)
    assert synth is False
    assert score == pytest.approx(50.0)


def test_normalize_clamps_above_max_to_100():
    score, synth = ms.normalize_with_provenance(0.80, -0.20, 0.40)
    assert synth is False
    assert score == 100.0


def test_normalize_clamps_below_min_to_0():
    score, synth = ms.normalize_with_provenance(-0.50, -0.20, 0.40)
    assert synth is False
    assert score == 0.0


def test_normalize_degenerate_range_returns_50():
    score, synth = ms.normalize_with_provenance(5.0, 1.0, 1.0)
    assert synth is False
    assert score == 50.0


def test_normalize_missing_with_peer_median_applies_minus_5_penalty():
    score, synth = ms.normalize_with_provenance(None, -1.0, 3.0, peer_median=60.0)
    assert synth is True
    assert score == pytest.approx(55.0)


def test_normalize_missing_peer_median_penalty_floored_at_zero():
    score, synth = ms.normalize_with_provenance(None, -1.0, 3.0, peer_median=3.0)
    assert synth is True
    assert score == 0.0


def test_normalize_missing_no_peer_median_falls_back_to_45():
    score, synth = ms.normalize_with_provenance(None, -1.0, 3.0, peer_median=None)
    assert synth is True
    assert score == 45.0


def test_normalize_nonfinite_treated_as_missing():
    score, synth = ms.normalize_with_provenance(float("nan"), -1.0, 3.0, peer_median=None)
    assert synth is True
    assert score == 45.0


# ── peaked (ported helper, retained for parity with legacy scoring_service) ────
def test_peaked_at_target_is_100():
    assert ms.peaked_score(1.0, target=1.0, half_range=1.0) == 100.0


def test_peaked_at_half_range_is_0():
    assert ms.peaked_score(2.0, target=1.0, half_range=1.0) == 0.0


def test_peaked_missing_returns_45():
    assert ms.peaked_score(None, target=1.0, half_range=1.0) == 45.0


# ── weights ──────────────────────────────────────────────────────────────────
def test_weights_sum_to_one():
    assert math.isclose(sum(ms.EQUITY_MANAGER_SCORE_WEIGHTS.values()), 1.0, abs_tol=1e-9)


def test_weights_are_legacy_renormalized():
    # Legacy risk weights 0.20/0.25/0.20/0.15 (sum 0.80) renormalized /0.80.
    assert ms.EQUITY_MANAGER_SCORE_WEIGHTS["return_consistency"] == pytest.approx(0.25)
    assert ms.EQUITY_MANAGER_SCORE_WEIGHTS["risk_adjusted_return"] == pytest.approx(0.3125)
    assert ms.EQUITY_MANAGER_SCORE_WEIGHTS["drawdown_control"] == pytest.approx(0.25)
    assert ms.EQUITY_MANAGER_SCORE_WEIGHTS["information_ratio"] == pytest.approx(0.1875)


def test_weights_have_no_fee_or_flows_or_robust_sharpe():
    # fee_efficiency / flows_momentum are unavailable in fund_risk_metrics;
    # there is no 'robust_sharpe' component in the legacy engine.
    assert "fee_efficiency" not in ms.EQUITY_MANAGER_SCORE_WEIGHTS
    assert "flows_momentum" not in ms.EQUITY_MANAGER_SCORE_WEIGHTS
    assert "robust_sharpe" not in ms.EQUITY_MANAGER_SCORE_WEIGHTS


# ── composite ────────────────────────────────────────────────────────────────
def test_composite_all_present_is_weighted_mean_in_range():
    metrics = {
        "return_1y": 0.10,
        "sharpe_1y": 1.0,
        "sharpe_cf": 1.0,
        "max_drawdown_1y": -0.25,
        "information_ratio_1y": 0.5,
    }
    result = ms.compute_equity_manager_score(metrics)
    assert result.degraded is False
    assert result.degraded_components == []
    # Every sub-score lands in [0, 100]; the composite is their weighted mean.
    expected = sum(
        result.components[name] * w
        for name, w in ms.EQUITY_MANAGER_SCORE_WEIGHTS.items()
    )
    assert result.score == pytest.approx(round(expected, 2))
    assert 0.0 <= result.score <= 100.0
    assert set(result.components) == set(ms.EQUITY_MANAGER_SCORE_WEIGHTS)


def test_composite_prefers_sharpe_cf_over_sharpe_1y_for_risk_adjusted():
    # risk_adjusted_return reads sharpe_cf when present; sharpe_1y differs but
    # must NOT change the risk_adjusted_return sub-score.
    base = {
        "return_1y": 0.10, "sharpe_1y": 0.0, "sharpe_cf": 2.0,
        "max_drawdown_1y": -0.25, "information_ratio_1y": 0.5,
    }
    r = ms.compute_equity_manager_score(base)
    # sharpe_cf=2.0 normalized on [-1, 3] -> 75.0
    assert r.components["risk_adjusted_return"] == pytest.approx(75.0)


def test_composite_falls_back_to_sharpe_1y_when_cf_missing():
    base = {
        "return_1y": 0.10, "sharpe_1y": 2.0, "sharpe_cf": None,
        "max_drawdown_1y": -0.25, "information_ratio_1y": 0.5,
    }
    r = ms.compute_equity_manager_score(base)
    # sharpe_1y=2.0 normalized on [-1, 3] -> 75.0
    assert r.components["risk_adjusted_return"] == pytest.approx(75.0)
    assert r.degraded is False  # sharpe_1y present -> not synthesized


def test_composite_both_sharpes_missing_synthesizes_risk_adjusted():
    base = {
        "return_1y": 0.10, "sharpe_1y": None, "sharpe_cf": None,
        "max_drawdown_1y": -0.25, "information_ratio_1y": 0.5,
    }
    r = ms.compute_equity_manager_score(base)
    assert r.degraded is True
    assert "risk_adjusted_return" in r.degraded_components
    # No peer_median given -> 45.0 neutral-below-midpoint fallback.
    assert r.components["risk_adjusted_return"] == pytest.approx(45.0)


def test_composite_missing_input_flags_degraded_with_opacity_penalty():
    metrics = {
        "return_1y": None, "sharpe_1y": 1.0, "sharpe_cf": 1.0,
        "max_drawdown_1y": -0.25, "information_ratio_1y": 0.5,
    }
    peer_medians = {"return_consistency": 70.0}
    r = ms.compute_equity_manager_score(metrics, peer_medians=peer_medians)
    assert r.degraded is True
    assert "return_consistency" in r.degraded_components
    # peer_median 70 - 5 opacity penalty
    assert r.components["return_consistency"] == pytest.approx(65.0)


def test_composite_score_is_rounded_to_two_decimals():
    metrics = {
        "return_1y": 0.123456, "sharpe_1y": 0.777, "sharpe_cf": 0.777,
        "max_drawdown_1y": -0.111, "information_ratio_1y": 0.333,
    }
    r = ms.compute_equity_manager_score(metrics)
    assert r.score == round(r.score, 2)
