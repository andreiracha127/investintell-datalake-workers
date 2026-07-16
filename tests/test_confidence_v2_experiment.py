"""Fast unit tests for the confidence-v2 experiment's pure helpers.

Covers the decision-rule machinery (quadrant posterior, publish/hysteresis,
min-rule), the Kalman local-level filter, and the gate/stability reports WITHOUT
the ~90s pack replay, which is deliberately NOT run in CI (the experiment is an
offline, on-demand recalibration probe). Frozen-baseline fidelity (22 valid / 66,
fresh_36m 6/36, streaks 18/18/38 on pack _003) is asserted by the script itself
at run time and aborts the sweep on any mismatch.
"""

from __future__ import annotations

import datetime as dt
import math

from src.quadrant_assemble import quadrant_from_signs

from scripts.regime_confidence_v2_experiment import (
    DEFAULT_LAMBDA_GRID,
    DEFAULT_TAU_GRID,
    TIMELINE_GATES,
    TimelineRow,
    gate_report,
    kalman_filter_series,
    min_rule_series,
    posterior_series,
    quadrant_posterior,
    to_markdown,
    window_stability,
)


def _row(as_of, g_score, i_score, g_u=0.5, i_u=0.5, coverage=1.0):
    return {"as_of": as_of, "coverage_quality": coverage,
            "g_score": g_score, "i_score": i_score,
            "g_u_adj": g_u, "i_u_adj": i_u}


def _probs_from_frozen_u(row):
    from statistics import NormalDist
    n = NormalDist()
    if row["g_score"] is None or row["i_score"] is None:
        return False, 0.5, 0.5
    return True, n.cdf(row["g_score"] / row["g_u_adj"]), \
        n.cdf(row["i_score"] / row["i_u_adj"])


def test_quadrant_posterior_sums_to_one_and_maps_signs():
    probs = quadrant_posterior(0.9, 0.8)
    assert abs(sum(probs.values()) - 1.0) < 1e-12
    # the argmax quadrant must be the (+,+) quadrant under high p_g, p_i
    assert max(probs, key=probs.get) == quadrant_from_signs(1, 1)
    assert abs(probs[quadrant_from_signs(1, 1)] - 0.72) < 1e-12
    assert abs(probs[quadrant_from_signs(-1, -1)] - 0.02) < 1e-12


def test_posterior_series_publishes_above_tau_and_abstains_below():
    strong = _row(dt.date(2024, 1, 31), 2.0, 2.0)     # p ~ 1.0 each axis
    weak = _row(dt.date(2024, 2, 29), 0.01, 0.01)     # posterior ~ 0.25
    out = posterior_series([strong, weak], _probs_from_frozen_u, tau=0.60,
                           delta=0.10)
    assert out[0].has_valid_quadrant()
    assert out[0].quadrant == quadrant_from_signs(1, 1)
    assert not out[1].has_valid_quadrant()


def test_posterior_series_respects_hard_gates():
    low_cov = _row(dt.date(2024, 1, 31), 2.0, 2.0, coverage=0.79)
    no_score = _row(dt.date(2024, 2, 29), None, 2.0)
    out = posterior_series([low_cov, no_score], _probs_from_frozen_u, tau=0.40,
                           delta=0.10)
    assert all(not r.has_valid_quadrant() for r in out)


def test_posterior_series_sticky_hysteresis_keeps_incumbent_within_delta():
    a = _row(dt.date(2024, 1, 31), 1.0, 1.0)          # publishes (+,+)
    # challenger (+,-) barely edges the incumbent: p_i just under 0.5
    b = _row(dt.date(2024, 2, 29), 1.0, -0.02)
    out = posterior_series([a, b], _probs_from_frozen_u, tau=0.40, delta=0.10)
    assert out[0].quadrant == quadrant_from_signs(1, 1)
    assert out[1].quadrant == quadrant_from_signs(1, 1)  # incumbent kept
    # with no stickiness the challenger flips immediately
    out0 = posterior_series([a, b], _probs_from_frozen_u, tau=0.40, delta=0.0)
    assert out0[1].quadrant == quadrant_from_signs(1, -1)


def test_min_rule_series_applies_min_axis_confidence():
    # growth strong, inflation ambiguous -> min confidence < 0.70 -> abstain
    amb = _row(dt.date(2024, 1, 31), 2.0, 0.05)
    strong = _row(dt.date(2024, 2, 29), 2.0, -2.0)
    out = min_rule_series([amb, strong], _probs_from_frozen_u, 0.70)
    assert not out[0].has_valid_quadrant()
    assert out[1].has_valid_quadrant()
    assert out[1].quadrant == quadrant_from_signs(1, -1)


def test_kalman_posterior_variance_shrinks_below_measurement_noise():
    scores = [1.0, 1.1, 0.9, 1.0, 1.05, 0.95, 1.0, 1.1, 0.9, 1.0, 1.05, 0.95,
              1.0, 1.1, 0.9, 1.0]
    covs = [1.0] * len(scores)
    out = kalman_filter_series(scores, covs, lam=0.25)
    m_last, p_last = out[-1]
    assert m_last is not None and p_last is not None
    assert 0.85 < m_last < 1.15            # tracks the level
    # steady-state posterior variance is well below the warmup R
    r_warm = 0.35 ** 2 / 2.25
    assert p_last < r_warm
    # missing observation -> predict-only (variance grows, mean held)
    out2 = kalman_filter_series(scores + [None], covs + [None], lam=0.25)
    m_none, p_none = out2[-1]
    assert m_none == m_last
    assert p_none > p_last


def test_kalman_adapts_to_level_shift():
    scores = [0.5] * 14 + [-1.5] * 10
    out = kalman_filter_series(scores, [1.0] * len(scores), lam=0.25)
    m_last, _ = out[-1]
    assert m_last is not None and m_last < -0.5   # converged to the new level


def test_gate_report_judges_the_ratified_bounds():
    tl = {"fresh_valid_rate": {"rolling_36m": 6 / 36},
          "max_abstention_streak_months": 18, "max_carry_age_months": 18,
          "max_same_quadrant_run_months": 38}
    g = gate_report(tl)
    assert not g["all_pass"]
    assert all(not g[k]["pass"] for k in ("fresh_valid_36m", "abstention_streak",
                                          "carry_age", "same_quadrant_run"))
    tl_ok = {"fresh_valid_rate": {"rolling_36m": 0.81},
             "max_abstention_streak_months": 3, "max_carry_age_months": 3,
             "max_same_quadrant_run_months": 12}
    assert gate_report(tl_ok)["all_pass"]


def test_window_stability_counts_flips_and_one_month_reversals():
    rows = [TimelineRow(dt.date(2024, m, 28), "valid", q)
            for m, q in enumerate(("expansion", "recovery", "expansion",
                                   "expansion", "contraction"), start=1)]
    rows.insert(2, TimelineRow(dt.date(2024, 2, 29), "low_confidence", None))
    stab = window_stability(rows)
    assert stab["flips"] == 3           # E->R, R->E, E->C over published months
    assert stab["one_month_reversals"] == 1   # E -> R -> E


def test_markdown_renders_a_row_per_cell_with_gate_marks():
    result = {
        "pack": "open_macro_v03_certified_input_pack_003",
        "chain_start": "2014-03-01",
        "metrics_window": {"start": "2021-01-01", "end": "2026-06-30"},
        "frozen_fidelity_anchor": {"n_valid": 22},
        "cells": [{
            "label": "frozen_v1_baseline",
            "timeline_metrics": {
                "n_valid": 22,
                "fresh_valid_rate": {"global": 0.333, "rolling_36m": 6 / 36},
                "max_abstention_streak_months": 18, "max_carry_age_months": 18,
                "max_same_quadrant_run_months": 38},
            "stability": {"flips_per_year": 1.27, "one_month_reversals": 2},
            "gates": gate_report({
                "fresh_valid_rate": {"rolling_36m": 6 / 36},
                "max_abstention_streak_months": 18, "max_carry_age_months": 18,
                "max_same_quadrant_run_months": 38}),
        }],
    }
    md = to_markdown(result)
    assert "| frozen_v1_baseline | 22 | 0.1667 |" in md
    assert "FFFF" in md


def test_default_grids_are_the_planned_sweep():
    assert DEFAULT_TAU_GRID == (0.40, 0.45, 0.50, 0.55, 0.60, 0.70)
    assert DEFAULT_LAMBDA_GRID == (0.10, 0.25, 0.50)
    assert TIMELINE_GATES["min_fresh_valid_rate_36m"] == 0.40
    assert math.isclose(TIMELINE_GATES["max_carry_age_months"], 3)
