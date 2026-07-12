"""Fast unit tests for the Tranche W5 recalibration experiment sweep logic.

Covers the pure helpers (min-confidence reclassification + markdown rendering) WITHOUT
the ~90s per-window_years pack replay, which is deliberately NOT run in CI (the
experiment is an offline, on-demand sensitivity probe). Default-cell fidelity
(window_years=10, min_confidence=0.70 reproduces the certified 23/66) is asserted by the
script itself at run time.
"""

from __future__ import annotations

import datetime as dt

from scripts.regime_recalibration_experiment import (
    DEFAULT_MIN_CONFIDENCE,
    DEFAULT_WINDOW_YEARS,
    reclassify_at_min_confidence,
    to_markdown,
)


class _Snap:
    """A DecisionRow-shaped stand-in with the fields reclassify reads."""

    def __init__(self, as_of, candidate_confidence, coverage_quality,
                 transition_pending, candidate_quadrant):
        self.as_of = as_of
        self.candidate_confidence = candidate_confidence
        self.coverage_quality = coverage_quality
        self.transition_pending = transition_pending
        self.candidate_quadrant = candidate_quadrant


def test_reclassify_threshold_moves_validity():
    series = [_Snap(dt.date(2024, 1, 31), 0.68, 1.0, False, "expansion")]
    # 0.68 clears a 0.65 floor -> valid; fails a 0.70 floor -> low_confidence.
    at65 = reclassify_at_min_confidence(series, 0.65)
    at70 = reclassify_at_min_confidence(series, 0.70)
    assert at65[0].has_valid_quadrant() is True
    assert at65[0].quadrant == "expansion"
    assert at70[0].has_valid_quadrant() is False
    assert at70[0].quadrant is None


def test_reclassify_respects_coverage_and_transition_gates():
    series = [
        _Snap(dt.date(2024, 1, 31), 0.99, 0.79, False, "expansion"),   # coverage < 0.80
        _Snap(dt.date(2024, 2, 29), 0.99, 1.00, True, "expansion"),    # transition pending
        _Snap(dt.date(2024, 3, 31), None, 1.00, False, "expansion"),   # no confidence
    ]
    out = reclassify_at_min_confidence(series, 0.60)
    assert all(not r.has_valid_quadrant() for r in out)


def test_markdown_renders_a_row_per_cell():
    result = {
        "chain_start": "2014-03-01",
        "metrics_window": {"start": "2021-01-01", "end": "2026-06-30"},
        "default_cell_fidelity": {"n_valid": 23},
        "cells": [{
            "baseline_window_years": 10, "min_confidence": 0.70,
            "timeline_metrics": {
                "n_valid": 23,
                "fresh_valid_rate": {"global": 0.348, "rolling_36m": 0.194},
                "max_abstention_streak_months": 18, "max_carry_age_months": 18,
                "max_same_quadrant_run_months": 38,
                "quadrant_mix": {"recovery": 4, "expansion": 12,
                                 "slowdown": 1, "contraction": 6}},
        }],
    }
    md = to_markdown(result)
    assert "| 10 | 0.70 | 23 |" in md
    assert "cont:6" in md


def test_default_grid_is_the_planned_sweep():
    assert DEFAULT_WINDOW_YEARS == (5, 7, 10)
    assert DEFAULT_MIN_CONFIDENCE == (0.60, 0.65, 0.70)
