# tests/test_quadrant_confidence.py
from __future__ import annotations

import datetime as dt

from src.quadrant_confidence import (
    MIN_UNCERTAINTY_VINTAGES,
    U_FLOOR_SEED,
    axis_confidence,
    axis_freshness,
    coverage_quality,
    freshness_value,
    resolve_status,
    source_health,
    uncertainty_raw,
)

UTC = dt.timezone.utc


def test_owner_constants_frozen() -> None:
    assert MIN_UNCERTAINTY_VINTAGES == 24
    assert U_FLOOR_SEED == {"growth": 0.25, "inflation": 0.25}


def test_uncertainty_raw_none_when_too_few_vintages() -> None:
    distinct = [0.001 * i for i in range(MIN_UNCERTAINTY_VINTAGES - 1)]
    assert uncertainty_raw(distinct, 0.25) is None


def test_uncertainty_raw_uses_mad_over_distinct_values() -> None:
    hist = [0.001 * i for i in range(30)]  # 30 distinct values
    u = uncertainty_raw(hist, 0.001)
    assert u is not None and u > 0.0


def test_uncertainty_raw_respects_floor() -> None:
    hist = [0.10] * 30  # one distinct value -> MAD 0 but ALSO < 24 distinct -> None
    assert uncertainty_raw(hist, 0.25) is None
    flat = [0.10 + 1e-9 * i for i in range(30)]  # ~flat but 30 distinct -> floored
    assert uncertainty_raw(flat, 0.25) == 0.25


def test_axis_confidence_strong_score_high_confidence() -> None:
    conf, u_adj = axis_confidence(0.90, 0.25, 1.0)
    assert conf > 0.99 and abs(u_adj - 0.25) < 1e-12


def test_axis_confidence_zero_score_is_half() -> None:
    conf, _ = axis_confidence(0.0, 0.25, 1.0)
    assert abs(conf - 0.50) < 1e-9


def test_axis_confidence_low_quality_inflates_uncertainty() -> None:
    # q_data below the 0.25 floor is clamped to 0.25 -> u_adj = u_raw / 0.25.
    _, u_adj = axis_confidence(0.30, 0.25, 0.10)
    assert abs(u_adj - 0.25 / 0.25) < 1e-12


def test_coverage_quality_importance_weighted() -> None:
    # (abs_weight, current_value_valid, history_coverage)
    items = [(0.5, True, 1.0), (0.5, False, 1.0)]  # second invalid -> usable 0
    assert abs(coverage_quality(items) - 0.5) < 1e-12
    # a partially-covered valid source counts its history_coverage.
    items2 = [(0.5, True, 1.0), (0.5, True, 0.6)]
    assert abs(coverage_quality(items2) - 0.8) < 1e-12


def test_freshness_value_piecewise() -> None:
    soft = dt.datetime(2024, 3, 10, tzinfo=UTC)
    hard = dt.datetime(2024, 3, 20, tzinfo=UTC)
    assert freshness_value(dt.datetime(2024, 3, 5, tzinfo=UTC), soft, hard) == 1.0
    assert freshness_value(dt.datetime(2024, 3, 25, tzinfo=UTC), soft, hard) == 0.0
    mid = freshness_value(dt.datetime(2024, 3, 15, tzinfo=UTC), soft, hard)
    assert abs(mid - 0.5) < 1e-9


def test_axis_freshness_weighted_mean() -> None:
    # (abs_weight, freshness_value)
    assert abs(axis_freshness([(0.5, 1.0), (0.5, 0.0)]) - 0.5) < 1e-12


def test_source_health_weighted_mean() -> None:
    # (abs_weight, health_i)
    assert abs(source_health([(0.5, 1.0), (0.5, 0.8)]) - 0.9) < 1e-12


def _kw(**over):
    base = dict(critical_structural_failure=False, coverage=1.0,
               critical_source_expired=False, source_health=1.0,
               candidate_confidence=0.85, transition_pending=False)
    base.update(over)
    return base


def test_resolve_status_invalid_first() -> None:
    # critical structural failure dominates even with everything else fine.
    assert resolve_status(**_kw(critical_structural_failure=True)) == "invalid"


def test_resolve_status_unavailable_when_coverage_below_min() -> None:
    assert resolve_status(**_kw(coverage=0.70)) == "unavailable"


def test_resolve_status_stale_when_critical_source_expired() -> None:
    assert resolve_status(**_kw(critical_source_expired=True)) == "stale"


def test_resolve_status_low_confidence_when_health_below_090() -> None:
    assert resolve_status(**_kw(source_health=0.85)) == "low_confidence"


def test_resolve_status_low_confidence_below_confidence_threshold() -> None:
    assert resolve_status(**_kw(candidate_confidence=0.65)) == "low_confidence"


def test_resolve_status_low_confidence_on_transition() -> None:
    assert resolve_status(**_kw(transition_pending=True)) == "low_confidence"


def test_resolve_status_order_invalid_beats_unavailable() -> None:
    # both a structural failure AND low coverage -> invalid wins (first in order).
    assert resolve_status(**_kw(critical_structural_failure=True,
                                coverage=0.10)) == "invalid"


def test_resolve_status_order_coverage_beats_health() -> None:
    # both coverage<0.80 AND health<0.90 -> coverage wins (precedes health in order).
    assert resolve_status(**_kw(coverage=0.10, source_health=0.10)) == "unavailable"


def test_resolve_status_order_stale_beats_low_confidence() -> None:
    # critical source expired AND health<0.90 -> stale wins (precedes health).
    assert resolve_status(**_kw(critical_source_expired=True,
                                source_health=0.10)) == "stale"


def test_resolve_status_valid_when_all_pass() -> None:
    assert resolve_status(**_kw()) == "valid"
