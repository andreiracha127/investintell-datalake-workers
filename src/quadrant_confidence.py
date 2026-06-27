# src/quadrant_confidence.py
"""Confidence (freeze v1 §4/§6, owner decision B) — an OPERATIONAL abstention proxy,
not a calibrated probability. Per axis: u_raw = max(1.4826·MAD(score over DISTINCT
vintages), u_floor) [NOT /sqrt(n), NOT forward-filled]; u_adj = u_raw / max(q_data,
0.25); confidence = Φ(|score| / u_adj), so 0.50 <= confidence <= 1.
candidate_confidence = min over axes.

The three qualities (owner decision B — FULL formulas, not half-implementations):
  coverage:  historyCoverage_i = min(1, nValid_i/minimum_valid_observations_i);
             usable_i = I(currentValueValid_i)·historyCoverage_i;
             coverage_a = Σ|w_i|·usable_i / Σ|w_i|; coverageQuality = min over axes.
  freshness: soft_deadline = next_expected_release + grace_period;
             hard_deadline = min(last_available_at + hard_max_age,
                                 soft_deadline + freshness_decay_window);
             freshness_i = 1 (now<=soft), 0 (now>=hard), linear between; per axis a
             |w|-weighted mean; snapshot = min over axes. A critical source past its
             hard_deadline is a HARD gate (-> stale); the linear decay before that is
             the SOFT penalty folded into q_data.
  source_health: per-source checks (schema, expected unit, finite value, one obs per
             period/vintage, valid release_at/available_at, valid revision lineage,
             transform compatible with unit, structurally-possible range — NEVER
             invalid merely for an extreme economic move); health_i =
             passed_weight/total_check_weight; sourceHealth_a = Σ|w_i|·health_i/Σ|w_i|;
             snapshot = min over axes.

q_data = min(coverageQuality, freshnessQuality, sourceHealthQuality);
u_adj_a = max(u_raw_a, u_floor_a) / max(q_data, 0.25).

STATUS ORDER (owner decision B — verbatim; SEPARATE from confidence):
  critical structural failure  -> invalid
  coverage < 0.80              -> unavailable
  critical source expired      -> stale
  health < 0.90                -> low_confidence
  confidence < 0.70            -> low_confidence
  transition_pending           -> low_confidence
  otherwise                    -> valid

The FORMULAS are frozen now; the THRESHOLDS/FLOORS (U_FLOOR_SEED, MIN_*, window)
belong to the parameter freeze. u_floor seed 0.25 per axis (owner decision B);
future calibration u_floor_a = max(0.25, P10(u_raw_a in training)), frozen in
confidence_model_version, never recomputed with future data. Calibrate the
thresholds/floors (A3) ONLY against abstention/flips/vintage-stability, NEVER
against CAGR/Sharpe.
"""
from typing import Literal

import statistics

ComputeStatus = Literal["valid", "low_confidence", "unavailable", "invalid"]

MIN_CANDIDATE_CONFIDENCE = 0.70
MIN_INPUT_COVERAGE = 0.80
MIN_SOURCE_HEALTH = 0.90
UNCERTAINTY_WINDOW_VINTAGES = 36
MIN_UNCERTAINTY_VINTAGES = 24
Q_DATA_FLOOR = 0.25
U_FLOOR_SEED = {"growth": 0.25, "inflation": 0.25}

_NORM = statistics.NormalDist()
_MAD_SCALE = 1.4826


def uncertainty_raw(score_history: list[float], u_floor: float) -> float | None:
    """1.4826·MAD over the DISTINCT score values in the window, floored at u_floor.

    Returns None when fewer than MIN_UNCERTAINTY_VINTAGES (24) *distinct* values are
    available (§6 -> caller treats as unavailable, confidence NULL). MAD, not stdev,
    for robustness; over distinct vintages, not forward-filled rows.
    """
    distinct = sorted(set(score_history))
    if len(distinct) < MIN_UNCERTAINTY_VINTAGES:
        return None
    median = statistics.median(distinct)
    mad = statistics.median([abs(v - median) for v in distinct])
    return max(_MAD_SCALE * mad, u_floor)


def axis_confidence(score: float, u_raw: float, q_data: float) -> tuple[float, float]:
    """(confidence, u_adj) for one axis. confidence = Φ(|score| / u_adj).

    ``u_raw`` is already floored by uncertainty_raw; u_adj divides by the clamped
    q_data so the worst data quality inflates uncertainty at most 4x.
    """
    u_adj = u_raw / max(q_data, Q_DATA_FLOOR)
    if u_adj <= 0.0:
        return 1.0, u_adj
    confidence = _NORM.cdf(abs(score) / u_adj)
    return confidence, u_adj


def coverage_quality(items: list[tuple[float, bool, float]]) -> float:
    """Σ|w_i|·usable_i / Σ|w_i| over (abs_weight, current_value_valid, history_cov).

    usable_i = I(current_value_valid_i)·history_coverage_i. Empty -> 0.0.
    """
    total = sum(abs(w) for w, _, _ in items)
    if total <= 0.0:
        return 0.0
    num = sum(abs(w) * (cov if valid else 0.0) for w, valid, cov in items)
    return num / total


def freshness_value(now, soft_deadline, hard_deadline) -> float:
    """1 if now<=soft, 0 if now>=hard, linear (hard-now)/(hard-soft) between."""
    if now <= soft_deadline:
        return 1.0
    if now >= hard_deadline:
        return 0.0
    span = (hard_deadline - soft_deadline).total_seconds()
    if span <= 0:
        return 0.0
    return (hard_deadline - now).total_seconds() / span


def axis_freshness(items: list[tuple[float, float]]) -> float:
    """|w|-weighted mean of per-source freshness_value over (abs_weight, fresh_i)."""
    total = sum(abs(w) for w, _ in items)
    if total <= 0.0:
        return 0.0
    return sum(abs(w) * f for w, f in items) / total


def source_health(items: list[tuple[float, float]]) -> float:
    """Σ|w_i|·health_i / Σ|w_i| over (abs_weight, health_i)."""
    total = sum(abs(w) for w, _ in items)
    if total <= 0.0:
        return 0.0
    return sum(abs(w) * h for w, h in items) / total


def resolve_status(
    *,
    critical_structural_failure: bool,
    coverage: float,
    critical_source_expired: bool,
    source_health: float,
    candidate_confidence: float,
    transition_pending: bool,
) -> ComputeStatus:
    """Owner decision B — apply the status order verbatim and return compute status.

    ``critical_source_expired`` is the HARD freshness gate (a critical source past
    its hard_deadline). The persisted column never stores 'stale'; the worker
    (Task 7) maps this compute-time 'stale' to 'low_confidence' before INSERT, and
    the read-side effective_status is the authoritative stale path.
    """
    if critical_structural_failure:
        return "invalid"
    if coverage < MIN_INPUT_COVERAGE:
        return "unavailable"
    if critical_source_expired:
        return "stale"  # type: ignore[return-value]  # compute-time; reader derives the read-side stale
    if source_health < MIN_SOURCE_HEALTH:
        return "low_confidence"
    if candidate_confidence < MIN_CANDIDATE_CONFIDENCE:
        return "low_confidence"
    if transition_pending:
        return "low_confidence"
    return "valid"
