# src/quadrant_confidence_v2.py
"""Confidence v2 — Kalman local-level ESTIMATION uncertainty + joint quadrant
posterior publish rule (confidence_v2.0, ratified path: PR #43 experiment ->
owner-approved recalibration, 2026-07-16).

WHY v2 (replaces the v1 abstention proxy for the v2 model stream only; v1 stays
frozen and reproducible under model_version macro_quadrant_us_v1):

* v1's ``u_raw = 1.4826*MAD(score over its own trailing vintages)`` measures the
  SCORE'S VARIABILITY, not the uncertainty of the estimate — a self-referential
  outlier test that abstains when the score is near zero (mid-cycle neutral, a
  legitimate well-measured state) and when trailing dispersion spikes (drawdown
  onsets — exactly when the signal matters). Certified pack _003 evidence:
  22/66 fresh-valid months, 18-month abstention streaks, one 38-month carried
  quadrant.
* v2 filters each axis score with a causal local-level Kalman filter
  (x_t = x_{t-1} + w_t, y_t = x_t + v_t) and reads confidence off the FILTERED
  STATE's posterior: p_axis = Phi(m_t / sqrt(P_t)) — the probability the axis
  sign is positive given everything observed. Neutral-but-well-measured months
  publish; noise-spike months shrink toward the filtered level instead of
  abstaining.
* The publish rule is a JOINT quadrant posterior (product of axis sign
  probabilities; first-order independence): publish argmax iff
  max posterior >= TAU_PUBLISH. min()-across-axes vetoes and the two-step
  deadband hysteresis are gone; persistence comes from the filter itself plus a
  sticky incumbent rule (challenger must beat the published incumbent by
  DELTA_STICKY).

PARAMETERS (frozen for confidence_v2.0 — calibrated on the pack _003 offline
experiment against abstention / flips / one-month reversals / streak metrics
ONLY, never CAGR/Sharpe; see docs/calibration/
open_macro_v03_confidence_v2_experiment_001.md):

  KALMAN_LAMBDA = 0.10        # Q/R signal-to-noise; smoothest stable cell
  TAU_PUBLISH   = 0.60        # min joint quadrant posterior to publish
  DELTA_STICKY  = 0.10        # incumbent stickiness margin
  R from robust MAD of score first-differences over a trailing window
  (method of moments: Var(diff y) = R*(2+lambda)), floored, quality-inflated by
  1/max(q_data, Q_DATA_FLOOR)^2 — the same 4x worst-case inflation contract as
  v1's u_adj.

The filter is STATELESS per run: each decision recomputes it over the trailing
``V2_FILTER_HISTORY_MONTHS`` monthly score observations (the worker's normal
recompute window), so the worker stays idempotent and PIT-safe exactly like the
v1 MAD path. Steady state is reached in ~10 observations, so the trailing-window
recompute and a persistent filter agree to numerical noise at the decision point.

STATUS ORDER (v2 — same hard gates as v1, confidence step swapped):
  critical structural failure       -> invalid
  coverage < 0.80                   -> unavailable
  critical source expired           -> stale (degraded to low_confidence at write)
  health < 0.90                     -> low_confidence
  filter unavailable (< MIN obs)    -> low_confidence  (insufficient_vintages)
  max quadrant posterior < 0.60     -> low_confidence
  otherwise                         -> valid
"""
from __future__ import annotations

import math
import statistics
from typing import Sequence

from src.quadrant_confidence import (
    MIN_INPUT_COVERAGE,
    MIN_SOURCE_HEALTH,
    Q_DATA_FLOOR,
)

CONFIDENCE_MODEL_VERSION_V2 = "confidence_v2.0"
CONFIDENCE_METHOD_V2 = "kalman_joint_posterior_v2"

KALMAN_LAMBDA = 0.10
TAU_PUBLISH = 0.60
DELTA_STICKY = 0.10

V2_FILTER_HISTORY_MONTHS = 48    # trailing monthly observations fed to the filter
MIN_FILTER_OBSERVATIONS = 24     # v1's MIN_UNCERTAINTY_VINTAGES spirit: fewer -> axis unavailable
KALMAN_R_WINDOW = 36             # trailing first-differences for the R estimate
KALMAN_MIN_DIFFS = 12            # fewer diffs -> conservative warmup R
KALMAN_R_FLOOR = 0.10 ** 2
KALMAN_WARMUP_MAD = 0.35

_NORM = statistics.NormalDist()
_MAD_SCALE = 1.4826


def kalman_filter_series(
    observations: Sequence[tuple[float | None, float | None]],
    lam: float = KALMAN_LAMBDA,
    *,
    r_window: int = KALMAN_R_WINDOW,
    min_diffs: int = KALMAN_MIN_DIFFS,
) -> list[tuple[float | None, float | None, float | None]]:
    """Causal scalar local-level Kalman filter over monthly (score, q_data) pairs.

    Returns ``[(m_t, P_t, R_t)]`` aligned to the input; ``(None, None, None)``
    until the first score arrives. A missing score is a predict-only step (the
    posterior variance grows by the last Q and the mean holds — the month is
    unpublishable anyway because the hard gates require a fresh score).

    R_t = max((1.4826*MAD(diff(score), trailing r_window))^2 / (2+lam), floor),
    inflated by 1/max(q_data, Q_DATA_FLOOR)^2 — degraded data quality can inflate
    measurement noise at most (1/Q_DATA_FLOOR)^2 = 16x in variance (4x in sigma),
    the same worst-case contract as v1's u_adj. Q = lam * R_t.

    KNOWN ESTIMATOR LIMIT: MAD has a breakdown on an exactly 50/50 bimodal diff
    distribution (an odd-count +d/-d alternation medians onto one mode and MAD
    collapses to 0, leaving R at the floor). Real macro score first-differences
    are continuous — the certified-pack evidence never approaches this — and the
    R floor bounds the damage; noted here so a future calibration can consider an
    IQR-blend estimator if a pathological basket ever surfaces.
    """
    m: float | None = None
    P: float | None = None
    last_q = 0.0
    diffs: list[float] = []
    prev_score: float | None = None
    out: list[tuple[float | None, float | None, float | None]] = []
    for score, q_data in observations:
        if score is None:
            if m is not None and P is not None:
                P = P + last_q
            out.append((m, P, None))
            prev_score = None
            continue
        if prev_score is not None:
            diffs.append(score - prev_score)
        prev_score = score
        window = diffs[-r_window:]
        if len(window) >= min_diffs:
            med = statistics.median(window)
            mad = statistics.median([abs(d - med) for d in window])
            r_base = (_MAD_SCALE * mad) ** 2 / (2.0 + lam)
        else:
            r_base = KALMAN_WARMUP_MAD ** 2 / (2.0 + lam)
        r_base = max(r_base, KALMAN_R_FLOOR)
        quality = max(q_data if q_data is not None else 0.0, Q_DATA_FLOOR)
        R = r_base / (quality ** 2)
        Q = lam * R
        last_q = Q
        if m is None or P is None:
            m, P = score, R
        else:
            P_pred = P + Q
            gain = P_pred / (P_pred + R)
            m = m + gain * (score - m)
            P = (1.0 - gain) * P_pred
        out.append((m, P, R))
    return out


def axis_sign_probability(m: float, P: float) -> float:
    """P(axis state > 0 | observations) = Phi(m / sqrt(P))."""
    if P <= 0.0:
        return 1.0 if m > 0 else 0.0
    return _NORM.cdf(m / math.sqrt(P))


def quadrant_posterior(p_growth_pos: float, p_inflation_pos: float) -> dict[str, float]:
    """4-quadrant posterior from the axis sign probabilities (first-order
    independence; the sign->quadrant map is the frozen freeze-v1 map)."""
    # local import avoids a src.db dependency at module import time
    from src.quadrant_assemble import quadrant_from_signs
    probs: dict[str, float] = {}
    for g_sign in (1, -1):
        for i_sign in (1, -1):
            quadrant = quadrant_from_signs(g_sign, i_sign)
            pg = p_growth_pos if g_sign == 1 else 1.0 - p_growth_pos
            pi = p_inflation_pos if i_sign == 1 else 1.0 - p_inflation_pos
            probs[quadrant] = pg * pi
    return probs


def publish_decision(
    posterior: dict[str, float],
    prev_published_quadrant: str | None,
    *,
    tau: float = TAU_PUBLISH,
    delta: float = DELTA_STICKY,
) -> tuple[bool, str | None, str, float]:
    """(publish, published_quadrant, candidate_quadrant, max_posterior).

    Publish the argmax quadrant iff its posterior clears ``tau``; sticky
    incumbent rule: keep the previously PUBLISHED quadrant while its posterior is
    within ``delta`` of the challenger (the challenger must WIN by delta, not tie).
    The candidate (audit/UI) quadrant is always the instantaneous argmax.
    """
    candidate = max(posterior, key=posterior.get)  # type: ignore[arg-type]
    max_p = posterior[candidate]
    if max_p < tau:
        return False, None, candidate, max_p
    published = candidate
    if (prev_published_quadrant is not None
            and published != prev_published_quadrant
            and posterior.get(prev_published_quadrant, 0.0) >= max_p - delta):
        published = prev_published_quadrant
    return True, published, candidate, max_p


def resolve_status_v2(
    *,
    critical_structural_failure: bool,
    coverage: float,
    critical_source_expired: bool,
    source_health: float,
    filter_available: bool,
    max_quadrant_posterior: float | None,
) -> str:
    """v2 status order — v1's hard gates verbatim with the confidence step swapped
    for the joint-posterior threshold; no transition_pending step (persistence
    lives in the filter + sticky rule, not a two-step deadband)."""
    if critical_structural_failure:
        return "invalid"
    if coverage < MIN_INPUT_COVERAGE:
        return "unavailable"
    if critical_source_expired:
        return "stale"
    if source_health < MIN_SOURCE_HEALTH:
        return "low_confidence"
    if not filter_available:
        return "low_confidence"
    if max_quadrant_posterior is None or max_quadrant_posterior < TAU_PUBLISH:
        return "low_confidence"
    return "valid"
