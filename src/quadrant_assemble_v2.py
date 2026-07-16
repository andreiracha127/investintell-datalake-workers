# src/quadrant_assemble_v2.py
"""v2 assembler — per-axis monthly score OBSERVATIONS -> QuadrantSnapshot under the
confidence_v2.0 policy (Kalman estimation uncertainty + joint quadrant posterior).

The snapshot SHAPE, staleness plumbing, deterministic uuid5 id, and DB persistence
are the frozen v1 contract UNCHANGED (src.quadrant_snapshot / src.quadrant_assemble
upsert path); only the confidence/hysteresis semantics differ:

* per-axis: candidate_confidence = max(p, 1-p) where p = Phi(m/sqrt(P)) off the
  filtered state; sign = internal_sign = sign(m) (no separate latched memory —
  the filter IS the memory); uncertainty_raw = sqrt(R) (measurement noise),
  uncertainty_adjusted = sqrt(P) (posterior std).
* snapshot: candidate_confidence = MAX JOINT QUADRANT POSTERIOR (the publish
  statistic, not min-across-axes); quadrant = the sticky published quadrant when
  status == valid; candidate_quadrant = instantaneous argmax posterior;
  transition_pending is True only while an axis filter is unavailable
  (no_score / insufficient_vintages) — there is no deadband state.

v1 (`build_snapshot`) stays frozen and byte-reproducible for
model_version macro_quadrant_us_v1; this module is ADDITIVE for the v2 stream.
"""
from __future__ import annotations

import datetime as _dt

from src import quadrant_confidence_v2 as _c2
from src.quadrant_assemble import quadrant_from_signs
from src.quadrant_snapshot import (
    AxisDiagnostics,
    QuadrantSnapshot,
    make_snapshot_id,
)
from src.quadrant_staleness import available_at_snapshot, compute_stale_after

# Latched-chain reads for the v2 stream: predecessor id (any status) and the
# previously PUBLISHED quadrant (the sticky incumbent), both STRICTLY BEFORE the
# target as_of (same PIT discipline as v1's load_previous_snapshot).
_PREV_ID_SQL = (
    "SELECT snapshot_id FROM regime_quadrant_snapshot "
    "WHERE model_version = %s AND as_of < %s "
    "ORDER BY as_of DESC, available_at DESC LIMIT 1"
)
_PREV_PUBLISHED_SQL = (
    "SELECT quadrant FROM regime_quadrant_snapshot "
    "WHERE model_version = %s AND as_of < %s AND quadrant IS NOT NULL "
    "ORDER BY as_of DESC, available_at DESC LIMIT 1"
)


def load_previous_state_v2(conn, model_version: str, as_of: _dt.date) -> dict:
    """{previous_snapshot_id, prev_published_quadrant} for the v2 latched chain."""
    with conn.cursor() as cur:
        cur.execute(_PREV_ID_SQL, (model_version, as_of))
        row = cur.fetchone()
        prev_id = str(row[0]) if row else None
        cur.execute(_PREV_PUBLISHED_SQL, (model_version, as_of))
        row = cur.fetchone()
        prev_published = row[0] if row else None
    return {"previous_snapshot_id": prev_id,
            "prev_published_quadrant": prev_published}


def classify_axis_v2(
    *,
    observations: list[tuple[float | None, float | None]],
    coverage: float,
    freshness: float,
    source_health: float,
    auxiliary_observations: list[tuple[float | None, float | None]] | None = None,
) -> tuple[AxisDiagnostics, bool, str | None, float | None]:
    """Filter one axis' chronological (score, q_data) observations (current month
    LAST) and derive the v2 diagnostics.

    ``auxiliary_observations`` (optional, aligned to ``observations``) is a second
    SENSOR of the same latent state — the market-implied standardized score. When
    provided the axis runs the dual-sensor fused filter
    (:func:`quadrant_confidence_v2.kalman_fused_filter_series`); the market can
    sharpen the state but never substitutes for macro (a missing macro month stays
    unpublishable). Omitted/None -> the single-sensor v2 path, byte-identical.

    Returns (diagnostics, filter_available, reason, p_sign_positive).
    ``filter_available`` is False when the current score is missing or fewer than
    MIN_FILTER_OBSERVATIONS scores exist in the window (axis not consumable).
    """
    q_data = min(coverage, freshness, source_health)
    current_score = observations[-1][0] if observations else None
    n_obs = sum(1 for s, _ in observations if s is not None)
    if current_score is None:
        return (AxisDiagnostics(None, None, None, None, None, None, None),
                False, "no_score", None)
    if n_obs < _c2.MIN_FILTER_OBSERVATIONS:
        return (AxisDiagnostics(current_score, None, None, None, None, None, None),
                False, "insufficient_vintages", None)
    if auxiliary_observations is not None:
        filtered = _c2.kalman_fused_filter_series(observations,
                                                  auxiliary_observations)
    else:
        filtered = _c2.kalman_filter_series(observations)
    m, P, R = filtered[-1]
    if m is None or P is None or P <= 0.0:
        return (AxisDiagnostics(current_score, None, None, None, None, None, None),
                False, "filter_unavailable", None)
    p_pos = _c2.axis_sign_probability(m, P)
    sign = 1 if m > 0 else -1
    axis_conf = max(p_pos, 1.0 - p_pos)
    diag = AxisDiagnostics(
        score=current_score, sign=sign, internal_sign=sign,
        candidate_confidence=axis_conf,
        margin=m,                                  # filtered level (signed persistence margin)
        uncertainty_raw=(R ** 0.5) if R is not None else None,
        uncertainty_adjusted=P ** 0.5,
    )
    return diag, True, None, p_pos


def build_snapshot_v2(
    *,
    as_of: _dt.date,
    computed_at: _dt.datetime,
    previous_snapshot_id: str | None,
    prev_published_quadrant: str | None,
    growth_observations: list[tuple[float | None, float | None]],
    growth_coverage: float,
    growth_freshness: float,
    growth_health: float,
    inflation_observations: list[tuple[float | None, float | None]],
    inflation_coverage: float,
    inflation_freshness: float,
    inflation_health: float,
    input_available_ats: list[_dt.datetime],
    critical_expiries: list[_dt.datetime],
    model_version: str,
    source_vintage_hash: str,
    critical_structural_failure: bool = False,
    growth_auxiliary_observations: list[tuple[float | None, float | None]] | None = None,
    inflation_auxiliary_observations: list[tuple[float | None, float | None]] | None = None,
    confidence_method: str = _c2.CONFIDENCE_METHOD_V2,
) -> QuadrantSnapshot:
    """Assemble the v2 QuadrantSnapshot. Observations are chronological monthly
    (score, q_data) pairs with the CURRENT month last (the worker's trailing
    V2_FILTER_HISTORY_MONTHS recompute window). Optional per-axis auxiliary
    (market-implied) observation streams engage the dual-sensor fused filter;
    omitted -> single-sensor v2, byte-identical."""
    g_diag, g_ok, g_reason, g_p = classify_axis_v2(
        observations=growth_observations, coverage=growth_coverage,
        freshness=growth_freshness, source_health=growth_health,
        auxiliary_observations=growth_auxiliary_observations)
    i_diag, i_ok, i_reason, i_p = classify_axis_v2(
        observations=inflation_observations, coverage=inflation_coverage,
        freshness=inflation_freshness, source_health=inflation_health,
        auxiliary_observations=inflation_auxiliary_observations)

    filter_available = g_ok and i_ok
    transition_pending = not filter_available
    reason_bits = [r for r in (g_reason, i_reason) if r is not None]
    transition_reason = ",".join(reason_bits) if reason_bits else None

    posterior: dict[str, float] | None = None
    max_posterior: float | None = None
    candidate_quadrant = None
    published_quadrant = None
    if filter_available:
        posterior = _c2.quadrant_posterior(g_p, i_p)
        publish, published_quadrant, candidate_quadrant, max_posterior = (
            _c2.publish_decision(posterior, prev_published_quadrant))
    else:
        # audit guess from instantaneous score signs, mirroring v1's candidate path
        g_cand = _sign_or_none(g_diag.score)
        i_cand = _sign_or_none(i_diag.score)
        candidate_quadrant = quadrant_from_signs(g_cand, i_cand)

    coverage_quality = min(growth_coverage, inflation_coverage)
    freshness_quality = min(growth_freshness, inflation_freshness)
    source_health_quality = min(growth_health, inflation_health)
    critical_source_expired = freshness_quality <= 0.0

    available_at = available_at_snapshot(computed_at, input_available_ats)
    data_stale_after, pipeline_stale_after, stale_after = compute_stale_after(
        computed_at, critical_expiries)

    status = _c2.resolve_status_v2(
        critical_structural_failure=critical_structural_failure,
        coverage=coverage_quality,
        critical_source_expired=critical_source_expired,
        source_health=source_health_quality,
        filter_available=filter_available,
        max_quadrant_posterior=max_posterior,
    )
    if status == "stale":  # persisted column never stores 'stale' (v1 contract)
        status = "low_confidence"

    candidate_confidence: float | None = max_posterior
    if status == "valid":
        quadrant = published_quadrant
        if quadrant is None:
            status = "low_confidence"
    else:
        quadrant = None
    if status in ("unavailable", "invalid"):
        candidate_confidence = None

    snapshot_id = make_snapshot_id(
        model_version, as_of, source_vintage_hash, previous_snapshot_id)
    return QuadrantSnapshot(
        snapshot_id=snapshot_id,
        previous_snapshot_id=previous_snapshot_id,
        quadrant=quadrant,
        candidate_quadrant=candidate_quadrant,
        candidate_confidence=candidate_confidence,
        growth=g_diag, inflation=i_diag,
        coverage_quality=coverage_quality,
        freshness_quality=freshness_quality,
        source_health_quality=source_health_quality,
        transition_pending=transition_pending,
        transition_reason=transition_reason,
        as_of=as_of, available_at=available_at, computed_at=computed_at,
        data_stale_after=data_stale_after,
        pipeline_stale_after=pipeline_stale_after,
        stale_after=stale_after,
        status_at_compute=status,
        model_version=model_version,
        confidence_model_version=_c2.CONFIDENCE_MODEL_VERSION_V2,
        confidence_method=confidence_method,
        source_vintage_hash=source_vintage_hash,
    )


def _sign_or_none(score: float | None) -> int | None:
    if score is None:
        return None
    return 1 if score > 0 else -1
