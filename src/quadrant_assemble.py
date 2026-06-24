# src/quadrant_assemble.py
"""Shared assembler — turns per-axis (score, history, quality, prev_sign) into the
ONE QuadrantSnapshot both A2 workers emit, then into DB rows. The macro and market
workers call build_snapshot with different SOURCES; the snapshot shape, hysteresis,
confidence, hard gates, and persistence are identical here (freeze v1 §3-§10).
"""
from __future__ import annotations

import datetime as _dt
import os

from src.db import LOCK_REGIME_QUADRANT  # noqa: F401  (re-export convenience)
from src.quadrant_confidence import (
    MIN_CANDIDATE_CONFIDENCE,
    axis_confidence,
    resolve_status,
    uncertainty_raw,
)
from src.quadrant_hysteresis import axis_hysteresis
from src.quadrant_snapshot import (
    AxisDiagnostics,
    QuadrantSnapshot,
    make_snapshot_id,
)
from src.quadrant_staleness import available_at_snapshot, compute_stale_after

_SCHEMA = "schemas/regime_quadrant_snapshot.sql"

# Latched-chain read (owner decision C): the newest row per model_version supplies
# the predecessor id + the per-axis latched sign the hysteresis resumes from.
_PREV_SQL = (
    "SELECT snapshot_id, growth_internal_sign, inflation_internal_sign "
    "FROM regime_quadrant_snapshot WHERE model_version = %s "
    "ORDER BY as_of DESC, available_at DESC LIMIT 1"
)


def load_previous_snapshot(conn, model_version: str) -> dict | None:
    """Return {previous_snapshot_id, growth_internal_sign, inflation_internal_sign}
    for the latest snapshot of ``model_version``, or None at genesis."""
    with conn.cursor() as cur:
        cur.execute(_PREV_SQL, (model_version,))
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "previous_snapshot_id": str(row[0]),
        "growth_internal_sign": row[1],
        "inflation_internal_sign": row[2],
    }


_QUADRANT_BY_SIGNS = {
    (1, -1): "recovery",
    (1, 1): "expansion",
    (-1, 1): "slowdown",
    (-1, -1): "contraction",
}


def quadrant_from_signs(growth_sign, inflation_sign):
    """Map effective axis signs to a quadrant; None if either sign is None."""
    if growth_sign is None or inflation_sign is None:
        return None
    return _QUADRANT_BY_SIGNS[(growth_sign, inflation_sign)]


def classify_axis(
    *,
    score: float | None,
    history: list[float],
    prev_sign: int | None,
    coverage: float,
    freshness: float,
    source_health: float,
    u_floor: float,
) -> tuple[AxisDiagnostics, bool, str | None, float]:
    """Run uncertainty -> confidence -> hysteresis for one axis.

    Returns (diagnostics, transition_pending, reason, q_data). When the score or
    uncertainty cannot be computed, returns a fully-NULL diagnostics with
    transition_pending=True (the axis is not consumable).
    """
    q_data = min(coverage, freshness, source_health)
    if score is None:
        # no score at all: carry the prior latched memory forward unchanged.
        return (AxisDiagnostics(None, None, prev_sign, None, None, None, None),
                True, "no_score", q_data)
    u_raw = uncertainty_raw(history, u_floor)
    if u_raw is None:
        return (AxisDiagnostics(score, None, prev_sign, None, None, None, None),
                True, "insufficient_vintages", q_data)
    confidence, u_adj = axis_confidence(score, u_raw, q_data)
    internal_sign, effective_sign, pending, reason = axis_hysteresis(
        prev_sign, score, confidence, min_confidence=MIN_CANDIDATE_CONFIDENCE,
    )
    margin = (prev_sign * score) if prev_sign is not None else abs(score)
    diag = AxisDiagnostics(
        score=score, sign=effective_sign, internal_sign=internal_sign,
        candidate_confidence=confidence,
        margin=margin, uncertainty_raw=u_raw, uncertainty_adjusted=u_adj,
    )
    return diag, pending, reason, q_data


def build_snapshot(
    *,
    as_of: _dt.date,
    computed_at: _dt.datetime,
    previous_snapshot_id: str | None,
    growth_score: float | None,
    growth_history: list[float],
    growth_prev_sign: int | None,
    growth_coverage: float,
    growth_freshness: float,
    growth_health: float,
    growth_contributions: dict[str, float],
    growth_u_floor: float,
    inflation_score: float | None,
    inflation_history: list[float],
    inflation_prev_sign: int | None,
    inflation_coverage: float,
    inflation_freshness: float,
    inflation_health: float,
    inflation_contributions: dict[str, float],
    inflation_u_floor: float,
    input_available_ats: list[_dt.datetime],
    critical_expiries: list[_dt.datetime],
    model_version: str,
    confidence_method: str,
    source_vintage_hash: str,
    critical_structural_failure: bool = False,
    confidence_model_version: str = "confidence_v1.0",
) -> QuadrantSnapshot:
    """Assemble the QuadrantSnapshot from per-axis inputs (freeze §3-§10, owner
    decisions B/C). previous_snapshot_id closes the latched chain and enters the
    deterministic uuid5; per-axis u_floor is the seed from U_FLOOR_SEED."""
    g_diag, g_pending, g_reason, g_q = classify_axis(
        score=growth_score, history=growth_history, prev_sign=growth_prev_sign,
        coverage=growth_coverage, freshness=growth_freshness,
        source_health=growth_health, u_floor=growth_u_floor)
    i_diag, i_pending, i_reason, i_q = classify_axis(
        score=inflation_score, history=inflation_history, prev_sign=inflation_prev_sign,
        coverage=inflation_coverage, freshness=inflation_freshness,
        source_health=inflation_health, u_floor=inflation_u_floor)

    transition_pending = g_pending or i_pending
    reason_bits = [r for r in (g_reason, i_reason)
                   if r not in (None, "init", "hold")]
    transition_reason = ",".join(reason_bits) if reason_bits else None

    # candidate classification follows the candidate sign of the score (NOT the
    # effective post-hysteresis sign), so the UI/audit always has a quadrant guess.
    g_cand = _candidate_sign(growth_score)
    i_cand = _candidate_sign(inflation_score)
    candidate_quadrant = quadrant_from_signs(g_cand, i_cand)

    # consumable quadrant uses the EFFECTIVE (post-hysteresis) signs.
    consumable_quadrant = quadrant_from_signs(g_diag.sign, i_diag.sign)

    coverage_quality = min(growth_coverage, inflation_coverage)
    freshness_quality = min(growth_freshness, inflation_freshness)
    source_health_quality = min(growth_health, inflation_health)
    # a critical source past its hard_deadline expired BEFORE the snapshot computed.
    critical_source_expired = freshness_quality <= 0.0

    confidences = [c for c in (g_diag.candidate_confidence,
                               i_diag.candidate_confidence) if c is not None]
    candidate_confidence = min(confidences) if confidences else None

    available_at = available_at_snapshot(computed_at, input_available_ats)
    data_stale_after, pipeline_stale_after, stale_after = compute_stale_after(
        computed_at, critical_expiries)

    status = resolve_status(
        critical_structural_failure=critical_structural_failure,
        coverage=coverage_quality,
        critical_source_expired=critical_source_expired,
        source_health=source_health_quality,
        candidate_confidence=candidate_confidence if candidate_confidence is not None else 0.0,
        transition_pending=transition_pending,
    )
    # The persisted column never stores 'stale' (read-side / view derive it); a
    # compute-time stale degrades to low_confidence.
    if status == "stale":
        status = "low_confidence"

    # §7 coherence: only 'valid' keeps a non-NULL consumable quadrant + confidence.
    if status == "valid":
        quadrant = consumable_quadrant
        if quadrant is None:
            # both signs must be effective for valid; otherwise demote.
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
        confidence_model_version=confidence_model_version,
        confidence_method=confidence_method,
        source_vintage_hash=source_vintage_hash,
    )


def _candidate_sign(score: float | None) -> int | None:
    if score is None:
        return None
    return 1 if score > 0 else -1


_RECORD_COLS = (
    "snapshot_id", "previous_snapshot_id",
    "quadrant", "candidate_quadrant", "candidate_confidence",
    "growth_score", "growth_sign", "growth_internal_sign",
    "growth_candidate_confidence", "growth_margin",
    "growth_uncertainty_raw", "growth_uncertainty_adjusted",
    "inflation_score", "inflation_sign", "inflation_internal_sign",
    "inflation_candidate_confidence",
    "inflation_margin", "inflation_uncertainty_raw", "inflation_uncertainty_adjusted",
    "coverage_quality", "freshness_quality", "source_health_quality",
    "transition_pending", "transition_reason",
    "as_of", "available_at", "computed_at",
    "data_stale_after", "pipeline_stale_after", "stale_after",
    "status_at_compute", "model_version", "confidence_model_version",
    "confidence_method", "source_vintage_hash",
)


def snapshot_to_record(s: QuadrantSnapshot) -> tuple:
    """Snapshot -> DB tuple in _RECORD_COLS order."""
    return (
        s.snapshot_id, s.previous_snapshot_id,
        s.quadrant, s.candidate_quadrant, s.candidate_confidence,
        s.growth.score, s.growth.sign, s.growth.internal_sign,
        s.growth.candidate_confidence, s.growth.margin,
        s.growth.uncertainty_raw, s.growth.uncertainty_adjusted,
        s.inflation.score, s.inflation.sign, s.inflation.internal_sign,
        s.inflation.candidate_confidence,
        s.inflation.margin, s.inflation.uncertainty_raw, s.inflation.uncertainty_adjusted,
        s.coverage_quality, s.freshness_quality, s.source_health_quality,
        s.transition_pending, s.transition_reason,
        s.as_of, s.available_at, s.computed_at,
        s.data_stale_after, s.pipeline_stale_after, s.stale_after,
        s.status_at_compute, s.model_version, s.confidence_model_version,
        s.confidence_method, s.source_vintage_hash,
    )


def audit_records(
    snapshot_id: str, contributions_by_axis: dict[str, dict[str, float]]
) -> list[tuple]:
    """One audit row per (snapshot_id, axis, series_id) from the contributions.

    The per-observation lineage columns (observation_period/vintage_id/
    revision_number) are NULL in A2 (the worker passes only the weighted z); A3
    wires the real lineage when the vintage walk threads it through.
    """
    rows: list[tuple] = []
    for axis, contribs in contributions_by_axis.items():
        for series_id, weighted_z in contribs.items():
            rows.append((snapshot_id, axis, series_id, weighted_z, None,
                         None, None, None, None, None, None, None))
    return rows


def ensure_schema(conn) -> None:
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), _SCHEMA)
    with open(sql_path, encoding="utf-8") as fh:
        conn.execute(fh.read())
    conn.commit()


_INSERT_SNAPSHOT = (
    f"INSERT INTO regime_quadrant_snapshot ({', '.join(_RECORD_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_RECORD_COLS))}) "
    f"ON CONFLICT (snapshot_id) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in _RECORD_COLS if c != "snapshot_id")
)
_INSERT_AUDIT = (
    "INSERT INTO regime_quadrant_indicator_audit "
    "(snapshot_id, axis, series_id, z_score, weight, coverage, freshness, "
    " source_health, anomaly, observation_period, vintage_id, revision_number) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
    "ON CONFLICT (snapshot_id, axis, series_id) DO UPDATE SET "
    "z_score = EXCLUDED.z_score"
)


def upsert_snapshot(conn, record: tuple, audit_rows: list[tuple]) -> None:
    """Idempotent upsert of one snapshot + its audit rows under one transaction."""
    with conn.cursor() as cur:
        cur.execute(_INSERT_SNAPSHOT, record)
        if audit_rows:
            cur.executemany(_INSERT_AUDIT, audit_rows)
    conn.commit()
