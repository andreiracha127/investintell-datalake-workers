"""QuadrantSnapshot / AxisDiagnostics — the EXACT contract both A2 workers emit.

Both MacroReleaseAxisModel (official) and MarketImpliedAxisModel (challenger)
build instances of QuadrantSnapshot (freeze v1 §3) — they differ ONLY in how the
per-axis scores are sourced, never in the snapshot shape. effective_status is
derived AT READ (the worker never rewrites old snapshots); status_at_compute is
persisted and immutable.
"""
from __future__ import annotations

import datetime as _dt
import uuid as _uuid
from dataclasses import dataclass
from typing import Literal

Quadrant = Literal["recovery", "expansion", "slowdown", "contraction"]
ComputeStatus = Literal["valid", "low_confidence", "unavailable", "invalid"]
EffectiveStatus = Literal["valid", "low_confidence", "stale", "unavailable", "invalid"]

# Fixed namespace for the deterministic snapshot uuid5 (owner decision). Stable
# forever — changing it would renumber every snapshot id.
REGIME_SNAPSHOT_NAMESPACE = _uuid.uuid5(
    _uuid.NAMESPACE_URL, "investintell/regime_quadrant_snapshot")


@dataclass(frozen=True)
class AxisDiagnostics:
    score: float | None
    sign: Literal[-1, 1] | None          # EFFECTIVE post-hysteresis (consumable) sign; NULL if not consumable
    internal_sign: Literal[-1, 1] | None  # LATCHED memory carried to the next run (persisted *_internal_sign)
    candidate_confidence: float | None
    margin: float | None
    uncertainty_raw: float | None
    uncertainty_adjusted: float | None


@dataclass(frozen=True)
class QuadrantSnapshot:
    snapshot_id: str                     # uuid5 as text (owner decision)
    previous_snapshot_id: str | None     # predecessor in the latched chain; None at genesis
    quadrant: Quadrant | None            # consumable; non-NULL only when status_at_compute=="valid"
    candidate_quadrant: Quadrant | None  # instantaneous classification (audit/UI)
    candidate_confidence: float | None
    growth: AxisDiagnostics
    inflation: AxisDiagnostics
    coverage_quality: float
    freshness_quality: float
    source_health_quality: float
    transition_pending: bool
    transition_reason: str | None
    as_of: _dt.date
    available_at: _dt.datetime
    computed_at: _dt.datetime
    data_stale_after: _dt.datetime
    pipeline_stale_after: _dt.datetime
    stale_after: _dt.datetime
    status_at_compute: ComputeStatus
    model_version: str
    confidence_model_version: str
    confidence_method: str
    source_vintage_hash: str


def make_snapshot_id(
    model_version: str,
    as_of: _dt.date,
    source_vintage_hash: str,
    previous_snapshot_id: str | None,
) -> str:
    """Deterministic ``uuid5`` over the canonical key (owner decision):

        model_version | as_of | source_vintage_hash | previous_snapshot_id|"GENESIS"

    The predecessor is part of the identity because the latched hysteresis result
    depends on it; re-running the same model with the same inputs AND predecessor
    reproduces the same id, so the daily upsert is idempotent. The genesis run
    (no predecessor) hashes the literal "GENESIS". Returned as text for the DB
    (the column is a real ``uuid``; psycopg adapts the text).
    """
    key = "|".join([
        model_version, as_of.isoformat(), source_vintage_hash,
        previous_snapshot_id or "GENESIS",
    ])
    return str(_uuid.uuid5(REGIME_SNAPSHOT_NAMESPACE, key))


def effective_status(snapshot: QuadrantSnapshot, now: _dt.datetime) -> EffectiveStatus:
    """Freeze §3: a valid snapshot becomes 'stale' once now >= stale_after; any
    other status passes through unchanged (never relabelled to stale)."""
    if snapshot.status_at_compute == "valid" and now >= snapshot.stale_after:
        return "stale"
    return snapshot.status_at_compute
