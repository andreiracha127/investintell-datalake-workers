# src/workers/quadrant_macro_v2.py
"""MacroReleaseAxisModel v2 — the confidence_v2.0 quadrant stream
(model_version macro_quadrant_us_v2).

SOURCING IS THE FROZEN v1 PATH UNCHANGED: the same PIT vintage read, the same
two-stage transform (economic_transform_id -> robust_z via standardized_latest),
the same axis_weights aggregation, the same coverage/freshness/health seeds and
staleness registry — imported directly from ``quadrant_macro``. Only the
confidence/publish policy differs (Kalman estimation uncertainty + joint quadrant
posterior; see ``src.quadrant_confidence_v2``).

The v2 stream writes the SAME regime_quadrant_snapshot table under its own
model_version — ADDITIVE: no schema change, v1 rows and the v1 worker untouched,
consumers select the stream by model_version. Never a fallback for v1 and never
backfilled: the monthly cron computes forward like every other stream.
"""
from __future__ import annotations

import datetime as _dt

from src import quadrant_assemble as qa
from src import quadrant_assemble_v2 as qa2
from src.db import LOCK_REGIME_QUADRANT, advisory_lock, connect
from src.quadrant_confidence_v2 import V2_FILTER_HISTORY_MONTHS
from src.workers.quadrant_macro import (
    _axis_specs,
    _coverage,
    _require_critical_expiries,
    _score_axis,
    _vintage_hash,
)

MODEL_VERSION = "macro_quadrant_us_v2"


def _axis_observations(
    conn, axis: str, decision_time: _dt.datetime,
) -> tuple[list[tuple[float | None, float | None]], tuple]:
    """Chronological trailing (score, q_data) observations for one axis, current
    month LAST, over V2_FILTER_HISTORY_MONTHS monthly walk-backs at the frozen
    ``t - 30*k days`` convention (k = H-1 .. 0). q_data per historical month is
    its coverage (freshness/health are the frozen v1 worker seeds = 1.0 while a
    score exists). Also returns the CURRENT month's full _score_axis tuple so the
    caller reuses contributions/z/expiries without recomputing."""
    specs = _axis_specs(axis)
    observations: list[tuple[float | None, float | None]] = []
    current: tuple | None = None
    for k in range(V2_FILTER_HISTORY_MONTHS - 1, -1, -1):
        t = decision_time - _dt.timedelta(days=30 * k)
        scored = _score_axis(conn, axis, t)
        score, _, z_by, _, _ = scored
        coverage = _coverage(z_by, specs)
        observations.append((score, coverage if score is not None else None))
        if k == 0:
            current = scored
    assert current is not None
    return observations, current


def ensure_schema(conn) -> None:
    qa.ensure_schema(conn)


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Compute today's v2 macro quadrant snapshot and upsert it (idempotent)."""
    decision_time = (
        _dt.datetime.fromisoformat(calc_date).replace(tzinfo=_dt.timezone.utc)
        if calc_date else _dt.datetime.now(_dt.timezone.utc)
    )
    as_of = decision_time.date()
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_REGIME_QUADRANT) as got:
            if not got:
                return {"days": 0, "upserted": 0, "skipped": "lock_busy"}
            ensure_schema(conn)

            prev = qa2.load_previous_state_v2(conn, MODEL_VERSION, as_of)

            g_obs, g_now = _axis_observations(conn, "growth", decision_time)
            i_obs, i_now = _axis_observations(conn, "inflation", decision_time)
            g_score, g_contrib, g_z, g_av, g_exp = g_now
            i_score, i_contrib, i_z, i_av, i_exp = i_now

            g_specs, i_specs = _axis_specs("growth"), _axis_specs("inflation")
            g_cov, i_cov = _coverage(g_z, g_specs), _coverage(i_z, i_specs)
            g_fresh = i_fresh = 1.0
            g_health = 1.0 if g_score is not None else 0.0
            i_health = 1.0 if i_score is not None else 0.0

            critical_expiries = [*g_exp, *i_exp]
            _require_critical_expiries(critical_expiries)

            snap = qa2.build_snapshot_v2(
                as_of=as_of, computed_at=decision_time,
                previous_snapshot_id=prev["previous_snapshot_id"],
                prev_published_quadrant=prev["prev_published_quadrant"],
                growth_observations=g_obs,
                growth_coverage=g_cov, growth_freshness=g_fresh,
                growth_health=g_health,
                inflation_observations=i_obs,
                inflation_coverage=i_cov, inflation_freshness=i_fresh,
                inflation_health=i_health,
                input_available_ats=[*g_av, *i_av],
                critical_expiries=critical_expiries,
                model_version=MODEL_VERSION,
                source_vintage_hash=_vintage_hash(g_z, i_z, as_of),
            )
            qa.upsert_snapshot(
                conn, qa.snapshot_to_record(snap),
                qa.audit_records(snap.snapshot_id,
                                 {"growth": g_contrib, "inflation": i_contrib}),
            )
    return {
        "days": 1, "upserted": 1, "status": snap.status_at_compute,
        "quadrant": snap.quadrant, "candidate_quadrant": snap.candidate_quadrant,
        "candidate_confidence": snap.candidate_confidence,
        "as_of": as_of.isoformat(), "model_version": MODEL_VERSION,
    }
