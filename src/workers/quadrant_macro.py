# src/workers/quadrant_macro.py
"""MacroReleaseAxisModel — the OFFICIAL strategic quadrant (freeze v1 §A, scope §1).

Consumes the point-in-time vintage store (A1 latest_vintage_as_of) for the seed
basket (A1 SEED_SOURCES), applies the per-series transform (seed: yoy), aggregates
by axis_weights, and emits the SAME QuadrantSnapshot the market worker emits via
the shared assembler. NEVER reads the latest-revision macro_data (look-ahead).
Market-implied is a separate worker and NEVER a fallback here: a bad macro snapshot
is persisted as non-valid and the backend turns that into QUADRANT_UNAVAILABLE.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
from typing import Any

from src import quadrant_assemble as qa
from src.db import LOCK_REGIME_QUADRANT, advisory_lock, connect
from src.macro_pit import latest_vintage_as_of
from src.macro_sources import SEED_SOURCES, axis_weights
from src.quadrant_confidence import U_FLOOR_SEED
from src.quadrant_score import axis_score, standardized_latest
from src.quadrant_staleness import source_expiry

MODEL_VERSION = "macro_quadrant_us_v1"
CONFIDENCE_METHOD = "rolling_score_mad_distinct_vintages_v1"
SCORE_HISTORY_VINTAGES = 36   # distinct vintages window for uncertainty (>= MIN 24)
FRESHNESS_DECAY_WINDOW = _dt.timedelta(days=14)  # soft->hard linear decay (decision D)


def _axis_specs(axis: str):
    return [s for s in SEED_SOURCES if s.axis == axis]


def _score_axis(
    conn, axis: str, decision_time: _dt.datetime,
) -> tuple[float | None, dict[str, float], dict[str, float], list[_dt.datetime], list[_dt.datetime]]:
    """Compute (score, contributions, raw_z_by_series, input_available_ats,
    critical_expiries) for one axis from the PIT vintage store.

    raw z per series = latest transformed value <= decision_date; available_at_j =
    the vintage available_at proxied by decision_time (the PIT read already filters
    available_at <= decision_time, so the value IS knowable now). critical_expiries
    uses each MacroSourceSpec's cadence/grace/hard_max_age.
    """
    specs = _axis_specs(axis)
    weights = axis_weights(axis)
    series_ids = [s.series_id for s in specs]
    decision_date = decision_time.date()
    pit = latest_vintage_as_of(conn, series_ids, decision_time)

    z_by_series: dict[str, float | None] = {}
    for spec in specs:
        series = pit.get(spec.series_id, {})
        # two-stage standardize (economic_transform_id -> robust_z); None = missing.
        z = standardized_latest(spec, series, decision_date)
        # direction: a source whose rise means the OPPOSITE of the axis flips sign.
        z_by_series[spec.series_id] = (z * spec.direction) if z is not None else None

    score, contributions = axis_score(weights, z_by_series)

    input_available_ats = [decision_time]  # PIT guarantees availability <= now
    critical_expiries: list[_dt.datetime] = []
    for spec in specs:
        if not spec.critical:
            continue
        # monthly macro: next_expected_release seed = available + cadence (~30d);
        # the hard_max_age (45d) usually binds. (A3 will wire real release calendars.)
        next_release = decision_time + _dt.timedelta(days=30)
        critical_expiries.append(source_expiry(
            decision_time, next_release, spec.grace_period, spec.hard_max_age,
            FRESHNESS_DECAY_WINDOW))
    return score, contributions, z_by_series, input_available_ats, critical_expiries


def _coverage(z_by_series: dict[str, float], specs) -> float:
    """Σ|w|·I(valid) / Σ|w| over the axis (freeze §6 importance-weighted coverage)."""
    total = sum(abs(s.weight) for s in specs)
    if total <= 0:
        return 0.0
    have = sum(abs(s.weight) for s in specs
               if z_by_series.get(s.series_id) is not None)
    return have / total


def _score_history(conn, axis: str, decision_time: _dt.datetime) -> list[float]:
    """Distinct-vintage score history for the uncertainty MAD window.

    Walk back SCORE_HISTORY_VINTAGES monthly decision points, recomputing the axis
    score at each, and keep the DISTINCT values (the worker's recompute is
    deterministic given the vintage store).
    """
    history: list[float] = []
    for k in range(SCORE_HISTORY_VINTAGES):
        t = decision_time - _dt.timedelta(days=30 * (k + 1))
        score, *_ = _score_axis(conn, axis, t)
        if score is not None:
            history.append(score)
    return history


def _require_critical_expiries(critical_expiries: list[_dt.datetime]) -> None:
    """Fail loud if the registry yields no critical source expiry.

    The macro quadrant's staleness (compute_stale_after) requires >=1 critical
    source expiry. All SEED_SOURCES are critical today, but a future registry edit
    that drops every critical flag would otherwise surface as a deep
    compute_stale_after ValueError; convert it to a clear worker-level message.
    """
    if not critical_expiries:
        raise ValueError(
            "macro quadrant requires >=1 critical source expiry; "
            "none found in registry")


def ensure_schema(conn) -> None:
    qa.ensure_schema(conn)


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Compute today's macro quadrant snapshot and upsert it (idempotent)."""
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

            # owner decision C — resume the latched chain from the last snapshot.
            prev = qa.load_previous_snapshot(conn, MODEL_VERSION)
            prev_id = prev["previous_snapshot_id"] if prev else None
            g_prev_sign = prev["growth_internal_sign"] if prev else None
            i_prev_sign = prev["inflation_internal_sign"] if prev else None

            g_score, g_contrib, g_z, g_av, g_exp = _score_axis(conn, "growth", decision_time)
            i_score, i_contrib, i_z, i_av, i_exp = _score_axis(conn, "inflation", decision_time)
            g_hist = _score_history(conn, "growth", decision_time)
            i_hist = _score_history(conn, "inflation", decision_time)

            g_specs, i_specs = _axis_specs("growth"), _axis_specs("inflation")
            g_cov, i_cov = _coverage(g_z, g_specs), _coverage(i_z, i_specs)
            # freshness/health: v1 seeds — PIT values are by construction fresh and
            # finite (the read already filtered availability); A3 wires real decay.
            g_fresh = i_fresh = 1.0
            g_health = 1.0 if g_score is not None else 0.0
            i_health = 1.0 if i_score is not None else 0.0

            # fail loud if a future registry edit drops every critical flag, so the
            # staleness guarantee (>=1 critical expiry) is a clear worker error, not a
            # deep compute_stale_after ValueError inside build_snapshot.
            critical_expiries = [*g_exp, *i_exp]
            _require_critical_expiries(critical_expiries)

            source_vintage_hash = _vintage_hash(g_z, i_z, as_of)
            snap = qa.build_snapshot(
                as_of=as_of, computed_at=decision_time, previous_snapshot_id=prev_id,
                growth_score=g_score, growth_history=g_hist, growth_prev_sign=g_prev_sign,
                growth_coverage=g_cov, growth_freshness=g_fresh, growth_health=g_health,
                growth_contributions=g_contrib, growth_u_floor=U_FLOOR_SEED["growth"],
                inflation_score=i_score, inflation_history=i_hist,
                inflation_prev_sign=i_prev_sign,
                inflation_coverage=i_cov, inflation_freshness=i_fresh, inflation_health=i_health,
                inflation_contributions=i_contrib, inflation_u_floor=U_FLOOR_SEED["inflation"],
                input_available_ats=[*g_av, *i_av],
                critical_expiries=critical_expiries,
                model_version=MODEL_VERSION, confidence_method=CONFIDENCE_METHOD,
                source_vintage_hash=source_vintage_hash,
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


def _vintage_hash(g_z: dict[str, Any], i_z: dict[str, Any], as_of: _dt.date) -> str:
    """Stable hash of the inputs that fed this snapshot (provenance / §8 cut)."""
    payload = repr((sorted(g_z.items()), sorted(i_z.items()), as_of.isoformat()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
