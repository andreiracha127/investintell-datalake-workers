# src/workers/quadrant_macro_v3.py
"""Market-fused macro quadrant v3 — model_version macro_quadrant_us_v3
(confidence_v2.0 policy + market-implied growth-axis sensor fusion).

Identical to ``quadrant_macro_v2`` except the GROWTH axis runs the dual-sensor
fused filter: the frozen macro-release composite (primary, unchanged sourcing)
plus the market-implied growth observation (auxiliary) — the preserved A2
challenger's SPY 126bd-return proxy, standardized with the frozen robust-z-10y
discipline, read PIT from the ``eod_prices`` table. The builder is the SAME
function the harness replay uses (``src.quadrant_market_observation``), so
worker/harness parity holds by construction.

Governance invariants (freeze scope §1): the market sensor can sharpen the
growth state but can NEVER substitute for macro — a missing macro month stays
unpublishable; the fusion weight is the sensors' data-estimated noise ratio
(never a calibrated blend parameter); the INFLATION axis stays macro-only.
ADDITIVE stream: same table, own model_version; v1/v2 rows and workers untouched.
"""
from __future__ import annotations

import datetime as _dt

from src import quadrant_assemble as qa
from src import quadrant_assemble_v2 as qa2
from src.db import LOCK_REGIME_QUADRANT, advisory_lock, connect
from src.quadrant_confidence_v2 import (
    CONFIDENCE_METHOD_V3_FUSED,
    V2_FILTER_HISTORY_MONTHS,
)
from src.quadrant_market_observation import (
    MARKET_GROWTH_TICKER,
    market_growth_observation_series,
)
from src.workers.quadrant_macro import (
    _axis_specs,
    _coverage,
    _require_critical_expiries,
    _vintage_hash,
)
from src.workers.quadrant_macro_v2 import _axis_observations

MODEL_VERSION = "macro_quadrant_us_v3"

_EOD_SQL = (
    "SELECT date, adj_close AS adjusted_close FROM eod_prices "
    "WHERE ticker = %(ticker)s AND date <= %(as_of)s "
    "AND adj_close IS NOT NULL AND adj_close > 0 ORDER BY date"
)


def _market_growth_observations(
    conn, decision_time: _dt.datetime,
) -> list[tuple[float | None, float | None]]:
    """The auxiliary growth sensor over the SAME trailing walk-back grid the v2
    macro observations use (t - 30*k days, k = H-1 .. 0, current month LAST)."""
    as_of = decision_time.date()
    with conn.cursor() as cur:
        cur.execute(_EOD_SQL, {"ticker": MARKET_GROWTH_TICKER, "as_of": as_of})
        rows = [{"ticker": MARKET_GROWTH_TICKER, "date": r[0],
                 "adjusted_close": float(r[1])} for r in cur.fetchall()]
    grid = [(decision_time - _dt.timedelta(days=30 * k)).date()
            for k in range(V2_FILTER_HISTORY_MONTHS - 1, -1, -1)]
    return market_growth_observation_series(rows, grid)


def ensure_schema(conn) -> None:
    qa.ensure_schema(conn)


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Compute today's v3 fused macro quadrant snapshot and upsert it (idempotent)."""
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
            g_aux = _market_growth_observations(conn, decision_time)

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
                growth_auxiliary_observations=g_aux,
                confidence_method=CONFIDENCE_METHOD_V3_FUSED,
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
