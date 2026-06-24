# src/workers/quadrant_market.py
"""MarketImpliedAxisModel — the preserved CHALLENGER (freeze scope §1, model
market_implied_quadrant_v0). Emits the SAME QuadrantSnapshot as the macro worker
via the shared assembler, from market proxies: growth = SPY 126d return; inflation
= TIP/IEF breakeven 126d momentum (the signals regime_gate already computes). This
worker runs SEPARATELY and is NEVER a fallback for the macro model — it exists for
shadow/regression/divergence research only.

Reuses the proxy fetch/align via a thin local copy of the price plumbing rather
than importing regime_gate (which must stay untouched). The score history for the
uncertainty MAD is the rolling 126d-return series over a 252-bd window.
"""
from __future__ import annotations

import datetime as _dt
import hashlib

from src import quadrant_assemble as qa
from src.db import LOCK_REGIME_QUADRANT, advisory_lock, connect
from src.quadrant_confidence import U_FLOOR_SEED
from src.quadrant_staleness import add_business_days, source_expiry

MODEL_VERSION = "market_implied_quadrant_v0"
CONFIDENCE_METHOD = "rolling_score_mad_252bd_v1"
WINDOW = 126
HISTORY_BD = 252
SPY_TICKER, IEF_TICKER, TIP_TICKER = "SPY", "IEF", "TIP"
HISTORY_START = _dt.date(2003, 1, 1)


def window_return(levels_desc: list[float], look: int) -> float | None:
    """levels newest-first: levels[0]/levels[look] - 1, or None during warmup."""
    if len(levels_desc) <= look:
        return None
    now, then = levels_desc[0], levels_desc[look]
    return (now / then - 1.0) if then > 0 else None


def rolling_score_history(levels_desc: list[float], look: int, span: int) -> list[float]:
    """Rolling window_return over the last ``span`` business days (newest-first)."""
    out: list[float] = []
    for offset in range(span):
        sub = levels_desc[offset:]
        r = window_return(sub, look)
        if r is not None:
            out.append(r)
    return out


def _fetch_levels(calc_date: _dt.date | None):
    """SPY level (growth) and TIP/IEF breakeven level (inflation), newest-first.

    Self-contained Tiingo fetch (does NOT import regime_gate). SPY is the spine;
    TIP/IEF carried forward on non-print days, both as ratio levels.
    """
    from src.workers._tiingo import TiingoClient

    with TiingoClient() as client:
        spy = client.fetch_daily_prices(SPY_TICKER, HISTORY_START, calc_date)
        ief = client.fetch_daily_prices(IEF_TICKER, HISTORY_START, calc_date)
        tip = client.fetch_daily_prices(TIP_TICKER, HISTORY_START, calc_date)
    if not spy:
        raise RuntimeError("Tiingo returned empty SPY history")

    ief_by = {d: float(v) for d, v in ief if v is not None and v > 0}
    tip_by = {d: float(v) for d, v in tip if v is not None and v > 0}
    last_ief = last_tip = None
    spy_levels: list[float] = []
    be_levels: list[float | None] = []
    for d, px in sorted(spy, key=lambda t: t[0]):
        if px is None or px <= 0:
            continue
        last_ief = ief_by.get(d, last_ief)
        last_tip = tip_by.get(d, last_tip)
        spy_levels.append(float(px))
        if last_tip is not None and last_ief and last_ief > 0:
            be_levels.append(last_tip / last_ief)
        else:
            be_levels.append(None)
    return spy_levels[::-1], be_levels[::-1]  # newest-first


def ensure_schema(conn) -> None:
    qa.ensure_schema(conn)


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Compute today's market-implied quadrant snapshot and upsert it."""
    cdate = _dt.date.fromisoformat(calc_date) if calc_date else None
    computed_at = _dt.datetime.now(_dt.timezone.utc)
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_REGIME_QUADRANT) as got:
            if not got:
                return {"days": 0, "upserted": 0, "skipped": "lock_busy"}
            ensure_schema(conn)

            as_of = cdate or computed_at.date()
            # owner decision C — resume the latched chain from the last snapshot
            # STRICTLY BEFORE today's as_of (idempotent rerun + no backfill look-ahead).
            prev = qa.load_previous_snapshot(conn, MODEL_VERSION, as_of)
            prev_id = prev["previous_snapshot_id"] if prev else None
            g_prev_sign = prev["growth_internal_sign"] if prev else None
            i_prev_sign = prev["inflation_internal_sign"] if prev else None

            spy_desc, be_desc = _fetch_levels(cdate)
            be_clean = [b for b in be_desc if b is not None]

            g_score = window_return(spy_desc, WINDOW)
            i_score = window_return(be_clean, WINDOW) if len(be_clean) > WINDOW else None
            g_hist = rolling_score_history(spy_desc, WINDOW, HISTORY_BD)
            i_hist = rolling_score_history(be_clean, WINDOW, HISTORY_BD)

            g_contrib = {"SPY_126d": g_score} if g_score is not None else {}
            i_contrib = {"TIP_IEF_126d": i_score} if i_score is not None else {}
            g_cov = 1.0 if g_score is not None else 0.0
            i_cov = 1.0 if i_score is not None else 0.0
            # market hard_max_age = 3 business days; available_at = computed (close+1
            # is already implied by using closes up to cdate). decay window 0 -> the
            # hard deadline binds immediately past soft for a daily source.
            expiries = [source_expiry(
                computed_at, add_business_days(computed_at, 1),
                _dt.timedelta(days=0), _dt.timedelta(days=3),
                _dt.timedelta(days=0))]

            vintage_hash = hashlib.sha256(
                repr((round(g_score or 0, 8), round(i_score or 0, 8),
                      as_of.isoformat())).encode()).hexdigest()
            snap = qa.build_snapshot(
                as_of=as_of, computed_at=computed_at, previous_snapshot_id=prev_id,
                growth_score=g_score, growth_history=g_hist, growth_prev_sign=g_prev_sign,
                growth_coverage=g_cov, growth_freshness=1.0,
                growth_health=1.0 if g_score is not None else 0.0,
                growth_contributions=g_contrib, growth_u_floor=U_FLOOR_SEED["growth"],
                inflation_score=i_score, inflation_history=i_hist,
                inflation_prev_sign=i_prev_sign,
                inflation_coverage=i_cov, inflation_freshness=1.0,
                inflation_health=1.0 if i_score is not None else 0.0,
                inflation_contributions=i_contrib, inflation_u_floor=U_FLOOR_SEED["inflation"],
                input_available_ats=[computed_at],
                critical_expiries=expiries,
                model_version=MODEL_VERSION, confidence_method=CONFIDENCE_METHOD,
                source_vintage_hash=vintage_hash,
            )
            qa.upsert_snapshot(
                conn, qa.snapshot_to_record(snap),
                qa.audit_records(snap.snapshot_id,
                                 {"growth": g_contrib, "inflation": i_contrib}),
            )
    return {
        "days": 1, "upserted": 1, "status": snap.status_at_compute,
        "quadrant": snap.quadrant, "candidate_quadrant": snap.candidate_quadrant,
        "as_of": as_of.isoformat(), "model_version": MODEL_VERSION,
    }
