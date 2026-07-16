"""Monthly quadrant decision engine v3 (harness replay of the market-fused
candidate: macro_quadrant_us_v3 = v2 + market-implied growth-axis fusion).

Identical to :mod:`decision_v2` except the GROWTH axis runs the dual-sensor fused
filter: the macro-release composite (primary sensor, unchanged) plus the
market-implied growth observation (auxiliary sensor) — the preserved A2 challenger's
growth proxy, computed PIT from the pack's eod_prices:

  raw_t = SPY adjusted-close 126-business-day return at month-end t
          (the frozen MarketImpliedAxisModel convention, quadrant_market.WINDOW)
  z_t   = robust_z_10y over the trailing 10 years of month-end raw values
          (the frozen macro standardizer discipline: clip((x-med)/(1.4826*MAD), ±4),
          >= MIN_MARKET_HISTORY_MONTHS distinct months required, else None)

Governance invariants (freeze scope §1 — market-implied is NEVER a fallback):
  * fusion happens only on months where the macro sensor observed; a missing
    macro month is unpublishable regardless of the market print;
  * the fusion weight is the sensors' DATA-ESTIMATED noise ratio (the same robust
    MAD estimator both sensors), never a calibrated blend parameter — tau/lambda/
    delta/floors are inherited frozen from confidence_v2.0;
  * the INFLATION axis stays macro-only: the certified pack carries no
    duration-matched breakeven pair (no IEF), and inventing a proxy convention
    ad hoc would be a calibration decision this candidate deliberately avoids.

Emits the same DecisionRow projection; downstream consumers are model-agnostic.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any, Mapping, Sequence

from src import quadrant_assemble_v2 as _qa2
from src.quadrant_confidence_v2 import (
    CONFIDENCE_METHOD_V3_FUSED,
    V2_FILTER_HISTORY_MONTHS,
)
from src.quadrant_market_observation import market_growth_observation_series

from . import decision as _d1
from .pit import PitIndex

MODEL_VERSION = "macro_quadrant_us_v3"


def run_decision_series_v3(
    macro_rows: Sequence[Mapping[str, Any]] | PitIndex,
    eod_rows: Sequence[Mapping[str, Any]],
    start: _dt.date,
    end: _dt.date,
) -> list[_d1.DecisionRow]:
    """The monthly latched v3 decision series (v2 + market-fused growth axis)."""
    index = macro_rows if isinstance(macro_rows, PitIndex) else PitIndex(macro_rows)
    score_cache: dict[tuple[str, _dt.datetime], tuple] = {}
    std_cache: dict = {}

    def score_axis(axis: str, when: _dt.datetime):
        key = (axis, when)
        cached = score_cache.get(key)
        if cached is None:
            cached = _d1._score_axis(index, axis, when, std_cache=std_cache)
            score_cache[key] = cached
        return cached

    g_specs, i_specs = _d1._axis_specs("growth"), _d1._axis_specs("inflation")
    decision_dates = _d1.month_end_decision_dates(start, end)

    # the auxiliary sensor needs the walk-back grid too: every month-end the
    # trailing-window observation builder will touch. Build once over the union.
    earliest_walkback = decision_dates[0]
    walk_months: list[_dt.date] = []
    seen: set[_dt.date] = set()
    for as_of in decision_dates:
        when = _d1._decision_time(as_of)
        for k in range(V2_FILTER_HISTORY_MONTHS - 1, -1, -1):
            t = (when - _dt.timedelta(days=30 * k)).date()
            if t not in seen:
                seen.add(t)
                walk_months.append(t)
    walk_months.sort()
    earliest_walkback = walk_months[0]
    market_by_date = dict(zip(
        walk_months,
        market_growth_observation_series(eod_rows, walk_months)))

    def axis_observations(axis: str, when: _dt.datetime, specs):
        macro_obs: list[tuple[float | None, float | None]] = []
        market_obs: list[tuple[float | None, float | None]] = []
        for k in range(V2_FILTER_HISTORY_MONTHS - 1, -1, -1):
            t = when - _dt.timedelta(days=30 * k)
            score, _, z_by = score_axis(axis, t)
            coverage = _d1._coverage(z_by, specs)
            macro_obs.append((score, coverage if score is not None else None))
            market_obs.append(market_by_date.get(t.date(), (None, None)))
        return macro_obs, market_obs

    prev_id: str | None = None
    prev_published: str | None = None
    out: list[_d1.DecisionRow] = []
    for as_of in decision_dates:
        decision_time = _d1._decision_time(as_of)

        g_score, g_contrib, g_z = score_axis("growth", decision_time)
        i_score, i_contrib, i_z = score_axis("inflation", decision_time)
        g_obs, g_aux = axis_observations("growth", decision_time, g_specs)
        i_obs, _ = axis_observations("inflation", decision_time, i_specs)

        g_cov = _d1._coverage(g_z, g_specs)
        i_cov = _d1._coverage(i_z, i_specs)
        g_health = 1.0 if g_score is not None else 0.0
        i_health = 1.0 if i_score is not None else 0.0

        expiry = decision_time + _dt.timedelta(days=45)
        source_vintage_hash = _d1._vintage_hash(g_z, i_z, as_of)

        snap = _qa2.build_snapshot_v2(
            as_of=as_of, computed_at=decision_time,
            previous_snapshot_id=prev_id,
            prev_published_quadrant=prev_published,
            growth_observations=g_obs,
            growth_coverage=g_cov, growth_freshness=1.0, growth_health=g_health,
            inflation_observations=i_obs,
            inflation_coverage=i_cov, inflation_freshness=1.0,
            inflation_health=i_health,
            input_available_ats=[decision_time],
            critical_expiries=[expiry],
            model_version=MODEL_VERSION,
            source_vintage_hash=source_vintage_hash,
            growth_auxiliary_observations=g_aux,
            confidence_method=CONFIDENCE_METHOD_V3_FUSED,
        )

        out.append(_d1.DecisionRow(
            as_of=as_of,
            quadrant=snap.quadrant,
            candidate_quadrant=snap.candidate_quadrant,
            status=snap.status_at_compute,
            growth_score=snap.growth.score,
            inflation_score=snap.inflation.score,
            growth_sign=snap.growth.sign,
            inflation_sign=snap.inflation.sign,
            growth_internal_sign=snap.growth.internal_sign,
            inflation_internal_sign=snap.inflation.internal_sign,
            coverage_quality=snap.coverage_quality,
            candidate_confidence=snap.candidate_confidence,
            transition_pending=snap.transition_pending,
            transition_reason=snap.transition_reason,
        ))

        prev_id = snap.snapshot_id
        if snap.quadrant is not None:
            prev_published = snap.quadrant

    return out
