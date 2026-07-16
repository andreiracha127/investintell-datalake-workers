"""Monthly quadrant decision engine v2 (harness reimplementation of the
macro_quadrant_us_v2 worker path).

Parity strategy — identical to ``decision.py``'s: the scoring formulas
(``standardized_latest`` / ``axis_score``) are IMPORTED UNMODIFIED and the v2
policy runs through ``src.quadrant_assemble_v2.build_snapshot_v2`` UNMODIFIED —
only the DB-coupled orchestration of ``quadrant_macro_v2`` is reimplemented
in-memory over pack vintage rows:

* per-axis observations = the worker's trailing ``V2_FILTER_HISTORY_MONTHS``
  monthly walk-backs at ``t - 30*k days`` (k = H-1 .. 0, current month LAST),
  each carrying its own coverage as q_data — byte-parity with
  ``quadrant_macro_v2._axis_observations``;
* the latched v2 chain threads previous_snapshot_id + the previously PUBLISHED
  quadrant (the sticky incumbent) from run to run, genesis = None — parity with
  ``quadrant_assemble_v2.load_previous_state_v2`` over an append-only stream.

Emits the same :class:`harness.phase0q.decision.DecisionRow` projection so every
downstream consumer (sleeve, metrics, timeline gates) is model-agnostic.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any, Mapping, Sequence

from src import quadrant_assemble_v2 as _qa2
from src.quadrant_confidence_v2 import V2_FILTER_HISTORY_MONTHS

from . import decision as _d1
from .pit import PitIndex

MODEL_VERSION = "macro_quadrant_us_v2"


def run_decision_series_v2(
    rows: Sequence[Mapping[str, Any]] | PitIndex,
    start: _dt.date,
    end: _dt.date,
) -> list[_d1.DecisionRow]:
    """Compute the monthly latched v2 decision series over ``[start, end]``.

    Faithfully reproduces ``quadrant_macro_v2.run`` executed once per month-end,
    threading the v2 latched chain (previous_snapshot_id + previously published
    quadrant) from run to run.
    """
    index = rows if isinstance(rows, PitIndex) else PitIndex(rows)
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

    def axis_observations(axis: str, when: _dt.datetime, specs):
        obs: list[tuple[float | None, float | None]] = []
        for k in range(V2_FILTER_HISTORY_MONTHS - 1, -1, -1):
            t = when - _dt.timedelta(days=30 * k)
            score, _, z_by = score_axis(axis, t)
            coverage = _d1._coverage(z_by, specs)
            obs.append((score, coverage if score is not None else None))
        return obs

    prev_id: str | None = None
    prev_published: str | None = None
    out: list[_d1.DecisionRow] = []
    for as_of in _d1.month_end_decision_dates(start, end):
        decision_time = _d1._decision_time(as_of)

        g_score, g_contrib, g_z = score_axis("growth", decision_time)
        i_score, i_contrib, i_z = score_axis("inflation", decision_time)
        g_obs = axis_observations("growth", decision_time, g_specs)
        i_obs = axis_observations("inflation", decision_time, i_specs)

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
