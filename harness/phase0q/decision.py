"""Monthly quadrant decision engine (harness reimplementation of the OFFICIAL
open_macro_v03 decision path).

Parity strategy — the scoring formulas are IMPORTED UNMODIFIED from the frozen
modules, and only the orchestration (which the DB-coupled worker performs) is
reimplemented in-memory over pack-v2 vintage rows:

* ``standardized_latest`` and ``axis_score`` (``src.quadrant_score``) UNMODIFIED,
  fed the axis specs from ``src.macro_sources.SEED_SOURCES`` and the normalized
  ``axis_weights``. This is exactly what ``quadrant_macro._score_axis`` does,
  including the per-series ``z * spec.direction`` sign-flow and the None==missing
  treatment.
* ``uncertainty_raw`` / ``axis_confidence`` (``src.quadrant_confidence``) and
  ``axis_hysteresis`` (``src.quadrant_hysteresis``) UNMODIFIED, via
  ``quadrant_assemble.classify_axis`` / ``build_snapshot`` UNMODIFIED — so the
  hysteresis / latch / coverage / status semantics are parity by construction.

The harness supplies the same per-run inputs the worker computes from the DB:
  * PIT read = ``harness.phase0q.pit.latest_vintage_as_of`` (parity-tested),
  * freshness = 1.0, health = 1.0 if score else 0.0 (worker v1 seeds),
  * score history = 36 monthly look-backs recomputed at ``t - 30*(k+1)`` days,
  * the latched chain: each decision threads the prior decision's per-axis
    ``internal_sign`` as ``prev_sign`` (owner decision C), starting genesis=None.

Scenario-grid parameter deltas (growth_weight / inflation_weight / risk_tilt /
*_delta_pp) do NOT alter the decision path in this harness: the calibration grid's
axis-weight probes are +/-2pp reweightings of the 50/50 axis blend that the
*allocator* would apply downstream, not the per-series SEED weights the classifier
uses. The classifier is deterministic given the vintage store and is therefore
identical across candidates; the parameters flow into the sleeve tilt/constraints
(see ``sleeve.py``). The decision series is computed ONCE and shared across the
grid, which also makes the grid's ``identical decision series`` requirement exact.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from src.macro_sources import SEED_SOURCES, axis_weights
from src.quadrant_score import axis_score, standardized_latest
from src import quadrant_assemble as _qa
from src.quadrant_confidence import U_FLOOR_SEED

from .pit import PitIndex

MODEL_VERSION = "macro_quadrant_us_v1"
CONFIDENCE_METHOD = "rolling_score_mad_distinct_vintages_v1"
SCORE_HISTORY_VINTAGES = 36  # mirrors quadrant_macro.SCORE_HISTORY_VINTAGES

_AXES = ("growth", "inflation")


def _axis_specs(axis: str):
    return [s for s in SEED_SOURCES if s.axis == axis]


def month_end_decision_dates(start: _dt.date, end: _dt.date) -> list[_dt.date]:
    """Month-end decision dates in ``[start, end]`` (rebalance cadence).

    A "month end" is the last calendar day of each month; the first date is the
    month-end on or after ``start`` and the last is the month-end on or before
    ``end`` (matches the sleeve's ``monthly, month-end decision date`` policy).
    """
    dates: list[_dt.date] = []
    year, month = start.year, start.month
    while True:
        me = _month_end(year, month)
        if me > end:
            break
        if me >= start:
            dates.append(me)
        month += 1
        if month > 12:
            month = 1
            year += 1
    return dates


def _month_end(year: int, month: int) -> _dt.date:
    if month == 12:
        return _dt.date(year, 12, 31)
    return _dt.date(year, month + 1, 1) - _dt.timedelta(days=1)


def _decision_time(as_of: _dt.date) -> _dt.datetime:
    return _dt.datetime(as_of.year, as_of.month, as_of.day, tzinfo=_dt.timezone.utc)


def _score_axis(
    index: PitIndex, axis: str, decision_time: _dt.datetime,
    *, std_cache: dict | None = None,
) -> tuple[float | None, dict[str, float], dict[str, float | None]]:
    """(score, contributions, z_by_series) for one axis — parity with
    ``quadrant_macro._score_axis`` (PIT read + per-series direction + axis_score).

    ``std_cache`` optionally memoizes ``standardized_latest`` by (series, as_of,
    PIT-series signature) — a pure-function cache that only accelerates repeated
    identical inputs and never changes the value.
    """
    specs = _axis_specs(axis)
    weights = axis_weights(axis)
    series_ids = [s.series_id for s in specs]
    decision_date = decision_time.date()
    pit = index.latest_vintage_as_of(series_ids, decision_time)

    z_by_series: dict[str, float | None] = {}
    for spec in specs:
        series = pit.get(spec.series_id, {})
        z = _standardized_latest_cached(spec, series, decision_date, std_cache)
        z_by_series[spec.series_id] = (z * spec.direction) if z is not None else None

    score, contributions = axis_score(weights, z_by_series)
    return score, contributions, z_by_series


def _standardized_latest_cached(spec, series, as_of, std_cache):
    if std_cache is None:
        return standardized_latest(spec, series, as_of)
    # Signature: only the periods <= as_of feed the transform/standardizer, and the
    # transform's own trailing-window logic + the 10y cutoff select the inputs; the
    # full PIT dict signature is a safe (identity) key. Frozen tuple of sorted items.
    signature = (spec.series_id, as_of, tuple(sorted(series.items())))
    cached = std_cache.get(signature)
    if cached is _MISS:
        return None
    if cached is not None:
        return cached
    value = standardized_latest(spec, series, as_of)
    std_cache[signature] = value if value is not None else _MISS
    return value


_MISS = object()


def _coverage(z_by_series: Mapping[str, float | None], specs) -> float:
    """Importance-weighted coverage — parity with ``quadrant_macro._coverage``."""
    total = sum(abs(s.weight) for s in specs)
    if total <= 0:
        return 0.0
    have = sum(abs(s.weight) for s in specs
               if z_by_series.get(s.series_id) is not None)
    return have / total


def _score_history(
    index: PitIndex, axis: str, decision_time: _dt.datetime,
) -> list[float]:
    """Distinct-vintage score history — parity with ``quadrant_macro._score_history``:
    walk back ``SCORE_HISTORY_VINTAGES`` points at ``t - 30*(k+1)`` days."""
    history: list[float] = []
    for k in range(SCORE_HISTORY_VINTAGES):
        t = decision_time - _dt.timedelta(days=30 * (k + 1))
        score, *_ = _score_axis(index, axis, t)
        if score is not None:
            history.append(score)
    return history


@dataclass(frozen=True)
class DecisionRow:
    """One monthly decision — the harness-visible projection of a QuadrantSnapshot."""

    as_of: _dt.date
    quadrant: str | None            # consumable (valid-only) quadrant
    candidate_quadrant: str | None
    status: str
    growth_score: float | None
    inflation_score: float | None
    growth_sign: int | None         # effective (post-hysteresis) sign
    inflation_sign: int | None
    growth_internal_sign: int | None  # latched memory threaded to next run
    inflation_internal_sign: int | None
    coverage_quality: float
    candidate_confidence: float | None
    transition_pending: bool
    transition_reason: str | None

    def has_valid_quadrant(self) -> bool:
        return self.status == "valid" and self.quadrant is not None


def run_decision_series(
    rows: Sequence[Mapping[str, Any]] | PitIndex,
    start: _dt.date,
    end: _dt.date,
) -> list[DecisionRow]:
    """Compute the monthly latched decision series over ``[start, end]``.

    ``rows`` may be the raw pack-v2 vintage rows or a prebuilt :class:`PitIndex`
    (reused across the scenario grid so the store is indexed once).

    Faithfully reproduces ``quadrant_macro.run`` executed once per month-end with
    ``quadrant_assemble.build_snapshot`` UNMODIFIED, threading the latched chain
    (previous_snapshot_id + per-axis internal_sign) from run to run.
    """
    index = rows if isinstance(rows, PitIndex) else PitIndex(rows)
    # Memoize per-(axis, decision_time) axis scoring: consecutive months' 36-point
    # history look-backs overlap almost entirely, so caching collapses ~87k
    # transform recomputations to a few thousand. Deterministic (pure function of
    # the frozen index + time), so it does not affect results — only speed.
    score_cache: dict[tuple[str, _dt.datetime], tuple] = {}
    std_cache: dict = {}

    def score_axis(axis: str, when: _dt.datetime):
        cache_key = (axis, when)
        cached = score_cache.get(cache_key)
        if cached is None:
            cached = _score_axis(index, axis, when, std_cache=std_cache)
            score_cache[cache_key] = cached
        return cached

    def score_history(axis: str, when: _dt.datetime) -> list[float]:
        history: list[float] = []
        for k in range(SCORE_HISTORY_VINTAGES):
            t = when - _dt.timedelta(days=30 * (k + 1))
            score, *_ = score_axis(axis, t)
            if score is not None:
                history.append(score)
        return history

    g_specs, i_specs = _axis_specs("growth"), _axis_specs("inflation")
    prev_id: str | None = None
    g_prev_sign: int | None = None
    i_prev_sign: int | None = None

    out: list[DecisionRow] = []
    for as_of in month_end_decision_dates(start, end):
        decision_time = _decision_time(as_of)

        g_score, g_contrib, g_z = score_axis("growth", decision_time)
        i_score, i_contrib, i_z = score_axis("inflation", decision_time)
        g_hist = score_history("growth", decision_time)
        i_hist = score_history("inflation", decision_time)

        g_cov = _coverage(g_z, g_specs)
        i_cov = _coverage(i_z, i_specs)
        g_health = 1.0 if g_score is not None else 0.0
        i_health = 1.0 if i_score is not None else 0.0

        # Deterministic critical expiry (build_snapshot requires >=1); the exact
        # value only feeds stale_after ordering, which the harness does not judge.
        expiry = decision_time + _dt.timedelta(days=45)
        source_vintage_hash = _vintage_hash(g_z, i_z, as_of)

        snap = _qa.build_snapshot(
            as_of=as_of, computed_at=decision_time, previous_snapshot_id=prev_id,
            growth_score=g_score, growth_history=g_hist, growth_prev_sign=g_prev_sign,
            growth_coverage=g_cov, growth_freshness=1.0, growth_health=g_health,
            growth_contributions=g_contrib, growth_u_floor=U_FLOOR_SEED["growth"],
            inflation_score=i_score, inflation_history=i_hist,
            inflation_prev_sign=i_prev_sign,
            inflation_coverage=i_cov, inflation_freshness=1.0, inflation_health=i_health,
            inflation_contributions=i_contrib, inflation_u_floor=U_FLOOR_SEED["inflation"],
            input_available_ats=[decision_time],
            critical_expiries=[expiry],
            model_version=MODEL_VERSION, confidence_method=CONFIDENCE_METHOD,
            source_vintage_hash=source_vintage_hash,
        )

        out.append(DecisionRow(
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

        # Advance the latched chain (owner decision C): the persisted per-axis
        # internal_sign becomes the next run's prev_sign.
        prev_id = snap.snapshot_id
        g_prev_sign = snap.growth.internal_sign
        i_prev_sign = snap.inflation.internal_sign

    return out


def _vintage_hash(g_z: Mapping[str, Any], i_z: Mapping[str, Any], as_of: _dt.date) -> str:
    """Stable provenance hash of the axis inputs — parity with
    ``quadrant_macro._vintage_hash`` (so snapshot ids match the worker)."""
    import hashlib
    payload = repr((sorted(g_z.items()), sorted(i_z.items()), as_of.isoformat()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
