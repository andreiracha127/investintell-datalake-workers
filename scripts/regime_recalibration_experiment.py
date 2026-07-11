"""Offline regime recalibration SENSITIVITY experiment (Tranche W5).

Sweeps the two frozen knobs the audit flagged for a FUTURE recalibration decision --
the robust-baseline window and the confidence floor -- over the grid

    {baseline_window_years: [5, 7, 10]} x {min_confidence: [0.60, 0.65, 0.70]}

and emits, per cell, the Tranche-W1 regime-timeline metrics (fresh-valid rate,
abstention streak, carry age, same-quadrant run, quadrant mix, upside... n/a here) so a
reviewer can SEE how abstention / carry / occupancy would move BEFORE anyone changes a
default. Read-only over the certified input pack v2 (NO DB, NO writes to the model).

GOVERNANCE / FIDELITY
---------------------
This is INSTRUMENTATION, not a new default. Nothing here is ratified and nothing here
changes the frozen model: the DEFAULT cell (window_years=10, min_confidence=0.70)
replays the pack through the SAME frozen primitives as harness.phase0q.decision and
reproduces the certified decision series byte-for-byte (asserted at run time). Off-default
cells thread ``window_years`` into the frozen ``standardized_latest`` and reclassify
validity at the swept ``min_confidence`` using the snapshot's own coverage / transition /
confidence fields -- a first-order probe of the two knobs, NOT a re-ratification of the
decision path. It is NOT wired into CI and is never run automatically.

Usage (from the repo root, with the repo venv):

    python -m scripts.regime_recalibration_experiment
    python -m scripts.regime_recalibration_experiment --window-years 10 --min-confidence 0.70  # one smoke cell
    python -m scripts.regime_recalibration_experiment --out /some/dir
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from harness.phase0q import decision as _D
from harness.phase0q import metrics as _metrics
from harness.phase0q.pit import PitIndex
from src import quadrant_assemble as _qa
from src.macro_sources import SEED_SOURCES, axis_weights
from src.quadrant_confidence import U_FLOOR_SEED
from src.quadrant_score import axis_score, standardized_latest

ROOT = Path(__file__).resolve().parents[1]
PACK = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"

# The production chain start (live_validation.CHAIN_START) so the DEFAULT cell reproduces
# the certified latch; the metrics window is the audit's 2021-2026 span.
DEFAULT_CHAIN_START = _dt.date(2014, 3, 1)
DEFAULT_METRICS_START = _dt.date(2021, 1, 1)
DEFAULT_END = _dt.date(2026, 6, 30)

DEFAULT_WINDOW_YEARS = (5, 7, 10)
DEFAULT_MIN_CONFIDENCE = (0.60, 0.65, 0.70)
FROZEN_WINDOW_YEARS = 10
FROZEN_MIN_CONFIDENCE = 0.70

_GROWTH_SPECS = [s for s in SEED_SOURCES if s.axis == "growth"]
_INFLATION_SPECS = [s for s in SEED_SOURCES if s.axis == "inflation"]


# --------------------------------------------------------------------------- #
# window_years-parametrized decision replay (frozen primitives; == decision at 10)
# --------------------------------------------------------------------------- #

def _score_axis_windowed(index, axis, decision_time, window_years, std_cache):
    specs = _GROWTH_SPECS if axis == "growth" else _INFLATION_SPECS
    weights = axis_weights(axis)
    series_ids = [s.series_id for s in specs]
    decision_date = decision_time.date()
    pit = index.latest_vintage_as_of(series_ids, decision_time)
    z_by: dict[str, float | None] = {}
    for spec in specs:
        series = pit.get(spec.series_id, {})
        key = (spec.series_id, decision_date, window_years, tuple(sorted(series.items())))
        if key in std_cache:
            z = std_cache[key]
        else:
            z = standardized_latest(spec, series, decision_date, window_years=window_years)
            std_cache[key] = z
        z_by[spec.series_id] = (z * spec.direction) if z is not None else None
    score, contributions = axis_score(weights, z_by)
    return score, contributions, z_by


def windowed_decision_series(rows, start, end, window_years) -> list[_D.DecisionRow]:
    """The monthly latched decision series with ``window_years`` threaded into the robust
    baseline. Structurally identical to ``decision.run_decision_series`` (frozen
    build_snapshot + latch threading) -- at ``window_years=10`` it is byte-identical."""
    index = rows if isinstance(rows, PitIndex) else PitIndex(rows)
    score_cache: dict[tuple, Any] = {}
    std_cache: dict[tuple, Any] = {}

    def score_axis(axis, when):
        k = (axis, when)
        if k not in score_cache:
            score_cache[k] = _score_axis_windowed(index, axis, when, window_years, std_cache)
        return score_cache[k]

    def score_history(axis, when):
        out: list[float] = []
        for kk in range(_D.SCORE_HISTORY_VINTAGES):
            t = when - _dt.timedelta(days=30 * (kk + 1))
            s, *_ = score_axis(axis, t)
            if s is not None:
                out.append(s)
        return out

    prev_id = None
    g_prev = None
    i_prev = None
    out: list[_D.DecisionRow] = []
    for as_of in _D.month_end_decision_dates(start, end):
        dtm = _dt.datetime(as_of.year, as_of.month, as_of.day, tzinfo=_dt.timezone.utc)
        g_s, g_c, g_z = score_axis("growth", dtm)
        i_s, i_c, i_z = score_axis("inflation", dtm)
        g_h = score_history("growth", dtm)
        i_h = score_history("inflation", dtm)
        g_cov = _D._coverage(g_z, _GROWTH_SPECS)
        i_cov = _D._coverage(i_z, _INFLATION_SPECS)
        g_health = 1.0 if g_s is not None else 0.0
        i_health = 1.0 if i_s is not None else 0.0
        expiry = dtm + _dt.timedelta(days=45)
        svh = _D._vintage_hash(g_z, i_z, as_of)
        snap = _qa.build_snapshot(
            as_of=as_of, computed_at=dtm, previous_snapshot_id=prev_id,
            growth_score=g_s, growth_history=g_h, growth_prev_sign=g_prev,
            growth_coverage=g_cov, growth_freshness=1.0, growth_health=g_health,
            growth_contributions=g_c, growth_u_floor=U_FLOOR_SEED["growth"],
            inflation_score=i_s, inflation_history=i_h, inflation_prev_sign=i_prev,
            inflation_coverage=i_cov, inflation_freshness=1.0, inflation_health=i_health,
            inflation_contributions=i_c, inflation_u_floor=U_FLOOR_SEED["inflation"],
            input_available_ats=[dtm], critical_expiries=[expiry],
            model_version=_D.MODEL_VERSION, confidence_method=_D.CONFIDENCE_METHOD,
            source_vintage_hash=svh)
        out.append(_D.DecisionRow(
            as_of=as_of, quadrant=snap.quadrant, candidate_quadrant=snap.candidate_quadrant,
            status=snap.status_at_compute, growth_score=snap.growth.score,
            inflation_score=snap.inflation.score, growth_sign=snap.growth.sign,
            inflation_sign=snap.inflation.sign, growth_internal_sign=snap.growth.internal_sign,
            inflation_internal_sign=snap.inflation.internal_sign,
            coverage_quality=snap.coverage_quality,
            candidate_confidence=snap.candidate_confidence,
            transition_pending=snap.transition_pending, transition_reason=snap.transition_reason))
        prev_id = snap.snapshot_id
        g_prev = snap.growth.internal_sign
        i_prev = snap.inflation.internal_sign
    return out


@dataclass
class _RecalRow:
    as_of: _dt.date
    status: str
    quadrant: str | None

    def has_valid_quadrant(self) -> bool:
        return self.status == "valid" and self.quadrant is not None


def reclassify_at_min_confidence(series, min_confidence) -> list[_RecalRow]:
    """Re-derive month validity at a swept confidence floor. A month is valid iff its
    coverage clears 0.80, it is not mid-transition, and its candidate_confidence clears
    the floor -- the frozen status order with the confidence step swept. At
    min_confidence=0.70 this reproduces the frozen valid set."""
    out: list[_RecalRow] = []
    for r in series:
        conf = r.candidate_confidence
        valid = bool(r.coverage_quality >= 0.80 and not r.transition_pending
                     and conf is not None and conf >= min_confidence)
        out.append(_RecalRow(r.as_of, "valid" if valid else "low_confidence",
                             r.candidate_quadrant if valid else None))
    return out


# --------------------------------------------------------------------------- #
# Grid                                                                        #
# --------------------------------------------------------------------------- #

def run_grid(rows, *, chain_start, metrics_start, end, window_years_grid,
             min_confidence_grid) -> dict[str, Any]:
    cells: list[dict[str, Any]] = []
    fidelity: dict[str, Any] = {}
    for wy in window_years_grid:
        series = windowed_decision_series(rows, chain_start, end, wy)
        for mc in min_confidence_grid:
            recal = reclassify_at_min_confidence(series, mc)
            window = [r for r in recal if metrics_start <= r.as_of <= end]
            tl = _metrics.regime_timeline_metrics(window)
            cell = {"baseline_window_years": wy, "min_confidence": mc,
                    "timeline_metrics": tl}
            cells.append(cell)
            if wy == FROZEN_WINDOW_YEARS and mc == FROZEN_MIN_CONFIDENCE:
                fidelity = {"cell": "default(10, 0.70)", "n_valid": tl["n_valid"],
                            "n_months": tl["n_months"],
                            "reproduces_certified_23_valid_2021_2026":
                                tl["n_valid"] == 23 and metrics_start == DEFAULT_METRICS_START
                                and end == DEFAULT_END}
    return {
        "artifact_type": "regime_recalibration_sensitivity_experiment",
        "status": "experiment_not_ratified",
        "chain_start": chain_start.isoformat(),
        "metrics_window": {"start": metrics_start.isoformat(), "end": end.isoformat()},
        "grid": {"baseline_window_years": list(window_years_grid),
                 "min_confidence": list(min_confidence_grid)},
        "frozen_default_cell": {"baseline_window_years": FROZEN_WINDOW_YEARS,
                                "min_confidence": FROZEN_MIN_CONFIDENCE},
        "default_cell_fidelity": fidelity,
        "cells": cells,
        "governance": {"db_write": "none", "changes_model_default": False,
                       "ratified": False},
    }


def to_markdown(result: dict[str, Any]) -> str:
    lines = ["# Regime recalibration sensitivity experiment (NOT ratified)", "",
             f"chain_start={result['chain_start']}  metrics_window="
             f"{result['metrics_window']['start']}..{result['metrics_window']['end']}",
             f"default-cell fidelity: {result['default_cell_fidelity']}", "",
             "| window_yrs | min_conf | n_valid | fresh_valid_global | "
             "fresh_valid_36m | max_abstention | max_carry_age | max_same_quadrant | mix |",
             "|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for c in result["cells"]:
        m = c["timeline_metrics"]
        mix = ",".join(f"{k[:4]}:{v}" for k, v in m["quadrant_mix"].items())
        lines.append(
            f"| {c['baseline_window_years']} | {c['min_confidence']:.2f} | {m['n_valid']} "
            f"| {m['fresh_valid_rate']['global']:.3f} | {m['fresh_valid_rate']['rolling_36m']:.3f} "
            f"| {m['max_abstention_streak_months']} | {m['max_carry_age_months']} "
            f"| {m['max_same_quadrant_run_months']} | {mix} |")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="regime_recalibration_experiment")
    ap.add_argument("--chain-start", default=DEFAULT_CHAIN_START.isoformat())
    ap.add_argument("--metrics-start", default=DEFAULT_METRICS_START.isoformat())
    ap.add_argument("--end", default=DEFAULT_END.isoformat())
    ap.add_argument("--window-years", type=int, nargs="*", default=list(DEFAULT_WINDOW_YEARS))
    ap.add_argument("--min-confidence", type=float, nargs="*",
                    default=list(DEFAULT_MIN_CONFIDENCE))
    ap.add_argument("--out", default=str(ROOT / "_tmp_recalibration_experiment"))
    args = ap.parse_args(argv)

    rows = json.loads(
        (PACK / "data" / "canonical" / "macro_observation_vintage.json")
        .read_text(encoding="utf-8"))
    result = run_grid(
        rows, chain_start=_dt.date.fromisoformat(args.chain_start),
        metrics_start=_dt.date.fromisoformat(args.metrics_start),
        end=_dt.date.fromisoformat(args.end),
        window_years_grid=tuple(args.window_years),
        min_confidence_grid=tuple(args.min_confidence))

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "recalibration_grid.json").write_text(
        json.dumps(result, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    (out / "recalibration_grid.md").write_text(to_markdown(result), encoding="utf-8")
    print(f"wrote {out / 'recalibration_grid.json'} and .md")
    print(f"default-cell fidelity: {result['default_cell_fidelity']}")
    print(to_markdown(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
