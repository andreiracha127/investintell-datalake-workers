"""Offline confidence-v2 candidate experiment (recalibration probe over pack _003).

Replays the certified input pack through the FROZEN v1 primitives, then re-derives
month validity under two CANDIDATE policies and sweeps their knobs, reporting the
Tranche-W1 regime-timeline metrics plus the ratified phase0q_005 timeline-gate
judgments per cell — so a reviewer can SEE how coverage / carry / occupancy /
stability move BEFORE anyone proposes a new default.

Candidate policies
------------------
V1  joint-quadrant-posterior decision rule on the FROZEN u_adj:
      p_axis = Phi(score / u_adj)   (SIGNED score — sign probability, not |.|)
      quadrant posterior = product over axes (independence first order);
      publish argmax iff max posterior >= tau; sticky hysteresis: keep the
      previously PUBLISHED quadrant unless the challenger beats it by delta.

V2  Kalman local-level filter per axis — replaces the score-dispersion denominator
    (1.4826*MAD of the score's own trailing range) with ESTIMATION uncertainty:
      x_t = x_{t-1} + w_t (Q),   y_t = x_t + v_t (R_t)
      R_t = (1.4826*MAD(diff(score), trailing 36 obs))^2 / (2 + lambda),
            floored at 0.10^2, inflated by 1/max(q_data, 0.25)^2   (q_data via
            coverage, mirroring u_adj's quality inflation);  Q = lambda * R_t.
      p_axis = Phi(m_t / sqrt(P_t));  same joint-posterior publish rule.
    A min-rule cell (min axis sign-confidence >= 0.70, frozen decision shape) is
    included to isolate the statistic fix from the decision-rule fix.

Hard gates preserved in EVERY cell: coverage >= 0.80, both axis scores present,
u_raw available (>= 24 distinct vintages). Metrics come from the harness's
regime_timeline_metrics (the exact ratified gate formulas). flips/reversals are
reported because the freeze rule calibrates ONLY against abstention / flips /
reversals / stability — NEVER CAGR/Sharpe.

GOVERNANCE / FIDELITY
---------------------
This is INSTRUMENTATION, not a new default and not a ratification. Nothing here
changes the frozen model: the FROZEN baseline cell replays the pack through the
SAME frozen primitives as harness.phase0q.decision and must reproduce the
certified pack _003 timeline EXACTLY (22 valid / 66 months, fresh_valid_36m
0.1667, abstention streak 18, carry age 18, same-quadrant run 38) — asserted at
run time; the script aborts on any mismatch. The upside-capture gate is OUT OF
SCOPE (it needs the LEAN backtest). NOT wired into CI; never run automatically.

KNOWN FIRST-ORDER LIMITS (candidate refinements, deliberately out of scope):
  * axis independence in the quadrant posterior (a Gaussian copula with rho
    estimated from the score histories is the natural refinement);
  * R_t / lambda are heuristic method-of-moments choices, not MLE.

Usage (from the repo root, with the repo venv):

    python -m scripts.regime_confidence_v2_experiment
    python -m scripts.regime_confidence_v2_experiment --out /some/dir
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import math
import statistics
from pathlib import Path
from typing import Any, Callable, Sequence

from harness.phase0q import decision as _D
from harness.phase0q import metrics as _metrics
from harness.phase0q.pit import PitIndex
from src import quadrant_assemble as _qa
from src.macro_sources import SEED_SOURCES, axis_weights
from src.quadrant_confidence import U_FLOOR_SEED
from src.quadrant_score import axis_score, standardized_latest

ROOT = Path(__file__).resolve().parents[1]
PACK = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_003"

CHAIN_START = _dt.date(2014, 3, 1)
METRICS_START = _dt.date(2021, 1, 1)
END = _dt.date(2026, 6, 30)
FROZEN_WINDOW_YEARS = 10

# ratified artifacts/quant/open_macro_v03_phase0q_005/timeline_gate_policy.json
TIMELINE_GATES = {
    "min_fresh_valid_rate_36m": 0.40,
    "max_abstention_streak_months": 6,
    "max_carry_age_months": 3,
    "max_same_quadrant_run_months": 12,
}

# certified pack _003 frozen-timeline anchor (2021-01..2026-06)
FROZEN_FIDELITY = {
    "n_valid": 22,
    "fresh_36m": 6 / 36,
    "max_abstention_streak_months": 18,
    "max_carry_age_months": 18,
    "max_same_quadrant_run_months": 38,
}

DEFAULT_TAU_GRID = (0.40, 0.45, 0.50, 0.55, 0.60, 0.70)
DEFAULT_LAMBDA_GRID = (0.10, 0.25, 0.50)
DEFAULT_DELTA = 0.10
KALMAN_R_WINDOW = 36
KALMAN_MIN_DIFFS = 12
KALMAN_R_FLOOR = 0.10 ** 2
KALMAN_WARMUP_MAD = 0.35

_NORM = statistics.NormalDist()
_GROWTH_SPECS = [s for s in SEED_SOURCES if s.axis == "growth"]
_INFLATION_SPECS = [s for s in SEED_SOURCES if s.axis == "inflation"]


# --------------------------------------------------------------------------- #
# Frozen replay capturing full per-axis diagnostics                            #
# --------------------------------------------------------------------------- #

def _score_axis(index, axis, decision_time, std_cache):
    specs = _GROWTH_SPECS if axis == "growth" else _INFLATION_SPECS
    weights = axis_weights(axis)
    series_ids = [s.series_id for s in specs]
    decision_date = decision_time.date()
    pit = index.latest_vintage_as_of(series_ids, decision_time)
    z_by: dict[str, float | None] = {}
    for spec in specs:
        series = pit.get(spec.series_id, {})
        key = (spec.series_id, decision_date, tuple(sorted(series.items())))
        if key in std_cache:
            z = std_cache[key]
        else:
            z = standardized_latest(spec, series, decision_date,
                                    window_years=FROZEN_WINDOW_YEARS)
            std_cache[key] = z
        z_by[spec.series_id] = (z * spec.direction) if z is not None else None
    score, contributions = axis_score(weights, z_by)
    return score, contributions, z_by


def frozen_chain(rows) -> list[dict[str, Any]]:
    """Monthly latched FROZEN chain with per-axis score/confidence/u_adj captured.

    Structurally identical to decision.run_decision_series (frozen build_snapshot +
    latch threading); the extra fields are read off the snapshot, never recomputed.
    """
    index = rows if isinstance(rows, PitIndex) else PitIndex(rows)
    score_cache: dict[tuple, Any] = {}
    std_cache: dict[tuple, Any] = {}

    def score_axis(axis, when):
        k = (axis, when)
        if k not in score_cache:
            score_cache[k] = _score_axis(index, axis, when, std_cache)
        return score_cache[k]

    def score_history(axis, when):
        out: list[float] = []
        for kk in range(_D.SCORE_HISTORY_VINTAGES):
            t = when - _dt.timedelta(days=30 * (kk + 1))
            s, *_ = score_axis(axis, t)
            if s is not None:
                out.append(s)
        return out

    prev_id, g_prev, i_prev = None, None, None
    out: list[dict[str, Any]] = []
    for as_of in _D.month_end_decision_dates(CHAIN_START, END):
        dtm = _dt.datetime(as_of.year, as_of.month, as_of.day,
                           tzinfo=_dt.timezone.utc)
        g_s, g_c, g_z = score_axis("growth", dtm)
        i_s, i_c, i_z = score_axis("inflation", dtm)
        snap = _qa.build_snapshot(
            as_of=as_of, computed_at=dtm, previous_snapshot_id=prev_id,
            growth_score=g_s, growth_history=score_history("growth", dtm),
            growth_prev_sign=g_prev,
            growth_coverage=_D._coverage(g_z, _GROWTH_SPECS),
            growth_freshness=1.0, growth_health=1.0 if g_s is not None else 0.0,
            growth_contributions=g_c, growth_u_floor=U_FLOOR_SEED["growth"],
            inflation_score=i_s, inflation_history=score_history("inflation", dtm),
            inflation_prev_sign=i_prev,
            inflation_coverage=_D._coverage(i_z, _INFLATION_SPECS),
            inflation_freshness=1.0, inflation_health=1.0 if i_s is not None else 0.0,
            inflation_contributions=i_c, inflation_u_floor=U_FLOOR_SEED["inflation"],
            input_available_ats=[dtm],
            critical_expiries=[dtm + _dt.timedelta(days=45)],
            model_version=_D.MODEL_VERSION, confidence_method=_D.CONFIDENCE_METHOD,
            source_vintage_hash=_D._vintage_hash(g_z, i_z, as_of))
        out.append({
            "as_of": as_of,
            "status": snap.status_at_compute,
            "quadrant": snap.quadrant,
            "candidate_quadrant": snap.candidate_quadrant,
            "candidate_confidence": snap.candidate_confidence,
            "coverage_quality": snap.coverage_quality,
            "transition_pending": snap.transition_pending,
            "g_score": snap.growth.score, "i_score": snap.inflation.score,
            "g_conf": snap.growth.candidate_confidence,
            "i_conf": snap.inflation.candidate_confidence,
            "g_u_adj": snap.growth.uncertainty_adjusted,
            "i_u_adj": snap.inflation.uncertainty_adjusted,
        })
        prev_id = snap.snapshot_id
        g_prev = snap.growth.internal_sign
        i_prev = snap.inflation.internal_sign
    return out


# --------------------------------------------------------------------------- #
# Pure decision-rule helpers (unit-tested; no pack required)                   #
# --------------------------------------------------------------------------- #

class TimelineRow:
    """Monthly row shaped for regime_timeline_metrics."""

    __slots__ = ("as_of", "status", "quadrant")

    def __init__(self, as_of: _dt.date, status: str, quadrant: str | None):
        self.as_of, self.status, self.quadrant = as_of, status, quadrant

    def has_valid_quadrant(self) -> bool:
        return self.status == "valid" and self.quadrant is not None


def quadrant_posterior(p_growth_pos: float, p_inflation_pos: float) -> dict[str, float]:
    """4-quadrant posterior from axis sign probabilities (independence)."""
    probs: dict[str, float] = {}
    for gs in (1, -1):
        for is_ in (1, -1):
            q = _qa.quadrant_from_signs(gs, is_)
            pg = p_growth_pos if gs == 1 else 1.0 - p_growth_pos
            pi = p_inflation_pos if is_ == 1 else 1.0 - p_inflation_pos
            probs[q] = pg * pi
    return probs


def _hard_gates_ok(row: dict[str, Any]) -> bool:
    return (row["coverage_quality"] is not None and row["coverage_quality"] >= 0.80
            and row["g_score"] is not None and row["i_score"] is not None)


def posterior_series(chain: Sequence[dict[str, Any]],
                     axis_prob_fn: Callable[[dict[str, Any]], tuple[bool, float, float]],
                     tau: float, delta: float) -> list[TimelineRow]:
    """Publish argmax quadrant when max posterior >= tau; sticky hysteresis: keep
    the previously PUBLISHED quadrant while its posterior >= challenger - delta.
    Hard gates (coverage, score presence, u availability) preserved."""
    out: list[TimelineRow] = []
    prev_pub: str | None = None
    for r in chain:
        ok, p_g, p_i = axis_prob_fn(r)
        if not (ok and _hard_gates_ok(r)):
            out.append(TimelineRow(r["as_of"], "low_confidence", None))
            continue
        probs = quadrant_posterior(p_g, p_i)
        best_q = max(probs, key=probs.get)  # type: ignore[arg-type]
        best_p = probs[best_q]
        if best_p < tau:
            out.append(TimelineRow(r["as_of"], "low_confidence", None))
            continue
        pub = best_q
        if (prev_pub is not None and pub != prev_pub
                and probs.get(prev_pub, 0.0) >= best_p - delta):
            pub = prev_pub
        prev_pub = pub
        out.append(TimelineRow(r["as_of"], "valid", pub))
    return out


def min_rule_series(chain: Sequence[dict[str, Any]],
                    axis_prob_fn: Callable[[dict[str, Any]], tuple[bool, float, float]],
                    min_confidence: float = 0.70) -> list[TimelineRow]:
    """Frozen decision SHAPE (min axis confidence >= floor) on candidate axis
    probabilities — isolates the statistic fix from the decision-rule fix."""
    out: list[TimelineRow] = []
    for r in chain:
        ok, p_g, p_i = axis_prob_fn(r)
        conf = min(max(p_g, 1 - p_g), max(p_i, 1 - p_i)) if ok else 0.0
        if not (ok and _hard_gates_ok(r) and conf >= min_confidence):
            out.append(TimelineRow(r["as_of"], "low_confidence", None))
            continue
        q = _qa.quadrant_from_signs(1 if p_g >= 0.5 else -1,
                                    1 if p_i >= 0.5 else -1)
        out.append(TimelineRow(r["as_of"], "valid", q))
    return out


def frozen_axis_probs(row: dict[str, Any]) -> tuple[bool, float, float]:
    """V1 — axis sign probability from the FROZEN u_adj (signed score)."""
    if (row["g_score"] is None or row["i_score"] is None
            or not row["g_u_adj"] or not row["i_u_adj"]):
        return False, 0.5, 0.5
    return (True,
            _NORM.cdf(row["g_score"] / row["g_u_adj"]),
            _NORM.cdf(row["i_score"] / row["i_u_adj"]))


def kalman_filter_series(scores: Sequence[float | None],
                         coverages: Sequence[float | None],
                         lam: float,
                         r_window: int = KALMAN_R_WINDOW,
                         min_diffs: int = KALMAN_MIN_DIFFS) -> list[tuple[float | None, float | None]]:
    """Causal scalar local-level Kalman over a monthly score series.

    Returns [(m_t, P_t)] aligned to the input; (None, None) while unavailable.
    Missing score -> predict-only (P grows by the last Q; the month is
    unpublishable anyway because the hard gates require a fresh score).
    """
    m: float | None = None
    P: float | None = None
    last_q = 0.0
    diffs: list[float] = []
    prev_s: float | None = None
    out: list[tuple[float | None, float | None]] = []
    for s, cov in zip(scores, coverages):
        if s is None:
            if m is not None and P is not None:
                P = P + last_q
            out.append((None, None) if m is None else (m, P))
            prev_s = None
            continue
        if prev_s is not None:
            diffs.append(s - prev_s)
        prev_s = s
        window = diffs[-r_window:]
        if len(window) >= min_diffs:
            med = statistics.median(window)
            mad = statistics.median([abs(d - med) for d in window])
            r_base = (1.4826 * mad) ** 2 / (2.0 + lam)
        else:
            r_base = KALMAN_WARMUP_MAD ** 2 / (2.0 + lam)
        r_base = max(r_base, KALMAN_R_FLOOR)
        q_data = max(cov if cov is not None else 0.0, 0.25)
        R = r_base / (q_data ** 2)
        Q = lam * R
        last_q = Q
        if m is None or P is None:
            m, P = s, R
        else:
            P_pred = P + Q
            K = P_pred / (P_pred + R)
            m = m + K * (s - m)
            P = (1.0 - K) * P_pred
        out.append((m, P))
    return out


def kalman_axis_probs_factory(chain: Sequence[dict[str, Any]],
                              lam: float) -> Callable[[dict[str, Any]], tuple[bool, float, float]]:
    filt = {
        axis: kalman_filter_series([r[f"{axis}_score"] for r in chain],
                                   [r["coverage_quality"] for r in chain], lam)
        for axis in ("g", "i")
    }
    idx = {r["as_of"]: k for k, r in enumerate(chain)}

    def axis_prob_fn(row: dict[str, Any]) -> tuple[bool, float, float]:
        k = idx[row["as_of"]]
        mg, Pg = filt["g"][k]
        mi, Pi = filt["i"][k]
        if (mg is None or mi is None or Pg is None or Pi is None
                or row["g_score"] is None or row["i_score"] is None):
            return False, 0.5, 0.5
        return (True,
                _NORM.cdf(mg / math.sqrt(Pg)),
                _NORM.cdf(mi / math.sqrt(Pi)))

    return axis_prob_fn


# --------------------------------------------------------------------------- #
# Evaluation vs the ratified timeline gates                                    #
# --------------------------------------------------------------------------- #

def gate_report(tl: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "fresh_valid_36m": (tl["fresh_valid_rate"]["rolling_36m"], ">=",
                            TIMELINE_GATES["min_fresh_valid_rate_36m"]),
        "abstention_streak": (tl["max_abstention_streak_months"], "<=",
                              TIMELINE_GATES["max_abstention_streak_months"]),
        "carry_age": (tl["max_carry_age_months"], "<=",
                      TIMELINE_GATES["max_carry_age_months"]),
        "same_quadrant_run": (tl["max_same_quadrant_run_months"], "<=",
                              TIMELINE_GATES["max_same_quadrant_run_months"]),
    }
    out: dict[str, Any] = {}
    for name, (val, op, bound) in checks.items():
        out[name] = {"measured": val, "bound": bound,
                     "pass": (val >= bound) if op == ">=" else (val <= bound)}
    out["all_pass"] = all(v["pass"] for k, v in out.items() if k != "all_pass")
    return out


def window_stability(window: Sequence[TimelineRow]) -> dict[str, Any]:
    """flips (consecutive published-quadrant changes) and 1-month reversals
    (A -> B -> A over successive published months) INSIDE the metrics window."""
    pubs = [r.quadrant for r in window if r.has_valid_quadrant()]
    flips = sum(1 for a, b in zip(pubs, pubs[1:]) if a != b)
    reversals = sum(1 for a, b, c in zip(pubs, pubs[1:], pubs[2:])
                    if a == c and b != a)
    return {"flips": flips, "one_month_reversals": reversals}


def evaluate_cell(label: str, series: Sequence[TimelineRow]) -> dict[str, Any]:
    window = [r for r in series if METRICS_START <= r.as_of <= END]
    tl = _metrics.regime_timeline_metrics(window)
    stab = window_stability(window)
    years = tl["n_months"] / 12.0 if tl["n_months"] else 1.0
    return {"label": label, "timeline_metrics": tl,
            "stability": {**stab, "flips_per_year": round(stab["flips"] / years, 2)},
            "gates": gate_report(tl)}


def frozen_baseline_rows(chain: Sequence[dict[str, Any]]) -> list[TimelineRow]:
    return [TimelineRow(r["as_of"], r["status"],
                        r["quadrant"] if r["status"] == "valid" else None)
            for r in chain]


def assert_frozen_fidelity(cell: dict[str, Any]) -> None:
    tl = cell["timeline_metrics"]
    got = {
        "n_valid": tl["n_valid"],
        "fresh_36m": tl["fresh_valid_rate"]["rolling_36m"],
        "max_abstention_streak_months": tl["max_abstention_streak_months"],
        "max_carry_age_months": tl["max_carry_age_months"],
        "max_same_quadrant_run_months": tl["max_same_quadrant_run_months"],
    }
    for key, expected in FROZEN_FIDELITY.items():
        actual = got[key]
        ok = (abs(actual - expected) < 1e-9 if isinstance(expected, float)
              else actual == expected)
        if not ok:
            raise AssertionError(
                f"frozen-baseline fidelity FAILED on {key}: expected {expected}, "
                f"got {actual} — the replay does not reproduce the certified "
                f"pack _003 timeline; results are NOT comparable. Aborting.")


# --------------------------------------------------------------------------- #
# Sweep + report                                                               #
# --------------------------------------------------------------------------- #

def run_experiment(rows, *, tau_grid=DEFAULT_TAU_GRID,
                   lambda_grid=DEFAULT_LAMBDA_GRID,
                   delta=DEFAULT_DELTA) -> dict[str, Any]:
    chain = frozen_chain(rows)

    cells: list[dict[str, Any]] = []
    baseline = evaluate_cell("frozen_v1_baseline", frozen_baseline_rows(chain))
    assert_frozen_fidelity(baseline)
    cells.append(baseline)

    for tau in tau_grid:
        cells.append(evaluate_cell(
            f"v1_joint_posterior_tau_{tau:.2f}",
            posterior_series(chain, frozen_axis_probs, tau, delta)))

    for lam in lambda_grid:
        fn = kalman_axis_probs_factory(chain, lam)
        for tau in tau_grid:
            cells.append(evaluate_cell(
                f"v2_kalman_lam_{lam:.2f}_tau_{tau:.2f}",
                posterior_series(chain, fn, tau, delta)))
        cells.append(evaluate_cell(
            f"v2_kalman_lam_{lam:.2f}_min_rule_0.70",
            min_rule_series(chain, fn, 0.70)))

    return {
        "artifact_type": "regime_confidence_v2_experiment",
        "status": "experiment_not_ratified",
        "pack": PACK.name,
        "chain_start": CHAIN_START.isoformat(),
        "metrics_window": {"start": METRICS_START.isoformat(),
                           "end": END.isoformat()},
        "timeline_gates": TIMELINE_GATES,
        "frozen_fidelity_anchor": FROZEN_FIDELITY,
        "grid": {"tau": list(tau_grid), "lambda": list(lambda_grid),
                 "delta": delta},
        "upside_capture_gate": "out_of_scope_requires_lean_backtest",
        "cells": cells,
        "governance": {"db_write": "none", "changes_model_default": False,
                       "ratified": False, "self_ratification": "prohibited"},
    }


def to_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# Regime confidence-v2 experiment (NOT ratified)", "",
        f"pack={result['pack']}  chain_start={result['chain_start']}  "
        f"metrics_window={result['metrics_window']['start']}"
        f"..{result['metrics_window']['end']}",
        f"frozen fidelity anchor: {result['frozen_fidelity_anchor']}", "",
        "| cell | n_valid | fresh_36m | fresh_global | abst | carry | run "
        "| flips/y | rev1m | gates |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for c in result["cells"]:
        m = c["timeline_metrics"]
        g = c["gates"]
        marks = "".join("P" if g[k]["pass"] else "F"
                        for k in ("fresh_valid_36m", "abstention_streak",
                                  "carry_age", "same_quadrant_run"))
        lines.append(
            f"| {c['label']} | {m['n_valid']} "
            f"| {m['fresh_valid_rate']['rolling_36m']:.4f} "
            f"| {m['fresh_valid_rate']['global']:.3f} "
            f"| {m['max_abstention_streak_months']} "
            f"| {m['max_carry_age_months']} "
            f"| {m['max_same_quadrant_run_months']} "
            f"| {c['stability']['flips_per_year']:.2f} "
            f"| {c['stability']['one_month_reversals']} "
            f"| {marks}{' ALL_PASS' if g['all_pass'] else ''} |")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="regime_confidence_v2_experiment")
    ap.add_argument("--tau", type=float, nargs="*", default=list(DEFAULT_TAU_GRID))
    ap.add_argument("--lambda", dest="lam", type=float, nargs="*",
                    default=list(DEFAULT_LAMBDA_GRID))
    ap.add_argument("--delta", type=float, default=DEFAULT_DELTA)
    ap.add_argument("--out", default=str(ROOT / "_tmp_confidence_v2_experiment"))
    args = ap.parse_args(argv)

    rows = json.loads(
        (PACK / "data" / "canonical" / "macro_observation_vintage.json")
        .read_text(encoding="utf-8"))
    result = run_experiment(rows, tau_grid=tuple(args.tau),
                            lambda_grid=tuple(args.lam), delta=args.delta)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "confidence_v2_experiment.json").write_text(
        json.dumps(result, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8")
    (out / "confidence_v2_experiment.md").write_text(
        to_markdown(result), encoding="utf-8")
    print(f"wrote {out / 'confidence_v2_experiment.json'} and .md")
    print(to_markdown(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
