"""Deterministic generator for the phase0q_004 deliverables (OOS semantics + sleeve).

Usage (from the repo root):

    python -m harness.phase0q.generate_phase0q_004

Writes three artifacts into ``artifacts/quant/open_macro_v03_phase0q_004/``, derived
ENTIRELY from already-committed, hash-pinned evidence (no new simulation):

  * ``oos_dispersion_semantics_amendment.json`` — quant_owner decision (2026-07-03):
    the OOS stability statistic becomes a stress-overlap jackknife — at most ONE fold
    (the argmax deviation) is excluded per statistic, and only when its test window
    overlaps a full_series stress window judged GO under carry semantics. Bounds are
    UNCHANGED. The excluded fold is reported in full as a diagnostic.
  * ``sleeve_selection_record.json`` — quant_owner decision (2026-07-03): compressed_50
    becomes the candidate sleeve (his post-cloud-leg condition satisfied by the closed
    reproducibility matrix); baseline_100 is retained as the documented reference.
  * ``quantitative_gate_judgment.phase0q_004.json`` — the consolidated judgment on the
    compressed_50 sleeve: turnover / drawdown / volatility / stress from the committed
    compression grid, OOS under the amended semantics. Supersedes phase0q_003 traceably.

**Governance (non-negotiable):** everything stays candidate_not_approved / A5 blocked;
this generator records decisions and re-aggregates measured evidence — it approves
nothing. Task 2 stays blocked until the quant_owner's threshold sign-off.
"""

from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_004"

GRID_DIR = ROOT / "artifacts" / "quant" / "open_macro_v03_compression_grid_001"
PHASE0Q_003_DIR = ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_003"
PHASE0Q_001_DIR = ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_001"
REPRO_DIR = ROOT / "artifacts" / "quant" / "open_macro_v03_reproducibility_001"

DECISION_DATE = "2026-07-03"
CANDIDATE_SLEEVE = "compressed_50"
REFERENCE_SLEEVE = "baseline_100"
BASE_COST_BPS = 5

GOVERNANCE_PINS = {
    "A5": "blocked",
    "activation_allowed": False,
    "allocator_publish": False,
    "db_write_mode": "none",
    "freeze_ready": False,
    "official_result": False,
    "runtime_activation": False,
}

# base profile bounds (unchanged from phase0q_001 selection; the amendment changes the
# STATISTIC, never a bound).
MDD_DEV_BOUND = 0.08
SIGMA_DEV_BOUND = 0.05
FOLD_ABS_MDD_BOUND = 0.25
FOLD_ABS_VOL_BOUND = 0.12


def _round12(value: float) -> float:
    return round(float(value), 12)


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8", newline="\n")


def _overlaps(fold: dict[str, Any], window: dict[str, Any]) -> bool:
    return fold["test_start"] <= window["end_date"] and window["start_date"] <= fold["test_end"]


def go_stress_windows() -> list[dict[str, Any]]:
    """full_series stress windows judged GO in the prevailing stress judgment (003)."""
    policy = _load(PHASE0Q_001_DIR / "stress_oos_policy.json")
    judgment = _load(PHASE0Q_003_DIR / "quantitative_gate_judgment.phase0q_003.json")
    judged = judgment["gates"]["stress"]["windows"]
    return [w for w in policy["stress_windows"]
            if w["coverage"] == "full_series" and judged.get(w["window_id"], {}).get("go") is True]


def jackknife_statistic(folds: list[dict[str, Any]], field: str, dev_bound: float,
                        stress_windows: list[dict[str, Any]]) -> dict[str, Any]:
    """Stress-overlap jackknife max-dev-from-median for one statistic.

    All folds first; if (and only if) the argmax-deviation fold's test window overlaps
    a GO full_series stress window, that single fold is excluded and median + max-dev
    are recomputed over the remaining folds. At most one fold is ever excluded.
    """
    values = [f[field] for f in folds]
    median_all = statistics.median(values)
    devs = [abs(v - median_all) for v in values]
    argmax = devs.index(max(devs))
    overlap = [w["window_id"] for w in stress_windows if _overlaps(folds[argmax], w)]

    result: dict[str, Any] = {
        "all_folds": {
            "median": _round12(median_all),
            "max_dev_from_median": _round12(max(devs)),
            "argmax_fold_index": folds[argmax]["fold_index"],
        },
        "dev_bound": dev_bound,
        "excluded_fold_index": None,
        "excluded_fold_stress_overlap": [],
    }
    eligible = folds
    if overlap:
        eligible = [f for i, f in enumerate(folds) if i != argmax]
        result["excluded_fold_index"] = folds[argmax]["fold_index"]
        result["excluded_fold_stress_overlap"] = overlap
    ev = [f[field] for f in eligible]
    med = statistics.median(ev)
    max_dev = max(abs(v - med) for v in ev)
    result["eligible_folds"] = {
        "count": len(eligible),
        "median": _round12(med),
        "max_dev_from_median": _round12(max_dev),
    }
    result["pass"] = max_dev <= dev_bound
    return result


def absolute_bounds_check(folds: list[dict[str, Any]],
                          excluded: set[int]) -> dict[str, Any]:
    """Per-fold absolute bounds over the eligible folds (the excluded stress-overlap
    fold's realized risk is judged by the stress gate, not twice)."""
    eligible = [f for f in folds if f["fold_index"] not in excluded]
    max_mdd = max(f["MDD"] for f in eligible)
    max_vol = max(f["volatility"] for f in eligible)
    return {
        "eligible_fold_count": len(eligible),
        "max_fold_MDD": _round12(max_mdd),
        "mdd_bound": FOLD_ABS_MDD_BOUND,
        "mdd_pass": max_mdd <= FOLD_ABS_MDD_BOUND,
        "max_fold_volatility": _round12(max_vol),
        "vol_bound": FOLD_ABS_VOL_BOUND,
        "vol_pass": max_vol <= FOLD_ABS_VOL_BOUND,
    }


def measure_variant_oos(variant: str, fold_report: dict[str, Any],
                        stress_windows: list[dict[str, Any]]) -> dict[str, Any]:
    folds = fold_report["variants"][variant]["folds"]
    mdd = jackknife_statistic(folds, "MDD", MDD_DEV_BOUND, stress_windows)
    vol = jackknife_statistic(folds, "volatility", SIGMA_DEV_BOUND, stress_windows)
    excluded = {i for i in (mdd["excluded_fold_index"], vol["excluded_fold_index"])
                if i is not None}
    absolute = absolute_bounds_check(folds, excluded)
    verdict_pass = mdd["pass"] and vol["pass"] and absolute["mdd_pass"] and absolute["vol_pass"]
    excluded_diag = [
        {k: (_round12(v) if isinstance(v, float) else v) for k, v in f.items()
         if not isinstance(v, (dict, list))}
        for f in folds if f["fold_index"] in excluded
    ]
    return {
        "variant_id": variant,
        "mdd_stability": mdd,
        "volatility_stability": vol,
        "absolute_bounds": absolute,
        "excluded_fold_diagnostics": excluded_diag,
        "verdict": "go_candidate" if verdict_pass else "no_go",
    }


def build_amendment(fold_report: dict[str, Any],
                    stress_windows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "artifact_type": "phase0q_oos_dispersion_semantics_amendment",
        "schema_version": 1,
        "phase0q_id": "open_macro_v03_phase0q_004",
        "decision_date": DECISION_DATE,
        "decided_by": "Andrei Rachadel",
        "decided_by_role": "quant_owner",
        "decision": (
            "The OOS stability statistic becomes a stress-overlap jackknife: max-dev-from-"
            "median is computed over all folds; if the argmax-deviation fold's test window "
            "overlaps a full_series stress window judged GO under carry semantics, that "
            "SINGLE fold is excluded and the statistic is recomputed over the remaining "
            "folds (median re-derived). At most one fold is excluded per statistic. The "
            "per-fold absolute bounds apply to the eligible folds; the excluded fold's "
            "realized risk is judged by the stress gate (no double counting of the same "
            "event by two gates with different semantics). The excluded fold is reported "
            "in full as a diagnostic."
        ),
        "rationale": (
            "max-dev-from-median has breakdown point zero: a single extreme regime defines "
            "the whole gate. Fold 5 (2022-03-01..2023-02-28, the 2022 inflation shock) is "
            "the sole bound violation on both statistics and on the absolute volatility "
            "bound, in both sleeves; the SECOND-largest deviation passes the unchanged "
            "bounds with margin everywhere. The same 2022 event is already judged GO by "
            "the stress gate (INFLATION_SHOCK_2022, carry semantics, realized risk within "
            "the base profile), so keeping it inside the OOS max-dev double-counts one "
            "event across two gates. Excluding at most the single argmax stress-overlap "
            "fold preserves the gate's power (8 of 9 folds always count) and loosens no "
            "numeric bound."
        ),
        "bounds_changed": False,
        "bounds": {
            "max_fold_mdd_deviation": MDD_DEV_BOUND,
            "max_fold_volatility_deviation": SIGMA_DEV_BOUND,
            "fold_absolute_max_drawdown": FOLD_ABS_MDD_BOUND,
            "fold_absolute_max_volatility": FOLD_ABS_VOL_BOUND,
        },
        "go_stress_windows_considered": [
            {"window_id": w["window_id"], "start_date": w["start_date"],
             "end_date": w["end_date"]} for w in stress_windows
        ],
        "re_measured": {
            variant: measure_variant_oos(variant, fold_report, stress_windows)
            for variant in (REFERENCE_SLEEVE, CANDIDATE_SLEEVE)
        },
        "provenance": [
            "artifacts/quant/open_macro_v03_compression_grid_001/oos_fold_report.json",
            "artifacts/quant/open_macro_v03_phase0q_003/quantitative_gate_judgment.phase0q_003.json",
            "artifacts/quant/open_macro_v03_phase0q_001/stress_oos_policy.json",
        ],
        "approved": False,
        "approval_required_from": "quant_owner",
        "status": "candidate_not_approved",
        "task2_effect": "Task 2 stays BLOCKED until the quant_owner's threshold sign-off.",
        "governance": dict(GOVERNANCE_PINS),
    }


def build_sleeve_record(grid: dict[str, Any]) -> dict[str, Any]:
    base = grid["variants"][REFERENCE_SLEEVE]["cost_grid"]["by_cost_bps"][str(BASE_COST_BPS)]
    cand = grid["variants"][CANDIDATE_SLEEVE]["cost_grid"]["by_cost_bps"][str(BASE_COST_BPS)]
    stress = grid["variants"][CANDIDATE_SLEEVE]["stress_windows"]
    return {
        "artifact_type": "phase0q_sleeve_selection_record",
        "schema_version": 1,
        "phase0q_id": "open_macro_v03_phase0q_004",
        "decision_date": DECISION_DATE,
        "decided_by": "Andrei Rachadel",
        "decided_by_role": "quant_owner",
        "selected_sleeve": CANDIDATE_SLEEVE,
        "reference_sleeve_retained": REFERENCE_SLEEVE,
        "decision_scope": "candidate sleeve for the phase0q_004 judgment; NOT final institutional approval",
        "precondition_satisfied": (
            "the quant_owner conditioned the sleeve decision on the closed cloud leg; the "
            "reproducibility matrix closed on 2026-07-03 (backtest efd8c9cc..., "
            "artifacts/quant/open_macro_v03_reproducibility_001, PR #27)."
        ),
        "comparison_at_base_cost": {
            "cost_bps": BASE_COST_BPS,
            REFERENCE_SLEEVE: base,
            CANDIDATE_SLEEVE: cand,
        },
        "candidate_stress_windows": stress,
        "rationale": (
            "compressed_50 nearly dominates baseline_100 on the measured grid: annualized "
            "one-way turnover 1.03 vs 1.61 (-36%), total volatility 7.38% vs 8.21%, window "
            "return +114.7% vs +103.5%, max drawdown ~equal (15.10% vs 14.97%); across OOS "
            "folds it is more stable (lower vol MAD) and its 2022 stress-window volatility "
            "is materially lower (13.31% vs 15.28%). Its only inferior number (MDD max-dev "
            "0.107 vs 0.101) is immaterial under the amended OOS semantics, where both "
            "sleeves pass."
        ),
        "provenance": [
            "artifacts/quant/open_macro_v03_compression_grid_001/grid_results.json",
            "artifacts/quant/open_macro_v03_compression_grid_001/oos_fold_report.json",
            "artifacts/quant/open_macro_v03_reproducibility_001/provenance.json",
        ],
        "approved": False,
        "approval_required_from": "quant_owner",
        "status": "candidate_not_approved",
        "task2_effect": "Task 2 stays BLOCKED until the quant_owner's threshold sign-off.",
        "governance": dict(GOVERNANCE_PINS),
    }


def build_judgment(grid: dict[str, Any], amendment: dict[str, Any]) -> dict[str, Any]:
    cand = grid["variants"][CANDIDATE_SLEEVE]["cost_grid"]["by_cost_bps"][str(BASE_COST_BPS)]
    stress = grid["variants"][CANDIDATE_SLEEVE]["stress_windows"]
    oos = amendment["re_measured"][CANDIDATE_SLEEVE]
    stress_windows = {
        wid: {
            **w,
            "go": (w["consumable_position_coverage"] >= 1.0
                   and w["worst_5d_return"] >= -0.10
                   and w["window_MDD"] <= 0.25),
        } for wid, w in stress.items()
    }
    gates = {
        "turnover": {
            "applied_rule": ("reference_sleeve_turnover_candidate_bound=2.00 (annualized "
                             "one-way sleeve turnover; phase0q_003 amendment); "
                             "signal_design_turnover_bound=0.60 is a separate design bound"),
            "measured": cand["annualized_turnover"],
            "reference_sleeve_turnover_candidate_bound": 2.0,
            "signal_design_turnover_bound": 0.6,
            "provenance": [
                "artifacts/quant/open_macro_v03_compression_grid_001/grid_results.json",
                "artifacts/quant/open_macro_v03_phase0q_003/turnover_threshold_context_amendment.json",
            ],
            "verdict": "pass_candidate_under_reference_sleeve_policy",
        },
        "drawdown": {
            "applied_rule": "max_drawdown <= 0.25 (base profile, unchanged)",
            "bound": 0.25,
            "measured": cand["max_drawdown"],
            "provenance": [
                "artifacts/quant/open_macro_v03_compression_grid_001/grid_results.json"],
            "verdict": "go" if cand["max_drawdown"] <= 0.25 else "no_go",
        },
        "volatility": {
            "applied_rule": "annualized_volatility <= 0.12 (base profile, unchanged)",
            "bound": 0.12,
            "measured": cand["annualized_volatility"],
            "provenance": [
                "artifacts/quant/open_macro_v03_compression_grid_001/grid_results.json"],
            "verdict": "go" if cand["annualized_volatility"] <= 0.12 else "no_go",
        },
        "stress": {
            "applied_rule": ("consumable_position_coverage == 1.0 AND realized risk within "
                             "base profile (worst_5d >= -0.10, window_MDD <= 0.25); carry "
                             "semantics per phase0q_003 amendment; measured on the "
                             "compressed_50 sleeve"),
            "windows": stress_windows,
            "provenance": [
                "artifacts/quant/open_macro_v03_compression_grid_001/grid_results.json",
                "artifacts/quant/open_macro_v03_phase0q_003/stress_gate_semantics_amendment.json",
            ],
            "verdict": "go" if all(w["go"] for w in stress_windows.values()) else "no_go",
        },
        "out_of_sample": {
            "applied_rule": ("stress-overlap jackknife max-dev-from-median (phase0q_004 "
                             "amendment); bounds UNCHANGED (0.08 MDD dev / 0.05 sigma dev; "
                             "absolutes 0.25 / 0.12 on eligible folds)"),
            "measured": oos,
            "provenance": [
                "artifacts/quant/open_macro_v03_phase0q_004/oos_dispersion_semantics_amendment.json",
                "artifacts/quant/open_macro_v03_compression_grid_001/oos_fold_report.json",
            ],
            "verdict": oos["verdict"],
        },
    }
    hard_gates_ok = all(gates[g]["verdict"] in ("go", "go_candidate",
                                                "pass_candidate_under_reference_sleeve_policy")
                        for g in gates)
    return {
        "artifact_type": "phase0q_quantitative_gate_judgment",
        "schema_version": 1,
        "phase0q_id": "open_macro_v03_phase0q_004",
        "decision_date": DECISION_DATE,
        "judged_sleeve": CANDIDATE_SLEEVE,
        "judgment_of": [
            "open_macro_v03_compression_grid_001",
            "open_macro_v03_reproducibility_001",
            "open_macro_v03_metric_evidence_001",
        ],
        "execution_legs": {
            "local_python_pure": "complete",
            "qc_research_object_store": "reproduced",
        },
        "base_profile": {
            "max_annualized_volatility": 0.12,
            "max_drawdown": 0.25,
            "max_fold_mdd_deviation": MDD_DEV_BOUND,
            "max_fold_volatility_deviation": SIGMA_DEV_BOUND,
            "max_one_way_turnover_annualized": 0.6,
            "min_worst_5d_return": -0.1,
        },
        "gates": gates,
        "overall_recommendation": "go_candidate" if hard_gates_ok else "no_go",
        "ratified_by": "quant_owner",
        "approved": False,
        "approval_required_from": "quant_owner",
        "status": "candidate_not_approved",
        "supersedes": ("artifacts/quant/open_macro_v03_phase0q_003/"
                       "quantitative_gate_judgment.phase0q_003.json"),
        "supersedes_note": ("a new traceable judgment supersedes phase0q_003 (which judged "
                            "the baseline_100 sleeve with OOS deferred); all superseded "
                            "artifacts are IMMUTABLE and unchanged (hash-pinned)."),
        "task2_gate_effect": ("Task 2 stays BLOCKED. This judgment is the input to the "
                              "quant_owner's threshold sign-off; only that sign-off (a "
                              "separate human act) unblocks Task 2."),
        "governance": dict(GOVERNANCE_PINS),
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fold_report = _load(GRID_DIR / "oos_fold_report.json")
    grid = _load(GRID_DIR / "grid_results.json")
    stress_windows = go_stress_windows()

    amendment = build_amendment(fold_report, stress_windows)
    sleeve_record = build_sleeve_record(grid)
    judgment = build_judgment(grid, amendment)

    _write(OUT_DIR / "oos_dispersion_semantics_amendment.json", amendment)
    _write(OUT_DIR / "sleeve_selection_record.json", sleeve_record)
    _write(OUT_DIR / "quantitative_gate_judgment.phase0q_004.json", judgment)
    for name in ("oos_dispersion_semantics_amendment.json", "sleeve_selection_record.json",
                 "quantitative_gate_judgment.phase0q_004.json"):
        print(f"wrote {OUT_DIR / name}")
    print(f"OOS verdicts: baseline={amendment['re_measured'][REFERENCE_SLEEVE]['verdict']} "
          f"candidate={amendment['re_measured'][CANDIDATE_SLEEVE]['verdict']}")
    print(f"overall: {judgment['overall_recommendation']} (approved=False, A5 blocked)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
