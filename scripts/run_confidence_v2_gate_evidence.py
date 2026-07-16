"""Run the full phase0q harness with the confidence-v2 decision model and write the
measured gate evidence (metric_evidence classification — approves nothing by itself).

This is the OFFICIAL judge path (runner.run_harness -> build_gate_report with the
ratified phase0q_006 timeline policy GATING), pointed at the certified pack _003
with RunConfig.decision_model="v2". Every gate is judged: the five metric gates
(turnover/drawdown/volatility/stress/OOS) at each cost level, plus the blocking
timeline gate (fresh-valid 36m, abstention streak, carry age, amended
same-quadrant-run) and the bull-year upside-capture gate.

Usage (from the repo root, with the repo venv):

    python -m scripts.run_confidence_v2_gate_evidence
    python -m scripts.run_confidence_v2_gate_evidence --out artifacts/quant/...

Timestamps/run_id are provenance strings injected from the CLI (defaults derive
from the current commit + UTC now); canonical payload hashes never include them.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import subprocess
import sys
from pathlib import Path

from harness.phase0q import runner

ROOT = Path(__file__).resolve().parents[1]
PACK = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_003"
DEFAULT_OUT = ROOT / "artifacts" / "quant" / "open_macro_v03_confidence_v2_evidence_001"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="run_confidence_v2_gate_evidence")
    ap.add_argument("--pack", default=str(PACK))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--decision-model", default="v2", choices=("v1", "v2", "v3"))
    args = ap.parse_args(argv)

    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True,
        check=True).stdout.strip()
    now = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
    run_id = args.run_id or (
        f"phase0q-{args.decision_model}-evidence-{now:%Y%m%d%H%M%S}")

    config = runner.RunConfig(
        run_id=run_id,
        started_at=now.isoformat(),
        finished_at=now.isoformat(),
        harness_commit=commit,
        decision_model=args.decision_model,
    )
    run = runner.run_harness(args.pack, config)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    paths = runner.write_evidence(out, run)

    report = run["gate_report"]
    timeline = report["timeline"]
    judgment = timeline["gate_judgment"]

    # Windowed timeline addenda. The harness's blocking judgment above runs over the
    # FULL union chain (2007-10.. — the pack cannot cover 2007-2013: coverage < 0.80
    # makes BOTH v1 and v2 'unavailable' there, so the chain-head abstention streak
    # measures the PACK's data horizon, not the model). The precedent v1 judgments
    # (phase0q_004/_005 and the pack _003 verdict) were measured on the certified
    # 2021-2026 window; the production runtime chain starts at live_validation
    # CHAIN_START (2014-03-01). Both scopes are recorded so the reviewer sees the
    # model's behaviour on the chain it will actually run.
    from harness.phase0q import metrics as _metrics
    decisions = run["decisions"]
    windows = {
        "full_union_chain": None,
        "runtime_chain_2014_03": _dt.date(2014, 3, 1),
        "precedent_2021_01": _dt.date(2021, 1, 1),
    }
    windowed = {}
    for name, start in windows.items():
        rows = decisions if start is None else [r for r in decisions if r.as_of >= start]
        tl = _metrics.regime_timeline_metrics(rows)
        windowed[name] = {k: tl[k] for k in (
            "n_valid", "n_months", "max_abstention_streak_months",
            "max_carry_age_months", "max_same_quadrant_run_months",
            "max_low_density_same_quadrant_run_months")}
        windowed[name]["fresh_valid_rate_36m"] = tl["fresh_valid_rate"]["rolling_36m"]
    summary = {
        "run_id": run_id,
        "harness_commit": commit,
        "decision_model": args.decision_model,
        "input_pack_sha256": run["input_pack_sha256"],
        "policy": judgment.get("phase0q_id"),
        "gates_overall_base_cost": {
            k: (v.get("go_no_go") if isinstance(v, dict) else v)
            for k, v in report["gates_overall_base_cost"].items()},
        "timeline_per_gate": {
            k: {"measured": v.get("measured"), "bound": v.get("bound"),
                "go": v.get("go")}
            for k, v in judgment["per_gate"].items()},
        "regime_timeline_metrics": {
            k: timeline["regime_timeline_metrics"][k]
            for k in ("n_valid", "n_months", "fresh_valid_rate",
                      "max_abstention_streak_months", "max_carry_age_months",
                      "max_same_quadrant_run_months",
                      "max_low_density_same_quadrant_run_months", "quadrant_mix")},
        "windowed_timeline_addenda": windowed,
        "upside_capture_by_calendar_year": {
            y: {"spy_return": e.get("spy_return"),
                "upside_capture": e.get("upside_capture"),
                "full_year_coverage": e.get("full_year_coverage")}
            for y, e in timeline["upside_capture_by_calendar_year"].items()},
        "evidence_files": [str(p) for p in paths],
    }
    (out / "evidence_run_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    overall = report["gates_overall_base_cost"]
    all_go = bool(overall) and all(
        v.get("go_no_go") == "go" for v in overall.values())
    print(f"\nOVERALL at base cost: {'GO' if all_go else 'NO_GO'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
