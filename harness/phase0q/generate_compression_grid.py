"""Deterministic generator for the compression_grid_001 deliverables.

Usage (from repo root, Windows PYTHONPATH):
    python -m harness.phase0q.generate_compression_grid <harness_commit>

Writes (LF-terminated, canonical) into
``artifacts/quant/open_macro_v03_compression_grid_001/``:
  * compression_grid_manifest.json
  * grid_results.json
  * oos_fold_report.json
  * compression_tradeoff_summary.md

Measurement only (A5 blocked, activation/approval false, replaces_baseline false).
The ``harness_commit`` argument pins the REAL code commit that produced the numbers
(two-step: commit code, regenerate with that SHA, commit evidence). Deterministic:
no wall-clock, no RNG; timestamps are constants inside the payload builders.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Mapping

from . import grid, runner, sleeve

OUT_DIR = Path("artifacts/quant/open_macro_v03_compression_grid_001")
PARAMS = sleeve.SleeveParams("baseline_current", 0.5, 0.5, 0.0, 0.0, 0.0)

_ORDER = ["baseline_100", "compressed_75", "compressed_50", "compressed_25"]


def measure_all(pack_dir: str | Path) -> dict[str, Any]:
    """Run the full compression grid + per-variant OOS fold report once."""
    pack = runner.load_and_verify_pack(pack_dir)
    prices = sleeve.PriceFrame(pack.eod_rows)
    all_windows = [runner.PRIMARY_WINDOW]
    all_windows += [(w["start"], w["end"]) for w in runner.STRESS_WINDOWS]
    decisions = runner.build_decision_series(pack, all_windows)
    grid_results = grid.measure_grid_results(prices, decisions, PARAMS)
    oos = {vid: grid.measure_oos_fold_report(prices, decisions, PARAMS, vid)
           for vid in grid.VARIANT_FACTORS}
    return {"grid": grid_results, "oos": oos}


def _fmt(x: float, nd: int = 4) -> str:
    return f"{x:.{nd}f}"


def _pct(x: float, nd: int = 2) -> str:
    return f"{x * 100:.{nd}f}%"


def build_tradeoff_summary_md(
    grid_results: Mapping[str, Any], oos: Mapping[str, Any],
) -> str:
    """Human-readable frontier table + dominance + monotonicity + measurement-only
    statement (compression_tradeoff_summary.md)."""
    lines: list[str] = []
    lines.append("# open_macro_v03 compression mini-grid — trade-off summary")
    lines.append("")
    lines.append("**MEASUREMENT ONLY.** This package measures four reference-sleeve "
                 "compression variants to inform the quant_owner's sleeve and OOS "
                 "bounds decisions. Nothing here replaces the baseline "
                 "(`replaces_baseline` is false everywhere), A5 stays **blocked**, "
                 "`runtime_activation`/`activation_allowed`/`allocator_publish`/"
                 "`official_result` are all false, `db_write_mode` is `none`, and no "
                 "gate verdict grants activation or approval. Status: "
                 "`candidate_not_approved`.")
    lines.append("")
    lines.append("## Naming convention")
    lines.append("")
    lines.append("`compressed_N` = **N% of the original inter-quadrant distance "
                 "RETAINED**; the compression factor (fraction of the distance each "
                 "quadrant weight vector is moved TOWARD the four-quadrant mean) is "
                 "`1 - N/100`. `baseline_100` = factor 0.0 (no compression). "
                 "`compressed_50` is numerically identical to `sleeve_compressed_50` "
                 "in evidence_002 (verified as a consistency test); `compressed_25` "
                 "(75% moved toward the mean) is the most compressed of the grid.")
    lines.append("")
    lines.append("## Frontier at 5 bps (primary window 2014-03..2026-06)")
    lines.append("")
    lines.append("| variant | factor | ann. turnover | ann. vol | max DD | window return |")
    lines.append("|---|---|---|---|---|---|")
    for vid in _ORDER:
        c = grid_results[vid]["cost_grid"]["by_cost_bps"]["5"]
        f = grid.VARIANT_FACTORS[vid]
        lines.append(f"| {vid} | {f:.2f} | {_fmt(c['annualized_turnover'])} | "
                     f"{_fmt(c['annualized_volatility'])} | {_fmt(c['max_drawdown'])} | "
                     f"{_pct(c['window_return'])} |")
    lines.append("")
    lines.append("## Cost sensitivity (annualized turnover is cost-invariant; return net of cost)")
    lines.append("")
    lines.append("| variant | ret 0bps | ret 5bps | ret 10bps | ret 25bps | MDD 0bps | MDD 25bps |")
    lines.append("|---|---|---|---|---|---|---|")
    for vid in _ORDER:
        cg = grid_results[vid]["cost_grid"]["by_cost_bps"]
        lines.append(f"| {vid} | {_pct(cg['0']['window_return'])} | "
                     f"{_pct(cg['5']['window_return'])} | {_pct(cg['10']['window_return'])} | "
                     f"{_pct(cg['25']['window_return'])} | {_fmt(cg['0']['max_drawdown'])} | "
                     f"{_fmt(cg['25']['max_drawdown'])} |")
    lines.append("")
    lines.append("## Monotonicity observations")
    lines.append("")
    base = grid_results["baseline_100"]["cost_grid"]["by_cost_bps"]["5"]
    c50 = grid_results["compressed_50"]["cost_grid"]["by_cost_bps"]["5"]
    c25 = grid_results["compressed_25"]["cost_grid"]["by_cost_bps"]["5"]
    lines.append(f"- **Turnover** decreases monotonically with compression: "
                 f"{_fmt(base['annualized_turnover'])} (baseline_100) -> "
                 f"{_fmt(c50['annualized_turnover'])} (compressed_50) -> "
                 f"{_fmt(c25['annualized_turnover'])} (compressed_25).")
    lines.append(f"- **Volatility** decreases monotonically: "
                 f"{_fmt(base['annualized_volatility'])} -> "
                 f"{_fmt(c50['annualized_volatility'])} -> "
                 f"{_fmt(c25['annualized_volatility'])}.")
    lines.append(f"- **Window return** increases monotonically: "
                 f"{_pct(base['window_return'])} -> {_pct(c50['window_return'])} -> "
                 f"{_pct(c25['window_return'])}.")
    lines.append(f"- **Max drawdown** rises slightly with compression "
                 f"({_fmt(base['max_drawdown'])} -> {_fmt(c50['max_drawdown'])} -> "
                 f"{_fmt(c25['max_drawdown'])}), the only metric that worsens; it "
                 f"stays well within the 0.25 base bound for every variant.")
    lines.append("")
    lines.append("## Does compressed_50 still (near-)dominate?")
    lines.append("")
    lines.append("`compressed_50` improves on the baseline across turnover, volatility "
                 "and return with an essentially unchanged drawdown, so it remains a "
                 "strong leading alternative candidate. It is NOT a strict Pareto "
                 "dominator of the whole grid: `compressed_25` posts lower turnover / "
                 "vol and higher return still, at the cost of a higher (but "
                 "in-bounds) drawdown. `compressed_50` is the balanced point that "
                 "keeps drawdown nearest the baseline while capturing most of the "
                 "turnover/vol/return gains — hence its `leading_alternative_candidate` "
                 "flag. The final sleeve choice is the quant_owner's.")
    lines.append("")
    lines.append("## OOS cross-fold dispersion (9 folds, 5 bps) — CRITICAL for the bounds decision")
    lines.append("")
    lines.append("| variant | MDD max-dev (bound 0.08) | vol max-dev (bound 0.05) | verdict |")
    lines.append("|---|---|---|---|")
    for vid in _ORDER:
        disp = oos[vid]["cross_fold_dispersion"]
        lines.append(f"| {vid} | {_fmt(disp['MDD_max_dev_from_median'], 5)} | "
                     f"{_fmt(disp['sigma_annual_max_dev_from_median'], 5)} | "
                     f"no_go_bounds_under_review |")
    lines.append("")
    lines.append("**Key finding:** compression reduces cross-fold **volatility** "
                 "dispersion monotonically but NOT the **MDD** dispersion, and "
                 "NEITHER clears its current bound at any compression level. The "
                 "cross-fold MDD max-deviation stays around 0.10-0.11 (> 0.08 bound) "
                 "for all four variants, and the volatility max-deviation stays "
                 "~0.056-0.071 (> 0.05 bound). Compression therefore does **not** by "
                 "itself bring OOS dispersion inside the current bounds. The OOS "
                 "verdict label stays `no_go_bounds_under_review` for every variant; "
                 "the bounds are UNTOUCHED and the decision is deferred to the "
                 "quant_owner.")
    lines.append("")
    lines.append("## Constraints")
    lines.append("")
    lines.append("Every variant satisfies the constraints for all four quadrant "
                 "targets: weights sum to 1, risk assets <= risk_cap (0.65), "
                 "defensive assets >= defensive_floor (0.20). Compression moves the "
                 "quadrant vectors toward their common centroid, which lowers risk "
                 "weight and raises defensive weight, so no renormalization "
                 "enforcement was triggered for any variant "
                 "(`any_renormalization_applied` is false throughout).")
    lines.append("")
    return "\n".join(lines) + "\n"


def generate(pack_dir: str | Path, harness_commit: str, out_dir: Path = OUT_DIR) -> list[Path]:
    measured = measure_all(pack_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    manifest = grid.build_compression_grid_manifest(harness_commit)
    p = out_dir / "compression_grid_manifest.json"
    p.write_text(runner.canonical_json(manifest), encoding="utf-8", newline="")
    written.append(p)

    results = grid.build_grid_results_payload(measured["grid"], harness_commit)
    p = out_dir / "grid_results.json"
    p.write_text(runner.canonical_json(results), encoding="utf-8", newline="")
    written.append(p)

    oos_payload = grid.build_oos_fold_report_payload(measured["oos"], harness_commit)
    p = out_dir / "oos_fold_report.json"
    p.write_text(runner.canonical_json(oos_payload), encoding="utf-8", newline="")
    written.append(p)

    md = build_tradeoff_summary_md(measured["grid"], measured["oos"])
    p = out_dir / "compression_tradeoff_summary.md"
    p.write_text(md, encoding="utf-8", newline="")
    written.append(p)

    return written


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python -m harness.phase0q.generate_compression_grid <harness_commit>",
              file=sys.stderr)
        return 2
    harness_commit = argv[1]
    pack_dir = Path("fixtures/p1_packs/open_macro_v03_certified_input_pack_002")
    written = generate(pack_dir, harness_commit)
    for path in written:
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
