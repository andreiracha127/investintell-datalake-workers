"""Adjudicate the out_of_sample gate under the phase0q_004 stress-overlap jackknife.

The runner's RAW OOS gate judges max-dev-from-median over ALL walk-forward folds.
phase0q_004 (ratified 2026-07-03, applied to the frozen v1 candidate) amended the
semantics: when the ARGMAX-deviation fold overlaps a named stress window, it is
excluded (jackknife) and the stability metric is re-judged on the eligible folds,
with the absolute bounds (MDD <= 0.25, vol <= 0.12) checked on the eligible set;
the excluded fold is surfaced as a diagnostic, never silently dropped.

This script applies EXACTLY that rule to a measured evidence directory and writes
``oos_jackknife_adjudication.json`` next to it. Bounds are the ratified base
profile; nothing here is tunable.

Usage:
    python -m scripts.adjudicate_oos_jackknife --evidence artifacts/quant/<dir>
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

STRESS_WINDOWS = {
    "COVID_2020": ("2020-02-15", "2020-04-30"),
    "INFLATION_SHOCK_2022": ("2022-01-01", "2022-10-31"),
    "SVB_2023": ("2023-03-01", "2023-05-31"),
    "Q4_2018": ("2018-10-01", "2018-12-31"),
    "GFC_2008": ("2007-10-01", "2009-03-31"),
    "TAPER_2013": ("2013-05-01", "2013-09-30"),
}
MDD_DEV_BOUND = 0.08
VOL_DEV_BOUND = 0.05
MDD_ABS_BOUND = 0.25
VOL_ABS_BOUND = 0.12


def _overlaps(fold) -> list[str]:
    fs, fe = fold["test_start"], fold["test_end"]
    return [name for name, (s, e) in STRESS_WINDOWS.items() if fs <= e and fe >= s]


def _judge(folds, metric_key, dev_bound):
    values = [f[metric_key] for f in folds]
    med = statistics.median(values)
    devs = [abs(v - med) for v in values]
    argmax = max(range(len(devs)), key=devs.__getitem__)
    all_block = {"median": med, "max_dev_from_median": devs[argmax],
                 "argmax_fold_index": folds[argmax]["fold_index"]}
    excluded = None
    eligible = folds
    if devs[argmax] > dev_bound and _overlaps(folds[argmax]):
        excluded = {"fold_index": folds[argmax]["fold_index"],
                    "stress_overlap": _overlaps(folds[argmax]),
                    metric_key: folds[argmax][metric_key]}
        eligible = [f for i, f in enumerate(folds) if i != argmax]
    e_values = [f[metric_key] for f in eligible]
    e_med = statistics.median(e_values)
    e_dev = max(abs(v - e_med) for v in e_values)
    return {
        "all_folds": all_block,
        "excluded_fold": excluded,
        "eligible_folds": {"count": len(eligible), "median": e_med,
                           "max_dev_from_median": e_dev},
        "dev_bound": dev_bound,
        "pass": e_dev <= dev_bound,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="adjudicate_oos_jackknife")
    ap.add_argument("--evidence", required=True)
    ap.add_argument("--candidate", default="baseline_current")
    ap.add_argument("--cost-bps", default="5")
    args = ap.parse_args(argv)

    evidence = Path(args.evidence)
    cell = json.loads(
        (evidence / "cells" / f"{args.candidate}__{args.cost_bps}bps.json")
        .read_text(encoding="utf-8"))
    folds = cell["out_of_sample"]["folds"]

    mdd = _judge(folds, "MDD", MDD_DEV_BOUND)
    vol = _judge(folds, "sigma_annual", VOL_DEV_BOUND)
    eligible_idx = {f["fold_index"] for f in folds}
    for block in (mdd, vol):
        if block["excluded_fold"]:
            eligible_idx.discard(block["excluded_fold"]["fold_index"])
    eligible = [f for f in folds if f["fold_index"] in eligible_idx]
    absolutes = {
        "eligible_fold_count": len(eligible),
        "max_fold_MDD": max(f["MDD"] for f in eligible),
        "mdd_bound": MDD_ABS_BOUND,
        "mdd_pass": max(f["MDD"] for f in eligible) <= MDD_ABS_BOUND,
        "max_fold_volatility": max(f["sigma_annual"] for f in eligible),
        "vol_bound": VOL_ABS_BOUND,
        "vol_pass": max(f["sigma_annual"] for f in eligible) <= VOL_ABS_BOUND,
    }
    verdict = ("go" if (mdd["pass"] and vol["pass"] and absolutes["mdd_pass"]
                        and absolutes["vol_pass"]) else "no_go")
    out = {
        "artifact_type": "phase0q_oos_jackknife_adjudication",
        "applied_rule": ("stress-overlap jackknife max-dev-from-median "
                         "(phase0q_004 amendment); bounds UNCHANGED"),
        "evidence_dir": str(evidence),
        "candidate_id": args.candidate,
        "cost_bps": int(args.cost_bps),
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "mdd_stability": mdd,
        "volatility_stability": vol,
        "absolute_bounds": absolutes,
        "verdict": verdict,
    }
    path = evidence / "oos_jackknife_adjudication.json"
    path.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
