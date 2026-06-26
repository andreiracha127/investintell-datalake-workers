"""Repeatability matrix evaluation for the quant-engine.

Given a group of runs that should be semantically identical (e.g. the same case
run host vs container and with ``jobs=1`` vs ``jobs=4``, repeated N times), this
module compares every run against a baseline using the closed comparator and
aggregates a determinism verdict. It is the pure decision layer; the actual
execution of host/container runs is driven by ``scripts/repeatability_matrix.py``.

The verdict is conservative: an empty baseline is rejected (an empty expected set
must never pass as "0 mismatches"), and ``sufficient`` reflects whether enough
repetitions were collected to treat the evidence as strong.
"""

from __future__ import annotations

from typing import Any

from .comparator import compare_manifests


def compare_run_group(
    runs: list[tuple[str, dict[str, Any]]],
    *,
    min_runs: int = 1,
) -> dict[str, Any]:
    """Compare a group of run manifests against the first (baseline) run.

    ``runs`` is an ordered list of ``(label, outputs_manifest)``. The first entry
    is the baseline. Returns an aggregated report with per-run diffs, the set of
    divergent labels, the total ``mismatch_count``, and the overall ``ok`` flag.
    """
    if not runs:
        return {
            "ok": False,
            "reason": "no_runs",
            "run_count": 0,
            "mismatch_count": 0,
            "sufficient": False,
            "divergent": [],
            "comparisons": {},
        }

    baseline_label, baseline = runs[0]
    baseline_artifacts = baseline.get("artifacts", [])
    run_count = len(runs)
    sufficient = run_count >= min_runs

    if not baseline_artifacts:
        return {
            "ok": False,
            "reason": "empty_baseline",
            "baseline": baseline_label,
            "run_count": run_count,
            "mismatch_count": 0,
            "sufficient": sufficient,
            "divergent": [],
            "comparisons": {},
        }

    comparisons: dict[str, dict[str, Any]] = {}
    divergent: list[str] = []
    for label, manifest in runs[1:]:
        diff = compare_manifests(baseline, manifest)
        comparisons[label] = diff
        if not diff["ok"]:
            divergent.append(label)

    mismatch_count = len(divergent)
    return {
        "ok": mismatch_count == 0,
        "reason": "ok" if mismatch_count == 0 else "divergence",
        "baseline": baseline_label,
        "run_count": run_count,
        "mismatch_count": mismatch_count,
        "sufficient": sufficient,
        "divergent": sorted(divergent),
        "comparisons": comparisons,
    }
