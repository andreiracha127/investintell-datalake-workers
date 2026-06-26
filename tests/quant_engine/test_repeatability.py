from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))
sys.path.insert(0, str(ROOT))

from investintell_quant_engine.repeatability import compare_run_group


def _manifest(artifacts, status="succeeded"):
    return {"status": status, "artifacts": [dict(a) for a in artifacts]}


def _a(path, sha):
    return {"path": path, "sha256": sha, "bytes": 1}


def test_identical_runs_pass_with_zero_mismatches():
    base = _manifest([_a("metrics.json", "x"), _a("runtime.json", "y")])
    group = [("host-jobs1", base), ("host-jobs4", base), ("container-jobs1", base)]
    report = compare_run_group(group)
    assert report["ok"] is True
    assert report["mismatch_count"] == 0
    assert report["run_count"] == 3
    assert report["baseline"] == "host-jobs1"


def test_divergent_run_is_caught():
    base = _manifest([_a("metrics.json", "x")])
    bad = _manifest([_a("metrics.json", "DIFFERENT")])
    report = compare_run_group([("a", base), ("b", base), ("c", bad)])
    assert report["ok"] is False
    assert report["mismatch_count"] == 1
    assert "c" in report["divergent"]
    assert "metrics.json" in report["comparisons"]["c"]["mismatched"]


def test_missing_artifact_in_one_run_is_caught():
    base = _manifest([_a("metrics.json", "x"), _a("runtime.json", "y")])
    short = _manifest([_a("metrics.json", "x")])
    report = compare_run_group([("a", base), ("b", short)])
    assert report["ok"] is False
    assert "b" in report["divergent"]


def test_empty_baseline_is_rejected():
    empty = _manifest([])
    report = compare_run_group([("a", empty), ("b", empty)])
    assert report["ok"] is False
    assert report["reason"] == "empty_baseline"


def test_insufficient_repetitions_flagged():
    base = _manifest([_a("metrics.json", "x")])
    report = compare_run_group([("a", base)], min_runs=10)
    assert report["sufficient"] is False
    # a single run cannot diverge, but it is not enough evidence
    assert report["run_count"] == 1
