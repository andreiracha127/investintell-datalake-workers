from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))
sys.path.insert(0, str(ROOT))

from investintell_quant_engine.comparator import compare_manifests


def _manifest(artifacts, status="succeeded"):
    return {"status": status, "artifacts": list(artifacts)}


def _a(path, sha, nbytes=10):
    return {"path": path, "sha256": sha, "bytes": nbytes}


def test_identical_manifests_are_ok():
    m = _manifest([_a("metrics.json", "aaa"), _a("runtime.json", "bbb")])
    result = compare_manifests(m, m)
    assert result["ok"] is True
    assert result["status_match"] is True
    assert result["missing"] == []
    assert result["unexpected"] == []
    assert result["mismatched"] == []
    assert result["expected_count"] == 2
    assert result["actual_count"] == 2


def test_mismatched_sha_is_flagged_and_not_ok():
    expected = _manifest([_a("metrics.json", "aaa")])
    actual = _manifest([_a("metrics.json", "zzz")])
    result = compare_manifests(expected, actual)
    assert result["ok"] is False
    assert result["mismatched"] == ["metrics.json"]
    assert result["missing"] == []
    assert result["unexpected"] == []


def test_missing_artifact_is_flagged_and_not_ok():
    expected = _manifest([_a("metrics.json", "aaa"), _a("runtime.json", "bbb")])
    actual = _manifest([_a("metrics.json", "aaa")])
    result = compare_manifests(expected, actual)
    assert result["ok"] is False
    assert result["missing"] == ["runtime.json"]
    assert result["unexpected"] == []
    assert result["mismatched"] == []


def test_unexpected_artifact_is_flagged_and_not_ok():
    expected = _manifest([_a("metrics.json", "aaa")])
    actual = _manifest([_a("metrics.json", "aaa"), _a("surprise.json", "ccc")])
    result = compare_manifests(expected, actual)
    assert result["ok"] is False
    assert result["unexpected"] == ["surprise.json"]
    assert result["missing"] == []
    assert result["mismatched"] == []


def test_status_divergence_breaks_ok():
    expected = _manifest([_a("metrics.json", "aaa")], status="succeeded")
    actual = _manifest([_a("metrics.json", "aaa")], status="failed")
    result = compare_manifests(expected, actual)
    assert result["status_match"] is False
    assert result["ok"] is False


def test_empty_actual_against_full_expected_is_not_ok():
    # Guards the report's "comparador fechado" rule: a comparator that only
    # iterates the intersection would report 0 mismatches here. It must not.
    expected = _manifest([_a("metrics.json", "aaa"), _a("runtime.json", "bbb")])
    actual = _manifest([])
    result = compare_manifests(expected, actual)
    assert result["ok"] is False
    assert result["missing"] == ["metrics.json", "runtime.json"]
    assert result["actual_count"] == 0


def test_duplicate_paths_are_flagged():
    expected = _manifest([_a("metrics.json", "aaa")])
    actual = _manifest([_a("metrics.json", "aaa"), _a("metrics.json", "aaa")])
    result = compare_manifests(expected, actual)
    assert result["ok"] is False
    assert "metrics.json" in result["duplicate_paths"]


def test_results_are_sorted_for_stable_reporting():
    expected = _manifest([_a("b.json", "1"), _a("a.json", "2"), _a("c.json", "3")])
    actual = _manifest([])
    result = compare_manifests(expected, actual)
    assert result["missing"] == ["a.json", "b.json", "c.json"]
