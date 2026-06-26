"""Closed comparator for quant-engine output manifests.

The report (`docs/architecture/deep-research-report.md`) mandates a *closed*
comparator as an obligatory engineering artifact: it must report
expected/actual/missing/unexpected/mismatched explicitly and must never report
"0 mismatches" for an empty or partial actual manifest. A comparator that only
iterates the intersection of paths would hide a missing-everything failure; this
one iterates the union and is honest about counts.
"""

from __future__ import annotations

from typing import Any


def _index_by_path(artifacts: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Index artifacts by path, recording any duplicated paths."""
    index: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    duplicates: set[str] = set()
    for artifact in artifacts:
        path = artifact["path"]
        if path in seen:
            duplicates.add(path)
        seen.add(path)
        index[path] = artifact
    return index, sorted(duplicates)


def compare_manifests(expected: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    """Compare two output manifests and return a closed diff.

    Each manifest is ``{"status": str, "artifacts": [{"path", "sha256", ...}]}``.
    The returned dict is fully closed: ``ok`` is true only when status matches,
    no path is missing/unexpected/mismatched, and no path is duplicated on
    either side.
    """
    exp_artifacts = list(expected.get("artifacts", []))
    act_artifacts = list(actual.get("artifacts", []))

    exp, exp_dups = _index_by_path(exp_artifacts)
    act, act_dups = _index_by_path(act_artifacts)

    missing = sorted(set(exp) - set(act))
    unexpected = sorted(set(act) - set(exp))
    mismatched = sorted(
        path
        for path in (set(exp) & set(act))
        if exp[path].get("sha256") != act[path].get("sha256")
    )
    duplicate_paths = sorted(set(exp_dups) | set(act_dups))

    status_match = expected.get("status") == actual.get("status")
    ok = (
        status_match
        and not missing
        and not unexpected
        and not mismatched
        and not duplicate_paths
    )

    return {
        "status_match": status_match,
        "expected_status": expected.get("status"),
        "actual_status": actual.get("status"),
        "expected_count": len(exp_artifacts),
        "actual_count": len(act_artifacts),
        "missing": missing,
        "unexpected": unexpected,
        "mismatched": mismatched,
        "duplicate_paths": duplicate_paths,
        "ok": ok,
    }
