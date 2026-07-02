"""Validate a fetched cloud verdict + complete the consolidated reproducibility report.

AFTER the orchestrator has (separately, in the reviewed main session) run the QC
Research notebook and fetched the emitted verdict JSON via
``lean cloud object-store get <verdict_key>``, this CLI:

  1. loads that verdict JSON + the bundle's ``expected_results_manifest.json``,
  2. validates the cloud-leg hashes against the local-leg expected hashes
     (exact string match for logical hashes; 1e-12 tolerance for any floats),
  3. writes a completed ``consolidated_reproducibility_report.json`` with both legs'
     hashes filled and a match verdict.

ZERO network calls. ZERO ``lean`` invocations. It reads a verdict file the
orchestrator already fetched; it never fetches anything itself. It NEVER flips any
governance flag: A5 stays blocked; runtime_activation / activation_allowed /
allocator_publish / official_result stay false; a matching verdict is reproducibility
evidence only and grants no activation or approval.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

from .bundle import GOVERNANCE_PINS, canonical_json_bytes

FLOAT_TOLERANCE = 1e-12


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _floats_equal(a: Any, b: Any) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=FLOAT_TOLERANCE)
    return a == b


def compare_leg_hashes(expected: dict[str, Any], verdict: dict[str, Any]) -> dict[str, Any]:
    """Compare the cloud verdict's recomputed hashes to the local-leg expected hashes.

    Logical hashes must match EXACTLY (string equality); any floats present are
    compared at 1e-12. Returns a structured comparison with per-key results.
    """
    exp_hashes = expected["output_logical_hashes"]
    got_hashes = verdict.get("output_logical_hashes", {})
    per_hash: dict[str, Any] = {}
    for key in sorted(exp_hashes):
        exp_v = exp_hashes[key]
        got_v = got_hashes.get(key)
        per_hash[key] = {
            "expected": exp_v,
            "actual": got_v,
            "match": exp_v == got_v,
        }

    exp_fingerprint = expected["run_fingerprint"]
    got_fingerprint = verdict.get("run_fingerprint")
    exp_local = expected["execution_legs"]["local_python_pure"]["logical_hash"]
    got_cloud = verdict.get("execution_legs", {}).get("qc_research_object_store", {}).get(
        "logical_hash", verdict.get("cloud_python_logical_hash"))

    mismatches = [k for k, v in per_hash.items() if not v["match"]]
    fingerprint_match = exp_fingerprint == got_fingerprint
    leg_match = exp_local == got_cloud
    all_match = not mismatches and fingerprint_match and leg_match

    return {
        "output_logical_hashes": per_hash,
        "run_fingerprint": {
            "expected": exp_fingerprint, "actual": got_fingerprint, "match": fingerprint_match,
        },
        "execution_leg_logical_hash": {
            "expected_local_python_pure": exp_local,
            "actual_qc_research_object_store": got_cloud,
            "match": leg_match,
        },
        "float_tolerance": FLOAT_TOLERANCE,
        "mismatch_count": len(mismatches) + (0 if fingerprint_match else 1) + (0 if leg_match else 1),
        "all_hashes_match": all_match,
    }


def build_consolidated_report(
    expected: dict[str, Any], verdict: dict[str, Any], comparison: dict[str, Any],
) -> dict[str, Any]:
    reproduced = comparison["all_hashes_match"]
    return {
        "artifact_type": "phase0q_cloud_consolidated_reproducibility_report",
        "schema_version": 1,
        "reproducibility_matrix": {
            "legs": ["local_python_pure", "qc_research_object_store"],
            "local_python_pure": {
                "status": "complete",
                "logical_hash": expected["execution_legs"]["local_python_pure"]["logical_hash"],
                "source": "artifacts/quant/open_macro_v03_metric_evidence_001",
            },
            "qc_research_object_store": {
                "status": "reproduced" if reproduced else "mismatch",
                "logical_hash": comparison["execution_leg_logical_hash"][
                    "actual_qc_research_object_store"],
                "source": "qc_research_notebook_verdict",
            },
        },
        "run_fingerprint": expected["run_fingerprint"],
        "comparison": comparison,
        "reproduced": reproduced,
        "verdict": "reproduced" if reproduced else "not_reproduced",
        "qc_project_id": expected.get("qc_project_id"),
        "harness_commit": expected.get("harness_commit"),
        "input_pack_sha256": expected.get("input_pack_sha256"),
        "contract_bundle_sha256": expected.get("contract_bundle_sha256"),
        "governance": dict(GOVERNANCE_PINS),
        "notes": (
            "Reproducibility evidence only. A matching cloud leg confirms the "
            "local_python_pure result is reproducible in qc_research_object_store; it "
            "grants NO activation and NO approval. A5 stays blocked; runtime_activation "
            "/ activation_allowed / allocator_publish / official_result stay false; "
            "db_write_mode is none; status is candidate_not_approved."
        ),
    }


def complete_report(
    verdict_path: str | Path,
    expected_manifest_path: str | Path,
    out_path: str | Path,
) -> dict[str, Any]:
    """Validate the fetched verdict and write the consolidated report. No network."""
    verdict = _read_json(Path(verdict_path))
    expected = _read_json(Path(expected_manifest_path))
    comparison = compare_leg_hashes(expected, verdict)
    report = build_consolidated_report(expected, verdict, comparison)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("wb") as handle:
        handle.write(canonical_json_bytes(report))

    return {
        "status": "completed",
        "reproduced": report["reproduced"],
        "verdict": report["verdict"],
        "mismatch_count": comparison["mismatch_count"],
        "consolidated_report": str(out_path),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m harness.phase0q_cloud.fetch_results",
        description="Validate a fetched cloud verdict and complete the consolidated report (no network).",
    )
    parser.add_argument("--verdict", required=True,
                        help="Path to the cloud verdict JSON already fetched via `lean cloud object-store get`.")
    parser.add_argument("--expected-manifest", required=True,
                        help="Path to the bundle's expected_results_manifest.json.")
    parser.add_argument("--out", required=True,
                        help="Output path for consolidated_reproducibility_report.json.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    summary = complete_report(args.verdict, args.expected_manifest, args.out)
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0 if summary["reproduced"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
