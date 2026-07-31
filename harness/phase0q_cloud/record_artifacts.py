"""Derive the COMMITTED cloud-leg record artifacts from a built bundle (build-only).

``artifacts/quant/open_macro_v03_cloud_leg_001/cloud_leg_manifest.json`` and
``consolidated_reproducibility_report.json`` were originally assembled in-session from
the 2026-07-02 bundle build. This module CODIFIES that derivation — the repo's
committed-record culture (``module_pins.json`` has ``build_stage_b_artifacts``;
``schema_migration_record`` documents its method): committed records are produced by a
checked-in generator from the sanctioned build flow, NEVER hand-edited. Whenever the
shipped closure legitimately evolves (e.g. the phase0q_005 ratification made the
timeline gate policy part of the runtime read surface and changed
``harness/phase0q/runner.py`` / ``metrics.py``), the regeneration is:

    python -m harness.phase0q_cloud.bundle <harness_commit> --bundle-dir build/phase0q_cloud_bundle
    python -m harness.phase0q_cloud.record_artifacts --bundle-dir build/phase0q_cloud_bundle

Pure construction: reads an existing bundle directory, writes the two records with the
canonical JSON writer. ZERO network calls, ZERO ``lean`` invocations, ZERO uploads.
Governance stays pinned (A5 blocked, nothing activates); the records keep status
``prepared_pending_upload`` / ``pending_cloud_leg`` — the actual upload + QC run remain
reviewed orchestrator steps, and the CLOSED historical run stays byte-frozen in
``artifacts/quant/open_macro_v03_reproducibility_001`` (cross-checked against the
cloud-leg manifest git blob AS OF that run, never the mutable working tree).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .bundle import REPO_ROOT, write_json

CLOUD_LEG_ID = "open_macro_v03_cloud_leg_001"
ARTIFACT_DIR = REPO_ROOT / "artifacts" / "quant" / CLOUD_LEG_ID

_MANIFEST_NOTES = (
    "PREPARED, PENDING UPLOAD. Build-only: zero network calls, zero uploads, zero "
    "lean invocations were performed. The orchestrator runs the reviewed bundle "
    "build, upload (manifest last), lean cloud push, and post-run fetch/verify in "
    "the main session. No verdict grants activation: A5 stays blocked; "
    "runtime_activation / activation_allowed / allocator_publish / official_result "
    "are false; db_write_mode is none; status is candidate_not_approved; approved "
    "is false."
)

_REPORT_NOTES = (
    "SKELETON — cloud side null/pending. Local hashes are filled from the "
    "committed immutable evidence + the live HEAD harness. The cloud side is "
    "completed by harness.phase0q_cloud.fetch_results after the orchestrator "
    "fetches the QC verdict JSON. Reproducibility evidence only; grants no "
    "activation or approval."
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _local_leg(expected: dict[str, Any]) -> dict[str, Any]:
    return expected["execution_legs"]["local_python_pure"]


def build_cloud_leg_manifest_record(bundle_manifest: dict[str, Any],
                                    expected: dict[str, Any]) -> dict[str, Any]:
    """The committed ``cloud_leg_manifest.json`` payload, derived field-for-field from
    the bundle's ``object_store_manifest.json`` + ``expected_results_manifest.json``.

    ``object_key_sha_table`` is the per-object projection (content_sha256 /
    file_size_bytes / object_store_key) of every uploadable bundle object — the pin
    table ``tests/test_phase0q_cloud_leg.py`` holds against a fresh HEAD build."""
    table = {
        rel: {
            "content_sha256": item["content_sha256"],
            "file_size_bytes": item["file_size_bytes"],
            "object_store_key": item["object_store_key"],
        }
        for rel, item in bundle_manifest["object_files"].items()
    }
    return {
        "artifact_type": "phase0q_cloud_leg_manifest",
        "schema_version": 1,
        "cloud_leg_id": CLOUD_LEG_ID,
        "status": "prepared_pending_upload",
        "build": {
            "builder": "harness.phase0q_cloud.bundle",
            "bundle_dir": "build/phase0q_cloud_bundle (LOCAL, not committed)",
            "deterministic_byte_identical_rebuild": True,
            "drift_refusal": "bundle build fails if any shipped source differs from git HEAD blob",
            "lean_invocations": 0,
            "network_calls": 0,
        },
        "bundle_size_bytes": bundle_manifest["bundle_size_bytes"],
        "file_count": bundle_manifest["file_count"],
        "contract_bundle_sha256": bundle_manifest["contract_bundle_sha256"],
        "harness_commit": bundle_manifest["harness_commit"],
        # The SHALLOW-SAFE byte pin. `harness_commit` names the round; these blob
        # ids prove the shipped bytes, and unlike a branch commit they survive the
        # squash-merge that deletes it. `verify_shipped_source_tree_hashes` checks
        # them against HEAD.
        "shipped_source_tree_hashes": dict(
            sorted(bundle_manifest["shipped_source_tree_hashes"].items())
        ),
        "input_pack_id": bundle_manifest["input_pack_id"],
        "input_pack_sha256": bundle_manifest["input_pack_sha256"],
        "governance": dict(bundle_manifest["governance"]),
        "notes": _MANIFEST_NOTES,
        "object_key_sha_table": table,
        "object_store_base_prefix": bundle_manifest["object_store_base_prefix"],
        "object_store_manifest_key": bundle_manifest["object_store_manifest_key"],
        "object_store_prefix_immutable": bundle_manifest["object_store_prefix_immutable"],
        "qc_project_id": bundle_manifest["qc_project_id"],
        "qc_project_name": bundle_manifest["qc_project_name"],
        "qc_project_workspace": "harness/phase0q_cloud/qc_project",
        "ratified_requirements": {
            "canonical_deterministic_writers": True,
            "contract_v2_shaped_results": True,
            "float_decimals": 12,
            "object_store_drift_refusal": "content_sha256 mismatch aborts",
            "rng": "none",
            "stable_hash_canonical_json": "investintell_quant_core.hashing.canonical.stable_hash",
        },
        "reproducibility_matrix": {
            "legs": ["local_python_pure", "qc_research_object_store"],
            "local_python_pure": {
                "logical_hash": _local_leg(expected)["logical_hash"],
                "run_fingerprint": expected["run_fingerprint"],
                "source": "artifacts/quant/open_macro_v03_metric_evidence_001",
                "status": "complete",
            },
            "qc_research_object_store": {
                "logical_hash": None,
                "status": "pending_upload",
            },
        },
        "upload_plan_reference": {
            "emitter": "harness.phase0q_cloud.upload_plan",
            "json": "build/phase0q_cloud_bundle/upload_plan.json",
            "manifest_uploaded_last": True,
            "script": "build/phase0q_cloud_bundle/upload_plan.sh",
        },
        "verdict_key_template": bundle_manifest["verdict_key_template"],
    }


def build_pending_consolidated_report(bundle_manifest: dict[str, Any],
                                      expected: dict[str, Any]) -> dict[str, Any]:
    """The committed PENDING ``consolidated_reproducibility_report.json`` payload
    (local leg filled from the live HEAD expected manifest; cloud side null/pending —
    completed later by ``fetch_results`` from a fetched QC verdict)."""
    return {
        "artifact_type": "phase0q_cloud_consolidated_reproducibility_report",
        "schema_version": 1,
        "cloud_leg_id": CLOUD_LEG_ID,
        "status": "pending_cloud_leg",
        "verdict": "pending",
        "reproduced": None,
        "comparison": None,
        "committed_evidence_001_anchor": expected["committed_evidence_001_anchor"],
        "contract_bundle_sha256": bundle_manifest["contract_bundle_sha256"],
        "harness_commit": bundle_manifest["harness_commit"],
        "input_pack_sha256": bundle_manifest["input_pack_sha256"],
        "qc_project_id": bundle_manifest["qc_project_id"],
        "run_fingerprint": expected["run_fingerprint"],
        "governance": dict(bundle_manifest["governance"]),
        "notes": _REPORT_NOTES,
        "reproducibility_matrix": {
            "legs": ["local_python_pure", "qc_research_object_store"],
            "local_python_pure": {
                "logical_hash": _local_leg(expected)["logical_hash"],
                "output_logical_hashes": expected["output_logical_hashes"],
                "source": "artifacts/quant/open_macro_v03_metric_evidence_001",
                "status": "complete",
            },
            "qc_research_object_store": {
                "logical_hash": None,
                "output_logical_hashes": None,
                "source": "qc_research_notebook_verdict (pending)",
                "status": "pending",
            },
        },
    }


def write_records(bundle_dir: str | Path, out_dir: str | Path = ARTIFACT_DIR) -> dict[str, Any]:
    """Derive + write both committed records from ``bundle_dir``. Returns a summary."""
    bundle_dir = Path(bundle_dir)
    manifest = _read_json(bundle_dir / "object_store_manifest.json")
    expected = _read_json(bundle_dir / "expected_results_manifest.json")
    out_dir = Path(out_dir)
    record = build_cloud_leg_manifest_record(manifest, expected)
    report = build_pending_consolidated_report(manifest, expected)
    write_json(out_dir / "cloud_leg_manifest.json", record)
    write_json(out_dir / "consolidated_reproducibility_report.json", report)
    return {
        "status": "records_written",
        "out_dir": str(out_dir),
        "object_count": len(record["object_key_sha_table"]),
        "local_python_pure_logical_hash":
            record["reproducibility_matrix"]["local_python_pure"]["logical_hash"],
        "executed": False,
        "network_calls": 0,
        "lean_invocations": 0,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m harness.phase0q_cloud.record_artifacts",
        description="Derive the committed cloud-leg record artifacts from a built bundle.")
    parser.add_argument("--bundle-dir", required=True,
                        help="bundle directory produced by harness.phase0q_cloud.bundle")
    parser.add_argument("--out-dir", default=str(ARTIFACT_DIR),
                        help=f"output directory (default: {ARTIFACT_DIR})")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    summary = write_records(args.bundle_dir, args.out_dir)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
