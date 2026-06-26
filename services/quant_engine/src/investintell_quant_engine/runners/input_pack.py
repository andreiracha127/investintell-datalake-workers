"""Certified Input Pack dry-run runner."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from investintell_quant_core.hashing.canonical import stable_hash

from investintell_quant_engine._paths import REPO_ROOT, ensure_repo_paths
from investintell_quant_engine.contract_bundle import verify_bundle
from investintell_quant_engine.preflight import validate_offline_request, validate_runtime_disabled

ensure_repo_paths()

from src.input_packs.hashing import canonical_json_sha256, load_json
from src.input_packs.verifier import verify_pack


def current_contract_bundle_sha256() -> str:
    result = verify_bundle(REPO_ROOT / "contracts" / "quant-engine" / "v1")
    if not result["ok"]:
        raise ValueError(f"current quant-engine contract bundle is invalid: {json.dumps(result, sort_keys=True)}")
    return str(result["bundle_sha256"]).removeprefix("sha256:")


def run_input_pack_dry_run(
    input_pack: str | Path,
    *,
    job_id: str | None = None,
    jobs: int = 1,
    offline: bool = True,
) -> dict[str, Any]:
    """Verify a Certified Input Pack without database or network access."""
    validate_offline_request(offline=offline, jobs=jobs)
    root = Path(input_pack)
    verification = verify_pack(root)
    if not verification["ok"]:
        raise ValueError(f"invalid certified input pack: {json.dumps(verification, sort_keys=True)}")

    manifest = load_json(root / "manifest.json")
    expected_contract = current_contract_bundle_sha256()
    pack_contract = str(manifest["contract_bundle_sha256"])
    if pack_contract != expected_contract:
        raise ValueError(
            "certified input pack contract_bundle_sha256 mismatch: "
            f"expected {expected_contract}, got {pack_contract}"
        )
    source_snapshot_sha256 = canonical_json_sha256(
        {
            "raw_snapshot_sha256": manifest["raw_snapshot_sha256"],
            "canonical_snapshot_sha256": manifest["canonical_snapshot_sha256"],
        }
    )
    fingerprint_payload = {
        "schema_version": 1,
        "job_type": "certified_input_pack_dry_run",
        "input_pack_id": manifest["input_pack_id"],
        "input_pack_sha256": manifest["input_pack_sha256"],
        "contract_bundle_sha256": manifest["contract_bundle_sha256"],
        "source_snapshot_sha256": source_snapshot_sha256,
        "runtime_activation": False,
    }
    run_fingerprint = stable_hash(fingerprint_payload)
    result = {
        "schema_version": 1,
        "job_type": "certified_input_pack_dry_run",
        "job_id": job_id or f"input-pack-dry-run-{run_fingerprint[:16]}",
        "execution_id": f"input-pack-dry-run-{run_fingerprint[:16]}",
        "run_fingerprint": run_fingerprint,
        "status": "succeeded",
        "classification": "input_pack_verified",
        "input_pack_id": manifest["input_pack_id"],
        "input_pack_sha256": manifest["input_pack_sha256"],
        "contract_bundle_sha256": manifest["contract_bundle_sha256"],
        "source_snapshot_sha256": source_snapshot_sha256,
        "output_logical_hashes": {
            "input_pack_sha256": manifest["input_pack_sha256"],
            "source_snapshot_sha256": source_snapshot_sha256,
        },
        "errors": [],
        "runtime_activation": False,
        "freeze_ready": False,
        "a3_status": "open_macro_v03",
        "a4_status": "input_pack_certified_for_calibration",
        "a5_status": "blocked",
    }
    validate_runtime_disabled(result)
    return result
