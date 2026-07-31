"""Certified Input Pack dry-run runner."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

from investintell_quant_core.hashing.canonical import stable_hash

from investintell_quant_engine._paths import REPO_ROOT, ensure_repo_paths
from investintell_quant_engine.contract_bundle import verify_bundle
from investintell_quant_engine.preflight import validate_offline_request, validate_runtime_disabled

ensure_repo_paths()

from src.input_packs.hashing import canonical_json_sha256, load_json
from src.input_packs.registry import RegistryError, load_registry


def _verify_pack_for(pack_id: str, root: Path) -> dict[str, Any]:
    """Verify ``root`` with the verifier the pack's registry PROFILE names.

    Before the registry there was one hard-wired entry point (the P0
    ``verify_pack``) that only accepted pack ``_001``, so this runner could not
    verify the pack that was actually current — it was effectively dead. The
    profile decides which verifier applies, so a P0 pack and a P1 pack are both
    verifiable through the same call.
    """
    registry = load_registry()
    entry = registry.entry(pack_id)
    target = registry.profiles[entry.profile]["verifier"]
    module_name, _, attr = target.partition(":")
    module = importlib.import_module(module_name)
    return getattr(module, attr)(root)


def contract_bundle_sha256_for(pack_id: str) -> str:
    """The contract bundle THIS pack is certified under, re-verified on disk.

    The old ``current_contract_bundle_sha256()`` hard-coded ``v1`` while the
    live packs were certified under ``v2``, so every dry run raised a mismatch.
    The binding is per pack and comes from the registry; the bundle itself is
    still recomputed and must verify.
    """
    entry = load_registry().entry(pack_id)
    result = verify_bundle(REPO_ROOT / entry.contract_dir)
    if not result["ok"]:
        raise ValueError(
            f"quant-engine contract bundle {entry.contract_dir} is invalid: "
            f"{json.dumps(result, sort_keys=True)}"
        )
    bundle_sha = str(result["bundle_sha256"]).removeprefix("sha256:")
    if bundle_sha != entry.contract_bundle_sha256:
        raise ValueError(
            f"contract bundle {entry.contract_dir} hashes to {bundle_sha}, but the "
            f"registry binds {pack_id} to {entry.contract_bundle_sha256}"
        )
    return bundle_sha


def _validate_expected_hash(*, name: str, expected: str | None, actual: str) -> None:
    if expected is not None and expected != actual:
        raise ValueError(f"certified input pack {name} mismatch: expected {expected}, got {actual}")


def run_input_pack_dry_run(
    input_pack: str | Path | None = None,
    *,
    profile: str | None = None,
    job_id: str | None = None,
    jobs: int = 1,
    offline: bool = True,
    expected_input_pack_sha256: str | None = None,
    expected_source_snapshot_sha256: str | None = None,
    expected_contract_bundle_sha256: str | None = None,
) -> dict[str, Any]:
    """Verify a Certified Input Pack without database or network access.

    ``input_pack`` defaults to whichever pack the registry currently promotes,
    so the dry run follows a promotion instead of having to be re-pointed.
    """
    validate_offline_request(offline=offline, jobs=jobs)
    if input_pack is None:
        registry = load_registry()
        root = registry.current(profile or "open_macro_v03_p1").dir
    else:
        root = Path(input_pack)

    manifest = load_json(root / "manifest.json")
    pack_id = str(manifest["input_pack_id"])
    try:
        verification = _verify_pack_for(pack_id, root)
    except RegistryError as exc:
        raise ValueError(f"certified input pack is not registered: {exc}") from exc
    if not verification["ok"]:
        raise ValueError(f"invalid certified input pack: {json.dumps(verification, sort_keys=True)}")

    expected_contract = contract_bundle_sha256_for(pack_id)
    pack_contract = str(manifest["contract_bundle_sha256"])
    _validate_expected_hash(
        name="contract_bundle_sha256",
        expected=expected_contract_bundle_sha256,
        actual=pack_contract,
    )
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
    _validate_expected_hash(
        name="input_pack_sha256",
        expected=expected_input_pack_sha256,
        actual=str(manifest["input_pack_sha256"]),
    )
    _validate_expected_hash(
        name="source_snapshot_sha256",
        expected=expected_source_snapshot_sha256,
        actual=source_snapshot_sha256,
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
