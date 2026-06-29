"""Artifact-only external executor handshake validator for open_macro_v03.

The validator is intentionally stdlib-only. It validates committed handshake
artifacts and evidence without importing Docker/subprocess helpers, DB clients,
backend routes, allocator code, or quant-engine runtime paths.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

HANDSHAKE_ID: Final = "open_macro_v03_external_executor_handshake_001"
RUNTIME_SKELETON_ID: Final = "open_macro_v03_runtime_skeleton_001"
SHADOW_ID: Final = "open_macro_v03_shadow_001"
CALIBRATION_ID: Final = "open_macro_v03_calibration_001"
INPUT_PACK_ID: Final = "open_macro_v03_certified_input_pack_001"
CONTROL_PLANE_CONTRACT_MERGE_COMMIT: Final = "ba7bc6c2f2f472fdf9e8318de5fd3804efc2cc71"
RUNTIME_SKELETON_MERGE_COMMIT: Final = "87e69a8cfb7aa646d1d0c7c9d7610ce914514cc7"

INPUT_PACK_SHA256: Final = "ae8b76e5959cb5e9c10ced7b33fc13a01a3484865deeead56c5b83b1c440e08f"
CALIBRATION_CONFIG_SHA256: Final = "869e392bd49c8f7e0bf60890d1658ef3cf0483655af3a1c9f105b99cd29c268c"
CONTRACT_BUNDLE_SHA256: Final = "4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a"
ENGINE_COMMIT: Final = "ee39adbe6cb6541d4fdfa78f1428478ffffaf638"
ENGINE_IMAGE_DIGEST: Final = "sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b"
RUN_FINGERPRINT: Final = "078cef19bdb6ad0de1716dd73a6e6807d45ca4cb6c675838947e2531832c8106"

REQUEST_ID: Final = "req-open-macro-v03-external-executor-handshake-001"
CORRELATION_ID: Final = "corr-open-macro-v03-external-executor-handshake-001"
EXECUTION_ID: Final = "exec-open-macro-v03-external-executor-handshake-001"
OUTPUT_ARTIFACT_URI: Final = f"artifact://shadow/{SHADOW_ID}/{HANDSHAKE_ID}"
EXECUTION_POLICY: Final = "isolated_external_executor_no_productive_runtime_docker"

EXPECTED_LABELS: Final[tuple[str, ...]] = tuple(
    f"{mode}_jobs{jobs}_r{repeat}"
    for mode in ("container", "host")
    for jobs in (1, 4)
    for repeat in (0, 1)
)
EXPECTED_HOST_RUNS: Final[tuple[str, ...]] = tuple(label for label in EXPECTED_LABELS if label.startswith("host_"))
EXPECTED_CONTAINER_RUNS: Final[tuple[str, ...]] = tuple(
    label for label in EXPECTED_LABELS if label.startswith("container_")
)
LOGS_REQUIRED: Final[tuple[str, ...]] = (
    "logs/control_plane_validator.log",
    "logs/external_executor.log",
)
OUTPUT_ARTIFACT_PATHS: Final[tuple[str, ...]] = (
    "control_plane_request.json",
    "shadow_job_envelope.json",
    "executor_acceptance.json",
    "executor_result_reference.json",
    "validation_report.json",
    "no_side_effects_report.json",
    "reproducibility_report.json",
    "handshake_report.md",
    *LOGS_REQUIRED,
)
ROOT_REQUIRED_FILES: Final[tuple[str, ...]] = (
    "handshake_manifest.json",
    "control_plane_request.json",
    "shadow_job_envelope.json",
    "executor_acceptance.json",
    "executor_result_reference.json",
    "shadow_result_manifest.json",
    "output_manifest.json",
    "validation_report.json",
    "no_side_effects_report.json",
    "reproducibility_report.json",
    "handshake_report.md",
    *LOGS_REQUIRED,
)
SIDE_EFFECT_PINS: Final[dict[str, object]] = {
    "runtime_activation": False,
    "A5": "blocked",
    "freeze_ready": False,
    "official_result": False,
    "allow_db_write": False,
    "allow_allocator_publish": False,
    "production_endpoint_activation": "none",
}
RESULT_SIDE_EFFECT_PINS: Final[dict[str, object]] = {
    "runtime_activation": False,
    "official_result": False,
    "allow_db_write": False,
    "allow_allocator_publish": False,
    "production_endpoint_activation": "none",
}
BACKEND_NO_EXEC_PINS: Final[dict[str, object]] = {
    "backend_executes_engine": False,
    "backend_executes_docker": False,
    "backend_executes_subprocess": False,
}
PINNED_PROVENANCE: Final[dict[str, object]] = {
    "input_pack_id": INPUT_PACK_ID,
    "input_pack_sha256": INPUT_PACK_SHA256,
    "calibration_id": CALIBRATION_ID,
    "calibration_config_sha256": CALIBRATION_CONFIG_SHA256,
    "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
    "engine_commit": ENGINE_COMMIT,
    "engine_image_digest": ENGINE_IMAGE_DIGEST,
}
RESULT_PROVENANCE: Final[dict[str, object]] = {
    "calibration_id": CALIBRATION_ID,
    "input_pack_sha256": INPUT_PACK_SHA256,
    "engine_commit": ENGINE_COMMIT,
    "engine_image_digest": ENGINE_IMAGE_DIGEST,
}


class HandshakeValidationError(ValueError):
    """Raised when a handshake artifact violates the inert contract."""


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def handshake_root(root: Path | None = None) -> Path:
    root = root or repo_root()
    return root / "artifacts" / "handshake" / HANDSHAKE_ID


def load_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _logical_text_bytes(path: Path) -> bytes:
    return path.read_bytes().replace(b"\r\n", b"\n")


def file_sha256(path: str | Path) -> str:
    candidate = Path(path)
    if candidate.suffix.lower() == ".json":
        return canonical_json_sha256(load_json(candidate))
    if candidate.suffix.lower() in {".md", ".log"}:
        return hashlib.sha256(_logical_text_bytes(candidate)).hexdigest()
    return hashlib.sha256(candidate.read_bytes()).hexdigest()


def file_logical_bytes(path: str | Path) -> int:
    candidate = Path(path)
    if candidate.suffix.lower() in {".md", ".log"}:
        return len(_logical_text_bytes(candidate))
    if candidate.suffix.lower() == ".json":
        return len(_logical_text_bytes(candidate))
    return candidate.stat().st_size


def _require_mapping(payload: Any, *, where: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise HandshakeValidationError(f"{where}: expected object")
    return payload


def _require_equal(payload: Mapping[str, Any], key: str, expected: object, *, where: str) -> None:
    actual = payload.get(key)
    matched = actual is expected if isinstance(expected, bool) else actual == expected
    if not matched:
        raise HandshakeValidationError(f"{where}: {key} {actual!r} != {expected!r}")


def _require_pins(payload: Mapping[str, Any], pins: Mapping[str, object], *, where: str) -> None:
    for key, expected in pins.items():
        _require_equal(payload, key, expected, where=where)


def _require_fields(payload: Mapping[str, Any], fields: Sequence[str], *, where: str) -> None:
    missing = [field for field in fields if field not in payload]
    if missing:
        raise HandshakeValidationError(f"{where}: missing required fields {missing}")


def _require_artifact_uri(value: Any, *, where: str) -> None:
    if value != OUTPUT_ARTIFACT_URI:
        raise HandshakeValidationError(f"{where}: output artifact URI must be {OUTPUT_ARTIFACT_URI}")


def reject_symlinks(root: Path) -> None:
    if root.is_symlink():
        raise HandshakeValidationError(f"handshake root is a symlink: {root}")
    if not root.exists():
        raise HandshakeValidationError(f"handshake root is missing: {root}")
    for candidate in root.rglob("*"):
        if candidate.is_symlink():
            raise HandshakeValidationError(f"handshake artifact is a symlink: {candidate}")


def validate_handshake_manifest(payload: Mapping[str, Any]) -> None:
    _require_pins(
        payload,
        {
            "handshake_id": HANDSHAKE_ID,
            "status": "candidate",
            "control_plane_contract_merge_commit": CONTROL_PLANE_CONTRACT_MERGE_COMMIT,
            "runtime_skeleton_id": RUNTIME_SKELETON_ID,
            "shadow_id": SHADOW_ID,
            "calibration_id": CALIBRATION_ID,
            "mode": "shadow",
            "execution_policy": EXECUTION_POLICY,
            **SIDE_EFFECT_PINS,
            **BACKEND_NO_EXEC_PINS,
        },
        where="handshake_manifest",
    )


def validate_control_plane_request(payload: Mapping[str, Any]) -> None:
    _require_fields(
        payload,
        (
            "schema_version",
            "handshake_id",
            "request_id",
            "correlation_id",
            "execution_id",
            "feature_flag_name",
            "feature_flag_default",
            "output_artifact_uri",
        ),
        where="control_plane_request",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "handshake_id": HANDSHAKE_ID,
            "control_plane_contract_merge_commit": CONTROL_PLANE_CONTRACT_MERGE_COMMIT,
            "runtime_skeleton_id": RUNTIME_SKELETON_ID,
            "shadow_id": SHADOW_ID,
            "mode": "shadow",
            "request_id": REQUEST_ID,
            "correlation_id": CORRELATION_ID,
            "execution_id": EXECUTION_ID,
            "feature_flag_name": "open_macro_v03_runtime_activation",
            "feature_flag_default": False,
            **PINNED_PROVENANCE,
            **SIDE_EFFECT_PINS,
            **BACKEND_NO_EXEC_PINS,
        },
        where="control_plane_request",
    )
    _require_artifact_uri(payload.get("output_artifact_uri"), where="control_plane_request")


def validate_shadow_job_envelope(payload: Mapping[str, Any]) -> None:
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "shadow_id": SHADOW_ID,
            "request_id": REQUEST_ID,
            "correlation_id": CORRELATION_ID,
            "execution_id": EXECUTION_ID,
            "run_fingerprint": RUN_FINGERPRINT,
            "as_of": "2026-06-26",
            "strategy": "open_macro_v03",
            "mode": "shadow",
            "execution_policy": EXECUTION_POLICY,
            "runtime_activation": False,
            "allow_db_write": False,
            "allow_allocator_publish": False,
            "production_endpoint_activation": "none",
            **PINNED_PROVENANCE,
        },
        where="shadow_job_envelope",
    )
    _require_artifact_uri(payload.get("output_artifact_uri"), where="shadow_job_envelope")


def validate_executor_acceptance(payload: Mapping[str, Any], envelope: Mapping[str, Any]) -> None:
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "handshake_id": HANDSHAKE_ID,
            "status": "accepted",
            "accepted": True,
            "request_id": REQUEST_ID,
            "correlation_id": CORRELATION_ID,
            "execution_id": EXECUTION_ID,
            "run_fingerprint": RUN_FINGERPRINT,
            "provenance_match": True,
            "execution_policy": EXECUTION_POLICY,
            "docker_network": "none",
            "input_pack_mount": "read_only",
            "calibration_mount": "read_only",
            "output_mount": "read_write",
            "writable_mounts": ["output"],
            "requires_docker_network_none": True,
            **PINNED_PROVENANCE,
            **SIDE_EFFECT_PINS,
            **BACKEND_NO_EXEC_PINS,
        },
        where="executor_acceptance",
    )
    for key in ("request_id", "correlation_id", "execution_id", "run_fingerprint"):
        if payload.get(key) != envelope.get(key):
            raise HandshakeValidationError(f"executor_acceptance: envelope mismatch for {key}")
    _validate_mounts(payload.get("mounts"))
    _validate_docker_run_policy(payload.get("docker_run_policy"))


def _validate_mounts(mounts: Any) -> None:
    if not isinstance(mounts, list):
        raise HandshakeValidationError("executor_acceptance.mounts must be a list")
    by_name = {entry.get("name"): entry for entry in mounts if isinstance(entry, Mapping)}
    for name in ("input_pack", "calibration", "contract_bundle"):
        if by_name.get(name, {}).get("mode") != "read_only":
            raise HandshakeValidationError(f"executor_acceptance: {name} mount must be read_only")
    if by_name.get("output", {}).get("mode") != "read_write":
        raise HandshakeValidationError("executor_acceptance: output mount must be read_write")
    writable = sorted(name for name, entry in by_name.items() if entry.get("mode") == "read_write")
    if writable != ["output"]:
        raise HandshakeValidationError(f"executor_acceptance: unexpected writable mounts {writable}")


def _validate_docker_run_policy(policy: Any) -> None:
    if not isinstance(policy, list) or "--network" not in policy:
        raise HandshakeValidationError("executor_acceptance: docker_run_policy must require --network none")
    network_index = policy.index("--network")
    if network_index + 1 >= len(policy) or policy[network_index + 1] != "none":
        raise HandshakeValidationError("executor_acceptance: docker_run_policy must require --network none")
    if "--read-only" not in policy:
        raise HandshakeValidationError("executor_acceptance: docker_run_policy must require --read-only")


def validate_executor_result_reference(payload: Mapping[str, Any]) -> None:
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "handshake_id": HANDSHAKE_ID,
            "status": "artifact_reference_only",
            "artifact_uri": OUTPUT_ARTIFACT_URI,
            "output_artifact_uri": OUTPUT_ARTIFACT_URI,
            **SIDE_EFFECT_PINS,
            **BACKEND_NO_EXEC_PINS,
        },
        where="executor_result_reference",
    )


def validate_shadow_result_manifest(
    payload: Mapping[str, Any], *, output_manifest_sha256: str | None = None
) -> None:
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "shadow_id": SHADOW_ID,
            "request_id": REQUEST_ID,
            "correlation_id": CORRELATION_ID,
            "execution_id": EXECUTION_ID,
            "run_fingerprint": RUN_FINGERPRINT,
            "status": "succeeded",
            "retryable": False,
            "retry_count": 0,
            **RESULT_PROVENANCE,
            **RESULT_SIDE_EFFECT_PINS,
        },
        where="shadow_result_manifest",
    )
    _require_artifact_uri(payload.get("output_artifact_uri"), where="shadow_result_manifest")
    if "failure_class" in payload or "side_effect_attempt_count" in payload:
        raise HandshakeValidationError("shadow_result_manifest: success cannot carry failure evidence")
    if output_manifest_sha256 is not None:
        _require_equal(
            payload,
            "output_manifest_sha256",
            output_manifest_sha256,
            where="shadow_result_manifest",
        )
    _require_zero_divergence(_require_mapping(payload.get("divergence_summary"), where="divergence_summary"))
    materiality = _require_mapping(payload.get("materiality_summary"), where="materiality_summary")
    _require_equal(materiality, "material_divergence", False, where="materiality_summary")
    _require_equal(materiality, "max_relative_delta_pct", 0.0, where="materiality_summary")


def _require_zero_divergence(divergence: Mapping[str, Any]) -> None:
    for field in (
        "missing_outputs",
        "unexpected_outputs",
        "mismatch_count",
        "nan_or_inf_count",
        "constraint_violations",
        "invariant_failures",
    ):
        _require_equal(divergence, field, 0, where="divergence_summary")


def validate_output_manifest(root: Path, payload: Mapping[str, Any]) -> None:
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "artifact_type": "external_executor_handshake_output_manifest",
            "handshake_id": HANDSHAKE_ID,
            "shadow_id": SHADOW_ID,
            "status": "succeeded",
            "logs_required": list(LOGS_REQUIRED),
            "unexpected_outputs": [],
        },
        where="output_manifest",
    )
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        raise HandshakeValidationError("output_manifest.artifacts must be a list")
    by_path = {entry.get("path"): entry for entry in artifacts if isinstance(entry, Mapping)}
    if set(by_path) != set(OUTPUT_ARTIFACT_PATHS):
        raise HandshakeValidationError(
            f"output_manifest artifacts mismatch: {sorted(set(by_path) ^ set(OUTPUT_ARTIFACT_PATHS))}"
        )
    for rel in OUTPUT_ARTIFACT_PATHS:
        entry = _require_mapping(by_path[rel], where=f"output_manifest[{rel}]")
        path = _ensure_child(root / rel, root)
        if not path.is_file():
            raise HandshakeValidationError(f"output_manifest artifact missing: {rel}")
        _require_equal(entry, "sha256", file_sha256(path), where=f"output_manifest[{rel}]")
        _require_equal(entry, "bytes", file_logical_bytes(path), where=f"output_manifest[{rel}]")
    for rel in LOGS_REQUIRED:
        if rel not in by_path:
            raise HandshakeValidationError(f"output_manifest missing required log {rel}")


def _ensure_child(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise HandshakeValidationError(f"path escapes handshake root: {path}") from exc
    return resolved


def validate_reproducibility_report(payload: Mapping[str, Any]) -> None:
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "handshake_id": HANDSHAKE_ID,
            "shadow_id": SHADOW_ID,
            "calibration_id": CALIBRATION_ID,
            "input_pack_sha256": INPUT_PACK_SHA256,
            "calibration_config_sha256": CALIBRATION_CONFIG_SHA256,
            "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
            "run_fingerprint": RUN_FINGERPRINT,
            "expected_run_count": len(EXPECTED_LABELS),
            "run_count": len(EXPECTED_LABELS),
            "expected_labels": list(EXPECTED_LABELS),
            "missing": [],
            "unexpected": [],
            "run_hash_mismatches": [],
            "duplicates": 0,
            "mismatch_count": 0,
            "container_runs": list(EXPECTED_CONTAINER_RUNS),
            "host_runs": list(EXPECTED_HOST_RUNS),
            "jobs_matrix": [1, 4],
            "repeat_runs_per_mode": 2,
            "network": "none",
            "db_access": False,
            "input_pack_mount": "read_only",
            "calibration_mount": "read_only",
            "output_mount": "read_write",
            "writable_mounts": ["output"],
            "path_independence": True,
            "docker_image_digest": ENGINE_IMAGE_DIGEST,
            "docker_image_provenance_ok": True,
            "ok": True,
        },
        where="reproducibility_report",
    )


def validate_no_side_effects_report(payload: Mapping[str, Any]) -> None:
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "handshake_id": HANDSHAKE_ID,
            "status": "pass",
            "db_write_mode": "none",
            "allocator_impact": "none",
            "production_impact": "none",
            **SIDE_EFFECT_PINS,
            **BACKEND_NO_EXEC_PINS,
        },
        where="no_side_effects_report",
    )
    checks = payload.get("checks")
    if not isinstance(checks, list) or not checks:
        raise HandshakeValidationError("no_side_effects_report checks must be a non-empty list")
    for check in checks:
        entry = _require_mapping(check, where="no_side_effects_report.check")
        _require_equal(entry, "allowed", False, where="no_side_effects_report.check")
        _require_equal(entry, "status", "pass", where="no_side_effects_report.check")


def validate_validation_report(payload: Mapping[str, Any]) -> None:
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "handshake_id": HANDSHAKE_ID,
            "status": "pass",
            "validated": True,
            "control_plane_contract_merge_commit": CONTROL_PLANE_CONTRACT_MERGE_COMMIT,
            "runtime_activation": False,
            "A5": "blocked",
            "official_result": False,
        },
        where="validation_report",
    )


def verify_handshake(root: Path | None = None) -> dict[str, Any]:
    bundle_root = root or handshake_root()
    reject_symlinks(bundle_root)
    missing = [rel for rel in ROOT_REQUIRED_FILES if not (bundle_root / rel).is_file()]
    if missing:
        raise HandshakeValidationError(f"missing handshake artifact: {missing[0]}")

    manifest = _require_mapping(load_json(bundle_root / "handshake_manifest.json"), where="handshake_manifest")
    request = _require_mapping(load_json(bundle_root / "control_plane_request.json"), where="control_plane_request")
    envelope = _require_mapping(load_json(bundle_root / "shadow_job_envelope.json"), where="shadow_job_envelope")
    acceptance = _require_mapping(load_json(bundle_root / "executor_acceptance.json"), where="executor_acceptance")
    reference = _require_mapping(
        load_json(bundle_root / "executor_result_reference.json"),
        where="executor_result_reference",
    )
    output_manifest = _require_mapping(load_json(bundle_root / "output_manifest.json"), where="output_manifest")
    validation_report = _require_mapping(load_json(bundle_root / "validation_report.json"), where="validation_report")
    no_side_effects = _require_mapping(
        load_json(bundle_root / "no_side_effects_report.json"),
        where="no_side_effects_report",
    )
    reproducibility = _require_mapping(
        load_json(bundle_root / "reproducibility_report.json"),
        where="reproducibility_report",
    )
    result = _require_mapping(load_json(bundle_root / "shadow_result_manifest.json"), where="shadow_result_manifest")

    validate_handshake_manifest(manifest)
    validate_control_plane_request(request)
    validate_shadow_job_envelope(envelope)
    validate_executor_acceptance(acceptance, envelope)
    validate_executor_result_reference(reference)
    validate_validation_report(validation_report)
    validate_no_side_effects_report(no_side_effects)
    validate_reproducibility_report(reproducibility)
    validate_output_manifest(bundle_root, output_manifest)
    validate_shadow_result_manifest(result, output_manifest_sha256=file_sha256(bundle_root / "output_manifest.json"))

    return {
        "handshake_id": HANDSHAKE_ID,
        "status": "validated",
        "runtime_activation": False,
        "A5": "blocked",
        "official_result": False,
        "backend_runtime_execution": "none",
        "allocator_impact": "none",
        "production_impact": "none",
        "validated": True,
    }
