"""Artifact-only external executor handshake validator for open_macro_v03.

The validator is intentionally stdlib-only. It validates committed handshake
artifacts and evidence without importing Docker/subprocess helpers, DB clients,
backend routes, allocator code, or quant-engine runtime paths.
"""

from __future__ import annotations

import datetime as dt
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
ENVELOPE_AS_OF: Final = "2026-06-26"
EXPECTED_RUN_OUTPUT_MANIFEST_SHA256: Final = "b49a36c99646a71f923b29a8275d21dd934e1e6f1c78bf803a476e4c96e72e15"

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
# shadow_result_manifest.json is intentionally excluded from OUTPUT_ARTIFACT_PATHS.
# It embeds output_manifest_sha256 to pin the hash of output_manifest.json; including it
# in output_manifest.json would create a circular hash dependency — each file's hash would
# depend on the other's finalised content. Its structural integrity is fully validated by
# validate_shadow_result_manifest(), which checks every pinned field plus the
# output_manifest_sha256 cross-reference.
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
EXPECTED_MOUNT_NAMES: Final[tuple[str, ...]] = (
    "input_pack",
    "calibration",
    "contract_bundle",
    "output",
)
EXPECTED_INPUT_BIND_TARGETS: Final[frozenset[str]] = frozenset(
    ("/input_pack", "/calibration", "/contracts")
)
DOCKER_RUN_OPTIONS_WITH_VALUE: Final[frozenset[str]] = frozenset(
    (
        "--add-host",
        "--cap-add",
        "--cap-drop",
        "--cidfile",
        "--cpus",
        "--device",
        "--dns",
        "--dns-search",
        "--entrypoint",
        "--env",
        "--env-file",
        "--group-add",
        "--hostname",
        "--ipc",
        "--label",
        "--log-driver",
        "--log-opt",
        "--memory",
        "--mount",
        "--name",
        "--net",
        "--network",
        "--pid",
        "--platform",
        "--pull",
        "--restart",
        "--security-opt",
        "--stop-signal",
        "--stop-timeout",
        "--ulimit",
        "--user",
        "--userns",
        "--volume",
        "--workdir",
        "-e",
        "-l",
        "-m",
        "-u",
        "-v",
        "-w",
    )
)
EXPECTED_NO_SIDE_EFFECT_CHECK_IDS: Final[tuple[str, ...]] = (
    "runtime_activation",
    "official_result",
    "db_write",
    "allocator_publish",
    "production_endpoint_activation",
    "backend_engine_execution",
    "backend_docker_execution",
    "backend_subprocess_execution",
)
EXPECTED_VALIDATION_CHECK_IDS: Final[tuple[str, ...]] = (
    "control_plane_request_valid",
    "shadow_job_envelope_valid",
    "executor_acceptance_valid",
    "no_side_effects",
    "reproducibility_reference_green",
    "logs_present",
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
MATERIALITY_PINS: Final[dict[str, object]] = {
    "threshold_version": "open_macro_v03_shadow_materiality_v1",
    "material_divergence": False,
    "max_relative_delta_pct": 0.0,
    "return_metric_delta_pct": 0.0,
    "risk_metric_delta_pct": 0.0,
    "allocation_weight_delta_pct": 0.0,
    "classification_rate_delta_pct": 0.0,
    "latency_p95_regression_pct": 0.0,
    "memory_peak_regression_pct": 0.0,
    "retry_rate_delta_pct": 0.0,
}
SHADOW_RESULT_EVIDENCE_HASH_FILES: Final[dict[str, str]] = {
    "output_manifest_sha256": "output_manifest.json",
    "reproducibility_report_sha256": "reproducibility_report.json",
    # The handshake evidence bundle maps result-manifest evidence slots onto the
    # local validation reports that replace pilot invariant/baseline reports.
    "invariant_report_sha256": "no_side_effects_report.json",
    "baseline_comparison_sha256": "validation_report.json",
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
    # JSON: hash over canonical re-serialisation (sorted keys, no whitespace) so the digest
    # is stable regardless of committed formatting. This intentionally diverges from what
    # `sha256sum` reports on the raw file; use file_logical_bytes for the on-disk byte count.
    candidate = Path(path)
    if candidate.suffix.lower() == ".json":
        return canonical_json_sha256(load_json(candidate))
    if candidate.suffix.lower() in {".md", ".log"}:
        return hashlib.sha256(_logical_text_bytes(candidate)).hexdigest()
    return hashlib.sha256(candidate.read_bytes()).hexdigest()


def file_logical_bytes(path: str | Path) -> int:
    # JSON uses raw CRLF-normalised bytes (not canonical serialisation) intentionally:
    # file_sha256 uses canonical re-serialisation so the two measures are complementary —
    # SHA catches content drift, byte count catches format/whitespace drift.
    candidate = Path(path)
    if candidate.suffix.lower() in {".json", ".md", ".log"}:
        return len(_logical_text_bytes(candidate))
    return candidate.stat().st_size


def _require_mapping(payload: Any, *, where: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise HandshakeValidationError(f"{where}: expected object")
    return payload


def _require_equal(payload: Mapping[str, Any], key: str, expected: object, *, where: str) -> None:
    actual = payload.get(key)
    if isinstance(expected, bool):
        matched = actual is expected
    elif isinstance(expected, (int, float)):
        matched = type(actual) is type(expected) and actual == expected
    else:
        matched = actual == expected
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
            "as_of": ENVELOPE_AS_OF,
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
    _validate_docker_run_policy(
        payload.get("docker_run_policy"),
        expected_image_digest=str(payload["engine_image_digest"]),
    )


def _validate_mounts(mounts: Any) -> None:
    if not isinstance(mounts, list):
        raise HandshakeValidationError("executor_acceptance.mounts must be a list")
    names: list[str] = []
    entries: list[Mapping[str, Any]] = []
    for entry in mounts:
        mapping = _require_mapping(entry, where="executor_acceptance.mounts[]")
        name = mapping.get("name")
        if not isinstance(name, str):
            raise HandshakeValidationError("executor_acceptance.mounts[]: name must be a string")
        names.append(name)
        entries.append(mapping)
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise HandshakeValidationError(f"executor_acceptance: duplicate mounts {duplicates}")
    if set(names) != set(EXPECTED_MOUNT_NAMES):
        raise HandshakeValidationError(
            f"executor_acceptance: mount names must be exactly {sorted(EXPECTED_MOUNT_NAMES)}"
        )
    by_name = {entry["name"]: entry for entry in entries}
    for name in ("input_pack", "calibration", "contract_bundle"):
        if by_name.get(name, {}).get("mode") != "read_only":
            raise HandshakeValidationError(f"executor_acceptance: {name} mount must be read_only")
    if by_name.get("output", {}).get("mode") != "read_write":
        raise HandshakeValidationError("executor_acceptance: output mount must be read_write")
    writable = sorted(name for name, entry in by_name.items() if entry.get("mode") == "read_write")
    if writable != ["output"]:
        raise HandshakeValidationError(f"executor_acceptance: unexpected writable mounts {writable}")


def _validate_docker_run_policy(policy: Any, *, expected_image_digest: str) -> None:
    if not isinstance(policy, list) or any(not isinstance(token, str) for token in policy):
        raise HandshakeValidationError("executor_acceptance: docker_run_policy must be a token list")
    options, image, command_args = _split_docker_run_policy(policy)

    network_values = _network_values(options)
    if network_values != ["none"]:
        raise HandshakeValidationError("executor_acceptance: docker_run_policy must require --network none")
    if any(_is_network_flag(token) for token in command_args):
        raise HandshakeValidationError("executor_acceptance: docker network flags must appear before image")
    if "--read-only" not in options:
        raise HandshakeValidationError("executor_acceptance: docker_run_policy must require --read-only")
    if "@" not in image:
        raise HandshakeValidationError("executor_acceptance: docker_run_policy image must be pinned by digest")
    image_digest = image.rsplit("@", 1)[1]
    if image_digest != expected_image_digest:
        raise HandshakeValidationError("executor_acceptance: docker_run_policy image digest mismatch")
    _validate_input_bind_mounts(options)


def _split_docker_run_policy(policy: list[str]) -> tuple[list[str], str, list[str]]:
    if policy[:2] != ["docker", "run"]:
        raise HandshakeValidationError("executor_acceptance: docker_run_policy must start with docker run")
    options: list[str] = []
    index = 2
    while index < len(policy):
        token = policy[index]
        if token == "--":
            index += 1
            break
        if not token.startswith("-"):
            break
        options.append(token)
        option_name = token.split("=", 1)[0]
        if "=" in token:
            index += 1
        elif option_name in DOCKER_RUN_OPTIONS_WITH_VALUE:
            if index + 1 >= len(policy):
                raise HandshakeValidationError(
                    f"executor_acceptance: docker option {token} requires a value"
                )
            options.append(policy[index + 1])
            index += 2
        else:
            index += 1
    if index >= len(policy):
        raise HandshakeValidationError("executor_acceptance: docker_run_policy must include pinned image")
    return options, policy[index], policy[index + 1 :]


def _is_network_flag(token: str) -> bool:
    return token in {"--network", "--net"} or token.startswith("--network=") or token.startswith("--net=")


def _network_values(options: Sequence[str]) -> list[str | None]:
    values: list[str | None] = []
    index = 0
    while index < len(options):
        token = options[index]
        if token in {"--network", "--net"}:
            values.append(options[index + 1] if index + 1 < len(options) else None)
            index += 2
        elif token.startswith("--network=") or token.startswith("--net="):
            values.append(token.split("=", 1)[1])
            index += 1
        else:
            index += 1
    return values


def _validate_input_bind_mounts(options: Sequence[str]) -> None:
    seen_targets: set[str] = set()
    for spec in _docker_mount_specs(options):
        attrs, flags = _parse_mount_spec(spec)
        if attrs.get("type") != "bind":
            continue
        target = attrs.get("dst") or attrs.get("destination") or attrs.get("target")
        if target in EXPECTED_INPUT_BIND_TARGETS:
            readonly = (
                "readonly" in flags
                or "ro" in flags
                or attrs.get("readonly") in {"true", "1"}
                or attrs.get("ro") in {"true", "1"}
            )
            if not readonly:
                raise HandshakeValidationError(
                    f"executor_acceptance: input bind mount {target} must be readonly"
                )
            seen_targets.add(target)
    missing = sorted(EXPECTED_INPUT_BIND_TARGETS - seen_targets)
    if missing:
        raise HandshakeValidationError(f"executor_acceptance: docker_run_policy missing input binds {missing}")


def _docker_mount_specs(options: Sequence[str]) -> list[str]:
    specs: list[str] = []
    index = 0
    while index < len(options):
        token = options[index]
        if token == "--mount":
            specs.append(options[index + 1])
            index += 2
        elif token.startswith("--mount="):
            specs.append(token.split("=", 1)[1])
            index += 1
        elif token in {"--volume", "-v"}:
            specs.append(_volume_to_mount_spec(options[index + 1]))
            index += 2
        elif token.startswith("--volume="):
            specs.append(_volume_to_mount_spec(token.split("=", 1)[1]))
            index += 1
        else:
            index += 1
    return specs


def _volume_to_mount_spec(spec: str) -> str:
    parts = spec.split(":")
    if len(parts) < 2:
        return ""
    flags = ",".join(parts[2:])
    return f"type=bind,dst={parts[1]}{',' + flags if flags else ''}"


def _parse_mount_spec(spec: str) -> tuple[dict[str, str], set[str]]:
    attrs: dict[str, str] = {}
    flags: set[str] = set()
    for raw_part in spec.split(","):
        part = raw_part.strip()
        if not part:
            continue
        key, sep, value = part.partition("=")
        if sep:
            attrs[key] = value
        else:
            flags.add(part)
    return attrs, flags


def validate_executor_result_reference(payload: Mapping[str, Any]) -> None:
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "handshake_id": HANDSHAKE_ID,
            "status": "artifact_reference_only",
            "artifact_uri": OUTPUT_ARTIFACT_URI,
            "output_artifact_uri": OUTPUT_ARTIFACT_URI,
            "output_manifest_path": "output_manifest.json",
            "shadow_result_manifest_path": "shadow_result_manifest.json",
            **SIDE_EFFECT_PINS,
            **BACKEND_NO_EXEC_PINS,
        },
        where="executor_result_reference",
    )


def validate_shadow_result_manifest(
    payload: Mapping[str, Any], *, evidence_hashes: Mapping[str, str] | None = None
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
    _require_fields(
        payload,
        ("started_at", "finished_at", "duration_ms", "memory_peak_bytes", "cpu_time_ms"),
        where="shadow_result_manifest",
    )
    _require_artifact_uri(payload.get("output_artifact_uri"), where="shadow_result_manifest")
    _validate_result_timing(payload)
    _require_int(payload, "memory_peak_bytes", where="shadow_result_manifest", minimum=0)
    _require_int(payload, "cpu_time_ms", where="shadow_result_manifest", minimum=0)
    if "failure_class" in payload or "side_effect_attempt_count" in payload:
        raise HandshakeValidationError("shadow_result_manifest: success cannot carry failure evidence")
    if evidence_hashes is not None:
        for field, expected_hash in evidence_hashes.items():
            _require_equal(payload, field, expected_hash, where="shadow_result_manifest")
    _require_zero_divergence(_require_mapping(payload.get("divergence_summary"), where="divergence_summary"))
    materiality = _require_mapping(payload.get("materiality_summary"), where="materiality_summary")
    _require_pins(materiality, MATERIALITY_PINS, where="materiality_summary")


def _validate_result_timing(payload: Mapping[str, Any]) -> None:
    started_at = _parse_utc_timestamp(payload.get("started_at"), where="shadow_result_manifest.started_at")
    finished_at = _parse_utc_timestamp(payload.get("finished_at"), where="shadow_result_manifest.finished_at")
    duration_ms = _require_int(payload, "duration_ms", where="shadow_result_manifest", minimum=1)
    if finished_at <= started_at:
        raise HandshakeValidationError("shadow_result_manifest: finished_at must be after started_at")
    expected_duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    if duration_ms != expected_duration_ms:
        raise HandshakeValidationError(
            f"shadow_result_manifest: duration_ms must match timestamp delta {expected_duration_ms}"
        )


def _parse_utc_timestamp(value: Any, *, where: str) -> dt.datetime:
    if not isinstance(value, str):
        raise HandshakeValidationError(f"{where}: expected UTC timestamp string")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HandshakeValidationError(f"{where}: invalid UTC timestamp {value!r}") from exc
    if parsed.tzinfo is None:
        raise HandshakeValidationError(f"{where}: timestamp must include timezone")
    return parsed.astimezone(dt.UTC)


def _require_int(payload: Mapping[str, Any], key: str, *, where: str, minimum: int) -> int:
    value = payload.get(key)
    if type(value) is not int or value < minimum:
        raise HandshakeValidationError(f"{where}: {key} must be an int >= {minimum}")
    return value


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
    by_path: dict[str, Mapping[str, Any]] = {}
    for entry in artifacts:
        mapping = _require_mapping(entry, where="output_manifest.artifacts[]")
        rel = mapping.get("path")
        if not isinstance(rel, str):
            raise HandshakeValidationError("output_manifest.artifacts[]: path must be a string")
        if rel in by_path:
            raise HandshakeValidationError(f"output_manifest duplicate artifact path: {rel}")
        by_path[rel] = mapping
    if set(by_path) != set(OUTPUT_ARTIFACT_PATHS):
        raise HandshakeValidationError(
            f"output_manifest artifacts mismatch: {sorted(set(by_path) ^ set(OUTPUT_ARTIFACT_PATHS))}"
        )
    unexpected_files = _unexpected_output_files(root)
    if unexpected_files:
        raise HandshakeValidationError(f"output_manifest unexpected files on disk: {unexpected_files}")
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


def _unexpected_output_files(root: Path) -> list[str]:
    allowed = set(ROOT_REQUIRED_FILES) | set(OUTPUT_ARTIFACT_PATHS)
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.relative_to(root).as_posix() not in allowed
    )


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
            "missing_hashes": [],
            "unexpected_hashes": [],
            "output_manifest_sha256_by_run": {
                label: EXPECTED_RUN_OUTPUT_MANIFEST_SHA256 for label in EXPECTED_LABELS
            },
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
    seen: dict[str, Mapping[str, Any]] = {}
    for check in checks:
        entry = _require_mapping(check, where="no_side_effects_report.check")
        check_id = entry.get("id")
        if not isinstance(check_id, str):
            raise HandshakeValidationError("no_side_effects_report.check: id must be a string")
        if check_id in seen:
            raise HandshakeValidationError(f"no_side_effects_report duplicate check id: {check_id}")
        seen[check_id] = entry
        _require_equal(entry, "allowed", False, where="no_side_effects_report.check")
        _require_equal(entry, "status", "pass", where="no_side_effects_report.check")
    if set(seen) != set(EXPECTED_NO_SIDE_EFFECT_CHECK_IDS):
        missing = sorted(set(EXPECTED_NO_SIDE_EFFECT_CHECK_IDS) - set(seen))
        unexpected = sorted(set(seen) - set(EXPECTED_NO_SIDE_EFFECT_CHECK_IDS))
        raise HandshakeValidationError(
            f"no_side_effects_report check ids mismatch: missing={missing} unexpected={unexpected}"
        )


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
    checks = payload.get("checks")
    if not isinstance(checks, list) or not checks:
        raise HandshakeValidationError("validation_report checks must be a non-empty list")
    seen: set[str] = set()
    for check in checks:
        entry = _require_mapping(check, where="validation_report.check")
        check_id = entry.get("id")
        if not isinstance(check_id, str):
            raise HandshakeValidationError("validation_report.check: id must be a string")
        if check_id in seen:
            raise HandshakeValidationError(f"validation_report duplicate check id: {check_id}")
        seen.add(check_id)
        _require_equal(entry, "status", "pass", where="validation_report.check")
    missing = sorted(set(EXPECTED_VALIDATION_CHECK_IDS) - seen)
    if missing:
        raise HandshakeValidationError(f"validation_report missing expected checks {missing}")


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
    validate_shadow_result_manifest(
        result,
        evidence_hashes={
            field: file_sha256(bundle_root / rel)
            for field, rel in SHADOW_RESULT_EVIDENCE_HASH_FILES.items()
        },
    )

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


if __name__ == "__main__":
    import sys

    try:
        result = verify_handshake()
        print(json.dumps(result, indent=2))
    except HandshakeValidationError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
