"""Offline controlled-shadow artifact validator for open_macro_v03.

This module validates committed evidence only. It deliberately does not import DB,
backend, allocator, Docker, subprocess, or runtime execution paths.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

import src.external_executor_handshake as hs
from services.quant_engine.src.investintell_quant_engine.contract_bundle import verify_bundle
from src.input_packs.hashing import file_sha256 as canonical_file_sha256
from src.input_packs.verifier import verify_pack

CONTROLLED_SHADOW_ID: Final = "open_macro_v03_controlled_shadow_001"
EXTERNAL_EXECUTOR_HANDSHAKE_MERGE_COMMIT: Final = "ab081183389dbe62e03d56dd493c443263f334e9"
CALIBRATION_RUN_MATRIX_SHA256: Final = "58b056ba7af0b419427de8ef6f9fbb718afca9bcd576224bf557d16401ab38ac"
CALIBRATION_OUTPUT_MANIFEST_SHA256: Final = "b49a36c99646a71f923b29a8275d21dd934e1e6f1c78bf803a476e4c96e72e15"
REQUEST_ID: Final = "req-open-macro-v03-controlled-shadow-001"
CORRELATION_ID: Final = "corr-open-macro-v03-controlled-shadow-001"
EXECUTION_ID: Final = "exec-open-macro-v03-controlled-shadow-001"
OUTPUT_ARTIFACT_URI: Final = f"artifact://shadow/{hs.SHADOW_ID}/{CONTROLLED_SHADOW_ID}"
EXECUTION_POLICY: Final = hs.EXECUTION_POLICY

EXPECTED_CONTROLLED_SHADOW_MANIFEST: Final[dict[str, object]] = {
    "controlled_shadow_id": CONTROLLED_SHADOW_ID,
    "external_executor_handshake_id": hs.HANDSHAKE_ID,
    "external_executor_handshake_001_merge_commit": EXTERNAL_EXECUTOR_HANDSHAKE_MERGE_COMMIT,
    "runtime_skeleton_id": hs.RUNTIME_SKELETON_ID,
    "shadow_id": hs.SHADOW_ID,
    "calibration_id": hs.CALIBRATION_ID,
    "input_pack_id": hs.INPUT_PACK_ID,
    "mode": "shadow",
    "runtime_activation": False,
    "A5": "blocked",
    "freeze_ready": False,
    "official_result": False,
    "allow_db_write": False,
    "allow_allocator_publish": False,
    "production_endpoint_activation": "none",
    "backend_executes_engine": False,
    "backend_executes_docker": False,
    "backend_executes_subprocess": False,
}

SIDE_EFFECT_PINS: Final[dict[str, object]] = {
    "runtime_activation": False,
    "A5": "blocked",
    "freeze_ready": False,
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
PROVENANCE_PINS: Final[dict[str, object]] = {
    "input_pack_id": hs.INPUT_PACK_ID,
    "input_pack_sha256": hs.INPUT_PACK_SHA256,
    "calibration_id": hs.CALIBRATION_ID,
    "calibration_config_sha256": hs.CALIBRATION_CONFIG_SHA256,
    "calibration_run_matrix_sha256": CALIBRATION_RUN_MATRIX_SHA256,
    "contract_bundle_sha256": hs.CONTRACT_BUNDLE_SHA256,
    "engine_commit": hs.ENGINE_COMMIT,
    "engine_image_digest": hs.ENGINE_IMAGE_DIGEST,
}
RESULT_PROVENANCE_PINS: Final[dict[str, object]] = {
    "input_pack_sha256": hs.INPUT_PACK_SHA256,
    "calibration_config_sha256": hs.CALIBRATION_CONFIG_SHA256,
    "calibration_run_matrix_sha256": CALIBRATION_RUN_MATRIX_SHA256,
    "engine_commit": hs.ENGINE_COMMIT,
    "engine_image_digest": hs.ENGINE_IMAGE_DIGEST,
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
DIVERGENCE_ZERO_PINS: Final[dict[str, int]] = {
    "mismatch_count": 0,
    "missing_outputs": 0,
    "unexpected_outputs": 0,
    "nan_or_inf_count": 0,
    "constraint_violations": 0,
    "invariant_failures": 0,
}

EXPECTED_LABELS: Final[tuple[str, ...]] = hs.EXPECTED_LABELS
EXPECTED_HOST_RUNS: Final[tuple[str, ...]] = hs.EXPECTED_HOST_RUNS
EXPECTED_CONTAINER_RUNS: Final[tuple[str, ...]] = hs.EXPECTED_CONTAINER_RUNS
LOGS_REQUIRED: Final[tuple[str, ...]] = hs.LOGS_REQUIRED
OUTPUT_ARTIFACT_PATHS: Final[tuple[str, ...]] = (
    "controlled_shadow_manifest.json",
    "control_plane_request.json",
    "shadow_job_envelope.json",
    "executor_acceptance.json",
    "baseline_comparison.json",
    "invariant_report.json",
    "reproducibility_report.json",
    "no_side_effects_report.json",
    "acceptance_report.json",
    "observability_evidence.json",
    "rollback_evidence.json",
    "controlled_shadow_report.md",
    *LOGS_REQUIRED,
)
FINAL_REQUIRED_FILES: Final[tuple[str, ...]] = (
    *OUTPUT_ARTIFACT_PATHS,
    "output_manifest.json",
    "shadow_result_manifest.json",
)
READ_ONLY_INPUT_PINS: Final[dict[str, dict[str, object]]] = {
    "input_pack": {
        "name": "input_pack",
        "path": "fixtures/input_packs/golden/certified_input_pack",
        "stable_id": hs.INPUT_PACK_ID,
        "sha256": hs.INPUT_PACK_SHA256,
        "bytes": 49375,
        "source_commit": "de5ab84bfa99aa240aa65bbe7f09ba90da2b2862",
        "mount": "read_only",
    },
    "calibration_config": {
        "name": "calibration_config",
        "path": "artifacts/calibration/open_macro_v03_calibration_001/calibration_config.json",
        "stable_id": "open_macro_v03_calibration_001:calibration_config",
        "sha256": hs.CALIBRATION_CONFIG_SHA256,
        "bytes": 2447,
        "source_commit": "10a49e1489661070986e241d9e04a8b890b54937",
        "mount": "read_only",
    },
    "calibration_run_matrix": {
        "name": "calibration_run_matrix",
        "path": "artifacts/calibration/open_macro_v03_calibration_001/run_matrix.json",
        "stable_id": "open_macro_v03_calibration_001:run_matrix",
        "sha256": CALIBRATION_RUN_MATRIX_SHA256,
        "bytes": 12923,
        "source_commit": "10a49e1489661070986e241d9e04a8b890b54937",
        "mount": "read_only",
    },
    "contract_bundle": {
        "name": "contract_bundle",
        "path": "contracts/quant-engine/v1/manifest.json",
        "stable_id": "quant-engine-contract-v1",
        "sha256": hs.CONTRACT_BUNDLE_SHA256,
        "bytes": 2521,
        "source_commit": EXTERNAL_EXECUTOR_HANDSHAKE_MERGE_COMMIT,
        "mount": "read_only",
    },
}
EXPECTED_OBSERVABILITY_EVIDENCE: Final[tuple[dict[str, object], ...]] = (
    {
        "id": "logs_present",
        "paths": list(LOGS_REQUIRED),
        "status": "pass",
    },
    {
        "id": "no_productive_runtime_metrics",
        "status": "pass",
        "value": True,
    },
    {
        "id": "artifact_only_output_uri",
        "status": "pass",
        "value": OUTPUT_ARTIFACT_URI,
    },
)
ROLLBACK_STEPS: Final[tuple[str, ...]] = (
    "Keep open_macro_v03 runtime activation flag disabled.",
    "Reject any controlled-shadow artifact with productive side-effect markers.",
    "Preserve artifacts for audit without publishing to allocator or DB.",
    "Continue A5 as blocked until a separate promotion review explicitly changes it.",
)
EXPECTED_DOCKER_BIND_MOUNTS: Final[frozenset[tuple[str, str, str]]] = frozenset(
    {
        ("/input_pack", "/input_pack", "read_only"),
        ("/calibration", "/calibration", "read_only"),
        ("/contracts", "/contracts", "read_only"),
        ("/outputs", "/outputs", "read_write"),
    }
)
DOCKER_OPTIONS_WITH_VALUE: Final[frozenset[str]] = frozenset({"--network", "--net", "--mount"})
ALLOWED_DOCKER_OPTIONS: Final[frozenset[str]] = frozenset({"--rm", "--read-only", "--network", "--net", "--mount"})
ALLOWED_BIND_ATTRS: Final[frozenset[str]] = frozenset({"type", "src", "source", "dst", "destination", "target", "readonly", "ro"})
ALLOWED_BIND_FLAGS: Final[frozenset[str]] = frozenset({"readonly", "ro"})
EXPECTED_NO_SIDE_EFFECT_CHECK_IDS: Final[tuple[str, ...]] = hs.EXPECTED_NO_SIDE_EFFECT_CHECK_IDS
EXPECTED_ACCEPTANCE_RULE_IDS: Final[tuple[str, ...]] = (
    "all_required_outputs_present",
    "no_unexpected_outputs",
    "mismatch_count_zero",
    "no_nan_or_inf",
    "all_constraints_satisfied",
    "invariant_failures_zero",
    "relative_deltas_below_hard_reject_threshold",
    "run_fingerprint_consistent",
    "output_manifest_complete",
    "result_reproducible",
    "immutable_input_pack_hashes_match",
    "immutable_calibration_hashes_match",
    "runtime_activation_false",
    "A5_blocked",
    "freeze_ready_false",
    "official_result_false",
    "allow_db_write_false",
    "allow_allocator_publish_false",
    "production_endpoint_activation_none",
    "backend_no_execution",
    "no_runtime_activation_attempt",
    "no_official_db_write_attempt",
    "no_allocator_publish_attempt",
)
EXPECTED_INVARIANT_CHECK_IDS: Final[tuple[str, ...]] = (
    "runtime_activation_false",
    "A5_blocked",
    "freeze_ready_false",
    "official_result_false",
    "allow_db_write_false",
    "allow_allocator_publish_false",
    "production_endpoint_activation_none",
    "backend_no_execution",
    "input_pack_read_only",
    "calibration_read_only",
    "contract_bundle_read_only",
    "output_dir_only_writable_mount",
    "network_none",
    "db_access_false",
    "source_tree_not_executor_output",
    "mismatch_count_zero",
    "immutable_input_pack_hashes_match",
    "immutable_calibration_hashes_match",
)
LOG_FORBIDDEN_TRUE_KEYS: Final[frozenset[str]] = hs.LOG_FORBIDDEN_TRUE_KEYS
LOG_FORBIDDEN_PRESENCE_KEYS: Final[frozenset[str]] = hs.LOG_FORBIDDEN_PRESENCE_KEYS


class ControlledShadowValidationError(ValueError):
    """Raised when controlled-shadow artifact evidence is invalid."""


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def controlled_shadow_root(root: Path | None = None) -> Path:
    return root or repo_root() / "artifacts" / "shadow" / CONTROLLED_SHADOW_ID


def _reject_duplicate_json_object_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise ControlledShadowValidationError(f"duplicate JSON object key {key!r}")
        payload[key] = value
    return payload


def _reject_non_standard_json_constant(value: str) -> None:
    raise ControlledShadowValidationError(f"non-standard JSON constant {value!r}")


def load_json(path: Path) -> Any:
    try:
        return json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_json_object_keys,
            parse_constant=_reject_non_standard_json_constant,
        )
    except json.JSONDecodeError as exc:
        raise ControlledShadowValidationError(f"invalid JSON in {path}: {exc}") from exc
    except ControlledShadowValidationError as exc:
        raise ControlledShadowValidationError(f"invalid JSON in {path}: {exc}") from exc


def _require_mapping(value: Any, *, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ControlledShadowValidationError(f"{where} must be a JSON object")
    return value


def _reject_unexpected_fields(payload: Mapping[str, Any], allowed: set[str] | frozenset[str], *, where: str) -> None:
    unexpected = sorted(set(payload) - allowed)
    if unexpected:
        raise ControlledShadowValidationError(f"{where}: unexpected fields {unexpected}")


def _require_fields(payload: Mapping[str, Any], fields: tuple[str, ...], *, where: str) -> None:
    missing = [field for field in fields if field not in payload]
    if missing:
        raise ControlledShadowValidationError(f"{where}: missing fields {missing}")


def _json_pin_equal(actual: Any, expected: Any) -> bool:
    if isinstance(actual, bool) or isinstance(expected, bool):
        return isinstance(actual, bool) and isinstance(expected, bool) and actual == expected
    if isinstance(actual, int | float) and isinstance(expected, int | float):
        return type(actual) is type(expected) and actual == expected
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _json_pin_equal(left, right) for left, right in zip(actual, expected)
        )
    if isinstance(actual, Mapping) and isinstance(expected, Mapping):
        return set(actual) == set(expected) and all(
            _json_pin_equal(actual[key], expected[key]) for key in expected
        )
    return type(actual) is type(expected) and actual == expected


def _require_equal(payload: Mapping[str, Any], field: str, expected: Any, *, where: str) -> None:
    actual = payload.get(field)
    if not _json_pin_equal(actual, expected):
        raise ControlledShadowValidationError(
            f"{where}: {field} expected {expected!r}, got {actual!r}"
        )


def _require_pins(payload: Mapping[str, Any], pins: Mapping[str, Any], *, where: str) -> None:
    for field, expected in pins.items():
        _require_equal(payload, field, expected, where=where)


def _ensure_child(path: Path, root: Path) -> Path:
    resolved_root = root.resolve()
    resolved = path.resolve()
    if not resolved.is_relative_to(resolved_root):
        raise ControlledShadowValidationError(f"path escapes controlled shadow root: {path}")
    return resolved


def _parse_log_tokens(line: str, *, source: str = "log") -> dict[str, str]:
    tokens: dict[str, str] = {}
    for raw in line.strip().split():
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        if key in tokens:
            raise ControlledShadowValidationError(f"{source}: duplicate log token {key}")
        tokens[key] = value
    return tokens


def _validate_log(path: Path, *, identity: Mapping[str, str], expected: Mapping[str, str]) -> None:
    if not path.is_file():
        raise ControlledShadowValidationError(f"missing controlled shadow log: {path.name}")
    tokens = _parse_log_tokens(path.read_text(encoding="utf-8"), source=path.name)
    for key, expected_value in {**identity, **expected}.items():
        if tokens.get(key) != expected_value:
            raise ControlledShadowValidationError(
                f"{path.name}: {key} expected {expected_value!r}, got {tokens.get(key)!r}"
            )
    for key in LOG_FORBIDDEN_TRUE_KEYS:
        if tokens.get(key) == "true":
            raise ControlledShadowValidationError(f"{path.name}: forbidden true marker {key}")
    for key in LOG_FORBIDDEN_PRESENCE_KEYS:
        if key in tokens:
            raise ControlledShadowValidationError(f"{path.name}: forbidden marker present {key}")


def _validate_logs(root: Path) -> None:
    _validate_log(
        root / "logs" / "control_plane_validator.log",
        identity={
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "handshake_id": hs.HANDSHAKE_ID,
            "validator": "control_plane",
            "status": "pass",
        },
        expected={
            "runtime_activation": "false",
            "A5": "blocked",
            "freeze_ready": "false",
            "official_result": "false",
            "allow_db_write": "false",
            "allow_allocator_publish": "false",
            "production_endpoint_activation": "none",
            "backend_executes_engine": "false",
            "backend_executes_docker": "false",
            "backend_executes_subprocess": "false",
        },
    )
    _validate_log(
        root / "logs" / "external_executor.log",
        identity={
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "handshake_id": hs.HANDSHAKE_ID,
            "executor": "external",
            "status": "succeeded",
        },
        expected={
            "execution_policy": EXECUTION_POLICY,
            "network": "none",
            "db_access": "false",
            "db_write": "false",
            "allocator_publish": "false",
            "input_pack_mount": "read_only",
            "calibration_mount": "read_only",
            "contract_bundle_mount": "read_only",
            "output_mount": "read_write",
            "writable_mounts": "output",
            "source_tree_writes": "false",
            "runtime_activation": "false",
            "allow_db_write": "false",
            "allow_allocator_publish": "false",
            "official_result": "false",
            "production_endpoint_activation": "none",
            "backend_executes_engine": "false",
            "backend_executes_docker": "false",
            "backend_executes_subprocess": "false",
        },
    )


def _unexpected_files(root: Path) -> list[str]:
    expected = set(FINAL_REQUIRED_FILES)
    actual: list[str] = []
    for path in root.rglob("*"):
        if path.is_file():
            actual.append(path.relative_to(root).as_posix())
    return sorted(set(actual) - expected)


def _file_logical_bytes(path: Path) -> int:
    return len(path.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8"))


def validate_controlled_shadow_manifest(payload: Mapping[str, Any]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset(EXPECTED_CONTROLLED_SHADOW_MANIFEST),
        where="controlled_shadow_manifest",
    )
    _require_pins(payload, EXPECTED_CONTROLLED_SHADOW_MANIFEST, where="controlled_shadow_manifest")


def validate_control_plane_request(payload: Mapping[str, Any]) -> None:
    allowed = frozenset(
        {
            "schema_version",
            "controlled_shadow_id",
            "external_executor_handshake_id",
            "external_executor_handshake_001_merge_commit",
            "request_id",
            "correlation_id",
            "execution_id",
            "runtime_skeleton_id",
            "shadow_id",
            "strategy",
            "mode",
            "as_of",
            "feature_flag_name",
            "feature_flag_default",
            "output_artifact_uri",
            "execution_window",
            "population_scope",
            "expected_run_count",
            "executor_identity",
            "rollback_owner",
            *PROVENANCE_PINS,
            *SIDE_EFFECT_PINS,
            *BACKEND_NO_EXEC_PINS,
        }
    )
    _reject_unexpected_fields(payload, allowed, where="control_plane_request")
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "external_executor_handshake_id": hs.HANDSHAKE_ID,
            "external_executor_handshake_001_merge_commit": EXTERNAL_EXECUTOR_HANDSHAKE_MERGE_COMMIT,
            "request_id": REQUEST_ID,
            "correlation_id": CORRELATION_ID,
            "execution_id": EXECUTION_ID,
            "runtime_skeleton_id": hs.RUNTIME_SKELETON_ID,
            "shadow_id": hs.SHADOW_ID,
            "strategy": "open_macro_v03",
            "mode": "shadow",
            "as_of": hs.ENVELOPE_AS_OF,
            "feature_flag_name": "open_macro_v03_runtime_activation",
            "feature_flag_default": False,
            "output_artifact_uri": OUTPUT_ARTIFACT_URI,
            "expected_run_count": len(EXPECTED_LABELS),
            **PROVENANCE_PINS,
            **SIDE_EFFECT_PINS,
            **BACKEND_NO_EXEC_PINS,
        },
        where="control_plane_request",
    )
    _require_mapping(payload.get("execution_window"), where="control_plane_request.execution_window")
    population_scope = _require_mapping(payload.get("population_scope"), where="control_plane_request.population_scope")
    _require_pins(
        population_scope,
        {"source": "immutable_certified_input_pack"},
        where="control_plane_request.population_scope",
    )
    executor_identity = _require_mapping(payload.get("executor_identity"), where="control_plane_request.executor_identity")
    _require_pins(
        executor_identity,
        {"is_external": True},
        where="control_plane_request.executor_identity",
    )
    if not isinstance(payload.get("rollback_owner"), str) or not payload["rollback_owner"]:
        raise ControlledShadowValidationError("control_plane_request.rollback_owner must be a non-empty string")


def validate_shadow_job_envelope(payload: Mapping[str, Any]) -> None:
    allowed = frozenset(
        {
            "schema_version",
            "controlled_shadow_id",
            "external_executor_handshake_id",
            "request_id",
            "correlation_id",
            "execution_id",
            "run_fingerprint",
            "strategy",
            "mode",
            "as_of",
            "execution_policy",
            "output_artifact_uri",
            "read_only_inputs",
            *PROVENANCE_PINS,
            *SIDE_EFFECT_PINS,
        }
    )
    _reject_unexpected_fields(payload, allowed, where="shadow_job_envelope")
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "external_executor_handshake_id": hs.HANDSHAKE_ID,
            "request_id": REQUEST_ID,
            "correlation_id": CORRELATION_ID,
            "execution_id": EXECUTION_ID,
            "run_fingerprint": hs.RUN_FINGERPRINT,
            "strategy": "open_macro_v03",
            "mode": "shadow",
            "as_of": hs.ENVELOPE_AS_OF,
            "execution_policy": EXECUTION_POLICY,
            "output_artifact_uri": OUTPUT_ARTIFACT_URI,
            **PROVENANCE_PINS,
            **SIDE_EFFECT_PINS,
        },
        where="shadow_job_envelope",
    )
    read_only_inputs = payload.get("read_only_inputs")
    if not isinstance(read_only_inputs, list) or not read_only_inputs:
        raise ControlledShadowValidationError("shadow_job_envelope.read_only_inputs must be a non-empty list")
    by_name: dict[str, Mapping[str, Any]] = {}
    for item in read_only_inputs:
        entry = _require_mapping(item, where="shadow_job_envelope.read_only_inputs[]")
        _reject_unexpected_fields(
            entry,
            frozenset(next(iter(READ_ONLY_INPUT_PINS.values()))),
            where="shadow_job_envelope.read_only_inputs[]",
        )
        name = entry.get("name")
        if not isinstance(name, str):
            raise ControlledShadowValidationError("read_only_inputs[].name must be a string")
        if name in by_name:
            raise ControlledShadowValidationError(f"shadow_job_envelope.read_only_inputs duplicate name: {name}")
        by_name[name] = entry
    if set(by_name) != set(READ_ONLY_INPUT_PINS):
        raise ControlledShadowValidationError("shadow_job_envelope.read_only_inputs identity mismatch")
    for name, expected in READ_ONLY_INPUT_PINS.items():
        _require_pins(by_name[name], expected, where=f"read_only_inputs[{name}]")


def _split_docker_run_policy(policy: list[str]) -> tuple[list[str], str, list[str]]:
    if policy[:2] != ["docker", "run"]:
        raise ControlledShadowValidationError("executor_acceptance.docker_run_policy must start with docker run")

    options: list[str] = []
    index = 2
    while index < len(policy):
        token = policy[index]
        if not token.startswith("-"):
            break

        option_name = token.split("=", 1)[0]
        if option_name not in ALLOWED_DOCKER_OPTIONS:
            raise ControlledShadowValidationError(
                f"executor_acceptance.docker_run_policy forbidden option {option_name}"
            )

        options.append(token)
        if "=" in token:
            index += 1
        elif option_name in DOCKER_OPTIONS_WITH_VALUE:
            if index + 1 >= len(policy):
                raise ControlledShadowValidationError(
                    f"executor_acceptance.docker_run_policy option {token} requires a value"
                )
            options.append(policy[index + 1])
            index += 2
        else:
            index += 1

    if index >= len(policy):
        raise ControlledShadowValidationError("executor_acceptance.docker_run_policy must include pinned image")
    return options, policy[index], policy[index + 1 :]


def _docker_network_values(options: list[str]) -> list[str | None]:
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


def _docker_mount_specs(options: list[str]) -> list[str]:
    specs: list[str] = []
    index = 0
    while index < len(options):
        token = options[index]
        if token == "--mount":
            if index + 1 >= len(options):
                raise ControlledShadowValidationError("executor_acceptance.docker_run_policy --mount requires a value")
            specs.append(options[index + 1])
            index += 2
        elif token.startswith("--mount="):
            specs.append(token.split("=", 1)[1])
            index += 1
        else:
            index += 1
    return specs


def _parse_mount_spec(spec: str) -> tuple[dict[str, str], set[str]]:
    attrs: dict[str, str] = {}
    flags: set[str] = set()
    for raw_part in spec.split(","):
        part = raw_part.strip()
        if not part:
            continue
        key, sep, value = part.partition("=")
        if sep:
            if key in attrs:
                raise ControlledShadowValidationError(
                    f"executor_acceptance.docker_run_policy duplicate mount attribute {key}"
                )
            attrs[key] = value
        else:
            flags.add(part)
    return attrs, flags


def _validate_docker_mount_tokens(options: list[str]) -> None:
    mounts: list[tuple[str, str, str]] = []

    for spec in _docker_mount_specs(options):
        attrs, flags = _parse_mount_spec(spec)
        unexpected_attrs = sorted(set(attrs) - ALLOWED_BIND_ATTRS)
        unexpected_flags = sorted(flags - ALLOWED_BIND_FLAGS)
        if unexpected_attrs or unexpected_flags:
            raise ControlledShadowValidationError(
                "executor_acceptance.docker_run_policy bind mounts include unsupported options"
            )
        if attrs.get("type") != "bind":
            raise ControlledShadowValidationError("executor_acceptance.docker_run_policy mounts must be bind mounts")

        source = attrs.get("src") or attrs.get("source")
        target = attrs.get("dst") or attrs.get("destination") or attrs.get("target")
        if not source or not target:
            raise ControlledShadowValidationError(
                "executor_acceptance.docker_run_policy bind mounts must include source and destination"
            )

        readonly = (
            "readonly" in flags
            or "ro" in flags
            or attrs.get("readonly") in {"true", "1"}
            or attrs.get("ro") in {"true", "1"}
        )
        mounts.append((source, target, "read_only" if readonly else "read_write"))

    if len(mounts) != len(set(mounts)):
        raise ControlledShadowValidationError("executor_acceptance.docker_run_policy duplicate bind mounts")
    if set(mounts) != EXPECTED_DOCKER_BIND_MOUNTS:
        raise ControlledShadowValidationError(
            f"executor_acceptance.docker_run_policy bind mounts mismatch: {sorted(mounts)}"
        )


def _validate_docker_run_policy(policy: Any, *, expected_image_digest: str) -> None:
    if not isinstance(policy, list) or any(not isinstance(token, str) for token in policy):
        raise ControlledShadowValidationError("executor_acceptance.docker_run_policy must be a token list")

    options, image, command_args = _split_docker_run_policy(policy)
    if _docker_network_values(options) != ["none"]:
        raise ControlledShadowValidationError("executor_acceptance.docker_run_policy must require --network none")
    if "--read-only" not in options:
        raise ControlledShadowValidationError("executor_acceptance.docker_run_policy must require --read-only")
    if command_args:
        raise ControlledShadowValidationError("executor_acceptance.docker_run_policy must not override image command")
    if "@" not in image:
        raise ControlledShadowValidationError("executor_acceptance.docker_run_policy image must be pinned by digest")
    image_digest = image.rsplit("@", 1)[1]
    if image_digest != expected_image_digest:
        raise ControlledShadowValidationError("executor_acceptance.docker_run_policy image digest mismatch")
    _validate_docker_mount_tokens(options)


def validate_executor_acceptance(payload: Mapping[str, Any], envelope: Mapping[str, Any]) -> None:
    allowed = frozenset(
        {
            "schema_version",
            "controlled_shadow_id",
            "external_executor_handshake_id",
            "status",
            "accepted",
            "request_id",
            "correlation_id",
            "execution_id",
            "run_fingerprint",
            "provenance_match",
            "execution_policy",
            "docker_network",
            "docker_run_policy",
            "input_pack_mount",
            "calibration_mount",
            "contract_bundle_mount",
            "output_mount",
            "writable_mounts",
            "mounts",
            "requires_docker_network_none",
            *PROVENANCE_PINS,
            *SIDE_EFFECT_PINS,
            *BACKEND_NO_EXEC_PINS,
        }
    )
    _reject_unexpected_fields(payload, allowed, where="executor_acceptance")
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "external_executor_handshake_id": hs.HANDSHAKE_ID,
            "status": "accepted",
            "accepted": True,
            "request_id": REQUEST_ID,
            "correlation_id": CORRELATION_ID,
            "execution_id": EXECUTION_ID,
            "run_fingerprint": hs.RUN_FINGERPRINT,
            "provenance_match": True,
            "execution_policy": EXECUTION_POLICY,
            "docker_network": "none",
            "input_pack_mount": "read_only",
            "calibration_mount": "read_only",
            "contract_bundle_mount": "read_only",
            "output_mount": "read_write",
            "writable_mounts": ["output"],
            "requires_docker_network_none": True,
            **PROVENANCE_PINS,
            **SIDE_EFFECT_PINS,
            **BACKEND_NO_EXEC_PINS,
        },
        where="executor_acceptance",
    )
    for key in ("request_id", "correlation_id", "execution_id", "run_fingerprint"):
        if payload.get(key) != envelope.get(key):
            raise ControlledShadowValidationError(f"executor_acceptance: envelope mismatch for {key}")
    mounts = payload.get("mounts")
    if not isinstance(mounts, list):
        raise ControlledShadowValidationError("executor_acceptance.mounts must be a list")
    expected_mounts = {
        "input_pack": "read_only",
        "calibration": "read_only",
        "contract_bundle": "read_only",
        "output": "read_write",
    }
    actual_mounts: dict[str, str] = {}
    for item in mounts:
        entry = _require_mapping(item, where="executor_acceptance.mounts[]")
        name = entry.get("name")
        mode = entry.get("mode")
        if isinstance(name, str) and isinstance(mode, str):
            if name in actual_mounts:
                raise ControlledShadowValidationError(f"executor_acceptance.mounts duplicate name: {name}")
            actual_mounts[name] = mode
    if actual_mounts != expected_mounts:
        raise ControlledShadowValidationError("executor_acceptance.mounts mismatch")
    engine_image_digest = payload.get("engine_image_digest")
    if not isinstance(engine_image_digest, str):
        raise ControlledShadowValidationError("executor_acceptance.engine_image_digest must be a string")
    _validate_docker_run_policy(payload.get("docker_run_policy"), expected_image_digest=engine_image_digest)


def validate_baseline_comparison(payload: Mapping[str, Any]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset(
            {
                "schema_version",
                "controlled_shadow_id",
                "shadow_id",
                "calibration_id",
                "policy_id",
                "status",
                "hash_comparison",
                "numeric_tolerances",
                "divergence_summary",
                "materiality_summary",
                "forbidden_effects",
                "evaluation",
            }
        ),
        where="baseline_comparison",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "shadow_id": hs.SHADOW_ID,
            "calibration_id": hs.CALIBRATION_ID,
            "policy_id": "open_macro_v03_shadow_baseline_comparison_policy_v1",
            "status": "pass",
            "hash_comparison": "exact",
            "numeric_tolerances": {
                "float_abs_tolerance": 1e-12,
                "float_rel_tolerance": 1e-10,
                "hash_comparison": "exact",
            },
        },
        where="baseline_comparison",
    )
    divergence = _require_mapping(payload.get("divergence_summary"), where="baseline_comparison.divergence_summary")
    _require_pins(divergence, DIVERGENCE_ZERO_PINS, where="baseline_comparison.divergence_summary")
    materiality = _require_mapping(payload.get("materiality_summary"), where="baseline_comparison.materiality_summary")
    _require_pins(materiality, MATERIALITY_PINS, where="baseline_comparison.materiality_summary")
    for field, value in materiality.items():
        if field.endswith("_pct") and (not isinstance(value, (int, float)) or not math.isfinite(value)):
            raise ControlledShadowValidationError(f"baseline_comparison.materiality_summary.{field} must be finite")
    forbidden = _require_mapping(payload.get("forbidden_effects"), where="baseline_comparison.forbidden_effects")
    forbidden_effect_pins = {
        "runtime_activation_attempt": False,
        "official_db_write_attempt": False,
        "allocator_publish_attempt": False,
        "production_endpoint_activation_attempt": False,
        "input_pack_change": "none",
        "calibration_pack_change": "none",
        "contract_v1_change": "none",
        "formula_change": "none",
    }
    _reject_unexpected_fields(
        forbidden,
        frozenset(forbidden_effect_pins),
        where="baseline_comparison.forbidden_effects",
    )
    _require_pins(
        forbidden,
        forbidden_effect_pins,
        where="baseline_comparison.forbidden_effects",
    )
    evaluation = _require_mapping(payload.get("evaluation"), where="baseline_comparison.evaluation")
    _require_pins(
        evaluation,
        {
            "status": "pass",
            "material_divergence": False,
            "rejection_rules_triggered": [],
            "review_rules_triggered": [],
        },
        where="baseline_comparison.evaluation",
    )


def validate_invariant_report(payload: Mapping[str, Any]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset({"schema_version", "controlled_shadow_id", "shadow_id", "calibration_id", "status", "ok", "checks", "failure_classes"}),
        where="invariant_report",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "shadow_id": hs.SHADOW_ID,
            "calibration_id": hs.CALIBRATION_ID,
            "status": "pass",
            "ok": True,
            "failure_classes": [],
        },
        where="invariant_report",
    )
    checks = _require_mapping(payload.get("checks"), where="invariant_report.checks")
    if set(checks) != set(EXPECTED_INVARIANT_CHECK_IDS):
        raise ControlledShadowValidationError("invariant_report.checks mismatch")
    for check_id, value in checks.items():
        if value is not True:
            raise ControlledShadowValidationError(f"invariant_report.checks.{check_id} must be true")


def validate_reproducibility_report(payload: Mapping[str, Any]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset(
            {
                "schema_version",
                "artifact_type",
                "controlled_shadow_id",
                "external_executor_handshake_id",
                "shadow_id",
                "calibration_id",
                "input_pack_sha256",
                "calibration_config_sha256",
                "calibration_run_matrix_sha256",
                "contract_bundle_sha256",
                "run_fingerprint",
                "expected_run_count",
                "run_count",
                "expected_labels",
                "missing",
                "unexpected",
                "missing_hashes",
                "unexpected_hashes",
                "output_manifest_sha256_by_run",
                "semantic_run_fingerprint_policy",
                "run_hash_mismatches",
                "duplicates",
                "mismatch_count",
                "container_runs",
                "host_runs",
                "jobs_matrix",
                "repeat_runs_per_mode",
                "network",
                "db_access",
                "input_pack_mount",
                "calibration_mount",
                "contract_bundle_mount",
                "output_mount",
                "writable_mounts",
                "path_independence",
                "docker_image_digest",
                "expected_docker_image_digest",
                "docker_image_provenance_ok",
                "ok",
            }
        ),
        where="reproducibility_report",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "artifact_type": "controlled_shadow_reproducibility_report",
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "external_executor_handshake_id": hs.HANDSHAKE_ID,
            "shadow_id": hs.SHADOW_ID,
            "calibration_id": hs.CALIBRATION_ID,
            "input_pack_sha256": hs.INPUT_PACK_SHA256,
            "calibration_config_sha256": hs.CALIBRATION_CONFIG_SHA256,
            "calibration_run_matrix_sha256": CALIBRATION_RUN_MATRIX_SHA256,
            "contract_bundle_sha256": hs.CONTRACT_BUNDLE_SHA256,
            "run_fingerprint": hs.RUN_FINGERPRINT,
            "expected_run_count": len(EXPECTED_LABELS),
            "run_count": len(EXPECTED_LABELS),
            "expected_labels": list(EXPECTED_LABELS),
            "missing": [],
            "unexpected": [],
            "missing_hashes": [],
            "unexpected_hashes": [],
            "output_manifest_sha256_by_run": {
                label: CALIBRATION_OUTPUT_MANIFEST_SHA256 for label in EXPECTED_LABELS
            },
            "semantic_run_fingerprint_policy": hs.SEMANTIC_RUN_FINGERPRINT_POLICY,
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
            "contract_bundle_mount": "read_only",
            "output_mount": "read_write",
            "writable_mounts": ["output"],
            "path_independence": True,
            "docker_image_digest": hs.ENGINE_IMAGE_DIGEST,
            "expected_docker_image_digest": hs.ENGINE_IMAGE_DIGEST,
            "docker_image_provenance_ok": True,
            "ok": True,
        },
        where="reproducibility_report",
    )


def validate_no_side_effects_report(payload: Mapping[str, Any]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset(
            {
                "schema_version",
                "controlled_shadow_id",
                "external_executor_handshake_id",
                "status",
                "db_write_mode",
                "allocator_impact",
                "production_impact",
                "checks",
                *SIDE_EFFECT_PINS,
                *BACKEND_NO_EXEC_PINS,
            }
        ),
        where="no_side_effects_report",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "external_executor_handshake_id": hs.HANDSHAKE_ID,
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
    if not isinstance(checks, list):
        raise ControlledShadowValidationError("no_side_effects_report.checks must be a list")
    seen: dict[str, Mapping[str, Any]] = {}
    for item in checks:
        entry = _require_mapping(item, where="no_side_effects_report.check")
        _reject_unexpected_fields(
            entry,
            hs.NO_SIDE_EFFECTS_REPORT_CHECK_FIELDS,
            where="no_side_effects_report.check",
        )
        check_id = entry.get("id")
        if not isinstance(check_id, str):
            raise ControlledShadowValidationError("no_side_effects_report.check.id must be a string")
        seen[check_id] = entry
        _require_equal(entry, "allowed", False, where=f"no_side_effects_report.check[{check_id}]")
        _require_equal(entry, "status", "pass", where=f"no_side_effects_report.check[{check_id}]")
    if set(seen) != set(EXPECTED_NO_SIDE_EFFECT_CHECK_IDS):
        raise ControlledShadowValidationError("no_side_effects_report.check ids mismatch")


def validate_acceptance_report(payload: Mapping[str, Any]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset(
            {
                "schema_version",
                "controlled_shadow_id",
                "external_executor_handshake_id",
                "shadow_id",
                "calibration_id",
                "status",
                "promotion_review_status",
                "rules",
                *SIDE_EFFECT_PINS,
                *BACKEND_NO_EXEC_PINS,
            }
        ),
        where="acceptance_report",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "external_executor_handshake_id": hs.HANDSHAKE_ID,
            "shadow_id": hs.SHADOW_ID,
            "calibration_id": hs.CALIBRATION_ID,
            "status": "artifact_gate_passed_a5_blocked",
            "promotion_review_status": "not_started_a5_blocked",
            **SIDE_EFFECT_PINS,
            **BACKEND_NO_EXEC_PINS,
        },
        where="acceptance_report",
    )
    rules = payload.get("rules")
    if not isinstance(rules, list):
        raise ControlledShadowValidationError("acceptance_report.rules must be a list")
    seen: dict[str, Mapping[str, Any]] = {}
    for item in rules:
        entry = _require_mapping(item, where="acceptance_report.rule")
        rule_id = entry.get("id")
        if not isinstance(rule_id, str):
            raise ControlledShadowValidationError("acceptance_report.rule.id must be a string")
        seen[rule_id] = entry
        _require_equal(entry, "status", "pass", where=f"acceptance_report.rule[{rule_id}]")
    if set(seen) != set(EXPECTED_ACCEPTANCE_RULE_IDS):
        raise ControlledShadowValidationError("acceptance_report.rule ids mismatch")


def validate_observability_evidence(payload: Mapping[str, Any]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset(
            {
                "schema_version",
                "artifact_type",
                "controlled_shadow_id",
                "external_executor_handshake_id",
                "shadow_id",
                "status",
                "runtime_activation",
                "evidence",
            }
        ),
        where="observability_evidence",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "artifact_type": "controlled_shadow_observability_evidence",
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "external_executor_handshake_id": hs.HANDSHAKE_ID,
            "shadow_id": hs.SHADOW_ID,
            "status": "pass",
            "runtime_activation": False,
            "evidence": list(EXPECTED_OBSERVABILITY_EVIDENCE),
        },
        where="observability_evidence",
    )


def validate_rollback_evidence(payload: Mapping[str, Any]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset(
            {
                "schema_version",
                "controlled_shadow_id",
                "external_executor_handshake_id",
                "shadow_id",
                "status",
                "rollback_owner",
                "rollback_steps",
                *SIDE_EFFECT_PINS,
            }
        ),
        where="rollback_evidence",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "external_executor_handshake_id": hs.HANDSHAKE_ID,
            "shadow_id": hs.SHADOW_ID,
            "status": "ready_no_runtime_teardown_required",
            "rollback_owner": "quant-engine-governance",
            "rollback_steps": list(ROLLBACK_STEPS),
            **SIDE_EFFECT_PINS,
        },
        where="rollback_evidence",
    )


def validate_output_manifest(root: Path, payload: Mapping[str, Any]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset({"schema_version", "artifact_type", "controlled_shadow_id", "shadow_id", "status", "logs_required", "unexpected_outputs", "artifacts"}),
        where="output_manifest",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "artifact_type": "controlled_shadow_output_manifest",
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "shadow_id": hs.SHADOW_ID,
            "status": "succeeded",
            "logs_required": list(LOGS_REQUIRED),
            "unexpected_outputs": [],
        },
        where="output_manifest",
    )
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        raise ControlledShadowValidationError("output_manifest.artifacts must be a list")
    by_path: dict[str, Mapping[str, Any]] = {}
    for item in artifacts:
        entry = _require_mapping(item, where="output_manifest.artifacts[]")
        rel = entry.get("path")
        if not isinstance(rel, str):
            raise ControlledShadowValidationError("output_manifest.artifacts[].path must be a string")
        if rel in by_path:
            raise ControlledShadowValidationError(f"output_manifest duplicate artifact path: {rel}")
        by_path[rel] = entry
    if set(by_path) != set(OUTPUT_ARTIFACT_PATHS):
        raise ControlledShadowValidationError("output_manifest artifact set mismatch")
    unexpected = _unexpected_files(root)
    if unexpected:
        raise ControlledShadowValidationError(f"unexpected controlled shadow files on disk: {unexpected}")
    for rel in OUTPUT_ARTIFACT_PATHS:
        entry = by_path[rel]
        path = _ensure_child(root / rel, root)
        if not path.is_file():
            raise ControlledShadowValidationError(f"output_manifest artifact missing: {rel}")
        _require_equal(entry, "sha256", hs.file_sha256(path), where=f"output_manifest[{rel}]")
        _require_equal(entry, "bytes", _file_logical_bytes(path), where=f"output_manifest[{rel}]")
    _validate_logs(root)


def validate_shadow_result_manifest(payload: Mapping[str, Any], *, evidence_hashes: Mapping[str, str]) -> None:
    _reject_unexpected_fields(
        payload,
        frozenset(
            {
                "schema_version",
                "controlled_shadow_id",
                "shadow_id",
                "calibration_id",
                "request_id",
                "correlation_id",
                "execution_id",
                "run_fingerprint",
                "status",
                "retryable",
                "retry_count",
                "started_at",
                "finished_at",
                "duration_ms",
                "memory_peak_bytes",
                "cpu_time_ms",
                "output_artifact_uri",
                "divergence_summary",
                "materiality_summary",
                "output_manifest_sha256",
                "baseline_comparison_sha256",
                "invariant_report_sha256",
                "reproducibility_report_sha256",
                "no_side_effects_report_sha256",
                *RESULT_PROVENANCE_PINS,
                *SIDE_EFFECT_PINS,
            }
        ),
        where="shadow_result_manifest",
    )
    _require_pins(
        payload,
        {
            "schema_version": 1,
            "controlled_shadow_id": CONTROLLED_SHADOW_ID,
            "shadow_id": hs.SHADOW_ID,
            "calibration_id": hs.CALIBRATION_ID,
            "request_id": REQUEST_ID,
            "correlation_id": CORRELATION_ID,
            "execution_id": EXECUTION_ID,
            "run_fingerprint": hs.RUN_FINGERPRINT,
            "status": "succeeded",
            "retryable": False,
            "retry_count": 0,
            "output_artifact_uri": OUTPUT_ARTIFACT_URI,
            **RESULT_PROVENANCE_PINS,
            **SIDE_EFFECT_PINS,
        },
        where="shadow_result_manifest",
    )
    for field, expected_hash in evidence_hashes.items():
        _require_equal(payload, field, expected_hash, where="shadow_result_manifest")
    started = _parse_timestamp(str(payload.get("started_at")), where="shadow_result_manifest.started_at")
    finished = _parse_timestamp(str(payload.get("finished_at")), where="shadow_result_manifest.finished_at")
    if finished <= started:
        raise ControlledShadowValidationError("shadow_result_manifest.finished_at must be after started_at")
    duration_ms = payload.get("duration_ms")
    if not isinstance(duration_ms, int) or duration_ms <= 0:
        raise ControlledShadowValidationError("shadow_result_manifest.duration_ms must be positive")
    actual_duration_ms = int((finished - started).total_seconds() * 1000)
    if duration_ms != actual_duration_ms:
        raise ControlledShadowValidationError("shadow_result_manifest.duration_ms does not match timestamps")
    for field in ("memory_peak_bytes", "cpu_time_ms"):
        value = payload.get(field)
        if not isinstance(value, int) or value < 0:
            raise ControlledShadowValidationError(f"shadow_result_manifest.{field} must be a non-negative integer")
    divergence = _require_mapping(payload.get("divergence_summary"), where="shadow_result_manifest.divergence_summary")
    _require_pins(divergence, DIVERGENCE_ZERO_PINS, where="shadow_result_manifest.divergence_summary")
    materiality = _require_mapping(payload.get("materiality_summary"), where="shadow_result_manifest.materiality_summary")
    _require_pins(materiality, MATERIALITY_PINS, where="shadow_result_manifest.materiality_summary")


def _parse_timestamp(value: str, *, where: str) -> dt.datetime:
    if not hs.UTC_Z_TIMESTAMP_RE.match(value):
        raise ControlledShadowValidationError(f"{where} must be a UTC Z timestamp")
    try:
        return dt.datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ControlledShadowValidationError(f"{where} must be calendar-valid") from exc


def validate_immutable_inputs(workspace_root: Path | None = None) -> dict[str, Any]:
    root = workspace_root or repo_root()
    input_pack_dir = root / "fixtures" / "input_packs" / "golden" / "certified_input_pack"
    pack_verification = verify_pack(input_pack_dir)
    if not pack_verification.get("ok"):
        raise ControlledShadowValidationError("input pack verification failed")
    input_manifest = _require_mapping(load_json(input_pack_dir / "manifest.json"), where="input_pack.manifest")
    _require_pins(
        input_manifest,
        {
            "input_pack_id": hs.INPUT_PACK_ID,
            "input_pack_sha256": hs.INPUT_PACK_SHA256,
            "contract_bundle_sha256": hs.CONTRACT_BUNDLE_SHA256,
            "runtime_activation": False,
        },
        where="input_pack.manifest",
    )

    calibration_dir = root / "artifacts" / "calibration" / hs.CALIBRATION_ID
    calibration_manifest = _require_mapping(
        load_json(calibration_dir / "calibration_manifest.json"),
        where="calibration_manifest",
    )
    _require_pins(
        calibration_manifest,
        {
            "calibration_id": hs.CALIBRATION_ID,
            "input_pack_id": hs.INPUT_PACK_ID,
            "input_pack_sha256": hs.INPUT_PACK_SHA256,
            "calibration_config_sha256": hs.CALIBRATION_CONFIG_SHA256,
            "run_matrix_sha256": CALIBRATION_RUN_MATRIX_SHA256,
            "output_manifest_sha256": CALIBRATION_OUTPUT_MANIFEST_SHA256,
            "runtime_activation": False,
            "A5": "blocked",
            "freeze_ready": False,
            "contract_bundle_sha256": hs.CONTRACT_BUNDLE_SHA256,
            "engine_commit": hs.ENGINE_COMMIT,
        },
        where="calibration_manifest",
    )
    if canonical_file_sha256(calibration_dir / "calibration_config.json") != hs.CALIBRATION_CONFIG_SHA256:
        raise ControlledShadowValidationError("calibration_config_sha256 mismatch")
    if canonical_file_sha256(calibration_dir / "run_matrix.json") != CALIBRATION_RUN_MATRIX_SHA256:
        raise ControlledShadowValidationError("run_matrix_sha256 mismatch")
    if canonical_file_sha256(calibration_dir / "output_manifest.json") != CALIBRATION_OUTPUT_MANIFEST_SHA256:
        raise ControlledShadowValidationError("calibration output_manifest_sha256 mismatch")

    contract_bundle_dir = root / "contracts" / "quant-engine" / "v1"
    contract_verification = verify_bundle(contract_bundle_dir)
    if not contract_verification.get("ok"):
        raise ControlledShadowValidationError("contract bundle verification failed")
    if contract_verification.get("bundle_sha256") != f"sha256:{hs.CONTRACT_BUNDLE_SHA256}":
        raise ControlledShadowValidationError("contract bundle_sha256 mismatch")
    return {
        "input_pack_id": hs.INPUT_PACK_ID,
        "input_pack_sha256": hs.INPUT_PACK_SHA256,
        "calibration_id": hs.CALIBRATION_ID,
        "calibration_config_sha256": hs.CALIBRATION_CONFIG_SHA256,
        "calibration_run_matrix_sha256": CALIBRATION_RUN_MATRIX_SHA256,
        "contract_bundle_sha256": hs.CONTRACT_BUNDLE_SHA256,
        "verified": True,
    }


def verify_controlled_shadow(
    root: Path | None = None,
    *,
    workspace_root: Path | None = None,
) -> dict[str, Any]:
    bundle_root = controlled_shadow_root(root)
    hs.reject_symlinks(bundle_root)
    missing = [rel for rel in FINAL_REQUIRED_FILES if not (bundle_root / rel).is_file()]
    if missing:
        raise ControlledShadowValidationError(f"missing controlled shadow artifact: {missing[0]}")

    manifest = _require_mapping(load_json(bundle_root / "controlled_shadow_manifest.json"), where="controlled_shadow_manifest")
    request = _require_mapping(load_json(bundle_root / "control_plane_request.json"), where="control_plane_request")
    envelope = _require_mapping(load_json(bundle_root / "shadow_job_envelope.json"), where="shadow_job_envelope")
    acceptance = _require_mapping(load_json(bundle_root / "executor_acceptance.json"), where="executor_acceptance")
    baseline = _require_mapping(load_json(bundle_root / "baseline_comparison.json"), where="baseline_comparison")
    invariant = _require_mapping(load_json(bundle_root / "invariant_report.json"), where="invariant_report")
    reproducibility = _require_mapping(load_json(bundle_root / "reproducibility_report.json"), where="reproducibility_report")
    no_side_effects = _require_mapping(load_json(bundle_root / "no_side_effects_report.json"), where="no_side_effects_report")
    acceptance_report = _require_mapping(load_json(bundle_root / "acceptance_report.json"), where="acceptance_report")
    observability = _require_mapping(load_json(bundle_root / "observability_evidence.json"), where="observability_evidence")
    rollback = _require_mapping(load_json(bundle_root / "rollback_evidence.json"), where="rollback_evidence")
    output_manifest = _require_mapping(load_json(bundle_root / "output_manifest.json"), where="output_manifest")
    result = _require_mapping(load_json(bundle_root / "shadow_result_manifest.json"), where="shadow_result_manifest")

    validate_controlled_shadow_manifest(manifest)
    validate_control_plane_request(request)
    validate_shadow_job_envelope(envelope)
    validate_executor_acceptance(acceptance, envelope)
    validate_baseline_comparison(baseline)
    validate_invariant_report(invariant)
    validate_reproducibility_report(reproducibility)
    validate_no_side_effects_report(no_side_effects)
    validate_acceptance_report(acceptance_report)
    validate_observability_evidence(observability)
    validate_rollback_evidence(rollback)
    validate_output_manifest(bundle_root, output_manifest)
    validate_shadow_result_manifest(
        result,
        evidence_hashes={
            "output_manifest_sha256": hs.file_sha256(bundle_root / "output_manifest.json"),
            "baseline_comparison_sha256": hs.file_sha256(bundle_root / "baseline_comparison.json"),
            "invariant_report_sha256": hs.file_sha256(bundle_root / "invariant_report.json"),
            "reproducibility_report_sha256": hs.file_sha256(bundle_root / "reproducibility_report.json"),
            "no_side_effects_report_sha256": hs.file_sha256(bundle_root / "no_side_effects_report.json"),
        },
    )
    immutable = validate_immutable_inputs(workspace_root)

    return {
        "controlled_shadow_id": CONTROLLED_SHADOW_ID,
        "external_executor_handshake_id": hs.HANDSHAKE_ID,
        "status": "validated",
        "runtime_activation": False,
        "A5": "blocked",
        "freeze_ready": False,
        "official_result": False,
        "allow_db_write": False,
        "allow_allocator_publish": False,
        "production_endpoint_activation": "none",
        "backend_runtime_execution": "none",
        "allocator_impact": "none",
        "production_impact": "none",
        "mismatch_count": 0,
        "immutable_inputs": immutable,
        "validated": True,
    }
