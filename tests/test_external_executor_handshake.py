from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
from copy import deepcopy
from pathlib import Path

import jsonschema
import pytest

from src import external_executor_handshake as hs

ROOT = Path(__file__).resolve().parents[1]
HANDSHAKE_ROOT = ROOT / "artifacts" / "handshake" / hs.HANDSHAKE_ID
SHADOW_ROOT = ROOT / "artifacts" / "shadow" / hs.SHADOW_ID
GITHUB_ACTIONS_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact(name: str) -> dict:
    return _json(HANDSHAKE_ROOT / name)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _refresh_manifest_entry(root: Path, manifest: dict, rel: str) -> None:
    for entry in manifest["artifacts"]:
        if entry["path"] == rel:
            entry["sha256"] = hs.file_sha256(root / rel)
            entry["bytes"] = hs.file_logical_bytes(root / rel)
            return
    raise AssertionError(f"missing manifest entry for {rel}")


def _manifest_entry(manifest: dict, rel: str) -> dict:
    for entry in manifest["artifacts"]:
        if entry["path"] == rel:
            return entry
    raise AssertionError(f"missing manifest entry for {rel}")


def test_external_executor_handshake_artifacts_verify_offline() -> None:
    result = hs.verify_handshake(HANDSHAKE_ROOT)

    assert result == {
        "handshake_id": hs.HANDSHAKE_ID,
        "status": "validated",
        "runtime_activation": False,
        "A5": "blocked",
        "official_result": False,
        "backend_runtime_execution": "none",
        "allocator_impact": "none",
        "production_impact": "none",
        "validated": True,
    }


def test_output_manifest_sha256_entries_are_raw_file_hashes() -> None:
    manifest = _artifact("output_manifest.json")
    rel = "control_plane_request.json"
    path = HANDSHAKE_ROOT / rel
    entry = _manifest_entry(manifest, rel)

    assert entry["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert entry["sha256"] == hs.file_sha256(path)


def test_shadow_job_envelope_validates_against_shadow_schema() -> None:
    schema = _json(SHADOW_ROOT / "shadow_job_envelope.schema.json")
    envelope = _artifact("shadow_job_envelope.json")

    jsonschema.validate(envelope, schema)
    hs.validate_shadow_job_envelope(envelope)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("official_result", True),
        ("runtime_activation_attempt", True),
        ("unexpected_envelope_property", "value"),
    ],
)
def test_shadow_job_envelope_rejects_unexpected_fields(field: str, bad: object) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    envelope[field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_shadow_job_envelope(envelope)


def test_shadow_job_envelope_allows_valid_optional_output_manifest_hash() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    envelope["output_manifest_sha256"] = "a" * 64

    hs.validate_shadow_job_envelope(envelope)


def test_shadow_job_envelope_binds_optional_output_manifest_hash() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    envelope["output_manifest_sha256"] = hs.file_sha256(HANDSHAKE_ROOT / "output_manifest.json")

    hs.validate_shadow_job_envelope_output_manifest_binding(
        envelope,
        HANDSHAKE_ROOT / "output_manifest.json",
    )


def test_verify_handshake_rejects_stale_envelope_output_manifest_hash(tmp_path: Path) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    envelope = _json(root / "shadow_job_envelope.json")
    envelope["output_manifest_sha256"] = "0" * 64
    _write_json(root / "shadow_job_envelope.json", envelope)
    output_manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, output_manifest, "shadow_job_envelope.json")
    _write_json(root / "output_manifest.json", output_manifest)
    result = _json(root / "shadow_result_manifest.json")
    result["output_manifest_sha256"] = hs.file_sha256(root / "output_manifest.json")
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(hs.HandshakeValidationError, match="output_manifest_sha256"):
        hs.verify_handshake(root)


def test_shadow_result_manifest_validates_against_shadow_schema() -> None:
    schema = _json(SHADOW_ROOT / "shadow_result_manifest.schema.json")
    result = _artifact("shadow_result_manifest.json")

    jsonschema.validate(result, schema)
    hs.validate_shadow_result_manifest(
        result,
        evidence_hashes={
            field: hs.file_sha256(HANDSHAKE_ROOT / rel)
            for field, rel in hs.SHADOW_RESULT_EVIDENCE_HASH_FILES.items()
        },
    )


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation", True),
        ("allow_db_write", True),
        ("allow_allocator_publish", True),
        ("production_endpoint_activation", "public"),
        ("official_result", True),
        ("engine_commit", "0" * 40),
        ("engine_image_digest", "sha256:" + "0" * 64),
        ("input_pack_sha256", "0" * 64),
        ("calibration_config_sha256", "1" * 64),
        ("contract_bundle_sha256", "2" * 64),
        ("runtime_skeleton_merge_commit", "0" * 40),
    ],
)
def test_control_plane_request_rejects_activation_and_provenance_drift(
    field: str, bad: object
) -> None:
    request = _artifact("control_plane_request.json")
    request[field] = bad

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_control_plane_request(request)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation_attempt", True),
        ("unexpected_request_property", "value"),
    ],
)
def test_control_plane_request_rejects_unexpected_fields(field: str, bad: object) -> None:
    request = _artifact("control_plane_request.json")
    request[field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_control_plane_request(request)


def test_verify_handshake_rejects_unexpected_request_fields_after_hash_refresh(
    tmp_path: Path,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    request = _json(root / "control_plane_request.json")
    request["runtime_activation_attempt"] = True
    _write_json(root / "control_plane_request.json", request)
    output_manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, output_manifest, "control_plane_request.json")
    _write_json(root / "output_manifest.json", output_manifest)
    result = _json(root / "shadow_result_manifest.json")
    result["output_manifest_sha256"] = hs.file_sha256(root / "output_manifest.json")
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.verify_handshake(root)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation", True),
        ("allow_db_write", True),
        ("allow_allocator_publish", True),
        ("production_endpoint_activation", "public"),
        ("engine_commit", "0" * 40),
        ("engine_image_digest", "sha256:" + "0" * 64),
        ("input_pack_sha256", "0" * 64),
        ("calibration_config_sha256", "1" * 64),
        ("contract_bundle_sha256", "2" * 64),
        ("output_artifact_uri", "artifact://other/path"),
    ],
)
def test_shadow_job_envelope_rejects_activation_and_provenance_drift(
    field: str, bad: object
) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    envelope[field] = bad

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_shadow_job_envelope(envelope)


def test_handshake_manifest_pins_runtime_skeleton_merge_commit() -> None:
    manifest = _artifact("handshake_manifest.json")
    hs.validate_handshake_manifest(manifest)

    missing = deepcopy(manifest)
    del missing["runtime_skeleton_merge_commit"]
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_handshake_manifest(missing)

    spoofed = deepcopy(manifest)
    spoofed["runtime_skeleton_merge_commit"] = "0" * 40
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_handshake_manifest(spoofed)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation_attempt", True),
        ("backend_runtime_execution", "docker"),
    ],
)
def test_handshake_manifest_rejects_unexpected_fields(field: str, bad: object) -> None:
    manifest = _artifact("handshake_manifest.json")
    manifest[field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_handshake_manifest(manifest)


def test_verify_handshake_rejects_unexpected_handshake_manifest_fields(
    tmp_path: Path,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    manifest = _json(root / "handshake_manifest.json")
    manifest["backend_runtime_execution"] = "docker"
    _write_json(root / "handshake_manifest.json", manifest)

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.verify_handshake(root)


def test_executor_acceptance_rejects_provenance_mismatch() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance["engine_commit"] = "0" * 40

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("docker_network", "bridge"),
        ("input_pack_mount", "read_write"),
        ("calibration_mount", "read_write"),
        ("output_mount", "read_only"),
        ("writable_mounts", ["input_pack", "output"]),
    ],
)
def test_executor_acceptance_rejects_mount_policy_drift(field: str, bad: object) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance[field] = bad

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation_attempt", True),
        ("official_db_write_attempt", True),
    ],
)
def test_executor_acceptance_rejects_unexpected_fields(field: str, bad: object) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance[field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_executor_acceptance(acceptance, envelope)


def test_verify_handshake_rejects_unexpected_executor_acceptance_fields_after_hash_refresh(
    tmp_path: Path,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    acceptance = _json(root / "executor_acceptance.json")
    acceptance["official_db_write_attempt"] = True
    _write_json(root / "executor_acceptance.json", acceptance)
    output_manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, output_manifest, "executor_acceptance.json")
    _write_json(root / "output_manifest.json", output_manifest)
    result = _json(root / "shadow_result_manifest.json")
    result["output_manifest_sha256"] = hs.file_sha256(root / "output_manifest.json")
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.verify_handshake(root)


def test_executor_acceptance_requires_docker_network_none() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    policy[policy.index("none")] = "bridge"
    acceptance["docker_run_policy"] = policy

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_rejects_network_flags_after_image() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    network_index = policy.index("--network")
    del policy[network_index : network_index + 2]
    acceptance["docker_run_policy"] = [*policy, "--network", "none"]

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_rejects_command_args_after_image() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance["docker_run_policy"] = [*acceptance["docker_run_policy"], "sh", "-c", "true"]

    with pytest.raises(hs.HandshakeValidationError, match="must not override image command"):
        hs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_rejects_multiple_network_flags() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    image_index = len(policy) - 1
    acceptance["docker_run_policy"] = [*policy[:image_index], "--network", "bridge", *policy[image_index:]]

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


@pytest.mark.parametrize("image", [None, "investintell/quant-engine:latest", "investintell/quant-engine@sha256:" + "f" * 64])
def test_executor_acceptance_requires_pinned_matching_image(image: str | None) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    acceptance["docker_run_policy"] = policy[:-1] if image is None else [*policy[:-1], image]

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


@pytest.mark.parametrize(
    "mount_spec",
    [
        "type=bind,src=/input_pack,dst=/input_pack",
        "type=bind,src=/calibration,dst=/calibration",
        "type=bind,src=/contracts,dst=/contracts",
    ],
)
def test_executor_acceptance_requires_readonly_input_bind_flags(mount_spec: str) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    policy[policy.index(f"{mount_spec},readonly")] = mount_spec
    acceptance["docker_run_policy"] = policy

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_accepts_ro_input_bind_alias() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance["docker_run_policy"] = [
        token.replace(",readonly", ",ro") for token in acceptance["docker_run_policy"]
    ]

    hs.validate_executor_acceptance(acceptance, envelope)


@pytest.mark.parametrize(
    "bad_mount",
    [
        "type=bind,src=/other_input_pack,dst=/input_pack,readonly",
        "type=bind,src=/input_pack,dst=/other_input_pack,readonly",
        "type=bind,src=/outputs,dst=/outputs,readonly",
    ],
)
def test_executor_acceptance_requires_exact_docker_bind_mounts(bad_mount: str) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    if "src=/outputs" in bad_mount:
        policy[policy.index("type=bind,src=/outputs,dst=/outputs")] = bad_mount
    else:
        policy[policy.index("type=bind,src=/input_pack,dst=/input_pack,readonly")] = bad_mount
    acceptance["docker_run_policy"] = policy

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


@pytest.mark.parametrize(
    "bad_mount",
    [
        "type=bind,src=/input_pack,dst=/input_pack,readonly,bind-recursive=writable",
        "type=bind,src=/input_pack,dst=/input_pack,readonly,bind-propagation=rshared",
        "type=bind,src=/calibration,dst=/calibration,readonly,bind-propagation=shared",
        "type=bind,src=/contracts,dst=/contracts,readonly,rshared",
        "type=bind,src=/outputs,dst=/outputs,bind-propagation=rshared",
    ],
)
def test_executor_acceptance_rejects_unsafe_extra_bind_options(bad_mount: str) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    if "src=/outputs" in bad_mount:
        policy[policy.index("type=bind,src=/outputs,dst=/outputs")] = bad_mount
    elif "src=/calibration" in bad_mount:
        policy[policy.index("type=bind,src=/calibration,dst=/calibration,readonly")] = bad_mount
    elif "src=/contracts" in bad_mount:
        policy[policy.index("type=bind,src=/contracts,dst=/contracts,readonly")] = bad_mount
    else:
        policy[policy.index("type=bind,src=/input_pack,dst=/input_pack,readonly")] = bad_mount
    acceptance["docker_run_policy"] = policy

    with pytest.raises(hs.HandshakeValidationError, match="unsupported options"):
        hs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_rejects_missing_extra_or_duplicate_docker_binds() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")

    missing_output = deepcopy(acceptance)
    policy = list(missing_output["docker_run_policy"])
    output_index = policy.index("type=bind,src=/outputs,dst=/outputs")
    del policy[output_index - 1 : output_index + 1]
    missing_output["docker_run_policy"] = policy
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(missing_output, envelope)

    extra = deepcopy(acceptance)
    policy = list(extra["docker_run_policy"])
    image_index = len(policy) - 1
    extra["docker_run_policy"] = [
        *policy[:image_index],
        "--mount",
        "type=bind,src=/,dst=/host,readonly",
        *policy[image_index:],
    ]
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(extra, envelope)

    duplicate = deepcopy(acceptance)
    policy = list(duplicate["docker_run_policy"])
    image_index = len(policy) - 1
    duplicate["docker_run_policy"] = [
        *policy[:image_index],
        "--mount",
        "type=bind,src=/input_pack,dst=/input_pack,readonly",
        *policy[image_index:],
    ]
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(duplicate, envelope)


@pytest.mark.parametrize(
    "bad_options",
    [
        ["--privileged"],
        ["--pid=host"],
        ["--pid", "host"],
        ["--cap-add=SYS_ADMIN"],
        ["--cap-add", "SYS_ADMIN"],
        ["--device=/dev/kvm"],
        ["--ipc=host"],
        ["--userns=host"],
    ],
)
def test_executor_acceptance_rejects_privileged_docker_options_before_image(
    bad_options: list[str],
) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    image_index = len(policy) - 1
    acceptance["docker_run_policy"] = [*policy[:image_index], *bad_options, *policy[image_index:]]

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_requires_output_only_writable_mount() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance["mounts"] = [
        {"name": "input_pack", "mode": "read_write"},
        {"name": "calibration", "mode": "read_only"},
        {"name": "contract_bundle", "mode": "read_only"},
        {"name": "output", "mode": "read_write"},
    ]

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_rejects_extra_duplicate_or_malformed_mounts() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")

    extra = deepcopy(acceptance)
    extra["mounts"].append({"name": "host_root", "mode": "read_only"})
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(extra, envelope)

    duplicate = deepcopy(acceptance)
    duplicate["mounts"].append({"name": "input_pack", "mode": "read_only"})
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(duplicate, envelope)

    malformed = deepcopy(acceptance)
    malformed["mounts"].append("input_pack")
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_acceptance(malformed, envelope)

    unexpected_fields = deepcopy(acceptance)
    unexpected_fields["mounts"][0]["actual_mode"] = "read_write"
    unexpected_fields["mounts"][0]["source"] = "/"
    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_executor_acceptance(unexpected_fields, envelope)


def test_executor_result_reference_pins_manifest_paths() -> None:
    reference = _artifact("executor_result_reference.json")
    hs.validate_executor_result_reference(reference)

    bad_output_path = deepcopy(reference)
    bad_output_path["output_manifest_path"] = "stale/output_manifest.json"
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_result_reference(bad_output_path)

    bad_result_path = deepcopy(reference)
    bad_result_path["shadow_result_manifest_path"] = "stale/shadow_result_manifest.json"
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_executor_result_reference(bad_result_path)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation_attempt", True),
        ("official_db_write_attempt", True),
        ("unexpected_reference_property", "value"),
    ],
)
def test_executor_result_reference_rejects_unexpected_fields(field: str, bad: object) -> None:
    reference = _artifact("executor_result_reference.json")
    reference[field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_executor_result_reference(reference)


def test_shadow_result_manifest_rejects_side_effect_attempt_on_success() -> None:
    result = _artifact("shadow_result_manifest.json")
    result["failure_class"] = "allocator_publish_attempt"
    result["side_effect_attempt_count"] = 1

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_shadow_result_manifest(result)


def test_shadow_result_manifest_rejects_non_zero_divergence() -> None:
    result = _artifact("shadow_result_manifest.json")
    result["divergence_summary"]["mismatch_count"] = 1

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_shadow_result_manifest(result)


@pytest.mark.parametrize("field", ("started_at", "finished_at", "duration_ms", "memory_peak_bytes", "cpu_time_ms"))
def test_shadow_result_manifest_requires_success_metadata(field: str) -> None:
    result = _artifact("shadow_result_manifest.json")
    del result[field]

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_shadow_result_manifest(result)


def test_shadow_result_manifest_rejects_inconsistent_duration_window() -> None:
    result = _artifact("shadow_result_manifest.json")
    result["finished_at"] = "2026-06-29T16:59:59Z"
    with pytest.raises(hs.HandshakeValidationError, match="finished_at"):
        hs.validate_shadow_result_manifest(result)

    result = _artifact("shadow_result_manifest.json")
    result["duration_ms"] = 1
    with pytest.raises(hs.HandshakeValidationError, match="duration_ms"):
        hs.validate_shadow_result_manifest(result)


@pytest.mark.parametrize("field", ("started_at", "finished_at"))
def test_shadow_result_manifest_requires_utc_z_timestamp_form(field: str) -> None:
    result = _artifact("shadow_result_manifest.json")
    result[field] = "2026-06-29T12:00:00-05:00"

    with pytest.raises(hs.HandshakeValidationError, match="UTC Z"):
        hs.validate_shadow_result_manifest(result)


@pytest.mark.parametrize("field", hs.SHADOW_RESULT_EVIDENCE_HASH_FILES)
def test_shadow_result_manifest_binds_evidence_hashes(field: str) -> None:
    result = _artifact("shadow_result_manifest.json")
    result[field] = "0" * 64

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_shadow_result_manifest(
            result,
            evidence_hashes={
                key: hs.file_sha256(HANDSHAKE_ROOT / rel)
                for key, rel in hs.SHADOW_RESULT_EVIDENCE_HASH_FILES.items()
            },
        )


@pytest.mark.parametrize(
    "field",
    [
        "return_metric_delta_pct",
        "risk_metric_delta_pct",
        "allocation_weight_delta_pct",
        "classification_rate_delta_pct",
        "latency_p95_regression_pct",
        "memory_peak_regression_pct",
        "retry_rate_delta_pct",
    ],
)
def test_shadow_result_manifest_rejects_non_zero_materiality_delta(field: str) -> None:
    result = _artifact("shadow_result_manifest.json")
    result["materiality_summary"][field] = 0.1

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_shadow_result_manifest(result)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("A5", "unblocked"),
        ("freeze_ready", True),
        ("runtime_activation_attempt", True),
        ("a5_status", "unblocked"),
        ("unexpected_result_property", "value"),
    ],
)
def test_shadow_result_manifest_rejects_unexpected_result_fields(field: str, bad: object) -> None:
    result = _artifact("shadow_result_manifest.json")
    result[field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_shadow_result_manifest(result)


def test_handshake_rejects_boolean_values_for_numeric_pins() -> None:
    request = _artifact("control_plane_request.json")
    request["schema_version"] = True
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_control_plane_request(request)

    result = _artifact("shadow_result_manifest.json")
    result["divergence_summary"]["mismatch_count"] = False
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_shadow_result_manifest(result)

    result = _artifact("shadow_result_manifest.json")
    result["materiality_summary"]["max_relative_delta_pct"] = False
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_shadow_result_manifest(result)


def test_output_manifest_requires_logs_and_current_hashes() -> None:
    manifest = _artifact("output_manifest.json")
    hs.validate_output_manifest(HANDSHAKE_ROOT, manifest)

    missing_log = deepcopy(manifest)
    missing_log["logs_required"] = ["logs/control_plane_validator.log"]
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_output_manifest(HANDSHAKE_ROOT, missing_log)

    bad_hash = deepcopy(manifest)
    bad_hash["artifacts"][0]["sha256"] = "0" * 64
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_output_manifest(HANDSHAKE_ROOT, bad_hash)


def test_verify_handshake_rejects_unexpected_output_manifest_artifact_fields_after_result_hash_refresh(
    tmp_path: Path,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    manifest = _json(root / "output_manifest.json")
    manifest["artifacts"][0]["runtime_activation_attempt"] = True
    _write_json(root / "output_manifest.json", manifest)
    result = _json(root / "shadow_result_manifest.json")
    result["output_manifest_sha256"] = hs.file_sha256(root / "output_manifest.json")
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.verify_handshake(root)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("official_result", True),
        ("runtime_activation_attempt", True),
        ("unexpected_output_manifest_property", "value"),
    ],
)
def test_verify_handshake_rejects_unexpected_output_manifest_fields_after_result_hash_refresh(
    tmp_path: Path,
    field: str,
    bad: object,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    manifest = _json(root / "output_manifest.json")
    manifest[field] = bad
    _write_json(root / "output_manifest.json", manifest)
    result = _json(root / "shadow_result_manifest.json")
    result["output_manifest_sha256"] = hs.file_sha256(root / "output_manifest.json")
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(hs.HandshakeValidationError, match="output_manifest: unexpected fields"):
        hs.verify_handshake(root)


def test_output_manifest_rejects_unlisted_files_on_disk(tmp_path: Path) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    manifest = _json(root / "output_manifest.json")
    (root / "logs" / "extra.log").write_text("unexpected\n", encoding="utf-8")

    with pytest.raises(hs.HandshakeValidationError, match="unexpected files on disk"):
        hs.validate_output_manifest(root, manifest)


@pytest.mark.parametrize(
    ("rel", "token"),
    [
        ("logs/control_plane_validator.log", "runtime_activation_attempt=true"),
        ("logs/control_plane_validator.log", "runtime_activation_attempt=blocked"),
        ("logs/control_plane_validator.log", "A5=unblocked"),
        ("logs/control_plane_validator.log", "freeze_ready=true"),
        ("logs/control_plane_validator.log", "allow_db_write=true allow_db_write=false"),
        ("logs/control_plane_validator.log", "backend_executes_engine_attempt=true"),
        ("logs/control_plane_validator.log", "backend_executes_engine_attempt=false"),
        ("logs/control_plane_validator.log", "backend_executes_docker_attempt=true"),
        ("logs/control_plane_validator.log", "backend_executes_docker_attempt=false"),
        ("logs/control_plane_validator.log", "backend_executes_subprocess_attempt=true"),
        ("logs/control_plane_validator.log", "backend_executes_subprocess_attempt=false"),
        ("logs/external_executor.log", "allocator_publish_attempt=true"),
        ("logs/external_executor.log", "allocator_publish_attempt=false"),
        ("logs/external_executor.log", "official_db_write_attempt=1"),
        ("logs/external_executor.log", "production_endpoint_activation_attempt=blocked"),
        ("logs/external_executor.log", "production_endpoint_activation=public"),
        ("logs/external_executor.log", "network=none network=bridge"),
        ("logs/external_executor.log", "input_pack_mount=read_only input_pack_mount=read_write"),
        ("logs/external_executor.log", "contract_bundle_mount=read_only contract_bundle_mount=read_write"),
        ("logs/external_executor.log", "db_access=false db_access=read_only"),
        ("logs/control_plane_validator.log", "runtime_activation=false runtime_activation=true"),
        ("logs/control_plane_validator.log", "allow_db_write=false allow_db_write=maybe"),
        ("logs/external_executor.log", "source_tree_writes=false source_tree_writes=true"),
    ],
)
def test_output_manifest_scans_required_logs_for_side_effect_attempts(
    tmp_path: Path,
    rel: str,
    token: str,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    log_path = root / rel
    log_path.write_text(log_path.read_text(encoding="utf-8") + f" {token}\n", encoding="utf-8")
    manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, manifest, rel)

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_output_manifest(root, manifest)


def test_output_manifest_requires_external_executor_db_access_attestation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    rel = "logs/external_executor.log"
    log_path = root / rel
    log_path.write_text(
        log_path.read_text(encoding="utf-8").replace(" db_access=false", ""),
        encoding="utf-8",
    )
    manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, manifest, rel)

    with pytest.raises(hs.HandshakeValidationError, match="db_access=false"):
        hs.validate_output_manifest(root, manifest)


@pytest.mark.parametrize(
    "token",
    [
        "db_access=false",
        "network=none",
        "input_pack_mount=read_only",
        "calibration_mount=read_only",
        "contract_bundle_mount=read_only",
        "output_mount=read_write",
        "writable_mounts=output",
        "source_tree_writes=false",
    ],
)
def test_output_manifest_requires_executor_isolation_attestations_from_executor_log(
    tmp_path: Path,
    token: str,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    executor_rel = "logs/external_executor.log"
    control_rel = "logs/control_plane_validator.log"
    executor_log = root / executor_rel
    control_log = root / control_rel
    executor_log.write_text(
        executor_log.read_text(encoding="utf-8").replace(f" {token}", ""),
        encoding="utf-8",
    )
    control_log.write_text(
        control_log.read_text(encoding="utf-8") + f" {token}\n",
        encoding="utf-8",
    )
    manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, manifest, executor_rel)
    _refresh_manifest_entry(root, manifest, control_rel)

    with pytest.raises(hs.HandshakeValidationError, match="logs/external_executor.log"):
        hs.validate_output_manifest(root, manifest)


@pytest.mark.parametrize(
    "token",
    [
        "runtime_activation=false",
        "allow_db_write=false",
        "allow_allocator_publish=false",
        "official_result=false",
        "production_endpoint_activation=none",
        "backend_executes_engine=false",
        "backend_executes_docker=false",
        "backend_executes_subprocess=false",
    ],
)
def test_output_manifest_requires_control_plane_attestations_from_control_plane_log(
    tmp_path: Path,
    token: str,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    control_rel = "logs/control_plane_validator.log"
    executor_rel = "logs/external_executor.log"
    control_log = root / control_rel
    executor_log = root / executor_rel
    control_log.write_text(
        control_log.read_text(encoding="utf-8").replace(f" {token}", ""),
        encoding="utf-8",
    )
    executor_log.write_text(
        executor_log.read_text(encoding="utf-8") + f" {token}\n",
        encoding="utf-8",
    )
    manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, manifest, control_rel)
    _refresh_manifest_entry(root, manifest, executor_rel)

    with pytest.raises(hs.HandshakeValidationError, match="logs/control_plane_validator.log"):
        hs.validate_output_manifest(root, manifest)


@pytest.mark.parametrize(
    ("rel", "old", "new"),
    [
        (
            "logs/control_plane_validator.log",
            f"handshake_id={hs.HANDSHAKE_ID}",
            "handshake_id=other_bundle",
        ),
        (
            "logs/external_executor.log",
            f"handshake_id={hs.HANDSHAKE_ID}",
            "handshake_id=other_bundle",
        ),
        ("logs/control_plane_validator.log", "validator=control_plane", "validator=external"),
        ("logs/external_executor.log", "executor=external", "executor=control_plane"),
        ("logs/control_plane_validator.log", "status=pass", "status=failed"),
        ("logs/external_executor.log", "status=accepted", "status=rejected"),
    ],
)
def test_output_manifest_pins_required_log_identity_and_status(
    tmp_path: Path,
    rel: str,
    old: str,
    new: str,
) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    log_path = root / rel
    log_path.write_text(log_path.read_text(encoding="utf-8").replace(old, new), encoding="utf-8")
    manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, manifest, rel)

    with pytest.raises(hs.HandshakeValidationError, match=rel):
        hs.validate_output_manifest(root, manifest)


def test_load_json_rejects_duplicate_runtime_activation_key(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text('{"runtime_activation": true, "runtime_activation": false}\n', encoding="utf-8")

    with pytest.raises(hs.HandshakeValidationError, match="duplicate JSON object key: runtime_activation"):
        hs.load_json(path)


def test_verify_handshake_rejects_duplicate_json_keys_before_validation(tmp_path: Path) -> None:
    root = tmp_path / "handshake"
    shutil.copytree(HANDSHAKE_ROOT, root)
    request = _artifact("control_plane_request.json")
    request_without_runtime = {
        key: value for key, value in request.items() if key != "runtime_activation"
    }
    request_text = json.dumps(request_without_runtime, sort_keys=True, indent=2)
    request_text = request_text.replace(
        "{\n",
        '{\n  "runtime_activation": true,\n  "runtime_activation": false,\n',
        1,
    )
    (root / "control_plane_request.json").write_text(request_text + "\n", encoding="utf-8")

    with pytest.raises(hs.HandshakeValidationError, match="duplicate JSON object key"):
        hs.verify_handshake(root)


def test_reproducibility_report_requires_green_jobs_matrix() -> None:
    report = _artifact("reproducibility_report.json")
    hs.validate_reproducibility_report(report)

    for field, bad in (
        ("jobs_matrix", [1]),
        ("run_count", 7),
        ("expected_run_count", 7),
        ("missing", ["host_jobs1_r0"]),
        ("unexpected", ["host_jobs8_r0"]),
        ("missing_hashes", ["host_jobs1_r0"]),
        ("unexpected_hashes", ["host_jobs8_r0"]),
        ("output_manifest_sha256_by_run", {label: "0" * 64 for label in hs.EXPECTED_LABELS}),
        ("run_hash_mismatches", ["container_jobs1_r0"]),
        ("duplicates", 1),
        ("mismatch_count", 1),
        ("network", "bridge"),
        ("db_access", True),
        ("input_pack_mount", "read_write"),
        ("calibration_mount", "read_write"),
        ("writable_mounts", ["input_pack", "output"]),
    ):
        broken = deepcopy(report)
        broken[field] = bad
        with pytest.raises(hs.HandshakeValidationError):
            hs.validate_reproducibility_report(broken)


def test_reproducibility_report_validates_against_handshake_schema() -> None:
    schema = _artifact("reproducibility_report.schema.json")
    report = _artifact("reproducibility_report.json")

    jsonschema.validate(report, schema)
    hs.validate_reproducibility_report_schema(schema)
    hs.validate_reproducibility_report(report)


@pytest.mark.parametrize(
    "field",
    [
        "artifact_type",
        "semantic_run_fingerprint_policy",
        "docker_image_id",
        "expected_docker_image_id",
        "expected_docker_image_digest",
    ],
)
def test_reproducibility_report_requires_schema_profile_fields(field: str) -> None:
    report = _artifact("reproducibility_report.json")
    del report[field]

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_reproducibility_report(report)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation", True),
        ("runtime_activation_attempt", True),
        ("unexpected_governance_field", False),
    ],
)
def test_reproducibility_report_rejects_unexpected_top_level_fields(
    field: str,
    bad: object,
) -> None:
    report = _artifact("reproducibility_report.json")
    report[field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_reproducibility_report(report)


@pytest.mark.parametrize("field", ("missing_hashes", "unexpected_hashes", "output_manifest_sha256_by_run"))
def test_reproducibility_report_requires_hash_membership_evidence(field: str) -> None:
    report = _artifact("reproducibility_report.json")
    del report[field]

    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_reproducibility_report(report)


def test_no_side_effects_report_requires_expected_checks() -> None:
    report = _artifact("no_side_effects_report.json")
    hs.validate_no_side_effects_report(report)

    missing = deepcopy(report)
    missing["checks"] = [check for check in missing["checks"] if check["id"] != "backend_docker_execution"]
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_no_side_effects_report(missing)

    unexpected = deepcopy(report)
    unexpected["checks"].append({"id": "unrelated_check", "allowed": False, "status": "pass"})
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_no_side_effects_report(unexpected)

    duplicate = deepcopy(report)
    duplicate["checks"].append(deepcopy(duplicate["checks"][0]))
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_no_side_effects_report(duplicate)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation_attempt", True),
        ("official_db_write_attempt", True),
        ("unexpected_report_property", "value"),
    ],
)
def test_no_side_effects_report_rejects_unexpected_top_level_fields(
    field: str,
    bad: object,
) -> None:
    report = _artifact("no_side_effects_report.json")
    report[field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_no_side_effects_report(report)


def test_no_side_effects_report_rejects_unexpected_check_fields() -> None:
    report = _artifact("no_side_effects_report.json")
    report["checks"][0]["observed"] = True

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_no_side_effects_report(report)


def test_validation_report_requires_green_expected_checks() -> None:
    report = _artifact("validation_report.json")
    hs.validate_validation_report(report)

    missing = deepcopy(report)
    missing["checks"] = [check for check in missing["checks"] if check["id"] != "logs_present"]
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_validation_report(missing)

    failed = deepcopy(report)
    failed["checks"][0]["status"] = "fail"
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_validation_report(failed)

    empty = deepcopy(report)
    empty["checks"] = []
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_validation_report(empty)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("freeze_ready", True),
        ("allow_db_write", True),
        ("production_endpoint_activation", "public"),
        ("unexpected_report_property", "value"),
    ],
)
def test_validation_report_rejects_unexpected_governance_fields(
    field: str,
    bad: object,
) -> None:
    report = _artifact("validation_report.json")
    report[field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_validation_report(report)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("actual", "fail"),
        ("observed", "missing"),
        ("unexpected_check_property", "value"),
    ],
)
def test_validation_report_rejects_unexpected_check_fields(field: str, bad: object) -> None:
    report = _artifact("validation_report.json")
    report["checks"][0][field] = bad

    with pytest.raises(hs.HandshakeValidationError, match="unexpected fields"):
        hs.validate_validation_report(report)


def test_feature_flag_default_remains_false() -> None:
    request = _artifact("control_plane_request.json")
    assert request["feature_flag_name"] == "open_macro_v03_runtime_activation"
    assert request["feature_flag_default"] is False

    request["feature_flag_default"] = True
    with pytest.raises(hs.HandshakeValidationError):
        hs.validate_control_plane_request(request)


def test_handshake_rejects_dangling_symlink(tmp_path: Path) -> None:
    dangling = tmp_path / "dangling"
    try:
        os.symlink(tmp_path / "missing", dangling)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with pytest.raises(hs.HandshakeValidationError):
        hs.reject_symlinks(tmp_path)


def test_handshake_validator_imports_no_runtime_side_effect_paths() -> None:
    source = (ROOT / "src" / "external_executor_handshake.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports |= {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }

    forbidden = {
        "subprocess",
        "docker",
        "sqlalchemy",
        "psycopg",
        "asyncpg",
        "fastapi",
        "src.shadow_pilot",
        "src.calibration_candidate",
        "investintell_quant_engine",
    }
    assert imports.isdisjoint(forbidden)


def test_github_actions_runs_external_executor_handshake_gate() -> None:
    text = GITHUB_ACTIONS_WORKFLOW.read_text(encoding="utf-8")

    assert "pull_request:" in text
    assert "tests/test_external_executor_handshake.py" in text
