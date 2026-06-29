from __future__ import annotations

import ast
import json
import os
from copy import deepcopy
from pathlib import Path

import jsonschema
import pytest

from src import external_executor_handshake as hs

ROOT = Path(__file__).resolve().parents[1]
HANDSHAKE_ROOT = ROOT / "artifacts" / "handshake" / hs.HANDSHAKE_ID
SHADOW_ROOT = ROOT / "artifacts" / "shadow" / hs.SHADOW_ID
RAILWAY_CI_DOCKERFILE = ROOT / "docker" / "railway-ci" / "Dockerfile"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact(name: str) -> dict:
    return _json(HANDSHAKE_ROOT / name)


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


def test_shadow_job_envelope_validates_against_shadow_schema() -> None:
    schema = _json(SHADOW_ROOT / "shadow_job_envelope.schema.json")
    envelope = _artifact("shadow_job_envelope.json")

    jsonschema.validate(envelope, schema)
    hs.validate_shadow_job_envelope(envelope)


def test_shadow_result_manifest_validates_against_shadow_schema() -> None:
    schema = _json(SHADOW_ROOT / "shadow_result_manifest.schema.json")
    result = _artifact("shadow_result_manifest.json")

    jsonschema.validate(result, schema)
    hs.validate_shadow_result_manifest(
        result,
        output_manifest_sha256=hs.file_sha256(HANDSHAKE_ROOT / "output_manifest.json"),
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


def test_executor_acceptance_requires_docker_network_none() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    policy[policy.index("none")] = "bridge"
    acceptance["docker_run_policy"] = policy

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


def test_reproducibility_report_requires_green_jobs_matrix() -> None:
    report = _artifact("reproducibility_report.json")
    hs.validate_reproducibility_report(report)

    for field, bad in (
        ("jobs_matrix", [1]),
        ("run_count", 7),
        ("expected_run_count", 7),
        ("missing", ["host_jobs1_r0"]),
        ("unexpected", ["host_jobs8_r0"]),
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


def test_railway_ci_runs_external_executor_handshake_gate() -> None:
    text = RAILWAY_CI_DOCKERFILE.read_text(encoding="utf-8")

    assert "COPY artifacts/handshake/open_macro_v03_external_executor_handshake_001" in text
    assert "tests/test_external_executor_handshake.py" in text
