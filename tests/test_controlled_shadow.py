from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

import src.controlled_shadow as cs
import src.external_executor_handshake as hs

ROOT = Path(__file__).resolve().parents[1]
BUNDLE_ROOT = ROOT / "artifacts" / "shadow" / cs.CONTROLLED_SHADOW_ID


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact(name: str) -> dict:
    return _json(BUNDLE_ROOT / name)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _refresh_manifest_entry(root: Path, manifest: dict, rel: str) -> None:
    for entry in manifest["artifacts"]:
        if entry["path"] == rel:
            entry["sha256"] = hs.file_sha256(root / rel)
            entry["bytes"] = len((root / rel).read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8"))
            return
    raise AssertionError(f"missing manifest entry for {rel}")


def _copy_bundle(tmp_path: Path) -> Path:
    root = tmp_path / cs.CONTROLLED_SHADOW_ID
    shutil.copytree(BUNDLE_ROOT, root)
    return root


def _copy_immutable_workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    shutil.copytree(
        ROOT / "fixtures" / "input_packs" / "golden" / "certified_input_pack",
        workspace / "fixtures" / "input_packs" / "golden" / "certified_input_pack",
    )
    shutil.copytree(
        ROOT / "artifacts" / "calibration" / hs.CALIBRATION_ID,
        workspace / "artifacts" / "calibration" / hs.CALIBRATION_ID,
    )
    shutil.copytree(
        ROOT / "contracts" / "quant-engine" / "v1",
        workspace / "contracts" / "quant-engine" / "v1",
    )
    return workspace


def test_controlled_shadow_artifacts_verify_offline() -> None:
    result = cs.verify_controlled_shadow(BUNDLE_ROOT, workspace_root=ROOT)

    assert result["controlled_shadow_id"] == cs.CONTROLLED_SHADOW_ID
    assert result["external_executor_handshake_id"] == hs.HANDSHAKE_ID
    assert result["runtime_activation"] is False
    assert result["A5"] == "blocked"
    assert result["freeze_ready"] is False
    assert result["official_result"] is False
    assert result["allow_db_write"] is False
    assert result["allow_allocator_publish"] is False
    assert result["production_endpoint_activation"] == "none"
    assert result["backend_runtime_execution"] == "none"
    assert result["mismatch_count"] == 0
    assert result["immutable_inputs"]["verified"] is True
    assert result["validated"] is True


def test_controlled_shadow_manifest_matches_required_schema() -> None:
    assert _artifact("controlled_shadow_manifest.json") == cs.EXPECTED_CONTROLLED_SHADOW_MANIFEST


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation", True),
        ("A5", "unblocked"),
        ("freeze_ready", True),
        ("official_result", True),
        ("allow_db_write", True),
        ("allow_allocator_publish", True),
        ("production_endpoint_activation", "public"),
        ("backend_executes_engine", True),
        ("backend_executes_docker", True),
        ("backend_executes_subprocess", True),
    ],
)
def test_controlled_shadow_manifest_rejects_activation_or_side_effects(field: str, bad: object) -> None:
    manifest = _artifact("controlled_shadow_manifest.json")
    manifest[field] = bad

    with pytest.raises(cs.ControlledShadowValidationError):
        cs.validate_controlled_shadow_manifest(manifest)


@pytest.mark.parametrize(
    ("artifact", "validator", "field", "bad"),
    [
        ("control_plane_request.json", cs.validate_control_plane_request, "runtime_activation", True),
        ("control_plane_request.json", cs.validate_control_plane_request, "allow_db_write", True),
        ("control_plane_request.json", cs.validate_control_plane_request, "backend_executes_docker", True),
        ("shadow_job_envelope.json", cs.validate_shadow_job_envelope, "allow_allocator_publish", True),
        ("shadow_job_envelope.json", cs.validate_shadow_job_envelope, "output_artifact_uri", "db://official/results"),
        ("no_side_effects_report.json", cs.validate_no_side_effects_report, "production_endpoint_activation", "public"),
        ("acceptance_report.json", cs.validate_acceptance_report, "A5", "unblocked"),
    ],
)
def test_controlled_shadow_gates_reject_forbidden_runtime_state(
    artifact: str,
    validator,
    field: str,
    bad: object,
) -> None:
    payload = _artifact(artifact)
    payload[field] = bad

    with pytest.raises(cs.ControlledShadowValidationError):
        validator(payload)


def test_executor_acceptance_rejects_backend_execution_attempt() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance["backend_executes_subprocess"] = True

    with pytest.raises(cs.ControlledShadowValidationError):
        cs.validate_executor_acceptance(acceptance, envelope)


def test_shadow_result_rejects_non_zero_mismatch_count(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    result = _json(root / "shadow_result_manifest.json")
    result["divergence_summary"]["mismatch_count"] = 1
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(cs.ControlledShadowValidationError, match="mismatch_count"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_reproducibility_report_requires_mismatch_count_zero() -> None:
    report = _artifact("reproducibility_report.json")
    report["mismatch_count"] = 1

    with pytest.raises(cs.ControlledShadowValidationError, match="mismatch_count"):
        cs.validate_reproducibility_report(report)


def test_output_manifest_rejects_hash_drift(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    report = root / "controlled_shadow_report.md"
    report.write_text(report.read_text(encoding="utf-8") + "\nHash drift sentinel.\n", encoding="utf-8")

    with pytest.raises(cs.ControlledShadowValidationError, match="sha256"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_shadow_result_rejects_stale_output_manifest_hash(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    output_manifest = _json(root / "output_manifest.json")
    output_manifest["status"] = "succeeded"
    _write_json(root / "output_manifest.json", output_manifest)
    result = _json(root / "shadow_result_manifest.json")
    result["output_manifest_sha256"] = "0" * 64
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(cs.ControlledShadowValidationError, match="output_manifest_sha256"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_output_manifest_rejects_unexpected_file(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    (root / "unexpected.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(cs.ControlledShadowValidationError, match="unexpected controlled shadow files"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_output_manifest_rejects_missing_output(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    (root / "observability_evidence.json").unlink()

    with pytest.raises(cs.ControlledShadowValidationError, match="missing controlled shadow artifact"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_logs_reject_forbidden_attempt_markers(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    log = root / "logs" / "external_executor.log"
    log.write_text(log.read_text(encoding="utf-8").strip() + " allocator_publish_attempt=true\n", encoding="utf-8")
    output_manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, output_manifest, "logs/external_executor.log")
    _write_json(root / "output_manifest.json", output_manifest)
    result = _json(root / "shadow_result_manifest.json")
    result["output_manifest_sha256"] = hs.file_sha256(root / "output_manifest.json")
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(cs.ControlledShadowValidationError, match="allocator_publish_attempt"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_immutable_inputs_validate_real_hashes() -> None:
    result = cs.validate_immutable_inputs(ROOT)

    assert result["input_pack_sha256"] == hs.INPUT_PACK_SHA256
    assert result["calibration_config_sha256"] == hs.CALIBRATION_CONFIG_SHA256
    assert result["calibration_run_matrix_sha256"] == cs.CALIBRATION_RUN_MATRIX_SHA256
    assert result["contract_bundle_sha256"] == hs.CONTRACT_BUNDLE_SHA256
    assert result["verified"] is True


def test_immutable_inputs_reject_input_pack_hash_drift(tmp_path: Path) -> None:
    workspace = _copy_immutable_workspace(tmp_path)
    manifest_path = workspace / "fixtures" / "input_packs" / "golden" / "certified_input_pack" / "manifest.json"
    manifest = _json(manifest_path)
    manifest["input_pack_sha256"] = "0" * 64
    _write_json(manifest_path, manifest)

    with pytest.raises(cs.ControlledShadowValidationError, match="input pack verification failed"):
        cs.validate_immutable_inputs(workspace)


def test_immutable_inputs_reject_calibration_hash_drift(tmp_path: Path) -> None:
    workspace = _copy_immutable_workspace(tmp_path)
    manifest_path = workspace / "artifacts" / "calibration" / hs.CALIBRATION_ID / "calibration_manifest.json"
    manifest = _json(manifest_path)
    manifest["run_matrix_sha256"] = "0" * 64
    _write_json(manifest_path, manifest)

    with pytest.raises(cs.ControlledShadowValidationError, match="run_matrix_sha256"):
        cs.validate_immutable_inputs(workspace)


def test_controlled_shadow_validator_avoids_productive_imports() -> None:
    source = (ROOT / "src" / "controlled_shadow.py").read_text(encoding="utf-8")

    assert "from src.db" not in source
    assert "import src.db" not in source
    assert "import subprocess" not in source
    assert "from subprocess" not in source
    assert "docker.from_env" not in source
