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


def _refresh_shadow_result_output_manifest_hash(root: Path) -> None:
    result = _json(root / "shadow_result_manifest.json")
    result["output_manifest_sha256"] = hs.file_sha256(root / "output_manifest.json")
    _write_json(root / "shadow_result_manifest.json", result)


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


def test_load_json_rejects_duplicate_json_object_keys(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text('{"runtime_activation": true, "runtime_activation": false}\n', encoding="utf-8")

    with pytest.raises(cs.ControlledShadowValidationError, match="duplicate JSON object key 'runtime_activation'"):
        cs.load_json(path)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_load_json_rejects_non_standard_json_constants(tmp_path: Path, constant: str) -> None:
    path = tmp_path / "non_standard_constant.json"
    path.write_text(f'{{"value": {constant}}}\n', encoding="utf-8")

    with pytest.raises(cs.ControlledShadowValidationError, match="non-standard JSON constant"):
        cs.load_json(path)


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
        ("control_plane_request.json", cs.validate_control_plane_request, "runtime_activation", 0),
        ("no_side_effects_report.json", cs.validate_no_side_effects_report, "runtime_activation", 0),
        ("acceptance_report.json", cs.validate_acceptance_report, "runtime_activation", 0),
    ],
)
def test_controlled_shadow_pins_reject_bool_int_coercion(
    artifact: str,
    validator,
    field: str,
    bad: object,
) -> None:
    payload = _artifact(artifact)
    payload[field] = bad

    with pytest.raises(cs.ControlledShadowValidationError, match=field):
        validator(payload)


def test_reproducibility_report_rejects_float_integer_count_pin() -> None:
    report = _artifact("reproducibility_report.json")
    report["mismatch_count"] = 0.0

    with pytest.raises(cs.ControlledShadowValidationError, match="mismatch_count"):
        cs.validate_reproducibility_report(report)


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


@pytest.mark.parametrize(
    ("section", "field", "bad"),
    [
        ("population_scope", "source", "live_control_plane"),
        ("population_scope", "row_selection", "filtered_runtime_scope"),
        ("population_scope", "scope_id", "open_macro_v03_certified_input_pack_999_as_of_2026_06_26"),
        ("executor_identity", "executor_id", "external-controlled-shadow-runner-999"),
        ("executor_identity", "owner", "shadow-ops"),
        ("executor_identity", "is_external", False),
        ("executor_identity", "is_external", 1),
    ],
)
def test_control_plane_request_rejects_unpinned_scope_and_executor_identity(
    section: str,
    field: str,
    bad: object,
) -> None:
    request = _artifact("control_plane_request.json")
    request[section][field] = bad

    with pytest.raises(cs.ControlledShadowValidationError, match=field):
        cs.validate_control_plane_request(request)


def test_control_plane_request_rejects_unexpected_executor_identity_fields() -> None:
    request = _artifact("control_plane_request.json")
    request["executor_identity"]["service_account"] = "shadow-runner"

    with pytest.raises(cs.ControlledShadowValidationError, match="executor_identity: unexpected fields"):
        cs.validate_control_plane_request(request)


def test_control_plane_request_rejects_unexpected_population_scope_fields() -> None:
    request = _artifact("control_plane_request.json")
    request["population_scope"]["runtime_filter"] = "top_10"

    with pytest.raises(cs.ControlledShadowValidationError, match="population_scope: unexpected fields"):
        cs.validate_control_plane_request(request)


def test_control_plane_request_rejects_rollback_owner_drift() -> None:
    request = _artifact("control_plane_request.json")
    request["rollback_owner"] = "runtime-operations"

    with pytest.raises(cs.ControlledShadowValidationError, match="rollback_owner"):
        cs.validate_control_plane_request(request)


@pytest.mark.parametrize(
    ("execution_window", "match"),
    [
        ({}, "missing fields"),
        (
            {
                "started_at": "2026-06-29T18:00:00Z",
                "finished_at": "2026-06-29T17:59:59Z",
                "timezone": "UTC",
                "window_id": "open_macro_v03_controlled_shadow_window_001",
            },
            "finished_at",
        ),
        (
            {
                "started_at": "2026-06-29T18:00:00-03:00",
                "finished_at": "2026-06-29T18:00:01Z",
                "timezone": "UTC",
                "window_id": "open_macro_v03_controlled_shadow_window_001",
            },
            "UTC Z",
        ),
        (
            {
                "started_at": "2026-06-29T18:00:00Z",
                "finished_at": "2026-06-29T18:00:01Z",
                "timezone": "America/Sao_Paulo",
                "window_id": "open_macro_v03_controlled_shadow_window_001",
            },
            "timezone",
        ),
        (
            {
                "started_at": "2026-06-29T18:00:00Z",
                "finished_at": "2026-06-29T18:00:01Z",
                "timezone": "UTC",
                "window_id": "open_macro_v03_controlled_shadow_window_001",
                "runtime_activation": True,
            },
            "unexpected fields",
        ),
        (
            {
                "started_at": "2030-01-01T00:00:00Z",
                "finished_at": "2030-01-01T00:00:01Z",
                "timezone": "UTC",
                "window_id": "open_macro_v03_controlled_shadow_window_001",
            },
            "started_at",
        ),
    ],
)
def test_control_plane_request_rejects_invalid_execution_window(
    execution_window: dict,
    match: str,
) -> None:
    request = _artifact("control_plane_request.json")
    request["execution_window"] = execution_window

    with pytest.raises(cs.ControlledShadowValidationError, match=match):
        cs.validate_control_plane_request(request)


@pytest.mark.parametrize(
    ("name", "field", "bad"),
    [
        ("input_pack", "path", "fixtures/input_packs/golden/other_pack"),
        ("input_pack", "sha256", "0" * 64),
        ("input_pack", "stable_id", "open_macro_v03_certified_input_pack_999"),
        ("calibration_config", "path", "artifacts/calibration/open_macro_v03_calibration_001/other.json"),
        ("calibration_run_matrix", "bytes", 12924),
        ("contract_bundle", "source_commit", "0" * 40),
    ],
)
def test_shadow_job_envelope_rejects_unpinned_read_only_inputs(
    name: str,
    field: str,
    bad: object,
) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    entry = next(item for item in envelope["read_only_inputs"] if item["name"] == name)
    entry[field] = bad

    with pytest.raises(cs.ControlledShadowValidationError, match=rf"read_only_inputs\[{name}\].*{field}"):
        cs.validate_shadow_job_envelope(envelope)


def test_executor_acceptance_rejects_backend_execution_attempt() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance["backend_executes_subprocess"] = True

    with pytest.raises(cs.ControlledShadowValidationError):
        cs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_rejects_duplicate_mount_names() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance["mounts"] = [
        {"name": "input_pack", "mode": "read_write"},
        *acceptance["mounts"],
    ]

    with pytest.raises(cs.ControlledShadowValidationError, match="duplicate name: input_pack"):
        cs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_rejects_unexpected_mount_fields() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance["mounts"][0]["writable"] = True

    with pytest.raises(cs.ControlledShadowValidationError, match=r"executor_acceptance.mounts\[\]: unexpected fields"):
        cs.validate_executor_acceptance(acceptance, envelope)


def test_executor_acceptance_requires_none_as_network_value() -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    network_index = policy.index("--network")
    policy[network_index + 1] = "bridge"
    acceptance["docker_run_policy"] = [*policy[: network_index + 2], "none", *policy[network_index + 2 :]]

    with pytest.raises(cs.ControlledShadowValidationError, match="docker_run_policy must require --network none"):
        cs.validate_executor_acceptance(acceptance, envelope)


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

    with pytest.raises(cs.ControlledShadowValidationError, match="bind mounts mismatch"):
        cs.validate_executor_acceptance(acceptance, envelope)


@pytest.mark.parametrize(
    "bad_mount",
    [
        "type=bind,src=/input_pack,source=/tmp/live,dst=/input_pack,readonly",
        "type=bind,src=/input_pack,dst=/input_pack,destination=/tmp/live,readonly",
        "type=bind,src=/input_pack,dst=/input_pack,target=/tmp/live,readonly",
    ],
)
def test_executor_acceptance_rejects_conflicting_docker_mount_aliases(bad_mount: str) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    policy = list(acceptance["docker_run_policy"])
    policy[policy.index("type=bind,src=/input_pack,dst=/input_pack,readonly")] = bad_mount
    acceptance["docker_run_policy"] = policy

    with pytest.raises(cs.ControlledShadowValidationError, match="conflicting mount"):
        cs.validate_executor_acceptance(acceptance, envelope)


@pytest.mark.parametrize(
    "image",
    [
        "investintell/quant-engine:latest",
        "investintell/quant-engine@sha256:" + "f" * 64,
    ],
)
def test_executor_acceptance_requires_pinned_matching_image(image: str) -> None:
    envelope = _artifact("shadow_job_envelope.json")
    acceptance = _artifact("executor_acceptance.json")
    acceptance["docker_run_policy"] = [*acceptance["docker_run_policy"][:-1], image]

    with pytest.raises(cs.ControlledShadowValidationError, match="image"):
        cs.validate_executor_acceptance(acceptance, envelope)


def test_shadow_result_rejects_non_zero_mismatch_count(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    result = _json(root / "shadow_result_manifest.json")
    result["divergence_summary"]["mismatch_count"] = 1
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(cs.ControlledShadowValidationError, match="mismatch_count"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_shadow_result_rejects_false_mismatch_count(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    result = _json(root / "shadow_result_manifest.json")
    result["divergence_summary"]["mismatch_count"] = False
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(cs.ControlledShadowValidationError, match="mismatch_count"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


@pytest.mark.parametrize("field", ("memory_peak_bytes", "cpu_time_ms"))
def test_shadow_result_rejects_boolean_resource_counters(tmp_path: Path, field: str) -> None:
    root = _copy_bundle(tmp_path)
    result = _json(root / "shadow_result_manifest.json")
    result[field] = False
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(cs.ControlledShadowValidationError, match=field):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_controlled_shadow_rejects_request_result_execution_window_mismatch(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    result = _json(root / "shadow_result_manifest.json")
    result["started_at"] = "2030-01-01T00:00:00Z"
    result["finished_at"] = "2030-01-01T00:00:01Z"
    result["duration_ms"] = 1000
    _write_json(root / "shadow_result_manifest.json", result)

    with pytest.raises(cs.ControlledShadowValidationError, match="execution_window.started_at"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_reproducibility_report_requires_mismatch_count_zero() -> None:
    report = _artifact("reproducibility_report.json")
    report["mismatch_count"] = 1

    with pytest.raises(cs.ControlledShadowValidationError, match="mismatch_count"):
        cs.validate_reproducibility_report(report)


def test_reproducibility_report_rejects_false_mismatch_count() -> None:
    report = _artifact("reproducibility_report.json")
    report["mismatch_count"] = False

    with pytest.raises(cs.ControlledShadowValidationError, match="mismatch_count"):
        cs.validate_reproducibility_report(report)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("float_abs_tolerance", 1.0),
        ("float_rel_tolerance", 1.0),
        ("hash_comparison", "approximate"),
        ("unexpected_tolerance", 0.0),
    ],
)
def test_baseline_comparison_rejects_unpinned_numeric_tolerances(field: str, bad: object) -> None:
    baseline = _artifact("baseline_comparison.json")
    baseline["numeric_tolerances"][field] = bad

    with pytest.raises(cs.ControlledShadowValidationError, match="numeric_tolerances"):
        cs.validate_baseline_comparison(baseline)


def test_baseline_comparison_rejects_unexpected_forbidden_effect_markers() -> None:
    baseline = _artifact("baseline_comparison.json")
    baseline["forbidden_effects"]["db_write"] = True

    with pytest.raises(
        cs.ControlledShadowValidationError,
        match="baseline_comparison.forbidden_effects: unexpected fields",
    ):
        cs.validate_baseline_comparison(baseline)


@pytest.mark.parametrize("summary", ["divergence_summary", "materiality_summary"])
def test_baseline_comparison_rejects_unexpected_summary_fields(summary: str) -> None:
    baseline = _artifact("baseline_comparison.json")
    baseline[summary]["unexpected_output_paths"] = ["artifact://shadow/unexpected.json"]

    with pytest.raises(
        cs.ControlledShadowValidationError,
        match=f"baseline_comparison.{summary}: unexpected fields",
    ):
        cs.validate_baseline_comparison(baseline)


def test_baseline_comparison_rejects_unexpected_evaluation_fields() -> None:
    baseline = _artifact("baseline_comparison.json")
    baseline["evaluation"]["runtime_activation_attempt"] = True

    with pytest.raises(
        cs.ControlledShadowValidationError,
        match="baseline_comparison.evaluation: unexpected fields",
    ):
        cs.validate_baseline_comparison(baseline)


@pytest.mark.parametrize("summary", ["divergence_summary", "materiality_summary"])
def test_shadow_result_rejects_unexpected_summary_fields(summary: str) -> None:
    result = _artifact("shadow_result_manifest.json")
    result[summary]["unexpected_output_paths"] = ["artifact://shadow/unexpected.json"]
    evidence_hashes = {
        field: result[field]
        for field in (
            "output_manifest_sha256",
            "baseline_comparison_sha256",
            "invariant_report_sha256",
            "reproducibility_report_sha256",
            "no_side_effects_report_sha256",
        )
    }

    with pytest.raises(
        cs.ControlledShadowValidationError,
        match=f"shadow_result_manifest.{summary}: unexpected fields",
    ):
        cs.validate_shadow_result_manifest(result, evidence_hashes=evidence_hashes)


def test_no_side_effects_report_rejects_unexpected_check_fields() -> None:
    report = _artifact("no_side_effects_report.json")
    report["checks"][0]["observed"] = True

    with pytest.raises(cs.ControlledShadowValidationError, match="no_side_effects_report.check: unexpected fields"):
        cs.validate_no_side_effects_report(report)


def test_acceptance_report_rejects_unexpected_rule_fields() -> None:
    report = _artifact("acceptance_report.json")
    report["rules"][0]["runtime_activation"] = True

    with pytest.raises(cs.ControlledShadowValidationError, match="acceptance_report.rule: unexpected fields"):
        cs.validate_acceptance_report(report)


@pytest.mark.parametrize("evidence", [None, "runtime_activation=true observed"])
def test_acceptance_report_requires_pinned_rule_evidence(evidence: str | None) -> None:
    report = _artifact("acceptance_report.json")
    rule = next(item for item in report["rules"] if item["id"] == "runtime_activation_false")
    if evidence is None:
        rule.pop("evidence")
    else:
        rule["evidence"] = evidence

    with pytest.raises(cs.ControlledShadowValidationError, match="evidence"):
        cs.validate_acceptance_report(report)


def test_output_manifest_rejects_hash_drift(tmp_path: Path) -> None:
    root = _copy_bundle(tmp_path)
    report = root / "controlled_shadow_report.md"
    report.write_text(report.read_text(encoding="utf-8") + "\nHash drift sentinel.\n", encoding="utf-8")

    with pytest.raises(cs.ControlledShadowValidationError, match="sha256"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_output_manifest_rejects_unexpected_artifact_entry_fields_with_refreshed_hash(
    tmp_path: Path,
) -> None:
    root = _copy_bundle(tmp_path)
    output_manifest = _json(root / "output_manifest.json")
    output_manifest["artifacts"][0]["runtime_activation"] = True
    _write_json(root / "output_manifest.json", output_manifest)
    _refresh_shadow_result_output_manifest_hash(root)

    with pytest.raises(cs.ControlledShadowValidationError, match=r"output_manifest\[.*\]: unexpected fields"):
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


@pytest.mark.parametrize(
    ("artifact", "field", "bad", "match"),
    [
        ("observability_evidence.json", "status", "fail", "observability_evidence"),
        ("observability_evidence.json", "runtime_activation", True, "runtime_activation"),
        ("rollback_evidence.json", "status", "fail", "rollback_evidence"),
        ("rollback_evidence.json", "runtime_activation", True, "runtime_activation"),
    ],
)
def test_controlled_shadow_rejects_invalid_evidence_with_refreshed_hashes(
    tmp_path: Path,
    artifact: str,
    field: str,
    bad: object,
    match: str,
) -> None:
    root = _copy_bundle(tmp_path)
    payload = _json(root / artifact)
    payload[field] = bad
    _write_json(root / artifact, payload)
    output_manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, output_manifest, artifact)
    _write_json(root / "output_manifest.json", output_manifest)
    _refresh_shadow_result_output_manifest_hash(root)

    with pytest.raises(cs.ControlledShadowValidationError, match=match):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


def test_controlled_shadow_rejects_failed_nested_observability_evidence_with_refreshed_hashes(
    tmp_path: Path,
) -> None:
    root = _copy_bundle(tmp_path)
    payload = _json(root / "observability_evidence.json")
    payload["evidence"][0]["status"] = "fail"
    _write_json(root / "observability_evidence.json", payload)
    output_manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, output_manifest, "observability_evidence.json")
    _write_json(root / "output_manifest.json", output_manifest)
    _refresh_shadow_result_output_manifest_hash(root)

    with pytest.raises(cs.ControlledShadowValidationError, match="observability_evidence"):
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


@pytest.mark.parametrize(
    ("rel", "token", "marker"),
    [
        ("logs/external_executor.log", "audit=db_write=true", "db_write=true"),
        ("logs/control_plane_validator.log", "audit=runtime_activation_attempt=false", "runtime_activation_attempt="),
    ],
)
def test_logs_reject_embedded_forbidden_markers_in_values(
    tmp_path: Path,
    rel: str,
    token: str,
    marker: str,
) -> None:
    root = _copy_bundle(tmp_path)
    log = root / rel
    log.write_text(log.read_text(encoding="utf-8").strip() + f" {token}\n", encoding="utf-8")
    output_manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, output_manifest, rel)
    _write_json(root / "output_manifest.json", output_manifest)
    _refresh_shadow_result_output_manifest_hash(root)

    with pytest.raises(cs.ControlledShadowValidationError, match=rf"embedded forbidden log marker {marker}"):
        cs.verify_controlled_shadow(root, workspace_root=ROOT)


@pytest.mark.parametrize(
    ("rel", "old", "new", "duplicate"),
    [
        ("logs/external_executor.log", " db_write=false", " db_write=true db_write=false", "db_write"),
        (
            "logs/control_plane_validator.log",
            " runtime_activation=false",
            " runtime_activation=true runtime_activation=false",
            "runtime_activation",
        ),
    ],
)
def test_logs_reject_duplicate_tokens_before_side_effect_checks(
    tmp_path: Path,
    rel: str,
    old: str,
    new: str,
    duplicate: str,
) -> None:
    root = _copy_bundle(tmp_path)
    log = root / rel
    log.write_text(log.read_text(encoding="utf-8").replace(old, new), encoding="utf-8")
    output_manifest = _json(root / "output_manifest.json")
    _refresh_manifest_entry(root, output_manifest, rel)
    _write_json(root / "output_manifest.json", output_manifest)
    _refresh_shadow_result_output_manifest_hash(root)

    with pytest.raises(cs.ControlledShadowValidationError, match=rf"duplicate log token {duplicate}"):
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


def test_immutable_inputs_reject_calibration_output_manifest_artifact_hash_drift(tmp_path: Path) -> None:
    workspace = _copy_immutable_workspace(tmp_path)
    calibration_dir = workspace / "artifacts" / "calibration" / hs.CALIBRATION_ID
    metrics_path = calibration_dir / "metrics_manifest.json"
    metrics = _json(metrics_path)
    metrics["tampered"] = True
    _write_json(metrics_path, metrics)

    with pytest.raises(
        cs.ControlledShadowValidationError,
        match=r"calibration_output_manifest\[metrics_manifest\.json\]: sha256",
    ):
        cs.validate_immutable_inputs(workspace)


def test_immutable_inputs_reject_contract_bundle_file_hash_drift(tmp_path: Path) -> None:
    workspace = _copy_immutable_workspace(tmp_path)
    schema_path = workspace / "contracts" / "quant-engine" / "v1" / "job-request.schema.json"
    schema_path.write_text(schema_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(cs.ControlledShadowValidationError, match="contract bundle verification failed"):
        cs.validate_immutable_inputs(workspace)


def test_controlled_shadow_validator_avoids_productive_imports() -> None:
    source = (ROOT / "src" / "controlled_shadow.py").read_text(encoding="utf-8")

    assert "from src.db" not in source
    assert "import src.db" not in source
    assert "import subprocess" not in source
    assert "from subprocess" not in source
    assert "docker.from_env" not in source
