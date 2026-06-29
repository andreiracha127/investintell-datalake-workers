from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = ROOT / "artifacts" / "runtime" / "open_macro_v03_runtime_skeleton_001"
PLAN = ROOT / "docs" / "planning" / "open_macro_v03_runtime_integration_skeleton_plan_001.md"
RAILWAY_CI_DOCKERFILE = ROOT / "docker" / "railway-ci" / "Dockerfile"


def _json(name: str) -> dict:
    return json.loads((RUNTIME_ROOT / name).read_text(encoding="utf-8"))


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _valid_envelope() -> dict:
    return {
        "schema_version": 1,
        "runtime_skeleton_id": "open_macro_v03_runtime_skeleton_001",
        "strategy": "open_macro_v03",
        "a5_preflight_id": "open_macro_v03_a5_preflight_001",
        "input_pack_id": "open_macro_v03_certified_input_pack_001",
        "input_pack_sha256": "ae8b76e5959cb5e9c10ced7b33fc13a01a3484865deeead56c5b83b1c440e08f",
        "calibration_id": "open_macro_v03_calibration_001",
        "calibration_config_sha256": "869e392bd49c8f7e0bf60890d1658ef3cf0483655af3a1c9f105b99cd29c268c",
        "contract_bundle_sha256": "4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a",
        "contract_version": "1.0.0",
        "engine_commit": "ee39adbe6cb6541d4fdfa78f1428478ffffaf638",
        "engine_image_digest": "sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b",
        "request_id": "req-open-macro-v03-runtime-skeleton-001",
        "correlation_id": "corr-open-macro-v03-runtime-skeleton-001",
        "execution_id": "exec-open-macro-v03-runtime-skeleton-001",
        "mode": "inert_skeleton",
        "runtime_activation": False,
        "A5": "blocked",
        "freeze_ready": False,
        "official_result": False,
        "allow_db_write": False,
        "allow_allocator_publish": False,
        "allocator_publish": False,
        "db_write_official": False,
        "production_endpoint_activation": "none",
        "feature_flag_name": "open_macro_v03_runtime_activation",
        "feature_flag_default": False,
        "docker_execution_from_backend": False,
        "execution_policy": "artifact_only_external_orchestrator_no_productive_backend_docker",
        "output_artifact_uri": "artifact://runtime/open_macro_v03_runtime_skeleton_001/envelope-001",
        "formula_changes": "none",
        "input_pack_changes": "none",
        "calibration_pack_changes": "none",
        "contract_v1_changes": "none",
    }


def _valid_result() -> dict:
    return {
        "schema_version": 1,
        "runtime_skeleton_id": "open_macro_v03_runtime_skeleton_001",
        "strategy": "open_macro_v03",
        "a5_preflight_id": "open_macro_v03_a5_preflight_001",
        "request_id": "req-open-macro-v03-runtime-skeleton-001",
        "correlation_id": "corr-open-macro-v03-runtime-skeleton-001",
        "execution_id": "exec-open-macro-v03-runtime-skeleton-001",
        "result_mode": "artifact_only_inert_manifest",
        "status": "not_executed",
        "runtime_activation": False,
        "A5": "blocked",
        "freeze_ready": False,
        "official_result": False,
        "allow_db_write": False,
        "allow_allocator_publish": False,
        "allocator_publish": False,
        "db_write_official": False,
        "production_endpoint_activation": "none",
        "feature_flag_name": "open_macro_v03_runtime_activation",
        "feature_flag_default": False,
        "docker_execution_from_backend": False,
        "artifact_uri": "artifact://runtime/open_macro_v03_runtime_skeleton_001/result-001",
    }


def test_runtime_skeleton_manifest_keeps_governance_inert() -> None:
    manifest = _json("runtime_skeleton_manifest.json")

    assert manifest["runtime_skeleton_id"] == "open_macro_v03_runtime_skeleton_001"
    assert manifest["strategy"] == "open_macro_v03"
    assert manifest["a5_preflight_id"] == "open_macro_v03_a5_preflight_001"
    assert manifest["a5_preflight_readiness_merge_commit"] == "42d48e5afb616f24125457b2f5be02d7b959ac63"
    assert manifest["pr8_head"] == "8cc383af0c78937b2a95bf3db946e94875431573"
    assert manifest["remote_railway_ci"] == "PASS"
    assert manifest["A3"] == "open_macro_v03"
    assert manifest["A4"] == "runtime_integration_skeleton_prepared"
    assert manifest["A5"] == "blocked"
    assert manifest["runtime_activation"] is False
    assert manifest["freeze_ready"] is False
    assert manifest["official_result"] is False
    assert manifest["allocator_publish"] is False
    assert manifest["db_write_official"] is False
    assert manifest["production_endpoint_activation"] == "none"
    assert manifest["feature_flag_default"] is False
    assert manifest["docker_execution_from_backend"] is False
    assert manifest["formula_changes"] == "none"
    assert manifest["input_pack_changes"] == "none"
    assert manifest["calibration_pack_changes"] == "none"
    assert manifest["contract_v1_changes"] == "none"


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation", True),
        ("A5", "unblocked"),
        ("freeze_ready", True),
        ("official_result", True),
        ("allow_db_write", True),
        ("allow_allocator_publish", True),
        ("allocator_publish", True),
        ("db_write_official", True),
        ("production_endpoint_activation", "public"),
        ("feature_flag_default", True),
        ("docker_execution_from_backend", True),
        ("input_pack_sha256", "0" * 64),
        ("calibration_config_sha256", "0" * 64),
        ("contract_bundle_sha256", "0" * 64),
        ("engine_commit", "0" * 40),
        ("engine_image_digest", "sha256:" + "0" * 64),
        ("formula_changes", "changed"),
        ("input_pack_changes", "changed"),
        ("calibration_pack_changes", "changed"),
        ("contract_v1_changes", "changed"),
    ],
)
def test_runtime_job_envelope_schema_rejects_activation_and_identity_drift(field: str, bad: object) -> None:
    schema = _json("runtime_job_envelope.schema.json")
    envelope = _valid_envelope()
    jsonschema.validate(envelope, schema)

    broken = deepcopy(envelope)
    broken[field] = bad
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(broken, schema)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("runtime_activation", True),
        ("A5", "unblocked"),
        ("freeze_ready", True),
        ("official_result", True),
        ("allow_db_write", True),
        ("allow_allocator_publish", True),
        ("allocator_publish", True),
        ("db_write_official", True),
        ("production_endpoint_activation", "public"),
        ("feature_flag_name", "other_runtime_activation"),
        ("feature_flag_default", True),
        ("docker_execution_from_backend", True),
    ],
)
def test_runtime_result_manifest_schema_rejects_official_results_and_side_effects(field: str, bad: object) -> None:
    schema = _json("runtime_result_manifest.schema.json")
    result = _valid_result()
    jsonschema.validate(result, schema)

    broken = deepcopy(result)
    broken[field] = bad
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(broken, schema)


def test_runtime_result_rejected_status_requires_failure_class() -> None:
    schema = _json("runtime_result_manifest.schema.json")
    result = _valid_result()
    result["status"] = "rejected"

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(result, schema)

    result["failure_class"] = "runtime_activation_attempt"
    result["side_effect_attempt_count"] = 1
    result["side_effect_attempt_evidence_sha256"] = "a" * 64
    jsonschema.validate(result, schema)


@pytest.mark.parametrize(
    "failure_class",
    [
        "runtime_activation_attempt",
        "official_db_write_attempt",
        "allocator_publish_attempt",
        "production_endpoint_activation_attempt",
        "docker_execution_from_backend_attempt",
    ],
)
def test_runtime_result_side_effect_rejections_require_audit_evidence(failure_class: str) -> None:
    schema = _json("runtime_result_manifest.schema.json")
    result = _valid_result()
    result.update(
        {
            "status": "rejected",
            "failure_class": failure_class,
            "side_effect_attempt_count": 1,
            "side_effect_attempt_evidence_sha256": "a" * 64,
        }
    )
    jsonschema.validate(result, schema)

    for required_field in (
        "side_effect_attempt_count",
        "side_effect_attempt_evidence_sha256",
    ):
        broken = deepcopy(result)
        del broken[required_field]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(broken, schema)


def test_runtime_result_identity_drift_requires_observed_identity_values() -> None:
    schema = _json("runtime_result_manifest.schema.json")
    envelope = _valid_envelope()
    result = _valid_result()
    required_fields = [
        "input_pack_id",
        "input_pack_sha256",
        "calibration_id",
        "calibration_config_sha256",
        "engine_commit",
        "engine_image_digest",
        "observed_input_pack_id",
        "observed_input_pack_sha256",
        "observed_calibration_id",
        "observed_calibration_config_sha256",
        "observed_engine_commit",
        "observed_engine_image_digest",
    ]
    result.update(
        {
            "status": "rejected",
            "failure_class": "identity_drift",
            "input_pack_id": envelope["input_pack_id"],
            "input_pack_sha256": envelope["input_pack_sha256"],
            "calibration_id": envelope["calibration_id"],
            "calibration_config_sha256": envelope["calibration_config_sha256"],
            "engine_commit": envelope["engine_commit"],
            "engine_image_digest": envelope["engine_image_digest"],
            "observed_input_pack_id": "open_macro_v03_certified_input_pack_drifted",
            "observed_input_pack_sha256": "0" * 64,
            "observed_calibration_id": "open_macro_v03_calibration_drifted",
            "observed_calibration_config_sha256": "1" * 64,
            "observed_engine_commit": "2" * 40,
            "observed_engine_image_digest": "sha256:" + "3" * 64,
        }
    )
    jsonschema.validate(result, schema)

    for required_field in required_fields:
        broken = deepcopy(result)
        del broken[required_field]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(broken, schema)


def test_runtime_result_contract_drift_requires_observed_contract_identity_values() -> None:
    schema = _json("runtime_result_manifest.schema.json")
    envelope = _valid_envelope()
    result = _valid_result()
    required_fields = [
        "contract_bundle_sha256",
        "contract_version",
        "engine_commit",
        "engine_image_digest",
        "observed_contract_bundle_sha256",
        "observed_contract_version",
        "observed_engine_commit",
        "observed_engine_image_digest",
    ]
    result.update(
        {
            "status": "rejected",
            "failure_class": "contract_drift",
            "contract_bundle_sha256": envelope["contract_bundle_sha256"],
            "contract_version": envelope["contract_version"],
            "engine_commit": envelope["engine_commit"],
            "engine_image_digest": envelope["engine_image_digest"],
            "observed_contract_bundle_sha256": "4" * 64,
            "observed_contract_version": "1.0.1",
            "observed_engine_commit": "5" * 40,
            "observed_engine_image_digest": "sha256:" + "6" * 64,
        }
    )
    jsonschema.validate(result, schema)

    for required_field in required_fields:
        broken = deepcopy(result)
        del broken[required_field]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(broken, schema)


def test_feature_flag_guard_defaults_false_and_allows_no_environment() -> None:
    report = _json("feature_flag_guard_report.json")

    assert report["feature_flag_name"] == "open_macro_v03_runtime_activation"
    assert report["default"] is False
    assert report["production_default"] is False
    assert report["feature_flag_default"] is False
    assert report["activation_allowed"] is False
    assert report["allowed_environments"] == []
    assert report["blast_radius"] == 0
    assert report["runtime_activation"] is False
    assert report["status"] == "pass_default_false"


def test_no_side_effects_report_blocks_allocator_db_endpoint_and_backend_docker() -> None:
    report = _json("no_side_effects_report.json")

    assert report["A5"] == "blocked"
    assert report["runtime_activation"] is False
    assert report["freeze_ready"] is False
    assert report["official_result"] is False
    assert report["db_write_official"] is False
    assert report["production_endpoint_activation"] == "none"
    assert report["feature_flag_default"] is False
    assert report["docker_execution_from_backend"] is False
    assert report["stop_if_backend_wiring_required"] == "missing_safe_control_plane_abstraction"
    assert {check["id"] for check in report["checks"]} >= {
        "runtime_activation",
        "allocator_publish",
        "official_db_write",
        "production_endpoint_activation",
        "docker_execution_from_backend",
        "formula_change",
        "input_pack_change",
        "calibration_pack_change",
        "contract_v1_change",
    }
    assert all(check["status"] == "pass" and check["allowed"] is False for check in report["checks"])


def test_plan_and_report_document_missing_backend_control_plane_abstraction() -> None:
    plan = _text(PLAN)
    report = _text(RUNTIME_ROOT / "integration_readiness_report.md")

    for text in (plan, report):
        assert "missing safe control-plane abstraction" in text
        assert "No backend runtime wiring" in text or "does not implement backend runtime integration" in text
        assert "runtime_activation" in text
        assert "`false`" in text
        assert "A5" in text
        assert "`blocked`" in text
        assert "Docker execution from productive backend" in text or "Docker execution from backend" in text


def test_contract_bundle_identity_is_referenced_without_contract_v1_change() -> None:
    contract_manifest = json.loads((ROOT / "contracts/quant-engine/v1/manifest.json").read_text(encoding="utf-8"))
    envelope_schema = _json("runtime_job_envelope.schema.json")

    assert contract_manifest["contract_version"] == "1.0.0"
    assert contract_manifest["bundle_sha256"] == "sha256:4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a"
    assert envelope_schema["properties"]["contract_version"]["const"] == "1.0.0"
    assert envelope_schema["properties"]["contract_bundle_sha256"]["const"] == (
        "4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a"
    )


def test_railway_ci_includes_runtime_skeleton_governance_test() -> None:
    text = _text(RAILWAY_CI_DOCKERFILE)

    assert (
        "COPY artifacts/runtime/open_macro_v03_runtime_skeleton_001 "
        "/app/artifacts/runtime/open_macro_v03_runtime_skeleton_001"
    ) in text
    assert "COPY docs/planning /app/docs/planning" in text
    assert "tests/test_runtime_integration_skeleton.py" in text
