from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[1]
SHADOW_ROOT = ROOT / "artifacts" / "shadow" / "open_macro_v03_shadow_001"
DOC = ROOT / "docs" / "shadow" / "open_macro_v03_shadow_readiness_001.md"
RAILWAY_CI_DOCKERFILE = ROOT / "docker" / "railway-ci" / "Dockerfile"


def _json(name: str) -> dict:
    return json.loads((SHADOW_ROOT / name).read_text(encoding="utf-8"))


def test_shadow_manifest_pins_validated_calibration_without_activation() -> None:
    manifest = _json("shadow_manifest.json")

    assert manifest["shadow_id"] == "open_macro_v03_shadow_001"
    assert manifest["status"] == "readiness_candidate"
    assert manifest["calibration_id"] == "open_macro_v03_calibration_001"
    assert manifest["calibration_001_merge_commit"] == "08fccef698195decaf814fcdd03c45e249bae8ad"
    assert manifest["calibration_pr_head"] == "10a49e1489661070986e241d9e04a8b890b54937"
    assert manifest["engine_commit"] == "ee39adbe6cb6541d4fdfa78f1428478ffffaf638"
    assert manifest["railway_image_digest"] == (
        "sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b"
    )
    assert manifest["runtime_activation"] is False
    assert manifest["A3"] == "open_macro_v03"
    assert manifest["A4"] == "shadow_readiness_prepared"
    assert manifest["A5"] == "blocked"
    assert manifest["freeze_ready"] is False
    assert manifest["allocator_impact"] == "none"
    assert manifest["db_write_mode"] == "none_or_artifact_only"
    assert manifest["feature_flag_default"] is False
    assert manifest["official_result"] is False
    assert manifest["production_endpoint_activation"] == "none"


def test_shadow_job_envelope_schema_is_inert() -> None:
    schema = _json("shadow_job_envelope.schema.json")
    jsonschema.Draft202012Validator.check_schema(schema)

    envelope = {
        "schema_version": 1,
        "shadow_id": "open_macro_v03_shadow_001",
        "calibration_id": "open_macro_v03_calibration_001",
        "input_pack_id": "open_macro_v03_certified_input_pack_001",
        "input_pack_sha256": "ae8b76e5959cb5e9c10ced7b33fc13a01a3484865deeead56c5b83b1c440e08f",
        "calibration_config_sha256": "869e392bd49c8f7e0bf60890d1658ef3cf0483655af3a1c9f105b99cd29c268c",
        "engine_commit": "ee39adbe6cb6541d4fdfa78f1428478ffffaf638",
        "engine_image_digest": "sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b",
        "request_id": "req-open-macro-v03-shadow-001",
        "correlation_id": "corr-open-macro-v03-shadow-001",
        "execution_id": "exec-open-macro-v03-shadow-001",
        "run_fingerprint": "a" * 64,
        "as_of": "2026-06-26",
        "strategy": "open_macro_v03",
        "mode": "shadow",
        "runtime_activation": False,
        "allow_db_write": False,
        "allow_allocator_publish": False,
        "execution_policy": "isolated_external_executor_no_productive_runtime_docker",
        "output_artifact_uri": "artifact://shadow/open_macro_v03_shadow_001/exec-open-macro-v03-shadow-001",
    }

    jsonschema.validate(envelope, schema)
    with_expected_output_hash = dict(envelope)
    with_expected_output_hash["output_manifest_sha256"] = "b" * 64
    jsonschema.validate(with_expected_output_hash, schema)

    for field in ("runtime_activation", "allow_db_write", "allow_allocator_publish"):
        bad = dict(envelope)
        bad[field] = True
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bad, schema)

    drifted_as_of = dict(envelope)
    drifted_as_of["as_of"] = "2026-06-27"
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(drifted_as_of, schema)

    for productive_uri in (
        "s3://prod-allocator/open_macro_v03/exec-001",
        "artifact://shadow/other_shadow/exec-001",
        "db://official/results/exec-001",
    ):
        productive_output = dict(envelope)
        productive_output["output_artifact_uri"] = productive_uri
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(productive_output, schema)


def test_shadow_result_manifest_schema_keeps_result_unofficial() -> None:
    schema = _json("shadow_result_manifest.schema.json")
    jsonschema.Draft202012Validator.check_schema(schema)

    result = {
        "schema_version": 1,
        "shadow_id": "open_macro_v03_shadow_001",
        "request_id": "req-open-macro-v03-shadow-001",
        "correlation_id": "corr-open-macro-v03-shadow-001",
        "execution_id": "exec-open-macro-v03-shadow-001",
        "run_fingerprint": "a" * 64,
        "calibration_id": "open_macro_v03_calibration_001",
        "input_pack_sha256": "ae8b76e5959cb5e9c10ced7b33fc13a01a3484865deeead56c5b83b1c440e08f",
        "engine_image_digest": "sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b",
        "engine_commit": "ee39adbe6cb6541d4fdfa78f1428478ffffaf638",
        "output_artifact_uri": "artifact://shadow/open_macro_v03_shadow_001/exec-open-macro-v03-shadow-001",
        "output_manifest_sha256": "b" * 64,
        "invariant_report_sha256": "c" * 64,
        "baseline_comparison_sha256": "d" * 64,
        "reproducibility_report_sha256": "e" * 64,
        "started_at": "2026-06-27T21:34:05Z",
        "finished_at": "2026-06-27T21:35:05Z",
        "status": "succeeded",
        "retryable": False,
        "duration_ms": 60000,
        "memory_peak_bytes": 524288000,
        "cpu_time_ms": 45000,
        "retry_count": 0,
        "materiality_summary": {
            "threshold_version": "open_macro_v03_shadow_materiality_v1",
            "material_divergence": False,
            "max_relative_delta_pct": 0.0,
        },
        "divergence_summary": {
            "missing_outputs": 0,
            "unexpected_outputs": 0,
            "mismatch_count": 0,
            "nan_or_inf_count": 0,
            "constraint_violations": 0,
            "invariant_failures": 0,
        },
        "runtime_activation": False,
        "allow_db_write": False,
        "allow_allocator_publish": False,
        "official_result": False,
    }

    jsonschema.validate(result, schema)

    for field in ("request_id", "correlation_id"):
        missing_identifier = dict(result)
        del missing_identifier[field]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(missing_identifier, schema)

    success_with_failure_class = dict(result)
    success_with_failure_class["failure_class"] = "executor_failure"
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(success_with_failure_class, schema)

    success_without_output_hash = dict(result)
    del success_without_output_hash["output_manifest_sha256"]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(success_without_output_hash, schema)

    failed_without_artifacts = {
        key: value
        for key, value in result.items()
        if key
        not in {
            "output_manifest_sha256",
            "invariant_report_sha256",
            "baseline_comparison_sha256",
            "reproducibility_report_sha256",
            "materiality_summary",
            "divergence_summary",
            "memory_peak_bytes",
            "cpu_time_ms",
        }
    }
    failed_without_artifacts["status"] = "failed"
    failed_without_artifacts["failure_class"] = "executor_failure"
    failed_without_artifacts["retryable"] = True
    jsonschema.validate(failed_without_artifacts, schema)

    failed_without_failure_class = dict(failed_without_artifacts)
    del failed_without_failure_class["failure_class"]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(failed_without_failure_class, schema)

    succeeded_with_missing_output = deepcopy(result)
    succeeded_with_missing_output["divergence_summary"]["missing_outputs"] = 1
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(succeeded_with_missing_output, schema)

    succeeded_with_hard_relative_delta = deepcopy(result)
    succeeded_with_hard_relative_delta["materiality_summary"]["max_relative_delta_pct"] = 2.1
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(succeeded_with_hard_relative_delta, schema)

    succeeded_at_hard_reject_boundary = deepcopy(result)
    succeeded_at_hard_reject_boundary["materiality_summary"]["max_relative_delta_pct"] = 2.0
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(succeeded_at_hard_reject_boundary, schema)

    succeeded_review_required = deepcopy(result)
    succeeded_review_required["materiality_summary"]["max_relative_delta_pct"] = 1.0
    succeeded_review_required["materiality_summary"]["material_divergence"] = True
    jsonschema.validate(succeeded_review_required, schema)

    succeeded_review_required_suppressed = deepcopy(result)
    succeeded_review_required_suppressed["materiality_summary"]["max_relative_delta_pct"] = 1.0
    succeeded_review_required_suppressed["materiality_summary"]["material_divergence"] = False
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(succeeded_review_required_suppressed, schema)

    succeeded_without_reproducibility = deepcopy(result)
    del succeeded_without_reproducibility["reproducibility_report_sha256"]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(succeeded_without_reproducibility, schema)

    for field in ("engine_commit", "output_artifact_uri"):
        missing_provenance = dict(result)
        del missing_provenance[field]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(missing_provenance, schema)

    productive_artifact_uri = dict(result)
    productive_artifact_uri["output_artifact_uri"] = "s3://prod-allocator/exec-001"
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(productive_artifact_uri, schema)

    for field in ("duration_ms", "memory_peak_bytes", "cpu_time_ms", "retry_count"):
        succeeded_without_operational = deepcopy(result)
        del succeeded_without_operational[field]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(succeeded_without_operational, schema)

        negative_operational = deepcopy(result)
        negative_operational[field] = -1
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(negative_operational, schema)

    for field in ("duration_ms", "retry_count"):
        failed_missing_telemetry = deepcopy(failed_without_artifacts)
        del failed_missing_telemetry[field]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(failed_missing_telemetry, schema)

    failed_without_resource_metrics = deepcopy(failed_without_artifacts)
    for field in ("memory_peak_bytes", "cpu_time_ms"):
        failed_without_resource_metrics.pop(field, None)
    jsonschema.validate(failed_without_resource_metrics, schema)

    other_engine_digest = dict(result)
    other_engine_digest["engine_image_digest"] = "sha256:" + "f" * 64
    jsonschema.validate(other_engine_digest, schema)

    bad_engine_digest = dict(result)
    bad_engine_digest["engine_image_digest"] = "not-a-digest"
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(bad_engine_digest, schema)

    with_regressions = deepcopy(result)
    with_regressions["materiality_summary"]["latency_p95_regression_pct"] = 12.0
    with_regressions["materiality_summary"]["memory_peak_regression_pct"] = 3.0
    with_regressions["materiality_summary"]["retry_rate_delta_pct"] = 0.5
    jsonschema.validate(with_regressions, schema)

    comparison_rejection = deepcopy(result)
    comparison_rejection["status"] = "rejected"
    comparison_rejection["failure_class"] = "hard_relative_delta_exceeded"
    comparison_rejection["materiality_summary"]["material_divergence"] = True
    comparison_rejection["materiality_summary"]["max_relative_delta_pct"] = 2.5
    jsonschema.validate(comparison_rejection, schema)

    comparison_rejection_missing_evidence = deepcopy(comparison_rejection)
    del comparison_rejection_missing_evidence["baseline_comparison_sha256"]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(comparison_rejection_missing_evidence, schema)

    invalid_timestamp = dict(result)
    invalid_timestamp["started_at"] = "not-a-date"
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_timestamp, schema)

    missing_invariant_count = deepcopy(result)
    del missing_invariant_count["divergence_summary"]["invariant_failures"]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(missing_invariant_count, schema)

    assert set(schema["$defs"]["failureClass"]["enum"]) >= {
        "runtime_activation_attempt",
        "official_db_write_attempt",
        "allocator_publish_attempt",
        "invariant_failure",
        "hard_relative_delta_exceeded",
    }

    for field in (
        "runtime_activation",
        "allow_db_write",
        "allow_allocator_publish",
        "official_result",
    ):
        bad = dict(result)
        bad[field] = True
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bad, schema)

        missing_flag = dict(result)
        del missing_flag[field]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(missing_flag, schema)


def test_baseline_comparison_policy_rejects_required_failure_classes() -> None:
    policy = _json("baseline_comparison_policy.json")

    assert policy["candidate"]["official_result"] is False
    assert policy["candidate"]["runtime_activation"] is False
    assert policy["official_baseline"]["remains_official"] is True
    assert policy["materiality_thresholds"]["missing_outputs_max"] == 0
    assert policy["materiality_thresholds"]["unexpected_outputs_max"] == 0
    assert policy["materiality_thresholds"]["mismatch_count_max"] == 0
    assert policy["materiality_thresholds"]["nan_or_inf_count_max"] == 0
    assert policy["materiality_thresholds"]["constraint_violations_max"] == 0
    assert policy["materiality_thresholds"]["invariant_failures_max"] == 0
    assert policy["materiality_thresholds"]["hard_reject_relative_delta_pct"] == 2.0
    assert set(policy["rejection_rules"]) >= {
        "missing_output",
        "unexpected_output",
        "mismatch_count_non_zero",
        "nan_or_inf",
        "constraint_violation",
        "run_fingerprint_inconsistent",
        "output_manifest_incomplete",
        "non_reproducible_result",
        "runtime_activation_attempt",
        "official_db_write_attempt",
        "allocator_publish_attempt",
        "production_endpoint_activation_attempt",
        "invariant_failure",
        "hard_relative_delta_exceeded",
    }
    assert set(policy["promotion_to_shadow_pilot_rules"]) >= {
        "invariant_failures_zero",
        "relative_deltas_below_hard_reject_threshold",
        "no_official_db_write_attempt",
        "no_allocator_publish_attempt",
    }
    assert policy["forbidden_effects"]["allocator_publish"] == "forbidden"
    assert policy["forbidden_effects"]["official_db_write"] == "forbidden"
    assert policy["forbidden_effects"]["production_endpoint_activation"] == "forbidden"


def test_shadow_readiness_doc_declares_no_runtime_or_a5_activation() -> None:
    text = DOC.read_text(encoding="utf-8")

    assert "does not start shadow execution" in text
    assert "A5: `blocked`" in text
    assert "freeze_ready: `false`" in text
    assert "runtime_activation: `false`" in text
    assert "No official DB writes" in text
    assert "No allocator publish path" in text


def test_acceptance_criteria_documents_execution_window_gate() -> None:
    text = (SHADOW_ROOT / "acceptance_criteria.md").read_text(encoding="utf-8")

    assert "non-positive execution window" in text
    assert "JSON Schema cannot compare two fields" in text


def test_railway_ci_runs_shadow_readiness_gate() -> None:
    text = RAILWAY_CI_DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "COPY artifacts/shadow/open_macro_v03_shadow_001 "
        "/app/artifacts/shadow/open_macro_v03_shadow_001"
    ) in text
    assert "COPY docs/shadow /app/docs/shadow" in text
    assert "tests/test_shadow_readiness.py" in text
