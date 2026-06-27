from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[1]
SHADOW_ROOT = ROOT / "artifacts" / "shadow" / "open_macro_v03_shadow_001"
DOC = ROOT / "docs" / "shadow" / "open_macro_v03_shadow_readiness_001.md"


def _json(name: str) -> dict:
    return json.loads((SHADOW_ROOT / name).read_text(encoding="utf-8"))


def test_shadow_manifest_pins_validated_calibration_without_activation() -> None:
    manifest = _json("shadow_manifest.json")

    assert manifest["shadow_id"] == "open_macro_v03_shadow_001"
    assert manifest["status"] == "readiness_candidate"
    assert manifest["calibration_id"] == "open_macro_v03_calibration_001"
    assert manifest["calibration_001_merge_commit"] == "08fccef698195decaf814fcdd03c45e249bae8ad"
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
        "as_of": "2026-06-27",
        "strategy": "open_macro_v03",
        "mode": "shadow",
        "runtime_activation": False,
        "allow_db_write": False,
        "allow_allocator_publish": False,
        "execution_policy": "isolated_external_executor_no_productive_runtime_docker",
        "output_artifact_uri": "artifact://shadow/open_macro_v03_shadow_001/exec-open-macro-v03-shadow-001",
        "output_manifest_sha256": "b" * 64,
    }

    jsonschema.validate(envelope, schema)
    for field in ("runtime_activation", "allow_db_write", "allow_allocator_publish"):
        bad = dict(envelope)
        bad[field] = True
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bad, schema)


def test_shadow_result_manifest_schema_keeps_result_unofficial() -> None:
    schema = _json("shadow_result_manifest.schema.json")
    jsonschema.Draft202012Validator.check_schema(schema)

    result = {
        "schema_version": 1,
        "shadow_id": "open_macro_v03_shadow_001",
        "execution_id": "exec-open-macro-v03-shadow-001",
        "run_fingerprint": "a" * 64,
        "calibration_id": "open_macro_v03_calibration_001",
        "input_pack_sha256": "ae8b76e5959cb5e9c10ced7b33fc13a01a3484865deeead56c5b83b1c440e08f",
        "engine_image_digest": "sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b",
        "output_manifest_sha256": "b" * 64,
        "invariant_report_sha256": "c" * 64,
        "baseline_comparison_sha256": "d" * 64,
        "started_at": "2026-06-27T21:34:05Z",
        "finished_at": "2026-06-27T21:35:05Z",
        "status": "succeeded",
        "failure_class": None,
        "retryable": False,
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
        },
        "runtime_activation": False,
        "official_result": False,
    }

    jsonschema.validate(result, schema)
    for field in ("runtime_activation", "official_result"):
        bad = dict(result)
        bad[field] = True
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bad, schema)


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
