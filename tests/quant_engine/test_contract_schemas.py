from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_ROOT = ROOT / "contracts" / "quant-engine" / "v1"


def _schema(name: str) -> dict:
    return json.loads((SCHEMA_ROOT / name).read_text(encoding="utf-8"))


def test_job_request_schema_has_required_contract_fields() -> None:
    schema = _schema("job-request.schema.json")

    assert schema["title"] == "QuantEngineJobRequest"
    variants = [schema["$defs"][variant["$ref"].removeprefix("#/$defs/")] for variant in schema["oneOf"]]
    assert {variant["properties"]["job_type"]["const"] for variant in variants} == {
        "a3_qc_parity",
        "certified_input_pack_dry_run",
    }
    a3_variant = schema["$defs"]["a3_qc_parity_request"]
    input_pack_variant = schema["$defs"]["certified_input_pack_dry_run_request"]
    assert set(a3_variant["required"]) >= {
        "input_bundle_uri",
        "input_bundle_logical_hash",
        "config_catalog_uri",
        "config_catalog_hash",
        "expected_parent_hashes",
        "engine_image_digest",
        "quant_core_version",
        "offline",
        "jobs",
        "output_uri",
    }
    assert set(input_pack_variant["required"]) >= {
        "input_pack_uri",
        "input_pack_sha256",
        "contract_bundle_sha256",
        "source_snapshot_sha256",
        "engine_image_digest",
        "offline",
        "jobs",
        "output_uri",
    }
    for variant in variants:
        assert variant["properties"]["offline"]["const"] is True


def test_job_result_schema_keeps_runtime_activation_disabled() -> None:
    schema = _schema("job-result.schema.json")

    assert schema["title"] == "QuantEngineJobResult"
    variants = [schema["$defs"][variant["$ref"].removeprefix("#/$defs/")] for variant in schema["oneOf"]]
    assert {variant["properties"]["job_type"]["const"] for variant in variants} == {
        "a3_qc_parity",
        "certified_input_pack_dry_run",
    }
    assert {variant["properties"]["a4_status"]["const"] for variant in variants} == {
        "harness_ready_provisional_A3",
        "input_pack_certified_for_calibration",
    }
    for variant in variants:
        assert variant["properties"]["runtime_activation"]["const"] is False
        assert variant["properties"]["a3_status"]["const"] == "open_macro_v03"
        assert variant["properties"]["a5_status"]["const"] == "blocked"


def test_engine_manifest_schema_declares_offline_execution() -> None:
    schema = _schema("engine-manifest.schema.json")

    assert schema["title"] == "EngineManifest"
    assert schema["properties"]["offline"]["const"] is True
    assert schema["properties"]["runtime_activation"]["const"] is False

