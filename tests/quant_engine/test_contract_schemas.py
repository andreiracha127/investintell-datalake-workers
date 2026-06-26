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
    assert set(schema["required"]) >= {
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
    assert schema["properties"]["offline"]["const"] is True


def test_job_result_schema_keeps_runtime_activation_disabled() -> None:
    schema = _schema("job-result.schema.json")

    assert schema["title"] == "QuantEngineJobResult"
    assert schema["properties"]["runtime_activation"]["const"] is False
    assert schema["properties"]["a3_status"]["const"] == "open_macro_v03"
    assert schema["properties"]["a4_status"]["const"] == "harness_ready_provisional_A3"
    assert schema["properties"]["a5_status"]["const"] == "blocked"


def test_engine_manifest_schema_declares_offline_execution() -> None:
    schema = _schema("engine-manifest.schema.json")

    assert schema["title"] == "EngineManifest"
    assert schema["properties"]["offline"]["const"] is True
    assert schema["properties"]["runtime_activation"]["const"] is False

