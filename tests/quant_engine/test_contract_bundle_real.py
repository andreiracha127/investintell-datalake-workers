from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))

from investintell_quant_engine.contract_bundle import verify_bundle

V1 = ROOT / "contracts" / "quant-engine" / "v1"

# The backend mirrors these schema hashes in
# backend/app/contracts/quant_engine_v1.py (SCHEMA_SHA256). Recording them here
# makes any drift in either repository fail loud.
BACKEND_MIRROR_SHA256 = {
    "engine-manifest.schema.json": "26757f96bdff5ac90b0e6422f213faac0db5b5289def9c2f0eae7b7f9fa45b9f",
    "job-request.schema.json": "a143bafe60f8414a3b1c04cc93b4ae8568ad51264c8f7e55d83ce9b3a633d593",
    "job-result.schema.json": "95626166653241b6fed455c18b530b057cb66837e920dbfd6fe1d71880ea4fe7",
}


def test_real_contract_bundle_verifies():
    result = verify_bundle(V1)
    assert result["ok"] is True, result
    assert result["bundle_sha256_match"] is True
    assert result["contract_version"] == "1.0.0"


def test_manifest_records_all_schemas_and_fixtures():
    manifest = json.loads((V1 / "manifest.json").read_text(encoding="utf-8"))
    paths = {f["path"] for f in manifest["files"]}
    assert {"job-request.schema.json", "job-result.schema.json", "engine-manifest.schema.json"} <= paths
    assert any(p.startswith("fixtures/valid/") for p in paths)
    assert any(p.startswith("fixtures/invalid/") for p in paths)


def test_schema_hashes_match_backend_mirror():
    for name, expected in BACKEND_MIRROR_SHA256.items():
        actual = hashlib.sha256((V1 / name).read_bytes()).hexdigest()
        assert actual == expected, f"{name} drifted from backend mirror"
