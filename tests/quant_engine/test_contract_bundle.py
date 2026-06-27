from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))
sys.path.insert(0, str(ROOT))

from investintell_quant_engine.contract_bundle import (
    build_manifest,
    bundle_sha256,
    verify_bundle,
    write_manifest,
)


def _seed_bundle(tmp_path: Path) -> Path:
    (tmp_path / "job-request.schema.json").write_text('{"title":"req"}', encoding="utf-8")
    (tmp_path / "job-result.schema.json").write_text('{"title":"res"}', encoding="utf-8")
    fx = tmp_path / "fixtures" / "valid"
    fx.mkdir(parents=True)
    (fx / "minimal.json").write_text('{"ok":true}', encoding="utf-8")
    return tmp_path


def test_build_manifest_lists_schemas_and_fixtures_with_sha256(tmp_path):
    _seed_bundle(tmp_path)
    manifest = build_manifest(tmp_path, contract_version="1.0.0")
    paths = [f["path"] for f in manifest["files"]]
    assert paths == [
        "fixtures/valid/minimal.json",
        "job-request.schema.json",
        "job-result.schema.json",
    ]
    assert manifest["contract_version"] == "1.0.0"
    assert manifest["bundle_sha256"].startswith("sha256:")
    for f in manifest["files"]:
        assert len(f["sha256"]) == 64


def test_manifest_excludes_itself(tmp_path):
    _seed_bundle(tmp_path)
    write_manifest(tmp_path, contract_version="1.0.0")
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert "manifest.json" not in [f["path"] for f in manifest["files"]]


def test_bundle_sha256_is_order_independent(tmp_path):
    files_a = [{"path": "b", "sha256": "2"}, {"path": "a", "sha256": "1"}]
    files_b = [{"path": "a", "sha256": "1"}, {"path": "b", "sha256": "2"}]
    assert bundle_sha256(files_a) == bundle_sha256(files_b)


def test_verify_passes_on_intact_bundle(tmp_path):
    _seed_bundle(tmp_path)
    write_manifest(tmp_path, contract_version="1.0.0")
    result = verify_bundle(tmp_path)
    assert result["ok"] is True
    assert result["mismatched"] == []
    assert result["bundle_sha256_match"] is True


def test_verify_detects_tampered_file(tmp_path):
    _seed_bundle(tmp_path)
    write_manifest(tmp_path, contract_version="1.0.0")
    (tmp_path / "job-request.schema.json").write_text('{"title":"TAMPERED"}', encoding="utf-8")
    result = verify_bundle(tmp_path)
    assert result["ok"] is False
    assert "job-request.schema.json" in result["mismatched"]


def test_verify_detects_missing_file(tmp_path):
    _seed_bundle(tmp_path)
    write_manifest(tmp_path, contract_version="1.0.0")
    (tmp_path / "job-result.schema.json").unlink()
    result = verify_bundle(tmp_path)
    assert result["ok"] is False
    assert "job-result.schema.json" in result["missing"]


def test_verify_detects_corrupted_bundle_sha(tmp_path):
    _seed_bundle(tmp_path)
    write_manifest(tmp_path, contract_version="1.0.0")
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    manifest["bundle_sha256"] = "sha256:deadbeef"
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    result = verify_bundle(tmp_path)
    assert result["ok"] is False
    assert result["bundle_sha256_match"] is False
