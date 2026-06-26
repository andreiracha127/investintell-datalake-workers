from __future__ import annotations

import json
import shutil
from pathlib import Path

from src.input_packs import build_manifest, verify_pack

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "fixtures" / "input_packs" / "golden" / "certified_input_pack"


def _copy_pack(tmp_path: Path) -> Path:
    target = tmp_path / "certified_input_pack"
    shutil.copytree(GOLDEN, target)
    return target


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_golden_pack_verifies_offline() -> None:
    result = verify_pack(GOLDEN)
    assert result["ok"] is True
    assert result["input_pack_sha256_match"] is True
    assert result["runtime_activation_ok"] is True
    assert result["provenance_complete"] is True


def test_build_manifest_reproduces_golden_hash(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    golden_manifest = _read_json(pack / "manifest.json")

    rebuilt = build_manifest(pack, golden_manifest)

    assert rebuilt == golden_manifest


def test_pack_hash_is_path_independent(tmp_path: Path) -> None:
    first = _copy_pack(tmp_path / "first")
    second = _copy_pack(tmp_path / "second")
    first_manifest = _read_json(first / "manifest.json")
    second_manifest = _read_json(second / "manifest.json")

    assert build_manifest(first, first_manifest)["input_pack_sha256"] == build_manifest(
        second, second_manifest
    )["input_pack_sha256"]


def test_verifier_detects_material_data_tampering(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    data_path = pack / "data" / "risk_metrics_inputs.json"
    rows = json.loads(data_path.read_text(encoding="utf-8"))
    rows[0]["value"] = 0.99
    _write_json(data_path, rows)

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is False
    assert result["table_hash_mismatches"][0]["path"] == "data/risk_metrics_inputs.json"


def test_verifier_rejects_table_hash_paths_outside_pack(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    table_hashes_path = pack / "table_hashes.json"
    table_hashes = _read_json(table_hashes_path)
    table_hashes["tables"][0]["path"] = "../outside.json"
    _write_json(table_hashes_path, table_hashes)

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["table_hash_mismatches"][0] == {
        "path": "../outside.json",
        "expected": "<inside pack>",
        "actual": "<outside pack>",
    }


def test_verifier_detects_component_manifest_tampering(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    raw_manifest_path = pack / "raw_snapshot_manifest.json"
    raw_manifest = _read_json(raw_manifest_path)
    raw_manifest["artifacts"][0]["rows"] = 3
    _write_json(raw_manifest_path, raw_manifest)

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is False
    assert result["component_hash_mismatches"][0]["path"] == "raw_snapshot_manifest.json"


def test_verifier_detects_missing_required_file(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    (pack / "provenance.json").unlink()

    result = verify_pack(pack)

    assert result["ok"] is False
    assert "provenance.json" in result["missing_required_files"]
    assert result["provenance_complete"] is False


def test_verifier_rejects_runtime_activation(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    manifest_path = pack / "manifest.json"
    manifest = _read_json(manifest_path)
    manifest["runtime_activation"] = True
    _write_json(manifest_path, manifest)

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["runtime_activation_ok"] is False
    assert result["schema_errors"]


def test_input_pack_code_has_no_db_connector_imports() -> None:
    for path in (ROOT / "src" / "input_packs").glob("*.py"):
        text = path.read_text(encoding="utf-8").lower()
        assert "psycopg" not in text
        assert "database_url" not in text
