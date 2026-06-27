from __future__ import annotations

import json
import shutil
from pathlib import Path

from src.input_packs import build_manifest, verify_pack
from src.input_packs.hashing import file_sha256

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


def _payload_rows_and_columns(path: Path) -> tuple[int, list[str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        columns = sorted({key for row in payload if isinstance(row, dict) for key in row})
        return len(payload), columns
    if isinstance(payload, dict):
        return 1, sorted(payload)
    return 0, []


def _refresh_snapshot_artifact(pack: Path, manifest_name: str, rel: str) -> None:
    rows, columns = _payload_rows_and_columns(pack / rel)
    snapshot = _read_json(pack / manifest_name)
    for artifact in snapshot["artifacts"]:
        if artifact["path"] == rel:
            artifact["rows"] = rows
            artifact["columns"] = columns
            artifact["sha256"] = file_sha256(pack / rel)
            break
    else:
        raise AssertionError(f"artifact not found in {manifest_name}: {rel}")
    _write_json(pack / manifest_name, snapshot)


def _refresh_table_hash(pack: Path, rel: str) -> None:
    rows, _columns = _payload_rows_and_columns(pack / rel)
    table_hashes = _read_json(pack / "table_hashes.json")
    for table in table_hashes["tables"]:
        if table["path"] == rel:
            table["rows"] = rows
            table["sha256"] = file_sha256(pack / rel)
            break
    else:
        raise AssertionError(f"table hash not found: {rel}")
    _write_json(pack / "table_hashes.json", table_hashes)


def _refresh_data_artifact(pack: Path, manifest_name: str, rel: str) -> None:
    _refresh_snapshot_artifact(pack, manifest_name, rel)
    _refresh_table_hash(pack, rel)


def _refresh_manifest(pack: Path) -> None:
    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))


def test_golden_pack_verifies_offline() -> None:
    result = verify_pack(GOLDEN)
    assert result["ok"] is True
    assert result["input_pack_sha256_match"] is True
    assert result["runtime_activation_ok"] is True
    assert result["provenance_complete"] is True
    assert result["expected_content_errors"] == []


def test_verifier_uses_embedded_pack_schemas_when_repo_schemas_are_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pack = _copy_pack(tmp_path)
    from src.input_packs import verifier

    monkeypatch.setattr(verifier, "_repo_root", lambda: tmp_path / "missing_repo_root")

    result = verifier.verify_pack(pack)

    assert result["ok"] is True


def test_verifier_prefers_trusted_repo_schemas_over_embedded_pack_schemas(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    _write_json(pack / "raw_snapshot_manifest.json", {})
    _write_json(
        pack / "schemas" / "snapshot_manifest.schema.json",
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
        },
    )

    table_hashes_path = pack / "table_hashes.json"
    table_hashes = _read_json(table_hashes_path)
    for table in table_hashes["tables"]:
        if table["path"] == "schemas/snapshot_manifest.schema.json":
            table["sha256"] = file_sha256(pack / table["path"])
            break
    _write_json(table_hashes_path, table_hashes)

    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert "raw_snapshot_manifest.json" in result["component_schema_errors"]


def test_verifier_requires_expected_p0_pack_content_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    for data_file in (pack / "data").rglob("*"):
        if data_file.is_file():
            data_file.unlink()

    table_hashes = _read_json(pack / "table_hashes.json")
    table_hashes["tables"] = [
        table for table in table_hashes["tables"] if not str(table["path"]).startswith("data/")
    ]
    _write_json(pack / "table_hashes.json", table_hashes)

    report_artifact = {
        "columns": ["schema_version", "input_pack_id", "profile", "as_of"],
        "dataset_name": "report:certification_summary",
        "path": "reports/certification_summary.json",
        "rows": 1,
        "sha256": file_sha256(pack / "reports" / "certification_summary.json"),
    }
    for filename, snapshot_kind in (
        ("raw_snapshot_manifest.json", "raw"),
        ("canonical_snapshot_manifest.json", "canonical"),
        ("derived_feature_manifest.json", "derived_feature"),
    ):
        original = _read_json(pack / filename)
        payload = {
            "schema_version": original["schema_version"],
            "snapshot_kind": snapshot_kind,
            "as_of": original["as_of"],
            "artifacts": [report_artifact],
        }
        if snapshot_kind == "derived_feature":
            payload["lineage"] = original["lineage"]
        _write_json(pack / filename, payload)

    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert any("raw_snapshot_manifest.json: missing required P0 artifacts" in error for error in result["expected_content_errors"])
    assert any("canonical_snapshot_manifest.json: missing required P0 artifacts" in error for error in result["expected_content_errors"])
    assert any("derived_feature_manifest.json: missing required P0 artifacts" in error for error in result["expected_content_errors"])
    assert any("table_hashes.json: missing required P0 data artifacts" in error for error in result["expected_content_errors"])


def test_verifier_rejects_empty_p0_data_artifact_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    rel = "data/raw/nav_timeseries.json"
    _write_json(pack / rel, [])

    raw_manifest = _read_json(pack / "raw_snapshot_manifest.json")
    for artifact in raw_manifest["artifacts"]:
        if artifact["path"] == rel:
            artifact["rows"] = 0
            artifact["sha256"] = file_sha256(pack / rel)
            break
    _write_json(pack / "raw_snapshot_manifest.json", raw_manifest)

    table_hashes = _read_json(pack / "table_hashes.json")
    for table in table_hashes["tables"]:
        if table["path"] == rel:
            table["rows"] = 0
            table["sha256"] = file_sha256(pack / rel)
            break
    _write_json(pack / "table_hashes.json", table_hashes)

    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert f"{rel}: expected non-empty P0 artifact rows" in result["expected_content_errors"]


def test_verifier_cross_checks_p0_artifact_row_counts_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    rel = "data/canonical/nav_timeseries.json"
    actual_rows = len(json.loads((pack / rel).read_text(encoding="utf-8")))

    canonical_manifest = _read_json(pack / "canonical_snapshot_manifest.json")
    for artifact in canonical_manifest["artifacts"]:
        if artifact["path"] == rel:
            artifact["rows"] = actual_rows + 1
            break
    _write_json(pack / "canonical_snapshot_manifest.json", canonical_manifest)

    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert (
        f"{rel}: component manifest rows {actual_rows + 1} do not match actual rows {actual_rows}"
        in result["expected_content_errors"]
    )


def test_verifier_validates_p0_source_rows_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    rel = "data/canonical/nav_timeseries.json"
    rows = json.loads((pack / rel).read_text(encoding="utf-8"))
    rows[0].pop("nav")
    _write_json(pack / rel, rows)

    canonical_manifest = _read_json(pack / "canonical_snapshot_manifest.json")
    for artifact in canonical_manifest["artifacts"]:
        if artifact["path"] == rel:
            artifact["sha256"] = file_sha256(pack / rel)
            break
    _write_json(pack / "canonical_snapshot_manifest.json", canonical_manifest)

    table_hashes = _read_json(pack / "table_hashes.json")
    for table in table_hashes["tables"]:
        if table["path"] == rel:
            table["sha256"] = file_sha256(pack / rel)
            break
    _write_json(pack / "table_hashes.json", table_hashes)

    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert f"{rel}[0]: missing required columns: nav" in result["expected_content_errors"]


def test_verifier_recomputes_canonical_rows_from_raw_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    rel = "data/canonical/instruments_universe.json"
    rows = json.loads((pack / rel).read_text(encoding="utf-8"))
    rows[0]["is_active"] = not rows[0]["is_active"]
    _write_json(pack / rel, rows)
    _refresh_data_artifact(pack, "canonical_snapshot_manifest.json", rel)
    _refresh_manifest(pack)

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert (
        "data/canonical/instruments_universe.json: canonical rows do not match normalized "
        "data/raw/instruments_universe.json rows"
        in result["expected_content_errors"]
    )


def test_verifier_rejects_p0_source_rows_after_manifest_as_of_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    for rel, manifest_name in (
        ("data/raw/eod_prices.json", "raw_snapshot_manifest.json"),
        ("data/canonical/eod_prices.json", "canonical_snapshot_manifest.json"),
    ):
        rows = json.loads((pack / rel).read_text(encoding="utf-8"))
        rows[0]["date"] = "2026-06-27"
        _write_json(pack / rel, rows)
        _refresh_data_artifact(pack, manifest_name, rel)
    _refresh_manifest(pack)

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert (
        "data/raw/eod_prices.json[0]: date '2026-06-27' is after manifest as_of '2026-06-26'"
        in result["expected_content_errors"]
    )
    assert (
        "data/canonical/eod_prices.json[0]: date '2026-06-27' is after manifest as_of '2026-06-26'"
        in result["expected_content_errors"]
    )


def test_verifier_validates_derived_feature_rows_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    rel = "data/derived/fund_nav_return_features.json"
    _write_json(pack / rel, [{"bogus": "row"}])
    _refresh_data_artifact(pack, "derived_feature_manifest.json", rel)
    _refresh_manifest(pack)

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert (
        f"{rel}[0]: missing required columns: feature_name, instrument_id, observation_date, value"
        in result["expected_content_errors"]
    )
    assert f"{rel}[0]: unexpected columns: bogus" in result["expected_content_errors"]


def test_verifier_requires_provenance_dataset_for_every_p0_table_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    provenance = _read_json(pack / "provenance.json")
    provenance["datasets"] = provenance["datasets"][:1]
    _write_json(pack / "provenance.json", provenance)
    _refresh_manifest(pack)

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert result["provenance_complete"] is False


def test_verifier_cross_checks_component_as_of_values_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    raw_manifest = _read_json(pack / "raw_snapshot_manifest.json")
    raw_manifest["as_of"] = "2026-06-25"
    _write_json(pack / "raw_snapshot_manifest.json", raw_manifest)

    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert (
        "raw_snapshot_manifest.json: as_of '2026-06-25' does not match manifest as_of '2026-06-26'"
        in result["expected_content_errors"]
    )


def test_verifier_cross_checks_source_and_provenance_identity_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    rewritten_source_commit = "1" * 40
    rewritten_builder_commit = "2" * 40
    source = _read_json(pack / "SOURCE.json")
    source["source_commit"] = rewritten_source_commit
    source["builder_commit"] = rewritten_builder_commit
    _write_json(pack / "SOURCE.json", source)

    provenance = _read_json(pack / "provenance.json")
    provenance["sources"][0]["source_commit"] = rewritten_source_commit
    _write_json(pack / "provenance.json", provenance)

    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert (
        f"SOURCE.json: source_commit {rewritten_source_commit!r} does not match manifest {manifest['source_commit']!r}"
        in result["identity_errors"]
    )
    assert (
        f"SOURCE.json: builder_commit {rewritten_builder_commit!r} does not match manifest {manifest['builder_commit']!r}"
        in result["identity_errors"]
    )
    assert any("provenance.json: sources[0].source_commit" in error for error in result["identity_errors"])


def test_verifier_rejects_duplicate_artifact_paths_even_when_hashes_match(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    raw_manifest = _read_json(pack / "raw_snapshot_manifest.json")
    raw_manifest["artifacts"].append(dict(raw_manifest["artifacts"][0]))
    _write_json(pack / "raw_snapshot_manifest.json", raw_manifest)

    table_hashes = _read_json(pack / "table_hashes.json")
    duplicate_table_path = table_hashes["tables"][0]["path"]
    table_hashes["tables"].append(dict(table_hashes["tables"][0]))
    _write_json(pack / "table_hashes.json", table_hashes)

    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    duplicate_path = raw_manifest["artifacts"][0]["path"]
    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is True
    assert f"raw_snapshot_manifest.json: duplicate artifact path {duplicate_path}" in result["duplicate_path_errors"]
    assert f"table_hashes.json: duplicate table path {duplicate_table_path}" in result["duplicate_path_errors"]


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
    data_path = pack / "data" / "derived" / "fund_nav_return_features.json"
    rows = json.loads(data_path.read_text(encoding="utf-8"))
    rows[0]["value"] = 0.99
    _write_json(data_path, rows)

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["input_pack_sha256_match"] is False
    assert result["table_hash_mismatches"][0]["path"] == "data/derived/fund_nav_return_features.json"


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


def test_verifier_detects_missing_table_artifact(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    (pack / "data" / "canonical" / "nav_timeseries.json").unlink()

    result = verify_pack(pack)

    assert result["ok"] is False
    assert "data/canonical/nav_timeseries.json" in result["missing_table_artifacts"]


def test_verifier_rejects_unlisted_extra_file(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    _write_json(pack / "data" / "derived" / "extra.json", {"unexpected": True})

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["unexpected_files"] == ["data/derived/extra.json"]
    assert result["input_pack_sha256_match"] is False


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


def test_verifier_enforces_manifest_date_format(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    manifest = _read_json(pack / "manifest.json")
    manifest["as_of"] = "20260625"
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert any("as_of" in error for error in result["schema_errors"])


def test_verifier_validates_component_manifest_schemas(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    table_hashes_path = pack / "table_hashes.json"
    table_hashes = _read_json(table_hashes_path)
    table_hashes.pop("tables")
    _write_json(table_hashes_path, table_hashes)
    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert "table_hashes.json" in result["component_schema_errors"]


def test_verifier_validates_empty_component_manifest_schemas(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    raw_manifest_path = pack / "raw_snapshot_manifest.json"
    _write_json(raw_manifest_path, {})
    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert "raw_snapshot_manifest.json" in result["component_schema_errors"]


def test_verifier_cross_checks_component_artifact_hashes(tmp_path: Path) -> None:
    pack = _copy_pack(tmp_path)
    raw_manifest_path = pack / "raw_snapshot_manifest.json"
    raw_manifest = _read_json(raw_manifest_path)
    raw_manifest["artifacts"][0]["sha256"] = "0" * 64
    _write_json(raw_manifest_path, raw_manifest)
    manifest = _read_json(pack / "manifest.json")
    _write_json(pack / "manifest.json", build_manifest(pack, manifest))

    result = verify_pack(pack)

    assert result["ok"] is False
    assert result["component_artifact_hash_mismatches"][0]["manifest"] == "raw_snapshot_manifest.json"
    assert result["component_artifact_hash_mismatches"][0]["path"] == "data/raw/nav_timeseries.json"


def test_input_pack_code_has_no_db_connector_imports() -> None:
    for path in (ROOT / "src" / "input_packs").glob("*.py"):
        text = path.read_text(encoding="utf-8").lower()
        assert "psycopg" not in text
        assert "database_url" not in text
