"""Offline verifier for Certified Input Packs v1."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .hashing import file_sha256, load_json
from .manifest import (
    COMPONENT_HASH_FIELDS,
    MANIFEST_NAME,
    REQUIRED_DIRS,
    REQUIRED_FILES,
    compute_input_pack_sha256,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _schema_errors(instance: dict[str, Any], schema_path: Path) -> list[str]:
    try:
        import jsonschema
    except ImportError as exc:  # pragma: no cover - existing tests already require it.
        return [f"jsonschema unavailable: {exc}"]

    schema = load_json(schema_path)
    validator = jsonschema.Draft202012Validator(schema)
    return [
        f"{'/'.join(str(p) for p in error.absolute_path) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(instance), key=lambda e: list(e.absolute_path))
    ]


def _load_json_or_error(path: Path, errors: list[str]) -> dict[str, Any]:
    try:
        payload = load_json(path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        errors.append(f"{path.name}: {exc}")
        return {}
    if not isinstance(payload, dict):
        errors.append(f"{path.name}: expected JSON object")
        return {}
    return payload


def _verify_component_hashes(root: Path, manifest: dict[str, Any]) -> list[dict[str, str]]:
    mismatches: list[dict[str, str]] = []
    for filename, field in COMPONENT_HASH_FIELDS.items():
        path = root / filename
        if not path.exists():
            continue
        expected = manifest.get(field)
        actual = file_sha256(path)
        if expected != actual:
            mismatches.append(
                {
                    "path": filename,
                    "field": field,
                    "expected": str(expected),
                    "actual": actual,
                }
            )
    return mismatches


def _verify_table_hashes(root: Path, table_hashes: dict[str, Any]) -> tuple[list[str], list[dict[str, str]]]:
    missing: list[str] = []
    mismatches: list[dict[str, str]] = []
    for table in table_hashes.get("tables", []):
        if not isinstance(table, dict):
            mismatches.append({"path": "<invalid-entry>", "expected": "<object>", "actual": type(table).__name__})
            continue
        rel = table.get("path")
        expected = table.get("sha256")
        if not isinstance(rel, str):
            mismatches.append({"path": "<missing-path>", "expected": str(expected), "actual": "<no path>"})
            continue
        path = root / rel
        if not path.exists():
            missing.append(rel)
            continue
        actual = file_sha256(path)
        if expected != actual:
            mismatches.append({"path": rel, "expected": str(expected), "actual": actual})
    return sorted(missing), sorted(mismatches, key=lambda m: m["path"])


def _provenance_complete(provenance: dict[str, Any]) -> bool:
    required_collections = ("datasets", "jobs", "runs", "sources")
    if any(not provenance.get(name) for name in required_collections):
        return False
    first_dataset = provenance["datasets"][0]
    first_job = provenance["jobs"][0]
    first_run = provenance["runs"][0]
    first_source = provenance["sources"][0]
    return all(
        [
            first_dataset.get("dataset_name"),
            first_dataset.get("snapshot_id"),
            first_job.get("job_name"),
            first_run.get("run_id"),
            first_source.get("source_repo"),
            first_source.get("source_commit"),
        ]
    )


def verify_pack(
    pack_dir: str | Path,
    *,
    manifest_schema_path: str | Path | None = None,
) -> dict[str, Any]:
    """Verify a pack without connecting to external systems."""
    root = Path(pack_dir)
    manifest_path = root / MANIFEST_NAME
    schema_path = (
        Path(manifest_schema_path)
        if manifest_schema_path is not None
        else _repo_root() / "schemas" / "input_packs" / "input_pack_manifest.schema.json"
    )

    parse_errors: list[str] = []
    manifest = _load_json_or_error(manifest_path, parse_errors) if manifest_path.exists() else {}
    table_hashes = _load_json_or_error(root / "table_hashes.json", parse_errors) if (root / "table_hashes.json").exists() else {}
    provenance = _load_json_or_error(root / "provenance.json", parse_errors) if (root / "provenance.json").exists() else {}

    missing_required_files = sorted(path for path in REQUIRED_FILES if not (root / path).is_file())
    missing_required_dirs = sorted(path for path in REQUIRED_DIRS if not (root / path).is_dir())

    schema_errors = _schema_errors(manifest, schema_path) if manifest else ["manifest.json: missing or invalid"]
    component_hash_mismatches = _verify_component_hashes(root, manifest) if manifest else []
    missing_table_artifacts, table_hash_mismatches = _verify_table_hashes(root, table_hashes)

    actual_input_pack_sha256 = compute_input_pack_sha256(root, manifest) if manifest else None
    expected_input_pack_sha256 = manifest.get("input_pack_sha256")
    input_pack_sha256_match = bool(
        expected_input_pack_sha256 and expected_input_pack_sha256 == actual_input_pack_sha256
    )

    runtime_activation = manifest.get("runtime_activation")
    runtime_activation_ok = runtime_activation is False
    provenance_complete = _provenance_complete(provenance)

    ok = all(
        [
            not parse_errors,
            not missing_required_files,
            not missing_required_dirs,
            not schema_errors,
            not component_hash_mismatches,
            not missing_table_artifacts,
            not table_hash_mismatches,
            input_pack_sha256_match,
            runtime_activation_ok,
            provenance_complete,
        ]
    )

    return {
        "ok": ok,
        "parse_errors": parse_errors,
        "missing_required_files": missing_required_files,
        "missing_required_dirs": missing_required_dirs,
        "schema_errors": schema_errors,
        "component_hash_mismatches": component_hash_mismatches,
        "missing_table_artifacts": missing_table_artifacts,
        "table_hash_mismatches": table_hash_mismatches,
        "expected_input_pack_sha256": expected_input_pack_sha256,
        "actual_input_pack_sha256": actual_input_pack_sha256,
        "input_pack_sha256_match": input_pack_sha256_match,
        "runtime_activation": runtime_activation,
        "runtime_activation_ok": runtime_activation_ok,
        "provenance_complete": provenance_complete,
    }

