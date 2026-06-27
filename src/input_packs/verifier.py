"""Offline verifier for Certified Input Packs v1."""

from __future__ import annotations

import datetime as dt
import json
import math
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .hashing import file_sha256, load_json
from .manifest import (
    COMPONENT_HASH_FIELDS,
    MANIFEST_NAME,
    REQUIRED_DIRS,
    REQUIRED_FILES,
    compute_input_pack_sha256,
    iter_pack_files,
)
from .p0_contract import P0_TABLE_SPECS, TableSpec

COMPONENT_SCHEMA_FILES: dict[str, str] = {
    "SOURCE.json": "source.schema.json",
    "raw_snapshot_manifest.json": "snapshot_manifest.schema.json",
    "canonical_snapshot_manifest.json": "snapshot_manifest.schema.json",
    "derived_feature_manifest.json": "snapshot_manifest.schema.json",
    "table_hashes.json": "table_hashes.schema.json",
    "provenance.json": "provenance.schema.json",
}
SNAPSHOT_MANIFEST_FILES = (
    "raw_snapshot_manifest.json",
    "canonical_snapshot_manifest.json",
    "derived_feature_manifest.json",
)
P0_INPUT_PACK_ID = "open_macro_v03_certified_input_pack_001"
P0_SOURCE_TABLES = tuple(spec.name for spec in P0_TABLE_SPECS)
P0_RAW_ARTIFACT_PATHS = tuple(f"data/raw/{name}.json" for name in P0_SOURCE_TABLES)
P0_CANONICAL_ARTIFACT_PATHS = tuple(f"data/canonical/{name}.json" for name in P0_SOURCE_TABLES)
P0_DERIVED_ARTIFACT_PATHS = (
    "data/derived/fund_nav_return_features.json",
    "data/derived/market_price_return_features.json",
    "data/derived/macro_observation_features.json",
    "data/derived/fund_universe_features.json",
    "data/derived/holdings_summary_features.json",
    "data/derived/flow_momentum_features.json",
    "data/derived/feature_lineage.json",
)
P0_EXPECTED_SNAPSHOT_ARTIFACTS = {
    "raw_snapshot_manifest.json": frozenset(P0_RAW_ARTIFACT_PATHS),
    "canonical_snapshot_manifest.json": frozenset(P0_CANONICAL_ARTIFACT_PATHS),
    "derived_feature_manifest.json": frozenset(P0_DERIVED_ARTIFACT_PATHS),
}
P0_REQUIRED_DATA_ARTIFACTS = frozenset(
    (*P0_RAW_ARTIFACT_PATHS, *P0_CANONICAL_ARTIFACT_PATHS, *P0_DERIVED_ARTIFACT_PATHS)
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _schema_root(pack_root: Path) -> Path:
    repo_schemas = _repo_root() / "schemas" / "input_packs"
    if (repo_schemas / "input_pack_manifest.schema.json").is_file():
        return repo_schemas
    embedded = pack_root / "schemas"
    if (embedded / "input_pack_manifest.schema.json").is_file():
        return embedded
    return repo_schemas


def _schema_errors(instance: dict[str, Any], schema_path: Path) -> list[str]:
    try:
        import jsonschema
    except ImportError as exc:  # pragma: no cover - existing tests already require it.
        return [f"jsonschema unavailable: {exc}"]

    schema = load_json(schema_path)
    validator = jsonschema.Draft202012Validator(
        schema,
        format_checker=jsonschema.FormatChecker(),
    )
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


def _pack_relative_path(root: Path, rel: str) -> Path | None:
    """Resolve a table artifact path only when it stays inside ``root``."""
    path = Path(rel)
    if path.is_absolute():
        return None
    root_resolved = root.resolve()
    candidate = (root / path).resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError:
        return None
    return candidate


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
        path = _pack_relative_path(root, rel)
        if path is None:
            mismatches.append({"path": rel, "expected": "<inside pack>", "actual": "<outside pack>"})
            continue
        if not path.exists():
            missing.append(rel)
            continue
        actual = file_sha256(path)
        if expected != actual:
            mismatches.append({"path": rel, "expected": str(expected), "actual": actual})
    return sorted(missing), sorted(mismatches, key=lambda m: m["path"])


def _verify_component_artifact_hashes(
    root: Path,
    manifests: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    mismatches: list[dict[str, str]] = []
    for manifest_name, payload in manifests.items():
        artifacts = payload.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            rel = artifact.get("path")
            expected = artifact.get("sha256")
            if not isinstance(rel, str):
                mismatches.append(
                    {
                        "manifest": manifest_name,
                        "path": "<missing-path>",
                        "expected": str(expected),
                        "actual": "<no path>",
                    }
                )
                continue
            path = _pack_relative_path(root, rel)
            if path is None:
                mismatches.append(
                    {
                        "manifest": manifest_name,
                        "path": rel,
                        "expected": "<inside pack>",
                        "actual": "<outside pack>",
                    }
                )
                continue
            if not path.exists():
                mismatches.append(
                    {
                        "manifest": manifest_name,
                        "path": rel,
                        "expected": str(expected),
                        "actual": "<missing>",
                    }
                )
                continue
            actual = file_sha256(path)
            if expected != actual:
                mismatches.append(
                    {
                        "manifest": manifest_name,
                        "path": rel,
                        "expected": str(expected),
                        "actual": actual,
                    }
                )
    return sorted(mismatches, key=lambda m: (m["manifest"], m["path"]))


def _unexpected_files(root: Path, table_hashes: dict[str, Any]) -> list[str]:
    expected = set(REQUIRED_FILES)
    for table in table_hashes.get("tables", []):
        if isinstance(table, dict) and isinstance(table.get("path"), str):
            expected.add(table["path"])
    actual = {
        path.relative_to(root).as_posix()
        for path in iter_pack_files(root, include_manifest=True)
    }
    return sorted(actual - expected)


def _artifact_paths(payload: dict[str, Any]) -> set[str]:
    paths: set[str] = set()
    artifacts = payload.get("artifacts", [])
    if not isinstance(artifacts, list):
        return paths
    for artifact in artifacts:
        if isinstance(artifact, dict) and isinstance(artifact.get("path"), str):
            paths.add(artifact["path"])
    return paths


def _table_paths(table_hashes: dict[str, Any]) -> set[str]:
    paths: set[str] = set()
    tables = table_hashes.get("tables", [])
    if not isinstance(tables, list):
        return paths
    for table in tables:
        if isinstance(table, dict) and isinstance(table.get("path"), str):
            paths.add(table["path"])
    return paths


def _artifact_rows_by_path(component_payloads: dict[str, dict[str, Any]]) -> dict[str, int]:
    rows_by_path: dict[str, int] = {}
    for filename in SNAPSHOT_MANIFEST_FILES:
        artifacts = component_payloads.get(filename, {}).get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            path = artifact.get("path")
            rows = artifact.get("rows")
            if isinstance(path, str) and isinstance(rows, int):
                rows_by_path[path] = rows
    return rows_by_path


def _table_rows_by_path(table_hashes: dict[str, Any]) -> dict[str, int]:
    rows_by_path: dict[str, int] = {}
    tables = table_hashes.get("tables", [])
    if not isinstance(tables, list):
        return rows_by_path
    for table in tables:
        if not isinstance(table, dict):
            continue
        path = table.get("path")
        rows = table.get("rows")
        if isinstance(path, str) and isinstance(rows, int):
            rows_by_path[path] = rows
    return rows_by_path


def _duplicate_path_errors(component_payloads: dict[str, dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    seen_artifacts: dict[str, str] = {}
    for filename in SNAPSHOT_MANIFEST_FILES:
        local_seen: set[str] = set()
        artifacts = component_payloads.get(filename, {}).get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            path = artifact.get("path")
            if not isinstance(path, str):
                continue
            if path in local_seen:
                errors.append(f"{filename}: duplicate artifact path {path}")
            local_seen.add(path)
            prior_manifest = seen_artifacts.get(path)
            if prior_manifest is not None and prior_manifest != filename:
                errors.append(f"{filename}: artifact path {path} also appears in {prior_manifest}")
            else:
                seen_artifacts[path] = filename

    table_seen: set[str] = set()
    tables = component_payloads.get("table_hashes.json", {}).get("tables", [])
    if isinstance(tables, list):
        for table in tables:
            if not isinstance(table, dict):
                continue
            path = table.get("path")
            if not isinstance(path, str):
                continue
            if path in table_seen:
                errors.append(f"table_hashes.json: duplicate table path {path}")
            table_seen.add(path)
    return sorted(errors)


def _identity_errors(
    manifest: dict[str, Any],
    source: dict[str, Any],
    provenance: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    for field in ("source_repo", "source_commit", "builder_commit", "builder_code_sha256"):
        expected = manifest.get(field)
        actual = source.get(field)
        if actual != expected:
            errors.append(f"SOURCE.json: {field} {actual!r} does not match manifest {expected!r}")

    sources = provenance.get("sources", [])
    if isinstance(sources, list):
        for index, entry in enumerate(sources):
            if not isinstance(entry, dict):
                continue
            for field in ("source_repo", "source_commit"):
                expected = manifest.get(field)
                actual = entry.get(field)
                if actual != expected:
                    errors.append(
                        f"provenance.json: sources[{index}].{field} {actual!r} "
                        f"does not match manifest {expected!r}"
                    )
    return errors


def _json_row_count(path: Path) -> int | None:
    payload = load_json(path)
    if not isinstance(payload, list):
        return None
    return len(payload)


def _is_iso_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        dt.date.fromisoformat(value[:10])
    except ValueError:
        return False
    return value == value[:10]


def _is_number(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return value is None
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    return False


def _is_raw_number(value: Any) -> bool:
    if _is_number(value):
        return True
    if not isinstance(value, str):
        return False
    try:
        return Decimal(value).is_finite()
    except InvalidOperation:
        return False


def _is_bool_or_raw_bool(value: Any, *, canonical: bool) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return True
    if canonical:
        return False
    if isinstance(value, int) and value in (0, 1):
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "t", "1", "yes", "y", "false", "f", "0", "no", "n"}
    return False


def _validate_p0_source_row(rel: str, row: dict[str, Any], index: int, spec: TableSpec, *, canonical: bool) -> list[str]:
    errors: list[str] = []
    missing = [column for column in spec.columns if column not in row]
    if missing:
        errors.append(f"{rel}[{index}]: missing required columns: {', '.join(missing)}")

    for column in spec.key_columns:
        value = row.get(column)
        if value is None or isinstance(value, (dict, list)):
            errors.append(f"{rel}[{index}]: invalid key column {column}")

    for column in spec.date_columns:
        if column in row and row[column] is not None and not _is_iso_date(row[column]):
            errors.append(f"{rel}[{index}]: invalid date column {column}: {row[column]!r}")

    for column in spec.numeric_columns:
        if column in row:
            ok = _is_number(row[column]) if canonical else _is_raw_number(row[column])
            if not ok:
                errors.append(f"{rel}[{index}]: invalid numeric column {column}: {row[column]!r}")

    for column in spec.boolean_columns:
        if column in row and not _is_bool_or_raw_bool(row[column], canonical=canonical):
            errors.append(f"{rel}[{index}]: invalid boolean column {column}: {row[column]!r}")

    return errors


def _validate_p0_source_artifact(root: Path, rel: str, spec: TableSpec, *, canonical: bool) -> list[str]:
    path = _pack_relative_path(root, rel)
    if path is None or not path.exists():
        return []
    try:
        payload = load_json(path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        return [f"{rel}: cannot read P0 JSON artifact rows: {exc}"]
    if not isinstance(payload, list):
        return [f"{rel}: expected JSON array for P0 artifact"]

    errors: list[str] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for index, row in enumerate(payload):
        if not isinstance(row, dict):
            errors.append(f"{rel}[{index}]: expected JSON object row")
            continue
        errors.extend(_validate_p0_source_row(rel, row, index, spec, canonical=canonical))
        key = tuple(row.get(column) for column in spec.key_columns)
        if all(value is not None and not isinstance(value, (dict, list)) for value in key):
            if key in seen_keys:
                errors.append(f"{rel}[{index}]: duplicate natural key {key!r}")
            seen_keys.add(key)
    return errors


def _verify_p0_source_artifact_rows(root: Path) -> list[str]:
    errors: list[str] = []
    for spec in P0_TABLE_SPECS:
        errors.extend(
            _validate_p0_source_artifact(
                root,
                f"data/raw/{spec.name}.json",
                spec,
                canonical=False,
            )
        )
        errors.extend(
            _validate_p0_source_artifact(
                root,
                f"data/canonical/{spec.name}.json",
                spec,
                canonical=True,
            )
        )
    return errors


def _verify_expected_p0_content(
    root: Path,
    manifest: dict[str, Any],
    component_payloads: dict[str, dict[str, Any]],
) -> list[str]:
    errors: list[str] = []
    actual_pack_id = manifest.get("input_pack_id")
    if actual_pack_id != P0_INPUT_PACK_ID:
        errors.append(f"manifest.json: expected input_pack_id {P0_INPUT_PACK_ID}, got {actual_pack_id!r}")
    expected_as_of = manifest.get("as_of")

    for filename, expected_paths in P0_EXPECTED_SNAPSHOT_ARTIFACTS.items():
        payload = component_payloads.get(filename, {})
        if payload.get("as_of") != expected_as_of:
            errors.append(f"{filename}: as_of {payload.get('as_of')!r} does not match manifest as_of {expected_as_of!r}")
        actual_paths = _artifact_paths(payload)
        missing = sorted(expected_paths - actual_paths)
        unexpected = sorted(actual_paths - expected_paths)
        if missing:
            errors.append(f"{filename}: missing required P0 artifacts: {', '.join(missing)}")
        if unexpected:
            errors.append(f"{filename}: unexpected P0 artifacts: {', '.join(unexpected)}")

    table_paths = _table_paths(component_payloads.get("table_hashes.json", {}))
    missing_table_paths = sorted(P0_REQUIRED_DATA_ARTIFACTS - table_paths)
    unexpected_data_paths = sorted(
        path for path in table_paths - P0_REQUIRED_DATA_ARTIFACTS if path.startswith("data/")
    )
    if missing_table_paths:
        errors.append(f"table_hashes.json: missing required P0 data artifacts: {', '.join(missing_table_paths)}")
    if unexpected_data_paths:
        errors.append(f"table_hashes.json: unexpected P0 data artifacts: {', '.join(unexpected_data_paths)}")

    artifact_rows = _artifact_rows_by_path(component_payloads)
    table_rows = _table_rows_by_path(component_payloads.get("table_hashes.json", {}))
    for rel in sorted(P0_REQUIRED_DATA_ARTIFACTS):
        path = _pack_relative_path(root, rel)
        if path is None or not path.exists():
            continue
        try:
            actual_rows = _json_row_count(path)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"{rel}: cannot read P0 JSON artifact rows: {exc}")
            continue
        if actual_rows is None:
            errors.append(f"{rel}: expected JSON array for P0 artifact")
            continue
        if actual_rows <= 0:
            errors.append(f"{rel}: expected non-empty P0 artifact rows")
        manifest_rows = artifact_rows.get(rel)
        if manifest_rows is not None and manifest_rows != actual_rows:
            errors.append(f"{rel}: component manifest rows {manifest_rows} do not match actual rows {actual_rows}")
        table_hash_rows = table_rows.get(rel)
        if table_hash_rows is not None and table_hash_rows != actual_rows:
            errors.append(f"{rel}: table_hashes rows {table_hash_rows} do not match actual rows {actual_rows}")

    errors.extend(_verify_p0_source_artifact_rows(root))
    return errors


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
    if manifest_schema_path is not None:
        schema_path = Path(manifest_schema_path)
        schema_root = schema_path.parent
    else:
        schema_root = _schema_root(root)
        schema_path = schema_root / "input_pack_manifest.schema.json"

    parse_errors: list[str] = []
    manifest = _load_json_or_error(manifest_path, parse_errors) if manifest_path.exists() else {}
    source = _load_json_or_error(root / "SOURCE.json", parse_errors) if (root / "SOURCE.json").exists() else {}
    raw_snapshot = _load_json_or_error(root / "raw_snapshot_manifest.json", parse_errors) if (root / "raw_snapshot_manifest.json").exists() else {}
    canonical_snapshot = _load_json_or_error(root / "canonical_snapshot_manifest.json", parse_errors) if (root / "canonical_snapshot_manifest.json").exists() else {}
    derived_feature = _load_json_or_error(root / "derived_feature_manifest.json", parse_errors) if (root / "derived_feature_manifest.json").exists() else {}
    table_hashes = _load_json_or_error(root / "table_hashes.json", parse_errors) if (root / "table_hashes.json").exists() else {}
    provenance = _load_json_or_error(root / "provenance.json", parse_errors) if (root / "provenance.json").exists() else {}

    missing_required_files = sorted(path for path in REQUIRED_FILES if not (root / path).is_file())
    missing_required_dirs = sorted(path for path in REQUIRED_DIRS if not (root / path).is_dir())

    schema_errors = _schema_errors(manifest, schema_path) if manifest else ["manifest.json: missing or invalid"]
    component_payloads = {
        "SOURCE.json": source,
        "raw_snapshot_manifest.json": raw_snapshot,
        "canonical_snapshot_manifest.json": canonical_snapshot,
        "derived_feature_manifest.json": derived_feature,
        "table_hashes.json": table_hashes,
        "provenance.json": provenance,
    }
    component_schema_errors = {
        filename: errors
        for filename, errors in (
            (
                filename,
                _schema_errors(
                    payload,
                    schema_root / COMPONENT_SCHEMA_FILES[filename],
                ),
            )
            for filename, payload in component_payloads.items()
            if (root / filename).is_file()
        )
        if errors
    }
    component_hash_mismatches = _verify_component_hashes(root, manifest) if manifest else []
    component_artifact_hash_mismatches = _verify_component_artifact_hashes(
        root,
        {filename: component_payloads[filename] for filename in SNAPSHOT_MANIFEST_FILES},
    )
    missing_table_artifacts, table_hash_mismatches = _verify_table_hashes(root, table_hashes)
    unexpected_files = _unexpected_files(root, table_hashes)
    duplicate_path_errors = _duplicate_path_errors(component_payloads)
    identity_errors = _identity_errors(manifest, source, provenance) if manifest else []
    expected_content_errors = _verify_expected_p0_content(root, manifest, component_payloads) if manifest else []

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
            not component_schema_errors,
            not component_hash_mismatches,
            not component_artifact_hash_mismatches,
            not missing_table_artifacts,
            not table_hash_mismatches,
            not unexpected_files,
            not duplicate_path_errors,
            not identity_errors,
            not expected_content_errors,
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
        "component_schema_errors": component_schema_errors,
        "component_hash_mismatches": component_hash_mismatches,
        "component_artifact_hash_mismatches": component_artifact_hash_mismatches,
        "missing_table_artifacts": missing_table_artifacts,
        "table_hash_mismatches": table_hash_mismatches,
        "unexpected_files": unexpected_files,
        "duplicate_path_errors": duplicate_path_errors,
        "identity_errors": identity_errors,
        "expected_content_errors": expected_content_errors,
        "expected_input_pack_sha256": expected_input_pack_sha256,
        "actual_input_pack_sha256": actual_input_pack_sha256,
        "input_pack_sha256_match": input_pack_sha256_match,
        "runtime_activation": runtime_activation,
        "runtime_activation_ok": runtime_activation_ok,
        "provenance_complete": provenance_complete,
    }
