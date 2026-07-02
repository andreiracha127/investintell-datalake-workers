"""Offline verifier for the P1 Certified Input Pack v2.

Reuses the immutable P0 generic helpers (hash-tree checks, row normalization,
aggregate pack digest) from ``src/input_packs`` unchanged, and layers the
P1-specific table + governance contract on top. It never connects to external
systems.

Why a separate verifier (delta vs P0 ``verify_pack``):

* P0 ``verify_pack`` hard-codes ``P0_INPUT_PACK_ID``, the nine ``P0_TABLE_SPECS``,
  P0 derived-feature recomputation and P0-specific provenance dataset naming; none
  apply to the two P1 tables.
* P1 has no derived feature layer (``data/derived`` is absent) and treats
  ``data/raw`` and ``data/canonical`` as identical normalized rows.
* P1 pins governance flags (A5=blocked, runtime_activation=false, ...) and the v2
  contract bundle sha in the manifest; those are asserted here.

Generic pieces reused verbatim from ``src/input_packs``:
``compute_input_pack_sha256``, ``iter_pack_files``, ``file_sha256``, ``load_json``,
and the ``normalize_row`` / ``row_sort_key`` normalization used to prove
canonical == normalized(raw).
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from src.input_packs.hashing import file_sha256, load_json
from src.input_packs.manifest import (
    COMPONENT_HASH_FIELDS,
    MANIFEST_NAME,
    compute_input_pack_sha256,
    iter_pack_files,
)
from src.input_packs.p0_contract import TableSpec, normalize_row, row_sort_key

from .contract import P1_TABLE_SPECS

INPUT_PACK_ID = "open_macro_v03_certified_input_pack_002"
INPUT_PACK_VERSION = 2

REQUIRED_FILES: tuple[str, ...] = (
    "manifest.json",
    "SOURCE.json",
    "raw_snapshot_manifest.json",
    "canonical_snapshot_manifest.json",
    "table_hashes.json",
    "provenance.json",
)
REQUIRED_DIRS: tuple[str, ...] = ("schemas", "data", "reports")

SNAPSHOT_MANIFEST_FILES = ("raw_snapshot_manifest.json", "canonical_snapshot_manifest.json")

P1_RAW_ARTIFACT_PATHS = tuple(f"data/raw/{spec.name}.json" for spec in P1_TABLE_SPECS)
P1_CANONICAL_ARTIFACT_PATHS = tuple(f"data/canonical/{spec.name}.json" for spec in P1_TABLE_SPECS)
P1_REQUIRED_DATA_ARTIFACTS = frozenset((*P1_RAW_ARTIFACT_PATHS, *P1_CANONICAL_ARTIFACT_PATHS))
P1_SOURCE_TABLES = tuple(spec.name for spec in P1_TABLE_SPECS)

GOVERNANCE_EXPECTATIONS: dict[str, Any] = {
    "A5": "blocked",
    "runtime_activation": False,
    "activation_allowed": False,
    "official_result": False,
    "allocator_publish": False,
    "db_write_mode": "none",
    "classification": "metric_evidence_only",
}

CONTRACT_BUNDLE_SHA256 = "db85c58968becd890d49d0a022b54b9493449e8c9ff444c88da10678c5d6f53b"


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


def _pack_relative_path(root: Path, rel: str) -> Path | None:
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


def _load_json_array(root: Path, rel: str) -> tuple[list[Any] | None, str | None]:
    path = _pack_relative_path(root, rel)
    if path is None or not path.exists():
        return None, None
    try:
        payload = load_json(path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        return None, f"{rel}: cannot read P1 JSON artifact rows: {exc}"
    if not isinstance(payload, list):
        return None, f"{rel}: expected JSON array for P1 artifact"
    return payload, None


def _verify_component_hashes(root: Path, manifest: dict[str, Any]) -> list[dict[str, str]]:
    mismatches: list[dict[str, str]] = []
    for filename, field in COMPONENT_HASH_FIELDS.items():
        path = root / filename
        if not path.exists():
            continue
        expected = manifest.get(field)
        try:
            actual = file_sha256(path)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            actual = f"<unreadable: {exc}>"
        if expected != actual:
            mismatches.append(
                {"path": filename, "field": field, "expected": str(expected), "actual": actual}
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
        path = _pack_relative_path(root, rel)
        if path is None:
            mismatches.append({"path": rel, "expected": "<inside pack>", "actual": "<outside pack>"})
            continue
        if not path.exists():
            missing.append(rel)
            continue
        try:
            actual = file_sha256(path)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            actual = f"<unreadable: {exc}>"
        if expected != actual:
            mismatches.append({"path": rel, "expected": str(expected), "actual": actual})
    return sorted(missing), sorted(mismatches, key=lambda m: m["path"])


def _verify_component_artifact_hashes(root: Path, manifests: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
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
                    {"manifest": manifest_name, "path": "<missing-path>", "expected": str(expected), "actual": "<no path>"}
                )
                continue
            path = _pack_relative_path(root, rel)
            if path is None or not path.exists():
                mismatches.append(
                    {"manifest": manifest_name, "path": rel, "expected": str(expected), "actual": "<missing>"}
                )
                continue
            try:
                actual = file_sha256(path)
            except (OSError, json.JSONDecodeError, ValueError) as exc:
                actual = f"<unreadable: {exc}>"
            if expected != actual:
                mismatches.append(
                    {"manifest": manifest_name, "path": rel, "expected": str(expected), "actual": actual}
                )
    return sorted(mismatches, key=lambda m: (m["manifest"], m["path"]))


def _unexpected_files(root: Path, table_hashes: dict[str, Any]) -> list[str]:
    expected = set(REQUIRED_FILES)
    for table in table_hashes.get("tables", []):
        if isinstance(table, dict) and isinstance(table.get("path"), str):
            expected.add(table["path"])
    actual = {path.relative_to(root).as_posix() for path in iter_pack_files(root, include_manifest=True)}
    return sorted(actual - expected)


def _is_iso_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        dt.date.fromisoformat(value[:10])
    except ValueError:
        return False
    return value == value[:10]


def _is_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if value is None:
        return True
    return isinstance(value, (int, float))


def _validate_p1_source_artifact(root: Path, rel: str, spec: TableSpec) -> list[str]:
    payload, load_error = _load_json_array(root, rel)
    if load_error:
        return [load_error]
    if payload is None:
        return []
    errors: list[str] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for index, row in enumerate(payload):
        if not isinstance(row, dict):
            errors.append(f"{rel}[{index}]: expected JSON object row")
            continue
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
            if column in row and not _is_number(row[column]):
                errors.append(f"{rel}[{index}]: invalid numeric column {column}: {row[column]!r}")
        key = tuple(row.get(column) for column in spec.key_columns)
        if all(v is not None and not isinstance(v, (dict, list)) for v in key):
            if key in seen_keys:
                errors.append(f"{rel}[{index}]: duplicate natural key {key!r}")
            seen_keys.add(key)
    return errors


def _verify_raw_equals_canonical(root: Path, spec: TableSpec) -> list[str]:
    raw_rel = f"data/raw/{spec.name}.json"
    canonical_rel = f"data/canonical/{spec.name}.json"
    raw_rows, raw_error = _load_json_array(root, raw_rel)
    canonical_rows, canonical_error = _load_json_array(root, canonical_rel)
    if raw_error or canonical_error or raw_rows is None or canonical_rows is None:
        return []
    if any(not isinstance(row, dict) for row in (*raw_rows, *canonical_rows)):
        return []
    try:
        expected = sorted((normalize_row(row, spec) for row in raw_rows), key=lambda row: row_sort_key(row, spec))
    except ValueError as exc:
        return [f"{raw_rel}: cannot normalize raw rows for canonical comparison: {exc}"]
    actual = sorted(canonical_rows, key=lambda row: row_sort_key(row, spec))
    if actual != expected:
        return [f"{canonical_rel}: canonical rows do not match normalized {raw_rel} rows"]
    return []


def _json_row_count(path: Path) -> int | None:
    payload = load_json(path)
    if not isinstance(payload, list):
        return None
    return len(payload)


def _table_rows_by_path(payload: dict[str, Any], key: str, items_key: str) -> dict[str, int]:
    rows_by_path: dict[str, int] = {}
    for item in payload.get(items_key, []) if isinstance(payload, dict) else []:
        if isinstance(item, dict) and isinstance(item.get("path"), str) and isinstance(item.get("rows"), int):
            rows_by_path[item["path"]] = item["rows"]
    return rows_by_path


def _verify_expected_p1_content(
    root: Path,
    manifest: dict[str, Any],
    component_payloads: dict[str, dict[str, Any]],
) -> list[str]:
    errors: list[str] = []
    if manifest.get("input_pack_id") != INPUT_PACK_ID:
        errors.append(f"manifest.json: expected input_pack_id {INPUT_PACK_ID}, got {manifest.get('input_pack_id')!r}")
    if manifest.get("input_pack_version") != INPUT_PACK_VERSION:
        errors.append(
            f"manifest.json: expected input_pack_version {INPUT_PACK_VERSION}, got {manifest.get('input_pack_version')!r}"
        )
    if manifest.get("contract_bundle_sha256") != CONTRACT_BUNDLE_SHA256:
        errors.append(
            f"manifest.json: expected contract_bundle_sha256 {CONTRACT_BUNDLE_SHA256}, "
            f"got {manifest.get('contract_bundle_sha256')!r}"
        )
    for key, expected in GOVERNANCE_EXPECTATIONS.items():
        if manifest.get(key) != expected:
            errors.append(f"manifest.json: governance pin {key} expected {expected!r}, got {manifest.get(key)!r}")

    expected_as_of = manifest.get("as_of")
    expected_snapshot = {
        "raw_snapshot_manifest.json": (frozenset(P1_RAW_ARTIFACT_PATHS), "raw"),
        "canonical_snapshot_manifest.json": (frozenset(P1_CANONICAL_ARTIFACT_PATHS), "canonical"),
    }
    for filename, (expected_paths, kind) in expected_snapshot.items():
        payload = component_payloads.get(filename, {})
        if payload.get("as_of") != expected_as_of:
            errors.append(f"{filename}: as_of {payload.get('as_of')!r} does not match manifest as_of {expected_as_of!r}")
        if payload.get("snapshot_kind") != kind:
            errors.append(f"{filename}: snapshot_kind {payload.get('snapshot_kind')!r} != {kind!r}")
        actual_paths = {
            a.get("path")
            for a in payload.get("artifacts", [])
            if isinstance(a, dict) and isinstance(a.get("path"), str)
        }
        missing = sorted(expected_paths - actual_paths)
        unexpected = sorted(p for p in actual_paths - expected_paths if isinstance(p, str))
        if missing:
            errors.append(f"{filename}: missing required P1 artifacts: {', '.join(missing)}")
        if unexpected:
            errors.append(f"{filename}: unexpected P1 artifacts: {', '.join(unexpected)}")

    table_paths = {
        t.get("path")
        for t in component_payloads.get("table_hashes.json", {}).get("tables", [])
        if isinstance(t, dict) and isinstance(t.get("path"), str)
    }
    missing_table_paths = sorted(P1_REQUIRED_DATA_ARTIFACTS - table_paths)
    if missing_table_paths:
        errors.append(f"table_hashes.json: missing required P1 data artifacts: {', '.join(missing_table_paths)}")

    # No derived layer allowed.
    for path in sorted(table_paths):
        if isinstance(path, str) and path.startswith("data/derived/"):
            errors.append(f"table_hashes.json: P1 pack must not contain derived artifacts: {path}")
    if (root / "derived_feature_manifest.json").exists():
        errors.append("derived_feature_manifest.json: P1 pack must not have a derived feature manifest")

    # Row-count consistency across manifests + on-disk.
    snap_rows: dict[str, int] = {}
    for filename in SNAPSHOT_MANIFEST_FILES:
        for artifact in component_payloads.get(filename, {}).get("artifacts", []):
            if isinstance(artifact, dict) and isinstance(artifact.get("path"), str) and isinstance(artifact.get("rows"), int):
                snap_rows[artifact["path"]] = artifact["rows"]
    table_rows = _table_rows_by_path(component_payloads.get("table_hashes.json", {}), "path", "tables")
    for rel in sorted(P1_REQUIRED_DATA_ARTIFACTS):
        path = _pack_relative_path(root, rel)
        if path is None or not path.exists():
            continue
        actual_rows = _json_row_count(path)
        if actual_rows is None:
            errors.append(f"{rel}: expected JSON array for P1 artifact")
            continue
        if actual_rows <= 0:
            errors.append(f"{rel}: expected non-empty P1 artifact rows")
        if rel in snap_rows and snap_rows[rel] != actual_rows:
            errors.append(f"{rel}: snapshot manifest rows {snap_rows[rel]} do not match actual rows {actual_rows}")
        if rel in table_rows and table_rows[rel] != actual_rows:
            errors.append(f"{rel}: table_hashes rows {table_rows[rel]} do not match actual rows {actual_rows}")

    for spec in P1_TABLE_SPECS:
        errors.extend(_validate_p1_source_artifact(root, f"data/raw/{spec.name}.json", spec))
        errors.extend(_validate_p1_source_artifact(root, f"data/canonical/{spec.name}.json", spec))
        errors.extend(_verify_raw_equals_canonical(root, spec))
    return errors


def _identity_errors(manifest: dict[str, Any], source: dict[str, Any], provenance: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in ("source_repo", "source_commit", "builder_commit", "builder_code_sha256"):
        if source.get(field) != manifest.get(field):
            errors.append(f"SOURCE.json: {field} {source.get(field)!r} does not match manifest {manifest.get(field)!r}")
    sources = provenance.get("sources", [])
    if isinstance(sources, list):
        for index, entry in enumerate(sources):
            if not isinstance(entry, dict):
                continue
            for field in ("source_repo", "source_commit"):
                if entry.get(field) != manifest.get(field):
                    errors.append(
                        f"provenance.json: sources[{index}].{field} {entry.get(field)!r} "
                        f"does not match manifest {manifest.get(field)!r}"
                    )
    return errors


def _provenance_complete(provenance: dict[str, Any], *, expected_as_of: str | None) -> bool:
    required = ("datasets", "jobs", "runs", "sources")
    if any(not provenance.get(name) for name in required):
        return False
    datasets, jobs, runs, sources = (provenance[name] for name in required)
    if not all(isinstance(c, list) for c in (datasets, jobs, runs, sources)):
        return False
    if not all(isinstance(item, dict) for c in (datasets, jobs, runs, sources) for item in c):
        return False
    datasets_by_name = {str(d.get("dataset_name")): d for d in datasets if d.get("dataset_name")}
    if set(datasets_by_name) != set(P1_SOURCE_TABLES):
        return False
    if expected_as_of:
        try:
            suffix = dt.date.fromisoformat(expected_as_of).strftime("%Y%m%d")
        except ValueError:
            return False
        for name in P1_SOURCE_TABLES:
            if datasets_by_name[name].get("snapshot_id") != f"{name}_{suffix}":
                return False
    return all(
        [jobs[0].get("job_name"), runs[0].get("run_id"), sources[0].get("source_repo"), sources[0].get("source_commit")]
    )


def verify_pack(pack_dir: str | Path) -> dict[str, Any]:
    """Verify a P1 pack without connecting to external systems."""
    root = Path(pack_dir)
    parse_errors: list[str] = []

    def _load(name: str) -> dict[str, Any]:
        path = root / name
        return _load_json_or_error(path, parse_errors) if path.exists() else {}

    manifest = _load(MANIFEST_NAME)
    source = _load("SOURCE.json")
    raw_snapshot = _load("raw_snapshot_manifest.json")
    canonical_snapshot = _load("canonical_snapshot_manifest.json")
    table_hashes = _load("table_hashes.json")
    provenance = _load("provenance.json")

    component_payloads = {
        "SOURCE.json": source,
        "raw_snapshot_manifest.json": raw_snapshot,
        "canonical_snapshot_manifest.json": canonical_snapshot,
        "table_hashes.json": table_hashes,
        "provenance.json": provenance,
    }

    missing_required_files = sorted(path for path in REQUIRED_FILES if not (root / path).is_file())
    missing_required_dirs = sorted(path for path in REQUIRED_DIRS if not (root / path).is_dir())

    component_hash_mismatches = _verify_component_hashes(root, manifest) if manifest else []
    component_artifact_hash_mismatches = _verify_component_artifact_hashes(
        root, {name: component_payloads[name] for name in SNAPSHOT_MANIFEST_FILES}
    )
    missing_table_artifacts, table_hash_mismatches = _verify_table_hashes(root, table_hashes)
    unexpected_files = _unexpected_files(root, table_hashes)
    identity_errors = _identity_errors(manifest, source, provenance) if manifest else []
    expected_content_errors = _verify_expected_p1_content(root, manifest, component_payloads) if manifest else []

    try:
        actual_input_pack_sha256 = compute_input_pack_sha256(root, manifest) if manifest else None
    except (OSError, json.JSONDecodeError, ValueError):
        actual_input_pack_sha256 = None
    expected_input_pack_sha256 = manifest.get("input_pack_sha256")
    input_pack_sha256_match = bool(
        expected_input_pack_sha256 and expected_input_pack_sha256 == actual_input_pack_sha256
    )

    runtime_activation = manifest.get("runtime_activation")
    runtime_activation_ok = runtime_activation is False
    provenance_complete = _provenance_complete(provenance, expected_as_of=manifest.get("as_of") if manifest else None)

    ok = all(
        [
            not parse_errors,
            not missing_required_files,
            not missing_required_dirs,
            not component_hash_mismatches,
            not component_artifact_hash_mismatches,
            not missing_table_artifacts,
            not table_hash_mismatches,
            not unexpected_files,
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
        "component_hash_mismatches": component_hash_mismatches,
        "component_artifact_hash_mismatches": component_artifact_hash_mismatches,
        "missing_table_artifacts": missing_table_artifacts,
        "table_hash_mismatches": table_hash_mismatches,
        "unexpected_files": unexpected_files,
        "identity_errors": identity_errors,
        "expected_content_errors": expected_content_errors,
        "expected_input_pack_sha256": expected_input_pack_sha256,
        "actual_input_pack_sha256": actual_input_pack_sha256,
        "input_pack_sha256_match": input_pack_sha256_match,
        "runtime_activation": runtime_activation,
        "runtime_activation_ok": runtime_activation_ok,
        "provenance_complete": provenance_complete,
    }
