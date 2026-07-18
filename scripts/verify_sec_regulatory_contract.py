#!/usr/bin/env python3
"""Fail-closed integrity verification for the frozen SEC regulatory contract.

The app owns the authoritative Phase 0 bundle.  This worker-side verifier
checks only the pinned mirror and deliberately does not update source data,
thresholds, readiness, or runtime state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUNDLE_ROOT = ROOT / "contracts" / "sec-regulatory" / "v1"

SOURCE_REPOSITORY = "andreiracha127/investintell-light"
PHASE0_BASE_COMMIT = "a5c6823e7e2c5b2c54aecf4f855528d0e4b716c3"
SOURCE_COMMIT = "24d7732a38f438a523ba2f09986086e961c2165b"
SOURCE_PATH = "backend/contracts/sec-regulatory/v1"
TARGET_PATH = "contracts/sec-regulatory/v1"
CONTRACT_VERSION = "v1"
WORKER_BASE_COMMIT = "8636c14f08aa8d27b0f8e1d627a65072bf9772bf"
EXPECTED_MIRROR_FILE_COUNT = 35
WORKER_ONLY_FILES = (
    "worker-equivalence-manifest.json",
    "worker-provenance.json",
)
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
RR1_TXT_PRIMARY_KEY = [
    "adsh", "tag", "version", "ddate", "series", "class",
    "measure", "document", "otherdims", "iprx",
]


class ContractVerificationError(Exception):
    """Raised when the pinned, immutable mirror is not exact."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractVerificationError(f"cannot read JSON {path.name}: {exc}") from exc
    if not isinstance(value, dict):
        raise ContractVerificationError(f"{path.name} must contain a JSON object")
    return value


def _require_exact_metadata(data: dict[str, Any], expected: dict[str, Any], name: str) -> None:
    for key, value in expected.items():
        if data.get(key) != value:
            raise ContractVerificationError(
                f"{name} has invalid {key!r}: expected {value!r}, got {data.get(key)!r}"
            )


def _normal_relative_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ContractVerificationError(f"{field} must be a normalized non-empty POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise ContractVerificationError(f"{field} must not escape the contract root")
    return value


def _relative_files(bundle_root: Path) -> set[str]:
    files: set[str] = set()
    for path in bundle_root.rglob("*"):
        if path.is_symlink():
            raise ContractVerificationError(f"symlinks are not permitted in the mirror: {path}")
        if path.is_file():
            files.add(path.relative_to(bundle_root).as_posix())
    return files


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_gate_registry(bundle_root: Path) -> None:
    registry = _read_json(bundle_root / "sec-quality-gate-registry.yaml")
    gates = registry.get("gates")
    if not isinstance(gates, list):
        raise ContractVerificationError("quality gate registry must contain a gates list")

    indexed: dict[str, dict[str, Any]] = {}
    for gate in gates:
        if not isinstance(gate, dict) or not isinstance(gate.get("gate_id"), str):
            raise ContractVerificationError("quality gate registry contains a malformed gate")
        gate_id = gate["gate_id"]
        if gate_id in indexed:
            raise ContractVerificationError(f"quality gate registry duplicates {gate_id}")
        indexed[gate_id] = gate

    canonical_pin = indexed.get("CONTRACT-WORKER-CANONICAL-PIN")
    if canonical_pin is None or canonical_pin.get("status") != "blocked":
        raise ContractVerificationError(
            "CONTRACT-WORKER-CANONICAL-PIN must remain present with status 'blocked'"
        )

    rr1_quarantine = indexed.get("SRC-RR1-DATE-QUARANTINE")
    if rr1_quarantine is None:
        raise ContractVerificationError("SRC-RR1-DATE-QUARANTINE is missing from the quality gate registry")
    if rr1_quarantine.get("threshold") != {"operator": "eq", "value": 0}:
        raise ContractVerificationError("SRC-RR1-DATE-QUARANTINE has drifted from its frozen threshold")
    if rr1_quarantine.get("status") != "pending":
        raise ContractVerificationError("SRC-RR1-DATE-QUARANTINE must retain its frozen pending status")


def _verify_source_table_semantics(bundle_root: Path) -> None:
    """Fail closed when table keys, flags, or logical parents drift internally."""
    for path in sorted((bundle_root / "source-tables").glob("*.json")):
        contract = _read_json(path)
        variants = contract.get("schema_variants")
        if not isinstance(variants, list) or not variants:
            raise ContractVerificationError(f"{path.name} has no schema variants")
        for variant_index, variant in enumerate(variants):
            tables = variant.get("tables") if isinstance(variant, dict) else None
            if not isinstance(tables, list) or not tables:
                raise ContractVerificationError(f"{path.name} variant {variant_index} has no tables")
            table_names = {
                table.get("source_file") for table in tables if isinstance(table, dict)
            }
            for table in tables:
                if not isinstance(table, dict):
                    raise ContractVerificationError(f"{path.name} variant {variant_index} has malformed table")
                source_file = table.get("source_file")
                columns = table.get("columns")
                key = table.get("candidate_primary_key")
                parents = table.get("logical_parents")
                if not isinstance(columns, list) or not isinstance(key, list) or not isinstance(parents, list):
                    raise ContractVerificationError(f"{path.name} {source_file} has malformed semantics")
                names = [column.get("name") for column in columns if isinstance(column, dict)]
                flagged = [
                    column.get("name") for column in columns
                    if isinstance(column, dict) and column.get("candidate_key") is True
                ]
                if len(names) != len(columns) or len(set(names)) != len(names):
                    raise ContractVerificationError(f"{path.name} {source_file} has invalid columns")
                if len(set(key)) != len(key) or any(item not in names for item in key):
                    raise ContractVerificationError(f"{path.name} {source_file} has invalid candidate key")
                if set(flagged) != set(key):
                    raise ContractVerificationError(f"{path.name} {source_file} key flags drifted")
                if any(parent not in table_names or parent == source_file for parent in parents):
                    raise ContractVerificationError(f"{path.name} {source_file} logical parents drifted")
        if contract.get("family") == "rr1":
            txt_tables = [
                [table for table in variant["tables"] if table.get("source_file") == "txt.tsv"]
                for variant in variants
            ]
            if len(variants) != 6 or any(len(tables) != 1 for tables in txt_tables):
                raise ContractVerificationError("rr1.json must contain one TXT table in each of six variants")
            if any(tables[0].get("candidate_primary_key") != RR1_TXT_PRIMARY_KEY for tables in txt_tables):
                raise ContractVerificationError("rr1.json TXT key differs from merged Figure 7 correction")


def verify_contract(bundle_root: Path = DEFAULT_BUNDLE_ROOT) -> int:
    """Verify the complete frozen mirror, returning its mirrored file count."""
    bundle_root = bundle_root.resolve()
    if not bundle_root.is_dir():
        raise ContractVerificationError(f"bundle root does not exist: {bundle_root}")

    provenance = _read_json(bundle_root / "worker-provenance.json")
    expected_provenance = {
        "schema_version": 1,
        "contract_version": CONTRACT_VERSION,
        "source_repository": SOURCE_REPOSITORY,
        "phase0_base_commit": PHASE0_BASE_COMMIT,
        "source_commit": SOURCE_COMMIT,
        "source_path": SOURCE_PATH,
        "target_path": TARGET_PATH,
        "worker_base_commit": WORKER_BASE_COMMIT,
        "worker_only_files": list(WORKER_ONLY_FILES),
    }
    _require_exact_metadata(provenance, expected_provenance, "worker-provenance.json")

    manifest = _read_json(bundle_root / "worker-equivalence-manifest.json")
    expected_manifest = {
        "schema_version": 1,
        "contract_version": CONTRACT_VERSION,
        "source_repository": SOURCE_REPOSITORY,
        "phase0_base_commit": PHASE0_BASE_COMMIT,
        "source_commit": SOURCE_COMMIT,
        "source_path": SOURCE_PATH,
        "target_path": TARGET_PATH,
        "worker_only_files": list(WORKER_ONLY_FILES),
    }
    _require_exact_metadata(manifest, expected_manifest, "worker-equivalence-manifest.json")

    files = manifest.get("files")
    if not isinstance(files, list):
        raise ContractVerificationError("equivalence manifest files must be a list")
    if len(files) < EXPECTED_MIRROR_FILE_COUNT:
        raise ContractVerificationError(
            f"equivalence manifest must contain at least {EXPECTED_MIRROR_FILE_COUNT} files"
        )

    expected_actual = set(WORKER_ONLY_FILES)
    source_paths: set[str] = set()
    worker_paths: list[str] = []
    for index, mapping in enumerate(files):
        if not isinstance(mapping, dict):
            raise ContractVerificationError(f"equivalence mapping {index} must be an object")
        source_path = _normal_relative_path(mapping.get("source_path"), f"files[{index}].source_path")
        worker_path = _normal_relative_path(mapping.get("worker_path"), f"files[{index}].worker_path")
        sha256 = mapping.get("sha256")
        if not isinstance(sha256, str) or SHA256_RE.fullmatch(sha256) is None:
            raise ContractVerificationError(f"files[{index}].sha256 must be a lowercase SHA-256 digest")
        if not source_path.startswith(f"{SOURCE_PATH}/"):
            raise ContractVerificationError(f"files[{index}] source path drifted: {source_path}")
        if not worker_path.startswith(f"{TARGET_PATH}/"):
            raise ContractVerificationError(f"files[{index}] worker path drifted: {worker_path}")
        source_tail = source_path.removeprefix(f"{SOURCE_PATH}/")
        worker_tail = worker_path.removeprefix(f"{TARGET_PATH}/")
        if source_tail != worker_tail:
            raise ContractVerificationError(f"files[{index}] does not map the same source and worker relative path")
        if source_path in source_paths or worker_path in worker_paths:
            raise ContractVerificationError(f"duplicate mapping at files[{index}]")
        source_paths.add(source_path)
        worker_paths.append(worker_path)
        expected_actual.add(worker_tail)

    if len(files) != EXPECTED_MIRROR_FILE_COUNT:
        raise ContractVerificationError(
            f"equivalence manifest must contain exactly {EXPECTED_MIRROR_FILE_COUNT} files"
        )
    if worker_paths != sorted(worker_paths, key=str.casefold):
        raise ContractVerificationError("equivalence mappings must use stable worker-path ordering")

    actual = _relative_files(bundle_root)
    missing = sorted(expected_actual - actual)
    extra = sorted(actual - expected_actual)
    if missing:
        raise ContractVerificationError(f"missing mirror files: {', '.join(missing)}")
    if extra:
        raise ContractVerificationError(f"extra mirror files: {', '.join(extra)}")

    for mapping in files:
        worker_tail = mapping["worker_path"].removeprefix(f"{TARGET_PATH}/")
        actual_sha256 = _sha256(bundle_root / worker_tail)
        if actual_sha256 != mapping["sha256"]:
            raise ContractVerificationError(
                f"content SHA mismatch for {worker_tail}: expected {mapping['sha256']}, got {actual_sha256}"
            )

    _verify_gate_registry(bundle_root)
    _verify_source_table_semantics(bundle_root)
    return len(files)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-root", type=Path, default=DEFAULT_BUNDLE_ROOT)
    args = parser.parse_args(argv)
    try:
        file_count = verify_contract(args.bundle_root)
    except ContractVerificationError as exc:
        print(f"SEC regulatory contract verification failed: {exc}", file=sys.stderr)
        return 1
    print(f"SEC regulatory contract verified {file_count} mirrored files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
