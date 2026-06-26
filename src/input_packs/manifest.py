"""Manifest assembly and aggregate hashing for certified input packs."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from .hashing import canonical_json_sha256, file_sha256

MANIFEST_NAME = "manifest.json"

REQUIRED_FILES: tuple[str, ...] = (
    "manifest.json",
    "SOURCE.json",
    "raw_snapshot_manifest.json",
    "canonical_snapshot_manifest.json",
    "derived_feature_manifest.json",
    "table_hashes.json",
    "provenance.json",
)

REQUIRED_DIRS: tuple[str, ...] = ("schemas", "data", "reports")

COMPONENT_HASH_FIELDS: Mapping[str, str] = {
    "raw_snapshot_manifest.json": "raw_snapshot_sha256",
    "canonical_snapshot_manifest.json": "canonical_snapshot_sha256",
    "derived_feature_manifest.json": "derived_feature_sha256",
}


def iter_pack_files(pack_dir: str | Path, *, include_manifest: bool = False) -> list[Path]:
    """Return all pack files sorted by relative POSIX path."""
    root = Path(pack_dir)
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if not include_manifest and rel == MANIFEST_NAME:
            continue
        files.append(path)
    return sorted(files, key=lambda p: p.relative_to(root).as_posix())


def file_entries(pack_dir: str | Path, *, include_manifest: bool = False) -> list[dict[str, str]]:
    root = Path(pack_dir)
    return [
        {"path": path.relative_to(root).as_posix(), "sha256": file_sha256(path)}
        for path in iter_pack_files(root, include_manifest=include_manifest)
    ]


def normalized_manifest_for_pack_hash(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Return the manifest view used for aggregate pack hashing."""
    normalized = deepcopy(dict(manifest))
    normalized["input_pack_sha256"] = ""
    return normalized


def compute_input_pack_sha256(pack_dir: str | Path, manifest: Mapping[str, Any]) -> str:
    """Compute the aggregate pack digest without path-dependent metadata."""
    payload = {
        "files": file_entries(pack_dir, include_manifest=False),
        "manifest": normalized_manifest_for_pack_hash(manifest),
    }
    return canonical_json_sha256(payload)


def build_manifest(pack_dir: str | Path, base_manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Fill component hashes and ``input_pack_sha256`` for a pack directory."""
    root = Path(pack_dir)
    manifest = deepcopy(dict(base_manifest))
    manifest["runtime_activation"] = False

    for filename, field in COMPONENT_HASH_FIELDS.items():
        path = root / filename
        if path.exists():
            manifest[field] = file_sha256(path)

    manifest["input_pack_sha256"] = compute_input_pack_sha256(root, manifest)
    return manifest


def write_manifest(pack_dir: str | Path, base_manifest: Mapping[str, Any]) -> Path:
    """Write a deterministic pretty manifest to ``manifest.json``."""
    root = Path(pack_dir)
    manifest = build_manifest(root, base_manifest)
    path = root / MANIFEST_NAME
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path

