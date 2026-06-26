"""Versioned contract bundle: build, hash, and verify integrity.

The worker repository owns the canonical quant-engine JSON Schemas; the backend
mirrors their hashes. This module turns that contract surface into a versioned,
verifiable bundle: a `manifest.json` recording `contract_version`, a per-file
`sha256`, and a single `bundle_sha256` over the whole set. The same verifier runs
in either repository so drift between worker and backend fails loud.

The hashed set is the schemas (`*.schema.json`) plus the positive/negative
fixtures (`fixtures/**/*.json`). `manifest.json` itself is never part of the set.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

MANIFEST_NAME = "manifest.json"


def compute_file_sha256(path: str | Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def iter_bundle_files(bundle_dir: str | Path) -> list[Path]:
    """Return the contract files in the bundle: schemas + fixtures, sorted.

    `manifest.json` is excluded so the manifest never hashes itself.
    """
    root = Path(bundle_dir)
    files: list[Path] = []
    files.extend(root.glob("*.schema.json"))
    files.extend(p for p in root.glob("fixtures/**/*.json") if p.is_file())
    return sorted(f for f in files if f.name != MANIFEST_NAME)


def bundle_sha256(files: list[dict[str, str]]) -> str:
    """Single digest over the (path, sha256) set, independent of input order."""
    canonical = json.dumps(
        sorted(({"path": f["path"], "sha256": f["sha256"]} for f in files), key=lambda x: x["path"]),
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_manifest(bundle_dir: str | Path, *, contract_version: str) -> dict[str, Any]:
    root = Path(bundle_dir)
    files = [
        {"path": p.relative_to(root).as_posix(), "sha256": compute_file_sha256(p)}
        for p in iter_bundle_files(root)
    ]
    files.sort(key=lambda f: f["path"])
    return {
        "contract_version": contract_version,
        "bundle_sha256": bundle_sha256(files),
        "files": files,
    }


def write_manifest(bundle_dir: str | Path, *, contract_version: str) -> Path:
    root = Path(bundle_dir)
    manifest = build_manifest(root, contract_version=contract_version)
    path = root / MANIFEST_NAME
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def verify_bundle(bundle_dir: str | Path) -> dict[str, Any]:
    """Recompute every hash and the bundle digest; return a closed verdict."""
    root = Path(bundle_dir)
    manifest = json.loads((root / MANIFEST_NAME).read_text(encoding="utf-8"))
    recorded = {f["path"]: f["sha256"] for f in manifest.get("files", [])}

    actual = {
        p.relative_to(root).as_posix(): compute_file_sha256(p)
        for p in iter_bundle_files(root)
    }

    missing = sorted(set(recorded) - set(actual))
    unexpected = sorted(set(actual) - set(recorded))
    mismatched = sorted(p for p in (set(recorded) & set(actual)) if recorded[p] != actual[p])

    # The manifest must be internally consistent: its bundle_sha256 must equal the
    # digest of its own recorded file list.
    expected_files = [{"path": p, "sha256": s} for p, s in recorded.items()]
    bundle_sha256_match = manifest.get("bundle_sha256") == bundle_sha256(expected_files)

    ok = not missing and not unexpected and not mismatched and bundle_sha256_match
    return {
        "contract_version": manifest.get("contract_version"),
        "bundle_sha256": manifest.get("bundle_sha256"),
        "missing": missing,
        "unexpected": unexpected,
        "mismatched": mismatched,
        "bundle_sha256_match": bundle_sha256_match,
        "ok": ok,
    }
