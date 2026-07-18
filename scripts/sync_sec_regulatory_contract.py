#!/usr/bin/env python3
"""Synchronize the SEC contract from its merged application authority."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TARGET_ROOT = ROOT / "contracts" / "sec-regulatory" / "v1"
SOURCE_PREFIX = "backend/contracts/sec-regulatory/v1"
TARGET_PREFIX = "contracts/sec-regulatory/v1"
SOURCE_REPOSITORY = "andreiracha127/investintell-light"
PHASE0_BASE_COMMIT = "a5c6823e7e2c5b2c54aecf4f855528d0e4b716c3"
SOURCE_COMMIT = "24d7732a38f438a523ba2f09986086e961c2165b"
WORKER_BASE_COMMIT = "8636c14f08aa8d27b0f8e1d627a65072bf9772bf"
WORKER_ONLY_FILES = ("worker-equivalence-manifest.json", "worker-provenance.json")


def _git(app_repo: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", *args], cwd=app_repo, check=False, capture_output=True,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace").strip())
    return result.stdout


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=False, ensure_ascii=True) + "\n",
        encoding="utf-8", newline="\n",
    )


def sync(app_repo: Path) -> int:
    _git(app_repo, "cat-file", "-e", f"{SOURCE_COMMIT}^{{commit}}")
    names = _git(
        app_repo, "ls-tree", "-r", "--name-only", SOURCE_COMMIT, "--", SOURCE_PREFIX,
    ).decode("utf-8").splitlines()
    names = sorted((name for name in names if name.startswith(f"{SOURCE_PREFIX}/")), key=str.casefold)
    if not names:
        raise RuntimeError("merged application commit contains no SEC contract files")

    mappings: list[dict[str, str]] = []
    source_tails: set[str] = set()
    for source_path in names:
        tail = source_path.removeprefix(f"{SOURCE_PREFIX}/")
        source_tails.add(tail)
        payload = _git(app_repo, "show", f"{SOURCE_COMMIT}:{source_path}")
        target = TARGET_ROOT / tail
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        mappings.append({
            "source_path": source_path,
            "worker_path": f"{TARGET_PREFIX}/{tail}",
            "sha256": hashlib.sha256(payload).hexdigest(),
        })

    existing_tails = {
        path.relative_to(TARGET_ROOT).as_posix()
        for path in TARGET_ROOT.rglob("*") if path.is_file()
    } - set(WORKER_ONLY_FILES)
    stale = sorted(existing_tails - source_tails)
    if stale:
        raise RuntimeError(f"stale worker mirror files require review: {stale}")

    shared = {
        "schema_version": 1,
        "contract_version": "v1",
        "source_repository": SOURCE_REPOSITORY,
        "phase0_base_commit": PHASE0_BASE_COMMIT,
        "source_commit": SOURCE_COMMIT,
        "source_path": SOURCE_PREFIX,
        "target_path": TARGET_PREFIX,
        "worker_only_files": list(WORKER_ONLY_FILES),
    }
    _write_json(
        TARGET_ROOT / "worker-equivalence-manifest.json",
        {**shared, "files": mappings},
    )
    _write_json(
        TARGET_ROOT / "worker-provenance.json",
        {**shared, "worker_base_commit": WORKER_BASE_COMMIT},
    )
    return len(mappings)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-repo", type=Path, required=True)
    args = parser.parse_args()
    count = sync(args.app_repo.resolve())
    print(f"synchronized {count} files from merged app commit {SOURCE_COMMIT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
