"""Classify a Git diff into the focused CI lanes that must run."""

from __future__ import annotations

import argparse
import fnmatch
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from harness.direct_activation.compute_manifest import STAGE_A_COMPUTE_PATHS


@dataclass(frozen=True)
class Scope:
    nport_changed: bool = False
    quant_changed: bool = False


ALL_LANES = {
    ".github/workflows/ci.yml",
    "requirements.quant-engine.lock",
    "scripts/ci/classify_changes.py",
}
NPORT_PATTERNS = (
    "requirements.txt",
    "src/bond_pilot/*.py",
    "src/workers/nport_*.py",
    "src/workers/_openfigi.py",
    "src/workers/_yahoo_sector.py",
    "scripts/backfill_nport_holding_attributes.py",
    "scripts/load_nport_fund_flows.py",
    "scripts/run_bond_pilot.py",
    "schemas/nport_*.sql",
    "tests/bond_pilot/*.py",
    "tests/bond_pilot/fixtures/*.json",
    "tests/test_nport_*.py",
    "tests/test_openfigi.py",
    "tests/test_yahoo_sector.py",
    "tests/test_backfill_nport_holding_attributes.py",
    "tests/test_load_nport_*.py",
    "docs/bond-pilot-option-a.md",
)
SHARED_PATHS = {"src/db.py"}
QUANT_PATHS = {"requirements.quant-engine.in"}
QUANT_PREFIXES = (
    "contracts/quant-engine/",
    "docker/quant-engine/",
    "harness/",
    "packages/",
    "services/",
    "src/input_packs/",
    "tests/input_packs/",
    "tests/quant_core/",
    "tests/quant_engine/",
)
CODE_AND_CONFIG_SUFFIXES = (
    ".json",
    ".lock",
    ".py",
    ".sql",
    ".toml",
    ".yaml",
    ".yml",
)


def _normalize_path(raw_path: str) -> str:
    path = raw_path.replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]
    return path


def classify_paths(paths: Iterable[str]) -> Scope:
    """Return the lanes required by ``paths``, failing closed for unknown code."""
    nport = False
    quant = False
    stage_a_paths = set(STAGE_A_COMPUTE_PATHS)
    for raw_path in paths:
        path = _normalize_path(raw_path)
        if not path:
            continue
        if path in ALL_LANES:
            nport = quant = True
            continue
        if path in SHARED_PATHS or (
            path.startswith("fixtures/input_packs/") and "/sec_nport_" in path
        ):
            nport = quant = True
            continue
        if any(fnmatch.fnmatchcase(path, pattern) for pattern in NPORT_PATTERNS):
            nport = True
            continue
        if (
            path in QUANT_PATHS
            or path in stage_a_paths
            or path.startswith(QUANT_PREFIXES)
            or (path.startswith("src/") and not path.startswith("src/workers/nport_"))
            or (
                path.startswith("tests/")
                and not path.startswith("tests/test_nport_")
                and not path.startswith("tests/test_load_nport_")
            )
        ):
            quant = True
            continue
        if path.endswith(CODE_AND_CONFIG_SUFFIXES) and not path.startswith("docs/"):
            nport = quant = True
    return Scope(nport_changed=nport, quant_changed=quant)


def changed_paths(base: str, head: str, *, root: Path) -> list[str]:
    """Return changed paths between two verified commits."""
    for revision in (base, head):
        if not revision or set(revision) == {"0"}:
            raise ValueError(f"invalid Git revision: {revision!r}")
        subprocess.run(
            ["git", "rev-parse", "--verify", f"{revision}^{{commit}}"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        )
    result = subprocess.run(
        [
            "git",
            "diff",
            "--name-status",
            "-z",
            "--diff-filter=ACDMRT",
            base,
            head,
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    tokens = [token for token in result.stdout.split("\0") if token]
    paths: list[str] = []
    index = 0
    while index < len(tokens):
        status = tokens[index]
        index += 1
        path_count = 2 if status.startswith(("R", "C")) else 1
        paths.extend(tokens[index : index + path_count])
        index += path_count
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--github-output", type=Path, required=True)
    args = parser.parse_args(argv)

    root = Path(__file__).resolve().parents[2]
    paths = changed_paths(args.base, args.head, root=root)
    scope = classify_paths(paths)

    print("changed paths:")
    for path in paths:
        print(f"  {path}")
    print(f"scope: nport={scope.nport_changed} quant={scope.quant_changed}")

    with args.github_output.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(f"nport_changed={str(scope.nport_changed).lower()}\n")
        stream.write(f"quant_changed={str(scope.quant_changed).lower()}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
