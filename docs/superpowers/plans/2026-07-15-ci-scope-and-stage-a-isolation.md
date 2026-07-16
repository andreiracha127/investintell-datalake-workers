# CI Scope and Stage A Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace duplicate, monolithic PR validation with one scoped required check and bind Stage A evidence only to its actual compute closure.

**Architecture:** A pure-Python change classifier owns CI lane routing and is called by the always-present `quant-engine` GitHub Actions job. Stage A gets a separate explicit file manifest, while the shared provenance helpers accept an optional surface list so Phase 1 retains its existing broad-tree contract. The Stage A binding test becomes the fast precheck and final evidence is regenerated once after all code is committed.

**Tech Stack:** Python 3.13, pytest, Ruff 0.15.9, GitHub Actions YAML, Git object hashes, Docker-based Stage A measurement.

## Global Constraints

- Exactly one workflow run per commit pushed to an open PR; `push` validation is limited to `main`.
- Preserve the required job/check name `quant-engine` and create it for every PR.
- N-PORT CI uses no live database, network service, Docker build, deployment credential, or production data.
- Unknown code/config paths fail closed by selecting both lanes; documentation-only changes may no-op green.
- The expensive quant suite runs only after the Stage A binding precheck succeeds.
- Phase 1 keeps its existing broad `COMPUTE_TREES`; only Stage A receives the explicit compute-file manifest.
- Stage A remains measurement-only: A5 is blocked, `freeze_ready=false`, and `runtime_activation=false`.
- Do not push intermediate commits. Perform one remote push only after local code gates and final evidence verification.

---

## File map

- Create `scripts/ci/classify_changes.py`: deterministic path-to-lane classifier and GitHub output CLI.
- Create `tests/test_ci_path_scope.py`: behavioral routing tests for N-PORT, quant, shared, ambiguous, and documentation paths.
- Modify `.github/workflows/ci.yml`: single-run triggers, concurrency cancellation, conditional N-PORT and quant lanes, and fast binding precheck.
- Modify `tests/test_remote_ci_runner.py`: parsed workflow contract covering triggers, conditions, ordering, and stable job identity.
- Create `harness/direct_activation/compute_manifest.py`: Stage A entry points and exact compute-file manifest.
- Create `tests/test_stage_a_compute_manifest.py`: manifest integrity, project-import closure, and N-PORT exclusion tests.
- Modify `harness/dark_launch/measure_observability.py`: parameterize provenance helpers without changing Phase 1 defaults.
- Modify `harness/direct_activation/measure_stage_a.py`: pass the Stage A manifest to clean-tree and hash helpers and update provenance documentation.
- Modify `tests/test_direct_activation_stage_a.py`: bind committed evidence to the explicit manifest rather than broad directories.
- Modify `docs/superpowers/specs/2026-07-15-ci-scope-and-stage-a-isolation-design.md`: record the achievable pre/post-evidence verification order.
- Regenerate `artifacts/a5/open_macro_v03_direct_activation_stage_a_001/*.json`: bind the final 16-run Stage A result to the final code commit.

### Task 1: Deterministic CI path classifier

**Files:**
- Create: `scripts/ci/classify_changes.py`
- Create: `tests/test_ci_path_scope.py`

**Interfaces:**
- Produces: `Scope(nport_changed: bool, quant_changed: bool)`.
- Produces: `classify_paths(paths: Iterable[str]) -> Scope`.
- Produces: CLI `python scripts/ci/classify_changes.py --base <sha> --head <sha> --github-output <path>`.
- Produces GitHub outputs `nport_changed=true|false` and `quant_changed=true|false`.

- [x] **Step 1: Write failing classifier tests**

```python
from scripts.ci.classify_changes import Scope, classify_paths


def test_nport_only_paths_select_only_nport() -> None:
    assert classify_paths([
        "src/workers/nport_lookthrough.py",
        "schemas/nport_lookthrough.sql",
        "tests/test_nport_lookthrough.py",
        "tests/test_load_nport_fund_flows.py",
    ]) == Scope(nport_changed=True, quant_changed=False)


def test_stage_a_compute_path_selects_quant_only() -> None:
    assert classify_paths(["src/quadrant_score.py"]) == Scope(False, True)


def test_shared_db_and_nport_input_fixture_select_both() -> None:
    assert classify_paths(["src/db.py"]) == Scope(True, True)
    assert classify_paths([
        "fixtures/input_packs/p0_sources/open_macro_v03/sec_nport_holdings.json"
    ]) == Scope(True, True)


def test_workflow_dependency_and_unknown_code_changes_fail_closed() -> None:
    assert classify_paths([".github/workflows/ci.yml"]) == Scope(True, True)
    assert classify_paths(["requirements.quant-engine.lock"]) == Scope(True, True)
    assert classify_paths(["tools/new_worker.py"]) == Scope(True, True)


def test_documentation_only_change_selects_no_lane() -> None:
    assert classify_paths(["docs/architecture/decision.md"]) == Scope(False, False)
```

- [x] **Step 2: Run the tests and verify RED**

Run: `python -m pytest tests/test_ci_path_scope.py -q`

Expected: collection error `ModuleNotFoundError: No module named 'scripts.ci.classify_changes'`.

- [x] **Step 3: Implement the classifier and fail-closed CLI**

```python
from __future__ import annotations

import argparse
import fnmatch
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

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
    "src/workers/nport_*.py",
    "scripts/load_nport_fund_flows.py",
    "schemas/nport_*.sql",
    "tests/test_nport_*.py",
    "tests/test_load_nport_*.py",
)
SHARED_PATHS = {"src/db.py"}
QUANT_PREFIXES = (
    "harness/",
    "packages/",
    "services/",
    "src/input_packs/",
    "tests/input_packs/",
    "tests/quant_core/",
    "tests/quant_engine/",
)
QUANT_FILE_SUFFIXES = (".json", ".py", ".sql", ".toml", ".lock", ".yml", ".yaml")


def classify_paths(paths: Iterable[str]) -> Scope:
    nport = False
    quant = False
    for raw_path in paths:
        path = raw_path.replace("\\", "/").lstrip("./")
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
        matched_nport = any(fnmatch.fnmatchcase(path, pattern) for pattern in NPORT_PATTERNS)
        if matched_nport:
            nport = True
            continue
        if path.startswith(QUANT_PREFIXES) or (
            path.startswith("src/") and not path.startswith("src/workers/nport_")
        ) or (path.startswith("tests/") and not path.startswith("tests/test_nport_")):
            quant = True
            continue
        if path.endswith(QUANT_FILE_SUFFIXES) and not path.startswith("docs/"):
            nport = quant = True
    return Scope(nport, quant)


def changed_paths(base: str, head: str, *, root: Path) -> list[str]:
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
        ["git", "diff", "--name-only", "--diff-filter=ACMRT", base, head],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
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
```

This runs `git diff --name-only --diff-filter=ACMRT <base> <head>` with
`check=True`, rejects missing/all-zero revisions, prints the changed paths and
selected scope, and appends both lowercase boolean outputs to the exact GitHub
output file.

- [x] **Step 4: Extend tests for the CLI output and invalid revisions**

Use a temporary GitHub output file and invoke `main()` with the current repository `HEAD^` and `HEAD`; assert both output keys exist. Monkeypatch only the subprocess boundary for the invalid-revision case and assert `SystemExit` is non-zero rather than silently selecting no lanes.

- [x] **Step 5: Run classifier tests and verify GREEN**

Run: `python -m pytest tests/test_ci_path_scope.py -q`

Expected: all classifier tests pass.

- [x] **Step 6: Commit the classifier slice**

```powershell
git add scripts/ci/classify_changes.py tests/test_ci_path_scope.py
git commit -m "feat(ci): classify changed paths into scoped lanes"
```

### Task 2: One always-present, selectively executed GitHub check

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `tests/test_remote_ci_runner.py`

**Interfaces:**
- Consumes: `scripts/ci/classify_changes.py` outputs from Task 1.
- Preserves: workflow job id and display name `quant-engine`.
- Produces: `nport_changed` and `quant_changed` step outputs used by all conditional steps.

- [x] **Step 1: Replace text-only workflow assertions with parsed contract tests**

Load the YAML using `yaml.load(text, Loader=yaml.BaseLoader)` so YAML 1.1 does not coerce the `on` key to a boolean. Add assertions that:

```python
workflow = yaml.load(_workflow_text(), Loader=yaml.BaseLoader)
assert workflow["on"]["push"]["branches"] == ["main"]
assert "pull_request" in workflow["on"]
assert workflow["concurrency"]["cancel-in-progress"] == "true"
job = workflow["jobs"]["quant-engine"]
assert job["name"] == "quant-engine"
steps = {step["name"]: step for step in job["steps"]}
assert steps["Detect changed paths"]["id"] == "changes"
assert "nport_changed == 'true'" in steps["Run focused N-PORT tests"]["if"]
assert "quant_changed == 'true'" in steps["Verify Stage A binding"]["if"]
names = [step["name"] for step in job["steps"]]
assert names.index("Verify Stage A binding") < names.index(
    "Run governance and quant-engine tests"
)
```

Also assert the N-PORT pytest command names all three focused test files, Ruff is pinned to `0.15.9`, and every expensive quant verification/compile/artifact step has the quant condition.

- [x] **Step 2: Run the workflow contract and verify RED**

Run: `python -m pytest tests/test_remote_ci_runner.py -q`

Expected: failure because `feat/**` is still a push branch and `concurrency`, path detection, and conditional lanes do not exist.

- [x] **Step 3: Implement the workflow structure**

Use this step order in `.github/workflows/ci.yml`:

```yaml
on:
  pull_request:
  push:
    branches:
      - main

concurrency:
  group: ${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}
  cancel-in-progress: true

jobs:
  quant-engine:
    name: quant-engine
    runs-on: ubuntu-latest
    steps:
      - name: Check out repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.13"
      - name: Detect changed paths
        id: changes
        env:
          BASE_SHA: ${{ github.event.pull_request.base.sha || github.event.before }}
          HEAD_SHA: ${{ github.sha }}
        run: >-
          python scripts/ci/classify_changes.py
          --base "$BASE_SHA" --head "$HEAD_SHA"
          --github-output "$GITHUB_OUTPUT"
```

Install dependencies only when either output is true. Run the three N-PORT tests, Ruff, and compileall only when `nport_changed == 'true'`. Run the single Stage A binding test immediately after dependency installation when `quant_changed == 'true'`; condition every existing quant verification, full pytest, compilation, and artifact step on the same output. Add a no-op summary step when both outputs are false.

- [x] **Step 4: Run workflow and classifier contracts and verify GREEN**

Run: `python -m pytest tests/test_remote_ci_runner.py tests/test_ci_path_scope.py -q`

Expected: all tests pass.

- [x] **Step 5: Run the exact local N-PORT lane**

```powershell
python -m pytest tests/test_nport_lookthrough.py tests/test_nport_cusip_enrichment.py tests/test_load_nport_fund_flows.py -q
ruff check src/workers/nport_lookthrough.py src/workers/nport_cusip_enrichment.py scripts/load_nport_fund_flows.py tests/test_nport_lookthrough.py tests/test_nport_cusip_enrichment.py tests/test_load_nport_fund_flows.py
python -m compileall -q src/workers/nport_lookthrough.py src/workers/nport_cusip_enrichment.py scripts/load_nport_fund_flows.py
```

Expected: pytest passes with only the existing environment-dependent skips; Ruff and compileall exit 0.

- [x] **Step 6: Commit the workflow slice**

```powershell
git add .github/workflows/ci.yml tests/test_remote_ci_runner.py
git commit -m "ci: scope PR validation and cancel superseded runs"
```

### Task 3: Explicit Stage A compute manifest and scoped provenance

**Files:**
- Create: `harness/direct_activation/compute_manifest.py`
- Create: `tests/test_stage_a_compute_manifest.py`
- Modify: `scripts/ci/classify_changes.py`
- Modify: `tests/test_ci_path_scope.py`
- Modify: `harness/dark_launch/measure_observability.py`
- Modify: `harness/direct_activation/measure_stage_a.py`
- Modify: `tests/test_direct_activation_stage_a.py`

**Interfaces:**
- Produces: `STAGE_A_ENTRYPOINTS: tuple[str, ...]`.
- Produces: `STAGE_A_COMPUTE_PATHS: tuple[str, ...]` containing repository-relative file paths.
- Extends: `classify_paths()` to consume `STAGE_A_COMPUTE_PATHS` as its Stage A routing source.
- Extends: `_worker_commit(compute_paths: tuple[str, ...] = COMPUTE_TREES) -> str`.
- Extends: `_compute_tree_hashes(commit: str, compute_paths: tuple[str, ...] = COMPUTE_TREES) -> dict[str, str]`.
- Preserves: no-argument behavior for all Phase 1 callers.

- [x] **Step 1: Write failing manifest and provenance tests**

Use this AST helper in the test file so the contract follows real project imports
without importing application modules:

```python
import ast
from collections import deque
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SEARCH_ROOTS = (
    ROOT,
    ROOT / "packages" / "investintell_quant_core" / "src",
    ROOT / "services" / "quant_engine" / "src",
)


def _resolve_module(module: str) -> Path | None:
    relative = Path(*module.split("."))
    for search_root in SEARCH_ROOTS:
        module_file = search_root / relative.with_suffix(".py")
        package_file = search_root / relative / "__init__.py"
        if module_file.is_file():
            return module_file
        if package_file.is_file():
            return package_file
    return None


def _module_name(path: Path) -> str:
    for search_root in SEARCH_ROOTS:
        try:
            relative = path.relative_to(search_root)
        except ValueError:
            continue
        parts = list(relative.with_suffix("").parts)
        if parts[-1] == "__init__":
            parts.pop()
        return ".".join(parts)
    raise AssertionError(f"outside project search roots: {path}")


def _package_initializers(path: Path) -> set[Path]:
    result: set[Path] = set()
    for search_root in SEARCH_ROOTS:
        try:
            relative = path.relative_to(search_root)
        except ValueError:
            continue
        cursor = search_root
        for part in relative.parts[:-1]:
            cursor /= part
            init = cursor / "__init__.py"
            if init.is_file():
                result.add(init)
        break
    return result


def _direct_imports(path: Path) -> set[Path]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    current = _module_name(path).split(".")
    if path.name != "__init__.py":
        current.pop()
    resolved: set[Path] = set()
    for node in ast.walk(tree):
        modules: list[str] = []
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            parent = current[: len(current) - max(node.level - 1, 0)] if node.level else []
            base_parts = parent + (node.module.split(".") if node.module else [])
            base = ".".join(base_parts)
            for alias in node.names:
                candidate = f"{base}.{alias.name}" if base else alias.name
                if _resolve_module(candidate) is not None:
                    modules.append(candidate)
                elif base:
                    modules.append(base)
        for module in modules:
            imported = _resolve_module(module)
            if imported is not None:
                resolved.add(imported)
                resolved.update(_package_initializers(imported))
    return resolved


def project_import_closure(entry_points: tuple[str, ...]) -> set[str]:
    pending = deque((ROOT / path).resolve() for path in entry_points)
    seen: set[Path] = set()
    while pending:
        path = pending.popleft()
        if path in seen:
            continue
        seen.add(path)
        pending.extend(_direct_imports(path) - seen)
    return {path.relative_to(ROOT).as_posix() for path in seen}
```

The tests assert:

```python
def test_manifest_is_sorted_unique_existing_files() -> None:
    assert STAGE_A_COMPUTE_PATHS == tuple(sorted(set(STAGE_A_COMPUTE_PATHS)))
    assert all((ROOT / path).is_file() for path in STAGE_A_COMPUTE_PATHS)


def test_manifest_excludes_unrelated_nport_workers() -> None:
    assert not any("nport" in path.lower() for path in STAGE_A_COMPUTE_PATHS)


def test_stage_a_project_import_closure_is_manifested() -> None:
    closure = project_import_closure(STAGE_A_ENTRYPOINTS)
    assert closure <= set(STAGE_A_COMPUTE_PATHS), sorted(
        closure - set(STAGE_A_COMPUTE_PATHS)
    )
```

Add this behavior test for the provenance call boundary:

```python
def test_stage_a_measure_uses_explicit_compute_manifest(monkeypatch) -> None:
    from harness.direct_activation import measure_stage_a as ms

    received: list[tuple[str, ...]] = []
    monkeypatch.setattr(ms.mo, "_load_repeatability_module", lambda: object())
    monkeypatch.setattr(
        ms.mo,
        "_worker_commit",
        lambda paths: received.append(paths) or "a" * 40,
    )
    monkeypatch.setattr(
        ms.mo,
        "_compute_tree_hashes",
        lambda commit, paths: received.append(paths) or {},
    )

    result = ms.measure(0, skip_container=True, image="unused", repo=ROOT)

    assert result["tree_hashes"] == {}
    assert received == [STAGE_A_COMPUTE_PATHS, STAGE_A_COMPUTE_PATHS]
```

- [x] **Step 2: Run the manifest tests and verify RED**

Run: `python -m pytest tests/test_stage_a_compute_manifest.py -q`

Expected: collection error because `harness.direct_activation.compute_manifest` does not exist.

- [x] **Step 3: Add the manifest and parameterize provenance helpers**

Create this explicit sorted manifest; if the RED closure output identifies another
real project import, add that exact file before making the test green:

```python
STAGE_A_ENTRYPOINTS = (
    "harness/direct_activation/measure_stage_a.py",
    "scripts/repeatability_matrix.py",
)

STAGE_A_COMPUTE_PATHS = tuple(sorted((
    "harness/__init__.py",
    "harness/dark_launch/__init__.py",
    "harness/dark_launch/measure_observability.py",
    "harness/direct_activation/__init__.py",
    "harness/direct_activation/build_stage_a_amendment.py",
    "harness/direct_activation/compute_manifest.py",
    "harness/direct_activation/live_validation.py",
    "harness/direct_activation/measure_stage_a.py",
    "harness/direct_activation/measure_stage_a_child.py",
    "harness/phase0q/__init__.py",
    "harness/phase0q/decision.py",
    "harness/phase0q/pit.py",
    "harness/phase0q/sleeve.py",
    "scripts/p1_export/__init__.py",
    "scripts/p1_export/export_p1_sources.py",
    "scripts/repeatability_matrix.py",
    "scripts/__init__.py",
    "services/quant_engine/src/investintell_quant_engine/__init__.py",
    "services/quant_engine/src/investintell_quant_engine/comparator.py",
    "services/quant_engine/src/investintell_quant_engine/outputs_manifest.py",
    "services/quant_engine/src/investintell_quant_engine/repeatability.py",
    "services/quant_engine/src/investintell_quant_engine/version.py",
    "src/__init__.py",
    "src/db.py",
    "src/input_packs/__init__.py",
    "src/input_packs/hashing.py",
    "src/input_packs/manifest.py",
    "src/input_packs/p0_contract.py",
    "src/input_packs/p0_derived.py",
    "src/input_packs/verifier.py",
    "src/macro_sources.py",
    "src/macro_transforms.py",
    "src/quadrant_assemble.py",
    "src/quadrant_confidence.py",
    "src/quadrant_hysteresis.py",
    "src/quadrant_score.py",
    "src/quadrant_snapshot.py",
    "src/quadrant_staleness.py",
)))
```

Change the shared helpers without changing defaults:

```python
def _worker_commit(compute_paths: tuple[str, ...] = COMPUTE_TREES) -> str:
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    dirty: list[str] = []
    for line in status.splitlines():
        changed_paths = [
            path.strip('"').replace("\\", "/")
            for path in line[3:].split(" -> ")
        ]
        for changed_path in changed_paths:
            if any(
                changed_path.startswith(surface)
                if surface.endswith("/")
                else changed_path == surface
                for surface in compute_paths
            ):
                dirty.append(line)
                break
    if dirty:
        raise RuntimeError(
            "refusing to measure from a dirty compute tree "
            f"(worker_commit would be unreproducible): {dirty}"
        )
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _compute_tree_hashes(
    commit: str,
    compute_paths: tuple[str, ...] = COMPUTE_TREES,
) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for surface in compute_paths:
        relative = surface.rstrip("/")
        hashes[relative] = subprocess.run(
            ["git", "rev-parse", f"{commit}:{relative}"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    return hashes
```

Directory surfaces ending in `/` retain prefix matching. Exact file surfaces match only the normalized path itself. `measure_stage_a.measure()` passes `STAGE_A_COMPUTE_PATHS` to both helpers; Phase 1 continues calling them without the optional argument.

Import `STAGE_A_COMPUTE_PATHS` in `scripts/ci/classify_changes.py`, select the quant
lane when a changed path is in that tuple, and add this routing contract:

```python
def test_every_stage_a_compute_path_selects_quant() -> None:
    for path in STAGE_A_COMPUTE_PATHS:
        assert classify_paths([path]).quant_changed, path
```

- [x] **Step 4: Update the committed-evidence binding contract**

In `test_reproducibility_record_pins_a_clean_16_run_reproduction`, replace the hard-coded broad directory set with `set(STAGE_A_COMPUTE_PATHS)`. Keep the existing HEAD and present-worker-commit Git object comparisons for every path. Update comments/docstrings to say Stage A reuses the provenance mechanism but certifies its own explicit compute closure.

- [x] **Step 5: Run manifest/provenance tests and close any import-closure gaps**

Run: `python -m pytest tests/test_stage_a_compute_manifest.py tests/test_ci_path_scope.py tests/test_direct_activation_stage_a.py -q`

Expected before recertification: manifest/provenance unit tests pass; the committed reproduction binding test is the only expected failure because the artifact still contains the old broad surfaces. Any missing import listed by the closure test is added to the explicit manifest before proceeding.

- [x] **Step 6: Run all non-evidence local gates**

Run the workflow/classifier tests, N-PORT lane, Ruff over changed Python files, compileall over changed Python files, and the governance/quant command excluding only `test_reproducibility_record_pins_a_clean_16_run_reproduction`.

Expected: all executed checks pass; the excluded binding test remains deliberately red until Task 4.

- [x] **Step 7: Commit the final code surface before measuring**

```powershell
git add .github/workflows/ci.yml scripts/ci tests/test_ci_path_scope.py tests/test_remote_ci_runner.py harness/direct_activation/compute_manifest.py harness/direct_activation/measure_stage_a.py harness/dark_launch/measure_observability.py tests/test_stage_a_compute_manifest.py tests/test_direct_activation_stage_a.py docs/superpowers
git commit -m "fix(ci): isolate Stage A from unrelated worker changes"
```

Verify `git status --short` is empty before official measurement.

### Task 4: One final Stage A recertification

**Files:**
- Regenerate: `artifacts/a5/open_macro_v03_direct_activation_stage_a_001/live_validation_record.json`
- Regenerate: `artifacts/a5/open_macro_v03_direct_activation_stage_a_001/reproducibility_record.json`
- Regenerate: `artifacts/a5/open_macro_v03_direct_activation_stage_a_001/slo_conformance_record.json`
- Regenerate if latency requires it: `artifacts/a5/open_macro_v03_direct_activation_stage_a_001/slo_threshold_amendment_record.json`

**Interfaces:**
- Consumes: final clean code commit and `STAGE_A_COMPUTE_PATHS`.
- Produces: 8 host + 8 container identical runs bound to that commit's exact file blobs.

- [ ] **Step 1: Regenerate the live-validation record at the clean code HEAD**

Run: `python -m harness.direct_activation.live_validation`

Expected: exit 0, decision/allocation summary printed, and `A5 blocked; Stage A validates only` retained.

- [ ] **Step 2: Run the official Stage A measurement once**

Run: `python -m harness.direct_activation.measure_stage_a --amend-latency`

Expected: 8 successful host runs plus 8 successful container runs, `mismatch_count=0`, all hard SLOs conforming, latency either conforming directly or through the signed amendment, and records written to the committed Stage A directory.

- [ ] **Step 3: Verify generated evidence before committing**

Run: `python -m pytest tests/test_direct_activation_stage_a.py -q`

Expected: all Stage A tests pass, including the HEAD blob binding and blocked-governance assertions.

- [ ] **Step 4: Commit only generated evidence**

```powershell
git add artifacts/a5/open_macro_v03_direct_activation_stage_a_001
git diff --cached --check
git commit -m "test(a5): rebind Stage A to scoped compute manifest"
```

### Task 5: Full verification and branch handoff

**Files:**
- Verify only; no new implementation files expected.

**Interfaces:**
- Produces: fresh local evidence for every acceptance criterion before any remote push.

- [ ] **Step 1: Run the exact N-PORT lane**

Run the three commands from Task 2 Step 5.

Expected: pytest, Ruff, and compileall all exit 0.

- [ ] **Step 2: Run the workflow, classifier, and manifest contracts**

Run: `python -m pytest tests/test_remote_ci_runner.py tests/test_ci_path_scope.py tests/test_stage_a_compute_manifest.py -q`

Expected: all tests pass.

- [ ] **Step 3: Run the complete quant/governance CI command locally**

Run the exact pytest path list from `.github/workflows/ci.yml`, followed by its compileall command and both artifact verifier commands.

Expected: zero failures; Stage A binding passes before and inside the complete suite.

- [ ] **Step 4: Verify repository and commit scope**

```powershell
git diff --check origin/feat/nport-equity-geography...HEAD
git status --short --branch
git log --oneline origin/feat/nport-equity-geography..HEAD
```

Expected: no whitespace errors, clean worktree, and only the design/plan, CI, manifest/provenance, and recertification commits.

- [ ] **Step 5: Invoke branch-finishing workflow**

Use `superpowers:finishing-a-development-branch`, present the required integration choices, and do not push until the user selects the existing-PR push option. For that option, push the feature branch once, preserve the worktree, verify exactly one GitHub Actions run for the pushed SHA, and monitor it to terminal status.
