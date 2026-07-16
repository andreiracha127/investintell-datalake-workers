from __future__ import annotations

import ast
from collections import deque
from pathlib import Path

from harness.direct_activation.compute_manifest import (
    STAGE_A_COMPUTE_PATHS,
    STAGE_A_ENTRYPOINTS,
)
from scripts.ci import classify_changes as ci_scope
from scripts.ci.classify_changes import classify_paths


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
            parent = (
                current[: len(current) - max(node.level - 1, 0)]
                if node.level
                else []
            )
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


def test_every_stage_a_compute_path_selects_quant() -> None:
    assert ci_scope.STAGE_A_COMPUTE_PATHS is STAGE_A_COMPUTE_PATHS
    for path in STAGE_A_COMPUTE_PATHS:
        assert classify_paths([path]).quant_changed, path


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
