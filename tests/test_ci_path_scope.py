from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.ci.classify_changes import Scope, changed_paths, classify_paths, main


ROOT = Path(__file__).resolve().parents[1]


def test_nport_only_paths_select_only_nport() -> None:
    assert classify_paths(
        [
            "src/workers/nport_lookthrough.py",
            "schemas/nport_lookthrough.sql",
            "tests/test_nport_lookthrough.py",
            "tests/test_load_nport_fund_flows.py",
        ]
    ) == Scope(nport_changed=True, quant_changed=False)


def test_stage_a_compute_path_selects_quant_only() -> None:
    assert classify_paths(["src/quadrant_score.py"]) == Scope(
        nport_changed=False,
        quant_changed=True,
    )


def test_shared_db_and_nport_input_fixture_select_both() -> None:
    assert classify_paths(["src/db.py"]) == Scope(True, True)
    assert classify_paths(
        ["fixtures/input_packs/p0_sources/open_macro_v03/sec_nport_holdings.json"]
    ) == Scope(True, True)


@pytest.mark.parametrize(
    "path",
    [
        ".github/workflows/ci.yml",
        "requirements.quant-engine.lock",
        "tools/new_worker.py",
    ],
)
def test_workflow_dependency_and_unknown_code_changes_fail_closed(path: str) -> None:
    assert classify_paths([path]) == Scope(True, True)


def test_documentation_only_change_selects_no_lane() -> None:
    assert classify_paths(["docs/architecture/decision.md"]) == Scope(False, False)


def test_windows_paths_are_normalized() -> None:
    assert classify_paths([r"src\workers\nport_lookthrough.py"]) == Scope(True, False)


def test_changed_paths_rejects_an_all_zero_revision() -> None:
    with pytest.raises(ValueError, match="invalid Git revision"):
        changed_paths("0" * 40, "a" * 40, root=ROOT)


def test_cli_writes_both_github_outputs(tmp_path: Path) -> None:
    base = subprocess.run(
        ["git", "rev-parse", "HEAD^"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    output = tmp_path / "github-output.txt"

    assert main(["--base", base, "--head", head, "--github-output", str(output)]) == 0

    lines = output.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert lines[0].startswith("nport_changed=")
    assert lines[1].startswith("quant_changed=")
    assert {line.split("=", 1)[1] for line in lines} <= {"true", "false"}
