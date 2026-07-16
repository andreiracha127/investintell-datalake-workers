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


@pytest.mark.parametrize(
    "path",
    [
        "src/workers/_openfigi.py",
        "src/workers/_yahoo_sector.py",
        "scripts/backfill_nport_holding_attributes.py",
        "tests/test_openfigi.py",
        "tests/test_yahoo_sector.py",
        "tests/test_backfill_nport_holding_attributes.py",
    ],
)
def test_nport_enrichment_helpers_select_only_nport(path: str) -> None:
    assert classify_paths([path]) == Scope(nport_changed=True, quant_changed=False)


def test_stage_a_compute_path_selects_quant_only() -> None:
    assert classify_paths(["src/quadrant_score.py"]) == Scope(
        nport_changed=False,
        quant_changed=True,
    )


@pytest.mark.parametrize(
    "path",
    [
        "docker/quant-engine/Dockerfile",
        "docker/quant-engine/entrypoint.sh",
    ],
)
def test_quant_docker_context_selects_quant_only(path: str) -> None:
    assert classify_paths([path]) == Scope(nport_changed=False, quant_changed=True)


def test_railway_requirements_select_nport_only() -> None:
    assert classify_paths(["requirements.txt"]) == Scope(
        nport_changed=True,
        quant_changed=False,
    )


@pytest.mark.parametrize(
    "path",
    [
        "requirements.quant-engine.in",
        "contracts/quant-engine/v1/CHANGELOG.md",
    ],
)
def test_quant_inputs_and_contract_bundle_docs_select_quant_only(path: str) -> None:
    assert classify_paths([path]) == Scope(
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


def test_changed_paths_includes_deleted_files(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "ci-scope@example.invalid"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "CI Scope Test"],
        cwd=tmp_path,
        check=True,
    )
    governed = tmp_path / "src" / "quadrant_score.py"
    governed.parent.mkdir(parents=True)
    governed.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "add governed file"], cwd=tmp_path, check=True)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    governed.unlink()
    subprocess.run(["git", "add", "-u"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "delete governed file"], cwd=tmp_path, check=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    assert changed_paths(base, head, root=tmp_path) == ["src/quadrant_score.py"]


def test_changed_paths_includes_both_sides_of_a_rename(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "ci-scope@example.invalid"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "CI Scope Test"],
        cwd=tmp_path,
        check=True,
    )
    governed = tmp_path / "src" / "quadrant_score.py"
    governed.parent.mkdir(parents=True)
    governed.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "add governed file"], cwd=tmp_path, check=True)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    destination = tmp_path / "docs" / "quadrant_score.py"
    destination.parent.mkdir(parents=True)
    subprocess.run(
        ["git", "mv", "src/quadrant_score.py", "docs/quadrant_score.py"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(["git", "commit", "-qm", "move governed file"], cwd=tmp_path, check=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    assert changed_paths(base, head, root=tmp_path) == [
        "src/quadrant_score.py",
        "docs/quadrant_score.py",
    ]


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
