from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
GITHUB_ACTIONS_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PRE_PUSH_HOOK = ROOT / ".githooks" / "pre-push"
REMOTE_CI_DOC = ROOT / "docs" / "architecture" / "remote-ssh-ci.md"


def _workflow_text() -> str:
    return GITHUB_ACTIONS_WORKFLOW.read_text(encoding="utf-8")


def _workflow() -> dict[str, object]:
    return yaml.load(_workflow_text(), Loader=yaml.BaseLoader)


def _steps_by_name() -> tuple[list[str], dict[str, dict[str, str]]]:
    steps = _workflow()["jobs"]["quant-engine"]["steps"]
    names = [step["name"] for step in steps]
    return names, {step["name"]: step for step in steps}


def test_github_actions_workflow_runs_once_per_pr_and_on_main_pushes() -> None:
    workflow = _workflow()

    assert "pull_request" in workflow["on"]
    assert workflow["on"]["push"]["branches"] == ["main"]
    assert workflow["concurrency"]["cancel-in-progress"] == "true"
    assert "github.workflow" in workflow["concurrency"]["group"]
    assert "github.event.pull_request.number" in workflow["concurrency"]["group"]


def test_github_actions_workflow_preserves_required_job_identity() -> None:
    job = _workflow()["jobs"]["quant-engine"]

    assert job["name"] == "quant-engine"
    assert job["runs-on"] == "ubuntu-latest"
    assert job["env"]["PYTHONPATH"] == (
        ".:packages/investintell_quant_core/src:services/quant_engine/src"
    )
    names, steps = _steps_by_name()
    assert steps["Check out repository"]["with"]["fetch-depth"] == "0"
    assert steps["Set up Python"]["with"]["python-version"] == "3.13"
    assert steps["Detect changed paths"]["id"] == "changes"
    assert names.index("Verify Stage A binding") < names.index(
        "Run governance and quant-engine tests"
    )


def test_github_actions_workflow_runs_quant_engine_gate() -> None:
    _, steps = _steps_by_name()
    quant_steps = (
        "Verify Stage A binding",
        "Verify quant requirements lock",
        "Verify certified input pack",
        "Verify contract bundle",
        "Verify contract bundle v2",
        "Run governance and quant-engine tests",
        "Compile Python modules",
        "Verify calibration artifacts",
    )

    for name in quant_steps:
        assert "quant_changed == 'true'" in steps[name]["if"], name
    assert "test_reproducibility_record_pins_a_clean_16_run_reproduction" in steps[
        "Verify Stage A binding"
    ]["run"]
    assert "tests/input_packs" in steps["Run governance and quant-engine tests"]["run"]
    assert "tests/quant_engine" in steps["Run governance and quant-engine tests"]["run"]
    assert "tests/test_controlled_shadow.py" in steps[
        "Run governance and quant-engine tests"
    ]["run"]
    assert "scripts/ci/verify_quant_requirements.py" in steps[
        "Verify quant requirements lock"
    ]["run"]
    assert "tests/test_quant_requirements_sync.py" in steps[
        "Run governance and quant-engine tests"
    ]["run"]


def test_github_actions_workflow_runs_focused_nport_gate() -> None:
    _, steps = _steps_by_name()
    nport_steps = (
        "Validate Railway dependencies",
        "Run focused N-PORT tests",
        "Lint N-PORT modules",
        "Compile N-PORT modules",
    )

    for name in nport_steps:
        assert "nport_changed == 'true'" in steps[name]["if"], name
    railway_command = steps["Validate Railway dependencies"]["run"]
    assert "--dry-run" in railway_command
    assert "--ignore-installed" in railway_command
    assert "-r requirements.txt" in railway_command
    command = steps["Run focused N-PORT tests"]["run"]
    assert "tests/test_nport_lookthrough.py" in command
    assert "tests/test_nport_cusip_enrichment.py" in command
    assert "tests/test_openfigi.py" in command
    assert "tests/test_yahoo_sector.py" in command
    assert "tests/test_backfill_nport_holding_attributes.py" in command
    assert "tests/test_load_nport_fund_flows.py" in command
    assert "ruff==0.15.9" in steps["Install dependencies"]["run"]
    lint_command = steps["Lint N-PORT modules"]["run"]
    assert "ruff check" in lint_command
    assert "src/workers/_openfigi.py" in lint_command
    assert "src/workers/_yahoo_sector.py" in lint_command
    assert "scripts/backfill_nport_holding_attributes.py" in lint_command
    compile_command = steps["Compile N-PORT modules"]["run"]
    assert "python -m compileall" in compile_command
    assert "src/workers/_openfigi.py" in compile_command
    assert "src/workers/_yahoo_sector.py" in compile_command
    assert "scripts/backfill_nport_holding_attributes.py" in compile_command


def test_quant_suite_uses_all_runner_cpus_without_parallelizing_nport() -> None:
    _, steps = _steps_by_name()

    assert "pytest-xdist==3.8.0" in steps["Install dependencies"]["run"]
    quant_command = steps["Run governance and quant-engine tests"]["run"]
    assert "-p xdist.plugin" in quant_command
    assert "-n auto" in quant_command
    assert "--dist loadscope" in quant_command
    assert "--color=no" in quant_command
    assert "-n auto" not in steps["Run focused N-PORT tests"]["run"]


def test_github_actions_workflow_does_not_use_remote_docker_ci() -> None:
    text = _workflow_text()

    assert "docker build" not in text
    assert "docker/railway-ci/Dockerfile" not in text
    assert "run_remote_railway_ci.ps1" not in text
    assert "REMOTE_CI_STATUS" not in text


def test_pre_push_hook_no_longer_invokes_remote_docker_ci() -> None:
    text = PRE_PUSH_HOOK.read_text(encoding="utf-8")

    assert "GitHub Actions" in text
    assert "run_remote_railway_ci.ps1" not in text
    assert "INVESTINTELL_SKIP_REMOTE_CI" not in text
    assert "docker build" not in text
    assert "powershell.exe" not in text


def test_remote_ssh_ci_doc_records_retired_state() -> None:
    text = REMOTE_CI_DOC.read_text(encoding="utf-8")

    assert "Retired Remote SSH CI" in text
    assert "GitHub Actions" in text
    assert "REMOTE_CI_STATUS=PASS" not in text
