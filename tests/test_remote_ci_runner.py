from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GITHUB_ACTIONS_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PRE_PUSH_HOOK = ROOT / ".githooks" / "pre-push"
REMOTE_CI_DOC = ROOT / "docs" / "architecture" / "remote-ssh-ci.md"


def _workflow_text() -> str:
    return GITHUB_ACTIONS_WORKFLOW.read_text(encoding="utf-8")


def test_github_actions_workflow_runs_on_prs_and_pushes() -> None:
    text = _workflow_text()

    assert "pull_request:" in text
    assert "push:" in text
    assert "runs-on: ubuntu-latest" in text
    assert 'python-version: "3.13"' in text
    assert "PYTHONPATH: .:packages/investintell_quant_core/src:services/quant_engine/src" in text


def test_github_actions_workflow_runs_quant_engine_gate() -> None:
    text = _workflow_text()

    assert "python docker/railway-ci/verify_input_pack.py" in text
    assert "python scripts/contract_bundle.py verify" in text
    assert "tests/input_packs" in text
    assert "tests/quant_engine" in text
    assert "tests/test_controlled_shadow.py" in text
    assert "python -m compileall" in text
    assert "python docker/railway-ci/verify_calibration_artifacts.py" in text


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
