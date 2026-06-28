from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REMOTE_RUNNER = ROOT / "scripts" / "ci" / "run_remote_railway_ci.ps1"
PRE_PUSH_HOOK = ROOT / ".githooks" / "pre-push"
REMOTE_CI_DOC = ROOT / "docs" / "architecture" / "remote-ssh-ci.md"
RAILWAY_CI_DOCKERFILE = ROOT / "docker" / "railway-ci" / "Dockerfile"


def test_remote_ci_runner_targets_legion_and_archives_current_commit() -> None:
    text = REMOTE_RUNNER.read_text(encoding="utf-8")

    assert 'RemoteHost = "andrei@100.96.0.3"' in text
    assert 'RemoteRoot = "C:\\Users\\Andrei\\ci-runners"' in text
    assert "Invoke-Git archive" in text
    assert "scp" in text
    assert "docker/railway-ci/Dockerfile" in text
    assert "Out-Null" in text
    assert "REMOTE_CI_LOG_TAIL" in text
    assert "CLIXML" in text
    assert "-OutputFormat Text" in text
    assert "-Command -" in text
    assert "-split" in text
    assert "REMOTE_CI_STATUS=PASS" in text


def test_remote_ci_runner_handles_docker_desktop_ssh_sessions() -> None:
    text = REMOTE_RUNNER.read_text(encoding="utf-8")

    assert 'BuilderMode = "Auto"' in text
    assert "DOCKER_BUILDKIT" in text
    assert "REMOTE_CI_BUILDKIT_CREDENTIAL_HELPER_FALLBACK=true" in text
    assert "error getting credentials|specified logon session|credsStore" in text


def test_railway_ci_dockerfile_stays_legacy_builder_compatible() -> None:
    # The credential-helper fallback drops to the legacy builder
    # (DOCKER_BUILDKIT=0), which cannot parse heredoc RUN blocks (a BuildKit-only
    # syntax). The Dockerfile must therefore avoid heredocs and call extracted
    # scripts instead, so the fallback can actually build the image.
    text = RAILWAY_CI_DOCKERFILE.read_text(encoding="utf-8")

    assert "<<'PY'" not in text
    assert "<<PY" not in text
    assert "RUN python docker/railway-ci/verify_input_pack.py" in text
    assert "RUN python docker/railway-ci/verify_calibration_artifacts.py" in text


def test_pre_push_hook_invokes_remote_ci_runner() -> None:
    text = PRE_PUSH_HOOK.read_text(encoding="utf-8")

    assert "INVESTINTELL_SKIP_REMOTE_CI" in text
    assert "scripts/ci/run_remote_railway_ci.ps1" in text
    assert "-NonInteractive" in text
    assert "-OutputFormat Text" in text
    assert "powershell.exe" in text


def test_remote_ci_doc_records_install_and_evidence_commands() -> None:
    text = REMOTE_CI_DOC.read_text(encoding="utf-8")

    assert "git config core.hooksPath .githooks" in text
    assert "-NonInteractive -OutputFormat Text" in text
    assert "REMOTE_CI_SHA=<commit>" in text
    assert "REMOTE_CI_STATUS=PASS" in text
