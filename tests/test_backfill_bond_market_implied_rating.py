"""Tests for the market-implied rating backfill CLI (dry-run / apply semantics)."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import backfill_bond_market_implied_rating as cli  # noqa: E402


def test_dry_run_reports_the_plan_without_touching_the_worker(monkeypatch, capsys) -> None:
    calls: dict[str, object] = {}
    plan_result = {"state": "planned", "row_count": 12, "d_confirmed_count": 0}
    monkeypatch.setattr(cli.worker, "plan", lambda dsn: (calls.setdefault("plan", dsn), plan_result)[1])
    monkeypatch.setattr(cli.worker, "run", lambda dsn: pytest.fail("dry-run must not publish"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://dry-run")

    assert cli.main([]) == 0
    assert calls["plan"] == "postgresql://dry-run"
    assert json.loads(capsys.readouterr().out) == plan_result


def test_apply_sets_the_force_flag_for_exactly_one_call(monkeypatch, capsys) -> None:
    seen: dict[str, object] = {}

    def fake_run(dsn):
        seen["env"] = os.environ.get(cli.FORCE_ENV)
        seen["dsn"] = dsn
        return {"state": "published", "row_count": 9, "aborted": False}

    monkeypatch.setattr(cli.worker, "run", fake_run)
    monkeypatch.setattr(cli.worker, "plan", lambda dsn: pytest.fail("apply must publish"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    monkeypatch.delenv(cli.FORCE_ENV, raising=False)

    assert cli.main(["--apply"]) == 0
    assert seen == {"env": "1", "dsn": "postgresql://apply"}
    assert cli.FORCE_ENV not in os.environ
    assert json.loads(capsys.readouterr().out)["state"] == "published"


def test_apply_restores_a_pre_existing_force_value(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    monkeypatch.setenv(cli.FORCE_ENV, "operator-value")
    monkeypatch.setattr(cli.worker, "run", lambda dsn: {"state": "published"})
    assert cli.main(["--apply"]) == 0
    assert os.environ[cli.FORCE_ENV] == "operator-value"


@pytest.mark.parametrize(
    "conflicting",
    (
        ["--apply", "--dry-run"],
        ["--apply", "--determinism-check"],
        ["--dry-run", "--determinism-check"],
    ),
)
def test_modes_are_mutually_exclusive(conflicting, monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    with pytest.raises(SystemExit):
        cli.main(conflicting)


@pytest.mark.parametrize(
    "result",
    (
        {"state": "gate_failed", "aborted": True},
        {"state": "published", "aborted": False, "anchor_drift": True},
        {"state": "publish_failed", "aborted": False},
    ),
)
def test_failure_states_exit_non_zero(result) -> None:
    assert cli.exit_code(result) == 1


@pytest.mark.parametrize(
    "result",
    (
        {"state": "planned", "aborted": False},
        {"state": "published", "aborted": False},
        {"state": "published_no_defaults", "aborted": False},
        {"state": "current", "aborted": False},
    ),
)
def test_success_states_exit_zero(result) -> None:
    assert cli.exit_code(result) == 0


def test_forced_republish_context_manager_is_exception_safe(monkeypatch) -> None:
    monkeypatch.delenv(cli.FORCE_ENV, raising=False)
    with pytest.raises(RuntimeError):
        with cli.forced_republish():
            assert os.environ[cli.FORCE_ENV] == "1"
            raise RuntimeError("boom")
    assert cli.FORCE_ENV not in os.environ


def test_determinism_check_delegates_to_the_replay_and_never_touches_the_worker(
    monkeypatch, capsys, tmp_path
) -> None:
    seen: dict[str, object] = {}
    receipt = {"verdict": "deterministic", "rows_digest": "d" * 64, "child_pids": [11, 12]}

    def determinism_check(dsn, **kwargs):
        seen["dsn"] = dsn
        seen["kwargs"] = kwargs
        return 0, receipt

    monkeypatch.setattr(cli.replay, "determinism_check", determinism_check)
    monkeypatch.setattr(cli.worker, "run", lambda dsn: pytest.fail("determinism check must not publish"))
    monkeypatch.setattr(cli.worker, "plan", lambda dsn: pytest.fail("determinism check is not a plan"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://check")
    monkeypatch.delenv(cli.FORCE_ENV, raising=False)

    assert cli.main([
        "--determinism-check", "--receipt", str(tmp_path / "r.json"), "--work-dir", str(tmp_path),
        "--statement-timeout-seconds", "120", "--child-timeout-seconds", "60",
    ]) == 0
    assert seen["dsn"] == "postgresql://check"
    assert seen["kwargs"] == {
        "work_dir": tmp_path, "receipt_path": tmp_path / "r.json",
        "statement_timeout_s": 120, "child_timeout_s": 60,
    }
    assert cli.FORCE_ENV not in os.environ
    assert json.loads(capsys.readouterr().out) == receipt


@pytest.mark.parametrize("code", (1, 2))
def test_determinism_check_exit_code_is_the_replay_verdict(monkeypatch, capsys, code) -> None:
    monkeypatch.setattr(
        cli.replay, "determinism_check", lambda dsn, **kwargs: (code, {"verdict": "mismatch"})
    )
    monkeypatch.setenv("DATABASE_URL", "postgresql://check")
    assert cli.main(["--determinism-check"]) == code
    assert json.loads(capsys.readouterr().out) == {"verdict": "mismatch"}
