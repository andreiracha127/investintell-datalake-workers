"""Tests for the market-implied rating backfill CLI (dry-run / apply semantics)."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import backfill_bond_market_implied_rating as cli  # noqa: E402

DIGEST = "d" * 64
FINGERPRINT = "f" * 64
#: The minimum a digest-bound --apply accepts (both values from the accepted receipt).
APPLY_BOUND = ["--apply", "--expect-rows-digest", DIGEST, "--expect-input-fingerprint", FINGERPRINT]


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

    def fake_run(dsn, *, expectations):
        seen["env"] = os.environ.get(cli.FORCE_ENV)
        seen["dsn"] = dsn
        seen["expectations"] = expectations
        return {"state": "published", "row_count": 9, "aborted": False}

    monkeypatch.setattr(cli.worker, "run", fake_run)
    monkeypatch.setattr(cli.worker, "plan", lambda dsn: pytest.fail("apply must publish"))
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    monkeypatch.delenv(cli.FORCE_ENV, raising=False)

    assert cli.main(APPLY_BOUND) == 0
    assert seen == {
        "env": "1", "dsn": "postgresql://apply",
        "expectations": cli.worker.ApplyExpectations(rows_digest=DIGEST, input_fingerprint=FINGERPRINT),
    }
    assert cli.FORCE_ENV not in os.environ
    assert json.loads(capsys.readouterr().out)["state"] == "published"


def test_apply_restores_a_pre_existing_force_value(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    monkeypatch.setenv(cli.FORCE_ENV, "operator-value")
    monkeypatch.setattr(cli.worker, "run", lambda dsn, **kw: {"state": "published"})
    assert cli.main(APPLY_BOUND) == 0
    assert os.environ[cli.FORCE_ENV] == "operator-value"


# --------------------------------------------------------------------------- #
# --apply is digest-bound: refused before the DSN is resolved without BOTH the
# rows digest and the input fingerprint (64 lowercase hex each).
# --------------------------------------------------------------------------- #
def _arm_refusal_probes(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://must-not-be-resolved")
    monkeypatch.setattr(cli, "resolve_dsn", lambda *a, **k: pytest.fail("DSN resolved before the refusal"))
    monkeypatch.setattr(cli.worker, "run", lambda *a, **k: pytest.fail("worker ran despite the refusal"))
    monkeypatch.setattr(cli.worker, "plan", lambda *a, **k: pytest.fail("plan ran despite the refusal"))
    monkeypatch.delenv(cli.FORCE_ENV, raising=False)


@pytest.mark.parametrize(
    ("argv", "named"),
    [
        (["--apply"], ["--expect-rows-digest", "--expect-input-fingerprint"]),
        (["--apply", "--expect-rows-digest", DIGEST], ["--expect-input-fingerprint"]),
        (["--apply", "--expect-input-fingerprint", FINGERPRINT], ["--expect-rows-digest"]),
        (["--apply", "--expect-current-pointer", "p1", "--expect-panel-publication", "panel"],
         ["--expect-rows-digest", "--expect-input-fingerprint"]),
    ],
)
def test_apply_without_both_digest_expectations_is_refused_before_dsn(monkeypatch, capsys, argv, named):
    _arm_refusal_probes(monkeypatch)
    with pytest.raises(SystemExit) as excinfo:
        cli.main(argv)
    assert excinfo.value.code != 0
    err = capsys.readouterr().err
    assert "digest-bound" in err
    for flag in named:
        assert flag in err
    assert cli.FORCE_ENV not in os.environ


@pytest.mark.parametrize(
    ("flag", "bad"),
    [
        ("--expect-rows-digest", "D" * 64),       # uppercase hex
        ("--expect-rows-digest", "d" * 63),       # too short
        ("--expect-rows-digest", "d" * 65),       # too long
        ("--expect-rows-digest", "g" * 64),       # not hex
        ("--expect-input-fingerprint", "F" * 64),
        ("--expect-input-fingerprint", "sha256:" + "f" * 57),
        ("--expect-input-fingerprint", ""),
    ],
)
def test_apply_refuses_a_malformed_digest_or_fingerprint_before_dsn(monkeypatch, capsys, flag, bad):
    _arm_refusal_probes(monkeypatch)
    argv = ["--apply", "--expect-rows-digest", DIGEST, "--expect-input-fingerprint", FINGERPRINT]
    argv[argv.index(flag) + 1] = bad
    with pytest.raises(SystemExit) as excinfo:
        cli.main(argv)
    assert excinfo.value.code != 0
    err = capsys.readouterr().err
    assert flag in err
    assert "64 lowercase hex" in err


def test_determinism_check_also_refuses_a_malformed_digest(monkeypatch):
    _arm_refusal_probes(monkeypatch)
    monkeypatch.setattr(cli.replay, "determinism_check", lambda *a, **k: pytest.fail("ran"))
    with pytest.raises(SystemExit):
        cli.main(["--determinism-check", "--expect-rows-digest", "D" * 64])


def test_determinism_check_does_not_require_the_digest_expectations(monkeypatch, capsys):
    """The optional-expectations contract is unchanged for the read-only check."""
    monkeypatch.setattr(cli.replay, "determinism_check", lambda dsn, **kwargs: (0, {"verdict": "deterministic"}))
    monkeypatch.setenv("DATABASE_URL", "postgresql://check")
    assert cli.main(["--determinism-check"]) == 0
    assert json.loads(capsys.readouterr().out) == {"verdict": "deterministic"}


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
        "expect_input_fingerprint": None, "expect_rows_digest": None,
        "expect_panel_publication": None, "expect_current_pointer": None,
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
