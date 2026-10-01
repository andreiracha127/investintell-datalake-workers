"""--apply preconditions: refuse BEFORE install_schema / materialize on any mismatch.

Fake connections only (the worker suite's harness); no DSN, no database.
"""
from __future__ import annotations

import json
import os

import pytest
from test_bond_market_implied_rating_worker import (
    PARENT,
    POINTER,
    REVISION,
    _matching_build,
    _patch_worker,
    _snapshot,
)

from scripts import backfill_bond_market_implied_rating as cli
from src.workers import bond_market_implied_rating as worker

FINGERPRINT = worker.policy.snapshot_fingerprint(_snapshot())
BUILD_DIGEST = "b" * 64  # what the harness' stubbed _build_payload reports


def _run(monkeypatch, **fields):
    conn, events = _patch_worker(monkeypatch, build={**_matching_build(), "code_revision": "old"})
    result = worker.run("postgresql://example", expectations=worker.ApplyExpectations(**fields))
    return result, events, conn


def _assert_refused(result, field, *, expected, actual):
    assert result["state"] == "precondition_failed"
    assert result["reason"] == "implied_rating_precondition_failed"
    assert result["aborted"] is True
    assert result["input_reasons"] == [f"expected_{field}_mismatch"]
    assert result["expectation"] == field
    assert result["expected"] == expected
    assert result["actual"] == actual


def test_pointer_mismatch_refuses_before_reading_ddl_or_writing(monkeypatch):
    result, events, conn = _run(monkeypatch, current_pointer="someone-else")
    _assert_refused(result, "current_pointer", expected="someone-else", actual=POINTER)
    assert events == []
    assert conn.statements == []


def test_panel_publication_mismatch_refuses_before_reading_ddl_or_writing(monkeypatch):
    result, events, _ = _run(monkeypatch, panel_publication_id="panel-other")
    _assert_refused(result, "panel_publication_id", expected="panel-other", actual=PARENT["publication_id"])
    assert events == []


def test_input_fingerprint_mismatch_refuses_after_the_read_but_before_ddl(monkeypatch):
    result, events, _ = _run(monkeypatch, input_fingerprint="0" * 64)
    _assert_refused(result, "input_fingerprint", expected="0" * 64, actual=FINGERPRINT)
    assert events == ["read_snapshot", "commit"]
    assert "install_schema" not in events


def test_rows_digest_mismatch_refuses_after_the_build_but_before_materialize(monkeypatch):
    result, events, _ = _run(monkeypatch, rows_digest="0" * 64)
    _assert_refused(result, "rows_digest", expected="0" * 64, actual=BUILD_DIGEST)
    assert events == ["read_snapshot", "commit", "install_schema", "commit", "build_payload"]
    assert not any(isinstance(event, tuple) and event[0] == "materialize" for event in events)


def test_all_expectations_matching_publishes_normally(monkeypatch):
    result, events, _ = _run(
        monkeypatch, current_pointer=POINTER, panel_publication_id=PARENT["publication_id"],
        input_fingerprint=FINGERPRINT, rows_digest=BUILD_DIGEST,
    )
    assert result["state"] == "published"
    assert result["code_revision"] == REVISION
    assert events[-1] == ("materialize", POINTER)


def test_no_expectations_changes_nothing(monkeypatch):
    _, events = _patch_worker(monkeypatch, build={**_matching_build(), "code_revision": "old"})
    result = worker.run("postgresql://example")
    assert result["state"] == "published"
    assert events == [
        "read_snapshot", "commit", "install_schema", "commit", "build_payload", ("materialize", POINTER),
    ]


def test_current_short_circuit_cannot_satisfy_a_digest_expectation(monkeypatch):
    _, events = _patch_worker(monkeypatch, build=_matching_build())
    result = worker.run(
        "postgresql://example", expectations=worker.ApplyExpectations(rows_digest=BUILD_DIGEST)
    )
    assert result["state"] == "precondition_failed"
    assert result["input_reasons"] == ["expected_rows_digest_unverifiable"]
    assert events == []
    # A pointer/panel pin alone is still honoured by the short-circuit.
    _, events = _patch_worker(monkeypatch, build=_matching_build())
    result = worker.run(
        "postgresql://example", expectations=worker.ApplyExpectations(current_pointer=POINTER)
    )
    assert result["state"] == "current"
    assert events == []


def test_panel_convergence_cannot_satisfy_a_fingerprint_expectation(monkeypatch):
    _, events = _patch_worker(monkeypatch, build={
        **_matching_build(), "panel_publication_id": "panel-previous", "input_fingerprint": FINGERPRINT,
    })
    result = worker.run(
        "postgresql://example",
        expectations=worker.ApplyExpectations(input_fingerprint=FINGERPRINT, rows_digest=BUILD_DIGEST),
    )
    assert result["state"] == "precondition_failed"
    assert result["input_reasons"] == ["expected_rows_digest_unverifiable"]
    assert events == ["read_snapshot", "commit"]


# --------------------------------------------------------------------------- #
# CLI wiring
# --------------------------------------------------------------------------- #
def test_apply_passes_the_expectations_to_the_worker(monkeypatch, capsys):
    seen = {}

    def fake_run(dsn, *, expectations=None):
        seen["expectations"] = expectations
        seen["force"] = os.environ.get(cli.FORCE_ENV)
        return {"state": "published", "aborted": False}

    monkeypatch.setattr(cli.worker, "run", fake_run)
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    monkeypatch.delenv(cli.FORCE_ENV, raising=False)
    assert cli.main([
        "--apply", "--expect-current-pointer", "p1", "--expect-panel-publication", "panel",
        "--expect-input-fingerprint", "f" * 64, "--expect-rows-digest", "d" * 64,
    ]) == 0
    assert seen["force"] == "1"
    assert seen["expectations"] == worker.ApplyExpectations(
        input_fingerprint="f" * 64, rows_digest="d" * 64,
        panel_publication_id="panel", current_pointer="p1",
    )
    assert json.loads(capsys.readouterr().out)["state"] == "published"


def test_apply_without_expectations_calls_the_worker_as_before(monkeypatch):
    calls = []
    monkeypatch.setattr(cli.worker, "run", lambda dsn: calls.append(dsn) or {"state": "published"})
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    assert cli.main(["--apply"]) == 0
    assert calls == ["postgresql://apply"]


def test_precondition_failure_exits_non_zero(monkeypatch, capsys):
    monkeypatch.setattr(cli.worker, "run", lambda dsn, **kw: {
        "state": "precondition_failed", "aborted": True,
        "input_reasons": ["expected_rows_digest_mismatch"],
    })
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    assert cli.main(["--apply", "--expect-rows-digest", "d" * 64]) == 1
    assert cli.exit_code({"state": "precondition_failed", "aborted": False}) == 1


def test_expectations_are_refused_on_a_dry_run(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setattr(cli.worker, "plan", lambda dsn: pytest.fail("must not plan"))
    with pytest.raises(SystemExit):
        cli.main(["--expect-rows-digest", "d" * 64])
    with pytest.raises(SystemExit):
        cli.main(["--dry-run", "--expect-current-pointer", "p"])


def test_determinism_check_receives_all_four_expectations(monkeypatch, capsys):
    seen = {}

    def determinism_check(dsn, **kwargs):
        seen.update(kwargs)
        return 0, {"verdict": "deterministic"}

    monkeypatch.setattr(cli.replay, "determinism_check", determinism_check)
    monkeypatch.setenv("DATABASE_URL", "postgresql://check")
    assert cli.main([
        "--determinism-check", "--expect-current-pointer", "p1", "--expect-panel-publication",
        "panel", "--expect-input-fingerprint", "f" * 64, "--expect-rows-digest", "d" * 64,
    ]) == 0
    assert seen["expect_current_pointer"] == "p1"
    assert seen["expect_panel_publication"] == "panel"
    assert seen["expect_input_fingerprint"] == "f" * 64
    assert seen["expect_rows_digest"] == "d" * 64
