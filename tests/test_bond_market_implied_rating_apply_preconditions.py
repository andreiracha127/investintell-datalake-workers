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


@pytest.mark.parametrize(
    "argv",
    [
        ["--apply"],
        ["--apply", "--expect-rows-digest", "d" * 64],
        ["--apply", "--expect-input-fingerprint", "f" * 64],
        ["--apply", "--expect-current-pointer", "p1", "--expect-panel-publication", "panel"],
    ],
)
def test_apply_without_the_digest_expectations_is_refused_before_the_worker_or_dsn(monkeypatch, capsys, argv):
    """There is no unrestricted --apply: a manual publication must say what it publishes."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    monkeypatch.setattr(cli, "resolve_dsn", lambda *a, **k: pytest.fail("DSN resolved"))
    monkeypatch.setattr(cli.worker, "run", lambda *a, **k: pytest.fail("worker ran"))
    monkeypatch.delenv(cli.FORCE_ENV, raising=False)
    with pytest.raises(SystemExit) as excinfo:
        cli.main(argv)
    assert excinfo.value.code != 0
    assert "digest-bound" in capsys.readouterr().err
    assert cli.FORCE_ENV not in os.environ


def test_apply_with_both_digest_expectations_always_passes_them_to_the_worker(monkeypatch):
    calls = []

    def fake_run(dsn, *, expectations):
        calls.append((dsn, expectations))
        return {"state": "published"}

    monkeypatch.setattr(cli.worker, "run", fake_run)
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    assert cli.main(["--apply", "--expect-rows-digest", "d" * 64, "--expect-input-fingerprint", "f" * 64]) == 0
    assert calls == [("postgresql://apply", worker.ApplyExpectations(rows_digest="d" * 64, input_fingerprint="f" * 64))]


def test_precondition_failure_exits_non_zero(monkeypatch, capsys):
    monkeypatch.setattr(cli.worker, "run", lambda dsn, **kw: {
        "state": "precondition_failed", "aborted": True,
        "input_reasons": ["expected_rows_digest_mismatch"],
    })
    monkeypatch.setenv("DATABASE_URL", "postgresql://apply")
    assert cli.main([
        "--apply", "--expect-rows-digest", "d" * 64, "--expect-input-fingerprint", "f" * 64,
    ]) == 1
    assert cli.exit_code({"state": "precondition_failed", "aborted": False}) == 1


# --------------------------------------------------------------------------- #
# Every post-manifest refusal carries the collected build manifest
# --------------------------------------------------------------------------- #
MANIFEST = {"schema": "bond_build_manifest/1", "packages": {"numpy": "test-stack"}}


def test_rows_digest_mismatch_refusal_carries_the_build_manifest(monkeypatch):
    monkeypatch.setattr(worker, "collect_build_manifest", lambda: MANIFEST)
    result, _, _ = _run(monkeypatch, rows_digest="0" * 64)
    _assert_refused(result, "rows_digest", expected="0" * 64, actual=BUILD_DIGEST)
    assert result["build_manifest"] == MANIFEST


@pytest.mark.parametrize("field", ["current_pointer", "panel_publication_id", "input_fingerprint"])
def test_every_precondition_refusal_carries_the_build_manifest(monkeypatch, field):
    monkeypatch.setattr(worker, "collect_build_manifest", lambda: MANIFEST)
    result, _, _ = _run(monkeypatch, **{field: "0" * 64})
    assert result["state"] == "precondition_failed"
    assert result["build_manifest"] == MANIFEST


def test_gate_refusals_and_publications_carry_the_build_manifest(monkeypatch):
    monkeypatch.setattr(worker, "collect_build_manifest", lambda: MANIFEST)
    _patch_worker(monkeypatch, build=_matching_build(), mirror=False)
    refused = worker.run("postgresql://example")
    assert refused["state"] == "gate_failed"
    assert refused["build_manifest"] == MANIFEST
    _patch_worker(monkeypatch, build={**_matching_build(), "code_revision": "old"})
    published = worker.run("postgresql://example")
    assert published["state"] == "published"
    assert published["build_manifest"] == MANIFEST
    _patch_worker(monkeypatch, build=_matching_build())
    current = worker.run("postgresql://example")
    assert current["state"] == "current"
    assert current["build_manifest"] == MANIFEST


# --------------------------------------------------------------------------- #
# Service-variable expectations (the only channel through src.run_worker)
# --------------------------------------------------------------------------- #
ENV = worker.EXPECTATION_ENV_VARS
PANEL_UUID = "11111111-1111-4111-8111-111111111111"
POINTER_UUID = "22222222-2222-4222-8222-222222222222"


def _clear_env(monkeypatch):
    for var in ENV.values():
        monkeypatch.delenv(var, raising=False)
    monkeypatch.delenv(worker.FORCE_ENV, raising=False)


def test_expectations_from_env_parses_and_normalises_all_four(monkeypatch):
    _clear_env(monkeypatch)
    assert worker.expectations_from_env() == worker.ApplyExpectations()
    monkeypatch.setenv(ENV["rows_digest"], " " + "d" * 64 + " ")
    monkeypatch.setenv(ENV["input_fingerprint"], "f" * 64)
    monkeypatch.setenv(ENV["panel_publication_id"], PANEL_UUID.upper())
    monkeypatch.setenv(ENV["current_pointer"], POINTER_UUID)
    assert worker.expectations_from_env() == worker.ApplyExpectations(
        rows_digest="d" * 64, input_fingerprint="f" * 64,
        panel_publication_id=PANEL_UUID, current_pointer=POINTER_UUID,
    )


@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_env_expectations_are_unset(monkeypatch, blank):
    _clear_env(monkeypatch)
    for var in ENV.values():
        monkeypatch.setenv(var, blank)
    assert worker.expectations_from_env() == worker.ApplyExpectations()


@pytest.mark.parametrize(
    ("field", "bad", "reason"),
    [
        ("rows_digest", "D" * 64, "not_sha256_hex"),
        ("rows_digest", "d" * 63, "not_sha256_hex"),
        ("input_fingerprint", "0x" + "f" * 62, "not_sha256_hex"),
        ("panel_publication_id", "panel-current", "not_uuid"),
        ("current_pointer", "null", "not_uuid"),
    ],
)
def test_malformed_env_expectations_refuse_before_connecting(monkeypatch, field, bad, reason):
    _clear_env(monkeypatch)
    monkeypatch.setenv(ENV[field], bad)
    with pytest.raises(worker.ExpectationError) as excinfo:
        worker.expectations_from_env()
    assert (excinfo.value.field, excinfo.value.reason, excinfo.value.source) == (field, reason, ENV[field])
    # Through run(): a typed refusal, no connection, no DDL, manifest attached.
    monkeypatch.setattr(worker, "connect", lambda _dsn: pytest.fail("connected"))
    monkeypatch.setattr(worker, "resolve_dsn", lambda _dsn: pytest.fail("DSN resolved"))
    monkeypatch.setattr(worker, "_code_revision", lambda: REVISION)
    monkeypatch.setattr(worker, "collect_build_manifest", lambda: MANIFEST)
    result = worker.run("postgresql://example")
    assert result["state"] == "precondition_failed"
    assert result["aborted"] is True
    assert result["input_reasons"] == [f"expected_{field}_malformed"]
    assert result["expectation"] == field
    assert result["env_var"] == ENV[field]
    assert result["actual"] == bad
    assert result["build_manifest"] == MANIFEST


def test_env_expectations_are_honoured_on_a_normal_run(monkeypatch):
    """Not forced: env expectations are optional but, when set, compared like CLI ones."""
    _, events = _patch_worker(monkeypatch, build={**_matching_build(), "code_revision": "old"})
    _clear_env(monkeypatch)
    monkeypatch.setenv(ENV["rows_digest"], "0" * 64)
    result = worker.run("postgresql://example")
    _assert_refused(result, "rows_digest", expected="0" * 64, actual=BUILD_DIGEST)
    assert not any(isinstance(event, tuple) and event[0] == "materialize" for event in events)
    _, events = _patch_worker(monkeypatch, build={**_matching_build(), "code_revision": "old"})
    monkeypatch.setenv(ENV["rows_digest"], BUILD_DIGEST)
    monkeypatch.setenv(ENV["input_fingerprint"], FINGERPRINT)
    result = worker.run("postgresql://example")
    assert result["state"] == "published"
    assert events[-1] == ("materialize", POINTER)


def test_env_fingerprint_mismatch_refuses_before_ddl(monkeypatch):
    _, events = _patch_worker(monkeypatch, build={**_matching_build(), "code_revision": "old"})
    _clear_env(monkeypatch)
    monkeypatch.setenv(ENV["input_fingerprint"], "0" * 64)
    result = worker.run("postgresql://example")
    _assert_refused(result, "input_fingerprint", expected="0" * 64, actual=FINGERPRINT)
    assert "install_schema" not in events


def test_explicit_and_env_expectations_merge_and_a_conflict_refuses(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(ENV["input_fingerprint"], FINGERPRINT)
    merged = worker.merge_expectations(
        worker.ApplyExpectations(rows_digest=BUILD_DIGEST), worker.expectations_from_env()
    )
    assert merged == worker.ApplyExpectations(rows_digest=BUILD_DIGEST, input_fingerprint=FINGERPRINT)
    monkeypatch.setenv(ENV["rows_digest"], "0" * 64)
    with pytest.raises(worker.ExpectationError) as excinfo:
        worker.merge_expectations(worker.ApplyExpectations(rows_digest=BUILD_DIGEST), worker.expectations_from_env())
    assert (excinfo.value.field, excinfo.value.reason) == ("rows_digest", "conflict")
    _, events = _patch_worker(monkeypatch, build={**_matching_build(), "code_revision": "old"})
    monkeypatch.setenv(ENV["rows_digest"], "0" * 64)
    result = worker.run(
        "postgresql://example", expectations=worker.ApplyExpectations(rows_digest=BUILD_DIGEST)
    )
    assert result["state"] == "precondition_failed"
    assert result["input_reasons"] == ["expected_rows_digest_conflict"]
    assert events == []


# --------------------------------------------------------------------------- #
# Forced republication is digest-bound in the worker itself
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("env", "missing"),
    [
        ({}, ["rows_digest", "input_fingerprint"]),
        ({"rows_digest": BUILD_DIGEST}, ["input_fingerprint"]),
        ({"input_fingerprint": FINGERPRINT}, ["rows_digest"]),
        ({"panel_publication_id": PANEL_UUID, "current_pointer": POINTER_UUID},
         ["rows_digest", "input_fingerprint"]),
    ],
)
def test_forced_republish_without_digest_expectations_refuses_before_connecting(monkeypatch, env, missing):
    _, events = _patch_worker(monkeypatch, build=_matching_build())
    _clear_env(monkeypatch)
    monkeypatch.setenv(worker.FORCE_ENV, "1")
    for field, value in env.items():
        monkeypatch.setenv(ENV[field], value)
    monkeypatch.setattr(worker, "connect", lambda _dsn: pytest.fail("connected"))
    monkeypatch.setattr(worker, "collect_build_manifest", lambda: MANIFEST)
    result = worker.run("postgresql://example")
    assert result["state"] == "precondition_failed"
    assert result["reason"] == "implied_rating_precondition_failed"
    assert result["aborted"] is True
    assert result["input_reasons"] == [f"expected_{field}_required" for field in missing]
    assert result["missing_expectations"] == missing
    assert result["env_vars"] == [ENV[field] for field in missing]
    assert result["forced_republish"] is True
    assert result["build_manifest"] == MANIFEST
    assert events == []


def test_forced_republish_with_env_digest_expectations_publishes(monkeypatch):
    _, events = _patch_worker(monkeypatch, build=_matching_build())
    _clear_env(monkeypatch)
    monkeypatch.setenv(worker.FORCE_ENV, "1")
    monkeypatch.setenv(ENV["rows_digest"], BUILD_DIGEST)
    monkeypatch.setenv(ENV["input_fingerprint"], FINGERPRINT)
    result = worker.run("postgresql://example")
    assert result["state"] == "published"
    assert result["rebuild_reasons"] == []
    assert events[-1] == ("materialize", POINTER)


def test_forced_republish_with_explicit_digest_expectations_publishes(monkeypatch):
    """The backfill CLI path: flags, not variables."""
    _, events = _patch_worker(monkeypatch, build=_matching_build())
    _clear_env(monkeypatch)
    monkeypatch.setenv(worker.FORCE_ENV, "true")
    result = worker.run(
        "postgresql://example",
        expectations=worker.ApplyExpectations(rows_digest=BUILD_DIGEST, input_fingerprint=FINGERPRINT),
    )
    assert result["state"] == "published"
    assert events[-1] == ("materialize", POINTER)


def test_stage_seven_call_without_any_variable_is_unchanged(monkeypatch):
    """bond_live_daily Stage 7 calls run(dsn) with no expectations and no force flag."""
    _, events = _patch_worker(monkeypatch, build={**_matching_build(), "code_revision": "old"})
    _clear_env(monkeypatch)
    from src.workers import bond_live_daily

    result = bond_live_daily._publish_implied_rating("postgresql://example")
    assert result["state"] == "published"
    assert events == [
        "read_snapshot", "commit", "install_schema", "commit", "build_payload", ("materialize", POINTER),
    ]
    _, events = _patch_worker(monkeypatch, build=_matching_build())
    assert bond_live_daily._publish_implied_rating("postgresql://example")["state"] == "current"
    assert events == []


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
