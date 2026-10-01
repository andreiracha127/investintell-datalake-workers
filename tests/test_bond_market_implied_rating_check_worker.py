"""WORKER=bond_market_implied_rating_check: the G1(a) determinism replay through src.run_worker.

The deployed bond-live-daily image is a cron container that exits between runs,
so the determinism check must be reachable through the platform entry point.
Fake connections; the replay children are REAL fresh interpreter processes on a
tiny synthetic export. No DSN, no database, no write.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
from test_bond_market_implied_rating_replay import (
    SECRET_DSN,
    _direct_build,
    _patch_capture,
    _snapshot,
)
from test_run_worker_calc_date import _run_lane

from src.bonds import implied_rating_replay as replay
from src.workers import bond_market_implied_rating as worker
from src.workers import bond_market_implied_rating_check as check

ENV = worker.EXPECTATION_ENV_VARS
PANEL_UUID = "11111111-1111-4111-8111-111111111111"


def _clear_env(monkeypatch):
    for var in ENV.values():
        monkeypatch.delenv(var, raising=False)
    monkeypatch.delenv(worker.FORCE_ENV, raising=False)


# --------------------------------------------------------------------------- #
# End to end: same replay as the CLI, receipt returned in the stats
# --------------------------------------------------------------------------- #
def test_check_worker_runs_the_replay_and_returns_the_full_receipt(monkeypatch):
    snapshot = _snapshot()
    direct = _direct_build(snapshot)
    conn, seen = _patch_capture(monkeypatch, snapshot=snapshot)
    _clear_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    stats = check.run(SECRET_DSN)
    assert stats["state"] == "deterministic"
    assert stats["aborted"] is False
    assert stats["reason"] is None
    assert stats["verdict"] == "deterministic"
    assert stats["exit_code"] == 0
    assert stats["mismatch_reasons"] == []
    assert stats["rows_digest"] == direct["rows_digest"]
    assert stats["input_fingerprint"] == direct["input_fingerprint"]
    assert stats["publication_id"] == direct["publication"].publication_id
    assert len(stats["child_pids"]) == 2
    # The FULL receipt rides in the stats: the Railway log line is the receipt.
    receipt = stats["receipt"]
    assert receipt["schema"] == replay.RECEIPT_SCHEMA
    assert receipt["verdict"] == "deterministic"
    assert [child["pid"] for child in receipt["children"]] == stats["child_pids"]
    assert all(child["build_manifest"]["schema"] == "bond_build_manifest/1" for child in receipt["children"])
    assert receipt["parent_build_manifest"]["schema"] == "bond_build_manifest/1"
    assert receipt["expectations"] == {
        "input_fingerprint": None, "rows_digest": None,
        "panel_publication_id": None, "current_pointer": None,
    }
    # Work files live in a fresh ABSOLUTE temp dir (ephemeral disk; nothing relies on it).
    work = Path(stats["work_dir"])
    assert work.is_absolute() and work.is_dir()
    assert work.name.startswith("bond-implied-rating-check-")
    assert work.parent == Path(tempfile.gettempdir()).resolve()
    assert Path(stats["receipt_path"]).parent == work
    assert Path(receipt["export"]["path"]).parent == work
    # What run_worker prints: one JSON document, serializable, secret-free.
    text = json.dumps({"worker": "bond_market_implied_rating_check", **stats}, default=str)
    assert SECRET_DSN not in text and "hunter2" not in text
    assert seen["dsn"] == SECRET_DSN
    assert conn.commits >= 2


def test_check_worker_uses_a_fresh_work_dir_per_run(monkeypatch):
    snapshot = _snapshot()
    dirs = []
    for _ in range(2):
        monkeypatch.undo()
        _patch_capture(monkeypatch, snapshot=snapshot)
        _clear_env(monkeypatch)
        dirs.append(Path(check.run(SECRET_DSN)["work_dir"]))
    assert dirs[0] != dirs[1]
    assert all(path.is_absolute() for path in dirs)


def test_check_worker_reports_a_mismatch_as_aborted(monkeypatch):
    snapshot = _snapshot()
    _patch_capture(monkeypatch, snapshot=snapshot)
    _clear_env(monkeypatch)
    real_run_child = replay.run_child

    def run_child(index, **kwargs):
        result = real_run_child(index, **kwargs)
        if index == 2:
            result["rows_digest"] = "f" * 64
        return result

    monkeypatch.setattr(replay, "run_child", run_child)
    stats = check.run(SECRET_DSN)
    assert stats["state"] == "determinism_mismatch"
    assert stats["aborted"] is True
    assert stats["reason"] == "implied_rating_determinism_mismatch"
    assert stats["verdict"] == "mismatch"
    assert stats["exit_code"] == replay.EXIT_MISMATCH
    assert "rows_digest_mismatch" in stats["mismatch_reasons"]
    assert stats["receipt"]["verdict"] == "mismatch"


@pytest.mark.parametrize("moved", ["panel", "pointer"])
def test_check_worker_reports_a_refusal_as_aborted(monkeypatch, moved):
    _patch_capture(monkeypatch, snapshot=_snapshot(), pointer="implied-current", moved=moved)
    _clear_env(monkeypatch)
    stats = check.run(SECRET_DSN)
    assert stats["state"] == "determinism_refused"
    assert stats["aborted"] is True
    assert stats["reason"] == "implied_rating_determinism_refused"
    assert stats["exit_code"] == replay.EXIT_OPERATIONAL
    assert stats["receipt"]["refusal"]["input_reasons"] in (["inputs_moved"], ["panel_pointer_moved"])


# --------------------------------------------------------------------------- #
# Expectations come from the service variables
# --------------------------------------------------------------------------- #
def test_check_worker_forwards_env_expectations_to_the_replay(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(ENV["rows_digest"], "d" * 64)
    monkeypatch.setenv(ENV["input_fingerprint"], "f" * 64)
    monkeypatch.setenv(ENV["panel_publication_id"], PANEL_UUID)
    seen = {}

    def determinism_check(dsn, **kwargs):
        seen["dsn"] = dsn
        seen.update(kwargs)
        return 0, {"verdict": "deterministic", "mismatch_reasons": [], "exit_code": 0}

    monkeypatch.setattr(replay, "determinism_check", determinism_check)
    stats = check.run("postgresql://check")
    assert stats["state"] == "deterministic"
    assert seen["dsn"] == "postgresql://check"
    assert seen["expect_rows_digest"] == "d" * 64
    assert seen["expect_input_fingerprint"] == "f" * 64
    assert seen["expect_panel_publication"] == PANEL_UUID
    assert seen["expect_current_pointer"] is None
    assert Path(seen["work_dir"]).is_absolute()


def test_check_worker_mismatched_env_expectation_is_a_mismatch(monkeypatch):
    snapshot = _snapshot()
    _patch_capture(monkeypatch, snapshot=snapshot)
    _clear_env(monkeypatch)
    monkeypatch.setenv(ENV["input_fingerprint"], "0" * 64)
    monkeypatch.setattr(replay, "run_child", lambda *a, **k: pytest.fail("fingerprint checked first"))
    stats = check.run(SECRET_DSN)
    assert stats["state"] == "determinism_mismatch"
    assert stats["aborted"] is True
    assert stats["mismatch_reasons"] == ["expected_input_fingerprint_mismatch"]


@pytest.mark.parametrize(
    ("field", "bad"),
    [("rows_digest", "D" * 64), ("input_fingerprint", "f" * 63), ("current_pointer", "not-a-uuid")],
)
def test_check_worker_refuses_a_malformed_env_expectation_before_connecting(monkeypatch, field, bad):
    _clear_env(monkeypatch)
    monkeypatch.setenv(ENV[field], bad)
    monkeypatch.setattr(replay, "determinism_check", lambda *a, **k: pytest.fail("replay ran"))
    monkeypatch.setattr(replay, "read_only_connect", lambda *a, **k: pytest.fail("connected"))
    stats = check.run(SECRET_DSN)
    assert stats["state"] == "determinism_refused"
    assert stats["aborted"] is True
    assert stats["verdict"] == "refused"
    assert stats["input_reasons"] == [f"expected_{field}_malformed"]
    assert stats["env_var"] == ENV[field]
    assert stats["receipt"] is None


def test_check_worker_ignores_the_force_flag_and_never_writes(monkeypatch):
    snapshot = _snapshot()
    _patch_capture(monkeypatch, snapshot=snapshot)  # install_schema / materialize pytest.fail
    _clear_env(monkeypatch)
    monkeypatch.setenv(worker.FORCE_ENV, "1")
    assert check.run(SECRET_DSN)["state"] == "deterministic"
    source = Path(check.__file__).read_text(encoding="utf-8")
    body = source.split('"""', 2)[2]
    assert "install_schema" not in body
    assert "materialize" not in body
    assert "psycopg" not in body


# --------------------------------------------------------------------------- #
# src.run_worker contract
# --------------------------------------------------------------------------- #
def test_run_worker_lists_the_check_worker_and_the_module_takes_no_limit_or_calc_date():
    import inspect

    from src import run_worker

    source = Path(run_worker.__file__).read_text(encoding="utf-8")
    assert "|bond_market_implied_rating_check" in source
    parameters = inspect.signature(check.run).parameters
    assert "limit" not in parameters and "calc_date" not in parameters


@pytest.mark.parametrize(
    ("stats", "code"),
    [
        ({"state": "deterministic", "aborted": False, "verdict": "deterministic",
          "receipt": {"verdict": "deterministic", "rows_digest": "d" * 64}}, 0),
        ({"state": "determinism_mismatch", "aborted": True, "verdict": "mismatch",
          "mismatch_reasons": ["rows_digest_mismatch"], "receipt": {"verdict": "mismatch"}}, 1),
        ({"state": "determinism_refused", "aborted": True, "verdict": "refused",
          "receipt": {"verdict": "refused"}}, 1),
    ],
)
def test_run_worker_exit_code_follows_the_check_verdict(stats, code):
    """The real src.run_worker.main in a fresh interpreter; the receipt lands on stdout."""
    returncode, payload = _run_lane("bond_market_implied_rating_check", stats)
    assert returncode == code
    assert payload == {"worker": "bond_market_implied_rating_check", **stats}
    assert payload["receipt"]["verdict"] == stats["verdict"]
