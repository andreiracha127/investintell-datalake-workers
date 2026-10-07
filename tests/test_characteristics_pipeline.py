"""Characteristics only publish a certified cohort and skip unchanged heavy IO."""

from __future__ import annotations

import datetime as dt
from contextlib import contextmanager

import pytest

from src.workers import _fund_pipeline_freshness as freshness
from src.workers import characteristics as worker


class Conn:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, *_args):
        pass

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def _patch(monkeypatch, verdicts):
    conn = Conn()
    source = freshness.SourceCohort(
        dt.date(2026, 7, 31), dt.date(2026, 7, 31), dt.date(2026, 5, 1),
        verdict={"alarm": False},
    )

    @contextmanager
    def lock(*_args):
        yield True

    monkeypatch.setattr(worker, "connect", lambda *_a: conn)
    monkeypatch.setattr(worker, "advisory_lock", lock)
    monkeypatch.setattr(worker.freshness, "read_source_cohort", lambda *_a, **_k: source)
    answers = iter(verdicts)
    monkeypatch.setattr(worker.freshness, "probe_stage", lambda *_a: next(answers))
    return conn, source


def test_current_inputs_are_a_noop_without_rebuilding_history(monkeypatch):
    conn, source = _patch(monkeypatch, [{"alarm": False}, {"alarm": False}])
    monkeypatch.setattr(worker, "_run_layer1_setbased", lambda *_a: pytest.fail("heavy L1"))
    monkeypatch.setattr(worker, "_run_layer2_setbased", lambda *_a, **_k: pytest.fail("heavy L2"))
    assert worker.run("unused", source=source)["status"] == "current"
    assert conn.closed


def test_stale_source_is_rejected_before_characteristics_are_written(monkeypatch):
    conn, source = _patch(monkeypatch, [])
    source.verdict = {"alarm": True, "breaches": ["SOURCE_REPORT_STALE"]}
    monkeypatch.setattr(worker, "_run_layer1_setbased", lambda *_a: pytest.fail("heavy L1"))
    with pytest.raises(freshness.FundPipelineBlocked):
        worker.run("unused", source=source)
    assert conn.commits == 0


def test_company_failure_never_runs_or_commits_equity(monkeypatch):
    conn, source = _patch(monkeypatch, [{"alarm": False}, {"alarm": True}])

    def failed(*_args, **_kwargs):
        raise RuntimeError("company failure")

    monkeypatch.setattr(worker, "_run_layer1_setbased", failed)
    monkeypatch.setattr(worker, "_run_layer2_setbased", lambda *_a, **_k: pytest.fail("L2"))
    with pytest.raises(RuntimeError, match="company failure"):
        worker.run("unused", source=source)
    assert conn.rollbacks == 1


def test_equity_candidate_is_rolled_back_when_postcheck_is_incomplete(monkeypatch):
    conn, source = _patch(monkeypatch, [
        {"alarm": False}, {"alarm": True},
        {"alarm": True, "breaches": ["DERIVED_COHORT_COVERAGE_BELOW_FLOOR"]},
    ])
    monkeypatch.setattr(worker, "_run_layer1_setbased", lambda *_a, **_k: (10, 50))
    commits_seen = []

    def equity(_conn, _limit, *, commit=True):
        commits_seen.append(commit)
        return 8, 40

    monkeypatch.setattr(worker, "_run_layer2_setbased", equity)
    with pytest.raises(freshness.FundPipelineBlocked):
        worker.run("unused", source=source)
    assert commits_seen == [False]
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_standalone_company_refresh_proceeds_when_nport_is_stale(monkeypatch):
    conn, source = _patch(monkeypatch, [])
    source.verdict = {"alarm": True, "breaches": ["SOURCE_REPORT_STALE"]}
    company_calls = []

    def company(*_a, **_k):
        company_calls.append(True)
        return 10, 50

    monkeypatch.setattr(worker, "_run_layer1_setbased", company)
    monkeypatch.setattr(worker, "_run_layer2_setbased", lambda *_a, **_k: pytest.fail("fund writes"))
    with pytest.raises(freshness.FundPipelineBlocked) as exc:
        worker.run("unused")
    assert company_calls == [True]
    assert conn.commits == 1
    assert exc.value.verdict["company_upserted"] == 50
