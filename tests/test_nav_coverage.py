"""Coverage alarm: pure rule, chain attachment and run_worker exit."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager

import pytest

from src import run_worker
from src.workers import _nav_coverage as coverage
from src.workers import nav_current_daily_chain as chain


def _snapshot(**overrides):
    return {
        "state": "complete",
        "published": True,
        "run_id": "readiness",
        "sample_id": "s" * 64,
        "as_of_session": "2026-10-05",
        "instrument_count": 8268,
        "active_count": 7489,
        "ready_count": 7100,
        **overrides,
    }


def test_healthy_cohort_raises_no_alarm():
    result = coverage.assess_coverage(_snapshot(), min_active_share=0.85, min_ready_share=0.80)
    assert result["alarm"] is False and result["breaches"] == []
    assert result["active_share"] == round(7489 / 8268, 4)
    assert result["ready_share"] == round(7100 / 7489, 4)


@pytest.mark.parametrize(
    ("counts", "breaches"),
    [
        # 2026-10-06 before the catalog repair: 2,899 ACTIVE of 8,268, 0 admissible.
        ({"active_count": 2899, "ready_count": 0},
         ["ACTIVE_SHARE_BELOW_FLOOR", "READY_SHARE_BELOW_FLOOR"]),
        ({"active_count": 7489, "ready_count": 100}, ["READY_SHARE_BELOW_FLOOR"]),
        ({"active_count": 5000, "ready_count": 5000}, ["ACTIVE_SHARE_BELOW_FLOOR"]),
        ({"active_count": 0, "ready_count": 0},
         ["ACTIVE_SHARE_BELOW_FLOOR", "READY_SHARE_BELOW_FLOOR"]),
    ],
)
def test_each_floor_breach_is_named(counts, breaches):
    result = coverage.assess_coverage(
        _snapshot(**counts), min_active_share=0.85, min_ready_share=0.80
    )
    assert result["alarm"] is True and result["breaches"] == breaches


@pytest.mark.parametrize("missing", ["instrument_count", "active_count", "ready_count"])
def test_missing_counts_are_an_alarm(missing):
    snapshot = _snapshot()
    del snapshot[missing]
    result = coverage.assess_coverage(snapshot, min_active_share=0.85, min_ready_share=0.80)
    assert result["alarm"] is True and result["breaches"] == ["COVERAGE_COUNTS_MISSING"]


def test_floors_default_and_override():
    assert coverage.floors_from_env({}) == (0.85, 0.80)
    assert coverage.floors_from_env(
        {"NAV_COVERAGE_MIN_ACTIVE_SHARE": "0.3", "NAV_COVERAGE_MIN_READY_SHARE": " "}
    ) == (0.3, 0.80)


@pytest.mark.parametrize("raw", ["abc", "1.5", "-0.1", "nan"])
def test_malformed_floor_is_a_configuration_error(raw):
    with pytest.raises(ValueError, match="NAV_COVERAGE_MIN_ACTIVE_SHARE"):
        coverage.floors_from_env({"NAV_COVERAGE_MIN_ACTIVE_SHARE": raw})


def _patch_chain(monkeypatch, snapshot):
    @contextmanager
    def lock(*_args):
        yield True

    class Guard:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    monkeypatch.setattr(chain, "connect", lambda *_args: Guard())
    monkeypatch.setattr(chain, "advisory_lock", lock)
    monkeypatch.setattr(chain, "_due_session", lambda *_args: "2026-10-05")
    monkeypatch.setattr(chain, "_provider_session", lambda *_a, **_k: {"pending": False})
    monkeypatch.setattr(
        chain.matview_refresh, "_refresh_all", lambda _dsn, views: list(views)
    )

    def ingest(*_args, **_kwargs):
        return {"ingestion_run_id": "ingest"}

    def risk(*_args, **_kwargs):
        return {
            "calc_date": "2026-10-05",
            "mv_refreshed": True,
            "risk_run_id": "risk",
            "risk_publication": {
                "eligible": True, "published": True, "as_of_session": "2026-10-05"
            },
        }

    return {
        "ingestion_runner": ingest,
        "risk_runner": risk,
        "readiness_runner": lambda *_a: snapshot,
    }


def test_chain_attaches_coverage_after_publication(monkeypatch):
    runners = _patch_chain(monkeypatch, _snapshot(active_count=2899, ready_count=0))
    stats = chain.run("unused", **runners)
    assert stats["published"] is True and stats["status"] == "complete"
    assert stats["coverage"]["alarm"] is True
    assert stats["coverage"]["breaches"] == [
        "ACTIVE_SHARE_BELOW_FLOOR", "READY_SHARE_BELOW_FLOOR"
    ]


def test_chain_rejects_malformed_floor_before_any_work(monkeypatch):
    runners = _patch_chain(monkeypatch, _snapshot())
    monkeypatch.setenv("NAV_COVERAGE_MIN_READY_SHARE", "lots")
    monkeypatch.setattr(chain, "connect", lambda *_a: pytest.fail("work started"))
    with pytest.raises(ValueError, match="NAV_COVERAGE_MIN_READY_SHARE"):
        chain.run("unused", **runners)


def test_provider_session_pending_preserves_publications_without_coverage_alarm(monkeypatch):
    runners = _patch_chain(monkeypatch, _snapshot())
    monkeypatch.setattr(
        chain, "_provider_session",
        lambda *_a, **_k: {"pending": True, "active_count": 10, "pending_count": 9},
        raising=False,
    )
    monkeypatch.setattr(
        chain.matview_refresh, "_refresh_all",
        lambda *_a: pytest.fail("provider deferral must precede risk/MV changes"),
    )
    runners["risk_runner"] = lambda *_a, **_k: pytest.fail("risk pointer changed")
    runners["readiness_runner"] = lambda *_a, **_k: pytest.fail("readiness published")

    stats = chain.run("unused", **runners)

    assert stats["status"] == "deferred"
    assert stats["reason"] == "PROVIDER_SESSION_PENDING"
    assert stats["retryable"] is True and stats["published"] is False
    assert stats["as_of_session"] == "2026-10-05"
    assert stats["ingestion_run_id"] == "ingest"
    assert "coverage" not in stats


def test_majority_pending_with_failed_attempts_blocks_before_risk(monkeypatch):
    runners = _patch_chain(monkeypatch, _snapshot())
    monkeypatch.setattr(
        chain, "_provider_session",
        lambda *_a, **_k: {
            "pending": False, "majority_pending": True,
            "active_count": 10, "pending_count": 9, "failed_attempt_count": 1,
        },
    )
    monkeypatch.setattr(
        chain.matview_refresh, "_refresh_all",
        lambda *_a: pytest.fail("mixed provider lag/failure must preserve publications"),
    )
    runners["risk_runner"] = lambda *_a, **_k: pytest.fail("risk pointer changed")
    runners["readiness_runner"] = lambda *_a, **_k: pytest.fail("readiness published")

    stats = chain.run("unused", **runners)

    assert stats["status"] == stats["state"] == "blocked"
    assert stats["reason"] == "PROVIDER_SESSION_PENDING_WITH_ERRORS"
    assert stats["retryable"] is True and stats["published"] is False
    assert stats["provider_session"]["failed_attempt_count"] == 1
    assert "coverage" not in stats


@pytest.mark.parametrize("alarm", [True, False])
def test_run_worker_fails_the_run_on_alarm_only(monkeypatch, capsys, alarm):
    stats = {
        "status": "complete", "state": "complete", "published": True,
        "coverage": {"alarm": alarm, "breaches": ["READY_SHARE_BELOW_FLOOR"] if alarm else []},
    }
    monkeypatch.setenv("WORKER", "nav_current_daily_chain")
    monkeypatch.setattr(run_worker, "resolve_dsn", lambda: "unused")
    monkeypatch.setattr(chain, "run", lambda *_a, **_k: stats)
    monkeypatch.setattr(sys, "argv", ["run_worker"])
    if alarm:
        with pytest.raises(SystemExit) as exit_info:
            run_worker.main()
        assert exit_info.value.code == 1
        lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        assert lines[-1]["event"] == "nav_coverage_alarm"
    else:
        run_worker.main()
