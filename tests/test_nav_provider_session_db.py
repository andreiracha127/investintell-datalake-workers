"""Provider publication timing, with real governed DB evidence and no HTTP."""

from __future__ import annotations

import datetime as dt
import uuid

import pytest

from scripts import rebase_fund_nav_window as cli
from src.workers import fund_nav_readiness as readiness
from src.workers import nav_economic_rebase as rebase
from src.workers import nav_current_daily_chain as chain
from src.workers._nav_policy import resolve_policy_and_grid
from src.workers._nav_provider_session import assess_provider_session
from tests import test_fund_nav_readiness_db as base
from tests.test_nav_economic_rebase_db import _legacy, _limits

test_dsn = base.test_dsn
schema = base.schema


def _attempts(conn, grid, ids, *, status="success_no_new", old=False, complete=True):
    run_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO nav_ingestion_runs (run_id,requested_end,status) VALUES (%s,%s,'running')",
        (run_id, grid[-1]),
    )
    conn.commit()
    now = dt.datetime.now(dt.timezone.utc)
    attempted = (
        dt.datetime.combine(grid[-1], dt.time(20), dt.timezone.utc)
        if old else now - dt.timedelta(seconds=2)
    )
    for iid in ids:
        success = status in {"success_new", "success_no_new"}
        conn.execute(
            """INSERT INTO nav_ingestion_attempts
               (run_id,instrument_id,ticker,provider,requested_start,requested_end,
                attempted_at,finished_at,status,newest_observed_date,row_count,reason_code)
               VALUES (%s,%s,'SYN','tiingo',%s,%s,%s,%s,%s,%s,%s,%s)""",
            (run_id, iid, grid[-2], grid[-1], attempted, attempted + dt.timedelta(seconds=1),
             status, grid[-2] if success else None, 1 if success else 0,
             None if success else status.upper()),
        )
    conn.commit()
    if complete:
        conn.execute("UPDATE nav_ingestion_runs SET status='completed' WHERE run_id=%s", (run_id,))
        conn.commit()
    return run_id


def _mixed_cohort(test_dsn, schema):
    current, grid, _ = base._seed(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        pending = [_legacy(conn, f"P{i}", grid[-2:-1]) for i in range(3)]
        chronic = _legacy(conn, "OLD", grid[-4:-3])
        run_id = _attempts(conn, grid, pending)
    return current, grid, pending, chronic, run_id


def _assess(conn, **kwargs):
    at = conn.execute("SELECT clock_timestamp()").fetchone()[0]
    policy, grid, closed = resolve_policy_and_grid(conn, at)
    return assess_provider_session(conn, policy, grid, closed, at, **kwargs)


def test_majority_fresh_provider_lag_defers_despite_chronic_minority(test_dsn, schema):
    _current, _grid, _pending, _chronic, run_id = _mixed_cohort(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        result = _assess(conn, ingestion_run_id=str(run_id))
        assert result["pending"] is True
        assert (result["active_count"], result["pending_count"], result["current_count"]) == (5, 3, 1)
        assert result["failed_attempt_count"] == 0
        # Rebase planning uses exactly the same DB decision and performs no apply.
        with pytest.raises(rebase.RebaseError, match="PROVIDER_SESSION_PENDING") as error:
            rebase.build_rebase_plan(conn, None, _limits(5), schema=schema, schema_pins={})
        assert error.value.retryable is True


@pytest.mark.parametrize(
    "status", ["transient_error", "rate_limited", "invalid_payload", "empty", "not_attempted_budget"]
)
def test_fresh_failed_response_vetoes_benign_deferral(test_dsn, schema, status):
    _current, grid, _pending, chronic, _run_id = _mixed_cohort(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        _attempts(conn, grid, [chronic], status=status)
        result = _assess(conn)
        assert result["pending_count"] == 3
        assert result["failed_attempt_count"] == 1
        assert result["pending"] is False
        assert result["majority_pending"] is True
        with pytest.raises(rebase.RebaseError, match="PROVIDER_SESSION_PENDING_WITH_ERRORS") as error:
            rebase.build_rebase_plan(conn, None, _limits(5), schema=schema, schema_pins={})
        assert error.value.retryable is True


@pytest.mark.parametrize("changes", [{"old": True}, {"complete": False}])
def test_old_or_incomplete_success_is_not_pending_evidence(test_dsn, schema, changes):
    _current, grid, pending, _chronic, _run_id = _mixed_cohort(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        _attempts(conn, grid, pending, **changes)
        result = _assess(conn)
        assert result["pending_count"] == 0
        assert result["pending"] is False


def test_catalog_regression_keeps_normal_coverage_alarm_path(test_dsn, schema):
    _mixed_cohort(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        conn.execute("INSERT INTO funds_profile_mv VALUES (%s)", (uuid.uuid4(),))
        conn.commit()
        assert _assess(conn)["pending"] is False  # ACTIVE share fell below 0.85.


def test_rebase_cli_reports_retryable_without_writing_plan(test_dsn, schema, tmp_path):
    _mixed_cohort(test_dsn, schema)
    args = cli._parser().parse_args([
        "--mode", "plan", "--schema", schema, "--contract", rebase.CONTRACT_VERSION,
        "--plan-file", str(tmp_path / "plan.json"),
    ])
    results = []
    with base._connect(test_dsn, schema) as conn:
        result = cli._plan(
            conn, args, _limits(5), {},
            lambda exit_status, **payload: results.append((exit_status, payload)) or exit_status,
        )
    assert result == rebase.EXIT_FAILED
    assert results == [(rebase.EXIT_FAILED, {"code": "PROVIDER_SESSION_PENDING", "retryable": True})]
    assert not (tmp_path / "plan.json").exists()


@pytest.mark.parametrize("failure", [False, True])
def test_chain_db_deferral_retains_both_publications(test_dsn, schema, monkeypatch, failure):
    _current, grid, pending, chronic, run_id = _mixed_cohort(test_dsn, schema)
    if failure:
        # One completed ingestion envelope can contain successful lag responses
        # and one failed response. Both must remain visible in the same run.
        with base._connect(test_dsn, schema) as conn:
            run_id = _attempts(conn, grid, pending, complete=False)
            conn.execute(
                """INSERT INTO nav_ingestion_attempts
                   (run_id,instrument_id,ticker,provider,requested_start,requested_end,
                    attempted_at,finished_at,status,reason_code)
                   VALUES (%s,%s,'OLD','tiingo',%s,%s,clock_timestamp(),clock_timestamp(),
                           'transient_error','TRANSIENT_ERROR')""",
                (run_id, chronic, grid[-2], grid[-1]),
            )
            conn.execute("UPDATE nav_ingestion_runs SET status='completed' WHERE run_id=%s", (run_id,))
            conn.commit()
    monkeypatch.setattr(readiness, "connect", lambda dsn: base._connect(dsn, schema))
    monkeypatch.setattr(chain, "connect", lambda dsn: base._connect(dsn, schema))
    prior = readiness.run(test_dsn)
    with base._connect(test_dsn, schema) as conn:
        risk_before = conn.execute(
            "SELECT revision_id,published_risk_run_id FROM fund_nav_risk_publication"
        ).fetchone()
    monkeypatch.setattr(
        chain.matview_refresh, "_refresh_all", lambda *_a: pytest.fail("MV refreshed while pending")
    )
    result = chain.run(
        test_dsn,
        ingestion_runner=lambda *_a, **_k: {"ingestion_run_id": str(run_id)},
        risk_runner=lambda *_a, **_k: pytest.fail("risk ran while pending"),
        readiness_runner=lambda *_a, **_k: pytest.fail("readiness ran while pending"),
    )
    reason = "PROVIDER_SESSION_PENDING_WITH_ERRORS" if failure else "PROVIDER_SESSION_PENDING"
    assert result["reason"] == reason and result["retryable"] is True
    assert result["state"] == ("blocked" if failure else "deferred")
    assert "coverage" not in result
    with base._connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT revision_id,published_risk_run_id FROM fund_nav_risk_publication"
        ).fetchone() == risk_before
        assert conn.execute(
            "SELECT run_id::text FROM fund_nav_readiness_current"
        ).fetchone()[0] == prior["run_id"]


def test_readiness_preserves_old_snapshot_until_risk_matches_new_policy(test_dsn, schema, monkeypatch):
    iid, grid, _ = base._seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: base._connect(dsn, schema))
    prior = readiness.run(test_dsn)
    with base._connect(test_dsn, schema) as conn:
        base._publish_rollover(conn, iid, grid)
        before = conn.execute("SELECT count(*) FROM fund_nav_readiness_runs").fetchone()[0]

    result = readiness.run(test_dsn)

    assert result["published"] is False and result["retryable"] is True
    assert result["reason"] == "RETURN_SAMPLE_NOT_CURRENT"
    with base._connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT run_id::text FROM fund_nav_readiness_current"
        ).fetchone()[0] == prior["run_id"]
        assert conn.execute("SELECT count(*) FROM fund_nav_readiness_runs").fetchone()[0] == before
