"""Explicit, unscheduled current-daily publication order; no cron is enabled.

Result contract (W2/W3): contention at any stage is
``{"status": "lock_busy", "state": "lock_busy", "published": false,
"retryable": true}``; an expected, non-busy dependency outcome that prevents
publication is ``{"status": "blocked", "state": "blocked", "published": false,
"retryable": <bool>, "reason": <code>}``; unexpected errors propagate. Only a
real risk publication for the pinned due session followed by a readiness
pointer for that same session is ``published: true``.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Callable

from src.db import LOCK_FUND_NAV_CURRENT_CHAIN, advisory_lock, connect
from src.workers import (
    fund_nav_readiness,
    instrument_ingestion,
    matview_refresh,
    risk_metrics,
)

LOCK_BUSY_RESULT = fund_nav_readiness.LOCK_BUSY_RESULT


def _lock_busy(stage: str) -> dict[str, Any]:
    return {**LOCK_BUSY_RESULT, "blocked_stage": stage}


def _blocked(stage: str, reason: str | None, retryable: bool) -> dict[str, Any]:
    return {
        "status": "blocked",
        "state": "blocked",
        "published": False,
        "retryable": bool(retryable),
        "reason": reason,
        "blocked_stage": stage,
    }


def _due_session(dsn: str) -> str:
    # An unpublished calendar must stop before any provider request or DB writes.
    with connect(dsn) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        decision_at = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        _policy, grid, _closed = fund_nav_readiness._policy_and_grid(conn, decision_at)
        conn.rollback()
    return grid[-1].isoformat()


def run(
    dsn: str,
    *,
    ingestion_runner: Callable[..., dict[str, Any]] = instrument_ingestion.run,
    risk_runner: Callable[..., dict[str, Any]] = risk_metrics.run,
    readiness_runner: Callable[..., dict[str, Any]] = fund_nav_readiness.run,
) -> dict[str, Any]:
    """Policy/calendar -> attempts/NAV -> coverage -> risk -> atomic pointer."""
    with connect(dsn) as guard:
        with advisory_lock(guard, LOCK_FUND_NAV_CURRENT_CHAIN) as acquired:
            if not acquired:
                return _lock_busy("nav_current_daily_chain")
            as_of = _due_session(dsn)
            ingest = ingestion_runner(
                dsn, calc_date=as_of, target_session=dt.date.fromisoformat(as_of)
            )
            if ingest.get("skipped") == "lock_busy":
                return _lock_busy("instrument_ingestion")
            if ingest.get("skipped") or ingest.get("aborted"):
                raise RuntimeError("NAV_DATA_UNAVAILABLE: ingestion run incomplete")
            refreshed = matview_refresh._refresh_all(dsn, ["fund_nav_coverage_mv"])
            if refreshed != ["fund_nav_coverage_mv"]:
                raise RuntimeError("NAV_DATA_UNAVAILABLE: coverage MV not refreshed")
            risk = risk_runner(dsn, calc_date=as_of)
            publication = risk.get("risk_publication") or {}
            if risk.get("skipped") == "lock_busy" or publication.get("reason") == "LOCK_BUSY":
                return _lock_busy("risk_metrics")
            if risk.get("skipped") or risk.get("aborted"):
                return _blocked("risk_metrics", "RISK_RUN_INCOMPLETE", True)
            if risk.get("mv_refreshed") is not True:
                return _blocked("risk_metrics", "MV_REFRESH_FAILED", True)
            if not (
                publication.get("eligible") is True
                and publication.get("published") is True
            ):
                return _blocked(
                    "risk_metrics",
                    publication.get("reason") or "RISK_NOT_PUBLISHED",
                    bool(publication.get("retryable", True)),
                )
            if risk.get("calc_date") != as_of or publication.get("as_of_session") != as_of:
                return _blocked("risk_metrics", "DUE_SESSION_CHANGED", True)
            snapshot = readiness_runner(dsn)
            if snapshot.get("status") == "lock_busy":
                return _lock_busy("fund_nav_readiness")
            if snapshot.get("state") != "complete" or snapshot.get("published") is not True:
                return _blocked("fund_nav_readiness", "READINESS_NOT_PUBLISHED", True)
            if snapshot.get("as_of_session") != as_of:
                return _blocked("fund_nav_readiness", "DUE_SESSION_CHANGED", True)
            return {
                "status": "complete",
                "state": "complete",
                "published": True,
                "retryable": False,
                "as_of_session": as_of,
                "ingestion_run_id": ingest.get("ingestion_run_id"),
                "risk_run_id": risk.get("risk_run_id"),
                "readiness_run_id": snapshot["run_id"],
                "sample_id": snapshot["sample_id"],
                "ready_count": snapshot["ready_count"],
            }
