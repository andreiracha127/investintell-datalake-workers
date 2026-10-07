"""Explicit current-daily publication order; scheduling is external.

Result contract (W2/W3): contention at any stage is
``{"status": "lock_busy", "state": "lock_busy", "published": false,
"retryable": true}``; an expected, non-busy dependency outcome that prevents
publication is ``{"status": "blocked", "state": "blocked", "published": false,
"retryable": <bool>, "reason": <code>}``. Proven provider publication lag
returns ``status/state: deferred``, ``published: false``, ``retryable: true``
and ``reason: PROVIDER_SESSION_PENDING`` before risk invalidates its pointer.
Unexpected errors propagate. Only a real risk publication for the pinned due
session followed by a readiness pointer for that same session is published.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Callable

from src.db import LOCK_FUND_NAV_CURRENT_CHAIN, advisory_lock, connect
from src.workers import (
    _nav_coverage,
    fund_nav_readiness,
    instrument_ingestion,
    matview_refresh,
    risk_metrics,
)
from src.workers._nav_provider_session import assess_provider_session

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


def _provider_session(
    dsn: str, as_of: str, ingestion_run_id: str | None, *, min_active_share: float
) -> dict[str, Any]:
    with connect(dsn) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        decision_at = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        policy, grid, closed = fund_nav_readiness._policy_and_grid(conn, decision_at)
        if grid[-1].isoformat() != as_of:
            return {"pending": False, "reason": "DUE_SESSION_CHANGED"}
        result = assess_provider_session(
            conn, policy, grid, closed, decision_at,
            ingestion_run_id=ingestion_run_id, min_active_share=min_active_share,
        )
        conn.rollback()
    return result


def run(
    dsn: str,
    *,
    ingestion_runner: Callable[..., dict[str, Any]] = instrument_ingestion.run,
    risk_runner: Callable[..., dict[str, Any]] = risk_metrics.run,
    readiness_runner: Callable[..., dict[str, Any]] = fund_nav_readiness.run,
) -> dict[str, Any]:
    """Policy/calendar -> attempts/NAV -> coverage -> risk -> atomic pointer."""
    # A malformed coverage floor is a configuration error: fail before any work.
    min_active_share, min_ready_share = _nav_coverage.floors_from_env()
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
            provider_session = _provider_session(
                dsn, as_of, ingest.get("ingestion_run_id"),
                min_active_share=min_active_share,
            )
            if provider_session.get("reason") == "DUE_SESSION_CHANGED":
                return _blocked("instrument_ingestion", "DUE_SESSION_CHANGED", True)
            if provider_session.get("majority_pending") and provider_session["failed_attempt_count"]:
                return {
                    **_blocked("provider_session", "PROVIDER_SESSION_PENDING_WITH_ERRORS", True),
                    "as_of_session": as_of,
                    "ingestion_run_id": ingest.get("ingestion_run_id"),
                    "provider_session": provider_session,
                }
            if provider_session["pending"]:
                # Risk generation invalidates its old pointer before computing;
                # defer before it (and before any readiness publication).
                return {
                    "status": "deferred", "state": "deferred", "published": False,
                    "retryable": True, "reason": "PROVIDER_SESSION_PENDING",
                    "blocked_stage": "provider_session", "as_of_session": as_of,
                    "ingestion_run_id": ingest.get("ingestion_run_id"),
                    "provider_session": provider_session,
                }
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
                return _blocked(
                    "fund_nav_readiness", snapshot.get("reason") or "READINESS_NOT_PUBLISHED", True
                )
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
                # Measured after the pointer is published; run_worker fails the
                # run on an alarm so the platform shows it, the snapshot stays.
                "coverage": _nav_coverage.assess_coverage(
                    snapshot,
                    min_active_share=min_active_share,
                    min_ready_share=min_ready_share,
                ),
            }
