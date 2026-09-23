"""Explicit, unscheduled current-daily publication order; no cron is enabled."""

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
                return {"published": False, "skipped": "lock_busy"}
            as_of = _due_session(dsn)
            ingest = ingestion_runner(
                dsn, calc_date=as_of, target_session=dt.date.fromisoformat(as_of)
            )
            if ingest.get("skipped") or ingest.get("aborted"):
                raise RuntimeError("NAV_DATA_UNAVAILABLE: ingestion run incomplete")
            refreshed = matview_refresh._refresh_all(dsn, ["fund_nav_coverage_mv"])
            if refreshed != ["fund_nav_coverage_mv"]:
                raise RuntimeError("NAV_DATA_UNAVAILABLE: coverage MV not refreshed")
            risk = risk_runner(dsn, calc_date=as_of)
            if risk.get("skipped") or risk.get("mv_refreshed") is not True:
                raise RuntimeError(
                    "RETURN_SAMPLE_NOT_CURRENT: risk publication incomplete"
                )
            snapshot = readiness_runner(dsn)
            if snapshot.get("state") != "complete" or not snapshot.get("published"):
                raise RuntimeError(
                    "RETURN_SAMPLE_NOT_CURRENT: readiness pointer unchanged"
                )
            return {
                "published": True,
                "as_of_session": as_of,
                "ingestion_run_id": ingest.get("ingestion_run_id"),
                "risk_run_id": risk.get("risk_run_id"),
                "readiness_run_id": snapshot["run_id"],
                "sample_id": snapshot["sample_id"],
                "ready_count": snapshot["ready_count"],
            }
