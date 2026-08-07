"""Publish fund analytics in dependency order.

The individual workers remain independently runnable, but the production
publication path must not rely on cron timing.  This one-shot orchestrator makes
the dependency explicit: risk commits and refreshes its read model, momentum
uses that exact calculation date, then momentum refreshes ``funds_list_mv``.
"""
from __future__ import annotations

from typing import Any, Callable

from src.db import LOCK_ANALYTICS_REFRESH_CHAIN, advisory_lock, connect
from src.workers import momentum_metrics, risk_metrics


class DependencyBlocked(RuntimeError):
    """A predecessor did not publish a fresh result."""


def _require_success(stage: str, stats: dict[str, Any]) -> None:
    if stats.get("skipped"):
        raise DependencyBlocked(f"{stage} blocked: {stats['skipped']}")
    if stats.get("aborted"):
        raise DependencyBlocked(f"{stage} aborted before publication")


def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
    risk_runner: Callable[..., dict[str, Any]] = risk_metrics.run,
    momentum_runner: Callable[..., dict[str, Any]] = momentum_metrics.run,
) -> dict[str, Any]:
    """Run risk then momentum/catalogue publication under an outer lock."""
    with connect(dsn) as guard:
        with advisory_lock(guard, LOCK_ANALYTICS_REFRESH_CHAIN) as got:
            if not got:
                return {
                    "published": False,
                    "stages": [],
                    "skipped": "lock_busy",
                    "blocked_dependency": "analytics_refresh_chain",
                }

            risk = risk_runner(dsn, calc_date=calc_date, limit=limit)
            _require_success("risk_metrics", risk)
            if risk.get("mv_refreshed") is not True:
                reason = risk.get("mv_refresh_error", "fund_risk_latest_mv not refreshed")
                raise DependencyBlocked(f"momentum_metrics blocked: {reason}")

            risk_date = risk.get("calc_date")
            if not risk_date:
                raise DependencyBlocked("momentum_metrics blocked: risk calc_date missing")
            if calc_date is not None and str(risk_date) != calc_date:
                raise DependencyBlocked(
                    f"momentum_metrics blocked: risk calc_date {risk_date} != {calc_date}"
                )

            momentum = momentum_runner(dsn, calc_date=str(risk_date), limit=limit)
            _require_success("momentum_metrics", momentum)
            if str(momentum.get("calc_date")) != str(risk_date):
                raise DependencyBlocked(
                    "funds_list_mv blocked: momentum watermark does not match risk"
                )

            return {
                "published": True,
                "calc_date": str(risk_date),
                "stages": [
                    {"name": "risk_metrics", "stats": risk},
                    {"name": "momentum_metrics", "stats": momentum},
                    {"name": "funds_list_mv", "refreshed": True},
                ],
            }
