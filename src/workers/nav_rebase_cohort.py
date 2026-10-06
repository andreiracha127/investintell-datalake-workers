"""``WORKER=nav_rebase_cohort``: one governed NAV rebase cohort run, on demand.

Runs ``scripts/rebase_fund_nav_cohort.py`` (which drives the unchanged
``scripts/rebase_fund_nav_window.py`` operator) on the ``maintenance-runner``
service, next to the database. Never scheduled: it refuses to do anything
unless ``NAV_REBASE_CONFIRM=nav_rebase_cohort_v1`` is set, so a stray deploy,
restart or cron of the shared start command cannot start a rebase. Unset the
variable (and WORKER) after the run.

Environment (only env is settable on the service; ``railway.toml`` fixes the
start command):

* ``NAV_REBASE_CONFIRM=nav_rebase_cohort_v1`` -- required.
* DSN: ``NAV_READINESS_DATABASE_URL`` when set; otherwise the service's
  ``DATABASE_URL`` (as resolved by ``run_worker``), but ONLY with
  ``NAV_REBASE_ALLOW_DATABASE_URL=1``.
* ``TIINGO_API_KEY`` -- required unless dry run (checked before planning).
* ``WORKER_LIMIT=<n>`` -> ``--max-instruments-total n`` (canary).
* Optional: ``NAV_REBASE_MAX_BATCHES``, ``NAV_REBASE_RATE_PER_SECOND``
  (<= 2.5), ``NAV_REBASE_MAX_SECONDS`` (per batch), ``NAV_REBASE_SCHEMA``
  (default ``public``), ``NAV_REBASE_DRY_RUN=1`` (plan only).

Returns the driver summary as stats. ``state`` is ``complete`` only for a
clean run (exit 0); anything else is ``failed``/``blocked`` (stopped runs also
carry ``aborted: true``), so ``run_worker`` exits non-zero and the deploy is
never painted green over failed, unknown or untried instruments. Readiness is
not republished here: run ``nav_current_daily_chain`` afterwards.
"""

from __future__ import annotations

import os
from typing import Any

from scripts import rebase_fund_nav_cohort as driver
from src.workers import nav_economic_rebase as rebase

CONFIRM_ENV = "NAV_REBASE_CONFIRM"
CONFIRM_VALUE = "nav_rebase_cohort_v1"


def _blocked(code: str) -> dict[str, Any]:
    return {
        "status": "blocked",
        "state": "blocked",
        "code": code,
        "committed": 0,
        "readiness_republish_required": False,
    }


def _int_env(name: str) -> int | None:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else None


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


def config_from_env(limit: int | None = None) -> driver.CohortConfig:
    """Build and validate the driver config; raises ``ValueError``/``RebaseError``."""
    return driver.CohortConfig(
        schema=os.environ.get("NAV_REBASE_SCHEMA", "").strip() or driver.DEFAULT_SCHEMA,
        max_seconds=_float_env("NAV_REBASE_MAX_SECONDS", driver.DEFAULT_MAX_SECONDS),
        rate_per_second=_float_env(
            "NAV_REBASE_RATE_PER_SECOND", rebase.MAX_RATE_PER_SECOND
        ),
        max_batches=_int_env("NAV_REBASE_MAX_BATCHES"),
        max_instruments_total=limit,
        dry_run=os.environ.get("NAV_REBASE_DRY_RUN", "").strip() == "1",
    ).validate()


def run(dsn: str, *, limit: int | None = None) -> dict[str, Any]:
    """One confirmed cohort run; ``limit`` is ``WORKER_LIMIT`` (total instruments)."""
    if os.environ.get(CONFIRM_ENV, "").strip() != CONFIRM_VALUE:
        return _blocked("CONFIRMATION_REQUIRED")
    try:
        config = config_from_env(limit)
    except rebase.RebaseError as exc:
        return _blocked(exc.code)
    except ValueError:
        return _blocked("CONFIG_INVALID")
    dsn_scope = driver.operator_dsn(
        allow_database_url=driver.database_url_allowed(), database_url=dsn
    )
    with dsn_scope, driver.sigterm_as_interrupt():
        exit_status, summary = driver.run_cohort(config)
    if exit_status == driver.EXIT_OK:
        state = "complete"
    elif summary["batches"] == 0 and not summary["dry_run"]:
        state = "blocked"  # stopped before any apply (config, DSN, plan)
    else:
        state = "failed"
    return {
        **summary,
        "state": state,
        "aborted": summary["status"] == "stopped",
    }
