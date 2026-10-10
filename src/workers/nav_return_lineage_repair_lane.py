"""``WORKER=nav_return_lineage_repair_lane``: one governed return-lineage repair run, on demand.

Runs ``scripts/repair_nav_return_lineage_cohort.py`` (which calls the unchanged
operator library ``src/workers/nav_return_lineage_repair.py`` in-process) on
the ``maintenance-runner`` service, next to the database. Never scheduled: it
refuses to do anything unless
``NAV_LINEAGE_REPAIR_CONFIRM=nav_return_lineage_repair_v1`` is set, so a stray
deploy, restart or cron of the shared start command cannot start a repair.
Unset the variable (and WORKER) after the run.

Environment (only env is settable on the service; ``railway.toml`` fixes the
start command):

* ``NAV_LINEAGE_REPAIR_CONFIRM=nav_return_lineage_repair_v1`` -- required.
* DSN: ``NAV_READINESS_DATABASE_URL`` when set; otherwise the service's
  ``DATABASE_URL`` (as resolved by ``run_worker``), but ONLY with
  ``NAV_LINEAGE_REPAIR_ALLOW_DATABASE_URL=1``.
* ``TIINGO_API_KEY`` -- required unless dry run (checked before planning).
* ``WORKER_LIMIT=<n>`` -> at most n instruments in total (canary).
* Optional: ``NAV_LINEAGE_REPAIR_VALIDATE_ONLY=1`` (fetch and compare, no
  writes), ``NAV_LINEAGE_REPAIR_DRY_RUN=1`` (plan only, no HTTP),
  ``NAV_LINEAGE_REPAIR_MAX_BATCHES``, ``NAV_LINEAGE_REPAIR_RATE_PER_SECOND``
  (default 1.0, <= 2.5), ``NAV_LINEAGE_REPAIR_MAX_SECONDS`` (per batch, default
  900), ``NAV_LINEAGE_REPAIR_SCHEMA`` (default ``public``).

Returns the driver summary as stats. ``state`` is ``complete`` only for a
clean run (exit 0: every submitted instrument repaired, validated or already
repaired); residual skips, failures and stops are ``failed`` (stopped runs
also carry ``aborted: true``), a refusal before any batch is ``blocked``, so
``run_worker`` exits non-zero. Readiness is not republished here: the daily
chain does it.
"""

from __future__ import annotations

import os
from typing import Any

from scripts import repair_nav_return_lineage_cohort as driver
from scripts.rebase_fund_nav_cohort import sigterm_as_interrupt
from src.workers import nav_return_lineage_repair as repair

CONFIRM_ENV = "NAV_LINEAGE_REPAIR_CONFIRM"
CONFIRM_VALUE = "nav_return_lineage_repair_v1"
ENV_PREFIX = "NAV_LINEAGE_REPAIR_"


def _blocked(code: str) -> dict[str, Any]:
    return {
        "status": "blocked",
        "state": "blocked",
        "code": code,
        "committed_rows": 0,
        "readiness_republish_required": False,
    }


def _env(name: str) -> str:
    return os.environ.get(ENV_PREFIX + name, "").strip()


def _int_env(name: str) -> int | None:
    raw = _env(name)
    return int(raw) if raw else None


def _float_env(name: str, default: float) -> float:
    raw = _env(name)
    return float(raw) if raw else default


def config_from_env(limit: int | None = None) -> driver.CohortConfig:
    """Build and validate the driver config; raises ``ValueError``/``RepairError``."""
    return driver.CohortConfig(
        schema=_env("SCHEMA") or driver.DEFAULT_SCHEMA,
        max_seconds=_float_env("MAX_SECONDS", driver.DEFAULT_MAX_SECONDS),
        rate_per_second=_float_env("RATE_PER_SECOND", driver.DEFAULT_RATE_PER_SECOND),
        max_batches=_int_env("MAX_BATCHES"),
        max_instruments_total=limit,
        dry_run=_env("DRY_RUN") == "1",
        validate_only=_env("VALIDATE_ONLY") == "1",
    ).validate()


def run(dsn: str, *, limit: int | None = None) -> dict[str, Any]:
    """One confirmed repair run; ``limit`` is ``WORKER_LIMIT`` (total instruments)."""
    if os.environ.get(CONFIRM_ENV, "").strip() != CONFIRM_VALUE:
        return _blocked("CONFIRMATION_REQUIRED")
    try:
        config = config_from_env(limit)
    except repair.RepairError as exc:
        return _blocked(exc.code)
    except ValueError:
        return _blocked("CONFIG_INVALID")
    target = driver.operator_dsn(
        allow_database_url=driver.database_url_allowed(), database_url=dsn
    )
    with sigterm_as_interrupt():
        exit_status, summary = driver.run_cohort(config, dsn=target)
    if exit_status == driver.EXIT_OK:
        state = "complete"
    elif summary["batches"] == 0 and not summary["dry_run"]:
        state = "blocked"  # stopped before any apply (config, DSN, key, schema, plan)
    else:
        state = "failed"
    return {**summary, "state": state, "aborted": summary["status"] == "stopped"}
