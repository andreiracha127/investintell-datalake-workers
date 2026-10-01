"""Determinism check (DG-4 G1(a)) as a Railway worker -- never writes.

``WORKER=bond_market_implied_rating_check`` runs the SAME two-fresh-process
determinism replay as ``scripts/backfill_bond_market_implied_rating.py
--determinism-check`` (``src.bonds.implied_rating_replay.determinism_check``;
no logic lives here) through the platform's only entry point,
``python -m src.run_worker``. That is what makes the check runnable on the
EXACT deployed ``bond-live-daily`` image: the service is a cron container that
exits between runs, so there is nothing to ``railway ssh`` into, and a
``startCommand`` is replaced by the config-as-code file. The operator
temporarily points ``WORKER`` at this module, triggers one run, reads the
result from the service logs and restores ``WORKER`` (runbook
``docs/runbooks/bond-market-implied-rating-republication.md`` §2.2).

The container disk is ephemeral, so the FULL receipt is returned inside the
stats dict ``run_worker`` prints as one JSON line -- the Railway log IS the
receipt. The work files (immutable snapshot export, child outputs) go to a
fresh absolute temp directory and are not needed afterwards.

Exit contract: ``state == "deterministic"`` and ``aborted: false`` only when the
replay's verdict is ``deterministic``; any mismatch or refusal returns
``aborted: true`` (``state`` ``determinism_mismatch`` / ``determinism_refused``)
so ``run_worker`` exits 1. Optional expectations come from the
``BOND_IMPLIED_RATING_EXPECT_*`` service variables (all four are honoured; a
malformed value refuses before connecting). The database session is opened
read-only by the replay itself; this module performs no DDL, no write.
"""
from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path
from typing import Any

from src.bonds import implied_rating_replay as replay
from src.workers import bond_market_implied_rating as worker

LOGGER = logging.getLogger(__name__)

STATE_BY_VERDICT = {
    "deterministic": "deterministic",
    "mismatch": "determinism_mismatch",
    "refused": "determinism_refused",
}
RECEIPT_SUMMARY_FIELDS: tuple[str, ...] = (
    "verdict", "exit_code", "mismatch_reasons", "code_revision", "policy_digest",
    "panel_publication_id", "panel_last_closed_month", "current_pointer",
    "input_fingerprint", "rows_digest", "row_count", "publication_id",
    "latest_month_witnessed_count", "child_pids", "work_dir", "receipt_path",
)


def fresh_work_dir() -> Path:
    """A new, absolute, empty directory for this run's export and child outputs."""
    return Path(tempfile.mkdtemp(prefix="bond-implied-rating-check-")).resolve()


def run(dsn: str | None = None) -> dict[str, Any]:
    """Run the G1(a) determinism replay and return its receipt as the worker stats.

    Takes no ``limit`` / ``calc_date`` on purpose: ``run_worker`` refuses
    ``WORKER_LIMIT`` / ``WORKER_CALC_DATE`` for a worker that does not read them.
    """
    started = time.monotonic()
    try:
        expectations = worker.expectations_from_env()
    except worker.ExpectationError as exc:
        refusal = worker._expectation_error_failure(exc, started=started)
        return {
            **refusal,
            "state": "determinism_refused",
            "verdict": "refused",
            "receipt": None,
        }
    work_dir = fresh_work_dir()
    LOGGER.info(
        "bond_market_implied_rating_check: determinism replay in %s (expectations: %s)",
        work_dir, "set" if expectations.any() else "none",
    )
    code, receipt = replay.determinism_check(
        dsn,
        work_dir=work_dir,
        expect_input_fingerprint=expectations.input_fingerprint,
        expect_rows_digest=expectations.rows_digest,
        expect_panel_publication=expectations.panel_publication_id,
        expect_current_pointer=expectations.current_pointer,
    )
    verdict = str(receipt.get("verdict"))
    passed = code == replay.EXIT_DETERMINISTIC and verdict == "deterministic"
    if passed:
        state = "deterministic"
    elif verdict == "mismatch":
        state = STATE_BY_VERDICT["mismatch"]
    else:
        state = STATE_BY_VERDICT["refused"]
    if not passed:
        LOGGER.warning(
            "bond_market_implied_rating_check %s: exit=%s reasons=%s",
            verdict, code, receipt.get("mismatch_reasons") or receipt.get("refusal"),
        )
    return {
        "state": state,
        "aborted": not passed,
        "reason": None if passed else f"implied_rating_{state}",
        **{field: receipt.get(field) for field in RECEIPT_SUMMARY_FIELDS},
        "expectations": receipt.get("expectations"),
        "receipt": receipt,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
