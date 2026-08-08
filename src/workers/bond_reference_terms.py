"""Railway adapter for the resumable, database-backed Finnhub terms backfill.

The implementation remains in :mod:`scripts.backfill_bond_reference_terms` so
the direct operator CLI and Railway use the same idempotent cursor/ledger path.
This module only adapts that path to ``src.run_worker``: it receives the already
resolved database DSN, gets the provider credential exclusively from the
environment, and turns incomplete provider results into a non-zero one-shot
worker outcome.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from scripts import backfill_bond_reference_terms as _backfill
from src.workers import _finnhub


DEFAULT_LIMIT = 100
DEFAULT_STALE_AFTER_DAYS = 30
_INCOMPLETE_KEYS = ("empty", "mismatch", "transient", "config_error")


def _config_error_summary(*, batch_label: str, limit: int) -> dict[str, Any]:
    """Return the script's typed error shape without opening a DB connection."""
    return {
        "batch_label": batch_label,
        "attempted": 0,
        "loaded": 0,
        "already_complete": 0,
        "empty": 0,
        "mismatch": 0,
        "transient": 0,
        "config_error": 1,
        "reason_counts": {"config_error": 1},
        "cursor_before": None,
        "cursor_after": None,
        "resume_cursor": None,
        "limit": limit,
        "aborted": "bond_reference_terms_config_error",
    }


def run(dsn: str, *, limit: int = DEFAULT_LIMIT) -> dict[str, Any]:
    """Run one bounded, resumable batch and report a Railway-safe JSON result.

    ``dsn`` is deliberately an argument from ``run_worker`` rather than an
    environment lookup here: the adapter has one database target and never has
    a local-file or parquet fallback.
    """
    batch_label = date.today().isoformat()
    try:
        client = _finnhub.client_from_env()
    except _finnhub.FinnhubConfigError:
        return _config_error_summary(batch_label=batch_label, limit=limit)

    summary = _backfill.run(
        client,
        dsn=dsn,
        batch_label=batch_label,
        limit=limit,
        stale_after_days=DEFAULT_STALE_AFTER_DAYS,
    )
    if any(summary.get(key, 0) for key in _INCOMPLETE_KEYS):
        summary = {**summary, "aborted": "bond_reference_terms_incomplete"}
    return summary
