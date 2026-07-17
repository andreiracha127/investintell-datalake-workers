"""Rebuild the derived certified metric surface in dependency order."""

from __future__ import annotations

import importlib
import json
from typing import Any

from src.db import resolve_dsn


WORKERS = (
    "factor_model",
    "fund_factors",
    "risk_metrics",
    "active_share_metrics",
    "momentum_metrics",
    "screener_metrics",
    "matview_refresh",
)

_FAILED_STATUSES = frozenset({"failed", "partial"})
_INCOMPLETE_STATUSES = frozenset({"skipped", "no_data"})


def _is_incomplete(result: dict[str, Any]) -> bool:
    return (
        result.get("status") in _FAILED_STATUSES | _INCOMPLETE_STATUSES
        or bool(result.get("skipped"))
        or result.get("mv_refreshed") is False
    )


def main() -> None:
    dsn = resolve_dsn()

    for worker in WORKERS:
        module = importlib.import_module(f"src.workers.{worker}")
        result = module.run(dsn) or {}
        if not isinstance(result, dict):
            raise TypeError(
                f"{worker}.run() returned {type(result).__name__}, expected dict"
            )

        print(json.dumps({"worker": worker, **result}, default=str), flush=True)
        if _is_incomplete(result):
            raise RuntimeError(
                f"Certified metric rebuild incomplete at {worker}: {result}"
            )

    print(
        json.dumps({"status": "succeeded", "workers": len(WORKERS)}),
        flush=True,
    )


if __name__ == "__main__":
    main()
