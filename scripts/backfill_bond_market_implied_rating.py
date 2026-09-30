"""Rebuild ``bond_market_implied_rating_v1`` from the closed panel history.

The product is a full rebuild by construction (the market-level chain is
cumulative and the bucket state is path-dependent), so this CLI is the
operator's one-shot: it reads the served panel snapshot and either REPORTS the
publication the worker would write (default, ``--dry-run``) or publishes it
(``--apply``).

  * ``--dry-run`` never writes: no DDL install, no build pin, no pointer move.
    It prints the publication identity, the frozen policy digest, the panel
    parent, the D counts, the resolved calibration anchor and the bucket
    histogram the next publication would carry.
  * ``--apply`` runs the worker with ``BOND_IMPLIED_RATING_FORCE_REPUBLISH=1``
    for exactly that call (the flag is removed afterwards), so the
    short-circuit cannot silently turn a requested rebuild into a no-op. The
    worker still refuses a stale pointer (compare-and-set) and an anchor drift.

Nothing here touches tables other than the product's own relations and the
shared derived-publication ledger the worker owns. Production execution is an
authorized operator step (Railway private-network pattern); no secrets are
printed.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
from typing import Any, Iterator

import psycopg

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""} and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db import resolve_dsn  # noqa: E402
from src.workers import bond_market_implied_rating as worker  # noqa: E402

FORCE_ENV = "BOND_IMPLIED_RATING_FORCE_REPUBLISH"
FAILURE_STATES = frozenset({
    "gate_failed", "publish_failed", "materialize_failed", "anchor_drift",
})


@contextmanager
def forced_republish() -> Iterator[None]:
    """Set the worker's force flag for exactly one call, then restore it."""
    previous = os.environ.get(FORCE_ENV)
    os.environ[FORCE_ENV] = "1"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(FORCE_ENV, None)
        else:
            os.environ[FORCE_ENV] = previous


def exit_code(result: dict[str, Any]) -> int:
    """0 for a plan or a publication; 1 for a typed refusal (operator-visible)."""
    if result.get("aborted"):
        return 1
    if str(result.get("state")) in FAILURE_STATES:
        return 1
    if result.get("anchor_drift"):
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dsn", default=None, help="target datalake DSN; defaults to DATABASE_URL"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run", action="store_true",
        help="read-only plan of the next publication (this is the default)",
    )
    mode.add_argument(
        "--apply", action="store_true",
        help="publish through the worker with the force-republish flag",
    )
    args = parser.parse_args(argv)

    dsn = resolve_dsn(args.dsn)
    if args.apply:
        try:
            with forced_republish():
                result = worker.run(dsn)
        except (psycopg.Error, ValueError) as exc:
            print(json.dumps({"state": "failed", "error": type(exc).__name__}), file=sys.stderr)
            return 2
        print(json.dumps(result, default=str, sort_keys=True))
        return exit_code(result)
    result = worker.plan(dsn)
    print(json.dumps(result, default=str, sort_keys=True))
    return exit_code(result)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
