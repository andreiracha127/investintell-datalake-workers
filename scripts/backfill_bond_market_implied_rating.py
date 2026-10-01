"""Rebuild ``bond_market_implied_rating_v1`` from the closed panel history.

The product is a full rebuild by construction (the market-level chain is
cumulative and the bucket state is path-dependent), so this CLI is the
operator's one-shot: it reads the served panel snapshot and either REPORTS the
publication the worker would write (default, ``--dry-run``), PROVES the build
is process-independent (``--determinism-check``) or publishes it
(``--apply``). The three modes are mutually exclusive; only ``--apply`` writes.

  * ``--dry-run`` never writes: no DDL install, no build pin, no pointer move.
    It prints the publication identity, the frozen policy digest, the panel
    parent, the D counts, the resolved calibration anchor, the bucket
    histogram and the runtime ``build_manifest`` the next publication would
    carry.
  * ``--determinism-check`` (DG-4 G1(a)) never writes either: it reads the
    snapshot ONCE on a read-only session with bounded timeouts, exports it to
    an immutable sha256-pinned file, rebuilds it in TWO sequential fresh
    interpreter processes and compares digests, counts and the full frames.
    It writes a JSON receipt (``--receipt``) and exits non-zero on any
    mismatch, refusal or moved input. See ``src.bonds.implied_rating_replay``.
  * ``--apply`` runs the worker with ``BOND_IMPLIED_RATING_FORCE_REPUBLISH=1``
    for exactly that call (the flag is removed afterwards), so the
    short-circuit cannot silently turn a requested rebuild into a no-op. The
    worker still refuses a stale pointer (compare-and-set) and an anchor drift.
    ``--apply`` is DIGEST-BOUND: ``--expect-rows-digest`` and
    ``--expect-input-fingerprint`` (64 lowercase hex each, from the accepted
    determinism receipt) are REQUIRED and the CLI refuses before resolving the
    DSN without them. ``--expect-current-pointer`` /
    ``--expect-panel-publication`` are optional pins. A mismatch refuses
    (``precondition_failed``) before the DDL replay and before ``materialize``;
    the rows digest is the one value only known after the build, so it is
    checked right before the write.

Nothing here touches tables other than the product's own relations and the
shared derived-publication ledger the worker owns. Production execution is an
authorized operator step; on the deployed ``bond-live-daily`` image the same
two operations run through ``python -m src.run_worker`` with
``WORKER=bond_market_implied_rating_check`` / ``WORKER=bond_market_implied_rating``
and the ``BOND_IMPLIED_RATING_EXPECT_*`` variables (see the runbook). No
secrets are printed.
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

from src.bonds import implied_rating_replay as replay  # noqa: E402
from src.db import resolve_dsn  # noqa: E402
from src.workers import bond_market_implied_rating as worker  # noqa: E402

FORCE_ENV = "BOND_IMPLIED_RATING_FORCE_REPUBLISH"
FAILURE_STATES = frozenset({
    "gate_failed", "publish_failed", "materialize_failed", "anchor_drift",
    "precondition_failed", "latest_month_unwitnessed",
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
        "--determinism-check", action="store_true",
        help="read-only G1(a) replay: one snapshot read, two fresh processes, "
             "compared digests + frames, JSON receipt; never writes",
    )
    mode.add_argument(
        "--apply", action="store_true",
        help="publish through the worker with the force-republish flag",
    )
    expect = parser.add_argument_group(
        "preconditions (--apply and --determinism-check): each set value must match "
        "or the run refuses BEFORE any DDL replay or write",
    )
    expect.add_argument(
        "--expect-current-pointer", default=None, metavar="UUID",
        help="the product's current pointer must be exactly this publication id",
    )
    expect.add_argument(
        "--expect-panel-publication", default=None, metavar="UUID",
        help="the panel's current validated publication id must be exactly this",
    )
    expect.add_argument(
        "--expect-input-fingerprint", default=None, metavar="SHA256",
        help="the closed snapshot fingerprint must equal this value",
    )
    expect.add_argument(
        "--expect-rows-digest", default=None, metavar="SHA256",
        help="the rebuilt rows_digest must equal this value (checked before materialize)",
    )
    check = parser.add_argument_group("determinism check")
    check.add_argument(
        "--receipt", default=None, metavar="PATH",
        help="where to write the determinism receipt JSON (default: <work-dir>/determinism_receipt.json)",
    )
    check.add_argument(
        "--work-dir", default=None, metavar="DIR",
        help="directory for the immutable snapshot export and child outputs (default: a new temp dir)",
    )
    check.add_argument(
        "--statement-timeout-seconds", type=int, default=replay.DEFAULT_STATEMENT_TIMEOUT_S,
        help=f"read-only session statement_timeout (default {replay.DEFAULT_STATEMENT_TIMEOUT_S})",
    )
    check.add_argument(
        "--child-timeout-seconds", type=int, default=replay.DEFAULT_CHILD_TIMEOUT_S,
        help=f"wall-clock cap per replay child (default {replay.DEFAULT_CHILD_TIMEOUT_S})",
    )
    args = parser.parse_args(argv)

    expectations = worker.ApplyExpectations(
        input_fingerprint=args.expect_input_fingerprint,
        rows_digest=args.expect_rows_digest,
        panel_publication_id=args.expect_panel_publication,
        current_pointer=args.expect_current_pointer,
    )
    if expectations.any() and not (args.apply or args.determinism_check):
        parser.error("--expect-* preconditions apply to --apply and --determinism-check only")
    for field in worker.FORCED_REQUIRED_EXPECTATIONS:
        value = getattr(expectations, field)
        if value is not None:
            try:
                worker.validate_sha256_hex(field, value, source=f"--expect-{field.replace('_', '-')}")
            except worker.ExpectationError as exc:
                parser.error(f"{exc} (expected exactly 64 lowercase hex characters)")
    if args.apply:
        # A manual apply is digest-bound: it must state WHAT it publishes
        # (the receipt's rows_digest and input_fingerprint) or it does not run.
        # Checked before the DSN is even resolved; the worker enforces the same
        # rule under BOND_IMPLIED_RATING_FORCE_REPUBLISH, so no path around it.
        missing = expectations.missing(worker.FORCED_REQUIRED_EXPECTATIONS)
        if missing:
            flags = ", ".join(f"--expect-{field.replace('_', '-')}" for field in missing)
            parser.error(
                f"--apply is digest-bound and refuses without {flags}: take both values from "
                "the accepted determinism receipt (docs/runbooks/"
                "bond-market-implied-rating-republication.md §2.3)"
            )

    dsn = resolve_dsn(args.dsn)
    if args.determinism_check:
        code, receipt = replay.determinism_check(
            dsn,
            work_dir=None if args.work_dir is None else Path(args.work_dir),
            receipt_path=None if args.receipt is None else Path(args.receipt),
            expect_input_fingerprint=expectations.input_fingerprint,
            expect_rows_digest=expectations.rows_digest,
            expect_panel_publication=expectations.panel_publication_id,
            expect_current_pointer=expectations.current_pointer,
            statement_timeout_s=args.statement_timeout_seconds,
            child_timeout_s=args.child_timeout_seconds,
        )
        print(json.dumps(receipt, default=str, sort_keys=True))
        return code
    if args.apply:
        try:
            with forced_republish():
                result = worker.run(dsn, expectations=expectations)
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
