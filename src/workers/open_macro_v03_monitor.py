"""open_macro_v03 monitor — the live B5 alert (missing_output_slo + re-scoped guards).

Read-only sentinel over the three sanctioned Stage B tables. The alert MECHANISM is
a non-zero exit: any illegal daily state raises, Railway marks the cron run failed,
and that failure IS the alarm. Exit 0 = healthy day. Scheduled 07:15 UTC Mon-Fri
(30 minutes after the main worker).

For the current America/New_York business day (same ``resolve_as_of`` as the main
worker; Saturday/Sunday exits 0 with ``non_business_day``):

1. **missing_output_slo** — the only two LEGAL states for ``as_of`` are
   (a) decision AND allocation rows both ``publish_state='published'`` +
   ``valid_status='valid'`` + ``valid_until > now()``; or
   (b) NEITHER output row AND a staleness-block ledger row (an intentional block).
   Anything else raises with a classification: ``missing_output`` (nothing anywhere),
   ``partial_pair`` (one-of-two), ``block_and_output_coexist`` (ledger + any output
   row), ``expired_current`` (pair present but ``valid_until <= now``),
   ``unpublished_row`` (any current row not published), ``invalidated_current``
   (a current row stamped invalidated — the kill switch fired today).
2. **Justified staleness-block cross-check** — on a block day the monitor recomputes
   ONLY the staleness gate over pack v2 + the live delta (the main worker's own
   helpers, imported, never duplicated) and confirms a breach actually exists today
   (``unjustified_staleness_block`` otherwise). The ledger row's input hashes are a
   FROZEN first-write snapshot (the ledger is immutable — ``ON CONFLICT DO NOTHING``),
   so they deliberately do NOT have to equal the monitor's live recomputation: a late
   same-day input (a normal macro vintage arrival) that leaves the breach standing
   must not turn a still-justified block into a false alarm. The persisting breach is
   the justification.
3. **Re-scoped attempt detectors** (what is observable in the DB) — an invalidated
   row whose ``invalidated_at`` is missing or in the future raises
   ``incoherent_invalidation``; any row in either output table with a FUTURE
   ``as_of`` raises ``future_as_of_write``.

The monitor has NO DDL/DML write path of any kind — not even ensure_schema (it only
pins its own session ``search_path`` to public, a session-local GUC, never a data
write). Missing tables raise ``schema_missing`` (itself a correct alert once
activation is live). To avoid
false alarms BEFORE the governance flip, it reads the SAME committed
``activation_envelope.json`` as the main worker: while ``runtime_activation`` is not
true it exits 0 with ``pre_activation`` without touching the DB — so it can be
deployed AHEAD of the flip (B5 ordering: monitoring live before the first write). It
mirrors the worker's Gate 1 + Gate 2 EXACTLY: while the ``open_macro_v03_runtime_activation``
feature flag is unset OR any ``check_governance`` gate is still false it exits 0 with
``pre_activation`` (no DB), so a deploy-ahead or a partial final flip never pages a
``missing_output`` for a day the writer is intentionally gated off — the monitor arms
only the instant run() itself would proceed to write.
"""

from __future__ import annotations

import datetime as _dt
import os
import time
from typing import Any

from src.db import connect
from src.workers.open_macro_v03 import (
    ENVELOPE_PATH,
    RUNTIME_FLAG_ENV,
    OpenMacroV03Error,
    _load_json,
    check_governance,
    compose_inputs,
    pin_search_path,
    resolve_as_of,
    verify_schema,
)
from harness.direct_activation.live_validation import staleness_report

OUTPUT_TABLES = ("open_macro_v03_decisions", "open_macro_v03_allocations")
ALL_TABLES = OUTPUT_TABLES + ("open_macro_v03_staleness_blocks",)


class OpenMacroV03MonitorAlert(RuntimeError):
    """An illegal daily state; raising it is the alarm (non-zero exit)."""

    def __init__(self, classification: str, detail: str = "") -> None:
        self.classification = classification
        super().__init__(f"{classification}: {detail}" if detail else classification)


_DAY_ROW_SQL = ("SELECT publish_state, valid_status, valid_until "
                "FROM {table} WHERE as_of = %s")
_LEDGER_SQL = "SELECT 1 FROM open_macro_v03_staleness_blocks WHERE as_of = %s"
_FUTURE_SQL = "SELECT count(*) FROM {table} WHERE as_of > %s"
_INCOHERENT_SQL = (
    "SELECT count(*) FROM {table} WHERE valid_status = 'invalidated' "
    "AND (invalidated_at IS NULL OR invalidated_at > now())")


def _check_schema(conn) -> None:
    """The full live catalog must match the committed DDL expectations (the SAME
    read-only ``verify_schema`` the worker uses); the monitor never creates or
    repairs anything — a missing or diverging catalog is itself the alarm."""
    try:
        verify_schema(conn)
    except OpenMacroV03Error as exc:
        raise OpenMacroV03MonitorAlert("schema_missing", str(exc)) from exc


def _check_future_writes(conn, as_of: _dt.date) -> None:
    with conn.cursor() as cur:
        for table in OUTPUT_TABLES:
            cur.execute(_FUTURE_SQL.format(table=table), (as_of,))
            count = cur.fetchone()[0]
            if count:
                raise OpenMacroV03MonitorAlert(
                    "future_as_of_write",
                    f"{count} row(s) in {table} with as_of > {as_of.isoformat()}")


def _check_invalidation_coherence(conn) -> None:
    with conn.cursor() as cur:
        for table in OUTPUT_TABLES:
            cur.execute(_INCOHERENT_SQL.format(table=table))
            count = cur.fetchone()[0]
            if count:
                raise OpenMacroV03MonitorAlert(
                    "incoherent_invalidation",
                    f"{count} invalidated row(s) in {table} with a missing or "
                    "future invalidated_at")


def _day_row(conn, table: str, as_of: _dt.date):
    with conn.cursor() as cur:
        cur.execute(_DAY_ROW_SQL.format(table=table), (as_of,))
        return cur.fetchone()


def _ledger_row(conn, as_of: _dt.date):
    with conn.cursor() as cur:
        cur.execute(_LEDGER_SQL, (as_of,))
        return cur.fetchone()


def _check_block_justified(conn, as_of: _dt.date) -> None:
    """A recorded block is honoured only if JUSTIFIED: the recomputed staleness gate
    must actually breach today.

    The ledger row's input hashes are a FROZEN first-write snapshot (the ledger is
    immutable — a re-run of a still-stale day preserves the first record), so they
    deliberately do NOT track inputs that move later the same day. Requiring the
    live recomputed hashes to equal that frozen snapshot would raise a false
    ``block_input_hash_mismatch`` after a normal late macro-vintage arrival on a day
    that is still legitimately blocked; the persisting breach is the justification."""
    vintage_rows, price_rows = compose_inputs(conn, as_of)
    report = staleness_report(vintage_rows, price_rows, as_of)
    if not report["breaches"]:
        raise OpenMacroV03MonitorAlert(
            "unjustified_staleness_block",
            f"ledger row for {as_of.isoformat()} but the recomputed inputs are fresh")


def run(dsn: str, *, as_of: str | None = None) -> dict[str, Any]:
    """Daily read-only monitor. Raises on any illegal state (the alarm); returns
    stats for the Railway log on a healthy or justified-block day."""
    t0 = time.monotonic()

    # Pre-activation: the committed envelope gates the monitor exactly like the
    # worker, so a deploy ahead of the flip never false-alarms and arms itself
    # the instant the envelope flips. No DB access in this state.
    # Mirror the worker's Gate 1 + Gate 2 EXACTLY: arm only when run() would proceed to
    # write. If the Railway feature flag is unset (flag_off) or ANY governance gate is
    # still false (governance_blocked), the writer is intentionally gated off, so a
    # missing_output alarm here would be a false page during deploy-ahead / partial-flip.
    if os.environ.get(RUNTIME_FLAG_ENV) != "true":
        return {"status": "pre_activation", "reason": "flag_off"}
    envelope = _load_json(ENVELOPE_PATH)
    reason = check_governance(envelope)
    if reason is not None:
        return {"status": "pre_activation", "reason": reason}

    as_of_date = resolve_as_of(as_of)
    if as_of_date is None:
        return {"status": "non_business_day"}

    conn = connect(dsn)
    try:
        pin_search_path(conn)
        _check_schema(conn)
        checks: dict[str, Any] = {"schema": "ok"}

        _check_future_writes(conn, as_of_date)
        checks["future_as_of_write"] = "none"
        _check_invalidation_coherence(conn)
        checks["invalidation_coherence"] = "ok"

        decision = _day_row(conn, "open_macro_v03_decisions", as_of_date)
        allocation = _day_row(conn, "open_macro_v03_allocations", as_of_date)
        ledger = _ledger_row(conn, as_of_date)
        now = _dt.datetime.now(_dt.timezone.utc)

        if ledger is not None:
            # legal state (b): an intentional, durable staleness-block — but only
            # with NO output rows, and only if independently justified.
            if decision is not None or allocation is not None:
                raise OpenMacroV03MonitorAlert(
                    "block_and_output_coexist",
                    f"ledger row AND output row(s) both present for "
                    f"{as_of_date.isoformat()}")
            _check_block_justified(conn, as_of_date)
            checks["missing_output_slo"] = "staleness_block"
            checks["staleness_block_justified"] = True
            return {"status": "staleness_block_day", "as_of": as_of_date.isoformat(),
                    "checks": checks, "wall_ms": int((time.monotonic() - t0) * 1000)}

        # legal state (a): both rows, published, valid, unexpired.
        if decision is None and allocation is None:
            raise OpenMacroV03MonitorAlert(
                "missing_output",
                f"no decision, no allocation and no staleness-block ledger row for "
                f"{as_of_date.isoformat()} (silent worker exit)")
        if decision is None or allocation is None:
            present = "decision" if decision is not None else "allocation"
            raise OpenMacroV03MonitorAlert(
                "partial_pair",
                f"only the {present} row exists for {as_of_date.isoformat()} "
                "(one-of-two consumer-visible state)")
        for name, row in (("decision", decision), ("allocation", allocation)):
            if row[0] != "published":
                raise OpenMacroV03MonitorAlert(
                    "unpublished_row", f"{name} row has publish_state={row[0]!r}")
        for name, row in (("decision", decision), ("allocation", allocation)):
            if row[1] != "valid":
                raise OpenMacroV03MonitorAlert(
                    "invalidated_current",
                    f"{name} row for the current day has valid_status={row[1]!r}")
        for name, row in (("decision", decision), ("allocation", allocation)):
            if row[2] is None or row[2] <= now:
                raise OpenMacroV03MonitorAlert(
                    "expired_current",
                    f"{name} row valid_until {row[2]} has lapsed (<= now)")

        checks["missing_output_slo"] = "pass"
        return {"status": "healthy", "as_of": as_of_date.isoformat(),
                "checks": checks, "wall_ms": int((time.monotonic() - t0) * 1000)}
    finally:
        conn.close()
