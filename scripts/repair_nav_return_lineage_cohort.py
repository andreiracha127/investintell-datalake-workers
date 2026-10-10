"""Cohort driver for the governed return-lineage flag repair (nav-return-lineage-repair-v1).

Calls the operator library ``src/workers/nav_return_lineage_repair.py``
IN-PROCESS and unchanged, with the same schema check, REPEATABLE READ READ ONLY
plan and ``run_repair`` guards as ``scripts/repair_nav_return_lineage_v1.py``.
The driver only sequences those calls and counts their results:

* **Plan**: one ``build_plan`` over every instrument with a bad row. Eligible
  items (``reasons == []``) are queued in plan order; ``--max-instruments-total``
  caps the queue. ``max_requests`` is the largest bad-row count of a queued
  item, clamped to 20..10000 (an item above 10000 can never be repaired in one
  batch and stays a ``REQUEST_BUDGET_TOO_SMALL`` residual). The plan carries
  exactly these limits, so plan limits equal run limits.
* **Apply**: allowlists of <= 20 whose bad rows fit one batch's request budget
  (``max_requests`` is per ``run_repair`` call, not per instrument) and, paced at
  ``--rate-per-second``, half of ``--max-seconds``; a larger item goes alone.
  Before each allowlist the schema/access check and the plan's schema pins are
  re-verified; then one ``run_repair`` on a connection idle in between.
* **Second pass**: per-instrument ``PLAN_STALE`` skips are re-planned ONCE after
  a first pass that made progress, and only those instruments are retried; one
  now ineligible is a residual with its plan reason. One the fresh plan no
  longer finds goes back through ``run_repair`` with its pass-1 item (a
  reconcile plan: the pass-1 plan with only those items): the operator's
  resume branch, with no request and no write, returns ``ALREADY_REPAIRED``
  only when the stored rows hold the intended values under an attributed
  clear, otherwise ``UNATTRIBUTED_CLEAR`` or ``PLAN_STALE`` (residuals).
* **Stops**: operator exit 3, 4 (5 when this run already wrote), 5 (budget,
  interrupt, ``PROVIDER_UNAVAILABLE``, ``PROVIDER_BUDGET``, lock after work),
  ``CLOCK_SKEW``, ``COMMIT_UNKNOWN``, a driver-contract code, or
  ``--max-failed-batches`` failed batches in a row. A batch that failed before
  any instrument is retried whole; an instrument the operator failed on twice
  is recorded failed, and the untried ones go back in front. A failed batch
  stays unresolved (``pending_batch_failure``) until a later batch succeeds.

Stdout: one sanitized JSON line per plan and per batch, then the summary
(counts by code; never a DSN, URL, payload, exception text or id list). The one
exception is a ``COMMIT_UNKNOWN`` batch line, which names that instrument and
its run ids: they are needed to reconcile before any retry.

Exit: 0 every submitted instrument committed/validated/already repaired (or a
requested cap was reached cleanly), or dry run; 2 residual skips, failed or
unknown instruments, an unresolved failed batch (also when a cap ended the
run), or a driver-level stop; 3/4/5 as the stopping operator call. Readiness is NOT republished here: the daily chain does it.

Run from the repository root: ``python -m scripts.repair_nav_return_lineage_cohort``.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import sys
import time
import uuid
from collections import Counter, deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus

from scripts import fund_nav_readiness_schema as schema_operator
from scripts.rebase_fund_nav_cohort import sigterm_as_interrupt
from scripts.rebase_fund_nav_window import _SAFE, _schema_pins
from src.workers import nav_return_lineage_repair as repair

DRIVER_VERSION = "nav-return-lineage-repair-cohort-v1"
DEFAULT_SCHEMA = "public"
BATCH_SIZE = 20
MIN_REQUESTS = 20
MAX_REQUESTS = 10000
DEFAULT_MAX_SECONDS = 900.0
DEFAULT_RATE_PER_SECOND = 1.0
DEFAULT_MAX_FAILED_BATCHES = 3
MAX_INSTRUMENT_ATTEMPTS = 2  # the operator failed on it twice: record it failed
PACING_SHARE = 0.5  # pacing of one batch's requests may use half its time budget

DSN_ENV = "NAV_READINESS_DATABASE_URL"
ALLOW_DATABASE_URL_ENV = "NAV_LINEAGE_REPAIR_ALLOW_DATABASE_URL"

EXIT_OK, EXIT_FAILED, EXIT_INCOMPATIBLE, EXIT_LOCK_BUSY, EXIT_INTERRUPTED = 0, 2, 3, 4, 5
STOP_EXITS = (EXIT_INCOMPATIBLE, EXIT_LOCK_BUSY, EXIT_INTERRUPTED)
INCOMPATIBLE_SQLSTATES = ("42501", "42P01", "42703", "42883", "0A000")

STALE = "PLAN_STALE"
OVERSIZED = "REQUEST_BUDGET_TOO_SMALL"
# Exit-2 codes that stop the run: a lost commit acknowledgement must be
# reconciled first; a clock ahead of the database fails every later batch too.
STOP_CODES = frozenset({"CLOCK_SKEW", "COMMIT_UNKNOWN"})
# The driver broke the operator contract: every later batch would repeat it.
FATAL_CODES = frozenset(
    {
        "PLAN_DIGEST_MISMATCH",
        "PLAN_LIMITS_MISMATCH",
        "ALLOWLIST_INVALID",
        "ALLOWLIST_OUTSIDE_PLAN",
        OVERSIZED,
        "CONNECTION_NOT_IDLE",
        "BATCH_SIZE_INVALID",
        "MAX_REQUESTS_INVALID",
        "MAX_SECONDS_INVALID",
        "RATE_INVALID",
        "UNSAFE_CODE",
    }
)
DONE = frozenset({"committed", "validated", "noop"})


class DriverStop(Exception):
    """Internal: ends the run with a sanitized stop code and exit status."""

    def __init__(self, code: str, exit_status: int) -> None:
        super().__init__(code)
        self.code = code
        self.exit_status = exit_status


def _code(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if _SAFE.fullmatch(text) else "UNSAFE_CODE"


def _codes(counts: Any) -> dict[str, int]:
    out: Counter[str] = Counter()
    if isinstance(counts, dict):
        for key, value in counts.items():
            out[_code(key) or "NONE"] += int(value)
    return dict(sorted(out.items()))


def _uuid(value: Any) -> str:
    return str(uuid.UUID(str(value)))


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, default=str), flush=True)


def _psycopg_stop(exc: psycopg.Error) -> DriverStop:
    if exc.sqlstate in INCOMPATIBLE_SQLSTATES:
        return DriverStop("SCHEMA_ACCESS_INCOMPATIBLE", EXIT_INCOMPATIBLE)
    return DriverStop("DATABASE_ERROR", EXIT_FAILED)


# ──────────────────────────────────────────────────────────────────────────────
# Configuration, DSN and the database seam
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class CohortConfig:
    schema: str = DEFAULT_SCHEMA
    max_seconds: float = DEFAULT_MAX_SECONDS
    rate_per_second: float = DEFAULT_RATE_PER_SECOND
    max_batches: int | None = None
    max_instruments_total: int | None = None
    max_failed_batches: int = DEFAULT_MAX_FAILED_BATCHES
    dry_run: bool = False
    validate_only: bool = False

    def limits(self, max_requests: int = MIN_REQUESTS) -> repair.RepairLimits:
        return repair.RepairLimits(
            batch_size=BATCH_SIZE,
            max_requests=max_requests,
            max_seconds=float(self.max_seconds),
            rate_per_second=float(self.rate_per_second),
        )

    def validate(self) -> CohortConfig:
        """Driver-level bounds; raises ``RepairError`` with a sanitized code."""
        if not schema_operator.SCHEMA_NAME.fullmatch(str(self.schema)):
            raise repair.RepairError("SCHEMA_INVALID")
        self.limits().validate()
        if self.max_batches is not None and self.max_batches < 1:
            raise repair.RepairError("MAX_BATCHES_INVALID")
        if self.max_instruments_total is not None and self.max_instruments_total < 1:
            raise repair.RepairError("MAX_INSTRUMENTS_TOTAL_INVALID")
        if self.max_failed_batches < 1:
            raise repair.RepairError("MAX_FAILED_BATCHES_INVALID")
        return self


def database_url_allowed(flag: bool = False) -> bool:
    return flag or os.environ.get(ALLOW_DATABASE_URL_ENV, "").strip() == "1"


def operator_dsn(*, allow_database_url: bool, database_url: str | None) -> str | None:
    """``NAV_READINESS_DATABASE_URL`` wins; ``database_url`` only when allowed."""
    return os.environ.get(DSN_ENV) or (database_url if allow_database_url else None)


class PostgresDatabase:
    """The operator's own database calls, connected the way its CLI connects."""

    def __init__(self, dsn: str, schema: str) -> None:
        self.dsn, self.schema, self.conn = dsn, schema, None

    def check(self, plan: dict | None = None) -> None:
        """Schema and access on a fresh autocommit connection, as the operator
        CLI checks them on every invocation; with a plan, also its pins."""
        with psycopg.connect(self.dsn, autocommit=True, connect_timeout=5) as conn:
            try:
                ready = schema_operator._check(conn, self.schema)["ready"]
            except (schema_operator.PrerequisiteBlocked, ValueError):
                ready = False
        if not ready or (plan is not None and plan.get("schema_pins") != _schema_pins()):
            raise repair.RepairError("SCHEMA_ACCESS_INCOMPATIBLE", EXIT_INCOMPATIBLE)

    def _connection(self) -> psycopg.Connection:
        """One working connection, reopened when broken; always idle on return."""
        conn = self.conn
        if conn is None or conn.closed or conn.broken:
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.close()
            self.conn = None
            conn = psycopg.connect(self.dsn, autocommit=False, connect_timeout=5)
            conn.execute(
                sql.SQL("SET search_path TO {}, pg_temp").format(sql.Identifier(self.schema))
            )
            conn.commit()
            self.conn = conn
        elif conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        return conn

    def plan(self, limits: repair.RepairLimits) -> dict:
        conn = self._connection()
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        try:
            return repair.build_plan(
                conn, schema=self.schema, limits=limits, schema_pins=_schema_pins()
            )
        finally:
            conn.rollback()

    def repair(self, plan, ids, sha, limits, client, validate_only) -> dict:
        return repair.run_repair(
            self._connection(),
            plan,
            instrument_ids=ids,
            supplied_sha256=sha,
            limits=limits,
            client=client,
            validate_only=validate_only,
        )

    def count_bad(self) -> int:
        conn = self._connection()
        conn.execute("SET TRANSACTION READ ONLY")
        try:
            return int(
                conn.execute("SELECT count(*) FROM nav_timeseries WHERE " + repair.BAD).fetchone()[0]
            )
        finally:
            conn.rollback()

    def close(self) -> None:
        if self.conn is not None:
            with contextlib.suppress(Exception):
                self.conn.close()
            self.conn = None


# ──────────────────────────────────────────────────────────────────────────────
# Batching
# ──────────────────────────────────────────────────────────────────────────────
def request_budget(items: Iterable[dict]) -> int:
    """Largest bad-row count of an item to apply, clamped to 20..10000."""
    largest = max((len(item["rows"]) for item in items), default=0)
    return min(MAX_REQUESTS, max(MIN_REQUESTS, largest))


def rows_per_batch(limits: repair.RepairLimits) -> int:
    """Bad rows one allowlist may carry: the request budget, and no more than
    half of what the batch's time budget can pace."""
    paced = math.floor(limits.max_seconds * limits.rate_per_second * PACING_SHARE)
    return max(1, min(limits.max_requests, paced))


@dataclass(frozen=True)
class _Planned:
    plan: dict
    sha256: str
    limits: repair.RepairLimits
    queue: tuple[dict, ...]
    reconcile: bool = False


def reconcile_plan(source: _Planned, instrument_ids: Iterable[str]) -> _Planned | None:
    """The plan for stale items a re-plan no longer finds: ``source`` (the plan
    they went stale under: version, schema, pins, as_of, limits) with only
    their ORIGINAL items, in plan order. ``run_repair`` resumes such an item in
    its read-only branch: ``ALREADY_REPAIRED`` only when the stored rows hold
    the intended values and the clear is attributed, otherwise
    ``UNATTRIBUTED_CLEAR`` or ``PLAN_STALE``."""
    wanted = set(instrument_ids)
    items = [item for item in source.plan["items"] if item["instrument_id"] in wanted]
    if not items:
        return None
    plan = {**source.plan, "items": items}
    return _Planned(plan, repair.sha256(plan), source.limits, tuple(items), reconcile=True)


# ──────────────────────────────────────────────────────────────────────────────
# The cohort loop
# ──────────────────────────────────────────────────────────────────────────────
class _Cohort:
    def __init__(self, config, db, client_factory, emit, monotonic) -> None:
        self.config = config
        self.db = db
        self.client_factory = client_factory
        self.client = None
        self.emit = emit
        self.monotonic = monotonic
        self.started = monotonic()
        self.plans = 0
        self.passes = 0
        self.batches = 0
        self.failed_streak = 0
        self.first: dict | None = None  # summarize() of the first plan
        self.planned: _Planned | None = None
        self.queue: deque[dict] = deque()
        self.deferred = 0  # queued in a later segment of the current pass
        self.beyond_cap = 0  # eligible but past --max-instruments-total
        # Code of the last failed batch whose chunk went back in the queue;
        # cleared once a later batch (the retry) succeeds.
        self.pending_batch_failure: str | None = None
        self.submitted: set[str] = set()
        self.final: dict[str, dict] = {}  # last outcome per instrument
        self.attempts: Counter[str] = Counter()  # in-loop batch failures per instrument
        self.stale: list[str] = []  # PLAN_STALE skips of the current pass
        self.pass_progress = 0
        self.requests = 0
        self.errors_by_code: Counter[str] = Counter()
        self.remaining_bad_rows: int | None = None

    def _elapsed(self, since: float | None = None) -> float:
        return round(self.monotonic() - (self.started if since is None else since), 3)

    def _client(self, limits):
        if self.client is None:
            self.client = (self.client_factory or repair.ProviderClient)(limits)
        return self.client

    @property
    def wrote(self) -> bool:
        return any(o["status"] in ("committed", "unknown") for o in self.final.values())

    # -- plan ------------------------------------------------------------------
    def make_plan(self, pass_no: int, scope: set[str] | None) -> list[_Planned]:
        """Plan once; the pass's segments in order (pass 2: the reconcile of the
        stale items the fresh plan no longer finds, then the fresh queue)."""
        began = self.monotonic()
        self.plans += 1
        previous = self.planned  # the plan the scoped items went stale under
        plan = self.db.plan(self.config.limits())
        summary = repair.summarize(plan)
        eligible = [item for item in plan["items"] if not item["reasons"]]
        reconcile = None
        if scope is None:
            candidates = eligible[: self.config.max_instruments_total]
            self.beyond_cap = len(eligible) - len(candidates)
        else:
            present = {item["instrument_id"]: item for item in plan["items"]}
            for iid in scope:
                item = present.get(iid)
                if item is not None and item["reasons"]:  # a residual now, real reason
                    self.final[iid] = {
                        "status": "skipped",
                        "code": _code(item["reasons"][0]),
                        "rows": len(item["rows"]),
                    }
            # No longer bad: only the operator can tell an attributed repair
            # from an unattributed clear, against the item it went stale under.
            assert previous is not None
            reconcile = reconcile_plan(previous, scope - set(present))
            candidates = [item for item in eligible if item["instrument_id"] in scope]
        oversized = [item for item in candidates if len(item["rows"]) > MAX_REQUESTS]
        for item in oversized:  # could never fit one run_repair call
            self.final[item["instrument_id"]] = {
                "status": "skipped", "code": OVERSIZED, "rows": len(item["rows"])
            }
        queue = tuple(item for item in candidates if len(item["rows"]) <= MAX_REQUESTS)
        limits = self.config.limits(request_budget(queue)).validate()
        plan["limits"] = limits.canonical()  # plan limits == run limits
        sha = repair.sha256(plan)
        if self.first is None:
            self.first = summary
        self.emit(
            {
                "event": "plan",
                "pass": pass_no,
                "plan_sha256": sha,
                "instruments": summary["instruments"],
                "rows": summary["rows"],
                "eligible_instruments": summary["eligible_instruments"],
                "history_older_than_30_days": summary["history_older_than_30_days"],
                "not_eligible_rows_by_reason": _codes(summary["residual_counts"]),
                "source_counts": _codes(summary["source_counts"]),
                "to_apply": len(queue),
                "to_apply_rows": sum(len(item["rows"]) for item in queue),
                "to_reconcile": len(reconcile.queue) if reconcile else 0,
                "over_request_cap": len(oversized),
                "max_requests": limits.max_requests,
                "rows_per_batch": rows_per_batch(limits),
                "elapsed_s": self._elapsed(began),
            }
        )
        self.planned = _Planned(plan, sha, limits, queue)
        return [reconcile, self.planned] if reconcile else [self.planned]

    # -- apply -------------------------------------------------------------------
    def next_chunk(self, limits) -> list[dict]:
        cap = rows_per_batch(limits)
        chunk: list[dict] = []
        rows = 0
        while self.queue and len(chunk) < limits.batch_size:
            n = len(self.queue[0]["rows"])
            if chunk and rows + n > cap:
                break
            chunk.append(self.queue.popleft())
            rows += n
        return chunk

    def apply(self, pass_no: int, planned: _Planned, chunk: list[dict]) -> None:
        ids = [item["instrument_id"] for item in chunk]
        began = self.monotonic()
        counted = False
        try:
            # Schema, access and plan pins before EVERY batch, as the operator
            # CLI checks them per invocation: a multi-hour run must notice a
            # migration or an access-profile change (exit 3).
            self.db.check(planned.plan)
            self.batches += 1
            counted = True
            self.submitted.update(ids)
            result = self.db.repair(
                planned.plan,
                ids,
                planned.sha256,
                planned.limits,
                self._client(planned.limits),
                self.config.validate_only,
            )
        except psycopg.Error as exc:  # never str(exc): it may carry connection text
            stop = _psycopg_stop(exc)
            if stop.exit_status == EXIT_INCOMPATIBLE:
                self.queue.extendleft(reversed(chunk))
                raise stop from None
            # The database was unreachable: a batch failure before any instrument.
            self.batches += 0 if counted else 1
            result = {"status": "failed", "code": stop.code, "exit_code": EXIT_FAILED}
        except BaseException:
            self.queue.extendleft(reversed(chunk))  # report them as remaining
            raise
        outcomes: dict[str, dict] = {}
        for entry in result.get("instruments") or []:
            try:
                outcomes[_uuid(entry["instrument_id"])] = entry
            except (KeyError, TypeError, ValueError):
                continue
        progress = 0
        untried: list[dict] = []
        unknown: dict | None = None
        for item in chunk:
            iid = item["instrument_id"]
            entry = outcomes.get(iid)
            if entry is None or entry.get("status") == "stopped":
                untried.append(item)  # never finished in this call
                continue
            status, code = _code(entry.get("status")) or "NONE", _code(entry.get("code"))
            self.final[iid] = {"status": status, "code": code, "rows": int(entry.get("rows") or 0)}
            if status in ("committed", "validated"):
                progress += 1
            elif status == "skipped" and code == STALE:
                self.stale.append(iid)
            elif status == "unknown":
                unknown = {
                    "instrument_id": iid,
                    "run_ids": [_uuid(run) for run in entry.get("run_ids") or []],
                }
        self.pass_progress += progress
        self.requests += int(result.get("requests") or 0)
        exit_status = int(result.get("exit_code") or 0)
        code = _code(result.get("code"))
        line = {
            "event": "batch",
            "pass": pass_no,
            "batch": self.batches,
            "reconcile": planned.reconcile,
            "plan_sha256": planned.sha256,
            "size": len(chunk),
            "rows": sum(len(item["rows"]) for item in chunk),
            "exit": exit_status,
            "status": _code(result.get("status")),
            "code": code,
            "committed": sum(1 for o in outcomes.values() if o.get("status") == "committed"),
            "changed_rows": int(result.get("changed_rows") or 0),
            "validated_rows": int(result.get("validated_rows") or 0),
            "residual_counts": _codes(result.get("residual_counts")),
            "untried": len(untried),
            "requests": int(result.get("requests") or 0),
            "clock_skew_ms": result.get("clock_skew_ms"),
            "elapsed_s": self._elapsed(began),
            "total_elapsed_s": self._elapsed(),
        }
        if unknown is not None:
            line["commit_unknown"] = unknown  # reconcile these runs before a retry
        self.emit(line)
        if progress:
            self.failed_streak = 0
        if exit_status in STOP_EXITS or code in STOP_CODES or code in FATAL_CODES:
            self.queue.extendleft(reversed(untried))
            if exit_status == EXIT_LOCK_BUSY and self.wrote:
                exit_status = EXIT_INTERRUPTED  # lock stop after work
            if exit_status not in STOP_EXITS:
                exit_status = EXIT_FAILED
            raise DriverStop(code or "OPERATOR_STOP", exit_status)
        if exit_status == EXIT_OK:
            # A requeued chunk leads the queue: this batch was its retry.
            self.pending_batch_failure = None
        else:
            # run_repair gives no outcome for the instrument it failed on. Past
            # its preflight (``clock_skew_ms`` set) that is the first untried
            # one: retry it once, then record it failed. A failure before any
            # instrument blames none: the whole chunk is retried.
            if untried and "clock_skew_ms" in result:
                culprit = untried[0]
                self.attempts[culprit["instrument_id"]] += 1
                if self.attempts[culprit["instrument_id"]] >= MAX_INSTRUMENT_ATTEMPTS:
                    self.final[culprit["instrument_id"]] = {
                        "status": "failed", "code": code or "NONE", "rows": len(culprit["rows"])
                    }
                    untried = untried[1:]
            self.queue.extendleft(reversed(untried))
            # Requeued work is unresolved until its retry succeeds: a batch cap
            # or a stop that ends the run first must not read as clean. With
            # nothing requeued, the failure is already a recorded outcome.
            self.pending_batch_failure = (code or "NONE") if untried else None
            if not progress:
                self.failed_streak += 1
                if self.failed_streak >= self.config.max_failed_batches:
                    raise DriverStop("CONSECUTIVE_FAILED_BATCHES", EXIT_FAILED)

    def _batch_cap_reached(self) -> bool:
        return self.config.max_batches is not None and self.batches >= self.config.max_batches

    def run_pass(self, pass_no: int, segments: list[_Planned]) -> None:
        """Apply each segment's queue under its own plan, in order; what a cap
        or a stop leaves (this segment's queue, later segments) is remaining."""
        self.passes += 1
        self.stale = []
        self.pass_progress = 0
        self.deferred = sum(len(planned.queue) for planned in segments)
        for planned in segments:
            self.deferred -= len(planned.queue)
            self.queue = deque(planned.queue)
            while self.queue:
                if self._batch_cap_reached():
                    return
                self.apply(pass_no, planned, self.next_chunk(planned.limits))

    # -- run ---------------------------------------------------------------------
    def run(self) -> None:
        self.db.check()
        segments = self.make_plan(1, None)
        if self.config.dry_run:
            self.queue = deque(self.planned.queue)
            return
        self.run_pass(1, segments)
        # Re-plan ONCE, only after a pass that made progress, and retry only
        # the instruments whose plan went stale under them.
        if self.stale and self.pass_progress and not self._batch_cap_reached():
            self.run_pass(2, self.make_plan(2, set(self.stale)))

    def recount(self, stop: DriverStop | None) -> None:
        """Remaining bad rows, read-only, with the operator's own predicate."""
        if not self.plans or (stop is not None and stop.code == "INTERRUPTED"):
            return
        try:
            self.remaining_bad_rows = self.db.count_bad()
        except KeyboardInterrupt:
            return
        except Exception as exc:
            self.errors_by_code["RECOUNT_FAILED:" + type(exc).__name__] += 1

    def summary(self, stop: DriverStop | None) -> tuple[int, dict]:
        first = self.first or {}
        by_status: Counter[str] = Counter()
        rows_by_status: Counter[str] = Counter()
        skipped_rows: Counter[str] = Counter()
        failed_by_code: Counter[str] = Counter()
        for outcome in self.final.values():
            by_status[outcome["status"]] += 1
            rows_by_status[outcome["status"]] += outcome["rows"]
            if outcome["status"] == "skipped":
                skipped_rows[outcome["code"] or "NONE"] += outcome["rows"]
            elif outcome["status"] == "failed":
                failed_by_code[outcome["code"] or "NONE"] += 1
        remaining = len(self.queue) + self.deferred + self.beyond_cap
        clean = (
            not self.errors_by_code
            and self.pending_batch_failure is None
            and all(o["status"] in DONE for o in self.final.values())
        )
        if self.config.dry_run and stop is None:
            status, exit_status = "planned", EXIT_OK
        elif stop is not None:
            status, exit_status = "stopped", stop.exit_status
        else:
            status = "limit_reached" if remaining else "completed"
            exit_status = EXIT_OK if clean else EXIT_FAILED
        return exit_status, {
            "event": "summary",
            "driver": DRIVER_VERSION,
            "operator_version": repair.VERSION,
            "status": status,
            "exit": exit_status,
            "clean": clean,
            "stop_code": stop.code if stop else None,
            "dry_run": self.config.dry_run,
            "validate_only": self.config.validate_only,
            "plans": self.plans,
            "passes": self.passes,
            "batches": self.batches,
            "plan_sha256": self.planned.sha256 if self.planned else None,
            "max_requests": self.planned.limits.max_requests if self.planned else None,
            "instruments_planned": first.get("instruments", 0),
            "rows_planned": first.get("rows", 0),
            "eligible_instruments": first.get("eligible_instruments", 0),
            "not_eligible_rows_by_reason": _codes(first.get("residual_counts")),
            "submitted": len(self.submitted),
            "committed_instruments": by_status["committed"],
            "committed_rows": rows_by_status["committed"],
            "validated_instruments": by_status["validated"],
            "validated_rows": rows_by_status["validated"],
            "already_repaired": by_status["noop"],
            "skipped_instruments": by_status["skipped"],
            "skipped_rows_by_code": dict(sorted(skipped_rows.items())),
            "failed_instruments": by_status["failed"],
            "failed_by_code": dict(sorted(failed_by_code.items())),
            "unknown_instruments": by_status["unknown"],
            "remaining_instruments": remaining,
            "pending_batch_failure": self.pending_batch_failure,
            "requests_used": self.requests,
            "errors_by_code": dict(sorted(self.errors_by_code.items())),
            "remaining_bad_rows": self.remaining_bad_rows,
            "readiness_republish_required": False,
            "elapsed_s": self._elapsed(),
        }


def run_cohort(
    config: CohortConfig,
    *,
    dsn: str | None = None,
    database=None,
    client_factory=None,
    emit: Callable[[dict], None] = _emit,
    monotonic: Callable[[], float] = time.monotonic,
) -> tuple[int, dict]:
    """Plan once, apply in batches, re-plan stale items once; ``(exit, summary)``.

    ``database`` replaces the PostgreSQL seam (tests); otherwise ``dsn`` is
    required. The summary is returned, not emitted (the CLI prints it, the
    worker lane returns it as stats).
    """
    cohort = _Cohort(config, database, client_factory, emit, monotonic)
    try:
        config.validate()
        if database is None and not dsn:
            raise repair.RepairError("DSN_REQUIRED")
        if not config.dry_run and client_factory is None and not os.environ.get("TIINGO_API_KEY"):
            # Fail before an expensive plan: Tiingo rows need the key.
            raise repair.RepairError("PROVIDER_NOT_CONFIGURED")
    except repair.RepairError as exc:
        return cohort.summary(DriverStop(exc.code, exc.exit_code))
    cohort.db = database or PostgresDatabase(dsn, config.schema)
    stop: DriverStop | None = None
    try:
        cohort.run()
    except DriverStop as exc:
        stop = exc
    except repair.RepairError as exc:
        stop = DriverStop(exc.code, exc.exit_code)
    except KeyboardInterrupt:
        stop = DriverStop("INTERRUPTED", EXIT_INTERRUPTED)
    except psycopg.Error as exc:  # never str(exc): it may carry connection text
        stop = _psycopg_stop(exc)
    except Exception as exc:
        cohort.errors_by_code["DRIVER_EXCEPTION:" + type(exc).__name__] += 1
        stop = DriverStop("VALIDATION_OR_IO_ERROR", EXIT_FAILED)
    finally:
        close = getattr(cohort.client, "close", None)
        if close:
            with contextlib.suppress(Exception):
                close()
    try:
        cohort.recount(stop)
    finally:
        cohort.db.close()
    return cohort.summary(stop)


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--max-seconds", type=float, default=DEFAULT_MAX_SECONDS)
    parser.add_argument("--rate-per-second", type=float, default=DEFAULT_RATE_PER_SECOND)
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--max-instruments-total", type=int)
    parser.add_argument("--max-failed-batches", type=int, default=DEFAULT_MAX_FAILED_BATCHES)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--confirm", help=f"required unless --dry-run: {repair.CONFIRM_TOKEN}")
    parser.add_argument("--allow-database-url", action="store_true")
    return parser


def _database_url() -> str | None:
    if not os.environ.get("DATABASE_URL"):
        return None
    from src.db import resolve_dsn  # DATABASE_URL (+ TLS params), as run_worker

    return resolve_dsn()


def main(argv: Iterable[str] | None = None, *, client_factory=None) -> int:
    args = _parser().parse_args(None if argv is None else list(argv))
    config = CohortConfig(
        schema=args.schema,
        max_seconds=args.max_seconds,
        rate_per_second=args.rate_per_second,
        max_batches=args.max_batches,
        max_instruments_total=args.max_instruments_total,
        max_failed_batches=args.max_failed_batches,
        dry_run=args.dry_run,
        validate_only=args.validate_only,
    )
    if not args.dry_run and args.confirm != repair.CONFIRM_TOKEN:
        exit_status, summary = _Cohort(config, None, None, _emit, time.monotonic).summary(
            DriverStop("CONFIRMATION_REQUIRED", EXIT_FAILED)
        )
    else:
        allow = database_url_allowed(args.allow_database_url)
        dsn = operator_dsn(
            allow_database_url=allow, database_url=_database_url() if allow else None
        )
        with sigterm_as_interrupt():
            exit_status, summary = run_cohort(config, dsn=dsn, client_factory=client_factory)
    _emit(summary)
    return exit_status


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
