"""Cohort driver for the governed economic NAV rebase (w1-tiingo-adjusted-daily-v1).

Drives ``scripts/rebase_fund_nav_window.py`` IN-PROCESS and unchanged: every
plan and every apply is one ``main(argv)`` call of that operator, so its
guards, receipts, pinned budgets, mutex and sanitized result stay exactly as
designed. The driver only sequences invocations and reads their JSON results:

* **Plan**: one read-only plan over the readiness cohort (or ``--instrument-id``
  scope) with fixed per-batch budgets (batch = max-instruments = max-requests,
  default 20; ``--max-seconds``; ``--rate-per-second`` <= 2.5). Plan files go
  to a fresh work directory (the operator never overwrites one); the file's
  SHA256 must equal the reported ``plan_sha256``.
* **Apply**: the planned instruments in plan order, allowlists of <= batch
  size, each apply with the plan file and its SHA256 and the SAME budgets
  (the operator rejects any other limits as ``PLAN_LIMITS_MISMATCH``).
* **Stale plans**: ``PLAN_STALE`` at the batch level (pins moved before any
  instrument: due/closed session rolled, policy pointer republished, schema
  pins changed) re-plans the remaining instruments at once and continues. A
  per-instrument ``PLAN_STALE`` (NAV head, lifecycle, hold or pins moved under
  that instrument) puts it back for the next re-plan, at most
  ``MAX_STALE_REPLANS_PER_INSTRUMENT`` times. Re-plans are capped in total
  (``--max-replans``) and in a row without progress (``MAX_IDLE_REPLANS``).
* **Stops**: operator exit 3 (schema/access/dependency incompatible), 4 (lock
  busy) or 5 (budget, lock or interrupt after work), a provider rate limit,
  a configuration/driver error code, or ``--max-failed-batches`` consecutive
  batches without progress. A per-instrument failure is recorded and never
  retried. ``--max-batches`` / ``--max-instruments-total`` cap a canary run;
  ``--dry-run`` plans only.

Stdout: one sanitized JSON line per plan and per batch, then one summary line
(counts by code, instrument ids, plan hashes, run ids; never a DSN, URL,
payload or exception text). DSN: only ``NAV_READINESS_DATABASE_URL``, as the
operator reads it; ``DATABASE_URL`` is used only with ``--allow-database-url``
or ``NAV_REBASE_ALLOW_DATABASE_URL=1``, and only for this process.

Exit: 0 every planned instrument committed/already applied (or the requested
cap was reached cleanly), or dry run; 2 failed/unknown instruments, operator
bookkeeping errors, or a driver-level stop; 3/4/5 as the stopping operator
invocation (4 only when this run committed nothing, 5 otherwise). Publishing
readiness is NOT part of this command: re-run ``nav_current_daily_chain``.

Run from the repository root: ``python -m scripts.rebase_fund_nav_cohort``.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import signal
import sys
import tempfile
import threading
import time
import uuid
from collections import Counter, deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts import rebase_fund_nav_window as operator_cli
from src.workers import nav_economic_rebase as rebase

DRIVER_VERSION = "nav-rebase-cohort-v1"
DEFAULT_SCHEMA = "public"
DEFAULT_MAX_SECONDS = 600.0
DEFAULT_MAX_FAILED_BATCHES = 3
DEFAULT_MAX_REPLANS = 10
MAX_IDLE_REPLANS = 3
MAX_STALE_REPLANS_PER_INSTRUMENT = 2
LIST_CAP = 100  # instrument ids per list in one output line

DSN_ENV = "NAV_READINESS_DATABASE_URL"
ALLOW_DATABASE_URL_ENV = "NAV_REBASE_ALLOW_DATABASE_URL"

EXIT_OK = rebase.EXIT_OK
EXIT_FAILED = rebase.EXIT_FAILED
EXIT_INCOMPATIBLE = rebase.EXIT_INCOMPATIBLE
EXIT_LOCK_BUSY = rebase.EXIT_LOCK_BUSY
EXIT_INTERRUPTED = rebase.EXIT_INTERRUPTED

STALE = "PLAN_STALE"
# Another invocation cannot succeed after these: configuration or driver error.
FATAL_CODES = frozenset(
    {
        "PROVIDER_NOT_CONFIGURED",
        "DSN_REQUIRED",
        "SCHEMA_INVALID",
        "CONTRACT_UNSUPPORTED",
        "BUDGETS_REQUIRED",
        "BATCH_SIZE_INVALID",
        "MAX_INSTRUMENTS_INVALID",
        "MAX_REQUESTS_INVALID",
        "MAX_SECONDS_INVALID",
        "RATE_INVALID",
        "APPLY_REQUIRES_PLAN_HASH_AND_ALLOWLIST",
        "PLAN_FILE_EXISTS",
        "PLAN_FILE_INVALID",
        "PLAN_FILE_MISMATCH",
        "PLAN_VERSION_MISMATCH",
        "PLAN_HASH_MISMATCH",
        "PLAN_LIMITS_MISMATCH",
        "ALLOWLIST_INVALID",
        "OPERATOR_OUTPUT_INVALID",
        "OPERATOR_EXCEPTION",
        "UNSAFE_CODE",
    }
)
# The shared Tiingo account is throttling: stop instead of burning the fleet budget.
PROVIDER_STOP_CODES = frozenset({"PROVIDER_RATE_LIMITED"})
# Planned earlier, excluded by a re-plan for a reason that is still success.
RECONCILED_EXCLUSIONS = frozenset({"ALREADY_RECONCILED"})
DONE = ("committed", "committed_unverified", "already_applied")

_SAFE = operator_cli._SAFE  # the operator's own sanitized-code alphabet

Invoke = Callable[[list[str]], tuple[int, dict]]
Emit = Callable[[dict], None]


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


# ──────────────────────────────────────────────────────────────────────────────
# Configuration and operator invocation
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class CohortConfig:
    schema: str = DEFAULT_SCHEMA
    scope: tuple[str, ...] | None = None
    batch_size: int = rebase.MAX_BATCH
    max_seconds: float = DEFAULT_MAX_SECONDS
    rate_per_second: float = rebase.MAX_RATE_PER_SECOND
    max_batches: int | None = None
    max_instruments_total: int | None = None
    max_failed_batches: int = DEFAULT_MAX_FAILED_BATCHES
    max_replans: int = DEFAULT_MAX_REPLANS
    dry_run: bool = False
    work_dir: str | None = None

    def limits(self) -> rebase.RebaseLimits:
        """batch = max-instruments = max-requests: one request per instrument."""
        return rebase.RebaseLimits(
            batch_size=self.batch_size,
            max_instruments=self.batch_size,
            max_requests=self.batch_size,
            max_seconds=float(self.max_seconds),
            rate_per_second=float(self.rate_per_second),
        )

    def validate(self) -> CohortConfig:
        """Driver-level bounds; raises ``RebaseError`` with a sanitized code."""
        self.limits().validate()
        if self.max_batches is not None and self.max_batches < 1:
            raise rebase.RebaseError("MAX_BATCHES_INVALID")
        if self.max_instruments_total is not None and self.max_instruments_total < 1:
            raise rebase.RebaseError("MAX_INSTRUMENTS_TOTAL_INVALID")
        if self.max_failed_batches < 1:
            raise rebase.RebaseError("MAX_FAILED_BATCHES_INVALID")
        if self.max_replans < 0:
            raise rebase.RebaseError("MAX_REPLANS_INVALID")
        if self.scope is not None:
            try:
                [_uuid(v) for v in self.scope]
            except ValueError:
                raise rebase.RebaseError("SCOPE_INVALID") from None
            if not self.scope:
                raise rebase.RebaseError("SCOPE_INVALID")
        return self

    def operator_argv(self) -> list[str]:
        limits = self.limits().canonical()
        return [
            "--schema",
            self.schema,
            "--contract",
            rebase.CONTRACT_VERSION,
            "--batch-size",
            str(limits["batch_size"]),
            "--max-instruments",
            str(limits["max_instruments"]),
            "--max-requests",
            str(limits["max_requests"]),
            "--max-seconds",
            repr(limits["max_seconds"]),
            "--rate-per-second",
            repr(limits["rate_per_second"]),
        ]


def operator_invoker(client_factory=None) -> Invoke:
    """``argv -> (exit, result)`` through ``rebase_fund_nav_window.main``.

    The operator's single stdout JSON object is captured, never echoed; the
    driver re-emits only selected, sanitized fields.
    """

    def invoke(argv: list[str]) -> tuple[int, dict]:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            exit_status = operator_cli.main(argv, client_factory=client_factory)
        lines = buffer.getvalue().strip().splitlines()
        try:
            result = json.loads(lines[-1])
        except (IndexError, ValueError):
            result = None
        if not isinstance(result, dict):
            return EXIT_FAILED, {"status": "blocked", "code": "OPERATOR_OUTPUT_INVALID"}
        return int(exit_status), result

    return invoke


@contextlib.contextmanager
def operator_dsn(*, allow_database_url: bool, database_url: str | None) -> Iterator[None]:
    """Expose the DSN where the operator reads it, for this run only.

    ``NAV_READINESS_DATABASE_URL`` wins when present. Otherwise ``database_url``
    (the DATABASE_URL-derived DSN) is used only when explicitly allowed; with
    neither, the operator itself refuses with ``DSN_REQUIRED`` before any I/O.
    """
    if os.environ.get(DSN_ENV) or not (allow_database_url and database_url):
        yield
        return
    os.environ[DSN_ENV] = database_url
    try:
        yield
    finally:
        os.environ.pop(DSN_ENV, None)


@contextlib.contextmanager
def sigterm_as_interrupt() -> Iterator[None]:
    """Railway stops a container with SIGTERM: route it through the operator's
    KeyboardInterrupt path (in-flight instrument rolled back, run finalized,
    mutex released, truthful ``partial`` result)."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def interrupt(_signum, _frame):
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGTERM, interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def database_url_allowed(flag: bool = False) -> bool:
    return flag or os.environ.get(ALLOW_DATABASE_URL_ENV, "").strip() == "1"


# ──────────────────────────────────────────────────────────────────────────────
# The cohort loop
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class _Plan:
    index: int
    path: str
    sha256: str
    ids: tuple[str, ...]


class _Cohort:
    def __init__(
        self,
        config: CohortConfig,
        invoke: Invoke,
        emit: Emit,
        monotonic: Callable[[], float],
        work_dir: Path,
    ) -> None:
        self.config = config
        self.invoke = invoke
        self.emit = emit
        self.monotonic = monotonic
        self.started = monotonic()
        self.work_dir = work_dir
        self.base_argv = config.operator_argv()
        self.plan: _Plan | None = None
        self.plans = 0
        self.replans = 0
        self.idle_replans = 0
        self.batches = 0
        self.failed_streak = 0
        self.queue: deque[str] = deque()
        self.pending: list[str] = []  # per-instrument PLAN_STALE awaiting a re-plan
        self.stale_replans: Counter[str] = Counter()
        self.submitted: set[str] = set()
        self.planned_initial = 0
        self.totals: Counter[str] = Counter()
        self.failed_by_code: Counter[str] = Counter()
        self.excluded_by_code: Counter[str] = Counter()
        self.replan_excluded_by_code: Counter[str] = Counter()
        self.errors_by_code: Counter[str] = Counter()
        self.failed_ids: list[dict] = []
        self.unknown_ids: list[str] = []
        self.requests_used = 0
        self.republish = False
        self.run_ids: list[str] = []

    # -- operator calls ------------------------------------------------------
    def _call(self, argv: list[str]) -> tuple[int, dict]:
        try:
            return self.invoke(argv)
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # never str(exc): it may carry connection text
            self.errors_by_code["OPERATOR_EXCEPTION:" + type(exc).__name__] += 1
            return EXIT_FAILED, {"status": "blocked", "code": "OPERATOR_EXCEPTION"}

    def _elapsed(self) -> float:
        return round(self.monotonic() - self.started, 3)

    # -- plan ------------------------------------------------------------------
    def make_plan(self, scope: list[str] | None) -> None:
        index = self.plans
        self.plans += 1
        path = self.work_dir / f"plan-{index:03d}.json"
        argv = [*self.base_argv, "--plan-file", str(path)]
        for iid in scope or ():
            argv += ["--instrument-id", iid]
        began = self.monotonic()
        exit_status, result = self._call(argv)
        line: dict[str, Any] = {
            "event": "plan",
            "plan_index": index,
            "scope": "cohort" if scope is None else "instruments",
            "scope_size": None if scope is None else len(scope),
            "exit": exit_status,
            "status": _code(result.get("status")),
            "code": _code(result.get("code")),
            "plan_sha256": None,
            "planned": 0,
            "excluded_counts": {},
            "needs_counts": {},
            "elapsed_s": round(self.monotonic() - began, 3),
        }
        if exit_status != EXIT_OK or result.get("status") != "planned":
            self.emit(line)
            code = _code(result.get("code")) or "PLAN_FAILED"
            raise DriverStop(code, exit_status if exit_status != EXIT_OK else EXIT_FAILED)
        try:
            data = path.read_bytes()
            sha = hashlib.sha256(data).hexdigest()
            if sha != result.get("plan_sha256"):
                raise ValueError("plan file digest differs from the reported hash")
            manifest = json.loads(data.decode("utf-8"))
            items = manifest["instruments"]
            ids = tuple(_uuid(item["instrument_id"]) for item in items)
            excluded = Counter(_code(code) or "NONE" for _iid, code in manifest["excluded"])
            needs = Counter(
                "+".join(_code(need) or "NONE" for need in item["needs"]) for item in items
            )
        except (OSError, ValueError, KeyError, TypeError):
            self.emit({**line, "code": "PLAN_FILE_MISMATCH"})
            raise DriverStop("PLAN_FILE_MISMATCH", EXIT_FAILED) from None
        line.update(
            plan_sha256=sha,
            planned=len(ids),
            excluded_counts=dict(sorted(excluded.items())),
            needs_counts=dict(sorted(needs.items())),
        )
        self.emit(line)
        if self.plan is None:
            # First plan (cohort or explicit scope): exclusions = never planned.
            self.planned_initial = len(ids)
            self.excluded_by_code.update(excluded)
        else:
            # Planned before, dropped now (e.g. ALREADY_RECONCILED, NAV_STALE).
            self.replan_excluded_by_code.update(excluded)
        self.plan = _Plan(index, str(path), sha, ids)
        self.queue = deque(ids)
        self.pending = []

    def replan(self, scope: list[str]) -> None:
        self.replans += 1
        self.idle_replans += 1
        if self.replans > self.config.max_replans:
            raise DriverStop("REPLAN_LIMIT", EXIT_FAILED)
        if self.idle_replans > MAX_IDLE_REPLANS:
            raise DriverStop("REPLAN_NO_PROGRESS", EXIT_FAILED)
        unique = list(dict.fromkeys(scope))
        if unique:
            self.make_plan(unique)
        else:
            self.queue, self.pending = deque(), []

    # -- apply -------------------------------------------------------------------
    def next_chunk(self) -> list[str]:
        cap = self.config.max_instruments_total
        chunk: list[str] = []
        while self.queue and len(chunk) < self.config.batch_size:
            iid = self.queue[0]
            if (
                cap is not None
                and iid not in self.submitted
                and len(self.submitted) + sum(1 for c in chunk if c not in self.submitted)
                >= cap
            ):
                break
            chunk.append(self.queue.popleft())
        return chunk

    def apply(self, chunk: list[str]) -> None:
        assert self.plan is not None
        plan = self.plan
        argv = [
            *self.base_argv,
            "--mode",
            "apply",
            "--plan-file",
            plan.path,
            "--plan-sha256",
            plan.sha256,
        ]
        for iid in chunk:
            argv += ["--instrument-id", iid]
        self.submitted.update(chunk)
        self.batches += 1
        began = self.monotonic()
        try:
            exit_status, result = self._call(argv)
        except KeyboardInterrupt:
            self.queue.extendleft(reversed(chunk))  # report them as remaining
            raise
        outcomes: dict[str, dict] = {}
        for entry in result.get("instruments") or []:
            try:
                outcomes[_uuid(entry["instrument_id"])] = entry
            except (KeyError, TypeError, ValueError):
                continue
        progress = 0
        requeue: list[str] = []
        stale: list[str] = []
        batch_failed: list[dict] = []
        for iid in chunk:
            entry = outcomes.get(iid) or {}
            status = entry.get("status", "not_attempted")
            code = _code(entry.get("code"))
            if status in DONE:
                self.totals[status] += 1
                progress += 1
            elif status == "unknown":
                self.totals["unknown"] += 1
                self.unknown_ids.append(iid)
            elif status == "failed" and code == STALE:
                if self.stale_replans[iid] < MAX_STALE_REPLANS_PER_INSTRUMENT:
                    stale.append(iid)
                else:
                    batch_failed.append({"instrument_id": iid, "code": code})
            elif status == "failed":
                batch_failed.append({"instrument_id": iid, "code": code or "NONE"})
            else:  # not_attempted / lock_busy: never tried in this invocation
                requeue.append(iid)
        for failure in batch_failed:
            self.totals["failed"] += 1
            self.failed_by_code[failure["code"]] += 1
            self.failed_ids.append(failure)
        for error in result.get("errors") or []:
            self.errors_by_code[_code(error) or "NONE"] += 1
        self.requests_used += int(result.get("requests_used") or 0)
        self.republish = self.republish or bool(result.get("readiness_republish_required"))
        if result.get("run_id"):
            self.run_ids.append(_uuid(result["run_id"]))
        top_code = _code(result.get("code"))
        self.emit(
            {
                "event": "batch",
                "batch": self.batches,
                "plan_index": plan.index,
                "plan_sha256": plan.sha256,
                "size": len(chunk),
                "exit": exit_status,
                "status": _code(result.get("status")),
                "code": top_code,
                "run_id": result.get("run_id") and _uuid(result["run_id"]),
                **{
                    key: int(result.get(key) or 0)
                    for key in (
                        "committed",
                        "committed_unverified",
                        "already_applied",
                        "unknown",
                        "failed",
                        "not_attempted",
                        "requests_used",
                        "orphan_runs_recovered",
                    )
                },
                "reason_counts": _codes(result.get("reason_counts")),
                "errors": [_code(e) for e in result.get("errors") or []],
                "stale_for_replan": len(stale),
                "requeued": len(requeue),
                "failed_instruments": batch_failed[:LIST_CAP],
                "readiness_republish_required": bool(
                    result.get("readiness_republish_required")
                ),
                "elapsed_s": round(self.monotonic() - began, 3),
                "total_elapsed_s": self._elapsed(),
            }
        )
        if progress:
            self.failed_streak = 0
            self.idle_replans = 0
        self.pending.extend(stale)
        # Instruments the invocation never tried go back to the front, in order.
        self.queue.extendleft(reversed(requeue))
        if exit_status in (EXIT_INCOMPATIBLE, EXIT_LOCK_BUSY, EXIT_INTERRUPTED):
            if exit_status == EXIT_LOCK_BUSY and self.wrote:
                exit_status = EXIT_INTERRUPTED  # lock stop after work
            raise DriverStop(top_code or "OPERATOR_STOP", exit_status)
        if top_code in FATAL_CODES:
            raise DriverStop(top_code, EXIT_FAILED)
        if any(f["code"] in PROVIDER_STOP_CODES for f in batch_failed):
            raise DriverStop("PROVIDER_RATE_LIMITED", EXIT_INTERRUPTED)
        if top_code == STALE and not progress:
            # Pins moved before any instrument of this plan could be applied.
            self.replan_remaining()
            return
        if not progress and exit_status != EXIT_OK:
            self.failed_streak += 1
            if self.failed_streak >= self.config.max_failed_batches:
                raise DriverStop("CONSECUTIVE_FAILED_BATCHES", EXIT_FAILED)

    @property
    def wrote(self) -> bool:
        return bool(
            self.totals["committed"]
            + self.totals["committed_unverified"]
            + self.totals["unknown"]
        )

    def replan_remaining(self) -> None:
        scope = [*self.queue, *self.pending]
        for iid in self.pending:
            self.stale_replans[iid] += 1
        self.replan(scope)

    # -- run ---------------------------------------------------------------------
    def run(self) -> None:
        self.make_plan(list(self.config.scope) if self.config.scope else None)
        if self.config.dry_run:
            return
        while True:
            if not self.queue:
                if not self.pending:
                    return
                self.replan_remaining()
                continue
            if self.config.max_batches is not None and self.batches >= self.config.max_batches:
                return
            chunk = self.next_chunk()
            if not chunk:
                return  # --max-instruments-total reached
            self.apply(chunk)

    def summary(self, stop: DriverStop | None) -> tuple[int, dict]:
        remaining = len(dict.fromkeys([*self.queue, *self.pending]))
        dropped = {
            code: n
            for code, n in self.replan_excluded_by_code.items()
            if code not in RECONCILED_EXCLUSIONS
        }
        clean = not (
            self.totals["failed"]
            or self.totals["unknown"]
            or self.errors_by_code
            or dropped
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
            "contract_version": rebase.CONTRACT_VERSION,
            "status": status,
            "exit": exit_status,
            "clean": clean,
            "stop_code": stop.code if stop else None,
            "dry_run": self.config.dry_run,
            "plans": self.plans,
            "replans": self.replans,
            "batches": self.batches,
            "last_plan_sha256": self.plan.sha256 if self.plan else None,
            "planned_initial": self.planned_initial,
            "submitted": len(self.submitted),
            "committed": self.totals["committed"],
            "committed_unverified": self.totals["committed_unverified"],
            "already_applied": self.totals["already_applied"],
            "unknown": self.totals["unknown"],
            "failed": self.totals["failed"],
            "failed_by_code": dict(sorted(self.failed_by_code.items())),
            "not_planned_by_code": dict(sorted(self.excluded_by_code.items())),
            "replan_excluded_by_code": dict(sorted(self.replan_excluded_by_code.items())),
            "remaining": remaining,
            "requests_used": self.requests_used,
            "errors_by_code": dict(sorted(self.errors_by_code.items())),
            "failed_instruments": self.failed_ids[:LIST_CAP],
            "unknown_instruments": self.unknown_ids[:LIST_CAP],
            "run_ids": self.run_ids[-LIST_CAP:],
            "readiness_republish_required": self.republish,
            "elapsed_s": self._elapsed(),
        }


def run_cohort(
    config: CohortConfig,
    *,
    client_factory=None,
    invoke: Invoke | None = None,
    emit: Emit = _emit,
    monotonic: Callable[[], float] = time.monotonic,
) -> tuple[int, dict]:
    """Plan once, apply in batches, re-plan stale pins; return ``(exit, summary)``.

    Emits one line per plan and per batch through ``emit``; the summary is
    returned, not emitted (the CLI prints it, the worker lane returns it).
    """
    try:
        config.validate()
    except rebase.RebaseError as exc:
        return _refused(config, exc.code, emit, monotonic)
    if not config.dry_run and client_factory is None and invoke is None:
        # Fail before an expensive cohort plan: the real client needs the key.
        if not os.environ.get("TIINGO_API_KEY"):
            return _refused(config, "PROVIDER_NOT_CONFIGURED", emit, monotonic)
    invoke = invoke or operator_invoker(client_factory)
    holder = (
        contextlib.nullcontext(config.work_dir)
        if config.work_dir
        else tempfile.TemporaryDirectory(prefix="nav-rebase-cohort-")
    )
    with holder as directory:
        work_dir = Path(directory)
        work_dir.mkdir(parents=True, exist_ok=True)
        cohort = _Cohort(config, invoke, emit, monotonic, work_dir)
        stop: DriverStop | None = None
        try:
            cohort.run()
        except DriverStop as exc:
            stop = exc
        except KeyboardInterrupt:
            stop = DriverStop("INTERRUPTED", EXIT_INTERRUPTED)
        return cohort.summary(stop)


def _refused(config, code, emit, monotonic) -> tuple[int, dict]:
    cohort = _Cohort(config, lambda argv: (EXIT_FAILED, {}), emit, monotonic, Path("."))
    return cohort.summary(DriverStop(code, EXIT_FAILED))


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--instrument-id", action="append", default=[])
    parser.add_argument("--batch-size", type=int, default=rebase.MAX_BATCH)
    parser.add_argument("--max-seconds", type=float, default=DEFAULT_MAX_SECONDS)
    parser.add_argument(
        "--rate-per-second", type=float, default=rebase.MAX_RATE_PER_SECOND
    )
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--max-instruments-total", type=int)
    parser.add_argument(
        "--max-failed-batches", type=int, default=DEFAULT_MAX_FAILED_BATCHES
    )
    parser.add_argument("--max-replans", type=int, default=DEFAULT_MAX_REPLANS)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--work-dir")
    parser.add_argument("--allow-database-url", action="store_true")
    return parser


def config_from_args(args: argparse.Namespace) -> CohortConfig:
    return CohortConfig(
        schema=args.schema,
        scope=tuple(args.instrument_id) if args.instrument_id else None,
        batch_size=args.batch_size,
        max_seconds=args.max_seconds,
        rate_per_second=args.rate_per_second,
        max_batches=args.max_batches,
        max_instruments_total=args.max_instruments_total,
        max_failed_batches=args.max_failed_batches,
        max_replans=args.max_replans,
        dry_run=args.dry_run,
        work_dir=args.work_dir,
    )


def _database_url() -> str | None:
    if not os.environ.get("DATABASE_URL"):
        return None
    from src.db import resolve_dsn  # DATABASE_URL (+ TLS params), as run_worker

    return resolve_dsn()


def main(argv: Iterable[str] | None = None, *, client_factory=None) -> int:
    args = _parser().parse_args(None if argv is None else list(argv))
    config = config_from_args(args)
    allow = database_url_allowed(args.allow_database_url)
    dsn = operator_dsn(
        allow_database_url=allow, database_url=_database_url() if allow else None
    )
    with dsn, sigterm_as_interrupt():
        exit_status, summary = run_cohort(config, client_factory=client_factory)
    _emit(summary)
    return exit_status


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
