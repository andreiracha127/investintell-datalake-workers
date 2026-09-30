"""Governed economic NAV rebase (N1): one adjusted Tiingo full-window snapshot.

Library for ``scripts/rebase_fund_nav_window.py``; never scheduled (no cron,
no ``run_worker`` lane). A rebase is new proof from a *current* adjusted
snapshot fetched once per instrument, not a point-in-time reconstruction nor an
attestation of legacy history:

* ``build_rebase_plan`` (read-only, no HTTP) inventories the readiness cohort
  and pins policy, due/closed sessions, the exact 401-session grid, per
  instrument lifecycle evidence, NAV head and active reexpression events, the
  fetch window and the budgets in a canonical manifest (SHA256).
* ``validate_adjusted_snapshot`` (pure) accepts only a complete, adjusted,
  finite, positive, duplicate-free, in-window, session-aligned series that
  needs no repair. No fallback provider, no sanitizer interpolation.
* ``apply_rebase_instrument`` runs after the fetch, in ONE transaction per
  instrument: INGESTION -> READINESS transaction locks, revalidation of every
  pin, success attempt + levels + returns + row evidence for every reconciled
  date + receipt + RESOLVED events, verified again by deferred DB triggers.
* ``run_rebase`` holds the operator mutex (session lock) for the whole batch,
  recovers only earlier ``operation='rebase'`` runs, enforces the pinned
  budgets and reports a truthful, sanitized per-instrument result. Committed
  instruments stay committed when a later one fails: a batch is not atomic.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import math
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

from src.db import (
    LOCK_FUND_NAV_READINESS,
    LOCK_INSTRUMENT_INGESTION,
    LOCK_NAV_ECONOMIC_REBASE,
)
from src.workers._nav_policy import (
    ENDPOINTS,
    PROFILE,
    PROVIDER_CONTRACT_VERSION,
    PolicyUnavailable,
    canonical_digest,
    level_evidence_digest,
    resolve_policy_and_grid,
)
from src.workers._nav_sanitize import sanitize_nav_series
from src.workers._tiingo import (
    DEFAULT_RATE_PER_S,
    NavFetchResult,
    NavObservation,
    TiingoBudgetExceeded,
    TiingoDeadlineExceeded,
)
from src.workers.fund_nav_readiness import _per_date_lineage
from src.workers.instrument_ingestion import (
    NavWriteResult,
    _insert_row_evidence_tx,
    _numeric6,
    _recover_orphan_runs,
    _write_instrument_nav_tx,
)

CONTRACT_VERSION = PROVIDER_CONTRACT_VERSION  # 'w1-tiingo-adjusted-daily-v1'
PLAN_VERSION = "nav-rebase-plan-v1"
SNAPSHOT_VERSION = "nav-rebase-snapshot-v1"
PROVIDER = "tiingo"
MAX_BATCH = 20
MAX_RATE_PER_SECOND = DEFAULT_RATE_PER_S  # never faster than the shared Tiingo pacing

EXIT_OK = 0
EXIT_FAILED = 2
EXIT_INCOMPATIBLE = 3
EXIT_LOCK_BUSY = 4
EXIT_INTERRUPTED = 5

LOCAL_FAILURE_CODES = frozenset(
    {
        "LOCK_BUSY",
        "PLAN_STALE",
        "DATABASE_ERROR",
        "HOLD_SCOPE_NOT_COVERED",
    }
)
RETRYABLE_CODES = frozenset(
    {
        "PROVIDER_RATE_LIMITED",
        "PROVIDER_TRANSIENT_ERROR",
        "LOCK_BUSY",
        "BUDGET_EXHAUSTED",
        "DATABASE_ERROR",
    }
)
_PROVIDER_FAILURE_STATUS = {
    "not_found": "not_found",
    "empty": "empty",
    "success_no_new": "empty",
    "rate_limited": "rate_limited",
    "transient_error": "transient_error",
    "invalid_payload": "invalid_payload",
    "not_configured": "not_configured",
}


class RebaseError(Exception):
    """Sanitized, bounded failure of one instrument or of the whole invocation."""

    def __init__(self, code: str, *, retryable: bool | None = None):
        super().__init__(code)
        self.code = code
        self.retryable = code in RETRYABLE_CODES if retryable is None else retryable


class RebaseLockBusy(RebaseError):
    def __init__(self) -> None:
        super().__init__("LOCK_BUSY", retryable=True)


class RebaseDeadline(RebaseError):
    """The pinned wall-clock budget expired before any DML; nothing written."""

    def __init__(self) -> None:
        super().__init__("BUDGET_EXHAUSTED", retryable=True)


class RebaseCommitUnknown(Exception):
    """COMMIT acknowledgement lost: the instrument may be committed."""

    def __init__(self, instrument_id: str, receipt_id: str) -> None:
        super().__init__("COMMIT_UNKNOWN")
        self.instrument_id = instrument_id
        self.receipt_id = receipt_id


def _rollback_quietly(conn) -> None:
    """Best-effort rollback that never masks the original error."""
    try:
        conn.rollback()
    except psycopg.Error:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Plan
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RebaseLimits:
    batch_size: int
    max_instruments: int
    max_requests: int
    max_seconds: float
    rate_per_second: float

    def validate(self) -> RebaseLimits:
        if not 1 <= self.batch_size <= MAX_BATCH:
            raise RebaseError("BATCH_SIZE_INVALID")
        if not 1 <= self.max_instruments <= self.batch_size:
            raise RebaseError("MAX_INSTRUMENTS_INVALID")
        if not 0 <= self.max_requests <= self.max_instruments:
            raise RebaseError("MAX_REQUESTS_INVALID")
        if not (math.isfinite(self.max_seconds) and self.max_seconds > 0):
            raise RebaseError("MAX_SECONDS_INVALID")
        if not (
            math.isfinite(self.rate_per_second)
            and 0 < self.rate_per_second <= MAX_RATE_PER_SECOND
        ):
            raise RebaseError("RATE_INVALID")
        return self

    def canonical(self) -> dict:
        return {
            "batch_size": self.batch_size,
            "max_instruments": self.max_instruments,
            "max_requests": self.max_requests,
            "max_seconds": float(self.max_seconds),
            "rate_per_second": float(self.rate_per_second),
        }


@dataclass(frozen=True)
class PlanItem:
    instrument_id: str
    ticker: str
    currency: str
    lifecycle_evidence_id: str
    revision_head: int
    active_event_ids: tuple[int, ...]
    needs: tuple[str, ...]
    window_start: dt.date
    window_end: dt.date
    session_count: int
    sessions_digest: str

    def canonical(self) -> dict:
        return {
            "instrument_id": self.instrument_id,
            "ticker": self.ticker,
            "provider": PROVIDER,
            "currency": self.currency,
            "lifecycle_evidence_id": self.lifecycle_evidence_id,
            "revision_head": self.revision_head,
            "active_event_ids": list(self.active_event_ids),
            "needs": list(self.needs),
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "session_count": self.session_count,
            "sessions_digest": self.sessions_digest,
        }

    @classmethod
    def from_canonical(cls, value: Mapping[str, Any]) -> PlanItem:
        return cls(
            instrument_id=str(uuid.UUID(value["instrument_id"])),
            ticker=value["ticker"],
            currency=value["currency"],
            lifecycle_evidence_id=str(uuid.UUID(value["lifecycle_evidence_id"])),
            revision_head=int(value["revision_head"]),
            active_event_ids=tuple(int(v) for v in value["active_event_ids"]),
            needs=tuple(value["needs"]),
            window_start=dt.date.fromisoformat(value["window_start"]),
            window_end=dt.date.fromisoformat(value["window_end"]),
            session_count=int(value["session_count"]),
            sessions_digest=value["sessions_digest"],
        )


@dataclass(frozen=True)
class RebasePlan:
    manifest: dict
    sha256: str
    items: tuple[PlanItem, ...]

    @property
    def limits(self) -> RebaseLimits:
        return RebaseLimits(**self.manifest["limits"])

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> RebasePlan:
        """Rebuild from a saved manifest; its SHA256 is recomputed, never trusted."""
        if (
            manifest.get("plan_version") != PLAN_VERSION
            or manifest.get("contract_version") != CONTRACT_VERSION
        ):
            raise RebaseError("PLAN_VERSION_MISMATCH")
        items = tuple(PlanItem.from_canonical(v) for v in manifest["instruments"])
        return cls(dict(manifest), canonical_digest(manifest), items)

    def item(self, instrument_id: str) -> PlanItem | None:
        for item in self.items:
            if item.instrument_id == instrument_id:
                return item
        return None


def plan_bytes(plan: RebasePlan) -> bytes:
    """Canonical manifest bytes whose SHA256 is ``plan.sha256``."""
    return json.dumps(
        plan.manifest,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")


def _latest_lifecycle(cur, iid: str, policy: Mapping, at: dt.datetime) -> dict | None:
    cur.execute(
        """SELECT * FROM nav_instrument_policy_evidence
           WHERE instrument_id=%s AND policy_id=%s AND policy_version=%s
             AND known_at <= %s AND effective_at <= %s AND recorded_at <= %s
           ORDER BY effective_at DESC, known_at DESC, recorded_at DESC, evidence_id DESC
           LIMIT 1""",
        (iid, policy["policy_id"], policy["policy_version"], at, at, at),
    )
    return cur.fetchone()


def _instrument_identity(cur, iid: str) -> dict | None:
    cur.execute(
        "SELECT ticker, currency FROM instruments_universe WHERE instrument_id=%s",
        (iid,),
    )
    return cur.fetchone()


def _head(cur, iid: str) -> int:
    cur.execute(
        "SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s", (iid,)
    )
    row = cur.fetchone()
    return int(row["revision_id"]) if row else 0


def _relevant_events(cur, iid: str, grid_start: dt.date) -> list[dict]:
    """Active holds that touch the due grid or later; wholly older ones are
    ledger-only and never block (preserved, never resolved here)."""
    cur.execute(
        """SELECT event_id, first_changed_date, last_changed_date
           FROM fund_nav_reexpression_holds
           WHERE instrument_id=%s AND last_changed_date >= %s ORDER BY event_id""",
        (iid, grid_start),
    )
    return cur.fetchall()


def _sessions(cur, policy: Mapping, start: dt.date, end: dt.date) -> list[dict]:
    cur.execute(
        """SELECT session_date, nav_due_at FROM nav_valuation_schedules
           WHERE calendar_id=%s AND calendar_version=%s
             AND session_date BETWEEN %s AND %s ORDER BY session_date""",
        (policy["calendar_id"], policy["calendar_version"], start, end),
    )
    return cur.fetchall()


def _sessions_digest(sessions: Iterable[Mapping]) -> str:
    return canonical_digest(
        [
            [
                s["session_date"].isoformat(),
                s["nav_due_at"].astimezone(dt.timezone.utc).isoformat(),
            ]
            for s in sessions
        ]
    )


def _policy_pins(policy: Mapping, pointer_published_at: dt.datetime | None) -> dict:
    return {
        "policy_id": policy["policy_id"],
        "policy_version": policy["policy_version"],
        "policy_hash": policy["policy_hash"],
        "calendar_id": policy["calendar_id"],
        "calendar_version": policy["calendar_version"],
        "calendar_source": policy["calendar_source"],
        "modeling_currency": policy["modeling_currency"],
        "required_nav_kind": policy["required_nav_kind"],
        "published_at": policy["published_at"].astimezone(dt.timezone.utc).isoformat(),
        "pointer_published_at": (
            pointer_published_at.astimezone(dt.timezone.utc).isoformat()
            if pointer_published_at
            else None
        ),
    }


def _current_pins(
    conn, decision_at: dt.datetime
) -> tuple[dict, list[dt.date], dt.date]:
    try:
        policy, grid, closed = resolve_policy_and_grid(conn, decision_at)
    except PolicyUnavailable as exc:
        raise RebaseError(exc.reason) from None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT published_at FROM nav_policy_current WHERE readiness_profile=%s",
            (PROFILE,),
        )
        pointer = cur.fetchone()
    return _policy_pins(policy, pointer[0] if pointer else None), grid, closed


def build_rebase_plan(
    conn,
    scope: Iterable[str] | None,
    limits: RebaseLimits,
    *,
    schema: str,
    schema_pins: Mapping[str, Any],
) -> RebasePlan:
    """Read-only inventory; call inside a READ ONLY transaction. No HTTP.

    Cohort: the readiness cohort (``funds_profile_mv``), optionally narrowed to
    ``scope``. An instrument is planned only with current published policy, the
    latest lifecycle evidence ACTIVE/daily with identity, return basis and
    currency verified, a ticker, the policy modeling currency (never a default),
    current NAV, and a need: missing per-date lineage or an active hold on the
    grid. Everything else is listed as excluded with a bounded reason code.
    """
    limits.validate()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT clock_timestamp() AS now")
        decision_at = cur.fetchone()["now"]
    policy_pins, grid, closed = _current_pins(conn, decision_at)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT to_regclass('funds_profile_mv') IS NOT NULL AS present")
        if not cur.fetchone()["present"]:
            raise RebaseError("COHORT_UNAVAILABLE")
        cur.execute("SELECT instrument_id FROM funds_profile_mv ORDER BY instrument_id")
        cohort = {str(row["instrument_id"]) for row in cur.fetchall()}
        cur.execute("SELECT to_regclass('instruments_universe') IS NOT NULL AS present")
        if not cur.fetchone()["present"]:
            raise RebaseError("INSTRUMENT_CATALOG_UNAVAILABLE")
        if scope is None:
            candidates = sorted(cohort)
        else:
            candidates = sorted({str(uuid.UUID(str(v))) for v in scope})
        policy = {**policy_pins}
        items: list[PlanItem] = []
        excluded: list[list[str]] = []
        for iid in candidates:
            code = None
            lifecycle = identity = None
            if iid not in cohort:
                code = "NOT_IN_COHORT"
            else:
                lifecycle = _latest_lifecycle(cur, iid, policy, decision_at)
                identity = _instrument_identity(cur, iid)
            if code is None:
                if lifecycle is None:
                    code = "LIFECYCLE_MISSING"
                elif lifecycle["fund_status"] != "ACTIVE":
                    code = "FUND_NOT_ACTIVE"
                elif lifecycle["valuation_frequency"] != "daily":
                    code = "NOT_DAILY"
                elif not lifecycle["identity_verified"]:
                    code = "IDENTITY_UNVERIFIED"
                elif not lifecycle["return_basis_verified"]:
                    code = "RETURN_BASIS_UNVERIFIED"
                elif not lifecycle["currency_verified"]:
                    code = "CURRENCY_UNVERIFIED"
                elif identity is None:
                    code = "INSTRUMENT_UNKNOWN"
                elif not (identity["ticker"] or "").strip():
                    code = "TICKER_MISSING"
                elif identity["currency"] is None:
                    code = "CURRENCY_UNKNOWN"
                elif identity["currency"] != policy["modeling_currency"]:
                    code = "CURRENCY_UNSUPPORTED"
            if code is None:
                cur.execute(
                    "SELECT max(nav_date) AS last FROM nav_timeseries WHERE instrument_id=%s",
                    (iid,),
                )
                last = cur.fetchone()["last"]
                if last is None or last < grid[-1]:
                    code = "NAV_STALE"
                elif last > closed:
                    code = "NAV_BEYOND_CLOSED"
            if code is not None:
                excluded.append([iid, code])
                continue
            verified, _lineage = _per_date_lineage(cur, iid, grid, closed, decision_at)
            events = _relevant_events(cur, iid, grid[0])
            needs = tuple(
                n
                for n, flag in (
                    ("LINEAGE_MISSING", not verified),
                    ("ACTIVE_HOLD", bool(events)),
                )
                if flag
            )
            if not needs:
                excluded.append([iid, "ALREADY_RECONCILED"])
                continue
            start = min([grid[0], *(e["first_changed_date"] for e in events)])
            cur.execute(
                """SELECT coverage_start FROM nav_policy_versions
                   WHERE policy_id=%s AND policy_version=%s""",
                (policy["policy_id"], policy["policy_version"]),
            )
            coverage_start = cur.fetchone()["coverage_start"]
            if start < coverage_start or any(
                e["last_changed_date"] > closed for e in events
            ):
                excluded.append([iid, "HOLD_SCOPE_NOT_COVERED"])
                continue
            sessions = _sessions(cur, policy, start, closed)
            items.append(
                PlanItem(
                    instrument_id=iid,
                    ticker=identity["ticker"].strip().upper(),
                    currency=identity["currency"],
                    lifecycle_evidence_id=str(lifecycle["evidence_id"]),
                    revision_head=_head(cur, iid),
                    active_event_ids=tuple(int(e["event_id"]) for e in events),
                    needs=needs,
                    window_start=start,
                    window_end=closed,
                    session_count=len(sessions),
                    sessions_digest=_sessions_digest(sessions),
                )
            )
    manifest = {
        "plan_version": PLAN_VERSION,
        "contract_version": CONTRACT_VERSION,
        "provider": PROVIDER,
        "schema": schema,
        "schema_pins": dict(schema_pins),
        "policy": policy_pins,
        "due_session": grid[-1].isoformat(),
        "closed_session": closed.isoformat(),
        "grid": {
            "start": grid[0].isoformat(),
            "end": grid[-1].isoformat(),
            "count": len(grid),
            "digest": canonical_digest([d.isoformat() for d in grid]),
        },
        "limits": limits.canonical(),
        "scope": None if scope is None else candidates,
        "instruments": [item.canonical() for item in items],
        "excluded": excluded,
    }
    return RebasePlan(manifest, canonical_digest(manifest), tuple(items))


# ──────────────────────────────────────────────────────────────────────────────
# Snapshot validation (pure)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ValidatedSnapshot:
    instrument_id: str
    observations: tuple[NavObservation, ...]
    provider_snapshot_sha256: str
    attempted_at: dt.datetime
    finished_at: dt.datetime


def provider_snapshot_digest(
    item: PlanItem, observations: Iterable[NavObservation]
) -> str:
    """Canonical digest of the observations USED, not of the HTTP bytes."""
    return canonical_digest(
        {
            "version": SNAPSHOT_VERSION,
            "contract": CONTRACT_VERSION,
            "provider": PROVIDER,
            "ticker": item.ticker,
            "window": [item.window_start.isoformat(), item.window_end.isoformat()],
            "observations": [
                [o.date.isoformat(), str(_numeric6(o.price)), o.kind]
                for o in observations
            ],
        }
    )


def validate_adjusted_snapshot(
    plan_item: PlanItem,
    fetch: NavFetchResult,
    *,
    sessions: Iterable[dt.date],
    due_sessions: Iterable[dt.date],
) -> ValidatedSnapshot:
    """Accept exactly one complete adjusted snapshot of the planned window.

    ``sessions`` are the pinned calendar sessions of the window and
    ``due_sessions`` the ones already due. Any raw/mixed/unknown kind,
    non-finite or non-positive level, duplicate, out-of-window or non-session
    date, missing due session, or a series the sanitizer would repair fails the
    whole instrument: nothing is truncated, interpolated or spliced.
    """
    if fetch.status not in ("success_new", "success_no_new"):
        raise RebaseError(
            "PROVIDER_"
            + _PROVIDER_FAILURE_STATUS.get(fetch.status, "invalid_payload").upper()
        )
    if not fetch.observations:
        raise RebaseError("PROVIDER_EMPTY")
    if fetch.attempted_at is None or fetch.finished_at is None:
        raise RebaseError("PROVIDER_INVALID_PAYLOAD")
    observations = sorted(fetch.observations, key=lambda o: o.date)
    dates = [o.date for o in observations]
    if len(set(dates)) != len(dates):
        raise RebaseError("DUPLICATE_DATE")
    if any(o.kind != "adjusted" for o in observations):
        raise RebaseError("RAW_OR_MIXED_KIND")
    if any(
        o.price is None or not math.isfinite(o.price) or o.price <= 0
        for o in observations
    ):
        raise RebaseError("NONFINITE_OR_NONPOSITIVE")
    if any(not plan_item.window_start <= d <= plan_item.window_end for d in dates):
        raise RebaseError("OUT_OF_WINDOW")
    session_set = set(sessions)
    if any(d not in session_set for d in dates):
        raise RebaseError("NON_SESSION_DATE")
    if not set(due_sessions) <= set(dates):
        raise RebaseError("GRID_INCOMPLETE")
    clean = sanitize_nav_series([(o.date, o.price) for o in observations])
    if clean.dead or clean.scale_step or any(clean.repaired):
        raise RebaseError("REPAIR_REQUIRED")
    if len(observations) < ENDPOINTS:
        raise RebaseError("GRID_INCOMPLETE")
    kept = tuple(observations)
    return ValidatedSnapshot(
        plan_item.instrument_id,
        kept,
        provider_snapshot_digest(plan_item, kept),
        fetch.attempted_at,
        fetch.finished_at,
    )


def build_rebase_rows(
    item: PlanItem, snapshot: ValidatedSnapshot, calendar: tuple[str, str, str]
) -> list[dict[str, Any]]:
    """Observed adjusted levels exactly as fetched: nav == source_nav, no repair.

    NULL legacy metadata becomes the provider's observed value, never a value
    computed from the stored series. Returns are recomputed by the writer
    (``mode='rebase'``) from persisted neighbours.
    """
    return [
        {
            "instrument_id": uuid.UUID(item.instrument_id),
            "nav_date": obs.date,
            "nav": round(obs.price, 6),
            "return_1d": None,
            "return_type": "log",
            "currency": item.currency,
            "source": PROVIDER,
            "source_nav": round(obs.price, 6),
            "source_nav_kind": "adjusted",
            "nav_repair_kind": "none",
            "calendar_id": calendar[0],
            "calendar_version": calendar[1],
            "calendar_source": calendar[2],
        }
        for obs in snapshot.observations
    ]


# ──────────────────────────────────────────────────────────────────────────────
# Apply (one instrument, one transaction)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class InstrumentRebaseResult:
    instrument_id: str
    status: str
    code: str | None = None
    receipt_id: str | None = None
    changed_level_rows: int = 0
    changed_return_rows: int = 0
    revision_count: int = 0
    resolved_events: list[int] = field(default_factory=list)
    retryable: bool = False

    def as_dict(self) -> dict:
        return {
            "instrument_id": self.instrument_id,
            "status": self.status,
            "code": self.code,
            "receipt_id": self.receipt_id,
            "changed_level_rows": self.changed_level_rows,
            "changed_return_rows": self.changed_return_rows,
            "revision_count": self.revision_count,
            "resolved_events": list(self.resolved_events),
        }


def _grid_json(dates: Iterable[dt.date]) -> str:
    return "[" + ",".join(f'"{d.isoformat()}"' for d in dates) + "]"


def _evidence_json(rows: Iterable[Mapping[str, Any]]) -> str:
    return (
        "["
        + ",".join(
            '["{}","{}"]'.format(
                row["nav_date"].isoformat(),
                level_evidence_digest(
                    row["nav_date"],
                    row["nav"],
                    row["source_nav"],
                    row["source"],
                    row["source_nav_kind"],
                    row["currency"],
                    row["nav_repair_kind"],
                ),
            )
            for row in sorted(rows, key=lambda r: r["nav_date"])
        )
        + "]"
    )


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _try_writer_locks(cur) -> bool:
    """INGESTION -> READINESS transaction locks, before any DML (never waits)."""
    for key in (LOCK_INSTRUMENT_INGESTION, LOCK_FUND_NAV_READINESS):
        cur.execute("SELECT pg_try_advisory_xact_lock(%s) AS got", (key,))
        if not cur.fetchone()["got"]:
            return False
    return True


def _pins_stale(
    plan: RebasePlan, pins: Mapping, grid: list[dt.date], closed: dt.date
) -> bool:
    """Every pinned policy fact, INCLUDING the pointer's server publication
    instant: a repoint V1->V2->V1 or a republication of the same target is a
    new pointer state, so a plan made before it is stale."""
    manifest = plan.manifest
    return (
        dict(pins) != manifest["policy"]
        or grid[-1].isoformat() != manifest["due_session"]
        or closed.isoformat() != manifest["closed_session"]
        or canonical_digest([d.isoformat() for d in grid]) != manifest["grid"]["digest"]
    )


def _revalidate(cur, conn, plan: RebasePlan, item: PlanItem, now: dt.datetime) -> None:
    pins, grid, closed = _current_pins(conn, now)
    manifest = plan.manifest
    if (
        _pins_stale(plan, pins, grid, closed)
        or grid[-1].isoformat() != manifest["due_session"]
        or closed.isoformat() != manifest["closed_session"]
        or canonical_digest([d.isoformat() for d in grid]) != manifest["grid"]["digest"]
    ):
        raise RebaseError("PLAN_STALE")
    identity = _instrument_identity(cur, item.instrument_id)
    lifecycle = _latest_lifecycle(cur, item.instrument_id, manifest["policy"], now)
    if (
        identity is None
        or (identity["ticker"] or "").strip().upper() != item.ticker
        or identity["currency"] != item.currency
        or lifecycle is None
        or str(lifecycle["evidence_id"]) != item.lifecycle_evidence_id
        or not (
            lifecycle["fund_status"] == "ACTIVE"
            and lifecycle["valuation_frequency"] == "daily"
            and lifecycle["identity_verified"]
            and lifecycle["return_basis_verified"]
            and lifecycle["currency_verified"]
        )
        or _head(cur, item.instrument_id) != item.revision_head
        or tuple(
            int(e["event_id"])
            for e in _relevant_events(cur, item.instrument_id, grid[0])
        )
        != item.active_event_ids
        or _sessions_digest(
            _sessions(cur, manifest["policy"], item.window_start, item.window_end)
        )
        != item.sessions_digest
    ):
        raise RebaseError("PLAN_STALE")


def apply_rebase_instrument(
    conn,
    plan: RebasePlan,
    plan_item: PlanItem,
    snapshot: ValidatedSnapshot,
    run_id: uuid.UUID,
    *,
    remaining: Callable[[], float] | None = None,
) -> InstrumentRebaseResult:
    """Reconcile one instrument in ONE transaction; commits or rolls back.

    The caller holds the operator mutex and has NO open transaction (the fetch
    happened before, outside any lock). Raises ``RebaseLockBusy`` (nothing
    written), ``RebaseDeadline`` (budget expired before the first DML; nothing
    written), ``RebaseError``/``psycopg.Error`` after a full rollback, or
    ``RebaseCommitUnknown`` when the COMMIT acknowledgement was lost (the
    outcome must then be reconciled by receipt, never assumed).
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise RuntimeError("rebase apply requires an idle connection")
    iid = plan_item.instrument_id
    policy = plan.manifest["policy"]
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SET LOCAL statement_timeout = '120s'")
            cur.execute("SET LOCAL lock_timeout = '2s'")
            if not _try_writer_locks(cur):
                raise RebaseLockBusy()
            cur.execute("SELECT clock_timestamp() AS now")
            now = cur.fetchone()["now"]
            _revalidate(cur, conn, plan, plan_item, now)
            fetched = {o.date for o in snapshot.observations}
            cur.execute(
                """SELECT nav_date, nav, source_nav, source, source_nav_kind, currency,
                          nav_repair_kind
                   FROM nav_timeseries WHERE instrument_id=%s
                     AND nav_date BETWEEN %s AND %s ORDER BY nav_date""",
                (iid, plan_item.window_start, plan_item.window_end),
            )
            stored = {row["nav_date"]: row for row in cur.fetchall()}
            if set(stored) - fetched:
                raise RebaseError("ORPHAN_STORED_DATE")
            sessions = _sessions(
                cur, policy, plan_item.window_start, plan_item.window_end
            )
            if (
                not {s["session_date"] for s in sessions if s["nav_due_at"] <= now}
                <= fetched
            ):
                raise RebaseError("GRID_INCOMPLETE")
            if remaining is not None and remaining() <= 0:
                raise RebaseDeadline()  # no DML after the budget expired
            rows = build_rebase_rows(
                plan_item,
                snapshot,
                (
                    policy["calendar_id"],
                    policy["calendar_version"],
                    policy["calendar_source"],
                ),
            )
            changes = any(
                row["nav_date"] not in stored
                or level_evidence_digest(
                    row["nav_date"],
                    *[
                        stored[row["nav_date"]][k]
                        for k in (
                            "nav",
                            "source_nav",
                            "source",
                            "source_nav_kind",
                            "currency",
                            "nav_repair_kind",
                        )
                    ],
                )
                != level_evidence_digest(
                    row["nav_date"],
                    row["nav"],
                    row["source_nav"],
                    row["source"],
                    row["source_nav_kind"],
                    row["currency"],
                    row["nav_repair_kind"],
                )
                for row in rows
            )
            cur.execute(
                """INSERT INTO nav_ingestion_attempts
                   (run_id, instrument_id, ticker, provider, requested_start, requested_end,
                    attempted_at, finished_at, status, newest_observed_date, row_count,
                    reason_code, commit_xid)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,pg_current_xact_id())""",
                (
                    run_id,
                    iid,
                    plan_item.ticker,
                    PROVIDER,
                    plan_item.window_start,
                    plan_item.window_end,
                    snapshot.attempted_at,
                    snapshot.finished_at,
                    "success_new" if changes else "success_no_new",
                    max(fetched),
                    len(rows),
                ),
            )
            before_head = plan_item.revision_head
            written: NavWriteResult = _write_instrument_nav_tx(
                conn, rows, run_id=run_id, provider=PROVIDER, mode="rebase"
            )
            after_head = _head(cur, iid)
            _insert_row_evidence_tx(
                conn,
                run_id=run_id,
                instrument_id=uuid.UUID(iid),
                provider=PROVIDER,
                rows=rows,
            )
            cur.execute(
                """SELECT session_date FROM nav_valuation_schedules
                   WHERE calendar_id=%s AND calendar_version=%s
                     AND session_date BETWEEN %s AND %s
                     AND nav_due_at <= clock_timestamp()
                   ORDER BY session_date""",
                (
                    policy["calendar_id"],
                    policy["calendar_version"],
                    plan_item.window_start,
                    plan_item.window_end,
                ),
            )
            due = [row["session_date"] for row in cur.fetchall()]
            if not set(due) <= fetched:
                raise RebaseError("GRID_INCOMPLETE")
            receipt_id = uuid.uuid4()
            cur.execute(
                """INSERT INTO nav_rebase_receipts
                   (receipt_id, run_id, instrument_id, provider, contract_version,
                    plan_sha256, policy_id, policy_version, policy_hash,
                    lifecycle_evidence_id, window_start, window_end, grid_digest,
                    provider_snapshot_sha256, row_evidence_digest, before_head,
                    after_head, observed_levels_count, changed_level_rows,
                    changed_return_rows, committed_at, commit_xid)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                           clock_timestamp(),pg_current_xact_id())""",
                (
                    receipt_id,
                    run_id,
                    iid,
                    PROVIDER,
                    CONTRACT_VERSION,
                    plan.sha256,
                    policy["policy_id"],
                    policy["policy_version"],
                    policy["policy_hash"],
                    plan_item.lifecycle_evidence_id,
                    plan_item.window_start,
                    plan_item.window_end,
                    _sha256_text(_grid_json(due)),
                    snapshot.provider_snapshot_sha256,
                    _sha256_text(_evidence_json(rows)),
                    before_head,
                    after_head,
                    len(rows),
                    written.changed_level_rows,
                    written.changed_return_rows,
                ),
            )
            # Resolve only events whose WHOLE interval lies inside the reconciled
            # window (including detections this rebase just opened). A grid-
            # relevant event crossing the window boundary blocks the instrument;
            # events wholly before the grid are ledger-only and stay untouched.
            cur.execute(
                """SELECT event_id, first_changed_date, last_changed_date
                   FROM fund_nav_reexpression_holds WHERE instrument_id=%s
                   ORDER BY event_id""",
                (iid,),
            )
            grid_start = dt.date.fromisoformat(plan.manifest["grid"]["start"])
            resolved: list[int] = []
            for event in cur.fetchall():
                inside = (
                    event["first_changed_date"] >= plan_item.window_start
                    and event["last_changed_date"] <= plan_item.window_end
                )
                if not inside:
                    if event["last_changed_date"] >= grid_start:
                        raise RebaseError("HOLD_SCOPE_NOT_COVERED")
                    continue
                cur.execute(
                    """INSERT INTO fund_nav_reexpression_events
                       (instrument_id, event_kind, first_changed_date, last_changed_date,
                        source_run_id, source_provider, revision_head, recorded_at,
                        reason_code, resolves_event_id, rebase_receipt_id)
                       VALUES (%s,'RESOLVED',%s,%s,%s,%s,0,clock_timestamp(),
                               'FULL_WINDOW_RECONCILED',%s,%s)""",
                    (
                        iid,
                        event["first_changed_date"],
                        event["last_changed_date"],
                        run_id,
                        PROVIDER,
                        event["event_id"],
                        receipt_id,
                    ),
                )
                resolved.append(int(event["event_id"]))
    except BaseException:
        _rollback_quietly(conn)
        raise
    try:
        conn.commit()
    except psycopg.Error as exc:
        if getattr(conn, "broken", False) or exc.sqlstate is None:
            # Connection lost around COMMIT: the server may or may not have
            # committed. Never report this as "no writes".
            raise RebaseCommitUnknown(iid, str(receipt_id)) from None
        _rollback_quietly(conn)
        raise  # the server refused the COMMIT: definitely rolled back
    return InstrumentRebaseResult(
        instrument_id=iid,
        status="committed",
        receipt_id=str(receipt_id),
        changed_level_rows=written.changed_level_rows,
        changed_return_rows=written.changed_return_rows,
        revision_count=written.revision_count,
        resolved_events=resolved,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Batch orchestration
# ──────────────────────────────────────────────────────────────────────────────
def _record_failure(
    conn, run_id: uuid.UUID, item: PlanItem, code: str, fetch: NavFetchResult | None
) -> None:
    """Terminal failure attempt without any NAV in its own transaction. The
    caller never lets an error here replace the instrument's own outcome."""
    provider_code = code.startswith("PROVIDER_")
    if provider_code:
        status = _PROVIDER_FAILURE_STATUS.get(
            code.removeprefix("PROVIDER_").lower(), "invalid_payload"
        )
    elif code in ("PLAN_STALE", "LOCK_BUSY", "DATABASE_ERROR"):
        status = "transient_error"
    else:
        status = "invalid_payload"
    # Only a provider outcome carries a provider instant (the fetch result or
    # the 429 breaker). Local operator conditions (lock, drift, DB, hold scope)
    # are audited without one, so they never outrank a real provider attempt
    # when readiness selects the latest attempt.
    now = dt.datetime.now(dt.timezone.utc)
    if code in LOCAL_FAILURE_CODES:
        attempted = finished = None
    elif fetch is not None:
        attempted, finished = fetch.attempted_at, fetch.finished_at
    else:
        attempted = finished = now if provider_code else None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO nav_ingestion_attempts
                   (run_id, instrument_id, ticker, provider, requested_start, requested_end,
                    attempted_at, finished_at, status, newest_observed_date, row_count,
                    reason_code, commit_xid)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,0,%s,pg_current_xact_id())""",
                (
                    run_id,
                    item.instrument_id,
                    item.ticker,
                    PROVIDER,
                    item.window_start,
                    item.window_end,
                    attempted,
                    finished,
                    status,
                    code[:48],
                ),
            )
        conn.commit()
    except BaseException:
        _rollback_quietly(conn)
        raise


def _receipt_state(cur, plan: RebasePlan, item: PlanItem) -> str | None:
    """``already_applied`` when this plan's receipt still describes the
    instrument (same head, no active event on the grid); ``stale`` when a
    receipt exists but data moved on (a new plan is required); else None."""
    cur.execute(
        """SELECT after_head FROM nav_rebase_receipts
           WHERE plan_sha256=%s AND instrument_id=%s""",
        (plan.sha256, item.instrument_id),
    )
    receipt = cur.fetchone()
    if receipt is None:
        return None
    grid_start = dt.date.fromisoformat(plan.manifest["grid"]["start"])
    if _head(cur, item.instrument_id) == receipt["after_head"] and not _relevant_events(
        cur, item.instrument_id, grid_start
    ):
        return "already_applied"
    return "stale"


def _reconcile_commit(
    reconnect, plan: RebasePlan, unknown: RebaseCommitUnknown
) -> dict | None:
    """Look the lost-ack instrument's receipt up on a fresh connection.

    Returns the receipt counts when it is committed; ``None`` when the outcome
    cannot be established (no reconnect, reconnect failure, or no receipt yet:
    the old backend may still be finishing its COMMIT). Never "zero writes".
    """
    if reconnect is None:
        return None
    try:
        with reconnect() as fresh:
            row = fresh.execute(
                """SELECT receipt_id::text, changed_level_rows, changed_return_rows
                   FROM nav_rebase_receipts
                   WHERE plan_sha256=%s AND instrument_id=%s AND receipt_id=%s""",
                (plan.sha256, unknown.instrument_id, unknown.receipt_id),
            ).fetchone()
            fresh.rollback()
    except psycopg.Error:
        return None
    if row is None:
        return None
    return {
        "receipt_id": row[0],
        "changed_level_rows": row[1],
        "changed_return_rows": row[2],
    }


class _Batch:
    """Accumulated, truthful state of one invocation. Every exit path after the
    mutex is taken reports from here, so a cleanup failure never erases
    confirmed outcomes, the run id or the republish requirement."""

    def __init__(self, result: dict, items: list[PlanItem]) -> None:
        self.result = result
        self.items = items
        self.outcomes: dict[str, InstrumentRebaseResult] = {}
        self.errors: list[str] = []
        self.run_id: uuid.UUID | None = None
        self.stop_code: str | None = None
        self.status: str | None = None
        self.code: str | None = None
        self.retryable: bool | None = None

    def rest(self, todo: list[PlanItem], start: int, code: str) -> None:
        for item in todo[start:]:
            self.outcomes.setdefault(
                item.instrument_id,
                InstrumentRebaseResult(
                    item.instrument_id, "not_attempted", code, retryable=True
                ),
            )

    def safe(self, code: str, fn: Callable[[], Any]) -> bool:
        """Run a bookkeeping step; a failure is recorded, never raised."""
        try:
            fn()
        except Exception:
            self.errors.append(code)
            return False
        return True

    def finish(self) -> dict:
        self.result["errors"] = list(self.errors)
        return _finish(
            self.result,
            status=self.status,
            code=self.code,
            retryable=self.retryable,
            outcomes=self.outcomes,
            not_attempted=[
                i for i in self.items if i.instrument_id not in self.outcomes
            ],
            stop_code=self.stop_code,
        )


def run_rebase(
    conn,
    plan: RebasePlan,
    *,
    instrument_ids: Iterable[str],
    limits: RebaseLimits,
    supplied_sha256: str,
    client: Any,
    monotonic: Callable[[], float] = time.monotonic,
    reconnect: Callable[[], Any] | None = None,
) -> dict:
    """Apply an explicit allowlist of a pinned plan and return the stable,
    sanitized result (see ``exit_code``). After the mutex is taken it never
    raises an ``Exception``: failures of bookkeeping (failure attempt, run
    finalization, mutex release) are appended to ``errors`` and keep every
    confirmed outcome.

    ``conn`` is a non-autocommit connection with the target schema on its
    search_path; ``client`` exposes ``fetch_daily_observations(ticker, start,
    end, max_attempts=1, remaining=...)`` (one HTTP request, no hidden retries,
    pacing bounded by the remaining budget). ``reconnect`` (optional) opens a
    fresh connection with the same search_path to reconcile a lost COMMIT
    acknowledgement by receipt. The ``max_seconds`` deadline is measured on
    ``monotonic`` and checked before pacing, before the request, after the
    fetch and again before the first DML of each instrument.
    """
    started = monotonic()

    def remaining() -> float:
        return limits.max_seconds - (monotonic() - started)

    result = _empty_result(plan.sha256)
    try:
        if not hmac.compare_digest(plan.sha256, (supplied_sha256 or "").lower()):
            raise RebaseError("PLAN_HASH_MISMATCH")
        limits.validate()
        if limits.canonical() != plan.manifest["limits"]:
            raise RebaseError("PLAN_LIMITS_MISMATCH")
        ids = [str(uuid.UUID(str(v))) for v in instrument_ids]
        if (
            not ids
            or len(set(ids)) != len(ids)
            or len(ids) > limits.batch_size
            or len(ids) > limits.max_instruments
            or any(plan.item(i) is None for i in ids)
        ):
            raise RebaseError("ALLOWLIST_INVALID")
    except (RebaseError, ValueError) as exc:
        code = exc.code if isinstance(exc, RebaseError) else "ALLOWLIST_INVALID"
        result["errors"] = []
        return _finish(result, status="blocked", code=code)
    items = sorted((plan.item(i) for i in ids), key=lambda item: item.instrument_id)
    result["planned"] = len(items)
    batch = _Batch(result, items)
    try:
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        got = conn.execute(
            "SELECT pg_try_advisory_lock(%s)", (LOCK_NAV_ECONOMIC_REBASE,)
        ).fetchone()[0]
        conn.commit()
    except psycopg.Error:
        _rollback_quietly(conn)
        batch.status, batch.code, batch.retryable = "blocked", "DATABASE_ERROR", True
        return batch.finish()
    if not got:
        batch.status, batch.code, batch.retryable = "lock_busy", "LOCK_BUSY", True
        return batch.finish()
    try:
        _run_batch(conn, plan, batch, limits, client, remaining, reconnect)
    except KeyboardInterrupt:
        # Operator interrupt: the in-flight instrument already rolled back;
        # committed ones stay committed (receipts). Truthful, not "no writes".
        batch.status, batch.code, batch.retryable = "partial", "INTERRUPTED", True
        batch.stop_code = "INTERRUPTED"
        if batch.run_id is not None:
            batch.safe(
                "FINALIZE_FAILED",
                lambda: _finalize(conn, batch.run_id, "aborted", "INTERRUPTED"),
            )
        batch.safe("MUTEX_RELEASE_FAILED", lambda: _release(conn))
        return batch.finish()
    except RebaseError as exc:
        stop = exc.code
        batch.status, batch.code, batch.retryable = None, stop, exc.retryable
        batch.stop_code = stop
        if not any(o.status in _WROTE for o in batch.outcomes.values()):
            batch.status = "blocked"
        if batch.run_id is not None:
            batch.safe(
                "FINALIZE_FAILED", lambda: _finalize(conn, batch.run_id, "failed", stop)
            )
        batch.safe("MUTEX_RELEASE_FAILED", lambda: _release(conn))
        return batch.finish()
    except psycopg.Error:
        # Outside an instrument transaction (orphan recovery, run row, preflight).
        _rollback_quietly(conn)
        batch.errors.append("DATABASE_ERROR")
        batch.stop_code, batch.retryable = "DATABASE_ERROR", True
        if batch.run_id is not None:
            batch.safe(
                "FINALIZE_FAILED",
                lambda: _finalize(conn, batch.run_id, "failed", "DATABASE_ERROR"),
            )
        batch.safe("MUTEX_RELEASE_FAILED", lambda: _release(conn))
        return batch.finish()
    except Exception:
        # Unexpected: keep every confirmed outcome, sanitized code only.
        batch.errors.append("UNEXPECTED_ERROR")
        batch.stop_code = "UNEXPECTED_ERROR"
        if batch.run_id is not None:
            batch.safe(
                "FINALIZE_FAILED",
                lambda: _finalize(conn, batch.run_id, "failed", "UNEXPECTED_ERROR"),
            )
        batch.safe("MUTEX_RELEASE_FAILED", lambda: _release(conn))
        return batch.finish()
    if batch.run_id is not None:
        failed = any(o.status in ("failed", "unknown") for o in batch.outcomes.values())
        if batch.stop_code in ("BUDGET_EXHAUSTED", "LOCK_BUSY"):
            final = ("aborted", batch.stop_code)
        elif failed or batch.stop_code:
            final = ("failed", batch.stop_code or "INSTRUMENT_FAILED")
        else:
            final = ("completed", None)
        if (
            not batch.safe(
                "FINALIZE_FAILED", lambda: _finalize(conn, batch.run_id, *final)
            )
            and reconnect is not None
        ):
            # The applying session may be gone; our own run is closed on a
            # fresh session so it is not left running (it is not a recovery).
            batch.safe(
                "FINALIZE_FAILED_RECONNECT",
                lambda: _finalize_fresh(reconnect, batch.run_id, *final),
            )
    batch.safe("MUTEX_RELEASE_FAILED", lambda: _release(conn))
    return batch.finish()


_WROTE = ("committed", "committed_unverified", "unknown")


def _run_batch(
    conn, plan, batch: _Batch, limits: RebaseLimits, client, remaining, reconnect
) -> None:
    items = batch.items
    batch.result["orphan_runs_recovered"] = _recover_orphan_runs(conn, "rebase")
    todo: list[PlanItem] = []
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute("SELECT clock_timestamp() AS now")
            now = cur.fetchone()["now"]
            pins, grid, closed = _current_pins(conn, now)
            if _pins_stale(plan, pins, grid, closed):
                raise RebaseError("PLAN_STALE")
            for item in items:
                state = _receipt_state(cur, plan, item)
                if state == "already_applied":
                    batch.outcomes[item.instrument_id] = InstrumentRebaseResult(
                        item.instrument_id, "already_applied"
                    )
                elif state == "stale":
                    batch.outcomes[item.instrument_id] = InstrumentRebaseResult(
                        item.instrument_id, "failed", "PLAN_STALE"
                    )
                else:
                    todo.append(item)
    finally:
        _rollback_quietly(conn)
    if not todo:
        return
    batch.run_id = uuid.uuid4()
    conn.execute(
        """INSERT INTO nav_ingestion_runs
           (run_id, requested_end, status, operation, contract_version, plan_sha256)
           VALUES (%s,%s,'running','rebase',%s,%s)""",
        (
            batch.run_id,
            dt.date.fromisoformat(plan.manifest["closed_session"]),
            CONTRACT_VERSION,
            plan.sha256,
        ),
    )
    conn.commit()
    batch.result["run_id"] = str(batch.run_id)
    requests_used = 0
    for position, item in enumerate(todo):
        if requests_used >= limits.max_requests or remaining() <= 0:
            batch.stop_code = "BUDGET_EXHAUSTED"
            batch.rest(todo, position, "BUDGET_EXHAUSTED")
            return
        fetch: NavFetchResult | None = None
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                sessions = _sessions(
                    cur, plan.manifest["policy"], item.window_start, item.window_end
                )
                cur.execute("SELECT clock_timestamp() AS now")
                now = cur.fetchone()["now"]
            conn.rollback()  # no transaction is held across the network
            # Counted when invoked (a request may be in flight when interrupted);
            # un-counted only when the client refused BEFORE sending.
            requests_used += 1
            batch.result["requests_used"] = requests_used
            try:
                fetch = client.fetch_daily_observations(
                    item.ticker,
                    item.window_start,
                    item.window_end,
                    max_attempts=1,
                    remaining=remaining,
                )
            except TiingoDeadlineExceeded:
                requests_used -= 1
                batch.result["requests_used"] = requests_used
                batch.stop_code = "BUDGET_EXHAUSTED"
                batch.rest(todo, position, "BUDGET_EXHAUSTED")
                return
            except TiingoBudgetExceeded:
                raise RebaseError("PROVIDER_RATE_LIMITED") from None
            batch.result["fetched"] += 1
            if remaining() <= 0:
                # The response arrived after the deadline: never apply it.
                batch.stop_code = "BUDGET_EXHAUSTED"
                batch.rest(todo, position, "BUDGET_EXHAUSTED")
                return
            snapshot = validate_adjusted_snapshot(
                item,
                fetch,
                sessions=[s["session_date"] for s in sessions],
                due_sessions=[
                    s["session_date"] for s in sessions if s["nav_due_at"] <= now
                ],
            )
            batch.outcomes[item.instrument_id] = apply_rebase_instrument(
                conn, plan, item, snapshot, batch.run_id, remaining=remaining
            )
        except RebaseDeadline:
            batch.stop_code = "BUDGET_EXHAUSTED"
            batch.rest(todo, position, "BUDGET_EXHAUSTED")
            return
        except RebaseLockBusy:
            batch.safe(
                "FAILURE_RECORD_FAILED",
                lambda: _record_failure(conn, batch.run_id, item, "LOCK_BUSY", fetch),
            )
            batch.outcomes[item.instrument_id] = InstrumentRebaseResult(
                item.instrument_id, "lock_busy", "LOCK_BUSY", retryable=True
            )
            batch.stop_code = "LOCK_BUSY"
            batch.rest(todo, position + 1, "LOCK_BUSY")
            return
        except RebaseCommitUnknown as unknown:
            reconciled = _reconcile_commit(reconnect, plan, unknown)
            if reconciled is not None:
                batch.outcomes[item.instrument_id] = InstrumentRebaseResult(
                    item.instrument_id,
                    "committed_unverified",
                    "COMMIT_ACK_LOST",
                    receipt_id=reconciled["receipt_id"],
                    changed_level_rows=reconciled["changed_level_rows"],
                    changed_return_rows=reconciled["changed_return_rows"],
                )
            else:
                batch.outcomes[item.instrument_id] = InstrumentRebaseResult(
                    item.instrument_id,
                    "unknown",
                    "COMMIT_OUTCOME_UNKNOWN",
                    receipt_id=unknown.receipt_id,
                    retryable=True,
                )
            # The applying session is unusable; stop the batch truthfully.
            batch.stop_code = "DATABASE_ERROR"
            batch.rest(todo, position + 1, "DATABASE_ERROR")
            return
        except RebaseError as exc:
            failure_code, failure_retryable = exc.code, exc.retryable
            batch.safe(
                "FAILURE_RECORD_FAILED",
                lambda: _record_failure(conn, batch.run_id, item, failure_code, fetch),
            )
            batch.outcomes[item.instrument_id] = InstrumentRebaseResult(
                item.instrument_id, "failed", failure_code, retryable=failure_retryable
            )
            if getattr(conn, "broken", False):
                batch.stop_code = "DATABASE_ERROR"
                batch.rest(todo, position + 1, "DATABASE_ERROR")
                return
        except psycopg.Error:
            _rollback_quietly(conn)
            batch.safe(
                "FAILURE_RECORD_FAILED",
                lambda: _record_failure(
                    conn, batch.run_id, item, "DATABASE_ERROR", fetch
                ),
            )
            batch.outcomes[item.instrument_id] = InstrumentRebaseResult(
                item.instrument_id, "failed", "DATABASE_ERROR", retryable=True
            )
            batch.stop_code = "DATABASE_ERROR"
            batch.rest(todo, position + 1, "DATABASE_ERROR")
            return


def _finalize(conn, run_id: uuid.UUID, status: str, reason: str | None) -> None:
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()
    conn.execute(
        """UPDATE nav_ingestion_runs SET status=%s, reason_code=%s
           WHERE run_id=%s AND status='running'""",
        (status, reason, run_id),
    )
    conn.commit()


def _finalize_fresh(
    reconnect, run_id: uuid.UUID, status: str, reason: str | None
) -> None:
    with reconnect() as fresh:
        _finalize(fresh, run_id, status, reason)


def _release(conn) -> None:
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()
    conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_NAV_ECONOMIC_REBASE,))
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Result contract
# ──────────────────────────────────────────────────────────────────────────────
def _empty_result(plan_sha256: str | None) -> dict:
    return {
        "status": "blocked",
        "contract_version": CONTRACT_VERSION,
        "plan_sha256": plan_sha256,
        "run_id": None,
        "planned": 0,
        "fetched": 0,
        "committed": 0,
        "committed_unverified": 0,
        "unknown": 0,
        "already_applied": 0,
        "failed": 0,
        "not_attempted": 0,
        "requests_used": 0,
        "orphan_runs_recovered": 0,
        "retryable": False,
        "reason_counts": {},
        "instruments": [],
        "readiness_republish_required": False,
        "errors": [],
        "code": None,
    }


def _finish(
    result: dict,
    *,
    status: str | None = None,
    code: str | None = None,
    retryable: bool | None = None,
    outcomes: Mapping[str, InstrumentRebaseResult] | None = None,
    not_attempted: Iterable[PlanItem] = (),
    stop_code: str | None = None,
) -> dict:
    outcomes = dict(outcomes or {})
    for item in not_attempted:
        outcomes.setdefault(
            item.instrument_id,
            InstrumentRebaseResult(
                item.instrument_id, "not_attempted", code, retryable=bool(retryable)
            ),
        )
    ordered = [outcomes[k] for k in sorted(outcomes)]
    counts = Counter(o.status for o in ordered)
    result.setdefault("errors", [])
    result["instruments"] = [o.as_dict() for o in ordered]
    result["committed"] = counts["committed"]
    result["committed_unverified"] = counts["committed_unverified"]
    result["unknown"] = counts["unknown"]
    result["already_applied"] = counts["already_applied"]
    result["failed"] = counts["failed"]
    result["not_attempted"] = counts["not_attempted"] + counts["lock_busy"]
    result["reason_counts"] = dict(
        sorted(Counter(o.code for o in ordered if o.code).items())
    )
    wrote = result["committed"] + result["committed_unverified"] + result["unknown"]
    # A possibly-committed instrument (unknown) also requires the republish check.
    result["readiness_republish_required"] = wrote > 0
    result["retryable"] = bool(retryable) or any(
        o.retryable
        for o in ordered
        if o.status in ("failed", "not_attempted", "lock_busy", "unknown")
    )
    troubled = result["failed"] or result["unknown"] or result["errors"]
    if status is None:
        if troubled:
            status = "partial" if wrote else "blocked"
        elif stop_code == "LOCK_BUSY" and not wrote:
            status = "lock_busy"
        elif stop_code:
            status = "partial"
        else:
            status = "completed"
    result["status"] = status
    result["code"] = (
        code
        if code is not None
        else (stop_code or (result["errors"][0] if result["errors"] else None))
    )
    return result


def exit_code(result: Mapping[str, Any]) -> int:
    """0 planned/complete; 2 validation/provider/instrument failure, an
    undetermined commit or a bookkeeping error (``partial`` when writes may
    exist); 4 lock busy with zero commits; 5 budget/lock/interrupt stop after
    work. (3 = schema/access/dependency incompatibility, decided by the CLI.)"""
    status = result["status"]
    if status in ("planned", "completed"):
        return EXIT_OK
    if status == "lock_busy":
        return EXIT_LOCK_BUSY
    if (
        result.get("failed")
        or result.get("unknown")
        or result.get("errors")
        or status == "blocked"
    ):
        return EXIT_FAILED
    return EXIT_INTERRUPTED
