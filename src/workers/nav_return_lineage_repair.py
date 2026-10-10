"""Repair stored NULL-return boundary flags using real provider confirmation.

The queue is the still-bad predicate, including non-cohort historical series.
Never present fetched levels to the writer. A normal ingestion ledger run per
actual date fetch avoids inventing a full-window attempt or claiming the Tiingo
economic-rebase contract. Writer mode='rebase' only recomputes the stored row.
All runs, attempts, writes and final-head evidence commit together per instrument.

Per instrument, phase A reads the stored state lock-free (a read-only
transaction that ends before any HTTP), then fetches and validates every bad
date with no lock and no open transaction. Phase B (apply only) takes the two
writer locks, re-checks the lock-time state against the approved plan and
writes with the pre-fetched provider timestamps. The global locks are never
held across a provider call, so daily lock holders are not starved.

The attempt timestamps come from the operator host clock while persistence
times come from the database, so a host clock ahead of the database is refused
(CLOCK_SKEW) before any fetch. Run apply BEFORE the daily ingestion chain: when
no run is pinned, readiness picks the winning attempt by attempted_at DESC, and
a repair attempt (historical requested_end) could otherwise become the latest.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

from src.db import LOCK_FUND_NAV_READINESS, LOCK_INSTRUMENT_INGESTION
from src.workers import instrument_ingestion as ingest
from src.workers._tiingo import (
    NavFetchResult, TiingoBudgetExceeded, TiingoDeadlineExceeded,
)

VERSION = "nav-return-lineage-repair-v1"
CONFIRM_TOKEN = "repair_nav_return_lineage_v1"
BAD = """return_1d IS NULL AND return_source_boundary IS NOT NULL
 AND return_start_date IS NULL AND return_uses_repaired_nav IS NULL
 AND return_semantics IS NULL AND return_verification_status IS NULL"""
# Plan fields that every daily ingestion of an active instrument moves. They
# stay in the reviewed plan as inventory but never make an item stale.
INFORMATIONAL_FIELDS = ("revision_head", "last_nav_date", "history_older_than_30_days")
MAX_CLOCK_AHEAD = dt.timedelta(milliseconds=250)
# Non-success provider statuses that are facts about the data of that date:
# a per-instrument residual. Every other non-success status (rate_limited,
# not_configured, transient_error, ...) is an outage and stops the run.
DATA_STATUS_CODES = {"empty": "PROVIDER_DATE_MISSING", "not_found": "PROVIDER_NOT_FOUND",
                     "invalid_payload": "PROVIDER_INVALID_PAYLOAD"}
PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"


class RepairError(Exception):
    def __init__(self, code: str, exit_code: int = 2):
        self.code, self.exit_code = code, exit_code
        super().__init__(code)


@dataclass(frozen=True)
class RepairLimits:
    batch_size: int = 20
    max_requests: int = 20
    max_seconds: float = 120.0
    rate_per_second: float = 1.0

    def validate(self):
        if not 1 <= self.batch_size <= 20:
            raise RepairError("BATCH_SIZE_INVALID")
        if not 0 <= self.max_requests <= 10000:
            raise RepairError("MAX_REQUESTS_INVALID")
        if not math.isfinite(self.max_seconds) or self.max_seconds <= 0:
            raise RepairError("MAX_SECONDS_INVALID")
        if not math.isfinite(self.rate_per_second) or not 0 < self.rate_per_second <= 2.5:
            raise RepairError("RATE_INVALID")
        return self

    def canonical(self):
        return {**asdict(self), "max_seconds": float(self.max_seconds),
                "rate_per_second": float(self.rate_per_second)}


def plan_bytes(plan: dict) -> bytes:
    return json.dumps(plan, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False, default=str).encode("utf-8")


def sha256(plan: dict) -> str:
    return hashlib.sha256(plan_bytes(plan)).hexdigest()


def _json(value):
    return json.loads(plan_bytes(value))


def _is_bad(row):
    return (row["return_1d"] is None and row["return_source_boundary"] is not None
            and all(row[k] is None for k in (
                "return_start_date", "return_uses_repaired_nav", "return_semantics",
                "return_verification_status")))


def _compatible(prev, row):
    # The existing writer's admission predicate; only used to classify a residual.
    return (prev is not None and prev["source"] == row["source"]
            and prev["source_nav_kind"] == row["source_nav_kind"]
            and row["source_nav_kind"] in ("adjusted", "raw", "unknown")
            and prev["nav_repair_kind"] is not None
            and row["nav_repair_kind"] is not None
            and prev["nav"] is not None and row["nav"] is not None
            and min(float(prev["nav"]), float(row["nav"])) > 0)


def _load_item(conn, iid: str, as_of: dt.date, *, full_state=False):
    with conn.cursor(row_factory=dict_row) as cur:
        if full_state:
            cur.execute("SELECT * FROM nav_timeseries WHERE instrument_id=%s ORDER BY nav_date",
                        (iid,))
        else:
            # The head pins every governed mutation. Plan only needs each target
            # and its neighbours, not decades of levels for 10,000 instruments.
            cur.execute("""WITH bad AS (
                SELECT nav_date FROM nav_timeseries WHERE instrument_id=%(iid)s AND """ + BAD + """
            ), dates AS (
                SELECT nav_date FROM bad
                UNION SELECT p.nav_date FROM bad b CROSS JOIN LATERAL (
                    SELECT nav_date FROM nav_timeseries WHERE instrument_id=%(iid)s
                    AND nav_date<b.nav_date ORDER BY nav_date DESC LIMIT 1) p
                UNION SELECT s.nav_date FROM bad b CROSS JOIN LATERAL (
                    SELECT nav_date FROM nav_timeseries WHERE instrument_id=%(iid)s
                    AND nav_date>b.nav_date ORDER BY nav_date LIMIT 1) s
            ) SELECT * FROM nav_timeseries WHERE instrument_id=%(iid)s
                AND nav_date IN (SELECT nav_date FROM dates) ORDER BY nav_date""", {"iid": iid})
        state = cur.fetchall()
        bad = [row for row in state if _is_bad(row)]
        if not bad:
            return None, state
        cur.execute("SELECT ticker,is_active FROM instruments_universe WHERE instrument_id=%s",
                    (iid,))
        identity = cur.fetchone()
        cur.execute("""SELECT event_id FROM fund_nav_reexpression_events d
            WHERE instrument_id=%s AND event_kind='DETECTED'
              AND reason_code='ADJUSTED_HISTORY_REEXPRESSION'
              AND NOT EXISTS (SELECT 1 FROM fund_nav_reexpression_events r
                              WHERE r.event_kind='RESOLVED' AND r.resolves_event_id=d.event_id)
            ORDER BY event_id""", (iid,))
        holds = [r["event_id"] for r in cur.fetchall()]
        cur.execute("SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s", (iid,))
        head = cur.fetchone()
        cur.execute("SELECT max(nav_date) AS last_nav FROM nav_timeseries WHERE instrument_id=%s", (iid,))
        last_nav = cur.fetchone()["last_nav"]
    reasons = []
    if holds:
        reasons.append("OPEN_REEXPRESSION")
    if not identity or not (identity["ticker"] or "").strip():
        reasons.append("MISSING_TICKER")
    if not identity or identity["is_active"] is not True:
        reasons.append("INACTIVE_INSTRUMENT")
    if any(r["source_nav_kind"] is None for r in bad):
        reasons.append("NULL_KIND")
    if any(r["source_nav_kind"] not in (None, "adjusted", "raw", "unknown") for r in bad):
        reasons.append("UNSUPPORTED_KIND")
    if any(r["source"] not in ("tiingo", "yahoo") for r in bad):
        reasons.append("UNSUPPORTED_SOURCE")
    if any(r["return_source_boundary"] is not True for r in bad):
        reasons.append("UNSUPPORTED_FLAG")
    if any(r["source_nav"] is None or not math.isfinite(float(r["source_nav"]))
           or float(r["source_nav"]) <= 0 for r in bad):
        reasons.append("INVALID_STORED_LEVEL")
    if any(_is_bad(r) and _compatible(state[i-1] if i else None, r)
           for i, r in enumerate(state)):
        reasons.append("RETURN_RECOMPUTE_REQUIRED")
    neighbor_positions = {j for i, r in enumerate(state) if _is_bad(r)
                          for j in (i-1, i, i+1) if 0 <= j < len(state)}
    item = {
        "instrument_id": iid, "identity": identity, "rows": bad,
        "open_events": holds, "revision_head": head["revision_id"] if head else 0,
        "neighborhood_sha256": sha256([state[i] for i in sorted(neighbor_positions)]),
        "last_nav_date": last_nav,
        # Stale history is observable; a delisting cannot be inferred from age alone.
        "history_older_than_30_days": last_nav < as_of - dt.timedelta(days=30),
        "reasons": reasons,
    }
    return _json(item), state


def _repair_key(item):
    """What the repair depends on: identity, the bad rows' full values, open
    reexpression events, the neighbourhood digest and the reasons."""
    return {k: v for k, v in item.items() if k not in INFORMATIONAL_FIELDS}


def build_plan(conn, *, schema="public", limits=None, schema_pins=None):
    """Caller supplies a repeatable-read, read-only transaction; no HTTP/ledger."""
    limits = (limits or RepairLimits()).validate()
    as_of = conn.execute("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date").fetchone()[0]
    ids = conn.execute("SELECT DISTINCT instrument_id FROM nav_timeseries WHERE " + BAD
                       + " ORDER BY instrument_id").fetchall()
    items = [_load_item(conn, str(iid), as_of)[0] for (iid,) in ids]
    return {"version": VERSION, "schema": schema, "schema_pins": schema_pins or {},
            "as_of": as_of.isoformat(), "limits": limits.canonical(), "items": items}


def summarize(plan):
    reasons, sources = Counter(), Counter()
    for item in plan["items"]:
        for reason in item["reasons"]:
            reasons[reason] += len(item["rows"])
        for row in item["rows"]:
            sources[row["source"] if row["source"] in ("tiingo", "yahoo") else "other"] += 1
    return {"instruments": len(plan["items"]),
            "rows": sum(len(i["rows"]) for i in plan["items"]),
            "eligible_instruments": sum(not i["reasons"] for i in plan["items"]),
            "history_older_than_30_days": sum(i["history_older_than_30_days"] for i in plan["items"]),
            "residual_counts": dict(sorted(reasons.items())), "source_counts": dict(sources)}


class ProviderClient:
    """Lazy adapters to the ingestor's typed provider paths, one request per call."""
    def __init__(self, limits):
        self.limits, self.clients = limits, {}

    def fetch(self, provider, ticker, start, end, *, remaining):
        if provider == "tiingo":
            from src.workers._tiingo import TiingoClient, TokenBucket
            if provider not in self.clients:
                self.clients[provider] = TiingoClient(bucket=TokenBucket(
                    max_tokens=1, refill_rate=self.limits.rate_per_second))
            return self.clients[provider].fetch_daily_observations(
                ticker, start, end, max_attempts=1, remaining=remaining)
        if provider == "yahoo":
            from src.workers._fallback_nav import FallbackNav
            if provider not in self.clients:
                self.clients[provider] = FallbackNav(eodhd_key="")
            result, _source, attempts = self.clients[provider].fetch_observations(
                ticker, start, end, max_attempts=1, remaining=remaining)
            return next((r for p, r in attempts if p == "yahoo"), result)
        raise RepairError("UNSUPPORTED_SOURCE")

    def close(self):
        for client in self.clients.values():
            client.close()


class _Budget:
    def __init__(self, limits):
        self.limits, self.started, self.last = limits, time.monotonic(), None
        self.requests = 0

    def remaining(self):
        return self.limits.max_seconds - (time.monotonic() - self.started)

    def check(self):
        if self.remaining() <= 0:
            raise RepairError("TIME_BUDGET", 5)

    def request(self):
        self.check()
        if self.requests >= self.limits.max_requests:
            raise RepairError("REQUEST_BUDGET", 5)
        delay = max(0, 1 / self.limits.rate_per_second - (time.monotonic() - self.last)) if self.last else 0
        if delay >= self.remaining():
            raise RepairError("TIME_BUDGET", 5)
        if delay:
            time.sleep(delay)
        self.check()
        self.requests += 1  # upper bound: client may refuse before sending
        self.last = time.monotonic()


def _validate(row, fetched):
    """None when the stored row is confirmed, a data residual code, or
    PROVIDER_UNAVAILABLE for an operational provider failure."""
    if fetched.status not in ingest.SUCCESS_ATTEMPTS:
        return DATA_STATUS_CODES.get(fetched.status, PROVIDER_UNAVAILABLE)
    matches = [o for o in fetched.observations if o.date == row["nav_date"]]
    if not matches:
        return "PROVIDER_DATE_MISSING"
    if len(matches) != 1:
        return "PROVIDER_INVALID_PAYLOAD"
    obs = matches[0]
    if obs.kind != row["source_nav_kind"]:
        return "KIND_MISMATCH"
    if obs.price is None or not math.isfinite(obs.price) or obs.price <= 0:
        return "PROVIDER_INVALID_PAYLOAD"
    if not math.isclose(float(row["source_nav"]), obs.price,
                        rel_tol=ingest.ADJUSTED_OVERLAP_REL_TOL,
                        abs_tol=ingest.ADJUSTED_OVERLAP_ABS_TOL):
        return "LEVEL_MISMATCH"
    if (fetched.attempted_at is None or fetched.finished_at is None
            or fetched.finished_at < fetched.attempted_at):
        return "PROVIDER_INVALID_TIMESTAMPS"
    return None


def _sql_budget(conn, budget):
    budget.check()
    conn.execute("SELECT set_config('statement_timeout',%s,true)",
                 (str(max(1, min(120000, int(budget.remaining() * 1000)))),))


def _assert_changes(conn, iid, before, selected, runs, old_head, budget):
    with conn.cursor(row_factory=dict_row) as cur:
        _sql_budget(conn, budget)
        cur.execute("SELECT * FROM nav_timeseries WHERE instrument_id=%s ORDER BY nav_date", (iid,))
        after = cur.fetchall()
        dates = {row["nav_date"] for row in selected}
        expected = [{**row, "return_source_boundary": None} if row["nav_date"] in dates else row
                    for row in before]
        if after != expected:
            raise RepairError("UNEXPECTED_NAV_CHANGE")
        _sql_budget(conn, budget)
        cur.execute("""SELECT * FROM fund_nav_data_revisions
                       WHERE instrument_id=%s AND revision_id>%s ORDER BY revision_id""",
                    (iid, old_head))
        revisions = cur.fetchall()
        by_date = {row["nav_date"]: (run, row["source"]) for run, row, _ in runs}
        if len(revisions) != len(dates) or {r["nav_date"] for r in revisions} != dates:
            raise RepairError("UNEXPECTED_REVISION_COUNT")
        cur.execute("SELECT pg_current_xact_id() AS xid")
        xid = cur.fetchone()["xid"]
        for rev in revisions:
            run, provider = by_date[rev["nav_date"]]
            if (rev["source_run_id"] != run or rev["source_provider"] != provider
                    or rev["source_attempt_xid"] != xid or not rev["derived_return_only"]
                    or rev["mutation_kind"] != "UPDATE" or not rev["data_changed"]
                    or rev["calendar_changed"] or rev["maintenance_run_id"] is not None
                    or rev["dependency_start_date"] is not None):
                raise RepairError("UNEXPECTED_REVISION_ATTRIBUTION")
        cur.execute("SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s", (iid,))
        if cur.fetchone()["revision_id"] != max(r["revision_id"] for r in revisions):
            raise RepairError("UNEXPECTED_REVISION_HEAD")
        _sql_budget(conn, budget)
        cur.execute("SET CONSTRAINTS ALL IMMEDIATE")


def _attributed_clear(conn, iid, approved):
    """Each formerly bad date's latest revision is newer than the plan's head
    and attributed to a same-xid successful attempt. An unattributed clear (a
    raw UPDATE, or a trigger bypass that left an older revision latest) still
    invalidates readiness lineage, and the bad predicate can no longer find it."""
    dates = [dt.date.fromisoformat(r["nav_date"]) for r in approved["rows"]]
    with conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT ON (r.nav_date) r.nav_date, r.revision_id,
                   r.source_run_id IS NOT NULL AND r.source_provider IS NOT NULL
                   AND r.source_attempt_xid IS NOT NULL AND EXISTS (
                       SELECT 1 FROM nav_ingestion_attempts a
                       WHERE a.run_id=r.source_run_id AND a.instrument_id=r.instrument_id
                         AND a.provider=r.source_provider AND a.status=ANY(%s)
                         AND a.commit_xid=r.source_attempt_xid)
            FROM fund_nav_data_revisions r
            WHERE r.instrument_id=%s AND r.nav_date=ANY(%s)
            ORDER BY r.nav_date, r.revision_id DESC""",
                    (list(ingest.SUCCESS_ATTEMPTS), iid, dates))
        latest = {day: (rev, ok) for day, rev, ok in cur.fetchall()}
    return all(day in latest and latest[day][0] > approved["revision_head"] and latest[day][1]
               for day in dates)


def _decide(conn, iid, current, before, approved):
    """Outcome that needs no write, or None. Phases A and B both use it inside
    their (read-only or locked) transaction."""
    if current is None:
        # Resume the same reviewed plan only if its formerly bad rows have
        # exactly the intended values, cleared by an attributed write (this
        # operator's earlier commit). Nothing is written in this branch.
        stored = {str(r["nav_date"]): _json(r) for r in before}
        if not all(stored.get(r["nav_date"]) == {**r, "return_source_boundary": None}
                   for r in approved["rows"]):
            return {"instrument_id": iid, "status": "skipped", "code": "PLAN_STALE",
                    "rows": len(approved["rows"])}
        if not _attributed_clear(conn, iid, approved):
            return {"instrument_id": iid, "status": "skipped", "code": "UNATTRIBUTED_CLEAR",
                    "rows": len(approved["rows"])}
        return {"instrument_id": iid, "status": "noop", "code": "ALREADY_REPAIRED", "rows": 0}
    if _repair_key(current) != _repair_key(approved):
        return {"instrument_id": iid, "status": "skipped", "code": "PLAN_STALE",
                "rows": len(approved["rows"])}
    if current["reasons"]:
        return {"instrument_id": iid, "status": "skipped", "code": current["reasons"][0],
                "rows": len(current["rows"])}
    return None


def _apply_instrument(conn, approved, as_of, client, budget, validate_only):
    iid = approved["instrument_id"]
    budget.check()
    # Phase A: no lock, and the read-only snapshot transaction ends before HTTP.
    try:
        conn.execute("SET TRANSACTION READ ONLY")
        _sql_budget(conn, budget)
        current, before = _load_item(conn, iid, as_of, full_state=True)
        decided = _decide(conn, iid, current, before, approved)
    finally:
        if not conn.closed:
            conn.rollback()
    if decided:
        return decided
    # The typed stored rows equal the approved plan rows here (_repair_key).
    count = len(current["rows"])
    ticker = current["identity"]["ticker"].strip()
    fetched_by_date = {}
    for row in (r for r in before if _is_bad(r)):
        budget.request()
        fetched = client.fetch(row["source"], ticker, row["nav_date"], row["nav_date"],
                               remaining=budget.remaining)
        budget.check()
        reason = _validate(row, fetched)
        if reason == PROVIDER_UNAVAILABLE:
            # An outage is not a fact about this row: stop instead of letting
            # the batch finish as complete. Nothing is written for it.
            return {"instrument_id": iid, "status": "stopped", "code": reason, "rows": count}
        if reason:
            return {"instrument_id": iid, "status": "skipped", "code": reason, "rows": count}
        # Persist the actual single-date observation in the attempt only, with
        # the provider's own timestamps. Provider levels never enter the NAV
        # writer or the row-evidence helper.
        fetched_by_date[row["nav_date"]] = NavFetchResult("success_no_new", tuple(
            o for o in fetched.observations if o.date == row["nav_date"]),
            fetched.attempted_at, fetched.finished_at)
    if validate_only:
        return {"instrument_id": iid, "status": "validated", "code": None, "rows": count}
    # Phase B: locks, lock-time re-check against the approved plan, writes.
    try:
        budget.check()
        conn.execute("SET LOCAL lock_timeout='2s'")
        _sql_budget(conn, budget)
        for lock in (LOCK_INSTRUMENT_INGESTION, LOCK_FUND_NAV_READINESS):
            if not conn.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock,)).fetchone()[0]:
                raise RepairError("LOCK_BUSY", 4)
        current, before = _load_item(conn, iid, as_of, full_state=True)
        decided = _decide(conn, iid, current, before, approved)
        selected = [r for r in before if _is_bad(r)]
        if not decided and {r["nav_date"] for r in selected} != set(fetched_by_date):
            decided = {"instrument_id": iid, "status": "skipped", "code": "PLAN_STALE",
                       "rows": len(approved["rows"])}
        if decided:
            conn.rollback()
            return decided
        head = current["revision_head"]  # read under the lock, never the plan's
        runs = []
        for row in selected:
            fetched = fetched_by_date[row["nav_date"]]
            _sql_budget(conn, budget)
            run = uuid.uuid4()
            conn.execute("""INSERT INTO nav_ingestion_runs (run_id,requested_end,status,operation)
                            VALUES (%s,%s,'running','normal')""", (run, row["nav_date"]))
            attempt = ingest.TickerPlan(current["identity"]["ticker"].strip(), row["nav_date"],
                                        ((uuid.UUID(iid), row["currency"]),), None)
            ingest._insert_attempt_tx(conn, run, attempt, row["source"], fetched, row["nav_date"])
            _sql_budget(conn, budget)
            written = ingest._write_instrument_nav_tx(
                conn, [row.copy()], run_id=run, provider=row["source"], mode="rebase")
            if (written.changed_level_rows != 0 or written.changed_return_rows != 1
                    or written.revision_count != 1 or written.detected_event_ids):
                raise RepairError("UNEXPECTED_WRITER_RESULT")
            runs.append((run, row, fetched))
        for run, row, _ in runs:
            _sql_budget(conn, budget)
            ingest._insert_row_evidence_tx(conn, run_id=run, instrument_id=uuid.UUID(iid),
                                           provider=row["source"], rows=[row])
            conn.execute("""UPDATE nav_ingestion_runs SET status='completed',
                            reason_code='RETURN_LINEAGE_REPAIR' WHERE run_id=%s""", (run,))
        _assert_changes(conn, iid, before, selected, runs, head, budget)
        _sql_budget(conn, budget)
    except BaseException:
        if not conn.closed:
            conn.rollback()
        raise
    try:
        conn.commit()
    except BaseException as exc:
        if not isinstance(exc, psycopg.Error) or conn.broken or exc.sqlstate is None:
            # The acknowledgement may have been lost after a successful COMMIT.
            # Do not call this a rollback; report the run IDs for reconciliation.
            return {"instrument_id": iid, "status": "unknown", "code": "COMMIT_UNKNOWN",
                    "rows": len(selected), "run_ids": [str(run) for run, _, _ in runs]}
        conn.rollback()
        raise
    return {"instrument_id": iid, "status": "committed", "code": None,
            "rows": len(selected), "run_ids": [str(run) for run, _, _ in runs]}


def _utc_now():
    return dt.datetime.now(dt.timezone.utc)


def _clock_skew(conn, clock):
    """Host minus database clock, estimated at the round-trip midpoint."""
    try:
        t0 = clock()
        db_now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        t1 = clock()
    finally:
        if not conn.closed:
            conn.rollback()
    return t0 + (t1 - t0) / 2 - db_now


def _normalized_ids(values):
    out = set()
    for value in values:
        try:
            out.add(str(uuid.UUID(str(value))))
        except ValueError:
            out.add(str(value))
    return out


def run_repair(conn, plan, *, instrument_ids, supplied_sha256, limits, client,
               validate_only=False, clock=None):
    result = {"status": "completed", "code": None, "exit_code": 0, "instruments": [],
              "changed_rows": 0, "validated_rows": 0, "requests": 0, "residual_counts": {},
              "plan_sha256": sha256(plan), "validate_only": validate_only}
    budget = _Budget(limits)
    try:
        limits.validate()
        if plan.get("version") != VERSION or supplied_sha256 != sha256(plan):
            raise RepairError("PLAN_DIGEST_MISMATCH")
        if plan.get("limits") != limits.canonical():
            raise RepairError("PLAN_LIMITS_MISMATCH")
        try:
            ids = [str(uuid.UUID(str(i))) for i in instrument_ids]
        except ValueError:
            raise RepairError("ALLOWLIST_INVALID") from None
        if not ids or len(set(ids)) != len(ids) or len(ids) > limits.batch_size:
            raise RepairError("ALLOWLIST_INVALID")
        items = {i["instrument_id"]: i for i in plan["items"]}
        if not set(ids) <= set(items):
            raise RepairError("ALLOWLIST_OUTSIDE_PLAN")
        if conn.info.transaction_status != TransactionStatus.IDLE:
            raise RepairError("CONNECTION_NOT_IDLE")
        # Attempt timestamps use this host's clock, persistence times the
        # database's: an ahead host would fail every apply at the attempt or
        # revision CHECK. Refused before any fetch; timestamps are never rewritten.
        skew = _clock_skew(conn, clock or _utc_now)
        result["clock_skew_ms"] = round(skew.total_seconds() * 1000, 1)
        if skew > MAX_CLOCK_AHEAD:
            raise RepairError("CLOCK_SKEW")
        for iid in ids:
            outcome = _apply_instrument(conn, items[iid], dt.date.fromisoformat(plan["as_of"]),
                                        client, budget, validate_only)
            result["instruments"].append(outcome)
            if outcome["status"] == "committed":
                result["changed_rows"] += outcome["rows"]
            elif outcome["status"] == "validated":
                result["validated_rows"] += outcome["rows"]
            elif outcome["status"] == "skipped":
                code = outcome["code"]
                result["residual_counts"][code] = result["residual_counts"].get(code, 0) + outcome["rows"]
            elif outcome["status"] == "unknown":
                result["possibly_changed_rows"] = outcome["rows"]
                raise RepairError("COMMIT_UNKNOWN")
            elif outcome["status"] == "stopped":
                raise RepairError(outcome["code"], 5)
    except (RepairError, TiingoDeadlineExceeded, TiingoBudgetExceeded, KeyboardInterrupt) as exc:
        if isinstance(exc, RepairError):
            code, exit_code = exc.code, exc.exit_code
        else:
            code, exit_code = ("INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "PROVIDER_BUDGET"), 5
        if exit_code == 4 and result["instruments"]:
            exit_code = 5
        result.update(status="stopped" if exit_code in (4, 5) else "failed", code=code, exit_code=exit_code)
    except psycopg.Error as exc:
        incompatible = exc.sqlstate in ("42501", "42P01", "42703", "42883", "0A000")
        expired = exc.sqlstate == "57014" and budget.remaining() <= 0
        result.update(status="stopped" if expired else "failed",
                      code="TIME_BUDGET" if expired else (
                          "SCHEMA_ACCESS_INCOMPATIBLE" if incompatible else "DATABASE_ERROR"),
                      exit_code=5 if expired else (3 if incompatible else 2))
    except Exception:
        result.update(status="failed", code="VALIDATION_OR_PROVIDER_ERROR", exit_code=2)
    finally:
        result["requests"] = budget.requests
    # A stopped instrument has an outcome but still needs the resumed run.
    completed_ids = _normalized_ids(i["instrument_id"] for i in result["instruments"]
                                    if i["status"] != "stopped")
    result["unprocessed_instruments"] = len(_normalized_ids(instrument_ids) - completed_ids)
    return result
