"""Database-free scenarios for the warmer's production-safety properties.

Each ``scenario(module)`` takes the warmer module (the real one, or a mutated
copy built by ``test_eod_history_mutants``) and returns ``None`` when the
property holds or a one-line description of how it fails. The same functions
back the regression tests (``test_eod_foreign_listing_coverage``) and the
mutation-kill suite, so a mutant is judged by the very scenario that pins the
rule.

The connection is a stand-in that only tracks whether a transaction is open:
a read that takes table locks is simulated by the fake plan / snapshot reads
opening one, and the property is that no Tiingo call ever runs while one is
open. The real-lock proof (``pg_locks`` on a hypertable with many chunks) runs
against TimescaleDB in ``test_eod_foreign_listing_coverage_db``.
"""

from __future__ import annotations

import contextlib
import datetime as dt
from typing import Any
from unittest import mock

import psycopg

from src.workers import eod_history_validation as v

D = dt.date
AS_OF = D(2026, 10, 9)
READ_RANGE = (D(1970, 1, 1), AS_OF)       # what a pass reads: [CALENDAR_SUPPORTED_FROM, as_of]
NOW = dt.datetime(2026, 10, 10, 12, 0, tzinfo=dt.UTC)
GOOD_META = {"name": "Alpha", "exchangeCode": "NYSE", "startDate": "2026-10-05",
             "endDate": "2026-10-09"}
CERTIFIED = v.Verdict(v.VERDICT_COMPLETE, "verified: 0 sessions", (), "digest")


class FakeConn:
    """A psycopg connection reduced to its transaction state."""

    def __init__(self) -> None:
        self.in_txn = False
        self.commits = 0
        self.rollbacks = 0
        self.cursors = 0

    def commit(self) -> None:
        self.in_txn = False
        self.commits += 1

    def rollback(self) -> None:
        self.in_txn = False
        self.rollbacks += 1

    def cursor(self) -> Any:
        self.cursors += 1
        raise AssertionError("the database was touched")

    def __enter__(self) -> FakeConn:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


class RecordingTiingo:
    """Meta and price answers fixed by the scenario; every call notes whether a
    transaction was open on the connection when it was made."""

    def __init__(self, conn: FakeConn, meta: tuple[str, Any], prices: tuple[str, Any]) -> None:
        self.conn, self.meta, self.prices = conn, meta, prices
        self.calls: list[str] = []
        self.held: list[str] = []

    def _note(self, kind: str) -> None:
        self.calls.append(kind)
        if self.conn.in_txn:
            self.held.append(kind)

    def fetch_meta_result(self, ticker: str) -> tuple[str, Any]:
        self._note("meta")
        return self.meta

    def fetch_daily_bars_result(self, ticker: str, start: dt.date, end: dt.date) -> tuple[str, Any]:
        self._note("prices")
        return self.prices


def _patched(module: Any, **fakes: Any) -> contextlib.ExitStack:
    stack = contextlib.ExitStack()
    for name, value in fakes.items():
        stack.enter_context(mock.patch.object(module, name, value, create=True))
    return stack


def cover(module: Any, *, meta: tuple[str, Any] = ("found", GOOD_META),
          prices: tuple[str, Any] = ("success_new", [{}]), footprint: Any = 0,
          promote: Any = None, tickers: tuple[str, ...] = ("AAA",)):
    """Run ``cover_foreign_history`` on a stand-in connection. ``plan`` and the
    stored-row snapshot open a transaction (as their real reads do); every
    other database helper is replaced by a recorder that commits like the real
    one. Returns ``(conn, tiingo, log, stats)``."""
    conn = FakeConn()
    tiingo = RecordingTiingo(conn, meta, prices)
    log: dict[str, Any] = {"snapshots": 0, "promotions": 0, "seeded": 0, "recorded": []}

    def plan(c, names, *, now, as_of):
        c.in_txn = True
        return {"complete": [], "waiting": {},
                "pending": [module.HistoryTask(t, None, None) for t in names]}

    def snapshot(c, ticker, **kwargs):
        c.in_txn = True
        log["snapshots"] += 1
        return {}

    def seed(c, ticker, meta_):
        c.commit()
        log["seeded"] += 1
        return "inserted"

    def record(c, ticker, status, *, detail=None, **kwargs):
        log["recorded"].append((ticker, status, detail))
        c.commit()

    def promoted(c, ticker, verdict, **kwargs):
        log["promotions"] += 1
        c.commit()
        return len(verdict.rows)

    def count(c, a, b):
        c.in_txn = True       # the count is a read: it opens a transaction
        # only "over" for the range a pass really reads
        return footprint if (a, b) == READ_RANGE else 0

    footprint_fn = footprint if callable(footprint) else count
    with _patched(
        module, ensure_status_table=lambda c: c.commit(), plan_foreign_history=plan,
        _stored_rows=snapshot, seed_listing_instrument=seed, record_ticker_status=record,
        promote=promote or promoted, chunk_footprint=footprint_fn,
        validate_series=lambda *a, **k: CERTIFIED,
    ):
        stats = module.cover_foreign_history(conn, tiingo, list(tickers), as_of=AS_OF, cap=5, now=NOW)
    return conn, tiingo, log, stats


# ──────────────────────────────────────────────────────────────────────────────
# A. No read transaction (and so no chunk lock) is held across an HTTP call
# ──────────────────────────────────────────────────────────────────────────────
def snapshot_released(module: Any) -> str | None:
    """Neither the plan's reads nor the stored-row snapshot may stay open across
    the meta call, the price request, or the phase's return (the 429 and
    no-key continuations included)."""
    for label, meta, prices in (
        ("price success", ("found", GOOD_META), ("success_new", [{}])),
        ("price rate_limited", ("found", GOOD_META), ("rate_limited", None)),
        ("price not_configured", ("found", GOOD_META), ("not_configured", None)),
        ("meta rate_limited", ("rate_limited", None), ("success_new", [{}])),
    ):
        conn, tiingo, log, _ = cover(module, meta=meta, prices=prices, tickers=("AAA", "BBB"))
        if tiingo.held:
            return f"{label}: a transaction was open during {sorted(set(tiingo.held))} calls"
        if conn.in_txn:
            return f"{label}: the phase returned with a transaction open"
    return None


# ──────────────────────────────────────────────────────────────────────────────
# B. Chunk-footprint preflight
# ──────────────────────────────────────────────────────────────────────────────
def footprint_ceiling(module: Any) -> str | None:
    limit = module.MAX_VERIFICATION_CHUNKS
    if limit != 800:
        return f"ceiling is {limit}, documented 800"
    # Over the ceiling: recorded, visible in the stats, nothing else done.
    conn, tiingo, log, stats = cover(module, footprint=limit + 1)
    if tiingo.calls or log["snapshots"] or log["promotions"] or log["seeded"]:
        return f"over the ceiling something still ran: {tiingo.calls} {log}"
    rec = log["recorded"]
    if len(rec) != 1 or rec[0][1] != "history_incomplete" or not str(rec[0][2]).startswith(
            "chunk_footprint_exceeded"):
        return f"over the ceiling the status was {rec}"
    if stats.get("chunk_footprint_exceeded") != 1 or stats.get("errors") != 1:
        return f"over the ceiling the stats were {stats}"
    # At the ceiling: inclusive, the pass goes ahead.
    _, tiingo, log, _ = cover(module, footprint=limit)
    if "meta" not in tiingo.calls or log["promotions"] != 1:
        return f"at the ceiling the pass did not run: {tiingo.calls} {log}"
    # Promotion repeats the check before it touches the database.
    for count, expect_refusal in ((limit + 1, True), (limit, False)):
        conn = FakeConn()
        with _patched(module, chunk_footprint=lambda c, a, b, n=count: n if (a, b) == READ_RANGE else 0):
            try:
                module.promote(conn, "AAA", CERTIFIED, through=AS_OF, history_start=D(2026, 10, 5))
            except module.ChunkFootprintExceeded:
                refused = True
            except AssertionError:      # reached the database: past the preflight
                refused = False
        if refused != expect_refusal or (refused and conn.cursors):
            return f"promote at {count} chunks: refused={refused}, cursors={conn.cursors}"
    # A promotion-time refusal is a recorded incomplete, not an unexpected failure.
    def growing(c, ticker, verdict, **kwargs):
        raise module.ChunkFootprintExceeded("chunk_footprint_exceeded: 801 chunks")
    _, _, log, stats = cover(module, promote=growing)
    rec = log["recorded"]
    if len(rec) != 1 or not str(rec[0][2]).startswith("chunk_footprint_exceeded"):
        return f"a promotion-time refusal was recorded as {rec}"
    if stats.get("chunk_footprint_exceeded") != 1:
        return f"a promotion-time refusal is not counted: {stats}"
    return None


# ──────────────────────────────────────────────────────────────────────────────
# G1. Malformed metadata dates are a retryable failure, never an interval
# ──────────────────────────────────────────────────────────────────────────────
MALFORMED_DATES = ("2020-01-01garbage", "2020-01-01T12:00:00Z", "20200101", 20200101,
                   " 2020-01-01", "2020-01-01T00:00:00+01:00", "2020-13-01", "2020-01-01 ",
                   "2020-01-01T00:00:00", "2020-01-01T00:00:00.5Z", "２０２０-０１-０１", True)
DOCUMENTED_DATES = ("2026-10-05", "2026-10-05T00:00:00.000Z", "2026-10-05T00:00:00Z")


def metadata_dates(module: Any) -> str | None:
    for bad in MALFORMED_DATES:           # the seeding path parses the same way
        if module._parse_meta_date(bad) is not None:
            return f"_parse_meta_date({bad!r}) accepted a malformed date"
    for good in DOCUMENTED_DATES:
        if module._parse_meta_date(good) != D(2026, 10, 5):
            return f"_parse_meta_date({good!r}) did not accept a documented form"
    for field in ("startDate", "endDate"):
        for bad in MALFORMED_DATES:
            _, tiingo, log, stats = cover(module, meta=("found", {**GOOD_META, field: bad}))
            rec = log["recorded"]
            if (len(rec) != 1 or rec[0][1] != "history_incomplete"
                    or not str(rec[0][2]).startswith("meta:malformed")):
                return f"{field}={bad!r}: recorded {rec}"
            if log["seeded"] or tiingo.calls != ["meta"] or log["snapshots"]:
                return f"{field}={bad!r}: it went on (seeded={log['seeded']}, calls={tiingo.calls})"
        for good in DOCUMENTED_DATES:
            _, tiingo, log, _ = cover(module, meta=("found", {**GOOD_META, field: good}))
            if tiingo.calls != ["meta", "prices"] or log["promotions"] != 1:
                return f"{field}={good!r} (documented form) was not accepted: {tiingo.calls}"
    # Absent is not malformed: the established paths are unchanged.
    for absent in (None, ""):
        _, _, log, _ = cover(module, meta=("found", {**GOOD_META, "startDate": absent}))
        if [r[1:3] for r in log["recorded"]] != [("tiingo_unknown", "no_start_date")]:
            return f"startDate={absent!r}: recorded {log['recorded']}"
        _, _, log, _ = cover(module, meta=("found", {**GOOD_META, "endDate": absent}))
        if [r[1:3] for r in log["recorded"]] != [("history_incomplete", "meta_without_end_date")]:
            return f"endDate={absent!r}: recorded {log['recorded']}"
    return None


# ──────────────────────────────────────────────────────────────────────────────
# G2 / G4. The entrypoint: source states and an isolated discovery failure
# ──────────────────────────────────────────────────────────────────────────────
class RingTiingo:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.fetched: list[str] = []

    def __enter__(self) -> RingTiingo:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def fetch_daily_bars(self, ticker: str, start: dt.date, end: dt.date) -> list:
        self.fetched.append(ticker)
        return []


def run_entrypoint(module: Any, discover: Any):
    """``run`` on a stand-in connection whose W1c discovery is ``discover``.
    Returns ``(stats, conn, ring_tickers_fetched, cover_calls)``."""
    conn = FakeConn()
    ring = RingTiingo()
    covers: list[Any] = []
    with _patched(
        module, connect=lambda dsn: conn, advisory_lock=lambda c, key: contextlib.nullcontext(True),
        ensure_status_table=lambda c: None, ensure_instruments=lambda c: 0,
        foreign_listing_tickers=discover, read_cursor=lambda c: None,
        warming_universe=lambda c: ["AAA", "BBB"], _ticker_watermarks=lambda c: {},
        write_cursor=lambda c, t: None, TiingoClient=lambda **kw: ring,
        cover_foreign_history=lambda *a, **k: covers.append(a) or {"processed": 1},
    ):
        stats = module.run("dsn", calc_date=AS_OF.isoformat(), limit=10, history_limit=25)
    return stats, conn, ring.fetched, covers


def discovery_isolation(module: Any) -> str | None:
    errors = (psycopg.errors.InsufficientPrivilege("permission denied for table"),
              psycopg.errors.QueryCanceled("canceling statement due to statement timeout"),
              psycopg.errors.UndefinedTable("relation does not exist"),
              RuntimeError("resolver exploded"))
    for exc in errors:
        def failing(c, as_of, exc=exc):
            c.in_txn = True
            raise exc
        try:
            stats, conn, fetched, covers = run_entrypoint(module, failing)
        except Exception as raised:           # noqa: BLE001
            return f"{type(exc).__name__}: discovery failure escaped run(): {type(raised).__name__}"
        want = {"source": "error", "reason": type(exc).__name__, "errors": 1}
        if stats.get("foreign_history") != want:
            return f"{type(exc).__name__}: foreign_history was {stats.get('foreign_history')}"
        if fetched != ["AAA", "BBB"] or "aborted" in stats:
            return f"{type(exc).__name__}: the ring did not run (fetched={fetched}, stats={stats})"
        if conn.rollbacks < 1 or conn.in_txn:
            return f"{type(exc).__name__}: the failed transaction was not rolled back"
        if covers:
            return f"{type(exc).__name__}: the history phase ran after a failed discovery"
    return None


def source_states(module: Any) -> str | None:
    stats, _, fetched, covers = run_entrypoint(module, lambda c, as_of: [])
    got = stats.get("foreign_history")
    if (not isinstance(got, dict) or got.get("source") != "empty"
            or got.get("source_tickers") != 0 or got.get("completed") != 0
            or got.get("processed") != 0 or covers or fetched != ["AAA", "BBB"]):
        return f"an empty source reported {got}"
    stats, *_ = run_entrypoint(module, lambda c, as_of: None)
    if stats.get("foreign_history") != {"source": "absent"}:
        return f"an absent resolver reported {stats.get('foreign_history')}"
    stats, _, _, covers = run_entrypoint(module, lambda c, as_of: ["TSM"])
    if len(covers) != 1 or stats.get("foreign_history") != {"processed": 1}:
        return f"a source with tickers reported {stats.get('foreign_history')}"
    return None
