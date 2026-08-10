"""Tests for the bond_live_daily worker and its provider client.

The stage functions are exercised against a tiny fake connection rather than a
disposable Postgres, because what needs pinning here is ORCHESTRATION -- which
window each bond is asked for, what is written, when a commit lands, and how a
provider failure is reported -- none of which needs a real planner. The SQL these
stages emit is separately drift-locked below and validated against production
before deploy.

ONE test needs a real database and says so (``SEC_TEST_DATABASE_URL``, skipped
without it): the one about what happens when PostgreSQL REFUSES a write. Its
whole subject is the aborted-transaction state -- the second statement raising
``InFailedSqlTransaction`` on top of the first -- which is a property of the
server, not of the worker, and a fake that raised on cue would be pinning the
test author's belief about Postgres rather than Postgres.
"""
from __future__ import annotations

import contextlib
import datetime as _dt
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

from src.bonds import live_daily
from src.workers import _finnhub, bond_live_daily


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.rowcount = len(rows) or 1

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Cursor:
    def __init__(self, conn) -> None:
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._conn.writes.append((sql, params))
        self.rowcount = 1


class FakeConn:
    """Answers queries from a prefix->rows table; records every write."""

    def __init__(self, answers: dict[str, list[tuple]]) -> None:
        self.answers = answers
        self.writes: list[tuple] = []
        self.commits = 0

    def execute(self, sql, params=None):
        for marker, rows in self.answers.items():
            if marker in sql:
                return _Result(rows)
        return _Result([])

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1


class FakeClient:
    """Records the windows it was asked for; replays scripted payloads."""

    def __init__(self, candles=None, curve=None, ticks=None, fail: set | None = None) -> None:
        self._candles = candles or {}
        self._curve = curve or {}
        self._ticks = ticks or {}
        self._fail = fail or set()
        self.candle_calls: list[tuple] = []
        self.tick_calls: list[tuple] = []

    def daily_candles(self, isin, from_ts, to_ts):
        self.candle_calls.append((isin, from_ts, to_ts))
        if isin in self._fail:
            raise _finnhub.FinnhubTransientError("down")
        return self._candles.get(isin, {"s": "no_data"})

    def yield_curve(self, code):
        if code in self._fail:
            raise _finnhub.FinnhubTransientError("down")
        return self._curve.get(code, {"data": []})

    def ticks(self, isin, day, **kwargs):
        self.tick_calls.append((isin, day))
        return self._ticks.get(isin, {"t": []})

    def stats(self):
        return {"http_calls": len(self.candle_calls)}


DAY = _dt.date(2026, 8, 6)
TODAY = _dt.date(2026, 8, 7)
UNIVERSE = [("912828XX1", "US912828XX10", 4.0, _dt.date(2031, 8, 6))]

#: A curve that answered for every tenor. Handed to RUN-level fixtures on
#: purpose: since 2026-08-08 a stage 2 that loaded no tenor at all is a red run
#: (an all-empty curve used to read exactly like a healthy one), so a fixture
#: meant to represent a green day has to give the curve a day. Stage 2's own
#: unit tests below script individual tenors and read the totals, so they keep
#: ``FakeClient``'s empty default -- an empty answer is what they are about.
HEALTHY_CURVE = {
    tenor: {"data": [{"d": TODAY.isoformat(), "v": 4.0}]}
    for tenor in bond_live_daily.CURVE_TENORS
}

#: Markers the FakeConn answers by. Kept next to each other because a run()-level
#: test drives all four query shapes through one connection.
Q_UNIVERSE = "FROM bond_reference_terms r"
Q_WATERMARK = "max(o.day)"
Q_ATTEMPTS = "FROM bond_live_daily_sweep"
Q_CURVE_WATERMARK = "GROUP BY tenor"
Q_ACTIVITY = "coalesce(sum(o.volume)"

REG_S_SNAPSHOT_ID = "7d2b63ce-63a0-534b-9741-d10242d399ad"


class _FakeConnect:
    """Stands in for src.db.connect: hands the same FakeConn to every caller."""

    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn
        self.opened = 0

    def __call__(self, dsn, **kwargs):
        self.opened += 1
        return self

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc):
        return False


def _drive_run(
    monkeypatch,
    *,
    conn: FakeConn | None = None,
    client=None,
    client_error: BaseException | None = None,
    acquired: bool = True,
    relations: bool = True,
    matview: dict | None = None,
    republish: dict | None = None,
    panel: dict | None = None,
    limit: int | None = None,
    calc_date: _dt.date = TODAY,
    connector: "_FakeConnect | None" = None,
    events: list[tuple[str, int]] | None = None,
    mapping_snapshot_id: str | None = REG_S_SNAPSHOT_ID,
    resolver=None,
):
    """Run ``bond_live_daily.run`` against fakes, exercising the REAL verdict.

    Only the seams that need a database are replaced (the connection, the lock,
    the DDL install, the matview refresh, the two publication workers). The stage
    functions, the coverage arithmetic and the state/abort decision are the
    shipping ones -- which is the whole point: what these tests pin is which
    outcomes are allowed to exit green.

    ``events`` opts into an ORDER trace: every replaced seam appends
    ``(name, conn.commits)`` as it is entered, so a test can assert both where
    the lock is released relative to stages 4 and 5 and whether the load
    connection was quiesced before them.
    """
    conn = conn if conn is not None else FakeConn({Q_UNIVERSE: list(UNIVERSE)})

    def _note(name: str) -> None:
        if events is not None:
            events.append((name, conn.commits))

    @contextlib.contextmanager
    def _lock(_conn, _lock_id):
        _note("lock_acquired")
        try:
            yield acquired
        finally:
            _note("lock_released")

    monkeypatch.setattr(bond_live_daily, "resolve_dsn", lambda dsn=None: "postgresql://x")
    monkeypatch.setattr(
        bond_live_daily, "connect", connector if connector is not None else _FakeConnect(conn)
    )
    monkeypatch.setattr(bond_live_daily, "advisory_lock", _lock)
    monkeypatch.setattr(bond_live_daily, "install_schema", lambda _c: None)
    monkeypatch.setattr(bond_live_daily, "_relation_exists", lambda _c, _n: relations)

    def _matview(_dsn):
        _note("matview")
        return matview or {"state": "refreshed", "matview": "bond_curated_securities"}

    def _republish_stub(_dsn):
        _note("republish")
        return republish or {"verdict": "recomputed"}

    def _panel_stub(_dsn, *, as_of=None):
        _note("panel")
        return panel or {"state": "published", "aborted": False}

    monkeypatch.setattr(bond_live_daily, "_refresh_curated", _matview)
    monkeypatch.setattr(bond_live_daily, "_republish", _republish_stub)
    monkeypatch.setattr(bond_live_daily, "_publish_panel", _panel_stub)

    def _identity_reg_s_map(_conn, *, snapshot_id, as_of, reference_cusip9s):
        references = list(reference_cusip9s)
        return SimpleNamespace(
            resolutions={
                reference: SimpleNamespace(
                    reference_cusip9=reference,
                    reg_s_cusip9=reference,
                    reg_s_isin=f"US{reference}0",
                )
                for reference in references
            },
            reason_by_reference={},
        )

    monkeypatch.setattr(
        bond_live_daily,
        "resolve_reg_s_cusip_map_from_db",
        resolver if resolver is not None else _identity_reg_s_map,
    )
    if mapping_snapshot_id is None:
        monkeypatch.delenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", raising=False)
    else:
        monkeypatch.setenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", mapping_snapshot_id)

    def _client():
        if client_error is not None:
            raise client_error
        return client if client is not None else FakeClient(curve=HEALTHY_CURVE)

    monkeypatch.setattr(bond_live_daily, "client_from_env", _client)
    return bond_live_daily.run(calc_date=calc_date.isoformat(), limit=limit)


def _candle_payload(day: _dt.date, price: float, yield_pct: float | None = 4.5):
    return {"s": "ok", "t": [live_daily.to_epoch(day)], "c": [price],
            "y": [yield_pct] if yield_pct is not None else [None]}


# --------------------------------------------------------------------------- #
# Governed Reg S execution universe
# --------------------------------------------------------------------------- #
def test_daily_loads_the_mapped_reg_s_isin_and_writes_the_execution_cusip(monkeypatch) -> None:
    """A Rule 144A identifier is never a provider fallback for Reg S execution."""
    resolver_calls: list[dict[str, object]] = []

    def resolve(_conn, *, snapshot_id, as_of, reference_cusip9s):
        references = list(reference_cusip9s)
        resolver_calls.append({
            "snapshot_id": snapshot_id,
            "as_of": as_of,
            "reference_cusip9s": references,
        })
        return SimpleNamespace(
            resolutions={
                "912828XX1": SimpleNamespace(
                    reference_cusip9="912828XX1",
                    reg_s_cusip9="G12345678",
                    reg_s_isin="XS1234567890",
                )
            },
            reason_by_reference={},
        )

    conn = FakeConn({Q_UNIVERSE: list(UNIVERSE)})
    client = FakeClient(candles={"XS1234567890": _candle_payload(TODAY, 99.0)})

    out = _drive_run(monkeypatch, conn=conn, client=client, resolver=resolve)

    assert resolver_calls == [
        {
            "snapshot_id": REG_S_SNAPSHOT_ID,
            "as_of": _dt.date(2026, 7, 31),
            "reference_cusip9s": ["912828XX1"],
        },
        {
            "snapshot_id": REG_S_SNAPSHOT_ID,
            "as_of": TODAY,
            "reference_cusip9s": ["912828XX1"],
        },
    ]
    assert [call[0] for call in client.candle_calls] == ["XS1234567890"]
    observation_writes = [
        params for sql, params in conn.writes if "INSERT INTO bond_observation_daily" in sql
    ]
    assert observation_writes and observation_writes[0][0] == "G12345678"
    assert out["coverage"]["universe"] == 1
    assert out["mapping_coverage"] == {
        "snapshot_id": REG_S_SNAPSHOT_ID,
        "reference_total": 1,
        "resolved": 1,
        "executable": 1,
        "omissions": {},
        "closed_as_of": "2026-07-31",
        "open_as_of": "2026-08-07",
        "closed": {"resolved": 1, "executable": 1, "omissions": {}},
        "open": {"resolved": 1, "executable": 1, "omissions": {}},
    }


def test_missing_reg_s_isin_is_a_coverage_omission_with_no_provider_or_write(monkeypatch) -> None:
    """Dropping execution ISIN must not silently reuse the Rule 144A ISIN."""
    def resolve(_conn, **_kwargs):
        return SimpleNamespace(
            resolutions={
                "912828XX1": SimpleNamespace(
                    reference_cusip9="912828XX1",
                    reg_s_cusip9="G12345678",
                )
            },
            reason_by_reference={},
        )

    conn = FakeConn({Q_UNIVERSE: list(UNIVERSE)})
    client = FakeClient(candles={"US912828XX10": _candle_payload(TODAY, 99.0)})

    out = _drive_run(monkeypatch, conn=conn, client=client, resolver=resolve)

    assert out["state"] == "no_executable_reg_s_universe"
    assert out["aborted"] is True
    assert out["mapping_coverage"]["omissions"] == {"missing_reg_s_isin": 1}
    assert client.candle_calls == [] and client.tick_calls == []
    assert conn.writes == []


def test_daily_sweep_executes_the_closed_and_open_reg_s_legs_when_mapping_changes(monkeypatch) -> None:
    """A closed-month backfill must not use the current Reg S identifier."""
    resolver_calls: list[_dt.date] = []

    def resolve(_conn, *, as_of, **_kwargs):
        resolver_calls.append(as_of)
        if as_of == _dt.date(2026, 7, 31):
            resolution = SimpleNamespace(
                reference_cusip9="912828XX1",
                reg_s_cusip9="OLDREG001",
                reg_s_isin="XS1234567890",
            )
        else:
            resolution = SimpleNamespace(
                reference_cusip9="912828XX1",
                reg_s_cusip9="NEWREG002",
                reg_s_isin="XS0987654321",
            )
        return SimpleNamespace(resolutions={"912828XX1": resolution}, reason_by_reference={})

    conn = FakeConn({Q_UNIVERSE: list(UNIVERSE)})
    client = FakeClient(candles={
        "XS1234567890": _candle_payload(TODAY, 99.0),
        "XS0987654321": _candle_payload(TODAY, 98.0),
    })

    out = _drive_run(monkeypatch, conn=conn, client=client, resolver=resolve)

    assert resolver_calls == [_dt.date(2026, 7, 31), TODAY]
    assert {isin for isin, *_window in client.candle_calls} == {"XS1234567890", "XS0987654321"}
    written_cusips = {
        params[0] for sql, params in conn.writes if "INSERT INTO bond_observation_daily" in sql
    }
    assert written_cusips == {"OLDREG001", "NEWREG002"}
    assert out["mapping_coverage"]["closed"] == {
        "resolved": 1, "executable": 1, "omissions": {},
    }
    assert out["mapping_coverage"]["open"] == {
        "resolved": 1, "executable": 1, "omissions": {},
    }


def test_cross_as_of_cusip_collision_is_a_typed_omission_not_an_identifier_guess(monkeypatch) -> None:
    """One CUSIP with two as-of ISINs cannot be selected silently for either leg."""
    def resolve(_conn, *, as_of, **_kwargs):
        return SimpleNamespace(
            resolutions={
                "912828XX1": SimpleNamespace(
                    reference_cusip9="912828XX1",
                    reg_s_cusip9="G12345678",
                    reg_s_isin=("XS1234567890" if as_of == _dt.date(2026, 7, 31) else "XS0987654321"),
                )
            },
            reason_by_reference={},
        )

    monkeypatch.setattr(bond_live_daily, "resolve_reg_s_cusip_map_from_db", resolve)
    rows, total, coverage = bond_live_daily._universe(
        FakeConn({"to_regclass": [(1,)], Q_UNIVERSE: list(UNIVERSE)}),
        None,
        snapshot_id=REG_S_SNAPSHOT_ID,
        as_of=TODAY,
    )

    assert rows == [] and total == 0
    assert coverage["closed"]["omissions"] == {"ambiguous_execution_cusip": 1}
    assert coverage["open"]["omissions"] == {"ambiguous_execution_cusip": 1}


def test_missing_mapping_snapshot_aborts_before_provider_calls(monkeypatch) -> None:
    """The immutable mapping pointer is a precondition, never an optional label."""
    client = FakeClient(candles={"US912828XX10": _candle_payload(TODAY, 99.0)})

    out = _drive_run(monkeypatch, client=client, mapping_snapshot_id=None)

    assert out["state"] == "no_reg_s_mapping_snapshot"
    assert out["aborted"] is True
    assert client.candle_calls == [] and client.tick_calls == []


# --------------------------------------------------------------------------- #
# Stage 1: candles
# --------------------------------------------------------------------------- #
def test_the_window_starts_at_each_bond_s_own_watermark() -> None:
    conn = FakeConn({"max(o.day)": [("912828XX1", DAY)]})
    client = FakeClient(candles={"US912828XX10": _candle_payload(TODAY, 99.0)})

    stats = bond_live_daily._load_candles(conn, client, UNIVERSE, TODAY)

    (_, from_ts, to_ts) = client.candle_calls[0]
    # The watermark day itself is re-read (a revised close must not be frozen).
    assert from_ts == live_daily.to_epoch(DAY)
    assert to_ts == live_daily.to_epoch(TODAY)
    assert stats["with_data"] == 1 and stats["last_day"] == TODAY.isoformat()
    assert conn.commits >= 1


def test_a_higher_ranked_bulk_day_cannot_move_the_live_lane_s_watermark() -> None:
    """The window follows what THIS lane loaded, not what the table holds.

    The upsert only refreshes same-or-lower-ranked rows, so a bulk day that
    outranks this feed and lands later is a day the worker can neither write nor
    step past: opening the next window there would skip every live day in
    between, and a delta never looks back. The source qualification is what
    makes the fake's answer -- and production's -- the LIVE max rather than the
    table's.
    """
    # Drift lock on the qualification itself. The source is INLINED, not bound,
    # because Postgres only uses the partial index when it can prove the query's
    # predicate implies the index's (see schemas/bond_live_daily.sql).
    assert f"WHERE o.source = '{live_daily.SOURCE_LIVE}'" in bond_live_daily._WATERMARK_SQL

    # A bond the bulk lanes cover but this feed never wrote answers NOTHING to a
    # source-qualified watermark, so it must get the cold-start window -- not one
    # opening at a bulk row's day, which is the bypass the qualification closes.
    conn = FakeConn({"max(o.day)": []})
    client = FakeClient(candles={"US912828XX10": _candle_payload(TODAY, 99.0)})

    bond_live_daily._load_candles(conn, client, UNIVERSE, TODAY)

    cold_start, _ = live_daily.fetch_window(None, TODAY)
    (_, from_ts, _) = client.candle_calls[0]
    assert from_ts == live_daily.to_epoch(cold_start)


def test_the_watermark_predicate_matches_the_partial_index_it_needs() -> None:
    """Two halves of one plan: if they drift, the daily read silently reverts.

    Measured on production 2026-08-07 (34.6M rows, 26 chunks): the qualified
    watermark costs 0.10s on this index and 45.5s without it -- a sequential
    scan of every chunk, which no test can see and no log line reports.
    """
    ddl = bond_live_daily.SCHEMA_PATH.read_text(encoding="utf-8")
    assert "bond_observation_daily_live_watermark_idx" in ddl
    assert f"WHERE source = '{live_daily.SOURCE_LIVE}'" in ddl
    # Guarded: install_schema runs BEFORE run() gets to report an absent
    # hypertable, so an unguarded reference would crash that reported no-op.
    assert "to_regclass('bond_observation_daily')" in ddl


def test_a_bond_the_provider_has_nothing_for_is_reported_not_failed() -> None:
    conn = FakeConn({"max(o.day)": []})
    stats = bond_live_daily._load_candles(conn, FakeClient(), UNIVERSE, TODAY)
    assert stats["no_data"] == 1
    assert stats["transient_failures"] == 0
    assert stats["aborted"] is False


def test_transient_provider_failures_are_counted_and_the_sweep_continues() -> None:
    universe = UNIVERSE + [("912828XX2", "US912828XX28", 4.0, _dt.date(2031, 8, 6))]
    conn = FakeConn({"max(o.day)": []})
    client = FakeClient(
        candles={"US912828XX28": _candle_payload(TODAY, 99.0)},
        fail={"US912828XX10"},
    )
    stats = bond_live_daily._load_candles(conn, client, universe, TODAY)
    assert stats["transient_failures"] == 1
    assert stats["with_data"] == 1
    assert stats["aborted"] is False


def test_a_sustained_provider_outage_aborts_rather_than_burning_the_window() -> None:
    isins = [f"US91282800{i:02d}" for i in range(60)]
    universe = [(f"9128280{i:02d}", isin, 4.0, _dt.date(2031, 8, 6))
                for i, isin in enumerate(isins)]
    conn = FakeConn({"max(o.day)": []})
    client = FakeClient(fail=set(isins))

    stats = bond_live_daily._load_candles(conn, client, universe, TODAY)

    assert stats["aborted"] is True
    assert stats["swept"] == _finnhub.MAX_CONSECUTIVE_FAILURES
    assert stats["swept"] < len(universe), "the sweep must stop, not finish"


def test_the_upsert_carries_the_full_declared_column_protocol() -> None:
    conn = FakeConn({"max(o.day)": []})
    client = FakeClient(candles={"US912828XX10": _candle_payload(TODAY, 99.0)})
    bond_live_daily._load_candles(conn, client, UNIVERSE, TODAY)

    sql, params = conn.writes[0]
    assert "INSERT INTO bond_observation_daily" in sql
    assert len(params) == len(live_daily.OBSERVATION_COLUMNS)
    # The idempotency rule itself: same-rank rows refresh, higher-rank ones win.
    assert "EXCLUDED.source_rank >= bond_observation_daily.source_rank" in sql


# --------------------------------------------------------------------------- #
# Stage 2: curve
# --------------------------------------------------------------------------- #
CURVE_WATERMARKS = "GROUP BY tenor"


def _curve_writes(conn: FakeConn, tenor: str) -> list[str]:
    """The days written for one tenor, in order. as_tuple() is (day, tenor, ...)."""
    return sorted(
        params[0].isoformat()
        for sql, params in conn.writes
        if "INSERT INTO bond_yield_curve_daily" in sql and params[1] == tenor
    )


def test_one_dead_tenor_does_not_cost_the_others() -> None:
    conn = FakeConn({CURVE_WATERMARKS: []})  # nothing loaded yet: cold start
    client = FakeClient(
        curve={"10y": {"data": [{"d": "2026-08-06", "v": 4.69}]}},
        fail={"30y"},
    )
    stats = bond_live_daily._load_curve(conn, client, TODAY)
    assert stats["failed_tenors"] == ["30y"]
    assert stats["tenors"] == 1
    assert stats["latest_day"] == "2026-08-06"
    # An empty table has no bound for anyone, so nothing is reported behind.
    assert stats["lagging_tenors"] == []


def test_a_lagging_tenor_recovers_the_days_the_others_advanced_past() -> None:
    """One tenor's outage must not become a permanent hole.

    While 30y was down the other tenors advanced the table's max day. Trimming
    the recovered 30y response -- which carries the provider's FULL history --
    at that table-wide max would discard exactly the days that were missed, and
    nothing ever asks for them again. Per tenor, the gap closes itself.
    """
    gap = ["2026-08-04", "2026-08-05", "2026-08-06"]
    conn = FakeConn({CURVE_WATERMARKS: [
        ("10y", _dt.date(2026, 8, 6)),   # kept advancing
        ("30y", _dt.date(2026, 8, 3)),   # three sessions behind
    ]})
    client = FakeClient(curve={
        "30y": {"data": [{"d": d, "v": 5.0} for d in ["2026-08-03", *gap]]},
        "10y": {"data": [{"d": "2026-08-06", "v": 4.69}]},
    })

    stats = bond_live_daily._load_curve(conn, client, TODAY)

    # Its own watermark day (re-read) plus every day it missed.
    assert _curve_writes(conn, "30y") == ["2026-08-03", *gap]
    # The tenor that never lagged is untouched by the other's recovery.
    assert _curve_writes(conn, "10y") == ["2026-08-06"]
    assert stats["lagging_tenors"] == ["30y"]


def test_a_tenor_never_loaded_is_not_filtered_by_its_neighbours_bound() -> None:
    """A new tenor cold-starts on the whole history, or lands empty forever."""
    conn = FakeConn({CURVE_WATERMARKS: [("10y", _dt.date(2026, 8, 6))]})
    client = FakeClient(curve={"30y": {"data": [
        {"d": "2019-01-02", "v": 3.0}, {"d": "2026-08-06", "v": 5.0},
    ]}})

    stats = bond_live_daily._load_curve(conn, client, TODAY)

    assert _curve_writes(conn, "30y") == ["2019-01-02", "2026-08-06"]
    assert stats["tenors"] == 1
    # Never loaded is a cold start, not a lag: it is about to load everything.
    assert stats["lagging_tenors"] == []


def test_a_replay_never_advances_the_curve_past_the_day_it_asked_for() -> None:
    """The curve was the last lane without a ceiling, and it needed one.

    The other lanes bound the REQUEST (``fetch_window``'s ``to``, the tick
    session, the activity ranking); the curve asks for no window at all -- one
    call returns the tenor's entire history, and the fold is the only place a
    ceiling can be applied. So on a replay (``WORKER_CALC_DATE``) against a
    partially loaded or cold curve table, this used to upsert every point AFTER
    the requested day as well: ``bond_yield_curve_daily`` walked forward with
    rates from sessions the replay is not loading, and stage 2 reported
    ``latest_day`` as today on a run whose prices stopped in May.

    Every call still succeeds, so nothing in the run's JSON said it was wrong --
    the same shape as the tick-cohort defect, on the lane that was argued to be
    the deliberate exception.
    """
    replay = _dt.date(2026, 5, 15)
    conn = FakeConn({CURVE_WATERMARKS: []})   # cold: no watermark trims anything
    client = FakeClient(curve={"10y": {"data": [
        {"d": "2026-05-14", "v": 4.40},
        {"d": "2026-05-15", "v": 4.42},
        # Real sessions, both of them -- just not the ones this run is loading.
        {"d": "2026-06-15", "v": 4.71},
        {"d": "2026-08-06", "v": 4.69},
    ]}})

    stats = bond_live_daily._load_curve(conn, client, replay)

    assert _curve_writes(conn, "10y") == ["2026-05-14", "2026-05-15"]
    assert stats["latest_day"] == replay.isoformat()
    assert stats["rows_upserted"] == 2


def test_a_tenor_already_past_the_replay_date_writes_nothing_rather_than_crashing() -> None:
    """The inverted window a replay creates: watermark AFTER the requested day.

    It is the normal state of a replay on a healthy table -- every tenor is
    loaded up to yesterday and the operator asks for a day in May. The fold has
    to land on empty, not raise, and the tenor must not be reported as loaded.
    """
    conn = FakeConn({CURVE_WATERMARKS: [("10y", _dt.date(2026, 8, 6))]})
    client = FakeClient(curve={"10y": {"data": [{"d": "2026-08-06", "v": 4.69}]}})

    stats = bond_live_daily._load_curve(conn, client, _dt.date(2026, 5, 15))

    assert _curve_writes(conn, "10y") == []
    assert stats["tenors"] == 0 and stats["rows_upserted"] == 0
    # Nothing to load is not a failure: the tenor answered, it had no new day.
    assert stats["failed_tenors"] == []
    # And it is the ONE empty fold that proves nothing about the provider, so it
    # is filed apart from the empties that do. The other twelve tenors have no
    # watermark here and the fake answers them with `{"data": []}`: a 200 that
    # folded to nothing for a reason no bound explains.
    assert stats["skipped_tenors"] == ["10y"]
    assert stats["empty_tenors"] == [
        t for t in bond_live_daily.CURVE_TENORS if t != "10y"
    ]


# --------------------------------------------------------------------------- #
# Stage 3: ticks
# --------------------------------------------------------------------------- #
def test_the_tick_lane_asks_for_the_previous_session_only() -> None:
    conn = FakeConn({
        "coalesce(sum(o.volume)": [("912828XX1", 1_000_000)],
    })
    client = FakeClient(ticks={"US912828XX10": {
        "t": [1, 2], "p": [99.0, 101.0], "si": [1, 2], "v": [10, 20],
    }})
    stats = bond_live_daily._load_ticks(conn, client, UNIVERSE, TODAY)
    assert client.tick_calls == [("US912828XX10", DAY.isoformat())]
    assert stats["traded"] == 1 and stats["day"] == DAY.isoformat()
    assert stats["aborted"] is False


def test_the_default_tick_scope_attempts_every_eligible_resolved_cusip() -> None:
    """An unset cap is the full resolved universe, not an activity head."""
    universe = [_bond("FULLSCOPE1"), _bond("FULLSCOPE2"), _bond("FULLSCOPE3")]
    client = FakeClient(ticks={
        row[1]: {"t": [], "total": 0}
        for row in universe
    })

    stats = bond_live_daily._load_ticks(FakeConn({}), client, universe, TODAY)

    assert [isin for isin, _ in client.tick_calls] == [row[1] for row in universe]
    assert stats["scope"] == "full_universe"
    assert stats["configured_top_n"] is None
    assert stats["degraded"] is False
    assert stats["attempted_cusips"] == 3 and stats["api_calls"] == 3
    assert stats["successes"] == 3 and stats["no_trades"] == 3
    assert stats["failures"] == 0 and stats["elapsed_seconds"] >= 0


def test_tick_payload_outcomes_distinguish_empty_error_malformed_and_zero_trades() -> None:
    """A 200 without a usable tape fails; an explicit empty tape is a quiet day."""
    universe = [_bond("EMPTYTAPE"), _bond("ERRORTAPE"), _bond("BADTAPE00"), _bond("ZEROTAPE0")]
    client = FakeClient(ticks={
        "USEMPTYTAPE0": {},
        "USERRORTAPE0": {"error": "unavailable"},
        "USBADTAPE000": {"t": "not-a-list"},
        "USZEROTAPE00": {"t": [], "total": 0},
    })

    stats = bond_live_daily._load_ticks(FakeConn({}), client, universe, TODAY)

    assert stats["successes"] == 1 and stats["no_trades"] == 1
    assert stats["failures"] == 3 and stats["transient_failures"] == 0
    assert stats["failure_reasons"] == {
        "api_empty": 1, "api_error": 1, "malformed_payload": 1,
    }
    assert stats["no_trade_reasons"] == {"valid_zero_trades": 1}
    assert stats["aborted"] is False


@pytest.mark.parametrize(
    "payload",
    [
        {"__finnhub_payload_state": "ok", "t": [1_722_470_400]},
        {"__finnhub_payload_state": "ok", "t": [1_722_470_400], "p": []},
        {"__finnhub_payload_state": "ok", "t": "not-a-list", "p": [100.0]},
    ],
)
def test_ok_tick_payload_state_does_not_bypass_structural_validation(payload) -> None:
    assert bond_live_daily._tick_payload_outcome(payload) == "malformed_payload"


@pytest.mark.parametrize(
    "payload",
    [
        {"t": [1_722_470_400], "p": [100.0]},
        {"t": [1_722_470_400, 1_722_470_401], "p": [100.0, 100.1], "si": [1]},
    ],
)
def test_nonempty_tick_payload_requires_a_side_for_each_trade(payload) -> None:
    assert bond_live_daily._tick_payload_outcome(payload) == "malformed_payload"


class _NoTape(FakeClient):
    """Every tick call exhausts its retry ladder and raises."""

    def ticks(self, isin, day, **kwargs):
        self.tick_calls.append((isin, day))
        raise _finnhub.FinnhubTransientError("down")


def _tick_cohort(size: int) -> tuple[list[tuple], list[tuple]]:
    """``(universe rows, activity rows)`` for ``size`` equally active bonds."""
    cusips = [f"9128280{i:02d}" for i in range(size)]
    return ([_bond(c) for c in cusips], [(c, 1_000) for c in cusips])


def test_a_sustained_outage_stops_the_tick_sweep_instead_of_burning_the_day() -> None:
    """The cost lane gets stage 1's breaker, on stage 1's constant.

    By the time ``client.ticks()`` raises, the client has already spent its whole
    retry ladder -- 126s of backoff per exhausted logical request (measured
    2026-08-07, after the trailing-sleep fix). Unbraked, an outage walks the
    entire default 500-bond cohort proving the provider is down: ~17.5h of
    backoff alone, hours past the 11:00 publication window, on a run that is
    already red. Braked, the worst case is the candle sweep's: 25 x 126s ~= 52min.
    """
    universe, activity = _tick_cohort(60)
    conn = FakeConn({Q_ACTIVITY: activity})

    stats = bond_live_daily._load_ticks(conn, _NoTape(), universe, TODAY)

    assert stats["aborted"] is True
    assert stats["swept"] == _finnhub.MAX_CONSECUTIVE_FAILURES
    assert stats["swept"] < stats["cohort"], "the sweep must stop, not finish"


def test_one_bad_tick_call_among_good_ones_is_not_an_outage() -> None:
    """CONSECUTIVE, like stage 1: any success resets the counter.

    A breaker that counted total failures would abort a healthy day on the 25th
    scattered timeout and report a cost lane the provider never refused.
    """
    universe, activity = _tick_cohort(60)
    conn = FakeConn({Q_ACTIVITY: activity})

    class _Flaky(FakeClient):
        def ticks(self, isin, day, **kwargs):
            self.tick_calls.append((isin, day))
            if len(self.tick_calls) % 2:
                raise _finnhub.FinnhubTransientError("down")
            return {"t": [1], "p": [99.0], "si": [1], "v": [10]}

    stats = bond_live_daily._load_ticks(conn, _Flaky(), universe, TODAY)

    assert stats["aborted"] is False
    assert stats["swept"] == 60 and stats["transient_failures"] == 30
    assert stats["traded"] == 30


def test_an_outage_that_cut_the_tape_short_fails_a_run_whose_calls_mostly_worked(
    monkeypatch,
) -> None:
    """The run-level half, and the case that used to exit green.

    Ten bonds' tape landed and then the provider went away, so ``swept`` and
    ``transient_failures`` disagree and the "every call failed" clause never
    fires -- while the cohort stops at bond 35 of 60. Unlike stage 1 there is no
    tick watermark to resume from: tomorrow's run asks for tomorrow's session, so
    the tape of every bond the outage cut off is gone for good. That is why a
    truncated cost lane is a failed run rather than a progress report.
    """
    universe, activity = _tick_cohort(60)
    conn = FakeConn({Q_UNIVERSE: universe, Q_ACTIVITY: activity})

    class _OutageAfter(FakeClient):
        def ticks(self, isin, day, **kwargs):
            self.tick_calls.append((isin, day))
            if len(self.tick_calls) > 10:
                raise _finnhub.FinnhubTransientError("down")
            return {"t": [1], "p": [99.0], "si": [1], "v": [10]}

    out = _drive_run(monkeypatch, conn=conn, client=_OutageAfter(curve=HEALTHY_CURVE))

    ticks = out["ticks"]
    assert ticks["aborted"] is True
    assert ticks["traded"] == 10, "what landed before the outage stays landed"
    assert ticks["transient_failures"] == _finnhub.MAX_CONSECUTIVE_FAILURES
    assert ticks["swept"] == 10 + _finnhub.MAX_CONSECUTIVE_FAILURES < ticks["cohort"]
    # One state for one event -- the provider stopped answering. Not folded into
    # ``aborted``, which names the CANDLE sweep: a run whose prices landed
    # cleanly must not send an operator to stage 1.
    assert out["state"] == "ticks_failed"
    assert out["aborted"] is True
    assert out["candles"]["aborted"] is False


class ActivityConn(FakeConn):
    """A connection whose activity query applies the bounds the SQL DECLARES.

    Fidelity is not the point; discrimination is. It aggregates the observation
    rows it was handed under exactly the day predicates that appear in
    ``_ACTIVITY_SQL``, so a query with no upper bound really does rank the cohort
    on rows the requested date has not reached -- which is the defect. A
    parameter the SQL binds but the caller never supplied raises here, the way
    psycopg would, so a half-applied fix cannot pass by accident either.
    """

    def __init__(self, observations: list[tuple], **answers) -> None:
        super().__init__(dict(answers))
        self._observations = observations

    def execute(self, sql, params=None):
        if Q_ACTIVITY not in sql:
            return super().execute(sql, params)
        bound = params or {}
        for name in ("since", "until"):
            if f"%({name})s" in sql and name not in bound:
                raise KeyError(name)
        totals: dict[str, float] = {}
        for cusip9, day, volume in self._observations:
            if "o.day >= %(since)s" in sql and day < bound["since"]:
                continue
            if "o.day <= %(until)s" in sql and day > bound["until"]:
                continue
            totals[cusip9] = totals.get(cusip9, 0.0) + volume
        return _Result(sorted(totals.items()))


def test_a_replay_ranks_the_tick_cohort_on_the_day_it_asked_for(monkeypatch) -> None:
    """Rows the replay date has not reached must not choose its cohort.

    ``WORKER_CALC_DATE`` replays an earlier session against a database that
    already holds later ones. Ranked through an open-ended window the top-N is
    dominated by bonds that became liquid AFTERWARDS, so the worker asks the
    provider for the replay day's tape of bonds that were not trading then -- and
    skips the ones that were. Every call succeeds, so nothing in the run's JSON
    says the cohort was wrong.
    """
    monkeypatch.setenv("BOND_TICK_TOP_N", "1")
    replay = _dt.date(2026, 5, 15)
    universe = [_bond("ACTIVETHEN"), _bond("ACTIVELATER"), _bond("ACTIVEAGES")]
    conn = ActivityConn([
        ("ACTIVETHEN", replay - _dt.timedelta(days=5), 1_000.0),
        # Nine thousand times as active -- but not until a month after the day
        # being replayed.
        ("ACTIVELATER", replay + _dt.timedelta(days=30), 9_000_000.0),
        # ...and the floor still holds: liquid last year is not liquid now.
        ("ACTIVEAGES", replay - _dt.timedelta(days=200), 5_000_000.0),
    ])
    client = FakeClient()

    stats = bond_live_daily._load_ticks(conn, client, universe, replay)

    assert client.tick_calls == [
        ("USACTIVETHEN0", live_daily.previous_business_day(replay).isoformat())
    ]
    assert stats["cohort"] == 1


def test_the_requested_day_s_own_session_counts_toward_the_cohort(monkeypatch) -> None:
    """The ceiling is INCLUSIVE: ``day == calc_date`` is the freshest evidence.

    It is also the day the cohort is being chosen for, so an exclusive bound
    would rank a daily run on everything except the session that just printed.
    """
    monkeypatch.setenv("BOND_TICK_TOP_N", "1")
    universe = [_bond("ONTHEDAYX"), _bond("EARLIERXX")]
    conn = ActivityConn([
        ("ONTHEDAYX", TODAY, 1_000.0),
        ("EARLIERXX", TODAY - _dt.timedelta(days=10), 500.0),
    ])
    client = FakeClient()

    bond_live_daily._load_ticks(conn, client, universe, TODAY)

    assert [isin for isin, _ in client.tick_calls] == ["USONTHEDAYX0"]


# --------------------------------------------------------------------------- #
# Stage 5: republish
# --------------------------------------------------------------------------- #
def test_a_failed_republication_is_flagged_so_the_run_exits_non_zero(monkeypatch) -> None:
    """"Load and recompute" -- a day whose recompute failed is not a green day.

    Nothing retries it before tomorrow: the 11:00 chain's run_id does not change
    just because this worker failed, so a silent success here would leave the
    product a day stale with no signal at all.
    """
    from src.workers import bond_metrics, bond_serving

    monkeypatch.setattr(bond_metrics, "run", lambda *a, **k: {"state": "ok"})
    monkeypatch.setattr(
        bond_serving, "run", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    out = bond_live_daily._republish("postgresql://x")
    assert out["verdict"] == "failed"
    assert out["bond_serving"]["state"] == "failed"

    # A worker that merely REPORTS a failed state counts too, not just a raise.
    monkeypatch.setattr(bond_serving, "run", lambda *a, **k: {"state": "failed"})
    assert bond_live_daily._republish("postgresql://x")["verdict"] == "failed"

    monkeypatch.setattr(bond_serving, "run", lambda *a, **k: {"state": "ok"})
    assert bond_live_daily._republish("postgresql://x")["verdict"] == "recomputed"


@pytest.mark.parametrize(
    "result, verdict",
    [
        # The publication protocol's own success states. ``current`` is a
        # self-promoted publication: a success, not an unrecognised result.
        ({"state": "ok"}, "recomputed"),
        ({"state": "current"}, "recomputed"),
        ({"state": "ready"}, "recomputed"),
        # The lock is already held -- by the 11:00 chain, or by a manual
        # rebuild. The worker RETURNED; it did not publish.
        ({"state": "locked"}, "locked"),
        # Dark: it ran and had nothing to publish. Also not a recompute.
        ({"state": "no_source"}, "no_op"),
        ({"state": "no_securities"}, "no_op"),
        ({"state": "no_observations"}, "no_op"),
        ({"state": "failed"}, "failed"),
        # Contract drift is never tolerated into a success (the chain's rule).
        ({"state": "half_published"}, "failed"),
        ({}, "failed"),
    ],
)
def test_only_a_worker_that_says_it_published_counts_as_a_recompute(result, verdict) -> None:
    """The whole vocabulary of the two publication workers, classified once.

    ``locked`` is the case that used to pass: both workers return it when their
    advisory lock is already held, and the old check only looked for ``failed``,
    so an overlapping chain or manual rebuild left this run green with stage 5
    never executed -- the day loaded and unserved behind a successful deploy.
    """
    assert bond_live_daily.republish_verdict(result) == verdict


def test_a_locked_publication_worker_stops_the_chain_and_fails_the_run(monkeypatch) -> None:
    """And it stops at the FIRST one: serving consumes the metric view."""
    from src.workers import bond_metrics, bond_serving

    seen: list[str] = []
    monkeypatch.setattr(
        bond_metrics, "run", lambda *a, **k: seen.append("metrics") or {"state": "locked"}
    )
    monkeypatch.setattr(
        bond_serving, "run", lambda *a, **k: seen.append("serving") or {"state": "ok"}
    )
    out = bond_live_daily._republish("postgresql://x")

    assert out["verdict"] == "locked"
    assert seen == ["metrics"], "a serving build over a stale metric view is not a recovery"

    run_out = _drive_run(monkeypatch, republish=out)
    assert run_out["state"] == "republish_locked"
    assert run_out["aborted"] is True


def test_a_dark_republication_is_not_a_green_day(monkeypatch) -> None:
    """Nothing was published, so the day's rows are loaded and unserved."""
    out = _drive_run(
        monkeypatch,
        republish={"bond_metrics": {"state": "no_source"}, "verdict": "no_op"},
    )
    assert out["state"] == "republish_no_op"
    assert out["aborted"] is True


def test_a_failed_matview_refresh_is_reported_and_never_costs_the_republication(
    monkeypatch,
) -> None:
    """REFRESH needs OWNERSHIP, not a grant -- so this stage can fail on privilege.

    It sits between the load and the republication, so raising would mean a
    permission problem silently costs the product the day's prices. It is caught
    and folded into the exit code instead: all the work runs, then the verdict.
    """
    import inspect

    source = inspect.getsource(bond_live_daily._refresh_curated)
    assert "except Exception" in source
    assert '"state": "failed"' in source

    out = _drive_run(
        monkeypatch,
        matview={"state": "failed", "error": "InsufficientPrivilege: must be owner"},
    )
    assert out["state"] == "matview_failed"
    assert out["aborted"] is True
    # The refresh failing must not skip the republication: all the work runs,
    # then the verdict. Read off the shipping code path's own output -- a flag
    # set while building the fixture would be set whether stage 5 ran or not.
    assert out["republish"]["verdict"] == "recomputed"


def test_matview_connection_failure_is_a_typed_stage_result(monkeypatch) -> None:
    def fail_connect(*_args, **_kwargs):
        raise OSError("database unavailable")

    monkeypatch.setattr(bond_live_daily, "connect", fail_connect)

    outcome = bond_live_daily._refresh_curated("postgresql://unreachable")

    assert outcome["state"] == "failed"
    assert outcome["matview"] == "bond_curated_securities"
    assert outcome["error"] == "OSError: database unavailable"


def test_a_matview_that_is_absent_is_a_stage_that_did_no_work(monkeypatch) -> None:
    """The hole the state table had: ``absent`` is not ``failed``, and used to pass.

    ``_refresh_curated`` reports three outcomes and only one of them refreshed
    anything. A verdict that enumerated ``failed`` let the third -- the matview
    missing from the database entirely -- exit GREEN with stage 4 having done
    nothing at all, which is a schema/deploy fault hidden behind a successful
    daily deploy, exactly the tier the exit contract removed everywhere else.

    So the clause asserts the SUCCESS state instead, which is the drift rule
    ``republish_verdict`` already applies to the publication workers: a state
    this contract does not know is never read as a success. One state,
    ``matview.state`` in the JSON says which shape and therefore which hand --
    ``failed`` is the ownership prerequisite, ``absent`` is a missing relation.
    """
    import inspect

    # The state this test is about has to still be a thing stage 4 can return.
    assert '"state": "absent"' in inspect.getsource(bond_live_daily._refresh_curated)

    out = _drive_run(
        monkeypatch, matview={"state": "absent", "matview": "bond_curated_securities"}
    )
    assert out["state"] == "matview_failed"
    assert out["aborted"] is True
    assert out["matview"]["state"] == "absent", "the JSON still says which shape"
    # And the load is not thrown away with it: stage 5 still ran.
    assert out["republish"]["verdict"] == "recomputed"

    # A state nobody has written yet is red too, rather than green by omission.
    assert _drive_run(monkeypatch, matview={"state": "skipped"})["state"] == "matview_failed"

    # The one green shape is the one that actually refreshed.
    assert _drive_run(monkeypatch, matview={"state": "refreshed"})["state"] == "ok"


def test_the_daily_lock_is_held_through_the_panel_publication(
    monkeypatch,
) -> None:
    """Stages 4 through 6 run INSIDE the lock, or the lock protects only writes.

    Released after stage 3, an overlapping manual restart takes this worker's
    lock while the first run is still refreshing and republishing. The second run
    commits a PREFIX of its own revised candles into ``bond_observation_daily``
    while the first run's ``bond_metrics``/``bond_serving`` build is reading it,
    then aborts on the publication locks -- and the first run exits green having
    served a mix of two sweeps. Nothing downstream can see it: every row is
    individually valid. The lock has to cover the READ, not just the write.

    The trace also pins the half that makes holding it free. A session advisory
    lock is not a transaction, but an uncommitted connection IS one, and stage 5
    takes MINUTES: the load connection is committed before stage 4 and never
    touched again, so it sits idle rather than idle-in-transaction and holds back
    no VACUUM horizon (the trap this repo has already paid for).
    """
    events: list[tuple[str, int]] = []
    out = _drive_run(monkeypatch, events=events)

    assert out["state"] == "ok"
    assert [name for name, _ in events] == [
        "lock_acquired", "matview", "republish", "panel", "lock_released"
    ]

    commits = dict(events)
    assert commits["matview"] > 0, "the load connection must be committed before stage 4"
    assert commits["panel"] == commits["republish"] == commits["matview"] == commits["lock_released"], (
        "nothing may run on the held connection while the publications build"
    )


def test_missing_provider_configuration_defers_panel_after_the_end_stages(monkeypatch) -> None:
    """Stages 4-5 still run, but stale inputs may not advance the panel pointer."""
    events: list[tuple[str, int]] = []
    out = _drive_run(
        monkeypatch,
        client_error=_finnhub.FinnhubConfigError("rejected"),
        events=events,
    )

    assert out["state"] == "no_api_key"
    assert [name for name, _ in events] == [
        "lock_acquired", "matview", "republish", "lock_released"
    ]
    assert out["panel"] == {
        "state": "deferred",
        "aborted": False,
        "reason": "input_lanes_failed",
        "blocked_by": ["no_api_key", "aborted", "curve_failed"],
    }


@pytest.mark.parametrize(
    ("loader", "stage_result", "expected_state"),
    [
        (
            "_load_candles",
            {"swept": 1, "resumed": 1, "with_data": 0, "aborted": False},
            "candles_failed",
        ),
        (
            "_load_curve",
            {"tenors": 0, "skipped_tenors": [], "failed_tenors": [], "empty_tenors": []},
            "curve_failed",
        ),
        (
            "_load_ticks",
            {
                "swept": 1,
                "failures": 1,
                "transient_failures": 1,
                "aborted": False,
                "degraded": False,
            },
            "ticks_failed",
        ),
        (
            "_load_ticks",
            {
                "swept": 1,
                "failures": 0,
                "transient_failures": 0,
                "aborted": False,
                "degraded": True,
            },
            "ticks_degraded_scope",
        ),
    ],
)
def test_failed_input_lane_defers_stage_six(
    monkeypatch, loader: str, stage_result: dict, expected_state: str
) -> None:
    events: list[tuple[str, int]] = []
    monkeypatch.setattr(bond_live_daily, loader, lambda *_args, **_kwargs: stage_result)

    out = _drive_run(monkeypatch, events=events)

    assert out["state"] == expected_state
    assert out["panel"]["state"] == "deferred"
    assert out["panel"]["reason"] == "input_lanes_failed"
    assert expected_state in out["panel"]["blocked_by"]
    assert "panel" not in [name for name, _ in events]


def test_run_worker_reads_the_top_level_aborted_key(monkeypatch) -> None:
    """The exit-code contract lives on that exact key -- keep them wired."""
    import inspect

    from src import run_worker

    assert 'stats.get("aborted")' in inspect.getsource(run_worker.main)
    assert _drive_run(monkeypatch)["aborted"] is False


# --------------------------------------------------------------------------- #
# The exit contract: which outcomes are allowed to exit green
# --------------------------------------------------------------------------- #
def test_a_complete_run_is_the_only_green_one(monkeypatch) -> None:
    out = _drive_run(monkeypatch)
    assert out["state"] == "ok"
    assert out["aborted"] is False
    assert out["halted_by"] == []
    assert out["coverage"] == {
        "universe": 1, "swept": 1, "remaining": 0, "complete": True, "limit": None,
    }


@pytest.mark.parametrize("panel_state", ["failed", "publish_failed", "gate_failed"])
def test_panel_failures_are_end_only_verdict_reasons(monkeypatch, panel_state) -> None:
    out = _drive_run(monkeypatch, panel={"state": panel_state, "aborted": True})

    assert out["state"] == f"panel_{panel_state}"
    assert f"panel_{panel_state}" in out["halted_by"]


def test_panel_already_current_is_a_healthy_daily_outcome(monkeypatch) -> None:
    out = _drive_run(
        monkeypatch,
        panel={"state": "current", "aborted": False, "reason": "panel_month_already_current"},
    )

    assert out["aborted"] is False
    assert out["state"] == "ok"
    assert out["panel"]["state"] == "current"


@pytest.mark.parametrize(
    "kwargs, state",
    [
        # Another holder has the lock: this run did NOTHING. Typed abort rather
        # than an in-process retry -- the cron/restartPolicy=NEVER argument.
        ({"acquired": False}, "locked"),
        # The serving repository never applied the hypertable's DDL.
        ({"relations": False}, "no_observation_table"),
        # The curated universe is empty or absent.
        ({"conn": FakeConn({})}, "no_universe"),
        # The Railway service is missing FINNHUB_API_KEY: no candles, no curve,
        # no ticks, no republication -- and, before this, a green deploy.
        ({"client_error": _finnhub.FinnhubConfigError("FINNHUB_API_KEY is not set")},
         "no_api_key"),
    ],
)
def test_a_run_that_did_no_work_never_exits_green(monkeypatch, kwargs, state) -> None:
    """The no-op exits are the ones most easily laundered into a green deploy.

    Each of these returns early having loaded nothing at all, and ``run_worker``
    exits non-zero on the top-level ``aborted`` key alone -- so an early return
    that omits it IS the silent failure. A missing secret, an absent hypertable
    and a held lock are operational faults with an operator behind them; the
    only place they can be seen is the deploy's exit code.
    """
    out = _drive_run(monkeypatch, **kwargs)
    assert out["state"] == state
    assert out["aborted"] is True
    # Same key on every result, so one log query reads them all.
    assert out["halted_by"][0] == state
    if state != "no_api_key":
        assert out["halted_by"] == [state]


def test_a_calc_date_past_the_execution_date_is_refused_never_clamped(monkeypatch) -> None:
    """The one input that can put a future day inside the requested window.

    ``fetch_window``'s ``to`` is always ``calc_date``, and that same value is the
    ``not_after`` bound ``candle_rows`` enforces -- so with a future
    ``WORKER_CALC_DATE`` every provider stamp in ``(today, calc_date]`` is
    accepted as a real session. ``max(day)`` of the observation table anchors the
    as_of of BOTH publications, so one such row dates the product into the future
    and turns every legitimate publication after it into an as-of regression that
    only a manual delete in production clears.

    Clamping it to today would hide that AND silently rewrite an operator's
    explicit parameter -- replaying a date nobody asked for while the logs named
    the one they did. It is refused instead, before a connection is opened.
    """
    conn = FakeConn({Q_UNIVERSE: list(UNIVERSE)})
    connector = _FakeConnect(conn)
    tomorrow = _dt.date.today() + _dt.timedelta(days=1)

    out = _drive_run(monkeypatch, conn=conn, connector=connector, calc_date=tomorrow)

    assert out["state"] == "calc_date_in_future"
    assert out["aborted"] is True
    assert out["halted_by"] == ["calc_date_in_future"]
    assert out["calc_date"] == tomorrow.isoformat()
    # Refused before anything opens: no connection, no lock, no DDL, no writes.
    assert connector.opened == 0
    assert conn.writes == [] and conn.commits == 0

    # The boundary is the execution date itself, which is not the future.
    assert _drive_run(monkeypatch, calc_date=_dt.date.today())["state"] == "ok"


def test_a_key_revoked_mid_sweep_is_typed_rather_than_a_traceback(monkeypatch) -> None:
    """4xx is non-transient by design, so it RAISES past the sweep's counter."""
    class _Revoked(FakeClient):
        def daily_candles(self, isin, from_ts, to_ts):
            raise _finnhub.FinnhubConfigError("401 bad key")

    out = _drive_run(monkeypatch, client=_Revoked())
    assert out["state"] == "provider_rejected"
    assert out["aborted"] is True


def test_a_stage_that_did_no_work_is_not_a_stage_that_had_nothing_to_do(
    monkeypatch,
) -> None:
    """ALL 13 tenors failing is the provider or the key, not a dropped tenor.

    A handful of failed tenors stays green on purpose -- that is precisely the
    case the per-tenor watermarks heal on the next run -- but a stage that
    returned nothing at all has done no work, and the run must say so.
    """
    conn = FakeConn({Q_UNIVERSE: list(UNIVERSE)})
    out = _drive_run(
        monkeypatch, conn=conn, client=FakeClient(fail=set(bond_live_daily.CURVE_TENORS))
    )
    assert out["curve"]["failed_tenors"] == list(bond_live_daily.CURVE_TENORS)
    assert out["state"] == "curve_failed"
    assert out["aborted"] is True

    # One dead tenor out of thirteen is still a green day.
    ok = _drive_run(monkeypatch, client=FakeClient(curve=HEALTHY_CURVE, fail={"30y"}))
    assert ok["curve"]["failed_tenors"] == ["30y"]
    assert ok["curve"]["tenors"] == len(bond_live_daily.CURVE_TENORS) - 1
    assert ok["state"] == "ok"


def test_a_tick_lane_that_failed_every_call_is_reported_not_swallowed(monkeypatch) -> None:
    class _NoTape(FakeClient):
        def ticks(self, isin, day, **kwargs):
            raise _finnhub.FinnhubTransientError("down")

    conn = FakeConn({
        Q_UNIVERSE: list(UNIVERSE),
        Q_ACTIVITY: [("912828XX1", 1_000_000)],
    })
    out = _drive_run(monkeypatch, conn=conn, client=_NoTape(curve=HEALTHY_CURVE))
    assert out["ticks"]["swept"] == 1 and out["ticks"]["transient_failures"] == 1
    assert out["state"] == "ticks_failed"
    assert out["aborted"] is True


# --------------------------------------------------------------------------- #
# An EMPTY lane is a lane that did no work
#
# The hole both of these close is the same one, in the two stages that load from
# a window: a provider that keeps answering 200 with nothing in it. Every call
# succeeds, every counter stays plausible, and the run republishes yesterday and
# exits green having loaded zero rows for the whole universe. What makes it a
# judgeable event rather than a guess is that each stage has a day it can PROVE
# the provider owes it -- a day this feed already loaded from that same endpoint.
# --------------------------------------------------------------------------- #
def _loaded(*cusips: str, day: _dt.date = DAY) -> list[tuple]:
    """Watermark rows: bonds this lane has already loaded up to ``day``."""
    return [(cusip, day) for cusip in cusips]


def _curve_at(day: _dt.date) -> list[tuple]:
    """A curve table loaded up to ``day`` for every tenor."""
    return [(tenor, day) for tenor in bond_live_daily.CURVE_TENORS]


def test_a_sweep_that_re_asked_for_loaded_days_and_got_nothing_is_not_green(
    monkeypatch,
) -> None:
    """The all-empty candle sweep, which used to be indistinguishable from a
    quiet day.

    Entitlement lost and surfaced as JSON, a shape change under ``s``/``t``/``c``,
    an ISIN mapping that stopped resolving: none of them raise. ``_load_candles``
    counted ``no_data`` and left ``aborted`` false, so stage 1 loaded nothing for
    the entire curated universe and the run still refreshed, republished and
    exited **ok** -- serving yesterday's observations as today's work.

    The evidence that makes it judgeable is the WINDOW. Every bond here carries a
    watermark, so its window re-opens on a day this feed already loaded FROM THIS
    PROVIDER: the provider served that day once, so an empty answer for it is a
    fault and not a market. That is what ``resumed`` counts.
    """
    conn = FakeConn({Q_UNIVERSE: list(UNIVERSE), Q_WATERMARK: _loaded("912828XX1")})
    out = _drive_run(monkeypatch, conn=conn, client=FakeClient(curve=HEALTHY_CURVE))

    assert out["candles"]["resumed"] == 1
    assert out["candles"]["with_data"] == 0 and out["candles"]["no_data"] == 1
    # Not the breaker: every call SUCCEEDED. That is the whole point.
    assert out["candles"]["aborted"] is False
    assert out["candles"]["transient_failures"] == 0
    assert out["state"] == "candles_failed"
    assert out["aborted"] is True

    # The control, on the same connection shape: one bond that answered is the
    # difference between a broken provider and a working one.
    green = _drive_run(
        monkeypatch,
        conn=FakeConn({Q_UNIVERSE: list(UNIVERSE), Q_WATERMARK: _loaded("912828XX1")}),
        client=FakeClient(
            candles={"US912828XX10": _candle_payload(TODAY, 99.0)}, curve=HEALTHY_CURVE
        ),
    )
    assert green["candles"]["with_data"] == 1
    assert green["state"] == "ok"


def test_a_cold_table_is_not_evidence_that_the_provider_broke(monkeypatch) -> None:
    """``resumed``, not ``swept`` -- and this is the first reason why.

    A bond this lane has never loaded gets ``fetch_window``'s 30-day cold-start
    window, and nothing in the database says the provider ever had a day for it.
    409 of the 10,073 curated bonds are exactly that (measured on production
    2026-08-08: attempted, never once returned data), and the sweep RING sorts
    never-loaded bonds first inside a round -- so a thin ``WORKER_LIMIT`` slice
    is *systematically* all-dataless, not unluckily so. Judged on ``swept`` this
    clause would fire on every capped run and mean nothing.
    """
    conn = FakeConn({Q_UNIVERSE: list(UNIVERSE)})   # no watermark rows at all
    out = _drive_run(monkeypatch, conn=conn, client=FakeClient(curve=HEALTHY_CURVE))

    assert out["candles"]["swept"] == 1 and out["candles"]["with_data"] == 0
    assert out["candles"]["resumed"] == 0, "no bond was asked about a day it had"
    assert out["state"] == "ok"
    assert out["halted_by"] == []


def test_a_replay_of_a_day_the_table_is_already_past_is_benign_in_both_lanes(
    monkeypatch,
) -> None:
    """The second reason, and it is the same event in stage 1 and stage 2.

    An operator replays a session in May against a table loaded to August.
    ``fetch_window`` clamps every bond's window to ``[calc_date, calc_date]`` and
    ``curve_points`` gets ``not_before > not_after`` for every tenor. If the
    replayed day was a Saturday -- or any session this provider has no tape for
    -- BOTH lanes come back with nothing, legitimately, for the entire universe.

    This repo deliberately owns no trading calendar (see
    ``live_daily.previous_business_day``: inventing one would make the lane wrong
    on exactly the days a holiday calendar is meant to fix), so the honest
    reading is that such a run proves nothing about the provider. Neither clause
    may fire.
    """
    replay = _dt.date(2026, 5, 15)
    conn = FakeConn({
        Q_UNIVERSE: list(UNIVERSE),
        Q_WATERMARK: _loaded("912828XX1", day=TODAY),   # already past the replay
        Q_CURVE_WATERMARK: _curve_at(TODAY),            # ...and so is every tenor
    })
    out = _drive_run(
        monkeypatch, conn=conn, client=FakeClient(curve=HEALTHY_CURVE), calc_date=replay
    )

    assert out["candles"]["with_data"] == 0 and out["candles"]["resumed"] == 0
    assert out["curve"]["tenors"] == 0
    assert out["curve"]["skipped_tenors"] == list(bond_live_daily.CURVE_TENORS)
    assert out["curve"]["empty_tenors"] == []
    assert out["state"] == "ok"
    assert out["halted_by"] == []


def test_a_curve_that_answered_with_nothing_for_every_tenor_did_no_work(
    monkeypatch,
) -> None:
    """The same hole in stage 2, and it did not need a failure to open.

    ``curve_points`` returns ``[]`` for a 200 whose ``data`` is empty, absent, or
    renamed. The loop just continued, so ``failed_tenors`` stayed EMPTY and the
    only run-level check -- all 13 tenors in ``failed_tenors`` -- could never
    fire: a curve table that received zero rows for every tenor read exactly like
    a healthy one.

    Unlike a candle window, this response is never empty for a market reason: one
    call returns the tenor's WHOLE history, so a weekend cannot empty it. A tenor
    whose watermark is at or before the requested day must return at least that
    watermark day, because this feed loaded that day out of this same response.
    """
    conn = FakeConn({
        Q_UNIVERSE: list(UNIVERSE),
        Q_WATERMARK: _loaded("912828XX1"),
        Q_CURVE_WATERMARK: _curve_at(DAY),
    })
    # Stage 1 is healthy, so the verdict is unambiguously about stage 2.
    client = FakeClient(candles={"US912828XX10": _candle_payload(TODAY, 99.0)})

    out = _drive_run(monkeypatch, conn=conn, client=client)

    assert out["candles"]["with_data"] == 1
    assert out["curve"]["tenors"] == 0 and out["curve"]["rows_upserted"] == 0
    # Nothing FAILED -- which is exactly why the old check could not see it.
    assert out["curve"]["failed_tenors"] == []
    assert out["curve"]["empty_tenors"] == list(bond_live_daily.CURVE_TENORS)
    assert out["state"] == "curve_failed"
    assert out["aborted"] is True


def test_a_replay_with_one_dead_tenor_is_red_even_though_twelve_were_benign(
    monkeypatch,
) -> None:
    """A documented consequence, so the next reviewer does not reopen it.

    "A handful of failed tenors stays green" is still true -- of a run that
    LOADED something. The clause asserts the success state, which is that stage 2
    loaded a tenor: a run in which nothing loaded and something failed is a run
    with a provider problem in it, whatever the other twelve were doing. The
    benign-empty exemption therefore has to be unanimous.
    """
    replay = _dt.date(2026, 5, 15)
    conn = FakeConn({
        Q_UNIVERSE: list(UNIVERSE),
        Q_CURVE_WATERMARK: _curve_at(TODAY),
    })
    out = _drive_run(
        monkeypatch,
        conn=conn,
        client=FakeClient(curve=HEALTHY_CURVE, fail={"30y"}),
        calc_date=replay,
    )

    assert out["curve"]["failed_tenors"] == ["30y"]
    assert len(out["curve"]["skipped_tenors"]) == len(bond_live_daily.CURVE_TENORS) - 1
    assert out["curve"]["tenors"] == 0
    assert out["state"] == "curve_failed"


def test_resumed_counts_the_attempt_not_the_outcome() -> None:
    """What ``resumed`` is measured over, pinned: the WINDOW, before the answer.

    A bond with a watermark that then failed transiently is still a bond that was
    asked about a day it had -- so an all-transient sweep too short to trip the
    breaker (``WORKER_LIMIT`` below ``MAX_CONSECUTIVE_FAILURES``) is red, which is
    the "every call failed" half that stage 3 always had and stage 1 did not.
    """
    universe = [_bond("LOADED0001"), _bond("LOADED0002"), _bond("COLDSTART1")]
    conn = FakeConn({Q_WATERMARK: _loaded("LOADED0001", "LOADED0002")})
    client = FakeClient(fail={"USLOADED00020"})    # one no-data, one failure

    stats = bond_live_daily._load_candles(conn, client, universe, TODAY)

    assert stats["swept"] == 3
    assert stats["resumed"] == 2, "the cold-start bond carries no promise"
    assert stats["no_data"] == 2 and stats["transient_failures"] == 1
    assert stats["with_data"] == 0 and stats["aborted"] is False

    # ...and a watermark PAST the requested day is not a re-read either: the
    # window inverts and `fetch_window` clamps it to the single replayed day.
    replay = _dt.date(2026, 5, 15)
    ahead = FakeConn({Q_WATERMARK: _loaded("LOADED0001", "LOADED0002", day=TODAY)})
    assert bond_live_daily._load_candles(ahead, FakeClient(), universe, replay)[
        "resumed"
    ] == 0


# --------------------------------------------------------------------------- #
# A refused write must surface its OWN error
# --------------------------------------------------------------------------- #
class _RefusingCursor(_Cursor):
    """A cursor that refuses one statement and then behaves like an aborted tx."""

    def execute(self, sql, params=None):
        if "INSERT INTO bond_observation_daily" in sql:
            self._conn.aborted = True
            raise RuntimeError("check constraint bond_observation_daily_price_sane")
        if self._conn.aborted:
            raise RuntimeError("current transaction is aborted")
        return super().execute(sql, params)


class RefusingConn(FakeConn):
    """FakeConn whose observation insert is refused, Postgres-style.

    Not a substitute for the real-database test below -- it cannot be, since the
    aborted-transaction rule is the server's. It pins the cheap half: which
    statement the worker attempts NEXT once a write has been refused.
    """

    def __init__(self, answers) -> None:
        super().__init__(answers)
        self.aborted = False

    def cursor(self):
        return _RefusingCursor(self)


def test_a_refused_insert_is_not_buried_under_the_progress_stamp() -> None:
    """The ``finally`` that stamped progress used to be the last thing to raise.

    An insert PostgreSQL refuses -- a constraint, a column the DDL never grew --
    leaves the transaction ABORTED, so every following statement raises
    ``InFailedSqlTransaction``. Stamping from a ``finally`` therefore ran exactly
    when it could not succeed, and a ``finally``-raised exception REPLACES the one
    already unwinding: the operator got "current transaction is aborted" and the
    actionable error was gone. The same incident ``src.db._release_advisory_lock``
    documents one frame up (2026-07-24), one frame further in.

    The stamp is called explicitly on the three HANDLED paths now, so the refused
    write simply propagates. The progress property is untouched, because the two
    never actually collided: the only path that loses its stamp is the one where
    the stamp could not have been written at all.
    """
    conn = RefusingConn({Q_WATERMARK: []})
    client = FakeClient(candles={"USBADBOND010": _candle_payload(TODAY, 99.0)})

    with pytest.raises(RuntimeError, match="check constraint"):
        bond_live_daily._load_candles(conn, client, [_bond("BADBOND01")], TODAY)

    # Nothing was attempted after the refusal -- not even the stamp, which is
    # what used to raise second and win.
    assert not any("bond_live_daily_sweep" in sql for sql, _ in conn.writes)


@pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)
def test_a_refused_insert_surfaces_its_own_error_against_a_real_transaction() -> None:
    """The same claim, proved where the aborted-transaction rule actually lives.

    A fake can be told to raise second; only a real server proves that it WOULD.
    So this one builds a disposable schema, gives ``bond_observation_daily`` a
    constraint the second bond's candle violates, and asserts two things at once:

      * the exception that escapes is the CheckViolation -- the actionable one --
        and not the ``InFailedSqlTransaction`` the progress stamp used to raise
        on top of it;
      * the sweep progress that was already committed SURVIVES. The first bond's
        row and its stamp are durable, so the next run resumes rather than
        repeating -- the property the ``finally`` existed for, kept without it.
        The bond that broke is deliberately NOT stamped: the ring must not
        advance past a bond whose rows never landed.
    """
    import psycopg

    schema = f"bld_{uuid4().hex[:12]}"
    universe = [_bond("GOODBOND1"), _bond("BADBOND02")]
    client = FakeClient(candles={
        "USGOODBOND10": _candle_payload(TODAY, 10.0),
        "USBADBOND020": _candle_payload(TODAY, 99.0),
    })

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        conn.execute(f"CREATE SCHEMA {schema}")
        conn.execute(f"SET search_path TO {schema}")
        conn.execute(
            "CREATE TABLE bond_observation_daily ("
            " cusip9 text NOT NULL, day date NOT NULL, price numeric, ytm numeric,"
            " volume numeric, price_type text, accrued text, source text,"
            " source_rank int NOT NULL, ytm_basis text,"
            " PRIMARY KEY (cusip9, day),"
            # Stands in for any constraint the served DDL carries that a payload
            # can violate. What matters is that Postgres refuses the statement.
            " CONSTRAINT bond_observation_daily_price_sane CHECK (price < 50))"
        )
        conn.execute("CREATE TABLE bond_curated_universe (cusip9 text PRIMARY KEY)")
        conn.execute(
            "CREATE TABLE bond_live_daily_sweep ("
            " cusip9 text PRIMARY KEY, last_attempt_at timestamptz NOT NULL)"
        )
        conn.commit()
        try:
            # One bond per commit, so the good bond's progress is durable before
            # the bad one breaks the transaction.
            original = bond_live_daily.COMMIT_EVERY
            bond_live_daily.COMMIT_EVERY = 1
            with pytest.raises(psycopg.errors.CheckViolation) as refused:
                bond_live_daily._load_candles(conn, client, universe, TODAY)
            assert "bond_observation_daily_price_sane" in str(refused.value)

            conn.rollback()
            assert conn.execute(
                "SELECT cusip9 FROM bond_observation_daily"
            ).fetchall() == [("GOODBOND1",)]
            assert conn.execute(
                "SELECT cusip9 FROM bond_live_daily_sweep ORDER BY 1"
            ).fetchall() == [("GOODBOND1",)]
        finally:
            bond_live_daily.COMMIT_EVERY = original
            conn.rollback()
            conn.execute(f"DROP SCHEMA {schema} CASCADE")
            conn.commit()


# --------------------------------------------------------------------------- #
# Thread 3: a capped sweep must advance, and must not claim the day
# --------------------------------------------------------------------------- #
def _order(rows, attempts=None, watermarks=None) -> list[str]:
    return [
        row[0]
        for row in bond_live_daily.sweep_priority_order(
            rows, attempts or {}, watermarks or {}
        )
    ]


def _bond(cusip: str) -> tuple:
    return (cusip, f"US{cusip}0", 4.0, _dt.date(2031, 8, 6))


def _stamp(minute: int) -> _dt.datetime:
    return _dt.datetime(2026, 8, 7, 7, minute, tzinfo=_dt.timezone.utc)


def test_successive_capped_sweeps_cover_the_universe_instead_of_one_prefix() -> None:
    """The ring property, stated as the thing an operator actually gets.

    A CUSIP-sorted prefix hands every capped run the same first N bonds, so the
    rest of the curated universe is never loaded -- the budget-bounded catch-up
    documented in the runbook never catches anything up. Ordering by the sweep
    RING makes ceil(universe / limit) runs cover everything.
    """
    universe = [_bond(f"91282800{i}") for i in range(6)]
    attempts: dict[str, _dt.datetime] = {}
    limit = 2
    seen: list[str] = []

    for run_index in range(3):
        batch = _order(universe, attempts)[:limit]
        seen.extend(batch)
        for cusip in batch:
            attempts[cusip] = _stamp(run_index)

    assert sorted(seen) == sorted(c for c, *_ in universe)
    assert len(set(seen)) == len(universe), "no bond was swept twice before all were swept"

    # And it WRAPS: the fourth run returns to the cohort of the first.
    assert _order(universe, attempts)[:limit] == seen[:limit]


def test_a_bond_the_provider_has_no_data_for_cannot_own_the_head_forever() -> None:
    """Why the ring is keyed on the attempt and not on the loaded watermark.

    A bond the provider has nothing for never gains a watermark. Under a
    "most behind first" order keyed on the watermark alone it is permanently the
    most behind, so it re-takes the head of every capped run -- and once that
    dataless cohort reaches WORKER_LIMIT the sweep stops advancing at all, which
    is the same starvation as the CUSIP prefix under a better name.
    """
    dataless = [_bond("DEAD00001"), _bond("DEAD00002")]
    fresh = [_bond("LIVE00001"), _bond("LIVE00002")]
    universe = dataless + fresh
    # Round 1 asked about the two dataless bonds and loaded nothing from them.
    attempts = {"DEAD00001": _stamp(0), "DEAD00002": _stamp(0)}
    watermarks: dict[str, _dt.date] = {}

    assert _order(universe, attempts, watermarks)[:2] == ["LIVE00001", "LIVE00002"]

    # The watermark still orders WITHIN a round: swept together, the one
    # furthest behind goes first -- which is also how a transient failure
    # (stamped, watermark unmoved) retries ahead of a clean load.
    same_round = {c: _stamp(0) for c, *_ in universe}
    assert _order(universe, same_round, {
        "DEAD00001": _dt.date(2026, 8, 6), "DEAD00002": _dt.date(2026, 1, 2),
        "LIVE00001": _dt.date(2026, 8, 7), "LIVE00002": _dt.date(2026, 8, 7),
    }) == ["DEAD00002", "DEAD00001", "LIVE00001", "LIVE00002"]


def test_every_bond_the_sweep_reaches_is_stamped_whatever_came_of_it() -> None:
    """Data, no data, or a transient failure -- the attempt is what advances.

    Stamping only successes would leave exactly the bonds the provider is worst
    at parked at the head of the ring forever.
    """
    universe = [_bond("AAAAAAAA1"), _bond("BBBBBBBB2"), _bond("CCCCCCCC3")]
    conn = FakeConn({Q_WATERMARK: []})
    client = FakeClient(
        candles={"USAAAAAAAA10": _candle_payload(TODAY, 99.0)},   # data
        fail={"USCCCCCCCC30"},                                   # transient failure
    )                                                            # B: no data

    stats = bond_live_daily._load_candles(conn, client, universe, TODAY)

    stamped = [
        params[0] for sql, params in conn.writes
        if "INSERT INTO bond_live_daily_sweep" in sql
    ]
    assert stamped == ["AAAAAAAA1", "BBBBBBBB2", "CCCCCCCC3"]
    assert stats["with_data"] == 1 and stats["no_data"] == 1
    assert stats["transient_failures"] == 1

    # The stamp lands AFTER the bond's own rows, so the ring never advances past
    # a bond whose rows did not land.
    kinds = [sql.split()[2] for sql, _ in conn.writes]
    assert kinds[:2] == ["bond_observation_daily", "bond_live_daily_sweep"]


def test_a_sweep_the_provider_cut_short_keeps_the_progress_it_made() -> None:
    """An aborted run must resume, not restart: the stamps are committed."""
    isins = [f"US91282800{i:02d}" for i in range(60)]
    universe = [(f"9128280{i:02d}", isin, 4.0, _dt.date(2031, 8, 6))
                for i, isin in enumerate(isins)]
    conn = FakeConn({Q_WATERMARK: []})

    stats = bond_live_daily._load_candles(conn, FakeClient(fail=set(isins)), universe, TODAY)

    assert stats["aborted"] is True
    stamped = [
        params[0] for sql, params in conn.writes
        if "INSERT INTO bond_live_daily_sweep" in sql
    ]
    # Exactly the prefix it swept, including the bond it aborted on.
    assert stamped == [c for c, *_ in universe[:stats["swept"]]]
    assert conn.commits >= 1, "the progress has to be durable or the next run repeats it"


def test_a_capped_run_reports_the_budget_it_covered_not_the_day(monkeypatch) -> None:
    """The second half of the defect: coverage is not a matter of taste.

    The republication still runs -- the rows it loaded are real, and every
    security carries its own observation date, so the payload stays honest. What
    must not happen is the RUN reporting a covered day while most of the curated
    universe was never asked about. It goes green on the run that finally
    finishes the ring, and that transition is the signal.
    """
    universe = [_bond(f"91282800{i}") for i in range(5)]
    conn = FakeConn({Q_UNIVERSE: universe})

    out = _drive_run(monkeypatch, conn=conn, limit=2)

    assert out["state"] == "partial_sweep"
    assert out["aborted"] is True
    assert out["coverage"] == {
        "universe": 5, "swept": 2, "remaining": 3, "complete": False, "limit": 2,
    }
    # A limit WIDER than the universe covered the day; it is not a partial run.
    assert _drive_run(monkeypatch, conn=FakeConn({Q_UNIVERSE: universe}), limit=50)[
        "state"
    ] == "ok"


def test_partial_sweep_defers_stage_six_until_a_full_rerun(monkeypatch) -> None:
    """The panel pointer may advance only after the sweep's coverage verdict is complete."""
    universe = [_bond(f"91282800{i}") for i in range(5)]
    partial_events: list[tuple[str, int]] = []

    partial = _drive_run(
        monkeypatch,
        conn=FakeConn({Q_UNIVERSE: universe}),
        limit=2,
        events=partial_events,
    )

    assert partial["state"] == "partial_sweep"
    assert partial["panel"] == {"state": "deferred", "aborted": False, "reason": "partial_sweep"}
    assert [name for name, _ in partial_events] == [
        "lock_acquired", "matview", "republish", "lock_released",
    ]

    full_events: list[tuple[str, int]] = []
    full = _drive_run(
        monkeypatch,
        conn=FakeConn({Q_UNIVERSE: universe}),
        limit=5,
        events=full_events,
    )

    assert full["state"] == "ok"
    assert full["panel"]["state"] == "published"
    assert [name for name, _ in full_events] == [
        "lock_acquired", "matview", "republish", "panel", "lock_released",
    ]


def test_the_capped_universe_is_the_ring_prefix_not_the_cusip_prefix(monkeypatch) -> None:
    """End to end through _universe: the cap slices the RING."""
    universe = [_bond("AAAAAAAA1"), _bond("BBBBBBBB2"), _bond("CCCCCCCC3")]
    conn = FakeConn({
        "to_regclass": [(1,)],   # both input relations exist
        Q_UNIVERSE: universe,
        # AAAAAAAA1 was swept this morning; the other two never have been.
        Q_ATTEMPTS: [("AAAAAAAA1", _stamp(0))],
    })

    def resolve(_conn, *, snapshot_id, as_of, reference_cusip9s):
        return SimpleNamespace(
            resolutions={
                reference: SimpleNamespace(
                    reference_cusip9=reference,
                    reg_s_cusip9=reference,
                    reg_s_isin=f"US{reference}0",
                )
                for reference in reference_cusip9s
            },
            reason_by_reference={},
        )

    monkeypatch.setattr(bond_live_daily, "resolve_reg_s_cusip_map_from_db", resolve)

    rows, total, coverage = bond_live_daily._universe(
        conn, 2, snapshot_id=REG_S_SNAPSHOT_ID, as_of=TODAY,
    )

    assert [row[0] for row in rows] == ["BBBBBBBB2", "CCCCCCCC3"]
    assert total == 3, "the cap must not hide how big the universe is"
    assert coverage["executable"] == 3


def test_an_explicit_tick_cap_is_reported_as_a_degraded_scope(monkeypatch) -> None:
    """Emergency top-N is available, but can never read like full daily coverage."""
    monkeypatch.setenv("BOND_TICK_TOP_N", "1")
    conn = FakeConn({Q_ACTIVITY: [("912828XX1", 1_000_000), ("NOTSWEPT1", 5)]})
    stats = bond_live_daily._load_ticks(conn, FakeClient(), UNIVERSE, TODAY)
    assert stats["scope"] == "bounded_top_n"
    assert stats["configured_top_n"] == 1
    assert stats["degraded"] is True
    assert stats["degraded_reason"] == "bounded_tick_scope"
    assert stats["cohort"] == 1 and stats["attempted_cusips"] == 1


def test_an_emergency_tick_cap_makes_the_run_verdict_non_green(monkeypatch) -> None:
    """A successful capped tape sweep is explicitly degraded, not ``ok``."""
    monkeypatch.setenv("BOND_TICK_TOP_N", "1")
    conn = FakeConn({
        Q_UNIVERSE: list(UNIVERSE),
        Q_ACTIVITY: [("912828XX1", 1_000_000)],
    })

    out = _drive_run(monkeypatch, conn=conn, client=FakeClient(curve=HEALTHY_CURVE))

    assert out["ticks"]["degraded"] is True
    assert out["state"] == "ticks_degraded_scope"
    assert out["aborted"] is True
    assert "ticks_degraded_scope" in out["halted_by"]


# --------------------------------------------------------------------------- #
# Provider client
# --------------------------------------------------------------------------- #
class _Response:
    def __init__(self, body: bytes, headers: dict | None = None) -> None:
        self._body = body
        self.headers = headers or {}

    def read(self):
        return self._body

    def close(self):
        return None


def test_the_tick_request_always_asks_for_json() -> None:
    """Without format=json the redirect target serves CSV with a Go-fmt bug."""
    seen: list[str] = []

    def opener(url, timeout):
        seen.append(url)
        return _Response(b'{"t": [], "total": 0}')

    client = _finnhub.FinnhubClient("k", opener=opener, sleep=lambda _s: None)
    client.ticks("US912828XX10", "2026-08-06")
    assert "format=json" in seen[0]
    assert "exchange=trace" in seen[0]


def test_a_429_is_retried_and_a_400_is_not() -> None:
    import urllib.error

    calls = {"n": 0}

    def flaky(url, timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            raise urllib.error.HTTPError(url, 429, "slow down", {}, None)
        return _Response(b'{"s": "ok"}')

    client = _finnhub.FinnhubClient("k", opener=flaky, sleep=lambda _s: None)
    assert client.daily_candles("X", 0, 1) == {"s": "ok"}
    assert client.retries == 1

    def forbidden(url, timeout):
        raise urllib.error.HTTPError(url, 403, "nope", {}, None)

    hard = _finnhub.FinnhubClient("k", opener=forbidden, sleep=lambda _s: None)
    with pytest.raises(_finnhub.FinnhubConfigError):
        hard.daily_candles("X", 0, 1)


def test_retries_are_finite_and_end_in_a_typed_transient_error() -> None:
    def dead(url, timeout):
        raise OSError("connection reset")

    client = _finnhub.FinnhubClient("k", opener=dead, sleep=lambda _s: None)
    with pytest.raises(_finnhub.FinnhubTransientError):
        client.daily_candles("X", 0, 1)
    assert client.errors["network"] == _finnhub.MAX_RETRIES + 1


class _DyingResponse:
    """Connects, then dies while the body is being consumed."""

    def __init__(self, error: BaseException, closed: list[str]) -> None:
        self._error = error
        self._closed = closed
        self.headers: dict = {}

    def read(self):
        raise self._error

    def close(self):
        self._closed.append("closed")


def test_a_body_read_failure_is_retried_and_then_succeeds() -> None:
    """urlopen can return and the socket still die mid-body.

    That failure used to sit outside the retrying scope and escaped as a raw
    timeout, past loaders that only catch FinnhubTransientError -- one bad read
    aborted the whole unsupervised nightly sweep.
    """
    closed: list[str] = []
    calls = {"n": 0}

    def flaky(url, timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            return _DyingResponse(TimeoutError("read timed out"), closed)
        return _Response(b'{"s": "ok"}')

    client = _finnhub.FinnhubClient("k", opener=flaky, sleep=lambda _s: None)
    assert client.daily_candles("X", 0, 1) == {"s": "ok"}
    assert client.retries == 1
    # The half-read call still burned quota, and its socket was released.
    assert client.http_calls == 2
    assert client.errors["network"] == 1
    assert closed == ["closed"]


def test_a_persistent_body_read_failure_is_a_typed_transient_error() -> None:
    """A dead body ends as FinnhubTransientError, never as a raw OSError."""
    closed: list[str] = []

    def dying(url, timeout):
        return _DyingResponse(OSError("connection reset by peer"), closed)

    client = _finnhub.FinnhubClient("k", opener=dying, sleep=lambda _s: None)
    with pytest.raises(_finnhub.FinnhubTransientError):
        client.daily_candles("X", 0, 1)
    assert client.errors["network"] == _finnhub.MAX_RETRIES + 1
    # Every attempt closed its response -- retrying must not leak sockets.
    assert len(closed) == _finnhub.MAX_RETRIES + 1


def test_a_credential_failure_still_fails_fast_after_one_call() -> None:
    """Retrying a rejected key only burns quota: 4xx stays non-transient."""
    import urllib.error

    calls = {"n": 0}

    def forbidden(url, timeout):
        calls["n"] += 1
        raise urllib.error.HTTPError(url, 401, "bad key", {}, None)

    client = _finnhub.FinnhubClient("k", opener=forbidden, sleep=lambda _s: None)
    with pytest.raises(_finnhub.FinnhubConfigError):
        client.daily_candles("X", 0, 1)
    assert calls["n"] == 1
    assert client.retries == 0


def test_a_nearly_drained_window_sleeps_to_the_reset_instant() -> None:
    slept: list[float] = []
    headers = {"X-Ratelimit-Remaining": "1", "X-Ratelimit-Reset": "1000"}

    client = _finnhub.FinnhubClient(
        "k",
        opener=lambda url, timeout: _Response(b'{"s":"ok"}', headers),
        sleep=slept.append,
        clock=lambda: 940.0,
    )
    client.daily_candles("X", 0, 1)
    assert slept and slept[0] == pytest.approx(61.0)


def test_an_absent_key_is_a_configuration_fault_not_an_empty_day(monkeypatch) -> None:
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    with pytest.raises(_finnhub.FinnhubConfigError):
        _finnhub.client_from_env()


# --------------------------------------------------------------------------- #
# Freshness drift locks (the 2e mechanism)
# --------------------------------------------------------------------------- #
def test_the_metric_inputs_read_the_dense_series_per_field() -> None:
    """price and ytm resolve on their OWN latest day, and duration settles on ytm's.

    Folding them into one latest-row rule would let a fresh price erase an older
    bond's yield -- and the duration solved from it. The same per-field scope is
    what the alias-disagreement refusal is measured over, so both halves of the
    contract are pinned here.
    """
    from src.workers import bond_metrics

    sql = bond_metrics._inputs_sql(governed=True, live=True)
    assert "live_price" in sql and "live_yield" in sql
    assert "price_date" in sql and "ytm_date" in sql
    # Across a security's multiple CUSIP9 aliases, on the field's latest day:
    #   * the pick is deterministic (source precedence, then the CUSIP), so a
    #     replay is byte-identical -- day is already fixed by the cohort;
    #   * and a DISAGREEMENT is refused rather than tie-broken, per field.
    assert sql.count("ORDER BY security_id, source_rank DESC, cusip9") == 2
    assert "price_lo IS DISTINCT FROM price_hi" in sql
    assert "ytm_lo IS DISTINCT FROM ytm_hi" in sql
    assert "CASE WHEN w.price_ambiguous THEN NULL" in sql
    assert "CASE WHEN w.ytm_ambiguous THEN NULL" in sql
    # ...which is the serving latest lane's rule, not a second semantics: same
    # spread test, same day scope. Drift between the two surfaces would mean one
    # of them serving a number the other refuses.
    from src.bonds import serving_materializer as materializer

    assert "price_lo IS DISTINCT FROM price_hi" in materializer._LATEST_OBSERVATION_LIVE
    # The duration settles on the YIELD's date, never on the price's.
    assert "i.maturity_date <= i.ytm_date" in bond_metrics._DURATION_LATERALS
    assert "i.observation_date" not in bond_metrics._DURATION_LATERALS


def test_every_assembled_inputs_variant_binds_the_same_parameter() -> None:
    """All four lane combinations keep the %(as_of)s placeholder, so one call
    site can always bind one dict."""
    from src.workers import bond_metrics

    for governed in (True, False):
        for live in (True, False):
            assert "%(as_of)s" in bond_metrics._inputs_sql(governed=governed, live=live)


def test_the_serving_latest_lane_reads_the_resolved_observation() -> None:
    from src.bonds import serving_materializer as materializer

    assert "_bond_latest_observation" in materializer._LATEST_PRICE_PCT_SQL
    assert "_bond_latest_observation" in materializer._OBSERVATIONS_SQL
    # The dense row wins only when STRICTLY newer, and the prune runs first so
    # the inline scalar subquery can never see two rows for one security.
    assert "v.observation_date > g.observation_date" in materializer._LATEST_OBSERVATION_PRUNE
    assert "NOT EXISTS" in materializer._LATEST_OBSERVATION_MERGE
    # The fund_asof (point-in-time) lane is deliberately untouched.
    assert "bond_price_fund_asof_v1" in materializer._OBSERVATIONS_SQL


def test_the_serving_as_of_follows_the_freshest_input() -> None:
    """Without this the publication identity replays and the payload never moves.

    The dense arm moved into ``_live_anchor`` when the anchor learned to ask the
    PIT alias question, so this probe follows it there: asserting on
    ``_resolve_as_of``'s own source would now pass on a docstring mention and
    prove nothing.
    """
    import inspect

    from src.workers import bond_serving

    resolve = inspect.getsource(bond_serving._resolve_as_of)
    assert "_live_anchor" in resolve, "the dense arm must still feed the anchor"
    assert "max(anchors)" in resolve

    anchor = inspect.getsource(bond_serving._live_anchor)
    assert bond_serving.LIVE_OBSERVATION_TABLE == "bond_observation_daily"
    assert bond_serving.LIVE_OBSERVATION_TABLE in bond_serving._LIVE_ANCHOR_SQL
    # The anchor may only claim a day the build can actually READ: the same PIT
    # alias window the live lane joins through, and the same price predicate the
    # serving cohort filters on (price-only here -- bond_metrics carries the OR
    # because it runs a second, ytm-keyed cohort this build has no equivalent of).
    assert "valid_from" in bond_serving._LIVE_ANCHOR_SQL
    assert "valid_to" in bond_serving._LIVE_ANCHOR_SQL
    assert "price" in bond_serving._LIVE_ANCHOR_SQL
    assert "_relation_exists" in anchor, "an absent alias view must not be read past"


def test_retention_keeps_the_app_pinned_publication() -> None:
    """Deleting what the app still points at is the worst failure available."""
    from src.workers import bond_serving

    assert "bond_serving_app_current_pointer" in bond_serving._KEEP_APP_PINNED
    assert "LIMIT 2" in bond_serving._KEEP_TWO_MOST_RECENT
    assert "LIMIT %s" in bond_serving._PRUNE_BATCH_SQL, "the delete must be batched"
