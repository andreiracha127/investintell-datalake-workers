"""DB-free tests for the bond_live_daily worker and its provider client.

The stage functions are exercised against a tiny fake connection rather than a
disposable Postgres, because what needs pinning here is ORCHESTRATION -- which
window each bond is asked for, what is written, when a commit lands, and how a
provider failure is reported -- none of which needs a real planner. The SQL these
stages emit is separately drift-locked below and validated against production
before deploy.
"""
from __future__ import annotations

import contextlib
import datetime as _dt

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

#: Markers the FakeConn answers by. Kept next to each other because a run()-level
#: test drives all four query shapes through one connection.
Q_UNIVERSE = "FROM bond_reference_terms r"
Q_WATERMARK = "max(o.day)"
Q_ATTEMPTS = "FROM bond_live_daily_sweep"
Q_CURVE_WATERMARK = "GROUP BY tenor"
Q_ACTIVITY = "coalesce(sum(o.volume)"


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
    limit: int | None = None,
    calc_date: _dt.date = TODAY,
    connector: "_FakeConnect | None" = None,
):
    """Run ``bond_live_daily.run`` against fakes, exercising the REAL verdict.

    Only the seams that need a database are replaced (the connection, the lock,
    the DDL install, the matview refresh, the two publication workers). The stage
    functions, the coverage arithmetic and the state/abort decision are the
    shipping ones -- which is the whole point: what these tests pin is which
    outcomes are allowed to exit green.
    """
    conn = conn if conn is not None else FakeConn({Q_UNIVERSE: list(UNIVERSE)})

    @contextlib.contextmanager
    def _lock(_conn, _lock_id):
        yield acquired

    monkeypatch.setattr(bond_live_daily, "resolve_dsn", lambda dsn=None: "postgresql://x")
    monkeypatch.setattr(
        bond_live_daily, "connect", connector if connector is not None else _FakeConnect(conn)
    )
    monkeypatch.setattr(bond_live_daily, "advisory_lock", _lock)
    monkeypatch.setattr(bond_live_daily, "install_schema", lambda _c: None)
    monkeypatch.setattr(bond_live_daily, "_relation_exists", lambda _c, _n: relations)
    monkeypatch.setattr(
        bond_live_daily, "_refresh_curated",
        lambda _dsn: matview or {"state": "refreshed", "matview": "bond_curated_securities"},
    )
    monkeypatch.setattr(
        bond_live_daily, "_republish", lambda _dsn: republish or {"verdict": "recomputed"}
    )

    def _client():
        if client_error is not None:
            raise client_error
        return client if client is not None else FakeClient()

    monkeypatch.setattr(bond_live_daily, "client_from_env", _client)
    return bond_live_daily.run(calc_date=calc_date.isoformat(), limit=limit)


def _candle_payload(day: _dt.date, price: float, yield_pct: float | None = 4.5):
    return {"s": "ok", "t": [live_daily.to_epoch(day)], "c": [price],
            "y": [yield_pct] if yield_pct is not None else [None]}


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
    stats = bond_live_daily._load_curve(conn, client)
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

    stats = bond_live_daily._load_curve(conn, client)

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

    stats = bond_live_daily._load_curve(conn, client)

    assert _curve_writes(conn, "30y") == ["2019-01-02", "2026-08-06"]
    assert stats["tenors"] == 1
    # Never loaded is a cold start, not a lag: it is about to load everything.
    assert stats["lagging_tenors"] == []


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

    out = _drive_run(monkeypatch, conn=conn, client=_OutageAfter())

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
    ok = _drive_run(monkeypatch, client=FakeClient(fail={"30y"}))
    assert ok["curve"]["failed_tenors"] == ["30y"]
    assert ok["state"] == "ok"


def test_a_tick_lane_that_failed_every_call_is_reported_not_swallowed(monkeypatch) -> None:
    class _NoTape(FakeClient):
        def ticks(self, isin, day, **kwargs):
            raise _finnhub.FinnhubTransientError("down")

    conn = FakeConn({
        Q_UNIVERSE: list(UNIVERSE),
        Q_ACTIVITY: [("912828XX1", 1_000_000)],
    })
    out = _drive_run(monkeypatch, conn=conn, client=_NoTape())
    assert out["ticks"]["swept"] == 1 and out["ticks"]["transient_failures"] == 1
    assert out["state"] == "ticks_failed"
    assert out["aborted"] is True


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


def test_the_capped_universe_is_the_ring_prefix_not_the_cusip_prefix(monkeypatch) -> None:
    """End to end through _universe: the cap slices the RING."""
    universe = [_bond("AAAAAAAA1"), _bond("BBBBBBBB2"), _bond("CCCCCCCC3")]
    conn = FakeConn({
        "to_regclass": [(1,)],   # both input relations exist
        Q_UNIVERSE: universe,
        # AAAAAAAA1 was swept this morning; the other two never have been.
        Q_ATTEMPTS: [("AAAAAAAA1", _stamp(0))],
    })

    rows, total = bond_live_daily._universe(conn, 2)

    assert [row[0] for row in rows] == ["BBBBBBBB2", "CCCCCCCC3"]
    assert total == 3, "the cap must not hide how big the universe is"


def test_the_tick_cohort_reports_the_truncation_a_capped_run_imposes() -> None:
    """A capped run shrinks the cost lane too; ``cohort`` vs ``swept`` says so."""
    conn = FakeConn({Q_ACTIVITY: [("912828XX1", 1_000_000), ("NOTSWEPT1", 5)]})
    stats = bond_live_daily._load_ticks(conn, FakeClient(), UNIVERSE, TODAY)
    assert stats["cohort"] == 2 and stats["swept"] == 1


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
