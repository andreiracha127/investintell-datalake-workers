"""R2-B N1 / N1-a / N1-b on real PostgreSQL 18.4 + TimescaleDB 2.27.2.

Same disposable-database guards as ``test_fund_nav_readiness_db`` (loopback
``NAV_W1_TEST_DSN``, no skip). The provider is ALWAYS a local fake: any
Python-level socket connection fails the test, ``TIINGO_API_KEY`` is removed and
the real ``TiingoClient``/``FallbackNav`` are replaced wherever a worker would
build them. psycopg talks to the database through libpq, not Python sockets.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import socket
import threading
import time
import tracemalloc
import uuid

import psycopg
import pytest
from psycopg import sql

from scripts import fund_nav_readiness_schema as operator
from scripts import rebase_fund_nav_window as cli
from src.db import LOCK_INSTRUMENT_INGESTION, LOCK_NAV_ECONOMIC_REBASE
from src.workers import _fallback_nav
from src.workers import fund_nav_readiness as readiness
from src.workers import instrument_ingestion as ingest
from src.workers import nav_economic_rebase as rebase
from src.workers._tiingo import NavFetchResult, NavObservation, TiingoBudgetExceeded
from tests import test_fund_nav_readiness_db as base
from tests.test_fund_nav_readiness_db import (
    NAV_SQL,
    STAMP,
    _connect,
    _head,
    _holds,
    _install,
    _provider_write,
    _publish_risk_run,
    _reason,
    _reexpress,
    _running_run,
    _seed,
)

test_dsn = base.test_dsn
schema = base.schema

LIFECYCLE_SQL = """INSERT INTO nav_instrument_policy_evidence
   (instrument_id,policy_id,policy_version,known_at,effective_at,fund_status,
    valuation_frequency,identity_verified,return_basis_verified,currency_verified,
    evidence_reference)
   VALUES (%s,'synthetic','v1',clock_timestamp()-interval '1 day',
           clock_timestamp()-interval '1 day','ACTIVE','daily',true,true,true,'r2b')"""
LEGACY_COLUMNS = (
    "nav_date, nav, return_1d, currency, source, return_type, source_nav, source_nav_kind,"
    " nav_repair_kind, return_start_date, return_source_boundary, return_uses_repaired_nav,"
    " return_semantics, return_verification_status, calendar_id, calendar_version,"
    " calendar_source"
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*_args, **_kwargs):
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)

    class _Forbidden:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("a real provider client was constructed")

    monkeypatch.setattr(ingest, "TiingoClient", _Forbidden)
    monkeypatch.setattr(_fallback_nav, "FallbackNav", _Forbidden)


# ── fixtures ────────────────────────────────────────────────────────────────
def _price(index: int, factor: float = 1.0) -> float:
    return round((100.0 + index * 0.01) * factor, 6)


def _universe(conn):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS instruments_universe (instrument_id uuid PRIMARY KEY,"
        "ticker text,currency text,is_active boolean,attributes jsonb)"
    )


def _register(
    conn, iid, ticker, *, currency="USD", aum=100, cohort=True, lifecycle=True
):
    _universe(conn)
    conn.execute(
        "INSERT INTO instruments_universe VALUES (%s,%s,%s,true,"
        "jsonb_build_object('aum_usd',%s::text))",
        (iid, ticker, currency, str(aum)),
    )
    if cohort:
        conn.execute(
            "INSERT INTO funds_profile_mv VALUES (%s) ON CONFLICT DO NOTHING", (iid,)
        )
    if lifecycle:
        conn.execute(LIFECYCLE_SQL, (iid,))
    conn.commit()
    return iid


def _replica(conn):
    conn.execute("SET LOCAL session_replication_role = replica")


def _legacy(conn, ticker, grid, *, factor=2.0, extra=()):
    """Pre-provenance history (NULL kind/source/calendar), no revision, no head."""
    iid = _register(conn, uuid.uuid4(), ticker)
    _replica(conn)
    conn.cursor().executemany(
        "INSERT INTO nav_timeseries (instrument_id,nav_date,nav,currency) "
        "VALUES (%s,%s,%s,'USD')",
        [(iid, d, _price(i, factor)) for i, d in enumerate(grid)]
        + [(iid, d, 1.0) for d in extra],
    )
    conn.commit()
    return iid


def _typed_copy(conn, source, ticker, grid):
    """PR132-typed levels identical to ``source`` written before W1 (no level
    revision, no calendar), then calendar-stamped by governed maintenance: the
    typed-but-unattributed case N1 must prove without rewriting anything."""
    iid = _register(conn, uuid.uuid4(), ticker)
    typed = LEGACY_COLUMNS.rsplit(", calendar_id", 1)[0]
    _replica(conn)
    conn.execute(
        f"INSERT INTO nav_timeseries (instrument_id, {typed}) "
        f"SELECT %s, {typed} FROM nav_timeseries WHERE instrument_id=%s",
        (iid, source),
    )
    conn.commit()
    operator._apply_calendar_maintenance_tx(
        conn, [str(iid)], grid[0], grid[-1], "f" * 64
    )
    conn.commit()
    return iid


def _stamp():
    now = dt.datetime.now(dt.timezone.utc)
    return now - dt.timedelta(minutes=3), now - dt.timedelta(minutes=2)


def _result(days, prices, *, kind="adjusted", status=None):
    attempted, finished = _stamp()
    observations = tuple(NavObservation(d, p, kind) for d, p in zip(days, prices))
    return NavFetchResult(status or "success_new", observations, attempted, finished)


class FakeTiingo:
    """One canned full-window response per ticker; records every call."""

    def __init__(self, responses, *, before=None):
        self.responses = responses
        self.calls: list[tuple[str, dt.date, dt.date]] = []
        self.before = before or {}

    def fetch_daily_observations(
        self, ticker, start, end, *, max_attempts=None, remaining=None
    ):
        assert max_attempts == 1, "rebase must bound HTTP to exactly one request"
        assert callable(remaining), "rebase must propagate its deadline to the fetch"
        self.calls.append((ticker, start, end))
        hook = self.before.get(ticker)
        if hook:
            hook()
        response = self.responses[ticker]
        if isinstance(response, BaseException):
            raise response
        return response(start, end) if callable(response) else response


def _full(grid, factor=1.0, **kwargs):
    return _result(grid, [_price(i, factor) for i in range(len(grid))], **kwargs)


def _limits(n, *, requests=None, seconds=600.0, rate=2.5):
    return rebase.RebaseLimits(
        batch_size=max(n, 1),
        max_instruments=max(n, 1),
        max_requests=n if requests is None else requests,
        max_seconds=seconds,
        rate_per_second=rate,
    )


def _plan(test_dsn, schema, scope, limits):
    with _connect(test_dsn, schema) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        try:
            return rebase.build_rebase_plan(
                conn, scope, limits, schema=schema, schema_pins={"fixture": "r2b"}
            )
        finally:
            conn.rollback()


def _apply(test_dsn, schema, plan, ids, client, limits, **kwargs):
    with _connect(test_dsn, schema) as conn:
        return rebase.run_rebase(
            conn,
            plan,
            instrument_ids=[str(i) for i in ids],
            limits=limits,
            supplied_sha256=plan.sha256,
            client=client,
            **kwargs,
        )


def _nav_state(conn, iid):
    """Digest of every persisted column of an instrument (rollback proof)."""
    rows = conn.execute(
        "SELECT to_jsonb(n) - 'instrument_id' FROM nav_timeseries n WHERE instrument_id=%s "
        "ORDER BY nav_date",
        (iid,),
    ).fetchall()
    return hashlib.sha256(
        json.dumps([r[0] for r in rows], sort_keys=True).encode()
    ).hexdigest()


def _ledger(conn, iid):
    return conn.execute(
        """SELECT (SELECT count(*) FROM fund_nav_data_revisions WHERE instrument_id=%(i)s),
                  (SELECT count(*) FROM nav_ingestion_row_evidence WHERE instrument_id=%(i)s),
                  (SELECT count(*) FROM nav_rebase_receipts WHERE instrument_id=%(i)s),
                  (SELECT count(*) FROM fund_nav_reexpression_events
                   WHERE instrument_id=%(i)s AND event_kind='RESOLVED'),
                  (SELECT count(*) FROM nav_ingestion_attempts WHERE instrument_id=%(i)s
                   AND status IN ('success_new','success_no_new'))""",
        {"i": iid},
    ).fetchone()


def _lineage(conn, iid, grid):
    from psycopg.rows import dict_row

    now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
    with conn.cursor(row_factory=dict_row) as cur:
        return readiness._per_date_lineage(cur, iid, grid, grid[-1], now)[0]


def _nav_rows(conn, iid, since=dt.date.min):
    return conn.execute(
        "SELECT nav_date, nav FROM nav_timeseries WHERE instrument_id=%s AND nav_date >= %s "
        "ORDER BY nav_date",
        (iid, since),
    ).fetchall()


def _returns(conn, iid, grid):
    return conn.execute(
        "SELECT return_1d FROM nav_timeseries WHERE instrument_id=%s AND nav_date "
        "BETWEEN %s AND %s ORDER BY nav_date",
        (iid, grid[1], grid[-1]),
    ).fetchall()


def _readiness(test_dsn, schema, monkeypatch):
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    return readiness.run(test_dsn)


# ── N1: full window ─────────────────────────────────────────────────────────
def test_n1_legacy_null_window_rebases_to_401_level_proofs_400_returns_and_receipt(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        _register(conn, iid, "SEED", cohort=False, lifecycle=False)
        legacy = _legacy(conn, "LEG", grid)
        unknown = _register(conn, uuid.uuid4(), "UNK")
        conn.execute(
            "DELETE FROM instruments_universe WHERE instrument_id=%s", (unknown,)
        )
        conn.commit()
        no_life = _register(conn, uuid.uuid4(), "NOL", lifecycle=False)
        no_ccy = _register(conn, uuid.uuid4(), "NCY", currency=None)
        eur = _register(conn, uuid.uuid4(), "EUR", currency="EUR")
        runs_before = conn.execute(
            "SELECT count(*), (SELECT count(*) FROM nav_calendar_maintenance_runs) "
            "FROM nav_ingestion_runs"
        ).fetchone()
    _readiness(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, legacy) == "NAV_DATA_UNAVAILABLE"  # NULL kind
    outsider = uuid.uuid4()
    limits = _limits(1)
    plan = _plan(
        test_dsn, schema, [iid, legacy, unknown, no_life, no_ccy, eur, outsider], limits
    )
    assert [item.instrument_id for item in plan.items] == [str(legacy)]
    assert plan.items[0].needs == ("LINEAGE_MISSING",)
    assert dict(plan.manifest["excluded"]) == {
        str(iid): "ALREADY_RECONCILED",
        str(unknown): "INSTRUMENT_UNKNOWN",
        str(no_life): "LIFECYCLE_MISSING",
        str(no_ccy): "CURRENCY_UNKNOWN",
        str(eur): "CURRENCY_UNSUPPORTED",
        str(outsider): "NOT_IN_COHORT",
    }
    assert plan.manifest["grid"]["count"] == 401
    assert plan.manifest["contract_version"] == "w1-tiingo-adjusted-daily-v1"
    fake = FakeTiingo({"LEG": _full(grid, 2.0)})
    result = _apply(test_dsn, schema, plan, [legacy], fake, limits)
    assert (result["status"], rebase.exit_code(result)) == ("completed", 0)
    assert fake.calls == [("LEG", grid[0], grid[-1])]
    outcome = result["instruments"][0]
    assert outcome["status"] == "committed" and outcome["changed_level_rows"] == 401
    assert result["readiness_republish_required"] is True
    with _connect(test_dsn, schema) as conn:
        receipt = conn.execute(
            "SELECT observed_levels_count, before_head, after_head, grid_digest, "
            "provider_snapshot_sha256, changed_return_rows FROM nav_rebase_receipts "
            "WHERE instrument_id=%s",
            (legacy,),
        ).fetchone()
        grid_json = "[" + ",".join(f'"{d.isoformat()}"' for d in grid) + "]"
        snapshot = rebase.provider_snapshot_digest(
            plan.items[0], fake.responses["LEG"].observations
        )
        assert receipt[:3] == (401, 0, _head(conn, legacy))
        assert receipt[3] == hashlib.sha256(grid_json.encode()).hexdigest()
        assert receipt[4] == snapshot and receipt[5] >= 400
        # 401 attributed level proofs, 400 coherent observed-interval log returns.
        rows = conn.execute(
            "SELECT nav_date, nav, source_nav, source_nav_kind, source, nav_repair_kind, "
            "return_1d, return_start_date, return_semantics, return_type, calendar_id "
            "FROM nav_timeseries WHERE instrument_id=%s ORDER BY nav_date",
            (legacy,),
        ).fetchall()
        assert len(rows) == 401
        assert all(
            r[1] == r[2]
            and r[3] == "adjusted"
            and r[4] == "tiingo"
            and r[5] == "none"
            and r[9] == "log"
            and r[10] == STAMP[0]
            for r in rows
        )
        assert rows[0][6] is None
        for prev, cur in zip(rows, rows[1:]):
            assert cur[7] == prev[0] and cur[8] == "observed_interval_log_ratio"
            assert abs(float(cur[6]) - math.log(float(cur[1]) / float(prev[1]))) < 1e-8
        assert _lineage(conn, legacy, grid) is True
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_ingestion_row_evidence WHERE instrument_id=%s",
                (legacy,),
            ).fetchone()[0]
            == 401
        )
        # Only the governed rebase run: no synthetic normal run, no maintenance.
        runs = conn.execute(
            "SELECT operation, status, contract_version, plan_sha256 FROM nav_ingestion_runs "
            "ORDER BY started_at"
        ).fetchall()
        assert len(runs) == runs_before[0] + 1
        assert runs[-1] == ("rebase", "completed", rebase.CONTRACT_VERSION, plan.sha256)
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_calendar_maintenance_runs"
            ).fetchone()[0]
            == runs_before[1]
        )
    _readiness(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        # Modified NAV invalidates the old risk input; only a new full risk run
        # (separate existing step) readmits.
        assert _reason(conn, legacy) == "RETURN_SAMPLE_NOT_CURRENT"
        _publish_risk_run(
            conn, grid[-1], {iid: _nav_rows(conn, iid), legacy: _nav_rows(conn, legacy)}
        )
    assert _readiness(test_dsn, schema, monkeypatch)["ready_count"] == 2


def test_n1_typed_unchanged_history_gets_row_evidence_with_zero_dml_and_retry_is_noop(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        typed = _typed_copy(conn, iid, "TYP", grid)
        xmins = conn.execute(
            "SELECT array_agg(xmin::text ORDER BY nav_date) FROM nav_timeseries "
            "WHERE instrument_id=%s",
            (typed,),
        ).fetchone()[0]
        head = _head(conn, typed)
        # Only calendar-only maintenance revisions exist; no level attribution.
        assert _ledger(conn, typed) == (401, 0, 0, 0, 0)
        assert (
            conn.execute(
                "SELECT count(*) FROM fund_nav_data_revisions WHERE instrument_id=%s "
                "AND (data_changed OR maintenance_run_id IS NULL)",
                (typed,),
            ).fetchone()[0]
            == 0
        )
    _readiness(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, typed) == "NAV_DATA_UNAVAILABLE"  # typed but unproven
    limits = _limits(1)
    plan = _plan(test_dsn, schema, [typed], limits)
    fake = FakeTiingo({"TYP": _full(grid)})
    result = _apply(test_dsn, schema, plan, [typed], fake, limits)
    assert result["status"] == "completed"
    outcome = result["instruments"][0]
    assert (
        outcome["changed_level_rows"],
        outcome["changed_return_rows"],
        outcome["revision_count"],
    ) == (0, 0, 0)
    with _connect(test_dsn, schema) as conn:
        # Real fetch proof, no fake revision, no rewritten row, head unchanged.
        assert _ledger(conn, typed) == (401, 401, 1, 0, 1)
        assert (
            conn.execute(
                "SELECT array_agg(xmin::text ORDER BY nav_date) FROM nav_timeseries "
                "WHERE instrument_id=%s",
                (typed,),
            ).fetchone()[0]
            == xmins
        )
        assert conn.execute(
            "SELECT before_head, after_head FROM nav_rebase_receipts WHERE instrument_id=%s",
            (typed,),
        ).fetchone() == (head, head)
        assert (
            conn.execute(
                "SELECT status FROM nav_ingestion_attempts WHERE instrument_id=%s",
                (typed,),
            ).fetchone()[0]
            == "success_no_new"
        )
        assert _lineage(conn, typed, grid) is True
        runs = conn.execute("SELECT count(*) FROM nav_ingestion_runs").fetchone()[0]
    # Retry of the same plan: receipt-backed already_applied, no fetch, no run.
    again = _apply(test_dsn, schema, plan, [typed], fake, limits)
    assert (again["status"], again["already_applied"], again["run_id"]) == (
        "completed",
        1,
        None,
    )
    assert len(fake.calls) == 1
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute("SELECT count(*) FROM nav_ingestion_runs").fetchone()[0]
            == runs
        )
        assert _ledger(conn, typed) == (401, 401, 1, 0, 1)
        _publish_risk_run(
            conn, grid[-1], {iid: _nav_rows(conn, iid), typed: _nav_rows(conn, typed)}
        )
    assert _readiness(test_dsn, schema, monkeypatch)["ready_count"] == 2


@pytest.mark.parametrize(
    ("case", "code", "status"),
    [
        ("raw", "RAW_OR_MIXED_KIND", "invalid_payload"),
        ("mixed", "RAW_OR_MIXED_KIND", "invalid_payload"),
        ("missing", "GRID_INCOMPLETE", "invalid_payload"),
        ("duplicate", "DUPLICATE_DATE", "invalid_payload"),
        ("nonfinite", "NONFINITE_OR_NONPOSITIVE", "invalid_payload"),
        ("repair", "REPAIR_REQUIRED", "invalid_payload"),
        ("orphan", "ORPHAN_STORED_DATE", "invalid_payload"),
        ("identity", "PLAN_STALE", "transient_error"),
        ("not_found", "PROVIDER_NOT_FOUND", "not_found"),
        ("rate_limited", "PROVIDER_RATE_LIMITED", "rate_limited"),
    ],
)
def test_n1_bad_snapshot_or_source_mismatch_rolls_back_the_whole_instrument(
    test_dsn, schema, case, code, status
):
    _iid, grid, _ = _seed(test_dsn, schema)
    saturday = grid[5] + dt.timedelta(days=(5 - grid[5].weekday()) % 7)
    assert saturday.weekday() == 5 and grid[0] < saturday < grid[-1]
    with _connect(test_dsn, schema) as conn:
        legacy = _legacy(
            conn, "BAD", grid, extra=[saturday] if case == "orphan" else ()
        )
        before = (_nav_state(conn, legacy), _ledger(conn, legacy), _holds(conn, legacy))
    limits = _limits(1)
    plan = _plan(test_dsn, schema, [legacy], limits)
    prices = [_price(i, 2.0) for i in range(len(grid))]
    responses = {
        "raw": _full(grid, 2.0, kind="raw"),
        "mixed": NavFetchResult(
            "success_new",
            tuple(
                NavObservation(d, p, "raw" if i == 3 else "adjusted")
                for i, (d, p) in enumerate(zip(grid, prices))
            ),
            *_stamp(),
        ),
        "missing": _result(grid[:200] + grid[201:], prices[:200] + prices[201:]),
        "duplicate": _result([*grid, grid[7]], [*prices, prices[7]]),
        "nonfinite": _result(grid, [*prices[:9], math.nan, *prices[10:]]),
        "repair": _result(grid, [*prices[:150], prices[150] * 1e-4, *prices[151:]]),
        "orphan": _full(grid, 2.0),
        "identity": _full(grid, 2.0),
        "not_found": NavFetchResult("not_found", (), *_stamp()),
        "rate_limited": TiingoBudgetExceeded("30 consecutive 429s"),
    }
    response = responses[case]
    hooks = {}
    if case == "identity":

        def drift():
            with _connect(test_dsn, schema) as other:
                other.execute(
                    "UPDATE instruments_universe SET currency='EUR' "
                    "WHERE instrument_id=%s",
                    (legacy,),
                )

        hooks["BAD"] = drift
    fake = FakeTiingo({"BAD": response}, before=hooks)
    result = _apply(test_dsn, schema, plan, [legacy], fake, limits)
    assert (result["status"], rebase.exit_code(result)) == ("blocked", 2)
    assert result["instruments"][0]["code"] == code and result["committed"] == 0
    assert result["retryable"] is (code in rebase.RETRYABLE_CODES)
    with _connect(test_dsn, schema) as conn:
        assert (
            _nav_state(conn, legacy),
            _ledger(conn, legacy),
            _holds(conn, legacy),
        ) == before
        attempt = conn.execute(
            "SELECT a.status, a.reason_code, r.status, r.reason_code FROM nav_ingestion_attempts a "
            "JOIN nav_ingestion_runs r USING (run_id) WHERE a.instrument_id=%s",
            (legacy,),
        ).fetchone()
        assert attempt == (status, code, "failed", "INSTRUMENT_FAILED")
        assert _lineage(conn, legacy, grid) is False


# ── N1-a + N1-b(5): normal reexpression keeps the hold; rebase resolves it ──
def _make_due(conn, iid, grid):
    """Unattributed removal of the due level: the next normal run must fetch it."""
    conn.execute(
        "DELETE FROM nav_timeseries WHERE instrument_id=%s AND nav_date=%s",
        (iid, grid[-1]),
    )
    conn.commit()


class _Primary:
    responses: dict = {}
    calls: list = []

    def __init__(self, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def fetch_daily_observations(self, ticker, start, end):
        type(self).calls.append((ticker, start, end))
        response = type(self).responses[ticker]
        if isinstance(response, BaseException):
            raise response
        return response(start, end)


class _NoFallback:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def fetch_observations(self, _ticker, _start, _end):
        return NavFetchResult("not_found"), None, []


def _normal(monkeypatch, schema, responses, *, concurrency=1):
    _Primary.responses = responses
    _Primary.calls = []
    monkeypatch.setattr(ingest, "connect", lambda dsn: _connect(dsn, schema))
    monkeypatch.setattr(ingest, "TiingoClient", _Primary)
    monkeypatch.setattr(_fallback_nav, "FallbackNav", _NoFallback)
    monkeypatch.setattr(ingest, "FETCH_CONCURRENCY", concurrency)
    return _Primary.calls


def _window(grid, factor=1.0):
    def respond(start, end):
        days = [(i, d) for i, d in enumerate(grid) if start <= d <= end]
        return _result([d for _, d in days], [_price(i, factor) for i, _ in days])

    return respond


def test_n1a_overlap_reexpression_holds_then_rebase_preserves_returns_and_resolves(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        _register(conn, iid, "AAA", aum=900, cohort=False, lifecycle=False)
        original = _returns(conn, iid, grid)
        # Ledger-only detection wholly before the grid (never blocks, never erased).
        old = [dt.date(2012, 3, day) for day in (5, 6, 7)]
        _provider_write(
            conn,
            ingest.build_rows(
                (
                    *(
                        NavObservation(d, 20.0 + i, "adjusted")
                        for i, d in enumerate(old)
                    ),
                    NavObservation(grid[0], _price(0), "adjusted"),
                ),
                [(iid, "USD")],
                calendar={grid[0]: STAMP},
            ),
        )
        _reexpress(conn, iid, old[:2])
        old_event = _holds(conn, iid)[0][0]
        assert ingest._rebase_required_count(conn) == 0  # ledger-only, pre-grid
        _make_due(conn, iid, grid)
    calls = _normal(monkeypatch, schema, {"AAA": _window(grid, 0.5)})
    stats = ingest.run(
        test_dsn, calc_date=grid[-1].isoformat(), target_session=grid[-1]
    )
    # The normal overlap fetch saw re-expressed adjusted history: hold opened,
    # instrument counted as pending an operator rebase, no full-window fetch.
    assert stats["reexpression_detected"] == 1 and stats["rebase_required_count"] == 1
    assert len(calls) == 1 and calls[0][1] > grid[0]
    _readiness(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) == "RETURN_INTERVAL_INCOMPATIBLE"
        normal_event = _holds(conn, iid)[-1][0]
        assert _lineage(conn, iid, grid) is True  # attributed; only the hold blocks
    limits = _limits(1)
    plan = _plan(test_dsn, schema, [iid], limits)
    assert plan.items[0].needs == ("ACTIVE_HOLD",)
    assert plan.items[0].active_event_ids == (normal_event,)
    assert plan.items[0].window_start == grid[0]
    # A failure before the receipt leaves the hold active.
    failed = _apply(
        test_dsn,
        schema,
        plan,
        [iid],
        FakeTiingo({"AAA": _full(grid, 0.5, kind="raw")}),
        limits,
    )
    assert failed["instruments"][0]["code"] == "RAW_OR_MIXED_KIND"
    with _connect(test_dsn, schema) as conn:
        assert normal_event in [h[0] for h in _holds(conn, iid)]
    result = _apply(
        test_dsn, schema, plan, [iid], FakeTiingo({"AAA": _full(grid, 0.5)}), limits
    )
    assert result["status"] == "completed"
    resolved = result["instruments"][0]["resolved_events"]
    with _connect(test_dsn, schema) as conn:
        audit = conn.execute(
            "SELECT event_id, event_kind, resolves_event_id, rebase_receipt_id IS NOT NULL, "
            "first_changed_date, last_changed_date FROM fund_nav_reexpression_events "
            "WHERE instrument_id=%s ORDER BY event_id",
            (iid,),
        ).fetchall()
        detected = [e for e in audit if e[1] == "DETECTED"]
        # The rebase itself observed the proportional re-expression (DETECTED)
        # and resolved every in-window event after its validated receipt.
        assert len(detected) == 3 and normal_event in resolved
        assert sorted(resolved) == sorted(e[0] for e in detected if e[0] != old_event)
        assert all(e[3] for e in audit if e[1] == "RESOLVED")
        assert [h[0] for h in _holds(conn, iid)] == [
            old_event
        ]  # preserved, ledger-only
        after = _returns(conn, iid, grid)
        assert len(after) == 400 and all(
            abs(float(a[0]) - float(b[0])) <= 1e-8 for a, b in zip(after, original)
        )
        _publish_risk_run(conn, grid[-1], {iid: _nav_rows(conn, iid, grid[0])})
    _readiness(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) is None
    # A new detection after resolution reopens the hold.
    with _connect(test_dsn, schema) as conn:
        _reexpress(conn, iid, grid[-2:], factor=3.0)
        assert len(_holds(conn, iid)) == 2
    _readiness(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) == "RETURN_INTERVAL_INCOMPATIBLE"


def test_n1a_event_crossing_policy_coverage_is_not_planned_or_resolved(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        _register(conn, iid, "AAA", cohort=False, lifecycle=False)
        old = dt.date(2015, 6, 1)
        _provider_write(
            conn,
            ingest.build_rows(
                (
                    NavObservation(old, 10.0, "adjusted"),
                    NavObservation(grid[0], _price(0), "adjusted"),
                ),
                [(iid, "USD")],
                calendar={grid[0]: STAMP},
            ),
        )
        _provider_write(
            conn,
            ingest.build_rows(
                (
                    NavObservation(old, 5.0, "adjusted"),
                    NavObservation(grid[0], _price(0, 0.5), "adjusted"),
                ),
                [(iid, "USD")],
                calendar={grid[0]: STAMP},
            ),
        )
        event = _holds(conn, iid)[0]
        assert (event[1], event[2]) == (old, grid[0])
    plan = _plan(test_dsn, schema, [iid], _limits(1))
    assert plan.items == () and plan.manifest["excluded"] == [
        [str(iid), "HOLD_SCOPE_NOT_COVERED"]
    ]
    _readiness(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) == "RETURN_INTERVAL_INCOMPATIBLE"
        assert (
            conn.execute("SELECT count(*) FROM nav_rebase_receipts").fetchone()[0] == 0
        )


# ── N1-b: per-instrument atomicity in the normal worker ─────────────────────
def test_n1b_budget_after_a_commits_keeps_a_valid_under_aborted_parent(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        _register(conn, iid, "AAA", aum=900, cohort=False, lifecycle=False)
        b = _register(conn, uuid.uuid4(), "BBB", aum=500)
        c = _register(conn, uuid.uuid4(), "CCC", aum=100)
        _make_due(conn, iid, grid)
        revision_floor = conn.execute(
            "SELECT COALESCE(max(revision_id),0) FROM fund_nav_data_revisions"
        ).fetchone()[0]
    _normal(
        monkeypatch,
        schema,
        {
            "AAA": _window(grid),
            "BBB": TiingoBudgetExceeded("30 consecutive 429s"),
            "CCC": _window(grid),
        },
    )
    stats = ingest.run(
        test_dsn, calc_date=grid[-1].isoformat(), target_session=grid[-1]
    )
    assert stats["aborted"] == "tiingo_budget" and stats["run_status"] == "aborted"
    assert stats["instruments_committed"] == 1 and stats["not_attempted"] == 1
    run_id = uuid.UUID(stats["ingestion_run_id"])
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT status, reason_code, completed_at IS NOT NULL FROM nav_ingestion_runs "
            "WHERE run_id=%s",
            (run_id,),
        ).fetchone() == ("aborted", "TIINGO_BUDGET", True)
        attempts = {
            row[0]: row[1:]
            for row in conn.execute(
                "SELECT instrument_id, status, reason_code FROM nav_ingestion_attempts "
                "WHERE run_id=%s",
                (run_id,),
            ).fetchall()
        }
        assert attempts[iid] == ("success_new", None)
        assert attempts[b] == ("rate_limited", "TIINGO_BUDGET")
        assert attempts[c] == ("not_attempted_budget", "TIINGO_BUDGET_ABORT")
        # A: level + evidence committed together; B and C: no NAV at all.
        dates = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT nav_date FROM fund_nav_data_revisions WHERE revision_id > %s "
                "AND instrument_id=%s",
                (revision_floor, iid),
            ).fetchall()
        ]
        assert dates == [grid[-1]]  # unchanged overlap dates made no revision
        evidence = conn.execute(
            "SELECT count(*), min(nav_date), max(nav_date) FROM nav_ingestion_row_evidence "
            "WHERE run_id=%s AND instrument_id=%s",
            (run_id, iid),
        ).fetchone()
        assert evidence[0] > 1 and evidence[2] == grid[-1]
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_timeseries WHERE instrument_id = ANY(%s)",
                ([b, c],),
            ).fetchone()[0]
            == 0
        )
        assert _lineage(conn, iid, grid) is True
    # The re-fetched due level equals the level the published risk run used:
    # A is admissible although its parent run aborted (no parent-status gate).
    _readiness(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) is None
    # Retry is a NEW run (attempts are append-only); A is no longer due and its
    # not_due marker (no provider instant) does not displace its proof.
    _normal(monkeypatch, schema, {"BBB": _window(grid), "CCC": _window(grid)})
    retry = ingest.run(
        test_dsn, calc_date=grid[-1].isoformat(), target_session=grid[-1]
    )
    assert (
        retry["ingestion_run_id"] != str(run_id) and retry["run_status"] == "completed"
    )
    assert _readiness(test_dsn, schema, monkeypatch)["ready_count"] == 1
    # Next normal overlap fetch of identical data is a real no-op: row
    # evidence only; no NAV DML, no revision, no head move, no price tampering.
    later = grid[-1] + dt.timedelta(days=10)
    with _connect(test_dsn, schema) as conn:
        before = (
            _head(conn, iid),
            _nav_state(conn, iid),
            conn.execute("SELECT count(*) FROM fund_nav_data_revisions").fetchone()[0],
        )
    calls = _normal(
        monkeypatch, schema, {t: _window(grid) for t in ("AAA", "BBB", "CCC")}
    )
    noop = ingest.run(test_dsn, calc_date=later.isoformat())
    assert noop["run_status"] == "completed"
    assert ("AAA", grid[-1] - dt.timedelta(days=7), later) in calls
    with _connect(test_dsn, schema) as conn:
        after = (
            _head(conn, iid),
            _nav_state(conn, iid),
            conn.execute("SELECT count(*) FROM fund_nav_data_revisions").fetchone()[0],
        )
        assert after == before
        attempt = conn.execute(
            "SELECT a.status, count(e.nav_date) FROM nav_ingestion_attempts a "
            "LEFT JOIN nav_ingestion_row_evidence e USING (run_id, instrument_id, provider) "
            "WHERE a.run_id=%s AND a.instrument_id=%s GROUP BY a.status",
            (uuid.UUID(noop["ingestion_run_id"]), iid),
        ).fetchone()
        assert attempt[0] == "success_no_new" and attempt[1] > 1
        assert _lineage(conn, iid, grid) is True
    assert _readiness(test_dsn, schema, monkeypatch)["ready_count"] == 1


def test_n1b_crash_in_chunk2_rolls_back_the_instrument_and_fails_the_run(
    test_dsn, schema, monkeypatch
):
    _iid, grid, _ = _seed(test_dsn, schema)
    days = grid[-10:]
    with _connect(test_dsn, schema) as conn:
        good = _register(conn, uuid.uuid4(), "GOOD", aum=900)
        bad = _register(conn, uuid.uuid4(), "CRSH", aum=100)
        conn.execute(
            sql.SQL(
                "CREATE FUNCTION r2b_fail_chunk2() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN IF NEW.instrument_id = {} AND NEW.nav_date = {} THEN "
                "RAISE EXCEPTION 'injected chunk2 failure'; END IF; RETURN NEW; END $$"
            ).format(sql.Literal(str(bad)), sql.Literal(days[5]))
        )
        conn.execute(
            "CREATE TRIGGER r2b_fail_chunk2 BEFORE INSERT ON nav_timeseries "
            "FOR EACH ROW EXECUTE FUNCTION r2b_fail_chunk2()"
        )
        conn.commit()
    monkeypatch.setattr(ingest, "UPSERT_CHUNK", 4)

    def ten(start, end):
        return _result(days, [_price(i) for i in range(len(days))])

    _normal(monkeypatch, schema, {"GOOD": ten, "CRSH": ten})
    with pytest.raises(psycopg.Error, match="injected chunk2 failure"):
        ingest.run(test_dsn, calc_date=grid[-1].isoformat(), target_session=grid[-1])
    with _connect(test_dsn, schema) as conn:
        run = conn.execute(
            "SELECT run_id, status, reason_code FROM nav_ingestion_runs "
            "WHERE operation='normal' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        assert run[1:] == ("failed", "UNEXPECTED_ERROR")
        # Chunk 1 (4 rows + their returns) did not survive the chunk-2 failure.
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s", (bad,)
            ).fetchone()[0]
            == 0
        )
        assert _ledger(conn, bad) == (0, 0, 0, 0, 0)
        assert conn.execute(
            "SELECT status, reason_code FROM nav_ingestion_attempts WHERE instrument_id=%s",
            (bad,),
        ).fetchone() == ("transient_error", "PERSISTENCE_FAILED")
        # The instrument committed before the crash keeps its atomic proof.
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s", (good,)
            ).fetchone()[0]
            == 10
        )
        assert _ledger(conn, good)[1:] == (10, 0, 0, 1)
        assert _lineage(conn, good, days) is True
        conn.execute("DROP TRIGGER r2b_fail_chunk2 ON nav_timeseries")
        conn.commit()
    # An arbitrary provider exception also finalizes the run (never left running).
    _normal(monkeypatch, schema, {"CRSH": RuntimeError("boom"), "GOOD": ten})
    with pytest.raises(RuntimeError, match="boom"):
        ingest.run(test_dsn, calc_date=grid[-1].isoformat(), target_session=grid[-1])
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_ingestion_runs WHERE status='running'"
            ).fetchone()[0]
            == 0
        )
        assert conn.execute(
            "SELECT status, reason_code FROM nav_ingestion_runs WHERE operation='normal' "
            "ORDER BY started_at DESC LIMIT 1"
        ).fetchone() == ("failed", "UNEXPECTED_ERROR")


def test_n1b_killed_runs_are_recovered_only_by_their_operation_owner(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        _register(conn, iid, "AAA", cohort=False, lifecycle=False)
        legacy = _legacy(conn, "LEG", grid)
        # kill -9 after the per-instrument commit: parent never finalized.
        killed = _provider_write(
            conn,
            ingest.build_rows(
                (NavObservation(grid[-1], _price(400) + 0.01, "adjusted"),),
                [(iid, "USD")],
                calendar={grid[-1]: STAMP},
            ),
            start=grid[-1],
            complete=False,
        )
        orphan_rebase = _running_run(conn, grid[-1], operation="rebase", plan="e" * 64)
        conn.commit()
    _normal(monkeypatch, schema, {})
    stats = ingest.run(
        test_dsn, calc_date=grid[-1].isoformat(), target_session=grid[-1]
    )
    assert stats["orphan_runs_recovered"] == 1
    with _connect(test_dsn, schema) as conn:
        statuses = {
            row[0]: row[1:]
            for row in conn.execute(
                "SELECT run_id, status, reason_code FROM nav_ingestion_runs"
            ).fetchall()
        }
        assert statuses[killed] == ("aborted", "INTERRUPTED")
        assert statuses[orphan_rebase] == ("running", None)  # not normal's to take
        assert _lineage(conn, iid, grid) is True  # committed proof survives
    limits = _limits(1)
    plan = _plan(test_dsn, schema, [legacy], limits)
    # A live rebase operator holds the mutex: busy, nothing recovered or written.
    with psycopg.connect(test_dsn, autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (LOCK_NAV_ECONOMIC_REBASE,))
        busy = _apply(test_dsn, schema, plan, [legacy], FakeTiingo({}), limits)
        assert (busy["status"], rebase.exit_code(busy), busy["run_id"]) == (
            "lock_busy",
            4,
            None,
        )
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute(
                "SELECT status FROM nav_ingestion_runs WHERE run_id=%s",
                (orphan_rebase,),
            ).fetchone()[0]
            == "running"
        )
    done = _apply(
        test_dsn, schema, plan, [legacy], FakeTiingo({"LEG": _full(grid, 2.0)}), limits
    )
    assert done["status"] == "completed" and done["orphan_runs_recovered"] == 1
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT status, reason_code FROM nav_ingestion_runs WHERE run_id=%s",
            (orphan_rebase,),
        ).fetchone() == ("aborted", "INTERRUPTED")


# ── rebase batch: locks, budgets, stale plans, hashes ───────────────────────
def test_rebase_lock_busy_before_and_after_commits_then_retry(test_dsn, schema):
    _iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        first, second = sorted(
            (_legacy(conn, "L1", grid), _legacy(conn, "L2", grid)), key=str
        )
        tickers = dict(
            conn.execute(
                "SELECT instrument_id, ticker FROM instruments_universe "
                "WHERE instrument_id = ANY(%s)",
                ([first, second],),
            ).fetchall()
        )
        runs_before = conn.execute(
            "SELECT count(*) FROM nav_ingestion_runs"
        ).fetchone()[0]
    limits = _limits(2)
    plan = _plan(test_dsn, schema, [first, second], limits)
    responses = {tickers[first]: _full(grid, 2.0), tickers[second]: _full(grid, 2.0)}
    # Writer locks busy before any commit: exit 4 and zero instrument commits.
    with psycopg.connect(test_dsn, autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (LOCK_INSTRUMENT_INGESTION,))
        busy = _apply(
            test_dsn, schema, plan, [first, second], FakeTiingo(responses), limits
        )
    assert (busy["status"], rebase.exit_code(busy), busy["committed"]) == (
        "lock_busy",
        4,
        0,
    )
    assert [i["status"] for i in busy["instruments"]] == ["lock_busy", "not_attempted"]
    with _connect(test_dsn, schema) as conn:
        assert _ledger(conn, first)[2] == 0
        # A local lock condition is audited without a provider instant.
        assert conn.execute(
            "SELECT status, attempted_at FROM nav_ingestion_attempts WHERE instrument_id=%s "
            "AND reason_code='LOCK_BUSY'",
            (first,),
        ).fetchone() == ("transient_error", None)
        assert conn.execute(
            "SELECT status, reason_code FROM nav_ingestion_runs "
            "ORDER BY started_at DESC LIMIT 1"
        ).fetchone() == ("aborted", "LOCK_BUSY")
        assert conn.execute("SELECT count(*) FROM nav_ingestion_runs").fetchone()[
            0
        ] == (runs_before + 1)
    # Busy AFTER the first instrument committed: partial, exit 5, never "no writes".
    holder = psycopg.connect(test_dsn, autocommit=True)
    try:
        grab = {
            tickers[second]: lambda: holder.execute(
                "SELECT pg_advisory_lock(%s)", (LOCK_INSTRUMENT_INGESTION,)
            )
        }
        partial = _apply(
            test_dsn,
            schema,
            plan,
            [first, second],
            FakeTiingo(responses, before=grab),
            limits,
        )
    finally:
        holder.close()
    assert (partial["status"], rebase.exit_code(partial), partial["committed"]) == (
        "partial",
        5,
        1,
    )
    assert [i["status"] for i in partial["instruments"]] == ["committed", "lock_busy"]
    # Retry after release: the committed one is already_applied (no fetch).
    fake = FakeTiingo(responses)
    retry = _apply(test_dsn, schema, plan, [first, second], fake, limits)
    assert (retry["status"], retry["already_applied"], retry["committed"]) == (
        "completed",
        1,
        1,
    )
    assert [call[0] for call in fake.calls] == [tickers[second]]
    with _connect(test_dsn, schema) as conn:
        assert _lineage(conn, first, grid) and _lineage(conn, second, grid)


def test_rebase_budget_commits_a_not_b_and_plan_hash_stale_and_allowlist(
    test_dsn, schema
):
    _iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        a, b = sorted((_legacy(conn, "BA", grid), _legacy(conn, "BB", grid)), key=str)
        names = dict(
            conn.execute(
                "SELECT instrument_id, ticker FROM instruments_universe"
            ).fetchall()
        )
        stale = _legacy(conn, "ST", grid)
    limits = _limits(2, requests=1)
    plan = _plan(test_dsn, schema, [a, b], limits)
    fake = FakeTiingo({names[a]: _full(grid, 2.0), names[b]: _full(grid, 2.0)})
    result = _apply(test_dsn, schema, plan, [a, b], fake, limits)
    assert (result["status"], rebase.exit_code(result)) == ("partial", 5)
    assert [(i["status"], i["code"]) for i in result["instruments"]] == [
        ("committed", None),
        ("not_attempted", "BUDGET_EXHAUSTED"),
    ]
    assert result["requests_used"] == 1 and len(fake.calls) == 1 and result["retryable"]
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT status, reason_code FROM nav_ingestion_runs WHERE run_id=%s",
            (uuid.UUID(result["run_id"]),),
        ).fetchone() == ("aborted", "BUDGET_EXHAUSTED")
        # A's receipt/lineage stand although the parent aborted; B untouched.
        assert _lineage(conn, a, grid) is True and _ledger(conn, b) == (0, 0, 0, 0, 0)
    # Wall-clock budget: exhausted before the first fetch.
    ticks = iter([0.0, 1000.0])
    timed = _apply(
        test_dsn,
        schema,
        _plan(test_dsn, schema, [b], _limits(1, seconds=5.0)),
        [b],
        FakeTiingo({}),
        _limits(1, seconds=5.0),
        monotonic=lambda: next(ticks),
    )
    assert (timed["status"], timed["instruments"][0]["code"]) == (
        "partial",
        "BUDGET_EXHAUSTED",
    )
    # Hash, limits and allowlist are pinned.
    for kwargs, code in (
        ({"supplied_sha256": "0" * 64}, "PLAN_HASH_MISMATCH"),
        ({"limits": _limits(2, requests=2)}, "PLAN_LIMITS_MISMATCH"),
        ({"instrument_ids": [str(stale)]}, "ALLOWLIST_INVALID"),
        ({"instrument_ids": [str(a), str(a)]}, "ALLOWLIST_INVALID"),
    ):
        with _connect(test_dsn, schema) as conn:
            args = {
                "instrument_ids": [str(b)],
                "limits": limits,
                "supplied_sha256": plan.sha256,
                **kwargs,
            }
            blocked = rebase.run_rebase(conn, plan, client=FakeTiingo({}), **args)
        assert (blocked["status"], blocked["code"], rebase.exit_code(blocked)) == (
            "blocked",
            code,
            2,
        )
    # Head drift after planning: PLAN_STALE, nothing but a failure attempt.
    stale_plan = _plan(test_dsn, schema, [stale], _limits(1))
    with _connect(test_dsn, schema) as conn:
        conn.execute(
            "UPDATE nav_timeseries SET aum_usd=1 WHERE instrument_id=%s AND nav_date=%s",
            (stale, grid[3]),
        )
        conn.commit()
        state = (_nav_state(conn, stale), _ledger(conn, stale))
    drift = _apply(
        test_dsn,
        schema,
        stale_plan,
        [stale],
        FakeTiingo({"ST": _full(grid, 2.0)}),
        _limits(1),
    )
    assert (
        drift["instruments"][0]["code"] == "PLAN_STALE" and rebase.exit_code(drift) == 2
    )
    with _connect(test_dsn, schema) as conn:
        assert (_nav_state(conn, stale), _ledger(conn, stale)) == state


def test_rebase_interrupt_after_first_commit_is_partial_and_releases_mutex(
    test_dsn, schema
):
    _iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        first, second = sorted(
            (_legacy(conn, "K1", grid), _legacy(conn, "K2", grid)), key=str
        )
        tickers = dict(
            conn.execute(
                "SELECT instrument_id, ticker FROM instruments_universe "
                "WHERE instrument_id = ANY(%s)",
                ([first, second],),
            ).fetchall()
        )
    limits = _limits(2)
    plan = _plan(test_dsn, schema, [first, second], limits)
    responses = {tickers[first]: _full(grid, 2.0), tickers[second]: KeyboardInterrupt()}
    result = _apply(
        test_dsn, schema, plan, [first, second], FakeTiingo(responses), limits
    )
    assert (result["status"], result["code"], rebase.exit_code(result)) == (
        "partial",
        "INTERRUPTED",
        5,
    )
    assert [i["status"] for i in result["instruments"]] == [
        "committed",
        "not_attempted",
    ]
    assert result["requests_used"] == 2
    with _connect(test_dsn, schema) as conn:
        assert conn.execute(
            "SELECT status, reason_code FROM nav_ingestion_runs WHERE run_id=%s",
            (uuid.UUID(result["run_id"]),),
        ).fetchone() == ("aborted", "INTERRUPTED")
        assert _lineage(conn, first, grid) is True and _ledger(conn, second)[2] == 0
        # The session mutex was released: another holder can take it at once.
        assert (
            conn.execute(
                "SELECT pg_try_advisory_lock(%s)", (LOCK_NAV_ECONOMIC_REBASE,)
            ).fetchone()[0]
            is True
        )
        conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_NAV_ECONOMIC_REBASE,))


# ── R2-C F3: truthful outcome across bookkeeping and commit failures ────────
def _pair(test_dsn, schema, prefix):
    _iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        first, second = sorted(
            (_legacy(conn, f"{prefix}1", grid), _legacy(conn, f"{prefix}2", grid)),
            key=str,
        )
        tickers = dict(
            conn.execute(
                "SELECT instrument_id, ticker FROM instruments_universe "
                "WHERE instrument_id = ANY(%s)",
                ([first, second],),
            ).fetchall()
        )
    return grid, first, second, tickers


def _run_status(test_dsn, schema, run_id):
    with _connect(test_dsn, schema) as conn:
        return conn.execute(
            "SELECT status, reason_code FROM nav_ingestion_runs WHERE run_id=%s",
            (uuid.UUID(run_id),),
        ).fetchone()


@pytest.mark.parametrize("step", ["finalize", "release"])
def test_committed_outcome_survives_finalize_or_release_error(
    test_dsn, schema, monkeypatch, step
):
    grid, first, second, tickers = _pair(
        test_dsn, schema, "F" if step == "finalize" else "R"
    )
    limits = _limits(2)
    plan = _plan(test_dsn, schema, [first, second], limits)
    responses = {tickers[first]: _full(grid, 2.0), tickers[second]: _full(grid, 2.0)}
    real = getattr(rebase, f"_{step}")

    def broken(*args, **kwargs):
        raise psycopg.OperationalError("simulated connection failure")

    monkeypatch.setattr(rebase, f"_{step}", broken)
    result = _apply(
        test_dsn, schema, plan, [first, second], FakeTiingo(responses), limits
    )
    monkeypatch.setattr(rebase, f"_{step}", real)
    code = "FINALIZE_FAILED" if step == "finalize" else "MUTEX_RELEASE_FAILED"
    assert result["errors"] == [code]
    assert (result["status"], result["code"], rebase.exit_code(result)) == (
        "partial",
        code,
        2,
    )
    assert (result["committed"], result["readiness_republish_required"]) == (2, True)
    assert result["run_id"] is not None
    assert all(i["receipt_id"] for i in result["instruments"])
    assert "simulated" not in json.dumps(result)  # sanitized, codes only
    status = _run_status(test_dsn, schema, result["run_id"])
    # finalize failed: the run stays running until the next owner recovers it.
    assert status == (("running", None) if step == "finalize" else ("completed", None))
    with _connect(test_dsn, schema) as conn:
        assert _lineage(conn, first, grid) and _lineage(conn, second, grid)


def test_failure_attempt_record_error_keeps_both_instrument_outcomes(
    test_dsn, schema, monkeypatch
):
    grid, first, second, tickers = _pair(test_dsn, schema, "W")
    limits = _limits(2)
    plan = _plan(test_dsn, schema, [first, second], limits)
    responses = {
        tickers[first]: _full(grid, 2.0),
        tickers[second]: _full(grid, kind="raw"),
    }

    def broken(*args, **kwargs):
        raise psycopg.OperationalError("simulated insert failure")

    monkeypatch.setattr(rebase, "_record_failure", broken)
    result = _apply(
        test_dsn, schema, plan, [first, second], FakeTiingo(responses), limits
    )
    assert result["errors"] == ["FAILURE_RECORD_FAILED"]
    assert [(i["status"], i["code"]) for i in result["instruments"]] == [
        ("committed", None),
        ("failed", "RAW_OR_MIXED_KIND"),
    ]
    assert (result["status"], rebase.exit_code(result)) == ("partial", 2)
    assert _run_status(test_dsn, schema, result["run_id"]) == (
        "failed",
        "INSTRUMENT_FAILED",
    )


class _LostAckConnection:
    """Delegates to a real session; the armed COMMIT really commits, then the
    acknowledgement is 'lost' and the session behaves as dead afterwards."""

    def __init__(self, conn):
        self._conn = conn
        self.armed = False
        self.dead = False

    @property
    def broken(self):
        return self.dead

    def commit(self):
        if self.dead:
            raise psycopg.OperationalError("connection lost")
        if self.armed:
            self._conn.commit()
            self.armed, self.dead = False, True
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        return self._conn.commit()

    def execute(self, *args, **kwargs):
        if self.dead:
            raise psycopg.OperationalError("connection lost")
        return self._conn.execute(*args, **kwargs)

    def rollback(self):
        if self.dead:
            raise psycopg.OperationalError("connection lost")
        return self._conn.rollback()

    def __getattr__(self, name):
        return getattr(self._conn, name)


@pytest.mark.parametrize("reconcile", [True, False])
def test_lost_commit_ack_is_reconciled_by_receipt_never_zero_writes(
    test_dsn, schema, monkeypatch, reconcile
):
    grid, first, second, tickers = _pair(test_dsn, schema, "A" if reconcile else "U")
    limits = _limits(2)
    plan = _plan(test_dsn, schema, [first, second], limits)
    responses = {tickers[first]: _full(grid, 2.0), tickers[second]: _full(grid, 2.0)}
    real_evidence = rebase._insert_row_evidence_tx
    holder: dict = {}

    def arm(conn, **kwargs):
        written = real_evidence(conn, **kwargs)
        holder["proxy"].armed = True  # the next COMMIT is the instrument's
        return written

    monkeypatch.setattr(rebase, "_insert_row_evidence_tx", arm)
    with _connect(test_dsn, schema) as raw:
        holder["proxy"] = proxy = _LostAckConnection(raw)
        result = rebase.run_rebase(
            proxy,
            plan,
            instrument_ids=[str(first), str(second)],
            limits=limits,
            supplied_sha256=plan.sha256,
            client=FakeTiingo(responses),
            reconnect=(lambda: _connect(test_dsn, schema)) if reconcile else None,
        )
    outcome = result["instruments"][0]
    if reconcile:
        assert (outcome["status"], outcome["code"]) == (
            "committed_unverified",
            "COMMIT_ACK_LOST",
        )
        assert outcome["changed_level_rows"] == 401
        assert result["committed_unverified"] == 1 and result["unknown"] == 0
        # Own run closed on a fresh session after the dead one failed.
        assert _run_status(test_dsn, schema, result["run_id"]) == (
            "failed",
            "DATABASE_ERROR",
        )
    else:
        assert (outcome["status"], outcome["code"]) == (
            "unknown",
            "COMMIT_OUTCOME_UNKNOWN",
        )
        assert result["unknown"] == 1
    assert outcome["receipt_id"] is not None
    assert result["instruments"][1]["status"] == "not_attempted"
    assert result["readiness_republish_required"] is True
    assert "FINALIZE_FAILED" in result["errors"]
    assert "MUTEX_RELEASE_FAILED" in result["errors"]
    assert result["status"] == "partial" and rebase.exit_code(result) == 2
    with _connect(test_dsn, schema) as conn:
        # The server did commit: receipt present, lineage proven.
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_rebase_receipts WHERE receipt_id=%s",
                (uuid.UUID(outcome["receipt_id"]),),
            ).fetchone()[0]
            == 1
        )
        assert _lineage(conn, first, grid) is True


# ── R2-C F4: the wall-clock budget reaches pacing, fetch and DML ─────────────
def test_fetch_returning_after_deadline_is_never_applied(test_dsn, schema):
    grid, first, second, tickers = _pair(test_dsn, schema, "D")
    limits = _limits(2, seconds=60.0)
    plan = _plan(test_dsn, schema, [first, second], limits)
    clock = [0.0]

    def slow_fetch():
        clock[0] = 61.0  # the provider answers after the deadline

    fake = FakeTiingo(
        {tickers[first]: _full(grid, 2.0), tickers[second]: _full(grid, 2.0)},
        before={tickers[second]: slow_fetch},
    )
    with _connect(test_dsn, schema) as conn:
        before = (_nav_state(conn, second), _ledger(conn, second))
    result = _apply(
        test_dsn,
        schema,
        plan,
        [first, second],
        fake,
        limits,
        monotonic=lambda: clock[0],
    )
    assert [(i["status"], i["code"]) for i in result["instruments"]] == [
        ("committed", None),
        ("not_attempted", "BUDGET_EXHAUSTED"),
    ]
    assert (result["status"], rebase.exit_code(result)) == ("partial", 5)
    assert (result["requests_used"], result["fetched"]) == (2, 2)
    assert _run_status(test_dsn, schema, result["run_id"]) == (
        "aborted",
        "BUDGET_EXHAUSTED",
    )
    with _connect(test_dsn, schema) as conn:
        assert (_nav_state(conn, second), _ledger(conn, second)) == before


def test_token_bucket_wait_beyond_deadline_sends_no_request(
    test_dsn, schema, monkeypatch
):
    from src.workers import _tiingo

    grid, first, second, tickers = _pair(test_dsn, schema, "T")
    limits = _limits(2, seconds=60.0, rate=0.01)  # the 2nd token needs 100 s
    plan = _plan(test_dsn, schema, [first, second], limits)
    slept: list[float] = []
    monkeypatch.setattr(_tiingo.time, "sleep", slept.append)
    bars = [
        {"date": d.isoformat(), "adjClose": _price(i, 2.0), "close": 1.0}
        for i, d in enumerate(grid)
    ]

    class _Response:
        status_code = 200

        def json(self):
            return bars

    class _Http:
        calls = 0

        def get(self, *_args, **kwargs):
            type(self).calls += 1
            assert kwargs["timeout"] <= 60.0
            return _Response()

        def close(self):
            pass

    client = _tiingo.TiingoClient(
        key="offline-test-key",
        bucket=_tiingo.TokenBucket(max_tokens=1.0, refill_rate=limits.rate_per_second),
    )
    client._client.close()
    client._client = _Http()
    result = _apply(test_dsn, schema, plan, [first, second], client, limits)
    assert [(i["status"], i["code"]) for i in result["instruments"]] == [
        ("committed", None),
        ("not_attempted", "BUDGET_EXHAUSTED"),
    ]
    assert (_Http.calls, client.requests_made, result["requests_used"], slept) == (
        1,
        1,
        1,
        [],
    )
    assert (result["status"], rebase.exit_code(result)) == ("partial", 5)


def test_apply_rechecks_deadline_before_any_dml(test_dsn, schema):
    grid, first, _second, tickers = _pair(test_dsn, schema, "Q")
    limits = _limits(1)
    plan = _plan(test_dsn, schema, [first], limits)
    item = plan.items[0]
    snapshot = rebase.validate_adjusted_snapshot(
        item, _full(grid, 2.0), sessions=grid, due_sessions=grid
    )
    with _connect(test_dsn, schema) as conn:
        before = (_nav_state(conn, first), _ledger(conn, first))
        conn.rollback()  # apply requires an idle session
        with pytest.raises(rebase.RebaseDeadline):
            rebase.apply_rebase_instrument(
                conn, plan, item, snapshot, uuid.uuid4(), remaining=lambda: 0.0
            )
        assert (_nav_state(conn, first), _ledger(conn, first)) == before
    assert tickers[first] == item.ticker


# ── R2-C F6: the pointer publication instant is a plan pin ───────────────────
def _repoint_v1_v2_v1(test_dsn, schema, iid, grid):
    from tests.test_fund_nav_readiness_db import _publish_rollover

    with _connect(test_dsn, schema) as conn:
        _publish_rollover(conn, iid, grid)
        conn.execute("UPDATE nav_policy_current SET policy_version='v1'")
        conn.commit()


def test_pointer_republication_makes_plan_stale_at_preflight_and_revalidate(
    test_dsn, schema
):
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        first = _legacy(conn, "P1", grid)
        second = _legacy(conn, "P2", grid)
    limits = _limits(1)
    plan_a = _plan(test_dsn, schema, [first], limits)
    _repoint_v1_v2_v1(test_dsn, schema, iid, grid)
    fake = FakeTiingo({"P1": _full(grid, 2.0)})
    stale = _apply(test_dsn, schema, plan_a, [first], fake, limits)
    # Same policy/version/hash as planned, but a later pointer publication.
    assert (stale["status"], stale["code"], rebase.exit_code(stale)) == (
        "blocked",
        "PLAN_STALE",
        2,
    )
    assert fake.calls == [] and stale["run_id"] is None
    # Revalidation inside the instrument transaction: republication of the SAME
    # target while the fetch is in flight is caught before any DML.
    plan_b = _plan(test_dsn, schema, [second], limits)

    def republish():
        with _connect(test_dsn, schema) as conn:
            conn.execute("UPDATE nav_policy_current SET policy_version=policy_version")
            conn.commit()

    with _connect(test_dsn, schema) as conn:
        before = (_nav_state(conn, second), _ledger(conn, second))
    during = _apply(
        test_dsn,
        schema,
        plan_b,
        [second],
        FakeTiingo({"P2": _full(grid, 2.0)}, before={"P2": republish}),
        limits,
    )
    assert during["instruments"][0]["code"] == "PLAN_STALE"
    with _connect(test_dsn, schema) as conn:
        assert (_nav_state(conn, second), _ledger(conn, second)) == before


def test_already_applied_needs_the_exact_pinned_state(test_dsn, schema):
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        legacy = _legacy(conn, "AA", grid)
    limits = _limits(1)
    plan = _plan(test_dsn, schema, [legacy], limits)
    fake = FakeTiingo({"AA": _full(grid, 2.0)})
    assert _apply(test_dsn, schema, plan, [legacy], fake, limits)["committed"] == 1
    replay = _apply(test_dsn, schema, plan, [legacy], fake, limits)
    assert (replay["status"], replay["already_applied"], len(fake.calls)) == (
        "completed",
        1,
        1,
    )  # recognised before any fetch
    _repoint_v1_v2_v1(test_dsn, schema, iid, grid)
    moved = _apply(test_dsn, schema, plan, [legacy], fake, limits)
    assert (
        moved["status"],
        moved["code"],
        moved["already_applied"],
        len(fake.calls),
    ) == ("blocked", "PLAN_STALE", 0, 1)


# ── R2-C F5: a receipt can supersede an earlier unknown derived return ──────
def _unknown_derived_touch(conn, iid, day):
    """Two unattributed return-only writes that restore the value exactly: the
    latest derived revision of ``day`` is unknown, the current value is not."""
    for delta in ("+ 0.001", "- 0.001"):
        conn.execute(
            f"UPDATE nav_timeseries SET return_1d = return_1d {delta} "
            "WHERE instrument_id=%s AND nav_date=%s",
            (iid, day),
        )
    conn.commit()


def _per_date(conn, iid, grid):
    from psycopg.rows import dict_row

    now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
    with conn.cursor(row_factory=dict_row) as cur:
        verified, lineage = readiness._per_date_lineage(cur, iid, grid, grid[-1], now)
    return verified, {entry[0]: entry[-1] for entry in lineage}


def test_receipt_supersedes_unknown_derived_return_until_a_later_unknown_write(
    test_dsn, schema, monkeypatch
):
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        _register(conn, iid, "SEED", cohort=False, lifecycle=False)
        _unknown_derived_touch(conn, iid, grid[5])
        verified, per_date = _per_date(conn, iid, grid)
        assert verified is False and per_date[grid[5].isoformat()] is False
        revisions = conn.execute(
            "SELECT count(*) FROM fund_nav_data_revisions"
        ).fetchone()[0]
    limits = _limits(1)
    plan = _plan(test_dsn, schema, [iid], limits)
    assert plan.items[0].needs == ("LINEAGE_MISSING",)
    result = _apply(
        test_dsn, schema, plan, [iid], FakeTiingo({"SEED": _full(grid)}), limits
    )
    outcome = result["instruments"][0]
    # No-op recovery: zero DML, zero revisions, only evidence + receipt.
    assert (
        outcome["status"],
        outcome["changed_level_rows"],
        outcome["changed_return_rows"],
        outcome["revision_count"],
    ) == ("committed", 0, 0, 0)
    with _connect(test_dsn, schema) as conn:
        assert (
            conn.execute("SELECT count(*) FROM fund_nav_data_revisions").fetchone()[0]
            == revisions
        )
        verified, per_date = _per_date(conn, iid, grid)
        assert verified is True and per_date[grid[5].isoformat()] is True
    _readiness(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        assert _reason(conn, iid) is None
        # A LATER unknown derived write is not covered by the receipt.
        _unknown_derived_touch(conn, iid, grid[5])
        verified, per_date = _per_date(conn, iid, grid)
        assert verified is False and per_date[grid[5].isoformat()] is False
        # A current return different from the reconciled levels never passes.
        conn.execute(
            "UPDATE nav_timeseries SET return_1d = return_1d + 0.001 "
            "WHERE instrument_id=%s AND nav_date=%s",
            (iid, grid[7]),
        )
        conn.commit()
        assert _per_date(conn, iid, grid)[1][grid[7].isoformat()] is False


def test_receipt_never_supersedes_a_return_whose_start_is_outside_its_window(
    test_dsn, schema
):
    iid, grid, _ = _seed(test_dsn, schema)
    before_grid = grid[0] - dt.timedelta(days=7)
    with _connect(test_dsn, schema) as conn:
        _register(conn, iid, "SEED", cohort=False, lifecycle=False)
        # Attributed pre-grid level: grid[0]'s return now starts outside the grid.
        _provider_write(
            conn,
            ingest.build_rows(
                (NavObservation(before_grid, 99.5, "adjusted"),), [(iid, "USD")]
            ),
        )
        assert (
            conn.execute(
                "SELECT return_start_date FROM nav_timeseries WHERE instrument_id=%s "
                "AND nav_date=%s",
                (iid, grid[0]),
            ).fetchone()[0]
            == before_grid
        )
        _unknown_derived_touch(conn, iid, grid[0])
        _unknown_derived_touch(conn, iid, grid[5])
    limits = _limits(1)
    plan = _plan(test_dsn, schema, [iid], limits)
    assert plan.items[0].window_start == grid[0]
    result = _apply(
        test_dsn, schema, plan, [iid], FakeTiingo({"SEED": _full(grid)}), limits
    )
    assert result["instruments"][0]["changed_return_rows"] == 0
    with _connect(test_dsn, schema) as conn:
        verified, per_date = _per_date(conn, iid, grid)
    assert per_date[grid[5].isoformat()] is True  # both endpoints in the receipt
    assert per_date[grid[0].isoformat()] is False  # start date outside the receipt
    assert verified is False


# ── derived returns and chunk boundaries ─────────────────────────────────────
def test_chunked_write_equals_unchunked_and_successor_return_is_derived(
    test_dsn, schema, monkeypatch
):
    _iid, grid, _ = _seed(test_dsn, schema)
    days = grid[-30:]
    fields = (
        "nav_date, nav, return_1d, return_start_date, return_source_boundary, "
        "return_uses_repaired_nav, return_semantics, return_verification_status, "
        "source_nav, source_nav_kind, nav_repair_kind, calendar_id"
    )
    with _connect(test_dsn, schema) as conn:
        a, b = uuid.uuid4(), uuid.uuid4()
        series = tuple(
            NavObservation(d, _price(i), "adjusted") for i, d in enumerate(days)
        )
        calendar = {d: STAMP for d in days}
        _provider_write(
            conn, ingest.build_rows(series, [(a, "USD")], calendar=calendar)
        )
        monkeypatch.setattr(ingest, "UPSERT_CHUNK", 7)
        _provider_write(
            conn, ingest.build_rows(series, [(b, "USD")], calendar=calendar)
        )
        # Same partial correction on both, at a chunk boundary (index 13/14).
        fix = (
            NavObservation(days[13], _price(13) + 0.05, "adjusted"),
            NavObservation(days[14], _price(14) + 0.05, "adjusted"),
        )
        monkeypatch.setattr(ingest, "UPSERT_CHUNK", 500)
        _provider_write(conn, ingest.build_rows(fix, [(a, "USD")], calendar=calendar))
        monkeypatch.setattr(ingest, "UPSERT_CHUNK", 1)
        run_b = _provider_write(
            conn, ingest.build_rows(fix, [(b, "USD")], calendar=calendar)
        )
        state = [
            conn.execute(
                f"SELECT {fields} FROM nav_timeseries WHERE instrument_id=%s "
                "ORDER BY nav_date",
                (i,),
            ).fetchall()
            for i in (a, b)
        ]
        assert state[0] == state[1]
        # The successor (days[15]) was not fetched: its return revision is
        # derived from days[14], and its level keeps the original origin.
        successor = conn.execute(
            "SELECT derived_return_only, dependency_start_date, data_changed "
            "FROM fund_nav_data_revisions WHERE instrument_id=%s AND nav_date=%s "
            "AND source_run_id=%s",
            (b, days[15], run_b),
        ).fetchall()
        assert successor == [(True, days[14], True)]
        level_origin = conn.execute(
            "SELECT source_run_id FROM fund_nav_data_revisions WHERE instrument_id=%s "
            "AND nav_date=%s AND NOT derived_return_only ORDER BY revision_id DESC LIMIT 1",
            (b, days[15]),
        ).fetchone()[0]
        assert level_origin != run_b
        assert _lineage(conn, a, days) is True and _lineage(conn, b, days) is True
        # Any later unknown write invalidates.
        conn.execute(
            "UPDATE nav_timeseries SET nav=nav+1 WHERE instrument_id=%s AND nav_date=%s",
            (b, days[20]),
        )
        conn.commit()
        assert _lineage(conn, b, days) is False


# ── CLI contract (plan file, hash, exit codes, sanitized stdout) ────────────
def test_cli_plan_apply_retry_incompatible_end_to_end(
    test_dsn, schema, tmp_path, capsys, monkeypatch
):
    _iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        legacy = _legacy(conn, "CLI", grid)
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    plan_file = tmp_path / "plan.json"
    budgets = [
        "--max-requests",
        "1",
        "--max-seconds",
        "300",
        "--rate-per-second",
        "1",
        "--batch-size",
        "1",
    ]
    common = ["--schema", schema, "--contract", rebase.CONTRACT_VERSION, *budgets]

    def run(argv, factory=None):
        code = cli.main(argv, client_factory=factory)
        out = capsys.readouterr().out
        assert "password" not in out and test_dsn not in out
        return code, json.loads(out)

    code, planned = run(
        [*common, "--instrument-id", str(legacy), "--plan-file", str(plan_file)]
    )
    assert (code, planned["status"], planned["planned"]) == (0, "planned", 1)
    digest = planned["plan_sha256"]
    assert hashlib.sha256(plan_file.read_bytes()).hexdigest() == digest
    code, again = run(
        [*common, "--instrument-id", str(legacy), "--plan-file", str(plan_file)]
    )
    assert (code, again["code"]) == (2, "PLAN_FILE_EXISTS")
    apply = [
        *common,
        "--mode",
        "apply",
        "--plan-file",
        str(plan_file),
        "--plan-sha256",
        digest,
        "--instrument-id",
        str(legacy),
    ]
    fake = FakeTiingo({"CLI": _full(grid, 2.0)})
    code, applied = run(apply, lambda limits: fake)
    assert (code, applied["status"], applied["committed"]) == (0, "completed", 1)
    assert set(applied["instruments"][0]) == {
        "instrument_id",
        "status",
        "code",
        "receipt_id",
        "changed_level_rows",
        "changed_return_rows",
        "revision_count",
        "resolved_events",
    }
    code, replay = run(apply, lambda limits: fake)
    assert (code, replay["already_applied"], len(fake.calls)) == (0, 1, 1)
    code, missing_key = run(apply)  # real client factory: no key, no request
    assert (code, missing_key["code"]) == (2, "PROVIDER_NOT_CONFIGURED")
    with _connect(test_dsn, schema) as conn:
        conn.execute("REVOKE SELECT ON nav_policy_current FROM app_runtime")
        conn.commit()
    code, incompatible = run(apply, lambda limits: fake)
    assert code == 3 and incompatible["status"] == "blocked"


# ── compressed canary (<= 20 instruments, full window, mocked provider) ─────
def _compressed_bootstrap(dsn, schema_name):
    with _connect(dsn, schema_name, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        conn.execute(
            "ALTER TABLE nav_timeseries SET "
            "(timescaledb.compress, timescaledb.compress_segmentby='instrument_id')"
        )
        _install(conn, schema_name)
        base._access_fixture(conn, schema_name)
        conn.execute("CREATE TABLE funds_profile_mv (instrument_id uuid PRIMARY KEY)")


def _compression(conn, schema_name):
    return conn.execute(
        "SELECT count(*) FILTER (WHERE is_compressed), count(*) "
        "FROM timescaledb_information.chunks WHERE hypertable_schema=%s "
        "AND hypertable_name='nav_timeseries'",
        (schema_name,),
    ).fetchone()


def _compress_all(conn, schema_name):
    conn.execute(
        "SELECT compress_chunk(c, if_not_compressed=>true) "
        "FROM show_chunks('nav_timeseries') c"
    )
    conn.commit()
    compressed = _compression(conn, schema_name)
    assert compressed[0] == compressed[1] > 1  # every chunk really compressed
    return compressed


def test_compressed_chunks_accept_normal_ingestion_and_calendar_maintenance(
    test_dsn, schema, monkeypatch
):
    """Row locks on compressed tuples fail in TimescaleDB (0A000); the governed
    writers rely on advisory locks and must work on compressed history."""
    monkeypatch.setattr(base, "_bootstrap", _compressed_bootstrap)
    iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        _register(conn, iid, "AAA", cohort=False, lifecycle=False)
        typed = _register(conn, uuid.uuid4(), "TYP")
        typed_columns = LEGACY_COLUMNS.rsplit(", calendar_id", 1)[0]
        _replica(conn)
        conn.execute(
            f"INSERT INTO nav_timeseries (instrument_id, {typed_columns}) "
            f"SELECT %s, {typed_columns} FROM nav_timeseries WHERE instrument_id=%s",
            (typed, iid),
        )
        conn.commit()
        _make_due(conn, iid, grid)
        _compress_all(conn, schema)
    _normal(monkeypatch, schema, {"AAA": _window(grid)})
    stats = ingest.run(
        test_dsn, calc_date=grid[-1].isoformat(), target_session=grid[-1]
    )
    assert stats["run_status"] == "completed" and stats["instruments_committed"] == 1
    with _connect(test_dsn, schema) as conn:
        assert _lineage(conn, iid, grid) is True
        _compress_all(conn, schema)
        result = operator._apply_calendar_maintenance_tx(
            conn, [str(typed)], grid[0], grid[-1], "f" * 64
        )
        conn.commit()
        assert result["status"] == "completed" and result["changed_rows"] == 401, result
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_timeseries WHERE instrument_id=%s "
                "AND calendar_id IS NOT NULL",
                (typed,),
            ).fetchone()[0]
            == 401
        )


def test_compressed_canary_20_instruments_full_window(test_dsn, schema, monkeypatch):
    monkeypatch.setattr(base, "_bootstrap", _compressed_bootstrap)
    _iid, grid, _ = _seed(test_dsn, schema)
    count = 20
    with _connect(test_dsn, schema) as conn:
        ids = sorted(
            (
                _legacy(conn, f"C{n:02d}", grid, factor=1.0 + n / 100)
                for n in range(count)
            ),
            key=str,
        )
        tickers = dict(
            conn.execute(
                "SELECT instrument_id, ticker FROM instruments_universe"
            ).fetchall()
        )
        compressed_before = _compress_all(conn, schema)
    limits = _limits(count)
    plan = _plan(test_dsn, schema, ids, limits)
    assert len(plan.items) == count
    responses = {tickers[i]: _full(grid, 1.0 + int(tickers[i][1:]) / 100) for i in ids}
    # Crash inside the first instrument's transaction (after its compressed-chunk
    # writes): everything of it rolls back; the batch stops truthfully.
    with _connect(test_dsn, schema) as conn:
        states = {
            i: (_nav_state(conn, i), _ledger(conn, i), _holds(conn, i)) for i in ids
        }
        conn.execute(
            sql.SQL(
                "CREATE FUNCTION r2b_canary_crash() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN IF NEW.instrument_id = {} THEN RAISE EXCEPTION 'canary crash'; END IF; "
                "RETURN NEW; END $$"
            ).format(sql.Literal(str(ids[0])))
        )
        conn.execute(
            "CREATE TRIGGER r2b_canary_crash BEFORE INSERT ON "
            "nav_ingestion_row_evidence FOR EACH ROW "
            "EXECUTE FUNCTION r2b_canary_crash()"
        )
        conn.commit()
    crashed = _apply(test_dsn, schema, plan, ids, FakeTiingo(responses), limits)
    assert (crashed["status"], rebase.exit_code(crashed), crashed["committed"]) == (
        "blocked",
        2,
        0,
    )
    assert crashed["instruments"][0]["code"] == "DATABASE_ERROR"
    with _connect(test_dsn, schema) as conn:
        assert {
            i: (_nav_state(conn, i), _ledger(conn, i)[:4], _holds(conn, i)) for i in ids
        } == {i: (s[0], s[1][:4], s[2]) for i, s in states.items()}
        assert _compression(conn, schema) == compressed_before
        conn.execute("DROP TRIGGER r2b_canary_crash ON nav_ingestion_row_evidence")
        conn.commit()
        wal_before, revisions_before = conn.execute(
            "SELECT pg_current_wal_lsn(), (SELECT count(*) FROM fund_nav_data_revisions)"
        ).fetchone()
    lock_peaks: list[int] = []
    original = rebase._write_instrument_nav_tx

    def measured(conn, rows, **kwargs):
        written = original(conn, rows, **kwargs)
        lock_peaks.append(
            conn.execute(
                "SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid()"
            ).fetchone()[0]
        )
        return written

    monkeypatch.setattr(rebase, "_write_instrument_nav_tx", measured)
    tracemalloc.start()
    started = time.perf_counter()
    result = _apply(test_dsn, schema, plan, ids, FakeTiingo(responses), limits)
    elapsed = time.perf_counter() - started
    peak_bytes = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert (result["status"], result["committed"]) == ("completed", count), (
        result["reason_counts"],
        result["instruments"][:2],
    )
    with _connect(test_dsn, schema) as conn:
        wal_after, revisions_after = conn.execute(
            "SELECT pg_current_wal_lsn(), (SELECT count(*) FROM fund_nav_data_revisions)"
        ).fetchone()
        wal_bytes = conn.execute(
            "SELECT pg_wal_lsn_diff(%s, %s)", (wal_after, wal_before)
        ).fetchone()[0]
        assert all(_lineage(conn, i, grid) for i in ids)
        compressed_after = _compression(conn, schema)
        receipts = conn.execute("SELECT count(*) FROM nav_rebase_receipts").fetchone()[
            0
        ]
    # Zero-op: same plan again, no fetch, no revision.
    zero_fake = FakeTiingo(responses)
    started = time.perf_counter()
    zero = _apply(test_dsn, schema, plan, ids, zero_fake, limits)
    zero_elapsed = time.perf_counter() - started
    with _connect(test_dsn, schema) as conn:
        zero_revisions = conn.execute(
            "SELECT count(*) FROM fund_nav_data_revisions"
        ).fetchone()[0]
    assert (zero["already_applied"], len(zero_fake.calls)) == (count, 0)
    assert zero_revisions == revisions_after and receipts == count
    report = {
        "instruments": count,
        "levels_per_instrument": len(grid),
        "chunks_compressed_before": list(compressed_before),
        "chunks_compressed_after": list(compressed_after),
        "changed_level_rows": sum(
            i["changed_level_rows"] for i in result["instruments"]
        ),
        "changed_return_rows": sum(
            i["changed_return_rows"] for i in result["instruments"]
        ),
        "revisions": int(revisions_after - revisions_before),
        "row_evidence": count * len(grid),
        "elapsed_seconds": round(elapsed, 3),
        "wal_bytes": int(wal_bytes),
        "max_backend_locks_after_write": max(lock_peaks),
        "python_peak_bytes": peak_bytes,
        "zero_op_elapsed_seconds": round(zero_elapsed, 3),
        "zero_op_revisions": int(zero_revisions - revisions_after),
    }
    assert report["changed_level_rows"] == count * len(grid)
    target = os.environ.get("NAV_CANARY_REPORT")
    if target:
        with open(target, "w", encoding="utf-8") as handle:
            json.dump(report, handle, sort_keys=True)


def test_threads_are_not_left_behind_by_offline_fakes():
    assert not [
        t
        for t in threading.enumerate()
        if t.name.startswith("ThreadPoolExecutor") and t.is_alive() and not t.daemon
    ]
