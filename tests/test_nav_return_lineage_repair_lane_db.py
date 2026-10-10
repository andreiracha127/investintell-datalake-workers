"""The lineage repair lane end to end on PostgreSQL 18 + TimescaleDB 2.27.2.

``WORKER=nav_return_lineage_repair_lane`` through ``run_worker`` runs the real
driver, schema check, plan and operator; only the provider is the local
``FakeProvider`` (in place of the real ``ProviderClient``). Same disposable
database guards as the operator's DB suite (loopback ``NAV_W1_TEST_DSN``,
``nav_readiness_w1*`` database, no skip).
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

from scripts import fund_nav_readiness_schema as schema_operator
from scripts import repair_nav_return_lineage_cohort as driver
from src.workers import nav_return_lineage_repair as repair
from src.workers import nav_return_lineage_repair_lane as lane
from tests import test_fund_nav_readiness_db as base
from tests.test_nav_return_lineage_repair_db import (  # noqa: F401 - autouse fixture
    FakeProvider,
    _ledger,
    _limits,
    _plan,
    _rows,
    _seed,
    _set_flag,
    no_network,
)
from tests.test_nav_return_lineage_repair_db import _run as _run_repair

test_dsn = base.test_dsn
schema = base.schema


def test_lane_repairs_the_seeded_cohort_and_recounts_what_remains(
    test_dsn, schema, monkeypatch, capsys
):
    import src.run_worker as rw

    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        good = [_seed(conn, ticker=ticker) for ticker in ("GOOD1", "GOOD2")]
        mismatch, _ = _seed(conn, ticker="MISMATCH")
        no_ticker, _ = _seed(conn, ticker="")  # planned, never eligible
        untouched = {iid: _rows(conn, iid) for iid in (mismatch, no_ticker)}
        before = {iid: _rows(conn, iid) for iid, _ in good}
        conn.rollback()
    fake = FakeProvider(by_ticker={"MISMATCH": ("success_new", 150.0)})
    monkeypatch.setattr(repair, "ProviderClient", lambda limits: fake)
    monkeypatch.setenv("TIINGO_API_KEY", "test-key-not-real")  # presence check only
    monkeypatch.setenv("WORKER", "nav_return_lineage_repair_lane")
    monkeypatch.setenv(lane.CONFIRM_ENV, lane.CONFIRM_VALUE)
    monkeypatch.setenv(driver.ALLOW_DATABASE_URL_ENV, "1")
    monkeypatch.delenv(driver.DSN_ENV, raising=False)
    monkeypatch.setenv("NAV_LINEAGE_REPAIR_SCHEMA", schema)
    monkeypatch.setenv("NAV_LINEAGE_REPAIR_RATE_PER_SECOND", "2.5")
    monkeypatch.setattr(rw, "resolve_dsn", lambda: test_dsn)  # the service DATABASE_URL
    capsys.readouterr()
    try:
        rw.main()
        code = 0
    except SystemExit as exc:
        code = exc.code
    out = capsys.readouterr().out
    assert test_dsn not in out and "password" not in out
    lines = [json.loads(line) for line in out.splitlines()]
    assert [line.get("event") for line in lines] == ["plan", "batch", "summary"]
    stats = lines[-1]
    # A residual (LEVEL_MISMATCH) is never painted green.
    assert code == 1, stats
    assert (stats["worker"], stats["state"], stats["status"], stats["aborted"]) == (
        "nav_return_lineage_repair_lane", "failed", "completed", False)
    assert (stats["instruments_planned"], stats["eligible_instruments"]) == (4, 3)
    assert stats["not_eligible_rows_by_reason"] == {"MISSING_TICKER": 1}
    assert (stats["committed_instruments"], stats["committed_rows"]) == (2, 2)
    assert stats["skipped_rows_by_code"] == {"LEVEL_MISMATCH": 1}
    assert (stats["requests_used"], stats["batches"], stats["plans"]) == (3, 1, 1)
    assert stats["remaining_bad_rows"] == 2  # the mismatch and the ineligible one
    assert stats["readiness_republish_required"] is False
    assert sorted(ticker for _, ticker, _, _ in fake.calls) == ["GOOD1", "GOOD2", "MISMATCH"]
    with base._connect(test_dsn, schema) as conn:
        for iid, day in good:
            expected = [dict(row) for row in before[iid]]
            expected[1]["return_source_boundary"] = None
            assert _rows(conn, iid) == expected
        assert {iid: _rows(conn, iid) for iid in untouched} == untouched
        assert _ledger(conn) == (2, 2, 2, 2, 2, 0)
        planned_again = repair.build_plan(conn, schema=schema)["items"]
        assert sorted(item["instrument_id"] for item in planned_again) == sorted(
            str(iid) for iid in (mismatch, no_ticker))


@pytest.mark.parametrize("change", ["access_not_ready", "schema_pins_moved"])
def test_schema_change_between_batches_stops_with_exit_3_and_keeps_commits(
    test_dsn, schema, monkeypatch, change
):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        tickers = {str(_seed(conn, ticker=t)[0]): t for t in ("ALPHA", "BRAVO")}
        first, second = sorted(tickers)  # plan order
        untouched = _rows(conn, second)
        conn.rollback()
    calls = {"check": 0, "pins": 0}
    real_check, real_pins = schema_operator._check, driver._schema_pins

    def check(conn, name):  # before the plan, before batch 1, then flips
        calls["check"] += 1
        result = real_check(conn, name)
        return {**result, "ready": False} if (
            change == "access_not_ready" and calls["check"] > 2) else result

    def pins():  # the plan's, batch 1's, then flips
        calls["pins"] += 1
        result = real_pins()
        return {**result, "sql_sha256": "0" * 64} if (
            change == "schema_pins_moved" and calls["pins"] > 2) else result

    monkeypatch.setattr(schema_operator, "_check", check)
    monkeypatch.setattr(driver, "_schema_pins", pins)
    fake = FakeProvider()
    lines: list[dict] = []
    code, summary = driver.run_cohort(
        # rows_per_batch = floor(4 s * 0.5/s * 0.5) = 1: one instrument per batch.
        driver.CohortConfig(schema=schema, max_seconds=4, rate_per_second=0.5),
        dsn=test_dsn, client_factory=lambda limits: fake, emit=lines.append,
    )
    assert test_dsn not in json.dumps([lines, summary])
    assert (code, summary["status"], summary["stop_code"]) == (
        3, "stopped", "SCHEMA_ACCESS_INCOMPATIBLE")
    assert [line["event"] for line in lines] == ["plan", "batch"]
    assert (summary["batches"], summary["committed_instruments"]) == (1, 1)
    assert (summary["remaining_instruments"], summary["remaining_bad_rows"]) == (1, 1)
    assert [ticker for _, ticker, _, _ in fake.calls] == [tickers[first]]
    with base._connect(test_dsn, schema) as conn:
        assert _rows(conn, second) == untouched
        assert _ledger(conn) == (1, 1, 1, 1, 1, 0)


def _move_neighbour(dsn, schema, iid, day):
    """The row after the bad date changes (not the bad row): the plan goes stale."""
    with base._connect(dsn, schema) as other:
        other.execute("SET LOCAL session_replication_role = replica")
        other.execute(
            "UPDATE nav_timeseries SET aum_usd=654321 WHERE instrument_id=%s AND nav_date=%s",
            (iid, day + dt.timedelta(days=1)),
        )
        other.commit()


@pytest.mark.parametrize("cleared_by", ["operator", "raw_update", "trigger_bypass"])
def test_a_stale_item_the_replan_no_longer_finds_is_reconciled_by_its_attribution(
    test_dsn, schema, monkeypatch, cleared_by
):
    base._bootstrap(test_dsn, schema)
    with base._connect(test_dsn, schema) as conn:
        _seed(conn, ticker="GOOD")
        stale, day = _seed(conn, ticker="STALE")
        conn.rollback()
    real_plan = driver.PostgresDatabase.plan
    after_clear: dict = {}

    def plan(self, limits):
        if self.conn is not None:  # the re-plan: STALE's flag was cleared meanwhile
            with base._connect(test_dsn, schema) as other:
                if cleared_by == "operator":  # another run of the operator: attributed
                    other_run = _run_repair(
                        other, _plan(other, schema, _limits()), [stale], FakeProvider())
                    assert other_run["changed_rows"] == 1, other_run
                else:  # raw UPDATE (unattributed revision) or trigger bypass (none)
                    _set_flag(other, stale, day, None,
                              bypass_triggers=cleared_by == "trigger_bypass")
                after_clear.update(rows=_rows(other, stale), ledger=_ledger(other))
        planned = real_plan(self, limits)
        if not after_clear:  # pass 1 will find STALE's neighbourhood moved
            _move_neighbour(test_dsn, schema, stale, day)
        return planned

    monkeypatch.setattr(driver.PostgresDatabase, "plan", plan)
    fake = FakeProvider()
    lines: list[dict] = []
    code, summary = driver.run_cohort(
        driver.CohortConfig(schema=schema, rate_per_second=2.5),
        dsn=test_dsn, client_factory=lambda limits: fake, emit=lines.append,
    )
    assert test_dsn not in json.dumps([lines, summary])
    plans = [line for line in lines if line["event"] == "plan"]
    batches = [line for line in lines if line["event"] == "batch"]
    assert [(p["pass"], p["to_apply"], p["to_reconcile"]) for p in plans] == [
        (1, 2, 0), (2, 0, 1)]
    assert batches[0]["residual_counts"] == {"PLAN_STALE": 1}
    # The reconcile batch: STALE's pass-1 item, no request, nothing written.
    assert (batches[1]["reconcile"], batches[1]["size"], batches[1]["requests"]) == (True, 1, 0)
    assert [ticker for _, ticker, _, _ in fake.calls] == ["GOOD"]
    assert (summary["requests_used"], summary["committed_instruments"]) == (1, 1)
    with base._connect(test_dsn, schema) as conn:
        assert _rows(conn, stale) == after_clear["rows"]
        assert _ledger(conn) == after_clear["ledger"]
    assert summary["remaining_bad_rows"] == 0  # the predicate no longer finds STALE
    if cleared_by == "operator":
        assert batches[1]["residual_counts"] == {}
        assert (summary["already_repaired"], summary["skipped_rows_by_code"]) == (1, {})
        assert (code, summary["status"], summary["clean"]) == (0, "completed", True)
    else:
        assert batches[1]["residual_counts"] == {"UNATTRIBUTED_CLEAR": 1}
        assert summary["already_repaired"] == 0
        assert summary["skipped_rows_by_code"] == {"UNATTRIBUTED_CLEAR": 1}
        assert (code, summary["status"], summary["clean"]) == (2, "completed", False)
