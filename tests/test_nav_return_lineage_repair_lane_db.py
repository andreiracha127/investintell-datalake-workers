"""The lineage repair lane end to end on PostgreSQL 18 + TimescaleDB 2.27.2.

``WORKER=nav_return_lineage_repair_lane`` through ``run_worker`` runs the real
driver, schema check, plan and operator; only the provider is the local
``FakeProvider`` (in place of the real ``ProviderClient``). Same disposable
database guards as the operator's DB suite (loopback ``NAV_W1_TEST_DSN``,
``nav_readiness_w1*`` database, no skip).
"""

from __future__ import annotations

import json

from scripts import repair_nav_return_lineage_cohort as driver
from src.workers import nav_return_lineage_repair as repair
from src.workers import nav_return_lineage_repair_lane as lane
from tests import test_fund_nav_readiness_db as base
from tests.test_nav_return_lineage_repair_db import (  # noqa: F401 - autouse fixture
    FakeProvider,
    _ledger,
    _rows,
    _seed,
    no_network,
)

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
