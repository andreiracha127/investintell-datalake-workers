"""Cohort driver end to end on real PostgreSQL 18.4 + TimescaleDB 2.27.2.

The REAL operator (``rebase_fund_nav_window.main``) runs in-process under the
driver; only the provider is a local fake (``FakeTiingo`` via
``client_factory``). Same disposable-database guards as the operator's own DB
suite (loopback ``NAV_W1_TEST_DSN``, ``nav_readiness_w1*`` database, no skip).
"""

from __future__ import annotations

import json

import psycopg

from scripts import rebase_fund_nav_cohort as driver
from src.db import LOCK_INSTRUMENT_INGESTION
from tests import test_fund_nav_readiness_db as base
from tests.test_fund_nav_readiness_db import _connect, _seed
from tests.test_nav_economic_rebase_db import (  # noqa: F401 - autouse fixture
    FakeTiingo,
    _full,
    _ledger,
    _legacy,
    _lineage,
    no_network,
)

test_dsn = base.test_dsn
schema = base.schema


def _tickers(conn, ids):
    return dict(
        conn.execute(
            "SELECT instrument_id::text, ticker FROM instruments_universe "
            "WHERE instrument_id = ANY(%s)",
            ([str(i) for i in ids],),
        ).fetchall()
    )


def _drive(test_dsn, schema, monkeypatch, tmp_path, fake, **config):
    monkeypatch.setenv(driver.DSN_ENV, test_dsn)
    lines: list[dict] = []
    code, summary = driver.run_cohort(
        driver.CohortConfig(schema=schema, work_dir=str(tmp_path), **config),
        client_factory=lambda limits: fake,
        emit=lines.append,
    )
    text = json.dumps([lines, summary])
    assert test_dsn not in text and "password" not in text
    return code, summary, lines


def test_cohort_plans_once_applies_in_batches_and_a_rerun_finds_nothing(
    test_dsn, schema, monkeypatch, tmp_path
):
    _seed_iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        ids = sorted((str(_legacy(conn, f"C{n}", grid)) for n in range(3)))
        tickers = _tickers(conn, ids)
    fake = FakeTiingo({tickers[i]: _full(grid, 2.0) for i in ids})
    code, summary, lines = _drive(
        test_dsn, schema, monkeypatch, tmp_path / "run", fake, batch_size=2
    )
    assert (code, summary["status"], summary["clean"]) == (0, "completed", True)
    assert (summary["planned_initial"], summary["committed"], summary["batches"]) == (
        3,
        3,
        2,
    )
    assert sum(summary["not_planned_by_code"].values()) == 1  # the seeded fund
    assert [line["size"] for line in lines if line["event"] == "batch"] == [2, 1]
    assert summary["requests_used"] == len(fake.calls) == 3
    assert summary["readiness_republish_required"] is True
    with _connect(test_dsn, schema) as conn:
        for iid in ids:
            assert _lineage(conn, iid, grid) is True
            assert _ledger(conn, iid)[2] == 1  # exactly one receipt
    # Resumable by construction: reconciled funds are no longer planned.
    again = FakeTiingo({})
    code, summary, lines = _drive(
        test_dsn, schema, monkeypatch, tmp_path / "rerun", again, batch_size=2
    )
    assert (code, summary["status"], summary["batches"], again.calls) == (
        0,
        "completed",
        0,
        [],
    )
    assert summary["not_planned_by_code"].get("ALREADY_RECONCILED", 0) >= 3


def test_stale_pins_mid_run_are_replanned_and_the_run_completes(
    test_dsn, schema, monkeypatch, tmp_path
):
    """A pointer republication during the first fetch: that instrument fails
    PLAN_STALE (revalidation, nothing written), the next batch is blocked at
    preflight with PLAN_STALE, the driver re-plans both and commits both."""
    _seed_iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        first, second = sorted(str(_legacy(conn, f"S{n}", grid)) for n in range(2))
        tickers = _tickers(conn, [first, second])
    fired: list[bool] = []

    def republish_once():
        if fired:
            return
        fired.append(True)
        with _connect(test_dsn, schema) as conn:
            conn.execute("UPDATE nav_policy_current SET policy_version=policy_version")
            conn.commit()

    fake = FakeTiingo(
        {tickers[i]: _full(grid, 2.0) for i in (first, second)},
        before={tickers[first]: republish_once},
    )
    code, summary, lines = _drive(
        test_dsn, schema, monkeypatch, tmp_path, fake, batch_size=1
    )
    batches = [line for line in lines if line["event"] == "batch"]
    plans = [line for line in lines if line["event"] == "plan"]
    assert [b["reason_counts"] for b in batches[:2]] == [
        {"PLAN_STALE": 1},
        {"PLAN_STALE": 1},
    ]
    assert batches[0]["failed"] == 1 and batches[1]["code"] == "PLAN_STALE"
    assert [p["scope"] for p in plans] == ["cohort", "instruments"]
    assert plans[1]["scope_size"] == 2 and plans[1]["planned"] == 2
    assert plans[0]["plan_sha256"] != plans[1]["plan_sha256"]
    assert [b["committed"] for b in batches[2:]] == [1, 1]
    assert {b["plan_sha256"] for b in batches[2:]} == {plans[1]["plan_sha256"]}
    assert (code, summary["status"], summary["committed"], summary["replans"]) == (
        0,
        "completed",
        2,
        1,
    )
    assert summary["failed"] == 0 and summary["remaining"] == 0
    with _connect(test_dsn, schema) as conn:
        assert _lineage(conn, first, grid) and _lineage(conn, second, grid)


def test_writer_lock_busy_stops_with_exit_4_and_nothing_written(
    test_dsn, schema, monkeypatch, tmp_path
):
    _seed_iid, grid, _ = _seed(test_dsn, schema)
    with _connect(test_dsn, schema) as conn:
        ids = sorted(str(_legacy(conn, f"B{n}", grid)) for n in range(2))
        tickers = _tickers(conn, ids)
    fake = FakeTiingo({tickers[i]: _full(grid, 2.0) for i in ids})
    with psycopg.connect(test_dsn, autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (LOCK_INSTRUMENT_INGESTION,))
        code, summary, lines = _drive(test_dsn, schema, monkeypatch, tmp_path, fake)
    assert (code, summary["status"], summary["stop_code"]) == (4, "stopped", "LOCK_BUSY")
    assert (summary["committed"], summary["batches"], summary["remaining"]) == (0, 1, 2)
    with _connect(test_dsn, schema) as conn:
        assert all(_ledger(conn, iid)[2] == 0 for iid in ids)
