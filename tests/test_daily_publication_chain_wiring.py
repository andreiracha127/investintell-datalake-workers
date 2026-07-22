"""Wiring tests for the daily publication chain worker (Increment 2, Task 6).

Proves the real stage->worker wiring: the eight frozen stages in binding order,
worker-result classification, the advisory lock that blocks overlapping runs, and
a DARK-mode smoke of the real bond lane (bond_security_master / bond_price_
observations / bond_serving) against a protocol-only schema — every stage reports
``dark_no_source`` and nothing is promoted (reported, never a silent success).
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _daily_chain_fixtures import admin_connect, base_dsn, new_schema, worker_conn  # noqa: E402
from _bond_serving_fixtures import protocol_only_schema  # noqa: E402

from src.bonds import daily_chain  # noqa: E402
from src.bonds.daily_chain import (  # noqa: E402
    StageContext,
    StageStatus,
    TerminalStageError,
    TransientStageError,
    classify_worker_result,
)
from src.db import LOCK_DAILY_PUBLICATION_CHAIN, advisory_lock
from src.workers import daily_publication_chain as chain_worker  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL not set"
)

D1 = date(2025, 12, 31)


def _search_path_dsn(schema: str) -> str:
    base = base_dsn()
    if base.startswith("postgres"):
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}options=-c%20search_path%3D{schema}"
    return f"{base} options='-c search_path={schema}'"


# --------------------------------------------------------------------------- #
# Static wiring
# --------------------------------------------------------------------------- #

def test_default_stages_are_the_eight_frozen_stages_in_order():
    names = [s.name for s in chain_worker.build_default_stages()]
    assert names == list(daily_chain.STAGE_ORDER)
    assert names == ["ingest", "pit_update", "materialize", "mixed_build",
                     "validate", "promote", "refresh", "probe"]


def test_build_stages_subset_preserves_frozen_order():
    names = [s.name for s in chain_worker.build_stages(["probe", "ingest", "refresh"])]
    assert names == ["ingest", "refresh", "probe"]


def test_build_watermarks_reports_max_date_per_source_and_dark_is_empty():
    admin = admin_connect()
    schema = new_schema(admin)
    conn = worker_conn(schema)
    try:
        # Dark: no source tables exist -> empty watermarks, never a fabricated date.
        assert chain_worker.build_watermarks(conn, D1) == {}
        # A present source with rows -> its max observed date (isoformat).
        conn.execute("CREATE TABLE bond_price_observation (as_of date)")
        conn.execute("INSERT INTO bond_price_observation VALUES ('2025-01-10'),('2025-02-15')")
        # A present-but-empty source -> honest None (no rows to date from).
        conn.execute("CREATE TABLE ncen_effective_filings (effective_date date)")
        conn.commit()
        wm = chain_worker.build_watermarks(conn, D1)
        assert wm == {"bond_price_observation": "2025-02-15", "ncen_effective_filings": None}
    finally:
        conn.close()
        admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_staleness_threshold_is_env_configurable(monkeypatch):
    monkeypatch.delenv("DAILY_CHAIN_STALENESS_THRESHOLD_DAYS", raising=False)
    assert chain_worker._staleness_threshold() == chain_worker._DEFAULT_STALENESS_THRESHOLD_DAYS
    monkeypatch.setenv("DAILY_CHAIN_STALENESS_THRESHOLD_DAYS", "7")
    assert chain_worker._staleness_threshold() == 7
    monkeypatch.setenv("DAILY_CHAIN_STALENESS_THRESHOLD_DAYS", "-1")  # negative disables
    assert chain_worker._staleness_threshold() is None


def test_classify_worker_result_mapping():
    assert classify_worker_result({"state": "ok", "rows": 3}).status is StageStatus.SUCCEEDED
    assert classify_worker_result({"status": "ready"}).status is StageStatus.SUCCEEDED
    dark = classify_worker_result({"state": "no_source", "rows": 0})
    assert dark.status is StageStatus.SKIPPED and dark.reason == "dark_no_source"
    assert classify_worker_result({"state": "no_observations"}).reason == "dark_no_source"
    with pytest.raises(TransientStageError):
        classify_worker_result({"state": "locked"})
    with pytest.raises(TerminalStageError):
        classify_worker_result({"state": "failed"})
    with pytest.raises(TerminalStageError):
        classify_worker_result({"state": "weird_unmapped"})


def test_ingest_stage_missing_source_root_is_reported_dark(monkeypatch):
    # A missing local SOURCE_ROOT makes the ingestion workers raise
    # FileNotFoundError; the ingest stage maps that to a REPORTED dark skip, not
    # a crash (reconciled with the required-stage rule).
    def raising(dsn, *, calc_date=None):
        raise FileNotFoundError("source root absent")

    monkeypatch.setattr(chain_worker, "_worker", lambda name: raising)
    ctx = StageContext(conn=None, dsn=base_dsn(), source_day=D1,
                       run_id=daily_chain.chain_run_id_for("c", D1, "r", "v"),
                       chain="c", code_revision="r", config_version="v")
    outcome = chain_worker.stage_ingest(ctx)
    assert outcome.status is StageStatus.SKIPPED and outcome.reason == "dark_no_source"


# --------------------------------------------------------------------------- #
# Advisory lock blocks overlapping runs
# --------------------------------------------------------------------------- #

def test_advisory_lock_blocks_overlapping_run():
    admin = admin_connect()
    schema = new_schema(admin)
    holder = worker_conn(schema)
    try:
        with advisory_lock(holder, LOCK_DAILY_PUBLICATION_CHAIN) as got:
            assert got  # this connection holds the chain-run lock
            # A second run cannot acquire it -> returns locked, does no work.
            result = chain_worker.run(_search_path_dsn(schema))
            assert result["state"] == "locked"
            assert result["runs"] == []
        # Once released, the lock is acquirable again (no source days here).
        result2 = chain_worker.run(_search_path_dsn(schema))
        assert result2["state"] in ("no_source_days", "ok")
    finally:
        holder.close()
        admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


# --------------------------------------------------------------------------- #
# Real bond-lane DARK smoke through the chain engine
# --------------------------------------------------------------------------- #

def test_real_bond_lane_runs_dark_and_promotes_nothing():
    admin = admin_connect()
    book_schema = None
    dark_schema = None
    try:
        # Bookkeeping conn lives in its own schema; the real workers are routed
        # (via dsn search_path) into a protocol-only schema with NO bond source.
        cur = admin.cursor()
        dark_schema = protocol_only_schema(cur)  # bond serving DDL, no snapshots
        admin.commit()
        book_schema = new_schema(admin)
        book_conn = worker_conn(book_schema)
        dsn = _search_path_dsn(dark_schema)

        stages = chain_worker.build_stages(["pit_update", "refresh", "probe"])
        summaries = daily_chain.run_chain(
            book_conn, stages=stages, source_days=[D1],
            code_revision="revtest", config_version="v1", dsn=dsn,
        )
        book_conn.close()

        s = summaries[0]
        # Every real bond-lane stage reported a dark skip; probe (read-only) ok.
        by_stage = {st["stage"]: st for st in s["stages"]}
        assert by_stage["pit_update"]["status"] == "skipped"
        assert by_stage["pit_update"]["reason"] == "dark_no_source"
        assert by_stage["refresh"]["status"] == "skipped"
        assert by_stage["refresh"]["reason"] == "dark_no_source"
        assert by_stage["probe"]["status"] == "succeeded"
        # Reported, not silent: completed in dark mode, nothing promoted.
        assert s["status"] == "completed"
        assert s["promoted"] == []
        assert "DARK mode" in (s["alert"] or "")
        pointer = admin.execute(
            f'SELECT count(*) FROM "{dark_schema}".sec_derived_current_pointers'
        ).fetchone()[0]
        assert pointer == 0
    finally:
        if book_schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{book_schema}" CASCADE')
        if dark_schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{dark_schema}" CASCADE')
        admin.close()
