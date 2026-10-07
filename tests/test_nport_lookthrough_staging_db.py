"""Atomic staging proof on our own disposable pinned Timescale container.

Opt in with NPORT_LOOKTHROUGH_STAGING_DB_TEST=1. No DSN or repository
credentials are read. The container publishes only a random localhost port.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
import subprocess
import time
import uuid

import psycopg
import pytest
from psycopg import sql

from src.workers import _fund_pipeline_freshness as freshness
from src.workers import nport_lookthrough as worker

pytestmark = pytest.mark.skipif(
    os.getenv("NPORT_LOOKTHROUGH_STAGING_DB_TEST") != "1",
    reason="requires an explicitly enabled disposable TimescaleDB Docker test",
)


def _docker(*args, check=True):
    result = subprocess.run(["docker", *args], text=True, capture_output=True, timeout=40)
    if check and result.returncode:
        raise AssertionError(result.stderr or result.stdout)
    return result


@pytest.fixture(scope="module")
def dsn():
    name = f"nport-lookthrough-staging-test-{uuid.uuid4().hex[:12]}"
    _docker("run", "--detach", "--rm", "--pull=never", "--name", name,
            "--env", "POSTGRES_HOST_AUTH_METHOD=trust", "--publish", "127.0.0.1::5432",
            "timescale/timescaledb:2.27.2-pg18")
    try:
        endpoint = _docker("port", name, "5432/tcp").stdout.strip()
        assert endpoint.startswith("127.0.0.1:")
        connection_string = f"host=127.0.0.1 port={endpoint.rsplit(':', 1)[1]} dbname=postgres user=postgres"
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                with psycopg.connect(connection_string, connect_timeout=2) as conn:
                    conn.execute("SELECT 1")
                break
            except psycopg.OperationalError:
                time.sleep(0.25)
        else:
            pytest.fail("disposable TimescaleDB did not start")
        yield connection_string
    finally:
        _docker("stop", "--time=1", name, check=False)


@pytest.fixture
def database(dsn):
    schema = f"lookthrough_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(dsn, autocommit=True) as owner:
        owner.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    def connect():
        conn = psycopg.connect(dsn)
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        conn.commit()
        return conn

    try:
        with connect() as conn:
            conn.execute((Path(__file__).parents[1] / "schemas" / "nport_lookthrough.sql").read_text())
        yield connect
    finally:
        with psycopg.connect(dsn, autocommit=True) as owner:
            owner.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def _outputs(value, issuer):
    day = dt.datetime.now(dt.UTC).date() - dt.timedelta(days=20)
    holding = {"cusip": "111111111", "issuer_name": issuer, "asset_class": "EC",
               "currency": "USD", "pct_of_nav": value, "isin": "US1111111111"}
    return worker.expand_series("S1", lambda _sid: (day, [holding]), {"cusip": {}, "isin": {}})


def _currency(conn):
    return conn.execute(
        "SELECT direct_pct FROM nport_lookthrough_exposures "
        "WHERE series_id='S1' AND dimension='currency' AND key='USD'"
    ).fetchone()[0]


def _seed(database):
    original, summary = _outputs(100, "Old")
    candidate, next_summary = _outputs(80, "New")
    run_id = str(uuid.uuid4())
    with database() as conn:
        worker._upsert_series(conn, "S1", original, summary, 100)
    with database() as conn:
        worker._upsert_series(conn, "S1", candidate, next_summary, 100, run_id)
    return run_id, next_summary


def test_candidates_remain_invisible_until_one_atomic_promotion(database):
    run_id, summary = _seed(database)
    day = summary["report_date"]
    source = freshness.SourceCohort(
        day, day, freshness.month_start(day, -2),
        series={"S1": freshness.Observation("S1", day, 1,
                                            dt.datetime.now(dt.UTC) - dt.timedelta(hours=1))},
        verdict={"alarm": False},
    )
    with database() as producer, database() as observer:
        assert worker._probe_staged(producer, source, run_id)["alarm"] is False
        assert _currency(observer) == 100
        worker._publish_staged(producer, run_id)
        assert _currency(producer) == 80
        assert _currency(observer) == 100
        producer.commit()
        assert _currency(observer) == 80
        assert producer.execute("SELECT count(*) FROM nport_lookthrough_candidate_summary").fetchone()[0] == 0


def test_insert_failure_rolls_back_deletion_and_preserves_last_good(database):
    run_id, _summary = _seed(database)
    with database() as conn:
        conn.execute("ALTER TABLE nport_lookthrough_exposures ADD CONSTRAINT reject_new CHECK(label <> 'New')")
    with database() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            worker._publish_staged(conn, run_id)
        conn.rollback()
        assert _currency(conn) == 100
        assert conn.execute("SELECT count(*) FROM nport_lookthrough_candidate_summary").fetchone()[0] == 1
