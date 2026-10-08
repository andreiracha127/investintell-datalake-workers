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

from src import db
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


@pytest.fixture
def raw_source(dsn):
    """Minimal public source relations read_source_cohort scans, removed afterwards."""
    day = dt.datetime.now(dt.UTC).date() - dt.timedelta(days=40)
    with psycopg.connect(dsn, autocommit=True) as owner:
        owner.execute("""CREATE TABLE public.sec_nport_holdings (
            series_id text, report_date date, created_at timestamptz,
            asset_class text, market_value bigint, pct_of_nav numeric DEFAULT 100)""")
        owner.execute("CREATE TABLE public.cagg_nport_series_profile "
                      "(series_id text, report_day date, n_holdings integer, coverage_pct numeric DEFAULT 100)")
        owner.execute("INSERT INTO public.sec_nport_holdings "
                      "SELECT 'S' || n, %s, now(), 'EC', 1 FROM generate_series(1, 1000) n", (day,))
        for report_day in (day, freshness.month_start(day, -2) - dt.timedelta(days=1)):
            owner.execute("INSERT INTO public.cagg_nport_series_profile "
                          "SELECT 'S' || n, %s, 1 FROM generate_series(1, 1000) n", (report_day,))
    try:
        yield day
    finally:
        with psycopg.connect(dsn, autocommit=True) as owner:
            owner.execute("DROP TABLE public.sec_nport_holdings, public.cagg_nport_series_profile")


def test_promotion_is_serialized_with_the_loader_lock(dsn, database, raw_source, pipeline_tables):
    run_id, _summary = _seed(database)
    with database() as producer, database() as observer,             psycopg.connect(dsn, autocommit=True) as loader:
        source = freshness.read_source_cohort(producer)
        input_snapshot = worker.inputs.snapshot(producer, source, "lookthrough")
        producer.commit()
        # A running load holds the lifecycle lock (nport_parallel_load's blocking
        # session lock): promotion refuses instead of waiting.
        loader.execute("SELECT pg_advisory_lock(%s)", (db.LOCK_NPORT_LOAD,))
        with pytest.raises(freshness.FundPipelineBlocked, match="SOURCE_LOAD_IN_PROGRESS"):
            worker._promote(producer, source, run_id, None, input_snapshot)
        producer.rollback()
        assert _currency(observer) == 100
        # A load that committed after the source read blocks promotion too.
        loader.execute("INSERT INTO public.sec_nport_holdings VALUES ('S1', %s, now(), 'EC', 1)",
                       (raw_source + dt.timedelta(days=31),))
        loader.execute("SELECT pg_advisory_unlock(%s)", (db.LOCK_NPORT_LOAD,))
        with pytest.raises(freshness.FundPipelineBlocked, match="SOURCE_CHANGED_DURING_BUILD"):
            worker._promote(producer, source, run_id, None, input_snapshot)
        producer.rollback()
        assert _currency(observer) == 100
        # Once promotion holds the lock, a starting load waits until COMMIT.
        source = freshness.read_source_cohort(producer)
        input_snapshot = worker.inputs.snapshot(producer, source, "lookthrough")
        producer.commit()
        worker._promote(producer, source, run_id, None, input_snapshot)
        loader.execute("SET lock_timeout = '300ms'")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            loader.execute("SELECT pg_advisory_lock(%s)", (db.LOCK_NPORT_LOAD,))
        producer.commit()
        assert _currency(observer) == 80
        loader.execute("SELECT pg_advisory_lock(%s)", (db.LOCK_NPORT_LOAD,))
        loader.execute("SELECT pg_advisory_unlock(%s)", (db.LOCK_NPORT_LOAD,))


@pytest.fixture
def pipeline_tables(dsn):
    """Only disposable public inputs; production credentials are never consulted."""
    from src.workers import characteristics

    tables = [
        "instruments_universe", "instrument_identity", "sec_cusip_ticker_map", "nav_timeseries",
        "sec_fund_classes", "sec_etfs", "sec_company_tickers_mf", "sec_isin_sector",
        "company_characteristics_monthly", "equity_characteristics_monthly",
        "nport_equity_exposure_summary", "nport_equity_country_exposures",
        "nport_equity_holding_weights", "nport_pipeline_publications",
        "nport_lookthrough_summary", "nport_lookthrough_exposures",
        "nport_lookthrough_candidate_summary", "nport_lookthrough_candidate_exposures",
    ]
    with psycopg.connect(dsn) as conn:
        conn.execute("""
            CREATE TABLE instruments_universe (instrument_id uuid, ticker text, isin text, attributes jsonb);
            CREATE TABLE instrument_identity (instrument_id uuid, sec_series_id text, cusip_9 text, isin text);
            CREATE TABLE sec_cusip_ticker_map (cusip text, ticker text, issuer_cik text, gics_sector text);
            CREATE TABLE nav_timeseries (instrument_id uuid, nav_date date, nav numeric);
            CREATE TABLE sec_fund_classes (ticker text, series_id text);
            CREATE TABLE sec_etfs (ticker text, series_id text, isin text);
            CREATE TABLE sec_company_tickers_mf (ticker text, series_id text);
            CREATE TABLE sec_isin_sector (isin text, gics_sector text);
        """)
        worker.ensure_schema(conn)
        if hasattr(characteristics, "ensure_schema"):
            characteristics.ensure_schema(conn)
        else:  # Allow this regression fixture to run against the pre-bootstrap worker.
            conn.execute((Path(__file__).parents[1] / "schemas" / "characteristics.sql").read_text())
    try:
        yield
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            for table in tables:
                conn.execute(sql.SQL("DROP TABLE IF EXISTS public.{} CASCADE").format(sql.Identifier(table)))


def _seed_pipeline(dsn):
    exposures, summary = _outputs(100, "Old")
    day = summary["report_date"]
    source = freshness.SourceCohort(
        day, day, freshness.month_start(day, -2),
        series={"S1": freshness.Observation("S1", day, 1,
                    dt.datetime.now(dt.UTC) - dt.timedelta(hours=1), True)},
        verdict={"alarm": False},
    )
    with psycopg.connect(dsn) as conn:
        worker._upsert_series(conn, "S1", exposures, summary, 100)
        # CHILD is deliberately outside the parent cohort, but may be expanded.
        conn.execute("INSERT INTO nport_equity_exposure_summary VALUES (%s, 'CHILD', 100, 100, 'q', now())", (day,))
        conn.execute("INSERT INTO nport_equity_country_exposures VALUES (%s, 'CHILD', 'US', 100, 'q', now())", (day,))
        conn.execute("INSERT INTO nport_equity_holding_weights VALUES (%s, 'CHILD', '111111111', 100, 100, 'q', now())", (day,))
        if hasattr(worker, "inputs"):
            worker.inputs.certify(conn, source, "lookthrough", worker.inputs.snapshot(conn, source, "lookthrough"))
    return source


@pytest.mark.parametrize("repair", [
    "UPDATE nport_equity_country_exposures SET country = 'GB'",
    "UPDATE nport_equity_holding_weights SET signed_pct_of_nav = 80",
    "UPDATE nport_equity_exposure_summary SET net_equity_pct = 80",
    "DELETE FROM nport_equity_country_exposures",
    "INSERT INTO sec_isin_sector VALUES ('US1111111111', 'Energy')",
    "INSERT INTO instrument_identity VALUES (gen_random_uuid(), 'CHILD', '111111111', NULL)",
])
def test_exact_sidecar_and_mapping_repairs_invalidate_young_output(dsn, pipeline_tables, repair):
    source = _seed_pipeline(dsn)
    with psycopg.connect(dsn) as conn:
        assert freshness.probe_stage(conn, source, "lookthrough")["alarm"] is False
        conn.execute(repair)  # Same raw report/count/load and even unchanged sidecar computed_at.
        assert freshness.probe_stage(conn, source, "lookthrough")["alarm"] is True


def test_sidecar_writer_is_serialized_through_publication_commit(dsn, pipeline_tables):
    source = _seed_pipeline(dsn)
    with psycopg.connect(dsn) as producer, psycopg.connect(dsn) as writer:
        before = worker.inputs.snapshot(producer, source, "lookthrough")
        producer.commit()
        writer.execute("UPDATE nport_equity_country_exposures SET direct_pct = 80")
        with pytest.raises(freshness.FundPipelineBlocked, match="INPUT_WRITE_IN_PROGRESS"):
            worker.inputs.guard(producer, source, "lookthrough", before)
        producer.rollback()
        writer.rollback()
        worker.inputs.guard(producer, source, "lookthrough", before)
        writer.execute("SET LOCAL lock_timeout = '100ms'")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            writer.execute("UPDATE nport_equity_country_exposures SET direct_pct = 80")
        writer.rollback()
        producer.commit()
        writer.execute("UPDATE nport_equity_country_exposures SET direct_pct = 80")
        writer.commit()
        with pytest.raises(freshness.FundPipelineBlocked, match="INPUT_CHANGED_DURING_BUILD"):
            worker.inputs.guard(producer, source, "lookthrough", before)
        producer.rollback()


@pytest.mark.parametrize("repair_kind", ["nav", "company", "mapping"])
def test_characteristics_lineage_tracks_nav_company_and_identity_repairs(dsn, pipeline_tables, repair_kind):
    from src.workers import characteristics

    source = _seed_pipeline(dsn)
    iid = str(uuid.uuid4())
    with psycopg.connect(dsn) as conn:
        conn.execute("INSERT INTO instruments_universe VALUES (%s, 'T', NULL, '{\"series_id\":\"S1\"}')", (iid,))
        conn.execute("INSERT INTO equity_characteristics_monthly (instrument_id, ticker, as_of) VALUES (%s, 'T', %s)",
                     (iid, source.as_of))
        repairs = {
            "nav": ("INSERT INTO nav_timeseries VALUES (%s, %s, 10)", (iid, source.as_of)),
            "company": ("INSERT INTO company_characteristics_monthly (cik, period_end, book_equity) VALUES (1, %s, 100)", (source.as_of,)),
            "mapping": ("INSERT INTO sec_cusip_ticker_map VALUES ('111111111', 'T', '1', 'Energy')", ()),
        }
        if hasattr(characteristics, "inputs"):
            characteristics.inputs.certify(conn, source, "characteristics",
                                           characteristics.inputs.snapshot(conn, source, "characteristics"))
        assert not freshness.probe_stage(conn, source, "characteristics")["alarm"]
        conn.execute(*repairs[repair_kind])
        assert freshness.probe_stage(conn, source, "characteristics")["alarm"]


def test_characteristics_worker_holds_loader_lock_through_commit(dsn, pipeline_tables, raw_source, monkeypatch):
    from src.workers import characteristics

    iid = str(uuid.uuid4())
    with psycopg.connect(dsn) as conn:
        conn.execute("INSERT INTO instruments_universe VALUES (%s, 'T', NULL, '{\"series_id\":\"S1\"}')", (iid,))
        source = freshness.read_source_cohort(conn)
        assert not source.verdict["alarm"]
    monkeypatch.setattr(characteristics, "connect", lambda *_a: psycopg.connect(dsn))
    monkeypatch.setattr(characteristics, "_run_layer1_setbased", lambda *_a, **_k: (1, 1))
    def layer2(conn, _limit, **_kwargs):
        conn.execute("INSERT INTO equity_characteristics_monthly (instrument_id, ticker, as_of) VALUES (%s, 'T', %s)", (iid, raw_source))
        return 1, 1
    monkeypatch.setattr(characteristics, "_run_layer2_setbased", layer2)
    read = freshness.read_source_cohort
    with psycopg.connect(dsn, autocommit=True) as loader:
        loader.execute("SELECT pg_advisory_lock(%s)", (db.LOCK_NPORT_LOOKTHROUGH,))
        with pytest.raises(freshness.FundPipelineBlocked, match="INPUT_READER_IN_PROGRESS"):
            characteristics.run("unused", source=source)
        loader.execute("SELECT pg_advisory_unlock(%s)", (db.LOCK_NPORT_LOOKTHROUGH,))
        loader.execute("SELECT pg_advisory_lock(%s)", (db.LOCK_NPORT_LOAD,))
        with pytest.raises(freshness.FundPipelineBlocked, match="SOURCE_LOAD_IN_PROGRESS"):
            characteristics.run("unused", source=source)
        assert loader.execute("SELECT count(*) FROM equity_characteristics_monthly").fetchone()[0] == 0
        loader.execute("SELECT pg_advisory_unlock(%s)", (db.LOCK_NPORT_LOAD,))
        loader.execute("SET lock_timeout = '100ms'")
        def final_read(conn, **kwargs):
            result = read(conn, **kwargs)
            with pytest.raises(psycopg.errors.LockNotAvailable):
                loader.execute("SELECT pg_advisory_lock(%s)", (db.LOCK_NPORT_LOAD,))
            assert loader.execute("SELECT count(*) FROM equity_characteristics_monthly").fetchone()[0] == 0
            return result
        monkeypatch.setattr(freshness, "read_source_cohort", final_read)
        assert characteristics.run("unused", source=source)["status"] == "succeeded"
        assert loader.execute("SELECT count(*) FROM equity_characteristics_monthly").fetchone()[0] == 1
        loader.execute("SELECT pg_advisory_lock(%s)", (db.LOCK_NPORT_LOAD,))
        loader.execute("SELECT pg_advisory_unlock(%s)", (db.LOCK_NPORT_LOAD,))


def test_missing_outputs_are_health_alarms_without_ddl(dsn, raw_source):
    from src.workers import fund_pipeline_health as health

    with psycopg.connect(dsn) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        source = freshness.SourceCohort(raw_source, raw_source, freshness.month_start(raw_source, -2))
        assert freshness.probe_stage(conn, source, "characteristics")["breaches"] == ["STAGE_SCHEMA_MISSING"]
        assert freshness.probe_stage(conn, source, "lookthrough")["breaches"] == ["STAGE_SCHEMA_MISSING"]
        assert health._input_watermarks(conn)["lookthrough"] is None
        assert health._latest_classification(conn) is None


def test_receipt_is_atomic_and_bootstrap_is_idempotent(dsn, pipeline_tables):
    source = _seed_pipeline(dsn)
    with psycopg.connect(dsn) as producer, psycopg.connect(dsn) as observer:
        old = observer.execute("SELECT input_signature FROM nport_pipeline_publications").fetchone()[0]
        producer.execute("UPDATE nport_equity_country_exposures SET direct_pct=80")
        worker.inputs.certify(producer, source, "lookthrough", worker.inputs.snapshot(producer, source, "lookthrough"))
        assert observer.execute("SELECT input_signature FROM nport_pipeline_publications").fetchone()[0] == old
        producer.rollback()
        assert producer.execute("SELECT input_signature FROM nport_pipeline_publications").fetchone()[0] == old
        worker.ensure_schema(producer)
        assert producer.execute("SELECT input_signature FROM nport_pipeline_publications").fetchone()[0] == old
        assert _currency(producer) == 100


def test_empty_database_health_is_red_and_read_only(dsn):
    from src.workers import fund_pipeline_health as health

    result = health.run(dsn)
    assert result["state"] == "failed"
    assert "nport" in result["freshness"]["failed_stages"]
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT to_regclass('public.nport_pipeline_publications')").fetchone()[0] is None


def test_cagg_must_materialize_same_count_weight_repairs(dsn, raw_source):
    with psycopg.connect(dsn) as conn:
        source = freshness.read_source_cohort(conn)
        assert not freshness.probe_stage(conn, source, "cagg")["alarm"]
        conn.execute("UPDATE sec_nport_holdings SET pct_of_nav=80")
        repaired = freshness.read_source_cohort(conn)
        assert freshness.probe_stage(conn, repaired, "cagg")["alarm"]


def test_characteristics_bootstrap_preserves_populated_plain_tables(database, pipeline_tables):
    from src.workers import characteristics

    ddl = (Path(__file__).parents[1] / "schemas" / "characteristics.sql").read_text()
    with database() as conn:
        conn.execute(ddl.split("DO $$", 1)[0])
        conn.execute("INSERT INTO company_characteristics_monthly (cik, period_end, book_equity) VALUES (1, current_date, 10)")
        characteristics.ensure_schema(conn)
        assert conn.execute("SELECT book_equity FROM company_characteristics_monthly").fetchone()[0] == 10
        assert conn.execute("SELECT count(*) FROM timescaledb_information.hypertables "
                            "WHERE hypertable_schema = current_schema() "
                            "AND hypertable_name = 'company_characteristics_monthly'").fetchone()[0] == 0


def test_lookthrough_rejects_sidecar_repair_during_build(dsn, pipeline_tables, raw_source, monkeypatch):
    _seed_pipeline(dsn)
    with psycopg.connect(dsn) as conn:
        for table in ("nport_equity_exposure_summary", "nport_equity_country_exposures", "nport_equity_holding_weights"):
            conn.execute(sql.SQL("UPDATE {} SET report_date=%s").format(sql.Identifier(table)), (raw_source,))
        source = freshness.read_source_cohort(conn)
    monkeypatch.setattr(worker.freshness, "probe_stage", lambda _c, _s, stage: {"alarm": stage == "lookthrough"})
    monkeypatch.setattr(worker.nport_identifier_coverage, "probe", lambda *_a, **_k: {"state": "clean"})
    monkeypatch.setattr(worker, "_probe_staged", lambda *_a: {"alarm": False})
    def build(_dsn, _day, _funds, _sectors, _parents, run_id):
        exposures, summary = _outputs(80, "New")
        summary.update(report_date=raw_source, oldest_report_date=raw_source)
        with psycopg.connect(dsn) as conn:
            worker._upsert_series(conn, "S1", exposures, summary, 100, run_id)
            conn.execute("UPDATE nport_equity_country_exposures SET direct_pct=80")
        return 1, 1, 3
    monkeypatch.setattr(worker, "_process_shard", build)
    with pytest.raises(freshness.FundPipelineBlocked, match="INPUT_CHANGED_DURING_BUILD"):
        worker.run(dsn, source=source, serial=True)
    with psycopg.connect(dsn) as conn:
        assert _currency(conn) == 100


def test_auxiliary_guards_work_with_existing_table_write_grants(dsn, pipeline_tables):
    source = _seed_pipeline(dsn)
    role = f"pipeline_writer_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
        conn.execute(sql.SQL("GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO {}").format(sql.Identifier(role)))
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role)))
            before = worker.inputs.snapshot(conn, source, "lookthrough")
            worker.inputs.guard(conn, source, "lookthrough", before)
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_missing_exposure_table_is_a_structured_stage_alarm(dsn, pipeline_tables):
    source = _seed_pipeline(dsn)
    with psycopg.connect(dsn) as conn:
        conn.execute("DROP TABLE nport_lookthrough_exposures")
        assert freshness.probe_stage(conn, source, "lookthrough")["breaches"] == ["STAGE_SCHEMA_MISSING"]
