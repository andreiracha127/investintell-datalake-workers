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


# ─────────────────────────────────────────────────────────────────────────────
# Publication against compressed serving history (incident 2026-10-08)
# ─────────────────────────────────────────────────────────────────────────────
# Light's production columnstore DDL: segmentby series_id,dimension,key;
# orderby report_date DESC; compressed after six months.
COLUMNSTORE = """ALTER TABLE nport_lookthrough_exposures SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'series_id,dimension,key',
    timescaledb.orderby = 'report_date DESC')"""
OLD_DAY = dt.date(2025, 1, 31)
OLD_SIBLING = dt.date(2025, 1, 15)  # same chunk: its batches also hold OLD_DAY
OTHER_CHUNK_DAY = dt.date(2025, 3, 31)
COMPRESS_BEFORE = dt.date(2025, 6, 1)
N_SERIES, N_KEYS = 100, 40
CHANGED = "S007"
LIMIT_GUC = "timescaledb.max_tuples_decompressed_per_dml_transaction"
OLD_JOIN_DELETE = """DELETE FROM nport_lookthrough_exposures target
    USING nport_lookthrough_candidate_summary candidate
    WHERE candidate.run_id = %s AND target.series_id = candidate.series_id
      AND target.report_date = candidate.report_date"""
EXPOSURE_ROW = "series_id, report_date, dimension, key, label, direct_pct, indirect_pct"
SUMMARY_CONTENT = ", ".join(worker._SUMMARY_COLS)


def _recent_day():
    return dt.datetime.now(dt.UTC).date() - dt.timedelta(days=20)


def _install_compressed_history(conn):
    """Serving history like production: compressed old chunks, an uncompressed recent one."""
    conn.execute(COLUMNSTORE)
    series = [f"S{n:03d}" for n in range(1, N_SERIES + 1)]
    days = [OLD_SIBLING, OLD_DAY, OTHER_CHUNK_DAY, _recent_day()]
    conn.execute(
        """INSERT INTO nport_lookthrough_exposures
               (series_id, report_date, dimension, key, label, direct_pct, indirect_pct, computed_at)
           SELECT s, d, 'issuer', 'K' || lpad(k::text, 2, '0'), 'Issuer ' || k,
                  k / 10.0, 0, now() - interval '30 days'
           FROM unnest(%s::text[]) s, generate_series(0, %s - 1) k, unnest(%s::date[]) d""",
        (series, N_KEYS, days),
    )
    conn.execute(
        f"""INSERT INTO nport_lookthrough_summary ({SUMMARY_CONTENT}, computed_at)
            SELECT s, d, 100, 100, 0, 0, 0, 0, 0, 50, 50, 0, 99.5, %s, 0, d,
                   now() - interval '30 days'
            FROM unnest(%s::text[]) s, unnest(%s::date[]) d""",
        (N_KEYS, series, days),
    )
    for (chunk,) in conn.execute(
        "SELECT show_chunks('nport_lookthrough_exposures', older_than => %s::date)",
        (COMPRESS_BEFORE,),
    ).fetchall():
        conn.execute("SELECT compress_chunk(%s)", (chunk,))
    conn.commit()
    assert _chunk(conn, OLD_DAY) == _chunk(conn, OLD_SIBLING) != _chunk(conn, OTHER_CHUNK_DAY)
    assert _heap_rows(conn, OLD_DAY) == _heap_rows(conn, OTHER_CHUNK_DAY) == 0
    assert conn.execute(
        "SELECT count(*) FROM nport_lookthrough_exposures WHERE report_date < %s", (COMPRESS_BEFORE,),
    ).fetchone()[0] == N_SERIES * N_KEYS * 3 > 10_000
    return series


def _chunk(conn, day):
    return conn.execute(
        """SELECT format('%%I.%%I', chunk_schema, chunk_name)
           FROM timescaledb_information.chunks
           WHERE format('%%I.%%I', hypertable_schema, hypertable_name)::regclass
                 = 'nport_lookthrough_exposures'::regclass
             AND (range_start AT TIME ZONE 'UTC')::date <= %s
             AND %s < (range_end AT TIME ZONE 'UTC')::date""",
        (day, day),
    ).fetchone()[0]


def _heap_rows(conn, day, *, by_series=False):
    """Rows in the chunk's uncompressed heap: what DML decompressed or inserted."""
    chunk = _chunk(conn, day)
    if by_series:
        return dict(conn.execute(
            f"SELECT series_id, count(*) FROM ONLY {chunk} GROUP BY series_id"
        ).fetchall())
    return conn.execute(f"SELECT count(*) FROM ONLY {chunk}").fetchone()[0]


def _exposures(conn, day, series):
    return conn.execute(
        f"""SELECT {EXPOSURE_ROW} FROM nport_lookthrough_exposures
            WHERE report_date = %s AND series_id = ANY(%s) ORDER BY series_id, dimension, key""",
        (day, series),
    ).fetchall()


def _summaries(conn, day, series):
    return conn.execute(
        f"""SELECT {SUMMARY_CONTENT} FROM nport_lookthrough_summary
            WHERE report_date = %s AND series_id = ANY(%s) ORDER BY series_id""",
        (day, series),
    ).fetchall()


def _stage_copy(conn, run_id, day, series):
    """Stage candidates identical in content to the serving rows."""
    conn.execute(
        f"""INSERT INTO nport_lookthrough_candidate_exposures (run_id, {EXPOSURE_ROW})
            SELECT %s, {EXPOSURE_ROW} FROM nport_lookthrough_exposures
            WHERE report_date = %s AND series_id = ANY(%s)""",
        (run_id, day, series),
    )
    conn.execute(
        f"""INSERT INTO nport_lookthrough_candidate_summary (run_id, {SUMMARY_CONTENT})
            SELECT %s, {SUMMARY_CONTENT} FROM nport_lookthrough_summary
            WHERE report_date = %s AND series_id = ANY(%s)""",
        (run_id, day, series),
    )


def _stage_changed_key(conn, run_id):
    """CHANGED at OLD_DAY: one value revised, one key dropped, one key added."""
    _stage_copy(conn, run_id, OLD_DAY, [CHANGED])
    conn.execute("""UPDATE nport_lookthrough_candidate_exposures SET direct_pct = 77
                    WHERE run_id = %s AND key = 'K00'""", (run_id,))
    conn.execute("""DELETE FROM nport_lookthrough_candidate_exposures
                    WHERE run_id = %s AND key = %s""", (run_id, f"K{N_KEYS - 1:02d}"))
    conn.execute("""INSERT INTO nport_lookthrough_candidate_exposures
                    (run_id, series_id, report_date, dimension, key, label, direct_pct)
                    VALUES (%s, %s, %s, 'issuer', 'KNEW', 'New issuer', 3)""",
                 (run_id, CHANGED, OLD_DAY))


@pytest.fixture
def compressed_history(dsn):
    """A non-superuser owner (like worker_writer) owns the schema and its hypertables."""
    role = f"lookthrough_writer_{uuid.uuid4().hex[:8]}"
    schema = f"lookthrough_compressed_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER").format(sql.Identifier(role)))
        admin.execute(sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(
            sql.Identifier(schema), sql.Identifier(role)))

    def connect(**options):
        conn = psycopg.connect(psycopg.conninfo.make_conninfo(dsn, user=role), **options)
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        if not options.get("autocommit"):
            conn.commit()
        return conn

    try:
        with connect() as conn:
            assert conn.execute(
                "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
            ).fetchone() == (False,)
            conn.execute((Path(__file__).parents[1] / "schemas" / "nport_lookthrough.sql").read_text())
            series = _install_compressed_history(conn)
        yield connect, series
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            admin.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_incident_join_delete_decompresses_all_history(compressed_history):
    """Reproduction: one candidate key in an uncompressed chunk still made the
    old join DELETE decompress every compressed chunk; the new path does not."""
    connect, _series = compressed_history
    run_id = str(uuid.uuid4())
    with connect() as conn:
        _stage_copy(conn, run_id, _recent_day(), ["S001"])
        conn.execute("UPDATE nport_lookthrough_candidate_exposures SET direct_pct = 5 "
                     "WHERE run_id = %s AND key = 'K01'", (run_id,))
        # A lowered limit, still far above anything the candidate needs.
        conn.execute(f"SET {LIMIT_GUC} = 1000")
        conn.commit()
        with pytest.raises(psycopg.errors.ConfigurationLimitExceeded):
            conn.execute(OLD_JOIN_DELETE, (run_id,))
        conn.rollback()
        stats = worker._publish_staged(conn, run_id)
        conn.commit()
        assert stats["changed_keys"] == 1 and stats["decompression_bound"] == 0
        assert _exposures(conn, _recent_day(), ["S001"])[1][5] == 5
        assert _heap_rows(conn, OLD_DAY) == _heap_rows(conn, OTHER_CHUNK_DAY) == 0
        assert conn.execute(f"SHOW {LIMIT_GUC}").fetchone()[0] == "1000"


def test_changed_key_in_compressed_chunk_publishes_within_the_counted_bound(compressed_history):
    """Constant per-date quals scope decompression to the changed series'
    segments; the limit is SET LOCAL to the counted bound, overriding a
    session limit far below it."""
    connect, _series = compressed_history
    run_id = str(uuid.uuid4())
    # Each CHANGED batch in the chunk holds OLD_SIBLING and OLD_DAY.
    expected_bound = N_KEYS * 2
    with connect() as conn:
        _stage_changed_key(conn, run_id)
        conn.commit()
        # The bound is exact here: the DELETE really decompresses that many.
        conn.execute(f"SET LOCAL {LIMIT_GUC} = {expected_bound - 1}")
        conn.execute("SET LOCAL plan_cache_mode = force_custom_plan")
        with pytest.raises(psycopg.errors.ConfigurationLimitExceeded):
            conn.execute("DELETE FROM nport_lookthrough_exposures "
                         "WHERE report_date = %s AND series_id = ANY(%s)", (OLD_DAY, [CHANGED]))
        conn.rollback()
        conn.execute(f"SET {LIMIT_GUC} = 1")
        conn.commit()
        stats = worker._publish_staged(conn, run_id)
        assert conn.execute(f"SHOW {LIMIT_GUC}").fetchone()[0] == str(expected_bound)
        conn.commit()
        assert conn.execute(f"SHOW {LIMIT_GUC}").fetchone()[0] == "1"
    assert stats == {
        "candidate_keys": 1, "changed_keys": 1, "unchanged_keys": 0, "report_dates": 1,
        "decompression_bound": expected_bound,
        "decompression_ceiling": worker.DEFAULT_DECOMPRESSION_CEILING,
        "exposure_rows_inserted": N_KEYS,
    }
    with connect() as conn:
        # Only CHANGED left the compressed batches: its sibling-date rows plus
        # the inserted replacement. The other chunk was never touched.
        assert _heap_rows(conn, OLD_DAY, by_series=True) == {CHANGED: N_KEYS * 2}
        assert _heap_rows(conn, OTHER_CHUNK_DAY) == 0


def test_prepared_generic_plans_cannot_widen_decompression(compressed_history, monkeypatch):
    """psycopg prepares a statement after five executions and Postgres may then
    switch to a generic plan whose $n quals give Timescale no batch filter (the
    2026-10-07 NAV incident). The publication forces custom plans locally, so
    even a session pinned to generic plans stays segment-scoped."""
    connect, series = compressed_history
    changed = series[:12]
    monkeypatch.setattr(worker, "PUBLISH_SERIES_BATCH", 1)  # 12 executions per statement
    run_id = str(uuid.uuid4())
    with connect() as conn:
        _stage_copy(conn, run_id, OLD_DAY, changed)
        conn.execute("UPDATE nport_lookthrough_candidate_exposures SET direct_pct = direct_pct + 1 "
                     "WHERE run_id = %s AND key = 'K00'", (run_id,))
        conn.execute("SET plan_cache_mode = force_generic_plan")
        conn.commit()
        stats = worker._publish_staged(conn, run_id)
        conn.commit()
        assert conn.execute("SHOW plan_cache_mode").fetchone()[0] == "force_generic_plan"
    assert stats["changed_keys"] == len(changed)
    assert stats["decompression_bound"] == len(changed) * N_KEYS * 2
    with connect() as conn:
        assert set(_heap_rows(conn, OLD_DAY, by_series=True)) == set(changed)


def test_changed_key_is_replaced_and_its_chunk_neighbours_stay_untouched(compressed_history):
    connect, series = compressed_history
    neighbours = [sid for sid in series if sid != CHANGED]
    run_id = str(uuid.uuid4())
    with connect() as conn:
        before_neighbours = _exposures(conn, OLD_DAY, neighbours)
        before_sibling = _exposures(conn, OLD_SIBLING, series)
        before_summaries = _summaries(conn, OLD_DAY, neighbours)
        _stage_changed_key(conn, run_id)
        conn.execute("UPDATE nport_lookthrough_candidate_summary SET n_children_expanded = 3 "
                     "WHERE run_id = %s", (run_id,))
        candidate = conn.execute(
            f"""SELECT {EXPOSURE_ROW} FROM nport_lookthrough_candidate_exposures
                WHERE run_id = %s ORDER BY series_id, dimension, key""", (run_id,),
        ).fetchall()
        candidate_summary = conn.execute(
            f"""SELECT {SUMMARY_CONTENT}, computed_at FROM nport_lookthrough_candidate_summary
                WHERE run_id = %s""", (run_id,),
        ).fetchall()
        conn.commit()
        worker._publish_staged(conn, run_id)
        conn.commit()
        assert len(candidate) == N_KEYS
        assert (CHANGED, OLD_DAY, "issuer", "K00", "Issuer 0", 77, 0) in candidate
        assert _exposures(conn, OLD_DAY, [CHANGED]) == candidate
        assert conn.execute(
            f"""SELECT {SUMMARY_CONTENT}, computed_at FROM nport_lookthrough_summary
                WHERE series_id = %s AND report_date = %s""", (CHANGED, OLD_DAY),
        ).fetchall() == candidate_summary
        assert _exposures(conn, OLD_DAY, neighbours) == before_neighbours
        assert _exposures(conn, OLD_SIBLING, series) == before_sibling
        assert _summaries(conn, OLD_DAY, neighbours) == before_summaries
        # Neighbours were neither decompressed nor rewritten.
        assert set(_heap_rows(conn, OLD_DAY, by_series=True)) == {CHANGED}
        assert conn.execute(
            "SELECT count(*) FROM nport_lookthrough_candidate_exposures"
        ).fetchone()[0] == 0


def test_republishing_unchanged_content_issues_no_exposure_dml(compressed_history):
    """Unchanged keys only advance their summary computed_at (the freshness
    probe reads it as the key's last verification); the compressed exposure
    hypertable sees no DML at all."""
    connect, series = compressed_history
    run_id = str(uuid.uuid4())
    days = (OLD_DAY, _recent_day())
    with connect() as conn:
        exposures_before = {day: _exposures(conn, day, series) for day in days}
        summaries_before = {day: _summaries(conn, day, series) for day in days}
        for day in days:
            _stage_copy(conn, run_id, day, series)
        staged_at = conn.execute(
            "SELECT DISTINCT computed_at FROM nport_lookthrough_candidate_summary WHERE run_id = %s",
            (run_id,),
        ).fetchall()
        conn.execute(f"SET {LIMIT_GUC} = 1")
        conn.commit()
        stats = worker._publish_staged(conn, run_id)
        activity = {
            (schema, name): (ins, upd, dele)
            for schema, name, ins, upd, dele in conn.execute(
                """SELECT schemaname, relname, n_tup_ins, n_tup_upd, n_tup_del
                   FROM pg_stat_xact_all_tables WHERE n_tup_ins + n_tup_upd + n_tup_del > 0"""
            ).fetchall()
        }
        exposure_relations, summary_relations = (
            {tuple(chunk.split(".", 1)) for (chunk,) in conn.execute(
                "SELECT show_chunks(%s::regclass)::text", (table,)).fetchall()}
            for table in ("nport_lookthrough_exposures", "nport_lookthrough_summary")
        )
        conn.commit()
        assert stats["changed_keys"] == 0 and stats["unchanged_keys"] == 2 * N_SERIES
        assert stats["decompression_bound"] == 0 and stats["exposure_rows_inserted"] == 0
        touched = set(activity)
        assert not touched & exposure_relations
        assert not any(name.startswith("compress_hyper_") for _schema, name in touched)
        assert not any(name == "nport_lookthrough_exposures" for _schema, name in touched)
        summary_activity = [activity[rel] for rel in touched & summary_relations]
        assert sum(upd for _ins, upd, _del in summary_activity) == 2 * N_SERIES
        assert sum(ins + dele for ins, _upd, dele in summary_activity) == 0
        for day in days:
            assert _exposures(conn, day, series) == exposures_before[day]
            assert _summaries(conn, day, series) == summaries_before[day]
        assert conn.execute(
            "SELECT DISTINCT computed_at FROM nport_lookthrough_summary WHERE report_date = ANY(%s)",
            (list(days),),
        ).fetchall() == staged_at
        assert _heap_rows(conn, OLD_DAY) == 0


def test_decompression_ceiling_breach_fails_loud_and_preserves_last_good(
    database, raw_source, pipeline_tables, monkeypatch,
):
    """Through the real promotion: no write and no receipt; then a lawful
    ceiling publishes and certifies in the same transaction."""
    with database() as conn:
        series = _install_compressed_history(conn)
        run_id = str(uuid.uuid4())
        _stage_changed_key(conn, run_id)
        conn.commit()
    with database() as producer:
        before = _exposures(producer, OLD_DAY, series)
        source = freshness.read_source_cohort(producer)
        input_snapshot = worker.inputs.snapshot(producer, source, "lookthrough")
        producer.commit()
        monkeypatch.setenv(worker.DECOMPRESSION_CEILING_ENV, str(N_KEYS * 2 - 1))
        with pytest.raises(freshness.FundPipelineBlocked) as blocked:
            worker._promote(producer, source, run_id, None, input_snapshot)
        producer.rollback()
        verdict = blocked.value.verdict
        assert verdict["breaches"] == ["LOOKTHROUGH_DECOMPRESSION_CEILING_EXCEEDED"]
        assert verdict["decompression_bound"] == N_KEYS * 2
        assert verdict["decompression_ceiling"] == N_KEYS * 2 - 1
        assert verdict["ceiling_env"] == worker.DECOMPRESSION_CEILING_ENV
        assert verdict["last_good_preserved"] is True
        assert _exposures(producer, OLD_DAY, series) == before
        assert _heap_rows(producer, OLD_DAY) == 0
        receipts = ("SELECT count(*) FROM public.nport_pipeline_publications "
                    "WHERE stage = 'lookthrough'")
        assert producer.execute(receipts).fetchone()[0] == 0
        assert producer.execute(
            "SELECT count(*) FROM nport_lookthrough_candidate_summary WHERE run_id = %s", (run_id,),
        ).fetchone()[0] == 1
        monkeypatch.setenv(worker.DECOMPRESSION_CEILING_ENV, str(N_KEYS * 2))
        assert worker._promote(producer, source, run_id, None, input_snapshot)["changed_keys"] == 1
        producer.commit()
        assert _exposures(producer, OLD_DAY, [CHANGED]) != [row for row in before if row[0] == CHANGED]
        assert producer.execute(receipts).fetchone()[0] == 1


def test_publication_lock_holds_off_chunk_compression(compressed_history):
    """The bound count and the DML see one compression state: the policy
    cannot compress a serving chunk until the publication commits; readers
    are not blocked."""
    connect, _series = compressed_history
    run_id = str(uuid.uuid4())
    with connect() as producer, connect(autocommit=True) as policy:
        _stage_copy(producer, run_id, _recent_day(), ["S001"])
        producer.commit()
        worker._publish_staged(producer, run_id)
        recent = _chunk(policy, _recent_day())
        policy.execute("SET lock_timeout = '300ms'")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            policy.execute("SELECT compress_chunk(%s)", (recent,))
        assert policy.execute("SELECT count(*) FROM nport_lookthrough_exposures").fetchone()[0] > 0
        producer.commit()
        policy.execute("SELECT compress_chunk(%s)", (recent,))
