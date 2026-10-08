"""Permission/refresh contract on an isolated, pinned TimescaleDB container.

Opt in with NPORT_CAGG_REFRESH_DB_TEST=1. No DSN or repository credentials are
read: the test starts its own network-isolated container, with no host ports or
volumes, and uses docker exec. Each case gets a disposable database.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "schemas" / "nport_series_profile_refresh_request_v1.sql"
ROLLBACK = ROOT / "schemas" / "nport_series_profile_refresh_request_v1.rollback.sql"
pytestmark = pytest.mark.skipif(
    os.environ.get("NPORT_CAGG_REFRESH_DB_TEST") != "1",
    reason="requires an explicitly enabled disposable TimescaleDB Docker test",
)


def _docker(*args: str, sql: str | None = None, check: bool = True):
    result = subprocess.run(
        ["docker", *args], input=sql, text=True, capture_output=True,
        timeout=40, check=False,
    )
    if check and result.returncode:
        raise AssertionError(result.stderr or result.stdout)
    return result


@pytest.fixture(scope="module")
def container():
    name = f"nport-cagg-refresh-test-{uuid.uuid4().hex[:12]}"
    _docker(
        "run", "--detach", "--rm", "--pull=never", "--network=none",
        "--name", name, "--env", "POSTGRES_HOST_AUTH_METHOD=trust",
        "timescale/timescaledb:2.27.2-pg18",
    )
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            ready = _docker("exec", name, "pg_isready", "-U", "postgres", check=False)
            if ready.returncode == 0:
                # pg_isready also succeeds during initdb's temporary server.
                running = _docker("exec", name, "cat", "/proc/1/comm")
                if running.stdout.strip() == "postgres":
                    break
            time.sleep(0.25)
        else:
            pytest.fail("disposable TimescaleDB did not start")
        _docker(
            "exec", "-i", name, "psql", "-X", "-U", "postgres", "-d", "postgres",
            "-v", "ON_ERROR_STOP=1", sql="CREATE ROLE worker_writer LOGIN; CREATE ROLE other_reader LOGIN;",
        )
        yield name
    finally:
        _docker("stop", "--time=1", name, check=False)


@pytest.fixture
def db(container):
    db_name = f"nport_cagg_test_{uuid.uuid4().hex[:12]}"

    def execute(sql: str, *, check: bool = True):
        return _docker(
            "exec", "-i", container, "psql", "-X", "-U", "postgres", "-d", db_name,
            "-A", "-t", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose",
            sql=sql, check=check,
        )

    _docker(
        "exec", "-i", container, "psql", "-X", "-U", "postgres", "-d", "postgres",
        # The image's template1 already contains Timescale. Installing from
        # template0 also registers this database with the background launcher.
        "-v", "ON_ERROR_STOP=1", sql=f"CREATE DATABASE {db_name} TEMPLATE template0;",
    )
    execute("""
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        CREATE TABLE public.sec_nport_holdings (
            series_id text NOT NULL, report_date date NOT NULL,
            market_value numeric, pct_of_nav numeric, cusip text
        );
        SELECT public.create_hypertable('public.sec_nport_holdings', 'report_date');
        INSERT INTO public.sec_nport_holdings
            VALUES ('S1', current_date - 70, 1000, 100, '123456789');
        CREATE MATERIALIZED VIEW public.cagg_nport_series_profile
        WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
        SELECT series_id, public.time_bucket('1 day'::interval, report_date) AS report_day,
               count(*) AS n_holdings, sum(market_value) AS total_market_value,
               sum(pct_of_nav) AS coverage_pct,
               sum(CASE WHEN cusip LIKE 'IS:%' OR cusip LIKE 'LE:%' OR cusip LIKE 'H:%'
                        THEN 1 ELSE 0 END) AS n_synthetic
        FROM public.sec_nport_holdings
        GROUP BY series_id, public.time_bucket('1 day'::interval, report_date)
        WITH NO DATA;
        SELECT public.add_continuous_aggregate_policy(
            'public.cagg_nport_series_profile'::regclass,
            start_offset => NULL::interval, end_offset => interval '1 day',
            schedule_interval => interval '6 hours',
            initial_start => clock_timestamp() + interval '6 hours'
        );
        GRANT SELECT ON public.cagg_nport_series_profile TO worker_writer;
    """)
    # This switch permits a counterfactual run against the pre-migration catalog.
    if os.environ.get("NPORT_CAGG_REFRESH_TEST_BASELINE") != "1":
        execute(MIGRATION.read_text(encoding="utf-8"))
    yield execute
    _docker(
        "exec", "-i", container, "psql", "-X", "-U", "postgres", "-d", "postgres",
        "-v", "ON_ERROR_STOP=1", sql=f"DROP DATABASE {db_name} WITH (FORCE);",
    )


def _worker(db, sql: str, *, check: bool = True):
    return db("SET SESSION AUTHORIZATION worker_writer;\n" + sql, check=check)


def _snapshot(db):
    return json.loads(db("""
        SELECT json_build_object(
            'job_id', job_id, 'owner', owner::text, 'config', config,
            'interval', schedule_interval::text, 'scheduled', scheduled,
            'next_start', next_start::text
        ) FROM timescaledb_information.jobs
        WHERE hypertable_name = 'cagg_nport_series_profile';
    """).stdout.strip())


def _wait_for_owner_refresh(db, job_id):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        complete = db(f"""
            SELECT count(*) = 1 AND EXISTS (
                SELECT 1 FROM timescaledb_information.job_stats
                WHERE job_id = {job_id} AND last_run_status = 'Success'
                  AND last_run_started_at > now()-interval '1 minute'
            ) FROM public.cagg_nport_series_profile;
        """).stdout.strip()
        if complete == "t":
            return
        time.sleep(0.25)
    pytest.fail("expedited postgres-owned policy did not materialize source holdings")


def test_worker_request_advances_existing_owner_policy_and_materializes(db):
    before = _snapshot(db)
    result = _worker(db, "SELECT public.request_nport_series_profile_refresh();")
    assert str(before["job_id"]) in result.stdout.splitlines()
    _wait_for_owner_refresh(db, before["job_id"])
    after = _snapshot(db)
    for key in ("job_id", "owner", "config", "interval", "scheduled"):
        assert after[key] == before[key]
    assert db(f"""
        SELECT last_run_status FROM timescaledb_information.job_stats
        WHERE job_id = {before["job_id"]};
    """).stdout.strip() == "Success"


def test_refresh_owner_check_and_definer_transaction_limit_remain(db):
    direct = _worker(db, """
        CALL public.refresh_continuous_aggregate(
            'public.cagg_nport_series_profile'::regclass, current_date-70, current_date-69
        );
    """, check=False)
    assert direct.returncode != 0 and "42501" in direct.stderr
    db("""
        CREATE PROCEDURE public.invalid_definer_refresh()
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
        BEGIN
            CALL public.refresh_continuous_aggregate(
                'public.cagg_nport_series_profile'::regclass, current_date-70, current_date-69
            );
        END $$;
        GRANT EXECUTE ON PROCEDURE public.invalid_definer_refresh() TO worker_writer;
    """)
    wrapped = _worker(db, "CALL public.invalid_definer_refresh();", check=False)
    assert wrapped.returncode != 0 and "25001" in wrapped.stderr


def test_only_worker_can_request_and_no_target_or_range_is_accepted(db):
    other = db("SET SESSION AUTHORIZATION other_reader; SELECT public.request_nport_series_profile_refresh();", check=False)
    assert other.returncode != 0 and "42501" in other.stderr
    for args in (
        "'public.sec_nport_holdings'::regclass",
        "current_date-3650,current_date",
        "'public.sec_nport_holdings'::regclass,current_date-3650,current_date",
    ):
        rejected = _worker(db, f"SELECT public.request_nport_series_profile_refresh({args});", check=False)
        assert rejected.returncode != 0 and "42883" in rejected.stderr
    contract = json.loads(db("""
        SELECT json_build_object('definer', p.prosecdef, 'owner', r.rolname,
                                 'config', p.proconfig)
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_roles r ON r.oid=p.proowner
        WHERE p.oid='public.request_nport_series_profile_refresh()'::regprocedure;
    """).stdout.strip())
    assert contract == {
        "definer": True, "owner": "postgres",
        "config": ["search_path=pg_catalog, pg_temp", "lock_timeout=2s"],
    }


def test_qualified_target_and_exact_alter_job_signature_reject_shadowing(db):
    db("""
        CREATE FUNCTION public.alter_job(integer,timestamptz) RETURNS void
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'overload_hijacked'; END $$;
    """)
    result = _worker(db, """
        CREATE TEMP TABLE cagg_nport_series_profile (report_day date);
        SET search_path=pg_temp,public;
        SELECT public.request_nport_series_profile_refresh();
    """)
    assert result.returncode == 0 and "overload_hijacked" not in result.stderr


@pytest.mark.parametrize("drift", ["disabled", "config", "owner", "interval", "missing"])
def test_policy_drift_fails_closed(db, drift):
    job_id = _snapshot(db)["job_id"]
    statements = {
        "disabled": f"SELECT public.alter_job({job_id},scheduled=>false);",
        "config": f"UPDATE _timescaledb_catalog.bgw_job SET config=jsonb_set(config,'{{end_offset}}','\"2 days\"') WHERE id={job_id};",
        "owner": f"UPDATE _timescaledb_catalog.bgw_job SET owner='worker_writer'::regrole WHERE id={job_id};",
        "interval": f"SELECT public.alter_job({job_id},schedule_interval=>interval '1 minute');",
        "missing": "SELECT public.remove_continuous_aggregate_policy('public.cagg_nport_series_profile');",
    }
    db(statements[drift])
    result = _worker(db, "SELECT public.request_nport_series_profile_refresh();", check=False)
    assert result.returncode != 0 and "55000" in result.stderr


def test_request_inside_transaction_is_allowed_and_already_due_is_not_delayed(db):
    job_id = _snapshot(db)["job_id"]
    db(f"SELECT public.alter_job({job_id},next_start=>clock_timestamp()-interval '1 second');")
    before = _snapshot(db)["next_start"]
    _worker(db, "BEGIN; SELECT public.request_nport_series_profile_refresh(); COMMIT;")
    assert _snapshot(db)["next_start"] == before


def test_recent_attempt_is_not_expedited_again(db):
    job_id = _snapshot(db)["job_id"]
    _worker(db, "SELECT public.request_nport_series_profile_refresh();")
    _wait_for_owner_refresh(db, job_id)
    before = _snapshot(db)["next_start"]
    _worker(db, "SELECT public.request_nport_series_profile_refresh();")
    assert _snapshot(db)["next_start"] == before


def test_reapplication_cleans_extra_acl_and_rollback_keeps_policy(db):
    before = _snapshot(db)
    db("GRANT EXECUTE ON FUNCTION public.request_nport_series_profile_refresh() TO other_reader;")
    db(MIGRATION.read_text(encoding="utf-8"))
    other = db("SET SESSION AUTHORIZATION other_reader; SELECT public.request_nport_series_profile_refresh();", check=False)
    assert other.returncode != 0 and "42501" in other.stderr
    db(ROLLBACK.read_text(encoding="utf-8"))
    assert _snapshot(db) == before
    missing = _worker(db, "SELECT public.request_nport_series_profile_refresh();", check=False)
    assert missing.returncode != 0 and "42883" in missing.stderr
