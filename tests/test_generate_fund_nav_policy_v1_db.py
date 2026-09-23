"""Real disposable PG18/Timescale tests for the read-only current-catalog snapshot."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid
from pathlib import Path

import psycopg
import pytest

from scripts import fund_nav_readiness_schema as operator
from scripts import generate_fund_nav_policy_v1 as generator
from src.workers import fund_nav_readiness as readiness
from src.workers import instrument_ingestion as ingestion
from src.workers import risk_metrics as risk
from src.workers._tiingo import NavObservation

ROOT = Path(__file__).parents[1]
NAV_SQL = (ROOT / "schemas" / "instrument_ingestion.sql").read_text(encoding="utf-8")
SCHEMA_SQL = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
START = dt.date(2024, 1, 1)
END = dt.date(2027, 12, 31)


@pytest.fixture(scope="module")
def dsn():
    value = os.environ["NAV_POLICY_TEST_DSN"]  # never skip this DB gate
    parsed = psycopg.conninfo.conninfo_to_dict(value)
    assert parsed.get("host") in ("127.0.0.1", "localhost", "::1")
    assert parsed.get("dbname", "").startswith("nav_policy_test")
    with psycopg.connect(value) as conn:
        version, ext = conn.execute(
            "SELECT current_setting('server_version_num')::int, "
            "(SELECT extversion FROM pg_extension WHERE extname='timescaledb')"
        ).fetchone()
        assert version >= 180000 and ext.startswith("2.27")
    return value


@pytest.fixture
def catalog(dsn):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS public.instruments_universe
                     (instrument_id uuid PRIMARY KEY,instrument_type text,ticker text,
                      isin text,currency text,is_active boolean)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS public.funds_v
                     (instrument_id uuid,series_id text,ticker text,isin text,
                      currency text,fund_type text)""")
        conn.execute("TRUNCATE TABLE public.instruments_universe, public.funds_v")
        active, inactive = uuid.uuid4(), uuid.uuid4()
        conn.execute(
            "INSERT INTO public.instruments_universe VALUES "
            "(%s,'fund','FAKEA','US0000000001','USD',true),"
            "(%s,'fund','FAKEB','US0000000002','USD',false)",
            (active, inactive),
        )
        conn.execute(
            "INSERT INTO public.funds_v VALUES "
            "(%s,'SYNTH-SERIES','FAKEA','US0000000001','USD','etf')",
            (active,),
        )
    return {"active": active, "inactive": inactive}


def _artifact(dsn, tmp_path, monkeypatch):
    output = tmp_path / "approved-policy.json"
    monkeypatch.setenv("NAV_POLICY_TEST_DSN", dsn)
    args = [
        "build",
        "--dsn-env",
        "NAV_POLICY_TEST_DSN",
        "--custody-root",
        str(tmp_path),
        "--output",
        str(output),
        "--coverage-start",
        START.isoformat(),
        "--coverage-end",
        END.isoformat(),
        "--policy-id",
        "synthetic-xnys",
        "--policy-version",
        "v1",
    ]
    assert generator.main(args) == 0
    raw = output.read_bytes()
    return output, raw, json.loads(raw)


def test_read_only_repeatable_snapshot_and_no_xid(dsn, catalog, monkeypatch):
    original = generator._catalog_rows
    details = []

    def concurrent_update(cursor):
        cursor.execute(
            "SELECT current_setting('transaction_read_only') AS read_only,"
            "txid_current_if_assigned() AS xid"
        )
        details.append(cursor.fetchone())
        with psycopg.connect(dsn, autocommit=True) as writer:
            writer.execute(
                "UPDATE public.instruments_universe SET is_active=false "
                "WHERE instrument_id=%s",
                (catalog["active"],),
            )
        return original(cursor)

    monkeypatch.setattr(generator, "_catalog_rows", concurrent_update)
    instant, instruments, funds = generator.read_catalog_snapshot(dsn)
    assert details[0]["read_only"] == "on" and details[0]["xid"] is None
    assert instant.tzinfo is not None
    assert (
        next(row for row in instruments if row["instrument_id"] == catalog["active"])[
            "is_active"
        ]
        is True
    )
    assert len(funds) == 1
    with psycopg.connect(dsn) as conn:
        assert (
            conn.execute(
                "SELECT is_active FROM public.instruments_universe "
                "WHERE instrument_id=%s",
                (catalog["active"],),
            ).fetchone()[0]
            is False
        )


def test_db_build_rejects_duplicate_and_conflicting_identity_without_network(
    dsn, catalog, tmp_path, monkeypatch, capsys
):
    with psycopg.connect(dsn) as conn:
        duplicate = uuid.uuid4()
        conn.execute(
            "INSERT INTO public.instruments_universe VALUES "
            "(%s,'fund','FAKEA','US0000000003','USD',true)",
            (duplicate,),
        )
        conn.execute(
            "INSERT INTO public.funds_v VALUES "
            "(%s,'OTHER-SERIES','FAKEA','US0000000003','USD','etf')",
            (duplicate,),
        )
    output, raw, policy = _artifact(dsn, tmp_path, monkeypatch)
    printed = capsys.readouterr().out
    assert "navpolicytest" not in printed and "FAKEA" not in printed
    assert "US0000000001" not in raw.decode()
    by_id = {row["instrument_id"]: row for row in policy["instrument_evidence"]}
    assert by_id[str(catalog["active"])]["fund_status"] == "UNKNOWN"
    assert by_id[str(catalog["inactive"])]["fund_status"] == "INACTIVE"
    assert by_id[str(duplicate)]["valuation_frequency"] == "unknown"
    assert generator.main(["verify", "--policy-file", str(output)]) == 0
    with pytest.raises(
        generator.PolicyGenerationError, match="artifact_already_exists"
    ):
        generator.write_artifact(
            output, raw, force=False, build=True, custody_root=tmp_path
        )


def test_build_cli_requires_force_to_replace_and_verify_needs_no_db(
    dsn, catalog, tmp_path, monkeypatch, capsys
):
    output, raw, first = _artifact(dsn, tmp_path, monkeypatch)
    capsys.readouterr()
    arguments = [
        "build",
        "--dsn-env",
        "NAV_POLICY_TEST_DSN",
        "--custody-root",
        str(tmp_path),
        "--output",
        str(output),
        "--coverage-start",
        START.isoformat(),
        "--coverage-end",
        END.isoformat(),
        "--policy-id",
        "synthetic-xnys",
        "--policy-version",
        "v1",
    ]
    assert generator.main(arguments) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "blocked"
    assert output.read_bytes() == raw
    assert generator.main([*arguments, "--force"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ok"
    second = json.loads(output.read_bytes())
    assert first["generation"]["policy_hash"] == second["generation"]["policy_hash"]
    assert (
        first["generation"]["source_snapshot_sha256"]
        == second["generation"]["source_snapshot_sha256"]
    )
    monkeypatch.delenv("NAV_POLICY_TEST_DSN")
    assert generator.main(["verify", "--policy-file", str(output)]) == 0
    capsys.readouterr()
    with pytest.raises(
        generator.PolicyGenerationError, match="artifact_inside_git_checkout"
    ):
        generator.write_artifact(
            generator.ROOT / "tests" / "_unwritten_policy_artifact.json",
            raw,
            force=False,
            build=True,
            custody_root=tmp_path,
        )


def test_generated_policy_operator_apply_and_readiness_on_local_pg(
    dsn, catalog, tmp_path, monkeypatch, capsys
):
    output, raw, policy = _artifact(dsn, tmp_path, monkeypatch)
    capsys.readouterr()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(NAV_SQL)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS public.funds_profile_mv "
            "(instrument_id uuid PRIMARY KEY)"
        )
        conn.execute(
            "INSERT INTO public.funds_profile_mv VALUES (%s) ON CONFLICT DO NOTHING",
            (catalog["active"],),
        )
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", dsn)
    sql_sha = hashlib.sha256(SCHEMA_SQL).hexdigest()
    base = [
        "--schema",
        "public",
        "--expected-sql-sha256",
        sql_sha,
        "--policy-file",
        str(output),
    ]
    assert operator.main(base) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "upgrade_required"
    plan = operator.plan_hash(SCHEMA_SQL, "public", raw, [], None, None)
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "applied"
    with psycopg.connect(dsn) as conn:
        approved = conn.execute(
            "SELECT policy_hash,published_at,calendar_id,calendar_version "
            "FROM nav_policy_versions WHERE policy_id='synthetic-xnys'"
        ).fetchone()
        assert approved[0] == policy["generation"]["policy_hash"]
        assert (approved[2], approved[3]) == (
            policy["calendar_id"],
            policy["calendar_version"],
        )
        assert (
            conn.execute("SELECT count(*) FROM nav_valuation_schedules").fetchone()[0]
            == (policy["calendar_session_count"])
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM nav_instrument_policy_evidence"
            ).fetchone()[0]
            == 2
        )
    assert operator.main(base) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready"
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == 0
    capsys.readouterr()
    with psycopg.connect(dsn) as conn:
        assert (
            conn.execute(
                "SELECT published_at FROM nav_policy_versions "
                "WHERE policy_id='synthetic-xnys'"
            ).fetchone()[0]
            == approved[1]
        )
        assert (
            conn.execute("SELECT count(*) FROM nav_valuation_schedules").fetchone()[0]
            == (policy["calendar_session_count"])
        )
        now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        pinned, grid, closed = readiness._policy_and_grid(conn, now)
        assert (
            len(grid) == 401 and pinned["calendar_digest"] == policy["calendar_digest"]
        )
        # XNYS has a real close before the 18:05 ET due on this date.
        not_due = dt.datetime(2026, 9, 23, 21, 0, tzinfo=dt.timezone.utc)
        _, previous_grid, extra_closed = readiness._policy_and_grid(conn, not_due)
        assert previous_grid[-1] == dt.date(2026, 9, 22)
        assert extra_closed == dt.date(2026, 9, 23)

    early_observations = tuple(
        NavObservation(day, round(100.0 + index * 0.01, 6), "adjusted")
        for index, day in enumerate(previous_grid)
    )
    early_rows = ingestion.build_rows(
        early_observations,
        [(catalog["active"], "USD")],
        calendar={
            day: (
                policy["calendar_id"],
                policy["calendar_version"],
                policy["calendar_source"],
            )
            for day in previous_grid
        },
    )
    early = readiness.assess_instrument(
        catalog["active"],
        pinned,
        previous_grid,
        early_rows,
        extra_closed,
        extra_closed,
        {
            "evidence_id": uuid.uuid4(),
            "fund_status": "ACTIVE",
            "valuation_frequency": "daily",
            "identity_verified": True,
            "return_basis_verified": True,
            "currency_verified": True,
            "known_at": not_due,
            "effective_at": not_due,
        },
        {"run_id": uuid.uuid4(), "status": "success_new"},
        {
            "calc_date": previous_grid[-1],
            "feature_as_of": previous_grid[-1],
            "input_max_date": previous_grid[-1],
            "exclusion_reason": None,
            "risk_run_id": uuid.uuid4(),
            "input_fingerprint": "f" * 64,
        },
        True,
    )
    assert early["admissible"] is True and early["is_current"] is True

    run_id, risk_run = uuid.uuid4(), uuid.uuid4()
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "INSERT INTO nav_ingestion_runs "
            "(run_id,started_at,completed_at,requested_end,status) "
            "VALUES (%s,clock_timestamp(),clock_timestamp(),%s,'completed')",
            (run_id, grid[-1]),
        )
        conn.execute(
            "INSERT INTO nav_ingestion_attempts "
            "(run_id,instrument_id,ticker,provider,requested_start,requested_end,"
            "attempted_at,finished_at,status,newest_observed_date,row_count) "
            "VALUES (%s,%s,'FAKEA','tiingo',%s,%s,clock_timestamp(),clock_timestamp(),"
            "'success_new',%s,401)",
            (run_id, catalog["active"], grid[0], grid[-1], grid[-1]),
        )
        conn.commit()
        rows = ingestion.build_rows(
            tuple(
                NavObservation(day, round(100.0 + index * 0.01, 6), "adjusted")
                for index, day in enumerate(grid)
            ),
            [(catalog["active"], "USD")],
            calendar={
                day: (
                    policy["calendar_id"],
                    policy["calendar_version"],
                    policy["calendar_source"],
                )
                for day in grid
            },
        )
        ingestion.upsert_nav_timeseries(conn, rows, run_id=run_id)
        conn.execute(
            "INSERT INTO fund_nav_risk_runs "
            "(risk_run_id,calc_date,status,expected_rows,persisted_rows,completed_at) "
            "VALUES (%s,%s,'complete',1,1,clock_timestamp())",
            (risk_run, grid[-1]),
        )
        nav_rows = conn.execute(
            "SELECT nav_date,nav FROM nav_timeseries "
            "WHERE instrument_id=%s ORDER BY nav_date",
            (catalog["active"],),
        ).fetchall()
        risk._persist_feature_evidence(
            conn,
            catalog["active"],
            grid[-1],
            nav_rows,
            0.04,
            risk_run,
            None,
            {},
            {},
            [],
        )
        conn.execute(
            "INSERT INTO fund_nav_risk_publication "
            "(readiness_profile,revision_id,state,published_risk_run_id) "
            "VALUES ('current_daily_nav_v1',1,'idle',%s)",
            (risk_run,),
        )
        conn.commit()
    outcome = readiness.run(dsn)
    assert outcome["ready_count"] == 1
    with psycopg.connect(dsn) as conn:
        assert conn.execute(
            "SELECT ready,snapshot_current,latest_closed_session "
            "FROM fund_nav_readiness_current_v1 WHERE instrument_id=%s",
            (catalog["active"],),
        ).fetchone() == (True, True, closed)
        assert (
            conn.execute(
                "SELECT count(*) FROM fund_nav_readiness_current_v1 "
                "WHERE reason_code='INACTIVE_FUND'"
            ).fetchone()[0]
            == 0
        )
