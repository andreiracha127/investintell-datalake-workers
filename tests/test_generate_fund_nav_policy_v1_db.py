"""Real disposable PG18/Timescale tests for the read-only current-catalog snapshot."""

from __future__ import annotations

import contextlib
import copy
import datetime as dt
import hashlib
import json
import os
import stat
import time
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from scripts import fund_nav_readiness_schema as operator
from scripts import generate_fund_nav_policy_v1 as generator
from scripts import verify_fund_nav_identity_v2 as verifier
from src.workers import fund_nav_readiness as readiness
from src.workers import instrument_ingestion as ingestion
from src.workers import risk_metrics as risk
from src.workers._nav_policy import (
    FEATURE_DEFINITION_VERSION,
    canonical_digest,
    load_calendar_equivalence,
    risk_universe_digest,
)
from src.workers._tiingo import NavObservation
from tests._nav_identity_fixtures import entity, synthetic_figi

ROOT = Path(__file__).parents[1]
NAV_SQL = (ROOT / "schemas" / "instrument_ingestion.sql").read_text(encoding="utf-8")
SCHEMA_SQL = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
_RISK_SQL = (ROOT / "schemas" / "risk_metrics.sql").read_text(encoding="utf-8")
RISK_READ_MODEL_SQL = _RISK_SQL[
    _RISK_SQL.index("CREATE TABLE IF NOT EXISTS fund_risk_metrics") : _RISK_SQL.index(
        "CREATE MATERIALIZED VIEW funds_list_mv"
    )
]
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


CATALOG_DDL = (
    # A database reused from the v1 suite has funds_v as a plain table.
    """DO $$ BEGIN
         IF (SELECT relkind FROM pg_class WHERE oid = to_regclass('public.funds_v')) = 'r'
         THEN DROP TABLE public.funds_v; END IF;
       END $$""",
    """CREATE TABLE IF NOT EXISTS public.instruments_universe
       (instrument_id uuid PRIMARY KEY,instrument_type text,ticker text,
        isin text,currency text,is_active boolean)""",
    # funds_v is a VIEW over a base table, as in production: SELECT on the
    # view never implies SELECT on the registry base table.
    """CREATE TABLE IF NOT EXISTS public.nav_fixture_funds
       (instrument_id uuid,series_id text,ticker text,isin text,cusip text,
        currency text,fund_type text)""",
    """CREATE OR REPLACE VIEW public.funds_v AS
       SELECT instrument_id,series_id,ticker,isin,cusip,currency,fund_type
       FROM public.nav_fixture_funds""",
    """CREATE TABLE IF NOT EXISTS public.instrument_identity
       (instrument_id uuid,sec_series_id text,sec_class_id text,ticker text,
        isin text,cusip_9 text,figi text,resolution_status text,conflict_state jsonb)""",
)


def _seed(conn, *entities) -> None:
    for iu, fund, registry in entities:
        if iu is not None:
            conn.execute(
                "INSERT INTO public.instruments_universe VALUES (%s,%s,%s,%s,%s,%s)",
                tuple(iu[k] for k in generator.SOURCE_FIELDS["instruments"]),
            )
        if fund is not None:
            conn.execute(
                "INSERT INTO public.nav_fixture_funds VALUES (%s,%s,%s,%s,%s,%s,%s)",
                tuple(fund[k] for k in generator.SOURCE_FIELDS["funds"]),
            )
        if registry is not None:
            values = [registry[k] for k in generator.SOURCE_FIELDS["identity"]]
            values[-1] = None if values[-1] is None else Jsonb(values[-1])
            conn.execute(
                "INSERT INTO public.instrument_identity VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                values,
            )


def _uuid_entity(identifier: uuid.UUID, number: int, **kwargs):
    rows = entity(number, **kwargs)
    for row in rows:
        row["instrument_id"] = identifier
    return rows


@pytest.fixture
def catalog(dsn):
    with psycopg.connect(dsn, autocommit=True) as conn:
        for statement in CATALOG_DDL:
            conn.execute(statement)
        conn.execute(
            "TRUNCATE TABLE public.instruments_universe, public.nav_fixture_funds, "
            "public.instrument_identity"
        )
        active, inactive = uuid.uuid4(), uuid.uuid4()
        active_rows = _uuid_entity(active, 1, ticker="FAKEA", series="S900000001", figi=synthetic_figi(1))
        inactive_rows = _uuid_entity(inactive, 2, ticker="FAKEB", active=False)
        _seed(conn, active_rows, (inactive_rows[0], None, None))
    return {"active": active, "inactive": inactive}


def _build_args(dsn_env, root, output, *extra, version="2026-09-24.2", end=END):
    return [
        "build",
        "--dsn-env",
        dsn_env,
        "--custody-root",
        str(root),
        "--output",
        str(output),
        "--coverage-start",
        START.isoformat(),
        "--coverage-end",
        end.isoformat(),
        "--policy-id",
        "synthetic-xnys",
        "--policy-version",
        version,
        *extra,
    ]


def _artifact(dsn, tmp_path, monkeypatch, *extra, end=END, name="approved-policy.json"):
    output = tmp_path / name
    monkeypatch.setenv("NAV_POLICY_TEST_DSN", dsn)
    assert (
        generator.main(_build_args("NAV_POLICY_TEST_DSN", tmp_path, output, *extra, end=end))
        == 0
    )
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
            writer.execute(
                "UPDATE public.instrument_identity SET resolution_status='candidate' "
                "WHERE instrument_id=%s",
                (catalog["active"],),
            )
        return original(cursor)

    monkeypatch.setattr(generator, "_catalog_rows", concurrent_update)
    instant, instruments, funds, identity = generator.read_catalog_snapshot(dsn)
    assert details[0]["read_only"] == "on" and details[0]["xid"] is None
    assert instant.tzinfo is not None
    assert (
        next(row for row in instruments if row["instrument_id"] == catalog["active"])[
            "is_active"
        ]
        is True
    )
    assert [row["resolution_status"] for row in identity] == ["canonical"]
    assert identity[0]["conflict_state"] == {}  # jsonb object, not the string '{}'
    assert len(funds) == 1
    with psycopg.connect(dsn) as conn:
        assert conn.execute(
            "SELECT i.is_active, r.resolution_status FROM public.instruments_universe i "
            "JOIN public.instrument_identity r USING (instrument_id) WHERE instrument_id=%s",
            (catalog["active"],),
        ).fetchone() == (False, "candidate")


def test_db_build_rejects_duplicate_and_conflicting_identity_without_network(
    dsn, catalog, tmp_path, monkeypatch, capsys
):
    with psycopg.connect(dsn) as conn:
        duplicate = uuid.uuid4()
        _seed(conn, _uuid_entity(duplicate, 3, ticker="FAKEA", series="OTHER-SERIES"))
    output, raw, policy = _artifact(dsn, tmp_path, monkeypatch)
    printed = capsys.readouterr().out
    assert "navpolicytest" not in printed and "FAKEA" not in printed
    assert "S900000001" not in raw.decode() and "FAKEA" not in raw.decode()
    by_id = {row["instrument_id"]: row for row in policy["instrument_evidence"]}
    assert by_id[str(catalog["active"])]["fund_status"] == "UNKNOWN"
    assert by_id[str(catalog["inactive"])]["fund_status"] == "INACTIVE"
    assert by_id[str(duplicate)]["valuation_frequency"] == "unknown"
    assert policy["generation"]["counts"]["identity_first_failure"] == {"ticker.global_conflict": 2}
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
    arguments = _build_args("NAV_POLICY_TEST_DSN", tmp_path, output)
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
    # Bootstrap publication into an empty schema: only the receipt FILE
    # validation is replaced (governed receipts are tested end to end below);
    # plan-v4, locks, freshness and pointer checks run for real.
    from tests.test_fund_nav_readiness_db import _install_receipt_seam

    _install_receipt_seam(monkeypatch)
    sql_sha = hashlib.sha256(SCHEMA_SQL).hexdigest()
    base = [
        "--schema",
        "public",
        "--expected-sql-sha256",
        sql_sha,
        "--policy-file",
        str(output),
    ]
    # Light runtime prerequisites that W1 never grants (role, USAGE, external
    # read dependencies); without them the operator is truthfully blocked.
    with psycopg.connect(dsn, autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname='app_runtime'").fetchone():
            pytest.fail("fixture role app_runtime is required (created by the harness)")
        conn.execute("GRANT USAGE ON SCHEMA public TO app_runtime")
        if conn.execute("SELECT to_regclass('public.fund_risk_latest_mv') IS NULL").fetchone()[0]:
            conn.execute(RISK_READ_MODEL_SQL)
        conn.execute("GRANT SELECT ON nav_timeseries, fund_risk_latest_mv TO app_runtime")
    assert operator.main(base) == 0
    planned = json.loads(capsys.readouterr().out)
    assert (planned["status"], planned["compatibility"]) == ("planned", "absent")
    assert planned["plan"]["operations"] == ["ddl", "policy"]
    plan = planned["plan_sha256"]
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert (applied["status"], applied["ddl"], applied["policy"], applied["published"]) == (
        "applied", "applied", "committed", True,
    )
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
    # Replaying the committed plan is a truthful no-op (no re-stamp).
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == 0
    replay = json.loads(capsys.readouterr().out)
    assert (replay["status"], replay["ddl"], replay["dml_committed"]) == (
        "unchanged", "unchanged", False,
    )
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
        # A real XNYS session closes before its 18:05 ET due. The instant must
        # follow the policy publication (a fixed past instant is a time bomb), so
        # use the next session after the DB clock, between its close and due.
        session, close_at, due_at, prior = conn.execute(
            """SELECT s.session_date, s.valuation_close_at, s.nav_due_at,
                      (SELECT max(p.session_date) FROM nav_valuation_schedules p
                       WHERE p.calendar_id=s.calendar_id
                         AND p.calendar_version=s.calendar_version
                         AND p.session_date < s.session_date)
               FROM nav_valuation_schedules s
               WHERE s.calendar_id=%s AND s.calendar_version=%s
                 AND s.valuation_close_at > clock_timestamp()
               ORDER BY s.session_date LIMIT 1""",
            (pinned["calendar_id"], pinned["calendar_version"]),
        ).fetchone()
        assert close_at < due_at
        not_due = close_at + (due_at - close_at) / 2
        _, previous_grid, extra_closed = readiness._policy_and_grid(conn, not_due)
        assert previous_grid[-1] == prior
        assert extra_closed == session
        # Real published-session proof for the earlier grid (no literal shortcut).
        early_proof = load_calendar_equivalence(
            conn,
            pinned,
            previous_grid,
            [(pinned["calendar_id"], pinned["calendar_version"], pinned["calendar_source"])],
            not_due,
        )
        assert len(early_proof) == 401

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
        equivalence=early_proof,
    )
    assert early["admissible"] is True and early["is_current"] is True
    assert len(early["calendar_equivalence_digest"]) == 64

    run_id, risk_run = uuid.uuid4(), uuid.uuid4()
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "INSERT INTO nav_ingestion_runs (run_id,requested_end,status) "
            "VALUES (%s,%s,'running')",
            (run_id, grid[-1]),
        )
        conn.commit()
        # Attempt and NAV commit together (same xid), then the run completes.
        conn.execute(
            "INSERT INTO nav_ingestion_attempts "
            "(run_id,instrument_id,ticker,provider,requested_start,requested_end,"
            "attempted_at,finished_at,status,newest_observed_date,row_count) "
            "VALUES (%s,%s,'FAKEA','tiingo',%s,%s,clock_timestamp(),clock_timestamp(),"
            "'success_new',%s,401)",
            (run_id, catalog["active"], grid[0], grid[-1], grid[-1]),
        )
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
        ingestion.upsert_nav_timeseries(conn, rows, run_id=run_id, provider="tiingo")
        conn.execute(
            "UPDATE nav_ingestion_runs SET status='completed' WHERE run_id=%s", (run_id,)
        )
        conn.commit()
        # Explicit P1 fixture registration (no W3 worker classification).
        conn.execute(
            "INSERT INTO fund_nav_risk_runs (risk_run_id,calc_date,run_scope,"
            "policy_id,policy_version,policy_hash,due_session,universe_digest,"
            "feature_definition_version,status,expected_rows) "
            "VALUES (%s,%s,'current_full',%s,%s,%s,%s,%s,%s,'running',1)",
            (risk_run, grid[-1], pinned["policy_id"], pinned["policy_version"],
             pinned["policy_hash"], grid[-1],
             risk_universe_digest([catalog["active"]]), FEATURE_DEFINITION_VERSION),
        )
        conn.execute(
            "INSERT INTO fund_nav_risk_run_members VALUES (%s,%s)",
            (risk_run, catalog["active"]),
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
        conn.commit()
        risk._finish_risk_run(conn, risk_run, 1, 1)
        conn.execute(
            "INSERT INTO fund_nav_risk_publication "
            "(readiness_profile,revision_id,state,published_risk_run_id) "
            "VALUES ('current_daily_nav_v1',1,'idle',%s)",
            (risk_run,),
        )
        conn.execute(
            "UPDATE fund_nav_risk_runs SET status='complete',"
            "completed_at=clock_timestamp() WHERE risk_run_id=%s",
            (risk_run,),
        )
        assert conn.execute(
            "SELECT run_scope FROM fund_nav_risk_runs WHERE risk_run_id=%s",
            (risk_run,),
        ).fetchone() == ("current_full",)
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


# ── identity v2: privileges, limits, projection and cardinality in PG ────────
def _role_dsn(dsn: str, role: str) -> str:
    params = psycopg.conninfo.conninfo_to_dict(dsn)
    params["user"] = role
    params.pop("password", None)
    return psycopg.conninfo.make_conninfo(**params)


def test_view_access_does_not_imply_registry_privilege(dsn, catalog, tmp_path, monkeypatch, capsys):
    role = "nav_policy_reader_" + uuid.uuid4().hex[:12]
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
        conn.execute(
            sql.SQL("GRANT SELECT ON public.instruments_universe, public.funds_v TO {}").format(
                sql.Identifier(role)
            )
        )
    try:
        reader = _role_dsn(dsn, role)
        with psycopg.connect(reader) as conn:  # the view itself is readable
            assert conn.execute("SELECT count(*) FROM public.funds_v").fetchone()[0] == 1
        with pytest.raises(generator.PolicyGenerationError, match="catalog_source_privilege_missing"):
            generator.read_catalog_snapshot(reader)
        monkeypatch.setenv("NAV_POLICY_READER_DSN", reader)
        output = tmp_path / "no-privilege.json"
        assert generator.main(_build_args("NAV_POLICY_READER_DSN", tmp_path, output)) == 2
        blocked = json.loads(capsys.readouterr().out)
        assert blocked["code"] == "catalog_source_privilege_missing" and not output.exists()
        assert role not in json.dumps(blocked)
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("REVOKE ALL ON public.instruments_universe, public.funds_v FROM {}").format(sql.Identifier(role)))
            conn.execute(sql.SQL("REVOKE USAGE ON SCHEMA public FROM {}").format(sql.Identifier(role)))
            conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_missing_registry_relation_blocks_with_sqlstate_only(dsn, catalog, tmp_path, monkeypatch, capsys):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE public.instrument_identity")
    monkeypatch.setenv("NAV_POLICY_TEST_DSN", dsn)
    output = tmp_path / "missing.json"
    assert generator.main(_build_args("NAV_POLICY_TEST_DSN", tmp_path, output)) == 2
    blocked = json.loads(capsys.readouterr().out)
    assert (blocked["reason"], blocked["sqlstate"]) == ("UndefinedTable", "42P01")
    assert not output.exists()


def test_registry_row_sentinel_aborts_before_classification(dsn, catalog):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO public.instrument_identity (instrument_id, resolution_status, conflict_state) "
            "SELECT gen_random_uuid(), 'candidate', '{}'::jsonb FROM generate_series(1, 100000)"
        )
    with pytest.raises(generator.PolicyGenerationError, match="catalog_row_limit_exceeded"):
        generator.read_catalog_snapshot(dsn)


def test_database_projection_divergence_aborts_entire_build(dsn, catalog, tmp_path, monkeypatch, capsys):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE public.nav_fixture_funds SET cusip=NULL WHERE instrument_id=%s",
            (catalog["active"],),
        )
    monkeypatch.setenv("NAV_POLICY_TEST_DSN", dsn)
    output = tmp_path / "diverged.json"
    assert generator.main(_build_args("NAV_POLICY_TEST_DSN", tmp_path, output)) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "catalog_identity_projection_mismatch"
    assert not output.exists()


def test_duplicate_registry_row_and_non_object_conflict_state(dsn, catalog, tmp_path, monkeypatch):
    with psycopg.connect(dsn, autocommit=True) as conn:
        second, third = uuid.uuid4(), uuid.uuid4()
        rows = _uuid_entity(second, 5)
        _seed(conn, rows, (None, None, rows[2]))
        stringly = _uuid_entity(third, 6)
        _seed(conn, (stringly[0], stringly[1], None))
        conn.execute(
            "INSERT INTO public.instrument_identity VALUES (%s,%s,NULL,%s,%s,%s,NULL,'canonical',to_jsonb('{}'::text))",
            (third, stringly[2]["sec_series_id"], stringly[2]["ticker"], stringly[2]["isin"], stringly[2]["cusip_9"]),
        )
    _, _, policy = _artifact(dsn, tmp_path, monkeypatch)
    status = {row["instrument_id"]: row["fund_status"] for row in policy["instrument_evidence"]}
    assert status[str(second)] == status[str(third)] == "UNKNOWN"
    assert status[str(catalog["active"])] == "ACTIVE"
    assert policy["generation"]["counts"]["identity_first_failure"] == {
        "cardinality.registry_duplicate": 1,
        "registry.conflict_state_not_empty": 1,
    }


def _install_w1(dsn: str, schema: str) -> None:
    with psycopg.connect(dsn, autocommit=True, options=f"-csearch_path={schema},public") as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        conn.execute(NAV_SQL)
        operator.apply_ddl(conn, schema, SCHEMA_SQL)


def _hand_authored_previous(policy: dict) -> dict:
    """The pre-v2 current pointer: same calendar, no generator metadata."""
    previous = copy.deepcopy(policy)
    for key in ("generation", "generator_version", "provider_contract"):
        previous.pop(key)
    previous["policy_version"] = "2026-09-23.1"
    for row in previous["instrument_evidence"]:
        row["evidence_reference"] = "fixture-previous-identity"
    return previous


def test_v2_publication_moves_pointer_reuses_calendar_and_v1_stays_immutable(
    dsn, catalog, tmp_path, monkeypatch, capsys
):
    output, raw, policy = _artifact(dsn, tmp_path, monkeypatch)
    schema = "nav_policy_v2_" + uuid.uuid4().hex[:12]
    _install_w1(dsn, schema)
    try:
        previous = _hand_authored_previous(policy)
        with psycopg.connect(dsn, options=f"-csearch_path={schema},public") as conn:
            first_hash, changed = operator._publish_policy(conn, operator._policy(previous)[0])
            conn.commit()
            assert changed is True
            before = conn.execute(
                "SELECT to_jsonb(v) FROM nav_policy_versions v WHERE policy_version='2026-09-23.1'"
            ).fetchone()[0]
            sessions = conn.execute("SELECT count(*) FROM nav_valuation_schedules").fetchone()[0]
            v2_hash, changed = operator._publish_policy(conn, operator._policy(str(output))[0])
            conn.commit()
            assert changed is True and v2_hash == policy["generation"]["policy_hash"] != first_hash
            assert conn.execute(
                "SELECT c.policy_id, c.policy_version, v.policy_hash FROM nav_policy_current c "
                "JOIN nav_policy_versions v USING (policy_id, policy_version)"
            ).fetchone() == ("synthetic-xnys", "2026-09-24.2", v2_hash)
            assert conn.execute(
                "SELECT to_jsonb(v) FROM nav_policy_versions v WHERE policy_version='2026-09-23.1'"
            ).fetchone()[0] == before
            assert conn.execute("SELECT count(*) FROM nav_valuation_schedules").fetchone()[0] == sessions
            assert conn.execute(
                "SELECT count(DISTINCT (calendar_id, calendar_version)) FROM nav_policy_versions"
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT policy_version, count(*) FROM nav_instrument_policy_evidence "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall() == [("2026-09-23.1", 2), ("2026-09-24.2", 2)]
            state = conn.execute(
                "SELECT (SELECT count(*) FROM nav_policy_versions),"
                "(SELECT count(*) FROM nav_instrument_policy_evidence),"
                "(SELECT to_jsonb(c) FROM nav_policy_current c)"
            ).fetchone()
        # A generator-v1 artifact is refused by v2 code before any database work.
        v1 = copy.deepcopy(policy)
        v1["generator_version"] = v1["generation"]["generator_version"] = "fund-nav-policy-generator-v1"
        v1["generation"]["source_query_version"] = "nav-current-catalog-snapshot-v1"
        v1["policy_version"] = "2026-09-23.1"
        v1_path = tmp_path / "v1.json"
        v1_path.write_bytes(generator.canonical_json(v1))
        monkeypatch.setenv("NAV_READINESS_DATABASE_URL", dsn)
        capsys.readouterr()
        result = operator.main(
            [
                "--schema", schema,
                "--expected-sql-sha256", hashlib.sha256(SCHEMA_SQL).hexdigest(),
                "--policy-file", str(v1_path),
                "--mode", "apply", "--plan-sha256", "0" * 64,
            ]
        )
        emitted = json.loads(capsys.readouterr().out)
        # The CLI refuses any policy without the governed receipt; the parser
        # refuses v1 metadata on its own, independently of the receipt.
        assert result != 0 and emitted["code"] == "audit_receipt_required"
        assert emitted["dml_committed"] is False and emitted["published"] is False
        with pytest.raises(ValueError, match="generator_metadata_invalid"):
            operator._policy(str(v1_path))
        with pytest.raises((generator.PolicyGenerationError, ValueError)):
            generator.verify_artifact(v1)
        with psycopg.connect(dsn, options=f"-csearch_path={schema},public") as conn:
            assert conn.execute(
                "SELECT (SELECT count(*) FROM nav_policy_versions),"
                "(SELECT count(*) FROM nav_instrument_policy_evidence),"
                "(SELECT to_jsonb(c) FROM nav_policy_current c)"
            ).fetchone() == state
            immutable = conn.execute(
                "SELECT (SELECT jsonb_agg(to_jsonb(v) ORDER BY policy_version) FROM nav_policy_versions v),"
                "(SELECT md5(string_agg(to_jsonb(e)::text, '' ORDER BY evidence_id)) "
                " FROM nav_instrument_policy_evidence e),"
                "(SELECT md5(string_agg(to_jsonb(s)::text, '' ORDER BY session_date)) "
                " FROM nav_valuation_schedules s)"
            ).fetchone()
            conn.rollback()
            # Pointer-only rollback: same advisory locks as the operator, CAS on
            # the expected .2 pointer and hash-pinned .1 target; no row of any
            # version/evidence/calendar table is inserted, updated or deleted.
            rollback_sql = (
                "UPDATE nav_policy_current c SET policy_id=%(pid)s, policy_version=%(target)s "
                "FROM nav_policy_versions t, nav_policy_versions cur "
                "WHERE c.readiness_profile='current_daily_nav_v1' "
                "AND c.policy_id=%(pid)s AND c.policy_version=%(expected)s "
                "AND cur.policy_id=c.policy_id AND cur.policy_version=c.policy_version "
                "AND cur.policy_hash=%(expected_hash)s "
                "AND t.policy_id=%(pid)s AND t.policy_version=%(target)s "
                "AND t.policy_hash=%(target_hash)s AND t.published_at IS NOT NULL "
                "AND t.calendar_id=cur.calendar_id AND t.calendar_version=cur.calendar_version"
            )
            params = {
                "pid": "synthetic-xnys",
                "expected": "2026-09-24.2",
                "expected_hash": v2_hash,
                "target": "2026-09-23.1",
                "target_hash": first_hash,
            }
            for key in (operator.LOCK_INSTRUMENT_INGESTION, operator.LOCK_FUND_NAV_READINESS):
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (key,))
            conn.execute(
                "SELECT 1 FROM nav_policy_current WHERE readiness_profile='current_daily_nav_v1' FOR UPDATE"
            )
            stamp_before = conn.execute("SELECT published_at FROM nav_policy_current").fetchone()[0]
            assert conn.execute(rollback_sql, params).rowcount == 1
            conn.commit()
            assert conn.execute(
                "SELECT policy_version, published_at > %s FROM nav_policy_current", (stamp_before,)
            ).fetchone() == ("2026-09-23.1", True)
            # Idempotent replay: the CAS no longer matches, nothing changes.
            assert conn.execute(rollback_sql, params).rowcount == 0
            conn.commit()
            assert conn.execute(
                "SELECT (SELECT jsonb_agg(to_jsonb(v) ORDER BY policy_version) FROM nav_policy_versions v),"
                "(SELECT md5(string_agg(to_jsonb(e)::text, '' ORDER BY evidence_id)) "
                " FROM nav_instrument_policy_evidence e),"
                "(SELECT md5(string_agg(to_jsonb(s)::text, '' ORDER BY session_date)) "
                " FROM nav_valuation_schedules s)"
            ).fetchone() == immutable
            # A stale expectation (wrong hash) never moves the pointer.
            assert conn.execute(
                rollback_sql, {**params, "expected": "2026-09-23.1", "target": "2026-09-24.2",
                               "expected_hash": "0" * 64, "target_hash": v2_hash}
            ).rowcount == 0
            conn.rollback()
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


SEC_TABLE_SQL = (ROOT / "schemas" / "sec_company_tickers_mf.sql").read_text(encoding="utf-8")
SEC_CONFIG = {
    "source_contract": "sec-company-tickers-mf-class-map-v1",
    "relation": "public.sec_company_tickers_mf",
    "timestamp_column": "updated_at",
    "query_contract_sha256": verifier.SEC_QUERY_CONTRACT_SHA256,
    "max_synced_age_days": 7,
}


def _audit_config(query: str, **builder) -> dict:
    return {
        "audit_config_version": verifier.AUDIT_CONFIG_VERSION,
        "audit_contract_version": verifier.AUDIT_CONTRACT_VERSION,
        "structural_daily_ceiling": 5103,
        "canary_salt": "db-canary",
        "sec": dict(SEC_CONFIG),
        "builder": {
            "light_revision": "b" * 40,
            "cohort_query": query,
            "cohort_parameters": {},
            "label_to_sleeve": {"Large Blend": "equity", "Government Bond": "fixed_income"},
            "stage1_quotas": {"equity": 1},
            "margin_fraction": "1/10",
            **builder,
        },
    }


COHORT_QUERY = (
    "SELECT instrument_id, strategy_label FROM public.nav_fixture_cohort ORDER BY instrument_id"
)


def _put(root: Path, name: str, data: bytes) -> Path:
    """Private custody file (0600, new inode, single link)."""
    path = root / name
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sec_insert(conn, rows, *, age: str = "0 seconds") -> None:
    for class_id, series_id, ticker in rows:
        conn.execute(
            "INSERT INTO public.sec_company_tickers_mf "
            "(class_id, cik, series_id, ticker, updated_at) "
            "VALUES (%s, '0000000001', %s, %s, clock_timestamp() - %s::interval)",
            (class_id, series_id, ticker, age),
        )


@pytest.fixture
def live_audit(dsn, catalog, tmp_path, monkeypatch, capsys, request):
    """Catalog + cohort + SEC class map + policy/snapshot + hash-pinned previous.

    Indirect ``{"coverage_end": date}`` builds an already-expired policy.
    """
    coverage_end = getattr(request, "param", {}).get("coverage_end", END)
    custody = tmp_path / "custody"
    custody.mkdir(mode=0o700)
    with psycopg.connect(dsn, autocommit=True) as conn:
        extra = [_uuid_entity(uuid.uuid4(), n, fund_type=kind) for n, kind in ((11, "mutual_fund"), (12, "etf"))]
        for rows in extra:
            _seed(conn, rows)
        conn.execute(
            "DROP TABLE IF EXISTS public.nav_fixture_cohort, public.sec_company_tickers_mf, "
            "public.nav_fixture_sink"
        )
        conn.execute("DROP SEQUENCE IF EXISTS public.nav_audit_seq")
        conn.execute("CREATE TABLE public.nav_fixture_cohort (instrument_id uuid, strategy_label text)")
        conn.execute(SEC_TABLE_SQL)
        members = [catalog["active"], catalog["inactive"], *(rows[0]["instrument_id"] for rows in extra)]
        for identifier, label in zip(members, ("Large Blend", "Large Blend", "Government Bond", "Large Blend")):
            conn.execute("INSERT INTO public.nav_fixture_cohort VALUES (%s,%s)", (identifier, label))
        # Every ACTIVE (FAKEA etf, #11 mutual fund, #12 etf) has one fresh mapping.
        _sec_insert(
            conn,
            [
                ("C900000001", "S900000001", "FAKEA"),
                ("C000000011", "S000000011", "T11"),
                ("C000000012", "S000000012", "T12"),
            ],
        )
    output, raw, policy = _artifact(
        dsn, custody, monkeypatch, "--source-snapshot-output", str(custody / "source.json"),
        end=coverage_end,
    )
    capsys.readouterr()
    previous_doc = _hand_authored_previous(policy)
    previous = json.dumps(previous_doc, sort_keys=True).encode()
    _put(custody, "v1.json", previous)
    config_path = _put(custody, "audit-config.json", json.dumps(_audit_config(COHORT_QUERY)).encode())
    monkeypatch.setenv("NAV_AUDIT_DSN", dsn)
    counter = iter(range(1000))

    def run(
        config: dict | None, *extra: str, dsn_env: str = "NAV_AUDIT_DSN",
        policy_path: Path | None = None, snapshot_path: Path | None = None,
    ) -> tuple[int, dict, dict, Path]:
        n = next(counter)
        path = config_path if config is None else _put(custody, f"config-{n}.json", json.dumps(config).encode())
        capture_path = custody / f"capture-{n}.json"
        code = verifier.main(
            [
                "--policy-file", str(policy_path or output),
                "--source-snapshot-file", str(snapshot_path or custody / "source.json"),
                "--dsn-env", dsn_env,
                "--capture-output", str(capture_path),
                "--audit-config", str(path),
                "--previous-policy-file", str(custody / "v1.json"),
                "--previous-policy-sha256", hashlib.sha256(previous).hexdigest(),
                "--custody-root", str(custody),
                "--output", str(custody / f"audit-{n}.json"),
                "--strict",
                *extra,
            ]
        )
        printed = capsys.readouterr().out
        assert "postgresql" not in printed and str(catalog["active"]) not in printed
        dossier_path = custody / f"audit-{n}.json"
        dossier = json.loads(dossier_path.read_bytes()) if dossier_path.exists() else {}
        return code, json.loads(printed), dossier, capture_path

    return {
        "run": run,
        "custody": custody,
        "policy": policy,
        "policy_path": output,
        "previous_doc": previous_doc,
        "previous": previous,
        "config_path": config_path,
        "members": members,
        "extra": extra,
    }


def test_live_readonly_audit_strict_pass_and_canary(dsn, catalog, live_audit):
    custody = live_audit["custody"]
    code, summary, dossier, capture_path = live_audit["run"](None, "--canary-output", str(custody / "canary.json"))
    assert code == 0 and set(summary["gates"].values()) == {"PASS"}, (summary["gates"], dossier["gates"])
    assert summary["sec_outcomes"]["matched"] == 3
    assert sum(summary["sec_outcomes"].values()) == 3
    assert summary["canary"]["status"] == "written" and summary["canary"]["size"] == 3
    capture_raw = capture_path.read_bytes()
    for path in (capture_path, custody / "canary.json"):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    bundle = json.loads(capture_raw)
    assert bundle["cohort"]["row_count"] == 4 and bundle["sec"]["row_count"] == 3
    assert bundle["sec"]["relation"] == "public.sec_company_tickers_mf"
    assert bundle["source_snapshot_sha256"] == live_audit["policy"]["generation"]["source_snapshot_sha256"]
    assert dossier["inputs"]["capture"]["capture_bundle_sha256"] == hashlib.sha256(capture_raw).hexdigest()
    freshness = dossier["details"]["A8"]["freshness"]
    assert freshness["max_synced_age_days"] == 7 and freshness["valid_until"] is not None
    canary = json.loads((custody / "canary.json").read_bytes())
    assert canary["policy_hash"] == live_audit["policy"]["generation"]["policy_hash"]
    assert canary["capture_bundle_sha256"] == hashlib.sha256(capture_raw).hexdigest()
    assert canary["selection_sha256"] == dossier["details"]["canary_selection_sha256"]
    replay = verifier.audit(
        live_audit["policy_path"].read_bytes(),
        snapshot_raw=(custody / "source.json").read_bytes(),
        capture_raw=capture_raw,
        previous_raw=live_audit["previous"],
        config_raw=live_audit["config_path"].read_bytes(),
    )
    assert replay["gates"] == dossier["gates"]


def test_live_cohort_is_bounded_server_side(dsn, catalog, live_audit, monkeypatch):
    fetched = []
    original = verifier.drain_bounded

    def counting(fetchmany, **kwargs):
        def spy(size):
            chunk = fetchmany(size)
            fetched.append(len(chunk))
            return chunk

        return original(spy, **kwargs)

    monkeypatch.setattr(verifier, "drain_bounded", counting)
    huge = _audit_config(
        "SELECT gen_random_uuid() AS instrument_id, 'Large Blend' AS strategy_label "
        "FROM generate_series(1, 250000)"
    )
    code, summary, dossier, capture_path = live_audit["run"](huge)
    assert code == 3 and summary["gates"]["A7"] == "NOT_EVALUATED"
    assert dossier["gates"]["A7"]["code"] == "cohort_row_ceiling_exceeded"
    assert sum(fetched) == verifier.ROW_CEILING + 1
    assert max(fetched) <= verifier.FETCH_BATCH
    assert json.loads(capture_path.read_bytes())["cohort"]["rows"] == []
    own_limit = _audit_config(COHORT_QUERY + " LIMIT 2")
    fetched.clear()
    code, summary, dossier, capture_path = live_audit["run"](own_limit)
    assert json.loads(capture_path.read_bytes())["cohort"]["row_count"] == 2
    assert dossier["details"]["A7"]["cohort_rows"] == 2 and sum(fetched) == 2


def test_live_cohort_write_attempts_are_rejected_or_read_only(dsn, catalog, live_audit):
    custody = live_audit["custody"]
    for query, expected in (
        ("SELECT nextval('public.nav_audit_seq') AS instrument_id, 'x' AS strategy_label", "audit_config_cohort_query_unsafe"),
        ("WITH d AS (DELETE FROM public.nav_fixture_cohort RETURNING *) SELECT * FROM d", "audit_config_builder_invalid"),
    ):
        code, summary, dossier, capture_path = live_audit["run"](_audit_config(query))
        assert code == 2 and summary["code"] == expected
        assert not dossier and not capture_path.exists()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("CREATE TABLE public.nav_fixture_sink (x int)")
        conn.execute(
            "CREATE OR REPLACE FUNCTION public.nav_fixture_write() RETURNS uuid LANGUAGE plpgsql AS "
            "$$ BEGIN INSERT INTO public.nav_fixture_sink VALUES (1); RETURN gen_random_uuid(); END $$"
        )
    code, summary, dossier, capture_path = live_audit["run"](
        _audit_config("SELECT public.nav_fixture_write() AS instrument_id, 'x' AS strategy_label")
    )
    assert code == 3 and summary["gates"]["A7"] == "NOT_EVALUATED"
    assert dossier["gates"]["A7"]["code"] == "cohort_query_failed:25006"
    assert capture_path.exists()
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT count(*) FROM public.nav_fixture_sink").fetchone()[0] == 0
    assert (custody / "approved-policy.json").exists()


def test_live_source_drift_fails_a5_and_skips_a7_a8(dsn, catalog, live_audit):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("UPDATE public.instruments_universe SET currency='EUR' WHERE instrument_id=%s", (catalog["active"],))
    code, summary, dossier, capture_path = live_audit["run"](None)
    assert code == 3 and summary["gates"]["A5"] == "FAIL"
    assert summary["gates"]["A7"] == summary["gates"]["A8"] == "NOT_EVALUATED"
    assert json.loads(capture_path.read_bytes())["source_snapshot_sha256"] == dossier["inputs"]["live_source_snapshot_sha256"]


# ── A8 against the real public.sec_company_tickers_mf DDL ───────────────────
@pytest.mark.parametrize(
    "mutation,status,expected",
    [
        ("UPDATE public.sec_company_tickers_mf SET updated_at = clock_timestamp() - interval '8 days' "
         "WHERE ticker='T11'", "FAIL", {"failed": ["fresh_within_max_age"]}),
        ("UPDATE public.sec_company_tickers_mf SET updated_at = clock_timestamp() + interval '1 hour' "
         "WHERE ticker='T12'", "FAIL", {"failed": ["synced_not_in_future"]}),
        ("INSERT INTO public.sec_company_tickers_mf (class_id,cik,series_id,ticker) "
         "VALUES ('S900000001:FAKEA','1','S900000001','FAKEA')", "FAIL",
         {"outcome": "poisoned_active_mapping"}),
        ("INSERT INTO public.sec_company_tickers_mf (class_id,cik,series_id,ticker) "
         "VALUES ('C900000099','1','S900000001','FAKEA')", "FAIL", {"outcome": "ambiguous"}),
        ("UPDATE public.sec_company_tickers_mf SET series_id='S000000099' WHERE ticker='T12'",
         "FAIL", {"outcome": "contradiction"}),
        ("UPDATE public.sec_company_tickers_mf SET series_id='' WHERE ticker='T12'",
         "NOT_EVALUATED", {"outcome": "partial"}),
        ("DELETE FROM public.sec_company_tickers_mf WHERE ticker='T11'", "NOT_EVALUATED",
         {"outcome": "missing"}),
        ("INSERT INTO public.sec_company_tickers_mf (class_id,cik,series_id,ticker) "
         "VALUES ('S000000777:T777','1','S000000777','T777')", "PASS",
         {"excluded_poisoned_count": 1}),
        ("DROP TABLE public.sec_company_tickers_mf", "NOT_EVALUATED",
         {"code": "sec_relation_missing"}),
        ("ALTER TABLE public.sec_company_tickers_mf DROP COLUMN updated_at", "NOT_EVALUATED",
         {"code": "sec_query_failed:42703"}),
        ("INSERT INTO public.sec_company_tickers_mf (class_id,cik,series_id,ticker) "
         "SELECT 'C8' || lpad(g::text, 8, '0'), '1', 'S8' || lpad(g::text, 8, '0'), 'X' || g "
         "FROM generate_series(1, 100000) g", "NOT_EVALUATED",
         {"code": "sec_row_ceiling_exceeded"}),
        # Round3 F2: the valid FAKEA row cannot hide a related malformed or
        # incomplete row; unrelated malformed rows stay excluded.
        ("INSERT INTO public.sec_company_tickers_mf (class_id,cik,series_id,ticker) "
         "VALUES ('C900000002','1','SX','FAKEA')", "FAIL",
         {"outcome": "poisoned_active_mapping"}),
        ("INSERT INTO public.sec_company_tickers_mf (class_id,cik,series_id,ticker) "
         "VALUES ('C900000003','1','','FAKEA')", "NOT_EVALUATED", {"outcome": "partial"}),
        ("INSERT INTO public.sec_company_tickers_mf (class_id,cik,series_id,ticker) "
         "VALUES ('C000000778','1','SX','T778')", "PASS", {"excluded_poisoned_count": 1}),
    ],
    ids=["stale", "future", "poison", "duplicate_ticker", "contradiction", "partial",
         "missing", "unrelated_poison", "table_missing", "column_missing", "ceiling",
         "valid_plus_malformed", "valid_plus_partial", "unrelated_malformed_series"],
)
def test_live_a8_company_tickers_mf_matrix(dsn, catalog, live_audit, mutation, status, expected):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(mutation)
    code, summary, dossier, capture_path = live_audit["run"](None)
    gate = dossier["gates"]["A8"]
    assert gate["status"] == status, gate
    assert code == (0 if status == "PASS" else 3)
    detail = dossier["details"]["A8"]
    if "failed" in expected:
        assert sorted(k for k, v in gate["checks"].items() if not v) == expected["failed"]
    if "outcome" in expected:
        assert detail["outcomes"][expected["outcome"]] == 1
    if "code" in expected:
        assert gate["code"] == expected["code"]
        assert json.loads(capture_path.read_bytes())["sec"]["relation"] == "public.sec_company_tickers_mf"
    if "excluded_poisoned_count" in expected:
        assert detail["excluded_poisoned_count"] == 1 and detail["active_poisoned_count"] == 0


def test_live_a8_permission_missing_is_not_evaluated(dsn, catalog, live_audit, monkeypatch):
    role = "nav_audit_reader_" + uuid.uuid4().hex[:12]
    ident = sql.Identifier(role)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE ROLE {} LOGIN").format(ident))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(ident))
        conn.execute(
            sql.SQL(
                "GRANT SELECT ON public.instruments_universe, public.funds_v, "
                "public.nav_fixture_funds, public.instrument_identity, public.nav_fixture_cohort TO {}"
            ).format(ident)
        )
    try:
        monkeypatch.setenv("NAV_AUDIT_READER_DSN", _role_dsn(dsn, role))
        code, summary, dossier, capture_path = live_audit["run"](None, dsn_env="NAV_AUDIT_READER_DSN")
        assert code == 3
        assert dossier["gates"]["A8"] == {
            "status": "NOT_EVALUATED",
            "code": "sec_privilege_missing",
            "checks": {},
        }
        assert dossier["gates"]["A7"]["status"] == "PASS"
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP OWNED BY {}").format(ident))
            conn.execute(sql.SQL("DROP ROLE {}").format(ident))


# ── governed plan-v4 publication, end to end ────────────────────────────────
def _operator_schema(dsn: str) -> str:
    """Ready W1 schema with its own Light read dependencies (never public's)."""
    schema = "nav_gov_" + uuid.uuid4().hex[:12]
    _install_w1(dsn, schema)
    with psycopg.connect(dsn, autocommit=True, options=f"-csearch_path={schema},public") as conn:
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO app_runtime").format(sql.Identifier(schema)))
        conn.execute(RISK_READ_MODEL_SQL)
        conn.execute("GRANT SELECT ON nav_timeseries, fund_risk_latest_mv TO app_runtime")
        conn.execute("CREATE TABLE funds_profile_mv (instrument_id uuid PRIMARY KEY)")
    return schema


def _pointer(dsn, schema):
    with psycopg.connect(dsn, options=f"-csearch_path={schema},public") as conn:
        return conn.execute(
            "SELECT c.policy_id, c.policy_version, v.policy_hash FROM nav_policy_current c "
            "JOIN nav_policy_versions v USING (policy_id, policy_version)"
        ).fetchone()


def _state(dsn, schema):
    with psycopg.connect(dsn, options=f"-csearch_path={schema},public") as conn:
        return conn.execute(
            "SELECT (SELECT jsonb_agg(to_jsonb(v) ORDER BY policy_version) FROM nav_policy_versions v),"
            "(SELECT count(*) FROM nav_instrument_policy_evidence),"
            "(SELECT to_jsonb(c) FROM nav_policy_current c),"
            "(SELECT count(*) FROM nav_policy_publication_receipts)"
        ).fetchone()


def _ledger(dsn, schema):
    """The private publication-receipt ledger (F4), oldest first."""
    with psycopg.connect(dsn, options=f"-csearch_path={schema},public") as conn:
        return conn.execute(
            "SELECT plan_sha256, policy_artifact_sha256, policy_document_digest, "
            "audit_receipt_sha256, audit_dossier_sha256, canary_manifest_sha256, "
            "capture_bundle_sha256, previous_policy_id, previous_policy_version, "
            "previous_policy_hash, "
            "captured_at <= published_at AND published_at <= sec_valid_until "
            "FROM nav_policy_publication_receipts ORDER BY published_at"
        ).fetchall()


def _receipt_files(live_audit, capsys, *, tag="canary", policy_path=None, snapshot_path=None):
    """Strict live audit → offline canary replay: the four operator inputs."""
    custody = live_audit["custody"]
    policy_path = policy_path or live_audit["policy_path"]
    snapshot_path = snapshot_path or custody / "source.json"
    code, summary, _dossier, capture_path = live_audit["run"](
        None, policy_path=policy_path, snapshot_path=snapshot_path
    )
    assert code == 0, summary
    code = verifier.main(
        [
            "--policy-file", str(policy_path),
            "--source-snapshot-file", str(snapshot_path),
            "--capture-file", str(capture_path),
            "--audit-config", str(live_audit["config_path"]),
            "--previous-policy-file", str(custody / "v1.json"),
            "--previous-policy-sha256", hashlib.sha256(live_audit["previous"]).hexdigest(),
            "--custody-root", str(custody),
            "--output", str(custody / f"audit-{tag}.json"),
            "--canary-output", str(custody / f"{tag}.json"),
        ]
    )
    printed = capsys.readouterr().out
    assert code == 0, printed
    return {
        "policy": policy_path,
        "audit_dossier": custody / f"audit-{tag}.json",
        "canary_manifest": custody / f"{tag}.json",
        "capture": capture_path,
    }


@contextlib.contextmanager
def _governed_env(dsn, live_audit, monkeypatch, capsys):
    files = _receipt_files(live_audit, capsys)
    schema = _operator_schema(dsn)
    try:
        with psycopg.connect(dsn, options=f"-csearch_path={schema},public") as conn:
            operator._publish_policy(conn, operator._policy(live_audit["previous_doc"])[0])
            conn.commit()
        # The operator pins the reviewed audit config file; the test pins its own.
        monkeypatch.setattr(operator, "AUDIT_CONFIG", live_audit["config_path"])
        monkeypatch.setenv("NAV_READINESS_DATABASE_URL", dsn)
        yield {**live_audit, "schema": schema, "files": files}
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def governed(dsn, catalog, live_audit, monkeypatch, capsys):
    """Strict live audit → offline canary replay → operator inputs (custody)."""
    with _governed_env(dsn, live_audit, monkeypatch, capsys) as env:
        yield env


def _op_args(env, files=None, hashes=None, *, mode=None, plan=None):
    files = {**env["files"], **(files or {})}
    hashes = hashes or {}
    hashes = {**{k: _sha(p) for k, p in files.items() if k not in hashes}, **hashes}
    args = [
        "--schema", env["schema"],
        "--expected-sql-sha256", hashlib.sha256(SCHEMA_SQL).hexdigest(),
        "--custody-root", str(env["custody"]),
    ]
    for key in ("policy", "audit_dossier", "canary_manifest", "capture"):
        flag = key.replace("_", "-")
        args += [f"--{flag}-file", str(files[key]), f"--{flag}-sha256", hashes[key]]
    if mode:
        args += ["--mode", mode, "--plan-sha256", plan]
    return args


def _op(args, capsys):
    code = operator.main(args)
    printed = capsys.readouterr().out
    return code, json.loads(printed), printed


def test_governed_publication_check_apply_replay(dsn, catalog, governed, capsys):
    env = governed
    previous_pointer = _pointer(dsn, env["schema"])
    code, out, printed = _op(_op_args(env), capsys)
    assert code == 0 and out["status"] == "ready", out
    plan = out["plan"]
    assert plan["plan_version"] == "nav-schema-plan-v4"
    assert plan["audit"] == out["audit"]
    assert set(out["audit"]) == set(operator.AUDIT_RECEIPT_KEYS)
    assert out["audit"]["previous_policy_identity"] == list(previous_pointer)
    assert out["audit"]["audit_dossier_sha256"] == _sha(env["files"]["audit_dossier"])
    assert plan["operation"]["audit"] == [
        _sha(env["files"]["audit_dossier"]),
        _sha(env["files"]["canary_manifest"]),
        _sha(env["files"]["capture"]),
    ]
    assert str(catalog["active"]) not in printed and "FAKEA" not in printed
    for legacy in (operator.previous_version_digest(plan), operator.v2_version_digest(plan)):
        code, blocked, _ = _op(_op_args(env, mode="apply", plan=legacy), capsys)
        assert (code, blocked["code"], blocked["dml_committed"]) == (2, "plan_version_mismatch", False)
    previous_row = _state(dsn, env["schema"])[0][0]
    # F1: the receipt retains the capture/decision/min-matched instants and
    # the SEC deadline is exactly the oldest matched updated_at + 7 days.
    audit = out["audit"]
    assert audit["captured_at"] == audit["decision_at"]
    assert dt.datetime.fromisoformat(audit["sec_valid_until"]) == (
        dt.datetime.fromisoformat(audit["min_matched_synced_at"]) + dt.timedelta(days=7))
    code, applied, _ = _op(_op_args(env, mode="apply", plan=out["plan_sha256"]), capsys)
    assert (code, applied["status"], applied["policy"], applied["published"]) == (
        0, "applied", "committed", True)
    assert _pointer(dsn, env["schema"]) == (
        "synthetic-xnys", "2026-09-24.2", env["policy"]["generation"]["policy_hash"])
    after = _state(dsn, env["schema"])
    assert after[0][0] == previous_row  # v1 version row immutable
    # F4: exactly one private ledger row binds the plan and the audit receipt.
    ledger = _ledger(dsn, env["schema"])
    assert ledger == [(
        out["plan_sha256"], plan["policy_sha256"], plan["policy_document_digest"],
        canonical_digest(audit), _sha(env["files"]["audit_dossier"]),
        _sha(env["files"]["canary_manifest"]), _sha(env["files"]["capture"]),
        *previous_pointer, True,
    )]
    code, replay, _ = _op(_op_args(env, mode="apply", plan=out["plan_sha256"]), capsys)
    assert (code, replay["status"], replay["dml_committed"]) == (0, "unchanged", False)
    assert _state(dsn, env["schema"]) == after
    assert _ledger(dsn, env["schema"]) == ledger


def _rewrite(env, key, name, mutate):
    """Rewrite one JSON input canonically into a NEW custody file."""
    document = json.loads(env["files"][key].read_bytes())
    mutate(document)
    return {key: _put(env["custody"], name, generator.canonical_json(document))}


def _relink(env, tag, *, capture=None, dossier=None):
    """Consistently rewrite capture/dossier and relink the canary manifest.

    Integrity links are not signatures: every hash still matches, so only the
    operator's time-window rules can refuse such a chain.
    """
    files = dict(env["files"])
    capture_doc = json.loads(files["capture"].read_bytes())
    dossier_doc = json.loads(files["audit_dossier"].read_bytes())
    manifest_doc = json.loads(files["canary_manifest"].read_bytes())
    if capture is not None:
        capture(capture_doc)
        raw = generator.canonical_json(capture_doc)
        files["capture"] = _put(env["custody"], f"c-{tag}.json", raw)
        digest = hashlib.sha256(raw).hexdigest()
        dossier_doc["inputs"]["capture"].update(
            capture_bundle_sha256=digest, captured_at=capture_doc["captured_at"]
        )
        manifest_doc["capture_bundle_sha256"] = digest
    if dossier is not None:
        dossier(dossier_doc)
    raw = generator.canonical_json(dossier_doc)
    files["audit_dossier"] = _put(env["custody"], f"d-{tag}.json", raw)
    manifest_doc["audit_report_sha256"] = hashlib.sha256(raw).hexdigest()
    files["canary_manifest"] = _put(
        env["custody"], f"m-{tag}.json", generator.canonical_json(manifest_doc)
    )
    return files


def _later(value: str, **delta) -> str:
    return (dt.datetime.fromisoformat(value) + dt.timedelta(**delta)).isoformat()


@pytest.mark.parametrize(
    "case,code",
    [
        ("policy_hash_arg", "policy_sha256_mismatch"),
        ("dossier_hash_arg", "audit_dossier_sha256_mismatch"),
        ("manifest_hash_arg", "canary_manifest_sha256_mismatch"),
        ("capture_hash_arg", "capture_sha256_mismatch"),
        ("malformed_hash_arg", "audit_receipt_hash_invalid"),
        ("gate_fail", "audit_not_strict_pass"),
        ("gate_not_evaluated", "audit_not_strict_pass"),
        ("check_missing", "audit_not_strict_pass"),
        ("check_false", "audit_not_strict_pass"),
        ("result_fail", "audit_dossier_invalid"),
        ("not_strict", "audit_dossier_invalid"),
        ("canary_not_requested", "audit_dossier_invalid"),
        ("dossier_extra_key", "audit_dossier_invalid"),
        ("dossier_duplicate_key", "audit_dossier_invalid"),
        ("contract_sha", "audit_contract_mismatch"),
        ("sec_contract", "audit_contract_mismatch"),
        ("verifier_sha", "audit_verifier_mismatch"),
        ("config_sha", "audit_config_mismatch"),
        ("policy_artifact_sha", "audit_policy_mismatch"),
        ("live_drift", "audit_policy_mismatch"),
        ("previous_identity", "audit_previous_policy_invalid"),
        ("a8_valid_until_missing", "audit_dossier_invalid"),
        ("a8_outcomes", "audit_dossier_invalid"),
        ("manifest_foreign_uuid", "canary_manifest_invalid"),
        ("manifest_report_sha", "canary_manifest_invalid"),
        ("manifest_selection", "canary_manifest_invalid"),
        ("manifest_salt", "canary_manifest_invalid"),
        ("manifest_extra_key", "canary_manifest_invalid"),
        ("capture_sec_row", "audit_capture_mismatch"),
        ("capture_kind_round1", "audit_capture_mismatch"),
        ("policy_not_canonical", "policy_not_canonical"),
        ("policy_hand_authored", "policy_not_generated"),
        ("repo_config_pin", "audit_config_mismatch"),
        ("paths_collide", "audit_receipt_paths_collide"),
        ("missing_arg", "audit_receipt_required"),
        # Round3 F5: nested dossier objects are typed before any lookup.
        ("capture_cohort_list", "audit_dossier_invalid"),
        ("capture_sec_number", "audit_dossier_invalid"),
        ("a7_list", "audit_dossier_invalid"),
        # Round3 F1: the retained window is internally consistent...
        ("deadline_inconsistent", "audit_sec_deadline_inconsistent"),
        ("decision_not_capture", "audit_receipt_time_invalid"),
        # ...and a consistently relinked future capture fails on the DB clock.
        ("future_capture", "audit_capture_in_future"),
    ],
)
def test_governed_receipt_tamper_matrix(dsn, catalog, governed, capsys, monkeypatch, case, code):
    env = governed
    before = _state(dsn, env["schema"])
    files, hashes = {}, {}

    def dossier(mutate, name="d.json"):
        files.update(_rewrite(env, "audit_dossier", name, mutate))

    def manifest(mutate):
        files.update(_rewrite(env, "canary_manifest", "m.json", mutate))

    if case.endswith("_hash_arg") and case != "malformed_hash_arg":
        key = {"policy": "policy", "dossier": "audit_dossier", "manifest": "canary_manifest",
               "capture": "capture"}[case.split("_")[0]]
        hashes[key] = "0" * 64
    elif case == "malformed_hash_arg":
        hashes["policy"] = "not-a-hash"
    elif case == "gate_fail":
        dossier(lambda d: d["gates"]["A8"].update(status="FAIL"))
    elif case == "gate_not_evaluated":
        dossier(lambda d: d["gates"]["A7"].update(status="NOT_EVALUATED", code="x"))
    elif case == "check_missing":
        dossier(lambda d: d["gates"]["A8"]["checks"].pop("fresh_within_max_age"))
    elif case == "check_false":
        dossier(lambda d: d["gates"]["A4"]["checks"].update(structural_daily_within_ceiling=False))
    elif case == "result_fail":
        dossier(lambda d: d.update(result="fail"))
    elif case == "not_strict":
        dossier(lambda d: d.update(strict=False))
    elif case == "canary_not_requested":
        dossier(lambda d: d.update(canary_requested=False))
    elif case == "dossier_extra_key":
        dossier(lambda d: d.update(extra=1))
    elif case == "dossier_duplicate_key":
        raw = env["files"]["audit_dossier"].read_bytes()
        files["audit_dossier"] = _put(env["custody"], "dup.json", raw.replace(b'{"audit_version"', b'{"result":"pass","audit_version"', 1))
    elif case == "contract_sha":
        dossier(lambda d: d["inputs"].update(audit_contract_sha256="0" * 64))
    elif case == "sec_contract":
        dossier(lambda d: d["inputs"].update(sec_source_contract="fund-classes-latest"))
    elif case == "verifier_sha":
        dossier(lambda d: d["inputs"].update(verifier_source_sha256="0" * 64))
    elif case == "config_sha":
        dossier(lambda d: d["inputs"].update(audit_config_sha256="0" * 64))
    elif case == "policy_artifact_sha":
        dossier(lambda d: d["inputs"].update(policy_artifact_sha256="0" * 64))
    elif case == "live_drift":
        dossier(lambda d: d["inputs"].update(live_source_snapshot_sha256="0" * 64))
    elif case == "previous_identity":
        dossier(lambda d: d["inputs"].update(previous_policy_identity=None))
    elif case == "a8_valid_until_missing":
        dossier(lambda d: d["details"]["A8"]["freshness"].update(valid_until=None))
    elif case == "a8_outcomes":
        dossier(lambda d: d["details"]["A8"]["outcomes"].update(matched=2, missing=1))
    elif case == "manifest_foreign_uuid":
        manifest(lambda m: m.update(allowlist=sorted([*m["allowlist"], str(catalog["inactive"])]),
                                    size=m["size"] + 1))
    elif case == "manifest_report_sha":
        manifest(lambda m: m.update(audit_report_sha256="0" * 64))
    elif case == "manifest_selection":
        manifest(lambda m: m.update(allowlist=m["allowlist"][:1], size=1))
    elif case == "manifest_salt":
        manifest(lambda m: m.update(salt="other"))
    elif case == "manifest_extra_key":
        manifest(lambda m: m.update(note="x"))
    elif case == "capture_sec_row":
        files.update(_rewrite(env, "capture", "c.json",
                              lambda c: c["sec"]["rows"][0].update(ticker="OTHER")))
    elif case == "capture_kind_round1":
        files.update(_rewrite(env, "capture", "c.json",
                              lambda c: c.update(kind="nav-identity-audit-capture-v2")))
    elif case == "policy_not_canonical":
        document = json.loads(env["files"]["policy"].read_bytes())
        files["policy"] = _put(env["custody"], "p.json", json.dumps(document, indent=1).encode())
    elif case == "policy_hand_authored":
        files["policy"] = _put(env["custody"], "p.json",
                               generator.canonical_json(env["previous_doc"]))
    elif case == "repo_config_pin":
        monkeypatch.setattr(operator, "AUDIT_CONFIG", ROOT / "configs" / "nav_identity_audit_v2.json")
    elif case == "paths_collide":
        files["capture"] = env["files"]["audit_dossier"]
    elif case == "capture_cohort_list":
        dossier(lambda d: d["inputs"]["capture"].update(cohort=[]))
    elif case == "capture_sec_number":
        dossier(lambda d: d["inputs"]["capture"].update(sec=5))
    elif case == "a7_list":
        dossier(lambda d: d["details"].update(A7=[]))
    elif case == "deadline_inconsistent":
        dossier(lambda d: d["details"]["A8"]["freshness"].update(
            valid_until=_later(d["details"]["A8"]["freshness"]["valid_until"], days=1)))
    elif case == "decision_not_capture":
        dossier(lambda d: d["details"]["A8"]["freshness"].update(
            decision_at=_later(d["details"]["A8"]["freshness"]["decision_at"], seconds=1)))
    elif case == "future_capture":
        future = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).isoformat()
        files.update(_relink(
            env, case,
            capture=lambda c: c.update(captured_at=future),
            dossier=lambda d: d["details"]["A8"]["freshness"].update(decision_at=future),
        ))
    args = _op_args(env, files, hashes)
    if case == "missing_arg":
        index = args.index("--canary-manifest-file")
        del args[index:index + 2]
    result, out, _ = _op(args, capsys)
    assert (result, out["code"], out["dml_committed"]) == (2, code, False), out
    assert _state(dsn, env["schema"]) == before


@pytest.mark.parametrize(
    "case,code",
    [
        ("symlink_file", "custody_symlink_invalid"),
        ("symlink_parent", "custody_symlink_invalid"),
        ("mode_0644", "custody_file_not_private"),
        ("hardlink", "custody_file_not_regular"),
        ("directory", "custody_file_not_regular"),
        ("outside_root", "custody_path_outside_root"),
        ("missing", "custody_file_missing"),
        ("root_0755", "custody_root_not_private"),
        ("root_symlink", "custody_root_invalid"),
        ("root_in_git", "custody_inside_git_checkout"),
        ("toctou_replace", "custody_file_changed"),
    ],
)
def test_governed_custody_matrix(dsn, catalog, governed, capsys, monkeypatch, tmp_path, case, code):
    env = governed
    custody = env["custody"]
    dossier_path = env["files"]["audit_dossier"]
    files, extra_root = {}, None
    if case == "symlink_file":
        link = custody / "link.json"
        link.symlink_to(dossier_path)
        files["audit_dossier"] = link
    elif case == "symlink_parent":
        (custody / "real").mkdir(mode=0o700)
        _put(custody / "real", "d.json", dossier_path.read_bytes())
        (custody / "alias").symlink_to(custody / "real", target_is_directory=True)
        files["audit_dossier"] = custody / "alias" / "d.json"
    elif case == "mode_0644":
        copy_path = _put(custody, "open.json", dossier_path.read_bytes())
        copy_path.chmod(0o644)
        files["audit_dossier"] = copy_path
    elif case == "hardlink":
        os.link(dossier_path, custody / "hard.json")
        files["audit_dossier"] = custody / "hard.json"
    elif case == "directory":
        (custody / "dir.json").mkdir(mode=0o700)
        files["audit_dossier"] = custody / "dir.json"
    elif case == "outside_root":
        outside = tmp_path / "elsewhere"
        outside.mkdir(mode=0o700)
        files["audit_dossier"] = _put(outside, "d.json", dossier_path.read_bytes())
    elif case == "missing":
        files["audit_dossier"] = custody / "absent.json"
    elif case == "root_0755":
        custody.chmod(0o755)
    elif case == "root_symlink":
        extra_root = tmp_path / "root-link"
        extra_root.symlink_to(custody, target_is_directory=True)
    elif case == "root_in_git":
        (custody / ".git").mkdir()
    hashes = {"audit_dossier": _sha(dossier_path)} if case in ("missing", "directory") else {}
    if case in ("symlink_file", "symlink_parent", "outside_root"):
        hashes = {"audit_dossier": _sha(dossier_path)}
    args = _op_args(env, files, hashes)
    if extra_root is not None:
        args[args.index("--custody-root") + 1] = str(extra_root)
    if case == "toctou_replace":
        replacement = _put(custody, "swap.json", b"{}\n")
        real_read = os.read
        target_inode = os.stat(dossier_path).st_ino
        swapped = []

        def racing_read(fd, size):
            # Swap the path while the dossier's own descriptor is being read.
            if not swapped and os.fstat(fd).st_ino == target_inode:
                swapped.append(True)
                os.replace(replacement, dossier_path)
            return real_read(fd, size)

        monkeypatch.setattr(operator.os, "read", racing_read)
    before = _state(dsn, env["schema"])
    try:
        result, out, _ = _op(args, capsys)
    finally:
        custody.chmod(0o700)
    assert (result, out["code"], out["dml_committed"]) == (2, code, False), out
    assert _state(dsn, env["schema"]) == before


def test_governed_pointer_must_be_previous_or_target(dsn, catalog, governed, capsys):
    env = governed
    code, out, _ = _op(_op_args(env), capsys)
    plan = out["plan_sha256"]
    # A different version becomes current after the audit: the receipt's
    # previous identity no longer holds, at check and at apply (under locks).
    foreign = copy.deepcopy(env["previous_doc"])
    foreign["policy_version"] = "2026-09-23.9"
    with psycopg.connect(dsn, options=f"-csearch_path={env['schema']},public") as conn:
        operator._publish_policy(conn, operator._policy(foreign)[0])
        conn.commit()
    before = _state(dsn, env["schema"])
    code, out, _ = _op(_op_args(env), capsys)
    assert (code, out["code"]) == (2, "current_pointer_not_previous")
    code, out, _ = _op(_op_args(env, mode="apply", plan=plan), capsys)
    assert (code, out["code"], out["dml_committed"]) == (2, "current_pointer_not_previous", False)
    assert _state(dsn, env["schema"]) == before


def _expiring_sec_window(dsn, seconds: int) -> None:
    """T11's mapping reaches the 7-day limit ``seconds`` from now (DB clock)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE public.sec_company_tickers_mf SET updated_at = clock_timestamp() "
            "- interval '7 days' + make_interval(secs => %s::float8) WHERE ticker='T11'",
            (seconds,),
        )


def _sleep_past(instant: dt.datetime, margin: float = 1.5) -> None:
    remaining = (instant - dt.datetime.now(dt.timezone.utc)).total_seconds()
    time.sleep(max(0.0, remaining) + margin)


def test_governed_window_expires_between_check_and_writer_locks(
    dsn, catalog, live_audit, capsys, monkeypatch
):
    """Real time passes after the preliminary check, before the NAV locks:
    sec_valid_until (oldest matched updated_at + 7d) is re-decided under them."""
    _expiring_sec_window(dsn, 40)
    with _governed_env(dsn, live_audit, monkeypatch, capsys) as env:
        code, out, _ = _op(_op_args(env), capsys)
        assert (code, out["status"]) == (0, "ready"), out
        valid_until = dt.datetime.fromisoformat(out["audit"]["sec_valid_until"])
        real_locks = operator._try_nav_writer_locks
        reached = []

        def locks_after_expiry(conn):
            # Reached only once apply's preliminary window check has passed.
            reached.append(conn.execute("SELECT clock_timestamp()").fetchone()[0])
            _sleep_past(valid_until)
            return real_locks(conn)

        monkeypatch.setattr(operator, "_try_nav_writer_locks", locks_after_expiry)
        before = _state(dsn, env["schema"])
        code, out, _ = _op(_op_args(env, mode="apply", plan=out["plan_sha256"]), capsys)
        assert (code, out["code"], out["dml_committed"], out["policy"]) == (
            2, "audit_sec_freshness_expired", False, "rolled_back")
        assert len(reached) == 1 and reached[0] < valid_until
        assert _state(dsn, env["schema"]) == before
        assert _ledger(dsn, env["schema"]) == []


def test_governed_replay_rechecks_the_receipt_window(
    dsn, catalog, live_audit, capsys, monkeypatch
):
    _expiring_sec_window(dsn, 40)
    with _governed_env(dsn, live_audit, monkeypatch, capsys) as env:
        code, out, _ = _op(_op_args(env), capsys)
        assert code == 0, out
        plan = out["plan_sha256"]
        valid_until = dt.datetime.fromisoformat(out["audit"]["sec_valid_until"])
        code, applied, _ = _op(_op_args(env, mode="apply", plan=plan), capsys)
        assert (code, applied["status"], applied["policy"]) == (0, "applied", "committed")
        code, replay, _ = _op(_op_args(env, mode="apply", plan=plan), capsys)
        assert (code, replay["status"], replay["dml_committed"]) == (0, "unchanged", False)
        after, ledger = _state(dsn, env["schema"]), _ledger(dsn, env["schema"])
        _sleep_past(valid_until)
        # Replay and check use the same window: an expired receipt never
        # reports success, even for an operation already committed.
        code, replay, _ = _op(_op_args(env, mode="apply", plan=plan), capsys)
        assert (code, replay["code"], replay["dml_committed"]) == (
            2, "audit_sec_freshness_expired", False)
        code, check, _ = _op(_op_args(env), capsys)
        assert (code, check["code"]) == (2, "audit_sec_freshness_expired")
        assert (_state(dsn, env["schema"]), _ledger(dsn, env["schema"])) == (after, ledger)


@pytest.mark.parametrize(
    "live_audit", [{"coverage_end": dt.date(2026, 6, 30)}], indirect=True
)
def test_governed_expired_policy_is_refused_at_check_and_apply(
    dsn, catalog, live_audit, capsys, monkeypatch
):
    """A strict PASS audit of an already-expired calendar never publishes."""
    expiry = dt.datetime.fromisoformat(live_audit["policy"]["valid_through"])
    assert expiry < dt.datetime.now(dt.timezone.utc)
    with _governed_env(dsn, live_audit, monkeypatch, capsys) as env:
        before = _state(dsn, env["schema"])
        code, out, _ = _op(_op_args(env), capsys)
        assert (code, out["code"]) == (2, "policy_valid_through_expired")
        code, out, _ = _op(_op_args(env, mode="apply", plan="0" * 64), capsys)
        assert (code, out["code"], out["dml_committed"]) == (
            2, "policy_valid_through_expired", False)
        assert _state(dsn, env["schema"]) == before


def test_governed_target_pointer_is_exact_replay_only(
    dsn, catalog, governed, capsys, monkeypatch
):
    """Round3 F3/F4: once published, only the exact recorded operation replays."""
    env = governed
    code, out, _ = _op(_op_args(env), capsys)
    plan, original = out["plan_sha256"], out["audit"]
    code, applied, _ = _op(_op_args(env, mode="apply", plan=plan), capsys)
    assert (code, applied["status"]) == (0, "applied")
    after, ledger = _state(dsn, env["schema"]), _ledger(dsn, env["schema"])
    # (a) Another valid strict audit + canary of the SAME policy bytes.
    second = {**env, "files": _receipt_files(env, capsys, tag="canary-second")}
    assert _sha(second["files"]["policy"]) == _sha(env["files"]["policy"])
    assert _sha(second["files"]["audit_dossier"]) != _sha(env["files"]["audit_dossier"])
    code, out_a, _ = _op(_op_args(second), capsys)
    assert (code, out_a["code"]) == (2, "target_policy_not_exact_replay")
    code, out_a, _ = _op(_op_args(second, mode="apply", plan=plan), capsys)
    assert (code, out_a["code"], out_a["dml_committed"]) == (
        2, "target_policy_not_exact_replay", False)
    # The stale-replay proof is bound to the original plan AND audit identity.
    evidence = operator._policy(env["files"]["policy"].read_bytes())[0]
    policy_sha = _sha(env["files"]["policy"])
    with psycopg.connect(dsn, options=f"-csearch_path={env['schema']},public") as conn:
        assert operator._already_applied(
            conn, plan, evidence, [], audit=original, policy_sha256=policy_sha) is True
        assert operator._already_applied(
            conn, plan, evidence, [], audit=out_a["audit"], policy_sha256=policy_sha) is False
        assert operator._already_applied(
            conn, "e" * 64, evidence, [], audit=original, policy_sha256=policy_sha) is False
        conn.rollback()
    # (b) The same version regenerated later (same policy_hash, new lifecycle
    # instants), audited against the original predecessor: never appended.
    custody = env["custody"]
    regenerated, raw, document = _artifact(
        dsn, custody, monkeypatch, "--source-snapshot-output", str(custody / "source-regen.json"),
        name="policy-regen.json",
    )
    capsys.readouterr()
    assert document["generation"]["policy_hash"] == env["policy"]["generation"]["policy_hash"]
    assert raw != env["files"]["policy"].read_bytes()
    regen = {**env, "files": _receipt_files(
        env, capsys, tag="canary-regen", policy_path=regenerated,
        snapshot_path=custody / "source-regen.json")}
    code, out_b, _ = _op(_op_args(regen), capsys)
    assert (code, out_b["code"]) == (2, "target_policy_not_exact_replay")
    code, out_b, _ = _op(_op_args(regen, mode="apply", plan=plan), capsys)
    assert (code, out_b["code"], out_b["dml_committed"]) == (
        2, "target_policy_not_exact_replay", False)
    assert (_state(dsn, env["schema"]), _ledger(dsn, env["schema"])) == (after, ledger)
    # (c) The original chain remains an exact, write-free replay.
    code, replay, _ = _op(_op_args(env, mode="apply", plan=plan), capsys)
    assert (code, replay["status"], replay["dml_committed"]) == (0, "unchanged", False)
    code, fresh, _ = _op(_op_args(env), capsys)
    assert (code, fresh["status"]) == (0, "ready")
    code, replay, _ = _op(_op_args(env, mode="apply", plan=fresh["plan_sha256"]), capsys)
    assert (code, replay["status"], replay["policy"]) == (0, "unchanged", "unchanged")
    assert (_state(dsn, env["schema"]), _ledger(dsn, env["schema"])) == (after, ledger)
