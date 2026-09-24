"""Real disposable PG18/Timescale tests for the read-only current-catalog snapshot."""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import stat
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
        active_rows = _uuid_entity(active, 1, ticker="FAKEA", series="SYNTH-SERIES", figi=synthetic_figi(1))
        inactive_rows = _uuid_entity(inactive, 2, ticker="FAKEB", active=False)
        _seed(conn, active_rows, (inactive_rows[0], None, None))
    return {"active": active, "inactive": inactive}


def _build_args(dsn_env, root, output, *extra, version="2026-09-24.2"):
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
        END.isoformat(),
        "--policy-id",
        "synthetic-xnys",
        "--policy-version",
        version,
        *extra,
    ]


def _artifact(dsn, tmp_path, monkeypatch, *extra):
    output = tmp_path / "approved-policy.json"
    monkeypatch.setenv("NAV_POLICY_TEST_DSN", dsn)
    assert generator.main(_build_args("NAV_POLICY_TEST_DSN", tmp_path, output, *extra)) == 0
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
    assert "SYNTH-SERIES" not in raw.decode() and "FAKEA" not in raw.decode()
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
        assert result != 0 and emitted["code"] == "generator_metadata_invalid"
        assert emitted["dml_committed"] is False and emitted["published"] is False
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


def _audit_config(query: str, **builder) -> dict:
    return {
        "audit_config_version": verifier.AUDIT_CONFIG_VERSION,
        "audit_contract_version": verifier.AUDIT_CONTRACT_VERSION,
        "active_ceiling": 5103,
        "canary_salt": "db-canary",
        "sec": {"max_synced_age_days": 7},
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


@pytest.fixture
def live_audit(dsn, catalog, tmp_path, monkeypatch, capsys):
    custody = tmp_path / "custody"
    custody.mkdir(mode=0o700)
    with psycopg.connect(dsn, autocommit=True) as conn:
        extra = [_uuid_entity(uuid.uuid4(), n, fund_type=kind) for n, kind in ((11, "mutual_fund"), (12, "etf"))]
        for rows in extra:
            _seed(conn, rows)
        conn.execute("DROP TABLE IF EXISTS public.nav_fixture_cohort, public.fund_classes_latest_mv, public.nav_fixture_sink")
        conn.execute("CREATE TABLE public.nav_fixture_cohort (instrument_id uuid, strategy_label text)")
        conn.execute(
            "CREATE TABLE public.fund_classes_latest_mv (class_id text, series_id text, ticker text, "
            "source_period_end date, synced_at timestamptz)"
        )
        members = [catalog["active"], catalog["inactive"], *(rows[0]["instrument_id"] for rows in extra)]
        for identifier, label in zip(members, ("Large Blend", "Large Blend", "Government Bond", "Large Blend")):
            conn.execute("INSERT INTO public.nav_fixture_cohort VALUES (%s,%s)", (identifier, label))
        conn.execute(
            "INSERT INTO public.fund_classes_latest_mv VALUES "
            "('C000000001','SYNTH-SERIES','FAKEA','2026-06-30',clock_timestamp())"
        )
    output, raw, policy = _artifact(dsn, custody, monkeypatch, "--source-snapshot-output", str(custody / "source.json"))
    capsys.readouterr()
    previous = json.dumps(
        {
            "generator_version": "fund-nav-policy-generator-v1",
            "generation": {"policy_hash": "1" * 64},
            "instrument_evidence": [
                {"instrument_id": str(catalog["active"]), "fund_status": "ACTIVE"},
                {"instrument_id": str(catalog["inactive"]), "fund_status": "INACTIVE"},
            ],
            **{
                k: policy[k]
                for k in (
                    "calendar_id", "calendar_version", "calendar_source", "calendar_digest",
                    "calendar_session_count", "coverage_start", "coverage_end", "sessions",
                )
            },
        }
    ).encode()
    (custody / "v1.json").write_bytes(previous)
    monkeypatch.setenv("NAV_AUDIT_DSN", dsn)
    counter = iter(range(1000))

    def run(config: dict, *extra: str) -> tuple[int, dict, dict, Path]:
        n = next(counter)
        config_path = custody / f"config-{n}.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        capture_path = custody / f"capture-{n}.json"
        code = verifier.main(
            [
                "--policy-file", str(output),
                "--source-snapshot-file", str(custody / "source.json"),
                "--dsn-env", "NAV_AUDIT_DSN",
                "--capture-output", str(capture_path),
                "--audit-config", str(config_path),
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

    return {"run": run, "custody": custody, "policy": policy, "members": members}


def test_live_readonly_audit_strict_pass_and_canary(dsn, catalog, live_audit):
    custody = live_audit["custody"]
    config = _audit_config(
        "SELECT instrument_id, strategy_label FROM public.nav_fixture_cohort ORDER BY instrument_id"
    )
    code, summary, dossier, capture_path = live_audit["run"](config, "--canary-output", str(custody / "canary.json"))
    assert code == 0 and set(summary["gates"].values()) == {"PASS"}, summary["gates"]
    assert summary["sec_outcomes"] == {"matched": 1, "missing": 2}
    assert summary["canary"]["status"] == "written" and summary["canary"]["size"] == 3
    capture_raw = capture_path.read_bytes()
    for path in (capture_path, custody / "canary.json"):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    bundle = json.loads(capture_raw)
    assert bundle["cohort"]["row_count"] == 4 and bundle["sec"]["row_count"] == 1
    assert bundle["source_snapshot_sha256"] == live_audit["policy"]["generation"]["source_snapshot_sha256"]
    assert dossier["inputs"]["capture"]["capture_bundle_sha256"] == hashlib.sha256(capture_raw).hexdigest()
    assert dossier["details"]["A8"]["freshness"]["max_synced_age_days"] == 7
    canary = json.loads((custody / "canary.json").read_bytes())
    assert canary["policy_hash"] == live_audit["policy"]["generation"]["policy_hash"]
    assert canary["capture_bundle_sha256"] == hashlib.sha256(capture_raw).hexdigest()
    assert len(canary["allowlist"]) == 3
    # The persisted bundle replays offline to the identical dossier content.
    replay = verifier.audit(
        (custody / "approved-policy.json").read_bytes(),
        snapshot_raw=(custody / "source.json").read_bytes(),
        capture_raw=capture_raw,
        previous_raw=(custody / "v1.json").read_bytes(),
        config_raw=json.dumps(config).encode(),
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
    # The outer LIMIT stops the server at the sentinel: 250k produced, 100,001
    # sent, read in bounded batches (never a whole-result fetchall).
    assert sum(fetched) == verifier.ROW_CEILING + 1
    assert max(fetched) <= verifier.FETCH_BATCH
    assert json.loads(capture_path.read_bytes())["cohort"]["rows"] == []
    # A query's own ORDER BY / LIMIT is preserved inside the wrapper.
    own_limit = _audit_config(
        "SELECT instrument_id, strategy_label FROM public.nav_fixture_cohort ORDER BY instrument_id LIMIT 2"
    )
    fetched.clear()
    code, summary, dossier, capture_path = live_audit["run"](own_limit)
    assert json.loads(capture_path.read_bytes())["cohort"]["row_count"] == 2
    assert dossier["details"]["A7"]["cohort_rows"] == 2 and sum(fetched) == 2


def test_live_cohort_write_attempts_are_rejected_or_read_only(dsn, catalog, live_audit):
    custody = live_audit["custody"]
    # Lexically unsafe queries never reach the database.
    for query, expected in (
        ("SELECT nextval('public.nav_audit_seq') AS instrument_id, 'x' AS strategy_label", "audit_config_cohort_query_unsafe"),
        ("WITH d AS (DELETE FROM public.nav_fixture_cohort RETURNING *) SELECT * FROM d", "audit_config_builder_invalid"),
    ):
        code, summary, dossier, capture_path = live_audit["run"](_audit_config(query))
        assert code == 2 and summary["code"] == expected
        assert not dossier and not capture_path.exists()
    # A write hidden in a function passes the lexical check but not READ ONLY.
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
    config = _audit_config("SELECT instrument_id, strategy_label FROM public.nav_fixture_cohort")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("UPDATE public.instruments_universe SET currency='EUR' WHERE instrument_id=%s", (catalog["active"],))
    code, summary, dossier, capture_path = live_audit["run"](config)
    assert code == 3 and summary["gates"]["A5"] == "FAIL"
    assert summary["gates"]["A7"] == summary["gates"]["A8"] == "NOT_EVALUATED"
    assert json.loads(capture_path.read_bytes())["source_snapshot_sha256"] == dossier["inputs"]["live_source_snapshot_sha256"]


def test_live_stale_sec_fails_a8_and_refuses_canary(dsn, catalog, live_audit):
    custody = live_audit["custody"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("UPDATE public.fund_classes_latest_mv SET synced_at = clock_timestamp() - interval '8 days'")
    config = _audit_config("SELECT instrument_id, strategy_label FROM public.nav_fixture_cohort")
    code, summary, dossier, _ = live_audit["run"](config, "--canary-output", str(custody / "canary-stale.json"))
    assert code == 3 and summary["gates"]["A8"] == "FAIL"
    assert summary["canary"] == {"status": "refused", "code": "canary_requires_strict_audit_pass"}
    assert dossier["gates"]["A8"]["checks"]["fresh_within_max_age"] is False
    assert not (custody / "canary-stale.json").exists()
