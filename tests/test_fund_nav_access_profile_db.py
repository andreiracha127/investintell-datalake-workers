"""R2-A N2/N6: SECURITY DEFINER search_path and the light_app_runtime_v1 profile.

Real PostgreSQL 18.4 / TimescaleDB 2.27.2 (NAV_W1_TEST_DSN, loopback, no skip).
The cluster fixture roles ``app_runtime`` and ``worker_writer`` must exist
(non-superuser, NOLOGIN is enough: the tests use ``SET ROLE``). Roles are
cluster-wide, so every membership granted here is revoked in ``finally``.
"""

from __future__ import annotations

import hashlib
import json
import uuid

import psycopg
import pytest
from psycopg import sql

from scripts import fund_nav_readiness_schema as operator
from src.workers import fund_nav_readiness as readiness
from tests import test_fund_nav_readiness_db as base
from tests.test_fund_nav_readiness_db import ROOT, _bootstrap, _connect, _seed

# Shared DB fixtures (same loopback/version guards and disposable schemas).
test_dsn = base.test_dsn
schema = base.schema

DDL = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
SNAPSHOT = "fund_nav_snapshot_current_at_v1"


def _check(test_dsn, schema):
    with _connect(test_dsn, schema, autocommit=True) as conn:
        return operator._check(conn, schema)


def _published(test_dsn, schema, monkeypatch):
    iid, grid, _ = _seed(test_dsn, schema)
    monkeypatch.setattr(readiness, "connect", lambda dsn: _connect(dsn, schema))
    report = readiness.run(test_dsn)
    assert report["ready_count"] == 1
    return iid, uuid.UUID(report["run_id"])


def _snapshot(conn, iid, run_id, at=None):
    return conn.execute(
        f"SELECT {SNAPSHOT}(%s, %s, COALESCE(%s, clock_timestamp()))",
        (iid, run_id, at),
    ).fetchone()[0]


# ──────────────────────────────────────────────────────────────────────────────
# N2: pg_temp cannot shadow the SECURITY DEFINER snapshot
# ──────────────────────────────────────────────────────────────────────────────
SHADOWED = (
    "fund_nav_readiness_current",
    "fund_nav_readiness_runs",
    "fund_nav_readiness_v1",
    "nav_policy_versions",
    "nav_policy_current",
    "nav_valuation_schedules",
    "fund_nav_risk_publication",
    "fund_nav_risk_runs",
    "fund_nav_feature_evidence",
    "fund_nav_data_heads",
    "fund_nav_reexpression_holds",
    "nav_instrument_policy_evidence",
)


def _shadow_all(conn):
    """As app_runtime: empty temp homonyms built from catalog metadata only."""
    for name in SHADOWED:
        columns = conn.execute(
            """SELECT a.attname, format_type(a.atttypid, a.atttypmod)
               FROM pg_attribute a WHERE a.attrelid = to_regclass(%s)
                 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum""",
            (name,),
        ).fetchall()
        assert columns, name
        conn.execute(
            sql.SQL("CREATE TEMP TABLE {} ({})").format(
                sql.Identifier(name),
                sql.SQL(", ").join(
                    sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(typ))
                    for col, typ in columns
                ),
            )
        )


def test_app_runtime_temp_homonyms_cannot_change_snapshot(
    test_dsn, schema, monkeypatch
):
    iid, run_id = _published(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema) as conn:
        conn.execute("SET ROLE app_runtime")
        assert _snapshot(conn, iid, run_id) is True
        _shadow_all(conn)  # empty homonyms for every relation the function reads
        assert _snapshot(conn, iid, run_id) is True
        # The attacker sees its own empty shadow when naming it unqualified.
        assert (
            conn.execute("SELECT count(*) FROM fund_nav_readiness_current").fetchone()[
                0
            ]
            == 0
        )
        conn.rollback()


def test_schema_only_search_path_would_be_shadowable_negative_control(
    test_dsn, schema, monkeypatch
):
    """Without an explicit trailing pg_temp, PostgreSQL searches pg_temp FIRST."""
    iid, run_id = _published(test_dsn, schema, monkeypatch)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(
            sql.SQL(
                "ALTER FUNCTION {}(uuid,uuid,timestamptz) SET search_path = {}"
            ).format(sql.Identifier(SNAPSHOT), sql.Identifier(schema))
        )
        report = operator._check(conn, schema)
        assert (report["compatibility"], report["ready"]) == ("incompatible", False)
    with _connect(test_dsn, schema) as conn:
        conn.execute("SET ROLE app_runtime")
        _shadow_all(conn)
        assert _snapshot(conn, iid, run_id) is False  # shadow wins: the attack works
        conn.rollback()


@pytest.mark.parametrize(
    "path",
    ['"$user", {s}, pg_temp', "{s}, public, pg_temp", "pg_temp, {s}", "{s}"],
)
def test_manifest_rejects_any_other_function_search_path(test_dsn, schema, path):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(
            f"ALTER FUNCTION {SNAPSHOT}(uuid,uuid,timestamptz) "
            f"SET search_path = {path.format(s=json.dumps(schema))}"
        )
        report = operator._check(conn, schema)
    assert report["compatibility"] == "incompatible"
    assert "functions" in report["mismatches"]


def test_security_definer_owner_change_is_not_ready(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(
            f"ALTER FUNCTION {SNAPSHOT}(uuid,uuid,timestamptz) OWNER TO worker_writer"
        )
        report = operator._check(conn, schema)
    assert report["ready"] is False and report["access"] == "unsafe"


# ──────────────────────────────────────────────────────────────────────────────
# N6: light_app_runtime_v1
# ──────────────────────────────────────────────────────────────────────────────
def test_profile_is_exact_and_app_reads_only_its_objects(test_dsn, schema, monkeypatch):
    iid, run_id = _published(test_dsn, schema, monkeypatch)
    report = _check(test_dsn, schema)
    assert (report["status"], report["access"], report["dependencies"]) == (
        "ready",
        "exact",
        {"fund_risk_latest_mv": "ok", "nav_timeseries": "ok"},
    )
    assert report["access_sha256"] == report["reference_access_sha256"]
    with _connect(test_dsn, schema) as conn:
        conn.execute("SET ROLE app_runtime")
        for name in operator.READ_RELATIONS:
            conn.execute(
                sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(name))
            )
        conn.execute("SELECT count(*) FROM fund_risk_latest_mv")
        assert _snapshot(conn, iid, run_id) is True
        for statement in (
            "SELECT count(*) FROM nav_ingestion_attempts",
            "SELECT count(*) FROM fund_nav_data_revisions",
            "SELECT count(*) FROM nav_instrument_policy_evidence",
            "SELECT count(*) FROM fund_nav_reexpression_holds",
            "SELECT count(*) FROM nav_rebase_receipts",
            "DELETE FROM fund_nav_readiness_current",
            "UPDATE nav_policy_current SET policy_version = policy_version",
            "INSERT INTO nav_timeseries (instrument_id, nav_date) VALUES (gen_random_uuid(), CURRENT_DATE)",
            "SELECT nav_level_evidence_digest_v1(CURRENT_DATE, 1, 1, 'a', 'b', 'c', 'd')",
            "CREATE TABLE app_owned (x int)",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(statement)
            conn.rollback()
            conn.execute("SET ROLE app_runtime")


def test_missing_expected_grants_are_repairable_by_the_ddl(
    test_dsn, schema, monkeypatch, capsys
):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute("REVOKE SELECT ON nav_policy_current FROM app_runtime")
        conn.execute(
            f"REVOKE EXECUTE ON FUNCTION {SNAPSHOT}(uuid,uuid,timestamptz) FROM app_runtime"
        )
    report = _check(test_dsn, schema)
    assert (report["compatibility"], report["access"], report["ready"]) == (
        "exact",
        "repairable",
        False,
    )
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    base = [
        "--schema",
        schema,
        "--expected-sql-sha256",
        hashlib.sha256(DDL).hexdigest(),
    ]
    assert operator.main(base) == 0
    plan = json.loads(capsys.readouterr().out)["plan_sha256"]
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", plan]) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["ddl"], out["access"]) == ("applied", "exact")
    assert _check(test_dsn, schema)["ready"] is True


UNSAFE_GRANTS = {
    "dml_on_read_model": ["GRANT INSERT ON fund_nav_readiness_v1 TO app_runtime"],
    "truncate": ["GRANT TRUNCATE ON nav_policy_current TO app_runtime"],
    "private_evidence_select": [
        "GRANT SELECT ON nav_ingestion_attempts TO app_runtime"
    ],
    "private_column_select": [
        "GRANT SELECT (instrument_id) ON fund_nav_data_revisions TO app_runtime"
    ],
    "read_model_column_update": [
        "GRANT UPDATE (state) ON fund_nav_readiness_runs TO app_runtime"
    ],
    "public_select": ["GRANT SELECT ON fund_nav_readiness_v1 TO PUBLIC"],
    "grant_option": [
        "GRANT SELECT ON nav_policy_versions TO app_runtime WITH GRANT OPTION"
    ],
    "sequence_usage": [
        "GRANT USAGE ON SEQUENCE fund_nav_data_revisions_revision_id_seq TO app_runtime"
    ],
    "schema_create": ["GRANT CREATE ON SCHEMA {schema} TO app_runtime"],
    "helper_execute": [
        "GRANT EXECUTE ON FUNCTION nav_level_evidence_digest_v1"
        "(date,numeric,numeric,text,text,text,text) TO app_runtime"
    ],
}


@pytest.mark.parametrize("case", sorted(UNSAFE_GRANTS))
def test_extra_privileges_are_unsafe_and_block_before_ddl(
    test_dsn, schema, monkeypatch, capsys, case
):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        for statement in UNSAFE_GRANTS[case]:
            conn.execute(
                statement.format(schema=sql.Identifier(schema).as_string(conn))
            )
        signature_before = operator._check(conn, schema)["access_sha256"]
    report = _check(test_dsn, schema)
    assert report["ready"] is False
    assert report["access"] == "unsafe" or report["compatibility"] == "incompatible"
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    base = [
        "--schema",
        schema,
        "--expected-sql-sha256",
        hashlib.sha256(DDL).hexdigest(),
    ]
    assert operator.main([*base, "--mode", "apply", "--plan-sha256", "0" * 64]) == 3
    out = json.loads(capsys.readouterr().out)
    assert out["ddl"] == "not_attempted" and out["dml_committed"] is False
    assert out["code"] in ("blocked_access", "incompatible_schema")
    assert (
        _check(test_dsn, schema)["access_sha256"] == signature_before
    )  # not normalized


@pytest.mark.parametrize(
    "edge", ["inherit", "set_only", "transitive", "owner", "admin_option"]
)
def test_role_membership_escapes_are_unsafe(test_dsn, schema, edge):
    _bootstrap(test_dsn, schema)
    mid = "nav_mid_" + uuid.uuid4().hex[:10]
    grants = {
        "inherit": ["GRANT worker_writer TO app_runtime WITH INHERIT TRUE, SET TRUE"],
        "set_only": ["GRANT worker_writer TO app_runtime WITH INHERIT FALSE, SET TRUE"],
        "transitive": [
            f"CREATE ROLE {mid} NOLOGIN",
            f"GRANT worker_writer TO {mid}",
            f"GRANT {mid} TO app_runtime WITH INHERIT FALSE, SET FALSE",
        ],
        "owner": [
            f"GRANT {mid} TO app_runtime",
            f"ALTER TABLE nav_policy_current OWNER TO {mid}",
        ],
        "admin_option": [
            f"CREATE ROLE {mid} NOLOGIN",
            f"GRANT {mid} TO app_runtime WITH ADMIN TRUE, INHERIT FALSE, SET FALSE",
        ],
    }[edge]
    if edge == "owner":
        grants.insert(0, f"CREATE ROLE {mid} NOLOGIN")
    try:
        with _connect(test_dsn, schema, autocommit=True) as conn:
            for statement in grants:
                conn.execute(statement)
            report = operator._check(conn, schema)
        assert report["access"] == "unsafe" and report["ready"] is False
    finally:
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute("REVOKE worker_writer FROM app_runtime")
            if conn.execute(
                "SELECT 1 FROM pg_roles WHERE rolname=%s", (mid,)
            ).fetchone():
                conn.execute(f"REVOKE {mid} FROM app_runtime")
                conn.execute(
                    sql.SQL(
                        "ALTER TABLE {}.nav_policy_current OWNER TO CURRENT_USER"
                    ).format(sql.Identifier(schema))
                )
                conn.execute(f"DROP ROLE {mid}")


def test_missing_role_is_blocked_not_created(test_dsn, schema, monkeypatch, capsys):
    _bootstrap(test_dsn, schema)
    manifest = operator.load_manifest(DDL)
    monkeypatch.setattr(operator, "ACCESS_ROLE", "nav_absent_" + uuid.uuid4().hex[:8])
    monkeypatch.setattr(operator, "load_manifest", lambda _ddl: manifest)
    report = _check(test_dsn, schema)
    assert (report["access"], report["code"], report["ready"]) == (
        "role_missing",
        "blocked_role_missing",
        False,
    )
    assert set(report["dependencies"].values()) == {"role_missing"}
    monkeypatch.setenv("NAV_READINESS_DATABASE_URL", test_dsn)
    base = [
        "--schema",
        schema,
        "--expected-sql-sha256",
        hashlib.sha256(DDL).hexdigest(),
    ]
    assert operator.main(base) == 3
    assert json.loads(capsys.readouterr().out)["code"] == "blocked_role_missing"
    with psycopg.connect(test_dsn) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM pg_roles WHERE rolname=%s",
                (operator.ACCESS_ROLE,),
            ).fetchone()[0]
            == 0
        )


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        ("DROP MATERIALIZED VIEW fund_risk_latest_mv", "missing"),
        ("REVOKE SELECT ON fund_risk_latest_mv FROM app_runtime", "select_missing"),
        ("GRANT INSERT ON fund_risk_latest_mv TO app_runtime", "writable"),
    ],
)
def test_external_mv_dependency_blocks_publication_readiness(
    test_dsn, schema, statement, expected
):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute(statement)
        report = operator._check(conn, schema)
    assert report["dependencies"]["fund_risk_latest_mv"] == expected
    assert (report["compatibility"], report["access"]) == ("exact", "exact")
    assert (report["ready"], report["code"]) == (False, "blocked_dependency")


def test_nav_timeseries_dependency_write_grant_blocks(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute("GRANT UPDATE ON nav_timeseries TO app_runtime")
        report = operator._check(conn, schema)
    assert report["dependencies"]["nav_timeseries"] == "writable"
    assert report["ready"] is False


# ── R2-C F2: effective write-class privileges on read dependencies ──────────
DEPENDENCY_GRANTS = [
    ("GRANT UPDATE (nav) ON {rel} TO app_runtime", "writable"),
    ("GRANT INSERT (instrument_id) ON {rel} TO app_runtime", "writable"),
    ("GRANT REFERENCES (instrument_id) ON {rel} TO app_runtime", "writable"),
    ("GRANT REFERENCES ON {rel} TO app_runtime", "writable"),
    ("GRANT TRIGGER ON {rel} TO app_runtime", "writable"),
    ("GRANT MAINTAIN ON {rel} TO app_runtime", "writable"),
    ("GRANT TRUNCATE ON {rel} TO app_runtime", "writable"),
    ("GRANT DELETE ON {rel} TO PUBLIC", "writable"),
    ("GRANT SELECT ON {rel} TO app_runtime WITH GRANT OPTION", "grantable"),
    (
        "GRANT SELECT (instrument_id) ON {rel} TO app_runtime WITH GRANT OPTION",
        "grantable",
    ),
]


@pytest.mark.parametrize("relation", ["nav_timeseries", "fund_risk_latest_mv"])
@pytest.mark.parametrize(("grant", "expected"), DEPENDENCY_GRANTS)
def test_dependency_write_class_privileges_block(
    test_dsn, schema, relation, grant, expected
):
    _bootstrap(test_dsn, schema)
    column = "calc_date" if relation == "fund_risk_latest_mv" else "nav"
    with _connect(test_dsn, schema, autocommit=True) as conn:
        assert operator._check(conn, schema)["dependencies"][relation] == "ok"
        conn.execute(grant.format(rel=relation).replace("(nav)", f"({column})"))
        before = conn.execute(
            "SELECT relacl::text FROM pg_class WHERE oid = %s::regclass", (relation,)
        ).fetchone()[0]
        report = operator._check(conn, schema)
        # Detection only: the operator never revokes anything.
        assert (
            conn.execute(
                "SELECT relacl::text FROM pg_class WHERE oid = %s::regclass",
                (relation,),
            ).fetchone()[0]
            == before
        )
    assert report["dependencies"][relation] == expected
    assert (report["ready"], report["code"]) == (False, "blocked_dependency")


@pytest.mark.parametrize("membership", ["inherit", "set_only"])
def test_dependency_privilege_through_reachable_role_blocks(
    test_dsn, schema, membership
):
    _bootstrap(test_dsn, schema)
    mid = "nav_dep_mid_" + uuid.uuid4().hex[:8]
    top = "nav_dep_top_" + uuid.uuid4().hex[:8]
    option = (
        "INHERIT TRUE, SET FALSE"
        if membership == "inherit"
        else "INHERIT FALSE, SET TRUE"
    )
    with _connect(test_dsn, schema, autocommit=True) as conn:
        try:
            conn.execute(f"CREATE ROLE {mid} NOLOGIN")
            conn.execute(f"CREATE ROLE {top} NOLOGIN")
            # app_runtime -> mid -> top (transitive); top holds column UPDATE.
            conn.execute(f"GRANT {mid} TO app_runtime WITH {option}")
            conn.execute(f"GRANT {top} TO {mid} WITH {option}")
            conn.execute(
                f"GRANT UPDATE (nav_quality_ok) ON fund_risk_latest_mv TO {top}"
            )
            report = operator._check(conn, schema)
            assert report["dependencies"]["fund_risk_latest_mv"] == "writable"
            assert report["ready"] is False
        finally:
            conn.execute(
                f"REVOKE UPDATE (nav_quality_ok) ON fund_risk_latest_mv FROM {top}"
            )
            conn.execute(f"DROP ROLE IF EXISTS {top}")
            conn.execute(f"DROP ROLE IF EXISTS {mid}")


# ── R2-C F7: consumed columns of the risk read model ────────────────────────
@pytest.mark.parametrize(
    ("projection", "expected"),
    [
        ("instrument_id, calc_date, nav_quality_ok", "ok"),
        ("instrument_id, calc_date", "column_mismatch"),
        (
            "instrument_id, calc_date, nav_quality_ok::text AS nav_quality_ok",
            "column_mismatch",
        ),
        (
            "instrument_id::text AS instrument_id, calc_date, nav_quality_ok",
            "column_mismatch",
        ),
        (
            "instrument_id, calc_date::timestamp AS calc_date, nav_quality_ok",
            "column_mismatch",
        ),
    ],
)
def test_risk_read_model_requires_consumed_columns_and_types(
    test_dsn, schema, projection, expected
):
    _bootstrap(test_dsn, schema)
    with _connect(test_dsn, schema, autocommit=True) as conn:
        conn.execute("DROP MATERIALIZED VIEW fund_risk_latest_mv")
        conn.execute(
            f"CREATE MATERIALIZED VIEW fund_risk_latest_mv AS SELECT DISTINCT ON "
            f"(instrument_id) {projection} FROM fund_risk_metrics "
            f"WHERE organization_id IS NULL ORDER BY instrument_id, calc_date DESC"
        )
        conn.execute("GRANT SELECT ON fund_risk_latest_mv TO app_runtime")
        report = operator._check(conn, schema)
    assert report["dependencies"]["fund_risk_latest_mv"] == expected
    assert report["ready"] is (expected == "ok")
    # External, not owned: the dependency never enters the W1 catalog hash.
    assert report["catalog_sha256"] == report["reference_catalog_sha256"]


def test_homonymous_grants_in_other_schema_do_not_mask_or_satisfy(test_dsn, schema):
    _bootstrap(test_dsn, schema)
    other = schema + "_ho"
    try:
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(other)))
            conn.execute(
                sql.SQL("CREATE TABLE {}.nav_ingestion_attempts (x int)").format(
                    sql.Identifier(other)
                )
            )
            conn.execute(
                sql.SQL("GRANT ALL ON {}.nav_ingestion_attempts TO app_runtime").format(
                    sql.Identifier(other)
                )
            )
        assert _check(test_dsn, schema)["access"] == "exact"
        with _connect(test_dsn, schema, autocommit=True) as conn:
            conn.execute("REVOKE SELECT ON nav_policy_versions FROM app_runtime")
            conn.execute(
                sql.SQL(
                    "GRANT SELECT ON {}.nav_ingestion_attempts TO app_runtime"
                ).format(sql.Identifier(other))
            )
            assert operator._check(conn, schema)["access"] == "repairable"
    finally:
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(other)
                )
            )


def test_access_classifier_is_pure_and_never_normalizes_extras():
    manifest = operator.load_manifest(DDL)
    expected = manifest["access_profile"]["signature"]
    assert operator.classify_access(expected, expected) == ("exact", [])
    assert operator.classify_access(
        expected, {"role": {"name": "app_runtime", "exists": False}}
    )[0] == ("role_missing")
    extra = json.loads(json.dumps(expected))
    extra["relations"]["nav_policy_current"]["effective"].append("UPDATE")
    assert operator.classify_access(expected, extra)[0] == "unsafe"
    privileged = json.loads(json.dumps(expected))
    privileged["role"]["bypassrls"] = True
    assert operator.classify_access(expected, privileged)[0] == "unsafe"
    missing = json.loads(json.dumps(expected))
    missing["relations"]["nav_policy_current"]["effective"] = []
    missing["relations"]["nav_policy_current"]["direct"] = []
    missing["relations"]["nav_policy_current"]["column_effective"] = []
    assert operator.classify_access(expected, missing)[0] == "repairable"
    no_usage = json.loads(json.dumps(missing))
    no_usage["schema"]["usage"] = False
    assert operator.classify_access(expected, no_usage)[0] == "incomplete"


def test_generator_refuses_without_safe_fixture_role(test_dsn, monkeypatch):
    from scripts import generate_fund_nav_readiness_catalog as generator

    monkeypatch.setattr(operator, "ACCESS_ROLE", "nav_absent_" + uuid.uuid4().hex[:8])
    with pytest.raises(ValueError, match="fixture_role_missing"):
        generator.fresh_signatures(test_dsn)
    monkeypatch.setattr(operator, "ACCESS_ROLE", "worker_writer")
    try:
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute("GRANT pg_read_all_data TO worker_writer")
        with pytest.raises(ValueError, match="fixture_role_unsafe"):
            generator.fresh_signatures(test_dsn)
    finally:
        with psycopg.connect(test_dsn, autocommit=True) as conn:
            conn.execute("REVOKE pg_read_all_data FROM worker_writer")
