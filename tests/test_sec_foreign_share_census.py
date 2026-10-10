"""Census storage/API contracts in disposable loopback PostgreSQL.

Set SEC_TEST_DATABASE_URL; run this file alone with PYTEST_WORKERS=2.
Parser fixtures and reconciliation tests live in their dedicated test files.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "schemas" / "sec_foreign_share_census_v1.sql"
ROLLBACK = ROOT / "schemas" / "sec_foreign_share_census_v1.rollback.sql"
READERS = ("app_runtime", "app_analytics_ro", "mcp_ro")


@pytest.fixture(scope="module")
def sql_database():
    dsn = os.environ.get("SEC_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SEC_TEST_DATABASE_URL not set (disposable local Postgres only)")
    psycopg = pytest.importorskip("psycopg")
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    if info.get("host") not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail("SEC_TEST_DATABASE_URL must use a disposable loopback database")
    if info.get("port") == "65432" or info.get("user") in {*READERS, "worker_writer"}:
        pytest.fail("Refusing production/reader connection for SQL tests")
    with psycopg.connect(dsn, autocommit=True) as conn:
        from psycopg import sql

        for role in ("worker_writer", *READERS):
            if not conn.execute("SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=%s", [role]).fetchone():
                conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        conn.execute(MIGRATION.read_text(encoding="utf-8"))
        yield conn


@pytest.fixture
def db(sql_database):
    sql_database.execute("BEGIN")
    sql_database.execute("TRUNCATE public.sec_foreign_share_census, public.sec_foreign_share_census_sources")
    try:
        yield sql_database
    finally:
        sql_database.execute("ROLLBACK")


def add(db, **overrides):
    from psycopg import sql
    from psycopg.types.json import Jsonb

    row = {
        "fact_hash": uuid4().hex,
        "cik": 1846832,
        "adsh": "0000950170-25-058197",
        "form": "20-F",
        "filed": "2025-04-15",
        "accepted": "2025-04-15T09:00:00Z",
        "period_end": "2024-12-31",
        "shares_as_of": "2024-12-31",
        "date_explicit": True,
        "classes": [
            {"class_name": "Class A common shares", "class_key": "class:a", "class_kind": "ordinary", "shares": 151420944},
            {"class_name": "Class B common shares", "class_key": "class:b", "class_kind": "ordinary", "shares": 134054192},
        ],
        "stated_total": 285475136,
        "computed_total": 285475136,
        "complete": True,
        "conflicting": False,
        "reasons": [],
        "cross_checks": {},
        "source_url": "https://www.sec.gov/Archives/edgar/data/1846832/test.htm",
        "source_sha256": "a" * 64,
        "evidence_text": "Class A common shares: 151,420,944; Class B common shares: 134,054,192.",
        "evidence_location": "cover:outstanding-shares",
        "parser_version": "test-census-v1",
        "available_on": "2025-04-16",
        "loaded_on": "2026-10-10",
        "source_package": uuid4().hex,
    }
    row.update(overrides)
    # Follow filed+1 unless the test explicitly supplies a reconciliation clock.
    if "filed" in overrides and "available_on" not in overrides:
        row["available_on"] = (dt.date.fromisoformat(row["filed"]) + dt.timedelta(days=1)).isoformat()
    for key in ("classes", "cross_checks"):
        row[key] = Jsonb(row[key])
    statement = sql.SQL("INSERT INTO public.sec_foreign_share_census ({}) VALUES ({}) RETURNING id").format(
        sql.SQL(",").join(map(sql.Identifier, row)),
        sql.SQL(",").join(sql.Placeholder() for _ in row),
    )
    return db.execute(statement, list(row.values())).fetchone()[0]


def at(db, day, *, cik=1846832):
    return db.execute(
        "SELECT id,status,complete,conflicting,shares_as_of,classes,stated_total,computed_total,"
        "cross_checks,source_available_on,available_on,source_sha256,evidence_text,parser_version "
        "FROM public.sec_foreign_share_census_at(%s,%s::date)",
        [cik, day],
    ).fetchone()


def test_source_availability_and_full_class_provenance(db):
    census_id = add(db)
    assert at(db, "2025-04-15") is None
    row = at(db, "2025-04-16")
    assert row[:5] == (census_id, "complete", True, False, dt.date(2024, 12, 31))
    assert [entry["shares"] for entry in row[5]] == [151420944, 134054192]
    assert row[6:9] == (285475136, 285475136, {})
    assert row[9:11] == (dt.date(2025, 4, 16), dt.date(2025, 4, 16))
    assert row[11] == "a" * 64 and "Class B" in row[12] and row[13] == "test-census-v1"
    assert at(db, "2026-12-31", cik=999) is None


def test_republication_floor_and_reconciliation_both_gate_visibility(db):
    census_id = add(db, publication_floor_on="2025-05-20", available_on="2025-06-02")
    assert at(db, "2025-05-19") is None
    assert at(db, "2025-06-01") is None
    row = at(db, "2025-06-02")
    assert row[0] == census_id
    assert row[9:11] == (dt.date(2025, 5, 20), dt.date(2025, 6, 2))


@pytest.mark.parametrize("reason", [None, "source", "parser_correction"])
def test_retirement_distinguishes_changed_source_from_corrected_reading(db, reason):
    census_id = add(db, retired_on="2025-06-02", retired_reason=reason)
    prior = at(db, "2025-04-16")
    assert (prior is None) == (reason == "parser_correction")
    if prior:
        assert prior[0] == census_id
    assert at(db, "2025-06-02") is None


def test_parser_correction_restates_public_history_but_source_change_does_not(db):
    add(db, retired_on="2026-10-10", retired_reason="parser_correction", complete=False)
    corrected = add(db, parser_version="test-census-v2", available_on="2025-04-16")
    assert at(db, "2025-04-16")[0] == corrected
    db.execute("UPDATE public.sec_foreign_share_census SET retired_on='2026-10-11', retired_reason='source' WHERE id=%s", [corrected])
    replacement = add(db, source_sha256="b" * 64, publication_floor_on="2026-10-11", available_on="2026-10-11")
    assert at(db, "2026-10-10")[0] == corrected
    assert at(db, "2026-10-11")[0] == replacement


def test_statement_date_precedes_filing_order_and_future_counts_are_excluded(db):
    newer_date = add(db, shares_as_of="2025-01-31")
    add(db, filed="2025-05-01", shares_as_of="2024-12-31")
    assert at(db, "2025-05-02")[0] == newer_date
    future = add(db, filed="2025-05-03", shares_as_of="2025-08-31")
    assert at(db, "2025-08-30")[0] == newer_date
    assert at(db, "2025-08-31")[0] == future


def test_filing_order_uses_acceptance_then_accession(db):
    add(db, accepted="2025-04-15T09:00:00Z")
    later = add(db, accepted="2025-04-15T10:00:00Z", adsh="0000950170-25-058198")
    add(db, accepted=None, adsh="0000950170-25-058200")
    assert at(db, "2025-04-16")[0] == later
    last = add(db, accepted="2025-04-15T10:00:00Z", adsh="0000950170-25-058199")
    assert at(db, "2025-04-16")[0] == last


@pytest.mark.parametrize("complete,conflicting,status", [(False, False, "incomplete"), (True, True, "conflicting"), (False, True, "conflicting")])
def test_latest_refusal_never_falls_back_to_older_complete_census(db, complete, conflicting, status):
    add(db)
    latest = add(db, filed="2026-04-15", shares_as_of="2025-12-31", complete=complete, conflicting=conflicting,
                 cross_checks={"issuer_total": {"status": "mismatch", "w1_shares": 100}}, reasons=["xbrl_total_mismatch"])
    row = at(db, "2026-04-16")
    assert row[:4] == (latest, status, complete, conflicting)
    assert row[8]["issuer_total"]["w1_shares"] == 100


def test_unknown_statement_date_is_a_visible_incomplete_control(db):
    add(db)
    latest = add(db, filed="2026-04-15", period_end="2025-12-31", shares_as_of=None, date_explicit=False,
                 complete=False, computed_total=None, classes=[], reasons=["statement_date_unavailable"])
    assert at(db, "2026-04-16")[:5] == (latest, "incomplete", False, False, None)


def test_nil_preferred_and_deferred_classes_remain_in_returned_census(db):
    classes = [
        {"class_name": "Ordinary shares", "class_key": "ordinary", "class_kind": "ordinary", "shares": 100},
        {"class_name": "Series A preference shares", "class_key": "series:a", "class_kind": "preferred", "shares": 0},
        {"class_name": "Deferred shares", "class_key": None, "class_kind": "other", "shares": 12},
    ]
    add(db, classes=classes, stated_total=112, computed_total=112)
    assert at(db, "2025-04-16")[5] == classes


@pytest.mark.parametrize("overrides", [
    {"available_on": "2025-04-15"},
    {"publication_floor_on": "2025-05-20", "available_on": "2025-05-19"},
    {"retired_reason": "parser_correction"},
    {"retired_reason": "other", "retired_on": "2026-10-10"},
    {"classes": {}},
    {"classes": []},
    {"shares_as_of": None, "date_explicit": False},
    {"stated_total": 100},
    {"computed_total": None},
    {"computed_total": "NaN"},
    {"computed_total": "Infinity"},
    {"stated_total": -1},
    {"computed_total": 1.5},
    {"evidence_text": " "},
    {"form": "6-K"},
])
def test_storage_rejects_unusable_complete_or_invalid_provenance(db, overrides):
    psycopg = pytest.importorskip("psycopg")
    db.execute("SAVEPOINT invalid_census")
    with pytest.raises(psycopg.errors.CheckViolation):
        add(db, **overrides)
    db.execute("ROLLBACK TO SAVEPOINT invalid_census")


def test_missing_statement_is_source_metadata_only(db):
    db.execute(
        "INSERT INTO public.sec_foreign_share_census_sources "
        "(source_package,adsh,cik,source_url,source_sha256,parser_version,first_loaded_on,last_loaded_on,census_count) "
        "VALUES ('empty','0000950170-25-058197',1846832,'https://www.sec.gov/test',%s,'test-v1','2026-10-10','2026-10-10',0)",
        ["a" * 64],
    )
    assert at(db, "2026-10-10") is None
    assert db.execute("SELECT census_count FROM public.sec_foreign_share_census_sources WHERE source_package='empty'").fetchone() == (0,)


def test_census_api_inlines_without_security_or_search_path_configuration(db):
    add(db)
    plan = db.execute(
        "EXPLAIN (VERBOSE, COSTS OFF) SELECT c.status,c.classes FROM public.sec_foreign_share_census_at(1846832,'2025-04-16') c"
    ).fetchall()
    assert not any("Function Scan" in line[0] for line in plan)
    assert any("sec_foreign_share_census" in line[0] for line in plan)
    assert db.execute(
        "SELECT l.lanname,p.provolatile,p.proconfig,p.prosecdef,p.proparallel "
        "FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang "
        "WHERE p.oid='public.sec_foreign_share_census_at(bigint,date)'::regprocedure"
    ).fetchone() == ("sql", "s", None, False, "s")


def test_owner_and_reader_grants_allow_only_reads(db):
    from psycopg import sql
    psycopg = pytest.importorskip("psycopg")

    add(db)
    assert db.execute(
        "SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class "
        "WHERE oid IN ('public.sec_foreign_share_census'::regclass,"
        "'public.sec_foreign_share_census_sources'::regclass,'public.sec_foreign_share_census_id_seq'::regclass)"
    ).fetchall() == [("worker_writer",)] * 3
    assert db.execute(
        "SELECT pg_catalog.pg_get_userbyid(proowner) FROM pg_catalog.pg_proc "
        "WHERE oid='public.sec_foreign_share_census_at(bigint,date)'::regprocedure"
    ).fetchone() == ("worker_writer",)
    for role in READERS:
        for table in ("sec_foreign_share_census", "sec_foreign_share_census_sources"):
            assert db.execute("SELECT pg_catalog.has_table_privilege(%s,%s,'SELECT')", [role, "public." + table]).fetchone() == (True,)
            for privilege in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"):
                assert db.execute("SELECT pg_catalog.has_table_privilege(%s,%s,%s)", [role, "public." + table, privilege]).fetchone() == (False,)
        assert db.execute(
            "SELECT pg_catalog.has_function_privilege(%s,'public.sec_foreign_share_census_at(bigint,date)','EXECUTE'),"
            "pg_catalog.has_sequence_privilege(%s,'public.sec_foreign_share_census_id_seq','USAGE')", [role, role]
        ).fetchone() == (True, False)
        db.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
        assert at(db, "2025-04-16")[1] == "complete"
        db.execute("SAVEPOINT reader_write")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            db.execute("DELETE FROM public.sec_foreign_share_census")
        db.execute("ROLLBACK TO SAVEPOINT reader_write")
        db.execute("RESET ROLE")
    # PUBLIC is represented by grantee=0, including the default function ACL.
    assert db.execute(
        "SELECT count(*) FROM pg_catalog.pg_class c CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a "
        "WHERE c.oid IN ('public.sec_foreign_share_census'::regclass,"
        "'public.sec_foreign_share_census_sources'::regclass,'public.sec_foreign_share_census_id_seq'::regclass) AND a.grantee=0"
    ).fetchone() == (0,)
    assert db.execute(
        "SELECT count(*) FROM pg_catalog.pg_proc p CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a "
        "WHERE p.oid='public.sec_foreign_share_census_at(bigint,date)'::regprocedure AND a.grantee=0"
    ).fetchone() == (0,)


def test_migration_cycle_is_additive_idempotent_and_removes_only_census(sql_database):
    from psycopg import sql

    conn = sql_database
    schema = "census_cycle_" + uuid4().hex
    migration = MIGRATION.read_text(encoding="utf-8").replace("public.", schema + ".")
    rollback = ROLLBACK.read_text(encoding="utf-8").replace("public.", schema + ".")
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        conn.execute(sql.SQL("CREATE TABLE {} (proof text)").format(sql.Identifier(schema, "existing_w1_evidence")))
        conn.execute(sql.SQL("INSERT INTO {} VALUES ('keep')").format(sql.Identifier(schema, "existing_w1_evidence")))
        conn.execute(migration)
        conn.execute(sql.SQL("INSERT INTO {} (source_package,adsh,cik,source_url,source_sha256,parser_version,first_loaded_on,last_loaded_on,census_count) VALUES ('empty','0000950170-25-058197',1846832,'https://www.sec.gov/test',%s,'test-v1','2026-10-10','2026-10-10',0)").format(sql.Identifier(schema, "sec_foreign_share_census_sources")), ["a" * 64])
        relation = schema + ".sec_foreign_share_census_sources"
        before = conn.execute("SELECT oid,relfilenode,relowner,relacl FROM pg_catalog.pg_class WHERE oid=%s::regclass", [relation]).fetchone()
        api_before = conn.execute("SELECT oid,prosrc,proowner,proacl,proconfig FROM pg_catalog.pg_proc WHERE oid=%s::regprocedure", [schema + ".sec_foreign_share_census_at(bigint,date)"]).fetchone()
        conn.execute(migration)
        assert conn.execute("SELECT oid,relfilenode,relowner,relacl FROM pg_catalog.pg_class WHERE oid=%s::regclass", [relation]).fetchone() == before
        assert conn.execute("SELECT oid,prosrc,proowner,proacl,proconfig FROM pg_catalog.pg_proc WHERE oid=%s::regprocedure", [schema + ".sec_foreign_share_census_at(bigint,date)"]).fetchone() == api_before
        assert conn.execute(sql.SQL("SELECT census_count FROM {}").format(sql.Identifier(schema, "sec_foreign_share_census_sources"))).fetchall() == [(0,)]
        conn.execute(rollback)
        conn.execute(rollback)
        assert conn.execute("SELECT pg_catalog.to_regclass(%s),pg_catalog.to_regclass(%s),pg_catalog.to_regprocedure(%s)", [schema + ".sec_foreign_share_census", relation, schema + ".sec_foreign_share_census_at(bigint,date)"]).fetchone() == (None, None, None)
        assert conn.execute(sql.SQL("SELECT proof FROM {}").format(sql.Identifier(schema, "existing_w1_evidence"))).fetchone() == ("keep",)
        conn.execute(migration)
        assert conn.execute("SELECT pg_catalog.to_regclass(%s) IS NOT NULL", [relation]).fetchone() == (True,)
    finally:
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_migration_replaces_reader_default_write_grants(sql_database):
    from psycopg import sql

    conn = sql_database
    schema = "census_grants_" + uuid4().hex
    migration = MIGRATION.read_text(encoding="utf-8").replace("public.", schema + ".")
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        for role in READERS:
            conn.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT ALL ON TABLES TO {}").format(sql.Identifier(schema), sql.Identifier(role)))
            conn.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT ALL ON SEQUENCES TO {}").format(sql.Identifier(schema), sql.Identifier(role)))
        conn.execute(migration)
        for role in READERS:
            assert conn.execute("SELECT pg_catalog.has_table_privilege(%s,%s,'SELECT'),pg_catalog.has_table_privilege(%s,%s,'INSERT,UPDATE,DELETE,TRUNCATE'),pg_catalog.has_sequence_privilege(%s,%s,'USAGE,SELECT,UPDATE')", [role, schema + ".sec_foreign_share_census", role, schema + ".sec_foreign_share_census", role, schema + ".sec_foreign_share_census_id_seq"]).fetchone() == (True, False, False)
    finally:
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_new_migrations_and_tests_are_lf_only():
    for path in (MIGRATION, ROLLBACK, Path(__file__)):
        assert b"\r" not in path.read_bytes(), path.name
