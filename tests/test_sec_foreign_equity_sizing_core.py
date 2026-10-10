"""W1c core composition, loader guard and exact B2 rollback on loopback PG18."""
from __future__ import annotations

from datetime import date
import hashlib
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

import pytest

from scripts import load_sec_foreign_listing_evidence as loader
import test_sec_foreign_listing_evidence as foreign
import test_sec_foreign_listing_loader as loader_tests

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "schemas"
MIGRATION = (SCHEMAS / "sec_foreign_equity_sizing_v1.sql").read_text(encoding="utf-8")
ROLLBACK = (SCHEMAS / "sec_foreign_equity_sizing_v1.rollback.sql").read_text(encoding="utf-8")
LEGACY_SIGNATURE = "public.sec_foreign_listing_at(bigint,text,date)"
CORE_SIGNATURE = "public.sec_foreign_listing_context_at(bigint,text,date,date)"
ELECTION_SIGNATURE = "public.sec_foreign_listing_election_at(bigint,text,date,date)"


def _body(schema: str, name: str) -> str:
    match = re.search(r"CREATE OR REPLACE FUNCTION (?:public\.)?" + name + r"\(.*?AS \$fn\$(.*?)\$fn\$;", schema, re.DOTALL)
    assert match is not None
    return match.group(1)


def test_rollback_sources_are_exact_v2_and_v3_bodies():
    v2 = (SCHEMAS / "sec_foreign_listing_evidence_v2.sql").read_text(encoding="utf-8")
    w3 = (SCHEMAS / "sec_ticker_cik_history_v3.sql").read_text(encoding="utf-8")
    assert hashlib.md5(_body(ROLLBACK, "sec_foreign_listing_at").encode()).hexdigest() == "60f5d1bf86a645a41fb7e23328c7ab8a"
    assert _body(ROLLBACK, "sec_foreign_listing_at") == _body(v2, "sec_foreign_listing_at")
    for name in ("sec_cover_class_shares_at", "sec_cover_ticker_shares_at"):
        assert _body(ROLLBACK, name) == _body(w3, name)


@pytest.fixture(scope="module")
def core_database():
    dsn = os.environ.get("SEC_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SEC_TEST_DATABASE_URL is unset")
    assert urlsplit(dsn).hostname in {"127.0.0.1", "localhost", "::1"}, "Disposable loopback PostgreSQL only"
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert int(conn.execute("SHOW server_version_num").fetchone()[0]) >= 180000
        for name in ("sec_ticker_cik_history_v1.sql", "sec_ticker_cik_history_v2.sql",
                     "sec_ticker_cik_history_v3.sql", "sec_foreign_listing_evidence.sql",
                     "sec_foreign_listing_evidence_v2.sql"):
            conn.execute((SCHEMAS / name).read_text(encoding="utf-8"))
        originals = {signature: conn.execute(
            "SELECT pg_catalog.pg_get_functiondef(%s::regprocedure)", (signature,),
        ).fetchone()[0] for signature in (
            LEGACY_SIGNATURE,
            "public.sec_cover_class_shares_at(bigint,text,date,integer)",
            "public.sec_cover_ticker_shares_at(text,bigint,date,integer)",
        )}
        conn.execute(MIGRATION)
    return dsn, originals


@pytest.fixture
def db(core_database):
    psycopg = pytest.importorskip("psycopg")
    conn = psycopg.connect(core_database[0])
    conn.execute("TRUNCATE public.sec_foreign_listing_evidence, public.sec_foreign_listing_sources")
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


def _context(conn, effective: str, known: str):
    return conn.execute(
        "SELECT * FROM public.sec_foreign_listing_context_at(1046179,'TSM',%s::date,%s::date)",
        (effective, known),
    ).fetchone()


def _check_loader(conn):
    with conn.cursor() as cursor:
        loader.require_schema(cursor)


def test_core_economic_date_is_separate_from_knowledge_cutoff(db):
    foreign.add(db, filed="2019-12-31")
    foreign.add_ratio(db, filed="2019-12-31", effective="2020-01-01")
    foreign.add_ratio(db, source="securities_description", filed="2019-12-31", effective="2020-01-01")
    foreign.add_ratio(db, ratio=(10, 1), source="f6", filed="2021-05-15", effective="2021-06-01", adsh="0001193125-21-000002")
    foreign.add_ratio(db, ratio=(10, 1), source="ratio_change_6k", form="6-K", filed="2021-05-16",
                      effective="2021-06-01", adsh="0001193125-21-000003",
                      ratio_change_program_key="old_cusip:123456789")
    count_date = _context(db, "2021-01-01", "2021-12-31")
    current = _context(db, "2021-12-31", "2021-12-31")
    assert count_date[:6] == ("resolved", "ads", 5, 1, "resolved", "resolved")
    assert current[:6] == ("resolved", "ads", 10, 1, "resolved", "resolved")
    assert current[9:12] == ("old_cusip:123456789", date(2021, 6, 1), None)
    assert foreign.resolve(db, "2021-12-31") == current[:7]


def test_core_metadata_uses_elected_ratios_even_when_a_control_is_audited(db):
    foreign.add(db)
    foreign.add_ratio(db, underlying_class="class_b")
    foreign.add_ratio(db, source="securities_description", underlying_class="class_b")
    control = foreign.add_ratio(
        db, ratio=(20, 1), source="ratio_change_6k", form="6-K", underlying_class="class_a",
        filed="2020-04-01", effective="2020-06-01", adsh="0001193125-20-000002",
        ratio_effectiveness_pending=True, ratio_effectiveness_pending_text="Awaiting depositary notice",
        ratio_effectiveness_conditions=["depositary_notice"],
    )
    result = _context(db, "2020-12-31", "2020-12-31")
    assert result[0] == "ambiguous"
    assert control in result[6]
    assert result[7:9] == (None, "class_b")


def test_core_parser_correction_is_invisible_with_both_clocks(db):
    old = foreign.add(db, listed_type="ordinary_direct", retired="2026-10-10", retired_reason="parser_correction")
    result = _context(db, "2020-12-31", "2025-12-31")
    assert result[:7] == ("none", None, None, None, "none", "none", [])
    assert old not in result[6]


@pytest.mark.parametrize("signature", [LEGACY_SIGNATURE, CORE_SIGNATURE, ELECTION_SIGNATURE])
def test_loader_refuses_function_configuration_changes(db, signature):
    _check_loader(db)
    db.execute("ALTER FUNCTION " + signature + " SET search_path TO pg_catalog, public")
    with pytest.raises(RuntimeError, match="schema is not v2"):
        _check_loader(db)


@pytest.mark.parametrize("signature", [CORE_SIGNATURE, ELECTION_SIGNATURE])
def test_loader_refuses_public_execute_on_core(db, signature):
    _check_loader(db)
    db.execute("GRANT EXECUTE ON FUNCTION " + signature + " TO PUBLIC")
    with pytest.raises(RuntimeError, match="schema is not v2"):
        _check_loader(db)


def test_reapply_and_rollback_restore_exact_functions_keep_rows_and_loader_works(db, core_database):
    source = loader_tests._source("b2-sizing-roundtrip")
    fact = loader_tests._fact(source)
    applied = loader.apply_evidence(db, loader_tests._manifest(source), [fact], date(2026, 10, 10))
    assert applied["inserted"] == 1
    before = db.execute(
        "SELECT tableoid,ctid,id,fact_hash FROM public.sec_foreign_listing_evidence ORDER BY id"
    ).fetchall()
    relnodes = db.execute(
        "SELECT relname,relfilenode FROM pg_catalog.pg_class WHERE oid IN "
        "('public.sec_foreign_listing_evidence'::regclass,'public.sec_foreign_listing_sources'::regclass) ORDER BY relname"
    ).fetchall()
    for migration in (MIGRATION, MIGRATION, ROLLBACK):
        db.execute(migration.replace("BEGIN;", "").replace("COMMIT;", ""))
    for signature, original in core_database[1].items():
        assert db.execute("SELECT pg_catalog.pg_get_functiondef(%s::regprocedure)", (signature,)).fetchone() == (original,)
    for signature in (CORE_SIGNATURE, ELECTION_SIGNATURE):
        assert db.execute("SELECT pg_catalog.to_regprocedure(%s)", (signature,)).fetchone() == (None,)
    assert db.execute("SELECT tableoid,ctid,id,fact_hash FROM public.sec_foreign_listing_evidence ORDER BY id").fetchall() == before
    assert db.execute(
        "SELECT relname,relfilenode FROM pg_catalog.pg_class WHERE oid IN "
        "('public.sec_foreign_listing_evidence'::regclass,'public.sec_foreign_listing_sources'::regclass) ORDER BY relname"
    ).fetchall() == relnodes
    _check_loader(db)
    repeated = loader.apply_evidence(db, loader_tests._manifest(source), [fact], date(2026, 10, 10))
    assert repeated["inserted"] == 0
    assert db.execute("SELECT listed_type FROM public.sec_foreign_listing_at(123,'ABC','2025-12-31')").fetchone() == ("ordinary_direct",)


def test_competing_program_diagnostic_preserves_both_public_abis(db):
    foreign.add(db, underlying_class="class_a")
    foreign.add_ratio(db, underlying_class="class_a")
    foreign.add_ratio(db, source="securities_description", underlying_class="class_a")
    for index, program in enumerate(("old_cusip:123456789", "old_cusip:987654321"), 2):
        foreign.add_ratio(
            db, source="ratio_change_6k", form="6-K", underlying_class="class_a",
            filed="2020-04-01", effective="2020-06-01",
            adsh=f"0001193125-20-00000{index}", ratio_change_program_key=program,
        )
    public = _context(db, "2020-12-31", "2020-12-31")
    internal = db.execute(
        "SELECT * FROM public.sec_foreign_listing_election_at(1046179,'TSM','2020-12-31','2020-12-31')"
    ).fetchone()
    assert len(public) == 12 and len(internal) == 13
    assert public[:6] == ("resolved", "ads", 5, 1, "resolved", "resolved")
    assert public[7:10] == ("class_a", "class_a", None)
    assert internal[:12] == public
    assert internal[12] is True
    assert foreign.resolve(db, "2020-12-31") == public[:7]


def test_same_ratio_without_program_keys_is_distinct_from_competing_programs(db):
    foreign.add(db)
    foreign.add_ratio(db)
    foreign.add_ratio(db, source="securities_description")
    assert db.execute(
        "SELECT status,program_key,program_ambiguous FROM public.sec_foreign_listing_election_at(1046179,'TSM','2020-12-31','2020-12-31')"
    ).fetchone() == ("resolved", None, False)


def test_loader_refuses_same_election_body_with_changed_output_abi(db):
    from psycopg import sql
    definition, owner = db.execute(
        "SELECT pg_catalog.pg_get_functiondef(p.oid), pg_catalog.pg_get_userbyid(p.proowner) "
        "FROM pg_catalog.pg_proc p WHERE p.oid=%s::regprocedure", (ELECTION_SIGNATURE,),
    ).fetchone()
    db.execute("DROP FUNCTION " + ELECTION_SIGNATURE)
    db.execute(definition.replace("program_ambiguous boolean)", "wrong_program_ambiguous boolean)"))
    db.execute(sql.SQL("ALTER FUNCTION " + ELECTION_SIGNATURE + " OWNER TO {}").format(sql.Identifier(owner)))
    db.execute("REVOKE ALL ON FUNCTION " + ELECTION_SIGNATURE + " FROM PUBLIC")
    for role, in db.execute(
        "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname IN ('app_runtime','app_analytics_ro','mcp_ro')"
    ).fetchall():
        db.execute(sql.SQL("GRANT EXECUTE ON FUNCTION " + ELECTION_SIGNATURE + " TO {}").format(sql.Identifier(role)))
    with pytest.raises(RuntimeError, match="schema is not v2"):
        _check_loader(db)
