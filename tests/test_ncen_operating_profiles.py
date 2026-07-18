from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest

from src.ncen.schema import json_typed_projection, load_ncen_contract, parse_row


ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
NCEN_METADATA_SHA = "fb55228ca976c43955c9a49bccf2bc21c8b70d3c7194f936f13289f06acca737"


def _fixture(cur):
    schema = f"ncen_operating_profile_fixture_{uuid4().hex}"
    run_id, package_id, publication_id = uuid4(), uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
    cur.execute("""CREATE TABLE ncen_raw_v2_rows(
        raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ingestion_run_id uuid,
        source_table text, parse_status text, typed_projection jsonb, accession_number text, fund_id text)""")
    for ddl_name in ("sec_derived_publications.sql", "ncen_effective_views.sql", "ncen_operating_profiles.sql"):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    cur.execute("""INSERT INTO sec_derived_publications
        (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
        VALUES(%s,'ncen_operating_profile_v1',1,%s,%s,%s)""",
        (publication_id, run_id, package_id, "a" * 64))
    return schema, run_id, package_id, publication_id


def _raw(cur, run_id, table, body, *, accession=None, fund_id=None):
    cur.execute("""INSERT INTO ncen_raw_v2_rows
        (ingestion_run_id,source_table,parse_status,typed_projection,accession_number,fund_id)
        VALUES(%s,%s,'typed',%s::jsonb,%s,%s)""",
        (run_id, table, json.dumps(body), accession, fund_id))


def _submission(cur, run_id, accession, cik="C1", form="N-CEN", filing_date="2026-02-01", ending="2025-12-31"):
    _raw(cur, run_id, "SUBMISSION.tsv", {
        "ACCESSION_NUMBER": accession, "CIK": cik, "SUBMISSION_TYPE": form,
        "FILING_DATE": filing_date, "REPORT_ENDING_PERIOD": ending,
    }, accession=accession)


def _fund(cur, run_id, accession, fund_id, series_id, **fields):
    table = load_ncen_contract(NCEN_METADATA_SHA).table_for_filename("FUND_REPORTED_INFO.tsv")
    lexical = {name: "" for name in table.headers}
    lexical.update({"ACCESSION_NUMBER": accession, "FUND_ID": fund_id, "SERIES_ID": series_id, **fields})
    parsed = parse_row(table.columns, tuple(lexical[name] for name in table.headers))
    assert parsed.parse_status == "typed"
    body = json_typed_projection(parsed.typed)
    _raw(cur, run_id, "FUND_REPORTED_INFO.tsv", body, accession=accession, fund_id=fund_id)


def _prepare_second_run(cur):
    run_id, package_id = uuid4(), uuid4()
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    return run_id


def test_ncen_operating_profile_inherits_amendment_winner_and_preserves_provider_children():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, base_run, _, publication_id = _fixture(cur)
        amended_run = _prepare_second_run(cur)
        _submission(cur, base_run, "BASE", filing_date="2026-01-10")
        _submission(cur, amended_run, "AMEND", form="N-CEN/A", filing_date="2026-02-10")
        _fund(cur, amended_run, "AMEND", "F1", "S1", IS_SEC_LENDING_AUTHORIZED="Y",
              HAS_LINE_OF_CREDIT="Y", IS_ETF="Y")
        _raw(cur, amended_run, "ADVISER.tsv", {"FUND_ID": "F1", "ADVISER_NAME": "Alpha", "ADVISER_LEI": "L1"}, fund_id="F1")
        _raw(cur, amended_run, "ADVISER.tsv", {"FUND_ID": "F1", "ADVISER_NAME": "Beta", "ADVISER_LEI": "L2"}, fund_id="F1")
        _raw(cur, amended_run, "ADMIN.tsv", {"FUND_ID": "F1", "ADMIN_NAME": "Admin"}, fund_id="F1")
        _raw(cur, amended_run, "SEC_LENDING_IDEMNITY_PROVIDER.tsv", {"FUND_ID": "F1", "INDEMNITY_PROVIDER_NAME": "Indemnity"}, fund_id="F1")
        _raw(cur, amended_run, "LINE_OF_CREDIT_DETAIL.tsv", {"FUND_ID": "F1", "LINE_OF_CREDIT_SEQNUM": "1", "CREDIT_TYPE": "bank"}, fund_id="F1")
        _raw(cur, amended_run, "LINE_OF_CREDIT_INSTITUTION.tsv", {"FUND_ID": "F1", "LINE_OF_CREDIT_SEQNUM": "1", "CREDIT_INSTITUTION_NAME": "Bank"}, fund_id="F1")
        _raw(cur, amended_run, "ETF.tsv", {"FUND_ID": "F1", "SERIES_ID": "S1", "IS_FUND_IN_KIND_ETF": "Y"}, fund_id="F1")
        _raw(cur, amended_run, "AUTHORIZED_PARTICIPANT.tsv", {"FUND_ID": "F1", "PARTICIPANT_NAME": "AP"}, fund_id="F1")
        cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("""SELECT accession_number,series_id,service_providers_state,securities_lending_state,
                              liquidity_backstop_state,etf_primary_market_state,fund_structure_state,
                              provider_children,liquidity_backstop,etf_primary_market
                       FROM ncen_operating_profiles""")
        row = cur.fetchone()
        assert row[:7] == ("AMEND", "S1", "available", "available", "available", "available", "available")
        assert {child["source_table"] for child in row[7]} == {"ADVISER.tsv", "ADMIN.tsv", "SEC_LENDING_IDEMNITY_PROVIDER.tsv"}
        assert {child["evidence"]["ADVISER_NAME"] for child in row[7] if child["source_table"] == "ADVISER.tsv"} == {"Alpha", "Beta"}
        assert row[8]["credit_facilities"][0]["evidence"]["CREDIT_TYPE"] == "bank"
        assert row[9]["authorized_participants"][0]["evidence"]["PARTICIPANT_NAME"] == "AP"
        assert row[9]["is_etf"] is True
        cur.execute("SELECT fund_structure FROM ncen_operating_profiles")
        assert cur.fetchone()[0]["normalized"] == {"IS_ETF": True}
        cur.execute("SELECT ncen_conditional_positive_flag('Y'),ncen_conditional_positive_flag('N'),ncen_conditional_positive_flag('X'),ncen_conditional_positive_flag(NULL)")
        assert cur.fetchone() == (True, None, None, None)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_ncen_operating_profile_requires_exact_positive_etf_literal_and_child_evidence():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        _submission(cur, run_id, "A1")
        _fund(cur, run_id, "A1", "ETF-MISSING", "S1", IS_ETF="Y")
        _fund(cur, run_id, "A1", "UNDECLARED-NEGATIVE", "S2", IS_ETF="N")
        cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT fund_id,etf_primary_market_state,fund_structure_state FROM ncen_operating_profiles ORDER BY fund_id")
        assert cur.fetchall() == [("ETF-MISSING", "unavailable", "available"), ("UNDECLARED-NEGATIVE", "unavailable", "unavailable")]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_ncen_operating_profile_rejects_unexpected_etf_code_even_when_children_exist():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        _submission(cur, run_id, "A1")
        _fund(cur, run_id, "A1", "BAD-FLAG", "S1", IS_ETF="X", IS_INTERVAL="X", IS_SECONDARY_COMMON="X")
        _raw(cur, run_id, "ETF.tsv", {"FUND_ID": "BAD-FLAG", "SERIES_ID": "S1"}, fund_id="BAD-FLAG")
        _raw(cur, run_id, "AUTHORIZED_PARTICIPANT.tsv", {"FUND_ID": "BAD-FLAG", "PARTICIPANT_NAME": "AP"}, fund_id="BAD-FLAG")
        cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("""SELECT etf_primary_market_state,etf_primary_market_reason_code,etf_primary_market,
                              fund_structure_state,fund_structure_reason_code,fund_structure
                       FROM ncen_operating_profiles""")
        assert cur.fetchone() == (
            "unavailable", "unsupported_etf_flag_lexical", None,
            "unavailable", "fund_structure_not_reported", None,
        )
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_ncen_operating_profile_fails_closed_on_missing_or_ambiguous_fund_identity_and_conflicting_credit_key():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        _submission(cur, run_id, "A1")
        with pytest.raises(psycopg.Error, match="missing N-CEN fund identity"):
            cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        _fund(cur, run_id, "A1", "F1", "S1", HAS_LINE_OF_CREDIT="Y")
        _raw(cur, run_id, "LINE_OF_CREDIT_DETAIL.tsv", {"FUND_ID": "F1", "LINE_OF_CREDIT_SEQNUM": "1"}, fund_id="F1")
        _raw(cur, run_id, "LINE_OF_CREDIT_DETAIL.tsv", {"FUND_ID": "F1", "LINE_OF_CREDIT_SEQNUM": "1", "CREDIT_TYPE": "other"}, fund_id="F1")
        with pytest.raises(psycopg.Error, match="conflicting N-CEN credit-facility child key"):
            cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_ncen_operating_profile_fails_closed_on_ambiguous_fund_identity():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        _submission(cur, run_id, "A1")
        _fund(cur, run_id, "A1", "F1", "S1")
        _fund(cur, run_id, "A1", "F1", "S1")
        with pytest.raises(psycopg.Error, match="ambiguous N-CEN fund identity"):
            cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_ncen_operating_profile_is_immutable_after_validation_and_current_view_never_reads_raw_rows():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        _submission(cur, run_id, "A1")
        _fund(cur, run_id, "A1", "F1", "S1")
        cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication('ncen_operating_profile_v1',%s)", (publication_id,))
        with pytest.raises(psycopg.Error, match="prepared N-CEN operating-profile publication"):
            cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        with pytest.raises(psycopg.Error, match="operating profile is immutable"):
            cur.execute("UPDATE ncen_operating_profiles SET series_id='S2' WHERE publication_id=%s", (publication_id,))
        cur.execute("SELECT accession_number FROM sec_current_ncen_operating_profiles")
        assert cur.fetchone()[0] == "A1"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_ncen_operating_profile_exact_rebuild_is_idempotent_but_as_of_or_effective_selection_change_fails_closed():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        _submission(cur, run_id, "A1")
        _fund(cur, run_id, "A1", "F1", "S1")
        cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (0,)
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-07-01')", (publication_id,))
        amended_run = _prepare_second_run(cur)
        _submission(cur, amended_run, "A2", form="N-CEN/A", filing_date="2026-03-01")
        _fund(cur, amended_run, "A2", "F2", "S2")
        with pytest.raises(psycopg.Error, match="already pinned to effective-input fingerprint"):
            cur.execute("SELECT build_ncen_operating_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT as_of_date,input_fingerprint FROM ncen_operating_profile_builds WHERE publication_id=%s", (publication_id,))
        assert cur.fetchone()[0] == date(2026, 6, 30)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_ncen_operating_profile_ddl_is_ncen_native_and_current_view_is_derived_only():
    ddl = (ROOT / "schemas" / "ncen_operating_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("ncen_operating_profile_v1", "ncen_effective_filings", "input_fingerprint", "provider_children",
                  "sec_derived_current_pointers", "IS_SEC_LENDING_AUTHORIZED", "HAS_LINE_OF_CREDIT", "IS_ETF",
                  "ncen_conditional_positive_flag", "lexical_value = 'Y'"):
        assert token in ddl
    assert "sec_w1_nport_real" not in lower
    assert "cik:" not in lower
    assert "jsonb_typeof(s.fund_evidence->'is_etf')='boolean'" not in lower
    current_view = lower.split("create or replace view sec_current_ncen_operating_profiles", 1)[1]
    assert "ncen_raw_v2_rows" not in current_view
