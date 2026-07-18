from __future__ import annotations

import json
from decimal import Decimal
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"


def _fixture(cur):
    schema = f"rr1_fee_profile_fixture_{uuid4().hex}"
    run_id, package_id, publication_id = uuid4(), uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
    cur.execute("""CREATE TABLE rr1_effective_facts(
        raw_row_id bigint, ingestion_run_id uuid, source_table text, accession_number text,
        tag text, version text, data_date date, series_id text, class_id text, measure_id text,
        document_id text, dimensions text, occurrence text, fact_typed_projection jsonb,
        effective_date date, accepted_at timestamptz, filed_date date, form text)""")
    for ddl_name in ("sec_derived_publications.sql", "rr1_fee_profiles.sql"):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    cur.execute("""INSERT INTO sec_derived_publications
        (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
        VALUES(%s,'rr1_fee_profile_v1',1,%s,%s,%s)""",
        (publication_id, run_id, package_id, "a" * 64))
    return schema, run_id, publication_id


def _fact(cur, run_id, tag, value, *, accession="A1", series="S1", class_id="C1", document="D1",
          measure="", dimensions="", occurrence="1", data_date="2025-12-31", effective="2026-01-01",
          filed="2026-01-02", raw_row_id=1):
    cur.execute("""INSERT INTO rr1_effective_facts VALUES
        (%s,%s,'num.tsv',%s,%s,'rr/2025',%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,now(),%s,'N-1A')""",
        (raw_row_id, run_id, accession, tag, data_date, series, class_id, measure, document,
         dimensions, occurrence, json.dumps({"value": value}) if value is not None else "{}", effective, filed))


def test_rr1_fee_profiles_preserve_class_context_canonical_and_original_tag_evidence():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, publication_id = _fixture(cur)
        _fact(cur, run_id, "ManagementFeesOverAssets", "0.35", class_id="C1", document="D1", raw_row_id=11)
        _fact(cur, run_id, "ManagementFeesOverAssets", "0.45", class_id="C2", document="D2", raw_row_id=12)
        _fact(cur, run_id, "DistributionAndService12b1FeesOverAssets", "0", class_id="C1", document="D1", raw_row_id=13)
        assert cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,)).fetchone() == (14,)
        cur.execute("""SELECT series_id,class_id,document_id,canonical_concept,original_tag,original_version,
                              value_numeric,status,measure_id,dimensions,occurrence,accession_number
                       FROM rr1_fee_profiles
                       WHERE canonical_concept IN ('management_fee','distribution_12b1')
                       ORDER BY class_id,canonical_concept""")
        assert cur.fetchall() == [
            ("S1", "C1", "D1", "distribution_12b1", "DistributionAndService12b1FeesOverAssets", "rr/2025", 0, "available", "", "", "1", "A1"),
            ("S1", "C1", "D1", "management_fee", "ManagementFeesOverAssets", "rr/2025", Decimal("0.35"), "available", "", "", "1", "A1"),
            ("S1", "C2", "D2", "distribution_12b1", None, None, None, "unavailable", "", "", "1", "A1"),
            ("S1", "C2", "D2", "management_fee", "ManagementFeesOverAssets", "rr/2025", Decimal("0.45"), "available", "", "", "1", "A1"),
        ]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_rr1_fee_profiles_keep_missing_distinct_from_zero_and_degrade_invalid_value():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, publication_id = _fixture(cur)
        _fact(cur, run_id, "ManagementFeesOverAssets", "0", raw_row_id=1)
        _fact(cur, run_id, "OtherExpensesOverAssets", None, raw_row_id=2)
        cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("""SELECT canonical_concept,value_numeric,status,reason_code FROM rr1_fee_profiles
                       ORDER BY canonical_concept""")
        rows = dict((concept, (value, state, reason)) for concept, value, state, reason in cur.fetchall())
        assert rows["management_fee"] == (0, "available", None)
        assert rows["other_expense"] == (None, "degraded", "selected_fact_has_no_numeric_value")
        assert rows["net_expense"] == (None, "unavailable", "concept_not_reported")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_rr1_fee_profiles_fail_closed_on_conflicting_selected_canonical_fact():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, publication_id = _fixture(cur)
        _fact(cur, run_id, "ManagementFeesOverAssets", "0.35", raw_row_id=1)
        _fact(cur, run_id, "ManagementFeesOverAssets", "0.45", raw_row_id=2)
        with pytest.raises(psycopg.Error, match="conflicting RR1 fee facts"):
            cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_rr1_fee_profiles_inherit_effective_winner_and_pin_immutable_build_identity():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, publication_id = _fixture(cur)
        amended_run = uuid4()
        cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (amended_run,))
        _fact(cur, amended_run, "ManagementFeesOverAssets", "0.20", accession="AMEND", raw_row_id=9)
        assert cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,)).fetchone() == (7,)
        cur.execute("SELECT source_run_id,accession_number,value_numeric FROM rr1_fee_profiles WHERE canonical_concept='management_fee'")
        assert cur.fetchone() == (amended_run, "AMEND", Decimal("0.20"))
        assert cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,)).fetchone() == (0,)
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-07-01')", (publication_id,))
        _fact(cur, amended_run, "OtherExpensesOverAssets", "0.10", raw_row_id=10)
        with pytest.raises(psycopg.Error, match="already pinned to effective-input fingerprint"):
            cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_rr1_fee_profiles_are_immutable_after_validation_and_current_view_is_derived_only():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, publication_id = _fixture(cur)
        _fact(cur, run_id, "ManagementFeesOverAssets", "0.35")
        cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication('rr1_fee_profile_v1',%s)", (publication_id,))
        with pytest.raises(psycopg.Error, match="prepared RR1 fee-profile publication"):
            cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,))
        with pytest.raises(psycopg.Error, match="RR1 fee profile is immutable"):
            cur.execute("UPDATE rr1_fee_profiles SET class_id='C2' WHERE publication_id=%s", (publication_id,))
        assert cur.execute("SELECT value_numeric FROM sec_current_rr1_fee_profiles WHERE canonical_concept='management_fee'").fetchone() == (Decimal("0.35"),)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_rr1_fee_profile_ddl_is_effective_only_and_current_view_never_reads_raw():
    ddl = (ROOT / "schemas" / "rr1_fee_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("rr1_fee_profile_v1", "rr1_effective_facts", "input_fingerprint", "management_fee",
                  "DistributionAndService12b1FeesOverAssets", "sec_derived_current_pointers"):
        assert token in ddl
    assert "sec_w1_nport_real" not in lower
    current_view = lower.split("create or replace view sec_current_rr1_fee_profiles", 1)[1]
    assert "rr1_raw_v2_rows" not in current_view
    assert "rr1_effective_facts" not in current_view


def test_rr1_fee_profiles_map_only_the_seven_official_rr_local_names_and_keep_text_occurrence():
    import psycopg

    tags = {
        "ManagementFeesOverAssets": "management_fee",
        "DistributionAndService12b1FeesOverAssets": "distribution_12b1",
        "AcquiredFundFeesAndExpensesOverAssets": "acquired_fund_expense",
        "OtherExpensesOverAssets": "other_expense",
        "ExpensesOverAssets": "gross_expense",
        "FeeWaiverOrReimbursementOverAssets": "waiver_reimbursement",
        "NetExpensesOverAssets": "net_expense",
    }
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, publication_id = _fixture(cur)
        for n, tag in enumerate(tags, start=1):
            _fact(cur, run_id, tag, str(n / 100), occurrence="01", raw_row_id=n)
        _fact(cur, run_id, "ManagementFees", "0.99", class_id="PROSE", raw_row_id=99)
        assert cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,)).fetchone() == (7,)
        cur.execute("SELECT canonical_concept,original_tag,occurrence FROM rr1_fee_profiles ORDER BY canonical_concept")
        assert {concept: tag for concept, tag, _ in cur.fetchall()} == {v: k for k, v in tags.items()}
        assert {occurrence for _, _, occurrence in cur.execute("SELECT canonical_concept,original_tag,occurrence FROM rr1_fee_profiles")} == {"01"}
        assert cur.execute("SELECT count(*) FROM rr1_fee_profiles WHERE class_id='PROSE'").fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_rr1_fee_profile_validation_and_current_pointer_require_a_closed_build():
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, _, publication_id = _fixture(cur)
        with pytest.raises(psycopg.Error, match="requires a pinned build"):
            cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("ALTER TABLE sec_derived_publications DISABLE TRIGGER rr1_fee_profile_publication_validation_guard")
        cur.execute("INSERT INTO sec_derived_publication_tokens(publication_id,backend_pid) VALUES(%s,pg_backend_pid())", (publication_id,))
        cur.execute("UPDATE sec_derived_publications SET lifecycle_state='validated', validated_at=now() WHERE publication_id=%s", (publication_id,))
        cur.execute("DELETE FROM sec_derived_publication_tokens WHERE publication_id=%s", (publication_id,))
        cur.execute("ALTER TABLE sec_derived_publications ENABLE TRIGGER rr1_fee_profile_publication_validation_guard")
        with pytest.raises(psycopg.Error, match="requires a closed pinned build"):
            cur.execute("SELECT sec_set_current_derived_publication('rr1_fee_profile_v1',%s)", (publication_id,))
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
