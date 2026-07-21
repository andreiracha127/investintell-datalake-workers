from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ncen_derived_fixtures import (  # noqa: E402
    ROOT, base_fixture, dsn, fund, prepare_second_run, raw, registrant, submission,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

DDL = ("ncen_derived_common.sql", "ncen_operational_event_profiles.sql")
PRODUCT = "ncen_operational_event_v1"


def _fixture(cur):
    return base_fixture(cur, PRODUCT, DDL)


def test_operational_events_encode_families_count_changes_and_risk_indicator():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1", cik="C1")
        # Two funds in the filing must still yield ONE registrant-grain event row.
        fund(cur, run_id, "A1", "F1", "S1", HAS_CUSTODIAN_HIRED_FIRED_MI="Y")
        fund(cur, run_id, "A1", "F2", "S2")
        registrant(cur, run_id, "A1", CIK="C1",
                   IS_MATERIAL_WEAKNESS_NOTED="Y", IS_ACCT_OPINION_QUALIFIED="N",
                   IS_NAV_ERROR_CORRECTED="Y", IS_VALUE_METHOD_CHANGED="Y",
                   IS_ACCT_PRINCIPLE_CHANGED="N", HAS_LEGAL_PROCEEDING="Y",
                   IS_PUB_ACCOUNTANT_CHANGED="Y")
        raw(cur, run_id, "CHIEF_COMPLIANCE_OFFICER.tsv",
            {"ACCESSION_NUMBER": "A1", "CCO_SEQNUM": "1", "IS_CHANGED_SINCE_LAST_FILING": "Y"}, accession="A1")
        raw(cur, run_id, "VALUATION_METHOD_CHANGE.tsv",
            {"ACCESSION_NUMBER": "A1", "VALUATION_METHOD_CHANGE_SEQNUM": "1",
             "ASSET_TYPE": "Equity", "CHANGE_EXPLANATION": "moved to fair value"}, accession="A1")
        cur.execute("SELECT build_ncen_operational_event_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT count(*) FROM ncen_operational_event_profiles")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT operational_event_state,operational_events FROM ncen_operational_event_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        norm = payload["events"]["normalized"]
        assert norm["material_weakness"] == "true"
        assert norm["qualified_audit_opinion"] == "false"
        assert norm["nav_error_correction"] == "true"
        assert norm["valuation_method_change"] == "true"
        assert norm["accounting_principle_change"] == "false"
        assert norm["legal_proceedings"] == "true"
        assert norm["cco_change"] == "true"
        assert norm["provider_change"] == "true"          # custodian hired/fired on F1 + accountant changed
        assert payload["valuation_method_changes"][0]["evidence"]["ASSET_TYPE"] == "Equity"
        assert payload["fund_change_rollup"]["funds_total"] == 2
        assert payload["fund_change_rollup"]["provider_change_fund_count"] == 1
        d = payload["derived"]
        # true families: material_weakness, nav_error, valuation_method, legal, cco, provider = 6
        assert d["operational_change_count"] == 6
        assert d["valuation_control_risk_indicator"] is True
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_operational_events_all_silent_count_zero_and_no_risk_no_synthetic_negatives():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1")
        registrant(cur, run_id, "A1")  # every event column blank
        cur.execute("SELECT build_ncen_operational_event_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT operational_events FROM ncen_operational_event_profiles")
        payload = cur.fetchone()[0]
        assert set(payload["events"]["normalized"].values()) == {"not_reported"}
        assert payload["derived"]["operational_change_count"] == 0
        assert payload["derived"]["valuation_control_risk_indicator"] is False
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_operational_events_aggregate_two_sources_with_or_semantics():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        # Legal proceeding only on SUBMISSION; financial support only on REGISTRANT.
        submission(cur, run_id, "A1")
        raw(cur, run_id, "SUBMISSION.tsv", {
            "ACCESSION_NUMBER": "A2", "CIK": "C2", "SUBMISSION_TYPE": "N-CEN",
            "FILING_DATE": "2026-02-01", "REPORT_ENDING_PERIOD": "2025-12-31",
            "IS_LEGAL_PROCEEDINGS": "Y", "IS_CHANGE_ACC_PRINCIPLES": "N",
        }, accession="A2")
        fund(cur, run_id, "A1", "F1", "S1")
        fund(cur, run_id, "A2", "F2", "S2")
        registrant(cur, run_id, "A1", HAS_LEGAL_PROCEEDING="N")
        registrant(cur, run_id, "A2", HAS_LEGAL_PROCEEDING="N", IS_ACCT_PRINCIPLE_CHANGED="Y")
        cur.execute("SELECT build_ncen_operational_event_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT accession_number,operational_events->'events'->'normalized' FROM ncen_operational_event_profiles ORDER BY accession_number")
        rows = dict(cur.fetchall())
        # A2: SUBMISSION says legal='Y' though REGISTRANT says 'N' -> OR rollup wins with 'true'.
        assert rows["A2"]["legal_proceedings"] == "true"
        # A2 accounting principle: REGISTRANT 'Y' though SUBMISSION 'N' -> 'true'.
        assert rows["A2"]["accounting_principle_change"] == "true"
        # A1: both sources negative -> reported 'false', never not_reported.
        assert rows["A1"]["legal_proceedings"] == "false"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_operational_events_fail_closed_on_ambiguous_registrant_and_is_immutable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1")
        registrant(cur, run_id, "A1")
        registrant(cur, run_id, "A1")  # two registrant rows for one accession
        with pytest.raises(psycopg.Error, match="ambiguous N-CEN registrant identity"):
            cur.execute("SELECT build_ncen_operational_event_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1")
        registrant(cur, run_id, "A1", IS_MATERIAL_WEAKNESS_NOTED="Y")
        cur.execute("SELECT build_ncen_operational_event_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))
        with pytest.raises(psycopg.Error, match="operational event profile is immutable"):
            cur.execute("UPDATE ncen_operational_event_profiles SET registrant_cik='X' WHERE publication_id=%s", (publication_id,))
        cur.execute("SELECT accession_number FROM sec_current_ncen_operational_event_profiles")
        assert cur.fetchone()[0] == "A1"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_operational_events_prefers_amendment_and_ddl_is_native():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, base_run, _, publication_id = _fixture(cur)
        amended_run = prepare_second_run(cur)
        submission(cur, base_run, "BASE", filing_date="2026-01-10")
        submission(cur, amended_run, "AMEND", form="N-CEN/A", filing_date="2026-02-10")
        fund(cur, base_run, "BASE", "F1", "S1")
        registrant(cur, base_run, "BASE", IS_NAV_ERROR_CORRECTED="Y")
        fund(cur, amended_run, "AMEND", "F1", "S1")
        registrant(cur, amended_run, "AMEND", IS_NAV_ERROR_CORRECTED="N")
        cur.execute("SELECT build_ncen_operational_event_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT accession_number,operational_events->'events'->'normalized'->>'nav_error_correction' FROM ncen_operational_event_profiles")
        assert cur.fetchone() == ("AMEND", "false")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

    ddl = (ROOT / "schemas" / "ncen_operational_event_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("ncen_operational_event_v1", "ncen_effective_filings", "ncen_tristate_or",
                  "operational_change_count", "valuation_control_risk_indicator", "IS_MATERIAL_WEAKNESS_NOTED"):
        assert token in ddl
    for banned in ("vendor", "sha256", "cik:"):
        assert banned not in lower
    current_view = lower.split("create or replace view sec_current_ncen_operational_event_profiles", 1)[1]
    assert "ncen_raw_v2_rows" not in current_view
