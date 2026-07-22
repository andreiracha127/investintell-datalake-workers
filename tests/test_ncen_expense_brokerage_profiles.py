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

DDL = ("ncen_derived_common.sql", "ncen_expense_brokerage_profiles.sql")
PRODUCT = "ncen_expense_brokerage_v1"


def _fixture(cur):
    return base_fixture(cur, PRODUCT, DDL)


def _broker(cur, run_id, fund_id, **fields):
    raw(cur, run_id, "BROKER.tsv", {"FUND_ID": fund_id, **fields}, fund_id=fund_id)


def _principal(cur, run_id, fund_id, **fields):
    raw(cur, run_id, "PRINCIPAL_TRANSACTION.tsv", {"FUND_ID": fund_id, **fields}, fund_id=fund_id)


def test_expense_brokerage_available_full():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1",
             MANAGEMENT_FEE="150000", NET_OPERATING_EXPENSES="90000", MONTHLY_AVG_NET_ASSETS="50000000",
             HAS_EXP_LIMIT="Y", HAS_EXP_REDUCED_WAIVED="Y", HAS_EXP_SUBJ_RECOUP="N", HAS_EXP_RECOUPED="N",
             AGG_COMMISSION="12000", AGG_PRINCIPAL="34000", DID_PAY_BROKER_RESEARCH="Y")
        _broker(cur, run_id, "F1", BROKER_NAME="Broker A", BROKER_LEI="LEI-B", GROSS_COMMISSION="8000")
        _principal(cur, run_id, "F1", PRINCIPAL_NAME="Dealer X", PRINCIPAL_LEI="LEI-P",
                   PRINCIPAL_TOTAL_PURCHASE_SALE="34000")
        registrant(cur, run_id, "A1", IS_ACCT_OPINION_QUALIFIED="Y", IS_MATERIAL_WEAKNESS_NOTED="N")
        cur.execute("SELECT build_ncen_expense_brokerage_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT expense_brokerage_state,expense_brokerage FROM ncen_expense_brokerage_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        assert float(payload["expenses"]["management_fee"]) == 150000
        assert float(payload["expenses"]["net_operating_expenses"]) == 90000
        arr = payload["expenses"]["expense_arrangements"]
        assert arr["has_expense_reduced_waived"] == "true"
        assert arr["has_expense_subject_recoupment"] == "false"
        assert float(payload["brokerage"]["aggregate_commission"]) == 12000
        assert payload["brokerage"]["paid_broker_research"] == "true"
        assert payload["brokerage"]["brokers"][0]["identifier_value"] == "LEI-B"
        assert float(payload["brokerage"]["brokers"][0]["gross_commission"]) == 8000
        assert payload["brokerage"]["principal_dealers"][0]["identifier_value"] == "LEI-P"
        xref = payload["audit_control_cross_reference"]
        assert xref["operational_event_grain"] == "registrant"
        assert xref["operational_event_product"] == "ncen_operational_event_v1"
        assert xref["disclosures"]["qualified_audit_opinion"] == "true"
        assert xref["disclosures"]["material_weakness"] == "false"
        rc = payload["derived"]["regulatory_complexity"]
        assert rc["broker_count"] == 1 and rc["principal_dealer_count"] == 1
        assert rc["expense_arrangement_flag_count"] == 2   # limit + waiver
        assert rc["audit_control_disclosure_count"] == 1   # qualified opinion only
        assert rc["has_audit_control_disclosure"] is True
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_expense_brokerage_waiver_flag_without_value():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        # Waiver flag set but N-CEN carries no waiver amount: the flag is true,
        # the value stays absent, and nothing is coerced to a synthetic zero.
        fund(cur, run_id, "A1", "F1", "S1", MANAGEMENT_FEE="150000", HAS_EXP_REDUCED_WAIVED="Y")
        cur.execute("SELECT build_ncen_expense_brokerage_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT expense_brokerage_state,expense_brokerage FROM ncen_expense_brokerage_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        assert payload["expenses"]["expense_arrangements"]["has_expense_reduced_waived"] == "true"
        assert float(payload["expenses"]["management_fee"]) == 150000
        assert payload["expenses"]["net_operating_expenses"] is None       # absent, not zero
        # No synthetic waiver amount is fabricated anywhere in the payload.
        assert "waiver_value" not in payload["expenses"]
        assert "waiver_amount" not in payload["expenses"]["expense_arrangements"]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_expense_brokerage_audit_control_is_cross_reference_not_registrant_grain():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        # Two funds on one registrant filing: audit/control disclosure is a
        # per-fund cross-reference to the registrant-grain operational-event
        # record, not re-materialized at registrant grain.
        fund(cur, run_id, "A1", "F1", "S1", MANAGEMENT_FEE="100000")
        fund(cur, run_id, "A1", "F2", "S2", MANAGEMENT_FEE="200000")
        registrant(cur, run_id, "A1", IS_MATERIAL_WEAKNESS_NOTED="Y", IS_ACCT_OPINION_QUALIFIED="Y")
        cur.execute("SELECT build_ncen_expense_brokerage_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (2,)   # fund grain: one row per fund, not one per registrant
        cur.execute("SELECT fund_id, expense_brokerage->'audit_control_cross_reference', "
                    "expense_brokerage->'derived'->'regulatory_complexity' "
                    "FROM ncen_expense_brokerage_profiles ORDER BY fund_id")
        for fund_id, xref, rc in cur.fetchall():
            assert xref["operational_event_grain"] == "registrant"
            assert xref["operational_event_product"] == "ncen_operational_event_v1"
            assert xref["registrant_cik"] == "C1"
            assert xref["accession_number"] == "A1"
            assert xref["disclosures"]["material_weakness"] == "true"
            assert xref["disclosures"]["qualified_audit_opinion"] == "true"
            assert rc["audit_control_disclosure_count"] == 2
            assert rc["has_audit_control_disclosure"] is True
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_expense_brokerage_one_to_many_children_never_multiply_fund_grain():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", MANAGEMENT_FEE="100000")
        for i in range(2):
            _broker(cur, run_id, "F1", BROKER_NAME=f"B{i}", BROKER_LEI=f"LEI-B{i}",
                    GROSS_COMMISSION=str(1000 * (i + 1)))
            _principal(cur, run_id, "F1", PRINCIPAL_NAME=f"D{i}", PRINCIPAL_LEI=f"LEI-P{i}",
                       PRINCIPAL_TOTAL_PURCHASE_SALE=str(2000 * (i + 1)))
        cur.execute("SELECT build_ncen_expense_brokerage_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT count(*), (expense_brokerage->'derived'->'regulatory_complexity'->>'broker_count')::int, "
                    "(expense_brokerage->'derived'->'regulatory_complexity'->>'principal_dealer_count')::int "
                    "FROM ncen_expense_brokerage_profiles GROUP BY 2,3")
        rows, brokers, dealers = cur.fetchone()
        assert rows == 1 and brokers == 2 and dealers == 2
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_expense_brokerage_prefers_amendment_and_ddl_is_native():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, base_run, _, publication_id = _fixture(cur)
        amended_run = prepare_second_run(cur)
        submission(cur, base_run, "BASE", filing_date="2026-01-10")
        submission(cur, amended_run, "AMEND", form="N-CEN/A", filing_date="2026-02-10")
        fund(cur, base_run, "BASE", "F1", "S1", MANAGEMENT_FEE="100000")
        fund(cur, amended_run, "AMEND", "F1", "S1", MANAGEMENT_FEE="200000")
        cur.execute("SELECT build_ncen_expense_brokerage_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT accession_number,(expense_brokerage->'expenses'->>'management_fee')::float "
                    "FROM ncen_expense_brokerage_profiles")
        acc, fee = cur.fetchone()
        assert acc == "AMEND" and fee == 200000
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

    ddl = (ROOT / "schemas" / "ncen_expense_brokerage_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("ncen_expense_brokerage_v1", "ncen_effective_filings", "ncen_operational_event_v1",
                  "regulatory_complexity", "row multiplication"):
        assert token in ddl
    for banned in ("vendor", "sha256", "cik:"):
        assert banned not in lower
    current_view = lower.split("create or replace view sec_current_ncen_expense_brokerage_profiles", 1)[1]
    assert "ncen_raw_v2_rows" not in current_view


def test_expense_brokerage_duplicate_registrant_fails_closed():
    """Task 1b: the parent CTE folds one REGISTRANT surface onto each fund 1:1.  A
    filing exposing two REGISTRANT rows would multiply the fund grain and the fund PK
    would keep an arbitrary registrant -- so the build must fail closed instead."""
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", MANAGEMENT_FEE="150000")
        # two REGISTRANT rows under the SAME accession -> ambiguous registrant identity.
        registrant(cur, run_id, "A1", IS_ACCT_OPINION_QUALIFIED="Y")
        registrant(cur, run_id, "A1", IS_ACCT_OPINION_QUALIFIED="N")
        with pytest.raises(psycopg.errors.RaiseException, match="ambiguous N-CEN registrant identity"):
            cur.execute("SELECT build_ncen_expense_brokerage_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
