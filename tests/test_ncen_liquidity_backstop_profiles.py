from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _ncen_derived_fixtures import (  # noqa: E402
    ROOT, base_fixture, dsn, fund, prepare_second_run, raw, submission,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

DDL = ("ncen_derived_common.sql", "ncen_liquidity_backstop_profiles.sql")
PRODUCT = "ncen_liquidity_backstop_v1"


def _fixture(cur):
    return base_fixture(cur, PRODUCT, DDL)


def _loc_detail(cur, run_id, fund_id, seqnum, **fields):
    body = {"FUND_ID": fund_id, "LINE_OF_CREDIT_SEQNUM": seqnum, **fields}
    raw(cur, run_id, "LINE_OF_CREDIT_DETAIL.tsv", body, fund_id=fund_id)


def _loc_institution(cur, run_id, fund_id, seqnum, name):
    raw(cur, run_id, "LINE_OF_CREDIT_INSTITUTION.tsv",
        {"FUND_ID": fund_id, "LINE_OF_CREDIT_SEQNUM": seqnum, "CREDIT_INSTITUTION_NAME": name}, fund_id=fund_id)


def _credit_user(cur, run_id, fund_id, seqnum, name, file_num):
    raw(cur, run_id, "CREDIT_USER.tsv",
        {"FUND_ID": fund_id, "LINE_OF_CREDIT_SEQNUM": seqnum, "FUND_NAME": name, "SEC_FILE_NUM": file_num}, fund_id=fund_id)


def test_liquidity_backstop_loc_facilities_and_derived_utilization():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1",
             HAS_LINE_OF_CREDIT="Y", HAS_INTERFUND_LENDING="Y", HAS_INTERFUND_BORROWING="Y",
             HAS_SWING_PRICING="Y", SWING_FACTOR_UPPER_LIMIT="2.5", MONTHLY_AVG_NET_ASSETS="50000000")
        _loc_detail(cur, run_id, "F1", "1", IS_CREDIT_LINE_COMMITTED="Y", CREDIT_TYPE="Committed",
                    LINE_OF_CREDIT_SIZE="1000000", IS_CREDIT_LINE_USED="Y",
                    AVERAGE_CREDIT_LINE_USED="250000", DAYS_CREDIT_USED="30")
        _loc_institution(cur, run_id, "F1", "1", "Bank A")
        _loc_institution(cur, run_id, "F1", "1", "Bank B")
        _credit_user(cur, run_id, "F1", "1", "Sister Fund 1", "811-1")
        _credit_user(cur, run_id, "F1", "1", "Sister Fund 2", "811-2")
        raw(cur, run_id, "INTER_FUND_LENDING_DETAIL.tsv",
            {"FUND_ID": "F1", "LENDING_LOAN_AVERAGE": "100000", "LENDING_DAYS_OUTSTANDING": "10"}, fund_id="F1")
        raw(cur, run_id, "INTER_FUND_BORROWING_DETAIL.tsv",
            {"FUND_ID": "F1", "BORROWING_LOAN_AVERAGE": "200000", "BORROWING_DAYS_OUTSTANDING": "5"}, fund_id="F1")
        cur.execute("SELECT build_ncen_liquidity_backstop_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT liquidity_backstop_state,liquidity_backstop FROM ncen_liquidity_backstop_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        loc = payload["lines_of_credit"]
        assert loc["state"] == "available"
        assert loc["derived"]["facility_count"] == 1
        assert abs(float(loc["derived"]["aggregate_utilization"]) - 0.25) < 1e-6
        assert loc["derived"]["participating_institution_count"] == 2
        assert loc["derived"]["shared_credit_user_count"] == 2
        fac = loc["facilities"][0]
        assert abs(float(fac["utilization"]) - 0.25) < 1e-6
        assert float(fac["days_used"]) == 30
        assert sorted(fac["participating_institutions"]) == ["Bank A", "Bank B"]
        assert len(fac["credit_users"]) == 2
        inter = payload["interfund"]
        assert abs(float(inter["derived"]["interfund_borrowing_intensity"]) - 0.004) < 1e-6
        assert float(inter["lending"]["loan_average"]) == 100000
        assert payload["swing_pricing"]["has_swing_pricing"] == "true"
        assert abs(float(payload["swing_pricing"]["swing_factor_upper_limit"]) - 2.5) < 1e-6
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_liquidity_backstop_loc_exists_but_unused_is_real_zero_not_absent():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", HAS_LINE_OF_CREDIT="Y")
        # LOC exists but the source reports it was not used (used amount 0).
        _loc_detail(cur, run_id, "F1", "1", LINE_OF_CREDIT_SIZE="1000000",
                    IS_CREDIT_LINE_USED="N", AVERAGE_CREDIT_LINE_USED="0", DAYS_CREDIT_USED="0")
        cur.execute("SELECT build_ncen_liquidity_backstop_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT liquidity_backstop FROM ncen_liquidity_backstop_profiles")
        payload = cur.fetchone()[0]
        loc = payload["lines_of_credit"]
        assert loc["state"] == "available"           # the LOC applies
        fac = loc["facilities"][0]
        assert fac["is_used"] == "false"
        assert float(fac["utilization"]) == 0.0      # a real, source-reported zero
        assert float(loc["derived"]["aggregate_utilization"]) == 0.0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_liquidity_backstop_no_loc_is_not_applicable_never_zero():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", HAS_LINE_OF_CREDIT="N")
        cur.execute("SELECT build_ncen_liquidity_backstop_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT liquidity_backstop_state,liquidity_backstop FROM ncen_liquidity_backstop_profiles")
        state, payload = cur.fetchone()
        assert state == "available"                          # the fund still discloses the flags
        loc = payload["lines_of_credit"]
        assert loc["state"] == "not_applicable"              # no LOC at all
        assert loc["facilities"] == []
        # Utilization is absent, NEVER a synthetic zero.
        assert loc["derived"]["aggregate_utilization"] is None
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_liquidity_backstop_one_to_many_children_never_multiply_fund_grain():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", HAS_LINE_OF_CREDIT="Y", HAS_INTERFUND_BORROWING="Y",
             MONTHLY_AVG_NET_ASSETS="10000000")
        for seq in ("1", "2"):
            _loc_detail(cur, run_id, "F1", seq, LINE_OF_CREDIT_SIZE="1000000",
                        IS_CREDIT_LINE_USED="Y", AVERAGE_CREDIT_LINE_USED="100000", DAYS_CREDIT_USED="10")
            _loc_institution(cur, run_id, "F1", seq, f"Bank {seq}a")
            _loc_institution(cur, run_id, "F1", seq, f"Bank {seq}b")
            _credit_user(cur, run_id, "F1", seq, f"User {seq}a", f"811-{seq}a")
            _credit_user(cur, run_id, "F1", seq, f"User {seq}b", f"811-{seq}b")
        # multiple interfund borrowing detail rows must not multiply the fund either
        raw(cur, run_id, "INTER_FUND_BORROWING_DETAIL.tsv",
            {"FUND_ID": "F1", "BORROWING_LOAN_AVERAGE": "50000", "BORROWING_DAYS_OUTSTANDING": "3"}, fund_id="F1")
        raw(cur, run_id, "INTER_FUND_BORROWING_DETAIL.tsv",
            {"FUND_ID": "F1", "BORROWING_LOAN_AVERAGE": "50000", "BORROWING_DAYS_OUTSTANDING": "4"}, fund_id="F1")
        cur.execute("SELECT build_ncen_liquidity_backstop_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)  # exactly one fund row despite 2x(2+2) + 2 fan-out
        cur.execute("SELECT count(*), (liquidity_backstop->'lines_of_credit'->'derived'->>'facility_count')::int "
                    "FROM ncen_liquidity_backstop_profiles GROUP BY 2")
        rows, facilities = cur.fetchone()
        assert rows == 1 and facilities == 2
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_liquidity_backstop_zero_and_missing_denominators_not_coerced():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        submission(cur, run_id, "A2", cik="C2")
        # F1: LOC size missing -> utilization must not be coerced.
        fund(cur, run_id, "A1", "F1", "S1", HAS_LINE_OF_CREDIT="Y")
        _loc_detail(cur, run_id, "F1", "1", IS_CREDIT_LINE_USED="Y", AVERAGE_CREDIT_LINE_USED="500")
        # F2: borrowing present but net assets missing -> intensity must not be coerced.
        fund(cur, run_id, "A2", "F2", "S2", HAS_INTERFUND_BORROWING="Y")
        raw(cur, run_id, "INTER_FUND_BORROWING_DETAIL.tsv",
            {"FUND_ID": "F2", "BORROWING_LOAN_AVERAGE": "200000", "BORROWING_DAYS_OUTSTANDING": "5"}, fund_id="F2")
        cur.execute("SELECT build_ncen_liquidity_backstop_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT fund_id,liquidity_backstop FROM ncen_liquidity_backstop_profiles ORDER BY fund_id")
        (f1, p1), (f2, p2) = cur.fetchall()
        assert p1["lines_of_credit"]["facilities"][0]["utilization"] is None
        assert p1["lines_of_credit"]["derived"]["aggregate_utilization"] is None
        assert p2["interfund"]["derived"]["interfund_borrowing_intensity"] is None
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_liquidity_backstop_prefers_amendment_and_ddl_is_native():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, base_run, _, publication_id = _fixture(cur)
        amended_run = prepare_second_run(cur)
        submission(cur, base_run, "BASE", filing_date="2026-01-10")
        submission(cur, amended_run, "AMEND", form="N-CEN/A", filing_date="2026-02-10")
        fund(cur, base_run, "BASE", "F1", "S1", HAS_LINE_OF_CREDIT="Y")
        _loc_detail(cur, base_run, "F1", "1", LINE_OF_CREDIT_SIZE="1000000",
                    IS_CREDIT_LINE_USED="Y", AVERAGE_CREDIT_LINE_USED="100000")
        fund(cur, amended_run, "AMEND", "F1", "S1", HAS_LINE_OF_CREDIT="Y")
        _loc_detail(cur, amended_run, "F1", "1", LINE_OF_CREDIT_SIZE="1000000",
                    IS_CREDIT_LINE_USED="Y", AVERAGE_CREDIT_LINE_USED="900000")
        cur.execute("SELECT build_ncen_liquidity_backstop_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT accession_number,"
                    "(liquidity_backstop->'lines_of_credit'->'derived'->>'aggregate_utilization')::float "
                    "FROM ncen_liquidity_backstop_profiles")
        acc, util = cur.fetchone()
        assert acc == "AMEND" and abs(util - 0.9) < 1e-6
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

    ddl = (ROOT / "schemas" / "ncen_liquidity_backstop_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("ncen_liquidity_backstop_v1", "ncen_effective_filings", "ncen_safe_ratio",
                  "aggregate_utilization", "interfund_borrowing_intensity", "row multiplication"):
        assert token in ddl
    for banned in ("vendor", "sha256", "cik:"):
        assert banned not in lower
    current_view = lower.split("create or replace view sec_current_ncen_liquidity_backstop_profiles", 1)[1]
    assert "ncen_raw_v2_rows" not in current_view
