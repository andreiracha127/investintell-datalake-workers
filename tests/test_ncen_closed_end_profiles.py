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

DDL = ("ncen_derived_common.sql", "ncen_closed_end_profiles.sql")
PRODUCT = "ncen_closed_end_v1"


def _fixture(cur):
    return base_fixture(cur, PRODUCT, DDL)


def _rights(cur, run_id, fund_id, **fields):
    raw(cur, run_id, "RIGHTS_OFFERING_FUND.tsv", {"FUND_ID": fund_id, **fields}, fund_id=fund_id)


def _debt_default(cur, run_id, fund_id, **fields):
    raw(cur, run_id, "LONGTERM_DEBT_DEFAULT.tsv", {"FUND_ID": fund_id, **fields}, fund_id=fund_id)


def _arrears(cur, run_id, fund_id, **fields):
    raw(cur, run_id, "DIVIDENDS_IN_ARREAR.tsv", {"FUND_ID": fund_id, **fields}, fund_id=fund_id)


def test_closed_end_available_with_full_part_f():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1",
             MARKET_PRICE_PER_SHARE="10.50", NAV_PER_SHARE="12.00",
             DID_MAKE_RIGHTS_OFFERING="Y", DID_MAKE_SECOND_OFFERING="Y", IS_SECONDARY_COMMON="Y",
             DID_REPURCHASE_SECURITY="Y", IS_REPUR_COMMON="Y",
             IS_LONG_TERM_DEBT_DEFAULT="Y", IS_ACCUM_DIVIDEND_IN_ARREARS="Y", IS_SECURITY_MAT_MODIFIED="N")
        _rights(cur, run_id, "F1", IS_RIGHTS_OFFER_COMMON="Y", PCT_PARTCI_PRIMARY_OFFERING="75.5")
        _debt_default(cur, run_id, "F1", DEFAULT_NATURE="Missed coupon", TOTAL_DEFAULT_AMNT="500000",
                      DEFAULT_AMNT_PER_1000="20")
        _arrears(cur, run_id, "F1", ISSUE_TITLE="Series A Preferred", AMOUNT_PER_SHARE_IN_ARREAR="2.50")
        cur.execute("SELECT build_ncen_closed_end_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT closed_end_state,closed_end FROM ncen_closed_end_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        assert float(payload["market_valuation"]["market_price_per_share"]) == 10.50
        assert float(payload["market_valuation"]["nav_per_share"]) == 12.00
        assert abs(float(payload["derived"]["premium_discount_ratio"]) - (-0.125)) < 1e-6
        assert payload["offerings"]["made_rights_offering"] == "true"
        assert payload["offerings"]["made_secondary_offering"] == "true"
        assert payload["offerings"]["rights_offerings"][0]["pct_participation_primary"] is not None
        assert payload["repurchases"]["repurchased_security"] == "true"
        assert payload["debt_default"]["has_long_term_debt_default"] == "true"
        assert float(payload["debt_default"]["details"][0]["total_amount"]) == 500000
        assert payload["arrears"]["has_accumulated_dividends_in_arrears"] == "true"
        assert payload["arrears"]["details"][0]["issue_title"] == "Series A Preferred"
        assert payload["security_modification"]["has_material_modification"] == "false"
        d = payload["derived"]
        assert d["rights_offering_count"] == 1 and d["debt_default_count"] == 1 and d["arrears_count"] == 1
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_closed_end_open_end_fund_is_not_applicable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        # Open-end fund: no Part F evidence, no market/NAV, not an interval fund.
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="Y")
        cur.execute("SELECT build_ncen_closed_end_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT closed_end_state,closed_end_reason_code,closed_end FROM ncen_closed_end_profiles")
        state, reason, payload = cur.fetchone()
        assert state == "not_applicable"
        assert reason == "fund_is_not_closed_end"
        assert payload is None
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_closed_end_interval_fund_is_applicable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        # Interval fund flagged, no other Part F flags answered -> still applicable.
        fund(cur, run_id, "A1", "F1", "S1", IS_INTERVAL="Y",
             DID_REPURCHASE_SECURITY="Y", IS_REPUR_COMMON="Y")
        cur.execute("SELECT build_ncen_closed_end_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT closed_end_state,closed_end FROM ncen_closed_end_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        assert payload["interval_fund"]["is_interval"] == "true"
        assert payload["repurchases"]["repurchased_security"] == "true"
        # No market/NAV reported -> premium/discount is NULL, never a synthetic 0.
        assert payload["derived"]["premium_discount_ratio"] is None
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_closed_end_with_arrears():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1",
             MARKET_PRICE_PER_SHARE="8.00", NAV_PER_SHARE="9.00",
             IS_ACCUM_DIVIDEND_IN_ARREARS="Y", IS_LONG_TERM_DEBT_DEFAULT="N")
        _arrears(cur, run_id, "F1", ISSUE_TITLE="Series A Preferred", AMOUNT_PER_SHARE_IN_ARREAR="2.50")
        _arrears(cur, run_id, "F1", ISSUE_TITLE="Series B Preferred", AMOUNT_PER_SHARE_IN_ARREAR="1.25")
        cur.execute("SELECT build_ncen_closed_end_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT closed_end_state,closed_end FROM ncen_closed_end_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        assert payload["arrears"]["has_accumulated_dividends_in_arrears"] == "true"
        assert payload["derived"]["arrears_count"] == 2
        # No debt default reported (flag answered 'N') -> empty details, not fabricated.
        assert payload["debt_default"]["has_long_term_debt_default"] == "false"
        assert payload["debt_default"]["details"] == []
        assert abs(float(payload["derived"]["premium_discount_ratio"]) - (-0.111111)) < 1e-5
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_closed_end_premium_discount_zero_and_missing_denominator_not_coerced():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        submission(cur, run_id, "A2", cik="C2")
        submission(cur, run_id, "A3", cik="C3")
        # NAV == 0 -> denominator zero -> NULL.
        fund(cur, run_id, "A1", "F1", "S1", MARKET_PRICE_PER_SHARE="10", NAV_PER_SHARE="0",
             DID_REPURCHASE_SECURITY="N")
        # NAV missing -> denominator missing -> NULL.
        fund(cur, run_id, "A2", "F2", "S2", MARKET_PRICE_PER_SHARE="10", DID_REPURCHASE_SECURITY="N")
        # market price missing -> numerator missing leg -> NULL (never 0).
        fund(cur, run_id, "A3", "F3", "S3", NAV_PER_SHARE="10", DID_REPURCHASE_SECURITY="N")
        cur.execute("SELECT build_ncen_closed_end_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT fund_id,(closed_end->'derived'->>'premium_discount_ratio') "
                    "FROM ncen_closed_end_profiles ORDER BY fund_id")
        rows = cur.fetchall()
        assert [r[1] for r in rows] == [None, None, None]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_closed_end_one_to_many_children_never_multiply_fund_grain():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", MARKET_PRICE_PER_SHARE="11", NAV_PER_SHARE="10",
             DID_MAKE_RIGHTS_OFFERING="Y", IS_LONG_TERM_DEBT_DEFAULT="Y", IS_ACCUM_DIVIDEND_IN_ARREARS="Y")
        for i in range(2):
            _rights(cur, run_id, "F1", IS_RIGHTS_OFFER_COMMON="Y", RIGHTS_OFFER_DESC=f"R{i}")
            _debt_default(cur, run_id, "F1", DEFAULT_NATURE=f"D{i}", TOTAL_DEFAULT_AMNT=str(1000 * (i + 1)))
            _arrears(cur, run_id, "F1", ISSUE_TITLE=f"P{i}", AMOUNT_PER_SHARE_IN_ARREAR=str(i + 1))
        cur.execute("SELECT build_ncen_closed_end_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)  # one fund row despite 2x2x2 children
        cur.execute("SELECT count(*), (closed_end->'derived'->>'rights_offering_count')::int, "
                    "(closed_end->'derived'->>'debt_default_count')::int, "
                    "(closed_end->'derived'->>'arrears_count')::int "
                    "FROM ncen_closed_end_profiles GROUP BY 2,3,4")
        rows, rights, defaults, arrears = cur.fetchone()
        assert rows == 1 and rights == 2 and defaults == 2 and arrears == 2
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_closed_end_prefers_amendment_and_ddl_is_native():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, base_run, _, publication_id = _fixture(cur)
        amended_run = prepare_second_run(cur)
        submission(cur, base_run, "BASE", filing_date="2026-01-10")
        submission(cur, amended_run, "AMEND", form="N-CEN/A", filing_date="2026-02-10")
        fund(cur, base_run, "BASE", "F1", "S1", MARKET_PRICE_PER_SHARE="9", NAV_PER_SHARE="10",
             DID_REPURCHASE_SECURITY="Y")
        fund(cur, amended_run, "AMEND", "F1", "S1", MARKET_PRICE_PER_SHARE="11", NAV_PER_SHARE="10",
             DID_REPURCHASE_SECURITY="Y")
        cur.execute("SELECT build_ncen_closed_end_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT accession_number,(closed_end->'derived'->>'premium_discount_ratio')::float "
                    "FROM ncen_closed_end_profiles")
        acc, ratio = cur.fetchone()
        assert acc == "AMEND" and abs(ratio - 0.1) < 1e-6
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

    ddl = (ROOT / "schemas" / "ncen_closed_end_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("ncen_closed_end_v1", "ncen_effective_filings", "ncen_safe_ratio",
                  "premium_discount", "row multiplication"):
        assert token in ddl
    for banned in ("vendor", "sha256", "cik:"):
        assert banned not in lower
    current_view = lower.split("create or replace view sec_current_ncen_closed_end_profiles", 1)[1]
    assert "ncen_raw_v2_rows" not in current_view
