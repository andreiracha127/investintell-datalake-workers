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

DDL = ("ncen_derived_common.sql", "ncen_etf_primary_market_profiles.sql")
PRODUCT = "ncen_etf_primary_market_v1"


def _fixture(cur):
    return base_fixture(cur, PRODUCT, DDL)


def _etf(cur, run_id, fund_id, **fields):
    raw(cur, run_id, "ETF.tsv", {"FUND_ID": fund_id, **fields}, fund_id=fund_id)


def _ap(cur, run_id, fund_id, **fields):
    raw(cur, run_id, "AUTHORIZED_PARTICIPANT.tsv", {"FUND_ID": fund_id, **fields}, fund_id=fund_id)


def test_etf_primary_market_aps_creation_unit_mix_fees_tracking():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="Y", IS_INDEX_AFFILIATED="Y", IS_INDEX_EXCLUSIVE="N")
        _etf(cur, run_id, "F1", IS_COLLATERAL_REQUIRED="Y", NUM_SHARES_PER_CREATION_UNIT="50000",
             PURCHASED_AVG_PCT_CASH="10", PURCHASED_AVG_PCT_NON_CASH="90",
             REDEEMED_AVG_PCT_CASH="15", REDEEMED_AVG_PCT_NON_CASH="85",
             PURCH_AVG_FEE_PER_UNIT="500", REDEEM_AVG_FEE_PER_UNIT="500",
             IS_PERF_TRACKED_AFFILIA_PERSON="Y", IS_PERF_TRACKED_EXCLUSIVELY="N",
             ANNUAL_DIFF_B4_FEE_EXPENSE="0.10", ANNUAL_DIFF_AFTER_FEE_EXPENSE="0.35",
             ANNUAL_STDV_B4_FEE_EXPENSE="0.05", ANNUAL_STDV_AFTER_FEE_EXPENSE="0.06",
             IS_FUND_IN_KIND_ETF="N")
        _ap(cur, run_id, "F1", PARTICIPANT_NAME="AP One", PARTICIPANT_LEI="LEI-AP1",
            PURCHASE_VALUE="6000000", REDEEM_VALUE="2000000")
        _ap(cur, run_id, "F1", PARTICIPANT_NAME="AP Two", PARTICIPANT_LEI="LEI-AP2",
            PURCHASE_VALUE="1000000", REDEEM_VALUE="1000000")
        cur.execute("SELECT build_ncen_etf_primary_market_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT etf_primary_market_state,etf_primary_market FROM ncen_etf_primary_market_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        assert float(payload["creation_unit_shares"]) == 50000
        assert payload["is_collateral_required"] == "true"
        assert float(payload["cash_in_kind_mix"]["purchased_avg_pct_cash"]) == 10
        assert payload["index_flags"]["index_affiliated"] == "true"
        assert payload["index_flags"]["index_exclusive"] == "false"
        assert abs(float(payload["tracking"]["difference_before_fee"]) - 0.10) < 1e-9
        assert abs(float(payload["tracking"]["difference_after_fee"]) - 0.35) < 1e-9
        aps = {a["identifier_value"]: a for a in payload["authorized_participants"]}
        assert set(aps) == {"LEI-AP1", "LEI-AP2"}
        d = payload["derived"]
        assert d["authorized_participant_count"] == 2
        # net flow = (6M+1M purchases) - (2M+1M redemptions) = 4,000,000
        assert float(d["net_primary_market_flow"]) == 4000000
        # HHI over total AP value: shares (8M,2M)/10M -> 0.8^2 + 0.2^2 = 0.68
        assert abs(float(d["ap_concentration_hhi"]) - 0.68) < 1e-6
        # fee-attributable tracking drag = after - before = 0.25
        assert abs(float(d["fee_attributable_tracking_drag"]) - 0.25) < 1e-9
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_etf_primary_market_non_etf_fund_is_not_applicable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="N", IS_ETMF="N")
        cur.execute("SELECT build_ncen_etf_primary_market_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT etf_primary_market_state,etf_primary_market_reason_code,etf_primary_market "
                    "FROM ncen_etf_primary_market_profiles")
        state, reason, payload = cur.fetchone()
        assert state == "not_applicable"
        assert reason == "fund_is_not_etf"
        assert payload is None
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_etf_primary_market_etf_without_detail_is_unavailable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        # ETF flagged but no ETF.tsv primary-market disclosure row.
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="Y")
        cur.execute("SELECT build_ncen_etf_primary_market_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT etf_primary_market_state,etf_primary_market_reason_code,etf_primary_market "
                    "FROM ncen_etf_primary_market_profiles")
        state, reason, payload = cur.fetchone()
        assert state == "unavailable"
        assert reason == "etf_primary_market_not_reported"
        assert payload is None
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_etf_primary_market_one_to_many_aps_never_multiply_fund_grain():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="Y")
        _etf(cur, run_id, "F1", NUM_SHARES_PER_CREATION_UNIT="25000")
        for i in range(3):
            _ap(cur, run_id, "F1", PARTICIPANT_NAME=f"AP{i}", PARTICIPANT_LEI=f"LEI-AP{i}",
                PURCHASE_VALUE="1000000", REDEEM_VALUE="0")
        cur.execute("SELECT build_ncen_etf_primary_market_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)  # one fund row despite 3 APs
        cur.execute("SELECT count(*), (etf_primary_market->'derived'->>'authorized_participant_count')::int, "
                    "(etf_primary_market->'derived'->>'ap_concentration_hhi')::float "
                    "FROM ncen_etf_primary_market_profiles GROUP BY 2,3")
        rows, ap_count, hhi = cur.fetchone()
        assert rows == 1 and ap_count == 3
        assert abs(hhi - (3 * (1 / 3) ** 2)) < 1e-6  # three equal APs -> 1/3
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_etf_primary_market_zero_and_missing_denominators_not_coerced():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", IS_ETF="Y")
        # APs all zero value -> HHI denominator zero; only before-fee tracking -> drag missing.
        _etf(cur, run_id, "F1", NUM_SHARES_PER_CREATION_UNIT="25000", ANNUAL_DIFF_B4_FEE_EXPENSE="0.10")
        _ap(cur, run_id, "F1", PARTICIPANT_NAME="AP0", PARTICIPANT_LEI="LEI-AP0",
            PURCHASE_VALUE="0", REDEEM_VALUE="0")
        cur.execute("SELECT build_ncen_etf_primary_market_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT etf_primary_market FROM ncen_etf_primary_market_profiles")
        payload = cur.fetchone()[0]
        assert payload["derived"]["ap_concentration_hhi"] is None       # zero total flow, not synthetic 0/1
        assert payload["derived"]["fee_attributable_tracking_drag"] is None  # after-fee missing
        # net flow is a real 0 here (source reported zeros on both legs)
        assert float(payload["derived"]["net_primary_market_flow"]) == 0.0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_etf_primary_market_prefers_amendment_and_ddl_is_native():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, base_run, _, publication_id = _fixture(cur)
        amended_run = prepare_second_run(cur)
        submission(cur, base_run, "BASE", filing_date="2026-01-10")
        submission(cur, amended_run, "AMEND", form="N-CEN/A", filing_date="2026-02-10")
        fund(cur, base_run, "BASE", "F1", "S1", IS_ETF="Y")
        _etf(cur, base_run, "F1", NUM_SHARES_PER_CREATION_UNIT="10000")
        fund(cur, amended_run, "AMEND", "F1", "S1", IS_ETF="Y")
        _etf(cur, amended_run, "F1", NUM_SHARES_PER_CREATION_UNIT="75000")
        cur.execute("SELECT build_ncen_etf_primary_market_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT accession_number,(etf_primary_market->>'creation_unit_shares')::float "
                    "FROM ncen_etf_primary_market_profiles")
        acc, unit = cur.fetchone()
        assert acc == "AMEND" and unit == 75000
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

    ddl = (ROOT / "schemas" / "ncen_etf_primary_market_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("ncen_etf_primary_market_v1", "ncen_effective_filings", "ncen_provider_identity",
                  "ap_concentration_hhi", "net_primary_market_flow", "fee_attributable_tracking_drag",
                  "row multiplication"):
        assert token in ddl
    for banned in ("vendor", "sha256", "cik:"):
        assert banned not in lower
    current_view = lower.split("create or replace view sec_current_ncen_etf_primary_market_profiles", 1)[1]
    assert "ncen_raw_v2_rows" not in current_view
