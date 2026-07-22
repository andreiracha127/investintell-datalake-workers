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

DDL = ("ncen_derived_common.sql", "ncen_securities_lending_profiles.sql")
PRODUCT = "ncen_securities_lending_v1"


def _fixture(cur):
    return base_fixture(cur, PRODUCT, DDL)


def _agent(cur, run_id, fund_id, seqnum, **fields):
    raw(cur, run_id, "SECURITY_LENDING.tsv",
        {"FUND_ID": fund_id, "SECURITY_LENDING_SEQNUM": seqnum, **fields}, fund_id=fund_id)


def _indemnity(cur, run_id, fund_id, seqnum, name, lei=None):
    body = {"FUND_ID": fund_id, "SECURITY_LENDING_SEQNUM": seqnum, "INDEMNITY_PROVIDER_NAME": name}
    if lei is not None:
        body["INDEMNITY_PROVIDER_LEI"] = lei
    raw(cur, run_id, "SEC_LENDING_IDEMNITY_PROVIDER.tsv", body, fund_id=fund_id)


def _collateral_manager(cur, run_id, fund_id, **fields):
    raw(cur, run_id, "COLLATERAL_MANAGER.tsv", {"FUND_ID": fund_id, **fields}, fund_id=fund_id)


def test_securities_lending_authorized_activity_and_relations():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        # NB: IS_COLLATERAL_LIQUIDATED carries a date-parse policy in the frozen
        # contract, so any Y/N quarantines the whole FUND_REPORTED_INFO row; it is
        # therefore only ever readable as not_reported from the typed surface and
        # must never be fabricated.  IS_IMPACTED_ADVERSELY is a normal Y/N flag.
        fund(cur, run_id, "A1", "F1", "S1",
             IS_SEC_LENDING_AUTHORIZED="Y", DID_LEND_SECURITIES="Y",
             AVG_VALUE_SEC_LOAN="1000000", NET_INCOME_SEC_LENDING="25000",
             MONTHLY_AVG_NET_ASSETS="50000000", IS_IMPACTED_ADVERSELY="N")
        _agent(cur, run_id, "F1", "1", SECURITIES_AGENT_NAME="Lending Agent", SECURITIES_AGENT_LEI="LEI-AGT",
               IS_AFFILIATED="Y", SECURITY_AGENT_IDEMNITY="Y", DID_INDEMNIFICATION_RIGHTS="Y")
        _indemnity(cur, run_id, "F1", "1", "Indemnitor Co", lei="LEI-IND")
        _collateral_manager(cur, run_id, "F1", COLLATERAL_MANAGER_NAME="Collat Mgr", COLLATERAL_MANAGER_LEI="LEI-CM",
                            IS_AFFILIATED="N", IS_AFFILIATED_WITH_FUND="N")
        cur.execute("SELECT build_ncen_securities_lending_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT securities_lending_state,securities_lending FROM ncen_securities_lending_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        assert payload["authorization"] == "true"
        assert payload["activity"] == "true"
        assert float(payload["average_on_loan"]) == 1000000
        assert float(payload["net_income"]) == 25000
        # collateral liquidation is unreadable from the typed surface (see note above)
        assert payload["collateral_liquidation"] == "not_reported"
        assert payload["adverse_impact"] == "false"
        assert payload["agents"][0]["identifier_value"] == "LEI-AGT"
        assert payload["agents"][0]["is_affiliated"] == "true"
        assert payload["indemnity_providers"][0]["identifier_value"] == "LEI-IND"
        assert payload["collateral_managers"][0]["identifier_value"] == "LEI-CM"
        d = payload["derived"]
        assert abs(float(d["lending_yield"]) - 0.025) < 1e-6            # 25000/1000000
        assert abs(float(d["lending_intensity"]) - 0.02) < 1e-6         # 1000000/50000000
        assert d["agent_count"] == 1
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_securities_lending_not_authorized_is_not_applicable_not_zero():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", IS_SEC_LENDING_AUTHORIZED="N")
        cur.execute("SELECT build_ncen_securities_lending_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT securities_lending_state,securities_lending_reason_code,securities_lending "
                    "FROM ncen_securities_lending_profiles")
        state, reason, payload = cur.fetchone()
        assert state == "not_applicable"
        assert reason == "securities_lending_not_authorized"
        # No synthetic zero net income: the family simply does not apply.
        assert payload is None
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_securities_lending_authorized_but_inactive_metrics_absent_not_zero():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        # authorized but did not lend; no avg loan / net income reported.
        fund(cur, run_id, "A1", "F1", "S1", IS_SEC_LENDING_AUTHORIZED="Y", DID_LEND_SECURITIES="N")
        cur.execute("SELECT build_ncen_securities_lending_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT securities_lending_state,securities_lending FROM ncen_securities_lending_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        assert payload["authorization"] == "true"
        assert payload["activity"] == "false"
        assert payload["average_on_loan"] is None            # absent, not zero
        assert payload["net_income"] is None                 # absent, not zero
        assert payload["derived"]["lending_yield"] is None    # missing denominator
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_securities_lending_one_to_many_children_never_multiply_fund_grain():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1", IS_SEC_LENDING_AUTHORIZED="Y", DID_LEND_SECURITIES="Y")
        for i in range(2):
            _agent(cur, run_id, "F1", str(i + 1), SECURITIES_AGENT_NAME=f"Agent{i}", SECURITIES_AGENT_LEI=f"LEI-A{i}")
            _indemnity(cur, run_id, "F1", str(i + 1), f"Indem{i}", lei=f"LEI-I{i}")
        for i in range(2):
            _collateral_manager(cur, run_id, "F1", COLLATERAL_MANAGER_NAME=f"CM{i}", COLLATERAL_MANAGER_LEI=f"LEI-C{i}")
        cur.execute("SELECT build_ncen_securities_lending_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)  # one fund row despite 2 agents x 2 indemnitors x 2 managers
        cur.execute("SELECT count(*), (securities_lending->'derived'->>'agent_count')::int, "
                    "(securities_lending->'derived'->>'collateral_manager_count')::int "
                    "FROM ncen_securities_lending_profiles GROUP BY 2,3")
        rows, agents, managers = cur.fetchone()
        assert rows == 1 and agents == 2 and managers == 2
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_securities_lending_zero_and_missing_denominators_not_coerced():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        submission(cur, run_id, "A2", cik="C2")
        # F1: avg on loan zero -> yield denominator zero.
        fund(cur, run_id, "A1", "F1", "S1", IS_SEC_LENDING_AUTHORIZED="Y", DID_LEND_SECURITIES="Y",
             AVG_VALUE_SEC_LOAN="0", NET_INCOME_SEC_LENDING="0", MONTHLY_AVG_NET_ASSETS="50000000")
        # F2: net assets missing -> intensity denominator missing.
        fund(cur, run_id, "A2", "F2", "S2", IS_SEC_LENDING_AUTHORIZED="Y", DID_LEND_SECURITIES="Y",
             AVG_VALUE_SEC_LOAN="1000000", NET_INCOME_SEC_LENDING="10000")
        cur.execute("SELECT build_ncen_securities_lending_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT fund_id,securities_lending FROM ncen_securities_lending_profiles ORDER BY fund_id")
        (f1, p1), (f2, p2) = cur.fetchall()
        assert p1["derived"]["lending_yield"] is None          # 0 denominator, not a synthetic 0
        assert p2["derived"]["lending_intensity"] is None      # missing denominator
        assert abs(float(p2["derived"]["lending_yield"]) - 0.01) < 1e-6
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_securities_lending_prefers_amendment_and_ddl_is_native():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, base_run, _, publication_id = _fixture(cur)
        amended_run = prepare_second_run(cur)
        submission(cur, base_run, "BASE", filing_date="2026-01-10")
        submission(cur, amended_run, "AMEND", form="N-CEN/A", filing_date="2026-02-10")
        fund(cur, base_run, "BASE", "F1", "S1", IS_SEC_LENDING_AUTHORIZED="Y", DID_LEND_SECURITIES="Y",
             AVG_VALUE_SEC_LOAN="1000000", NET_INCOME_SEC_LENDING="10000")
        fund(cur, amended_run, "AMEND", "F1", "S1", IS_SEC_LENDING_AUTHORIZED="Y", DID_LEND_SECURITIES="Y",
             AVG_VALUE_SEC_LOAN="1000000", NET_INCOME_SEC_LENDING="40000")
        cur.execute("SELECT build_ncen_securities_lending_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT accession_number,(securities_lending->'derived'->>'lending_yield')::float "
                    "FROM ncen_securities_lending_profiles")
        acc, yld = cur.fetchone()
        assert acc == "AMEND" and abs(yld - 0.04) < 1e-6
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

    ddl = (ROOT / "schemas" / "ncen_securities_lending_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("ncen_securities_lending_v1", "ncen_effective_filings", "ncen_safe_ratio",
                  "lending_yield", "lending_intensity", "row multiplication"):
        assert token in ddl
    for banned in ("vendor", "sha256", "cik:"):
        assert banned not in lower
    current_view = lower.split("create or replace view sec_current_ncen_securities_lending_profiles", 1)[1]
    assert "ncen_raw_v2_rows" not in current_view
