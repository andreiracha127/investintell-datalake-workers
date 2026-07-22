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

DDL = ("ncen_derived_common.sql", "ncen_provider_network_profiles.sql")
PRODUCT = "ncen_provider_network_v1"


def _fixture(cur):
    return base_fixture(cur, PRODUCT, DDL)


def test_provider_network_normalizes_identifiers_and_derives_affiliated_exposure():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1")
        # LEI wins over CRD; CRD used when LEI absent; PCAOB for the accountant; name is the fallback.
        raw(cur, run_id, "ADVISER.tsv", {"FUND_ID": "F1", "ADVISER_NAME": "Alpha Advisers", "ADVISER_TYPE": "Adviser", "ADVISER_LEI": "LEI-ADV", "CRD_NUM": "111", "IS_AFFILIATED": "Y"}, fund_id="F1")
        raw(cur, run_id, "ADVISER.tsv", {"FUND_ID": "F1", "ADVISER_NAME": "Sub Beta", "ADVISER_TYPE": "Sub-Adviser", "ADVISER_LEI": "LEI-SUB", "IS_AFFILIATED": "N"}, fund_id="F1")
        raw(cur, run_id, "CUSTODIAN.tsv", {"FUND_ID": "F1", "CUSTODIAN_NAME": "CustCo", "CUSTODIAN_LEI": "LEI-CUST", "IS_AFFILIATED": "N"}, fund_id="F1")
        raw(cur, run_id, "BROKER.tsv", {"FUND_ID": "F1", "BROKER_NAME": "BrokerCo", "CRD_NUM": "CRD-BR"}, fund_id="F1")
        raw(cur, run_id, "PRICING_SERVICE.tsv", {"FUND_ID": "F1", "PRICING_SERVICE_NAME": "PriceOnly"}, fund_id="F1")
        raw(cur, run_id, "PUBLIC_ACCOUNTANT.tsv", {"ACCESSION_NUMBER": "A1", "PUB_ACCOUNTANT_NAME": "Auditors LLP", "PCAOB_NUM": "PCAOB-42"}, accession="A1")
        raw(cur, run_id, "PRINCIPAL_UNDERWRITER.tsv", {"ACCESSION_NUMBER": "A1", "UNDERWRITER_NAME": "UWCo", "UNDERWRITER_LEI": "LEI-UW", "IS_AFFILIATED": "Y"}, accession="A1")
        cur.execute("SELECT build_ncen_provider_network_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)
        cur.execute("SELECT count(*) FROM ncen_provider_network_profiles")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT provider_network_state,provider_network FROM ncen_provider_network_profiles")
        state, payload = cur.fetchone()
        assert state == "available"
        by_role = {p["role"]: p for p in payload["providers"]}
        assert by_role["adviser"]["identifier_kind"] == "lei"
        assert by_role["adviser"]["identifier_value"] == "LEI-ADV"
        assert by_role["subadviser"]["identifier_value"] == "LEI-SUB"
        assert by_role["broker"]["identifier_kind"] == "crd"
        assert by_role["broker"]["identifier_value"] == "CRD-BR"
        assert by_role["public_accountant"]["identifier_kind"] == "pcaob"
        assert by_role["public_accountant"]["identifier_value"] == "PCAOB-42"
        assert by_role["pricing_service"]["identifier_kind"] == "name"
        assert by_role["pricing_service"]["identifier_value"] == "priceonly"
        d = payload["derived"]
        assert d["total_provider_count"] == 7
        # affiliated: Alpha adviser + affiliated underwriter = 2 of 7
        assert d["affiliated_provider_count"] == 2
        assert abs(float(d["affiliated_service_exposure"]) - (2 / 7)) < 1e-6
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_provider_network_one_to_many_children_never_multiply_fund_grain():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1")
        for i in range(3):
            raw(cur, run_id, "ADVISER.tsv", {"FUND_ID": "F1", "ADVISER_NAME": f"Adv{i}", "ADVISER_LEI": f"LEI-A{i}"}, fund_id="F1")
        for i in range(2):
            raw(cur, run_id, "CUSTODIAN.tsv", {"FUND_ID": "F1", "CUSTODIAN_NAME": f"Cust{i}", "CUSTODIAN_LEI": f"LEI-C{i}"}, fund_id="F1")
        raw(cur, run_id, "PUBLIC_ACCOUNTANT.tsv", {"ACCESSION_NUMBER": "A1", "PUB_ACCOUNTANT_NAME": "Aud", "PCAOB_NUM": "PC-1"}, accession="A1")
        cur.execute("SELECT build_ncen_provider_network_profiles(%s,'2026-06-30')", (publication_id,))
        assert cur.fetchone() == (1,)  # exactly one fund row despite 3x2 fan-out
        cur.execute("SELECT count(*), (provider_network->'derived'->>'total_provider_count')::int FROM ncen_provider_network_profiles GROUP BY 2")
        rows, total = cur.fetchone()
        assert rows == 1 and total == 6  # 3 advisers + 2 custodians + 1 accountant, not 6+ multiplied
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_provider_network_flags_identifier_conflict_without_arbitrary_merge():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        fund(cur, run_id, "A1", "F1", "S1")
        # Same reported name, two different LEIs -> a genuine identifier conflict.
        raw(cur, run_id, "CUSTODIAN.tsv", {"FUND_ID": "F1", "CUSTODIAN_NAME": "Ambiguous Bank", "CUSTODIAN_LEI": "LEI-1"}, fund_id="F1")
        raw(cur, run_id, "CUSTODIAN.tsv", {"FUND_ID": "F1", "CUSTODIAN_NAME": "Ambiguous Bank", "CUSTODIAN_LEI": "LEI-2"}, fund_id="F1")
        cur.execute("SELECT build_ncen_provider_network_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT provider_network_state,provider_network FROM ncen_provider_network_profiles")
        state, payload = cur.fetchone()
        assert state == "available"  # a conflict is a quality flag, not a build failure
        assert payload["derived"]["identifier_conflict_count"] >= 1
        # Both rows retained as evidence; nothing silently collapsed.
        assert len(payload["providers"]) == 2
        assert {p["identifier_value"] for p in payload["providers"]} == {"LEI-1", "LEI-2"}
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_provider_network_zero_and_missing_denominators_are_not_coerced():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = _fixture(cur)
        submission(cur, run_id, "A1")
        submission(cur, run_id, "A2", cik="C2")
        fund(cur, run_id, "A1", "F1", "S1")  # no providers at all -> missing denominator
        fund(cur, run_id, "A2", "F2", "S2")
        raw(cur, run_id, "CUSTODIAN.tsv", {"FUND_ID": "F2", "CUSTODIAN_NAME": "Solo", "CUSTODIAN_LEI": "LEI-X", "IS_AFFILIATED": "N"}, fund_id="F2")
        cur.execute("SELECT build_ncen_provider_network_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT fund_id,provider_network_state,provider_network_reason_code,provider_network FROM ncen_provider_network_profiles ORDER BY fund_id")
        f1, f2 = cur.fetchall()
        # F1: zero providers -> unavailable, no synthetic zero ratio.
        assert f1[1:3] == ("unavailable", "provider_network_not_reported")
        assert f1[3] is None
        # F2: one non-affiliated provider -> exposure is a real 0, HHI a real 1.0 (single entity).
        assert f2[1] == "available"
        assert float(f2[3]["derived"]["affiliated_service_exposure"]) == 0.0
        assert float(f2[3]["derived"]["provider_concentration_hhi"]) == 1.0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_provider_network_prefers_amendment_and_ddl_is_native():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, base_run, _, publication_id = _fixture(cur)
        amended_run = prepare_second_run(cur)
        submission(cur, base_run, "BASE", filing_date="2026-01-10")
        submission(cur, amended_run, "AMEND", form="N-CEN/A", filing_date="2026-02-10")
        fund(cur, base_run, "BASE", "F1", "S1")
        raw(cur, base_run, "ADVISER.tsv", {"FUND_ID": "F1", "ADVISER_NAME": "Stale", "ADVISER_LEI": "LEI-OLD"}, fund_id="F1")
        fund(cur, amended_run, "AMEND", "F1", "S1")
        raw(cur, amended_run, "ADVISER.tsv", {"FUND_ID": "F1", "ADVISER_NAME": "Fresh", "ADVISER_LEI": "LEI-NEW"}, fund_id="F1")
        cur.execute("SELECT build_ncen_provider_network_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT accession_number,provider_network->'providers'->0->>'identifier_value' FROM ncen_provider_network_profiles")
        assert cur.fetchone() == ("AMEND", "LEI-NEW")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')

    ddl = (ROOT / "schemas" / "ncen_provider_network_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("ncen_provider_network_v1", "ncen_effective_filings", "ncen_provider_identity",
                  "affiliated_service_exposure", "provider_concentration_hhi", "row multiplication"):
        assert token in ddl
    for banned in ("vendor", "sha256", "cik:"):
        assert banned not in lower
    current_view = lower.split("create or replace view sec_current_ncen_provider_network_profiles", 1)[1]
    assert "ncen_raw_v2_rows" not in current_view
