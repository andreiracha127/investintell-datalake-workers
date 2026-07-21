from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rr1_derived_fixtures import ROOT, base_fixture, dsn, fact  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

DDL = ("rr1_shareholder_cost_profiles.sql",)
PRODUCT = "rr1_shareholder_cost_profile_v1"


def _build(cur, publication_id, as_of="2026-06-30"):
    return cur.execute("SELECT build_rr1_shareholder_cost_profiles(%s,%s)", (publication_id, as_of)).fetchone()[0]


def test_shareholder_costs_preserve_class_context_canonical_tag_and_declared_unit():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        # Two directly-paid shareholder fees (fractions, uom=pure) + an expense
        # example (currency, uom=USD) on the same class.
        fact(cur, run_id, "MaximumSalesChargeImposedOnPurchasesOverOfferingPrice", "0.0525", uom="pure", raw_row_id=1)
        fact(cur, run_id, "RedemptionFeeOverRedemption", "0.02", uom="pure", raw_row_id=2)
        fact(cur, run_id, "ExpenseExampleYear01", "108", uom="USD", raw_row_id=3)
        _build(cur, publication_id)
        cur.execute(
            """SELECT canonical_concept,cost_group,unit_class,original_tag,original_version,
                      value_numeric,declared_unit,status
               FROM rr1_shareholder_cost_profiles
               WHERE status='available' ORDER BY canonical_concept"""
        )
        assert cur.fetchall() == [
            ("expense_example_1y", "expense_example", "currency", "ExpenseExampleYear01", "rr/2025", Decimal("108"), "USD", "available"),
            ("redemption_fee", "shareholder_fee", "fraction", "RedemptionFeeOverRedemption", "rr/2025", Decimal("0.02"), "pure", "available"),
            ("sales_charge_purchase", "shareholder_fee", "fraction", "MaximumSalesChargeImposedOnPurchasesOverOfferingPrice", "rr/2025", Decimal("0.0525"), "pure", "available"),
        ]
        # Every canonical concept has a row for the reported context; nine total.
        assert cur.execute("SELECT count(*) FROM rr1_shareholder_cost_profiles").fetchone() == (9,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_shareholder_costs_keep_missing_distinct_from_zero_and_degrade_nonnumeric():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, "ExchangeFeeOverRedemption", "0", uom="pure", raw_row_id=1)
        fact(cur, run_id, "ExpenseExampleYear03", "n/a", uom="USD", raw_row_id=2)
        _build(cur, publication_id)
        cur.execute(
            "SELECT canonical_concept,value_numeric,status,reason_code FROM rr1_shareholder_cost_profiles ORDER BY canonical_concept"
        )
        rows = {c: (v, s, r) for c, v, s, r in cur.fetchall()}
        # A genuine reported zero stays a real 0/available (not a gap).
        assert rows["exchange_fee"] == (0, "available", None)
        # Present-but-non-numeric degrades; never coerced to zero.
        assert rows["expense_example_3y"] == (None, "degraded", "selected_fact_has_no_numeric_value")
        # An unreported concept is unavailable and distinct from a reported zero.
        assert rows["account_fee"] == (None, "unavailable", "concept_not_reported")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_unmapped_custom_tag_never_enters_canonical_metrics():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        # A canonical shareholder fee on class C1 (creates a real context).
        fact(cur, run_id, "RedemptionFeeOverRedemption", "0.01", class_id="C1", raw_row_id=1)
        # A filer-specific custom fee on class C2 in a custom namespace: it must
        # neither map to a canonical concept nor manufacture a C2 context.
        fact(cur, run_id, "FundSpecificShareholderCharge", "0.03", class_id="C2",
             version="custom/0001", raw_row_id=2)
        # An RR-namespaced tag that belongs to a different snapshot (fee waterfall)
        # is also out of scope here.
        fact(cur, run_id, "ManagementFeesOverAssets", "0.4", class_id="C1", raw_row_id=3)
        _build(cur, publication_id)
        assert cur.execute(
            "SELECT count(*) FROM rr1_shareholder_cost_profiles WHERE original_tag='FundSpecificShareholderCharge'"
        ).fetchone() == (0,)
        assert cur.execute(
            "SELECT count(*) FROM rr1_shareholder_cost_profiles WHERE original_tag='ManagementFeesOverAssets'"
        ).fetchone() == (0,)
        assert cur.execute(
            "SELECT count(DISTINCT class_id) FROM rr1_shareholder_cost_profiles"
        ).fetchone() == (1,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_multi_measure_multi_document_facts_keep_distinct_dimension_contexts():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        # Same class/concept, different measure + document contexts: the selection
        # preserves the dimensions, so both survive as distinct rows.
        fact(cur, run_id, "RedemptionFeeOverRedemption", "0.01", measure="M1", document="D1", raw_row_id=1)
        fact(cur, run_id, "RedemptionFeeOverRedemption", "0.02", measure="M2", document="D2", raw_row_id=2)
        _build(cur, publication_id)
        cur.execute(
            """SELECT measure_id,document_id,value_numeric FROM rr1_shareholder_cost_profiles
               WHERE canonical_concept='redemption_fee' AND status='available'
               ORDER BY measure_id"""
        )
        assert cur.fetchall() == [("M1", "D1", Decimal("0.01")), ("M2", "D2", Decimal("0.02"))]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_shareholder_costs_immutable_after_validation_and_current_view_is_derived_only():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, "RedemptionFeeOverRedemption", "0.01", raw_row_id=1)
        _build(cur, publication_id)
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))
        with pytest.raises(psycopg.Error, match="prepared"):
            _build(cur, publication_id)
        with pytest.raises(psycopg.Error, match="immutable"):
            cur.execute("UPDATE rr1_shareholder_cost_profiles SET class_id='C9' WHERE publication_id=%s", (publication_id,))
        assert cur.execute(
            "SELECT value_numeric FROM sec_current_rr1_shareholder_cost_profiles WHERE canonical_concept='redemption_fee'"
        ).fetchone() == (Decimal("0.01"),)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_shareholder_cost_build_pin_is_immutable_and_idempotent():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, "RedemptionFeeOverRedemption", "0.01", raw_row_id=1)
        assert _build(cur, publication_id) == 9
        assert _build(cur, publication_id) == 0  # idempotent re-run inserts nothing
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            _build(cur, publication_id, as_of="2026-07-01")
        fact(cur, run_id, "ExpenseExampleYear05", "300", uom="USD", raw_row_id=2)
        with pytest.raises(psycopg.Error, match="already pinned to effective-input fingerprint"):
            _build(cur, publication_id)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_shareholder_cost_ddl_is_effective_only_and_leaks_no_source_identity():
    ddl = (ROOT / "schemas" / "rr1_shareholder_cost_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("rr1_shareholder_cost_profile_v1", "rr1_effective_facts", "input_fingerprint",
                  "ExpenseExampleYear01", "MaximumSalesChargeImposedOnPurchasesOverOfferingPrice",
                  "sec_derived_current_pointers"):
        assert token in ddl
    # No source/licence identity leaks into the derived surface.
    for forbidden in ("vendor", "sha256", "cik:", "sec_w1_nport_real", "filename"):
        assert forbidden not in lower
    current_view = lower.split("create or replace view sec_current_rr1_shareholder_cost_profiles", 1)[1]
    assert "rr1_raw_v2_rows" not in current_view
    assert "rr1_effective_facts" not in current_view
