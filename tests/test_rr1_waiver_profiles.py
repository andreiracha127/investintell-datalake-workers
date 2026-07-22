from __future__ import annotations

import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rr1_derived_fixtures import ROOT, base_fixture, dsn, fact  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

DDL = ("rr1_waiver_profiles.sql",)
PRODUCT = "rr1_waiver_profile_v1"

WAIVER = "FeeWaiverOrReimbursementOverAssets"
GROSS = "ExpensesOverAssets"
NET = "NetExpensesOverAssets"
TERMINATION = "FeeWaiverOrReimbursementOverAssetsDateOfTermination"


def _build(cur, publication_id, as_of="2026-06-30"):
    return cur.execute("SELECT build_rr1_waiver_profiles(%s,%s)", (publication_id, as_of)).fetchone()[0]


def test_waiver_profile_reconstructs_gross_to_net_and_measures_durability():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, WAIVER, "0.15", uom="pure", raw_row_id=1)
        fact(cur, run_id, GROSS, "0.90", uom="pure", raw_row_id=2)
        fact(cur, run_id, NET, "0.75", uom="pure", raw_row_id=3)
        fact(cur, run_id, TERMINATION, "2026-09-30", source_table="txt.tsv", uom=None, raw_row_id=4)
        assert _build(cur, publication_id) == 1
        cur.execute(
            """SELECT waiver_over_assets,gross_expense_over_assets,net_expense_over_assets,
                      effective_date,termination_date,term_days,remaining_days,
                      gross_minus_waiver,net_reconstruction_gap,reconciliation_status,cliff_flag,
                      status,reason_code
               FROM rr1_waiver_profiles"""
        )
        row = cur.fetchone()
        assert row == (
            Decimal("0.15"), Decimal("0.90"), Decimal("0.75"),
            date(2026, 1, 1), date(2026, 9, 30),
            (date(2026, 9, 30) - date(2026, 1, 1)).days,   # term = termination - effective
            (date(2026, 9, 30) - date(2026, 6, 30)).days,  # remaining = termination - as_of
            Decimal("0.75"), Decimal("0.00"), "reconciled", True,
            "available", None,
        )
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_waiver_without_termination_date_is_flagged_not_fabricated():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, WAIVER, "0.10", uom="pure", raw_row_id=1)
        fact(cur, run_id, GROSS, "0.80", uom="pure", raw_row_id=2)
        fact(cur, run_id, NET, "0.70", uom="pure", raw_row_id=3)
        _build(cur, publication_id)
        cur.execute(
            """SELECT termination_date,term_days,remaining_days,cliff_flag,termination_reason_code,
                      reconciliation_status,net_reconstruction_gap,status
               FROM rr1_waiver_profiles"""
        )
        # No termination fact: every termination-dependent metric is honestly NULL,
        # never a fabricated date/duration.  The waiver size and gross->net
        # reconstruction still stand on their own.
        assert cur.fetchone() == (
            None, None, None, None, "termination_date_not_reported",
            "reconciled", Decimal("0.00"), "available",
        )
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_gross_to_net_divergence_beyond_tolerance_is_a_quality_flag_not_a_value():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        # gross - waiver = 0.75 but net is reported 0.80: a real disclosure conflict.
        fact(cur, run_id, WAIVER, "0.15", uom="pure", raw_row_id=1)
        fact(cur, run_id, GROSS, "0.90", uom="pure", raw_row_id=2)
        fact(cur, run_id, NET, "0.80", uom="pure", raw_row_id=3)
        _build(cur, publication_id)
        cur.execute(
            "SELECT gross_minus_waiver,net_expense_over_assets,net_reconstruction_gap,reconciliation_status FROM rr1_waiver_profiles"
        )
        recon, net, gap, status = cur.fetchone()
        assert recon == Decimal("0.75")
        assert net == Decimal("0.80")
        assert gap == Decimal("-0.05")  # (gross - waiver) - net, preserved verbatim
        assert status == "divergent"
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_incomplete_reconstruction_when_a_leg_is_missing():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, WAIVER, "0.15", uom="pure", raw_row_id=1)
        fact(cur, run_id, GROSS, "0.90", uom="pure", raw_row_id=2)
        # NET absent
        _build(cur, publication_id)
        cur.execute(
            "SELECT net_expense_over_assets,gross_minus_waiver,net_reconstruction_gap,reconciliation_status FROM rr1_waiver_profiles"
        )
        assert cur.fetchone() == (None, Decimal("0.75"), None, "incomplete")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_cliff_is_true_near_termination_and_false_when_far():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        # Active waiver terminating far in the future -> not a cliff.
        fact(cur, run_id, WAIVER, "0.10", uom="pure", class_id="C1", raw_row_id=1)
        fact(cur, run_id, TERMINATION, "2028-01-01", source_table="txt.tsv", uom=None, class_id="C1", raw_row_id=2)
        # Active waiver terminating within the horizon -> a cliff.
        fact(cur, run_id, WAIVER, "0.10", uom="pure", class_id="C2", raw_row_id=3)
        fact(cur, run_id, TERMINATION, "2026-09-30", source_table="txt.tsv", uom=None, class_id="C2", raw_row_id=4)
        _build(cur, publication_id)
        cur.execute("SELECT class_id,cliff_flag FROM rr1_waiver_profiles ORDER BY class_id")
        assert cur.fetchall() == [("C1", False), ("C2", True)]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_unparseable_termination_date_is_null_and_flagged():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, WAIVER, "0.10", uom="pure", raw_row_id=1)
        fact(cur, run_id, TERMINATION, "21020906", source_table="txt.tsv", uom=None, raw_row_id=2)
        _build(cur, publication_id)
        cur.execute("SELECT termination_date,termination_reason_code,remaining_days FROM rr1_waiver_profiles")
        assert cur.fetchone() == (None, "termination_date_unparseable", None)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_waiver_profile_fails_closed_on_row_multiplying_leg():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, WAIVER, "0.15", uom="pure", raw_row_id=1)
        # Two gross facts for the same class context would fan out the waiver row.
        fact(cur, run_id, GROSS, "0.90", uom="pure", raw_row_id=2)
        fact(cur, run_id, GROSS, "0.91", uom="pure", raw_row_id=3)
        with pytest.raises(psycopg.Error, match="conflicting RR1 waiver facts"):
            _build(cur, publication_id)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_waiver_profiles_immutable_after_validation_and_current_view_is_derived_only():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, WAIVER, "0.15", uom="pure", raw_row_id=1)
        _build(cur, publication_id)
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))
        with pytest.raises(psycopg.Error, match="prepared"):
            _build(cur, publication_id)
        with pytest.raises(psycopg.Error, match="immutable"):
            cur.execute("UPDATE rr1_waiver_profiles SET class_id='C9' WHERE publication_id=%s", (publication_id,))
        assert cur.execute(
            "SELECT waiver_over_assets FROM sec_current_rr1_waiver_profiles"
        ).fetchone() == (Decimal("0.15"),)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_waiver_ddl_is_effective_only_and_leaks_no_source_identity():
    ddl = (ROOT / "schemas" / "rr1_waiver_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("rr1_waiver_profile_v1", "rr1_effective_facts", "input_fingerprint",
                  "FeeWaiverOrReimbursementOverAssets", "sec_derived_current_pointers"):
        assert token in ddl
    for forbidden in ("vendor", "sha256", "cik:", "sec_w1_nport_real", "filename"):
        assert forbidden not in lower
    current_view = lower.split("create or replace view sec_current_rr1_waiver_profiles", 1)[1]
    assert "rr1_raw_v2_rows" not in current_view
    assert "rr1_effective_facts" not in current_view
