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

DDL = ("rr1_reported_performance_profiles.sql",)
PRODUCT = "rr1_reported_performance_profile_v1"
RET = "AvgAnnlRtrPct"
INCEPTION = "AverageAnnualReturnInceptionDate"
BEST = "HighestQuarterlyReturn"
BEST_LBL = "BarChartHighestQuarterlyReturnLabel"
WORST = "LowestQuarterlyReturn"
WORST_LBL = "BarChartLowestQuarterlyReturnLabel"
YTD = "YearToDateReturn"


def _build(cur, publication_id, as_of="2026-06-30"):
    return cur.execute("SELECT build_rr1_reported_performance_profiles(%s,%s)", (publication_id, as_of)).fetchone()[0]


def test_multi_class_multi_horizon_returns_with_benchmark_and_tax_dimensions_are_preserved():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        # Class C1: two horizons (before taxes) via distinct otherdims.
        fact(cur, run_id, RET, "0.10", class_id="C1", dimensions="PeriodAxis=Year01", raw_row_id=1)
        fact(cur, run_id, RET, "0.085", class_id="C1", dimensions="PeriodAxis=Year05", raw_row_id=2)
        # Class C1 after-tax treatment (Performance Measure axis carried in measure).
        fact(cur, run_id, RET, "0.07", class_id="C1", measure="AfterTaxesOnDistributionsMember",
             dimensions="PeriodAxis=Year01", raw_row_id=3)
        # Class C2: one horizon.
        fact(cur, run_id, RET, "0.09", class_id="C2", document="D2", dimensions="PeriodAxis=Year01", raw_row_id=4)
        # Declared broad-based index benchmark return (benchmark dimension + measure).
        fact(cur, run_id, RET, "0.12", class_id="", measure="BroadBasedIndexMember",
             dimensions="IndexAxis=SP500Member", raw_row_id=5)
        assert _build(cur, publication_id) == 5
        cur.execute(
            """SELECT class_id,measure_id,dimensions,value_numeric,treatment,status
               FROM rr1_reported_performance_profiles
               WHERE canonical_concept='avg_annual_return'
               ORDER BY class_id,measure_id,dimensions"""
        )
        assert cur.fetchall() == [
            ("", "BroadBasedIndexMember", "IndexAxis=SP500Member", Decimal("0.12"), "broad_market_index", "available"),
            ("C1", "", "PeriodAxis=Year01", Decimal("0.10"), "before_taxes", "available"),
            ("C1", "", "PeriodAxis=Year05", Decimal("0.085"), "before_taxes", "available"),
            ("C1", "AfterTaxesOnDistributionsMember", "PeriodAxis=Year01", Decimal("0.07"), "after_tax_distributions", "available"),
            ("C2", "", "PeriodAxis=Year01", Decimal("0.09"), "before_taxes", "available"),
        ]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_best_and_worst_quarter_without_ytd_are_emitted_and_ytd_is_absent():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, BEST, "0.18", class_id="C1", raw_row_id=1)
        fact(cur, run_id, BEST_LBL, "2020-06-30", class_id="C1", source_table="txt.tsv", uom=None, raw_row_id=2)
        fact(cur, run_id, WORST, "-0.22", class_id="C1", raw_row_id=3)
        fact(cur, run_id, WORST_LBL, "2020-03-31", class_id="C1", source_table="txt.tsv", uom=None, raw_row_id=4)
        assert _build(cur, publication_id) == 4
        cur.execute(
            """SELECT canonical_concept,value_kind,value_numeric,value_label,status
               FROM rr1_reported_performance_profiles ORDER BY canonical_concept"""
        )
        assert cur.fetchall() == [
            ("best_quarter_period", "label", None, "2020-06-30", "available"),
            ("best_quarter_return", "numeric", Decimal("0.18"), None, "available"),
            ("worst_quarter_period", "label", None, "2020-03-31", "available"),
            ("worst_quarter_return", "numeric", Decimal("-0.22"), None, "available"),
        ]
        # YTD was never disclosed -> no synthetic row (open-world absence).
        cur.execute("SELECT count(*) FROM rr1_reported_performance_profiles WHERE canonical_concept='year_to_date_return'")
        assert cur.fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_inception_date_typed_and_malformed_inception_is_degraded_not_fabricated():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, INCEPTION, "2015-01-05", class_id="C1", source_table="txt.tsv", uom=None, raw_row_id=1)
        fact(cur, run_id, INCEPTION, "2015-13-99", class_id="C2", source_table="txt.tsv", uom=None, raw_row_id=2)
        _build(cur, publication_id)
        cur.execute(
            """SELECT class_id,value_kind,value_date,value_numeric,status,reason_code
               FROM rr1_reported_performance_profiles WHERE canonical_concept='since_inception_date' ORDER BY class_id"""
        )
        assert cur.fetchall() == [
            ("C1", "date", date(2015, 1, 5), None, "available", None),
            ("C2", "date", None, None, "degraded", "value_unparseable_date"),
        ]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_non_numeric_return_is_degraded_never_zero():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RET, "n/a", class_id="C1", dimensions="PeriodAxis=Year01", raw_row_id=1)
        _build(cur, publication_id)
        cur.execute(
            "SELECT value_numeric,status,reason_code FROM rr1_reported_performance_profiles WHERE canonical_concept='avg_annual_return'"
        )
        assert cur.fetchone() == (None, "degraded", "value_non_numeric")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_performance_fails_closed_when_a_context_multiplies_a_concept():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RET, "0.10", class_id="C1", dimensions="PeriodAxis=Year01", raw_row_id=1)
        # Same class + measure + dimensions + context: a fan-out that would multiply the row.
        fact(cur, run_id, RET, "0.11", class_id="C1", dimensions="PeriodAxis=Year01", raw_row_id=2)
        with pytest.raises(psycopg.Error, match="conflicting RR1 reported-performance facts"):
            _build(cur, publication_id)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_unmapped_custom_performance_tag_never_enters_the_snapshot():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, "CustomReturnTag", "0.99", version="0001234567-25-000001",
             dimensions="PeriodAxis=Year01", raw_row_id=1)
        assert _build(cur, publication_id) == 0
        cur.execute("SELECT count(*) FROM rr1_reported_performance_profiles")
        assert cur.fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_performance_immutable_after_validation_and_current_view_is_derived_only():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RET, "0.10", class_id="C1", dimensions="PeriodAxis=Year01", raw_row_id=1)
        _build(cur, publication_id)
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))
        with pytest.raises(psycopg.Error, match="prepared"):
            _build(cur, publication_id)
        with pytest.raises(psycopg.Error, match="immutable"):
            cur.execute("UPDATE rr1_reported_performance_profiles SET value_numeric=0.99 WHERE publication_id=%s", (publication_id,))
        assert cur.execute(
            "SELECT value_numeric FROM sec_current_rr1_reported_performance_profiles"
        ).fetchone() == (Decimal("0.10"),)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_performance_build_pin_is_immutable_and_idempotent():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RET, "0.10", class_id="C1", dimensions="PeriodAxis=Year01", raw_row_id=1)
        assert _build(cur, publication_id) == 1
        assert _build(cur, publication_id) == 0
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            _build(cur, publication_id, as_of="2026-07-01")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_performance_ddl_is_standalone_effective_only_and_leaks_no_source_identity():
    ddl = (ROOT / "schemas" / "rr1_reported_performance_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("rr1_reported_performance_profile_v1", "rr1_effective_facts", "input_fingerprint",
                  "AvgAnnlRtrPct", "sec_derived_current_pointers"):
        assert token in ddl
    for forbidden in ("vendor", "sha256", "cik:", "sec_w1_nport_real", "filename"):
        assert forbidden not in lower
    # Reported (prospectus) performance is a standalone product: it must never read
    # or merge realized NAV performance surfaces.
    for nav_token in ("nav_performance", "realized_return", "stock_daily_returns", "eod_prices"):
        assert nav_token not in lower
    current_view = lower.split("create or replace view sec_current_rr1_reported_performance_profiles", 1)[1]
    assert "rr1_raw_v2_rows" not in current_view
    assert "rr1_effective_facts" not in current_view
