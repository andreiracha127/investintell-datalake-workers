from __future__ import annotations

import os
import re
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

# The RR elements that actually carry these concepts in the published corpus.
Y01 = "AverageAnnualReturnYear01"
Y05 = "AverageAnnualReturnYear05"
Y10 = "AverageAnnualReturnYear10"
YSI = "AverageAnnualReturnSinceInception"
INCEPTION = "AverageAnnualReturnInceptionDate"
BEST = "BarChartHighestQuarterlyReturn"
BEST_DATE = "BarChartHighestQuarterlyReturnDate"
WORST = "BarChartLowestQuarterlyReturn"
WORST_DATE = "BarChartLowestQuarterlyReturnDate"
YTD = "BarChartYearToDateReturn"
YTD_DATE = "BarChartYearToDateReturnDate"

# Element names the previous concept map selected.  None of them can ever match a
# fact under an ``rr/%`` version: ``AvgAnnlRtrPct`` is the OEF (Tailored Shareholder
# Report) element, and the other three do not exist in any namespace.
PHANTOM_TAGS = (
    "AvgAnnlRtrPct",
    "HighestQuarterlyReturn",
    "LowestQuarterlyReturn",
    "YearToDateReturn",
    "BarChartHighestQuarterlyReturnLabel",
    "BarChartLowestQuarterlyReturnLabel",
    "BarChartYearToDateReturnLabel",
)


def _build(cur, publication_id, as_of="2026-06-30"):
    return cur.execute(
        "SELECT build_rr1_reported_performance_profiles(%s,%s)", (publication_id, as_of)
    ).fetchone()[0]


def test_the_four_average_annual_return_horizons_share_one_context_and_are_four_concepts():
    """The horizon is in the ELEMENT NAME, never in ``otherdims``.

    All four horizons are disclosed at the identical preserved context, so they can
    only coexist as four canonical concepts; one shared concept would trip the
    fan-out guard.  ``measure_id`` (the Performance Measure axis) is kept VERBATIM and
    is what separates a second leg at the same context.  The member shapes used here
    are the ones actually observed at class grain -- EMPTY (the overwhelming majority,
    classified ``before_taxes``) and a filer-named index string (``unclassified``).
    The taxonomy's after-tax members do NOT occur on these elements in this corpus,
    so no fixture pretends they do.
    """
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        for i, (tag, value) in enumerate(
            ((Y01, "0.10"), (Y05, "0.085"), (Y10, "0.07"), (YSI, "0.065")), start=1
        ):
            fact(cur, run_id, tag, value, class_id="C1", raw_row_id=i)
        # Same class, same context, second leg: separated by the measure member.
        fact(cur, run_id, Y01, "0.12", class_id="C1", measure="SP500Index", raw_row_id=5)
        # A second share class of the same series.
        fact(cur, run_id, Y01, "0.09", class_id="C2", document="D2", raw_row_id=6)
        assert _build(cur, publication_id) == 6
        cur.execute(
            """SELECT canonical_concept,class_id,measure_id,value_numeric,treatment,status
               FROM rr1_reported_performance_profiles
               ORDER BY class_id,measure_id,canonical_concept"""
        )
        assert cur.fetchall() == [
            ("avg_annual_return_since_inception", "C1", "", Decimal("0.065"), "before_taxes", "available"),
            ("avg_annual_return_year01", "C1", "", Decimal("0.10"), "before_taxes", "available"),
            ("avg_annual_return_year05", "C1", "", Decimal("0.085"), "before_taxes", "available"),
            ("avg_annual_return_year10", "C1", "", Decimal("0.07"), "before_taxes", "available"),
            ("avg_annual_return_year01", "C1", "SP500Index", Decimal("0.12"), "unclassified", "available"),
            ("avg_annual_return_year01", "C2", "", Decimal("0.09"), "before_taxes", "available"),
        ]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_phantom_elements_of_the_previous_concept_map_resolve_to_nothing():
    """``AvgAnnlRtrPct`` is OEF-only; the other six element names do not exist.

    Seeding every one of them -- including ``AvgAnnlRtrPct`` under an ``rr/%``
    version, which the source never carries -- must produce an EMPTY snapshot.
    """
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        for i, tag in enumerate(PHANTOM_TAGS, start=1):
            fact(cur, run_id, tag, "0.10", class_id="C1", dimensions=f"PeriodAxis=X{i}",
                 raw_row_id=i)
        # The real corpus shape: AvgAnnlRtrPct only ever appears under oef/*.
        fact(cur, run_id, "AvgAnnlRtrPct", "0.10", version="oef/2023", class_id="C1",
             raw_row_id=100)
        assert _build(cur, publication_id) == 0
        cur.execute("SELECT count(*) FROM rr1_reported_performance_profiles")
        assert cur.fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_quarter_and_ytd_use_barchart_elements_and_the_period_is_a_typed_date():
    """The period is the ISO date element, not the preparer's caption element.

    ``HighestQuarterlyReturnLabel`` and friends DO exist, but they carry free-text
    captions ("Best Quarter", whole sentences).  Seeding them alongside the date
    elements proves the caption never becomes the period.
    """
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, BEST, "0.18", class_id="C1", raw_row_id=1)
        fact(cur, run_id, BEST_DATE, "2020-06-30", class_id="C1", source_table="txt.tsv",
             uom=None, raw_row_id=2)
        fact(cur, run_id, WORST, "-0.22", class_id="C1", raw_row_id=3)
        fact(cur, run_id, WORST_DATE, "2020-03-31", class_id="C1", source_table="txt.tsv",
             uom=None, raw_row_id=4)
        # Preparer captions -- present in the source, never the period.
        fact(cur, run_id, "HighestQuarterlyReturnLabel", "Best Quarter", class_id="C1",
             source_table="txt.tsv", uom=None, raw_row_id=5)
        fact(cur, run_id, "LowestQuarterlyReturnLabel", "Worst calendar quarter", class_id="C1",
             source_table="txt.tsv", uom=None, raw_row_id=6)
        assert _build(cur, publication_id) == 4
        cur.execute(
            """SELECT canonical_concept,value_kind,value_numeric,value_date,value_label,status
               FROM rr1_reported_performance_profiles ORDER BY canonical_concept"""
        )
        assert cur.fetchall() == [
            ("best_quarter_period", "date", None, date(2020, 6, 30), None, "available"),
            ("best_quarter_return", "numeric", Decimal("0.18"), None, None, "available"),
            ("worst_quarter_period", "date", None, date(2020, 3, 31), None, "available"),
            ("worst_quarter_return", "numeric", Decimal("-0.22"), None, None, "available"),
        ]
        # YTD was never disclosed -> no synthetic row (open-world absence).
        cur.execute(
            "SELECT count(*) FROM rr1_reported_performance_profiles "
            "WHERE canonical_concept IN ('year_to_date_return','year_to_date_period')"
        )
        assert cur.fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_year_to_date_return_and_period_are_emitted_from_the_barchart_elements():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, YTD, "0.0511", class_id="C1", raw_row_id=1)
        fact(cur, run_id, YTD_DATE, "2019-09-30", class_id="C1", source_table="txt.tsv",
             uom=None, raw_row_id=2)
        assert _build(cur, publication_id) == 2
        cur.execute(
            """SELECT canonical_concept,value_kind,value_numeric,value_date,status
               FROM rr1_reported_performance_profiles ORDER BY canonical_concept"""
        )
        assert cur.fetchall() == [
            ("year_to_date_period", "date", None, date(2019, 9, 30), "available"),
            ("year_to_date_return", "numeric", Decimal("0.0511"), None, "available"),
        ]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_series_level_index_leg_is_left_to_the_benchmark_product():
    """The class-EMPTY leg of the same elements is the declared benchmark return.

    It is a property of the SERIES and belongs to ``rr1_benchmark_profile_v1``; this
    product is class grain and must not double-report it (its ``measure_id`` -- the
    only thing naming the index -- is not part of this product's served payload).
    """
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, Y01, "0.10", class_id="C1", raw_row_id=1)
        fact(cur, run_id, Y01, "0.12", class_id="", measure="SP500Index", raw_row_id=2)
        fact(cur, run_id, Y05, "0.11", class_id="", measure="ICEBofAUSBroadMarketIndex",
             raw_row_id=3)
        assert _build(cur, publication_id) == 1
        cur.execute("SELECT class_id,canonical_concept,value_numeric FROM rr1_reported_performance_profiles")
        assert cur.fetchall() == [("C1", "avg_annual_return_year01", Decimal("0.10"))]
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
        fact(cur, run_id, Y01, "n/a", class_id="C1", raw_row_id=1)
        _build(cur, publication_id)
        cur.execute(
            "SELECT value_numeric,status,reason_code FROM rr1_reported_performance_profiles "
            "WHERE canonical_concept='avg_annual_return_year01'"
        )
        assert cur.fetchone() == (None, "degraded", "value_non_numeric")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_performance_fails_closed_when_a_context_multiplies_a_concept():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, Y01, "0.10", class_id="C1", raw_row_id=1)
        # Same class + measure + dimensions + context: a fan-out that would multiply the row.
        fact(cur, run_id, Y01, "0.11", class_id="C1", raw_row_id=2)
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
        fact(cur, run_id, Y01, "0.10", class_id="C1", raw_row_id=1)
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
        fact(cur, run_id, Y01, "0.10", class_id="C1", raw_row_id=1)
        assert _build(cur, publication_id) == 1
        assert _build(cur, publication_id) == 0
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            _build(cur, publication_id, as_of="2026-07-01")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_concept_map_selects_only_elements_that_exist_in_the_rr_corpus():
    """The frozen map is pinned tag-by-tag; a phantom element cannot creep back."""
    ddl = (ROOT / "schemas" / "rr1_reported_performance_profiles.sql").read_text(encoding="utf-8")
    body = ddl.split("CREATE OR REPLACE FUNCTION rr1_reported_performance_concept_map()", 1)[1]
    body = body.split("$$;", 1)[0]
    mapped = set(re.findall(r"'([A-Za-z][A-Za-z0-9]*)',\s*'(?:num|txt)\.tsv'", body))
    assert mapped == {
        "AverageAnnualReturnYear01", "AverageAnnualReturnYear05", "AverageAnnualReturnYear10",
        "AverageAnnualReturnSinceInception", "BarChartHighestQuarterlyReturn",
        "BarChartLowestQuarterlyReturn", "BarChartYearToDateReturn",
        "AverageAnnualReturnInceptionDate", "BarChartHighestQuarterlyReturnDate",
        "BarChartLowestQuarterlyReturnDate", "BarChartYearToDateReturnDate",
    }
    for phantom in PHANTOM_TAGS:
        assert phantom not in mapped


def test_performance_ddl_is_standalone_effective_only_and_leaks_no_source_identity():
    ddl = (ROOT / "schemas" / "rr1_reported_performance_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("rr1_reported_performance_profile_v1", "rr1_effective_facts", "input_fingerprint",
                  "AverageAnnualReturnYear01", "sec_derived_current_pointers"):
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
