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

DDL = ("rr1_class_cost_dispersion.sql",)
PRODUCT = "rr1_class_cost_dispersion_v1"
NET = "NetExpensesOverAssets"


def _build(cur, publication_id, as_of="2026-06-30"):
    return cur.execute("SELECT build_rr1_class_cost_dispersion(%s,%s)", (publication_id, as_of)).fetchone()[0]


def test_dispersion_spans_net_expense_across_classes_with_per_class_evidence():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, NET, "0.70", class_id="C1", document="D1", raw_row_id=1)
        fact(cur, run_id, NET, "0.90", class_id="C2", document="D2", raw_row_id=2)
        fact(cur, run_id, NET, "0.80", class_id="C3", document="D3", raw_row_id=3)
        assert _build(cur, publication_id) == 1  # one series-grain row
        cur.execute(
            """SELECT series_id,numeric_class_count,class_total,net_min,net_max,net_spread,
                      net_min_class_id,net_max_class_id,status,reason_code,
                      jsonb_array_length(per_class_evidence)
               FROM rr1_class_cost_dispersion"""
        )
        assert cur.fetchone() == (
            "S1", 3, 3, Decimal("0.70"), Decimal("0.90"), Decimal("0.20"),
            "C1", "C2", "available", None, 3,
        )
        # Per-class evidence preserves each class's document context and net value.
        cur.execute(
            """SELECT e->>'class_id', e->>'document_id', e->>'net_expense', e->>'net_state'
               FROM rr1_class_cost_dispersion, jsonb_array_elements(per_class_evidence) e
               ORDER BY e->>'class_id'"""
        )
        assert cur.fetchall() == [
            ("C1", "D1", "0.70", "available"),
            ("C2", "D2", "0.90", "available"),
            ("C3", "D3", "0.80", "available"),
        ]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_single_class_series_is_not_applicable_and_spread_is_null_not_zero():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, NET, "0.75", class_id="C1", raw_row_id=1)
        _build(cur, publication_id)
        cur.execute(
            "SELECT numeric_class_count,class_total,net_min,net_max,net_spread,status,reason_code FROM rr1_class_cost_dispersion"
        )
        # A single class is not a dispersion; the spread is NULL, never a synthetic 0.
        assert cur.fetchone() == (1, 1, Decimal("0.75"), Decimal("0.75"), None, "not_applicable", "single_class_series")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_non_numeric_net_is_excluded_from_stats_but_kept_in_evidence():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, NET, "0.70", class_id="C1", raw_row_id=1)
        fact(cur, run_id, NET, "0.90", class_id="C2", raw_row_id=2)
        fact(cur, run_id, NET, "n/a", class_id="C3", raw_row_id=3)
        _build(cur, publication_id)
        cur.execute("SELECT numeric_class_count,class_total,net_min,net_max,net_spread,status FROM rr1_class_cost_dispersion")
        # 2 numeric classes drive the spread; the non-numeric C3 still counts toward class_total.
        assert cur.fetchone() == (2, 3, Decimal("0.70"), Decimal("0.90"), Decimal("0.20"), "available")
        cur.execute(
            """SELECT e->>'class_id', e->>'net_expense', e->>'net_state'
               FROM rr1_class_cost_dispersion, jsonb_array_elements(per_class_evidence) e
               WHERE e->>'class_id'='C3'"""
        )
        assert cur.fetchone() == ("C3", None, "degraded")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_all_non_numeric_net_yields_unavailable_dispersion():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, NET, "n/a", class_id="C1", raw_row_id=1)
        fact(cur, run_id, NET, "", class_id="C2", raw_row_id=2)
        _build(cur, publication_id)
        cur.execute("SELECT numeric_class_count,class_total,net_min,net_max,net_spread,status,reason_code FROM rr1_class_cost_dispersion")
        # No numeric class -> unavailable; both non-numeric classes still count toward class_total.
        assert cur.fetchone() == (0, 2, None, None, None, "unavailable", "no_numeric_net_expense")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_dispersion_fails_closed_when_a_class_multiplies_net_rows():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, NET, "0.70", class_id="C1", raw_row_id=1)
        # Same class + series + data_date under two documents: a class-grain fan-out
        # that would double-count the class in the series roll-up.
        fact(cur, run_id, NET, "0.72", class_id="C1", document="D2", raw_row_id=2)
        with pytest.raises(psycopg.Error, match="conflicting RR1 class net-expense facts"):
            _build(cur, publication_id)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_distinct_series_get_distinct_dispersion_rows():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, NET, "0.70", series="S1", class_id="C1", raw_row_id=1)
        fact(cur, run_id, NET, "0.90", series="S1", class_id="C2", raw_row_id=2)
        fact(cur, run_id, NET, "0.50", series="S2", class_id="C1", raw_row_id=3)
        fact(cur, run_id, NET, "0.60", series="S2", class_id="C2", raw_row_id=4)
        assert _build(cur, publication_id) == 2
        cur.execute("SELECT series_id,net_spread FROM rr1_class_cost_dispersion ORDER BY series_id")
        assert cur.fetchall() == [("S1", Decimal("0.20")), ("S2", Decimal("0.10"))]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_dispersion_immutable_after_validation_and_current_view_is_derived_only():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, NET, "0.70", class_id="C1", raw_row_id=1)
        fact(cur, run_id, NET, "0.90", class_id="C2", raw_row_id=2)
        _build(cur, publication_id)
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))
        with pytest.raises(psycopg.Error, match="prepared"):
            _build(cur, publication_id)
        with pytest.raises(psycopg.Error, match="immutable"):
            cur.execute("UPDATE rr1_class_cost_dispersion SET series_id='S9' WHERE publication_id=%s", (publication_id,))
        assert cur.execute(
            "SELECT net_spread FROM sec_current_rr1_class_cost_dispersion"
        ).fetchone() == (Decimal("0.20"),)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_dispersion_build_pin_is_immutable_and_idempotent():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, NET, "0.70", class_id="C1", raw_row_id=1)
        fact(cur, run_id, NET, "0.90", class_id="C2", raw_row_id=2)
        assert _build(cur, publication_id) == 1
        assert _build(cur, publication_id) == 0
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            _build(cur, publication_id, as_of="2026-07-01")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_dispersion_ddl_is_effective_only_and_leaks_no_source_identity():
    ddl = (ROOT / "schemas" / "rr1_class_cost_dispersion.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("rr1_class_cost_dispersion_v1", "rr1_effective_facts", "input_fingerprint",
                  "NetExpensesOverAssets", "sec_derived_current_pointers"):
        assert token in ddl
    for forbidden in ("vendor", "sha256", "cik:", "sec_w1_nport_real", "filename"):
        assert forbidden not in lower
    current_view = lower.split("create or replace view sec_current_rr1_class_cost_dispersion", 1)[1]
    assert "rr1_raw_v2_rows" not in current_view
    assert "rr1_effective_facts" not in current_view
