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

DDL = ("rr1_turnover_profiles.sql",)
PRODUCT = "rr1_turnover_profile_v1"
RATE = "PortfolioTurnoverRate"
TEXT = "PortfolioTurnoverTextBlock"


def _build(cur, publication_id, as_of="2026-06-30"):
    return cur.execute("SELECT build_rr1_turnover_profiles(%s,%s)", (publication_id, as_of)).fetchone()[0]


def test_numeric_and_text_present_and_number_rendered_in_text_is_corroborated():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RATE, "0.45", raw_row_id=1)
        fact(cur, run_id, TEXT, "The Fund's portfolio turnover rate was 45% of the average value.",
             source_table="txt.tsv", uom=None, raw_row_id=2)
        assert _build(cur, publication_id) == 1
        cur.execute(
            """SELECT turnover_rate,declared_unit,turnover_numeric_present,turnover_text_present,
                      narrative_consistency,status,reason_code FROM rr1_turnover_profiles"""
        )
        assert cur.fetchone() == (
            Decimal("0.45"), "pure", True, True, "corroborated", "available", None,
        )
        # The full narrative text is NEVER copied into the public snapshot; only a
        # digest + length live in INTERNAL provenance.
        cur.execute(
            """SELECT provenance ? 'text_block_md5', provenance->>'text_block_length',
                      (provenance->>'text_block_md5') ~ '^[0-9a-f]{32}$'
               FROM rr1_turnover_profiles"""
        )
        assert cur.fetchone() == (True, "64", True)
        # No column anywhere carries the raw narrative string.
        cur.execute(
            "SELECT count(*) FROM rr1_turnover_profiles WHERE (provenance::text) LIKE '%average value%'"
        )
        assert cur.fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_number_not_found_in_text_is_unreconciled_without_judgement():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RATE, "0.45", raw_row_id=1)
        fact(cur, run_id, TEXT, "Portfolio turnover is described in the narrative below.",
             source_table="txt.tsv", uom=None, raw_row_id=2)
        _build(cur, publication_id)
        cur.execute("SELECT turnover_rate,narrative_consistency,status FROM rr1_turnover_profiles")
        # Both legs exist but the number is not textually referenced -> flagged for
        # reconciliation; the rate still stands and status is still available.
        assert cur.fetchone() == (Decimal("0.45"), "unreconciled", "available")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_number_only_and_text_only_are_distinguished():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RATE, "0.30", series="S1", class_id="C1", raw_row_id=1)
        fact(cur, run_id, TEXT, "Turnover discussion only, no structured rate.",
             source_table="txt.tsv", uom=None, series="S2", class_id="C2", raw_row_id=2)
        assert _build(cur, publication_id) == 2
        cur.execute(
            """SELECT series_id,turnover_rate,turnover_numeric_present,turnover_text_present,
                      narrative_consistency,status,reason_code
               FROM rr1_turnover_profiles ORDER BY series_id"""
        )
        assert cur.fetchall() == [
            ("S1", Decimal("0.30"), True, False, "number_only", "available", None),
            ("S2", None, False, True, "text_only", "degraded", "turnover_rate_not_reported"),
        ]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_non_numeric_rate_leg_is_degraded_not_fabricated():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RATE, "n/a", raw_row_id=1)
        _build(cur, publication_id)
        cur.execute(
            "SELECT turnover_rate,turnover_numeric_present,narrative_consistency,status,reason_code FROM rr1_turnover_profiles"
        )
        # A reported-but-non-numeric rate never becomes a synthetic 0.
        assert cur.fetchone() == (None, True, "number_only", "degraded", "turnover_rate_non_numeric")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_turnover_fails_closed_when_a_context_multiplies_the_rate_leg():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RATE, "0.45", raw_row_id=1)
        # Same context, a second rate fact: a fan-out that would multiply the row.
        fact(cur, run_id, RATE, "0.46", raw_row_id=2)
        with pytest.raises(psycopg.Error, match="conflicting RR1 turnover facts"):
            _build(cur, publication_id)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_unmapped_custom_turnover_tag_never_enters_the_snapshot():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        # A filer-custom tag (version = accession, not rr/%) that looks like turnover.
        fact(cur, run_id, "PortfolioTurnoverRateCustom", "0.99", version="0001234567-25-000001", raw_row_id=1)
        # A foreign standard tag under a non-turnover concept.
        fact(cur, run_id, "SomeOtherRrTag", "0.10", raw_row_id=2)
        assert _build(cur, publication_id) == 0
        cur.execute("SELECT count(*) FROM rr1_turnover_profiles")
        assert cur.fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_turnover_immutable_after_validation_and_current_view_is_derived_only():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RATE, "0.45", raw_row_id=1)
        _build(cur, publication_id)
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))
        with pytest.raises(psycopg.Error, match="prepared"):
            _build(cur, publication_id)
        with pytest.raises(psycopg.Error, match="immutable"):
            cur.execute("UPDATE rr1_turnover_profiles SET turnover_rate=0.99 WHERE publication_id=%s", (publication_id,))
        assert cur.execute(
            "SELECT turnover_rate FROM sec_current_rr1_turnover_profiles"
        ).fetchone() == (Decimal("0.45"),)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_turnover_build_pin_is_immutable_and_idempotent():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        fact(cur, run_id, RATE, "0.45", raw_row_id=1)
        assert _build(cur, publication_id) == 1
        assert _build(cur, publication_id) == 0
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            _build(cur, publication_id, as_of="2026-07-01")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_turnover_ddl_is_effective_only_and_leaks_no_source_identity():
    ddl = (ROOT / "schemas" / "rr1_turnover_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("rr1_turnover_profile_v1", "rr1_effective_facts", "input_fingerprint",
                  "PortfolioTurnoverRate", "PortfolioTurnoverTextBlock", "sec_derived_current_pointers"):
        assert token in ddl
    for forbidden in ("vendor", "sha256", "cik:", "sec_w1_nport_real", "filename"):
        assert forbidden not in lower
    current_view = lower.split("create or replace view sec_current_rr1_turnover_profiles", 1)[1]
    assert "rr1_raw_v2_rows" not in current_view
    assert "rr1_effective_facts" not in current_view
