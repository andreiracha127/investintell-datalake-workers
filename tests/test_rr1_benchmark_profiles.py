from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rr1_derived_fixtures import ROOT, base_fixture, dsn, fact  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

DDL = ("rr1_benchmark_profiles.sql",)
PRODUCT = "rr1_benchmark_profile_v1"
RET = "AvgAnnlRtrPct"
IDX = "BroadBasedIndexMember"
SP500 = "IndexAxis=SP500Member"
AGG = "IndexAxis=AggBondMember"


def _bench(cur, run_id, dimensions, *, document="D1", data_date="2025-12-31", value="0.12",
           series="S1", class_id="", raw_row_id=1):
    fact(cur, run_id, RET, value, measure=IDX, dimensions=dimensions, document=document,
         data_date=data_date, series=series, class_id=class_id, raw_row_id=raw_row_id)


def _build(cur, publication_id, as_of="2026-06-30"):
    return cur.execute("SELECT build_rr1_benchmark_profiles(%s,%s)", (publication_id, as_of)).fetchone()[0]


def test_same_benchmark_across_documents_is_consistent_without_judgement():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        _bench(cur, run_id, SP500, document="D1", raw_row_id=1)
        _bench(cur, run_id, SP500, document="D2", value="0.13", raw_row_id=2)
        # The fund's OWN class return (not a benchmark) must never be counted.
        fact(cur, run_id, RET, "0.09", class_id="C1", measure="", dimensions="PeriodAxis=Year01", raw_row_id=3)
        assert _build(cur, publication_id) == 1
        cur.execute(
            """SELECT series_id,class_id,declared_benchmark_count,observation_count,context_count,
                      document_count,period_count,primary_benchmark,benchmark_consistency,status,reason_code
               FROM rr1_benchmark_profiles"""
        )
        assert cur.fetchone() == (
            "S1", "", 1, 2, 2, 2, 1, SP500, "consistent", "available", None,
        )
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_multiple_declared_benchmarks_are_reported_as_multiple_without_judgement():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        _bench(cur, run_id, SP500, document="D1", raw_row_id=1)
        _bench(cur, run_id, AGG, document="D2", value="0.05", raw_row_id=2)
        _build(cur, publication_id)
        cur.execute(
            """SELECT declared_benchmark_count,primary_benchmark,benchmark_consistency,status
               FROM rr1_benchmark_profiles"""
        )
        # Two distinct declared benchmarks: reported as multiple, no primary, no judgement.
        assert cur.fetchone() == (2, None, "multiple_declared", "available")
        cur.execute(
            """SELECT e->>'benchmark_identifier', e->>'observation_count'
               FROM rr1_benchmark_profiles, jsonb_array_elements(per_benchmark_evidence) e
               ORDER BY e->>'benchmark_identifier'"""
        )
        assert cur.fetchall() == [(AGG, "1"), (SP500, "1")]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_single_observation_cannot_assess_consistency():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        _bench(cur, run_id, SP500, document="D1", raw_row_id=1)
        _build(cur, publication_id)
        cur.execute(
            "SELECT declared_benchmark_count,context_count,primary_benchmark,benchmark_consistency,status FROM rr1_benchmark_profiles"
        )
        assert cur.fetchone() == (1, 1, SP500, "single_observation", "available")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_unnamed_benchmark_dimension_is_degraded_not_fabricated():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        # A broad-market-index return with NO naming dimension: benchmark present but unnamed.
        _bench(cur, run_id, "", document="D1", raw_row_id=1)
        _bench(cur, run_id, "", document="D2", value="0.13", raw_row_id=2)
        _build(cur, publication_id)
        cur.execute(
            "SELECT declared_benchmark_count,observation_count,primary_benchmark,benchmark_consistency,status,reason_code FROM rr1_benchmark_profiles"
        )
        assert cur.fetchone() == (0, 2, None, "single_observation", "degraded", "benchmark_dimension_unnamed")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_distinct_series_class_get_distinct_benchmark_rows():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        _bench(cur, run_id, SP500, series="S1", class_id="", document="D1", raw_row_id=1)
        _bench(cur, run_id, SP500, series="S1", class_id="", document="D2", raw_row_id=2)
        _bench(cur, run_id, AGG, series="S2", class_id="C9", document="D1", raw_row_id=3)
        assert _build(cur, publication_id) == 2
        cur.execute("SELECT series_id,class_id,primary_benchmark FROM rr1_benchmark_profiles ORDER BY series_id")
        assert cur.fetchall() == [("S1", "", SP500), ("S2", "C9", AGG)]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_unmapped_custom_benchmark_tag_never_enters_the_snapshot():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        # Custom return-like tag (version = accession) carrying a benchmark dimension.
        fact(cur, run_id, "CustomIndexReturn", "0.99", version="0001234567-25-000001",
             measure=IDX, dimensions=SP500, raw_row_id=1)
        assert _build(cur, publication_id) == 0
        cur.execute("SELECT count(*) FROM rr1_benchmark_profiles")
        assert cur.fetchone() == (0,)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_benchmark_immutable_after_validation_and_current_view_is_derived_only():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        _bench(cur, run_id, SP500, document="D1", raw_row_id=1)
        _bench(cur, run_id, SP500, document="D2", raw_row_id=2)
        _build(cur, publication_id)
        cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))
        with pytest.raises(psycopg.Error, match="prepared"):
            _build(cur, publication_id)
        with pytest.raises(psycopg.Error, match="immutable"):
            cur.execute("UPDATE rr1_benchmark_profiles SET primary_benchmark='X' WHERE publication_id=%s", (publication_id,))
        assert cur.execute(
            "SELECT benchmark_consistency FROM sec_current_rr1_benchmark_profiles"
        ).fetchone() == ("consistent",)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_benchmark_build_pin_is_immutable_and_idempotent():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _, publication_id = base_fixture(cur, PRODUCT, DDL)
        _bench(cur, run_id, SP500, document="D1", raw_row_id=1)
        assert _build(cur, publication_id) == 1
        assert _build(cur, publication_id) == 0
        with pytest.raises(psycopg.Error, match="already pinned to as_of_date"):
            _build(cur, publication_id, as_of="2026-07-01")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_benchmark_ddl_is_effective_only_and_leaks_no_source_identity():
    ddl = (ROOT / "schemas" / "rr1_benchmark_profiles.sql").read_text(encoding="utf-8")
    lower = ddl.lower()
    for token in ("rr1_benchmark_profile_v1", "rr1_effective_facts", "input_fingerprint",
                  "AvgAnnlRtrPct", "sec_derived_current_pointers"):
        assert token in ddl
    for forbidden in ("vendor", "sha256", "cik:", "sec_w1_nport_real", "filename"):
        assert forbidden not in lower
    current_view = lower.split("create or replace view sec_current_rr1_benchmark_profiles", 1)[1]
    assert "rr1_raw_v2_rows" not in current_view
    assert "rr1_effective_facts" not in current_view
