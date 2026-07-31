from __future__ import annotations

import json
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rr1_derived_fixtures import ROOT, dsn  # noqa: E402

from src.rr1 import derived_profiles  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)


def _raw_surface(conn):
    """Stand up the raw RR1 surface + the frozen effective views + derived DDL,
    exercising the real amendment-aware selection end to end."""
    schema = f"rr1_materializer_fixture_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    cur = conn.cursor()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
    cur.execute(
        """CREATE TABLE rr1_raw_v2_rows(raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        ingestion_run_id uuid, source_table text, parse_status text, typed_projection jsonb, adsh text, tag text,
        version text, series text, class text, measure text, document text, otherdims text, iprx text)"""
    )
    derived_profiles.install_schema(conn)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    return schema, run_id, package_id, cur


def _sub(cur, run_id, adsh, *, effdate, accepted="2026-02-01T10:00:00", filed="2026-02-02",
         form="N-1A", parse_status="typed"):
    body = {"effdate": effdate, "accepted": accepted, "filed": filed, "form": form}
    cur.execute(
        "INSERT INTO rr1_raw_v2_rows(ingestion_run_id,source_table,parse_status,typed_projection,adsh) "
        "VALUES(%s,'sub.tsv',%s,%s::jsonb,%s)",
        (run_id, parse_status, json.dumps(body), adsh),
    )


def _fact(cur, run_id, adsh, tag, value, *, source_table="num.tsv", version="rr/2025", uom="pure",
          series="S1", class_id="C1", measure="", document="D1", otherdims="", iprx="1", ddate="2025-12-31"):
    projection: dict[str, str] = {"ddate": ddate}
    if uom is not None:
        projection["uom"] = uom
    if value is not None:
        projection["value"] = value
    cur.execute(
        """INSERT INTO rr1_raw_v2_rows(ingestion_run_id,source_table,parse_status,typed_projection,adsh,tag,version,
        series,class,measure,document,otherdims,iprx) VALUES(%s,%s,'typed',%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (run_id, source_table, json.dumps(projection), adsh, tag, version, series, class_id, measure, document, otherdims, iprx),
    )


def _seed_full_fee_table(cur, run_id):
    _sub(cur, run_id, "A1", effdate="2026-01-01")
    # Class C1 full annual operating-expense table.
    _fact(cur, run_id, "A1", "ManagementFeesOverAssets", "0.40", class_id="C1")
    _fact(cur, run_id, "A1", "ExpensesOverAssets", "0.85", class_id="C1")
    _fact(cur, run_id, "A1", "NetExpensesOverAssets", "0.70", class_id="C1")
    _fact(cur, run_id, "A1", "FeeWaiverOrReimbursementOverAssets", "0.15", class_id="C1")
    # A second class, so the snapshot carries more than one share class.
    _fact(cur, run_id, "A1", "ManagementFeesOverAssets", "0.45", class_id="C2", document="D2")
    _fact(cur, run_id, "A1", "NetExpensesOverAssets", "0.90", class_id="C2", document="D2")


def test_materializer_publishes_every_product_prepared_to_validated_to_current():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn:
        schema, run_id, package_id, cur = _raw_surface(conn)
        _seed_full_fee_table(cur, run_id)

        results = derived_profiles.materialize_all(
            conn, as_of=date(2026, 6, 30), source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        assert {r["product"] for r in results} == set(derived_profiles.PRODUCTS)
        assert all(r["state"] == "current" for r in results)

        for product in derived_profiles.PRODUCTS:
            cur.execute("SELECT lifecycle_state FROM sec_current_derived_publications WHERE product=%s", (product,))
            assert cur.fetchone() == ("validated",), product
        for view in ("sec_current_rr1_fee_profiles",):
            cur.execute(f"SELECT count(*) FROM {view}")
            assert cur.fetchone()[0] >= 1, view

        # Re-running is idempotent: no new publications, pointers unchanged.
        derived_profiles.materialize_all(
            conn, as_of=date(2026, 6, 30), source_run_id=run_id,
            source_package_id=package_id, code_revision="testrev",
        )
        cur.execute("SELECT count(*) FROM sec_derived_publications")
        assert cur.fetchone()[0] == len(derived_profiles.PRODUCTS)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_materializer_products_map_to_installed_build_functions_and_worker_uses_derived_lock():
    installed = "\n".join(
        (ROOT / "schemas" / name).read_text(encoding="utf-8") for name in derived_profiles._SCHEMA_FILES
    )
    for product, build_fn in derived_profiles.PRODUCTS.items():
        assert f"FUNCTION {build_fn}(" in installed, product
    worker = (ROOT / "src" / "workers" / "rr1_derived_profiles.py").read_text(encoding="utf-8")
    assert "LOCK_RR1_DERIVED_PROFILES" in worker


def test_malformed_effective_date_is_quarantined_and_cannot_win_selection():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn:
        schema, run_id, package_id, cur = _raw_surface(conn)
        # A valid older prospectus reports a 0.40% management fee.
        _sub(cur, run_id, "OLD", effdate="2024-06-01")
        _fact(cur, run_id, "OLD", "ManagementFeesOverAssets", "0.40", class_id="C1")
        # A later "amendment" whose effective date 21020906 is malformed: the parser
        # quarantines it (typed effdate NULL), so it can never win the selection --
        # its 99% value must not surface.
        _sub(cur, run_id, "NEW", effdate=None, parse_status="quarantined",
             accepted="2026-05-01T10:00:00", filed="2026-05-02")
        _fact(cur, run_id, "NEW", "ManagementFeesOverAssets", "0.99", class_id="C1")

        # The effective view already drops the quarantined amendment.
        cur.execute("SELECT accession_number,fact_typed_projection->>'value' FROM rr1_effective_facts WHERE tag='ManagementFeesOverAssets'")
        assert cur.fetchall() == [("OLD", "0.40")]

        publication_id = uuid4()
        cur.execute(
            """INSERT INTO sec_derived_publications
            (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
            VALUES(%s,'rr1_fee_profile_v1',1,%s,%s,%s)""",
            (publication_id, run_id, package_id, "c" * 64),
        )
        cur.execute("SELECT build_rr1_fee_profiles(%s,'2026-06-30')", (publication_id,))
        cur.execute("SELECT value_numeric,status FROM rr1_fee_profiles WHERE canonical_concept='management_fee'")
        assert cur.fetchone() == (Decimal("0.40"), "available")
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
