"""Shared synthetic-fixture helpers for the bond_rating_history_v1 product.

DSN-agnostic (Global Constraint): reads the disposable Postgres endpoint from
``SEC_TEST_DATABASE_URL``.  The leading underscore keeps pytest from collecting
this module as a test file.  Ratings are synthetic only — NO production rating
source exists or is authorized (the license gate is exercised with a synthetic
licensed_source_ref token, never a real vendor feed).
"""
from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from src.bonds.ratings import RatingObservationInput

ROOT = Path(__file__).resolve().parents[1]

# Opaque internal agency codes (NEVER an agency name in any serving field).
AGENCY_A = "AG01"
AGENCY_B = "AG02"


def dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def base_fixture(cur):
    """Stand up an isolated schema, source lineage, agency map, and the rating DDL."""
    schema = f"bond_rating_fixture_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    for ddl_name in ("sec_derived_publications.sql", "bond_rating_history_v1.sql"):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)  # idempotency
    # Internal (opaque -> label) agency mapping. Datalake-internal only.
    cur.execute("INSERT INTO bond_rating_agency_map(agency_code, agency_label) VALUES(%s,%s),(%s,%s)",
                (AGENCY_A, "internal_label_a", AGENCY_B, "internal_label_b"))
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
    return schema, run_id, package_id


def rating_input(
    *,
    subject_kind="security",
    security_id=None,
    issuer_id=None,
    agency_code=AGENCY_A,
    rating="R_AA",
    watch="none",
    outlook="stable",
    valid_from,
    valid_to=None,
    licensed_source_ref="lic:fixture-ref-0001",
    observation_id=None,
) -> RatingObservationInput:
    """Build one synthetic rating observation input (carries a license ref)."""
    oid = observation_id or str(uuid4())
    return RatingObservationInput(
        observation_id=oid,
        subject_kind=subject_kind,
        security_id=security_id,
        issuer_id=issuer_id,
        agency_code=agency_code,
        rating=rating,
        watch=watch,
        outlook=outlook,
        valid_from=valid_from,
        valid_to=valid_to,
        licensed_source_ref=licensed_source_ref,
        source_lineage={"engine": "fixture", "observation_id": oid},
    )
