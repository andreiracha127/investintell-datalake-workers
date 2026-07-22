"""Shared synthetic-fixture helpers for the bond_curve_v1 spot/par curve product.

DSN-agnostic (Global Constraint): every caller reads the disposable Postgres
endpoint from ``SEC_TEST_DATABASE_URL`` so the suite runs identically under the
keyword and URL DSN conventions.  The leading underscore keeps pytest from
collecting this module as a test file.  Curves are synthetic node sets only — no
production curve source exists or is authorized (fixtures only).
"""
from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from src.bonds.curves import CurveObservationInput

ROOT = Path(__file__).resolve().parents[1]


def dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def base_fixture(cur):
    """Stand up an isolated schema, source lineage, and the curve DDL."""
    schema = f"bond_curve_fixture_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    for ddl_name in ("sec_derived_publications.sql", "bond_curve_v1.sql"):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)  # idempotency
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
    return schema, run_id, package_id


def curve_input(
    *,
    curve_date,
    nodes,
    currency="USD",
    curve_type="spot",
    interpolation="linear",
    observation_id=None,
) -> CurveObservationInput:
    """Build one synthetic curve observation input (raw nodes as observed)."""
    oid = observation_id or str(uuid4())
    return CurveObservationInput(
        observation_id=oid,
        curve_date=curve_date,
        currency=currency,
        curve_type=curve_type,
        interpolation=interpolation,
        nodes=tuple(nodes),
        source_lineage={"engine": "fixture", "observation_id": oid},
    )
