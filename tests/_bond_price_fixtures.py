"""Shared synthetic-fixture helpers for the bond_price_observation_v1 lanes.

DSN-agnostic by design (Global Constraint): every caller reads the disposable
Postgres endpoint from ``SEC_TEST_DATABASE_URL`` so the suite runs identically
under the keyword and URL DSN conventions.  The leading underscore keeps pytest
from collecting this module as a test file.  Observations are synthetic price/
trade rows only — no production price source exists or is authorized (the TRACE
144A pilot does not authorize one).
"""
from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from src.bonds.price_observations import PriceObservationInput

ROOT = Path(__file__).resolve().parents[1]


def dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def base_fixture(cur):
    """Stand up an isolated schema, source lineage, and the price-lane DDL."""
    schema = f"bond_price_fixture_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    for ddl_name in ("sec_derived_publications.sql", "bond_price_observations_v1.sql"):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)  # idempotency
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
    return schema, run_id, package_id


def price_input(
    *,
    observation_date,
    cusip9=None,
    price=None,
    price_type=None,
    accrued_treatment=None,
    ytm=None,
    db_type=None,
    observation_id=None,
) -> PriceObservationInput:
    """Build one synthetic price/trade observation input."""
    oid = observation_id or str(uuid4())
    return PriceObservationInput(
        observation_id=oid,
        observation_date=observation_date,
        cusip9_input=cusip9,
        price=price,
        price_type=price_type,
        accrued_treatment=accrued_treatment,
        ytm=ytm,
        db_type=db_type,
        source_lineage={"engine": "fixture", "observation_id": oid},
    )
