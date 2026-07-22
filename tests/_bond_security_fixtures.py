"""Shared synthetic-fixture helpers for the bond_security_v1 security master.

DSN-agnostic by design (Global Constraint): every caller reads the disposable
Postgres endpoint from ``SEC_TEST_DATABASE_URL`` so the suite runs identically
under the keyword and URL DSN conventions.  The leading underscore keeps pytest
from collecting this module as a test file.  Observations are N-PORT-shaped
synthetic rows only — no production data is read.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def base_fixture(cur):
    """Stand up an isolated schema, source lineage, and the bond_security DDL."""
    schema = f"bond_security_fixture_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    for ddl_name in ("sec_derived_publications.sql", "bond_security_v1.sql"):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)  # idempotency
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
    return schema, run_id, package_id


def observe(cur, run_id, *, as_of, observation_date, cusip9=None, isin=None, **terms):
    """Insert one immutable N-PORT-shaped identity/terms observation."""
    observation_id = terms.pop("observation_id", str(uuid4()))
    lineage = json.dumps({"engine": "fixture", "observation_id": observation_id})
    cur.execute(
        "INSERT INTO bond_security_observation "
        "(observation_id, as_of, observation_date, source_run_id, cusip9_input, isin_input, "
        " issuer_name, currency, coupon_type, coupon_rate, maturity_date, seniority, secured, "
        " is_144a, day_count, settlement_convention, source_lineage) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
        (
            observation_id, as_of, observation_date, run_id, cusip9, isin,
            terms.get("issuer_name"), terms.get("currency"), terms.get("coupon_type"),
            terms.get("coupon_rate"), terms.get("maturity_date"), terms.get("seniority"),
            terms.get("secured"), terms.get("is_144a"), terms.get("day_count"),
            terms.get("settlement_convention"), lineage,
        ),
    )
    return observation_id
