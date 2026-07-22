"""Shared fixture helpers for the Phase-10 gate machine DB-backed suite.

DSN-agnostic (Global Constraint): reads the disposable Postgres endpoint from
``SEC_TEST_DATABASE_URL``.  The leading underscore keeps pytest from collecting
this module as a test file.  Every input is SYNTHETIC — no production bond source
(prices, curves, ratings) exists or is authorized (plan Global Constraint #3); the
fixtures merely let the gate's PIT / source-qualification predicates be exercised
in BOTH senses.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from uuid import uuid4

from src.bonds import curves, ratings

ROOT = Path(__file__).resolve().parents[1]

AS_OF = date(2026, 6, 30)
CURVE_DATE = date(2026, 6, 30)

# The DDL the gate's PIT predicates read (plus the gate's own qualification table).
_DDL = (
    "sec_derived_publications.sql",
    "bond_curve_v1.sql",
    "bond_rating_history_v1.sql",
    "bond_price_observations_v1.sql",
    "bond_price_eligibility_v1.sql",
    "bond_source_qualification.sql",
)


def dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def base_fixture(cur):
    """Isolated schema + validated source lineage + every PIT/gate DDL installed."""
    schema = f"phase10_gate_fixture_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    for ddl_name in _DDL:
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)  # idempotency
    # Internal (opaque -> label) agency mapping, required by the rating DDL.
    cur.execute(
        "INSERT INTO bond_rating_agency_map(agency_code, agency_label) VALUES(%s,%s)",
        ("AG01", "internal_label_a"),
    )
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
    return schema, run_id, package_id


def publish_curve(conn, run_id, package_id):
    """Publish one validated, current bond_curve_v1 (satisfies PIT_SPOT_CURVE)."""
    curves.load_curve_observations(
        conn,
        [curves.CurveObservationInput(
            observation_id=str(uuid4()), curve_date=CURVE_DATE, currency="USD",
            curve_type="spot", interpolation="linear",
            nodes=((1.0, 0.03), (2.0, 0.035), (5.0, 0.04)),
            source_lineage={"engine": "fixture"},
        )],
        as_of=AS_OF, source_run_id=run_id,
    )
    return curves.materialize(
        conn, as_of=AS_OF, source_run_id=run_id, source_package_id=package_id,
        code_revision="gaterev",
    )


def publish_licensed_ratings(conn, run_id, package_id):
    """Publish a LICENSED, active bond_rating_history_v1 (satisfies PIT_LICENSED_RATINGS)."""
    ratings.load_rating_observations(
        conn,
        [ratings.RatingObservationInput(
            observation_id=str(uuid4()), subject_kind="security", security_id=uuid4(),
            agency_code="AG01", rating="R_AA", valid_from=date(2025, 1, 1),
            licensed_source_ref="lic:fixture-ref-0001",
            source_lineage={"engine": "fixture"},
        )],
        as_of=AS_OF, source_run_id=run_id,
    )
    return ratings.materialize(
        conn, as_of=AS_OF, source_run_id=run_id, source_package_id=package_id,
        code_revision="gaterev", license_verified=True,
    )


def publish_unlicensed_ratings(conn, run_id, package_id):
    """Publish a license-gated (not_applicable, empty) rating run — does NOT satisfy the gate."""
    return ratings.materialize(
        conn, as_of=AS_OF, source_run_id=run_id, source_package_id=package_id,
        code_revision="gaterev", license_verified=False,
    )


def insert_eligible_price(conn, run_id):
    """Insert one ELIGIBLE bond_price_observation (satisfies PIT_ELIGIBLE_PRICE)."""
    from psycopg.types.json import Jsonb

    conn.execute(
        "INSERT INTO bond_price_observation"
        "(observation_id, as_of, observation_date, source_run_id, security_id, cusip9_input,"
        " normalized_cusip9, identity_state, identity_reason_code, price, price_state, price_type,"
        " accrued_treatment, ytm, db_type, db_type_state, daily_key_state, source_row_number,"
        " source_lineage) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,'resolved',NULL,%s,'present','trade','clean',NULL,NULL,'null',"
        "'unique_in_matching_cohort',1,%s)",
        (uuid4(), AS_OF, AS_OF, run_id, uuid4(), "037833100", "037833100", 101.5,
         Jsonb({"engine": "fixture"})),
    )


def insert_ineligible_price(conn, run_id):
    """Insert a NON-eligible bond_price_observation (price_type='model' -> not eligible)."""
    from psycopg.types.json import Jsonb

    conn.execute(
        "INSERT INTO bond_price_observation"
        "(observation_id, as_of, observation_date, source_run_id, security_id, cusip9_input,"
        " normalized_cusip9, identity_state, identity_reason_code, price, price_state, price_type,"
        " accrued_treatment, ytm, db_type, db_type_state, daily_key_state, source_row_number,"
        " source_lineage) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,'resolved',NULL,%s,'present','model','clean',NULL,NULL,'null',"
        "'unique_in_matching_cohort',1,%s)",
        (uuid4(), AS_OF, AS_OF, run_id, uuid4(), "037833100", "037833100", 101.5,
         Jsonb({"engine": "fixture"})),
    )


def qualify_source(conn, metric_id, *, source_contract_ref="contract:future-activation"):
    """Insert an ACTIVE source-qualification row for one metric (future-activation sim)."""
    conn.execute(
        "INSERT INTO bond_source_qualification(metric_id, source_contract_ref) VALUES(%s,%s)",
        (metric_id, source_contract_ref),
    )
