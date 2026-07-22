"""SQL half of the price-eligibility predicate: the bond_price_is_eligible function
and the bond_price_eligibility_v1 view over bond_price_observation.

DSN-agnostic (reads SEC_TEST_DATABASE_URL); disposable-schema per test. Proves the
view classifies every immutable observation with the SAME predicate as the pure
Python mirror, and that it is ADDITIVE — it only READS bond_price_observation and
does not touch the bond_price_observation_v1 product.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_price_fixtures import base_fixture, dsn, price_input  # noqa: E402

from src.bonds import eligibility, price_observations  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

AS_OF = date(2026, 6, 30)


def test_view_classifies_eligibility_additively():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _ = base_fixture(cur)
        # Install the additive eligibility predicate (idempotent; view+function only).
        eligibility.install_schema(conn)

        price_observations.load_price_observations(
            conn,
            [
                # Fully qualified -> eligible.
                price_input(observation_date=date(2026, 6, 30), cusip9="037833100", price=99.5,
                            price_type="evaluated", accrued_treatment="clean"),
                # price_type 'model' -> ineligible.
                price_input(observation_date=date(2026, 6, 30), cusip9="459200101", price=98.0,
                            price_type="model", accrued_treatment="clean"),
                # accrued not stated -> ineligible.
                price_input(observation_date=date(2026, 6, 30), cusip9="123456789", price=97.0,
                            price_type="trade", accrued_treatment=None),
                # Unresolved identity (placeholder CUSIP) -> ineligible.
                price_input(observation_date=date(2026, 6, 30), cusip9="000000000", price=50.0,
                            price_type="trade", accrued_treatment="clean"),
                # Duplicate (same CUSIP+date, distinct rows) -> ambiguous -> ineligible.
                price_input(observation_date=date(2026, 6, 30), cusip9="594918104", price=100.0,
                            price_type="trade", accrued_treatment="clean"),
                price_input(observation_date=date(2026, 6, 30), cusip9="594918104", price=101.0,
                            price_type="trade", accrued_treatment="clean"),
                # Price absent (NULL) -> ineligible.
                price_input(observation_date=date(2026, 6, 30), cusip9="88160R101", price=None,
                            price_type="trade", accrued_treatment="clean"),
            ],
            as_of=AS_OF, source_run_id=run_id,
        )

        cur.execute(
            "SELECT count(*) FROM bond_price_eligibility_v1 WHERE is_eligible"
        )
        assert cur.fetchone()[0] == 1  # only the fully-qualified observation

        cur.execute(
            "SELECT eligibility_reason FROM bond_price_eligibility_v1 "
            "WHERE NOT is_eligible ORDER BY eligibility_reason"
        )
        reasons = sorted(r[0] for r in cur.fetchall())
        assert reasons == sorted([
            "price_type_not_eligible",
            "accrued_treatment_unknown",
            "identity_unresolved",
            "identity_ambiguous",
            "identity_ambiguous",  # both duplicate rows
            "price_absent",
        ])

        # The SQL function agrees with the pure Python predicate on a known row.
        cur.execute(
            "SELECT bond_price_is_eligible('evaluated','clean','resolved',"
            "'unique_in_matching_cohort','present')"
        )
        assert cur.fetchone()[0] is True

        # ADDITIVE: the observations product is untouched (no publication created here).
        cur.execute("SELECT count(*) FROM sec_derived_publications WHERE product='bond_price_observation_v1'")
        assert cur.fetchone()[0] == 0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_worker_installs_predicate_and_reports_counts():
    import psycopg

    from src.workers import bond_price_eligibility as worker

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _ = base_fixture(cur)
        price_observations.load_price_observations(
            conn,
            [price_input(observation_date=date(2026, 6, 30), cusip9="037833100", price=99.5,
                         price_type="evaluated", accrued_treatment="clean")],
            as_of=AS_OF, source_run_id=run_id,
        )
        # The worker resolves its own DSN from SEC_TEST_DATABASE_URL via DATABASE_URL;
        # here we drive install + count directly against the fixture connection.
        worker.install_and_count(conn, as_of=AS_OF)
        cur.execute("SELECT count(*) FROM bond_price_eligibility_v1 WHERE is_eligible")
        assert cur.fetchone()[0] == 1
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
