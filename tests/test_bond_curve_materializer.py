"""bond_curve_v1 materializer + SpotCurve round-trip over the sec_derived_publications
protocol.

DSN-agnostic (reads SEC_TEST_DATABASE_URL); disposable-schema per test. Proves:
prepared -> validated -> current promotion + idempotent rerun, degenerate curves
typed and NEVER published, the published nodes feed src.bonds.pricing.SpotCurve
without adaptation (round-trip), immutable observations, and a partial build can
never become current.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_curve_fixtures import base_fixture, curve_input, dsn  # noqa: E402

from src.bonds import curves  # noqa: E402
from src.bonds.cashflows import BondTerms, DayCount, Frequency, generate_schedule  # noqa: E402
from src.bonds.pricing import SpotCurve, curve_price  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

AS_OF = date(2026, 6, 30)
CURVE_DATE = date(2026, 6, 30)


def _publish(conn, run_id, package_id):
    return curves.materialize(
        conn, as_of=AS_OF, source_run_id=run_id,
        source_package_id=package_id, code_revision="testrev",
    )


def test_publishes_prepared_to_validated_to_current_idempotently():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        curves.load_curve_observations(
            conn,
            [
                curve_input(curve_date=CURVE_DATE, nodes=[(1.0, 0.03), (2.0, 0.035), (5.0, 0.04)]),
                # Degenerate: single node -> typed, never published.
                curve_input(curve_date=CURVE_DATE, curve_type="par", nodes=[(1.0, 0.03)]),
            ],
            as_of=AS_OF, source_run_id=run_id,
        )
        result = _publish(conn, run_id, package_id)
        assert result["state"] == "current"
        assert result["curves"] == 1  # only the valid 3-node curve is published
        assert result["rejected"] == 1

        cur.execute("SELECT lifecycle_state FROM sec_current_derived_publications WHERE product='bond_curve_v1'")
        assert cur.fetchone() == ("validated",)
        cur.execute("SELECT node_count FROM bond_curve_v1")
        assert cur.fetchone() == (3,)
        cur.execute("SELECT count(*) FROM bond_curve_node_v1")
        assert cur.fetchone()[0] == 3

        again = _publish(conn, run_id, package_id)
        assert again["publication_id"] == result["publication_id"]
        cur.execute("SELECT count(*) FROM sec_derived_publications WHERE product='bond_curve_v1'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM bond_curve_v1_builds")
        assert cur.fetchone()[0] == 1
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_degenerate_curves_are_typed_and_never_published():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        curves.load_curve_observations(
            conn,
            [
                # Duplicate tenor -> not strictly increasing after canonicalization.
                curve_input(curve_date=CURVE_DATE, nodes=[(1.0, 0.03), (1.0, 0.035)]),
                # Non-finite rate.
                curve_input(curve_date=CURVE_DATE, curve_type="par",
                            nodes=[(1.0, 0.03), (2.0, float("nan"))]),
                # Unsupported interpolation.
                curve_input(curve_date=date(2026, 5, 31), interpolation="cubic",
                            nodes=[(1.0, 0.03), (2.0, 0.035)]),
            ],
            as_of=AS_OF, source_run_id=run_id,
        )
        result = _publish(conn, run_id, package_id)
        assert result["curves"] == 0
        assert result["rejected"] == 3
        cur.execute("SELECT count(*) FROM bond_curve_v1")
        assert cur.fetchone()[0] == 0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_published_snapshot_round_trips_into_spotcurve():
    """MANDATORY: the published curve feeds src.bonds.pricing.SpotCurve unadapted."""
    import psycopg

    nodes = [(0.5, 0.028), (1.0, 0.030), (2.0, 0.035), (5.0, 0.040)]
    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        obs = curve_input(curve_date=CURVE_DATE, nodes=nodes)
        curves.load_curve_observations(conn, [obs], as_of=AS_OF, source_run_id=run_id)
        _publish(conn, run_id, package_id)

        curve_id = curves.curve_id_for("USD", CURVE_DATE, "spot")
        spot = curves.spot_curve_from_snapshot(conn, curve_id)
        assert isinstance(spot, SpotCurve)
        # Round-trip: nodes recovered exactly.
        assert [(round(t, 6), round(r, 6)) for t, r in spot.nodes] == [
            (round(t, 6), round(r, 6)) for t, r in nodes
        ]
        # Interpolation behaves as declared (linear between nodes, flat outside).
        assert spot.rate(2.0) == pytest.approx(0.035)
        assert spot.rate(3.5) == pytest.approx(0.035 + (0.040 - 0.035) * (3.5 - 2.0) / (5.0 - 2.0))
        assert spot.rate(0.1) == pytest.approx(0.028)  # flat below first node
        assert spot.rate(30.0) == pytest.approx(0.040)  # flat above last node
        # End-to-end: the snapshot-derived curve prices a bond IDENTICALLY to a
        # hand-built SpotCurve with the same nodes (feeds pricing.curve_price unadapted).
        terms = BondTerms(
            issue_date=date(2024, 6, 30), maturity_date=date(2029, 6, 30), coupon_rate=0.04,
            frequency=Frequency.SEMIANNUAL, day_count=DayCount.THIRTY_360_US, face=100.0,
        )
        schedule = generate_schedule(terms)
        settlement = date(2026, 6, 30)
        by_hand = SpotCurve(nodes=tuple(nodes))
        assert curve_price(schedule, settlement, spot) == pytest.approx(
            curve_price(schedule, settlement, by_hand)
        )
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_observation_rows_are_immutable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _ = base_fixture(cur)
        obs = curve_input(curve_date=CURVE_DATE, nodes=[(1.0, 0.03), (2.0, 0.035)])
        curves.load_curve_observations(conn, [obs], as_of=AS_OF, source_run_id=run_id)
        with pytest.raises(psycopg.Error, match="bond_curve_observation is immutable"):
            cur.execute("UPDATE bond_curve_observation SET currency='EUR' WHERE observation_id=%s", (obs.observation_id,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        with pytest.raises(psycopg.Error, match="bond_curve_observation is immutable"):
            cur.execute("DELETE FROM bond_curve_observation WHERE observation_id=%s", (obs.observation_id,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_partial_non_validated_build_can_never_become_current():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        publication_id = uuid4()
        cur.execute(
            "INSERT INTO sec_derived_publications"
            "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint) "
            "VALUES(%s,'bond_curve_v1',1,%s,%s,%s)",
            (publication_id, run_id, package_id, "a" * 64),
        )
        with pytest.raises(psycopg.Error, match="requires a validated publication"):
            cur.execute("SELECT sec_set_current_derived_publication('bond_curve_v1',%s)", (publication_id,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute("SELECT count(*) FROM sec_derived_current_pointers")
        assert cur.fetchone()[0] == 0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_snapshot_rows_frozen_after_validation():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        curves.load_curve_observations(
            conn,
            [curve_input(curve_date=CURVE_DATE, nodes=[(1.0, 0.03), (2.0, 0.035)])],
            as_of=AS_OF, source_run_id=run_id,
        )
        result = _publish(conn, run_id, package_id)
        with pytest.raises(psycopg.Error, match="requires a prepared bond_curve_v1 publication"):
            cur.execute(
                "INSERT INTO bond_curve_v1"
                "(publication_id,source_run_id,curve_id,curve_key,currency,curve_date,curve_type,"
                " interpolation,node_count,measured_at,provenance) "
                "VALUES(%s,%s,%s,'k','USD',%s,'spot','linear',2,%s,'{}')",
                (result["publication_id"], run_id, uuid4(), CURVE_DATE, AS_OF),
            )
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
