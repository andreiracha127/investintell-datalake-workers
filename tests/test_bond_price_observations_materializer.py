"""bond_price_observation_v1 materializer + lanes over the sec_derived_publications
protocol.

DSN-agnostic (reads SEC_TEST_DATABASE_URL); disposable-schema per test. Proves:
prepared -> validated -> current promotion + idempotent rerun, duplicate
(security_id, observation_date) ambiguity retained end-to-end, STALE boundary
(30d vs 31d) in the fund_asof lane, no look-ahead, the fund_asof lane can never
return a latest-stamped row (structural), immutable observations, unresolved
identities never published, a partial build can never become current, and the DB
PIT rows feed the matching library.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_price_fixtures import base_fixture, dsn, price_input  # noqa: E402

from uuid import uuid5  # noqa: E402

from src.bonds import price_observations  # noqa: E402
from src.bonds.debt_mapping import DebtMapping  # noqa: E402
from src.bonds.identifiers import normalize_cusip9  # noqa: E402
from src.bonds.matching import HoldingRecord  # noqa: E402
from src.bonds.security_master import NAMESPACE_BOND_SECURITY  # noqa: E402
from src.bonds.states import MatchState  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

AS_OF = date(2026, 6, 30)


def _security_id(cusip9: str):
    return uuid5(NAMESPACE_BOND_SECURITY, f"cusip9:{normalize_cusip9(cusip9).normalized_cusip9}")


def _publish(conn, run_id, package_id):
    return price_observations.materialize(
        conn, as_of=AS_OF, source_run_id=run_id,
        source_package_id=package_id, code_revision="testrev",
    )


def test_publishes_prepared_to_validated_to_current_idempotently():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        price_observations.load_price_observations(
            conn,
            [
                price_input(observation_date=date(2026, 5, 31), cusip9="037833100", price=99.5,
                            price_type="evaluated", accrued_treatment="clean", db_type=3),
                price_input(observation_date=date(2026, 6, 30), cusip9="037833100", price=100.25,
                            price_type="evaluated", accrued_treatment="clean", db_type=3),
                # Placeholder identity -> landed but never published.
                price_input(observation_date=date(2026, 6, 30), cusip9="000000000", price=50.0),
            ],
            as_of=AS_OF, source_run_id=run_id,
        )
        result = _publish(conn, run_id, package_id)
        assert result["state"] == "current"
        assert result["observations"] == 3
        assert result["published"] == 2  # the placeholder identity is not published

        cur.execute("SELECT lifecycle_state FROM sec_current_derived_publications WHERE product='bond_price_observation_v1'")
        assert cur.fetchone() == ("validated",)
        cur.execute("SELECT count(*) FROM bond_price_observation_v1")
        assert cur.fetchone()[0] == 2

        again = _publish(conn, run_id, package_id)
        assert again["publication_id"] == result["publication_id"]
        cur.execute("SELECT count(*) FROM sec_derived_publications WHERE product='bond_price_observation_v1'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM bond_price_observation_v1_builds")
        assert cur.fetchone()[0] == 1
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_duplicate_security_date_retained_and_ambiguous_end_to_end():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        price_observations.load_price_observations(
            conn,
            [
                price_input(observation_date=date(2026, 6, 30), cusip9="459200101", price=98.0),
                price_input(observation_date=date(2026, 6, 30), cusip9="459200101", price=102.0),
            ],
            as_of=AS_OF, source_run_id=run_id,
        )
        _publish(conn, run_id, package_id)

        cur.execute(
            "SELECT price, daily_key_state FROM bond_price_observation_v1 ORDER BY price"
        )
        rows = cur.fetchall()
        # Both distinct source rows retained; neither dropped; both ambiguous.
        assert [float(p) for p, _ in rows] == [98.0, 102.0]
        assert {s for _, s in rows} == {"duplicate_in_matching_cohort"}
        # The informative latest lane surfaces both duplicates (no arbitrary winner).
        cur.execute("SELECT count(*) FROM bond_price_latest_v1")
        assert cur.fetchone()[0] == 2
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fund_asof_no_lookahead_and_stale_boundary():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        obs_date = date(2026, 6, 1)
        future_date = date(2026, 6, 20)
        price_observations.load_price_observations(
            conn,
            [
                # Two observations for one security: the later one must stay invisible
                # until it happens (no look-ahead).
                price_input(observation_date=obs_date, cusip9="037833100", price=99.0),
                price_input(observation_date=future_date, cusip9="037833100", price=101.0),
                # A separate single-observation security for the STALE boundary.
                price_input(observation_date=obs_date, cusip9="459200101", price=100.0),
            ],
            as_of=AS_OF, source_run_id=run_id,
        )
        _publish(conn, run_id, package_id)

        # No look-ahead: at obs_date + 5d the future observation is invisible; the
        # security's most-recent visible observation is the one on obs_date.
        cur.execute(
            "SELECT observation_date, price FROM bond_price_fund_asof_v1(%s) WHERE security_id=%s",
            (obs_date + timedelta(days=5), _security_id("037833100")),
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == obs_date

        # STALE boundary on the single-observation security: age 30d is not stale;
        # age 31d is stale (consistent with MatchState.STALE, age >= 31).
        stale_sid = _security_id("459200101")
        cur.execute(
            "SELECT observation_age_days, is_stale FROM bond_price_fund_asof_v1(%s) WHERE security_id=%s",
            (obs_date + timedelta(days=30), stale_sid),
        )
        assert cur.fetchone() == (30, False)
        cur.execute(
            "SELECT observation_age_days, is_stale FROM bond_price_fund_asof_v1(%s) WHERE security_id=%s",
            (obs_date + timedelta(days=31), stale_sid),
        )
        assert cur.fetchone() == (31, True)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fund_asof_lane_can_never_return_latest_stamped_rows():
    """MANDATORY (SQL half): the point-in-time lane is structurally not the latest lane."""
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        price_observations.load_price_observations(
            conn,
            [price_input(observation_date=date(2026, 6, 1), cusip9="037833100", price=99.0)],
            as_of=AS_OF, source_run_id=run_id,
        )
        _publish(conn, run_id, package_id)

        cur.execute("SELECT DISTINCT lane FROM bond_price_fund_asof_v1(%s)", (AS_OF,))
        assert [r[0] for r in cur.fetchall()] == ["fund_asof"]
        cur.execute("SELECT DISTINCT lane FROM bond_price_latest_v1")
        assert [r[0] for r in cur.fetchall()] == ["latest"]
        # The lane domain rejects any value outside the closed vocabulary.
        with pytest.raises(psycopg.Error):
            cur.execute("SELECT 'bogus'::bond_price_lane")
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_observation_rows_are_immutable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _ = base_fixture(cur)
        obs = price_input(observation_date=date(2026, 6, 1), cusip9="037833100", price=99.0)
        price_observations.load_price_observations(conn, [obs], as_of=AS_OF, source_run_id=run_id)
        with pytest.raises(psycopg.Error, match="bond_price_observation is immutable"):
            cur.execute("UPDATE bond_price_observation SET price=1 WHERE observation_id=%s", (obs.observation_id,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        with pytest.raises(psycopg.Error, match="bond_price_observation is immutable"):
            cur.execute("DELETE FROM bond_price_observation WHERE observation_id=%s", (obs.observation_id,))
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
            "VALUES(%s,'bond_price_observation_v1',1,%s,%s,%s)",
            (publication_id, run_id, package_id, "a" * 64),
        )
        with pytest.raises(psycopg.Error, match="requires a validated publication"):
            cur.execute("SELECT sec_set_current_derived_publication('bond_price_observation_v1',%s)", (publication_id,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute("SELECT count(*) FROM sec_derived_current_pointers")
        assert cur.fetchone()[0] == 0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_snapshot_rows_frozen_after_validation():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        price_observations.load_price_observations(
            conn,
            [price_input(observation_date=date(2026, 6, 1), cusip9="037833100", price=99.0)],
            as_of=AS_OF, source_run_id=run_id,
        )
        result = _publish(conn, run_id, package_id)
        with pytest.raises(psycopg.Error, match="requires a prepared bond_price_observation_v1 publication"):
            cur.execute(
                "INSERT INTO bond_price_observation_v1"
                "(publication_id,source_run_id,security_id,observation_date,source_row_number,price_state,"
                " price_type,accrued_treatment,db_type_state,daily_key_state,measured_at,provenance) "
                "VALUES(%s,%s,%s,%s,0,'present','trade','clean','null','unique_in_matching_cohort',%s,'{}')",
                (result["publication_id"], run_id, uuid4(), date(2026, 6, 1), AS_OF),
            )
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_db_pit_rows_feed_the_matching_library():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _ = base_fixture(cur)
        price_observations.load_price_observations(
            conn,
            [price_input(observation_date=date(2026, 6, 1), cusip9="123456789", price=100.0, db_type=3)],
            as_of=AS_OF, source_run_id=run_id,
        )
        rows = price_observations.pit_observation_rows(conn, AS_OF)
        assert rows and all("lane" not in r for r in rows)  # canonical PIT source, unstamped
        holding = HoldingRecord(
            publication_id="pub", accession_number="acc", holding_id="h", source_run_id="run",
            report_date="2026-06-15", filing_date="2026-06-20", series_id="s", class_id="c",
            instrument_id="i", issuer_category="fixture_debt", asset_class="fixture_asset",
            instrument_structure="fixture_structure", original_cusip="123456789",
            signed_market_value=100.0, signed_pct_of_nav=10.0, currency="USD",
        )
        mapping = DebtMapping(rules=(("fixture_debt", "fixture_asset", "fixture_structure", "eligible_debt"),))
        matched = price_observations.match_fund_holdings_asof(
            [holding], mapping, rows, ["123456789"], "2026-01-01", "2026-12-31"
        )
        assert matched[0].state is MatchState.MATCHED
        assert matched[0].is_144a is True
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
