"""bond_rating_history_v1 materializer + LICENSE GATE over the sec_derived_publications
protocol.

DSN-agnostic (reads SEC_TEST_DATABASE_URL); disposable-schema per test. Proves:
BOTH license paths — (a) license verified -> rating rows published with PIT
half-open windows and license_verified=true; (b) license NOT verified -> the WHOLE
product is published as state 'not_applicable' / reason 'no_licensed_source' with
ZERO rating rows (never data without a license); the per-observation
licensed_source_ref is mandatory (NOT NULL); prepared -> validated -> current +
idempotent rerun; agency codes stay opaque; immutable observations; partial build
can never become current.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bond_rating_fixtures import AGENCY_A, base_fixture, dsn, rating_input  # noqa: E402

from src.bonds import ratings  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

AS_OF = date(2026, 6, 30)
SEC_ID = uuid4()


def _publish(conn, run_id, package_id, *, license_verified):
    return ratings.materialize(
        conn, as_of=AS_OF, source_run_id=run_id, source_package_id=package_id,
        code_revision="testrev", license_verified=license_verified,
    )


def test_license_verified_publishes_rating_rows_with_pit_windows():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        ratings.load_rating_observations(
            conn,
            [
                rating_input(security_id=SEC_ID, rating="R_A", valid_from=date(2025, 1, 1),
                             valid_to=date(2026, 1, 1)),
                rating_input(security_id=SEC_ID, rating="R_AA", valid_from=date(2026, 1, 1),
                             valid_to=None),
            ],
            as_of=AS_OF, source_run_id=run_id,
        )
        result = _publish(conn, run_id, package_id, license_verified=True)
        assert result["state"] == "current"
        assert result["product_state"] == "active"
        assert result["license_verified"] is True
        assert result["published"] == 2

        cur.execute("SELECT product_state, license_verified, reason_code FROM sec_current_bond_rating_history_v1_status")
        assert cur.fetchone() == ("active", True, None)
        cur.execute(
            "SELECT rating, valid_from, valid_to, license_verified FROM bond_rating_history_v1 ORDER BY valid_from"
        )
        rows = cur.fetchall()
        assert rows[0] == ("R_A", date(2025, 1, 1), date(2026, 1, 1), True)
        assert rows[1] == ("R_AA", date(2026, 1, 1), None, True)
        # Agency codes stay opaque (no agency name anywhere in the snapshot).
        cur.execute("SELECT DISTINCT agency_code FROM bond_rating_history_v1")
        assert [r[0] for r in cur.fetchall()] == [AGENCY_A]

        again = _publish(conn, run_id, package_id, license_verified=True)
        assert again["publication_id"] == result["publication_id"]
        cur.execute("SELECT count(*) FROM sec_derived_publications WHERE product='bond_rating_history_v1'")
        assert cur.fetchone()[0] == 1
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_no_license_publishes_whole_product_not_applicable_with_zero_rows():
    """MANDATORY: without a verified license the WHOLE product is not_applicable."""
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        # Observations are present (and each carries a license ref), but the product
        # license is NOT verified -> nothing is published as data.
        ratings.load_rating_observations(
            conn,
            [rating_input(security_id=SEC_ID, rating="R_A", valid_from=date(2025, 1, 1))],
            as_of=AS_OF, source_run_id=run_id,
        )
        result = _publish(conn, run_id, package_id, license_verified=False)
        assert result["state"] == "current"
        assert result["product_state"] == "not_applicable"
        assert result["reason"] == "no_licensed_source"
        assert result["license_verified"] is False
        assert result["published"] == 0

        # A validated publication EXISTS (Task 6 gate can see a validated run), but it
        # carries NO rating rows.
        cur.execute("SELECT lifecycle_state FROM sec_current_derived_publications WHERE product='bond_rating_history_v1'")
        assert cur.fetchone() == ("validated",)
        cur.execute("SELECT count(*) FROM bond_rating_history_v1")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT product_state, reason_code, license_verified FROM sec_current_bond_rating_history_v1_status")
        assert cur.fetchone() == ("not_applicable", "no_licensed_source", False)
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_observation_without_license_ref_is_refused_by_ddl():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _ = base_fixture(cur)
        with pytest.raises(psycopg.Error):
            cur.execute(
                "INSERT INTO bond_rating_observation"
                "(observation_id, as_of, source_run_id, subject_kind, security_id, agency_code, rating, "
                " valid_from, licensed_source_ref, source_lineage) "
                "VALUES(%s,%s,%s,'security',%s,%s,'R_A',%s,NULL,'{\"e\":1}')",
                (uuid4(), AS_OF, run_id, SEC_ID, AGENCY_A, date(2025, 1, 1)),
            )
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_observation_rows_are_immutable():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _ = base_fixture(cur)
        obs = rating_input(security_id=SEC_ID, valid_from=date(2025, 1, 1))
        ratings.load_rating_observations(conn, [obs], as_of=AS_OF, source_run_id=run_id)
        with pytest.raises(psycopg.Error, match="bond_rating_observation is immutable"):
            cur.execute("UPDATE bond_rating_observation SET rating='X' WHERE observation_id=%s", (obs.observation_id,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        with pytest.raises(psycopg.Error, match="bond_rating_observation is immutable"):
            cur.execute("DELETE FROM bond_rating_observation WHERE observation_id=%s", (obs.observation_id,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_snapshot_row_cannot_coexist_with_unlicensed_build_db_level():
    """MANDATORY (DB-level): a rating row cannot be inserted under an unlicensed build.

    Even bypassing the Python materializer, the cross-table write guard forbids a
    snapshot row from coexisting with a not_applicable / license_verified=false build,
    so "no verified license => zero rating rows" is enforced by the DB itself.
    """
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)

        def _prepared_pub(license_verified, product_state, reason):
            pub = uuid4()
            version = cur.execute(
                "SELECT COALESCE(max(publication_version),0)+1 FROM sec_derived_publications "
                "WHERE product='bond_rating_history_v1'"
            ).fetchone()[0]
            cur.execute(
                "INSERT INTO sec_derived_publications"
                "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint) "
                "VALUES(%s,'bond_rating_history_v1',%s,%s,%s,%s)",
                (pub, version, run_id, package_id, "b" * 64),
            )
            cur.execute(
                "INSERT INTO bond_rating_history_v1_builds"
                "(publication_id,input_fingerprint,as_of_date,observation_input_count,product_state,"
                " reason_code,license_verified) VALUES(%s,%s,%s,0,%s,%s,%s)",
                (pub, "b" * 64, AS_OF, product_state, reason, license_verified),
            )
            return pub

        def _insert_row(pub):
            cur.execute(
                "INSERT INTO bond_rating_history_v1"
                "(publication_id,source_run_id,subject_kind,subject_ref,security_id,agency_code,rating,"
                " valid_from,license_verified,licensed_source_ref,measured_at,provenance) "
                "VALUES(%s,%s,'security',%s,%s,%s,'R_A',%s,true,'lic:x',%s,'{}')",
                (pub, run_id, str(SEC_ID), SEC_ID, AGENCY_A, date(2025, 1, 1), AS_OF),
            )

        # Unlicensed build -> the row is refused DB-side.
        unlicensed = _prepared_pub(False, "not_applicable", "no_licensed_source")
        with pytest.raises(psycopg.Error, match="requires a license-verified build"):
            _insert_row(unlicensed)
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute("SELECT count(*) FROM bond_rating_history_v1")
        assert cur.fetchone()[0] == 0

        # Licensed build -> the identical row is accepted (licensed path unchanged).
        licensed = _prepared_pub(True, "active", None)
        _insert_row(licensed)
        cur.execute("SELECT count(*) FROM bond_rating_history_v1 WHERE publication_id=%s", (licensed,))
        assert cur.fetchone()[0] == 1
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_partial_non_validated_build_can_never_become_current():
    import psycopg

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id = base_fixture(cur)
        publication_id = uuid4()
        cur.execute(
            "INSERT INTO sec_derived_publications"
            "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint) "
            "VALUES(%s,'bond_rating_history_v1',1,%s,%s,%s)",
            (publication_id, run_id, package_id, "a" * 64),
        )
        with pytest.raises(psycopg.Error, match="requires a validated publication"):
            cur.execute("SELECT sec_set_current_derived_publication('bond_rating_history_v1',%s)", (publication_id,))
        conn.rollback()
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute("SELECT count(*) FROM sec_derived_current_pointers")
        assert cur.fetchone()[0] == 0
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
