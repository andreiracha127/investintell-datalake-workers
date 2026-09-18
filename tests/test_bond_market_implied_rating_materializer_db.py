"""Disposable-PostgreSQL integration for the implied-rating publication writer.

Skipped without ``SEC_TEST_DATABASE_URL`` (the same requirement as the panel
materializer suite). Proves against REAL PostgreSQL: the shared derived
ledger lifecycle (prepared -> validated -> current), the product build pin, the
row write guard and CHECKs, the immutable snapshot, the plan-named read views
and the compare-and-set pointer.
"""
from __future__ import annotations

from datetime import date
import os
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest

from src.bonds import implied_rating as policy
from src.bonds.errors import BondError
from src.bonds.implied_rating_materializer import (
    ImpliedRatingPublication,
    install_schema,
    materialize,
    publication_id_for,
    publication_row_tuples,
)

ROOT = Path(__file__).resolve().parents[1]
MONTHS = pd.date_range("2025-01-01", periods=3, freq="MS")


def _snapshot(months: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    index_months = MONTHS if months is None else months
    rows = []
    for index, cusip in enumerate(("AAA000001", "BBB000002")):
        for month in index_months:
            rows.append({
                "cusip_id": cusip,
                "month": month,
                "price": 95.0,
                "spread_final_bps": 80.0 + 60.0 * index,
                "mod_dur": 5.0,
                "trade_count": 10,
                "dollar_volume": 1_000_000.0,
                "maturity_date": date(2035, 1, 1),
            })
    return pd.DataFrame(rows)


def _publication(frame: pd.DataFrame, *, panel_publication_id: str) -> ImpliedRatingPublication:
    end = pd.Timestamp(frame["month"].max())
    rows = policy.build_publication_rows(frame, last_closed_month=end)
    anchor = policy.market_anchor_for_snapshot(frame, last_closed_month=end)
    fingerprint = policy.snapshot_fingerprint(frame)
    confirmed, candidates = policy.default_counts(rows)
    return ImpliedRatingPublication(
        publication_id=publication_id_for(policy.POLICY_DIGEST, "db-test", fingerprint),
        panel_publication_id=panel_publication_id,
        policy_version=policy.POLICY_VERSION,
        policy_digest=policy.POLICY_DIGEST,
        code_revision="db-test",
        panel_last_closed_month=end.date(),
        first_month=frame["month"].min().date(),
        last_month=end.date(),
        input_fingerprint=fingerprint,
        l_anchor=anchor,
        rows_digest=policy.rows_digest(rows),
        d_confirmed_count=confirmed,
        d_candidate_count=candidates,
        row_count=len(rows),
    )


@pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"),
    reason="SEC_TEST_DATABASE_URL unavailable",
)
def test_implied_rating_publication_against_postgres() -> None:
    import psycopg
    from psycopg import sql

    schema = f"test_bond_implied_rating_{uuid4().hex}"
    run_id, package_id, panel_id = uuid4(), uuid4(), uuid4()
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(
            sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO worker_writer").format(
                sql.Identifier(schema)
            )
        )
        conn.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        )
        try:
            # Lineage the shared ledger validates against, plus the panel anchor
            # table our build pin references (a minimal stand-in for the panel).
            conn.execute(
                "CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, "
                "raw_validated_at timestamptz)"
            )
            conn.execute(
                "CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, "
                "run_id uuid NOT NULL)"
            )
            conn.execute(
                "CREATE VIEW sec_validated_raw_runs AS SELECT run_id, raw_validated_at "
                "FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
            )
            conn.execute("CREATE TABLE bond_panel_publications(publication_id uuid PRIMARY KEY)")
            conn.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
            conn.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
            conn.execute("INSERT INTO bond_panel_publications VALUES(%s)", (panel_id,))

            install_schema(conn)
            install_schema(conn)
            # The product's read views are OWNED by worker_writer (as in
            # production, where the worker applies the DDL) and PostgreSQL runs
            # a view with its owner's privileges: the role needs to read the
            # ledger the views join. Production grants are operational.
            conn.execute(
                sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO worker_writer").format(
                    sql.Identifier(schema)
                )
            )

            frame = _snapshot()
            publication = _publication(frame, panel_publication_id=str(panel_id))
            payload = publication_row_tuples(
                publication,
                policy.build_publication_rows(frame, last_closed_month=MONTHS[-1]),
            )
            result = materialize(conn, publication, payload, expected_pointer=None)
            assert result.lifecycle == "validated" and not result.reused

            lifecycle = conn.execute(
                "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
                (publication.publication_id,),
            ).fetchone()[0]
            assert lifecycle == "validated"
            pointer = conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers "
                "WHERE product = 'bond_market_implied_rating_v1'"
            ).fetchone()[0]
            assert str(pointer) == publication.publication_id

            header = conn.execute(
                "SELECT row_count, d_confirmed_count, d_candidate_count, policy_digest "
                "FROM bond_market_implied_rating_publications WHERE publication_id=%s",
                (publication.publication_id,),
            ).fetchone()
            assert header[0] == publication.row_count
            assert header[1] == publication.d_confirmed_count
            assert header[2] == publication.d_candidate_count
            assert header[3] == policy.POLICY_DIGEST
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1_current"
            ).fetchone()[0] == publication.row_count
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_app_pointer "
                "WHERE product = 'bond_market_implied_rating_v1'"
            ).fetchone()[0] == 1

            # Idempotent replay: same identity, same pin, no duplicate rows.
            again = materialize(
                conn, publication, payload, expected_pointer=publication.publication_id
            )
            assert again.reused
            assert conn.execute(
                "SELECT count(*) FROM bond_market_implied_rating_v1 WHERE publication_id=%s",
                (publication.publication_id,),
            ).fetchone()[0] == publication.row_count

            # A later panel month mints a NEW identity and advances the pointer
            # from the predecessor it was told to expect.
            later_months = pd.date_range("2025-01-01", periods=4, freq="MS")
            later_frame = _snapshot(later_months)
            later = _publication(later_frame, panel_publication_id=str(panel_id))
            later_result = materialize(
                conn, later,
                publication_row_tuples(
                    later,
                    policy.build_publication_rows(
                        later_frame, last_closed_month=later_months[-1]
                    ),
                ),
                expected_pointer=publication.publication_id,
            )
            assert not later_result.reused
            assert str(conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers "
                "WHERE product = 'bond_market_implied_rating_v1'"
            ).fetchone()[0]) == later.publication_id

            # Compare-and-set: a THIRD build told to expect the first
            # publication (stale) refuses and moves nothing.
            third_months = pd.date_range("2025-01-01", periods=5, freq="MS")
            third_frame = _snapshot(third_months)
            third = _publication(third_frame, panel_publication_id=str(panel_id))
            with pytest.raises(BondError) as stale:
                materialize(
                    conn, third,
                    publication_row_tuples(
                        third,
                        policy.build_publication_rows(
                            third_frame, last_closed_month=third_months[-1]
                        ),
                    ),
                    expected_pointer=publication.publication_id,
                )
            assert stale.value.code == "pointer_moved"
            assert str(conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers "
                "WHERE product = 'bond_market_implied_rating_v1'"
            ).fetchone()[0]) == later.publication_id

            # The published snapshot is immutable and the write guard refuses
            # rows for a validated publication. Each expected failure runs in
            # its own savepoint so the working transaction stays usable.
            with pytest.raises(psycopg.Error), conn.transaction():
                conn.execute(
                    "UPDATE bond_market_implied_rating_v1 SET implied_bucket='AAA' "
                    "WHERE publication_id=%s",
                    (publication.publication_id,),
                )
            with pytest.raises(psycopg.Error), conn.transaction():
                conn.execute(
                    "DELETE FROM bond_market_implied_rating_v1 WHERE publication_id=%s",
                    (publication.publication_id,),
                )
            with pytest.raises(psycopg.Error), conn.transaction():
                conn.execute(
                    "INSERT INTO bond_market_implied_rating_v1 "
                    "(publication_id, month, cusip_id, implied_bucket, witnessed, carry_months, "
                    "spell_id, d_candidate, d_confirmed, censoring, policy_version, policy_digest) "
                    "VALUES (%s, %s, 'X', 'AAA', true, 0, 1, false, false, 'none', %s, %s)",
                    (
                        publication.publication_id, MONTHS[0].date(),
                        policy.POLICY_VERSION, policy.POLICY_DIGEST,
                    ),
                )

            # A prepared publication with a pin still cannot publish a D row
            # without its event month (the DDL CHECK, not the guard).
            prepared_id = str(uuid4())
            conn.execute(
                "INSERT INTO sec_derived_publications (publication_id, product, "
                "publication_version, source_run_id, source_package_id, build_fingerprint) "
                "VALUES (%s, 'bond_market_implied_rating_v1', 99, %s, %s, %s)",
                (prepared_id, run_id, package_id, "0" * 64),
            )
            conn.execute(
                "INSERT INTO bond_market_implied_rating_v1_builds (publication_id, "
                "panel_publication_id, policy_version, policy_digest, code_revision, "
                "panel_last_closed_month, as_of_date, first_month, last_month, "
                "input_fingerprint, l_anchor, row_count, rows_digest, d_confirmed_count, "
                "d_candidate_count) VALUES (%s, %s, %s, %s, 'db-test', %s, %s, %s, %s, %s, "
                "%s, 1, %s, 0, 0)",
                (
                    prepared_id, str(panel_id), policy.POLICY_VERSION, policy.POLICY_DIGEST,
                    MONTHS[-1].date(), MONTHS[-1].date(), MONTHS[0].date(),
                    MONTHS[-1].date(), "1" * 64, 0.0, "2" * 64,
                ),
            )
            with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
                conn.execute(
                    "INSERT INTO bond_market_implied_rating_v1 "
                    "(publication_id, month, cusip_id, implied_bucket, witnessed, carry_months, "
                    "spell_id, d_candidate, d_confirmed, censoring, policy_version, policy_digest) "
                    "VALUES (%s, %s, 'X', 'D', true, 0, 1, true, false, 'none', %s, %s)",
                    (prepared_id, MONTHS[0].date(), policy.POLICY_VERSION, policy.POLICY_DIGEST),
                )
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
