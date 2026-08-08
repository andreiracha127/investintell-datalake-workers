"""Disposable-PostgreSQL integration for the panel publication writer."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
import os
from uuid import uuid4

import pytest

from src.bonds.panel_materializer import (
    MaterializationError,
    install_schema,
    materialize,
)


def _snapshot(month: str) -> dict[str, object]:
    return {
        "month": month,
        "cusip_id": "037833100",
        "issuer_id": "issuer-1",
        "eligibility_state": "included",
        "eligibility_reason": "eligible",
        "spread_definition": "ytm_minus_interpolated_dgs",
    }


def _rating(month: str) -> dict[str, object]:
    return {
        "month": month,
        "cusip_id": "037833100",
        "rating_bucket": "BBB",
        "rating_as_of_month": "2024-02-01",
        "rating_state": "static_current",
        "rating_reason": "static_rating_snapshot",
    }


def _rv(month: str) -> dict[str, object]:
    return {
        "month": month,
        "cusip_id": "037833100",
        "eligibility_state": "included",
        "eligibility_reason": "eligible",
        "residual_bps": 10.0,
        "rv_signal": 0.5,
    }


def _return(month: str) -> dict[str, object]:
    return {
        "month": month,
        "cusip_id": "037833100",
        "total_return": 0.01,
        "price_return": 0.008,
        "carry_return": 0.002,
        "exit_basis": "observed",
    }


def _facts(snapshot_months: list[str], closed_months: list[str]) -> dict[str, list[dict[str, object]]]:
    return {
        "snapshot": [_snapshot(month) for month in snapshot_months],
        "rv_signal": [_rv(month) for month in closed_months],
        "returns": [_return(month) for month in closed_months],
        "rating_pit": [_rating(month) for month in snapshot_months],
    }


@pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"),
    reason="SEC_TEST_DATABASE_URL unavailable",
)
def test_base_rerun_delta_and_failed_promotion_against_postgres() -> None:
    import psycopg
    from psycopg import sql

    schema = f"test_bond_panel_{uuid4().hex}"
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
            install_schema(conn)
            install_schema(conn)
            base_facts = _facts(["2024-01-01"], ["2024-01-01"])
            base = materialize(
                conn,
                as_of=date(2024, 1, 31),
                code_revision="db-test-base",
                facts=base_facts,
                source_lineage={"test": "db"},
            )
            again = materialize(
                conn,
                as_of=date(2024, 1, 31),
                code_revision="db-test-base",
                facts=base_facts,
                source_lineage={"test": "db"},
            )
            assert again.publication_id == base.publication_id

            delta = materialize(
                conn,
                as_of=date(2024, 3, 15),
                code_revision="db-test-delta",
                facts=_facts(["2024-02-01", "2024-03-01"], ["2024-02-01"]),
                source_lineage={"test": "db"},
                parent_publication_id=base.publication_id,
                first_month=date(2024, 1, 1),
                last_closed_month=date(2024, 2, 1),
                open_month=date(2024, 3, 1),
            )
            pointer = conn.execute(
                "SELECT publication_id FROM bond_panel_app_pointer "
                "WHERE product='bond_panel_v1'"
            ).fetchone()[0]
            assert str(pointer) == delta.publication_id
            assert conn.execute(
                "SELECT count(*) FROM bond_panel_current_snapshot_v1"
            ).fetchone()[0] == 3
            assert conn.execute(
                "SELECT count(*) FROM bond_panel_current_rating_pit_v1"
            ).fetchone()[0] == 3
            assert conn.execute(
                "SELECT count(*) FROM bond_panel_current_returns_v1"
            ).fetchone()[0] == 2
            assert conn.execute(
                "SELECT count(*) FROM bond_panel_current_rv_signal_v1"
            ).fetchone()[0] == 2
            assert conn.execute(
                "SELECT total_return, price_return, carry_return, exit_basis "
                "FROM bond_panel_returns WHERE publication_id=%s",
                (delta.publication_id,),
            ).fetchone() == (
                Decimal("0.01"),
                Decimal("0.008"),
                Decimal("0.002"),
                "observed",
            )
            assert conn.execute(
                "SELECT residual_bps, rv_signal, spread_definition "
                "FROM bond_panel_rv_signal WHERE publication_id=%s",
                (delta.publication_id,),
            ).fetchone() == (
                Decimal("10.0"),
                Decimal("0.5"),
                "ytm_minus_interpolated_dgs",
            )

            invalid = _facts(["2024-03-01", "2024-04-01"], ["2024-03-01"])
            invalid["rv_signal"].append(_rv("2024-04-01"))
            with pytest.raises(
                MaterializationError,
                match="open month may not carry returns or rv signal",
            ):
                materialize(
                    conn,
                    as_of=date(2024, 4, 15),
                    code_revision="db-test-invalid",
                    facts=invalid,
                    source_lineage={"test": "db"},
                    parent_publication_id=delta.publication_id,
                    first_month=date(2024, 1, 1),
                    last_closed_month=date(2024, 3, 1),
                    open_month=date(2024, 4, 1),
                )
            pointer_after = conn.execute(
                "SELECT publication_id FROM bond_panel_app_pointer "
                "WHERE product='bond_panel_v1'"
            ).fetchone()[0]
            assert pointer_after == pointer

            with pytest.raises(MaterializationError, match="current pointer"):
                materialize(
                    conn,
                    as_of=date(2024, 4, 16),
                    code_revision="db-test-stale-parent",
                    facts=_facts(["2024-03-01", "2024-04-01"], ["2024-03-01"]),
                    source_lineage={"test": "db"},
                    parent_publication_id=base.publication_id,
                    first_month=date(2024, 1, 1),
                    last_closed_month=date(2024, 3, 1),
                    open_month=date(2024, 4, 1),
                )
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
