"""Disposable-PostgreSQL integration for the panel publication writer."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
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


def _insert_direct_base(
    conn,
    *,
    publication_id,
    config_hash: str,
    code_revision: str,
    cusip_id: str,
    source_lineage: dict[str, object],
    gate_evidence: dict[str, object],
    model_cusip_id: str | None = None,
) -> None:
    month = date(2024, 1, 1)
    model_cusip_id = model_cusip_id or cusip_id
    conn.execute(
        "INSERT INTO bond_panel_publications (publication_id, publication_status, config_hash, "
        "input_fingerprint, code_revision, first_month, last_closed_month, open_month, "
        "snapshot_rows, rv_signal_rows, returns_rows, ratings_pit_rows, source_lineage, gate_evidence) "
        "VALUES (%s, 'prepared', %s, %s, %s, %s, %s, NULL, 1, 1, 1, 1, %s::jsonb, %s::jsonb)",
        (
            publication_id,
            config_hash,
            "f" * 64,
            code_revision,
            month,
            month,
            json.dumps(source_lineage, sort_keys=True),
            json.dumps(gate_evidence, sort_keys=True),
        ),
    )
    conn.execute(
        "INSERT INTO bond_panel_snapshot (publication_id, month, cusip_id, eligibility_state, "
        "eligibility_reason, payload) VALUES (%s, %s, %s, 'included', 'eligible', '{}'::jsonb)",
        (publication_id, month, cusip_id),
    )
    conn.execute(
        "INSERT INTO bond_panel_rv_signal (publication_id, month, cusip_id, eligibility_state, "
        "eligibility_reason, payload) VALUES (%s, %s, %s, 'included', 'eligible', '{}'::jsonb)",
        (publication_id, month, model_cusip_id),
    )
    conn.execute(
        "INSERT INTO bond_panel_returns (publication_id, month, cusip_id, total_return, exit_basis, "
        "payload) VALUES (%s, %s, %s, 0.01, 'observed', '{}'::jsonb)",
        (publication_id, month, model_cusip_id),
    )
    conn.execute(
        "INSERT INTO bond_panel_rating_pit (publication_id, month, cusip_id, rating_bucket, "
        "rating_state, rating_reason, payload) VALUES (%s, %s, %s, 'BBB', 'static_current', "
        "'migration_test', '{}'::jsonb)",
        (publication_id, month, cusip_id),
    )
    conn.execute(
        "UPDATE bond_panel_publications SET publication_status='validated', validated_at=now() "
        "WHERE publication_id=%s",
        (publication_id,),
    )


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

            sibling = materialize(
                conn,
                as_of=date(2024, 4, 15),
                code_revision="db-test-sibling",
                facts=_facts(["2024-03-01", "2024-04-01"], ["2024-03-01"]),
                source_lineage={"test": "db-sibling"},
                parent_publication_id=delta.publication_id,
                first_month=date(2024, 1, 1),
                last_closed_month=date(2024, 3, 1),
                open_month=date(2024, 4, 1),
            )
            with pytest.raises(MaterializationError, match="no longer current"):
                materialize(
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
            assert str(pointer) == sibling.publication_id
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


@pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"),
    reason="SEC_TEST_DATABASE_URL unavailable",
)
def test_legacy_pointer_remains_readable_until_authorized_complete_reg_s_base_transition() -> None:
    import psycopg
    from psycopg import sql

    legacy_hash = "0c0d78a866bc1090"
    active_hash = "180a82b3f1413d43"
    schema = f"test_bond_panel_transition_{uuid4().hex}"
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
            conn.execute(
                "ALTER TABLE bond_panel_publications "
                "DROP CONSTRAINT bond_panel_publications_config_hash_check"
            )
            conn.execute(
                "ALTER TABLE bond_panel_publications ADD CONSTRAINT "
                "bond_panel_publications_config_hash_check "
                f"CHECK (config_hash IN ('{legacy_hash}', '{active_hash}')) NOT VALID",
            )
            legacy_id = uuid4()
            _insert_direct_base(
                conn,
                publication_id=legacy_id,
                config_hash=legacy_hash,
                code_revision="legacy-base",
                cusip_id="LEGACY144",
                source_lineage={"distribution_rule": "rule_144a"},
                gate_evidence={"test": "legacy"},
            )
            conn.execute(
                "INSERT INTO bond_panel_app_pointer (product, publication_id) "
                "VALUES ('bond_panel_v1', %s)",
                (legacy_id,),
            )

            install_schema(conn)
            assert conn.execute(
                "SELECT cusip_id FROM bond_panel_current_snapshot_v1"
            ).fetchall() == [("LEGACY144",)]

            unauthorized_id = uuid4()
            _insert_direct_base(
                conn,
                publication_id=unauthorized_id,
                config_hash=active_hash,
                code_revision="unauthorized-reg-s-base",
                cusip_id="REGSUNAUT",
                source_lineage={
                    "distribution_rule": "reg_s",
                    "distribution_mapping_snapshot_id": "snapshot-1",
                },
                gate_evidence={},
            )
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="authorized complete Reg S replacement base",
            ):
                with conn.transaction():
                    conn.execute(
                        "UPDATE bond_panel_app_pointer SET publication_id=%s "
                        "WHERE product='bond_panel_v1'",
                        (unauthorized_id,),
                    )

            malformed_id = uuid4()
            malformed_revision = "malformed-reg-s-base"
            _insert_direct_base(
                conn,
                publication_id=malformed_id,
                config_hash=active_hash,
                code_revision=malformed_revision,
                cusip_id="REGSSNAP1",
                model_cusip_id="REGSORPH1",
                source_lineage={
                    "distribution_rule": "reg_s",
                    "distribution_mapping_snapshot_id": "snapshot-1",
                },
                gate_evidence={
                    "config_transition": {
                        "contract": "rule_144a_to_reg_s_base_v1",
                        "from_publication_id": str(legacy_id),
                        "from_config_hash": legacy_hash,
                        "to_config_hash": active_hash,
                        "authorized_code_revision": malformed_revision,
                    }
                },
            )
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="authorized complete Reg S replacement base",
            ):
                with conn.transaction():
                    conn.execute(
                        "UPDATE bond_panel_app_pointer SET publication_id=%s "
                        "WHERE product='bond_panel_v1'",
                        (malformed_id,),
                    )

            authorized_id = uuid4()
            authorized_revision = "authorized-reg-s-base"
            _insert_direct_base(
                conn,
                publication_id=authorized_id,
                config_hash=active_hash,
                code_revision=authorized_revision,
                cusip_id="REGSEXEC1",
                source_lineage={
                    "distribution_rule": "reg_s",
                    "distribution_mapping_snapshot_id": "snapshot-1",
                },
                gate_evidence={
                    "config_transition": {
                        "contract": "rule_144a_to_reg_s_base_v1",
                        "from_publication_id": str(legacy_id),
                        "from_config_hash": legacy_hash,
                        "to_config_hash": active_hash,
                        "authorized_code_revision": authorized_revision,
                    }
                },
            )
            conn.execute(
                "UPDATE bond_panel_app_pointer SET publication_id=%s "
                "WHERE product='bond_panel_v1'",
                (authorized_id,),
            )
            assert conn.execute(
                "SELECT cusip_id FROM bond_panel_current_snapshot_v1"
            ).fetchall() == [("REGSEXEC1",)]
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
