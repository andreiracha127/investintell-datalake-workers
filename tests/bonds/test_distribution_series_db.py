"""Optional PostgreSQL exercise for the immutable distribution-series registry."""

from __future__ import annotations

from datetime import date, datetime, timezone
import os
import threading
from uuid import uuid4

import pytest


@pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"),
    reason="SEC_TEST_DATABASE_URL unavailable",
)
def test_registry_loader_is_idempotent_and_db_resolver_is_governed() -> None:
    import psycopg
    from psycopg import sql

    from src.bonds.distribution_series import (
        DistributionMappingSnapshot,
        DistributionSnapshotApproval,
        DistributionPairDecision,
        DistributionPairIdentifier,
        DistributionParserObservation,
        DistributionSourceEvidence,
        InvalidDistributionSnapshotError,
        distribution_snapshot_content_hash,
        load_distribution_registry,
        install_schema,
        resolve_reg_s_cusip_from_db,
        resolve_reg_s_cusip_map_from_db,
    )

    schema = f"test_distribution_series_{uuid4().hex}"
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        try:
            install_schema(conn)
            install_schema(conn)
            source = DistributionSourceEvidence(
                "source-1", "0000000000-24-000001", "F-4", "prospectus",
                "https://sec.example/source", datetime(2024, 1, 2, tzinfo=timezone.utc), "a" * 64, "parser-v1",
            )
            rule_144a_observation = DistributionParserObservation(
                "observation-144a", source.source_evidence_id, "parser-v1", "page=3;block=2",
                "Rule 144A CUSIP", "123456789", "123456789", "validated",
            )
            reg_s_observation = DistributionParserObservation(
                "observation-regs", source.source_evidence_id, "parser-v1", "page=3;block=2",
                "Regulation S CUSIP", "987654321", "987654321", "validated",
            )
            reg_s_isin_observation = DistributionParserObservation(
                "observation-regs-isin", source.source_evidence_id, "parser-v1", "page=3;block=2",
                "Regulation S ISIN", "XS1234567890", "XS1234567890", "validated",
            )
            decision = DistributionPairDecision(
                "decision-1", "snapshot-1", "approved", rule_144a_observation.parser_observation_id,
                date(2024, 1, 1), pair_key="issue-pair-1",
            )
            identifiers = (
                DistributionPairIdentifier("id-144a", decision.decision_id, rule_144a_observation.parser_observation_id, "rule_144a", "cusip9", "123456789", "permanent", date(2024, 1, 1)),
                DistributionPairIdentifier("id-regs", decision.decision_id, reg_s_observation.parser_observation_id, "reg_s", "cusip9", "987654321", "permanent", date(2024, 1, 1)),
                DistributionPairIdentifier("id-regs-isin", decision.decision_id, reg_s_isin_observation.parser_observation_id, "reg_s", "isin", "XS1234567890", "permanent", date(2024, 1, 1)),
            )
            snapshot = DistributionMappingSnapshot(
                "snapshot-1", "draft",
                distribution_snapshot_content_hash("snapshot-1", (decision,), identifiers),
            )
            approval = DistributionSnapshotApproval(snapshot.snapshot_id, snapshot.content_hash)
            first_load = load_distribution_registry(
                conn, source_evidence=(source,), parser_observations=(rule_144a_observation, reg_s_observation, reg_s_isin_observation), snapshots=(snapshot,),
                decisions=(decision,), identifiers=identifiers, approvals=(approval,),
            )
            second_load = load_distribution_registry(
                conn, source_evidence=(source,), parser_observations=(rule_144a_observation, reg_s_observation, reg_s_isin_observation), snapshots=(snapshot,),
                decisions=(decision,), identifiers=identifiers, approvals=(approval,),
            )
            assert first_load == {
                "source_evidence": 1, "parser_observations": 3, "snapshots": 1,
                "decisions": 1, "identifiers": 3, "approvals": 1,
            }
            assert second_load == {
                "source_evidence": 0, "parser_observations": 0, "snapshots": 0,
                "decisions": 0, "identifiers": 0, "approvals": 0,
            }
            resolved = resolve_reg_s_cusip_from_db(
                conn, snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1), reference_cusip9="123456789",
            )
            assert resolved.reg_s_cusip9 == "987654321"
            assert resolved.reg_s_isin == "XS1234567890"
            bulk = resolve_reg_s_cusip_map_from_db(
                conn, snapshot_id=snapshot.snapshot_id, as_of=date(2024, 6, 1),
                reference_cusip9s=("123456789", "unmapped"),
            )
            assert bulk.resolutions["123456789"] == resolved
            assert bulk.resolutions["123456789"].reg_s_isin == "XS1234567890"
            assert bulk.reason_by_reference == {"UNMAPPED": "no_validated_source"}
            forged_decision = DistributionPairDecision(
                "decision-forged", "snapshot-forged", "approved",
                rule_144a_observation.parser_observation_id, date(2024, 1, 1),
                pair_key="issue-pair-forged",
            )
            forged_identifiers = (
                DistributionPairIdentifier("id-forged-144a", forged_decision.decision_id, rule_144a_observation.parser_observation_id, "rule_144a", "cusip9", "123456789", "permanent", date(2024, 1, 1)),
                DistributionPairIdentifier("id-forged-regs", forged_decision.decision_id, reg_s_observation.parser_observation_id, "reg_s", "cusip9", "987654321", "permanent", date(2024, 1, 1)),
            )
            forged_snapshot = DistributionMappingSnapshot("snapshot-forged", "draft", "f" * 64)
            load_distribution_registry(
                conn, snapshots=(forged_snapshot,), decisions=(forged_decision,),
                identifiers=forged_identifiers,
            )
            conn.execute(
                "INSERT INTO bond_distribution_snapshot_approval(snapshot_id,content_hash) VALUES(%s,%s)",
                (forged_snapshot.snapshot_id, forged_snapshot.content_hash),
            )
            with pytest.raises(InvalidDistributionSnapshotError, match="snapshot_content_hash_mismatch"):
                resolve_reg_s_cusip_from_db(
                    conn, snapshot_id=forged_snapshot.snapshot_id, as_of=date(2024, 6, 1),
                    reference_cusip9="123456789",
                )
            late_decision = DistributionPairDecision(
                "late-decision", snapshot.snapshot_id, "approved", rule_144a_observation.parser_observation_id,
                date(2024, 1, 1), pair_key="late-pair",
            )
            with pytest.raises(psycopg.Error, match="closed after approval"):
                load_distribution_registry(conn, decisions=(late_decision,))
            with pytest.raises(psycopg.Error, match="immutable"):
                conn.execute("UPDATE bond_distribution_source_evidence SET source_url='changed'")
            conn.rollback()
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"),
    reason="SEC_TEST_DATABASE_URL unavailable; two-connection lock regression skipped",
)
def test_snapshot_lock_serializes_composition_before_approval() -> None:
    import psycopg
    from psycopg import sql

    from src.bonds.distribution_series import (
        DistributionMappingSnapshot,
        DistributionPairDecision,
        DistributionPairIdentifier,
        DistributionParserObservation,
        DistributionSourceEvidence,
        approve_mapping_snapshot,
        distribution_snapshot_content_hash,
        install_schema,
        load_distribution_registry,
    )

    schema = f"test_distribution_series_lock_{uuid4().hex}"
    source = DistributionSourceEvidence(
        "source-lock", "0000000000-24-000002", "F-4", "prospectus",
        "https://sec.example/source-lock", datetime(2024, 1, 2, tzinfo=timezone.utc),
        "b" * 64, "parser-v1",
    )
    rule_144a_observation = DistributionParserObservation(
        "observation-lock-144a", source.source_evidence_id, "parser-v1", "page=4;block=1",
        "Rule 144A CUSIP", "123456789", "123456789", "validated",
    )
    reg_s_observation = DistributionParserObservation(
        "observation-lock-regs", source.source_evidence_id, "parser-v1", "page=4;block=1",
        "Regulation S CUSIP", "987654321", "987654321", "validated",
    )
    decision = DistributionPairDecision(
        "decision-lock", "snapshot-lock", "approved", rule_144a_observation.parser_observation_id,
        date(2024, 1, 1), pair_key="issue-pair-lock",
    )
    identifiers = (
        DistributionPairIdentifier("id-lock-144a", decision.decision_id, rule_144a_observation.parser_observation_id, "rule_144a", "cusip9", "123456789", "permanent", date(2024, 1, 1)),
        DistributionPairIdentifier("id-lock-regs", decision.decision_id, reg_s_observation.parser_observation_id, "reg_s", "cusip9", "987654321", "permanent", date(2024, 1, 1)),
    )
    snapshot = DistributionMappingSnapshot(
        "snapshot-lock", "draft",
        distribution_snapshot_content_hash("snapshot-lock", (decision,), identifiers),
    )
    url = os.environ["SEC_TEST_DATABASE_URL"]
    setup = psycopg.connect(url)
    composer = psycopg.connect(url)
    finalizer = psycopg.connect(url)
    try:
        setup.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        setup.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        install_schema(setup)
        load_distribution_registry(
            setup,
            source_evidence=(source,),
            parser_observations=(rule_144a_observation, reg_s_observation),
            snapshots=(snapshot,),
        )
        setup.commit()
        for conn in (composer, finalizer):
            conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
            conn.commit()

        composition_started = threading.Event()
        release_composer = threading.Event()
        finalized = threading.Event()
        errors: list[BaseException] = []

        def compose() -> None:
            try:
                with composer.transaction():
                    load_distribution_registry(composer, decisions=(decision,), identifiers=identifiers)
                    composition_started.set()
                    if not release_composer.wait(timeout=5):
                        raise TimeoutError("test did not release composition transaction")
            except BaseException as error:  # record worker failures for the main assertion
                errors.append(error)

        def finalize() -> None:
            try:
                assert approve_mapping_snapshot(
                    finalizer, snapshot_id=snapshot.snapshot_id, content_hash=snapshot.content_hash
                )
                finalizer.commit()
            except BaseException as error:  # record worker failures for the main assertion
                errors.append(error)
            finally:
                finalized.set()

        compose_thread = threading.Thread(target=compose)
        compose_thread.start()
        assert composition_started.wait(timeout=5)
        finalize_thread = threading.Thread(target=finalize)
        finalize_thread.start()
        assert not finalized.wait(timeout=0.2)
        release_composer.set()
        compose_thread.join(timeout=5)
        finalize_thread.join(timeout=5)
        assert not compose_thread.is_alive()
        assert not finalize_thread.is_alive()
        assert errors == []
    finally:
        for conn in (composer, finalizer):
            conn.close()
        setup.execute("SET search_path TO public")
        setup.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        setup.commit()
        setup.close()
