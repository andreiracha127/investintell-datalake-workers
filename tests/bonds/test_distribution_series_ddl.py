"""Static contracts for the Regulation S distribution-series registry DDL."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_ddl_has_additive_immutable_evidence_and_governed_pair_registry() -> None:
    ddl = (ROOT / "schemas" / "bond_distribution_series_v1.sql").read_text(encoding="utf-8")

    for token in (
        "bond_distribution_source_evidence",
        "bond_distribution_parser_observation",
        "bond_distribution_mapping_snapshot",
        "bond_distribution_snapshot_approval",
        "bond_distribution_pair_decision",
        "bond_distribution_pair_identifier",
        "source_observation_id text NOT NULL REFERENCES bond_distribution_parser_observation",
        "bond_distribution_source_evidence is immutable",
        "bond_distribution_parser_observation is immutable",
        "bond_distribution_mapping_snapshot is immutable",
        "bond_distribution_pair_decision is immutable",
        "bond_distribution_pair_identifier is immutable",
        "distribution_rule IN ('reg_s','rule_144a')",
        "identifier_kind IN ('cusip9','isin','common_code')",
        "identifier_tenure IN ('temporary','permanent','not_stated')",
        "snapshot_status IN ('draft','approved','revoked')",
        "decision_state IN ('candidate','approved','ambiguous','rejected','revoked')",
        "raw_document_sha256",
        "sec_accession",
        "filed_at",
        "search_query_id",
        "block_locator",
        "exact_source_label",
        "normalized_value",
        "valid_to IS NULL OR valid_to > valid_from",
        "approved snapshot/reference CUSIP cannot map to multiple Reg S CUSIPs",
        "identifier source observation does not match value/kind/side taxonomy",
        "snapshot composition is closed after approval",
        "content hash is computed by the controlled loader path",
        "observed_value !~ '^[A-Z0-9]{9}$'",
        "observed_value !~ '^[A-Z0-9]{12}$'",
        "observed_value !~ '^[0-9]{9}$'",
        "FOR UPDATE",
        "snapshot composition is closed after approval",
    ):
        assert token in ddl, token


def test_ddl_keeps_common_code_local_and_does_not_create_global_security_aliases() -> None:
    ddl = (ROOT / "schemas" / "bond_distribution_series_v1.sql").read_text(encoding="utf-8")

    assert "registry-local" in ddl
    assert "bond_security_alias_v1" not in ddl
    assert "CREATE TABLE IF NOT EXISTS bond_security" not in ddl


def test_ddl_serializes_composition_and_approval_on_the_snapshot_row() -> None:
    ddl = (ROOT / "schemas" / "bond_distribution_series_v1.sql").read_text(encoding="utf-8")

    assert "WHERE snapshot_id = target_snapshot_id FOR UPDATE" in ddl
    assert "WHERE snapshot_id = NEW.snapshot_id FOR UPDATE" in ddl


def test_ddl_normalizes_registry_ownership_to_the_runtime_worker_role() -> None:
    ddl = (ROOT / "schemas" / "bond_distribution_series_v1.sql").read_text(encoding="utf-8")

    for relation in (
        "bond_distribution_source_evidence",
        "bond_distribution_parser_observation",
        "bond_distribution_mapping_snapshot",
        "bond_distribution_snapshot_approval",
        "bond_distribution_pair_decision",
        "bond_distribution_pair_identifier",
    ):
        assert f"ALTER TABLE {relation} OWNER TO worker_writer" in ddl
        assert f"REVOKE ALL ON TABLE {relation} FROM PUBLIC" in ddl

    for function in (
        "bond_distribution_prevent_conflicting_approved_cusip_mapping",
        "bond_distribution_pair_identifier_observation_guard",
        "bond_distribution_snapshot_composition_guard",
        "bond_distribution_snapshot_approval_guard",
        "bond_distribution_prevent_mutation",
    ):
        assert f"ALTER FUNCTION {function}() OWNER TO worker_writer" in ddl
        assert f"REVOKE ALL ON FUNCTION {function}() FROM PUBLIC" in ddl


def test_resolver_derives_validation_from_parser_observations_not_decision_flags() -> None:
    resolver = (ROOT / "src" / "bonds" / "distribution_series.py").read_text(encoding="utf-8")

    assert "source_validated" not in resolver
    assert "observation_state != \"validated\"" in resolver
    assert "== reference_observation.source_evidence_id" in resolver
    assert "== reference_observation.block_locator" in resolver
    assert "identifier_kind_from_source_label" in resolver
    assert "identifier.identifier_value == observation.normalized_value" in resolver
    assert "validate_distribution_snapshot_approval" in resolver
