"""Static contract checks for the bond_security_v1 DDL and worker wiring."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_ddl_declares_immutable_observation_and_guarded_snapshot() -> None:
    ddl = (ROOT / "schemas" / "bond_security_v1.sql").read_text(encoding="utf-8")
    for token in (
        "bond_security_observation",
        "bond_security_observation is immutable",
        "bond_security_v1_builds",
        "input_fingerprint",
        "char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$')",
        "bond_security_v1 build requires a prepared bond_security_v1 publication",
        "bond_security_v1 snapshot requires a prepared bond_security_v1 publication",
        "matching pinned build metadata",
        "alias_kind IN ('cusip9','isin')",
        "identity_state IN ('resolved','ambiguous')",
        "valid_from",
        "valid_to",
        "sec_current_bond_security_v1",
        "sec_current_bond_security_alias_v1",
    ):
        assert token in ddl, token
    # A reason code exists exactly when the identity is not resolved.
    assert "(identity_state = 'resolved') = (identity_reason_code IS NULL)" in ddl
    # PIT window sanity: valid_to closes at/after valid_from.
    assert "valid_to IS NULL OR valid_to >= valid_from" in ddl
    # The published snapshot never reaches back into raw/observation rows.
    assert "sec_derived_current_pointers" in ddl


def test_worker_uses_the_dedicated_advisory_lock_and_protocol() -> None:
    worker = (ROOT / "src" / "workers" / "bond_security_master.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_SECURITY_MASTER" in worker
    assert "advisory_lock" in worker
    db = (ROOT / "src" / "db.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_SECURITY_MASTER = 900_341" in db
    materializer = (ROOT / "src" / "bonds" / "security_master.py").read_text(encoding="utf-8")
    assert "sec_validate_derived_publication" in materializer
    assert "sec_set_current_derived_publication" in materializer


def test_identity_key_is_documented_in_ddl() -> None:
    ddl = (ROOT / "schemas" / "bond_security_v1.sql").read_text(encoding="utf-8")
    assert "uuid5(NAMESPACE_BOND_SECURITY, identity_key)" in ddl
    assert "cusip9:" in ddl and "isin:" in ddl
