from __future__ import annotations

import hashlib
import importlib.util
import re
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1]
BOOTSTRAP_PATH = ROOT / "schemas" / "instrument_ingestion.sql"
UPGRADE_PATH = ROOT / "schemas" / "nav_timeseries_provenance.sql"
RUNNER_PATH = ROOT / "scripts" / "nav_timeseries_provenance_schema.py"
BOOTSTRAP = BOOTSTRAP_PATH.read_text(encoding="utf-8")
UPGRADE = UPGRADE_PATH.read_text(encoding="utf-8")

EXPECTED_COLUMNS = {
    "source_nav": "NUMERIC(18,6)",
    "source_nav_kind": "VARCHAR(16)",
    "nav_repair_kind": "VARCHAR(48)",
    "return_start_date": "DATE",
    "return_source_boundary": "BOOLEAN",
    "return_uses_repaired_nav": "BOOLEAN",
    "return_semantics": "VARCHAR(48)",
    "return_verification_status": "VARCHAR(24)",
    "calendar_id": "VARCHAR(128)",
    "calendar_version": "VARCHAR(64)",
    "calendar_source": "TEXT",
}
LEGACY_COLUMNS = {
    "instrument_id": "UUID NOT NULL",
    "nav_date": "DATE NOT NULL",
    "nav": "NUMERIC(18,6)",
    "return_1d": "NUMERIC(12,8)",
    "aum_usd": "NUMERIC(18,2)",
    "currency": "VARCHAR(3)",
    "source": "VARCHAR(30) DEFAULT 'tiingo'",
    "return_type": "VARCHAR(10) NOT NULL DEFAULT 'arithmetic'",
}


def _normalized(value: str) -> str:
    return " ".join(value.split())


def _load_runner():
    spec = importlib.util.spec_from_file_location("nav_schema_runner", RUNNER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bootstrap_declares_exact_nullable_contract_and_preserves_legacy() -> None:
    normalized = _normalized(BOOTSTRAP)
    for name, declaration in {**LEGACY_COLUMNS, **EXPECTED_COLUMNS}.items():
        assert re.search(
            rf"\b{re.escape(name)}\s+{re.escape(declaration)}(?:\s*,|\s+PRIMARY)",
            normalized,
        )

    for name in EXPECTED_COLUMNS:
        declaration = re.search(rf"\b{re.escape(name)}\s+([^,]+),", normalized)
        assert declaration
        assert "NOT NULL" not in declaration.group(1)
        assert "DEFAULT" not in declaration.group(1)
        assert "GENERATED" not in declaration.group(1)
        assert "IDENTITY" not in declaration.group(1)

    assert "SELECT create_hypertable('nav_timeseries', 'nav_date'" in BOOTSTRAP


def test_upgrade_is_transactional_additive_guarded_and_idempotent() -> None:
    normalized = _normalized(UPGRADE)
    assert UPGRADE.startswith("-- Transactional additive upgrade")
    assert normalized.startswith(
        "-- Transactional additive upgrade for nav_timeseries provenance columns."
    )
    assert UPGRADE.rstrip().endswith("COMMIT;")
    assert "SET LOCAL lock_timeout = '2s';" in UPGRADE
    assert "SET LOCAL statement_timeout = '15s';" in UPGRADE
    assert "SET LOCAL idle_in_transaction_session_timeout = '20s';" in UPGRADE
    assert "LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE" in UPGRADE
    assert "requires exactly one explicit target schema" in UPGRADE
    assert "current_setting('search_path')" in UPGRADE
    assert "pg_catalog.pg_attribute" in UPGRADE
    assert "pg_catalog.pg_attrdef" in UPGRADE
    assert "pg_catalog.format_type" in UPGRADE
    assert "a.attnotnull" in UPGRADE
    assert "a.attgenerated" in UPGRADE
    assert "a.attidentity" in UPGRADE
    assert "NOT a.attisdropped" in UPGRADE
    assert UPGRADE.count("ALTER TABLE %I.%I") == 1

    for name, declaration in EXPECTED_COLUMNS.items():
        assert f"ADD COLUMN IF NOT EXISTS {name} {declaration}" in UPGRADE

    forbidden = (
        "CREATE TABLE",
        "CREATE_HYPERTABLE",
        "UPDATE NAV_TIMESERIES",
        "DELETE FROM NAV_TIMESERIES",
        "DROP COLUMN",
        "ALTER COLUMN",
        "CREATE INDEX",
        "GRANT ",
    )
    upper = UPGRADE.upper()
    for token in forbidden:
        assert token not in upper


def test_runner_allowlists_file_hash_and_schema_without_exposing_a_dsn() -> None:
    runner = _load_runner()
    expected_contract = tuple(
        (name, declaration.lower().replace("varchar", "character varying"))
        for name, declaration in EXPECTED_COLUMNS.items()
    )
    assert runner.EXPECTED_COLUMNS == expected_contract
    assert (
        runner._resolve_sql_file("schemas/nav_timeseries_provenance.sql")
        == UPGRADE_PATH.resolve()
    )
    with pytest.raises(runner.RunnerInputError, match="sql_file_not_allowlisted"):
        runner._resolve_sql_file("schemas/instrument_ingestion.sql")
    assert runner._validate_schema("public") == "public"
    with pytest.raises(runner.RunnerInputError, match="invalid_schema_identifier"):
        runner._validate_schema("public, pg_catalog")

    content = UPGRADE_PATH.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    assert runner._read_verified_sql(UPGRADE_PATH, digest) == (content, digest)
    with pytest.raises(runner.RunnerInputError, match="sql_sha256_mismatch"):
        runner._read_verified_sql(UPGRADE_PATH, "0" * 64)

    source = RUNNER_PATH.read_text(encoding="utf-8")
    assert 'os.environ["DATABASE_URL"]' in source
    assert "--dsn" not in source
    assert "str(exc)" not in source
