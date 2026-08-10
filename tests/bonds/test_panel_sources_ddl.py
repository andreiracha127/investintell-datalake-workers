"""Static schema contracts for immutable worker-owned FF17 attributes."""
from __future__ import annotations

from pathlib import Path
import re

from src.bonds import panel_sources

ROOT = Path(__file__).resolve().parents[2]


def test_ddl_declares_static_ff17_sources_evidence_and_worker_writer_ownership() -> None:
    ddl = (ROOT / "schemas" / "bond_panel_sources.sql").read_text(encoding="utf-8")
    for token in (
        "CREATE TABLE IF NOT EXISTS bond_issuer_sector",
        "cusip9",
        "ff17num smallint NOT NULL CHECK (ff17num BETWEEN 1 AND 17)",
        "source text NOT NULL CHECK (source IN ('osbap', 'sic_map'))",
        "disagreement_count integer NOT NULL DEFAULT 0 CHECK (disagreement_count >= 0)",
        "source_provenance jsonb NOT NULL",
        "bond_issuer_sector is immutable",
        "BEFORE UPDATE OR DELETE",
        "ALTER TABLE bond_issuer_sector OWNER TO worker_writer",
        "ALTER FUNCTION bond_issuer_sector_immutable() OWNER TO worker_writer",
        "pg_roles",
        "current_user",
    ):
        assert token in ddl, token


def test_ddl_declares_immutable_monthly_osbap_trace_liquidity_evidence() -> None:
    ddl = (ROOT / "schemas" / "bond_panel_sources.sql").read_text(encoding="utf-8")
    for token in (
        "CREATE TABLE IF NOT EXISTS bond_liquidity_monthly",
        "PRIMARY KEY (cusip9, month, source)",
        "rel_bid_ask_bps numeric",
        "quoted_days integer NOT NULL",
        "dollar_volume numeric",
        "quote_state text NOT NULL CHECK (quote_state IN ('quoted', 'unquoted'))",
        "reason_code text NOT NULL",
        "source text NOT NULL CHECK (source = 'osbap_trace_historical')",
        "artifact_sha256",
        "bond_liquidity_monthly is immutable",
        "ALTER TABLE bond_liquidity_monthly OWNER TO worker_writer",
        "ALTER FUNCTION bond_liquidity_monthly_immutable() OWNER TO worker_writer",
        "REVOKE ALL ON bond_liquidity_monthly FROM PUBLIC",
    ):
        assert token in ddl, token
    for reason_code in panel_sources.LIQUIDITY_REASON_CODES:
        assert f"'{reason_code}'" in ddl, reason_code
    vocabulary = ddl.split("reason_code text NOT NULL CHECK (reason_code IN (", 1)[1].split(")),", 1)[0]
    assert set(re.findall(r"'([^']+)'", vocabulary)) == panel_sources.LIQUIDITY_REASON_CODES
