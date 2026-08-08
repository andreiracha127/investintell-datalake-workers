"""Static schema contracts for immutable worker-owned FF17 attributes."""
from __future__ import annotations

from pathlib import Path

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
