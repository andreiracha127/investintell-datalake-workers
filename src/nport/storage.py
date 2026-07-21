"""Instalação da superfície SQL N-PORT."""

from __future__ import annotations

import json
from pathlib import Path

import psycopg

from .schema import contract_catalog_payload


ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = ROOT / "schemas" / "nport_raw.sql"


def install_schema(conn: psycopg.Connection) -> None:
    """Instala idempotentemente a camada N-PORT após os manifestos SEC."""
    with conn.cursor() as cur:
        cur.execute(DDL_PATH.read_text(encoding="utf-8"))
        cur.execute(
            "SELECT nport_install_contract_catalog(%s::jsonb)",
            (json.dumps(contract_catalog_payload(), sort_keys=True, separators=(",", ":")),),
        )
