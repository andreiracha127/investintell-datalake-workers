"""Shared helpers for the daily publication chain tests (Increment 2, Task 6).

DSN-agnostic by design (Global Constraint): every caller reads the disposable
Postgres endpoint from ``SEC_TEST_DATABASE_URL`` so the suite runs identically
under the keyword and URL DSN conventions. The leading underscore keeps pytest
from collecting this module as a test file.

The engine tests drive the chain with *injected* stage units (fast, deterministic)
plus, for the promotion/rollback cases, the REAL derived-publication protocol
(``sec_derived_publications.sql``) so fail-closed promotion and pointer rollback
are proven against the production mechanism, not a stand-in.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from uuid import UUID, uuid4

import psycopg

ROOT = Path(__file__).resolve().parents[1]
DERIVED_PROTOCOL_SQL = ROOT / "schemas" / "sec_derived_publications.sql"


def base_dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def admin_connect() -> psycopg.Connection:
    """Autocommit admin connection for schema setup/teardown and assertions."""
    return psycopg.connect(base_dsn(), autocommit=True)


def new_schema(admin: psycopg.Connection) -> str:
    schema = f"daily_chain_{uuid4().hex}"
    admin.execute(f'CREATE SCHEMA "{schema}"')
    return schema


def worker_conn(schema: str) -> psycopg.Connection:
    """A non-autocommit connection pinned to ``schema`` (the chain's own conn)."""
    conn = psycopg.connect(base_dsn())
    conn.execute(f'SET search_path TO "{schema}"')
    conn.commit()
    return conn


def install_derived_protocol(conn: psycopg.Connection) -> None:
    """Minimal source-lineage tables + the real derived-publication protocol."""
    conn.execute("CREATE TABLE IF NOT EXISTS sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    conn.execute("CREATE TABLE IF NOT EXISTS sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    conn.execute(
        "CREATE OR REPLACE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    conn.execute(DERIVED_PROTOCOL_SQL.read_text(encoding="utf-8"))


def make_validated_publication(conn: psycopg.Connection, product: str, version: int) -> UUID:
    """Prepare + validate one derived publication for ``product`` and return its id."""
    run_id, package_id, pub_id = uuid4(), uuid4(), uuid4()
    conn.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
    conn.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
    fingerprint = hashlib.sha256(f"{product}|{version}".encode()).hexdigest()
    conn.execute(
        "INSERT INTO sec_derived_publications "
        "(publication_id, product, publication_version, source_run_id, source_package_id, "
        " build_fingerprint, lifecycle_state) VALUES (%s,%s,%s,%s,%s,%s,'prepared')",
        (pub_id, product, version, run_id, package_id, fingerprint),
    )
    conn.execute("SELECT sec_validate_derived_publication(%s)", (pub_id,))
    return pub_id
