"""Materializer for the N-CEN derived-profile snapshots.

Three fund/registrant-grain snapshots are built over the amendment-aware
effective selection and published one complete version at a time through the
existing ``sec_derived_publications`` protocol (prepared -> validated -> current
pointer).  The heavy lifting lives in the SQL ``build_*`` functions; this module
only owns the deterministic publication identity and the lifecycle wiring so a
build is reproducible and restartable.

Contract mirrors the other SEC derived builders: the caller resolves the
validated N-CEN source run/package (Global Constraint 9 keeps that out of any
production mutation here), and each product is promoted atomically.
"""
from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import psycopg

ROOT = Path(__file__).resolve().parents[2]

# uuid5 namespace for reproducible publication identities (distinct constant).
_NAMESPACE = UUID("7c0f4d4e-9b2a-5f3c-8d1e-2a6b9c4e7f10")

# product -> SQL build function.  One complete version per product is promoted.
PRODUCTS: dict[str, str] = {
    "ncen_structure_profile_v1": "build_ncen_structure_profiles",
    "ncen_provider_network_v1": "build_ncen_provider_network_profiles",
    "ncen_operational_event_v1": "build_ncen_operational_event_profiles",
    "ncen_liquidity_backstop_v1": "build_ncen_liquidity_backstop_profiles",
    "ncen_securities_lending_v1": "build_ncen_securities_lending_profiles",
    "ncen_etf_primary_market_v1": "build_ncen_etf_primary_market_profiles",
    "ncen_closed_end_v1": "build_ncen_closed_end_profiles",
    "ncen_expense_brokerage_v1": "build_ncen_expense_brokerage_profiles",
}

_SCHEMA_FILES = (
    "sec_derived_publications.sql",
    "ncen_effective_views.sql",
    "ncen_derived_common.sql",
    "ncen_structure_profiles.sql",
    "ncen_provider_network_profiles.sql",
    "ncen_operational_event_profiles.sql",
    "ncen_liquidity_backstop_profiles.sql",
    "ncen_securities_lending_profiles.sql",
    "ncen_etf_primary_market_profiles.sql",
    "ncen_closed_end_profiles.sql",
    "ncen_expense_brokerage_profiles.sql",
)


def install_schema(conn: psycopg.Connection) -> None:
    """Apply the derived-profile DDL idempotently (publications must pre-exist)."""
    with conn.cursor() as cur:
        for name in _SCHEMA_FILES:
            cur.execute((ROOT / "schemas" / name).read_text(encoding="utf-8"))


def publication_id_for(product: str, as_of: date, code_revision: str) -> UUID:
    return uuid5(_NAMESPACE, f"{product}|{as_of.isoformat()}|{code_revision}")


def _build_fingerprint(product: str, as_of: date, source_run_id: UUID) -> str:
    return hashlib.sha256(f"{product}|{as_of.isoformat()}|{source_run_id}".encode()).hexdigest()


def materialize_product(
    conn: psycopg.Connection,
    *,
    product: str,
    as_of: date,
    source_run_id: UUID,
    source_package_id: UUID,
    code_revision: str,
) -> dict[str, Any]:
    """Prepare -> build -> validate -> current, idempotently, for one product."""
    if product not in PRODUCTS:
        raise ValueError(f"unknown N-CEN derived product {product!r}")
    build_fn = PRODUCTS[product]
    publication_id = publication_id_for(product, as_of, code_revision)

    existing = conn.execute(
        "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
        (publication_id,),
    ).fetchone()
    if existing is None:
        version = conn.execute(
            "SELECT COALESCE(max(publication_version),0)+1 FROM sec_derived_publications WHERE product=%s",
            (product,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO sec_derived_publications"
            "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint) "
            "VALUES(%s,%s,%s,%s,%s,%s)",
            (publication_id, product, version, source_run_id, source_package_id,
             _build_fingerprint(product, as_of, source_run_id)),
        )
        lifecycle = "prepared"
    else:
        lifecycle = existing[0]

    inserted: int | None = None
    if lifecycle == "prepared":
        inserted = conn.execute(f"SELECT {build_fn}(%s,%s)", (publication_id, as_of)).fetchone()[0]
        conn.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))

    current = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s", (product,)
    ).fetchone()
    if current is None or current[0] != publication_id:
        conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (product, publication_id))

    return {
        "product": product,
        "publication_id": str(publication_id),
        "rows_built": inserted,
        "state": "current",
    }


def materialize_all(
    conn: psycopg.Connection,
    *,
    as_of: date,
    source_run_id: UUID,
    source_package_id: UUID,
    code_revision: str,
) -> list[dict[str, Any]]:
    return [
        materialize_product(
            conn, product=product, as_of=as_of, source_run_id=source_run_id,
            source_package_id=source_package_id, code_revision=code_revision,
        )
        for product in PRODUCTS
    ]
