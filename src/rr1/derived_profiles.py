"""Materializer for the RR1 fee-profile snapshot.

``rr1_fee_profile_v1`` is the ONLY surviving RR1 derived product: it feeds the
curated share-class block at the top of the fund dossier.  The six other RR1
profile products (shareholder cost, waiver, class cost dispersion, turnover,
reported performance, benchmark) were removed on 2026-07-30 together with the
nine N-CEN profile products -- no metrics worker or quant engine consumed them,
they only backed redundant dossier accordions, and one of them
(``rr1_class_cost_dispersion_v1``) took the bond publication chain down in
production.

The product is built over the amendment-aware effective selection and promoted
one complete version at a time through the existing ``sec_derived_publications``
protocol (prepared -> validated -> current pointer).  The heavy lifting lives in
the SQL ``build_rr1_fee_profiles`` function; this module only owns the
deterministic publication identity and the lifecycle wiring.

Contract mirrors the other SEC derived builders: the caller resolves the
validated RR1 source run/package (Global Constraint 9 keeps that out of any
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
_NAMESPACE = UUID("b6e1f2a4-7c53-5a91-9d84-3f2c6a1e8b40")

# product -> SQL build function.  One complete version per product is promoted.
PRODUCTS: dict[str, str] = {
    "rr1_fee_profile_v1": "build_rr1_fee_profiles",
}

_SCHEMA_FILES = (
    "sec_derived_publications.sql",
    "rr1_effective_views.sql",
    "rr1_derived_common.sql",
    "rr1_fee_profiles.sql",
    # Versioned governance infra (no snapshot build function): installed with the
    # RR1 derived surface but deliberately absent from PRODUCTS.  It still gates
    # the crosswalk evidence that rides on a fee fact resolved from a custom tag.
    "rr1_custom_tag_crosswalk.sql",
)


def install_schema(conn: psycopg.Connection) -> None:
    """Apply the RR1 derived DDL idempotently (publications must pre-exist)."""
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
        raise ValueError(f"unknown RR1 derived product {product!r}")
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
