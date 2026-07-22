"""Registered materializer for the bond_rating_history_v1 point-in-time rating history.

Builds and promotes one complete ``bond_rating_history_v1`` snapshot over the
immutable ``bond_rating_observation`` inputs, under an advisory lock, through the
shared derived-publication protocol (prepared -> validated -> current pointer).

LICENSE GATE (fail-closed): the product-level ``license_verified`` flag is read
from the environment (``BOND_RATING_LICENSE_VERIFIED``) and defaults to FALSE.
This shipment authorizes NO production rating license, so by default the product
is published ``not_applicable`` / ``no_licensed_source`` with zero rating rows.
An operator who has verified a license may set the flag; even then the source is
fixtures-only in this increment (Global Constraint: nothing reaches production).

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
When no validated source run exists, or when no observations are present, the
worker is a no-op.
"""
from __future__ import annotations

import os
import subprocess
from datetime import date
from typing import Any

from src.bonds import ratings
from src.db import LOCK_BOND_RATING_HISTORY, advisory_lock, connect, resolve_dsn

_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})


def _license_verified() -> bool:
    return os.getenv("BOND_RATING_LICENSE_VERIFIED", "").strip().lower() in _TRUE_TOKENS


def _code_revision() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _latest_validated_source(conn: Any) -> tuple[Any, Any] | None:
    row = conn.execute(
        "SELECT r.run_id, p.package_id "
        "FROM sec_validated_raw_runs r "
        "JOIN sec_source_packages p ON p.run_id=r.run_id "
        "ORDER BY r.raw_validated_at DESC, p.package_id LIMIT 1"
    ).fetchone()
    return (row[0], row[1]) if row else None


def _resolve_as_of(conn: Any, calc_date: str | None) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    row = conn.execute("SELECT max(as_of) FROM bond_rating_observation").fetchone()
    return row[0] if row else None


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    with connect(resolve_dsn(dsn)) as conn, advisory_lock(conn, LOCK_BOND_RATING_HISTORY) as acquired:
        if not acquired:
            return {"state": "locked", "product": ratings.PRODUCT}
        ratings.install_schema(conn)
        source = _latest_validated_source(conn)
        if source is None:
            conn.commit()
            return {"state": "no_source", "product": ratings.PRODUCT}
        source_run_id, source_package_id = source
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_observations", "product": ratings.PRODUCT}
        result = ratings.materialize(
            conn, as_of=as_of, source_run_id=source_run_id,
            source_package_id=source_package_id, code_revision=_code_revision(),
            license_verified=_license_verified(),
        )
        conn.commit()
    return {"state": "ok", **result}
