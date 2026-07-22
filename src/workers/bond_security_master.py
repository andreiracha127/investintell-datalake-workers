"""Registered materializer for the bond_security_v1 point-in-time security master.

Builds and promotes one complete ``bond_security_v1`` snapshot (securities +
point-in-time aliases) over the immutable ``bond_security_observation`` inputs,
under an advisory lock, through the shared derived-publication protocol
(prepared -> validated -> current pointer).

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
Global Constraint 9: this ships without running any production backfill; when no
validated source run exists, or when no observations are present, the worker is a
no-op.
"""
from __future__ import annotations

import subprocess
from datetime import date
from typing import Any

from src.bonds import security_master
from src.db import LOCK_BOND_SECURITY_MASTER, advisory_lock, connect, resolve_dsn


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
    """Resolve the newest validated raw run and one of its loaded packages."""
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
    row = conn.execute("SELECT max(as_of) FROM bond_security_observation").fetchone()
    return row[0] if row else None


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    with connect(resolve_dsn(dsn)) as conn, advisory_lock(conn, LOCK_BOND_SECURITY_MASTER) as acquired:
        if not acquired:
            return {"state": "locked", "product": security_master.PRODUCT}
        security_master.install_schema(conn)
        source = _latest_validated_source(conn)
        if source is None:
            conn.commit()
            return {"state": "no_source", "product": security_master.PRODUCT}
        source_run_id, source_package_id = source
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_observations", "product": security_master.PRODUCT}
        result = security_master.materialize(
            conn, as_of=as_of, source_run_id=source_run_id,
            source_package_id=source_package_id, code_revision=_code_revision(),
        )
        conn.commit()
    return {"state": "ok", **result}
