"""Registered materializer for the public ``bond_serving_v1`` product.

Projects the current bond security / price-lane / N-PORT reverse-lookup snapshots
into the public-only ``bond_serving_facts`` surface across the four bond serving
surfaces (catalog / detail / observations / fund_exposure) and promotes one
complete serving version via the shared derived-publication current pointer. A
SIBLING product to ``sec_regulatory_serving_v1`` -- bonds publish on their own
cadence/lifecycle so fund dossier freshness never couples to bond freshness. The
app pins an exact publication of this product.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
Global Constraint: ships without running any production backfill or authorizing
any production price/holdings source (the TRACE 144A pilot authorizes none). When
no current bond snapshot / validated source anchor exists the worker is a no-op.
"""
from __future__ import annotations

import subprocess
from datetime import date
from typing import Any

from src.bonds import serving_materializer as materializer
from src.db import LOCK_BOND_SERVING, advisory_lock, connect, resolve_dsn


def _code_revision() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _resolve_as_of(conn: Any, calc_date: str | None) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    row = conn.execute("SELECT max(measured_at) FROM sec_current_bond_security_v1").fetchone()
    return row[0] if row and row[0] else None


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    with connect(resolve_dsn(dsn)) as conn, advisory_lock(conn, LOCK_BOND_SERVING) as acquired:
        if not acquired:
            return {"state": "locked", "rows": 0}
        materializer.install_schema(conn)
        # No current bond security snapshot yet -> nothing to serve (dark until backfill).
        if not conn.execute(
            "SELECT to_regclass('sec_current_bond_security_v1') IS NOT NULL"
        ).fetchone()[0]:
            conn.commit()
            return {"state": "no_source", "rows": 0}
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_securities", "rows": 0}
        try:
            result = materializer.materialize(conn, as_of=as_of, code_revision=_code_revision())
        except RuntimeError:
            conn.rollback()
            return {"state": "no_source", "rows": 0}
        conn.commit()
    return {"state": "ok", **result}
