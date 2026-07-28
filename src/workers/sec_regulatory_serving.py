"""Registered materializer for the public ``sec_regulatory_serving_v1`` product.

Projects the current N-CEN/RR1 snapshot views into the public-only
``sec_regulatory_serving_facts`` surface and promotes one complete serving
version via the shared derived-publication current pointer. The app pins an exact
publication of this product; this worker never touches the app-owned composition
layer.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
Global Constraint 9: ships without running any production backfill; when no
current family snapshot / validated source anchor exists the worker is a no-op.
"""
from __future__ import annotations

import subprocess
from datetime import date
from typing import Any

from src.db import LOCK_SEC_REGULATORY_SERVING, advisory_lock, connect
from src.sec_serving import materializer


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
    row = conn.execute(
        """
        SELECT max(d) FROM (
            SELECT max(measured_at) AS d FROM sec_current_ncen_structure_profiles
            UNION ALL SELECT max(data_date) FROM sec_current_rr1_fee_profiles
        ) x
        """
    ).fetchone()
    return row[0] if row and row[0] else None


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None,
        allow_as_of_regression: bool = False) -> dict[str, object]:
    with connect(dsn) as conn, advisory_lock(conn, LOCK_SEC_REGULATORY_SERVING) as acquired:
        if not acquired:
            return {"state": "locked", "rows": 0}
        materializer.install_schema(conn)
        # No current family snapshot yet -> nothing to serve (dark until backfill).
        if not conn.execute(
            "SELECT to_regclass('sec_current_ncen_structure_profiles') IS NOT NULL"
        ).fetchone()[0]:
            conn.commit()
            return {"state": "no_source", "rows": 0}
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_effective_facts", "rows": 0}
        try:
            result = materializer.materialize(
                conn, as_of=as_of, code_revision=_code_revision(),
                allow_as_of_regression=allow_as_of_regression)
        except RuntimeError:
            conn.rollback()
            return {"state": "no_source", "rows": 0}
        conn.commit()
    return {"state": "ok", **result}
