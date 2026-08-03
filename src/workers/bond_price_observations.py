"""Registered materializer for the bond_price_observation_v1 price/trade lanes.

Builds and promotes one complete ``bond_price_observation_v1`` snapshot (the
point-in-time price panel behind the informative ``latest`` lane and the
point-in-time ``fund_asof`` lane) over the immutable ``bond_price_observation``
inputs, under an advisory lock, through the shared derived-publication protocol
(prepared -> validated -> current pointer).

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
Global Constraint: this ships without running any production backfill or
authorizing any production price source. When no validated source run exists, or
when no observations are present, the worker is a no-op. FIXTURES ONLY — the 144A
pricing pilot does not authorize a production price source (handoff warning).
"""
from __future__ import annotations

import os
import subprocess
from datetime import date
from typing import Any

from src.bonds import price_observations
from src.db import LOCK_BOND_PRICE_OBSERVATIONS, advisory_lock, connect, resolve_dsn

# Build stamps a deploy may inject (the container image carries no ``.git``;
# without them every build of one as_of collapses onto a single publication_id
# and materialize only re-points instead of rebuilding).
_REVISION_ENV_VARS = ("CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA")


def _code_revision() -> str:
    for var in _REVISION_ENV_VARS:
        value = os.getenv(var)
        if value:
            return value.strip()
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        stamped = out.stdout.strip()
        if stamped:
            return stamped
    except Exception:
        pass
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
    row = conn.execute("SELECT max(as_of) FROM bond_price_observation").fetchone()
    return row[0] if row else None


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    with connect(resolve_dsn(dsn)) as conn, advisory_lock(conn, LOCK_BOND_PRICE_OBSERVATIONS) as acquired:
        if not acquired:
            return {"state": "locked", "product": price_observations.PRODUCT}
        price_observations.install_schema(conn)
        source = _latest_validated_source(conn)
        if source is None:
            conn.commit()
            return {"state": "no_source", "product": price_observations.PRODUCT}
        source_run_id, source_package_id = source
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_observations", "product": price_observations.PRODUCT}
        result = price_observations.materialize(
            conn, as_of=as_of, source_run_id=source_run_id,
            source_package_id=source_package_id, code_revision=_code_revision(),
        )
        conn.commit()
    return {"state": "ok", **result}
