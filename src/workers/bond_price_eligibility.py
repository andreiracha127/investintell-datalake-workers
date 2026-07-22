"""Registered installer for the additive bond_price_eligibility_v1 predicate.

Installs (idempotently) the pure ``bond_price_is_eligible`` SQL function and the
``bond_price_eligibility_v1`` classifying view over the immutable
``bond_price_observation`` inputs, under an advisory lock, and reports how many
observations qualify at the latest as_of.

This worker creates NO publication and NO current pointer — it is purely ADDITIVE
and never alters the bond_price_observation_v1 product (Global Constraint: nothing
reaches production; fixtures only).

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
"""
from __future__ import annotations

from datetime import date
from typing import Any

from src.bonds import eligibility
from src.db import LOCK_BOND_PRICE_ELIGIBILITY, advisory_lock, connect, resolve_dsn

PRODUCT = "bond_price_eligibility_v1"


def _resolve_as_of(conn: Any, calc_date: str | None) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    row = conn.execute("SELECT max(as_of) FROM bond_price_observation").fetchone()
    return row[0] if row else None


def install_and_count(conn: Any, *, as_of: date | None) -> dict[str, Any]:
    """Install the predicate and count eligible/total observations at ``as_of``."""
    eligibility.install_schema(conn)
    if as_of is None:
        return {"product": PRODUCT, "as_of": None, "eligible": 0, "observations": 0}
    total, eligible = conn.execute(
        "SELECT count(*), count(*) FILTER (WHERE is_eligible) "
        "FROM bond_price_eligibility_v1 WHERE as_of=%s",
        (as_of,),
    ).fetchone()
    return {"product": PRODUCT, "as_of": as_of.isoformat(), "eligible": int(eligible),
            "observations": int(total)}


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    with connect(resolve_dsn(dsn)) as conn, advisory_lock(conn, LOCK_BOND_PRICE_ELIGIBILITY) as acquired:
        if not acquired:
            return {"state": "locked", "product": PRODUCT}
        eligibility.install_schema(conn)
        as_of = _resolve_as_of(conn, calc_date)
        result = install_and_count(conn, as_of=as_of)
        conn.commit()
    return {"state": "ok", **result}
