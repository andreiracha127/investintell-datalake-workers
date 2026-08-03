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
any production price/holdings source (the 144A pricing pilot authorizes none). When
no current bond snapshot / validated source anchor exists the worker is a no-op.
"""
from __future__ import annotations

import os
import subprocess
from datetime import date
from typing import Any

from src.bonds import serving_materializer as materializer
from src.db import LOCK_BOND_SERVING, advisory_lock, connect, resolve_dsn


def _code_revision() -> str:
    """The revision the publication identity is derived from.

    ``CODE_REVISION`` first, exactly like ``bond_security_master`` and
    ``mixed_quant_publication``: the deployed image carries no ``.git``, so the
    git fallback returns "unknown" there and every build of a given ``as_of``
    collapses onto ONE ``publication_id``. ``materialize`` treats an existing id
    as already built and only re-points, so a code change would silently re-serve
    the previous payload instead of rebuilding -- which is exactly what a Wave 1b
    republication hit on 2026-07-30. The dl-bond-chain job already sets the env
    var; this makes the worker honour it.
    """
    configured = os.getenv("CODE_REVISION")
    if configured:
        return configured
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


def _advance_app_pin(conn: Any, worker_publication_id: str) -> dict[str, Any]:
    """Advance the app-side serving pin to the just-validated worker publication.

    The app reads through ``bond_serving_publications`` (its own prepared ->
    validated -> current protocol) and pins an EXACT worker publication_id, so a
    fresh serving build is invisible until this pin advances. Doing it here —
    only AFTER the worker publication is validated and current — removes the
    manual re-pin step without weakening the guarantee the pin exists for (the
    app never reads a half-built serving).

    Honest no-ops: the app protocol absent (``app_protocol_missing``, e.g. unit
    schemas that never install the app DDL) or the publication already pinned
    (``already_pinned``). A failure to advance raises — the operator must see
    it, because the app would silently keep serving the previous payload.
    """
    if not conn.execute(
        "SELECT to_regclass('bond_serving_publications') IS NOT NULL"
    ).fetchone()[0]:
        return {"app_pin": "app_protocol_missing"}
    worker = conn.execute(
        "SELECT publication_id, publication_version FROM sec_derived_publications "
        "WHERE publication_id=%s AND lifecycle_state='validated'",
        (worker_publication_id,),
    ).fetchone()
    if worker is None:
        return {"app_pin": "worker_publication_not_validated"}
    already = conn.execute(
        "SELECT app_publication_id FROM bond_serving_publications "
        "WHERE worker_publication_id=%s AND lifecycle_state='validated'",
        (worker_publication_id,),
    ).fetchone()
    if already is not None:
        return {"app_pin": "already_pinned", "app_publication_id": str(already[0])}
    app_id, next_version = conn.execute(
        "SELECT gen_random_uuid(), COALESCE(max(app_publication_version),0)+1 "
        "FROM bond_serving_publications"
    ).fetchone()
    conn.execute(
        "INSERT INTO bond_serving_publications "
        "(app_publication_id, app_publication_version, worker_publication_id, "
        " worker_publication_version, lifecycle_state) "
        "VALUES (%s,%s,%s,%s,'prepared')",
        (app_id, next_version, worker[0], worker[1]),
    )
    conn.execute("SELECT bond_validate_serving_publication(%s)", (app_id,))
    conn.execute("SELECT bond_set_current_serving_publication(%s)", (app_id,))
    return {
        "app_pin": "advanced",
        "app_publication_id": str(app_id),
        "app_publication_version": next_version,
    }


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
        except (
            materializer.BondFundExposureMultiplicationError,
            materializer.BondServingSurfaceCoverageError,
        ):
            # Integrity/coverage violations are ACTIONABLE failures (spec §5: never a
            # silent success). Roll back the partial build but let the typed error
            # PROPAGATE -- it must never be laundered into the empty-source dark state
            # below, which is indistinguishable from a genuinely absent source.
            conn.rollback()
            raise
        except RuntimeError:
            # No validated source run/package anchor yet -> dark until backfill.
            conn.rollback()
            return {"state": "no_source", "rows": 0}
        conn.commit()
        # Worker publication validated + current: advance the app pin in its own
        # transaction (a pin failure must not roll back the worker publication).
        pin = _advance_app_pin(conn, result["publication_id"])
        conn.commit()
    return {"state": "ok", **result, **pin}
