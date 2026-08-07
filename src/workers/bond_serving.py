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
    """The day this serving snapshot speaks for: the freshest input it carries.

    It used to be the security master's ``max(measured_at)`` alone, and that is
    what made a DAILY refresh structurally impossible (measured 2026-08-07): the
    publication identity is ``uuid5(product | as_of | code_revision)``, the
    master's measured_at sat at 2026-07-23, and ``CODE_REVISION`` only moves on a
    deploy -- so every run of an undeployed day resolved to the SAME
    publication_id, which ``materialize`` treats as already built and merely
    re-points. Fresh prices could land forever and the served payload would never
    change.

    Taking the greatest of the master's date and the dense daily series' last day
    fixes that at the honest end: the snapshot's as_of now follows its data. The
    deliberately rejected alternative was salting the identity with an input
    fingerprint, which would mint a new publication every day while the payload
    still CLAIMED 2026-07-23 -- a fresh price stamped with a stale date is the
    one outcome worse than a stale price.

    The free property: on a day the series does not advance (a weekend), as_of
    does not move either, the identity replays, and the build is a cheap
    re-point instead of a needless 2M-row rewrite.
    """
    if calc_date:
        return date.fromisoformat(calc_date)
    anchors: list[date] = []
    row = conn.execute("SELECT max(measured_at) FROM sec_current_bond_security_v1").fetchone()
    if row and row[0]:
        anchors.append(row[0])
    if conn.execute("SELECT to_regclass('bond_observation_daily')").fetchone()[0]:
        row = conn.execute("SELECT max(day) FROM bond_observation_daily").fetchone()
        if row and row[0]:
            anchors.append(row[0])
    return max(anchors) if anchors else None


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


# --------------------------------------------------------------------------- #
# Retention
# --------------------------------------------------------------------------- #
# A complete serving publication is ~2.0M facts / ~1.2 GB (measured 2026-08-07).
# That was harmless while a rebuild needed a deploy; now that ``as_of`` follows
# the daily series a rebuild happens most days, and unbounded retention would add
# ~1.2 GB/day forever. So the worker prunes what nothing can reach.
#
# The keep-set is a UNION of three, and all three are load-bearing:
#   * the worker's own current pointer -- what the next build compares against;
#   * whatever the APP's current pin references -- the pin advance can honestly
#     fail (``app_protocol_missing``, ``worker_publication_not_validated``) and
#     leave the app on an older publication; deleting that one breaks the
#     product silently, which is the worst failure available here;
#   * the immediately-prior worker publication -- ``daily_chain`` compensation
#     restores the PRE-RUN pointer on a failed run, and restoring a pointer to a
#     publication whose facts were deleted is an empty product.
#
# Rows go in bounded batches with a commit each: one 2M-row DELETE is a long
# transaction, and a long transaction holds back VACUUM for the WHOLE database
# (a trap this repo has already paid for). Note the disk is RE-USED, not
# returned -- bond_serving_facts is a plain table -- which is the point: steady
# state stays around three publications instead of growing without bound.
_KEEP_WORKER_CURRENT = (
    "SELECT publication_id FROM sec_derived_current_pointers "
    "WHERE product = 'bond_serving_v1'"
)
_KEEP_APP_PINNED = (
    "SELECT s.worker_publication_id FROM bond_serving_app_current_pointer p "
    "JOIN bond_serving_publications s ON s.app_publication_id = p.app_publication_id"
)
_KEEP_TWO_MOST_RECENT = (
    "SELECT publication_id FROM (SELECT publication_id FROM sec_derived_publications "
    "WHERE product = 'bond_serving_v1' ORDER BY publication_version DESC LIMIT 2) recent"
)

_PRUNE_BATCH_SQL = """
DELETE FROM bond_serving_facts
WHERE ctid = ANY (ARRAY(
    SELECT ctid FROM bond_serving_facts
    WHERE publication_id = ANY(%s) LIMIT %s
))
"""

PRUNE_BATCH_ROWS = 50_000


def _prune_superseded_facts(dsn: str, *, batch: int = PRUNE_BATCH_ROWS) -> dict[str, Any]:
    """Delete facts of serving publications nothing can reach any more."""
    with connect(dsn) as conn:
        if not conn.execute(
            "SELECT to_regclass('bond_serving_facts') IS NOT NULL"
        ).fetchone()[0]:
            conn.commit()
            return {"pruned_publications": 0, "pruned_rows": 0, "state": "no_facts_table"}
        arms = [_KEEP_WORKER_CURRENT, _KEEP_TWO_MOST_RECENT]
        if conn.execute(
            "SELECT to_regclass('bond_serving_app_current_pointer') IS NOT NULL"
        ).fetchone()[0]:
            arms.append(_KEEP_APP_PINNED)
        # An absent app protocol (unit schemas) drops that ARM; it never widens
        # the delete, because every remaining arm is still a keep.
        keep = [
            row[0]
            for row in conn.execute("\nUNION\n".join(arms)).fetchall()
            if row[0]
        ]
        stale = [
            row[0] for row in conn.execute(
                "SELECT DISTINCT publication_id FROM bond_serving_facts "
                "WHERE NOT (publication_id = ANY(%s))", (keep,)
            ).fetchall()
        ]
        conn.commit()
        if not stale:
            return {"pruned_publications": 0, "pruned_rows": 0, "kept": len(keep)}
        removed = 0
        while True:
            deleted = conn.execute(_PRUNE_BATCH_SQL, (stale, batch)).rowcount
            conn.commit()
            removed += deleted
            if deleted < batch:
                break
    return {"pruned_publications": len(stale), "pruned_rows": removed, "kept": len(keep)}


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
    # Retention runs LAST, on its own connection, outside the serving lock's
    # transaction: it must never be able to roll back a promotion, and it must
    # see the pin this run just advanced (which is what keeps the app's
    # publication in the keep-set).
    try:
        retention = _prune_superseded_facts(resolve_dsn(dsn))
    except Exception as exc:  # reported, never fatal: the publication is current
        retention = {"state": "failed", "error": f"{type(exc).__name__}: {exc}"}
    return {"state": "ok", **result, **pin, "retention": retention}
