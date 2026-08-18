"""Couple a current N-PORT V2 publication to its V2-dependent serving build.

This is deliberately a *post-publication* worker.  ``nport_ingestion`` writes
the legacy landing tables and does not create or promote ``sec_nport_holdings_v2``
publications, while ``nport_lookthrough`` still reads that legacy surface.  The
trigger boundary here is therefore the already validated current V2 pointer.

Once that pointer exists, the operator-owned identity materialized view is
refreshed (plain on first population, concurrent thereafter), then proven fresh
against the same pointer.  Only that proof permits the V2-dependent fixed-income
serving worker to run.  Every completed stage is returned if a later stage
fails, so an interrupted one-shot run is observable and safe to rerun.
"""
from __future__ import annotations

from typing import Any, Callable

from src.db import LOCK_NPORT_V2_PUBLICATION_CHAIN, advisory_lock, connect
from src.workers import nport_fixed_income_serving, nport_holdings_identity_freshness


SOURCE_PRODUCT = "sec_nport_holdings_v2"
IDENTITY_MATVIEW = "nport_holdings_snapshot_identity_v1"
_DOWNSTREAM_SUCCESS_STATES = frozenset({"published", "already_published", "already_validated"})


def _current_v2_publication(conn: Any) -> tuple[str, str] | None:
    """Return the validated publication currently selected by the canonical pointer."""
    row = conn.execute(
        "SELECT p.publication_id::text, p.source_run_id::text "
        "FROM sec_derived_current_pointers c "
        "JOIN sec_derived_publications p ON p.publication_id = c.publication_id "
        "WHERE c.product = %s AND p.product = %s AND p.lifecycle_state = 'validated'",
        (SOURCE_PRODUCT, SOURCE_PRODUCT),
    ).fetchone()
    return (str(row[0]), str(row[1])) if row else None


def _refresh_identity_matview(dsn: str) -> dict[str, Any]:
    """Refresh the identity matview, using a plain bootstrap refresh when needed."""
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT relispopulated FROM pg_class WHERE oid = to_regclass(%s)",
                (f"public.{IDENTITY_MATVIEW}",),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(f"required materialized view public.{IDENTITY_MATVIEW} is missing")
            bootstrap = not bool(row[0])
            concurrently = "" if bootstrap else "CONCURRENTLY "
            cur.execute(f"REFRESH MATERIALIZED VIEW {concurrently}public.{IDENTITY_MATVIEW}")
    return {"name": IDENTITY_MATVIEW, "refreshed": True, "bootstrap": bootstrap}


def _probe_identity(dsn: str) -> dict[str, Any]:
    """Return the structured identity verdict without treating non-fresh as success."""
    with connect(dsn, autocommit=True) as conn:
        return nport_holdings_identity_freshness.probe(conn)


def _blocked(
    *,
    source_publication_id: str | None,
    source_run_id: str | None,
    stages: list[dict[str, Any]],
    reason: str,
    **details: Any,
) -> dict[str, Any]:
    """Make an incomplete chain observable and non-green to the dispatcher."""
    return {
        "published": False,
        "aborted": True,
        "source_publication_id": source_publication_id,
        "source_run_id": source_run_id,
        "stages": stages,
        "blocked_dependency": reason,
        **details,
    }


def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
    current_publication: Callable[[Any], tuple[str, str] | None] = _current_v2_publication,
    identity_refresher: Callable[[str], dict[str, Any]] = _refresh_identity_matview,
    identity_probe: Callable[[str], dict[str, Any]] = _probe_identity,
    downstream_runner: Callable[..., dict[str, Any]] = nport_fixed_income_serving.run,
) -> dict[str, Any]:
    """Refresh and prove identity before running the V2 fixed-income publication.

    ``calc_date`` and ``limit`` are passed through only to the existing downstream
    worker for dispatcher compatibility.  They never select a different source:
    the source is always the canonical current V2 pointer.
    """
    with connect(dsn) as guard:
        with advisory_lock(guard, LOCK_NPORT_V2_PUBLICATION_CHAIN) as acquired:
            if not acquired:
                return {
                    "published": False,
                    "stages": [],
                    "skipped": "lock_busy",
                    "blocked_dependency": "nport_v2_publication_chain",
                }

            source = current_publication(guard)
            if source is None:
                return _blocked(
                    source_publication_id=None,
                    source_run_id=None,
                    stages=[],
                    reason="no_current_validated_v2_publication",
                )
            source_publication_id, source_run_id = source
            stages: list[dict[str, Any]] = [
                {
                    "name": SOURCE_PRODUCT,
                    "publication_id": source_publication_id,
                    "source_run_id": source_run_id,
                    "state": "validated_current",
                }
            ]

            try:
                refresh = identity_refresher(dsn)
            except Exception as exc:
                return _blocked(
                    source_publication_id=source_publication_id,
                    source_run_id=source_run_id,
                    stages=stages,
                    reason="identity_refresh_failed",
                    error=str(exc),
                )
            stages.append({"name": IDENTITY_MATVIEW, **refresh})

            try:
                verdict = identity_probe(dsn)
            except Exception as exc:
                return _blocked(
                    source_publication_id=source_publication_id,
                    source_run_id=source_run_id,
                    stages=stages,
                    reason="identity_probe_failed",
                    error=str(exc),
                )
            stages.append({"name": "nport_holdings_identity_freshness", "verdict": verdict})

            if verdict.get("state") != "fresh":
                return _blocked(
                    source_publication_id=source_publication_id,
                    source_run_id=source_run_id,
                    stages=stages,
                    reason="identity_not_fresh",
                    identity_verdict=verdict,
                )
            if str(verdict.get("publication_id")) != source_publication_id:
                return _blocked(
                    source_publication_id=source_publication_id,
                    source_run_id=source_run_id,
                    stages=stages,
                    reason="identity_publication_mismatch",
                    identity_verdict=verdict,
                )

            # The V2 publisher is external to this worker, so it cannot share
            # this chain's advisory lock. Re-read its canonical pointer after
            # the refresh/probe boundary; do not run a dependent build for a
            # source whose identity proof has already been superseded.
            if current_publication(guard) != source:
                return _blocked(
                    source_publication_id=source_publication_id,
                    source_run_id=source_run_id,
                    stages=stages,
                    reason="current_v2_pointer_changed_before_downstream",
                    identity_verdict=verdict,
                )

            try:
                downstream = downstream_runner(dsn, calc_date=calc_date, limit=limit)
            except Exception as exc:
                return _blocked(
                    source_publication_id=source_publication_id,
                    source_run_id=source_run_id,
                    stages=stages,
                    reason="fixed_income_serving_failed",
                    error=str(exc),
                )
            stages.append({"name": "nport_fixed_income_serving", "stats": downstream})
            if downstream.get("state") not in _DOWNSTREAM_SUCCESS_STATES:
                return _blocked(
                    source_publication_id=source_publication_id,
                    source_run_id=source_run_id,
                    stages=stages,
                    reason="fixed_income_serving_not_published",
                    downstream=downstream,
                )
            if current_publication(guard) != source:
                return _blocked(
                    source_publication_id=source_publication_id,
                    source_run_id=source_run_id,
                    stages=stages,
                    reason="current_v2_pointer_changed_after_downstream",
                    identity_verdict=verdict,
                    downstream=downstream,
                )

            return {
                "published": True,
                "source_publication_id": source_publication_id,
                "source_run_id": source_run_id,
                "stages": stages,
                "downstream": downstream,
            }
