"""Cache layer over the SEC "effective" selection views.

``ncen_effective_filings`` and ``rr1_effective_facts`` are plain views: a self-join
of the raw landing table plus a ``dense_rank()``/``count(*) OVER`` window with no
date predicate.  Reading either one expands the whole history of
``ncen_raw_v2_rows`` / ``rr1_raw_v2_rows``.  The daily publication chain reads them
only for ``max(effective_date)`` and does it twice per run
(``discover_source_days`` and ``build_watermarks``), so the chain pays four full
expansions a day to learn two dates.

This module puts a matview in front of each read (DDL: ``schemas/
sec_effective_matviews.sql``) under one rule:

    the view is the authority, the matview is only a cache.

A matview is used ONLY while the source signature recorded at its last refresh
still equals the live signature of the family's validated-run surface.  Anything
else -- matview absent, never populated, signature moved, state row missing --
resolves back to the view.  A missed or forbidden refresh therefore costs the old
full scan; it can never produce a stale watermark.

The signature is ``(count(*), max(raw_validated_at))`` over
``sec_validated_raw_runs`` for the family.  Both effective views admit a raw row
only through a join to that relation, so a family whose validated-run surface is
unchanged cannot change the views' content by landing rows.

Nothing here installs DDL: in production the runtime role does not own the raw
views (2026-07-24 logged ``ERROR: must be owner of view
ncen_effective_filing_candidates`` from a worker-side ``CREATE OR REPLACE``).  The
migration is applied by an operator; this module probes and refreshes.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from src.db import LOCK_SEC_EFFECTIVE_MATVIEWS, advisory_lock, connect

LOGGER = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = ROOT / "schemas" / "sec_effective_matviews.sql"

STATE_TABLE = "sec_effective_matview_state"


@dataclass(frozen=True)
class EffectiveMatview:
    """One cached read path.

    ``view`` is the authority the matview mirrors; ``name`` is the matview;
    ``source_family`` selects the validated-run surface whose signature decides
    freshness; ``watermark_column`` is the column both relations expose so a
    caller can swap the relation name and keep its query.
    """

    name: str
    view: str
    source_family: str
    watermark_column: str


REGISTRY: tuple[EffectiveMatview, ...] = (
    EffectiveMatview(
        name="ncen_effective_filings_mv",
        view="ncen_effective_filings",
        source_family="ncen",
        watermark_column="effective_date",
    ),
    # A per-date roll-up, not a mirror: see the DDL for why rr1_effective_facts is
    # deliberately not duplicated. ``effective_date`` is the shared column, so a
    # max(effective_date) read is relation-swappable.
    EffectiveMatview(
        name="rr1_effective_fact_calendar_mv",
        view="rr1_effective_facts",
        source_family="rr1",
        watermark_column="effective_date",
    ),
)

_BY_VIEW = {entry.view: entry for entry in REGISTRY}


def _relation_exists(conn: Any, relation: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,)).fetchone()
    return bool(row and row[0])


def _is_populated(conn: Any, matview: str) -> bool:
    """True when the matview exists AND carries data.

    ``CREATE MATERIALIZED VIEW ... WITH NO DATA`` leaves a relation that raises on
    every SELECT until the first refresh, so existence alone is not enough.
    """
    row = conn.execute(
        "SELECT c.relispopulated FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relkind = 'm' AND c.relname = %s "
        "  AND n.nspname = ANY (current_schemas(true)) "
        "ORDER BY array_position(current_schemas(true), n.nspname) LIMIT 1",
        (matview,),
    ).fetchone()
    return bool(row and row[0])


def source_signature(conn: Any, family: str) -> tuple[int, Any]:
    """``(validated run count, max raw_validated_at)`` for one source family.

    Cheap by construction: ``sec_validated_raw_runs`` has one row per ingest run,
    not per raw row.  Returns ``(0, None)`` when the relation is absent so a
    database without the SEC ingestion surface degrades to "no signature".
    """
    if not _relation_exists(conn, "sec_validated_raw_runs"):
        return (0, None)
    row = conn.execute(
        "SELECT count(*)::bigint, max(raw_validated_at) "
        "FROM sec_validated_raw_runs WHERE source_family = %s",
        (family,),
    ).fetchone()
    return (int(row[0]), row[1]) if row else (0, None)


def _recorded_signature(conn: Any, matview: str) -> tuple[int, Any] | None:
    if not _relation_exists(conn, STATE_TABLE):
        return None
    row = conn.execute(
        f"SELECT source_run_count, source_validated_at FROM {STATE_TABLE} WHERE matview = %s",
        (matview,),
    ).fetchone()
    return (int(row[0]), row[1]) if row else None


def is_fresh(conn: Any, entry: EffectiveMatview) -> bool:
    """True when ``entry``'s matview may be read instead of its view."""
    if not _is_populated(conn, entry.name):
        return False
    recorded = _recorded_signature(conn, entry.name)
    if recorded is None:
        return False
    return recorded == source_signature(conn, entry.source_family)


def resolve_relation(conn: Any, view: str) -> str:
    """The relation to read for ``view``: its matview when fresh, else the view.

    Callers keep their own query (both relations expose ``watermark_column``);
    only the relation name changes.  Any doubt resolves to the view, which is
    always correct and merely slower.
    """
    entry = _BY_VIEW.get(view)
    if entry is None:
        return view
    try:
        return entry.name if is_fresh(conn, entry) else view
    except Exception as error:  # pragma: no cover - defensive: never break a read
        LOGGER.warning("effective matview probe failed for %s: %s", view, error)
        return view


def _refresh_one(conn: Any, entry: EffectiveMatview) -> dict[str, Any]:
    """Refresh one matview and record the signature it now reflects.

    The signature is read BEFORE the refresh on purpose: a run that lands while
    the refresh is in flight must leave the recorded signature behind the live
    one (=> the matview reads as stale and the next pass refreshes again), never
    ahead of it (=> a stale matview trusted as fresh).
    """
    run_count, validated_at = source_signature(conn, entry.source_family)
    populated = _is_populated(conn, entry.name)
    started = time.monotonic()
    # CONCURRENTLY needs an already-populated matview and a UNIQUE index (both in
    # the DDL); the very first refresh must be the plain form.
    concurrently = "CONCURRENTLY " if populated else ""
    conn.execute(f"REFRESH MATERIALIZED VIEW {concurrently}{entry.name}")
    elapsed = time.monotonic() - started
    row_count = int(conn.execute(f"SELECT count(*) FROM {entry.name}").fetchone()[0])
    conn.execute(
        f"INSERT INTO {STATE_TABLE}"
        "(matview, source_family, source_run_count, source_validated_at, row_count,"
        " refreshed_at, refresh_seconds) "
        "VALUES (%s,%s,%s,%s,%s, now(), %s) "
        "ON CONFLICT (matview) DO UPDATE SET "
        "  source_family = EXCLUDED.source_family,"
        "  source_run_count = EXCLUDED.source_run_count,"
        "  source_validated_at = EXCLUDED.source_validated_at,"
        "  row_count = EXCLUDED.row_count,"
        "  refreshed_at = EXCLUDED.refreshed_at,"
        "  refresh_seconds = EXCLUDED.refresh_seconds",
        (entry.name, entry.source_family, run_count, validated_at, row_count, elapsed),
    )
    return {
        "matview": entry.name,
        "state": "refreshed",
        "concurrently": bool(concurrently),
        "rows": row_count,
        "seconds": round(elapsed, 3),
    }


def refresh_stale(
    dsn: str,
    *,
    force: bool = False,
    only: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Refresh every registered matview whose source signature moved.

    Opens its OWN autocommit connection: ``REFRESH ... CONCURRENTLY`` cannot run
    inside a transaction block, and callers (the chain, the matview worker) hold
    their own transactional connection.

    Every outcome is REPORTED, never raised: a matview is a cache, and a database
    where the migration has not been applied, or where this role may not refresh,
    must keep working at the old cost.  ``force`` refreshes regardless of the
    signature (an operator's re-proof path).
    """
    wanted = set(only) if only is not None else None
    outcomes: list[dict[str, Any]] = []
    with connect(dsn, autocommit=True) as conn:
        with advisory_lock(conn, LOCK_SEC_EFFECTIVE_MATVIEWS) as acquired:
            if not acquired:
                return [{"state": "lock_busy"}]
            if not _relation_exists(conn, STATE_TABLE):
                return [{"state": "not_installed", "detail": STATE_TABLE}]
            for entry in REGISTRY:
                if wanted is not None and entry.name not in wanted:
                    continue
                try:
                    if not _relation_exists(conn, entry.name):
                        outcomes.append({"matview": entry.name, "state": "absent"})
                        continue
                    if not force and is_fresh(conn, entry):
                        outcomes.append({"matview": entry.name, "state": "fresh"})
                        continue
                    outcomes.append(_refresh_one(conn, entry))
                except Exception as error:  # pragma: no cover - reported, not raised
                    LOGGER.warning("effective matview refresh failed for %s: %s", entry.name, error)
                    outcomes.append({
                        "matview": entry.name,
                        "state": "refresh_failed",
                        "error": f"{type(error).__name__}: {error}".splitlines()[0],
                    })
    return outcomes
