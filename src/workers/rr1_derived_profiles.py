"""Registered materializer for the RR1 fee-profile snapshot.

Builds and promotes ``rr1_fee_profile_v1`` -- the only surviving RR1 derived
product -- over the amendment-aware effective selection.  It lands one complete
version under an advisory lock and is promoted to its current pointer atomically
via the shared derived-publication protocol.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
Global Constraint 9: this ships without running any production backfill; when no
validated RR1 source run exists the worker is a no-op.
"""
from __future__ import annotations

import subprocess
from datetime import date
from typing import Any

from src import sec_effective_matviews
from src.db import LOCK_RR1_DERIVED_PROFILES, advisory_lock, connect
from src.rr1 import derived_profiles


def _code_revision() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _latest_validated_rr1(conn: Any) -> tuple[Any, Any] | None:
    row = conn.execute(
        "SELECT r.run_id, p.package_id "
        "FROM sec_validated_raw_runs r "
        "JOIN sec_source_packages p ON p.run_id=r.run_id "
        " AND p.source_family='rr1' AND p.package_state='loaded' "
        "WHERE r.source_family='rr1' "
        "ORDER BY r.raw_validated_at DESC LIMIT 1"
    ).fetchone()
    return (row[0], row[1]) if row else None


def _resolve_as_of(conn: Any, calc_date: str | None) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    # The watermark comes from the per-date calendar matview when it is fresh (see
    # src/sec_effective_matviews.py); otherwise from the view itself, which is
    # always correct and merely re-expands the whole raw selection.
    relation = sec_effective_matviews.resolve_relation(conn, "rr1_effective_facts")
    row = conn.execute(f"SELECT max(effective_date) FROM {relation}").fetchone()
    return row[0] if row else None


def _materialize_effective_cache(conn: Any, as_of: date) -> None:
    """Evaluate the amendment-aware RR1 fact view once for the whole build.

    The fee product computes fingerprints, rows, and closure checks over the same
    effective fact set several times. Letting each SQL function expand the view
    again multiplies the full raw scan many times. A transaction-local table
    shadows the public view for this session and is discarded automatically on
    commit/rollback.

    This cache is NOT semantically neutral, and that is the trap it once set.
    The ``effective_date`` and ``tag`` predicates are neutral -- every consumer
    re-applies at least as strict a filter -- but the series/class predicate is
    a *scope* decision: it drops the RR1 facts reported at series (fund) grain,
    with no class dimension.  In production, for as_of 2026-07-01, that is 2748
    of 1803426 fee-tag rows.

    While that predicate lived only here, ``build_rr1_fee_profiles`` and
    ``rr1_fee_profile_build_is_closed`` read two different input sets depending
    on who was connected: the builder pinned a count/fingerprint over the
    filtered set, and the guard recomputed them over the unfiltered view, so a
    perfectly good build could never be certified -- nor its pointer repointed
    -- from any session but the one that built it.  schemas/rr1_fee_profiles.sql
    now states the same scope in the product's own selection, so both readings
    agree and this filter is merely redundant.  It is kept so this worker's
    input set stays byte-identical to the one that produced the published
    snapshots.

    The six other RR1 profile products that used to widen this cache (waiver,
    turnover, shareholder cost, reported performance, class cost dispersion,
    benchmark) were removed on 2026-07-30, together with the series-level tag
    exemption that only the benchmark product needed.
    """
    tags = [
        row[0]
        for row in conn.execute(
            "SELECT original_tag FROM rr1_fee_profile_concept_map()"
        ).fetchall()
    ]
    conn.execute(
        "CREATE TEMP TABLE rr1_effective_facts ON COMMIT PRESERVE ROWS AS "
        "SELECT * FROM public.rr1_effective_facts "
        "WHERE effective_date<=%s AND tag=ANY(%s) "
        "AND nullif(btrim(series_id),'') IS NOT NULL "
        "AND nullif(btrim(class_id),'') IS NOT NULL",
        (as_of, tags),
    )
    conn.execute(
        "CREATE INDEX ON rr1_effective_facts"
        "(source_table,tag,version,effective_date,series_id,class_id)"
    )
    conn.execute("ANALYZE rr1_effective_facts")


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, object]:
    with connect(dsn) as conn, advisory_lock(conn, LOCK_RR1_DERIVED_PROFILES) as acquired:
        if not acquired:
            return {"state": "locked", "products": 0}
        derived_profiles.install_schema(conn)
        source = _latest_validated_rr1(conn)
        if source is None:
            conn.commit()
            return {"state": "no_source", "products": 0}
        source_run_id, source_package_id = source
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_effective_facts", "products": 0}
        _materialize_effective_cache(conn, as_of)
        conn.commit()
        results: list[dict[str, object]] = []
        failures: dict[str, str] = {}
        revision = _code_revision()
        for product in derived_profiles.PRODUCTS:
            try:
                result = derived_profiles.materialize_product(
                    conn,
                    product=product,
                    as_of=as_of,
                    source_run_id=source_run_id,
                    source_package_id=source_package_id,
                    code_revision=revision,
                )
                conn.commit()
                results.append(result)
            except Exception as error:
                conn.rollback()
                failures[product] = f"{type(error).__name__}: {error}".splitlines()[0]
    return {
        "state": "ok" if not failures else "partial",
        "products": len(results),
        "failed_products": failures,
        "as_of": as_of.isoformat(),
        "results": results,
    }
