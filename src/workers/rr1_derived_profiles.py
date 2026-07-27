"""Registered materializer for the RR1 derived-profile snapshots.

Builds and promotes the fee-profile, shareholder-cost, waiver-durability, and
class cost-dispersion snapshots over the amendment-aware effective selection.
Each product lands one complete version under an advisory lock and is promoted to
its current pointer atomically via the shared derived-publication protocol.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
Global Constraint 9: this ships without running any production backfill; when no
validated RR1 source run exists the worker is a no-op.
"""
from __future__ import annotations

import subprocess
from datetime import date
from typing import Any

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
    row = conn.execute("SELECT max(effective_date) FROM rr1_effective_facts").fetchone()
    return row[0] if row else None


def _materialize_effective_cache(conn: Any, as_of: date) -> None:
    """Evaluate the amendment-aware RR1 fact view once for the whole build.

    Every RR1 product computes fingerprints, rows, and closure checks over the
    same effective fact set. Letting each SQL function expand the view again
    multiplies the full raw scan many times. A transaction-local table shadows
    the public view for this session, preserves identical row semantics, and is
    discarded automatically on commit/rollback.
    """
    tags = [
        row[0]
        for row in conn.execute(
            "SELECT original_tag FROM rr1_fee_profile_concept_map()"
            " UNION SELECT original_tag FROM rr1_shareholder_cost_concept_map()"
            " UNION SELECT original_tag FROM rr1_waiver_concept_map()"
            " UNION SELECT original_tag FROM rr1_turnover_concept_map()"
            " UNION SELECT original_tag FROM rr1_reported_performance_concept_map()"
            " UNION SELECT original_tag FROM rr1_benchmark_concept_map()"
            " UNION SELECT 'AvgAnnlRtrPct'"
            " UNION SELECT 'NetExpensesOverAssets'"
        ).fetchall()
    ]
    # A declared benchmark is a property of the SERIES, so its facts carry an EMPTY
    # class.  Requiring a class for every cached fact would starve the benchmark
    # product of its entire input.  The exemption is strictly ADDITIVE: no other
    # concept map resolves these tags, so no other product's input changes.
    series_level_tags = [
        row[0]
        for row in conn.execute("SELECT original_tag FROM rr1_benchmark_concept_map()").fetchall()
    ]
    conn.execute(
        "CREATE TEMP TABLE rr1_effective_facts ON COMMIT PRESERVE ROWS AS "
        "SELECT * FROM public.rr1_effective_facts "
        "WHERE effective_date<=%s AND tag=ANY(%s) "
        "AND nullif(btrim(series_id),'') IS NOT NULL "
        "AND (nullif(btrim(class_id),'') IS NOT NULL OR tag=ANY(%s))",
        (as_of, tags, series_level_tags),
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
