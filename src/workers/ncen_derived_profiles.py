"""Registered materializer for the N-CEN derived-profile snapshots.

Builds and promotes the structure/reliance, provider-network, and
operational-event snapshots over the amendment-aware effective selection.  Each
product lands one complete version under an advisory lock and is promoted to its
current pointer atomically via the shared derived-publication protocol.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
Global Constraint 9: this ships without running any production backfill; when no
validated N-CEN source run exists the worker is a no-op.
"""
from __future__ import annotations

import subprocess
from datetime import date
from typing import Any

from src.db import LOCK_NCEN_DERIVED_PROFILES, advisory_lock, connect
from src.ncen import derived_profiles


def _code_revision() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _latest_validated_ncen(conn: Any) -> tuple[Any, Any] | None:
    row = conn.execute(
        "SELECT r.run_id, p.package_id "
        "FROM sec_validated_raw_runs r "
        "JOIN sec_source_packages p ON p.run_id=r.run_id "
        " AND p.source_family='ncen' AND p.package_state='loaded' "
        "WHERE r.source_family='ncen' "
        "ORDER BY r.raw_validated_at DESC LIMIT 1"
    ).fetchone()
    return (row[0], row[1]) if row else None


def _resolve_as_of(conn: Any, calc_date: str | None) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    row = conn.execute("SELECT max(effective_date) FROM ncen_effective_filings").fetchone()
    return row[0] if row else None


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, object]:
    with connect(dsn) as conn, advisory_lock(conn, LOCK_NCEN_DERIVED_PROFILES) as acquired:
        if not acquired:
            return {"state": "locked", "products": 0}
        derived_profiles.install_schema(conn)
        source = _latest_validated_ncen(conn)
        if source is None:
            conn.commit()
            return {"state": "no_source", "products": 0}
        source_run_id, source_package_id = source
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_effective_filings", "products": 0}
        conn.execute(
            """
            CREATE TEMP TABLE ncen_effective_filings ON COMMIT PRESERVE ROWS AS
            SELECT e.*
            FROM public.ncen_effective_filings e
            WHERE EXISTS (
                SELECT 1 FROM ncen_raw_v2_rows f
                WHERE f.ingestion_run_id=e.ingestion_run_id
                  AND f.accession_number=e.accession_number
                  AND f.source_table='FUND_REPORTED_INFO.tsv'
                  AND f.parse_status='typed'
                  AND nullif(btrim(f.fund_id),'') IS NOT NULL
            )
              AND NOT EXISTS (
                SELECT 1 FROM ncen_raw_v2_rows f
                WHERE f.ingestion_run_id=e.ingestion_run_id
                  AND f.accession_number=e.accession_number
                  AND f.source_table='FUND_REPORTED_INFO.tsv'
                  AND f.parse_status='typed'
                  AND nullif(btrim(f.fund_id),'') IS NULL
            )
              AND NOT EXISTS (
                SELECT 1
                FROM ncen_raw_v2_rows f
                WHERE f.ingestion_run_id=e.ingestion_run_id
                  AND f.accession_number=e.accession_number
                  AND f.source_table='FUND_REPORTED_INFO.tsv'
                  AND f.parse_status='typed'
                GROUP BY f.fund_id
                HAVING count(*)<>1
            )
            """
        )
        conn.execute(
            "CREATE INDEX ON ncen_effective_filings(ingestion_run_id,accession_number,effective_date)"
        )
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
