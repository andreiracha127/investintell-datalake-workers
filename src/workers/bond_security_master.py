"""Registered materializer for the bond_security_v1 point-in-time security master.

Builds and promotes one complete ``bond_security_v1`` snapshot (securities +
point-in-time aliases) over the immutable ``bond_security_observation`` inputs,
under an advisory lock, through the shared derived-publication protocol
(prepared -> validated -> current pointer).

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
Global Constraint 9: this ships without running any production backfill; when no
validated source run exists, or when no observations are present, the worker is a
no-op.
"""
from __future__ import annotations

import os
import subprocess
from datetime import date
from typing import Any

from src.bonds import security_master
from src.db import LOCK_BOND_SECURITY_MASTER, advisory_lock, connect, resolve_dsn


def _code_revision() -> str:
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


def _current_nport_source(conn: Any) -> tuple[Any, Any, Any] | None:
    """Resolve the raw lineage pinned by the exact current N-PORT publication."""
    row = conn.execute(
        "SELECT c.publication_id,p.source_run_id,p.source_package_id "
        "FROM sec_derived_current_pointers c "
        "JOIN sec_derived_publications p ON p.publication_id=c.publication_id "
        "WHERE c.product='sec_nport_holdings_v2' "
        "AND p.product='sec_nport_holdings_v2' AND p.lifecycle_state='validated' "
        "FOR UPDATE OF c"
    ).fetchone()
    return (row[0], row[1], row[2]) if row else None


def _resolve_as_of(conn: Any, calc_date: str | None) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    row = conn.execute("SELECT max(as_of) FROM bond_security_observation").fetchone()
    return row[0] if row else None


def _load_nport_observations(conn: Any, as_of: date, nport_publication_id: Any) -> int:
    """Append the current N-PORT V2 debt cohort as immutable bond observations.

    The current publication id is captured inside the statement and every
    observation id is deterministic over that immutable publication/holding
    lineage. Missing terms remain NULL; no price, schedule, seniority, or rating
    is inferred from the filing.
    """
    available = conn.execute(
        "SELECT to_regclass('sec_nport_holdings_v2_current') IS NOT NULL"
    ).fetchone()[0]
    if not available:
        return 0
    result = conn.execute(
        """
        WITH current_source AS (
            SELECT h.*, h.publication_id AS nport_publication_id
            FROM sec_nport_holdings_v2_current h
            WHERE h.publication_id=%s
              AND h.report_date<=%s
              AND upper(btrim(coalesce(h.source_typed_projection->>'ASSET_CAT','')))='DBT'
              AND (nullif(btrim(h.cusip),'') IS NOT NULL OR nullif(btrim(h.isin),'') IS NOT NULL)
        ), projected AS (
            SELECT *,
                   source_typed_projection->'DEBT_SECURITY' AS debt,
                   nullif(btrim(source_typed_projection->>'CURRENCY_CODE'),'') AS observed_currency
            FROM current_source
        )
        INSERT INTO bond_security_observation(
            observation_id,as_of,observation_date,source_run_id,
            cusip9_input,isin_input,issuer_name,currency,coupon_type,coupon_rate,
            coupon_schedule,maturity_date,seniority,secured,call_schedule,put_schedule,
            is_144a,day_count,settlement_convention,source_lineage
        )
        SELECT
            md5('nport-v2|'||nport_publication_id::text||'|'||accession_number||'|'||holding_id)::uuid,
            %s,report_date,source_run_id,
            nullif(btrim(cusip),''),nullif(btrim(isin),''),nullif(btrim(issuer_name),''),
            observed_currency,nullif(btrim(debt->>'COUPON_TYPE'),''),
            CASE WHEN debt->>'ANNUALIZED_RATE' ~ '^[+-]?[0-9]+([.][0-9]+)?$'
                 THEN (debt->>'ANNUALIZED_RATE')::numeric END,
            NULL,
            CASE WHEN pg_input_is_valid(debt->>'MATURITY_DATE','date')
                 THEN (debt->>'MATURITY_DATE')::date END,
            NULL,NULL,NULL,NULL,NULL,NULL,NULL,
            jsonb_build_object(
                'source_surface','sec_nport_holdings_v2',
                'publication_id',nport_publication_id,
                'source_run_id',source_run_id,
                'accession_number',accession_number,
                'holding_id',holding_id,
                'report_date',report_date,
                'filing_date',filing_date,
                'evidence_fields',jsonb_build_array(
                    'CUSIP','ISIN','ISSUER_NAME','CURRENCY_CODE',
                    'DEBT_SECURITY.COUPON_TYPE','DEBT_SECURITY.ANNUALIZED_RATE',
                    'DEBT_SECURITY.MATURITY_DATE'
                )
            )
        FROM projected
        ON CONFLICT (observation_id) DO NOTHING
        """,
        (nport_publication_id, as_of, as_of),
    )
    return max(result.rowcount, 0)


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    with connect(resolve_dsn(dsn)) as conn, advisory_lock(conn, LOCK_BOND_SECURITY_MASTER) as acquired:
        if not acquired:
            return {"state": "locked", "product": security_master.PRODUCT}
        security_master.install_schema(conn)
        source = _current_nport_source(conn)
        if source is None:
            conn.commit()
            return {"state": "no_source", "product": security_master.PRODUCT}
        load_as_of = date.fromisoformat(calc_date) if calc_date else date.today()
        nport_publication_id, source_run_id, source_package_id = source
        loaded = _load_nport_observations(conn, load_as_of, nport_publication_id)
        as_of = _resolve_as_of(conn, calc_date)
        if as_of is None:
            conn.commit()
            return {"state": "no_observations", "product": security_master.PRODUCT}
        result = security_master.materialize(
            conn, as_of=as_of, source_run_id=source_run_id,
            source_package_id=source_package_id, code_revision=_code_revision(),
        )
        conn.commit()
    return {"state": "ok", "observations_loaded": loaded, **result}
