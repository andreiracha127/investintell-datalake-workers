"""Transactional checkpoint control plane for restartable derived SEC work."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg

from src.db import connect


ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = ROOT / "schemas" / "sec_derived_work_units.sql"


class DerivedWorkUnitError(RuntimeError):
    """Indicates incompatible, stale, or concurrent work-unit control state."""


@dataclass(frozen=True)
class DerivedWorkUnitStatus:
    """Current checkpoint state plus the validated raw-run provenance it uses."""

    work_unit_id: UUID
    run_id: UUID
    source_family: str
    source_quarter: str
    product: str
    publication_version: int
    unit_key: str
    input_fingerprint: str
    state: str
    attempt_count: int
    lease_token: UUID | None
    started_at: datetime | None
    heartbeat_at: datetime | None
    completed_at: datetime | None
    failure_code: str | None
    failure_detail: str | None
    output_fingerprint: str | None
    evidence: dict[str, Any] | None


_STATUS_COLUMNS = """
    w.work_unit_id, w.run_id, r.source_family, r.source_quarter,
    w.product, w.publication_version, w.unit_key, w.input_fingerprint,
    w.state, w.attempt_count, w.lease_token, w.started_at, w.heartbeat_at,
    w.completed_at, w.failure_code, w.failure_detail, w.output_fingerprint, w.evidence
"""


def _status(row: tuple[Any, ...]) -> DerivedWorkUnitStatus:
    return DerivedWorkUnitStatus(*row)


def _load_status(conn: psycopg.Connection, work_unit_id: UUID, *, lock: bool = False) -> DerivedWorkUnitStatus | None:
    lock_clause = " FOR UPDATE OF w" if lock else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {_STATUS_COLUMNS}
            FROM sec_derived_work_units AS w
            JOIN sec_validated_raw_runs AS r ON r.run_id = w.run_id
            WHERE w.work_unit_id = %s{lock_clause}
            """,
            (work_unit_id,),
        )
        row = cur.fetchone()
    return _status(row) if row is not None else None


def _require_status(conn: psycopg.Connection, work_unit_id: UUID, *, lock: bool = False) -> DerivedWorkUnitStatus:
    status = _load_status(conn, work_unit_id, lock=lock)
    if status is None:
        raise DerivedWorkUnitError("work unit requires a validated raw run")
    return status


def install_schema(conn: psycopg.Connection | None = None, *, dsn: str | None = None) -> None:
    """Install the idempotent checkpoint schema without changing caller ownership."""
    if conn is None:
        with connect(dsn) as owned_conn:
            install_schema(owned_conn)
            owned_conn.commit()
        return
    with conn.cursor() as cur:
        cur.execute(DDL_PATH.read_text(encoding="utf-8"))


def create_or_resume_work_unit(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    product: str,
    publication_version: int,
    unit_key: str,
    input_fingerprint: str,
    work_unit_id: UUID | None = None,
) -> DerivedWorkUnitStatus:
    """Create one deterministic checkpoint, guarded by validated raw provenance."""
    candidate_id = work_unit_id or uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_derived_work_units
                (work_unit_id, run_id, product, publication_version, unit_key, input_fingerprint)
            SELECT %s, r.run_id, %s, %s, %s, %s
            FROM sec_validated_raw_runs AS r
            WHERE r.run_id = %s
            ON CONFLICT (run_id, product, publication_version, unit_key) DO NOTHING
            """,
            (candidate_id, product, publication_version, unit_key, input_fingerprint, run_id),
        )
    status = _require_status_by_identity(
        conn,
        run_id=run_id,
        product=product,
        publication_version=publication_version,
        unit_key=unit_key,
        lock=True,
    )
    if status.input_fingerprint != input_fingerprint:
        raise DerivedWorkUnitError("conflicting input fingerprint for work-unit business identity")
    return status


def _require_status_by_identity(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    product: str,
    publication_version: int,
    unit_key: str,
    lock: bool = False,
) -> DerivedWorkUnitStatus:
    lock_clause = " FOR UPDATE OF w" if lock else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {_STATUS_COLUMNS}
            FROM sec_derived_work_units AS w
            JOIN sec_validated_raw_runs AS r ON r.run_id = w.run_id
            WHERE w.run_id = %s AND w.product = %s AND w.publication_version = %s
              AND w.unit_key = %s{lock_clause}
            """,
            (run_id, product, publication_version, unit_key),
        )
        row = cur.fetchone()
    if row is None:
        raise DerivedWorkUnitError("work unit requires a validated raw run")
    return _status(row)


def claim_work_unit(conn: psycopg.Connection, *, work_unit_id: UUID) -> DerivedWorkUnitStatus:
    """Acquire the sole lease for a pending or failed checkpoint."""
    status = _require_status(conn, work_unit_id, lock=True)
    if status.state not in {"pending", "failed"}:
        raise DerivedWorkUnitError("work unit is not pending or failed")
    lease_token = uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sec_derived_work_units
            SET state = 'running', attempt_count = attempt_count + 1,
                lease_token = %s, started_at = statement_timestamp(),
                heartbeat_at = statement_timestamp(), failure_code = NULL,
                failure_detail = NULL, updated_at = statement_timestamp()
            WHERE work_unit_id = %s AND state IN ('pending', 'failed')
            """,
            (lease_token, work_unit_id),
        )
        if cur.rowcount != 1:
            raise DerivedWorkUnitError("work unit claim lost to another worker")
    return _require_status(conn, work_unit_id)


def heartbeat_work_unit(
    conn: psycopg.Connection, *, work_unit_id: UUID, lease_token: UUID
) -> DerivedWorkUnitStatus:
    """Record liveness only for the current lease holder."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sec_derived_work_units
            SET heartbeat_at = statement_timestamp(), updated_at = statement_timestamp()
            WHERE work_unit_id = %s AND state = 'running' AND lease_token = %s
            """,
            (work_unit_id, lease_token),
        )
        if cur.rowcount != 1:
            raise DerivedWorkUnitError("heartbeat requires the active work-unit lease")
    return _require_status(conn, work_unit_id)


def recover_stale_work_unit(
    conn: psycopg.Connection,
    *,
    work_unit_id: UUID,
    stale_before: datetime,
) -> DerivedWorkUnitStatus:
    """Explicitly replace a lease whose persisted heartbeat predates the caller's cutoff."""
    status = _require_status(conn, work_unit_id, lock=True)
    if (
        status.state != "running"
        or status.heartbeat_at is None
        or status.heartbeat_at >= stale_before
    ):
        raise DerivedWorkUnitError("running work unit is not stale at the supplied cutoff")

    replacement_lease = uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sec_derived_work_units
            SET attempt_count = attempt_count + 1, lease_token = %s,
                started_at = statement_timestamp(), heartbeat_at = statement_timestamp(),
                updated_at = statement_timestamp()
            WHERE work_unit_id = %s AND state = 'running'
              AND lease_token = %s AND heartbeat_at < %s
            """,
            (replacement_lease, work_unit_id, status.lease_token, stale_before),
        )
        if cur.rowcount != 1:
            raise DerivedWorkUnitError("stale work-unit recovery lost to another worker")
    return _require_status(conn, work_unit_id)


def complete_work_unit(
    conn: psycopg.Connection,
    *,
    work_unit_id: UUID,
    lease_token: UUID,
    output_fingerprint: str,
    evidence: dict[str, Any],
) -> DerivedWorkUnitStatus:
    """Atomically close a leased unit; exact replays are safe, conflicts fail closed."""
    if evidence is None:
        raise DerivedWorkUnitError("completion requires evidence")
    status = _require_status(conn, work_unit_id, lock=True)
    if status.state == "completed":
        if status.output_fingerprint == output_fingerprint and status.evidence == evidence:
            return status
        raise DerivedWorkUnitError("conflicting completion evidence or output fingerprint")
    if status.state != "running" or status.lease_token != lease_token:
        raise DerivedWorkUnitError("completion requires the active work-unit lease")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sec_derived_work_units
            SET state = 'completed', lease_token = NULL, started_at = NULL, heartbeat_at = NULL,
                completed_at = statement_timestamp(), output_fingerprint = %s,
                evidence = %s::jsonb, updated_at = statement_timestamp()
            WHERE work_unit_id = %s AND state = 'running' AND lease_token = %s
            """,
            (output_fingerprint, _json(evidence), work_unit_id, lease_token),
        )
        if cur.rowcount != 1:
            raise DerivedWorkUnitError("completion lost the active work-unit lease")
    return _require_status(conn, work_unit_id)


def fail_work_unit(
    conn: psycopg.Connection,
    *,
    work_unit_id: UUID,
    lease_token: UUID,
    failure_code: str,
    failure_detail: str | None = None,
) -> DerivedWorkUnitStatus:
    """Record an auditable failure, releasing the checkpoint for an explicit retry."""
    if not failure_code:
        raise DerivedWorkUnitError("failure requires a failure code")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sec_derived_work_units
            SET state = 'failed', lease_token = NULL, started_at = NULL, heartbeat_at = NULL,
                failure_code = %s, failure_detail = %s, updated_at = statement_timestamp()
            WHERE work_unit_id = %s AND state = 'running' AND lease_token = %s
            """,
            (failure_code, failure_detail, work_unit_id, lease_token),
        )
        if cur.rowcount != 1:
            raise DerivedWorkUnitError("failure requires the active work-unit lease")
    return _require_status(conn, work_unit_id)


def list_resumable_work_units(
    conn: psycopg.Connection, *, run_id: UUID | None = None
) -> list[DerivedWorkUnitStatus]:
    """List only pending or failed checkpoints from validated raw runs."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {_STATUS_COLUMNS}
            FROM sec_derived_work_units AS w
            JOIN sec_validated_raw_runs AS r ON r.run_id = w.run_id
            WHERE w.state IN ('pending', 'failed') AND (%s IS NULL OR w.run_id = %s)
            ORDER BY r.source_family, r.source_quarter, w.product, w.publication_version, w.unit_key
            """,
            (run_id, run_id),
        )
        return [_status(row) for row in cur.fetchall()]


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
