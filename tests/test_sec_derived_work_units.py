"""Transactional contract tests for restartable derived SEC work units."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import timedelta
from urllib.parse import urlparse
from uuid import UUID, uuid4

import pytest

from src.sec_regulatory.derived_work_units import (
    DerivedWorkUnitError,
    claim_work_unit,
    complete_work_unit,
    create_or_resume_work_unit,
    fail_work_unit,
    heartbeat_work_unit,
    install_schema,
    list_resumable_work_units,
    recover_stale_work_unit,
)


_LOCAL_TEST_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _safe_test_dsn() -> str | None:
    """Only an explicitly configured local disposable database is eligible."""
    dsn = os.getenv("SEC_TEST_DATABASE_URL")
    if not dsn:
        return None
    parsed = urlparse(dsn)
    if parsed.hostname not in _LOCAL_TEST_HOSTS:
        raise RuntimeError("SEC_TEST_DATABASE_URL must target a local disposable database")
    return dsn


@pytest.fixture
def work_database() -> tuple[str, str, UUID]:
    dsn = _safe_test_dsn()
    if dsn is None:
        pytest.skip("SEC_TEST_DATABASE_URL ausente")

    import psycopg

    schema = f"derived_work_units_{uuid4().hex}"
    run_id = uuid4()
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute(
                """CREATE TABLE sec_ingestion_runs (
                    run_id uuid PRIMARY KEY,
                    source_family text NOT NULL,
                    source_quarter text NOT NULL,
                    current_state text NOT NULL,
                    raw_validated_at timestamptz
                )"""
            )
            cur.execute(
                """CREATE VIEW sec_validated_raw_runs AS
                    SELECT run_id, source_family, source_quarter, raw_validated_at
                    FROM sec_ingestion_runs
                    WHERE raw_validated_at IS NOT NULL"""
            )
            cur.execute(
                """CREATE TABLE sec_derived_current_pointers (
                    product text PRIMARY KEY,
                    publication_id uuid NOT NULL
                )"""
            )
            install_schema(conn)
            install_schema(conn)
            cur.execute(
                """INSERT INTO sec_ingestion_runs
                    (run_id, source_family, source_quarter, current_state, raw_validated_at)
                   VALUES (%s, 'nport', '2024Q1', 'raw_validated', now())""",
                (run_id,),
            )
    try:
        yield dsn, schema, run_id
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@contextmanager
def _connection(dsn: str, schema: str):
    import psycopg

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
        yield conn


def _create(
    conn,
    run_id: UUID,
    *,
    unit_key: str = "2024Q1",
    input_fingerprint: str = "b" * 64,
):
    return create_or_resume_work_unit(
        conn,
        run_id=run_id,
        product="sec_nport_holdings_v2",
        publication_version=1,
        unit_key=unit_key,
        input_fingerprint=input_fingerprint,
    )


def test_refuses_remote_test_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_TEST_DATABASE_URL", "postgresql://user:pw@db.example.com/test")
    with pytest.raises(RuntimeError, match="local disposable"):
        _safe_test_dsn()


def test_creation_requires_validated_raw_run_and_reads_source_metadata(work_database) -> None:
    dsn, schema, run_id = work_database
    with _connection(dsn, schema) as conn:
        status = _create(conn, run_id)
        repeated = _create(conn, run_id)
        with pytest.raises(DerivedWorkUnitError, match="validated raw"):
            _create(conn, uuid4())
        conn.commit()

    assert repeated.work_unit_id == status.work_unit_id
    assert (status.source_family, status.source_quarter, status.state, status.attempt_count) == (
        "nport", "2024Q1", "pending", 0,
    )


def test_business_identity_refuses_a_conflicting_input_fingerprint(work_database) -> None:
    dsn, schema, run_id = work_database
    with _connection(dsn, schema) as conn:
        original = _create(conn, run_id)
        with pytest.raises(DerivedWorkUnitError, match="input fingerprint"):
            _create(conn, run_id, input_fingerprint="f" * 64)
        with conn.cursor() as cur:
            cur.execute("SELECT work_unit_id, input_fingerprint FROM sec_derived_work_units")
            persisted = cur.fetchall()
        conn.commit()

    assert persisted == [(original.work_unit_id, "b" * 64)]


def test_only_one_claimant_can_hold_a_work_unit(work_database) -> None:
    dsn, schema, run_id = work_database
    with _connection(dsn, schema) as conn:
        unit = _create(conn, run_id)
        conn.commit()

    with _connection(dsn, schema) as first, _connection(dsn, schema) as second:
        claimed = claim_work_unit(first, work_unit_id=unit.work_unit_id)
        first.commit()
        with pytest.raises(DerivedWorkUnitError, match="pending or failed"):
            claim_work_unit(second, work_unit_id=unit.work_unit_id)
        second.rollback()

    assert claimed.state == "running"
    assert claimed.lease_token is not None
    assert claimed.attempt_count == 1


def test_failed_work_resumes_with_a_new_attempt_and_lease(work_database) -> None:
    dsn, schema, run_id = work_database
    with _connection(dsn, schema) as conn:
        unit = _create(conn, run_id)
        claimed = claim_work_unit(conn, work_unit_id=unit.work_unit_id)
        heartbeat = heartbeat_work_unit(conn, work_unit_id=unit.work_unit_id, lease_token=claimed.lease_token)
        failed = fail_work_unit(
            conn,
            work_unit_id=unit.work_unit_id,
            lease_token=claimed.lease_token,
            failure_code="worker_crash",
            failure_detail="synthetic interruption",
        )
        resumed = claim_work_unit(conn, work_unit_id=unit.work_unit_id)
        conn.commit()

    assert heartbeat.heartbeat_at is not None
    assert (failed.state, failed.failure_code, failed.attempt_count) == ("failed", "worker_crash", 1)
    assert resumed.state == "running"
    assert resumed.attempt_count == 2
    assert resumed.lease_token != claimed.lease_token


def test_explicit_stale_recovery_rotates_lease_once_and_invalidates_old_owner(work_database) -> None:
    dsn, schema, run_id = work_database
    with _connection(dsn, schema) as conn:
        unit = _create(conn, run_id)
        claimed = claim_work_unit(conn, work_unit_id=unit.work_unit_id)
        assert list_resumable_work_units(conn, run_id=run_id) == []
        conn.commit()

    stale_before = claimed.heartbeat_at + timedelta(microseconds=1)
    with _connection(dsn, schema) as first:
        recovered = recover_stale_work_unit(
            first,
            work_unit_id=unit.work_unit_id,
            stale_before=stale_before,
        )
        first.commit()

    with _connection(dsn, schema) as second:
        with pytest.raises(DerivedWorkUnitError, match="not stale"):
            recover_stale_work_unit(
                second,
                work_unit_id=unit.work_unit_id,
                stale_before=stale_before,
            )
        with pytest.raises(DerivedWorkUnitError, match="active work-unit lease"):
            heartbeat_work_unit(
                second,
                work_unit_id=unit.work_unit_id,
                lease_token=claimed.lease_token,
            )
        with pytest.raises(DerivedWorkUnitError, match="active work-unit lease"):
            complete_work_unit(
                second,
                work_unit_id=unit.work_unit_id,
                lease_token=claimed.lease_token,
                output_fingerprint="7" * 64,
                evidence={"rows": 1},
            )
        current = heartbeat_work_unit(
            second,
            work_unit_id=unit.work_unit_id,
            lease_token=recovered.lease_token,
        )
        second.commit()

    assert recovered.state == "running"
    assert recovered.attempt_count == 2
    assert recovered.lease_token != claimed.lease_token
    assert current.lease_token == recovered.lease_token


def test_stale_recovery_uses_statement_time_when_recoverer_transaction_started_first(work_database) -> None:
    dsn, schema, run_id = work_database
    with _connection(dsn, schema) as setup:
        unit = _create(setup, run_id, unit_key="old-transaction-recovery")
        setup.commit()

    with _connection(dsn, schema) as old_transaction, _connection(dsn, schema) as claimant:
        with old_transaction.cursor() as cur:
            cur.execute("SELECT now()")
            old_transaction_started_at = cur.fetchone()[0]

        claimed = claim_work_unit(claimant, work_unit_id=unit.work_unit_id)
        claimant.commit()
        assert old_transaction_started_at < claimed.started_at

        recovered = recover_stale_work_unit(
            old_transaction,
            work_unit_id=unit.work_unit_id,
            stale_before=claimed.heartbeat_at + timedelta(microseconds=1),
        )
        old_transaction.commit()

    assert recovered.lease_token != claimed.lease_token
    assert recovered.started_at >= claimed.started_at
    assert recovered.heartbeat_at >= claimed.heartbeat_at


def test_completion_is_idempotent_only_for_identical_evidence(work_database) -> None:
    dsn, schema, run_id = work_database
    with _connection(dsn, schema) as conn:
        unit = _create(conn, run_id)
        claimed = claim_work_unit(conn, work_unit_id=unit.work_unit_id)
        completed = complete_work_unit(
            conn,
            work_unit_id=unit.work_unit_id,
            lease_token=claimed.lease_token,
            output_fingerprint="c" * 64,
            evidence={"rows": 1, "checkpoint": "synthetic"},
        )
        repeated = complete_work_unit(
            conn,
            work_unit_id=unit.work_unit_id,
            lease_token=claimed.lease_token,
            output_fingerprint="c" * 64,
            evidence={"checkpoint": "synthetic", "rows": 1},
        )
        with pytest.raises(DerivedWorkUnitError, match="conflicting completion"):
            complete_work_unit(
                conn,
                work_unit_id=unit.work_unit_id,
                lease_token=claimed.lease_token,
                output_fingerprint="d" * 64,
                evidence={"rows": 2},
            )
        conn.commit()

    assert completed == repeated
    assert completed.state == "completed"
    assert completed.evidence == {"rows": 1, "checkpoint": "synthetic"}


def test_direct_sql_cannot_mutate_identity_or_terminal_evidence(work_database) -> None:
    import psycopg

    dsn, schema, run_id = work_database
    with _connection(dsn, schema) as conn:
        pending = _create(conn, run_id, unit_key="immutable-identity")
        with pytest.raises(psycopg.errors.RaiseException, match="lifecycle"):
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE sec_derived_work_units SET unit_key='rewritten' WHERE work_unit_id=%s",
                        (pending.work_unit_id,),
                    )

        terminal = _create(conn, run_id, unit_key="immutable-evidence")
        claimed = claim_work_unit(conn, work_unit_id=terminal.work_unit_id)
        completed = complete_work_unit(
            conn,
            work_unit_id=terminal.work_unit_id,
            lease_token=claimed.lease_token,
            output_fingerprint="9" * 64,
            evidence={"rows": 7},
        )
        with pytest.raises(psycopg.errors.RaiseException, match="lifecycle"):
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE sec_derived_work_units SET evidence='{\"rows\":8}' WHERE work_unit_id=%s",
                        (terminal.work_unit_id,),
                    )
        with pytest.raises(psycopg.errors.RaiseException, match="lifecycle"):
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM sec_derived_work_units WHERE work_unit_id=%s",
                        (terminal.work_unit_id,),
                    )
        conn.commit()

    assert completed.evidence == {"rows": 7}


def test_resumable_listing_excludes_completed_and_completion_mutates_no_raw_or_pointer(work_database) -> None:
    dsn, schema, run_id = work_database
    with _connection(dsn, schema) as conn:
        pending = _create(conn, run_id, unit_key="pending")
        complete = _create(conn, run_id, unit_key="complete")
        claimed = claim_work_unit(conn, work_unit_id=complete.work_unit_id)
        with conn.cursor() as cur:
            cur.execute("SELECT current_state, raw_validated_at FROM sec_ingestion_runs WHERE run_id=%s", (run_id,))
            raw_before = cur.fetchone()
            cur.execute("SELECT count(*) FROM sec_derived_current_pointers")
            pointer_count_before = cur.fetchone()[0]
        complete_work_unit(
            conn,
            work_unit_id=complete.work_unit_id,
            lease_token=claimed.lease_token,
            output_fingerprint="e" * 64,
            evidence={"rows": 0},
        )
        resumable = list_resumable_work_units(conn, run_id=run_id)
        with conn.cursor() as cur:
            cur.execute("SELECT current_state, raw_validated_at FROM sec_ingestion_runs WHERE run_id=%s", (run_id,))
            raw_after = cur.fetchone()
            cur.execute("SELECT count(*) FROM sec_derived_current_pointers")
            pointer_count_after = cur.fetchone()[0]
        conn.commit()

    assert [item.work_unit_id for item in resumable] == [pending.work_unit_id]
    assert raw_after == raw_before
    assert pointer_count_after == pointer_count_before == 0
