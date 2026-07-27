"""Release semantics of ``src.db.advisory_lock`` (incident 2026-07-24).

The chain runs recorded ``current transaction is aborted, commands ignored until
end of transaction block`` as the *terminal cause* of their ``materialize``
stage. That string is not a cause — it is the mask. The prod Postgres log shows
the pair verbatim::

    ERROR:  must be owner of view ncen_effective_filing_candidates
    STATEMENT:  -- Effective N-CEN selection: ... CREATE OR REPLACE VIEW ...
    ERROR:  current transaction is aborted, commands ignored until end of ...
    STATEMENT:  SELECT pg_advisory_unlock($1)

``advisory_lock``'s ``finally`` emitted the unlock while the body's exception was
already in flight and the transaction was ABORTED; a ``finally``-raised exception
REPLACES the in-flight one, so the real cause never left the worker.

These tests pin both halves of the contract: the body's exception survives, and
the lock is still released.
"""
from __future__ import annotations

import os

import psycopg
import pytest

from src.db import advisory_lock, connect

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL not set"
)

# Test-only advisory lock id, outside every registered worker band (900_2xx/900_3xx).
TEST_LOCK_ID = 990_001


def _dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def _advisory_locks_held(lock_id: int) -> int:
    with psycopg.connect(_dsn(), autocommit=True) as admin:
        return admin.execute(
            "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=%s",
            (lock_id,),
        ).fetchone()[0]


def test_release_does_not_mask_the_body_error():
    """The exception that escapes is the body's, never the unlock's."""
    conn = connect(_dsn())
    try:
        with pytest.raises(psycopg.Error) as excinfo:
            with advisory_lock(conn, TEST_LOCK_ID) as acquired:
                assert acquired
                conn.execute("SELECT * FROM a_relation_that_does_not_exist_xyz")
        assert isinstance(excinfo.value, psycopg.errors.UndefinedTable)
        assert "current transaction is aborted" not in str(excinfo.value)
    finally:
        conn.close()


def test_lock_is_released_even_when_the_transaction_is_aborted():
    """Not masking must not become "not releasing": the unlock still happens."""
    conn = connect(_dsn())
    try:
        with pytest.raises(psycopg.Error):
            with advisory_lock(conn, TEST_LOCK_ID) as acquired:
                assert acquired
                conn.execute("SELECT * FROM a_relation_that_does_not_exist_xyz")
        # The connection is still open, so a leaked session lock would be visible.
        assert _advisory_locks_held(TEST_LOCK_ID) == 0
    finally:
        conn.close()


def test_release_is_a_no_op_on_a_clean_exit():
    """The happy path is untouched: acquire, work, release."""
    conn = connect(_dsn())
    try:
        with advisory_lock(conn, TEST_LOCK_ID) as acquired:
            assert acquired
            assert conn.execute("SELECT 1").fetchone()[0] == 1
            assert _advisory_locks_held(TEST_LOCK_ID) == 1
        assert _advisory_locks_held(TEST_LOCK_ID) == 0
    finally:
        conn.close()
