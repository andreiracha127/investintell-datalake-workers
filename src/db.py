"""Shared DB access for data-lake workers.

Single dependency surface for connecting to the TimescaleDB Cloud data-lake.
Workers must use ``connect()`` and ``advisory_lock()`` — never hard-code DSNs.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator

import psycopg


def resolve_dsn(dsn: str | None = None) -> str:
    """Return an explicit DSN, else DATABASE_URL from the environment."""
    dsn = dsn or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("no DSN: pass dsn=... or set DATABASE_URL")
    return dsn


def connect(dsn: str | None = None, *, autocommit: bool = False) -> psycopg.Connection:
    """Open a psycopg3 connection to the target (cloud) database."""
    return psycopg.connect(resolve_dsn(dsn), autocommit=autocommit)


@contextlib.contextmanager
def advisory_lock(conn: psycopg.Connection, lock_id: int) -> Iterator[bool]:
    """Try a session advisory lock; yields True if acquired. Releases on exit.

    Each worker owns a distinct lock_id (900_2xx range) so concurrent Railway
    services do not serialize against each other across different workers.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
        got = bool(cur.fetchone()[0])
    try:
        yield got
    finally:
        if got:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))


# Advisory lock id registry (keep distinct per worker).
# Metrics band: 900_2xx. Ingestion band: 900_3xx (docs/INGESTION_DESIGN.md §1).
LOCK_RISK_METRICS = 900_201
LOCK_CHARACTERISTICS = 900_202
LOCK_FACTOR_MODEL = 900_203
LOCK_NPORT_LOOKTHROUGH = 900_204
LOCK_CREDIT_REGIME = 900_205
LOCK_REGIME_COMPOSITE = 900_206
LOCK_ACTIVE_SHARE_METRICS = 900_207
LOCK_MOMENTUM_METRICS = 900_208
LOCK_MACRO_INGESTION = 900_320
LOCK_TREASURY_INGESTION = 900_324
LOCK_INSTRUMENT_INGESTION = 900_331
LOCK_BENCHMARK_INGEST = 900_332
LOCK_EOD_PRICES_WARMER = 900_335
LOCK_SEC_13F_INGESTION = 900_305
LOCK_FORM345_INGESTION = 900_306
