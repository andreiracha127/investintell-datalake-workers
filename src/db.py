"""Shared DB access for data-lake workers.

Single dependency surface for connecting to the TimescaleDB Cloud data-lake.
Workers must use ``connect()`` and ``advisory_lock()`` — never hard-code DSNs.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg


def _materialize_tls() -> dict[str, str] | None:
    """PEMs via env → arquivos (key 0600). Railway não tem secret-files; env é o canal."""
    ca, crt, key = (os.getenv(v) for v in ("DB_TLS_CA_PEM", "DB_TLS_CERT_PEM", "DB_TLS_KEY_PEM"))
    if not (ca and crt and key):
        return None
    dir_str = os.getenv("DB_TLS_DIR", "/tmp/db-tls").rstrip("/")
    d = Path(dir_str)
    d.mkdir(parents=True, exist_ok=True)
    (d / "ca.crt").write_text(ca)
    (d / "client.crt").write_text(crt)
    kp = d / "client.key"
    kp.write_text(key)
    kp.chmod(0o600)
    # Build DSN path values by plain "/" concatenation (not pathlib join): DSN
    # paths are URI-shaped and libpq/psycopg accept forward slashes on every
    # platform, including native Windows. Using the raw DB_TLS_DIR string keeps
    # this stable across OSes instead of pathlib re-normalizing separators.
    return {"sslmode": "verify-full", "sslrootcert": f"{dir_str}/ca.crt",
            "sslcert": f"{dir_str}/client.crt", "sslkey": f"{dir_str}/client.key"}


def _apply_tls(dsn: str, tls: dict[str, str]) -> str:
    """Strip any pre-existing sslmode/ssl* query params and append the TLS ones.

    NOTE: passwords in DATABASE_URL must be URL-encoded (e.g. "/" -> "%2F",
    "@" -> "%40"). urlsplit/urlunsplit only split/rejoin the DSN around
    delimiters — they never percent-decode or re-encode the netloc — so an
    already-encoded userinfo section round-trips byte-identical here. Task 9
    wires the Railway envs; the live `timescale-worker-writer` password
    contains "/" and must be encoded before it reaches DATABASE_URL.
    """
    parts = urlsplit(dsn)
    kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if not (k == "sslmode" or k.startswith("ssl"))]
    kept.extend(tls.items())
    # safe chars keep file-path separators (POSIX "/" and Windows "\") and
    # drive-letter colons readable in the DSN instead of percent-encoded
    # (libpq parses either, but unescaped is what operators expect in logs).
    query = urlencode(kept, safe="/:\\")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def resolve_dsn(dsn: str | None = None) -> str:
    """Return an explicit DSN, else DATABASE_URL from the environment.

    When ``DB_TLS_CA_PEM``/``DB_TLS_CERT_PEM``/``DB_TLS_KEY_PEM`` are set, the
    PEMs are materialized to disk (see ``_materialize_tls``) and the resolved
    DSN gets verify-full mTLS query params appended, regardless of whether the
    DSN came from the explicit argument or DATABASE_URL.
    """
    dsn = dsn or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("no DSN: pass dsn=... or set DATABASE_URL")
    tls = _materialize_tls()
    if tls:
        dsn = _apply_tls(dsn, tls)
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
LOCK_REGIME_GATE = 900_207
LOCK_REGIME_QUADRANT = 900_208
# TECH DEBT: main also introduced screener_metrics with 900_207. Keep the
# combo-regime-gate lock ownership here; reassign main-added metrics locks
# before these services are allowed to run concurrently.
LOCK_SCREENER_METRICS = 900_207
LOCK_FUND_FACTORS = 900_214
# open_macro_v03 direct-activation runtime worker (Stage B). 900_215 is the next
# free id in the metrics band (900_2xx); it does not collide with any worker above
# (900_207 is the known SCREENER/REGIME_GATE double-assignment, left untouched).
LOCK_OPEN_MACRO_V03 = 900_215
LOCK_IPCA_FACTOR_PACK = 900_216
LOCK_FUND_INSTITUTIONAL_REVEAL = 900_209
LOCK_MATVIEW_REFRESH = 900_210
LOCK_STOCK_DAILY_RETURNS = 900_211
LOCK_ACTIVE_SHARE_METRICS = 900_212
LOCK_MOMENTUM_METRICS = 900_213
LOCK_MACRO_INGESTION = 900_320
LOCK_MACRO_VINTAGE = 900_321
LOCK_TREASURY_INGESTION = 900_324
LOCK_INSTRUMENT_INGESTION = 900_331
LOCK_BENCHMARK_INGEST = 900_332
LOCK_SEC_13F_INGESTION = 900_305
LOCK_FORM345_INGESTION = 900_306
LOCK_NPORT_CUSIP_ENRICHMENT = 900_308
LOCK_SEC_COMPANY_TICKERS_MF = 900_309
LOCK_EOD_PRICES_WARMER = 900_335
# tiingo_fund_meta ingestion worker (fund catalog descriptive prose + startDate).
# 900_336 is the next free id in the market/identity ingestion band (900_3xx),
# immediately after eod_prices_warmer's 900_335; no collision with any worker above.
LOCK_TIINGO_FUND_META = 900_336
