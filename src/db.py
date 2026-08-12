"""Shared DB access for data-lake workers.

Single dependency surface for connecting to the TimescaleDB Cloud data-lake.
Workers must use ``connect()`` and ``advisory_lock()`` — never hard-code DSNs.
"""

from __future__ import annotations

import contextlib
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg
from psycopg.pq import TransactionStatus


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


def _transaction_is_aborted(conn: psycopg.Connection) -> bool:
    """True when the connection sits in an ABORTED transaction.

    Introspection is defensive on purpose: ``advisory_lock`` is also driven with
    duck-typed connection stubs in the worker suites, and a stub that exposes no
    ``info`` simply has no aborted transaction to end.
    """
    info = getattr(conn, "info", None)
    return getattr(info, "transaction_status", None) is TransactionStatus.INERROR


def _release_advisory_lock(conn: psycopg.Connection, lock_id: int) -> None:
    """Release the session lock WITHOUT masking an exception already in flight.

    This runs from ``advisory_lock``'s ``finally``, i.e. possibly while the body's
    exception is propagating. Two rules make it safe:

    1. If the body left the transaction ABORTED, *any* statement raises
       ``InFailedSqlTransaction``; end that transaction first so the unlock is a
       legal statement. (``ROLLBACK`` does not drop session advisory locks.)
    2. A ``finally``-raised exception REPLACES the in-flight one, so a release
       failure must never escape while another exception is unwinding — it would
       become the recorded cause and bury the real one.

    Incident 2026-07-24 (chain runs 468c9ead / b3722d63 / b2e61544): the prod
    Postgres log records ``ERROR: must be owner of view
    ncen_effective_filing_candidates`` immediately followed by ``ERROR: current
    transaction is aborted ... STATEMENT: SELECT pg_advisory_unlock($1)``. The
    second error was the one every caller saw; the first was lost.
    """
    in_flight = sys.exc_info()[0] is not None
    try:
        if _transaction_is_aborted(conn):
            conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
    except Exception:
        # Never bury the caller's error. A session advisory lock dies with the
        # session, and every caller closes this connection right after, so the
        # lock cannot outlive the failure it would otherwise have masked.
        if not in_flight:
            raise


@contextlib.contextmanager
def advisory_lock(conn: psycopg.Connection, lock_id: int) -> Iterator[bool]:
    """Try a session advisory lock; yields True if acquired. Releases on exit.

    Each worker owns a distinct lock_id (900_2xx range) so concurrent Railway
    services do not serialize against each other across different workers.

    The release is deliberately non-masking: see ``_release_advisory_lock``.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
        got = bool(cur.fetchone()[0])
    try:
        yield got
    finally:
        if got:
            _release_advisory_lock(conn, lock_id)


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
# 900_216 was taken by the IPCA factor pack while this branch sat unmerged; two
# workers sharing an advisory lock id block each other silently, so this takes
# the next free id in the band instead.
LOCK_MARKET_OVERVIEW_SNAPSHOT = 900_217
# open_macro v4.0-rev (M-COMP4) monthly regime worker. It publishes to the SIBLING
# open_macro_v04_* tables, so it deliberately does NOT share LOCK_OPEN_MACRO_V03:
# the two workers touch disjoint tables and must be allowed to run concurrently
# (the v03 daily job is live and certified; serializing against a dark-run v4
# backfill would delay it for no reason). 900_218 is the next free id in the
# metrics band (900_2xx) after LOCK_MARKET_OVERVIEW_SNAPSHOT.
LOCK_OPEN_MACRO_V04 = 900_218
# fund_peer_groups quarterly partitioner (fund_peer_groups_v1). It reads the N-PORT
# look-through and the served universe and republishes ONE anchor atomically; a second
# concurrent run would DELETE the anchor the first one is still inserting. It shares no
# table with any worker above, so it gets its own id rather than borrowing one:
# 900_219 is the next free id in the metrics band (900_2xx) after LOCK_OPEN_MACRO_V04.
LOCK_FUND_PEER_GROUPS = 900_219
# open_macro_v03_decision_chain producer. It APPENDS the next missing month to the
# certified chain; two concurrent runs would both replay the same series and race on
# the same primary key (the second insert is a no-op, but the ~2 min replay is pure
# waste). It shares no table with LOCK_OPEN_MACRO_V03 (that worker writes the daily
# ledger, never the chain) nor with LOCK_OPEN_MACRO_V04 (which only READS the chain
# and must stay free to run), so it takes its own id: 900_220 is the next free one in
# the metrics band (900_2xx) after LOCK_FUND_PEER_GROUPS.
LOCK_OPEN_MACRO_V03_CHAIN = 900_220
# Public status-only evidence has its own atomic publication boundary and must not
# share the v04 decision worker lock: the evidence worker reads an already-published
# v04 row, then owns only its private lineage + public evidence relations.
LOCK_OPEN_MACRO_V04_PIT_EVIDENCE = 900_221
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
LOCK_NPORT_INGESTION = 900_307
# N-CEN V2 raw/shadow landing owns its package-level worker lock.  Deliberately
# distinct from N-PORT and the 13F/Form345 SEC ingestion lanes.
LOCK_NCEN_INGESTION = 900_310
# 900_338 belonged to the N-CEN derived-profile publication worker, removed on
# 2026-07-30 with the nine N-CEN profile products.  The id stays retired rather
# than recycled so an in-flight lock from an old revision can never collide with
# a new lane.
# RR1 derived-profile publication worker (the fee waterfall -- the only surviving
# RR1 derived product).  Distinct from the RR1 raw-landing lane below; 900_339 is
# the next free id after the retired 900_338.
LOCK_RR1_DERIVED_PROFILES = 900_339
# Public serving projection worker (sec_regulatory_serving_v1). Projects the
# current RR1 snapshots into the public-only serving surface consumed by the
# app. 900_340 is the next free id after the RR1 derived lock (900_339).
LOCK_SEC_REGULATORY_SERVING = 900_340
# RR1 V2 raw landing owns the next documented free ingestion lock.  900_311
# and 900_312 remain reserved by the ESMA workers.
LOCK_RR1_INGESTION = 900_313
LOCK_SEC_COMPANY_TICKERS_MF = 900_309
LOCK_EOD_PRICES_WARMER = 900_335
# tiingo_fund_meta ingestion worker (fund catalog descriptive prose + startDate).
# 900_336 is the next free id in the market/identity ingestion band (900_3xx),
# immediately after eod_prices_warmer's 900_335; no collision with any worker above.
LOCK_TIINGO_FUND_META = 900_336
# mixed_quant_v1 point-in-time publication worker. Serializes the build of one
# inactive publication; 900_337 is the next free id after tiingo_fund_meta.
LOCK_MIXED_QUANT_PUBLICATION = 900_337
# bond_security_v1 point-in-time security master materializer (Increment 2).
# Serializes the single prepared->validated->current build under the shared
# derived-publication protocol. 900_341 is the next free id in the ingestion band
# (900_3xx) after LOCK_SEC_REGULATORY_SERVING (900_340).
LOCK_BOND_SECURITY_MASTER = 900_341
# bond_price_observation_v1 point-in-time price/trade observation materializer
# (Increment 2, Task 4). Serializes the single prepared->validated->current build
# of the price lanes under the shared derived-publication protocol. 900_342 is the
# next free id in the ingestion band (900_3xx) after LOCK_BOND_SECURITY_MASTER.
LOCK_BOND_PRICE_OBSERVATIONS = 900_342
# bond_serving_v1 public serving projection worker (Increment 2, Task 5). Projects
# the current bond security / price-lane / N-PORT reverse-lookup snapshots into the
# public-only bond serving surface consumed by the app. Sibling product to
# sec_regulatory_serving_v1. 900_343 is the next free id in the ingestion band
# (900_3xx) after LOCK_BOND_PRICE_OBSERVATIONS.
LOCK_BOND_SERVING = 900_343
# Daily publication chain orchestrator (Increment 2, Task 6). A single chain-run
# advisory lock held for the whole run so overlapping runs cannot interleave the
# eight publication stages (spec §5). It is a level ABOVE the per-worker locks
# above: while the chain lock is held the chain invokes the individual workers,
# each of which still takes its own lock. 900_344 is the next free id in the
# ingestion band (900_3xx) after LOCK_BOND_SERVING.
LOCK_DAILY_PUBLICATION_CHAIN = 900_344
# bond_curve_v1 point-in-time spot/par curve materializer (Increment 3, Task 5).
# Serializes the single prepared->validated->current build of the curve snapshot
# under the shared derived-publication protocol. 900_345 is the next free id in the
# ingestion band (900_3xx) after LOCK_DAILY_PUBLICATION_CHAIN.
LOCK_BOND_CURVE = 900_345
# bond_rating_history_v1 point-in-time rating history materializer (Increment 3,
# Task 5) with an explicit license gate. Serializes the single build. 900_346 is
# the next free id after LOCK_BOND_CURVE.
LOCK_BOND_RATING_HISTORY = 900_346
# bond_price_eligibility_v1 additive price-eligibility predicate installer
# (Increment 3, Task 5). Serializes the idempotent DDL install of the eligibility
# view/function over bond_price_observation. 900_347 is the next free id after
# LOCK_BOND_RATING_HISTORY.
LOCK_BOND_PRICE_ELIGIBILITY = 900_347
# bond_price_ingest artifact-pinned source ingest worker (activation Wave 1,
# Task 1). Lands pinned source-artifact rows into the immutable
# bond_price_observation table and registers the validated source pair the
# price materializer discovers via sec_validated_raw_runs. 900_348 is the next
# free id in the ingestion band (900_3xx) after LOCK_BOND_PRICE_ELIGIBILITY.
LOCK_BOND_PRICE_INGEST = 900_348
# bond_source_qualify owner-authorized source-qualification worker (activation
# Wave 1, Task 2). Serializes the self-installing bond_source_qualification DDL
# and the idempotent qualification INSERT: CREATE TABLE IF NOT EXISTS is not
# race-safe on first concurrent creation, so the lock is taken BEFORE
# install_gate_schema (same idiom as the sibling ingest worker). 900_349 is the
# next free id after LOCK_BOND_PRICE_INGEST.
LOCK_BOND_SOURCE_QUALIFY = 900_349
# bond_metric_v1 compute+persist worker (activation Wave 1, Task 3). Runs the
# validated pure pricing/cashflow engines per security over the current security
# terms + eligible latest price and publishes the bond_metric_v1 product through
# the shared derived-publication protocol. The lock is taken BEFORE the
# self-installing DDL (fleet idiom: CREATE TABLE IF NOT EXISTS is not race-safe
# on first concurrent creation). 900_350 is the next free id after
# LOCK_BOND_SOURCE_QUALIFY.
LOCK_BOND_METRICS = 900_350
# nport_fixed_income_features_v1 build+promotion worker (Wave 3). Serializes the
# idempotent DDL/builder install and the single prepared->validated->current
# build of the eight N-PORT fixed-income relations the app's fixed-income
# dossier reads. The lock is taken BEFORE the self-installing DDL (fleet idiom:
# CREATE TABLE IF NOT EXISTS is not race-safe on first concurrent creation).
# 900_351 is the next free id in the ingestion band (900_3xx) after
# LOCK_BOND_METRICS.
LOCK_NPORT_FIXED_INCOME_SERVING = 900_351
# SEC "effective" selection matview cache (Wave 5, data-cost). Serializes the
# conditional REFRESH of ncen_effective_filings_mv / rr1_effective_fact_calendar_mv
# so the daily chain and the matview_refresh worker cannot expand the same raw
# selection twice at once. 900_352 is the next free id after
# LOCK_NPORT_FIXED_INCOME_SERVING.
LOCK_SEC_EFFECTIVE_MATVIEWS = 900_352
# Bond live daily feed (candles/curve/ticks + republication). Its own id, NOT
# shared with LOCK_BOND_METRICS or LOCK_BOND_SERVING: the worker invokes both of
# those publication workers as its last stage, and they take their own locks on
# their own connections — reusing an id here would deadlock the worker against
# itself. 900_353 is the next free id after LOCK_SEC_EFFECTIVE_MATVIEWS.
LOCK_BOND_LIVE_DAILY = 900_353
# Immutable Regulation S / Rule 144A registry loader.  It is intentionally
# distinct from every bond materializer because drafts and approvals are an
# operator-gated evidence lane, not a publication stage.  900_354 is the next
# free documented ingestion lock after the daily bond worker.
LOCK_BOND_DISTRIBUTION_REGISTRY_BACKFILL = 900_354
