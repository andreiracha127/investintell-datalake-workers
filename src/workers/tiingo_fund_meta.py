"""tiingo_fund_meta worker — persist Tiingo fund metadata for the fund catalog.

The Investintell-Light fund dossier needs (a) a per-fund descriptive paragraph
and (b) inception dates. Tiingo's end-of-day metadata endpoint
``GET https://api.tiingo.com/tiingo/daily/{ticker}`` returns a single JSON object
``{ticker, name, description, startDate, endDate, exchangeCode}``. Nothing in the
data lake persisted that today, so this worker caches it in ``tiingo_fund_meta``.

SCOPE: descriptive prose (``description``) + ``startDate`` (inception) only. The
legacy allocation repo deliberately sources fund *attributes* from SEC filings —
that decision stands (see schemas/tiingo_fund_meta.sql). Downstream inception
back-fill of ``sec_registered_funds`` / ``sec_etfs`` is proposed as a manual,
NULL-only enrichment in ``schemas/enrichment/tiingo_fund_meta_inception.sql`` —
this worker never writes those catalog tables.

Universe = distinct non-null tickers from the fund catalog tables
(``sec_fund_classes``, ``sec_etfs``, ``sec_registered_funds``); the source list
``CATALOG_TICKER_SOURCES`` is a module constant so it is trivial to extend.

Incremental / resumable: each run skips tickers whose row is younger than
``refresh_days`` (skip-when-fresh) and re-fetches the rest; an unknown ticker
(Tiingo 404) is recorded once as ``source_status='not_found'`` so it is not
re-queried every cycle. Idempotent upsert keyed by ticker; a run aborts cleanly
(and resumes next cycle) if Tiingo trips the shared 30×429 breaker.

Contract:  run(dsn=None, *, refresh_days=30, limit=None) -> {"universe",
"fetched", "upserted", "not_found", "skipped_fresh", ...}. Env: TIINGO_API_KEY.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from src.db import LOCK_TIINGO_FUND_META, advisory_lock, connect, resolve_dsn
from src.workers._tiingo import (
    DEFAULT_RATE_PER_S,
    TiingoBudgetExceeded,
    TiingoClient,
    TokenBucket,
)

DEFAULT_REFRESH_DAYS = 30     # re-fetch a cached ticker only after it is this stale
PROGRESS_EVERY = 500          # emit a heartbeat log every N tickers (observability)

# Tiingo pacing for the metadata endpoint. The account is on the Power tier
# (10k req/h, no X-RateLimit headers). The old note called 10 req/s "≈ 36k/h,
# well within reach" — 36k/h is 3.6x the ceiling; what was actually within reach
# was the *sweep size*, which says nothing about the rate other consumers see.
# The 30x429 breaker is a backstop, not a licence to pace above the account.
FETCH_RATE_PER_S = DEFAULT_RATE_PER_S
FETCH_BURST = 10.0

# Fund catalog tables whose ``ticker`` column seeds the fetch universe. Append a
# table here (and re-run) to widen coverage — the universe SQL is composed from
# this list, so no other change is needed.
CATALOG_TICKER_SOURCES: tuple[str, ...] = (
    "sec_fund_classes",
    "sec_etfs",
    "sec_registered_funds",
)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tiingo_fund_meta (
    ticker        text        PRIMARY KEY,
    name          text,
    description   text,
    exchange_code text,
    start_date    date,
    end_date      date,
    fetched_at    timestamptz NOT NULL DEFAULT now(),
    source_status text
);
CREATE INDEX IF NOT EXISTS tiingo_fund_meta_ticker_idx
    ON tiingo_fund_meta (upper(ticker));
CREATE INDEX IF NOT EXISTS tiingo_fund_meta_fetched_at_idx
    ON tiingo_fund_meta (fetched_at);
"""

UPSERT_SQL = """
    INSERT INTO tiingo_fund_meta
        (ticker, name, description, exchange_code, start_date, end_date, source_status, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
    ON CONFLICT (ticker) DO UPDATE SET
        name          = EXCLUDED.name,
        description   = EXCLUDED.description,
        exchange_code = EXCLUDED.exchange_code,
        start_date    = EXCLUDED.start_date,
        end_date      = EXCLUDED.end_date,
        source_status = EXCLUDED.source_status,
        fetched_at    = now()
"""

# The stored content columns, in UPSERT_SQL order (ticker excluded): used to
# detect whether a re-fetch actually changed anything vs only bumped fetched_at.
_CONTENT_COLUMNS: tuple[str, ...] = (
    "name", "description", "exchange_code", "start_date", "end_date", "source_status",
)


# ──────────────────────────────────────────────────────────────────────────────
# Pure helpers (no network, no DB)
# ──────────────────────────────────────────────────────────────────────────────
def universe_sql(sources: tuple[str, ...] = CATALOG_TICKER_SOURCES) -> str:
    """Compose the ``UNION`` of distinct non-null catalog tickers.

    Each source contributes ``SELECT DISTINCT upper(ticker) ...`` filtered to
    non-null / non-blank tickers; ``UNION`` dedups across tables. Upper-casing
    matches the catalog crosswalk convention (see nport_lookthrough's t2s CTE)
    and keeps the ticker PK canonical."""
    if not sources:
        raise ValueError("CATALOG_TICKER_SOURCES must not be empty")
    selects = [
        f"SELECT DISTINCT upper(ticker) AS ticker FROM {table} "
        f"WHERE ticker IS NOT NULL AND btrim(ticker) <> ''"
        for table in sources
    ]
    return "\nUNION\n".join(selects) + "\nORDER BY ticker"


def _parse_date(value: Any) -> _dt.date | None:
    """Tiingo ISO date string (or None/'') → date, tolerant of junk."""
    if not value:
        return None
    try:
        return _dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def build_meta_row(ticker: str, payload: dict | None) -> tuple[Any, ...]:
    """Ticker + Tiingo meta payload → an UPSERT_SQL row tuple (fetched_at excluded).

    ``payload is None`` (Tiingo 404 / unknown ticker) yields a ``not_found`` row
    with NULL content so the miss is cached and not re-queried until it goes
    stale again. A present payload yields ``source_status='ok'``."""
    if payload is None:
        return (ticker, None, None, None, None, None, "not_found")
    return (
        ticker,
        payload.get("name"),
        payload.get("description"),
        payload.get("exchangeCode"),
        _parse_date(payload.get("startDate")),
        _parse_date(payload.get("endDate")),
        "ok",
    )


def content_changed(row: tuple[Any, ...], existing: dict[str, Any] | None) -> bool:
    """True when the fetched row differs from the stored content (ticker aside).

    ``existing`` is the current DB row as a ``{column: value}`` dict (or None for
    a brand-new ticker). ``row`` is a ``build_meta_row`` tuple: index 0 is the
    ticker, indices 1.. line up with ``_CONTENT_COLUMNS``."""
    if existing is None:
        return True
    return any(
        row[i + 1] != existing.get(col)
        for i, col in enumerate(_CONTENT_COLUMNS)
    )


def is_fresh(existing: dict[str, Any] | None, now: _dt.datetime, refresh_days: int) -> bool:
    """True when the stored row is younger than ``refresh_days`` → skip re-fetch."""
    if existing is None:
        return False
    fetched_at = existing.get("fetched_at")
    if fetched_at is None:
        return False
    return fetched_at > now - _dt.timedelta(days=refresh_days)


# ──────────────────────────────────────────────────────────────────────────────
# DB I/O
# ──────────────────────────────────────────────────────────────────────────────
def ensure_schema(conn) -> None:
    """Self-bootstrap the table + indexes (idempotent; safe to call every run).

    Statements are executed one at a time so this does not depend on the driver
    allowing multiple semicolon-separated commands in a single execute()."""
    statements = [s.strip() for s in _SCHEMA_SQL.split(";") if s.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    conn.commit()


def select_universe(conn, *, sources: tuple[str, ...] = CATALOG_TICKER_SOURCES) -> list[str]:
    """Distinct upper-cased fund-catalog tickers across ``sources``."""
    with conn.cursor() as cur:
        cur.execute(universe_sql(sources))
        return [r[0] for r in cur.fetchall()]


def existing_meta(conn) -> dict[str, dict[str, Any]]:
    """Current ``tiingo_fund_meta`` rows keyed by ticker (for freshness/diff)."""
    cols = ("ticker", *_CONTENT_COLUMNS, "fetched_at")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, name, description, exchange_code, start_date, "
            "end_date, source_status, fetched_at FROM tiingo_fund_meta"
        )
        rows = cur.fetchall()
    return {r[0]: dict(zip(cols, r)) for r in rows}


def upsert_meta(conn, row: tuple[Any, ...]) -> None:
    with conn.cursor() as cur:
        cur.execute(UPSERT_SQL, row)
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(
    dsn: str | None = None,
    *,
    refresh_days: int = DEFAULT_REFRESH_DAYS,
    limit: int | None = None,
) -> dict:
    """Refresh ``tiingo_fund_meta`` from Tiingo for the fund-catalog universe."""
    now = _dt.datetime.now(_dt.timezone.utc)
    fetched = upserted = changed = not_found = skipped_fresh = 0
    aborted: str | None = None

    with connect(resolve_dsn(dsn)) as conn:
        with advisory_lock(conn, LOCK_TIINGO_FUND_META) as got:
            if not got:
                return {"skipped": "lock_busy"}

            ensure_schema(conn)
            tickers = select_universe(conn)
            if limit:
                tickers = tickers[:limit]
            existing = existing_meta(conn)
            print(
                f"tiingo_fund_meta: {len(tickers)} catalog tickers, "
                f"{len(existing)} cached, refresh_days={refresh_days}",
                flush=True,
            )

            bucket = TokenBucket(max_tokens=FETCH_BURST, refill_rate=FETCH_RATE_PER_S)
            with TiingoClient(bucket=bucket) as tiingo:
                for i, ticker in enumerate(tickers, start=1):
                    prior = existing.get(ticker)
                    if is_fresh(prior, now, refresh_days):
                        skipped_fresh += 1
                        continue
                    try:
                        payload = tiingo.fetch_meta(ticker)
                    except TiingoBudgetExceeded as exc:
                        aborted = str(exc)
                        break
                    fetched += 1
                    row = build_meta_row(ticker, payload)
                    if payload is None:
                        not_found += 1
                    # Upsert when content changed (or the ticker is new); an
                    # unchanged row would only bump fetched_at, but we still
                    # persist that so the skip-when-fresh gate advances and the
                    # ticker is not re-fetched next cycle.
                    if content_changed(row, prior):
                        changed += 1
                    upsert_meta(conn, row)
                    upserted += 1
                    if i % PROGRESS_EVERY == 0:
                        print(
                            f"tiingo_fund_meta: {i}/{len(tickers)} tickers, "
                            f"upserted={upserted}, not_found={not_found}",
                            flush=True,
                        )

    stats: dict[str, Any] = {
        "universe": len(tickers),
        "fetched": fetched,
        "upserted": upserted,
        "changed": changed,
        "not_found": not_found,
        "skipped_fresh": skipped_fresh,
    }
    if aborted:
        stats["aborted"] = aborted
    return stats
