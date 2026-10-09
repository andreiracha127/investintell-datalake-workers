"""tiingo_fund_meta worker — persist Tiingo metadata for every fund and ETF we price.

The Investintell-Light fund dossier needs (a) a per-fund descriptive paragraph
and (b) inception dates, and the Light walk-forward bounds every fund or ETF line
by its Tiingo span (``startDate``..``endDate``), refusing a line with no row.
Tiingo's end-of-day metadata endpoint ``GET https://api.tiingo.com/tiingo/daily/{ticker}``
returns a single JSON object ``{ticker, name, description, startDate, endDate,
exchangeCode}``; this worker caches it in ``tiingo_fund_meta``.

SCOPE: descriptive prose (``description``) + the ``startDate``/``endDate`` span
only. The legacy allocation repo deliberately sources fund *attributes* from SEC
filings — that decision stands (see schemas/tiingo_fund_meta.sql). Downstream
inception back-fill of ``sec_registered_funds`` / ``sec_etfs`` is proposed as a
manual, NULL-only enrichment in ``schemas/enrichment/tiingo_fund_meta_inception.sql``
— this worker never writes those catalog tables.

Universe = the fund catalog (``CATALOG_TICKER_SOURCES``) ∪ every ticker in
``eod_prices`` or ``universe_constituents`` that Tiingo lists as an ETF or a
mutual fund. The catalog alone missed priced funds: AGG is in ``eod_prices`` and
Light resolves it as a fund, but it is in none of the SEC catalog tables. Neither
price table carries an asset type and the meta endpoint returns none, so the type
comes from the price source itself: Tiingo's ``supported_tickers.csv``
(``assetType``), read once per run. A reused ticker takes the type of its current
listing, the security the meta endpoint describes. A ticker Tiingo does not list
gets no row. Catalog tickers bypass the type filter and refresh as before.

Incremental / resumable: a ticker is due when it has no row or its row is older
than ``refresh_days``. Missing rows go first (a backfill is not queued behind a
refresh wave), then stale rows, stalest first. ``limit`` caps the due fetches of
one run, so ``WORKER_LIMIT`` plus a multi-hour cron spreads a sweep over several
runs and each run resumes where the last stopped; ``fetched_at`` is the cursor.
An unknown ticker (Tiingo 404) is recorded once as ``source_status='not_found'``
so it is not re-queried every cycle. Idempotent upsert keyed by ticker; a run
aborts cleanly (and resumes next cycle) if Tiingo trips the shared 30×429 breaker.

Contract:  run(dsn=None, *, refresh_days=30, limit=None) -> {"universe", "catalog",
"extension", "supported_tickers", "due", "deferred", "fetched", "upserted",
"changed", "not_found", "skipped_fresh"[, "aborted"]}. Env: TIINGO_API_KEY.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Mapping
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

# Tiingo ``assetType`` values (supported_tickers.csv) that make a priced ticker a
# fund line. The third value, ``Stock``, is excluded.
FUND_ASSET_TYPES: frozenset[str] = frozenset({"ETF", "Mutual Fund"})

# Every ticker we hold prices for or screen: the same union eod_prices_warmer
# sweeps, without its ``status = 'active'`` filter on the screener universe.
PRICED_TICKERS_SQL = """
    SELECT DISTINCT ticker FROM eod_prices
    UNION
    SELECT ticker FROM universe_constituents
"""

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

_NEVER = _dt.datetime.min.replace(tzinfo=_dt.timezone.utc)


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


def select_extension(priced: Iterable[str | None], asset_types: Mapping[str, str]) -> list[str]:
    """Priced tickers whose current Tiingo listing is an ETF or a mutual fund.

    ``asset_types`` is ``parse_supported_tickers`` output (upper-cased keys). A
    ticker Tiingo does not list is left out: no signal is not a fund. Returns
    upper-cased, de-duplicated, sorted tickers."""
    selected = set()
    for raw in priced:
        ticker = (raw or "").strip().upper()
        if ticker and asset_types.get(ticker) in FUND_ASSET_TYPES:
            selected.add(ticker)
    return sorted(selected)


def due_tickers(
    universe: Iterable[str],
    existing: Mapping[str, dict[str, Any]],
    now: _dt.datetime,
    refresh_days: int,
) -> list[str]:
    """Tickers to fetch: no row first (sorted), then stale rows, stalest first.

    Ordering the stale tail by ``fetched_at`` is what lets a capped run resume:
    each fetch moves its ticker to the back, so the next run starts where this
    one stopped without a cursor table."""
    ordered = sorted(set(universe))
    missing = [t for t in ordered if t not in existing]
    stale = sorted(
        (t for t in ordered
         if t in existing and not is_fresh(existing[t], now, refresh_days)),
        key=lambda t: (existing[t].get("fetched_at") or _NEVER, t),
    )
    return missing + stale


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


def select_priced_tickers(conn) -> list[str]:
    """Tickers present in ``eod_prices`` or ``universe_constituents`` (raw case)."""
    with conn.cursor() as cur:
        cur.execute(PRICED_TICKERS_SQL)
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
    """Refresh ``tiingo_fund_meta`` for the catalog and every priced ETF/fund."""
    now = _dt.datetime.now(_dt.timezone.utc)
    fetched = upserted = changed = not_found = 0
    aborted: list[str] = []

    with connect(resolve_dsn(dsn)) as conn:
        with advisory_lock(conn, LOCK_TIINGO_FUND_META) as got:
            if not got:
                return {"skipped": "lock_busy"}

            ensure_schema(conn)
            catalog = set(select_universe(conn))
            bucket = TokenBucket(max_tokens=FETCH_BURST, refill_rate=FETCH_RATE_PER_S)
            with TiingoClient(bucket=bucket) as tiingo:
                # Without Tiingo's listing nothing can be typed, so only the
                # catalog refreshes and the run reports itself aborted (exit 1).
                try:
                    asset_types = tiingo.fetch_supported_asset_types()
                except Exception as exc:  # transport error or unusable file
                    asset_types = {}
                    aborted.append(
                        f"supported_tickers unavailable ({type(exc).__name__}: {exc}); "
                        "refreshed the catalog only"
                    )
                extension: set[str] = set()
                if asset_types:
                    extension = set(select_extension(select_priced_tickers(conn), asset_types))
                extension -= catalog
                universe = catalog | extension
                existing = existing_meta(conn)
                due = due_tickers(universe, existing, now, refresh_days)
                batch = due[:limit] if limit else due
                print(
                    f"tiingo_fund_meta: {len(catalog)} catalog + {len(extension)} priced "
                    f"ETF/fund tickers, {len(existing)} cached, {len(due)} due, "
                    f"fetching {len(batch)}, refresh_days={refresh_days}",
                    flush=True,
                )

                for i, ticker in enumerate(batch, start=1):
                    prior = existing.get(ticker)
                    try:
                        payload = tiingo.fetch_meta(ticker)
                    except TiingoBudgetExceeded as exc:
                        aborted.append(str(exc))
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
                            f"tiingo_fund_meta: {i}/{len(batch)} tickers, "
                            f"upserted={upserted}, not_found={not_found}",
                            flush=True,
                        )

    stats: dict[str, Any] = {
        "universe": len(universe),
        "catalog": len(catalog),
        "extension": len(extension),
        "supported_tickers": len(asset_types),
        "due": len(due),
        "deferred": len(due) - fetched,
        "fetched": fetched,
        "upserted": upserted,
        "changed": changed,
        "not_found": not_found,
        "skipped_fresh": len(universe) - len(due),
    }
    if aborted:
        stats["aborted"] = "; ".join(aborted)
    return stats
