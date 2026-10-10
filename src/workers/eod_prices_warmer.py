"""eod_prices_warmer worker — keep the API's ``eod_prices`` universe fresh.

Strategy B (Investintell-Light API latency-tail fix): the API serves /stocks/*
DB-first from ``eod_prices`` and never fetches a *stale* ticker synchronously on
the request path. This worker keeps that table fresh out-of-band for every
active screener constituent, every ticker already present in ``eod_prices``, and
the benchmark ETFs needed by the screener metrics worker.

Universe = active ``universe_constituents`` ∪ ``SELECT DISTINCT ticker FROM
eod_prices`` ∪ ``INDEX_TICKERS``. This keeps the public stock screener covered
instead of relying on a ticker to be queried once before it becomes warm. The
worker also seeds ``instruments`` for active screener tickers before touching
``eod_prices`` because the Timescale table has a ticker FK to ``instruments``.

Incremental for existing tickers, two-year cold start for newly covered screener
tickers: existing tickers fetch from ``max(date) − overlap`` (revisions), while
new tickers fetch enough history for screener beta_2y. ``eod_prices`` upserts
land on recent uncompressed chunks once the initial cold load is done.

Second source — W1c foreign listings, full history. Light sizes historical
equity priors as SEC share counts × ``eod_prices`` closes, so a US-listed foreign
line (ADS or direct ordinary listing) needs its whole price history, not two
years. Each run reads the distinct symbols whose
``public.sec_foreign_listing_at(cik, symbol, d)`` answer has ``listing_status =
'resolved'`` at the run date or at any of the 2010/2015/2020/2025 year-ends,
over the ``(cik, symbol)`` pairs of the active W1c evidence facts, restricted to
US-exchange ticker shapes (one bounded query, JIT off). The screener universe
(``universe_constituents``) does not change.

Every covered ticker gets a verification pass before it is ``history_complete``:
a fresh meta call (Tiingo ``startDate``; it also fills NULL ``instruments``
metadata without overwriting non-null fields), then ONE price request for the
interval ``[startDate, min(as_of, max(meta endDate, last stored date <=
as_of))]``, fixed before the fetch. ``eod_history_validation.validate_series``
— pure, and the only place any rule lives — judges the whole series against
the interval's XNYS sessions and every stored row in scope: strict date syntax,
bar coherence (OHLC order, one adjustment factor per bar, finite in-range
values), the exact session set (no hole anywhere, no off-session bar), and raw
and adjusted OHLC equal on every shared date. Its verdict is
``complete_nothing_to_insert`` or ``load`` (the exact rows to insert), or a
``history_incomplete`` / ``history_conflict`` / ``adjustment_rebase_required``
status; a mixed adjusted basis is never complete. Existing rows are never
rewritten, and there is no shortcut from ``min(date)``.

The phase never holds a database transaction across an HTTP call. The plan,
the footprint count and the stored-row snapshot are each ended by a commit
before the next Tiingo request (the snapshot is in memory; ``promote`` locks the
instrument, re-reads and compares the digest). A pass reads the ticker's rows
in ``[1970-01-01, as_of]`` and every read locks each chunk of that range, so a
range of more than ``MAX_VERIFICATION_CHUNKS`` (800) chunks is recorded
``history_incomplete`` (``chunk_footprint_exceeded``, counted in the run
statistics) before the meta call, and again before promotion, and does nothing
else. Tiingo metadata dates must be in the documented forms; a malformed one is
``meta:malformed_start_date`` / ``meta:malformed_end_date``, retried with
backoff and never parsed into an interval.

The W1c source is optional and isolated: discovery runs under a statement
timeout (``FOREIGN_DISCOVERY_TIMEOUT_MS``) and any exception is rolled back and
reported as ``foreign_history: {source: "error", reason: <ExceptionType>}``
while the ring runs and the worker exits zero. An empty source is reported as
``foreign_history: {source: "empty", ...}``.

``promote`` is the only writer of history rows and of ``history_complete``. In
one transaction it locks the ticker's ``instruments`` row, re-reads the stored
rows and compares their digest with the snapshot the verdict judged; if they
changed it writes nothing (``stored_rows_changed_during_pass``), else it inserts
the verdict's rows (possibly none) and the status together. A run never looks
past its ``as_of``: a historical run completes only through it
(``complete_through``), and every completion is re-verified after
``REVERIFY_DAYS``. Retryable failures (transient HTTP, malformed or unusable
bars, incomplete responses, any unexpected per-ticker exception) are
``history_incomplete`` with backoff (12 h doubling, up to 7 days) and count as
errors; the queue orders by next attempt, so a failing ticker cannot hold a cap
slot. Tickers Tiingo does not know are recorded, reported and re-checked after
``UNKNOWN_RECHECK_DAYS``. The ring is unchanged by this source: a covered
screener constituent without rows gets its usual 745-day warming, and the
history phase later extends it backward through the same verification.

History rows go through ``INSERT … ON CONFLICT DO NOTHING`` for dates the ticker
does not have, mostly into old, compressed chunks of the ``eod_prices``
hypertable (segmentby ticker, orderby date DESC). A key older than the ticker's
first row, or of a ticker without rows, matches no compressed batch, so nothing
is decompressed; an interior gap decompresses only the ticker's own batch
around it, bounded by the ticker's row count and far below the per-transaction
limit of 100,000. The columnstore policy recompresses the partial chunks. The phase
runs after the ring, on the same token bucket, and stops at
``HISTORY_TICKERS_PER_RUN`` tickers (env ``EOD_HISTORY_TICKERS_PER_RUN``, 0 =
off; never more than ``limit``). Whatever is still pending resumes on the next
run.

NOTE vs ``instrument_ingestion`` (which refreshes ``nav_timeseries`` for the fund
catalog): this worker targets ``eod_prices`` (stock/ETF OHLCV the /stocks/* API
reads). They cover different tables — do not conflate.

Contract:  run(dsn, *, calc_date=None, limit=None, history_limit=None)
-> {"fetched", "upserted", ..., "foreign_history": {...}}
``limit`` caps the number of ring tickers (smoke runs) and the history phase;
``history_limit`` overrides the history cap. Env: TIINGO_API_KEY.
"""

from __future__ import annotations

import datetime as _dt
import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import psycopg
import psycopg.sql

from src.db import LOCK_EOD_PRICES_WARMER, advisory_lock, connect
from src.workers._tiingo import (
    DEFAULT_RATE_PER_S,
    TiingoBudgetExceeded,
    TiingoClient,
    TokenBucket,
)
from src.workers.eod_history_validation import (
    CALENDAR_SUPPORTED_FROM,
    STORED_FIELDS,
    SUCCESS_VERDICTS,
    VERDICT_CONFLICT,
    VERDICT_INCOMPLETE,
    VERDICT_REBASE,
    Verdict,
    parse_bar_date,
    stored_digest,
    validate_series,
    xnys_calendar,
)

UPSERT_CHUNK = 500            # short transactions; well under the 65535-param ceiling (14/row)
WATERMARK_OVERLAP_DAYS = 5    # re-fetch the last few days to absorb provider revisions
NEW_TICKER_LOOKBACK_DAYS = 745  # covers screener beta_2y lookback on cold tickers

# Tiingo pacing — the shared account budget, not a per-worker one. The previous
# 25 req/s "fast lane" was 90k req/h against a ceiling of 10k req/h on today's key
# and lower still on the one live during the incident: it finished the sweep in
# ~90s by spending the fleet's whole hourly budget, and the regime workers that ran
# inside the same rolling hour got nothing but 429s. A slower sweep on a daily cron
# costs nothing; starving every other consumer does. The full sweep is ~5.4k
# tickers, so pair this with WORKER_LIMIT + a multi-hour cron to keep each run a
# fraction of the budget instead of most of it.
FETCH_RATE_PER_S = DEFAULT_RATE_PER_S
FETCH_BURST = 20.0
PROGRESS_EVERY = 500  # emit a heartbeat log every N tickers (observability)

# Index / benchmark ETFs the API and screener need even if never queried.
INDEX_TICKERS: tuple[str, ...] = ("SPY", "QQQ", "DIA", "IWM", "GLD", "AGG", "TLT", "USO")

# The six sleeves open_macro_v03 prices its books on. That worker fail-closes when
# any of them is more than 3 business days stale, so if the sweep never reaches
# them the whole macro signal goes dark — /macro and the builder both render
# "no usable macro signal". It happened: the sweep is alphabetical, the Tiingo
# budget dies around the 300th ticker, and S/T/G/D are never reached, so these
# stayed frozen at 2026-07-16 for 8 business days while the A-names refreshed
# daily. Cheap tickers, load-bearing signal — they go first, every run.
# LQD joins for open_macro v4.0-rev: the dominance book routes 0.155 into LQD
# through the B60-LQD barbell, and the v4 worker refuses to price a book on a
# session where any instrument is missing. Without LQD warmed, every dominance
# month is unpriceable — the same alphabetical-sweep failure mode that froze
# S/T/G/D for 8 business days, except LQD would never have been fetched at all.
MACRO_SLEEVE_TICKERS: tuple[str, ...] = ("SPY", "TLT", "TIP", "SHY", "GLD", "DBC",
                                         "LQD")

# Priority head: fetched before the long tail and never subject to the resume
# cursor, so a truncated run still refreshes everything the decision layer needs.
PRIORITY_TICKERS: tuple[str, ...] = tuple(
    dict.fromkeys(MACRO_SLEEVE_TICKERS + INDEX_TICKERS)
)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS eod_warmer_cursor (
    worker       text        PRIMARY KEY,
    last_ticker  text        NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);
"""
_CURSOR_KEY = "eod_prices_warmer"
_PRIORITY_SET = frozenset(PRIORITY_TICKERS)

# eod_prices price columns (all NOT NULL) → Tiingo daily-bar JSON key.
_BAR_KEYS: dict[str, str] = {
    "open": "open", "high": "high", "low": "low", "close": "close",
    "volume": "volume", "adj_open": "adjOpen", "adj_high": "adjHigh",
    "adj_low": "adjLow", "adj_close": "adjClose", "adj_volume": "adjVolume",
    "div_cash": "divCash", "split_factor": "splitFactor",
}
_EOD_COLUMNS: tuple[str, ...] = tuple(_BAR_KEYS)

EOD_UPSERT_SQL = """
    INSERT INTO eod_prices (
        ticker, date, open, high, low, close, volume,
        adj_open, adj_high, adj_low, adj_close, adj_volume, div_cash, split_factor
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        adj_open = EXCLUDED.adj_open,
        adj_high = EXCLUDED.adj_high,
        adj_low = EXCLUDED.adj_low,
        adj_close = EXCLUDED.adj_close,
        adj_volume = EXCLUDED.adj_volume,
        div_cash = EXCLUDED.div_cash,
        split_factor = EXCLUDED.split_factor
"""

SEED_ACTIVE_INSTRUMENTS_SQL = """
    INSERT INTO instruments (ticker, name, asset_type)
    SELECT ticker, name, 'stock'
    FROM universe_constituents
    WHERE status = 'active'
    ON CONFLICT (ticker) DO NOTHING
"""

SEED_EXTRA_INSTRUMENT_SQL = """
    INSERT INTO instruments (ticker, name, asset_type)
    VALUES (%s, %s, 'etf')
    ON CONFLICT (ticker) DO NOTHING
"""

# ──────────────────────────────────────────────────────────────────────────────
# Second source: W1c-resolved US-listed foreign lines, full history
# ──────────────────────────────────────────────────────────────────────────────
FOREIGN_LISTING_SOURCE = "w1c_foreign_listing"
# The run date is always probed too; these are the year-ends Light sizes priors at.
FOREIGN_LISTING_YEAR_ENDS: tuple[_dt.date, ...] = (
    _dt.date(2010, 12, 31), _dt.date(2015, 12, 31),
    _dt.date(2020, 12, 31), _dt.date(2025, 12, 31),
)
# US-exchange ticker shape: 1-5 letters, optionally one class letter (BRK-B).
# Drops home-market codes (SANB11, VIVT3), note lines (AMX22) and preferred or
# warrant suffixes (NM-PG, ALLG-WS). Tiingo still decides existence.
US_LISTING_TICKER_PATTERN = r"^[A-Z]{1,5}(-[A-Z])?$"
_US_LISTING_TICKER = re.compile(US_LISTING_TICKER_PATTERN)
# Full-history tickers per run. Each costs two logical requests (meta + one
# full-range price window) and, with the client's three-attempt retry ladder, at
# most six HTTP attempts. The default is the cautious rollout value; raise it
# with EOD_HISTORY_TICKERS_PER_RUN (250 is the steady-state target: <= 1,500 HTTP
# attempts per run on the shared bucket). 0 turns the phase off.
HISTORY_TICKERS_PER_RUN = 25
HISTORY_LIMIT_ENV = "EOD_HISTORY_TICKERS_PER_RUN"
UNKNOWN_RECHECK_DAYS = 30
# W1c discovery is optional, so it is bounded: a statement that runs past this
# is cancelled and reported as ``foreign_history: {source: "error"}`` while the
# ring runs. Measured 10 s over 1,676 symbols on 2026-10-10 (six times that is
# the limit); a persistent timeout shows as ``reason: QueryCanceled`` every run.
FOREIGN_DISCOVERY_TIMEOUT_MS = 60_000
# A verification pass reads the ticker's rows in [CALENDAR_SUPPORTED_FROM, as_of]
# and promotion reads them again, and each read takes a lock on every chunk of
# that range (about five locks per chunk). Past this many chunks the pass is
# recorded ``history_incomplete`` (``chunk_footprint_exceeded``) and does
# nothing else. Production, 2026-10-10: 787 chunks in all, 689 overlapping the
# range (one new chunk a year at 360-day chunks).
MAX_VERIFICATION_CHUNKS = 800
# A completed ticker is verified again after this many days, so a later change
# on Tiingo's side (an earlier startDate, a revised raw close, a stored gap) is
# found; the pass inserts nothing unless the series is still coherent.
REVERIFY_DAYS = 90
# A retryable failure (transient HTTP, unusable bars, an incomplete response)
# waits RETRY_BASE_HOURS, doubling with each consecutive failure up to
# RETRY_MAX_HOURS, so a persistently failing ticker cannot hold a cap slot.
RETRY_BASE_HOURS = 12
RETRY_MAX_HOURS = 7 * 24
_W1C_RESOLVER = "public.sec_foreign_listing_at(bigint,text,date)"

# Status of a covered ticker's history after its verification pass.
STATUS_COMPLETE = "history_complete"
STATUS_UNKNOWN = "tiingo_unknown"              # Tiingo does not know it (30-day recheck)
STATUS_INCOMPLETE = "history_incomplete"       # retryable, with backoff
STATUS_REBASE = "adjustment_rebase_required"   # fail-closed: adjusted basis moved
STATUS_CONFLICT = "history_conflict"           # fail-closed: raw closes differ
_STATUSES = (STATUS_COMPLETE, STATUS_UNKNOWN, STATUS_INCOMPLETE, STATUS_REBASE, STATUS_CONFLICT)

# One bounded query: the active-fact (cik, symbol) pairs of the US ticker shape,
# each probed at the run date and the year-ends; a line counts once its listing
# resolved at any probe. sec_foreign_listing_at is a large SQL function that the
# planner inlines, so the caller disables JIT for the transaction (CI servers
# have a JIT provider; production does not).
FOREIGN_LISTING_SQL = """
    WITH lines AS MATERIALIZED (
        SELECT DISTINCT cik, symbol
        FROM public.sec_foreign_listing_evidence
        WHERE retired_on IS NULL
          AND symbol ~ %(pattern)s
    ), probe AS (
        SELECT DISTINCT unnest(%(dates)s::date[]) AS as_of
    )
    SELECT DISTINCT l.symbol
    FROM lines l
    CROSS JOIN probe p
    CROSS JOIN LATERAL public.sec_foreign_listing_at(l.cik, l.symbol, p.as_of) r
    WHERE r.listing_status = 'resolved'
    ORDER BY 1
"""

# Created and migrated by the worker (like eod_warmer_cursor), under the
# warmer's advisory lock. CREATE TABLE IF NOT EXISTS does not migrate an older
# shape, so every column is also added if missing, the status CHECK is replaced
# when it lacks a status, and the result is verified (ensure_status_table).
# column → (information_schema data_type, is_nullable)
_STATUS_COLUMNS: dict[str, tuple[str, str]] = {
    "ticker": ("text", "NO"), "source": ("text", "NO"), "status": ("text", "NO"),
    "detail": ("text", "YES"), "history_start": ("date", "YES"),
    "complete_through": ("date", "YES"),
    "retry_after": ("timestamp with time zone", "YES"),
    "attempts": ("integer", "NO"), "checked_at": ("timestamp with time zone", "NO"),
}
_STATUS_NOT_NULL = tuple(c for c, (_, nullable) in _STATUS_COLUMNS.items() if nullable == "NO")
_STATUS_CHECK = "eod_warmer_ticker_status_status_check"
_STATUS_CHECK_SQL = "CHECK (status IN ({}))".format(", ".join(f"'{s}'" for s in _STATUSES))
_STATUS_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS eod_warmer_ticker_status (
    ticker         text        PRIMARY KEY,
    source         text        NOT NULL,
    status         text        NOT NULL,
    detail         text,
    history_start  date,
    -- NULL: verified to Tiingo's end at checked_at (the ring keeps the tail).
    -- A date: a historical (as-of) run verified only through it.
    complete_through date,
    retry_after    timestamptz,
    attempts       integer     NOT NULL DEFAULT 0,
    checked_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT {_STATUS_CHECK} {_STATUS_CHECK_SQL}
);
ALTER TABLE eod_warmer_ticker_status
    ADD COLUMN IF NOT EXISTS detail        text,
    ADD COLUMN IF NOT EXISTS history_start date,
    ADD COLUMN IF NOT EXISTS complete_through date,
    ADD COLUMN IF NOT EXISTS retry_after   timestamptz,
    ADD COLUMN IF NOT EXISTS attempts      integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS checked_at    timestamptz NOT NULL DEFAULT now();
"""
_STATUS_GRANTS_SQL = """
DO $$
DECLARE reader text;
BEGIN
    FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
            EXECUTE format('GRANT SELECT ON TABLE eod_warmer_ticker_status TO %I', reader);
        END IF;
    END LOOP;
END $$;
"""

RECORD_STATUS_SQL = """
    INSERT INTO eod_warmer_ticker_status
        (ticker, source, status, detail, history_start, complete_through, retry_after,
         attempts, checked_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
    ON CONFLICT (ticker) DO UPDATE SET
        source = EXCLUDED.source,
        status = EXCLUDED.status,
        detail = EXCLUDED.detail,
        history_start = EXCLUDED.history_start,
        complete_through = EXCLUDED.complete_through,
        retry_after = EXCLUDED.retry_after,
        attempts = EXCLUDED.attempts,
        checked_at = EXCLUDED.checked_at
"""

# Insert a Tiingo-described instrument, or fill only the NULL metadata of an
# existing row: a non-null field is never overwritten, asset_type of an existing
# row is left alone, and a row with nothing to fill is not touched at all.
SEED_LISTING_INSTRUMENT_SQL = """
    INSERT INTO instruments
        (ticker, name, exchange_code, asset_type, tiingo_start_date, tiingo_end_date)
    VALUES (%(ticker)s, %(name)s, %(exchange_code)s, 'stock', %(start)s, %(end)s)
    ON CONFLICT (ticker) DO UPDATE SET
        name = COALESCE(instruments.name, EXCLUDED.name),
        exchange_code = COALESCE(instruments.exchange_code, EXCLUDED.exchange_code),
        tiingo_start_date = COALESCE(instruments.tiingo_start_date, EXCLUDED.tiingo_start_date),
        tiingo_end_date = COALESCE(instruments.tiingo_end_date, EXCLUDED.tiingo_end_date),
        updated_at = now()
    WHERE (instruments.name IS NULL AND EXCLUDED.name IS NOT NULL)
       OR (instruments.exchange_code IS NULL AND EXCLUDED.exchange_code IS NOT NULL)
       OR (instruments.tiingo_start_date IS NULL AND EXCLUDED.tiingo_start_date IS NOT NULL)
       OR (instruments.tiingo_end_date IS NULL AND EXCLUDED.tiingo_end_date IS NOT NULL)
    RETURNING (xmax = 0) AS inserted
"""

# Only dates the ticker does not have are inserted, so there is never a conflict
# to update; DO NOTHING keeps existing rows untouched even under a race. A key
# outside the ticker's compressed batches' date ranges decompresses nothing; an
# interior gap decompresses only the ticker's own batch around it.
EOD_HISTORY_INSERT_SQL = """
    INSERT INTO eod_prices (
        ticker, date, open, high, low, close, volume,
        adj_open, adj_high, adj_low, adj_close, adj_volume, div_cash, split_factor
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, date) DO NOTHING
"""

@dataclass(frozen=True)
class HistoryTask:
    """One W1c-covered ticker whose history is not recorded complete."""

    ticker: str
    instrument: dict[str, Any] | None   # name, exchange_code, tiingo_start/end_date
    min_date: _dt.date | None           # earliest eod_prices row, None = no rows
    status: str | None = None           # previous status, if any
    attempts: int = 0                   # consecutive retryable failures
    next_attempt: _dt.datetime | None = None  # previous retry_after (None = never tried)


def retry_after(now: _dt.datetime, attempts: int) -> _dt.datetime:
    """Backoff for the ``attempts``-th consecutive retryable failure: 12 h,
    24 h, 48 h … capped at seven days."""
    hours = min(RETRY_BASE_HOURS * 2 ** max(attempts - 1, 0), RETRY_MAX_HOURS)
    return now + _dt.timedelta(hours=hours)


def verification_interval(
    start: _dt.date, meta_end: _dt.date | None, stored: dict[_dt.date, Any],
    as_of: _dt.date,
) -> tuple[_dt.date, _dt.date]:
    """The interval a verification pass must cover, fixed BEFORE the fetch:
    ``[max(startDate, CALENDAR_SUPPORTED_FROM), min(as_of, max(meta endDate,
    last stored date in scope))]`` (``as_of`` when neither exists). The request
    asks for exactly this interval, and ``validate_series`` holds the response
    to it. Nothing after ``as_of`` or before the certified calendar domain is in
    scope: those dates are never requested, compared or inserted."""
    in_scope = [d for d in stored if CALENDAR_SUPPORTED_FROM <= d <= as_of]
    ends = [d for d in (meta_end, max(in_scope) if in_scope else None) if d is not None]
    return max(start, CALENDAR_SUPPORTED_FROM), min(as_of, max(ends)) if ends else as_of


def classify_history_task(task: HistoryTask) -> str:
    """``new`` (no rows: one full cold load) or ``existing`` (rows: verified
    against Tiingo's full range; missing dates inserted only on one basis)."""
    return "new" if task.min_date is None else "existing"


def history_cap(history_limit: int | None, limit: int | None) -> int:
    """Per-run full-history ticker cap: explicit > env > default, never above ``limit``."""
    cap = history_limit
    if cap is None:
        raw = os.getenv(HISTORY_LIMIT_ENV, "").strip()
        if raw:
            try:
                cap = int(raw)
            except ValueError as exc:
                raise ValueError(f"{HISTORY_LIMIT_ENV}={raw!r} is not an integer") from exc
        else:
            cap = HISTORY_TICKERS_PER_RUN
    if cap < 0:
        raise ValueError(f"history cap must be >= 0 (got {cap})")
    if limit:
        cap = min(cap, limit)
    return cap


def _parse_meta_date(value: Any) -> _dt.date | None:
    """A metadata date in exactly the forms the bar validator accepts
    (``YYYY-MM-DD`` or ``YYYY-MM-DDT00:00:00[.000]Z``); None when absent or
    malformed. Nothing is truncated or coerced."""
    return parse_bar_date(value)


def _meta_date_malformed(value: Any) -> bool:
    """Present, but not in a documented form. Absent (None or "") is not
    malformed: it keeps its own path."""
    return value is not None and value != "" and parse_bar_date(value) is None


# ──────────────────────────────────────────────────────────────────────────────
# Pure helpers
# ──────────────────────────────────────────────────────────────────────────────
def build_eod_rows(ticker: str, bars: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    """Tiingo daily bars → eod_prices tuples ``(ticker, date, …12 price cols)``.

    Every ``eod_prices`` column is NOT NULL and numeric, so an element that is
    not a mapping, has no parseable date, misses a field, or carries a value
    that is not a finite number in range (``_valid_values``) is dropped rather
    than stored. The history phase treats any dropped element as an unusable
    response."""
    rows: list[tuple[Any, ...]] = []
    for bar in bars:
        if not isinstance(bar, Mapping):
            continue
        try:
            day = _dt.date.fromisoformat(str(bar["date"])[:10])
            values = [bar[_BAR_KEYS[col]] for col in _EOD_COLUMNS]
        except (KeyError, ValueError, TypeError):
            continue
        if not _valid_values(dict(zip(_EOD_COLUMNS, values))):
            continue
        rows.append((ticker, day, *values))
    return rows


_PRICE_COLUMNS = ("open", "high", "low", "close", "adj_open", "adj_high", "adj_low", "adj_close")
_NON_NEGATIVE_COLUMNS = ("volume", "adj_volume", "div_cash")


def _is_number(value: Any) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value))


def _valid_values(values: dict[str, Any]) -> bool:
    """Numeric (not bool, not a numeric string), finite, prices and the split
    factor positive, volumes and dividends non-negative."""
    if not all(_is_number(v) for v in values.values()):
        return False
    return (all(values[c] > 0 for c in _PRICE_COLUMNS)
            and all(values[c] >= 0 for c in _NON_NEGATIVE_COLUMNS)
            and values["split_factor"] > 0)


# ──────────────────────────────────────────────────────────────────────────────
# DB I/O
# ──────────────────────────────────────────────────────────────────────────────
def warming_universe(conn, *, extra: tuple[str, ...] = INDEX_TICKERS) -> list[str]:
    """Active screener tickers + already-known EOD tickers + benchmark ETFs.

    W1c-covered foreign lines join through the ``eod_prices`` term once they
    have rows; the ring itself is unchanged by that source."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker FROM universe_constituents WHERE status = 'active'
            UNION
            SELECT DISTINCT ticker FROM eod_prices
            """
        )
        tickers = {r[0] for r in cur.fetchall()}
    tickers.update(extra)
    return sorted(tickers)


def order_sweep(
    tickers: list[str],
    *,
    resume_after: str | None = None,
    priority: tuple[str, ...] = PRIORITY_TICKERS,
) -> list[str]:
    """Priority head first, then the tail rotated to start after the last cursor.

    A run that dies on budget must not leave the same suffix cold forever. The
    tail is rotated rather than truncated so every ticker is still reached — just
    over several runs — and the rotation wraps, so the sweep is a ring and no
    ticker can starve. ``priority`` is exempt from the rotation entirely: those
    are re-fetched on every run regardless of where the cursor sits.
    """
    known = set(tickers)
    head = [t for t in priority if t in known]
    tail = [t for t in tickers if t not in set(head)]
    if resume_after is not None and tail:
        # bisect-free and tolerant of a cursor whose ticker has since left the
        # universe: split on the first entry that sorts after it.
        cut = next((i for i, t in enumerate(tail) if t > resume_after), len(tail))
        tail = tail[cut:] + tail[:cut]
    return head + tail


def read_cursor(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
        cur.execute("SELECT last_ticker FROM eod_warmer_cursor WHERE worker = %s", (_CURSOR_KEY,))
        row = cur.fetchone()
    conn.commit()
    return row[0] if row else None


def write_cursor(conn, ticker: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO eod_warmer_cursor (worker, last_ticker, updated_at)
               VALUES (%s, %s, now())
               ON CONFLICT (worker) DO UPDATE
               SET last_ticker = EXCLUDED.last_ticker, updated_at = now()""",
            (_CURSOR_KEY, ticker),
        )
    conn.commit()


def ensure_instruments(conn, *, extra: tuple[str, ...] = INDEX_TICKERS) -> int:
    """Seed FK parent rows for active screener and benchmark tickers."""
    inserted = 0
    with conn.cursor() as cur:
        cur.execute(SEED_ACTIVE_INSTRUMENTS_SQL)
        inserted += max(cur.rowcount, 0)
        cur.executemany(
            SEED_EXTRA_INSTRUMENT_SQL,
            [(ticker, ticker) for ticker in extra],
        )
        inserted += max(cur.rowcount, 0)
    conn.commit()
    return inserted


def _ticker_watermarks(conn) -> dict[str, _dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT ticker, max(date) FROM eod_prices GROUP BY ticker")
        return {r[0]: r[1] for r in cur.fetchall() if r[1] is not None}


def upsert_eod_prices(conn, rows: list[tuple[Any, ...]]) -> int:
    """Chunked idempotent upsert (per-chunk commit for fault isolation)."""
    upserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), UPSERT_CHUNK):
            chunk = rows[i:i + UPSERT_CHUNK]
            try:
                cur.executemany(EOD_UPSERT_SQL, chunk)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            upserted += len(chunk)
    return upserted


# ──────────────────────────────────────────────────────────────────────────────
# W1c foreign-listing source + full-history phase (DB I/O)
# ──────────────────────────────────────────────────────────────────────────────
class ChunkFootprintExceeded(RuntimeError):
    """The eod_prices chunks in the read range exceed ``MAX_VERIFICATION_CHUNKS``."""


# Chunks of eod_prices overlapping [since, through]: a chunk covers the dates
# [range_start, range_end), read in UTC so the session time zone cannot move a
# boundary. Catalog rows only: no lock on any chunk. 0 without TimescaleDB or
# without the table.
_CHUNK_COUNT_SQL = """
    SELECT count(*) FROM timescaledb_information.chunks c
    WHERE c.hypertable_name = 'eod_prices'
      AND c.hypertable_schema = (SELECT n.nspname FROM pg_catalog.pg_class t
                                 JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                                 WHERE t.oid = to_regclass('eod_prices'))
      AND (c.range_end AT TIME ZONE 'UTC')::date > %(since)s
      AND (c.range_start AT TIME ZONE 'UTC')::date <= %(through)s
"""


def chunk_footprint(conn, since: _dt.date, through: _dt.date) -> int:
    """Number of ``eod_prices`` chunks overlapping ``[since, through]``."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('timescaledb_information.chunks') IS NOT NULL")
        if not cur.fetchone()[0]:
            return 0
        cur.execute(_CHUNK_COUNT_SQL, {"since": since, "through": through})
        return cur.fetchone()[0]


def require_chunk_footprint(conn, since: _dt.date, through: _dt.date) -> None:
    """Raise ``ChunkFootprintExceeded`` when the range spans more chunks than
    ``MAX_VERIFICATION_CHUNKS``. The count's own transaction is closed first."""
    chunks = chunk_footprint(conn, since, through)
    conn.commit()
    if chunks > MAX_VERIFICATION_CHUNKS:
        raise ChunkFootprintExceeded(
            f"chunk_footprint_exceeded: {chunks} chunks in {since}..{through}, "
            f"limit {MAX_VERIFICATION_CHUNKS}")


def foreign_listing_tickers(conn, as_of: _dt.date) -> list[str] | None:
    """Distinct US-shaped symbols of W1c lines resolved at ``as_of`` or a year-end.

    ``None`` when the W1c resolver is not installed (the ring still runs). JIT
    is off and the statement timeout is ``FOREIGN_DISCOVERY_TIMEOUT_MS`` for
    this transaction only."""
    # No look-ahead: a historical run probes only year-ends on or before as_of.
    dates = sorted({as_of, *(d for d in FOREIGN_LISTING_YEAR_ENDS if d <= as_of)})
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT set_config('statement_timeout', %s, true)",
                    (f"{FOREIGN_DISCOVERY_TIMEOUT_MS}ms",))
        cur.execute("SELECT to_regprocedure(%s) IS NOT NULL", (_W1C_RESOLVER,))
        if not cur.fetchone()[0]:
            return None
        cur.execute("SET LOCAL jit = off")
        cur.execute(
            FOREIGN_LISTING_SQL,
            {"pattern": US_LISTING_TICKER_PATTERN, "dates": dates},
        )
        symbols = [r[0] for r in cur.fetchall()]
    # The SQL filter is authoritative; this keeps the Python constant honest.
    return [s for s in symbols if _US_LISTING_TICKER.match(s)]


def ensure_status_table(conn) -> None:
    """Create or migrate ``eod_warmer_ticker_status``, then verify its shape.

    Every run: add any missing column, set NOT NULL on the required columns,
    drop every CHECK that mentions ``status`` and add exactly the expected one,
    grant SELECT to the readers. Then verify through ``information_schema``
    that every column has its expected type and nullability, and probe the
    CHECK (each status inserts, ``'bogus'`` and NULL are rejected; probe rows
    are rolled back). Anything else raises ``RuntimeError``."""
    with conn.cursor() as cur:
        cur.execute(_STATUS_SCHEMA_SQL)
        for col in _STATUS_NOT_NULL:
            cur.execute(
                psycopg.sql.SQL("ALTER TABLE eod_warmer_ticker_status ALTER COLUMN {} SET NOT NULL")
                .format(psycopg.sql.Identifier(col))
            )
        cur.execute(
            """SELECT conname FROM pg_constraint
               WHERE conrelid = 'eod_warmer_ticker_status'::regclass AND contype = 'c'
                 AND pg_get_constraintdef(oid) LIKE '%%status%%'"""
        )
        for (name,) in cur.fetchall():
            cur.execute(
                psycopg.sql.SQL("ALTER TABLE eod_warmer_ticker_status DROP CONSTRAINT {}")
                .format(psycopg.sql.Identifier(name))
            )
        cur.execute(
            f"ALTER TABLE eod_warmer_ticker_status ADD CONSTRAINT {_STATUS_CHECK} "
            f"{_STATUS_CHECK_SQL}"
        )
        cur.execute(_STATUS_GRANTS_SQL)
        cur.execute(
            """SELECT column_name, data_type, is_nullable FROM information_schema.columns
               WHERE table_name = 'eod_warmer_ticker_status'
                 AND table_schema = (SELECT n.nspname FROM pg_class c
                                     JOIN pg_namespace n ON n.oid = c.relnamespace
                                     WHERE c.oid = 'eod_warmer_ticker_status'::regclass)"""
        )
        columns = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    wrong = {c: columns.get(c) for c, shape in _STATUS_COLUMNS.items() if columns.get(c) != shape}
    accepted = {s for s in (*_STATUSES, "bogus", None) if _status_accepted(conn, s)}
    if wrong or accepted != set(_STATUSES):
        conn.rollback()
        raise RuntimeError(
            f"eod_warmer_ticker_status has an unexpected shape: columns {wrong or 'ok'}, "
            f"accepted statuses {sorted(accepted, key=str)}"
        )
    conn.commit()


def _status_accepted(conn, status: str | None) -> bool:
    """Does the table accept ``status``? Probed in a savepoint, always rolled back."""
    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "INSERT INTO eod_warmer_ticker_status (ticker, source, status)"
                " VALUES ('__status_probe__', 'probe', %s)",
                (status,),
            )
            raise psycopg.Rollback()
    except (psycopg.errors.CheckViolation, psycopg.errors.NotNullViolation):
        return False
    return True


def read_ticker_status(conn, tickers: list[str]) -> dict[str, dict[str, Any]]:
    """Recorded history state per ticker; ``{}`` before the table exists
    (a read-only preview never creates it)."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('eod_warmer_ticker_status') IS NOT NULL")
        if not cur.fetchone()[0]:
            return {}
        cur.execute(
            """SELECT ticker, status, detail, history_start, retry_after, attempts,
                      checked_at, complete_through
               FROM eod_warmer_ticker_status WHERE ticker = ANY(%s)""",
            (list(tickers),),
        )
        return {
            r[0]: {"status": r[1], "detail": r[2], "history_start": r[3],
                   "retry_after": r[4], "attempts": r[5], "checked_at": r[6],
                   "complete_through": r[7]}
            for r in cur.fetchall()
        }


_NEVER = _dt.datetime.min.replace(tzinfo=_dt.UTC)


def plan_foreign_history(
    conn, tickers: list[str], *, now: _dt.datetime, as_of: _dt.date,
) -> dict[str, Any]:
    """Split the covered tickers into complete / waiting / pending (read-only).

    ``complete``: recorded ``history_complete``, still has rows, verified within
    ``REVERIFY_DAYS``, and — if a historical run verified it only through
    ``complete_through`` — through at least this run's ``as_of``.
    ``waiting``: a non-complete status whose ``retry_after`` is still ahead.
    ``pending``: everything else, as ``HistoryTask``s ordered by (next attempt,
    ticker) — never-tried tickers first, then the longest-waiting retries and
    due re-verifications — so a ticker that keeps failing rotates behind the
    others instead of holding a cap slot. Completion is only ever the recorded
    status."""
    status = read_ticker_status(conn, tickers)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT ticker, name, exchange_code, tiingo_start_date, tiingo_end_date
               FROM instruments WHERE ticker = ANY(%s)""",
            (list(tickers),),
        )
        instruments = {
            r[0]: {"name": r[1], "exchange_code": r[2],
                   "tiingo_start_date": r[3], "tiingo_end_date": r[4]}
            for r in cur.fetchall()
        }
        cur.execute(
            "SELECT ticker, min(date) FROM eod_prices WHERE ticker = ANY(%s) GROUP BY ticker",
            (list(tickers),),
        )
        mins = {r[0]: r[1] for r in cur.fetchall() if r[1] is not None}
    complete: list[str] = []
    waiting: dict[str, list[str]] = {}
    pending: list[HistoryTask] = []
    reverify_after = _dt.timedelta(days=REVERIFY_DAYS)
    for ticker in sorted(set(tickers)):
        state = status.get(ticker)
        next_attempt = state["retry_after"] if state else None
        if state and state["status"] == STATUS_COMPLETE:
            through = state["complete_through"]
            if (ticker in mins and state["checked_at"] + reverify_after > now
                    and (through is None or through >= as_of)):
                complete.append(ticker)
                continue
            # Due again: rows gone, verification aged, or only verified through
            # an earlier historical as-of. Queued behind never-tried tickers.
            next_attempt = state["checked_at"]
        elif (state and state["retry_after"] is not None and state["retry_after"] > now):
            waiting.setdefault(state["status"], []).append(ticker)
            continue
        pending.append(HistoryTask(
            ticker, instruments.get(ticker), mins.get(ticker),
            status=state["status"] if state else None,
            attempts=(state["attempts"] or 0) if state else 0,
            next_attempt=next_attempt,
        ))
    pending.sort(key=lambda t: (t.next_attempt or _NEVER, t.ticker))
    return {"complete": complete, "waiting": waiting, "pending": pending}


def record_ticker_status(
    conn, ticker: str, status: str, *, detail: str | None = None,
    history_start: _dt.date | None = None, retry_after: _dt.datetime | None = None,
    attempts: int = 0,
) -> None:
    """Record a NON-completion outcome. ``history_complete`` is written only by
    ``promote``, in the same transaction as the rows it certifies."""
    if status == STATUS_COMPLETE:
        raise ValueError("history_complete is written only by promote()")
    with conn.cursor() as cur:
        cur.execute(
            RECORD_STATUS_SQL,
            (ticker, FOREIGN_LISTING_SOURCE, status, detail, history_start, None,
             retry_after, attempts),
        )
    conn.commit()


def seed_listing_instrument(conn, ticker: str, meta: dict[str, Any]) -> str | None:
    """``inserted`` / ``filled`` / None (existing row had nothing to fill)."""
    with conn.cursor() as cur:
        cur.execute(
            SEED_LISTING_INSTRUMENT_SQL,
            {
                "ticker": ticker,
                "name": (meta.get("name") or None),
                "exchange_code": (meta.get("exchangeCode") or None),
                "start": _parse_meta_date(meta.get("startDate")),
                "end": _parse_meta_date(meta.get("endDate")),
            },
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return "inserted" if row[0] else "filled"


def promote(
    conn, ticker: str, verdict: Verdict, *, through: _dt.date, history_start: _dt.date,
    complete_through: _dt.date | None = None, since: _dt.date = CALENDAR_SUPPORTED_FROM,
    note: str | None = None,
) -> int | None:
    """Certify a successful verdict: the ONLY writer of history rows and of
    ``history_complete`` / ``complete_through``.

    One transaction: lock the ticker's ``instruments`` row ``FOR UPDATE``
    (other writers' foreign-key inserts into ``eod_prices`` for the ticker and
    Light's metadata upsert wait), re-read the stored rows in
    ``[since, through]`` — the same twelve-field projection the verdict judged —
    and recompute their digest. If it differs from the snapshot the verdict
    judged, nothing is written and None is returned (the caller records
    ``stored_rows_changed_during_pass``). Otherwise insert exactly the
    verdict's rows (possibly none, for ``complete_nothing_to_insert``) with
    ``INSERT … DO NOTHING`` in ``UPSERT_CHUNK`` batches and write
    ``history_complete``, atomically; returns rows inserted. A crash or error at
    any point leaves neither rows nor status. Before it touches anything it
    raises ``ChunkFootprintExceeded`` when ``[since, through]`` spans more than
    ``MAX_VERIFICATION_CHUNKS`` chunks."""
    if verdict.status not in SUCCESS_VERDICTS:
        raise ValueError(f"promote() takes a certifying verdict, not {verdict.status}")
    conn.commit()  # close any read transaction; this one holds only the promotion
    require_chunk_footprint(conn, since, through)
    inserted = 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM instruments WHERE ticker = %s FOR UPDATE", (ticker,))
            current = _stored_rows(conn, ticker, since=since, through=through)
            if stored_digest(current) != verdict.digest:
                conn.rollback()
                return None
            for i in range(0, len(verdict.rows), UPSERT_CHUNK):
                cur.executemany(EOD_HISTORY_INSERT_SQL, list(verdict.rows[i:i + UPSERT_CHUNK]))
                inserted += max(cur.rowcount, 0)
            if inserted != len(verdict.rows):  # a row appeared despite the lock
                conn.rollback()
                return None
            detail = f"{verdict.reason}; history_from={history_start}"
            if note:
                detail += f"; {note}"
            cur.execute(
                RECORD_STATUS_SQL,
                (ticker, FOREIGN_LISTING_SOURCE, STATUS_COMPLETE, detail, history_start,
                 complete_through, None, 0),
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return inserted


def _stored_rows(
    conn, ticker: str, *, through: _dt.date, since: _dt.date = CALENDAR_SUPPORTED_FROM,
) -> dict[_dt.date, dict[str, Any]]:
    """The stored-row snapshot: every ``STORED_FIELDS`` column of the ticker's
    rows in ``[since, through]``. The verification read and promote's locked
    re-read both use exactly this projection."""
    with conn.cursor() as cur:
        cur.execute(
            psycopg.sql.SQL(
                "SELECT date, {} FROM eod_prices WHERE ticker = %s AND date BETWEEN %s AND %s"
            ).format(psycopg.sql.SQL(", ").join(map(psycopg.sql.Identifier, STORED_FIELDS))),
            (ticker, since, through),
        )
        return {r[0]: dict(zip(STORED_FIELDS, r[1:])) for r in cur.fetchall()}


def cover_foreign_history(
    conn,
    tiingo: TiingoClient,
    tickers: list[str],
    *,
    as_of: _dt.date,
    cap: int,
    now: _dt.datetime | None = None,
) -> dict[str, Any]:
    """Give up to ``cap`` covered tickers their one verification pass.

    Per ticker: a meta call (fresh Tiingo startDate; fills NULL instruments
    metadata), then ONE price request for ``verification_interval`` (fixed
    before the fetch), judged by ``validate_series``. ``complete_nothing_to_insert``
    and ``load`` are certified by ``promote`` (rows and status in one locked
    transaction, refused if the stored rows changed). ``incomplete`` and transient
    Tiingo failures (5xx, timeouts, invalid bodies or elements) and any
    unexpected exception for the ticker are ``history_incomplete`` with backoff
    (12 h, doubling, capped at 7 days) and count as errors; 429s
    are account-wide, so they count as errors without a backoff and the 30×429
    breaker aborts the phase. ``conflict`` / ``rebase`` fail closed: nothing
    inserted, reported, rechecked after 30 days."""
    now = now or _dt.datetime.now(_dt.UTC)
    ensure_status_table(conn)
    plan = plan_foreign_history(conn, tickers, now=now, as_of=as_of)
    conn.commit()   # the plan's reads end here: no table lock is held across HTTP
    queue = plan["pending"]
    stats: dict[str, Any] = {
        "source_tickers": len(set(tickers)),
        "already_complete": len(plan["complete"]),
        "waiting": {k: len(v) for k, v in sorted(plan["waiting"].items())},
        "pending": len(queue),
        "processed": 0, "meta_requests": 0, "history_fetches": 0,
        "instruments_inserted": 0, "instruments_filled": 0,
        "history_rows": 0, "completed": 0, "verified_without_insert": 0, "errors": 0,
        "chunk_footprint_exceeded": 0,
    }
    recheck = now + _dt.timedelta(days=UNKNOWN_RECHECK_DAYS)
    unknown_now: list[str] = []
    fail_closed: dict[str, str] = {}
    errors: dict[str, str] = {}

    def retry_later(task: HistoryTask, start: _dt.date | None, reason: str) -> None:
        attempts = task.attempts + 1 if task.status == STATUS_INCOMPLETE else 1
        record_ticker_status(conn, task.ticker, STATUS_INCOMPLETE, detail=reason,
                             history_start=start, retry_after=retry_after(now, attempts),
                             attempts=attempts)
        errors[task.ticker] = reason

    for task in queue:
        if stats["processed"] >= cap:
            break
        ticker = task.ticker
        start: _dt.date | None = None
        try:
            # Before any request or read: a range of too many chunks is not
            # attempted at all (promote repeats the check).
            require_chunk_footprint(conn, CALENDAR_SUPPORTED_FROM, as_of)
            stats["meta_requests"] += 1
            status, meta = tiingo.fetch_meta_result(ticker)
            if status == "not_found":
                record_ticker_status(conn, ticker, STATUS_UNKNOWN, detail="not_found",
                                     retry_after=recheck)
                unknown_now.append(ticker)
                continue
            if status in ("rate_limited", "not_configured"):
                # Account-wide 429s or no API key: not this ticker's fault, so
                # nothing is recorded and it stays pending (the 429 breaker
                # aborts the phase if they persist).
                errors[ticker] = f"meta:{status}"
                continue
            if status != "found" or meta is None:
                retry_later(task, None, f"meta:{status}")
                continue
            malformed = next((name for name, key in (("start", "startDate"), ("end", "endDate"))
                              if _meta_date_malformed(meta.get(key))), None)
            if malformed:
                # Never truncated into a boundary: retried with backoff, and
                # nothing is seeded from it.
                retry_later(task, None, f"meta:malformed_{malformed}_date")
                continue
            start = _parse_meta_date(meta.get("startDate"))
            if start is None:
                record_ticker_status(conn, ticker, STATUS_UNKNOWN, detail="no_start_date",
                                     retry_after=recheck)
                unknown_now.append(ticker)
                continue
            seeded = seed_listing_instrument(conn, ticker, meta)
            if seeded:
                stats[f"instruments_{seeded}"] += 1
            meta_end = _parse_meta_date(meta.get("endDate"))
            if meta_end is None:
                retry_later(task, start, "meta_without_end_date")
                continue
            # The interval, and so every obligation, is fixed before the fetch.
            # Rows after as_of are outside this run: never compared or touched.
            stored = _stored_rows(conn, ticker, since=CALENDAR_SUPPORTED_FROM, through=as_of)
            # The snapshot is in memory: end its read transaction NOW. Left open
            # it holds an AccessShareLock on every chunk and index of the range
            # across the price request, its retries and every continuation.
            # promote() locks the instrument, re-reads and compares the digest.
            conn.commit()
            tiingo_start = start
            start, end = verification_interval(start, meta_end, stored, as_of)
            # A historical run that stops before Tiingo's end completes only
            # through its interval end; a later run re-verifies the tail.
            complete_through = end if meta_end > end else None
            if start > end:
                retry_later(task, start, f"start_date_after_interval_end: {start} > {end}")
                continue
            stats["history_fetches"] += 1
            status, bars = tiingo.fetch_daily_bars_result(ticker, start, end)
            if status == "not_found":
                record_ticker_status(conn, ticker, STATUS_UNKNOWN, detail="prices_not_found",
                                     retry_after=recheck)
                unknown_now.append(ticker)
                continue
            if status in ("rate_limited", "not_configured"):
                errors[ticker] = f"prices:{status}"
                continue
            if status not in ("success_new", "empty"):
                retry_later(task, start, f"prices:{status}")
                continue
            verdict = validate_series(ticker, (start, end), bars, stored, xnys_calendar())
            if verdict.status == VERDICT_INCOMPLETE:
                retry_later(task, start, verdict.reason)
                continue
            if verdict.status in (VERDICT_CONFLICT, VERDICT_REBASE):
                record_ticker_status(conn, ticker, verdict.status, detail=verdict.reason,
                                     history_start=start, retry_after=recheck)
                fail_closed[ticker] = verdict.reason
                continue
            inserted = promote(
                conn, ticker, verdict, through=as_of, history_start=start,
                complete_through=complete_through, since=CALENDAR_SUPPORTED_FROM,
                note=(f"tiingo_start={tiingo_start} (earlier history not certified)"
                      if tiingo_start < start else None))
            if inserted is None:
                retry_later(task, start, "stored_rows_changed_during_pass")
                continue
            stats["history_rows"] += inserted
            if not verdict.rows:
                stats["verified_without_insert"] += 1
            stats["completed"] += 1
        except ChunkFootprintExceeded as exc:
            conn.rollback()
            stats["chunk_footprint_exceeded"] += 1
            retry_later(task, start, str(exc))
        except TiingoBudgetExceeded as exc:
            # No price row was written for this ticker; it stays pending.
            stats["aborted"] = str(exc)
            break
        except Exception as exc:  # noqa: BLE001 — one ticker never stops the phase
            # Anything unforeseen (a malformed value the database rejects, a
            # parsing surprise) leaves this ticker retryable with backoff and
            # moves on; promote already rolled its transaction back.
            conn.rollback()
            retry_later(task, start, f"unexpected:{type(exc).__name__}")
        finally:
            if "aborted" not in stats:
                stats["processed"] += 1
    stats["errors"] = len(errors)
    if errors:
        stats["error_tickers"] = dict(sorted(errors.items())[:50])
    stats["tiingo_unknown"] = len(unknown_now)
    if unknown_now:
        stats["tiingo_unknown_tickers"] = sorted(unknown_now)[:100]
    stats["fail_closed"] = len(fail_closed)
    if fail_closed:
        stats["fail_closed_tickers"] = dict(sorted(fail_closed.items())[:100])
    # Deferred = pending tickers that did not settle this run: never attempted,
    # retrying with backoff, or blocked by 429s / a missing key. Only a recorded
    # history_complete, fail-closed or tiingo_unknown status settles a ticker.
    stats["deferred"] = len(queue) - (stats["completed"] + len(fail_closed) + len(unknown_now))
    return stats


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None,
        history_limit: int | None = None) -> dict:
    """Refresh eod_prices from Tiingo for every ticker in the warming universe,
    then bring W1c-covered foreign listings to full history (capped per run)."""
    as_of = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()
    cap = history_cap(history_limit, limit)
    now = _dt.datetime.now(_dt.UTC)
    fetched = upserted = skipped_rows = 0
    aborted: str | None = None
    last_done: str | None = None
    history: dict[str, Any] | None = None
    source_error: str | None = None

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_EOD_PRICES_WARMER) as got:
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}

            # Before any read of it, so an older shape is migrated (or refused).
            ensure_status_table(conn)
            instruments_seeded = ensure_instruments(conn)
            # The history source is optional: whatever its discovery raises (the
            # resolver or its evidence relation not deployed, a permission, the
            # statement timeout) never stops the ring. The failed transaction is
            # rolled back, the type is reported, the history phase is skipped.
            foreign: list[str] | None = None
            try:
                foreign = foreign_listing_tickers(conn, as_of)
            except Exception as exc:  # noqa: BLE001 — isolation is the point
                conn.rollback()
                source_error = type(exc).__name__
            resume_after = read_cursor(conn)
            universe = warming_universe(conn)
            tickers = order_sweep(universe, resume_after=resume_after)
            watermarks = _ticker_watermarks(conn)
            conn.commit()   # the universe and watermark reads end before any request
            if limit:
                tickers = tickers[:limit]
            print(
                f"eod_prices_warmer: {len(tickers)} tickers, as_of={as_of}, "
                f"resume_after={resume_after or '-'}, "
                f"foreign_listing={source_error and 'error' or ('absent' if foreign is None else len(foreign))}",
                flush=True,
            )

            bucket = TokenBucket(max_tokens=FETCH_BURST, refill_rate=FETCH_RATE_PER_S)
            with TiingoClient(bucket=bucket) as tiingo:
                for i, ticker in enumerate(tickers, start=1):
                    watermark = watermarks.get(ticker)
                    if watermark is not None:
                        start = watermark - _dt.timedelta(days=WATERMARK_OVERLAP_DAYS)
                    else:
                        start = as_of - _dt.timedelta(days=NEW_TICKER_LOOKBACK_DAYS)
                    try:
                        bars = tiingo.fetch_daily_bars(ticker, start, as_of)
                    except TiingoBudgetExceeded as exc:
                        aborted = str(exc)
                        break
                    fetched += len(bars)
                    rows = build_eod_rows(ticker, bars)
                    skipped_rows += len(bars) - len(rows)
                    if rows:
                        upserted += upsert_eod_prices(conn, rows)
                    # Advance only past the rotating tail: the priority head is
                    # re-fetched every run, so letting it move the cursor would
                    # rewind the sweep to the head's position on every abort.
                    if ticker not in _PRIORITY_SET:
                        last_done = ticker
                    if i % PROGRESS_EVERY == 0:
                        print(
                            f"eod_prices_warmer: {i}/{len(tickers)} tickers, "
                            f"upserted={upserted}",
                            flush=True,
                        )
                if last_done is not None:
                    write_cursor(conn, last_done)
                # Full history for W1c lines runs after the ring on the same
                # bucket, so it only spends what the ring left and never delays
                # the priority head. A ring that died on budget skips it.
                if foreign and cap > 0 and aborted is None:
                    history = cover_foreign_history(
                        conn, tiingo, foreign, as_of=as_of, cap=cap, now=now,
                    )
                    aborted = history.get("aborted")
            conn.commit()

    stats: dict[str, Any] = {
        "fetched": fetched, "upserted": upserted,
        "tickers": len(tickers), "instruments_seeded": instruments_seeded,
        "as_of": as_of.isoformat(),
    }
    if skipped_rows:
        stats["skipped_rows"] = skipped_rows
    if last_done:
        stats["cursor"] = last_done
    if source_error is not None:
        stats["foreign_history"] = {"source": "error", "reason": source_error, "errors": 1}
    elif foreign is None:
        stats["foreign_history"] = {"source": "absent"}
    elif history is not None:
        stats["foreign_history"] = history
    elif foreign:
        stats["foreign_history"] = {
            "source_tickers": len(foreign),
            "skipped": "aborted" if aborted else "cap_zero",
        }
    else:
        # The resolver exists and no line resolves (an early historical replay,
        # an empty evidence table): reported, so monitoring can tell it from a
        # worker that did not report the phase.
        stats["foreign_history"] = {
            "source": "empty", "source_tickers": 0, "already_complete": 0, "pending": 0,
            "processed": 0, "completed": 0, "errors": 0, "deferred": 0,
        }
    if aborted:
        stats["aborted"] = aborted
    return stats
