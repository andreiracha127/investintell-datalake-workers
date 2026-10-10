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
(``universe_constituents``) does not change. For these tickers the cold start is
Tiingo's own ``startDate`` (from a fresh meta call, which also fills NULL
``instruments`` metadata without overwriting non-null fields):

* no ``eod_prices`` rows → the whole history ``startDate → as_of``.
* rows that start after ``startDate`` (the screener's 745-day cohort) → the
  window runs from ``startDate`` into the first stored sessions. Only the prefix
  older than the first stored row is inserted, and only if Tiingo's raw close
  and adjusted OHLC on the first stored sessions match the stored values; a
  moved adjustment basis (a split or dividend since the stored rows were
  fetched) or a raw difference inserts nothing and is reported as
  ``adjustment_rebase_required`` / ``history_conflict``. Existing rows are
  never rewritten.
* rows that already reach ``startDate`` → nothing to load.

A ticker's history rows and its ``history_complete`` status in
``eod_warmer_ticker_status`` are written in ONE transaction, so an interrupted
load leaves neither and the next run refetches the whole window. Completion is
only ever that recorded status, never inferred from ``min(date)``, and the ring
admits a covered ticker on that status (``ring_excluded``). A response with an
unusable bar or an empty window is ``history_incomplete``: retried after
``INCOMPLETE_RETRY_HOURS`` and counted as an error. Tickers Tiingo does not know
are recorded, reported and re-checked after ``UNKNOWN_RECHECK_DAYS``.

History rows go through ``INSERT … ON CONFLICT DO NOTHING`` for keys strictly
older than the ticker's existing rows. That lands in old, compressed chunks of
the ``eod_prices`` hypertable (segmentby ticker, orderby date DESC) without
decompressing any existing batch, so the TimescaleDB per-transaction
decompression limit is never approached; the columnstore policy recompresses
the partial chunks. The phase runs after the ring, on the same token bucket,
and stops at ``HISTORY_TICKERS_PER_RUN`` tickers (env
``EOD_HISTORY_TICKERS_PER_RUN``; never more than ``limit``). Whatever is still
pending resumes on the next run.

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
import os
import re
from dataclasses import dataclass
from typing import Any

from src.db import LOCK_EOD_PRICES_WARMER, advisory_lock, connect
from src.workers._tiingo import (
    DEFAULT_RATE_PER_S,
    TiingoBudgetExceeded,
    TiingoClient,
    TokenBucket,
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
# price window) and, with the client's three-attempt retry ladder, at most six
# HTTP attempts. The default is the cautious rollout value; raise it with
# EOD_HISTORY_TICKERS_PER_RUN (250 is the steady-state target: <= 1,500 HTTP
# attempts per run on the shared bucket). 0 turns the phase off.
HISTORY_TICKERS_PER_RUN = 25
HISTORY_LIMIT_ENV = "EOD_HISTORY_TICKERS_PER_RUN"
UNKNOWN_RECHECK_DAYS = 30
# A retryable failure (unusable bars, an empty window) is retried after this.
INCOMPLETE_RETRY_HOURS = 12
# Tiingo's startDate may fall on a non-trading day; rows starting within this
# many days of it reach the start.
HISTORY_START_TOLERANCE_DAYS = 5
# A backfill window runs into the existing series so the prefix's adjustment
# basis can be checked against the stored rows: up to OVERLAP_SESSIONS stored
# sessions, fetched within OVERLAP_CALENDAR_DAYS after the first stored date.
OVERLAP_SESSIONS = 10
OVERLAP_CALENDAR_DAYS = 21
BASIS_REL_TOL = 1e-6
_W1C_RESOLVER = "public.sec_foreign_listing_at(bigint,text,date)"

# Status of a covered ticker's history. Only ``history_complete`` admits it to
# the ring; it is written in the same transaction as the history rows.
STATUS_COMPLETE = "history_complete"
STATUS_UNKNOWN = "tiingo_unknown"              # Tiingo does not know it (30-day recheck)
STATUS_INCOMPLETE = "history_incomplete"       # retryable: unusable bars, empty window
STATUS_REBASE = "adjustment_rebase_required"   # fail-closed: adjusted basis moved
STATUS_CONFLICT = "history_conflict"           # fail-closed: raw closes differ
_FAIL_CLOSED = frozenset({STATUS_REBASE, STATUS_CONFLICT})
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

# Created by the worker like eod_warmer_cursor. The warmer connects as
# worker_writer, whose default ACL in public grants only app_analytics_ro, so
# the API and read-only roles get SELECT explicitly (idempotent, existing roles).
_STATUS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS eod_warmer_ticker_status (
    ticker         text        PRIMARY KEY,
    source         text        NOT NULL,
    status         text        NOT NULL
                   CHECK (status IN ('history_complete', 'tiingo_unknown',
                                     'history_incomplete', 'adjustment_rebase_required',
                                     'history_conflict')),
    detail         text,
    history_start  date,
    retry_after    timestamptz,
    checked_at     timestamptz NOT NULL DEFAULT now()
);
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
        (ticker, source, status, detail, history_start, retry_after, checked_at)
    VALUES (%s, %s, %s, %s, %s, %s, now())
    ON CONFLICT (ticker) DO UPDATE SET
        source = EXCLUDED.source,
        status = EXCLUDED.status,
        detail = EXCLUDED.detail,
        history_start = EXCLUDED.history_start,
        retry_after = EXCLUDED.retry_after,
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

# History rows are keys older than everything the ticker already has, so there
# is never a conflict to update; DO NOTHING keeps existing rows untouched even
# if a window ever overlapped. Brand-new keys in a compressed chunk match no
# compressed batch (segmentby ticker, date min/max), so nothing is decompressed.
EOD_HISTORY_INSERT_SQL = """
    INSERT INTO eod_prices (
        ticker, date, open, high, low, close, volume,
        adj_open, adj_high, adj_low, adj_close, adj_volume, div_cash, split_factor
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, date) DO NOTHING
"""

# Stored values the backfill prefix must join: raw close and the adjusted OHLC.
_BASIS_COLUMNS: tuple[str, ...] = ("close", "adj_open", "adj_high", "adj_low", "adj_close")
_ROW_INDEX = {col: i + 2 for i, col in enumerate(_EOD_COLUMNS)}


@dataclass(frozen=True)
class HistoryTask:
    """One W1c-covered ticker whose history is not recorded complete."""

    ticker: str
    instrument: dict[str, Any] | None  # name, exchange_code, tiingo_start/end_date
    min_date: _dt.date | None          # earliest eod_prices row, None = no rows
    status: str | None = None          # previous status, if any


def history_fetch_window(
    start: _dt.date, min_date: _dt.date | None, as_of: _dt.date
) -> tuple[_dt.date, _dt.date] | None:
    """Price window for a covered ticker, or None when nothing is older.

    No rows → ``start → as_of`` (the cold start is Tiingo's startDate, not the
    screener's 745 days). Rows that begin after ``start`` → ``start`` through
    ``OVERLAP_CALENDAR_DAYS`` past the first stored date, so the older prefix
    and the first stored sessions arrive on one basis and can be compared. Rows
    that already reach ``start`` (within the tolerance) → None."""
    if min_date is None:
        return (start, as_of) if start <= as_of else None
    if min_date <= start + _dt.timedelta(days=HISTORY_START_TOLERANCE_DAYS):
        return None
    return start, min(as_of, min_date + _dt.timedelta(days=OVERLAP_CALENDAR_DAYS))


@dataclass(frozen=True)
class BasisCheck:
    """Fetched vs stored values on the first overlapping sessions."""

    verdict: str         # match | adjusted_moved | raw_differs | no_overlap
    sessions: int        # overlapping sessions compared
    ratio: float | None  # median fetched/stored adj_close (close for raw_differs)

    def detail(self) -> str:
        ratio = "n/a" if self.ratio is None else f"{self.ratio:.6f}"
        return f"{self.verdict}: ratio={ratio} over {self.sessions} sessions"


def _close_enough(a: float, b: float) -> bool:
    return abs(a - b) <= BASIS_REL_TOL * max(abs(a), abs(b))


def compare_basis(
    fetched: list[tuple[Any, ...]], stored: dict[_dt.date, dict[str, float]]
) -> BasisCheck:
    """Does the fetched series continue the stored one on the same basis?

    ``fetched`` are ``build_eod_rows`` tuples; ``stored`` maps date → stored
    values of ``_BASIS_COLUMNS``. The first ``OVERLAP_SESSIONS`` common dates
    are compared with a relative tolerance of ``BASIS_REL_TOL``. A raw-close
    difference is a different series (a vendor revision or another security);
    an adjusted-only difference means a split or dividend re-based Tiingo's
    adjusted history after the stored rows were written."""
    common = sorted((r for r in fetched if r[1] in stored), key=lambda r: r[1])
    common = common[:OVERLAP_SESSIONS]
    if not common:
        return BasisCheck("no_overlap", 0, None)

    def median_ratio(col: str) -> float | None:
        values = sorted(r[_ROW_INDEX[col]] / stored[r[1]][col]
                        for r in common if stored[r[1]][col])
        return values[len(values) // 2] if values else None

    if not all(_close_enough(r[_ROW_INDEX["close"]], stored[r[1]]["close"]) for r in common):
        return BasisCheck("raw_differs", len(common), median_ratio("close"))
    if not all(_close_enough(r[_ROW_INDEX[col]], stored[r[1]][col])
               for r in common for col in _BASIS_COLUMNS[1:]):
        return BasisCheck("adjusted_moved", len(common), median_ratio("adj_close"))
    return BasisCheck("match", len(common), 1.0)


def classify_history_task(task: HistoryTask) -> str:
    """``new`` (no rows: one full cold load) or ``existing`` (rows: the older
    prefix, if Tiingo has one, is checked against the stored basis)."""
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
    if not value:
        return None
    try:
        return _dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Pure helpers
# ──────────────────────────────────────────────────────────────────────────────
def build_eod_rows(ticker: str, bars: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    """Tiingo daily bars → eod_prices tuples ``(ticker, date, …12 price cols)``.

    Every ``eod_prices`` column is NOT NULL, so a bar missing any field (or with
    a None value) is dropped rather than violating the schema."""
    rows: list[tuple[Any, ...]] = []
    for bar in bars:
        try:
            day = _dt.date.fromisoformat(str(bar["date"])[:10])
            values = [bar[_BAR_KEYS[col]] for col in _EOD_COLUMNS]
        except (KeyError, ValueError):
            continue
        if any(v is None for v in values):
            continue
        rows.append((ticker, day, *values))
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# DB I/O
# ──────────────────────────────────────────────────────────────────────────────
def warming_universe(conn, *, extra: tuple[str, ...] = INDEX_TICKERS) -> list[str]:
    """Active screener tickers + already-known EOD tickers + benchmark ETFs.

    W1c-covered foreign lines join through the ``eod_prices`` term once the
    full-history phase has loaded them; ``run()`` drops covered lines whose
    history status is not complete (``ring_excluded``), so none takes the
    745-day cold start."""
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


def _screener_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM universe_constituents WHERE status = 'active'")
        return [r[0] for r in cur.fetchall()]


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
def foreign_listing_tickers(conn, as_of: _dt.date) -> list[str] | None:
    """Distinct US-shaped symbols of W1c lines resolved at ``as_of`` or a year-end.

    ``None`` when the W1c resolver is not installed (the ring still runs). JIT
    is off for this transaction only."""
    dates = sorted({as_of, *FOREIGN_LISTING_YEAR_ENDS})
    with conn.transaction(), conn.cursor() as cur:
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
    with conn.cursor() as cur:
        cur.execute(_STATUS_SCHEMA_SQL)
        cur.execute(_STATUS_GRANTS_SQL)
    conn.commit()


def read_ticker_status(conn, tickers: list[str]) -> dict[str, dict[str, Any]]:
    """Recorded history state per ticker; ``{}`` before the table exists
    (a read-only preview never creates it)."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('eod_warmer_ticker_status') IS NOT NULL")
        if not cur.fetchone()[0]:
            return {}
        cur.execute(
            """SELECT ticker, status, detail, history_start, retry_after, checked_at
               FROM eod_warmer_ticker_status WHERE ticker = ANY(%s)""",
            (list(tickers),),
        )
        return {
            r[0]: {"status": r[1], "detail": r[2], "history_start": r[3],
                   "retry_after": r[4], "checked_at": r[5]}
            for r in cur.fetchall()
        }


def ring_excluded(
    foreign: list[str], status: dict[str, dict[str, Any]], *,
    ring_owned: frozenset[str], watermarks: dict[str, _dt.date],
) -> frozenset[str]:
    """Covered tickers the ring must not touch this run.

    Admission comes from the durable status, never from rows: a covered ticker
    joins the ring once its history is ``history_complete``. Two exceptions
    keep existing series fresh without touching history: a screener/benchmark
    ticker that already has rows (the ring served it before this source
    existed), and a fail-closed ticker (rebase/conflict) whose existing rows the
    ring kept fresh before. Everything else open — no status, retryable,
    unknown — waits for the history phase, so nothing is cold-started with the
    745-day window and a failed load is never refreshed into looking complete."""
    settled = {t for t, s in status.items()
               if s["status"] == STATUS_COMPLETE or s["status"] in _FAIL_CLOSED}
    return frozenset(
        t for t in foreign
        if t not in settled and not (t in ring_owned and t in watermarks)
    )


def plan_foreign_history(
    conn, tickers: list[str], *, now: _dt.datetime
) -> dict[str, Any]:
    """Split the covered tickers into complete / waiting / pending (read-only).

    ``complete``: recorded ``history_complete`` and still has rows.
    ``waiting``: a non-complete status whose ``retry_after`` is still ahead.
    ``pending``: everything else, as ``HistoryTask``s sorted by ticker. The
    resume point is simply whatever is still pending; completion is only ever
    the recorded status, never inferred from the rows."""
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
    for ticker in sorted(set(tickers)):
        state = status.get(ticker)
        if state and state["status"] == STATUS_COMPLETE and ticker in mins:
            complete.append(ticker)
        elif (state and state["status"] != STATUS_COMPLETE
              and state["retry_after"] is not None and state["retry_after"] > now):
            waiting.setdefault(state["status"], []).append(ticker)
        else:
            pending.append(HistoryTask(ticker, instruments.get(ticker), mins.get(ticker),
                                       state["status"] if state else None))
    return {"complete": complete, "waiting": waiting, "pending": pending}


def record_ticker_status(
    conn, ticker: str, status: str, *, detail: str | None = None,
    history_start: _dt.date | None = None, retry_after: _dt.datetime | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            RECORD_STATUS_SQL,
            (ticker, FOREIGN_LISTING_SOURCE, status, detail, history_start, retry_after),
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


def load_ticker_history(
    conn, ticker: str, rows: list[tuple[Any, ...]], *,
    history_start: _dt.date, detail: str | None = None,
) -> int:
    """A ticker's whole history and its ``history_complete`` status, atomically.

    Every row (``INSERT … DO NOTHING``, sent in ``UPSERT_CHUNK`` batches) and
    the status write share ONE transaction: a crash or error at any batch
    leaves neither rows nor status, so the next run refetches the full window.
    ~7.5k rows for a 30-year ticker. Returns rows inserted."""
    conn.commit()  # close any read transaction; this one holds only the load
    inserted = 0
    try:
        with conn.cursor() as cur:
            for i in range(0, len(rows), UPSERT_CHUNK):
                cur.executemany(EOD_HISTORY_INSERT_SQL, rows[i:i + UPSERT_CHUNK])
                inserted += max(cur.rowcount, 0)
            cur.execute(
                RECORD_STATUS_SQL,
                (ticker, FOREIGN_LISTING_SOURCE, STATUS_COMPLETE, detail, history_start, None),
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return inserted


def _stored_basis(
    conn, ticker: str, first: _dt.date, through: _dt.date
) -> dict[_dt.date, dict[str, float]]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT date, close, adj_open, adj_high, adj_low, adj_close
               FROM eod_prices WHERE ticker = %s AND date BETWEEN %s AND %s""",
            (ticker, first, through),
        )
        return {r[0]: dict(zip(_BASIS_COLUMNS, r[1:])) for r in cur.fetchall()}


def cover_foreign_history(
    conn,
    tiingo: TiingoClient,
    tickers: list[str],
    *,
    as_of: _dt.date,
    cap: int,
    ring_owned: frozenset[str] = frozenset(),
    now: _dt.datetime | None = None,
) -> dict[str, Any]:
    """Bring up to ``cap`` covered tickers to full history.

    Per ticker: a meta call (fresh Tiingo startDate; fills NULL instruments
    metadata), then at most one price window. A ticker without rows loads
    ``startDate → as_of``; a ticker with rows loads only the prefix older than
    its first row, and only if Tiingo's values on the first stored sessions
    match the stored ones (raw close and adjusted OHLC). Rows and the
    ``history_complete`` status are written in one transaction.

    Not complete: an unknown ticker (recheck in ``UNKNOWN_RECHECK_DAYS``); a
    response with an unusable bar, an empty window, or no overlap to compare
    (``history_incomplete``, retry in ``INCOMPLETE_RETRY_HOURS``, counted as an
    error); a moved adjustment basis or a raw-close difference
    (``adjustment_rebase_required`` / ``history_conflict``: nothing inserted,
    no seam, reported). Transient Tiingo failures write nothing. Tickers the
    ring would serve but excludes while their history is open go first, so they
    rejoin the ring soonest. A budget abort stops the phase and is reported."""
    now = now or _dt.datetime.now(_dt.UTC)
    ensure_status_table(conn)
    plan = plan_foreign_history(conn, tickers, now=now)
    def priority(task: HistoryTask) -> tuple[bool, bool, str]:
        # First the lines the ring would serve but excludes while their history
        # is open (rows outside the screener, screener lines without rows),
        # then new coverage, then screener lines that only lack older history.
        waits_for_ring = (task.min_date is not None) != (task.ticker in ring_owned)
        return (not waits_for_ring, task.min_date is not None, task.ticker)

    queue = sorted(plan["pending"], key=priority)
    stats: dict[str, Any] = {
        "source_tickers": len(set(tickers)),
        "already_complete": len(plan["complete"]),
        "waiting": {k: len(v) for k, v in sorted(plan["waiting"].items())},
        "pending": len(queue),
        "processed": 0, "meta_requests": 0, "history_fetches": 0,
        "instruments_inserted": 0, "instruments_filled": 0,
        "history_rows": 0, "completed": 0, "errors": 0,
    }
    recheck = now + _dt.timedelta(days=UNKNOWN_RECHECK_DAYS)
    retry = now + _dt.timedelta(hours=INCOMPLETE_RETRY_HOURS)
    unknown_now: list[str] = []
    fail_closed: dict[str, str] = {}
    errors: dict[str, str] = {}

    def incomplete(ticker: str, start: _dt.date | None, reason: str) -> None:
        record_ticker_status(conn, ticker, STATUS_INCOMPLETE, detail=reason,
                             history_start=start, retry_after=retry)
        errors[ticker] = reason

    for task in queue:
        if stats["processed"] >= cap:
            break
        ticker = task.ticker
        start: _dt.date | None = None
        try:
            stats["meta_requests"] += 1
            status, meta = tiingo.fetch_meta_result(ticker)
            if status == "not_found":
                record_ticker_status(conn, ticker, STATUS_UNKNOWN, detail="not_found",
                                     retry_after=recheck)
                unknown_now.append(ticker)
                continue
            if status != "found" or meta is None:
                errors[ticker] = f"meta:{status}"
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
            window = history_fetch_window(start, task.min_date, as_of)
            if window is None:
                # Rows already reach Tiingo's current startDate: nothing older
                # exists to load. Recorded, never re-inferred from the rows.
                record_ticker_status(conn, ticker, STATUS_COMPLETE,
                                     detail="rows_reach_tiingo_start", history_start=start)
                stats["completed"] += 1
                continue
            stats["history_fetches"] += 1
            status, bars = tiingo.fetch_daily_bars_result(ticker, *window)
            if status == "not_found":
                record_ticker_status(conn, ticker, STATUS_UNKNOWN, detail="prices_not_found",
                                     retry_after=recheck)
                unknown_now.append(ticker)
                continue
            if status not in ("success_new", "empty"):
                errors[ticker] = f"prices:{status}"
                continue
            rows = build_eod_rows(ticker, bars)
            if len(rows) != len(bars):
                incomplete(ticker, start, f"unusable_bars={len(bars) - len(rows)}")
                continue
            prefix = rows if task.min_date is None else [r for r in rows if r[1] < task.min_date]
            if not prefix:
                # Meta says history starts before the first stored row (or a
                # cold ticker has a startDate) but the window came back empty.
                incomplete(ticker, start, "empty_window")
                continue
            if task.min_date is not None:
                check = compare_basis(rows, _stored_basis(conn, ticker, task.min_date, window[1]))
                if check.verdict == "no_overlap":
                    incomplete(ticker, start, check.detail())
                    continue
                if check.verdict != "match":
                    record_ticker_status(
                        conn, ticker,
                        STATUS_REBASE if check.verdict == "adjusted_moved" else STATUS_CONFLICT,
                        detail=check.detail(), history_start=start, retry_after=recheck,
                    )
                    fail_closed[ticker] = check.detail()
                    continue
            stats["history_rows"] += load_ticker_history(
                conn, ticker, prefix, history_start=start,
                detail=None if task.min_date is None else f"prefix_before={task.min_date}",
            )
            stats["completed"] += 1
        except TiingoBudgetExceeded as exc:
            # No price row was written for this ticker; it stays pending.
            stats["aborted"] = str(exc)
            break
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
    stats["deferred"] = len(queue) - stats["processed"]
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
    fetched = upserted = skipped_rows = 0
    aborted: str | None = None
    last_done: str | None = None
    history: dict[str, Any] | None = None

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_EOD_PRICES_WARMER) as got:
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}

            instruments_seeded = ensure_instruments(conn)
            foreign = foreign_listing_tickers(conn, as_of)
            resume_after = read_cursor(conn)
            universe = warming_universe(conn)
            tickers = order_sweep(universe, resume_after=resume_after)
            watermarks = _ticker_watermarks(conn)
            # A covered foreign line joins the ring once its history status is
            # complete (see ring_excluded); until then the history phase below
            # owns it, so it never takes the 745-day window.
            ring_owned = frozenset(_screener_tickers(conn)) | frozenset(INDEX_TICKERS)
            excluded = ring_excluded(
                foreign or [], read_ticker_status(conn, foreign or []),
                ring_owned=ring_owned, watermarks=watermarks,
            )
            tickers = [t for t in tickers if t not in excluded]
            if limit:
                tickers = tickers[:limit]
            print(
                f"eod_prices_warmer: {len(tickers)} tickers, as_of={as_of}, "
                f"resume_after={resume_after or '-'}, "
                f"foreign_listing={'absent' if foreign is None else len(foreign)}",
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
                        conn, tiingo, foreign, as_of=as_of, cap=cap,
                        ring_owned=ring_owned,
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
    if foreign is None:
        stats["foreign_history"] = {"source": "absent"}
    elif history is not None:
        stats["foreign_history"] = history
    elif foreign:
        stats["foreign_history"] = {
            "source_tickers": len(foreign),
            "skipped": "aborted" if aborted else "cap_zero",
        }
    if aborted:
        stats["aborted"] = aborted
    return stats
