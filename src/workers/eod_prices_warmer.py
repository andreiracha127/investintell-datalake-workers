"""eod_prices_warmer worker — keep the API's ``eod_prices`` universe fresh.

Strategy B (Investintell-Light API latency-tail fix): the API serves /stocks/*
DB-first from ``eod_prices`` and never fetches a *stale* ticker synchronously on
the request path. This worker keeps that table fresh out-of-band for every
active screener constituent, every ticker already present in ``eod_prices``, and
the benchmark ETFs needed by the screener metrics worker.

Universe = active ``universe_constituents`` ∪ ``SELECT DISTINCT ticker FROM
eod_prices`` ∪ ``INDEX_TICKERS``. This keeps the public stock screener covered
instead of relying on a ticker to be queried once before it becomes warm.

Incremental only: every warmed ticker already has history, so we fetch from
``max(date) − overlap`` (revisions) through today. ``eod_prices`` upserts land on
recent uncompressed chunks — the same path the API's on-demand ingest uses.

NOTE vs ``instrument_ingestion`` (which refreshes ``nav_timeseries`` for the fund
catalog): this worker targets ``eod_prices`` (stock/ETF OHLCV the /stocks/* API
reads). They cover different tables — do not conflate.

Contract:  run(dsn, *, calc_date=None, limit=None) -> {"fetched", "upserted", ...}
``limit`` caps the number of tickers (smoke runs). Env: TIINGO_API_KEY.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from src.db import LOCK_EOD_PRICES_WARMER, advisory_lock, connect
from src.workers._tiingo import TiingoBudgetExceeded, TiingoClient, TokenBucket

UPSERT_CHUNK = 500            # short transactions; well under the 65535-param ceiling (14/row)
WATERMARK_OVERLAP_DAYS = 5    # re-fetch the last few days to absorb provider revisions
NEW_TICKER_LOOKBACK_DAYS = 745  # covers screener beta_2y lookback on cold tickers

# Tiingo pacing — fast lane, matching instrument_ingestion. The account's hourly
# budget far exceeds this, and the warming universe (~2k tickers) is small, so
# the full sweep finishes in ~90s instead of ~15min at the 2.5 req/s default.
FETCH_RATE_PER_S = 25.0
FETCH_BURST = 20.0
PROGRESS_EVERY = 500  # emit a heartbeat log every N tickers (observability)

# Index / benchmark ETFs the API and screener need even if never queried.
INDEX_TICKERS: tuple[str, ...] = ("SPY", "QQQ", "DIA", "IWM", "GLD", "AGG", "TLT", "USO")

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
    """Active screener tickers + already-known EOD tickers + benchmark ETFs."""
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
            cur.executemany(EOD_UPSERT_SQL, chunk)
            conn.commit()
            upserted += len(chunk)
    return upserted


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Refresh eod_prices from Tiingo for every ticker in the warming universe."""
    as_of = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()
    fetched = upserted = skipped_rows = 0
    aborted: str | None = None

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_EOD_PRICES_WARMER) as got:
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}

            tickers = warming_universe(conn)
            if limit:
                tickers = tickers[:limit]
            watermarks = _ticker_watermarks(conn)
            print(
                f"eod_prices_warmer: {len(tickers)} tickers, as_of={as_of}",
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
                    if i % PROGRESS_EVERY == 0:
                        print(
                            f"eod_prices_warmer: {i}/{len(tickers)} tickers, "
                            f"upserted={upserted}",
                            flush=True,
                        )
            conn.commit()

    stats: dict[str, Any] = {
        "fetched": fetched, "upserted": upserted,
        "tickers": len(tickers), "as_of": as_of.isoformat(),
    }
    if skipped_rows:
        stats["skipped_rows"] = skipped_rows
    if aborted:
        stats["aborted"] = aborted
    return stats
