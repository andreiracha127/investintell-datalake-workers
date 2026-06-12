"""benchmark_ingest worker — Tiingo benchmark ETF prices → benchmark_nav.

Standalone reimplementation of the monolith ``benchmark_ingest`` worker
(reference: ``app/jobs/workers/benchmark_ingest.py``). For every allocation
block with a benchmark ticker it fetches the daily adjusted close from Tiingo
(one call per unique ticker), validates data quality (>5% NaN rejects the
ticker), computes log ``return_1d`` and upserts into ``benchmark_nav`` with
conflict key (block_id, nav_date), chunked 200 with per-chunk commit.

Block→ticker map: the cloud data-lake does not hold ``allocation_blocks``
(monolith table); when it is absent or empty we fall back to
``DEFAULT_BLOCK_TICKERS`` — copied verbatim from the mother DB on 2026-06-11.
Ship/apply ``schemas/benchmark_ingest.sql`` to override via the table.

Incremental: per-block watermark ``max(nav_date)`` − 7d overlap (revisions);
blocks with no history get the full ~15y lookback. ``benchmark_nav`` is a
compressed hypertable — upserts only land safely on recent uncompressed
chunks, which is exactly what the watermark path touches.

Contract:  run(dsn, *, calc_date=None, limit=None) -> {"fetched", "upserted", ...}
``limit`` caps the number of tickers (smoke runs). Env: TIINGO_API_KEY.
"""

from __future__ import annotations

import datetime as _dt
import math
from typing import Any

from src.db import LOCK_BENCHMARK_INGEST, advisory_lock, connect
from src.workers._tiingo import TiingoBudgetExceeded, TiingoClient

UPSERT_CHUNK = 200          # short transactions, matches the monolith
MAX_NAN_RATIO = 0.05
DEFAULT_LOOKBACK_DAYS = 5475  # ~15y: covers GFC 2008 + Taper Tantrum 2013
WATERMARK_OVERLAP_DAYS = 7

# Verbatim from the mother DB allocation_blocks (2026-06-11) — fallback when
# the cloud has no allocation_blocks table. Semantic + per-org UUID blocks.
DEFAULT_BLOCK_TICKERS: dict[str, str] = {
    "251dc77e-d713-40c5-b444-271667b40886": "SPY",
    "2bc7a4ec-47e6-4d26-b4a2-2bc04d47d258": "IWD",
    "2e259d57-2d55-4279-83e4-768b4ccb0055": "HYG",
    "3144284d-f188-4d29-a320-f24e79bc7ab3": "IWF",
    "36b41a24-79a8-432f-9df3-f68f4cb67ce6": "IEF",
    "7b5006d2-84fd-484d-8f02-40b3f021e236": "IWM",
    "9ec925d8-6385-4a68-9ccf-bba4a74063be": "SPY",
    "a097c86b-18ac-449c-a198-a4cc7ead2f81": "IWD",
    "a0f62f61-a8f1-4104-bb67-494028b53ff0": "IWF",
    "ab380588-dcae-41c5-b64f-5a61bd79b843": "HYG",
    "alt_commodities": "DJP",
    "alt_gold": "GLD",
    "alt_real_estate": "VNQ",
    "cash": "SHV",
    "cbbe5a5c-a966-4471-9576-09f8d42eff67": "EFA",
    "d7d5e52f-e415-44b5-891a-0e4817937016": "IEF",
    "de1f6baf-3660-4bc9-a312-2944591fd7b7": "IWM",
    "dm_asia_equity": "EWJ",
    "dm_europe_equity": "VGK",
    "e1e5eef0-f3f3-402f-b58d-0a584197580b": "EFA",
    "em_equity": "EEM",
    "factor_source_intl_developed": "EFA",
    "factor_source_us_growth": "IWF",
    "fi_em_debt": "EMB",
    "fi_ig_corporate": "LQD",
    "fi_us_aggregate": "AGG",
    "fi_us_high_yield": "HYG",
    "fi_us_short_term": "SHY",
    "fi_us_tips": "TIP",
    "fi_us_treasury": "IEF",
    "na_equity_growth": "QQQ",
    "na_equity_large": "SPY",
    "na_equity_small": "IWM",
    "na_equity_value": "IWD",
}


# ──────────────────────────────────────────────────────────────────────────────
# Pure helpers
# ──────────────────────────────────────────────────────────────────────────────
def nan_ratio_ok(series: list[tuple[_dt.date, float | None]]) -> bool:
    """Reject a ticker whose price series is empty or >5% NaN."""
    if not series:
        return False
    nan = sum(1 for _, v in series if v is None)
    return nan / len(series) <= MAX_NAN_RATIO


def build_rows(series: list[tuple[_dt.date, float | None]],
               block_ids: list[str]) -> list[dict[str, Any]]:
    """One ticker series → rows for every block sharing it (log return_1d)."""
    rows: list[dict[str, Any]] = []
    prev: float | None = None
    for d, price in sorted(series):
        if price is None or price <= 0:
            continue
        ret = round(math.log(price / prev), 8) if prev else None
        for block_id in block_ids:
            rows.append({
                "block_id": block_id,
                "nav_date": d,
                "nav": round(price, 6),
                "return_1d": ret,
                "return_type": "log",
                "source": "tiingo",
            })
        prev = price
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# DB I/O
# ──────────────────────────────────────────────────────────────────────────────
def block_ticker_map(conn) -> dict[str, str]:
    """block_id → benchmark_ticker from allocation_blocks; embedded fallback."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT block_id, benchmark_ticker FROM allocation_blocks
                   WHERE benchmark_ticker IS NOT NULL AND is_active""")
            rows = cur.fetchall()
        if rows:
            return {r[0]: r[1].strip().upper() for r in rows}
    except Exception:
        conn.rollback()
    return dict(DEFAULT_BLOCK_TICKERS)


def _block_watermarks(conn) -> dict[str, _dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT block_id, max(nav_date) FROM benchmark_nav GROUP BY block_id")
        return {r[0]: r[1] for r in cur.fetchall() if r[1] is not None}


def upsert_benchmark_nav(conn, rows: list[dict[str, Any]]) -> int:
    """Chunked idempotent upsert (per-chunk commit for fault isolation)."""
    upserted = 0
    sql = """
        INSERT INTO benchmark_nav (block_id, nav_date, nav, return_1d, return_type, source)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (block_id, nav_date) DO UPDATE SET
            nav = EXCLUDED.nav,
            return_1d = EXCLUDED.return_1d,
            return_type = EXCLUDED.return_type,
            source = EXCLUDED.source,
            updated_at = now()
    """
    with conn.cursor() as cur:
        for i in range(0, len(rows), UPSERT_CHUNK):
            chunk = rows[i:i + UPSERT_CHUNK]
            cur.executemany(sql, [
                (r["block_id"], r["nav_date"], r["nav"], r["return_1d"],
                 r["return_type"], r["source"])
                for r in chunk
            ])
            conn.commit()
            upserted += len(chunk)
    return upserted


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Refresh benchmark_nav from Tiingo for every benchmarked block."""
    as_of = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()
    fetched = upserted = 0
    skipped_quality: list[str] = []
    aborted = None

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_BENCHMARK_INGEST) as got:
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}

            mapping = block_ticker_map(conn)
            watermarks = _block_watermarks(conn)
            ticker_blocks: dict[str, list[str]] = {}
            for block_id, ticker in mapping.items():
                ticker_blocks.setdefault(ticker, []).append(block_id)

            tickers = sorted(ticker_blocks)
            if limit:
                tickers = tickers[:limit]

            with TiingoClient() as tiingo:
                for ticker in tickers:
                    blocks = ticker_blocks[ticker]
                    marks = [watermarks[b] for b in blocks if b in watermarks]
                    start = (min(marks) - _dt.timedelta(days=WATERMARK_OVERLAP_DAYS)
                             if len(marks) == len(blocks)
                             else as_of - _dt.timedelta(days=DEFAULT_LOOKBACK_DAYS))
                    try:
                        series = tiingo.fetch_daily_prices(ticker, start, as_of)
                    except TiingoBudgetExceeded as exc:
                        aborted = str(exc)
                        break
                    fetched += len(series)
                    if not nan_ratio_ok(series):
                        skipped_quality.append(ticker)
                        continue
                    upserted += upsert_benchmark_nav(conn, build_rows(series, blocks))
            conn.commit()

    stats: dict[str, Any] = {
        "fetched": fetched, "upserted": upserted,
        "tickers": len(tickers), "as_of": as_of.isoformat(),
    }
    if skipped_quality:
        stats["skipped_quality"] = skipped_quality
    if aborted:
        stats["aborted"] = aborted
    return stats
