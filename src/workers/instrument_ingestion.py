"""instrument_ingestion worker — Tiingo daily prices → nav_timeseries.

Standalone reimplementation of the monolith ``instrument_ingestion`` worker
(reference: ``app/jobs/workers/instrument_ingestion.py``), adapted from the
monolith's full-universe ~15y refetch to a **stale-only, AUM-prioritised
sweep**: only tickers whose newest nav_date is stale are fetched, from their
watermark, so a daily run moves a fraction of the data. The account's Tiingo
budget was verified empirically on 2026-06-12 (150 req in 2.9s, zero 429 —
Power tier, 10k req/h), so ``DEFAULT_TICKER_CAP`` admits the whole ~6.1k-ticker
universe in a single run; fetches run on ``FETCH_CONCURRENCY`` threads paced
at 10 req/s (a full sweep is ~6.1k requests ≈ 10 min, still under the hourly
budget), upserts stay on the main thread/connection. The cap remains as a
guard against unbounded universe growth.

Faithful where it matters:
  * universe = ``instruments_universe WHERE is_active AND ticker IS NOT NULL``;
    one Tiingo call per **unique** ticker, fanned out to every instrument
    sharing it (each row keeps its instrument's currency);
  * ``adjClose`` preferred (split/dividend-adjusted), fallback ``close``;
  * rows: nav rounded 6, log ``return_1d`` rounded 8, ``return_type='log'``,
    ``source='tiingo'`` — matching the 26.8M existing rows;
  * upsert ON CONFLICT (instrument_id, nav_date) DO UPDATE, chunk 500 with
    per-chunk commit;
  * 30-consecutive-429 breaker aborts cleanly (resume next cycle from the
    watermarks). ~416 no-ticker instruments are skipped by design.

UCITS coverage: tickers Tiingo returns empty for (the ``.L/.PA/.MI/.SW``
European share classes — design §1D provider gap) fall through to the
``_fallback_nav`` chain: EODHD when ``EODHD_API_KEY`` is set, else Yahoo
(which fed the existing 622k ``source='yahoo'`` rows). Rows carry the actual
provider in ``source``; ``stats["fallback_loaded"]`` reports the split.

Contract:  run(dsn, *, calc_date=None, limit=None) -> {"fetched", "upserted", ...}
``limit`` overrides DEFAULT_TICKER_CAP. Env: TIINGO_API_KEY.
"""

from __future__ import annotations

import datetime as _dt
import math
from dataclasses import dataclass
from typing import Any

from src.db import LOCK_INSTRUMENT_INGESTION, advisory_lock, connect
from src.workers._nav_sanitize import sanitize_nav_series
from src.workers._tiingo import TiingoBudgetExceeded, TiingoClient

UPSERT_CHUNK = 500
DEFAULT_LOOKBACK_DAYS = 5475   # ~15y for first-time backfills
WATERMARK_OVERLAP_DAYS = 7     # re-fetch overlap to catch revisions
STALE_AFTER_DAYS = 2           # weekend-tolerant: refreshed daily data is fresh
DEFAULT_TICKER_CAP = 10_000    # full universe fits the verified 10k req/h budget
# Railway service: 24 vCPU / 24 GB. Fetches are I/O-bound; concurrency matches
# the cores and the bucket caps the burst — a full sweep is ~6.1k requests, so
# even at 25 req/s the hourly total stays under the verified 10k req/h budget.
FETCH_CONCURRENCY = 24         # parallel Tiingo fetches (upserts stay single-conn)
FETCH_RATE_PER_S = 25.0        # shared bucket: 6.1k req/sweep stays < 10k/h


@dataclass(frozen=True)
class TickerPlan:
    """One Tiingo fetch: a ticker, its start date and target instruments."""

    ticker: str
    start_date: _dt.date
    instruments: tuple[tuple[Any, str], ...]  # (instrument_id, currency)
    max_aum: float | None


# ──────────────────────────────────────────────────────────────────────────────
# Pure planning + row building
# ──────────────────────────────────────────────────────────────────────────────
def select_stale_tickers(universe: list[dict[str, Any]],
                         watermarks: dict[str, _dt.date],
                         as_of: _dt.date, cap: int) -> list[TickerPlan]:
    """Stale-only, AUM-prioritised fetch plan (one entry per unique ticker).

    A ticker is stale when it has no NAV history or its newest nav_date is
    older than STALE_AFTER_DAYS. Plans are ordered by AUM descending (NULLs
    last) and capped to bound the run within the Tiingo budget.
    """
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for inst in universe:
        ticker = (inst.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        by_ticker.setdefault(ticker, []).append(inst)

    plans: list[TickerPlan] = []
    threshold = as_of - _dt.timedelta(days=STALE_AFTER_DAYS)
    for ticker, instruments in by_ticker.items():
        wm = watermarks.get(ticker)
        if wm is not None and wm >= threshold:
            continue  # fresh
        start = (wm - _dt.timedelta(days=WATERMARK_OVERLAP_DAYS) if wm is not None
                 else as_of - _dt.timedelta(days=DEFAULT_LOOKBACK_DAYS))
        aums = [i["aum_usd"] for i in instruments if i.get("aum_usd") is not None]
        plans.append(TickerPlan(
            ticker=ticker,
            start_date=start,
            instruments=tuple((i["instrument_id"], i.get("currency") or "USD")
                              for i in instruments),
            max_aum=max(aums) if aums else None,
        ))
    plans.sort(key=lambda p: (p.max_aum is None, -(p.max_aum or 0.0), p.ticker))
    return plans[:cap]


def build_rows(series: list[tuple[_dt.date, float | None]],
               instruments: list[tuple[Any, str]] | tuple[tuple[Any, str], ...],
               source: str = "tiingo") -> list[dict[str, Any]]:
    """One ticker series → rows for every instrument sharing it (log returns).

    Runs ``sanitize_nav_series`` over the price series BEFORE computing
    ``return_1d`` so a transient near-zero glitch (Bug 2) never reaches
    nav_timeseries as an impossible log return. Dead / scale-step series are not
    repaired (the eligibility flag handles them); their values pass through.
    """
    ordered = sorted((d, p) for d, p in series if p is not None and p > 0)
    clean = sanitize_nav_series(ordered)
    rows: list[dict[str, Any]] = []
    prev: float | None = None
    for (d, _orig), price in zip(ordered, clean.nav):
        if price is None or price <= 0:
            continue
        ret = round(math.log(price / prev), 8) if prev else None
        for instrument_id, currency in instruments:
            rows.append({
                "instrument_id": instrument_id,
                "nav_date": d,
                "nav": round(price, 6),
                "return_1d": ret,
                "return_type": "log",
                "currency": currency,
                "source": source,
            })
        prev = price
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# DB I/O
# ──────────────────────────────────────────────────────────────────────────────
def _fetch_universe(conn) -> list[dict[str, Any]]:
    """Active instruments with a ticker, plus their attributes-resident AUM."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT instrument_id, ticker, currency,
                      NULLIF(attributes->>'aum_usd', '')::numeric AS aum_usd
               FROM instruments_universe
               WHERE is_active AND ticker IS NOT NULL AND ticker != ''""")
        return [{"instrument_id": r[0], "ticker": r[1], "currency": r[2],
                 "aum_usd": float(r[3]) if r[3] is not None else None}
                for r in cur.fetchall()]


def _fetch_watermarks(conn) -> dict[str, _dt.date]:
    """Newest nav_date per ticker (min across instruments sharing the ticker,
    so a brand-new share class forces a refetch deep enough to cover it)."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT upper(iu.ticker), min(mx) FROM (
                   SELECT instrument_id, max(nav_date) AS mx
                   FROM nav_timeseries GROUP BY instrument_id
               ) n JOIN instruments_universe iu USING (instrument_id)
               WHERE iu.ticker IS NOT NULL AND iu.ticker != ''
               GROUP BY upper(iu.ticker)""")
        return {r[0]: r[1] for r in cur.fetchall() if r[1] is not None}


def upsert_nav_timeseries(conn, rows: list[dict[str, Any]]) -> int:
    """Chunked idempotent upsert (per-chunk commit for fault isolation)."""
    upserted = 0
    sql = """
        INSERT INTO nav_timeseries
            (instrument_id, nav_date, nav, return_1d, return_type, currency, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, nav_date) DO UPDATE SET
            nav = EXCLUDED.nav,
            return_1d = EXCLUDED.return_1d,
            return_type = EXCLUDED.return_type,
            currency = EXCLUDED.currency,
            source = EXCLUDED.source
    """
    with conn.cursor() as cur:
        for i in range(0, len(rows), UPSERT_CHUNK):
            chunk = rows[i:i + UPSERT_CHUNK]
            cur.executemany(sql, [
                (r["instrument_id"], r["nav_date"], r["nav"], r["return_1d"],
                 r["return_type"], r["currency"], r["source"])
                for r in chunk
            ])
            conn.commit()
            upserted += len(chunk)
    return upserted


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Refresh nav_timeseries for the stalest/biggest tickers from Tiingo."""
    as_of = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()
    cap = limit if limit is not None else DEFAULT_TICKER_CAP
    fetched = upserted = 0
    empty_tickers: list[str] = []
    aborted = None

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_INSTRUMENT_INGESTION) as got:
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}

            universe = _fetch_universe(conn)
            watermarks = _fetch_watermarks(conn)
            plans = select_stale_tickers(universe, watermarks, as_of, cap)

            done = 0
            fallback_loaded: dict[str, int] = {}
            from src.workers._fallback_nav import FallbackNav
            from src.workers._tiingo import TokenBucket
            with TiingoClient(bucket=TokenBucket(max_tokens=20,
                                                 refill_rate=FETCH_RATE_PER_S)) as tiingo, \
                    FallbackNav() as fallback:
                import concurrent.futures

                def fetch_one(p: TickerPlan) -> tuple[list, str | None]:
                    """Primary (Tiingo), then the EODHD→Yahoo fallback chain."""
                    series = tiingo.fetch_daily_prices(p.ticker, p.start_date, as_of)
                    if series:
                        return series, "tiingo"
                    return fallback.fetch(p.ticker, p.start_date, as_of)

                # Fetches fan out across threads (httpx.Client is thread-safe);
                # upserts stay serialized on this one connection.
                with concurrent.futures.ThreadPoolExecutor(FETCH_CONCURRENCY) as pool:
                    futures = {pool.submit(fetch_one, p): p for p in plans}
                    # Rows accumulate across tickers and flush in large batches:
                    # one commit per ~2k rows instead of one per ticker (the DB
                    # round-trip, not Tiingo, dominates a watermark sweep).
                    pending: list[dict[str, Any]] = []
                    for fut in concurrent.futures.as_completed(futures):
                        plan = futures[fut]
                        try:
                            series, source = fut.result()
                        except TiingoBudgetExceeded as exc:
                            aborted = str(exc)
                            pool.shutdown(cancel_futures=True)
                            break
                        if not series or source is None:
                            empty_tickers.append(plan.ticker)  # gap em todos os provedores
                            continue
                        fetched += len(series)
                        if source != "tiingo":
                            fallback_loaded[source] = fallback_loaded.get(source, 0) + 1
                        pending.extend(build_rows(series, plan.instruments, source))
                        done += 1
                        if len(pending) >= 4 * UPSERT_CHUNK:
                            upserted += upsert_nav_timeseries(conn, pending)
                            pending = []
                    if pending:
                        upserted += upsert_nav_timeseries(conn, pending)
            conn.commit()

    stats: dict[str, Any] = {
        "fetched": fetched, "upserted": upserted,
        "tickers_planned": len(plans), "tickers_loaded": done,
        "tickers_empty": len(empty_tickers), "as_of": as_of.isoformat(),
    }
    if fallback_loaded:
        stats["fallback_loaded"] = fallback_loaded
    if aborted:
        stats["aborted"] = aborted
    return stats
