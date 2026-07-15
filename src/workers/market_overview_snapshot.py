"""Build the complete `/stocks/overview` read model outside the request path."""

from __future__ import annotations

import datetime as dt
import math
import statistics
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from psycopg.types.json import Jsonb

from src.db import LOCK_MARKET_OVERVIEW_SNAPSHOT, advisory_lock, connect


PRICE_FLOOR = 5.0
MIN_DOLLAR_VOLUME = 5_000_000.0
TOP_N = 25
NEAR_EXTREME_PCT = 0.02
SPARK_POINTS = 30
LOOKBACK_52W_DAYS = 364
INDEX_WINDOW_DAYS = 60
SNAPSHOT_KEY = "market-overview-v1"
SCHEMA_VERSION = 1
INDEX_TICKERS = ("SPY", "QQQ", "DIA", "IWM")
INDEX_NAMES = {
    "SPY": "S&P 500",
    "QQQ": "Nasdaq 100",
    "DIA": "Dow Jones",
    "IWM": "Russell 2000",
}


@dataclass(frozen=True)
class OverviewRow:
    ticker: str
    name: str | None
    sector: str | None
    last: float
    prev: float
    volume: int
    high_52w: float
    low_52w: float


def _require_finite(value: object, *, path: str = "payload") -> None:
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must contain only finite numbers")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            _require_finite(item, path=f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _require_finite(item, path=f"{path}[{index}]")


def _leader(row: OverviewRow) -> dict[str, object]:
    return {
        "ticker": row.ticker,
        "name": row.name,
        "sector": row.sector,
        "last": row.last,
        "change": row.last - row.prev,
        "change_pct": row.last / row.prev - 1,
        "volume": row.volume,
        "high_52w": row.high_52w,
        "low_52w": row.low_52w,
    }


def _breadth(rows: Sequence[OverviewRow]) -> dict[str, int | float] | None:
    if not rows:
        return None

    advancing = declining = unchanged = 0
    new_highs = new_lows = 0
    up_volume = 0.0
    total_volume = 0.0
    for row in rows:
        total_volume += row.volume
        if row.last > row.prev:
            advancing += 1
            up_volume += row.volume
        elif row.last < row.prev:
            declining += 1
        else:
            unchanged += 1
        if row.high_52w > 0 and row.last >= row.high_52w:
            new_highs += 1
        if row.low_52w > 0 and row.last <= row.low_52w:
            new_lows += 1

    return {
        "tracked": len(rows),
        "advancing": advancing,
        "declining": declining,
        "unchanged": unchanged,
        "advance_decline_ratio": advancing / declining if declining else float(advancing),
        "new_highs_52w": new_highs,
        "new_lows_52w": new_lows,
        "up_volume_share": up_volume / total_volume if total_volume > 0 else 0.0,
    }


def _index_cards(index_closes: Mapping[str, Sequence[float]]) -> list[dict[str, object]]:
    cards: list[dict[str, object]] = []
    for ticker in INDEX_TICKERS:
        closes = [float(value) for value in index_closes.get(ticker, ())][-SPARK_POINTS:]
        _require_finite(closes, path=f"indices.{ticker}")
        if len(closes) < 2:
            continue
        if closes[-2] <= 0:
            raise ValueError(f"indices.{ticker} previous close must be positive")
        cards.append(
            {
                "ticker": ticker,
                "name": INDEX_NAMES[ticker],
                "last": closes[-1],
                "change_pct": closes[-1] / closes[-2] - 1,
                "spark": closes,
            }
        )
    return cards


def build_payload(
    as_of: dt.date,
    rows: Sequence[OverviewRow],
    index_closes: Mapping[str, Sequence[float]],
) -> dict[str, Any]:
    """Apply the existing overview rules and return a JSON-compatible payload."""
    if not rows:
        raise ValueError("market overview source rows are unavailable")
    if not isinstance(as_of, dt.date):
        raise ValueError("market overview watermark is unavailable")

    for index, row in enumerate(rows):
        _require_finite(asdict(row), path=f"rows[{index}]")
        if row.prev <= 0:
            raise ValueError(f"rows[{index}].prev must be positive")

    liquid = [
        row
        for row in rows
        if row.last >= PRICE_FLOOR and row.last * row.volume >= MIN_DOLLAR_VOLUME
    ]
    by_change = sorted(liquid, key=lambda row: row.last / row.prev - 1, reverse=True)
    by_dollar_volume = sorted(liquid, key=lambda row: row.last * row.volume, reverse=True)
    at_high = sorted(
        (
            row
            for row in liquid
            if row.high_52w > 0
            and row.last >= row.high_52w * (1 - NEAR_EXTREME_PCT)
        ),
        key=lambda row: row.last / row.high_52w,
        reverse=True,
    )
    at_low = sorted(
        (
            row
            for row in liquid
            if row.low_52w > 0
            and row.last <= row.low_52w * (1 + NEAR_EXTREME_PCT)
        ),
        key=lambda row: row.last / row.low_52w,
    )

    changes_by_sector: dict[str, list[float]] = defaultdict(list)
    for row in liquid:
        if row.sector:
            changes_by_sector[row.sector].append(row.last / row.prev - 1)
    sectors = sorted(
        (
            {
                "sector": sector,
                "change_pct_median": statistics.median(changes),
                "n": len(changes),
            }
            for sector, changes in changes_by_sector.items()
        ),
        key=lambda item: item["change_pct_median"],
        reverse=True,
    )

    payload: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "universe_size": len(rows),
        "indices": _index_cards(index_closes),
        "most_active": [_leader(row) for row in by_dollar_volume[:TOP_N]],
        "gainers": [_leader(row) for row in by_change[:TOP_N] if row.last > row.prev],
        "losers": [
            _leader(row) for row in reversed(by_change[-TOP_N:]) if row.last < row.prev
        ],
        "highs_52w": [_leader(row) for row in at_high[:TOP_N]],
        "lows_52w": [_leader(row) for row in at_low[:TOP_N]],
        "sectors": sectors,
        "breadth": _breadth(liquid),
    }
    _require_finite(payload)
    return payload


def _fetch_watermark(conn: Any) -> dt.date:
    with conn.cursor() as cur:
        cur.execute("SELECT max(as_of) FROM price_latest_mv")
        row = cur.fetchone()
    watermark = row[0] if row else None
    if not isinstance(watermark, dt.date):
        raise RuntimeError("price_latest_mv watermark is unavailable")
    return watermark


def _fetch_overview_rows(conn: Any, as_of: dt.date) -> list[OverviewRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                pl.ticker,
                uc.name,
                uc.sector,
                pl.last_close,
                pl.prev_close,
                COALESCE(ep.volume, 0)::bigint AS volume
            FROM price_latest_mv AS pl
            JOIN universe_constituents AS uc
              ON uc.ticker = pl.ticker
             AND uc.status = 'active'
            LEFT JOIN eod_prices AS ep
              ON ep.ticker = pl.ticker AND ep.date = pl.as_of
            WHERE pl.last_close IS NOT NULL
              AND pl.prev_close IS NOT NULL
              AND pl.prev_close > 0
            ORDER BY pl.ticker
            """
        )
        latest = cur.fetchall()

    start_date = as_of - dt.timedelta(days=LOOKBACK_52W_DAYS)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ep.ticker, max(ep.close), min(ep.close)
            FROM eod_prices AS ep
            JOIN universe_constituents AS uc
              ON uc.ticker = ep.ticker
             AND uc.status = 'active'
            WHERE ep.date >= %s
              AND ep.date <= %s
              AND ep.close IS NOT NULL
            GROUP BY ep.ticker
            """,
            (start_date, as_of),
        )
        extremes = {ticker: (high, low) for ticker, high, low in cur.fetchall()}

    rows: list[OverviewRow] = []
    for ticker, name, sector, last, prev, volume in latest:
        bounds = extremes.get(ticker)
        if bounds is None:
            continue
        high, low = bounds
        rows.append(
            OverviewRow(
                ticker=str(ticker),
                name=name,
                sector=sector,
                last=float(last),
                prev=float(prev),
                volume=int(volume),
                high_52w=float(high),
                low_52w=float(low),
            )
        )
    return rows


def _fetch_index_closes(conn: Any, as_of: dt.date) -> dict[str, list[float]]:
    start_date = as_of - dt.timedelta(days=INDEX_WINDOW_DAYS)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker, date, close
            FROM (
                SELECT
                    ep.ticker,
                    ep.date,
                    ep.close,
                    row_number() OVER (
                        PARTITION BY ep.ticker ORDER BY ep.date DESC
                    ) AS rn
                FROM eod_prices AS ep
                WHERE ep.ticker = ANY(%s)
                  AND ep.date >= %s
                  AND ep.date <= %s
                  AND ep.close IS NOT NULL
            ) AS ranked
            WHERE rn <= %s
            ORDER BY ticker, date
            """,
            (list(INDEX_TICKERS), start_date, as_of, SPARK_POINTS),
        )
        rows = cur.fetchall()

    closes: dict[str, list[float]] = defaultdict(list)
    for ticker, _date, close in rows:
        closes[str(ticker)].append(float(close))
    return dict(closes)


def _publish_snapshot(
    dsn: str,
    *,
    as_of: dt.date,
    computed_at: dt.datetime,
    payload: dict[str, Any],
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO market_overview_snapshot (
                    snapshot_key, schema_version, as_of, computed_at, payload
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_key) DO UPDATE
                SET schema_version = EXCLUDED.schema_version,
                    as_of = EXCLUDED.as_of,
                    computed_at = EXCLUDED.computed_at,
                    payload = EXCLUDED.payload
                """,
                (
                    SNAPSHOT_KEY,
                    SCHEMA_VERSION,
                    as_of,
                    computed_at,
                    Jsonb(payload),
                ),
            )


def run(dsn: str, *, now: dt.datetime | None = None) -> dict[str, object]:
    """Build and atomically publish the current complete overview snapshot."""
    started = time.perf_counter()
    computed_at = now or dt.datetime.now(dt.UTC)
    if computed_at.tzinfo is None:
        raise ValueError("computed_at must be timezone-aware")

    with connect(dsn, autocommit=True) as read_conn:
        with advisory_lock(read_conn, LOCK_MARKET_OVERVIEW_SNAPSHOT) as got_lock:
            if not got_lock:
                return {"published": 0, "skipped": "lock_busy"}

            as_of = _fetch_watermark(read_conn)
            rows = _fetch_overview_rows(read_conn, as_of)
            index_closes = _fetch_index_closes(read_conn, as_of)
            payload = build_payload(as_of, rows, index_closes)
            _publish_snapshot(
                dsn,
                as_of=as_of,
                computed_at=computed_at,
                payload=payload,
            )

    return {
        "as_of": as_of.isoformat(),
        "computed_at": computed_at.isoformat(),
        "source_rows": len(rows),
        "indices": len(payload["indices"]),
        "published": 1,
        "elapsed_ms": round((time.perf_counter() - started) * 1_000, 3),
    }
