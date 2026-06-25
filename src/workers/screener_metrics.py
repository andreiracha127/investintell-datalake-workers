"""screener_metrics worker - materialize the equity screener read model.

The Light backend reads ``screener_equity_snapshot_mv`` for all screener
build/results requests. This worker owns the recurring snapshot behind that
view: price-derived metrics come from ``eod_prices`` and fundamentals come
directly from the latest ``company_characteristics_monthly`` row for each
active ``universe_constituents.cik``.

This deliberately bypasses the older ``fundamentals_snapshot`` copy table:
``characteristics`` already materializes issuer fundamentals, so the screener
can consume that source directly and avoid another stale intermediate.
"""

from __future__ import annotations

import datetime as _dt
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from src.db import LOCK_SCREENER_METRICS, advisory_lock, connect

BENCHMARK_TICKERS: tuple[str, ...] = ("SPY", "GLD", "AGG", "TLT", "USO")
LOOKBACK_DAYS = 745
DEFAULT_BATCH_SIZE = 200
UPSERT_CHUNK = 500
TRADING_DAYS = 252

_RETURN_WINDOWS: dict[str, pd.DateOffset] = {
    "ret_1w": pd.DateOffset(weeks=1),
    "ret_1m": pd.DateOffset(months=1),
    "ret_3m": pd.DateOffset(months=3),
    "ret_6m": pd.DateOffset(months=6),
    "ret_1y": pd.DateOffset(years=1),
}
_VOL_WINDOWS: dict[str, pd.DateOffset] = {
    "vol_1m": pd.DateOffset(months=1),
    "vol_3m": pd.DateOffset(months=3),
    "vol_6m": pd.DateOffset(months=6),
    "vol_1y": pd.DateOffset(years=1),
}
_BETA_WINDOWS: dict[str, pd.DateOffset] = {
    "beta_3m_spy": pd.DateOffset(months=3),
    "beta_6m_spy": pd.DateOffset(months=6),
    "beta_1y_spy": pd.DateOffset(years=1),
    "beta_2y_spy": pd.DateOffset(years=2),
}
_CORR_COLUMNS: dict[str, str] = {
    "corr_spy": "SPY",
    "corr_gld": "GLD",
    "corr_agg": "AGG",
    "corr_tlt": "TLT",
    "corr_uso": "USO",
}
_CORR_WINDOW = pd.DateOffset(years=1)
_SMA_WINDOWS: dict[str, int] = {
    "pct_above_sma20": 20,
    "pct_above_sma50": 50,
    "pct_above_sma200": 200,
}

_FUNDAMENTAL_COLUMNS: tuple[str, ...] = (
    "market_cap",
    "pe_ratio",
    "roe",
    "roa",
    "gross_margin",
    "de_ratio",
    "investment_growth",
    "profitability_gross",
    "fundamentals_period_end",
)

METRIC_COLUMNS: tuple[str, ...] = (
    *_RETURN_WINDOWS,
    "ret_ytd",
    "ret_mtd",
    *_VOL_WINDOWS,
    *_BETA_WINDOWS,
    *_CORR_COLUMNS,
    *_SMA_WINDOWS,
    "price_close",
    "avg_volume_1m",
    *_FUNDAMENTAL_COLUMNS,
)

UPSERT_SQL = f"""
    INSERT INTO screener_metrics (
        ticker, computed_at, as_of, {", ".join(METRIC_COLUMNS)}
    ) VALUES (
        %(ticker)s, %(computed_at)s, %(as_of)s,
        {", ".join(f"%({col})s" for col in METRIC_COLUMNS)}
    )
    ON CONFLICT (ticker) DO UPDATE SET
        computed_at = EXCLUDED.computed_at,
        as_of = EXCLUDED.as_of,
        {", ".join(f"{col} = EXCLUDED.{col}" for col in METRIC_COLUMNS)}
"""


@dataclass
class MetricsReport:
    total_active: int = 0
    computed: int = 0
    skipped_no_eod: int = 0
    deleted_inactive: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    elapsed_seconds: float = 0.0


def simple_returns(prices: pd.Series) -> pd.Series:
    values = prices.astype(float)
    return values.pct_change().dropna()


def annualized_volatility(returns: pd.Series) -> float | None:
    if len(returns) < 2:
        return None
    vol = float(np.std(returns.to_numpy(dtype=float), ddof=1) * np.sqrt(TRADING_DAYS))
    return vol if np.isfinite(vol) else None


def _aligned_pair(left: pd.Series, right: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    joined = pd.concat([left, right], axis=1, join="inner").dropna()
    if len(joined) < 10:
        raise ValueError("insufficient overlapping observations")
    return joined.iloc[:, 0].to_numpy(dtype=float), joined.iloc[:, 1].to_numpy(dtype=float)


def beta(fund_returns: pd.Series, benchmark_returns: pd.Series) -> float:
    fund, bench = _aligned_pair(fund_returns, benchmark_returns)
    bvar = float(np.var(bench, ddof=1))
    if bvar == 0 or not np.isfinite(bvar):
        raise ValueError("zero benchmark variance")
    return float(np.cov(fund, bench, ddof=1)[0, 1] / bvar)


def correlation(left_returns: pd.Series, right_returns: pd.Series) -> float:
    left, right = _aligned_pair(left_returns, right_returns)
    if float(np.std(left, ddof=1)) == 0 or float(np.std(right, ddof=1)) == 0:
        raise ValueError("zero variance")
    return float(np.corrcoef(left, right)[0, 1])


def _trailing_return(
    adj_close: pd.Series, as_of_ts: pd.Timestamp, offset: pd.DateOffset
) -> float | None:
    start = as_of_ts - offset
    if adj_close.index[0] > start:
        return None
    base = float(adj_close.loc[:start].iloc[-1])
    if base <= 0:
        return None
    return float(adj_close.iloc[-1]) / base - 1.0


def _calendar_return(adj_close: pd.Series, boundary: pd.Timestamp) -> float | None:
    before = adj_close.loc[adj_close.index < boundary]
    if before.empty:
        return None
    base = float(before.iloc[-1])
    if base <= 0:
        return None
    return float(adj_close.iloc[-1]) / base - 1.0


def _window_prices(
    adj_close: pd.Series, as_of_ts: pd.Timestamp, offset: pd.DateOffset
) -> pd.Series | None:
    start = as_of_ts - offset
    if adj_close.index[0] > start:
        return None
    base_pos = int(adj_close.index.searchsorted(start, side="right")) - 1
    return adj_close.iloc[base_pos:]


def _trailing_volatility(
    adj_close: pd.Series, as_of_ts: pd.Timestamp, offset: pd.DateOffset
) -> float | None:
    window = _window_prices(adj_close, as_of_ts, offset)
    if window is None or len(window) < 3:
        return None
    return annualized_volatility(simple_returns(window))


def _window_returns(
    returns: pd.Series, as_of_ts: pd.Timestamp, offset: pd.DateOffset
) -> pd.Series:
    return returns.loc[returns.index > as_of_ts - offset]


def _trailing_beta(
    returns: pd.Series,
    first_price_ts: pd.Timestamp,
    benchmark_returns: pd.Series | None,
    as_of_ts: pd.Timestamp,
    offset: pd.DateOffset,
) -> float | None:
    if benchmark_returns is None or first_price_ts > as_of_ts - offset:
        return None
    try:
        return beta(_window_returns(returns, as_of_ts, offset), benchmark_returns)
    except ValueError:
        return None


def _trailing_correlation(
    returns: pd.Series,
    first_price_ts: pd.Timestamp,
    benchmark_returns: pd.Series | None,
    as_of_ts: pd.Timestamp,
    offset: pd.DateOffset,
) -> float | None:
    if benchmark_returns is None or first_price_ts > as_of_ts - offset:
        return None
    try:
        return correlation(_window_returns(returns, as_of_ts, offset), benchmark_returns)
    except ValueError:
        return None


def _pct_above_sma(adj_close: pd.Series, window: int) -> float | None:
    if len(adj_close) < window:
        return None
    sma = float(adj_close.iloc[-window:].mean())
    if sma <= 0:
        return None
    return float(adj_close.iloc[-1]) / sma - 1.0


def _fundamentals_metrics(
    fundamentals: Mapping[str, Any] | None, price_close: float | None
) -> dict[str, Any]:
    out: dict[str, Any] = dict.fromkeys(_FUNDAMENTAL_COLUMNS)
    if fundamentals is None:
        return out

    shares = fundamentals.get("shares_outstanding")
    net_income = fundamentals.get("net_income_ttm")
    book_equity = fundamentals.get("book_equity")
    total_assets = fundamentals.get("total_assets")
    revenue = fundamentals.get("revenue")
    gross_profit = fundamentals.get("gross_profit")

    if shares is not None and shares > 0 and price_close is not None:
        out["market_cap"] = float(shares) * price_close
    if out["market_cap"] is not None and net_income is not None and net_income > 0:
        out["pe_ratio"] = out["market_cap"] / float(net_income)
    if book_equity is not None and book_equity > 0:
        if net_income is not None:
            out["roe"] = float(net_income) / float(book_equity)
        if total_assets is not None:
            out["de_ratio"] = (float(total_assets) - float(book_equity)) / float(
                book_equity
            )
    if revenue is not None and revenue > 0 and gross_profit is not None:
        out["gross_margin"] = float(gross_profit) / float(revenue)
    out["roa"] = fundamentals.get("quality_roa")
    out["investment_growth"] = fundamentals.get("investment_growth")
    out["profitability_gross"] = fundamentals.get("profitability_gross")
    out["fundamentals_period_end"] = fundamentals.get("period_end")
    return out


def compute_ticker_metrics(
    prices: pd.DataFrame,
    benchmark_returns_map: Mapping[str, pd.Series],
    fundamentals: Mapping[str, Any] | None,
    as_of: _dt.date,
) -> dict[str, Any]:
    as_of_ts = pd.Timestamp(as_of)
    adj_close = prices["adj_close"]
    first_ts = pd.Timestamp(adj_close.index[0])
    spy_returns = benchmark_returns_map.get("SPY")

    out: dict[str, Any] = {}
    for col, offset in _RETURN_WINDOWS.items():
        out[col] = _trailing_return(adj_close, as_of_ts, offset)
    out["ret_ytd"] = _calendar_return(adj_close, pd.Timestamp(as_of.year, 1, 1))
    out["ret_mtd"] = _calendar_return(adj_close, pd.Timestamp(as_of.year, as_of.month, 1))

    for col, offset in _VOL_WINDOWS.items():
        out[col] = _trailing_volatility(adj_close, as_of_ts, offset)

    returns = simple_returns(adj_close) if len(adj_close) >= 2 else pd.Series(dtype=float)
    for col, offset in _BETA_WINDOWS.items():
        out[col] = _trailing_beta(returns, first_ts, spy_returns, as_of_ts, offset)
    for col, bench in _CORR_COLUMNS.items():
        out[col] = _trailing_correlation(
            returns, first_ts, benchmark_returns_map.get(bench), as_of_ts, _CORR_WINDOW
        )

    for col, window in _SMA_WINDOWS.items():
        out[col] = _pct_above_sma(adj_close, window)

    price_close = float(prices["close"].iloc[-1])
    out["price_close"] = price_close
    month_volume = prices["volume"].loc[prices.index > as_of_ts - pd.DateOffset(months=1)]
    mean_volume = float(month_volume.mean())
    out["avg_volume_1m"] = None if pd.isna(mean_volume) else mean_volume
    out.update(_fundamentals_metrics(fundamentals, price_close))
    return out


def group_price_rows(rows: Iterable[tuple[Any, ...]]) -> dict[str, pd.DataFrame]:
    grouped: dict[str, dict[str, list[Any]]] = {}
    for ticker, date, adj_close, close, volume in rows:
        bucket = grouped.setdefault(
            ticker, {"date": [], "adj_close": [], "close": [], "volume": []}
        )
        bucket["date"].append(date)
        bucket["adj_close"].append(adj_close)
        bucket["close"].append(close)
        bucket["volume"].append(volume)
    return {
        ticker: pd.DataFrame(
            {
                "adj_close": cols["adj_close"],
                "close": cols["close"],
                "volume": cols["volume"],
            },
            index=pd.DatetimeIndex(pd.to_datetime(cols["date"])),
        )
        for ticker, cols in grouped.items()
    }


def _active_tickers(conn, *, tickers: list[str] | None, limit: int | None) -> list[str]:
    params: list[Any] = []
    where = "WHERE status = 'active'"
    if tickers:
        where += " AND ticker = ANY(%s)"
        params.append(tickers)
    sql = f"SELECT ticker FROM universe_constituents {where} ORDER BY ticker"
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [r[0] for r in cur.fetchall()]


def _load_price_frames(
    conn, tickers: list[str], start: _dt.date, end: _dt.date
) -> dict[str, pd.DataFrame]:
    if not tickers:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker, date, adj_close, close, volume
            FROM eod_prices
            WHERE ticker = ANY(%s) AND date >= %s AND date <= %s
            ORDER BY ticker, date
            """,
            (tickers, start, end),
        )
        return group_price_rows(cur.fetchall())


def _load_company_characteristics(conn, tickers: list[str]) -> dict[str, dict[str, Any]]:
    if not tickers:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                uc.ticker,
                cc.period_end,
                cc.book_equity,
                cc.total_assets,
                cc.net_income_ttm,
                cc.revenue,
                cc.gross_profit,
                cc.shares_outstanding,
                cc.quality_roa,
                cc.investment_growth,
                cc.profitability_gross
            FROM universe_constituents uc
            LEFT JOIN LATERAL (
                SELECT *
                FROM company_characteristics_monthly cc
                WHERE cc.cik = uc.cik
                ORDER BY cc.period_end DESC
                LIMIT 1
            ) cc ON true
            WHERE uc.ticker = ANY(%s)
            """,
            (tickers,),
        )
        cols = (
            "ticker",
            "period_end",
            "book_equity",
            "total_assets",
            "net_income_ttm",
            "revenue",
            "gross_profit",
            "shares_outstanding",
            "quality_roa",
            "investment_growth",
            "profitability_gross",
        )
        out: dict[str, dict[str, Any]] = {}
        for row in cur.fetchall():
            item = dict(zip(cols, row))
            ticker = item.pop("ticker")
            if item["period_end"] is not None:
                out[ticker] = item
        return out


def _upsert_metrics(conn, records: list[dict[str, Any]]) -> int:
    upserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(records), UPSERT_CHUNK):
            chunk = records[i : i + UPSERT_CHUNK]
            cur.executemany(UPSERT_SQL, chunk)
            conn.commit()
            upserted += len(chunk)
    return upserted


def _delete_inactive_metrics(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM screener_metrics sm
            WHERE NOT EXISTS (
                SELECT 1
                FROM universe_constituents uc
                WHERE uc.ticker = sm.ticker AND uc.status = 'active'
            )
            """
        )
        deleted = cur.rowcount
    conn.commit()
    return int(deleted)


def _refresh_screener_equity_snapshot(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW screener_equity_snapshot_mv")


def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
    tickers: list[str] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict:
    """Compute screener metrics for active constituents and refresh the MV."""
    started = time.monotonic()
    anchor = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()
    load_start = anchor - _dt.timedelta(days=LOOKBACK_DAYS)

    report = MetricsReport(null_counts=dict.fromkeys(METRIC_COLUMNS, 0))
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_SCREENER_METRICS) as got:
            if not got:
                return {"status": "skipped", "reason": "lock_busy", "computed": 0}

            todo = _active_tickers(conn, tickers=tickers, limit=limit)
            report.total_active = len(todo)
            report.deleted_inactive = _delete_inactive_metrics(conn)

            benchmark_frames = _load_price_frames(
                conn, list(BENCHMARK_TICKERS), load_start, anchor
            )
            benchmark_returns_map = {
                ticker: simple_returns(frame["adj_close"])
                for ticker, frame in benchmark_frames.items()
                if len(frame) >= 2
            }
            missing = set(BENCHMARK_TICKERS) - set(benchmark_returns_map)
            if missing:
                raise RuntimeError(f"Benchmark series unavailable: {sorted(missing)}")

            computed_at = _dt.datetime.now(_dt.UTC)
            for i in range(0, len(todo), batch_size):
                batch = todo[i : i + batch_size]
                frames = _load_price_frames(conn, batch, load_start, anchor)
                fundamentals = _load_company_characteristics(conn, batch)

                records: list[dict[str, Any]] = []
                for ticker in batch:
                    frame = frames.get(ticker)
                    if frame is None or frame.empty:
                        report.skipped_no_eod += 1
                        continue
                    as_of = pd.Timestamp(frame.index[-1]).date()
                    metrics = compute_ticker_metrics(
                        frame,
                        benchmark_returns_map,
                        fundamentals.get(ticker),
                        as_of,
                    )
                    for col in METRIC_COLUMNS:
                        if metrics[col] is None:
                            report.null_counts[col] += 1
                    records.append(
                        {"ticker": ticker, "computed_at": computed_at, "as_of": as_of}
                        | metrics
                    )

                if records:
                    report.computed += _upsert_metrics(conn, records)
                print(
                    f"screener_metrics: {report.computed}/{report.total_active} "
                    f"computed, skipped_no_eod={report.skipped_no_eod}",
                    flush=True,
                )

    if report.computed > 0:
        _refresh_screener_equity_snapshot(dsn)

    report.elapsed_seconds = time.monotonic() - started
    return {
        "status": "succeeded",
        "total_active": report.total_active,
        "computed": report.computed,
        "skipped_no_eod": report.skipped_no_eod,
        "deleted_inactive": report.deleted_inactive,
        "null_counts": report.null_counts,
        "elapsed_seconds": round(report.elapsed_seconds, 3),
        "as_of": anchor.isoformat(),
    }
