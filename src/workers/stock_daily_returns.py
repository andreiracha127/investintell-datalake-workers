"""Materializa daily simple returns por ticker em stock_daily_returns.

Por que worker e não continuous aggregate: return_1d = adj_close[t]/adj_close[t-1]-1
exige lag()/self-join, e o TimescaleDB proíbe window functions e self-joins em
continuous aggregates. Por isso (igual a nav_timeseries.return_1d nos fundos) o
retorno de stock é computado em Python e upsertado numa tabela base worker-owned
(schemas/stock_daily_returns.sql). Idempotente (ON CONFLICT DO UPDATE). O refresh
roda dentro de um advisory lock próprio (900_211) para não correr contra si mesmo.
"""

from __future__ import annotations

from src.db import LOCK_STOCK_DAILY_RETURNS, advisory_lock, connect

_SELECT = """
    SELECT ticker, date, adj_close
    FROM eod_prices
    WHERE adj_close IS NOT NULL AND adj_close > 0
    ORDER BY ticker, date
"""

_UPSERT = """
    INSERT INTO stock_daily_returns (ticker, date, return_1d, adj_close)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (ticker, date)
    DO UPDATE SET return_1d = EXCLUDED.return_1d, adj_close = EXCLUDED.adj_close
"""


def run(dsn: str) -> dict:
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_STOCK_DAILY_RETURNS) as got:
            if not got:
                return {"tickers": 0, "upserted": 0, "skipped": "lock_busy"}
            with conn.cursor() as cur:
                cur.execute(_SELECT)
                price_rows = cur.fetchall()

            payload: list[tuple] = []
            tickers: set[str] = set()
            prev_ticker: str | None = None
            prev_close: float | None = None
            for ticker, date, adj_close in price_rows:
                tickers.add(ticker)
                close = float(adj_close)
                if ticker != prev_ticker:
                    ret = None  # first observation per ticker has no return
                else:
                    ret = close / prev_close - 1.0 if prev_close else None
                payload.append((ticker, date, ret, close))
                prev_ticker, prev_close = ticker, close

            if payload:
                with conn.cursor() as cur:
                    cur.executemany(_UPSERT, payload)
                conn.commit()
            return {"tickers": len(tickers), "upserted": len(payload)}
