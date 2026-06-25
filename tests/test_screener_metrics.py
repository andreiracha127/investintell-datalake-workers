from __future__ import annotations

import datetime as dt

import pandas as pd

from src.db import LOCK_SCREENER_METRICS
from src.workers import screener_metrics as sm


def _price_frame(days: int = 800) -> pd.DataFrame:
    start = dt.date(2024, 1, 1)
    dates = [start + dt.timedelta(days=i) for i in range(days)]
    adj = [100.0 + i * 0.1 for i in range(days)]
    return pd.DataFrame(
        {
            "adj_close": adj,
            "close": adj,
            "volume": [1_000_000 + i for i in range(days)],
        },
        index=pd.DatetimeIndex(pd.to_datetime(dates)),
    )


def test_compute_ticker_metrics_includes_price_and_company_fundamentals():
    prices = _price_frame()
    bench_returns = {
        ticker: sm.simple_returns(prices["adj_close"])
        for ticker in sm.BENCHMARK_TICKERS
    }
    fundamentals = {
        "period_end": dt.date(2025, 12, 31),
        "book_equity": 500.0,
        "total_assets": 900.0,
        "net_income_ttm": 100.0,
        "revenue": 1_000.0,
        "gross_profit": 400.0,
        "shares_outstanding": 10.0,
        "quality_roa": 0.1111,
        "investment_growth": 0.05,
        "profitability_gross": 0.4,
    }

    out = sm.compute_ticker_metrics(
        prices,
        bench_returns,
        fundamentals,
        pd.Timestamp(prices.index[-1]).date(),
    )

    assert set(sm.METRIC_COLUMNS) <= set(out)
    assert out["price_close"] == prices["close"].iloc[-1]
    assert out["market_cap"] == fundamentals["shares_outstanding"] * out["price_close"]
    assert out["pe_ratio"] == out["market_cap"] / fundamentals["net_income_ttm"]
    assert out["roe"] == fundamentals["net_income_ttm"] / fundamentals["book_equity"]
    assert out["roa"] == fundamentals["quality_roa"]
    assert out["gross_margin"] == 0.4
    assert out["de_ratio"] == 0.8
    assert out["fundamentals_period_end"] == fundamentals["period_end"]
    assert out["ret_1y"] is not None
    assert out["vol_1y"] is not None
    assert out["pct_above_sma200"] is not None


def test_group_price_rows_builds_per_ticker_frames():
    rows = [
        ("AAPL", dt.date(2026, 1, 1), 10.0, 10.1, 100),
        ("AAPL", dt.date(2026, 1, 2), 11.0, 11.1, 200),
        ("MSFT", dt.date(2026, 1, 1), 20.0, 20.1, 300),
    ]

    grouped = sm.group_price_rows(rows)

    assert sorted(grouped) == ["AAPL", "MSFT"]
    assert list(grouped["AAPL"]["adj_close"]) == [10.0, 11.0]
    assert list(grouped["MSFT"]["volume"]) == [300]


def test_upsert_sql_targets_screener_metrics_and_all_columns():
    assert "INSERT INTO screener_metrics" in sm.UPSERT_SQL
    assert "ON CONFLICT (ticker) DO UPDATE" in sm.UPSERT_SQL
    for col in ("computed_at", "as_of", *sm.METRIC_COLUMNS):
        assert f"{col} = EXCLUDED.{col}" in sm.UPSERT_SQL
    assert "fundamentals_snapshot" not in sm.UPSERT_SQL


def test_company_characteristics_query_uses_direct_company_source(monkeypatch):
    captured = {}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params

        def fetchall(self):
            return []

    class Conn:
        def cursor(self):
            return Cursor()

    assert sm._load_company_characteristics(Conn(), ["AAPL"]) == {}
    assert "company_characteristics_monthly" in captured["sql"]
    assert "fundamentals_snapshot" not in captured["sql"]
    assert "universe_constituents" in captured["sql"]


def test_screener_lock_id_is_registered():
    assert LOCK_SCREENER_METRICS == 900_207
