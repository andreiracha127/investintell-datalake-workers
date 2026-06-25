"""Benchmark source = proxy ETF (eod_prices) for the risk_metrics worker.

These hit the cloud data-lake (DATABASE_URL) to prove the new wiring returns
proxy-ETF tickers and reads return series from eod_prices, not benchmark_nav.
Self-skips if DATABASE_URL is unset / the cloud is unreachable.
"""
from __future__ import annotations

import datetime as _dt
import os

import psycopg
import pytest

from src.workers import risk_metrics as rm

CLOUD_DSN = os.environ.get("DATABASE_URL", "")


def _cloud():
    if not CLOUD_DSN:
        pytest.skip("DATABASE_URL not set")
    try:
        return psycopg.connect(CLOUD_DSN, connect_timeout=10)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"cloud unreachable: {exc}")


def test_fund_benchmarks_returns_proxy_tickers():
    with _cloud() as conn:
        m = rm._fetch_fund_benchmarks(conn)
    assert m, "expected a non-empty fund->ticker map"
    vals = set(m.values())
    # proxy tickers, not block_ids like 'na_equity_large'
    assert rm.EQUITY_BENCHMARK_KEY in vals
    assert all("_" not in v and v == v.upper() for v in vals)


def test_benchmark_returns_keyed_by_ticker_from_eod():
    with _cloud() as conn:
        br = rm._fetch_benchmark_returns(conn, _dt.date(2024, 12, 31))
    assert rm.EQUITY_BENCHMARK_KEY in br and len(br[rm.EQUITY_BENCHMARK_KEY]) > 200
    _d, r = br[rm.EQUITY_BENCHMARK_KEY][-1]
    assert isinstance(r, float) and abs(r) < 0.5


def test_equity_crisis_benchmark_from_eod():
    with _cloud() as conn:
        eq = rm._fetch_equity_crisis_benchmark(conn, _dt.date(2024, 12, 31))
    # long window must span multiple crises → plenty of daily returns
    assert len(eq) > 1000
    assert all(abs(r) < 0.5 for _d, r in eq[-50:])
