from __future__ import annotations

import datetime as dt
from contextlib import contextmanager

import pandas as pd
import pytest

from src.db import LOCK_REGIME_GATE, LOCK_SCREENER_METRICS
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
    assert LOCK_SCREENER_METRICS == 900_221
    assert LOCK_SCREENER_METRICS != LOCK_REGIME_GATE


class _CaptureCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = conn.delete_count

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.conn.sql.append((sql, params))

    def executemany(self, sql, records):
        self.conn.sql.append((sql, records))

    def fetchall(self):
        return self.conn.fetchall_rows

    def fetchone(self):
        return self.conn.fetchone_rows.pop(0)


class _CaptureConn:
    def __init__(self, *, fetchall_rows=None, fetchone_rows=None, delete_count=0):
        self.fetchall_rows = fetchall_rows or []
        self.fetchone_rows = list(fetchone_rows or [])
        self.delete_count = delete_count
        self.sql = []
        self.commits = 0

    def cursor(self):
        return _CaptureCursor(self)

    def commit(self):
        self.commits += 1


def test_eligible_tickers_uses_canonical_function_anchor_and_requested_filters():
    conn = _CaptureConn(fetchall_rows=[("AAA",), ("ZZZ",)])
    anchor = dt.date(2026, 8, 12)

    tickers = sm._eligible_tickers(
        conn, anchor, tickers=["AAA", "ZZZ"], limit=1
    )

    assert tickers == ["AAA", "ZZZ"]
    sql, params = conn.sql[-1]
    assert "screener_equity_eligible(%s)" in sql
    assert "status = 'active'" not in sql
    assert "ticker = ANY(%s)" in sql
    assert "LIMIT %s" in sql
    assert params == [anchor, ["AAA", "ZZZ"], 1]


def test_load_price_frames_selects_only_rows_usable_for_eligibility():
    conn = _CaptureConn()

    assert sm._load_price_frames(conn, ["AAA"], dt.date(2026, 1, 1), dt.date(2026, 8, 12)) == {}

    sql, _params = conn.sql[-1]
    assert "adj_close IS NOT NULL" in sql
    assert "close IS NOT NULL" in sql


def test_universe_counts_keep_active_eligible_and_excluded_honest():
    conn = _CaptureConn(fetchone_rows=[(4_988, 4_780)])
    anchor = dt.date(2026, 8, 12)

    active, eligible = sm._screener_universe_counts(conn, anchor)

    assert (active, eligible) == (4_988, 4_780)
    sql, params = conn.sql[-1]
    assert "status = 'active'" in sql
    assert "screener_equity_eligible(%s)" in sql
    assert params == (anchor,)


def test_upsert_metrics_does_not_commit_per_chunk(monkeypatch):
    monkeypatch.setattr(sm, "UPSERT_CHUNK", 1)
    conn = _CaptureConn()

    assert sm._upsert_metrics(conn, [{"ticker": "AAA"}, {"ticker": "BBB"}]) == 2

    assert conn.commits == 0
    assert len(conn.sql) == 2


def test_metric_parity_mismatch_raises_with_counts():
    conn = _CaptureConn(fetchone_rows=[(2, 1, 1, 0, 1)])

    with pytest.raises(RuntimeError, match=r"eligible=2.*current=1.*missing=1.*stale=1"):
        sm._verify_metric_parity(conn, dt.date(2026, 8, 12), dt.datetime(2026, 8, 12, tzinfo=dt.UTC))


def test_snapshot_parity_mismatch_raises_with_counts():
    conn = _CaptureConn(fetchone_rows=[(2, 1, 1, 0, 1)])

    with pytest.raises(RuntimeError, match=r"eligible=2.*snapshot=1.*missing=1.*stale=1"):
        sm._verify_snapshot_parity(conn, dt.date(2026, 8, 12), dt.datetime(2026, 8, 12, tzinfo=dt.UTC))


def test_refresh_pins_the_worker_anchor_in_the_publish_transaction():
    conn = _CaptureConn()
    anchor = dt.date(2026, 8, 12)

    sm._refresh_screener_equity_snapshot(conn, anchor)

    assert conn.sql == [
        (
            "SELECT set_config('investintell.screener_as_of', %s, true)",
            (anchor.isoformat(),),
        ),
        ("REFRESH MATERIALIZED VIEW screener_equity_snapshot_mv", None),
    ]


def test_publish_lock_is_transaction_scoped_and_nonblocking():
    conn = _CaptureConn(fetchone_rows=[(True,)])

    assert sm._try_publish_lock(conn) is True

    sql, params = conn.sql[-1]
    assert "pg_try_advisory_xact_lock" in sql
    assert "pg_try_advisory_lock" not in sql
    assert params == (LOCK_SCREENER_METRICS,)


@contextmanager
def _held_lock():
    yield True


class _RunConn(_CaptureConn):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.events = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_args):
        self.events.append("connection-exit")
        if exc_type is None:
            self.commits += 1
        return False

    def commit(self):
        self.events.append("explicit-commit")
        self.commits += 1


def _wire_run(monkeypatch, conn, *, selected, frames):
    monkeypatch.setattr(sm, "connect", lambda *_args, **_kwargs: conn)
    monkeypatch.setattr(sm, "_publish_transaction_lock", lambda *_args: _held_lock())
    monkeypatch.setattr(sm, "_eligible_tickers", lambda *_args, **_kwargs: selected)
    monkeypatch.setattr(
        sm,
        "_screener_universe_counts",
        lambda *_args, **_kwargs: (len(selected), len(selected)),
    )
    monkeypatch.setattr(sm, "_load_price_frames", lambda *_args, **_kwargs: frames)
    monkeypatch.setattr(sm, "_load_company_characteristics", lambda *_args, **_kwargs: {})


def test_full_run_aborts_when_an_eligible_frame_is_missing_and_does_not_refresh(monkeypatch):
    conn = _RunConn()
    frames = {ticker: _price_frame() for ticker in sm.BENCHMARK_TICKERS}
    frames["AAA"] = _price_frame()
    _wire_run(monkeypatch, conn, selected=["AAA", "BBB"], frames=frames)
    refreshed = []
    monkeypatch.setattr(sm, "_refresh_screener_equity_snapshot", lambda *_args: refreshed.append(True))

    with pytest.raises(RuntimeError, match="computed=1.*eligible=2"):
        sm.run("postgresql://unused", batch_size=10)

    assert refreshed == []


def test_partial_and_historical_runs_never_refresh(monkeypatch):
    conn = _RunConn()
    frames = {ticker: _price_frame() for ticker in (*sm.BENCHMARK_TICKERS, "AAA")}
    _wire_run(monkeypatch, conn, selected=["AAA"], frames=frames)
    refreshed = []
    monkeypatch.setattr(sm, "_refresh_screener_equity_snapshot", lambda *_args: refreshed.append(True))

    partial = sm.run("postgresql://unused", tickers=["AAA"])
    historical = sm.run("postgresql://unused", calc_date="2026-08-12")

    assert partial["published"] is False
    assert historical["published"] is False
    assert refreshed == []


def test_full_run_publishes_only_after_compute_delete_and_both_parity_checks(monkeypatch):
    conn = _RunConn()
    frames = {ticker: _price_frame() for ticker in (*sm.BENCHMARK_TICKERS, "AAA")}
    _wire_run(monkeypatch, conn, selected=["AAA"], frames=frames)
    order = []
    monkeypatch.setattr(sm, "_upsert_metrics", lambda *_args: order.append("compute") or 1)
    monkeypatch.setattr(sm, "_delete_ineligible_metrics", lambda *_args: order.append("delete") or 0)
    monkeypatch.setattr(sm, "_verify_metric_parity", lambda *_args: order.append("metric-parity"))
    monkeypatch.setattr(sm, "_refresh_screener_equity_snapshot", lambda *_args: order.append("refresh"))
    monkeypatch.setattr(sm, "_verify_snapshot_parity", lambda *_args: order.append("snapshot-parity"))

    report = sm.run("postgresql://unused")

    assert order == ["compute", "delete", "metric-parity", "refresh", "snapshot-parity"]
    assert report["published"] is True
    assert report["eligible"] == 1
    assert report["excluded"] == 0
    assert report["selected"] == 1
    assert report["coverage_as_of"] == report["anchor"]
    assert conn.events == ["explicit-commit", "connection-exit"]


def test_full_run_refuses_an_empty_canonical_eligible_set(monkeypatch):
    conn = _RunConn()
    frames = {ticker: _price_frame() for ticker in sm.BENCHMARK_TICKERS}
    _wire_run(monkeypatch, conn, selected=[], frames=frames)
    monkeypatch.setattr(
        sm,
        "_screener_universe_counts",
        lambda *_args, **_kwargs: (4_988, 0),
    )

    with pytest.raises(RuntimeError, match="eligible set is empty"):
        sm.run("postgresql://unused")

    assert "explicit-commit" not in conn.events


def test_snapshot_validation_failure_rolls_back_without_publishing(monkeypatch):
    conn = _RunConn()
    frames = {ticker: _price_frame() for ticker in (*sm.BENCHMARK_TICKERS, "AAA")}
    _wire_run(monkeypatch, conn, selected=["AAA"], frames=frames)
    refreshed_with = []
    monkeypatch.setattr(sm, "_upsert_metrics", lambda *_args: 1)
    monkeypatch.setattr(sm, "_delete_ineligible_metrics", lambda *_args: 0)
    monkeypatch.setattr(sm, "_verify_metric_parity", lambda *_args: None)
    monkeypatch.setattr(
        sm,
        "_refresh_screener_equity_snapshot",
        lambda publish_conn, _anchor: refreshed_with.append(publish_conn),
    )
    monkeypatch.setattr(
        sm,
        "_verify_snapshot_parity",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("snapshot mismatch")),
    )

    with pytest.raises(RuntimeError, match="snapshot mismatch"):
        sm.run("postgresql://unused")

    assert refreshed_with == [conn]
    assert "explicit-commit" not in conn.events
