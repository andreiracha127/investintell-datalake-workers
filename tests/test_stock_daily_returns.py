"""Unit test for the stock_daily_returns worker.

Style mirrors tests/test_risk_metrics.py: a fake connection captures autocommit
and the upserted rows, returning fixed price rows. The test asserts return_1d is
the relative change between consecutive adj_closes per ticker (first point NULL).
"""

from __future__ import annotations

import src.workers.stock_daily_returns as sdr


class _FakeCursor:
    def __init__(self, sink, rows):
        self._sink = sink
        self._rows = rows
        self._last = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._sink.setdefault("sql", []).append(sql)
        self._last = sql

    def executemany(self, sql, rows):
        self._sink.setdefault("upserts", []).extend(list(rows))

    def fetchone(self):
        return (True,)  # pg_try_advisory_lock -> got

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, sink, rows):
        self._sink = sink
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return _FakeCursor(self._sink, self._rows)

    def commit(self):
        self._sink["committed"] = True


def test_computes_return_1d_per_ticker_first_point_null(monkeypatch):
    import datetime as dt

    sink: dict = {}
    rows = [
        ("AAPL", dt.date(2026, 6, 16), 100.0),
        ("AAPL", dt.date(2026, 6, 17), 110.0),
        ("AAPL", dt.date(2026, 6, 18), 99.0),
    ]

    def _fake_connect(dsn=None, *, autocommit=False):
        sink["autocommit"] = autocommit
        return _FakeConn(sink, rows)

    monkeypatch.setattr(sdr, "connect", _fake_connect)
    result = sdr.run("postgres://x")

    upserts = {(t, d): r for (t, d, r, _ac) in sink["upserts"]}
    assert upserts[("AAPL", dt.date(2026, 6, 16))] is None
    assert abs(upserts[("AAPL", dt.date(2026, 6, 17))] - 0.10) < 1e-9
    assert abs(upserts[("AAPL", dt.date(2026, 6, 18))] - (99.0 / 110.0 - 1.0)) < 1e-9
    assert result["tickers"] == 1
    assert result["upserted"] == 3
