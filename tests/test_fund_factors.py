# tests/test_fund_factors.py
import numpy as np

import src.workers.fund_factors as ff


def test_ols_factor_exposures_recovers_known_betas():
    rng = np.random.default_rng(0)
    n = 120
    f1 = rng.normal(size=n)
    f2 = rng.normal(size=n)
    y = 0.3 * f1 - 0.5 * f2 + rng.normal(scale=1e-6, size=n)  # ruído ínfimo
    out = ff.ols_factor_exposures(y, np.column_stack([f1, f2]))
    betas = {row["factor"]: row["beta"] for row in out}
    assert abs(betas["Factor 1"] - 0.3) < 1e-3
    assert abs(betas["Factor 2"] + 0.5) < 1e-3
    assert all(row["significance"] == "***" for row in out)  # |t| enorme


def test_ols_short_series_returns_empty():
    assert ff.ols_factor_exposures(np.zeros(3), np.zeros((3, 2))) == []


class _FakeCursor:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, *_a): self._sink["sql"] = " ".join(str(sql).split())


class _FakeConn:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return _FakeCursor(self._sink)


def test_refresh_latest_mv_concurrently_autocommit(monkeypatch):
    sink = {}
    def _fake_connect(dsn=None, *, autocommit=False):
        sink["autocommit"] = autocommit
        return _FakeConn(sink)
    monkeypatch.setattr(ff, "connect", _fake_connect)
    ff._refresh_latest_mv("postgres://x")
    assert sink["autocommit"] is True
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_factor_exposures_latest_mv" in sink["sql"]
