# tests/test_fund_factors.py
import datetime as dt

import numpy as np
import pytest

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
    assert [row["factor_index"] for row in out] == [1, 2]
    assert all(row["n_observations"] == n for row in out)
    assert all(row["r_squared"] > 0.999 for row in out)


def test_ols_short_series_returns_empty():
    assert ff.ols_factor_exposures(np.zeros(3), np.zeros((3, 2))) == []


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (float("nan"), None),
        (float("inf"), None),
        (float("-inf"), None),
        (999_999.99999999, 999_999.99999999),
        (-999_999.99999999, -999_999.99999999),
        (1_000_000.0, None),
        (-1_000_000.0, None),
        (0.123456789, 0.12345679),
    ],
)
def test_numeric_14_8_normalizes_storage_values(value, expected):
    assert ff._numeric_14_8(value) == expected


def test_near_perfect_fit_t_stat_is_not_persistable():
    rng = np.random.default_rng(0)
    n = 120
    f1 = rng.normal(size=n)
    f2 = rng.normal(size=n)
    y = 0.3 * f1 - 0.5 * f2 + rng.normal(scale=1e-6, size=n)

    out = ff.ols_factor_exposures(y, np.column_stack([f1, f2]))

    stored = [ff._storage_values(row) for row in out]
    assert all(beta is not None for beta, _, _ in stored)
    assert all(t_stat is None for _, t_stat, _ in stored)
    assert all(significance is None for _, _, significance in stored)


def test_monthly_returns_reject_gap_in_nav_months():
    class _Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, *_args): return None
        def fetchall(self):
            return [
                (dt.date(2026, 1, 1), 100.0),
                (dt.date(2026, 3, 1), 121.0),
                (dt.date(2026, 4, 1), 133.1),
            ]

    class _Conn:
        def cursor(self): return _Cursor()

    result = ff._fund_monthly_returns(
        _Conn(),
        "fund-id",
        [dt.date(2026, 3, 1), dt.date(2026, 4, 1)],
    )
    assert np.isnan(result[0])
    np.testing.assert_allclose(result[1], 0.1)


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
