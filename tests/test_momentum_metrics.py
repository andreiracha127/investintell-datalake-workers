from __future__ import annotations

import datetime as dt

import numpy as np
import pytest

from src.workers import momentum_metrics as mm


def test_rsi_14_rising_path_is_100():
    nav = np.linspace(100.0, 114.0, 15)
    assert mm.rsi_14(nav) == pytest.approx(100.0)


def test_bollinger_position_flat_path_is_mid_band():
    nav = np.array([100.0] * 20)
    assert mm.bollinger_position(nav) == pytest.approx(0.5)


def test_compute_nav_momentum_sets_blended_to_nav_when_flow_missing():
    nav = [100.0 + i * 0.2 for i in range(90)]
    out = mm.compute_nav_momentum(nav)
    assert out["rsi_14"] == pytest.approx(100.0)
    assert out["dtw_drift_score"] is not None
    assert out["nav_momentum_score"] is not None
    assert out["flow_momentum_score"] is None
    assert out["blended_momentum_score"] == out["nav_momentum_score"]


def test_compute_nav_momentum_requires_minimum_history():
    out = mm.compute_nav_momentum([100.0] * 10)
    assert all(value is None for value in out.values())


def test_compute_nav_momentum_uses_talib_backend_when_available(monkeypatch):
    calls: list[str] = []

    class FakeTalib:
        @staticmethod
        def RSI(close, timeperiod):
            calls.append(f"RSI:{timeperiod}")
            return np.array([np.nan] * (len(close) - 1) + [40.0])

        @staticmethod
        def BBANDS(close, timeperiod, nbdevup, nbdevdn):
            calls.append(f"BBANDS:{timeperiod}:{nbdevup}:{nbdevdn}")
            upper = np.array([np.nan] * (len(close) - 1) + [120.0])
            middle = np.array([np.nan] * len(close))
            lower = np.array([np.nan] * (len(close) - 1) + [80.0])
            return upper, middle, lower

    monkeypatch.setattr(mm, "_TALIB", FakeTalib)
    out = mm.compute_nav_momentum([100.0] * 30)
    assert calls == ["RSI:14", "BBANDS:20:2:2"]
    assert out["rsi_14"] == pytest.approx(40.0)
    assert out["bb_position"] == pytest.approx(50.0)
    assert out["nav_momentum_score"] == pytest.approx(45.0)


def test_compute_nport_flow_momentum_scores_reported_inflows_above_neutral():
    score = mm.compute_nport_flow_momentum([0.01, 0.015, 0.02, 0.025, 0.03])
    assert score is not None
    assert score > 50.0


def test_compute_nport_flow_momentum_scores_reported_redemptions_below_neutral():
    score = mm.compute_nport_flow_momentum([-0.01, -0.015, -0.02, -0.025, -0.03])
    assert score is not None
    assert score < 50.0


def _nav_aum_points(aum_step: float) -> list[mm.NavAumPoint]:
    start = dt.date(2024, 1, 1)
    points = []
    for i in range(90):
        nav = 100.0 + i * 0.2
        # Exact NAV-performance AUM would be proportional to nav. The step is
        # external daily flow layered on top.
        aum = 1_000_000.0 * (nav / 100.0) + i * aum_step
        points.append(mm.NavAumPoint(start + dt.timedelta(days=i), nav, aum))
    return points


def test_compute_daily_flow_pct_removes_nav_performance_from_aum_change():
    points = _nav_aum_points(aum_step=0.0)
    flows = mm.compute_daily_flow_pct(points)
    assert len(flows) == 89
    assert max(abs(v) for v in flows) < 1e-12


def test_compute_daily_flow_momentum_scores_fresh_redemptions_below_neutral():
    points = _nav_aum_points(aum_step=-1_500.0)
    score = mm.compute_daily_flow_momentum(mm.compute_daily_flow_pct(points))
    assert score is not None
    assert score < 50.0


def test_compute_momentum_blends_nav_and_daily_flow_scores_keeps_nport_separate():
    points = _nav_aum_points(aum_step=-1_500.0)
    out = mm.compute_momentum(
        points,
        [0.03, 0.025, 0.02, 0.015, 0.01],
        nport_as_of=dt.date(2024, 3, 31),
        calc_date=dt.date(2024, 6, 30),
    )
    assert out["nav_momentum_score"] is not None
    assert out["flow_momentum_score"] is not None
    assert out["nport_flow_momentum_score"] is not None
    assert out["nport_flow_as_of"] == dt.date(2024, 3, 31)
    assert out["nport_flow_staleness_days"] == 91
    assert out["blended_momentum_score"] == pytest.approx(
        0.5 * out["nav_momentum_score"] + 0.5 * out["flow_momentum_score"]
    )
