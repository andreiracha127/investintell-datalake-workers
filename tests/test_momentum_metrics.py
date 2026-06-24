from __future__ import annotations

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

