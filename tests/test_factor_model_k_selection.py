"""Tests for IPCA K-selection (Tier 3, T3B-2).

select_k grids K over [1, min(max_k, L)], scores each with walk-forward OOS R²,
and applies a >=3-fold reliability gate. Uses a self-contained synthetic-panel
generator (same DGP as test_factor_model) so the recovered best K matches the
true K of the data-generating process.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.workers import factor_model as fm


def _make_synthetic(*, L=4, K=2, T=120, N=60, noise=0.01, seed=7):
    """r_{i,t} = (z_{i,t} Gamma) f_t + noise. Returns (chars, returns, Gamma)."""
    rng = np.random.default_rng(seed)
    G_true, _ = np.linalg.qr(rng.standard_normal((L, K)))
    F = rng.standard_normal((K, T)) * 0.05
    months = pd.date_range("2010-01-31", periods=T, freq="ME")
    frames = []
    for t in range(T):
        Z = rng.standard_normal((N, L))
        beta = Z @ G_true
        r = beta @ F[:, t] + rng.standard_normal(N) * noise
        df = pd.DataFrame(Z, columns=fm.CHARS_COLS[:L])
        df["instrument_id"] = [f"id_{i:03d}" for i in range(N)]
        df["month"] = months[t]
        df["monthly_return"] = r
        frames.append(df)
    panel = pd.concat(frames, ignore_index=True)
    panel = panel.set_index(["instrument_id", "month"]).sort_index()
    return panel[fm.CHARS_COLS[:L]], panel["monthly_return"], G_true


def test_count_oos_folds_matches_window_math():
    # Expanding window: first fold at i=min_train, slide by test_window while
    # i + test_window <= n.
    chars, _returns, _ = _make_synthetic(T=120)
    n = chars.index.get_level_values("month").nunique()
    folds = fm._count_oos_folds(n, min_train=36, test_window=12)
    expected = len(range(36, n - 12 + 1, 12))
    assert folds == expected
    assert folds >= 3


def test_select_k_recovers_true_k():
    chars, returns, _ = _make_synthetic(K=2, T=120, noise=0.005)
    sel = fm.select_k(chars, returns, max_k=4, min_train=36, test_window=12)
    assert sel["best_k"] == 2
    assert sel["degraded"] is False
    assert sel["insufficient_folds"] is False
    assert sel["n_folds"] >= fm.MIN_FOLDS_FOR_K_SELECTION
    # best OOS R2 is the max over the grid and plausible for a strong signal.
    assert sel["best_oos_r_squared"] > 0.4
    # every K in the grid (1..L=4) has a recorded mean OOS score.
    assert set(sel["k_scores"].keys()) == {1, 2, 3, 4}


def test_select_k_degraded_when_too_few_folds():
    # T short enough that the expanding window yields < 3 folds -> degraded
    # fallback to the smallest K with any valid fold.
    # _count_oos_folds(50, 36, 12) = len(range(36, 39, 12)) = 1 < 3.
    chars, returns, _ = _make_synthetic(K=2, T=50, noise=0.01)
    sel = fm.select_k(chars, returns, max_k=3, min_train=36, test_window=12)
    assert sel["n_folds"] == 1
    assert sel["insufficient_folds"] is True
    assert sel["degraded"] is True
    assert sel["best_k"] == 1  # smallest K fallback
    assert sel["degraded_reason"] == "ipca_k_selection_insufficient_folds"


def test_select_k_raises_when_no_valid_fold():
    # n < min_train + test_window -> oos_r_squared returns None for every K.
    chars, returns, _ = _make_synthetic(K=2, T=20)
    with pytest.raises(ValueError, match="could not validate any K"):
        fm.select_k(chars, returns, max_k=3, min_train=36, test_window=12)


def test_select_k_clamps_grid_to_n_chars():
    # max_k above the number of instrument characteristics (L) is clamped to L.
    chars, returns, _ = _make_synthetic(L=3, K=2, T=120, noise=0.005)
    sel = fm.select_k(chars, returns, max_k=6, min_train=36, test_window=12)
    assert max(sel["k_scores"].keys()) == 3  # clamped to L=3
