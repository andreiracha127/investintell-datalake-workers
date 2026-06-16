"""Tests for the IPCA gamma drift monitor (Tier 3, T3B-3).

compute_gamma_drift is pure (Procrustes-aligned relative Frobenius drift); it
is rotation/sign invariant per Kelly-Pruitt-Su identification. The reader
monitor_gamma_drift pulls the two latest gamma_loadings from factor_model_fits
and is tested against a fake psycopg connection (no live DB).
"""

from __future__ import annotations

import numpy as np
import pytest

from src.workers.gamma_drift import (
    DRIFT_THRESHOLD,
    compute_gamma_drift,
    monitor_gamma_drift,
)


def test_identical_gamma_zero_drift():
    g = np.array([[1.0, 0.0], [0.0, 1.0], [0.5, -0.5]])
    assert compute_gamma_drift(g, g) == pytest.approx(0.0, abs=1e-12)


def test_sign_flip_is_zero_drift():
    # A pure sign flip is a valid IPCA equivalence -> drift 0 after Procrustes.
    g = np.array([[1.0, 0.2], [0.3, 1.0], [-0.4, 0.5]])
    assert compute_gamma_drift(g, -g) == pytest.approx(0.0, abs=1e-9)


def test_orthogonal_rotation_is_zero_drift():
    rng = np.random.default_rng(3)
    g, _ = np.linalg.qr(rng.standard_normal((5, 2)))
    theta = 0.7
    rot = np.array([[np.cos(theta), -np.sin(theta)], [np.sin(theta), np.cos(theta)]])
    assert compute_gamma_drift(g, g @ rot) == pytest.approx(0.0, abs=1e-9)


def test_real_drift_is_positive():
    g_old = np.array([[1.0, 0.0], [0.0, 1.0], [0.0, 0.0]])
    g_new = np.array([[1.0, 0.0], [0.0, 1.0], [0.5, 0.5]])  # genuine new loading
    drift = compute_gamma_drift(g_old, g_new)
    assert drift > 0.0


def test_shape_mismatch_raises():
    with pytest.raises(ValueError, match="Shape mismatch"):
        compute_gamma_drift(np.ones((3, 2)), np.ones((3, 3)))


def test_non_finite_raises():
    g = np.array([[1.0, np.nan], [0.0, 1.0]])
    with pytest.raises(ValueError, match="finite"):
        compute_gamma_drift(g, np.ones((2, 2)))


def test_non_2d_raises():
    with pytest.raises(ValueError, match="2D"):
        compute_gamma_drift(np.ones(4), np.ones(4))


# --- reader against a fake psycopg connection -------------------------------
class _FakeCur:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _FakeCur(self._rows)


def test_monitor_returns_none_with_one_fit():
    conn = _FakeConn([([[1.0, 0.0], [0.0, 1.0]],)])  # only one gamma row
    out = monitor_gamma_drift(conn, universe_hash="abc", engine="ipca",
                              asset_class="Equity")
    assert out is None


def test_monitor_flags_alert_on_large_drift():
    g_old = [[1.0, 0.0], [0.0, 1.0], [0.0, 0.0]]
    g_new = [[1.0, 0.0], [0.0, 1.0], [3.0, 3.0]]  # big genuine drift
    # rows ordered newest first (matches the ORDER BY fit_date DESC LIMIT 2).
    conn = _FakeConn([(g_new,), (g_old,)])
    out = monitor_gamma_drift(conn, universe_hash="abc", engine="ipca",
                              asset_class="Equity")
    assert out is not None
    assert out["drift"] > DRIFT_THRESHOLD
    assert out["alert"] is True


def test_monitor_no_alert_on_small_drift():
    g_old = [[1.0, 0.0], [0.0, 1.0], [0.0, 0.0]]
    g_new = [[1.0, 0.0], [0.0, 1.0], [0.01, 0.0]]  # tiny drift
    conn = _FakeConn([(g_new,), (g_old,)])
    out = monitor_gamma_drift(conn, universe_hash="abc", engine="ipca",
                              asset_class="Equity")
    assert out is not None
    assert out["drift"] < DRIFT_THRESHOLD
    assert out["alert"] is False
