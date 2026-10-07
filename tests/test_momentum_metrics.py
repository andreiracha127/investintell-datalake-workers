from __future__ import annotations

import contextlib
import datetime as dt

import numpy as np
import pytest

from src.workers import momentum_metrics as mm

# A run-up followed by a choppy pullback. A simple (Cutler) mean of the last 14
# deltas gives RSI 38.68 here; Wilder's smoothing over the full series gives 60.30.
REFERENCE_NAV = np.array(
    [
        100.00, 100.85, 101.62, 102.10, 103.05, 103.88, 104.41, 105.30, 106.02, 106.75,
        107.60, 108.12, 108.95, 109.70, 110.38, 110.02, 110.91, 110.20, 109.64, 110.15,
        109.32, 108.80, 109.45, 108.67, 108.10, 108.92, 108.31, 107.75, 108.40, 107.96,
    ]
)
# talib.RSI(REFERENCE_NAV, 14)[-1], identical to 1e-12 under TA-Lib 0.6.5 and 0.8.1.
REFERENCE_WILDER_RSI = 60.3017625561
# Last NAV's position inside talib.BBANDS(REFERENCE_NAV, 20, 2, 2).
REFERENCE_TALIB_BB_POSITION = 0.2052784239


def test_rsi_14_rising_path_is_100():
    nav = np.linspace(100.0, 114.0, 15)
    assert mm.rsi_14(nav) == pytest.approx(100.0)


def test_rsi_14_falling_path_is_0():
    nav = np.linspace(114.0, 100.0, 15)
    assert mm.rsi_14(nav) == 0.0


@pytest.mark.parametrize("level", [1.0, 10.37, 103.47])
def test_rsi_14_flat_path_is_undefined(level):
    # TA-Lib returns 0.0 here; the RSI is 0/0, so there is no signal.
    assert mm.rsi_14(np.full(260, level)) is None


def test_rsi_14_movement_below_tolerance_is_undefined():
    # TA-Lib 0.6.4 returns 0.0 below 1e-14 and 100.0 above it.
    tiny = 1.0 + np.cumsum(np.r_[0.0, np.full(59, 0.9e-14)])
    small = 1.0 + np.cumsum(np.r_[0.0, np.full(59, 1.1e-14)])
    assert mm.rsi_14(tiny) is None
    assert mm.rsi_14(small) == pytest.approx(100.0)


def test_rsi_14_matches_hard_coded_wilder_reference():
    assert abs(mm.rsi_14(REFERENCE_NAV) - REFERENCE_WILDER_RSI) < 1e-6


def test_rsi_14_matches_talib_rsi():
    talib = pytest.importorskip("talib")
    rng = np.random.default_rng(20261007)
    series = [REFERENCE_NAV, np.linspace(100.0, 114.0, 15)]
    for length in (15, 16, 30, 61, 260):
        series.append(100.0 * np.cumprod(1.0 + rng.normal(0.0004, 0.012, length)))
    for nav in series:
        expected = float(talib.RSI(nav, timeperiod=14)[-1])
        assert abs(mm.rsi_14(nav) - expected) < 1e-6


@pytest.mark.parametrize("level", [1.0, 10.37, 103.47])
def test_bollinger_position_flat_window_has_no_band(level):
    nav = np.r_[np.linspace(level * 0.9, level * 1.1, 40), np.full(20, level)]
    assert mm.bollinger_position(nav) is None


def test_bollinger_position_uses_population_stdev_like_talib():
    assert abs(mm.bollinger_position(REFERENCE_NAV) - REFERENCE_TALIB_BB_POSITION) < 1e-6


def test_numpy_fallback_matches_talib_nav_signal(monkeypatch):
    talib = pytest.importorskip("talib")
    monkeypatch.setattr(mm, "_TALIB", talib)
    rng = np.random.default_rng(7)
    for _ in range(50):
        nav = 100.0 * np.cumprod(1.0 + rng.normal(0.0004, 0.012, 260))
        rsi_ta, bb_ta, score_ta = mm._talib_nav_signal(nav)
        rsi_np, bb_np, score_np = mm._numpy_nav_signal(nav)
        assert abs(rsi_np - rsi_ta) < 1e-6
        # bollinger_position rounds to 6 decimals on the 0..1 scale before the
        # x100 scaling, so the band term can differ by up to 5e-5 points.
        assert abs(bb_np - bb_ta) < 1e-4
        assert abs(score_np - score_ta) < 1e-4


def _flat_tail_nav(level: float, seed: int) -> np.ndarray:
    """230 moving NAVs rounded to 4 decimals, then 30 days at the last one."""

    rng = np.random.default_rng(seed)
    moving = np.round(level * np.cumprod(1.0 + rng.normal(0.0, 0.01, 230)), 4)
    return np.r_[moving, np.full(30, moving[-1])]


@pytest.mark.parametrize("level", [1.0, 10.37, 103.47])
def test_flat_nav_has_no_nav_signal_in_either_path(monkeypatch, level):
    nav = np.full(260, level)
    assert mm._numpy_nav_signal(nav) == (None, None, None)
    talib = pytest.importorskip("talib")
    assert talib.RSI(nav, timeperiod=14)[-1] == 0.0
    monkeypatch.setattr(mm, "_TALIB", talib)
    assert mm._talib_nav_signal(nav) == (None, None, None)


def test_flat_band_window_scores_on_rsi_alone_in_both_paths(monkeypatch):
    # On TA-Lib 0.6.5 this series gets a ~1e-6 wide band (position about 0.495)
    # without the flat-window check.
    nav = _flat_tail_nav(10.37, seed=0)
    rsi_np, bb_np, score_np = mm._numpy_nav_signal(nav)
    assert rsi_np is not None
    assert bb_np is None
    assert score_np == rsi_np
    talib = pytest.importorskip("talib")
    monkeypatch.setattr(mm, "_TALIB", talib)
    rsi_ta, bb_ta, score_ta = mm._talib_nav_signal(nav)
    assert bb_ta is None
    assert abs(rsi_ta - rsi_np) < 1e-6
    assert abs(score_ta - score_np) < 1e-6


def test_compute_nav_momentum_flat_nav_is_unscored(monkeypatch):
    monkeypatch.setattr(mm, "_TALIB", None)
    out = mm.compute_nav_momentum([1.0] * 260)
    for column in ("rsi_14", "bb_position", "nav_momentum_score", "blended_momentum_score"):
        assert out[column] is None


def test_compute_momentum_flat_nav_blends_flow_alone():
    start = dt.date(2024, 1, 1)
    points = [
        mm.NavAumPoint(start + dt.timedelta(days=i), 1.0, 1_000_000.0 + i * 5_000.0)
        for i in range(90)
    ]
    out = mm.compute_momentum(points, [])
    assert out["rsi_14"] is None
    assert out["nav_momentum_score"] is None
    assert out["flow_momentum_score"] is not None
    assert out["blended_momentum_score"] == out["flow_momentum_score"]


def _run_single_fund(monkeypatch, calc_date, points, nport_flows, nport_as_of):
    written: list[tuple] = []

    class _Conn:
        def commit(self):
            pass

    @contextlib.contextmanager
    def _connect(_dsn, **_kwargs):
        yield _Conn()

    @contextlib.contextmanager
    def _lock(_conn, _key):
        yield True

    def _upsert(_conn, _date, rows):
        written.extend(rows)
        return len(rows)

    monkeypatch.setattr(mm, "connect", _connect)
    monkeypatch.setattr(mm, "advisory_lock", _lock)
    monkeypatch.setattr(mm, "_resolve_calc_date", lambda _conn, _date: calc_date)
    monkeypatch.setattr(mm, "_target_instruments", lambda *_a: [("flat-fund", "S000001")])
    monkeypatch.setattr(mm, "_fetch_nport_flow_pct_assets", lambda *_a: (nport_flows, nport_as_of))
    monkeypatch.setattr(mm, "_fetch_nav_aum", lambda *_a: points)
    monkeypatch.setattr(mm, "_upsert", _upsert)
    monkeypatch.setattr(mm, "_refresh_read_models", lambda _dsn: None)
    return mm.run("dsn"), written


def test_run_writes_nulls_for_an_unscored_fund(monkeypatch):
    calc_date = dt.date(2024, 6, 28)
    flat = [mm.NavAumPoint(calc_date - dt.timedelta(days=259 - i), 1.0, None) for i in range(260)]

    stats, written = _run_single_fund(monkeypatch, calc_date, flat, [], None)

    assert stats["scored"] == 0
    assert stats["upserted"] == 1
    (row,) = written
    values = dict(zip(mm.MOMENTUM_COLUMNS, row[3:]))
    for column in ("rsi_14", "bb_position", "nav_momentum_score", "blended_momentum_score"):
        assert values[column] is None
    expected = mm.compute_momentum(flat, [], nport_as_of=None, calc_date=calc_date)
    assert values == {column: expected.get(column) for column in mm.MOMENTUM_COLUMNS}


def test_run_keeps_independent_metrics_for_an_unscored_fund(monkeypatch):
    calc_date = dt.date(2024, 6, 28)
    flat = [mm.NavAumPoint(calc_date - dt.timedelta(days=259 - i), 1.0, None) for i in range(260)]
    flows = [0.8, -0.3, 1.1, 0.4]
    as_of = dt.date(2024, 5, 31)

    stats, written = _run_single_fund(monkeypatch, calc_date, flat, flows, as_of)

    expected = mm.compute_momentum(flat, flows, nport_as_of=as_of, calc_date=calc_date)
    assert expected["blended_momentum_score"] is None
    assert expected["nport_flow_momentum_score"] is not None
    (row,) = written
    values = dict(zip(mm.MOMENTUM_COLUMNS, row[3:]))
    assert stats["scored"] == 0
    assert values["blended_momentum_score"] is None
    assert values["nport_flow_momentum_score"] == expected["nport_flow_momentum_score"]
    assert values["dtw_drift_score"] == expected["dtw_drift_score"]
    assert values == {column: expected.get(column) for column in mm.MOMENTUM_COLUMNS}


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
    out = mm.compute_nav_momentum([99.9, 100.1] * 14 + [99.9, 100.0])
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
