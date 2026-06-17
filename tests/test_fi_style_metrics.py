"""Pure-math unit tests for the FI style regressions in the risk_metrics worker
(empirical_duration / credit_beta). No DB — synthetic factor changes only.

Ported from the legacy ``fixed_income_analytics_service``: empirical duration is
the −beta of fund daily returns on Δ DGS10 (10y Treasury yield); credit beta is
the −beta on Δ BAA10Y (Baa−10y credit spread). Both gated by R² ≥ FI_MIN_R2 and
≥ FI_MIN_OBS overlapping observations.
"""

from __future__ import annotations

import datetime as _dt

import numpy as np

from src.workers import risk_metrics as rm


def _dates(n: int) -> list[_dt.date]:
    d0 = _dt.date(2022, 1, 3)
    return [d0 + _dt.timedelta(days=i) for i in range(n)]


def test_ols_beta_r2_recovers_known_line() -> None:
    x = np.linspace(-1.0, 1.0, 100)
    y = 0.5 + 3.0 * x
    beta, r2 = rm._ols_beta_r2(y, x)
    assert abs(beta - 3.0) < 1e-9
    assert r2 > 0.999


def test_fi_factor_beta_recovers_duration() -> None:
    rng = np.random.default_rng(0)
    dates = _dates(300)
    yld = {d: float(rng.normal(0.0, 0.05)) for d in dates}  # daily yield change
    duration = 5.0
    fund = {d: -duration * yld[d] + float(rng.normal(0.0, 5e-4)) for d in dates}
    beta = rm._fi_factor_beta(fund, yld)  # = empirical_duration
    assert beta is not None
    assert abs(beta - duration) < 0.2


def test_fi_factor_beta_none_below_min_obs() -> None:
    dates = _dates(50)  # < FI_MIN_OBS
    yld = {d: 0.01 for d in dates}
    fund = {d: -0.05 for d in dates}
    assert rm._fi_factor_beta(fund, yld) is None


def test_fi_factor_beta_none_when_weak_fit() -> None:
    rng = np.random.default_rng(1)
    dates = _dates(300)
    yld = {d: float(rng.normal(0.0, 0.05)) for d in dates}
    fund = {d: float(rng.normal(0.0, 0.01)) for d in dates}  # unrelated → r²≈0
    assert rm._fi_factor_beta(fund, yld) is None


def test_fi_style_metrics_recovers_duration_and_credit() -> None:
    rng = np.random.default_rng(2)
    dates = _dates(300)
    yld = {d: float(rng.normal(0.0, 0.05)) for d in dates}
    spr = {d: float(rng.normal(0.0, 0.03)) for d in dates}
    dur, cb = 6.0, 2.5
    fund_dated = [
        (d, -dur * yld[d] - cb * spr[d] + float(rng.normal(0.0, 5e-4))) for d in dates
    ]
    macro = {rm.FI_YIELD_SERIES: yld, rm.FI_CREDIT_SERIES: spr}
    out = rm.fi_style_metrics(fund_dated, macro)
    assert out["empirical_duration"] is not None
    assert out["credit_beta"] is not None
    assert abs(out["empirical_duration"] - dur) < 0.6
    assert abs(out["credit_beta"] - cb) < 0.6


def test_fi_style_metrics_empty_macro_is_all_none() -> None:
    out = rm.fi_style_metrics([(_dt.date(2022, 1, 3), 0.01)], {})
    assert out == {"empirical_duration": None, "credit_beta": None}


# ── crisis_alpha ─────────────────────────────────────────────────────────────


def _crash_then_recover(n_pre: int, crash_len: int, crash_daily: float) -> list[float]:
    """Flat, then a sustained drawdown (>10% after ~crash_len days)."""
    return [0.0] * n_pre + [crash_daily] * crash_len


def test_crisis_alpha_positive_when_fund_outperforms_in_drawdown() -> None:
    dates = _dates(200)
    # Benchmark: flat 100d, then ~-1%/day for 60d → deep (>10%) drawdown.
    bench_r = _crash_then_recover(100, 100, -0.012)
    # Fund: flat in calm, only -0.2%/day during the crash → outperforms.
    fund_r = [0.0] * 100 + [-0.002] * 100
    bench = list(zip(dates, bench_r))
    fund = list(zip(dates, fund_r))
    score = rm.crisis_alpha(fund, bench)
    assert score is not None
    assert score > 0  # fund lost far less than equities during the crisis


def test_crisis_alpha_negative_when_fund_underperforms() -> None:
    dates = _dates(200)
    bench_r = [0.0] * 100 + [-0.012] * 100
    fund_r = [0.0] * 100 + [-0.02] * 100  # fund crashes harder than equities
    score = rm.crisis_alpha(list(zip(dates, fund_r)), list(zip(dates, bench_r)))
    assert score is not None
    assert score < 0


def test_crisis_alpha_none_without_enough_crisis_days() -> None:
    dates = _dates(200)
    bench_r = [0.0005] * 200  # gently rising, never in >10% drawdown
    fund_r = [0.0003] * 200
    assert rm.crisis_alpha(list(zip(dates, fund_r)), list(zip(dates, bench_r))) is None


def test_crisis_alpha_clamps_blowup_outlier() -> None:
    # A bad-NAV spike during the crisis would compound to an absurd cumulative;
    # the result must be clamped to the sane bound, not 1e6%.
    dates = _dates(200)
    bench_r = [0.0] * 100 + [-0.012] * 100
    fund_r = [0.0] * 100 + [5.0] + [-0.002] * 99  # one +500% bad-NAV day in crisis
    score = rm.crisis_alpha(list(zip(dates, fund_r)), list(zip(dates, bench_r)))
    assert score is not None
    assert -10.0 <= score <= 10.0


def test_crisis_alpha_none_below_min_overlap() -> None:
    dates = _dates(40)  # < 60 common days
    bench_r = [-0.02] * 40
    fund_r = [-0.01] * 40
    assert rm.crisis_alpha(list(zip(dates, fund_r)), list(zip(dates, bench_r))) is None


def test_relative_metrics_for_adds_crisis_alpha_when_bench_given() -> None:
    # 200 NAV rows for the fund; long equity bench with a deep drawdown.
    dates = _dates(200)
    nav_rows = [(dates[i], 100.0) for i in range(200)]  # flat fund (returns ~0)
    bench = list(zip(dates, [0.0] * 100 + [-0.012] * 100))
    out = rm.relative_metrics_for(nav_rows, None, {}, 0.04, None, bench)
    assert "crisis_alpha_score" in out


def test_relative_metrics_for_without_macro_omits_fi() -> None:
    # No macro_changes → FI keys absent (back-compat with existing callers).
    rows = [(_dt.date(2022, 1, 3) + _dt.timedelta(days=i), 100.0 + i) for i in range(30)]
    out = rm.relative_metrics_for(rows, None, {}, 0.04)
    assert "empirical_duration" not in out
    assert "credit_beta" not in out
