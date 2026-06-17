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


def test_relative_metrics_for_without_macro_omits_fi() -> None:
    # No macro_changes → FI keys absent (back-compat with existing callers).
    rows = [(_dt.date(2022, 1, 3) + _dt.timedelta(days=i), 100.0 + i) for i in range(30)]
    out = rm.relative_metrics_for(rows, None, {}, 0.04)
    assert "empirical_duration" not in out
    assert "credit_beta" not in out
