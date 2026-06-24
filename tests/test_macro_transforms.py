# tests/test_macro_transforms.py
from __future__ import annotations

import datetime as dt
import math

from src.macro_transforms import economic_transform, robust_z, standardize


def _monthly(values: list[float], start=dt.date(2022, 1, 1)) -> dict[dt.date, float]:
    """Build a {month-start: value} series of len(values) consecutive months."""
    out: dict[dt.date, float] = {}
    y, m = start.year, start.month
    for v in values:
        out[dt.date(y, m, 1)] = v
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def test_log_3m3m_ann_matches_formula() -> None:
    # 3m3mAnn_t = 4*[ log(mean(x_{t-2..t})) - log(mean(x_{t-5..t-3})) ].
    series = _monthly([100, 101, 102, 103, 104, 105])  # 6 months -> last is computable
    out = economic_transform("log_3m3m_ann_v1", series)
    last = max(out)
    recent = (103 + 104 + 105) / 3
    prior = (100 + 101 + 102) / 3
    assert abs(out[last] - 4.0 * (math.log(recent) - math.log(prior))) < 1e-12
    # the first 5 months have no 6-month base -> dropped
    assert dt.date(2022, 1, 1) not in out


def test_ann3m_minus_yoy_inflation_impulse() -> None:
    # 4*(log x_t - log x_{t-3}) - (log x_t - log x_{t-12}); needs >=13 months.
    series = _monthly([100 + i for i in range(13)])
    out = economic_transform("ann3m_minus_yoy_v1", series)
    t = max(out)
    x_t = series[t]
    x_t3 = series[dt.date(2022, 10, 1)]   # 3 months before 2023-01
    x_t12 = series[dt.date(2022, 1, 1)]
    expect = 4.0 * (math.log(x_t) - math.log(x_t3)) - (math.log(x_t) - math.log(x_t12))
    assert abs(out[t] - expect) < 1e-12


def test_mean3_gap_neutral_uses_neutral_level() -> None:
    series = _monthly([48, 50, 52])  # mean3 = 50
    out = economic_transform("mean3_gap_neutral_v1", series, neutral_level=50.0)
    assert abs(out[max(out)] - 0.0) < 1e-12


def test_delta_3m_level_is_level_change() -> None:
    series = _monthly([4.0, 4.0, 4.0, 4.4])  # x_t - x_{t-3} = 0.4
    out = economic_transform("delta_3m_level_v1", series)
    assert abs(out[max(out)] - 0.4) < 1e-12


def test_unknown_transform_raises() -> None:
    try:
        economic_transform("nope_v1", _monthly([1, 2, 3]))
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_robust_z_clips_at_four_sigma() -> None:
    hist = [0.0] * 11 + [1000.0]          # median 0, MAD 0 -> degenerate
    assert robust_z(hist) is None         # MAD == 0 -> undefined (caller floors elsewhere)
    spread = [float(i) for i in range(-5, 6)]  # symmetric, MAD > 0
    # an extreme current value clips to +4
    assert standardize("robust_z_10y_distinct_vintages_v1", spread, 1e6) == 4.0


def test_standardize_centers_on_median() -> None:
    hist = [1.0, 2.0, 3.0, 4.0, 5.0]
    z = standardize("robust_z_10y_distinct_vintages_v1", hist, 3.0)  # current == median
    assert abs(z - 0.0) < 1e-12
