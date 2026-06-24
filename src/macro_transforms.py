# src/macro_transforms.py
"""Two-stage macro transform (freeze scope §4, owner decision A): an
ECONOMIC transform per family extracts the impulse, then a single universal
STANDARDIZER ('robust_z_10y_distinct_vintages_v1') makes axes comparable.

There is NO automatic 3m3m->yoy fallback: a series without enough data for its
declared economic_transform_id stays UNAVAILABLE (Task 3 treats a None as a
missing input -> coverage penalty / abstention). Both stages belong to the
source_spec_version; changing either requires a new model_version.
"""
from __future__ import annotations

import datetime as _dt
import math
import statistics

_MAD_SCALE = 1.4826
_Z_CLIP = 4.0


def _sorted_periods(series: dict[_dt.date, float]) -> list[_dt.date]:
    return sorted(series)


def _shift_months(periods: list[_dt.date], idx: int, back: int) -> _dt.date | None:
    """The period ``back`` calendar months before periods[idx] IF it exists in the
    index (no interpolation; macro series are regular month-starts)."""
    target = periods[idx]
    y, m = target.year, target.month
    m -= back
    while m <= 0:
        m += 12
        y -= 1
    cand = _dt.date(y, m, 1)
    return cand if cand in set(periods) else None


def economic_transform(
    transform_id: str,
    series: dict[_dt.date, float],
    *,
    neutral_level: float | None = None,
) -> dict[_dt.date, float]:
    """{period: value} -> {period: economic impulse}. Periods without the required
    history are dropped (not interpolated)."""
    periods = _sorted_periods(series)
    pos = {p: i for i, p in enumerate(periods)}
    out: dict[_dt.date, float] = {}

    if transform_id == "log_3m3m_ann_v1":
        # 4 * [ log(mean(x_{t-2..t})) - log(mean(x_{t-5..t-3})) ]
        for t, i in pos.items():
            if i < 5:
                continue
            recent = [series[periods[j]] for j in (i - 2, i - 1, i)]
            prior = [series[periods[j]] for j in (i - 5, i - 4, i - 3)]
            mr, mp = statistics.fmean(recent), statistics.fmean(prior)
            if mr > 0 and mp > 0:
                out[t] = 4.0 * (math.log(mr) - math.log(mp))
        return out

    if transform_id == "log_qoq_saar_v1":
        # quarterly real GDP: 4 * (log x_t - log x_{t-1quarter==3 months})
        for t, i in pos.items():
            prev = _shift_months(periods, i, 3)
            if prev is None:
                continue
            xt, xp = series[t], series[prev]
            if xt > 0 and xp > 0:
                out[t] = 4.0 * (math.log(xt) - math.log(xp))
        return out

    if transform_id == "mean3_gap_neutral_v1":
        if neutral_level is None:
            raise ValueError("mean3_gap_neutral_v1 requires neutral_level")
        for t, i in pos.items():
            if i < 2:
                continue
            mean3 = statistics.fmean(series[periods[j]] for j in (i - 2, i - 1, i))
            out[t] = mean3 - neutral_level
        return out

    if transform_id == "delta_3m_level_v1":
        # x_t - x_{t-3}
        for t, i in pos.items():
            prev = _shift_months(periods, i, 3)
            if prev is not None:
                out[t] = series[t] - series[prev]
        return out

    if transform_id == "delta_3m_yoy_v1":
        # change in YoY: yoy_t - yoy_{t-3}, with yoy = x_t/x_{t-12} - 1
        yoy: dict[_dt.date, float] = {}
        for t, i in pos.items():
            prev12 = _shift_months(periods, i, 12)
            if prev12 is not None and series[prev12] != 0:
                yoy[t] = series[t] / series[prev12] - 1.0
        ypos = {p: i for i, p in enumerate(sorted(yoy))}
        yperiods = sorted(yoy)
        for t, i in ypos.items():
            if i >= 3:
                out[t] = yoy[t] - yoy[yperiods[i - 3]]
        return out

    if transform_id == "ann3m_minus_yoy_v1":
        # InflationImpulse: 4*(log x_t - log x_{t-3}) - (log x_t - log x_{t-12})
        for t, i in pos.items():
            p3 = _shift_months(periods, i, 3)
            p12 = _shift_months(periods, i, 12)
            if p3 is None or p12 is None:
                continue
            xt, x3, x12 = series[t], series[p3], series[p12]
            if xt > 0 and x3 > 0 and x12 > 0:
                out[t] = 4.0 * (math.log(xt) - math.log(x3)) - (
                    math.log(xt) - math.log(x12))
        return out

    raise ValueError(f"unknown economic_transform_id: {transform_id!r}")


def robust_z(values: list[float]) -> float | None:
    """Helper returning the robust scale only — None when MAD == 0 (degenerate)."""
    if len(values) < 2:
        return None
    median = statistics.median(values)
    mad = statistics.median([abs(v - median) for v in values])
    return _MAD_SCALE * mad if mad > 0 else None


def standardize(
    standardizer_id: str, history_distinct: list[float], current: float
) -> float | None:
    """z = clip( (current - median) / (1.4826*MAD), -4, +4 ) over DISTINCT vintages.

    Returns None when the robust scale is undefined (MAD == 0 or < 2 values); the
    caller treats None as a missing standardized input.
    """
    if standardizer_id != "robust_z_10y_distinct_vintages_v1":
        raise ValueError(f"unknown standardizer_id: {standardizer_id!r}")
    distinct = sorted(set(history_distinct))
    scale = robust_z(distinct)
    if scale is None:
        return None
    median = statistics.median(distinct)
    z = (current - median) / scale
    return max(-_Z_CLIP, min(_Z_CLIP, z))
