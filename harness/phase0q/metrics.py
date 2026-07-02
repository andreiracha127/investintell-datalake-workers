"""Metric extractor — EXACTLY the formulas frozen in
``artifacts/quant/open_macro_v03_phase0q_001/metric_definitions.json``.

Conventions (metric_definitions.json ``conventions``):
  * NAV basis = daily strategy NAV net of modeled transaction costs.
  * daily log returns r_t = ln(NAV_t / NAV_{t-1}).
  * trading_days_per_year = 252.

Metrics:
  * annualized_volatility = stdev(r_t, SAMPLE) * sqrt(252), per evaluation window.
  * max_drawdown          = max_t (peak_t - NAV_t)/peak_t, peak_t = max NAV_s (s<=t);
                            stress windows use the WINDOW-LOCAL peak.
  * turnover              = one_way_turnover_rebalance = 0.5*sum_i|w_post - w_pre|;
                            one_way_turnover_annualized = sum of per-rebalance one-way
                            turnover over the trailing 252 trading days. The harness
                            reports the annualized figure as the max over the window of
                            that trailing-252-day rolling sum (the worst 1y turnover),
                            plus the whole-window annualized average, so the gate can be
                            judged against the per-year bound. turnover_proxy is
                            PROHIBITED and never used.
  * stress_window_behavior = {window_return, window_MDD, worst_5d_return,
                            decision_coverage} per named window.
  * out_of_sample_stability = walk-forward folds (36m/12m/12m); per fold
                            {return_annualized, sigma_annual, MDD,
                             one_way_turnover_annualized}; stability = max abs deviation
                            of each fold metric from the cross-fold median.
"""

from __future__ import annotations

import datetime as _dt
import math
import statistics
from typing import Mapping, Sequence

TRADING_DAYS_PER_YEAR = 252


# --------------------------------------------------------------------------- #
# Primitive series metrics                                                     #
# --------------------------------------------------------------------------- #

def daily_log_returns(nav: Sequence[float]) -> list[float]:
    """r_t = ln(NAV_t / NAV_{t-1}); undefined for non-positive NAV segments."""
    out: list[float] = []
    for prev, cur in zip(nav, nav[1:]):
        if prev > 0 and cur > 0:
            out.append(math.log(cur / prev))
        else:
            out.append(0.0)
    return out


def annualized_volatility(nav: Sequence[float]) -> float:
    """sample stdev of daily log returns * sqrt(252). Zero when < 2 returns."""
    rets = daily_log_returns(nav)
    if len(rets) < 2:
        return 0.0
    return statistics.stdev(rets) * math.sqrt(TRADING_DAYS_PER_YEAR)


def max_drawdown(nav: Sequence[float]) -> float:
    """MDD = max_t (peak_t - NAV_t)/peak_t over the window (window-local peak)."""
    if not nav:
        return 0.0
    peak = nav[0]
    mdd = 0.0
    for value in nav:
        if value > peak:
            peak = value
        if peak > 0:
            dd = (peak - value) / peak
            if dd > mdd:
                mdd = dd
    return mdd


def window_return(nav: Sequence[float]) -> float:
    """Total window return NAV_last/NAV_first - 1 (0 for an empty/degenerate window)."""
    if len(nav) < 2 or nav[0] <= 0:
        return 0.0
    return nav[-1] / nav[0] - 1.0


def worst_k_day_return(nav: Sequence[float], k: int = 5) -> float:
    """Worst rolling k-trading-day simple return over the window (<=0 typically).

    Uses NAV endpoints k apart: min over t of NAV_{t}/NAV_{t-k} - 1. Falls back to
    the whole-window return when the window is shorter than k+1 points."""
    if len(nav) < 2:
        return 0.0
    if len(nav) <= k:
        return window_return(nav)
    worst = math.inf
    for i in range(k, len(nav)):
        if nav[i - k] > 0:
            ret = nav[i] / nav[i - k] - 1.0
            if ret < worst:
                worst = ret
    return 0.0 if worst is math.inf else worst


def worst_5d_return(nav: Sequence[float]) -> float:
    """Worst rolling 5-trading-day return (metric_definitions.json stress metric)."""
    return worst_k_day_return(nav, 5)


# --------------------------------------------------------------------------- #
# Turnover (annualized, one-way)                                              #
# --------------------------------------------------------------------------- #

def one_way_turnover_annualized(
    dates: Sequence[_dt.date],
    turnover_by_date: Mapping[_dt.date, float],
) -> dict[str, float]:
    """Annualized one-way turnover per metric_definitions.json.

    ``turnover_by_date`` maps each rebalance date to its one-way turnover
    (0.5*sum|dw|). The annualized figure = sum of one-way turnover over a trailing
    252-trading-day window. We report:
      * ``max_trailing_252`` = worst (max) trailing-252-day rolling sum (the peak
        1-year turnover the strategy incurs — the value the annual bound judges),
      * ``window_average_annualized`` = total one-way turnover * 252 / n_days (the
        mean annual rate across the whole window).
    """
    n = len(dates)
    if n == 0:
        return {"max_trailing_252": 0.0, "window_average_annualized": 0.0,
                "total_one_way": 0.0}
    per_day = [turnover_by_date.get(d, 0.0) for d in dates]
    total = sum(per_day)

    # trailing-252-trading-day rolling sum, worst value.
    max_trailing = 0.0
    window_sum = 0.0
    from collections import deque
    buf: deque[float] = deque()
    for value in per_day:
        buf.append(value)
        window_sum += value
        if len(buf) > TRADING_DAYS_PER_YEAR:
            window_sum -= buf.popleft()
        if window_sum > max_trailing:
            max_trailing = window_sum

    window_average = total * TRADING_DAYS_PER_YEAR / n
    return {"max_trailing_252": max_trailing,
            "window_average_annualized": window_average,
            "total_one_way": total}


def return_annualized(nav: Sequence[float], n_days: int) -> float:
    """Annualized total return: (1 + window_return) ** (252/n_days) - 1."""
    if n_days <= 0 or len(nav) < 2 or nav[0] <= 0:
        return 0.0
    total = nav[-1] / nav[0]
    if total <= 0:
        return -1.0
    return total ** (TRADING_DAYS_PER_YEAR / n_days) - 1.0


# --------------------------------------------------------------------------- #
# Stress window + OOS orchestration                                          #
# --------------------------------------------------------------------------- #

def decision_coverage(
    scheduled: Sequence[_dt.date],
    valid_decision_dates: set[_dt.date],
) -> float:
    """Fraction of scheduled decision dates with a valid quadrant decision."""
    if not scheduled:
        return 0.0
    return sum(1 for d in scheduled if d in valid_decision_dates) / len(scheduled)


def stress_window_metrics(
    dates: Sequence[_dt.date],
    nav: Sequence[float],
    turnover_by_date: Mapping[_dt.date, float],
    scheduled_decision_dates: Sequence[_dt.date],
    valid_decision_dates: set[_dt.date],
) -> dict[str, float]:
    """{window_return, window_MDD, worst_5d_return, decision_coverage,
    one_way_turnover_annualized(max_trailing_252)} for a named stress window."""
    return {
        "window_return": window_return(nav),
        "window_MDD": max_drawdown(nav),
        "worst_5d_return": worst_k_day_return(nav, 5),
        "annualized_volatility": annualized_volatility(nav),
        "decision_coverage": decision_coverage(
            scheduled_decision_dates, valid_decision_dates),
        "one_way_turnover_annualized": one_way_turnover_annualized(
            dates, turnover_by_date)["max_trailing_252"],
    }


def stability_from_folds(fold_metrics: Sequence[Mapping[str, float]]) -> dict[str, float]:
    """Cross-fold stability = max abs deviation of each metric from the cross-fold
    median (metric_definitions.json out_of_sample_stability)."""
    if not fold_metrics:
        return {}
    keys = fold_metrics[0].keys()
    out: dict[str, float] = {}
    for key in keys:
        values = [fold[key] for fold in fold_metrics]
        median = statistics.median(values)
        out[f"{key}_max_dev_from_median"] = max(abs(v - median) for v in values)
    return out
