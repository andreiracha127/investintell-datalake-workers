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
from typing import Any, Mapping, Sequence

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


# --------------------------------------------------------------------------- #
# Carry semantics (phase0q_003 DECISION 1)                                     #
# --------------------------------------------------------------------------- #

def consumable_position_coverage(
    global_chain: Sequence[Any],
    scheduled: Sequence[_dt.date],
) -> dict[str, Any]:
    """``consumable_position_coverage`` for a window (phase0q_003 DECISION 1).

    Fraction of ``scheduled`` dates where a *consumable* position exists for the
    sleeve: a FRESH valid decision on that date, OR carry-forward of the LAST VALID
    decision of the GLOBAL latched ``global_chain`` strictly on/before that date.

    Carry is only valid if a prior valid decision exists in the global chain; there
    is NO artificial per-window re-warmup. A scheduled date with no fresh valid
    decision and no prior valid latched position has an ABSENT consumable position
    (counts against coverage). Each carried date records its provenance (which
    decision date/quadrant is carried).

    ``global_chain`` must be the whole latched decision chain (not a window slice) so
    the carry seed is visible; passing only the window slice correctly reports the
    absence of a pre-window latched position.
    """
    valid = sorted(
        (d for d in global_chain if d.has_valid_quadrant()),
        key=lambda d: d.as_of,
    )
    fresh_dates = {d.as_of for d in valid}

    def last_valid_on_or_before(as_of: _dt.date):
        prior = None
        for d in valid:
            if d.as_of <= as_of:
                prior = d
            else:
                break
        return prior

    per_date: list[dict[str, Any]] = []
    fresh_count = 0
    carry_count = 0
    absent_count = 0
    for as_of in scheduled:
        if as_of in fresh_dates:
            fresh_count += 1
            per_date.append({"date": as_of.isoformat(), "source": "fresh",
                             "carried_from": None, "carried_quadrant": None})
            continue
        seed = last_valid_on_or_before(as_of)
        if seed is not None:
            carry_count += 1
            per_date.append({"date": as_of.isoformat(), "source": "carry",
                             "carried_from": seed.as_of.isoformat(),
                             "carried_quadrant": seed.quadrant})
        else:
            absent_count += 1
            per_date.append({"date": as_of.isoformat(), "source": "absent",
                             "carried_from": None, "carried_quadrant": None})

    n = len(scheduled)
    coverage = (fresh_count + carry_count) / n if n else 0.0
    return {
        "consumable_position_coverage": coverage,
        "fresh_count": fresh_count,
        "carry_count": carry_count,
        "absent_count": absent_count,
        "scheduled_count": n,
        "per_date": per_date,
    }


def carry_diagnostics(
    global_chain: Sequence[Any],
    scheduled: Sequence[_dt.date],
) -> dict[str, Any]:
    """Per-window DIAGNOSTICS (phase0q_003 DECISION 1): fresh_decision_rate,
    abstention_rate, deadband_count and hold_low_confidence_count. These are
    REPORTED, not gating. The deadband / hold_low_confidence counts come from the
    decision rows' ``transition_reason`` audit tags on the scheduled dates."""
    fresh_dates = {d.as_of for d in global_chain if d.has_valid_quadrant()}
    scheduled_set = set(scheduled)
    n = len(scheduled)
    fresh_count = sum(1 for d in scheduled if d in fresh_dates)
    abstain_count = n - fresh_count
    deadband_count = 0
    hold_low_confidence_count = 0
    for row in global_chain:
        if row.as_of not in scheduled_set:
            continue
        reason = getattr(row, "transition_reason", None) or ""
        tags = {bit.strip() for bit in reason.split(",") if bit.strip()}
        if "deadband" in tags:
            deadband_count += 1
        if "hold_low_confidence" in tags:
            hold_low_confidence_count += 1
    return {
        "scheduled_count": n,
        "fresh_count": fresh_count,
        "abstention_count": abstain_count,
        "fresh_decision_rate": (fresh_count / n) if n else 0.0,
        "abstention_rate": (abstain_count / n) if n else 0.0,
        "deadband_count": deadband_count,
        "hold_low_confidence_count": hold_low_confidence_count,
    }


# --------------------------------------------------------------------------- #
# Fold turnover excluding the initial acquisition (phase0q_003 DECISION 3)     #
# --------------------------------------------------------------------------- #

def fold_turnover_excluding_seed(
    dates: Sequence[_dt.date],
    turnover_by_date: Mapping[_dt.date, float],
    seed_date: _dt.date | None,
) -> dict[str, float]:
    """Fold economic turnover with the initial empty->position acquisition trade
    (the ``seed_date`` rebalance) EXCLUDED (phase0q_003 DECISION 3).

    Returns the seed one-way turnover, the total/annualized figures over the
    remaining (economic) trades, and the worst trailing-252 rolling sum computed on
    the seed-excluded per-day turnover series."""
    excl = {d: t for d, t in turnover_by_date.items() if d != seed_date}
    ann = one_way_turnover_annualized(dates, excl)
    seed_one_way = turnover_by_date.get(seed_date, 0.0) if seed_date is not None else 0.0
    return {
        "seed_one_way": seed_one_way,
        "seed_date": seed_date.isoformat() if seed_date is not None else None,
        "total_one_way_excl_seed": ann["total_one_way"],
        "max_trailing_252_excl_seed": ann["max_trailing_252"],
        "window_average_annualized_excl_seed": ann["window_average_annualized"],
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


# --------------------------------------------------------------------------- #
# Regime timeline metrics (Tranche W1 — ALWAYS computed, never gating here)    #
# --------------------------------------------------------------------------- #
# The phase0q_003 amendment reclassified fresh_decision_rate / abstention_rate as
# ``diagnostics_not_gating`` and made ``consumable_position_coverage`` the blocking
# stress metric (carry-forward counts as covered). That is exactly why a decision
# chain that abstained 43/66 months (2021-2026) and carried a 2023-02 contraction
# book for years reported 100% coverage and passed the risk-only envelope: the
# pathology was the optimum of the envelope, invisible to every reported number.
# These timeline metrics make the abstention/carry/quadrant-occupancy behaviour a
# FIRST-CLASS reported object over the LATCHED CONSUMABLE chain, so the regime the
# strategy is actually positioned in can never again be invisible. They are pure and
# deterministic; the proposed (not-yet-ratified) gate policy that judges them lives in
# ``artifacts/quant/open_macro_v03_phase0q_005/timeline_gate_policy.proposed.json``.


def _sorted_decisions(decisions: Sequence[Any]) -> list[Any]:
    """Decisions ordered by ``as_of`` (monthly cadence). Duplicate as_of months are a
    corrupt chain: the timeline reporter fails loud rather than silently double-count
    (parity with the carry-cap consumable path, which also rejects duplicate months)."""
    ordered = sorted(decisions, key=lambda d: d.as_of)
    seen: set[_dt.date] = set()
    for row in ordered:
        if row.as_of in seen:
            raise ValueError(
                f"regime_timeline_metrics: duplicate decision month {row.as_of} "
                "(a monthly latched chain has at most one row per month)")
        seen.add(row.as_of)
    return ordered


def _consumable_quadrants(ordered: Sequence[Any]) -> list[str | None]:
    """The consumable quadrant carried each month: the fresh quadrant when the month is
    a valid decision, else the LAST VALID quadrant on/before it (carry-forward), or
    ``None`` before the first valid decision seeds the chain (no consumable position)."""
    out: list[str | None] = []
    last_valid: str | None = None
    for row in ordered:
        if row.has_valid_quadrant():
            last_valid = row.quadrant
        out.append(last_valid)
    return out


def _trailing_fresh_valid_rate(ordered: Sequence[Any], months: int) -> float:
    """Fraction of the trailing ``months`` decision rows that are fresh-valid (the
    window the ratified-only gate ``min_fresh_valid_rate_36m`` judges). Uses
    min(months, n) as the denominator so a short chain is not diluted by absent tail."""
    if not ordered:
        return 0.0
    tail = ordered[-months:]
    return sum(1 for r in tail if r.has_valid_quadrant()) / len(tail)


def regime_timeline_metrics(decisions: Sequence[Any]) -> dict[str, Any]:
    """Timeline health of the monthly latched decision chain (Tranche W1).

    ``decisions`` is the monthly :class:`~harness.phase0q.decision.DecisionRow`-shaped
    chain (any object exposing ``as_of``, ``status``, ``quadrant`` and
    ``has_valid_quadrant()``). Returns a dict of REPORTED (never silently gating)
    diagnostics:

      * ``fresh_valid_rate``      global + trailing rolling 12/24/36-month rates,
      * ``max_abstention_streak_months``  longest run of consecutive non-valid months,
      * ``max_carry_age_months``  longest run of consecutive carried (gated) months that
        follows a valid seed — the carry age the carry_decay_v1 cap bounds,
      * ``max_same_quadrant_run_months``  longest run in the SAME consumable quadrant
        over the carry-filled chain (the 2023-02→2026 contraction anchor the audit flagged),
      * ``quadrant_mix``          fresh-valid month count per quadrant,
      * ``n_valid`` / ``n_low_confidence`` / ``n_months``.
    """
    ordered = _sorted_decisions(decisions)
    n = len(ordered)
    n_valid = sum(1 for r in ordered if r.has_valid_quadrant())
    n_low = sum(1 for r in ordered if getattr(r, "status", None) == "low_confidence")

    # longest abstention streak + longest carry age (a gated month that follows a seed).
    max_abstention = 0
    streak = 0
    max_carry_age = 0
    carry_age = 0
    seen_valid = False
    for row in ordered:
        if row.has_valid_quadrant():
            seen_valid = True
            streak = 0
            carry_age = 0
        else:
            streak += 1
            if streak > max_abstention:
                max_abstention = streak
            if seen_valid:  # carry age is only defined once a valid seed exists
                carry_age += 1
                if carry_age > max_carry_age:
                    max_carry_age = carry_age

    # longest same-quadrant run over the carry-filled consumable chain.
    consumable = _consumable_quadrants(ordered)
    max_same_run = 0
    run = 0
    prev: str | None = None
    for q in consumable:
        if q is None:
            run = 0
            prev = None
            continue
        run = run + 1 if q == prev else 1
        if run > max_same_run:
            max_same_run = run
        prev = q

    quadrant_mix = {q: 0 for q in ("recovery", "expansion", "slowdown", "contraction")}
    for row in ordered:
        if row.has_valid_quadrant():
            quadrant_mix[row.quadrant] = quadrant_mix.get(row.quadrant, 0) + 1

    return {
        "n_months": n,
        "n_valid": n_valid,
        "n_low_confidence": n_low,
        "fresh_valid_rate": {
            "global": (n_valid / n) if n else 0.0,
            "rolling_12m": _trailing_fresh_valid_rate(ordered, 12),
            "rolling_24m": _trailing_fresh_valid_rate(ordered, 24),
            "rolling_36m": _trailing_fresh_valid_rate(ordered, 36),
        },
        "max_abstention_streak_months": max_abstention,
        "max_carry_age_months": max_carry_age,
        "max_same_quadrant_run_months": max_same_run,
        "quadrant_mix": quadrant_mix,
        "first_as_of": ordered[0].as_of.isoformat() if ordered else None,
        "last_as_of": ordered[-1].as_of.isoformat() if ordered else None,
    }


def _calendar_year_return(series: Sequence[tuple[_dt.date, float]], year: int) -> float | None:
    """Total return of a (date, nav) series WITHIN one calendar year: nav_last/nav_first
    - 1 over the in-year points. ``None`` when fewer than two in-year points or a
    non-positive anchor (division guard)."""
    in_year = sorted((d, v) for d, v in series if d.year == year)
    if len(in_year) < 2:
        return None
    first, last = in_year[0][1], in_year[-1][1]
    if first is None or first <= 0 or last is None:
        return None
    return last / first - 1.0


def upside_capture_by_calendar_year(
    strategy_nav: Sequence[tuple[_dt.date, float]],
    spy_nav: Sequence[tuple[_dt.date, float]],
) -> dict[str, Any]:
    """Per calendar year: strategy return / SPY return, judged ONLY in years SPY rose
    (Tranche W1). The audit's core economic finding is that the model mapped the most
    bullish disinflationary years to the most defensive book; a benchmark-relative,
    per-calendar-year upside-capture number is how a ratified gate would catch that.

    ``strategy_nav`` / ``spy_nav`` are (date, nav) sequences. For each year present in
    BOTH, reports ``strategy_return``, ``spy_return``, ``spy_up`` (spy_return > 0) and
    ``upside_capture`` = strategy_return / spy_return when ``spy_up`` (else ``None`` —
    a down year is a downside-capture question, out of scope here). All divisions are
    guarded (a non-positive SPY return yields ``upside_capture: None``)."""
    strat_years = {d.year for d, _ in strategy_nav}
    spy_years = {d.year for d, _ in spy_nav}
    per_year: dict[str, Any] = {}
    for year in sorted(strat_years & spy_years):
        strat_ret = _calendar_year_return(strategy_nav, year)
        spy_ret = _calendar_year_return(spy_nav, year)
        if strat_ret is None or spy_ret is None:
            continue
        spy_up = spy_ret > 0.0
        capture = (strat_ret / spy_ret) if spy_up else None
        per_year[str(year)] = {
            "strategy_return": strat_ret,
            "spy_return": spy_ret,
            "spy_up": spy_up,
            "upside_capture": capture,
        }
    return per_year
