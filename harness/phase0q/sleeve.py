"""Reference-sleeve simulator (harness measurement instrument only).

Builds a daily, cost-net NAV series for the reference sleeve defined in
``reference_sleeve_proposal.json`` driven by the monthly quadrant decision series
from :mod:`harness.phase0q.decision`. This is NOT a productive allocation; it exists
solely to MEASURE turnover / drawdown / volatility / stress / OOS of the decision
path (proposal ``purpose``; allocator_publish=false).

Mechanics (reference_sleeve_proposal.json + scenario_grid.json):

* Instruments: SPY, TLT, TIP, GLD, DBC, SHY (adjusted_close daily prices).
* Per-quadrant baseline target weights (``per_quadrant_baseline_weights``), mapped
  from the decision engine's quadrant labels:
    recovery    -> Q1_growth_up_inflation_down
    expansion   -> Q2_growth_up_inflation_up
    slowdown    -> Q3_growth_down_inflation_up
    contraction -> Q4_growth_down_inflation_down
  (freeze quadrant naming: growth-up/infl-down = recovery, etc.)
* Scenario deltas: ``risk_tilt`` shifts weight between SPY (risk) and SHY
  (defensive) by the tilt fraction (add tilt to SPY, subtract from SHY, clamped
  >= 0). ``risk_cap_delta_pp`` / ``defensive_floor_delta_pp`` adjust the
  constraint_baselines (risk_cap 0.65, defensive_floor 0.20). growth/inflation
  weights are decision-path axis-blend probes and do not re-map the sleeve targets
  (see decision.py rationale); they are recorded on the cell for provenance.
* Constraints enforced on every target: risk assets (SPY+DBC) <= risk_cap; defensive
  assets (TLT+SHY+TIP) >= defensive_floor. Enforcement scales the offending group
  toward its bound and renormalizes to sum 1 (deterministic, no RNG).
* Rebalance: monthly month-end decision date, to the CURRENT quadrant's constrained
  target. A trade fires on a quadrant change OR any per-instrument weight drift
  > 0.05 from target (``rebalance_policy``). When the latest decision has no valid
  quadrant, the target is carried (weights drift with prices; no forced trade).
* Costs: one-way cost applied to 0.5*sum|dw| turned over (per cost grid bps).
* Pre-inception renormalization: before an instrument's first price date (DBC
  2006-02-06 binds), it is dropped from the target row and remaining weights
  renormalize proportionally (reduced_sleeve).
* Missing-day rule: a scheduled trade date with no price defers to the next session
  with data (no interpolation).
* Data quality: emit a ``data_quality`` section listing triggered flags
  (>3-day gaps, zero/negative closes, >5-session zero-volume runs).

The simulator is deterministic given prices + decisions + parameters + cost bps.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

SLEEVE_TICKERS: tuple[str, ...] = ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")

# Decision-engine quadrant label -> proposal quadrant key.
QUADRANT_TO_KEY = {
    "recovery": "Q1_growth_up_inflation_down",
    "expansion": "Q2_growth_up_inflation_up",
    "slowdown": "Q3_growth_down_inflation_up",
    "contraction": "Q4_growth_down_inflation_down",
}

PER_QUADRANT_BASELINE_WEIGHTS: dict[str, dict[str, float]] = {
    "Q1_growth_up_inflation_down": {"SPY": 0.60, "TLT": 0.20, "TIP": 0.05, "GLD": 0.05, "DBC": 0.00, "SHY": 0.10},
    "Q2_growth_up_inflation_up": {"SPY": 0.45, "TLT": 0.05, "TIP": 0.15, "GLD": 0.10, "DBC": 0.15, "SHY": 0.10},
    "Q3_growth_down_inflation_up": {"SPY": 0.15, "TLT": 0.05, "TIP": 0.25, "GLD": 0.20, "DBC": 0.15, "SHY": 0.20},
    "Q4_growth_down_inflation_down": {"SPY": 0.15, "TLT": 0.40, "TIP": 0.10, "GLD": 0.10, "DBC": 0.00, "SHY": 0.25},
}

RISK_ASSETS = ("SPY", "DBC")
DEFENSIVE_ASSETS = ("TLT", "SHY", "TIP")
RISK_CAP_BASELINE = 0.65
DEFENSIVE_FLOOR_BASELINE = 0.20
DRIFT_BAND = 0.05

DBC_INCEPTION = _dt.date(2006, 2, 6)
INSTRUMENT_INCEPTION = {"DBC": DBC_INCEPTION}


@dataclass(frozen=True)
class SleeveParams:
    """Scenario-grid parameters that reach the sleeve."""

    candidate_id: str
    growth_weight: float = 0.5
    inflation_weight: float = 0.5
    risk_tilt: float = 0.0
    defensive_floor_delta_pp: float = 0.0
    risk_cap_delta_pp: float = 0.0


@dataclass
class SleeveResult:
    """Daily NAV path plus per-rebalance turnover and data-quality flags."""

    dates: list[_dt.date]
    nav: list[float]
    rebalance_dates: list[_dt.date]
    one_way_turnover_by_date: dict[_dt.date, float]  # 0.5*sum|dw| at each trade
    reduced_sleeve_dates: list[_dt.date] = field(default_factory=list)
    data_quality_flags: list[dict[str, Any]] = field(default_factory=list)
    # The first rebalance is the initial empty->position acquisition (the "seed");
    # phase0q_003 DECISION 3 excludes it from fold ECONOMIC turnover.
    seed_rebalance_date: _dt.date | None = None

    def nav_by_date(self) -> dict[_dt.date, float]:
        return dict(zip(self.dates, self.nav))


# --------------------------------------------------------------------------- #
# Price frame                                                                 #
# --------------------------------------------------------------------------- #

class PriceFrame:
    """Adjusted-close price panel over the sleeve tickers, indexed by trading date."""

    def __init__(self, eod_rows: Sequence[Mapping[str, Any]]):
        by_ticker: dict[str, dict[_dt.date, float]] = {t: {} for t in SLEEVE_TICKERS}
        volume_by_ticker: dict[str, dict[_dt.date, float]] = {t: {} for t in SLEEVE_TICKERS}
        all_dates: set[_dt.date] = set()
        for row in eod_rows:
            ticker = row["ticker"]
            if ticker not in by_ticker:
                continue
            date = _dt.date.fromisoformat(row["date"][:10])
            ac = row.get("adjusted_close")
            by_ticker[ticker][date] = float(ac) if ac is not None else float("nan")
            vol = row.get("volume")
            volume_by_ticker[ticker][date] = float(vol) if vol is not None else float("nan")
            all_dates.add(date)
        self._by_ticker = by_ticker
        self._volume_by_ticker = volume_by_ticker
        self.dates = sorted(all_dates)

    def price(self, ticker: str, date: _dt.date) -> float | None:
        return self._by_ticker[ticker].get(date)

    def dates_in(self, start: _dt.date, end: _dt.date) -> list[_dt.date]:
        return [d for d in self.dates if start <= d <= end]

    def available_tickers(self, date: _dt.date) -> list[str]:
        return [t for t in SLEEVE_TICKERS if date in self._by_ticker[t]]

    def data_quality_flags(self, start: _dt.date, end: _dt.date) -> list[dict[str, Any]]:
        """Per-instrument checks over its coverage window inside [start, end]."""
        flags: list[dict[str, Any]] = []
        window = [d for d in self.dates if start <= d <= end]
        for ticker in SLEEVE_TICKERS:
            series = self._by_ticker[ticker]
            covered = [d for d in window if d in series]
            if not covered:
                continue
            for d in covered:
                v = series[d]
                if v is None or v != v or v <= 0:  # NaN or non-positive
                    flags.append({"ticker": ticker, "flag": "non_positive_adjusted_close",
                                  "date": d.isoformat()})
            # gap of > 3 consecutive trading days (window trading days between two
            # consecutive covered dates for this ticker).
            idx = {d: i for i, d in enumerate(window)}
            cov_idx = sorted(idx[d] for d in covered)
            for a, b in zip(cov_idx, cov_idx[1:]):
                if b - a - 1 > 3:
                    flags.append({"ticker": ticker, "flag": "gap_gt_3_sessions",
                                  "from": window[a].isoformat(), "to": window[b].isoformat(),
                                  "sessions": b - a - 1})
            # zero-volume runs longer than 5 sessions (survivorship_and_data_quality):
            # scan the ticker's covered sessions in order; a maximal run of >5
            # consecutive zero-volume sessions is flagged (do not interpolate).
            vseries = self._volume_by_ticker[ticker]
            run: list[_dt.date] = []
            for d in covered:
                v = vseries.get(d)
                is_zero = v is not None and v == v and v == 0.0
                if is_zero:
                    run.append(d)
                else:
                    if len(run) > 5:
                        flags.append({"ticker": ticker, "flag": "zero_volume_run_gt_5_sessions",
                                      "from": run[0].isoformat(), "to": run[-1].isoformat(),
                                      "sessions": len(run)})
                    run = []
            if len(run) > 5:
                flags.append({"ticker": ticker, "flag": "zero_volume_run_gt_5_sessions",
                              "from": run[0].isoformat(), "to": run[-1].isoformat(),
                              "sessions": len(run)})
        return sorted(flags, key=lambda f: (f["ticker"], f["flag"], f.get("date", ""),
                                            f.get("from", "")))


# --------------------------------------------------------------------------- #
# Target-weight construction                                                   #
# --------------------------------------------------------------------------- #

def compressed_quadrant_weights(fraction: float = 0.5) -> dict[str, dict[str, float]]:
    """``sleeve_compressed_50`` (phase0q_003 DECISION 2, alternative measurement).

    Each quadrant's weight vector is moved ``fraction`` of the way toward the MEAN of
    the four quadrant vectors, then renormalized to sum 1. This is a controlled
    turnover/risk/return trade-off probe (NOT a replacement sleeve): it does not
    blindly reduce weights, it compresses the four quadrants toward their common
    centroid so quadrant flips move fewer units.
    """
    keys = list(PER_QUADRANT_BASELINE_WEIGHTS)
    mean = {t: sum(PER_QUADRANT_BASELINE_WEIGHTS[k].get(t, 0.0) for k in keys) / len(keys)
            for t in SLEEVE_TICKERS}
    out: dict[str, dict[str, float]] = {}
    for k in keys:
        base = PER_QUADRANT_BASELINE_WEIGHTS[k]
        moved = {t: base.get(t, 0.0) + fraction * (mean[t] - base.get(t, 0.0))
                 for t in SLEEVE_TICKERS}
        out[k] = _renormalize({t: w for t, w in moved.items() if w > 0.0})
    return out


def _compressed_book_50() -> dict[str, dict[str, float]]:
    """Cached ``sleeve_compressed_50`` baseline book (computed once, deterministic)."""
    global _COMPRESSED_BOOK_50
    if _COMPRESSED_BOOK_50 is None:
        _COMPRESSED_BOOK_50 = compressed_quadrant_weights(0.5)
    return _COMPRESSED_BOOK_50


_COMPRESSED_BOOK_50: dict[str, dict[str, float]] | None = None


def target_weights(
    quadrant: str, params: SleeveParams, available: Sequence[str],
    *, compressed: bool = False,
) -> dict[str, float]:
    """Constrained target weights for a quadrant given scenario params and the
    price-available instrument subset (pre-inception renormalization).

    Order: baseline -> risk_tilt (SPY vs SHY) -> drop unavailable + renormalize ->
    enforce risk_cap / defensive_floor -> final renormalize to sum 1.

    ``compressed`` selects the ``sleeve_compressed_50`` baseline book (DECISION 2
    alternative measurement) instead of the standard per-quadrant baseline.
    """
    key = QUADRANT_TO_KEY[quadrant]
    book = _compressed_book_50() if compressed else PER_QUADRANT_BASELINE_WEIGHTS
    weights = dict(book[key])

    # risk_tilt: shift between SPY (risk) and SHY (defensive), clamped non-negative.
    tilt = params.risk_tilt
    weights["SPY"] = weights.get("SPY", 0.0) + tilt
    weights["SHY"] = weights.get("SHY", 0.0) - tilt
    weights = {t: max(0.0, w) for t, w in weights.items()}

    # pre-inception renormalization: keep only available instruments.
    avail = set(available)
    weights = {t: w for t, w in weights.items() if t in avail}
    weights = _renormalize(weights)

    # enforce constraints on the renormalized row.
    risk_cap = RISK_CAP_BASELINE + params.risk_cap_delta_pp / 100.0
    defensive_floor = DEFENSIVE_FLOOR_BASELINE + params.defensive_floor_delta_pp / 100.0
    weights = _enforce_risk_cap(weights, risk_cap)
    weights = _enforce_defensive_floor(weights, defensive_floor)
    return _renormalize(weights)


def _renormalize(weights: Mapping[str, float]) -> dict[str, float]:
    total = sum(weights.values())
    if total <= 0.0:
        # degenerate: equal-weight the available set.
        n = len(weights)
        return {t: 1.0 / n for t in weights} if n else {}
    return {t: w / total for t, w in weights.items()}


def _enforce_risk_cap(weights: dict[str, float], cap: float) -> dict[str, float]:
    """Scale the risk group down to ``cap`` (if it exceeds it), pushing the freed
    mass into the non-risk group proportionally."""
    risk = {t: weights[t] for t in RISK_ASSETS if t in weights}
    risk_sum = sum(risk.values())
    if risk_sum <= cap or risk_sum <= 0.0:
        return weights
    out = dict(weights)
    scale = cap / risk_sum
    for t in risk:
        out[t] = weights[t] * scale
    freed = risk_sum - cap
    non_risk = {t: w for t, w in weights.items() if t not in RISK_ASSETS}
    nr_sum = sum(non_risk.values())
    if nr_sum > 0.0:
        for t in non_risk:
            out[t] = weights[t] + freed * (weights[t] / nr_sum)
    return out


def _enforce_defensive_floor(weights: dict[str, float], floor: float) -> dict[str, float]:
    """Scale the defensive group up to ``floor`` (if below), pulling mass from the
    non-defensive group proportionally."""
    defensive = {t: weights[t] for t in DEFENSIVE_ASSETS if t in weights}
    def_sum = sum(defensive.values())
    if def_sum >= floor:
        return weights
    out = dict(weights)
    needed = floor - def_sum
    non_def = {t: w for t, w in weights.items() if t not in DEFENSIVE_ASSETS}
    nd_sum = sum(non_def.values())
    if nd_sum <= 0.0:
        return weights
    take = min(needed, nd_sum)
    for t in non_def:
        out[t] = weights[t] - take * (weights[t] / nd_sum)
    # distribute the taken mass into defensive proportionally (or equally if all 0).
    if def_sum > 0.0:
        for t in defensive:
            out[t] = weights[t] + take * (weights[t] / def_sum)
    else:
        for t in defensive:
            out[t] = weights[t] + take / len(defensive)
    return out


# --------------------------------------------------------------------------- #
# Simulation                                                                   #
# --------------------------------------------------------------------------- #

def simulate(
    prices: PriceFrame,
    decisions: Sequence[Any],       # decision.DecisionRow
    params: SleeveParams,
    *,
    start: _dt.date,
    end: _dt.date,
    cost_bps: float,
    compressed: bool = False,
) -> SleeveResult:
    """Run the daily cost-net NAV simulation over ``[start, end]``.

    ``decisions`` is the monthly latched decision series; each month-end that has a
    valid quadrant sets the active target. A trade fires on a quadrant change or a
    >5pp drift. One-way ``cost_bps`` is charged on 0.5*sum|dw| at each trade.

    ``compressed`` selects the ``sleeve_compressed_50`` baseline book (phase0q_003
    DECISION 2 alternative measurement).
    """
    trading_dates = prices.dates_in(start, end)
    if not trading_dates:
        return SleeveResult([], [], [], {})

    # Map each decision month-end to the first trading date >= it (missing-day rule).
    decision_by_trade_date = _schedule_decisions(prices, decisions, trading_dates)

    cost_rate = cost_bps / 10000.0
    nav = 1.0
    weights: dict[str, float] = {}   # current holdings weights (drift with prices)
    active_target: dict[str, float] = {}
    active_quadrant: str | None = None

    dates_out: list[_dt.date] = []
    nav_out: list[float] = []
    rebalance_dates: list[_dt.date] = []
    turnover_by_date: dict[_dt.date, float] = {}
    reduced_dates: list[_dt.date] = []

    prev_date: _dt.date | None = None
    for date in trading_dates:
        # 1) drift current weights forward by one day's price return.
        if prev_date is not None and weights:
            weights, day_return = _drift(weights, prices, prev_date, date)
            nav *= (1.0 + day_return)

        # 2) if a decision lands on this trading date, possibly rebalance.
        decision_row = decision_by_trade_date.get(date)
        if decision_row is not None:
            available = prices.available_tickers(date)
            if len(available) < len(SLEEVE_TICKERS):
                reduced_dates.append(date)
            new_quadrant = decision_row.quadrant if decision_row.has_valid_quadrant() else active_quadrant
            if new_quadrant is not None:
                desired = target_weights(new_quadrant, params, available, compressed=compressed)
                quadrant_changed = new_quadrant != active_quadrant
                drift_breached = _max_drift(weights, desired) > DRIFT_BAND if weights else True
                if quadrant_changed or drift_breached or not weights:
                    one_way = _one_way_turnover(weights, desired)
                    nav *= (1.0 - cost_rate * one_way)
                    weights = dict(desired)
                    active_target = dict(desired)
                    active_quadrant = new_quadrant
                    rebalance_dates.append(date)
                    turnover_by_date[date] = one_way

        dates_out.append(date)
        nav_out.append(nav)
        prev_date = date

    return SleeveResult(
        dates=dates_out, nav=nav_out, rebalance_dates=rebalance_dates,
        one_way_turnover_by_date=turnover_by_date,
        reduced_sleeve_dates=sorted(set(reduced_dates)),
        data_quality_flags=prices.data_quality_flags(start, end),
        seed_rebalance_date=rebalance_dates[0] if rebalance_dates else None,
    )


def _schedule_decisions(
    prices: PriceFrame, decisions: Sequence[Any], trading_dates: Sequence[_dt.date],
) -> dict[_dt.date, Any]:
    """Assign each decision's month-end to the first trading date on/after it that
    lies within the window (missing-day rule: defer to next session with data)."""
    trade_set = list(trading_dates)
    out: dict[_dt.date, Any] = {}
    import bisect
    for row in decisions:
        idx = bisect.bisect_left(trade_set, row.as_of)
        if idx < len(trade_set):
            landing = trade_set[idx]
            # keep the LATEST decision (by as_of) that lands on a given trade date.
            # Pre-window lookback decisions all bisect to the first in-window trade
            # date; the window must be seeded with the most recent latched position
            # (production semantics), not the earliest lookback quadrant.
            existing = out.get(landing)
            if existing is None or row.as_of >= existing.as_of:
                out[landing] = row
    return out


def _drift(
    weights: Mapping[str, float], prices: PriceFrame,
    prev_date: _dt.date, date: _dt.date,
) -> tuple[dict[str, float], float]:
    """Advance weights by one day; return (new_weights, portfolio_day_return).

    A ticker with a missing price on either day contributes zero return that day and
    its weight is carried (no interpolation, no synthetic prices)."""
    new_values: dict[str, float] = {}
    for ticker, w in weights.items():
        p0 = prices.price(ticker, prev_date)
        p1 = prices.price(ticker, date)
        if p0 and p1 and p0 > 0:
            ret = p1 / p0 - 1.0
        else:
            ret = 0.0
        new_values[ticker] = w * (1.0 + ret)
    gross = sum(new_values.values())
    day_return = gross - 1.0  # weights summed to 1 pre-drift
    if gross > 0:
        new_weights = {t: v / gross for t, v in new_values.items()}
    else:
        new_weights = dict(weights)
    return new_weights, day_return


def _max_drift(current: Mapping[str, float], target: Mapping[str, float]) -> float:
    tickers = set(current) | set(target)
    return max((abs(current.get(t, 0.0) - target.get(t, 0.0)) for t in tickers), default=0.0)


def _one_way_turnover(current: Mapping[str, float], target: Mapping[str, float]) -> float:
    tickers = set(current) | set(target)
    return 0.5 * sum(abs(target.get(t, 0.0) - current.get(t, 0.0)) for t in tickers)
