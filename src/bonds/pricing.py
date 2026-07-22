"""Pure, DB-free bond pricing / yield motor (Increment 3, Phase 10 — research grade).

Given a :class:`~src.bonds.cashflows.Schedule` (built by the Task-2 cash-flow
motor) plus an explicit settlement date, this module derives price, yield, and
risk metrics with periodic compounding at the coupon frequency. There is no I/O,
no database driver, no wall-clock read, and no third-party dependency (not even
``numpy``); every result is reproducible from its inputs.

Pricing model (documented conventions)
--------------------------------------
* **Periodic / street discounting.** For a coupon bond the remaining cash flows
  sit on a regular grid. With ``m = frequency`` and per-period rate ``i = y/m``,
  the full (dirty / invoice) price is

      dirty = Σ_k  CF_k / (1 + i) ** (w + j_k)

  where ``j_k = 0, 1, 2, …`` indexes the successive future coupon dates and
  ``w = 1 − days_accrued / days_in_period`` is the *remaining* fraction of the
  current coupon period, taken from the Task-2 accrued object. ``w`` uses the
  actual/actual (for the ACT family) or 30/360 (for 30/360) period fraction —
  the ISDA/ICMA "street" convention — so it is exact for regular schedules and
  yields the par-at-par identity ``price(y = coupon) = 100`` on a coupon date.

* **Clean vs dirty.** ``clean = dirty − accrued`` where ``accrued`` is the
  Task-2 accrued interest for the bond's own day count. **ACT/360 nuance:** the
  Task-2 ACT/360 accrued is *money-market* (``rate·face·actual/360``) while the
  discount exponent ``w`` uses the actual/actual period fraction; ``clean`` is
  *defined* as ``dirty − (money-market accrued)``, and the par identity still
  holds on coupon dates (accrued 0). The two conventions are documented rather
  than silently reconciled.

* **Zero-coupon bonds** have no coupon period: ``dirty = F / (1 + i) ** (m·τ)``
  with ``τ = year_fraction(settlement, maturity)`` in the bond's day count.
  ACT/ACT ICMA has no two-date year fraction, so a zero with that day count
  raises the same typed ``year_fraction_requires_coupon_period`` error the
  cash-flow motor uses — never a NaN.

* **Curve discounting** (Z-spread, rolldown) is a *separate* convention: each
  cash flow is discounted with ACT/365F time from the pricing date and periodic
  compounding at ``m`` on the interpolated zero rate plus a parallel spread —
  ``CF / (1 + (z(τ) + s)/m) ** (m·τ)``. The spot curve interpolates linearly in
  the zero rate between nodes and is flat outside the node range.

Every degenerate input raises a typed :class:`~src.bonds.errors.BondError` with
a stable ``code`` — never a NaN or a silent zero. No value produced here reaches
any production surface in this increment (Global Constraint #3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from .cashflows import (
    BondTerms,
    CallOption,
    CashFlow,
    CashFlowKind,
    Schedule,
    accrued_interest,
    generate_schedule,
    year_fraction,
)
from .errors import BondError

# --------------------------------------------------------------------------- #
# Solver / model constants (documented, declared once)
# --------------------------------------------------------------------------- #
# Periodic-rate bracket: i = y/m ∈ (PERIODIC_RATE_FLOOR, MAX_PERIODIC_RATE].
# In annualised terms the yield bracket is (FLOOR·m, MAX·m) — e.g. semiannual
# ⇒ y ∈ (−1.998, 20.0). Realistic prices always bracket inside this range.
PERIODIC_RATE_FLOOR = -0.999
MAX_PERIODIC_RATE = 10.0
# Parallel Z-spread bracket (annualised), decreasing PV in the spread.
SPREAD_FLOOR = -0.99
SPREAD_CEILING = 10.0
# Bisection controls.
_SOLVER_XTOL = 1e-14
_SOLVER_FTOL = 1e-10
_SOLVER_MAX_ITER = 200
# Default symmetric yield bump for effective duration / convexity (1 basis point).
DEFAULT_YIELD_BUMP = 1e-4
# Curve discounting uses ACT/365F calendar time.
_CURVE_DAYS_PER_YEAR = 365.0

# --------------------------------------------------------------------------- #
# Validation status (code markers consumed by the Phase-10 gate — Increment 3).
# --------------------------------------------------------------------------- #
# The honest, per-family validation state as ``tests/bonds/test_pricing.py``
# actually establishes it (Global Constraint #4 — never over-claim).  The Phase-10
# gate reads these exact strings as ``model_validated``; it never reinterprets them.
#
# * YIELD family (YTM and the price<->yield solve it anchors, plus current yield,
#   Z-spread, carry/rolldown): validated against a CANONICAL PUBLISHED worked
#   example (Fabozzi, *Bond Markets, Analysis and Strategies* — 10% coupon, 5y,
#   semiannual, price 96.23 <-> yield 11%), cross-checked to the closed-form
#   annuity price.  Status ``authoritative_published``.
# * DURATION family (modified / effective duration, and every duration-shaped
#   risk that inherits the bump-and-reprice machinery): the suite validates it
#   only ``convention_derived`` (Macaulay/(1+i)) and by internal-consistency
#   ``property`` — NO authoritative PRINTED duration sample is reproduced yet.
#   That printed sample is an OPEN PROGRAM ITEM, so the status stays
#   ``authoritative_sample_pending``; the gate surfaces it as
#   ``authoritative_duration_sample_pending``.
VALIDATION_STATUS_AUTHORITATIVE = "authoritative_published"
VALIDATION_STATUS_SAMPLE_PENDING = "authoritative_sample_pending"
PRICING_VALIDATION_STATUS: dict[str, str] = {
    "yield": VALIDATION_STATUS_AUTHORITATIVE,
    "duration": VALIDATION_STATUS_SAMPLE_PENDING,
}


# --------------------------------------------------------------------------- #
# Result / input value objects
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PriceQuote:
    """A price observation: ``dirty == clean + accrued`` by construction."""

    clean: float
    dirty: float
    accrued: float


@dataclass(frozen=True)
class YieldToWorst:
    """Yield-to-worst with the winning scenario and every yield-to-call."""

    ytw: float
    to_maturity: float
    worst_kind: str  # "maturity" | "call"
    worst_date: date
    calls: tuple[tuple[date, float], ...]  # (call_date, yield_to_call)


@dataclass(frozen=True)
class SpotCurve:
    """A spot (zero) curve: nodes of ``(tenor_years, zero_rate)``.

    Rates are annual nominal, compounded at the bond frequency when discounting.
    The curve interpolates linearly in the zero rate between nodes and is flat
    (constant) below the first and above the last node. At least one node is
    required; a single node is a flat curve.
    """

    nodes: tuple[tuple[float, float], ...]
    _sorted: tuple[tuple[float, float], ...] = field(default=(), compare=False, repr=False)

    def __post_init__(self) -> None:
        if not self.nodes:
            raise BondError("empty_spot_curve", {})
        ordered = tuple(sorted(self.nodes, key=lambda n: n[0]))
        previous: float | None = None
        for tenor, _rate in ordered:
            if not isinstance(tenor, (int, float)) or tenor <= 0:
                raise BondError("non_positive_curve_tenor", {"tenor": tenor})
            if previous is not None and tenor <= previous:
                raise BondError("curve_tenor_not_increasing", {"tenor": tenor})
            previous = float(tenor)
        object.__setattr__(self, "_sorted", ordered)

    def rate(self, tenor_years: float) -> float:
        """Linearly interpolated zero rate at ``tenor_years`` (flat outside)."""
        nodes = self._sorted
        if tenor_years <= nodes[0][0]:
            return nodes[0][1]
        if tenor_years >= nodes[-1][0]:
            return nodes[-1][1]
        for (t0, r0), (t1, r1) in zip(nodes, nodes[1:]):
            if t0 <= tenor_years <= t1:
                weight = (tenor_years - t0) / (t1 - t0)
                return r0 + weight * (r1 - r0)
        # Unreachable given the guards above, but never fall through silently.
        raise BondError("curve_interpolation_failed", {"tenor": tenor_years})


# --------------------------------------------------------------------------- #
# Core discounting
# --------------------------------------------------------------------------- #
def _validate_settlement(terms: BondTerms, settlement: date) -> None:
    if settlement < terms.issue_date:
        raise BondError(
            "settlement_before_issue",
            {"settlement": settlement.isoformat(), "issue_date": terms.issue_date.isoformat()},
        )
    if settlement >= terms.maturity_date:
        raise BondError(
            "settlement_after_maturity",
            {"settlement": settlement.isoformat(), "maturity_date": terms.maturity_date.isoformat()},
        )


def _accrued_amount(terms: BondTerms, settlement: date) -> float:
    """Task-2 accrued interest at ``settlement`` (0 for a zero-coupon bond)."""
    if terms.is_zero_coupon:
        _validate_settlement(terms, settlement)
        return 0.0
    return accrued_interest(terms, settlement).amount


def _future_flow_exponents(schedule: Schedule, settlement: date) -> tuple[int, tuple[tuple[float, float], ...]]:
    """Return ``(m, ((amount, period_exponent), …))`` for flows after settlement.

    The period exponent is ``w + j`` for a coupon bond (``w`` the remaining
    fraction of the current period, ``j`` the future-coupon index) and ``m·τ``
    for a zero-coupon bond.
    """
    terms = schedule.terms
    _validate_settlement(terms, settlement)
    m = int(terms.frequency)
    future = tuple(cf for cf in schedule.cashflows if cf.pay_date > settlement)
    if not future:
        raise BondError("no_future_cashflows", {"settlement": settlement.isoformat()})

    if terms.is_zero_coupon:
        tau = year_fraction(settlement, terms.maturity_date, terms.day_count)  # raises for ICMA
        exponent = m * tau
        return m, tuple((cf.amount, exponent) for cf in future)

    accrued = accrued_interest(terms, settlement)
    if accrued.days_in_period <= 0:
        raise BondError("degenerate_coupon_period", {"settlement": settlement.isoformat()})
    remaining = 1.0 - accrued.days_accrued / accrued.days_in_period
    distinct = sorted({cf.pay_date for cf in future})
    index = {pay_date: j for j, pay_date in enumerate(distinct)}
    return m, tuple((cf.amount, remaining + index[cf.pay_date]) for cf in future)


def _pv(pairs: tuple[tuple[float, float], ...], periodic_rate: float) -> float:
    if periodic_rate <= -1.0:
        raise BondError("yield_below_periodic_floor", {"periodic_rate": periodic_rate})
    base = 1.0 + periodic_rate
    return sum(amount / base**exponent for amount, exponent in pairs)


def dirty_price(schedule: Schedule, settlement: date, ytm: float) -> float:
    """Full (dirty / invoice) price at ``settlement`` for annualised yield ``ytm``."""
    m, pairs = _future_flow_exponents(schedule, settlement)
    return _pv(pairs, ytm / m)


def clean_price(schedule: Schedule, settlement: date, ytm: float) -> float:
    """Clean price = dirty − Task-2 accrued interest."""
    dirty = dirty_price(schedule, settlement, ytm)
    return dirty - _accrued_amount(schedule.terms, settlement)


def price_quote(schedule: Schedule, settlement: date, ytm: float) -> PriceQuote:
    """Bundle ``clean`` / ``dirty`` / ``accrued`` with ``dirty == clean + accrued``."""
    accrued = _accrued_amount(schedule.terms, settlement)
    dirty = dirty_price(schedule, settlement, ytm)
    return PriceQuote(clean=dirty - accrued, dirty=dirty, accrued=accrued)


# --------------------------------------------------------------------------- #
# Root finder (bracketing bisection on a monotone-decreasing value function)
# --------------------------------------------------------------------------- #
def _solve_decreasing(func, target: float, lo: float, hi: float, err_prefix: str) -> float:
    """Solve ``func(x) == target`` for a monotone-decreasing ``func`` on [lo, hi].

    ``func`` decreases in ``x`` (price falls as yield/spread rises). Returns the
    unique root, or raises ``{err_prefix}_out_of_bounds`` when ``target`` lies
    outside ``[func(hi), func(lo)]`` and ``{err_prefix}_no_convergence`` if the
    bracket fails to tighten (defensive; bisection converges in << max_iter).
    """
    g_lo = func(lo) - target
    g_hi = func(hi) - target
    if g_lo == 0.0:
        return lo
    if g_hi == 0.0:
        return hi
    if g_lo < 0.0:  # even at the lowest rate the value is below target
        raise BondError(f"{err_prefix}_out_of_bounds", {"target": target, "reason": "above_max_value"})
    if g_hi > 0.0:  # even at the highest rate the value is above target
        raise BondError(f"{err_prefix}_out_of_bounds", {"target": target, "reason": "below_min_value"})

    a, b = lo, hi
    for _ in range(_SOLVER_MAX_ITER):
        mid = 0.5 * (a + b)
        g_mid = func(mid) - target
        if g_mid == 0.0 or (b - a) < _SOLVER_XTOL:
            return mid
        if g_mid > 0.0:  # decreasing: still left of the root
            a = mid
        else:
            b = mid
    mid = 0.5 * (a + b)
    if abs(func(mid) - target) <= _SOLVER_FTOL:
        return mid
    raise BondError(f"{err_prefix}_no_convergence", {"target": target})


# --------------------------------------------------------------------------- #
# Yields
# --------------------------------------------------------------------------- #
def yield_to_maturity(schedule: Schedule, settlement: date, price: float, *, price_is_clean: bool = True) -> float:
    """Solve price → annualised yield (periodic compounding at the coupon freq).

    ``price`` is a clean price by default; pass ``price_is_clean=False`` for a
    dirty price. Bounded by ``PERIODIC_RATE_FLOOR/MAX_PERIODIC_RATE`` (see module
    docstring); a price outside that bracket raises ``ytm_out_of_bounds``.
    """
    if price <= 0.0:
        raise BondError("non_positive_price", {"price": price})
    m, pairs = _future_flow_exponents(schedule, settlement)
    target = price if not price_is_clean else price + _accrued_amount(schedule.terms, settlement)

    def pv_of_yield(y: float) -> float:
        return _pv(pairs, y / m)

    return _solve_decreasing(
        pv_of_yield, target, PERIODIC_RATE_FLOOR * m, MAX_PERIODIC_RATE * m, err_prefix="ytm"
    )


def current_yield(schedule: Schedule, clean_price_value: float) -> float:
    """Annual coupon income divided by clean price (0 for a zero-coupon bond)."""
    if clean_price_value <= 0.0:
        raise BondError("non_positive_price", {"price": clean_price_value})
    terms = schedule.terms
    return terms.coupon_rate * terms.face / clean_price_value


# --------------------------------------------------------------------------- #
# Yield to call / worst
# --------------------------------------------------------------------------- #
def _call_scenario_schedule(terms: BondTerms, call: CallOption) -> Schedule:
    """Truncated schedule redeemed at the call: coupons ≤ call date + call price.

    Reuses :func:`generate_schedule` (does not re-derive the coupon grid): the
    base schedule is generated, then truncated to cash flows on or before the
    call date, and its redemption replaced by the call price (per 100 of face,
    scaled to ``face``). The call date must be a coupon date (on the grid).
    """
    base = generate_schedule(terms)
    if call.call_date not in set(base.coupon_dates):
        raise BondError("call_not_on_coupon_grid", {"call_date": call.call_date.isoformat()})
    coupons = tuple(
        cf for cf in base.cashflows if cf.kind == CashFlowKind.COUPON and cf.pay_date <= call.call_date
    )
    redemption = CashFlow(call.call_date, call.price * terms.face / 100.0, CashFlowKind.REDEMPTION)
    coupon_dates_to_call = tuple(d for d in base.coupon_dates if d <= call.call_date)
    flows = tuple(
        sorted(
            coupons + (redemption,),
            key=lambda cf: (cf.pay_date, 0 if cf.kind == CashFlowKind.COUPON else 1),
        )
    )
    return Schedule(terms, coupon_dates_to_call, flows)


def yield_to_call(
    schedule: Schedule, settlement: date, price: float, call: CallOption, *, price_is_clean: bool = True
) -> float:
    """Yield that equates the price to the call-truncated schedule's PV."""
    scenario = _call_scenario_schedule(schedule.terms, call)
    return yield_to_maturity(scenario, settlement, price, price_is_clean=price_is_clean)


def yield_to_worst(schedule: Schedule, settlement: date, price: float, *, price_is_clean: bool = True) -> YieldToWorst:
    """Minimum qualified yield over maturity and each future call scenario.

    Calls whose date is at or before settlement are unqualified and skipped.
    For a premium callable this is ≤ the yield to maturity (property-tested).
    """
    terms = schedule.terms
    ytm = yield_to_maturity(schedule, settlement, price, price_is_clean=price_is_clean)
    scenarios: list[tuple[str, date, float]] = [("maturity", terms.maturity_date, ytm)]
    for call in terms.call_schedule:
        if call.call_date <= settlement:
            continue
        scenario = _call_scenario_schedule(terms, call)
        if all(cf.pay_date <= settlement for cf in scenario.cashflows):
            continue
        ytc = yield_to_maturity(scenario, settlement, price, price_is_clean=price_is_clean)
        scenarios.append(("call", call.call_date, ytc))

    worst_kind, worst_date, worst_yield = min(scenarios, key=lambda s: s[2])
    calls = tuple((d, y) for kind, d, y in scenarios if kind == "call")
    return YieldToWorst(
        ytw=worst_yield, to_maturity=ytm, worst_kind=worst_kind, worst_date=worst_date, calls=calls
    )


# --------------------------------------------------------------------------- #
# Duration / convexity
# --------------------------------------------------------------------------- #
def modified_duration(schedule: Schedule, settlement: date, ytm: float) -> float:
    """Analytic modified duration (years) = Macaulay / (1 + y/m), on dirty price.

    ``ModDur = −(1/P)·dP/dy`` with ``P`` the full price; equals
    ``(Σ exponent·PV_k) / (m·P·(1 + y/m))``.
    """
    m, pairs = _future_flow_exponents(schedule, settlement)
    i = ytm / m
    pv = _pv(pairs, i)
    if pv <= 0.0:
        raise BondError("non_positive_price", {"price": pv})
    base = 1.0 + i
    weighted = sum(exponent * amount / base**exponent for amount, exponent in pairs)
    macaulay_years = weighted / (m * pv)
    return macaulay_years / base


def effective_duration(schedule: Schedule, settlement: date, ytm: float, *, bump: float = DEFAULT_YIELD_BUMP) -> float:
    """Effective (bump-and-reprice) duration: ``(P(y−Δ) − P(y+Δ)) / (2·P·Δ)``.

    ``Δ = bump`` (default 1 bp). Priced on the full (dirty) price.
    """
    if bump <= 0.0:
        raise BondError("non_positive_bump", {"bump": bump})
    p0 = dirty_price(schedule, settlement, ytm)
    if p0 <= 0.0:
        raise BondError("non_positive_price", {"price": p0})
    p_up = dirty_price(schedule, settlement, ytm + bump)
    p_dn = dirty_price(schedule, settlement, ytm - bump)
    return (p_dn - p_up) / (2.0 * p0 * bump)


def convexity(schedule: Schedule, settlement: date, ytm: float, *, bump: float = DEFAULT_YIELD_BUMP) -> float:
    """Effective convexity via a central second difference (``bump²``)."""
    if bump <= 0.0:
        raise BondError("non_positive_bump", {"bump": bump})
    p0 = dirty_price(schedule, settlement, ytm)
    if p0 <= 0.0:
        raise BondError("non_positive_price", {"price": p0})
    p_up = dirty_price(schedule, settlement, ytm + bump)
    p_dn = dirty_price(schedule, settlement, ytm - bump)
    return (p_up + p_dn - 2.0 * p0) / (p0 * bump * bump)


# --------------------------------------------------------------------------- #
# Curve pricing, Z-spread, carry, rolldown
# --------------------------------------------------------------------------- #
def curve_price(schedule: Schedule, settlement: date, curve: SpotCurve, spread: float = 0.0) -> float:
    """Full (dirty) PV discounting each flow on ``curve`` + parallel ``spread``.

    Uses ACT/365F time from ``settlement`` and periodic compounding at ``m``:
    ``CF / (1 + (z(τ) + spread)/m) ** (m·τ)`` (see module docstring).
    """
    terms = schedule.terms
    _validate_settlement(terms, settlement)
    m = int(terms.frequency)
    future = tuple(cf for cf in schedule.cashflows if cf.pay_date > settlement)
    if not future:
        raise BondError("no_future_cashflows", {"settlement": settlement.isoformat()})
    total = 0.0
    for cf in future:
        tau = (cf.pay_date - settlement).days / _CURVE_DAYS_PER_YEAR
        base = 1.0 + (curve.rate(tau) + spread) / m
        if base <= 0.0:
            raise BondError("curve_rate_below_floor", {"tenor": tau, "rate_plus_spread": base - 1.0})
        total += cf.amount / base ** (m * tau)
    return total


def _curve_clean_price(schedule: Schedule, settlement: date, curve: SpotCurve, spread: float = 0.0) -> float:
    return curve_price(schedule, settlement, curve, spread) - _accrued_amount(schedule.terms, settlement)


def z_spread(
    schedule: Schedule, settlement: date, price: float, curve: SpotCurve, *, price_is_clean: bool = True
) -> float:
    """Parallel spread over ``curve`` that reprices the bond to ``price``.

    Bounded by ``SPREAD_FLOOR/SPREAD_CEILING``; a price outside the bracket
    raises ``z_spread_out_of_bounds`` and a stalled solve ``z_spread_no_convergence``.
    """
    if price <= 0.0:
        raise BondError("non_positive_price", {"price": price})
    target = price if not price_is_clean else price + _accrued_amount(schedule.terms, settlement)

    def pv_of_spread(s: float) -> float:
        return curve_price(schedule, settlement, curve, s)

    return _solve_decreasing(pv_of_spread, target, SPREAD_FLOOR, SPREAD_CEILING, err_prefix="z_spread")


def _validate_horizon(terms: BondTerms, settlement: date, horizon: date) -> None:
    if horizon <= settlement or horizon >= terms.maturity_date:
        raise BondError(
            "horizon_out_of_range",
            {
                "settlement": settlement.isoformat(),
                "horizon": horizon.isoformat(),
                "maturity": terms.maturity_date.isoformat(),
            },
        )


def carry(schedule: Schedule, settlement: date, horizon: date) -> float:
    """Gross coupon income over ``(settlement, horizon]``.

    ``carry = Σ coupons paid in (settlement, horizon] + (accrued(horizon) −
    accrued(settlement))`` — the total coupon accrued over the horizon,
    telescoping across any coupon payments. Excludes price change and funding.
    A zero-coupon bond carries 0 (its horizon return is pure rolldown).
    """
    terms = schedule.terms
    _validate_settlement(terms, settlement)
    _validate_horizon(terms, settlement, horizon)
    coupons = sum(
        cf.amount
        for cf in schedule.cashflows
        if cf.kind == CashFlowKind.COUPON and settlement < cf.pay_date <= horizon
    )
    return coupons + _accrued_amount(terms, horizon) - _accrued_amount(terms, settlement)


def rolldown(schedule: Schedule, settlement: date, horizon: date, curve: SpotCurve) -> float:
    """Clean-price roll on an UNCHANGED curve as the bond ages to ``horizon``.

    ``rolldown = clean_curve_price(horizon) − clean_curve_price(settlement)`` —
    the pull-to-par + roll-down-the-curve price effect from time passing while
    the spot curve is held fixed. It composes exactly with :func:`carry`:
    ``rolldown + carry == curve_dirty(horizon) − curve_dirty(settlement) +
    coupons_in_window`` (the full holding-period P&L on the curve).
    """
    terms = schedule.terms
    _validate_settlement(terms, settlement)
    _validate_horizon(terms, settlement, horizon)
    return _curve_clean_price(schedule, horizon, curve) - _curve_clean_price(schedule, settlement, curve)
