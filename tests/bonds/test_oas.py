"""Validation of the research-grade OAS motor (`src.bonds.oas`).

HONESTY POSTURE (Increment 3 Global Constraint #4 + Task-4 brief).  No
end-to-end published OAS worked example is reproducible from memory with the
confidence #4 demands, so **NO vector is labelled ``authoritative_published``**
and the engine's validation status STAYS ``model_validation_incomplete`` (a
module constant Task-6's gate consumes).  What IS asserted, each labelled
honestly:

- ``convention_derived`` — a deterministic small lattice (2 steps) whose
  calibration drift, zero-repricing, and callable backward induction are written
  out BY HAND in the comments and recomputed inline to full precision.  This is
  the strongest vector class: no external authority, pure arithmetic the reviewer
  re-derives.
- ``cross_check`` — the lattice reconciled against the Task-3 closed-form motor:
  (a) an option-free bond prices on the lattice EXACTLY as ``curve_price``
  (cash-flows on nodes, same ACT/365F curve time basis, same periodic curve DF),
  and (b) on a FLAT curve the lattice OAS equals the Task-3 periodic ``z_spread``
  under the exact continuous<->periodic compounding conversion.  The OAS<->Z-spread
  relationship and its step dependence are documented in the report.
- ``property`` — invariants the motor must satisfy: option cost >= 0 (callable
  OAS <= Z-spread), monotonicity (higher price -> lower OAS), call value shrinks
  as the call price rises, a deep-out-of-the-money call collapses the callable to
  the straight bond, the constant spread pulls out continuously for an
  option-free bond, and the callable value converges (decaying oscillation) as
  the step count grows.

Tolerances are declared inline via ``TOL_*``.  Volatility is an INPUT (constant
``sigma``), never calibrated — stated in the report and the module docstring.
"""

from __future__ import annotations

import math
from datetime import date

import pytest

from src.bonds.cashflows import (
    BondTerms,
    CallOption,
    DayCount,
    Frequency,
    generate_schedule,
)
from src.bonds.errors import BondError
from src.bonds.pricing import SpotCurve, curve_price, z_spread
from src.bonds.oas import (
    MODEL_VALIDATION_STATUS,
    HoLeeLattice,
    build_lattice,
    oas_from_price,
    solve_oas,
)

# Declared tolerances (per Global Constraint #4).
TOL_HAND = 1e-9  # inline-recomputed hand lattice arithmetic
TOL_REPRICE = 1e-12  # tree reprices its own calibration zeros
TOL_CROSS = 1e-9  # lattice option-free price vs Task-3 curve_price
TOL_CONV = 1e-9  # continuous <-> periodic spread conversion, flat curve
TOL_OAS = 1e-10  # OAS bisection round-trip


# --------------------------------------------------------------------------- #
# CONVENTION_DERIVED — hand lattice #1: calibration + zero repricing (2 steps).
#
#   dt = 1.0, sigma = 0.10  =>  rate_step = 2*sigma*sqrt(dt) = 0.20.
#   Target zeros from a flat 4% CONTINUOUS curve:
#       P1 = exp(-0.04) = 0.96078944...,  P2 = exp(-0.08) = 0.92311635...
#   Step 0 (root, Q(0,0)=1):  a_0 = -ln(P1) = 0.04 exactly (r_{0,0}=0.04).
#     d_{0,0} = exp(-0.04) = P1.  Q(1,0)=Q(1,1)=0.5*P1 = 0.48039472.
#   Step 1:  a_1 = -ln( P2 / [Q(1,0) + Q(1,1)*exp(-0.20)] )
#              = -ln( 0.92311635 / (0.48039472*(1+0.81873075)) )
#              = -0.05500831...   =>  r_{1,0} = -0.05500831, r_{1,1} = 0.14499169.
#   Tree reprices P2:  0.5*P1*(exp(0.05500831)+exp(-0.14499169)) = 0.92311635.
# --------------------------------------------------------------------------- #
def test_calibration_two_step_flat_curve_hand_convention_derived() -> None:
    p1, p2 = math.exp(-0.04), math.exp(-0.08)
    lat = HoLeeLattice(dt=1.0, sigma=0.10, target_dfs=(p1, p2))

    assert lat.rate_step == pytest.approx(0.20, abs=TOL_HAND)
    # a_0 = -ln(P1) = 0.04 exactly.
    assert lat.drift[0] == pytest.approx(0.04, abs=TOL_HAND)
    # a_1 recomputed inline from the state-price closed form.
    q1 = 0.5 * p1
    a1 = -math.log(p2 / (q1 + q1 * math.exp(-0.20)))
    assert lat.drift[1] == pytest.approx(a1, abs=TOL_HAND)
    # short-rate grid: r_{1,1} = a_1 + rate_step.
    assert lat.short_rate(1, 1) == pytest.approx(a1 + 0.20, abs=TOL_HAND)
    assert lat.short_rate(0, 0) == pytest.approx(0.04, abs=TOL_HAND)
    # the calibrated tree reprices its own zero curve exactly.
    assert lat.zero_price(0) == pytest.approx(1.0, abs=TOL_REPRICE)
    assert lat.zero_price(1) == pytest.approx(p1, abs=TOL_REPRICE)
    assert lat.zero_price(2) == pytest.approx(p2, abs=TOL_REPRICE)


# --------------------------------------------------------------------------- #
# CONVENTION_DERIVED — hand lattice #2: callable backward induction (2 steps).
#
#   Same lattice.  Bond: 5% annual coupon, 2y, face 100.  Nodes: cf[1]=5,
#   cf[2]=105.  Callable at node 1 at price 100 (a cash call payment of 100;
#   the coupon of 5 is paid in addition -> declared accrued treatment: calls sit
#   on coupon dates so accrued is 0 and the call payment is the clean call price).
#   Backward induction (spread 0):
#     V(2,*) = 105.
#     cont(1,0) = exp(+0.05500831)*105 = 110.9377;  min(.,100)=100 -> CALLED.
#     cont(1,1) = exp(-0.14499169)*105 =  90.8281;  min(.,100)=90.8281 -> HELD.
#     V(1,0) = 5 + 100      = 105.0000;  V(1,1) = 5 + 90.8281 = 95.8281.
#     cont(0,0) = exp(-0.04)*0.5*(105.0000+95.8281) = 96.476757.
#   Straight (no call): V(1,0)=5+110.9377=115.9377; V(0,0)=exp(-0.04)*0.5*
#     (115.9377+95.8281) = 101.731164.  Option cost = 5.254407.
# --------------------------------------------------------------------------- #
def test_backward_induction_callable_two_step_hand_convention_derived() -> None:
    p1, p2 = math.exp(-0.04), math.exp(-0.08)
    lat = HoLeeLattice(dt=1.0, sigma=0.10, target_dfs=(p1, p2))
    cashflows = {1: 5.0, 2: 105.0}

    # inline recomputation to full precision (reviewer re-derives the arithmetic)
    a1 = lat.drift[1]
    cont_10 = math.exp(-a1) * 105.0
    cont_11 = math.exp(-(a1 + 0.20)) * 105.0
    v1_0 = 5.0 + min(cont_10, 100.0)
    v1_1 = 5.0 + min(cont_11, 100.0)
    expected_callable = math.exp(-0.04) * 0.5 * (v1_0 + v1_1)
    expected_straight = math.exp(-0.04) * 0.5 * ((5.0 + cont_10) + (5.0 + cont_11))

    callable_v = lat.value(cashflows, {1: 100.0})
    straight_v = lat.value(cashflows)
    assert callable_v == pytest.approx(expected_callable, abs=TOL_HAND)
    assert straight_v == pytest.approx(expected_straight, abs=TOL_HAND)
    # exercise decision is real: at node (1,0) the call binds (cont > 100), at
    # (1,1) it does not (cont < 100).
    assert cont_10 > 100.0 and cont_11 < 100.0
    # a call price so high it never binds recovers the straight bond exactly.
    assert lat.value(cashflows, {1: 1e6}) == pytest.approx(straight_v, abs=TOL_HAND)
    # the callable is worth strictly less than the straight bond (option cost>0).
    assert callable_v < straight_v


# --------------------------------------------------------------------------- #
# Fixtures: an ACT/365F bond whose annual periods are all exactly 365 days
# (issue 2021-01-15 -> maturity 2024-01-15; no Feb-29 falls inside any span), so
# coupon times are tau = 1, 2, 3 EXACTLY and every cash flow lands on a lattice
# node for any steps_per_period.  This is what makes the option-free cross-check
# exact (not a coincidence of rounding).
# --------------------------------------------------------------------------- #
def _annual_bond(coupon: float = 0.05, calls: tuple[CallOption, ...] = ()) -> BondTerms:
    return BondTerms(
        issue_date=date(2021, 1, 15),
        maturity_date=date(2024, 1, 15),
        coupon_rate=coupon,
        frequency=Frequency.ANNUAL,
        day_count=DayCount.ACT_365F,
        face=100.0,
        call_schedule=calls,
    )


_SETTLE = date(2021, 1, 15)
_CURVE = SpotCurve(nodes=((1.0, 0.03), (3.0, 0.05)))
_FLAT = SpotCurve(nodes=((1.0, 0.04), (3.0, 0.04)))


# --------------------------------------------------------------------------- #
# CROSS_CHECK — an option-free bond prices on the lattice EXACTLY as the Task-3
# closed-form curve_price (same ACT/365F time, same periodic curve DF at nodes).
# Independent of step count, because the calibrated tree reprices every node's
# zero exactly.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("spp", [1, 2, 4, 8])
def test_option_free_lattice_equals_curve_price_cross_check(spp: int) -> None:
    terms = _annual_bond(coupon=0.05)
    sched = generate_schedule(terms)
    inp = build_lattice(sched, _SETTLE, _CURVE, sigma=0.10, steps_per_period=spp)
    lattice_price = inp.lattice.value(inp.cashflows_by_node, inp.call_by_node)
    closed = curve_price(sched, _SETTLE, _CURVE, 0.0)
    assert lattice_price == pytest.approx(closed, abs=TOL_CROSS)


# --------------------------------------------------------------------------- #
# CROSS_CHECK — on a FLAT curve, the option-free constant spread pulls out
# continuously, so lattice OAS is the CONTINUOUS spread that maps to the Task-3
# PERIODIC z_spread by the exact conversion s_c = m*ln((1+(z+s_p)/m)/(1+z/m)).
# --------------------------------------------------------------------------- #
def test_flat_curve_oas_equals_periodic_zspread_conversion_cross_check() -> None:
    terms = _annual_bond(coupon=0.05)  # no calls -> option-free
    sched = generate_schedule(terms)
    m = int(terms.frequency)  # 1
    z = 0.04
    target = 98.0  # below par -> positive spread
    oas = oas_from_price(sched, _SETTLE, _FLAT, target, sigma=0.10, steps_per_period=1)
    zs = z_spread(sched, _SETTLE, target, _FLAT)
    converted = m * math.log((1.0 + (z + zs) / m) / (1.0 + z / m))
    assert oas == pytest.approx(converted, abs=TOL_CONV)


# --------------------------------------------------------------------------- #
# PROPERTY — the option-free constant spread pulls out as exp(-s*tau) (the
# defining feature of a continuously-compounded lattice OAS).
# --------------------------------------------------------------------------- #
def test_option_free_spread_pulls_out_continuously_property() -> None:
    terms = _annual_bond(coupon=0.05)
    sched = generate_schedule(terms)
    inp = build_lattice(sched, _SETTLE, _CURVE, sigma=0.12, steps_per_period=3)
    base = {n: inp.cashflows_by_node[n] for n in inp.cashflows_by_node}
    s = 0.015
    priced = inp.lattice.value(base, {}, spread=s)
    # value(s) == sum_k cf_k * zero(node_k) * exp(-s*tau_k)
    expected = sum(
        amt * inp.lattice.zero_price(node) * math.exp(-s * inp.node_times[node])
        for node, amt in base.items()
    )
    assert priced == pytest.approx(expected, abs=1e-9)


# --------------------------------------------------------------------------- #
# PROPERTY — option cost >= 0: a callable's OAS <= the option-free Z-spread at
# the SAME price (the call can only lower the model value at a given spread, so
# matching the price needs a lower spread).
# --------------------------------------------------------------------------- #
def test_callable_oas_le_zspread_option_cost_nonneg_property() -> None:
    calls = (CallOption(date(2023, 1, 15), 100.0),)
    terms = _annual_bond(coupon=0.05, calls=calls)
    sched = generate_schedule(terms)
    # straight (option-free) schedule = same cash flows, ignore the call.
    straight = generate_schedule(_annual_bond(coupon=0.05))
    price = 99.0
    oas = oas_from_price(sched, _SETTLE, _CURVE, price, sigma=0.10, steps_per_period=8)
    zs = z_spread(straight, _SETTLE, price, _CURVE)
    assert oas <= zs + 1e-12  # option cost = zs - oas >= 0


# --------------------------------------------------------------------------- #
# PROPERTY — monotonicity: a higher target price implies a lower OAS.
# --------------------------------------------------------------------------- #
def test_oas_monotonic_in_price_property() -> None:
    calls = (CallOption(date(2023, 1, 15), 101.0),)
    terms = _annual_bond(coupon=0.05, calls=calls)
    sched = generate_schedule(terms)
    oas_low = oas_from_price(sched, _SETTLE, _CURVE, 95.0, sigma=0.10, steps_per_period=6)
    oas_high = oas_from_price(sched, _SETTLE, _CURVE, 103.0, sigma=0.10, steps_per_period=6)
    assert oas_high < oas_low


# --------------------------------------------------------------------------- #
# PROPERTY — the call value shrinks as the call price rises (a more expensive
# call is less valuable to the issuer, so the callable is worth more).
# --------------------------------------------------------------------------- #
def test_call_value_shrinks_as_call_price_rises_property() -> None:
    sched = generate_schedule(_annual_bond(coupon=0.08))
    inp_lo = build_lattice(sched, _SETTLE, _CURVE, sigma=0.10, steps_per_period=8)
    straight = inp_lo.lattice.value(inp_lo.cashflows_by_node, {})
    # value the SAME cash flows with calls at rising prices at node = 2*spp.
    call_node = 2 * 8
    v_100 = inp_lo.lattice.value(inp_lo.cashflows_by_node, {call_node: 100.0})
    v_105 = inp_lo.lattice.value(inp_lo.cashflows_by_node, {call_node: 105.0})
    v_110 = inp_lo.lattice.value(inp_lo.cashflows_by_node, {call_node: 110.0})
    # higher call price -> callable closer to straight -> smaller option value.
    assert (straight - v_100) > (straight - v_105) > (straight - v_110) >= 0.0


# --------------------------------------------------------------------------- #
# PROPERTY — a deep out-of-the-money call collapses the callable to the straight
# bond (the option is never exercised).
# --------------------------------------------------------------------------- #
def test_deep_out_of_money_call_equals_straight_property() -> None:
    calls = (CallOption(date(2023, 1, 15), 1000.0),)  # unreachably high
    terms = _annual_bond(coupon=0.05, calls=calls)
    sched = generate_schedule(terms)
    inp = build_lattice(sched, _SETTLE, _CURVE, sigma=0.10, steps_per_period=8)
    callable_v = inp.lattice.value(inp.cashflows_by_node, inp.call_by_node)
    straight_v = inp.lattice.value(inp.cashflows_by_node, {})
    assert callable_v == pytest.approx(straight_v, abs=1e-12)


# --------------------------------------------------------------------------- #
# PROPERTY — convergence: the callable value STABILISES as steps increase.  The
# binomial call boundary produces a decaying sawtooth (documented in the report),
# so the honest assertion is that successive changes at fine resolution are far
# smaller than at coarse resolution (Cauchy contraction), NOT monotone descent.
# --------------------------------------------------------------------------- #
def test_lattice_converges_as_steps_increase_property() -> None:
    calls = (CallOption(date(2023, 1, 15), 100.0),)
    terms = _annual_bond(coupon=0.05, calls=calls)
    sched = generate_schedule(terms)

    def callable_value(spp: int) -> float:
        inp = build_lattice(sched, _SETTLE, _CURVE, sigma=0.10, steps_per_period=spp)
        return inp.lattice.value(inp.cashflows_by_node, inp.call_by_node)

    coarse = abs(callable_value(16) - callable_value(8))
    fine = abs(callable_value(256) - callable_value(128))
    assert fine < coarse  # oscillation amplitude decays
    assert fine < 0.02  # the fine-grid band is tight


# --------------------------------------------------------------------------- #
# PROPERTY — OAS bisection round-trips: repricing at the solved OAS recovers the
# target price.
# --------------------------------------------------------------------------- #
def test_oas_round_trip_property() -> None:
    calls = (CallOption(date(2023, 1, 15), 100.0),)
    terms = _annual_bond(coupon=0.06, calls=calls)
    sched = generate_schedule(terms)
    inp = build_lattice(sched, _SETTLE, _CURVE, sigma=0.10, steps_per_period=8)
    target = 100.5
    oas = solve_oas(inp.lattice, target, inp.cashflows_by_node, inp.call_by_node)
    repriced = inp.lattice.value(inp.cashflows_by_node, inp.call_by_node, spread=oas)
    assert repriced == pytest.approx(target, abs=1e-8)


# --------------------------------------------------------------------------- #
# PROPERTY — the tree reprices its own calibration zeros at every node.
# --------------------------------------------------------------------------- #
def test_lattice_reprices_calibration_zeros_property() -> None:
    dfs = (0.97, 0.94, 0.90, 0.85, 0.79)
    lat = HoLeeLattice(dt=0.5, sigma=0.08, target_dfs=dfs)
    for n, df in enumerate(dfs, start=1):
        assert lat.zero_price(n) == pytest.approx(df, abs=TOL_REPRICE)


# --------------------------------------------------------------------------- #
# Validation status (Global Constraint #4 + brief): the engine STAYS
# model_validation_incomplete; Task-6's gate consumes this exact string.
# --------------------------------------------------------------------------- #
def test_model_validation_status_is_incomplete() -> None:
    assert MODEL_VALIDATION_STATUS == "model_validation_incomplete"


# --------------------------------------------------------------------------- #
# Typed degenerate inputs (never a silent NaN).
# --------------------------------------------------------------------------- #
def test_negative_volatility_is_typed_error() -> None:
    with pytest.raises(BondError) as e:
        HoLeeLattice(dt=1.0, sigma=-0.01, target_dfs=(0.96,))
    assert e.value.code == "invalid_volatility"


def test_non_positive_step_is_typed_error() -> None:
    with pytest.raises(BondError) as e:
        HoLeeLattice(dt=0.0, sigma=0.1, target_dfs=(0.96,))
    assert e.value.code == "non_positive_lattice_step"


def test_non_positive_discount_factor_is_typed_error() -> None:
    with pytest.raises(BondError) as e:
        HoLeeLattice(dt=1.0, sigma=0.1, target_dfs=(0.96, -0.1))
    assert e.value.code == "non_positive_discount_factor"


def test_empty_lattice_is_typed_error() -> None:
    with pytest.raises(BondError) as e:
        HoLeeLattice(dt=1.0, sigma=0.1, target_dfs=())
    assert e.value.code == "empty_lattice"


def test_invalid_steps_per_period_is_typed_error() -> None:
    sched = generate_schedule(_annual_bond(coupon=0.05))
    with pytest.raises(BondError) as e:
        build_lattice(sched, _SETTLE, _CURVE, sigma=0.10, steps_per_period=0)
    assert e.value.code == "invalid_steps"


def test_cashflow_off_lattice_node_is_typed_error() -> None:
    # a real semiannual bond has unequal (182/184-day) periods, so its coupon
    # times do NOT land on a uniform grid -> typed error, never a silent
    # placement approximation.
    terms = BondTerms(
        issue_date=date(2021, 1, 15),
        maturity_date=date(2024, 1, 15),
        coupon_rate=0.05,
        frequency=Frequency.SEMIANNUAL,
        day_count=DayCount.ACT_365F,
    )
    sched = generate_schedule(terms)
    with pytest.raises(BondError) as e:
        build_lattice(sched, _SETTLE, _CURVE, sigma=0.10, steps_per_period=1)
    assert e.value.code == "cashflow_off_lattice_node"


def test_oas_unbracketable_price_is_typed_error() -> None:
    sched = generate_schedule(_annual_bond(coupon=0.05))
    inp = build_lattice(sched, _SETTLE, _CURVE, sigma=0.10, steps_per_period=4)
    with pytest.raises(BondError) as e:
        # a price far above the value at the lowest spread -> unbracketable.
        solve_oas(inp.lattice, 1e6, inp.cashflows_by_node, inp.call_by_node)
    assert e.value.code == "oas_out_of_bounds"


def test_oas_non_positive_price_is_typed_error() -> None:
    sched = generate_schedule(_annual_bond(coupon=0.05))
    inp = build_lattice(sched, _SETTLE, _CURVE, sigma=0.10, steps_per_period=4)
    with pytest.raises(BondError) as e:
        solve_oas(inp.lattice, 0.0, inp.cashflows_by_node, inp.call_by_node)
    assert e.value.code == "non_positive_price"
