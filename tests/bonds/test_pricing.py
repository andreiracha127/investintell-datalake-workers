"""Validation of the pure pricing / yield motor (`src.bonds.pricing`).

Provenance labels (Increment 3 Global Constraint #4; brief guidance) — the
reviewer WILL recompute every number, so each vector states how its expected
value was obtained and to what tolerance:

- ``authoritative_published`` — a canonical published worked example reproduced
  faithfully, with the work cited. Exactly ONE is asserted here: the textbook
  10% / 5-year / semiannual bond priced to yield 11% ⇒ 96.23 (Fabozzi, *Bond
  Markets, Analysis and Strategies* / *Fixed Income Mathematics* — the single
  most reproduced bond-pricing worked example). The published figure is printed
  rounded to 96.23; the tolerance is set accordingly and the same example is
  ALSO cross-checked against the closed-form annuity price (so it is
  independently reproducible, not merely transcribed). Its inverse (price 96.23
  ⇒ yield 11%) is the authoritative YTM vector the DoD requires.
- ``convention_derived`` — expected value computed BY a documented pricing rule
  (annuity closed form, zero-coupon closed form, the modified-duration
  definition) stated inline and asserted to a declared tolerance. The citation
  is the FORMULA, not a book.
- ``property`` — an internal-consistency invariant the motor must satisfy
  (par-at-par identity, price↔yield round trip, analytic vs bumped duration,
  positive convexity, YTW ≤ YTM for a premium callable, the
  carry+rolldown P&L decomposition identity, z-spread ≈ 0 on a self-consistent
  flat curve).

Tolerances are declared inline via ``TOL_*``.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.bonds.cashflows import (
    BondTerms,
    CallOption,
    CashFlowKind,
    DayCount,
    Frequency,
    generate_schedule,
)
from src.bonds.errors import BondError
from src.bonds.pricing import (
    SpotCurve,
    YieldToWorst,
    carry,
    clean_price,
    convexity,
    curve_price,
    current_yield,
    dirty_price,
    effective_duration,
    modified_duration,
    price_quote,
    rolldown,
    yield_to_call,
    yield_to_maturity,
    yield_to_worst,
    z_spread,
)

# Declared tolerances (per Global Constraint #4).
TOL_PRICE = 1e-9  # closed-form / round-trip price agreement, per 100 face
TOL_PUBLISHED = 5e-3  # vs a figure printed rounded to 2 decimals (96.23)
TOL_YIELD = 1e-10  # solver-recovered yields vs closed form
TOL_DUR = 1e-4  # analytic vs bumped duration (bump-truncation limited)
TOL_MONEY = 1e-9


def _bullet(
    coupon_rate: float = 0.05,
    frequency: Frequency = Frequency.SEMIANNUAL,
    day_count: DayCount = DayCount.THIRTY_360_US,
    issue: date = date(2020, 1, 15),
    maturity: date = date(2025, 1, 15),
    face: float = 100.0,
    call_schedule: tuple[CallOption, ...] = (),
) -> BondTerms:
    return BondTerms(
        issue_date=issue,
        maturity_date=maturity,
        coupon_rate=coupon_rate,
        frequency=frequency,
        day_count=day_count,
        face=face,
        call_schedule=call_schedule,
    )


# --------------------------------------------------------------------------- #
# AUTHORITATIVE — Fabozzi canonical: 10% coupon, 5y, semiannual, yield 11%.
# Published price 96.23 (printed to 2 dp). Independently reproduced by the
# closed-form annuity: 5·a(0.055,10) + 100·1.055^-10 = 96.2311870857...
# --------------------------------------------------------------------------- #
def _fabozzi_bond() -> BondTerms:
    # settle on the dated date -> accrued 0, ten whole semiannual periods remain.
    return _bullet(coupon_rate=0.10, frequency=Frequency.SEMIANNUAL, issue=date(2020, 1, 15), maturity=date(2025, 1, 15))


def test_fabozzi_price_authoritative_published() -> None:
    terms = _fabozzi_bond()
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    dirty = dirty_price(sched, settle, 0.11)
    clean = clean_price(sched, settle, 0.11)
    # published, rounded: 96.23
    assert dirty == pytest.approx(96.23, abs=TOL_PUBLISHED)
    # accrued is 0 on the dated date -> clean == dirty
    assert clean == pytest.approx(dirty, abs=TOL_PRICE)
    # independent closed-form cross-check (convention_derived) to full precision
    i = 0.055
    closed = sum(5.0 / (1 + i) ** j for j in range(1, 11)) + 100.0 / (1 + i) ** 10
    assert dirty == pytest.approx(closed, abs=TOL_PRICE)


def test_fabozzi_ytm_authoritative_published() -> None:
    # inverse of the published example: price 96.23 (clean) ⇒ yield 11%.
    terms = _fabozzi_bond()
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    # use the full-precision clean price so the recovered yield is exact.
    p = clean_price(sched, settle, 0.11)
    y = yield_to_maturity(sched, settle, p)
    assert y == pytest.approx(0.11, abs=TOL_YIELD)
    # and the published rounded price maps to ~11% within published tolerance.
    y_rounded = yield_to_maturity(sched, settle, 96.23)
    assert y_rounded == pytest.approx(0.11, abs=1e-4)


def test_fabozzi_modified_duration_convention_derived() -> None:
    # convention_derived: modified duration = Macaulay/(1+i).  Macaulay (periods)
    # = Σ j·PV_j / PV; Macaulay(years) = /m; ModDur = /(1+i). Recomputed here.
    terms = _fabozzi_bond()
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    i = 0.055
    pv = sum(5.0 / (1 + i) ** j for j in range(1, 11)) + 100.0 / (1 + i) ** 10
    wsum = sum(j * 5.0 / (1 + i) ** j for j in range(1, 11)) + 10 * 100.0 / (1 + i) ** 10
    expected_moddur = (wsum / (2 * pv)) / (1 + i)  # ≈ 3.8224880
    assert modified_duration(sched, settle, 0.11) == pytest.approx(expected_moddur, abs=1e-9)


# --------------------------------------------------------------------------- #
# PAR IDENTITY (property): price at par ⇒ yield == coupon (exact).
# --------------------------------------------------------------------------- #
def test_par_bond_prices_to_100_when_yield_equals_coupon_property() -> None:
    terms = _bullet(coupon_rate=0.05, frequency=Frequency.SEMIANNUAL)
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)  # dated date, accrued 0
    assert dirty_price(sched, settle, 0.05) == pytest.approx(100.0, abs=TOL_PRICE)
    assert clean_price(sched, settle, 0.05) == pytest.approx(100.0, abs=TOL_PRICE)


def test_par_bond_ytm_of_100_equals_coupon_property() -> None:
    terms = _bullet(coupon_rate=0.05, frequency=Frequency.SEMIANNUAL)
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    assert yield_to_maturity(sched, settle, 100.0) == pytest.approx(0.05, abs=TOL_YIELD)


def test_premium_discount_monotonicity_property() -> None:
    terms = _bullet(coupon_rate=0.05, frequency=Frequency.SEMIANNUAL)
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    # yield below coupon ⇒ premium (>100); above ⇒ discount (<100).
    assert dirty_price(sched, settle, 0.03) > 100.0
    assert dirty_price(sched, settle, 0.07) < 100.0
    assert yield_to_maturity(sched, settle, 105.0) < 0.05
    assert yield_to_maturity(sched, settle, 95.0) > 0.05


# --------------------------------------------------------------------------- #
# ZERO-COUPON closed form (convention_derived).
#   P = F / (1 + y/m)^(m·τ);  YTM = m·((F/P)^(1/(m·τ)) − 1).
#   30/360, semiannual, 3y from dated date ⇒ m·τ = 6 (exact integer periods).
# --------------------------------------------------------------------------- #
def test_zero_coupon_price_and_ytm_closed_form_convention_derived() -> None:
    terms = _bullet(coupon_rate=0.0, frequency=Frequency.SEMIANNUAL, issue=date(2020, 1, 15), maturity=date(2023, 1, 15))
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    expected = 100.0 / (1 + 0.06 / 2) ** 6  # 83.748425668...
    assert dirty_price(sched, settle, 0.06) == pytest.approx(expected, abs=TOL_PRICE)
    # clean == dirty for a zero (no accrued)
    assert clean_price(sched, settle, 0.06) == pytest.approx(expected, abs=TOL_PRICE)
    # closed-form YTM recovery
    y = yield_to_maturity(sched, settle, expected)
    assert y == pytest.approx(0.06, abs=TOL_YIELD)


def test_zero_coupon_icma_year_fraction_is_typed_error() -> None:
    # ACT/ACT ICMA has no two-date year fraction; pricing a zero with it raises
    # the same typed error surfaced by year_fraction (never a NaN).
    terms = _bullet(coupon_rate=0.0, day_count=DayCount.ACT_ACT_ICMA, maturity=date(2030, 1, 15))
    sched = generate_schedule(terms)
    with pytest.raises(BondError) as excinfo:
        dirty_price(sched, date(2021, 1, 15), 0.05)
    assert excinfo.value.code == "year_fraction_requires_coupon_period"


# --------------------------------------------------------------------------- #
# ROUND TRIP (property): price(ytm(P)) == P across coupons / prices / settlements.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("coupon", [0.02, 0.05, 0.08])
@pytest.mark.parametrize("target", [80.0, 95.0, 100.0, 108.0, 130.0])
@pytest.mark.parametrize("settle", [date(2020, 1, 15), date(2020, 4, 10), date(2021, 6, 1)])
def test_price_yield_round_trip_property(coupon, target, settle) -> None:
    terms = _bullet(coupon_rate=coupon, frequency=Frequency.SEMIANNUAL, day_count=DayCount.ACT_ACT_ICMA)
    sched = generate_schedule(terms)
    y = yield_to_maturity(sched, settle, target)  # target is a CLEAN price
    assert clean_price(sched, settle, y) == pytest.approx(target, abs=1e-8)


# --------------------------------------------------------------------------- #
# CURRENT YIELD (convention_derived): annual coupon / clean price.
# --------------------------------------------------------------------------- #
def test_current_yield_convention_derived() -> None:
    terms = _bullet(coupon_rate=0.06, frequency=Frequency.SEMIANNUAL)
    sched = generate_schedule(terms)
    assert current_yield(sched, 120.0) == pytest.approx(6.0 / 120.0, abs=1e-12)
    assert current_yield(sched, 80.0) == pytest.approx(6.0 / 80.0, abs=1e-12)


# --------------------------------------------------------------------------- #
# DURATION / CONVEXITY (property): analytic modified ≈ bumped effective;
# convexity positive.
# --------------------------------------------------------------------------- #
def test_modified_matches_effective_duration_property() -> None:
    terms = _bullet(coupon_rate=0.05, frequency=Frequency.SEMIANNUAL)
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    md = modified_duration(sched, settle, 0.05)
    ed = effective_duration(sched, settle, 0.05)
    assert md == pytest.approx(ed, abs=TOL_DUR)
    assert md > 0.0


def test_convexity_is_positive_property() -> None:
    terms = _bullet(coupon_rate=0.05, frequency=Frequency.SEMIANNUAL)
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    assert convexity(sched, settle, 0.05) > 0.0


# --------------------------------------------------------------------------- #
# YIELD TO WORST (property): for a premium callable, YTW ≤ YTM and the worst
# scenario is a call.
# --------------------------------------------------------------------------- #
def test_ytw_le_ytm_for_premium_callable_property() -> None:
    calls = (CallOption(date(2023, 1, 15), 100.0), CallOption(date(2024, 1, 15), 100.0))
    terms = _bullet(coupon_rate=0.08, frequency=Frequency.SEMIANNUAL, maturity=date(2025, 1, 15), call_schedule=calls)
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    price = 108.0  # premium
    res = yield_to_worst(sched, settle, price)
    assert isinstance(res, YieldToWorst)
    ytm = yield_to_maturity(sched, settle, price)
    assert res.to_maturity == pytest.approx(ytm, abs=TOL_YIELD)
    assert res.ytw <= res.to_maturity
    assert res.worst_kind == "call"
    # yield_to_call for the first call equals the corresponding scenario yield
    ytc0 = yield_to_call(sched, settle, price, calls[0])
    assert any(d == calls[0].call_date and y == pytest.approx(ytc0, abs=TOL_YIELD) for d, y in res.calls)


def test_call_not_on_coupon_grid_is_typed_error() -> None:
    calls = (CallOption(date(2023, 3, 3), 101.0),)  # not a coupon date
    terms = _bullet(coupon_rate=0.05, maturity=date(2025, 1, 15), call_schedule=calls)
    sched = generate_schedule(terms)
    with pytest.raises(BondError) as excinfo:
        yield_to_call(sched, date(2020, 1, 15), 100.0, calls[0])
    assert excinfo.value.code == "call_not_on_coupon_grid"


# --------------------------------------------------------------------------- #
# Z-SPREAD (property + convention_derived).
# --------------------------------------------------------------------------- #
def test_zspread_zero_on_self_consistent_flat_curve_property() -> None:
    # a flat spot curve at rate r prices a bond; its z-spread to that price is 0.
    terms = _bullet(coupon_rate=0.05, frequency=Frequency.SEMIANNUAL)
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    curve = SpotCurve(nodes=((1.0, 0.04), (5.0, 0.04)))  # flat 4%
    p_dirty = curve_price(sched, settle, curve)
    p_clean = p_dirty  # accrued 0 at dated date
    s = z_spread(sched, settle, p_clean, curve)
    assert s == pytest.approx(0.0, abs=1e-9)


def test_zspread_positive_when_price_below_curve_property() -> None:
    terms = _bullet(coupon_rate=0.05, frequency=Frequency.SEMIANNUAL)
    sched = generate_schedule(terms)
    settle = date(2020, 1, 15)
    curve = SpotCurve(nodes=((1.0, 0.04), (5.0, 0.04)))
    fair = curve_price(sched, settle, curve)
    cheaper = fair - 3.0  # trading below the curve ⇒ positive spread
    assert z_spread(sched, settle, cheaper, curve) > 0.0


def test_zspread_reprices_to_target_property() -> None:
    terms = _bullet(coupon_rate=0.06, frequency=Frequency.SEMIANNUAL)
    sched = generate_schedule(terms)
    settle = date(2020, 4, 10)
    curve = SpotCurve(nodes=((0.5, 0.03), (2.0, 0.035), (5.0, 0.045)))
    target_clean = 101.5
    s = z_spread(sched, settle, target_clean, curve)
    # discounting on curve+s must reproduce the dirty target
    from src.bonds.pricing import _accrued_amount  # internal helper, tested via reprice

    dirty_target = target_clean + _accrued_amount(terms, settle)
    assert curve_price(sched, settle, curve, s) == pytest.approx(dirty_target, abs=1e-7)


# --------------------------------------------------------------------------- #
# CARRY / ROLLDOWN.
# --------------------------------------------------------------------------- #
def test_carry_is_coupon_income_over_horizon_convention_derived() -> None:
    # convention_derived: 8% semiannual, dated 2020-01-15. Over [2020-01-15,
    # 2020-07-15] exactly one 4.0 coupon is received and accrued nets to 0
    # (both endpoints on coupon dates) ⇒ carry = 4.0.
    terms = _bullet(coupon_rate=0.08, frequency=Frequency.SEMIANNUAL, maturity=date(2025, 1, 15))
    sched = generate_schedule(terms)
    c = carry(sched, date(2020, 1, 15), date(2020, 7, 15))
    assert c == pytest.approx(4.0, abs=TOL_MONEY)


def test_carry_within_period_is_accrual_only_property() -> None:
    # horizon inside a period (no coupon crossed) ⇒ carry = accrued gained > 0.
    terms = _bullet(coupon_rate=0.08, frequency=Frequency.SEMIANNUAL, day_count=DayCount.ACT_ACT_ICMA)
    sched = generate_schedule(terms)
    c = carry(sched, date(2020, 1, 15), date(2020, 4, 15))
    assert 0.0 < c < 4.0


def test_rolldown_plus_carry_equals_total_pnl_identity_property() -> None:
    # exact decomposition identity: rolldown(clean) + carry ==
    #   curve_dirty(horizon) − curve_dirty(settlement) + coupons_in_window.
    terms = _bullet(coupon_rate=0.06, frequency=Frequency.SEMIANNUAL, maturity=date(2025, 1, 15))
    sched = generate_schedule(terms)
    settle, horizon = date(2020, 1, 15), date(2020, 10, 15)
    curve = SpotCurve(nodes=((0.5, 0.03), (2.0, 0.035), (5.0, 0.045)))
    rd = rolldown(sched, settle, horizon, curve)
    cy = carry(sched, settle, horizon)
    coupons = sum(
        cf.amount for cf in sched.cashflows if cf.kind == CashFlowKind.COUPON and settle < cf.pay_date <= horizon
    )
    lhs = rd + cy
    rhs = curve_price(sched, horizon, curve) - curve_price(sched, settle, curve) + coupons
    assert lhs == pytest.approx(rhs, abs=1e-9)


def test_rolldown_zero_pulls_to_par_on_flat_curve_convention_derived() -> None:
    # convention_derived: a zero on a flat curve rolls UP toward par as time
    # passes. P(asof) = F/(1+r/m)^(m·τ). rolldown = P(horizon) − P(settle) > 0.
    terms = _bullet(coupon_rate=0.0, frequency=Frequency.SEMIANNUAL, issue=date(2020, 1, 15), maturity=date(2025, 1, 15))
    sched = generate_schedule(terms)
    settle, horizon = date(2020, 1, 15), date(2021, 1, 15)
    curve = SpotCurve(nodes=((1.0, 0.05), (5.0, 0.05)))  # flat 5%
    rd = rolldown(sched, settle, horizon, curve)
    p0 = curve_price(sched, settle, curve)
    p1 = curve_price(sched, horizon, curve)
    assert rd == pytest.approx(p1 - p0, abs=1e-12)  # zero ⇒ clean == dirty
    assert rd > 0.0


# --------------------------------------------------------------------------- #
# PRICE QUOTE bundle: dirty = clean + accrued.
# --------------------------------------------------------------------------- #
def test_price_quote_dirty_equals_clean_plus_accrued_property() -> None:
    terms = _bullet(coupon_rate=0.08, frequency=Frequency.SEMIANNUAL, day_count=DayCount.THIRTY_360_US)
    sched = generate_schedule(terms)
    settle = date(2020, 4, 15)  # mid-period ⇒ nonzero accrued
    q = price_quote(sched, settle, 0.05)
    assert q.dirty == pytest.approx(q.clean + q.accrued, abs=TOL_PRICE)
    assert q.accrued > 0.0


# --------------------------------------------------------------------------- #
# Typed degenerate inputs (never NaN / silent).
# --------------------------------------------------------------------------- #
def test_non_positive_price_is_typed_error() -> None:
    terms = _bullet()
    sched = generate_schedule(terms)
    with pytest.raises(BondError) as excinfo:
        yield_to_maturity(sched, date(2020, 1, 15), 0.0)
    assert excinfo.value.code == "non_positive_price"
    with pytest.raises(BondError) as excinfo2:
        current_yield(sched, -5.0)
    assert excinfo2.value.code == "non_positive_price"


def test_settlement_after_maturity_is_typed_error() -> None:
    terms = _bullet(maturity=date(2025, 1, 15))
    sched = generate_schedule(terms)
    with pytest.raises(BondError) as excinfo:
        dirty_price(sched, date(2025, 6, 1), 0.05)
    assert excinfo.value.code == "settlement_after_maturity"


def test_empty_spot_curve_is_typed_error() -> None:
    with pytest.raises(BondError) as excinfo:
        SpotCurve(nodes=())
    assert excinfo.value.code == "empty_spot_curve"


def test_horizon_out_of_range_is_typed_error() -> None:
    terms = _bullet(maturity=date(2025, 1, 15))
    sched = generate_schedule(terms)
    with pytest.raises(BondError) as excinfo:
        carry(sched, date(2020, 1, 15), date(2019, 12, 1))  # horizon before settle
    assert excinfo.value.code == "horizon_out_of_range"


def test_yield_out_of_bounds_is_typed_error() -> None:
    # a price far below the smallest PV reachable inside the yield bracket
    # (≈0.25 at y = 20·m for this bond) implies a yield beyond MAX_PERIODIC_RATE
    # ⇒ typed out-of-bounds, never a silent NaN.
    terms = _bullet(coupon_rate=0.05, maturity=date(2025, 1, 15))
    sched = generate_schedule(terms)
    with pytest.raises(BondError) as excinfo:
        yield_to_maturity(sched, date(2020, 1, 15), 1e-6)
    assert excinfo.value.code == "ytm_out_of_bounds"
