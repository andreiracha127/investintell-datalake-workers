"""Validation of the pure cash-flow motor (`src.bonds.cashflows`).

Provenance labels (Increment 3 Global Constraint #4; brief guidance):

- ``convention_derived``  — the expected value is computed BY the documented
  convention rule (the day-count arithmetic or ICMA Rule 251), stated in the
  test, and asserted to a declared tolerance. The citation is the CONVENTION
  RULE, not a book.
- ``property``            — an internal-consistency invariant the motor must
  satisfy (schedule sum, accrued=0 at a coupon date, accrual monotonicity, the
  ICMA regular-period identity, the ACT/365F leap-year >1 identity).
- ``authoritative_published`` — a verbatim worked example from a named work /
  edition / example. NONE are asserted here: no fixed-income textbook worked
  accrued-interest vector could be reproduced from memory with the confidence
  Global Constraint #4 demands, so each candidate was deliberately DOWNGRADED to
  ``convention_derived`` (citing the rule) rather than over-claimed. See the
  Task-2 report's provenance table.

Every numeric vector below declares its tolerance inline via ``TOL_*``.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.bonds.cashflows import (
    AccruedInterest,
    BondTerms,
    CallOption,
    CashFlow,
    CashFlowKind,
    DayCount,
    Frequency,
    Schedule,
    accrued_interest,
    coupon_dates,
    day_count_days,
    generate_schedule,
    icma_period_fraction,
    year_fraction,
)
from src.bonds.errors import BondError

# Declared tolerances (per Global Constraint #4: tolerance stated per vector class).
TOL_DAYS = 0  # day counts are exact integers
TOL_FRACTION = 1e-12  # year fractions / period fractions: exact rational arithmetic
TOL_MONEY = 1e-9  # accrued-interest amounts, per 100 face


# --------------------------------------------------------------------------- #
# 30/360 US (Bond Basis) day counts
# Rule (convention_derived): ISDA 2006 Definitions §4.16(f) "30/360" (bond
# basis), basic variant:
#   D1 = day(start); D2 = day(end)
#   if D1 == 31: D1 = 30
#   if D2 == 31 and D1 == 30: D2 = 30
#   days = 360*(Y2-Y1) + 30*(M2-M1) + (D2-D1)
# (The optional end-of-February refinement of SIFMA "30U/360" is intentionally
#  NOT applied; the motor documents and cites this exact ISDA variant only.)
# --------------------------------------------------------------------------- #
THIRTY_360_VECTORS = [
    # (start, end, expected_days) — expected derived by the rule above.
    (date(2007, 1, 15), date(2007, 7, 15), 180),
    (date(2007, 1, 31), date(2007, 7, 31), 180),  # both days 31 -> 30/30
    (date(2007, 8, 31), date(2008, 2, 28), 178),  # D1 31->30, D2 28 unchanged
    (date(2007, 1, 30), date(2007, 3, 31), 60),  # D2 31->30 because D1==30
    (date(2007, 2, 28), date(2007, 8, 31), 183),  # D1 28 (not 30/31): D2 stays 31
    (date(2008, 2, 29), date(2008, 8, 31), 182),  # D1 29 (not 30/31): D2 stays 31
]


@pytest.mark.parametrize("start,end,expected", THIRTY_360_VECTORS)
def test_thirty_360_us_day_counts_convention_derived(start, end, expected) -> None:
    assert day_count_days(start, end, DayCount.THIRTY_360_US) == pytest.approx(expected, abs=TOL_DAYS)


@pytest.mark.parametrize("start,end,expected", THIRTY_360_VECTORS)
def test_thirty_360_us_year_fraction_convention_derived(start, end, expected) -> None:
    assert year_fraction(start, end, DayCount.THIRTY_360_US) == pytest.approx(expected / 360.0, abs=TOL_FRACTION)


# --------------------------------------------------------------------------- #
# ACT/360 — actual days / 360  (convention_derived: money-market ACT/360 rule)
# --------------------------------------------------------------------------- #
ACT_360_VECTORS = [
    (date(2007, 1, 1), date(2007, 4, 1), 90, 90 / 360.0),
    (date(2020, 1, 1), date(2020, 3, 1), 60, 60 / 360.0),  # leap Feb -> 60 actual days
    (date(2007, 1, 1), date(2008, 1, 1), 365, 365 / 360.0),  # >1: characteristic of ACT/360
]


@pytest.mark.parametrize("start,end,days,frac", ACT_360_VECTORS)
def test_act_360_convention_derived(start, end, days, frac) -> None:
    assert day_count_days(start, end, DayCount.ACT_360) == days
    assert year_fraction(start, end, DayCount.ACT_360) == pytest.approx(frac, abs=TOL_FRACTION)


# --------------------------------------------------------------------------- #
# ACT/365F (Fixed) — actual days / 365  (convention_derived)
# --------------------------------------------------------------------------- #
ACT_365F_VECTORS = [
    (date(2007, 1, 1), date(2008, 1, 1), 365, 1.0),  # non-leap year -> exactly 1.0
    (date(2007, 1, 1), date(2007, 7, 1), 181, 181 / 365.0),
    (date(2020, 1, 1), date(2021, 1, 1), 366, 366 / 365.0),  # leap span -> >1 (property below)
]


@pytest.mark.parametrize("start,end,days,frac", ACT_365F_VECTORS)
def test_act_365f_convention_derived(start, end, days, frac) -> None:
    assert day_count_days(start, end, DayCount.ACT_365F) == days
    assert year_fraction(start, end, DayCount.ACT_365F) == pytest.approx(frac, abs=TOL_FRACTION)


def test_act_365f_leap_span_exceeds_one_property() -> None:
    # property: a 366-day (leap) span measured ACT/365F exceeds 1.0 — a defining
    # difference from ACT/ACT, where a whole leap year is exactly 1.0.
    yf = year_fraction(date(2020, 1, 1), date(2021, 1, 1), DayCount.ACT_365F)
    assert yf > 1.0


# --------------------------------------------------------------------------- #
# ACT/ACT ICMA (Rule 251) — accrued fraction over a coupon period.
# Rule (convention_derived): for a settlement inside a coupon period bounded by
# consecutive coupon dates [P0, P1], the accrued fraction of that period is
#   f = actual_days(P0, settle) / actual_days(P0, P1)
# and the per-annum year fraction is f / frequency (so a WHOLE regular period is
# exactly 1/frequency regardless of its actual day count — the ICMA identity).
# --------------------------------------------------------------------------- #
def test_icma_regular_period_identity_property() -> None:
    # property: a whole semiannual period spanning the 2020 leap day is exactly
    # 0.5 of a year under ICMA — NOT 182/365 or 183/365.
    p0, p1 = date(2019, 11, 15), date(2020, 5, 15)
    frac = icma_period_fraction(p0, p1, p1)
    assert frac == pytest.approx(1.0, abs=TOL_FRACTION)


def test_icma_partial_period_fraction_convention_derived() -> None:
    # convention_derived: P0=2019-11-15, P1=2020-05-15, settle=2020-02-15.
    #   actual(P0, settle) = 92 ; actual(P0, P1) = 182 ; f = 92/182.
    p0, p1, settle = date(2019, 11, 15), date(2020, 5, 15), date(2020, 2, 15)
    assert icma_period_fraction(p0, settle, p1) == pytest.approx(92 / 182, abs=TOL_FRACTION)


def test_year_fraction_icma_requires_period_context() -> None:
    with pytest.raises(BondError) as excinfo:
        year_fraction(date(2019, 11, 15), date(2020, 2, 15), DayCount.ACT_ACT_ICMA)
    assert excinfo.value.code == "year_fraction_requires_coupon_period"


# --------------------------------------------------------------------------- #
# Coupon schedule generation
# --------------------------------------------------------------------------- #
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


def test_semiannual_coupon_dates_step_back_from_maturity() -> None:
    dates = coupon_dates(_bullet(maturity=date(2022, 1, 15), issue=date(2020, 1, 15)))
    assert dates == (
        date(2020, 7, 15),
        date(2021, 1, 15),
        date(2021, 7, 15),
        date(2022, 1, 15),
    )


def test_annual_and_quarterly_frequencies() -> None:
    annual = coupon_dates(_bullet(frequency=Frequency.ANNUAL, issue=date(2020, 6, 30), maturity=date(2023, 6, 30)))
    assert annual == (date(2021, 6, 30), date(2022, 6, 30), date(2023, 6, 30))
    quarterly = coupon_dates(_bullet(frequency=Frequency.QUARTERLY, issue=date(2020, 1, 31), maturity=date(2021, 1, 31)))
    assert quarterly == (
        date(2020, 4, 30),  # April has no 31st -> clamped to month end
        date(2020, 7, 31),
        date(2020, 10, 31),
        date(2021, 1, 31),
    )


def test_bullet_schedule_cashflows_and_sum_property() -> None:
    terms = _bullet(coupon_rate=0.06, frequency=Frequency.SEMIANNUAL, maturity=date(2022, 1, 15))
    sched = generate_schedule(terms)
    assert isinstance(sched, Schedule)
    coupons = [cf for cf in sched.cashflows if cf.kind == CashFlowKind.COUPON]
    redemptions = [cf for cf in sched.cashflows if cf.kind == CashFlowKind.REDEMPTION]
    # 4 semiannual coupons of 3.0 each (6% / 2 * 100) + one redemption of 100.
    assert len(coupons) == 4
    assert all(cf.amount == pytest.approx(3.0, abs=TOL_MONEY) for cf in coupons)
    assert len(redemptions) == 1
    assert redemptions[0].amount == pytest.approx(100.0, abs=TOL_MONEY)
    assert redemptions[0].pay_date == date(2022, 1, 15)
    # property: total undiscounted flow = coupons + face.
    assert sched.total() == pytest.approx(4 * 3.0 + 100.0, abs=TOL_MONEY)
    # cashflows are chronologically non-decreasing.
    assert [cf.pay_date for cf in sched.cashflows] == sorted(cf.pay_date for cf in sched.cashflows)


def test_zero_coupon_schedule_is_single_redemption() -> None:
    terms = _bullet(coupon_rate=0.0, maturity=date(2030, 1, 15))
    sched = generate_schedule(terms)
    assert [cf.kind for cf in sched.cashflows] == [CashFlowKind.REDEMPTION]
    assert sched.cashflows[0].amount == pytest.approx(100.0, abs=TOL_MONEY)
    assert sched.cashflows[0].pay_date == date(2030, 1, 15)
    assert coupon_dates(terms) == ()


def test_callable_terms_do_not_change_base_schedule() -> None:
    # A call schedule affects YTW later (Task 3), not the base to-maturity schedule.
    calls = (CallOption(date(2023, 1, 15), 102.0), CallOption(date(2024, 1, 15), 101.0))
    base = generate_schedule(_bullet(coupon_rate=0.05, maturity=date(2025, 1, 15)))
    called = generate_schedule(_bullet(coupon_rate=0.05, maturity=date(2025, 1, 15), call_schedule=calls))
    assert base.cashflows == called.cashflows


def test_front_stub_issue_off_grid_is_typed_error() -> None:
    # An issue/dated date that does NOT fall on the coupon grid stepped back from
    # maturity implies an irregular (short/long) FIRST coupon period. The motor
    # does not support odd-first-coupon accrual, so instead of silently emitting a
    # FULL first coupon it raises a typed guard (regression: Task-2 review probe).
    # Grid from 2025-01-15 semiannual lands on 2020-07-15, 2020-01-15, ...; an
    # issue of 2020-02-10 sits inside a period -> front stub.
    off_grid = _bullet(coupon_rate=0.05, issue=date(2020, 2, 10), maturity=date(2025, 1, 15))
    with pytest.raises(BondError) as excinfo:
        coupon_dates(off_grid)
    assert excinfo.value.code == "front_stub_unsupported"
    with pytest.raises(BondError) as excinfo2:
        generate_schedule(off_grid)
    assert excinfo2.value.code == "front_stub_unsupported"


def test_on_grid_issue_is_accepted() -> None:
    # The mirror GREEN case: an issue that DOES land on the retrograde grid
    # (2020-01-15) yields a regular first period and a full set of coupons.
    on_grid = _bullet(coupon_rate=0.05, issue=date(2020, 1, 15), maturity=date(2025, 1, 15))
    assert coupon_dates(on_grid)[0] == date(2020, 7, 15)
    assert len(coupon_dates(on_grid)) == 10


# --------------------------------------------------------------------------- #
# Accrued interest
# --------------------------------------------------------------------------- #
def test_accrued_zero_on_coupon_date_property() -> None:
    terms = _bullet(coupon_rate=0.08, frequency=Frequency.SEMIANNUAL, maturity=date(2025, 1, 15))
    ai = accrued_interest(terms, date(2022, 7, 15))  # exactly a coupon date
    assert isinstance(ai, AccruedInterest)
    assert ai.amount == pytest.approx(0.0, abs=TOL_MONEY)
    assert ai.days_accrued == 0


def test_accrued_zero_at_issue_property() -> None:
    terms = _bullet(coupon_rate=0.08, issue=date(2020, 1, 15), maturity=date(2025, 1, 15))
    ai = accrued_interest(terms, date(2020, 1, 15))
    assert ai.amount == pytest.approx(0.0, abs=TOL_MONEY)


def test_accrued_monotonic_within_period_property() -> None:
    terms = _bullet(coupon_rate=0.08, day_count=DayCount.ACT_ACT_ICMA, maturity=date(2025, 1, 15))
    a1 = accrued_interest(terms, date(2022, 2, 15)).amount
    a2 = accrued_interest(terms, date(2022, 4, 15)).amount
    a3 = accrued_interest(terms, date(2022, 6, 15)).amount
    assert 0.0 < a1 < a2 < a3


def test_accrued_30_360_half_period_convention_derived() -> None:
    # convention_derived (30/360 US): prev coupon 2022-01-15, settle 2022-04-15.
    #   30/360 days(01-15, 04-15) = 90 ; days(01-15, 07-15) = 180 ; f = 90/180 = 1/2.
    #   coupon_per_period = 8% * 100 / 2 = 4.0 ; accrued = 4.0 * 1/2 = 2.0.
    terms = _bullet(coupon_rate=0.08, frequency=Frequency.SEMIANNUAL, day_count=DayCount.THIRTY_360_US)
    ai = accrued_interest(terms, date(2022, 4, 15))
    assert ai.previous_coupon_date == date(2022, 1, 15)
    assert ai.next_coupon_date == date(2022, 7, 15)
    assert ai.amount == pytest.approx(2.0, abs=TOL_MONEY)


def test_accrued_icma_partial_period_convention_derived() -> None:
    # convention_derived (ICMA Rule 251): coupon dates 2019-11-15 / 2020-05-15,
    # settle 2020-02-15. f = 92/182. coupon_per_period = 10% * 100 / 2 = 5.0.
    # accrued = 5.0 * 92/182 = 2.527472527...
    terms = BondTerms(
        issue_date=date(2019, 5, 15),
        maturity_date=date(2024, 5, 15),
        coupon_rate=0.10,
        frequency=Frequency.SEMIANNUAL,
        day_count=DayCount.ACT_ACT_ICMA,
    )
    ai = accrued_interest(terms, date(2020, 2, 15))
    assert ai.previous_coupon_date == date(2019, 11, 15)
    assert ai.next_coupon_date == date(2020, 5, 15)
    assert ai.days_accrued == 92
    assert ai.days_in_period == 182
    assert ai.amount == pytest.approx(5.0 * 92 / 182, abs=TOL_MONEY)


def test_accrued_act_360_convention_derived() -> None:
    # convention_derived (ACT/360 money-market accrual): semiannual 6% coupon,
    # face 100, prev coupon 2022-01-15, settle 2022-02-14 -> actual 30 days.
    # ACT/360 accrual is frequency-independent (rate * face * days/360):
    #   accrued = coupon_rate * face * 30/360 = 0.06 * 100 * 30/360 = 0.5.
    terms = _bullet(coupon_rate=0.06, frequency=Frequency.SEMIANNUAL, day_count=DayCount.ACT_360)
    ai = accrued_interest(terms, date(2022, 2, 14))
    assert ai.days_accrued == 30
    assert ai.amount == pytest.approx(0.06 * 100 * 30 / 360.0, abs=TOL_MONEY)


def test_zero_coupon_accrued_is_zero() -> None:
    terms = _bullet(coupon_rate=0.0, maturity=date(2030, 1, 15))
    ai = accrued_interest(terms, date(2025, 6, 1))
    assert ai.amount == pytest.approx(0.0, abs=TOL_MONEY)


# --------------------------------------------------------------------------- #
# Typed degenerate-input errors (never NaN / silent)
# --------------------------------------------------------------------------- #
def test_maturity_not_after_issue_is_typed_error() -> None:
    with pytest.raises(BondError) as excinfo:
        BondTerms(
            issue_date=date(2025, 1, 15),
            maturity_date=date(2025, 1, 15),
            coupon_rate=0.05,
            frequency=Frequency.SEMIANNUAL,
            day_count=DayCount.THIRTY_360_US,
        )
    assert excinfo.value.code == "non_positive_tenor"


def test_negative_coupon_is_typed_error() -> None:
    with pytest.raises(BondError) as excinfo:
        _bullet(coupon_rate=-0.01)
    assert excinfo.value.code == "negative_coupon"


def test_non_positive_face_is_typed_error() -> None:
    with pytest.raises(BondError) as excinfo:
        _bullet(face=0.0)
    assert excinfo.value.code == "non_positive_face"


def test_settlement_after_maturity_is_typed_error() -> None:
    terms = _bullet(maturity=date(2025, 1, 15))
    with pytest.raises(BondError) as excinfo:
        accrued_interest(terms, date(2025, 6, 1))
    assert excinfo.value.code == "settlement_after_maturity"


def test_settlement_before_issue_is_typed_error() -> None:
    terms = _bullet(issue=date(2020, 1, 15))
    with pytest.raises(BondError) as excinfo:
        accrued_interest(terms, date(2019, 12, 1))
    assert excinfo.value.code == "settlement_before_issue"


def test_unknown_day_count_is_typed_error() -> None:
    with pytest.raises(BondError) as excinfo:
        day_count_days(date(2020, 1, 1), date(2020, 7, 1), "ACT/999")  # type: ignore[arg-type]
    assert excinfo.value.code == "unknown_day_count"


def test_invalid_call_schedule_is_typed_error() -> None:
    # call date must fall within (issue, maturity]; price must be positive.
    with pytest.raises(BondError) as excinfo:
        _bullet(maturity=date(2025, 1, 15), call_schedule=(CallOption(date(2030, 1, 1), 100.0),))
    assert excinfo.value.code == "call_outside_life"

    with pytest.raises(BondError) as excinfo2:
        _bullet(call_schedule=(CallOption(date(2023, 1, 15), 0.0),))
    assert excinfo2.value.code == "non_positive_call_price"


def test_cashflow_and_schedule_are_frozen() -> None:
    cf = CashFlow(pay_date=date(2022, 1, 15), amount=3.0, kind=CashFlowKind.COUPON)
    with pytest.raises((AttributeError, TypeError)):
        cf.amount = 4.0  # type: ignore[misc]
