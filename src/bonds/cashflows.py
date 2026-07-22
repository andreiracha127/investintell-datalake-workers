"""Pure, DB-free bond cash-flow motor (Increment 3, Phase 10 — research grade).

Deterministic schedule generation, day-count conventions, and accrued interest
derived only from a bond's terms plus an explicit settlement date. There is no
I/O, no database driver, and no wall-clock read — settlement is always supplied
by the caller — so every result is reproducible from its inputs.

Scope (Increment 3 Task 2):

* schedule generation from terms — bullet fixed-coupon, zero-coupon, and
  callable bonds (the call schedule is carried on the terms and validated, but a
  call affects yield-to-worst later, not the base to-maturity schedule);
* coupon frequencies annual / semiannual / quarterly;
* day-count conventions 30/360 US (bond basis), ACT/ACT ICMA (Rule 251),
  ACT/360, and ACT/365F;
* accrued interest with the coupon period resolved from the schedule.

Degenerate inputs raise a typed :class:`~src.bonds.errors.BondError` with a
stable ``code`` — never a NaN or a silent zero.

No value produced here reaches any production surface in this increment
(Increment 3 Global Constraint #3); the module is a validated pure library.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date
from enum import IntEnum, StrEnum

from .errors import BondError


class Frequency(IntEnum):
    """Coupon payments per year. The integer value is the frequency itself."""

    ANNUAL = 1
    SEMIANNUAL = 2
    QUARTERLY = 4


class DayCount(StrEnum):
    """Supported day-count conventions."""

    THIRTY_360_US = "30/360 US"
    ACT_ACT_ICMA = "ACT/ACT ICMA"
    ACT_360 = "ACT/360"
    ACT_365F = "ACT/365F"


class CashFlowKind(StrEnum):
    COUPON = "coupon"
    REDEMPTION = "redemption"


@dataclass(frozen=True)
class CallOption:
    """A single entry of an issuer call schedule (price per 100 of face)."""

    call_date: date
    price: float


@dataclass(frozen=True)
class BondTerms:
    """Immutable, self-validating description of a fixed-coupon or zero bond.

    ``coupon_rate`` is the annual rate as a decimal fraction (``0.05`` = 5%); a
    ``coupon_rate`` of ``0.0`` denotes a zero-coupon bond. ``call_schedule`` is
    carried and validated but does not alter the base schedule.
    """

    issue_date: date
    maturity_date: date
    coupon_rate: float
    frequency: Frequency
    day_count: DayCount
    face: float = 100.0
    call_schedule: tuple[CallOption, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.face, (int, float)) or self.face <= 0:
            raise BondError("non_positive_face", {"face": self.face})
        if self.maturity_date <= self.issue_date:
            raise BondError(
                "non_positive_tenor",
                {"issue_date": self.issue_date.isoformat(), "maturity_date": self.maturity_date.isoformat()},
            )
        if not isinstance(self.coupon_rate, (int, float)) or self.coupon_rate < 0:
            raise BondError("negative_coupon", {"coupon_rate": self.coupon_rate})
        if not isinstance(self.frequency, Frequency):
            raise BondError("invalid_frequency", {"frequency": self.frequency})
        if not isinstance(self.day_count, DayCount):
            raise BondError("invalid_day_count", {"day_count": self.day_count})
        object.__setattr__(self, "call_schedule", tuple(self.call_schedule))
        self._validate_calls()

    def _validate_calls(self) -> None:
        previous: date | None = None
        for call in self.call_schedule:
            if not isinstance(call, CallOption):
                raise BondError("invalid_call_option", {"call": call})
            if not isinstance(call.price, (int, float)) or call.price <= 0:
                raise BondError("non_positive_call_price", {"price": call.price})
            if not (self.issue_date < call.call_date <= self.maturity_date):
                raise BondError("call_outside_life", {"call_date": call.call_date.isoformat()})
            if previous is not None and call.call_date <= previous:
                raise BondError("call_dates_not_increasing", {"call_date": call.call_date.isoformat()})
            previous = call.call_date

    @property
    def is_zero_coupon(self) -> bool:
        return self.coupon_rate == 0.0


@dataclass(frozen=True)
class CashFlow:
    """A single dated payment on the base (to-maturity) schedule."""

    pay_date: date
    amount: float
    kind: CashFlowKind


@dataclass(frozen=True)
class Schedule:
    """The base to-maturity cash-flow schedule generated from terms."""

    terms: BondTerms
    coupon_dates: tuple[date, ...]
    cashflows: tuple[CashFlow, ...]

    def total(self) -> float:
        """Sum of undiscounted cash-flow amounts (coupons + redemption)."""
        return sum(cf.amount for cf in self.cashflows)


@dataclass(frozen=True)
class AccruedInterest:
    """Accrued interest at a settlement date, with the resolving period exposed."""

    amount: float
    day_count: DayCount
    previous_coupon_date: date
    next_coupon_date: date
    settlement_date: date
    days_accrued: int
    days_in_period: int
    year_fraction: float


# --------------------------------------------------------------------------- #
# Day counts
# --------------------------------------------------------------------------- #
def _thirty_360_us_days(start: date, end: date) -> int:
    """30/360 (bond basis), basic variant — no end-of-February refinement.

    Follows ISDA 2006 Definitions §4.16(f) "30/360" (a.k.a. Bond Basis):
    D1=31 -> 30; then D2=31 -> 30 only when D1 (post-adjustment) is 30.

    Note (honest scope): SIFMA's "Standard Securities Calculation Methods"
    30U/360 adds an end-of-February refinement (both dates on the last day of
    February collapse to day 30, and a February month-end D1 forces D2). That
    refinement is deliberately NOT applied here, so this variant is cited to
    ISDA §4.16(f) only, not to SIFMA. A licensed 30U/360 would be a distinct
    ``DayCount`` member rather than a mutation of this one.
    """
    d1, d2 = start.day, end.day
    if d1 == 31:
        d1 = 30
    if d2 == 31 and d1 == 30:
        d2 = 30
    return 360 * (end.year - start.year) + 30 * (end.month - start.month) + (d2 - d1)


def day_count_days(start: date, end: date, day_count: DayCount) -> int:
    """The day-count numerator between two dates for ``day_count``.

    ACT/ACT ICMA, ACT/360, and ACT/365F all use the actual number of calendar
    days; 30/360 US uses the adjusted 30/360 arithmetic.
    """
    if day_count == DayCount.THIRTY_360_US:
        return _thirty_360_us_days(start, end)
    if day_count in (DayCount.ACT_360, DayCount.ACT_365F, DayCount.ACT_ACT_ICMA):
        return (end - start).days
    raise BondError("unknown_day_count", {"day_count": day_count})


def year_fraction(start: date, end: date, day_count: DayCount) -> float:
    """Per-annum year fraction for the linear conventions.

    ACT/ACT ICMA has no well-defined two-date year fraction without the enclosing
    coupon period and frequency, so it is rejected with a typed error; use
    :func:`icma_period_fraction` (and :func:`accrued_interest`) instead.
    """
    if day_count == DayCount.THIRTY_360_US:
        return _thirty_360_us_days(start, end) / 360.0
    if day_count == DayCount.ACT_360:
        return (end - start).days / 360.0
    if day_count == DayCount.ACT_365F:
        return (end - start).days / 365.0
    if day_count == DayCount.ACT_ACT_ICMA:
        raise BondError("year_fraction_requires_coupon_period", {"day_count": day_count})
    raise BondError("unknown_day_count", {"day_count": day_count})


def icma_period_fraction(period_start: date, settlement: date, period_end: date) -> float:
    """ACT/ACT ICMA (Rule 251) fraction of a coupon period elapsed at settlement.

    ``actual_days(period_start, settlement) / actual_days(period_start, period_end)``.
    A whole regular period therefore contributes exactly ``1.0`` (i.e. ``1/frequency``
    of a year) irrespective of its actual calendar length.
    """
    span = (period_end - period_start).days
    if span <= 0:
        raise BondError(
            "non_positive_coupon_period",
            {"period_start": period_start.isoformat(), "period_end": period_end.isoformat()},
        )
    return (settlement - period_start).days / span


# --------------------------------------------------------------------------- #
# Schedule generation
# --------------------------------------------------------------------------- #
def _shift_months(anchor: date, months: int) -> date:
    """Shift ``anchor`` by ``months`` keeping its day-of-month, clamped to month end."""
    total = anchor.year * 12 + (anchor.month - 1) + months
    year, month0 = divmod(total, 12)
    month = month0 + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(anchor.day, last_day))


def coupon_dates(terms: BondTerms) -> tuple[date, ...]:
    """Coupon payment dates in ascending order, stepping back from maturity.

    Dates are anchored on the maturity day-of-month (clamped to shorter months)
    and include maturity as the final coupon. A zero-coupon bond has none.

    The retrograde grid must land exactly on ``issue_date`` (the dated date):
    the first regular coupon period is ``[issue_date, first_coupon]``. If the
    grid instead steps *past* ``issue_date`` — i.e. the issue/dated date sits
    inside a coupon period — the instrument has an irregular (short or long)
    FIRST coupon (a "front stub"). This motor does not model odd-first-coupon
    accrual, so rather than silently emit a FULL first coupon it raises a typed
    :class:`~src.bonds.errors.BondError` with code ``front_stub_unsupported``.
    """
    if terms.is_zero_coupon:
        return ()
    step = 12 // int(terms.frequency)
    dates: list[date] = []
    k = 0
    while True:
        candidate = _shift_months(terms.maturity_date, -step * k)
        if candidate <= terms.issue_date:
            grid_start = candidate
            break
        dates.append(candidate)
        k += 1
    if grid_start != terms.issue_date:
        # The (month-end-clamped) grid stepped past the dated date: front stub.
        raise BondError(
            "front_stub_unsupported",
            {
                "issue_date": terms.issue_date.isoformat(),
                "grid_period_start": grid_start.isoformat(),
                "first_coupon": dates[-1].isoformat() if dates else None,
            },
        )
    dates.reverse()
    return tuple(dates)


def generate_schedule(terms: BondTerms) -> Schedule:
    """The base to-maturity schedule (coupons + redemption), ignoring calls."""
    if terms.is_zero_coupon:
        redemption = CashFlow(terms.maturity_date, float(terms.face), CashFlowKind.REDEMPTION)
        return Schedule(terms, (), (redemption,))

    cdates = coupon_dates(terms)
    coupon_amount = terms.coupon_rate * terms.face / int(terms.frequency)
    flows: list[CashFlow] = [CashFlow(d, coupon_amount, CashFlowKind.COUPON) for d in cdates]
    flows.append(CashFlow(terms.maturity_date, float(terms.face), CashFlowKind.REDEMPTION))
    # Stable chronological order; a coupon precedes the redemption on the same date.
    flows.sort(key=lambda cf: (cf.pay_date, 0 if cf.kind == CashFlowKind.COUPON else 1))
    return Schedule(terms, cdates, tuple(flows))


# --------------------------------------------------------------------------- #
# Accrued interest
# --------------------------------------------------------------------------- #
def accrued_interest(terms: BondTerms, settlement: date) -> AccruedInterest:
    """Accrued interest at ``settlement`` (per the bond's day-count convention).

    Raises a typed error when settlement is before issue or at/after maturity.
    Zero at any coupon date and at issue; for a zero-coupon bond it is always 0.
    """
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

    if terms.is_zero_coupon:
        return AccruedInterest(
            amount=0.0,
            day_count=terms.day_count,
            previous_coupon_date=terms.issue_date,
            next_coupon_date=terms.maturity_date,
            days_accrued=(settlement - terms.issue_date).days,
            days_in_period=(terms.maturity_date - terms.issue_date).days,
            year_fraction=0.0,
            settlement_date=settlement,
        )

    cdates = coupon_dates(terms)
    if settlement < cdates[0]:
        previous = terms.issue_date
    else:
        previous = max(d for d in cdates if d <= settlement)
    next_coupon = min(d for d in cdates if d > settlement)

    coupon_per_period = terms.coupon_rate * terms.face / int(terms.frequency)

    if terms.day_count == DayCount.ACT_ACT_ICMA:
        fraction_of_period = icma_period_fraction(previous, settlement, next_coupon)
        amount = coupon_per_period * fraction_of_period
        per_annum = fraction_of_period / int(terms.frequency)
        days_accrued = (settlement - previous).days
        days_in_period = (next_coupon - previous).days
    else:
        partial = year_fraction(previous, settlement, terms.day_count)
        amount = terms.coupon_rate * terms.face * partial
        per_annum = partial
        days_accrued = day_count_days(previous, settlement, terms.day_count)
        days_in_period = day_count_days(previous, next_coupon, terms.day_count)

    return AccruedInterest(
        amount=amount,
        day_count=terms.day_count,
        previous_coupon_date=previous,
        next_coupon_date=next_coupon,
        days_accrued=days_accrued,
        days_in_period=days_in_period,
        year_fraction=per_annum,
        settlement_date=settlement,
    )
