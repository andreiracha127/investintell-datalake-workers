"""Pure, DB-free Wave-1 metric engine runner (bonds activation Wave 1, Task 3).

Glue between the PUBLISHED security terms (``sec_current_bond_security_v1``),
the eligible latest CLEAN price in percent of par (``bond_price_eligibility_v1``
over ``bond_price_observation``), and the VALIDATED pure engines
(:mod:`src.bonds.cashflows`, :mod:`src.bonds.pricing`).  For one security it
produces exactly one typed row per Wave-1 metric — the rows the ``bond_metrics``
worker persists as the ``bond_metric_v1`` product.

Honesty rules (program Constraint 4 — no silent NaN, no synthetic zero):

* a metric value exists ONLY with ``status='available'`` and is always finite;
* every degenerate outcome is a TYPED status: the Phase-10 gate not passing
  (``gate_not_passed``), no eligible price (``no_eligible_price``), terms that
  cannot honestly parameterise the engine (``terms_insufficient`` + a typed
  reason code), or a typed engine refusal (``engine_typed_error`` + the engine's
  own ``BondError`` code, e.g. ``front_stub_unsupported``);
* the source's own ``ytm`` column is NEVER copied — every yield here is
  RECOMPUTED independently by the validated engines (Phase-10 recompute
  principle);
* deterministic by construction: no I/O, no environment read, no wall clock —
  ``as_of`` is the chain's calc-date, supplied by the caller.

Wave-1 metric set (Global Constraints 1-2): EXACTLY ``security_ytm``,
``security_ytw``, ``current_yield``, ``wal``.  OAS, z-spread and the duration
family are DELIBERATELY absent from this module and from the product.

Documented input conventions (decisions, mirrored in the Task 3 report):

* **Price** is the eligible latest CLEAN price in percent of par (the Task-1
  contract stamps ``price_type='trade'``, ``accrued_treatment='clean'``); yields
  are solved with ``price_is_clean=True`` at the price's own observation date
  (trade-date settlement; ``settlement_convention`` is deliberately not applied
  in Wave 1 — no calendar source exists, and a fabricated T+n shift would be a
  guess).
* **Coupon rate** is the published convention: PERCENT of par per annum (the
  serving surface renders ``coupon_rate::text || '%'``); it is converted to the
  engines' decimal fraction here, once, explicitly.
* **Coupon frequency** is derived from the reported ``coupon_schedule`` dates
  (month gap between the last two dates: 12/6/3 months); an underivable
  frequency is a typed insufficiency, never a defaulted "semiannual".
* **Dated date** (the engine's ``issue_date``) is anchored one coupon period
  before the FIRST reported coupon date, using the engine's own month-shift
  arithmetic; if the reported schedule sits off the maturity-anchored grid the
  engine's ``front_stub_unsupported`` refusal surfaces as a typed row.
* **Zero-coupon bonds** carry no coupon grid: the dated date is the settlement
  itself and the yield is quoted on the semiannual (US bond-equivalent) basis.
* **WAL** is computed from the schedule alone: the principal-weighted average
  ACT/365F time (years) from settlement to each FUTURE redemption cash flow —
  for a bullet bond, the time to maturity.  WAL settles at the chain calc-date
  (``as_of``); the price-derived yields settle at the price observation date.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from numbers import Real
from typing import Any, Mapping
from uuid import UUID

from .cashflows import (
    BondTerms,
    CallOption,
    CashFlowKind,
    DayCount,
    Frequency,
    Schedule,
    _shift_months,
    generate_schedule,
)
from .errors import BondError
from . import pricing

# --------------------------------------------------------------------------- #
# Wave-1 vocabulary (closed; mirrored by the bond_metric_v1 DDL CHECKs).
# --------------------------------------------------------------------------- #
WAVE1_METRICS: tuple[str, ...] = ("security_ytm", "security_ytw", "current_yield", "wal")

# The metrics whose engine input pipeline REQUIRES an eligible price; ``wal`` is
# schedule-only (its Phase-10 gate carries no eligible_price PIT input either).
PRICE_CONSUMING_METRICS: frozenset[str] = frozenset(
    {"security_ytm", "security_ytw", "current_yield"}
)

STATUS_AVAILABLE = "available"
STATUS_NO_ELIGIBLE_PRICE = "no_eligible_price"
STATUS_TERMS_INSUFFICIENT = "terms_insufficient"
STATUS_ENGINE_TYPED_ERROR = "engine_typed_error"
STATUS_GATE_NOT_PASSED = "gate_not_passed"

STATUSES: frozenset[str] = frozenset(
    {
        STATUS_AVAILABLE,
        STATUS_NO_ELIGIBLE_PRICE,
        STATUS_TERMS_INSUFFICIENT,
        STATUS_ENGINE_TYPED_ERROR,
        STATUS_GATE_NOT_PASSED,
    }
)

# Published day-count vocabulary -> engine convention.  '30/360' is the observed
# published spelling of the ISDA 30/360 US (bond basis) convention; anything not
# listed here is a typed insufficiency, never a guessed convention.
_DAY_COUNTS: dict[str, DayCount] = {
    "30/360 US": DayCount.THIRTY_360_US,
    "30/360": DayCount.THIRTY_360_US,
    "ACT/ACT ICMA": DayCount.ACT_ACT_ICMA,
    "ACT/360": DayCount.ACT_360,
    "ACT/365F": DayCount.ACT_365F,
}

# Reported coupon-date month gap -> engine frequency (closed; no default).
_FREQUENCY_BY_MONTH_GAP: dict[int, Frequency] = {
    12: Frequency.ANNUAL,
    6: Frequency.SEMIANNUAL,
    3: Frequency.QUARTERLY,
}

_FIXED_COUPON_TYPES = frozenset({"fixed"})
_ZERO_COUPON_TYPES = frozenset({"zero", "zero coupon", "zero-coupon", "zero_coupon"})

# Zero-coupon yields are quoted on the semiannual (US bond-equivalent) basis.
_ZERO_COUPON_QUOTE_FREQUENCY = Frequency.SEMIANNUAL

# WAL calendar time (ACT/365F), consistent with the pricing engine's curve time.
_WAL_DAYS_PER_YEAR = 365.0


# --------------------------------------------------------------------------- #
# Inputs / outputs
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SecurityTermsInput:
    """One published security's engine-relevant terms, exactly as published.

    Values arrive RAW (DB numerics may be :class:`~decimal.Decimal`, jsonb
    schedules are lists of mappings, dates may be ISO strings); every coercion
    below is explicit and typed — nothing is repaired or defaulted.
    """

    security_id: UUID
    coupon_type: object = None
    coupon_rate: object = None
    maturity_date: object = None
    day_count: object = None
    coupon_schedule: object = None
    call_schedule: object = None


@dataclass(frozen=True)
class EligiblePrice:
    """The security's eligible latest CLEAN price (% of par) and its date."""

    price: object
    observation_date: date


@dataclass(frozen=True)
class MetricRow:
    """One typed per-(security, metric) result row for ``bond_metric_v1``."""

    security_id: UUID
    metric_id: str
    value: float | None
    status: str
    engine_error_code: str | None
    as_of: date


class _TermsInsufficient(Exception):
    """Internal: published terms cannot honestly parameterise the engine."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


# --------------------------------------------------------------------------- #
# Typed coercions (explicit; absence/malformation is never repaired)
# --------------------------------------------------------------------------- #
def _as_finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _as_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _month_gap(earlier: date, later: date) -> int:
    return (later.year - earlier.year) * 12 + (later.month - earlier.month)


def _schedule_dates(raw: object) -> list[date]:
    """Parse the reported coupon_schedule jsonb into ascending dates (typed)."""
    if raw is None or (isinstance(raw, (list, tuple)) and len(raw) == 0):
        raise _TermsInsufficient("coupon_schedule_missing")
    if not isinstance(raw, (list, tuple)):
        raise _TermsInsufficient("coupon_schedule_invalid")
    dates: list[date] = []
    for entry in raw:
        if not isinstance(entry, Mapping):
            raise _TermsInsufficient("coupon_schedule_invalid")
        parsed = _as_date(entry.get("date"))
        if parsed is None:
            raise _TermsInsufficient("coupon_schedule_invalid")
        dates.append(parsed)
    return sorted(dates)


def _derive_frequency(dates: list[date]) -> Frequency:
    """Frequency from the month gap between the last two reported coupon dates."""
    if len(dates) < 2:
        raise _TermsInsufficient("coupon_frequency_underivable")
    gap = _month_gap(dates[-2], dates[-1])
    frequency = _FREQUENCY_BY_MONTH_GAP.get(gap)
    if frequency is None:
        raise _TermsInsufficient("coupon_frequency_underivable")
    return frequency


def _call_options(raw: object) -> tuple[CallOption, ...]:
    """Parse the reported call_schedule jsonb into typed CallOptions."""
    if raw is None or (isinstance(raw, (list, tuple)) and len(raw) == 0):
        return ()
    if not isinstance(raw, (list, tuple)):
        raise _TermsInsufficient("call_schedule_invalid")
    options: list[CallOption] = []
    for entry in raw:
        if not isinstance(entry, Mapping):
            raise _TermsInsufficient("call_schedule_invalid")
        call_date = _as_date(entry.get("call_date", entry.get("date")))
        call_price = _as_finite(entry.get("call_price", entry.get("price")))
        if call_date is None or call_price is None:
            raise _TermsInsufficient("call_schedule_invalid")
        options.append(CallOption(call_date=call_date, price=call_price))
    return tuple(options)


def derive_engine_terms(terms_row: SecurityTermsInput, settlement: date) -> BondTerms:
    """Published terms -> engine :class:`BondTerms` (typed; never a default).

    Raises :class:`_TermsInsufficient` (typed reason code) when the published
    terms cannot honestly parameterise the engine; lets the engine's own typed
    :class:`BondError` refusals (e.g. inconsistent call dates) propagate.
    """
    maturity = terms_row.maturity_date
    if maturity is None:
        raise _TermsInsufficient("maturity_date_missing")
    maturity_date = _as_date(maturity)
    if maturity_date is None:
        raise _TermsInsufficient("maturity_date_invalid")

    coupon_type_raw = terms_row.coupon_type
    if coupon_type_raw is None or not str(coupon_type_raw).strip():
        raise _TermsInsufficient("coupon_type_missing")
    coupon_type = str(coupon_type_raw).strip().lower()

    day_count_raw = terms_row.day_count
    if day_count_raw is None or not str(day_count_raw).strip():
        raise _TermsInsufficient("day_count_missing")
    day_count = _DAY_COUNTS.get(str(day_count_raw).strip())
    if day_count is None:
        raise _TermsInsufficient("day_count_unsupported")

    calls = _call_options(terms_row.call_schedule)

    if coupon_type in _ZERO_COUPON_TYPES:
        rate = terms_row.coupon_rate
        if rate is not None:
            rate_value = _as_finite(rate)
            if rate_value is None:
                raise _TermsInsufficient("coupon_rate_invalid")
            if rate_value != 0.0:
                raise _TermsInsufficient("coupon_type_conflicts_rate")
        # No coupon grid exists: the dated date is the settlement itself (only
        # the settlement->maturity discount time matters for a zero).
        return BondTerms(
            issue_date=settlement,
            maturity_date=maturity_date,
            coupon_rate=0.0,
            frequency=_ZERO_COUPON_QUOTE_FREQUENCY,
            day_count=day_count,
            call_schedule=calls,
        )

    if coupon_type not in _FIXED_COUPON_TYPES:
        raise _TermsInsufficient("coupon_type_unsupported")

    if terms_row.coupon_rate is None:
        raise _TermsInsufficient("coupon_rate_missing")
    rate_percent = _as_finite(terms_row.coupon_rate)
    if rate_percent is None:
        raise _TermsInsufficient("coupon_rate_invalid")
    # Published convention: percent of par per annum -> engine decimal fraction.
    coupon_rate = rate_percent / 100.0

    dates = _schedule_dates(terms_row.coupon_schedule)
    frequency = _derive_frequency(dates)
    step_months = 12 // int(frequency)
    # Dated-date anchor: one coupon period before the FIRST reported coupon
    # date, using the engine's own clamped month-shift so the retrograde grid
    # check in ``coupon_dates`` is exact (an off-grid reported schedule then
    # surfaces the engine's typed ``front_stub_unsupported`` refusal).
    issue_date = _shift_months(dates[0], -step_months)

    return BondTerms(
        issue_date=issue_date,
        maturity_date=maturity_date,
        coupon_rate=coupon_rate,
        frequency=frequency,
        day_count=day_count,
        call_schedule=calls,
    )


# --------------------------------------------------------------------------- #
# Metric computation
# --------------------------------------------------------------------------- #
def _price_value(eligible_price: EligiblePrice) -> float:
    value = _as_finite(eligible_price.price)
    if value is None:
        raise BondError("invalid_price_input", {"price": eligible_price.price})
    return value


def wal_years(schedule: Schedule, settlement: date) -> float:
    """Principal-weighted average life (years, ACT/365F) of future redemptions.

    From the schedule alone: no price, no yield.  Typed refusals mirror the
    engine vocabulary (settlement outside the bond's life is never a zero).
    """
    terms = schedule.terms
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
    future = [
        cf for cf in schedule.cashflows
        if cf.kind == CashFlowKind.REDEMPTION and cf.pay_date > settlement
    ]
    if not future:
        raise BondError("no_future_cashflows", {"settlement": settlement.isoformat()})
    total = sum(cf.amount for cf in future)
    if total <= 0.0:
        raise BondError("no_future_cashflows", {"settlement": settlement.isoformat()})
    weighted = sum(cf.amount * (cf.pay_date - settlement).days for cf in future)
    return weighted / total / _WAL_DAYS_PER_YEAR


def _finite_or_typed(value: float) -> float:
    """Fail-closed: a non-finite engine result is a typed error, never stored."""
    if not math.isfinite(value):
        raise BondError("non_finite_result", {"value": repr(value)})
    return float(value)


def compute_security_metrics(
    terms_row: SecurityTermsInput,
    eligible_price: EligiblePrice | None,
    *,
    as_of: date,
    gate_passed: Mapping[str, bool],
) -> tuple[MetricRow, ...]:
    """One typed row per Wave-1 metric for one security.

    Status precedence per metric (documented, honest order):
      1. ``gate_not_passed`` — the Phase-10 gate did not pass for the metric
         (fail-closed: a metric absent from ``gate_passed`` did NOT pass);
      2. ``no_eligible_price`` — the metric consumes a price and none is
         eligible (``wal`` is schedule-only and skips this check);
      3. ``terms_insufficient`` — the published terms cannot honestly
         parameterise the engine (typed reason code);
      4. ``engine_typed_error`` — a typed engine refusal (``BondError`` code);
      5. ``available`` — a finite recomputed value.
    """
    rows: list[MetricRow] = []
    # Per-settlement caches: terms/schedule derivation is deterministic, so the
    # yields (price-date settlement) and WAL (as_of settlement) share work.
    schedules: dict[date, Schedule] = {}

    def schedule_for(settlement: date) -> Schedule:
        if settlement not in schedules:
            schedules[settlement] = generate_schedule(
                derive_engine_terms(terms_row, settlement)
            )
        return schedules[settlement]

    def compute(metric: str) -> float:
        if metric == "wal":
            return wal_years(schedule_for(as_of), as_of)
        assert eligible_price is not None  # guarded by the precedence ladder
        settlement = eligible_price.observation_date
        schedule = schedule_for(settlement)
        price_value = _price_value(eligible_price)
        if metric == "security_ytm":
            return pricing.yield_to_maturity(schedule, settlement, price_value)
        if metric == "security_ytw":
            return pricing.yield_to_worst(schedule, settlement, price_value).ytw
        if metric == "current_yield":
            return pricing.current_yield(schedule, price_value)
        raise BondError("unknown_metric", {"metric": metric})

    for metric in WAVE1_METRICS:
        if not gate_passed.get(metric, False):
            rows.append(MetricRow(terms_row.security_id, metric, None,
                                  STATUS_GATE_NOT_PASSED, None, as_of))
            continue
        if metric in PRICE_CONSUMING_METRICS and eligible_price is None:
            rows.append(MetricRow(terms_row.security_id, metric, None,
                                  STATUS_NO_ELIGIBLE_PRICE, None, as_of))
            continue
        try:
            value = _finite_or_typed(compute(metric))
        except _TermsInsufficient as exc:
            rows.append(MetricRow(terms_row.security_id, metric, None,
                                  STATUS_TERMS_INSUFFICIENT, exc.code, as_of))
        except BondError as exc:
            rows.append(MetricRow(terms_row.security_id, metric, None,
                                  STATUS_ENGINE_TYPED_ERROR, exc.code, as_of))
        else:
            rows.append(MetricRow(terms_row.security_id, metric, value,
                                  STATUS_AVAILABLE, None, as_of))
    return tuple(rows)


__all__ = [
    "PRICE_CONSUMING_METRICS",
    "STATUS_AVAILABLE",
    "STATUS_ENGINE_TYPED_ERROR",
    "STATUS_GATE_NOT_PASSED",
    "STATUS_NO_ELIGIBLE_PRICE",
    "STATUS_TERMS_INSUFFICIENT",
    "STATUSES",
    "WAVE1_METRICS",
    "EligiblePrice",
    "MetricRow",
    "SecurityTermsInput",
    "compute_security_metrics",
    "derive_engine_terms",
    "wal_years",
]
