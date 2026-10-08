"""US federal holidays (5 U.S.C. § 6103(a)) as the Federal Reserve observes them.

Rule-based and data-free, so a release-date stamp derived from it is reproducible
from the code alone. Scope: point-in-time release stamping of weekly Federal
Reserve data (the regime_composite NFCI vote). It is NOT an exchange calendar and
it does not replace the quadrant freshness model's documented Mon–Fri business
days (src/quadrant_staleness.py, an A3 calibration point left untouched).

Observance follows the Federal Reserve Bank holiday schedule, the Chicago Fed
being a Reserve Bank: a holiday that falls on a Sunday is observed on the
following Monday; a holiday that falls on a Saturday is NOT moved (Reserve Banks
are open the preceding Friday). 5 U.S.C. § 6103(b) instead has the executive
branch observe a Saturday holiday on the Friday before; the two conventions
differ ONLY on that Friday, which can never be the Monday–Wednesday a
release-delay test inspects, so the NFCI stamp is identical under either reading.
Ad-hoc closures (national days of mourning, e.g. 2018-12-05) are not § 6103
holidays and are not modelled.

Holidays: New Year's Day (Jan 1); Birthday of Martin Luther King, Jr. (third
Monday of January, since 1986); Washington's Birthday (third Monday of February);
Memorial Day (last Monday of May); Juneteenth National Independence Day (Jun 19,
since 2021); Independence Day (Jul 4); Labor Day (first Monday of September);
Columbus Day (second Monday of October); Veterans Day (Nov 11); Thanksgiving Day
(fourth Thursday of November); Christmas Day (Dec 25). Inauguration Day is a
District of Columbia-area holiday only and is excluded.
"""
from __future__ import annotations

import datetime as _dt
from functools import lru_cache

MONDAY, THURSDAY, SATURDAY, SUNDAY = 0, 3, 5, 6
MLK_DAY_SINCE = 1986        # Pub. L. 98-144, first observed 1986
JUNETEENTH_SINCE = 2021     # Pub. L. 117-17, enacted 2021-06-17


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> _dt.date:
    """The ``n``-th (1-based) ``weekday`` of ``month``."""
    first = _dt.date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + _dt.timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> _dt.date:
    """The last ``weekday`` of ``month``."""
    nxt = _dt.date(year + 1, 1, 1) if month == 12 else _dt.date(year, month + 1, 1)
    last = nxt - _dt.timedelta(days=1)
    return last - _dt.timedelta(days=(last.weekday() - weekday) % 7)


def _observed(holiday: _dt.date) -> _dt.date:
    """Federal Reserve observance: Sunday -> following Monday; Saturday unchanged."""
    if holiday.weekday() == SUNDAY:
        return holiday + _dt.timedelta(days=1)
    return holiday


@lru_cache(maxsize=None)
def federal_holidays(year: int) -> frozenset[_dt.date]:
    """The observed federal holidays of ``year`` (see the module docstring)."""
    fixed = [
        _dt.date(year, 1, 1),     # New Year's Day
        _dt.date(year, 7, 4),     # Independence Day
        _dt.date(year, 11, 11),   # Veterans Day
        _dt.date(year, 12, 25),   # Christmas Day
    ]
    if year >= JUNETEENTH_SINCE:
        fixed.append(_dt.date(year, 6, 19))  # Juneteenth National Independence Day
    floating = [
        _nth_weekday(year, 2, MONDAY, 3),     # Washington's Birthday
        _last_weekday(year, 5, MONDAY),       # Memorial Day
        _nth_weekday(year, 9, MONDAY, 1),     # Labor Day
        _nth_weekday(year, 10, MONDAY, 2),    # Columbus Day
        _nth_weekday(year, 11, THURSDAY, 4),  # Thanksgiving Day
    ]
    if year >= MLK_DAY_SINCE:
        floating.append(_nth_weekday(year, 1, MONDAY, 3))  # Birthday of M. L. King, Jr.
    return frozenset(_observed(d) for d in fixed) | frozenset(floating)


def is_federal_holiday(day: _dt.date) -> bool:
    """True when ``day`` is an observed federal holiday (Reserve Bank schedule)."""
    return day in federal_holidays(day.year)
