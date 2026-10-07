"""US federal holidays (5 U.S.C. § 6103) under the Federal Reserve observance."""
from __future__ import annotations

import datetime as dt

import pytest

from src import us_federal_holidays as fh


def test_2025_is_the_published_eleven_holiday_set() -> None:
    # The Federal Reserve Board's 2025 holiday schedule, every date a weekday.
    assert fh.federal_holidays(2025) == frozenset({
        dt.date(2025, 1, 1),    # New Year's Day (Wed)
        dt.date(2025, 1, 20),   # Birthday of Martin Luther King, Jr. (Mon)
        dt.date(2025, 2, 17),   # Washington's Birthday (Mon)
        dt.date(2025, 5, 26),   # Memorial Day (Mon)
        dt.date(2025, 6, 19),   # Juneteenth (Thu)
        dt.date(2025, 7, 4),    # Independence Day (Fri)
        dt.date(2025, 9, 1),    # Labor Day (Mon)
        dt.date(2025, 10, 13),  # Columbus Day (Mon)
        dt.date(2025, 11, 11),  # Veterans Day (Tue)
        dt.date(2025, 11, 27),  # Thanksgiving Day (Thu)
        dt.date(2025, 12, 25),  # Christmas Day (Thu)
    })


@pytest.mark.parametrize("sunday, observed", [
    (dt.date(2022, 6, 19), dt.date(2022, 6, 20)),   # Juneteenth 2022
    (dt.date(2022, 12, 25), dt.date(2022, 12, 26)),  # Christmas 2022
    (dt.date(2023, 1, 1), dt.date(2023, 1, 2)),     # New Year's Day 2023
    (dt.date(2021, 7, 4), dt.date(2021, 7, 5)),     # Independence Day 2021
])
def test_a_sunday_holiday_is_observed_on_monday(sunday, observed) -> None:
    assert sunday.weekday() == fh.SUNDAY
    assert fh.is_federal_holiday(observed)
    assert not fh.is_federal_holiday(sunday)


def test_a_saturday_holiday_is_not_moved_to_friday() -> None:
    # Reserve Banks are open the preceding Friday (differs from 5 U.S.C. 6103(b),
    # which can only ever move a holiday onto a Friday - moot for a Mon-Wed test).
    saturday = dt.date(2026, 7, 4)
    assert saturday.weekday() == fh.SATURDAY
    assert fh.is_federal_holiday(saturday)
    assert not fh.is_federal_holiday(dt.date(2026, 7, 3))


def test_juneteenth_exists_only_from_2021() -> None:
    assert not fh.is_federal_holiday(dt.date(2020, 6, 19))  # a Friday, pre-enactment
    assert fh.is_federal_holiday(dt.date(2021, 6, 19))      # Saturday, not moved
    assert not fh.is_federal_holiday(dt.date(2021, 6, 18))
    assert fh.is_federal_holiday(dt.date(2024, 6, 19))      # Wednesday


def test_mlk_day_exists_only_from_1986() -> None:
    assert fh.is_federal_holiday(dt.date(1986, 1, 20))
    assert not fh.is_federal_holiday(dt.date(1985, 1, 21))
    assert len(fh.federal_holidays(1985)) == 9  # no MLK, no Juneteenth


def test_floating_holidays_land_on_the_right_weekday() -> None:
    for year in range(2007, 2031):
        days = fh.federal_holidays(year)
        assert len(days) == (11 if year >= 2021 else 10)
        assert all(d.weekday() != fh.SUNDAY for d in days)
        assert fh._nth_weekday(year, 11, fh.THURSDAY, 4) in days
        assert fh._last_weekday(year, 5, fh.MONDAY) in days
        assert fh._last_weekday(year, 5, fh.MONDAY).month == 5
        assert fh._last_weekday(year, 5, fh.MONDAY).weekday() == fh.MONDAY


def test_ordinary_weekdays_are_not_holidays() -> None:
    assert not fh.is_federal_holiday(dt.date(2025, 3, 12))
    assert not fh.is_federal_holiday(dt.date(2018, 12, 5))  # day of mourning, not 6103
