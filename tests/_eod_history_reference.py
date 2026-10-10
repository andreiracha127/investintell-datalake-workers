"""Deliberately simple reference for ``validate_series`` (tests only).

Written independently of ``src/workers/eod_history_validation.py``: its own
date parser, its own bar predicate with its own exact (``Fraction``)
tolerance arithmetic and bounds, its own session calendar, plain set
arithmetic, and the same written rule order (production module docstring).

Calendar independence: the reference does NOT use exchange_calendars (nor
pandas, dateutil or any other calendar package). It covers the whole supported
domain, every year 1970-2026, from its own data: a hard-coded table of the
NYSE holiday rules with the years each rule applied (``RULES``), its own
Easter (anonymous Gregorian algorithm) and nth-weekday helpers, the
exchange's weekend-observance rule (``_observed``), and an explicit list of
one-off closures (``SPECIAL_CLOSURES``). Sessions are weekdays minus those
closures. Fuzz intervals are drawn from every year, so production's
exchange_calendars sessions are checked against an independent source over
the whole domain. Returns ``(status, code, rows)``.

Sources:
[NYSE-H] NYSE Archives, "History of New York Stock Exchange Holidays" and
    "New York Stock Exchange Special Closings, 1885-date" (revised through
    January 2011), copy at
    http://s3.amazonaws.com/armstrongeconomics-wp/2013/07/NYSE-Closings.pdf
    Rules: New Year's, Independence, Labor, Thanksgiving and Christmas Days
    and Good Friday closed every year; Washington's Birthday and Memorial Day
    observed on Mondays since 1971; Martin Luther King, Jr. Day closed all day
    beginning in 1998; Election Day closed in presidential election years
    only, 1972-1980; Columbus Day closed only through 1953 and Veterans Day
    only through 1953 (a two-minute silence 1954-2006); a Sunday holiday
    closes the following Monday, and by Board policy of July 3, 1959 (Rule 51)
    a Saturday holiday closes the preceding Friday "unless it ends a monthly
    or yearly accounting period". Its full-day special closings from 1970
    through January 2011 are exactly the 1972-2007 entries below.
[RULE-7.2] NYSE Rule 7.2 as quoted in SEC Release No. 34-93183
    (SR-NYSE-2021-56), 86 FR 55068 (Oct. 5, 2021), which adds Juneteenth
    National Independence Day (June 19) from 2022 with the same weekend rule
    and accounting-period exception:
    https://www.govinfo.gov/content/pkg/FR-2021-10-05/pdf/2021-21742.pdf
[NYSE-2026] NYSE holidays and trading hours (2026 schedule; "Because the
    holiday falls on Saturday, January 1, 2028, no New Year's Day holiday is
    observed"): https://www.nyse.com/markets/hours-calendars
[USC] 5 U.S.C. 6103 (third Monday in January/February, last Monday in May,
    first Monday in September, fourth Thursday in November; Pub. L. 90-363
    effective January 1, 1971): https://www.law.cornell.edu/uscode/text/5/6103
[ELECTION] Election Day open from 1984: FINRA (NASD) Notice to Members 84-43,
    https://www.finra.org/rules-guidance/notices/84-43 and
    https://www.csmonitor.com/1984/1105/110516.html
[SANDY] NYSE Euronext, Oct. 30, 2012, reopening on Oct. 31,
    https://mondovisione.com/news/nyse-euronext-statement-on-opening-of-us-markets-on-wednesday-oct-31-2012-20121030/
    and AP, two-day weather closure:
    https://www.mprnews.org/story/2012/10/31/wall-street-back-in-business-after-storm-shutdown
[ICE-2018] https://ir.theice.com/press/news-details/2018/New-York-Stock-Exchange-to-Honor-President-George-H-W-Bush/default.aspx
[ICE-2025] https://ir.theice.com/press/news-details/2024/The-New-York-Stock-Exchange-Will-Close-Markets-on-January-9-to-Honor-the-Passing-of-Former-President-Jimmy-Carter-on-National-Day-of-Mourning/default.aspx
"""

from __future__ import annotations

import datetime as dt
import math
import re
from fractions import Fraction

SUPPORTED_FROM = dt.date(1970, 1, 1)
LAST_YEAR = 2026
_MON, _TUE, _THU = 0, 1, 3


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> dt.date:
    """The ``n``-th (1-based) ``weekday`` of the month."""
    first = dt.date(year, month, 1)
    return first + dt.timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    last = dt.date(year + month // 12, month % 12 + 1, 1) - dt.timedelta(days=1)
    return last - dt.timedelta(days=(last.weekday() - weekday) % 7)


def _easter(year: int) -> dt.date:
    """Gregorian Easter Sunday (anonymous Gregorian / Meeus-Jones-Butcher)."""
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l_ = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_) // 451
    month, day = divmod(h + l_ - 7 * m + 114, 31)
    return dt.date(year, month, day + 1)


def _observed(day: dt.date):
    """The weekday the exchange closes for a holiday falling on ``day``, or None.

    Sunday -> the following Monday. Saturday -> the preceding Friday, unless
    that Friday is the last weekday of its month (it ends a monthly or yearly
    accounting period: 1970-05-29, every Dec. 31) [NYSE-H, RULE-7.2].
    """
    if day.weekday() == 6:
        return day + dt.timedelta(days=1)
    if day.weekday() == 5:
        friday = day - dt.timedelta(days=1)
        return None if (friday + dt.timedelta(days=3)).month != friday.month else friday
    return day


_ALL = range(1970, LAST_YEAR + 1)
# (holiday, years the rule applied, the holiday's date in year y) [NYSE-H, USC]
RULES = (
    ("New Year's Day", _ALL, lambda y: dt.date(y, 1, 1)),
    ("Martin Luther King, Jr. Day", range(1998, LAST_YEAR + 1), lambda y: _nth_weekday(y, 1, _MON, 3)),
    ("Washington's Birthday", range(1970, 1971), lambda y: dt.date(y, 2, 22)),
    ("Washington's Birthday", range(1971, LAST_YEAR + 1), lambda y: _nth_weekday(y, 2, _MON, 3)),
    ("Good Friday", _ALL, lambda y: _easter(y) - dt.timedelta(days=2)),
    ("Memorial Day", range(1970, 1971), lambda y: dt.date(y, 5, 30)),
    ("Memorial Day", range(1971, LAST_YEAR + 1), lambda y: _last_weekday(y, 5, _MON)),
    ("Juneteenth", range(2022, LAST_YEAR + 1), lambda y: dt.date(y, 6, 19)),  # [RULE-7.2]
    ("Independence Day", _ALL, lambda y: dt.date(y, 7, 4)),
    ("Labor Day", _ALL, lambda y: _nth_weekday(y, 9, _MON, 1)),
    # Tuesday after the first Monday in November, presidential years only [NYSE-H, ELECTION]
    ("Election Day", range(1972, 1981, 4), lambda y: _nth_weekday(y, 11, _MON, 1) + dt.timedelta(days=1)),
    ("Thanksgiving Day", _ALL, lambda y: _nth_weekday(y, 11, _THU, 4)),
    ("Christmas Day", _ALL, lambda y: dt.date(y, 12, 25)),
)
SPECIAL_CLOSURES = tuple(sorted(dt.date.fromisoformat(s) for s in (
    "1972-12-28",  # funeral of former President Truman [NYSE-H]
    "1973-01-25",  # funeral of former President L. B. Johnson [NYSE-H]
    "1977-07-14",  # New York City blackout [NYSE-H]
    "1985-09-27",  # Hurricane Gloria [NYSE-H]
    "1994-04-27",  # funeral of former President Nixon [NYSE-H]
    "2001-09-11", "2001-09-12", "2001-09-13", "2001-09-14",  # World Trade Center attack [NYSE-H]
    "2004-06-11",  # national day of mourning, former President Reagan [NYSE-H]
    "2007-01-02",  # national day of mourning, former President Ford [NYSE-H]
    "2012-10-29", "2012-10-30",  # Hurricane Sandy [SANDY]
    "2018-12-05",  # national day of mourning, former President G. H. W. Bush [ICE-2018]
    "2025-01-09",  # national day of mourning, former President Carter [ICE-2025]
)))


def ref_closed_days(year: int) -> frozenset:
    """Weekdays of ``year`` on which the NYSE did not trade (rules + special list)."""
    out = {d for d in SPECIAL_CLOSURES if d.year == year}
    for _name, years, date_in in RULES:
        if year in years:
            d = _observed(date_in(year))
            if d is not None:
                assert d.year == year, (year, d)
                out.add(d)
    return frozenset(d for d in out if d.weekday() < 5)


HOLIDAYS = {y: " ".join(d.strftime("%m-%d") for d in sorted(ref_closed_days(y))) for y in _ALL}
YEARS = tuple(sorted(HOLIDAYS))
_CLOSED = {dt.date(y, int(md[:2]), int(md[3:])) for y, s in HOLIDAYS.items() for md in s.split()}

KEYS = ("open", "high", "low", "close", "volume", "adjOpen", "adjHigh", "adjLow",
        "adjClose", "adjVolume", "divCash", "splitFactor")
COLS = ("open", "high", "low", "close", "volume", "adj_open", "adj_high", "adj_low",
        "adj_close", "adj_volume", "div_cash", "split_factor")
PAIRS_RAW = [("open", "open"), ("high", "high"), ("low", "low"), ("close", "close")]
PAIRS_ADJ = [("adjOpen", "adj_open"), ("adjHigh", "adj_high"), ("adjLow", "adj_low"),
             ("adjClose", "adj_close")]


def ref_sessions(a: dt.date, b: dt.date):
    """Weekdays minus listed closures; None outside the supported domain."""
    if a < SUPPORTED_FROM:
        return None
    out = set()
    d = a
    while d <= b:
        if d.year not in HOLIDAYS:
            raise AssertionError(f"reference calendar has no data for {d.year}")
        if d.weekday() < 5 and d not in _CLOSED:
            out.add(d)
        d += dt.timedelta(days=1)
    return out


def ref_date(x):
    if type(x) is not str or not x.isascii():
        return None
    if not re.fullmatch(r"\d\d\d\d-\d\d-\d\d(T00:00:00(\.000)?Z)?", x):
        return None
    try:
        return dt.date(int(x[0:4]), int(x[5:7]), int(x[8:10]))
    except ValueError:
        return None


def ref_values_ok(v: dict) -> bool:
    for k in KEYS:
        x = v.get(k, "missing")
        if type(x) not in (int, float):
            return False
        try:
            if not math.isfinite(x):
                return False
        except OverflowError:
            return False
    raw = [v[k] for k in ("open", "high", "low", "close")]
    adjusted = [v[k] for k in ("adjOpen", "adjHigh", "adjLow", "adjClose")]
    if min(raw + adjusted) <= 0 or v["splitFactor"] <= 0:
        return False
    if v["volume"] < 0 or v["adjVolume"] < 0 or v["divCash"] < 0:
        return False
    if max(raw) > 10**7 or max(adjusted) > 10**13:
        return False
    if v["volume"] > 10**13 or v["adjVolume"] > 10**13:
        return False
    if v["divCash"] > 10**7 or v["splitFactor"] > 10**4:
        return False
    if not (v["low"] <= v["open"] <= v["high"] and v["low"] <= v["close"] <= v["high"]):
        return False
    if not (v["adjLow"] <= v["adjOpen"] <= v["adjHigh"]
            and v["adjLow"] <= v["adjClose"] <= v["adjHigh"]):
        return False
    # Same factor within 1%: |adj/raw - adjClose/close| <= adjClose/close / 100,
    # checked as fractions so nothing overflows.
    f = Fraction(v["adjClose"]) / Fraction(v["close"])
    for r, a in (("open", "adjOpen"), ("high", "adjHigh"), ("low", "adjLow")):
        if abs(Fraction(v[a]) / Fraction(v[r]) - f) * 100 > f:
            return False
    return True


def ref_bar_ok(b) -> bool:
    return type(b) is dict and ref_date(b.get("date")) is not None and ref_values_ok(b)


def ref_stored_ok(row: dict) -> bool:
    return ref_values_ok({k: row[c] for k, c in zip(KEYS, COLS) if c in row})


def _eq(a, b) -> bool:
    fa, fb = Fraction(a), Fraction(b)
    return abs(fa - fb) * 1_000_000 <= max(abs(fa), abs(fb))


def reference_verdict(start, end, bars, stored, gaps=frozenset()):
    if start > end:
        return "history_incomplete", "empty_interval", {}
    exp = ref_sessions(start, end)
    if exp is None:
        return "history_incomplete", "interval_outside_calendar", {}
    if type(bars) is not list:
        return "history_incomplete", "malformed_body", {}
    if len(bars) == 0:
        return "history_incomplete", "empty_window", {}
    for b in bars:
        if not ref_bar_ok(b):
            return "history_incomplete", "unusable_bar", {}
    dates = [ref_date(b["date"]) for b in bars]
    if len(set(dates)) != len(dates):
        return "history_incomplete", "duplicate_dates", {}
    prov = {ref_date(b["date"]): b for b in bars}
    for d in prov:
        if d < start or d > end:
            return "history_incomplete", "bars_outside_request", {}
    for d in prov:
        if d not in exp:
            return "history_incomplete", "off_session_bar", {}
    for d in stored:
        if not ref_stored_ok(stored[d]):
            return "history_conflict", "stored_bar_invalid", {}
    common = set(prov) & set(stored)
    for d in common:
        for k, s in PAIRS_RAW:
            if not _eq(prov[d][k], stored[d][s]):
                return "history_conflict", "raw_differs", {}
    for d in stored:
        if d < start or d > end:
            return "history_conflict", "stored_outside_provider_range", {}
    for d in stored:
        if d not in exp:
            return "history_conflict", "stored_off_session", {}
    for d in common:
        for k, s in PAIRS_ADJ:
            if not _eq(prov[d][k], stored[d][s]):
                return "adjustment_rebase_required", "adjusted_moved", {}
    if set(stored) - set(prov):
        return "history_incomplete", "stored_sessions_missing", {}
    if (exp - set(gaps)) - set(prov) - set(stored):
        return "history_incomplete", "sessions_missing", {}
    new = {d: tuple(prov[d][k] for k in KEYS) for d in prov if d not in stored}
    if not new:
        return "complete_nothing_to_insert", "verified", {}
    return "load", "inserted", new
