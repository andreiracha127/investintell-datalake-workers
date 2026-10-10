"""Deliberately simple reference for ``validate_series`` (tests only).

Written independently of ``src/workers/eod_history_validation.py``: its own
date parser, its own bar predicate with its own exact (``Fraction``)
tolerance arithmetic and bounds, its own session calendar, plain set
arithmetic, and the same written rule order (production module docstring).

Calendar independence: the reference does NOT use exchange_calendars. Its
sessions are weekdays minus a hard-coded list of NYSE closures, written from
the exchange's published holiday schedules, for the years in ``HOLIDAYS``
only (a weekday rule plus holidays and special closures such as 2001-09-11..14,
2012-10-29/30, 1985-09-27, 1973-01-25, 2025-01-09; in 1970 Saturday holidays
were observed on the Friday except at a month end, so 1970-07-03 is closed and
1970-05-29 is not). Fuzz intervals are drawn
from those years, so production's exchange_calendars sessions are checked
against an independent source there; other years remain dependent on
exchange_calendars alone. Returns ``(status, code, rows)``.
"""

from __future__ import annotations

import datetime as dt
import math
import re
from fractions import Fraction

SUPPORTED_FROM = dt.date(1970, 1, 1)
HOLIDAYS = {
    1970: "01-01 02-23 03-27 07-03 09-07 11-26 12-25",
    1973: "01-01 01-25 02-19 04-20 05-28 07-04 09-03 11-22 12-25",
    1974: "01-01 02-18 04-12 05-27 07-04 09-02 11-28 12-25",
    1982: "01-01 02-15 04-09 05-31 07-05 09-06 11-25 12-24",
    1985: "01-01 02-18 04-05 05-27 07-04 09-02 09-27 11-28 12-25",
    2001: "01-01 01-15 02-19 04-13 05-28 07-04 09-03 09-11 09-12 09-13 09-14 11-22 12-25",
    2012: "01-02 01-16 02-20 04-06 05-28 07-04 09-03 10-29 10-30 11-22 12-25",
    2023: "01-02 01-16 02-20 04-07 05-29 06-19 07-04 09-04 11-23 12-25",
    2024: "01-01 01-15 02-19 03-29 05-27 06-19 07-04 09-02 11-28 12-25",
    2025: "01-01 01-09 01-20 02-17 04-18 05-26 06-19 07-04 09-01 11-27 12-25",
    2026: "01-01 01-19 02-16 04-03 05-25 06-19 07-03 09-07 11-26 12-25",
}
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
    prices = [v[k] for k in ("open", "high", "low", "close", "adjOpen", "adjHigh", "adjLow", "adjClose")]
    if min(prices) <= 0 or v["splitFactor"] <= 0:
        return False
    if v["volume"] < 0 or v["adjVolume"] < 0 or v["divCash"] < 0:
        return False
    if max(prices) > 10**7 or v["volume"] > 10**13 or v["adjVolume"] > 10**13:
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
