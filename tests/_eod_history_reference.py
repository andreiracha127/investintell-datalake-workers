"""Deliberately simple reference for ``validate_series`` (tests only).

Written independently of ``src/workers/eod_history_validation.py``: its own
date parser, its own bar predicate, its own session sets straight from
``exchange_calendars``, plain set arithmetic, and the same written rule order
(module docstring of the production validator). Returns
``(status, code, rows)`` where ``code`` is the reason's leading token and
``rows`` maps each date to insert to its twelve eod_prices price fields.
"""

from __future__ import annotations

import datetime as dt
import math
import re

import exchange_calendars as xcals

_CAL = xcals.get_calendar("XNYS", start="1900-01-01")
KEYS = ("open", "high", "low", "close", "volume", "adjOpen", "adjHigh", "adjLow",
        "adjClose", "adjVolume", "divCash", "splitFactor")
PAIRS_RAW = [("open", "open"), ("high", "high"), ("low", "low"), ("close", "close")]
PAIRS_ADJ = [("adjOpen", "adj_open"), ("adjHigh", "adj_high"), ("adjLow", "adj_low"),
             ("adjClose", "adj_close")]


def ref_sessions(a: dt.date, b: dt.date):
    if a < _CAL.first_session.date() or b > _CAL.last_session.date():
        return None
    out = set()
    for ts in _CAL.sessions_in_range(str(a), str(b)):
        out.add(dt.date(ts.year, ts.month, ts.day))
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


def ref_bar_ok(b) -> bool:
    if type(b) is not dict:
        return False
    if ref_date(b.get("date")) is None:
        return False
    for k in KEYS:
        if k not in b or type(b[k]) not in (int, float) or not math.isfinite(b[k]):
            return False
    for k in ("open", "high", "low", "close", "adjOpen", "adjHigh", "adjLow", "adjClose",
              "splitFactor"):
        if not b[k] > 0:
            return False
    for k in ("volume", "adjVolume", "divCash"):
        if b[k] < 0:
            return False
    if not (b["low"] <= b["open"] <= b["high"] and b["low"] <= b["close"] <= b["high"]):
        return False
    if not (b["adjLow"] <= b["adjOpen"] <= b["adjHigh"]
            and b["adjLow"] <= b["adjClose"] <= b["adjHigh"]):
        return False
    f = b["adjClose"] / b["close"]
    for r, a in (("open", "adjOpen"), ("high", "adjHigh"), ("low", "adjLow")):
        if abs(b[a] / b[r] - f) > 0.01 * f:
            return False
    return True


def _eq(a, b) -> bool:
    return abs(a - b) <= 1e-6 * max(abs(a), abs(b))


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
