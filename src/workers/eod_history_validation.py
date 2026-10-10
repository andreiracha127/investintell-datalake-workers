"""Whole-ticker validation of a provider price series against stored rows.

``validate_series`` is the ONLY place that decides whether a Tiingo daily
series for one ticker may be written to ``eod_prices`` and certified
``history_complete`` (see ``eod_prices_warmer.promote``). It is pure: no
database, no network, no clock. Its inputs are the interval fixed before the
fetch, the provider's raw bars exactly as returned, the stored rows in scope
(all twelve price fields, ``STORED_FIELDS``), a session calendar and an (empty
by default) set of evidenced session gaps.

A bar — provider or stored — is valid when every one of its twelve fields is a
finite number (not a bool, not a string) inside its bound (``PRICE_MAX`` for
raw prices, ``ADJ_PRICE_MAX`` for adjusted prices; prices and the split factor
positive, volumes and dividend non-negative), raw and adjusted
``low <= min(open, close) <= max(open, close) <= high`` hold, and the
adjustment factors implied by adjOpen/open, adjHigh/high and adjLow/low are
within ``FACTOR_REL_TOL`` of adjClose/close. Every tolerance comparison is
exact rational arithmetic (``fractions.Fraction``) on validated finite values,
so no quotient can overflow to infinity or NaN and the boundary (``==``) is
well defined: a difference exactly at a tolerance is accepted. The ratios named
in a conflict or rebase reason are formatted from the exact fraction with
integer arithmetic (``_format_ratio``), never through a float.

Rules, applied in this order; the first that fails decides the verdict:

 1. ``start > end`` → incomplete ``empty_interval``.
 2. The interval must lie inside the calendar's supported range (from
    ``CALENDAR_SUPPORTED_FROM``), else incomplete ``interval_outside_calendar``
    (checked on the interval itself, whatever the response holds).
 3. A non-list body → incomplete ``malformed_body``; an empty one → incomplete
    ``empty_window``.
 4. Every provider element must be a valid bar whose ``date`` is exactly
    ``YYYY-MM-DD`` or Tiingo's ``YYYY-MM-DDT00:00:00[.000]Z`` (ASCII digits;
    nothing truncated or coerced), else incomplete ``unusable_bar``.
 5. A date returned twice → incomplete ``duplicate_dates``.
 6. A bar outside the interval → incomplete ``bars_outside_request``.
 7. A bar on a non-session → incomplete ``off_session_bar``.
 8. Every stored row must be a valid bar, else conflict ``stored_bar_invalid``
    — before any comparison uses it.
 9. Any shared (stored and returned) date whose raw open/high/low/close
    differs beyond ``STORED_REL_TOL`` → conflict ``raw_differs``.
10. A stored date outside the interval → conflict
    ``stored_outside_provider_range``; a stored date on a non-session →
    conflict ``stored_off_session``.
11. Any shared date whose adjusted open/high/low/close differs beyond
    ``STORED_REL_TOL`` → ``adjustment_rebase_required`` (``adjusted_moved``,
    with the median observed adjClose ratio). Checked whether or not anything
    is missing: a mixed adjusted basis is never complete.
12. A stored date the response does not contain → incomplete
    ``stored_sessions_missing``.
13. A session of the interval (minus ``accepted_gaps``) in neither the store
    nor the response → incomplete ``sessions_missing`` — interior sessions as
    much as the ends.
14. Otherwise the returned dates are exactly the interval's sessions: nothing
    new → ``complete_nothing_to_insert``; else ``load`` with exactly the rows
    for the returned dates that are not stored.

Volume, adjusted volume, dividend and split factor of retained rows are
validated (rule 8) and covered by the snapshot digest, but not compared with
the provider.

Every verdict carries ``digest``, a hash of the whole stored snapshot (all
``STORED_FIELDS``) it judged; promotion re-reads the same projection under a
lock and refuses if it changed.
"""

from __future__ import annotations

import datetime as _dt
import functools
import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Protocol

VERDICT_COMPLETE = "complete_nothing_to_insert"
VERDICT_LOAD = "load"
VERDICT_INCOMPLETE = "history_incomplete"
VERDICT_CONFLICT = "history_conflict"
VERDICT_REBASE = "adjustment_rebase_required"
SUCCESS_VERDICTS = frozenset({VERDICT_COMPLETE, VERDICT_LOAD})

# Certified calendar domain. The NYSE has traded a five-day week since
# September 1952 and exchange_calendars' XNYS can be constructed earlier than
# its holiday history is accurate; the oldest covered ticker starts on
# 1973-05-03. Nothing before this date is requested, compared or certified.
CALENDAR_SUPPORTED_FROM = _dt.date(1970, 1, 1)

# Stored rows and provider bars are both Tiingo values: they must agree to
# floating-point noise. Exact: |a - b| <= 1e-6 * max(|a|, |b|).
STORED_REL_TOL = Fraction(1, 1_000_000)
# Within one bar, adjOpen/open, adjHigh/high, adjLow/low and adjClose/close
# must name the same adjustment factor. Tiingo rounds adjusted fields (often
# to three decimals), so real bars disagree slightly: measured on 2,126,745
# stored production rows (2026-10-10) the largest deviation was 1.7e-3 (0.17%,
# AAMRQ), and 192 rows exceeded 1e-6. 1% accepts all of them and still rejects
# a misapplied split or the gate's 1-vs-900 bar. It is a WITHIN-bar check: a
# small corporate-action misapplication applied coherently to a whole bar is
# not detectable here. Sub-dollar prices rounded to three decimals could
# exceed it; none do in that sample.
FACTOR_REL_TOL = Fraction(1, 100)

# Magnitude bounds (inclusive upper, exclusive lower where stated). Raw prices:
# BRK-A, the highest US share price, is below 1e6; 1e7 leaves headroom while
# excluding overflow-scale values. Adjusted prices have their own, larger bound:
# cumulative reverse splits scale a history up (DryShips' disclosed ratios
# multiply to 11,760,000, so a 1.0 raw close adjusts to 1.176e7); 1e13 leaves
# a million-fold margin over the raw bound and is still far from float
# overflow. Volumes: daily US share volume is far below 1e13. Dividend: a raw
# per-share amount, never above the raw price bound. Split factor: real splits
# are well within 1e4.
PRICE_MAX = 1e7
ADJ_PRICE_MAX = 1e13
VOLUME_MAX = 1e13
DIVIDEND_MAX = PRICE_MAX
SPLIT_MAX = 1e4

_DATE = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})(?:T00:00:00(?:\.000)?Z)?")
_RAW = ("open", "high", "low", "close")
_ADJ = ("adjOpen", "adjHigh", "adjLow", "adjClose")
_PRICES = _RAW + _ADJ
_VOLUMES = ("volume", "adjVolume")
# eod_prices column → bar key, in eod_prices column order.
EOD_FIELDS: tuple[tuple[str, str], ...] = (
    ("open", "open"), ("high", "high"), ("low", "low"), ("close", "close"),
    ("volume", "volume"), ("adj_open", "adjOpen"), ("adj_high", "adjHigh"),
    ("adj_low", "adjLow"), ("adj_close", "adjClose"), ("adj_volume", "adjVolume"),
    ("div_cash", "divCash"), ("split_factor", "splitFactor"),
)
_BAR_KEYS = tuple(key for _, key in EOD_FIELDS)
_COL_TO_KEY = dict(EOD_FIELDS)
# The stored-row projection: snapshot, digest and the locked re-read all use it.
STORED_FIELDS: tuple[str, ...] = tuple(col for col, _ in EOD_FIELDS)
STORED_RAW = (("open", "open"), ("high", "high"), ("low", "low"), ("close", "close"))
STORED_ADJ = (("adj_open", "adjOpen"), ("adj_high", "adjHigh"),
              ("adj_low", "adjLow"), ("adj_close", "adjClose"))


class SessionCalendar(Protocol):
    def sessions(self, first: _dt.date, last: _dt.date) -> frozenset[_dt.date]:
        """Sessions in ``[first, last]``; ``ValueError`` outside the supported range."""


class XnysCalendar:
    """NYSE sessions from ``exchange_calendars`` (pinned in requirements.txt),
    for ``[CALENDAR_SUPPORTED_FROM, the calendar's last session]`` only.

    The underlying calendar's first session is the first one on or after
    ``CALENDAR_SUPPORTED_FROM`` (1970-01-02; New Year's Day was closed), and
    ``sessions_in_range`` refuses a range that starts before it, so the query
    starts at that session: the days in between are non-sessions by
    construction, not out of range."""

    def __init__(self) -> None:
        import exchange_calendars as xcals

        self._cal = xcals.get_calendar("XNYS", start=CALENDAR_SUPPORTED_FROM.isoformat())
        self.first = CALENDAR_SUPPORTED_FROM
        self._first_session = self._cal.first_session.date()
        self.last = self._cal.last_session.date()

    def sessions(self, first: _dt.date, last: _dt.date) -> frozenset[_dt.date]:
        if first < self.first or last > self.last:
            raise ValueError(f"{first}..{last} is outside XNYS {self.first}..{self.last}")
        lo = max(first, self._first_session)
        if lo > last:
            return frozenset()
        return frozenset(ts.date() for ts in self._cal.sessions_in_range(
            lo.isoformat(), last.isoformat()))


@functools.lru_cache(maxsize=1)
def xnys_calendar() -> XnysCalendar:
    return XnysCalendar()


@dataclass(frozen=True)
class Verdict:
    status: str                                   # one of the VERDICT_* values
    reason: str
    rows: tuple[tuple[Any, ...], ...] = ()        # eod_prices tuples to insert ("load")
    digest: str = ""                              # stored snapshot the verdict judged

    @property
    def certifies(self) -> bool:
        return self.status in SUCCESS_VERDICTS


def stored_digest(stored: Mapping[_dt.date, Mapping[str, Any]]) -> str:
    """Order-independent hash of the whole stored snapshot: every date and
    every ``STORED_FIELDS`` value, by its exact representation."""
    h = hashlib.sha256()
    for day in sorted(stored):
        values = stored[day]
        h.update(day.isoformat().encode())
        for col in STORED_FIELDS:
            h.update(b"|" + repr(values.get(col)).encode())
        h.update(b"\n")
    return h.hexdigest()


def parse_bar_date(value: Any) -> _dt.date | None:
    """Exact ``YYYY-MM-DD`` or ``YYYY-MM-DDT00:00:00[.000]Z``; anything else None."""
    if not isinstance(value, str):
        return None
    m = _DATE.fullmatch(value)
    if m is None:
        return None
    try:
        return _dt.date(int(m[1]), int(m[2]), int(m[3]))
    except ValueError:
        return None


def _finite(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:          # an int too large for a float
        return False


def _within(a: float, b: float, tol: Fraction) -> bool:
    """Exact ``|a - b| <= tol * max(|a|, |b|)`` on finite values."""
    fa, fb = Fraction(a), Fraction(b)
    return abs(fa - fb) <= tol * max(abs(fa), abs(fb))


def values_problem(values: Mapping[str, Any]) -> str | None:
    """Why the twelve fields (bar keys) are not a valid bar, or None."""
    for key in _BAR_KEYS:
        if key not in values:
            return f"missing_{key}"
        if not _finite(values[key]):
            return f"non_numeric_{key}"
    if any(values[k] <= 0 for k in _PRICES) or values["splitFactor"] <= 0:
        return "non_positive"
    if any(values[k] < 0 for k in _VOLUMES) or values["divCash"] < 0:
        return "negative"
    if (any(values[k] > PRICE_MAX for k in _RAW)
            or any(values[k] > ADJ_PRICE_MAX for k in _ADJ)
            or any(values[k] > VOLUME_MAX for k in _VOLUMES)
            or values["divCash"] > DIVIDEND_MAX or values["splitFactor"] > SPLIT_MAX):
        return "value_out_of_bounds"
    for o, h, lo, c in (_RAW, _ADJ):
        if not (values[lo] <= min(values[o], values[c]) and max(values[o], values[c]) <= values[h]):
            return "ohlc_order"
    # factor(field) = adj/raw; exact cross-multiplied comparison with close:
    # |adj*close - adjClose*raw| <= tol * adjClose * raw   (all positive).
    close, adj_close = Fraction(values["close"]), Fraction(values["adjClose"])
    for raw, adj in (("open", "adjOpen"), ("high", "adjHigh"), ("low", "adjLow")):
        r, a = Fraction(values[raw]), Fraction(values[adj])
        if abs(a * close - adj_close * r) > FACTOR_REL_TOL * adj_close * r:
            return "adjustment_factor"
    return None


def bar_problem(bar: Any) -> str | None:
    """Why a provider element is not a usable bar, or None if it is."""
    if not isinstance(bar, Mapping):
        return "not_an_object"
    if parse_bar_date(bar.get("date")) is None:
        return "malformed_date"
    return values_problem(bar)


def stored_problem(row: Mapping[str, Any]) -> str | None:
    """Why a stored row (eod_prices column names) is not a valid bar, or None."""
    return values_problem({_COL_TO_KEY[col]: row[col] for col in STORED_FIELDS if col in row})


def _median(values: list[Fraction]) -> Fraction | None:
    values = sorted(values)
    return values[len(values) // 2] if values else None


def _format_ratio(m: Fraction) -> str:
    """A positive exact ratio to six decimals, from integers only: fixed point
    for 1e-6 <= m < 1e9 (``2.000000``), scientific outside (``2.023767e+330``).
    Rounds half to even like ``format``; never converts to a float, so no
    ratio of two valid prices can overflow."""
    if Fraction(1, 10**6) <= m < 10**9:
        n = round(m * 10**6)
        return f"{n // 10**6}.{n % 10**6:06d}"
    e = len(str(m.numerator)) - len(str(m.denominator))      # within one of the exponent
    while m < Fraction(10) ** e:
        e -= 1
    while m >= Fraction(10) ** (e + 1):
        e += 1
    c = round(m / Fraction(10) ** e * 10**6)                  # 1_000_000 .. 10_000_000
    if c == 10**7:
        c, e = 10**6, e + 1
    return f"{c // 10**6}.{c % 10**6:06d}e{'+' if e >= 0 else '-'}{abs(e):02d}"


def _ratio(pairs: list[tuple[float, float]]) -> str:
    m = _median([Fraction(a) / Fraction(b) for a, b in pairs])
    return "n/a" if m is None else _format_ratio(m)


def validate_series(
    ticker: str,
    interval: tuple[_dt.date, _dt.date],
    provider_bars: Any,
    stored: Mapping[_dt.date, Mapping[str, Any]],
    calendar: SessionCalendar,
    accepted_gaps: frozenset[_dt.date] = frozenset(),
) -> Verdict:
    """Apply rules 1-14 (module docstring) and return the verdict."""
    start, end = interval
    digest = stored_digest(stored)

    def verdict(status: str, reason: str, rows: tuple = ()) -> Verdict:
        return Verdict(status, reason, rows, digest)

    if start > end:
        return verdict(VERDICT_INCOMPLETE, "empty_interval")
    try:
        expected = calendar.sessions(start, end)
    except ValueError:
        return verdict(VERDICT_INCOMPLETE, f"interval_outside_calendar: {start}..{end}")
    if not isinstance(provider_bars, list):
        return verdict(VERDICT_INCOMPLETE, "malformed_body")
    if not provider_bars:
        return verdict(VERDICT_INCOMPLETE, "empty_window")
    for i, bar in enumerate(provider_bars):
        problem = bar_problem(bar)
        if problem is not None:
            return verdict(VERDICT_INCOMPLETE, f"unusable_bar: {problem} at index {i}")
    bars = {parse_bar_date(b["date"]): b for b in provider_bars}
    if len(bars) != len(provider_bars):
        return verdict(VERDICT_INCOMPLETE,
                       f"duplicate_dates={len(provider_bars) - len(bars)}")
    outside = sorted(d for d in bars if d < start or d > end)
    if outside:
        return verdict(VERDICT_INCOMPLETE, f"bars_outside_request: first={outside[0]}")
    off = sorted(d for d in bars if d not in expected)
    if off:
        return verdict(VERDICT_INCOMPLETE, f"off_session_bar: {len(off)} first={off[0]}")

    for day in sorted(stored):
        problem = stored_problem(stored[day])
        if problem is not None:
            return verdict(VERDICT_CONFLICT, f"stored_bar_invalid: {problem} on {day}")
    shared = sorted(set(bars) & set(stored))
    raw_bad = [d for d in shared
               if not all(_within(bars[d][key], stored[d][col], STORED_REL_TOL)
                          for col, key in STORED_RAW)]
    if raw_bad:
        return verdict(VERDICT_CONFLICT, "raw_differs: ratio={} on {}/{} sessions".format(
            _ratio([(bars[d]["close"], stored[d]["close"]) for d in raw_bad]),
            len(raw_bad), len(shared)))
    stored_outside = sorted(d for d in stored if d < start or d > end)
    if stored_outside:
        return verdict(VERDICT_CONFLICT,
                       f"stored_outside_provider_range: {len(stored_outside)} stored sessions "
                       f"outside {start}..{end}, first={stored_outside[0]}")
    stored_off = sorted(d for d in stored if d not in expected)
    if stored_off:
        return verdict(VERDICT_CONFLICT,
                       f"stored_off_session: {len(stored_off)} first={stored_off[0]}")
    adj_bad = [d for d in shared
               if not all(_within(bars[d][key], stored[d][col], STORED_REL_TOL)
                          for col, key in STORED_ADJ)]
    if adj_bad:
        return verdict(VERDICT_REBASE, "adjusted_moved: ratio={} on {}/{} sessions".format(
            _ratio([(bars[d]["adjClose"], stored[d]["adj_close"]) for d in adj_bad]),
            len(adj_bad), len(shared)))
    absent = sorted(d for d in stored if d not in bars)
    if absent:
        return verdict(VERDICT_INCOMPLETE,
                       f"stored_sessions_missing={len(absent)} first={absent[0]}")
    holes = sorted((expected - accepted_gaps) - set(bars) - set(stored))
    if holes:
        return verdict(VERDICT_INCOMPLETE, f"sessions_missing={len(holes)} first={holes[0]}")
    new = sorted(d for d in bars if d not in stored)
    if not new:
        return verdict(VERDICT_COMPLETE, f"verified: {len(shared)} sessions, 0 missing")
    rows = tuple((ticker, d, *(bars[d][key] for _, key in EOD_FIELDS)) for d in new)
    return verdict(VERDICT_LOAD, f"inserted {len(rows)} missing sessions", rows)
