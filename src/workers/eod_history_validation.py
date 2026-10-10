"""Whole-ticker validation of a provider price series against stored rows.

``validate_series`` is the ONLY place that decides whether a Tiingo daily
series for one ticker may be written to ``eod_prices`` and certified
``history_complete`` (see ``eod_prices_warmer.promote``). It is pure: no
database, no network, no clock. Its inputs are the interval fixed before the
fetch, the provider's raw bars exactly as returned, the stored rows in scope,
a session calendar and an (empty by default) set of evidenced session gaps.

Rules, applied in this order; the first that fails decides the verdict:

 1. ``start > end`` → incomplete ``empty_interval``.
 2. The interval must lie inside the calendar, else incomplete
    ``interval_outside_calendar`` (checked on the interval itself, whatever
    the response holds).
 3. An empty response → incomplete ``empty_window``; a non-list body →
    incomplete ``malformed_body``.
 4. Every element must be a usable bar, else incomplete ``unusable_bar``:
    a mapping whose ``date`` is exactly ``YYYY-MM-DD`` or Tiingo's daily
    convention ``YYYY-MM-DDT00:00:00[.000]Z`` (midnight UTC naming the
    trading date; nothing is truncated or coerced); every required field a
    finite number (not a bool, not a string); prices positive; volumes and
    dividend non-negative; split factor positive; raw and adjusted
    ``low <= min(open, close) <= max(open, close) <= high``; and the
    adjustment factors implied by adjOpen/open, adjHigh/high and adjLow/low
    within ``FACTOR_REL_TOL`` of adjClose/close.
 5. A date returned twice → incomplete ``duplicate_dates``.
 6. A bar outside the interval → incomplete ``bars_outside_request``.
 7. A bar on a non-session → incomplete ``off_session_bar``.
 8. Any shared (stored and returned) date whose raw open/high/low/close
    differs beyond ``STORED_REL_TOL`` → conflict ``raw_differs``.
 9. A stored date outside the interval → conflict
    ``stored_outside_provider_range``; a stored date on a non-session →
    conflict ``stored_off_session``.
10. Any shared date whose adjusted open/high/low/close differs beyond
    ``STORED_REL_TOL`` → ``adjustment_rebase_required`` (``adjusted_moved``,
    with the median observed adjClose ratio). Checked whether or not anything
    is missing: a mixed adjusted basis is never complete.
11. A stored date the response does not contain → incomplete
    ``stored_sessions_missing``.
12. A session of the interval (minus ``accepted_gaps``) in neither the store
    nor the response → incomplete ``sessions_missing`` — interior sessions as
    much as the ends.
13. Otherwise the returned dates are exactly the interval's sessions: nothing
    new → ``complete_nothing_to_insert``; else ``load`` with exactly the rows
    for the returned dates that are not stored.

Every verdict carries ``digest``, a hash of the stored snapshot it judged;
promotion re-reads the store under a lock and refuses if it changed.
"""

from __future__ import annotations

import datetime as _dt
import functools
import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

VERDICT_COMPLETE = "complete_nothing_to_insert"
VERDICT_LOAD = "load"
VERDICT_INCOMPLETE = "history_incomplete"
VERDICT_CONFLICT = "history_conflict"
VERDICT_REBASE = "adjustment_rebase_required"
SUCCESS_VERDICTS = frozenset({VERDICT_COMPLETE, VERDICT_LOAD})

# Stored rows and provider bars are both Tiingo values: they must agree to
# floating-point noise.
STORED_REL_TOL = 1e-6
# Within one bar, adjOpen/open, adjHigh/high, adjLow/low and adjClose/close
# must name the same adjustment factor. Tiingo rounds adjusted fields (often
# to three decimals), so real bars disagree slightly: measured on 2,126,745
# stored production rows (2026-10-10) the largest deviation was 1.7e-3 (0.17%,
# AAMRQ), and 192 rows exceeded 1e-6. 1% accepts all of them and still rejects
# a misapplied split (0.5 vs 1.0) or the gate's 1-vs-900 bar. Sub-dollar
# prices rounded to three decimals could exceed it; none do in that sample.
FACTOR_REL_TOL = 1e-2

_DATE = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})(?:T00:00:00(?:\.000)?Z)?")
_RAW = ("open", "high", "low", "close")
_ADJ = ("adjOpen", "adjHigh", "adjLow", "adjClose")
_OTHER = ("volume", "adjVolume", "divCash", "splitFactor")
# eod_prices column order of the price fields, and the bar key for each.
EOD_FIELDS: tuple[tuple[str, str], ...] = (
    ("open", "open"), ("high", "high"), ("low", "low"), ("close", "close"),
    ("volume", "volume"), ("adj_open", "adjOpen"), ("adj_high", "adjHigh"),
    ("adj_low", "adjLow"), ("adj_close", "adjClose"), ("adj_volume", "adjVolume"),
    ("div_cash", "divCash"), ("split_factor", "splitFactor"),
)
# Stored fields compared with the response (eod_prices column → bar key).
STORED_RAW = (("open", "open"), ("high", "high"), ("low", "low"), ("close", "close"))
STORED_ADJ = (("adj_open", "adjOpen"), ("adj_high", "adjHigh"),
              ("adj_low", "adjLow"), ("adj_close", "adjClose"))
STORED_FIELDS = tuple(col for col, _ in STORED_RAW + STORED_ADJ)


class SessionCalendar(Protocol):
    def sessions(self, first: _dt.date, last: _dt.date) -> frozenset[_dt.date]:
        """Sessions in ``[first, last]``; ``ValueError`` outside the calendar."""


class XnysCalendar:
    """NYSE sessions from ``exchange_calendars`` (pinned in requirements.txt),
    built from 1900 so it spans every Tiingo history (SONY 1974, NVO 1982)."""

    def __init__(self) -> None:
        import exchange_calendars as xcals

        self._cal = xcals.get_calendar("XNYS", start="1900-01-01")
        self.first = self._cal.first_session.date()
        self.last = self._cal.last_session.date()

    def sessions(self, first: _dt.date, last: _dt.date) -> frozenset[_dt.date]:
        if first < self.first or last > self.last:
            raise ValueError(f"{first}..{last} is outside XNYS {self.first}..{self.last}")
        if first > last:
            return frozenset()
        return frozenset(ts.date() for ts in self._cal.sessions_in_range(
            first.isoformat(), last.isoformat()))


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


def stored_digest(stored: Mapping[_dt.date, Mapping[str, float]]) -> str:
    """Order-independent hash of the stored rows (dates and compared fields)."""
    h = hashlib.sha256()
    for day in sorted(stored):
        values = stored[day]
        h.update(day.isoformat().encode())
        for col in STORED_FIELDS:
            h.update(b"|" + repr(float(values[col])).encode())
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
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value))


def bar_problem(bar: Any) -> str | None:
    """Why a provider element is not a usable bar, or None if it is."""
    if not isinstance(bar, Mapping):
        return "not_an_object"
    if parse_bar_date(bar.get("date")) is None:
        return "malformed_date"
    for key in _RAW + _ADJ + _OTHER:
        if key not in bar:
            return f"missing_{key}"
        if not _finite(bar[key]):
            return f"non_numeric_{key}"
    if any(bar[k] <= 0 for k in _RAW + _ADJ) or bar["splitFactor"] <= 0:
        return "non_positive"
    if bar["volume"] < 0 or bar["adjVolume"] < 0 or bar["divCash"] < 0:
        return "negative"
    for o, h, lo, c in (_RAW, _ADJ):
        if not (bar[lo] <= min(bar[o], bar[c]) and max(bar[o], bar[c]) <= bar[h]):
            return "ohlc_order"
    factor = bar["adjClose"] / bar["close"]
    for raw, adj in (("open", "adjOpen"), ("high", "adjHigh"), ("low", "adjLow")):
        if abs(bar[adj] / bar[raw] - factor) > FACTOR_REL_TOL * factor:
            return "adjustment_factor"
    return None


def _same(a: float, b: float) -> bool:
    return abs(a - b) <= STORED_REL_TOL * max(abs(a), abs(b))


def _median(values: list[float]) -> float | None:
    values = sorted(values)
    return values[len(values) // 2] if values else None


def _ratio(values: list[float]) -> str:
    m = _median(values)
    return "n/a" if m is None else f"{m:.6f}"


def validate_series(
    ticker: str,
    interval: tuple[_dt.date, _dt.date],
    provider_bars: Any,
    stored: Mapping[_dt.date, Mapping[str, float]],
    calendar: SessionCalendar,
    accepted_gaps: frozenset[_dt.date] = frozenset(),
) -> Verdict:
    """Apply rules 1-13 (module docstring) and return the verdict."""
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

    shared = sorted(set(bars) & set(stored))
    raw_bad = [d for d in shared
               if not all(_same(bars[d][key], stored[d][col]) for col, key in STORED_RAW)]
    if raw_bad:
        return verdict(VERDICT_CONFLICT, "raw_differs: ratio={} on {}/{} sessions".format(
            _ratio([bars[d]["close"] / stored[d]["close"] for d in raw_bad]),
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
               if not all(_same(bars[d][key], stored[d][col]) for col, key in STORED_ADJ)]
    if adj_bad:
        return verdict(VERDICT_REBASE, "adjusted_moved: ratio={} on {}/{} sessions".format(
            _ratio([bars[d]["adjClose"] / stored[d]["adj_close"] for d in adj_bad]),
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
