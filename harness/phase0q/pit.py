"""In-memory point-in-time (PIT) vintage selection over pack-v2 rows.

Faithful reimplementation of the DISTINCT-ON selection semantics of
``src/macro_pit.py`` ``latest_vintage_as_of`` (the ``_PIT_SQL`` query, read-only
reference — never modified):

    SELECT DISTINCT ON (series_id, observation_period)
           series_id, observation_period, value
    FROM macro_observation_vintage
    WHERE series_id = ANY(%s) AND available_at <= %s
    ORDER BY series_id, observation_period, available_at DESC

i.e. for each ``(series_id, observation_period)`` keep the row from the LATEST
``available_at`` that is at or before ``decision_time`` (never forward-filling
beyond what was actually published). This module operates on the pack-v2
``macro_observation_vintage`` rows (list of dicts) instead of a DB cursor, and
returns the exact same ``{series_id: {observation_period(date): value(float)}}``
shape ``latest_vintage_as_of`` returns.

Ties on ``available_at`` (same-timestamp vintages for one period) are resolved
deterministically: the row with the larger ``vintage_date`` then larger
``revision_number`` wins, so the pure-python leg is order-independent of the input
row order (SQL ``DISTINCT ON`` leaves same-key ties unordered; the harness pins a
total order so two runs are byte-identical).
"""

from __future__ import annotations

import bisect
import datetime as _dt
from typing import Any, Iterable, Mapping

# Column names in the pack-v2 macro_observation_vintage rows.
_SERIES = "series_id"
_PERIOD = "observation_period"
_VALUE = "value"
_AVAILABLE_AT = "available_at"
_VINTAGE_DATE = "vintage_date"
_REVISION = "revision_number"


def _parse_available_at(raw: str) -> _dt.datetime:
    """Parse an ISO-8601 ``available_at`` string to an aware ``datetime`` (UTC).

    Pack-v2 stores ``available_at`` as e.g. ``"2014-02-19T00:00:00+00:00"``. A
    naive value (no tzinfo) is treated as UTC so it compares against the
    tz-aware ``decision_time`` the worker uses.
    """
    value = _dt.datetime.fromisoformat(raw)
    if value.tzinfo is None:
        value = value.replace(tzinfo=_dt.timezone.utc)
    return value


def _parse_date(raw: str) -> _dt.date:
    return _dt.date.fromisoformat(raw[:10])


def _tie_key(row: Mapping[str, Any]) -> tuple:
    """Deterministic ordering key for rows sharing (series, period).

    Mirrors the SQL ``ORDER BY ... available_at DESC`` primary sort, then adds a
    stable tie-break (vintage_date, revision_number) so equal-``available_at``
    vintages resolve identically on every run regardless of input order.
    """
    available_at = _parse_available_at(row[_AVAILABLE_AT])
    vintage_date = _parse_date(row[_VINTAGE_DATE]) if row.get(_VINTAGE_DATE) else _dt.date.min
    revision = row.get(_REVISION)
    revision = float(revision) if revision is not None else float("-inf")
    return (available_at, vintage_date, revision)


def latest_vintage_as_of(
    rows: Iterable[Mapping[str, Any]],
    series_ids: list[str],
    decision_time: _dt.datetime,
) -> dict[str, dict[_dt.date, float]]:
    """Per series, ``{observation_period: value-as-known-at-decision_time}``.

    In-memory equivalent of ``src.macro_pit.latest_vintage_as_of`` over pack-v2
    ``rows``. ``series_ids`` restricts the basket (like the SQL ``ANY(%s)``
    filter); ``decision_time`` is the tz-aware PIT cutoff (``available_at <=``).
    """
    wanted = set(series_ids)
    # Per (series, period): the currently-winning row (max _tie_key).
    best: dict[str, dict[_dt.date, tuple]] = {sid: {} for sid in series_ids}
    for row in rows:
        sid = row[_SERIES]
        if sid not in wanted:
            continue
        available_at = _parse_available_at(row[_AVAILABLE_AT])
        if available_at > decision_time:
            continue
        period = _parse_date(row[_PERIOD])
        key = _tie_key(row)
        current = best[sid].get(period)
        if current is None or key > current[0]:
            best[sid][period] = (key, float(row[_VALUE]))

    out: dict[str, dict[_dt.date, float]] = {sid: {} for sid in series_ids}
    for sid, periods in best.items():
        for period, (_key, value) in periods.items():
            out[sid][period] = value
    return out


class PitIndex:
    """Pre-indexed PIT store for fast repeated ``latest_vintage_as_of`` reads.

    The naive scan above is O(rows) per query; the harness calls the PIT read ~74
    times per decision (2 axes x 37 history look-backs) x ~148 months, so the naive
    path would rescan the whole store thousands of times. This index groups rows by
    ``(series_id, observation_period)`` and pre-sorts each group's vintages by the
    SAME deterministic ``_tie_key`` (available_at, vintage_date, revision). A query
    then bisects each group for the latest vintage whose ``available_at`` is at or
    before ``decision_time``. The RESULT is byte-identical to
    ``latest_vintage_as_of`` (asserted by a parity test); only the algorithm differs.
    """

    def __init__(self, rows: Iterable[Mapping[str, Any]]):
        # Per (series, period): parallel ascending arrays of available_at and value,
        # sorted by the full deterministic tie_key so the rightmost element with
        # available_at <= cutoff is exactly the DISTINCT-ON winner.
        grouped: dict[str, dict[_dt.date, list[tuple]]] = {}
        for row in rows:
            sid = row[_SERIES]
            period = _parse_date(row[_PERIOD])
            grouped.setdefault(sid, {}).setdefault(period, []).append(
                (_tie_key(row), float(row[_VALUE])))
        self._avail: dict[str, dict[_dt.date, list[_dt.datetime]]] = {}
        self._value: dict[str, dict[_dt.date, list[float]]] = {}
        for sid, periods in grouped.items():
            self._avail[sid] = {}
            self._value[sid] = {}
            for period, entries in periods.items():
                entries.sort(key=lambda e: e[0])
                self._avail[sid][period] = [key[0] for key, _ in entries]
                self._value[sid][period] = [value for _, value in entries]

    def latest_vintage_as_of(
        self, series_ids: list[str], decision_time: _dt.datetime,
    ) -> dict[str, dict[_dt.date, float]]:
        out: dict[str, dict[_dt.date, float]] = {sid: {} for sid in series_ids}
        for sid in series_ids:
            periods = self._avail.get(sid)
            if not periods:
                continue
            values = self._value[sid]
            for period, avails in periods.items():
                idx = bisect.bisect_right(avails, decision_time) - 1
                if idx >= 0:
                    out[sid][period] = values[period][idx]
        return out
