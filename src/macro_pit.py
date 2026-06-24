# src/macro_pit.py
"""Point-in-time reads over macro_observation_vintage.

Answers "what did the system know at decision_time?" — for each series and
observation period, the value from the latest vintage whose available_at is at or
before decision_time. This is the contract A2's classifier consumes; it never
forward-fills beyond what was actually published.
"""
from __future__ import annotations

import datetime as _dt

_PIT_SQL = (
    "SELECT DISTINCT ON (series_id, observation_period) "
    "       series_id, observation_period, value "
    "FROM macro_observation_vintage "
    "WHERE series_id = ANY(%s) AND available_at <= %s "
    "ORDER BY series_id, observation_period, available_at DESC"
)


def latest_vintage_as_of(
    conn, series_ids: list[str], decision_time: _dt.datetime
) -> dict[str, dict[_dt.date, float]]:
    """Per series, {observation_period: value-as-known-at-decision_time}."""
    out: dict[str, dict[_dt.date, float]] = {sid: {} for sid in series_ids}
    with conn.cursor() as cur:
        cur.execute(_PIT_SQL, (list(series_ids), decision_time))
        for series_id, period, value in cur.fetchall():
            out.setdefault(series_id, {})[period] = float(value)
    return out
