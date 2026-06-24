"""macro_vintage worker — point-in-time vintage ingestion for the macro quadrant.

Fetches each basket series from ALFRED (output_type=2 = all vintages in one call),
compresses to real revisions (a new row only when the value changes across vintage
dates), and upserts idempotently into macro_observation_vintage (vintages are
immutable -> ON CONFLICT DO NOTHING). Reuses the FRED TokenBucket. The latest-
revision macro_data table is untouched.
"""
from __future__ import annotations

import datetime as _dt
import math
import re
from typing import Any

_VINTAGE_COL = re.compile(r"_(\d{8})$")
_MISSING = frozenset((".", "#N/A", "", "NaN", "nan", "null", "None"))


def parse_alfred_vintages(series_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """ALFRED output_type=2 JSON -> compressed vintage rows (one per real revision).

    Columns named ``<SERIES>_YYYYMMDD`` carry the value as known on that vintage
    date; non-vintage columns (e.g. ``date``) and missing markers are skipped.
    Within each observation period, vintages are sorted by date and a row is
    emitted only when the value differs from the previous kept value.
    """
    by_period: dict[_dt.date, list[tuple[_dt.date, float]]] = {}
    for obs in payload.get("observations", []):
        try:
            period = _dt.date.fromisoformat(obs["date"])
        except (KeyError, ValueError):
            continue
        for col, raw in obs.items():
            m = _VINTAGE_COL.search(col)
            if not m:
                continue
            s = str(raw).strip()
            if s in _MISSING:
                continue
            try:
                v = float(s)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(v):
                continue
            vd = _dt.datetime.strptime(m.group(1), "%Y%m%d").date()
            by_period.setdefault(period, []).append((vd, v))

    rows: list[dict[str, Any]] = []
    for period in sorted(by_period):
        last_val: float | None = None
        rev = 0
        for vd, v in sorted(by_period[period], key=lambda t: t[0]):
            if last_val is None or v != last_val:
                rows.append({
                    "series_id": series_id, "observation_period": period,
                    "vintage_date": vd, "value": v, "revision_number": rev,
                })
                last_val = v
                rev += 1
    return rows
