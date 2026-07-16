# src/quadrant_market_observation.py
"""The market-implied GROWTH observation for the fused v3 model — ONE builder
shared by the production worker (DB eod_prices rows) and the harness replay
(certified-pack eod rows), so parity holds by construction.

Frozen conventions only (no new calibration surface):
  raw_t = SPY adjusted-close 126-business-day return at month-end t
          (quadrant_market.WINDOW — the preserved A2 challenger's growth proxy)
  z_t   = robust_z_10y over the trailing 10 years of month-end raw values
          (macro_transforms.standardize: clip((x-med)/(1.4826*MAD), ±4)),
          requiring >= MIN_MARKET_HISTORY_MONTHS distinct months, else None.

PIT discipline: only sessions dated <= the month-end feed that month-end's
observation (an exchange close is available same-day; hard_max_age for a daily
source is the market worker's frozen 3-business-day rule, enforced upstream by
the worker's read window, not here).
"""
from __future__ import annotations

import bisect
import datetime as _dt
from typing import Any, Mapping, Sequence

from src.macro_transforms import standardize

MARKET_GROWTH_TICKER = "SPY"
MARKET_WINDOW_BD = 126           # quadrant_market.WINDOW (frozen challenger)
MARKET_Z_WINDOW_YEARS = 10       # robust_z_10y (frozen macro standardizer window)
MIN_MARKET_HISTORY_MONTHS = 24   # MIN_UNCERTAINTY_VINTAGES discipline
_STANDARDIZER_ID = "robust_z_10y_distinct_vintages_v1"


def market_growth_observation_series(
    eod_rows: Sequence[Mapping[str, Any]],
    month_ends: Sequence[_dt.date],
) -> list[tuple[float | None, float | None]]:
    """(z, quality) per requested month-end, PIT from eod rows.

    ``eod_rows`` need ``ticker``, ``date`` (date or ISO string) and
    ``adjusted_close``. Quality is 1.0 whenever the observation exists (a daily
    exchange print carries the v1 market-worker's full quality seeds).
    """
    sessions = sorted(
        (row["date"] if isinstance(row["date"], _dt.date)
         else _dt.date.fromisoformat(row["date"]), float(row["adjusted_close"]))
        for row in eod_rows
        if row.get("ticker") == MARKET_GROWTH_TICKER
        and row.get("adjusted_close") is not None
        and float(row["adjusted_close"]) > 0)
    dates = [d for d, _ in sessions]
    levels = [v for _, v in sessions]

    def raw_at(as_of: _dt.date) -> float | None:
        idx = bisect.bisect_right(dates, as_of) - 1
        if idx < MARKET_WINDOW_BD:
            return None
        then = levels[idx - MARKET_WINDOW_BD]
        return (levels[idx] / then - 1.0) if then > 0 else None

    raw_by_month: dict[_dt.date, float | None] = {t: raw_at(t) for t in month_ends}
    out: list[tuple[float | None, float | None]] = []
    for t in month_ends:
        current = raw_by_month[t]
        if current is None:
            out.append((None, None))
            continue
        cutoff = _dt.date(t.year - MARKET_Z_WINDOW_YEARS, t.month, 1)
        history = [raw_by_month[m] for m in month_ends
                   if cutoff <= m <= t and raw_by_month[m] is not None]
        if len(set(history)) < MIN_MARKET_HISTORY_MONTHS:
            out.append((None, None))
            continue
        z = standardize(_STANDARDIZER_ID, history, current)
        out.append((z, 1.0) if z is not None else (None, None))
    return out
