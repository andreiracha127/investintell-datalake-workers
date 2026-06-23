"""Reprocess NAV glitches for affected funds: read existing nav_timeseries,
sanitize, upsert corrected (nav, return_1d), then refresh cagg_nav_daily.

Operates on existing rows (NOT a Tiingo re-fetch) so it is deterministic. The
default selection is every instrument with any ``abs(return_1d) > 1.0`` (the 279
funds). ``--dry-run`` reports per-fund changes without writing.

Write strategy: only the rows whose ``nav`` OR ``return_1d`` actually change are
upserted (a repaired point i and the following point i+1, whose return depends on
i). Untouched rows keep their original ``source`` / provenance. The cagg refresh
window is the touched span, end-exclusive +1 day to cover the last daily bucket.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import math
import os

from src.db import connect
from src.workers._nav_sanitize import SanitizeResult, sanitize_nav_series

SELECT_AFFECTED = """
SELECT DISTINCT instrument_id FROM nav_timeseries
WHERE abs(return_1d) > 1.0
"""
SELECT_ROWS = """
SELECT nav_date, nav FROM nav_timeseries
WHERE instrument_id = %s AND nav IS NOT NULL ORDER BY nav_date
"""
UPSERT = """
INSERT INTO nav_timeseries (instrument_id, nav_date, nav, return_1d, return_type, currency, source)
VALUES (%s, %s, %s, %s, 'log', COALESCE(%s, 'USD'), 'reprocess')
ON CONFLICT (instrument_id, nav_date) DO UPDATE SET nav = EXCLUDED.nav, return_1d = EXCLUDED.return_1d
"""


def plan_repairs(
    rows_by_fund: dict[object, list[tuple[_dt.date, float]]],
) -> dict[object, SanitizeResult]:
    """Per-fund SanitizeResult for already-loaded (nav_date, nav) rows (pure)."""
    return {fid: sanitize_nav_series(rows) for fid, rows in rows_by_fund.items()}


def run(dsn: str, *, dry_run: bool = True, fund_ids: list | None = None) -> dict:
    stats = {"funds": 0, "repaired_funds": 0, "rows_updated": 0, "dead": 0, "scale_step": 0}
    touched_min: _dt.date | None = None
    touched_max: _dt.date | None = None
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            if fund_ids is None:
                cur.execute(SELECT_AFFECTED)
                fund_ids = [r[0] for r in cur.fetchall()]
            for iid in fund_ids:
                cur.execute(SELECT_ROWS, (iid,))
                rows = [(d, float(n)) for d, n in cur.fetchall()]
                res: SanitizeResult = sanitize_nav_series(rows)
                stats["funds"] += 1
                if res.dead:
                    stats["dead"] += 1
                if res.scale_step:
                    stats["scale_step"] += 1
                if not any(res.repaired):
                    continue
                stats["repaired_funds"] += 1
                navs = res.nav
                # A repaired point i changes nav[i] AND the returns at i and i+1.
                affected: set[int] = set()
                for i, rep in enumerate(res.repaired):
                    if rep:
                        affected.add(i)
                        if i + 1 < len(navs):
                            affected.add(i + 1)
                updates = []
                prev: float | None = None
                for i, (d, _orig) in enumerate(rows):
                    nav = navs[i]
                    ret = round(math.log(nav / prev), 8) if prev else None
                    if i in affected:
                        updates.append((iid, d, round(nav, 6), ret, None))
                        touched_min = d if touched_min is None or d < touched_min else touched_min
                        touched_max = d if touched_max is None or d > touched_max else touched_max
                    prev = nav
                if not dry_run and updates:
                    cur.executemany(UPSERT, updates)
                    conn.commit()
                stats["rows_updated"] += sum(1 for rep in res.repaired if rep)
        if not dry_run and touched_min is not None and touched_max is not None:
            window_end = touched_max + _dt.timedelta(days=1)
            with connect(dsn, autocommit=True) as rconn, rconn.cursor() as rcur:
                rcur.execute(
                    "CALL refresh_continuous_aggregate('cagg_nav_daily', %s, %s)",
                    (touched_min, window_end),
                )
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    dsn = os.environ["DATALAKE_DB_URL"]
    out = run(dsn, dry_run=not args.apply)
    print(out)
