"""Identifier coverage of ``sec_nport_holdings``, measured per ``report_date``.

WHY THIS EXISTS
---------------
``sec_nport_holdings`` is loaded quarterly, one DERA package at a time, by an
operator script that lives OUTSIDE this repository. A package whose ISIN side
was lost during that parse still loads: every row arrives, every other column is
populated, and the only visible difference is that holdings that should carry
``cusip = 'IS:<isin>'`` plus a populated ``isin`` arrive as ``cusip = 'LE:<lei>'``
with ``isin`` NULL. Nothing fails. The table just quietly stops being joinable
for a slice of history.

That has already happened twice, and neither time was noticed by anything in
this repository:

===================  ===========  ==========================================
report_date          isin filled  DERA package the filings belong to
===================  ===========  ==========================================
2023-08-31           60.9 %       2023q4
2023-09-29           16.0 %       2023q4
2023-09-30           15.6 %       2023q4
2023-10-31           15.2 %       2023q4
2024-11-29           62.0 %       2025q1
2024-11-30           51.3 %       2025q1
2024-12-31           27.3 %       2025q1
2025-01-31           28.0 %       2025q1
===================  ===========  ==========================================

The other 92 ``report_date`` values with at least 1 000 rows (2019-09-30 through
2026-01-31) sit between **94.45 %** and **99.26 %**, median 98.50 %. The loss is
scoped to the PACKAGE, not to the report_date: the same report_date served by a
neighbouring package is clean (e.g. 2023-10-31 is 13.6 % for the filings that
landed in 2023q4 and 98.3 % for the ones that landed in 2024q1).

WHY THE ISIN COLUMN AND NOT "IS THE ROW IDENTIFIABLE"
-----------------------------------------------------
The consumer-visible symptom is a row with no usable identifier at all, so that
is the tempting thing to gate on. It does not separate. Over the same history the
share of rows carrying a CUSIP-9 **or** an ISIN **or** an ``IS:`` key bottoms out
at 96.57 % on a clean date and reaches 96.13 % on a degraded one -- 0.44 pp of
daylight, which is not a gate, it is a coin toss. The ``isin`` column's fill rate
separates the same two populations by **32.5 pp** (worst clean 94.45 % vs best
degraded 62.0 %), because most rows carry a real CUSIP and survive the loss with
their identifiability intact while their ISIN silently disappears.

So the gate is the ISIN fill rate and ``DEFAULT_FLOOR`` is 0.90: 4.45 pp below the
worst clean date ever observed and 28.0 pp above the best degraded one. Both
numbers are reported, because the second one is what the downstream peer/
look-through work actually feels.

CONTRACT
--------
``probe(conn, *, window_days, floor, min_rows) -> dict`` -- pure SELECT, never
raises on a degraded verdict, never writes. Repair procedure:
``docs/runbooks/nport-identifier-coverage.md``. It is called at the end of
``nport_lookthrough.run()`` (weekly, Sunday 04:00 UTC), which is the recurring
job that reads this table and the one whose output degrades first. There is no
new worker and no new cron: a quarterly load is detected within seven days.

The window is bounded on purpose. A load writes the report_dates of one package
plus late/amended filings for a few older ones, so only the tail can change;
scanning it costs one aggregate over ~7 report_dates (7.1M rows, measured at
8.4 s warm / 20.9 s cold against the production datalake) instead of a full pass
over 96M. The two historical windows above are OUTSIDE any sane tail and will
never be re-flagged by this probe -- they are a repair, not an alert.
"""
from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

TABLE = "sec_nport_holdings"

#: How far back from ``max(report_date)`` to look. A quarterly DERA package
#: carries three monthly report_dates, so 150 days always covers the most recent
#: package whole, plus the previous one's tail where amendments land.
DEFAULT_WINDOW_DAYS = 150
#: ISIN fill rate below which a report_date is called degraded. See the module
#: docstring for the separation this number sits in the middle of.
DEFAULT_FLOOR = 0.90
#: report_dates thinner than this are not judged. Off-cycle fiscal month-ends
#: carry a few dozen rows (2020-08-30 has 31, at 61.3 % fill) and one filer's
#: junk would otherwise read as a package failure.
DEFAULT_MIN_ROWS = 1000

#: The identifiability rule the peer/look-through work uses: a real CUSIP-9, or a
#: populated ISIN column, or the synthetic ``IS:<isin>`` key the bulk parser
#: writes when a holding has an ISIN but no CUSIP.
_IDENTIFIABLE = (
    "((coalesce(cusip, '') ~ '^[0-9A-Za-z]{9}$')"
    " OR (upper(trim(coalesce(isin, ''))) ~ '^[A-Z]{2}[0-9A-Za-z]{10}$')"
    " OR (upper(coalesce(cusip, '')) ~ '^IS:[A-Z]{2}[0-9A-Za-z]{10}$'))"
)

COVERAGE_SQL = f"""
SELECT report_date,
       count(*)                                                        AS n_rows,
       count(*) FILTER (WHERE isin IS NOT NULL AND isin <> '')          AS n_isin,
       count(*) FILTER (WHERE {_IDENTIFIABLE})                          AS n_identifiable
FROM {TABLE}
WHERE report_date > (SELECT max(report_date) FROM {TABLE})
                    - make_interval(days => %s)
GROUP BY 1
ORDER BY 1
"""


def _round(value: float) -> float:
    return round(value, 4)


def probe(
    conn: Any,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    floor: float = DEFAULT_FLOOR,
    min_rows: int = DEFAULT_MIN_ROWS,
) -> dict[str, Any]:
    """Measure identifier coverage over the recent tail of ``sec_nport_holdings``.

    Returns a structured verdict and never raises on a degraded reading. Three
    states, and the difference between the last two matters:

    * ``clean``       -- every judged report_date is at or above ``floor``.
    * ``degraded``    -- at least one is below it. A load lost its ISIN side.
    * ``undecidable`` -- the window held no report_date with ``min_rows`` rows.
      That is NOT a pass: it means the probe had nothing to judge, which itself
      says the table is empty or the load never landed.
    """
    with conn.cursor() as cur:
        cur.execute(COVERAGE_SQL, (window_days,))
        rows = cur.fetchall()

    checked: list[dict[str, Any]] = []
    for report_date, n_rows, n_isin, n_identifiable in rows:
        entry = {
            "report_date": str(report_date),
            "rows": n_rows,
            "isin_fill": _round(n_isin / n_rows) if n_rows else None,
            "identifiable_share": _round(n_identifiable / n_rows) if n_rows else None,
            "judged": n_rows >= min_rows,
        }
        entry["degraded"] = bool(entry["judged"] and entry["isin_fill"] < floor)
        checked.append(entry)

    judged = [e for e in checked if e["judged"]]
    degraded = [e for e in judged if e["degraded"]]
    state = "undecidable" if not judged else ("degraded" if degraded else "clean")

    verdict = {
        "state": state,
        "window_days": window_days,
        "floor": floor,
        "min_rows": min_rows,
        "report_dates_seen": len(checked),
        "report_dates_judged": len(judged),
        "degraded_report_dates": [e["report_date"] for e in degraded],
        "worst_isin_fill": min((e["isin_fill"] for e in judged), default=None),
        "checked": checked,
    }

    if state == "degraded":
        LOGGER.warning(
            "nport_identifier_coverage: %s lost its ISIN side on %s (floor=%.2f, "
            "readings=%s). The rows are there and every other column is populated, so "
            "nothing downstream will fail -- holdings that should carry cusip='IS:<isin>' "
            "carry cusip='LE:<lei>' with isin NULL instead, and stop joining. The fix is "
            "to re-parse and re-load the DERA package those filings belong to; a plain "
            "re-run does nothing, because the loader is ON CONFLICT (report_date, "
            "series_id, cusip) DO NOTHING and the bad rows already own the key.",
            TABLE,
            ", ".join(verdict["degraded_report_dates"]),
            floor,
            [(e["report_date"], e["isin_fill"]) for e in degraded],
        )
    elif state == "undecidable":
        LOGGER.warning(
            "nport_identifier_coverage: no report_date in the last %s days of %s has "
            "%s rows; coverage could not be judged. This is NOT a proof of coverage.",
            window_days, TABLE, min_rows,
        )
    return verdict
