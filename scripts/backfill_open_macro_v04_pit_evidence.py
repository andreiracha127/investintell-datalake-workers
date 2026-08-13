"""Plan or explicitly apply Open Macro v04 PIT evidence history.

The default is a read-only dry run. Passing ``--apply`` is required before any
evidence relation is written. This script never refreshes source data and never
prints private values, exact cutoffs, vintages, or model inputs.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from typing import Any

from src.db import connect, resolve_dsn
from src.workers import open_macro_v04_pit_evidence as evidence


DECISION_MONTHS_SQL = (
    "SELECT as_of FROM open_macro_v04_decisions "
    "WHERE valid_status = 'valid' AND publish_state = 'published' "
    "AND as_of BETWEEN %(start)s AND %(end)s ORDER BY as_of"
)


def decision_months(conn, start: dt.date, end: dt.date) -> list[dt.date]:
    with conn.cursor() as cur:
        cur.execute(DECISION_MONTHS_SQL, {"start": start, "end": end})
        return [row[0] for row in cur.fetchall()]


def run(
    dsn: str,
    *,
    start: dt.date,
    end: dt.date,
    apply: bool = False,
) -> dict[str, Any]:
    if start > end:
        raise ValueError("start must be on or before end")
    conn = connect(dsn)
    try:
        evidence.pin_search_path(conn)
        # A dry run has no bootstrap phase.  Set isolation before its advisory
        # lock query so that query becomes the first statement of the one PIT
        # source snapshot (pg_try_advisory_lock itself starts a transaction).
        if not apply:
            evidence.begin_consistent_read(conn)
        with evidence.advisory_lock(
            conn, evidence.LOCK_OPEN_MACRO_V04_PIT_EVIDENCE
        ) as acquired:
            if not acquired:
                return {
                    "mode": "apply" if apply else "dry_run",
                    "status": "lock_busy",
                }
            if apply:
                # Session advisory locks survive COMMIT.  End the transaction
                # opened by pg_try_advisory_lock before bootstrap, then begin a
                # fresh repeatable-read snapshot for each published month.
                conn.commit()
                evidence.ensure_schema(conn)
                evidence.pin_search_path(conn)
            months = decision_months(conn, start, end)
            if apply:
                # decision_months is only the work list; source legs for each
                # output month receive their own post-bootstrap PIT snapshot.
                conn.commit()
            coverage = {"complete": 0, "partial": 0, "unavailable": 0}
            outcomes = {"would_publish": 0, "no_op": 0, "conflict": 0}
            published = 0
            for month in months:
                if apply:
                    evidence.begin_consistent_read(conn)
                materialization = evidence.materialize_from_connection(conn, month)
                coverage[materialization.header["coverage_state"]] += 1
                if apply:
                    result = evidence.publish(conn, materialization)
                    conn.commit()
                    published += int(result == "published")
                else:
                    outcomes[evidence.publication_outcome(conn, materialization)] += 1
            summary: dict[str, Any] = {
                "mode": "apply" if apply else "dry_run",
                "decisions": len(months),
                **coverage,
            }
            if apply:
                summary["published"] = published
            else:
                summary.update(outcomes)
            return summary
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=dt.date.fromisoformat, required=True)
    parser.add_argument("--end", type=dt.date.fromisoformat, required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write immutable snapshots; omitted means a read-only dry run.",
    )
    args = parser.parse_args()
    print(
        json.dumps(
            run(resolve_dsn(), start=args.start, end=args.end, apply=args.apply),
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
