"""Recognize one-session provider publication lag without changing admission.

A successful, current-target Tiingo response ending at the previous governed
session proves that the provider has not supplied the due session for that
instrument. A majority of those observations can defer a publication. Missing
history, old attempts and failures are never affirmative evidence. A fresh
failed attempt changes a majority-pending outcome to an explicit blocking
failure so ingestion failures remain visible without replacing the snapshot.
This is only scheduling: every existing snapshot pin and expiry still applies.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from typing import Any

from psycopg.rows import dict_row

from src.workers._nav_coverage import DEFAULT_MIN_ACTIVE_SHARE


_SESSION_SQL = """
WITH members AS (
    SELECT f.instrument_id, e.fund_status,
           e.valuation_frequency = 'daily' AND e.identity_verified
             AND e.return_basis_verified AND e.currency_verified AS eligible
    FROM funds_profile_mv f
    LEFT JOIN LATERAL (
        SELECT fund_status, valuation_frequency, identity_verified,
               return_basis_verified, currency_verified
        FROM nav_instrument_policy_evidence
        WHERE instrument_id = f.instrument_id
          AND policy_id = %(policy_id)s AND policy_version = %(policy_version)s
          AND known_at <= %(at)s AND effective_at <= %(at)s AND recorded_at <= %(at)s
        ORDER BY effective_at DESC, known_at DESC, recorded_at DESC, evidence_id DESC
        LIMIT 1
    ) e ON true
), due AS (
    SELECT nav_due_at FROM nav_valuation_schedules
    WHERE calendar_id = %(calendar_id)s AND calendar_version = %(calendar_version)s
      AND session_date = %(due)s
), observed AS (
    SELECT m.*,
           COALESCE(m.eligible AND n.last_nav = %(previous)s
             AND a.newest_observed_date = %(previous)s
             AND a.status IN ('success_new', 'success_no_new')
             AND a.run_status = 'completed' AND a.operation = 'normal'
             AND a.requested_start <= %(previous)s AND a.requested_end >= %(due)s
             AND a.attempted_at >= due.nav_due_at AND a.finished_at <= %(at)s
             AND a.finished_at >= a.attempted_at AND a.persisted_at >= a.finished_at
             AND a.row_count > 0 AND a.reason_code IS NULL, false) AS pending,
           COALESCE(a.requested_end >= %(due)s AND a.persisted_at >= due.nav_due_at
             AND (a.status NOT IN ('success_new', 'success_no_new', 'not_due')
                  OR a.run_status <> 'completed'), false) AS failed_attempt,
           COALESCE(n.last_nav >= %(due)s AND n.last_nav <= %(closed)s, false) AS current
    FROM members m CROSS JOIN due
    LEFT JOIN LATERAL (
        SELECT max(nav_date) AS last_nav FROM nav_timeseries
        WHERE instrument_id = m.instrument_id
    ) n ON m.fund_status = 'ACTIVE'
    LEFT JOIN LATERAL (
        SELECT a.*, r.status AS run_status, r.operation
        FROM nav_ingestion_attempts a JOIN nav_ingestion_runs r USING (run_id)
        WHERE a.instrument_id = m.instrument_id AND a.provider = 'tiingo'
          AND a.persisted_at <= %(at)s
          AND (%(run_id)s::uuid IS NULL OR a.run_id = %(run_id)s::uuid)
        ORDER BY a.persisted_at DESC, a.attempted_at DESC NULLS LAST, a.run_id DESC
        LIMIT 1
    ) a ON m.fund_status = 'ACTIVE'
)
SELECT count(*)::int AS instrument_count,
       count(*) FILTER (WHERE fund_status = 'ACTIVE')::int AS active_count,
       count(*) FILTER (WHERE fund_status = 'ACTIVE' AND pending)::int AS pending_count,
       count(*) FILTER (WHERE fund_status = 'ACTIVE' AND current)::int AS current_count,
       count(*) FILTER (WHERE fund_status = 'ACTIVE' AND failed_attempt)::int
           AS failed_attempt_count
FROM observed
"""


def assess_provider_session(
    conn,
    policy: Mapping[str, Any],
    grid: Sequence[dt.date],
    closed: dt.date,
    decision_at: dt.datetime,
    *,
    ingestion_run_id: str | None = None,
    min_active_share: float = DEFAULT_MIN_ACTIVE_SHARE,
) -> dict[str, Any]:
    """Read only; use an already verified policy/grid in the same transaction.

    The chain limits evidence to its just-finished ingestion. A read-only rebase
    plan uses the newest recorded attempt per instrument. Both require observations
    made after the due time, and only a single missing session can count. A minority
    with older/missing history remains excluded, rather than forcing publication of
    a mostly stale snapshot. Actual failed attempts require an explicit failure;
    catalog regressions retain the caller's ordinary coverage alarm behavior.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            _SESSION_SQL,
            {
                "policy_id": policy["policy_id"],
                "policy_version": policy["policy_version"],
                "calendar_id": policy["calendar_id"],
                "calendar_version": policy["calendar_version"],
                "at": decision_at,
                "due": grid[-1],
                "previous": grid[-2],
                "closed": closed,
                "run_id": ingestion_run_id,
            },
        )
        counts = cur.fetchone()
    cohort, active = counts["instrument_count"], counts["active_count"]
    majority_pending = bool(
        active > 0
        and cohort > 0
        and active / cohort >= min_active_share
        and counts["pending_count"] * 2 > active
    )
    return {
        **counts,
        "as_of_session": grid[-1].isoformat(),
        "previous_session": grid[-2].isoformat(),
        "majority_pending": majority_pending,
        "pending": majority_pending and counts["failed_attempt_count"] == 0,
    }
