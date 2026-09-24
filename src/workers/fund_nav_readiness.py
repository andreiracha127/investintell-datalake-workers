"""DB-first current daily NAV evidence; no provider reads or inferred calendar."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import uuid
from collections.abc import Mapping
from typing import Any

from psycopg.rows import dict_row

from src.db import LOCK_FUND_NAV_READINESS, advisory_lock, connect
from src.workers._nav_policy import (
    ENDPOINTS,
    FEATURE_DEFINITION_VERSION,
    PROFILE,
    CalendarKey,
    CalendarSessionProof,
    calendar_equivalence_digest,
    equivalence_mapping_digest,
    level_evidence_digest,
    load_calendar_equivalence,
    resolve_policy_and_grid,
)

SUCCESS_ATTEMPTS = ("success_new", "success_no_new")
INTERVALS = ENDPOINTS - 1
_NAV_FIELDS = """nav_date, nav, return_1d, return_type, source, source_nav, source_nav_kind,
    nav_repair_kind, return_start_date, return_source_boundary,
    return_uses_repaired_nav, return_semantics, return_verification_status,
    calendar_id, calendar_version, calendar_source, currency"""
_ROW_FIELDS = (
    "run_id",
    "instrument_id",
    "readiness_profile",
    "readiness_version",
    "policy_id",
    "policy_version",
    "policy_hash",
    "calendar_id",
    "calendar_version",
    "window_start",
    "window_end",
    "fund_status",
    "is_active",
    "status_known_at",
    "status_effective_at",
    "lifecycle_evidence_id",
    "valuation_frequency",
    "last_nav",
    "first_nav",
    "last_usable_return_end",
    "is_current",
    "missed_due_sessions",
    "missing_session_count",
    "observed_levels_count",
    "admissible_returns_count",
    "interval_compatible",
    "return_semantics",
    "identity_verified",
    "return_basis_verified",
    "currency_verified",
    "ingestion_run_id",
    "feature_evidence_id",
    "risk_run_id",
    "risk_input_fingerprint",
    "nav_revision_id",
    "risk_input_max_date",
    "feature_as_of",
    "cohort",
    "admissible",
    "reason_code",
    "input_fingerprint",
    "sample_id",
    "calendar_equivalence_digest",
)


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def sample_id(policy: dict, grid: list[dt.date]) -> str:
    return digest(
        {
            "profile": PROFILE,
            "policy_hash": policy["policy_hash"],
            "calendar": [policy["calendar_id"], policy["calendar_version"]],
            "grid401": [d.isoformat() for d in grid],
        }
    )


Equivalence = Mapping[CalendarKey, CalendarSessionProof]


def _calendar_valid(row: dict, equivalence: Equivalence) -> bool:
    stamp = (row["calendar_id"], row["calendar_version"], row["calendar_source"])
    return None not in stamp and (row["nav_date"], *stamp) in equivalence


def _valid_interval(
    previous: dict, current: dict, policy: dict, equivalence: Equivalence
) -> bool:
    """One declared interval; each level's stamp must have a session proof.

    ``equivalence`` is the immutable mapping from ``load_calendar_equivalence``
    (or its pure core); there is no literal current-stamp fallback.
    """
    if (
        current["return_start_date"] != previous["nav_date"]
        or current["return_semantics"] != policy["required_return_semantics"]
        or current["return_type"] != "log"
        or current["return_verification_status"] != "unverified"
        or current["return_source_boundary"] is not False
        or current["return_uses_repaired_nav"] is not False
    ):
        return False
    for row in (previous, current):
        if (
            row["source_nav_kind"] != policy["required_nav_kind"]
            or row["nav_repair_kind"] != "none"
            or row["source_nav"] is None
            or row["nav"] is None
            or not _calendar_valid(row, equivalence)
            or row["currency"] != policy["modeling_currency"]
        ):
            return False
        try:
            if float(row["source_nav"]) != float(row["nav"]):
                return False
        except (TypeError, ValueError, OverflowError):
            return False
    if (
        previous["source"] != current["source"]
        or previous["source_nav_kind"] != current["source_nav_kind"]
    ):
        return False
    try:
        a, b = float(previous["nav"]), float(current["nav"])
        stored = float(current["return_1d"])
        if not all(map(math.isfinite, (a, b, stored))) or min(a, b) <= 0:
            return False
        # NAV rounded to 6 decimals, log returns to 8; endpoint precision sets
        # the error bound rather than the maximum error in a historical sample.
        tolerance = 0.5e-8 + 0.5e-6 / a + 0.5e-6 / b
        return abs(math.log(b / a) - stored) <= tolerance
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        return False


def assess_instrument(
    instrument_id: Any,
    policy: dict,
    grid: list[dt.date],
    nav_rows: list[dict],
    latest_nav: dt.date | None,
    closed_session: dt.date,
    lifecycle: dict | None,
    attempt: dict | None,
    feature: dict | None,
    nav_input_matches: bool,
    earliest_nav: dt.date | None = None,
    nav_revision_id: int = 0,
    reexpression_hold: bool = False,
    revision_source_verified: bool = True,
    *,
    equivalence: Equivalence,
    lineage: list | None = None,
) -> dict[str, Any]:
    """Exact 401 observed levels / 400 declared intervals, not tail/dropna.

    ``equivalence`` validates each level's calendar stamp per date (rollover);
    ``lineage`` is the per-date revision attribution proof from
    ``_input_evidence`` and is bound into the evidence fingerprint.
    """
    status = lifecycle["fund_status"] if lifecycle else "UNKNOWN"
    frequency = lifecycle["valuation_frequency"] if lifecycle else "unknown"
    by_date = {row["nav_date"]: row for row in nav_rows}
    present = [d for d in grid if d in by_date and by_date[d]["nav"] is not None]
    missing = ENDPOINTS - len(present)
    missed_due = sum(latest_nav is None or date > latest_nav for date in grid)
    current = latest_nav is not None and latest_nav >= grid[-1] and grid[-1] in present
    compatible = len(present) == ENDPOINTS and len(by_date) == ENDPOINTS
    equivalence_digest = calendar_equivalence_digest(grid, by_date, equivalence)
    valid_returns = 0
    last_return = None
    if compatible:
        for start, end in zip(grid, grid[1:]):
            if _valid_interval(by_date[start], by_date[end], policy, equivalence):
                valid_returns += 1
                last_return = end
            else:
                compatible = False
    risk_end = feature["input_max_date"] if feature else None
    feature_as_of = feature["feature_as_of"] if feature else None
    evidence_ok = (
        feature is not None
        and feature["exclusion_reason"] is None
        and nav_input_matches
        and risk_end == grid[-1]
        and feature_as_of == grid[-1]
    )
    attempted = attempt is not None and attempt["status"] in SUCCESS_ATTEMPTS
    identity = bool(lifecycle and lifecycle["identity_verified"])
    basis = bool(lifecycle and lifecycle["return_basis_verified"])
    currency = bool(lifecycle and lifecycle["currency_verified"])
    # §12.2D: an absent kind is missing data; an explicit non-required kind or
    # a declared incompatible return convention is a known incompatibility,
    # reported even without lineage (never hidden behind "missing").
    kind_missing = any(row["source_nav_kind"] is None for row in nav_rows)
    semantics_incompatible = any(
        (
            row["source_nav_kind"] is not None
            and row["source_nav_kind"] != policy["required_nav_kind"]
        )
        or (
            row["nav_date"] != grid[0]
            and (
                row["return_type"] != "log"
                or (
                    row["return_semantics"] is not None
                    and row["return_semantics"] != policy["required_return_semantics"]
                )
            )
        )
        for row in nav_rows
    )
    native_currency = all(
        row["currency"] == policy["modeling_currency"] for row in nav_rows
    )
    reason = None
    if status == "INACTIVE":
        reason = "INACTIVE_FUND"
    elif status != "ACTIVE":
        reason = "UNKNOWN_FUND_STATUS"
    elif frequency != "daily":
        reason = "UNSUPPORTED_VALUATION_FREQUENCY"
    elif latest_nav is not None and latest_nav > closed_session:
        reason = "NAV_DATA_UNAVAILABLE"
    elif latest_nav is None or latest_nav < grid[-1]:
        reason = "NAV_STALE"
    elif not (identity and basis and currency):
        reason = "NAV_DATA_UNAVAILABLE"  # lifecycle evidence gate (preserved)
    elif kind_missing:
        reason = "NAV_DATA_UNAVAILABLE"
    elif semantics_incompatible:
        reason = "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    elif not (attempted and native_currency and revision_source_verified):
        reason = "NAV_DATA_UNAVAILABLE"  # attempt/row-evidence/lineage/FX gaps
    elif reexpression_hold or not compatible or missing or valid_returns != INTERVALS:
        reason = "RETURN_INTERVAL_INCOMPATIBLE"
    elif not evidence_ok:
        reason = "RETURN_SAMPLE_NOT_CURRENT"
    result = {
        "instrument_id": instrument_id,
        "readiness_profile": PROFILE,
        "readiness_version": 1,
        "policy_id": policy["policy_id"],
        "policy_version": policy["policy_version"],
        "policy_hash": policy["policy_hash"],
        "calendar_id": policy["calendar_id"],
        "calendar_version": policy["calendar_version"],
        "window_start": grid[0],
        "window_end": grid[-1],
        "fund_status": status,
        "is_active": True
        if status == "ACTIVE"
        else False
        if status == "INACTIVE"
        else None,
        "status_known_at": lifecycle["known_at"] if lifecycle else None,
        "status_effective_at": lifecycle["effective_at"] if lifecycle else None,
        "lifecycle_evidence_id": lifecycle["evidence_id"] if lifecycle else None,
        "valuation_frequency": frequency,
        "last_nav": latest_nav,
        "first_nav": earliest_nav
        if earliest_nav is not None
        else (nav_rows[0]["nav_date"] if nav_rows else None),
        "last_usable_return_end": last_return,
        "is_current": bool(current and latest_nav <= closed_session),
        "missed_due_sessions": missed_due,
        "missing_session_count": missing,
        "observed_levels_count": len(present),
        "admissible_returns_count": valid_returns,
        "interval_compatible": compatible and valid_returns == INTERVALS,
        "return_semantics": "observed_interval_log_ratio"
        if valid_returns == INTERVALS
        else None,
        "identity_verified": identity,
        "return_basis_verified": basis,
        "currency_verified": currency,
        "ingestion_run_id": attempt["run_id"] if attempt else None,
        "feature_evidence_id": (
            f"{instrument_id}:{feature['calc_date']}:{FEATURE_DEFINITION_VERSION}"
            if feature
            else None
        ),
        "risk_run_id": feature["risk_run_id"] if feature else None,
        "risk_input_fingerprint": feature["input_fingerprint"] if feature else None,
        "nav_revision_id": nav_revision_id,
        "risk_input_max_date": risk_end,
        "feature_as_of": feature_as_of,
        "cohort": "current_daily" if frequency == "daily" else None,
        "admissible": reason is None,
        "reason_code": reason,
        "calendar_equivalence_digest": equivalence_digest,
    }
    result["input_fingerprint"] = digest(
        {
            "policy": policy["policy_hash"],
            "grid": grid,
            "nav": nav_rows,
            "last_nav": latest_nav,
            "first_nav": result["first_nav"],
            "lifecycle": lifecycle,
            "attempt": attempt,
            "feature": feature,
            "nav_input_matches": nav_input_matches,
            "nav_revision_id": nav_revision_id,
            "reexpression_hold": reexpression_hold,
            "revision_source_verified": revision_source_verified,
            "lineage": lineage,
            "calendar_equivalence_digest": equivalence_digest,
        }
    )
    return result


# Shared with risk and the chain through the leaf module (no readiness<->risk
# import); kept under this name for existing callers.
_policy_and_grid = resolve_policy_and_grid

# W2: contention is a retry, never a success (normalized for the runner).
LOCK_BUSY_RESULT = {
    "status": "lock_busy",
    "state": "lock_busy",
    "published": False,
    "retryable": True,
}


_LINEAGE_ROWS_SQL = """
SELECT nav_date, nav, source_nav, source, source_nav_kind, currency, nav_repair_kind,
       calendar_id, calendar_version, calendar_source, return_1d, return_start_date
FROM nav_timeseries WHERE instrument_id=%(iid)s AND nav_date BETWEEN %(start)s AND %(end)s
"""

# Committed full-window reconciliations (N1): receipts are written only in the
# same transaction as the attempt, the levels/returns and the row evidence of
# every reconciled date, and verified by deferred triggers at COMMIT.
_LINEAGE_RECEIPTS_SQL = """
SELECT run_id, provider, window_start, window_end, after_head
FROM nav_rebase_receipts
WHERE instrument_id = %s AND committed_at <= %s
ORDER BY after_head DESC, committed_at DESC
"""

# Per date: the latest LEVEL revision (economic level/provenance; INSERT, DELETE
# or level UPDATE), the latest DERIVED return-only revision and the latest
# CALENDAR revision, each with its attribution (N1-b: run, provider and the
# xid of the writing transaction; parent run status is never read).
_LINEAGE_REVISIONS_SQL = """
WITH latest AS (
    SELECT r.nav_date,
           (array_agg(r.revision_id ORDER BY r.revision_id DESC)
               FILTER (WHERE r.data_changed AND NOT r.derived_return_only))[1]
               AS level_revision_id,
           (array_agg(r.revision_id ORDER BY r.revision_id DESC)
               FILTER (WHERE r.derived_return_only))[1] AS derived_revision_id,
           (array_agg(r.revision_id ORDER BY r.revision_id DESC)
               FILTER (WHERE r.calendar_changed))[1] AS calendar_revision_id
    FROM fund_nav_data_revisions r
    WHERE r.instrument_id = %(iid)s AND r.nav_date BETWEEN %(start)s AND %(end)s
    GROUP BY r.nav_date
)
SELECT l.nav_date, l.level_revision_id, l.derived_revision_id, l.calendar_revision_id,
       lv.source_run_id AS level_run_id, lv.source_provider AS level_provider,
       lv.source_attempt_xid AS level_xid, lv.recorded_at AS level_recorded_at,
       dv.source_run_id AS derived_run_id, dv.source_provider AS derived_provider,
       dv.source_attempt_xid AS derived_xid, dv.recorded_at AS derived_recorded_at,
       dv.dependency_start_date,
       c.source_run_id AS calendar_run_id, c.source_provider AS calendar_provider,
       c.source_attempt_xid AS calendar_xid, c.recorded_at AS calendar_recorded_at,
       c.maintenance_run_id AS calendar_maintenance_id,
       m.status AS maintenance_status, m.completed_at AS maintenance_completed_at,
       m.calendar_id AS maintenance_calendar_id,
       m.calendar_version AS maintenance_calendar_version,
       m.calendar_source AS maintenance_calendar_source,
       (l.nav_date BETWEEN m.window_start AND m.window_end
        AND %(iid)s = ANY(m.instrument_ids)) AS maintenance_in_scope
FROM latest l
LEFT JOIN fund_nav_data_revisions lv ON lv.revision_id = l.level_revision_id
LEFT JOIN fund_nav_data_revisions dv ON dv.revision_id = l.derived_revision_id
LEFT JOIN fund_nav_data_revisions c ON c.revision_id = l.calendar_revision_id
LEFT JOIN nav_calendar_maintenance_runs m ON m.maintenance_run_id = c.maintenance_run_id
ORDER BY l.nav_date
"""

# Row evidence of real fetches (unchanged confirmations included), newest
# head first; its attempt is read by identity, never by parent status.
_LINEAGE_EVIDENCE_SQL = """
SELECT e.nav_date, e.run_id, e.provider, e.level_digest, e.revision_head,
       e.commit_xid, e.recorded_at,
       a.status AS attempt_status, a.commit_xid AS attempt_xid,
       a.finished_at, a.persisted_at, a.requested_start, a.requested_end
FROM nav_ingestion_row_evidence e
JOIN nav_ingestion_attempts a USING (run_id, instrument_id, provider)
WHERE e.instrument_id = %(iid)s AND e.nav_date BETWEEN %(start)s AND %(end)s
  AND e.recorded_at <= %(decision_at)s
ORDER BY e.nav_date, e.revision_head DESC, e.recorded_at DESC, e.run_id
"""


def _attempt_proves(
    attempt: Mapping[str, Any] | None,
    day: dt.date,
    xid: Any,
    decision_at: dt.datetime,
    dependency: dt.date | None = None,
) -> bool:
    """A committed success attempt of the SAME transaction covering the date.

    For a derived return-only revision outside the fetched window the attempt
    must cover the dependency (the changed predecessor), never the date itself:
    no fetch of the successor is claimed.
    """
    if attempt is None or xid is None:
        return False
    if (
        attempt["status"] not in SUCCESS_ATTEMPTS
        or attempt["commit_xid"] != xid
        or attempt["finished_at"] is None
        or attempt["finished_at"] > decision_at
        or attempt["persisted_at"] is None
        or attempt["persisted_at"] > decision_at
    ):
        return False
    start, end = attempt["requested_start"], attempt["requested_end"]
    return start <= day <= end or (dependency is not None and start <= dependency <= end)


def _per_date_lineage(
    cur,
    instrument_id: Any,
    grid: list[dt.date],
    end: dt.date,
    decision_at: dt.datetime,
) -> tuple[bool, list]:
    """Prove economic level, dependent return and calendar lineage per date.

    Level: the latest level revision is attributed to a same-xid success
    attempt of the row's provider, OR a real-fetch row evidence matches the
    current level projection (``nav-level-evidence-v1``) and was not superseded
    by a later level revision of that date (``revision_head >= level rev``).
    Return: a newer derived return-only revision must itself be attributed
    (its attempt covers the date or the dependency start), or be superseded by
    a later committed full-window rebase receipt that reconciled both return
    endpoints to the current levels and return (``receipt_reconciled_return``). Calendar: the latest
    calendar revision is attributed to such an attempt or to a completed,
    in-scope maintenance run with the same tuple. Parent run status is never an
    economic condition; any unknown (unattributed) later revision invalidates.
    """
    params = {"iid": instrument_id, "start": grid[0], "end": end,
              "decision_at": decision_at}
    cur.execute(_LINEAGE_ROWS_SQL, params)
    rows = {row["nav_date"]: row for row in cur.fetchall()}
    cur.execute(_LINEAGE_REVISIONS_SQL, params)
    revisions = {row["nav_date"]: row for row in cur.fetchall()}
    cur.execute(_LINEAGE_EVIDENCE_SQL, params)
    evidence: dict[dt.date, list] = {}
    for row in cur.fetchall():
        evidence.setdefault(row["nav_date"], []).append(row)
    keys = sorted(
        {
            (str(rev[f"{kind}_run_id"]), rev[f"{kind}_provider"])
            for rev in revisions.values()
            for kind in ("level", "derived", "calendar")
            if rev[f"{kind}_run_id"] is not None
        }
    )
    attempts: dict[tuple[str, str], dict] = {}
    if keys:
        cur.execute(
            """SELECT run_id, provider, status, commit_xid, requested_start,
                      requested_end, finished_at, persisted_at
               FROM nav_ingestion_attempts
               WHERE instrument_id = %s AND run_id = ANY(%s::uuid[])""",
            (instrument_id, sorted({run for run, _ in keys})),
        )
        for row in cur.fetchall():
            attempts[(str(row["run_id"]), row["provider"])] = row

    by_run: dict[tuple[str, dt.date], Mapping[str, Any]] = {
        (str(ev["run_id"]), ev["nav_date"]): ev
        for day_evidence in evidence.values()
        for ev in day_evidence
    }
    receipts: list | None = None

    def current_digest(level: Mapping[str, Any]) -> str:
        return level_evidence_digest(
            level["nav_date"], level["nav"], level["source_nav"], level["source"],
            level["source_nav_kind"], level["currency"], level["nav_repair_kind"],
        )

    def receipt_reconciled_return(day: dt.date, row: Mapping[str, Any],
                                  derived_revision_id: int) -> bool:
        """An unattributed derived return is superseded by a LATER governed
        full-window reconciliation that covers both return endpoints.

        Pinned to one committed receipt: its window covers ``day`` and the
        return start date, the derived revision predates the receipt's final
        head (a later unknown derived write has a higher revision id and
        re-invalidates), the receipt run's row evidence of both endpoints
        still equals their current levels, and the current return is exactly
        the canonical log ratio of those levels. No revision is fabricated.
        """
        nonlocal receipts
        pred = row.get("return_start_date")
        value = row.get("return_1d")
        if pred is None or value is None or row.get("nav") is None:
            return False
        pred_row = rows.get(pred)
        if pred_row is None:
            cur.execute(
                """SELECT nav_date, nav, source_nav, source, source_nav_kind, currency,
                          nav_repair_kind
                   FROM nav_timeseries WHERE instrument_id = %s AND nav_date = %s""",
                (instrument_id, pred),
            )
            pred_row = cur.fetchone()
        if pred_row is None or pred_row["nav"] is None or float(pred_row["nav"]) <= 0:
            return False
        expected = round(math.log(float(row["nav"]) / float(pred_row["nav"])), 8)
        if abs(float(value) - expected) > 5e-9:
            return False
        if receipts is None:
            cur.execute(_LINEAGE_RECEIPTS_SQL, (instrument_id, decision_at))
            receipts = cur.fetchall()
        for receipt in receipts:
            if not (
                receipt["window_start"] <= pred
                and day <= receipt["window_end"]
                and derived_revision_id <= receipt["after_head"]
            ):
                continue
            run = str(receipt["run_id"])
            if (run, pred) not in by_run:
                # The receipt window may start before the lineage range.
                cur.execute(
                    """SELECT e.nav_date, e.run_id, e.provider, e.level_digest,
                              e.commit_xid, a.commit_xid AS attempt_xid
                       FROM nav_ingestion_row_evidence e
                       JOIN nav_ingestion_attempts a USING (run_id, instrument_id, provider)
                       WHERE e.instrument_id = %s AND e.run_id = %s AND e.nav_date = %s
                         AND e.recorded_at <= %s""",
                    (instrument_id, receipt["run_id"], pred, decision_at),
                )
                found = cur.fetchone()
                if found is not None:
                    by_run[(run, pred)] = found
            endpoints = (by_run.get((run, day)), by_run.get((run, pred)))
            if all(
                ev is not None
                and ev["provider"] == receipt["provider"]
                and ev["commit_xid"] == ev["attempt_xid"]
                and ev["level_digest"] == current_digest(level)
                for ev, level in zip(endpoints, (row, pred_row))
            ):
                return True
        return False

    def attributed(rev: Mapping[str, Any], kind: str, day: dt.date,
                   provider: str | None, dependency: dt.date | None = None) -> bool:
        run_id = rev[f"{kind}_run_id"]
        if run_id is None or rev[f"{kind}_recorded_at"] > decision_at:
            return False
        if provider is not None and rev[f"{kind}_provider"] != provider:
            return False
        return _attempt_proves(
            attempts.get((str(run_id), rev[f"{kind}_provider"])),
            day, rev[f"{kind}_xid"], decision_at, dependency,
        )

    # A revision from another day cannot stand in for a missing grid level.
    verified = set(grid).issubset(rows)
    lineage = []
    for day in sorted(set(rows) | set(revisions)):
        row = rows.get(day)
        rev = revisions.get(day)
        level_rev_id = rev["level_revision_id"] if rev else None
        level_proof = None
        if rev is not None and level_rev_id is not None and attributed(
            rev, "level", day, row["source"] if row else None
        ):
            level_proof = ["revision", level_rev_id, str(rev["level_run_id"])]
        elif row is not None:
            current = level_evidence_digest(
                day, row["nav"], row["source_nav"], row["source"],
                row["source_nav_kind"], row["currency"], row["nav_repair_kind"],
            )
            for ev in evidence.get(day, ()):
                if (
                    ev["revision_head"] >= (level_rev_id or 0)
                    and ev["level_digest"] == current
                    and ev["provider"] == row["source"]
                    and ev["commit_xid"] == ev["attempt_xid"]
                    and _attempt_proves(
                        {**ev, "status": ev["attempt_status"], "commit_xid": ev["attempt_xid"]},
                        day, ev["commit_xid"], decision_at,
                    )
                ):
                    level_proof = ["evidence", str(ev["run_id"]), ev["provider"],
                                   ev["revision_head"]]
                    break
        level_ok = level_proof is not None
        derived_ok = True
        if rev is not None and rev["derived_revision_id"] is not None:
            derived_ok = attributed(
                rev, "derived", day, None, rev["dependency_start_date"]
            ) or (
                row is not None
                and receipt_reconciled_return(day, row, rev["derived_revision_id"])
            )
        stamp = (
            (row["calendar_id"], row["calendar_version"], row["calendar_source"])
            if row is not None and row["calendar_id"] is not None
            else None
        )
        calendar_ok = True
        calendar_attribution = None
        if stamp is not None:
            if rev is None or rev["calendar_revision_id"] is None:
                calendar_ok = False
            elif rev["calendar_run_id"] is not None:
                calendar_attribution = ["provider", str(rev["calendar_run_id"])]
                calendar_ok = attributed(rev, "calendar", day, row["source"])
            elif rev["calendar_maintenance_id"] is not None:
                calendar_attribution = [
                    "maintenance",
                    str(rev["calendar_maintenance_id"]),
                ]
                calendar_ok = bool(
                    rev["maintenance_status"] == "completed"
                    and rev["maintenance_completed_at"] is not None
                    and rev["maintenance_completed_at"] <= decision_at
                    and rev["maintenance_in_scope"]
                    and (
                        rev["maintenance_calendar_id"],
                        rev["maintenance_calendar_version"],
                        rev["maintenance_calendar_source"],
                    )
                    == stamp
                )
            else:
                calendar_ok = False
        ok = level_ok and derived_ok and calendar_ok
        verified = verified and ok
        lineage.append(
            [
                day.isoformat(),
                level_proof,
                rev["derived_revision_id"] if rev else None,
                rev["calendar_revision_id"] if rev else None,
                calendar_attribution,
                ok,
            ]
        )
    return verified, lineage


def _input_evidence(
    conn,
    instrument_id: Any,
    policy: dict,
    decision_at: dt.datetime,
    grid: list[dt.date],
    published_risk_run_id: uuid.UUID,
    closed_session: dt.date,
) -> tuple:
    with conn.cursor(row_factory=dict_row) as cur:
        # N3: same filters and deterministic tie-break as the snapshot function.
        cur.execute(
            """SELECT * FROM nav_instrument_policy_evidence
               WHERE instrument_id=%s AND policy_id=%s AND policy_version=%s
                 AND known_at <= %s AND effective_at <= %s AND recorded_at <= %s
                ORDER BY effective_at DESC, known_at DESC, recorded_at DESC,
                         evidence_id DESC
                LIMIT 1""",
            (
                instrument_id,
                policy["policy_id"],
                policy["policy_version"],
                decision_at,
                decision_at,
                decision_at,
            ),
        )
        lifecycle = cur.fetchone()
        cur.execute(
            "SELECT revision_id FROM fund_nav_data_heads WHERE instrument_id=%s",
            (instrument_id,),
        )
        nav_head = cur.fetchone()
        nav_revision_id = nav_head["revision_id"] if nav_head else 0
        lineage_verified, lineage = _per_date_lineage(
            cur, instrument_id, grid, closed_session, decision_at
        )
        # Row evidence can prove a typed level that never had a revision (head
        # 0); the per-date proof, not the existence of a head, is the gate.
        revision_source_verified = lineage_verified
        # N1-a: only an active (unresolved) DETECTED event that overlaps the
        # due grid blocks; a detection wholly before the window is ledger-only.
        cur.execute(
            """SELECT event_id FROM fund_nav_reexpression_holds
               WHERE instrument_id=%s AND last_changed_date >= %s
                 AND first_changed_date <= %s
               ORDER BY event_id""",
            (instrument_id, grid[0], grid[-1]),
        )
        active_holds = [row["event_id"] for row in cur.fetchall()]
        reexpression_hold = bool(active_holds)
        cur.execute(
            """SELECT """
            + _NAV_FIELDS
            + """ FROM nav_timeseries
               WHERE instrument_id=%s AND nav_date BETWEEN %s AND %s
               ORDER BY nav_date""",
            (instrument_id, grid[0], grid[-1]),
        )
        nav_rows = cur.fetchall()
        cur.execute(
            """SELECT min(nav_date) AS first_nav, max(nav_date) AS last_nav FROM nav_timeseries
               WHERE instrument_id=%s""",
            (instrument_id,),
        )
        bounds = cur.fetchone()
        earliest_nav, latest_nav = bounds["first_nav"], bounds["last_nav"]
        # N1-b: the winning attempt is the latest one persisted by decision_at,
        # whatever its parent run's status (a batch aborted after this
        # instrument committed does not erase its atomic proof).
        cur.execute(
            """SELECT a.run_id, a.status, a.finished_at FROM nav_ingestion_attempts a
               WHERE a.instrument_id=%s AND a.attempted_at IS NOT NULL
                 AND a.attempted_at <= %s AND a.persisted_at <= %s
               ORDER BY a.attempted_at DESC, a.persisted_at DESC, a.provider DESC
               LIMIT 1""",
            (instrument_id, decision_at, decision_at),
        )
        attempt = cur.fetchone()
        # Identity is (published risk_run_id, instrument_id, definition): never the
        # latest evidence by date, which could belong to a later diagnostic run.
        # N3: the risk run is complete by decision_at, the feature was computed
        # by decision_at, and the run pins the readiness policy ID/version/hash.
        cur.execute(
            """SELECT e.*, risk.completed_at AS risk_completed_at
               FROM fund_nav_feature_evidence e
               JOIN fund_nav_risk_runs risk ON risk.risk_run_id=e.risk_run_id
               WHERE e.risk_run_id=%s AND e.instrument_id=%s
                 AND e.definition_version=%s
                 AND risk.feature_definition_version=e.definition_version
                 AND e.calc_date <= %s AND risk.status='complete'
                 AND risk.run_scope='current_full'
                 AND risk.completed_at <= %s AND e.computed_at <= %s
                 AND risk.policy_id = %s AND risk.policy_version = %s
                 AND risk.policy_hash = %s""",
            (
                published_risk_run_id,
                instrument_id,
                FEATURE_DEFINITION_VERSION,
                grid[-1],
                decision_at,
                decision_at,
                policy["policy_id"],
                policy["policy_version"],
                policy["policy_hash"],
            ),
        )
        feature = cur.fetchone()
        cur.execute(
            """SELECT ex.calc_date, risk.completed_at FROM fund_nav_risk_exclusions ex
               JOIN fund_nav_risk_runs risk ON risk.risk_run_id=ex.risk_run_id
               WHERE ex.risk_run_id=%s AND ex.instrument_id=%s
                 AND ex.calc_date <= %s AND risk.status='complete'
                 AND risk.completed_at <= %s""",
            (published_risk_run_id, instrument_id, grid[-1], decision_at),
        )
        exclusion = cur.fetchone()
        if exclusion and (
            feature is None
            or (exclusion["calc_date"], exclusion["completed_at"])
            >= (feature["calc_date"], feature["risk_completed_at"])
        ):
            feature = None
        nav_matches = False
        if feature is not None:
            lower = feature["calc_date"] - dt.timedelta(days=11 * 366)
            cur.execute(
                """SELECT nav_date, nav FROM nav_timeseries
                   WHERE instrument_id=%s AND nav_date >= %s AND nav_date <= %s
                     AND nav IS NOT NULL ORDER BY nav_date""",
                (instrument_id, lower, feature["calc_date"]),
            )
            actual = cur.fetchall()
            expected_hash = digest(
                [(r["nav_date"].isoformat(), str(r["nav"])) for r in actual]
            )
            nav_matches = (
                (
                    expected_hash == feature["nav_input_fingerprint"]
                    and len(actual) == feature["nav_count"]
                    and actual[0]["nav_date"] == feature["nav_start"]
                    and actual[-1]["nav_date"] == feature["nav_end"]
                )
                if actual
                else False
            )
    return (
        lifecycle,
        nav_rows,
        earliest_nav,
        latest_nav,
        attempt,
        feature,
        nav_matches,
        nav_revision_id,
        reexpression_hold,
        revision_source_verified,
        lineage,
    )


def _observed_stamps(
    conn, instrument_ids: list, grid: list[dt.date]
) -> list[tuple[str, str, str]]:
    """Distinct stamps on the cohort's grid rows: one batched catalogue read."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT DISTINCT calendar_id, calendar_version, calendar_source
               FROM nav_timeseries
               WHERE instrument_id = ANY(%s) AND nav_date = ANY(%s::date[])
                 AND calendar_id IS NOT NULL AND calendar_version IS NOT NULL
                 AND calendar_source IS NOT NULL""",
            (instrument_ids, list(grid)),
        )
        return [tuple(row) for row in cur.fetchall()]


def run(dsn: str) -> dict[str, Any]:
    """Materialize a complete snapshot and flip the one-row pointer atomically."""
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FUND_NAV_READINESS) as got:
            if not got:
                return dict(LOCK_BUSY_RESULT)
            conn.commit()  # session lock survives; begin a fresh repeatable-read txn
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur.execute("SELECT clock_timestamp() AS decision_at")
                decision_at = cur.fetchone()["decision_at"]
            policy, grid, closed = _policy_and_grid(conn, decision_at)
            sid = sample_id(policy, grid)
            run_id = uuid.uuid4()
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """SELECT revision_id,state,published_risk_run_id
                       FROM fund_nav_risk_publication
                       WHERE readiness_profile=%s""",
                    (PROFILE,),
                )
                risk_publication = cur.fetchone()
                if (
                    risk_publication is None
                    or risk_publication["state"] != "idle"
                    or risk_publication["published_risk_run_id"] is None
                ):
                    raise RuntimeError(
                        "RETURN_SAMPLE_NOT_CURRENT: risk run not published"
                    )
                cur.execute(
                    "SELECT instrument_id FROM funds_profile_mv ORDER BY instrument_id"
                )
                instrument_ids = [r["instrument_id"] for r in cur.fetchall()]
                if not instrument_ids:
                    raise RuntimeError(
                        "NAV_DATA_UNAVAILABLE: no fund cohort to publish"
                    )
                cur.execute("SELECT max(nav_date) AS watermark FROM nav_timeseries")
                watermark = cur.fetchone()["watermark"]
                stamps = _observed_stamps(conn, instrument_ids, grid)
                equivalence = load_calendar_equivalence(
                    conn, policy, grid, stamps, decision_at
                )
                equivalence_digest = equivalence_mapping_digest(equivalence)
                cur.execute(
                    """INSERT INTO fund_nav_readiness_runs
                        (run_id, readiness_profile, readiness_version, policy_id,
                         policy_version, policy_hash, calendar_id, calendar_version,
                         decision_at, as_of_session, latest_closed_session,
                         window_start, window_end, sample_id, input_watermark,
                         risk_publication_revision,published_risk_run_id,
                         state, expected_rows)
                        VALUES (%s,%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'building',%s)""",
                    (
                        run_id,
                        PROFILE,
                        policy["policy_id"],
                        policy["policy_version"],
                        policy["policy_hash"],
                        policy["calendar_id"],
                        policy["calendar_version"],
                        decision_at,
                        grid[-1],
                        closed,
                        grid[0],
                        grid[-1],
                        sid,
                        watermark,
                        risk_publication["revision_id"],
                        risk_publication["published_risk_run_id"],
                        len(instrument_ids),
                    ),
                )
                insert_sql = (
                    "INSERT INTO fund_nav_readiness_v1 ("
                    + ",".join(_ROW_FIELDS)
                    + ") VALUES ("
                    + ",".join(["%s"] * len(_ROW_FIELDS))
                    + ")"
                )
                fingerprints = []
                ready_count = 0
                for instrument_id in instrument_ids:
                    (
                        lifecycle,
                        nav_rows,
                        first_nav,
                        last_nav,
                        attempt,
                        feature,
                        match,
                        revision_id,
                        hold,
                        revision_verified,
                        lineage,
                    ) = _input_evidence(
                        conn,
                        instrument_id,
                        policy,
                        decision_at,
                        grid,
                        risk_publication["published_risk_run_id"],
                        closed,
                    )
                    row = assess_instrument(
                        instrument_id,
                        policy,
                        grid,
                        nav_rows,
                        last_nav,
                        closed,
                        lifecycle,
                        attempt,
                        feature,
                        match,
                        earliest_nav=first_nav,
                        nav_revision_id=revision_id,
                        reexpression_hold=hold,
                        revision_source_verified=revision_verified,
                        equivalence=equivalence,
                        lineage=lineage,
                    )
                    row.update(run_id=run_id, sample_id=sid)
                    cur.execute(insert_sql, tuple(row[name] for name in _ROW_FIELDS))
                    fingerprints.append((str(instrument_id), row["input_fingerprint"]))
                    ready_count += int(row["admissible"])
                cur.execute(
                    """SELECT count(*) AS n FROM fund_nav_readiness_v1 WHERE run_id=%s""",
                    (run_id,),
                )
                if cur.fetchone()["n"] != len(instrument_ids):
                    raise RuntimeError(
                        "NAV_DATA_UNAVAILABLE: incomplete readiness build"
                    )
                cur.execute(
                    """SELECT session_date FROM nav_valuation_schedules
                       WHERE calendar_id=%s AND calendar_version=%s AND nav_due_at <= clock_timestamp()
                       ORDER BY session_date DESC LIMIT 1""",
                    (policy["calendar_id"], policy["calendar_version"]),
                )
                if cur.fetchone()["session_date"] != grid[-1]:
                    raise RuntimeError(
                        "NAV_POLICY_UNAVAILABLE: due session advanced during build"
                    )
                # The final re-read must reproduce the same calendar proof.
                reread = load_calendar_equivalence(
                    conn, policy, grid, stamps, decision_at
                )
                if equivalence_mapping_digest(reread) != equivalence_digest:
                    raise RuntimeError(
                        "NAV_POLICY_UNAVAILABLE: calendar equivalence changed during build"
                    )
                cur.execute(
                    """UPDATE fund_nav_readiness_runs
                       SET state='complete', completed_at=clock_timestamp(),
                           published_rows=%s, run_fingerprint=%s WHERE run_id=%s""",
                    (
                        len(instrument_ids),
                        digest(
                            {
                                "rows": fingerprints,
                                "calendar_digest": policy["calendar_digest"],
                                "calendar_equivalence": equivalence_digest,
                                "latest_closed_session": closed,
                                "risk_publication_revision": risk_publication[
                                    "revision_id"
                                ],
                                "published_risk_run_id": risk_publication[
                                    "published_risk_run_id"
                                ],
                            }
                        ),
                        run_id,
                    ),
                )
                cur.execute(
                    """INSERT INTO fund_nav_readiness_current
                       (readiness_profile, run_id, state)
                       VALUES (%s,%s,'complete')
                       ON CONFLICT (readiness_profile) DO UPDATE SET
                         run_id=EXCLUDED.run_id, state=EXCLUDED.state,
                         published_at=clock_timestamp()""",
                    (PROFILE, run_id),
                )
            conn.commit()
            return {
                "state": "complete",
                "published": True,
                "run_id": str(run_id),
                "sample_id": sid,
                "as_of_session": grid[-1].isoformat(),
                "instrument_count": len(instrument_ids),
                "ready_count": ready_count,
            }
