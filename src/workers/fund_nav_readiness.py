"""DB-first current daily NAV evidence; no provider reads or inferred calendar."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import uuid
from typing import Any

from psycopg.rows import dict_row

from src.db import LOCK_FUND_NAV_READINESS, advisory_lock, connect
from src.workers._nav_policy import calendar_digest
from src.workers.risk_metrics import FEATURE_DEFINITION_VERSION

PROFILE = "current_daily_nav_v1"
INTERVALS = 400
ENDPOINTS = INTERVALS + 1
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


def _valid_interval(previous: dict, current: dict, policy: dict) -> bool:
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
            or row["calendar_id"] != policy["calendar_id"]
            or row["calendar_version"] != policy["calendar_version"]
            or row["calendar_source"] != policy["calendar_source"]
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
) -> dict[str, Any]:
    """Exact 401 observed levels / 400 declared intervals, not tail/dropna."""
    status = lifecycle["fund_status"] if lifecycle else "UNKNOWN"
    frequency = lifecycle["valuation_frequency"] if lifecycle else "unknown"
    by_date = {row["nav_date"]: row for row in nav_rows}
    present = [d for d in grid if d in by_date and by_date[d]["nav"] is not None]
    missing = ENDPOINTS - len(present)
    missed_due = sum(latest_nav is None or date > latest_nav for date in grid)
    current = latest_nav is not None and latest_nav >= grid[-1] and grid[-1] in present
    compatible = len(present) == ENDPOINTS and len(by_date) == ENDPOINTS
    valid_returns = 0
    last_return = None
    if compatible:
        for start, end in zip(grid, grid[1:]):
            if _valid_interval(by_date[start], by_date[end], policy):
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
    attempted = attempt is not None and attempt["status"] in (
        "success_new",
        "success_no_new",
    )
    identity = bool(lifecycle and lifecycle["identity_verified"])
    basis = bool(lifecycle and lifecycle["return_basis_verified"])
    currency = bool(lifecycle and lifecycle["currency_verified"])
    semantics_supported = all(
        row["source_nav_kind"] == policy["required_nav_kind"]
        and (
            row["nav_date"] == grid[0]
            or (
                row["return_semantics"] == policy["required_return_semantics"]
                and row["return_type"] == "log"
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
    elif not (identity and basis and currency and attempted and native_currency):
        reason = "NAV_DATA_UNAVAILABLE"
    elif not semantics_supported:
        reason = "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    elif not revision_source_verified:
        reason = "NAV_DATA_UNAVAILABLE"
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
        }
    )
    return result


def _policy_and_grid(
    conn, decision_at: dt.datetime
) -> tuple[dict, list[dt.date], dt.date]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """SELECT policy.* FROM nav_policy_current current_policy
               JOIN nav_policy_versions policy USING (policy_id, policy_version)
               WHERE current_policy.readiness_profile = %s
                 AND policy.published_at <= %s AND current_policy.published_at <= %s""",
            (PROFILE, decision_at, decision_at),
        )
        policy = cur.fetchone()
        if not policy:
            raise RuntimeError("NAV_POLICY_UNAVAILABLE: no published policy")
        if decision_at > policy["valid_through"]:
            raise RuntimeError("NAV_POLICY_UNAVAILABLE: calendar coverage expired")
        cur.execute(
            """SELECT session_date,valuation_close_at,nav_due_at,source_reference
               FROM nav_valuation_schedules WHERE calendar_id=%s AND calendar_version=%s
               ORDER BY session_date""",
            (policy["calendar_id"], policy["calendar_version"]),
        )
        catalog = cur.fetchall()
        if (
            len(catalog) != policy["calendar_session_count"]
            or not catalog
            or catalog[0]["session_date"] != policy["coverage_start"]
            or catalog[-1]["session_date"] != policy["coverage_end"]
            or calendar_digest(
                [
                    (
                        s["session_date"],
                        s["valuation_close_at"],
                        s["nav_due_at"],
                        s["source_reference"],
                    )
                    for s in catalog
                ]
            )
            != policy["calendar_digest"]
        ):
            raise RuntimeError("NAV_POLICY_UNAVAILABLE: calendar digest mismatch")
        cur.execute(
            """SELECT session_date, calendar_source FROM nav_valuation_schedules
               WHERE calendar_id = %s AND calendar_version = %s AND nav_due_at <= %s
               ORDER BY session_date DESC LIMIT %s""",
            (policy["calendar_id"], policy["calendar_version"], decision_at, ENDPOINTS),
        )
        sessions = cur.fetchall()
        if (
            len(sessions) != ENDPOINTS
            or sessions[0]["session_date"] < policy["coverage_start"]
            or sessions[0]["session_date"] > policy["coverage_end"]
            or any(s["calendar_source"] != policy["calendar_source"] for s in sessions)
        ):
            raise RuntimeError("NAV_POLICY_UNAVAILABLE: incomplete calendar window")
        cur.execute(
            """SELECT session_date FROM nav_valuation_schedules
               WHERE calendar_id=%s AND calendar_version=%s
                 AND valuation_close_at <= %s
               ORDER BY session_date DESC LIMIT 1""",
            (policy["calendar_id"], policy["calendar_version"], decision_at),
        )
        closed = cur.fetchone()
        if not closed:
            raise RuntimeError("NAV_POLICY_UNAVAILABLE: no closed session")
        if closed["session_date"] > policy["coverage_end"]:
            raise RuntimeError("NAV_POLICY_UNAVAILABLE: closed session beyond coverage")
    return (
        policy,
        list(reversed([r["session_date"] for r in sessions])),
        closed["session_date"],
    )


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
        cur.execute(
            """SELECT * FROM nav_instrument_policy_evidence
               WHERE instrument_id=%s AND policy_id=%s AND policy_version=%s
                 AND known_at <= %s AND effective_at <= %s
                ORDER BY effective_at DESC, known_at DESC LIMIT 1""",
            (
                instrument_id,
                policy["policy_id"],
                policy["policy_version"],
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
        cur.execute(
            """SELECT 1 FROM (
                 SELECT DISTINCT ON (nav_date) nav_date,source_run_id
                 FROM fund_nav_data_revisions
                 WHERE instrument_id=%s AND nav_date BETWEEN %s AND %s
                 ORDER BY nav_date,revision_id DESC
               ) latest
               LEFT JOIN nav_ingestion_runs run ON run.run_id=latest.source_run_id
               WHERE latest.source_run_id IS NULL
                  OR run.status IS DISTINCT FROM 'completed'
                  OR NOT EXISTS (
                    SELECT 1 FROM nav_ingestion_attempts a
                    WHERE a.instrument_id=%s AND a.run_id=latest.source_run_id
                      AND a.status IN ('success_new','success_no_new'))
               LIMIT 1""",
            (instrument_id, grid[0], closed_session, instrument_id),
        )
        revision_source_verified = nav_revision_id > 0 and cur.fetchone() is None
        cur.execute(
            "SELECT 1 FROM fund_nav_reexpression_holds WHERE instrument_id=%s",
            (instrument_id,),
        )
        reexpression_hold = cur.fetchone() is not None
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
        cur.execute(
            """SELECT a.run_id, a.status, a.finished_at FROM nav_ingestion_attempts a
               JOIN nav_ingestion_runs run ON run.run_id=a.run_id
               WHERE a.instrument_id=%s AND a.attempted_at <= %s
                 AND run.status='completed' AND a.attempted_at IS NOT NULL
               ORDER BY a.attempted_at DESC LIMIT 1""",
            (instrument_id, decision_at),
        )
        attempt = cur.fetchone()
        cur.execute(
            """SELECT e.*, risk.completed_at AS risk_completed_at
               FROM fund_nav_feature_evidence e
               JOIN fund_nav_risk_runs risk ON risk.risk_run_id=e.risk_run_id
                WHERE e.instrument_id=%s AND e.definition_version=%s
                  AND e.calc_date <= %s AND risk.status='complete'
                  AND e.risk_run_id=%s
               ORDER BY e.calc_date DESC, risk.completed_at DESC LIMIT 1""",
            (
                instrument_id,
                FEATURE_DEFINITION_VERSION,
                grid[-1],
                published_risk_run_id,
            ),
        )
        feature = cur.fetchone()
        cur.execute(
            """SELECT ex.calc_date, risk.completed_at FROM fund_nav_risk_exclusions ex
               JOIN fund_nav_risk_runs risk ON risk.risk_run_id=ex.risk_run_id
                WHERE ex.instrument_id=%s AND ex.calc_date <= %s
                  AND ex.risk_run_id=%s
                 AND risk.status='complete'
               ORDER BY ex.calc_date DESC, risk.completed_at DESC LIMIT 1""",
            (instrument_id, grid[-1], published_risk_run_id),
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
    )


def run(dsn: str) -> dict[str, Any]:
    """Materialize a complete snapshot and flip the one-row pointer atomically."""
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FUND_NAV_READINESS) as got:
            if not got:
                return {"state": "locked", "published": False}
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
