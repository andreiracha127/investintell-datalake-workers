"""Governed NAV readiness schema/policy operator. Check is the default; no live action implicit."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import sys
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql

from src.workers._nav_policy import (
    ADJUSTED_OVERLAP_ABS_TOL,
    ADJUSTED_OVERLAP_REL_TOL,
    CATALOG_EVIDENCE_REFERENCE,
    CURRENT_CATALOG_QUERY_VERSION,
    GENERATOR_VERSION,
    PROVIDER_CONTRACT_VERSION,
    SOURCE_QUERY_SHA256,
    calendar_digest,
    generation_metadata_digest,
    instrument_evidence_digest,
    policy_content_digest,
)
from src.workers._nav_sanitize import REPAIRED_NAV_KINDS

ROOT = Path(__file__).resolve().parents[1]
DDL = ROOT / "schemas" / "fund_nav_readiness_v1.sql"
TABLES = (
    "nav_policy_versions",
    "nav_valuation_schedules",
    "nav_policy_current",
    "nav_instrument_policy_evidence",
    "nav_ingestion_runs",
    "nav_ingestion_attempts",
    "fund_nav_data_heads",
    "fund_nav_data_revisions",
    "fund_nav_reexpression_holds",
    "fund_nav_risk_runs",
    "fund_nav_risk_publication",
    "fund_nav_risk_exclusions",
    "fund_nav_feature_evidence",
    "fund_nav_readiness_runs",
    "fund_nav_readiness_v1",
    "fund_nav_readiness_current",
)
CRITICAL_COLUMNS = {
    "nav_policy_versions": {
        "policy_id": "text",
        "policy_version": "text",
        "policy_hash": "character(64)",
        "required_nav_kind": "text",
        "required_return_semantics": "text",
        "modeling_currency": "character varying(3)",
        "currency_treatment": "text",
        "published_at": "timestamp with time zone",
        "coverage_start": "date",
        "coverage_end": "date",
        "valid_through": "timestamp with time zone",
        "calendar_session_count": "integer",
        "calendar_digest": "character(64)",
    },
    "nav_valuation_schedules": {
        "session_date": "date",
        "nav_due_at": "timestamp with time zone",
    },
    "nav_policy_current": {"policy_id": "text", "policy_version": "text"},
    "nav_instrument_policy_evidence": {
        "evidence_id": "uuid",
        "instrument_id": "uuid",
        "recorded_at": "timestamp with time zone",
    },
    "nav_ingestion_attempts": {
        "instrument_id": "uuid",
        "run_id": "uuid",
        "status": "text",
        "persisted_at": "timestamp with time zone",
    },
    "fund_nav_data_heads": {"instrument_id": "uuid", "revision_id": "bigint"},
    "fund_nav_data_revisions": {"revision_id": "bigint", "source_run_id": "uuid"},
    "fund_nav_reexpression_holds": {"instrument_id": "uuid", "reason_code": "text"},
    "fund_nav_risk_runs": {"risk_run_id": "uuid", "status": "text"},
    "fund_nav_risk_publication": {
        "revision_id": "bigint",
        "state": "text",
        "published_risk_run_id": "uuid",
    },
    "fund_nav_risk_exclusions": {"risk_run_id": "uuid", "reason_code": "text"},
    "fund_nav_feature_evidence": {
        "input_max_date": "date",
        "nav_input_fingerprint": "character(64)",
    },
    "fund_nav_readiness_runs": {
        "run_id": "uuid",
        "as_of_session": "date",
        "latest_closed_session": "date",
        "risk_publication_revision": "bigint",
        "sample_id": "character(64)",
        "state": "text",
    },
    "fund_nav_readiness_v1": {
        "run_id": "uuid",
        "instrument_id": "uuid",
        "admissible": "boolean",
        "window_end": "date",
        "missing_session_count": "integer",
        "reason_code": "text",
        "lifecycle_evidence_id": "uuid",
        "nav_revision_id": "bigint",
        "risk_run_id": "uuid",
        "risk_input_fingerprint": "character(64)",
    },
    "fund_nav_readiness_current": {"run_id": "uuid", "state": "text"},
}
SCHEMA_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_$]{0,62}\Z")


def _policy(path: str | dict | None) -> tuple[dict | None, bytes]:
    if path is None:
        return None, b""
    if isinstance(path, dict):
        policy = path
        raw = (
            json.dumps(policy, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
    else:
        raw = Path(path).read_bytes()
        policy = json.loads(raw)
    required = {
        "policy_id",
        "policy_version",
        "calendar_id",
        "calendar_version",
        "calendar_source",
        "source_reference",
        "timezone",
        "sessions",
        "instrument_evidence",
        "coverage_start",
        "coverage_end",
        "valid_through",
        "calendar_session_count",
        "calendar_digest",
    }
    if (
        not required <= policy.keys()
        or policy.get("readiness_profile") != "current_daily_nav_v1"
        or policy.get("valuation_frequency") != "daily"
        or policy["timezone"] != "America/New_York"
        or policy.get("sample_intervals") != 400
        or policy.get("annualization_sessions") != 252
        or policy.get("required_nav_kind") != "adjusted"
        or policy.get("required_return_semantics") != "observed_interval_log_ratio"
        or policy.get("modeling_currency") != "USD"
        or policy.get("currency_treatment") != "native_only"
        or set(policy.get("repaired_nav_kinds", [])) != REPAIRED_NAV_KINDS
        or len(policy.get("repaired_nav_kinds", [])) != len(REPAIRED_NAV_KINDS)
        or policy.get("adjusted_overlap_absolute_tolerance") != ADJUSTED_OVERLAP_ABS_TOL
        or policy.get("adjusted_overlap_relative_tolerance") != ADJUSTED_OVERLAP_REL_TOL
        or policy.get("publication_state") != "approved"
        or any(
            not isinstance(policy.get(key), str) or not policy[key].strip()
            for key in (
                "policy_id",
                "policy_version",
                "calendar_id",
                "calendar_version",
                "calendar_source",
                "source_reference",
            )
        )
    ):
        raise ValueError("policy_evidence_incomplete")
    coverage_start = dt.date.fromisoformat(policy["coverage_start"])
    coverage_end = dt.date.fromisoformat(policy["coverage_end"])
    valid_through = dt.datetime.fromisoformat(policy["valid_through"])
    if valid_through.tzinfo is None:
        raise ValueError("calendar_expiration_unverified")
    dates = []
    normalized_sessions = []
    for entry in policy["sessions"]:
        date = dt.date.fromisoformat(entry["session_date"])
        close = dt.datetime.fromisoformat(entry["valuation_close_at"])
        due = dt.datetime.fromisoformat(entry["nav_due_at"])
        if close.tzinfo is None or due.tzinfo is None or due < close:
            raise ValueError("session_deadline_unverified")
        if close.astimezone(ZoneInfo("America/New_York")).date() != date:
            raise ValueError("session_date_not_ny_valuation_date")
        dates.append(date)
        reference = entry.get("source_reference") or policy["source_reference"]
        if not isinstance(reference, str) or not reference.strip():
            raise ValueError("calendar_source_unverified")
        normalized_sessions.append((date, close, due, reference))
    if (
        len(dates) < 401
        or dates != sorted(set(dates))
        or len(dates) != policy["calendar_session_count"]
        or dates[0] != coverage_start
        or dates[-1] != coverage_end
        or valid_through < normalized_sessions[-1][2]
        or calendar_digest(normalized_sessions) != policy["calendar_digest"]
    ):
        raise ValueError("calendar_not_ordered_or_too_short")
    seen_evidence = set()
    for row in policy["instrument_evidence"]:
        uuid.UUID(row["instrument_id"])
        if (
            row["instrument_id"],
            row["known_at"],
            row["effective_at"],
        ) in seen_evidence:
            raise ValueError("duplicate_instrument_evidence")
        seen_evidence.add((row["instrument_id"], row["known_at"], row["effective_at"]))
        if row["fund_status"] not in ("ACTIVE", "INACTIVE", "UNKNOWN"):
            raise ValueError("status_unrecognized")
        if row["valuation_frequency"] not in ("daily", "weekly", "monthly", "unknown"):
            raise ValueError("frequency_unrecognized")
        for key in ("known_at", "effective_at"):
            if dt.datetime.fromisoformat(row[key]).tzinfo is None:
                raise ValueError("evidence_timestamp_unverified")
        if any(
            type(row.get(key)) is not bool
            for key in (
                "identity_verified",
                "return_basis_verified",
                "currency_verified",
            )
        ):
            raise ValueError("evidence_boolean_unverified")
        if (
            not isinstance(row["evidence_reference"], str)
            or not row["evidence_reference"].strip()
        ):
            raise ValueError("identity_evidence_missing")
    generation = policy.get("generation")
    if generation is None and (
        "generator_version" in policy or "provider_contract" in policy
    ):
        raise ValueError("generator_metadata_invalid")
    if generation is not None:
        if not isinstance(generation, dict):
            raise ValueError("generator_metadata_invalid")
        try:
            generated_at = dt.datetime.fromisoformat(generation["generated_at"])
            counts = generation["counts"]
            if (
                generated_at.tzinfo is None
                or policy.get("generator_version") != GENERATOR_VERSION
                or policy.get("provider_contract") != PROVIDER_CONTRACT_VERSION
                or generation["generator_version"] != GENERATOR_VERSION
                or generation["provider_contract"] != PROVIDER_CONTRACT_VERSION
                or any(
                    row["evidence_reference"] != CATALOG_EVIDENCE_REFERENCE
                    for row in policy["instrument_evidence"]
                )
                or generation["source_query_version"] != CURRENT_CATALOG_QUERY_VERSION
                or generation["source_query_sha256"] != SOURCE_QUERY_SHA256
                or generation["calendar_package"] != "exchange_calendars==4.13.2"
                or generation["calendar_digest"] != policy["calendar_digest"]
                or generation["policy_hash"] != policy_content_digest(policy)
                or generation["instrument_evidence_digest"]
                != instrument_evidence_digest(policy["instrument_evidence"])
                or generation["generation_sha256"]
                != generation_metadata_digest(generation)
                or counts["instrument_evidence"] != len(policy["instrument_evidence"])
                or dt.date.fromisoformat(generation["requested_coverage_start"])
                > coverage_start
                or dt.date.fromisoformat(generation["requested_coverage_end"])
                < coverage_end
                or any(
                    dt.datetime.fromisoformat(row[key]) != generated_at
                    for row in policy["instrument_evidence"]
                    for key in ("known_at", "effective_at")
                )
            ):
                raise ValueError("generator_metadata_invalid")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("generator_metadata_invalid") from exc
    return policy, raw


def plan_hash(
    ddl: bytes,
    schema: str,
    policy: bytes,
    instrument_ids: list[str],
    start: str | None,
    end: str | None,
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "ddl_sha256": hashlib.sha256(ddl).hexdigest(),
                "schema": schema,
                "policy_sha256": hashlib.sha256(policy).hexdigest() if policy else None,
                "ids": sorted(instrument_ids),
                "start": start,
                "end": end,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _check(conn, schema: str) -> dict:
    conn.execute("BEGIN TRANSACTION READ ONLY")
    try:
        conn.execute("SET LOCAL statement_timeout = '5s'")
        conn.execute("SET LOCAL lock_timeout = '1s'")
        conn.execute(
            sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema))
        )
        if conn.execute("SELECT current_schema()").fetchone()[0] != schema:
            raise ValueError("target_schema_missing")
        rows = conn.execute(
            """SELECT relname, relkind FROM pg_class c JOIN pg_namespace n
               ON c.relnamespace=n.oid WHERE n.nspname=%s AND relname=ANY(%s)""",
            (schema, list(TABLES)),
        ).fetchall()
        kinds = dict(rows)
        if any(kind != "r" for kind in kinds.values()):
            raise ValueError("schema_relation_kind_mismatch")
        columns = conn.execute(
            """SELECT c.relname,a.attname,format_type(a.atttypid,a.atttypmod)
               FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid
               JOIN pg_attribute a ON a.attrelid=c.oid
               WHERE n.nspname=%s AND c.relname=ANY(%s)
                 AND a.attnum>0 AND NOT a.attisdropped""",
            (schema, ["nav_timeseries", *TABLES]),
        ).fetchall()
        by_table = {}
        for table, name, data_type in columns:
            by_table.setdefault(table, {})[name] = data_type
        from scripts.nav_timeseries_provenance_schema import EXPECTED_COLUMNS

        if any(
            by_table.get("nav_timeseries", {}).get(name) != data_type
            for name, data_type in EXPECTED_COLUMNS
        ):
            raise ValueError("nav_provenance_pr132_missing")
        if any(
            by_table.get(table, {}).get(name) != data_type
            for table, expected in CRITICAL_COLUMNS.items()
            if table in kinds
            for name, data_type in expected.items()
        ):
            raise ValueError("readiness_schema_contract_mismatch")
        view = conn.execute(
            """SELECT relkind FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid
               WHERE n.nspname=%s AND c.relname='fund_nav_readiness_current_v1'""",
            (schema,),
        ).fetchone()
        if view is not None and view[0] != "v":
            raise ValueError("readiness_view_contract_mismatch")
        present_triggers = set(
            conn.execute(
                """SELECT c.relname,t.tgname FROM pg_trigger t
               JOIN pg_class c ON c.oid=t.tgrelid
               JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname=%s AND NOT t.tgisinternal""",
                (schema,),
            ).fetchall()
        )
        required_triggers = {
            ("nav_timeseries", "fund_nav_stamp_revision"),
            ("fund_nav_data_revisions", "fund_nav_revision_append_only"),
            ("nav_policy_versions", "nav_policy_freeze"),
            ("nav_valuation_schedules", "nav_schedule_freeze"),
            ("nav_instrument_policy_evidence", "nav_instrument_evidence_append_only"),
            ("fund_nav_readiness_runs", "fund_nav_readiness_run_freeze"),
            ("fund_nav_readiness_v1", "fund_nav_readiness_row_freeze"),
        }
        resolver = conn.execute(
            """SELECT prosecdef,proconfig FROM pg_proc f
               JOIN pg_namespace n ON n.oid=f.pronamespace
               WHERE n.nspname=%s AND f.proname='fund_nav_snapshot_current_at_v1'""",
            (schema,),
        ).fetchone()
        secured_resolver = bool(
            resolver
            and resolver[0]
            and resolver[1]
            and any(
                setting.startswith(f"search_path={schema}") for setting in resolver[1]
            )
        )
        return {
            "status": "ready"
            if (
                len(kinds) == len(TABLES)
                and view is not None
                and required_triggers <= present_triggers
                and secured_resolver
            )
            else "upgrade_required",
            "tables_present": len(kinds),
            "tables_expected": len(TABLES),
            "postgres_version": conn.execute(
                "SELECT current_setting('server_version_num')"
            ).fetchone()[0],
        }
    finally:
        conn.execute("ROLLBACK")


def _publish_policy(conn, evidence: dict) -> str:
    policy_hash = policy_content_digest(evidence)
    pid, ver = evidence["policy_id"], evidence["policy_version"]
    conn.execute(
        """INSERT INTO nav_policy_versions
            (policy_id,policy_version,policy_hash,readiness_profile,valuation_frequency,
             calendar_id,calendar_version,calendar_source,timezone,sample_intervals,
              annualization_sessions,coverage_start,coverage_end,valid_through,
              calendar_session_count,calendar_digest,required_nav_kind,
              required_return_semantics,modeling_currency,currency_treatment,
              source_reference,published_at)
             VALUES (%s,%s,%s,%s,'daily',%s,%s,%s,%s,400,252,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
           ON CONFLICT (policy_id,policy_version) DO NOTHING""",
        (
            pid,
            ver,
            policy_hash,
            "current_daily_nav_v1",
            evidence["calendar_id"],
            evidence["calendar_version"],
            evidence["calendar_source"],
            evidence["timezone"],
            evidence["coverage_start"],
            evidence["coverage_end"],
            evidence["valid_through"],
            evidence["calendar_session_count"],
            evidence["calendar_digest"],
            evidence["required_nav_kind"],
            evidence["required_return_semantics"],
            evidence["modeling_currency"],
            evidence["currency_treatment"],
            evidence["source_reference"],
        ),
    )
    existing = conn.execute(
        "SELECT policy_hash FROM nav_policy_versions WHERE policy_id=%s AND policy_version=%s",
        (pid, ver),
    ).fetchone()
    if existing[0] != policy_hash:
        raise ValueError("immutable_policy_conflict")
    for session in evidence["sessions"]:
        fields = (
            evidence["calendar_id"],
            evidence["calendar_version"],
            session["session_date"],
            session["valuation_close_at"],
            session["nav_due_at"],
            evidence["calendar_source"],
            session.get("source_reference") or evidence["source_reference"],
        )
        saved = conn.execute(
            """SELECT valuation_close_at,nav_due_at,calendar_source,source_reference
               FROM nav_valuation_schedules WHERE calendar_id=%s AND calendar_version=%s
                 AND session_date=%s""",
            fields[:3],
        ).fetchone()
        if saved is None:
            conn.execute(
                """INSERT INTO nav_valuation_schedules
                   (calendar_id,calendar_version,session_date,valuation_close_at,nav_due_at,
                    calendar_source,source_reference) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                fields,
            )
            saved = conn.execute(
                """SELECT valuation_close_at,nav_due_at,calendar_source,source_reference
                   FROM nav_valuation_schedules WHERE calendar_id=%s AND calendar_version=%s
                     AND session_date=%s""",
                fields[:3],
            ).fetchone()
        if saved != (
            dt.datetime.fromisoformat(fields[3]),
            dt.datetime.fromisoformat(fields[4]),
            fields[5],
            fields[6],
        ):
            raise ValueError("immutable_session_conflict")
    persisted_sessions = conn.execute(
        """SELECT session_date,valuation_close_at,nav_due_at,source_reference
           FROM nav_valuation_schedules WHERE calendar_id=%s AND calendar_version=%s
           ORDER BY session_date""",
        (evidence["calendar_id"], evidence["calendar_version"]),
    ).fetchall()
    if (
        len(persisted_sessions) != evidence["calendar_session_count"]
        or calendar_digest(persisted_sessions) != evidence["calendar_digest"]
    ):
        raise ValueError("published_calendar_digest_mismatch")
    for row in evidence["instrument_evidence"]:
        saved = conn.execute(
            """SELECT effective_at,fund_status,valuation_frequency,identity_verified,
                      return_basis_verified,currency_verified,evidence_reference
               FROM nav_instrument_policy_evidence
                WHERE instrument_id=%s AND policy_id=%s AND policy_version=%s
                  AND known_at=%s AND effective_at=%s""",
            (row["instrument_id"], pid, ver, row["known_at"], row["effective_at"]),
        ).fetchone()
        expected = (
            dt.datetime.fromisoformat(row["effective_at"]),
            row["fund_status"],
            row["valuation_frequency"],
            bool(row["identity_verified"]),
            bool(row["return_basis_verified"]),
            bool(row["currency_verified"]),
            row["evidence_reference"],
        )
        if saved is not None and saved != expected:
            raise ValueError("immutable_instrument_evidence_conflict")
        if saved is None:
            conn.execute(
                """INSERT INTO nav_instrument_policy_evidence
               (instrument_id,policy_id,policy_version,known_at,effective_at,fund_status,
                valuation_frequency,identity_verified,return_basis_verified,
                currency_verified,evidence_reference)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    row["instrument_id"],
                    pid,
                    ver,
                    row["known_at"],
                    row["effective_at"],
                    row["fund_status"],
                    row["valuation_frequency"],
                    bool(row["identity_verified"]),
                    bool(row["return_basis_verified"]),
                    bool(row["currency_verified"]),
                    row["evidence_reference"],
                ),
            )
    conn.execute(
        """UPDATE nav_policy_versions SET published_at=clock_timestamp()
           WHERE policy_id=%s AND policy_version=%s AND published_at IS NULL""",
        (pid, ver),
    )
    conn.execute(
        """INSERT INTO nav_policy_current (readiness_profile,policy_id,policy_version)
           VALUES ('current_daily_nav_v1',%s,%s)
           ON CONFLICT (readiness_profile) DO UPDATE SET
             policy_id=EXCLUDED.policy_id,policy_version=EXCLUDED.policy_version,
             published_at=clock_timestamp()
           WHERE nav_policy_current.policy_id IS DISTINCT FROM EXCLUDED.policy_id
              OR nav_policy_current.policy_version IS DISTINCT FROM EXCLUDED.policy_version""",
        (pid, ver),
    )
    return policy_hash


def _backfill(conn, instrument_ids: list[str], start: dt.date, end: dt.date) -> int:
    """Mark only proven calendar dates on already-typed, unmodified source NAV.

    Legacy NULL kind/source_nav remains NULL. No NAV/return values or PIT flags
    are created here; existing provenance is never overwritten with conflict.
    """
    policy = conn.execute(
        """SELECT p.policy_id,p.policy_version,p.calendar_id,p.calendar_version,p.calendar_source
           FROM nav_policy_current c JOIN nav_policy_versions p USING(policy_id,policy_version)
           WHERE c.readiness_profile='current_daily_nav_v1' AND p.published_at IS NOT NULL"""
    ).fetchone()
    if not policy:
        raise ValueError("published_policy_required_for_backfill")
    count = 0
    for instrument_id in instrument_ids:
        verified = conn.execute(
            """SELECT 1 FROM nav_instrument_policy_evidence
               WHERE instrument_id=%s AND policy_id=%s AND policy_version=%s
                 AND known_at<=clock_timestamp() AND effective_at<=clock_timestamp()
                 AND valuation_frequency='daily' AND identity_verified
                 AND return_basis_verified AND currency_verified
               ORDER BY known_at DESC LIMIT 1""",
            (instrument_id, policy[0], policy[1]),
        ).fetchone()
        if not verified:
            continue
        updated = conn.execute(
            """UPDATE nav_timeseries n SET calendar_id=s.calendar_id,
                 calendar_version=s.calendar_version, calendar_source=s.calendar_source
               FROM nav_valuation_schedules s
               WHERE n.instrument_id=%s AND n.nav_date BETWEEN %s AND %s
                 AND n.nav_date=s.session_date AND s.calendar_id=%s
                 AND s.calendar_version=%s AND s.calendar_source=%s
                  AND n.source_nav IS NOT NULL AND n.source_nav_kind='adjusted'
                  AND n.currency='USD'
                 AND n.nav_repair_kind='none' AND n.source_nav=n.nav
                 AND n.calendar_id IS NULL AND n.calendar_version IS NULL
                 AND n.calendar_source IS NULL""",
            (instrument_id, start, end, policy[2], policy[3], policy[4]),
        )
        count += updated.rowcount
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("check", "apply"), default="check")
    parser.add_argument("--schema", required=True)
    parser.add_argument("--expected-sql-sha256", required=True)
    parser.add_argument("--plan-sha256")
    parser.add_argument("--policy-file")
    parser.add_argument("--instrument-id", action="append", default=[])
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args(argv)
    try:
        if not SCHEMA_NAME.fullmatch(args.schema):
            raise ValueError("invalid_schema")
        ddl = DDL.read_bytes()
        sql_hash = hashlib.sha256(ddl).hexdigest()
        if not hmac.compare_digest(sql_hash, args.expected_sql_sha256.lower()):
            raise ValueError("ddl_hash_mismatch")
        policy, raw = _policy(args.policy_file)
        ids = [str(uuid.UUID(value)) for value in args.instrument_id]
        if len(ids) > 20 or len(set(ids)) != len(ids):
            raise ValueError("backfill_allowlist_invalid")
        if ids and (not args.start or not args.end):
            raise ValueError("bounded_dates_required")
        start = dt.date.fromisoformat(args.start) if args.start else None
        end = dt.date.fromisoformat(args.end) if args.end else None
        if ids and (end < start or (end - start).days > 600):
            raise ValueError("backfill_window_invalid")
        digest = plan_hash(ddl, args.schema, raw, ids, args.start, args.end)
        if args.mode == "apply" and not hmac.compare_digest(
            digest, args.plan_sha256 or ""
        ):
            raise ValueError("plan_hash_required")
        dsn = os.environ["NAV_READINESS_DATABASE_URL"]
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
            state = _check(conn, args.schema)
            if args.mode == "apply":
                conn.execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(args.schema))
                )
                conn.execute(ddl.decode("utf-8"))
                conn.execute("BEGIN")
                try:
                    conn.execute("SET LOCAL statement_timeout='30s'")
                    policy_hash = _publish_policy(conn, policy) if policy else None
                    changed = _backfill(conn, ids, start, end) if ids else 0
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                state = {
                    "status": "applied",
                    "policy_hash": policy_hash,
                    "backfill_calendar_rows": changed,
                }
        print(
            json.dumps(
                {
                    "mode": args.mode,
                    "schema": args.schema,
                    "sql_sha256": sql_hash,
                    "plan_sha256": digest,
                    **state,
                },
                sort_keys=True,
            )
        )
        return 0
    except (ValueError, TypeError, OSError, KeyError, psycopg.Error) as exc:
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "reason": type(exc).__name__,
                    "sqlstate": exc.sqlstate
                    if isinstance(exc, psycopg.Error)
                    else None,
                }
            )
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
