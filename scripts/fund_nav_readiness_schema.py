"""Governed NAV readiness schema/policy operator. Check is the default; no live action implicit.

Exit codes: 0 success/ready/dry-run, 2 blocked (invalid input, prerequisite,
DB error), 3 ``upgrade_required`` with an incompatible existing schema (no DDL
or maintenance is executed), 4 maintenance lock busy (retryable, no writes).
"""

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

from src.db import LOCK_FUND_NAV_READINESS, LOCK_INSTRUMENT_INGESTION
from src.workers._nav_policy import (
    ADJUSTED_OVERLAP_ABS_TOL,
    ADJUSTED_OVERLAP_REL_TOL,
    CATALOG_EVIDENCE_REFERENCE,
    CURRENT_CATALOG_QUERY_VERSION,
    GENERATOR_VERSION,
    PROVIDER_CONTRACT_VERSION,
    SOURCE_QUERY_SHA256,
    calendar_digest,
    canonical_digest,
    generation_metadata_digest,
    instrument_evidence_digest,
    policy_content_digest,
)
from src.workers._nav_sanitize import REPAIRED_NAV_KINDS

ROOT = Path(__file__).resolve().parents[1]
DDL = ROOT / "schemas" / "fund_nav_readiness_v1.sql"
CATALOG_MANIFEST = ROOT / "schemas" / "fund_nav_readiness_v1.catalog.json"
EXIT_INCOMPATIBLE = 3
EXIT_LOCK_BUSY = 4
MAX_MAINTENANCE_INSTRUMENTS = 20
MAX_MAINTENANCE_WINDOW_DAYS = 600
TABLES = (
    "nav_policy_versions",
    "nav_valuation_schedules",
    "nav_policy_current",
    "nav_instrument_policy_evidence",
    "nav_ingestion_runs",
    "nav_ingestion_attempts",
    "nav_calendar_maintenance_runs",
    "fund_nav_data_heads",
    "fund_nav_data_revisions",
    "fund_nav_reexpression_holds",
    "fund_nav_risk_runs",
    "fund_nav_risk_run_members",
    "fund_nav_risk_publication",
    "fund_nav_risk_exclusions",
    "fund_nav_feature_evidence",
    "fund_nav_readiness_runs",
    "fund_nav_readiness_v1",
    "fund_nav_readiness_current",
)
VIEWS = ("fund_nav_readiness_current_v1",)
# Owned tables plus the external NAV hypertable whose W1 trigger is contract.
TRIGGER_RELATIONS = (*TABLES, "nav_timeseries")
FUNCTIONS = (
    "nav_policy_freeze_v1",
    "nav_instrument_evidence_append_only_v1",
    "nav_uuid_array_unique_v1",
    "nav_calendar_maintenance_guard_v1",
    "fund_nav_revision_append_only_v1",
    "fund_nav_stamp_revision_v1",
    "fund_nav_risk_run_guard_v1",
    "fund_nav_risk_evidence_guard_v1",
    "fund_nav_readiness_freeze_v1",
    "fund_nav_snapshot_current_at_v1",
)


class MaintenanceBusy(RuntimeError):
    """A NAV writer/publisher holds a lock; retry later, nothing was written."""


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


SCHEMA_TOKEN = "@schema@"


def _canonical_text(text: str | None, schema: str) -> str | None:
    """Replace qualifiers of the *target* schema by a stable token.

    ``catalog_signature`` pins ``search_path`` to the target schema, so owned
    objects print unqualified; any remaining qualifier names another schema and
    is kept verbatim (a cross-schema reference is a real difference).
    """
    if text is None:
        return None
    pattern = rf'(?<![\w$."])(?:"{re.escape(schema)}"|{re.escape(schema)})\.'
    return re.sub(pattern, SCHEMA_TOKEN + ".", text)


def _canonical_config(config: list[str] | None, schema: str) -> list[str] | None:
    """Full proconfig; the target schema inside ``search_path`` becomes a token.

    Every other element (``public``, ``pg_temp``, ``$user``...) and every other
    setting is preserved, so an appended path or new setting is a mismatch.
    """
    if config is None:
        return None
    canonical = []
    for entry in config:
        name, _, value = entry.partition("=")
        if name == "search_path":
            parts = []
            for part in value.split(","):
                element = part.strip()
                bare = element[1:-1] if element.startswith('"') and element.endswith('"') else element
                parts.append(SCHEMA_TOKEN if bare == schema else element)
            entry = "search_path=" + ", ".join(parts)
        canonical.append(entry)
    return canonical


def _function_key(name: str, args: str, schema: str) -> str:
    return f"{name}({_canonical_text(args, schema)})"


def catalog_signature(conn, schema: str) -> dict:
    """The one catalog extractor used by ``_check`` and the tracked generator.

    Covers owned relations (kind), column type/nullability/default/generated/
    identity, constraint kind+definition (PK/FK/UNIQUE/CHECK), index
    definitions, view definitions, owned functions keyed ``name(identity args)``
    with body hash and semantic attributes, and *every* non-internal trigger on
    ``TRIGGER_RELATIONS`` of the target schema (owned tables plus the external
    ``nav_timeseries``), including the schema-qualified function it executes.
    Constraint names are excluded (auto-names are not semantics).

    Must run inside an open transaction: ``search_path`` is pinned locally.
    """
    conn.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema)))
    owned = [*TABLES, *VIEWS]
    relations = {
        name: kind
        for name, kind in conn.execute(
            """SELECT c.relname, c.relkind::text FROM pg_class c
               JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname=%s AND c.relname=ANY(%s)
               ORDER BY c.relname""",
            (schema, owned),
        ).fetchall()
    }
    columns: dict[str, list] = {}
    for table, name, data_type, not_null, default, generated, identity in conn.execute(
        """SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
                  a.attnotnull, pg_get_expr(d.adbin, d.adrelid),
                  a.attgenerated::text, a.attidentity::text
           FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
           JOIN pg_attribute a ON a.attrelid=c.oid
           LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
           WHERE n.nspname=%s AND c.relname=ANY(%s)
             AND a.attnum>0 AND NOT a.attisdropped
           ORDER BY c.relname, a.attnum""",
        (schema, owned),
    ).fetchall():
        columns.setdefault(table, []).append(
            [name, data_type, not_null, _canonical_text(default, schema), generated, identity]
        )
    constraints: dict[str, list] = {}
    for table, kind, definition in conn.execute(
        """SELECT c.relname, con.contype::text, pg_get_constraintdef(con.oid, true)
           FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
           JOIN pg_namespace n ON n.oid=c.relnamespace
           WHERE n.nspname=%s AND c.relname=ANY(%s)""",
        (schema, list(TABLES)),
    ).fetchall():
        constraints.setdefault(table, []).append([kind, _canonical_text(definition, schema)])
    indexes: dict[str, list] = {}
    for table, definition in conn.execute(
        """SELECT c.relname, pg_get_indexdef(i.indexrelid)
           FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid
           JOIN pg_namespace n ON n.oid=c.relnamespace
           WHERE n.nspname=%s AND c.relname=ANY(%s)""",
        (schema, list(TABLES)),
    ).fetchall():
        indexes.setdefault(table, []).append(_canonical_text(definition, schema))
    views = {
        name: _canonical_text(definition, schema)
        for name, definition in conn.execute(
            """SELECT c.relname, pg_get_viewdef(c.oid, true) FROM pg_class c
               JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname=%s AND c.relname=ANY(%s) AND c.relkind='v'""",
            (schema, list(VIEWS)),
        ).fetchall()
    }
    functions: dict[str, dict] = {}
    for (
        name, args, result, language, volatility, strict, definer, leakproof,
        parallel, kind, source, config, public_execute,
    ) in conn.execute(
        """SELECT p.proname, pg_get_function_identity_arguments(p.oid),
                  pg_get_function_result(p.oid), l.lanname, p.provolatile::text,
                  p.proisstrict, p.prosecdef, p.proleakproof, p.proparallel::text,
                  p.prokind::text, p.prosrc, p.proconfig,
                  has_function_privilege('public', p.oid, 'EXECUTE')
           FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
           JOIN pg_language l ON l.oid=p.prolang
           WHERE n.nspname=%s AND p.proname=ANY(%s)""",
        (schema, list(FUNCTIONS)),
    ).fetchall():
        functions[_function_key(name, args, schema)] = {
            "result": _canonical_text(result, schema),
            "language": language,
            "volatility": volatility,
            "strict": strict,
            "security_definer": definer,
            "leakproof": leakproof,
            "parallel": parallel,
            "kind": kind,
            "body_sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
            "config": _canonical_config(config, schema),
            "public_execute": public_execute,
        }
    triggers: dict[str, list] = {}
    for table, name, definition, enabled, fn_schema, fn_name, fn_args in conn.execute(
        """SELECT c.relname, t.tgname, pg_get_triggerdef(t.oid, true),
                  t.tgenabled::text, fn_ns.nspname, fn.proname,
                  pg_get_function_identity_arguments(fn.oid)
           FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
           JOIN pg_namespace n ON n.oid=c.relnamespace
           JOIN pg_proc fn ON fn.oid=t.tgfoid
           JOIN pg_namespace fn_ns ON fn_ns.oid=fn.pronamespace
           WHERE n.nspname=%s AND c.relname=ANY(%s) AND NOT t.tgisinternal""",
        (schema, list(TRIGGER_RELATIONS)),
    ).fetchall():
        target = SCHEMA_TOKEN if fn_schema == schema else fn_schema
        triggers.setdefault(table, []).append(
            [name, _canonical_text(definition, schema), enabled,
             f"{target}.{fn_name}({_canonical_text(fn_args, schema)})"]
        )
    for bucket in (constraints, indexes, triggers):
        for values in bucket.values():
            values.sort(key=lambda value: json.dumps(value))
    return {
        "relations": relations,
        "columns": columns,
        "constraints": constraints,
        "indexes": indexes,
        "views": views,
        "functions": dict(sorted(functions.items())),
        "triggers": triggers,
    }


def load_manifest(ddl: bytes) -> dict:
    """The pinned fresh-DDL manifest, rejected when stale or tampered."""
    manifest = json.loads(CATALOG_MANIFEST.read_text(encoding="utf-8"))
    if manifest.get("ddl_sha256") != hashlib.sha256(ddl).hexdigest():
        raise ValueError("catalog_manifest_stale")
    if manifest.get("signature_sha256") != canonical_digest(manifest.get("signature")):
        raise ValueError("catalog_manifest_tampered")
    return manifest


def _recreatable(trigger: list) -> bool:
    """Triggers the idempotent DDL recreates: those executing an owned function."""
    target = trigger[3]
    return target.startswith(SCHEMA_TOKEN + ".") and target.split(".", 1)[1].split("(")[0] in FUNCTIONS


def classify_catalog(expected: dict, actual: dict) -> tuple[str, list[str]]:
    """``exact`` | ``absent`` | ``repairable`` | ``incompatible`` (pure).

    * absent: no owned relation or function, and the external relations carry
      exactly the expected non-W1 triggers (an orphan W1 trigger is not clean).
    * repairable: every non-trigger section is exact and, per trigger relation,
      actual triggers are a subset of expected with only W1-recreatable ones
      missing. Extra, disabled, redefined or foreign-schema triggers are not.
    * incompatible: anything else; no DDL or maintenance may run.
    """
    mismatches = sorted(
        section
        for section in set(expected) | set(actual)
        if actual.get(section) != expected.get(section)
    )
    if not mismatches:
        return "exact", mismatches
    expected_triggers = expected.get("triggers", {})
    actual_triggers = actual.get("triggers", {})
    if not actual.get("relations") and not actual.get("functions"):
        external_clean = all(
            sorted(actual_triggers.get(relation, []), key=json.dumps)
            == sorted(
                (t for t in expected_triggers.get(relation, []) if not _recreatable(t)),
                key=json.dumps,
            )
            for relation in TRIGGER_RELATIONS
        )
        return ("absent" if external_clean else "incompatible"), mismatches
    if mismatches == ["triggers"]:
        repairable = True
        for relation in TRIGGER_RELATIONS:
            wanted = expected_triggers.get(relation, [])
            present = actual_triggers.get(relation, [])
            if any(trigger not in wanted for trigger in present):
                repairable = False
            elif any(t not in present and not _recreatable(t) for t in wanted):
                repairable = False
        if set(actual_triggers) - set(TRIGGER_RELATIONS):
            repairable = False
        if repairable:
            return "repairable", mismatches
    return "incompatible", mismatches


def apply_ddl(conn, schema: str, ddl: bytes) -> None:
    """Apply the readiness DDL exactly as the operator does in production.

    The DDL owns its BEGIN/COMMIT, so the connection must be autocommit. The
    session ``search_path`` is the target schema only: SECURITY DEFINER
    functions capture it via ``SET search_path FROM CURRENT``.
    """
    if not conn.autocommit:
        raise ValueError("autocommit_connection_required_for_ddl")
    conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
    conn.execute(ddl.decode("utf-8"))


def _check(conn, schema: str) -> dict:
    """Read-only catalog comparison against the pinned fresh-DDL manifest.

    ``compatibility`` from ``classify_catalog``; only ``exact`` is ready.
    ``CREATE ... IF NOT EXISTS`` is never treated as an upgrade path.
    """
    manifest = load_manifest(DDL.read_bytes())
    conn.execute("BEGIN TRANSACTION READ ONLY")
    try:
        conn.execute("SET LOCAL statement_timeout = '5s'")
        conn.execute("SET LOCAL lock_timeout = '1s'")
        if not conn.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=%s)", (schema,)
        ).fetchone()[0]:
            raise ValueError("target_schema_missing")
        nav_columns = dict(
            conn.execute(
                """SELECT a.attname, format_type(a.atttypid,a.atttypmod)
                   FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid
                   JOIN pg_attribute a ON a.attrelid=c.oid
                   WHERE n.nspname=%s AND c.relname='nav_timeseries'
                     AND a.attnum>0 AND NOT a.attisdropped""",
                (schema,),
            ).fetchall()
        )
        from scripts.nav_timeseries_provenance_schema import EXPECTED_COLUMNS

        if any(nav_columns.get(name) != data_type for name, data_type in EXPECTED_COLUMNS):
            raise ValueError("nav_provenance_pr132_missing")
        actual = catalog_signature(conn, schema)
        compatibility, mismatches = classify_catalog(manifest["signature"], actual)
        return {
            "status": "ready" if compatibility == "exact" else "upgrade_required",
            "compatibility": compatibility,
            "mismatches": mismatches,
            "tables_present": sum(
                1 for kind in actual["relations"].values() if kind == "r"
            ),
            "tables_expected": len(TABLES),
            "catalog_sha256": canonical_digest(actual),
            "reference_catalog_sha256": manifest["signature_sha256"],
            "postgres_version": conn.execute(
                "SELECT current_setting('server_version_num')"
            ).fetchone()[0],
            "reference_postgres_version": manifest["postgres_version_num"],
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


_SCOPE_ROWS_SQL = """
SELECT instrument_id::text, nav_date, nav::text, source_nav::text, source_nav_kind,
       currency, nav_repair_kind, calendar_id, calendar_version, calendar_source
FROM nav_timeseries
WHERE instrument_id = ANY(%s::uuid[]) AND nav_date BETWEEN %s AND %s
ORDER BY instrument_id, nav_date
"""
_CANDIDATES_SQL = """
WITH life AS (
    SELECT DISTINCT ON (e.instrument_id) e.instrument_id, e.valuation_frequency,
           e.identity_verified, e.return_basis_verified, e.currency_verified
    FROM nav_instrument_policy_evidence e
    WHERE e.instrument_id = ANY(%(ids)s::uuid[])
      AND e.policy_id = %(policy_id)s AND e.policy_version = %(policy_version)s
      AND e.known_at <= clock_timestamp() AND e.effective_at <= clock_timestamp()
    ORDER BY e.instrument_id, e.effective_at DESC, e.known_at DESC
)
SELECT n.instrument_id::text, n.nav_date
FROM nav_timeseries n
JOIN life ON life.instrument_id = n.instrument_id
JOIN nav_valuation_schedules s
  ON s.calendar_id = %(calendar_id)s AND s.calendar_version = %(calendar_version)s
 AND s.session_date = n.nav_date AND s.calendar_source = %(calendar_source)s
WHERE n.instrument_id = ANY(%(ids)s::uuid[])
  AND n.nav_date BETWEEN %(start)s AND %(end)s
  AND n.calendar_id IS NULL AND n.calendar_version IS NULL AND n.calendar_source IS NULL
  AND n.source_nav IS NOT NULL AND n.source_nav_kind = 'adjusted'
  AND n.currency = 'USD' AND n.nav_repair_kind = 'none' AND n.source_nav = n.nav
  AND life.valuation_frequency = 'daily' AND life.identity_verified
  AND life.return_basis_verified AND life.currency_verified
ORDER BY n.instrument_id, n.nav_date
"""


def _pinned_policy(conn, *, lock: bool) -> dict:
    """The current, published, unexpired policy; FOR SHARE pins it in apply."""
    row = conn.execute(
        """SELECT p.policy_id, p.policy_version, p.policy_hash, p.calendar_id,
                  p.calendar_version, p.calendar_source
           FROM nav_policy_current c
           JOIN nav_policy_versions p USING (policy_id, policy_version)
           WHERE c.readiness_profile = 'current_daily_nav_v1'
             AND p.published_at IS NOT NULL AND p.valid_through >= clock_timestamp()"""
        + (" FOR SHARE OF c, p" if lock else "")
    ).fetchone()
    if row is None:
        raise ValueError("published_policy_required_for_backfill")
    return dict(
        zip(
            (
                "policy_id",
                "policy_version",
                "policy_hash",
                "calendar_id",
                "calendar_version",
                "calendar_source",
            ),
            row,
        )
    )


def _scope_rows(conn, ids: list[str], start: dt.date, end: dt.date, *, lock: bool) -> list:
    return [
        list(row)
        for row in conn.execute(
            _SCOPE_ROWS_SQL + (" FOR UPDATE" if lock else ""), (ids, start, end)
        ).fetchall()
    ]


def _candidates(conn, policy: dict, ids: list[str], start: dt.date, end: dt.date) -> list:
    return [
        (iid, day)
        for iid, day in conn.execute(
            _CANDIDATES_SQL, {**policy, "ids": ids, "start": start, "end": end}
        ).fetchall()
    ]


def _stamped(rows: list, candidates: list, policy: dict) -> list:
    wanted = set(candidates)
    tuple_ = [policy["calendar_id"], policy["calendar_version"], policy["calendar_source"]]
    return [row[:7] + tuple_ if (row[0], row[1]) in wanted else row for row in rows]


def _maintenance_plan(conn, ids: list[str], start: dt.date, end: dt.date) -> dict:
    """Dry run: read-only, no locks, no writes; reports what apply would stamp."""
    conn.execute("BEGIN TRANSACTION READ ONLY")
    try:
        conn.execute("SET LOCAL statement_timeout = '30s'")
        policy = _pinned_policy(conn, lock=False)
        before = _scope_rows(conn, ids, start, end, lock=False)
        candidates = _candidates(conn, policy, ids, start, end)
        return {
            "status": "dry_run",
            "operation": "calendar_stamp",
            "policy": {k: policy[k] for k in ("policy_id", "policy_version", "policy_hash")},
            "calendar": [
                policy["calendar_id"],
                policy["calendar_version"],
                policy["calendar_source"],
            ],
            "scope_rows": len(before),
            "eligible_rows": len(candidates),
            "before_digest": canonical_digest(before),
            "expected_after_digest": canonical_digest(_stamped(before, candidates, policy)),
            "candidate_digest": canonical_digest(candidates),
        }
    finally:
        conn.execute("ROLLBACK")


def _apply_calendar_maintenance(
    conn, ids: list[str], start: dt.date, end: dt.date, plan_sha256: str
) -> dict:
    """One maintenance transaction: locks, pins, run, stamp, verify, complete.

    Any failure rolls back levels, revisions and the run together. A replay with
    nothing left to stamp is a no-op without a run or revision. No provider run
    or attempt is created and nothing is fetched.
    """
    conn.execute("BEGIN")
    try:
        conn.execute("SET LOCAL statement_timeout = '60s'")
        conn.execute("SET LOCAL lock_timeout = '2s'")
        for key in (LOCK_INSTRUMENT_INGESTION, LOCK_FUND_NAV_READINESS):
            if not conn.execute("SELECT pg_try_advisory_xact_lock(%s)", (key,)).fetchone()[0]:
                raise MaintenanceBusy("nav_writer_lock_busy")
        policy = _pinned_policy(conn, lock=True)
        before = _scope_rows(conn, ids, start, end, lock=True)
        candidates = _candidates(conn, policy, ids, start, end)
        before_digest = canonical_digest(before)
        if not candidates:
            conn.execute("ROLLBACK")
            return {"status": "no_op", "eligible_rows": 0, "before_digest": before_digest}
        run_id = uuid.uuid4()
        conn.execute(
            """INSERT INTO nav_calendar_maintenance_runs
               (maintenance_run_id, operation, policy_id, policy_version, policy_hash,
                calendar_id, calendar_version, calendar_source, plan_sha256,
                instrument_ids, window_start, window_end, status, before_digest)
               VALUES (%s,'calendar_stamp',%s,%s,%s,%s,%s,%s,%s,%s::uuid[],%s,%s,
                       'running',%s)""",
            (
                run_id,
                policy["policy_id"],
                policy["policy_version"],
                policy["policy_hash"],
                policy["calendar_id"],
                policy["calendar_version"],
                policy["calendar_source"],
                plan_sha256,
                ids,
                start,
                end,
                before_digest,
            ),
        )
        conn.execute("SELECT set_config('nav.ingestion_run_id', '', true)")
        conn.execute(
            "SELECT set_config('nav.maintenance_run_id', %s, true)", (str(run_id),)
        )
        updated = conn.execute(
            """UPDATE nav_timeseries n
               SET calendar_id=%s, calendar_version=%s, calendar_source=%s
               FROM unnest(%s::uuid[], %s::date[]) AS c(instrument_id, nav_date)
               WHERE n.instrument_id = c.instrument_id AND n.nav_date = c.nav_date
                 AND n.calendar_id IS NULL AND n.calendar_version IS NULL
                 AND n.calendar_source IS NULL""",
            (
                policy["calendar_id"],
                policy["calendar_version"],
                policy["calendar_source"],
                [iid for iid, _ in candidates],
                [day for _, day in candidates],
            ),
        ).rowcount
        conn.execute("SELECT set_config('nav.maintenance_run_id', '', true)")
        after = _scope_rows(conn, ids, start, end, lock=False)
        after_digest = canonical_digest(after)
        revisions = conn.execute(
            "SELECT count(*) FROM fund_nav_data_revisions WHERE maintenance_run_id=%s",
            (run_id,),
        ).fetchone()[0]
        if (
            updated != len(candidates)
            or revisions != updated
            or after_digest != canonical_digest(_stamped(before, candidates, policy))
        ):
            raise ValueError("maintenance_verification_failed")
        conn.execute(
            """UPDATE nav_calendar_maintenance_runs
               SET status='completed', completed_at=clock_timestamp(),
                   changed_rows=%s, after_digest=%s
               WHERE maintenance_run_id=%s AND status='running'""",
            (updated, after_digest, run_id),
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return {
        "status": "completed",
        "maintenance_run_id": str(run_id),
        "changed_rows": updated,
        "before_digest": before_digest,
        "after_digest": after_digest,
    }


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, default=str))


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
        if len(ids) > MAX_MAINTENANCE_INSTRUMENTS or len(set(ids)) != len(ids):
            raise ValueError("backfill_allowlist_invalid")
        if ids and (not args.start or not args.end):
            raise ValueError("bounded_dates_required")
        start = dt.date.fromisoformat(args.start) if args.start else None
        end = dt.date.fromisoformat(args.end) if args.end else None
        if ids and (end < start or (end - start).days > MAX_MAINTENANCE_WINDOW_DAYS):
            raise ValueError("backfill_window_invalid")
        digest = plan_hash(ddl, args.schema, raw, ids, args.start, args.end)
        if args.mode == "apply" and not hmac.compare_digest(
            digest, args.plan_sha256 or ""
        ):
            raise ValueError("plan_hash_required")
        dsn = os.environ["NAV_READINESS_DATABASE_URL"]
        header = {
            "mode": args.mode,
            "schema": args.schema,
            "sql_sha256": sql_hash,
            "plan_sha256": digest,
        }
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
            state = _check(conn, args.schema)
            if state["compatibility"] == "incompatible":
                # Old/incompatible shape: no DDL, policy or maintenance mutation.
                _emit({**header, **state})
                return EXIT_INCOMPATIBLE
            conn.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(args.schema))
            )
            if args.mode == "apply":
                apply_ddl(conn, args.schema, ddl)
                if _check(conn, args.schema)["status"] != "ready":
                    raise ValueError("schema_not_ready_after_apply")
                policy_hash = None
                if policy:
                    conn.execute("BEGIN")
                    try:
                        conn.execute("SET LOCAL statement_timeout='30s'")
                        policy_hash = _publish_policy(conn, policy)
                        conn.execute("COMMIT")
                    except BaseException:
                        conn.execute("ROLLBACK")
                        raise
                maintenance = (
                    _apply_calendar_maintenance(conn, ids, start, end, digest)
                    if ids
                    else None
                )
                state = {
                    "status": "applied",
                    "policy_hash": policy_hash,
                    "maintenance": maintenance,
                    "backfill_calendar_rows": (maintenance or {}).get("changed_rows", 0),
                }
            elif ids:
                if state["status"] != "ready":
                    raise ValueError("schema_not_ready_for_maintenance_plan")
                state = {**state, "maintenance_plan": _maintenance_plan(conn, ids, start, end)}
        _emit({**header, **state})
        return 0
    except MaintenanceBusy:
        _emit({"status": "lock_busy", "state": "lock_busy", "retryable": True, "published": False})
        return EXIT_LOCK_BUSY
    except (ValueError, TypeError, OSError, KeyError, psycopg.Error) as exc:
        _emit(
            {
                "status": "blocked",
                "reason": type(exc).__name__,
                "code": str(exc) if isinstance(exc, ValueError) else None,
                "sqlstate": exc.sqlstate if isinstance(exc, psycopg.Error) else None,
            }
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
