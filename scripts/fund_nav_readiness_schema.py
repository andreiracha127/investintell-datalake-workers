"""Governed NAV readiness schema/policy operator. Check is the default; no live action implicit.

Exit codes: 0 success/ready/unchanged/check; 2 input, verification or SQL
failure (DML rolled back); 3 incompatible structure, PR132 prerequisite, access
profile (role missing/unsafe) or external dependency; 4 lock busy with
``dml_committed=false`` (the separate DDL step may still have been applied).

Policy publication (``--policy-file``) is governed: the policy must be a
generator-v2 catalog artifact and must come with a strict all-PASS audit
dossier and the canary manifest of that same audit, all read once from a
private POSIX custody root (``--custody-root``, 0700; files 0600, regular, no
symlink/escape) and pinned by SHA-256 on the command line. The receipt is
validated against the leaf contract only (never by importing the auditor or
the classifier) and is part of the nav-schema-plan-v4 digest. Hand-authored
documents stay a programmatic parser input (``_policy``), never a CLI
publication path. Integrity and linkage are not authorship signatures: an
authorized operator able to rewrite every artifact and hash remains the trust
boundary.

The receipt window is enforced by the database clock at check, again under
the NAV writer locks at apply, and on every replay: ``captured_at <= now <=
policy.valid_through`` and ``now <= sec_valid_until`` (exactly the oldest
matched SEC ``updated_at`` + 7 days). Each committed publication appends one
row to the private ``nav_policy_publication_receipts`` ledger (same
transaction), binding the plan-v4 digest, the policy bytes/document and the
normalized receipt. A pointer already at the target is accepted only as an
exact no-write replay of such a row; a stale plan replays only with the row of
that exact plan digest and audit identity.

Run from the repository root: ``python -m scripts.fund_nav_readiness_schema``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import stat
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql

from scripts.nav_identity_audit_contract import (
    A4_DETAIL_KEYS,
    A8_DETAIL_KEYS,
    A8_FRESHNESS_KEYS,
    A8_OUTCOMES,
    AUDIT_CONTRACT_SHA256,
    AUDIT_CONTRACT_VERSION,
    AUDIT_RECEIPT_KEYS,
    AUDIT_RECEIPT_TIME_KEYS,
    AUDIT_VERSION,
    CANARY_KIND,
    CANARY_MANIFEST_KEYS,
    CANARY_MAX,
    CANARY_SELECTION_KEYS,
    CANARY_STRATA,
    CAPTURE_KIND,
    CATALOG_SOURCE_QUERY_SHA256,
    DOSSIER_DETAIL_KEYS,
    DOSSIER_INPUT_KEYS,
    DOSSIER_KEYS,
    GATE_CHECKS,
    GATE_NAMES,
    PUBLICATION_RECEIPT_RELATION,
    SEC_MAX_SYNCED_AGE_DAYS,
    SEC_QUERY_CONTRACT_SHA256,
    SEC_RELATION,
    SEC_SOURCE_CONTRACT,
    SEC_TIMESTAMP_COLUMN,
)
from scripts.nav_identity_audit_contract import PLAN_VERSION as _LEAF_PLAN_VERSION
from src.db import LOCK_FUND_NAV_READINESS, LOCK_INSTRUMENT_INGESTION
from src.workers._nav_policy import (
    ADJUSTED_OVERLAP_ABS_TOL,
    ADJUSTED_OVERLAP_REL_TOL,
    CATALOG_EVIDENCE_REFERENCE,
    CURRENT_CATALOG_QUERY_VERSION,
    GENERATOR_VERSION,
    PROVIDER_CONTRACT_VERSION,
    RETIRED_CATALOG_QUERY_VERSIONS,
    RETIRED_GENERATOR_VERSIONS,
    SOURCE_QUERY_SHA256,
    calendar_digest,
    canonical_digest,
    generation_metadata_digest,
    instrument_evidence_digest,
    policy_content_digest,
    validate_generation_v2,
)
from src.workers._nav_sanitize import REPAIRED_NAV_KINDS

ROOT = Path(__file__).resolve().parents[1]
DDL = ROOT / "schemas" / "fund_nav_readiness_v1.sql"
# Versioned audit code/config pinned by the receipt (repository files, not 0600).
AUDIT_VERIFIER = ROOT / "scripts" / "verify_fund_nav_identity_v2.py"
AUDIT_CONFIG = ROOT / "configs" / "nav_identity_audit_v2.json"
MAX_CUSTODY_BYTES = 512 * 1024 * 1024
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
    PUBLICATION_RECEIPT_RELATION,
    "nav_ingestion_runs",
    "nav_ingestion_attempts",
    "nav_calendar_maintenance_runs",
    "fund_nav_data_heads",
    "fund_nav_data_revisions",
    "nav_ingestion_row_evidence",
    "nav_rebase_receipts",
    "fund_nav_reexpression_events",
    "fund_nav_risk_runs",
    "fund_nav_risk_run_members",
    "fund_nav_risk_publication",
    "fund_nav_risk_exclusions",
    "fund_nav_feature_evidence",
    "fund_nav_readiness_runs",
    "fund_nav_readiness_v1",
    "fund_nav_readiness_current",
)
# fund_nav_reexpression_holds is a private view over the event ledger (R2-A);
# a pre-R2 table of that name is an incompatible shape, never migrated.
VIEWS = ("fund_nav_reexpression_holds", "fund_nav_readiness_current_v1")
# Owned tables plus the external NAV hypertable whose W1 trigger is contract.
TRIGGER_RELATIONS = (*TABLES, "nav_timeseries")
FUNCTIONS = (
    "nav_policy_freeze_v1",
    "nav_policy_pointer_stamp_v1",
    "nav_instrument_evidence_append_only_v1",
    "nav_policy_publication_receipt_guard_v1",
    "nav_ingestion_run_guard_v1",
    "nav_ingestion_attempt_guard_v1",
    "nav_level_evidence_digest_v1",
    "nav_uuid_array_unique_v1",
    "nav_calendar_maintenance_guard_v1",
    "fund_nav_revision_append_only_v1",
    "fund_nav_revision_attribution_v1",
    "nav_row_evidence_guard_v1",
    "nav_row_evidence_verify_v1",
    "nav_rebase_receipt_guard_v1",
    "nav_rebase_receipt_verify_v1",
    "fund_nav_reexpression_event_guard_v1",
    "fund_nav_stamp_revision_v1",
    "fund_nav_risk_run_guard_v1",
    "fund_nav_risk_evidence_guard_v1",
    "fund_nav_readiness_freeze_v1",
    "fund_nav_readiness_pointer_stamp_v1",
    "fund_nav_snapshot_current_at_v1",
)
# Additive objects of the plan-v4 publication receipt: an older W1 schema that
# lacks ALL of them (and is otherwise exact/repairable) is repairable by the
# idempotent DDL; any partial or reshaped presence stays incompatible.
ADDITIVE_RELATIONS = (PUBLICATION_RECEIPT_RELATION,)
ADDITIVE_FUNCTIONS = ("nav_policy_publication_receipt_guard_v1",)
# N6 access profile: the Light read runtime.
ACCESS_PROFILE = "light_app_runtime_v1"
ACCESS_ROLE = "app_runtime"
READ_RELATIONS = (
    "nav_policy_versions",
    "nav_policy_current",
    "nav_valuation_schedules",
    "fund_nav_readiness_runs",
    "fund_nav_readiness_v1",
    "fund_nav_readiness_current",
    "fund_nav_readiness_current_v1",
)
EXECUTE_FUNCTIONS = ("fund_nav_snapshot_current_at_v1",)
RELATION_PRIVILEGES = (
    "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN",
)
COLUMN_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "REFERENCES")
SEQUENCE_PRIVILEGES = ("USAGE", "SELECT", "UPDATE")
# Roles whose reachability from app_runtime is a write/ownership escape.
WRITER_ROLES = ("worker_writer",)
# External read dependencies of the Light NAV consumer (not owned, not hashed).
# Columns are the ones the Light NAV flow reads: the readiness-family join
# (instrument_id, calc_date) and select_universe_funds' quality gate
# (nav_quality_ok, backend/app/optimizer/data.py); NAV levels for returns.
DEPENDENCIES = {
    "fund_risk_latest_mv": {
        "relkind": "m",
        "columns": {"instrument_id": "uuid", "calc_date": "date", "nav_quality_ok": "boolean"},
    },
    "nav_timeseries": {
        "relkind": "r",
        "columns": {"instrument_id": "uuid", "nav_date": "date", "nav": "numeric(18,6)"},
    },
}
# Any of these, effective for app_runtime or a role it can reach (INHERIT,
# SET, PUBLIC or ownership), makes a read dependency unsafe: writes, refresh/
# maintenance, trigger creation and foreign-key references.
WRITE_PRIVILEGES = ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN")
COLUMN_WRITE_PRIVILEGES = ("INSERT", "UPDATE", "REFERENCES")
EXIT_BLOCKED = 2
# v4 adds the governed audit receipt (``audit`` and ``operation.audit``);
# v3 added the full-document policy digest and the explicit operation
# identity. v1/v2/v3 digests of the same inputs are recognised and rejected.
PLAN_VERSION = _LEAF_PLAN_VERSION
PREVIOUS_PLAN_VERSION = "nav-schema-plan-v3"
V2_PLAN_VERSION = "nav-schema-plan-v2"
_V3_ONLY_FIELDS = ("policy_document_digest", "operation")
_V4_ONLY_FIELDS = ("audit",)


class MaintenanceBusy(RuntimeError):
    """A NAV writer/publisher holds a lock; retry later, nothing was written."""


SCHEMA_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_$]{0,62}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
LOWER_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")


def _unique_pairs(pairs: list) -> dict:
    keys = [key for key, _value in pairs]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate_key")
    return dict(pairs)


def _no_constant(_name: str):
    raise ValueError("non_finite_constant")


def _strict_object(raw: bytes, code: str) -> dict:
    """Strict JSON object: duplicate keys, NaN/Infinity or non-object → code."""
    try:
        value = json.loads(raw, object_pairs_hook=_unique_pairs, parse_constant=_no_constant)
    except (ValueError, UnicodeDecodeError, RecursionError) as exc:
        raise ValueError(code) from exc
    if not isinstance(value, dict):
        raise ValueError(code)
    return value


def _document_bytes(value) -> bytes:
    """Canonical artifact bytes (sorted keys, compact, ASCII, trailing LF)."""
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _policy(path: str | dict | bytes | None) -> tuple[dict | None, bytes]:
    """Parse and validate a policy document (programmatic input).

    Accepts a path, a dict or raw bytes. This parser is the shared validation
    of hand-authored fixtures and generated artifacts; it is NOT the CLI
    publication path, which additionally requires the governed audit receipt
    (``_governed_policy``).
    """
    if path is None:
        return None, b""
    if isinstance(path, dict):
        policy = path
        raw = (
            json.dumps(policy, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
    else:
        raw = path if isinstance(path, bytes) else Path(path).read_bytes()
        policy = _strict_object(raw, "policy_not_object")
    if not isinstance(policy, dict):
        raise ValueError("policy_not_object")
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
    # Nested shapes before any lookup: a list/number where an object is
    # expected is a static code, never an AttributeError.
    if not isinstance(policy["sessions"], list) or any(
        not isinstance(entry, dict) for entry in policy["sessions"]
    ):
        raise ValueError("policy_sessions_invalid")
    if not isinstance(policy["instrument_evidence"], list) or any(
        not isinstance(row, dict) or not isinstance(row.get("instrument_id"), str)
        for row in policy["instrument_evidence"]
    ):
        raise ValueError("instrument_evidence_shape_invalid")
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
    # Retired (v1) and generator-owned catalog references can never be applied
    # by this operator without full current-generator metadata: a v1 artifact
    # stays immutable and is never re-published or re-audited by v2 code.
    catalog_prefixes = tuple(
        f"{version}:"
        for version in (*RETIRED_CATALOG_QUERY_VERSIONS, CURRENT_CATALOG_QUERY_VERSION)
    )

    def _retired(value: object, retired: frozenset[str]) -> bool:
        return isinstance(value, str) and value in retired

    if _retired(policy.get("generator_version"), RETIRED_GENERATOR_VERSIONS) or (
        isinstance(generation, dict)
        and (
            _retired(generation.get("generator_version"), RETIRED_GENERATOR_VERSIONS)
            or _retired(
                generation.get("source_query_version"), RETIRED_CATALOG_QUERY_VERSIONS
            )
        )
    ):
        raise ValueError("generator_metadata_invalid")
    if generation is None and (
        "generator_version" in policy
        or "provider_contract" in policy
        or any(
            row["evidence_reference"].startswith(catalog_prefixes)
            for row in policy["instrument_evidence"]
        )
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
            # v2 identity diagnostics: partition, first-failure closure, flags
            # and ACTIVE digests are recounted from the document itself.
            validate_generation_v2(policy)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("generator_metadata_invalid") from exc
    return policy, raw


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
    access = manifest.get("access_profile") or {}
    if (
        access.get("name") != ACCESS_PROFILE
        or access.get("role") != ACCESS_ROLE
        or access.get("read_relations") != list(READ_RELATIONS)
        or access.get("execute_functions") != list(EXECUTE_FUNCTIONS)
        or access.get("signature_sha256") != canonical_digest(access.get("signature"))
    ):
        raise ValueError("catalog_manifest_tampered")
    return manifest


def _recreatable(trigger: list) -> bool:
    """Triggers the idempotent DDL recreates: those executing an owned function."""
    target = trigger[3]
    return target.startswith(SCHEMA_TOKEN + ".") and target.split(".", 1)[1].split("(")[0] in FUNCTIONS


def _without_additive(signature: dict) -> dict:
    """The signature minus every object of the additive publication receipt.

    Only ``ADDITIVE_RELATIONS`` (with their columns, constraints, indexes and
    triggers) and ``ADDITIVE_FUNCTIONS`` are removed; nothing else is ever
    normalized away.
    """
    reduced = json.loads(json.dumps(signature))
    for section in ("relations", "columns", "constraints", "indexes", "triggers"):
        bucket = reduced.get(section)
        if isinstance(bucket, dict):
            for name in ADDITIVE_RELATIONS:
                bucket.pop(name, None)
    functions = reduced.get("functions")
    if isinstance(functions, dict):
        reduced["functions"] = {
            key: value
            for key, value in functions.items()
            if key.split("(", 1)[0] not in ADDITIVE_FUNCTIONS
        }
    return reduced


def _additive_absent(actual: dict) -> bool:
    """No trace of the additive receipt objects in the actual catalog."""
    return not any(
        name in actual.get(section, {})
        for section in ("relations", "columns", "constraints", "indexes", "triggers")
        for name in ADDITIVE_RELATIONS
    ) and not any(
        key.split("(", 1)[0] in ADDITIVE_FUNCTIONS for key in actual.get("functions", {})
    )


def classify_catalog(expected: dict, actual: dict) -> tuple[str, list[str]]:
    """``exact`` | ``absent`` | ``repairable`` | ``incompatible`` (pure).

    * absent: no owned relation or function, and the external relations carry
      exactly the expected non-W1 triggers (an orphan W1 trigger is not clean).
    * repairable: every non-trigger section is exact and, per trigger relation,
      actual triggers are a subset of expected with only W1-recreatable ones
      missing. Extra, disabled, redefined or foreign-schema triggers are not.
      Also repairable: an older W1 schema lacking EVERY additive publication-
      receipt object (table, function, trigger, constraints, index) and
      otherwise matching under the same rules; a partially present or
      reshaped additive object is incompatible.
    * incompatible: anything else; no DDL or maintenance may run.
    """
    return _classify_catalog(expected, actual, additive=True)


def _classify_catalog(
    expected: dict, actual: dict, *, additive: bool
) -> tuple[str, list[str]]:
    mismatches = sorted(
        section
        for section in set(expected) | set(actual)
        if actual.get(section) != expected.get(section)
    )
    if not mismatches:
        return "exact", mismatches
    if additive and actual.get("relations") and _additive_absent(actual):
        base, _ = _classify_catalog(_without_additive(expected), actual, additive=False)
        return ("repairable" if base in ("exact", "repairable") else "incompatible"), (
            mismatches
        )
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
    session path is ``<schema>, pg_temp`` (identifier-quoted): every W1
    function captures exactly that path via ``SET search_path FROM CURRENT``,
    so pg_temp is searched last and ``$user``/public are never trusted.
    """
    if not conn.autocommit:
        raise ValueError("autocommit_connection_required_for_ddl")
    conn.execute(sql.SQL("SET search_path TO {}, pg_temp").format(sql.Identifier(schema)))
    conn.execute(ddl.decode("utf-8"))


class PrerequisiteBlocked(ValueError):
    """Structure/ACL/dependency state that must block before any DDL or DML."""


# ──────────────────────────────────────────────────────────────────────────────
# N5: PR132 nav_timeseries provenance prerequisite (reused extractor)
# ──────────────────────────────────────────────────────────────────────────────
def pr132_violations(relkind: str | None, state: dict[str, tuple]) -> list[str]:
    """Exact PR132 contract: plain table and 11 columns ``(type,False,'','',False)``.

    The tuple is (type, not null, generated, identity, has default) from
    ``scripts.nav_timeseries_provenance_schema._column_state``.
    """
    from scripts.nav_timeseries_provenance_schema import EXPECTED_COLUMNS

    if relkind is None:
        return ["relation:missing"]
    issues = [] if relkind == "r" else [f"relkind:{relkind}"]
    for name, type_name in EXPECTED_COLUMNS:
        actual = state.get(name)
        if actual is None:
            issues.append(f"missing:{name}")
        elif tuple(actual) != (type_name, False, "", "", False):
            issues.append(f"mismatch:{name}")
    return issues


def _pr132_state(conn, schema: str) -> list[str]:
    from scripts.nav_timeseries_provenance_schema import _column_state

    relation = conn.execute(
        """SELECT c.oid, c.relkind::text FROM pg_catalog.pg_class c
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = %s AND c.relname = 'nav_timeseries'""",
        (schema,),
    ).fetchone()
    if relation is None:
        return pr132_violations(None, {})
    return pr132_violations(relation[1], _column_state(conn, relation[0]))


# ──────────────────────────────────────────────────────────────────────────────
# N6: access profile light_app_runtime_v1
# ──────────────────────────────────────────────────────────────────────────────
_REACH_SQL = """
WITH RECURSIVE reach(roleid, path, via_admin) AS (
    SELECT %(role)s::oid, ARRAY[%(role)s::oid], false
    UNION ALL
    SELECT m.roleid, reach.path || m.roleid, reach.via_admin OR m.admin_option
    FROM reach JOIN pg_catalog.pg_auth_members m ON m.member = reach.roleid
    WHERE m.roleid <> ALL (reach.path) AND cardinality(reach.path) < 64
)
SELECT reach.roleid, reach.via_admin,
       array_to_string(ARRAY(SELECT r.rolname FROM unnest(reach.path) WITH ORDINALITY
                             AS p(oid, ord) JOIN pg_catalog.pg_roles r ON r.oid = p.oid
                             ORDER BY p.ord), '>') AS path_names,
       role.rolname, role.rolsuper, role.rolcreaterole, role.rolbypassrls
FROM reach JOIN pg_catalog.pg_roles role ON role.oid = reach.roleid
"""


def _reachable_roles(conn, role_oid: int) -> list[tuple]:
    """Every role reachable through any membership edge (INHERIT, SET or ADMIN).

    NOINHERIT is not treated as safe: a SET path still lets the role act as the
    target. PostgreSQL rejects membership cycles; the path guard bounds depth.
    """
    return conn.execute(_REACH_SQL, {"role": role_oid}).fetchall()


def access_profile_signature(conn, schema: str) -> dict:
    """Deterministic effective-access state of ``app_runtime`` on W1 objects.

    No OIDs or grantor IDs. Effective privileges are the union over every role
    reachable from app_runtime (``has_*_privilege`` covers INHERIT and PUBLIC;
    the union covers SET ROLE paths). Must run inside an open transaction.
    """
    role = conn.execute(
        """SELECT oid, rolsuper, rolcreaterole, rolbypassrls, rolreplication
           FROM pg_catalog.pg_roles WHERE rolname = %s""",
        (ACCESS_ROLE,),
    ).fetchone()
    if role is None:
        return {"role": {"name": ACCESS_ROLE, "exists": False}}
    reach = _reachable_roles(conn, role[0])
    roles = [row[0] for row in reach]
    owners = {
        row[0]
        for row in conn.execute(
            """SELECT c.relowner FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = %(s)s AND c.relname = ANY(%(rels)s)
               UNION SELECT p.proowner FROM pg_catalog.pg_proc p
               JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
               WHERE n.nspname = %(s)s AND p.proname = ANY(%(fns)s)
               UNION SELECT n.nspowner FROM pg_catalog.pg_namespace n
               WHERE n.nspname = %(s)s""",
            {"s": schema, "rels": [*TABLES, *VIEWS], "fns": list(FUNCTIONS)},
        ).fetchall()
    }
    unsafe = sorted(
        f"{reason}:{path}"
        for roleid, via_admin, path, name, superuser, createrole, bypassrls in reach
        for reason, hit in (
            ("owner", roleid in owners),
            ("writer", name in WRITER_ROLES),
            ("superuser", superuser),
            ("createrole", createrole),
            ("bypassrls", bypassrls),
            ("admin_option", via_admin),
        )
        if hit
    )
    relations: dict[str, dict] = {}
    for relname, priv, effective, grantable in conn.execute(
        """SELECT c.relname, p.priv,
                  bool_or(has_table_privilege(r.roleid, c.oid, p.priv)),
                  bool_or(has_table_privilege(r.roleid, c.oid, p.priv || ' WITH GRANT OPTION'))
           FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           CROSS JOIN unnest(%(privs)s::text[]) AS p(priv)
           CROSS JOIN unnest(%(roles)s::oid[]) AS r(roleid)
           WHERE n.nspname = %(s)s AND c.relname = ANY(%(rels)s)
           GROUP BY c.relname, p.priv""",
        {"privs": list(RELATION_PRIVILEGES), "roles": roles, "s": schema,
         "rels": [*TABLES, *VIEWS]},
    ).fetchall():
        entry = relations.setdefault(
            relname,
            {"effective": [], "grantable": [], "direct": [], "public": [],
             "column_effective": [], "column_acl": []},
        )
        if effective:
            entry["effective"].append(priv)
        if grantable:
            entry["grantable"].append(priv)
    for relname, grantee, priv in conn.execute(
        """SELECT c.relname, a.grantee, a.privilege_type
           FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace,
                aclexplode(c.relacl) a
           WHERE n.nspname = %s AND c.relname = ANY(%s)
             AND (a.grantee = 0 OR a.grantee = %s)""",
        (schema, [*TABLES, *VIEWS], role[0]),
    ).fetchall():
        if relname in relations:
            relations[relname]["public" if grantee == 0 else "direct"].append(priv)
    for relname, priv, effective in conn.execute(
        """SELECT c.relname, p.priv, bool_or(has_any_column_privilege(r.roleid, c.oid, p.priv))
           FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           CROSS JOIN unnest(%(privs)s::text[]) AS p(priv)
           CROSS JOIN unnest(%(roles)s::oid[]) AS r(roleid)
           WHERE n.nspname = %(s)s AND c.relname = ANY(%(rels)s)
           GROUP BY c.relname, p.priv""",
        {"privs": list(COLUMN_PRIVILEGES), "roles": roles, "s": schema,
         "rels": [*TABLES, *VIEWS]},
    ).fetchall():
        if effective and relname in relations:
            relations[relname]["column_effective"].append(priv)
    for relname, attname, grantee, priv in conn.execute(
        """SELECT c.relname, att.attname, a.grantee, a.privilege_type
           FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           JOIN pg_catalog.pg_attribute att ON att.attrelid = c.oid AND att.attacl IS NOT NULL,
                aclexplode(att.attacl) a
           WHERE n.nspname = %s AND c.relname = ANY(%s)
             AND (a.grantee = 0 OR a.grantee = ANY(%s))""",
        (schema, [*TABLES, *VIEWS], roles),
    ).fetchall():
        if relname in relations:
            label = "PUBLIC" if grantee == 0 else "reachable"
            relations[relname]["column_acl"].append(f"{attname}:{label}:{priv}")
    sequences: dict[str, dict] = {}
    for seqname, priv, effective, public in conn.execute(
        """SELECT s.relname, p.priv,
                  bool_or(has_sequence_privilege(r.roleid, s.oid, p.priv)),
                  EXISTS (SELECT 1 FROM aclexplode(s.relacl) a WHERE a.grantee = 0)
           FROM pg_catalog.pg_class s
           JOIN pg_catalog.pg_depend d ON d.objid = s.oid
            AND d.classid = 'pg_catalog.pg_class'::regclass
            AND d.refclassid = 'pg_catalog.pg_class'::regclass AND d.deptype IN ('a','i')
           JOIN pg_catalog.pg_class t ON t.oid = d.refobjid
           JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
           CROSS JOIN unnest(%(privs)s::text[]) AS p(priv)
           CROSS JOIN unnest(%(roles)s::oid[]) AS r(roleid)
           WHERE s.relkind = 'S' AND n.nspname = %(s)s AND t.relname = ANY(%(rels)s)
           GROUP BY s.relname, s.relacl, p.priv""",
        {"privs": list(SEQUENCE_PRIVILEGES), "roles": roles, "s": schema,
         "rels": list(TABLES)},
    ).fetchall():
        entry = sequences.setdefault(seqname, {"effective": [], "public": public})
        if effective:
            entry["effective"].append(priv)
    functions: dict[str, dict] = {}
    for name, args, execute, grantable, direct, public, definer, owner_matches in conn.execute(
        """SELECT p.proname, pg_get_function_identity_arguments(p.oid),
                  bool_or(has_function_privilege(r.roleid, p.oid, 'EXECUTE')),
                  bool_or(has_function_privilege(r.roleid, p.oid,
                                                 'EXECUTE WITH GRANT OPTION')),
                  EXISTS (SELECT 1 FROM aclexplode(p.proacl) a
                          WHERE a.grantee = %(role)s AND a.privilege_type = 'EXECUTE'),
                  has_function_privilege('public', p.oid, 'EXECUTE'),
                  p.prosecdef,
                  p.proowner = (SELECT c.relowner FROM pg_catalog.pg_class c
                                WHERE c.relnamespace = n.oid
                                  AND c.relname = 'fund_nav_readiness_v1')
           FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
           CROSS JOIN unnest(%(roles)s::oid[]) AS r(roleid)
           WHERE n.nspname = %(s)s AND p.proname = ANY(%(fns)s)
           GROUP BY p.oid, p.proname, p.proacl, p.prosecdef, p.proowner, n.oid""",
        {"role": role[0], "roles": roles, "s": schema, "fns": list(FUNCTIONS)},
    ).fetchall():
        functions[_function_key(name, args, schema)] = {
            "execute": execute,
            "grantable": grantable,
            "direct": direct,
            "public": public,
            "definer_owner_consistent": bool(owner_matches) if definer else None,
        }
    schema_row = conn.execute(
        """SELECT bool_or(has_schema_privilege(r.roleid, n.oid, 'USAGE')),
                  bool_or(has_schema_privilege(r.roleid, n.oid, 'CREATE')),
                  EXISTS (SELECT 1 FROM aclexplode(n.nspacl) a
                          WHERE a.grantee = 0 AND a.privilege_type = 'CREATE')
           FROM pg_catalog.pg_namespace n CROSS JOIN unnest(%s::oid[]) AS r(roleid)
           WHERE n.nspname = %s GROUP BY n.oid, n.nspacl""",
        (roles, schema),
    ).fetchone()
    for bucket in relations.values():
        for key in ("effective", "grantable", "direct", "public", "column_effective",
                    "column_acl"):
            bucket[key].sort()
    for bucket in sequences.values():
        bucket["effective"].sort()
    return {
        "role": {
            "name": ACCESS_ROLE,
            "exists": True,
            "superuser": role[1],
            "createrole": role[2],
            "bypassrls": role[3],
            "replication": role[4],
        },
        "relations": dict(sorted(relations.items())),
        "sequences": dict(sorted(sequences.items())),
        "functions": dict(sorted(functions.items())),
        "schema": {
            "usage": bool(schema_row and schema_row[0]),
            "create": bool(schema_row and schema_row[1]),
            "public_create": bool(schema_row and schema_row[2]),
        },
        "unsafe_membership": unsafe,
    }


def classify_access(expected: dict, actual: dict) -> tuple[str, list[str]]:
    """``exact`` | ``role_missing`` | ``repairable`` | ``incomplete`` | ``unsafe``.

    Repairable: after filling absent W1 objects with their expected entry and
    restoring only the expected read grants (SELECT on the seven read models,
    EXECUTE on the snapshot), the state equals the expected profile.
    Incomplete: additionally only schema USAGE is missing, which W1 never
    grants (an external, governed prerequisite). Anything else (extra,
    grant-option, column or PUBLIC privileges, reachable writer/owner or
    privileged roles, schema CREATE) is unsafe and is never normalized away.
    """
    if not actual.get("role", {}).get("exists"):
        return "role_missing", ["role"]
    mismatches = sorted(
        key for key in set(expected) | set(actual) if actual.get(key) != expected.get(key)
    )
    if not mismatches:
        return "exact", []
    repaired = json.loads(json.dumps(actual))
    for bucket in ("relations", "sequences", "functions"):
        for name, wanted in expected.get(bucket, {}).items():
            present = repaired.setdefault(bucket, {}).get(name)
            if present is None:
                repaired[bucket][name] = wanted
                continue
            if bucket == "relations" and name in READ_RELATIONS:
                for key in ("effective", "direct", "column_effective"):
                    if "SELECT" in wanted[key] and "SELECT" not in present[key]:
                        present[key] = sorted([*present[key], "SELECT"])
            if bucket == "functions" and name.split("(")[0] in EXECUTE_FUNCTIONS:
                present["execute"] = present["execute"] or wanted["execute"]
                present["direct"] = present["direct"] or wanted["direct"]
    if repaired == expected:
        return "repairable", mismatches
    wanted_schema = expected.get("schema", {})
    present_schema = repaired.get("schema", {})
    if wanted_schema.get("usage") and not present_schema.get("usage"):
        repaired["schema"] = {**present_schema, "usage": True}
        if repaired == expected:
            return "incomplete", mismatches
    return "unsafe", mismatches


def dependency_checks(conn, schema: str) -> dict[str, str]:
    """External read dependencies of the Light consumer (never granted here).

    ``ok`` only with the expected relkind and consumed column types, SELECT for
    app_runtime and NO effective write-class privilege for app_runtime or any
    role it reaches (INHERIT, SET or ADMIN membership; PUBLIC and ownership are
    covered by ``has_*_privilege``): table INSERT/UPDATE/DELETE/TRUNCATE/
    REFERENCES/TRIGGER/MAINTAIN, any column INSERT/UPDATE/REFERENCES
    (``writable``), and no grant option on any privilege (``grantable``).
    Detection only: nothing is ever revoked here.
    """
    role = conn.execute(
        "SELECT oid FROM pg_catalog.pg_roles WHERE rolname = %s", (ACCESS_ROLE,)
    ).fetchone()
    roles = [row[0] for row in _reachable_roles(conn, role[0])] if role else []
    results: dict[str, str] = {}
    for name, spec in DEPENDENCIES.items():
        relation = conn.execute(
            """SELECT c.oid, c.relkind::text FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = %s AND c.relname = %s""",
            (schema, name),
        ).fetchone()
        if relation is None:
            results[name] = "missing"
            continue
        if relation[1] != spec["relkind"]:
            results[name] = "wrong_kind"
            continue
        columns = dict(
            conn.execute(
                """SELECT attname, format_type(atttypid, atttypmod)
                   FROM pg_catalog.pg_attribute
                   WHERE attrelid = %s AND attnum > 0 AND NOT attisdropped""",
                (relation[0],),
            ).fetchall()
        )
        if any(columns.get(col) != typ for col, typ in spec["columns"].items()):
            results[name] = "column_mismatch"
            continue
        if not role:
            results[name] = "role_missing"
            continue
        params = {"role": role[0], "rel": relation[0], "roles": roles}
        readable = conn.execute(
            "SELECT has_table_privilege(%(role)s, %(rel)s, 'SELECT')", params
        ).fetchone()[0]
        writable, grantable = conn.execute(
            """SELECT
                 EXISTS (SELECT 1 FROM unnest(%(roles)s::oid[]) AS r(roleid)
                         CROSS JOIN unnest(%(privs)s::text[]) AS p(priv)
                         WHERE has_table_privilege(r.roleid, %(rel)s, p.priv))
                 OR EXISTS (SELECT 1 FROM unnest(%(roles)s::oid[]) AS r(roleid)
                            CROSS JOIN unnest(%(cols)s::text[]) AS p(priv)
                            WHERE has_any_column_privilege(r.roleid, %(rel)s, p.priv)),
                 EXISTS (SELECT 1 FROM unnest(%(roles)s::oid[]) AS r(roleid)
                         CROSS JOIN unnest(%(all)s::text[]) AS p(priv)
                         WHERE has_table_privilege(r.roleid, %(rel)s,
                                                   p.priv || ' WITH GRANT OPTION'))
                 OR EXISTS (SELECT 1 FROM unnest(%(roles)s::oid[]) AS r(roleid)
                            CROSS JOIN unnest(%(allcols)s::text[]) AS p(priv)
                            WHERE has_any_column_privilege(r.roleid, %(rel)s,
                                                           p.priv || ' WITH GRANT OPTION'))""",
            {
                **params,
                "privs": list(WRITE_PRIVILEGES),
                "cols": list(COLUMN_WRITE_PRIVILEGES),
                "all": list(RELATION_PRIVILEGES),
                "allcols": list(COLUMN_PRIVILEGES),
            },
        ).fetchone()
        if writable:
            results[name] = "writable"
        elif grantable:
            results[name] = "grantable"
        else:
            results[name] = "ok" if readable else "select_missing"
    return results


def _check(conn, schema: str) -> dict:
    """Read-only structure + access + dependency comparison (no DDL, no DML).

    ``compatibility`` (structure) from ``classify_catalog``; ``access`` from
    ``classify_access``; ``dependencies`` from ``dependency_checks``. Ready only
    when structure and access are exact and every dependency is ``ok``; a
    structurally exact schema with an unsafe profile is never ready. Raises
    ``PrerequisiteBlocked`` for PR132 violations before reading anything else.
    """
    manifest = load_manifest(DDL.read_bytes())
    conn.execute("BEGIN TRANSACTION READ ONLY")
    try:
        conn.execute("SET LOCAL statement_timeout = '10s'")
        conn.execute("SET LOCAL lock_timeout = '1s'")
        if not conn.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=%s)", (schema,)
        ).fetchone()[0]:
            raise ValueError("target_schema_missing")
        pr132 = _pr132_state(conn, schema)
        if pr132:
            code = (
                "nav_provenance_pr132_missing"
                if all(item.startswith(("missing:", "relation:")) for item in pr132)
                else "nav_provenance_pr132_incompatible"
            )
            raise PrerequisiteBlocked(code, pr132)
        actual = catalog_signature(conn, schema)
        compatibility, mismatches = classify_catalog(manifest["signature"], actual)
        access = access_profile_signature(conn, schema)
        access_status, access_mismatches = classify_access(
            manifest["access_profile"]["signature"], access
        )
        dependencies = dependency_checks(conn, schema)
        ready = (
            compatibility == "exact"
            and access_status == "exact"
            and all(value == "ok" for value in dependencies.values())
        )
        if compatibility == "incompatible":
            code = "incompatible_schema"
        elif access_status == "unsafe":
            code = "blocked_access"
        elif access_status == "role_missing":
            code = "blocked_role_missing"
        elif access_status == "incomplete":
            code = "blocked_schema_usage_missing"
        elif any(value != "ok" for value in dependencies.values()):
            code = "blocked_dependency"
        else:
            code = None
        return {
            "status": "ready" if ready else "upgrade_required",
            "ready": ready,
            "compatibility": compatibility,
            "mismatches": mismatches,
            "access_profile": ACCESS_PROFILE,
            "access": access_status,
            "access_mismatches": access_mismatches,
            "dependencies": dependencies,
            "code": code,
            "tables_present": sum(
                1 for kind in actual["relations"].values() if kind == "r"
            ),
            "tables_expected": len(TABLES),
            "catalog_sha256": canonical_digest(actual),
            "reference_catalog_sha256": manifest["signature_sha256"],
            "access_sha256": canonical_digest(access),
            "reference_access_sha256": manifest["access_profile"]["signature_sha256"],
            "postgres_version": conn.execute(
                "SELECT current_setting('server_version_num')"
            ).fetchone()[0],
            "reference_postgres_version": manifest["postgres_version_num"],
        }
    finally:
        conn.execute("ROLLBACK")


# Blocks before DDL: the idempotent DDL cannot make these safe.
PRE_DDL_BLOCKS = ("incompatible_schema", "blocked_access")


def _publish_policy(conn, evidence: dict) -> tuple[str, bool]:
    """Insert/verify immutable policy evidence and point current at it.

    Never commits. Returns ``(policy_hash, changed)``; an identical policy that
    is already current changes nothing (no pointer UPDATE, so no new stamp).
    """
    policy_hash = policy_content_digest(evidence)
    pid, ver = evidence["policy_id"], evidence["policy_version"]
    changed = False
    changed |= conn.execute(
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
    ).rowcount > 0
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
            changed = True
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
            changed = True
    changed |= conn.execute(
        """UPDATE nav_policy_versions SET published_at=clock_timestamp()
           WHERE policy_id=%s AND policy_version=%s AND published_at IS NULL""",
        (pid, ver),
    ).rowcount > 0
    # The pointer trigger stamps published_at; an unchanged target is not updated.
    changed |= conn.execute(
        """INSERT INTO nav_policy_current (readiness_profile,policy_id,policy_version)
           VALUES ('current_daily_nav_v1',%s,%s)
           ON CONFLICT (readiness_profile) DO UPDATE SET
             policy_id=EXCLUDED.policy_id,policy_version=EXCLUDED.policy_version
           WHERE nav_policy_current.policy_id IS DISTINCT FROM EXCLUDED.policy_id
              OR nav_policy_current.policy_version IS DISTINCT FROM EXCLUDED.policy_version""",
        (pid, ver),
    ).rowcount > 0
    return policy_hash, changed


_SCOPE_ROWS_SQL = """
SELECT instrument_id::text, nav_date, nav::text, source_nav::text, source_nav_kind,
       currency, nav_repair_kind, calendar_id, calendar_version, calendar_source
FROM nav_timeseries
WHERE instrument_id = ANY(%s::uuid[]) AND nav_date BETWEEN %s AND %s
ORDER BY instrument_id, nav_date
"""
_POLICY_COLUMNS = (
    "policy_id",
    "policy_version",
    "policy_hash",
    "calendar_id",
    "calendar_version",
    "calendar_source",
)


def _relation_exists(conn, name: str) -> bool:
    return conn.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()[0]


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
    return dict(zip(_POLICY_COLUMNS, row))


def _proposed_policy(evidence: dict) -> dict:
    return {
        "policy_id": evidence["policy_id"],
        "policy_version": evidence["policy_version"],
        "policy_hash": policy_content_digest(evidence),
        "calendar_id": evidence["calendar_id"],
        "calendar_version": evidence["calendar_version"],
        "calendar_source": evidence["calendar_source"],
    }


def _scope_rows(conn, ids: list[str], start: dt.date, end: dt.date, *, lock: bool) -> list:
    """Scope rows; never row-locks nav_timeseries.

    TimescaleDB rejects row locks on compressed tuples (SQLSTATE 0A000 "locking
    compressed tuples is not supported"), and the production hypertable is
    compressed. Apply is serialized instead by the INGESTION -> READINESS
    transaction locks every governed NAV writer holds; ``lock`` is kept for the
    caller's intent and the policy row locks, not for nav_timeseries.
    """
    del lock
    return [list(row) for row in conn.execute(_SCOPE_ROWS_SQL, (ids, start, end)).fetchall()]


def _latest_lifecycle(rows: list[tuple], now: dt.datetime) -> dict[str, tuple]:
    """Latest (effective, known) lifecycle row known by ``now`` per instrument."""
    latest: dict[str, tuple] = {}
    for iid, known, effective, frequency, identity, basis, currency in rows:
        if known > now or effective > now:
            continue
        key = (effective, known)
        if iid not in latest or key > latest[iid][0]:
            latest[iid] = (key, (frequency, identity, basis, currency))
    return {iid: value for iid, (_key, value) in latest.items()}


def _candidates(
    conn, policy: dict, ids: list[str], rows: list, *, evidence: dict | None = None
) -> list:
    """Unstamped, economically stampable rows for ``policy`` (DB or proposed).

    A proposed policy is evaluated in memory from its file (sessions and
    lifecycle evidence) plus any DB lifecycle for the same version; nothing is
    published. The DB stamp trigger re-validates every row at write time.
    """
    now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
    if evidence is not None:
        sessions = {dt.date.fromisoformat(s["session_date"]) for s in evidence["sessions"]}
        lifecycle_rows = [
            (
                row["instrument_id"],
                dt.datetime.fromisoformat(row["known_at"]),
                dt.datetime.fromisoformat(row["effective_at"]),
                row["valuation_frequency"],
                bool(row["identity_verified"]),
                bool(row["return_basis_verified"]),
                bool(row["currency_verified"]),
            )
            for row in evidence["instrument_evidence"]
            if row["instrument_id"] in ids
        ]
    else:
        sessions = {
            row[0]
            for row in conn.execute(
                """SELECT session_date FROM nav_valuation_schedules
                   WHERE calendar_id=%s AND calendar_version=%s AND calendar_source=%s""",
                (policy["calendar_id"], policy["calendar_version"], policy["calendar_source"]),
            ).fetchall()
        }
        lifecycle_rows = []
    if _relation_exists(conn, "nav_instrument_policy_evidence"):
        lifecycle_rows += [
            tuple(row)
            for row in conn.execute(
                """SELECT instrument_id::text, known_at, effective_at, valuation_frequency,
                          identity_verified, return_basis_verified, currency_verified
                   FROM nav_instrument_policy_evidence
                   WHERE instrument_id = ANY(%s::uuid[]) AND policy_id=%s
                     AND policy_version=%s""",
                (ids, policy["policy_id"], policy["policy_version"]),
            ).fetchall()
        ]
    lifecycle = _latest_lifecycle(lifecycle_rows, now)
    eligible = []
    for iid, day, nav, source_nav, kind, currency, repair, cal_id, cal_ver, cal_src in rows:
        facts = lifecycle.get(iid)
        if (
            cal_id is None and cal_ver is None and cal_src is None
            and source_nav is not None and nav is not None
            and kind == "adjusted" and currency == "USD" and repair == "none"
            and Decimal(source_nav) == Decimal(nav)
            and day in sessions
            and facts is not None and facts[0] == "daily" and all(facts[1:])
        ):
            eligible.append((iid, day))
    return eligible


def _stamped(rows: list, candidates: list, policy: dict) -> list:
    wanted = set(candidates)
    tuple_ = [policy["calendar_id"], policy["calendar_version"], policy["calendar_source"]]
    return [row[:7] + tuple_ if (row[0], row[1]) in wanted else row for row in rows]


def _maintenance_state(
    conn, ids: list[str], start: dt.date, end: dt.date, *, evidence: dict | None,
    lock: bool = False,
) -> dict:
    policy = _proposed_policy(evidence) if evidence is not None else _pinned_policy(
        conn, lock=lock
    )
    rows = _scope_rows(conn, ids, start, end, lock=lock)
    candidates = _candidates(conn, policy, ids, rows, evidence=evidence)
    return {
        "policy": [policy[key] for key in _POLICY_COLUMNS],
        "scope_rows": len(rows),
        "eligible_rows": len(candidates),
        "before_digest": canonical_digest(rows),
        "candidate_digest": canonical_digest(candidates),
        "expected_after_digest": canonical_digest(_stamped(rows, candidates, policy)),
        "_rows": rows,
        "_candidates": candidates,
        "_policy": policy,
    }


def _public(state: dict | None) -> dict | None:
    return None if state is None else {k: v for k, v in state.items() if not k.startswith("_")}


def _maintenance_plan(conn, ids: list[str], start: dt.date, end: dt.date) -> dict:
    """Dry run against the current DB policy: read-only, no locks, no writes."""
    conn.execute("BEGIN TRANSACTION READ ONLY")
    try:
        conn.execute("SET LOCAL statement_timeout = '30s'")
        state = _maintenance_state(conn, ids, start, end, evidence=None)
        return {
            "status": "dry_run",
            "operation": "calendar_stamp",
            "policy": dict(zip(_POLICY_COLUMNS[:3], state["policy"][:3])),
            "calendar": state["policy"][3:],
            **{k: v for k, v in _public(state).items() if k != "policy"},
        }
    finally:
        conn.execute("ROLLBACK")


def _apply_calendar_maintenance_tx(
    conn, ids: list[str], start: dt.date, end: dt.date, plan_sha256: str,
    *, expected_candidate_digest: str | None = None,
) -> dict:
    """Maintenance body inside the caller's transaction: no BEGIN/COMMIT/locks.

    Pins the (possibly just published) current policy, stamps exactly the
    candidates, verifies revisions/digests and completes the run. Raises on any
    verification failure; the caller rolls back everything with it.
    """
    state = _maintenance_state(conn, ids, start, end, evidence=None, lock=True)
    policy, candidates = state["_policy"], state["_candidates"]
    if expected_candidate_digest is not None and (
        state["candidate_digest"] != expected_candidate_digest
    ):
        raise ValueError("PLAN_STALE")
    if not candidates:
        return {"status": "no_op", "eligible_rows": 0, "before_digest": state["before_digest"]}
    run_id = uuid.uuid4()
    conn.execute(
        """INSERT INTO nav_calendar_maintenance_runs
           (maintenance_run_id, operation, policy_id, policy_version, policy_hash,
            calendar_id, calendar_version, calendar_source, plan_sha256,
            instrument_ids, window_start, window_end, status, before_digest)
           VALUES (%s,'calendar_stamp',%s,%s,%s,%s,%s,%s,%s,%s::uuid[],%s,%s,
                   'running',%s)""",
        (run_id, *[policy[key] for key in _POLICY_COLUMNS], plan_sha256, ids, start, end,
         state["before_digest"]),
    )
    conn.execute("SELECT set_config('nav.ingestion_run_id', '', true)")
    conn.execute("SELECT set_config('nav.ingestion_provider', '', true)")
    conn.execute("SELECT set_config('nav.maintenance_run_id', %s, true)", (str(run_id),))
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
        or after_digest != canonical_digest(_stamped(state["_rows"], candidates, policy))
    ):
        raise ValueError("maintenance_verification_failed")
    conn.execute(
        """UPDATE nav_calendar_maintenance_runs
           SET status='completed', completed_at=clock_timestamp(),
               changed_rows=%s, after_digest=%s
           WHERE maintenance_run_id=%s AND status='running'""",
        (updated, after_digest, run_id),
    )
    return {
        "status": "completed",
        "maintenance_run_id": str(run_id),
        "changed_rows": updated,
        "before_digest": state["before_digest"],
        "after_digest": after_digest,
    }


def _try_nav_writer_locks(conn) -> bool:
    """INGESTION -> READINESS transaction locks, before any DML (N4 order)."""
    for key in (LOCK_INSTRUMENT_INGESTION, LOCK_FUND_NAV_READINESS):
        if not conn.execute("SELECT pg_try_advisory_xact_lock(%s)", (key,)).fetchone()[0]:
            return False
    return True


def _apply_calendar_maintenance(
    conn, ids: list[str], start: dt.date, end: dt.date, plan_sha256: str
) -> dict:
    """Maintenance-only transaction (autocommit caller): locks, body, commit."""
    conn.execute("BEGIN")
    try:
        conn.execute("SET LOCAL statement_timeout = '60s'")
        conn.execute("SET LOCAL lock_timeout = '2s'")
        if not _try_nav_writer_locks(conn):
            raise MaintenanceBusy("nav_writer_lock_busy")
        result = _apply_calendar_maintenance_tx(conn, ids, start, end, plan_sha256)
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT" if result["status"] == "completed" else "ROLLBACK")
    return result


# ──────────────────────────────────────────────────────────────────────────────
# N4: plan v3 and the single DML transaction
# ──────────────────────────────────────────────────────────────────────────────
def _pointer_state(conn) -> list | None:
    if not _relation_exists(conn, "nav_policy_current"):
        return None
    row = conn.execute(
        """SELECT c.policy_id, c.policy_version, p.policy_hash
           FROM nav_policy_current c JOIN nav_policy_versions p USING (policy_id, policy_version)
           WHERE c.readiness_profile = 'current_daily_nav_v1'"""
    ).fetchone()
    return list(row) if row else None


def _version_hash(conn, evidence: dict | None) -> str | None:
    if evidence is None or not _relation_exists(conn, "nav_policy_versions"):
        return None
    row = conn.execute(
        "SELECT policy_hash FROM nav_policy_versions WHERE policy_id=%s AND policy_version=%s",
        (evidence["policy_id"], evidence["policy_version"]),
    ).fetchone()
    return row[0] if row else None


def _dml_state(conn, evidence: dict | None, ids: list[str], start, end) -> dict:
    """Before-state pinned by the plan; recomputed under the locks at apply."""
    return {
        "policy_pointer": _pointer_state(conn),
        "policy_version_hash": _version_hash(conn, evidence),
        "maintenance": _public(
            _maintenance_state(conn, ids, start, end, evidence=evidence) if ids else None
        ),
    }


def build_plan(
    *, ddl: bytes, manifest: dict, schema: str, policy_raw: bytes, evidence: dict | None,
    ids: list[str], start: str | None, end: str | None, dml_state: dict,
    audit: dict | None = None,
) -> tuple[dict, str]:
    """Normalized nav-schema-plan-v4 and its SHA256 (canonical JSON).

    ``policy_hash`` identifies the immutable policy version (it excludes
    instrument evidence and generation by design); ``policy_document_digest``
    is the canonical digest of the WHOLE document, so a new lifecycle row for
    the same version is a different operation. ``audit`` is the governed
    receipt (hashes/aggregates only) that a policy publication requires;
    ``operation`` is the ordered identity a replay must match exactly (it
    includes the audit receipt); ``before`` pins the pre-state.
    """
    if (evidence is None) != (audit is None):
        raise ValueError("audit_receipt_required")
    operations = sorted(
        ["ddl", *(["policy"] if evidence else []), *(["maintenance"] if ids else [])]
    )
    plan = {
        "plan_version": PLAN_VERSION,
        "sql_sha256": hashlib.sha256(ddl).hexdigest(),
        "catalog_sha256": manifest["signature_sha256"],
        "access_profile": ACCESS_PROFILE,
        "access_sha256": manifest["access_profile"]["signature_sha256"],
        "schema": schema,
        "operations": operations,
        "policy_sha256": hashlib.sha256(policy_raw).hexdigest() if policy_raw else None,
        "policy_hash": policy_content_digest(evidence) if evidence else None,
        "policy_document_digest": canonical_digest(evidence) if evidence else None,
        "audit": audit,
        "operation": {
            "policy": (
                [evidence["policy_id"], evidence["policy_version"],
                 policy_content_digest(evidence), canonical_digest(evidence)]
                if evidence else None
            ),
            "audit": (
                None if audit is None
                else [audit["audit_dossier_sha256"], audit["canary_manifest_sha256"],
                      audit["capture_bundle_sha256"]]
            ),
            "maintenance": (
                {"instrument_ids": sorted(ids), "window": [start, end]} if ids else None
            ),
        },
        "instrument_ids": sorted(ids),
        "start": start,
        "end": end,
        "before": dml_state,
    }
    return plan, canonical_digest(plan)


def previous_version_digest(plan: dict) -> str:
    """Digest the same inputs would have had under nav-schema-plan-v3."""
    legacy = {k: v for k, v in plan.items() if k not in _V4_ONLY_FIELDS}
    legacy["operation"] = {
        k: v for k, v in plan["operation"].items() if k != "audit"
    }
    legacy["plan_version"] = PREVIOUS_PLAN_VERSION
    return canonical_digest(legacy)


def v2_version_digest(plan: dict) -> str:
    """Digest the same inputs would have had under nav-schema-plan-v2."""
    legacy = {
        k: v for k, v in plan.items() if k not in (*_V3_ONLY_FIELDS, *_V4_ONLY_FIELDS)
    }
    legacy["plan_version"] = V2_PLAN_VERSION
    return canonical_digest(legacy)


def plan_hash(
    ddl: bytes,
    schema: str,
    policy: bytes,
    instrument_ids: list[str],
    start: str | None,
    end: str | None,
) -> str:
    """Legacy nav-schema-plan-v1 digest: recognised only to reject it."""
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


_POLICY_RELATIONS = (
    "nav_policy_versions",
    "nav_policy_current",
    "nav_valuation_schedules",
    "nav_instrument_policy_evidence",
)


def _policy_facts_exact(conn, evidence: dict) -> bool:
    """Read-only: every fact the document publishes is persisted exactly.

    The immutable version row (content hash, published), the current pointer
    at it, every valuation session tuple with the persisted calendar count and
    digest, and EVERY instrument lifecycle row with identical content. A new
    lifecycle row for the same policy version is therefore never a replay.
    """
    if not all(_relation_exists(conn, name) for name in _POLICY_RELATIONS):
        return False
    policy_hash = policy_content_digest(evidence)
    pid, ver = evidence["policy_id"], evidence["policy_version"]
    version = conn.execute(
        """SELECT policy_hash, published_at IS NOT NULL FROM nav_policy_versions
           WHERE policy_id=%s AND policy_version=%s""",
        (pid, ver),
    ).fetchone()
    if version is None or version[0] != policy_hash or not version[1]:
        return False
    if _pointer_state(conn) != [pid, ver, policy_hash]:
        return False
    persisted = conn.execute(
        """SELECT session_date, valuation_close_at, nav_due_at, calendar_source,
                  source_reference
           FROM nav_valuation_schedules WHERE calendar_id=%s AND calendar_version=%s
           ORDER BY session_date""",
        (evidence["calendar_id"], evidence["calendar_version"]),
    ).fetchall()
    expected = sorted(
        (
            dt.date.fromisoformat(s["session_date"]),
            dt.datetime.fromisoformat(s["valuation_close_at"]),
            dt.datetime.fromisoformat(s["nav_due_at"]),
            evidence["calendar_source"],
            s.get("source_reference") or evidence["source_reference"],
        )
        for s in evidence["sessions"]
    )
    if [tuple(row) for row in persisted] != expected:
        return False
    if (
        len(persisted) != evidence["calendar_session_count"]
        or calendar_digest([(r[0], r[1], r[2], r[4]) for r in persisted])
        != evidence["calendar_digest"]
    ):
        return False
    for row in evidence["instrument_evidence"]:
        saved = conn.execute(
            """SELECT fund_status, valuation_frequency, identity_verified,
                      return_basis_verified, currency_verified, evidence_reference
               FROM nav_instrument_policy_evidence
               WHERE instrument_id=%s AND policy_id=%s AND policy_version=%s
                 AND known_at=%s AND effective_at=%s""",
            (row["instrument_id"], pid, ver, row["known_at"], row["effective_at"]),
        ).fetchone()
        if saved != (
            row["fund_status"],
            row["valuation_frequency"],
            bool(row["identity_verified"]),
            bool(row["return_basis_verified"]),
            bool(row["currency_verified"]),
            row["evidence_reference"],
        ):
            return False
    return True


def _maintenance_receipt_exact(
    conn, plan_sha256: str, ids: list[str], start: dt.date, end: dt.date,
    policy: dict,
) -> bool:
    """Read-only: a completed receipt of THIS maintenance operation whose
    post-state still holds.

    Same plan hash, operation, sorted instrument scope, window and policy pins;
    and the current scope rows still digest to the receipt's ``after_digest``
    (a later insert/stamp/edit in scope makes it a different state).
    """
    if not _relation_exists(conn, "nav_calendar_maintenance_runs"):
        return False
    receipts = conn.execute(
        """SELECT instrument_ids::text[], window_start, window_end, policy_id,
                  policy_version, policy_hash, calendar_id, calendar_version,
                  calendar_source, after_digest
           FROM nav_calendar_maintenance_runs
           WHERE plan_sha256 = %s AND status = 'completed'
             AND operation = 'calendar_stamp'""",
        (plan_sha256,),
    ).fetchall()
    pins = [policy[key] for key in _POLICY_COLUMNS]
    current = canonical_digest(_scope_rows(conn, ids, start, end, lock=False))
    return any(
        sorted(r[0]) == sorted(ids)
        and (r[1], r[2]) == (start, end)
        and list(r[3:9]) == pins
        and r[9] == current
        for r in receipts
    )


def _already_applied(
    conn, plan_sha256: str, evidence: dict | None, ids: list[str],
    start: dt.date | None = None, end: dt.date | None = None,
    *, audit: dict | None = None, policy_sha256: str | None = None,
) -> bool:
    """A stale plan is a replay only when this exact operation is persisted.

    Policy: every document fact persisted exactly (``_policy_facts_exact``)
    AND a publication receipt of this very plan digest, policy bytes/document
    and normalized audit receipt (``_publication_receipt_exact``): another
    valid audit of the same policy bytes never replays the old plan.
    Maintenance: an exact receipt of this plan hash, scope, window and pins
    whose post-state still holds (``_maintenance_receipt_exact``). A current
    pointer alone, or any receipt carrying the hash, never suffices.
    """
    if evidence is None and not ids:
        return False
    if evidence is not None and not (
        _policy_facts_exact(conn, evidence)
        and _publication_receipt_exact(
            conn, evidence, audit, policy_sha256, plan_sha256=plan_sha256
        )
    ):
        return False
    if ids:
        try:
            policy = (
                _proposed_policy(evidence) if evidence is not None
                else _pinned_policy(conn, lock=False)
            )
        except ValueError:
            return False
        return _maintenance_receipt_exact(conn, plan_sha256, ids, start, end, policy)
    return True


# ── governed publication receipt (plan-v4) ───────────────────────────────────
RECEIPT_FILE_ARGS = (
    ("policy_file", "policy_sha256"),
    ("audit_dossier_file", "audit_dossier_sha256"),
    ("canary_manifest_file", "canary_manifest_sha256"),
    ("capture_file", "capture_sha256"),
)


def _platform_name() -> str:
    return os.name


def _inside_git_checkout(path: Path) -> bool:
    for ancestor in (path, *path.parents):
        try:
            (ancestor / ".git").lstat()  # presence only; contents never read
        except FileNotFoundError:
            continue
        return True
    return False


def _custody_root(value: str | None) -> Path:
    """Private POSIX custody root: absolute, no symlink, owner-only, no Git."""
    if _platform_name() != "posix":
        raise ValueError("custody_requires_posix")
    if not value:
        raise ValueError("audit_receipt_required")
    raw = Path(value).expanduser().absolute()
    try:
        root = raw.resolve(strict=True)
        info = os.stat(root, follow_symlinks=False)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise ValueError("custody_root_missing") from exc
    if raw != root or not stat.S_ISDIR(info.st_mode):
        raise ValueError("custody_root_invalid")
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError("custody_root_not_private")
    if _inside_git_checkout(root):
        raise ValueError("custody_inside_git_checkout")
    return root


def _read_custody(root: Path, value: str) -> bytes:
    """Read one custody file ONCE through no-follow fds; validate what was read.

    The path must resolve inside the root without any symlink component; the
    final file must be a regular owner-only (0600-compatible) single-link file;
    size, inode and mtime are checked before and after the read so a
    concurrent rewrite or replacement blocks instead of being half-read. The
    final component is opened NON-BLOCKING and classified by ``fstat`` before
    any read: a FIFO (or device/socket) is rejected at once instead of
    blocking on a missing writer.
    """
    raw = Path(value).expanduser().absolute()
    if raw.resolve(strict=False) != raw:
        raise ValueError("custody_symlink_invalid")
    try:
        relative = raw.relative_to(root)
    except ValueError as exc:
        raise ValueError("custody_path_outside_root") from exc
    if not relative.parts or ".." in relative.parts:
        raise ValueError("custody_path_outside_root")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(root, directory_flags)
    try:
        for component in relative.parts[:-1]:
            try:
                following = os.open(component, directory_flags, dir_fd=fd)
            except OSError as exc:
                raise ValueError("custody_symlink_invalid") from exc
            os.close(fd)
            fd = following
        try:
            file_fd = os.open(
                relative.parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=fd,
            )
        except FileNotFoundError as exc:
            raise ValueError("custody_file_missing") from exc
        except OSError as exc:
            raise ValueError("custody_symlink_invalid") from exc
    finally:
        os.close(fd)
    try:
        before = os.fstat(file_fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("custody_file_not_regular")
        # Regular file established: restore blocking reads (no effect on a
        # regular file's data, but keeps the read loop's semantics explicit).
        os.set_blocking(file_fd, True)
        if before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) & 0o077:
            raise ValueError("custody_file_not_private")
        if before.st_nlink != 1:
            raise ValueError("custody_file_not_regular")
        if before.st_size > MAX_CUSTODY_BYTES:
            raise ValueError("custody_file_too_large")
        chunks, total = [], 0
        while True:
            chunk = os.read(file_fd, 1 << 20)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_CUSTODY_BYTES:
                raise ValueError("custody_file_too_large")
            chunks.append(chunk)
        after = os.fstat(file_fd)
    finally:
        os.close(file_fd)
    content = b"".join(chunks)
    if (
        (before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_ino, after.st_size, after.st_mtime_ns)
        or len(content) != before.st_size
    ):
        raise ValueError("custody_file_changed")
    try:
        current = os.stat(raw, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ValueError("custody_file_changed") from exc
    if (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
        raise ValueError("custody_file_changed")
    return content


def _hash_arg(value: str | None) -> str:
    text = (value or "").lower()
    if not HEX64.fullmatch(text):
        raise ValueError("audit_receipt_hash_invalid")
    return text


def _aware(value) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


SEC_MAX_AGE = dt.timedelta(days=SEC_MAX_SYNCED_AGE_DAYS)


def _hex64(value) -> bool:
    return isinstance(value, str) and HEX64.fullmatch(value) is not None


def _receipt_window(times: dict) -> dict[str, dt.datetime]:
    """Aware, ordered, internally consistent audit instants (static codes).

    ``captured_at == decision_at`` (the capture's DB clock is the A8 decision
    instant); ``min_matched_synced_at <= decision_at`` within the maximum age;
    ``sec_valid_until`` is exactly ``min_matched_synced_at + max age``.
    """
    parsed = {key: _aware(times.get(key)) for key in AUDIT_RECEIPT_TIME_KEYS}
    if any(value is None for value in parsed.values()):
        raise ValueError("audit_receipt_time_invalid")
    captured, decided = parsed["captured_at"], parsed["decision_at"]
    minimum, until = parsed["min_matched_synced_at"], parsed["sec_valid_until"]
    if captured != decided or minimum > decided or decided - minimum > SEC_MAX_AGE:
        raise ValueError("audit_receipt_time_invalid")
    if until != minimum + SEC_MAX_AGE:
        raise ValueError("audit_sec_deadline_inconsistent")
    return parsed


def _uuid_list(value) -> list[str]:
    if (
        not isinstance(value, list)
        or any(not isinstance(item, str) or not LOWER_UUID.fullmatch(item) for item in value)
        or value != sorted(set(value))
    ):
        raise ValueError("canary_manifest_invalid")
    return value


def _validate_dossier(dossier: dict, policy: dict, policy_sha: str) -> None:
    """Strict all-PASS dossier of exactly this policy, contract and code."""
    generation = policy["generation"]
    gates = dossier.get("gates")
    inputs = dossier.get("inputs")
    details = dossier.get("details")
    if (
        set(dossier) != set(DOSSIER_KEYS)
        or dossier["audit_version"] != AUDIT_VERSION
        or dossier["result"] != "pass"
        or dossier["strict"] is not True
        or dossier["canary_requested"] is not True
        or not isinstance(gates, dict)
        or set(gates) != set(GATE_NAMES)
        or not isinstance(inputs, dict)
        or set(inputs) != set(DOSSIER_INPUT_KEYS)
        or not isinstance(details, dict)
        or set(details) != set(DOSSIER_DETAIL_KEYS)
    ):
        raise ValueError("audit_dossier_invalid")
    for name in GATE_NAMES:
        gate = gates[name]
        if (
            not isinstance(gate, dict)
            or set(gate) != {"status", "code", "checks"}
            or gate["status"] != "PASS"
            or gate["code"] is not None
            or not isinstance(gate["checks"], dict)
            or set(gate["checks"]) != set(GATE_CHECKS[name])
            or any(value is not True for value in gate["checks"].values())
        ):
            raise ValueError("audit_not_strict_pass")
    if (
        inputs["audit_contract_version"] != AUDIT_CONTRACT_VERSION
        or inputs["audit_contract_sha256"] != AUDIT_CONTRACT_SHA256
        or inputs["source_query_sha256"] != CATALOG_SOURCE_QUERY_SHA256
        or inputs["sec_source_contract"] != SEC_SOURCE_CONTRACT
        or inputs["sec_query_contract_sha256"] != SEC_QUERY_CONTRACT_SHA256
    ):
        raise ValueError("audit_contract_mismatch")
    if (
        inputs["policy_artifact_sha256"] != policy_sha
        or inputs["policy_id"] != policy["policy_id"]
        or inputs["policy_version"] != policy["policy_version"]
        or inputs["policy_hash"] != generation["policy_hash"]
        or inputs["policy_generation_sha256"] != generation["generation_sha256"]
        or inputs["source_snapshot_sha256"] != generation["source_snapshot_sha256"]
        or inputs["live_source_snapshot_sha256"] != generation["source_snapshot_sha256"]
    ):
        raise ValueError("audit_policy_mismatch")
    if inputs["verifier_source_sha256"] != _sha256(AUDIT_VERIFIER.read_bytes()):
        raise ValueError("audit_verifier_mismatch")
    if inputs["audit_config_sha256"] != _sha256(AUDIT_CONFIG.read_bytes()):
        raise ValueError("audit_config_mismatch")
    identity = inputs["previous_policy_identity"]
    if (
        not isinstance(identity, list)
        or len(identity) != 3
        or not all(isinstance(part, str) and part for part in identity)
        or not HEX64.fullmatch(identity[2])
        or not isinstance(inputs["previous_policy_sha256"], str)
        or not HEX64.fullmatch(inputs["previous_policy_sha256"])
        or not isinstance(inputs["source_snapshot_file_sha256"], str)
        or not HEX64.fullmatch(inputs["source_snapshot_file_sha256"])
    ):
        raise ValueError("audit_previous_policy_invalid")
    # Nested objects are typed before any lookup (static codes, no
    # AttributeError): the recorded capture, its cohort/SEC parts and A7.
    capture = inputs["capture"]
    recorded_cohort = capture.get("cohort") if isinstance(capture, dict) else None
    recorded_sec = capture.get("sec") if isinstance(capture, dict) else None
    a4, a7, a8 = details["A4"], details["A7"], details["A8"]
    if (
        not isinstance(capture, dict)
        or not _hex64(capture.get("capture_bundle_sha256"))
        or _aware(capture.get("captured_at")) is None
        or not _hex64(capture.get("source_snapshot_sha256"))
        or not isinstance(recorded_cohort, dict)
        or not _hex64(recorded_cohort.get("rows_sha256"))
        or not isinstance(recorded_sec, dict)
        or not _hex64(recorded_sec.get("rows_sha256"))
        or not isinstance(a7, dict)
        or not _hex64(a7.get("eligible_cohort_sha256"))
        or type(a7.get("eligible_cohort_size")) is not int
        or not isinstance(a7.get("light_revision"), str)
        or not _hex64(a7.get("cohort_query_sha256"))
        or not _hex64(a7.get("cohort_rows_sha256"))
    ):
        raise ValueError("audit_dossier_invalid")
    if (
        not isinstance(a4, dict)
        or set(a4) != set(A4_DETAIL_KEYS)
        or a4["ceiling_population"] != "structural_pre_claims_daily"
        or not isinstance(a8, dict)
        or set(a8) != set(A8_DETAIL_KEYS)
        or a8["source_contract"] != SEC_SOURCE_CONTRACT
        or a8["relation"] != SEC_RELATION
        or a8["timestamp_column"] != SEC_TIMESTAMP_COLUMN
        or a8["sec_query_contract_sha256"] != SEC_QUERY_CONTRACT_SHA256
        or not isinstance(a8["freshness"], dict)
        or set(a8["freshness"]) != set(A8_FRESHNESS_KEYS)
        or a8["freshness"]["max_synced_age_days"] != SEC_MAX_SYNCED_AGE_DAYS
        or _aware(a8["freshness"]["valid_until"]) is None
        or not isinstance(a8["outcomes"], dict)
        or set(a8["outcomes"]) != set(A8_OUTCOMES)
        or a8["outcomes"]["matched"] != sum(
            1 for row in policy["instrument_evidence"] if row["fund_status"] == "ACTIVE"
        )
        or any(a8["outcomes"][name] != 0 for name in A8_OUTCOMES if name != "matched")
        or not _hex64(details["canary_selection_sha256"])
    ):
        raise ValueError("audit_dossier_invalid")
    # F1 time window, retained and re-derived from the dossier itself: the
    # capture instant IS the A8 decision instant, the SEC deadline is exactly
    # the oldest matched updated_at + max age, every matched/lineage instant
    # lies in [min matched, decision] and generation precedes the capture.
    freshness = a8["freshness"]
    window = _receipt_window(
        {
            "captured_at": capture["captured_at"],
            "decision_at": freshness["decision_at"],
            "min_matched_synced_at": freshness["min_matched_synced_at"],
            "sec_valid_until": freshness["valid_until"],
        }
    )
    max_matched = _aware(freshness["max_matched_synced_at"])
    lineage_max = _aware(freshness["lineage_max_synced_at"])
    generated = _aware(generation.get("generated_at"))
    if (
        max_matched is None
        or lineage_max is None
        or generated is None
        or not (
            window["min_matched_synced_at"]
            <= max_matched
            <= lineage_max
            <= window["decision_at"]
        )
        or generated > window["captured_at"]
    ):
        raise ValueError("audit_receipt_time_invalid")


def _validate_capture(capture: dict, capture_raw: bytes, dossier: dict, policy: dict) -> set:
    """Capture of the same snapshot; cohort/SEC row digests recomputed here."""
    recorded = dossier["inputs"]["capture"]
    if (
        capture_raw != _document_bytes(capture)
        or capture.get("kind") != CAPTURE_KIND
        or capture.get("audit_contract_version") != AUDIT_CONTRACT_VERSION
        or capture.get("source_query_sha256") != CATALOG_SOURCE_QUERY_SHA256
        or capture.get("source_snapshot_sha256")
        != policy["generation"]["source_snapshot_sha256"]
        or not isinstance(recorded, dict)
        or recorded.get("capture_bundle_sha256") != _sha256(capture_raw)
        or recorded.get("captured_at") != capture.get("captured_at")
        or recorded.get("source_snapshot_sha256") != capture.get("source_snapshot_sha256")
    ):
        raise ValueError("audit_capture_mismatch")
    cohort, sec = capture.get("cohort"), capture.get("sec")
    if not isinstance(cohort, dict) or not isinstance(sec, dict):
        raise ValueError("audit_capture_mismatch")
    for part, name in ((cohort, "cohort"), (sec, "sec")):
        rows = part.get("rows")
        if (
            part.get("state") != "captured"
            or not isinstance(rows, list)
            or _sha256(_document_bytes(rows)) != part.get("rows_sha256")
            or recorded[name].get("rows_sha256") != part.get("rows_sha256")
            or part.get("row_count") != len(rows)
        ):
            raise ValueError("audit_capture_mismatch")
    if (
        sec.get("source_contract") != SEC_SOURCE_CONTRACT
        or sec.get("relation") != SEC_RELATION
        or sec.get("timestamp_column") != SEC_TIMESTAMP_COLUMN
        or sec.get("query_contract_sha256") != SEC_QUERY_CONTRACT_SHA256
        or dossier["details"]["A8"]["sec_rows_sha256"] != sec.get("rows_sha256")
        or dossier["details"]["A7"].get("cohort_rows_sha256") != cohort.get("rows_sha256")
    ):
        raise ValueError("audit_capture_mismatch")
    members = set()
    for row in cohort["rows"]:
        if not isinstance(row, dict) or not isinstance(row.get("instrument_id"), str):
            raise ValueError("audit_capture_mismatch")
        members.add(row["instrument_id"])
    return members


def _selection_digest(manifest: dict) -> str:
    return _sha256(_document_bytes({key: manifest[key] for key in CANARY_SELECTION_KEYS}))


def _validate_manifest(
    manifest: dict, dossier: dict, dossier_sha: str, capture_sha: str,
    policy: dict, policy_sha: str, cohort_members: set,
) -> None:
    inputs, a7 = dossier["inputs"], dossier["details"]["A7"]
    config = _strict_object(AUDIT_CONFIG.read_bytes(), "audit_config_mismatch")
    allowlist = _uuid_list(manifest.get("allowlist")) if isinstance(manifest, dict) else None
    active_daily = {
        row["instrument_id"]
        for row in policy["instrument_evidence"]
        if row["fund_status"] == "ACTIVE" and row["valuation_frequency"] == "daily"
    }
    if (
        set(manifest) != set(CANARY_MANIFEST_KEYS)
        or manifest["kind"] != CANARY_KIND
        or manifest["audit_contract_version"] != AUDIT_CONTRACT_VERSION
        or manifest["audit_contract_sha256"] != AUDIT_CONTRACT_SHA256
        or manifest["audit_report_sha256"] != dossier_sha
        or manifest["capture_bundle_sha256"] != capture_sha
        or manifest["audit_config_sha256"] != inputs["audit_config_sha256"]
        or manifest["verifier_source_sha256"] != inputs["verifier_source_sha256"]
        or manifest["sec_source_contract"] != SEC_SOURCE_CONTRACT
        or manifest["sec_query_contract_sha256"] != SEC_QUERY_CONTRACT_SHA256
        or manifest["policy_id"] != policy["policy_id"]
        or manifest["policy_version"] != policy["policy_version"]
        or manifest["policy_hash"] != policy["generation"]["policy_hash"]
        or manifest["policy_artifact_sha256"] != policy_sha
        or manifest["active_daily_set_sha256"]
        != policy["generation"]["active_daily_set_sha256"]
        or manifest["eligible_cohort_sha256"] != a7.get("eligible_cohort_sha256")
        or manifest["eligible_cohort_size"] != a7.get("eligible_cohort_size")
        or manifest["light_revision"] != a7.get("light_revision")
        or manifest["cohort_query_sha256"] != a7.get("cohort_query_sha256")
        or manifest["max_size"] != CANARY_MAX
        or manifest["salt"] != config.get("canary_salt")
        or type(manifest["size"]) is not int
        or not 1 <= manifest["size"] <= CANARY_MAX
        or manifest["size"] != len(allowlist)
        or type(manifest["cohort_size"]) is not int
        or manifest["cohort_size"] != manifest["eligible_cohort_size"]
        or not isinstance(manifest["strata"], dict)
        or set(manifest["strata"]) != set(CANARY_STRATA)
        or manifest["selection_sha256"] != _selection_digest(manifest)
        or manifest["selection_sha256"] != dossier["details"]["canary_selection_sha256"]
        or not set(allowlist) <= active_daily
        or not set(allowlist) <= cohort_members
    ):
        raise ValueError("canary_manifest_invalid")


def _governed_policy(args) -> tuple[dict | None, bytes, dict | None]:
    """The ONLY CLI policy path: policy + strict dossier + canary + capture.

    Returns ``(evidence, raw, audit_receipt)``. No flag, environment variable
    or fallback publishes a policy without the receipt.
    """
    receipt_args = [
        getattr(args, name) for pair in RECEIPT_FILE_ARGS for name in pair
    ] + [args.custody_root]
    if args.policy_file is None:
        if any(value is not None for value in receipt_args):
            raise ValueError("audit_receipt_without_policy")
        return None, b"", None
    if any(value is None for value in receipt_args):
        raise ValueError("audit_receipt_required")
    root = _custody_root(args.custody_root)
    paths = [Path(getattr(args, file_arg)).expanduser().absolute()
             for file_arg, _hash in RECEIPT_FILE_ARGS]
    if len(set(paths)) != len(paths):
        raise ValueError("audit_receipt_paths_collide")
    content = {}
    for file_arg, hash_arg in RECEIPT_FILE_ARGS:
        expected = _hash_arg(getattr(args, hash_arg))
        data = _read_custody(root, getattr(args, file_arg))
        if not hmac.compare_digest(_sha256(data), expected):
            raise ValueError(f"{hash_arg}_mismatch")
        content[file_arg] = data
    raw = content["policy_file"]
    policy = _strict_object(raw, "policy_not_object")
    if raw != _document_bytes(policy):
        raise ValueError("policy_not_canonical")
    if not isinstance(policy.get("generation"), dict):
        raise ValueError("policy_not_generated")
    evidence, _ = _policy(raw)
    policy_sha = _sha256(raw)
    dossier_raw = content["audit_dossier_file"]
    dossier = _strict_object(dossier_raw, "audit_dossier_invalid")
    if dossier_raw != _document_bytes(dossier):
        raise ValueError("audit_dossier_invalid")
    _validate_dossier(dossier, evidence, policy_sha)
    capture_raw = content["capture_file"]
    capture = _strict_object(capture_raw, "audit_capture_mismatch")
    members = _validate_capture(capture, capture_raw, dossier, evidence)
    manifest_raw = content["canary_manifest_file"]
    manifest = _strict_object(manifest_raw, "canary_manifest_invalid")
    if manifest_raw != _document_bytes(manifest):
        raise ValueError("canary_manifest_invalid")
    dossier_sha, capture_sha = _sha256(dossier_raw), _sha256(capture_raw)
    _validate_manifest(
        manifest, dossier, dossier_sha, capture_sha, evidence, policy_sha, members
    )
    inputs = dossier["inputs"]
    freshness = dossier["details"]["A8"]["freshness"]
    receipt = {
        "audit_contract_version": AUDIT_CONTRACT_VERSION,
        "audit_contract_sha256": AUDIT_CONTRACT_SHA256,
        "audit_config_sha256": inputs["audit_config_sha256"],
        "verifier_source_sha256": inputs["verifier_source_sha256"],
        "audit_dossier_sha256": dossier_sha,
        "canary_manifest_sha256": _sha256(manifest_raw),
        "canary_selection_sha256": manifest["selection_sha256"],
        "capture_bundle_sha256": capture_sha,
        "source_snapshot_sha256": inputs["source_snapshot_sha256"],
        "source_snapshot_file_sha256": inputs["source_snapshot_file_sha256"],
        "sec_source_contract": SEC_SOURCE_CONTRACT,
        "sec_query_contract_sha256": SEC_QUERY_CONTRACT_SHA256,
        "sec_rows_sha256": capture["sec"]["rows_sha256"],
        "captured_at": inputs["capture"]["captured_at"],
        "decision_at": freshness["decision_at"],
        "min_matched_synced_at": freshness["min_matched_synced_at"],
        "sec_valid_until": freshness["valid_until"],
        "previous_policy_identity": list(inputs["previous_policy_identity"]),
        "previous_policy_sha256": inputs["previous_policy_sha256"],
    }
    if set(receipt) != set(AUDIT_RECEIPT_KEYS):  # pragma: no cover - contract guard
        raise ValueError("audit_receipt_invalid")
    return evidence, raw, receipt


def _pointer_identity(conn) -> list | None:
    row = conn.execute(
        """SELECT c.policy_id, c.policy_version, v.policy_hash
             FROM nav_policy_current c
             JOIN nav_policy_versions v USING (policy_id, policy_version)
            WHERE c.readiness_profile = 'current_daily_nav_v1'"""
    ).fetchone()
    return None if row is None else [row[0], row[1], row[2]]


def _audit_window(conn, evidence: dict, audit: dict) -> None:
    """The receipt's time window holds NOW, by the database clock.

    ``captured_at <= now <= policy.valid_through`` and ``now <=
    sec_valid_until``, after re-validating the receipt's own consistency
    (``_receipt_window``). Run at check, again under the NAV writer locks at
    apply, and before any replay decision.
    """
    window = _receipt_window(audit)
    valid_through = _aware(evidence.get("valid_through"))
    if valid_through is None:
        raise ValueError("calendar_expiration_unverified")
    now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
    if now < window["captured_at"]:
        raise ValueError("audit_capture_in_future")
    if now > valid_through:
        raise ValueError("policy_valid_through_expired")
    if now > window["sec_valid_until"]:
        raise ValueError("audit_sec_freshness_expired")


def _receipt_identity(evidence: dict, audit: dict, policy_sha256: str | None) -> tuple:
    return (
        evidence["policy_id"],
        evidence["policy_version"],
        policy_content_digest(evidence),
        policy_sha256,
        canonical_digest(evidence),
        canonical_digest(audit),
        audit["audit_dossier_sha256"],
        audit["canary_manifest_sha256"],
        audit["capture_bundle_sha256"],
    )


def _publication_receipt_exact(
    conn, evidence: dict, audit: dict | None, policy_sha256: str | None,
    *, plan_sha256: str | None = None,
) -> bool:
    """Read-only: a persisted publication receipt of THIS governed operation.

    Same policy identity/hash, policy bytes, whole document, normalized audit
    receipt and dossier/manifest/capture hashes; with ``plan_sha256`` also the
    same original plan-v4 digest. Absent relation or receipt: not proven.
    """
    if audit is None or not policy_sha256 or not _relation_exists(
        conn, PUBLICATION_RECEIPT_RELATION
    ):
        return False
    rows = conn.execute(
        """SELECT plan_sha256 FROM nav_policy_publication_receipts
           WHERE readiness_profile = 'current_daily_nav_v1'
             AND policy_id = %s AND policy_version = %s AND policy_hash = %s
             AND policy_artifact_sha256 = %s AND policy_document_digest = %s
             AND audit_receipt_sha256 = %s AND audit_dossier_sha256 = %s
             AND canary_manifest_sha256 = %s AND capture_bundle_sha256 = %s""",
        _receipt_identity(evidence, audit, policy_sha256),
    ).fetchall()
    return any(plan_sha256 is None or row[0] == plan_sha256 for row in rows)


def _audit_pointer(conn, evidence: dict, audit: dict, policy_sha256: str | None) -> bool:
    """The pointer is the audited previous version, or the target as a replay.

    A pointer already at the target is accepted ONLY as an exact replay: every
    document fact persisted exactly AND a publication receipt of this exact
    governed operation. Anything else at the target (new evidence, another
    generation or audit of the same version) is ``target_policy_not_exact_replay``.
    Returns True for an exact replay: no write may follow.
    """
    pointer = _pointer_identity(conn)
    target = [evidence["policy_id"], evidence["policy_version"],
              policy_content_digest(evidence)]
    if pointer == target:
        if _policy_facts_exact(conn, evidence) and _publication_receipt_exact(
            conn, evidence, audit, policy_sha256
        ):
            return True
        raise ValueError("target_policy_not_exact_replay")
    if pointer != audit["previous_policy_identity"]:
        raise ValueError("current_pointer_not_previous")
    return False


def _record_publication(
    conn, evidence: dict, audit: dict, policy_sha256: str, plan_sha256: str
) -> None:
    """Append the publication receipt in the publication transaction."""
    previous = audit["previous_policy_identity"] or [None, None, None]
    identity = _receipt_identity(evidence, audit, policy_sha256)
    conn.execute(
        """INSERT INTO nav_policy_publication_receipts
           (readiness_profile, policy_id, policy_version, policy_hash, plan_version,
            plan_sha256, policy_artifact_sha256, policy_document_digest,
            audit_receipt_sha256, audit_dossier_sha256, canary_manifest_sha256,
            capture_bundle_sha256, previous_policy_id, previous_policy_version,
            previous_policy_hash, captured_at, sec_valid_until)
           VALUES ('current_daily_nav_v1', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, %s, %s)""",
        (
            *identity[:3],
            PLAN_VERSION,
            plan_sha256,
            *identity[3:],
            *previous,
            audit["captured_at"],
            audit["sec_valid_until"],
        ),
    )


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, default=str))


_SAFE_CODES = re.compile(r"[a-z0-9_]{1,64}|[A-Z_]{1,48}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("check", "apply"), default="check")
    parser.add_argument("--schema", required=True)
    parser.add_argument("--expected-sql-sha256", required=True)
    parser.add_argument("--plan-sha256")
    parser.add_argument("--policy-file")
    parser.add_argument("--policy-sha256")
    parser.add_argument("--audit-dossier-file")
    parser.add_argument("--audit-dossier-sha256")
    parser.add_argument("--canary-manifest-file")
    parser.add_argument("--canary-manifest-sha256")
    parser.add_argument("--capture-file")
    parser.add_argument("--capture-sha256")
    parser.add_argument("--custody-root")
    parser.add_argument("--instrument-id", action="append", default=[])
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args(argv)
    out: dict = {
        "status": "blocked",
        "mode": args.mode,
        "schema": args.schema,
        "sql_sha256": None,
        "catalog_sha256": None,
        "access_profile": ACCESS_PROFILE,
        "plan_version": PLAN_VERSION,
        "plan_sha256": None,
        "ddl": "not_attempted",
        "policy": "not_attempted",
        "maintenance": "not_attempted",
        "dml_committed": False,
        "published": False,
        "retryable": False,
        "maintenance_run_id": None,
        "changed_rows": 0,
        "audit": None,
        "code": None,
    }

    def finish(exit_code: int, **updates) -> int:
        out.update(updates)
        _emit(out)
        return exit_code

    try:
        if not SCHEMA_NAME.fullmatch(args.schema):
            raise ValueError("invalid_schema")
        ddl = DDL.read_bytes()
        out["sql_sha256"] = hashlib.sha256(ddl).hexdigest()
        if not hmac.compare_digest(out["sql_sha256"], args.expected_sql_sha256.lower()):
            raise ValueError("ddl_hash_mismatch")
        manifest = load_manifest(ddl)
        out["catalog_sha256"] = manifest["signature_sha256"]
        evidence, raw, audit = _governed_policy(args)
        out["audit"] = audit  # hashes, previous identity and validity only
        ids = [str(uuid.UUID(value)) for value in args.instrument_id]
        if len(ids) > MAX_MAINTENANCE_INSTRUMENTS or len(set(ids)) != len(ids):
            raise ValueError("backfill_allowlist_invalid")
        if ids and (not args.start or not args.end):
            raise ValueError("bounded_dates_required")
        start = dt.date.fromisoformat(args.start) if args.start else None
        end = dt.date.fromisoformat(args.end) if args.end else None
        if ids and (end < start or (end - start).days > MAX_MAINTENANCE_WINDOW_DAYS):
            raise ValueError("backfill_window_invalid")
        dsn = os.environ["NAV_READINESS_DATABASE_URL"]
    except (ValueError, TypeError, OSError, KeyError) as exc:
        code = str(exc) if isinstance(exc, ValueError) and _SAFE_CODES.fullmatch(str(exc)) else (
            type(exc).__name__
        )
        return finish(EXIT_BLOCKED, code=code)

    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
            try:
                state = _check(conn, args.schema)
            except PrerequisiteBlocked as exc:
                return finish(EXIT_INCOMPATIBLE, code=exc.args[0], prerequisite=exc.args[1])
            out.update(
                compatibility=state["compatibility"],
                access=state["access"],
                dependencies=state["dependencies"],
            )
            if state["code"] in PRE_DDL_BLOCKS:
                return finish(EXIT_INCOMPATIBLE, code=state["code"])
            conn.execute(sql.SQL("SET search_path TO {}, pg_temp").format(
                sql.Identifier(args.schema)))
            if ids and evidence is None and not state["ready"]:
                return finish(EXIT_BLOCKED, code="schema_not_ready_for_maintenance_plan")
            policy_sha = hashlib.sha256(raw).hexdigest() if raw else None
            conn.execute("BEGIN TRANSACTION READ ONLY")
            try:
                conn.execute("SET LOCAL statement_timeout = '30s'")
                conn.execute("SET LOCAL lock_timeout = '2s'")
                if audit is not None:
                    # The time window never needs W1 relations; the pointer
                    # rule runs whenever the policy relations exist (an
                    # absent schema has no pointer) and again under locks.
                    _audit_window(conn, evidence, audit)
                    if state["compatibility"] in ("exact", "repairable"):
                        _audit_pointer(conn, evidence, audit, policy_sha)
                dml_state = _dml_state(conn, evidence, ids, start, end)
            finally:
                conn.execute("ROLLBACK")
            plan, digest = build_plan(
                ddl=ddl, manifest=manifest, schema=args.schema, policy_raw=raw,
                evidence=evidence, ids=ids, start=args.start, end=args.end,
                dml_state=dml_state, audit=audit,
            )
            out["plan_sha256"] = digest
            if args.mode == "check":
                blocked = state["code"] is not None
                return finish(
                    EXIT_INCOMPATIBLE if blocked else 0,
                    status="blocked" if blocked else ("ready" if state["ready"] else "planned"),
                    code=state["code"],
                    plan=plan,
                )
            supplied = (args.plan_sha256 or "").lower()
            if supplied and supplied in (
                plan_hash(ddl, args.schema, raw, ids, args.start, args.end),
                previous_version_digest(plan),
                v2_version_digest(plan),
            ):
                return finish(EXIT_BLOCKED, code="plan_version_mismatch")
            stale = not hmac.compare_digest(digest, supplied)
            if stale and not (
                supplied
                and _plan_already_committed(
                    conn, supplied, evidence, ids, start, end,
                    audit=audit, policy_sha256=policy_sha,
                )
            ):
                return finish(EXIT_BLOCKED, code="PLAN_STALE" if supplied else "plan_hash_required")
            if state["compatibility"] == "exact" and state["access"] == "exact":
                out["ddl"] = "unchanged"
            else:
                apply_ddl(conn, args.schema, ddl)
                out["ddl"] = "applied"
            state = _check(conn, args.schema)
            out.update(access=state["access"], dependencies=state["dependencies"],
                       compatibility=state["compatibility"])
            if not state["ready"]:
                return finish(EXIT_INCOMPATIBLE, code=state["code"] or "schema_not_ready_after_apply")
            conn.execute(sql.SQL("SET search_path TO {}, pg_temp").format(
                sql.Identifier(args.schema)))
            if not evidence and not ids:
                return finish(0, status="applied" if out["ddl"] == "applied" else "unchanged")
            return _apply_dml(conn, out, finish, evidence, ids, start, end, digest,
                              plan, supplied, stale, audit=audit)
    except psycopg.Error as exc:
        return finish(EXIT_BLOCKED, code="database_error", sqlstate=exc.sqlstate)
    except (ValueError, TypeError, KeyError) as exc:
        code = str(exc) if isinstance(exc, ValueError) and _SAFE_CODES.fullmatch(str(exc)) else (
            type(exc).__name__
        )
        return finish(EXIT_BLOCKED, code=code)


def _plan_already_committed(
    conn, supplied: str, evidence, ids, start, end, *, audit=None, policy_sha256=None
) -> bool:
    """Read-only pre-DDL screen; ``_apply_dml`` decides again under the locks."""
    conn.execute("BEGIN TRANSACTION READ ONLY")
    try:
        return _already_applied(
            conn, supplied, evidence, ids, start, end,
            audit=audit, policy_sha256=policy_sha256,
        )
    finally:
        conn.execute("ROLLBACK")


def _apply_dml(conn, out, finish, evidence, ids, start, end, digest, plan, supplied,
               stale, *, audit=None):
    """One DML transaction: locks INGESTION->READINESS before any write."""
    requested = {"policy": evidence is not None, "maintenance": bool(ids)}
    if evidence is not None and audit is None:
        raise ValueError("audit_receipt_required")
    conn.execute("BEGIN")
    try:
        conn.execute("SET LOCAL statement_timeout = '60s'")
        conn.execute("SET LOCAL lock_timeout = '2s'")
        if not _try_nav_writer_locks(conn):
            conn.execute("ROLLBACK")
            return finish(EXIT_LOCK_BUSY, status="lock_busy", retryable=True,
                          code="nav_writer_lock_busy")
        replay = False
        if audit is not None:
            # Re-decided under the locks by the DB clock: the full receipt
            # window (capture <= now <= policy expiry, SEC deadline), then the
            # pointer: the audited previous version, or already the target
            # only as an exact replay of a recorded publication (no write).
            _audit_window(conn, evidence, audit)
            replay = _audit_pointer(conn, evidence, audit, plan["policy_sha256"])
        # Only a stale plan may be a replay, and only when this exact
        # operation (policy facts + publication receipt of the supplied plan
        # digest and audit identity; maintenance receipt) is persisted.
        if stale or _dml_state(conn, evidence, ids, start, end) != plan["before"]:
            if _already_applied(
                conn, supplied, evidence, ids, start, end,
                audit=audit, policy_sha256=plan["policy_sha256"],
            ):
                conn.execute("ROLLBACK")
                return finish(
                    0, status="unchanged", published=evidence is not None,
                    **{key: "unchanged" for key, on in requested.items() if on},
                )
            raise ValueError("PLAN_STALE")
        policy_changed = False
        if evidence is not None and not replay:
            _policy_hash, policy_changed = _publish_policy(conn, evidence)
            if policy_changed:
                _record_publication(conn, evidence, audit, plan["policy_sha256"], digest)
        maintenance = None
        if ids:
            maintenance = _apply_calendar_maintenance_tx(
                conn, ids, start, end, digest,
                expected_candidate_digest=plan["before"]["maintenance"]["candidate_digest"],
            )
        changed = policy_changed or (maintenance or {}).get("status") == "completed"
        conn.execute("COMMIT" if changed else "ROLLBACK")
    except BaseException as exc:
        conn.execute("ROLLBACK")
        rolled = {key: "rolled_back" for key, on in requested.items() if on}
        if isinstance(exc, psycopg.Error):
            return finish(EXIT_BLOCKED, code="database_error", sqlstate=exc.sqlstate, **rolled)
        if isinstance(exc, (ValueError, TypeError, KeyError)):
            code = str(exc) if _SAFE_CODES.fullmatch(str(exc)) else type(exc).__name__
            return finish(EXIT_BLOCKED, code=code, **rolled)
        raise
    return finish(
        0,
        status="applied" if changed else "unchanged",
        dml_committed=changed,
        published=evidence is not None,
        policy=("committed" if policy_changed else "unchanged") if evidence else "not_attempted",
        maintenance=(
            ("committed" if maintenance["status"] == "completed" else "unchanged")
            if maintenance else "not_attempted"
        ),
        maintenance_run_id=(maintenance or {}).get("maintenance_run_id"),
        changed_rows=(maintenance or {}).get("changed_rows", 0),
    )


if __name__ == "__main__":
    sys.exit(main())
