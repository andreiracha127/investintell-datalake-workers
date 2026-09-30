"""Governed transport for the allowlisted NAV provenance schema upgrade."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql


REPO_ROOT = Path(__file__).resolve().parents[1]
ALLOWED_SQL_FILE = (REPO_ROOT / "schemas" / "nav_timeseries_provenance.sql").resolve()
SCHEMA_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_$]{0,62}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
EXPECTED_COLUMNS = (
    ("source_nav", "numeric(18,6)"),
    ("source_nav_kind", "character varying(16)"),
    ("nav_repair_kind", "character varying(48)"),
    ("return_start_date", "date"),
    ("return_source_boundary", "boolean"),
    ("return_uses_repaired_nav", "boolean"),
    ("return_semantics", "character varying(48)"),
    ("return_verification_status", "character varying(24)"),
    ("calendar_id", "character varying(128)"),
    ("calendar_version", "character varying(64)"),
    ("calendar_source", "text"),
)


class RunnerInputError(ValueError):
    """Raised when governed runner inputs do not match the allowlist."""


def _resolve_sql_file(value: str) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = REPO_ROOT / candidate
    resolved = candidate.resolve()
    if resolved != ALLOWED_SQL_FILE:
        raise RunnerInputError("sql_file_not_allowlisted")
    return resolved


def _validate_schema(value: str) -> str:
    if not SCHEMA_RE.fullmatch(value):
        raise RunnerInputError("invalid_schema_identifier")
    return value


def _read_verified_sql(path: Path, expected_sha256: str) -> tuple[bytes, str]:
    expected = expected_sha256.lower()
    if not SHA256_RE.fullmatch(expected):
        raise RunnerInputError("invalid_expected_sha256")
    content = path.read_bytes()
    actual = hashlib.sha256(content).hexdigest()
    if not hmac.compare_digest(actual, expected):
        raise RunnerInputError("sql_sha256_mismatch")
    return content, actual


def _column_state(
    conn: psycopg.Connection[Any], relation_oid: int
) -> dict[str, tuple[Any, ...]]:
    rows = conn.execute(
        """
        SELECT a.attname,
               pg_catalog.format_type(a.atttypid, a.atttypmod),
               a.attnotnull,
               a.attgenerated,
               a.attidentity,
               ad.oid IS NOT NULL
          FROM pg_catalog.pg_attribute AS a
          LEFT JOIN pg_catalog.pg_attrdef AS ad
            ON ad.adrelid = a.attrelid
           AND ad.adnum = a.attnum
         WHERE a.attrelid = %s
           AND a.attnum > 0
           AND NOT a.attisdropped
        """,
        (relation_oid,),
    ).fetchall()
    return {row[0]: tuple(row[1:]) for row in rows}


def _check(conn: psycopg.Connection[Any], schema: str, sha256: str) -> dict[str, Any]:
    conn.execute("BEGIN TRANSACTION READ ONLY")
    try:
        conn.execute("SET LOCAL lock_timeout = '1s'")
        conn.execute("SET LOCAL statement_timeout = '5s'")
        conn.execute("SET LOCAL idle_in_transaction_session_timeout = '10s'")
        conn.execute(
            sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema))
        )

        current_schema = conn.execute("SELECT current_schema()").fetchone()[0]
        if current_schema != schema:
            raise RunnerInputError("target_schema_not_available")

        relation = conn.execute(
            """
            SELECT c.oid, c.relkind
              FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
             WHERE n.nspname = %s
               AND c.relname = 'nav_timeseries'
            """,
            (schema,),
        ).fetchone()
        if relation is None:
            raise RunnerInputError("target_table_missing")
        if relation[1] != "r":
            raise RunnerInputError("target_relation_wrong_kind")

        state = _column_state(conn, relation[0])
        missing = []
        mismatched = []
        for name, type_name in EXPECTED_COLUMNS:
            actual = state.get(name)
            if actual is None:
                missing.append(name)
            elif actual != (type_name, False, "", "", False):
                mismatched.append(name)

        server_version_num = conn.execute(
            "SELECT current_setting('server_version_num')::integer"
        ).fetchone()[0]
        timescale = conn.execute(
            "SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'timescaledb'"
        ).fetchone()
        status = (
            "contract_mismatch"
            if mismatched
            else ("upgrade_required" if missing else "ready")
        )
        return {
            "mode": "check",
            "schema": schema,
            "sha256": sha256,
            "status": status,
            "missing_count": len(missing),
            "mismatch_count": len(mismatched),
            "server_version_num": server_version_num,
            "timescaledb_extversion": timescale[0] if timescale else None,
        }
    finally:
        conn.execute("ROLLBACK")


def _apply(
    conn: psycopg.Connection[Any], schema: str, content: bytes, sha256: str
) -> dict[str, Any]:
    conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
    started = time.monotonic()
    conn.execute(content.decode("utf-8"))
    elapsed_ms = round((time.monotonic() - started) * 1000)
    return {
        "mode": "apply",
        "schema": schema,
        "sha256": sha256,
        "status": "applied",
        "elapsed_ms": elapsed_ms,
    }


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("check", "apply"))
    parser.add_argument("--schema", required=True)
    parser.add_argument("--sql-file", required=True)
    parser.add_argument("--expected-sha256", required=True)
    args = parser.parse_args(argv)

    try:
        schema = _validate_schema(args.schema)
        path = _resolve_sql_file(args.sql_file)
        content, sha256 = _read_verified_sql(path, args.expected_sha256)
        dsn = os.environ["DATABASE_URL"]
    except (KeyError, OSError, RunnerInputError):
        _emit({"status": "input_error"})
        return 2

    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            result = (
                _check(conn, schema, sha256)
                if args.mode == "check"
                else _apply(conn, schema, content, sha256)
            )
    except RunnerInputError:
        _emit(
            {
                "mode": args.mode,
                "schema": schema,
                "sha256": sha256,
                "status": "check_failed",
            }
        )
        return 3
    except psycopg.Error as exc:
        _emit(
            {
                "mode": args.mode,
                "schema": schema,
                "sha256": sha256,
                "status": "database_error",
                "sqlstate": exc.sqlstate or "unknown",
            }
        )
        return 4
    except Exception:
        _emit(
            {
                "mode": args.mode,
                "schema": schema,
                "sha256": sha256,
                "status": "internal_error",
            }
        )
        return 5

    _emit(result)
    return 3 if result.get("status") == "contract_mismatch" else 0


if __name__ == "__main__":
    sys.exit(main())
