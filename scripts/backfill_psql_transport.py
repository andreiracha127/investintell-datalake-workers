"""Bounded, stdin-safe ``psql`` transport for immutable parquet backfills.

The caller owns artifact parsing and cursor semantics.  This module only turns
already validated rows into one transaction that can be piped to a private
Railway Postgres service.  It intentionally never opens a connection or writes
an intermediate file.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from typing import Any


_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe_sql_identifier:{value}")
    return f'"{value}"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _copy_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def _copy_csv(rows: Iterable[Sequence[Any]]) -> str:
    """Return COPY CSV data; quote every non-null field so ``\\.`` is never a terminator."""
    def field(value: Any) -> str:
        if value is None:
            return r"\N"
        text = _copy_value(value)
        assert text is not None
        return '"' + text.replace('"', '""') + '"'

    return "".join(",".join(field(value) for value in row) + "\n" for row in rows)


def render_immutable_batch(
    *,
    target: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    artifact_sha256: str,
    start_after: int,
    committed_through: int,
    skipped: int,
    target_evidence_sql: str,
    column_types: Sequence[str] | None = None,
    nullable_columns: Sequence[str] = (),
) -> str:
    """Emit one idempotent immutable batch, with only SQL/COPY on stdout.

    The target evidence expression must evaluate to a JSONB object.  The
    generated transaction locks the target briefly, rejects either duplicate
    or existing-key evidence drift, and uses ``ON CONFLICT DO NOTHING`` only
    after that preflight passes.
    """
    if len(columns) == 0 or len(columns) != len(set(columns)):
        raise ValueError("columns must be non-empty and unique")
    if not key_columns or any(column not in columns for column in key_columns):
        raise ValueError("key_columns must be a non-empty subset of columns")
    if column_types is None:
        column_types = ("text",) * len(columns)
    if len(column_types) != len(columns):
        raise ValueError("column_types must match columns")
    if start_after < 0 or committed_through < start_after or skipped < 0:
        raise ValueError("invalid cursor or skipped count")
    if not re.fullmatch(r"[0-9a-f]{64}", artifact_sha256):
        raise ValueError("artifact_sha256 must be lowercase SHA-256")
    if any(len(row) != len(columns) for row in rows):
        raise ValueError("row width must match columns")

    nullable = set(nullable_columns)
    if not nullable.issubset(columns):
        raise ValueError("nullable_columns must be a subset of columns")
    quoted_target = _identifier(target)
    quoted_columns = [_identifier(column) for column in columns]
    quoted_keys = [_identifier(column) for column in key_columns]
    definitions = ", ".join(
        f"{quoted_column} {kind}" if column in nullable else f"{quoted_column} {kind} NOT NULL"
        for column, quoted_column, kind in zip(columns, quoted_columns, column_types, strict=True)
    )
    key_join = " AND ".join(f"t.{key} = s.{key}" for key in quoted_keys)
    key_join_staged = " AND ".join(f"a.{key} = b.{key}" for key in quoted_keys)
    comparison = "ROW(" + ", ".join(f"t.{column}" for column in quoted_columns) + ") IS DISTINCT FROM ROW(" + ", ".join(f"s.{column}" for column in quoted_columns) + ")"
    duplicate_comparison = "ROW(" + ", ".join(f"a.{column}" for column in quoted_columns) + ") IS DISTINCT FROM ROW(" + ", ".join(f"b.{column}" for column in quoted_columns) + ")"
    insert_columns = ", ".join(quoted_columns)
    key_list = ", ".join(quoted_keys)
    copy_payload = _copy_csv(rows)
    staged_count = len(rows)
    stage_values = ", ".join(f"s.{column}" for column in quoted_columns)

    return f"""\\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
CREATE TEMP TABLE _backfill_stage ({definitions}) ON COMMIT DROP;
COPY _backfill_stage ({insert_columns}) FROM STDIN WITH (FORMAT csv, NULL '\\N');
{copy_payload}\\.
LOCK TABLE {quoted_target} IN SHARE ROW EXCLUSIVE MODE;
DO $immutable_backfill$
BEGIN
    IF EXISTS (
        SELECT 1 FROM _backfill_stage a
        JOIN _backfill_stage b ON {key_join_staged} AND a.ctid < b.ctid
        WHERE {duplicate_comparison}
    ) THEN
        RAISE EXCEPTION 'immutable evidence conflict inside staged backfill slice';
    END IF;
    IF EXISTS (
        SELECT 1 FROM _backfill_stage s
        JOIN {quoted_target} t ON {key_join}
        WHERE {comparison}
    ) THEN
        RAISE EXCEPTION 'immutable evidence conflict with existing target row';
    END IF;
END
$immutable_backfill$;
CREATE TEMP TABLE _backfill_result (
    staged integer NOT NULL,
    inserted integer NOT NULL,
    existing integer NOT NULL,
    conflicted integer NOT NULL
) ON COMMIT PRESERVE ROWS;
WITH inserted AS (
    INSERT INTO {quoted_target} ({insert_columns})
    SELECT {stage_values} FROM _backfill_stage s
    ON CONFLICT ({key_list}) DO NOTHING
    RETURNING 1
)
INSERT INTO _backfill_result (staged, inserted, existing, conflicted)
SELECT {staged_count}, count(*)::integer, {staged_count} - count(*)::integer, 0 FROM inserted;
COMMIT;
SELECT jsonb_build_object(
    'artifact_sha256', {_sql_string(artifact_sha256)},
    'start_after', {start_after},
    'committed_through', {committed_through},
    'staged', (SELECT staged FROM _backfill_result),
    'inserted', (SELECT inserted FROM _backfill_result),
    'existing', (SELECT existing FROM _backfill_result),
    'conflicted', (SELECT conflicted FROM _backfill_result),
    'skipped', {skipped}
) || ({target_evidence_sql}) AS backfill_evidence;
"""


def render_schema(schema_sql: str) -> str:
    """Emit DDL as the administrative session; each schema transfers ownership.

    ``worker_writer`` intentionally has no blanket ``CREATE`` privilege on a
    hardened ``public`` schema.  Switching roles before ``CREATE TABLE`` would
    therefore fail before the DDL's explicit ``ALTER ... OWNER`` clauses run.
    Fact batches still use ``SET LOCAL ROLE worker_writer``; schema installation
    instead fails closed unless the psql session can create and transfer every
    declared object.
    """
    return "\\set ON_ERROR_STOP on\nBEGIN;\n" + schema_sql.rstrip() + "\nCOMMIT;\n"
