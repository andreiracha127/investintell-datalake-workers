"""psql transport contracts, run without a database."""
from __future__ import annotations

import csv
import io
import json

from scripts.backfill_psql_transport import render_immutable_batch


def test_copy_transport_round_trips_json_provenance_with_quotes_and_newlines() -> None:
    provenance = {"note": "single ' quote, double \" quote, and a newline\nnext"}
    emitted = render_immutable_batch(
        target="immutable_target",
        columns=("key", "source_provenance"),
        key_columns=("key",),
        rows=[("k1", provenance)],
        artifact_sha256="a" * 64,
        start_after=0,
        committed_through=1,
        skipped=0,
        target_evidence_sql="jsonb_build_object('target_row_count', (SELECT count(*) FROM immutable_target))",
    )

    copy_payload = emitted.split("FROM STDIN WITH (FORMAT csv, NULL '\\N');\n", 1)[1].split("\n\\.\n", 1)[0]
    parsed = next(csv.reader(io.StringIO(copy_payload)))
    assert json.loads(parsed[1]) == provenance
    assert emitted.startswith("\\set ON_ERROR_STOP on\n")
    assert "RAISE EXCEPTION" in emitted
    assert "IS DISTINCT FROM" in emitted
    assert "ON CONFLICT" in emitted


def test_empty_psql_slice_is_a_safe_transaction_with_zero_reconciliation() -> None:
    emitted = render_immutable_batch(
        target="immutable_target",
        columns=("key", "source_provenance"),
        key_columns=("key",), rows=[], artifact_sha256="a" * 64,
        start_after=9, committed_through=9, skipped=2,
        target_evidence_sql="jsonb_build_object('target_row_count', (SELECT count(*) FROM immutable_target))",
    )

    assert "SELECT 0, count(*)::integer, 0 - count(*)::integer, 0 FROM inserted" in emitted
    assert "'skipped', 2" in emitted
    assert "'committed_through', 9" in emitted


def test_copy_transport_preserves_sql_null_as_an_unquoted_copy_null_marker() -> None:
    emitted = render_immutable_batch(
        target="immutable_target",
        columns=("key", "optional_value"), key_columns=("key",), rows=[("k1", None)],
        nullable_columns=("optional_value",),
        artifact_sha256="a" * 64, start_after=0, committed_through=1, skipped=0,
        target_evidence_sql="jsonb_build_object()",
    )

    assert '"k1",\\N\n\\.\n' in emitted
    assert '"optional_value" text NOT NULL' not in emitted
