"""One-time generic static-rating artifact loader; never a runtime input."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from src.bonds.static_ratings import StaticRating, StaticRatingRefusal
from scripts.backfill_psql_transport import render_immutable_batch, render_schema


def install_schema(conn: Any) -> None:
    ddl = (Path(__file__).resolve().parents[2] / "schemas" / "bond_rating_static.sql").read_text(encoding="utf-8")
    conn.execute(ddl)


def load_static_mapping(conn: Any, rows: Sequence[StaticRating], *, batch_size: int = 500) -> dict[str, int]:
    """Insert exact rows once; an immutable non-identical collision is fatal."""
    if not rows:
        raise StaticRatingRefusal("empty_static_mapping")
    role = conn.execute("SELECT current_user").fetchone()[0]
    if role != "worker_writer":
        raise StaticRatingRefusal("writer_role_required")
    source_hashes = {row.source_sha256 for row in rows}
    if len(source_hashes) != 1:
        raise StaticRatingRefusal("mixed_source_sha256")
    source_sha256 = next(iter(source_hashes))
    target_hashes = {str(row[0]).strip() for row in conn.execute("SELECT DISTINCT source_sha256 FROM bond_rating_static").fetchall()}
    if target_hashes and target_hashes != {source_sha256}:
        raise StaticRatingRefusal("target_source_sha256_conflict")
    inserted = existing = 0
    for start in range(0, len(rows), batch_size):
        for row in rows[start:start + batch_size]:
            prior = conn.execute(
                "SELECT rating_bucket, rating_as_of_month, rating_state, reason_code, source_sha256, source_row_number FROM bond_rating_static WHERE cusip9=%s",
                (row.cusip9,),
            ).fetchone()
            expected = (row.rating_bucket, row.rating_as_of_month, row.rating_state, "static_backfill", row.source_sha256, row.source_row_number)
            if prior is not None:
                if tuple(prior) != expected:
                    raise StaticRatingRefusal("immutable_conflict")
                existing += 1
                continue
            conn.execute(
                "INSERT INTO bond_rating_static (cusip9,rating_bucket,rating_as_of_month,rating_state,reason_code,source_sha256,source_row_number) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (row.cusip9, row.rating_bucket, row.rating_as_of_month, row.rating_state, "static_backfill", row.source_sha256, row.source_row_number),
            )
            inserted += 1
    if inserted + existing != len(rows):
        raise StaticRatingRefusal("insert_reconciliation_failed")
    return {"inserted": inserted, "existing": existing, "conflicted": 0, "skipped": 0, "reconciled": inserted + existing}


def render_copy_slice(rows: Sequence[StaticRating], *, cursor: int, limit: int) -> str:
    """Return a stdout-pure psql/COPY slice for Railway SSH transport."""
    if cursor < 0 or limit <= 0:
        raise StaticRatingRefusal("invalid_cursor_slice")
    ordered = tuple(sorted(rows, key=lambda row: row.cusip9))
    if not ordered:
        raise StaticRatingRefusal("empty_static_mapping")
    if cursor > len(ordered):
        raise StaticRatingRefusal("cursor_beyond_mapping")
    if cursor == len(ordered):
        raise StaticRatingRefusal("cursor_at_end_requires_no_batch")
    source_hashes = {row.source_sha256 for row in ordered}
    if len(source_hashes) != 1:
        raise StaticRatingRefusal("mixed_source_sha256")
    selected = ordered[cursor:cursor + limit]
    committed_through = cursor + len(selected)
    source_sha256 = source_hashes.pop()
    rows_for_copy = tuple(
        (row.cusip9, row.rating_bucket, row.rating_as_of_month, row.rating_state,
         "static_backfill", row.source_sha256, row.source_row_number)
        for row in selected
    )
    target_evidence_sql = (
        "jsonb_build_object("
        f"'source_sha256', '{source_sha256}', 'cursor', {cursor}, 'selected', {len(selected)}, "
        f"'committed_through', {committed_through}, 'remaining', {len(ordered) - committed_through}, "
        f"'done', {'true' if committed_through == len(ordered) else 'false'}, "
        "'target_total_count', (SELECT count(*) FROM bond_rating_static))"
    )
    emitted = render_immutable_batch(
        target="bond_rating_static",
        columns=("cusip9", "rating_bucket", "rating_as_of_month", "rating_state", "reason_code", "source_sha256", "source_row_number"),
        key_columns=("cusip9",), rows=rows_for_copy, artifact_sha256=source_sha256,
        start_after=cursor, committed_through=committed_through, skipped=0,
        target_evidence_sql=target_evidence_sql,
        column_types=("char(9)", "text", "date", "text", "text", "char(64)", "bigint"),
    )
    guard = f"""DO $static_mapping_cursor$
DECLARE
    target_count bigint;
BEGIN
    IF EXISTS (SELECT 1 FROM bond_rating_static WHERE source_sha256 <> '{source_sha256}') THEN
        RAISE EXCEPTION 'mixed static-rating source_sha256';
    END IF;
    SELECT count(*) INTO target_count FROM bond_rating_static;
    IF target_count > {len(ordered)} THEN
        RAISE EXCEPTION 'static-rating target exceeds pinned mapping';
    END IF;
    IF target_count < {cursor} THEN
        RAISE EXCEPTION 'non-contiguous static-rating cursor';
    END IF;
    IF target_count > {cursor} AND (
        target_count < {committed_through}
        OR EXISTS (SELECT 1 FROM _backfill_stage s LEFT JOIN bond_rating_static t USING (cusip9) WHERE t.cusip9 IS NULL)
    ) THEN
        RAISE EXCEPTION 'non-contiguous static-rating cursor';
    END IF;
END
$static_mapping_cursor$;
"""
    marker = 'LOCK TABLE "bond_rating_static" IN SHARE ROW EXCLUSIVE MODE;\n'
    if marker not in emitted:  # pragma: no cover - protects the shared transport seam
        raise RuntimeError("psql_transport_lock_shape_changed")
    return emitted.replace(marker, marker + guard, 1)


def render_schema_install() -> str:
    """Emit only psql that installs the owned static-rating relation."""

    schema_path = Path(__file__).resolve().parents[2] / "schemas" / "bond_rating_static.sql"
    return render_schema(schema_path.read_text(encoding="utf-8"))
