"""Stream an operator-pinned OSBAP/TRACE artifact into immutable monthly liquidity.

The parquet is a one-time historical-backfill input.  It is not a runtime data
source.  The cursor is the deterministic one-based artifact row number, so a
stopped run resumes with ``--resume-after`` without holding the artifact in RAM.
"""
from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

import pyarrow.parquet as pq
import psycopg
from psycopg.types.json import Jsonb

from src.bonds.errors import BondError
from src.bonds.panel_sources import resolve_monthly_liquidity
from src.db import resolve_dsn
from scripts.backfill_psql_transport import render_immutable_batch, render_schema

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schemas" / "bond_panel_sources.sql"
REQUIRED_PANEL_COLUMNS = ("cusip_id", "month", "rel_bid_ask_bps", "quoted_days", "dollar_volume")
SOURCE = "osbap_trace_historical"


class PanelArtifactError(ValueError):
    pass


class BackfillConflictError(RuntimeError):
    def __init__(self, summary: dict[str, Any]) -> None:
        self.summary = summary
        super().__init__("bond_liquidity_monthly immutable evidence conflict")


@dataclass(frozen=True)
class LiquidityRow:
    cusip9: str
    month: str
    rel_bid_ask_bps: float | None
    quoted_days: int
    dollar_volume: float | None
    quote_state: str
    reason_code: str
    source: str
    source_provenance: dict[str, Any]
    cursor: int

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.cusip9, self.month, self.source)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class PanelRowStream:
    """A single-use bounded parquet stream with post-consumption accounting."""

    def __init__(self, path: Path, *, batch_size: int, resume_after: int, max_rows: int | None = None) -> None:
        if not path.is_file():
            raise PanelArtifactError("artifact_unavailable")
        self.path = path
        self.batch_size = batch_size
        self.resume_after = resume_after
        self.max_rows = max_rows
        self.artifact_sha256 = _sha256(path)
        self.source_rows = 0
        self.attempted_rows = 0
        self.resume_skipped_rows = 0
        self.last_cursor = 0
        self.reason_counts: Counter[str] = Counter()

    def __iter__(self) -> Iterator[LiquidityRow]:
        try:
            parquet = pq.ParquetFile(self.path)
        except Exception as exc:
            raise PanelArtifactError("unreadable_parquet") from exc
        missing = [column for column in REQUIRED_PANEL_COLUMNS if column not in parquet.schema_arrow.names]
        if missing:
            raise PanelArtifactError(f"missing_required_columns:{','.join(missing)}")
        cursor = slice_rows = 0
        for batch in parquet.iter_batches(columns=list(REQUIRED_PANEL_COLUMNS), batch_size=self.batch_size):
            values = batch.to_pydict()
            for raw in zip(*(values[column] for column in REQUIRED_PANEL_COLUMNS), strict=True):
                cursor += 1
                if cursor > self.resume_after and self.max_rows is not None and slice_rows >= self.max_rows:
                    return
                self.source_rows += 1
                self.last_cursor = cursor
                if cursor <= self.resume_after:
                    self.resume_skipped_rows += 1
                    continue
                slice_rows += 1
                self.attempted_rows += 1
                try:
                    resolved = resolve_monthly_liquidity(*raw)
                except BondError as exc:
                    self.reason_counts[exc.code] += 1
                    continue
                yield LiquidityRow(
                    resolved.cusip9, resolved.month.isoformat(), resolved.rel_bid_ask_bps,
                    resolved.quoted_days, resolved.dollar_volume, resolved.quote_state,
                    resolved.reason_code, SOURCE,
                    {
                        "artifact_sha256": self.artifact_sha256,
                        "source_columns": list(REQUIRED_PANEL_COLUMNS),
                        "row_identity": {"artifact_row": cursor},
                    },
                    cursor,
                )


def panel_row_stream(path: Path, *, batch_size: int = 10_000, resume_after: int = 0,
                     max_rows: int | None = None) -> PanelRowStream:
    if batch_size <= 0 or resume_after < 0 or max_rows is not None and max_rows <= 0:
        raise ValueError("batch_size must be positive, resume_after non-negative, and max_rows positive")
    return PanelRowStream(path, batch_size=batch_size, resume_after=resume_after, max_rows=max_rows)


def classify_immutable_row(existing: LiquidityRow, incoming: LiquidityRow) -> str:
    return "existing" if existing == incoming else "conflicted"


def validate_immutable_batch(
    existing_rows: Mapping[tuple[str, str, str], LiquidityRow], incoming_rows: Sequence[LiquidityRow]
) -> tuple[list[LiquidityRow], int]:
    """Preflight a whole transaction; do not begin inserts if any row conflicts."""
    pending: list[LiquidityRow] = []
    existing = 0
    seen = dict(existing_rows)
    for incoming in incoming_rows:
        current = seen.get(incoming.key)
        if current is None:
            pending.append(incoming)
            seen[incoming.key] = incoming
        elif classify_immutable_row(current, incoming) == "existing":
            existing += 1
        else:
            raise BackfillConflictError({
                "conflicted": 1, "pending_inserts": 0, "conflict_key": list(incoming.key),
                "prior_cursor": current.cursor, "conflict_cursor": incoming.cursor,
            })
    return pending, existing


def install_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def row_from_db(row: Sequence[Any]) -> LiquidityRow:
    """Rebuild a row exactly as persisted, including immutable artifact identity."""
    provenance = dict(row[8])
    row_identity = provenance.get("row_identity")
    if not isinstance(row_identity, dict) or not isinstance(row_identity.get("artifact_row"), int):
        raise ValueError("stored_liquidity_row_missing_artifact_cursor")
    return LiquidityRow(
        row[0], row[1], None if row[2] is None else float(row[2]), row[3],
        None if row[4] is None else float(row[4]), row[5], row[6], row[7], provenance,
        row_identity["artifact_row"],
    )


def _existing_rows(conn: psycopg.Connection, incoming: Sequence[LiquidityRow]) -> dict[tuple[str, str, str], LiquidityRow]:
    found: dict[tuple[str, str, str], LiquidityRow] = {}
    for item in incoming:
        row = conn.execute(
            "SELECT cusip9, month::text, rel_bid_ask_bps, quoted_days, dollar_volume, quote_state, "
            "reason_code, source, source_provenance FROM bond_liquidity_monthly "
            "WHERE cusip9=%s AND month=%s::date AND source=%s",
            item.key,
        ).fetchone()
        if row is not None:
            found[item.key] = row_from_db(row)
    return found


def persist_batch(conn: psycopg.Connection, incoming: Sequence[LiquidityRow]) -> tuple[int, int]:
    existing_rows = _existing_rows(conn, incoming)
    pending, existing = validate_immutable_batch(existing_rows, incoming)
    for item in pending:
        conn.execute(
            "INSERT INTO bond_liquidity_monthly "
            "(cusip9, month, rel_bid_ask_bps, quoted_days, dollar_volume, quote_state, reason_code, source, source_provenance) "
            "VALUES (%s,%s::date,%s,%s,%s,%s,%s,%s,%s)",
            (item.cusip9, item.month, item.rel_bid_ask_bps, item.quoted_days, item.dollar_volume,
             item.quote_state, item.reason_code, item.source, Jsonb(item.source_provenance)),
        )
    return len(pending), existing


def _coverage_with_rates(coverage: Mapping[str, Mapping[str, int]]) -> dict[str, dict[str, int | float]]:
    return {
        year: {**counts, "quote_coverage": counts["quoted_rows"] / counts["rows"] if counts["rows"] else 0.0}
        for year, counts in sorted(coverage.items())
    }


def build_checkpoint(*, artifact_sha256: str, committed_through: int, inserted: int,
                     existing: int, skipped: int) -> dict[str, int | str]:
    return {
        "artifact_sha256": artifact_sha256, "committed_through": committed_through,
        "inserted": inserted, "existing": existing, "skipped": skipped,
    }


def conflict_summary(detail: Mapping[str, Any], *, artifact_sha256: str,
                     last_safely_committed_cursor: int, inserted: int, existing: int,
                     skipped: int) -> dict[str, Any]:
    return {
        **detail, "artifact_sha256": artifact_sha256,
        "last_safely_committed_cursor": last_safely_committed_cursor,
        "inserted": inserted, "existing": existing, "skipped": skipped,
    }


def _require_expected_sha256(actual: str, expected: str | None) -> None:
    if expected is not None and actual != expected:
        raise PanelArtifactError("artifact_sha256_mismatch")


def emit_psql_batch(
    panel: Path, *, start_after: int, max_rows: int, expected_sha256: str | None = None,
) -> str:
    """Stream one bounded artifact-row slice into a private-network psql transaction."""
    stream = panel_row_stream(panel, resume_after=start_after, max_rows=max_rows)
    _require_expected_sha256(stream.artifact_sha256, expected_sha256)
    rows = list(stream)
    if start_after > stream.last_cursor:
        raise PanelArtifactError("start_after_exceeds_artifact_cursor")
    committed_through = stream.last_cursor
    evidence = """jsonb_build_object(
        'target_row_count', (SELECT count(*) FROM bond_liquidity_monthly),
        'target_quote_coverage_by_year', COALESCE((SELECT jsonb_object_agg(year, coverage)
            FROM (SELECT EXTRACT(YEAR FROM month)::integer::text AS year,
                jsonb_build_object('rows', count(*), 'quoted_rows', count(*) FILTER (WHERE quote_state = 'quoted')) AS coverage
                FROM bond_liquidity_monthly GROUP BY 1 ORDER BY 1) coverage_by_year), '{}'::jsonb)
    )"""
    return render_immutable_batch(
        target="bond_liquidity_monthly",
        columns=("cusip9", "month", "rel_bid_ask_bps", "quoted_days", "dollar_volume", "quote_state", "reason_code", "source", "source_provenance"),
        column_types=("text", "date", "numeric", "integer", "numeric", "text", "text", "text", "jsonb"),
        nullable_columns=("rel_bid_ask_bps", "dollar_volume"),
        key_columns=("cusip9", "month", "source"),
        rows=[(
            row.cusip9, row.month, row.rel_bid_ask_bps, row.quoted_days, row.dollar_volume,
            row.quote_state, row.reason_code, row.source, row.source_provenance,
        ) for row in rows],
        artifact_sha256=stream.artifact_sha256,
        start_after=start_after,
        committed_through=committed_through,
        skipped=sum(stream.reason_counts.values()),
        target_evidence_sql=evidence,
    )


def target_metrics(conn: Any) -> tuple[int, dict[str, dict[str, int]]]:
    """Read the complete immutable target, never just the resumed input slice."""
    row_count = conn.execute("SELECT count(*) FROM bond_liquidity_monthly").fetchone()[0]
    coverage: dict[str, dict[str, int]] = {}
    for year, rows, quoted_rows in conn.execute(
        "SELECT EXTRACT(YEAR FROM month)::integer, count(*), "
        "count(*) FILTER (WHERE quote_state = 'quoted') "
        "FROM bond_liquidity_monthly GROUP BY 1 ORDER BY 1"
    ).fetchall():
        coverage[str(year)] = {"rows": rows, "quoted_rows": quoted_rows}
    return row_count, coverage


def build_summary(*, artifact_sha256: str, source_rows: int, attempted_rows: int,
                  resume_skipped_rows: int, inserted: int, existing: int, conflicted: int,
                  skipped: int, last_cursor: int, reason_counts: dict[str, int],
                  slice_quote_coverage_by_year: dict[str, dict[str, int]], target_row_count: int,
                  target_quote_coverage_by_year: dict[str, dict[str, int]]) -> dict[str, Any]:
    return {
        "artifact_sha256": artifact_sha256, "source_rows": source_rows, "inserted": inserted,
        "attempted_rows": attempted_rows, "resume_skipped_rows": resume_skipped_rows,
        "existing": existing, "slice_row_count": inserted + existing, "target_row_count": target_row_count,
        "conflicted": conflicted, "skipped": skipped, "last_cursor": last_cursor,
        "reason_counts": dict(sorted(reason_counts.items())),
        "slice_quote_coverage_by_year": _coverage_with_rates(slice_quote_coverage_by_year),
        "target_quote_coverage_by_year": _coverage_with_rates(target_quote_coverage_by_year),
    }


def run(dsn: str, panel: Path, *, batch_size: int = 10_000, resume_after: int = 0,
        checkpoint_sink: Callable[[dict[str, int | str]], None] | None = None) -> dict[str, Any]:
    stream = panel_row_stream(panel, batch_size=batch_size, resume_after=resume_after)
    inserted = existing = last_cursor = last_safely_committed_cursor = 0
    coverage: dict[str, Counter[str]] = {}
    with psycopg.connect(dsn) as conn:
        install_schema(conn)
        conn.commit()
        batch: list[LiquidityRow] = []
        for item in stream:
            batch.append(item)
            if len(batch) == batch_size:
                try:
                    with conn.transaction():
                        added, already = persist_batch(conn, batch)
                except BackfillConflictError as exc:
                    raise BackfillConflictError(conflict_summary(
                        exc.summary, artifact_sha256=stream.artifact_sha256,
                        last_safely_committed_cursor=last_safely_committed_cursor,
                        inserted=inserted, existing=existing, skipped=sum(stream.reason_counts.values()),
                    )) from exc
                inserted += added
                existing += already
                for row in batch:
                    stats = coverage.setdefault(row.month[:4], Counter())
                    stats["rows"] += 1
                    stats["quoted_rows"] += row.quote_state == "quoted"
                last_safely_committed_cursor = batch[-1].cursor
                if checkpoint_sink is not None:
                    checkpoint_sink(build_checkpoint(
                        artifact_sha256=stream.artifact_sha256, committed_through=last_safely_committed_cursor,
                        inserted=inserted, existing=existing, skipped=sum(stream.reason_counts.values()),
                    ))
                batch.clear()
        if batch:
            try:
                with conn.transaction():
                    added, already = persist_batch(conn, batch)
            except BackfillConflictError as exc:
                raise BackfillConflictError(conflict_summary(
                    exc.summary, artifact_sha256=stream.artifact_sha256,
                    last_safely_committed_cursor=last_safely_committed_cursor,
                    inserted=inserted, existing=existing, skipped=sum(stream.reason_counts.values()),
                )) from exc
            inserted += added
            existing += already
            for row in batch:
                stats = coverage.setdefault(row.month[:4], Counter())
                stats["rows"] += 1
                stats["quoted_rows"] += row.quote_state == "quoted"
            last_safely_committed_cursor = batch[-1].cursor
            if checkpoint_sink is not None:
                checkpoint_sink(build_checkpoint(
                    artifact_sha256=stream.artifact_sha256, committed_through=last_safely_committed_cursor,
                    inserted=inserted, existing=existing, skipped=sum(stream.reason_counts.values()),
                ))
        last_cursor = stream.last_cursor
        target_row_count, target_coverage = target_metrics(conn)
    return build_summary(
        artifact_sha256=stream.artifact_sha256, source_rows=stream.source_rows,
        attempted_rows=stream.attempted_rows, resume_skipped_rows=stream.resume_skipped_rows,
        inserted=inserted, existing=existing, conflicted=0, skipped=sum(stream.reason_counts.values()),
        last_cursor=last_cursor, reason_counts=dict(stream.reason_counts),
        slice_quote_coverage_by_year={year: dict(counts) for year, counts in coverage.items()},
        target_row_count=target_row_count, target_quote_coverage_by_year=target_coverage,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__ + "\n\nPrivate production route: emit then pipe only stdout to "
        "`railway ssh --service market-clean-serial -- psql -v ON_ERROR_STOP=1 -f -`. "
        "Run --emit-schema once, then bounded --emit-batch-psql slices.",
    )
    parser.add_argument("--dsn", default=None, help="target datalake DSN; defaults to DATABASE_URL")
    parser.add_argument("--panel", type=Path, help="explicit OSBAP/TRACE parquet artifact")
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--resume-after", type=int, default=0, help="last completed artifact-row cursor")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--emit-schema", action="store_true", help="emit schema SQL for the private psql route")
    mode.add_argument("--emit-batch-psql", action="store_true", help="emit one bounded psql/COPY transaction to stdout")
    parser.add_argument("--start-after", type=int, default=0, help="artifact-row cursor already committed (emit mode)")
    parser.add_argument("--max-rows", type=int, help="maximum artifact rows in one emitted transaction")
    parser.add_argument("--expected-sha256", help="fail before emit unless this exact artifact SHA-256 matches")
    args = parser.parse_args(argv)
    if args.emit_schema:
        print(render_schema(SCHEMA_PATH.read_text(encoding="utf-8")), end="")
        return 0
    if args.panel is None:
        parser.error("--panel is required unless --emit-schema is selected")
    if args.emit_batch_psql:
        if args.max_rows is None:
            parser.error("--max-rows is required with --emit-batch-psql")
        if args.expected_sha256 is None:
            parser.error("--expected-sha256 is required with --emit-batch-psql")
        try:
            print(emit_psql_batch(
                args.panel, start_after=args.start_after, max_rows=args.max_rows,
                expected_sha256=args.expected_sha256,
            ), end="")
        except (PanelArtifactError, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            return 2
        return 0
    try:
        summary = run(
            resolve_dsn(args.dsn), args.panel, batch_size=args.batch_size, resume_after=args.resume_after,
            checkpoint_sink=lambda checkpoint: print(
                json.dumps({"checkpoint": checkpoint}, sort_keys=True), flush=True
            ),
        )
    except BackfillConflictError as exc:
        print(json.dumps(exc.summary, sort_keys=True))
        return 2
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
