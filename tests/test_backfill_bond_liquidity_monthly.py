"""T2a OSBAP/TRACE monthly-liquidity backfill contracts without a database."""
from __future__ import annotations

from contextlib import nullcontext
import hashlib
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import backfill_bond_liquidity_monthly as backfill


def _write_panel(path: Path) -> str:
    pq.write_table(
        pa.table(
            {
                "cusip_id": ["037833100", "459200101", "594918104", "bad", "023135106"],
                "month": ["2025-01-31", "2025-02", "2025-03", "2025-04", "not-a-month"],
                "rel_bid_ask_bps": [12.5, -1.0, None, 4.0, 5.0],
                "quoted_days": [18, 7, 3, 1, 2],
                "dollar_volume": [1250000.0, 99.0, None, 42.0, 55.0],
                "ignored": list(range(5)),
            }
        ),
        path,
    )
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_streamed_panel_rows_pin_artifact_and_keep_row_identity_provenance(tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    expected_sha = _write_panel(panel)

    stream = backfill.panel_row_stream(panel, batch_size=1)
    rows = list(stream)

    assert stream.artifact_sha256 == expected_sha
    assert [row.cursor for row in rows] == [1, 2, 3]
    assert rows[0].source_provenance == {
        "artifact_sha256": expected_sha,
        "source_columns": list(backfill.REQUIRED_PANEL_COLUMNS),
        "row_identity": {"artifact_row": 1},
    }
    assert (rows[1].rel_bid_ask_bps, rows[1].quoted_days, rows[1].reason_code) == (
        None, 0, "crossed_rel_bid_ask_bps"
    )
    assert (rows[2].dollar_volume, rows[2].reason_code) == (None, "missing_rel_bid_ask_bps_missing_dollar_volume")
    assert stream.source_rows == 5
    assert dict(stream.reason_counts) == {"invalid_cusip9": 1, "invalid_month": 1}


def test_streamed_panel_rows_use_deterministic_cursor_and_do_not_materialize_parquet_table(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    panel = tmp_path / "panel.parquet"
    _write_panel(panel)
    monkeypatch.setattr(pq, "read_table", lambda *_args, **_kwargs: pytest.fail("must stream batches"))

    rows = list(backfill.panel_row_stream(panel, batch_size=2, resume_after=1))

    assert [row.cursor for row in rows] == [2, 3]


def test_stream_tracks_attempted_and_resume_skipped_rows_separately(tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    _write_panel(panel)
    stream = backfill.panel_row_stream(panel, batch_size=2, resume_after=2)
    assert [row.cursor for row in stream] == [3]
    assert (stream.source_rows, stream.attempted_rows, stream.resume_skipped_rows) == (5, 3, 2)


def test_immutable_batch_validation_is_idempotent_and_rejects_a_conflict_before_publish() -> None:
    incoming = backfill.LiquidityRow(
        "037833100", "2025-01-01", 10.0, 2, 100.0, "quoted",
        "valid_quote_valid_dollar_volume", "osbap_trace_historical",
        {"artifact_sha256": "a" * 64, "row_identity": {"artifact_row": 1}}, 1,
    )
    assert backfill.validate_immutable_batch({incoming.key: incoming}, [incoming]) == ([], 1)

    changed = backfill.LiquidityRow(
        "037833100", "2025-01-01", 11.0, 2, 100.0, "quoted",
        "valid_quote_valid_dollar_volume", "osbap_trace_historical",
        {"artifact_sha256": "a" * 64, "row_identity": {"artifact_row": 2}}, 2,
    )
    with pytest.raises(backfill.BackfillConflictError) as excinfo:
        backfill.validate_immutable_batch({incoming.key: incoming}, [incoming, changed])
    assert excinfo.value.summary["conflicted"] == 1
    assert excinfo.value.summary["pending_inserts"] == 0
    assert (excinfo.value.summary["prior_cursor"], excinfo.value.summary["conflict_cursor"]) == (1, 2)

    with pytest.raises(backfill.BackfillConflictError) as duplicate:
        backfill.validate_immutable_batch({}, [incoming, changed])
    assert duplicate.value.summary["prior_cursor"] == 1
    assert duplicate.value.summary["conflict_cursor"] == 2


def test_db_round_trip_preserves_exact_evidence_equality_including_artifact_row_provenance() -> None:
    incoming = backfill.LiquidityRow(
        "037833100", "2025-01-01", 10.1, 2, 100.2, "quoted",
        "valid_quote_valid_dollar_volume", "osbap_trace_historical",
        {"artifact_sha256": "a" * 64, "source_columns": [], "row_identity": {"artifact_row": 7}}, 7,
    )
    round_tripped = backfill.row_from_db((
        "037833100", "2025-01-01", Decimal("10.1"), 2, Decimal("100.2"), "quoted",
        "valid_quote_valid_dollar_volume", "osbap_trace_historical", incoming.source_provenance,
    ))
    assert backfill.classify_immutable_row(round_tripped, incoming) == "existing"


def test_conflict_summary_contains_actionable_provenance_and_safe_resume_cursor() -> None:
    conflict = backfill.conflict_summary(
        {"conflicted": 1, "conflict_key": ["037833100", "2025-01-01", "osbap_trace_historical"], "conflict_cursor": 9},
        artifact_sha256="a" * 64, last_safely_committed_cursor=5, inserted=4, existing=1, skipped=2,
    )
    assert conflict["artifact_sha256"] == "a" * 64
    assert conflict["last_safely_committed_cursor"] == 5
    assert conflict["conflict_key"] == ["037833100", "2025-01-01", "osbap_trace_historical"]
    assert conflict["conflict_cursor"] == 9


def test_checkpoint_emits_only_committed_resume_evidence() -> None:
    checkpoint = backfill.build_checkpoint(
        artifact_sha256="a" * 64, committed_through=20, inserted=12, existing=8, skipped=3,
    )
    assert checkpoint == {
        "artifact_sha256": "a" * 64, "committed_through": 20,
        "inserted": 12, "existing": 8, "skipped": 3,
    }


def test_target_metrics_reads_total_table_counts_with_a_fake_connection() -> None:
    class Result:
        def __init__(self, row: tuple[int, ...] | None = None, rows: list[tuple[int, int, int]] | None = None) -> None:
            self.row = row
            self.rows = rows or []

        def fetchone(self) -> tuple[int, ...] | None:
            return self.row

        def fetchall(self) -> list[tuple[int, int, int]]:
            return self.rows

    class Connection:
        def execute(self, query: str) -> Result:
            if "SELECT count(*)" in query:
                return Result((100,))
            return Result(rows=[(2024, 40, 20), (2025, 60, 30)])

    assert backfill.target_metrics(Connection()) == (
        100, {"2024": {"rows": 40, "quoted_rows": 20}, "2025": {"rows": 60, "quoted_rows": 30}},
    )


def test_run_emits_a_reusable_checkpoint_after_each_committed_batch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    panel = tmp_path / "unused.parquet"
    panel.write_bytes(b"fixture")
    rows = [
        backfill.LiquidityRow("037833100", "2025-01-01", 1.0, 1, 1.0, "quoted", "valid_quote_valid_dollar_volume", backfill.SOURCE, {"artifact_sha256": "a" * 64, "row_identity": {"artifact_row": cursor}}, cursor)
        for cursor in (1, 2)
    ]

    class Stream:
        artifact_sha256 = "a" * 64
        source_rows = attempted_rows = 2
        resume_skipped_rows = 0
        last_cursor = 2
        reason_counts: dict[str, int] = {}

        def __iter__(self):
            return iter(rows)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def commit(self) -> None:
            return None

        def transaction(self):
            return nullcontext()

    monkeypatch.setattr(backfill, "panel_row_stream", lambda *_args, **_kwargs: Stream())
    monkeypatch.setattr(backfill.psycopg, "connect", lambda _dsn: Connection())
    monkeypatch.setattr(backfill, "install_schema", lambda _conn: None)
    monkeypatch.setattr(backfill, "persist_batch", lambda _conn, items: (len(items), 0))
    monkeypatch.setattr(backfill, "target_metrics", lambda _conn: (2, {"2025": {"rows": 2, "quoted_rows": 2}}))
    checkpoints: list[dict[str, int | str]] = []

    summary = backfill.run("postgresql://redacted", panel, batch_size=1, checkpoint_sink=checkpoints.append)

    assert [checkpoint["committed_through"] for checkpoint in checkpoints] == [1, 2]
    assert summary["target_row_count"] == 2


def test_run_conflict_after_a_prior_commit_reports_the_safe_resume_cursor(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    panel = tmp_path / "unused.parquet"
    panel.write_bytes(b"fixture")
    rows = [
        backfill.LiquidityRow("037833100", "2025-01-01", float(cursor), 1, 1.0, "quoted", "valid_quote_valid_dollar_volume", backfill.SOURCE, {"artifact_sha256": "a" * 64, "row_identity": {"artifact_row": cursor}}, cursor)
        for cursor in (1, 2)
    ]

    class Stream:
        artifact_sha256 = "a" * 64
        source_rows = attempted_rows = 2
        resume_skipped_rows = 0
        last_cursor = 2
        reason_counts: dict[str, int] = {}

        def __iter__(self):
            return iter(rows)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def commit(self) -> None:
            return None

        def transaction(self):
            return nullcontext()

    calls = 0

    def persist(_conn: object, items: list[backfill.LiquidityRow]) -> tuple[int, int]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise backfill.BackfillConflictError({"conflicted": 1, "conflict_key": list(items[0].key), "conflict_cursor": 2})
        return (1, 0)

    monkeypatch.setattr(backfill, "panel_row_stream", lambda *_args, **_kwargs: Stream())
    monkeypatch.setattr(backfill.psycopg, "connect", lambda _dsn: Connection())
    monkeypatch.setattr(backfill, "install_schema", lambda _conn: None)
    monkeypatch.setattr(backfill, "persist_batch", persist)

    with pytest.raises(backfill.BackfillConflictError) as excinfo:
        backfill.run("postgresql://redacted", panel, batch_size=1)
    assert excinfo.value.summary["last_safely_committed_cursor"] == 1
    assert excinfo.value.summary["conflict_cursor"] == 2


def test_summary_distinguishes_resume_slice_from_total_target_coverage() -> None:
    summary = backfill.build_summary(
        artifact_sha256="a" * 64, source_rows=5, attempted_rows=3, resume_skipped_rows=2,
        inserted=2, existing=1, conflicted=0, skipped=2, last_cursor=3,
        reason_counts={"invalid_cusip9": 1, "invalid_month": 1},
        slice_quote_coverage_by_year={"2025": {"rows": 3, "quoted_rows": 1}},
        target_row_count=100,
        target_quote_coverage_by_year={"2024": {"rows": 40, "quoted_rows": 20}, "2025": {"rows": 60, "quoted_rows": 30}},
    )
    assert summary["source_rows"] == 5
    assert summary["attempted_rows"] == 3
    assert summary["resume_skipped_rows"] == 2
    assert summary["slice_row_count"] == 3
    assert summary["target_row_count"] == 100
    assert summary["last_cursor"] == 3
    assert summary["reason_counts"] == {"invalid_cusip9": 1, "invalid_month": 1}
    assert summary["slice_quote_coverage_by_year"] == {"2025": {"rows": 3, "quoted_rows": 1, "quote_coverage": 1 / 3}}
    assert summary["target_quote_coverage_by_year"] == {
        "2024": {"rows": 40, "quoted_rows": 20, "quote_coverage": 0.5},
        "2025": {"rows": 60, "quoted_rows": 30, "quote_coverage": 0.5},
    }
