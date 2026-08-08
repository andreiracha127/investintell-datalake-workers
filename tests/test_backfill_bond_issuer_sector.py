"""One-time FF17 backfill planning contracts, exercised without a database."""
from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import backfill_bond_issuer_sector as backfill


def _write_panel(path: Path) -> str:
    pq.write_table(
        pa.table(
            {
                "cusip_id": ["037833100", "037833100", "459200101", "bad"],
                "ff17num": [4, 4, 16, 7],
                "ignored": ["a", "b", "c", "d"],
            }
        ),
        path,
    )
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_panel_load_hashes_artifact_streams_required_columns_and_records_modal_evidence(tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    expected_hash = _write_panel(panel)

    loaded = backfill.load_osbap_panel(panel, batch_size=1)

    assert loaded.artifact_sha256 == expected_hash
    assert [(row.cusip9, row.ff17num, row.disagreement_count) for row in loaded.rows] == [
        ("037833100", 4, 0),
        ("459200101", 16, 0),
    ]
    assert loaded.reason_counts == {"invalid_cusip9": 1}


def test_panel_load_uses_deterministic_mode_and_counts_dissenting_months(tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    pq.write_table(
        pa.table({"cusip_id": ["037833100"] * 4, "ff17num": [8, 4, 8, 4]}), panel
    )

    loaded = backfill.load_osbap_panel(panel)

    assert [(row.cusip9, row.ff17num, row.disagreement_count) for row in loaded.rows] == [
        ("037833100", 4, 2),
    ]


def test_immutable_merge_is_idempotent_only_for_identical_evidence_and_refuses_drift() -> None:
    incoming = backfill.SectorRow.osbap("037833100", 4, 0, "a" * 64)
    assert backfill.classify_immutable_row(incoming, incoming) == "existing"

    changed_hash = backfill.SectorRow.osbap("037833100", 4, 0, "b" * 64)
    changed_sector = backfill.SectorRow.osbap("037833100", 8, 0, "a" * 64)
    assert backfill.classify_immutable_row(incoming, changed_hash) == "conflicted"
    assert backfill.classify_immutable_row(incoming, changed_sector) == "conflicted"


def test_sic_fallback_only_emits_exact_cusip9_with_a_resolved_canonical_sector() -> None:
    rows, reasons = backfill.sic_rows_from_exact_matches(
        [("459200101", 6021), ("037833", 6021), ("594918104", None), ("023135106", 2069)]
    )
    assert [(row.cusip9, row.ff17num, row.source) for row in rows] == [("459200101", 16, "sic_map")]
    assert reasons == {"invalid_cusip9": 1, "missing_sic": 1, "sic_not_in_ff17_definition": 1}


def test_summary_reports_each_source_and_honest_no_sector_count() -> None:
    summary = backfill.build_summary(
        artifact_sha256="a" * 64,
        attempted=5,
        inserted=2,
        existing=1,
        conflicted=0,
        skipped=2,
        source_coverage={"osbap": 1, "sic_map": 1},
        no_sector=3,
        reason_counts={"missing_sic": 2},
    )
    assert summary["artifact_sha256"] == "a" * 64
    assert summary["source_coverage"] == {"osbap": 1, "sic_map": 1, "no_sector": 3}
    assert summary["reason_counts"] == {"missing_sic": 2}


def test_conflicted_immutable_rows_fail_the_backfill_instead_of_becoming_a_partial_success() -> None:
    summary = backfill.build_summary(
        artifact_sha256="a" * 64, attempted=1, inserted=0, existing=0,
        conflicted=1, skipped=0, source_coverage={}, no_sector=0, reason_counts={},
    )
    with pytest.raises(backfill.BackfillConflictError):
        backfill.require_no_conflicts(summary)


def test_missing_required_panel_column_is_a_typed_failure(tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    pq.write_table(pa.table({"cusip_id": ["037833100"]}), panel)
    with pytest.raises(backfill.PanelArtifactError, match="missing_required_columns"):
        backfill.load_osbap_panel(panel)


def test_psql_emit_uses_a_canonical_bounded_sector_cursor_and_never_writes_progress_to_stdout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    panel = tmp_path / "panel.parquet"
    _write_panel(panel)

    assert backfill.main([
        "--panel", str(panel), "--emit-batch-psql", "--start-after", "1", "--max-rows", "1",
        "--expected-sha256", backfill._sha256(panel),
    ]) == 0

    emitted = capsys.readouterr()
    assert emitted.err == ""
    assert emitted.out.startswith("\\set ON_ERROR_STOP on\nBEGIN;\nSET LOCAL ROLE worker_writer;")
    assert "'committed_through', 2" in emitted.out
    assert "037833100" not in emitted.out
    assert "459200101" in emitted.out
    assert "ON CONFLICT" in emitted.out
    assert "IS DISTINCT FROM" in emitted.out


def test_psql_sector_emit_rejects_a_cursor_past_the_canonical_artifact(tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    _write_panel(panel)

    with pytest.raises(backfill.PanelArtifactError, match="start_after_exceeds_artifact_cursor"):
        backfill.emit_psql_batch(panel, start_after=99, max_rows=1)
