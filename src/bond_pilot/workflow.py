"""Manual, internal-only orchestration for isolated bond-pilot runs."""

from __future__ import annotations

import json
from pathlib import Path
import shutil
from typing import Mapping, Sequence
from uuid import uuid4

from src.db import connect, resolve_dsn

from .artifacts import write_checksums, write_json_once
from .contracts import PilotError
from .db_calibration import (
    load_phase4_v2_evidence,
    load_phase4_v2_evidence_approval,
    run_v2_calibration,
)
from .debt_mapping import load_approved_debt_mapping, load_fixture_debt_mapping
from .matching import ObservationIndex, compute_cross_series_summary, compute_series_metrics, match_holdings_asof
from .nport import load_fixture_result
from .panel import build_observed_panel
from .reporting import write_internal_reports
from .source_artifact import load_candidate, load_source_approval, qualify_source, verify_source_approval


def qualify(*, source: str | Path, run_dir: str | Path, expected_sha256: str | None = None) -> Mapping[str, object]:
    """Manually qualify a source artifact; it deliberately creates no approval."""
    candidate = qualify_source(source, Path(run_dir), expected_sha256=expected_sha256)
    return {"qualification": "unapproved", "source_manifest": str(Path(run_dir) / "source-manifest.json"), "artifact_sha256": candidate.artifact_sha256}


def _work_directory(run_dir: Path) -> Path:
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    work = run_dir.parent / f".{run_dir.name}.{uuid4().hex}.work"
    work.mkdir()
    return work


def _fixture_provenance(mapping: object) -> dict[str, object]:
    return {
        "schema_version": "mapping-provenance-v1",
        "mapping_version": mapping.mapping_version,
        "scope": "synthetic_fixture_only",
        "mapping_sha256": mapping.mapping_sha256,
        "approval_state": "synthetic_fixture_only",
    }


def run_fixture(*, source_manifest: str | Path, source_approval: str | Path, fixture: str | Path, mapping: str | Path, run_dir: str | Path) -> Mapping[str, object]:
    """Execute the complete offline fixture path and atomically publish internal evidence."""
    output = Path(run_dir)
    candidate = load_candidate(Path(source_manifest))
    approval = load_source_approval(Path(source_approval))
    verify_source_approval(candidate, approval)
    debt_mapping = load_fixture_debt_mapping(mapping)
    fixture_result = load_fixture_result(fixture)
    work = _work_directory(output)
    try:
        panel_path = work / "bond-observed-daily.parquet"
        panel_result = build_observed_panel(candidate.local_extracted_path, panel_path, (row.original_cusip for row in fixture_result.holdings))
        index_path = work / "observations.sqlite"
        with ObservationIndex.build(panel_path, index_path, (row.original_cusip for row in fixture_result.holdings)) as observations:
            matches = match_holdings_asof(fixture_result.holdings, debt_mapping, observations, candidate.global_start, candidate.global_cutoff)
            latest = observations.latest_rows()
        metrics = compute_series_metrics(matches)
        write_internal_reports(
            run_dir=output, source_candidate=candidate, source_approval=approval, debt_mapping=debt_mapping,
            mapping_provenance=_fixture_provenance(debt_mapping), nport_manifest=fixture_result.manifest(),
            panel_result=panel_result, panel_path=panel_path, matches=matches, series_metrics=metrics,
            cross_series_summary=compute_cross_series_summary(metrics), latest_observations=latest,
            calibration_report={"execution": "fixture"},
        )
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return {"calibration": "not_started", "phase4": "pre_backfill", "representative_post_backfill": False, "run_dir": str(output)}


def _calibration_inputs(*, source_manifest: str | Path, source_approval: str | Path, mapping: str | Path, mapping_approval: str | Path, evidence: str | Path, evidence_approval: str | Path, mode: str, series_ids: Sequence[str]) -> tuple[object, object, object, object, tuple[str, ...]]:
    """Validate every file-backed authority before a DSN is resolved or a connection opens."""
    candidate = load_candidate(Path(source_manifest))
    approval = load_source_approval(Path(source_approval))
    verify_source_approval(candidate, approval)
    debt_mapping = load_approved_debt_mapping(mapping, mapping_approval)
    phase4 = load_phase4_v2_evidence(evidence)
    phase4_approval = load_phase4_v2_evidence_approval(evidence_approval)
    series = tuple(series_ids)
    if mode not in {"calibration", "first_bounded"} or not series:
        raise PilotError("run_budget_required")
    return candidate, debt_mapping, phase4, phase4_approval, series


def run_calibration(*, source_manifest: str | Path, source_approval: str | Path, mapping: str | Path, mapping_approval: str | Path, evidence: str | Path, evidence_approval: str | Path, mode: str, series_ids: Sequence[str], run_dir: str | Path) -> Mapping[str, object]:
    """Run the governed V2 reader only after all source, mapping, and Phase 4 pins validate."""
    _candidate, _mapping, phase4, phase4_approval, series = _calibration_inputs(
        source_manifest=source_manifest, source_approval=source_approval, mapping=mapping,
        mapping_approval=mapping_approval, evidence=evidence, evidence_approval=evidence_approval,
        mode=mode, series_ids=series_ids,
    )
    output = Path(run_dir)
    work = _work_directory(output)
    try:
        checkpoint = work / "checkpoint.json"
        dsn = resolve_dsn()
        with connect(dsn) as connection:
            result = run_v2_calibration(connection, evidence=phase4, approval=phase4_approval, series_ids=series, mode=mode, checkpoint_path=checkpoint, run_id=output.name)
        payload = {"internal_only": True, "mode": mode, "rows_read": result.rows_read, "pages": result.pages, "partial": result.partial, "checkpoint": json.loads(checkpoint.read_text(encoding="utf-8"))}
        output.mkdir(parents=True, exist_ok=False)
        write_json_once(output / "calibration-report.json", payload)
        write_checksums(output)
        return payload
    finally:
        shutil.rmtree(work, ignore_errors=True)


def write_stop_report(run_dir: str | Path, error: PilotError) -> None:
    """Publish a minimal internal-only stop pack when the caller supplied a safe empty destination."""
    output = Path(run_dir)
    if output.exists():
        return
    try:
        output.mkdir(parents=True, exist_ok=False)
        write_json_once(output / "stop-report.json", {"internal_only": True, "status": "stopped", "code": error.code, "details": error.details})
        write_checksums(output)
    except (OSError, PilotError):
        shutil.rmtree(output, ignore_errors=True)
