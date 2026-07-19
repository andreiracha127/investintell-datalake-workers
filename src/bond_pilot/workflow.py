"""Manual, internal-only orchestration for isolated bond-pilot runs."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from typing import Mapping, Sequence
from uuid import uuid4

import psycopg
from src.db import connect, resolve_dsn

from .artifacts import write_checksums, write_json_once
from .contracts import PilotError
from .db_calibration import (
    load_phase4_v2_evidence,
    load_phase4_v2_evidence_approval,
    run_v2_calibration,
    validate_v2_request,
    encode_calibration_rows,
)
from .debt_mapping import load_approved_debt_mapping, load_fixture_debt_mapping
from .matching import ObservationIndex, compute_cross_series_summary, compute_series_metrics, match_holdings_asof
from .nport import load_fixture_result
from .panel import build_observed_panel
from .reporting import write_internal_reports
from .source_artifact import (
    _publish_directory_no_replace,
    load_candidate,
    load_source_approval,
    qualify_source,
    verify_source_approval,
)


_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _output_path(value: str | Path) -> Path:
    """Require a new destination outside the checkout, including after symlink resolution."""
    raw = Path(value)
    lexical = raw if raw.is_absolute() else Path.cwd() / raw
    resolved = lexical.resolve(strict=False)
    if _within(lexical.absolute(), _REPOSITORY_ROOT) or _within(resolved, _REPOSITORY_ROOT):
        raise PilotError("invalid_output_path")
    if os.path.lexists(lexical) or os.path.lexists(resolved):
        raise PilotError("already_exists", {"path": str(lexical)})
    return resolved


def _staging(output: Path, purpose: str) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(3):
        staging = output.parent / f".{output.name}.{purpose}-{uuid4().hex}.partial-dir"
        try:
            staging.mkdir()
        except FileExistsError:
            continue
        return staging
    raise PilotError("attempt_directory_collision", {"path": str(output)})


def _publish(staging: Path, output: Path) -> None:
    _publish_directory_no_replace(staging, output)


def qualify(*, source: str | Path, run_dir: str | Path, expected_sha256: str | None = None) -> Mapping[str, object]:
    """Manually qualify a source artifact; it deliberately creates no approval."""
    output = _output_path(run_dir)
    candidate = qualify_source(source, output, expected_sha256=expected_sha256)
    return {"qualification": "unapproved", "source_manifest": str(output / "source-manifest.json"), "artifact_sha256": candidate.artifact_sha256}


def _fixture_provenance(mapping: object) -> dict[str, object]:
    return {"schema_version": "mapping-provenance-v1", "mapping_version": mapping.mapping_version, "scope": "synthetic_fixture_only", "mapping_sha256": mapping.mapping_sha256, "approval_state": "synthetic_fixture_only"}


def run_fixture(*, source_manifest: str | Path, source_approval: str | Path, fixture: str | Path, mapping: str | Path, run_dir: str | Path) -> Mapping[str, object]:
    """Execute the complete offline fixture path and atomically publish internal evidence."""
    output = _output_path(run_dir)
    candidate = load_candidate(Path(source_manifest))
    approval = load_source_approval(Path(source_approval))
    verify_source_approval(candidate, approval)
    debt_mapping = load_fixture_debt_mapping(mapping)
    fixture_result = load_fixture_result(fixture)
    work = _staging(output, "fixture-work")
    try:
        panel_path = work / "bond-observed-daily.parquet"
        panel_result = build_observed_panel(candidate.local_extracted_path, panel_path, (row.original_cusip for row in fixture_result.holdings))
        index_path = work / "observations.sqlite"
        with ObservationIndex.build(panel_path, index_path, (row.original_cusip for row in fixture_result.holdings)) as observations:
            matches = match_holdings_asof(fixture_result.holdings, debt_mapping, observations, candidate.global_start, candidate.global_cutoff)
            latest = observations.latest_rows()
        metrics = compute_series_metrics(matches)
        write_internal_reports(run_dir=output, source_candidate=candidate, source_approval=approval, debt_mapping=debt_mapping, mapping_provenance=_fixture_provenance(debt_mapping), nport_manifest=fixture_result.manifest(), panel_result=panel_result, panel_path=panel_path, matches=matches, series_metrics=metrics, cross_series_summary=compute_cross_series_summary(metrics), latest_observations=latest, calibration_report={"execution": "fixture"})
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return {"calibration": "not_started", "phase4": "pre_backfill", "representative_post_backfill": False, "run_dir": str(output)}


def _calibration_inputs(*, source_manifest: str | Path, source_approval: str | Path, mapping: str | Path, mapping_approval: str | Path, evidence: str | Path, evidence_approval: str | Path, mode: str, series_ids: Sequence[str]) -> tuple[object, object, object, object, object, tuple[str, ...]]:
    """Validate every file-backed authority before a DSN is resolved or a connection opens."""
    candidate = load_candidate(Path(source_manifest))
    approval = load_source_approval(Path(source_approval))
    verify_source_approval(candidate, approval)
    debt_mapping = load_approved_debt_mapping(mapping, mapping_approval)
    phase4 = load_phase4_v2_evidence(evidence)
    phase4_approval = load_phase4_v2_evidence_approval(evidence_approval)
    series = validate_v2_request(phase4, phase4_approval, mode, series_ids)
    return candidate, approval, debt_mapping, phase4, phase4_approval, series


def _calibration_provenance(candidate: object, approval: object, mapping: object, evidence: object, evidence_approval: object, series: tuple[str, ...], mode: str) -> dict[str, object]:
    return {"internal_only": True, "source": {**candidate.to_json_mapping(), "approval": approval.to_json_mapping()}, "mapping": mapping.to_mapping(), "phase4": {"evidence_sha256": evidence.artifact_sha256, "approval_sha256": evidence_approval.artifact_sha256, "approval_authority_sha256": hashlib.sha256(str(evidence_approval.values["approved_by"]).encode("utf-8")).hexdigest(), "evidence": dict(evidence.values), "approval": dict(evidence_approval.values)}, "governed_request": {"mode": mode, "series_ids": list(series)}}


_CHECKPOINT_FIELDS = frozenset({"schema_version", "run_id", "evidence_sha256", "approval_sha256", "approval_authority_sha256", "publication_sha256", "mode", "series_ids", "seam", "relation", "query_version", "query_sha256", "method_version", "method_sha256", "resolved_reports", "last_key", "pages", "rows", "elapsed_seconds", "output_hash", "output_state", "stop_reason"})
_RESUME_FILES = frozenset({"checkpoint.json", "calibration-provenance.json", "stop-report.json", "checksums.sha256"})
_CHECKSUM_LINE = re.compile(r"^([0-9a-f]{64})  ([^\\/]+(?:/[^\\/]+)*)$")


def _checkpoint_metadata(checkpoint: Path) -> tuple[dict[str, object], str]:
    try:
        raw = checkpoint.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PilotError("calibration_checkpoint_invalid") from exc
    if not isinstance(payload, dict) or set(payload) != _CHECKPOINT_FIELDS or not isinstance(payload.get("output_hash"), str) or len(payload["output_hash"]) != 64 or any(character not in "0123456789abcdef" for character in payload["output_hash"]):
        raise PilotError("calibration_checkpoint_invalid")
    return payload, hashlib.sha256(raw).hexdigest()


def _cross_bind_checkpoint(payload: Mapping[str, object], provenance: Mapping[str, object], result: object, mode: str, series: tuple[str, ...]) -> None:
    phase4 = provenance["phase4"]
    expected = {
        "evidence_sha256": phase4["evidence_sha256"], "approval_sha256": phase4["approval_sha256"],
        "approval_authority_sha256": phase4["approval_authority_sha256"], "publication_sha256": phase4["evidence"]["publication_sha256"],
        "mode": mode, "series_ids": list(series), "seam": phase4["evidence"]["seam"], "relation": phase4["evidence"]["relation"],
        "pages": result.pages, "rows": result.rows_read, "last_key": list(result.last_key) if result.last_key else None,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise PilotError("calibration_checkpoint_mismatch")
    if payload["output_state"] != ("budget_reached" if result.partial else "complete"):
        raise PilotError("calibration_checkpoint_mismatch")


def _resume_input(path: str | Path, provenance: Mapping[str, object]) -> tuple[bytes, dict[str, object], str]:
    root = Path(path)
    resolved = root.resolve(strict=False)
    if not root.is_dir() or os.path.islink(root) or _within(resolved, _REPOSITORY_ROOT):
        raise PilotError("calibration_resume_invalid")
    files = {item.name for item in root.iterdir() if item.is_file() and not os.path.islink(item)}
    if files != _RESUME_FILES or any(item.is_dir() or os.path.islink(item) for item in root.iterdir()):
        raise PilotError("calibration_resume_invalid")
    manifest = (root / "checksums.sha256").read_text(encoding="utf-8")
    checks: dict[str, str] = {}
    for line in manifest.splitlines():
        match = _CHECKSUM_LINE.fullmatch(line)
        if match is None or match.group(2) in checks or match.group(2) == "checksums.sha256":
            raise PilotError("calibration_resume_invalid")
        checks[match.group(2)] = match.group(1)
    if set(checks) != _RESUME_FILES - {"checksums.sha256"}:
        raise PilotError("calibration_resume_invalid")
    for name, digest in checks.items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest:
            raise PilotError("calibration_resume_invalid")
    checkpoint_bytes = (root / "checkpoint.json").read_bytes()
    checkpoint, _digest = _checkpoint_metadata(root / "checkpoint.json")
    try:
        prior = json.loads((root / "calibration-provenance.json").read_text(encoding="utf-8"))
        stop = json.loads((root / "stop-report.json").read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, OSError) as exc:
        raise PilotError("calibration_resume_invalid") from exc
    if prior != provenance or not isinstance(stop, dict) or stop.get("status") != "stopped" or checkpoint.get("output_state") != "stopped":
        raise PilotError("calibration_resume_invalid")
    return checkpoint_bytes, checkpoint, hashlib.sha256(manifest.encode("utf-8")).hexdigest()


def _write_calibration_pack(staging: Path, *, candidate: object, approval: object, mapping: object, evidence: object, evidence_approval: object, series: tuple[str, ...], mode: str, result: object, checkpoint: Path, resume_digest: str | None = None) -> Mapping[str, object]:
    provenance = _calibration_provenance(candidate, approval, mapping, evidence, evidence_approval, series, mode)
    checkpoint_payload, checkpoint_sha256 = _checkpoint_metadata(checkpoint)
    _cross_bind_checkpoint(checkpoint_payload, provenance, result, mode, series)
    report: dict[str, object] = {"internal_only": True, "mode": mode, "rows_read": result.rows_read, "pages": result.pages, "partial": result.partial, "output_hash": checkpoint_payload["output_hash"], "checkpoint_sha256": checkpoint_sha256, "rows_artifact": "calibration-rows-v1.json", "rows_artifact_scope": "this_invocation", "invocation_rows": len(result.rows), "cumulative_rows": result.rows_read}
    if resume_digest is not None:
        report["resume_pack_checksums_sha256"] = resume_digest
    try:
        write_json_once(staging / "calibration-rows-v1.json", encode_calibration_rows(result.rows))
        write_json_once(staging / "calibration-provenance.json", provenance)
        write_json_once(staging / "calibration-report.json", report)
        write_checksums(staging)
    except PilotError as exc:
        if exc.code == "calibration_serialization_failed":
            raise PilotError("calibration_row_serialization_failed") from exc
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise PilotError("calibration_artifact_write_failed") from exc
    return report


def _write_calibration_stop(staging: Path, error: PilotError, provenance: Mapping[str, object]) -> None:
    write_json_once(staging / "calibration-provenance.json", dict(provenance))
    write_json_once(staging / "stop-report.json", {"internal_only": True, "status": "stopped", "code": error.code, "exception_class": error.__class__.__name__})
    write_checksums(staging)


def run_calibration(*, source_manifest: str | Path, source_approval: str | Path, mapping: str | Path, mapping_approval: str | Path, evidence: str | Path, evidence_approval: str | Path, mode: str, series_ids: Sequence[str], run_dir: str | Path, resume_pack: str | Path | None = None) -> Mapping[str, object]:
    """Run the governed V2 reader only after all source, mapping, and Phase 4 pins validate."""
    output = _output_path(run_dir)
    try:
        candidate, approval, debt_mapping, phase4, phase4_approval, series = _calibration_inputs(source_manifest=source_manifest, source_approval=source_approval, mapping=mapping, mapping_approval=mapping_approval, evidence=evidence, evidence_approval=evidence_approval, mode=mode, series_ids=series_ids)
    except PilotError as error:
        write_stop_report(output, error)
        raise
    staging = _staging(output, "calibration")
    provenance = _calibration_provenance(candidate, approval, debt_mapping, phase4, phase4_approval, series, mode)
    resume_bytes: bytes | None = None
    resume_checkpoint: dict[str, object] | None = None
    resume_digest: str | None = None
    if resume_pack is not None:
        try:
            resume_bytes, resume_checkpoint, resume_digest = _resume_input(resume_pack, provenance)
        except PilotError as error:
            shutil.rmtree(staging, ignore_errors=True)
            write_stop_report(output, error)
            raise
    published = False
    try:
        checkpoint = staging / "checkpoint.json"
        if resume_bytes is not None:
            checkpoint.write_bytes(resume_bytes)
        try:
            dsn = resolve_dsn()
        except (RuntimeError, OSError, psycopg.Error) as exc:
            raise PilotError("calibration_connection_failed") from exc
        try:
            connection = connect(dsn)
        except (RuntimeError, OSError, psycopg.Error) as exc:
            raise PilotError("calibration_connection_failed") from exc
        try:
            with connection:
                result = run_v2_calibration(connection, evidence=phase4, approval=phase4_approval, series_ids=series, mode=mode, checkpoint_path=checkpoint, run_id=(resume_checkpoint or {}).get("run_id", output.name))
        except PilotError:
            raise
        except psycopg.Error as exc:
            raise PilotError("calibration_database_failed") from exc
        report = _write_calibration_pack(staging, candidate=candidate, approval=approval, mapping=debt_mapping, evidence=phase4, evidence_approval=phase4_approval, series=series, mode=mode, result=result, checkpoint=checkpoint, resume_digest=resume_digest)
        _publish(staging, output)
        published = True
        return report
    except PilotError as error:
        if not published:
            try:
                _write_calibration_stop(staging, error, provenance)
                _publish(staging, output)
                published = True
            except (OSError, PilotError):
                pass
        raise
    finally:
        if not published:
            shutil.rmtree(staging, ignore_errors=True)


def write_stop_report(run_dir: str | Path, error: PilotError) -> None:
    """Atomically publish a generic internal stop pack without touching an existing final."""
    try:
        output = _output_path(run_dir)
        staging = _staging(output, "stop")
    except PilotError:
        return
    published = False
    try:
        write_json_once(staging / "stop-report.json", {"internal_only": True, "status": "stopped", "code": error.code})
        write_checksums(staging)
        _publish(staging, output)
        published = True
    except (OSError, PilotError):
        return
    finally:
        if not published:
            shutil.rmtree(staging, ignore_errors=True)
