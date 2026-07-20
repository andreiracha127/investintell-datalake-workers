"""Manual, internal-only orchestration for isolated bond-pilot runs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
from typing import Mapping, Sequence
from contextlib import contextmanager

import psycopg
from src.db import connect, resolve_dsn

from .artifacts import canonical_json_bytes, validated_local_path
from ._secure_local_fs import secure_open_dir
from .output_pack import OutputPack, open_validated_output_pack, validate_output_pack
from .contracts import PilotError
from .db_calibration import (
    load_phase4_v2_evidence,
    load_phase4_v2_evidence_approval,
    run_v2_calibration,
    load_latest_calibration_checkpoint,
    validate_calibration_checkpoint_chain,
    validate_v2_request,
    encode_calibration_rows,
)
from .debt_mapping import load_approved_debt_mapping, load_fixture_debt_mapping
from .matching import ObservationIndex, compute_cross_series_summary, compute_series_metrics, match_holdings_asof
from .nport import load_fixture_result
from .panel import build_observed_panel
from .reporting import write_internal_reports
from .source_artifact import (
    load_candidate,
    load_candidate_file,
    load_source_approval,
    qualify_source,
    verify_source_approval,
)


_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_SOURCE_PAYLOADS = ("qualification-report.json", "source-manifest.json", "source.parquet", "source.zip")
_FIXTURE_PAYLOADS = (
    "bond-latest.parquet",
    "bond-observed-daily.parquet",
    "calibration-report.json",
    "fund-asof-match.parquet",
    "fund-series-metrics.parquet",
    "nport-extract-manifest.json",
    "pilot-report.md",
    "quality-summary.json",
    "source-manifest.json",
)
_STOP_PAYLOADS = ("stop-report.json",)
_CALIBRATION_SUCCESS_STATIC = ("calibration-provenance.json", "calibration-report.json", "calibration-rows-v1.json")
_CALIBRATION_STOP_STATIC = ("calibration-provenance.json", "stop-report.json")
_CHECKPOINT_NAME = re.compile(r"^checkpoint-\d{12}\.json$")


def _within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _output_path(value: str | Path) -> Path:
    """Return only an output label; retained directory capabilities authorize creation."""
    lexical = validated_local_path(value, error_code="invalid_output_path")
    if _within(lexical, _REPOSITORY_ROOT):
        raise PilotError("invalid_output_path")
    return lexical


@contextmanager
def _output_pack(value: str | Path, *, schema: str) -> object:
    output = _output_path(value)
    with secure_open_dir(output.parent, error_code="unsafe_parent") as parent:
        pack = OutputPack.create(parent, run_id=output.name, pack_schema_version=schema, producer_version="bond-pilot")
        try:
            yield output, parent, pack
        finally:
            pack.close()


def qualify(*, source: str | Path, run_dir: str | Path, expected_sha256: str | None = None) -> Mapping[str, object]:
    """Manually qualify a source artifact; it deliberately creates no approval."""
    with _output_pack(run_dir, schema="bond-pilot-source-v1") as (output, parent, pack):
        candidate = qualify_source(source, pack, expected_sha256=expected_sha256)
        pack.finalize()
        validate_output_pack(parent, output.name, expected_pack_schema_version="bond-pilot-source-v1", expected_payloads=_SOURCE_PAYLOADS)
    return {"qualification": "unapproved", "source_manifest": str(output / "source-manifest.json"), "artifact_sha256": candidate.artifact_sha256}


def _fixture_provenance(mapping: object) -> dict[str, object]:
    return {"schema_version": "mapping-provenance-v2", "mapping_contract": "composite-exact-v2", "mapping_version": mapping.mapping_version, "scope": "synthetic_fixture_only", "mapping_sha256": mapping.mapping_sha256, "approval_state": "synthetic_fixture_only", "approval_reference": "synthetic_fixture_only"}


def run_fixture(*, source_manifest: str | Path, source_approval: str | Path, fixture: str | Path, mapping: str | Path, run_dir: str | Path) -> Mapping[str, object]:
    """Execute the offline fixture path using only retained input/output capabilities."""
    approval = load_source_approval(source_approval)
    debt_mapping = load_fixture_debt_mapping(mapping)
    fixture_result = load_fixture_result(fixture)
    manifest_label = validated_local_path(source_manifest, error_code="source_integrity_failed")
    source_run = manifest_label.parent.name
    if not source_run:
        raise PilotError("source_integrity_failed")
    with secure_open_dir(manifest_label.parent.parent, error_code="source_integrity_failed") as source_parent:
        with open_validated_output_pack(
            source_parent,
            source_run,
            expected_pack_schema_version="bond-pilot-source-v1",
            expected_payloads=_SOURCE_PAYLOADS,
        ) as (source_pack, _completed):
            with source_pack.open_file("source-manifest.json", error_code="source_integrity_failed") as source_manifest_file:
                candidate = load_candidate_file(source_manifest_file)
            if manifest_label != source_pack.path / "source-manifest.json" or candidate.local_archive_path != str(source_pack.path / "source.zip") or candidate.local_extracted_path != str(source_pack.path / "source.parquet"):
                raise PilotError("source_integrity_failed")
            verify_source_approval(candidate, approval)
            with source_pack.open_file("source.parquet", error_code="source_integrity_failed") as source_parquet:
                with _output_pack(run_dir, schema="bond-pilot-fixture-v1") as (output, output_parent, pack):
                    with pack.create_payload("bond-observed-daily.parquet") as panel_output:
                        panel_result = build_observed_panel(source_parquet, panel_output, (row.original_cusip for row in fixture_result.holdings))
                    with pack.directory.open_file("bond-observed-daily.parquet", error_code="source_integrity_failed") as panel_input:
                        with ObservationIndex.build(panel_input, None, (row.original_cusip for row in fixture_result.holdings)) as observations:
                            matches = match_holdings_asof(fixture_result.holdings, debt_mapping, observations, candidate.global_start, candidate.global_cutoff)
                            latest = observations.latest_rows()
                    metrics = compute_series_metrics(matches, debt_mapping)
                    write_internal_reports(run_dir=pack, source_candidate=candidate, source_approval=approval, debt_mapping=debt_mapping, mapping_provenance=_fixture_provenance(debt_mapping), nport_manifest=fixture_result.manifest(), panel_result=panel_result, panel_path="bond-observed-daily.parquet", matches=matches, series_metrics=metrics, cross_series_summary=compute_cross_series_summary(metrics), latest_observations=latest, calibration_report={"execution": "fixture"})
                    validate_output_pack(output_parent, output.name, expected_pack_schema_version="bond-pilot-fixture-v1", expected_payloads=_FIXTURE_PAYLOADS)
    return {"calibration": "not_started", "phase4": "pre_backfill", "representative_post_backfill": False, "run_dir": str(_output_path(run_dir))}


def _calibration_inputs(*, source_manifest: str | Path, source_approval: str | Path, mapping: str | Path, mapping_approval: str | Path, evidence: str | Path, evidence_approval: str | Path, mode: str, series_ids: Sequence[str]) -> tuple[object, object, object, object, object, tuple[str, ...]]:
    """Validate every file-backed authority before a DSN is resolved or a connection opens."""
    candidate = load_candidate(source_manifest)
    approval = load_source_approval(source_approval)
    verify_source_approval(candidate, approval)
    debt_mapping = load_approved_debt_mapping(mapping, mapping_approval)
    phase4 = load_phase4_v2_evidence(evidence)
    phase4_approval = load_phase4_v2_evidence_approval(evidence_approval)
    if phase4.values["mapping_artifact_sha256"] != debt_mapping.mapping_sha256 or phase4.values["mapping_approval_sha256"] != debt_mapping.approval_sha256:
        raise PilotError("phase4b_v2_unavailable")
    series = validate_v2_request(phase4, phase4_approval, mode, series_ids)
    return candidate, approval, debt_mapping, phase4, phase4_approval, series


def _calibration_provenance(candidate: object, approval: object, mapping: object, evidence: object, evidence_approval: object, series: tuple[str, ...], mode: str) -> dict[str, object]:
    mapping_evidence = {"schema_version": mapping.schema_version, "mapping_version": mapping.mapping_version, "scope": mapping.scope, "mapping_sha256": mapping.mapping_sha256, "observed_composite_values_sha256": mapping.observed_composite_values_sha256, "approval_reference": mapping.approval_sha256, "mapping_contract": "composite-exact-v2", "classification_fields": ["issuer_category", "asset_class", "instrument_structure"]}
    return {"internal_only": True, "source": {**candidate.to_json_mapping(), "approval": approval.to_json_mapping()}, "mapping": mapping_evidence, "phase4": {"evidence_sha256": evidence.artifact_sha256, "approval_sha256": evidence_approval.artifact_sha256, "approval_authority_sha256": hashlib.sha256(str(evidence_approval.values["approved_by"]).encode("utf-8")).hexdigest(), "evidence": dict(evidence.values), "approval": dict(evidence_approval.values)}, "governed_request": {"mode": mode, "series_ids": list(series)}}


_MAX_RESUME_CONTROL_BYTES = 1024 * 1024


def _strict_captured_json(raw: bytes) -> dict[str, object]:
    def duplicate(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    def nonfinite(value: str) -> object:
        raise ValueError(f"non-finite {value}")

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=duplicate, parse_constant=nonfinite)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise PilotError("calibration_resume_invalid") from exc
    if not isinstance(value, dict):
        raise PilotError("calibration_resume_invalid")
    return value


def _cross_bind_checkpoint(payload: Mapping[str, object], provenance: Mapping[str, object], result: object, mode: str, series: tuple[str, ...]) -> None:
    if result.mode != mode:
        raise PilotError("calibration_checkpoint_mismatch")
    phase4 = provenance["phase4"]
    expected = {
        "evidence_sha256": phase4["evidence_sha256"], "approval_sha256": phase4["approval_sha256"],
        "approval_authority_sha256": phase4["approval_authority_sha256"], "publication_sha256": phase4["evidence"]["publication_sha256"],
        "mode": mode, "series_ids": list(series), "seam": phase4["evidence"]["seam"], "relation": phase4["evidence"]["relation"],
        "pages": result.pages, "rows": result.rows_read, "last_key": tuple(result.last_key) if result.last_key else None,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise PilotError("calibration_checkpoint_mismatch")
    if payload["output_state"] != ("budget_reached" if result.partial else "complete"):
        raise PilotError("calibration_checkpoint_mismatch")


def _dynamic_calibration_payloads(files: tuple[str, ...], static: tuple[str, ...], *, error_code: str) -> tuple[str, ...]:
    payloads = tuple(name for name in files if name not in {"checksums.sha256", "completion.json"})
    checkpoints = tuple(name for name in payloads if _CHECKPOINT_NAME.fullmatch(name))
    if not checkpoints or set(payloads) != set((*static, *checkpoints)):
        raise PilotError(error_code)
    return tuple(sorted(payloads))


def _resume_input(path: str | Path, provenance: Mapping[str, object], evidence: object, approval: object, mode: str, series: tuple[str, ...]) -> tuple[bytes, dict[str, object], str]:
    """Read only a completion-sealed prior pack; never reopen its persisted labels."""
    root = validated_local_path(path, error_code="calibration_resume_invalid")
    if _within(root, _REPOSITORY_ROOT) or not root.name:
        raise PilotError("calibration_resume_invalid")
    try:
        with secure_open_dir(root.parent, error_code="calibration_resume_invalid") as parent:
            with open_validated_output_pack(
                parent,
                root.name,
                expected_pack_schema_version="bond-pilot-calibration-v1",
            ) as (directory, completed):
                payloads = _dynamic_calibration_payloads(completed.files, _CALIBRATION_STOP_STATIC, error_code="calibration_resume_invalid")
                if set(directory.enumerate()) != set((*payloads, "checksums.sha256", "completion.json")):
                    raise PilotError("calibration_resume_invalid")
                chain = validate_calibration_checkpoint_chain(
                    directory,
                    evidence=evidence,
                    approval=approval,
                    mode=mode,
                    series_ids=series,
                    error_code="calibration_resume_invalid",
                )
                checkpoint_bytes, checkpoint = chain[-1]
                for name in ("calibration-provenance.json", "stop-report.json"):
                    with directory.open_file(name, error_code="calibration_resume_invalid") as child:
                        if name == "calibration-provenance.json":
                            prior = _strict_captured_json(child.read_all(max_bytes=_MAX_RESUME_CONTROL_BYTES))
                        else:
                            stop = _strict_captured_json(child.read_all(max_bytes=_MAX_RESUME_CONTROL_BYTES))
    except PilotError as exc:
        raise PilotError("calibration_resume_invalid") from exc
    if canonical_json_bytes(prior) != canonical_json_bytes(provenance) or set(stop) != {"internal_only", "status", "code", "exception_class"} or stop["internal_only"] is not True or stop["status"] != "stopped" or stop["exception_class"] != "PilotError" or not isinstance(stop["code"], str) or stop["code"] != checkpoint.get("stop_reason") or checkpoint.get("output_state") != "stopped":
        raise PilotError("calibration_resume_invalid")
    return checkpoint_bytes, checkpoint, str(completed.completion["checksums_sha256"])


def _write_calibration_pack(pack: OutputPack, *, candidate: object, approval: object, mapping: object, evidence: object, evidence_approval: object, series: tuple[str, ...], mode: str, result: object, expected_run_id: str, resume_digest: str | None = None) -> Mapping[str, object]:
    provenance = _calibration_provenance(candidate, approval, mapping, evidence, evidence_approval, series, mode)
    checkpoint_raw, checkpoint_payload = load_latest_calibration_checkpoint(pack, evidence=evidence, approval=evidence_approval, mode=mode, series_ids=series, run_id=expected_run_id)
    checkpoint_sha256 = hashlib.sha256(checkpoint_raw).hexdigest()
    _cross_bind_checkpoint(checkpoint_payload, provenance, result, mode, series)
    report: dict[str, object] = {"internal_only": True, "mode": mode, "rows_read": result.rows_read, "pages": result.pages, "partial": result.partial, "output_hash": checkpoint_payload["output_hash"], "checkpoint_sha256": checkpoint_sha256, "rows_artifact": "calibration-rows-v1.json", "rows_artifact_scope": "this_invocation", "invocation_rows": len(result.rows), "cumulative_rows": result.rows_read}
    if resume_digest is not None:
        report["resume_pack_checksums_sha256"] = resume_digest
    try:
        pack.write_payload("calibration-rows-v1.json", canonical_json_bytes(encode_calibration_rows(result.rows)))
        pack.write_payload("calibration-provenance.json", canonical_json_bytes(provenance))
        pack.write_payload("calibration-report.json", canonical_json_bytes(report))
    except PilotError as exc:
        if exc.code == "calibration_serialization_failed":
            raise PilotError("calibration_row_serialization_failed") from exc
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise PilotError("calibration_artifact_write_failed") from exc
    return report


def _write_calibration_stop(pack: OutputPack, error: PilotError, provenance: Mapping[str, object]) -> None:
    pack.write_payload("calibration-provenance.json", canonical_json_bytes(dict(provenance)))
    pack.write_payload("stop-report.json", canonical_json_bytes({"internal_only": True, "status": "stopped", "code": error.code, "exception_class": error.__class__.__name__}))


def run_calibration(*, source_manifest: str | Path, source_approval: str | Path, mapping: str | Path, mapping_approval: str | Path, evidence: str | Path, evidence_approval: str | Path, mode: str, series_ids: Sequence[str], run_dir: str | Path, resume_pack: str | Path | None = None) -> Mapping[str, object]:
    """Run the governed V2 reader into one sealed capability-created output pack."""
    try:
        candidate, approval, debt_mapping, phase4, phase4_approval, series = _calibration_inputs(source_manifest=source_manifest, source_approval=source_approval, mapping=mapping, mapping_approval=mapping_approval, evidence=evidence, evidence_approval=evidence_approval, mode=mode, series_ids=series_ids)
    except PilotError as error:
        write_stop_report(run_dir, error)
        raise
    provenance = _calibration_provenance(candidate, approval, debt_mapping, phase4, phase4_approval, series, mode)
    resume_bytes: bytes | None = None
    resume_checkpoint: dict[str, object] | None = None
    resume_digest: str | None = None
    if resume_pack is not None:
        try:
            resume_bytes, resume_checkpoint, resume_digest = _resume_input(resume_pack, provenance, phase4, phase4_approval, mode, series)
        except PilotError as error:
            write_stop_report(run_dir, error)
            raise
    with _output_pack(run_dir, schema="bond-pilot-calibration-v1") as (output, parent, pack):
        expected_run_id = str(resume_checkpoint["run_id"]) if resume_checkpoint is not None else output.name
        try:
            if resume_bytes is not None and int((resume_checkpoint or {}).get("rows", 0)) > 0:
                resumed_seed = dict(resume_checkpoint or {})
                resumed_seed["output_state"] = "in_progress"
                resumed_seed["stop_reason"] = None
                pack.write_payload("checkpoint-000000000001.json", canonical_json_bytes(resumed_seed))
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
                    initial_checkpoint = resume_checkpoint if resume_bytes is not None and int((resume_checkpoint or {}).get("rows", 0)) == 0 else None
                    result = run_v2_calibration(connection, evidence=phase4, approval=phase4_approval, series_ids=series, mode=mode, checkpoint_pack=pack, run_id=expected_run_id, initial_checkpoint=initial_checkpoint)
            except PilotError:
                raise
            except psycopg.Error as exc:
                raise PilotError("calibration_database_failed") from exc
            report = _write_calibration_pack(pack, candidate=candidate, approval=approval, mapping=debt_mapping, evidence=phase4, evidence_approval=phase4_approval, series=series, mode=mode, result=result, expected_run_id=expected_run_id, resume_digest=resume_digest)
            completed = pack.finalize()
            payloads = _dynamic_calibration_payloads(completed.files, _CALIBRATION_SUCCESS_STATIC, error_code="incomplete_output")
            validate_output_pack(parent, output.name, expected_pack_schema_version="bond-pilot-calibration-v1", expected_payloads=payloads)
            return report
        except PilotError as error:
            if pack.state == "BUILDING":
                try:
                    _write_calibration_stop(pack, error, provenance)
                    completed = pack.finalize()
                    payloads = _dynamic_calibration_payloads(completed.files, _CALIBRATION_STOP_STATIC, error_code="incomplete_output")
                    validate_output_pack(parent, output.name, expected_pack_schema_version="bond-pilot-calibration-v1", expected_payloads=payloads)
                except PilotError:
                    pass
            raise


def write_stop_report(run_dir: str | Path, error: PilotError) -> None:
    """Publish a sealed internal stop pack without reopening or replacing output labels."""
    try:
        with _output_pack(run_dir, schema="bond-pilot-stop-v1") as (output, parent, pack):
            pack.write_payload("stop-report.json", canonical_json_bytes({"internal_only": True, "status": "stopped", "code": error.code}))
            pack.finalize()
            validate_output_pack(parent, output.name, expected_pack_schema_version="bond-pilot-stop-v1", expected_payloads=_STOP_PAYLOADS)
    except (OSError, PilotError):
        return
