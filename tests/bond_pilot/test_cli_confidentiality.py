from __future__ import annotations

import ast
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
from types import SimpleNamespace

import pytest

from scripts.run_bond_pilot import build_parser, main
from src.bond_pilot.contracts import PilotError, SourceApproval
from src.bond_pilot.db_calibration import REQUIRED_COLUMNS
from src.bond_pilot import artifacts, source_artifact
from src.bond_pilot._secure_local_fs import secure_open_dir
from src.bond_pilot.output_pack import OutputPack
from src.bond_pilot.source_artifact import load_candidate, qualify_source
from src.bond_pilot import workflow
from src.bond_pilot.workflow import qualify, run_calibration, run_fixture


_FIXTURE_MAPPING = Path(__file__).parent / "fixtures" / "debt-mapping-test-v2.json"


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, *_: object) -> None:
        return None


def _checkpoint(values: dict[str, object], output_hash: str = "f" * 64, *, state: str = "complete", reason: str | None = None, run_id: str = "calibration", pages: int | None = None, rows_read: int | None = None, reports: tuple[tuple[str, str, str, str], ...] | None = None, last_key: tuple[str, ...] | None = None) -> bytes:
    from src.bond_pilot import db_calibration as calibration

    return calibration.canonical_json_bytes(calibration._checkpoint_payload(
        run_id=run_id, evidence=values["evidence"], approval=values["approval"], mode=values["mode"],
        series_ids=values["series_ids"], reports=(reports if reports is not None else ([(values["series_ids"][0], "2024-03-31", "pub-1", "acc-1")] if state == "complete" else ())), last_key=(last_key if last_key is not None else ((values["series_ids"][0], "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1") if state == "complete" and values.get("rows") else None)), pages=(0 if state == "stopped" and pages is None else pages or 1), rows=(len(values.get("rows", ())) if rows_read is None else rows_read),
        elapsed_seconds=1.0, output_hash=output_hash, output_state=state, stop_reason=reason,
    ))


def _write_mock_checkpoint(values: dict[str, object], *, rows: tuple[dict[str, object], ...] = (), output_hash: str = "f" * 64, state: str = "complete", reason: str | None = None, pages: int | None = None, rows_read: int | None = None, reports: tuple[tuple[str, str, str, str], ...] | None = None, last_key: tuple[str, ...] | None = None) -> bytes:
    """Test adapter: mocks write immutable checkpoints through the supplied capability."""
    pack = values["checkpoint_pack"]
    assert isinstance(pack, OutputPack)
    names = [name for name in pack.directory.enumerate() if re.fullmatch(r"checkpoint-\d{12}\.json", name)]
    raw = _checkpoint({**values, "rows": rows}, output_hash=output_hash, state=state, reason=reason, run_id=str(values["run_id"]), pages=pages, rows_read=rows_read, reports=reports, last_key=last_key)
    pack.write_payload(f"checkpoint-{len(names) + 1:012d}.json", raw)
    return raw


def _calibration_row(**overrides: object) -> dict[str, object]:
    row = {column: f"{column}-value" for column in REQUIRED_COLUMNS}
    row.update({"report_date": date(2024, 3, 31), "filing_date": date(2024, 4, 15), "signed_market_value": Decimal("10.250"), "signed_pct_of_nav": 0.1})
    row.update(overrides)
    return row


def _approval_for(candidate) -> SourceApproval:
    return SourceApproval(
        "source-approval-v1", candidate.source_locator, candidate.artifact_sha256,
        candidate.schema_sha256, candidate.global_cutoff, "internal fixture terms",
        True, False, "fixture reviewer", "2026-07-19T12:00:00Z",
    )


def _fixture(path: Path) -> Path:
    path.write_text(json.dumps({"schema_version": "nport-fixture-v1", "phase4_state": "pre_backfill", "holdings": [{
        "publication_id": "publication-1", "accession_number": "0000000000-24-000001", "holding_id": "lot-1",
        "source_run_id": "source-run-1", "report_date": "2024-03-31", "filing_date": "2024-04-15",
        "series_id": "series-1", "class_id": None, "instrument_id": "instrument-1", "issuer_category": "fixture_debt", "asset_class": "fixture_asset", "instrument_structure": "fixture_structure",
        "cusip": "123456789", "signed_market_value": 100.0, "signed_pct_of_nav": 0.1, "currency": "USD",
    }]}), encoding="utf-8")
    return path


def _qualified_source(tmp_path: Path, make_source_zip) -> tuple[Path, Path, Path]:
    archive = make_source_zip()
    qualified_dir = tmp_path / "qualified"
    qualify(source=archive, run_dir=qualified_dir)
    candidate = load_candidate(qualified_dir / "source-manifest.json")
    approval = _approval_for(candidate)
    approval_path = tmp_path / "source-approval.json"
    approval_path.write_text(json.dumps(approval.to_json_mapping()), encoding="utf-8")
    return qualified_dir / "source-manifest.json", approval_path, archive


def _calibration_documents(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    observed = "c" * 64
    mapping = tmp_path / "mapping.json"
    mapping.write_text(json.dumps({"schema_version": "debt-mapping-v2", "mapping_version": "real-v2", "observed_composite_values_sha256": observed, "rules": [{"issuer_category": "debt", "asset_class": "asset", "instrument_structure": "structure", "decision": "eligible_debt"}]}), encoding="utf-8")
    mapping_approval = tmp_path / "mapping-approval.json"
    mapping_approval.write_text(json.dumps({"schema_version": "debt-mapping-approval-v2", "mapping_sha256": hashlib.sha256(mapping.read_bytes()).hexdigest(), "observed_composite_values_sha256": observed, "evidence": [{"reference": "internal evidence", "sha256": "d" * 64}], "approved_by": "reviewer", "approved_at": "2026-07-19T12:00:00Z"}), encoding="utf-8")
    hash_fields = {field: "a" * 64 for field in ("phase4_run_sha256", "reconciliation_sha256", "publication_sha256", "schema_sha256", "lineage_attestation_sha256")}
    evidence = tmp_path / "evidence.json"
    mapping_pins = {"mapping_contract": "composite-exact-v2", "mapping_schema_version": "debt-mapping-v2", "mapping_artifact_sha256": hashlib.sha256(mapping.read_bytes()).hexdigest(), "mapping_approval_sha256": hashlib.sha256(mapping_approval.read_bytes()).hexdigest()}
    evidence.write_text(json.dumps({"schema_version": "phase4b-v2-evidence-v2", "phase4_status": "completed", "reconciled": True, "v2_published": True, "seam": "nport-v2-current", "relation": "public.sec_nport_holdings_v2_current", "required_columns": list(REQUIRED_COLUMNS), **hash_fields, **mapping_pins, "approved_series": ["series-1"], "approved_by": "phase4 reviewer", "approved_at": "2026-07-19T12:00:00Z"}), encoding="utf-8")
    evidence_approval = tmp_path / "evidence-approval.json"
    evidence_approval.write_text(json.dumps({"schema_version": "phase4b-v2-evidence-approval-v2", "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(), **hash_fields, **mapping_pins, "seam": "nport-v2-current", "relation": "public.sec_nport_holdings_v2_current", "approved_series": ["series-1"], "approved_modes": ["calibration"], "allow_read": True, "approved_by": "phase4 approver", "approved_at": "2026-07-19T12:00:00Z"}), encoding="utf-8")
    monkeypatch.setenv("BOND_PILOT_PHASE4_V2_APPROVAL_SHA256", hashlib.sha256(evidence_approval.read_bytes()).hexdigest())
    monkeypatch.setenv("BOND_PILOT_PHASE4_V2_APPROVER_ID", "phase4 approver")
    return {"source_manifest": source_manifest, "source_approval": source_approval, "mapping": mapping, "mapping_approval": mapping_approval, "evidence": evidence, "evidence_approval": evidence_approval}


def _write_stopped_resume_pack(path: Path, *, provenance: object, evidence: object, approval: object, series: tuple[str, ...], checkpoint_raw: bytes | None = None, provenance_raw: bytes | None = None, stop_raw: bytes | None = None) -> None:
    values = {"evidence": evidence, "approval": approval, "mode": "calibration", "series_ids": series, "rows": ()}
    with secure_open_dir(path.parent, error_code="unsafe_parent") as parent:
        pack = OutputPack.create(parent, run_id=path.name, pack_schema_version="bond-pilot-calibration-v1", producer_version="test")
        try:
            pack.write_payload("checkpoint-000000000001.json", checkpoint_raw or _checkpoint(values, output_hash=hashlib.sha256(b"").hexdigest(), state="stopped", reason="unsafe_query_plan", run_id="original-run"))
            pack.write_payload("calibration-provenance.json", provenance_raw or json.dumps(provenance).encode("utf-8"))
            pack.write_payload("stop-report.json", stop_raw or json.dumps({"internal_only": True, "status": "stopped", "code": "unsafe_query_plan", "exception_class": "PilotError"}).encode("utf-8"))
            pack.finalize()
        finally:
            pack.close()


def _write_resume_pack_with_checkpoints(path: Path, *, provenance: object, checkpoints: tuple[tuple[str, bytes], ...], stop_reason: str = "unsafe_query_plan") -> None:
    with secure_open_dir(path.parent, error_code="unsafe_parent") as parent:
        pack = OutputPack.create(parent, run_id=path.name, pack_schema_version="bond-pilot-calibration-v1", producer_version="test")
        try:
            for name, raw in checkpoints:
                pack.write_payload(name, raw)
            pack.write_payload("calibration-provenance.json", json.dumps(provenance).encode("utf-8"))
            pack.write_payload("stop-report.json", json.dumps({"internal_only": True, "status": "stopped", "code": stop_reason, "exception_class": "PilotError"}).encode("utf-8"))
            pack.finalize()
        finally:
            pack.close()


def test_parser_exposes_exact_manual_commands_and_never_accepts_relation() -> None:
    parser = build_parser()
    commands = parser._subparsers._group_actions[0].choices  # type: ignore[attr-defined]
    assert set(commands) == {"qualify", "fixture-run", "calibrate"}
    for command in commands.values():
        assert "--relation" not in command.format_help()
    with pytest.raises(SystemExit):
        parser.parse_args(["calibrate", "--relation", "anything"])


def test_direct_script_help_works_for_each_manual_command() -> None:
    root = Path(__file__).resolve().parents[2]
    for command in ("qualify", "fixture-run", "calibrate"):
        completed = subprocess.run(["python", "scripts/run_bond_pilot.py", command, "--help"], cwd=root, capture_output=True, text=True)
        assert completed.returncode == 0
        assert "--relation" not in completed.stdout


def test_fixture_run_executes_offline_path_and_publishes_internal_evidence(tmp_path: Path, make_source_zip) -> None:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    fixture = _fixture(tmp_path / "fixture.json")
    mapping = _FIXTURE_MAPPING
    outcome = run_fixture(
        source_manifest=source_manifest, source_approval=source_approval, fixture=fixture,
        mapping=mapping, run_dir=tmp_path / "fixture-run",
    )
    assert outcome["calibration"] == "not_started"
    assert outcome["phase4"] == "pre_backfill"
    assert outcome["representative_post_backfill"] is False
    run_dir = tmp_path / "fixture-run"
    quality = json.loads((run_dir / "quality-summary.json").read_text(encoding="utf-8"))
    assert quality["internal_only"] is True
    assert quality["source"]["source_locator"] == str(_)
    assert (run_dir / "checksums.sha256").is_file()
    assert not list(tmp_path.glob("*.sqlite*"))
    assert not list(tmp_path.glob(".fixture-run.fixture-work-*.partial-dir"))


@pytest.mark.parametrize("artifact", ["archive", "extracted"])
def test_fixture_run_rejects_replaced_approved_source_before_panel_or_report(artifact: str, tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    candidate = load_candidate(source_manifest)
    Path(candidate.local_archive_path if artifact == "archive" else candidate.local_extracted_path).write_bytes(b"replaced")
    panel_calls: list[Path] = []
    monkeypatch.setattr(workflow, "build_observed_panel", lambda source, *_args: panel_calls.append(Path(source)))
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: pytest.fail("fixture run must not resolve a DSN"))
    monkeypatch.setattr(workflow, "connect", lambda _dsn: pytest.fail("fixture run must not connect"))

    with pytest.raises(PilotError, match="^incomplete_output$") as error:
        run_fixture(
            source_manifest=source_manifest,
            source_approval=source_approval,
            fixture=_fixture(tmp_path / "fixture.json"),
            mapping=_FIXTURE_MAPPING,
            run_dir=tmp_path / "fixture-run",
        )

    assert error.value.details == {}
    assert panel_calls == []
    assert not (tmp_path / "fixture-run" / "completion.json").exists()
    assert not list(tmp_path.glob(".fixture-run.fixture-work-*.partial-dir"))


def test_fixture_run_consumes_only_requalified_staging_parquet_and_retains_approved_provenance(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    candidate = load_candidate(source_manifest)
    real_panel = workflow.build_observed_panel
    real_reports = workflow.write_internal_reports
    panel_sources: list[object] = []
    report_sources: list[object] = []

    def capture_panel(source: object, *args: object):
        panel_sources.append(source)
        return real_panel(source, *args)

    def capture_reports(*args: object, **kwargs: object):
        report_sources.append(kwargs["source_candidate"])
        return real_reports(*args, **kwargs)

    monkeypatch.setattr(workflow, "build_observed_panel", capture_panel)
    monkeypatch.setattr(workflow, "write_internal_reports", capture_reports)
    run_fixture(
        source_manifest=source_manifest,
        source_approval=source_approval,
        fixture=_fixture(tmp_path / "fixture.json"),
        mapping=_FIXTURE_MAPPING,
        run_dir=tmp_path / "fixture-run",
    )

    assert panel_sources and panel_sources == [panel_sources[0]]
    assert hasattr(panel_sources[0], "read")
    assert panel_sources[0] != candidate.local_extracted_path
    assert report_sources == [candidate]
    assert not list(tmp_path.glob(".fixture-run.fixture-work-*.partial-dir"))


def test_fixture_run_leaves_interrupted_pack_incomplete_after_panel_failure(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    monkeypatch.setattr(workflow, "build_observed_panel", lambda *_args: (_ for _ in ()).throw(PilotError("panel_failed")))

    with pytest.raises(PilotError, match="^panel_failed$"):
        run_fixture(
            source_manifest=source_manifest,
            source_approval=source_approval,
            fixture=_fixture(tmp_path / "fixture.json"),
            mapping=_FIXTURE_MAPPING,
            run_dir=tmp_path / "fixture-run",
        )

    assert (tmp_path / "fixture-run").is_dir()
    assert not (tmp_path / "fixture-run" / "completion.json").exists()


def test_fixture_run_never_treats_candidate_local_archive_path_as_network_locator(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    manifest = json.loads(source_manifest.read_text(encoding="utf-8"))
    manifest["local_archive_path"] = "https://example.test/source.zip"
    source_manifest.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(source_artifact.httpx, "Client", lambda: pytest.fail("fixture run must not create an HTTP client"))

    with pytest.raises((PilotError, OSError)):
        run_fixture(
            source_manifest=source_manifest,
            source_approval=source_approval,
            fixture=_fixture(tmp_path / "fixture.json"),
            mapping=_FIXTURE_MAPPING,
            run_dir=tmp_path / "fixture-run",
        )

    assert not (tmp_path / "fixture-run").exists()


@pytest.mark.parametrize("unsafe", [r"\\server\share\source.zip", "//server/share/source.zip", r"\\?\C:\source.zip", r"\\.\PhysicalDrive0", "https://example.test/source.zip", "file:///C:/source.zip"])
@pytest.mark.parametrize("field", ["local_archive_path", "local_extracted_path"])
def test_fixture_run_rejects_unc_device_and_uri_candidate_paths_before_filesystem_access(field: str, unsafe: str, tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    manifest = json.loads(source_manifest.read_text(encoding="utf-8"))
    manifest[field] = unsafe
    source_manifest.write_text(json.dumps(manifest), encoding="utf-8")
    real_lstat = source_artifact.os.lstat
    real_open = source_artifact.Path.open
    real_is_file = source_artifact.Path.is_file

    def guarded_lstat(path: object):
        if str(path) == unsafe:
            pytest.fail("unsafe candidate path must be rejected before lstat")
        return real_lstat(path)

    def guarded_open(path: Path, *args: object, **kwargs: object):
        if str(path) == unsafe:
            pytest.fail("unsafe candidate path must be rejected before open")
        return real_open(path, *args, **kwargs)

    def guarded_is_file(path: Path):
        if str(path) == unsafe:
            pytest.fail("unsafe candidate path must be rejected before stat")
        return real_is_file(path)

    monkeypatch.setattr(source_artifact.os, "lstat", guarded_lstat)
    monkeypatch.setattr(source_artifact.Path, "open", guarded_open)
    monkeypatch.setattr(source_artifact.Path, "is_file", guarded_is_file)
    monkeypatch.setattr(source_artifact.httpx, "Client", lambda: pytest.fail("fixture run must not create an HTTP client"))

    with pytest.raises((PilotError, OSError)):
        run_fixture(
            source_manifest=source_manifest,
            source_approval=source_approval,
            fixture=_fixture(tmp_path / "fixture.json"),
            mapping=_FIXTURE_MAPPING,
            run_dir=tmp_path / "fixture-run",
        )

    assert not (tmp_path / "fixture-run").exists()


def test_fixture_run_rejects_incomplete_source_pack_before_panel_or_path_fallback(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = make_source_zip()
    source_root = tmp_path / "incomplete-source"
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = OutputPack.create(parent, run_id=source_root.name, pack_schema_version="bond-pilot-source-v1", producer_version="test")
        try:
            candidate = qualify_source(archive, pack)
            approval = _approval_for(candidate)
            approval_path = tmp_path / "source-approval.json"
            approval_path.write_text(json.dumps(approval.to_json_mapping()), encoding="utf-8")
            monkeypatch.setattr(workflow, "build_observed_panel", lambda *_args: pytest.fail("incomplete pack must fail before panel"))
            with pytest.raises(PilotError, match="^incomplete_output$"):
                run_fixture(source_manifest=source_root / "source-manifest.json", source_approval=approval_path, fixture=_fixture(tmp_path / "fixture.json"), mapping=_FIXTURE_MAPPING, run_dir=tmp_path / "fixture-run")
            assert not (tmp_path / "fixture-run").exists()
        finally:
            pack.close()


def test_fixture_run_repeated_id_never_overwrites_completed_pack(tmp_path: Path, make_source_zip) -> None:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    run_dir = tmp_path / "fixture-run"
    run_fixture(source_manifest=source_manifest, source_approval=source_approval, fixture=_fixture(tmp_path / "fixture.json"), mapping=_FIXTURE_MAPPING, run_dir=run_dir)
    before = {path.name: path.read_bytes() for path in run_dir.iterdir()}
    with pytest.raises(PilotError, match="^already_exists$"):
        run_fixture(source_manifest=source_manifest, source_approval=source_approval, fixture=_fixture(tmp_path / "fixture-again.json"), mapping=_FIXTURE_MAPPING, run_dir=run_dir)
    assert before == {path.name: path.read_bytes() for path in run_dir.iterdir()}


def test_fixture_run_uses_retained_source_capability_and_preserves_input_bytes(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, source_approval, archive = _qualified_source(tmp_path, make_source_zip)
    candidate = load_candidate(source_manifest)
    source_pack = source_manifest.parent
    before = {path.name: path.read_bytes() for path in source_pack.iterdir()} | {"archive": archive.read_bytes()}
    panel_sources: list[object] = []
    real_panel = workflow.build_observed_panel
    monkeypatch.setattr(workflow, "build_observed_panel", lambda source, *args: panel_sources.append(source) or real_panel(source, *args))
    run_fixture(source_manifest=source_manifest, source_approval=source_approval, fixture=_fixture(tmp_path / "fixture.json"), mapping=_FIXTURE_MAPPING, run_dir=tmp_path / "fixture-run")
    assert panel_sources and hasattr(panel_sources[0], "read")
    assert panel_sources[0] != candidate.local_extracted_path
    assert before == ({path.name: path.read_bytes() for path in source_pack.iterdir()} | {"archive": archive.read_bytes()})


def test_fixture_run_consumes_pinned_source_directory_after_name_swap(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    original = source_manifest.parent
    displaced = tmp_path / "displaced-source"
    real_open = workflow.open_validated_output_pack

    @contextmanager
    def swap_after_validation(*args, **kwargs):
        with real_open(*args, **kwargs) as opened:
            try:
                original.rename(displaced)
                original.mkdir()
                (original / "source.parquet").write_bytes(b"attacker")
            except OSError as exc:
                pytest.skip(f"directory swap unavailable: {exc}")
            yield opened

    monkeypatch.setattr(workflow, "open_validated_output_pack", swap_after_validation)
    run_fixture(source_manifest=source_manifest, source_approval=source_approval, fixture=_fixture(tmp_path / "fixture.json"), mapping=_FIXTURE_MAPPING, run_dir=tmp_path / "fixture-run")
    assert (tmp_path / "fixture-run" / "completion.json").is_file()
    assert (original / "source.parquet").read_bytes() == b"attacker"


def test_fixture_run_rejects_in_place_panel_mutation_during_observation_index(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    real_build = workflow.ObservationIndex.build

    def mutate_then_build(panel_input, *args):
        status = panel_input.path.stat()
        os.utime(panel_input.path, ns=(status.st_atime_ns, status.st_mtime_ns + 1_000_000_000))
        return real_build(panel_input, *args)

    monkeypatch.setattr(workflow.ObservationIndex, "build", mutate_then_build)
    with pytest.raises(PilotError, match="source_integrity_failed"):
        run_fixture(source_manifest=source_manifest, source_approval=source_approval, fixture=_fixture(tmp_path / "fixture.json"), mapping=_FIXTURE_MAPPING, run_dir=tmp_path / "fixture-run")
    assert not (tmp_path / "fixture-run" / "completion.json").exists()


@pytest.mark.parametrize("missing", ["source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval"])
def test_calibrate_prevalidates_every_governance_input_before_dsn_or_connection(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    calls: list[str] = []
    monkeypatch.setattr("src.bond_pilot.workflow.resolve_dsn", lambda: calls.append("resolve") or "dsn")
    monkeypatch.setattr("src.bond_pilot.workflow.connect", lambda _dsn: calls.append("connect"))
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "run"}
    kwargs[missing] = tmp_path / f"also-missing-{missing}.json"
    with pytest.raises(PilotError):
        run_calibration(**kwargs)
    assert calls == []
    assert (kwargs["run_dir"] / "stop-report.json").is_file()
    assert (kwargs["run_dir"] / "checksums.sha256").is_file()


@pytest.mark.parametrize("control", ["mapping", "mapping_approval", "evidence", "evidence_approval"])
def test_calibrate_rejects_nonlocal_control_before_open_or_connection(control: str, tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "run"}
    unsafe = r"\\server\share\control.json"
    kwargs[control] = unsafe
    real_lstat = artifacts.os.lstat
    real_open = artifacts.Path.open
    monkeypatch.setattr(artifacts.os, "lstat", lambda path: pytest.fail("nonlocal control must fail before lstat") if str(path) == unsafe else real_lstat(path))
    monkeypatch.setattr(artifacts.Path, "open", lambda path, *args, **kwargs: pytest.fail("nonlocal control must fail before open") if str(path) == unsafe else real_open(path, *args, **kwargs))
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: pytest.fail("control must fail before DSN"))
    monkeypatch.setattr(workflow, "connect", lambda _dsn: pytest.fail("control must fail before connection"))

    with pytest.raises(PilotError):
        run_calibration(**kwargs)


def test_calibrate_rejects_relative_mapping_before_connection(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "run"}
    kwargs["mapping"] = "mapping.json"
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: pytest.fail("mapping must fail before DSN"))
    monkeypatch.setattr(workflow, "connect", lambda _dsn: pytest.fail("mapping must fail before connection"))

    with pytest.raises(PilotError, match="debt_mapping_unapproved"):
        run_calibration(**kwargs)


def test_calibrate_revalidates_mismatched_phase4_pins_before_dsn_or_connection(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: calls.append("connect"))
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "run"}
    approval = json.loads(kwargs["evidence_approval"].read_text(encoding="utf-8"))
    approval["evidence_sha256"] = "f" * 64
    kwargs["evidence_approval"].write_text(json.dumps(approval), encoding="utf-8")
    monkeypatch.setenv("BOND_PILOT_PHASE4_V2_APPROVAL_SHA256", hashlib.sha256(kwargs["evidence_approval"].read_bytes()).hexdigest())
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        run_calibration(**kwargs)
    assert calls == []


def test_output_path_gate_precedes_dsn_and_recognizes_lexists(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    root = Path(__file__).resolve().parents[2]
    with pytest.raises(PilotError, match="invalid_output_path"):
        qualify(source=tmp_path / "missing.zip", run_dir=root / "unused-pilot-output")
    for existing in (tmp_path / "existing-dir", tmp_path / "existing-file"):
        if existing.name.endswith("dir"):
            existing.mkdir()
        else:
            existing.write_text("winner", encoding="utf-8")
        with pytest.raises(PilotError, match="already_exists"):
            qualify(source=tmp_path / "missing.zip", run_dir=existing)
    dangling = tmp_path / "dangling"
    try:
        os.symlink(tmp_path / "missing-target", dangling)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(PilotError, match="already_exists"):
        qualify(source=tmp_path / "missing.zip", run_dir=dangling)
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: calls.append("connect"))
    calibration = _calibration_documents(tmp_path, make_source_zip, monkeypatch)
    existing_calibration = tmp_path / "calibration-final"
    existing_calibration.mkdir()
    with pytest.raises(PilotError, match="already_exists"):
        run_calibration(**calibration, mode="calibration", series_ids=("series-1",), run_dir=existing_calibration)
    assert calls == []


_PUBLIC_KEYS = {"value", "values", "unit", "units", "date", "as_of_date", "freshness", "quality", "availability", "methodology_version", "is_144a"}
_FORBIDDEN = {"source", "provider", "vendor", "upstream", "url", "file", "row_id", "hash", "lineage", "license", "entitlement", "error"}
_FORBIDDEN_TEXT = re.compile(r"\b(?:source|provider|vendor|upstream|lineage|license|entitlement|error|trace|finra|osbap|openbondassetpricing|bonds-api|bonds api|wrds|developer_finra|sec|n-?port)\b", re.IGNORECASE)
_URI = re.compile(r"^[a-z][a-z0-9+.-]*:", re.IGNORECASE)
_FILE = re.compile(r"[A-Za-z0-9_-]+\.[A-Za-z][A-Za-z0-9_-]{0,15}(?:$|[?#])", re.IGNORECASE)
_HEX_DIGEST = re.compile(r"^(?:(?:sha256:)?[0-9a-f]{64}|(?:sha512:)?[0-9a-f]{128})$", re.IGNORECASE)
_BASE64_DIGEST = re.compile(r"^(?:(?:sha256:)?(?:[A-Za-z0-9+/]{43}=?|[A-Za-z0-9_-]{43}=?)|(?:sha512:)?(?:[A-Za-z0-9+/]{86}(?:==)?|[A-Za-z0-9_-]{86}(?:==)?))$", re.IGNORECASE)


def _assert_future_public(value: object) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            assert isinstance(key, str)
            key_text = key.casefold()
            assert key_text in _PUBLIC_KEYS
            if key_text != "methodology_version":
                assert "version" not in key_text
            assert not any(token in key_text for token in _FORBIDDEN)
            _assert_future_public(child)
    elif isinstance(value, list):
        for child in value:
            _assert_future_public(child)
    elif isinstance(value, str):
        lowered = value.casefold()
        assert not any(token in lowered for token in _FORBIDDEN)
        assert _FORBIDDEN_TEXT.search(value) is None
        assert _URI.match(value) is None
        assert not any(marker in value for marker in ("/", "\\"))
        assert _FILE.search(value) is None
        assert _HEX_DIGEST.fullmatch(value) is None and _BASE64_DIGEST.fullmatch(value) is None
    elif value is None or isinstance(value, bool | int):
        return
    elif isinstance(value, float):
        assert math.isfinite(value)
        return
    else:
        raise AssertionError("future public payload must be JSON-only")


def test_future_public_allowlist_rejects_nested_provenance_and_source_family_literals() -> None:
    _assert_future_public({"values": [{"value": 1.0, "unit": "seconds"}], "is_144a": True, "quality": {"freshness": "daily"}, "methodology_version": "bond-metrics-v1"})
    for value in ({"quality": {"source_url": "https://example.invalid"}}, {"methodology_version": "TRACE-v1"}, {"value": "C:/secret/file.csv"}, {"value": "../../secret"}, {"value": "mailto:owner@example.invalid"}, {"value": "row.SEC.nport"}, {"value": "artifact.xlsx"}, {"value": "data.avro"}, {"value": "snapshot.gz"}, {"value": "report.parquet?download=1"}, {"value": "a" * 64}, {"value": "sha256:" + "a" * 64}, {"value": "a" * 128}, {"value": "sha512:" + "a" * 128}, {"value": "A" * 43 + "="}, {"value": "sha256:" + "A" * 43 + "="}, {"value": "A" * 86 + "=="}, {"value": "sha512:" + "A" * 86 + "=="}, {"value": "_" * 86 + "=="}, {"value": "sha512:" + "_" * 86 + "=="}, {"value": ("not-json",)}, {"value": float("nan")}):
        with pytest.raises(AssertionError):
            _assert_future_public(value)


@pytest.mark.parametrize("digest", [
    "A" * 43, "A" * 43 + "=", "-" * 43, "-" * 43 + "=",
    "sha256:" + "A" * 43, "sha256:" + "A" * 43 + "=", "sha256:" + "-" * 43, "sha256:" + "-" * 43 + "=",
    "A" * 86, "A" * 86 + "==", "_" * 86, "_" * 86 + "==",
    "sha512:" + "A" * 86, "sha512:" + "A" * 86 + "==", "sha512:" + "_" * 86, "sha512:" + "_" * 86 + "==",
])
def test_future_public_allowlist_rejects_padded_and_unpadded_algorithm_bound_base64(digest: str) -> None:
    with pytest.raises(AssertionError):
        _assert_future_public({"value": digest})


def test_internal_manifest_retains_full_provenance_and_pilot_stays_out_of_public_surfaces() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "src" / "bond_pilot" / "reporting.py").read_text(encoding="utf-8")
    assert "source_candidate.to_json_mapping" in source and "internal_only" in source
    forbidden_roots = ("frontend", "api", "public", "backend/app")
    changed = subprocess.run(["git", "diff", "--name-only", "main...HEAD"], cwd=root, capture_output=True, text=True, check=True).stdout.splitlines()
    assert not [path for path in changed if path.startswith(("frontend/", "api/", "public/", "backend/app/"))]
    for base in forbidden_roots:
        if (root / base).exists():
            assert not [path for path in (root / base).rglob("*") if path.is_file() and "bond_pilot" in path.read_text(encoding="utf-8", errors="ignore")]
    for path in (root / "src" / "run.py", root / "src" / "run_worker.py"):
        assert "bond_pilot" not in ast.dump(ast.parse(path.read_text(encoding="utf-8")))
    diff = subprocess.run(["git", "diff", "--exit-code", "main...HEAD", "--", "src/run.py", "src/run_worker.py", "src/workers", "requirements.txt", "pyproject.toml"], cwd=root, capture_output=True, text=True)
    assert diff.returncode == 0, diff.stdout + diff.stderr


def test_cli_typed_stop_publishes_internal_only_checksums_and_exit_two(tmp_path: Path) -> None:
    code = main(["fixture-run", "--source-manifest", str(tmp_path / "missing.json"), "--source-approval", str(tmp_path / "missing-approval.json"), "--fixture", str(tmp_path / "fixture.json"), "--mapping", "tests/bond_pilot/fixtures/debt-mapping-test-v2.json", "--run-dir", str(tmp_path / "stop")])
    assert code == 2
    report = json.loads((tmp_path / "stop" / "stop-report.json").read_text(encoding="utf-8"))
    assert report["internal_only"] is True
    assert (tmp_path / "stop" / "checksums.sha256").is_file()


def test_stop_report_never_replaces_existing_final_or_repo_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    final = tmp_path / "winner"
    final.mkdir()
    (final / "sentinel").write_text("winner", encoding="utf-8")
    workflow.write_stop_report(final, PilotError("stopped"))
    assert (final / "sentinel").read_text(encoding="utf-8") == "winner"
    repo = tmp_path / "repo"
    repo.mkdir()
    monkeypatch.setattr(workflow, "_REPOSITORY_ROOT", repo)
    internal = repo / "stop-path"
    workflow.write_stop_report(internal, PilotError("stopped"))
    assert not internal.exists()


def test_calibration_publishes_atomic_internal_pack_and_checkpoint_on_typed_stop(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def stop(_connection, **values):
        _write_mock_checkpoint(values, state="stopped", reason="unsafe_query_plan")
        raise PilotError("unsafe_query_plan")
    monkeypatch.setattr(workflow, "run_v2_calibration", stop)
    with pytest.raises(PilotError, match="unsafe_query_plan"):
        run_calibration(**kwargs)
    final = kwargs["run_dir"]
    assert (final / "checkpoint-000000000001.json").is_file()
    assert (final / "stop-report.json").is_file()
    assert (final / "checksums.sha256").is_file()
    assert not list(tmp_path.glob(".calibration.*.partial-dir"))


def test_calibration_success_pack_is_atomic_and_binds_internal_provenance(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def succeed(_connection, **values):
        rows = (_calibration_row(),)
        _write_mock_checkpoint(values, rows=rows)
        return SimpleNamespace(rows=rows, rows_read=1, pages=1, partial=False, last_key=("series-1", "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1"), mode="calibration")
    monkeypatch.setattr(workflow, "run_v2_calibration", succeed)
    report = run_calibration(**kwargs)
    final = kwargs["run_dir"]
    assert report["rows_artifact"] == "calibration-rows-v1.json"
    provenance = json.loads((final / "calibration-provenance.json").read_text(encoding="utf-8"))
    assert provenance["internal_only"] is True
    assert provenance["source"]["approval"]["source_locator"]
    assert provenance["mapping"]["observed_composite_values_sha256"] == "c" * 64
    assert "rules" not in provenance["mapping"]
    assert provenance["mapping"]["approval_reference"] == hashlib.sha256(kwargs["mapping_approval"].read_bytes()).hexdigest()
    assert provenance["phase4"]["evidence_sha256"] and provenance["phase4"]["approval_sha256"]
    assert provenance["phase4"]["evidence"]["approved_by"] == "phase4 reviewer"
    assert provenance["phase4"]["approval"]["approved_modes"] == ["calibration"]
    checkpoint_path = max(final.glob("checkpoint-*.json"))
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert report["output_hash"] == checkpoint["output_hash"] == "f" * 64
    assert report["checkpoint_sha256"] == hashlib.sha256(artifacts.canonical_json_bytes(checkpoint)).hexdigest()
    rows = json.loads((final / "calibration-rows-v1.json").read_text(encoding="utf-8"))
    assert rows["columns"] == list(REQUIRED_COLUMNS)
    assert rows["rows"][0][REQUIRED_COLUMNS.index("signed_market_value")] == {"type": "decimal", "value": "10.250"}
    assert checkpoint_path.is_file() and (final / "checksums.sha256").is_file()
    assert not list(tmp_path.glob(".calibration.*.partial-dir"))


def test_calibration_late_write_failure_leaves_no_final_or_staging_and_retry_succeeds(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def succeed(_connection, **values):
        _write_mock_checkpoint(values, output_hash=hashlib.sha256(b"").hexdigest())
        return SimpleNamespace(rows=(), rows_read=0, pages=1, partial=False, last_key=None, mode="calibration")
    monkeypatch.setattr(workflow, "run_v2_calibration", succeed)
    original = OutputPack.write_payload
    def fail_late(pack: OutputPack, name: str, contents: bytes):
        if name == "calibration-report.json":
            raise RuntimeError("late write")
        return original(pack, name, contents)
    monkeypatch.setattr(OutputPack, "write_payload", fail_late)
    with pytest.raises(RuntimeError, match="late write"):
        run_calibration(**kwargs)
    assert kwargs["run_dir"].is_dir()
    assert not (kwargs["run_dir"] / "completion.json").exists()
    monkeypatch.setattr(OutputPack, "write_payload", original)
    assert run_calibration(**{**kwargs, "run_dir": tmp_path / "calibration-retry"})["internal_only"] is True


def test_publish_races_preserve_existing_winner_for_calibration_and_stop(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def succeed(_connection, **values):
        _write_mock_checkpoint(values, output_hash=hashlib.sha256(b"").hexdigest())
        return SimpleNamespace(rows=(), rows_read=0, pages=1, partial=False, last_key=None, mode="calibration")
    monkeypatch.setattr(workflow, "run_v2_calibration", succeed)
    def winner(_parent: object, *, run_id: str, **_kwargs: object) -> OutputPack:
        output = tmp_path / run_id
        output.mkdir()
        (output / "sentinel").write_text("winner", encoding="utf-8")
        raise PilotError("already_exists")
    monkeypatch.setattr(OutputPack, "create", winner)
    with pytest.raises(PilotError, match="already_exists"):
        run_calibration(**kwargs)
    assert (kwargs["run_dir"] / "sentinel").read_text(encoding="utf-8") == "winner"
    stop = tmp_path / "calibration"
    workflow.write_stop_report(stop, PilotError("stopped"))
    assert (stop / "sentinel").read_text(encoding="utf-8") == "winner"
    assert not list(tmp_path.glob(".stop.*.partial-dir"))


@pytest.mark.parametrize("bad", [object(), float("nan"), {"nested": "no"}])
def test_calibration_row_serialization_fails_closed_with_typed_stop(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch, bad: object) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def succeed(_connection, **values):
        rows = (_calibration_row(cusip=bad),)
        _write_mock_checkpoint(values, rows=rows)
        return SimpleNamespace(rows=rows, rows_read=1, pages=1, partial=False, last_key=("series-1", "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1"), mode="calibration")
    monkeypatch.setattr(workflow, "run_v2_calibration", succeed)
    with pytest.raises(PilotError, match="calibration_row_serialization_failed"):
        run_calibration(**kwargs)
    assert (kwargs["run_dir"] / "stop-report.json").is_file()
    assert not (kwargs["run_dir"] / "calibration-report.json").exists()


def test_calibration_maps_operational_failures_without_leaking_messages(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: (_ for _ in ()).throw(RuntimeError("postgres://secret")))
    with pytest.raises(PilotError, match="calibration_connection_failed"):
        run_calibration(**kwargs)
    stop = json.loads((kwargs["run_dir"] / "stop-report.json").read_text(encoding="utf-8"))
    assert "postgres" not in json.dumps(stop) and stop["exception_class"] == "PilotError"


def test_resume_pack_is_validated_before_connection_and_keeps_source_pack_immutable(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    candidate, approval, mapping, evidence, evidence_approval, series = workflow._calibration_inputs(**{key: kwargs[key] for key in ("source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval", "mode", "series_ids")})
    provenance = workflow._calibration_provenance(candidate, approval, mapping, evidence, evidence_approval, series, "calibration")
    prior = tmp_path / "prior"
    prior_key = ("series-1", "2024-03-31", "pub-1", "acc-1", "h-1000", "run-1", "instrument-1000")
    reports = (("series-1", "2024-03-31", "pub-1", "acc-1"),)
    values = {"evidence": evidence, "approval": evidence_approval, "mode": "calibration", "series_ids": series, "rows": ()}
    checkpoint = _checkpoint(values, state="stopped", reason="unsafe_query_plan", run_id="original-run", pages=1, rows_read=1000, reports=reports, last_key=prior_key)
    _write_stopped_resume_pack(prior, provenance=provenance, evidence=evidence, approval=evidence_approval, series=series, checkpoint_raw=checkpoint)
    before = {path.name: path.read_bytes() for path in prior.iterdir()}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def resumed(_connection, **values):
        pack = values["checkpoint_pack"]
        assert isinstance(pack, OutputPack)
        with pack.directory.open_file("checkpoint-000000000001.json", error_code="checkpoint_invalid") as current:
            seed = json.loads(current.read_all(max_bytes=1024 * 1024))
            assert seed["run_id"] == "original-run"
            assert seed["pages"] == 1 and seed["rows"] == 1000
            assert seed["output_state"] == "in_progress" and seed["stop_reason"] is None
        final_key = ("series-1", "2024-03-31", "pub-1", "acc-1", "h-1001", "run-1", "instrument-1001")
        rows = (_calibration_row(holding_id="h-1001", instrument_id="instrument-1001"),)
        _write_mock_checkpoint(values, rows=rows, output_hash="e" * 64, pages=2, rows_read=1001, reports=reports, last_key=final_key)
        return SimpleNamespace(rows=rows, rows_read=1001, pages=2, partial=False, last_key=final_key, mode="calibration")
    monkeypatch.setattr(workflow, "run_v2_calibration", resumed)
    result = run_calibration(**kwargs, resume_pack=prior)
    assert result["resume_pack_checksums_sha256"] == hashlib.sha256(before["checksums.sha256"]).hexdigest()
    assert result["invocation_rows"] == 1 and result["cumulative_rows"] == 1001 and result["rows_artifact_scope"] == "this_invocation"
    assert before == {path.name: path.read_bytes() for path in prior.iterdir()}
    tampered = tmp_path / "tampered"
    tampered.mkdir()
    for name, contents in before.items():
        (tampered / name).write_bytes(contents)
    (tampered / "checkpoint-000000000001.json").write_text("{}", encoding="utf-8")
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        run_calibration(**{**kwargs, "run_dir": tmp_path / "tampered-output"}, resume_pack=tampered)
    assert calls == []


@pytest.mark.parametrize("case", ["gap", "invalid_earlier", "counter_regression", "cursor_hash_regression", "terminal_successor", "noncanonical_earlier"])
def test_resume_rejects_invalid_sealed_checkpoint_chain_before_connection(case: str, tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    candidate, source_approval, mapping, evidence, evidence_approval, series = workflow._calibration_inputs(**{key: kwargs[key] for key in ("source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval", "mode", "series_ids")})
    provenance = workflow._calibration_provenance(candidate, source_approval, mapping, evidence, evidence_approval, series, "calibration")
    values = {"evidence": evidence, "approval": evidence_approval, "mode": "calibration", "series_ids": series, "rows": ()}
    reports = (("series-1", "2024-03-31", "pub-1", "acc-1"),)
    first_key = ("series-1", "2024-03-31", "pub-1", "acc-1", "h-1000", "run-1", "instrument-1000")
    second_key = ("series-1", "2024-03-31", "pub-1", "acc-1", "h-2000", "run-1", "instrument-2000")
    in_progress = _checkpoint(values, output_hash="a" * 64, state="in_progress", run_id="original-run", pages=1, rows_read=1000, reports=reports, last_key=first_key)
    stopped = _checkpoint(values, output_hash="a" * 64, state="stopped", reason="unsafe_query_plan", run_id="original-run", pages=1, rows_read=1000, reports=reports, last_key=first_key)

    if case == "gap":
        checkpoints = (("checkpoint-000000000001.json", in_progress), ("checkpoint-000000000003.json", stopped))
    elif case == "invalid_earlier":
        checkpoints = (("checkpoint-000000000001.json", b"{}"), ("checkpoint-000000000002.json", stopped))
    elif case == "counter_regression":
        earlier = _checkpoint(values, output_hash="b" * 64, state="in_progress", run_id="original-run", pages=2, rows_read=2000, reports=reports, last_key=second_key)
        checkpoints = (("checkpoint-000000000001.json", earlier), ("checkpoint-000000000002.json", stopped))
    elif case == "cursor_hash_regression":
        changed = _checkpoint(values, output_hash="c" * 64, state="stopped", reason="unsafe_query_plan", run_id="original-run", pages=1, rows_read=1000, reports=reports, last_key=second_key)
        checkpoints = (("checkpoint-000000000001.json", in_progress), ("checkpoint-000000000002.json", changed))
    elif case == "terminal_successor":
        checkpoints = (("checkpoint-000000000001.json", stopped), ("checkpoint-000000000002.json", stopped))
    else:
        noncanonical = json.dumps(json.loads(in_progress)).encode("utf-8")
        checkpoints = (("checkpoint-000000000001.json", noncanonical), ("checkpoint-000000000002.json", stopped))

    _write_resume_pack_with_checkpoints(tmp_path / "prior", provenance=provenance, checkpoints=checkpoints)
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        run_calibration(**kwargs, resume_pack=tmp_path / "prior")
    assert calls == []
    assert not (kwargs["run_dir"] / "calibration-report.json").exists()


def test_zero_row_stopped_checkpoint_resumes_without_invalid_seed(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    candidate, source_approval, mapping, evidence, evidence_approval, series = workflow._calibration_inputs(**{key: kwargs[key] for key in ("source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval", "mode", "series_ids")})
    provenance = workflow._calibration_provenance(candidate, source_approval, mapping, evidence, evidence_approval, series, "calibration")
    prior = tmp_path / "prior"
    _write_stopped_resume_pack(prior, provenance=provenance, evidence=evidence, approval=evidence_approval, series=series)
    prior_digest = hashlib.sha256((prior / "checksums.sha256").read_bytes()).hexdigest()
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())

    def resumed(_connection, **values):
        pack = values["checkpoint_pack"]
        assert isinstance(pack, OutputPack)
        assert not [name for name in pack.directory.enumerate() if name.startswith("checkpoint-")]
        assert values["run_id"] == "original-run"
        initial = values["initial_checkpoint"]
        assert initial["output_state"] == "stopped"
        assert initial["pages"] == initial["rows"] == 0
        assert initial["elapsed_seconds"] == 1.0
        assert initial["run_id"] == "original-run"
        rows = (_calibration_row(),)
        key = ("series-1", "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1")
        _write_mock_checkpoint(values, rows=rows, last_key=key)
        return SimpleNamespace(rows=rows, rows_read=1, pages=1, partial=False, last_key=key, mode="calibration")

    monkeypatch.setattr(workflow, "run_v2_calibration", resumed)
    result = run_calibration(**kwargs, resume_pack=prior)
    assert result["resume_pack_checksums_sha256"] == prior_digest
    assert result["invocation_rows"] == 1 and result["cumulative_rows"] == 1
    checkpoint = json.loads((kwargs["run_dir"] / "checkpoint-000000000001.json").read_bytes())
    assert checkpoint["run_id"] == "original-run"
    assert checkpoint["output_state"] == "complete"


def test_output_path_rejects_repository_descendant_before_pack_creation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workflow, "_REPOSITORY_ROOT", tmp_path / "repo")
    with pytest.raises(PilotError, match="^invalid_output_path$"):
        qualify(source=tmp_path / "missing.zip", run_dir=tmp_path / "repo" / "new-run")


@pytest.mark.parametrize("field,value", [("evidence_sha256", "0" * 64), ("query_sha256", "0" * 64), ("method_sha256", "0" * 64), ("mode", "first_bounded")])
def test_resume_governance_mismatches_fail_before_dsn(field: str, value: str, tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    candidate, approval, mapping, evidence, evidence_approval, series = workflow._calibration_inputs(**{key: kwargs[key] for key in ("source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval", "mode", "series_ids")})
    prior = tmp_path / "prior"
    payload = json.loads(_checkpoint({"evidence": evidence, "approval": evidence_approval, "mode": "calibration", "series_ids": series, "rows": ()}, output_hash=hashlib.sha256(b"").hexdigest(), state="stopped", reason="unsafe_query_plan", run_id="original-run"))
    payload[field] = value
    _write_stopped_resume_pack(prior, provenance=workflow._calibration_provenance(candidate, approval, mapping, evidence, evidence_approval, series, "calibration"), evidence=evidence, approval=evidence_approval, series=series, checkpoint_raw=json.dumps(payload).encode("utf-8"))
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        run_calibration(**kwargs, resume_pack=prior)
    assert calls == []


@pytest.mark.parametrize("oversized", ["checkpoint-000000000001.json", "checksums.sha256"])
def test_resume_oversized_control_file_fails_before_dsn(oversized: str, tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    prior = tmp_path / "prior"
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = OutputPack.create(parent, run_id=prior.name, pack_schema_version="bond-pilot-calibration-v1", producer_version="test")
        try:
            pack.write_payload("checkpoint-000000000001.json", b"x" * (workflow._MAX_RESUME_CONTROL_BYTES + 1) if oversized.startswith("checkpoint-") else b"{}")
            pack.write_payload("calibration-provenance.json", b"{}")
            pack.write_payload("stop-report.json", b"{}")
            pack.finalize()
        finally:
            pack.close()
    if oversized == "checksums.sha256":
        (prior / oversized).write_bytes(b"x" * (workflow._MAX_RESUME_CONTROL_BYTES + 1))
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        run_calibration(**kwargs, resume_pack=prior)
    assert calls == []


@pytest.mark.parametrize("field,value", [("run_id", "other-run"), ("mode", "first_bounded"), ("query_sha256", "0" * 64), ("method_sha256", "0" * 64), ("output_state", "stopped"), ("stop_reason", "unexpected")])
def test_final_checkpoint_mismatches_are_not_published(field: str, value: str, tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / f"bad-{field}"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def mismatched(_connection, **values):
        rows = (_calibration_row(),)
        raw = _checkpoint({**values, "rows": rows}, run_id=str(values["run_id"]))
        payload = json.loads(raw)
        payload[field] = value
        _write_mock_checkpoint(values, rows=rows)
        pack = values["checkpoint_pack"]
        assert isinstance(pack, OutputPack)
        pack.write_payload("checkpoint-000000000002.json", json.dumps(payload).encode("utf-8"))
        key = ("series-1", "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1")
        return SimpleNamespace(rows=rows, rows_read=1, pages=1, partial=False, last_key=key, mode="calibration")
    monkeypatch.setattr(workflow, "run_v2_calibration", mismatched)
    with pytest.raises(PilotError):
        run_calibration(**kwargs)
    assert not (kwargs["run_dir"] / "calibration-report.json").exists()


def test_calibrate_rejects_phase4_evidence_not_bound_to_exact_composite_mapping(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    evidence = json.loads(kwargs["evidence"].read_text(encoding="utf-8"))
    evidence["mapping_artifact_sha256"] = "f" * 64
    kwargs["evidence"].write_text(json.dumps(evidence), encoding="utf-8")
    approval = json.loads(kwargs["evidence_approval"].read_text(encoding="utf-8"))
    approval["evidence_sha256"] = hashlib.sha256(kwargs["evidence"].read_bytes()).hexdigest()
    approval["mapping_artifact_sha256"] = "f" * 64
    kwargs["evidence_approval"].write_text(json.dumps(approval), encoding="utf-8")
    monkeypatch.setenv("BOND_PILOT_PHASE4_V2_APPROVAL_SHA256", hashlib.sha256(kwargs["evidence_approval"].read_bytes()).hexdigest())
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: pytest.fail("mapping binding must fail before DSN"))
    with pytest.raises(PilotError, match="phase4b_v2_unavailable"):
        run_calibration(**kwargs)
    assert not (kwargs["run_dir"] / "calibration-report.json").exists()


@pytest.mark.parametrize("document,raw", [
    ("calibration-provenance.json", b'{"internal_only":true,"internal_only":true}'),
    ("stop-report.json", b'{"internal_only":true,"internal_only":true}'),
    ("calibration-provenance.json", b'{"value":NaN}'),
    ("stop-report.json", b'{"value":Infinity}'),
    ("calibration-provenance.json", b'{"value":-Infinity}'),
])
def test_resume_captured_json_rejects_duplicate_and_nonfinite_values_before_dsn(document: str, raw: bytes, tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    candidate, approval, mapping, evidence, evidence_approval, series = workflow._calibration_inputs(**{key: kwargs[key] for key in ("source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval", "mode", "series_ids")})
    raw_kwargs = {"provenance_raw": raw} if document == "calibration-provenance.json" else {"stop_raw": raw}
    _write_stopped_resume_pack(tmp_path / "prior", provenance=workflow._calibration_provenance(candidate, approval, mapping, evidence, evidence_approval, series, "calibration"), evidence=evidence, approval=evidence_approval, series=series, **raw_kwargs)
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        run_calibration(**kwargs, resume_pack=tmp_path / "prior")
    assert calls == []


def test_resume_provenance_comparison_is_type_strict_before_dsn(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    candidate, approval, mapping, evidence, evidence_approval, series = workflow._calibration_inputs(**{key: kwargs[key] for key in ("source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval", "mode", "series_ids")})
    provenance = workflow._calibration_provenance(candidate, approval, mapping, evidence, evidence_approval, series, "calibration")
    masquerading = {**provenance, "internal_only": 1}
    _write_stopped_resume_pack(tmp_path / "prior", provenance=provenance, evidence=evidence, approval=evidence_approval, series=series, provenance_raw=json.dumps(masquerading).encode("utf-8"))
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        run_calibration(**kwargs, resume_pack=tmp_path / "prior")
    assert calls == []


@pytest.mark.parametrize("stop", [
    {"internal_only": True, "status": "stopped", "code": "unsafe_query_plan", "exception_class": "PilotError", "extra": True},
    {"internal_only": True, "status": "stopped", "code": "unsafe_query_plan"},
    {"internal_only": 1, "status": "stopped", "code": "unsafe_query_plan", "exception_class": "PilotError"},
    {"internal_only": True, "status": "complete", "code": "unsafe_query_plan", "exception_class": "PilotError"},
    {"internal_only": True, "status": "stopped", "code": True, "exception_class": "PilotError"},
    {"internal_only": True, "status": "stopped", "code": "unsafe_query_plan", "exception_class": "RuntimeError"},
])
def test_resume_stop_report_requires_exact_schema_and_types_before_dsn(stop: dict[str, object], tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    candidate, approval, mapping, evidence, evidence_approval, series = workflow._calibration_inputs(**{key: kwargs[key] for key in ("source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval", "mode", "series_ids")})
    _write_stopped_resume_pack(tmp_path / "prior", provenance=workflow._calibration_provenance(candidate, approval, mapping, evidence, evidence_approval, series, "calibration"), evidence=evidence, approval=evidence_approval, series=series, stop_raw=json.dumps(stop).encode("utf-8"))
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        run_calibration(**kwargs, resume_pack=tmp_path / "prior")
    assert calls == []


def test_resume_relative_root_fails_before_dsn(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    candidate, approval, mapping, evidence, evidence_approval, series = workflow._calibration_inputs(**{key: kwargs[key] for key in ("source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval", "mode", "series_ids")})
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        run_calibration(**kwargs, resume_pack="prior")
    assert calls == []


def test_resume_enumerates_through_directory_capability_not_os_scandir(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "resumed"}
    prior = tmp_path / "prior"
    prior.mkdir()
    candidate, source_approval, mapping, evidence, evidence_approval, series = workflow._calibration_inputs(**{key: kwargs[key] for key in ("source_manifest", "source_approval", "mapping", "mapping_approval", "evidence", "evidence_approval", "mode", "series_ids")})
    provenance = workflow._calibration_provenance(candidate, source_approval, mapping, evidence, evidence_approval, series, "calibration")

    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        workflow._resume_input(prior, provenance, evidence, evidence_approval, "calibration", series)


def test_final_checkpoint_is_captured_by_bounded_regular_file_reader(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def succeed(_connection, **values):
        rows = (_calibration_row(),)
        _write_mock_checkpoint(values, rows=rows)
        key = ("series-1", "2024-03-31", "pub-1", "acc-1", "h-1", "run-1", "instrument-1")
        return SimpleNamespace(rows=rows, rows_read=1, pages=1, partial=False, last_key=key, mode="calibration")
    monkeypatch.setattr(workflow, "run_v2_calibration", succeed)
    captured: list[str] = []
    original = OutputPack.write_payload
    monkeypatch.setattr(OutputPack, "write_payload", lambda pack, name, contents: captured.append(name) or original(pack, name, contents))
    run_calibration(**kwargs)
    assert "checkpoint-000000000001.json" in captured
