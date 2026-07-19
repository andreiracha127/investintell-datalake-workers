from __future__ import annotations

import ast
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
from src.bond_pilot.source_artifact import qualify_source
from src.bond_pilot import workflow
from src.bond_pilot.workflow import qualify, run_calibration, run_fixture


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, *_: object) -> None:
        return None


def _checkpoint(values: dict[str, object], output_hash: str = "f" * 64, *, state: str = "complete", reason: str | None = None, run_id: str = "calibration", pages: int | None = None) -> bytes:
    from src.bond_pilot import db_calibration as calibration

    return calibration.canonical_json_bytes(calibration._checkpoint_payload(
        run_id=run_id, evidence=values["evidence"], approval=values["approval"], mode=values["mode"],
        series_ids=values["series_ids"], reports=(), last_key=None, pages=(0 if state == "stopped" and pages is None else pages or 1), rows=len(values.get("rows", ())),
        elapsed_seconds=1.0, output_hash=output_hash, output_state=state, stop_reason=reason,
    ))


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
        "series_id": "series-1", "class_id": None, "instrument_id": "instrument-1", "issuer_category": "fixture_debt",
        "cusip": "123456789", "signed_market_value": 100.0, "signed_pct_of_nav": 0.1, "currency": "USD",
    }]}), encoding="utf-8")
    return path


def _qualified_source(tmp_path: Path, make_source_zip) -> tuple[Path, Path, Path]:
    archive = make_source_zip()
    qualified_dir = tmp_path / "qualified"
    candidate = qualify_source(archive, qualified_dir)
    approval = _approval_for(candidate)
    approval_path = tmp_path / "source-approval.json"
    approval_path.write_text(json.dumps(approval.to_json_mapping()), encoding="utf-8")
    return qualified_dir / "source-manifest.json", approval_path, archive


def _calibration_documents(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    source_manifest, source_approval, _ = _qualified_source(tmp_path, make_source_zip)
    observed = "c" * 64
    mapping = tmp_path / "mapping.json"
    mapping.write_text(json.dumps({"schema_version": "debt-mapping-v1", "mapping_version": "real-v1", "observed_values_sha256": observed, "categories": {"debt": "debt_like_eligible"}}), encoding="utf-8")
    mapping_approval = tmp_path / "mapping-approval.json"
    mapping_approval.write_text(json.dumps({"schema_version": "debt-mapping-approval-v1", "mapping_sha256": hashlib.sha256(mapping.read_bytes()).hexdigest(), "observed_values_sha256": observed, "evidence": [{"reference": "internal evidence", "sha256": "d" * 64}], "approved_by": "reviewer", "approved_at": "2026-07-19T12:00:00Z"}), encoding="utf-8")
    hash_fields = {field: "a" * 64 for field in ("phase4_run_sha256", "reconciliation_sha256", "publication_sha256", "schema_sha256", "lineage_attestation_sha256")}
    evidence = tmp_path / "evidence.json"
    evidence.write_text(json.dumps({"schema_version": "phase4b-v2-evidence-v1", "phase4_status": "completed", "reconciled": True, "v2_published": True, "seam": "nport-v2-current", "relation": "public.sec_nport_holdings_v2_current", "required_columns": ["publication_id", "accession_number", "holding_id", "source_run_id", "report_date", "filing_date", "series_id", "class_id", "instrument_id", "issuer_category", "cusip", "signed_market_value", "signed_pct_of_nav", "currency"], **hash_fields, "approved_series": ["series-1"], "approved_by": "phase4 reviewer", "approved_at": "2026-07-19T12:00:00Z"}), encoding="utf-8")
    evidence_approval = tmp_path / "evidence-approval.json"
    evidence_approval.write_text(json.dumps({"schema_version": "phase4b-v2-evidence-approval-v1", "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(), **hash_fields, "seam": "nport-v2-current", "relation": "public.sec_nport_holdings_v2_current", "approved_series": ["series-1"], "approved_modes": ["calibration"], "allow_read": True, "approved_by": "phase4 approver", "approved_at": "2026-07-19T12:00:00Z"}), encoding="utf-8")
    monkeypatch.setenv("BOND_PILOT_PHASE4_V2_APPROVAL_SHA256", hashlib.sha256(evidence_approval.read_bytes()).hexdigest())
    monkeypatch.setenv("BOND_PILOT_PHASE4_V2_APPROVER_ID", "phase4 approver")
    return {"source_manifest": source_manifest, "source_approval": source_approval, "mapping": mapping, "mapping_approval": mapping_approval, "evidence": evidence, "evidence_approval": evidence_approval}


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
    mapping = Path("tests/bond_pilot/fixtures/debt-mapping-test-v1.json")
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


def test_output_path_gate_precedes_source_or_dsn_and_recognizes_lexists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    existing_calibration = tmp_path / "calibration-final"
    existing_calibration.mkdir()
    with pytest.raises(PilotError, match="already_exists"):
        run_calibration(source_manifest=tmp_path / "missing", source_approval=tmp_path / "missing", mapping=tmp_path / "missing", mapping_approval=tmp_path / "missing", evidence=tmp_path / "missing", evidence_approval=tmp_path / "missing", mode="calibration", series_ids=("S1",), run_dir=existing_calibration)
    assert calls == []


_PUBLIC_KEYS = {"value", "values", "unit", "units", "date", "as_of_date", "freshness", "quality", "availability", "methodology_version", "is_144a"}
_FORBIDDEN = {"source", "provider", "vendor", "upstream", "url", "file", "row_id", "hash", "lineage", "license", "entitlement", "error"}
_FORBIDDEN_TEXT = re.compile(r"\b(?:source|provider|vendor|upstream|lineage|license|entitlement|error|trace|finra|osbap|openbondassetpricing|bonds-api|bonds api|wrds|developer_finra|sec|n-?port)\b", re.IGNORECASE)
_URI = re.compile(r"^[a-z][a-z0-9+.-]*:", re.IGNORECASE)
_FILE = re.compile(r"[A-Za-z0-9_-]+\.[A-Za-z][A-Za-z0-9_-]{0,15}(?:$|[?#])", re.IGNORECASE)
_HEX_DIGEST = re.compile(r"^(?:sha(?:256|512):)?[0-9a-f]{64}$", re.IGNORECASE)
_BASE64_DIGEST = re.compile(r"^(?:sha(?:256|512):)?(?:[A-Za-z0-9+/]{43}=|[A-Za-z0-9_-]{43}=|[A-Za-z0-9+/]{86}==|[A-Za-z0-9_-]{86}==)$", re.IGNORECASE)


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
    for value in ({"quality": {"source_url": "https://example.invalid"}}, {"methodology_version": "TRACE-v1"}, {"value": "C:/secret/file.csv"}, {"value": "../../secret"}, {"value": "mailto:owner@example.invalid"}, {"value": "row.SEC.nport"}, {"value": "artifact.xlsx"}, {"value": "data.avro"}, {"value": "snapshot.gz"}, {"value": "report.parquet?download=1"}, {"value": "sha256:" + "a" * 64}, {"value": "sha512:" + "a" * 128}, {"value": "A" * 43 + "="}, {"value": "A" * 86 + "=="}, {"value": ("not-json",)}, {"value": float("nan")}):
        with pytest.raises(AssertionError):
            _assert_future_public(value)


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
    code = main(["fixture-run", "--source-manifest", str(tmp_path / "missing.json"), "--source-approval", str(tmp_path / "missing-approval.json"), "--fixture", str(tmp_path / "fixture.json"), "--mapping", "tests/bond_pilot/fixtures/debt-mapping-test-v1.json", "--run-dir", str(tmp_path / "stop")])
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
        values["checkpoint_path"].write_text('{"checkpoint":"retained"}', encoding="utf-8")
        raise PilotError("unsafe_query_plan")
    monkeypatch.setattr(workflow, "run_v2_calibration", stop)
    with pytest.raises(PilotError, match="unsafe_query_plan"):
        run_calibration(**kwargs)
    final = kwargs["run_dir"]
    assert (final / "checkpoint.json").is_file()
    assert (final / "stop-report.json").is_file()
    assert (final / "checksums.sha256").is_file()
    assert not list(tmp_path.glob(".calibration.*.partial-dir"))


def test_calibration_success_pack_is_atomic_and_binds_internal_provenance(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def succeed(_connection, **values):
        values["rows"] = (_calibration_row(),)
        values["checkpoint_path"].write_bytes(_checkpoint(values))
        return SimpleNamespace(rows=values["rows"], rows_read=1, pages=1, partial=False, last_key=None)
    monkeypatch.setattr(workflow, "run_v2_calibration", succeed)
    report = run_calibration(**kwargs)
    final = kwargs["run_dir"]
    assert report["rows_artifact"] == "calibration-rows-v1.json"
    provenance = json.loads((final / "calibration-provenance.json").read_text(encoding="utf-8"))
    assert provenance["internal_only"] is True
    assert provenance["source"]["approval"]["source_locator"]
    assert provenance["mapping"]["observed_values_sha256"] == "c" * 64
    assert provenance["mapping"]["approval"]["approved_by"] == "reviewer"
    assert provenance["phase4"]["evidence_sha256"] and provenance["phase4"]["approval_sha256"]
    assert provenance["phase4"]["evidence"]["approved_by"] == "phase4 reviewer"
    assert provenance["phase4"]["approval"]["approved_modes"] == ["calibration"]
    checkpoint = json.loads((final / "checkpoint.json").read_text(encoding="utf-8"))
    assert report["output_hash"] == checkpoint["output_hash"] == "f" * 64
    assert report["checkpoint_sha256"] == hashlib.sha256((final / "checkpoint.json").read_bytes()).hexdigest()
    rows = json.loads((final / "calibration-rows-v1.json").read_text(encoding="utf-8"))
    assert rows["columns"] == list(REQUIRED_COLUMNS)
    assert rows["rows"][0][REQUIRED_COLUMNS.index("signed_market_value")] == {"type": "decimal", "value": "10.250"}
    assert (final / "checkpoint.json").is_file() and (final / "checksums.sha256").is_file()
    assert not list(tmp_path.glob(".calibration.*.partial-dir"))


def test_calibration_late_write_failure_leaves_no_final_or_staging_and_retry_succeeds(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def succeed(_connection, **values):
        values["rows"] = ()
        values["checkpoint_path"].write_bytes(_checkpoint(values))
        return SimpleNamespace(rows=(), rows_read=0, pages=1, partial=False, last_key=None)
    monkeypatch.setattr(workflow, "run_v2_calibration", succeed)
    original = workflow.write_json_once
    def fail_late(path: Path, value: object):
        if path.name == "calibration-report.json":
            raise RuntimeError("late write")
        return original(path, value)
    monkeypatch.setattr(workflow, "write_json_once", fail_late)
    with pytest.raises(RuntimeError, match="late write"):
        run_calibration(**kwargs)
    assert not kwargs["run_dir"].exists() and not list(tmp_path.glob(".calibration.*.partial-dir"))
    monkeypatch.setattr(workflow, "write_json_once", original)
    assert run_calibration(**kwargs)["internal_only"] is True


def test_publish_races_preserve_existing_winner_for_calibration_and_stop(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def succeed(_connection, **values):
        values["rows"] = ()
        values["checkpoint_path"].write_bytes(_checkpoint(values))
        return SimpleNamespace(rows=(), rows_read=0, pages=1, partial=False, last_key=None)
    monkeypatch.setattr(workflow, "run_v2_calibration", succeed)
    def winner(_staging: Path, output: Path) -> None:
        output.mkdir()
        (output / "sentinel").write_text("winner", encoding="utf-8")
        raise PilotError("already_exists")
    monkeypatch.setattr(workflow, "_publish", winner)
    with pytest.raises(PilotError, match="already_exists"):
        run_calibration(**kwargs)
    assert (kwargs["run_dir"] / "sentinel").read_text(encoding="utf-8") == "winner"
    stop = tmp_path / "stop"
    workflow.write_stop_report(stop, PilotError("stopped"))
    assert (stop / "sentinel").read_text(encoding="utf-8") == "winner"
    assert not list(tmp_path.glob(".stop.*.partial-dir"))


@pytest.mark.parametrize("bad", [object(), float("nan"), {"nested": "no"}])
def test_calibration_row_serialization_fails_closed_with_typed_stop(tmp_path: Path, make_source_zip, monkeypatch: pytest.MonkeyPatch, bad: object) -> None:
    kwargs = {**_calibration_documents(tmp_path, make_source_zip, monkeypatch), "mode": "calibration", "series_ids": ("series-1",), "run_dir": tmp_path / "calibration"}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def succeed(_connection, **values):
        values["rows"] = (_calibration_row(cusip=bad),)
        values["checkpoint_path"].write_bytes(_checkpoint(values))
        return SimpleNamespace(rows=values["rows"], rows_read=1, pages=1, partial=False, last_key=None)
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
    prior.mkdir()
    values = {"evidence": evidence, "approval": evidence_approval, "mode": "calibration", "series_ids": series, "rows": ()}
    checkpoint = _checkpoint(values, output_hash=hashlib.sha256(b"").hexdigest(), state="stopped", reason="unsafe_query_plan", run_id="original-run")
    (prior / "checkpoint.json").write_bytes(checkpoint)
    (prior / "calibration-provenance.json").write_text(json.dumps(provenance), encoding="utf-8")
    (prior / "stop-report.json").write_text(json.dumps({"internal_only": True, "status": "stopped", "code": "unsafe_query_plan"}), encoding="utf-8")
    from src.bond_pilot.artifacts import write_checksums
    write_checksums(prior)
    before = {path.name: path.read_bytes() for path in prior.iterdir()}
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: "dsn")
    monkeypatch.setattr(workflow, "connect", lambda _dsn: _Connection())
    def resumed(_connection, **values):
        values["checkpoint_path"].write_bytes(_checkpoint({**values, "rows": ()}, run_id="original-run"))
        return SimpleNamespace(rows=(), rows_read=0, pages=1, partial=False, last_key=None)
    monkeypatch.setattr(workflow, "run_v2_calibration", resumed)
    result = run_calibration(**kwargs, resume_pack=prior)
    assert result["resume_pack_checksums_sha256"] == hashlib.sha256(before["checksums.sha256"]).hexdigest()
    assert before == {path.name: path.read_bytes() for path in prior.iterdir()}
    tampered = tmp_path / "tampered"
    tampered.mkdir()
    for name, contents in before.items():
        (tampered / name).write_bytes(contents)
    (tampered / "checkpoint.json").write_text("{}", encoding="utf-8")
    calls: list[str] = []
    monkeypatch.setattr(workflow, "resolve_dsn", lambda: calls.append("resolve") or "dsn")
    with pytest.raises(PilotError, match="calibration_resume_invalid"):
        run_calibration(**{**kwargs, "run_dir": tmp_path / "tampered-output"}, resume_pack=tampered)
    assert calls == []
