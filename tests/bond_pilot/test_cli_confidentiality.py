from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from scripts.run_bond_pilot import build_parser, main
from src.bond_pilot.contracts import PilotError, SourceApproval
from src.bond_pilot.source_artifact import qualify_source
from src.bond_pilot.workflow import run_calibration, run_fixture


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


_PUBLIC_KEYS = {"value", "values", "unit", "units", "date", "as_of_date", "freshness", "quality", "availability", "methodology_version", "is_144a"}
_FORBIDDEN = {"source", "provider", "vendor", "upstream", "url", "file", "version", "row_id", "hash", "lineage", "license", "entitlement", "error", "trace", "osbap", "finra", "wrds", "sec"}


def _assert_future_public(value: object) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key).casefold()
            assert key_text in _PUBLIC_KEYS
            assert not any(token in key_text for token in _FORBIDDEN)
            _assert_future_public(child)
    elif isinstance(value, list):
        for child in value:
            _assert_future_public(child)
    elif isinstance(value, str):
        lowered = value.casefold()
        assert not any(token in lowered for token in _FORBIDDEN)


def test_future_public_allowlist_rejects_nested_provenance_and_source_family_literals() -> None:
    _assert_future_public({"value": 1.0, "is_144a": True, "quality": {"freshness": "daily"}})
    with pytest.raises(AssertionError):
        _assert_future_public({"quality": {"source_url": "https://example.invalid"}})
    with pytest.raises(AssertionError):
        _assert_future_public({"methodology_version": "trace-v1"})


def test_internal_manifest_retains_full_provenance_and_pilot_stays_out_of_public_surfaces() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "src" / "bond_pilot" / "reporting.py").read_text(encoding="utf-8")
    assert "source_candidate.to_json_mapping" in source and "internal_only" in source
    forbidden_roots = ("frontend", "api", "public", "backend/app")
    assert not [path for base in forbidden_roots if (root / base).exists() for path in (root / base).rglob("*bond_pilot*")]
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
