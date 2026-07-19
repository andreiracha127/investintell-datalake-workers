from __future__ import annotations

import json
import signal
from pathlib import Path

import pytest


def _sources(root: Path) -> tuple[object, ...]:
    from src.sec_regulatory.historical_backfill import SourceSpec

    result = []
    for form, directory, count in (("nport", "nport", 1), ("ncen", "ncen", 1), ("rr1", "RR1", 1)):
        package = root / directory / "2024q1_fixture"
        package.mkdir(parents=True)
        (package / "submission.tsv").write_text("ok\n", encoding="utf-8")
        result.append(SourceSpec(form, root / directory, count))
    return tuple(result)


def test_production_paths_reject_case_symlink_writable_source_and_state_overlap(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, SourceSpec, validate_production_paths

    source_mount = tmp_path / "srv" / "sec-corpus"
    sources = _sources(source_mount)
    state = tmp_path / "var" / "lib" / "sec-backfill" / "run-1"
    state.mkdir(parents=True)
    mount = {"read_only": True, "device": 11, "durable": True}
    state_mount = {"read_only": False, "device": 12, "durable": True}
    validate_production_paths(sources, state, source_mount=source_mount, state_root=state.parent, mount_inspector=lambda path: mount if path.is_relative_to(source_mount) else state_mount)

    wrong_case = (sources[0], sources[1], SourceSpec("rr1", source_mount / "rr1", 1))
    with pytest.raises(BackfillSafetyError, match="RR1|root"):
        validate_production_paths(wrong_case, state, source_mount=source_mount, state_root=state.parent, mount_inspector=lambda path: mount if path.is_relative_to(source_mount) else state_mount)
    with pytest.raises(BackfillSafetyError, match="read-only"):
        validate_production_paths(sources, state, source_mount=source_mount, state_root=state.parent, mount_inspector=lambda _path: {"read_only": False, "device": 11, "durable": True})
    with pytest.raises(BackfillSafetyError, match="different filesystem"):
        validate_production_paths(sources, state, source_mount=source_mount, state_root=state.parent, mount_inspector=lambda path: {"read_only": path.is_relative_to(source_mount), "device": 11, "durable": True})
    with pytest.raises(BackfillSafetyError, match="state"):
        validate_production_paths(sources, source_mount / "nport" / "run", source_mount=source_mount, state_root=source_mount, mount_inspector=lambda _path: mount)

    escaped = source_mount / "ncen" / "escape"
    try:
        escaped.symlink_to(tmp_path, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks unavailable in this test environment")
    with pytest.raises(BackfillSafetyError, match="symlink"):
        validate_production_paths(sources, state, source_mount=source_mount, state_root=state.parent, mount_inspector=lambda path: mount if path.is_relative_to(source_mount) else state_mount)


def test_durable_json_write_fsyncs_file_then_replace_then_parent_and_cleans_temp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    events: list[str] = []
    original_fsync = backfill.os.fsync
    original_replace = backfill.os.replace
    monkeypatch.setattr(backfill.os, "fsync", lambda fd: events.append("fsync") or original_fsync(fd))
    monkeypatch.setattr(backfill.os, "replace", lambda source, target: events.append("replace") or original_replace(source, target))
    monkeypatch.setattr(backfill, "_fsync_parent_directory", lambda _path: events.append("parent"))
    path = tmp_path / "state" / "status.json"
    backfill._write_status(path, {"schema_version": 1, "value": "safe"})
    assert events == ["fsync", "replace", "parent"]
    assert json.loads(path.read_text(encoding="utf-8"))["value"] == "safe"

    monkeypatch.setattr(backfill.os, "replace", lambda _source, _target: (_ for _ in ()).throw(OSError("replace failed")))
    with pytest.raises(OSError, match="replace failed"):
        backfill._write_status(path, {"schema_version": 1, "value": "safe"})
    assert not list(path.parent.glob(".status.json.*.tmp"))


def test_status_projects_authorization_and_rejects_bearer_pem_and_credentials(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, _write_status

    path = tmp_path / "status.json"
    _write_status(path, {"authorization_lineage": {"authorization_id": "auth-1", "target": {"project": "project", "vm": "vm", "zone": "zone", "database": "market", "server_address": "10.0.0.1", "role": "runner"}, "secret_version_resource": "projects/project/secrets/sec-backfill/versions/1", "preflight_attestation": {"opaque": "must-not-persist"}}})
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored["authorization_lineage"] == {"authorization_id": "auth-1", "target": {"project": "project", "vm": "vm", "zone": "zone", "database": "market", "server_address": "10.0.0.1", "role": "runner"}, "secret_version_resource": "projects/project/secrets/sec-backfill/versions/1"}
    for secret in ("Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig", "-----BEGIN PRIVATE KEY-----", "aws_access_key_id=AKIA1234567890123456", "host=db user=x password=y", "host=db passfile='/tmp/secret'", "token=opaque", "private-key=opaque"):
        with pytest.raises(BackfillSafetyError, match="credential"):
            _write_status(path, {"message": secret})


def test_signal_requests_boundary_stop_after_inflight_package_checkpoint(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import SourceSpec, build_inventory, install_boundary_stop_handlers, run_supervisor

    root = tmp_path / "nport"
    for name in ("2024q1_fixture", "2024q2_fixture"):
        package = root / name
        package.mkdir(parents=True)
        (package / "submission.tsv").write_text("ok\n", encoding="utf-8")
    inventory = build_inventory((SourceSpec("nport", root, 2),))
    seen: list[str] = []
    with install_boundary_stop_handlers() as stop:
        def execute(package: dict[str, object]) -> dict[str, object]:
            seen.append(str(package["identity"]))
            signal.raise_signal(signal.SIGTERM)
            return {"state": "raw_validated"}

        outcome = run_supervisor(inventory, status_path=tmp_path / "run" / "status.json", code_sha="code-v1", lease_owner="test", execute_package=execute, boundary_stop_requested=stop.is_set)
    assert outcome["state"] == "stopped"
    assert len(seen) == 1
    durable = json.loads((tmp_path / "run" / "status.json").read_text(encoding="utf-8"))
    assert durable["packages"][seen[0]]["state"] == "raw_validated"
