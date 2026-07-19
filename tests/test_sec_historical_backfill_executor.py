import json
import os
from pathlib import Path
import time

import pytest


def _authorization(*, code_sha: str = "code-v1", inventory_hash: str = "inventory-v1") -> dict[str, object]:
    return {
        "schema_version": 1,
        "stage": "phase4_historical_backfill",
        "code_sha": code_sha,
        "inventory_hash": inventory_hash,
        "target_mode": "local_disposable",
        "dsn_env_var": "SEC_BACKFILL_FAKE_DSN",
        "target": {
            "project": "local-disposable",
            "vm": "local-disposable",
            "zone": "local-disposable",
            "host": "localhost",
            "resolved_addresses": ["127.0.0.1"],
            "database": "sec_backfill_test",
            "server_address": "127.0.0.1",
            "role": "sec_backfill_test",
            "secret_source": "pytest-disposable-fixture",
            "postgresql_identity": "PostgreSQL 18",
            "timescaledb_identity": "TimescaleDB 2.27",
        },
        "writable_tables": ["sec_raw.nport_filings"],
        "pointer_table_denylist": ["sec_current.provider_pointer"],
        "sanitized_command": ["historical-backfill", "start"],
        "run_directory": "E:/runs/fake",
        "authorization_id": "auth-local-001",
        "stop_contract_hash": "a" * 64,
        "reconciliation_contract_hash": "b" * 64,
    }


def test_execution_authorization_requires_exact_schema_and_matches_code_and_inventory(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, load_execution_authorization

    artifact = _authorization()
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")

    loaded = load_execution_authorization(path, code_sha="code-v1", inventory_hash="inventory-v1")

    assert loaded["authorization_id"] == "auth-local-001"
    with pytest.raises(BackfillSafetyError, match="authorization schema"):
        path.write_text(json.dumps({**artifact, "unexpected": True}), encoding="utf-8")
        load_execution_authorization(path, code_sha="code-v1", inventory_hash="inventory-v1")
    with pytest.raises(BackfillSafetyError, match="code SHA"):
        path.write_text(json.dumps(artifact), encoding="utf-8")
        load_execution_authorization(path, code_sha="other", inventory_hash="inventory-v1")


def test_invalid_authorization_never_resolves_the_dsn_environment_variable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, build_authorized_executor

    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(_authorization(code_sha="wrong")), encoding="utf-8")
    seen: list[str] = []
    monkeypatch.setattr(os, "environ", {"SEC_BACKFILL_FAKE_DSN": "postgresql://never:read@localhost/test"})

    with pytest.raises(BackfillSafetyError, match="code SHA"):
        build_authorized_executor(path, inventory={"inventory_hash": "inventory-v1"}, code_sha="code-v1", connection_factory=lambda value: seen.append(value))

    assert seen == []


class _Connection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def _single_package_inventory(tmp_path: Path) -> tuple[dict[str, object], Path]:
    from src.sec_regulatory.historical_backfill import SourceSpec, build_inventory

    root = tmp_path / "nport"
    package = root / "2024q1_nport"
    package.mkdir(parents=True)
    (package / "submission.tsv").write_text("a\tb\n1\t2\n", encoding="utf-8")
    return build_inventory((SourceSpec("nport", root, 1),)), package


def test_authorized_executor_verifies_target_then_dispatches_exact_manifest_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.sec_regulatory.historical_backfill import build_authorized_executor

    inventory, package_path = _single_package_inventory(tmp_path)
    artifact = _authorization(inventory_hash=str(inventory["inventory_hash"]))
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text(json.dumps(artifact), encoding="utf-8")
    connection = _Connection()
    calls: list[tuple[Path, Path]] = []
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")

    executor = build_authorized_executor(
        authorization_path,
        inventory=inventory,
        code_sha="code-v1",
        connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {
            "database": "sec_backfill_test",
            "server_address": "127.0.0.1",
            "role": "sec_backfill_test",
            "postgresql_identity": "PostgreSQL 18",
            "timescaledb_identity": "TimescaleDB 2.27",
            "is_superuser": False,
            "owns_any_table": False,
            "writable_tables": ["sec_raw.nport_filings"],
        },
        schema_installers={"manifest": lambda _conn: None, "nport": lambda _conn: None},
        dispatchers={"nport": lambda _conn, *, package, source_root: calls.append((package, source_root)) or {"package": package.relative_to(source_root).as_posix(), "state": "raw_validated", "rows": 3, "run_id": "run-1"}},
    )

    result = executor(dict(inventory["packages"][0]))

    assert result == {"state": "raw_validated", "rows": 3, "run_id": "run-1"}
    assert calls == [(package_path, package_path.parent)]
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed


@pytest.mark.parametrize(
    "actual",
    (
        {"database": "market"},
        {"server_address": "10.0.0.1"},
        {"role": "postgres"},
        {"is_superuser": True},
        {"owns_any_table": True},
        {"writable_tables": ["sec_raw.nport_filings", "sec_current.provider_pointer"]},
    ),
)
def test_authorized_executor_refuses_identity_or_privilege_drift_before_schema_install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, actual: dict[str, object]) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, build_authorized_executor

    inventory, _ = _single_package_inventory(tmp_path)
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text(json.dumps(_authorization(inventory_hash=str(inventory["inventory_hash"]))), encoding="utf-8")
    connection = _Connection()
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    inspected = {
        "database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test",
        "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27",
        "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"],
        **actual,
    }
    installed: list[str] = []
    executor = build_authorized_executor(
        authorization_path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: inspected,
        schema_installers={"manifest": lambda _conn: installed.append("manifest"), "nport": lambda _conn: installed.append("nport")},
        dispatchers={"nport": lambda *_args, **_kwargs: pytest.fail("dispatch must not run")},
    )

    with pytest.raises(BackfillSafetyError, match="target|privilege|writable"):
        executor(dict(inventory["packages"][0]))

    assert installed == []
    assert connection.rollbacks == 1
    assert connection.closed


def test_cli_binds_explicit_authorization_to_status_and_rejects_resume_drift(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backfill, "build_historical_inventory", lambda: inventory)
    monkeypatch.setattr(backfill, "_validate_historical_boundary", lambda _inventory: None)
    monkeypatch.setattr(backfill, "code_identity", lambda: "code-v1")

    class Executor:
        authorization_id = "auth-one"
        target_identity = {"kind": "local_disposable", "database": "sec_backfill_test"}

        def __call__(self, package: dict[str, object]) -> dict[str, object]:
            return {"package": package["relative_package_path"], "state": "raw_validated", "rows": 1}

    monkeypatch.setattr(backfill, "build_authorized_executor", lambda *_args, **_kwargs: Executor())
    assert backfill.cli(["start", "--run-dir", str(tmp_path / "run"), "--execution-authorization", str(authorization_path)]) == 0
    status = json.loads((tmp_path / "run" / "status.json").read_text(encoding="utf-8"))
    assert status["authorization_id"] == "auth-one"
    assert status["target_identity"] == {"kind": "local_disposable", "database": "sec_backfill_test"}

    class DriftedExecutor(Executor):
        authorization_id = "auth-two"

    monkeypatch.setattr(backfill, "build_authorized_executor", lambda *_args, **_kwargs: DriftedExecutor())
    with pytest.raises(backfill.BackfillSafetyError, match="authorization"):
        backfill.cli(["resume", "--run-dir", str(tmp_path / "run"), "--execution-authorization", str(authorization_path)])


def test_supervisor_heartbeats_a_blocking_authorized_executor_and_stops_after_completion(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"
    observed: list[tuple[str, str]] = []

    def blocking_executor(package: dict[str, object]) -> dict[str, object]:
        first = json.loads(status_path.read_text(encoding="utf-8"))["heartbeat_at"]
        time.sleep(0.05)
        observed.append((first, json.loads(status_path.read_text(encoding="utf-8"))["heartbeat_at"]))
        return {"package": package["relative_package_path"], "state": "raw_validated"}

    result = run_supervisor(
        inventory, status_path=status_path, code_sha="code-v1", execute_package=blocking_executor,
        lease_owner="unit-test", heartbeat_interval_seconds=0.01,
    )

    assert result["state"] == "ok"
    assert observed and observed[0][0] != observed[0][1]
    completed = json.loads(status_path.read_text(encoding="utf-8"))
    heartbeat_at_completion = completed["heartbeat_at"]
    time.sleep(0.03)
    assert json.loads(status_path.read_text(encoding="utf-8"))["heartbeat_at"] == heartbeat_at_completion
