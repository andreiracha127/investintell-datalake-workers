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


def test_authorization_fingerprint_binds_complete_artifact_command_and_run_directory(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, authorization_fingerprint, load_execution_authorization

    artifact = _authorization()
    artifact["run_directory"] = str((tmp_path / "run").resolve())
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")

    loaded = load_execution_authorization(
        path, code_sha="code-v1", inventory_hash="inventory-v1",
        run_directory=tmp_path / "run", command=("historical-backfill", "start"),
    )

    assert loaded["authorization_fingerprint"] == authorization_fingerprint(artifact)
    changed = {**artifact, "writable_tables": ["sec_raw.changed"]}
    path.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(BackfillSafetyError, match="run directory|command"):
        load_execution_authorization(path, code_sha="code-v1", inventory_hash="inventory-v1", run_directory=tmp_path / "other", command=("historical-backfill", "resume"))
    assert authorization_fingerprint(changed) != authorization_fingerprint(artifact)


def test_authorization_and_connected_writable_tables_reject_current_namespace_and_duplicates(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, load_execution_authorization

    artifact = _authorization()
    path = tmp_path / "authorization.json"
    for writable in (["sec_current.other"], ["provider_pointer"], ["sec_raw.nport_filings", "sec_raw.nport_filings"]):
        path.write_text(json.dumps({**artifact, "writable_tables": writable}), encoding="utf-8")
        with pytest.raises(BackfillSafetyError, match="writable"):
            load_execution_authorization(path, code_sha="code-v1", inventory_hash="inventory-v1")


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


class _Cursor:
    def __init__(self, events: list[str], busy: bool = False) -> None:
        self.events = events
        self.busy = busy
        self.query = ""

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, _params: object = None) -> None:
        self.query = query
        self.events.append("try_lock" if "pg_try_advisory_lock" in query else "unlock")

    def fetchone(self) -> tuple[bool]:
        return (not self.busy,)


class _LockConnection(_Connection):
    def __init__(self, events: list[str], busy: bool = False) -> None:
        super().__init__()
        self.events = events
        self.busy = busy

    def cursor(self) -> _Cursor:
        return _Cursor(self.events, self.busy)


def _single_package_inventory(tmp_path: Path) -> tuple[dict[str, object], Path]:
    from src.sec_regulatory.historical_backfill import SourceSpec, build_inventory

    root = tmp_path / "nport"
    package = root / "2024q1_nport"
    package.mkdir(parents=True)
    (package / "submission.tsv").write_text("a\tb\n1\t2\n", encoding="utf-8")
    return build_inventory((SourceSpec("nport", root, 1),)), package


def test_authorized_executor_verifies_target_then_dispatches_exact_manifest_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, package_path = _single_package_inventory(tmp_path)
    artifact = _authorization(inventory_hash=str(inventory["inventory_hash"]))
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text(json.dumps(artifact), encoding="utf-8")
    connection = _LockConnection([])
    calls: list[tuple[Path, Path]] = []
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "nport:fixed-digest")

    executor = backfill.build_authorized_executor(
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


def test_authorized_executor_uses_nonblocking_form_lock_before_schema_install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(_authorization(inventory_hash=str(inventory["inventory_hash"]))), encoding="utf-8")
    events: list[str] = []
    connection = _LockConnection(events)
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "nport:fixed-digest")
    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {"database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test", "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27", "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"]},
        schema_installers={"manifest": lambda _conn: events.append("manifest"), "nport": lambda _conn: events.append("form")},
        dispatchers={"nport": lambda _conn, *, package, source_root: {"package": package.relative_to(source_root).as_posix(), "state": "raw_validated", "rows": 0, "run_id": "run-1"}},
    )

    executor(dict(inventory["packages"][0]))

    assert events == ["try_lock", "manifest", "form", "unlock"]


def test_lock_busy_refuses_before_schema_or_dispatch_and_rolls_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(_authorization(inventory_hash=str(inventory["inventory_hash"]))), encoding="utf-8")
    events: list[str] = []
    connection = _LockConnection(events, busy=True)
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "nport:fixed-digest")
    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {"database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test", "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27", "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"]},
        schema_installers={"manifest": lambda _conn: events.append("manifest"), "nport": lambda _conn: events.append("form")},
        dispatchers={"nport": lambda *_args, **_kwargs: pytest.fail("busy lock must not dispatch")},
    )

    with pytest.raises(backfill.BackfillSafetyError, match="lock_busy"):
        executor(dict(inventory["packages"][0]))

    assert events == ["try_lock"]
    assert connection.rollbacks == 1
    assert connection.closed


def test_executor_commits_only_valid_explicit_failure_with_fixed_reason(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(_authorization(inventory_hash=str(inventory["inventory_hash"]))), encoding="utf-8")
    connection = _LockConnection([])
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "nport:fixed-digest")
    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {"database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test", "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27", "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"]},
        schema_installers={"manifest": lambda _conn: None, "nport": lambda _conn: None},
        dispatchers={"nport": lambda _conn, *, package, source_root: {"package": package.relative_to(source_root).as_posix(), "state": "failed", "reason": "postgresql://user:secret@host/db"}},
    )

    assert executor(dict(inventory["packages"][0])) == {"state": "failed", "reason_code": "ingester_failed"}
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed


def test_supervisor_persists_fixed_refusal_code_and_nonsecret_error_digest(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"
    result = run_supervisor(
        inventory, status_path=status_path, code_sha="code-v1", lease_owner="unit-test",
        execute_package=lambda _package: {"state": "failed", "reason_code": "lock_busy", "dsn": "postgresql://fake:secret@localhost/test"},
    )

    assert result["reason"] == "lock_busy"
    record = json.loads(status_path.read_text(encoding="utf-8"))["packages"][inventory["packages"][0]["identity"]]
    assert record["reason_code"] == "lock_busy"
    assert len(record["error_digest"]) == 64
    assert "secret" not in status_path.read_text(encoding="utf-8")


def test_supervisor_distinguishes_executor_lock_refusal_from_source_drift(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    outcome = run_supervisor(
        inventory, status_path=tmp_path / "run" / "status.json", code_sha="code-v1", lease_owner="unit-test",
        execute_package=lambda _package: (_ for _ in ()).throw(BackfillSafetyError("lock_busy")),
    )

    assert outcome["reason"] == "lock_busy"


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
        authorization_fingerprint = "a" * 64
        authorization_lineage = {"authorization_id": "auth-one", "sanitized_command": ["historical-backfill", "start"]}
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


def test_cli_lineage_persists_fingerprint_and_refuses_authorization_omission(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backfill, "build_historical_inventory", lambda: inventory)
    monkeypatch.setattr(backfill, "_validate_historical_boundary", lambda _inventory: None)
    monkeypatch.setattr(backfill, "code_identity", lambda: "code-v1")
    seen: dict[str, object] = {}

    class Executor:
        authorization_id = "auth-one"
        authorization_fingerprint = "f" * 64
        authorization_lineage = {"authorization_id": "auth-one", "target_mode": "local_disposable"}
        target_identity = {"database": "sec_backfill_test"}

        def __call__(self, package: dict[str, object]) -> dict[str, object]:
            return {"package": package["relative_package_path"], "state": "raw_validated", "rows": 0, "run_id": "run-1"}

    def build(*_args: object, **kwargs: object) -> Executor:
        seen.update(kwargs)
        return Executor()

    monkeypatch.setattr(backfill, "build_authorized_executor", build)
    run_dir = tmp_path / "run"
    assert backfill.cli(["start", "--run-dir", str(run_dir), "--execution-authorization", str(authorization_path)]) == 0
    status = json.loads((run_dir / "status.json").read_text(encoding="utf-8"))
    assert status["authorization_fingerprint"] == "f" * 64
    assert status["packages"][inventory["packages"][0]["identity"]]["authorization_fingerprint"] == "f" * 64
    assert seen["run_directory"] == run_dir
    assert seen["command"] == ("historical-backfill", "start")

    with pytest.raises(backfill.BackfillSafetyError, match="authorization"):
        backfill.cli(["resume", "--run-dir", str(run_dir)])


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


def test_authorized_cli_uses_sublease_heartbeat_and_surfaces_renewal_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backfill, "build_historical_inventory", lambda: inventory)
    monkeypatch.setattr(backfill, "_validate_historical_boundary", lambda _inventory: None)
    monkeypatch.setattr(backfill, "code_identity", lambda: "code-v1")
    monkeypatch.setattr(backfill, "AUTHORIZED_HEARTBEAT_INTERVAL_SECONDS", 0.01)

    class Executor:
        authorization_id = "auth-one"
        authorization_fingerprint = "f" * 64
        authorization_lineage = {"authorization_id": "auth-one"}
        target_identity = {"database": "sec_backfill_test"}

        def __call__(self, package: dict[str, object]) -> dict[str, object]:
            time.sleep(0.04)
            return {"package": package["relative_package_path"], "state": "raw_validated", "rows": 0, "run_id": "run-1"}

    monkeypatch.setattr(backfill, "build_authorized_executor", lambda *_args, **_kwargs: Executor())
    monkeypatch.setattr(backfill, "heartbeat", lambda *_args, **_kwargs: (_ for _ in ()).throw(backfill.BackfillSafetyError("renewal failure")))

    assert backfill.cli(["start", "--run-dir", str(tmp_path / "run"), "--execution-authorization", str(authorization_path)]) == 1
    status = json.loads((tmp_path / "run" / "status.json").read_text(encoding="utf-8"))
    assert status["packages"][inventory["packages"][0]["identity"]]["reason_code"] == "heartbeat_renewal_failed"
