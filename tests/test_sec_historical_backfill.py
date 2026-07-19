import json
import hashlib
from datetime import UTC, datetime
import multiprocessing
from pathlib import Path
import sys
import time

import pytest


def _single_package_inventory(tmp_path: Path) -> tuple[dict[str, object], Path]:
    from src.sec_regulatory.historical_backfill import SourceSpec, build_inventory

    root = tmp_path / "nport"
    package = root / "2024q1_nport"
    package.mkdir(parents=True)
    (package / "submission.tsv").write_text("a\tb\n1\t2\n", encoding="utf-8")
    return build_inventory((SourceSpec("nport", root, 1),)), package


def _concurrent_supervisor(status_path: str, inventory: dict[str, object], counter_path: str) -> None:
    from src.sec_regulatory.historical_backfill import run_supervisor

    def execute(_package: dict[str, object]) -> dict[str, str]:
        with Path(counter_path).open("a", encoding="utf-8") as counter:
            counter.write("executed\n")
        time.sleep(0.2)
        return {"state": "raw_validated"}

    run_supervisor(inventory, status_path=Path(status_path), code_sha="code-v1", execute_package=execute, lease_owner=f"pid-{__import__('os').getpid()}")


def test_inventory_is_deterministic_and_rejects_root_or_count_drift(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, SourceSpec, build_inventory

    root = tmp_path / "nport"
    package = root / "2024q1_nport"
    package.mkdir(parents=True)
    (package / "submission.tsv").write_text("a\tb\n1\t2\n", encoding="utf-8")
    spec = SourceSpec("nport", root, 1)

    first = build_inventory((spec,))
    second = build_inventory((spec,))

    assert first == second
    assert first["packages"][0]["form"] == "nport"
    assert first["packages"][0]["files"][0]["relative_path"] == "submission.tsv"
    with pytest.raises(BackfillSafetyError, match="package count drift"):
        build_inventory((SourceSpec("nport", root, 2),))
    with pytest.raises(BackfillSafetyError, match="root drift"):
        build_inventory((SourceSpec("nport", root / "different", 1),))


def test_local_canary_target_rejects_production_or_uncertain_identity() -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, validate_canary_target

    target = {
        "host": "localhost",
        "resolved_addresses": ["127.0.0.1", "::1"],
        "database": "sec_backfill_test",
        "role": "sec_backfill_test",
        "secret_source": "local-disposable-fixture",
    }
    assert validate_canary_target(target)["database"] == "sec_backfill_test"
    for field, value in (("host", "timescale-sp"), ("database", "market"), ("role", "production_writer"), ("secret_source", "gcloud-secret")):
        rejected = {**target, field: value}
        with pytest.raises(BackfillSafetyError):
            validate_canary_target(rejected)


def test_resume_skips_only_matching_terminal_successes_and_stops_at_first_failure(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    package = inventory["packages"][0]
    status_path = tmp_path / "status.json"
    status_path.write_text(json.dumps({
        "inventory_hash": inventory["inventory_hash"],
        "code_sha": "code-v1",
        "packages": {
            package["identity"]: {"state": "raw_validated", "package_sha256": package["package_sha256"], "inventory_hash": inventory["inventory_hash"], "code_sha": "code-v1"}
        },
    }), encoding="utf-8")
    result = run_supervisor(inventory, status_path=status_path, code_sha="code-v1", execute_package=lambda _package: (_ for _ in ()).throw(AssertionError("terminal source must be skipped")), lease_owner="unit-test")

    assert result["state"] == "ok"
    durable = json.loads(status_path.read_text(encoding="utf-8"))
    assert durable["packages"][package["identity"]]["state"] == "raw_validated"


def test_dispatcher_routes_historical_backfill_without_resolving_a_database(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import src.run as dispatcher

    called: list[list[str]] = []
    monkeypatch.setattr(sys, "argv", ["run", "historical-backfill", "status", "--run-dir", str(tmp_path)])
    monkeypatch.setattr(dispatcher, "historical_backfill_cli", lambda argv: called.append(list(argv)) or 7, raising=False)

    assert dispatcher.main() == 7
    assert called == [["status", "--run-dir", str(tmp_path)]]


def test_heartbeat_renews_only_the_foreground_active_package_lease(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, heartbeat

    status_path = tmp_path / "status.json"
    status_path.write_text(json.dumps({
        "active_package": "rr1:2024Q1:package",
        "active_attempt": 2,
        "lease": {"owner": "unit-owner", "expires_at": "2024-01-01T00:00:00+00:00"},
    }), encoding="utf-8")

    renewed = heartbeat(status_path, lease_owner="unit-owner", active_attempt=2, lease_seconds=30, now=datetime(2024, 1, 1, tzinfo=UTC))

    assert renewed["active_package"] == "rr1:2024Q1:package"
    assert renewed["active_attempt"] == 2
    assert renewed["lease"]["expires_at"] == "2024-01-01T00:00:30+00:00"
    with pytest.raises(BackfillSafetyError, match="lease owner"):
        heartbeat(status_path, lease_owner="different-owner", active_attempt=2)


@pytest.mark.parametrize("tamper", ("hash", "duplicate"))
def test_supervisor_rejects_forged_or_ambiguous_inventory_before_execution(tmp_path: Path, tamper: str) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    if tamper == "hash":
        inventory["inventory_hash"] = "forged"
    else:
        inventory["packages"] = [*inventory["packages"], dict(inventory["packages"][0])]
        canonical = {key: value for key, value in inventory.items() if key != "inventory_hash"}
        inventory["inventory_hash"] = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")).hexdigest()
    with pytest.raises(BackfillSafetyError):
        run_supervisor(inventory, status_path=tmp_path / "run" / "status.json", code_sha="code-v1", execute_package=lambda _package: {"state": "raw_validated"}, lease_owner="unit-test")


def test_supervisor_fails_closed_when_a_package_changes_after_inventory(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import run_supervisor

    inventory, package = _single_package_inventory(tmp_path)

    def mutate(_package: dict[str, object]) -> dict[str, str]:
        (package / "submission.tsv").write_text("changed\n", encoding="utf-8")
        return {"state": "raw_validated"}

    result = run_supervisor(inventory, status_path=tmp_path / "run" / "status.json", code_sha="code-v1", execute_package=mutate, lease_owner="unit-test")

    assert result["state"] == "failed"
    assert result["reason"] == "source_drift"


def test_supervisor_never_persists_dsn_from_executor_result_or_exception(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"
    result = run_supervisor(inventory, status_path=status_path, code_sha="code-v1", execute_package=lambda _package: {"state": "failed", "dsn": "postgresql://user:secret@example.test/db"}, lease_owner="unit-test")

    assert result["state"] == "failed"
    assert "secret" not in status_path.read_text(encoding="utf-8")
    assert "postgresql" not in status_path.read_text(encoding="utf-8")


def test_canary_target_rejects_application_and_shared_roles() -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, validate_canary_target

    target = {"host": "localhost", "resolved_addresses": ["127.0.0.1"], "database": "sec_backfill_test", "role": "sec_backfill_test", "secret_source": "local-disposable-fixture"}
    for field, value in (("database", "app"), ("role", "worker_writer")):
        with pytest.raises(BackfillSafetyError):
            validate_canary_target({**target, field: value})


def test_status_rejects_a_run_directory_inside_the_repository(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    monkeypatch.setattr(backfill, "DEFAULT_RUN_DIR", tmp_path)
    with pytest.raises(backfill.BackfillSafetyError, match="outside Git"):
        backfill.cli(["status", "--run-dir", str(Path(backfill.__file__).resolve().parents[2])])


@pytest.mark.parametrize(("stats", "expected"), (({"state": "failed"}, 1), ({"rows": 4}, 0), ({"state": "risk_on"}, 0), (None, 0)))
def test_dispatcher_preserves_legacy_success_shapes_and_fails_only_explicit_failure(monkeypatch: pytest.MonkeyPatch, stats: object, expected: int) -> None:
    import types
    import src.run as dispatcher

    monkeypatch.setattr(sys, "argv", ["run", "rr1_ingestion"])
    monkeypatch.setattr(dispatcher.importlib, "import_module", lambda _name: types.SimpleNamespace(run=lambda *_args, **_kwargs: stats))
    monkeypatch.setattr(dispatcher, "resolve_dsn", lambda: "unit-test-dsn")

    assert dispatcher.main() == expected


def test_inventory_round_trip_keeps_security_filename_and_command_redacts_postgresql_dsn(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import SourceSpec, _sanitize_command, _write_inventory, _load_status, build_inventory

    inventory, package = _single_package_inventory(tmp_path)
    (package / "security.tsv").write_text("safe\n", encoding="utf-8")
    inventory = build_inventory((SourceSpec("nport", package.parent, 1),))
    inventory_path = tmp_path / "inventory.json"
    _write_inventory(inventory_path, inventory)

    assert _load_status(inventory_path) == inventory
    assert _sanitize_command(("run", "postgresql://user:secret@example.test/db")) == ["run", "[redacted]"]


def test_supervisor_cross_process_lock_allows_exactly_one_executor(tmp_path: Path) -> None:
    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"
    counter_path = tmp_path / "executions.txt"
    context = multiprocessing.get_context("spawn")
    processes = [context.Process(target=_concurrent_supervisor, args=(str(status_path), inventory, str(counter_path))) for _ in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(15)
    assert all(process.exitcode == 0 for process in processes)
    assert counter_path.read_text(encoding="utf-8").splitlines() == ["executed"]


def test_inventory_rejects_symlink_package_escape(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, SourceSpec, build_inventory

    root = tmp_path / "nport"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "submission.tsv").write_text("x\n", encoding="utf-8")
    link = root / "2024q1_nport"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
    with pytest.raises(BackfillSafetyError, match="symlink|escapes"):
        build_inventory((SourceSpec("nport", root, 1),))


def test_start_rejects_existing_external_run_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    (tmp_path / "status.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backfill, "build_historical_inventory", lambda: (_ for _ in ()).throw(AssertionError("start must reject before inventory")))
    with pytest.raises(backfill.BackfillSafetyError, match="empty external"):
        backfill.cli(["start", "--run-dir", str(tmp_path)])


def test_resume_rejects_arbitrary_fixture_roots_at_the_historical_boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    (tmp_path / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")
    (tmp_path / "status.json").write_text(json.dumps({"schema_version": 1, "inventory_hash": inventory["inventory_hash"], "code_sha": "stale"}), encoding="utf-8")
    monkeypatch.setattr(backfill, "code_identity", lambda: "current")
    with pytest.raises(backfill.BackfillSafetyError, match="immutable 82-package"):
        backfill.cli(["resume", "--run-dir", str(tmp_path)])
