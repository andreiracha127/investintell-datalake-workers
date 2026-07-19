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


def test_controlled_boundary_crash_occurs_only_after_terminal_checkpoint(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"
    executed: list[bool] = []
    with pytest.raises(BackfillSafetyError, match="confirmed commit and checkpoint"):
        run_supervisor(
            inventory, status_path=status_path, code_sha="code-v1", lease_owner="unit-test",
            execute_package=lambda package: executed.append(True) or {"package": package["relative_package_path"], "state": "raw_validated", "rows": 0, "run_id": "committed-run"},
            controlled_boundary_crash=True,
        )
    record = json.loads(status_path.read_text(encoding="utf-8"))["packages"][inventory["packages"][0]["identity"]]
    assert executed == [True] and record["state"] == "raw_validated" and record["run_id"] == "committed-run"


def test_resume_rejects_mixed_canary_full_authorization_lineage(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"
    canary_lineage = {"execution_mode": "canary", "package_scope": [{"identity": "nport:2024Q1:fixture"}], "canary_certificate": None}
    run_supervisor(inventory, status_path=status_path, code_sha="code-v1", lease_owner="unit-test", execute_package=lambda package: {"package": package["relative_package_path"], "state": "raw_validated"}, authorization_id="auth", authorization_fingerprint="a" * 64, authorization_lineage=canary_lineage)
    with pytest.raises(BackfillSafetyError, match="fingerprint|lineage"):
        run_supervisor(inventory, status_path=status_path, code_sha="code-v1", lease_owner="unit-test", execute_package=lambda _package: pytest.fail("mixed lineage must not execute"), authorization_id="auth", authorization_fingerprint="b" * 64, authorization_lineage={**canary_lineage, "execution_mode": "full", "canary_certificate": {"certificate_id": "c"}})


def test_boundary_stop_is_bound_to_the_contract_and_starts_no_package(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"
    outcome = run_supervisor(inventory, status_path=status_path, code_sha="code-v1", lease_owner="unit-test", execute_package=lambda _package: pytest.fail("boundary stop must start no package"), stop_contract_hash="a" * 64, boundary_stop_requested=lambda: True)
    assert outcome["state"] == "stopped"
    durable = json.loads(status_path.read_text(encoding="utf-8"))
    assert durable["final_exit_state"] == "stopped_boundary" and durable["stop_contract_hash"] == "a" * 64
    with pytest.raises(BackfillSafetyError, match="stop contract"):
        run_supervisor(inventory, status_path=status_path, code_sha="code-v1", lease_owner="unit-test", execute_package=lambda _package: pytest.fail("mismatch must not execute"), stop_contract_hash="b" * 64)


def test_supervisor_blocks_and_retains_fence_on_ambiguous_commit(tmp_path: Path) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"

    class Executor:
        def execute_with_fence(self, package: dict[str, object], fence: object) -> object:
            evidence = {
                "identity": package["identity"], "inventory_package_sha256": package["package_sha256"],
                "package_sha256": "c" * 64, "package_id": "33333333-3333-4333-8333-333333333333",
                "run_id": "22222222-2222-4222-8222-222222222222",
                "supervisor_run_id": "11111111-1111-4111-8111-111111111111",
                "authorization_fingerprint": "a" * 64, "reconciliation_hash": "f" * 64,
            }
            fence("issued", evidence)  # type: ignore[operator]
            fence("ambiguous", evidence)  # type: ignore[operator]
            raise backfill.AmbiguousCommitError("uncertain")

    outcome = backfill.run_supervisor(
        inventory, status_path=status_path, code_sha="code-v1", lease_owner="unit-test",
        execute_package=Executor(), authorization_id="auth", authorization_fingerprint="a" * 64,
        supervisor_run_id="11111111-1111-4111-8111-111111111111",
    )

    durable = json.loads(status_path.read_text(encoding="utf-8"))
    record = durable["packages"][inventory["packages"][0]["identity"]]
    assert outcome["state"] == "blocked" and durable["final_exit_state"] == "blocked_ambiguous_commit"
    assert record["state"] == "ambiguous_commit" and record["commit_window"] == "ambiguous"
    assert durable["lease"] is not None and durable["active_package"] == inventory["packages"][0]["identity"]


def _protected_fence_evidence(package: dict[str, object]) -> dict[str, object]:
    return {
        "identity": package["identity"], "inventory_package_sha256": package["package_sha256"],
        "package_sha256": "c" * 64, "package_id": "33333333-3333-4333-8333-333333333333",
        "run_id": "22222222-2222-4222-8222-222222222222",
        "supervisor_run_id": "11111111-1111-4111-8111-111111111111",
        "authorization_fingerprint": "a" * 64, "reconciliation_hash": "f" * 64,
        "terminal_result": {"state": "raw_validated", "rows": 1, "run_id": "22222222-2222-4222-8222-222222222222", "reconciliation_hash": "f" * 64},
    }


def test_expired_issued_fence_after_uncatchable_commit_death_never_redispatches(tmp_path: Path) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"

    class DyingExecutor:
        def execute_with_fence(self, package: dict[str, object], fence: object) -> object:
            fence("issued", _protected_fence_evidence(package))  # type: ignore[operator]
            raise SystemExit("simulated process death after server COMMIT")

    with pytest.raises(SystemExit, match="server COMMIT"):
        backfill.run_supervisor(
            inventory, status_path=status_path, code_sha="code-v1", lease_owner="first",
            execute_package=DyingExecutor(), authorization_id="auth", authorization_fingerprint="a" * 64,
            supervisor_run_id="11111111-1111-4111-8111-111111111111",
        )
    crashed = json.loads(status_path.read_text(encoding="utf-8"))
    crashed["lease"]["expires_at"] = "2000-01-01T00:00:00+00:00"
    backfill._write_status(status_path, crashed)
    calls: list[str] = []

    outcome = backfill.run_supervisor(
        inventory, status_path=status_path, code_sha="code-v1", lease_owner="resume",
        execute_package=lambda package: calls.append(str(package["identity"])) or {"state": "raw_validated"},
        authorization_id="auth", authorization_fingerprint="a" * 64,
        supervisor_run_id="11111111-1111-4111-8111-111111111111",
    )

    durable = json.loads(status_path.read_text(encoding="utf-8"))
    record = durable["packages"][inventory["packages"][0]["identity"]]
    assert outcome["state"] == "blocked" and calls == []
    assert record["state"] == "recovery_required" and record["commit_window"] == "issued"


def test_confirmed_fence_with_terminal_fsync_failure_never_redispatches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"

    class ConfirmingExecutor:
        def execute_with_fence(self, package: dict[str, object], fence: object) -> object:
            evidence = _protected_fence_evidence(package)
            fence("issued", evidence)  # type: ignore[operator]
            fence("confirmed", evidence)  # type: ignore[operator]
            return evidence["terminal_result"]

    real_write_status = backfill._write_status
    writes = 0

    def fail_terminal_write(path: Path, status: dict[str, object]) -> None:
        nonlocal writes
        writes += 1
        if writes == 4:
            raise OSError("simulated terminal fsync failure")
        real_write_status(path, status)

    monkeypatch.setattr(backfill, "_write_status", fail_terminal_write)
    with pytest.raises(OSError, match="terminal fsync"):
        backfill.run_supervisor(
            inventory, status_path=status_path, code_sha="code-v1", lease_owner="first",
            execute_package=ConfirmingExecutor(), authorization_id="auth", authorization_fingerprint="a" * 64,
            supervisor_run_id="11111111-1111-4111-8111-111111111111",
        )
    monkeypatch.setattr(backfill, "_write_status", real_write_status)
    crashed = json.loads(status_path.read_text(encoding="utf-8"))
    crashed["lease"]["expires_at"] = "2000-01-01T00:00:00+00:00"
    real_write_status(status_path, crashed)
    calls: list[str] = []

    outcome = backfill.run_supervisor(
        inventory, status_path=status_path, code_sha="code-v1", lease_owner="resume",
        execute_package=lambda package: calls.append(str(package["identity"])) or {"state": "raw_validated"},
        authorization_id="auth", authorization_fingerprint="a" * 64,
        supervisor_run_id="11111111-1111-4111-8111-111111111111",
    )

    durable = json.loads(status_path.read_text(encoding="utf-8"))
    record = durable["packages"][inventory["packages"][0]["identity"]]
    assert outcome["state"] == "blocked" and calls == []
    assert record["state"] == "recovery_required" and record["commit_window"] == "confirmed"


def test_full_supervisor_seeds_three_promoted_canaries_and_executes_exactly_79(tmp_path: Path) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    specs = []
    for form, count in (("nport", 26), ("ncen", 17), ("rr1", 39)):
        root = tmp_path / form
        for index in range(count):
            package = root / f"2024q1_{form}_{index:02d}"
            package.mkdir(parents=True)
            (package / "source.tsv").write_text("x\n", encoding="utf-8")
        specs.append(backfill.SourceSpec(form, root, count))
    inventory = backfill.build_inventory(tuple(specs))
    promoted = {form: next(item for item in inventory["packages"] if item["form"] == form) for form in ("nport", "ncen", "rr1")}
    seeds = {
        item["identity"]: {
            "state": "canary_promoted", "attempt": 0, "package_sha256": item["package_sha256"],
            "run_id": f"0000000{index}-1111-4111-8111-111111111111", "reconciliation_hash": f"{index}" * 64,
            "package_transition_id": index,
        }
        for index, item in enumerate(promoted.values(), start=1)
    }
    executed: list[str] = []

    outcome = backfill.run_supervisor(
        inventory, status_path=tmp_path / "run" / "status.json", code_sha="code-v1", lease_owner="unit-test",
        execute_package=lambda package: executed.append(package["identity"]) or {"state": "raw_validated", "rows": 0},
        authorization_id="full", authorization_fingerprint="a" * 64,
        supervisor_run_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        execution_identities=[item["identity"] for item in inventory["packages"]], seeded_records=seeds,
    )

    assert outcome["state"] == "ok" and len(executed) == 79
    assert not set(executed) & set(seeds)


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


def test_sanitizer_redacts_postgres_scheme_but_preserves_harmless_security_fields() -> None:
    from src.sec_regulatory.historical_backfill import _redact, _sanitize_command

    assert _sanitize_command(("run", "postgresql://u:p@host/db")) == ["run", "[redacted]"]
    assert _redact({"security_note": "security review", "label": "security review"}) == {"security_note": "security review", "label": "security review"}


def test_historical_boundary_calls_root_validator_and_rejects_noncanonical_or_unbalanced_inventory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    specs = []
    for form, count in (("nport", 26), ("ncen", 17), ("rr1", 39)):
        root = tmp_path / form
        for index in range(count):
            package = root / f"2024q1_{form}_{index:02d}"
            package.mkdir(parents=True)
            (package / "source.tsv").write_text("x\n", encoding="utf-8")
        specs.append(backfill.SourceSpec(form, root, count))
    monkeypatch.setattr(backfill, "IMMUTABLE_SOURCES", tuple(specs))
    calls: list[bool] = []
    monkeypatch.setattr(backfill, "validate_immutable_roots", lambda: calls.append(True))
    inventory = backfill.build_inventory(tuple(specs))

    backfill._validate_historical_boundary(inventory)
    assert calls == [True]
    inventory["packages"][0]["relative_package_path"] = "./aliased"
    canonical = {key: value for key, value in inventory.items() if key != "inventory_hash"}
    inventory["inventory_hash"] = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")).hexdigest()
    with pytest.raises(backfill.BackfillSafetyError, match="noncanonical"):
        backfill._validate_historical_boundary(inventory)


def test_historical_boundary_rejects_case_alias_to_the_same_resolved_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    specs = []
    for form, count in (("nport", 26), ("ncen", 17), ("rr1", 39)):
        root = tmp_path / form
        for index in range(count):
            package = root / f"2024q1_{form}_{index:02d}"
            package.mkdir(parents=True)
            (package / "source.tsv").write_text("x\n", encoding="utf-8")
        specs.append(backfill.SourceSpec(form, root, count))
    monkeypatch.setattr(backfill, "IMMUTABLE_SOURCES", tuple(specs))
    monkeypatch.setattr(backfill, "validate_immutable_roots", lambda: None)
    inventory = backfill.build_inventory(tuple(specs))
    first, second = inventory["packages"][0], inventory["packages"][1]
    second["relative_package_path"] = first["relative_package_path"].upper()
    second["quarter"] = first["quarter"]
    second["identity"] = f"{second['form']}:{second['quarter']}:{second['relative_package_path']}"
    canonical = {key: value for key, value in inventory.items() if key != "inventory_hash"}
    inventory["inventory_hash"] = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")).hexdigest()
    with pytest.raises(backfill.BackfillSafetyError, match="alias|duplicate"):
        backfill._validate_historical_boundary(inventory)


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
