import json
from datetime import UTC, datetime
from pathlib import Path
import sys

import pytest


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

    inventory = {
        "inventory_hash": "inventory-v1",
        "packages": [
            {"identity": "nport:2024Q1:first", "package_sha256": "one"},
            {"identity": "nport:2024Q2:second", "package_sha256": "two"},
            {"identity": "rr1:2024Q3:third", "package_sha256": "three"},
        ],
    }
    status_path = tmp_path / "status.json"
    status_path.write_text(json.dumps({
        "inventory_hash": "inventory-v1",
        "code_sha": "code-v1",
        "packages": {
            "nport:2024Q1:first": {"state": "raw_validated", "package_sha256": "one", "inventory_hash": "inventory-v1", "code_sha": "code-v1"}
        },
    }), encoding="utf-8")
    calls: list[str] = []

    def execute(package: dict[str, str]) -> dict[str, str]:
        calls.append(package["identity"])
        return {"state": "failed" if package["identity"].endswith("second") else "raw_validated"}

    result = run_supervisor(inventory, status_path=status_path, code_sha="code-v1", execute_package=execute, lease_owner="unit-test")

    assert result["state"] == "failed"
    assert result["failed_package"] == "nport:2024Q2:second"
    assert calls == ["nport:2024Q2:second"]
    durable = json.loads(status_path.read_text(encoding="utf-8"))
    assert durable["packages"]["nport:2024Q1:first"]["state"] == "raw_validated"
    assert durable["packages"]["nport:2024Q2:second"]["state"] == "failed"


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

    renewed = heartbeat(status_path, lease_owner="unit-owner", lease_seconds=30, now=datetime(2024, 1, 1, tzinfo=UTC))

    assert renewed["active_package"] == "rr1:2024Q1:package"
    assert renewed["active_attempt"] == 2
    assert renewed["lease"]["expires_at"] == "2024-01-01T00:00:30+00:00"
    with pytest.raises(BackfillSafetyError, match="lease owner"):
        heartbeat(status_path, lease_owner="different-owner")
