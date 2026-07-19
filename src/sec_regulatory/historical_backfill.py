"""Fail-closed, file-only supervision for the SEC historical backfill.

This module deliberately has no database connection code.  It inventories immutable
SEC roots and keeps resumable run state in an external directory; a future executor
must be supplied explicitly and can only report a package terminal state.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, cast


class BackfillSafetyError(RuntimeError):
    """A historical run cannot establish a safe, reproducible boundary."""


@dataclass(frozen=True)
class SourceSpec:
    form: str
    root: Path
    expected_packages: int


IMMUTABLE_SOURCES = (
    SourceSpec("nport", Path(r"E:\Edgard\nport"), 26),
    SourceSpec("ncen", Path(r"E:\Edgard\ncen"), 17),
    SourceSpec("rr1", Path(r"E:\Edgard\RR1"), 39),
)
EXCLUDED_ROOT = Path(r"E:\Edgard\13-F")
DEFAULT_RUN_DIR = Path(r"E:\investintell-sec-runs\historical-backfill")
SUCCESS_STATES = frozenset({"raw_validated", "duplicate"})
_QUARTER = re.compile(r"(?P<year>\d{4})[^0-9]*q(?P<quarter>[1-4])", re.IGNORECASE)


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _quarter(package: Path) -> str:
    match = _QUARTER.search(package.name)
    if match is None:
        raise BackfillSafetyError(f"cannot derive source quarter from package: {package.name}")
    return f"{match['year']}Q{match['quarter']}"


def _package_inventory(spec: SourceSpec, package: Path) -> dict[str, object]:
    files = []
    for path in sorted((candidate for candidate in package.rglob("*") if candidate.is_file()), key=lambda candidate: candidate.relative_to(package).as_posix()):
        relative_path = path.relative_to(package).as_posix()
        files.append({"relative_path": relative_path, "byte_count": path.stat().st_size, "sha256": sha256_file(path)})
    if not files:
        raise BackfillSafetyError(f"empty package: {package}")
    relative_package = package.relative_to(spec.root).as_posix()
    package_hash = _sha256_bytes(_canonical_json(files).encode("ascii"))
    return {
        "identity": f"{spec.form}:{_quarter(package)}:{relative_package}",
        "form": spec.form,
        "quarter": _quarter(package),
        "relative_package_path": relative_package,
        "file_count": len(files),
        "byte_count": sum(cast(int, file["byte_count"]) for file in files),
        "files": files,
        "package_sha256": package_hash,
    }


def build_inventory(sources: Sequence[SourceSpec]) -> dict[str, object]:
    """Create a deterministic source inventory without modifying SEC roots."""
    packages: list[dict[str, object]] = []
    roots: list[dict[str, str]] = []
    for spec in sources:
        if not spec.root.is_dir():
            raise BackfillSafetyError(f"root drift: unavailable root {spec.root}")
        discovered = sorted((path for path in spec.root.iterdir() if path.is_dir()), key=lambda path: path.name.casefold())
        if len(discovered) != spec.expected_packages:
            raise BackfillSafetyError(f"package count drift for {spec.form}: expected {spec.expected_packages}, found {len(discovered)}")
        roots.append({"form": spec.form, "root": str(spec.root)})
        packages.extend(_package_inventory(spec, package) for package in discovered)
    if len(packages) != sum(spec.expected_packages for spec in sources):
        raise BackfillSafetyError("total package count drift")
    packages.sort(key=lambda package: str(package["identity"]))
    document: dict[str, object] = {"schema_version": 1, "roots": roots, "packages": packages}
    document["inventory_hash"] = _sha256_bytes(_canonical_json(document).encode("ascii"))
    return document


def validate_immutable_roots(sources: Sequence[SourceSpec] = IMMUTABLE_SOURCES) -> None:
    """Reject any spelling, resolution, form, or count change to SEC root policy."""
    if len(sources) != len(IMMUTABLE_SOURCES) or any(
        actual.form != expected.form or str(actual.root) != str(expected.root) or actual.expected_packages != expected.expected_packages
        for actual, expected in zip(sources, IMMUTABLE_SOURCES, strict=True)
    ):
        raise BackfillSafetyError("root drift: historical source configuration differs from immutable policy")
    for spec in sources:
        resolved = spec.root.resolve()
        if str(resolved) != str(spec.root):
            raise BackfillSafetyError(f"root drift: resolved root differs from recorded root for {spec.form}")
    if EXCLUDED_ROOT in (spec.root for spec in sources):
        raise BackfillSafetyError("13-F is explicitly excluded from this backfill")


def build_historical_inventory() -> dict[str, object]:
    validate_immutable_roots()
    return build_inventory(IMMUTABLE_SOURCES)


def validate_canary_target(target: Mapping[str, object]) -> dict[str, object]:
    """Require a disposable loopback-only target before destructive fault injection."""
    required = {"host", "resolved_addresses", "database", "role", "secret_source"}
    if set(target) != required:
        raise BackfillSafetyError("uncertain canary target identity")
    host = str(target["host"]).casefold()
    addresses = tuple(str(address).casefold() for address in target["resolved_addresses"] if isinstance(address, str)) if isinstance(target["resolved_addresses"], (list, tuple)) else ()
    database = str(target["database"]).casefold()
    role = str(target["role"]).casefold()
    secret_source = str(target["secret_source"]).casefold()
    if host not in {"localhost", "127.0.0.1", "::1"} or not addresses or any(address not in {"127.0.0.1", "::1"} for address in addresses):
        raise BackfillSafetyError("canary target must resolve only to loopback")
    if database == "market" or not database or any(term in database for term in ("prod", "production")):
        raise BackfillSafetyError("canary target must use a disposable database")
    if not role or any(term in role for term in ("prod", "production")):
        raise BackfillSafetyError("canary target must use a disposable role")
    if not secret_source or any(term in secret_source for term in ("gcloud", "iap", "timescale-sp", "prod", "production", "secret-manager")):
        raise BackfillSafetyError("canary target must not use a production secret source")
    return dict(target)


def _now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: datetime | None = None) -> str:
    return (value or _now()).isoformat()


def _load_status(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackfillSafetyError(f"invalid durable status: {path}") from exc
    if not isinstance(loaded, dict):
        raise BackfillSafetyError("invalid durable status document")
    return loaded


def _assert_no_secret(value: object) -> None:
    if isinstance(value, Mapping):
        for item in value.values():
            _assert_no_secret(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_secret(item)
    elif isinstance(value, str) and any(marker in value.casefold() for marker in ("postgres://", "password=", "apikey=", "token=")):
        raise BackfillSafetyError("credential material is forbidden in status artifacts")


def _sanitize_command(command: Sequence[str]) -> list[str]:
    sanitized = []
    for argument in command:
        value = str(argument)
        if any(marker in value.casefold() for marker in ("postgres://", "password", "apikey", "token", "secret")):
            sanitized.append("[redacted]")
        else:
            sanitized.append(value)
    return sanitized


def _write_status(path: Path, status: Mapping[str, object]) -> None:
    _assert_no_secret(status)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(_canonical_json(status) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _assert_external_run_dir(run_dir: Path) -> None:
    resolved = run_dir.resolve()
    repo_root = Path(__file__).resolve().parents[2]
    forbidden = (repo_root, *(source.root for source in IMMUTABLE_SOURCES), EXCLUDED_ROOT)
    if any(resolved == root.resolve() or resolved.is_relative_to(root.resolve()) for root in forbidden):
        raise BackfillSafetyError("run directory must be outside Git and SEC source roots")


def _can_resume(record: Mapping[str, object] | None, package: Mapping[str, object], inventory_hash: str, code_sha: str) -> bool:
    return bool(record and record.get("state") in SUCCESS_STATES and record.get("package_sha256") == package.get("package_sha256") and record.get("inventory_hash") == inventory_hash and record.get("code_sha") == code_sha)


def _unexpired_lease(status: Mapping[str, object]) -> bool:
    lease = status.get("lease")
    if not isinstance(lease, Mapping) or not isinstance(lease.get("expires_at"), str):
        return False
    try:
        return datetime.fromisoformat(lease["expires_at"]) > _now()
    except ValueError as exc:
        raise BackfillSafetyError("invalid durable lease expiry") from exc


def heartbeat(status_path: Path, *, lease_owner: str, lease_seconds: int = 60, now: datetime | None = None) -> dict[str, Any]:
    """Renew a foreground-owned active package lease; no background process is started."""
    status = _load_status(status_path)
    lease = status.get("lease")
    if not isinstance(lease, dict) or lease.get("owner") != lease_owner or not status.get("active_package"):
        raise BackfillSafetyError("heartbeat requires the current active package lease owner")
    current = now or _now()
    status["heartbeat_at"] = _timestamp(current)
    status["lease"] = {"owner": lease_owner, "expires_at": _timestamp(current + timedelta(seconds=lease_seconds))}
    _write_status(status_path, status)
    return status


def run_supervisor(
    inventory: Mapping[str, object],
    *,
    status_path: Path,
    code_sha: str,
    execute_package: Callable[[dict[str, object]], Mapping[str, object]],
    lease_owner: str,
    command: Sequence[str] = ("historical-backfill",),
) -> dict[str, Any]:
    """Run one package at a time, recording a durable state after every attempt."""
    _assert_external_run_dir(status_path.parent)
    inventory_hash = inventory.get("inventory_hash")
    packages = inventory.get("packages")
    if not isinstance(inventory_hash, str) or not isinstance(packages, list) or not code_sha or not lease_owner:
        raise BackfillSafetyError("invalid inventory or supervisor identity")
    if any(not isinstance(package, dict) or not isinstance(package.get("identity"), str) for package in packages):
        raise BackfillSafetyError("invalid package inventory")
    status = _load_status(status_path)
    if _unexpired_lease(status):
        raise BackfillSafetyError("an active historical package lease has not expired")
    existing_value = status.get("packages")
    existing: dict[str, Any] = dict(existing_value) if isinstance(existing_value, dict) else {}
    status = {
        "schema_version": 1,
        "sanitized_command": _sanitize_command(command),
        "code_sha": code_sha,
        "interpreter": sys.version.split()[0],
        "dependency_identity": {"python": sys.implementation.name, "psycopg": importlib.metadata.version("psycopg")},
        "target_identity": {"kind": "unconfigured", "value": "no_database_connection"},
        "inventory_hash": inventory_hash,
        "packages": existing,
        "active_package": None,
        "active_attempt": None,
        "lease": None,
        "heartbeat_at": _timestamp(),
        "final_exit_state": "running",
    }
    for package in sorted(packages, key=lambda candidate: str(candidate["identity"])):
        identity = str(package["identity"])
        old_record = existing.get(identity)
        if isinstance(old_record, dict) and _can_resume(old_record, package, inventory_hash, code_sha):
            continue
        attempts = int(old_record.get("attempt", 0)) + 1 if isinstance(old_record, dict) else 1
        current = _now()
        status["active_package"] = identity
        status["active_attempt"] = attempts
        status["lease"] = {"owner": lease_owner, "expires_at": _timestamp(current + timedelta(seconds=60))}
        status["heartbeat_at"] = _timestamp(current)
        existing[identity] = {"state": "running", "attempt": attempts, "package_sha256": package.get("package_sha256"), "inventory_hash": inventory_hash, "code_sha": code_sha}
        _write_status(status_path, status)
        try:
            result = dict(execute_package(package))
            state = result.get("state")
        except Exception as exc:  # executor boundaries must be recorded, never ignored
            result = {"state": "failed", "reason": f"{type(exc).__name__}: {exc}"}
            state = "failed"
        if state not in SUCCESS_STATES and state != "failed":
            result = {"state": "failed", "reason": f"unexpected package state: {state!r}"}
            state = "failed"
        existing[identity] = {"state": state, "attempt": attempts, "package_sha256": package.get("package_sha256"), "inventory_hash": inventory_hash, "code_sha": code_sha, "result": result}
        status["active_package"] = None
        status["active_attempt"] = None
        status["lease"] = None
        status["heartbeat_at"] = _timestamp()
        if state == "failed":
            status["final_exit_state"] = "failed"
            status["failed_package"] = identity
            _write_status(status_path, status)
            return {"state": "failed", "failed_package": identity, "status_path": str(status_path)}
        _write_status(status_path, status)
    status["final_exit_state"] = "ok"
    _write_status(status_path, status)
    return {"state": "ok", "status_path": str(status_path)}


def code_identity() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], text=True).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise BackfillSafetyError("unable to establish code SHA") from exc


def _unconfigured_executor(_package: dict[str, object]) -> Mapping[str, object]:
    return {"state": "failed", "reason": "historical package executor is not configured"}


def cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m src.run historical-backfill")
    parser.add_argument("action", choices=("start", "status", "resume"))
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    args = parser.parse_args(argv)
    status_path = args.run_dir / "status.json"
    if args.action == "status":
        print(_canonical_json(_load_status(status_path)))
        return 0
    _assert_external_run_dir(args.run_dir)
    inventory = build_historical_inventory()
    _write_status(args.run_dir / "inventory.json", inventory)
    outcome = run_supervisor(inventory, status_path=status_path, code_sha=code_identity(), execute_package=_unconfigured_executor, lease_owner=f"pid-{os.getpid()}", command=("historical-backfill", args.action))
    print(_canonical_json(outcome))
    return 0 if outcome["state"] == "ok" else 1
