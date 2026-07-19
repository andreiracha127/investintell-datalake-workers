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
import tempfile
import time
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
_SENSITIVE = ("password", "token", "secret", "credential", "dsn", "uri", "url", "postgres://", "postgresql://")


class _FileLock:
    """A small, cross-process advisory lock backed by a lock file."""

    def __init__(self, path: Path, *, timeout_seconds: float = 10.0) -> None:
        self.path = path
        self.timeout_seconds = timeout_seconds
        self._handle: Any = None

    def __enter__(self) -> "_FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                self._handle = self.path.open("a+b")
                self._handle.seek(0)
                if self._handle.read(1) == b"":
                    self._handle.write(b"0")
                    self._handle.flush()
                if os.name == "nt":
                    import msvcrt

                    self._handle.seek(0)
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[attr-defined]
                return self
            except OSError:
                if self._handle is not None:
                    self._handle.close()
                    self._handle = None
                if time.monotonic() >= deadline:
                    raise BackfillSafetyError(f"timed out acquiring historical run lock: {self.path}")
                time.sleep(0.02)

    def __exit__(self, *_args: object) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]
        finally:
            self._handle.close()


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
    _assert_safe_source_path(package, spec.root)
    files = []
    for path in sorted((candidate for candidate in package.rglob("*") if candidate.is_file()), key=lambda candidate: candidate.relative_to(package).as_posix()):
        _assert_safe_source_path(path, spec.root)
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


def _is_reparse(path: Path) -> bool:
    attributes = getattr(path.stat(), "st_file_attributes", 0)
    reparse = getattr(__import__("stat"), "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return path.is_symlink() or bool(attributes & reparse)


def _assert_safe_source_path(path: Path, root: Path) -> None:
    if _is_reparse(path):
        raise BackfillSafetyError(f"source symlink or reparse point is forbidden: {path}")
    try:
        resolved_root = root.resolve(strict=True)
        resolved_path = path.resolve(strict=True)
    except OSError as exc:
        raise BackfillSafetyError(f"source path resolution failed: {path}") from exc
    excluded = EXCLUDED_ROOT.resolve()
    if not resolved_path.is_relative_to(resolved_root) or resolved_path == excluded or resolved_path.is_relative_to(excluded):
        raise BackfillSafetyError(f"source path escapes its immutable root: {path}")


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


def _validate_inventory(inventory: Mapping[str, object]) -> dict[str, Path]:
    if set(inventory) != {"schema_version", "roots", "packages", "inventory_hash"} or inventory.get("schema_version") != 1:
        raise BackfillSafetyError("invalid inventory schema")
    roots = inventory.get("roots")
    packages = inventory.get("packages")
    claimed_hash = inventory.get("inventory_hash")
    if not isinstance(roots, list) or not isinstance(packages, list) or not isinstance(claimed_hash, str):
        raise BackfillSafetyError("invalid inventory schema")
    canonical = {key: value for key, value in inventory.items() if key != "inventory_hash"}
    if _sha256_bytes(_canonical_json(canonical).encode("ascii")) != claimed_hash:
        raise BackfillSafetyError("inventory hash mismatch")
    root_by_form: dict[str, Path] = {}
    for root in roots:
        if not isinstance(root, dict) or set(root) != {"form", "root"} or not isinstance(root["form"], str) or not isinstance(root["root"], str):
            raise BackfillSafetyError("invalid inventory root schema")
        if root["form"] in root_by_form:
            raise BackfillSafetyError("duplicate inventory root form")
        root_by_form[root["form"]] = Path(root["root"])
    identities: set[str] = set()
    required_package = {"identity", "form", "quarter", "relative_package_path", "file_count", "byte_count", "files", "package_sha256"}
    for package in packages:
        if not isinstance(package, dict) or set(package) != required_package:
            raise BackfillSafetyError("invalid inventory package schema")
        identity = package.get("identity")
        form = package.get("form")
        if not isinstance(identity, str) or not isinstance(form, str) or form not in root_by_form or identity in identities:
            raise BackfillSafetyError("duplicate or invalid package identity")
        if not isinstance(package.get("quarter"), str) or not isinstance(package.get("relative_package_path"), str) or not isinstance(package.get("package_sha256"), str):
            raise BackfillSafetyError("invalid inventory package schema")
        if not isinstance(package.get("file_count"), int) or not isinstance(package.get("byte_count"), int) or not isinstance(package.get("files"), list):
            raise BackfillSafetyError("invalid inventory package schema")
        files = cast(list[object], package["files"])
        if len(files) != package["file_count"]:
            raise BackfillSafetyError("invalid inventory file count")
        file_paths: set[str] = set()
        byte_count = 0
        for file in files:
            if not isinstance(file, dict) or set(file) != {"relative_path", "byte_count", "sha256"}:
                raise BackfillSafetyError("invalid inventory file schema")
            relative_path = file.get("relative_path")
            size = file.get("byte_count")
            digest = file.get("sha256")
            if not isinstance(relative_path, str) or not isinstance(size, int) or not isinstance(digest, str) or relative_path in file_paths:
                raise BackfillSafetyError("invalid inventory file schema")
            file_paths.add(relative_path)
            byte_count += size
        if byte_count != package["byte_count"]:
            raise BackfillSafetyError("invalid inventory byte count")
        identities.add(identity)
    return root_by_form


def _verify_package_unchanged(package: Mapping[str, object], root_by_form: Mapping[str, Path]) -> None:
    form = cast(str, package["form"])
    root = root_by_form[form]
    relative = Path(cast(str, package["relative_package_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise BackfillSafetyError("invalid package relative path")
    source = root / relative
    actual = _package_inventory(SourceSpec(form, root, 0), source)
    expected = {key: package[key] for key in ("file_count", "byte_count", "files", "package_sha256")}
    observed = {key: actual[key] for key in expected}
    if observed != expected:
        raise BackfillSafetyError("source drift detected against immutable inventory")


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


def _validate_historical_boundary(inventory: Mapping[str, object]) -> None:
    """Apply the fixed 82-package production policy, never fixture roots."""
    _validate_inventory(inventory)
    expected_roots = [{"form": spec.form, "root": str(spec.root)} for spec in IMMUTABLE_SOURCES]
    if inventory.get("roots") != expected_roots or len(cast(list[object], inventory["packages"])) != 82:
        raise BackfillSafetyError("historical inventory differs from immutable 82-package source policy")
    for package in cast(list[dict[str, object]], inventory["packages"]):
        identity = f"{package['form']}:{package['quarter']}:{package['relative_package_path']}"
        if package["identity"] != identity or package["form"] not in {"nport", "ncen", "rr1"}:
            raise BackfillSafetyError("historical package identity differs from source policy")


def validate_canary_target(target: Mapping[str, object]) -> dict[str, object]:
    """Require a disposable loopback-only target before destructive fault injection.

    A future connection integration must revalidate this declared identity against
    the identity actually resolved by the connected server before any mutation.
    """
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
    if database != "sec_backfill_test" and not database.startswith("sec_backfill_test_"):
        raise BackfillSafetyError("canary target must use a disposable database")
    if role != "sec_backfill_test" and not role.startswith("sec_backfill_test_"):
        raise BackfillSafetyError("canary target must use a disposable role")
    if secret_source not in {"local-disposable-fixture", "pytest-disposable-fixture"}:
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
        for key, item in value.items():
            if isinstance(key, str) and any(marker in key.casefold() for marker in _SENSITIVE):
                raise BackfillSafetyError("credential material is forbidden in status artifacts")
            _assert_no_secret(item)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            _assert_no_secret(item)
    elif isinstance(value, str) and any(marker in value.casefold() for marker in _SENSITIVE):
        raise BackfillSafetyError("credential material is forbidden in status artifacts")


def _redact(value: object, *, key: str = "") -> object:
    if any(marker in key.casefold() for marker in _SENSITIVE):
        return "[redacted]"
    if isinstance(value, Mapping):
        return {str(item_key): _redact(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_redact(item) for item in value]
    if isinstance(value, str) and any(marker in value.casefold() for marker in _SENSITIVE):
        return "[redacted]"
    return value


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
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_status = _redact(status)
    _assert_no_secret(safe_status)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as temporary:
        temporary.write(_canonical_json(safe_status) + "\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def _write_inventory(path: Path, inventory: Mapping[str, object]) -> None:
    """Persist the integrity artifact verbatim; unlike status it is never redacted."""
    _validate_inventory(inventory)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as temporary:
        temporary.write(_canonical_json(inventory) + "\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


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


def heartbeat(status_path: Path, *, lease_owner: str, active_attempt: int, lease_seconds: int = 60, now: datetime | None = None) -> dict[str, Any]:
    """Renew a foreground-owned active package lease; no background process is started."""
    with _FileLock(status_path.with_suffix(".status.lock")):
        status = _load_status(status_path)
        lease = status.get("lease")
        if not isinstance(lease, dict) or lease.get("owner") != lease_owner or not status.get("active_package") or status.get("active_attempt") != active_attempt:
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
    if not code_sha or not lease_owner:
        raise BackfillSafetyError("invalid inventory or supervisor identity")
    root_by_form = _validate_inventory(inventory)
    inventory_hash = cast(str, inventory["inventory_hash"])
    packages = cast(list[dict[str, object]], inventory["packages"])
    with _FileLock(status_path.with_suffix(".run.lock")):
        return _run_supervisor_locked(inventory_hash, packages, root_by_form, status_path, code_sha, execute_package, lease_owner, command)


def _run_supervisor_locked(
    inventory_hash: str,
    packages: list[dict[str, object]],
    root_by_form: Mapping[str, Path],
    status_path: Path,
    code_sha: str,
    execute_package: Callable[[dict[str, object]], Mapping[str, object]],
    lease_owner: str,
    command: Sequence[str],
) -> dict[str, Any]:
    with _FileLock(status_path.with_suffix(".status.lock")):
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
        with _FileLock(status_path.with_suffix(".status.lock")):
            _write_status(status_path, status)
        try:
            _verify_package_unchanged(package, root_by_form)
            result = dict(execute_package(package))
            state = result.get("state")
            _verify_package_unchanged(package, root_by_form)
        except BackfillSafetyError:
            result = {"state": "failed", "reason_code": "source_drift"}
            state = "failed"
        except Exception:  # executor boundaries must be recorded, never ignored
            result = {"state": "failed", "reason_code": "executor_exception"}
            state = "failed"
        if state not in SUCCESS_STATES and state != "failed":
            result = {"state": "failed", "reason_code": "unexpected_package_state"}
            state = "failed"
        if state == "failed" and result.get("reason_code") not in {"source_drift", "executor_exception", "unexpected_package_state", "executor_unconfigured"}:
            result = {"state": "failed", "reason_code": "executor_reported_failure"}
        existing[identity] = {"state": state, "attempt": attempts, "package_sha256": package.get("package_sha256"), "inventory_hash": inventory_hash, "code_sha": code_sha, "result_state": result.get("state"), "reason_code": result.get("reason_code")}
        status["active_package"] = None
        status["active_attempt"] = None
        status["lease"] = None
        status["heartbeat_at"] = _timestamp()
        if state == "failed":
            status["final_exit_state"] = "failed"
            status["failed_package"] = identity
            with _FileLock(status_path.with_suffix(".status.lock")):
                _write_status(status_path, status)
            return {"state": "failed", "failed_package": identity, "reason": result.get("reason_code"), "status_path": str(status_path)}
        with _FileLock(status_path.with_suffix(".status.lock")):
            _write_status(status_path, status)
    status["final_exit_state"] = "ok"
    with _FileLock(status_path.with_suffix(".status.lock")):
        _write_status(status_path, status)
    return {"state": "ok", "status_path": str(status_path)}


def code_identity() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], text=True).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise BackfillSafetyError("unable to establish code SHA") from exc


def _unconfigured_executor(_package: dict[str, object]) -> Mapping[str, object]:
    return {"state": "failed", "reason_code": "executor_unconfigured"}


def cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m src.run historical-backfill")
    parser.add_argument("action", choices=("start", "status", "resume"))
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    args = parser.parse_args(argv)
    _assert_external_run_dir(args.run_dir)
    status_path = args.run_dir / "status.json"
    if args.action == "status":
        print(_canonical_json(_load_status(status_path)))
        return 0
    with _FileLock(args.run_dir / ".lifecycle.lock"):
        inventory_path = args.run_dir / "inventory.json"
        if args.action == "start":
            if status_path.exists() or inventory_path.exists():
                raise BackfillSafetyError("start requires an empty external run directory")
            inventory = build_historical_inventory()
            _validate_historical_boundary(inventory)
            _write_inventory(inventory_path, inventory)
        else:
            if not status_path.exists() or not inventory_path.exists():
                raise BackfillSafetyError("resume requires existing status and inventory artifacts")
            inventory = _load_status(inventory_path)
            _validate_historical_boundary(inventory)
            status = _load_status(status_path)
            if status.get("schema_version") != 1 or status.get("inventory_hash") != inventory.get("inventory_hash") or status.get("code_sha") != code_identity():
                raise BackfillSafetyError("resume status does not match inventory or code identity")
        outcome = run_supervisor(inventory, status_path=status_path, code_sha=code_identity(), execute_package=_unconfigured_executor, lease_owner=f"pid-{os.getpid()}", command=("historical-backfill", args.action))
        print(_canonical_json(outcome))
        return 0 if outcome["state"] == "ok" else 1
