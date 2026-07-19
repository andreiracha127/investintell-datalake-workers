"""Fail-closed, file-only supervision for the SEC historical backfill.

This module deliberately has no database connection code.  It inventories immutable
SEC roots and keeps resumable run state in an external directory; a future executor
must be supplied explicitly and can only report a package terminal state.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import importlib
import importlib.metadata
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
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
FAILURE_REASON_CODES = frozenset({"source_drift", "executor_exception", "unexpected_package_state", "executor_unconfigured", "heartbeat_renewal_failed", "lock_busy", "authorization_refusal", "target_refusal", "privilege_refusal", "executor_refusal", "ingester_failed", "executor_reported_failure"})
_QUARTER = re.compile(r"(?P<year>\d{4})[^0-9]*q(?P<quarter>[1-4])", re.IGNORECASE)
_SENSITIVE_KEYS = frozenset({"password", "token", "secret", "credential", "dsn", "database_url", "connection_url"})
_SENSITIVE_SUFFIXES = ("_password", "_token", "_secret", "_credential", "_dsn")
_DATABASE_SCHEMES = ("postgres://", "postgresql://")
_AUTHORIZATION_FIELDS = frozenset(
    {
        "schema_version",
        "stage",
        "code_sha",
        "inventory_hash",
        "target_mode",
        "dsn_env_var",
        "target",
        "writable_tables",
        "pointer_table_denylist",
        "sanitized_command",
        "run_directory",
        "authorization_id",
        "stop_contract_hash",
        "reconciliation_contract_hash",
    }
)
_TARGET_FIELDS = frozenset(
    {
        "project",
        "vm",
        "zone",
        "host",
        "resolved_addresses",
        "database",
        "server_address",
        "role",
        "secret_source",
        "postgresql_identity",
        "timescaledb_identity",
    }
)
_PRODUCTION_TARGET = {
    "project": "investintell-research-analisys",
    "vm": "timescale-sp",
    "zone": "southamerica-east1-a",
    "database": "market",
}
AUTHORIZED_HEARTBEAT_INTERVAL_SECONDS = 15.0


def _is_sensitive_key(key: str) -> bool:
    normalized = key.casefold()
    return normalized in _SENSITIVE_KEYS or normalized.endswith(_SENSITIVE_SUFFIXES)


def _is_sensitive_value(value: str) -> bool:
    return value.casefold().startswith(_DATABASE_SCHEMES)


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


def _error_digest(reason_code: str) -> str:
    return _sha256_bytes(reason_code.encode("ascii"))


def _reason_code_for_safety_error(error: BackfillSafetyError) -> str:
    message = str(error).casefold()
    if "source drift" in message:
        return "source_drift"
    if "lock_busy" in message:
        return "lock_busy"
    if "privilege" in message or "writable table" in message:
        return "privilege_refusal"
    if "connected target" in message or "target identity" in message:
        return "target_refusal"
    if "authorization" in message:
        return "authorization_refusal"
    return "executor_refusal"


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
    expected = {key: package[key] for key in ("identity", "form", "quarter", "relative_package_path", "file_count", "byte_count", "files", "package_sha256")}
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
    validate_immutable_roots()
    _validate_inventory(inventory)
    expected_roots = [{"form": spec.form, "root": str(spec.root)} for spec in IMMUTABLE_SOURCES]
    expected_counts = Counter({"nport": 26, "ncen": 17, "rr1": 39})
    packages = cast(list[dict[str, object]], inventory["packages"])
    observed_counts = Counter(cast(str, package["form"]) for package in packages)
    if inventory.get("roots") != expected_roots or len(packages) != 82 or observed_counts != expected_counts:
        raise BackfillSafetyError("historical inventory differs from immutable 82-package source policy")
    roots = {spec.form: spec.root for spec in IMMUTABLE_SOURCES}
    normalized_paths: set[tuple[str, str]] = set()
    resolved_targets: set[tuple[str, str]] = set()
    for package in packages:
        relative = cast(str, package["relative_package_path"])
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts or relative != relative_path.as_posix():
            raise BackfillSafetyError("historical package path is noncanonical")
        identity = f"{package['form']}:{package['quarter']}:{package['relative_package_path']}"
        if package["identity"] != identity or package["form"] not in {"nport", "ncen", "rr1"}:
            raise BackfillSafetyError("historical package identity differs from source policy")
        form = cast(str, package["form"])
        normalized = (form, relative.casefold())
        source = roots[form] / relative_path
        resolved = source.resolve(strict=True)
        resolved_key = (form, str(resolved).casefold())
        if normalized in normalized_paths or resolved_key in resolved_targets:
            raise BackfillSafetyError("historical package path aliases a duplicate resolved target")
        normalized_paths.add(normalized)
        resolved_targets.add(resolved_key)
        actual = _package_inventory(SourceSpec(form, roots[form], 0), source)
        if any(actual[key] != package[key] for key in ("identity", "form", "quarter", "relative_package_path", "file_count", "byte_count", "files", "package_sha256")):
            raise BackfillSafetyError("historical package metadata differs from resolved source")


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


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9a-f]{64}", value))


def _validate_execution_authorization(document: Mapping[str, object], *, code_sha: str, inventory_hash: str) -> dict[str, object]:
    """Validate a file-only authorization before resolving a connection secret."""
    if set(document) != _AUTHORIZATION_FIELDS or document.get("schema_version") != 1 or document.get("stage") != "phase4_historical_backfill":
        raise BackfillSafetyError("invalid execution authorization schema")
    if document.get("code_sha") != code_sha:
        raise BackfillSafetyError("execution authorization code SHA mismatch")
    if document.get("inventory_hash") != inventory_hash:
        raise BackfillSafetyError("execution authorization inventory hash mismatch")
    mode = document.get("target_mode")
    if mode not in {"local_disposable", "production_authorized"}:
        raise BackfillSafetyError("invalid execution authorization target mode")
    dsn_env_var = document.get("dsn_env_var")
    if not isinstance(dsn_env_var, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", dsn_env_var):
        raise BackfillSafetyError("execution authorization requires a DSN environment variable name")
    target = document.get("target")
    if not isinstance(target, dict) or set(target) != _TARGET_FIELDS:
        raise BackfillSafetyError("invalid execution authorization target")
    if not isinstance(target.get("resolved_addresses"), list) or not all(isinstance(value, str) and value for value in target["resolved_addresses"]):
        raise BackfillSafetyError("invalid execution authorization target")
    for field in _TARGET_FIELDS - {"resolved_addresses"}:
        if not isinstance(target.get(field), str) or not target[field]:
            raise BackfillSafetyError("invalid execution authorization target")
    writable = document.get("writable_tables")
    denied = document.get("pointer_table_denylist")
    if not isinstance(writable, list) or not writable or not all(isinstance(value, str) and value for value in writable) or len(set(writable)) != len(writable):
        raise BackfillSafetyError("invalid execution authorization writable table allowlist")
    if any(value.casefold().startswith("sec_current.") or "provider" in value.casefold() or "pointer" in value.casefold() for value in writable):
        raise BackfillSafetyError("invalid execution authorization writable table allowlist")
    if not isinstance(denied, list) or not denied or not all(isinstance(value, str) and value for value in denied) or set(writable) & set(denied):
        raise BackfillSafetyError("invalid execution authorization pointer table denylist")
    sanitized_command = document.get("sanitized_command")
    if not isinstance(sanitized_command, list) or not all(isinstance(value, str) and not _is_sensitive_value(value) for value in sanitized_command):
        raise BackfillSafetyError("invalid execution authorization sanitized command")
    run_directory = document.get("run_directory")
    if not isinstance(run_directory, str) or not run_directory or _is_sensitive_value(run_directory):
        raise BackfillSafetyError("invalid execution authorization run directory")
    if not isinstance(document.get("authorization_id"), str) or not document["authorization_id"]:
        raise BackfillSafetyError("execution authorization requires a separately issued authorization ID")
    if not _is_sha256(document.get("stop_contract_hash")) or not _is_sha256(document.get("reconciliation_contract_hash")):
        raise BackfillSafetyError("invalid execution authorization contract hash")
    if mode == "local_disposable":
        validate_canary_target(
            {
                "host": target["host"],
                "resolved_addresses": target["resolved_addresses"],
                "database": target["database"],
                "role": target["role"],
                "secret_source": target["secret_source"],
            }
        )
        if target["server_address"] not in {"127.0.0.1", "::1"}:
            raise BackfillSafetyError("local disposable server address must be loopback")
    else:
        if any(target[field] != expected for field, expected in _PRODUCTION_TARGET.items()):
            raise BackfillSafetyError("invalid production execution authorization target")
        if target["role"].casefold() in {"postgres", "owner", "superuser"}:
            raise BackfillSafetyError("production execution authorization role is unsafe")
    return dict(document)


def authorization_fingerprint(document: Mapping[str, object]) -> str:
    """Fingerprint the complete file-only authorization artifact after schema validation."""
    return _sha256_bytes(_canonical_json(document).encode("ascii"))


def load_execution_authorization(
    path: Path,
    *,
    code_sha: str,
    inventory_hash: str,
    run_directory: Path | None = None,
    command: Sequence[str] | None = None,
) -> dict[str, object]:
    """Load the exact authorization artifact without resolving its secret reference."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackfillSafetyError("invalid execution authorization artifact") from exc
    if not isinstance(document, dict):
        raise BackfillSafetyError("invalid execution authorization artifact")
    validated = _validate_execution_authorization(document, code_sha=code_sha, inventory_hash=inventory_hash)
    if run_directory is not None and validated["run_directory"] != str(run_directory.resolve()):
        raise BackfillSafetyError("execution authorization run directory mismatch")
    if command is not None and validated["sanitized_command"] != list(command):
        raise BackfillSafetyError("execution authorization command mismatch")
    validated["authorization_fingerprint"] = authorization_fingerprint(validated)
    return validated


def _inspect_connected_target(connection: object) -> dict[str, object]:
    """Query the connected server identity and effective write privileges."""
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute(
            "SELECT current_database(), inet_server_addr()::text, current_user, version(), "
            "COALESCE((SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'), '')"
        )
        identity = cursor.fetchone()
        cursor.execute(
            "SELECT r.rolsuper, EXISTS (SELECT 1 FROM pg_class c WHERE c.relowner = r.oid AND c.relkind IN ('r', 'p', 'v', 'm', 'f')) "
            "FROM pg_roles r WHERE r.rolname = current_user"
        )
        role_status = cursor.fetchone()
        cursor.execute(
            "SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') "
            "AND has_table_privilege(current_user, c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE') "
            "ORDER BY n.nspname, c.relname"
        )
        writable = [row[0] for row in cursor.fetchall()]
    if not isinstance(identity, tuple) or len(identity) != 5 or not isinstance(role_status, tuple) or len(role_status) != 2:
        raise BackfillSafetyError("uncertain connected target identity")
    return {
        "database": identity[0],
        "server_address": identity[1],
        "role": identity[2],
        "postgresql_identity": identity[3],
        "timescaledb_identity": identity[4],
        "is_superuser": role_status[0],
        "owns_any_table": role_status[1],
        "writable_tables": writable,
    }


def _default_schema_installers(form: str) -> dict[str, Callable[[object], None]]:
    manifests = importlib.import_module("src.sec_regulatory.manifests")
    storage = importlib.import_module(f"src.{form}.storage")
    return {"manifest": manifests.install_schema, form: storage.install_schema}


def _default_dispatcher(form: str) -> Callable[..., Mapping[str, object]]:
    module = importlib.import_module(f"src.{form}.ingestion")
    return cast(Callable[..., Mapping[str, object]], module.ingest_package)


def _derive_form_lock_key(form: str, package: Path) -> str:
    """Recreate the exact form-specific advisory lock key used by its ingester."""
    schema = importlib.import_module(f"src.{form}.schema")
    if form == "nport":
        verified = schema.verify_package(package, schema.load_nport_contract())
        digest = schema.package_sha256(verified.file_hashes, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256)
    elif form == "ncen":
        verified = schema.verify_package(package)
        digest = schema.package_sha256(verified.file_hashes, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256)
    elif form == "rr1":
        verified = schema.verify_package(package)
        digest = schema.package_sha256(verified.file_hashes, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256, metadata_filename=verified.metadata_filename)
    else:
        raise BackfillSafetyError("unsupported historical package form")
    return f"{form}:{digest}"


def _try_form_advisory_lock(connection: object, key: str) -> bool:
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (key,))
        return cursor.fetchone() == (True,)


def _release_form_advisory_lock(connection: object, key: str) -> None:
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,))


class AuthorizedPackageExecutor:
    """One-package executor whose connection exists only after authorization validation."""

    def __init__(
        self,
        authorization: Mapping[str, object],
        inventory: Mapping[str, object],
        connection_factory: Callable[[str], object] | None,
        target_inspector: Callable[[object], Mapping[str, object]] | None,
        schema_installers: Mapping[str, Callable[[object], None]] | None,
        dispatchers: Mapping[str, Callable[..., Mapping[str, object]]] | None,
    ) -> None:
        self.authorization = dict(authorization)
        self.inventory = inventory
        self.connection_factory = connection_factory
        self.target_inspector = target_inspector or _inspect_connected_target
        self.schema_installers = schema_installers
        self.dispatchers = dispatchers
        target = cast(Mapping[str, object], authorization["target"])
        self.target_identity = {key: target[key] for key in ("project", "vm", "zone", "database", "server_address", "role")}
        self.authorization_id = cast(str, authorization["authorization_id"])
        self.authorization_fingerprint = cast(str, authorization["authorization_fingerprint"])
        self.authorization_lineage = {key: value for key, value in authorization.items() if key != "authorization_fingerprint"}

    def _validate_connected_target(self, actual: Mapping[str, object]) -> None:
        target = cast(Mapping[str, object], self.authorization["target"])
        required = {"database", "server_address", "role", "postgresql_identity", "timescaledb_identity", "is_superuser", "owns_any_table", "writable_tables"}
        if set(actual) != required:
            raise BackfillSafetyError("uncertain connected target identity")
        for field in ("database", "server_address", "role", "postgresql_identity", "timescaledb_identity"):
            if actual[field] != target[field]:
                raise BackfillSafetyError("connected target identity mismatch")
        if actual["is_superuser"] is not False or actual["owns_any_table"] is not False:
            raise BackfillSafetyError("connected target privilege is unsafe")
        writable = actual["writable_tables"]
        if not isinstance(writable, list) or not all(isinstance(value, str) for value in writable) or len(set(writable)) != len(writable):
            raise BackfillSafetyError("uncertain effective writable table set")
        allowed = set(cast(list[str], self.authorization["writable_tables"]))
        denied = set(cast(list[str], self.authorization["pointer_table_denylist"]))
        if set(writable) != allowed or set(writable) & denied or any(value.casefold().startswith("sec_current.") or "provider" in value.casefold() or "pointer" in value.casefold() for value in writable):
            raise BackfillSafetyError("connected target writable table set is unsafe")

    @staticmethod
    def _terminal_result(result: Mapping[str, object], expected_package: str) -> dict[str, object]:
        if result.get("package") != expected_package:
            raise BackfillSafetyError("ingester returned a mismatched package identity")
        state = result.get("state")
        rows = result.get("rows")
        reconciliation_hash = result.get("reconciliation_hash")
        if reconciliation_hash is not None and not _is_sha256(reconciliation_hash):
            raise BackfillSafetyError("ingester returned an invalid reconciliation hash")
        if state == "raw_validated":
            if set(result) - {"package", "state", "run_id", "rows", "reconciliation_hash", "resumed"}:
                raise BackfillSafetyError("ingester returned unexpected raw_validated fields")
            run_id = result.get("run_id")
            resumed = result.get("resumed")
            if rows is None and resumed is True:
                rows = 0
            if resumed is not None and resumed is not True:
                raise BackfillSafetyError("ingester returned an invalid raw_validated resume marker")
            if not isinstance(run_id, str) or not run_id or not isinstance(rows, int) or isinstance(rows, bool) or rows < 0:
                raise BackfillSafetyError("ingester returned an invalid raw_validated result")
            safe: dict[str, object] = {"state": state, "run_id": run_id, "rows": rows}
        elif state == "duplicate":
            if set(result) - {"package", "state", "rows", "reconciliation_hash"}:
                raise BackfillSafetyError("ingester returned unexpected duplicate fields")
            if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0:
                raise BackfillSafetyError("ingester returned an invalid duplicate result")
            safe = {"state": state, "rows": rows}
        elif state == "failed":
            if set(result) - {"package", "state", "run_id", "reason", "reason_code"}:
                raise BackfillSafetyError("ingester returned unexpected failed fields")
            safe = {"state": state, "reason_code": "ingester_failed"}
        else:
            raise BackfillSafetyError("ingester returned a nonterminal package state")
        if reconciliation_hash is not None:
            safe["reconciliation_hash"] = reconciliation_hash
        return safe

    def __call__(self, package: dict[str, object]) -> Mapping[str, object]:
        root_by_form = _validate_inventory(self.inventory)
        identity = package.get("identity")
        expected = next((candidate for candidate in cast(list[dict[str, object]], self.inventory["packages"]) if candidate["identity"] == identity), None)
        if expected is None or any(package.get(key) != expected.get(key) for key in expected):
            raise BackfillSafetyError("package is not bound to the validated inventory")
        form = cast(str, expected["form"])
        if form not in {"nport", "ncen", "rr1"}:
            raise BackfillSafetyError("unsupported historical package form")
        _verify_package_unchanged(expected, root_by_form)
        dsn_env_var = cast(str, self.authorization["dsn_env_var"])
        dsn = os.environ.get(dsn_env_var)
        if not dsn:
            raise BackfillSafetyError("authorized executor DSN environment variable is unavailable")
        factory = self.connection_factory
        if factory is None:
            psycopg = importlib.import_module("psycopg")
            factory = psycopg.connect
        connection = factory(dsn)
        lock_key: str | None = None
        try:
            self._validate_connected_target(self.target_inspector(connection))
            source_package = root_by_form[form] / Path(cast(str, expected["relative_package_path"]))
            lock_key = _derive_form_lock_key(form, source_package)
            if not _try_form_advisory_lock(connection, lock_key):
                lock_key = None
                raise BackfillSafetyError("lock_busy")
            installers = self.schema_installers or _default_schema_installers(form)
            if set(installers) != {"manifest", form}:
                raise BackfillSafetyError("authorized executor schema installer boundary is invalid")
            installers["manifest"](connection)
            installers[form](connection)
            dispatcher = (self.dispatchers or {}).get(form) if self.dispatchers is not None else _default_dispatcher(form)
            if dispatcher is None:
                raise BackfillSafetyError("authorized executor dispatcher is unavailable")
            root = root_by_form[form]
            result = dict(dispatcher(connection, package=source_package, source_root=root))
            safe = self._terminal_result(result, cast(str, expected["relative_package_path"]))
            commit = getattr(connection, "commit", None)
            if not callable(commit):
                raise BackfillSafetyError("authorized executor connection cannot commit")
            commit()
            return safe
        except Exception:
            rollback = getattr(connection, "rollback", None)
            if callable(rollback):
                rollback()
            raise
        finally:
            if lock_key is not None:
                try:
                    _release_form_advisory_lock(connection, lock_key)
                except Exception:
                    pass
            close = getattr(connection, "close", None)
            if callable(close):
                close()


def build_authorized_executor(
    authorization_path: Path,
    *,
    inventory: Mapping[str, object],
    code_sha: str,
    connection_factory: Callable[[str], object] | None = None,
    target_inspector: Callable[[object], Mapping[str, object]] | None = None,
    schema_installers: Mapping[str, Callable[[object], None]] | None = None,
    dispatchers: Mapping[str, Callable[..., Mapping[str, object]]] | None = None,
    run_directory: Path | None = None,
    command: Sequence[str] | None = None,
) -> AuthorizedPackageExecutor:
    """Bind an inert executor to an exact authorization and inventory artifact."""
    inventory_hash = inventory.get("inventory_hash")
    if not isinstance(inventory_hash, str):
        raise BackfillSafetyError("invalid inventory for authorized executor")
    authorization = load_execution_authorization(authorization_path, code_sha=code_sha, inventory_hash=inventory_hash, run_directory=run_directory, command=command)
    return AuthorizedPackageExecutor(authorization, inventory, connection_factory, target_inspector, schema_installers, dispatchers)


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
            if isinstance(key, str) and _is_sensitive_key(key):
                raise BackfillSafetyError("credential material is forbidden in status artifacts")
            _assert_no_secret(item)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            _assert_no_secret(item)
    elif isinstance(value, str) and _is_sensitive_value(value):
        raise BackfillSafetyError("credential material is forbidden in status artifacts")


def _redact(value: object, *, key: str = "") -> object:
    if _is_sensitive_key(key):
        return "[redacted]"
    if isinstance(value, Mapping):
        return {str(item_key): _redact(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_redact(item) for item in value]
    if isinstance(value, str) and _is_sensitive_value(value):
        return "[redacted]"
    return value


def _sanitize_command(command: Sequence[str]) -> list[str]:
    sanitized = []
    for argument in command:
        value = str(argument)
        if _is_sensitive_value(value) or _is_sensitive_key(value.removeprefix("--").split("=", 1)[0]):
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


def _can_resume(record: Mapping[str, object] | None, package: Mapping[str, object], inventory_hash: str, code_sha: str, authorization_fingerprint: str | None) -> bool:
    return bool(
        record
        and record.get("state") in SUCCESS_STATES
        and record.get("package_sha256") == package.get("package_sha256")
        and record.get("inventory_hash") == inventory_hash
        and record.get("code_sha") == code_sha
        and record.get("authorization_fingerprint") == authorization_fingerprint
    )


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


class _LeaseHeartbeat:
    """A bounded in-process lease renewal loop for one active package attempt."""

    def __init__(self, status_path: Path, lease_owner: str, active_attempt: int, interval_seconds: float) -> None:
        self.status_path = status_path
        self.lease_owner = lease_owner
        self.active_attempt = active_attempt
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._state_lock = threading.Lock()
        self._failure_reason: str | None = None
        self._thread = threading.Thread(target=self._run, name="historical-backfill-heartbeat", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self.interval_seconds * 3))

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                heartbeat(self.status_path, lease_owner=self.lease_owner, active_attempt=self.active_attempt)
            except Exception:
                with self._state_lock:
                    self._failure_reason = "heartbeat_renewal_failed"
                return

    @property
    def failure_reason(self) -> str | None:
        with self._state_lock:
            return self._failure_reason


def run_supervisor(
    inventory: Mapping[str, object],
    *,
    status_path: Path,
    code_sha: str,
    execute_package: Callable[[dict[str, object]], Mapping[str, object]],
    lease_owner: str,
    command: Sequence[str] = ("historical-backfill",),
    authorization_id: str | None = None,
    target_identity: Mapping[str, object] | None = None,
    authorization_fingerprint: str | None = None,
    authorization_lineage: Mapping[str, object] | None = None,
    heartbeat_interval_seconds: float | None = None,
) -> dict[str, Any]:
    """Run one package at a time, recording a durable state after every attempt."""
    _assert_external_run_dir(status_path.parent)
    if not code_sha or not lease_owner or (heartbeat_interval_seconds is not None and heartbeat_interval_seconds <= 0):
        raise BackfillSafetyError("invalid inventory or supervisor identity")
    root_by_form = _validate_inventory(inventory)
    inventory_hash = cast(str, inventory["inventory_hash"])
    packages = cast(list[dict[str, object]], inventory["packages"])
    with _FileLock(status_path.with_suffix(".run.lock")):
        return _run_supervisor_locked(inventory_hash, packages, root_by_form, status_path, code_sha, execute_package, lease_owner, command, authorization_id, target_identity, authorization_fingerprint, authorization_lineage, heartbeat_interval_seconds)


def _run_supervisor_locked(
    inventory_hash: str,
    packages: list[dict[str, object]],
    root_by_form: Mapping[str, Path],
    status_path: Path,
    code_sha: str,
    execute_package: Callable[[dict[str, object]], Mapping[str, object]],
    lease_owner: str,
    command: Sequence[str],
    authorization_id: str | None,
    target_identity: Mapping[str, object] | None,
    authorization_fingerprint: str | None,
    authorization_lineage: Mapping[str, object] | None,
    heartbeat_interval_seconds: float | None,
) -> dict[str, Any]:
    with _FileLock(status_path.with_suffix(".status.lock")):
        status = _load_status(status_path)
        if _unexpired_lease(status):
            raise BackfillSafetyError("an active historical package lease has not expired")
        if status and status.get("authorization_id") != authorization_id:
            raise BackfillSafetyError("resume status does not match execution authorization identity")
        if status and ("authorization_fingerprint" in status and status.get("authorization_fingerprint") != authorization_fingerprint or "authorization_fingerprint" not in status and authorization_fingerprint is not None):
            raise BackfillSafetyError("resume status does not match execution authorization fingerprint")
        if status and ("authorization_lineage" in status and status.get("authorization_lineage") != (dict(authorization_lineage) if authorization_lineage is not None else None) or "authorization_lineage" not in status and authorization_lineage is not None):
            raise BackfillSafetyError("resume status does not match execution authorization lineage")
        if status and "target_identity" in status and status.get("target_identity") != (dict(target_identity) if target_identity is not None else {"kind": "unconfigured", "value": "no_database_connection"}):
            raise BackfillSafetyError("resume status does not match execution target identity")
        existing_value = status.get("packages")
        existing: dict[str, Any] = dict(existing_value) if isinstance(existing_value, dict) else {}
    status = {
        "schema_version": 1,
        "sanitized_command": _sanitize_command(command),
        "code_sha": code_sha,
        "interpreter": sys.version.split()[0],
        "dependency_identity": {"python": sys.implementation.name, "psycopg": importlib.metadata.version("psycopg")},
        "target_identity": dict(target_identity) if target_identity is not None else {"kind": "unconfigured", "value": "no_database_connection"},
        "authorization_id": authorization_id,
        "authorization_fingerprint": authorization_fingerprint,
        "authorization_lineage": dict(authorization_lineage) if authorization_lineage is not None else None,
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
        if isinstance(old_record, dict) and _can_resume(old_record, package, inventory_hash, code_sha, authorization_fingerprint):
            continue
        attempts = int(old_record.get("attempt", 0)) + 1 if isinstance(old_record, dict) else 1
        current = _now()
        status["active_package"] = identity
        status["active_attempt"] = attempts
        status["lease"] = {"owner": lease_owner, "expires_at": _timestamp(current + timedelta(seconds=60))}
        status["heartbeat_at"] = _timestamp(current)
        existing[identity] = {"state": "running", "attempt": attempts, "package_sha256": package.get("package_sha256"), "inventory_hash": inventory_hash, "code_sha": code_sha, "authorization_id": authorization_id, "authorization_fingerprint": authorization_fingerprint}
        with _FileLock(status_path.with_suffix(".status.lock")):
            _write_status(status_path, status)
        pulse = _LeaseHeartbeat(status_path, lease_owner, attempts, heartbeat_interval_seconds) if heartbeat_interval_seconds is not None else None
        if pulse is not None:
            pulse.start()
        try:
            try:
                _verify_package_unchanged(package, root_by_form)
                result = dict(execute_package(package))
                state = result.get("state")
                _verify_package_unchanged(package, root_by_form)
            except BackfillSafetyError as error:
                result = {"state": "failed", "reason_code": _reason_code_for_safety_error(error)}
                state = "failed"
            except Exception:  # executor boundaries must be recorded, never ignored
                result = {"state": "failed", "reason_code": "executor_exception"}
                state = "failed"
        finally:
            if pulse is not None:
                pulse.stop()
        if pulse is not None and pulse.failure_reason is not None:
            result = {"state": "failed", "reason_code": pulse.failure_reason}
            state = "failed"
        if state not in SUCCESS_STATES and state != "failed":
            result = {"state": "failed", "reason_code": "unexpected_package_state"}
            state = "failed"
        if state == "failed" and result.get("reason_code") not in FAILURE_REASON_CODES:
            result = {"state": "failed", "reason_code": "executor_reported_failure"}
        reason_code = cast(str, result.get("reason_code", ""))
        existing[identity] = {"state": state, "attempt": attempts, "package_sha256": package.get("package_sha256"), "inventory_hash": inventory_hash, "code_sha": code_sha, "authorization_id": authorization_id, "authorization_fingerprint": authorization_fingerprint, "result_state": result.get("state"), "reason_code": reason_code or None, "error_digest": _error_digest(reason_code) if state == "failed" and reason_code else None, "rows": result.get("rows"), "run_id": result.get("run_id"), "reconciliation_hash": result.get("reconciliation_hash")}
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
    parser.add_argument("--execution-authorization", type=Path)
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
        current_code_sha = code_identity()
        if args.execution_authorization is None:
            executor: Callable[[dict[str, object]], Mapping[str, object]] = _unconfigured_executor
            authorization_id = None
            target_identity = None
            authorization_fingerprint = None
            authorization_lineage = None
        else:
            authorization_command: Sequence[str] = ("historical-backfill", args.action)
            if args.action == "resume":
                prior_lineage = status.get("authorization_lineage") if isinstance(status, dict) else None
                if isinstance(prior_lineage, Mapping) and isinstance(prior_lineage.get("sanitized_command"), list) and all(isinstance(value, str) for value in prior_lineage["sanitized_command"]):
                    authorization_command = cast(list[str], prior_lineage["sanitized_command"])
            executor = build_authorized_executor(args.execution_authorization, inventory=inventory, code_sha=current_code_sha, run_directory=args.run_dir, command=authorization_command)
            authorization_id = executor.authorization_id
            target_identity = executor.target_identity
            authorization_fingerprint = executor.authorization_fingerprint
            authorization_lineage = executor.authorization_lineage
        outcome = run_supervisor(inventory, status_path=status_path, code_sha=current_code_sha, execute_package=executor, lease_owner=f"pid-{os.getpid()}", command=("historical-backfill", args.action), authorization_id=authorization_id, target_identity=target_identity, authorization_fingerprint=authorization_fingerprint, authorization_lineage=authorization_lineage, heartbeat_interval_seconds=AUTHORIZED_HEARTBEAT_INTERVAL_SECONDS if args.execution_authorization is not None else None)
        print(_canonical_json(outcome))
        return 0 if outcome["state"] == "ok" else 1
