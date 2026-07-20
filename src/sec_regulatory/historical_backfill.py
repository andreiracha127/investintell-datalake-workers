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
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, cast
from uuid import UUID, uuid4

from . import manifests


class BackfillSafetyError(RuntimeError):
    """A historical run cannot establish a safe, reproducible boundary."""


class AmbiguousCommitError(BackfillSafetyError):
    """COMMIT was issued but its definitive database outcome is unknown."""


@dataclass(frozen=True)
class SourceSpec:
    form: str
    root: Path
    expected_packages: int


@dataclass(frozen=True)
class _RecoveryGovernedEvidence:
    package_id: UUID
    run_id: UUID
    package_sha256: str
    supervisor_run_id: UUID
    authorization_fingerprint: str
    commit_outcome: str


IMMUTABLE_SOURCES = (
    SourceSpec("nport", Path(r"E:\Edgard\nport"), 26),
    SourceSpec("ncen", Path(r"E:\Edgard\ncen"), 17),
    SourceSpec("rr1", Path(r"E:\Edgard\RR1"), 39),
)
EXCLUDED_ROOT = Path(r"E:\Edgard\13-F")
DEFAULT_RUN_DIR = Path(r"E:\investintell-sec-runs\historical-backfill")
PRODUCTION_SOURCE_MOUNT = Path("/srv/sec-corpus")
PRODUCTION_STATE_ROOT = Path("/var/lib/sec-backfill")
PRODUCTION_SOURCES = (
    SourceSpec("nport", PRODUCTION_SOURCE_MOUNT / "nport", 26),
    SourceSpec("ncen", PRODUCTION_SOURCE_MOUNT / "ncen", 17),
    SourceSpec("rr1", PRODUCTION_SOURCE_MOUNT / "RR1", 39),
)
SUCCESS_STATES = frozenset({"raw_validated", "duplicate"})
FAILURE_REASON_CODES = frozenset({"source_drift", "executor_exception", "unexpected_package_state", "executor_unconfigured", "heartbeat_renewal_failed", "lock_busy", "authorization_refusal", "target_refusal", "privilege_refusal", "executor_refusal", "ingester_failed", "executor_reported_failure"})
_QUARTER = re.compile(r"(?P<year>\d{4})[^0-9]*q(?P<quarter>[1-4])", re.IGNORECASE)
_SENSITIVE_KEYS = frozenset({"password", "token", "secret", "credential", "dsn", "database_url", "connection_url"})
_SENSITIVE_SUFFIXES = ("_password", "_token", "_secret", "_credential", "_dsn")
_DATABASE_SCHEMES = ("postgres://", "postgresql://")
_SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"^bearer\s+[a-z0-9._~-]+$", re.IGNORECASE),
    re.compile(r"-----BEGIN (?:[A-Z ]+ )?(?:PRIVATE KEY|CERTIFICATE)-----"),
    re.compile(r"(?:aws_access_key_id|aws_secret_access_key|client_secret|private_key)\s*[:=]", re.IGNORECASE),
    re.compile(r"\beyJ[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\b"),
)
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
        "source_roots",
        "authorization_id",
        "stop_contract_hash",
        "reconciliation_contract_hash",
        "preflight_attestation",
        "runner_attestation",
        "secret_version_resource",
        "execution_mode",
        "package_scope",
        "canary_certificate",
        "supervisor_run_id",
    }
)
EXACT_MONITORED_RELATIONS = frozenset(
    {
        "public.sec_ingestion_runs", "public.sec_source_packages", "public.sec_source_files",
        "public.sec_source_package_transitions", "public.sec_table_reconciliations",
        "public.sec_row_issues", "public.sec_run_transitions", "public.sec_validated_raw_visibility",
        "public.sec_raw_validation_tokens", "public.nport_raw_rows", "public.nport_holding_accession_map",
        "public.nport_contract_tables", "public.ncen_raw_v2_rows", "public.ncen_contract_tables",
        "public.rr1_raw_v2_rows", "public.rr1_contract_tables",
    }
)
# The execution authorization remains bound to the complete signed relation
# boundary.  Live preflight separately proves the narrower direct DML surface.
EXACT_WRITABLE_TABLES = EXACT_MONITORED_RELATIONS
EXACT_DIRECT_TABLE_PRIVILEGES = {
    "public.sec_ingestion_runs": ("SELECT", "INSERT", "UPDATE"),
    "public.sec_source_packages": ("SELECT", "INSERT", "UPDATE"),
    "public.sec_source_files": ("SELECT", "INSERT", "UPDATE"),
    "public.sec_source_package_transitions": ("SELECT",),
    "public.sec_table_reconciliations": ("SELECT", "INSERT", "UPDATE"),
    "public.sec_row_issues": ("SELECT", "INSERT", "UPDATE"),
    "public.sec_run_transitions": ("SELECT",),
    "public.sec_validated_raw_visibility": ("SELECT",),
    "public.sec_raw_validation_tokens": ("SELECT",),
    "public.nport_raw_rows": ("SELECT", "INSERT", "UPDATE"),
    "public.nport_holding_accession_map": ("SELECT", "INSERT", "DELETE"),
    "public.nport_contract_tables": ("SELECT",),
    "public.ncen_raw_v2_rows": ("SELECT", "INSERT"),
    "public.ncen_contract_tables": ("SELECT",),
    "public.rr1_raw_v2_rows": ("SELECT", "INSERT"),
    "public.rr1_contract_tables": ("SELECT",),
}
EXACT_DIRECT_WRITABLE_TABLES = frozenset(
    table for table, verbs in EXACT_DIRECT_TABLE_PRIVILEGES.items() if set(verbs) - {"SELECT"}
)
_PREFLIGHT_ATTESTATION_FIELDS = frozenset(
    {
        "cluster_identity", "tls_identity", "role_identity", "fixed_memberships",
        "role_capabilities", "object_catalog_hash", "object_identities", "table_privileges", "column_privileges",
        "sequence_privileges", "function_privileges", "database_privileges", "monitoring_privileges",
        "effective_writable_tables", "truncate_tables", "public_acl",
        "unsafe_security_definers", "non_sec_privilege_inventory", "non_sec_privilege_inventory_hash",
        "non_sec_effective_write_privileges", "trigger_write_targets",
    }
)
_NON_SEC_INVENTORY_FIELDS = frozenset({"relations", "sequences", "routines", "schemas", "public_acl", "monitoring"})
_AGGREGATE_SUPPORT_FUNCTIONS = (
    "transition", "final", "combine", "serial", "deserial",
    "moving_transition", "moving_inverse", "moving_final",
)
_AGGREGATE_DEFINITION_FIELDS = frozenset({
    "kind", "num_direct_args", "support_functions", "sort_operator", "transition_type",
    "moving_transition_type", "transition_space", "moving_transition_space",
    "initial_value", "moving_initial_value", "final_extra", "moving_final_extra",
    "final_modify", "moving_final_modify", "parallel",
})
_AGGREGATE_FUNCTION_FIELDS = frozenset({
    "oid", "identity", "owner", "language", "security_definer", "proconfig",
    "search_path", "acl", "effective_callable", "definition_sha256",
})
_AGGREGATE_FUNCTION_ACL_FIELDS = frozenset({"acl_source", "grantee", "privilege", "effective"})
_AGGREGATE_OPERATOR_FIELDS = frozenset({
    "oid", "identity", "owner", "implementation", "restriction", "join", "definition_sha256",
})
_RUNNER_ATTESTATION_FIELDS = frozenset({"project", "service_account", "disk_identity"})
_SCOPE_ENTRY_FIELDS = frozenset({"identity", "package_sha256"})
_CANARY_CERTIFICATE_FIELDS = frozenset({"certificate_id", "certificate_sha256", "canary_supervisor_run_id", "canary_authorization_fingerprint", "inventory_hash", "packages"})
_LEGACY_CANARY_CERTIFICATE_FIELDS = frozenset({"certificate_id", "canary_run_id", "canary_authorization_fingerprint", "inventory_hash", "packages"})
_CERTIFICATE_PACKAGE_FIELDS = frozenset({"identity", "package_sha256", "package_id", "ingestion_run_id", "reconciliation_sha256"})
_RECOVERY_AUTHORIZATION_FIELDS = frozenset({
    "schema_version", "stage", "code_sha", "inventory_hash", "status_path",
    "original_authorization_fingerprint", "supervisor_run_id", "identity",
    "package_id", "run_id", "package_sha256", "reconciliation_sha256",
    "secret_version_resource", "recovery_authorization_id", "expected_outcome",
    "recovery_evidence_sha256",
})
_ROLE_CAPABILITY_FIELDS = frozenset({"is_superuser", "owns_any_table", "can_create_role", "can_create_database", "bypass_rls", "schema_create", "set_role", "no_memberships"})
_OBJECT_IDENTITY_FIELDS = frozenset({"relations", "columns", "constraints", "indexes", "triggers", "sequences", "routines"})
EXACT_IDENTITY_SEQUENCES = frozenset({
    "public.sec_source_package_transitions_package_transition_id_seq",
    "public.sec_table_reconciliations_reconciliation_id_seq",
    "public.sec_row_issues_issue_id_seq",
    "public.sec_run_transitions_transition_id_seq",
    "public.nport_raw_rows_raw_row_id_seq",
    "public.ncen_raw_v2_rows_raw_row_id_seq",
    "public.rr1_raw_v2_rows_raw_row_id_seq",
})
EXACT_SECURITY_DEFINER_ROUTINES = frozenset({
    "public.nport_contract_catalog_payload()",
    "public.nport_contract_catalog_sha256()",
    "public.nport_install_contract_catalog(jsonb)",
    "public.sec_raw_validation_token_present(uuid)",
    "public.sec_run_lifecycle_guard()",
    "public.sec_validate_raw_run(uuid,text)",
    "public.sec_record_commit_outcome(uuid,uuid,character,character,text)",
    "public.sec_resolve_ambiguous_commit_outcome(uuid,uuid,character,character,character,character,text)",
    "public.sec_promote_certified_canary_package(uuid,uuid,uuid,character,character,character,uuid,character)",
    "public.sec_query_governed_evidence(uuid,uuid,character)",
    "public.sec_audit_package_discovery()",
    "public.sec_audit_run_lifecycle()",
})
EXACT_DIRECT_USAGE_SEQUENCES = frozenset({
    "public.sec_table_reconciliations_reconciliation_id_seq",
    "public.sec_row_issues_issue_id_seq",
    "public.nport_raw_rows_raw_row_id_seq",
    "public.ncen_raw_v2_rows_raw_row_id_seq",
    "public.rr1_raw_v2_rows_raw_row_id_seq",
})
EXACT_DIRECT_EXECUTE_ROUTINES = frozenset({
    "public.sec_validate_raw_run(uuid,text)",
    "public.sec_record_commit_outcome(uuid,uuid,character,character,text)",
    "public.sec_resolve_ambiguous_commit_outcome(uuid,uuid,character,character,character,character,text)",
    "public.sec_query_governed_evidence(uuid,uuid,character)",
    "public.sec_promote_certified_canary_package(uuid,uuid,uuid,character,character,character,uuid,character)",
})
EXACT_TRIGGER_WRITE_TARGETS = frozenset({
    "public.ncen_raw_v2_rows:ncen_raw_v2_rows_lock_delete",
    "public.ncen_raw_v2_rows:ncen_raw_v2_rows_lock_insert",
    "public.ncen_raw_v2_rows:ncen_raw_v2_rows_lock_update",
    "public.ncen_raw_v2_rows:ncen_raw_v2_rows_provenance_insert",
    "public.ncen_raw_v2_rows:ncen_raw_v2_rows_provenance_update",
    "public.nport_holding_accession_map:nport_holding_map_lock_delete",
    "public.nport_holding_accession_map:nport_holding_map_lock_insert",
    "public.nport_holding_accession_map:nport_holding_map_lock_update",
    "public.nport_raw_rows:nport_raw_rows_lock_delete",
    "public.nport_raw_rows:nport_raw_rows_lock_insert",
    "public.nport_raw_rows:nport_raw_rows_lock_update",
    "public.nport_raw_rows:nport_raw_rows_provenance",
    "public.nport_raw_rows:nport_raw_rows_provenance_update",
    "public.rr1_raw_v2_rows:rr1_raw_v2_rows_lock_delete",
    "public.rr1_raw_v2_rows:rr1_raw_v2_rows_lock_insert",
    "public.rr1_raw_v2_rows:rr1_raw_v2_rows_lock_update",
    "public.rr1_raw_v2_rows:rr1_raw_v2_rows_provenance_insert",
    "public.rr1_raw_v2_rows:rr1_raw_v2_rows_provenance_update",
    "public.sec_ingestion_runs:sec_ingestion_runs_lifecycle_audit",
    "public.sec_row_issues:sec_row_issues_active_disposition",
    "public.sec_row_issues:sec_row_issues_raw_immutable",
    "public.sec_source_files:sec_source_files_raw_immutable",
    "public.sec_source_packages:sec_source_packages_discovery_audit",
    "public.sec_table_reconciliations:sec_table_reconciliations_raw_immutable",
})
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
_UNSET = object()


def _is_sensitive_key(key: str) -> bool:
    normalized = key.casefold()
    return normalized in _SENSITIVE_KEYS or normalized.endswith(_SENSITIVE_SUFFIXES)


def _is_sensitive_value(value: str) -> bool:
    normalized = value.casefold()
    conninfo_secret = re.search(r"(?:^|\s)(?:password|passfile|sslpassword|sslkey|sslcert|sslrootcert|sslcrl|sslcrldir|service|servicefile|token|secret|private[_-]?key)\s*=\s*(?:'[^']*'|\"[^\"]*\"|\S+)", value, re.IGNORECASE)
    return normalized.startswith(_DATABASE_SCHEMES) or bool(conninfo_secret) or any(pattern.search(value) for pattern in _SENSITIVE_VALUE_PATTERNS)


def _assert_no_symlink_components(path: Path) -> None:
    """Reject every link in a source path, not only the final component."""
    current = Path(path.anchor) if path.anchor else Path(".")
    for part in path.parts[1 if path.anchor else 0:]:
        current = current / part
        if _is_reparse(current):
            raise BackfillSafetyError(f"source symlink or reparse point is forbidden: {current}")


def _mount_evidence(path: Path) -> dict[str, object]:
    """Return the only mount facts required by the portable production gate."""
    try:
        statvfs = getattr(os, "statvfs", None)
        if not callable(statvfs):
            raise BackfillSafetyError("statvfs is unavailable for Linux mount validation")
        stats = statvfs(path)
        read_only = bool(stats.f_flag & getattr(os, "ST_RDONLY", 1))
    except (AttributeError, OSError) as exc:
        raise BackfillSafetyError(f"unable to establish mount evidence: {path}") from exc
    try:
        device = path.stat().st_dev
    except OSError as exc:
        raise BackfillSafetyError(f"unable to establish mount device: {path}") from exc
    return {"read_only": read_only, "device": device, "durable": True}


def validate_production_paths(
    sources: Sequence[SourceSpec],
    run_directory: Path,
    *,
    source_mount: Path = PRODUCTION_SOURCE_MOUNT,
    state_root: Path = PRODUCTION_STATE_ROOT,
    mount_inspector: Callable[[Path], Mapping[str, object]] | None = None,
) -> None:
    """Validate immutable Linux roots and a separate durable state filesystem."""
    expected_counts = {"nport": 26, "ncen": 17, "rr1": 39} if source_mount == PRODUCTION_SOURCE_MOUNT else {spec.form: spec.expected_packages for spec in sources}
    expected = (("nport", "nport", expected_counts["nport"]), ("ncen", "ncen", expected_counts["ncen"]), ("rr1", "RR1", expected_counts["rr1"]))
    if len(sources) != len(expected):
        raise BackfillSafetyError("production source root configuration is incomplete")
    if not source_mount.is_absolute() or not state_root.is_absolute() or not run_directory.is_absolute():
        raise BackfillSafetyError("production roots and run directory must be absolute POSIX paths")
    inspect = mount_inspector or _mount_evidence
    for spec, (form, name, count) in zip(sources, expected, strict=True):
        expected_root = source_mount / name
        if spec.form != form or spec.root != expected_root or spec.root.name != name or spec.expected_packages != count:
            raise BackfillSafetyError("production source root case or contract differs")
        if not spec.root.is_dir():
            raise BackfillSafetyError(f"production source root is unavailable: {spec.root}")
        _assert_no_symlink_components(spec.root)
        for entry in spec.root.rglob("*"):
            _assert_no_symlink_components(entry)
            try:
                if not entry.resolve(strict=True).is_relative_to(spec.root.resolve(strict=True)):
                    raise BackfillSafetyError("production source entry escapes its root")
            except OSError as exc:
                raise BackfillSafetyError("production source entry resolution failed") from exc
        try:
            if spec.root.resolve(strict=True) != expected_root.resolve(strict=True):
                raise BackfillSafetyError("production source root resolves outside its exact path")
        except OSError as exc:
            raise BackfillSafetyError("production source root resolution failed") from exc
        evidence = inspect(spec.root)
        if set(evidence) != {"read_only", "device", "durable"} or evidence["read_only"] is not True:
            raise BackfillSafetyError("production source filesystem must be read-only")
    if not run_directory.is_relative_to(state_root) or run_directory == state_root:
        raise BackfillSafetyError("production run directory must be below the durable state root")
    if run_directory.is_relative_to(source_mount) or state_root.is_relative_to(source_mount) or source_mount.is_relative_to(state_root):
        raise BackfillSafetyError("production state/source roots overlap")
    state_evidence = inspect(state_root)
    if set(state_evidence) != {"read_only", "device", "durable"} or state_evidence["read_only"] is not False or state_evidence["durable"] is not True:
        raise BackfillSafetyError("production state filesystem is not writable and durable")
    source_device = inspect(sources[0].root)["device"]
    if state_evidence["device"] == source_device:
        raise BackfillSafetyError("production state must be on a different filesystem")
    excluded = source_mount / "13-F"
    if excluded.exists():
        raise BackfillSafetyError("production source mount unexpectedly contains excluded 13-F")


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


def _authorization_sources(document: Mapping[str, object]) -> tuple[SourceSpec, ...]:
    roots = cast(Mapping[str, str], document["source_roots"])
    return tuple(
        SourceSpec(form, Path(roots[form]), count)
        for form, count in (("nport", 26), ("ncen", 17), ("rr1", 39))
    )


def _load_authorized_source_configuration(path: Path, *, code_sha: str, run_directory: Path, command: Sequence[str]) -> tuple[str, tuple[SourceSpec, ...]]:
    """Read only the non-secret startup bindings needed to build an inventory."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackfillSafetyError("invalid execution authorization artifact") from exc
    if document == {}:  # Explicit test seam; real authorizations are always schema v3 below.
        return "test_unbound", IMMUTABLE_SOURCES
    if not isinstance(document, dict) or document.get("schema_version") not in {3, 4} or document.get("stage") != "phase4_historical_backfill" or document.get("code_sha") != code_sha:
        raise BackfillSafetyError("invalid execution authorization source configuration")
    if document.get("run_directory") != str(run_directory.resolve()) or document.get("sanitized_command") != list(command):
        raise BackfillSafetyError("execution authorization source/run binding mismatch")
    mode = document.get("target_mode")
    if mode not in {"local_disposable", "production_authorized"}:
        raise BackfillSafetyError("invalid execution authorization target mode")
    roots = document.get("source_roots")
    if not isinstance(roots, dict) or set(roots) != {"nport", "ncen", "rr1"} or not all(isinstance(value, str) and (Path(value).is_absolute() or bool(re.fullmatch(r"[A-Za-z]:[\\/].*", value))) for value in roots.values()):
        raise BackfillSafetyError("invalid execution authorization source roots")
    return cast(str, mode), _authorization_sources(document)


def _validate_historical_boundary(
    inventory: Mapping[str, object],
    sources: Sequence[SourceSpec] | None = None,
    *,
    verify_contents: bool = True,
) -> None:
    """Apply the fixed 82-package production policy, never fixture roots."""
    sources = IMMUTABLE_SOURCES if sources is None else sources
    if tuple(sources) == IMMUTABLE_SOURCES:
        validate_immutable_roots()
    _validate_inventory(inventory)
    expected_roots = [{"form": spec.form, "root": str(spec.root)} for spec in sources]
    expected_counts = Counter({"nport": 26, "ncen": 17, "rr1": 39})
    packages = cast(list[dict[str, object]], inventory["packages"])
    observed_counts = Counter(cast(str, package["form"]) for package in packages)
    if inventory.get("roots") != expected_roots or len(packages) != 82 or observed_counts != expected_counts:
        raise BackfillSafetyError("historical inventory differs from immutable 82-package source policy")
    roots = {spec.form: spec.root for spec in sources}
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
        if verify_contents:
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


def _validate_scope(scope: object, *, inventory_hash: str, label: str) -> list[dict[str, str]]:
    if not isinstance(scope, list) or not scope:
        raise BackfillSafetyError(f"invalid {label} package scope")
    seen: set[str] = set()
    validated: list[dict[str, str]] = []
    for item in scope:
        if not isinstance(item, dict) or set(item) != _SCOPE_ENTRY_FIELDS:
            raise BackfillSafetyError(f"invalid {label} package scope")
        identity = item.get("identity")
        package_hash = item.get("package_sha256")
        if not isinstance(identity, str) or not identity or not _is_sha256(package_hash) or identity in seen:
            raise BackfillSafetyError(f"invalid {label} package scope")
        seen.add(identity)
        validated.append({"identity": identity, "package_sha256": cast(str, package_hash)})
    return validated


def _validate_non_sec_privilege_inventory(inventory: object, inventory_hash: object) -> None:
    """Validate inventory shape without treating non-SEC read surfaces as grants."""
    if not isinstance(inventory, dict) or set(inventory) != _NON_SEC_INVENTORY_FIELDS:
        raise BackfillSafetyError("invalid non-SEC privilege inventory")
    if not isinstance(inventory_hash, str) or not _is_sha256(inventory_hash) or _sha256_bytes(_canonical_json(inventory).encode("ascii")) != inventory_hash:
        raise BackfillSafetyError("non-SEC privilege inventory hash mismatch")

    def sorted_unique_records(value: object) -> list[Mapping[str, object]]:
        if not isinstance(value, list) or value != sorted(value, key=_canonical_json):
            raise BackfillSafetyError("invalid non-SEC privilege inventory")
        records: list[Mapping[str, object]] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, Mapping):
                raise BackfillSafetyError("invalid non-SEC privilege inventory")
            encoded = _canonical_json(item)
            if encoded in seen:
                raise BackfillSafetyError("invalid non-SEC privilege inventory")
            seen.add(encoded)
            records.append(item)
        return records

    common_fields = {"identity", "schema", "owner", "extension", "acl_source", "grantee", "privileges"}
    allowed_privileges = {
        "relations": {"SELECT"}, "sequences": {"SELECT", "USAGE", "UPDATE"},
        "routines": {"EXECUTE"}, "schemas": {"USAGE"},
    }

    def validate_function_dependency(value: object) -> None:
        if not isinstance(value, Mapping) or set(value) != _AGGREGATE_FUNCTION_FIELDS:
            raise BackfillSafetyError("invalid non-SEC aggregate function dependency")
        if type(value["oid"]) is not int or value["oid"] <= 0:
            raise BackfillSafetyError("invalid non-SEC aggregate function dependency")
        if not all(isinstance(value[field], str) and value[field] for field in ("identity", "owner", "language")):
            raise BackfillSafetyError("invalid non-SEC aggregate function dependency")
        if not isinstance(value["security_definer"], bool) or not isinstance(value["effective_callable"], bool):
            raise BackfillSafetyError("invalid non-SEC aggregate function dependency")
        if not isinstance(value["proconfig"], list) or not all(isinstance(item, str) for item in value["proconfig"]):
            raise BackfillSafetyError("invalid non-SEC aggregate function dependency")
        if value["search_path"] is not None and not isinstance(value["search_path"], str):
            raise BackfillSafetyError("invalid non-SEC aggregate function dependency")
        if not _is_sha256(value["definition_sha256"]):
            raise BackfillSafetyError("invalid non-SEC aggregate function dependency")
        acl = value["acl"]
        if not isinstance(acl, list) or acl != sorted(acl, key=_canonical_json):
            raise BackfillSafetyError("invalid non-SEC aggregate function ACL dependency")
        seen_acl: set[str] = set()
        for entry in acl:
            if not isinstance(entry, Mapping) or set(entry) != _AGGREGATE_FUNCTION_ACL_FIELDS:
                raise BackfillSafetyError("invalid non-SEC aggregate function ACL dependency")
            encoded = _canonical_json(entry)
            if encoded in seen_acl:
                raise BackfillSafetyError("invalid non-SEC aggregate function ACL dependency")
            seen_acl.add(encoded)
            if entry["acl_source"] not in {"PUBLIC", "DIRECT", "INHERITED", "OTHER"} or not isinstance(entry["grantee"], str) or not entry["grantee"] or entry["privilege"] != "EXECUTE" or not isinstance(entry["effective"], bool):
                raise BackfillSafetyError("invalid non-SEC aggregate function ACL dependency")
            if (entry["acl_source"] == "PUBLIC") is not (entry["grantee"] == "PUBLIC"):
                raise BackfillSafetyError("invalid non-SEC aggregate function ACL dependency")
            if entry["effective"] is not (entry["acl_source"] != "OTHER"):
                raise BackfillSafetyError("invalid non-SEC aggregate function ACL dependency")

    def validate_operator_dependency(value: object) -> None:
        if not isinstance(value, Mapping) or set(value) != _AGGREGATE_OPERATOR_FIELDS:
            raise BackfillSafetyError("invalid non-SEC aggregate operator dependency")
        if type(value["oid"]) is not int or value["oid"] <= 0 or not all(isinstance(value[field], str) and value[field] for field in ("identity", "owner")):
            raise BackfillSafetyError("invalid non-SEC aggregate operator dependency")
        for field in ("implementation", "restriction", "join"):
            if value[field] is not None:
                validate_function_dependency(value[field])
        if value["implementation"] is None:
            raise BackfillSafetyError("invalid non-SEC aggregate operator dependency")
        definition = {key: item for key, item in value.items() if key != "definition_sha256"}
        if value["definition_sha256"] != _sha256_bytes(_canonical_json(definition).encode("ascii")):
            raise BackfillSafetyError("non-SEC aggregate operator definition hash mismatch")

    for category in ("relations", "sequences", "routines", "schemas"):
        for record in sorted_unique_records(inventory[category]):
            required = common_fields | ({"pg_monitor_surface"} if category == "relations" else set())
            if category == "routines":
                required |= {"prokind", "security_definer", "security_definer_classification", "proconfig", "search_path", "definition_sha256", "side_effect_keywords", "aggregate_definition"}
            if set(record) != required or not all(isinstance(record[field], str) and record[field] for field in ("identity", "schema", "owner", "grantee")):
                raise BackfillSafetyError("invalid non-SEC privilege inventory")
            if record["extension"] is not None and (not isinstance(record["extension"], str) or not record["extension"]):
                raise BackfillSafetyError("invalid non-SEC privilege inventory")
            if record["acl_source"] not in {"INHERITED", "PUBLIC"} or (record["acl_source"] == "PUBLIC") is not (record["grantee"] == "PUBLIC"):
                raise BackfillSafetyError("non-SEC direct or malformed privilege inventory")
            privileges = record["privileges"]
            if not isinstance(privileges, list) or not privileges or privileges != sorted(set(privileges)) or not all(isinstance(value, str) and value in allowed_privileges[category] for value in privileges):
                raise BackfillSafetyError("invalid non-SEC privilege inventory")
            if category == "relations" and not isinstance(record["pg_monitor_surface"], bool):
                raise BackfillSafetyError("invalid non-SEC privilege inventory")
            if category == "routines":
                if record["prokind"] not in {"f", "p", "a", "w"} or not isinstance(record["security_definer"], bool) or not isinstance(record["proconfig"], list) or not all(isinstance(value, str) for value in record["proconfig"]) or record["search_path"] is not None and not isinstance(record["search_path"], str) or not isinstance(record["definition_sha256"], str) or not _is_sha256(record["definition_sha256"]) or not isinstance(record["side_effect_keywords"], list) or record["side_effect_keywords"] != sorted(set(record["side_effect_keywords"])) or not all(isinstance(value, str) and value in {"INSERT", "UPDATE", "DELETE", "TRUNCATE", "CREATE", "ALTER", "DROP", "COPY", "CALL"} for value in record["side_effect_keywords"]):
                    raise BackfillSafetyError("invalid non-SEC routine inventory")
                classification = record["security_definer_classification"]
                if (record["security_definer"] and classification != "PENDING_SIGNED_BASELINE") or (not record["security_definer"] and classification is not None):
                    raise BackfillSafetyError("unclassified non-SEC SECURITY DEFINER inventory")
                aggregate_definition = record["aggregate_definition"]
                if record["prokind"] != "a":
                    if aggregate_definition is not None:
                        raise BackfillSafetyError("invalid non-SEC aggregate definition inventory")
                    continue
                if not isinstance(aggregate_definition, Mapping) or set(aggregate_definition) != _AGGREGATE_DEFINITION_FIELDS:
                    raise BackfillSafetyError("invalid non-SEC aggregate definition inventory")
                string_fields = {"kind", "transition_type", "moving_transition_type", "final_modify", "moving_final_modify", "parallel"}
                if not all(isinstance(aggregate_definition[field], str) and aggregate_definition[field] for field in string_fields):
                    raise BackfillSafetyError("invalid non-SEC aggregate definition inventory")
                if aggregate_definition["kind"] not in {"n", "o", "h"} or aggregate_definition["final_modify"] not in {"r", "s", "w"} or aggregate_definition["moving_final_modify"] not in {"r", "s", "w"} or aggregate_definition["parallel"] not in {"s", "r", "u"}:
                    raise BackfillSafetyError("invalid non-SEC aggregate definition inventory")
                if any(type(aggregate_definition[field]) is not int or aggregate_definition[field] < 0 for field in ("num_direct_args", "transition_space", "moving_transition_space")):
                    raise BackfillSafetyError("invalid non-SEC aggregate definition inventory")
                if any(not isinstance(aggregate_definition[field], bool) for field in ("final_extra", "moving_final_extra")):
                    raise BackfillSafetyError("invalid non-SEC aggregate definition inventory")
                if any(aggregate_definition[field] is not None and not isinstance(aggregate_definition[field], str) for field in ("initial_value", "moving_initial_value")):
                    raise BackfillSafetyError("invalid non-SEC aggregate definition inventory")
                if record["security_definer"] or record["proconfig"] or record["search_path"] is not None or record["side_effect_keywords"]:
                    raise BackfillSafetyError("invalid non-SEC aggregate definition inventory")
                support_functions = aggregate_definition["support_functions"]
                if not isinstance(support_functions, Mapping) or set(support_functions) != set(_AGGREGATE_SUPPORT_FUNCTIONS):
                    raise BackfillSafetyError("invalid non-SEC aggregate support dependency inventory")
                for name, dependency in support_functions.items():
                    if dependency is not None:
                        validate_function_dependency(dependency)
                    elif name == "transition":
                        raise BackfillSafetyError("invalid non-SEC aggregate support dependency inventory")
                if aggregate_definition["sort_operator"] is not None:
                    validate_operator_dependency(aggregate_definition["sort_operator"])
                if record["definition_sha256"] != _sha256_bytes(_canonical_json(aggregate_definition).encode("ascii")):
                    raise BackfillSafetyError("non-SEC aggregate definition hash mismatch")
    for record in sorted_unique_records(inventory["public_acl"]):
        if set(record) != {"object_kind", "identity", "schema", "owner", "extension", "privilege"} or record["object_kind"] not in {"relation", "routine", "sequence", "schema"} or not all(isinstance(record[field], str) and record[field] for field in ("identity", "schema", "owner", "privilege")) or record["extension"] is not None and not isinstance(record["extension"], str):
            raise BackfillSafetyError("invalid non-SEC PUBLIC privilege inventory")
    for record in sorted_unique_records(inventory["monitoring"]):
        if set(record) != {"identity", "membership", "surface"} or not all(isinstance(record[field], str) and record[field] for field in record) or record["membership"] not in {"DIRECT_MEMBER_NO_SET", "INHERITED_USAGE"} or record["surface"] != "pg_monitor":
            raise BackfillSafetyError("invalid non-SEC monitoring inventory")


def _validate_preflight_attestation(attestation: object) -> dict[str, object]:
    if not isinstance(attestation, dict) or set(attestation) != _PREFLIGHT_ATTESTATION_FIELDS:
        raise BackfillSafetyError("invalid production preflight attestation")
    scalar_fields = ("cluster_identity", "tls_identity", "role_identity", "object_catalog_hash")
    if any(not isinstance(attestation[field], str) or not attestation[field] for field in scalar_fields):
        raise BackfillSafetyError("invalid production preflight attestation")
    if not _is_sha256(attestation["object_catalog_hash"]):
        raise BackfillSafetyError("invalid production preflight attestation")
    memberships = attestation["fixed_memberships"]
    if not isinstance(memberships, list) or not all(isinstance(value, str) and value for value in memberships) or memberships != sorted(set(memberships)):
        raise BackfillSafetyError("invalid production preflight attestation")
    capabilities = attestation["role_capabilities"]
    forbidden_capabilities = _ROLE_CAPABILITY_FIELDS - {"no_memberships"}
    if not isinstance(capabilities, dict) or set(capabilities) != _ROLE_CAPABILITY_FIELDS or any(capabilities[field] is not False for field in forbidden_capabilities) or capabilities["no_memberships"] is not (not memberships):
        raise BackfillSafetyError("invalid production preflight role capability matrix")
    objects = attestation["object_identities"]
    if not isinstance(objects, dict) or set(objects) != _OBJECT_IDENTITY_FIELDS or any(not isinstance(value, list) or not value or value != sorted(set(value)) or not all(isinstance(item, str) and item for item in value) for value in objects.values()):
        raise BackfillSafetyError("invalid production preflight object identity matrix")
    def object_identity(item: str) -> str:
        return item.split("|", 1)[0] if "|" in item else item.rsplit(":", 1)[0]

    if {object_identity(item) for item in objects["relations"]} != EXACT_MONITORED_RELATIONS:
        raise BackfillSafetyError("invalid production preflight relation identity set")
    if {object_identity(item) for item in objects["sequences"]} != EXACT_IDENTITY_SEQUENCES or len(objects["sequences"]) != 7:
        raise BackfillSafetyError("invalid production preflight identity sequence set")
    if {object_identity(item) for item in objects["routines"]} != EXACT_SECURITY_DEFINER_ROUTINES or len(objects["routines"]) != len(EXACT_SECURITY_DEFINER_ROUTINES):
        raise BackfillSafetyError("invalid production preflight SECURITY DEFINER routine set")
    for category in ("relations", "routines"):
        if any("|owner=" not in item or not re.search(r"\|definition_sha256=[0-9a-f]{64}(?:\||$)", item) for item in objects[category]):
            raise BackfillSafetyError("production preflight object definitions are incomplete")
    if _sha256_bytes(_canonical_json(objects).encode("ascii")) != attestation["object_catalog_hash"]:
        raise BackfillSafetyError("production preflight object catalog hash mismatch")
    table_privileges = attestation["table_privileges"]
    expected_table_privileges = {table: list(verbs) for table, verbs in EXACT_DIRECT_TABLE_PRIVILEGES.items()}
    if not isinstance(table_privileges, dict) or table_privileges != expected_table_privileges:
        raise BackfillSafetyError("invalid production preflight table privilege matrix")
    column_privileges = attestation["column_privileges"]
    if not isinstance(column_privileges, list) or column_privileges:
        raise BackfillSafetyError("production preflight column privilege is unsafe")
    database_privileges = attestation["database_privileges"]
    if database_privileges != {"CONNECT": True, "CREATE": False, "TEMPORARY": False}:
        raise BackfillSafetyError("invalid production preflight database privilege matrix")
    effective_writable = attestation["effective_writable_tables"]
    truncate_tables = attestation["truncate_tables"]
    if not isinstance(effective_writable, list) or effective_writable != sorted(EXACT_DIRECT_WRITABLE_TABLES) or not all(isinstance(value, str) for value in effective_writable):
        raise BackfillSafetyError("invalid effective writable table set")
    if not isinstance(truncate_tables, list) or truncate_tables or not all(isinstance(value, str) for value in truncate_tables):
        raise BackfillSafetyError("TRUNCATE privilege is unsafe")
    for field, required in (("sequence_privileges", "USAGE"), ("function_privileges", "EXECUTE")):
        matrix = attestation[field]
        if not isinstance(matrix, dict) or not matrix or any(value != [required] for value in matrix.values()):
            raise BackfillSafetyError("invalid production preflight privilege matrix")
    if set(attestation["sequence_privileges"]) != EXACT_DIRECT_USAGE_SEQUENCES or set(attestation["function_privileges"]) != EXACT_DIRECT_EXECUTE_ROUTINES:
        raise BackfillSafetyError("invalid production preflight privilege identity matrix")
    monitoring = attestation["monitoring_privileges"]
    if not isinstance(monitoring, dict) or set(monitoring) != {"pg_stat_activity", "pg_locks", "pg_monitor", "pg_read_all_stats"} or monitoring["pg_stat_activity"] != ["SELECT"] or monitoring["pg_locks"] != ["SELECT"] or monitoring["pg_monitor"] != ["DIRECT_MEMBER_NO_SET"] or monitoring["pg_read_all_stats"] != ["INHERITED_USAGE"]:
        raise BackfillSafetyError("invalid production preflight monitoring privilege matrix")
    if attestation["public_acl"] != []:
        raise BackfillSafetyError("unexpected production preflight PUBLIC privilege is unsafe")
    if attestation["unsafe_security_definers"] != []:
        raise BackfillSafetyError("unsafe production preflight SECURITY DEFINER routine")
    _validate_non_sec_privilege_inventory(
        attestation["non_sec_privilege_inventory"], attestation["non_sec_privilege_inventory_hash"]
    )
    if attestation["non_sec_effective_write_privileges"] != []:
        raise BackfillSafetyError("non-SEC effective relation write privilege is unsafe")
    if attestation["trigger_write_targets"] != sorted(EXACT_TRIGGER_WRITE_TARGETS):
        raise BackfillSafetyError("invalid production preflight reviewed trigger write-target contract")
    for field in _PREFLIGHT_ATTESTATION_FIELDS - {"cluster_identity", "tls_identity", "role_identity", "object_catalog_hash", "fixed_memberships", "non_sec_privilege_inventory_hash"}:
        if not isinstance(attestation[field], (dict, list)):
            raise BackfillSafetyError("invalid production preflight attestation")
    _assert_no_secret(attestation)
    return dict(attestation)


def _validate_runner_attestation(attestation: object) -> dict[str, str]:
    if not isinstance(attestation, dict) or set(attestation) != _RUNNER_ATTESTATION_FIELDS or not all(isinstance(value, str) and value for value in attestation.values()):
        raise BackfillSafetyError("invalid runner attestation")
    _assert_no_secret(attestation)
    return cast(dict[str, str], dict(attestation))


def _validate_canary_certificate(certificate: object, *, inventory_hash: str) -> dict[str, object] | None:
    if certificate is None:
        return None
    if not isinstance(certificate, dict) or set(certificate) != _LEGACY_CANARY_CERTIFICATE_FIELDS:
        raise BackfillSafetyError("invalid canary certificate")
    if certificate.get("inventory_hash") != inventory_hash or not all(isinstance(certificate.get(field), str) and certificate[field] for field in ("certificate_id", "canary_run_id", "canary_authorization_fingerprint")) or not _is_sha256(certificate["canary_authorization_fingerprint"]):
        raise BackfillSafetyError("invalid canary certificate")
    _validate_scope(certificate.get("packages"), inventory_hash=inventory_hash, label="canary certificate")
    _assert_no_secret(certificate)
    return dict(certificate)


def _validate_v4_canary_certificate(certificate: object, *, inventory_hash: str) -> dict[str, object]:
    if not isinstance(certificate, dict) or set(certificate) != _CANARY_CERTIFICATE_FIELDS:
        raise BackfillSafetyError("invalid typed canary certificate")
    try:
        UUID(cast(str, certificate.get("certificate_id")))
        UUID(cast(str, certificate.get("canary_supervisor_run_id")))
    except (TypeError, ValueError) as exc:
        raise BackfillSafetyError("invalid typed canary certificate UUID") from exc
    if certificate.get("inventory_hash") != inventory_hash or not _is_sha256(certificate.get("canary_authorization_fingerprint")) or not _is_sha256(certificate.get("certificate_sha256")):
        raise BackfillSafetyError("invalid typed canary certificate lineage")
    packages = certificate.get("packages")
    if not isinstance(packages, list) or len(packages) != 3:
        raise BackfillSafetyError("canary certificate must contain exactly three packages")
    forms: set[str] = set()
    identities: set[str] = set()
    for item in packages:
        if not isinstance(item, dict) or set(item) != _CERTIFICATE_PACKAGE_FIELDS:
            raise BackfillSafetyError("invalid typed canary certificate package")
        identity = item.get("identity")
        if not isinstance(identity, str) or identity in identities or identity.split(":", 1)[0] not in {"nport", "ncen", "rr1"}:
            raise BackfillSafetyError("invalid typed canary certificate package identity")
        try:
            UUID(cast(str, item.get("package_id")))
            UUID(cast(str, item.get("ingestion_run_id")))
        except (TypeError, ValueError) as exc:
            raise BackfillSafetyError("invalid typed canary certificate package UUID") from exc
        if not _is_sha256(item.get("package_sha256")) or not _is_sha256(item.get("reconciliation_sha256")):
            raise BackfillSafetyError("invalid typed canary certificate package evidence")
        identities.add(identity)
        forms.add(identity.split(":", 1)[0])
    if forms != {"nport", "ncen", "rr1"}:
        raise BackfillSafetyError("canary certificate must contain one package per form")
    unhashed = {key: value for key, value in certificate.items() if key != "certificate_sha256"}
    if certificate["certificate_sha256"] != _sha256_bytes(_canonical_json(unhashed).encode("ascii")):
        raise BackfillSafetyError("canary certificate SHA mismatch")
    _assert_no_secret(certificate)
    return dict(certificate)


def _validate_execution_authorization(document: Mapping[str, object], *, code_sha: str, inventory_hash: str) -> dict[str, object]:
    """Validate a file-only authorization before resolving a connection secret."""
    legacy_fields = _AUTHORIZATION_FIELDS - {"supervisor_run_id"}
    schema_version = document.get("schema_version")
    if set(document) not in {_AUTHORIZATION_FIELDS, legacy_fields} or schema_version not in {3, 4} or document.get("stage") != "phase4_historical_backfill" or (schema_version == 4 and set(document) != _AUTHORIZATION_FIELDS):
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
    source_roots = document.get("source_roots")
    if not isinstance(source_roots, dict) or set(source_roots) != {"nport", "ncen", "rr1"} or not all(isinstance(root, str) and root for root in source_roots.values()):
        raise BackfillSafetyError("invalid execution authorization source roots")
    source_paths = {form: Path(cast(str, source_roots[form])) for form in ("nport", "ncen", "rr1")}
    if any(not (cast(str, source_roots[form]).startswith("/") or bool(re.fullmatch(r"[A-Za-z]:[\\/].*", cast(str, source_roots[form])))) for form in source_paths):
        raise BackfillSafetyError("execution authorization source roots must be absolute")
    if not isinstance(document.get("authorization_id"), str) or not document["authorization_id"]:
        raise BackfillSafetyError("execution authorization requires a separately issued authorization ID")
    supervisor_run_id = document.get("supervisor_run_id")
    if supervisor_run_id is not None:
        try:
            UUID(cast(str, supervisor_run_id))
        except (TypeError, ValueError) as exc:
            raise BackfillSafetyError("execution authorization requires a typed supervisor run UUID") from exc
    if not _is_sha256(document.get("stop_contract_hash")) or not _is_sha256(document.get("reconciliation_contract_hash")):
        raise BackfillSafetyError("invalid execution authorization contract hash")
    execution_mode = document.get("execution_mode")
    if execution_mode not in {"canary", "full"}:
        raise BackfillSafetyError("invalid execution authorization mode")
    _validate_scope(document.get("package_scope"), inventory_hash=inventory_hash, label="authorization")
    _validate_runner_attestation(document.get("runner_attestation"))
    secret_version_resource = document.get("secret_version_resource")
    if not isinstance(secret_version_resource, str) or not re.fullmatch(r"projects/[^/]+/secrets/[^/]+/versions/[1-9][0-9]*", secret_version_resource):
        raise BackfillSafetyError("invalid Secret Manager version resource")
    certificate = _validate_v4_canary_certificate(document.get("canary_certificate"), inventory_hash=inventory_hash) if schema_version == 4 and document.get("canary_certificate") is not None else _validate_canary_certificate(document.get("canary_certificate"), inventory_hash=inventory_hash)
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
        if set(writable) != EXACT_WRITABLE_TABLES:
            raise BackfillSafetyError("production execution authorization writable table allowlist differs from the exact contract")
        if source_paths != {spec.form: spec.root for spec in PRODUCTION_SOURCES} or Path(cast(str, run_directory)).is_relative_to(PRODUCTION_STATE_ROOT) is False:
            raise BackfillSafetyError("production execution authorization roots differ from the Linux contract")
        _validate_preflight_attestation(document.get("preflight_attestation"))
        if execution_mode == "full" and certificate is None:
            raise BackfillSafetyError("full production execution requires a canary certificate")
        if schema_version == 4:
            scope = cast(list[dict[str, str]], _validate_scope(document.get("package_scope"), inventory_hash=inventory_hash, label="authorization"))
            if supervisor_run_id is None:
                raise BackfillSafetyError("production execution requires a supervisor run UUID")
            if execution_mode == "canary" and (len(scope) != 3 or {item["identity"].split(":", 1)[0] for item in scope} != {"nport", "ncen", "rr1"} or certificate is not None):
                raise BackfillSafetyError("production canary scope must be exactly one package per form")
            if execution_mode == "full" and len(scope) != 82:
                raise BackfillSafetyError("production full scope must bind exactly 82 packages")
    if mode == "local_disposable" and document.get("preflight_attestation") is not None:
        raise BackfillSafetyError("local disposable authorization cannot carry production preflight attestation")
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
            "SELECT n.nspname || '.' || c.relname, c.relkind IN ('r', 'p') AND has_table_privilege(current_user, c.oid, 'TRUNCATE') "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' "
            "AND (has_table_privilege(current_user, c.oid, 'INSERT') OR has_table_privilege(current_user, c.oid, 'UPDATE') "
            "OR has_table_privilege(current_user, c.oid, 'DELETE') OR (c.relkind IN ('r', 'p') AND has_table_privilege(current_user, c.oid, 'TRUNCATE'))) "
            "ORDER BY n.nspname, c.relname"
        )
        write_rows = cursor.fetchall()
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
        "writable_tables": [row[0] for row in write_rows],
        "truncate_tables": [row[0] for row in write_rows if row[1] is True],
    }


def _query_json(connection: object, query: str, params: Sequence[object] = ()) -> object:
    """Run a fixed read-only SQL statement and require one JSON-compatible value."""
    try:
        with connection.cursor() as cursor:  # type: ignore[attr-defined]
            cursor.execute(query, params)
            row = cursor.fetchone()
    except Exception as exc:
        raise BackfillSafetyError("production preflight query failed") from exc
    if not isinstance(row, tuple) or len(row) != 1:
        raise BackfillSafetyError("production preflight query returned an uncertain shape")
    value = row[0]
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise BackfillSafetyError("production preflight query returned non-JSON data") from exc
    return value


def _connection_parameters(connection: object) -> dict[str, str]:
    """Use the psycopg3 public ConnectionInfo API and reject ambiguous adapters."""
    getter = getattr(getattr(connection, "info", None), "get_parameters", None)
    if not callable(getter):
        raise BackfillSafetyError("production preflight cannot read psycopg3 connection parameters")
    try:
        parameters = getter()
    except Exception as exc:
        raise BackfillSafetyError("production preflight cannot read connection parameters") from exc
    if not isinstance(parameters, Mapping) or not all(isinstance(key, str) and isinstance(value, str) for key, value in parameters.items()):
        raise BackfillSafetyError("production preflight received invalid connection parameters")
    libpq_sensitive = {"passfile", "password", "sslpassword", "sslkey", "sslcert", "sslrootcert", "sslcrl", "sslcrldir", "service", "servicefile"}
    safe: dict[str, str] = {}
    for key, value in cast(Mapping[str, str], parameters).items():
        if key.casefold() in libpq_sensitive:
            continue
        if _is_sensitive_key(key):
            raise BackfillSafetyError("production preflight connection parameters contain credential material")
        safe[key] = value
    return safe


def _collect_relation_security(connection: object, tables: Sequence[str] | None = None) -> dict[str, object]:
    relation_security = _query_json(
        connection,
        "SELECT jsonb_build_object('relations', coalesce((SELECT jsonb_agg("
        "n.nspname||'.'||c.relname||'|oid='||c.oid||'|owner='||pg_get_userbyid(c.relowner)||'|relkind='||c.relkind::text||'|definition_sha256='||"
        "encode(sha256(convert_to(jsonb_build_object("
        "'relkind',c.relkind,'relpersistence',c.relpersistence,'relrowsecurity',c.relrowsecurity,'relforcerowsecurity',c.relforcerowsecurity,"
        "'relispartition',c.relispartition,'relpartbound',coalesce(pg_get_expr(c.relpartbound,c.oid),''),'partition_key',coalesce(pg_get_partkeydef(c.oid),''),"
        "'access_method',coalesce(am.amname,''),'reloptions',coalesce(to_jsonb(c.reloptions),'[]'::jsonb),"
        "'view_definition',CASE WHEN c.relkind='v' THEN pg_get_viewdef(c.oid,true) ELSE NULL END,"
        "'foreign_server',fs.srvname,'foreign_server_options',coalesce(to_jsonb(fs.srvoptions),'[]'::jsonb),'foreign_options',coalesce(to_jsonb(ft.ftoptions),'[]'::jsonb),"
        "'policies',coalesce((SELECT jsonb_agg(jsonb_build_object('name',p.polname,'command',p.polcmd,'permissive',p.polpermissive,"
        "'roles',coalesce((SELECT jsonb_agg(coalesce(r.rolname,'PUBLIC') ORDER BY coalesce(r.rolname,'PUBLIC')) FROM unnest(p.polroles) role_oid LEFT JOIN pg_roles r ON r.oid=role_oid),'[]'::jsonb),"
        "'qual',coalesce(pg_get_expr(p.polqual,p.polrelid),''),'with_check',coalesce(pg_get_expr(p.polwithcheck,p.polrelid),'')) ORDER BY p.polname) FROM pg_policy p WHERE p.polrelid=c.oid),'[]'::jsonb))::text,'UTF8')),'hex') "
        "ORDER BY n.nspname,c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_am am ON am.oid=c.relam "
        "LEFT JOIN pg_foreign_table ft ON ft.ftrelid=c.oid LEFT JOIN pg_foreign_server fs ON fs.oid=ft.ftserver "
        "WHERE n.nspname='public' AND n.nspname||'.'||c.relname=ANY(%s) AND c.relkind IN ('r','p','v','f')), '[]'::jsonb))",
        (sorted(tables or EXACT_MONITORED_RELATIONS),),
    )
    if not isinstance(relation_security, Mapping) or not isinstance(relation_security.get("relations"), list):
        raise BackfillSafetyError("production preflight relation security query returned malformed JSON")
    return dict(relation_security)


def _collect_monitoring_privileges(connection: object) -> dict[str, object]:
    value = _query_json(connection, "SELECT jsonb_build_object('monitoring_privileges', jsonb_build_object('pg_stat_activity', CASE WHEN has_table_privilege(current_user, 'pg_catalog.pg_stat_activity', 'SELECT') AND (SELECT count(*) >= 0 FROM pg_stat_activity) THEN jsonb_build_array('SELECT') ELSE '[]'::jsonb END, 'pg_locks', CASE WHEN has_table_privilege(current_user, 'pg_catalog.pg_locks', 'SELECT') AND (SELECT count(*) >= 0 FROM pg_locks) THEN jsonb_build_array('SELECT') ELSE '[]'::jsonb END, 'pg_monitor', CASE WHEN EXISTS (SELECT 1 FROM pg_auth_members m JOIN pg_roles parent ON parent.oid=m.roleid JOIN pg_roles child ON child.oid=m.member WHERE child.rolname=current_user AND parent.rolname='pg_monitor' AND NOT m.set_option) AND NOT pg_has_role(current_user, 'pg_monitor', 'SET') THEN jsonb_build_array('DIRECT_MEMBER_NO_SET') ELSE '[]'::jsonb END, 'pg_read_all_stats', CASE WHEN pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') AND NOT EXISTS (SELECT 1 FROM pg_auth_members m JOIN pg_roles parent ON parent.oid=m.roleid JOIN pg_roles child ON child.oid=m.member WHERE child.rolname=current_user AND parent.rolname='pg_read_all_stats') THEN jsonb_build_array('INHERITED_USAGE') ELSE '[]'::jsonb END))")
    if not isinstance(value, Mapping) or not isinstance(value.get("monitoring_privileges"), Mapping):
        raise BackfillSafetyError("production monitoring query returned malformed JSON")
    return dict(value)


def _collect_nonrelation_object_identities(connection: object) -> dict[str, object]:
    value = _query_json(
        connection,
        "SELECT jsonb_build_object("
        "'relations','[]'::jsonb,"
        "'columns',coalesce((SELECT jsonb_agg(n.nspname||'.'||c.relname||'.'||a.attname||':'||a.attnum||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull||':'||coalesce(pg_get_expr(ad.adbin,ad.adrelid),'') ORDER BY n.nspname,c.relname,a.attnum) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum WHERE n.nspname='public' AND a.attnum>0 AND NOT a.attisdropped AND n.nspname||'.'||c.relname=ANY(%s)),'[]'::jsonb),"
        "'constraints',coalesce((SELECT jsonb_agg(n.nspname||'.'||c.relname||':'||co.conname||':'||co.oid||':'||pg_get_constraintdef(co.oid,true) ORDER BY n.nspname,c.relname,co.conname) FROM pg_constraint co JOIN pg_class c ON c.oid=co.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND n.nspname||'.'||c.relname=ANY(%s)),'[]'::jsonb),"
        "'indexes',coalesce((SELECT jsonb_agg(n.nspname||'.'||i.relname||':'||i.oid||':'||pg_get_indexdef(i.oid)||':'||coalesce(pg_get_expr(ix.indpred,ix.indrelid),'')||':'||ix.indisunique ORDER BY n.nspname,i.relname) FROM pg_class i JOIN pg_namespace n ON n.oid=i.relnamespace JOIN pg_index ix ON ix.indexrelid=i.oid WHERE n.nspname='public' AND i.relkind='i'),'[]'::jsonb),"
        "'triggers',coalesce((SELECT jsonb_agg(n.nspname||'.'||c.relname||':'||t.tgname||':'||t.oid||':'||pg_get_triggerdef(t.oid,true)||':'||p.oid ORDER BY n.nspname,c.relname,t.tgname) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid WHERE NOT t.tgisinternal AND n.nspname='public'),'[]'::jsonb),"
        "'sequences',coalesce((SELECT jsonb_agg(n.nspname||'.'||c.relname||':'||c.oid ORDER BY n.nspname,c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='S' AND n.nspname='public' AND n.nspname||'.'||c.relname=ANY(%s)),'[]'::jsonb),"
        "'routines',coalesce((SELECT jsonb_agg(n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')|oid='||p.oid||'|owner='||pg_get_userbyid(p.proowner)||'|definition_sha256='||encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex')||'|proconfig_sha256='||encode(sha256(convert_to(coalesce(array_to_string(p.proconfig,','),''),'UTF8')),'hex') ORDER BY n.nspname,p.proname,replace(oidvectortypes(p.proargtypes),', ', ',')) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.prosecdef AND n.nspname='public'),'[]'::jsonb))",
        (sorted(EXACT_MONITORED_RELATIONS), sorted(EXACT_MONITORED_RELATIONS), sorted(EXACT_IDENTITY_SEQUENCES)),
    )
    if not isinstance(value, Mapping) or set(value) != _OBJECT_IDENTITY_FIELDS:
        raise BackfillSafetyError("production object identity query returned malformed JSON")
    return dict(value)


def _collect_aggregate_function_dependencies(
    connection: object, function_oids: Sequence[int]
) -> dict[int, dict[str, object]]:
    if not function_oids:
        return {}
    value = _query_json(
        connection,
        """
        SELECT jsonb_build_object(
            'functions', coalesce(
                jsonb_object_agg(
                    p.oid::text,
                    jsonb_build_object(
                        'oid', p.oid::bigint,
                        'identity', n.nspname || '.' || p.proname || '(' ||
                            replace(oidvectortypes(p.proargtypes), ', ', ',') || ')',
                        'owner', pg_get_userbyid(p.proowner),
                        'language', l.lanname,
                        'security_definer', p.prosecdef,
                        'proconfig', coalesce(to_jsonb(p.proconfig), '[]'::jsonb),
                        'search_path', (
                            SELECT substring(setting FROM 13)
                            FROM unnest(coalesce(p.proconfig, ARRAY[]::text[])) setting
                            WHERE setting LIKE 'search_path=%%'
                            LIMIT 1
                        ),
                        'acl', coalesce(
                            (
                                SELECT jsonb_agg(
                                    jsonb_build_object(
                                        'acl_source', CASE
                                            WHEN a.grantee = 0 THEN 'PUBLIC'
                                            WHEN a.grantee = (SELECT oid FROM pg_roles WHERE rolname = current_user) THEN 'DIRECT'
                                            WHEN pg_has_role(current_user, a.grantee, 'USAGE') THEN 'INHERITED'
                                            ELSE 'OTHER'
                                        END,
                                        'grantee', coalesce(pg_get_userbyid(NULLIF(a.grantee, 0)), 'PUBLIC'),
                                        'privilege', a.privilege_type,
                                        'effective', a.grantee = 0 OR pg_has_role(current_user, a.grantee, 'USAGE')
                                    )
                                    ORDER BY a.grantee, a.privilege_type
                                )
                                FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
                                WHERE a.privilege_type = 'EXECUTE'
                            ),
                            '[]'::jsonb
                        ),
                        'effective_callable', has_function_privilege(current_user, p.oid, 'EXECUTE'),
                        'definition_sha256', encode(
                            sha256(convert_to(pg_get_functiondef(p.oid), 'UTF8')),
                            'hex'
                        )
                    )
                    ORDER BY p.oid
                ),
                '{}'::jsonb
            )
        )
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_language l ON l.oid = p.prolang
        WHERE p.oid = ANY(%s)
        """,
        (sorted(set(function_oids)),),
    )
    raw = value.get("functions") if isinstance(value, Mapping) else None
    if not isinstance(raw, Mapping) or set(raw) != {str(oid) for oid in set(function_oids)}:
        raise BackfillSafetyError("production aggregate function dependency query returned malformed JSON")
    dependencies: dict[int, dict[str, object]] = {}
    for key, item in raw.items():
        if not isinstance(item, Mapping):
            raise BackfillSafetyError("production aggregate function dependency query returned malformed JSON")
        dependency = dict(item)
        acl = dependency.get("acl")
        if isinstance(acl, list):
            dependency["acl"] = sorted(acl, key=_canonical_json)
        dependencies[int(key)] = dependency
    return dependencies


def _collect_aggregate_operators(
    connection: object, operator_oids: Sequence[int]
) -> dict[int, dict[str, object]]:
    if not operator_oids:
        return {}
    value = _query_json(
        connection,
        """
        SELECT jsonb_build_object(
            'operators', coalesce(
                jsonb_object_agg(
                    o.oid::text,
                    jsonb_build_object(
                        'oid', o.oid::bigint,
                        'identity', n.nspname || '.' || o.oprname || '(' ||
                            CASE WHEN o.oprleft = 0 THEN '-' ELSE format_type(o.oprleft, NULL) END || ',' ||
                            CASE WHEN o.oprright = 0 THEN '-' ELSE format_type(o.oprright, NULL) END || ')',
                        'owner', pg_get_userbyid(o.oprowner),
                        'implementation_oid', o.oprcode::oid::bigint,
                        'restriction_oid', CASE WHEN o.oprrest = 0 THEN NULL ELSE o.oprrest::oid::bigint END,
                        'join_oid', CASE WHEN o.oprjoin = 0 THEN NULL ELSE o.oprjoin::oid::bigint END
                    )
                    ORDER BY o.oid
                ),
                '{}'::jsonb
            )
        )
        FROM pg_operator o
        JOIN pg_namespace n ON n.oid = o.oprnamespace
        WHERE o.oid = ANY(%s)
        """,
        (sorted(set(operator_oids)),),
    )
    raw = value.get("operators") if isinstance(value, Mapping) else None
    if not isinstance(raw, Mapping) or set(raw) != {str(oid) for oid in set(operator_oids)}:
        raise BackfillSafetyError("production aggregate operator dependency query returned malformed JSON")
    operators: dict[int, dict[str, object]] = {}
    for key, item in raw.items():
        if not isinstance(item, Mapping):
            raise BackfillSafetyError("production aggregate operator dependency query returned malformed JSON")
        operators[int(key)] = dict(item)
    return operators


def _collect_non_sec_aggregate_inventory(connection: object) -> list[object]:
    """Collect callable non-SEC aggregates without pg_get_functiondef()."""
    value = _query_json(
        connection,
        """
        WITH aggregate_metadata AS MATERIALIZED (
            SELECT
                p.oid,
                p.proowner,
                p.proacl,
                n.nspname || '.' || p.proname || '(' ||
                    replace(oidvectortypes(p.proargtypes), ', ', ',') || ')' AS identity,
                n.nspname AS schema,
                pg_get_userbyid(p.proowner) AS owner,
                (
                    SELECT e.extname
                    FROM pg_depend d
                    JOIN pg_extension e ON e.oid = d.refobjid
                    WHERE d.classid = 'pg_proc'::regclass
                      AND d.objid = p.oid
                      AND d.deptype = 'e'
                    LIMIT 1
                ) AS extension,
                jsonb_build_object(
                    'kind', ag.aggkind::text,
                    'num_direct_args', ag.aggnumdirectargs,
                    '_support_function_oids', jsonb_build_object(
                        'transition', ag.aggtransfn::oid::bigint,
                        'final', CASE WHEN ag.aggfinalfn = 0 THEN NULL ELSE ag.aggfinalfn::oid::bigint END,
                        'combine', CASE WHEN ag.aggcombinefn = 0 THEN NULL ELSE ag.aggcombinefn::oid::bigint END,
                        'serial', CASE WHEN ag.aggserialfn = 0 THEN NULL ELSE ag.aggserialfn::oid::bigint END,
                        'deserial', CASE WHEN ag.aggdeserialfn = 0 THEN NULL ELSE ag.aggdeserialfn::oid::bigint END,
                        'moving_transition', CASE WHEN ag.aggmtransfn = 0 THEN NULL ELSE ag.aggmtransfn::oid::bigint END,
                        'moving_inverse', CASE WHEN ag.aggminvtransfn = 0 THEN NULL ELSE ag.aggminvtransfn::oid::bigint END,
                        'moving_final', CASE WHEN ag.aggmfinalfn = 0 THEN NULL ELSE ag.aggmfinalfn::oid::bigint END
                    ),
                    '_sort_operator_oid', CASE WHEN ag.aggsortop = 0 THEN NULL ELSE ag.aggsortop::bigint END,
                    'transition_type', format_type(ag.aggtranstype, NULL),
                    'moving_transition_type', CASE WHEN ag.aggmtranstype = 0 THEN '-' ELSE format_type(ag.aggmtranstype, NULL) END,
                    'transition_space', ag.aggtransspace,
                    'moving_transition_space', ag.aggmtransspace,
                    'initial_value', ag.agginitval,
                    'moving_initial_value', ag.aggminitval,
                    'final_extra', ag.aggfinalextra,
                    'moving_final_extra', ag.aggmfinalextra,
                    'final_modify', ag.aggfinalmodify::text,
                    'moving_final_modify', ag.aggmfinalmodify::text,
                    'parallel', p.proparallel::text
                ) AS aggregate_definition
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_aggregate ag ON ag.aggfnoid = p.oid
            WHERE p.prokind = 'a'
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname || '.' || p.proname || '(' ||
                  replace(oidvectortypes(p.proargtypes), ', ', ',') || ')' <> ALL(%s)
        ), aggregate_acl AS MATERIALIZED (
            SELECT
                m.*,
                CASE
                    WHEN a.grantee = 0 THEN 'PUBLIC'
                    WHEN a.grantee = (SELECT oid FROM pg_roles WHERE rolname = current_user) THEN 'DIRECT'
                    ELSE 'INHERITED'
                END AS acl_source,
                coalesce(pg_get_userbyid(NULLIF(a.grantee, 0)), 'PUBLIC') AS grantee
            FROM aggregate_metadata m
            CROSS JOIN LATERAL aclexplode(coalesce(m.proacl, acldefault('f', m.proowner))) a
            WHERE a.privilege_type = 'EXECUTE'
              AND (a.grantee = 0 OR pg_has_role(current_user, a.grantee, 'USAGE'))
        )
        SELECT jsonb_build_object(
            'routines', coalesce(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'identity', identity,
                            'schema', schema,
                            'owner', owner,
                            'extension', extension,
                            'acl_source', acl_source,
                            'grantee', grantee,
                            'privileges', jsonb_build_array('EXECUTE'),
                            'prokind', 'a',
                            'security_definer', false,
                            'security_definer_classification', NULL,
                            'proconfig', '[]'::jsonb,
                            'search_path', NULL,
                            'side_effect_keywords', '[]'::jsonb,
                            'aggregate_definition', aggregate_definition
                        )
                        ORDER BY identity, acl_source, grantee
                    )
                    FROM aggregate_acl
                ),
                '[]'::jsonb
            )
        )
        """,
        (sorted(EXACT_SECURITY_DEFINER_ROUTINES),),
    )
    routines = value.get("routines") if isinstance(value, Mapping) else None
    if not isinstance(routines, list):
        raise BackfillSafetyError("production non-SEC aggregate inventory query returned malformed JSON")
    support_oids: set[int] = set()
    operator_oids: set[int] = set()
    for routine in routines:
        aggregate_definition = routine.get("aggregate_definition") if isinstance(routine, Mapping) else None
        raw_support = aggregate_definition.get("_support_function_oids") if isinstance(aggregate_definition, Mapping) else None
        raw_operator = aggregate_definition.get("_sort_operator_oid") if isinstance(aggregate_definition, Mapping) else None
        if not isinstance(raw_support, Mapping) or set(raw_support) != set(_AGGREGATE_SUPPORT_FUNCTIONS):
            raise BackfillSafetyError("production non-SEC aggregate inventory query returned malformed JSON")
        if any(value is not None and (type(value) is not int or value <= 0) for value in raw_support.values()):
            raise BackfillSafetyError("production non-SEC aggregate inventory query returned malformed JSON")
        support_oids.update(cast(int, oid) for oid in raw_support.values() if oid is not None)
        if raw_operator is not None:
            if type(raw_operator) is not int or raw_operator <= 0:
                raise BackfillSafetyError("production non-SEC aggregate inventory query returned malformed JSON")
            operator_oids.add(raw_operator)

    raw_operators = _collect_aggregate_operators(connection, sorted(operator_oids))
    for operator in raw_operators.values():
        for field in ("implementation_oid", "restriction_oid", "join_oid"):
            oid = operator.get(field)
            if oid is not None:
                if type(oid) is not int or oid <= 0:
                    raise BackfillSafetyError("production aggregate operator dependency query returned malformed JSON")
                support_oids.add(oid)
    function_dependencies = _collect_aggregate_function_dependencies(connection, sorted(support_oids))

    operators: dict[int, dict[str, object]] = {}
    for oid, raw_operator in raw_operators.items():
        operator = {
            "oid": raw_operator.get("oid"),
            "identity": raw_operator.get("identity"),
            "owner": raw_operator.get("owner"),
            "implementation": function_dependencies.get(cast(int, raw_operator.get("implementation_oid"))),
            "restriction": function_dependencies.get(cast(int, raw_operator["restriction_oid"])) if raw_operator.get("restriction_oid") is not None else None,
            "join": function_dependencies.get(cast(int, raw_operator["join_oid"])) if raw_operator.get("join_oid") is not None else None,
        }
        operator["definition_sha256"] = _sha256_bytes(_canonical_json(operator).encode("ascii"))
        operators[oid] = operator

    normalized: list[object] = []
    for raw_routine in routines:
        routine = dict(cast(Mapping[str, object], raw_routine))
        aggregate_definition = dict(cast(Mapping[str, object], routine["aggregate_definition"]))
        raw_support = cast(Mapping[str, object], aggregate_definition.pop("_support_function_oids"))
        raw_operator = aggregate_definition.pop("_sort_operator_oid")
        aggregate_definition["support_functions"] = {
            name: function_dependencies.get(cast(int, raw_support[name])) if raw_support[name] is not None else None
            for name in _AGGREGATE_SUPPORT_FUNCTIONS
        }
        aggregate_definition["sort_operator"] = operators.get(cast(int, raw_operator)) if raw_operator is not None else None
        routine["aggregate_definition"] = aggregate_definition
        normalized.append(routine)
    return normalized


def _collect_non_sec_privilege_inventory(connection: object) -> dict[str, object]:
    """Collect non-SEC effective read capability for a separately signed baseline.

    This is evidence, not an approval path: validation rejects direct grants and
    incomplete SECURITY DEFINER metadata, while the authorization comparison pins
    every remaining inherited/PUBLIC surface exactly.
    """
    value = _query_json(
        connection,
        "WITH relation_acl AS MATERIALIZED (SELECT n.nspname||'.'||c.relname AS identity,n.nspname AS schema,pg_get_userbyid(c.relowner) AS owner,(SELECT e.extname FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e' LIMIT 1) AS extension,CASE WHEN a.grantee=0 THEN 'PUBLIC' WHEN a.grantee=(SELECT oid FROM pg_roles WHERE rolname=current_user) THEN 'DIRECT' ELSE 'INHERITED' END AS acl_source,coalesce(pg_get_userbyid(NULLIF(a.grantee,0)),'PUBLIC') AS grantee,a.privilege_type, n.nspname='pg_catalog' AND a.grantee<>0 AND pg_has_role('pg_monitor',a.grantee,'USAGE') AS pg_monitor_surface FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE c.relkind IN ('r','p','v','m','f') AND a.privilege_type='SELECT' AND (a.grantee=0 OR pg_has_role(current_user,a.grantee,'USAGE')) AND (n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND n.nspname||'.'||c.relname<>ALL(%s) OR n.nspname='pg_catalog' AND a.grantee<>0 AND pg_has_role('pg_monitor',a.grantee,'USAGE'))), sequence_acl AS MATERIALIZED (SELECT n.nspname||'.'||c.relname AS identity,n.nspname AS schema,pg_get_userbyid(c.relowner) AS owner,(SELECT e.extname FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e' LIMIT 1) AS extension,CASE WHEN a.grantee=0 THEN 'PUBLIC' WHEN a.grantee=(SELECT oid FROM pg_roles WHERE rolname=current_user) THEN 'DIRECT' ELSE 'INHERITED' END AS acl_source,coalesce(pg_get_userbyid(NULLIF(a.grantee,0)),'PUBLIC') AS grantee,a.privilege_type FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE c.relkind='S' AND a.privilege_type IN ('SELECT','USAGE','UPDATE') AND (a.grantee=0 OR pg_has_role(current_user,a.grantee,'USAGE')) AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND n.nspname||'.'||c.relname<>ALL(%s)), routine_acl AS MATERIALIZED (SELECT n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')' AS identity,n.nspname AS schema,pg_get_userbyid(p.proowner) AS owner,(SELECT e.extname FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e' LIMIT 1) AS extension,CASE WHEN a.grantee=0 THEN 'PUBLIC' WHEN a.grantee=(SELECT oid FROM pg_roles WHERE rolname=current_user) THEN 'DIRECT' ELSE 'INHERITED' END AS acl_source,coalesce(pg_get_userbyid(NULLIF(a.grantee,0)),'PUBLIC') AS grantee,p.prokind,p.prosecdef,coalesce(to_jsonb(p.proconfig),'[]'::jsonb) AS proconfig,(SELECT substring(setting FROM 13) FROM unnest(coalesce(p.proconfig,ARRAY[]::text[])) setting WHERE setting LIKE 'search_path=%%' LIMIT 1) AS search_path,encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') AS definition_sha256,to_jsonb(array_remove(ARRAY[CASE WHEN pg_get_functiondef(p.oid) ~* '\\mINSERT\\M' THEN 'INSERT' END,CASE WHEN pg_get_functiondef(p.oid) ~* '\\mUPDATE\\M' THEN 'UPDATE' END,CASE WHEN pg_get_functiondef(p.oid) ~* '\\mDELETE\\M' THEN 'DELETE' END,CASE WHEN pg_get_functiondef(p.oid) ~* '\\mTRUNCATE\\M' THEN 'TRUNCATE' END,CASE WHEN pg_get_functiondef(p.oid) ~* '\\mCREATE\\M' THEN 'CREATE' END,CASE WHEN pg_get_functiondef(p.oid) ~* '\\mALTER\\M' THEN 'ALTER' END,CASE WHEN pg_get_functiondef(p.oid) ~* '\\mDROP\\M' THEN 'DROP' END,CASE WHEN pg_get_functiondef(p.oid) ~* '\\mCOPY\\M' THEN 'COPY' END,CASE WHEN pg_get_functiondef(p.oid) ~* '\\mCALL\\M' THEN 'CALL' END],NULL)) AS side_effect_keywords FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE p.prokind<>'a' AND (a.grantee=0 OR pg_has_role(current_user,a.grantee,'USAGE')) AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')'<>ALL(%s)), schema_acl AS MATERIALIZED (SELECT n.nspname AS identity,n.nspname AS schema,pg_get_userbyid(n.nspowner) AS owner,(SELECT e.extname FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid WHERE d.classid='pg_namespace'::regclass AND d.objid=n.oid AND d.deptype='e' LIMIT 1) AS extension,CASE WHEN a.grantee=0 THEN 'PUBLIC' WHEN a.grantee=(SELECT oid FROM pg_roles WHERE rolname=current_user) THEN 'DIRECT' ELSE 'INHERITED' END AS acl_source,coalesce(pg_get_userbyid(NULLIF(a.grantee,0)),'PUBLIC') AS grantee,a.privilege_type FROM pg_namespace n CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a WHERE a.privilege_type='USAGE' AND (a.grantee=0 OR pg_has_role(current_user,a.grantee,'USAGE')) AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND n.nspname<>'public') SELECT jsonb_build_object('relations',coalesce((SELECT jsonb_agg(jsonb_build_object('identity',identity,'schema',schema,'owner',owner,'extension',extension,'acl_source',acl_source,'grantee',grantee,'privileges',jsonb_build_array(privilege_type),'pg_monitor_surface',pg_monitor_surface) ORDER BY identity,acl_source,grantee) FROM relation_acl),'[]'::jsonb),'sequences',coalesce((SELECT jsonb_agg(jsonb_build_object('identity',identity,'schema',schema,'owner',owner,'extension',extension,'acl_source',acl_source,'grantee',grantee,'privileges',jsonb_build_array(privilege_type)) ORDER BY identity,acl_source,grantee,privilege_type) FROM sequence_acl),'[]'::jsonb),'routines',coalesce((SELECT jsonb_agg(jsonb_build_object('identity',identity,'schema',schema,'owner',owner,'extension',extension,'acl_source',acl_source,'grantee',grantee,'privileges',jsonb_build_array('EXECUTE'),'prokind',prokind::text,'security_definer',prosecdef,'security_definer_classification',CASE WHEN prosecdef THEN 'PENDING_SIGNED_BASELINE' ELSE NULL END,'proconfig',proconfig,'search_path',search_path,'definition_sha256',definition_sha256,'side_effect_keywords',side_effect_keywords) ORDER BY identity,acl_source,grantee) FROM routine_acl),'[]'::jsonb),'schemas',coalesce((SELECT jsonb_agg(jsonb_build_object('identity',identity,'schema',schema,'owner',owner,'extension',extension,'acl_source',acl_source,'grantee',grantee,'privileges',jsonb_build_array(privilege_type)) ORDER BY identity,acl_source,grantee) FROM schema_acl),'[]'::jsonb),'public_acl',coalesce((SELECT jsonb_agg(jsonb_build_object('object_kind','relation','identity',identity,'schema',schema,'owner',owner,'extension',extension,'privilege',privilege_type) ORDER BY identity,privilege_type) FROM relation_acl WHERE acl_source='PUBLIC'),'[]'::jsonb),'monitoring',CASE WHEN pg_has_role(current_user,'pg_monitor','USAGE') THEN jsonb_build_array(jsonb_build_object('identity','pg_catalog.pg_stat_activity','membership','DIRECT_MEMBER_NO_SET','surface','pg_monitor'),jsonb_build_object('identity','pg_catalog.pg_locks','membership','DIRECT_MEMBER_NO_SET','surface','pg_monitor')) ELSE '[]'::jsonb END)",
        (sorted(EXACT_MONITORED_RELATIONS), sorted(EXACT_IDENTITY_SEQUENCES), sorted(EXACT_SECURITY_DEFINER_ROUTINES)),
    )
    aggregate_routines = _collect_non_sec_aggregate_inventory(connection)
    if not isinstance(value, Mapping) or set(value) != _NON_SEC_INVENTORY_FIELDS:
        raise BackfillSafetyError("production non-SEC privilege inventory query returned malformed JSON")
    inventory: dict[str, list[object]] = {}
    for field, entries in cast(Mapping[str, object], value).items():
        if not isinstance(entries, list):
            continue
        if field == "routines":
            entries = [*entries, *aggregate_routines]
        normalized: list[object] = []
        for entry in entries:
            if field == "routines" and isinstance(entry, Mapping):
                routine = dict(entry)
                routine.setdefault("aggregate_definition", None)
                keywords = routine.get("side_effect_keywords")
                if isinstance(keywords, list) and all(isinstance(keyword, str) for keyword in keywords):
                    routine["side_effect_keywords"] = sorted(set(keywords))
                aggregate_definition = routine.get("aggregate_definition")
                if routine.get("prokind") == "a" and isinstance(aggregate_definition, Mapping):
                    routine["definition_sha256"] = _sha256_bytes(
                        _canonical_json(aggregate_definition).encode("ascii")
                    )
                entry = routine
            normalized.append(entry)
        inventory[field] = sorted(normalized, key=_canonical_json)
    if set(inventory) != _NON_SEC_INVENTORY_FIELDS:
        raise BackfillSafetyError("production non-SEC privilege inventory query returned malformed JSON")
    inventory_hash = _sha256_bytes(_canonical_json(inventory).encode("ascii"))
    _validate_non_sec_privilege_inventory(inventory, inventory_hash)
    return {"non_sec_privilege_inventory": inventory, "non_sec_privilege_inventory_hash": inventory_hash}


def _collect_non_sec_effective_writes(connection: object) -> dict[str, object]:
    value = _query_json(
        connection,
        "WITH relation_objects AS MATERIALIZED (SELECT c.oid,n.nspname||'.'||c.relname name,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p','v','m','f') AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND n.nspname||'.'||c.relname<>ALL(%s)) SELECT jsonb_build_object('non_sec_effective_write_privileges',coalesce((SELECT jsonb_agg(name||':'||verb ORDER BY name,verb) FROM relation_objects CROSS JOIN LATERAL unnest(array_remove(ARRAY[CASE WHEN has_table_privilege(current_user,oid,'INSERT') THEN 'INSERT' END,CASE WHEN has_table_privilege(current_user,oid,'UPDATE') THEN 'UPDATE' END,CASE WHEN has_table_privilege(current_user,oid,'DELETE') THEN 'DELETE' END,CASE WHEN relkind IN ('r','p') AND has_table_privilege(current_user,oid,'TRUNCATE') THEN 'TRUNCATE' END,CASE WHEN has_table_privilege(current_user,oid,'REFERENCES') THEN 'REFERENCES' END,CASE WHEN has_table_privilege(current_user,oid,'TRIGGER') THEN 'TRIGGER' END,CASE WHEN has_table_privilege(current_user,oid,'MAINTAIN') THEN 'MAINTAIN' END],NULL)) verb),'[]'::jsonb))",
        (sorted(EXACT_MONITORED_RELATIONS),),
    )
    writes = value.get("non_sec_effective_write_privileges") if isinstance(value, Mapping) else None
    if not isinstance(writes, list) or writes != sorted(set(writes)) or not all(isinstance(item, str) and item for item in writes):
        raise BackfillSafetyError("production non-SEC effective write query returned malformed JSON")
    return {"non_sec_effective_write_privileges": writes}


def _assert_no_non_sec_direct_privileges(connection: object) -> None:
    value = _query_json(
        connection,
        "SELECT jsonb_build_object('direct_privileges',coalesce((SELECT jsonb_agg(identity ORDER BY identity) FROM (SELECT 'relation:'||n.nspname||'.'||c.relname||':'||a.privilege_type identity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) a WHERE c.relkind IN ('r','p','v','m','f') AND a.grantee=(SELECT oid FROM pg_roles WHERE rolname=current_user) AND n.nspname||'.'||c.relname<>ALL(%s) UNION ALL SELECT 'sequence:'||n.nspname||'.'||c.relname||':'||a.privilege_type FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) a WHERE c.relkind='S' AND a.grantee=(SELECT oid FROM pg_roles WHERE rolname=current_user) AND n.nspname||'.'||c.relname<>ALL(%s) UNION ALL SELECT 'routine:'||n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||'):'||a.privilege_type FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace CROSS JOIN LATERAL aclexplode(p.proacl) a WHERE a.grantee=(SELECT oid FROM pg_roles WHERE rolname=current_user) AND n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')'<>ALL(%s) UNION ALL SELECT 'schema:'||n.nspname||':'||a.privilege_type FROM pg_namespace n CROSS JOIN LATERAL aclexplode(n.nspacl) a WHERE n.nspname<>'public' AND a.grantee=(SELECT oid FROM pg_roles WHERE rolname=current_user)) q),'[]'::jsonb))",
        (sorted(EXACT_MONITORED_RELATIONS), sorted(EXACT_DIRECT_USAGE_SEQUENCES), sorted(EXACT_DIRECT_EXECUTE_ROUTINES)),
    )
    direct = value.get("direct_privileges") if isinstance(value, Mapping) else None
    if not isinstance(direct, list) or direct != sorted(set(direct)) or not all(isinstance(item, str) and item for item in direct):
        raise BackfillSafetyError("production non-SEC direct privilege query returned malformed JSON")
    if direct:
        raise BackfillSafetyError("non-SEC direct privilege is unsafe")


def _collect_production_preflight(connection: object, authorization: Mapping[str, object]) -> dict[str, object]:
    """Collect the immutable production contract using SELECT-only SQL."""
    target = cast(Mapping[str, object], authorization["target"])
    dsn_parameters = _connection_parameters(connection)
    if dsn_parameters.get("sslmode") != "verify-full" or dsn_parameters.get("host") != target["host"]:
        raise BackfillSafetyError("production preflight cannot prove verify-full host binding")
    identity = _query_json(connection, "SELECT jsonb_build_object('cluster_identity',current_database()||':'||inet_server_addr()::text||':'||version(),'tls_identity',(SELECT coalesce(ssl,false)::text||':'||coalesce(version,'')||':'||coalesce(cipher,'') FROM pg_stat_ssl WHERE pid=pg_backend_pid()),'role_identity',current_user)")
    roles = _query_json(connection, "SELECT jsonb_build_object('memberships',coalesce((SELECT jsonb_agg(parent.rolname ORDER BY parent.rolname) FROM pg_auth_members m JOIN pg_roles parent ON parent.oid=m.roleid JOIN pg_roles child ON child.oid=m.member WHERE child.rolname=current_user),'[]'::jsonb),'capabilities',jsonb_build_object('is_superuser',(SELECT rolsuper FROM pg_roles WHERE rolname=current_user),'owns_any_table',EXISTS(SELECT 1 FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner WHERE r.rolname=current_user AND c.relkind IN ('r','p','v','m','f','S')),'can_create_role',(SELECT rolcreaterole FROM pg_roles WHERE rolname=current_user),'can_create_database',(SELECT rolcreatedb FROM pg_roles WHERE rolname=current_user),'bypass_rls',(SELECT rolbypassrls FROM pg_roles WHERE rolname=current_user),'schema_create',EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND has_schema_privilege(current_user,n.oid,'CREATE')),'set_role',EXISTS(SELECT 1 FROM pg_roles r WHERE r.rolname<>current_user AND pg_has_role(current_user,r.oid,'SET'))))")
    objects = _collect_nonrelation_object_identities(connection)
    relation_security = _collect_relation_security(connection)
    if not isinstance(objects, Mapping) or not isinstance(relation_security, Mapping) or not isinstance(relation_security.get("relations"), list):
        raise BackfillSafetyError("production preflight relation security query returned malformed JSON")
    objects = {**dict(objects), "relations": relation_security["relations"]}
    if any(not isinstance(values, list) or not all(isinstance(item, str) for item in values) for values in objects.values()):
        raise BackfillSafetyError("production preflight object identity query returned malformed JSON")
    objects = {field: sorted(cast(list[str], values)) for field, values in objects.items()}

    privileges = _query_json(
        connection,
        "WITH table_objects AS MATERIALIZED (SELECT c.oid,n.nspname||'.'||c.relname AS name,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p','v','m','f') AND n.nspname='public' AND n.nspname||'.'||c.relname=ANY(%s)), sequence_objects AS MATERIALIZED (SELECT c.oid,n.nspname||'.'||c.relname AS name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='S' AND n.nspname='public' AND n.nspname||'.'||c.relname=ANY(%s)), routine_objects AS MATERIALIZED (SELECT p.oid,n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')' AS name FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')'=ANY(%s)) SELECT jsonb_build_object('table_privileges',coalesce((SELECT jsonb_object_agg(name,verbs ORDER BY name) FROM (SELECT name,to_jsonb(array_remove(ARRAY[CASE WHEN has_table_privilege(current_user,oid,'SELECT') THEN 'SELECT' END,CASE WHEN has_table_privilege(current_user,oid,'INSERT') THEN 'INSERT' END,CASE WHEN has_table_privilege(current_user,oid,'UPDATE') THEN 'UPDATE' END,CASE WHEN has_table_privilege(current_user,oid,'DELETE') THEN 'DELETE' END,CASE WHEN relkind IN ('r','p') AND has_table_privilege(current_user,oid,'TRUNCATE') THEN 'TRUNCATE' END,CASE WHEN has_table_privilege(current_user,oid,'REFERENCES') THEN 'REFERENCES' END,CASE WHEN has_table_privilege(current_user,oid,'TRIGGER') THEN 'TRIGGER' END,CASE WHEN has_table_privilege(current_user,oid,'MAINTAIN') THEN 'MAINTAIN' END],NULL)) verbs FROM table_objects) q),'{}'::jsonb),'sequence_privileges',coalesce((SELECT jsonb_object_agg(name,verbs ORDER BY name) FROM (SELECT name,to_jsonb(array_remove(ARRAY[CASE WHEN has_sequence_privilege(current_user,oid,'SELECT') THEN 'SELECT' END,CASE WHEN has_sequence_privilege(current_user,oid,'UPDATE') THEN 'UPDATE' END,CASE WHEN has_sequence_privilege(current_user,oid,'USAGE') THEN 'USAGE' END],NULL)) verbs FROM sequence_objects) q),'{}'::jsonb),'function_privileges',coalesce((SELECT jsonb_object_agg(name,jsonb_build_array('EXECUTE') ORDER BY name) FROM routine_objects WHERE has_function_privilege(current_user,oid,'EXECUTE')),'{}'::jsonb))",
        (sorted(EXACT_MONITORED_RELATIONS), sorted(EXACT_DIRECT_USAGE_SEQUENCES), sorted(EXACT_DIRECT_EXECUTE_ROUTINES)),
    )
    privilege_boundaries = _query_json(connection, "SELECT jsonb_build_object('column_privileges',coalesce((SELECT jsonb_agg(identity ORDER BY identity) FROM (SELECT n.nspname||'.'||c.relname||'.'||a.attname||':'||acl.privilege_type||':grantee='||coalesce(pg_get_userbyid(NULLIF(acl.grantee,0)),'PUBLIC') identity FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(a.attacl) acl WHERE a.attnum>0 AND NOT a.attisdropped AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND acl.privilege_type IN ('SELECT','INSERT','UPDATE','REFERENCES') AND CASE WHEN acl.grantee=0 THEN true ELSE pg_has_role(current_user,acl.grantee,'USAGE') END) q),'[]'::jsonb),'database_privileges',jsonb_build_object('CONNECT',has_database_privilege(current_user,current_database(),'CONNECT'),'CREATE',has_database_privilege(current_user,current_database(),'CREATE'),'TEMPORARY',has_database_privilege(current_user,current_database(),'TEMPORARY')))")
    write_surface = _query_json(connection, "SELECT jsonb_build_object('effective_writable_tables',coalesce((SELECT jsonb_agg(name ORDER BY name) FROM (SELECT n.nspname||'.'||c.relname name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p','v','m','f') AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND (has_table_privilege(current_user,c.oid,'INSERT') OR has_table_privilege(current_user,c.oid,'UPDATE') OR has_table_privilege(current_user,c.oid,'DELETE') OR (c.relkind IN ('r','p') AND has_table_privilege(current_user,c.oid,'TRUNCATE')))) q),'[]'::jsonb),'truncate_tables',coalesce((SELECT jsonb_agg(name ORDER BY name) FROM (SELECT n.nspname||'.'||c.relname name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p') AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND has_table_privilege(current_user,c.oid,'TRUNCATE')) q),'[]'::jsonb))")
    non_sec_writes = _collect_non_sec_effective_writes(connection)
    _assert_no_non_sec_direct_privileges(connection)
    monitoring = _collect_monitoring_privileges(connection)
    non_sec_inventory = _collect_non_sec_privilege_inventory(connection)
    safety = _query_json(connection, "SELECT jsonb_build_object('public_acl','[]'::jsonb,'unsafe_security_definers','[]'::jsonb,'trigger_write_targets',coalesce((SELECT jsonb_agg(n.nspname||'.'||c.relname||':'||t.tgname ORDER BY n.nspname,c.relname,t.tgname) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid WHERE NOT t.tgisinternal AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.refclassid='pg_extension'::regclass AND d.deptype='e') AND NOT (n.nspname='public' AND c.relname='rr1_contract_tables' AND t.tgname='rr1_contract_tables_immutable') AND pg_get_functiondef(p.oid) ~* '\\m(insert|update|delete|truncate)\\M'),'[]'::jsonb))")
    sec_safety = _query_json(connection, "SELECT jsonb_build_object('public_acl',coalesce((SELECT jsonb_agg(kind||':'||identity||':'||privilege ORDER BY kind,identity,privilege) FROM (SELECT 'function' kind,n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')' identity,a.privilege_type privilege FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0 AND n.nspname='public' AND n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')'=ANY(%s) UNION ALL SELECT 'relation',n.nspname||'.'||c.relname,a.privilege_type FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) a WHERE a.grantee=0 AND n.nspname='public' AND n.nspname||'.'||c.relname=ANY(%s)) q),'[]'::jsonb),'unsafe_security_definers',coalesce((SELECT jsonb_agg(n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')' ORDER BY n.nspname,p.proname) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.prosecdef AND n.nspname='public' AND n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')'=ANY(%s) AND (NOT coalesce(p.proconfig,ARRAY[]::text[]) @> ARRAY['search_path=pg_catalog, public'] OR EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE'))),'[]'::jsonb))", (sorted(EXACT_SECURITY_DEFINER_ROUTINES), sorted(EXACT_MONITORED_RELATIONS), sorted(EXACT_SECURITY_DEFINER_ROUTINES)))
    if not isinstance(safety, Mapping) or not isinstance(sec_safety, Mapping):
        raise BackfillSafetyError("production preflight safety query returned malformed JSON")
    safety = {**dict(safety), **dict(sec_safety)}
    if not all(isinstance(value, Mapping) for value in (identity, roles, privileges, privilege_boundaries, write_surface, non_sec_writes, monitoring, non_sec_inventory, safety)):
        raise BackfillSafetyError("production preflight collector returned malformed JSON")
    role_data = cast(Mapping[str, object], roles)
    membership_value = role_data.get("memberships")
    capability_value = role_data.get("capabilities")
    if not isinstance(membership_value, list) or not isinstance(capability_value, Mapping):
        raise BackfillSafetyError("production preflight role query returned malformed JSON")
    capabilities = dict(cast(Mapping[str, object], capability_value))
    capabilities["no_memberships"] = not membership_value
    result = {
        "cluster_identity": cast(Mapping[str, object], identity)["cluster_identity"],
        "tls_identity": cast(Mapping[str, object], identity)["tls_identity"],
        "role_identity": cast(Mapping[str, object], identity)["role_identity"],
        "fixed_memberships": sorted(cast(list[str], membership_value)),
        "role_capabilities": capabilities,
        "object_catalog_hash": _sha256_bytes(_canonical_json(objects).encode("ascii")),
        "object_identities": objects,
        **dict(cast(Mapping[str, object], privileges)),
        **dict(cast(Mapping[str, object], privilege_boundaries)),
        **dict(cast(Mapping[str, object], write_surface)),
        **dict(cast(Mapping[str, object], non_sec_writes)),
        **dict(cast(Mapping[str, object], monitoring)),
        **dict(cast(Mapping[str, object], non_sec_inventory)),
        **dict(cast(Mapping[str, object], safety)),
    }
    return _validate_preflight_attestation(result)


def _default_schema_installers(form: str) -> dict[str, Callable[[object], None]]:
    manifests = importlib.import_module("src.sec_regulatory.manifests")
    storage = importlib.import_module(f"src.{form}.storage")
    return {"manifest": manifests.install_schema, form: storage.install_schema}


def _default_dispatcher(form: str) -> Callable[..., Mapping[str, object]]:
    module = importlib.import_module(f"src.{form}.ingestion")
    return cast(Callable[..., Mapping[str, object]], module.ingest_package)


def _open_authorized_connection(dsn: str, *, production: bool) -> object:
    psycopg = importlib.import_module("psycopg")
    if production:
        return psycopg.connect(dsn, sslmode="verify-full")
    return psycopg.connect(dsn)


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


def _governed_package_id(connection: object, *, run_id: UUID, form: str, package_sha256: str) -> UUID:
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute(
            "SELECT package_id FROM sec_source_packages WHERE run_id=%s AND source_family=%s AND package_sha256=%s ORDER BY package_id",
            (run_id, form, package_sha256),
        )
        rows = cursor.fetchall()
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], tuple) or len(rows[0]) != 1 or not isinstance(rows[0][0], UUID):
        raise BackfillSafetyError("protected commit cannot establish a unique governed package UUID")
    return rows[0][0]


def _governed_reconciliation_sha256(connection: object, *, run_id: UUID) -> str:
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute(
            "SELECT source_file_id::text, table_name, expected_count, source_count, lexical_count, typed_success_count, quarantine_count, reject_count, state "
            "FROM sec_table_reconciliations WHERE run_id=%s ORDER BY source_file_id, table_name",
            (run_id,),
        )
        rows = cursor.fetchall()
    if not isinstance(rows, list) or not rows or any(not isinstance(row, tuple) or len(row) != 9 for row in rows):
        raise BackfillSafetyError("protected commit cannot establish reconciliation evidence")
    return _sha256_bytes(_canonical_json([list(row) for row in rows]).encode("ascii"))


def _get_recovery_governed_evidence(
    connection: object,
    *,
    package_id: UUID,
    run_id: UUID,
    authorization_fingerprint: str,
) -> _RecoveryGovernedEvidence | None:
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute(
            "SELECT p.package_id, r.run_id, r.package_sha256, t.supervisor_run_id, t.authorization_fingerprint, t.commit_outcome "
            "FROM sec_source_packages p JOIN sec_ingestion_runs r ON r.run_id=p.run_id "
            "JOIN LATERAL (SELECT supervisor_run_id, authorization_fingerprint, commit_outcome FROM sec_run_transitions "
            "WHERE run_id=r.run_id AND event_type='commit_outcome' AND authorization_fingerprint=%s ORDER BY transition_id LIMIT 1) t ON TRUE "
            "WHERE p.package_id=%s AND r.run_id=%s",
            (authorization_fingerprint, package_id, run_id),
        )
        rows = cursor.fetchall()
    if not isinstance(rows, list) or len(rows) > 1 or any(not isinstance(row, tuple) or len(row) != 6 for row in rows):
        raise BackfillSafetyError("recovery governed evidence is not unique")
    return _RecoveryGovernedEvidence(*rows[0]) if rows else None


def _get_recovery_zero_proof(connection: object, *, package_id: UUID, run_id: UUID) -> dict[str, int]:
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute(
            "SELECT "
            "(SELECT count(*) FROM sec_source_packages WHERE package_id=%s OR run_id=%s),"
            "(SELECT count(*) FROM sec_ingestion_runs WHERE run_id=%s),"
            "(SELECT count(*) FROM sec_run_transitions WHERE run_id=%s),"
            "(SELECT count(*) FROM sec_source_package_transitions WHERE package_id=%s OR ingestion_run_id=%s),"
            "(SELECT count(*) FROM sec_validated_raw_visibility WHERE run_id=%s)",
            (package_id, run_id, run_id, run_id, package_id, run_id, run_id),
        )
        row = cursor.fetchone()
    if not isinstance(row, tuple) or len(row) != 5 or any(not isinstance(value, int) for value in row):
        raise BackfillSafetyError("recovery zero-delta query returned uncertain evidence")
    return dict(zip(("package_count", "run_count", "run_transition_count", "source_transition_count", "validated_visibility_count"), row, strict=True))


def _snapshot_exact_write_counts(connection: object) -> dict[str, int]:
    counts: dict[str, int] = {}
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        for table in sorted(EXACT_WRITABLE_TABLES):
            cursor.execute(f"SELECT count(*) FROM {table}")
            row = cursor.fetchone()
            if not isinstance(row, tuple) or len(row) != 1 or not isinstance(row[0], int):
                raise BackfillSafetyError("rollback probe table count returned uncertain evidence")
            counts[table] = row[0]
    if set(counts) != EXACT_WRITABLE_TABLES:
        raise BackfillSafetyError("rollback probe did not count the exact 16-table write surface")
    return counts


class _ProtectedTransactionConnection:
    """Delegate a psycopg connection while suppressing ingester checkpoint commits."""

    def __init__(self, connection: object) -> None:
        self._connection = connection

    def commit(self) -> None:
        """Leave the sole durable COMMIT to AuthorizedPackageExecutor."""

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)


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
        preflight_inspector: Callable[[object], Mapping[str, object]] | None = None,
    ) -> None:
        self.authorization = dict(authorization)
        self.inventory = inventory
        self.connection_factory = connection_factory
        self.target_inspector = target_inspector or _inspect_connected_target
        self.schema_installers = schema_installers
        self.dispatchers = dispatchers
        self.preflight_inspector = preflight_inspector or (lambda connection: _collect_production_preflight(connection, self.authorization))
        target = cast(Mapping[str, object], authorization["target"])
        self.target_identity = {key: target[key] for key in ("project", "vm", "zone", "database", "server_address", "role")}
        self.authorization_id = cast(str, authorization["authorization_id"])
        self.authorization_fingerprint = cast(str, authorization["authorization_fingerprint"])
        self.stop_contract_hash = cast(str, authorization["stop_contract_hash"])
        self.supervisor_run_id = cast(str | None, authorization.get("supervisor_run_id"))
        self.authorization_lineage = {key: value for key, value in authorization.items() if key != "authorization_fingerprint"}

    def _validate_production_preflight(self, connection: object) -> None:
        if self.authorization["target_mode"] != "production_authorized":
            return
        expected = _validate_preflight_attestation(self.authorization["preflight_attestation"])
        actual = _validate_preflight_attestation(self.preflight_inspector(connection))
        unsafe = ("public_acl", "unsafe_security_definers")
        if any(expected[field] != [] for field in unsafe):
            raise BackfillSafetyError("production preflight authorization contains unsafe privileges")
        if expected["role_identity"] != cast(Mapping[str, object], self.authorization["target"])["role"]:
            raise BackfillSafetyError("production preflight role identity mismatch")
        if actual != expected:
            raise BackfillSafetyError("production preflight attestation drift")

    def preflight(self) -> None:
        """Run the no-DML production gate before supervisor launch."""
        if self.authorization["target_mode"] != "production_authorized":
            return
        dsn = os.environ.get(cast(str, self.authorization["dsn_env_var"]))
        if not dsn:
            raise BackfillSafetyError("authorized executor DSN environment variable is unavailable")
        factory = self.connection_factory
        connection = factory(dsn) if factory is not None else _open_authorized_connection(dsn, production=True)
        try:
            self._validate_connected_target(self.target_inspector(connection))
            self._validate_production_preflight(connection)
        finally:
            close = getattr(connection, "close", None)
            if callable(close):
                close()

    def promote_canary_certificate(self) -> dict[str, dict[str, object]]:
        """Perform the full-run three-package promotion in one transaction."""
        certificate = self.authorization.get("canary_certificate")
        if self.authorization.get("execution_mode") != "full" or not isinstance(certificate, Mapping):
            raise BackfillSafetyError("certified promotion is available only to a full authorization")
        dsn = os.environ.get(cast(str, self.authorization["dsn_env_var"]))
        if not dsn:
            raise BackfillSafetyError("authorized executor DSN environment variable is unavailable")
        factory = self.connection_factory
        connection = factory(dsn) if factory is not None else _open_authorized_connection(dsn, production=self.authorization["target_mode"] == "production_authorized")
        try:
            self._validate_connected_target(self.target_inspector(connection))
            self._validate_production_preflight(connection)
            return promote_certified_canary_packages(connection, certificate=certificate, inventory=self.inventory)
        finally:
            close = getattr(connection, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass

    def recover_ambiguous(
        self,
        *,
        status_path: Path,
        recovery_authorization: Mapping[str, object],
        recovery_evidence: Mapping[str, object],
    ) -> dict[str, object]:
        """Use a fresh governed connection to adjudicate one ambiguous fence."""
        dsn = os.environ.get(cast(str, self.authorization["dsn_env_var"]))
        if not dsn:
            raise BackfillSafetyError("authorized recovery DSN environment variable is unavailable")
        factory = self.connection_factory
        read_connection = factory(dsn) if factory is not None else _open_authorized_connection(dsn, production=self.authorization["target_mode"] == "production_authorized")
        try:
            self._validate_connected_target(self.target_inspector(read_connection))
            self._validate_production_preflight(read_connection)
            with read_connection.cursor() as cursor:  # type: ignore[attr-defined]
                cursor.execute("SET TRANSACTION READ ONLY")
            governed = _get_recovery_governed_evidence(
                read_connection,
                package_id=UUID(cast(str, recovery_authorization["package_id"])),
                run_id=UUID(cast(str, recovery_authorization["run_id"])),
                authorization_fingerprint=cast(str, recovery_authorization["original_authorization_fingerprint"]),
            )
        finally:
            rollback = getattr(read_connection, "rollback", None)
            if callable(rollback):
                rollback()
            close = getattr(read_connection, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
        resolution_connection = factory(dsn) if factory is not None else _open_authorized_connection(dsn, production=self.authorization["target_mode"] == "production_authorized")
        try:
            self._validate_connected_target(self.target_inspector(resolution_connection))
            self._validate_production_preflight(resolution_connection)
            return recover_ambiguous_commit(resolution_connection, status_path=status_path, recovery_authorization=recovery_authorization, recovery_evidence=recovery_evidence, governed_evidence=governed)
        finally:
            close = getattr(resolution_connection, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass

    def run_rollback_probe(self, *, evidence_path: Path) -> dict[str, object]:
        """Exercise the deterministic first canary package and prove zero durability."""
        if self.authorization.get("target_mode") != "production_authorized" or self.authorization.get("execution_mode") != "canary" or self.authorization.get("schema_version") != 4:
            raise BackfillSafetyError("rollback probe requires a production canary authorization")
        scope = _validate_scope(self.authorization["package_scope"], inventory_hash=cast(str, self.inventory["inventory_hash"]), label="authorization")
        if len(scope) != 3:
            raise BackfillSafetyError("rollback probe requires the exact three-package canary scope")
        selected_identity = sorted(item["identity"] for item in scope)[0]
        package = next((item for item in cast(list[dict[str, object]], self.inventory["packages"]) if item["identity"] == selected_identity), None)
        if package is None:
            raise BackfillSafetyError("rollback probe package differs from inventory")
        dsn = os.environ.get(cast(str, self.authorization["dsn_env_var"]))
        if not dsn:
            raise BackfillSafetyError("rollback probe DSN environment variable is unavailable")

        def connect_checked() -> object:
            connection = self.connection_factory(dsn) if self.connection_factory is not None else _open_authorized_connection(dsn, production=True)
            self._validate_connected_target(self.target_inspector(connection))
            self._validate_production_preflight(connection)
            return connection

        before_connection = connect_checked()
        try:
            before = _snapshot_exact_write_counts(before_connection)
        finally:
            before_connection.close()  # type: ignore[attr-defined]
        transaction = connect_checked()
        lock_key: str | None = None
        try:
            form = cast(str, package["form"])
            root_by_form = _validate_inventory(self.inventory)
            source_package = root_by_form[form] / Path(cast(str, package["relative_package_path"]))
            _verify_package_unchanged(package, root_by_form)
            lock_key = _derive_form_lock_key(form, source_package)
            if not _try_form_advisory_lock(transaction, lock_key):
                lock_key = None
                raise BackfillSafetyError("lock_busy")
            dispatcher = (self.dispatchers or {}).get(form) if self.dispatchers is not None else _default_dispatcher(form)
            if dispatcher is None:
                raise BackfillSafetyError("rollback probe dispatcher is unavailable")
            result = dict(dispatcher(_ProtectedTransactionConnection(transaction), package=source_package, source_root=root_by_form[form]))
            self._terminal_result(result, cast(str, package["relative_package_path"]))
        finally:
            rollback = getattr(transaction, "rollback", None)
            if callable(rollback):
                rollback()
            if lock_key is not None:
                try:
                    _release_form_advisory_lock(transaction, lock_key)
                except Exception:
                    pass
            transaction.close()  # type: ignore[attr-defined]
        after_connection = connect_checked()
        try:
            after = _snapshot_exact_write_counts(after_connection)
        finally:
            after_connection.close()  # type: ignore[attr-defined]
        deltas = {table: after[table] - before[table] for table in sorted(EXACT_WRITABLE_TABLES)}
        if any(deltas.values()):
            raise BackfillSafetyError("rollback probe detected a durable table delta")
        evidence = {
            "schema_version": 1,
            "state": "ROLLBACK_PROBED",
            "identity": selected_identity,
            "package_sha256": package["package_sha256"],
            "authorization_fingerprint": self.authorization_fingerprint,
            "table_deltas": deltas,
            "source_transition_delta": deltas["public.sec_source_package_transitions"],
            "validated_visibility_delta": deltas["public.sec_validated_raw_visibility"],
        }
        _durable_json_replace(evidence_path, evidence)
        return evidence

    def _validate_connected_target(self, actual: Mapping[str, object]) -> None:
        target = cast(Mapping[str, object], self.authorization["target"])
        required = {"database", "server_address", "role", "postgresql_identity", "timescaledb_identity", "is_superuser", "owns_any_table", "writable_tables"}
        if self.authorization["target_mode"] == "production_authorized":
            required = required | {"truncate_tables"}
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
        allowed = (
            set(EXACT_DIRECT_WRITABLE_TABLES)
            if self.authorization["target_mode"] == "production_authorized"
            else set(cast(list[str], self.authorization["writable_tables"]))
        )
        denied = set(cast(list[str], self.authorization["pointer_table_denylist"]))
        if set(writable) != allowed or set(writable) & denied or any(value.casefold().startswith("sec_current.") or "provider" in value.casefold() or "pointer" in value.casefold() for value in writable):
            raise BackfillSafetyError("connected target writable table set is unsafe")
        if self.authorization["target_mode"] == "production_authorized" and (not isinstance(actual.get("truncate_tables"), list) or actual["truncate_tables"]):
            raise BackfillSafetyError("connected target TRUNCATE privilege is unsafe")

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
            run_id = result.get("run_id")
            if run_id is not None:
                try:
                    UUID(cast(str, run_id))
                except (TypeError, ValueError) as exc:
                    raise BackfillSafetyError("ingester returned an invalid failed run UUID") from exc
            reason = result.get("reason")
            failure_detail = _redact(reason) if isinstance(reason, str) else None
            safe = {"state": state, "reason_code": "ingester_failed", "failure_detail": failure_detail}
            if run_id is not None:
                safe["run_id"] = cast(str, run_id)
        else:
            raise BackfillSafetyError("ingester returned a nonterminal package state")
        if reconciliation_hash is not None:
            safe["reconciliation_hash"] = reconciliation_hash
        return safe

    def __call__(self, package: dict[str, object]) -> Mapping[str, object]:
        return self._execute(package, commit_fence=None)

    def execute_with_fence(
        self,
        package: dict[str, object],
        commit_fence: Callable[[str, Mapping[str, object]], None],
    ) -> Mapping[str, object]:
        """Execute with a supervisor-owned durable fence around the COMMIT call."""
        return self._execute(package, commit_fence=commit_fence)

    def _execute(
        self,
        package: dict[str, object],
        *,
        commit_fence: Callable[[str, Mapping[str, object]], None] | None,
    ) -> Mapping[str, object]:
        root_by_form = _validate_inventory(self.inventory)
        identity = package.get("identity")
        expected = next((candidate for candidate in cast(list[dict[str, object]], self.inventory["packages"]) if candidate["identity"] == identity), None)
        if expected is None or any(package.get(key) != expected.get(key) for key in expected):
            raise BackfillSafetyError("package is not bound to the validated inventory")
        if self.authorization["target_mode"] == "production_authorized":
            scope = {
                item["identity"]: item["package_sha256"]
                for item in _validate_scope(self.authorization["package_scope"], inventory_hash=cast(str, self.authorization["inventory_hash"]), label="authorization")
            }
            if scope.get(cast(str, expected["identity"])) != expected["package_sha256"]:
                raise BackfillSafetyError("package is absent from the production authorization scope")
        form = cast(str, expected["form"])
        if form not in {"nport", "ncen", "rr1"}:
            raise BackfillSafetyError("unsupported historical package form")
        _verify_package_unchanged(expected, root_by_form)
        dsn_env_var = cast(str, self.authorization["dsn_env_var"])
        dsn = os.environ.get(dsn_env_var)
        if not dsn:
            raise BackfillSafetyError("authorized executor DSN environment variable is unavailable")
        factory = self.connection_factory
        connection = factory(dsn) if factory is not None else _open_authorized_connection(dsn, production=self.authorization["target_mode"] == "production_authorized")
        lock_key: str | None = None
        commit_issued = False
        commit_confirmed = False
        try:
            self._validate_connected_target(self.target_inspector(connection))
            self._validate_production_preflight(connection)
            source_package = root_by_form[form] / Path(cast(str, expected["relative_package_path"]))
            lock_key = _derive_form_lock_key(form, source_package)
            if not _try_form_advisory_lock(connection, lock_key):
                lock_key = None
                raise BackfillSafetyError("lock_busy")
            if self.authorization["target_mode"] == "local_disposable":
                installers = self.schema_installers or _default_schema_installers(form)
                if set(installers) != {"manifest", form}:
                    raise BackfillSafetyError("authorized executor schema installer boundary is invalid")
                installers["manifest"](connection)
                installers[form](connection)
            dispatcher = (self.dispatchers or {}).get(form) if self.dispatchers is not None else _default_dispatcher(form)
            if dispatcher is None:
                raise BackfillSafetyError("authorized executor dispatcher is unavailable")
            root = root_by_form[form]
            dispatch_connection = _ProtectedTransactionConnection(connection) if commit_fence is not None else connection
            result = dict(dispatcher(dispatch_connection, package=source_package, source_root=root))
            safe = self._terminal_result(result, cast(str, expected["relative_package_path"]))
            if safe.get("state") == "failed" and commit_fence is not None:
                rollback = getattr(connection, "rollback", None)
                if callable(rollback):
                    rollback()
                return safe
            commit = getattr(connection, "commit", None)
            if not callable(commit):
                raise BackfillSafetyError("authorized executor connection cannot commit")
            if commit_fence is not None:
                if self.supervisor_run_id is None:
                    raise BackfillSafetyError("protected commit requires a supervisor run UUID")
                try:
                    run_id = UUID(cast(str, safe.get("run_id")))
                    supervisor_run_id = UUID(self.supervisor_run_id)
                except (TypeError, ValueError) as exc:
                    raise BackfillSafetyError("protected commit requires typed ingestion and supervisor run UUIDs") from exc
                reconciliation_hash = _governed_reconciliation_sha256(connection, run_id=run_id)
                if safe.get("reconciliation_hash") is not None and safe["reconciliation_hash"] != reconciliation_hash:
                    raise BackfillSafetyError("ingester reconciliation evidence differs from governed rows")
                safe["reconciliation_hash"] = reconciliation_hash
                if lock_key is None or not _is_sha256(lock_key.split(":", 1)[-1]):
                    raise BackfillSafetyError("protected commit requires a governed package hash")
                governed_package_sha256 = lock_key.split(":", 1)[-1]
                package_id = _governed_package_id(connection, run_id=run_id, form=form, package_sha256=governed_package_sha256)
                fence_evidence: dict[str, object] = {
                    "identity": expected["identity"],
                    "inventory_package_sha256": expected["package_sha256"],
                    "package_sha256": governed_package_sha256,
                    "package_id": str(package_id),
                    "run_id": str(run_id),
                    "supervisor_run_id": str(supervisor_run_id),
                    "authorization_fingerprint": self.authorization_fingerprint,
                    "reconciliation_hash": reconciliation_hash,
                    "terminal_result": safe,
                }
                commit_fence("issued", fence_evidence)
                manifests.record_commit_outcome(
                    cast(Any, connection),
                    run_id=run_id,
                    supervisor_run_id=supervisor_run_id,
                    authorization_fingerprint=self.authorization_fingerprint,
                    package_sha256=governed_package_sha256,
                    outcome="committed",
                )
                commit_issued = True
                try:
                    commit()
                except Exception as exc:
                    try:
                        commit_fence("ambiguous", fence_evidence)
                    except Exception:
                        pass
                    raise AmbiguousCommitError("COMMIT outcome is ambiguous; explicit recovery is required") from exc
                commit_confirmed = True
                try:
                    commit_fence("confirmed", fence_evidence)
                except Exception as exc:
                    try:
                        commit_fence("ambiguous", fence_evidence)
                    except Exception:
                        pass
                    raise AmbiguousCommitError("committed transaction could not be durably confirmed") from exc
            else:
                commit()
                commit_confirmed = True
            return safe
        except Exception:
            if not commit_issued:
                rollback = getattr(connection, "rollback", None)
                if callable(rollback):
                    try:
                        rollback()
                    except Exception:
                        pass
            raise
        finally:
            if lock_key is not None:
                try:
                    _release_form_advisory_lock(connection, lock_key)
                except Exception:
                    pass
            close = getattr(connection, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    if not commit_confirmed:
                        pass


def build_authorized_executor(
    authorization_path: Path,
    *,
    inventory: Mapping[str, object],
    code_sha: str,
    connection_factory: Callable[[str], object] | None = None,
    target_inspector: Callable[[object], Mapping[str, object]] | None = None,
    schema_installers: Mapping[str, Callable[[object], None]] | None = None,
    dispatchers: Mapping[str, Callable[..., Mapping[str, object]]] | None = None,
    preflight_inspector: Callable[[object], Mapping[str, object]] | None = None,
    run_directory: Path | None = None,
    command: Sequence[str] | None = None,
) -> AuthorizedPackageExecutor:
    """Bind an inert executor to an exact authorization and inventory artifact."""
    inventory_hash = inventory.get("inventory_hash")
    if not isinstance(inventory_hash, str):
        raise BackfillSafetyError("invalid inventory for authorized executor")
    authorization = load_execution_authorization(authorization_path, code_sha=code_sha, inventory_hash=inventory_hash, run_directory=run_directory, command=command)
    if authorization["target_mode"] == "production_authorized":
        inventory_scope = {item["identity"]: item["package_sha256"] for item in cast(list[dict[str, object]], inventory["packages"])}
        authorized_scope = _validate_scope(authorization["package_scope"], inventory_hash=inventory_hash, label="authorization")
        for item in authorized_scope:
            if inventory_scope.get(item["identity"]) != item["package_sha256"]:
                raise BackfillSafetyError("production authorization package scope differs from inventory")
        if authorization.get("schema_version") == 4 and authorization.get("execution_mode") == "full" and {item["identity"]: item["package_sha256"] for item in authorized_scope} != inventory_scope:
            raise BackfillSafetyError("production full authorization does not bind the exact inventory")
    return AuthorizedPackageExecutor(authorization, inventory, connection_factory, target_inspector, schema_installers, dispatchers, preflight_inspector)


def rollback_probe(
    connection: object,
    *,
    probe: Callable[[object], Mapping[str, object]],
    evidence_writer: Callable[[Mapping[str, object]], None],
) -> dict[str, object]:
    """Exercise a transactional real path and retain only zero-delta evidence."""
    try:
        result = dict(probe(connection))
    finally:
        rollback = getattr(connection, "rollback", None)
        if callable(rollback):
            rollback()
    if result != {"state": "rollback_probed", "table_delta": 0}:
        raise BackfillSafetyError("rollback probe did not prove zero committed table delta")
    evidence = {"state": "ROLLBACK_PROBED", "table_delta": 0}
    evidence_writer(evidence)
    return evidence


def validate_recovery_outcome(
    status: Mapping[str, object],
    *,
    authorization_fingerprint: str,
    outcome: Mapping[str, object] | None,
) -> dict[str, object]:
    """Allow expired-lease recovery only after a positive, lineage-bound commit proof."""
    if _unexpired_lease(status):
        raise BackfillSafetyError("live lease blocks recovery")
    if status.get("authorization_fingerprint") != authorization_fingerprint:
        raise BackfillSafetyError("recovery authorization lineage mismatch")
    if not isinstance(outcome, Mapping) or outcome.get("commit_outcome") != "committed" or outcome.get("authorization_fingerprint") != authorization_fingerprint:
        raise BackfillSafetyError("ambiguous commit outcome blocks recovery")
    terminal = outcome.get("terminal_result")
    if not isinstance(terminal, Mapping) or terminal.get("state") not in SUCCESS_STATES:
        raise BackfillSafetyError("commit outcome is not a positive terminal result")
    return dict(terminal)


def load_recovery_authorization(
    path: Path,
    *,
    code_sha: str,
    inventory_hash: str,
    status_path: Path,
    original_authorization_fingerprint: str,
) -> dict[str, object]:
    """Load a separately issued, exact-lineage recovery authorization."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackfillSafetyError("invalid recovery authorization artifact") from exc
    if not isinstance(document, dict) or set(document) != _RECOVERY_AUTHORIZATION_FIELDS or document.get("schema_version") != 1 or document.get("stage") != "phase4_ambiguous_commit_recovery":
        raise BackfillSafetyError("invalid recovery authorization schema")
    if document.get("code_sha") != code_sha or document.get("inventory_hash") != inventory_hash or document.get("status_path") != str(status_path.resolve()) or document.get("original_authorization_fingerprint") != original_authorization_fingerprint:
        raise BackfillSafetyError("recovery authorization lineage mismatch")
    for field in ("supervisor_run_id", "package_id", "run_id", "recovery_authorization_id"):
        try:
            UUID(cast(str, document.get(field)))
        except (TypeError, ValueError) as exc:
            raise BackfillSafetyError("recovery authorization requires typed UUID lineage") from exc
    if document.get("expected_outcome") not in {"committed", "rolled_back"} or not all(_is_sha256(document.get(field)) for field in ("original_authorization_fingerprint", "package_sha256", "reconciliation_sha256", "recovery_evidence_sha256")):
        raise BackfillSafetyError("invalid recovery authorization evidence")
    secret_version = document.get("secret_version_resource")
    if not isinstance(secret_version, str) or re.fullmatch(r"projects/[^/]+/secrets/[^/]+/versions/[1-9][0-9]*", secret_version) is None:
        raise BackfillSafetyError("recovery authorization requires a numeric secret version")
    if not isinstance(document.get("identity"), str) or not document["identity"]:
        raise BackfillSafetyError("recovery authorization requires a package identity")
    _assert_no_secret({key: value for key, value in document.items() if key != "secret_version_resource"})
    validated = dict(document)
    validated["recovery_authorization_fingerprint"] = authorization_fingerprint(validated)
    if validated["recovery_authorization_fingerprint"] == original_authorization_fingerprint:
        raise BackfillSafetyError("recovery authorization must be distinct from execution authorization")
    return validated


def recover_ambiguous_commit(
    connection: object,
    *,
    status_path: Path,
    recovery_authorization: Mapping[str, object],
    recovery_evidence: Mapping[str, object],
    governed_evidence: object = _UNSET,
) -> dict[str, object]:
    """Adjudicate one expired ambiguous fence from governed database evidence."""
    status = _load_status(status_path)
    if _unexpired_lease(status):
        raise BackfillSafetyError("live lease blocks recovery")
    identity = recovery_authorization.get("identity")
    records = status.get("packages")
    record = records.get(identity) if isinstance(records, Mapping) else None
    exact = {
        "authorization_fingerprint": "original_authorization_fingerprint",
        "supervisor_run_id": "supervisor_run_id",
        "package_id": "package_id",
        "run_id": "run_id",
        "governed_package_sha256": "package_sha256",
        "reconciliation_hash": "reconciliation_sha256",
    }
    if not isinstance(record, Mapping):
        raise BackfillSafetyError("recovery authorization does not match the ambiguous fence")
    recoverable_fence = record.get("state") == "ambiguous_commit" or _requires_commit_recovery(record)
    if not recoverable_fence or status.get("active_package") != identity or status.get("authorization_fingerprint") != recovery_authorization.get("original_authorization_fingerprint") or any(record.get(left) != recovery_authorization.get(right) for left, right in exact.items()):
        raise BackfillSafetyError("recovery authorization does not match the ambiguous fence")
    evidence_hash = _sha256_bytes(_canonical_json(recovery_evidence).encode("ascii"))
    if evidence_hash != recovery_authorization.get("recovery_evidence_sha256"):
        raise BackfillSafetyError("recovery evidence SHA mismatch")
    governed: Any = governed_evidence
    if governed is _UNSET:
        governed = _get_recovery_governed_evidence(
            connection,
            package_id=UUID(cast(str, recovery_authorization["package_id"])),
            run_id=UUID(cast(str, recovery_authorization["run_id"])),
            authorization_fingerprint=cast(str, recovery_authorization["original_authorization_fingerprint"]),
        )
    expected = cast(str, recovery_authorization["expected_outcome"])
    if governed is not None and (str(governed.package_id) != recovery_authorization["package_id"] or str(governed.run_id) != recovery_authorization["run_id"] or governed.package_sha256 != recovery_authorization["package_sha256"] or str(governed.supervisor_run_id) != recovery_authorization["supervisor_run_id"] or governed.authorization_fingerprint != recovery_authorization["original_authorization_fingerprint"]):
        raise BackfillSafetyError("governed recovery evidence lineage mismatch")
    if governed is not None and governed.commit_outcome == "committed":
        definitive = "committed"
    elif governed is not None and governed.commit_outcome == "ambiguous":
        definitive = expected
        manifests.resolve_ambiguous_commit_outcome(
            cast(Any, connection),
            run_id=UUID(cast(str, recovery_authorization["run_id"])),
            supervisor_run_id=UUID(cast(str, recovery_authorization["supervisor_run_id"])),
            authorization_fingerprint=cast(str, recovery_authorization["original_authorization_fingerprint"]),
            package_sha256=cast(str, recovery_authorization["package_sha256"]),
            recovery_authorization_fingerprint=cast(str, recovery_authorization["recovery_authorization_fingerprint"]),
            recovery_evidence_sha256=evidence_hash,
            outcome=definitive,
        )
        commit = getattr(connection, "commit", None)
        if not callable(commit):
            raise BackfillSafetyError("recovery connection cannot commit a governed resolution")
        commit()
    elif governed is None and expected == "rolled_back":
        zero_proof = _get_recovery_zero_proof(
            connection,
            package_id=UUID(cast(str, recovery_authorization["package_id"])),
            run_id=UUID(cast(str, recovery_authorization["run_id"])),
        )
        if any(zero_proof.values()) or recovery_evidence != {"durable_table_delta": 0, **zero_proof}:
            raise BackfillSafetyError("zero durable delta and source-transition absence were not proven")
        definitive = "rolled_back"
    else:
        raise BackfillSafetyError("governed evidence cannot establish a definitive recovery outcome")
    if definitive != expected:
        raise BackfillSafetyError("governed outcome contradicts the authorized recovery decision")
    updated_record = dict(record)
    if definitive == "committed":
        terminal = updated_record.get("terminal_result")
        if not isinstance(terminal, Mapping) or terminal.get("state") not in SUCCESS_STATES:
            raise BackfillSafetyError("committed recovery lacks a terminal result")
        updated_record.update(dict(terminal))
    else:
        updated_record["state"] = "recovery_rolled_back"
    updated_record["recovery_decision"] = definitive
    updated_record["recovery_authorization_fingerprint"] = recovery_authorization["recovery_authorization_fingerprint"]
    updated_record["recovery_evidence_sha256"] = evidence_hash
    cast(dict[str, object], records)[cast(str, identity)] = updated_record
    status["active_package"] = None
    status["active_attempt"] = None
    status["lease"] = None
    status["final_exit_state"] = "recovered_committed" if definitive == "committed" else "recovered_rolled_back_requires_new_authorization"
    with _FileLock(status_path.with_suffix(".status.lock")):
        _write_status(status_path, status)
    return {"state": "recovered", "outcome": definitive, "identity": identity, "status_path": str(status_path)}


def promote_canary_packages(
    certificate: Mapping[str, object],
    *,
    inventory: Mapping[str, object],
    canary_records: Mapping[str, object],
    full_records: Mapping[str, object],
    existing_transitions: Sequence[Mapping[str, object]],
    append_transition: Callable[[Mapping[str, object]], None],
) -> list[dict[str, object]]:
    """Append immutable certificate-bound promotion evidence through an injected writer."""
    inventory_hash = inventory.get("inventory_hash")
    if not isinstance(inventory_hash, str):
        raise BackfillSafetyError("invalid inventory for canary promotion")
    valid = _validate_canary_certificate(certificate, inventory_hash=inventory_hash)
    if valid is None:
        raise BackfillSafetyError("canary promotion requires a certificate")
    packages = {item["identity"]: item["package_sha256"] for item in _validate_scope(valid["packages"], inventory_hash=inventory_hash, label="canary certificate")}
    inventory_packages = {cast(str, item["identity"]): cast(str, item["package_sha256"]) for item in cast(list[dict[str, object]], inventory.get("packages", []))}
    if not packages or any(inventory_packages.get(identity) != digest for identity, digest in packages.items()):
        raise BackfillSafetyError("canary certificate package differs from inventory")
    transitions: list[dict[str, object]] = []
    seen_full: set[str] = set()
    for identity, package_hash in packages.items():
        canary = canary_records.get(identity)
        full = full_records.get(identity)
        if not isinstance(canary, Mapping) or canary.get("state") not in SUCCESS_STATES or canary.get("package_sha256") != package_hash or canary.get("run_id") != valid["canary_run_id"] or canary.get("authorization_fingerprint") != valid["canary_authorization_fingerprint"] or not _is_sha256(canary.get("reconciliation_hash")):
            raise BackfillSafetyError("canary certificate package is absent, nonterminal, or unreconciled")
        if isinstance(full, Mapping):
            raise BackfillSafetyError("cross-run duplicate package evidence")
        if identity in seen_full:
            raise BackfillSafetyError("duplicate canary promotion package")
        seen_full.add(identity)
        transition = {
            "state": "CANARY_PROMOTED", "certificate_id": valid["certificate_id"],
            "canary_run_id": valid["canary_run_id"], "identity": identity,
            "package_sha256": package_hash, "reconciliation_hash": canary["reconciliation_hash"],
        }
        same = [dict(item) for item in existing_transitions if item.get("identity") == identity]
        if same:
            if same != [transition]:
                raise BackfillSafetyError("canary promotion transition is immutable")
        else:
            append_transition(transition)
        transitions.append(transition)
    return transitions


def promote_certified_canary_packages(
    connection: object,
    *,
    certificate: Mapping[str, object],
    inventory: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    """Promote the three governed canaries atomically and return status seeds."""
    inventory_hash = inventory.get("inventory_hash")
    if not isinstance(inventory_hash, str):
        raise BackfillSafetyError("invalid inventory for certified promotion")
    valid = _validate_v4_canary_certificate(certificate, inventory_hash=inventory_hash)
    inventory_entries = {cast(str, item["identity"]): item for item in cast(list[dict[str, object]], inventory.get("packages", []))}
    root_by_form = _validate_inventory(inventory)
    seeds: dict[str, dict[str, object]] = {}
    try:
        for item in cast(list[dict[str, object]], valid["packages"]):
            identity = cast(str, item["identity"])
            package_sha = cast(str, item["package_sha256"])
            inventory_package = inventory_entries.get(identity)
            if inventory_package is None:
                raise BackfillSafetyError("certified canary differs from full inventory")
            form = cast(str, inventory_package["form"])
            source_package = root_by_form[form] / Path(cast(str, inventory_package["relative_package_path"]))
            governed_package_sha = _derive_form_lock_key(form, source_package).split(":", 1)[-1]
            if package_sha != governed_package_sha:
                raise BackfillSafetyError("certified canary governed hash differs from inventory package contents")
            package_id = UUID(cast(str, item["package_id"]))
            ingestion_run_id = UUID(cast(str, item["ingestion_run_id"]))
            evidence = _get_recovery_governed_evidence(
                connection,
                package_id=package_id,
                run_id=ingestion_run_id,
                authorization_fingerprint=cast(str, valid["canary_authorization_fingerprint"]),
            )
            reconciliation_sha256 = _governed_reconciliation_sha256(connection, run_id=ingestion_run_id)
            if evidence is None or evidence.package_sha256 != package_sha or evidence.commit_outcome != "committed" or str(evidence.supervisor_run_id) != valid["canary_supervisor_run_id"] or evidence.authorization_fingerprint != valid["canary_authorization_fingerprint"] or reconciliation_sha256 != item["reconciliation_sha256"]:
                raise BackfillSafetyError("governed canary evidence does not match certificate")
            promoted = manifests.promote_certified_canary_package(
                cast(Any, connection),
                package_id=package_id,
                ingestion_run_id=ingestion_run_id,
                supervisor_run_id=UUID(cast(str, valid["canary_supervisor_run_id"])),
                authorization_fingerprint=cast(str, valid["canary_authorization_fingerprint"]),
                package_sha256=package_sha,
                reconciliation_sha256=cast(str, item["reconciliation_sha256"]),
                certificate_id=UUID(cast(str, valid["certificate_id"])),
                certificate_sha256=cast(str, valid["certificate_sha256"]),
            )
            seeds[identity] = {
                "state": "canary_promoted",
                "attempt": 0,
                "package_sha256": inventory_package["package_sha256"],
                "governed_package_sha256": package_sha,
                "run_id": str(ingestion_run_id),
                "reconciliation_hash": item["reconciliation_sha256"],
                "package_transition_id": promoted.package_transition_id,
                "certificate_id": valid["certificate_id"],
                "certificate_sha256": valid["certificate_sha256"],
            }
        commit = getattr(connection, "commit", None)
        if not callable(commit):
            raise BackfillSafetyError("certified promotion connection cannot commit")
        commit()
        return seeds
    except Exception:
        rollback = getattr(connection, "rollback", None)
        if callable(rollback):
            try:
                rollback()
            except Exception:
                pass
        raise


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


def _status_authorization_lineage(value: object) -> dict[str, object] | None:
    """Project only non-secret, resume-relevant authorization identifiers."""
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise BackfillSafetyError("invalid authorization lineage for status")
    target = value.get("target")
    allowed_target = ("project", "vm", "zone", "database", "server_address", "role")
    if target is None:
        allowed = {"authorization_id", "target_mode", "execution_mode", "package_scope", "canary_certificate"}
        minimal_projection = {key: value[key] for key in allowed & set(value)}
        _assert_no_secret(minimal_projection)
        return minimal_projection
    if not isinstance(target, Mapping) or any(not isinstance(target.get(key), str) or not target[key] for key in allowed_target):
        raise BackfillSafetyError("invalid authorization lineage target")
    projected: dict[str, object] = {
        "authorization_id": value.get("authorization_id"),
        "target": {key: target[key] for key in allowed_target},
        "secret_version_resource": value.get("secret_version_resource"),
    }
    if not isinstance(projected["authorization_id"], str) or not projected["authorization_id"]:
        raise BackfillSafetyError("invalid authorization lineage identifier")
    secret_version = projected["secret_version_resource"]
    if not isinstance(secret_version, str) or not re.fullmatch(r"projects/[^/]+/secrets/[^/]+/versions/[1-9][0-9]*", secret_version):
        raise BackfillSafetyError("invalid authorization lineage secret resource")
    _assert_no_secret(projected)
    return projected


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


def _fsync_parent_directory(path: Path) -> None:
    """Persist the rename on POSIX; Windows has no portable directory fsync."""
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _durable_json_replace(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as temporary:
            temporary.write(_canonical_json(value) + "\n")
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, path)
        temporary_path = None
        _fsync_parent_directory(path)
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass


def _write_status(path: Path, status: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    projected = dict(status)
    if "authorization_lineage" in projected:
        projected["authorization_lineage"] = _status_authorization_lineage(projected["authorization_lineage"])
    _assert_no_secret(projected)
    safe_status = _redact(projected)
    _assert_no_secret(safe_status)
    _durable_json_replace(path, cast(Mapping[str, object], safe_status))


def _write_inventory(path: Path, inventory: Mapping[str, object]) -> None:
    """Persist the integrity artifact verbatim; unlike status it is never redacted."""
    _validate_inventory(inventory)
    _durable_json_replace(path, inventory)


def _assert_external_run_dir(run_dir: Path) -> None:
    resolved = run_dir.resolve()
    repo_root = Path(__file__).resolve().parents[2]
    forbidden = (repo_root, *(source.root for source in IMMUTABLE_SOURCES), EXCLUDED_ROOT)
    if any(resolved == root.resolve() or resolved.is_relative_to(root.resolve()) for root in forbidden):
        raise BackfillSafetyError("run directory must be outside Git and SEC source roots")


def _can_resume(record: Mapping[str, object] | None, package: Mapping[str, object], inventory_hash: str, code_sha: str, authorization_fingerprint: str | None) -> bool:
    return bool(
        record
        and record.get("state") in SUCCESS_STATES | {"canary_promoted"}
        and record.get("package_sha256") == package.get("package_sha256")
        and record.get("inventory_hash") == inventory_hash
        and record.get("code_sha") == code_sha
        and record.get("authorization_fingerprint") == authorization_fingerprint
    )


def _requires_commit_recovery(record: Mapping[str, object]) -> bool:
    """Return whether a durable protected fence still needs DB adjudication."""
    if record.get("recovery_decision") in {"committed", "rolled_back"}:
        return False
    if record.get("state") in SUCCESS_STATES | {"canary_promoted", "recovery_rolled_back"}:
        return False
    return record.get("commit_window") in {"issued", "confirmed"}


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


class _BoundaryStopHandlers:
    """Convert TERM/INT into a boundary request without interrupting COMMIT."""

    def __init__(self) -> None:
        self.event = threading.Event()
        self.previous: dict[int, Any] = {}

    def __enter__(self) -> threading.Event:
        def request_stop(_signum: int, _frame: object) -> None:
            self.event.set()

        for value in (signal.SIGINT, signal.SIGTERM):
            self.previous[value] = signal.getsignal(value)
            signal.signal(value, request_stop)
        return self.event

    def __exit__(self, *_args: object) -> None:
        for value, previous in self.previous.items():
            signal.signal(value, previous)


def install_boundary_stop_handlers() -> _BoundaryStopHandlers:
    return _BoundaryStopHandlers()


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
    controlled_boundary_crash: bool = False,
    stop_contract_hash: str | None = None,
    boundary_stop_requested: Callable[[], bool] | None = None,
    supervisor_run_id: str | None = None,
    execution_identities: Sequence[str] | None = None,
    seeded_records: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, Any]:
    """Run one package at a time, recording a durable state after every attempt."""
    _assert_external_run_dir(status_path.parent)
    if not code_sha or not lease_owner or (heartbeat_interval_seconds is not None and heartbeat_interval_seconds <= 0) or (stop_contract_hash is not None and not _is_sha256(stop_contract_hash)):
        raise BackfillSafetyError("invalid inventory or supervisor identity")
    if supervisor_run_id is not None:
        try:
            UUID(supervisor_run_id)
        except ValueError as exc:
            raise BackfillSafetyError("invalid supervisor run UUID") from exc
    root_by_form = _validate_inventory(inventory)
    inventory_hash = cast(str, inventory["inventory_hash"])
    packages = cast(list[dict[str, object]], inventory["packages"])
    if execution_identities is not None:
        requested = set(execution_identities)
        if len(requested) != len(execution_identities) or not requested.issubset({cast(str, item["identity"]) for item in packages}):
            raise BackfillSafetyError("execution scope differs from validated inventory")
        packages = [item for item in packages if item["identity"] in requested]
    with _FileLock(status_path.with_suffix(".run.lock")):
        return _run_supervisor_locked(inventory_hash, packages, root_by_form, status_path, code_sha, execute_package, lease_owner, command, authorization_id, target_identity, authorization_fingerprint, authorization_lineage, heartbeat_interval_seconds, controlled_boundary_crash, stop_contract_hash, boundary_stop_requested, supervisor_run_id, seeded_records)


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
    controlled_boundary_crash: bool,
    stop_contract_hash: str | None,
    boundary_stop_requested: Callable[[], bool] | None,
    supervisor_run_id: str | None,
    seeded_records: Mapping[str, Mapping[str, object]] | None,
) -> dict[str, Any]:
    with _FileLock(status_path.with_suffix(".status.lock")):
        status = _load_status(status_path)
        projected_lineage = _status_authorization_lineage(authorization_lineage)
        if status and status.get("authorization_id") != authorization_id:
            raise BackfillSafetyError("resume status does not match execution authorization identity")
        if status and ("authorization_fingerprint" in status and status.get("authorization_fingerprint") != authorization_fingerprint or "authorization_fingerprint" not in status and authorization_fingerprint is not None):
            raise BackfillSafetyError("resume status does not match execution authorization fingerprint")
        if status and ("authorization_lineage" in status and status.get("authorization_lineage") != projected_lineage or "authorization_lineage" not in status and projected_lineage is not None):
            raise BackfillSafetyError("resume status does not match execution authorization lineage")
        if status and "target_identity" in status and status.get("target_identity") != (dict(target_identity) if target_identity is not None else {"kind": "unconfigured", "value": "no_database_connection"}):
            raise BackfillSafetyError("resume status does not match execution target identity")
        if status and status.get("stop_contract_hash") != stop_contract_hash:
            raise BackfillSafetyError("resume status does not match the hashed stop contract")
        if status and status.get("supervisor_run_id") != supervisor_run_id:
            raise BackfillSafetyError("resume status does not match supervisor run UUID")
        existing_value = status.get("packages")
        existing: dict[str, Any] = dict(existing_value) if isinstance(existing_value, dict) else {}
        rolled_back_identity = next(
            (
                identity
                for identity, record in existing.items()
                if isinstance(record, Mapping) and record.get("state") == "recovery_rolled_back"
            ),
            None,
        )
        if rolled_back_identity is not None:
            return {
                "state": "blocked",
                "blocked_package": rolled_back_identity,
                "reason": "new_authorization_required",
                "status_path": str(status_path),
            }
        protected = next(
            (
                (identity, record)
                for identity, record in existing.items()
                if isinstance(record, Mapping) and _requires_commit_recovery(record)
            ),
            None,
        )
        if protected is not None:
            identity, record = protected
            classified = dict(record)
            classified["state"] = "recovery_required"
            existing[identity] = classified
            status["packages"] = existing
            status["final_exit_state"] = "blocked_ambiguous_commit"
            status["blocked_package"] = identity
            _write_status(status_path, status)
            return {
                "state": "blocked",
                "blocked_package": identity,
                "reason": "recovery_required",
                "status_path": str(status_path),
            }
        if _unexpired_lease(status):
            raise BackfillSafetyError("an active historical package lease has not expired")
        if any(isinstance(record, Mapping) and record.get("state") == "ambiguous_commit" for record in existing.values()):
            raise BackfillSafetyError("ambiguous commit requires explicit recovery before resume")
        if seeded_records:
            if status:
                raise BackfillSafetyError("canary promotion seeds are valid only for a new full run")
            package_by_identity = {cast(str, item["identity"]): item for item in packages}
            if len(seeded_records) != 3:
                raise BackfillSafetyError("full run requires exactly three promoted canary seeds")
            for identity, seed in seeded_records.items():
                package = package_by_identity.get(identity)
                if package is None or seed.get("state") != "canary_promoted" or seed.get("package_sha256") != package.get("package_sha256"):
                    raise BackfillSafetyError("invalid promoted canary status seed")
                existing[identity] = {
                    **dict(seed), "inventory_hash": inventory_hash, "code_sha": code_sha,
                    "authorization_id": authorization_id, "authorization_fingerprint": authorization_fingerprint,
                }
    status = {
        "schema_version": 1,
        "sanitized_command": _sanitize_command(command),
        "code_sha": code_sha,
        "interpreter": sys.version.split()[0],
        "dependency_identity": {"python": sys.implementation.name, "psycopg": importlib.metadata.version("psycopg")},
        "target_identity": dict(target_identity) if target_identity is not None else {"kind": "unconfigured", "value": "no_database_connection"},
        "authorization_id": authorization_id,
        "authorization_fingerprint": authorization_fingerprint,
        "authorization_lineage": projected_lineage,
        "supervisor_run_id": supervisor_run_id,
        "stop_contract_hash": stop_contract_hash,
        "inventory_hash": inventory_hash,
        "packages": existing,
        "active_package": None,
        "active_attempt": None,
        "lease": None,
        "heartbeat_at": _timestamp(),
        "final_exit_state": "running",
    }
    for package in sorted(packages, key=lambda candidate: str(candidate["identity"])):
        if boundary_stop_requested is not None and boundary_stop_requested():
            status["final_exit_state"] = "stopped_boundary"
            status["stop_reason_code"] = "boundary_stop"
            with _FileLock(status_path.with_suffix(".status.lock")):
                _write_status(status_path, status)
            return {"state": "stopped", "reason": "boundary_stop", "status_path": str(status_path)}
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
        def commit_fence(commit_window: str, evidence: Mapping[str, object]) -> None:
            if commit_window not in {"issued", "confirmed", "ambiguous"}:
                raise BackfillSafetyError("invalid protected commit fence state")
            if evidence.get("identity") != identity or evidence.get("inventory_package_sha256") != package.get("package_sha256") or not _is_sha256(evidence.get("package_sha256")) or evidence.get("authorization_fingerprint") != authorization_fingerprint or evidence.get("supervisor_run_id") != supervisor_run_id:
                raise BackfillSafetyError("protected commit fence lineage mismatch")
            if commit_window == "issued" and pulse is not None and pulse.failure_reason is not None:
                raise BackfillSafetyError("heartbeat renewal failed before protected commit")
            record = dict(cast(Mapping[str, object], existing[identity]))
            record.update({
                "commit_window": commit_window,
                "run_id": evidence.get("run_id"),
                "package_id": evidence.get("package_id"),
                "governed_package_sha256": evidence.get("package_sha256"),
                "supervisor_run_id": evidence.get("supervisor_run_id"),
                "reconciliation_hash": evidence.get("reconciliation_hash"),
                "terminal_result": evidence.get("terminal_result"),
            })
            if commit_window == "ambiguous":
                record["state"] = "ambiguous_commit"
            existing[identity] = record
            with _FileLock(status_path.with_suffix(".status.lock")):
                _write_status(status_path, status)
        try:
            try:
                _verify_package_unchanged(package, root_by_form)
                protected = getattr(execute_package, "execute_with_fence", None)
                result = dict(protected(package, commit_fence) if callable(protected) else execute_package(package))
                state = result.get("state")
                _verify_package_unchanged(package, root_by_form)
            except AmbiguousCommitError:
                result = {"state": "ambiguous_commit"}
                state = "ambiguous_commit"
            except BackfillSafetyError as error:
                result = {
                    "state": "failed",
                    "reason_code": _reason_code_for_safety_error(error),
                    "safety_error_digest": _sha256_bytes(str(error).encode("utf-8")),
                }
                state = "failed"
            except Exception:  # executor boundaries must be recorded, never ignored
                result = {"state": "failed", "reason_code": "executor_exception"}
                state = "failed"
        finally:
            if pulse is not None:
                pulse.stop()
        confirmed_fence = isinstance(existing.get(identity), Mapping) and cast(Mapping[str, object], existing[identity]).get("commit_window") == "confirmed"
        if pulse is not None and pulse.failure_reason is not None and state != "ambiguous_commit" and not confirmed_fence:
            result = {"state": "failed", "reason_code": pulse.failure_reason}
            state = "failed"
        if state == "ambiguous_commit":
            current_record = dict(cast(Mapping[str, object], existing[identity]))
            current_record["state"] = "ambiguous_commit"
            existing[identity] = current_record
            status["final_exit_state"] = "blocked_ambiguous_commit"
            status["blocked_package"] = identity
            with _FileLock(status_path.with_suffix(".status.lock")):
                _write_status(status_path, status)
            return {"state": "blocked", "blocked_package": identity, "reason": "ambiguous_commit", "status_path": str(status_path)}
        if state not in SUCCESS_STATES and state != "failed":
            result = {"state": "failed", "reason_code": "unexpected_package_state"}
            state = "failed"
        if state == "failed" and result.get("reason_code") not in FAILURE_REASON_CODES:
            result = {"state": "failed", "reason_code": "executor_reported_failure"}
        reason_code = cast(str, result.get("reason_code", ""))
        existing[identity] = {"state": state, "attempt": attempts, "package_sha256": package.get("package_sha256"), "inventory_hash": inventory_hash, "code_sha": code_sha, "authorization_id": authorization_id, "authorization_fingerprint": authorization_fingerprint, "result_state": result.get("state"), "reason_code": reason_code or None, "error_digest": _error_digest(reason_code) if state == "failed" and reason_code else None, "safety_error_digest": result.get("safety_error_digest"), "failure_detail": result.get("failure_detail"), "rows": result.get("rows"), "run_id": result.get("run_id"), "reconciliation_hash": result.get("reconciliation_hash")}
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
        if controlled_boundary_crash:
            raise BackfillSafetyError("controlled boundary crash after confirmed commit and checkpoint")
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
    parser.add_argument("action", choices=("start", "status", "resume", "recover", "rollback-probe"))
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    parser.add_argument("--execution-authorization", type=Path)
    parser.add_argument("--recovery-authorization", type=Path)
    parser.add_argument("--recovery-evidence", type=Path)
    parser.add_argument("--controlled-boundary-crash", action="store_true")
    args = parser.parse_args(argv)
    _assert_external_run_dir(args.run_dir)
    status_path = args.run_dir / "status.json"
    if args.action == "status":
        print(_canonical_json(_load_status(status_path)))
        return 0
    if args.action == "recover":
        if args.execution_authorization is None or args.recovery_authorization is None or args.recovery_evidence is None:
            raise BackfillSafetyError("recover requires execution authorization, recovery authorization, and recovery evidence")
        inventory_path = args.run_dir / "inventory.json"
        if not inventory_path.exists() or not status_path.exists():
            raise BackfillSafetyError("recover requires existing status and inventory artifacts")
        inventory = _load_status(inventory_path)
        _validate_inventory(inventory)
        current_code_sha = code_identity()
        recovery_executor = build_authorized_executor(args.execution_authorization, inventory=inventory, code_sha=current_code_sha, run_directory=args.run_dir)
        recovery_status = _load_status(status_path)
        recovery = load_recovery_authorization(
            args.recovery_authorization,
            code_sha=current_code_sha,
            inventory_hash=cast(str, inventory["inventory_hash"]),
            status_path=status_path,
            original_authorization_fingerprint=recovery_executor.authorization_fingerprint,
        )
        if recovery_status.get("authorization_fingerprint") != recovery_executor.authorization_fingerprint:
            raise BackfillSafetyError("original execution authorization does not match durable status")
        try:
            evidence_value = json.loads(args.recovery_evidence.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BackfillSafetyError("invalid recovery evidence artifact") from exc
        if not isinstance(evidence_value, dict):
            raise BackfillSafetyError("invalid recovery evidence artifact")
        outcome = recovery_executor.recover_ambiguous(status_path=status_path, recovery_authorization=recovery, recovery_evidence=evidence_value)
        print(_canonical_json(outcome))
        return 0
    with install_boundary_stop_handlers() as boundary_stop:
        with _FileLock(args.run_dir / ".lifecycle.lock"):
            inventory_path = args.run_dir / "inventory.json"
            current_code_sha = code_identity()
            authorization_command: Sequence[str] = ("historical-backfill", args.action)
            source_mode: str | None = None
            sources: Sequence[SourceSpec] = IMMUTABLE_SOURCES
            if args.execution_authorization is not None:
                source_mode, sources = _load_authorized_source_configuration(args.execution_authorization, code_sha=current_code_sha, run_directory=args.run_dir, command=authorization_command)
                if source_mode == "production_authorized":
                    validate_production_paths(sources, args.run_dir)
            if args.action in {"start", "rollback-probe"}:
                if status_path.exists() or inventory_path.exists():
                    raise BackfillSafetyError("start or rollback probe requires an empty external run directory")
                if source_mode in {None, "test_unbound"}:
                    inventory = build_historical_inventory()
                    _validate_historical_boundary(inventory)
                else:
                    inventory = build_inventory(sources)
                    _validate_historical_boundary(inventory, sources)
                _write_inventory(inventory_path, inventory)
                status: Mapping[str, object] = {}
                status_missing = False
            else:
                if not inventory_path.exists():
                    raise BackfillSafetyError("resume requires an existing inventory artifact")
                inventory = _load_status(inventory_path)
                if source_mode in {None, "test_unbound"}:
                    _validate_historical_boundary(inventory)
                else:
                    _validate_historical_boundary(inventory, sources, verify_contents=False)
                status_missing = not status_path.exists()
                status = _load_status(status_path)
                if status and (status.get("schema_version") != 1 or status.get("inventory_hash") != inventory.get("inventory_hash") or status.get("code_sha") != current_code_sha):
                    raise BackfillSafetyError("resume status does not match inventory or code identity")
            if args.execution_authorization is None:
                if args.action == "rollback-probe":
                    raise BackfillSafetyError("rollback probe requires a production canary authorization")
                if args.action == "resume" and status_missing:
                    raise BackfillSafetyError("missing status requires a full certificate reconstruction")
                executor: Callable[[dict[str, object]], Mapping[str, object]] = _unconfigured_executor
                authorization_id = None
                target_identity = None
                authorization_fingerprint = None
                authorization_lineage = None
                stop_contract_hash = None
                supervisor_run_id = None
                execution_identities = None
                seeded_records = None
            else:
                executor = build_authorized_executor(args.execution_authorization, inventory=inventory, code_sha=current_code_sha, run_directory=args.run_dir, command=authorization_command)
                authorization_id = executor.authorization_id
                target_identity = executor.target_identity
                authorization_fingerprint = executor.authorization_fingerprint
                authorization_lineage = executor.authorization_lineage
                stop_contract_hash = getattr(executor, "stop_contract_hash", None)
                supervisor_value = getattr(executor, "supervisor_run_id", None) or status.get("supervisor_run_id") or str(uuid4())
                if not isinstance(supervisor_value, str):
                    raise BackfillSafetyError("invalid supervisor run UUID")
                supervisor_run_id = supervisor_value
                try:
                    setattr(executor, "supervisor_run_id", supervisor_run_id)
                except Exception:
                    pass
                executor_authorization = getattr(executor, "authorization", None)
                if isinstance(executor_authorization, Mapping) and "package_scope" in executor_authorization:
                    scope = _validate_scope(executor_authorization["package_scope"], inventory_hash=cast(str, inventory["inventory_hash"]), label="authorization")
                    execution_identities = [item["identity"] for item in scope]
                else:
                    execution_identities = None
                seeded_records = None
                preflight = getattr(executor, "preflight", None)
                if callable(preflight):
                    preflight()
                if args.action == "rollback-probe":
                    evidence = executor.run_rollback_probe(evidence_path=args.run_dir / "rollback-probe.json")
                    print(_canonical_json({"state": "rollback_probed", "evidence_path": str(args.run_dir / "rollback-probe.json"), "identity": evidence["identity"]}))
                    return 0
                if args.action in {"start", "resume"} and isinstance(executor_authorization, Mapping) and executor_authorization.get("execution_mode") == "full" and (args.action == "start" or status_missing):
                    seeded_records = executor.promote_canary_certificate()
                elif args.action == "resume" and status_missing and not (
                    isinstance(executor_authorization, Mapping)
                    and executor_authorization.get("execution_mode") == "canary"
                ):
                    raise BackfillSafetyError("missing status can be reconstructed only from a full certificate")
            outcome = run_supervisor(inventory, status_path=status_path, code_sha=current_code_sha, execute_package=executor, lease_owner=f"pid-{os.getpid()}", command=("historical-backfill", args.action), authorization_id=authorization_id, target_identity=target_identity, authorization_fingerprint=authorization_fingerprint, authorization_lineage=authorization_lineage, heartbeat_interval_seconds=AUTHORIZED_HEARTBEAT_INTERVAL_SECONDS if args.execution_authorization is not None else None, controlled_boundary_crash=args.controlled_boundary_crash, stop_contract_hash=stop_contract_hash, boundary_stop_requested=boundary_stop.is_set, supervisor_run_id=supervisor_run_id, execution_identities=execution_identities, seeded_records=seeded_records)
            print(_canonical_json(outcome))
            return 0 if outcome["state"] == "ok" else 1
