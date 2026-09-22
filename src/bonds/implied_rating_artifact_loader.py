"""Fail-closed loader for the frozen round-002 implied-rating artifact.

The module admits one byte-pinned Parquet artifact and publishes it only through
the existing implied-rating materializer.  The checked-in contract is
intentionally identity-pending until a separately reviewed input identity
receipt supplies the producer fingerprint and deterministic publication UUID.
"""
from __future__ import annotations

import contextlib
import hashlib
import importlib.metadata
import json
import math
import os
import re
import stat
import sys
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO
from uuid import UUID, uuid4

try:
    import resource as _resource
except ImportError:  # pragma: no cover - Windows verification path
    _resource = None

import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from psycopg.pq import TransactionStatus

from src.bonds import implied_rating as policy
from src.bonds.errors import BondError
from src.bonds.implied_rating_materializer import (
    BUILD_COLUMNS,
    FRAME_COLUMNS,
    ImpliedRatingPublication,
    build_fingerprint,
    install_schema,
    materialize,
    publication_id_for,
    publication_row_tuples,
)

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts" / "bond_market_implied_rating_round002_artifact.json"
PRODUCT = "bond_market_implied_rating_v1"
PANEL_PRODUCT = "bond_panel_v1"
CONTRACT_SCHEMA = "bond_market_implied_rating_round002_artifact/1"
IDENTITY_SCHEMA = "bond_market_implied_rating_round002_input_identity/1"
RECEIPT_SCHEMA = "bond_market_implied_rating_artifact_loader_receipt/1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CUSIP_RE = re.compile(r"^[0-9A-Z]{9}$")
ROW_BATCH = 50_000
CONNECT_TIMEOUT_SECONDS = 15
LOCK_TIMEOUT_MS = 5_000
SQL_TIMEOUT_MS = 7_200_000
IDLE_TRANSACTION_TIMEOUT_MS = 7_200_000
SUPERVISED_PROCESS_CAP_SECONDS = 9_000
TIMEOUT_DECISION = (
    "measured full-volume rollback 1991s and apply 1229s; retain finite 2h SQL/idle "
    "timeouts with a separate 9000s supervised whole-process cap"
)


class ContractStatus(StrEnum):
    IDENTITY_PENDING = "identity_pending"
    READY = "ready"


class PublicationState(StrEnum):
    ABSENT = "absent"
    EXACT_CURRENT = "exact_current"
    EXACT_VALIDATED_UNPOINTED = "exact_validated_unpointed"


class ErrorCode(StrEnum):
    MISSING_IDENTITY = "missing_identity"
    CONTRACT_INVALID = "contract_invalid"
    RUNTIME_MISMATCH = "runtime_mismatch"
    SOURCE_MISMATCH = "source_mismatch"
    FILE_UNSAFE = "file_unsafe"
    FILE_CHANGED = "file_changed"
    FILE_HASH_MISMATCH = "file_hash_mismatch"
    JSON_INVALID = "json_invalid"
    MANIFEST_MISMATCH = "manifest_mismatch"
    ARROW_SCHEMA_MISMATCH = "arrow_schema_mismatch"
    RESOURCE_LIMIT = "resource_limit"
    ROW_INVALID = "row_invalid"
    ROW_KEY_MISMATCH = "row_key_mismatch"
    ROW_SUMMARY_MISMATCH = "row_summary_mismatch"
    ROW_DIGEST_MISMATCH = "row_digest_mismatch"
    IDENTITY_MISMATCH = "identity_mismatch"
    PARENT_MISMATCH = "parent_mismatch"
    SCHEMA_MISMATCH = "schema_mismatch"
    PUBLICATION_CONFLICT = "publication_conflict"
    STORED_MISMATCH = "stored_mismatch"
    LOCK_TIMEOUT = "lock_timeout"
    DB_FAILURE = "db_failure"
    RECEIPT_FAILURE = "receipt_failure"
    COMMIT_UNKNOWN = "commit_unknown"
    COMMITTED_EVIDENCE_INCOMPLETE = "committed_evidence_incomplete"
    CONFIGURATION_ERROR = "configuration_error"
    MATERIALIZER_REFUSAL = "materializer_refusal"


class ArtifactLoaderError(BondError):
    """Stable refusal with deliberately small, sanitized details."""

    def __init__(
        self,
        code: ErrorCode,
        *,
        field: str | None = None,
        receipt_written: bool = False,
        phase: str | None = None,
        schema_installed: bool | None = None,
        transaction_outcome: str | None = None,
        outcome: str | None = None,
    ) -> None:
        details = {} if field is None else {"field": field}
        super().__init__(code.value, details)
        self.error_code = code
        self.receipt_written = receipt_written
        self.phase = phase
        self.schema_installed = schema_installed
        self.transaction_outcome = transaction_outcome
        self.outcome = outcome


@dataclass(frozen=True, slots=True)
class FilePin:
    label: str
    relative_path: str
    size_bytes: int | None
    sha256: str | None


@dataclass(frozen=True, slots=True)
class ArrowFieldPin:
    name: str
    type_name: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class SourcePin:
    relative_path: str
    historical_sha256: str
    accepted_runtime_sha256: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class IdentityPins:
    input_fingerprint: str
    publication_id: str
    receipt: FilePin
    stopped_run_evidence_sha256: str


@dataclass(frozen=True, slots=True)
class ParentPins:
    publication_id: str
    parent_publication_id: str
    config_hash: str
    code_revision: str
    first_month: date
    last_closed_month: date
    open_month: date
    declared_counts: tuple[int, int, int, int]
    repair_contract: str
    source_sha256_keys: tuple[str, ...]
    snapshot_source_sha256: str
    header_sha256: str | None


@dataclass(frozen=True, slots=True)
class AdmissionLimits:
    max_manifest_bytes: int
    max_row_groups: int
    max_rows: int
    max_decoded_bytes: int
    max_footer_bytes: int
    max_string_bytes: int


@dataclass(frozen=True, slots=True)
class RuntimePins:
    python_minor: str
    distributions: tuple[tuple[str, str], ...]
    base_image: str
    connect_timeout_seconds: int
    lock_timeout_ms: int
    statement_timeout_ms: int
    idle_transaction_timeout_ms: int
    supervised_process_cap_seconds: int
    timeout_decision: str


@dataclass(frozen=True, slots=True)
class ArtifactSummary:
    row_count: int
    first_month: date
    last_month: date
    month_count: int
    unique_key_count: int
    witnessed_count: int
    d_confirmed_count: int
    d_candidate_count: int
    histogram: tuple[tuple[str, int], ...]
    null_counts: tuple[tuple[str, int], ...]
    rows_digest: str


@dataclass(frozen=True, slots=True)
class FrozenArtifactContract:
    schema_version: str
    status: ContractStatus
    artifact: FilePin
    manifests: tuple[FilePin, ...]
    producer_revision: str
    producer_manifest_sha256: str
    sources: tuple[SourcePin, ...]
    policy_version: str
    policy_digest: str
    anchor_value: float
    anchor_repr: str
    fields: tuple[ArrowFieldPin, ...]
    expected: ArtifactSummary
    parent: ParentPins
    identity: IdentityPins | None
    runtime: RuntimePins
    limits: AdmissionLimits | None


@dataclass(frozen=True, slots=True)
class FileEvidence:
    label: str
    relative_path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class VerifiedArtifact:
    contract_sha256: str
    publication: ImpliedRatingPublication
    summary: ArtifactSummary
    files: tuple[FileEvidence, ...]
    table: pa.Table


@dataclass(frozen=True, slots=True)
class ParentEvidence:
    pointer_id: str
    pointer_changed_at: datetime
    header_sha256: str
    server_version_num: int


@dataclass(frozen=True, slots=True)
class StoredEvidence:
    state: PublicationState
    publication_id: str
    source_run_id: str
    source_package_id: str
    summary: ArtifactSummary
    pointer_id: str | None
    prepared_at: datetime
    validated_at: datetime


@dataclass(frozen=True, slots=True)
class ReceiptRef:
    phase: str
    basename: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class OperationResult:
    outcome: str
    publication_id: str | None
    schema_installed: bool
    stored: StoredEvidence | None
    parent: ParentEvidence | None
    receipts: tuple[ReceiptRef, ...]


@dataclass(frozen=True, slots=True)
class ReleaseEvidence:
    loader_commit: str
    loader_tree: str
    review_changeset_sha256: str
    review_report_sha256: str
    context_archive_sha256: str
    inventory_sha256: str
    exclusion_evidence_sha256: str
    requirements_lock_sha256: str
    dockerfile_sha256: str
    railway_toml_sha256: str
    target: str
    release_context_sha256: str


def _fail(code: ErrorCode, field: str) -> None:
    raise ArtifactLoaderError(code, field=field)


def _strict_json(raw: bytes, *, label: str) -> Any:
    def reject_duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail(ErrorCode.JSON_INVALID, f"{label}.duplicate_key")
            result[key] = value
        return result

    def reject_constant(_: str) -> Any:
        _fail(ErrorCode.JSON_INVALID, f"{label}.nonfinite")

    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicate,
            parse_constant=reject_constant,
        )
    except ArtifactLoaderError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactLoaderError(ErrorCode.JSON_INVALID, field=label) from exc


def _closed_object(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != keys:
        _fail(ErrorCode.CONTRACT_INVALID, label)
    return value


def _string(value: Any, label: str) -> str:
    if type(value) is not str or not value:
        _fail(ErrorCode.CONTRACT_INVALID, label)
    return value


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        _fail(ErrorCode.CONTRACT_INVALID, label)
    return value


def _sha(value: Any, label: str) -> str:
    text = _string(value, label)
    if SHA256_RE.fullmatch(text) is None:
        _fail(ErrorCode.CONTRACT_INVALID, label)
    return text


def _uuid(value: Any, label: str) -> str:
    text = _string(value, label)
    try:
        parsed = str(UUID(text))
    except ValueError as exc:
        raise ArtifactLoaderError(ErrorCode.CONTRACT_INVALID, field=label) from exc
    if parsed != text:
        _fail(ErrorCode.CONTRACT_INVALID, label)
    return text


def _date(value: Any, label: str) -> date:
    text = _string(value, label)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ArtifactLoaderError(ErrorCode.CONTRACT_INVALID, field=label) from exc
    if parsed.isoformat() != text:
        _fail(ErrorCode.CONTRACT_INVALID, label)
    return parsed


def _relative_path(value: Any, label: str) -> str:
    text = _string(value, label)
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts or "." in path.parts or "\\" in text:
        _fail(ErrorCode.CONTRACT_INVALID, label)
    return text


def _file_pin(value: Any, label: str) -> FilePin:
    obj = _closed_object(value, {"label", "relative_path", "size_bytes", "sha256"}, label)
    size = obj["size_bytes"]
    digest = obj["sha256"]
    if size is not None:
        size = _integer(size, f"{label}.size_bytes", minimum=1)
    if digest is not None:
        digest = _sha(digest, f"{label}.sha256")
    return FilePin(
        label=_string(obj["label"], f"{label}.label"),
        relative_path=_relative_path(obj["relative_path"], f"{label}.relative_path"),
        size_bytes=size,
        sha256=digest,
    )


def _summary(value: Any, label: str) -> ArtifactSummary:
    keys = {
        "row_count", "first_month", "last_month", "month_count", "unique_key_count",
        "witnessed_count", "d_confirmed_count", "d_candidate_count", "histogram",
        "rows_digest",
    }
    obj = _closed_object(value, keys, label)
    histogram = obj["histogram"]
    if type(histogram) is not dict or set(histogram) != set(policy.BUCKET_ORDER):
        _fail(ErrorCode.CONTRACT_INVALID, f"{label}.histogram")
    pairs = tuple(
        (bucket, _integer(histogram[bucket], f"{label}.histogram.{bucket}"))
        for bucket in policy.BUCKET_ORDER
    )
    return ArtifactSummary(
        row_count=_integer(obj["row_count"], f"{label}.row_count", minimum=1),
        first_month=_date(obj["first_month"], f"{label}.first_month"),
        last_month=_date(obj["last_month"], f"{label}.last_month"),
        month_count=_integer(obj["month_count"], f"{label}.month_count", minimum=1),
        unique_key_count=_integer(
            obj["unique_key_count"], f"{label}.unique_key_count", minimum=1
        ),
        witnessed_count=_integer(obj["witnessed_count"], f"{label}.witnessed_count"),
        d_confirmed_count=_integer(
            obj["d_confirmed_count"], f"{label}.d_confirmed_count"
        ),
        d_candidate_count=_integer(
            obj["d_candidate_count"], f"{label}.d_candidate_count"
        ),
        histogram=pairs,
        null_counts=(),
        rows_digest=_sha(obj["rows_digest"], f"{label}.rows_digest"),
    )


def _parse_contract(raw: bytes) -> FrozenArtifactContract:
    root = _closed_object(
        _strict_json(raw, label="contract"),
        {
            "schema_version", "status", "artifact", "manifests", "producer", "arrow_schema",
            "expected", "anchor", "parent", "identity", "runtime", "admission_limits",
        },
        "contract",
    )
    if root["schema_version"] != CONTRACT_SCHEMA:
        _fail(ErrorCode.CONTRACT_INVALID, "schema_version")
    try:
        status = ContractStatus(root["status"])
    except (TypeError, ValueError) as exc:
        raise ArtifactLoaderError(ErrorCode.CONTRACT_INVALID, field="status") from exc

    producer = _closed_object(
        root["producer"],
        {"revision", "policy_version", "policy_digest", "manifest_module_sha256", "sources"},
        "producer",
    )
    source_values = producer["sources"]
    if type(source_values) is not list:
        _fail(ErrorCode.CONTRACT_INVALID, "producer.sources")
    sources: list[SourcePin] = []
    for index, value in enumerate(source_values):
        obj = _closed_object(
            value,
            {"relative_path", "historical_sha256", "accepted_runtime_sha256"},
            f"producer.sources.{index}",
        )
        accepted = obj["accepted_runtime_sha256"]
        if type(accepted) is not list:
            _fail(ErrorCode.CONTRACT_INVALID, f"producer.sources.{index}.accepted")
        sources.append(SourcePin(
            relative_path=_relative_path(
                obj["relative_path"], f"producer.sources.{index}.relative_path"
            ),
            historical_sha256=_sha(
                obj["historical_sha256"], f"producer.sources.{index}.historical_sha256"
            ),
            accepted_runtime_sha256=tuple(
                _sha(item, f"producer.sources.{index}.accepted.{item_index}")
                for item_index, item in enumerate(accepted)
            ),
        ))

    manifest_values = root["manifests"]
    if type(manifest_values) is not list:
        _fail(ErrorCode.CONTRACT_INVALID, "manifests")
    manifests = tuple(
        _file_pin(value, f"manifests.{index}")
        for index, value in enumerate(manifest_values)
    )
    if len({pin.label for pin in manifests}) != len(manifests):
        _fail(ErrorCode.CONTRACT_INVALID, "manifests.labels")

    schema_values = root["arrow_schema"]
    if type(schema_values) is not list:
        _fail(ErrorCode.CONTRACT_INVALID, "arrow_schema")
    fields: list[ArrowFieldPin] = []
    for index, value in enumerate(schema_values):
        obj = _closed_object(value, {"name", "type", "nullable"}, f"arrow_schema.{index}")
        if type(obj["nullable"]) is not bool:
            _fail(ErrorCode.CONTRACT_INVALID, f"arrow_schema.{index}.nullable")
        fields.append(ArrowFieldPin(
            _string(obj["name"], f"arrow_schema.{index}.name"),
            _string(obj["type"], f"arrow_schema.{index}.type"),
            obj["nullable"],
        ))

    anchor = _closed_object(root["anchor"], {"value", "repr"}, "anchor")
    if type(anchor["value"]) is not float or not math.isfinite(anchor["value"]):
        _fail(ErrorCode.CONTRACT_INVALID, "anchor.value")
    anchor_repr = _string(anchor["repr"], "anchor.repr")
    if repr(anchor["value"]) != anchor_repr or float(anchor_repr) != anchor["value"]:
        _fail(ErrorCode.CONTRACT_INVALID, "anchor.repr")

    parent_obj = _closed_object(
        root["parent"],
        {
            "publication_id", "parent_publication_id", "config_hash", "code_revision",
            "first_month", "last_closed_month", "open_month", "declared_counts",
            "repair_contract", "source_sha256_keys", "snapshot_source_sha256",
            "header_sha256",
        },
        "parent",
    )
    counts = parent_obj["declared_counts"]
    source_keys = parent_obj["source_sha256_keys"]
    if type(counts) is not list or len(counts) != 4:
        _fail(ErrorCode.CONTRACT_INVALID, "parent.declared_counts")
    if type(source_keys) is not list or not source_keys:
        _fail(ErrorCode.CONTRACT_INVALID, "parent.source_sha256_keys")
    header_sha = parent_obj["header_sha256"]
    if header_sha is not None:
        header_sha = _sha(header_sha, "parent.header_sha256")
    parent = ParentPins(
        publication_id=_uuid(parent_obj["publication_id"], "parent.publication_id"),
        parent_publication_id=_uuid(
            parent_obj["parent_publication_id"], "parent.parent_publication_id"
        ),
        config_hash=_string(parent_obj["config_hash"], "parent.config_hash"),
        code_revision=_string(parent_obj["code_revision"], "parent.code_revision"),
        first_month=_date(parent_obj["first_month"], "parent.first_month"),
        last_closed_month=_date(
            parent_obj["last_closed_month"], "parent.last_closed_month"
        ),
        open_month=_date(parent_obj["open_month"], "parent.open_month"),
        declared_counts=tuple(
            _integer(value, f"parent.declared_counts.{index}", minimum=1)
            for index, value in enumerate(counts)
        ),
        repair_contract=_string(parent_obj["repair_contract"], "parent.repair_contract"),
        source_sha256_keys=tuple(
            _string(value, f"parent.source_sha256_keys.{index}")
            for index, value in enumerate(source_keys)
        ),
        snapshot_source_sha256=_sha(
            parent_obj["snapshot_source_sha256"], "parent.snapshot_source_sha256"
        ),
        header_sha256=header_sha,
    )

    identity: IdentityPins | None = None
    if root["identity"] is not None:
        identity_obj = _closed_object(
            root["identity"],
            {"input_fingerprint", "publication_id", "receipt", "stopped_run_evidence_sha256"},
            "identity",
        )
        identity = IdentityPins(
            input_fingerprint=_sha(identity_obj["input_fingerprint"], "identity.input_fingerprint"),
            publication_id=_uuid(identity_obj["publication_id"], "identity.publication_id"),
            receipt=_file_pin(identity_obj["receipt"], "identity.receipt"),
            stopped_run_evidence_sha256=_sha(
                identity_obj["stopped_run_evidence_sha256"],
                "identity.stopped_run_evidence_sha256",
            ),
        )

    runtime_obj = _closed_object(
        root["runtime"],
        {
            "python_minor", "distributions", "base_image", "connect_timeout_seconds",
            "lock_timeout_ms", "statement_timeout_ms", "idle_transaction_timeout_ms",
            "supervised_process_cap_seconds", "timeout_decision",
        },
        "runtime",
    )
    distribution_values = runtime_obj["distributions"]
    if type(distribution_values) is not dict or not distribution_values:
        _fail(ErrorCode.CONTRACT_INVALID, "runtime.distributions")
    runtime = RuntimePins(
        python_minor=_string(runtime_obj["python_minor"], "runtime.python_minor"),
        distributions=tuple(
            sorted(
                (
                    _string(name, "runtime.distribution.name"),
                    _string(version, f"runtime.distributions.{name}"),
                )
                for name, version in distribution_values.items()
            )
        ),
        base_image=_string(runtime_obj["base_image"], "runtime.base_image"),
        connect_timeout_seconds=_integer(
            runtime_obj["connect_timeout_seconds"], "runtime.connect_timeout_seconds", minimum=1
        ),
        lock_timeout_ms=_integer(runtime_obj["lock_timeout_ms"], "runtime.lock_timeout_ms", minimum=1),
        statement_timeout_ms=_integer(
            runtime_obj["statement_timeout_ms"], "runtime.statement_timeout_ms", minimum=1
        ),
        idle_transaction_timeout_ms=_integer(
            runtime_obj["idle_transaction_timeout_ms"],
            "runtime.idle_transaction_timeout_ms",
            minimum=1,
        ),
        supervised_process_cap_seconds=_integer(
            runtime_obj["supervised_process_cap_seconds"],
            "runtime.supervised_process_cap_seconds",
            minimum=1,
        ),
        timeout_decision=_string(runtime_obj["timeout_decision"], "runtime.timeout_decision"),
    )

    limits: AdmissionLimits | None = None
    if root["admission_limits"] is not None:
        limits_obj = _closed_object(
            root["admission_limits"],
            {
                "max_manifest_bytes", "max_row_groups", "max_rows", "max_decoded_bytes",
                "max_footer_bytes", "max_string_bytes",
            },
            "admission_limits",
        )
        limits = AdmissionLimits(**{
            key: _integer(value, f"admission_limits.{key}", minimum=1)
            for key, value in limits_obj.items()
        })

    return FrozenArtifactContract(
        schema_version=CONTRACT_SCHEMA,
        status=status,
        artifact=_file_pin(root["artifact"], "artifact"),
        manifests=manifests,
        producer_revision=_string(producer["revision"], "producer.revision"),
        producer_manifest_sha256=_sha(
            producer["manifest_module_sha256"], "producer.manifest_module_sha256"
        ),
        sources=tuple(sources),
        policy_version=_string(producer["policy_version"], "producer.policy_version"),
        policy_digest=_sha(producer["policy_digest"], "producer.policy_digest"),
        anchor_value=anchor["value"],
        anchor_repr=anchor_repr,
        fields=tuple(fields),
        expected=_summary(root["expected"], "expected"),
        parent=parent,
        identity=identity,
        runtime=runtime,
        limits=limits,
    )


def _read_contract_bytes() -> bytes:
    try:
        return CONTRACT_PATH.read_bytes()
    except OSError as exc:
        raise ArtifactLoaderError(
            ErrorCode.CONTRACT_INVALID, field="contract.read"
        ) from exc


def load_frozen_contract() -> FrozenArtifactContract:
    raw = _read_contract_bytes()
    return _parse_contract(raw)


def _require_ready(contract: FrozenArtifactContract) -> None:
    if contract.status is ContractStatus.IDENTITY_PENDING or contract.identity is None:
        _fail(ErrorCode.MISSING_IDENTITY, "identity")
    if contract.limits is None:
        _fail(ErrorCode.MISSING_IDENTITY, "readiness_evidence")
    if any(pin.sha256 is None or pin.size_bytes is None for pin in contract.manifests):
        _fail(ErrorCode.MISSING_IDENTITY, "manifest_pins")
    if not contract.sources or any(not pin.accepted_runtime_sha256 for pin in contract.sources):
        _fail(ErrorCode.MISSING_IDENTITY, "source_pins")
    expected_id = publication_id_for(
        contract.policy_digest,
        contract.producer_revision,
        contract.identity.input_fingerprint,
    )
    if expected_id != contract.identity.publication_id:
        _fail(ErrorCode.IDENTITY_MISMATCH, "publication_id")


def _runtime_versions() -> dict[str, str]:
    try:
        return {
            name: importlib.metadata.version(name)
            for name in ("numpy", "pandas", "pyarrow", "psycopg", "psycopg-binary")
        }
    except importlib.metadata.PackageNotFoundError as exc:
        raise ArtifactLoaderError(
            ErrorCode.RUNTIME_MISMATCH, field="runtime.distribution_missing"
        ) from exc


def _verify_runtime(contract: FrozenArtifactContract) -> None:
    actual_minor = f"{sys.version_info.major}.{sys.version_info.minor}"
    if actual_minor != contract.runtime.python_minor:
        _fail(ErrorCode.RUNTIME_MISMATCH, "python_minor")
    actual = _runtime_versions()
    for name, version in contract.runtime.distributions:
        if actual.get(name) != version:
            _fail(ErrorCode.RUNTIME_MISMATCH, f"distribution.{name}")
    timeout_contract = (
        contract.runtime.connect_timeout_seconds,
        contract.runtime.lock_timeout_ms,
        contract.runtime.statement_timeout_ms,
        contract.runtime.idle_transaction_timeout_ms,
        contract.runtime.supervised_process_cap_seconds,
        contract.runtime.timeout_decision,
    )
    expected_timeouts = (
        CONNECT_TIMEOUT_SECONDS,
        LOCK_TIMEOUT_MS,
        SQL_TIMEOUT_MS,
        IDLE_TRANSACTION_TIMEOUT_MS,
        SUPERVISED_PROCESS_CAP_SECONDS,
        TIMEOUT_DECISION,
    )
    if timeout_contract != expected_timeouts:
        _fail(ErrorCode.RUNTIME_MISMATCH, "runtime.timeout_contract")


def _hash_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _hash_handle(handle: BinaryIO) -> str:
    handle.seek(0)
    digest = hashlib.sha256()
    while chunk := handle.read(1024 * 1024):
        digest.update(chunk)
    handle.seek(0)
    return digest.hexdigest()


def _stat_identity(result: os.stat_result) -> tuple[int, int, int, int, int]:
    if sys.platform == "win32":
        # CPython's path and descriptor stat calls expose different synthetic
        # inode/ctime values on Windows. Size+mtime bind the pathname while the
        # descriptor is hash-checked both before and after decoding.
        return (0, 0, int(result.st_size), int(result.st_mtime_ns), 0)
    return (
        int(result.st_dev), int(result.st_ino), int(result.st_size),
        int(result.st_mtime_ns), int(result.st_ctime_ns),
    )


def _is_reparse(result: os.stat_result) -> bool:
    attributes = int(getattr(result, "st_file_attributes", 0))
    reparse = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse)


@contextlib.contextmanager
def _open_regular(root: Path, relative_path: str) -> Iterator[tuple[BinaryIO, Path, os.stat_result]]:
    safe_relative = _relative_path(relative_path, "file.relative_path")
    try:
        root = root.resolve(strict=True)
        current = root
        for component in PurePosixPath(safe_relative).parts:
            current = current / component
            before = current.lstat()
            if stat.S_ISLNK(before.st_mode) or _is_reparse(before):
                _fail(ErrorCode.FILE_UNSAFE, "file.symlink")
        if not stat.S_ISREG(before.st_mode):
            _fail(ErrorCode.FILE_UNSAFE, "file.kind")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(current, flags)
    except ArtifactLoaderError:
        raise
    except OSError as exc:
        raise ArtifactLoaderError(ErrorCode.FILE_UNSAFE, field="file.open") from exc
    try:
        descriptor = os.fstat(fd)
        if not stat.S_ISREG(descriptor.st_mode) or _stat_identity(descriptor) != _stat_identity(before):
            _fail(ErrorCode.FILE_CHANGED, "file.open_identity")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            yield handle, current, descriptor
    finally:
        os.close(fd)


def _verify_unchanged(path: Path, descriptor: os.stat_result, handle: BinaryIO) -> None:
    after_descriptor = os.fstat(handle.fileno())
    after_path = path.lstat()
    expected = _stat_identity(descriptor)
    if _stat_identity(after_descriptor) != expected or _stat_identity(after_path) != expected:
        _fail(ErrorCode.FILE_CHANGED, "file.final_identity")


def _read_pinned_json(
    root: Path, pin: FilePin, limits: AdmissionLimits
) -> tuple[Any, FileEvidence]:
    if pin.sha256 is None or pin.size_bytes is None:
        _fail(ErrorCode.MISSING_IDENTITY, f"file.{pin.label}")
    if pin.size_bytes > limits.max_manifest_bytes:
        _fail(ErrorCode.RESOURCE_LIMIT, f"file.{pin.label}.size")
    with _open_regular(root, pin.relative_path) as (handle, path, descriptor):
        if descriptor.st_size != pin.size_bytes:
            _fail(ErrorCode.FILE_HASH_MISMATCH, f"file.{pin.label}.size")
        raw = handle.read(limits.max_manifest_bytes + 1)
        if len(raw) != pin.size_bytes or len(raw) > limits.max_manifest_bytes:
            _fail(ErrorCode.RESOURCE_LIMIT, f"file.{pin.label}.read")
        digest = _hash_bytes(raw)
        if digest != pin.sha256:
            _fail(ErrorCode.FILE_HASH_MISMATCH, f"file.{pin.label}.sha256")
        _verify_unchanged(path, descriptor, handle)
    return _strict_json(raw, label=pin.label), FileEvidence(
        pin.label, pin.relative_path, len(raw), digest
    )


def _verify_sources(contract: FrozenArtifactContract) -> tuple[FileEvidence, ...]:
    evidence: list[FileEvidence] = []
    for pin in contract.sources:
        if (
            pin.relative_path == "src/bonds/implied_rating.py"
            and pin.historical_sha256 != contract.producer_manifest_sha256
        ):
            _fail(ErrorCode.SOURCE_MISMATCH, "producer.historical_sha256")
        path = ROOT / PurePosixPath(pin.relative_path)
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise ArtifactLoaderError(
                ErrorCode.SOURCE_MISMATCH, field="source.read"
            ) from exc
        digest = _hash_bytes(raw)
        if digest not in pin.accepted_runtime_sha256:
            _fail(ErrorCode.SOURCE_MISMATCH, pin.relative_path)
        evidence.append(FileEvidence("source", pin.relative_path, len(raw), digest))
    return tuple(evidence)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if type(value) is not dict:
        _fail(ErrorCode.MANIFEST_MISMATCH, label)
    return value


def _manifest_value(root: Any, *path: str) -> Any:
    value = root
    for key in path:
        value = _mapping(value, ".".join(path))[key] if key in _mapping(value, key) else None
        if value is None:
            _fail(ErrorCode.MANIFEST_MISMATCH, ".".join(path))
    return value


def _verify_manifests(
    contract: FrozenArtifactContract, documents: Mapping[str, Any]
) -> None:
    required = {"build", "export", "gates"}
    if set(documents) != required:
        _fail(ErrorCode.MANIFEST_MISMATCH, "manifest.labels")
    build = documents["build"]
    export = documents["export"]
    gates = documents["gates"]
    expected = contract.expected
    checks: tuple[tuple[Any, Any, str], ...] = (
        (_manifest_value(build, "git", "head"), contract.producer_revision, "build.git.head"),
        (_manifest_value(build, "producer", "module_sha256"), contract.producer_manifest_sha256, "build.producer.module_sha256"),
        (_manifest_value(build, "producer", "policy_version"), contract.policy_version, "build.producer.policy_version"),
        (_manifest_value(build, "producer", "policy_digest"), contract.policy_digest, "build.producer.policy_digest"),
        (_manifest_value(build, "anchor_repr"), contract.anchor_repr, "build.anchor_repr"),
        (_manifest_value(build, "anchor_resolved"), contract.anchor_value, "build.anchor_resolved"),
        (_manifest_value(build, "rows"), expected.row_count, "build.rows"),
        (_manifest_value(build, "rows_digest"), expected.rows_digest, "build.rows_digest"),
        (_manifest_value(build, "rows_digest_after_parquet_roundtrip"), expected.rows_digest, "build.roundtrip_digest"),
        (_manifest_value(build, "roundtrip_digest_equal"), True, "build.roundtrip_equal"),
        (_manifest_value(build, "last_closed_month"), expected.last_month.isoformat(), "build.last_month"),
        (_manifest_value(build, "parquet", "bytes"), contract.artifact.size_bytes, "build.parquet.bytes"),
        (_manifest_value(build, "parquet", "sha256"), contract.artifact.sha256, "build.parquet.sha"),
        (_manifest_value(build, "default_counts", "d_confirmed_rows"), expected.d_confirmed_count, "build.confirmed"),
        (_manifest_value(build, "default_counts", "d_candidate_rows"), expected.d_candidate_count, "build.candidate"),
        (_manifest_value(export, "source_pointer"), contract.parent.publication_id, "export.pointer"),
        (_manifest_value(export, "source_parent"), contract.parent.parent_publication_id, "export.parent"),
        (_manifest_value(export, "publication", "publication_id"), contract.parent.publication_id, "export.publication"),
        (_manifest_value(export, "publication", "parent_publication_id"), contract.parent.parent_publication_id, "export.publication_parent"),
        (_manifest_value(export, "publication", "status"), "validated", "export.status"),
        (_manifest_value(export, "publication", "config_hash"), contract.parent.config_hash, "export.config"),
        (_manifest_value(export, "publication", "code_revision"), contract.parent.code_revision, "export.revision"),
        (_manifest_value(export, "pointer_checks", "pointer_unchanged"), True, "export.pointer_unchanged"),
        (_manifest_value(export, "pointer_checks", "child_validated"), True, "export.child_validated"),
        (_manifest_value(gates, "arm"), "baseline", "gates.arm"),
        (_manifest_value(gates, "data", "rows_total"), expected.row_count, "gates.rows"),
        (_manifest_value(gates, "data", "witnessed_rows_total"), expected.witnessed_count, "gates.witnessed"),
        (_manifest_value(gates, "d_inventory", "d_confirmed_rows_total"), expected.d_confirmed_count, "gates.confirmed"),
        (_manifest_value(gates, "d_inventory", "d_candidate_rows_total"), expected.d_candidate_count, "gates.candidate"),
        (_manifest_value(gates, "protocol", "policy_version"), contract.policy_version, "gates.policy_version"),
        (_manifest_value(gates, "protocol", "policy_digest"), contract.policy_digest, "gates.policy_digest"),
    )
    for actual, pinned, label in checks:
        if type(actual) is not type(pinned) or actual != pinned:
            _fail(ErrorCode.MANIFEST_MISMATCH, label)
    histogram = _manifest_value(gates, "data", "implied_bucket_distribution_total")
    if histogram != dict(expected.histogram):
        _fail(ErrorCode.MANIFEST_MISMATCH, "gates.histogram")


def _verify_identity_receipt(
    receipt: Any,
    contract: FrozenArtifactContract,
    manifest_pins: Mapping[str, FilePin],
    manifests: Mapping[str, Any],
) -> None:
    if contract.identity is None:
        _fail(ErrorCode.MISSING_IDENTITY, "identity")
    obj = _closed_object(
        receipt,
        {
            "schema_version", "snapshot", "parser", "producer_module_sha256",
            "input_fingerprint", "publication_id", "stopped_run_evidence_sha256",
            "artifact_sha256", "build_manifest_sha256", "export_manifest_sha256",
            "gate_manifest_sha256", "materializer_module_sha256", "build_fingerprint",
            "original_receipt_sha256", "evidence_manifest_sha256", "authority",
            "type_sensitivity",
        },
        "identity_receipt",
    )
    if obj["schema_version"] != IDENTITY_SCHEMA:
        _fail(ErrorCode.IDENTITY_MISMATCH, "receipt.schema_version")
    checks = {
        "producer_module_sha256": contract.producer_manifest_sha256,
        "input_fingerprint": contract.identity.input_fingerprint,
        "publication_id": contract.identity.publication_id,
        "stopped_run_evidence_sha256": contract.identity.stopped_run_evidence_sha256,
        "artifact_sha256": contract.artifact.sha256,
        "build_manifest_sha256": manifest_pins["build"].sha256,
        "export_manifest_sha256": manifest_pins["export"].sha256,
        "gate_manifest_sha256": manifest_pins["gates"].sha256,
        "materializer_module_sha256": _hash_bytes(
            (ROOT / "src" / "bonds" / "implied_rating_materializer.py").read_bytes()
        ),
        "build_fingerprint": build_fingerprint(
            contract.policy_digest,
            contract.producer_revision,
            contract.identity.input_fingerprint,
        ),
    }
    for field, expected in checks.items():
        if obj[field] != expected:
            _fail(ErrorCode.IDENTITY_MISMATCH, f"receipt.{field}")
    snapshot = _closed_object(
        obj["snapshot"], {"relative_path", "bytes", "rows", "sha256"}, "receipt.snapshot"
    )
    parser = _closed_object(
        obj["parser"],
        {
            "source_sha256", "function_source_sha256", "python", "pandas", "numpy",
            "authority", "type_sensitivity",
        },
        "receipt.parser",
    )
    if (
        _relative_path(snapshot["relative_path"], "receipt.snapshot.relative_path")
        != "snapshot.csv"
        or _integer(snapshot["bytes"], "receipt.snapshot.bytes", minimum=1) <= 0
        or _integer(snapshot["rows"], "receipt.snapshot.rows", minimum=1) <= 0
        or _sha(snapshot["sha256"], "receipt.snapshot.sha256")
        != _manifest_value(manifests["build"], "snapshot", "sha256")
        or snapshot["rows"] != _manifest_value(manifests["build"], "snapshot", "rows")
    ):
        _fail(ErrorCode.IDENTITY_MISMATCH, "receipt.snapshot")
    for field in ("source_sha256", "function_source_sha256"):
        _sha(parser[field], f"receipt.parser.{field}")
    for field in ("python", "pandas", "numpy", "authority", "type_sensitivity"):
        _string(parser[field], f"receipt.parser.{field}")
    if (
        obj["authority"] != "historical_round_002_csv_pandas"
        or parser["authority"] != obj["authority"]
        or obj["type_sensitivity"] != parser["type_sensitivity"]
    ):
        _fail(ErrorCode.IDENTITY_MISMATCH, "receipt.authority")
    _sha(obj["original_receipt_sha256"], "receipt.original_receipt_sha256")
    _sha(obj["evidence_manifest_sha256"], "receipt.evidence_manifest_sha256")


def _verify_arrow_schema(schema: pa.Schema, contract: FrozenArtifactContract) -> None:
    observed = tuple((field.name, str(field.type), field.nullable) for field in schema)
    expected = tuple((field.name, field.type_name, field.nullable) for field in contract.fields)
    if observed != expected:
        _fail(ErrorCode.ARROW_SCHEMA_MISMATCH, "arrow.schema")
    if tuple(field.name for field in contract.fields) != policy.PUBLICATION_COLUMNS:
        _fail(ErrorCode.SOURCE_MISMATCH, "publication_columns")
    if tuple(FRAME_COLUMNS) != policy.PUBLICATION_COLUMNS:
        _fail(ErrorCode.SOURCE_MISMATCH, "materializer_columns")


def _admit_arrow(table: pa.Table, contract: FrozenArtifactContract) -> None:
    _verify_arrow_schema(table.schema, contract)
    required = (
        "month", "cusip_id", "implied_bucket", "witnessed", "carry_months", "spell_id",
        "d_candidate", "d_confirmed", "censoring", "policy_version", "policy_digest",
    )
    for name in required:
        if table[name].null_count:
            _fail(ErrorCode.ROW_INVALID, f"row.required_null.{name}")
    for name in (
        "spread_norm_log", "neutralized_score", "market_level_l", "recovery_observed"
    ):
        values = table[name]
        finite = pc.is_finite(values)
        valid = pc.invert(pc.is_null(values))
        invalid = pc.and_(valid, pc.invert(pc.fill_null(finite, False)))
        if pc.any(invalid).as_py():
            _fail(ErrorCode.ROW_INVALID, f"row.nonfinite.{name}")
    cusips = table["cusip_id"].to_pylist()
    if any(type(value) is not str or CUSIP_RE.fullmatch(value) is None for value in cusips):
        _fail(ErrorCode.ROW_INVALID, "row.cusip_id")
    if any(len(value.encode("utf-8")) > contract.limits.max_string_bytes for value in cusips):
        _fail(ErrorCode.RESOURCE_LIMIT, "row.cusip_id.size")


def _to_canonical_frame(table: pa.Table) -> pd.DataFrame:
    frame = pd.DataFrame(index=np.arange(table.num_rows))
    for name in policy.PUBLICATION_COLUMNS:
        column = table[name]
        if name in {"month", "d_event_month"}:
            frame[name] = pd.to_datetime(column.to_pandas())
        elif name in {"cusip_id", "implied_bucket", "censoring", "policy_version", "policy_digest"}:
            frame[name] = np.asarray(column.to_pylist(), dtype=object)
        elif name in {"witnessed", "d_candidate", "d_confirmed"}:
            frame[name] = np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.bool_)
        elif name in {"carry_months", "spell_id"}:
            frame[name] = np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.int64)
        else:
            frame[name] = np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.float64)
    return frame.loc[:, list(policy.PUBLICATION_COLUMNS)]


def _validate_frame(
    frame: pd.DataFrame, contract: FrozenArtifactContract, *, compare_expected: bool = True
) -> ArtifactSummary:
    if tuple(frame.columns) != policy.PUBLICATION_COLUMNS:
        _fail(ErrorCode.ROW_INVALID, "row.columns")
    if len(frame) == 0:
        _fail(ErrorCode.ROW_INVALID, "row.empty")
    month = frame["month"]
    event = frame["d_event_month"]
    if month.isna().any() or (
        (month.dt.day != 1) | (month.dt.hour != 0) | (month.dt.minute != 0)
        | (month.dt.second != 0) | (month.dt.microsecond != 0)
    ).any():
        _fail(ErrorCode.ROW_INVALID, "row.month")
    event_present = event.notna()
    if (
        event_present
        & (
            (event.dt.day != 1) | (event.dt.hour != 0) | (event.dt.minute != 0)
            | (event.dt.second != 0) | (event.dt.microsecond != 0)
        )
    ).any():
        _fail(ErrorCode.ROW_INVALID, "row.d_event_month")
    if frame.duplicated(["month", "cusip_id"]).any():
        _fail(ErrorCode.ROW_KEY_MISMATCH, "row.duplicate_key")
    if not frame["cusip_id"].map(lambda value: CUSIP_RE.fullmatch(value) is not None).all():
        _fail(ErrorCode.ROW_INVALID, "row.cusip_id")
    if not frame["implied_bucket"].isin(policy.BUCKET_ORDER).all():
        _fail(ErrorCode.ROW_INVALID, "row.implied_bucket")
    if not frame["censoring"].isin(policy.CENSORING_KINDS).all():
        _fail(ErrorCode.ROW_INVALID, "row.censoring")
    if not frame["policy_version"].eq(contract.policy_version).all():
        _fail(ErrorCode.ROW_INVALID, "row.policy_version")
    if not frame["policy_digest"].eq(contract.policy_digest).all():
        _fail(ErrorCode.ROW_INVALID, "row.policy_digest")
    witnessed = frame["witnessed"]
    confirmed = frame["d_confirmed"]
    candidate = frame["d_candidate"]
    if (frame["carry_months"] < 0).any() or (frame["carry_months"] > 2_147_483_647).any():
        _fail(ErrorCode.ROW_INVALID, "row.carry_months")
    if (frame["spell_id"] < 1).any() or (frame["spell_id"] > 2_147_483_647).any():
        _fail(ErrorCode.ROW_INVALID, "row.spell_id")
    if ((frame["implied_bucket"] == "D") != confirmed).any():
        _fail(ErrorCode.ROW_INVALID, "row.default_bucket")
    if (event_present != confirmed).any():
        _fail(ErrorCode.ROW_INVALID, "row.default_event")
    if (event_present & ((event > month) | (event < pd.Timestamp(contract.expected.first_month)))).any():
        _fail(ErrorCode.ROW_INVALID, "row.default_event_window")
    if (frame["spread_norm_log"].isna() != ~witnessed).any():
        _fail(ErrorCode.ROW_INVALID, "row.spread_null")
    if (frame["neutralized_score"].isna() != ~witnessed).any():
        _fail(ErrorCode.ROW_INVALID, "row.score_null")
    terminal = frame["implied_bucket"].isin(("D", "WITHDRAWN", "NOT_RATED"))
    if (~((witnessed == frame["carry_months"].eq(0)) | terminal)).any():
        _fail(ErrorCode.ROW_INVALID, "row.witnessed_carry")
    if (candidate & ~witnessed).any():
        _fail(ErrorCode.ROW_INVALID, "row.candidate_witness")
    recovery = frame["recovery_observed"]
    if (recovery.notna() & (~confirmed | (recovery < 0))).any():
        _fail(ErrorCode.ROW_INVALID, "row.recovery")

    unique_months = pd.DatetimeIndex(sorted(month.unique()))
    expected_months = pd.date_range(
        contract.expected.first_month, contract.expected.last_month, freq="MS"
    )
    if not unique_months.equals(expected_months):
        _fail(ErrorCode.ROW_KEY_MISMATCH, "row.month_window")
    histogram_counter = Counter(frame["implied_bucket"].tolist())
    histogram = tuple((bucket, int(histogram_counter.get(bucket, 0))) for bucket in policy.BUCKET_ORDER)
    digest = policy.rows_digest(frame)
    summary = ArtifactSummary(
        row_count=len(frame),
        first_month=month.min().date(),
        last_month=month.max().date(),
        month_count=len(unique_months),
        unique_key_count=int(frame[["month", "cusip_id"]].drop_duplicates().shape[0]),
        witnessed_count=int(witnessed.sum()),
        d_confirmed_count=int(confirmed.sum()),
        d_candidate_count=int(candidate.sum()),
        histogram=histogram,
        null_counts=tuple((column, int(frame[column].isna().sum())) for column in frame.columns),
        rows_digest=digest,
    )
    if compare_expected:
        expected = contract.expected
        comparable = (
            summary.row_count, summary.first_month, summary.last_month, summary.month_count,
            summary.unique_key_count, summary.witnessed_count, summary.d_confirmed_count,
            summary.d_candidate_count, summary.histogram,
        )
        pinned = (
            expected.row_count, expected.first_month, expected.last_month, expected.month_count,
            expected.unique_key_count, expected.witnessed_count, expected.d_confirmed_count,
            expected.d_candidate_count, expected.histogram,
        )
        if comparable != pinned:
            _fail(ErrorCode.ROW_SUMMARY_MISMATCH, "row.summary")
        if summary.rows_digest != expected.rows_digest:
            _fail(ErrorCode.ROW_DIGEST_MISMATCH, "row.rows_digest")
    return summary


def _read_parquet(root: Path, contract: FrozenArtifactContract) -> tuple[pa.Table, FileEvidence]:
    pin = contract.artifact
    limits = contract.limits
    assert limits is not None
    assert pin.sha256 is not None and pin.size_bytes is not None
    with _open_regular(root, pin.relative_path) as (handle, path, descriptor):
        if descriptor.st_size != pin.size_bytes:
            _fail(ErrorCode.FILE_HASH_MISMATCH, "artifact.size")
        if _hash_handle(handle) != pin.sha256:
            _fail(ErrorCode.FILE_HASH_MISMATCH, "artifact.sha256")
        parquet = pq.ParquetFile(handle)
        metadata = parquet.metadata
        if (
            metadata.num_row_groups != limits.max_row_groups
            or metadata.num_rows != contract.expected.row_count
            or metadata.num_rows > limits.max_rows
            or metadata.serialized_size > limits.max_footer_bytes
        ):
            _fail(ErrorCode.RESOURCE_LIMIT, "artifact.footer")
        decoded_bytes = sum(
            metadata.row_group(index).total_byte_size for index in range(metadata.num_row_groups)
        )
        if decoded_bytes > limits.max_decoded_bytes:
            _fail(ErrorCode.RESOURCE_LIMIT, "artifact.decoded_bytes")
        _verify_arrow_schema(parquet.schema_arrow, contract)
        batches = list(parquet.iter_batches(batch_size=65_536, use_threads=False))
        table = pa.Table.from_batches(batches, schema=parquet.schema_arrow)
        if table.num_rows != metadata.num_rows:
            _fail(ErrorCode.FILE_CHANGED, "artifact.row_count")
        _admit_arrow(table, contract)
        if _hash_handle(handle) != pin.sha256:
            _fail(ErrorCode.FILE_CHANGED, "artifact.final_sha256")
        _verify_unchanged(path, descriptor, handle)
    return table, FileEvidence(pin.label, pin.relative_path, pin.size_bytes, pin.sha256)


def _load_verified_artifact(
    artifact_root: Path, *, contract: FrozenArtifactContract, contract_sha256: str
) -> VerifiedArtifact:
    _require_ready(contract)
    _verify_runtime(contract)
    if contract.policy_version != policy.POLICY_VERSION or contract.policy_digest != policy.POLICY_DIGEST:
        _fail(ErrorCode.SOURCE_MISMATCH, "policy_identity")
    source_evidence = _verify_sources(contract)
    limits = contract.limits
    assert limits is not None and contract.identity is not None
    documents: dict[str, Any] = {}
    file_evidence: list[FileEvidence] = list(source_evidence)
    manifest_pins = {pin.label: pin for pin in contract.manifests}
    for pin in contract.manifests:
        document, evidence = _read_pinned_json(artifact_root, pin, limits)
        documents[pin.label] = document
        file_evidence.append(evidence)
    _verify_manifests(contract, documents)
    receipt_root = (
        ROOT
        if PurePosixPath(contract.identity.receipt.relative_path).parts[0] == "contracts"
        else artifact_root
    )
    receipt, evidence = _read_pinned_json(receipt_root, contract.identity.receipt, limits)
    file_evidence.append(evidence)
    _verify_identity_receipt(receipt, contract, manifest_pins, documents)
    table, artifact_evidence = _read_parquet(artifact_root, contract)
    file_evidence.append(artifact_evidence)
    frame = _to_canonical_frame(table)
    summary = _validate_frame(frame, contract)
    publication = ImpliedRatingPublication(
        publication_id=contract.identity.publication_id,
        panel_publication_id=contract.parent.publication_id,
        policy_version=contract.policy_version,
        policy_digest=contract.policy_digest,
        code_revision=contract.producer_revision,
        panel_last_closed_month=contract.parent.last_closed_month,
        first_month=contract.expected.first_month,
        last_month=contract.expected.last_month,
        input_fingerprint=contract.identity.input_fingerprint,
        l_anchor=contract.anchor_value,
        rows_digest=contract.expected.rows_digest,
        d_confirmed_count=contract.expected.d_confirmed_count,
        d_candidate_count=contract.expected.d_candidate_count,
        row_count=contract.expected.row_count,
    )
    return VerifiedArtifact(contract_sha256, publication, summary, tuple(file_evidence), table)


def load_verified_artifact(artifact_root: Path) -> VerifiedArtifact:
    raw = _read_contract_bytes()
    return _load_verified_artifact(
        artifact_root,
        contract=_parse_contract(raw),
        contract_sha256=_hash_bytes(raw),
    )


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str,
        allow_nan=False,
    ).encode("utf-8")


def _resource_usage() -> dict[str, int | str] | None:
    if _resource is None:
        return None
    usage = _resource.getrusage(_resource.RUSAGE_SELF)
    multiplier = 1 if sys.platform == "darwin" else 1024
    return {
        "max_rss_bytes": int(usage.ru_maxrss) * multiplier,
        "source": "getrusage_ru_maxrss",
    }


def _parent_projection(row: Sequence[Any]) -> dict[str, Any]:
    return {
        "publication_id": str(row[0]), "product": row[1],
        "parent_publication_id": None if row[2] is None else str(row[2]),
        "publication_status": row[3], "failure_reason": row[4],
        "config_hash": row[5].strip(), "input_fingerprint": row[6].strip(),
        "code_revision": row[7], "first_month": row[8].isoformat(),
        "last_closed_month": row[9].isoformat(),
        "open_month": None if row[10] is None else row[10].isoformat(),
        "counts": [int(row[11]), int(row[12]), int(row[13]), int(row[14])],
        "source_lineage": row[15], "gate_evidence": row[16],
    }


_PARENT_SQL = (
    "SELECT publication_id, product, parent_publication_id, publication_status, failure_reason, "
    "config_hash, input_fingerprint, code_revision, first_month, last_closed_month, open_month, "
    "snapshot_rows, rv_signal_rows, returns_rows, ratings_pit_rows, source_lineage, gate_evidence "
    "FROM bond_panel_publications WHERE publication_id=%s"
)


def _verify_parent(
    conn: psycopg.Connection, *, contract: FrozenArtifactContract, lock_pointer: bool
) -> ParentEvidence:
    lock_clause = " FOR SHARE" if lock_pointer else ""
    pointer = conn.execute(
        "SELECT publication_id, changed_at FROM bond_panel_app_pointer "
        "WHERE product=%s" + lock_clause,
        (PANEL_PRODUCT,),
    ).fetchone()
    if pointer is None or str(pointer[0]) != contract.parent.publication_id:
        _fail(ErrorCode.PARENT_MISMATCH, "parent.pointer")
    child_row = conn.execute(_PARENT_SQL, (contract.parent.publication_id,)).fetchone()
    parent_row = conn.execute(_PARENT_SQL, (contract.parent.parent_publication_id,)).fetchone()
    if child_row is None or parent_row is None:
        _fail(ErrorCode.PARENT_MISMATCH, "parent.header")
    child = _parent_projection(child_row)
    predecessor = _parent_projection(parent_row)
    pins = contract.parent
    expected = {
        "product": PANEL_PRODUCT,
        "parent_publication_id": pins.parent_publication_id,
        "publication_status": "validated",
        "failure_reason": None,
        "config_hash": pins.config_hash,
        "code_revision": pins.code_revision,
        "first_month": pins.first_month.isoformat(),
        "last_closed_month": pins.last_closed_month.isoformat(),
        "open_month": pins.open_month.isoformat(),
        "counts": list(pins.declared_counts),
    }
    for field, value in expected.items():
        if child[field] != value:
            _fail(ErrorCode.PARENT_MISMATCH, f"parent.{field}")
    if predecessor["publication_status"] != "validated":
        _fail(ErrorCode.PARENT_MISMATCH, "parent.predecessor_status")
    source = child["source_lineage"]
    gates = child["gate_evidence"]
    if type(source) is not dict or type(gates) is not dict:
        _fail(ErrorCode.PARENT_MISMATCH, "parent.json")
    if source.get("unit_repair", {}).get("contract") != pins.repair_contract:
        _fail(ErrorCode.PARENT_MISMATCH, "parent.repair_contract")
    if gates.get("unit_repair", {}).get("contract") != pins.repair_contract:
        _fail(ErrorCode.PARENT_MISMATCH, "parent.gate_contract")
    source_shas = source.get("source_sha256")
    if type(source_shas) is not dict or tuple(sorted(source_shas)) != tuple(sorted(pins.source_sha256_keys)):
        _fail(ErrorCode.PARENT_MISMATCH, "parent.source_keys")
    if source_shas.get("bond_panel_live.parquet") != pins.snapshot_source_sha256:
        _fail(ErrorCode.PARENT_MISMATCH, "parent.snapshot_sha")
    digest = _hash_bytes(_canonical_json_bytes({"child": child, "parent": predecessor}))
    if pins.header_sha256 is not None and digest != pins.header_sha256:
        _fail(ErrorCode.PARENT_MISMATCH, "parent.header_sha256")
    server_version = int(conn.execute("SHOW server_version_num").fetchone()[0])
    return ParentEvidence(str(pointer[0]), pointer[1], digest, server_version)


def verify_parent(conn: psycopg.Connection, *, lock_pointer: bool = False) -> ParentEvidence:
    contract = load_frozen_contract()
    _require_ready(contract)
    return _verify_parent(conn, contract=contract, lock_pointer=lock_pointer)


def _set_local_timeouts(conn: psycopg.Connection) -> None:
    server_version = int(conn.execute("SHOW server_version_num").fetchone()[0])
    if not 180_000 <= server_version < 190_000:
        _fail(ErrorCode.RUNTIME_MISMATCH, "postgres_major")
    conn.execute("SELECT set_config('lock_timeout', %s, true)", (str(LOCK_TIMEOUT_MS),))
    conn.execute("SELECT set_config('statement_timeout', %s, true)", (str(SQL_TIMEOUT_MS),))
    conn.execute(
        "SELECT set_config('idle_in_transaction_session_timeout', %s, true)",
        (str(IDLE_TRANSACTION_TIMEOUT_MS),),
    )


def _acquire_product_lock(conn: psycopg.Connection) -> None:
    try:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (PRODUCT,))
    except psycopg.errors.LockNotAvailable as exc:
        raise ArtifactLoaderError(
            ErrorCode.LOCK_TIMEOUT, field="transaction.lock_timeout"
        ) from exc


_SHARED_SCHEMA_OBJECTS = (
    "sec_derived_publications", "sec_derived_current_pointers",
    "sec_derived_publication_tokens", "sec_derived_pointer_tokens",
)
_PRODUCT_SCHEMA_OBJECTS = (
    "bond_market_implied_rating_v1_builds", "bond_market_implied_rating_v1",
    "bond_market_implied_rating_v1_current", "bond_market_implied_rating_publications",
    "bond_market_implied_rating_app_pointer",
)

_EXPECTED_COLUMNS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "sec_derived_publications": (
        ("publication_id", "uuid", "NO"), ("product", "text", "NO"),
        ("publication_version", "int4", "NO"), ("source_run_id", "uuid", "NO"),
        ("source_package_id", "uuid", "NO"), ("build_fingerprint", "bpchar", "NO"),
        ("prepared_at", "timestamptz", "NO"), ("validated_at", "timestamptz", "YES"),
        ("lifecycle_state", "text", "NO"),
    ),
    "sec_derived_current_pointers": (
        ("product", "text", "NO"), ("publication_id", "uuid", "NO"),
        ("set_at", "timestamptz", "NO"),
    ),
    "sec_derived_publication_tokens": (
        ("publication_id", "uuid", "NO"), ("backend_pid", "int4", "NO"),
    ),
    "sec_derived_pointer_tokens": (
        ("product", "text", "NO"), ("backend_pid", "int4", "NO"),
    ),
    "bond_market_implied_rating_v1_builds": (
        ("publication_id", "uuid", "NO"), ("panel_publication_id", "uuid", "NO"),
        ("policy_version", "text", "NO"), ("policy_digest", "bpchar", "NO"),
        ("code_revision", "text", "NO"), ("panel_last_closed_month", "date", "NO"),
        ("as_of_date", "date", "NO"), ("first_month", "date", "NO"),
        ("last_month", "date", "NO"), ("input_fingerprint", "bpchar", "NO"),
        ("l_anchor", "float8", "NO"), ("row_count", "int4", "NO"),
        ("rows_digest", "bpchar", "NO"), ("d_confirmed_count", "int4", "NO"),
        ("d_candidate_count", "int4", "NO"), ("created_at", "timestamptz", "NO"),
    ),
    "bond_market_implied_rating_v1": (
        ("publication_id", "uuid", "NO"), ("month", "date", "NO"),
        ("cusip_id", "text", "NO"), ("implied_bucket", "text", "NO"),
        ("spread_norm_log", "float8", "YES"), ("neutralized_score", "float8", "YES"),
        ("market_level_l", "float8", "YES"), ("witnessed", "bool", "NO"),
        ("carry_months", "int4", "NO"), ("spell_id", "int4", "NO"),
        ("d_candidate", "bool", "NO"), ("d_confirmed", "bool", "NO"),
        ("d_event_month", "date", "YES"), ("recovery_observed", "float8", "YES"),
        ("censoring", "text", "NO"), ("policy_version", "text", "NO"),
        ("policy_digest", "bpchar", "NO"),
    ),
}

_EXPECTED_CONSTRAINT_HASHES = {
    False: "9d69f7f1c5ebf1b77426b7c1857a4c165840fda7760b25151c05630e274eb64a",
    True: "26e024de4030da9600e2ca4704fd5186fe6e8bd385d4056638b76b59603f9689",
}

_EXPECTED_FUNCTION_CONTRACTS: dict[str, tuple[Any, ...]] = {
    "bond_market_implied_rating_v1_write_guard()": (
        "6823804a0b205c5f2b4e38b697b229665e597fdf4883cd53b5ba591022593fb0",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer", None,
    ),
    "sec_derived_pointer_guard()": (
        "0af65cf5d0c4c04762c82c95eb245888ece0db45d5275d6fcf6f5d73f65ae84e",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer", None,
    ),
    "sec_derived_publication_as_of(uuid)": (
        "db638702fda1189bf0d1f196a7a642db33228577a9363bb750d7dc24c9205e94",
        "target_publication_id uuid", "target_publication_id uuid", 0,
        "date", "plpgsql", "s", "u", False, False, False, "f", False,
        None, "worker_writer", None,
    ),
    "sec_derived_publication_delete_guard()": (
        "e87342589b00a20437f3c7bc8ce9760948121c801514c9103d863e8e1c64f958",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer", None,
    ),
    "sec_derived_publication_immutable()": (
        "9263cad4f6b6bf449185d794b0a3c3ddbb04b60144c95beb7e209685cdc40ffd",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer", None,
    ),
    "sec_derived_publication_is_validated(uuid,text)": (
        "07c3fac7bb10c6c2e8a2c3d735553cff06cbfee866d3ea8baa7c71ef16a5d505",
        "target_publication_id uuid, expected_product text",
        "target_publication_id uuid, expected_product text DEFAULT NULL::text", 1,
        "boolean", "sql", "s", "u", False, False, False, "f", False,
        None, "worker_writer", None,
    ),
    "sec_set_current_derived_publication(text,uuid,boolean)": (
        "603e42b96cab1950e0cdecc2b2d33e4a5e495f0824cfd73cee588cc50192a72f",
        "target_product text, target_publication_id uuid, allow_as_of_regression boolean",
        (
            "target_product text, target_publication_id uuid, "
            "allow_as_of_regression boolean DEFAULT false"
        ),
        1,
        "void", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer", None,
    ),
    "sec_validate_derived_publication(uuid)": (
        "01ec9ea04a6160b18c33b5afc1f1d9a7d1361c08986496536cb12689a29b527f",
        "target_publication_id uuid", "target_publication_id uuid", 0,
        "void", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer", None,
    ),
}

_EXPECTED_TRIGGER_CONTRACTS: dict[tuple[str, str], tuple[Any, ...]] = {
    (
        "bond_market_implied_rating_v1_builds_write_guard",
        "bond_market_implied_rating_v1_builds",
    ): (
        "bond_market_implied_rating_v1_write_guard()", 31, "O",
        "6089e331190a9417d0bb64c74cb19f8be3c7576911f935592991b551a514dfd6",
    ),
    (
        "bond_market_implied_rating_v1_rows_write_guard",
        "bond_market_implied_rating_v1",
    ): (
        "bond_market_implied_rating_v1_write_guard()", 31, "O",
        "f36f9ac8e0f76af581835f4221067d01af685e7192480502517c09583f475eae",
    ),
    ("sec_derived_current_pointer_guard", "sec_derived_current_pointers"): (
        "sec_derived_pointer_guard()", 31, "O",
        "0c98c240f333d02ddd1e15cf82a7ac5c0b6cb0ecfc7c4b3615248fe63cf48cd1",
    ),
    ("sec_derived_publications_delete_guard", "sec_derived_publications"): (
        "sec_derived_publication_delete_guard()", 11, "O",
        "9f0e3017e51e2a5da89cc86d7428edd03922760088b569257d337c5a9332ed37",
    ),
    ("sec_derived_publications_immutable", "sec_derived_publications"): (
        "sec_derived_publication_immutable()", 19, "O",
        "b8c1e0ced9ed78690ae79ff8aff6594c5671802676fd6a78db7de4bfec985ac6",
    ),
}

_EXPECTED_VIEW_DEFINITIONS: dict[str, str] = {
    "bond_market_implied_rating_v1_current": (
        "select r.publication_id, r.month, r.cusip_id, r.implied_bucket, r.spread_norm_log, "
        "r.neutralized_score, r.market_level_l, r.witnessed, r.carry_months, r.spell_id, "
        "r.d_candidate, r.d_confirmed, r.d_event_month, r.recovery_observed, r.censoring, "
        "r.policy_version, r.policy_digest from sec_derived_current_pointers pointer join "
        "bond_market_implied_rating_v1 r on r.publication_id = pointer.publication_id where "
        "pointer.product = 'bond_market_implied_rating_v1'::text;"
    ),
    "bond_market_implied_rating_publications": (
        "select s.publication_id, s.product, s.lifecycle_state as publication_status, null::text "
        "as failure_reason, b.policy_version, b.policy_digest, b.code_revision, "
        "b.panel_publication_id, b.panel_last_closed_month, b.first_month, b.last_month, "
        "b.input_fingerprint, b.row_count, b.rows_digest, b.d_confirmed_count, "
        "b.d_candidate_count, s.prepared_at as built_at, s.validated_at from "
        "sec_derived_publications s join bond_market_implied_rating_v1_builds b using "
        "(publication_id) where s.product = 'bond_market_implied_rating_v1'::text;"
    ),
    "bond_market_implied_rating_app_pointer": (
        "select product, publication_id, set_at as changed_at from "
        "sec_derived_current_pointers where product = 'bond_market_implied_rating_v1'::text;"
    ),
}


def _normalized_catalog_sql(value: str) -> str:
    return re.sub(r"\s+", " ", value.lower()).strip()


def _relation_columns(conn: psycopg.Connection, name: str) -> tuple[tuple[str, str, str], ...]:
    rows = conn.execute(
        "SELECT column_name, udt_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema=current_schema() AND table_name=%s ORDER BY ordinal_position",
        (name,),
    ).fetchall()
    return tuple((row[0], row[1], row[2]) for row in rows)


def _verify_relation_contracts(conn: psycopg.Connection, *, include_product: bool) -> None:
    relation_names = list(_SHARED_SCHEMA_OBJECTS)
    if include_product:
        relation_names.extend(_PRODUCT_SCHEMA_OBJECTS)
    kinds = dict(conn.execute(
        "SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s)",
        (relation_names,),
    ).fetchall())
    expected_kinds = {name: "r" for name in _SHARED_SCHEMA_OBJECTS}
    if include_product:
        expected_kinds.update({
            "bond_market_implied_rating_v1_builds": "r",
            "bond_market_implied_rating_v1": "r",
            "bond_market_implied_rating_v1_current": "v",
            "bond_market_implied_rating_publications": "v",
            "bond_market_implied_rating_app_pointer": "v",
        })
    if kinds != expected_kinds:
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.relation_kinds")
    for name in _SHARED_SCHEMA_OBJECTS[:2]:
        if _relation_columns(conn, name) != _EXPECTED_COLUMNS[name]:
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.columns.{name}")
    for name in _SHARED_SCHEMA_OBJECTS[2:]:
        if _relation_columns(conn, name) != _EXPECTED_COLUMNS[name]:
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.columns.{name}")
    if include_product:
        for name in ("bond_market_implied_rating_v1_builds", "bond_market_implied_rating_v1"):
            if _relation_columns(conn, name) != _EXPECTED_COLUMNS[name]:
                _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.columns.{name}")

    constraint_relations = list(_SHARED_SCHEMA_OBJECTS)
    if include_product:
        constraint_relations.extend(
            ("bond_market_implied_rating_v1_builds", "bond_market_implied_rating_v1")
        )
    constraint_rows = conn.execute(
        "SELECT c.relname,k.conname,k.contype,k.condeferrable,k.condeferred,"
        "k.convalidated,pg_get_constraintdef(k.oid,true) FROM pg_constraint k "
        "JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) "
        "ORDER BY c.relname,k.conname",
        (constraint_relations,),
    ).fetchall()
    constraint_hash = _hash_bytes(_canonical_json_bytes(constraint_rows))
    if constraint_hash != _EXPECTED_CONSTRAINT_HASHES[include_product]:
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.constraints")

    trigger_rows = conn.execute(
        "SELECT t.tgname,c.relname,p.oid::regprocedure::text,t.tgtype,t.tgenabled,"
        "pg_get_triggerdef(t.oid,true) FROM pg_trigger t "
        "JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_class c ON c.oid=t.tgrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) AND NOT t.tgisinternal"
        " ORDER BY t.tgname,c.relname",
        (relation_names,),
    ).fetchall()
    observed_triggers = {
        (row[0], row[1]): (
            row[2], int(row[3]), row[4],
            _hash_bytes(row[5].encode("utf-8")),
        )
        for row in trigger_rows
    }
    expected_triggers = {
        key: value
        for key, value in _EXPECTED_TRIGGER_CONTRACTS.items()
        if include_product or key[0].startswith("sec_")
    }
    if observed_triggers != expected_triggers:
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.triggers")

    function_names = sorted({signature.split("(", 1)[0] for signature in _EXPECTED_FUNCTION_CONTRACTS})
    function_rows = conn.execute(
        "SELECT p.oid::regprocedure::text,pg_get_function_identity_arguments(p.oid),"
        "pg_get_function_arguments(p.oid),p.pronargdefaults,"
        "pg_get_function_result(p.oid),l.lanname,p.provolatile,p.proparallel,"
        "p.prosecdef,p.proisstrict,p.proleakproof,p.prokind,p.proretset,p.proconfig,"
        "pg_get_userbyid(p.proowner),p.proacl,p.prosrc "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "JOIN pg_language l ON l.oid=p.prolang "
        "WHERE n.nspname=current_schema() AND p.proname=ANY(%s) "
        "ORDER BY p.oid::regprocedure::text",
        (function_names,),
    ).fetchall()
    observed_functions: dict[str, tuple[Any, ...]] = {}
    for row in function_rows:
        signature = row[0]
        definition_hash = _hash_bytes(row[16].encode("utf-8"))
        proconfig = None if row[13] is None else tuple(row[13])
        proacl = None if row[15] is None else tuple(row[15])
        observed_functions[signature] = (
            definition_hash, *row[1:13], proconfig, row[14], proacl
        )
    expected_functions = {
        signature: value
        for signature, value in _EXPECTED_FUNCTION_CONTRACTS.items()
        if include_product or not signature.startswith("bond_market_")
    }
    if observed_functions != expected_functions:
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.functions")

    if include_product:
        for view_name, expected in _EXPECTED_VIEW_DEFINITIONS.items():
            definition = conn.execute(
                "SELECT pg_get_viewdef(%s::regclass, true)", (view_name,)
            ).fetchone()[0]
            normalized = _normalized_catalog_sql(definition)
            if normalized != expected:
                _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.view.{view_name}")


def _schema_state(conn: psycopg.Connection) -> str:
    shared = [
        conn.execute("SELECT to_regclass(%s)", (name,)).fetchone()[0] is not None
        for name in _SHARED_SCHEMA_OBJECTS
    ]
    product = [
        conn.execute("SELECT to_regclass(%s)", (name,)).fetchone()[0] is not None
        for name in _PRODUCT_SCHEMA_OBJECTS
    ]
    if any(shared) and not all(shared):
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.shared_partial")
    if not any(product):
        if all(shared):
            _verify_relation_contracts(conn, include_product=False)
            return "shared_only"
        return "absent"
    if not all(product) or not all(shared):
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.partial")
    _verify_relation_contracts(conn, include_product=True)
    owners = conn.execute(
        "SELECT c.relname, pg_get_userbyid(c.relowner) FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s)",
        (list(_PRODUCT_SCHEMA_OBJECTS),),
    ).fetchall()
    if len(owners) != len(_PRODUCT_SCHEMA_OBJECTS) or any(
        owner != "worker_writer" for _, owner in owners
    ):
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.owners")
    return "compatible"


def _publication_state(
    conn: psycopg.Connection, publication: ImpliedRatingPublication | str
) -> PublicationState:
    publication_id = (
        publication.publication_id
        if isinstance(publication, ImpliedRatingPublication)
        else publication
    )
    pointer = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
        (PRODUCT,),
    ).fetchone()
    pointer_id = None if pointer is None else str(pointer[0])
    rows = conn.execute(
        "SELECT publication_id, lifecycle_state FROM sec_derived_publications WHERE product=%s",
        (PRODUCT,),
    ).fetchall()
    if pointer_id not in (None, publication_id):
        _fail(ErrorCode.PUBLICATION_CONFLICT, "publication.pointer")
    expected_rows = [row for row in rows if str(row[0]) == publication_id]
    foreign = [row for row in rows if str(row[0]) != publication_id]
    if foreign or len(expected_rows) > 1:
        _fail(ErrorCode.PUBLICATION_CONFLICT, "publication.history")
    if not expected_rows:
        if pointer_id is not None:
            _fail(ErrorCode.PUBLICATION_CONFLICT, "publication.pointer_orphan")
        return PublicationState.ABSENT
    if expected_rows[0][1] != "validated":
        _fail(ErrorCode.PUBLICATION_CONFLICT, "publication.lifecycle")
    if pointer_id == publication_id:
        return PublicationState.EXACT_CURRENT
    return PublicationState.EXACT_VALIDATED_UNPOINTED


def _stored_frame(rows: Sequence[Sequence[Any]]) -> pd.DataFrame:
    floating = {
        policy.PUBLICATION_COLUMNS.index(name): name
        for name in (
            "spread_norm_log", "neutralized_score", "market_level_l", "recovery_observed"
        )
    }
    for row in rows:
        for index, name in floating.items():
            value = row[index]
            if value is not None and not math.isfinite(float(value)):
                _fail(ErrorCode.STORED_MISMATCH, f"stored.nonfinite.{name}")
    frame = pd.DataFrame(rows, columns=policy.PUBLICATION_COLUMNS)
    frame["month"] = pd.to_datetime(frame["month"])
    frame["d_event_month"] = pd.to_datetime(frame["d_event_month"])
    for name in ("spread_norm_log", "neutralized_score", "market_level_l", "recovery_observed"):
        frame[name] = np.asarray(
            [np.nan if value is None else float(value) for value in frame[name]], dtype=np.float64
        )
    for name in ("witnessed", "d_candidate", "d_confirmed"):
        frame[name] = np.asarray(frame[name], dtype=np.bool_)
    for name in ("carry_months", "spell_id"):
        frame[name] = np.asarray(frame[name], dtype=np.int64)
    return frame.loc[:, list(policy.PUBLICATION_COLUMNS)]


def _verify_stored_publication(
    conn: psycopg.Connection,
    publication: ImpliedRatingPublication,
    *,
    contract: FrozenArtifactContract,
    require_current: bool,
) -> StoredEvidence:
    build = conn.execute(
        "SELECT " + ",".join(BUILD_COLUMNS) +
        " FROM bond_market_implied_rating_v1_builds WHERE publication_id=%s",
        (publication.publication_id,),
    ).fetchone()
    if build is None:
        _fail(ErrorCode.STORED_MISMATCH, "stored.build")
    expected_build = (
        publication.publication_id, publication.panel_publication_id,
        publication.policy_version, publication.policy_digest, publication.code_revision,
        publication.panel_last_closed_month, publication.panel_last_closed_month,
        publication.first_month, publication.last_month, publication.input_fingerprint,
        publication.l_anchor, publication.row_count, publication.rows_digest,
        publication.d_confirmed_count, publication.d_candidate_count,
    )
    normalized = tuple(str(value).strip() if index in {0, 1, 3, 9, 12} else value for index, value in enumerate(build))
    expected_normalized = tuple(
        str(value).strip() if index in {0, 1, 3, 9, 12} else value
        for index, value in enumerate(expected_build)
    )
    if normalized != expected_normalized:
        _fail(ErrorCode.STORED_MISMATCH, "stored.build_fields")
    ledger = conn.execute(
        "SELECT publication_id, product, source_run_id, source_package_id, build_fingerprint, "
        "prepared_at, validated_at, lifecycle_state FROM sec_derived_publications "
        "WHERE publication_id=%s",
        (publication.publication_id,),
    ).fetchone()
    if ledger is None or (
        str(ledger[0]) != publication.publication_id
        or ledger[1] != PRODUCT
        or ledger[4].strip() != build_fingerprint(
            publication.policy_digest, publication.code_revision, publication.input_fingerprint
        )
        or ledger[7] != "validated"
        or ledger[6] is None
        or ledger[5] > ledger[6]
    ):
        _fail(ErrorCode.STORED_MISMATCH, "stored.ledger")
    anchor = conn.execute(
        "SELECT 1 FROM sec_validated_raw_runs r "
        "JOIN sec_ingestion_runs i ON i.run_id=r.run_id AND i.raw_validated_at IS NOT NULL "
        "JOIN sec_source_packages p ON p.run_id=r.run_id AND p.package_id=%s WHERE r.run_id=%s",
        (ledger[3], ledger[2]),
    ).fetchone()
    if anchor is None:
        _fail(ErrorCode.STORED_MISMATCH, "stored.anchor")
    pointer = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
        (PRODUCT,),
    ).fetchone()
    pointer_id = None if pointer is None else str(pointer[0])
    if require_current and pointer_id != publication.publication_id:
        _fail(ErrorCode.STORED_MISMATCH, "stored.pointer")
    serving_header = conn.execute(
        "SELECT publication_id, product, publication_status, failure_reason, policy_version, "
        "policy_digest, code_revision, panel_publication_id, panel_last_closed_month, "
        "first_month, last_month, input_fingerprint, row_count, rows_digest, "
        "d_confirmed_count, d_candidate_count FROM bond_market_implied_rating_publications "
        "WHERE publication_id=%s",
        (publication.publication_id,),
    ).fetchone()
    expected_header = (
        publication.publication_id, PRODUCT, "validated", None,
        publication.policy_version, publication.policy_digest, publication.code_revision,
        publication.panel_publication_id, publication.panel_last_closed_month,
        publication.first_month, publication.last_month, publication.input_fingerprint,
        publication.row_count, publication.rows_digest, publication.d_confirmed_count,
        publication.d_candidate_count,
    )
    normalized_header = None if serving_header is None else tuple(
        str(value).strip() if index in {0, 5, 7, 11, 13} else value
        for index, value in enumerate(serving_header)
    )
    if normalized_header != expected_header:
        _fail(ErrorCode.STORED_MISMATCH, "stored.publications_view")
    serving_pointer = conn.execute(
        "SELECT publication_id FROM bond_market_implied_rating_app_pointer WHERE product=%s",
        (PRODUCT,),
    ).fetchone()
    serving_pointer_id = None if serving_pointer is None else str(serving_pointer[0])
    if serving_pointer_id != pointer_id:
        _fail(ErrorCode.STORED_MISMATCH, "stored.pointer_view")
    rows: list[Sequence[Any]] = []
    columns = ",".join(policy.PUBLICATION_COLUMNS)
    with conn.cursor(name=f"artifact_readback_{uuid4().hex}") as cursor:
        cursor.execute(
            f"SELECT {columns} FROM bond_market_implied_rating_v1 "
            "WHERE publication_id=%s ORDER BY month,cusip_id",
            (publication.publication_id,),
        )
        while batch := cursor.fetchmany(ROW_BATCH):
            rows.extend(batch)
            if len(rows) > contract.expected.row_count:
                _fail(ErrorCode.STORED_MISMATCH, "stored.row_bound")
    frame = _stored_frame(rows)
    summary = _validate_frame(frame, contract)
    current = conn.execute(
        "SELECT count(*), min(month), max(month), "
        "count(*) FILTER (WHERE d_confirmed), count(*) FILTER (WHERE d_candidate) "
        "FROM bond_market_implied_rating_v1_current"
    ).fetchone()
    expected_current = (
        (summary.row_count, summary.first_month, summary.last_month,
         summary.d_confirmed_count, summary.d_candidate_count)
        if pointer_id == publication.publication_id
        else (0, None, None, 0, 0)
    )
    if tuple(current) != expected_current:
        _fail(ErrorCode.STORED_MISMATCH, "stored.current_view")
    state = (
        PublicationState.EXACT_CURRENT
        if pointer_id == publication.publication_id
        else PublicationState.EXACT_VALIDATED_UNPOINTED
    )
    return StoredEvidence(
        state=state,
        publication_id=publication.publication_id,
        source_run_id=str(ledger[2]),
        source_package_id=str(ledger[3]),
        summary=summary,
        pointer_id=pointer_id,
        prepared_at=ledger[5],
        validated_at=ledger[6],
    )


def verify_stored_publication(
    conn: psycopg.Connection,
    publication: ImpliedRatingPublication,
    *,
    require_current: bool,
) -> StoredEvidence:
    contract = load_frozen_contract()
    _require_ready(contract)
    return _verify_stored_publication(
        conn, publication, contract=contract, require_current=require_current
    )


def _receipt_payload(
    *,
    phase: str,
    artifact: VerifiedArtifact,
    outcome: str,
    stored: StoredEvidence | None,
    operation_id: str | None = None,
    contract: FrozenArtifactContract | None = None,
    release: ReleaseEvidence | None = None,
    parent: ParentEvidence | None = None,
    schema_installed: bool | None = None,
    operation_started_at_utc: str | None = None,
) -> bytes:
    recorded_at = datetime.now(timezone.utc)
    elapsed_seconds = None
    if operation_started_at_utc is not None:
        elapsed_seconds = (
            recorded_at - datetime.fromisoformat(operation_started_at_utc)
        ).total_seconds()
    return _canonical_json_bytes({
        "schema_version": RECEIPT_SCHEMA,
        "phase": phase,
        "operation_id": operation_id or str(uuid4()),
        "operation_started_at_utc": operation_started_at_utc,
        "recorded_at_utc": recorded_at.isoformat(),
        "elapsed_seconds": elapsed_seconds,
        "outcome": outcome,
        "contract_sha256": artifact.contract_sha256,
        "publication_id": artifact.publication.publication_id,
        "artifact_sha256": next(
            item.sha256 for item in artifact.files if item.label == "artifact"
        ),
        "row_count": artifact.summary.row_count,
        "rows_digest": artifact.summary.rows_digest,
        "policy_version": artifact.publication.policy_version,
        "policy_digest": artifact.publication.policy_digest,
        "producer_revision": artifact.publication.code_revision,
        "input_fingerprint": artifact.publication.input_fingerprint,
        "panel_publication_id": artifact.publication.panel_publication_id,
        "first_month": artifact.publication.first_month,
        "last_month": artifact.publication.last_month,
        "l_anchor": artifact.publication.l_anchor,
        "files": [
            {
                "label": item.label,
                "relative_path": item.relative_path,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
            }
            for item in artifact.files
        ],
        "runtime": None if contract is None else {
            "python_minor": contract.runtime.python_minor,
            "distributions": dict(contract.runtime.distributions),
            "base_image": contract.runtime.base_image,
            "connect_timeout_seconds": contract.runtime.connect_timeout_seconds,
            "lock_timeout_ms": contract.runtime.lock_timeout_ms,
            "statement_timeout_ms": contract.runtime.statement_timeout_ms,
            "idle_transaction_timeout_ms": contract.runtime.idle_transaction_timeout_ms,
            "supervised_process_cap_seconds": contract.runtime.supervised_process_cap_seconds,
            "timeout_decision": contract.runtime.timeout_decision,
        },
        "limits": None if contract is None or contract.limits is None else {
            "max_manifest_bytes": contract.limits.max_manifest_bytes,
            "max_row_groups": contract.limits.max_row_groups,
            "max_rows": contract.limits.max_rows,
            "max_decoded_bytes": contract.limits.max_decoded_bytes,
            "max_footer_bytes": contract.limits.max_footer_bytes,
            "max_string_bytes": contract.limits.max_string_bytes,
        },
        "release": None if release is None else {
            "loader_commit": release.loader_commit,
            "loader_tree": release.loader_tree,
            "review_changeset_sha256": release.review_changeset_sha256,
            "review_report_sha256": release.review_report_sha256,
            "context_archive_sha256": release.context_archive_sha256,
            "inventory_sha256": release.inventory_sha256,
            "exclusion_evidence_sha256": release.exclusion_evidence_sha256,
            "requirements_lock_sha256": release.requirements_lock_sha256,
            "dockerfile_sha256": release.dockerfile_sha256,
            "railway_toml_sha256": release.railway_toml_sha256,
            "target": release.target,
            "release_context_sha256": release.release_context_sha256,
        },
        "parent": None if parent is None else {
            "pointer_id": parent.pointer_id,
            "pointer_changed_at": parent.pointer_changed_at,
            "header_sha256": parent.header_sha256,
            "server_version_num": parent.server_version_num,
        },
        "schema_installed": schema_installed,
        "resource_usage": _resource_usage(),
        "stored_source_run_id": None if stored is None else stored.source_run_id,
        "stored_source_package_id": None if stored is None else stored.source_package_id,
        "stored_pointer_id": None if stored is None else stored.pointer_id,
        "stored_prepared_at": None if stored is None else stored.prepared_at,
        "stored_validated_at": None if stored is None else stored.validated_at,
    })


_RELEASE_SOURCE_PATHS = (
    "src/db.py",
    "src/bonds/errors.py",
    "src/bonds/implied_rating.py",
    "src/bonds/implied_rating_materializer.py",
    "src/bonds/implied_rating_artifact_loader.py",
    "scripts/load_bond_market_implied_rating_artifact.py",
    "schemas/sec_derived_publications.sql",
    "schemas/bond_market_implied_rating_v1.sql",
    "contracts/bond_market_implied_rating_round002_artifact.json",
)


def _load_release_evidence(
    evidence_dir: Path, contract: FrozenArtifactContract
) -> ReleaseEvidence:
    try:
        with _open_regular(evidence_dir, "release-context.json") as (handle, path, descriptor):
            if descriptor.st_size <= 0 or descriptor.st_size > 65_536:
                _fail(ErrorCode.RECEIPT_FAILURE, "release_context.size")
            raw = handle.read(65_537)
            _verify_unchanged(path, descriptor, handle)
    except ArtifactLoaderError as exc:
        if exc.code == ErrorCode.RECEIPT_FAILURE.value:
            raise
        raise ArtifactLoaderError(
            ErrorCode.RECEIPT_FAILURE, field="release_context.open"
        ) from exc
    document = _closed_object(
        _strict_json(raw, label="release_context"),
        {
            "schema_version", "target", "loader_commit", "loader_tree",
            "review_changeset_sha256", "review_report_sha256", "context_archive_sha256",
            "inventory_sha256", "exclusion_evidence_sha256", "runtime_base_image",
            "requirements_lock_sha256", "dockerfile_sha256", "railway_toml_sha256",
            "source_sha256",
        },
        "release_context",
    )
    if document["schema_version"] != "bond_market_implied_rating_artifact_release/1":
        _fail(ErrorCode.RECEIPT_FAILURE, "release_context.schema_version")
    if document["target"] != "production":
        _fail(ErrorCode.RECEIPT_FAILURE, "release_context.target")
    if document["runtime_base_image"] != contract.runtime.base_image:
        _fail(ErrorCode.RECEIPT_FAILURE, "release_context.base_image")
    for field in ("loader_commit", "loader_tree"):
        value = _string(document[field], f"release_context.{field}")
        if re.fullmatch(r"[0-9a-f]{40}", value) is None:
            _fail(ErrorCode.RECEIPT_FAILURE, f"release_context.{field}")
    source_sha = document["source_sha256"]
    if type(source_sha) is not dict or tuple(sorted(source_sha)) != tuple(sorted(_RELEASE_SOURCE_PATHS)):
        _fail(ErrorCode.RECEIPT_FAILURE, "release_context.source_inventory")
    for relative_path in _RELEASE_SOURCE_PATHS:
        expected = _sha(source_sha[relative_path], f"release_context.source.{relative_path}")
        try:
            actual = _hash_bytes((ROOT / PurePosixPath(relative_path)).read_bytes())
        except OSError as exc:
            raise ArtifactLoaderError(
                ErrorCode.RECEIPT_FAILURE,
                field=f"release_context.source.{relative_path}",
            ) from exc
        if actual != expected:
            _fail(ErrorCode.RECEIPT_FAILURE, f"release_context.source.{relative_path}")
    runtime_root = ROOT / "docker" / "bond-implied-artifact-loader"
    runtime_checks = {
        "requirements_lock_sha256": runtime_root / "requirements.lock",
        "dockerfile_sha256": runtime_root / "Dockerfile",
        "railway_toml_sha256": runtime_root / "railway.toml",
    }
    for field, file_path in runtime_checks.items():
        try:
            actual = _hash_bytes(file_path.read_bytes())
        except OSError as exc:
            raise ArtifactLoaderError(
                ErrorCode.RECEIPT_FAILURE, field=f"release_context.{field}"
            ) from exc
        if _sha(document[field], f"release_context.{field}") != actual:
            _fail(ErrorCode.RECEIPT_FAILURE, f"release_context.{field}")
    return ReleaseEvidence(
        loader_commit=document["loader_commit"],
        loader_tree=document["loader_tree"],
        review_changeset_sha256=_sha(
            document["review_changeset_sha256"], "release_context.review_changeset_sha256"
        ),
        review_report_sha256=_sha(
            document["review_report_sha256"], "release_context.review_report_sha256"
        ),
        context_archive_sha256=_sha(
            document["context_archive_sha256"], "release_context.context_archive_sha256"
        ),
        inventory_sha256=_sha(
            document["inventory_sha256"], "release_context.inventory_sha256"
        ),
        exclusion_evidence_sha256=_sha(
            document["exclusion_evidence_sha256"],
            "release_context.exclusion_evidence_sha256",
        ),
        requirements_lock_sha256=_sha(
            document["requirements_lock_sha256"],
            "release_context.requirements_lock_sha256",
        ),
        dockerfile_sha256=_sha(
            document["dockerfile_sha256"], "release_context.dockerfile_sha256"
        ),
        railway_toml_sha256=_sha(
            document["railway_toml_sha256"], "release_context.railway_toml_sha256"
        ),
        target="production",
        release_context_sha256=_hash_bytes(raw),
    )


def _persist_receipt(evidence_dir: Path, *, phase: str, payload: bytes) -> ReceiptRef:
    try:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        if evidence_dir.is_symlink() or not evidence_dir.is_dir():
            _fail(ErrorCode.RECEIPT_FAILURE, "receipt.directory")
        basename = (
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
            f"-{phase}-{uuid4()}.json"
        )
        path = evidence_dir / basename
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        readback = path.read_bytes()
    except ArtifactLoaderError:
        raise
    except OSError as exc:
        raise ArtifactLoaderError(ErrorCode.RECEIPT_FAILURE, field="receipt.write") from exc
    if readback != payload:
        _fail(ErrorCode.RECEIPT_FAILURE, "receipt.readback")
    return ReceiptRef(phase, basename, _hash_bytes(payload), len(payload))


def persist_failure_receipt(
    evidence_dir: Path,
    *,
    operation_id: str,
    operation_started_at_utc: str,
    mode: str,
    code: str,
    publication_id: str | None,
    failure_phase: str = "unknown",
    schema_installed: bool | None = None,
    transaction_outcome: str | None = None,
    outcome: str = "failed",
) -> ReceiptRef:
    recorded_at = datetime.now(timezone.utc)
    payload = _canonical_json_bytes({
        "schema_version": RECEIPT_SCHEMA,
        "phase": "failure",
        "operation_id": operation_id,
        "operation_started_at_utc": operation_started_at_utc,
        "recorded_at_utc": recorded_at.isoformat(),
        "elapsed_seconds": (
            recorded_at - datetime.fromisoformat(operation_started_at_utc)
        ).total_seconds(),
        "mode": mode,
        "outcome": outcome,
        "code": code,
        "failure_phase": failure_phase,
        "publication_id": publication_id,
        "schema_installed": schema_installed,
        "transaction_outcome": transaction_outcome,
        "resource_usage": _resource_usage(),
    })
    return _persist_receipt(evidence_dir, phase="failure", payload=payload)


def _best_effort_failure_receipt(
    error: ArtifactLoaderError,
    evidence_dir: Path,
    *,
    operation_id: str,
    operation_started_at_utc: str,
    mode: str,
    publication_id: str | None,
) -> ArtifactLoaderError:
    try:
        persist_failure_receipt(
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode=mode,
            code=error.code,
            publication_id=publication_id,
            failure_phase=error.phase or "unknown",
            schema_installed=error.schema_installed,
            transaction_outcome=error.transaction_outcome,
            outcome=error.outcome or "failed",
        )
    except (ArtifactLoaderError, OSError):
        error.receipt_written = False
    else:
        error.receipt_written = True
    return error


def _artifact_payload(
    artifact: VerifiedArtifact, contract: FrozenArtifactContract
) -> list[tuple[Any, ...]]:
    frame = _to_canonical_frame(artifact.table)
    _validate_frame(frame, contract)
    payload: list[tuple[Any, ...]] = []
    for start in range(0, len(frame), ROW_BATCH):
        payload.extend(publication_row_tuples(
            artifact.publication, frame.iloc[start:start + ROW_BATCH]
        ))
    return payload


def _contract_from_artifact(artifact: VerifiedArtifact) -> FrozenArtifactContract:
    raw = _read_contract_bytes()
    contract = _parse_contract(raw)
    if _hash_bytes(raw) != artifact.contract_sha256:
        _fail(ErrorCode.CONTRACT_INVALID, "contract.changed")
    return contract


def _read_only_operation(
    artifact: VerifiedArtifact,
    *,
    contract: FrozenArtifactContract,
    connection_factory: Callable[[], psycopg.Connection],
    require_published: bool,
) -> OperationResult:
    with connection_factory() as conn, conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY")
        _set_local_timeouts(conn)
        _acquire_product_lock(conn)
        schema = _schema_state(conn)
        first_parent = _verify_parent(conn, contract=contract, lock_pointer=False)
        if schema in {"absent", "shared_only"}:
            if (
                schema == "shared_only"
                and _publication_state(conn, artifact.publication) is not PublicationState.ABSENT
            ):
                _fail(ErrorCode.PUBLICATION_CONFLICT, "publication.partial_state")
            if require_published:
                return OperationResult(
                    "not_published", artifact.publication.publication_id,
                    False, None, first_parent, (),
                )
            return OperationResult(
                "dry_run_verified_schema_install_required", artifact.publication.publication_id,
                False, None, first_parent, (),
            )
        state = _publication_state(conn, artifact.publication)
        stored = None
        if state is PublicationState.EXACT_CURRENT:
            stored = _verify_stored_publication(
                conn, artifact.publication, contract=contract, require_current=True
            )
            outcome = "already_published_verified"
        elif state is PublicationState.EXACT_VALIDATED_UNPOINTED:
            stored = _verify_stored_publication(
                conn, artifact.publication, contract=contract, require_current=False
            )
            outcome = "validated_not_current" if require_published else "dry_run_replay_verified"
        else:
            outcome = "not_published" if require_published else "dry_run_verified"
        second_parent = _verify_parent(conn, contract=contract, lock_pointer=False)
        if first_parent != second_parent:
            _fail(ErrorCode.PARENT_MISMATCH, "parent.race")
        return OperationResult(
            outcome, artifact.publication.publication_id, False, stored, first_parent, ()
        )


def _ensure_schema(
    contract: FrozenArtifactContract,
    connection_factory: Callable[[], psycopg.Connection],
) -> bool:
    with connection_factory() as conn, conn.transaction():
        _set_local_timeouts(conn)
        _acquire_product_lock(conn)
        _verify_parent(conn, contract=contract, lock_pointer=True)
        state = _schema_state(conn)
        if state == "compatible":
            return False
        if state == "shared_only":
            assert contract.identity is not None
            if (
                _publication_state(conn, contract.identity.publication_id)
                is not PublicationState.ABSENT
            ):
                _fail(ErrorCode.PUBLICATION_CONFLICT, "publication.partial_state")
        install_schema(conn)
        if _schema_state(conn) != "compatible":
            _fail(ErrorCode.SCHEMA_MISMATCH, "schema.install")
        return True


def _publish_verified_artifact(
    artifact: VerifiedArtifact,
    *,
    contract: FrozenArtifactContract,
    connection_factory: Callable[[], psycopg.Connection],
    evidence_dir: Path,
    release: ReleaseEvidence | None = None,
    operation_id: str | None = None,
    operation_started_at_utc: str | None = None,
) -> OperationResult:
    _require_ready(contract)
    operation_id = operation_id or str(uuid4())
    operation_started_at_utc = (
        operation_started_at_utc or datetime.now(timezone.utc).isoformat()
    )
    preapply = _persist_receipt(
        evidence_dir,
        phase="pre-apply",
        payload=_receipt_payload(
            phase="pre-apply",
            artifact=artifact,
            outcome="verified",
            stored=None,
            operation_id=operation_id,
            contract=contract,
            release=release,
            schema_installed=False,
            operation_started_at_utc=operation_started_at_utc,
        ),
    )
    schema_installed = False
    try:
        schema_installed = _ensure_schema(contract, connection_factory)
    except ArtifactLoaderError as exc:
        exc.phase = "schema_install"
        exc.schema_installed = None
        exc.transaction_outcome = "not_started"
        if not exc.receipt_written:
            _best_effort_failure_receipt(
                exc,
                evidence_dir,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
                mode="apply",
                publication_id=artifact.publication.publication_id,
            )
        raise
    except psycopg.Error as exc:
        primary = ArtifactLoaderError(
            ErrorCode.DB_FAILURE,
            field="schema.install",
            phase="schema_install",
            schema_installed=None,
            transaction_outcome="not_started",
        )
        _best_effort_failure_receipt(
            primary,
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode="apply",
            publication_id=artifact.publication.publication_id,
        )
        raise primary from exc
    try:
        payload = _artifact_payload(artifact, contract)
    except ArtifactLoaderError as exc:
        exc.phase = "payload"
        exc.schema_installed = schema_installed
        exc.transaction_outcome = "not_started"
        if not exc.receipt_written:
            _best_effort_failure_receipt(
                exc,
                evidence_dir,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
                mode="apply",
                publication_id=artifact.publication.publication_id,
            )
        raise
    precommit: ReceiptRef | None = None
    stored: StoredEvidence | None = None
    parent_evidence: ParentEvidence | None = None
    publish_outcome = "published_verified"
    commit_ready = False
    transaction_phase = "transaction_setup"
    try:
        with connection_factory() as conn:
            if conn.info.transaction_status is not TransactionStatus.IDLE:
                _fail(ErrorCode.DB_FAILURE, "transaction.not_idle")
            try:
                with conn.transaction():
                    _set_local_timeouts(conn)
                    _acquire_product_lock(conn)
                    transaction_phase = "parent_preflight"
                    parent_evidence = _verify_parent(
                        conn, contract=contract, lock_pointer=True
                    )
                    transaction_phase = "schema_recheck"
                    if _schema_state(conn) != "compatible":
                        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.changed")
                    transaction_phase = "publication_state"
                    state = _publication_state(conn, artifact.publication)
                    if state is PublicationState.EXACT_CURRENT:
                        publish_outcome = "already_published_verified"
                        payload.clear()
                        transaction_phase = "stored_precommit"
                        stored = _verify_stored_publication(
                            conn, artifact.publication, contract=contract, require_current=True
                        )
                    else:
                        if state is PublicationState.EXACT_VALIDATED_UNPOINTED:
                            transaction_phase = "replay_precheck"
                            _verify_stored_publication(
                                conn, artifact.publication, contract=contract, require_current=False
                            )
                            publish_outcome = "replay_promoted_verified"
                        transaction_phase = "materialize"
                        materialize(
                            conn, artifact.publication, payload, expected_pointer=None
                        )
                        payload.clear()
                        transaction_phase = "stored_precommit"
                        stored = _verify_stored_publication(
                            conn, artifact.publication, contract=contract, require_current=True
                        )
                    transaction_phase = "parent_recheck"
                    second_parent = _verify_parent(
                        conn, contract=contract, lock_pointer=True
                    )
                    if parent_evidence != second_parent:
                        _fail(ErrorCode.PARENT_MISMATCH, "parent.race")
                    transaction_phase = "precommit_receipt"
                    precommit = _persist_receipt(
                        evidence_dir,
                        phase="precommit",
                        payload=_receipt_payload(
                            phase="precommit", artifact=artifact,
                            outcome="not_committed", stored=stored,
                            operation_id=operation_id,
                            contract=contract,
                            release=release,
                            parent=parent_evidence,
                            schema_installed=schema_installed,
                            operation_started_at_utc=operation_started_at_utc,
                        ),
                    )
                    commit_ready = True
                    transaction_phase = "commit"
            except psycopg.errors.LockNotAvailable as exc:
                primary = ArtifactLoaderError(
                    ErrorCode.LOCK_TIMEOUT,
                    field="transaction.lock_timeout",
                    phase="transaction_lock",
                    schema_installed=schema_installed,
                    transaction_outcome="not_committed",
                )
                _best_effort_failure_receipt(
                    primary,
                    evidence_dir,
                    operation_id=operation_id,
                    operation_started_at_utc=operation_started_at_utc,
                    mode="apply",
                    publication_id=artifact.publication.publication_id,
                )
                raise primary from exc
            except psycopg.OperationalError as exc:
                code = ErrorCode.COMMIT_UNKNOWN if commit_ready else ErrorCode.DB_FAILURE
                field = "transaction.commit" if commit_ready else "transaction.operation"
                primary = ArtifactLoaderError(
                    code,
                    field=field,
                    phase="transaction_commit" if commit_ready else transaction_phase,
                    schema_installed=schema_installed,
                    transaction_outcome="unknown" if commit_ready else "not_committed",
                    outcome="recovery_required" if commit_ready else "failed",
                )
                _best_effort_failure_receipt(
                    primary,
                    evidence_dir,
                    operation_id=operation_id,
                    operation_started_at_utc=operation_started_at_utc,
                    mode="apply",
                    publication_id=artifact.publication.publication_id,
                )
                raise primary from exc
            except psycopg.Error as exc:
                primary = ArtifactLoaderError(
                    ErrorCode.DB_FAILURE,
                    field="transaction.database",
                    phase=transaction_phase,
                    schema_installed=schema_installed,
                    transaction_outcome="not_committed",
                )
                _best_effort_failure_receipt(
                    primary,
                    evidence_dir,
                    operation_id=operation_id,
                    operation_started_at_utc=operation_started_at_utc,
                    mode="apply",
                    publication_id=artifact.publication.publication_id,
                )
                raise primary from exc
            except ArtifactLoaderError as exc:
                exc.phase = exc.phase or transaction_phase
                exc.schema_installed = schema_installed
                exc.transaction_outcome = exc.transaction_outcome or "not_committed"
                if not exc.receipt_written:
                    _best_effort_failure_receipt(
                        exc,
                        evidence_dir,
                        operation_id=operation_id,
                        operation_started_at_utc=operation_started_at_utc,
                        mode="apply",
                        publication_id=artifact.publication.publication_id,
                    )
                raise
            except BondError as exc:
                primary = ArtifactLoaderError(
                    ErrorCode.MATERIALIZER_REFUSAL,
                    field=exc.code,
                    phase="materialize",
                    schema_installed=schema_installed,
                    transaction_outcome="not_committed",
                )
                _best_effort_failure_receipt(
                    primary,
                    evidence_dir,
                    operation_id=operation_id,
                    operation_started_at_utc=operation_started_at_utc,
                    mode="apply",
                    publication_id=artifact.publication.publication_id,
                )
                raise primary from exc
    except psycopg.Error as exc:
        code = ErrorCode.COMMIT_UNKNOWN if commit_ready else ErrorCode.DB_FAILURE
        primary = ArtifactLoaderError(
            code,
            field="transaction.connection_exit",
            phase="connection_exit",
            schema_installed=schema_installed,
            transaction_outcome="unknown" if commit_ready else "not_committed",
            outcome="recovery_required" if commit_ready else "failed",
        )
        _best_effort_failure_receipt(
            primary,
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode="apply",
            publication_id=artifact.publication.publication_id,
        )
        raise primary from exc
    finally:
        payload.clear()
    try:
        result = _read_only_operation(
            artifact,
            contract=contract,
            connection_factory=connection_factory,
            require_published=True,
        )
        if result.outcome != "already_published_verified" or result.stored is None:
            _fail(ErrorCode.STORED_MISMATCH, "readback.outcome")
    except (ArtifactLoaderError, psycopg.Error) as exc:
        primary = ArtifactLoaderError(
            ErrorCode.COMMIT_UNKNOWN,
            field="transaction.postcommit_readback",
            phase="postcommit_readback",
            schema_installed=schema_installed,
            transaction_outcome="committed",
            outcome="recovery_required",
        )
        _best_effort_failure_receipt(
            primary,
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode="apply",
            publication_id=artifact.publication.publication_id,
        )
        raise primary from exc
    try:
        readback = _persist_receipt(
            evidence_dir,
            phase="readback",
            payload=_receipt_payload(
                phase="readback", artifact=artifact,
                outcome=publish_outcome,
                stored=result.stored,
                operation_id=operation_id,
                contract=contract,
                release=release,
                parent=result.parent,
                schema_installed=schema_installed,
                operation_started_at_utc=operation_started_at_utc,
            ),
        )
    except ArtifactLoaderError as exc:
        primary = ArtifactLoaderError(
            ErrorCode.COMMITTED_EVIDENCE_INCOMPLETE,
            field="receipt.final_readback",
            phase="final_receipt",
            schema_installed=schema_installed,
            transaction_outcome="committed",
            outcome="recovery_required",
        )
        _best_effort_failure_receipt(
            primary,
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode="apply",
            publication_id=artifact.publication.publication_id,
        )
        raise primary from exc
    assert precommit is not None
    return OperationResult(
        publish_outcome,
        artifact.publication.publication_id,
        schema_installed,
        result.stored,
        result.parent,
        (preapply, precommit, readback),
    )


def publish_verified_artifact(
    artifact: VerifiedArtifact,
    *,
    evidence_dir: Path,
    connection_factory: Callable[[], psycopg.Connection],
    operation_id: str | None = None,
    operation_started_at_utc: str | None = None,
) -> OperationResult:
    operation_id = operation_id or str(uuid4())
    operation_started_at_utc = (
        operation_started_at_utc or datetime.now(timezone.utc).isoformat()
    )
    phase = "contract"
    try:
        contract = _contract_from_artifact(artifact)
        phase = "release_context"
        release = _load_release_evidence(evidence_dir, contract)
        phase = "publish"
        return _publish_verified_artifact(
            artifact,
            contract=contract,
            connection_factory=connection_factory,
            evidence_dir=evidence_dir,
            release=release,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
        )
    except ArtifactLoaderError as exc:
        if not exc.receipt_written:
            exc.phase = exc.phase or phase
            exc.transaction_outcome = exc.transaction_outcome or "not_started"
            _best_effort_failure_receipt(
                exc,
                evidence_dir,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
                mode="apply",
                publication_id=artifact.publication.publication_id,
            )
        raise
    except BondError as exc:
        primary = ArtifactLoaderError(
            ErrorCode.MATERIALIZER_REFUSAL,
            field=exc.code,
            phase=phase,
            transaction_outcome="not_started",
        )
        _best_effort_failure_receipt(
            primary,
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode="apply",
            publication_id=artifact.publication.publication_id,
        )
        raise primary from exc
    except psycopg.Error as exc:
        primary = ArtifactLoaderError(
            ErrorCode.DB_FAILURE,
            field="transaction.database",
            phase=phase,
            transaction_outcome="not_started",
        )
        _best_effort_failure_receipt(
            primary,
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode="apply",
            publication_id=artifact.publication.publication_id,
        )
        raise primary from exc


def dry_run_verified_artifact(
    artifact: VerifiedArtifact,
    *,
    evidence_dir: Path,
    connection_factory: Callable[[], psycopg.Connection],
    operation_id: str | None = None,
    operation_started_at_utc: str | None = None,
) -> OperationResult:
    contract = _contract_from_artifact(artifact)
    operation_id = operation_id or str(uuid4())
    operation_started_at_utc = (
        operation_started_at_utc or datetime.now(timezone.utc).isoformat()
    )
    try:
        result = _read_only_operation(
            artifact,
            contract=contract,
            connection_factory=connection_factory,
            require_published=False,
        )
    except ArtifactLoaderError as exc:
        schema_installed = (
            exc.schema_installed if exc.schema_installed is not None else False
        )
        transaction_outcome = exc.transaction_outcome or "read_only"
        exc.phase = exc.phase or "dry_run_read"
        exc.schema_installed = schema_installed
        exc.transaction_outcome = transaction_outcome
        if not exc.receipt_written:
            _best_effort_failure_receipt(
                exc,
                evidence_dir,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
                mode="dry-run",
                publication_id=artifact.publication.publication_id,
            )
        raise
    except psycopg.Error as exc:
        primary = ArtifactLoaderError(
            ErrorCode.DB_FAILURE,
            field="transaction.database",
            phase="dry_run_read",
            schema_installed=False,
            transaction_outcome="read_only",
        )
        _best_effort_failure_receipt(
            primary,
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode="dry-run",
            publication_id=artifact.publication.publication_id,
        )
        raise primary from exc
    receipt = _persist_receipt(
        evidence_dir,
        phase="dry-run",
        payload=_receipt_payload(
            phase="dry-run", artifact=artifact, outcome=result.outcome,
            stored=result.stored, operation_id=operation_id, contract=contract,
            parent=result.parent, schema_installed=False,
            operation_started_at_utc=operation_started_at_utc,
        ),
    )
    return OperationResult(
        result.outcome, result.publication_id, False, result.stored, result.parent, (receipt,)
    )


def recover_published_artifact(
    artifact: VerifiedArtifact,
    *,
    evidence_dir: Path,
    connection_factory: Callable[[], psycopg.Connection],
    operation_id: str | None = None,
    operation_started_at_utc: str | None = None,
) -> OperationResult:
    contract = _contract_from_artifact(artifact)
    operation_id = operation_id or str(uuid4())
    operation_started_at_utc = (
        operation_started_at_utc or datetime.now(timezone.utc).isoformat()
    )
    try:
        result = _read_only_operation(
            artifact,
            contract=contract,
            connection_factory=connection_factory,
            require_published=True,
        )
    except ArtifactLoaderError as exc:
        schema_installed = (
            exc.schema_installed if exc.schema_installed is not None else False
        )
        transaction_outcome = exc.transaction_outcome or "read_only"
        exc.phase = exc.phase or "verify_published_read"
        exc.schema_installed = schema_installed
        exc.transaction_outcome = transaction_outcome
        if not exc.receipt_written:
            _best_effort_failure_receipt(
                exc,
                evidence_dir,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
                mode="verify-published",
                publication_id=artifact.publication.publication_id,
            )
        raise
    except psycopg.Error as exc:
        primary = ArtifactLoaderError(
            ErrorCode.DB_FAILURE,
            field="transaction.database",
            phase="verify_published_read",
            schema_installed=False,
            transaction_outcome="read_only",
        )
        _best_effort_failure_receipt(
            primary,
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode="verify-published",
            publication_id=artifact.publication.publication_id,
        )
        raise primary from exc
    receipt = _persist_receipt(
        evidence_dir,
        phase="recovery-readback",
        payload=_receipt_payload(
            phase="recovery-readback", artifact=artifact, outcome=result.outcome,
            stored=result.stored, operation_id=operation_id, contract=contract,
            parent=result.parent, schema_installed=False,
            operation_started_at_utc=operation_started_at_utc,
        ),
    )
    return OperationResult(
        result.outcome, result.publication_id, False, result.stored, result.parent, (receipt,)
    )


__all__ = [
    "ArtifactLoaderError", "ErrorCode", "FrozenArtifactContract", "OperationResult",
    "VerifiedArtifact", "dry_run_verified_artifact", "load_frozen_contract",
    "load_verified_artifact", "persist_failure_receipt", "publish_verified_artifact",
    "recover_published_artifact",
    "verify_parent", "verify_stored_publication",
]
