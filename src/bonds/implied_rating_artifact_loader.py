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
from dataclasses import fields as dataclass_fields
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

    if type(root["parent"]) is dict and "header_sha256" not in root["parent"]:
        _fail(ErrorCode.CONTRACT_INVALID, "parent.header_sha256")
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
    elif status is ContractStatus.READY:
        # A READY contract must pin the reviewed child/predecessor header digest;
        # only a parse-only identity_pending contract may still leave it open.
        _fail(ErrorCode.CONTRACT_INVALID, "parent.header_sha256")
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
    header_sha = contract.parent.header_sha256
    if type(header_sha) is not str or SHA256_RE.fullmatch(header_sha) is None:
        _fail(ErrorCode.CONTRACT_INVALID, "parent.header_sha256")
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


def _publication_from_contract(contract: FrozenArtifactContract) -> ImpliedRatingPublication:
    """The only publication metadata a READY contract admits, derived from its pins."""
    _require_ready(contract)
    assert contract.identity is not None
    return ImpliedRatingPublication(
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


def _strict_same(actual: Any, expected: Any) -> bool:
    """Exact value equality that refuses Python's loose cross-type equality.

    ``True == 1``, ``np.float64(x) == x``, ``-0.0 == 0.0`` and date/datetime
    subclasses all compare equal under ``==``; admission of caller-supplied
    metadata must not.
    """
    if type(actual) is not type(expected):
        return False
    if type(expected) is float:
        return actual.hex() == expected.hex()
    if type(expected) is tuple:
        return len(actual) == len(expected) and all(
            _strict_same(left, right) for left, right in zip(actual, expected, strict=True)
        )
    return bool(actual == expected)


def _require_publication(
    publication: Any, expected: ImpliedRatingPublication
) -> ImpliedRatingPublication:
    if type(publication) is not ImpliedRatingPublication:
        _fail(ErrorCode.IDENTITY_MISMATCH, "publication.type")
    for field in dataclass_fields(ImpliedRatingPublication):
        if not _strict_same(getattr(publication, field.name), getattr(expected, field.name)):
            _fail(ErrorCode.IDENTITY_MISMATCH, f"publication.{field.name}")
    return expected


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
    publication = _publication_from_contract(contract)
    return VerifiedArtifact(contract_sha256, publication, summary, tuple(file_evidence), table)


def load_verified_artifact(artifact_root: Path) -> VerifiedArtifact:
    raw = _read_contract_bytes()
    return _load_verified_artifact(
        artifact_root,
        contract=_parse_contract(raw),
        contract_sha256=_hash_bytes(raw),
    )


_PINNED_SUMMARY_FIELDS = (
    "row_count", "first_month", "last_month", "month_count", "unique_key_count",
    "witnessed_count", "d_confirmed_count", "d_candidate_count", "histogram", "rows_digest",
)


def _summary_values(summary: ArtifactSummary) -> tuple[Any, ...]:
    return tuple(getattr(summary, field.name) for field in dataclass_fields(ArtifactSummary))


def _file_values(evidence: FileEvidence) -> tuple[Any, ...]:
    return (evidence.label, evidence.relative_path, evidence.size_bytes, evidence.sha256)


def _expected_file_evidence(contract: FrozenArtifactContract) -> tuple[FileEvidence, ...]:
    """File evidence a genuine offline load of ``contract`` produces, in load order."""
    assert contract.identity is not None
    pins = (*contract.manifests, contract.identity.receipt, contract.artifact)
    return (
        *_verify_sources(contract),
        *(
            FileEvidence(pin.label, pin.relative_path, pin.size_bytes, pin.sha256)
            for pin in pins
        ),
    )


def _bind_verified_artifact(
    artifact: Any, contract: FrozenArtifactContract
) -> VerifiedArtifact:
    """Rebind a caller-supplied artifact to the frozen contract before any DB use.

    ``VerifiedArtifact`` is a plain dataclass, so a programmatic caller can build
    or ``replace`` one with arbitrary metadata.  Every publication field, every
    pinned summary value, the Arrow null counts, the file evidence and the Arrow
    schema/row count must equal what the contract determines; the returned
    artifact carries the contract-derived publication, never the supplied one.
    Row *content* is bound to the contract's rows digest by the full frame
    recomputation in :func:`_artifact_payload` before any write.
    """
    publication = _publication_from_contract(contract)
    if type(artifact) is not VerifiedArtifact:
        _fail(ErrorCode.IDENTITY_MISMATCH, "artifact.type")
    contract_sha = artifact.contract_sha256
    if type(contract_sha) is not str or SHA256_RE.fullmatch(contract_sha) is None:
        _fail(ErrorCode.IDENTITY_MISMATCH, "artifact.contract_sha256")
    _require_publication(artifact.publication, publication)
    table = artifact.table
    if type(table) is not pa.Table:
        _fail(ErrorCode.IDENTITY_MISMATCH, "artifact.table")
    _verify_arrow_schema(table.schema, contract)
    if table.num_rows != contract.expected.row_count:
        _fail(ErrorCode.IDENTITY_MISMATCH, "artifact.table.row_count")
    summary = artifact.summary
    if type(summary) is not ArtifactSummary:
        _fail(ErrorCode.IDENTITY_MISMATCH, "artifact.summary")
    for name in _PINNED_SUMMARY_FIELDS:
        if not _strict_same(getattr(summary, name), getattr(contract.expected, name)):
            _fail(ErrorCode.IDENTITY_MISMATCH, f"artifact.summary.{name}")
    arrow_nulls = tuple(
        (name, int(table[name].null_count)) for name in policy.PUBLICATION_COLUMNS
    )
    if not _strict_same(summary.null_counts, arrow_nulls):
        _fail(ErrorCode.IDENTITY_MISMATCH, "artifact.summary.null_counts")
    expected_files = _expected_file_evidence(contract)
    files = artifact.files
    if (
        type(files) is not tuple
        or len(files) != len(expected_files)
        or any(type(item) is not FileEvidence for item in files)
    ):
        _fail(ErrorCode.IDENTITY_MISMATCH, "artifact.files")
    for index, (item, expected) in enumerate(zip(files, expected_files, strict=True)):
        if not _strict_same(_file_values(item), _file_values(expected)):
            _fail(ErrorCode.IDENTITY_MISMATCH, f"artifact.files.{index}")
    return VerifiedArtifact(contract_sha, publication, summary, files, table)


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
    # Always compared: the complete reviewed child/predecessor projection, not only
    # the core fields above, is the admitted lineage.
    if pins.header_sha256 is None or digest != pins.header_sha256:
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


_WORKER_ROLE = "worker_writer"


def _verify_worker_session(conn: psycopg.Connection) -> None:
    """The session must genuinely be the non-elevated LOGIN ``worker_writer``.

    ``current_user`` and ``session_user`` must both be the worker role, the session
    ``role`` setting must be ``none`` (an explicit role switch -- even to itself -- or
    an administrative login switched to the worker is refused), the role must be a
    LOGIN role without superuser/createrole/createdb/replication/bypassrls, and no
    SET- or inherit-enabled membership path may reach an elevated role.  Catalog
    lookups are schema-qualified, so a search_path shadow cannot answer them.  The
    refusal is a bounded field; role names and the DSN are never echoed.
    """
    row = conn.execute(
        "SELECT pg_catalog.current_setting('role'), current_user, session_user, "
        "r.rolcanlogin, r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolreplication, "
        "r.rolbypassrls, "
        "EXISTS (SELECT 1 FROM pg_catalog.pg_roles e "
        "WHERE e.oid <> r.oid AND (pg_catalog.pg_has_role(r.oid, e.oid, 'SET') "
        "OR pg_catalog.pg_has_role(r.oid, e.oid, 'USAGE')) "
        "AND (e.rolsuper OR e.rolcreaterole OR e.rolcreatedb OR e.rolreplication "
        "OR e.rolbypassrls)) "
        "FROM pg_catalog.pg_roles r WHERE r.rolname = session_user"
    ).fetchone()
    if row is None:
        _fail(ErrorCode.IDENTITY_MISMATCH, "database.session_role")
    role_setting, current_user, session_user, can_login, *elevated = row
    if (
        role_setting != "none"
        or current_user != _WORKER_ROLE
        or session_user != _WORKER_ROLE
        or can_login is not True
        or any(value is not False for value in elevated)
    ):
        _fail(ErrorCode.IDENTITY_MISMATCH, "database.session_role")


@contextlib.contextmanager
def _worker_connection(
    connection_factory: Callable[[], psycopg.Connection],
) -> Iterator[psycopg.Connection]:
    """Open a connection and admit its session before any DDL, lock or read."""
    with connection_factory() as conn:
        _verify_worker_session(conn)
        yield conn


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

# Exact PG18 column-expression semantics of the six physical tables, captured from a
# clean install of the reviewed DDL (schema first on the search_path, genuine
# worker_writer login): (atthasdef, pg_get_expr(adbin, adrelid, false) or None,
# attidentity, attgenerated).  Every column not listed has no default, no identity
# and no generated expression -- an explicit DEFAULT NULL is a drift, not an
# equivalent.  Strings are compared byte-for-byte, never case- or space-folded.
_NO_COLUMN_EXPRESSION: tuple[bool, str | None, str, str] = (False, None, "", "")
_EXPECTED_COLUMN_EXPRESSIONS: dict[str, dict[str, tuple[bool, str | None, str, str]]] = {
    "sec_derived_publications": {
        "prepared_at": (True, "now()", "", ""),
        "lifecycle_state": (True, "'prepared'::text", "", ""),
    },
    "sec_derived_current_pointers": {"set_at": (True, "now()", "", "")},
    "sec_derived_publication_tokens": {},
    "sec_derived_pointer_tokens": {},
    "bond_market_implied_rating_v1_builds": {"created_at": (True, "now()", "", "")},
    "bond_market_implied_rating_v1": {},
}
_PHYSICAL_TABLES = (
    *_SHARED_SCHEMA_OBJECTS,
    "bond_market_implied_rating_v1_builds", "bond_market_implied_rating_v1",
)
_SECONDARY_INDEXES = {
    "bond_market_implied_rating_v1_pub_month_idx": ("publication_id", "month"),
    "bond_market_implied_rating_v1_cusip_month_idx": ("cusip_id", "month"),
}

# Exact PG18 ``pg_attribute.atttypmod`` of the physical columns.  The reviewed DDL
# declares every digest/fingerprint as ``char(64)``, which PG18 stores as the
# bpchar modifier 64 + VARHDRSZ = 68; every other pinned column is an
# unconstrained type whose modifier is -1.  The type name alone cannot tell
# ``char(64)`` from ``char(32)`` or unbounded ``bpchar``, and the unchanged CHECK
# text keeps the constraint hash equal, so the modifier is compared explicitly.
# A malformed width is refused, never padded or normalized.
_UNCONSTRAINED_TYPMOD = -1
_CHAR64_TYPMOD = 68
_EXPECTED_COLUMN_TYPMODS: dict[str, dict[str, int]] = {
    "sec_derived_publications": {"build_fingerprint": _CHAR64_TYPMOD},
    "sec_derived_current_pointers": {},
    "sec_derived_publication_tokens": {},
    "sec_derived_pointer_tokens": {},
    "bond_market_implied_rating_v1_builds": {
        "policy_digest": _CHAR64_TYPMOD,
        "input_fingerprint": _CHAR64_TYPMOD,
        "rows_digest": _CHAR64_TYPMOD,
    },
    "bond_market_implied_rating_v1": {"policy_digest": _CHAR64_TYPMOD},
}


def _expected_relation_columns(name: str) -> tuple[tuple[Any, ...], ...]:
    """(name, type, typmod, nullable, hasdef, default, identity, generated) per column."""
    expressions = _EXPECTED_COLUMN_EXPRESSIONS[name]
    typmods = _EXPECTED_COLUMN_TYPMODS[name]
    return tuple(
        (
            column_name, type_name, typmods.get(column_name, _UNCONSTRAINED_TYPMOD), nullable,
            *expressions.get(column_name, _NO_COLUMN_EXPRESSION),
        )
        for column_name, type_name, nullable in _EXPECTED_COLUMNS[name]
    )


_EXPECTED_CONSTRAINT_HASHES = {
    False: "9d69f7f1c5ebf1b77426b7c1857a4c165840fda7760b25151c05630e274eb64a",
    True: "26e024de4030da9600e2ca4704fd5186fe6e8bd385d4056638b76b59603f9689",
}

# Function contracts: sha256(prosrc), identity args, full args, #defaults, result,
# language, volatility, parallel, SECURITY DEFINER, strict, leakproof, kind,
# set-returning, proconfig, owner.  Privileges are validated semantically by
# ``_verify_function_acls`` (an ACL's text form is not its meaning: NULL and an
# explicit owner/PUBLIC list grant the same EXECUTE).
_SHARED_FUNCTION_CONTRACTS: dict[str, tuple[Any, ...]] = {
    "sec_derived_pointer_guard()": (
        "0af65cf5d0c4c04762c82c95eb245888ece0db45d5275d6fcf6f5d73f65ae84e",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer",
    ),
    "sec_derived_publication_as_of(uuid)": (
        "db638702fda1189bf0d1f196a7a642db33228577a9363bb750d7dc24c9205e94",
        "target_publication_id uuid", "target_publication_id uuid", 0,
        "date", "plpgsql", "s", "u", False, False, False, "f", False,
        None, "worker_writer",
    ),
    "sec_derived_publication_delete_guard()": (
        "e87342589b00a20437f3c7bc8ce9760948121c801514c9103d863e8e1c64f958",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer",
    ),
    "sec_derived_publication_immutable()": (
        "9263cad4f6b6bf449185d794b0a3c3ddbb04b60144c95beb7e209685cdc40ffd",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer",
    ),
    "sec_derived_publication_is_validated(uuid,text)": (
        "07c3fac7bb10c6c2e8a2c3d735553cff06cbfee866d3ea8baa7c71ef16a5d505",
        "target_publication_id uuid, expected_product text",
        "target_publication_id uuid, expected_product text DEFAULT NULL::text", 1,
        "boolean", "sql", "s", "u", False, False, False, "f", False,
        None, "worker_writer",
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
        None, "worker_writer",
    ),
    "sec_validate_derived_publication(uuid)": (
        "01ec9ea04a6160b18c33b5afc1f1d9a7d1361c08986496536cb12689a29b527f",
        "target_publication_id uuid", "target_publication_id uuid", 0,
        "void", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer",
    ),
}
_PRODUCT_FUNCTION_CONTRACTS: dict[str, tuple[Any, ...]] = {
    "bond_market_implied_rating_v1_write_guard()": (
        "6823804a0b205c5f2b4e38b697b229665e597fdf4883cd53b5ba591022593fb0",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer",
    ),
}

# Trigger contracts: (trigger, relation) -> (function, tgtype, tgenabled,
# sha256(pg_get_triggerdef(oid, true))).
_SHARED_TRIGGER_CONTRACTS: dict[tuple[str, str], tuple[Any, ...]] = {
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
_PRODUCT_TRIGGER_CONTRACTS: dict[tuple[str, str], tuple[Any, ...]] = {
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
}

# The RR1 fee-profile guards (schemas/rr1_fee_profiles.sql) that production
# attaches to the shared ledger.  They are an existing, checked DB dependency,
# never installed or replaced here, and are admitted only as this complete
# exact pair of triggers plus functions (all-or-none).  Their product filter
# lives inside the pinned PL/pgSQL bodies, so exact source equality is what
# keeps them no-ops for every other product.
#
# Provenance: trigger definitions, tgtype/tgenabled, function result, language,
# volatility, SECURITY INVOKER, proconfig, owner and prosrc hashes equal the
# read-only production capture (receipt 148e6b2a...) and a clean PG 18.1/18.4
# install of the reviewed SQL.  Identity/full arguments, defaults, parallel,
# strict, leakproof, kind and set-returning come from that clean PG18
# reconstruction only, pending the bounded production attribute capture.
_RR1_TRIGGER_CONTRACTS: dict[tuple[str, str], tuple[Any, ...]] = {
    ("rr1_fee_profile_current_pointer_guard", "sec_derived_current_pointers"): (
        "rr1_fee_profile_current_pointer_guard()", 31, "O",
        "9d0c812daaac9ed748babba5767c460403fc5772baaed961263543bdad4a7167",
    ),
    ("rr1_fee_profile_publication_validation_guard", "sec_derived_publications"): (
        "rr1_fee_profile_publication_validation_guard()", 19, "O",
        "d00b5314cca674de68b895a00127306f5a6a3c61e218f910660fdc7bb4da8182",
    ),
}
_RR1_FUNCTION_CONTRACTS: dict[str, tuple[Any, ...]] = {
    "rr1_fee_profile_current_pointer_guard()": (
        "5e17ad39bf55b7f0c1802527b9b23b271e72f7525aec38402a77647a9b6767cb",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer",
    ),
    "rr1_fee_profile_publication_validation_guard()": (
        "49282817bd24062a49a9b5fb8b73eb095f9d8c2ff28c3755906c2256f6210608",
        "", "", 0, "trigger", "plpgsql", "v", "u", False, False, False, "f", False,
        None, "worker_writer",
    ),
}

# Shared-ledger extension profiles.  A clean install is "baseline"; the known
# production target carries the RR1 pair, and its envelope requires exactly that.
PROFILE_BASELINE = "baseline"
PROFILE_RR1 = "rr1_fee_profile_guards"
_PRODUCTION_REQUIRED_PROFILE = PROFILE_RR1
_PRODUCTION_REQUIRED_ROLES = ("worker_writer", "app_runtime", "app_analytics_ro")

# Function EXECUTE admission: the owner and PUBLIC must hold it (PostgreSQL's
# own default); app_runtime may hold a redundant explicit grant.  Nothing else.
_FUNCTION_OPTIONAL_GRANTEES = ("app_runtime",)

# Exact PG18 ``pg_get_viewdef(oid, false)`` renderings captured from a clean install
# of the reviewed DDL (schema on the search_path, as the loader runs).  Compared
# byte-for-byte: no lowercasing or whitespace folding, so a case-sensitive literal
# such as the product filter cannot drift while still comparing equal.
_EXPECTED_VIEW_DEFINITIONS: dict[str, str] = {
    "bond_market_implied_rating_v1_current": (
        " SELECT r.publication_id,\n    r.month,\n    r.cusip_id,\n    r.implied_bucket,\n"
        "    r.spread_norm_log,\n    r.neutralized_score,\n    r.market_level_l,\n"
        "    r.witnessed,\n    r.carry_months,\n    r.spell_id,\n    r.d_candidate,\n"
        "    r.d_confirmed,\n    r.d_event_month,\n    r.recovery_observed,\n"
        "    r.censoring,\n    r.policy_version,\n    r.policy_digest\n"
        "   FROM (sec_derived_current_pointers pointer\n"
        "     JOIN bond_market_implied_rating_v1 r ON ((r.publication_id = pointer.publication_id)))\n"
        "  WHERE (pointer.product = 'bond_market_implied_rating_v1'::text);"
    ),
    "bond_market_implied_rating_publications": (
        " SELECT s.publication_id,\n    s.product,\n    s.lifecycle_state AS publication_status,\n"
        "    NULL::text AS failure_reason,\n    b.policy_version,\n    b.policy_digest,\n"
        "    b.code_revision,\n    b.panel_publication_id,\n    b.panel_last_closed_month,\n"
        "    b.first_month,\n    b.last_month,\n    b.input_fingerprint,\n    b.row_count,\n"
        "    b.rows_digest,\n    b.d_confirmed_count,\n    b.d_candidate_count,\n"
        "    s.prepared_at AS built_at,\n    s.validated_at\n"
        "   FROM (sec_derived_publications s\n"
        "     JOIN bond_market_implied_rating_v1_builds b USING (publication_id))\n"
        "  WHERE (s.product = 'bond_market_implied_rating_v1'::text);"
    ),
    "bond_market_implied_rating_app_pointer": (
        " SELECT product,\n    publication_id,\n    set_at AS changed_at\n"
        "   FROM sec_derived_current_pointers\n"
        "  WHERE (product = 'bond_market_implied_rating_v1'::text);"
    ),
}

# Relation privilege admission.  The owner is trusted; known readers may hold
# non-grantable SELECT only (absence of a read grant is fine: read grants are
# operational).  Everything else -- PUBLIC, other grantees, write privileges,
# TRUNCATE (which bypasses row-level write guards), REFERENCES, TRIGGER,
# MAINTAIN, grant options, column-level grants -- is refused, never repaired.
_RELATION_OWNER = "worker_writer"
_READER_ROLES = ("app_analytics_ro", "app_runtime")
_READER_FORBIDDEN_TABLE_PRIVILEGES = (
    "INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, MAINTAIN"
)
_READER_FORBIDDEN_COLUMN_PRIVILEGES = "INSERT, UPDATE, REFERENCES"
# The owner's complete ordinary privilege set on every admitted table and view:
# exactly ``aclexplode(acldefault('r', owner))`` on PG18 (a NULL ACL means this
# default; the reviewed DDL's REVOKE-from-PUBLIC materializes it as the explicit
# ``arwdDxtm``).  Ownership only confers the right to grant, not ordinary access,
# so every one of these must remain an owner-issued entry of the relation's own ACL.
_OWNER_RELATION_PRIVILEGES = (
    "DELETE", "INSERT", "MAINTAIN", "REFERENCES", "SELECT", "TRIGGER", "TRUNCATE", "UPDATE",
)


def _acl_item_admitted(
    owner_oid: int, grantee_oid: int, grantee: str | None, privilege: str, grantable: bool
) -> bool:
    if grantee_oid == owner_oid:
        return True
    return (
        grantee_oid != 0
        and grantee in _READER_ROLES
        and privilege == "SELECT"
        and grantable is False
    )


def _verify_schema_privileges(conn: psycopg.Connection) -> None:
    """Readers cannot create in, or assume the owner of, any searched schema."""
    rows = conn.execute(
        "WITH RECURSIVE assumable(reader_oid,role_oid) AS ("
        "SELECT oid,oid FROM pg_catalog.pg_roles WHERE rolname=ANY(%s) "
        "UNION SELECT a.reader_oid,m.roleid FROM assumable a "
        "JOIN pg_catalog.pg_auth_members m ON m.member=a.role_oid "
        "WHERE m.set_option OR m.admin_option) "
        "SELECT n.nspname,r.rolname,EXISTS ("
        "SELECT 1 FROM assumable a JOIN pg_catalog.pg_roles target ON target.oid=a.role_oid "
        "WHERE a.reader_oid=r.oid AND (target.rolsuper "
        "OR pg_catalog.pg_has_role(target.oid,n.nspowner,'MEMBER') "
        "OR pg_catalog.has_schema_privilege(target.oid,n.oid,'CREATE'))) "
        "FROM pg_catalog.pg_namespace n "
        "LEFT JOIN pg_catalog.pg_roles r ON r.rolname=ANY(%s) "
        "WHERE n.nspname=ANY(pg_catalog.current_schemas(false)) "
        "ORDER BY n.nspname,r.rolname",
        (list(_READER_ROLES), list(_READER_ROLES)),
    ).fetchall()
    if not rows:
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.runtime_create")
    for _, reader, can_create in rows:
        if can_create:
            field = (
                "schema.runtime_create" if reader == "app_runtime"
                else f"schema.reader_role.{reader}"
            )
            _fail(ErrorCode.SCHEMA_MISMATCH, field)


def _verify_reader_memberships(conn: psycopg.Connection) -> None:
    """No reader holds, or can act as a principal holding, any ADMIN OPTION.

    An ADMIN OPTION lets its holder add memberships -- including its own, with SET
    or INHERIT enabled -- so a reader whose membership edge is ``INHERIT FALSE,
    SET FALSE, ADMIN TRUE`` on ``pg_write_all_data`` shows no effective write
    privilege yet can obtain one at will.  Any ADMIN OPTION edge held by a principal
    the reader can act as therefore refuses, whatever that edge's own INHERIT/SET
    flags and whatever the administered role grants today; admission never tries
    to prove an administered role will stay harmless, and it never edits
    memberships.

    Where an ADMIN OPTION can be exercised follows PG18 membership semantics:

    * each reader itself is a principal sessions may run as (a LOGIN session user,
      or a role its login may switch to);
    * switching the current role needs an unbroken chain of ``SET TRUE`` edges from
      the session user, so a role reached through an ``INHERIT``-only edge
      contributes its inherited roles but never extends the SET chain;
    * granting or revoking a membership uses the ADMIN OPTION of the current role
      and of every role it inherits -- an unbroken chain of ``INHERIT TRUE`` edges
      from a role the session can be (also what ``GRANTED BY`` requires of the
      grantor);
    * a current role with CREATEROLE may ALTER, RENAME or DROP every role it is
      admin of through ANY membership path: PG18 ``is_admin_of_role`` walks every
      edge, whatever its INHERIT/SET flags.  Making an administered role LOGIN and
      setting its password takes it over, so ADMIN held anywhere below a
      SET-reachable CREATEROLE principal refuses;
    * otherwise an edge with neither INHERIT nor SET conveys nothing, so ADMIN held
      beyond it is not reader capability and does not refuse.

    ``principal`` carries whether a SET chain is still unbroken: from a
    SET-reachable role it follows INHERIT and SET edges, from an inherit-only role
    INHERIT edges alone.  ``createrole_member`` follows every edge from each
    SET-reachable CREATEROLE principal.  The states are finite and UNION bounds
    both recursions, cyclic graphs included.  Checked for every schema state,
    before any DDL.
    """
    rows = conn.execute(
        "WITH RECURSIVE principal(reader_oid,role_oid,can_set) AS ("
        "SELECT oid,oid,true FROM pg_catalog.pg_roles WHERE rolname=ANY(%s) "
        "UNION SELECT p.reader_oid,m.roleid,p.can_set AND m.set_option "
        "FROM principal p JOIN pg_catalog.pg_auth_members m ON m.member=p.role_oid "
        "WHERE m.inherit_option OR (p.can_set AND m.set_option)), "
        "createrole_member(reader_oid,role_oid) AS ("
        "SELECT p.reader_oid,p.role_oid FROM principal p "
        "JOIN pg_catalog.pg_roles c ON c.oid=p.role_oid WHERE p.can_set AND c.rolcreaterole "
        "UNION SELECT o.reader_oid,m.roleid FROM createrole_member o "
        "JOIN pg_catalog.pg_auth_members m ON m.member=o.role_oid), "
        "administrator(reader_oid,role_oid) AS ("
        "SELECT reader_oid,role_oid FROM principal "
        "UNION SELECT reader_oid,role_oid FROM createrole_member) "
        "SELECT r.rolname,EXISTS (SELECT 1 FROM administrator a "
        "JOIN pg_catalog.pg_auth_members m ON m.member=a.role_oid "
        "WHERE a.reader_oid=r.oid AND m.admin_option) "
        "FROM pg_catalog.pg_roles r WHERE r.rolname=ANY(%s) ORDER BY r.rolname",
        (list(_READER_ROLES), list(_READER_ROLES)),
    ).fetchall()
    for reader, administers in rows:
        if administers:
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.reader_role.{reader}")


def _verify_relation_acls(conn: psycopg.Connection, relation_names: Sequence[str]) -> None:
    names = list(relation_names)
    owners = conn.execute(
        "SELECT c.relname, pg_get_userbyid(c.relowner) FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) ORDER BY c.relname",
        (names,),
    ).fetchall()
    if len(owners) != len(names) or any(owner != _RELATION_OWNER for _, owner in owners):
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.owners")
    relation_acl = conn.execute(
        "SELECT c.relname, c.relowner, acl.grantee, "
        "CASE WHEN acl.grantee = 0 THEN NULL ELSE pg_get_userbyid(acl.grantee) END, "
        "acl.privilege_type, acl.is_grantable FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) acl "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) "
        "ORDER BY c.relname, acl.grantee, acl.privilege_type",
        (names,),
    ).fetchall()
    for relname, owner_oid, grantee_oid, grantee, privilege, grantable in relation_acl:
        if not _acl_item_admitted(owner_oid, grantee_oid, grantee, privilege, grantable):
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.acl.{relname}")
    # Ownership is not ordinary access: PostgreSQL lets an owner revoke its own
    # table privileges.  Each relation's own ACL (NULL is the owner default) must
    # still carry the owner's complete ordinary set as owner-issued entries -- a
    # grant reaching the owner through another role cannot mask a self-revoke --
    # and the genuine worker session must effectively hold every one of them.
    # Nothing is repaired; a missing owner entry refuses.
    owner_privileges = conn.execute(
        "SELECT c.relname, ARRAY(SELECT acl.privilege_type "
        "FROM aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) acl "
        "WHERE acl.grantee=c.relowner AND acl.grantor=c.relowner), "
        "ARRAY(SELECT p.privilege FROM unnest(%s::text[]) AS p(privilege) "
        "WHERE NOT has_table_privilege(current_user, c.oid, p.privilege)) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) ORDER BY c.relname",
        (list(_OWNER_RELATION_PRIVILEGES), names),
    ).fetchall()
    if len(owner_privileges) != len(names):
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.owners")
    for relname, held, not_effective in owner_privileges:
        if tuple(sorted(set(held))) != _OWNER_RELATION_PRIVILEGES or not_effective:
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.owner_acl.{relname}")
    column_acl = conn.execute(
        "SELECT c.relname, c.relowner, acl.grantee, "
        "CASE WHEN acl.grantee = 0 THEN NULL ELSE pg_get_userbyid(acl.grantee) END, "
        "acl.privilege_type, acl.is_grantable FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum > 0 AND NOT a.attisdropped "
        "CROSS JOIN LATERAL aclexplode(a.attacl) acl "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) AND a.attacl IS NOT NULL "
        "ORDER BY c.relname, a.attnum, acl.grantee, acl.privilege_type",
        (names,),
    ).fetchall()
    for relname, owner_oid, grantee_oid, grantee, privilege, grantable in column_acl:
        if not _acl_item_admitted(owner_oid, grantee_oid, grantee, privilege, grantable):
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.column_acl.{relname}")
    # Keep the direct owner/ACL checks, then inspect every role assumable through
    # SET-enabled memberships. UNION bounds the walk even on cyclic graphs.
    readers = conn.execute(
        "WITH RECURSIVE assumable(reader_oid, role_oid) AS ("
        "SELECT oid, oid FROM pg_roles WHERE rolname=ANY(%s) "
        "UNION SELECT a.reader_oid, m.roleid FROM assumable a "
        "JOIN pg_auth_members m ON m.member=a.role_oid WHERE m.set_option) "
        "SELECT r.rolname, r.rolsuper, "
        "EXISTS (SELECT 1 FROM pg_roles w WHERE w.rolname=%s "
        "AND pg_has_role(r.oid, w.oid, 'MEMBER')), "
        "EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) "
        "AND (has_table_privilege(r.oid, c.oid, %s) "
        "OR has_any_column_privilege(r.oid, c.oid, %s))), "
        "EXISTS (SELECT 1 FROM assumable a JOIN pg_roles target ON target.oid=a.role_oid "
        "WHERE a.reader_oid=r.oid "
        "AND (target.rolsuper OR EXISTS (SELECT 1 FROM pg_roles w "
        "WHERE w.rolname=%s AND pg_has_role(target.oid, w.oid, 'MEMBER')) "
        "OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) "
        "AND (has_table_privilege(target.oid, c.oid, %s) "
        "OR has_any_column_privilege(target.oid, c.oid, %s))))) "
        "FROM pg_roles r WHERE r.rolname=ANY(%s) ORDER BY r.rolname",
        (
            list(_READER_ROLES), _RELATION_OWNER, names, _READER_FORBIDDEN_TABLE_PRIVILEGES,
            _READER_FORBIDDEN_COLUMN_PRIVILEGES, _RELATION_OWNER, names,
            _READER_FORBIDDEN_TABLE_PRIVILEGES, _READER_FORBIDDEN_COLUMN_PRIVILEGES,
            list(_READER_ROLES),
        ),
    ).fetchall()
    for rolname, superuser, reaches_owner, writes, assumable_writes in readers:
        if superuser or reaches_owner or writes or assumable_writes:
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.reader_role.{rolname}")


def _relation_columns(conn: psycopg.Connection, name: str) -> tuple[tuple[Any, ...], ...]:
    """Name, type, exact type modifier, nullability plus default/identity/generated.

    Reads ``pg_attribute`` of the schema-resolved relation (not the search_path)
    with a LEFT JOIN on ``pg_attrdef``; dropped and system columns are excluded.
    ``atttypmod`` is the raw PG18 modifier (68 for ``char(64)``, -1 when the type is
    unconstrained).  The default is the PG18 deparser's exact text, NULL when there
    is none.
    """
    rows = conn.execute(
        "SELECT a.attname, t.typname, a.atttypmod, "
        "CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END, "
        "a.atthasdef, pg_catalog.pg_get_expr(d.adbin, d.adrelid, false), "
        "a.attidentity::text, a.attgenerated::text "
        "FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON c.oid=a.attrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "JOIN pg_catalog.pg_type t ON t.oid=a.atttypid "
        "LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
        "WHERE n.nspname=current_schema() AND c.relname=%s "
        "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
        (name,),
    ).fetchall()
    return tuple(tuple(row) for row in rows)


def _function_acl_item_admitted(
    owner_oid: int,
    grantor_oid: int,
    grantee_oid: int,
    grantee: str | None,
    privilege: str,
    grantable: bool,
) -> bool:
    if grantor_oid != owner_oid or privilege != "EXECUTE" or grantable is not False:
        return False
    if grantee_oid in (owner_oid, 0):
        return True
    return grantee in _FUNCTION_OPTIONAL_GRANTEES


def _verify_function_acls(conn: psycopg.Connection, function_oids: dict[str, int]) -> None:
    """Semantic EXECUTE admission for every protected function.

    The ACL is expanded (NULL means the owner/PUBLIC default), every entry must be
    an owner-issued, non-grantable EXECUTE to the owner, PUBLIC or the optional
    redundant app_runtime grant, and owner plus PUBLIC EXECUTE must both be
    present.  Order and textual representation are irrelevant; unknown or
    unresolvable grantees, other grantors and grant options refuse.
    """
    if not function_oids:
        return
    rows = conn.execute(
        "SELECT p.oid::regprocedure::text,p.proowner,acl.grantor,acl.grantee,"
        "CASE WHEN acl.grantee=0 THEN NULL ELSE "
        "(SELECT r.rolname FROM pg_roles r WHERE r.oid=acl.grantee) END,"
        "acl.privilege_type,acl.is_grantable FROM pg_proc p "
        "CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) acl "
        "WHERE p.oid=ANY(%s) ORDER BY 1,acl.grantee,acl.privilege_type",
        (sorted(function_oids.values()),),
    ).fetchall()
    holders: dict[str, set[int]] = {signature: set() for signature in function_oids}
    for signature, owner_oid, grantor_oid, grantee_oid, grantee, privilege, grantable in rows:
        if not _function_acl_item_admitted(
            owner_oid, grantor_oid, grantee_oid, grantee, privilege, grantable
        ):
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.function_acl.{signature}")
        holders[signature].add(grantee_oid)
    owners = dict(conn.execute(
        "SELECT p.oid::regprocedure::text,p.proowner FROM pg_proc p WHERE p.oid=ANY(%s)",
        (sorted(function_oids.values()),),
    ).fetchall())
    for signature, granted in holders.items():
        if owners.get(signature) not in granted or 0 not in granted:
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.function_acl.{signature}")


def _verify_relation_contracts(conn: psycopg.Connection, *, include_product: bool) -> str:
    """Exact relation/trigger/function/view/ACL contract; returns the extension profile."""
    relation_names = list(_SHARED_SCHEMA_OBJECTS)
    if include_product:
        relation_names.extend(_PRODUCT_SCHEMA_OBJECTS)
    relations = {
        row[0]: (row[1], row[2])
        for row in conn.execute(
            "SELECT c.relname, c.relkind, c.relpersistence FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=current_schema() AND c.relname=ANY(%s)",
            (relation_names,),
        ).fetchall()
    }
    kinds = {name: kind for name, (kind, _) in relations.items()}
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
    physical = [name for name in _PHYSICAL_TABLES if name in expected_kinds]
    # Crash-safe WAL-logged tables only: an UNLOGGED (or temporary) ledger/product
    # table is truncated by crash recovery and never repaired here.
    for name in physical:
        if relations[name][1] != "p":
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.persistence.{name}")
    rls = conn.execute(
        "SELECT c.relname,c.relrowsecurity,c.relforcerowsecurity,"
        "EXISTS (SELECT 1 FROM pg_catalog.pg_policy p WHERE p.polrelid=c.oid) "
        "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) ORDER BY c.relname",
        (physical,),
    ).fetchall()
    if len(rls) != len(physical):
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.rls")
    for name, enabled, forced, policies in rls:
        if enabled or forced or policies:
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.rls.{name}")
    for name in physical:
        if _relation_columns(conn, name) != _expected_relation_columns(name):
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.columns.{name}")

    if include_product:
        indexes = conn.execute(
            "SELECT idx.relname,t.relname,idx.relkind,idx.relpersistence,"
            "idx.reloptions,idx.reltablespace,"
            "am.amname,i.indisvalid,i.indisready,i.indislive,i.indisunique,i.indisprimary,"
            "i.indisexclusion,i.indisreplident,i.indnkeyatts,i.indnatts,"
            "i.indnullsnotdistinct,i.indexprs IS NULL,i.indpred IS NULL,"
            "ARRAY(SELECT a.attname::text FROM generate_series(0,i.indnkeyatts-1) g "
            "JOIN pg_catalog.pg_attribute a ON a.attrelid=t.oid AND a.attnum=i.indkey[g] "
            "JOIN pg_catalog.pg_opclass op ON op.oid=i.indclass[g] "
            "WHERE op.opcdefault AND op.opcintype=a.atttypid "
            "AND i.indcollation[g]=a.attcollation AND i.indoption[g]=0 "
            "ORDER BY g) "
            "FROM pg_catalog.pg_class idx "
            "JOIN pg_catalog.pg_namespace n ON n.oid=idx.relnamespace "
            "JOIN pg_catalog.pg_index i ON i.indexrelid=idx.oid "
            "JOIN pg_catalog.pg_class t ON t.oid=i.indrelid "
            "JOIN pg_catalog.pg_am am ON am.oid=idx.relam "
            "WHERE n.nspname=current_schema() AND idx.relname=ANY(%s)",
            (list(_SECONDARY_INDEXES),),
        ).fetchall()
        observed = {row[0]: row[1:] for row in indexes}
        for name, columns in _SECONDARY_INDEXES.items():
            expected = (
                "bond_market_implied_rating_v1", "i", "p", None, 0, "btree",
                True, True, True, False, False, False, False,
                len(columns), len(columns), False, True, True, list(columns),
            )
            if observed.get(name) != expected:
                _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.index.{name}")

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
        "pg_get_triggerdef(t.oid,true),p.pronamespace=c.relnamespace FROM pg_trigger t "
        "JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_class c ON c.oid=t.tgrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relname=ANY(%s) AND NOT t.tgisinternal"
        " ORDER BY t.tgname,c.relname",
        (relation_names,),
    ).fetchall()
    # Every protected trigger executes a function of the ledger's own schema.
    if not all(row[6] is True for row in trigger_rows):
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.trigger_binding")
    observed_triggers = {
        (row[0], row[1]): (
            row[2], int(row[3]), row[4],
            _hash_bytes(row[5].encode("utf-8")),
        )
        for row in trigger_rows
    }
    baseline_triggers = dict(_SHARED_TRIGGER_CONTRACTS)
    baseline_functions = dict(_SHARED_FUNCTION_CONTRACTS)
    if include_product:
        baseline_triggers.update(_PRODUCT_TRIGGER_CONTRACTS)
        baseline_functions.update(_PRODUCT_FUNCTION_CONTRACTS)
    if observed_triggers == baseline_triggers:
        profile = PROFILE_BASELINE
        expected_functions = baseline_functions
    elif observed_triggers == {**baseline_triggers, **_RR1_TRIGGER_CONTRACTS}:
        profile = PROFILE_RR1
        expected_functions = {**baseline_functions, **_RR1_FUNCTION_CONTRACTS}
    else:
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.triggers")

    # Every protected name is looked up in every profile, so a lingering RR1 guard
    # without its trigger, a half pair or an overload of any name refuses.
    function_names = sorted({
        signature.split("(", 1)[0]
        for signature in (
            *_SHARED_FUNCTION_CONTRACTS, *_PRODUCT_FUNCTION_CONTRACTS, *_RR1_FUNCTION_CONTRACTS
        )
        if include_product or signature not in _PRODUCT_FUNCTION_CONTRACTS
    })
    function_rows = conn.execute(
        "SELECT p.oid::regprocedure::text,pg_get_function_identity_arguments(p.oid),"
        "pg_get_function_arguments(p.oid),p.pronargdefaults,"
        "pg_get_function_result(p.oid),l.lanname,p.provolatile,p.proparallel,"
        "p.prosecdef,p.proisstrict,p.proleakproof,p.prokind,p.proretset,p.proconfig,"
        "pg_get_userbyid(p.proowner),p.oid,p.prosrc "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "JOIN pg_language l ON l.oid=p.prolang "
        "WHERE n.nspname=current_schema() AND p.proname=ANY(%s) "
        "ORDER BY p.oid::regprocedure::text",
        (function_names,),
    ).fetchall()
    observed_functions: dict[str, tuple[Any, ...]] = {}
    function_oids: dict[str, int] = {}
    for row in function_rows:
        signature = row[0]
        definition_hash = _hash_bytes(row[16].encode("utf-8"))
        proconfig = None if row[13] is None else tuple(row[13])
        observed_functions[signature] = (definition_hash, *row[1:13], proconfig, row[14])
        function_oids[signature] = int(row[15])
    if observed_functions != expected_functions:
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.functions")
    _verify_function_acls(conn, function_oids)

    if include_product:
        observed_views = dict(conn.execute(
            "SELECT c.relname, pg_get_viewdef(c.oid, false) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=current_schema() AND c.relkind='v' AND c.relname=ANY(%s)",
            (list(_EXPECTED_VIEW_DEFINITIONS),),
        ).fetchall())
        for view_name, expected in _EXPECTED_VIEW_DEFINITIONS.items():
            if observed_views.get(view_name) != expected:
                _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.view.{view_name}")

    _verify_relation_acls(conn, relation_names)
    return profile


def _schema_state_and_profile(conn: psycopg.Connection) -> tuple[str, str | None]:
    """Schema state plus the admitted shared-ledger extension profile (None if absent)."""
    _verify_schema_privileges(conn)
    _verify_reader_memberships(conn)
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
            return "shared_only", _verify_relation_contracts(conn, include_product=False)
        return "absent", None
    if not all(product) or not all(shared):
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.partial")
    # Includes owner and relation/column ACL admission for all nine relations.
    return "compatible", _verify_relation_contracts(conn, include_product=True)


def _schema_state(conn: psycopg.Connection) -> str:
    return _schema_state_and_profile(conn)[0]


def _require_profile(observed: str | None, required: str | None) -> None:
    """An operation envelope's pinned profile; absence of the ledger never matches."""
    if required is not None and observed != required:
        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.profile")


def _verify_required_roles(conn: psycopg.Connection, roles: Sequence[str]) -> None:
    """The envelope's roles must exist; they are never created here."""
    if not roles:
        return
    present = {
        row[0] for row in conn.execute(
            "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)", (list(roles),)
        ).fetchall()
    }
    for role in roles:
        if role not in present:
            _fail(ErrorCode.SCHEMA_MISMATCH, f"schema.required_role.{role}")


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
    publication = _require_publication(publication, _publication_from_contract(contract))
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


# Every application file the image copies and the CLI executes or reads, including
# the package initializers and the whole eager ``src.bonds`` import closure: the
# CLI import runs ``src/bonds/__init__.py``, which imports the pure bond modules.
# The runtime files (Dockerfile, requirements.lock, railway.toml) carry their own
# release-context fields; the root-only startup wrapper is bound through the
# Dockerfile's literal digest (verified below), not by an inventory entry.
_RELEASE_SOURCE_PATHS = (
    "src/__init__.py",
    "src/db.py",
    "src/bonds/__init__.py",
    "src/bonds/cashflows.py",
    "src/bonds/debt_mapping.py",
    "src/bonds/errors.py",
    "src/bonds/identifiers.py",
    "src/bonds/implied_rating.py",
    "src/bonds/implied_rating_artifact_loader.py",
    "src/bonds/implied_rating_materializer.py",
    "src/bonds/matching.py",
    "src/bonds/oas.py",
    "src/bonds/panel_states.py",
    "src/bonds/pricing.py",
    "src/bonds/states.py",
    "scripts/__init__.py",
    "scripts/load_bond_market_implied_rating_artifact.py",
    "schemas/sec_derived_publications.sql",
    "schemas/bond_market_implied_rating_v1.sql",
    "contracts/bond_market_implied_rating_round002_artifact.json",
    "contracts/bond_market_implied_rating_round002_input_identity.json",
)
_BOOTSTRAP_RELATIVE_PATH = "docker/bond-implied-artifact-loader/bootstrap_evidence.py"
_BOOTSTRAP_LITERAL_RE = re.compile(
    rb'echo "([0-9a-f]{64})  /app/docker/bond-implied-artifact-loader/bootstrap_evidence\.py"'
    rb" \| sha256sum -c -"
)


def _verify_bootstrap_binding(dockerfile_raw: bytes) -> None:
    """The copied startup wrapper must still match the release-bound Dockerfile literal."""
    literals = _BOOTSTRAP_LITERAL_RE.findall(dockerfile_raw)
    if len(literals) != 1:
        _fail(ErrorCode.RECEIPT_FAILURE, "release_context.bootstrap_literal")
    try:
        actual = _hash_bytes((ROOT / PurePosixPath(_BOOTSTRAP_RELATIVE_PATH)).read_bytes())
    except OSError as exc:
        raise ArtifactLoaderError(
            ErrorCode.RECEIPT_FAILURE, field="release_context.bootstrap_sha256"
        ) from exc
    if actual != literals[0].decode("ascii"):
        _fail(ErrorCode.RECEIPT_FAILURE, "release_context.bootstrap_sha256")


# Independent trust root for the external release context.  The expected SHA256 of
# the exact release-context.json bytes is provisioned through protected deployment
# configuration by the authenticated deployer -- never by the evidence writer, a
# file, a CLI flag, an image default or this source.  It is absent by default, so
# apply refuses unless an approval was provisioned.  Local tests that set it prove
# only the comparison, not the operational approval or image binding.
APPROVED_RELEASE_CONTEXT_ENV = "BOND_ARTIFACT_APPROVED_RELEASE_CONTEXT_SHA256"


_APPROVED_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


def _approved_release_context_sha256() -> str:
    """Exactly 64 lowercase hex characters; no whitespace, newline or case folding."""
    value = os.environ.get(APPROVED_RELEASE_CONTEXT_ENV)
    if value is None or _APPROVED_SHA256_RE.match(value) is None:
        _fail(ErrorCode.RECEIPT_FAILURE, "release_context.approval")
    assert value is not None
    return value


def _load_release_evidence(
    evidence_dir: Path, contract: FrozenArtifactContract
) -> ReleaseEvidence:
    # The approval must exist before the file is even opened, and the raw bytes
    # must match it before any of the file's self-attested claims is parsed.
    approved = _approved_release_context_sha256()
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
    if _hash_bytes(raw) != approved:
        _fail(ErrorCode.RECEIPT_FAILURE, "release_context.approval")
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
    runtime_bytes: dict[str, bytes] = {}
    for field, file_path in runtime_checks.items():
        try:
            runtime_bytes[field] = file_path.read_bytes()
        except OSError as exc:
            raise ArtifactLoaderError(
                ErrorCode.RECEIPT_FAILURE, field=f"release_context.{field}"
            ) from exc
        if _sha(document[field], f"release_context.{field}") != _hash_bytes(runtime_bytes[field]):
            _fail(ErrorCode.RECEIPT_FAILURE, f"release_context.{field}")
    _verify_bootstrap_binding(runtime_bytes["dockerfile_sha256"])
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


class _DurableFs:
    """The exact OS primitives receipt durability depends on (substitutable in tests)."""

    open = staticmethod(os.open)
    close = staticmethod(os.close)
    fsync = staticmethod(os.fsync)
    write = staticmethod(os.write)
    read = staticmethod(os.read)
    mkdir = staticmethod(os.mkdir)
    fstat = staticmethod(os.fstat)


_DIRECTORY_FLAGS_NAMES = ("O_RDONLY", "O_DIRECTORY", "O_NOFOLLOW", "O_CLOEXEC")


def _directory_open_flags() -> int:
    """Directory-descriptor flags; refuse (never degrade) where they do not exist."""
    missing = [name for name in _DIRECTORY_FLAGS_NAMES if not hasattr(os, name)]
    if missing or os.open not in os.supports_dir_fd or os.mkdir not in os.supports_dir_fd:
        _fail(ErrorCode.RECEIPT_FAILURE, "receipt.directory_unsupported")
    flags = 0
    for name in _DIRECTORY_FLAGS_NAMES:
        flags |= getattr(os, name)
    return flags


def _open_directory(path: Path | str, flags: int, *, dir_fd: int | None = None) -> int:
    descriptor = _DurableFs.open(str(path), flags, dir_fd=dir_fd)
    try:
        if not stat.S_ISDIR(_DurableFs.fstat(descriptor).st_mode):
            _fail(ErrorCode.RECEIPT_FAILURE, "receipt.directory")
    except BaseException:
        _DurableFs.close(descriptor)
        raise
    return descriptor


def _pin_evidence_directory(evidence_dir: Path, flags: int) -> int:
    """Return a pinned descriptor of an evidence directory whose own entry is durable.

    The parent is pinned first and the leaf is opened relative to it, so both
    descriptors name the same directory entry; symlinks are refused (O_NOFOLLOW).
    For local use a *single* missing leaf (mode 0700) is created under an existing
    parent; missing ancestor chains are refused rather than recursively created.

    Unless the leaf is a mount root on another device -- the pre-mounted production
    ``/evidence`` volume, whose entry this process never creates -- the parent is
    fsynced on *every* admission, not only right after ``mkdir``.  A leaf left by an
    earlier attempt whose parent fsync failed (or that crashed before it) is thus
    never trusted as durable: every retry, including an immediate best-effort
    failure receipt, refuses until the parent sync succeeds.
    """
    name = evidence_dir.name
    if name in {"", ".", ".."}:
        _fail(ErrorCode.RECEIPT_FAILURE, "receipt.directory")
    try:
        parent_fd = _open_directory(evidence_dir.parent, flags)
    except FileNotFoundError as exc:
        raise ArtifactLoaderError(
            ErrorCode.RECEIPT_FAILURE, field="receipt.directory_parent"
        ) from exc
    try:
        created = False
        try:
            directory_fd = _open_directory(name, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            try:
                _DurableFs.mkdir(name, 0o700, dir_fd=parent_fd)
                created = True
            except FileExistsError:
                pass  # created concurrently: its entry is synced below like any leaf
            directory_fd = _open_directory(name, flags, dir_fd=parent_fd)
        try:
            mount_root = (
                _DurableFs.fstat(directory_fd).st_dev != _DurableFs.fstat(parent_fd).st_dev
            )
            if created or not mount_root:
                _DurableFs.fsync(parent_fd)
        except BaseException:
            _DurableFs.close(directory_fd)
            raise
    finally:
        _DurableFs.close(parent_fd)
    return directory_fd


def _persist_receipt(evidence_dir: Path, *, phase: str, payload: bytes) -> ReceiptRef:
    """Write one receipt durably before returning its reference.

    Ordering: pin the directory descriptor (its own entry synced into its parent
    unless it is a pre-mounted volume root), create the uniquely named receipt
    exclusively (mode 0600, no symlink follow) relative to it, write and fsync the
    file, fsync the directory so the new entry itself is durable, then read the
    exact bytes back.  Only then is a ReceiptRef returned, so a caller can never
    commit on a receipt whose directory entry could vanish on crash.  Every
    failure -- including unsupported directory fsync -- is RECEIPT_FAILURE; every
    descriptor is closed on every path.  This is the filesystem's durability
    ordering guarantee, not a claim about hardware beyond it.
    """
    basename = (
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
        f"-{phase}-{uuid4()}.json"
    )
    try:
        flags = _directory_open_flags()
        directory_fd = _pin_evidence_directory(evidence_dir, flags)
        try:
            file_fd = _DurableFs.open(
                basename,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
                dir_fd=directory_fd,
            )
            try:
                view = memoryview(payload)
                while view:
                    written = _DurableFs.write(file_fd, view)
                    if written <= 0:
                        _fail(ErrorCode.RECEIPT_FAILURE, "receipt.write")
                    view = view[written:]
                _DurableFs.fsync(file_fd)
            finally:
                _DurableFs.close(file_fd)
            _DurableFs.fsync(directory_fd)
            read_fd = _DurableFs.open(
                basename, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory_fd
            )
            try:
                chunks: list[bytes] = []
                remaining = len(payload) + 1
                while remaining > 0:
                    chunk = _DurableFs.read(read_fd, remaining)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                readback = b"".join(chunks)
            finally:
                _DurableFs.close(read_fd)
        finally:
            _DurableFs.close(directory_fd)
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


def _validated_artifact_frame(
    artifact: VerifiedArtifact, contract: FrozenArtifactContract
) -> pd.DataFrame:
    """Bind the complete table content and the full summary to the contract.

    Arrow admission plus the full canonical-frame validation bind every row to the
    contract (counts, window, histogram, rows digest), and the re-derived summary
    -- including null counts -- must equal the supplied one exactly.  Nothing is
    allocated for writing here; callers that only read never build a payload.
    """
    _admit_arrow(artifact.table, contract)
    frame = _to_canonical_frame(artifact.table)
    summary = _validate_frame(frame, contract)
    if not _strict_same(_summary_values(summary), _summary_values(artifact.summary)):
        _fail(ErrorCode.IDENTITY_MISMATCH, "artifact.summary")
    return frame


def _admit_read_only_content(
    artifact: VerifiedArtifact, contract: FrozenArtifactContract
) -> None:
    """Read-only operations bind the full content before any connection or receipt."""
    _validated_artifact_frame(artifact, contract)


def _artifact_payload(
    artifact: VerifiedArtifact, contract: FrozenArtifactContract
) -> list[tuple[Any, ...]]:
    """Admit the table content once and build the INSERT payload from it.

    The single full-frame validation both binds the rows to the contract (counts,
    window, histogram, rows digest) and re-derives the complete summary, which
    must equal the supplied one; rows are stamped with the contract publication.
    """
    frame = _validated_artifact_frame(artifact, contract)
    publication = _publication_from_contract(contract)
    payload: list[tuple[Any, ...]] = []
    for start in range(0, len(frame), ROW_BATCH):
        payload.extend(publication_row_tuples(
            publication, frame.iloc[start:start + ROW_BATCH]
        ))
    return payload


def _contract_from_artifact(artifact: VerifiedArtifact) -> FrozenArtifactContract:
    raw = _read_contract_bytes()
    contract = _parse_contract(raw)
    supplied = getattr(artifact, "contract_sha256", None)
    if type(supplied) is not str or _hash_bytes(raw) != supplied:
        _fail(ErrorCode.CONTRACT_INVALID, "contract.changed")
    return contract


def _admit_operation_artifact(
    artifact: VerifiedArtifact,
) -> tuple[FrozenArtifactContract, VerifiedArtifact]:
    """Public-entrypoint boundary: checked-in contract bytes, then metadata rebinding."""
    contract = _contract_from_artifact(artifact)
    return contract, _bind_verified_artifact(artifact, contract)


def _admit_read_only_operation_artifact(
    artifact: VerifiedArtifact,
) -> tuple[FrozenArtifactContract, VerifiedArtifact]:
    """Read-only public boundary: contract, metadata and the complete table content."""
    contract, admitted = _admit_operation_artifact(artifact)
    _admit_read_only_content(admitted, contract)
    return contract, admitted


def _read_only_operation(
    artifact: VerifiedArtifact,
    *,
    contract: FrozenArtifactContract,
    connection_factory: Callable[[], psycopg.Connection],
    require_published: bool,
    required_profile: str | None = None,
    required_roles: Sequence[str] = (),
    content_admitted: bool = False,
) -> OperationResult:
    artifact = _bind_verified_artifact(artifact, contract)
    if not content_admitted:
        # Full content binding before the connection: a swapped table can never
        # reach a read-only verdict, even through the private entrypoint.
        _admit_read_only_content(artifact, contract)
    with _worker_connection(connection_factory) as conn, conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY")
        _set_local_timeouts(conn)
        _acquire_product_lock(conn)
        _verify_required_roles(conn, required_roles)
        schema, profile = _schema_state_and_profile(conn)
        _require_profile(profile, required_profile)
        first_parent = _verify_parent(conn, contract=contract, lock_pointer=False)

        def recheck_schema() -> None:
            # READ COMMITTED sees concurrent DDL: the admitted schema and extension
            # profile must still hold when the read-only verdict is formed, and so
            # must the genuine worker session.
            if _schema_state_and_profile(conn) != (schema, profile):
                _fail(ErrorCode.SCHEMA_MISMATCH, "schema.profile_changed")
            _verify_worker_session(conn)

        def verdict(outcome: str, stored: StoredEvidence | None) -> OperationResult:
            # The one exit of every successful read-only outcome, the early
            # absent/shared-only returns included: the parent is read again in this
            # READ COMMITTED read-only transaction and must equal the first read
            # exactly (pointer, header digest, server), then the admitted schema,
            # profile and genuine session must still hold.  This bounds the verdict
            # to the read; it reserves nothing for a later apply, which keeps its
            # own locks and rechecks.
            second_parent = _verify_parent(conn, contract=contract, lock_pointer=False)
            if first_parent != second_parent:
                _fail(ErrorCode.PARENT_MISMATCH, "parent.race")
            recheck_schema()
            return OperationResult(
                outcome, artifact.publication.publication_id, False, stored, first_parent, ()
            )

        if schema in {"absent", "shared_only"}:
            if (
                schema == "shared_only"
                and _publication_state(conn, artifact.publication) is not PublicationState.ABSENT
            ):
                _fail(ErrorCode.PUBLICATION_CONFLICT, "publication.partial_state")
            return verdict(
                "not_published" if require_published
                else "dry_run_verified_schema_install_required",
                None,
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
        return verdict(outcome, stored)


def _ensure_schema_profile(
    contract: FrozenArtifactContract,
    connection_factory: Callable[[], psycopg.Connection],
    *,
    required_profile: str | None = None,
    required_roles: Sequence[str] = (),
) -> tuple[bool, str]:
    """Install the reviewed DDL when needed; return (installed, extension profile).

    Existing state -- shared or product -- is fully admitted (including ACLs and
    the extension profile) *before* any DDL runs, so installation never repairs
    an unsafe pre-existing state.  The install itself must not change the
    profile of a pre-existing ledger.
    """
    with _worker_connection(connection_factory) as conn, conn.transaction():
        _set_local_timeouts(conn)
        _acquire_product_lock(conn)
        _verify_parent(conn, contract=contract, lock_pointer=True)
        _verify_required_roles(conn, required_roles)
        state, profile = _schema_state_and_profile(conn)
        _require_profile(profile, required_profile)
        if state == "compatible":
            assert profile is not None
            return False, profile
        if state == "shared_only":
            assert contract.identity is not None
            if (
                _publication_state(conn, contract.identity.publication_id)
                is not PublicationState.ABSENT
            ):
                _fail(ErrorCode.PUBLICATION_CONFLICT, "publication.partial_state")
        install_schema(conn)
        # The reviewed DDL must not have changed the session identity either.
        _verify_worker_session(conn)
        installed_state, installed_profile = _schema_state_and_profile(conn)
        if installed_state != "compatible" or installed_profile is None:
            _fail(ErrorCode.SCHEMA_MISMATCH, "schema.install")
        if profile is not None and installed_profile != profile:
            _fail(ErrorCode.SCHEMA_MISMATCH, "schema.profile_changed")
        _require_profile(installed_profile, required_profile)
        # Recheck the genuine worker session before the schema transaction commits.
        _verify_worker_session(conn)
        return True, installed_profile


def _ensure_schema(
    contract: FrozenArtifactContract,
    connection_factory: Callable[[], psycopg.Connection],
) -> bool:
    return _ensure_schema_profile(contract, connection_factory)[0]


def _publish_verified_artifact(
    artifact: VerifiedArtifact,
    *,
    contract: FrozenArtifactContract,
    connection_factory: Callable[[], psycopg.Connection],
    evidence_dir: Path,
    release: ReleaseEvidence | None = None,
    operation_id: str | None = None,
    operation_started_at_utc: str | None = None,
    required_profile: str | None = None,
    required_roles: Sequence[str] = (),
) -> OperationResult:
    operation_id = operation_id or str(uuid4())
    operation_started_at_utc = (
        operation_started_at_utc or datetime.now(timezone.utc).isoformat()
    )
    # Admission of caller-supplied metadata and of the full table content happens
    # before the pre-apply success receipt and before any database connection.
    admitted_publication_id: str | None = None
    try:
        admitted_publication_id = _publication_from_contract(contract).publication_id
        artifact = _bind_verified_artifact(artifact, contract)
    except ArtifactLoaderError as exc:
        exc.phase = exc.phase or "artifact_binding"
        exc.transaction_outcome = exc.transaction_outcome or "not_started"
        if not exc.receipt_written:
            _best_effort_failure_receipt(
                exc,
                evidence_dir,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
                mode="apply",
                publication_id=admitted_publication_id,
            )
        raise
    try:
        payload = _artifact_payload(artifact, contract)
    except ArtifactLoaderError as exc:
        exc.phase = "payload"
        exc.schema_installed = False
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
        # The extension profile observed here is held for the whole operation:
        # any later observation must match it exactly.
        schema_installed, profile = _ensure_schema_profile(
            contract,
            connection_factory,
            required_profile=required_profile,
            required_roles=required_roles,
        )
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
    except psycopg.errors.LockNotAvailable as exc:
        # Parent-pointer FOR SHARE or DDL relation contention during schema setup is
        # the same retryable lock timeout as in the publication transaction.
        primary = ArtifactLoaderError(
            ErrorCode.LOCK_TIMEOUT,
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
                # A substituted publication connection is refused before any lock,
                # read or write; nothing has started, and a schema transaction that
                # already committed on a genuine connection stays committed.
                _verify_worker_session(conn)
            except ArtifactLoaderError as exc:
                exc.phase = "transaction_setup"
                exc.schema_installed = schema_installed
                exc.transaction_outcome = "not_started"
                _best_effort_failure_receipt(
                    exc,
                    evidence_dir,
                    operation_id=operation_id,
                    operation_started_at_utc=operation_started_at_utc,
                    mode="apply",
                    publication_id=artifact.publication.publication_id,
                )
                raise
            try:
                with conn.transaction():
                    _set_local_timeouts(conn)
                    _acquire_product_lock(conn)
                    transaction_phase = "parent_preflight"
                    parent_evidence = _verify_parent(
                        conn, contract=contract, lock_pointer=True
                    )
                    transaction_phase = "schema_recheck"
                    recheck_state, recheck_profile = _schema_state_and_profile(conn)
                    if recheck_state != "compatible":
                        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.changed")
                    if recheck_profile != profile:
                        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.profile_changed")
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
                    transaction_phase = "schema_precommit"
                    if _schema_state_and_profile(conn) != ("compatible", profile):
                        _fail(ErrorCode.SCHEMA_MISMATCH, "schema.profile_changed")
                    transaction_phase = "session_precommit"
                    _verify_worker_session(conn)
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
            required_profile=profile,
            required_roles=required_roles,
            # Admitted by the full-frame payload validation before any connection.
            content_admitted=True,
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
    # Failure receipts carry only a contract-admitted publication identity, never
    # the caller-supplied one.
    publication_id: str | None = None
    try:
        contract, admitted = _admit_operation_artifact(artifact)
        publication_id = admitted.publication.publication_id
        phase = "release_context"
        release = _load_release_evidence(evidence_dir, contract)
        phase = "publish"
        return _publish_verified_artifact(
            admitted,
            contract=contract,
            connection_factory=connection_factory,
            evidence_dir=evidence_dir,
            release=release,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            required_profile=_PRODUCTION_REQUIRED_PROFILE,
            required_roles=_PRODUCTION_REQUIRED_ROLES,
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
                publication_id=publication_id,
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
            publication_id=publication_id,
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
            publication_id=publication_id,
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
    operation_id = operation_id or str(uuid4())
    operation_started_at_utc = (
        operation_started_at_utc or datetime.now(timezone.utc).isoformat()
    )
    phase = "artifact_binding"
    publication_id: str | None = None
    try:
        # Contract, metadata and full table content are all admitted before any
        # connection or success receipt; no INSERT payload is ever built here.
        contract, artifact = _admit_read_only_operation_artifact(artifact)
        publication_id = artifact.publication.publication_id
        phase = "dry_run_read"
        result = _read_only_operation(
            artifact,
            contract=contract,
            connection_factory=connection_factory,
            require_published=False,
            required_profile=_PRODUCTION_REQUIRED_PROFILE,
            required_roles=_PRODUCTION_REQUIRED_ROLES,
            content_admitted=True,
        )
    except ArtifactLoaderError as exc:
        schema_installed = (
            exc.schema_installed if exc.schema_installed is not None else False
        )
        transaction_outcome = exc.transaction_outcome or (
            "not_started" if phase == "artifact_binding" else "read_only"
        )
        exc.phase = exc.phase or phase
        exc.schema_installed = schema_installed
        exc.transaction_outcome = transaction_outcome
        if not exc.receipt_written:
            _best_effort_failure_receipt(
                exc,
                evidence_dir,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
                mode="dry-run",
                publication_id=publication_id,
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
            publication_id=publication_id,
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
    operation_id = operation_id or str(uuid4())
    operation_started_at_utc = (
        operation_started_at_utc or datetime.now(timezone.utc).isoformat()
    )
    phase = "artifact_binding"
    publication_id: str | None = None
    try:
        # Recovery never trusts a table it has not bound to the contract: full
        # content admission precedes the connection and the recovery receipt.
        contract, artifact = _admit_read_only_operation_artifact(artifact)
        publication_id = artifact.publication.publication_id
        phase = "verify_published_read"
        result = _read_only_operation(
            artifact,
            contract=contract,
            connection_factory=connection_factory,
            require_published=True,
            required_profile=_PRODUCTION_REQUIRED_PROFILE,
            required_roles=_PRODUCTION_REQUIRED_ROLES,
            content_admitted=True,
        )
    except ArtifactLoaderError as exc:
        schema_installed = (
            exc.schema_installed if exc.schema_installed is not None else False
        )
        transaction_outcome = exc.transaction_outcome or (
            "not_started" if phase == "artifact_binding" else "read_only"
        )
        exc.phase = exc.phase or phase
        exc.schema_installed = schema_installed
        exc.transaction_outcome = transaction_outcome
        if not exc.receipt_written:
            _best_effort_failure_receipt(
                exc,
                evidence_dir,
                operation_id=operation_id,
                operation_started_at_utc=operation_started_at_utc,
                mode="verify-published",
                publication_id=publication_id,
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
            publication_id=publication_id,
        )
        raise primary from exc
    # The read-only verification is complete; only its durable evidence is left.
    # If that receipt cannot be made durable, the refusal still reports what the
    # read observed.  An exact current publication is committed state with
    # incomplete evidence (recovery required -- never a reason to apply again);
    # this reports what was observed, not a commit made here.  Any other verdict
    # (not published, validated but unpointed) is a read-only receipt failure and
    # never claims a current publication.  Either way recovery wrote nothing to
    # the database, and no success is returned without the durable receipt.
    try:
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
    except (ArtifactLoaderError, OSError) as exc:
        verified_current = result.outcome == "already_published_verified"
        primary = ArtifactLoaderError(
            ErrorCode.COMMITTED_EVIDENCE_INCOMPLETE if verified_current
            else ErrorCode.RECEIPT_FAILURE,
            field="receipt.recovery_readback",
            phase="recovery_receipt",
            schema_installed=False,
            transaction_outcome="read_only",
            outcome="recovery_required" if verified_current else "failed",
        )
        # A failure-receipt failure only clears ``receipt_written``; the primary
        # classification above is what is raised.
        _best_effort_failure_receipt(
            primary,
            evidence_dir,
            operation_id=operation_id,
            operation_started_at_utc=operation_started_at_utc,
            mode="verify-published",
            publication_id=publication_id,
        )
        raise primary from exc
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
