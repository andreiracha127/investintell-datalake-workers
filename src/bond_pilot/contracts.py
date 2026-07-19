"""Immutable internal data contracts for the bond-pilot experiment."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import re
from typing import Any, Mapping


class PilotError(ValueError):
    """A domain error whose stable code is safe for tests and callers to inspect."""

    def __init__(self, code: str, details: Mapping[str, Any] | None = None) -> None:
        self.code = code
        self.details = dict(details or {})
        super().__init__(f"{code}: {self.details}" if self.details else code)


class FieldState(StrEnum):
    PRESENT = "present"
    NULL = "null"
    INVALID = "invalid"
    NOT_IN_SCHEMA = "not_in_schema"


class IdentifierState(StrEnum):
    VALID_CUSIP9 = "valid_cusip9"
    BLANK = "blank"
    PLACEHOLDER = "placeholder"
    SYNTHETIC = "synthetic"
    INVALID_FORMAT = "invalid_format"


class DebtState(StrEnum):
    DEBT_LIKE_ELIGIBLE = "debt_like_eligible"
    INELIGIBLE_NON_DEBT = "ineligible_non_debt"
    AMBIGUOUS_CATEGORY = "ambiguous_category"
    MISSING_CATEGORY = "missing_category"


class MatchState(StrEnum):
    INELIGIBLE_NON_DEBT = "ineligible_non_debt"
    AMBIGUOUS_CATEGORY = "ambiguous_category"
    MISSING_CATEGORY = "missing_category"
    INVALID_IDENTIFIER = "invalid_identifier"
    OUTSIDE_WINDOW_BEFORE_SOURCE = "outside_window_before_source"
    OUTSIDE_WINDOW_AFTER_CUTOFF = "outside_window_after_cutoff"
    UNMATCHED_NO_CUSIP = "unmatched_no_cusip"
    UNMATCHED_NO_PRIOR_OBSERVATION = "unmatched_no_prior_observation"
    STALE = "stale"
    UNAVAILABLE_AMBIGUOUS = "unavailable_ambiguous"
    MATCHED = "matched"


@dataclass(frozen=True)
class ArtifactLimits:
    archive_bytes: int = 5 * 1024**3
    member_uncompressed_bytes: int = 10 * 1024**3
    streaming_chunk_bytes: int = 1024**2

    @property
    def max_archive_bytes(self) -> int:
        return self.archive_bytes

    @property
    def max_member_uncompressed_bytes(self) -> int:
        return self.member_uncompressed_bytes


def _require_nonempty(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PilotError(f"invalid_{field}", {"field": field})
    return value


def _require_sha256(value: object, field: str) -> str:
    value = _require_nonempty(value, field)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise PilotError(f"invalid_{field}", {"field": field})
    return value


def _require_bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise PilotError(f"invalid_{field}", {"field": field})
    return value


def _require_nonnegative(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise PilotError(f"invalid_{field}", {"field": field})
    return value


def _require_strings(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or any(not isinstance(item, str) or not item for item in value):
        raise PilotError(f"invalid_{field}", {"field": field})
    return tuple(value)


_RFC3339_UTC_SECONDS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


def _require_rfc3339_utc_timestamp(value: object, field: str) -> str:
    value = _require_nonempty(value, field)
    if not _RFC3339_UTC_SECONDS.fullmatch(value):
        raise PilotError(f"invalid_{field}", {"field": field})
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise PilotError(f"invalid_{field}", {"field": field}) from exc
    return value


@dataclass(frozen=True)
class SourceCandidate:
    schema_version: str
    source_locator: str
    local_archive_path: str
    local_extracted_path: str
    artifact_bytes: int
    artifact_sha256: str
    member_name: str
    member_uncompressed_bytes: int
    schema_sha256: str
    schema_columns: tuple[str, ...]
    schema_optional_columns: tuple[str, ...]
    row_count: int
    row_group_count: int
    global_start: str
    global_cutoff: str
    duplicate_check_scope: str
    approval_state: str = "unapproved"

    def __post_init__(self) -> None:
        if self.schema_version != "source-candidate-v1":
            raise PilotError("invalid_schema_version", {"field": "schema_version"})
        for field in (
            "source_locator",
            "local_archive_path",
            "local_extracted_path",
            "member_name",
            "global_start",
            "global_cutoff",
            "duplicate_check_scope",
        ):
            _require_nonempty(getattr(self, field), field)
        _require_sha256(self.artifact_sha256, "artifact_sha256")
        _require_sha256(self.schema_sha256, "schema_sha256")
        for field in ("artifact_bytes", "member_uncompressed_bytes", "row_count", "row_group_count"):
            _require_nonnegative(getattr(self, field), field)
        object.__setattr__(self, "schema_columns", _require_strings(self.schema_columns, "schema_columns"))
        object.__setattr__(self, "schema_optional_columns", _require_strings(self.schema_optional_columns, "schema_optional_columns"))
        if self.approval_state != "unapproved":
            raise PilotError("invalid_approval_state", {"field": "approval_state"})

    def to_json_mapping(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_locator": self.source_locator,
            "local_archive_path": self.local_archive_path,
            "local_extracted_path": self.local_extracted_path,
            "artifact_bytes": self.artifact_bytes,
            "artifact_sha256": self.artifact_sha256,
            "member_name": self.member_name,
            "member_uncompressed_bytes": self.member_uncompressed_bytes,
            "schema_sha256": self.schema_sha256,
            "schema_columns": list(self.schema_columns),
            "schema_optional_columns": list(self.schema_optional_columns),
            "row_count": self.row_count,
            "row_group_count": self.row_group_count,
            "global_start": self.global_start,
            "global_cutoff": self.global_cutoff,
            "duplicate_check_scope": self.duplicate_check_scope,
            "approval_state": self.approval_state,
        }

    @classmethod
    def from_json_mapping(cls, value: Mapping[str, object]) -> SourceCandidate:
        try:
            return cls(
                schema_version=value["schema_version"],
                source_locator=value["source_locator"],
                local_archive_path=value["local_archive_path"],
                local_extracted_path=value["local_extracted_path"],
                artifact_bytes=value["artifact_bytes"],
                artifact_sha256=value["artifact_sha256"],
                member_name=value["member_name"],
                member_uncompressed_bytes=value["member_uncompressed_bytes"],
                schema_sha256=value["schema_sha256"],
                schema_columns=_require_strings(value["schema_columns"], "schema_columns"),
                schema_optional_columns=_require_strings(value["schema_optional_columns"], "schema_optional_columns"),
                row_count=value["row_count"],
                row_group_count=value["row_group_count"],
                global_start=value["global_start"],
                global_cutoff=value["global_cutoff"],
                duplicate_check_scope=value["duplicate_check_scope"],
                approval_state=value.get("approval_state", "unapproved"),
            )
        except KeyError as exc:
            raise PilotError("missing_source_candidate_field", {"field": exc.args[0]}) from exc


@dataclass(frozen=True)
class SourceApproval:
    schema_version: str
    source_locator: str
    artifact_sha256: str
    schema_sha256: str
    cutoff: str
    terms_evidence: str
    local_use_allowed: bool
    redistribution_allowed: bool
    approved_by: str
    approved_at: str

    def __post_init__(self) -> None:
        if self.schema_version != "source-approval-v1":
            raise PilotError("invalid_schema_version", {"field": "schema_version"})
        for field in ("source_locator", "cutoff", "terms_evidence", "approved_by"):
            _require_nonempty(getattr(self, field), field)
        _require_rfc3339_utc_timestamp(self.approved_at, "approved_at")
        _require_sha256(self.artifact_sha256, "artifact_sha256")
        _require_sha256(self.schema_sha256, "schema_sha256")
        if self.local_use_allowed is not True:
            raise PilotError("invalid_local_use_allowed", {"field": "local_use_allowed"})
        _require_bool(self.redistribution_allowed, "redistribution_allowed")

    @classmethod
    def from_json_mapping(cls, value: Mapping[str, object]) -> SourceApproval:
        try:
            return cls(
                schema_version=value["schema_version"],
                source_locator=value["source_locator"],
                artifact_sha256=value["artifact_sha256"],
                schema_sha256=value["schema_sha256"],
                cutoff=value["cutoff"],
                terms_evidence=value["terms_evidence"],
                local_use_allowed=value["local_use_allowed"],
                redistribution_allowed=value["redistribution_allowed"],
                approved_by=value["approved_by"],
                approved_at=value["approved_at"],
            )
        except KeyError as exc:
            raise PilotError("missing_source_approval_field", {"field": exc.args[0]}) from exc

    def to_json_mapping(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_locator": self.source_locator,
            "artifact_sha256": self.artifact_sha256,
            "schema_sha256": self.schema_sha256,
            "cutoff": self.cutoff,
            "terms_evidence": self.terms_evidence,
            "local_use_allowed": self.local_use_allowed,
            "redistribution_allowed": self.redistribution_allowed,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at,
        }

    def validate_for(self, candidate: SourceCandidate) -> None:
        for field, candidate_value in (
            ("source_locator", candidate.source_locator),
            ("artifact_sha256", candidate.artifact_sha256),
            ("schema_sha256", candidate.schema_sha256),
            ("cutoff", candidate.global_cutoff),
        ):
            if getattr(self, field) != candidate_value:
                raise PilotError(f"{field}_mismatch", {"field": field})

    def validate_against(self, candidate: SourceCandidate) -> None:
        self.validate_for(candidate)
