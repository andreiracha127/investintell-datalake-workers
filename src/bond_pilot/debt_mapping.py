"""Explicit, approval-bound debt-category mappings for the pilot."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Mapping

from .contracts import DebtState, PilotError


_OUTCOMES = frozenset(
    {
        DebtState.DEBT_LIKE_ELIGIBLE.value,
        DebtState.INELIGIBLE_NON_DEBT.value,
        DebtState.AMBIGUOUS_CATEGORY.value,
    }
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _unapproved() -> PilotError:
    return PilotError("debt_mapping_unapproved")


def _sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def _read_mapping(path: str | Path) -> tuple[Path, bytes, dict[str, object]]:
    try:
        mapping_path = Path(path)
        raw = mapping_path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _unapproved() from exc
    if not isinstance(value, dict):
        raise _unapproved()
    return mapping_path, raw, value


def _categories(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or not value:
        raise _unapproved()
    if any(not isinstance(category, str) or not category or outcome not in _OUTCOMES for category, outcome in value.items()):
        raise _unapproved()
    return dict(value)


@dataclass(frozen=True)
class DebtMapping:
    """Exact category mapping; it intentionally performs no category normalization."""

    schema_version: str
    mapping_version: str
    scope: str
    categories: Mapping[str, str]
    observed_values_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mapping_version, str) or not self.mapping_version.strip():
            raise _unapproved()
        if self.schema_version not in {"debt-mapping-test-v1", "debt-mapping-v1"}:
            raise _unapproved()
        if self.schema_version == "debt-mapping-test-v1" and self.scope != "synthetic_fixture_only":
            raise _unapproved()
        if self.schema_version == "debt-mapping-v1" and self.scope != "approved_external":
            raise _unapproved()
        if self.schema_version == "debt-mapping-v1" and not _sha256(self.observed_values_sha256):
            raise _unapproved()
        object.__setattr__(self, "categories", MappingProxyType(_categories(self.categories)))

    def classify(self, category: object) -> DebtState:
        if category is None or (isinstance(category, str) and not category.strip()):
            return DebtState.MISSING_CATEGORY
        if not isinstance(category, str):
            return DebtState.AMBIGUOUS_CATEGORY
        value = self.categories.get(category)
        return DebtState(value) if value is not None else DebtState.AMBIGUOUS_CATEGORY

    def to_mapping(self) -> dict[str, object]:
        value: dict[str, object] = {
            "schema_version": self.schema_version,
            "mapping_version": self.mapping_version,
            "scope": self.scope,
            "categories": dict(self.categories),
        }
        if self.observed_values_sha256 is not None:
            value["observed_values_sha256"] = self.observed_values_sha256
        return value


def load_fixture_debt_mapping(mapping_path: str | Path) -> DebtMapping:
    """Load the one deliberately synthetic fixture mapping used by unit tests."""
    _, _, value = _read_mapping(mapping_path)
    expected = {
        "schema_version": "debt-mapping-test-v1",
        "mapping_version": "synthetic-test-v1",
        "scope": "synthetic_fixture_only",
        "categories": {
            "fixture_debt": "debt_like_eligible",
            "fixture_non_debt": "ineligible_non_debt",
            "fixture_ambiguous": "ambiguous_category",
        },
    }
    if value != expected:
        raise _unapproved()
    return DebtMapping(**value)


def _valid_evidence(value: object) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(
            isinstance(item, dict)
            and set(item) == {"reference", "sha256"}
            and isinstance(item["reference"], str)
            and bool(item["reference"].strip())
            and _sha256(item["sha256"])
            for item in value
        )
    )


def _valid_timestamp(value: object) -> bool:
    if not isinstance(value, str) or _UTC_TIMESTAMP.fullmatch(value) is None:
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return True


def load_approved_debt_mapping(mapping_path: str | Path, approval_path: str | Path) -> DebtMapping:
    """Load a calibrated mapping only when a separate approval binds every input hash."""
    _, raw_mapping, mapping = _read_mapping(mapping_path)
    _, _, approval = _read_mapping(approval_path)
    required_mapping = {"schema_version", "mapping_version", "observed_values_sha256", "categories"}
    required_approval = {
        "schema_version",
        "mapping_sha256",
        "observed_values_sha256",
        "evidence",
        "approved_by",
        "approved_at",
    }
    if set(mapping) != required_mapping or set(approval) != required_approval:
        raise _unapproved()
    if mapping["schema_version"] != "debt-mapping-v1" or approval["schema_version"] != "debt-mapping-approval-v1":
        raise _unapproved()
    if not isinstance(mapping["mapping_version"], str) or not mapping["mapping_version"].strip():
        raise _unapproved()
    if not _sha256(mapping["observed_values_sha256"]):
        raise _unapproved()
    if approval["mapping_sha256"] != hashlib.sha256(raw_mapping).hexdigest():
        raise _unapproved()
    if approval["observed_values_sha256"] != mapping["observed_values_sha256"]:
        raise _unapproved()
    if not _valid_evidence(approval["evidence"]):
        raise _unapproved()
    if not isinstance(approval["approved_by"], str) or not approval["approved_by"].strip() or not _valid_timestamp(approval["approved_at"]):
        raise _unapproved()
    return DebtMapping(
        schema_version="debt-mapping-v1",
        mapping_version=mapping["mapping_version"],
        scope="approved_external",
        categories=_categories(mapping["categories"]),
        observed_values_sha256=mapping["observed_values_sha256"],
    )
