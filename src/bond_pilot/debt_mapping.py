"""Exact, approval-bound composite debt decisions for the pilot."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Mapping, Sequence

from .artifacts import read_secure_local_file
from .contracts import DebtState, PilotError


_RULE_FIELDS = ("issuer_category", "asset_class", "instrument_structure")
_RULE_KEYS = frozenset({*_RULE_FIELDS, "decision"})
_DECISIONS = {
    "eligible_debt": DebtState.DEBT_LIKE_ELIGIBLE,
    "non_debt_excluded": DebtState.INELIGIBLE_NON_DEBT,
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_VALIDATION_TOKEN = object()
_MAX_MAPPING_BYTES = 1024 * 1024


def _unapproved() -> PilotError:
    return PilotError("debt_mapping_unapproved")


def _sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _unapproved()
        value[key] = item
    return value


def _read_mapping(path: str | Path) -> tuple[Path, bytes, dict[str, object]]:
    def nonfinite(_value: str) -> object:
        raise _unapproved()

    try:
        mapping_path, raw = read_secure_local_file(path, max_bytes=_MAX_MAPPING_BYTES, error_code="debt_mapping_unapproved")
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_keys, parse_constant=nonfinite)
    except PilotError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _unapproved() from exc
    if not isinstance(value, dict):
        raise _unapproved()
    return mapping_path, raw, value


def _rules(value: object) -> tuple[tuple[str, str, str, str], ...]:
    if not isinstance(value, list) or not value:
        raise _unapproved()
    output: list[tuple[str, str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for rule in value:
        if not isinstance(rule, dict) or set(rule) != _RULE_KEYS:
            raise _unapproved()
        components = tuple(rule[field] for field in _RULE_FIELDS)
        decision = rule["decision"]
        if any(not isinstance(component, str) or component == "" for component in components) or decision not in _DECISIONS:
            raise _unapproved()
        key = components  # type: ignore[assignment]
        if key in seen:
            raise _unapproved()
        seen.add(key)
        output.append((*components, decision))  # type: ignore[arg-type]
    return tuple(output)


def _freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _canonical_json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


@dataclass(frozen=True)
class DebtMapping:
    """Exact triple lookup with no aliases, normalization, or inferred decisions."""

    schema_version: str
    mapping_version: str
    scope: str
    rules: Sequence[object]
    observed_composite_values_sha256: str | None = None
    mapping_sha256: str | None = None
    approval_sha256: str | None = None
    approval_manifest: Mapping[str, object] | None = None
    approval_canonical_json: bytes | None = field(default=None, repr=False, compare=False)
    _validation_token: object = field(default=None, repr=False, compare=False)
    _lookup: Mapping[tuple[str, str, str], DebtState] = field(default_factory=dict, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._validation_token is not _VALIDATION_TOKEN:
            raise _unapproved()
        if not isinstance(self.mapping_version, str) or not self.mapping_version.strip() or self.schema_version not in {"debt-mapping-test-v2", "debt-mapping-v2"}:
            raise _unapproved()
        if self.schema_version == "debt-mapping-test-v2" and self.scope != "synthetic_fixture_only":
            raise _unapproved()
        if self.schema_version == "debt-mapping-v2" and (self.scope != "approved_external" or not _sha256(self.observed_composite_values_sha256)):
            raise _unapproved()
        if not _sha256(self.mapping_sha256):
            raise _unapproved()
        raw_rules = list(self.rules)
        if raw_rules and all(isinstance(rule, dict) for rule in raw_rules):
            normalized = _rules(raw_rules)
        else:
            try:
                normalized = _rules([dict(zip((*_RULE_FIELDS, "decision"), rule, strict=True)) for rule in raw_rules])
            except (TypeError, ValueError) as exc:
                raise _unapproved() from exc
        object.__setattr__(self, "rules", normalized)
        object.__setattr__(self, "_lookup", MappingProxyType({rule[:3]: _DECISIONS[rule[3]] for rule in normalized}))
        if self.schema_version == "debt-mapping-test-v2":
            if any(value is not None for value in (self.approval_sha256, self.approval_manifest, self.approval_canonical_json)):
                raise _unapproved()
        elif not _sha256(self.approval_sha256) or not isinstance(self.approval_manifest, Mapping) or not isinstance(self.approval_canonical_json, bytes):
            raise _unapproved()
        else:
            frozen = _freeze(self.approval_manifest)
            if _canonical_json(_thaw(frozen)) != self.approval_canonical_json:
                raise _unapproved()
            object.__setattr__(self, "approval_manifest", frozen)

    def classify(self, issuer_category: object, asset_class: object, instrument_structure: object) -> DebtState:
        components = (issuer_category, asset_class, instrument_structure)
        if any(component is None or component == "" for component in components):
            return DebtState.MISSING_CATEGORY
        if not all(isinstance(component, str) for component in components):
            return DebtState.AMBIGUOUS_CATEGORY
        return self._lookup.get(components, DebtState.AMBIGUOUS_CATEGORY)

    def to_mapping(self) -> dict[str, object]:
        value: dict[str, object] = {
            "schema_version": self.schema_version,
            "mapping_version": self.mapping_version,
            "scope": self.scope,
            "rules": [dict(zip((*_RULE_FIELDS, "decision"), rule, strict=True)) for rule in self.rules],
            "mapping_sha256": self.mapping_sha256,
        }
        if self.observed_composite_values_sha256 is not None:
            value["observed_composite_values_sha256"] = self.observed_composite_values_sha256
        if self.approval_manifest is not None:
            value["approval_sha256"] = self.approval_sha256
            value["approval"] = _thaw(self.approval_manifest)
        return value


def _is_loader_validated(mapping: object) -> bool:
    return isinstance(mapping, DebtMapping) and mapping._validation_token is _VALIDATION_TOKEN


def load_fixture_debt_mapping(mapping_path: str | Path) -> DebtMapping:
    """Load the one deliberately synthetic composite decision table used by tests."""
    _, raw, value = _read_mapping(mapping_path)
    expected_rules = (
        ("fixture_debt", "fixture_asset", "fixture_structure", "eligible_debt"),
        ("fixture_non_debt", "fixture_asset", "fixture_structure", "non_debt_excluded"),
        *( (f"synthetic_{family}", "synthetic_asset", "synthetic_structure", "non_debt_excluded") for family in ("mbs", "abs", "clo", "loan", "repo") ),
    )
    expected = {"schema_version": "debt-mapping-test-v2", "mapping_version": "synthetic-test-v2", "scope": "synthetic_fixture_only", "rules": [dict(zip((*_RULE_FIELDS, "decision"), rule, strict=True)) for rule in expected_rules]}
    if value != expected:
        raise _unapproved()
    return DebtMapping(**value, mapping_sha256=hashlib.sha256(raw).hexdigest(), _validation_token=_VALIDATION_TOKEN)


def _valid_evidence(value: object) -> bool:
    return isinstance(value, list) and bool(value) and all(isinstance(item, dict) and set(item) == {"reference", "sha256"} and isinstance(item["reference"], str) and bool(item["reference"].strip()) and _sha256(item["sha256"]) for item in value)


def _valid_timestamp(value: object) -> bool:
    if not isinstance(value, str) or _UTC_TIMESTAMP.fullmatch(value) is None:
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return True


def load_approved_debt_mapping(mapping_path: str | Path, approval_path: str | Path) -> DebtMapping:
    """Load an external composite mapping only when a separate approval binds its bytes."""
    _, raw_mapping, mapping = _read_mapping(mapping_path)
    _, raw_approval, approval = _read_mapping(approval_path)
    required_mapping = {"schema_version", "mapping_version", "observed_composite_values_sha256", "rules"}
    required_approval = {"schema_version", "mapping_sha256", "observed_composite_values_sha256", "evidence", "approved_by", "approved_at"}
    if set(mapping) != required_mapping or set(approval) != required_approval or mapping["schema_version"] != "debt-mapping-v2" or approval["schema_version"] != "debt-mapping-approval-v2":
        raise _unapproved()
    if not isinstance(mapping["mapping_version"], str) or not mapping["mapping_version"].strip() or not _sha256(mapping["observed_composite_values_sha256"]) or approval["mapping_sha256"] != hashlib.sha256(raw_mapping).hexdigest() or approval["observed_composite_values_sha256"] != mapping["observed_composite_values_sha256"] or not _valid_evidence(approval["evidence"]) or not isinstance(approval["approved_by"], str) or not approval["approved_by"].strip() or not _valid_timestamp(approval["approved_at"]):
        raise _unapproved()
    rules = _rules(mapping["rules"])
    return DebtMapping(schema_version="debt-mapping-v2", mapping_version=mapping["mapping_version"], scope="approved_external", rules=rules, observed_composite_values_sha256=mapping["observed_composite_values_sha256"], mapping_sha256=hashlib.sha256(raw_mapping).hexdigest(), approval_sha256=hashlib.sha256(raw_approval).hexdigest(), approval_manifest=approval, approval_canonical_json=_canonical_json(approval), _validation_token=_VALIDATION_TOKEN)
