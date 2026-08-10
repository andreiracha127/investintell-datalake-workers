"""Fail-closed, governed Regulation S / Rule 144A distribution-series registry.

The registry is deliberately not a security-master alias resolver.  It returns a
Regulation S CUSIP only from an explicitly approved same-pair decision whose
selected identifiers each link to validated parser observations in the same
source document and block.  It never infers a relationship from a security's
type, flags, issuer, coupon, maturity, identifier formatting, or absent
identifiers.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Literal
from uuid import UUID, uuid5

import psycopg

from src.bonds.identifiers import normalize_cusip9

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_distribution_series_v1.sql"

NAMESPACE_DISTRIBUTION_SERIES = UUID("b0d5ec00-0000-5000-a000-646973747269")

DistributionRule = Literal["reg_s", "rule_144a"]
IdentifierKind = Literal["cusip9", "isin", "common_code"]
IdentifierTenure = Literal["temporary", "permanent", "not_stated"]
SnapshotStatus = Literal["draft", "approved", "revoked"]
DecisionState = Literal["candidate", "approved", "ambiguous", "rejected", "revoked"]
ResolutionOmissionReason = Literal[
    "no_validated_source",
    "no_supported_reg_s_cusip",
    "ambiguous_mapping",
    "ambiguous_mapping_collision",
]


class DistributionSeriesError(RuntimeError):
    """Base error for governed distribution-series resolution failures."""


class InvalidDistributionSnapshotError(DistributionSeriesError):
    """The named snapshot does not exist or cannot authorize resolution."""


class NoValidatedDistributionSourceError(DistributionSeriesError):
    """No active same-pair decision has validated parser evidence."""


class NoSupportedRegSCusipError(DistributionSeriesError):
    """The governed pair exists but has no active Regulation S CUSIP9."""


class AmbiguousDistributionMappingError(DistributionSeriesError):
    """More than one active Regulation S CUSIP9 is supported."""


class ImmutableRegistryConflictError(DistributionSeriesError):
    """An idempotent registry load found different immutable content for one ID."""


@dataclass(frozen=True)
class DistributionSourceEvidence:
    source_evidence_id: str
    sec_accession: str
    form_type: str
    document_type: str
    source_url: str
    retrieved_at: datetime
    raw_document_sha256: str
    parser_version: str
    filed_at: datetime | None = None
    search_query_id: str | None = None
    document_url: str | None = None


@dataclass(frozen=True)
class DistributionParserObservation:
    parser_observation_id: str
    source_evidence_id: str
    parser_version: str
    block_locator: str
    exact_source_label: str
    source_value: str
    normalized_value: str | None
    observation_state: Literal["candidate", "validated", "rejected"]


@dataclass(frozen=True)
class DistributionMappingSnapshot:
    snapshot_id: str
    status: SnapshotStatus
    content_hash: str


@dataclass(frozen=True)
class DistributionSnapshotApproval:
    """Immutable closure record authorizing one complete draft snapshot."""

    snapshot_id: str
    content_hash: str


@dataclass(frozen=True)
class DistributionPairDecision:
    decision_id: str
    snapshot_id: str
    decision_state: DecisionState
    source_observation_id: str | None
    valid_from: date
    valid_to: date | None = None
    pair_key: str | None = None


@dataclass(frozen=True)
class DistributionPairIdentifier:
    identifier_id: str
    decision_id: str
    source_observation_id: str
    distribution_rule: DistributionRule
    identifier_kind: IdentifierKind
    identifier_value: str
    tenure: IdentifierTenure
    valid_from: date
    valid_to: date | None = None


@dataclass(frozen=True)
class DistributionResolution:
    snapshot_id: str
    decision_id: str
    reference_cusip9: str
    reg_s_cusip9: str
    reg_s_isin: str | None = None


@dataclass(frozen=True)
class DistributionResolutionMap:
    """Bulk registry output with explicit per-reference abstention reasons."""

    resolutions: dict[str, DistributionResolution]
    reason_by_reference: dict[str, ResolutionOmissionReason]


def deterministic_registry_id(kind: str, *parts: object) -> str:
    """Return a stable registry-local identifier; it conveys no security alias."""
    if not kind or any(part is None for part in parts):
        raise ValueError("registry id requires a kind and complete source parts")
    return str(uuid5(NAMESPACE_DISTRIBUTION_SERIES, "|".join((kind, *(str(p) for p in parts)))))


def source_evidence_id_for(
    sec_accession: str, document_type: str, raw_document_sha256: str
) -> str:
    return deterministic_registry_id("source-evidence", sec_accession, document_type, raw_document_sha256)


def parser_observation_id_for(
    source_evidence_id: str, parser_version: str, block_locator: str, exact_source_label: str, source_value: str
) -> str:
    return deterministic_registry_id(
        "parser-observation", source_evidence_id, parser_version, block_locator, exact_source_label, source_value
    )


def distribution_snapshot_content_hash(
    snapshot_id: str,
    decisions: Iterable[DistributionPairDecision],
    identifiers: Iterable[DistributionPairIdentifier],
) -> str:
    """Digest the complete, canonical mapping composition for one draft snapshot."""
    decision_rows = sorted(
        (decision for decision in decisions if decision.snapshot_id == snapshot_id),
        key=lambda decision: decision.decision_id,
    )
    decision_ids = {decision.decision_id for decision in decision_rows}
    identifier_rows = sorted(
        (identifier for identifier in identifiers if identifier.decision_id in decision_ids),
        key=lambda identifier: identifier.identifier_id,
    )
    payload = {
        "format": "bond_distribution_snapshot_composition_v1",
        "snapshot_id": snapshot_id,
        "decisions": [
            {
                "decision_id": decision.decision_id,
                "snapshot_id": decision.snapshot_id,
                "decision_state": decision.decision_state,
                "source_observation_id": decision.source_observation_id,
                "valid_from": decision.valid_from.isoformat(),
                "valid_to": decision.valid_to.isoformat() if decision.valid_to else None,
                "pair_key": decision.pair_key,
            }
            for decision in decision_rows
        ],
        "identifiers": [
            {
                "identifier_id": identifier.identifier_id,
                "decision_id": identifier.decision_id,
                "source_observation_id": identifier.source_observation_id,
                "distribution_rule": identifier.distribution_rule,
                "identifier_kind": identifier.identifier_kind,
                "identifier_value": identifier.identifier_value,
                "tenure": identifier.tenure,
                "valid_from": identifier.valid_from.isoformat(),
                "valid_to": identifier.valid_to.isoformat() if identifier.valid_to else None,
            }
            for identifier in identifier_rows
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


def validate_distribution_snapshot_approval(
    snapshot: DistributionMappingSnapshot,
    approval: DistributionSnapshotApproval,
    decisions: Iterable[DistributionPairDecision],
    identifiers: Iterable[DistributionPairIdentifier],
) -> str:
    """Require an approval's declared hash to bind the actual closed composition."""
    if approval.snapshot_id != snapshot.snapshot_id or snapshot.status != "draft":
        raise ImmutableRegistryConflictError("snapshot_approval_requires_draft_snapshot")
    computed = distribution_snapshot_content_hash(snapshot.snapshot_id, decisions, identifiers)
    if snapshot.content_hash != computed or approval.content_hash != computed:
        raise ImmutableRegistryConflictError("snapshot_content_hash_mismatch")
    return computed


def _active(valid_from: date, valid_to: date | None, as_of: date) -> bool:
    return valid_from <= as_of and (valid_to is None or as_of < valid_to)


def _normalized_reference_cusip9(value: str) -> str:
    """Canonicalize caller formatting only; this never qualifies or repairs a CUSIP."""
    return value.strip().upper()


_EXACT_LABEL_TAXONOMY: dict[str, tuple[DistributionRule, IdentifierKind]] = {
    "RULE 144A CUSIP": ("rule_144a", "cusip9"),
    "RULE144A CUSIP": ("rule_144a", "cusip9"),
    "144A CUSIP": ("rule_144a", "cusip9"),
    "RULE 144A CINS": ("rule_144a", "cusip9"),
    "RULE144A CINS": ("rule_144a", "cusip9"),
    "144A CINS": ("rule_144a", "cusip9"),
    "RULE 144A ISIN": ("rule_144a", "isin"),
    "RULE144A ISIN": ("rule_144a", "isin"),
    "144A ISIN": ("rule_144a", "isin"),
    "RULE 144A COMMON CODE": ("rule_144a", "common_code"),
    "RULE144A COMMON CODE": ("rule_144a", "common_code"),
    "144A COMMON CODE": ("rule_144a", "common_code"),
    "REGULATION S CUSIP": ("reg_s", "cusip9"),
    "REGULATIONS CUSIP": ("reg_s", "cusip9"),
    "REG S CUSIP": ("reg_s", "cusip9"),
    "REGS CUSIP": ("reg_s", "cusip9"),
    "REGULATION S CINS": ("reg_s", "cusip9"),
    "REGULATIONS CINS": ("reg_s", "cusip9"),
    "REG S CINS": ("reg_s", "cusip9"),
    "REGS CINS": ("reg_s", "cusip9"),
    "REGULATION S ISIN": ("reg_s", "isin"),
    "REGULATIONS ISIN": ("reg_s", "isin"),
    "REG S ISIN": ("reg_s", "isin"),
    "REGS ISIN": ("reg_s", "isin"),
    "REGULATION S COMMON CODE": ("reg_s", "common_code"),
    "REGULATIONS COMMON CODE": ("reg_s", "common_code"),
    "REG S COMMON CODE": ("reg_s", "common_code"),
    "REGS COMMON CODE": ("reg_s", "common_code"),
}


def identifier_kind_from_source_label(
    exact_source_label: str,
) -> tuple[DistributionRule, IdentifierKind] | None:
    """Map only exact anchored Regulation S/144A labels to the registry taxonomy."""
    normalized_label = re.sub(r"\s+", " ", exact_source_label.strip()).upper()
    return _EXACT_LABEL_TAXONOMY.get(normalized_label)


def _matches_validated_observation(
    identifier: DistributionPairIdentifier,
    observation: DistributionParserObservation | None,
) -> bool:
    if observation is None or observation.observation_state != "validated":
        return False
    taxonomy = identifier_kind_from_source_label(observation.exact_source_label)
    return bool(
        taxonomy
        and observation.normalized_value is not None
        and identifier_value_has_valid_syntax(identifier.identifier_kind, observation.normalized_value)
        and identifier.identifier_value == observation.normalized_value
        and (identifier.distribution_rule, identifier.identifier_kind) == taxonomy
    )


def identifier_value_has_valid_syntax(identifier_kind: str, value: object) -> bool:
    """Validate execution syntax without manufacturing a check-digit algorithm.

    The repository's canonical CUSIP qualifier enforces the established
    nine-character uppercase-alphanumeric form (and rejects placeholders) but
    deliberately has no check-digit rule.  CINS therefore follows that same
    structural CUSIP9 rule.  ISIN and Common Code are exact structural forms.
    """
    if not isinstance(value, str):
        return False
    if identifier_kind == "cusip9":
        return normalize_cusip9(value).normalized_cusip9 == value
    if identifier_kind == "isin":
        return re.fullmatch(r"[A-Z0-9]{12}", value) is not None
    if identifier_kind == "common_code":
        return re.fullmatch(r"[0-9]{9}", value) is not None
    return False


def _require_approved_snapshot(
    snapshot_id: str,
    snapshots: Iterable[DistributionMappingSnapshot],
    approvals: Iterable[DistributionSnapshotApproval],
    decisions: Iterable[DistributionPairDecision],
    identifiers: Iterable[DistributionPairIdentifier],
) -> None:
    matching_snapshots = [snapshot for snapshot in snapshots if snapshot.snapshot_id == snapshot_id]
    if not matching_snapshots:
        raise InvalidDistributionSnapshotError("snapshot_not_found")
    if len(matching_snapshots) != 1 or matching_snapshots[0].status != "draft":
        raise InvalidDistributionSnapshotError("snapshot_not_approved")
    matching_approvals = [approval for approval in approvals if approval.snapshot_id == snapshot_id]
    if (
        len(matching_approvals) != 1
        or matching_approvals[0].content_hash != matching_snapshots[0].content_hash
    ):
        raise InvalidDistributionSnapshotError("snapshot_not_approved")
    try:
        validate_distribution_snapshot_approval(
            matching_snapshots[0], matching_approvals[0], decisions, identifiers
        )
    except ImmutableRegistryConflictError as error:
        raise InvalidDistributionSnapshotError(str(error)) from error


@dataclass(frozen=True)
class _ResolutionIndex:
    observations_by_id: dict[str, DistributionParserObservation]
    references_by_cusip: dict[str, tuple[tuple[DistributionPairDecision, DistributionPairIdentifier], ...]]
    reg_s_by_decision: dict[str, tuple[DistributionPairIdentifier, ...]]


def _build_resolution_index(
    *,
    snapshot_id: str,
    as_of: date,
    decisions: Iterable[DistributionPairDecision],
    identifiers: Iterable[DistributionPairIdentifier],
    parser_observations: Iterable[DistributionParserObservation],
) -> _ResolutionIndex:
    """Traverse registry facts once into the strict resolver's lookup indexes."""
    observations_by_id = {
        observation.parser_observation_id: observation for observation in parser_observations
    }
    active_decisions = {
        decision.decision_id: decision
        for decision in decisions
        if (
            decision.snapshot_id == snapshot_id
            and decision.decision_state == "approved"
            and _active(decision.valid_from, decision.valid_to, as_of)
        )
    }
    references: dict[str, list[tuple[DistributionPairDecision, DistributionPairIdentifier]]] = {}
    reg_s: dict[str, list[DistributionPairIdentifier]] = {}
    for identifier in identifiers:
        decision = active_decisions.get(identifier.decision_id)
        if decision is None or not _active(identifier.valid_from, identifier.valid_to, as_of):
            continue
        if identifier.distribution_rule == "rule_144a" and identifier.identifier_kind == "cusip9":
            references.setdefault(identifier.identifier_value, []).append((decision, identifier))
        elif identifier.distribution_rule == "reg_s":
            reg_s.setdefault(decision.decision_id, []).append(identifier)
    return _ResolutionIndex(
        observations_by_id=observations_by_id,
        references_by_cusip={key: tuple(value) for key, value in references.items()},
        reg_s_by_decision={key: tuple(value) for key, value in reg_s.items()},
    )


def _resolve_reference_from_index(
    *, snapshot_id: str, reference_cusip9: str, index: _ResolutionIndex
) -> DistributionResolution:
    """Resolve one reference using precomputed facts, preserving scalar semantics."""
    candidates_by_decision: dict[str, list[tuple[DistributionPairDecision, DistributionPairIdentifier]]] = {}
    for decision, identifier in index.references_by_cusip.get(reference_cusip9, ()):
        candidates_by_decision.setdefault(decision.decision_id, []).append((decision, identifier))
    if any(len(candidates) > 1 for candidates in candidates_by_decision.values()):
        raise AmbiguousDistributionMappingError("ambiguous_mapping")
    relevant = [candidates[0] for candidates in candidates_by_decision.values()]
    selected = [
        (decision, identifier)
        for decision, identifier in relevant
        if _matches_validated_observation(identifier, index.observations_by_id.get(identifier.source_observation_id))
    ]
    if not relevant or not selected:
        raise NoValidatedDistributionSourceError("no_validated_source")

    candidate_pairs: list[tuple[str, str, str | None]] = []
    governed_without_cusip = False
    for decision, reference in selected:
        reference_observation = index.observations_by_id[reference.source_observation_id]
        compatible = [
            identifier
            for identifier in index.reg_s_by_decision.get(decision.decision_id, ())
            if _matches_validated_observation(
                identifier, index.observations_by_id.get(identifier.source_observation_id)
            )
            and index.observations_by_id[identifier.source_observation_id].source_evidence_id
            == reference_observation.source_evidence_id
            and index.observations_by_id[identifier.source_observation_id].block_locator
            == reference_observation.block_locator
        ]
        cusips = [identifier.identifier_value for identifier in compatible if identifier.identifier_kind == "cusip9"]
        isins = {identifier.identifier_value for identifier in compatible if identifier.identifier_kind == "isin"}
        if len(isins) > 1:
            raise AmbiguousDistributionMappingError("ambiguous_mapping")
        reg_s_isin = next(iter(isins), None)
        if not cusips:
            governed_without_cusip = governed_without_cusip or bool(compatible)
            continue
        candidate_pairs.extend((decision.decision_id, cusip, reg_s_isin) for cusip in cusips)
    if not candidate_pairs:
        if governed_without_cusip:
            raise NoSupportedRegSCusipError("no_supported_reg_s_cusip")
        raise NoValidatedDistributionSourceError("no_validated_source")
    unique = set(candidate_pairs)
    if len(unique) != 1:
        raise AmbiguousDistributionMappingError("ambiguous_mapping")
    decision_id, reg_s_cusip9, reg_s_isin = next(iter(unique))
    return DistributionResolution(snapshot_id, decision_id, reference_cusip9, reg_s_cusip9, reg_s_isin)


def resolve_reg_s_cusip(
    *,
    snapshot_id: str,
    as_of: date,
    reference_cusip9: str,
    snapshots: Iterable[DistributionMappingSnapshot],
    approvals: Iterable[DistributionSnapshotApproval],
    decisions: Iterable[DistributionPairDecision],
    identifiers: Iterable[DistributionPairIdentifier],
    parser_observations: Iterable[DistributionParserObservation],
) -> DistributionResolution:
    """Resolve exactly one approved active Rule 144A CUSIP9 to a Reg S CUSIP9.

    This function intentionally consumes only registry facts.  A selected Rule
    144A identifier and its selected Regulation S identifier must each have a
    validated parser observation from the same source evidence and block.  An
    ISIN/Common Code-only Reg S side remains represented but is ineligible for
    the current CUSIP panel.
    """
    snapshot_rows = tuple(snapshots)
    approval_rows = tuple(approvals)
    decision_rows = tuple(decisions)
    identifier_rows = tuple(identifiers)
    observation_rows = tuple(parser_observations)
    _require_approved_snapshot(
        snapshot_id, snapshot_rows, approval_rows, decision_rows, identifier_rows
    )
    index = _build_resolution_index(
        snapshot_id=snapshot_id, as_of=as_of, decisions=decision_rows, identifiers=identifier_rows,
        parser_observations=observation_rows,
    )
    return _resolve_reference_from_index(
        snapshot_id=snapshot_id, reference_cusip9=reference_cusip9, index=index
    )


def resolve_reg_s_cusip_map(
    *,
    snapshot_id: str,
    as_of: date,
    reference_cusip9s: Iterable[str],
    snapshots: Iterable[DistributionMappingSnapshot],
    approvals: Iterable[DistributionSnapshotApproval],
    decisions: Iterable[DistributionPairDecision],
    identifiers: Iterable[DistributionPairIdentifier],
    parser_observations: Iterable[DistributionParserObservation],
) -> DistributionResolutionMap:
    """Resolve a reference set from one immutable registry snapshot.

    The facts are materialized and indexed once so callers such as Stage 6 can
    scan a large reference universe without reloading or repeatedly traversing
    the registry.  Every reference then uses the same strict indexed rule as
    the scalar API.
    """
    snapshot_rows = tuple(snapshots)
    approval_rows = tuple(approvals)
    decision_rows = tuple(decisions)
    identifier_rows = tuple(identifiers)
    observation_rows = tuple(parser_observations)
    _require_approved_snapshot(
        snapshot_id, snapshot_rows, approval_rows, decision_rows, identifier_rows
    )
    index = _build_resolution_index(
        snapshot_id=snapshot_id, as_of=as_of, decisions=decision_rows,
        identifiers=identifier_rows, parser_observations=observation_rows,
    )

    normalized_references = tuple(dict.fromkeys(
        _normalized_reference_cusip9(reference) for reference in reference_cusip9s
    ))
    resolutions: dict[str, DistributionResolution] = {}
    reasons: dict[str, ResolutionOmissionReason] = {}
    for reference in normalized_references:
        try:
            resolutions[reference] = _resolve_reference_from_index(
                snapshot_id=snapshot_id, reference_cusip9=reference, index=index,
            )
        except NoValidatedDistributionSourceError:
            reasons[reference] = "no_validated_source"
        except NoSupportedRegSCusipError:
            reasons[reference] = "no_supported_reg_s_cusip"
        except AmbiguousDistributionMappingError:
            reasons[reference] = "ambiguous_mapping"

    by_execution_cusip: dict[str, list[str]] = {}
    for reference, resolution in resolutions.items():
        by_execution_cusip.setdefault(resolution.reg_s_cusip9, []).append(reference)
    for references in by_execution_cusip.values():
        if len(references) > 1:
            for reference in references:
                del resolutions[reference]
                reasons[reference] = "ambiguous_mapping_collision"

    if normalized_references and not resolutions and all(
        reason == "no_validated_source" for reason in reasons.values()
    ):
        raise NoValidatedDistributionSourceError("no_validated_source")
    return DistributionResolutionMap(resolutions=resolutions, reason_by_reference=reasons)


def install_schema(conn: psycopg.Connection) -> None:
    """Apply the additive registry DDL; it does not activate or publish anything."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def load_distribution_registry(
    conn: psycopg.Connection,
    *,
    source_evidence: Iterable[DistributionSourceEvidence] = (),
    parser_observations: Iterable[DistributionParserObservation] = (),
    snapshots: Iterable[DistributionMappingSnapshot] = (),
    decisions: Iterable[DistributionPairDecision] = (),
    identifiers: Iterable[DistributionPairIdentifier] = (),
    approvals: Iterable[DistributionSnapshotApproval] = (),
) -> dict[str, int]:
    """Atomically insert drafts then approval records, verifying every conflict byte-for-byte."""
    source_rows = tuple(source_evidence)
    observation_rows = tuple(parser_observations)
    snapshot_rows = tuple(snapshots)
    decision_rows = tuple(decisions)
    identifier_rows = tuple(identifiers)
    approval_rows = tuple(approvals)
    rows = {
        "source_evidence": 0,
        "parser_observations": 0,
        "snapshots": 0,
        "decisions": 0,
        "identifiers": 0,
        "approvals": 0,
    }
    with conn.transaction():
        for item in source_rows:
            rows["source_evidence"] += _insert_or_verify(
                conn,
                "INSERT INTO bond_distribution_source_evidence "
                "(source_evidence_id,sec_accession,form_type,document_type,filed_at,search_query_id,source_url,document_url,retrieved_at,raw_document_sha256,parser_version) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (source_evidence_id) DO NOTHING RETURNING source_evidence_id",
                (item.source_evidence_id, item.sec_accession, item.form_type, item.document_type, item.filed_at,
                 item.search_query_id, item.source_url, item.document_url, item.retrieved_at,
                 item.raw_document_sha256, item.parser_version),
                "SELECT source_evidence_id,sec_accession,form_type,document_type,filed_at,search_query_id,source_url,document_url,retrieved_at,raw_document_sha256,parser_version "
                "FROM bond_distribution_source_evidence WHERE source_evidence_id=%s",
            )
        for item in observation_rows:
            rows["parser_observations"] += _insert_or_verify(
                conn,
                "INSERT INTO bond_distribution_parser_observation "
                "(parser_observation_id,source_evidence_id,parser_version,block_locator,exact_source_label,source_value,normalized_value,observation_state) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (parser_observation_id) DO NOTHING RETURNING parser_observation_id",
                (item.parser_observation_id, item.source_evidence_id, item.parser_version, item.block_locator,
                 item.exact_source_label, item.source_value, item.normalized_value, item.observation_state),
                "SELECT parser_observation_id,source_evidence_id,parser_version,block_locator,exact_source_label,source_value,normalized_value,observation_state "
                "FROM bond_distribution_parser_observation WHERE parser_observation_id=%s",
            )
        for item in snapshot_rows:
            rows["snapshots"] += _insert_or_verify(
                conn,
                "INSERT INTO bond_distribution_mapping_snapshot(snapshot_id,snapshot_status,content_hash) "
                "VALUES(%s,%s,%s) ON CONFLICT (snapshot_id) DO NOTHING RETURNING snapshot_id",
                (item.snapshot_id, item.status, item.content_hash),
                "SELECT snapshot_id,snapshot_status,content_hash FROM bond_distribution_mapping_snapshot WHERE snapshot_id=%s",
            )
        for item in decision_rows:
            pair_key = item.pair_key or hashlib.sha256(item.decision_id.encode()).hexdigest()
            rows["decisions"] += _insert_or_verify(
                conn,
                "INSERT INTO bond_distribution_pair_decision "
                "(decision_id,snapshot_id,pair_key,decision_state,source_observation_id,valid_from,valid_to) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (decision_id) DO NOTHING RETURNING decision_id",
                (item.decision_id, item.snapshot_id, pair_key, item.decision_state,
                 item.source_observation_id, item.valid_from, item.valid_to),
                "SELECT decision_id,snapshot_id,pair_key,decision_state,source_observation_id,valid_from,valid_to "
                "FROM bond_distribution_pair_decision WHERE decision_id=%s",
            )
        for item in identifier_rows:
            rows["identifiers"] += _insert_or_verify(
                conn,
                "INSERT INTO bond_distribution_pair_identifier "
                "(identifier_id,decision_id,source_observation_id,distribution_rule,identifier_kind,identifier_value,identifier_tenure,valid_from,valid_to) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (identifier_id) DO NOTHING RETURNING identifier_id",
                (item.identifier_id, item.decision_id, item.source_observation_id, item.distribution_rule,
                 item.identifier_kind, item.identifier_value, item.tenure, item.valid_from, item.valid_to),
                "SELECT identifier_id,decision_id,source_observation_id,distribution_rule,identifier_kind,identifier_value,identifier_tenure,valid_from,valid_to "
                "FROM bond_distribution_pair_identifier WHERE identifier_id=%s",
            )
        for item in approval_rows:
            supplied = [snapshot for snapshot in snapshot_rows if snapshot.snapshot_id == item.snapshot_id]
            if len(supplied) > 1:
                raise ImmutableRegistryConflictError("snapshot_approval_requires_one_draft_snapshot")
            snapshot_row = conn.execute(
                "SELECT snapshot_id,snapshot_status,content_hash FROM bond_distribution_mapping_snapshot "
                "WHERE snapshot_id=%s FOR UPDATE",
                (item.snapshot_id,),
            ).fetchone()
            if snapshot_row is None:
                raise ImmutableRegistryConflictError("snapshot_approval_requires_one_draft_snapshot")
            database_snapshot = DistributionMappingSnapshot(
                str(snapshot_row[0]), snapshot_row[1], snapshot_row[2]
            )
            if supplied and supplied[0] != database_snapshot:
                raise ImmutableRegistryConflictError("snapshot_approval_requires_one_draft_snapshot")
            database_decisions, database_identifiers = _snapshot_composition_from_db(
                conn, snapshot_id=item.snapshot_id
            )
            validate_distribution_snapshot_approval(
                database_snapshot, item, database_decisions, database_identifiers
            )
            rows["approvals"] += _insert_or_verify(
                conn,
                "INSERT INTO bond_distribution_snapshot_approval(snapshot_id,content_hash) "
                "VALUES(%s,%s) ON CONFLICT (snapshot_id) DO NOTHING RETURNING snapshot_id",
                (item.snapshot_id, item.content_hash),
                "SELECT snapshot_id,content_hash FROM bond_distribution_snapshot_approval WHERE snapshot_id=%s",
            )
    return rows


def _insert_or_verify(
    conn: psycopg.Connection, insert_sql: str, values: tuple[object, ...], select_sql: str
) -> int:
    """Return 1 for an insertion, 0 for an equal immutable replay, else refuse."""
    existing = conn.execute(select_sql, (values[0],)).fetchone()
    if existing is not None:
        if existing != values:
            raise ImmutableRegistryConflictError(f"immutable_conflict:{values[0]}")
        return 0
    if conn.execute(insert_sql, values).fetchone() is not None:
        return 1
    existing = conn.execute(select_sql, (values[0],)).fetchone()
    if existing != values:
        raise ImmutableRegistryConflictError(f"immutable_conflict:{values[0]}")
    return 0


def approve_mapping_snapshot(
    conn: psycopg.Connection, *, snapshot_id: str, content_hash: str
) -> bool:
    """Close one fully loaded draft snapshot in its own atomic transaction."""
    with conn.transaction():
        snapshot_row = conn.execute(
            "SELECT snapshot_id,snapshot_status,content_hash FROM bond_distribution_mapping_snapshot "
            "WHERE snapshot_id=%s FOR UPDATE",
            (snapshot_id,),
        ).fetchone()
        if snapshot_row is None:
            raise ImmutableRegistryConflictError("snapshot_approval_requires_one_draft_snapshot")
        snapshot = DistributionMappingSnapshot(str(snapshot_row[0]), snapshot_row[1], snapshot_row[2])
        decisions, identifiers = _snapshot_composition_from_db(conn, snapshot_id=snapshot_id)
        validate_distribution_snapshot_approval(
            snapshot, DistributionSnapshotApproval(snapshot_id, content_hash), decisions, identifiers
        )
        return bool(_insert_or_verify(
            conn,
            "INSERT INTO bond_distribution_snapshot_approval(snapshot_id,content_hash) "
            "VALUES(%s,%s) ON CONFLICT (snapshot_id) DO NOTHING RETURNING snapshot_id",
            (snapshot_id, content_hash),
            "SELECT snapshot_id,content_hash FROM bond_distribution_snapshot_approval WHERE snapshot_id=%s",
        ))


def _snapshot_composition_from_db(
    conn: psycopg.Connection, *, snapshot_id: str
) -> tuple[list[DistributionPairDecision], list[DistributionPairIdentifier]]:
    """Read exactly the decision/identifier rows that form one snapshot hash."""
    decisions = [
        DistributionPairDecision(str(row[0]), str(row[1]), row[2], row[3], row[4], row[5], row[6])
        for row in conn.execute(
            "SELECT decision_id,snapshot_id,decision_state,source_observation_id,valid_from,valid_to,pair_key "
            "FROM bond_distribution_pair_decision WHERE snapshot_id=%s",
            (snapshot_id,),
        ).fetchall()
    ]
    identifiers = [
        DistributionPairIdentifier(str(row[0]), str(row[1]), str(row[2]), row[3], row[4], row[5], row[6], row[7], row[8])
        for row in conn.execute(
            "SELECT i.identifier_id,i.decision_id,i.source_observation_id,i.distribution_rule,i.identifier_kind,"
            "i.identifier_value,i.identifier_tenure,i.valid_from,i.valid_to "
            "FROM bond_distribution_pair_identifier i "
            "JOIN bond_distribution_pair_decision d ON d.decision_id=i.decision_id "
            "WHERE d.snapshot_id=%s",
            (snapshot_id,),
        ).fetchall()
    ]
    return decisions, identifiers


def _load_registry_snapshot(
    conn: psycopg.Connection, *, snapshot_id: str
) -> tuple[
    list[DistributionMappingSnapshot],
    list[DistributionSnapshotApproval],
    list[DistributionPairDecision],
    list[DistributionPairIdentifier],
    list[DistributionParserObservation],
]:
    """Read all facts needed by a named immutable snapshot in four bounded queries."""
    snapshot_rows = conn.execute(
        "SELECT snapshot_id,snapshot_status,content_hash FROM bond_distribution_mapping_snapshot WHERE snapshot_id=%s",
        (snapshot_id,),
    ).fetchall()
    approvals = [
        DistributionSnapshotApproval(str(row[0]), row[1])
        for row in conn.execute(
            "SELECT snapshot_id,content_hash FROM bond_distribution_snapshot_approval WHERE snapshot_id=%s",
            (snapshot_id,),
        ).fetchall()
    ]
    decisions = [
        DistributionPairDecision(str(row[0]), str(row[1]), row[2], row[3], row[4], row[5], row[6])
        for row in conn.execute(
            "SELECT d.decision_id,d.snapshot_id,d.decision_state,d.source_observation_id,"
            "d.valid_from,d.valid_to,d.pair_key FROM bond_distribution_pair_decision d "
            "WHERE d.snapshot_id=%s",
            (snapshot_id,),
        ).fetchall()
    ]
    identifiers = [
        DistributionPairIdentifier(str(row[0]), str(row[1]), str(row[2]), row[3], row[4], row[5], row[6], row[7], row[8])
        for row in conn.execute(
            "SELECT i.identifier_id,i.decision_id,i.source_observation_id,i.distribution_rule,i.identifier_kind,i.identifier_value,"
            "i.identifier_tenure,i.valid_from,i.valid_to "
            "FROM bond_distribution_pair_identifier i "
            "JOIN bond_distribution_pair_decision d ON d.decision_id=i.decision_id WHERE d.snapshot_id=%s",
            (snapshot_id,),
        ).fetchall()
    ]
    parser_observations = [
        DistributionParserObservation(str(row[0]), str(row[1]), row[2], row[3], row[4], row[5], row[6], row[7])
        for row in conn.execute(
            "SELECT parser_observation_id,source_evidence_id,parser_version,block_locator,exact_source_label,"
            "source_value,normalized_value,observation_state FROM bond_distribution_parser_observation "
            "WHERE parser_observation_id IN ("
            "SELECT i.source_observation_id FROM bond_distribution_pair_identifier i "
            "JOIN bond_distribution_pair_decision d ON d.decision_id=i.decision_id "
            "WHERE d.snapshot_id=%s)"
            ,
            (snapshot_id,),
        ).fetchall()
    ]
    snapshots = [DistributionMappingSnapshot(str(row[0]), row[1], row[2]) for row in snapshot_rows]
    return snapshots, approvals, decisions, identifiers, parser_observations


def resolve_reg_s_cusip_from_db(
    conn: psycopg.Connection, *, snapshot_id: str, as_of: date, reference_cusip9: str
) -> DistributionResolution:
    """Load one registry snapshot and apply the pure resolver without heuristics."""
    snapshots, approvals, decisions, identifiers, parser_observations = _load_registry_snapshot(
        conn, snapshot_id=snapshot_id
    )
    return resolve_reg_s_cusip(
        snapshot_id=snapshot_id, as_of=as_of, reference_cusip9=reference_cusip9,
        snapshots=snapshots, approvals=approvals, decisions=decisions, identifiers=identifiers,
        parser_observations=parser_observations,
    )


def resolve_reg_s_cusip_map_from_db(
    conn: psycopg.Connection,
    *,
    snapshot_id: str,
    as_of: date,
    reference_cusip9s: Iterable[str],
) -> DistributionResolutionMap:
    """Load the snapshot once and resolve a Stage-6 reference universe strictly."""
    snapshots, approvals, decisions, identifiers, parser_observations = _load_registry_snapshot(
        conn, snapshot_id=snapshot_id
    )
    return resolve_reg_s_cusip_map(
        snapshot_id=snapshot_id, as_of=as_of, reference_cusip9s=reference_cusip9s,
        snapshots=snapshots, approvals=approvals, decisions=decisions, identifiers=identifiers,
        parser_observations=parser_observations,
    )
