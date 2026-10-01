"""Agency-free, offline bridge from Light's owner review ledger.

JSON hashes prove integrity/lineage, NOT WorkOS authentication. The caller must
supply the independently configured owner. Only Light's authenticated write path
verifies JWTs; an offline operator is responsible for obtaining that export.
Nothing here reads a DSN, installs a schema, elects a pointer or estimates EL.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID, uuid5

PRODUCT = "bond_default_owner_evidence_v1"
EXPORT_VERSION = "bond_default_review_export_v1"
POLICY_VERSION = "bond_default_owner_event_policy_v1"
NAMESPACE = UUID("60f25eab-91f1-565b-bdb2-81bc91465e80")
POLICY_PATH = Path(__file__).resolve().parents[3] / "contracts/bonds/default_owner_event_policy_v1.json"
SHA = re.compile(r"^[0-9a-f]{64}$")
RATED = ("AAA", "AA", "A", "BBB", "BB", "B", "CCC")
DECISION_FIELDS = {
    "idempotency_key", "expected_previous_decision_id", "decision", "rationale",
    "confirmed_cusip", "issue_scope", "event_date", "event_type", "seniority",
    "security_status", "collateral_description", "citations", "market_prices",
}
RESOLUTION_FIELDS = {
    "idempotency_key", "decision_id", "expected_previous_resolution_id", "resolution_type",
    "resolution_date", "recovery_per_100", "recovery_form", "valuation_basis",
    "valuation_description", "citations",
}
RECORD_FIELDS = {"proposal_id", "sequence", "reviewer_sub", "recorded_at", "request_sha256"}
PROPOSAL_FIELDS = {
    "proposal_id", "source_episode_id", "issuer_name", "summary", "proposed_event_date",
    "proposed_event_type", "proposed_cusips", "citations", "uncertainties", "market_prices",
    "source_fingerprint", "source_manifest_sha256", "source_as_of", "source_obligation_id", "state",
}
CITATION_FIELDS = {
    "citation_id", "source_document_id", "document_sha256", "url", "quote", "locator", "public_at",
}
PRICE_FIELDS = {
    "month", "price_per_100", "source_publication_id", "source_reference", "owner_confirmed",
    "interpretation",
}
EVENT_TYPES = {"chapter_11", "chapter_7", "missed_payment_after_cure", "distressed_exchange"}


class OwnerEvidenceError(ValueError):
    """Sanitized refusal: the code is safe to print; source values are not echoed."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise OwnerEvidenceError(code)


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise OwnerEvidenceError("noncanonical_json") from exc


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _unique_json(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def decode_json(text: str) -> dict[str, Any]:
    try:
        result = json.loads(text, object_pairs_hook=_unique_json)
    except (ValueError, TypeError) as exc:
        if isinstance(exc, OwnerEvidenceError):
            raise
        raise OwnerEvidenceError("invalid_json") from exc
    _require(isinstance(result, dict), "object_required")
    canonical_json(result)  # reject NaN/Infinity, even in a field not subsequently used
    return result


def _object(value: Any, fields: set[str], code: str) -> dict[str, Any]:
    _require(isinstance(value, dict) and set(value) == fields, code)
    return value


def _text(value: Any, code: str, maximum: int = 8000) -> str:
    _require(isinstance(value, str) and bool(value.strip()) and value == value.strip()
             and len(value) <= maximum, code)
    return value


def _sha(value: Any) -> str:
    _require(isinstance(value, str) and SHA.fullmatch(value) is not None, "invalid_sha256")
    return value


def _uuid(value: Any) -> str:
    try:
        result = str(UUID(value)) if isinstance(value, str) else ""
    except ValueError as exc:
        raise OwnerEvidenceError("invalid_uuid") from exc
    _require(result == value, "invalid_uuid")
    return result


def _day(value: Any) -> date:
    try:
        result = date.fromisoformat(value) if isinstance(value, str) else None
    except ValueError as exc:
        raise OwnerEvidenceError("invalid_date") from exc
    _require(result is not None and result.isoformat() == value, "invalid_date")
    return result


def _instant(value: Any) -> datetime:
    try:
        result = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError as exc:
        raise OwnerEvidenceError("invalid_timestamp") from exc
    _require(result is not None and result.tzinfo is not None and result.utcoffset() is not None,
             "aware_timestamp_required")
    return result.astimezone(UTC)


def _number(value: Any, *, positive: bool = False) -> float:
    _require(type(value) in (int, float) and math.isfinite(value)
             and (value > 0 if positive else value >= 0), "invalid_amount")
    return float(value)


def _list(value: Any, maximum: int, *, minimum: int = 0) -> list[Any]:
    _require(isinstance(value, list) and minimum <= len(value) <= maximum, "invalid_list")
    return value


def validate_cusip9(value: Any, *, checksum: bool = True) -> str:
    _require(isinstance(value, str) and re.fullmatch(r"[0-9A-Z*@#]{8}[0-9]", value) is not None,
             "invalid_cusip9")
    total = 0
    for index, character in enumerate(value[:8]):
        number = (int(character) if character.isdigit() else ord(character) - ord("A") + 10
                  if character.isalpha() else {"*": 36, "@": 37, "#": 38}[character])
        if index % 2:
            number *= 2
        total += number // 10 + number % 10
    _require(not checksum or (10 - total % 10) % 10 == int(value[-1]), "invalid_cusip_checksum")
    return value


def _citations(values: Any, registry: dict[str, Any], documents: dict[str, str], *, maximum: int,
               cutoff: datetime | None = None, minimum: int = 1) -> None:
    ids: set[str] = set()
    for raw in _list(values, maximum, minimum=minimum):
        item = _object(raw, CITATION_FIELDS, "invalid_citation_shape")
        citation_id = _uuid(item["citation_id"])
        _require(citation_id not in ids, "duplicate_citation")
        ids.add(citation_id)
        source_id = _text(item["source_document_id"], "invalid_document_id", 256)
        document_hash = _sha(item["document_sha256"])
        _text(item["quote"], "citation_quote_required")
        url = _text(item["url"], "invalid_citation_url", 2048)
        parsed = urlsplit(url)
        _require(parsed.scheme in {"http", "https"} and bool(parsed.hostname)
                 and parsed.username is None and parsed.password is None, "invalid_citation_url")
        if item["locator"] is not None:
            _require(isinstance(item["locator"], str) and len(item["locator"]) <= 256,
                     "invalid_locator")
        if item["public_at"] is not None:
            public_at = _instant(item["public_at"])
            _require(cutoff is None or public_at <= cutoff, "citation_after_recording")
        _require(citation_id not in registry or registry[citation_id] == item,
                 "citation_identity_mismatch")
        _require(source_id not in documents or documents[source_id] == document_hash,
                 "document_identity_mismatch")
        registry[citation_id] = item
        documents[source_id] = document_hash


def _prices(values: Any, *, event_date: Any = None, require_confirmation: bool = False) -> None:
    months: set[date] = set()
    for raw in _list(values, 2):
        item = _object(raw, PRICE_FIELDS, "invalid_market_proxy_shape")
        month = _day(item["month"])
        _require(month.day == 1 and month not in months, "invalid_market_proxy_month")
        months.add(month)
        _number(item["price_per_100"], positive=True)
        _text(item["source_reference"], "market_proxy_source_required", 512)
        if item["source_publication_id"] is not None:
            _uuid(item["source_publication_id"])
        _require(type(item["owner_confirmed"]) is bool, "invalid_owner_confirmation")
        _require(item["interpretation"] == "market_recovery_proxy_not_realized",
                 "market_price_is_not_realized_recovery")
        if require_confirmation:
            _require(item["owner_confirmed"] is True and event_date is not None,
                     "market_proxy_requires_owner_confirmation")
            first = _day(event_date).replace(day=1)
            second = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
            _require(month in {first, second}, "invalid_market_proxy_month")


def _check_chain(records: Sequence[dict[str, Any]], *, previous_field: str, id_field: str) -> None:
    previous_id: str | None = None
    previous_time: datetime | None = None
    for sequence, record in enumerate(sorted(records, key=lambda row: row["sequence"]), start=1):
        _require(type(record["sequence"]) is int and record["sequence"] == sequence,
                 "noncontiguous_review_chain")
        _require(record[previous_field] == previous_id, "review_chain_fork_or_missing_predecessor")
        recorded_at = _instant(record["recorded_at"])
        _require(previous_time is None or recorded_at >= previous_time, "review_chain_time_regression")
        previous_id, previous_time = record[id_field], recorded_at


def _validate_export(raw: Mapping[str, Any], *, expected_owner_sub: str) -> dict[str, Any]:
    """Validate every record; return a detached canonical copy with current accepted heads.

    The caller's configured subject, not the JSON's subject, is the authority.
    Document bytes are not fetched here: hashes/quotes are retained as declarations.
    """
    _text(expected_owner_sub, "configured_owner_required", 256)
    _require(re.fullmatch(r"user_[0-9A-Za-z]+", expected_owner_sub) is not None,
             "configured_WorkOS_subject_required")
    export = decode_json(canonical_json(raw))
    _object(export, {"schema_version", "owner_sub", "proposals", "decisions", "resolutions",
                     "accepted_events", "payload_sha256"}, "invalid_export_shape")
    _require(export["schema_version"] == EXPORT_VERSION, "unsupported_export_version")
    _require(export["owner_sub"] == expected_owner_sub, "export_owner_mismatch")
    supplied_hash = _sha(export["payload_sha256"])
    _require(digest({key: value for key, value in export.items() if key != "payload_sha256"})
             == supplied_hash, "export_hash_mismatch")
    citations: dict[str, Any] = {}
    documents: dict[str, str] = {}
    proposals: dict[str, dict[str, Any]] = {}
    for raw_proposal in _list(export["proposals"], 100000):
        # Earlier v1 exports had episode-only proposals. The new nullable source
        # obligation field adds lineage, never an inferred CUSIP or chain key.
        fields = (PROPOSAL_FIELDS if isinstance(raw_proposal, dict) and "source_obligation_id" in raw_proposal
                  else PROPOSAL_FIELDS - {"source_obligation_id"})
        proposal = _object(raw_proposal, fields, "invalid_proposal_shape")
        obligation_id = proposal.get("source_obligation_id")
        if obligation_id is not None:
            _text(obligation_id, "invalid_source_obligation_id", 256)
            _require(obligation_id != "episode_scope", "reserved_obligation_sentinel")
        proposal_id = _uuid(proposal["proposal_id"])
        _require(proposal_id not in proposals, "duplicate_proposal_id")
        _text(proposal["source_episode_id"], "episode_required", 256)
        _text(proposal["issuer_name"], "issuer_name_required", 512)
        _text(proposal["summary"], "proposal_summary_required")
        _sha(proposal["source_fingerprint"])
        _sha(proposal["source_manifest_sha256"])
        _require(proposal["state"] == "unreviewed_proposal", "proposal_is_not_adjudication")
        if proposal["proposed_event_date"] is not None:
            _day(proposal["proposed_event_date"])
        _require(proposal["proposed_event_type"] is None
                 or proposal["proposed_event_type"] in EVENT_TYPES, "invalid_event_type")
        suggestions = _list(proposal["proposed_cusips"], 1000)
        for value in suggestions:
            validate_cusip9(value, checksum=False)
        _require(suggestions == sorted(set(suggestions)), "noncanonical_proposed_cusips")
        for value in _list(proposal["uncertainties"], 100):
            _require(isinstance(value, str), "invalid_uncertainty")
        if proposal["source_as_of"] is not None:
            _instant(proposal["source_as_of"])
        _citations(proposal["citations"], citations, documents, maximum=1000, minimum=0,
                   cutoff=_instant(proposal["source_as_of"]) if proposal["source_as_of"] is not None else None)
        _prices(proposal["market_prices"])
        proposals[proposal_id] = proposal
    decisions: dict[str, dict[str, Any]] = {}
    decision_chains: dict[str, list[dict[str, Any]]] = {}
    idempotency_keys: set[str] = set()
    for raw_decision in _list(export["decisions"], 1000000):
        item = _object(raw_decision, DECISION_FIELDS | RECORD_FIELDS | {"decision_id"},
                       "invalid_decision_shape")
        record_id = _uuid(item["decision_id"])
        proposal_id = _uuid(item["proposal_id"])
        key = _uuid(item["idempotency_key"])
        _require(record_id not in decisions and key not in idempotency_keys, "duplicate_decision_or_retry_key")
        idempotency_keys.add(key)
        _require(proposal_id in proposals, "decision_unknown_proposal")
        _require(item["reviewer_sub"] == expected_owner_sub, "reviewer_owner_mismatch")
        _require(type(item["sequence"]) is int and item["sequence"] > 0, "invalid_sequence")
        if item["expected_previous_decision_id"] is not None:
            _uuid(item["expected_previous_decision_id"])
        _require(digest({key: item[key] for key in DECISION_FIELDS}) == _sha(item["request_sha256"]),
                 "decision_request_hash_mismatch")
        recorded_at = _instant(item["recorded_at"])
        _text(item["rationale"], "decision_rationale_required")
        _require(item["decision"] in {"accept", "reject", "not_a_default"}, "invalid_disposition")
        _citations(item["citations"], citations, documents, maximum=100, cutoff=recorded_at)
        if item["confirmed_cusip"] is not None:
            validate_cusip9(item["confirmed_cusip"])
        if item["event_date"] is not None:
            _require(_day(item["event_date"]) <= recorded_at.date(), "event_after_recording")
        _require(item["event_type"] is None or item["event_type"] in EVENT_TYPES, "invalid_event_type")
        for field in ("issue_scope", "seniority", "collateral_description"):
            if item[field] is not None:
                _text(item[field], "invalid_issue_statement", 256 if field == "seniority" else 8000)
        _require(item["security_status"] is None or item["security_status"] in
                 {"secured", "unsecured", "partially_secured", "unknown"}, "invalid_security_status")
        if item["decision"] == "accept":
            _require(all(item[field] is not None for field in
                         ("confirmed_cusip", "issue_scope", "event_date", "event_type", "seniority",
                          "security_status", "collateral_description")), "accepted_issue_evidence_required")
        _prices(item["market_prices"], event_date=item["event_date"], require_confirmation=True)
        decisions[record_id] = item
        decision_chains.setdefault(proposal_id, []).append(item)
    for proposal_id, chain in decision_chains.items():
        _check_chain(chain, previous_field="expected_previous_decision_id", id_field="decision_id")
        # An immutable proposal must have been available at its review origin,
        # even if rejected or later superseded. Unknown legacy dates stay unknown.
        first_recorded_at = _instant(min(chain, key=lambda row: row["sequence"])["recorded_at"])
        proposal = proposals[proposal_id]
        _require(proposal["source_as_of"] is None
                 or _instant(proposal["source_as_of"]) <= first_recorded_at,
                 "proposal_after_first_decision")
        for citation in proposal["citations"]:
            _require(citation["public_at"] is None
                     or _instant(citation["public_at"]) <= first_recorded_at,
                     "proposal_citation_after_first_decision")
    resolutions: dict[str, dict[str, Any]] = {}
    resolution_chains: dict[str, list[dict[str, Any]]] = {}
    idempotency_keys = set()
    for raw_resolution in _list(export["resolutions"], 1000000):
        item = _object(raw_resolution, RESOLUTION_FIELDS | RECORD_FIELDS | {"resolution_id"},
                       "invalid_resolution_shape")
        record_id = _uuid(item["resolution_id"])
        decision_id = _uuid(item["decision_id"])
        proposal_id = _uuid(item["proposal_id"])
        key = _uuid(item["idempotency_key"])
        _require(record_id not in resolutions and key not in idempotency_keys, "duplicate_resolution_or_retry_key")
        idempotency_keys.add(key)
        _require(item["reviewer_sub"] == expected_owner_sub, "reviewer_owner_mismatch")
        _require(type(item["sequence"]) is int and item["sequence"] > 0, "invalid_sequence")
        if item["expected_previous_resolution_id"] is not None:
            _uuid(item["expected_previous_resolution_id"])
        _require(digest({key: item[key] for key in RESOLUTION_FIELDS}) == _sha(item["request_sha256"]),
                 "resolution_request_hash_mismatch")
        accepted = decisions.get(decision_id)
        _require(accepted is not None and accepted["decision"] == "accept"
                 and accepted["proposal_id"] == proposal_id, "resolution_issue_or_decision_mismatch")
        recorded_at = _instant(item["recorded_at"])
        resolution_day = _day(item["resolution_date"])
        _require(_instant(accepted["recorded_at"]) <= recorded_at
                 and _day(accepted["event_date"]) <= resolution_day <= recorded_at.date(),
                 "invalid_resolution_time")
        # A resolution may remain in history after its acceptance is superseded,
        # but it may not be newly appended to an already superseded decision.
        later = [row for row in decision_chains[proposal_id] if row["sequence"] > accepted["sequence"]]
        _require(not later or recorded_at <= min(_instant(row["recorded_at"]) for row in later),
                 "resolution_after_supersession")
        _text(item["resolution_type"], "resolution_type_required", 256)
        _text(item["valuation_description"], "valuation_description_required")
        _number(item["recovery_per_100"])
        _require(item["recovery_form"] in {"cash", "new_debt", "equity", "mixed"}, "invalid_recovery_form")
        _require(item["valuation_basis"] in {"estimated", "realized"}, "invalid_valuation_basis")
        _citations(item["citations"], citations, documents, maximum=100, cutoff=recorded_at)
        resolutions[record_id] = item
        resolution_chains.setdefault(decision_id, []).append(item)
    for chain in resolution_chains.values():
        _check_chain(chain, previous_field="expected_previous_resolution_id", id_field="resolution_id")
    heads = [max(chain, key=lambda row: row["sequence"]) for chain in decision_chains.values()]
    accepted = sorted((row for row in heads if row["decision"] == "accept"), key=lambda row: row["decision_id"])
    supplied = _list(export["accepted_events"], 100000)
    _require(sorted(supplied, key=lambda row: row.get("decision_id", "") if isinstance(row, dict) else "")
             == accepted, "accepted_projection_mismatch")
    return export


def validate_export(raw: Mapping[str, Any], *, expected_owner_sub: str) -> dict[str, Any]:
    """Fail-loud and sanitized even for malformed/unhashable JSON field values."""
    try:
        return _validate_export(raw, expected_owner_sub=expected_owner_sub)
    except OwnerEvidenceError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise OwnerEvidenceError("invalid_export_value") from exc


def validate_rating_binding(raw: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    value = decode_json(canonical_json(raw))
    _object(value, {"product", "publication_id", "policy_digest", "parent_panel_publication_id", "last_month"},
            "invalid_internal_rating_binding")
    _require(value["product"] == "bond_market_implied_rating_v1", "internal_rating_reference_required")
    _uuid(value["publication_id"])
    _uuid(value["parent_panel_publication_id"])
    _sha(value["policy_digest"])
    _require(_day(value["last_month"]).day == 1, "invalid_binding_month")
    return value


def binding_warnings(reference: Mapping[str, Any] | None,
                     current: Mapping[str, Any] | None = None) -> tuple[str, ...]:
    """Auxiliary evidence is not a day-one freshness dependency of market EL."""
    reference = validate_rating_binding(reference)
    current = validate_rating_binding(current)
    if reference is None:
        return ("auxiliary_internal_rating_binding_absent",)
    if current is None:
        return ("auxiliary_internal_rating_binding_currentness_unverified",)
    if reference == current:
        return ()
    code = ("auxiliary_internal_rating_binding_stale" if reference["last_month"] < current["last_month"]
            else "auxiliary_internal_rating_binding_mismatch")
    return (code,)


def build_bundle(raw: Mapping[str, Any], *, expected_owner_sub: str, code_revision: str,
                 knowledge_cutoff: str, rating_binding: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Create deterministic PREPARED artifact; no database mutation or authentication claim."""
    export = validate_export(raw, expected_owner_sub=expected_owner_sub)
    _text(code_revision, "code_revision_required", 256)
    cutoff = _instant(knowledge_cutoff)
    for item in export["decisions"] + export["resolutions"]:
        _require(_instant(item["recorded_at"]) <= cutoff, "record_after_knowledge_cutoff")
    for proposal in export["proposals"]:
        if proposal["source_as_of"] is not None:
            _require(_instant(proposal["source_as_of"]) <= cutoff, "proposal_after_knowledge_cutoff")
        for citation in proposal["citations"]:
            _require(citation["public_at"] is None or _instant(citation["public_at"]) <= cutoff,
                     "proposal_citation_after_knowledge_cutoff")
    policy = decode_json(POLICY_PATH.read_text(encoding="utf-8"))
    _require(policy["policy_version"] == POLICY_VERSION and policy["product"] == PRODUCT
             and policy["economic_authority"] is False, "invalid_owner_policy")
    policy_digest = digest(policy)
    code_digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    binding = validate_rating_binding(rating_binding)
    proposals = {item["proposal_id"]: item for item in export["proposals"]}
    events: list[dict[str, Any]] = []
    for decision in sorted(export["accepted_events"], key=lambda item: item["decision_id"]):
        proposal = proposals[decision["proposal_id"]]
        proof = {
            "basis": "owner_adjudicated_issue_scope", "cusip9": decision["confirmed_cusip"],
            "issue_scope": decision["issue_scope"], "decision_id": decision["decision_id"],
            "proposal_id": proposal["proposal_id"], "source_episode_id": proposal["source_episode_id"],
            "source_obligation_id": proposal.get("source_obligation_id"),
            "request_sha256": decision["request_sha256"],
            "citations": sorted(decision["citations"], key=lambda item: item["citation_id"]),
        }
        evidence_digest = digest(proof["citations"])
        link_digest = digest(proof)
        event_identity = {"decision_id": decision["decision_id"], "cusip9": decision["confirmed_cusip"],
                          "event_date": decision["event_date"], "event_type": decision["event_type"]}
        resolutions = sorted((item for item in export["resolutions"]
                              if item["decision_id"] == decision["decision_id"]), key=lambda item: item["sequence"])
        events.append({
            "event_id": str(uuid5(NAMESPACE, PRODUCT + "|event|" + digest(event_identity))),
            **event_identity, "proposal_id": proposal["proposal_id"],
            "source_episode_id": proposal["source_episode_id"],
            "source_obligation_id": proposal.get("source_obligation_id"), "owner_sub": expected_owner_sub,
            "recorded_at": decision["recorded_at"], "seniority": decision["seniority"],
            "security_status": decision["security_status"], "collateral_description": decision["collateral_description"],
            "evidence_sha256": evidence_digest, "link_sha256": link_digest, "issue_link": proof,
            "market_prices": decision["market_prices"], "resolutions": resolutions,
            "realized_recovery_per_100": (resolutions[-1]["recovery_per_100"]
                                          if resolutions and resolutions[-1]["valuation_basis"] == "realized" else None),
            "economic_authority": False,
        })
    events.sort(key=lambda item: item["event_id"])
    mapping_digest = digest(sorted((item["issue_link"] for item in events), key=lambda item: item["decision_id"]))
    pins = {
        "product": PRODUCT, "policy_version": POLICY_VERSION, "policy_digest": policy_digest,
        "code_revision": code_revision, "code_digest": code_digest,
        "export_sha256": export["payload_sha256"], "owner_sub": expected_owner_sub,
        "knowledge_cutoff": cutoff.isoformat(), "rating_binding": binding,
        "issuer_mapping_digest": mapping_digest, "events_sha256": digest(events),
    }
    publication_id = str(uuid5(NAMESPACE, PRODUCT + "|publication|" + digest(pins)))
    content = {
        "schema_version": PRODUCT + "_bundle", "publication_id": publication_id, **pins,
        "proposal_count": len(export["proposals"]), "decision_count": len(export["decisions"]),
        "resolution_count": len(export["resolutions"]), "accepted_event_count": len(events),
        "lifecycle_state": "prepared", "economic_authority": False,
        "authentication_claim": "offline_operator_import_not_JWT_verification",
        "source_export": export, "events": events,
    }
    return {**content, "bundle_sha256": digest(content)}


def verify_bundle(raw: Mapping[str, Any], *, expected_owner_sub: str) -> dict[str, Any]:
    """Replay source/projection/pins, not just a non-null receipt/digest."""
    bundle = decode_json(canonical_json(raw))
    try:
        rebuilt = build_bundle(bundle["source_export"], expected_owner_sub=expected_owner_sub,
                               code_revision=bundle["code_revision"], knowledge_cutoff=bundle["knowledge_cutoff"],
                               rating_binding=bundle["rating_binding"])
    except KeyError as exc:
        raise OwnerEvidenceError("invalid_bundle_shape") from exc
    _require(bundle == rebuilt, "bundle_replay_mismatch")
    return rebuilt


def recall_diagnostics(bundle: Mapping[str, Any], *, expected_owner_sub: str,
                       declared_cohort: Mapping[str, Any]) -> dict[str, Any]:
    """Explicit-cohort descriptive recall only; zero legal events never creates a gate.

    The caller must supply a hash-pinned declaration and ascertainment-complete
    event cohort/exposure. This does NOT establish legal-event surveillance or
    derive a safe cohort automatically from a model D label.
    """
    verified = verify_bundle(bundle, expected_owner_sub=expected_owner_sub)
    cohort = decode_json(canonical_json(declared_cohort))
    _object(cohort, {"cohort_id", "declaration_sha256", "legal_event_buckets",
                     "market_detected_event_ids", "exposure_by_bucket"}, "invalid_recall_cohort")
    _text(cohort["cohort_id"], "declared_cohort_required", 256)
    _require(digest({key: value for key, value in cohort.items() if key != "declaration_sha256"})
             == _sha(cohort["declaration_sha256"]), "recall_declaration_hash_mismatch")
    admitted = {item["event_id"] for item in verified["events"]}
    legal = cohort["legal_event_buckets"]
    exposure = cohort["exposure_by_bucket"]
    _require(isinstance(legal, dict) and isinstance(exposure, dict) and bool(exposure), "recall_exposure_required")
    _require(set(legal) <= admitted and all(bucket in RATED for bucket in legal.values()), "invalid_legal_cohort")
    _require(all(bucket in RATED and type(value) is int and value >= 0
                 for bucket, value in exposure.items()), "invalid_recall_exposure")
    detected = _list(cohort["market_detected_event_ids"], 100000)
    for event_id in detected:
        _uuid(event_id)
    _require(len(detected) == len(set(detected)) and set(detected) <= set(legal), "invalid_detected_cohort")
    results: dict[str, Any] = {}
    for bucket in exposure:
        ids = {key for key, value in legal.items() if value == bucket}
        _require(not ids or exposure[bucket] > 0, "recall_event_without_exposure")
        hits = len(ids & set(detected))
        results[bucket] = {"legal_events": len(ids), "market_detected_events": hits,
                           "exposure": exposure[bucket], "recall": hits / len(ids) if ids else None}
    _require(set(legal.values()) <= set(exposure), "recall_bucket_exposure_missing")
    return {"cohort_id": cohort["cohort_id"], "declaration_sha256": cohort["declaration_sha256"],
            "by_bucket": results, "purpose": "descriptive_protective_not_primary_PD",
            "automatic_blocker": False, "threshold": None}


class PostgresOwnerEvidenceStore:
    """Explicit persistence API over an already provisioned connection/schema.

    Callers own connection configuration. No credential lookup, schema install
    or pointer operation occurs at construction, prepare or validate. PostgreSQL
    execution requires separate authorization; tests may use a disposable DB.
    """

    def __init__(self, connection: Any, *, expected_owner_sub: str, schema: str = "public") -> None:
        _text(expected_owner_sub, "configured_owner_required", 256)
        _require(re.fullmatch(r"user_[0-9A-Za-z]+", expected_owner_sub) is not None,
                 "configured_WorkOS_subject_required")
        _require(re.fullmatch(r"[a-z_][a-z0-9_]*", schema) is not None, "invalid_schema")
        self.connection = connection
        self.schema = schema
        self.expected_owner_sub = expected_owner_sub

    def _table(self, suffix: str) -> str:
        return f'"{self.schema}"."bond_default_owner_evidence_v1_{suffix}"'

    def _stored(self, cursor: Any, publication_id: str) -> dict[str, Any]:
        cursor.execute(f"SELECT payload FROM {self._table('builds')} WHERE publication_id=%s FOR UPDATE",
                       (publication_id,))
        row = cursor.fetchone()
        _require(row is not None, "publication_not_prepared")
        bundle = verify_bundle(row[0], expected_owner_sub=self.expected_owner_sub)
        _require(bundle["publication_id"] == publication_id, "stored_publication_identity_mismatch")
        cursor.execute(f"SELECT event_id,payload FROM {self._table('events')} WHERE publication_id=%s ORDER BY event_id",
                       (publication_id,))
        rows = cursor.fetchall()
        actual = sorted((payload for _, payload in rows), key=lambda item: item.get("event_id", ""))
        _require(len(rows) == bundle["accepted_event_count"]
                 and all(str(event_id) == payload.get("event_id") for event_id, payload in rows)
                 and actual == bundle["events"] and digest(actual) == bundle["events_sha256"],
                 "stored_event_count_or_digest_mismatch")
        return bundle

    def prepare(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        bundle = verify_bundle(raw, expected_owner_sub=self.expected_owner_sub)
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (PRODUCT,))
            cursor.execute(
                f"INSERT INTO {self._table('builds')} "
                "(publication_id,policy_version,policy_digest,code_revision,code_digest,owner_sub,"
                "knowledge_cutoff,export_sha256,issuer_mapping_digest,events_sha256,bundle_sha256,"
                "proposal_count,decision_count,resolution_count,accepted_event_count,payload) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) "
                "ON CONFLICT (publication_id) DO NOTHING",
                tuple(bundle[key] for key in ("publication_id", "policy_version", "policy_digest", "code_revision",
                      "code_digest", "owner_sub", "knowledge_cutoff", "export_sha256", "issuer_mapping_digest",
                      "events_sha256", "bundle_sha256", "proposal_count", "decision_count", "resolution_count",
                      "accepted_event_count")) + (canonical_json(bundle),),
            )
            cursor.execute(f"SELECT payload FROM {self._table('builds')} WHERE publication_id=%s FOR UPDATE",
                           (bundle["publication_id"],))
            _require(cursor.fetchone()[0] == bundle, "prepared_replay_conflict")
            # Existing validated rows cannot accept any insert, even a no-op.
            cursor.execute(f"SELECT publication_id FROM {self._table('validations')} WHERE publication_id=%s",
                           (bundle["publication_id"],))
            if cursor.fetchone() is None:
                for event in bundle["events"]:
                    cursor.execute(
                        f"INSERT INTO {self._table('events')} "
                        "(publication_id,event_id,decision_id,proposal_id,cusip9,event_date,event_type,link_sha256,payload) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) "
                        "ON CONFLICT (publication_id,event_id) DO NOTHING",
                        (bundle["publication_id"],) + tuple(event[key] for key in
                         ("event_id", "decision_id", "proposal_id", "cusip9", "event_date", "event_type", "link_sha256"))
                        + (canonical_json(event),),
                    )
            stored = self._stored(cursor, bundle["publication_id"])
        return {"publication_id": stored["publication_id"], "operation": "prepared_or_replayed",
                "accepted_event_count": stored["accepted_event_count"], "bundle_sha256": stored["bundle_sha256"]}

    def validate(self, publication_id: str) -> dict[str, Any]:
        _uuid(publication_id)
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (PRODUCT,))
            bundle = self._stored(cursor, publication_id)
            receipt = {"publication_id": publication_id, "bundle_sha256": bundle["bundle_sha256"],
                       "events_sha256": bundle["events_sha256"], "accepted_event_count": bundle["accepted_event_count"],
                       "verification": "replayed_export_and_stored_issue_rows"}
            cursor.execute(
                f"INSERT INTO {self._table('validations')} "
                "(publication_id,bundle_sha256,events_sha256,accepted_event_count,receipt) "
                "VALUES (%s,%s,%s,%s,%s::jsonb) ON CONFLICT (publication_id) DO NOTHING",
                (publication_id, receipt["bundle_sha256"], receipt["events_sha256"],
                 receipt["accepted_event_count"], canonical_json(receipt)),
            )
            cursor.execute(f"SELECT receipt FROM {self._table('validations')} WHERE publication_id=%s", (publication_id,))
            _require(cursor.fetchone()[0] == receipt, "validation_receipt_replay_conflict")
        return {**receipt, "state": "validated"}
