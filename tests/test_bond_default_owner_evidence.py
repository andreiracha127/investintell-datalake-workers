"""Synthetic owner exports: no real corpus, DB, credential, custody or economic activation."""

from __future__ import annotations

import copy
import hashlib
import json
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from src.bonds.default_events.owner_evidence import (
    DECISION_FIELDS,
    RESOLUTION_FIELDS,
    OwnerEvidenceError,
    PostgresOwnerEvidenceStore,
    binding_warnings,
    build_bundle,
    decode_json,
    recall_diagnostics,
    validate_export,
    verify_bundle,
)
from src.workers.bond_default_owner_evidence import main

OWNER = "user_01M3T6XRJPZD863AT9X0H11B6W"
CUTOFF = "2020-12-01T00:00:00Z"
ROOT = Path(__file__).resolve().parents[1]


def sha(value):
    # Independent stdlib oracle for the Light wire's hash contract.
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def uid(label):
    return str(uuid5(NAMESPACE_URL, "synthetic-owner-evidence/" + label))


def citation():
    return {"citation_id": uid("citation"), "source_document_id": "synthetic-document",
            "document_sha256": "a" * 64, "url": "https://www.sec.gov/Archives/synthetic-event.htm",
            "quote": "Synthetic issue scope: 037833100 and 594918104 notes; filing stated default.",
            "locator": "p. 1", "public_at": "2020-01-15T00:00:00Z"}


def proposal(label="first"):
    return {"proposal_id": uid("proposal-" + label), "source_episode_id": "same-issuer-episode",
            "source_obligation_id": "synthetic-obligation-" + label,
            "issuer_name": "Synthetic issuer", "summary": "Unreviewed source proposal",
            "proposed_event_date": "2020-01-15", "proposed_event_type": "chapter_11",
            "proposed_cusips": ["037833100"], "citations": [citation()], "uncertainties": [],
            "market_prices": [], "source_fingerprint": "b" * 64, "source_manifest_sha256": "c" * 64,
            "source_as_of": "2020-01-16T00:00:00Z", "state": "unreviewed_proposal"}


def decision(p=None, *, status="accept", sequence=1, previous=None, label="first", cusip="037833100"):
    p = p or proposal()
    request = {
        "idempotency_key": uid("decision-retry-" + label), "expected_previous_decision_id": previous,
        "decision": status, "rationale": "Owner read cited documents and confirmed the exact issue scope.",
        "confirmed_cusip": cusip if status == "accept" else None,
        "issue_scope": "The cited exact issue notes" if status == "accept" else None,
        "event_date": "2020-01-15" if status == "accept" else None,
        "event_type": "chapter_11" if status == "accept" else None,
        "seniority": "senior" if status == "accept" else None,
        "security_status": "unsecured" if status == "accept" else None,
        "collateral_description": "No collateral" if status == "accept" else None,
        "citations": [citation()], "market_prices": [],
    }
    return {**request, "decision_id": uid("decision-" + label), "proposal_id": p["proposal_id"],
            "sequence": sequence, "reviewer_sub": OWNER, "recorded_at": "2020-01-20T00:00:00Z",
            "request_sha256": sha(request)}


def resolution(d, *, basis="realized", sequence=1, previous=None, label="first"):
    request = {"idempotency_key": uid("resolution-retry-" + label), "decision_id": d["decision_id"],
               "expected_previous_resolution_id": previous, "resolution_type": "confirmed_plan_distribution",
               "resolution_date": "2020-02-01", "recovery_per_100": 42.5, "recovery_form": "mixed",
               "valuation_basis": basis, "valuation_description": "Cash plus replacement notes",
               "citations": [citation()]}
    return {**request, "resolution_id": uid("resolution-" + label), "proposal_id": d["proposal_id"],
            "sequence": sequence, "reviewer_sub": OWNER, "recorded_at": "2020-02-20T00:00:00Z",
            "request_sha256": sha(request)}


def export(*, status="accept", zero=False):
    p = proposal()
    d = decision(p, status=status)
    value = {"schema_version": "bond_default_review_export_v1", "owner_sub": OWNER, "proposals": [p],
             "decisions": [] if zero else [d], "resolutions": [],
             "accepted_events": [d] if status == "accept" and not zero else []}
    return rehash(value)


def rehash(value):
    value["payload_sha256"] = sha({key: item for key, item in value.items() if key != "payload_sha256"})
    return value


def rehash_decision(value):
    value["request_sha256"] = sha({key: value[key] for key in DECISION_FIELDS})


def rehash_resolution(value):
    value["request_sha256"] = sha({key: value[key] for key in RESOLUTION_FIELDS})


def build(value=None, **kwargs):
    return build_bundle(export() if value is None else value, expected_owner_sub=OWNER,
                        code_revision="local-uncommitted-synthetic", knowledge_cutoff=CUTOFF, **kwargs)


def producer_source_sha():
    # Fresh fixtures pin the runtime bytes, never a hardcoded current-code hash.
    return hashlib.sha256((ROOT / "src/bonds/default_events/owner_evidence.py").read_bytes()).hexdigest()


def publication_pins(bundle):
    return {key: bundle[key] for key in (
        "product", "policy_version", "policy_digest", "code_revision", "code_digest",
        "export_sha256", "owner_sub", "knowledge_cutoff", "rating_binding",
        "issuer_mapping_digest", "events_sha256",
    )}


def oracle_publication_id(pins):
    return str(uuid5(UUID("60f25eab-91f1-565b-bdb2-81bc91465e80"),
                     "bond_default_owner_evidence_v1|publication|" + sha(pins)))


def seal_proposal_only_change(bundle, value):
    # Adversarial, fully rehashed fresh fixture; never repins an actual old bundle.
    result = copy.deepcopy(bundle)
    result["source_export"] = rehash(value)
    result["export_sha256"] = value["payload_sha256"]
    result["publication_id"] = oracle_publication_id(publication_pins(result))
    result["bundle_sha256"] = sha({key: item for key, item in result.items() if key != "bundle_sha256"})
    return result


def chronology_export(*, status="accept", source_as_of=None, citation_times=()):
    value = export(status=status)
    p = value["proposals"][0]
    p["source_as_of"] = source_as_of
    p["citations"] = [
        {**citation(), "citation_id": uid("proposal-citation-" + str(index)),
         "source_document_id": "synthetic-proposal-document-" + str(index),
         "document_sha256": sha("synthetic-proposal-document-" + str(index)), "public_at": instant}
        for index, instant in enumerate(citation_times)
    ]
    return rehash(value)


def origin_violations(value):
    # Independent UTC chronology oracle, not the production timestamp validator.
    violations = set()
    for p in value["proposals"]:
        chain = [d for d in value["decisions"] if d["proposal_id"] == p["proposal_id"]]
        if not chain:
            continue
        origin = datetime.fromisoformat(min(chain, key=lambda d: d["sequence"])["recorded_at"]).astimezone(UTC)
        if p["source_as_of"] is not None and datetime.fromisoformat(p["source_as_of"]).astimezone(UTC) > origin:
            violations.add("proposal_after_first_decision")
        if any(c["public_at"] is not None and datetime.fromisoformat(c["public_at"]).astimezone(UTC) > origin
               for c in p["citations"]):
            violations.add("proposal_citation_after_first_decision")
    return violations


def market_price(month="2020-02-01", *, confirmed=True):
    return {"month": month, "price_per_100": 35.0,
            "source_publication_id": uid("distinct-market-publication-" + month),
            "source_reference": "Synthetic market panel for " + month + "; not the January legal document",
            "owner_confirmed": confirmed, "interpretation": "market_recovery_proxy_not_realized"}


def market_export(*, status="accept", recorded_at="2020-02-01T00:00:00Z"):
    value = export(status=status)
    value["proposals"][0]["source_as_of"] = "2020-01-15T00:00:00Z"
    d = value["decisions"][0]
    d.update(recorded_at=recorded_at, event_date="2020-01-15", market_prices=[market_price()])
    rehash_decision(d)
    return rehash(value)


def market_time_violations(value, *, knowledge_cutoff=None):
    # Independent stdlib UTC/month-start oracle, not the producer's _prices/_instant.
    violations = set()
    cutoff_day = (datetime.fromisoformat(knowledge_cutoff).astimezone(UTC).date()
                  if knowledge_cutoff is not None else None)
    for kind in ("proposals", "decisions"):
        for item in value[kind]:
            instant = item["recorded_at"] if kind == "decisions" else item["source_as_of"]
            local_day = datetime.fromisoformat(instant).astimezone(UTC).date() if instant is not None else None
            for price in item["market_prices"]:
                month = date.fromisoformat(price["month"])
                if local_day is not None and month > local_day:
                    violations.add("market_proxy_after_recording" if kind == "decisions"
                                   else "market_proxy_after_knowledge_cutoff")
                if cutoff_day is not None and month > cutoff_day:
                    violations.add("market_proxy_after_knowledge_cutoff")
    return violations


def assert_export_hashes(value):
    for d in value["decisions"]:
        assert d["request_sha256"] == sha({key: d[key] for key in DECISION_FIELDS})
    assert value["payload_sha256"] == sha({key: item for key, item in value.items() if key != "payload_sha256"})


def seal_market_time_change(bundle, value, *, knowledge_cutoff=None):
    # Fresh adversarial fixtures only. Recompute projection/link/pins/full SHA so
    # replay reaches semantic chronology, not a stale request or bundle hash.
    result = copy.deepcopy(bundle)
    decisions = {d["decision_id"]: d for d in value["decisions"]}
    for event in result["events"]:
        d = decisions[event["decision_id"]]
        event["recorded_at"] = d["recorded_at"]
        event["market_prices"] = copy.deepcopy(d["market_prices"])
        event["issue_link"]["request_sha256"] = d["request_sha256"]
        event["link_sha256"] = sha(event["issue_link"])
    result["issuer_mapping_digest"] = sha(sorted(
        (e["issue_link"] for e in result["events"]), key=lambda item: item["decision_id"]))
    result["events_sha256"] = sha(result["events"])
    if knowledge_cutoff is not None:
        result["knowledge_cutoff"] = datetime.fromisoformat(knowledge_cutoff).astimezone(UTC).isoformat()
    result = seal_proposal_only_change(result, value)
    assert_export_hashes(result["source_export"])
    assert result["code_digest"] == producer_source_sha()
    assert result["publication_id"] == oracle_publication_id(publication_pins(result))
    assert result["events_sha256"] == sha(result["events"])
    assert result["bundle_sha256"] == sha({key: item for key, item in result.items() if key != "bundle_sha256"})
    return result


def test_one_accepted_event_has_explicit_issue_proof_and_derived_mapping_digest():
    value = build()
    assert value["accepted_event_count"] == 1
    item = value["events"][0]
    assert item["cusip9"] == "037833100"
    assert item["issue_link"]["basis"] == "owner_adjudicated_issue_scope"
    assert item["link_sha256"] == sha(item["issue_link"])
    assert value["issuer_mapping_digest"] == sha([item["issue_link"]])
    assert value["events_sha256"] == sha(value["events"])
    assert item["realized_recovery_per_100"] is None
    assert value["economic_authority"] is False
    assert value["authentication_claim"] == "offline_operator_import_not_JWT_verification"
    assert verify_bundle(value, expected_owner_sub=OWNER) == value == build()


def test_no_decisions_is_legitimate_and_never_auto_admits_valid_suggestions():
    value = build(export(zero=True))
    assert value["proposal_count"] == 1 and value["accepted_event_count"] == 0
    assert value["events"] == [] and value["issuer_mapping_digest"] == sha([])


@pytest.mark.parametrize("status", ["accept", "reject", "not_a_default"])
def test_three_distinct_dispositions_retained_in_source_ledger(status):
    value = build(export(status=status))
    assert value["source_export"]["decisions"][0]["decision"] == status
    assert value["accepted_event_count"] == int(status == "accept")


def test_two_issues_same_episode_remain_two_independent_acceptances():
    value = export()
    second = proposal("second-issue")
    second["proposed_cusips"] = ["594918104"]
    d = decision(second, label="second-issue", cusip="594918104")
    value["proposals"].append(second)
    value["decisions"].append(d)
    value["accepted_events"].append(d)
    bundle = build(rehash(value))
    assert {item["cusip9"] for item in bundle["events"]} == {"037833100", "594918104"}
    assert len({item["source_episode_id"] for item in bundle["events"]}) == 1
    assert len({item["proposal_id"] for item in bundle["events"]}) == 2


def test_superseding_one_issue_does_not_remove_other_issue_and_preserves_old_decision():
    value = export()
    second = proposal("second")
    d2 = decision(second, label="second", cusip="594918104")
    correction = decision(status="not_a_default", sequence=2,
                          previous=value["decisions"][0]["decision_id"], label="correction")
    correction["recorded_at"] = "2020-03-01T00:00:00Z"
    value["proposals"].append(second)
    value["decisions"].extend([d2, correction])
    value["accepted_events"] = [d2]
    bundle = build(rehash(value))
    assert [item["cusip9"] for item in bundle["events"]] == ["594918104"]
    assert len(bundle["source_export"]["decisions"]) == 3


@pytest.mark.parametrize("change,code", [
    (lambda v: v.update(owner_sub="user_FORGED"), "export_owner_mismatch"),
    (lambda v: v["decisions"][0].update(reviewer_sub="user_FORGED"), "reviewer_owner_mismatch"),
    (lambda v: v["decisions"][0].update(rationale="Hash changed"), "decision_request_hash_mismatch"),
    (lambda v: v.update(accepted_events=[]), "accepted_projection_mismatch"),
    (lambda v: v["proposals"][0].update(d_confirmed=True), "invalid_proposal_shape"),
])
def test_rehashed_export_does_not_bypass_semantic_owner_or_projection_checks(change, code):
    value = export()
    change(value)
    with pytest.raises(OwnerEvidenceError, match=code):
        build(rehash(value))


def test_hash_tamper_and_unconfigured_owner_refused():
    value = export()
    value["proposals"][0]["summary"] = "Tampered"
    with pytest.raises(OwnerEvidenceError, match="export_hash_mismatch"):
        build(value)
    with pytest.raises(OwnerEvidenceError, match="configured_owner_required"):
        validate_export(export(), expected_owner_sub="")
    with pytest.raises(OwnerEvidenceError, match="configured_WorkOS_subject_required"):
        validate_export(export(), expected_owner_sub="cloudflareaccess.com:test")


def test_fork_missing_predecessor_and_retry_duplication_refused():
    for mode in ("fork", "missing", "retry"):
        value = export()
        d = decision(sequence=2, previous=value["decisions"][0]["decision_id"], label="second")
        if mode == "fork":
            d["sequence"] = 1
            d["expected_previous_decision_id"] = None
        elif mode == "missing":
            d["expected_previous_decision_id"] = uid("missing")
        else:
            d["idempotency_key"] = value["decisions"][0]["idempotency_key"]
        rehash_decision(d)
        value["decisions"].append(d)
        value["accepted_events"] = [d]
        with pytest.raises(OwnerEvidenceError):
            build(rehash(value))


@pytest.mark.parametrize("field,new,code", [
    ("confirmed_cusip", "037833101", "invalid_cusip_checksum"),
    ("confirmed_cusip", "037833", "invalid_cusip9"),
    ("issue_scope", None, "accepted_issue_evidence_required"),
    ("event_type", "implied_D", "invalid_event_type"),
    ("seniority", None, "accepted_issue_evidence_required"),
    ("security_status", "nport_name_match", "invalid_security_status"),
    ("collateral_description", None, "accepted_issue_evidence_required"),
    ("event_date", "2030-01-01", "event_after_recording"),
])
def test_exact_admitted_issue_and_legal_event_fields_required(field, new, code):
    value = export()
    value["decisions"][0][field] = new
    rehash_decision(value["decisions"][0])
    with pytest.raises(OwnerEvidenceError, match=code):
        build(rehash(value))


@pytest.mark.parametrize("field,new", [("quote", ""), ("document_sha256", "bogus"),
                                       ("url", "file:///secret"), ("url", "https://user:password@example.org")])
def test_citations_require_public_url_quote_and_source_hash(field, new):
    value = export()
    value["decisions"][0]["citations"][0][field] = new
    rehash_decision(value["decisions"][0])
    with pytest.raises(OwnerEvidenceError):
        build(rehash(value))


def test_reusing_citation_identity_for_different_proof_refused():
    value = export()
    value["decisions"][0]["citations"][0]["quote"] = "Different quote but reused citation ID"
    rehash_decision(value["decisions"][0])
    with pytest.raises(OwnerEvidenceError, match="citation_identity_mismatch"):
        build(rehash(value))


@pytest.mark.parametrize("basis,expected", [("estimated", None), ("realized", 42.5)])
def test_optional_resolution_is_not_required_and_preserves_estimated_vs_realized(basis, expected):
    value = export()
    value["resolutions"] = [resolution(value["decisions"][0], basis=basis)]
    bundle = build(rehash(value))
    assert bundle["events"][0]["realized_recovery_per_100"] == expected
    assert bundle["events"][0]["resolutions"][0]["valuation_basis"] == basis


def test_resolution_issue_mismatch_or_fork_refused():
    value = export()
    r = resolution(value["decisions"][0])
    r["proposal_id"] = uid("another-proposal")
    value["resolutions"] = [r]
    with pytest.raises(OwnerEvidenceError, match="resolution_issue_or_decision_mismatch"):
        build(rehash(value))
    r["proposal_id"] = value["proposals"][0]["proposal_id"]
    r["expected_previous_resolution_id"] = uid("missing-resolution")
    rehash_resolution(r)
    with pytest.raises(OwnerEvidenceError, match="review_chain_fork"):
        build(rehash(value))


def test_market_price_kept_as_confirmed_proxy_never_as_realized_recovery():
    value = export()
    d = value["decisions"][0]
    d["market_prices"] = [{"month": "2020-01-01", "price_per_100": 35.0,
                           "source_publication_id": uid("price-publication"), "source_reference": "synthetic panel",
                           "owner_confirmed": True, "interpretation": "market_recovery_proxy_not_realized"}]
    rehash_decision(d)
    bundle = build(rehash(value))
    assert bundle["events"][0]["market_prices"][0]["price_per_100"] == 35
    assert bundle["events"][0]["realized_recovery_per_100"] is None
    d["market_prices"][0]["interpretation"] = "realized"
    rehash_decision(d)
    with pytest.raises(OwnerEvidenceError, match="market_price_is_not_realized"):
        build(rehash(value))


@pytest.mark.parametrize("status", ["accept", "reject", "not_a_default"])
@pytest.mark.parametrize("recorded_at", [
    "2020-01-15T00:00:00Z", "2020-01-31T23:59:59.999999Z",
    "2020-02-01T00:30:00+01:00", "2020-02-01T00:59:59.999999+01:00",
], ids=["original-early-January", "before-month-boundary", "UTC-still-January", "one-microsecond-before-boundary"])
def test_hashsealed_following_month_proxy_after_any_decision_recording_is_refused(status, recorded_at):
    cutoff = "2020-01-31T23:59:59.999999Z"
    value = market_export(status=status, recorded_at=recorded_at)
    d = value["decisions"][0]
    d["market_prices"] = [market_price("2020-01-01")]
    rehash_decision(d)
    valid_bundle = build_bundle(rehash(value), expected_owner_sub=OWNER, code_revision="synthetic-time-oracle",
                                knowledge_cutoff=cutoff)
    assert seal_market_time_change(valid_bundle, value) == valid_bundle
    d["market_prices"] = [market_price()]
    rehash_decision(d)
    rehash(value)
    assert_export_hashes(value)
    assert d["citations"] == [citation()]  # A valid January legal document, not the distinct February market source.
    assert d["market_prices"][0]["source_publication_id"] != d["citations"][0]["citation_id"]
    assert d["market_prices"][0]["owner_confirmed"] is True  # Confirmation does not prove time availability.
    assert market_time_violations(value, knowledge_cutoff=cutoff) == {
        "market_proxy_after_recording", "market_proxy_after_knowledge_cutoff",
    }
    sealed = seal_market_time_change(valid_bundle, value)
    before = copy.deepcopy(value)
    conn = FakeConnection()
    store = PostgresOwnerEvidenceStore(conn, expected_owner_sub=OWNER)
    for operation in (
        lambda: validate_export(value, expected_owner_sub=OWNER),
        lambda: build_bundle(value, expected_owner_sub=OWNER, code_revision="synthetic-time-oracle",
                             knowledge_cutoff=cutoff),
        lambda: verify_bundle(sealed, expected_owner_sub=OWNER),
        lambda: store.prepare(sealed),
    ):
        with pytest.raises(OwnerEvidenceError) as caught:
            operation()
        assert caught.value.code == str(caught.value) == "market_proxy_after_recording"
    assert value == before  # Refuse, never drop or mutate the future price.
    assert conn.statements == [] and conn.builds == conn.events == conn.receipts == {}


@pytest.mark.parametrize("first_status", ["accept", "reject", "not_a_default"])
@pytest.mark.parametrize("latest_status", ["accept", "reject", "not_a_default"])
def test_supersession_cannot_hide_old_future_market_proxy(first_status, latest_status):
    value = market_export(status=first_status)
    first = value["decisions"][0]
    latest = decision(status=latest_status, sequence=2, previous=first["decision_id"], label="market-supersession")
    latest["recorded_at"] = "2020-03-01T00:00:00Z"
    value["decisions"] = [latest, first]  # Serialization order is not review-chain order.
    value["accepted_events"] = [latest] if latest_status == "accept" else []
    valid_bundle = build(rehash(value))
    first["recorded_at"] = "2020-01-15T00:00:00Z"
    rehash_decision(first)
    rehash(value)
    assert_export_hashes(value)
    assert latest["market_prices"] == []
    assert market_time_violations(value) == {"market_proxy_after_recording"}
    sealed = seal_market_time_change(valid_bundle, value)
    conn = FakeConnection()
    for operation in (
        lambda: validate_export(value, expected_owner_sub=OWNER),
        lambda: build(value),
        lambda: verify_bundle(sealed, expected_owner_sub=OWNER),
        lambda: PostgresOwnerEvidenceStore(conn, expected_owner_sub=OWNER).prepare(sealed),
    ):
        with pytest.raises(OwnerEvidenceError) as caught:
            operation()
        assert caught.value.code == "market_proxy_after_recording"
    assert conn.statements == []


@pytest.mark.parametrize("status", ["unreviewed", "accept", "reject", "not_a_default"])
@pytest.mark.parametrize("confirmed", [False, True])
def test_known_proposal_source_as_of_also_bounds_its_market_proxies(status, confirmed):
    value = export(zero=status == "unreviewed", status="accept" if status == "unreviewed" else status)
    p = value["proposals"][0]
    p["market_prices"] = [market_price("2020-01-01", confirmed=confirmed)]
    valid_bundle = build(rehash(value))
    p["market_prices"] = [market_price(confirmed=confirmed)]
    rehash(value)
    assert_export_hashes(value)
    assert market_time_violations(value) == {"market_proxy_after_knowledge_cutoff"}
    sealed = seal_market_time_change(valid_bundle, value)
    for operation in (lambda: validate_export(value, expected_owner_sub=OWNER), lambda: build(value),
                      lambda: verify_bundle(sealed, expected_owner_sub=OWNER)):
        with pytest.raises(OwnerEvidenceError) as caught:
            operation()
        assert caught.value.code == "market_proxy_after_knowledge_cutoff"


@pytest.mark.parametrize("with_reviewed_issue", [False, True])
@pytest.mark.parametrize("confirmed", [False, True])
@pytest.mark.parametrize("cutoff", [
    "2020-01-31T23:59:59Z", "2020-01-31T20:59:59-03:00", "2020-02-01T05:29:59+05:30",
], ids=["UTC", "negative-offset-equivalent", "positive-offset-equivalent"])
def test_hashsealed_unreviewed_undated_proposal_future_price_obeys_global_bundle_cutoff(
    with_reviewed_issue, confirmed, cutoff,
):
    value = export(zero=not with_reviewed_issue)
    if with_reviewed_issue:
        p = proposal("unreviewed-future-market")
        value["proposals"].append(p)
    else:
        p = value["proposals"][0]
    p["source_as_of"] = None
    p["market_prices"] = [market_price(confirmed=confirmed)]
    rehash(value)
    assert_export_hashes(value)
    assert not any(d["proposal_id"] == p["proposal_id"] for d in value["decisions"])
    assert market_time_violations(value) == set()
    assert validate_export(value, expected_owner_sub=OWNER) == value  # No invented legacy timestamp/global cutoff.
    assert market_time_violations(value, knowledge_cutoff=cutoff) == {"market_proxy_after_knowledge_cutoff"}
    valid_bundle = build_bundle(value, expected_owner_sub=OWNER, code_revision="synthetic-global-cutoff",
                                knowledge_cutoff="2020-02-01T00:00:00Z")
    sealed = seal_market_time_change(valid_bundle, value, knowledge_cutoff=cutoff)
    before = copy.deepcopy(value)
    conn = FakeConnection()
    for operation in (
        lambda: build_bundle(value, expected_owner_sub=OWNER, code_revision="synthetic-global-cutoff",
                             knowledge_cutoff=cutoff),
        lambda: verify_bundle(sealed, expected_owner_sub=OWNER),
        lambda: PostgresOwnerEvidenceStore(conn, expected_owner_sub=OWNER).prepare(sealed),
    ):
        with pytest.raises(OwnerEvidenceError) as caught:
            operation()
        assert caught.value.code == str(caught.value) == "market_proxy_after_knowledge_cutoff"
    assert value == before and conn.statements == []
    assert conn.builds == conn.events == conn.receipts == {}


@pytest.mark.parametrize("status", ["accept", "reject", "not_a_default"])
@pytest.mark.parametrize("event_date,recorded_at,months", [
    ("2020-01-15", "2020-01-15T00:00:00Z", ["2020-01-01"]),
    ("2020-01-15", "2020-01-20T00:00:00Z", ["2020-01-01"]),
    ("2020-01-15", "2020-02-01T00:00:00Z", ["2020-01-01", "2020-02-01"]),
    ("2020-01-15", "2020-02-15T00:00:00Z", ["2020-02-01"]),
    ("2020-01-15", "2020-01-31T21:00:00-03:00", ["2020-02-01"]),
    ("2020-01-15", "2020-02-01T05:30:00+05:30", ["2020-02-01"]),
    ("2020-12-15", "2020-12-15T00:00:00Z", ["2020-12-01"]),
    ("2020-12-15", "2021-01-01T00:00:00Z", ["2020-12-01", "2021-01-01"]),
    ("2020-02-29", "2020-03-01T00:00:00Z", ["2020-03-01"]),
], ids=["same-event-day", "past-event-month", "following-month-start", "same-month-not-closed",
        "negative-offset-UTC-February", "positive-offset-equivalent", "December-event-month",
        "year-boundary", "leap-event-next-month"])
def test_valid_market_month_starts_preserve_prices_without_inventing_full_month_closure(
    status, event_date, recorded_at, months,
):
    value = market_export(status=status, recorded_at=recorded_at)
    d = value["decisions"][0]
    d["event_date"] = event_date
    d["market_prices"] = [market_price(month) for month in months]
    rehash_decision(d)
    rehash(value)
    before = copy.deepcopy(value)
    assert market_time_violations(value, knowledge_cutoff=recorded_at) == set()
    bundle = build_bundle(value, expected_owner_sub=OWNER, code_revision="synthetic-valid-time",
                          knowledge_cutoff=recorded_at)
    assert verify_bundle(bundle, expected_owner_sub=OWNER) == bundle
    assert seal_market_time_change(bundle, value) == bundle  # Independent full sealing agrees for valid cases.
    assert value == before == bundle["source_export"]
    assert bundle["accepted_event_count"] == int(status == "accept")
    if status == "accept":
        assert bundle["events"][0]["market_prices"] == d["market_prices"]
        assert bundle["events"][0]["realized_recovery_per_100"] is None
        assert bundle["events"][0]["economic_authority"] is False


@pytest.mark.parametrize("month,source_as_of,cutoff", [
    ("2019-12-01", "2020-01-15T00:00:00Z", "2020-01-15T00:00:00Z"),
    ("2020-01-01", "2020-01-15T00:00:00Z", "2020-01-15T00:00:00Z"),
    ("2020-02-01", "2020-02-01T00:00:00Z", "2020-02-01T00:00:00Z"),
    ("2020-02-01", "2020-01-31T21:00:00-03:00", "2020-02-01T05:30:00+05:30"),
    ("2020-02-01", "2020-02-15T00:00:00Z", "2020-02-15T00:00:00Z"),
    ("2020-01-01", None, "2020-01-15T00:00:00Z"),
    ("2020-02-01", None, "2020-02-01T00:00:00Z"),
], ids=["past-month", "observed-event-month", "exact-month-boundary", "offset-equivalent-boundary",
        "same-month-not-closed", "undated-event-month", "undated-February-boundary"])
def test_valid_unreviewed_proposal_prices_remain_visible_and_are_not_default_events(month, source_as_of, cutoff):
    value = export(zero=True)
    p = value["proposals"][0]
    p["source_as_of"] = source_as_of
    p["market_prices"] = [market_price(month, confirmed=False)]
    rehash(value)
    assert market_time_violations(value, knowledge_cutoff=cutoff) == set()
    bundle = build_bundle(value, expected_owner_sub=OWNER, code_revision="synthetic-valid-proposal",
                          knowledge_cutoff=cutoff)
    assert verify_bundle(bundle, expected_owner_sub=OWNER) == bundle
    assert bundle["source_export"] == value and bundle["events"] == []
    assert bundle["accepted_event_count"] == 0 and bundle["economic_authority"] is False


@pytest.mark.parametrize("change,code", [
    (lambda price: price.update(owner_confirmed=False), "market_proxy_requires_owner_confirmation"),
    (lambda price: price.update(month="2019-12-01"), "invalid_market_proxy_month"),
])
def test_time_valid_market_proxy_does_not_relax_confirmation_or_event_month_membership(change, code):
    value = market_export()
    d = value["decisions"][0]
    change(d["market_prices"][0])
    rehash_decision(d)
    with pytest.raises(OwnerEvidenceError) as caught:
        build(rehash(value))
    assert caught.value.code == code


def binding(month="2020-08-01"):
    return {"product": "bond_market_implied_rating_v1", "publication_id": uid("rating-" + month),
            "policy_digest": "d" * 64, "parent_panel_publication_id": uid("panel-" + month), "last_month": month}


def test_binding_absent_stale_and_foreign_are_descriptive_not_market_EL_gates():
    value = build(rating_binding=binding())
    assert binding_warnings(value["rating_binding"], binding("2020-09-01")) == (
        "auxiliary_internal_rating_binding_stale",)
    assert binding_warnings(None) == ("auxiliary_internal_rating_binding_absent",)
    assert binding_warnings(binding(), binding()) == ()
    other = binding()
    other["policy_digest"] = "e" * 64
    assert binding_warnings(binding(), other) == ("auxiliary_internal_rating_binding_mismatch",)
    assert value["economic_authority"] is False and value["accepted_event_count"] == 1


def test_knowledge_cutoff_refuses_future_records_not_stale_rating_evidence():
    with pytest.raises(OwnerEvidenceError, match="record_after_knowledge_cutoff"):
        build_bundle(export(), expected_owner_sub=OWNER, code_revision="synthetic",
                     knowledge_cutoff="2020-01-19T00:00:00Z")


def test_bundle_identity_changes_on_explicit_issue_scope_not_arbitrary_mapping_pin():
    first = build()
    value = export()
    value["decisions"][0]["issue_scope"] = "Owner corrected exact cited note scope"
    rehash_decision(value["decisions"][0])
    second = build(rehash(value))
    assert first["issuer_mapping_digest"] != second["issuer_mapping_digest"]
    assert first["publication_id"] != second["publication_id"]
    forged = copy.deepcopy(first)
    forged["issuer_mapping_digest"] = "f" * 64
    with pytest.raises(OwnerEvidenceError, match="bundle_replay_mismatch"):
        verify_bundle(forged, expected_owner_sub=OWNER)


def test_recall_explicit_cohort_and_exposure_no_invented_sample_or_threshold():
    bundle = build()
    event_id = bundle["events"][0]["event_id"]
    cohort = {"cohort_id": "explicit-complete-cohort", "legal_event_buckets": {event_id: "B"},
              "market_detected_event_ids": [], "exposure_by_bucket": {"B": 10}}
    cohort["declaration_sha256"] = sha(cohort)
    result = recall_diagnostics(bundle, expected_owner_sub=OWNER, declared_cohort=cohort)
    assert result["by_bucket"]["B"]["recall"] == 0.0
    assert result["automatic_blocker"] is False and result["threshold"] is None
    cohort["exposure_by_bucket"]["B"] = 0
    cohort["declaration_sha256"] = sha({k: v for k, v in cohort.items() if k != "declaration_sha256"})
    with pytest.raises(OwnerEvidenceError, match="recall_event_without_exposure"):
        recall_diagnostics(bundle, expected_owner_sub=OWNER, declared_cohort=cohort)


def test_zero_adjudication_recall_remains_partial_not_zero_default_claim():
    cohort = {"cohort_id": "explicit-empty-legal-cohort", "legal_event_buckets": {},
              "market_detected_event_ids": [], "exposure_by_bucket": {"B": 10}}
    cohort["declaration_sha256"] = sha(cohort)
    result = recall_diagnostics(build(export(zero=True)), expected_owner_sub=OWNER, declared_cohort=cohort)
    assert result["by_bucket"]["B"]["recall"] is None and result["automatic_blocker"] is False


def test_duplicate_json_keys_and_nonfinite_json_refused():
    with pytest.raises(OwnerEvidenceError, match="duplicate_json_key"):
        decode_json('{"owner_sub":"x","owner_sub":"y"}')
    with pytest.raises(OwnerEvidenceError, match="noncanonical_json"):
        decode_json('{"amount":NaN}')


def test_cli_default_is_offline_and_only_explicit_output_writes(tmp_path, capsys):
    source = tmp_path / "synthetic-export.json"
    source.write_text(json.dumps(export()), encoding="utf-8")
    argv = ["--input", str(source), "--owner-sub", OWNER, "--code-revision", "synthetic-uncommitted",
            "--knowledge-cutoff", CUTOFF]
    assert main(argv) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["state"] == "offline_verified_not_persisted" and report["pointer_changed"] is False
    assert sorted(item.name for item in tmp_path.iterdir()) == [source.name]
    output = tmp_path / "bundle.json"
    assert main([*argv, "--output", str(output)]) == 0
    capsys.readouterr()
    assert verify_bundle(json.loads(output.read_text(encoding="utf-8")), expected_owner_sub=OWNER)["accepted_event_count"] == 1
    assert main([*argv, "--output", str(output)]) == 3  # never overwrite an operator artifact


def test_cli_sanitizes_refusals_and_has_no_production_or_promotion_mode(tmp_path, capsys):
    source = tmp_path / "bad-export.json"
    value = export()
    value["decisions"][0]["reviewer_sub"] = "user_SECRETLIKE"
    source.write_text(json.dumps(rehash(value)), encoding="utf-8")
    argv = ["--input", str(source), "--owner-sub", OWNER, "--code-revision", "synthetic",
            "--knowledge-cutoff", CUTOFF]
    assert main(argv) == 4
    report = capsys.readouterr().out
    assert "SECRETLIKE" not in report and "reviewer_owner_mismatch" in report
    with pytest.raises(SystemExit):
        main([*argv, "--mode", "promote"])


@pytest.mark.parametrize("status", ["accept", "reject", "not_a_default"])
@pytest.mark.parametrize("source_as_of,citation_times,code", [
    ("2020-06-01T00:00:00Z", ["2020-01-15T00:00:00Z"], "proposal_after_first_decision"),
    (None, ["2020-01-15T00:00:00Z", "2020-06-01T00:00:00Z"], "proposal_citation_after_first_decision"),
    ("2020-06-01T00:00:00Z", ["2020-06-01T00:00:00Z"], "proposal_after_first_decision"),
], ids=["source-only", "citation-only", "source-and-citation"])
def test_hashsealed_future_proposal_refused_at_validation_build_and_replay(status, source_as_of, citation_times, code):
    value = chronology_export(status=status, source_as_of=source_as_of, citation_times=citation_times)
    first = value["decisions"][0]
    assert first["recorded_at"] == "2020-01-20T00:00:00Z"
    assert first["citations"] == [citation()]  # Different valid January document, not the future proposal citation.
    assert {c["citation_id"] for c in value["proposals"][0]["citations"]}.isdisjoint(
        c["citation_id"] for c in first["citations"])
    assert first["request_sha256"] == sha({key: first[key] for key in DECISION_FIELDS})
    assert value["payload_sha256"] == sha({key: item for key, item in value.items() if key != "payload_sha256"})
    assert code in origin_violations(value)
    sealed = seal_proposal_only_change(build(export(status=status)), value)
    assert sealed["bundle_sha256"] == sha({key: item for key, item in sealed.items() if key != "bundle_sha256"})
    for operation in (
        lambda: validate_export(value, expected_owner_sub=OWNER),
        lambda: build(value),
        lambda: verify_bundle(sealed, expected_owner_sub=OWNER),
    ):
        with pytest.raises(OwnerEvidenceError) as caught:
            operation()
        assert caught.value.code == str(caught.value) == code


@pytest.mark.parametrize("first_status", ["accept", "reject", "not_a_default"])
@pytest.mark.parametrize("latest_status", ["accept", "reject", "not_a_default"])
@pytest.mark.parametrize("kind,code", [
    ("source", "proposal_after_first_decision"),
    ("citation", "proposal_citation_after_first_decision"),
])
def test_later_supersession_cannot_hide_impossible_review_origin(first_status, latest_status, kind, code):
    value = export(status=first_status)
    first = value["decisions"][0]
    latest = decision(status=latest_status, sequence=2, previous=first["decision_id"], label="later-revision")
    latest["recorded_at"] = "2020-07-01T00:00:00Z"
    value["decisions"] = [latest, first]  # Wire order is deliberately not chain order.
    value["accepted_events"] = [latest] if latest_status == "accept" else []
    valid_bundle = build(rehash(value))
    p = value["proposals"][0]
    future = chronology_export(source_as_of="2020-06-01T00:00:00Z" if kind == "source" else None,
                               citation_times=["2020-01-15T00:00:00Z" if kind == "source"
                                               else "2020-06-01T00:00:00Z"])["proposals"][0]
    p["source_as_of"], p["citations"] = future["source_as_of"], future["citations"]
    assert origin_violations(value) == {code}
    for operation in (lambda: build(rehash(value)),
                      lambda: verify_bundle(seal_proposal_only_change(valid_bundle, value), expected_owner_sub=OWNER)):
        with pytest.raises(OwnerEvidenceError) as caught:
            operation()
        assert caught.value.code == code


@pytest.mark.parametrize("mode,code", [
    ("fork", "noncontiguous_review_chain"),
    ("missing", "review_chain_fork_or_missing_predecessor"),
    ("regression", "review_chain_time_regression"),
])
def test_invalid_chain_is_not_reinterpreted_as_a_proposal_origin(mode, code):
    value = chronology_export(source_as_of="2020-06-01T00:00:00Z", citation_times=[])
    first = value["decisions"][0]
    later = decision(sequence=2, previous=first["decision_id"], label="fork-test")
    later["recorded_at"] = "2020-07-01T00:00:00Z"
    if mode == "fork":
        later.update(sequence=1, expected_previous_decision_id=None)
    elif mode == "missing":
        later["expected_previous_decision_id"] = uid("missing-predecessor")
    else:
        later["recorded_at"] = "2020-01-19T00:00:00Z"
    rehash_decision(later)
    value["decisions"].append(later)
    value["accepted_events"] = [later]
    with pytest.raises(OwnerEvidenceError) as caught:
        build(rehash(value))
    assert caught.value.code == code


@pytest.mark.parametrize("status", ["accept", "reject", "not_a_default"])
@pytest.mark.parametrize("instant", [
    "2020-01-20T00:00:00Z", "2020-01-19T21:00:00-03:00", "2020-01-20T05:30:00+05:30",
    "2020-01-19T23:59:59.999999Z",
], ids=["exact-boundary", "equivalent-negative-offset", "equivalent-positive-offset", "before-boundary"])
def test_proposal_origin_compares_utc_instants_and_accepts_exact_boundary(status, instant):
    value = chronology_export(status=status, source_as_of=instant, citation_times=[instant])
    assert origin_violations(value) == set()
    bundle = build(value)
    assert verify_bundle(bundle, expected_owner_sub=OWNER) == bundle
    assert bundle["source_export"]["proposals"][0]["source_as_of"] == instant
    assert bundle["accepted_event_count"] == int(status == "accept")


@pytest.mark.parametrize("kind,code", [
    ("source", "proposal_after_first_decision"),
    ("citation", "proposal_citation_after_first_decision"),
])
def test_even_one_microsecond_after_first_decision_is_future(kind, code):
    future = "2020-01-19T21:00:00.000001-03:00"
    value = chronology_export(source_as_of=future if kind == "source" else None,
                               citation_times=[] if kind == "source" else [future])
    assert origin_violations(value) == {code}
    with pytest.raises(OwnerEvidenceError) as caught:
        build(value)
    assert caught.value.code == code


@pytest.mark.parametrize("status", ["accept", "reject", "not_a_default"])
@pytest.mark.parametrize("source_as_of,citation_times", [
    (None, [None]), (None, ["2020-01-15T00:00:00Z"]), ("2020-01-16T00:00:00Z", [None]), (None, []),
], ids=["all-unknown", "source-unknown", "citation-unknown", "no-proposal-citations"])
def test_unknown_legacy_dates_are_not_invented_or_an_adjudication_gate(status, source_as_of, citation_times):
    value = chronology_export(status=status, source_as_of=source_as_of, citation_times=citation_times)
    assert origin_violations(value) == set()
    bundle = build(value)
    assert verify_bundle(bundle, expected_owner_sub=OWNER) == bundle
    assert bundle["source_export"] == value  # No invented import/PIT/custody dates.
    assert bundle["accepted_event_count"] == int(status == "accept")


@pytest.mark.parametrize("source_as_of,citation_times", [
    ("2020-06-01T00:00:00Z", []), (None, ["2020-06-01T00:00:00Z"]),
    ("2020-06-01T00:00:00Z", ["2020-06-01T00:00:00Z"]), (None, [None]),
], ids=["known-source", "known-citation", "both-known", "undated"])
def test_unreviewed_proposal_has_no_invented_first_decision_gate(source_as_of, citation_times):
    value = chronology_export(source_as_of=source_as_of, citation_times=citation_times)
    value["decisions"], value["accepted_events"] = [], []
    assert origin_violations(value) == set()
    bundle = build(rehash(value))
    assert verify_bundle(bundle, expected_owner_sub=OWNER) == bundle
    assert bundle["accepted_event_count"] == 0 and bundle["proposal_count"] == 1


@pytest.mark.parametrize("kind,code", [
    ("source", "proposal_after_knowledge_cutoff"),
    ("citation", "proposal_citation_after_knowledge_cutoff"),
])
def test_unreviewed_proposal_still_obeys_bundle_knowledge_cutoff(kind, code):
    future = "2021-01-01T00:00:00Z"
    value = chronology_export(source_as_of=future if kind == "source" else None,
                               citation_times=[] if kind == "source" else [future])
    value["decisions"], value["accepted_events"] = [], []
    with pytest.raises(OwnerEvidenceError) as caught:
        build(rehash(value))
    assert caught.value.code == code


def test_review_origin_is_per_proposal_not_the_shared_episode_or_earliest_global_decision():
    value = export()
    second = proposal("later-origin")
    late = chronology_export(source_as_of="2020-06-01T00:00:00Z", citation_times=["2020-06-01T00:00:00Z"])
    second["source_as_of"], second["citations"] = late["proposals"][0]["source_as_of"], late["proposals"][0]["citations"]
    second["proposed_cusips"] = ["594918104"]
    d = decision(second, label="later-origin", cusip="594918104")
    d["recorded_at"] = "2020-07-01T00:00:00Z"
    value["proposals"].insert(0, second)
    value["decisions"].insert(0, d)
    value["accepted_events"].append(d)
    assert origin_violations(value) == set()
    bundle = build(rehash(value))
    assert verify_bundle(bundle, expected_owner_sub=OWNER) == bundle
    assert bundle["accepted_event_count"] == 2
    d["recorded_at"] = "2020-05-01T00:00:00Z"
    with pytest.raises(OwnerEvidenceError, match="^proposal_after_first_decision$"):
        build(rehash(value))


def test_existing_decision_citation_must_still_be_public_by_its_own_recording():
    value = export()
    d = value["decisions"][0]
    d["citations"] = [{**citation(), "citation_id": uid("future-decision-citation"),
                       "source_document_id": "synthetic-future-decision-document",
                       "public_at": "2020-01-21T00:00:00Z"}]
    rehash_decision(d)
    with pytest.raises(OwnerEvidenceError, match="^citation_after_recording$"):
        build(rehash(value))


@pytest.mark.parametrize("kind,code", [
    ("source", "proposal_after_first_decision"),
    ("citation", "proposal_citation_after_first_decision"),
])
def test_cli_proposal_origin_refusal_is_sanitized_and_writes_no_bundle(tmp_path, capsys, kind, code):
    value = chronology_export(source_as_of="2020-06-01T00:00:00Z" if kind == "source" else None,
                               citation_times=["2020-06-01T00:00:00Z"] if kind == "citation" else [])
    value["proposals"][0]["summary"] = "SECRETLIKE synthetic source text"
    source, output = tmp_path / "future-proposal.json", tmp_path / "refused-bundle.json"
    source.write_text(json.dumps(rehash(value)), encoding="utf-8")
    assert main(["--input", str(source), "--output", str(output), "--owner-sub", OWNER,
                 "--code-revision", "synthetic", "--knowledge-cutoff", CUTOFF]) == 4
    assert json.loads(capsys.readouterr().out) == {"state": "refused", "code": code}
    assert not output.exists()


@pytest.mark.parametrize("kind,code", [
    ("decision", "market_proxy_after_recording"),
    ("unreviewed-proposal", "market_proxy_after_knowledge_cutoff"),
])
def test_cli_future_market_proxy_refusal_is_sanitized_and_writes_no_bundle(tmp_path, capsys, kind, code):
    cutoff = "2020-01-31T23:59:59Z"
    if kind == "decision":
        value = market_export(recorded_at="2020-01-15T00:00:00Z")
        value["decisions"][0]["market_prices"][0]["source_reference"] = "SECRETLIKE synthetic future market source"
        rehash_decision(value["decisions"][0])
    else:
        value = export(zero=True)
        p = value["proposals"][0]
        p["source_as_of"] = None
        p["market_prices"] = [market_price(confirmed=False)]
        p["market_prices"][0]["source_reference"] = "SECRETLIKE synthetic future market source"
    rehash(value)
    assert_export_hashes(value)
    source, output = tmp_path / "future-market.json", tmp_path / "refused-market-bundle.json"
    source.write_text(json.dumps(value), encoding="utf-8")
    assert main(["--input", str(source), "--output", str(output), "--owner-sub", OWNER,
                 "--code-revision", "synthetic-market-time", "--knowledge-cutoff", cutoff]) == 4
    assert json.loads(capsys.readouterr().out) == {"state": "refused", "code": code}
    assert not output.exists()


def test_fresh_bundle_identity_pins_runtime_source_not_historical_producer_bytes():
    bundle = build()
    assert bundle["code_digest"] == producer_source_sha()
    assert bundle["publication_id"] == oracle_publication_id(publication_pins(bundle))
    historical_pins = {**publication_pins(bundle),
                       "code_digest": "baeca798ab24515becf1f8872455a11faac6d130b82f277b150aa12cc8c2a3d0"}
    assert bundle["code_digest"] != historical_pins["code_digest"]
    assert bundle["publication_id"] != oracle_publication_id(historical_pins)
    assert verify_bundle(bundle, expected_owner_sub=OWNER) == bundle


class FakeConnection:
    """Transactional double, not ACL evidence; the opt-in PG suite checks real roles."""

    def __init__(self):
        self.builds = {}
        self.events = {}
        self.receipts = {}
        self.statements = []

    @contextmanager
    def transaction(self):
        snapshot = copy.deepcopy((self.builds, self.events, self.receipts))
        try:
            yield
        except Exception:
            self.builds, self.events, self.receipts = snapshot
            raise

    @contextmanager
    def cursor(self):
        yield FakeCursor(self)


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.result = []

    def execute(self, sql, params):
        self.conn.statements.append(sql)
        self.result = []
        if "pg_advisory_xact_lock" in sql:
            return
        pub = params[0]
        if "INSERT INTO" in sql and "_builds\"" in sql:
            self.conn.builds.setdefault(pub, json.loads(params[-1]))
        elif "INSERT INTO" in sql and "_events\"" in sql:
            self.conn.events.setdefault((pub, params[1]), json.loads(params[-1]))
        elif "INSERT INTO" in sql and "_validations\"" in sql:
            self.conn.receipts.setdefault(pub, json.loads(params[-1]))
        elif "SELECT payload FROM" in sql:
            self.result = [(self.conn.builds[pub],)] if pub in self.conn.builds else []
        elif "SELECT event_id,payload" in sql:
            self.result = [(key[1], value) for key, value in self.conn.events.items() if key[0] == pub]
        elif "SELECT publication_id" in sql:
            self.result = [(pub,)] if pub in self.conn.receipts else []
        elif "SELECT receipt" in sql:
            self.result = [(self.conn.receipts[pub],)] if pub in self.conn.receipts else []
        else:
            raise AssertionError("unexpected/pointer SQL: " + sql)

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return self.result


def test_store_prepare_validate_replay_verifies_persisted_rows_never_elects_pointer():
    conn = FakeConnection()
    store = PostgresOwnerEvidenceStore(conn, expected_owner_sub=OWNER)
    bundle = build()
    store.prepare(bundle)
    assert conn.receipts == {}
    receipt = store.validate(bundle["publication_id"])
    assert receipt["state"] == "validated"
    assert store.validate(bundle["publication_id"]) == receipt
    store.prepare(bundle)
    assert len(conn.builds) == len(conn.events) == len(conn.receipts) == 1
    assert all("_point(" not in statement and "_pointer\"" not in statement for statement in conn.statements)


def test_store_empty_events_is_valid_and_not_assumed_positive_control():
    conn = FakeConnection()
    store = PostgresOwnerEvidenceStore(conn, expected_owner_sub=OWNER)
    bundle = build(export(zero=True))
    store.prepare(bundle)
    assert store.validate(bundle["publication_id"])["accepted_event_count"] == 0
    assert conn.events == {}


@pytest.mark.parametrize("tamper", ["missing", "changed", "extra"])
def test_store_count_or_digest_mismatch_never_yields_validated_receipt(tamper):
    conn = FakeConnection()
    store = PostgresOwnerEvidenceStore(conn, expected_owner_sub=OWNER)
    bundle = build()
    store.prepare(bundle)
    key = next(iter(conn.events))
    if tamper == "missing":
        conn.events.clear()
    elif tamper == "changed":
        conn.events[key]["cusip9"] = "594918104"
    else:
        conn.events[(key[0], uid("foreign-event"))] = copy.deepcopy(conn.events[key])
    with pytest.raises(OwnerEvidenceError, match="stored_event_count_or_digest_mismatch"):
        store.validate(bundle["publication_id"])
    assert conn.receipts == {}


def test_per_obligation_proposals_preserve_lineage_and_legacy_episode_only_exports():
    value = export()
    assert build(value)["events"][0]["source_obligation_id"] == "synthetic-obligation-first"
    del value["proposals"][0]["source_obligation_id"]
    assert build(rehash(value))["events"][0]["source_obligation_id"] is None
    value["proposals"][0]["source_obligation_id"] = "episode_scope"
    with pytest.raises(OwnerEvidenceError, match="reserved_obligation_sentinel"):
        build(rehash(value))


def test_proposal_missing_citations_stays_visible_but_decisions_require_citations():
    value = export(zero=True)
    value["proposals"][0]["citations"] = []
    assert build(rehash(value))["proposal_count"] == 1
    assert build(rehash(value))["accepted_event_count"] == 0
    value = export()
    value["decisions"][0]["citations"] = []
    rehash_decision(value["decisions"][0])
    with pytest.raises(OwnerEvidenceError, match="invalid_list"):
        build(rehash(value))


def test_expected_owner_is_required_for_bundle_replay_store_and_descriptive_recall():
    conn = FakeConnection()
    bundle = build()
    with pytest.raises(OwnerEvidenceError, match="export_owner_mismatch"):
        verify_bundle(bundle, expected_owner_sub="user_DIFFERENT")
    store = PostgresOwnerEvidenceStore(conn, expected_owner_sub="user_DIFFERENT")
    with pytest.raises(OwnerEvidenceError, match="export_owner_mismatch"):
        store.prepare(bundle)
    assert conn.statements == [] and conn.builds == {}


@pytest.mark.parametrize("field,bad", [("event_type", []), ("security_status", {}),
                                      ("sequence", True), ("recorded_at", "2020-01-20T00:00:00")])
def test_malformed_json_values_fail_loud_with_sanitized_refusal(field, bad):
    value = export()
    value["decisions"][0][field] = bad
    rehash_decision(value["decisions"][0])
    with pytest.raises(OwnerEvidenceError):
        build(rehash(value))


def test_repeated_distressed_market_prices_are_not_legal_events_without_owner_acceptance():
    value = export(zero=True)
    value["proposals"][0]["source_as_of"] = "2020-02-01T00:00:00Z"
    value["proposals"][0]["market_prices"] = [
        {"month": month, "price_per_100": 25.0, "source_publication_id": uid("market-" + month),
         "source_reference": "Synthetic confirmed market-D series", "owner_confirmed": False,
         "interpretation": "market_recovery_proxy_not_realized"}
        for month in ("2020-01-01", "2020-02-01")
    ]
    assert build(rehash(value))["events"] == []


def test_resolution_history_survives_supersession_without_projecting_into_a_current_event():
    value = export()
    value["resolutions"] = [resolution(value["decisions"][0])]
    correction = decision(status="reject", sequence=2,
                          previous=value["decisions"][0]["decision_id"], label="correction")
    correction["recorded_at"] = "2020-03-01T00:00:00Z"
    value["decisions"].append(correction)
    value["accepted_events"] = []
    bundle = build(rehash(value))
    assert bundle["events"] == [] and bundle["resolution_count"] == 1
    value["resolutions"][0]["recorded_at"] = "2020-03-02T00:00:00Z"
    with pytest.raises(OwnerEvidenceError, match="resolution_after_supersession"):
        build(rehash(value))


def test_sql_and_policy_are_separate_append_only_agency_free_products():
    sql = (ROOT / "schemas/bond_default_owner_evidence_v1.sql").read_text(encoding="utf-8")
    policy = json.loads((ROOT / "contracts/bonds/default_owner_event_policy_v1.json").read_text(encoding="utf-8"))
    assert policy["agency_rating_packages_required"] is False and policy["custody_split_filter"] is False
    assert policy["dual_review_required"] is False and policy["primary_expected_loss_numerator"] is False
    assert "bond_default_owner_events_v1_current" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql
    assert "stored_events IS DISTINCT FROM build.payload->'events'" in sql
    assert "elected IS DISTINCT FROM expected_current" in sql
    assert "owner evidence pointer CAS mismatch" in sql
    assert "agency_rocr_xbrl" not in sql and "REFERENCES sec_derived" not in sql


def test_sql_declares_narrow_lock_grant_protected_cas_and_bound_paths_not_acl_proof():
    # These are source regressions only; actual non-owner ACLs are tested on PG16.
    sql = (ROOT / "schemas/bond_default_owner_evidence_v1.sql").read_text(encoding="utf-8")
    assert "GRANT UPDATE(publication_id) ON bond_default_owner_evidence_v1_builds TO worker_writer" in sql
    assert "RETURNS void LANGUAGE plpgsql SECURITY DEFINER" in sql
    assert "SET search_path TO pg_catalog, %I, pg_temp" in sql
    assert "REVOKE ALL ON bond_default_owner_evidence_v1_builds" in sql
    assert "bond_default_owner_events_v1_current FROM worker_writer" in sql
    assert "GRANT SELECT ON bond_default_owner_evidence_v1_pointer TO worker_writer" in sql
    assert "GRANT SELECT,INSERT,UPDATE,DELETE ON bond_default_owner_evidence_v1_pointer" not in sql
    assert "v.bundle_sha256=b.bundle_sha256" in sql and "v.events_sha256=b.events_sha256" in sql


def test_store_custom_schema_quotes_all_persistence_queries_and_retains_row_locks():
    # This double proves query generation, not trigger search_path behavior or ACLs.
    conn = FakeConnection()
    store = PostgresOwnerEvidenceStore(conn, expected_owner_sub=OWNER, schema="private_owner_evidence")
    bundle = build()
    store.prepare(bundle)
    store.validate(bundle["publication_id"])
    persistence = [statement for statement in conn.statements if "pg_advisory_xact_lock" not in statement]
    assert all('"private_owner_evidence"."bond_default_owner_evidence_v1_' in statement for statement in persistence)
    assert any("FOR UPDATE" in statement for statement in persistence)
    assert all('"public".' not in statement for statement in persistence)
