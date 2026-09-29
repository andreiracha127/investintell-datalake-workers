"""In-memory publication lifecycle: prepare -> validate -> promote (plan section 3.3)."""

from __future__ import annotations

import datetime as dt
import importlib.util
import uuid
from pathlib import Path

import pytest

from src.bonds.default_events import contracts as c
from src.bonds.default_events import public_ratings as pr
from src.bonds.default_events import publication as p

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "bond_default_events"
UTC = dt.timezone.utc


def _load_synthetic():
    spec = importlib.util.spec_from_file_location("bond_default_synthetic", FIXTURES / "synthetic.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


syn = _load_synthetic()
K0 = syn.KNOWLEDGE_CUTOFF


@pytest.fixture
def store():
    return p.InMemoryPublicationStore()


@pytest.fixture(scope="module")
def qualified():
    return syn.build_bundle()


def _promoted(store, bundle, expected=None):
    p.prepare_bundle(store, bundle)
    p.validate_bundle(store, bundle.publication_id)
    return p.promote_bundle(store, bundle.publication_id, expected_pointer=expected)


def _code(bundle):
    with pytest.raises(c.ContractError) as info:
        p.check_bundle(bundle)
    return str(info.value).split(":", 1)[0]


# ---------------------------------------------------------------------------
# Happy path, replay and collisions
# ---------------------------------------------------------------------------
def test_full_lifecycle_and_reads(store, qualified):
    assert p.prepare_bundle(store, qualified) == "inserted"
    state = store.state(qualified.publication_id)
    assert state.lifecycle_state == "prepared" and state.publication_version == 1
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, qualified.publication_id, expected_pointer=None)
    with pytest.raises(p.PublicationError, match="no_current_publication"):
        store.current()
    assert p.validate_bundle(store, qualified.publication_id).lifecycle_state == "validated"
    assert p.validate_bundle(store, qualified.publication_id).lifecycle_state == "validated"  # idempotent
    assert p.promote_bundle(store, qualified.publication_id, expected_pointer=None) == qualified.publication_id
    manifest = store.current(expected_target_month=syn.TARGET_MONTH,
                             expected_panel_publication_id=qualified.manifest["panel_publication_id"])
    assert manifest["publication_id"] == qualified.publication_id
    with pytest.raises(p.PublicationError, match="target_month_mismatch"):
        store.current(expected_target_month=dt.date(2026, 7, 1))
    with pytest.raises(p.PublicationError, match="panel_mismatch"):
        store.current(expected_panel_publication_id=uuid.uuid4())
    assert store.read(qualified.publication_id).canonical_bytes() == qualified.canonical_bytes()
    # Promoting the current publication again with the right expectation is a no-op.
    assert p.promote_bundle(store, qualified.publication_id, expected_pointer=qualified.publication_id)


def test_exact_replay_is_a_noop_and_collision_fails(store, qualified):
    assert p.prepare_bundle(store, qualified) == "inserted"
    ledger_before = {k: dict(v) for k, v in store.ledger.items()}
    assert p.prepare_bundle(store, qualified) == "replayed"
    assert store.ledger == ledger_before and len(store.publications) == 1
    altered = syn.build_bundle(alter_output=True)
    assert altered.publication_id == qualified.publication_id
    with pytest.raises(p.PublicationCollision, match="publication_collision"):
        p.prepare_bundle(store, altered)
    assert store.publications[qualified.publication_id].bundle is not altered


def test_ledger_row_collision_is_atomic(store, qualified):
    p.prepare_bundle(store, qualified)
    edgar = next(o for o in qualified.frames["observations"] if o.observation_kind == "edgar_passage")
    changed = syn.replace_row(edgar, document_quote="SYNTHETIC: different bytes, same identity")
    assert changed.observation_id == edgar.observation_id
    frames = {"observations": tuple(changed if o is edgar else o for o in qualified.frames["observations"])}
    other = syn.reassemble(qualified, frames=frames)
    assert other.publication_id != qualified.publication_id
    ledger_before = {k: dict(v) for k, v in store.ledger.items()}
    with pytest.raises(p.PublicationCollision, match="ledger_row_collision"):
        p.prepare_bundle(store, other)
    assert store.ledger == ledger_before and other.publication_id not in store.publications


def test_consumed_packages_are_sealed(store, qualified):
    p.prepare_bundle(store, qualified)
    disputed = next(a for a in qualified.frames["adjudications"] if a.status == "disputed")
    extra = c.Adjudication.create(**{
        **{n: getattr(disputed, n) for n, _ in c.Adjudication.SPEC if n != "adjudication_id"},
        "rationale": "SYNTHETIC late addition to a consumed package",
    })
    grown = syn.reassemble(qualified, frames={"adjudications": (*qualified.frames["adjudications"], extra)})
    with pytest.raises(p.PublicationCollision, match="package_inventory_collision"):
        p.prepare_bundle(store, grown)
    assert extra.adjudication_id not in store.ledger["adjudications"]
    # A fresh package carrying the new record is fine.
    batch = syn.new_package("adjudication_batch", "SYNTHETIC-ADJ-9")
    moved = c.Adjudication.create(**{
        **{n: getattr(extra, n) for n, _ in c.Adjudication.SPEC if n != "adjudication_id"},
        "package_id": batch.package_id,
    })
    ok = syn.reassemble(qualified, frames={
        "source_packages": (*qualified.frames["source_packages"], batch),
        "adjudications": (*qualified.frames["adjudications"], moved),
    })
    assert p.prepare_bundle(store, ok) == "inserted"


def test_prepare_rejects_underived_or_stale_identities(store, qualified):
    values = dict(qualified.manifest.values)
    values["events_digest"] = "sha256:" + "3" * 64
    tampered = c.CreditBundle(c.BundleManifest(values), qualified.panel_grid, qualified.frames)
    with pytest.raises(p.PublicationError, match="invalid_bundle.*derived_fields_mismatch"):
        p.prepare_bundle(store, tampered)
    other_policy = c.assemble_bundle(**_assemble_args(qualified), policy_digest="sha256:" + "4" * 64)
    with pytest.raises(p.PublicationError, match="policy_digest_not_current"):
        p.prepare_bundle(store, other_policy)
    assert not store.publications


def _assemble_args(bundle):
    m = bundle.manifest
    fr = bundle.frames
    return {
        "target_month": m["target_month"], "knowledge_cutoff": m["knowledge_cutoff"],
        "knowledge_mode": m["knowledge_mode"], "build_scope": m["build_scope"],
        "quality_state": m["quality_state"], "code_digest": m["code_digest"],
        "panel_publication_id": m["panel_publication_id"], "panel_grid": bundle.panel_grid,
        "issuer_mapping_digest": m["issuer_mapping_digest"],
        "rating_declarations": m["rating_declarations"], "rating_input_digest": m["rating_input_digest"],
        "validation_receipt": m.receipt(), "source_packages": fr["source_packages"],
        "observations": fr["observations"], "event_links": fr["event_links"],
        "adjudications": fr["adjudications"], "events": fr["events"], "followups": fr["followups"],
        "exit_evidence": fr["exit_evidence"], "coverage": fr["coverage"], "ratings": fr["ratings"],
        "ncen_filings": fr["ncen_filings"], "family_contexts": fr["family_contexts"],
        "family_evidence": fr["family_evidence"], "proposal_evidence": fr["proposal_evidence"],
        "exchange_relations": fr["exchange_relations"],
    }


# ---------------------------------------------------------------------------
# Semantic validation rules (check_bundle)
# ---------------------------------------------------------------------------
def test_fixture_bundles_pass_semantic_validation(qualified):
    p.check_bundle(qualified)
    p.check_bundle(syn.build_bundle(quality_state="partial", with_receipt=False))
    p.check_bundle(syn.build_bundle(with_events=False))


def _event(bundle, primary):
    return next(e for e in bundle.frames["events"] if e.primary_type == primary)


def _swap(bundle, frame, old, new):
    return syn.reassemble(bundle, frames={frame: tuple(new if r is old else r for r in bundle.frames[frame])})


def test_evidence_known_at_must_be_the_max_of_public_and_link_time(qualified):
    event = _event(qualified, "payment_default")
    early = syn.replace_row(event, evidence_known_at=event.evidence_known_at + dt.timedelta(hours=1))
    assert _code(_swap(qualified, "events", event, early)) == "event_evidence_invalid"


def test_knowledge_cutoff_limits_admission(qualified):
    # Historical reconstruction: no ingestion cutoff, but evidence must be known by K.
    historical = syn.reassemble(qualified, knowledge_mode="historical_reconstruction",
                                knowledge_cutoff=dt.datetime(2026, 9, 21, 9, 30, tzinfo=UTC))
    assert _code(historical) == "event_evidence_invalid"
    # Current run: adjudication after K is refused before any event rule.
    current = syn.reassemble(qualified, knowledge_cutoff=dt.datetime(2026, 9, 21, 11, 0, tzinfo=UTC))
    assert _code(current) == "current_run_input_after_cutoff"
    receipt_late = syn.reassemble(qualified, knowledge_cutoff=dt.datetime(2026, 9, 24, 11, 0, tzinfo=UTC))
    assert _code(receipt_late) == "validation_receipt_after_cutoff"


def test_quarantined_link_cannot_support_an_event(qualified):
    event = _event(qualified, "default_state")
    quarantined = next(x for x in qualified.frames["event_links"] if x.status == "quarantined")
    links = c.sorted_uuids([*event.link_ids, quarantined.link_id])
    evidence = c.sorted_uuids([*event.evidence_observation_ids, quarantined.observation_id])
    bad = syn._derive_event(qualified, event, link_ids=links, evidence_observation_ids=evidence)
    assert _code(_swap(qualified, "events", event, bad)) == "event_evidence_invalid"


def test_every_effective_admission_has_an_event(qualified):
    state = _event(qualified, "default_state")
    frames = {"events": tuple(e for e in qualified.frames["events"] if e is not state)}
    assert _code(syn.reassemble(qualified, frames=frames)) == "admitted_adjudication_without_event"


def test_superseded_candidate_cannot_admit_and_forks_are_rejected(qualified):
    event = _event(qualified, "payment_default")
    candidate = next(a for a in qualified.frames["adjudications"]
                     if a.subject_id == event.episode_id and a.status == "candidate")
    fork = c.Adjudication.create(**{
        **{n: getattr(candidate, n) for n, _ in c.Adjudication.SPEC if n != "adjudication_id"},
        "rationale": "SYNTHETIC second unsuperseded record",
    })
    frames = {"adjudications": (*qualified.frames["adjudications"], fork)}
    assert _code(syn.reassemble(qualified, frames=frames)) == "adjudication_chain_fork"
    # An event citing only the superseded candidate has no effective admission.
    only_candidate = syn._derive_event(qualified, event, adjudication_ids=(candidate.adjudication_id,))
    assert _code(_swap(qualified, "events", event, only_candidate)) == "event_evidence_invalid"


def test_episodes_of_one_security_must_be_disjoint(qualified):
    event = _event(qualified, "payment_default")
    accepted = next(a for a in qualified.frames["adjudications"]
                    if a.subject_id == event.episode_id and a.status == "accepted_event")
    second_episode = c.uuid5_of("synthetic_episode", "A-2")
    second_adj = c.Adjudication.create(**{
        **{n: getattr(accepted, n) for n, _ in c.Adjudication.SPEC if n != "adjudication_id"},
        "subject_id": second_episode, "supersedes_adjudication_id": None,
    })
    ids = (second_adj.adjudication_id,)
    # Same dated onset as the first episode (same provenance), never resolved: overlapping.
    second = syn._derive_event(qualified, event, (second_adj,), episode_id=second_episode, adjudication_ids=ids)
    frames = {
        "adjudications": (*qualified.frames["adjudications"], second_adj),
        "events": (*qualified.frames["events"], second),
    }
    assert _code(syn.reassemble(qualified, frames=frames)) == "overlapping_episodes"


def test_full_grid_and_rating_rows(qualified):
    ratings = qualified.frames["ratings"]
    assert _code(syn.reassemble(qualified, frames={"ratings": ratings[1:]})) == "rating_grid_mismatch"
    missing = next(r for r in ratings if r.state == "missing" and r.default_overlay_episode_id is None)
    ghost = syn.replace_row(missing, default_overlay_episode_id=uuid.uuid4())
    assert _code(_swap(qualified, "ratings", missing, ghost)) == "rating_row_invalid"
    rated = next(r for r in ratings if r.state == "observed")
    edgar = next(o for o in qualified.frames["observations"] if o.observation_kind == "edgar_passage")
    not_agency = syn.replace_row(rated, agency_source_ids=(edgar.observation_id,))
    assert _code(_swap(qualified, "ratings", rated, not_agency)) == "rating_row_invalid"


def test_rating_rows_require_the_governed_declared_scope(qualified):
    package, action = syn._agency_inputs(qualified)
    out_of_scope = syn.replace_row(
        action,
        agency_rating_type="short_term",
        agency_scale="national",
    )
    ratings = tuple(
        syn.replace_row(
            row,
            action_input_digest=c.rating_action_input_digest(
                row.view_kind,
                [out_of_scope],
                [package],
                [],
            ),
        )
        if action.observation_id in row.agency_source_ids
        else row
        for row in qualified.frames["ratings"]
    )
    bundle = syn.reassemble(
        qualified,
        frames={
            "observations": tuple(
                out_of_scope if row is action else row
                for row in qualified.frames["observations"]
            ),
            "ratings": ratings,
        },
    )
    assert _code(bundle) == "rating_scope_invalid"


def test_rating_declarations_are_persisted_and_bound_to_input_and_grid(qualified):
    declarations = pr.rating_declarations_record(syn.RATING_SCOPES, ())
    assert qualified.manifest.to_record()["rating_declarations"] == declarations
    assert qualified.manifest["rating_declarations_digest"] == c.digest_of(declarations)
    assert qualified.manifest["rating_input_digest"] == c.rating_input_manifest_digest(
        declarations, qualified.frames["source_packages"])

    with_uncleared = pr.rating_declarations_record(
        syn.RATING_SCOPES,
        syn.UNCLEARED_RATING_SOURCES,
    )
    stale_input_digest = syn.reassemble(
        qualified,
        rating_declarations=with_uncleared,
        rating_input_digest=qualified.manifest["rating_input_digest"],
    )
    assert _code(stale_input_digest) == "rating_input_digest_mismatch"

    wrong_grid_state = syn.reassemble(qualified, rating_declarations=with_uncleared)
    assert _code(wrong_grid_state) == "rating_scope_invalid"

    multi_links, reason = RATING_REGRESSIONS["multiple_binding_links_latest_known"]
    assert reason is None
    row = next(
        r for r in multi_links.frames["ratings"]
        if r.cusip_id == syn.CUSIP_A and r.month == syn.GRID_MONTHS[0] and r.view_kind == "effective_audit"
    )
    assert len(row.binding_link_ids) == 2
    assert row.public_known_at == dt.datetime(2026, 9, 21, 9, 30, tzinfo=dt.timezone.utc)
    p.check_bundle(multi_links)


def test_nport_points_alone_are_not_continuous_followup(qualified):
    follow = next(f for f in qualified.frames["followups"] if f.status == "nondefault_continuous")
    nport_ids = tuple(oid for oid in follow.evidence_observation_ids
                      if next(o for o in qualified.frames["observations"] if o.observation_id == oid)
                      .observation_kind == "nport_holding")
    points_only = syn.replace_row(follow, evidence_observation_ids=nport_ids)
    assert _code(_swap(qualified, "followups", follow, points_only)) == "followup_invalid"


def test_coverage_cells_bind_the_publication_receipt(qualified):
    cell = next(x for x in qualified.frames["coverage"] if x.state == "qualified")
    other = syn.replace_row(cell, validation_receipt_digest="sha256:" + "5" * 64)
    assert _code(_swap(qualified, "coverage", cell, other)) == "coverage_receipt_mismatch"


def test_qualified_requires_positive_receipt_and_qualified_evidence(qualified):
    cell = next(x for x in qualified.frames["coverage"] if x.state == "qualified")
    partial_cell = syn.replace_row(cell, state="partial", validation_receipt_digest=None)
    no_receipt = syn.reassemble(
        qualified, frames={"coverage": tuple(partial_cell if x is cell else x for x in qualified.frames["coverage"])},
        validation_receipt=None,
    )
    assert _code(no_receipt) == "qualified_state_unsupported"
    receipt = qualified.manifest.receipt()
    weak = c.ValidationReceipt(**{**{n: getattr(receipt, n) for n, _ in receipt.SPEC}, "verdict": "partial"})
    weak_cell = syn.replace_row(cell, validation_receipt_digest=weak.digest())
    weak_bundle = syn.reassemble(
        qualified, frames={"coverage": tuple(weak_cell if x is cell else x for x in qualified.frames["coverage"])},
        validation_receipt=weak,
    )
    assert _code(weak_bundle) == "qualified_state_unsupported"
    unverified = tuple(
        syn.replace_row(r, state="pit_unverified", bucket=None, action_date=None, public_known_at=None,
                        agency_source_ids=(), action_input_digest=None) if r.state == "observed" else r
        for r in qualified.frames["ratings"]
    )
    assert _code(syn.reassemble(qualified, frames={"ratings": unverified})) == "qualified_state_unsupported"
    limited = syn.reassemble(qualified, build_scope="limited")
    assert _code(limited) == "qualified_state_unsupported"


# ---------------------------------------------------------------------------
# W0 amendment 1 (bundle v2): persisted dependency closure (plan section 9 items 1-3, 7)
# ---------------------------------------------------------------------------
V2_REGRESSIONS = syn.v2_regressions()


@pytest.mark.parametrize("name", sorted(V2_REGRESSIONS))
def test_v2_dependency_closure(store, name):
    bundle, expected = V2_REGRESSIONS[name]
    if expected is None:
        p.check_bundle(bundle)
        assert _promoted(store, bundle) == bundle.publication_id
    else:
        assert _code(bundle) == expected


def test_group_c_dependency_closure_is_transitive_and_deduplicated():
    bundle = syn._group_c_scan_bundle(syn.build_bundle())
    ix = p._Index.build(
        bundle.frames,
        k=bundle.manifest["knowledge_cutoff"],
        mode=bundle.manifest["knowledge_mode"],
        policy_digest=bundle.manifest["policy_digest"],
    )
    event = next(ev for ev in bundle.frames["events"] if ev.episode_id == syn.EPISODE_B)
    proposal = next(iter(bundle.frames["proposal_evidence"]))
    scan_context = next(ctx for ctx in bundle.frames["family_contexts"] if ctx.report_date == syn.SCAN_DATE)
    rows = p.event_dependency_rows(ix, **p._event_edges(event))

    # The proposal is reached directly by the event and again through its accepting decision.
    assert [row.proposal_evidence_id for row in rows["proposal_evidence"].values()] == [
        proposal.proposal_evidence_id
    ]
    assert scan_context.context_id in {row.context_id for row in rows["family_contexts"].values()}
    assert p.event_evidence_known_at(ix, event) == syn.GROUP_C_LATE_AT


def test_group_c_relation_builder_uses_the_supplied_draft():
    bundle = syn.build_bundle()
    exchange = syn._exchange_inputs(bundle)
    new_rows = () if exchange["new_doc"] is None else tuple(exchange["new_doc"])
    frames = syn.frames_with(bundle, (
        exchange["doc_pkg"], exchange["doc"], *new_rows, exchange["old_link"],
        exchange["new_link"], exchange["pair"],
    ))
    relation = syn._relation(bundle, exchange, frames)

    assert not frames["exchange_relations"]
    assert p.exchange_relation_known_at(
        frames,
        relation,
        knowledge_cutoff=bundle.manifest["knowledge_cutoff"],
        knowledge_mode=bundle.manifest["knowledge_mode"],
    ) == relation.evidence_known_at


GROUP_D_REGRESSIONS = syn.group_d_regressions()


@pytest.mark.parametrize("name", sorted(GROUP_D_REGRESSIONS))
def test_group_d_admission_and_cure_authority(store, name):
    bundle, expected = GROUP_D_REGRESSIONS[name]
    if expected is None:
        p.check_bundle(bundle)
        assert _promoted(store, bundle) == bundle.publication_id
        return
    assert _code(bundle) == expected
    p.prepare_bundle(store, bundle)
    with pytest.raises(p.PublicationError) as info:
        p.validate_bundle(store, bundle.publication_id)
    assert info.value.code == f"bond_credit_validate:{expected}"
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)


def test_historical_ncen_selection_refuses_future_or_unattested_acceptance():
    bundle, expected = V2_REGRESSIONS["historical_filing_accepted_after_cutoff"]
    assert expected == "ncen_provenance_invalid"
    ix = p._Index.build(
        bundle.frames,
        k=bundle.manifest["knowledge_cutoff"],
        mode=bundle.manifest["knowledge_mode"],
        policy_digest=bundle.manifest["policy_digest"],
    )
    selected, _, reason = p.ncen_selection(ix, "9999999911", syn.REPORT_DATE)
    assert selected is not None
    assert reason == "filing_acceptance_unattested"
    assert _code(bundle) == "ncen_provenance_invalid"


ROW_REFUSALS = syn.row_refusals()


@pytest.mark.parametrize("name", sorted(ROW_REFUSALS))
def test_v2_row_contract_refusals(name):
    build, reason = ROW_REFUSALS[name]
    with pytest.raises(c.ContractError) as info:
        build()
    assert str(info.value).startswith(reason)


def test_review_time_never_moves_public_evidence_time():
    """§9 item 4: moving only the corroboration review time neither backdates nor inflates the
    proposal's public knowledge time; review time only gates current-run admission."""
    q = syn.build_bundle()
    early = syn.corroborated_b(q)
    late_rec = V2_REGRESSIONS["corroboration_review_after_cutoff_reconstruction"][0]
    t_early = next(iter(early.frames["proposal_evidence"])).evidence_known_at
    t_late = next(iter(late_rec.frames["proposal_evidence"])).evidence_known_at
    assert t_early == t_late == dt.datetime(2026, 9, 24, 14, 0, tzinfo=dt.timezone.utc)
    cor = next(a for a in late_rec.frames["adjudications"] if a.subject_kind == "corroboration")
    assert cor.adjudicated_at > late_rec.manifest["knowledge_cutoff"] > t_late


def test_stale_as_of_is_mode_aware():
    """§9 item 9: a revision public by K but ingested after K counts in historical
    reconstruction and not in current_run (links carry no ingestion time)."""
    q = syn.build_bundle()
    edgar_a = syn._pieces(q)["edgar_a"]
    k = q.manifest["knowledge_cutoff"]
    revision = syn._variant(syn._obs_revision(edgar_a, "item-2.04-p3-corr", "correction"),
                            first_seen_at=k + dt.timedelta(hours=6))
    assert revision.public_available_at <= k < revision.first_seen_at
    obs = {o.observation_id: o for o in (edgar_a, revision)}
    stale_rec, _ = p._stale_as_of(obs, {}, k, current_run=False)
    stale_cur, _ = p._stale_as_of(obs, {}, k, current_run=True)
    assert edgar_a.observation_id in stale_rec
    assert edgar_a.observation_id not in stale_cur
    # A revision not public by K counts in neither mode.
    accept = "20260925050000"  # America/New_York -> 2026-09-25T09:00Z, after K
    accepted = c.edgar_acceptance_to_utc(accept)
    unpublished = syn._variant(revision, acceptance_raw=accept, acceptance_at=accepted, public_available_at=accepted,
                               first_seen_at=accepted + dt.timedelta(hours=1))
    assert unpublished.public_available_at > k
    later = {o.observation_id: o for o in (edgar_a, unpublished)}
    assert edgar_a.observation_id not in p._stale_as_of(later, {}, k, current_run=False)[0]


def test_exchange_relation_changes_publication_identity():
    base = syn.exchange_bundle()
    rel = next(iter(base.frames["exchange_relations"]))
    ended = syn.exchange_bundle(new_valid_to=dt.date(2027, 6, 30))
    assert next(iter(ended.frames["exchange_relations"])).relation_id == rel.relation_id  # same logical id
    assert ended.manifest["exchange_relations_digest"] != base.manifest["exchange_relations_digest"]
    assert ended.publication_id != base.publication_id


def test_bridge_outside_the_event_changes_context_identity():
    q = syn.build_bundle()
    bridged = syn.v2_regressions()["bridge_stale_components"][0]
    assert bridged.manifest["ncen_filing_inventory_digest"] != q.manifest["ncen_filing_inventory_digest"]
    assert bridged.publication_id != q.publication_id


def test_v1_payloads_are_rejected_even_without_schema_check():
    legacy = c.load_json_strict((FIXTURES / syn.LEGACY_V1_FIXTURE).read_bytes())
    assert legacy["contract_version"] == c.LEGACY_V1_CONTRACT_VERSION
    for schema_check in (True, False):
        with pytest.raises(c.ContractError, match="unsupported_contract_version"):
            c.CreditBundle.from_json_obj(legacy, schema_check=schema_check)
    # A v1 body relabelled as v2 still fails: its frame set is not the v2 frame set.
    relabelled = {**legacy, "contract_version": c.CONTRACT_VERSION,
                  "manifest": {**legacy["manifest"], "contract_version": c.CONTRACT_VERSION}}
    with pytest.raises(c.ContractError, match="frame_set_mismatch"):
        c.CreditBundle.from_json_obj(relabelled, schema_check=False)


def test_v2_payloads_are_rejected_by_the_retained_v1_schema(qualified):
    legacy_schema = c.load_legacy_v1_schema()
    assert legacy_schema["x-digest"] == c.LEGACY_V1_SCHEMA_DIGEST
    with pytest.raises(c.ContractError, match="schema_violation"):
        c.validate_against_schema(qualified.to_json_obj(), schema=legacy_schema)


@pytest.mark.parametrize("frame", ["ncen_filings", "family_contexts", "family_evidence", "proposal_evidence",
                                   "exchange_relations"])
def test_missing_mandatory_empty_frame_is_rejected(qualified, frame):
    payload = qualified.to_json_obj()
    del payload["frames"][frame]
    for schema_check in (True, False):
        with pytest.raises(c.ContractError, match="frame_set_mismatch"):
            c.CreditBundle.from_json_obj(payload, schema_check=schema_check)


def test_v2_fixture_round_trips_and_validates():
    ex = syn.exchange_bundle()
    decoded = c.CreditBundle.from_json_obj(ex.to_json_obj())
    assert decoded.canonical_bytes() == ex.canonical_bytes()
    p.check_bundle(decoded)


RATING_REGRESSIONS = syn.rating_regressions()


@pytest.mark.parametrize("name", sorted(RATING_REGRESSIONS))
def test_rating_input_qualification_and_public_pit_binding(store, name):
    """Qualified needs bound, approved rating input; rows bind the relied action (findings 1/4)."""
    bundle, reason = RATING_REGRESSIONS[name]
    if reason is None:
        p.check_bundle(bundle)
        assert _promoted(store, bundle) == bundle.publication_id
        return
    assert _code(bundle) == reason
    p.prepare_bundle(store, bundle)
    with pytest.raises(p.PublicationError) as info:
        p.validate_bundle(store, bundle.publication_id)
    assert info.value.code == f"bond_credit_validate:{reason}"
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)


CLOSURE_REGRESSIONS = syn.evidence_closure_regressions()


@pytest.mark.parametrize("name", sorted(CLOSURE_REGRESSIONS))
def test_evidence_closure_and_onset_lower_provenance(store, name):
    """Events close over their accepting adjudication; rows keep honest known-times (findings 5/7)."""
    bundle, reason = CLOSURE_REGRESSIONS[name]
    if reason is None:
        p.check_bundle(bundle)
        assert _promoted(store, bundle) == bundle.publication_id
        return
    assert _code(bundle) == reason
    p.prepare_bundle(store, bundle)
    with pytest.raises(p.PublicationError) as info:
        p.validate_bundle(store, bundle.publication_id)
    assert info.value.code == f"bond_credit_validate:{reason}"
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)


REVISION_REGRESSIONS = syn.revision_regressions()


@pytest.mark.parametrize("name", sorted(REVISION_REGRESSIONS))
def test_revision_chains_resolved_as_of_cutoff(store, name):
    """Outputs never rely on a link/observation superseded or retracted by K (finding 6)."""
    bundle, reason = REVISION_REGRESSIONS[name]
    if reason is None:
        p.check_bundle(bundle)
        assert _promoted(store, bundle) == bundle.publication_id
        return
    assert _code(bundle) == reason
    p.prepare_bundle(store, bundle)
    with pytest.raises(p.PublicationError) as info:
        p.validate_bundle(store, bundle.publication_id)
    assert info.value.code == f"bond_credit_validate:{reason}"
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)


def test_revision_cycle_is_refused(store):
    bundle = syn.observation_cycle_bundle()
    assert _code(bundle) == "supersession_cycle"
    p.prepare_bundle(store, bundle)
    with pytest.raises(p.PublicationError, match="bond_credit_validate:supersession_cycle"):
        p.validate_bundle(store, bundle.publication_id)


def test_invented_lower_bound_without_provenance_is_refused(qualified):
    """Reviewer's case: a null first-Y lower bound moved to the day before its upper bound."""
    state = next(e for e in qualified.frames["events"] if e.primary_type == "default_state")
    lower = state.onset_upper_inclusive - dt.timedelta(days=1)
    with pytest.raises(c.ContractError, match="onset_lower_evidence_ids:required_iff_lower_bound"):
        syn.replace_row(state, onset_lower_exclusive=lower, timing_class=c.derive_timing_class(
            lower, state.onset_upper_inclusive))


def test_rating_receipt_fields_are_paired_and_need_a_receipt(qualified):
    receipt = qualified.manifest.receipt()
    values = {n: getattr(receipt, n) for n, _ in receipt.SPEC}
    with pytest.raises(c.ContractError, match="rating_digests_both_or_none"):
        c.ValidationReceipt(**{**values, "rating_package_digest": None})
    record = dict(qualified.manifest.values)
    record.update({f: None for f in (*c.VALIDATION_FIELDS, "validation_digest")})
    with pytest.raises(c.ContractError, match="rating_qualification_without_receipt"):
        c.BundleManifest(record)
    assert c.rating_package_digest(
        [x for x in qualified.frames["source_packages"] if x.source_family != "agency_rocr_xbrl"]) is None


# ---------------------------------------------------------------------------
# Promotion state machine
# ---------------------------------------------------------------------------
def test_empty_qualified_bundle_needs_only_a_positive_receipt(store):
    empty = syn.build_bundle(with_events=False)
    assert not empty.frames["events"]
    assert _promoted(store, empty) == empty.publication_id


def test_partial_and_unavailable_builds_are_shadow_only(store, qualified):
    _promoted(store, qualified)
    partial = syn.build_bundle(quality_state="partial", with_receipt=False,
                               knowledge_cutoff=K0 + dt.timedelta(days=1))
    unavailable = syn.build_bundle(quality_state="unavailable", with_receipt=False,
                                   knowledge_cutoff=K0 + dt.timedelta(days=2))
    for shadow in (partial, unavailable):
        p.prepare_bundle(store, shadow)
        assert p.validate_bundle(store, shadow.publication_id).lifecycle_state == "validated"
        with pytest.raises(p.PublicationError, match="not_qualified_complete"):
            p.promote_bundle(store, shadow.publication_id, expected_pointer=qualified.publication_id)
        with pytest.raises(p.PublicationError, match="shadow_build_requires_allow_shadow"):
            store.read(shadow.publication_id)
        assert store.read(shadow.publication_id, allow_shadow=True).publication_id == shadow.publication_id
    assert store.pointer == qualified.publication_id


def test_compare_and_set_including_absent_pointer(store, qualified):
    p.prepare_bundle(store, qualified)
    p.validate_bundle(store, qualified.publication_id)
    with pytest.raises(p.PublicationError, match="cas_mismatch"):
        p.promote_bundle(store, qualified.publication_id, expected_pointer=uuid.uuid4())
    assert store.pointer is None
    p.promote_bundle(store, qualified.publication_id, expected_pointer=None)
    newer = syn.build_bundle(knowledge_cutoff=K0 + dt.timedelta(days=1))
    p.prepare_bundle(store, newer)
    p.validate_bundle(store, newer.publication_id)
    with pytest.raises(p.PublicationError, match="cas_mismatch"):
        p.promote_bundle(store, newer.publication_id, expected_pointer=None)
    assert p.promote_bundle(store, newer.publication_id, expected_pointer=qualified.publication_id)


def test_target_month_and_cutoff_never_regress(store, qualified):
    _promoted(store, qualified)
    older_k = syn.build_bundle(knowledge_cutoff=dt.datetime(2026, 9, 24, 18, 0, tzinfo=UTC))
    older_t = syn.build_bundle(target_month=dt.date(2026, 5, 1), knowledge_cutoff=K0 + dt.timedelta(days=1))
    same_tk = syn.reassemble(qualified, code_digest="sha256:" + "6" * 64)
    for bundle, code in ((older_k, "tk_regression"), (older_t, "tk_regression"), (same_tk, "tk_not_advanced")):
        p.prepare_bundle(store, bundle)
        p.validate_bundle(store, bundle.publication_id)
        with pytest.raises(p.PublicationError, match=code):
            p.promote_bundle(store, bundle.publication_id, expected_pointer=qualified.publication_id)
    assert store.pointer == qualified.publication_id
    later_t = syn.build_bundle(target_month=dt.date(2026, 7, 1), knowledge_cutoff=K0 + dt.timedelta(days=2))
    assert _promoted(store, later_t, qualified.publication_id) == later_t.publication_id


def test_revocation_makes_readers_refuse(store, qualified):
    _promoted(store, qualified)
    store.revoke(qualified.publication_id, "SYNTHETIC corrected evidence", "sha256:" + "7" * 64)
    with pytest.raises(p.PublicationError, match="current_publication:revoked"):
        store.current()
    with pytest.raises(p.PublicationError, match="read_publication:revoked"):
        store.read(qualified.publication_id, allow_shadow=True)
    with pytest.raises(p.PublicationError, match="already_revoked"):
        store.revoke(qualified.publication_id, "again", "sha256:" + "7" * 64)
    with pytest.raises(p.PublicationError, match="promote:revoked"):
        p.promote_bundle(store, qualified.publication_id, expected_pointer=qualified.publication_id)
    # A replacement at the same T/K may replace a revoked current publication.
    replacement = syn.reassemble(qualified, code_digest="sha256:" + "8" * 64)
    assert _promoted(store, replacement, qualified.publication_id) == replacement.publication_id
    assert store.current()["publication_id"] == replacement.publication_id


def test_revoke_requires_validated_publication(store, qualified):
    p.prepare_bundle(store, qualified)
    with pytest.raises(p.PublicationError, match="unknown_or_unvalidated"):
        store.revoke(qualified.publication_id, "reason", "sha256:" + "7" * 64)


def test_validation_failure_leaves_build_prepared(store, qualified):
    event = _event(qualified, "payment_default")
    bad = _swap(qualified, "events", event,
                syn.replace_row(event, evidence_known_at=event.evidence_known_at + dt.timedelta(hours=1)))
    p.prepare_bundle(store, bad)
    with pytest.raises(p.PublicationError, match="bond_credit_validate:event_evidence_invalid"):
        p.validate_bundle(store, bad.publication_id)
    assert store.state(bad.publication_id).lifecycle_state == "prepared"
    with pytest.raises(p.PublicationError, match="not_validated"):
        p.promote_bundle(store, bad.publication_id, expected_pointer=None)


# ---------------------------------------------------------------------------
# Immutability of bundles and of the in-memory store's copy
# ---------------------------------------------------------------------------
def _assert_frozen(bundle):
    events = bundle.frames["events"]
    with pytest.raises(TypeError):
        bundle.frames["events"] = ()  # type: ignore[index]
    with pytest.raises(TypeError):
        del bundle.frames["events"]  # type: ignore[attr-defined]
    with pytest.raises(TypeError):
        bundle.manifest.values["events_count"] = 0  # type: ignore[index]
    with pytest.raises(AttributeError):
        events.append(events[0])  # type: ignore[attr-defined]
    package = bundle.frames["source_packages"][0]
    with pytest.raises(TypeError):
        package.member_sha256s["forged"] = "0" * 64
    observation = next(o for o in bundle.frames["observations"] if o.field_presence)
    with pytest.raises(TypeError):
        observation.field_presence["nport_is_default"] = "absent"
    with pytest.raises(AttributeError):
        package.content_sha256 = "0" * 64  # frozen dataclass
    assert bundle.frames["events"] == events


def test_bundles_and_nested_values_are_deeply_frozen():
    bundle = syn.build_bundle()
    _assert_frozen(bundle)
    bundle.verify_frames_against_manifest()


def test_caller_mappings_are_copied_at_construction():
    bundle = syn.build_bundle()
    frames = dict(bundle.frames)
    copy = c.CreditBundle(bundle.manifest, bundle.panel_grid, frames)
    frames["events"] = ()
    assert copy.frames["events"] == bundle.frames["events"] and copy.frames is not frames
    values = dict(bundle.manifest.values)
    manifest = c.BundleManifest(values)
    values["events_count"] = 0
    assert manifest["events_count"] == bundle.manifest["events_count"]
    package = bundle.frames["source_packages"][0]
    members = dict(package.member_sha256s)
    rebuilt = syn.replace_row(package, member_sha256s=members)
    members["forged"] = "0" * 64
    assert dict(rebuilt.member_sha256s) == dict(package.member_sha256s)


def _tamper(bundle, frame, rows):
    """Bypass the read-only view the way a hostile caller would (not a supported API)."""
    object.__setattr__(bundle, "frames", {**bundle.frames, frame: rows})


def test_mutation_after_prepare_validate_and_read_cannot_reach_the_store(store):
    bundle = syn.build_bundle()
    events = bundle.frames["events"]
    assert len(events) == bundle.manifest["events_count"] > 0
    p.prepare_bundle(store, bundle)
    _assert_frozen(bundle)
    _tamper(bundle, "events", ())  # after prepare
    assert p.validate_bundle(store, bundle.publication_id).lifecycle_state == "validated"
    _tamper(bundle, "coverage", ())  # after validate
    assert p.promote_bundle(store, bundle.publication_id, expected_pointer=None) == bundle.publication_id
    read = store.read(bundle.publication_id)
    assert read is not bundle and read.frames["events"] == events
    read.verify_frames_against_manifest()
    _assert_frozen(read)


@pytest.mark.parametrize(("frame", "reason"), [
    ("events", "events:count_does_not_match_manifest"),
    ("coverage", "coverage:count_does_not_match_manifest"),
    ("observations", "observations:inventory_does_not_match_manifest"),
])
def test_promotion_reverifies_frames_against_the_manifest(store, frame, reason):
    bundle = syn.build_bundle()
    p.prepare_bundle(store, bundle)
    p.validate_bundle(store, bundle.publication_id)
    stored = store.publications[bundle.publication_id].bundle
    _tamper(stored, frame, stored.frames[frame][1:])
    with pytest.raises(c.ContractError, match=reason):
        stored.verify_frames_against_manifest()
    with pytest.raises(p.PublicationError, match="bond_credit_promote:frame_mismatch") as info:
        p.promote_bundle(store, bundle.publication_id, expected_pointer=None)
    assert reason in str(info.value)
    assert store.pointer is None
