"""Immutable bond-credit publication lifecycle: prepare -> validate -> promote.

State machine (implementation plan section 3.3):

* ``prepare_bundle`` verifies that every derived manifest field (inventories, frame
  digests, fingerprint, UUIDv8 publication id) is derived from the frames, appends the
  input ledger rows (idempotent; same id with different bytes is a collision) and
  inserts the publication as ``prepared`` with all child rows, in one transaction.
  An exact replay re-reads the persisted rows, verifies their hashes and is a no-op;
  a replay with different bytes fails.
* ``validate_bundle`` re-reads the persisted build, re-verifies every row hash and the
  full derivation, applies the semantic rules of :func:`check_bundle` and (in the
  database) ``bond_credit_validate``, then moves the build to ``validated`` in the
  same transaction. A validated partial/unavailable build is a shadow artifact.
* ``promote_bundle`` compare-and-sets the singleton pointer (absent pointer included)
  under a product advisory lock, only for a validated, unrevoked, complete and
  qualified build with non-regressing T and K.

Two stores implement the same contract: :class:`InMemoryPublicationStore` (tests) and
:class:`PostgresPublicationStore` (immutable DB writer over the four additive SQL
files). Error codes are shared: ``<function>:<reason>``.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Protocol

from . import contracts as c
from . import public_ratings as pr
from .contracts import (
    ADMITTING_STATUSES,
    FRAME_TYPES,
    MANIFEST_SPEC,
    OBSERVATION_FAMILIES,
    Adjudication,
    BundleManifest,
    ContractError,
    CreditBundle,
    CreditObservation,
    EventLink,
    SourcePackage,
    ValidationReceipt,
)


class PublicationError(RuntimeError):
    """A lifecycle rule refused the operation; ``code`` is ``<function>:<reason>``."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}:{detail}" if detail else code)
        self.code = code
        self.detail = detail


class PublicationCollision(PublicationError):
    """Same identity, different bytes."""


def _require(condition: bool, code: str, detail: object = "") -> None:
    if not condition:
        raise ContractError(f"{code}:{detail}" if detail != "" else code)


# ---------------------------------------------------------------------------
# Pure verification (shared by both stores)
# ---------------------------------------------------------------------------
def verify_derivation(bundle: CreditBundle) -> None:
    """Every derived manifest/lineage field must be recomputable from the frames."""
    m = bundle.manifest
    _require(m["policy_digest"] == c.POLICY_DIGEST, "policy_digest_not_current")
    _require(m["contract_digest"] == c.SCHEMA_DIGEST, "contract_digest_not_current")
    _require(m["sql_digest"] == c.sql_digest(), "sql_digest_not_current")
    fr = bundle.frames
    rebuilt = c.assemble_bundle(
        target_month=m["target_month"],
        knowledge_cutoff=m["knowledge_cutoff"],
        knowledge_mode=m["knowledge_mode"],
        build_scope=m["build_scope"],
        quality_state=m["quality_state"],
        code_digest=m["code_digest"],
        panel_publication_id=m["panel_publication_id"],
        panel_grid=bundle.panel_grid,
        issuer_mapping_digest=m["issuer_mapping_digest"],
        rating_declarations=m["rating_declarations"],
        rating_input_digest=m["rating_input_digest"],
        validation_receipt=m.receipt(),
        source_packages=fr["source_packages"],  # type: ignore[arg-type]
        observations=fr["observations"],  # type: ignore[arg-type]
        event_links=fr["event_links"],  # type: ignore[arg-type]
        adjudications=fr["adjudications"],  # type: ignore[arg-type]
        events=fr["events"],  # type: ignore[arg-type]
        followups=fr["followups"],  # type: ignore[arg-type]
        exit_evidence=fr["exit_evidence"],  # type: ignore[arg-type]
        coverage=fr["coverage"],  # type: ignore[arg-type]
        ratings=fr["ratings"],  # type: ignore[arg-type]
        ncen_filings=fr["ncen_filings"],  # type: ignore[arg-type]
        family_contexts=fr["family_contexts"],  # type: ignore[arg-type]
        family_evidence=fr["family_evidence"],  # type: ignore[arg-type]
        proposal_evidence=fr["proposal_evidence"],  # type: ignore[arg-type]
        exchange_relations=fr["exchange_relations"],  # type: ignore[arg-type]
        policy_digest=m["policy_digest"],
        contract_digest=m["contract_digest"],
        sql_digest_value=m["sql_digest"],
    )
    _require(rebuilt.canonical_bytes() == bundle.canonical_bytes(), "derived_fields_mismatch")


def check_bundle(bundle: CreditBundle) -> None:
    """Semantic validation (joins, temporal admission, episodes, grid, quality).

    Mirrors ``bond_credit_validate``; raises :class:`ContractError` with the same
    reason names on the first violated rule.
    """
    verify_derivation(bundle)
    m = bundle.manifest
    fr = bundle.frames
    k: dt.datetime = m["knowledge_cutoff"]
    current_run = m["knowledge_mode"] == "current_run"
    receipt = m.receipt()

    packages: dict[uuid.UUID, SourcePackage] = {p.package_id: p for p in fr["source_packages"]}  # type: ignore[attr-defined]
    obs: dict[uuid.UUID, CreditObservation] = {o.observation_id: o for o in fr["observations"]}  # type: ignore[attr-defined]
    links: dict[uuid.UUID, EventLink] = {x.link_id: x for x in fr["event_links"]}  # type: ignore[attr-defined]
    adjs: dict[uuid.UUID, Adjudication] = {a.adjudication_id: a for a in fr["adjudications"]}  # type: ignore[attr-defined]
    try:
        rating_scopes, uncleared_sources = pr.rating_declarations_from_record(m["rating_declarations"])
    except pr.RatingResolveError as exc:
        raise ContractError(f"rating_declarations_invalid:{exc}") from exc
    _require(
        m["rating_declarations_digest"]
        == pr.rating_declarations_digest(rating_scopes, uncleared_sources),
        "rating_declarations_invalid",
    )
    _require(
        m["rating_input_digest"]
        == pr.rating_input_manifest_digest(rating_scopes, uncleared_sources, packages.values()),
        "rating_input_digest_mismatch",
    )
    rating_scope_keys = {
        (scope.agency_name, scope.rating_type, scope.scale) for scope in rating_scopes
    }
    uncleared_cusips = {
        o.cusip9 for o in obs.values()
        if o.observation_kind == "agency_action" and o.cusip9 is not None
        and not c.is_approved_rating_package(packages[o.package_id])
    }
    for f in fr["ncen_filings"]:
        _require(
            packages[f.package_id].source_family in c.NCEN_FILING_FAMILIES,  # type: ignore[attr-defined]
            "ncen_provenance_invalid", f.filing_evidence_id,  # type: ignore[attr-defined]
        )

    # Inputs: family routing, supersession closure, current-run cutoff.
    for p in packages.values():
        _require(not (current_run and p.retrieved_at > k), "source_lineage_mismatch", p.package_id)
        _require(
            p.revision_of_package_id is None or p.revision_of_package_id in packages,
            "source_lineage_mismatch", p.package_id,
        )
    for o in obs.values():
        family = packages[o.package_id].source_family
        _require(family in OBSERVATION_FAMILIES[o.observation_kind], "observation_family_mismatch", o.observation_id)
        _require(not (current_run and o.first_seen_at > k), "current_run_input_after_cutoff", o.observation_id)
        _require(
            o.supersedes_observation_id is None or o.supersedes_observation_id in obs,
            "observation_supersedes_outside_inventory", o.observation_id,
        )
    for x in links.values():
        _require(packages[x.package_id].source_family == "link_batch", "link_family_mismatch", x.link_id)
        _require(x.observation_id in obs, "link_observation_outside_inventory", x.link_id)
        _require(
            x.supersedes_link_id is None or x.supersedes_link_id in links,
            "link_supersedes_outside_inventory", x.link_id,
        )
    for a in adjs.values():
        _require(
            packages[a.package_id].source_family == "adjudication_batch",
            "adjudication_family_mismatch", a.adjudication_id,
        )
        _require(not (current_run and a.adjudicated_at > k), "current_run_input_after_cutoff", a.adjudication_id)
        _require(
            set(a.evidence_observation_ids) <= set(obs) and set(a.link_ids) <= set(links),
            "adjudication_evidence_outside_inventory", a.adjudication_id,
        )
        if a.supersedes_adjudication_id is not None:
            parent = adjs.get(a.supersedes_adjudication_id)
            _require(
                parent is not None and parent.subject_id == a.subject_id and parent.subject_kind == a.subject_kind,
                "adjudication_supersedes_outside_inventory", a.adjudication_id,
            )
    _require(
        not (current_run and receipt is not None and receipt.issued_at > k),
        "validation_receipt_after_cutoff",
    )
    # Observation/link/filing revision chains resolved as of K (acyclic, no effective forks);
    # effective adjudication heads per (subject kind, subject).
    ix = _Index.build(fr, k=k, mode=m["knowledge_mode"], policy_digest=m["policy_digest"])
    stale_obs, stale_links = ix.stale_obs, ix.stale_links
    effective_ids = {a.adjudication_id for a in ix.heads.values()}
    _check_ncen_filings(ix)

    events = fr["events"]
    episodes_with_events = {e.episode_id for e in events}  # type: ignore[attr-defined]
    for (kind, subject), a in ix.heads.items():
        if a.status == "accepted_state" and a.reviewer_role == "policy_rule_engine":
            _require(bool(a.proposal_evidence_ids), "proposal_evidence_not_closed", a.adjudication_id)
        # Proposal evidence is publication-scoped: an effective decision's cited proposals must
        # be in this publication (superseded ledger rows may cite another publication's).
        _require(set(a.proposal_evidence_ids) <= set(ix.proposals), "proposal_evidence_not_closed", a.adjudication_id)
        if a.status in ADMITTING_STATUSES:
            _require(
                kind == "issue_episode" and a.policy_digest == m["policy_digest"] and subject in episodes_with_events,
                "admitted_adjudication_without_event", subject,
            )
        if kind == "issue_scope" and a.status == c.EVIDENCE_STATUS:
            scoped = links.get(subject)
            _require(scoped is not None and scoped.status == "quarantined", "adjudication_subject_invalid", subject)
        if kind == "exchange_pairing" and a.status == c.EVIDENCE_STATUS:
            _require(subject in ix.relations, "adjudication_subject_invalid", subject)

    # Persisted dependency closure: family contexts, proposals, relations.
    contexts_by_date = _check_family_contexts(ix)
    _check_proposals(ix, contexts_by_date)
    _check_relations(ix, {e.episode_id: e for e in events})  # type: ignore[attr-defined]
    _check_event_dependencies(ix, events)

    admitting_events: dict[uuid.UUID, Any] = {}
    for ev in events:
        key = ev.episode_id  # type: ignore[attr-defined]
        ev_obs = ev.evidence_observation_ids  # type: ignore[attr-defined]
        ev_links = ev.link_ids  # type: ignore[attr-defined]
        ev_adjs = ev.adjudication_ids  # type: ignore[attr-defined]
        ok = set(ev_obs) <= set(obs) and set(ev_links) <= set(links) and set(ev_adjs) <= set(adjs)
        _require(ok, "event_evidence_invalid", key)
        for lid in ev_links:
            x = links[lid]
            _require(
                (x.status == "admitted" or (x.status == "quarantined" and ix.scope_head(lid) is not None))
                and x.security_id == ev.security_id  # type: ignore[attr-defined]
                and x.cusip9 == ev.cusip9  # type: ignore[attr-defined]
                and x.observation_id in ev_obs
                and x.valid_from <= ev.onset_upper_inclusive  # type: ignore[attr-defined]
                and (x.valid_to is None or x.valid_to >= ev.onset_upper_inclusive),  # type: ignore[attr-defined]
                "event_evidence_invalid", key,
            )
        covered = {links[lid].observation_id for lid in ev_links}
        _require(set(ev_obs) <= covered, "event_evidence_invalid", key)
        accepting = []
        for aid in ev_adjs:
            a = adjs[aid]
            _require(
                a.subject_kind == "issue_episode" and a.subject_id == key and a.policy_digest == m["policy_digest"],
                "event_evidence_invalid", key,
            )
            if aid in effective_ids and a.status == ev.admission_status:  # type: ignore[attr-defined]
                accepting.append(a)
        _require(len(accepting) == 1, "event_evidence_invalid", key)
        accepted = accepting[0]
        _require(_event_admitting_head(ix, ev) is accepted, "event_evidence_invalid", key)
        admitting_events[accepted.adjudication_id] = ev
        _require(
            set(accepted.evidence_observation_ids) <= set(ev_obs) and set(accepted.link_ids) <= set(ev_links),
            "event_evidence_not_closed", key,
        )
        link_known = event_link_known_at(ix, ev)
        known = event_evidence_known_at(ix, ev)
        _require(ev.link_known_at == link_known, "event_evidence_invalid", key)  # type: ignore[attr-defined]
        _require(ev.evidence_known_at == known, "event_evidence_invalid", key)  # type: ignore[attr-defined]
        _require(known <= k, "event_evidence_invalid", key)
        _require(
            ev.dependency_digest == dependency_digest_of(event_dependency_rows(ix, **_event_edges(ev))),  # type: ignore[attr-defined]
            "dependency_digest_mismatch", key,
        )
        lower = ev.onset_lower_exclusive  # type: ignore[attr-defined]
        if lower is not None:
            _require(
                all(
                    oid in accepted.evidence_observation_ids and _dates_onset_lower(obs[oid], ev.cusip9, lower)  # type: ignore[attr-defined]
                    for oid in ev.onset_lower_evidence_ids  # type: ignore[attr-defined]
                ),
                "event_onset_lower_unsupported", key,
            )
        # A proposal-based state event takes both onset bounds from a relied proposal.
        state_proposals = [ix.proposals[p] for p in ev.proposal_evidence_ids]  # type: ignore[attr-defined]
        if state_proposals and ev.primary_type == "default_state":  # type: ignore[attr-defined]
            _require(any(p.onset_upper_inclusive == ev.onset_upper_inclusive for p in state_proposals),  # type: ignore[attr-defined]
                     "proposal_evidence_not_closed", key)
            _require(
                any(p.onset_upper_inclusive == ev.onset_upper_inclusive  # type: ignore[attr-defined]
                    and p.onset_lower_exclusive == lower
                    and set(p.onset_lower_evidence_ids) == set(ev.onset_lower_evidence_ids)  # type: ignore[attr-defined]
                    for p in state_proposals),
                "event_onset_lower_unsupported", key,
            )
        for rid in ev.resolution_refs:  # type: ignore[attr-defined]
            _require(rid in obs or rid in adjs, "resolution_ref_not_in_inventory", key)
        times = _support_times(ev.resolution_refs, ev.resolution_refs, ix)  # type: ignore[attr-defined]
        for rid in ev.resolution_refs:  # type: ignore[attr-defined]
            if rid in adjs:
                _require(rid == accepted.adjudication_id, "resolution_adjudication_invalid", key)
        if ev.resolution_refs:  # type: ignore[attr-defined]
            _require(
                bool(times) and ev.resolution_known_at == max(times) and max(times) <= k,  # type: ignore[attr-defined]
                "resolution_knowledge_invalid", key,
            )

    by_security: dict[uuid.UUID, list[Any]] = defaultdict(list)
    for ev in events:
        by_security[ev.security_id].append(ev)  # type: ignore[attr-defined]
    for rows in by_security.values():
        rows.sort(key=lambda e: (e.onset_upper_inclusive, str(e.episode_id)))
        for i, first in enumerate(rows):
            for second in rows[i + 1:]:
                _require(
                    first.resolution_date is not None
                    and second.onset_lower_exclusive is not None
                    and second.onset_lower_exclusive >= first.resolution_date,
                    "overlapping_episodes", second.episode_id,
                )

    grid_cusips = {cusip for cusip, _month in bundle.panel_grid}
    segments: dict[tuple[uuid.UUID, uuid.UUID], list[Any]] = defaultdict(list)
    for f in fr["followups"]:
        seg = f.segment_id  # type: ignore[attr-defined]
        _require(f.known_at <= k, "followup_invalid", seg)  # type: ignore[attr-defined]
        _require(set(f.evidence_observation_ids) <= set(obs), "followup_invalid", seg)  # type: ignore[attr-defined]
        _require(set(f.adjudication_ids) <= set(adjs), "followup_invalid", seg)  # type: ignore[attr-defined]
        # Expand the typed graph before the authority check so missing nested dependencies keep
        # their established reason; compare the resulting public time only after authority.
        support = _support_times(f.evidence_observation_ids, f.adjudication_ids, ix)  # type: ignore[attr-defined]
        for aid in f.adjudication_ids:  # type: ignore[attr-defined]
            authority = admitting_events.get(aid)
            _require(
                authority is not None
                and authority.security_id == f.security_id  # type: ignore[attr-defined]
                and authority.cusip9 == f.cusip9,  # type: ignore[attr-defined]
                "followup_adjudication_invalid", seg,
            )
        # Every supporting observation/link (including those its adjudications cite) is public
        # by the segment's own known_at.
        _require(all(t <= f.known_at for t in support), "followup_invalid", seg)  # type: ignore[attr-defined]
        if f.status == "nondefault_continuous":  # type: ignore[attr-defined]
            documentary = any(
                obs[oid].observation_kind != "nport_holding" for oid in f.evidence_observation_ids  # type: ignore[attr-defined]
            )
            surveillance = (
                f.completeness_basis == "surveillance_receipt"  # type: ignore[attr-defined]
                and receipt is not None
                and receipt.issued_at <= f.known_at  # type: ignore[attr-defined]
                and receipt.surveils(
                    f.cusip9 in grid_cusips, f.interval_start_exclusive, f.interval_end_inclusive,  # type: ignore[attr-defined]
                )
            )
            _require(documentary or surveillance, "followup_invalid", seg)
        segments[(f.security_id, f.spell_id)].append(f)  # type: ignore[attr-defined]
    for rows in segments.values():
        for i, first in enumerate(rows):
            for second in rows[i + 1:]:
                _require(
                    not (
                        first.interval_start_exclusive < second.interval_end_inclusive
                        and second.interval_start_exclusive < first.interval_end_inclusive
                    ),
                    "followup_invalid", second.segment_id,
                )

    for x in fr["exit_evidence"]:
        _require(x.known_at <= k, "exit_evidence_invalid", x.security_id)  # type: ignore[attr-defined]
        _require(set(x.evidence_observation_ids) <= set(obs), "exit_evidence_invalid", x.security_id)  # type: ignore[attr-defined]
        _require(
            all(obs[oid].public_available_at <= x.known_at for oid in x.evidence_observation_ids),  # type: ignore[attr-defined]
            "exit_evidence_invalid", x.security_id,  # type: ignore[attr-defined]
        )

    for cell in fr["coverage"]:
        digest = cell.validation_receipt_digest  # type: ignore[attr-defined]
        _require(digest is None or digest == m["validation_digest"], "coverage_receipt_mismatch", cell.period_label)  # type: ignore[attr-defined]

    grid = set(bundle.panel_grid)
    ratings = fr["ratings"]
    for view in c.ENUMS["view_kind"]:
        keys = {(r.cusip_id, r.month) for r in ratings if r.view_kind == view}  # type: ignore[attr-defined]
        count = sum(1 for r in ratings if r.view_kind == view)  # type: ignore[attr-defined]
        _require(keys == grid and count == len(grid), "rating_grid_mismatch", view)
    event_cusips = {(e.episode_id, e.cusip9) for e in events}  # type: ignore[attr-defined]
    for r in ratings:
        label = f"{r.cusip_id}|{r.month.isoformat()}|{r.view_kind}"  # type: ignore[attr-defined]
        _require(r.public_known_at is None or r.public_known_at <= k, "rating_row_invalid", label)  # type: ignore[attr-defined]
        for oid in r.agency_source_ids:  # type: ignore[attr-defined]
            _require(oid in obs and obs[oid].observation_kind == "agency_action", "rating_row_invalid", label)
            action = obs[oid]
            _require(
                (action.agency_name, action.agency_rating_type, action.agency_scale) in rating_scope_keys,
                "rating_scope_invalid",
                label,
            )
        _require(all(lid in links for lid in r.binding_link_ids), "rating_row_invalid", label)  # type: ignore[attr-defined]
        if not r.agency_source_ids and r.state in {"missing", "rights_unverified"}:  # type: ignore[attr-defined]
            declared_unavailable = r.cusip_id in uncleared_cusips or any(  # type: ignore[attr-defined]
                source.covers(r.month) for source in uncleared_sources  # type: ignore[attr-defined]
            )
            has_approved_candidate = _rating_candidate_exists(ix, r, rating_scope_keys)
            expected_state = "rights_unverified" if declared_unavailable and not has_approved_candidate else "missing"
            _require(r.state == expected_state, "rating_scope_invalid", label)  # type: ignore[attr-defined]
        overlay = r.default_overlay_episode_id  # type: ignore[attr-defined]
        _require(overlay is None or (overlay, r.cusip_id) in event_cusips, "rating_row_invalid", label)  # type: ignore[attr-defined]
        _require(_check_rating_row(ix, r), "rating_row_invalid", label)

    # No output row relies on an observation/link superseded or retracted as of K.
    for label, relied_obs, relied_links in _relied_inputs(fr, adjs, ix):
        _require(not (relied_obs & stale_obs or relied_links & stale_links), "evidence_superseded", label)

    if m["quality_state"] == "qualified":
        coverage_states = {cell.state for cell in fr["coverage"]}  # type: ignore[attr-defined]
        pit_states = {r.state for r in ratings if r.view_kind == "public_pit"}  # type: ignore[attr-defined]
        _require(
            receipt is not None
            and receipt.verdict == "qualified"
            and m["build_scope"] == "complete"
            and m["issuer_mapping_digest"] is not None
            and "qualified" in coverage_states
            and not coverage_states & {"partial", "unavailable"}
            and not pit_states & {"pit_unverified", "rights_unverified"}
            and _rating_input_qualified(receipt, m["rating_input_digest"], packages.values(), grid),
            "qualified_state_unsupported",
        )


def _stale_as_of(
    obs: Mapping[uuid.UUID, CreditObservation],
    links: Mapping[uuid.UUID, EventLink],
    k: dt.datetime,
    *,
    current_run: bool = False,
) -> tuple[set[uuid.UUID], set[uuid.UUID]]:
    """Observations and links no longer usable as evidence at cutoff ``k``.

    A revision counts only when known by ``k`` (observation ``public_available_at``, link
    ``link_known_at``) and, in ``current_run``, also ingested by ``k`` (observation
    ``first_seen_at``); later revisions are ignored, so each snapshot keeps its own K
    semantics. A record superseded by a counted revision is stale, and a ``retraction``
    observation is never evidence. Chains must be acyclic (``supersession_cycle``) and
    no record may have two counted revisions (``revision_chain_fork``). Mirrors
    ``bond_credit_validate``.
    """
    _require_acyclic(obs, "supersedes_observation_id")
    _require_acyclic(links, "supersedes_link_id")
    revised_obs = _revised_by(
        ((o.supersedes_observation_id, o.public_available_at, o.first_seen_at) for o in obs.values()), k, current_run,
    )
    revised_links = _revised_by(((x.supersedes_link_id, x.link_known_at, None) for x in links.values()), k, current_run)
    retracted = {oid for oid, o in obs.items() if o.revision_kind == "retraction"}
    return revised_obs | retracted, revised_links


def _require_acyclic(rows: Mapping[uuid.UUID, Any], parent_attr: str) -> None:
    for start in sorted(rows, key=str):
        seen = {start}
        parent = getattr(rows[start], parent_attr)
        while parent is not None and parent in rows:
            _require(parent not in seen, "supersession_cycle", start)
            seen.add(parent)
            parent = getattr(rows[parent], parent_attr)


def _revised_by(
    revisions: Iterable[tuple[uuid.UUID | None, dt.datetime, dt.datetime | None]], k: dt.datetime, current_run: bool,
) -> set[uuid.UUID]:
    revised: set[uuid.UUID] = set()
    for parent, known, seen in revisions:
        if parent is None or known > k or (current_run and seen is not None and seen > k):
            continue
        _require(parent not in revised, "revision_chain_fork", parent)
        revised.add(parent)
    return revised


def _relied_inputs(
    fr: Mapping[str, tuple[Any, ...]], adjs: Mapping[uuid.UUID, Adjudication], ix: _Index,
) -> Iterable[tuple[object, set[uuid.UUID], set[uuid.UUID]]]:
    """``(label, observation ids, link ids)`` each output row relies on.

    Events: evidence, links, onset-lower evidence, resolution support (references plus
    what referenced adjudications cite) and the dependency closure except context voter
    universes (N-PORT votes of other registrants stay context, checked at FE-1a level);
    follow-ups: evidence plus what their adjudications cite; exits: evidence; ratings:
    relied agency actions and binding links; relations: documents and both side links.
    """

    for ev in fr["events"]:
        rows = event_dependency_rows(ix, **_event_edges(ev))
        resolution = _dependency_rows(
            ix,
            observation_ids=(rid for rid in ev.resolution_refs if rid in ix.obs),
            adjudication_ids=(rid for rid in ev.resolution_refs if rid in adjs),
        )
        yield ev.episode_id, {
            *(row.observation_id for row in rows.get("observations", {}).values()),
            *(row.observation_id for row in resolution.get("observations", {}).values()),
        }, {
            *(row.link_id for row in rows.get("event_links", {}).values()),
            *(row.link_id for row in resolution.get("event_links", {}).values()),
        }
    for f in fr["followups"]:
        rows = _dependency_rows(
            ix, observation_ids=f.evidence_observation_ids, adjudication_ids=f.adjudication_ids,
        )
        yield f.segment_id, {
            row.observation_id for row in rows.get("observations", {}).values()
        }, {
            row.link_id for row in rows.get("event_links", {}).values()
        }
    for x in fr["exit_evidence"]:
        yield x.security_id, set(x.evidence_observation_ids), set()
    for r in fr["ratings"]:
        yield f"{r.cusip_id}|{r.month.isoformat()}|{r.view_kind}", set(r.agency_source_ids), set(r.binding_link_ids)


def _support_times(
    observation_ids: Iterable[uuid.UUID],
    adjudication_ids: Iterable[uuid.UUID],
    ix: _Index,
) -> list[dt.datetime]:
    """Public times of the complete support closure rooted at observations/decisions."""
    rows = _dependency_rows(
        ix,
        observation_ids=(row_id for row_id in observation_ids if row_id in ix.obs),
        adjudication_ids=(row_id for row_id in adjudication_ids if row_id in ix.adjs),
    )
    known = _dependency_known_at(ix, rows)
    return [] if known is None else [known]


def _dates_onset_lower(o: CreditObservation, cusip9: str, lower: dt.date) -> bool:
    """``o`` is explicit provenance for an onset lower bound ``lower`` of ``cusip9``.

    A credible prior N-PORT ``N`` of the same obligation reported exactly at ``lower``, or
    independent dating evidence: a day-precision date one day after ``lower`` or an interval
    whose lower bound is ``lower``. Mirrors ``bond_credit_validate``.
    """
    if o.observation_kind == "nport_holding":
        return o.nport_is_default == "N" and o.report_date == lower and o.cusip9 == cusip9
    if o.date_precision == "day":
        return o.effective_date is not None and o.effective_date - dt.timedelta(days=1) == lower
    if o.date_precision == "interval":
        return o.effective_lower_exclusive == lower
    return False


def _rating_input_qualified(
    receipt: ValidationReceipt | None,
    rating_input_digest: str,
    packages: Iterable[SourcePackage],
    grid: set[tuple[str, dt.date]],
) -> bool:
    """Qualified rating input: approved agency packages, a receipt bound to them, full grid coverage.

    ``missing`` rows mean "no action under qualified input", so a grid with no approved
    rating input (or one the receipt does not bind, or months past the verified
    coverage frontier) can never be ``qualified``. Mirrors ``bond_credit_validate``.
    """
    rating_packages = [p for p in packages if c.is_approved_rating_package(p)]
    if (
        not rating_packages
        or receipt is None
        or receipt.rating_input_digest != rating_input_digest
        or receipt.rating_package_digest != c.rating_package_digest(rating_packages)
    ):
        return False
    for view in c.ENUMS["view_kind"]:
        spans = [c.rating_coverage(p, view) for p in rating_packages]
        for month in {month for _cusip, month in grid}:
            month_end = c.month_end(month)
            if not any(
                start is not None and frontier is not None and start <= month and frontier >= month_end
                for start, frontier in spans
            ):
                return False
    return True


# ---------------------------------------------------------------------------
# v2 dependency closure (W0 amendment 1, section 5). Every rule below is mirrored by
# ``bond_credit_validate`` with the same reason names; see the SQL for the relational form.
# ---------------------------------------------------------------------------
NCEN_PARSED_FAMILIES = frozenset({"sec_ncen_dera", "sec_ncen_public_xml"})
_NCEN_TZ = "America/New_York"


def fund_key(o: CreditObservation) -> str | None:
    """W1 vote grouping identity: EDGAR series ID, else ``cik:<registrant>``."""
    if o.series_id is not None:
        return o.series_id
    if o.registrant_cik is not None:
        return f"cik:{o.registrant_cik}"
    return None


@dataclass
class _Index:
    """Typed lookups over one bundle's frames (shared by validation and closure helpers)."""

    packages: dict[uuid.UUID, SourcePackage]
    obs: dict[uuid.UUID, CreditObservation]
    links: dict[uuid.UUID, EventLink]
    adjs: dict[uuid.UUID, Adjudication]
    filings: dict[uuid.UUID, c.NcenFilingEvidence]
    contexts: dict[uuid.UUID, c.FamilyContext]
    members: dict[uuid.UUID, c.FamilyMembership]
    proposals: dict[uuid.UUID, c.ProposalEvidence]
    relations: dict[uuid.UUID, c.ExchangeRelation]
    k: dt.datetime
    current_run: bool
    policy_digest: str
    mode: str
    strict: bool
    stale_obs: set[uuid.UUID]
    stale_links: set[uuid.UUID]
    stale_filings: set[uuid.UUID]
    heads: dict[tuple[str, uuid.UUID], Adjudication]

    @classmethod
    def build(cls, frames: Mapping[str, Iterable[Any]], *, k: dt.datetime, mode: str, policy_digest: str,
              strict: bool = True) -> _Index:
        def by(frame: str, attr: str) -> dict[uuid.UUID, Any]:
            return {getattr(row, attr): row for row in frames.get(frame, ())}

        current_run = mode == "current_run"
        obs = by("observations", "observation_id")
        links = by("event_links", "link_id")
        filings = by("ncen_filings", "filing_evidence_id")
        adjs = by("adjudications", "adjudication_id")
        stale_obs, stale_links = _stale_as_of(obs, links, k, current_run=current_run)
        packages = by("source_packages", "package_id")
        _require_acyclic(packages, "revision_of_package_id")
        _require_acyclic(filings, "supersedes_filing_evidence_id")
        revised = _revised_by(
            ((f.supersedes_filing_evidence_id, f.public_available_at, f.first_seen_at) for f in filings.values()),
            k, current_run,
        )
        stale_filings = revised | {fid for fid, f in filings.items() if f.parse_status == "retracted"}
        return cls(
            packages=packages, obs=obs, links=links, adjs=adjs, filings=filings,
            contexts=by("family_contexts", "context_id"), members=by("family_evidence", "family_evidence_id"),
            proposals=by("proposal_evidence", "proposal_evidence_id"), relations=by("exchange_relations", "relation_id"),
            k=k, current_run=current_run, policy_digest=policy_digest, mode=mode, strict=strict,
            stale_obs=stale_obs, stale_links=stale_links, stale_filings=stale_filings,
            heads=_effective_heads(adjs, strict=strict),
        )

    # -- adjudications ------------------------------------------------------
    def head(self, kind: str, subject: uuid.UUID) -> Adjudication | None:
        return self.heads.get((kind, subject))

    def accepted_evidence(self, a: Adjudication | None, kind: str) -> bool:
        """``a`` is the effective, human ``accepted_evidence`` head of its evidence subject."""
        return (
            a is not None and a.subject_kind == kind and self.heads.get((kind, a.subject_id)) is a
            and a.status == c.EVIDENCE_STATUS and a.reviewer_role == "human_reviewer"
            and a.policy_digest == self.policy_digest
        )

    def pairing_head(self, subject: uuid.UUID) -> Adjudication | None:
        """Effective exchange-pairing decision as known by K, ignoring later revisions."""
        eligible = {
            aid: a for aid, a in self.adjs.items()
            if a.subject_kind == "exchange_pairing" and a.subject_id == subject and a.adjudicated_at <= self.k
        }
        if not eligible:
            return None
        return _effective_heads(eligible).get(("exchange_pairing", subject))

    # -- links ----------------------------------------------------------------
    def scope_head(self, link_id: uuid.UUID) -> Adjudication | None:
        """Effective accepted ``issue_scope`` decision admitting a quarantined link revision."""
        head = self.head("issue_scope", link_id)
        return head if self.accepted_evidence(head, "issue_scope") else None

    def usable_link(self, link_id: uuid.UUID) -> bool:
        x = self.links.get(link_id)
        if x is None or link_id in self.stale_links:
            return False
        return x.status == "admitted" or (x.status == "quarantined" and self.scope_head(link_id) is not None)

    def link_time(self, link_id: uuid.UUID) -> dt.datetime:
        """Public link knowledge: the link, plus an admitting scope decision's cited support."""
        x = self.links[link_id]
        times = [x.link_known_at]
        if x.status == "quarantined":
            head = self.scope_head(link_id)
            if head is not None:
                times += [self.obs[o].public_available_at for o in head.evidence_observation_ids]
                times += [self.links[lid].link_known_at for lid in head.link_ids]
        return max(times)

    # -- N-CEN ----------------------------------------------------------------
    def data_public_at(self, f: c.NcenFilingEvidence) -> dt.datetime:
        """Public time of the relied data version: the row or an equal-projection proof."""
        return min([f.public_available_at, *(self.filings[v].public_available_at for v in f.version_evidence_filing_ids)])

    def is_vote(self, o: CreditObservation) -> bool:
        """FE-1a voter: a Y/N N-PORT lot usable at K (any CUSIP; no flag or event filter)."""
        return (o.observation_kind == "nport_holding" and o.report_date is not None
                and o.cusip9 is not None and o.registrant_cik is not None
                and o.observation_id not in self.stale_obs and o.public_available_at <= self.k
                and o.field_presence.get("nport_is_default") == "present" and o.nport_is_default in ("Y", "N"))

    def eligible_votes(self, report_date: dt.date) -> list[CreditObservation]:
        return sorted((o for o in self.obs.values() if o.report_date == report_date and self.is_vote(o)),
                      key=lambda o: str(o.observation_id))


def _effective_heads(
    adjs: Mapping[uuid.UUID, Adjudication], *, strict: bool = True,
) -> dict[tuple[str, uuid.UUID], Adjudication]:
    """Unique unsuperseded record per ``(subject_kind, subject_id)``; acyclic chains.

    Non-strict mode (builder helpers over partial frames) keeps the latest-adjudicated head
    of a fork instead of refusing; validation is always strict.
    """
    _require_acyclic(adjs, "supersedes_adjudication_id")
    superseded = {a.supersedes_adjudication_id for a in adjs.values() if a.supersedes_adjudication_id}
    heads: dict[tuple[str, uuid.UUID], Adjudication] = {}
    for a in sorted(adjs.values(), key=lambda row: (row.adjudicated_at, str(row.adjudication_id))):
        if a.adjudication_id in superseded:
            continue
        key = (a.subject_kind, a.subject_id)
        _require(not strict or key not in heads, "adjudication_chain_fork", a.subject_id)
        heads[key] = a
    return heads


def _check_ncen_filings(ix: _Index) -> None:
    """N-CEN ledger provenance: families, header binding, revisions and version proofs.

    A row is never public before its own artifact (``public_available_at`` >= the owning
    package's ``first_verified_public_at``; earlier availability only via equal-projection
    version proofs), and a row's acceptance equals the acceptance attested by its header
    package (``edgar_acceptance_datetime`` basis), so a copy cannot backdate family data.
    """
    for fid, f in sorted(ix.filings.items(), key=lambda item: str(item[0])):
        package = ix.packages[f.package_id]
        family = package.source_family
        ok = family == "sec_edgar_index" if f.parse_status == "index_only" else family in NCEN_PARSED_FAMILIES
        ok = ok and f.public_available_at >= package.first_verified_public_at
        ok = ok and (f.acceptance_at is None or ix.data_public_at(f) >= f.acceptance_at)
        header = ix.packages.get(f.header_package_id) if f.header_package_id is not None else None
        ok = ok and (f.header_package_id is None or (
            header is not None and header.source_family == c.NCEN_HEADER_FAMILY
            and header.accession_number == f.accession_number
            and (f.acceptance_at is None or (header.public_time_basis == "edgar_acceptance_datetime"
                                             and header.first_verified_public_at == f.acceptance_at))))
        parent = ix.filings.get(f.supersedes_filing_evidence_id) if f.supersedes_filing_evidence_id else None
        ok = ok and (f.supersedes_filing_evidence_id is None or (
            parent is not None and parent.accession_number == f.accession_number
            and parent.registrant_cik == f.registrant_cik))
        for vid in f.version_evidence_filing_ids:
            proof = ix.filings.get(vid)
            ok = ok and proof is not None and proof.accession_number == f.accession_number \
                and proof.projection_digest == f.projection_digest and proof.parse_status == "parsed" \
                and not proof.version_evidence_filing_ids
        _require(ok, "ncen_provenance_invalid", fid)
        _require(not (ix.current_run and f.first_seen_at > ix.k), "current_run_input_after_cutoff", fid)


def ncen_selection(
    ix: _Index, cik: str, report_date: dt.date,
) -> tuple[uuid.UUID | None, tuple[uuid.UUID, ...], str | None]:
    """FE-1 effective N-CEN of ``cik`` for ``report_date`` at K from the persisted ledger.

    Returns ``(selected filing id, blocking filing ids, reason)``; ``reason`` is ``None``
    exactly when the selected filing is usable evidence. Copies of one accession are grouped
    (representative: earliest data public time, then id) and must agree on their projection;
    competing accessions of the latest in-window period are ordered only by exact acceptance;
    a visible filing of unknown period made public after the latest period blocks fallback.
    """
    floor = c.months_before(report_date, c.NCEN_EFFECTIVE_WINDOW_MONTHS)
    visible = [f for f in ix.filings.values()
               if f.registrant_cik == cik and f.filing_evidence_id not in ix.stale_filings
               and ix.data_public_at(f) <= ix.k]
    groups: dict[str, list[c.NcenFilingEvidence]] = defaultdict(list)
    for f in visible:
        groups[f.accession_number].append(f)
    for rows in groups.values():
        rows.sort(key=lambda f: (ix.data_public_at(f), str(f.filing_evidence_id)))
    reps = {acc: rows[0] for acc, rows in groups.items()}
    in_window = [a for a, r in reps.items() if r.report_period_end is not None and floor <= r.report_period_end <= report_date]
    top_period = max((reps[a].report_period_end for a in in_window), default=None)  # type: ignore[type-var]
    top = sorted(a for a in in_window if reps[a].report_period_end == top_period)
    horizon = c.date_only_public_available_at(top_period or floor, _NCEN_TZ)
    blockers = sorted(a for a, r in reps.items() if r.report_period_end is None and ix.data_public_at(r) >= horizon)
    reason: str | None = None
    pick: str | None = None
    if len(top) == 1:
        pick = top[0]
    elif len(top) > 1:
        times = [reps[a].acceptance_at for a in top]
        if None in times or len(set(times)) != len(times):
            reason = "selection_order_unresolved"
        else:
            pick = max(top, key=lambda a: reps[a].acceptance_at)  # type: ignore[arg-type,return-value]
    if reason is None and blockers:
        first = min((f for a in blockers for f in groups[a]), key=lambda f: str(f.filing_evidence_id))
        reason = "filing_not_acquired" if first.parse_status == "index_only" else "filing_period_unknown"
    if reason is None and pick is None:
        older = any(r.report_period_end is not None and r.report_period_end < floor for r in reps.values())
        reason = "effective_filing_older_than_15_months" if older else "no_effective_filing"
    if reason is None and pick is not None:
        rep = reps[pick]
        package = ix.packages[rep.package_id]
        if len({(f.projection_digest, f.form_type, f.report_period_end, f.acceptance_at, f.parse_status)
                for f in groups[pick]}) > 1:
            reason = "accession_copies_conflict"
        elif rep.parse_status == "index_only":
            reason = "filing_not_acquired"
        elif rep.parse_status != "parsed":
            reason = "effective_filing_quarantined"
        elif rep.acceptance_at is None or rep.acceptance_at > ix.k:
            reason = "filing_acceptance_unattested"
        elif rep.form_type == "N-CEN/A" and not (
            package.source_family == "sec_ncen_public_xml"
            and package.schema_version in c.NCEN_AMENDMENT_COMPLETE_SCHEMAS
        ):
            reason = "amendment_semantics_unknown"
    selected = reps[pick].filing_evidence_id if pick is not None else None
    blocking = {f.filing_evidence_id for a in (*top, *blockers) for f in groups[a]}
    blocking.discard(selected)  # type: ignore[arg-type]
    return selected, c.sorted_uuids(blocking), reason


def ncen_profile_reasons(f: c.NcenFilingEvidence, voting_keys: Iterable[str]) -> tuple[str, ...]:
    """FE-1 completeness and FE-1b series coverage of one effective filing (empty = complete)."""
    reasons: set[str] = set()
    if f.family_answer is None:
        reasons.add("b5_unanswered")
    elif f.family_answer == "Y":
        if c.ncen_clean(f.family_name_raw) is None:
            reasons.add("b5_family_name_missing")
        elif c.ncen_is_sentinel(f.family_name_raw):
            reasons.add("b5_family_name_sentinel")
        elif c.ncen_family_name_key(f.family_name_raw) is None:
            reasons.add("b5_family_name_unparseable")
    funds: set[str | None] = set(f.reported_series_ids) | {a["series_id"] for a in f.adviser_records}
    if not funds:
        reasons.add("no_funds")
    for series in funds:
        if not any(a["series_id"] == series and a["role"] == "adviser" for a in f.adviser_records):
            reasons.add("fund_without_current_adviser")
    if any(not c.ncen_adviser_tokens(a) for a in f.adviser_records):
        reasons.add("adviser_without_identifier")
    if any(not c.ncen_underwriter_tokens(u) for u in f.underwriter_records):
        reasons.add("underwriter_without_identifier")
    for key in voting_keys:
        if c.NCEN_SERIES_PATTERN.fullmatch(key) is None:
            reasons.add("voting_without_series_id")
        elif key not in f.reported_series_ids:
            reasons.add("voting_series_absent_from_effective_ncen")
    return tuple(sorted(reasons))


def ncen_component_id(report_date: dt.date, k: dt.datetime, mode: str, members: Iterable[str]) -> str:
    """W3 ``component_id_for``: rule version, R, K, mode and sorted members."""
    payload = [c.FAMILY_RULE_VERSION, report_date.isoformat(), c.ts_text(k), mode, sorted(members)]
    return "ncenfam:" + c.digest_of(payload)[7:39]


def ncen_components(
    profiles: Mapping[str, c.NcenFilingEvidence], report_date: dt.date, k: dt.datetime, mode: str,
) -> dict[str, str]:
    """Union-find over shared adviser, underwriter and B.5 family-name tokens."""
    parent = {cik: cik for cik in profiles}

    def find(item: str) -> str:
        while parent[item] != item:
            item = parent[item]
        return item

    owners: dict[tuple[str, str], str] = {}
    for cik in sorted(profiles):
        f = profiles[cik]
        keys = {("adv", t) for a in f.adviser_records for t in c.ncen_adviser_tokens(a)}
        keys |= {("uw", t) for u in f.underwriter_records for t in c.ncen_underwriter_tokens(u)}
        name = c.ncen_family_key(f.family_answer, f.family_name_raw)
        if name is not None:
            keys.add(("name", name))
        for key in sorted(keys):
            first = owners.setdefault(key, cik)
            ra, rb = find(first), find(cik)
            if ra != rb:
                low, high = sorted((ra, rb))
                parent[high] = low
    groups: dict[str, list[str]] = defaultdict(list)
    for cik in profiles:
        groups[find(cik)].append(cik)
    return {cik: ncen_component_id(report_date, k, mode, members) for members in groups.values() for cik in members}


@dataclass(frozen=True)
class ContextExpectation:
    """Recomputed FE-1 context: expected members, digests and knowledge time."""

    vote_observation_ids: tuple[uuid.UUID, ...]
    members: Mapping[str, Mapping[str, Any]]
    selection_filing_ids: tuple[uuid.UUID, ...]
    index_package_ids: tuple[uuid.UUID, ...]
    universe_digest: str
    evidence_known_at: dt.datetime | None


def expected_context(ix: _Index, report_date: dt.date) -> ContextExpectation:
    """Rebuild the full FE-1a/b context of ``report_date`` at K from the persisted inventory.

    Fails closed (``family_universe_not_closed``) when one filing family
    ``(registrant, fund key, R)`` has several accessions among the usable votes: W0 does not
    port N-PORT amendment selection, so such a universe is not expressible unambiguously.
    """
    votes = ix.eligible_votes(report_date)
    accessions: dict[tuple[str | None, str | None], set[str | None]] = defaultdict(set)
    by_reg: dict[str, list[CreditObservation]] = defaultdict(list)
    for o in votes:
        accessions[(o.registrant_cik, fund_key(o))].add(o.accession_number)
        by_reg[o.registrant_cik].append(o)  # type: ignore[index]
    _require(all(len(v) == 1 for v in accessions.values()), "family_universe_not_closed", report_date)
    members: dict[str, dict[str, Any]] = {}
    profiles: dict[str, c.NcenFilingEvidence] = {}
    selection: set[uuid.UUID] = set()
    for cik in sorted(by_reg):
        keys = tuple(sorted({fund_key(o) for o in by_reg[cik]}))  # type: ignore[type-var]
        selected, blocking, reason = ncen_selection(ix, cik, report_date)
        if reason is None and selected is not None:
            reasons = ncen_profile_reasons(ix.filings[selected], keys)
        else:
            reasons = (reason or "no_effective_filing",)
        members[cik] = {
            "voting_series_ids": keys,
            "vote_observation_ids": c.sorted_uuids(o.observation_id for o in by_reg[cik]),
            "selected_filing_id": selected, "blocking_filing_ids": blocking,
            "state": "incomplete" if reasons else "complete", "reasons": reasons, "component_id": None,
        }
        if not reasons and selected is not None:
            profiles[cik] = ix.filings[selected]
        if selected is not None:
            selection.add(selected)
            selection.update(ix.filings[selected].version_evidence_filing_ids)
        selection.update(blocking)
    for cik, component in ncen_components(profiles, report_date, ix.k, ix.mode).items():
        members[cik]["component_id"] = component
    universe = sorted(by_reg)
    index_packages = {f.package_id for f in ix.filings.values()
                      if f.registrant_cik in by_reg and ix.packages[f.package_id].source_family == "sec_edgar_index"}
    times = [o.public_available_at for o in votes] + [ix.data_public_at(ix.filings[x]) for x in selection]
    times += [ix.packages[x].first_verified_public_at for x in index_packages]
    return ContextExpectation(
        vote_observation_ids=c.sorted_uuids(o.observation_id for o in votes),
        members=members,
        selection_filing_ids=c.sorted_uuids(selection),
        index_package_ids=c.sorted_uuids(index_packages),
        universe_digest=c.encoded_digest([
            [cik, list(members[cik]["voting_series_ids"]), [str(x) for x in members[cik]["vote_observation_ids"]]]
            for cik in universe
        ]),
        evidence_known_at=max(times, default=None),
    )


def _check_family_contexts(ix: _Index) -> dict[dt.date, c.FamilyContext]:
    by_context: dict[uuid.UUID, list[c.FamilyMembership]] = defaultdict(list)
    for m in ix.members.values():
        _require(m.context_id in ix.contexts, "family_universe_not_closed", m.family_evidence_id)
        by_context[m.context_id].append(m)
    by_date: dict[dt.date, c.FamilyContext] = {}
    for ctx in sorted(ix.contexts.values(), key=lambda row: row.report_date):
        label = ctx.context_id
        _require(ctx.report_date not in by_date and ctx.knowledge_cutoff == ix.k, "family_selection_invalid", label)
        by_date[ctx.report_date] = ctx
        want = expected_context(ix, ctx.report_date)
        rows = {m.registrant_cik: m for m in by_context[ctx.context_id]}
        _require(
            ctx.vote_observation_ids == want.vote_observation_ids and set(rows) == set(want.members)
            and ctx.universe_digest == want.universe_digest
            and ctx.membership_digest == c.frame_digest(m.row_sha256() for m in rows.values()),
            "family_universe_not_closed", label,
        )
        for cik, m in rows.items():
            exp = want.members[cik]
            _require(
                m.valid_from == ctx.report_date and m.voting_series_ids == exp["voting_series_ids"]
                and m.vote_observation_ids == exp["vote_observation_ids"],
                "family_universe_not_closed", m.family_evidence_id,
            )
            _require(
                m.selected_filing_id == exp["selected_filing_id"] and m.blocking_filing_ids == exp["blocking_filing_ids"]
                and m.state == exp["state"] and m.reasons == exp["reasons"],
                "family_selection_invalid", m.family_evidence_id,
            )
            _require(m.component_id == exp["component_id"], "family_component_mismatch", m.family_evidence_id)
        _require(
            ctx.selection_filing_ids == want.selection_filing_ids and ctx.index_package_ids == want.index_package_ids,
            "family_selection_invalid", label,
        )
        _require(
            ctx.evidence_known_at == want.evidence_known_at and ctx.evidence_known_at <= ix.k,
            "dependency_knowledge_mismatch", label,
        )
    return by_date


@dataclass(frozen=True)
class VoteState:
    status: str
    basis: str
    y_ids: tuple[uuid.UUID, ...]
    n_ids: tuple[uuid.UUID, ...]
    relied_registrants: tuple[str, ...]


def vote_state(
    votes: Iterable[CreditObservation], members: Mapping[str, c.FamilyMembership], *, corroborated: bool,
) -> VoteState:
    """W1 consensus of one ``(cusip9, R)`` from persisted votes and FE-1 memberships.

    >= 2 distinct series AND >= 2 evidenced family components; unknown family identity never
    supplies independent support; same-family Y support needs reviewed corroboration; any
    disputed vote or Y/N conflict is ``conflict``. ``relied_registrants`` are the registrants
    of the relied side's series votes (their memberships are the proposal's family evidence).
    """
    flags: dict[tuple[str, str], set[str]] = defaultdict(set)
    regs: dict[tuple[str, str], str] = {}
    obs = list(votes)
    for o in obs:
        key = (o.accession_number or "", fund_key(o) or "")
        flags[key].add(o.nport_is_default or "")
        regs[key] = o.registrant_cik or ""
    value = {k: "disputed" if {"Y", "N"} <= f else ("Y" if "Y" in f else "N") for k, f in flags.items()}
    y_ids = c.sorted_uuids(o.observation_id for o in obs if o.nport_is_default == "Y")
    n_ids = c.sorted_uuids(o.observation_id for o in obs if o.nport_is_default == "N")
    ys = [k for k, v in value.items() if v == "Y"]
    ns = [k for k, v in value.items() if v == "N"]
    if any(v == "disputed" for v in value.values()) or (ys and ns):
        basis = "contradictory_lots" if any(v == "disputed" for v in value.values()) else "material_y_n_conflict"
        return VoteState("conflict", basis, y_ids, n_ids, ())
    if not ys and not ns:
        return VoteState("no_informative_vote", "no_y_or_n_vote", y_ids, n_ids, ())
    side = "Y" if ys else "N"
    keys = ys if ys else ns
    series = [k for k in keys if c.NCEN_SERIES_PATTERN.fullmatch(k[1])]
    funds = {k[1] for k in series}
    mapped = [members[regs[k]].component_id if regs[k] in members and members[regs[k]].state == "complete" else None
              for k in series]
    evidenced = {m for m in mapped if m is not None}
    relied = tuple(sorted({regs[k] for k in series}))
    if len(funds) < 2:
        ok, basis = False, "series_id_missing" if len(series) != len(keys) else "single_series"
    elif len(evidenced) >= 2:
        ok, basis = True, "independent_families"
    elif None in mapped:
        ok, basis = False, "family_independence_unknown"
    elif side == "Y" and corroborated:
        ok, basis = True, "same_family_corroborated"
    else:
        ok, basis = False, "same_family_uncorroborated" if side == "Y" else "same_family"
    status = ("consensus_" if ok else "candidate_") + side.lower()
    return VoteState(status, basis, y_ids, n_ids, relied)


def _corroboration_ok(ix: _Index, aid: uuid.UUID, cusip9: str, report_date: dt.date | None) -> bool:
    """Reviewed same-obligation documentary or rights-approved agency support, applicable at R."""
    a = ix.adjs.get(aid)
    if a is None or not ix.accepted_evidence(a, "corroboration"):
        return False
    for lid in a.link_ids:
        x = ix.links.get(lid)
        if x is None or not ix.usable_link(lid) or x.cusip9 != cusip9 or x.affected_scope not in (
            "issue", "issuer_affected_obligation"
        ):
            return False
    for oid in a.evidence_observation_ids:
        o = ix.obs.get(oid)
        if o is None or oid in ix.stale_obs or o.observation_kind == "nport_holding":
            return False
        if o.observation_kind == "agency_action" and not c.is_approved_rating_package(ix.packages[o.package_id]):
            return False
        if not any(ix.links[lid].observation_id == oid for lid in a.link_ids):
            return False
    if report_date is None:
        return True
    return a.support_valid_from <= report_date and (a.support_valid_to is None or report_date < a.support_valid_to)  # type: ignore[operator]


def _cusip_votes(ix: _Index, report_date: dt.date, cusip9: str) -> list[CreditObservation]:
    return [o for o in ix.eligible_votes(report_date) if o.cusip9 == cusip9]


def _context_members(ix: _Index, ctx: c.FamilyContext) -> dict[str, c.FamilyMembership]:
    return {m.registrant_cik: m for m in ix.members.values() if m.context_id == ctx.context_id}


def _check_proposals(ix: _Index, by_date: Mapping[dt.date, c.FamilyContext]) -> None:
    for pid, p in sorted(ix.proposals.items(), key=lambda item: str(item[0])):
        _require(
            p.policy_digest == ix.policy_digest
            and all(o in ix.obs and o not in ix.stale_obs for o in p.evidence_observation_ids)
            and all(f in ix.members for f in p.family_evidence_ids),
            "proposal_evidence_not_closed", pid,
        )
        upper = p.onset_upper_inclusive
        for aid in p.corroboration_adjudication_ids:
            _require(_corroboration_ok(ix, aid, p.cusip9, upper), "corroboration_not_effective", pid)
        cors = [ix.adjs[a] for a in p.corroboration_adjudication_ids]
        cor_obs = {o for a in cors for o in a.evidence_observation_ids}
        if p.proposed_status == "accepted_state":
            ctx_u = by_date.get(upper)  # type: ignore[arg-type]
            lower = p.onset_lower_exclusive
            ctx_l = by_date.get(lower) if lower is not None else None
            _require(ctx_u is not None and (lower is None or ctx_l is not None), "proposal_evidence_not_closed", pid)
            members_u = _context_members(ix, ctx_u)  # type: ignore[arg-type]
            st_u = vote_state(_cusip_votes(ix, upper, p.cusip9), members_u, corroborated=bool(cors))  # type: ignore[arg-type]
            family = {members_u[r].family_evidence_id for r in st_u.relied_registrants}
            ok = (st_u.status == "consensus_y" and st_u.basis == p.basis
                  and bool(cors) == (p.basis == "same_family_corroborated")
                  and p.onset_upper_evidence_ids == st_u.y_ids)
            lower_ids: tuple[uuid.UUID, ...] = ()
            if ctx_l is not None:
                members_l = _context_members(ix, ctx_l)
                st_l = vote_state(_cusip_votes(ix, lower, p.cusip9), members_l, corroborated=False)  # type: ignore[arg-type]
                ok = ok and st_l.status == "consensus_n" and p.onset_lower_evidence_ids == st_l.n_ids
                lower_ids = st_l.n_ids
                family |= {members_l[r].family_evidence_id for r in st_l.relied_registrants}
            # First credible Y / last credible N: every earlier report date on which this CUSIP
            # has a Y vote (or an N vote after the lower bound) needs a persisted context that
            # shows no earlier consensus Y (or later consensus N).
            for day in sorted({o.report_date for o in ix.obs.values()  # type: ignore[type-var]
                               if ix.is_vote(o) and o.cusip9 == p.cusip9
                               and o.report_date < upper  # type: ignore[operator]
                               and (o.nport_is_default == "Y" or lower is None or o.report_date > lower)}):  # type: ignore[operator]
                ctx = by_date.get(day)  # type: ignore[arg-type]
                if ctx is None:
                    ok = False
                    break
                applies = any(_corroboration_ok(ix, a.adjudication_id, p.cusip9, day) for a in cors)  # type: ignore[arg-type]
                st = vote_state(_cusip_votes(ix, day, p.cusip9), _context_members(ix, ctx), corroborated=applies)  # type: ignore[arg-type]
                ok = ok and st.status != "consensus_y" and (
                    (lower is not None and day <= lower) or st.status != "consensus_n")  # type: ignore[operator]
            ok = ok and p.family_evidence_ids == c.sorted_uuids(family)
            ok = ok and p.evidence_observation_ids == c.sorted_uuids({*st_u.y_ids, *lower_ids, *cor_obs})
            _require(ok, "proposal_evidence_not_closed", pid)
        else:
            _require(cor_obs <= set(p.evidence_observation_ids), "proposal_evidence_not_closed", pid)
        known = proposal_known_at(ix, p)
        _require(known is not None and p.evidence_known_at == known and known <= ix.k,
                 "dependency_knowledge_mismatch", pid)


def _proposal_context_ids(ix: _Index, p: Any) -> set[uuid.UUID]:
    """Every context the proposal validator consults, including first-Y/last-N scans."""
    by_date = {ctx.report_date: ctx for ctx in ix.contexts.values()}
    contexts = {
        ix.members[fid].context_id for fid in p.family_evidence_ids if fid in ix.members
    }
    if p.proposed_status != "accepted_state":
        return contexts
    upper, lower = p.onset_upper_inclusive, p.onset_lower_exclusive
    days = {upper}
    if lower is not None:
        days.add(lower)
    days |= {
        o.report_date for o in ix.obs.values()
        if ix.is_vote(o) and o.cusip9 == p.cusip9 and o.report_date < upper
        and (o.nport_is_default == "Y" or lower is None or o.report_date > lower)
    }
    for day in sorted(days):
        ctx = by_date.get(day)
        if ctx is None:
            raise ContractError(f"proposal_evidence_not_closed:{p.proposal_evidence_id}")
        contexts.add(ctx.context_id)
    return contexts


def _dependency_rows(
    ix: _Index, *, observation_ids: Iterable[uuid.UUID] = (), link_ids: Iterable[uuid.UUID] = (),
    adjudication_ids: Iterable[uuid.UUID] = (), proposal_evidence_ids: Iterable[uuid.UUID] = (),
    exchange_relation_ids: Iterable[uuid.UUID] = (), family_evidence_ids: Iterable[uuid.UUID] = (),
    context_ids: Iterable[uuid.UUID] = (),
) -> dict[str, dict[Any, Any]]:
    """Transitive publication-local support closure over explicit W0 dependency edges."""
    reached: dict[str, dict[Any, Any]] = defaultdict(dict)
    pending: list[tuple[str, uuid.UUID]] = [
        *(("observations", x) for x in observation_ids),
        *(("event_links", x) for x in link_ids),
        *(("adjudications", x) for x in adjudication_ids),
        *(("proposal_evidence", x) for x in proposal_evidence_ids),
        *(("exchange_relations", x) for x in exchange_relation_ids),
        *(("family_evidence", x) for x in family_evidence_ids),
        *(("family_contexts", x) for x in context_ids),
    ]
    indexes: dict[str, Mapping[uuid.UUID, Any]] = {
        "observations": ix.obs, "event_links": ix.links, "adjudications": ix.adjs,
        "proposal_evidence": ix.proposals, "exchange_relations": ix.relations,
        "family_evidence": ix.members, "family_contexts": ix.contexts,
        "ncen_filings": ix.filings, "source_packages": ix.packages,
    }
    seen: set[tuple[str, uuid.UUID]] = set()

    def push(frame: str, ids: Iterable[uuid.UUID]) -> None:
        pending.extend((frame, item) for item in ids)

    while pending:
        frame, row_id = pending.pop()
        typed = (frame, row_id)
        if typed in seen:
            continue
        seen.add(typed)
        row = indexes[frame].get(row_id)
        if row is None:
            _require(not ix.strict, "dependency_missing", f"{frame}:{row_id}")
            continue
        reached[frame][row.key()] = row
        if frame == "observations":
            push("source_packages", (row.package_id,))
        elif frame == "event_links":
            push("source_packages", (row.package_id,))
            push("observations", (row.observation_id,))
            usable = row.status == "admitted"
            if row.status == "quarantined":
                head = ix.scope_head(row.link_id)
                if head is None:
                    _require(not ix.strict, "dependency_unresolved", row.link_id)
                    continue
                usable = True
                push("adjudications", (head.adjudication_id,))
            _require(usable or not ix.strict, "dependency_unresolved", row.link_id)
        elif frame == "adjudications":
            push("source_packages", (row.package_id,))
            push("observations", row.evidence_observation_ids)
            push("event_links", row.link_ids)
            push("proposal_evidence", row.proposal_evidence_ids)
        elif frame == "proposal_evidence":
            push("observations", row.evidence_observation_ids)
            push("adjudications", row.corroboration_adjudication_ids)
            push("family_evidence", row.family_evidence_ids)
            push("family_contexts", _proposal_context_ids(ix, row))
        elif frame == "exchange_relations":
            push("observations", row.exchange_document_observation_ids)
            push("event_links", (row.old_link_id, row.new_link_id))
            push("adjudications", (row.pairing_adjudication_id,))
        elif frame == "family_evidence":
            push("family_contexts", (row.context_id,))
        elif frame == "family_contexts":
            push("family_evidence", (
                member.family_evidence_id for member in ix.members.values() if member.context_id == row.context_id
            ))
            push("observations", row.vote_observation_ids)
            push("ncen_filings", row.selection_filing_ids)
            push("source_packages", row.index_package_ids)
        elif frame == "ncen_filings":
            push("source_packages", (row.package_id,))
            if row.header_package_id is not None:
                push("source_packages", (row.header_package_id,))
            push("ncen_filings", row.version_evidence_filing_ids)
        elif frame == "source_packages" and row.revision_of_package_id is not None:
            push("source_packages", (row.revision_of_package_id,))

    if ix.strict:
        graph: dict[tuple[str, uuid.UUID], set[tuple[str, uuid.UUID]]] = defaultdict(set)
        reached_links = {row.link_id: row for row in reached.get("event_links", {}).values()}
        reached_adjs = {row.adjudication_id: row for row in reached.get("adjudications", {}).values()}
        for lid, link in reached_links.items():
            if link.status == "quarantined":
                head = ix.scope_head(lid)
                if head is not None and head.adjudication_id in reached_adjs:
                    graph[("link", lid)].add(("adjudication", head.adjudication_id))
        for aid, adjudication in reached_adjs.items():
            for lid in adjudication.link_ids:
                if adjudication.subject_kind == "issue_scope" and lid == adjudication.subject_id:
                    continue
                if lid in reached_links:
                    graph[("adjudication", aid)].add(("link", lid))
        state: dict[tuple[str, uuid.UUID], int] = {}

        def visit(node: tuple[str, uuid.UUID]) -> None:
            _require(state.get(node, 0) != 1, "dependency_cycle", node[1])
            if state.get(node, 0) == 2:
                return
            state[node] = 1
            for child in graph.get(node, set()):
                visit(child)
            state[node] = 2

        for node in graph:
            visit(node)
    return {frame: rows for frame, rows in reached.items() if rows}


def _dependency_known_at(ix: _Index, rows: Mapping[str, Mapping[Any, Any]]) -> dt.datetime | None:
    """Maximum public time across a complete closure, preserving logical N-CEN version time."""
    times = [row.public_available_at for row in rows.get("observations", {}).values()]
    times += [row.link_known_at for row in rows.get("event_links", {}).values()]
    times += [row.evidence_known_at for row in rows.get("family_contexts", {}).values()]
    return max(times, default=None)


def proposal_known_at(ix: _Index, p: Any) -> dt.datetime | None:
    """Public time of the proposal's complete transitive support closure."""
    rows = _dependency_rows(
        ix, observation_ids=p.evidence_observation_ids,
        adjudication_ids=p.corroboration_adjudication_ids,
        family_evidence_ids=p.family_evidence_ids,
        context_ids=_proposal_context_ids(ix, p),
    )
    return _dependency_known_at(ix, rows)


def relation_known_at(ix: _Index, r: c.ExchangeRelation) -> dt.datetime:
    """Public time of a relation: both sides' links and observations, the exchange documents
    and all public support cited by the pairing decision (never its review time)."""
    known = _dependency_known_at(ix, _dependency_rows(
        ix,
        observation_ids=r.exchange_document_observation_ids,
        link_ids=(r.old_link_id, r.new_link_id),
        adjudication_ids=(r.pairing_adjudication_id,),
    ))
    if known is None:
        raise ContractError(f"dependency_missing:{r.relation_id}")
    return known


def _check_relations(ix: _Index, events: Mapping[uuid.UUID, Any]) -> None:
    for rid, r in sorted(ix.relations.items(), key=lambda item: str(item[0])):
        ev = events.get(r.episode_id)
        d = r.exchange_effective_date
        old, new = ix.links.get(r.old_link_id), ix.links.get(r.new_link_id)
        ok = (ev is not None and ev.is_exchange and ev.security_id == r.old_security_id and ev.cusip9 == r.old_cusip9
              and ev.alias_spell_id == r.alias_spell_id and rid in ev.exchange_relation_ids
              and (d == ev.onset_upper_inclusive if ev.primary_type == "distressed_exchange"
                   else d >= ev.onset_upper_inclusive))
        for x, scope, security, cusip in ((old, "exchange_old", r.old_security_id, r.old_cusip9),
                                          (new, "exchange_new", r.new_security_id, r.new_cusip9)):
            ok = ok and x is not None and x.status == "admitted" and x.link_id not in ix.stale_links \
                and x.affected_scope == scope and x.security_id == security and x.cusip9 == cusip \
                and x.valid_from <= d and (x.valid_to is None or x.valid_to >= d) \
                and x.observation_id in r.exchange_document_observation_ids
        docs_ok = all(o in ix.obs and o not in ix.stale_obs and ix.obs[o].observation_kind != "nport_holding"
                      for o in r.exchange_document_observation_ids)
        ok = ok and docs_ok
        if ok:
            if new.valid_to == dt.date.max:  # type: ignore[union-attr]
                ok = False
            elif new.valid_to is None:  # type: ignore[union-attr]
                ok = r.valid_to is None and old.valid_to is None  # type: ignore[union-attr]
            else:
                ok = r.valid_to is not None and r.valid_to - dt.timedelta(days=1) == new.valid_to  # type: ignore[union-attr]
                ok = ok and (old.valid_to is None or r.valid_to - dt.timedelta(days=1) <= old.valid_to)  # type: ignore[union-attr]
        a = ix.adjs.get(r.pairing_adjudication_id)
        ok = ok and ix.pairing_head(rid) is a and a is not None \
            and a.subject_kind == "exchange_pairing" and a.status == c.EVIDENCE_STATUS \
            and a.reviewer_role == "human_reviewer" and a.policy_digest == ix.policy_digest \
            and a.adjudicated_at <= ix.k and a.subject_id == rid \
            and {r.old_link_id, r.new_link_id} <= set(a.link_ids) \
            and set(r.exchange_document_observation_ids) <= set(a.evidence_observation_ids) \
            and a.support_valid_from == d and a.support_valid_to == r.valid_to  # type: ignore[union-attr]
        ok = ok and all(
            lid in ix.links and (
                ix.links[lid].status == "admitted"
                or (ix.links[lid].status == "quarantined" and ix.scope_head(lid) is not None)
            )
            for lid in a.link_ids  # type: ignore[union-attr]
        )
        _require(ok, "exchange_pairing_invalid", rid)
        known = relation_known_at(ix, r)
        _require(r.evidence_known_at == known and known <= ix.k, "dependency_knowledge_mismatch", rid)
    # Directional graph: one old origin per new issue over overlapping validity; acyclic.
    rows = sorted(ix.relations.values(), key=lambda r: str(r.relation_id))
    for i, a in enumerate(rows):
        for b in rows[i + 1:]:
            if a.new_security_id == b.new_security_id and a.old_security_id != b.old_security_id and (
                (b.valid_to is None or a.valid_from < b.valid_to) and (a.valid_to is None or b.valid_from < a.valid_to)
            ):
                _require(False, "exchange_alias_ambiguous", b.new_security_id)
    successors: dict[uuid.UUID, set[uuid.UUID]] = defaultdict(set)
    for r in rows:
        successors[r.old_security_id].add(r.new_security_id)
    for start in sorted(successors, key=str):
        seen: set[uuid.UUID] = set()
        frontier = set(successors[start])
        while frontier:
            _require(start not in frontier, "dependency_cycle", start)
            seen |= frontier
            frontier = {n for f in frontier for n in successors.get(f, ())} - seen
    for ev in events.values():
        expected = {r.relation_id for r in rows if r.episode_id == ev.episode_id}
        _require(set(ev.exchange_relation_ids) == expected, "exchange_pairing_invalid", ev.episode_id)


def event_dependency_rows(
    ix: _Index,
    *,
    evidence_observation_ids: Iterable[uuid.UUID],
    onset_lower_evidence_ids: Iterable[uuid.UUID],
    link_ids: Iterable[uuid.UUID],
    adjudication_ids: Iterable[uuid.UUID],
    proposal_evidence_ids: Iterable[uuid.UUID],
    exchange_relation_ids: Iterable[uuid.UUID],
) -> dict[str, dict[Any, Any]]:
    """Distinct non-event rows an event reaches, keyed by frame then row key.

    Direct evidence and links, issue adjudications, scope decisions admitting quarantined
    direct links, proposals with their memberships, contexts (whole voter universe),
    selection filings and version proofs, corroborations, relations with their pairing
    decisions, and every package owning (or header-binding / indexing) a reached row.
    Resolution references keep their own knowledge time and are not part of the closure.
    """
    return _dependency_rows(
        ix,
        observation_ids=(*evidence_observation_ids, *onset_lower_evidence_ids),
        link_ids=link_ids,
        adjudication_ids=adjudication_ids,
        proposal_evidence_ids=proposal_evidence_ids,
        exchange_relation_ids=exchange_relation_ids,
    )


def dependency_digest_of(rows_by_frame: Mapping[str, Mapping[Any, Any]]) -> str:
    """``digest_of`` of ``{frame, count, digest}`` per reached non-empty frame, by frame name."""
    return c.digest_of([
        {"frame": frame, "count": len(rows), "digest": c.frame_digest(r.row_sha256() for r in rows.values())}
        for frame, rows in sorted(rows_by_frame.items()) if rows
    ])


def event_dependency_digest(frames: Mapping[str, Iterable[Any]], *, knowledge_cutoff: dt.datetime,
                            knowledge_mode: str, policy_digest: str = c.POLICY_DIGEST, **edges: Any) -> str:
    """Builder helper: the ``dependency_digest`` of an event's edges over ``frames``."""
    ix = _Index.build(frames, k=knowledge_cutoff, mode=knowledge_mode, policy_digest=policy_digest)
    return dependency_digest_of(event_dependency_rows(ix, **edges))


def _event_edges(ev: Any) -> dict[str, Any]:
    return {
        "evidence_observation_ids": ev.evidence_observation_ids,
        "onset_lower_evidence_ids": ev.onset_lower_evidence_ids,
        "link_ids": ev.link_ids,
        "adjudication_ids": ev.adjudication_ids,
        "proposal_evidence_ids": ev.proposal_evidence_ids,
        "exchange_relation_ids": ev.exchange_relation_ids,
    }


def event_link_known_at(ix: _Index, ev: Any) -> dt.datetime:
    """Max link knowledge over direct (scope-inclusive), relation, pairing and corroboration links."""
    rows = event_dependency_rows(ix, **_event_edges(ev))
    times = [ix.link_time(link.link_id) for link in rows.get("event_links", {}).values()]
    return max(times)


def event_evidence_known_at(ix: _Index, ev: Any) -> dt.datetime:
    """Public evidence boundary of an event over its full typed closure."""
    known = _dependency_known_at(ix, event_dependency_rows(ix, **_event_edges(ev)))
    if known is None:
        raise ContractError(f"dependency_missing:{getattr(ev, 'episode_id', 'event')}")
    return known


def _event_admitting_head(ix: _Index, event: Any) -> Adjudication | None:
    head = ix.heads.get(("issue_episode", event.episode_id))
    return head if (
        head is not None
        and head.adjudication_id in event.adjudication_ids
        and head.status == event.admission_status
        and head.policy_digest == ix.policy_digest
    ) else None


def _check_event_dependencies(ix: _Index, events: Iterable[Any]) -> None:
    for ev in events:
        key = ev.episode_id
        head = _event_admitting_head(ix, ev)
        _require(head is not None, "event_evidence_invalid", key)
        assert head is not None
        proposals = [ix.proposals.get(p) for p in ev.proposal_evidence_ids]
        ok = all(p is not None and p.proposed_status == "accepted_state" and p.cusip9 == ev.cusip9 for p in proposals)
        ok = ok and head.proposal_evidence_ids == ev.proposal_evidence_ids
        _require(ok, "proposal_evidence_not_closed", key)
        _require(all(r in ix.relations for r in ev.exchange_relation_ids), "exchange_pairing_invalid", key)


def _rating_candidate_exists(
    ix: _Index,
    row: Any,
    scope_keys: set[tuple[str | None, str | None, str | None]],
) -> bool:
    """Whether W2b has an approved, in-scope action group for this grid key.

    A candidate can still resolve to ``missing`` because of a symbol problem or same-day conflict;
    that is distinct from a genuinely unavailable key which declarations mark rights-unverified.
    """
    month_end = c.month_end(row.month)
    boundary = c.next_month_boundary_utc(row.month)
    for action in ix.obs.values():
        if (
            action.observation_kind != "agency_action"
            or action.observation_id in ix.stale_obs
            or action.agency_subject_kind != "instrument"
            or action.agency_action_date is None
            or action.agency_action_date > month_end
            or action.public_available_at > ix.k
            or (ix.current_run and action.first_seen_at > ix.k)
            or (row.view_kind == "public_pit" and action.public_available_at >= boundary)
            or (action.agency_name, action.agency_rating_type, action.agency_scale) not in scope_keys
        ):
            continue
        package = ix.packages.get(action.package_id)
        if package is None or not c.is_approved_rating_package(package):
            continue
        if action.cusip9 == row.cusip_id:
            return True
        if action.cusip9 is not None:
            continue
        if any(
            link.observation_id == action.observation_id
            and link.package_id in ix.packages
            and link.link_id not in ix.stale_links
            and link.status == "admitted"
            and link.affected_scope == "issue"
            and link.cusip9 == row.cusip_id
            and link.valid_from <= month_end
            and (link.valid_to is None or link.valid_to >= month_end)
            and link.link_known_at <= ix.k
            and (row.view_kind != "public_pit" or link.link_known_at < boundary)
            for link in ix.links.values()
        ):
            return True
    return False


def _check_rating_row(ix: _Index, row: Any) -> bool:
    """Rated-row derivation: binding links, times, frontier, bucket and ``action_input_digest``.

    Each relied action names the row CUSIP or is bound to it by persisted binding links:
    admitted, current at K, ``affected_scope == 'issue'``, valid at the month-end snapshot,
    known by K and (``public_pit``) before the next month boundary. The bucket is recomputed
    from the relied actions and the digest from their observation/package/link rows.
    """
    relied = [ix.obs[oid] for oid in row.agency_source_ids]
    if not relied:
        return not row.binding_link_ids and row.action_input_digest is None
    month_end = c.month_end(row.month)
    boundary = c.next_month_boundary_utc(row.month)
    binding: list[EventLink] = []
    for lid in row.binding_link_ids:
        x = ix.links.get(lid)
        if x is None or lid in ix.stale_links or x.status != "admitted" or x.affected_scope != "issue" \
                or x.cusip9 != row.cusip_id or x.observation_id not in row.agency_source_ids \
                or ix.obs[x.observation_id].cusip9 == row.cusip_id \
                or x.valid_from > month_end or (x.valid_to is not None and x.valid_to < month_end) \
                or x.link_known_at > ix.k \
                or (row.view_kind == "public_pit" and x.link_known_at >= boundary):
            return False
        binding.append(x)
    bound = {x.observation_id for x in binding}
    # An action names the row CUSIP or is bound to it by persisted binding links.
    if any(o.cusip9 != row.cusip_id and o.observation_id not in bound for o in relied):
        return False
    action_date = max(o.agency_action_date for o in relied)  # type: ignore[type-var]
    known = max([
        *(o.public_available_at for o in relied),
        *(x.link_known_at for x in binding),
    ])
    if row.action_date != action_date or action_date > month_end or row.public_known_at != known:
        return False
    if row.view_kind == "public_pit" and known >= boundary:
        return False
    frontiers = [c.rating_coverage(ix.packages[o.package_id], row.view_kind)[1] for o in relied]
    relied_frontier = None if None in frontiers else max(frontiers)  # type: ignore[type-var]
    if row.coverage_frontier is not None and row.coverage_frontier != relied_frontier:
        return False
    if row.state == "carried_verified" and (row.coverage_frontier is None or row.coverage_frontier < month_end):
        return False
    kinds = {c.classify_rating_action(o) for o in relied}
    if row.state in c.RATED_STATES and kinds != {("rated", row.bucket)}:
        return False
    if row.state == "withdrawn" and kinds != {("withdrawn", None)}:
        return False
    digest = c.rating_action_input_digest(
        row.view_kind, relied, {ix.packages[o.package_id].package_id: ix.packages[o.package_id] for o in relied}.values(),
        binding,
    )
    return row.action_input_digest == digest


# ---------------------------------------------------------------------------
# Builder API (pure; used by W0 fixtures, A2a/A2b/A3 and order-4 integration). Each helper
# derives exactly the value ``check_bundle``/``bond_credit_validate`` recompute, so builders
# never hand-assemble a derived scalar.
# ---------------------------------------------------------------------------
def dependency_index(
    frames: Mapping[str, Iterable[Any]], *, knowledge_cutoff: dt.datetime, knowledge_mode: str,
    policy_digest: str = c.POLICY_DIGEST,
) -> _Index:
    """Typed index over (partial) bundle frames for the builder helpers below."""
    return _Index.build(frames, k=c._as_ts("knowledge_cutoff", knowledge_cutoff), mode=knowledge_mode,
                        policy_digest=policy_digest, strict=False)


def build_family_frames(
    frames: Mapping[str, Iterable[Any]], report_dates: Iterable[dt.date], *, knowledge_cutoff: dt.datetime,
    knowledge_mode: str,
) -> tuple[tuple[c.FamilyContext, ...], tuple[c.FamilyMembership, ...]]:
    """Full FE-1 contexts and memberships of ``report_dates`` from the persisted inventory
    (``source_packages``, ``observations``, ``ncen_filings``); raises ``family_universe_not_closed``."""
    ix = dependency_index(frames, knowledge_cutoff=knowledge_cutoff, knowledge_mode=knowledge_mode)
    contexts: list[c.FamilyContext] = []
    members: list[c.FamilyMembership] = []
    for report_date in sorted(set(report_dates)):
        want = expected_context(ix, report_date)
        _require(want.evidence_known_at is not None, "family_universe_not_closed", report_date)
        context_id = c.FamilyContext.derive_id(c.FAMILY_RULE_VERSION, report_date, ix.k, want.vote_observation_ids,
                                               want.selection_filing_ids, want.index_package_ids)
        rows = [
            c.FamilyMembership(
                family_evidence_id=c.FamilyMembership.derive_id(context_id, cik), context_id=context_id,
                registrant_cik=cik, valid_from=report_date, valid_to=report_date + dt.timedelta(days=1),
                **dict(values),
            )
            for cik, values in sorted(want.members.items())
        ]
        members += rows
        contexts.append(c.FamilyContext(
            context_id=context_id, report_date=report_date, knowledge_cutoff=ix.k, rule_version=c.FAMILY_RULE_VERSION,
            vote_observation_ids=want.vote_observation_ids, selection_filing_ids=want.selection_filing_ids,
            index_package_ids=want.index_package_ids, universe_digest=want.universe_digest,
            membership_digest=c.frame_digest(m.row_sha256() for m in rows),
            evidence_known_at=want.evidence_known_at,  # type: ignore[arg-type]
        ))
    return tuple(contexts), tuple(members)


def derive_state_proposal(
    frames: Mapping[str, Iterable[Any]], *, cusip9: str, onset_upper_inclusive: dt.date,
    onset_lower_exclusive: dt.date | None = None, corroboration_adjudication_ids: Iterable[uuid.UUID] = (),
    knowledge_cutoff: dt.datetime, knowledge_mode: str, policy_digest: str = c.POLICY_DIGEST,
) -> c.ProposalEvidence:
    """Accepted W1 state proposal of ``cusip9`` recomputed from persisted contexts in ``frames``.

    The upper date needs a consensus Y (the lower date, when given, a consensus N) under
    the persisted FE-1 memberships; bounds, evidence and family edges are derived, never
    supplied. Raises ``proposal_evidence_not_closed`` when the persisted evidence does not
    support the requested bounds.
    """
    ix = dependency_index(frames, knowledge_cutoff=knowledge_cutoff, knowledge_mode=knowledge_mode,
                          policy_digest=policy_digest)
    by_date = {ctx.report_date: ctx for ctx in ix.contexts.values()}
    cors = c.sorted_uuids(corroboration_adjudication_ids)
    ctx_u = by_date.get(onset_upper_inclusive)
    _require(ctx_u is not None, "proposal_evidence_not_closed", cusip9)
    members_u = _context_members(ix, ctx_u)  # type: ignore[arg-type]
    st_u = vote_state(_cusip_votes(ix, onset_upper_inclusive, cusip9), members_u, corroborated=bool(cors))
    _require(st_u.status == "consensus_y", "proposal_evidence_not_closed", cusip9)
    family = {members_u[r].family_evidence_id for r in st_u.relied_registrants}
    lower_ids: tuple[uuid.UUID, ...] = ()
    if onset_lower_exclusive is not None:
        ctx_l = by_date.get(onset_lower_exclusive)
        _require(ctx_l is not None, "proposal_evidence_not_closed", cusip9)
        members_l = _context_members(ix, ctx_l)  # type: ignore[arg-type]
        st_l = vote_state(_cusip_votes(ix, onset_lower_exclusive, cusip9), members_l, corroborated=False)
        _require(st_l.status == "consensus_n", "proposal_evidence_not_closed", cusip9)
        lower_ids = st_l.n_ids
        family |= {members_l[r].family_evidence_id for r in st_l.relied_registrants}
    cor_obs = {o for a in cors for o in ix.adjs[a].evidence_observation_ids}
    evidence = c.sorted_uuids({*st_u.y_ids, *lower_ids, *cor_obs})
    families = c.sorted_uuids(family)
    draft = SimpleNamespace(
        proposal_evidence_id=uuid.UUID(int=0),
        cusip9=cusip9, proposed_status="accepted_state", onset_lower_exclusive=onset_lower_exclusive,
        onset_upper_inclusive=onset_upper_inclusive, evidence_observation_ids=evidence,
        family_evidence_ids=families, corroboration_adjudication_ids=cors,
    )
    known = proposal_known_at(ix, draft)
    return c.ProposalEvidence.create(
        cusip9=cusip9, proposed_status="accepted_state", basis=st_u.basis,
        onset_lower_exclusive=onset_lower_exclusive, onset_upper_inclusive=onset_upper_inclusive,
        onset_lower_evidence_ids=lower_ids, onset_upper_evidence_ids=st_u.y_ids,
        evidence_observation_ids=evidence, family_evidence_ids=families,
        corroboration_adjudication_ids=cors, evidence_known_at=known, policy_digest=policy_digest,
    )


def proposal_evidence_known_at(frames: Mapping[str, Iterable[Any]], *, knowledge_cutoff: dt.datetime,
                               knowledge_mode: str, **edges: Any) -> dt.datetime | None:
    """``ProposalEvidence.evidence_known_at`` for ``evidence_observation_ids``,
    ``family_evidence_ids`` and ``corroboration_adjudication_ids`` over ``frames``."""
    ix = dependency_index(frames, knowledge_cutoff=knowledge_cutoff, knowledge_mode=knowledge_mode)
    required = {"cusip9", "proposed_status", "onset_lower_exclusive", "onset_upper_inclusive"}
    _require(required <= set(edges), "dependency_missing", "proposal_descriptor")
    return proposal_known_at(ix, SimpleNamespace(proposal_evidence_id=uuid.UUID(int=0), **edges))


def exchange_relation_known_at(frames: Mapping[str, Iterable[Any]], relation: c.ExchangeRelation, *,
                               knowledge_cutoff: dt.datetime, knowledge_mode: str) -> dt.datetime:
    ix = dependency_index(frames, knowledge_cutoff=knowledge_cutoff, knowledge_mode=knowledge_mode)
    return relation_known_at(ix, relation)


def derive_event_fields(frames: Mapping[str, Iterable[Any]], *, knowledge_cutoff: dt.datetime,
                        knowledge_mode: str, **edges: Any) -> dict[str, Any]:
    """``link_known_at``, ``evidence_known_at``, ``dependency_digest`` and ``event_input_digest``
    of an event with the six typed edges in ``edges`` (sorted arrays are returned too)."""
    ix = dependency_index(frames, knowledge_cutoff=knowledge_cutoff, knowledge_mode=knowledge_mode)
    sorted_edges = {name: c.sorted_uuids(edges[name]) for name in (
        "evidence_observation_ids", "onset_lower_evidence_ids", "link_ids", "adjudication_ids",
        "proposal_evidence_ids", "exchange_relation_ids")}
    view = SimpleNamespace(**sorted_edges)
    dependency = dependency_digest_of(event_dependency_rows(ix, **sorted_edges))
    return {
        **sorted_edges,
        "link_known_at": event_link_known_at(ix, view),
        "evidence_known_at": event_evidence_known_at(ix, view),
        "dependency_digest": dependency,
        "event_input_digest": c.DefaultEpisode.derive_input_digest(
            sorted_edges["evidence_observation_ids"], sorted_edges["link_ids"], sorted_edges["adjudication_ids"],
            sorted_edges["proposal_evidence_ids"], sorted_edges["exchange_relation_ids"], dependency,
        ),
    }


def persisted_identity(bundle: CreditBundle) -> str:
    """Digest of everything a store persists: manifest + every frame row.

    The panel grid list itself is bound through ``panel_grid_digest``/``panel_grid_count``
    in the manifest and is not stored separately, so replay compares this digest.
    """
    payload = bundle.to_json_obj()
    payload.pop("panel_grid")
    return c.digest_of(payload)


def verify_schema(bundle: CreditBundle) -> CreditBundle:
    """Round-trip through the strict JSON schema and the typed decoder."""
    decoded = CreditBundle.from_json_obj(bundle.to_json_obj(), schema_check=True)
    if decoded.canonical_bytes() != bundle.canonical_bytes():
        raise ContractError("schema_roundtrip_mismatch")
    return decoded


# ---------------------------------------------------------------------------
# Store protocol and lifecycle functions
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PublicationState:
    publication_id: uuid.UUID
    publication_version: int
    lifecycle_state: str
    quality_state: str
    build_scope: str
    target_month: dt.date
    knowledge_cutoff: dt.datetime
    revoked: bool


class PublicationStore(Protocol):
    def prepare(self, bundle: CreditBundle) -> str: ...

    def validate(self, publication_id: uuid.UUID) -> PublicationState: ...

    def promote(self, publication_id: uuid.UUID, expected_pointer: uuid.UUID | None) -> uuid.UUID: ...

    def revoke(self, publication_id: uuid.UUID, reason: str, evidence_digest: str) -> None: ...

    def state(self, publication_id: uuid.UUID) -> PublicationState: ...

    def current(
        self, *, expected_target_month: dt.date | None = None,
        expected_panel_publication_id: uuid.UUID | None = None,
    ) -> BundleManifest: ...

    def read(self, publication_id: uuid.UUID, *, allow_shadow: bool = False) -> CreditBundle: ...


def prepare_bundle(store: PublicationStore, bundle: CreditBundle) -> str:
    """Persist a prepared build; returns ``inserted`` or ``replayed`` (exact no-op)."""
    try:
        verify_schema(bundle)
        verify_derivation(bundle)
    except ContractError as exc:
        raise PublicationError("bond_credit_prepare:invalid_bundle", str(exc)) from exc
    return store.prepare(bundle)


def validate_bundle(store: PublicationStore, publication_id: uuid.UUID) -> PublicationState:
    """Validate a prepared build in one transaction (idempotent once validated)."""
    return store.validate(publication_id)


def promote_bundle(
    store: PublicationStore, publication_id: uuid.UUID, *, expected_pointer: uuid.UUID | None
) -> uuid.UUID:
    """Compare-and-set the serving pointer to a qualified complete build."""
    return store.promote(publication_id, expected_pointer)


# ---------------------------------------------------------------------------
# In-memory store (mirrors the SQL lifecycle exactly)
# ---------------------------------------------------------------------------
@dataclass
class _MemPublication:
    bundle: CreditBundle
    version: int
    lifecycle_state: str = "prepared"


class InMemoryPublicationStore:
    """Test store with the database's immutability, CAS and revocation semantics."""

    def __init__(self) -> None:
        self.ledger: dict[str, dict[uuid.UUID, Any]] = {name: {} for name in c.INPUT_FRAMES}
        self.publications: dict[uuid.UUID, _MemPublication] = {}
        self.pointer: uuid.UUID | None = None
        self.revocations: dict[uuid.UUID, dict[str, Any]] = {}

    @staticmethod
    def _row_id(row: Any) -> uuid.UUID:
        return getattr(row, row.KEY[0])

    def prepare(self, bundle: CreditBundle) -> str:
        pid = bundle.publication_id
        existing = self.publications.get(pid)
        if existing is not None:
            if persisted_identity(existing.bundle) == persisted_identity(bundle):
                return "replayed"
            raise PublicationCollision("bond_credit_prepare:publication_collision", str(pid))
        sealed = {
            s.package_id for pub in self.publications.values() for s in pub.bundle.frames["publication_sources"]  # type: ignore[attr-defined]
        }
        staged: list[tuple[str, uuid.UUID, Any]] = []
        for frame in c.INPUT_FRAMES:
            for row in bundle.frames[frame]:
                rid = self._row_id(row)
                stored = self.ledger[frame].get(rid)
                if stored is not None:
                    if stored.row_sha256() != row.row_sha256():
                        raise PublicationCollision("bond_credit_prepare:ledger_row_collision", f"{frame}:{rid}")
                    continue
                if frame != "source_packages" and row.package_id in sealed:
                    raise PublicationCollision("bond_credit_prepare:package_inventory_collision", str(row.package_id))
                staged.append((frame, rid, row))
        owned: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
        for frame in c.INPUT_FRAMES[1:]:
            rows = list(self.ledger[frame].values()) + [r for f, _, r in staged if f == frame]
            for row in rows:
                owned[row.package_id].append(self._row_id(row))
        for source in bundle.frames["publication_sources"]:
            if c.id_inventory_digest(owned[source.package_id]) != source.row_inventory_digest:  # type: ignore[attr-defined]
                raise PublicationCollision(
                    "bond_credit_prepare:package_inventory_collision", str(source.package_id)  # type: ignore[attr-defined]
                )
        # Keep a private deep-frozen copy: nothing the caller holds can reach the stored build.
        stored_bundle = CreditBundle.from_json_obj(bundle.to_json_obj(), schema_check=False)
        if stored_bundle.canonical_bytes() != bundle.canonical_bytes():
            raise PublicationError("bond_credit_prepare:invalid_bundle", "copy_roundtrip_mismatch")
        stored_rows = {frame: {self._row_id(row): row for row in stored_bundle.frames[frame]} for frame in c.INPUT_FRAMES}
        for frame, rid, _row in staged:
            self.ledger[frame][rid] = stored_rows[frame][rid]
        version = max((p.version for p in self.publications.values()), default=0) + 1
        self.publications[pid] = _MemPublication(bundle=stored_bundle, version=version)
        return "inserted"

    def _get(self, publication_id: uuid.UUID, fn: str) -> _MemPublication:
        pub = self.publications.get(publication_id)
        if pub is None:
            raise PublicationError(f"{fn}:unknown_publication", str(publication_id))
        return pub

    def state(self, publication_id: uuid.UUID) -> PublicationState:
        pub = self._get(publication_id, "bond_credit_state")
        m = pub.bundle.manifest
        return PublicationState(
            publication_id=publication_id,
            publication_version=pub.version,
            lifecycle_state=pub.lifecycle_state,
            quality_state=m["quality_state"],
            build_scope=m["build_scope"],
            target_month=m["target_month"],
            knowledge_cutoff=m["knowledge_cutoff"],
            revoked=publication_id in self.revocations,
        )

    def validate(self, publication_id: uuid.UUID) -> PublicationState:
        pub = self._get(publication_id, "bond_credit_validate")
        if pub.lifecycle_state == "validated":
            return self.state(publication_id)
        try:
            check_bundle(pub.bundle)
        except ContractError as exc:
            reason, _, detail = str(exc).partition(":")
            raise PublicationError(f"bond_credit_validate:{reason}", detail) from exc
        pub.lifecycle_state = "validated"
        return self.state(publication_id)

    def promote(self, publication_id: uuid.UUID, expected_pointer: uuid.UUID | None) -> uuid.UUID:
        fn = "bond_credit_promote"
        if self.pointer != expected_pointer:
            raise PublicationError(f"{fn}:cas_mismatch", f"expected={expected_pointer} current={self.pointer}")
        pub = self._get(publication_id, fn)
        m = pub.bundle.manifest
        if pub.lifecycle_state != "validated":
            raise PublicationError(f"{fn}:not_validated")
        if m["build_scope"] != "complete" or m["quality_state"] != "qualified":
            raise PublicationError(f"{fn}:not_qualified_complete", f"{m['build_scope']}/{m['quality_state']}")
        if publication_id in self.revocations:
            raise PublicationError(f"{fn}:revoked")
        # Re-verify the stored frames against the manifest immediately before promotion
        # (mirrors ``bond_credit_assert_output_frames`` in ``bond_credit_promote``).
        try:
            pub.bundle.verify_frames_against_manifest()
        except ContractError as exc:
            _, _, detail = str(exc).partition(":")
            raise PublicationError(f"{fn}:frame_mismatch", detail) from exc
        if self.pointer == publication_id:
            return publication_id
        if self.pointer is not None:
            cur = self.publications[self.pointer].bundle.manifest
            if m["target_month"] < cur["target_month"] or m["knowledge_cutoff"] < cur["knowledge_cutoff"]:
                raise PublicationError(f"{fn}:tk_regression")
            if (
                m["target_month"] == cur["target_month"]
                and m["knowledge_cutoff"] == cur["knowledge_cutoff"]
                and self.pointer not in self.revocations
            ):
                raise PublicationError(f"{fn}:tk_not_advanced")
        self.pointer = publication_id
        return publication_id

    def revoke(self, publication_id: uuid.UUID, reason: str, evidence_digest: str) -> None:
        pub = self.publications.get(publication_id)
        if pub is None or pub.lifecycle_state != "validated":
            raise PublicationError("bond_credit_revoke:unknown_or_unvalidated", str(publication_id))
        if publication_id in self.revocations:
            raise PublicationError("bond_credit_revoke:already_revoked", str(publication_id))
        c._check_value("reason", "text", reason, c.ENUMS)
        c._check_value("evidence_digest", "digest", evidence_digest, c.ENUMS)
        self.revocations[publication_id] = {"reason": reason, "evidence_digest": evidence_digest}

    def current(
        self, *, expected_target_month: dt.date | None = None,
        expected_panel_publication_id: uuid.UUID | None = None,
    ) -> BundleManifest:
        fn = "bond_credit_current_publication"
        if self.pointer is None:
            raise PublicationError(f"{fn}:no_current_publication")
        pub = self.publications[self.pointer]
        m = pub.bundle.manifest
        if self.pointer in self.revocations:
            raise PublicationError(f"{fn}:revoked", str(self.pointer))
        if pub.lifecycle_state != "validated" or m["quality_state"] != "qualified" or m["build_scope"] != "complete":
            raise PublicationError(f"{fn}:not_serving_eligible", str(self.pointer))
        if expected_target_month is not None and m["target_month"] != expected_target_month:
            raise PublicationError(f"{fn}:target_month_mismatch")
        if expected_panel_publication_id is not None and m["panel_publication_id"] != expected_panel_publication_id:
            raise PublicationError(f"{fn}:panel_mismatch")
        return m

    def read(self, publication_id: uuid.UUID, *, allow_shadow: bool = False) -> CreditBundle:
        fn = "bond_credit_read_publication"
        pub = self._get(publication_id, fn)
        m = pub.bundle.manifest
        if pub.lifecycle_state != "validated":
            raise PublicationError(f"{fn}:not_validated", str(publication_id))
        if publication_id in self.revocations:
            raise PublicationError(f"{fn}:revoked", str(publication_id))
        if not allow_shadow and (m["quality_state"] != "qualified" or m["build_scope"] != "complete"):
            raise PublicationError(f"{fn}:shadow_build_requires_allow_shadow", str(publication_id))
        return pub.bundle


# ---------------------------------------------------------------------------
# PostgreSQL store (immutable writer / reader over the four SQL files)
# ---------------------------------------------------------------------------
TABLES = {
    "source_packages": "bond_default_source_package",
    "observations": "bond_credit_observation",
    "event_links": "bond_default_event_link",
    "adjudications": "bond_default_adjudication",
    "publication_sources": "bond_credit_publication_sources",
    "events": "bond_default_event_v1",
    "followups": "bond_default_followup_v1",
    "exit_evidence": "bond_default_exit_evidence_v1",
    "coverage": "bond_default_coverage_v1",
    "ratings": "bond_rating_history_public_v1",
    "ncen_filings": "bond_default_ncen_filing",
    "family_contexts": "bond_default_family_context_v2",
    "family_evidence": "bond_default_family_evidence_v2",
    "proposal_evidence": "bond_default_proposal_evidence_v2",
    "exchange_relations": "bond_default_exchange_relation_v2",
}
_SUPERSEDES = {
    "observations": "supersedes_observation_id",
    "event_links": "supersedes_link_id",
    "adjudications": "supersedes_adjudication_id",
    "source_packages": "revision_of_package_id",
    "ncen_filings": "supersedes_filing_evidence_id",
}
#: Frame -> package-owned id column (package inventory lineage).
_OWNED_ID = {"observations": "observation_id", "event_links": "link_id",
             "adjudications": "adjudication_id", "ncen_filings": "filing_evidence_id"}


def install_schema(conn: Any, schema: str) -> None:
    """Apply the four SQL files to ``schema`` (autocommit connection; files own BEGIN/COMMIT).

    For disposable test databases and explicit operator use only; the worker never
    installs schema implicitly.
    """
    from psycopg import sql

    if not conn.autocommit:
        raise ValueError("autocommit_connection_required_for_ddl")
    conn.execute(sql.SQL("SET search_path TO {}, pg_temp").format(sql.Identifier(schema)))
    for path in c.SQL_PATHS:
        conn.execute(path.read_text(encoding="utf-8"))


def _topological(rows: Iterable[Any], id_attr: str, parent_attr: str) -> list[Any]:
    pending = {getattr(row, id_attr): row for row in rows}
    ordered: list[Any] = []
    done: set[uuid.UUID] = set()
    while pending:
        ready = [
            rid for rid, row in pending.items()
            if getattr(row, parent_attr) is None or getattr(row, parent_attr) not in pending
        ]
        if not ready:
            raise ContractError("supersession_cycle")
        for rid in sorted(ready, key=str):
            ordered.append(pending.pop(rid))
            done.add(rid)
    return ordered


def _db_value(kind: str, value: Any) -> Any:
    from psycopg.types.json import Jsonb

    if value is None:
        return None
    base = kind.rstrip("?")
    if base in ("members", "presence"):
        return Jsonb(dict(value))
    if base == "rating_declarations":
        return Jsonb(c.normalize_rating_declarations_record(value))
    if base in c.STRUCTURED_KINDS:
        return Jsonb([dict(record) for record in value])
    if isinstance(value, tuple):
        return list(value)
    return value


def _raise_db(exc: Exception) -> None:
    import psycopg

    if isinstance(exc, psycopg.Error):
        message = (exc.diag.message_primary if exc.diag and exc.diag.message_primary else str(exc)).strip()
        head = message.split("\n", 1)[0]
        parts = head.split(":")
        if len(parts) >= 2 and parts[0].startswith("bond_credit_"):
            raise PublicationError(":".join(parts[:2]), ":".join(parts[2:])) from exc
        raise PublicationError("bond_credit_db:error", head) from exc
    raise exc


class PostgresPublicationStore:
    """Immutable DB writer/reader. Every identifier is schema-qualified."""

    def __init__(self, conn: Any, schema: str) -> None:
        self.conn = conn
        self.schema = schema

    # -- helpers ----------------------------------------------------------
    def _q(self, name: str) -> Any:
        from psycopg import sql

        return sql.Identifier(self.schema, name)

    def _fn(self, name: str) -> Any:
        from psycopg import sql

        return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(name))

    def _transaction(self) -> Any:
        from psycopg import pq

        if self.conn.info.transaction_status != pq.TransactionStatus.IDLE:
            raise PublicationError("bond_credit_db:connection_not_idle")
        return self.conn.transaction()

    def _run(self, action: Callable[[], Any]) -> Any:
        try:
            with self._transaction():
                return action()
        except PublicationError:
            raise
        except ContractError as exc:
            raise PublicationError("bond_credit_db:contract_violation", str(exc)) from exc
        except Exception as exc:  # psycopg errors -> coded publication errors
            _raise_db(exc)
            raise

    def _insert(self, frame: str, rows: Iterable[Any], publication_id: uuid.UUID | None = None) -> None:
        from psycopg import sql

        cls = FRAME_TYPES[frame]
        names = ([] if publication_id is None else ["publication_id"]) + [n for n, _ in cls.SPEC] + ["row_sha256"]
        statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            self._q(TABLES[frame]),
            sql.SQL(", ").join(sql.Identifier(n) for n in names),
            sql.SQL(", ").join(sql.Placeholder() for _ in names),
        )
        params = []
        for row in rows:
            values = [] if publication_id is None else [publication_id]
            values += [_db_value(kind, getattr(row, name)) for name, kind in cls.SPEC]
            values.append(row.row_sha256())
            params.append(values)
        if params:
            with self.conn.cursor() as cur:
                cur.executemany(statement, params)

    def _select(self, frame: str, where: Any, params: Iterable[Any], *, source: Any = None) -> list[tuple[Any, str]]:
        """Decoded, hash-checked rows of ``frame`` from its table (or ``source``, a FROM item)."""
        from psycopg import sql
        from psycopg.rows import dict_row

        cls = FRAME_TYPES[frame]
        names = [n for n, _ in cls.SPEC] + ["row_sha256"]
        statement = sql.SQL("SELECT {} FROM {} WHERE {}").format(
            sql.SQL(", ").join(sql.Identifier(n) for n in names),
            self._q(TABLES[frame]) if source is None else source, where,
        )
        with self.conn.cursor(row_factory=dict_row) as cur:
            records = cur.execute(statement, list(params)).fetchall()
        out = []
        for record in records:
            stored = record.pop("row_sha256")
            row = cls.from_record(record)
            if row.row_sha256() != stored:
                raise PublicationError("bond_credit_db:row_hash_mismatch", f"{frame}:{row.key()}")
            out.append((row, stored))
        return out

    def _load_manifest_row(self, publication_id: uuid.UUID) -> dict[str, Any] | None:
        from psycopg import sql
        from psycopg.rows import dict_row

        names = [n for n, _ in MANIFEST_SPEC] + [
            "publication_version", "lifecycle_state", "prepared_at", "validated_at",
        ]
        statement = sql.SQL("SELECT {} FROM {} WHERE publication_id = %s").format(
            sql.SQL(", ").join(sql.Identifier(n) for n in names),
            self._q("bond_credit_publications"),
        )
        with self.conn.cursor(row_factory=dict_row) as cur:
            return cur.execute(statement, [publication_id]).fetchone()

    def _read_bundle(self, row: Mapping[str, Any], *, reader_allow_shadow: bool | None = None) -> CreditBundle:
        """Bundle of the manifest ``row`` from the base tables (writer/auditor), or, when
        ``reader_allow_shadow`` is not ``None``, through the guarded ``bond_credit_read_<frame>``
        functions (serving reader: revoked/unvalidated/shadow refused per call)."""
        from psycopg import sql

        manifest = BundleManifest.from_record({n: row[n] for n, _ in MANIFEST_SPEC})
        pid = manifest["publication_id"]
        frames: dict[str, list[Any]] = {}
        if reader_allow_shadow is not None:
            for frame in (*c.INPUT_FRAMES, *c.OUTPUT_FRAMES):
                source = sql.SQL("{}(%s, %s)").format(self._fn(f"bond_credit_read_{frame}"))
                frames[frame] = [r for r, _ in self._select(
                    frame, sql.SQL("TRUE"), [pid, reader_allow_shadow], source=source)]
        else:
            by_pub = sql.SQL("publication_id = %s")
            sources = [r for r, _ in self._select("publication_sources", by_pub, [pid])]
            package_ids = [s.package_id for s in sources]  # type: ignore[attr-defined]
            for frame in c.INPUT_FRAMES:
                frames[frame] = [r for r, _ in self._select(frame, sql.SQL("package_id = ANY(%s)"), [package_ids])]
            for frame in c.OUTPUT_FRAMES:
                frames[frame] = sources if frame == "publication_sources" else [
                    r for r, _ in self._select(frame, by_pub, [pid])]
        grid = sorted(
            {(r.cusip_id, r.month) for r in frames["ratings"] if r.view_kind == "public_pit"},
            key=lambda item: f"{item[0]}|{item[1].isoformat()}",
        )
        ordered = {name: tuple(sorted(rows, key=lambda r: r.key())) for name, rows in frames.items()}
        return CreditBundle(manifest, tuple(grid), ordered)

    def _state_from(self, row: Mapping[str, Any], revoked: bool) -> PublicationState:
        return PublicationState(
            publication_id=row["publication_id"],
            publication_version=row["publication_version"],
            lifecycle_state=row["lifecycle_state"],
            quality_state=row["quality_state"],
            build_scope=row["build_scope"],
            target_month=row["target_month"],
            knowledge_cutoff=row["knowledge_cutoff"].astimezone(dt.timezone.utc),
            revoked=revoked,
        )

    def _lock_publication(self, publication_id: uuid.UUID) -> None:
        """Serialize prepare/validate of one publication id (no UPDATE privilege needed)."""
        self.conn.execute(
            "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(%s, 0))",
            [f"{c.PRODUCT}|publication|{publication_id}"],
        )

    def _verify_package_inventories(self, bundle: CreditBundle) -> None:
        """The persisted row set of every consumed package must equal the bundle's."""
        expected = {s.package_id: s.row_inventory_digest for s in bundle.frames["publication_sources"]}  # type: ignore[attr-defined]
        from psycopg import sql

        owned = sql.SQL(" UNION ALL ").join(
            sql.SQL("SELECT t.{} FROM {} t WHERE t.package_id = v.package_id").format(
                sql.Identifier(_OWNED_ID[frame]), self._q(TABLES[frame]))
            for frame in c.OWNED_INPUT_FRAMES
        )
        query = sql.SQL("SELECT v.package_id, ARRAY({}) FROM unnest(%s::uuid[]) AS v(package_id)").format(owned)
        for package_id, owned in self.conn.execute(query, [list(expected)]).fetchall():
            if c.id_inventory_digest(owned) != expected[package_id]:
                raise PublicationCollision("bond_credit_prepare:package_inventory_collision", str(package_id))

    def _is_revoked(self, publication_id: uuid.UUID) -> bool:
        from psycopg import sql

        statement = sql.SQL("SELECT {}(%s)").format(self._fn("bond_credit_is_revoked"))
        return bool(self.conn.execute(statement, [publication_id]).fetchone()[0])

    # -- lifecycle ----------------------------------------------------------
    def prepare(self, bundle: CreditBundle) -> str:
        def action() -> str:
            pid = bundle.publication_id
            self._lock_publication(pid)
            existing = self._load_manifest_row(pid)
            if existing is not None:
                persisted = self._read_bundle(existing)
                if persisted_identity(persisted) != persisted_identity(bundle):
                    raise PublicationCollision("bond_credit_prepare:publication_collision", str(pid))
                return "replayed"
            from psycopg import sql

            package_ids = [row.package_id for row in bundle.frames["source_packages"]]  # type: ignore[attr-defined]
            sealed = {
                row[0] for row in self.conn.execute(
                    sql.SQL("SELECT DISTINCT package_id FROM {} WHERE package_id = ANY(%s)").format(
                        self._q("bond_credit_publication_sources")),
                    [package_ids],
                ).fetchall()
            }
            for frame in c.INPUT_FRAMES:
                cls = FRAME_TYPES[frame]
                key = cls.KEY[0]
                rows = _topological(bundle.frames[frame], key, _SUPERSEDES[frame])
                ids = [getattr(r, key) for r in rows]
                stored = {
                    getattr(r, key): h
                    for r, h in self._select(frame, sql.SQL("{} = ANY(%s)").format(sql.Identifier(key)), [ids])
                }
                missing = [r for r in rows if getattr(r, key) not in stored]
                for row in rows:
                    rid = getattr(row, key)
                    if rid in stored and stored[rid] != row.row_sha256():
                        raise PublicationCollision("bond_credit_prepare:ledger_row_collision", f"{frame}:{rid}")
                for row in missing:
                    owner = row.package_id if frame != "source_packages" else None
                    if owner in sealed:
                        raise PublicationCollision("bond_credit_prepare:package_inventory_collision", str(owner))
                # Only absent rows are inserted; sealed packages also refuse new rows in SQL.
                self._insert(frame, missing)
            self._verify_package_inventories(bundle)

            record = bundle.manifest.values
            names = [n for n, _ in MANIFEST_SPEC]
            self.conn.execute(
                sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    self._q("bond_credit_publications"),
                    sql.SQL(", ").join(sql.Identifier(n) for n in names),
                    sql.SQL(", ").join(sql.Placeholder() for _ in names),
                ),
                [_db_value(kind, record[n]) for n, kind in MANIFEST_SPEC],
            )
            for frame in c.OUTPUT_FRAMES:
                self._insert(frame, bundle.frames[frame], publication_id=pid)
            return "inserted"

        return self._run(action)

    def state(self, publication_id: uuid.UUID) -> PublicationState:
        def action() -> PublicationState:
            row = self._load_manifest_row(publication_id)
            if row is None:
                raise PublicationError("bond_credit_state:unknown_publication", str(publication_id))
            return self._state_from(row, self._is_revoked(publication_id))

        return self._run(action)

    def validate(self, publication_id: uuid.UUID) -> PublicationState:
        from psycopg import sql

        def action() -> PublicationState:
            self._lock_publication(publication_id)
            row = self._load_manifest_row(publication_id)
            if row is None:
                raise PublicationError("bond_credit_validate:unknown_publication", str(publication_id))
            if row["lifecycle_state"] == "validated":
                return self._state_from(row, self._is_revoked(publication_id))
            bundle = self._read_bundle(row)
            try:
                check_bundle(bundle)
            except ContractError as exc:
                reason, _, detail = str(exc).partition(":")
                raise PublicationError(f"bond_credit_validate:{reason}", detail) from exc
            self.conn.execute(
                sql.SQL("SELECT {}(%s)").format(self._fn("bond_credit_validate")), [publication_id]
            )
            after = self._load_manifest_row(publication_id)
            assert after is not None
            return self._state_from(after, False)

        return self._run(action)

    def promote(self, publication_id: uuid.UUID, expected_pointer: uuid.UUID | None) -> uuid.UUID:
        from psycopg import sql

        def action() -> uuid.UUID:
            statement = sql.SQL("SELECT {}(%s, %s)").format(self._fn("bond_credit_promote"))
            return self.conn.execute(statement, [publication_id, expected_pointer]).fetchone()[0]

        return self._run(action)

    def revoke(self, publication_id: uuid.UUID, reason: str, evidence_digest: str) -> None:
        from psycopg import sql

        def action() -> None:
            statement = sql.SQL("SELECT {}(%s, %s, %s)").format(self._fn("bond_credit_revoke"))
            self.conn.execute(statement, [publication_id, reason, evidence_digest])

        self._run(action)

    def current(
        self, *, expected_target_month: dt.date | None = None,
        expected_panel_publication_id: uuid.UUID | None = None,
    ) -> BundleManifest:
        from psycopg import sql
        from psycopg.rows import dict_row

        def action() -> BundleManifest:
            statement = sql.SQL("SELECT * FROM {}(%s, %s)").format(self._fn("bond_credit_current_publication"))
            with self.conn.cursor(row_factory=dict_row) as cur:
                row = cur.execute(statement, [expected_target_month, expected_panel_publication_id]).fetchone()
            return BundleManifest.from_record({n: row[n] for n, _ in MANIFEST_SPEC})

        return self._run(action)

    def read(self, publication_id: uuid.UUID, *, allow_shadow: bool = False) -> CreditBundle:
        from psycopg import sql
        from psycopg.rows import dict_row

        def action() -> CreditBundle:
            # Serving read: manifest and every frame through the guarded SECURITY DEFINER
            # functions (the reader role holds no table privilege).
            statement = sql.SQL("SELECT * FROM {}(%s, %s)").format(self._fn("bond_credit_read_publication"))
            with self.conn.cursor(row_factory=dict_row) as cur:
                row = cur.execute(statement, [publication_id, allow_shadow]).fetchone()
            return self._read_bundle(row, reader_allow_shadow=allow_shadow)

        return self._run(action)
