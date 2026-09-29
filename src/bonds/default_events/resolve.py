"""Pure temporal/adjudication resolution for public bond default events (W2a).

``resolve_links``, ``resolve_episodes``, ``build_followup`` and ``build_coverage`` turn
frozen inputs into W0 contract rows. Phase-1 episode construction is intentionally limited
to closed non-consensus documentary events and explicit human state claims; proposal-backed
consensus and exchange acceptance remain typed refusals until their persisted adapters exist.

Everything here is pure and deterministic: no database, no network and no wall-clock
reads; the knowledge cutoff ``K`` is always passed in. Adjudications are *inputs*
(appended manual/signed records); nothing here creates, approves or edits an
adjudication, and parser/policy proposals never self-approve.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from typing import TYPE_CHECKING, Any

from . import contracts as c
from . import publication
from .contracts import (
    ADMITTING_STATUSES,
    POLICY_DIGEST,
    Adjudication,
    ContractError,
    CreditObservation,
    DefaultEpisode,
    EventLink,
    SourcePackage,
)

if TYPE_CHECKING:  # pragma: no cover - typing only (duck-typed at runtime)
    from .nport import StateProposal

__all__ = [
    "CANDIDATE_IS_NOT_A_DECISION",
    "CENSORING_REASONS",
    "LINK_BASES",
    "SCOPE_DECISION_STATUSES",
    "STANDALONE_DECISION_NOT_PERSISTABLE",
    "AsOf",
    "Censoring",
    "ContinuityEvidence",
    "CreditMarker",
    "EpisodeClaim",
    "EpisodeIssue",
    "EpisodeResolution",
    "EventFacts",
    "FollowUpResolution",
    "LinkIssue",
    "LinkRequest",
    "LinkResolution",
    "ResolveError",
    "ScopeOutcome",
    "build_coverage",
    "build_followup",
    "interval_covered",
    "interval_outcome",
    "outcome_interval",
    "resolve_episodes",
    "resolve_links",
    "scope_decision",
    "w0_event_closure_persistable",
]


class ResolveError(ValueError):
    """Fail-loud resolution error; ``issues`` carries every typed reason found."""

    def __init__(self, code: str, issues: Sequence[Any] = ()) -> None:
        self.code = code
        self.issues = tuple(issues)
        detail = "; ".join(str(i) for i in self.issues[:20])
        super().__init__(f"{code}: {detail}" if detail else code)


# ---------------------------------------------------------------------------
# Shared as-of-K helpers
# ---------------------------------------------------------------------------
KNOWLEDGE_MODES = ("current_run", "historical_reconstruction")


def _utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        raise ResolveError("knowledge_cutoff:timezone_required")
    return value.astimezone(dt.timezone.utc)


def _check_mode(mode: str) -> None:
    if mode not in KNOWLEDGE_MODES:
        raise ResolveError(f"knowledge_mode:invalid:{mode}")


def stale_observations(observations: Mapping[uuid.UUID, CreditObservation], k: dt.datetime) -> set[uuid.UUID]:
    """Observations unusable as evidence at ``k``: superseded by a revision public by ``k``,
    or retractions. Forks among revisions known by ``k`` are malformed inputs."""
    revised: set[uuid.UUID] = set()
    for o in observations.values():
        parent = o.supersedes_observation_id
        if parent is None or o.public_available_at > k:
            continue
        if parent in revised:
            raise ResolveError("observation_revision_fork", (str(parent),))
        revised.add(parent)
    return revised | {oid for oid, o in observations.items() if o.revision_kind == "retraction"}


def usable_observation(o: CreditObservation, k: dt.datetime, mode: str, stale: set[uuid.UUID]) -> str | None:
    """``None`` when ``o`` is usable evidence at ``k``, else the typed reason."""
    if o.public_available_at > k:
        return "observation_public_after_cutoff"
    if mode == "current_run" and o.first_seen_at > k:
        return "observation_not_ingested_by_cutoff"
    if o.observation_id in stale:
        return "observation_superseded_or_retracted"
    return None


def adjudications_as_of(adjudications: Iterable[Adjudication], k: dt.datetime, mode: str) -> dict[uuid.UUID, Adjudication]:
    """Adjudication records usable at ``k`` (current_run: ``adjudicated_at <= k``).

    A historical reconstruction keeps present-day review records (recorded separately, never
    claimed as historic possession)."""
    return {
        a.adjudication_id: a
        for a in adjudications
        if mode == "historical_reconstruction" or a.adjudicated_at <= k
    }


def effective_adjudications(adjs: Mapping[uuid.UUID, Adjudication]) -> dict[uuid.UUID, Adjudication]:
    """Subject -> unique unsuperseded record (policy ``adjudication.effective_rule``)."""
    superseded = {a.supersedes_adjudication_id for a in adjs.values() if a.supersedes_adjudication_id}
    effective: dict[uuid.UUID, Adjudication] = {}
    for a in sorted(adjs.values(), key=lambda x: str(x.adjudication_id)):
        if a.adjudication_id in superseded:
            continue
        if a.subject_id in effective:
            raise ResolveError("adjudication_chain_fork", (str(a.subject_id),))
        parent = a.supersedes_adjudication_id
        if parent is not None and (parent not in adjs or adjs[parent].subject_id != a.subject_id):
            raise ResolveError("adjudication_supersedes_outside_inventory", (str(a.adjudication_id),))
        effective[a.subject_id] = a
    return effective


def _chain_head(start: Adjudication, adjs: Mapping[uuid.UUID, Adjudication]) -> Adjudication:
    """Latest record superseding ``start`` (following the append-only chain)."""
    children: dict[uuid.UUID, Adjudication] = {
        a.supersedes_adjudication_id: a for a in adjs.values() if a.supersedes_adjudication_id is not None
    }
    head, seen = start, {start.adjudication_id}
    while head.adjudication_id in children:
        head = children[head.adjudication_id]
        if head.adjudication_id in seen:
            raise ResolveError("adjudication_supersession_cycle", (str(start.adjudication_id),))
        seen.add(head.adjudication_id)
    return head


# ---------------------------------------------------------------------------
# Links
# ---------------------------------------------------------------------------
#: Explicit identity bases that can admit a CUSIP-level link.
LINK_BASES = ("adjudicated_issue_scope", "document_stated_cusip", "nport_cusip_identity")
_DOCUMENT_SCOPES = frozenset({"issue", "exchange_old", "exchange_new"})


@dataclass(frozen=True)
class LinkRequest:
    """Proposed observation -> CUSIP9 link (from a parser, linker or reviewer batch).

    ``basis`` names the identity proof: ``nport_cusip_identity`` (the N-PORT holding's own
    validated CUSIP), ``document_stated_cusip`` (the passage/exhibit states the CUSIP),
    ``adjudicated_issue_scope`` (issuer-level evidence whose affected obligation was decided
    by a human scope adjudication citing this exact link). Any other basis (CUSIP6, CIK,
    LEI, name matches) is suspected and quarantined. ``identity_known_at`` is when the
    identity evidence (indenture, CUSIP master, exhibit) was itself known.
    """

    observation_id: uuid.UUID
    cusip9: str
    security_id: uuid.UUID
    obligor_id: str
    affected_scope: str
    basis: str
    identity_evidence_refs: tuple[str, ...]
    identity_known_at: dt.datetime
    valid_from: dt.date
    valid_to: dt.date | None = None
    rationale: str = "link request"


@dataclass(frozen=True)
class LinkIssue:
    observation_id: uuid.UUID
    cusip9: str
    reason: str
    link_id: uuid.UUID | None = None
    #: For ``standalone_decision_not_persistable``: the admitted revision the episode's accepting
    #: revision must cite to make the scope decision persistable in W0 v1.
    prospective_link_id: uuid.UUID | None = None

    def __str__(self) -> str:
        return f"{self.reason}:{self.cusip9}:{self.observation_id}"


@dataclass(frozen=True)
class LinkResolution:
    #: every emitted link revision (admitted, quarantined, rejected), deterministic order.
    links: tuple[EventLink, ...]
    issues: tuple[LinkIssue, ...]
    stats: Mapping[str, int] = field(default_factory=dict)

    @property
    def admitted(self) -> tuple[EventLink, ...]:
        """Admitted links not superseded by another emitted revision."""
        superseded = {x.supersedes_link_id for x in self.links if x.supersedes_link_id is not None}
        return tuple(x for x in self.links if x.status == "admitted" and x.link_id not in superseded)


def _make_link(
    package: SourcePackage, req: LinkRequest, *, status: str, known_at: dt.datetime, rationale: str,
    supersedes: uuid.UUID | None = None,
) -> EventLink:
    refs = c.sorted_texts(req.identity_evidence_refs)
    return EventLink.create(
        package_id=package.package_id,
        observation_id=req.observation_id,
        security_id=req.security_id,
        cusip9=req.cusip9,
        obligor_id=req.obligor_id,
        affected_scope=req.affected_scope,
        valid_from=req.valid_from,
        valid_to=req.valid_to,
        link_known_at=known_at,
        identity_evidence_refs=refs,
        identity_evidence_digest=c.digest_of(list(refs)),
        status=status,
        rationale=rationale,
        supersedes_link_id=supersedes,
    )


def _direct_status(req: LinkRequest, o: CreditObservation) -> tuple[str, str]:
    """(status, reason) for the non-adjudicated bases."""
    if req.basis not in LINK_BASES:
        return "quarantined", f"suspected_identity_basis:{req.basis}"
    if o.cusip9 is not None and o.cusip9 != req.cusip9:
        return "rejected", "cusip_contradicts_observation"
    if req.basis == "nport_cusip_identity":
        if o.observation_kind != "nport_holding":
            return "rejected", "nport_identity_requires_nport_holding"
        if o.cusip9 is None:
            return "quarantined", "nport_holding_without_validated_cusip"
        if req.affected_scope != "issue":
            return "quarantined", "nport_identity_scope_must_be_issue"
        return "admitted", "nport_cusip_identity"
    # document_stated_cusip
    if not o.observation_kind.endswith("_passage"):
        return "rejected", "document_basis_requires_passage"
    if o.cusip9 is None:
        return "quarantined", "issuer_level_evidence_without_adjudicated_scope"
    if req.affected_scope not in _DOCUMENT_SCOPES:
        return "quarantined", "issuer_scope_requires_adjudication"
    return "admitted", "document_stated_cusip"


#: A scope decision must carry an admitting status (a ``candidate`` decides nothing).
SCOPE_DECISION_STATUSES = ADMITTING_STATUSES


@dataclass(frozen=True)
class AsOf:
    """Inputs usable at cutoff ``k`` (shared by links, episodes and follow-up)."""

    k: dt.datetime
    mode: str
    obs: Mapping[uuid.UUID, CreditObservation]
    stale_obs: frozenset[uuid.UUID]
    links: Mapping[uuid.UUID, EventLink]
    stale_links: frozenset[uuid.UUID]
    adjs: Mapping[uuid.UUID, Adjudication]

    @classmethod
    def build(cls, k: dt.datetime, mode: str, observations: Iterable[CreditObservation],
              links: Iterable[EventLink], adjudications: Iterable[Adjudication]) -> AsOf:
        obs = {o.observation_id: o for o in observations}
        link_map = {x.link_id: x for x in links}
        return cls(k, mode, obs, frozenset(stale_observations(obs, k)), link_map,
                   frozenset(stale_links(link_map, k)), adjudications_as_of(adjudications, k, mode))

    def observation_problem(self, oid: uuid.UUID) -> str | None:
        o = self.obs.get(oid)
        if o is None:
            return "observation_outside_inventory"
        return usable_observation(o, self.k, self.mode, set(self.stale_obs))

    def link_problem(self, lid: uuid.UUID) -> str | None:
        x = self.links.get(lid)
        if x is None:
            return "link_outside_inventory"
        if x.status != "admitted":
            return f"link_{x.status}"
        if lid in self.stale_links:
            return "link_superseded"
        if x.link_known_at > self.k:
            return "link_known_after_cutoff"
        return self.observation_problem(x.observation_id)

    def effective_head(self, a: Adjudication) -> Adjudication:
        return _chain_head(a, self.adjs)

    def dependency_times(
        self, a: Adjudication, *, exempt_links: Iterable[uuid.UUID] = (),
    ) -> tuple[list[dt.datetime], str | None]:
        """Public/known times of everything ``a`` cites, or the first typed problem.

        Every cited observation must be usable at K (public, ingested in current_run, not
        superseded/retracted); every cited link admitted, current and known by K (``exempt_links``
        are the proposal/revision being decided). The review time itself is not included."""
        exempt = set(exempt_links)
        times: list[dt.datetime] = []
        for oid in a.evidence_observation_ids:
            problem = self.observation_problem(oid)
            if problem:
                return [], problem
            times.append(self.obs[oid].public_available_at)
        for lid in a.link_ids:
            if lid in exempt:
                continue
            problem = self.link_problem(lid)
            if problem:
                return [], problem
            times.append(self.links[lid].link_known_at)
        return times, None

    def review_times(self, *records: Adjudication) -> list[dt.datetime]:
        """Review completion counts toward knowledge only in ``current_run``; a historical
        reconstruction keeps present-day review visible in the adjudication rows only."""
        return [r.adjudicated_at for r in records] if self.mode == "current_run" else []


#: Typed fail-closed reasons of the interim v1 decision rule (W0 amendment 1, section 4.6).
CANDIDATE_IS_NOT_A_DECISION = "candidate_is_not_a_decision"
STANDALONE_DECISION_NOT_PERSISTABLE = "standalone_decision_not_persistable"


@dataclass(frozen=True)
class ScopeOutcome:
    """Result of :func:`scope_decision`.

    ``prospective_link_id`` is the admitted revision a valid but still standalone decision would
    produce: the episode's accepting revision must cite it (and not the quarantined proposal) for
    the decision to become persistable in W0 v1."""

    decision: Adjudication | None
    known_at: dt.datetime | None
    reason: str
    prospective_link_id: uuid.UUID | None = None


def scope_decision(proposal: EventLink, asof: AsOf, *, revision_id_for: Any = None) -> ScopeOutcome:
    """Persistable human scope decision for exactly ``proposal`` (never propagated to other issues).

    Interim v1 rule (W0 amendment 1, section 4.6): a ``candidate`` record is never a decision
    (``candidate_is_not_a_decision``). A scope decision counts only as an admitting human
    ``issue_episode`` record inside an episode's own adjudication chain: it cites the quarantined
    proposal and its document and is superseded by the chain's effective admitting head, which
    cites the admitted revision (``revision_id_for(decision, known_at)``) and the document but not
    the quarantined proposal. W0 v1 cannot persist any other positive decision (its own head, or a
    head that does not cite the revision): ``standalone_decision_not_persistable``, fail closed.
    Every observation and link relied on by the decision and its head must be usable at K;
    ``known_at`` is the max over the decision's public relied set plus proposal identity time.
    Current-run review eligibility remains a separate ``adjudicated_at <= K`` requirement;
    reconstruction can retain present-day review without backdating public link knowledge.
    """
    citing = sorted(
        (a for a in asof.adjs.values()
         if proposal.link_id in a.link_ids and proposal.observation_id in a.evidence_observation_ids),
        key=lambda a: (a.adjudicated_at, str(a.adjudication_id)),
    )
    reason, prospective = "scope_adjudication_missing", None
    for a in citing:
        if asof.mode == "current_run" and a.adjudicated_at > asof.k:
            reason = "scope_adjudication_missing"
            continue
        if a.status == "candidate":
            reason = CANDIDATE_IS_NOT_A_DECISION
            continue
        if a.reviewer_role != "human_reviewer":
            reason = "scope_adjudication_not_human"
            continue
        if a.policy_digest != POLICY_DIGEST:
            reason = "scope_adjudication_policy_mismatch"
            continue
        if a.status not in SCOPE_DECISION_STATUSES:
            reason = f"scope_adjudication_status_{a.status}"
            continue
        head = asof.effective_head(a)
        if asof.mode == "current_run" and head.adjudicated_at > asof.k:
            reason = "scope_adjudication_not_effective"
            continue
        if head.status not in SCOPE_DECISION_STATUSES or head.policy_digest != POLICY_DIGEST:
            reason = "scope_adjudication_not_effective"
            continue
        times, problem = asof.dependency_times(a, exempt_links=(proposal.link_id,))
        if problem:
            reason = f"scope_dependency_unusable:{problem}"
            continue
        known = max([proposal.link_known_at, *times])
        revision = revision_id_for(a, known) if revision_id_for else None
        in_chain = (
            a.subject_kind == "issue_episode" and head is not a and head.subject_kind == "issue_episode"
            and revision is not None and revision in head.link_ids and proposal.link_id not in head.link_ids
            and proposal.observation_id in head.evidence_observation_ids
        )
        if not in_chain:
            reason, prospective = STANDALONE_DECISION_NOT_PERSISTABLE, revision
            continue
        # The head (the episode's accepting record citing the admitted revision) must have usable
        # dependencies; its own times belong to what it decides, not to the link.
        _head_times, problem = asof.dependency_times(head, exempt_links=(proposal.link_id, revision))
        if problem:
            reason = f"scope_dependency_unusable:{problem}"
            continue
        if known > asof.k:
            reason = "link_known_after_cutoff"
            continue
        return ScopeOutcome(a, known, "adjudicated_issue_scope", revision)
    return ScopeOutcome(None, None, reason, prospective)


def resolve_links(
    requests: Iterable[LinkRequest],
    *,
    observations: Iterable[CreditObservation],
    adjudications: Iterable[Adjudication],
    link_package: SourcePackage,
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str = "current_run",
    links: Iterable[EventLink] = (),
    strict: bool = False,
) -> LinkResolution:
    """Resolve link requests into immutable :class:`EventLink` revisions as of ``K``.

    * Admission only from explicit CUSIP-level proof (see :class:`LinkRequest`).
    * Issuer-level evidence (passage without a stated CUSIP9) never propagates to the issues of
      a CUSIP6/issuer: its proposal link is emitted ``quarantined`` and only a persistable human
      scope decision citing that exact link id, inside an episode's own adjudication chain
      (:func:`scope_decision`), yields an ``admitted`` revision superseding it. Candidate records
      and standalone decisions fail closed (``candidate_is_not_a_decision``,
      ``standalone_decision_not_persistable`` with the ``prospective_link_id`` to cite).
    * ``link_known_at`` = max over the full relied set: observation public time, identity
      evidence time, every observation/link the scope decision cites (all usable at K), and in
      current_run the review time; a historical reconstruction uses contemporaneous public
      availability only (present-day review stays visible in the adjudication rows).
    * ``links`` is the prior link inventory the decisions may cite as support.
    * Observations unusable at K and links not known by K emit no row (typed issue instead).
    * Quarantine is ordinary output, so issues are returned; ``strict`` raises
      :class:`ResolveError` when a present scope decision fails (candidate, standalone, negative,
      non-human, unusable dependency or post-K).
    """
    k = _utc(knowledge_cutoff)
    _check_mode(knowledge_mode)
    if link_package.source_family != "link_batch":
        raise ResolveError("link_package:family_must_be_link_batch")
    asof = AsOf.build(k, knowledge_mode, observations, links, adjudications)
    obs = asof.obs
    stale = set(asof.stale_obs)
    links_out: dict[uuid.UUID, EventLink] = {}
    issues: list[LinkIssue] = []
    stats: Counter[str] = Counter()

    def issue(req: LinkRequest, reason: str, link_id: uuid.UUID | None = None,
              prospective: uuid.UUID | None = None) -> None:
        issues.append(LinkIssue(req.observation_id, req.cusip9, reason, link_id, prospective))
        stats[f"issue:{reason.split(':')[0]}"] += 1

    for req in requests:
        stats["requests"] += 1
        if not c.is_valid_cusip9(req.cusip9):
            issue(req, "cusip9_invalid")
            continue
        o = obs.get(req.observation_id)
        if o is None:
            issue(req, "observation_outside_inventory")
            continue
        unusable = usable_observation(o, k, knowledge_mode, stale)
        if unusable:
            issue(req, unusable)
            continue
        base_known = max(o.public_available_at, _utc(req.identity_known_at))
        if base_known > k:
            issue(req, "link_known_after_cutoff")
            continue
        if req.basis == "adjudicated_issue_scope":
            if o.cusip9 is not None and o.cusip9 != req.cusip9:
                emitted = _make_link(link_package, req, status="rejected", known_at=base_known,
                                     rationale="cusip_contradicts_observation")
                links_out[emitted.link_id] = emitted
                issue(req, "cusip_contradicts_observation", emitted.link_id)
                continue
            proposal = _make_link(link_package, req, status="quarantined", known_at=base_known,
                                  rationale=f"awaiting_adjudicated_scope:{req.rationale}")
            links_out[proposal.link_id] = proposal

            def revision(a: Adjudication, known: dt.datetime, req: LinkRequest = req,
                         proposal: EventLink = proposal) -> EventLink:
                return _make_link(link_package, req, status="admitted", known_at=known, supersedes=proposal.link_id,
                                  rationale=f"adjudicated_issue_scope:{a.adjudication_id}")

            outcome = scope_decision(proposal, asof, revision_id_for=lambda a, t, rev=revision: rev(a, t).link_id)
            if outcome.decision is None or outcome.known_at is None:
                issue(req, outcome.reason, proposal.link_id, outcome.prospective_link_id)
                continue
            admitted = revision(outcome.decision, outcome.known_at)
            links_out[admitted.link_id] = admitted
            stats["admitted"] += 1
            continue
        status, reason = _direct_status(req, o)
        emitted = _make_link(link_package, req, status=status, known_at=base_known, rationale=reason)
        links_out[emitted.link_id] = emitted
        stats[status] += 1
        if status != "admitted":
            issue(req, reason, emitted.link_id)
    ordered = tuple(sorted(links_out.values(), key=lambda x: str(x.link_id)))
    issues.sort(key=lambda i: (i.cusip9, str(i.observation_id), i.reason, str(i.link_id)))
    failed = [i for i in issues if _is_decision_failure(i.reason)]
    if strict and failed:
        raise ResolveError("link_scope_decision_failed", failed)
    return LinkResolution(ordered, tuple(issues), dict(sorted(stats.items())))


def _is_decision_failure(reason: str) -> bool:
    """A scope decision was present but cannot admit the link."""
    return reason in (CANDIDATE_IS_NOT_A_DECISION, STANDALONE_DECISION_NOT_PERSISTABLE) or (
        reason.startswith("scope_") and reason != "scope_adjudication_missing"
    )


def stale_links(links: Mapping[uuid.UUID, EventLink], k: dt.datetime) -> set[uuid.UUID]:
    """Links superseded by a revision known by ``k`` (forks are malformed inputs)."""
    revised: set[uuid.UUID] = set()
    for x in links.values():
        parent = x.supersedes_link_id
        if parent is None or x.link_known_at > k:
            continue
        if parent in revised:
            raise ResolveError("link_revision_fork", (str(parent),))
        revised.add(parent)
    return revised


# ---------------------------------------------------------------------------
# Episodes
# ---------------------------------------------------------------------------
EVENT_TYPES = ("agency_issue_default", "bankruptcy", "distressed_exchange", "payment_default")
_ADMISSIBLE_PROCEEDINGS = frozenset({"bankruptcy_petition", "receivership"})
_KIND_FLAGS = {
    "edgar_passage": "edgar_document",
    "issuer_document_passage": "issuer_document",
    "court_document_passage": "court_document",
}


@dataclass(frozen=True)
class EventFacts:
    """Human-reviewed typed facts an accepted event must satisfy (policy ``event_types``).

    Bankruptcy (Item 1.03): ``proceeding`` ``bankruptcy_petition``/``receivership`` with a
    verified debtor and affected obligation; ``plan_confirmation`` is corroboration,
    ``uncertain_petition`` a candidate. Payment default: missed payment and grace expired
    uncured; ``cured_within_grace`` or a ``technical_covenant`` acceleration without a
    missed payment (Item 2.04) is nonqualifying. Exchange: ``completed`` coercive impairment
    with old/new issue links; ``proposed`` stays candidate. Arrears/legal deferral (C.9.d)
    or PIK (C.9.e) alone never qualify.
    """

    proceeding: str | None = None
    debtor_verified: bool = False
    affected_obligation_verified: bool = False
    missed_payment: bool = False
    grace_period_expired_uncured: bool = False
    cured_within_grace: bool = False
    acceleration: str | None = None
    exchange_stage: str | None = None
    coercive_impairment: bool = False
    arrears_or_legal_deferral_only: bool = False
    pik_only: bool = False


@dataclass(frozen=True)
class EpisodeClaim:
    """Typed facts of one reviewed episode (``episode_id`` = adjudication ``subject_id``).

    Onset is ``onset_date`` (exact) or ``(onset_lower_exclusive, onset_upper_inclusive]``; a
    lower bound needs ``onset_lower_evidence_ids`` cited by the accepting adjudication.
    ``resolution_refs`` (observations or adjudications) must include evidence beyond N-PORT
    (an N after Y is never cure evidence). ``corroborating_types`` merges other reviewed event
    types of the *same* unresolved episode as corroboration flags.
    """

    episode_id: uuid.UUID
    primary_type: str
    onset_upper_inclusive: dt.date | None = None
    onset_lower_exclusive: dt.date | None = None
    onset_date: dt.date | None = None
    onset_lower_evidence_ids: tuple[uuid.UUID, ...] = ()
    recognition_date: dt.date | None = None
    facts: EventFacts = field(default_factory=EventFacts)
    corroborating_types: tuple[str, ...] = ()
    resolution_date: dt.date | None = None
    resolution_refs: tuple[uuid.UUID, ...] = ()
    issuer_episode_id: uuid.UUID | None = None

    def bounds(self) -> tuple[dt.date | None, dt.date]:
        if self.onset_date is not None:
            if self.onset_upper_inclusive not in (None, self.onset_date) or self.onset_lower_exclusive not in (
                None, self.onset_date - dt.timedelta(days=1),
            ):
                raise ResolveError("claim:onset_date_bounds_mismatch", (str(self.episode_id),))
            return self.onset_date - dt.timedelta(days=1), self.onset_date
        if self.onset_upper_inclusive is None:
            raise ResolveError("claim:onset_upper_required", (str(self.episode_id),))
        return self.onset_lower_exclusive, self.onset_upper_inclusive


@dataclass(frozen=True)
class EpisodeIssue:
    subject_id: uuid.UUID
    reason: str

    def __str__(self) -> str:
        return f"{self.reason}:{self.subject_id}"


@dataclass(frozen=True)
class CreditMarker:
    """Never-default N-PORT marker (C.9.d arrears/legal deferral, C.9.e PIK)."""

    cusip9: str
    report_date: dt.date
    kind: str
    observation_id: uuid.UUID


@dataclass(frozen=True)
class EpisodeResolution:
    episodes: tuple[DefaultEpisode, ...]
    issues: tuple[EpisodeIssue, ...]
    markers: tuple[CreditMarker, ...]
    #: exchange-new CUSIP9 -> alias spell id of the distressed-exchange episode it absorbs. Always
    #: empty while W0 cannot persist the exchange pairing (exchanges fail closed).
    alias_spells: Mapping[str, uuid.UUID]
    #: adjudication records usable at K (the bundle's adjudication inventory).
    adjudication_inventory: tuple[Adjudication, ...]
    stats: Mapping[str, int] = field(default_factory=dict)


def dates_onset_lower(o: CreditObservation, cusip9: str, lower: dt.date) -> bool:
    """Explicit provenance for onset lower bound ``lower`` (mirrors ``check_bundle``): a prior
    credible N-PORT N of the same issue at ``lower`` or independent dating evidence."""
    if o.observation_kind == "nport_holding":
        return o.nport_is_default == "N" and o.report_date == lower and o.cusip9 == cusip9
    if o.date_precision == "day":
        return o.effective_date is not None and o.effective_date - dt.timedelta(days=1) == lower
    if o.date_precision == "interval":
        return o.effective_lower_exclusive == lower
    return False


def w0_event_closure_persistable(cusip9: str, security_id: uuid.UUID, links: Iterable[EventLink]) -> bool:
    """Whether W0 can persist ``links`` in one event's closure.

    ``publication.check_bundle`` requires every event link to be admitted for the event's own
    ``cusip9``/``security_id`` and every accepting-adjudication link to be in the event's links,
    so an exchange's new-side link (another issue) cannot be persisted today."""
    return all(x.cusip9 == cusip9 and x.security_id == security_id for x in links)


def _exchange_pairing_problem(old_links: Sequence[EventLink], upper: dt.date, asof: AsOf) -> str:
    """Typed outcome of the reviewed old->new pairing of a completed exchange.

    A pairing is an effective human record with an admitting status that cites an old-side link,
    a new-side (``exchange_new``, other issue) link and both exchange-document observations; every
    relied observation/link must be usable at K and the new side valid at the exchange date. A
    shared ``document_sha256`` is never a pairing. Even a valid pairing is refused while W0 cannot
    persist its new side (``exchange_pairing_not_persistable``)."""
    old_ids = {x.link_id for x in old_links}
    reason = "exchange_pairing_unreviewed"
    for p in sorted(asof.adjs.values(), key=lambda x: (x.adjudicated_at, str(x.adjudication_id))):
        cited_old = old_ids & set(p.link_ids)
        new_side = [asof.links[lid] for lid in p.link_ids
                    if lid in asof.links and asof.links[lid].affected_scope == "exchange_new"]
        if not cited_old or not new_side:
            continue
        if p.reviewer_role != "human_reviewer" or p.policy_digest != POLICY_DIGEST:
            reason = "exchange_pairing_not_human"
            continue
        if p.status not in SCOPE_DECISION_STATUSES or asof.effective_head(p) is not p:
            reason = "exchange_pairing_not_effective"
            continue
        documents = {asof.links[lid].observation_id for lid in cited_old} | {x.observation_id for x in new_side}
        if not documents <= set(p.evidence_observation_ids):
            reason = "exchange_pairing_document_not_cited"
            continue
        _times, problem = asof.dependency_times(p)
        if problem:
            reason = f"exchange_pairing_dependency_unusable:{problem}"
            continue
        old_cusip = old_links[0].cusip9
        if any(x.cusip9 == old_cusip for x in new_side):
            reason = "exchange_new_side_is_old_issue"
            continue
        if not all(x.valid_from <= upper and (x.valid_to is None or x.valid_to >= upper) for x in new_side):
            reason = "exchange_new_link_not_valid_at_exchange"
            continue
        # A valid pairing still fails closed: its new side is another issue, which
        # ``w0_event_closure_persistable`` rejects, so no episode and no alias spell are emitted.
        return "exchange_pairing_not_persistable"
    return reason


def _proposal_lineage_problem(proposal: Any, materialized_known: dt.datetime) -> str | None:
    """Whether phase 1 can explain a transient W1 :class:`StateProposal`.

    W0 v2 can persist typed proposal/family rows, but this A3 adapter deliberately does not
    translate transient family or corroboration objects into those rows. The proposal's
    ``evidence_known_at`` also cannot exceed what the materialized direct closure explains
    (it is never overwritten with a larger scalar)."""
    if proposal.family_evidence or proposal.corroboration_adjudication_ids or proposal.corroboration_evidence_refs:
        return "proposal_lineage_not_persistable"
    if proposal.evidence_known_at > materialized_known:
        return "proposal_lineage_not_persistable"
    return None


def _disqualification(
    claim: EpisodeClaim, evidence: Sequence[CreditObservation], event_links: Sequence[EventLink],
    packages: Mapping[uuid.UUID, SourcePackage], upper: dt.date, asof: AsOf,
) -> str | None:
    """Policy admission gate for an accepted claim; ``None`` when it qualifies."""
    f = claim.facts
    if f.arrears_or_legal_deferral_only:
        return "arrears_or_legal_deferral_unknown_never_default"
    if f.pik_only:
        return "pik_never_default"
    kind = claim.primary_type
    if kind == "bankruptcy":
        if f.proceeding not in _ADMISSIBLE_PROCEEDINGS:
            return f"bankruptcy_{f.proceeding or 'unspecified'}_not_admissible"
        if not (f.debtor_verified and f.affected_obligation_verified):
            return "bankruptcy_debtor_and_affected_obligation_required"
    elif kind == "payment_default":
        if f.acceleration == "technical_covenant" and not f.missed_payment:
            return "technical_covenant_acceleration_nonqualifying"
        if not f.missed_payment:
            return "payment_default_requires_missed_payment"
        if f.cured_within_grace:
            return "cured_within_grace_nonqualifying"
        if not f.grace_period_expired_uncured:
            return "inside_grace_payment_is_candidate"
    elif kind == "distressed_exchange":
        if f.exchange_stage != "completed":
            return "proposed_exchange_is_candidate"
        if not f.coercive_impairment:
            return "exchange_not_distressed_impairment"
        old = [x for x in event_links if x.affected_scope == "exchange_old"]
        if not old:
            return "exchange_old_new_links_required"
        return _exchange_pairing_problem(old, upper, asof)
    elif kind == "agency_issue_default":
        if not any(
            o.observation_kind == "agency_action"
            and o.agency_subject_kind == "instrument"
            and (o.agency_rating_symbol == "D" or o.agency_action_classification == "WD")
            and c.is_approved_rating_package(packages[o.package_id])
            for o in evidence if o.package_id in packages
        ):
            return "agency_issue_default_requires_approved_instrument_action"
    elif kind != "default_state":
        return f"primary_type_invalid:{kind}"
    return None


def _flags(claim: EpisodeClaim, evidence: Sequence[CreditObservation], consensus: bool) -> tuple[str, ...]:
    flags = {_KIND_FLAGS[o.observation_kind] for o in evidence if o.observation_kind in _KIND_FLAGS}
    for o in evidence:
        if o.observation_kind == "agency_action":
            if o.agency_subject_kind == "issuer":
                flags.add("issuer_agency_default_linked")
            elif o.agency_action_classification == "WD":
                flags.add("agency_rac_wd")
            elif o.agency_rating_symbol == "D":
                flags.add("agency_issue_default")
    if consensus:
        flags.add("nport_consensus_state")
    for kind in (claim.primary_type, *claim.corroborating_types):
        if kind in EVENT_TYPES:
            flags.add(kind)
    return c.sorted_texts(flags)


def _claim_from_proposal(subject: uuid.UUID, proposal: Any) -> EpisodeClaim:
    return EpisodeClaim(
        episode_id=subject,
        primary_type="default_state",
        onset_upper_inclusive=proposal.onset_upper_inclusive,
        onset_lower_exclusive=proposal.onset_lower_exclusive,
        onset_lower_evidence_ids=tuple(proposal.onset_lower_evidence_ids),
    )


def _markers(obs: Iterable[CreditObservation]) -> tuple[CreditMarker, ...]:
    out = set()
    for o in obs:
        if o.observation_kind != "nport_holding" or o.cusip9 is None or o.report_date is None:
            continue
        if o.nport_arrears_or_deferral == "Y":
            out.add(CreditMarker(o.cusip9, o.report_date, "arrears_or_legal_deferral_unknown", o.observation_id))
        if o.nport_paid_in_kind == "Y":
            out.add(CreditMarker(o.cusip9, o.report_date, "paid_in_kind", o.observation_id))
    return tuple(sorted(out, key=lambda m: (m.cusip9, m.report_date, m.kind, str(m.observation_id))))


_EVENT_DEPENDENCY_FRAMES = (
    "source_packages",
    "observations",
    "event_links",
    "adjudications",
    "ncen_filings",
    "family_contexts",
    "family_evidence",
    "proposal_evidence",
    "exchange_relations",
)
_BASE_EVENT_FRAMES = ("source_packages", "observations", "event_links", "adjudications")


def _typed_frame_rows(frame: str, rows: Iterable[Any]) -> tuple[Any, ...]:
    """Validate one frame before any key-based index can silently overwrite a row."""
    expected = c.FRAME_TYPES[frame]
    found: dict[tuple[str, ...], Any] = {}
    for row in rows:
        if type(row) is not expected:
            raise ContractError(f"event_evidence_invalid:{frame}:row_type")
        key = row.key()
        if key in found:
            raise ContractError(f"event_evidence_invalid:{frame}:duplicate_identity:{'|'.join(key)}")
        found[key] = row
    return tuple(found[key] for key in sorted(found))


class _EventFrameBoundary:
    """Once-materialized, lazily validated nine-frame construction boundary."""

    def __init__(
        self,
        supplied: Mapping[str, Iterable[Any]] | None,
        direct: Mapping[str, tuple[Any, ...]],
    ) -> None:
        self._supplied = None if supplied is None else {
            frame: tuple(supplied[frame])
            for frame in _EVENT_DEPENDENCY_FRAMES
            if frame in supplied
        }
        self._direct = direct
        self._validated: Mapping[str, tuple[Any, ...]] | None = None
        self._problem: str | None = None
        self._checked = False

    def proposal_rows_present(self, proposal_ids: Iterable[uuid.UUID]) -> bool:
        """Whether correctly typed cited proposal rows are at least present in the supplied frame."""
        if self._supplied is None or "proposal_evidence" not in self._supplied:
            return False
        rows = self._supplied["proposal_evidence"]
        if any(type(row) is not c.ProposalEvidence for row in rows):
            return False
        available = {row.proposal_evidence_id for row in rows}
        return set(proposal_ids) <= available

    def validated(self) -> Mapping[str, tuple[Any, ...]]:
        if self._checked:
            if self._problem is not None:
                raise ContractError(self._problem)
            assert self._validated is not None
            return self._validated
        self._checked = True
        try:
            if self._supplied is None:
                raise ContractError("dependency_missing:frames")
            missing = [frame for frame in _EVENT_DEPENDENCY_FRAMES if frame not in self._supplied]
            if missing:
                raise ContractError(f"dependency_missing:frame:{missing[0]}")
            view = {
                frame: _typed_frame_rows(frame, self._supplied[frame])
                for frame in _EVENT_DEPENDENCY_FRAMES
            }
            for frame in _BASE_EVENT_FRAMES:
                supplied_rows = tuple((row.key(), row.row_sha256()) for row in view[frame])
                direct_rows = tuple((row.key(), row.row_sha256()) for row in self._direct[frame])
                if supplied_rows != direct_rows:
                    raise ContractError(f"event_evidence_invalid:base_frame_disagreement:{frame}")
            self._validated = view
            return view
        except ContractError as exc:
            self._problem = str(exc)
            raise


def resolve_episodes(
    adjudications: Iterable[Adjudication],
    *,
    observations: Iterable[CreditObservation],
    links: Iterable[EventLink],
    packages: Iterable[SourcePackage] = (),
    state_proposals: Iterable[StateProposal] = (),
    claims: Iterable[EpisodeClaim] = (),
    frames: Mapping[str, Iterable[Any]] | None = None,
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str = "current_run",
    strict: bool = True,
) -> EpisodeResolution:
    """Turn effective accepted adjudications into :class:`DefaultEpisode` rows as of ``K``.

    * Only the effective (unsuperseded) record of a subject counts; non-admitting statuses
      (candidate/nonqualifying/disputed/retracted) yield no event. Nothing is self-approved.
    * ``accepted_state`` uses the N-PORT :class:`StateProposal` of the linked CUSIP9 (or an
      explicit human claim); the adjudication must cite every proposal observation, a proposal
      with conflicts needs a human reviewer, and phase 1 refuses every proposal-backed state as
      ``proposal_evidence_not_closed`` or ``proposal_lineage_not_persistable``. An explicit human
      state without proposal lineage may proceed. ``accepted_event`` needs an :class:`EpisodeClaim`
      whose :class:`EventFacts` pass the admission gate; a completed exchange needs a reviewed
      old->new pairing and fails closed (``exchange_pairing_not_persistable``) while W0 cannot
      persist its new side. Resolution needs documentary same-obligation cure proof.
    * Evidence/links usable at K only; evidence set = the accepting adjudication's (plus
      admitted links covering it); ``evidence_known_at`` = max over the relied set <= K.
    * One episode per security per default spell: later Y or other types of the same spell are
      corroboration; a second spell needs a resolved first episode and an onset lower bound at
      or after that resolution. Overlaps are rejected.
    * A constructible event additionally requires ``frames`` with all nine v2 dependency
      inventories. The four base frames must exactly equal the once-materialized resolver
      inputs (adjudications use the complete as-of-eligible inventory); the other five may
      be explicitly empty only when no relied proposal/exchange path exists. Frame readiness
      is checked lazily, after existing negative gates, so no-event paths remain compatible.
    * ``strict`` raises :class:`ResolveError` listing every issue; otherwise offending episodes
      are dropped and reported.
    """
    k = _utc(knowledge_cutoff)
    _check_mode(knowledge_mode)
    try:
        observation_rows = _typed_frame_rows("observations", tuple(observations))
        link_rows = _typed_frame_rows("event_links", tuple(links))
        package_rows = _typed_frame_rows("source_packages", tuple(packages))
        adjudication_rows = _typed_frame_rows("adjudications", tuple(adjudications))
        eligible_adjudications = _typed_frame_rows(
            "adjudications",
            adjudications_as_of(adjudication_rows, k, knowledge_mode).values(),
        )
    except ContractError as exc:
        raise ResolveError(str(exc)) from exc
    proposal_rows = tuple(state_proposals)
    claim_rows = tuple(claims)
    frame_boundary = _EventFrameBoundary(
        frames,
        {
            "source_packages": package_rows,
            "observations": observation_rows,
            "event_links": link_rows,
            "adjudications": eligible_adjudications,
        },
    )
    asof = AsOf.build(k, knowledge_mode, observation_rows, link_rows, adjudication_rows)
    obs = asof.obs
    stale_o = set(asof.stale_obs)
    all_links = asof.links
    usable_links = [
        x for x in sorted(all_links.values(), key=lambda x: str(x.link_id)) if asof.link_problem(x.link_id) is None
    ]
    pkgs = {p.package_id: p for p in package_rows}
    adjs = asof.adjs
    effective = effective_adjudications(adjs)
    proposals: dict[str, Any] = {}
    for proposal in proposal_rows:
        if proposal.cusip9 in proposals:
            raise ResolveError("state_proposal_duplicate_cusip", (proposal.cusip9,))
        proposals[proposal.cusip9] = proposal
    claim_by_id: dict[uuid.UUID, EpisodeClaim] = {}
    for claim in claim_rows:
        if claim.episode_id in claim_by_id:
            raise ResolveError("claim_duplicate_episode", (str(claim.episode_id),))
        claim_by_id[claim.episode_id] = claim
    issues: list[EpisodeIssue] = []
    stats: Counter[str] = Counter()
    episodes: list[DefaultEpisode] = []
    alias_spells: dict[str, uuid.UUID] = {}

    for subject, a in sorted(effective.items(), key=lambda item: str(item[0])):
        stats[f"effective:{a.status}"] += 1
        if a.status not in ADMITTING_STATUSES:
            continue
        built = _build_episode(
            subject, a, adjs=adjs, obs=obs, stale_o=stale_o, all_links=all_links, usable_links=usable_links,
            pkgs=pkgs, proposals=proposals, claim=claim_by_id.get(subject), k=k, mode=knowledge_mode, asof=asof,
            frame_boundary=frame_boundary,
        )
        if isinstance(built, str):
            issues.append(EpisodeIssue(subject, built))
            continue
        episode, aliases = built
        episodes.append(episode)
        alias_spells.update(aliases)

    kept, overlap_issues = _disjoint(episodes)
    issues.extend(overlap_issues)
    issues.sort(key=lambda i: (str(i.subject_id), i.reason))
    if strict and issues:
        raise ResolveError("episode_resolution_failed", issues)
    stats["episodes"] = len(kept)
    inventory = tuple(sorted(adjs.values(), key=lambda x: str(x.adjudication_id)))
    usable_obs = [o for o in obs.values() if usable_observation(o, k, knowledge_mode, stale_o) is None]
    return EpisodeResolution(
        episodes=tuple(sorted(kept, key=lambda e: e.key())),
        issues=tuple(issues),
        markers=_markers(usable_obs),
        alias_spells=dict(sorted(alias_spells.items())),
        adjudication_inventory=inventory,
        stats=dict(sorted(stats.items())),
    )


def _build_episode(
    subject: uuid.UUID, a: Adjudication, *, adjs: Mapping[uuid.UUID, Adjudication],
    obs: Mapping[uuid.UUID, CreditObservation], stale_o: set[uuid.UUID], all_links: Mapping[uuid.UUID, EventLink],
    usable_links: Sequence[EventLink], pkgs: Mapping[uuid.UUID, SourcePackage], proposals: Mapping[str, Any],
    claim: EpisodeClaim | None, k: dt.datetime, mode: str, asof: AsOf, frame_boundary: _EventFrameBoundary,
) -> str | tuple[DefaultEpisode, dict[str, uuid.UUID]]:
    """One episode or the typed reason it cannot be admitted."""
    if a.policy_digest != POLICY_DIGEST:
        return "policy_digest_mismatch"
    usable_ids = {x.link_id for x in usable_links}
    cited = [all_links.get(lid) for lid in a.link_ids]
    if any(x is None for x in cited):
        return "adjudication_link_outside_inventory"
    event_links: list[EventLink] = cited  # type: ignore[assignment]
    new_side_obs: set[uuid.UUID] = set()
    if claim is not None and claim.primary_type == "distressed_exchange":
        # New-side links belong to the pairing (validated by ``_exchange_pairing_problem``), not
        # to the old issue's own closure.
        new_side_obs = {x.observation_id for x in event_links if x.affected_scope == "exchange_new"}
        event_links = [x for x in event_links if x.affected_scope != "exchange_new"]
        if not event_links:
            return "exchange_old_new_links_required"
    if any(x.link_id not in usable_ids for x in event_links):
        return "adjudication_link_not_admitted_at_cutoff"
    cusips = {x.cusip9 for x in event_links}
    if len(cusips) != 1 or len({x.security_id for x in event_links}) != 1:
        return "links_span_multiple_securities"
    cusip = event_links[0].cusip9
    proposal = proposals.get(cusip)
    from_proposal = False
    if a.status == "accepted_state":
        if claim is None:
            if proposal is None or proposal.proposed_status != "accepted_state":
                return "state_claim_missing"
            claim, from_proposal = _claim_from_proposal(subject, proposal), True
        elif a.reviewer_role != "human_reviewer":
            # Only a human may state bounds that did not come from the policy proposal.
            return "human_review_required:explicit_state_claim"
        if claim.primary_type != "default_state":
            return "accepted_state_requires_default_state"
    else:
        if claim is None:
            return "event_claim_missing"
        if claim.primary_type == "default_state":
            return "accepted_event_cannot_be_default_state"
    if from_proposal:
        if not set(proposal.evidence_observation_ids) <= set(a.evidence_observation_ids):
            return "adjudication_does_not_cover_proposal_evidence"
        if proposal.evidence_known_at > k:
            return "state_proposal_known_after_cutoff"
        if proposal.conflict_dates and a.reviewer_role != "human_reviewer":
            return "human_review_required:conflicting_sources"
    try:
        lower, upper = claim.bounds()
    except ResolveError as exc:
        return exc.code
    evidence: list[CreditObservation] = []
    for oid in a.evidence_observation_ids:
        o = obs.get(oid)
        if o is None:
            return "evidence_outside_inventory"
        if oid in new_side_obs:
            continue  # validated with the pairing
        reason = usable_observation(o, k, mode, stale_o)
        if reason:
            return f"evidence_unusable:{reason}"
        evidence.append(o)
    if not {x.observation_id for x in event_links} <= set(a.evidence_observation_ids):
        return "link_observation_not_in_evidence"
    # Every relied observation must be covered by an admitted link of this security.
    covered = {x.observation_id for x in event_links}
    for o in evidence:
        if o.observation_id in covered:
            continue
        extra = next((x for x in usable_links if x.observation_id == o.observation_id and x.cusip9 == cusip
                      and x.security_id == event_links[0].security_id), None)
        if extra is None:
            return "evidence_not_linked"
        event_links.append(extra)
        covered.add(o.observation_id)
    for x in event_links:
        if not (x.valid_from <= upper and (x.valid_to is None or x.valid_to >= upper)):
            return "link_not_valid_at_onset"
    obligors = {x.obligor_id for x in event_links}
    if len(obligors) != 1:
        return "links_span_multiple_obligors"
    reason = _disqualification(claim, evidence, event_links, pkgs, upper, asof)
    if reason:
        return reason
    lower_ids = c.sorted_uuids(claim.onset_lower_evidence_ids)
    if (lower is None) != (not lower_ids):
        return "onset_lower_evidence_required_iff_lower_bound"
    if lower is not None and not all(
        oid in a.evidence_observation_ids and dates_onset_lower(obs[oid], cusip, lower) for oid in lower_ids
    ):
        return "onset_lower_unsupported"
    link_known = max(x.link_known_at for x in event_links)
    known = max(max(o.public_available_at for o in evidence), link_known)
    if known > k:
        return "evidence_known_after_cutoff"
    lineage_problem = _proposal_lineage_problem(proposal, known) if (
        proposal is not None and proposal.proposed_status == "accepted_state"
    ) else None
    if from_proposal and lineage_problem is not None:
        return "proposal_lineage_not_persistable"
    resolution = _resolution(claim, upper, cusip, asof, a)
    if isinstance(resolution, str):
        return resolution
    resolution_known, resolution_refs = resolution
    if "distressed_exchange" in claim.corroborating_types:
        return "exchange_pairing_not_persistable"
    chain_has_proposal_support = any(
        row.subject_id == subject and row.policy_digest == POLICY_DIGEST and row.proposal_evidence_ids
        for row in adjs.values()
    )
    if from_proposal:
        if not a.proposal_evidence_ids or not frame_boundary.proposal_rows_present(a.proposal_evidence_ids):
            return "proposal_evidence_not_closed"
        return "proposal_lineage_not_persistable"
    if chain_has_proposal_support:
        return "proposal_lineage_not_persistable"
    # Phase 1 never emits proposal-backed consensus. A transient proposal beside an explicit
    # human claim is incidental and cannot change the event flags or bounds.
    consensus = False
    # Exchange episodes never reach here while W0 cannot persist the pairing (fail closed), so
    # no alias spell is ever emitted.
    aliases: dict[str, uuid.UUID] = {}
    alias_spell: uuid.UUID | None = None
    recognition = claim.recognition_date
    if recognition is None and claim.primary_type == "agency_issue_default":
        dates = [o.agency_action_date for o in evidence if o.observation_kind == "agency_action" and o.agency_action_date]
        recognition = min(dates) if dates else None
    evidence_ids = c.sorted_uuids(o.observation_id for o in evidence)
    link_ids = c.sorted_uuids(x.link_id for x in event_links)
    adj_ids = c.sorted_uuids(
        x.adjudication_id for x in adjs.values() if x.subject_id == subject and x.policy_digest == POLICY_DIGEST
    )
    # The v2 event closure spans the whole adjudication chain. A chain revision that still cites a
    # link superseded as of K (the quarantined proposal an in-chain scope decision admitted) would
    # put stale evidence into the published closure (``evidence_superseded``), so refuse it here.
    if any(lid in asof.stale_links for aid in adj_ids for lid in adjs[aid].link_ids):
        return "adjudication_chain_cites_superseded_link"
    edges = {
        "evidence_observation_ids": evidence_ids,
        "onset_lower_evidence_ids": lower_ids,
        "link_ids": link_ids,
        "adjudication_ids": adj_ids,
        "proposal_evidence_ids": c.sorted_uuids(a.proposal_evidence_ids),
        "exchange_relation_ids": (),
    }
    try:
        frozen_frames = frame_boundary.validated()
        strict_dependency_digest = publication.event_dependency_digest(
            frozen_frames,
            knowledge_cutoff=k,
            knowledge_mode=mode,
            **edges,
        )
        derived = publication.derive_event_fields(
            frozen_frames,
            knowledge_cutoff=k,
            knowledge_mode=mode,
            **edges,
        )
    except ContractError as exc:
        return f"contract:{exc}"
    if derived["dependency_digest"] != strict_dependency_digest:
        return f"contract:dependency_digest_mismatch:{subject}"
    if derived["link_known_at"] > k:
        return "link_known_after_cutoff"
    if derived["evidence_known_at"] > k:
        return "evidence_known_after_cutoff"
    obligor = next(iter(obligors))
    try:
        episode = DefaultEpisode(
            security_id=event_links[0].security_id,
            episode_id=subject,
            cusip9=cusip,
            obligor_id=obligor,
            issuer_episode_id=claim.issuer_episode_id or c.uuid5_of("bond_default_issuer_episode", obligor, str(subject)),
            primary_type=claim.primary_type,
            corroboration_flags=_flags(claim, evidence, consensus),
            admission_status=a.status,
            timing_class=c.derive_timing_class(lower, upper),
            onset_date=claim.onset_date,
            onset_lower_exclusive=lower,
            onset_upper_inclusive=upper,
            onset_lower_evidence_ids=derived["onset_lower_evidence_ids"],
            recognition_date=recognition,
            evidence_known_at=derived["evidence_known_at"],
            link_known_at=derived["link_known_at"],
            evidence_observation_ids=derived["evidence_observation_ids"],
            link_ids=derived["link_ids"],
            adjudication_ids=derived["adjudication_ids"],
            resolution_date=claim.resolution_date,
            resolution_refs=resolution_refs,
            resolution_known_at=resolution_known,
            alias_spell_id=alias_spell,
            proposal_evidence_ids=derived["proposal_evidence_ids"],
            exchange_relation_ids=derived["exchange_relation_ids"],
            dependency_digest=derived["dependency_digest"],
            event_input_digest=derived["event_input_digest"],
        )
    except ContractError as exc:
        return f"contract:{exc}"
    return episode, aliases


_NEGATIVE_STATUSES = frozenset({"retracted", "nonqualifying", "disputed"})


def _cure_proof_problem(o: CreditObservation, cusip9: str, resolution_date: dt.date, asof: AsOf) -> str | None:
    """``None`` when ``o`` is documentary cure/extinguishment proof of *this* obligation that
    supports ``resolution_date``; N-PORT holdings (N or otherwise) never qualify."""
    if o.observation_kind == "nport_holding":
        return "nport_n_is_not_cure_evidence"
    if not o.observation_kind.endswith("_passage"):
        return "resolution_proof_not_documentary"
    if o.cusip9 is not None and o.cusip9 != cusip9:
        return "resolution_scope_mismatch"
    if o.cusip9 is None and not any(
        x.observation_id == o.observation_id and x.cusip9 == cusip9 and asof.link_problem(x.link_id) is None
        for x in asof.links.values()
    ):
        return "resolution_scope_not_established"
    if o.date_precision == "day":
        supported = o.effective_date == resolution_date
    elif o.date_precision == "interval":
        supported = o.effective_upper_inclusive == resolution_date
    else:
        supported = False
    return None if supported else "resolution_date_unsupported"


def _resolution(
    claim: EpisodeClaim, upper: dt.date, cusip9: str, asof: AsOf, accepting: Adjudication,
) -> str | tuple[dt.datetime | None, tuple[uuid.UUID, ...]]:
    """(resolution_known_at, refs) or a typed reason.

    Resolution needs documentary cure/extinguishment proof of the same obligation supporting the
    stated date (:func:`_cure_proof_problem`): every direct observation reference must be such
    proof. Cure stays in the ``issue_episode`` chain (interim v1 rule): an adjudication reference
    counts only as this episode's own effective admitting revision ``accepting``, through its own
    usable underlying evidence (which must include such proof and no other obligation's
    documents). A ``candidate`` is never a decision; any other positive record is a standalone
    decision W0 v1 cannot persist. Times mirror ``check_bundle`` (observations plus what
    referenced adjudications cite)."""
    refs = c.sorted_uuids(claim.resolution_refs)
    if claim.resolution_date is None:
        return "resolution_refs_without_date" if refs else (None, ())
    if not refs:
        return "resolution_date_without_evidence"
    if claim.resolution_date < upper:
        return "resolution_before_onset"
    times: list[dt.datetime] = []
    proven = False
    for rid in refs:
        if rid in asof.obs:
            problem = asof.observation_problem(rid)
            if problem:
                return f"resolution_evidence_unusable:{problem}"
            problem = _cure_proof_problem(asof.obs[rid], cusip9, claim.resolution_date, asof)
            if problem:
                return problem
            proven = True
            times.append(asof.obs[rid].public_available_at)
        elif rid in asof.adjs:
            ra = asof.adjs[rid]
            if ra.status == "candidate":
                return CANDIDATE_IS_NOT_A_DECISION
            if ra.reviewer_role != "human_reviewer" or ra.policy_digest != POLICY_DIGEST:
                return "resolution_adjudication_not_human"
            if ra.subject_id == accepting.subject_id and ra is not accepting:
                return "resolution_adjudication_not_effective"  # superseded or negative record of this chain
            if ra.status in _NEGATIVE_STATUSES:
                return "resolution_adjudication_not_effective"
            if ra is not accepting:
                return STANDALONE_DECISION_NOT_PERSISTABLE
            dep_times, problem = asof.dependency_times(ra)
            if problem:
                return f"resolution_evidence_unusable:{problem}"
            reasons = []
            for oid in ra.evidence_observation_ids:
                reason = _cure_proof_problem(asof.obs[oid], cusip9, claim.resolution_date, asof)
                if reason in ("resolution_scope_mismatch", "resolution_scope_not_established"):
                    return reason
                reasons.append(reason)
            if None not in reasons:
                return min(reasons) if reasons else "resolution_documentary_proof_missing"  # type: ignore[type-var]
            proven = True
            times.extend(dep_times)
        else:
            return "resolution_ref_outside_inventory"
    if not proven or not times:
        return "resolution_documentary_proof_missing"
    if max(times) > asof.k:
        return "resolution_known_after_cutoff"
    return max(times), refs


def _disjoint(episodes: Sequence[DefaultEpisode]) -> tuple[list[DefaultEpisode], list[EpisodeIssue]]:
    """Per security: earlier episode resolved and the next onset lower bound at/after it."""
    by_security: dict[uuid.UUID, list[DefaultEpisode]] = defaultdict(list)
    for e in episodes:
        by_security[e.security_id].append(e)
    kept: list[DefaultEpisode] = []
    issues: list[EpisodeIssue] = []
    for rows in by_security.values():
        rows.sort(key=lambda e: (e.onset_upper_inclusive, str(e.episode_id)))
        accepted: list[DefaultEpisode] = []
        for e in rows:
            clash = any(
                not (
                    prior.resolution_date is not None
                    and e.onset_lower_exclusive is not None
                    and e.onset_lower_exclusive >= prior.resolution_date
                )
                for prior in accepted
            )
            if clash:
                issues.append(EpisodeIssue(e.episode_id, "overlapping_episodes"))
                continue
            accepted.append(e)
        kept.extend(accepted)
    return kept, issues


# ---------------------------------------------------------------------------
# Follow-up
# ---------------------------------------------------------------------------
CENSORING_REASONS = (
    "default_onset", "exchange_alias_absorbed", "panel_exit", "reentry_gap", "target_end",
)
_DOCUMENT_STATUSES = frozenset({"nondefault_continuous", "repaid", "resolved"})
_DOCUMENT_BASES = frozenset({"continuous_document", "issuer_trustee_confirmation"})
#: Canonical basis when compatible documents of different bases cover the same span.
_BASIS_ORDER = ("issuer_trustee_confirmation", "continuous_document")
Interval = tuple[dt.date, dt.date]  # (start_exclusive, end_inclusive]


@dataclass(frozen=True)
class ContinuityEvidence:
    """Documentary evidence of one issue's status over ``(start_exclusive, end_inclusive]``
    (continuous filings, issuer/trustee confirmation, proven repayment or resolution).

    At least one cited observation must be a document passage (N-PORT holdings never qualify).
    An issuer-level passage (no CUSIP9) counts for this issue only through an admitted link whose
    scope was established by a persistable human scope decision (:func:`scope_decision`).
    ``adjudication_ids`` (continuity / dispute decisions) must each be the effective admitting
    human revision of an admitted episode of this issue citing the document (interim v1 rule)."""

    cusip9: str
    start_exclusive: dt.date
    end_inclusive: dt.date
    status: str
    basis: str
    observation_ids: tuple[uuid.UUID, ...]
    adjudication_ids: tuple[uuid.UUID, ...] = ()


@dataclass(frozen=True)
class Censoring:
    cusip9: str
    spell_id: uuid.UUID
    at: dt.date
    reason: str


@dataclass(frozen=True)
class FollowUpResolution:
    followups: tuple[c.FollowUp, ...]
    censoring: tuple[Censoring, ...]
    issues: tuple[str, ...]


@dataclass(frozen=True)
class _Support:
    """One validated documentary support (dependencies resolved as of K)."""

    span: Interval
    status: str
    basis: str
    observation_ids: frozenset[uuid.UUID]
    adjudication_ids: frozenset[uuid.UUID]
    known: dt.datetime


def outcome_interval(month: dt.date, target_month: dt.date) -> Interval | None:
    """Plan section 5.1: the month-end snapshot of ``m`` covers ``(end(m), end(m+1)]``; starts
    at or after T have no outcome interval within T."""
    m = c.month_key(month)
    if m >= c.month_key(target_month):
        return None
    return c.month_end(m), c.month_end(c.add_months(m, 1))


def _runs(months: Iterable[dt.date]) -> list[list[dt.date]]:
    runs: list[list[dt.date]] = []
    for m in sorted(set(months)):
        if runs and c.add_months(runs[-1][-1], 1) == m:
            runs[-1].append(m)
        else:
            runs.append([m])
    return runs


def _subtract(window: Interval, holes: Iterable[Interval]) -> list[Interval]:
    parts = [window]
    for hs, he in holes:
        nxt: list[Interval] = []
        for s, e in parts:
            if he <= s or hs >= e:
                nxt.append((s, e))
                continue
            if hs > s:
                nxt.append((s, hs))
            if he < e:
                nxt.append((he, e))
        parts = nxt
    return sorted(parts)


def _intersect(a: Interval, b: Interval) -> Interval | None:
    s, e = max(a[0], b[0]), min(a[1], b[1])
    return (s, e) if s < e else None


def _union(intervals: Iterable[Interval]) -> list[Interval]:
    merged: list[Interval] = []
    for s, e in sorted(intervals):
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def interval_covered(target: Interval, intervals: Iterable[Interval]) -> bool:
    """``target`` lies entirely inside the union of ``intervals``."""
    return any(s <= target[0] and target[1] <= e for s, e in _union(intervals))


def _issuer_scope(oid: uuid.UUID, cusip9: str, asof: AsOf) -> tuple[dt.datetime | None, str]:
    """``(known_at, "")`` of the established scope of issuer-level document ``oid`` for
    ``cusip9`` (same rule as the link resolver), or ``(None, reason)``; a candidate or standalone
    scope decision on the quarantined proposal is reported as such (fail closed)."""
    reason = "continuity_scope_not_established"
    for x in sorted(asof.links.values(), key=lambda x: str(x.link_id)):
        if x.observation_id != oid or x.cusip9 != cusip9:
            continue
        if x.status == "quarantined" and x.link_id not in asof.stale_links:
            outcome = scope_decision(x, asof)
            if outcome.reason in (CANDIDATE_IS_NOT_A_DECISION, STANDALONE_DECISION_NOT_PERSISTABLE):
                reason = outcome.reason
            continue
        if asof.link_problem(x.link_id) is not None:
            continue
        proposal = asof.links.get(x.supersedes_link_id) if x.supersedes_link_id else None
        if proposal is None or proposal.status != "quarantined":
            continue
        outcome = scope_decision(proposal, asof, revision_id_for=lambda _a, _t, x=x: x.link_id)
        if outcome.decision is not None and outcome.known_at is not None:
            return max(outcome.known_at, x.link_known_at), ""
        if outcome.reason in (CANDIDATE_IS_NOT_A_DECISION, STANDALONE_DECISION_NOT_PERSISTABLE):
            reason = outcome.reason
    return None, reason


def _continuity_decision_problem(
    a: Adjudication | None, ev: ContinuityEvidence, asof: AsOf, episodes: Mapping[uuid.UUID, DefaultEpisode],
) -> str | None:
    """Interim v1 rule for a continuity/dispute decision record cited by ``ev``.

    A ``candidate`` is never a decision. A positive decision counts only as the effective admitting
    human revision of an admitted episode of this issue that W0 v1 persists (the record is in the
    episode's ``adjudication_ids``); anything else is ``standalone_decision_not_persistable``."""
    if a is None or a.reviewer_role != "human_reviewer" or a.policy_digest != POLICY_DIGEST:
        return "continuity_adjudication_unusable:not_available_human_record"
    if a.status == "candidate":
        return CANDIDATE_IS_NOT_A_DECISION
    if asof.effective_head(a) is not a or a.status in _NEGATIVE_STATUSES:
        return "continuity_adjudication_unusable:not_effective"
    episode = episodes.get(a.subject_id)
    if (
        a.status not in ADMITTING_STATUSES or a.subject_kind != "issue_episode" or episode is None
        or episode.cusip9 != ev.cusip9 or a.adjudication_id not in episode.adjudication_ids
    ):
        return STANDALONE_DECISION_NOT_PERSISTABLE
    if not set(a.evidence_observation_ids) & set(ev.observation_ids):
        return "continuity_adjudication_unusable:unrelated"
    return None


def _continuity_support(
    ev: ContinuityEvidence, asof: AsOf, episodes: Mapping[uuid.UUID, DefaultEpisode], issues: list[str],
) -> _Support | str:
    """Validated support, or the reason the document itself is unusable.

    A cited ``candidate`` or standalone decision is not a decision: it is ignored (typed issue
    appended to ``issues``) and contributes nothing, so the document stands on its own merits and a
    conflict it was meant to resolve stays disputed. Negative or unusable decisions invalidate the
    support."""
    if ev.status not in _DOCUMENT_STATUSES or ev.basis not in _DOCUMENT_BASES:
        return "continuity_status_or_basis_invalid"
    if not ev.start_exclusive < ev.end_inclusive:
        return "continuity_interval_empty"
    times: list[dt.datetime] = []
    documentary = False
    for oid in ev.observation_ids:
        problem = asof.observation_problem(oid)
        if problem:
            return f"continuity_evidence_unusable:{problem}"
        o = asof.obs[oid]
        if o.cusip9 is not None and o.cusip9 != ev.cusip9:
            return "continuity_scope_mismatch"
        if o.cusip9 is None:
            scope_known, reason = _issuer_scope(oid, ev.cusip9, asof)
            if scope_known is None:
                return reason
            times.append(scope_known)
        documentary = documentary or o.observation_kind.endswith("_passage")
        times.append(o.public_available_at)
    if not documentary:
        return "nport_only_is_not_continuous_evidence"
    decisions: set[uuid.UUID] = set()
    for aid in ev.adjudication_ids:
        a = asof.adjs.get(aid)
        problem = _continuity_decision_problem(a, ev, asof, episodes)
        if problem in (CANDIDATE_IS_NOT_A_DECISION, STANDALONE_DECISION_NOT_PERSISTABLE):
            issues.append(f"{problem}:{ev.cusip9}")
            continue
        if problem:
            return problem
        assert a is not None
        dep_times, problem = asof.dependency_times(a)
        if problem:
            return f"continuity_adjudication_unusable:{problem}"
        times += dep_times + asof.review_times(a)
        decisions.add(aid)
    known = max(times)
    if known > asof.k:
        return "continuity_known_after_cutoff"
    return _Support((ev.start_exclusive, ev.end_inclusive), ev.status, ev.basis, frozenset(ev.observation_ids),
                    frozenset(decisions), known)


def _dispute_winner(covering: Sequence[_Support], asof: AsOf) -> str | None:
    """Status chosen by an effective human adjudication citing every conflicting document."""
    every = set().union(*(s.observation_ids for s in covering))
    winners = {
        s.status for s in covering for aid in s.adjudication_ids
        if every <= set(asof.adjs[aid].evidence_observation_ids)
    }
    return next(iter(winners)) if len(winners) == 1 else None


def _documentary_pieces(
    window: Interval, supports: Sequence[_Support], asof: AsOf, cusip9: str, issues: list[str],
) -> list[tuple[Interval, _Support]]:
    """Canonical merge of documentary support inside ``window`` (order independent).

    Elementary intervals are cut at every support endpoint; compatible supports (same status)
    merge their dependencies; conflicting statuses leave the span unknown with a typed dispute
    issue unless an effective adjudication resolves them."""
    clipped = [(hit, s) for s in supports if (hit := _intersect(window, s.span)) is not None]
    cuts = sorted({window[0], window[1], *(p for hit, _s in clipped for p in hit)})
    out: list[tuple[Interval, _Support]] = []
    for lo, hi in pairwise(cuts):
        covering = [s for hit, s in clipped if hit[0] <= lo and hi <= hit[1]]
        if not covering:
            continue
        statuses = {s.status for s in covering}
        if len(statuses) > 1:
            winner = _dispute_winner(covering, asof)
            if winner is None:
                issues.append(f"continuity_status_dispute:{cusip9}:{lo.isoformat()}:{hi.isoformat()}")
                continue
            covering = [s for s in covering if s.status == winner]
        basis = next(x for x in _BASIS_ORDER if any(s.basis == x for s in covering))
        merged = _Support(
            (lo, hi), covering[0].status, basis, frozenset().union(*(s.observation_ids for s in covering)),
            frozenset().union(*(s.adjudication_ids for s in covering)), max(s.known for s in covering),
        )
        if out and out[-1][0][1] == lo and _same_support(out[-1][1], merged):
            out[-1] = ((out[-1][0][0], hi), merged)
        else:
            out.append(((lo, hi), merged))
    return out


def _same_support(a: _Support, b: _Support) -> bool:
    return (a.status, a.basis, a.observation_ids, a.adjudication_ids, a.known) == (
        b.status, b.basis, b.observation_ids, b.adjudication_ids, b.known)


def _blocking(o: CreditObservation) -> bool:
    """N-PORT evidence inconsistent with a nondefault interval: Y, an invalid default flag, or
    arrears/legal deferral."""
    presence = o.field_presence.get("nport_is_default")
    return o.nport_is_default == "Y" or presence == "invalid" or o.nport_arrears_or_deferral == "Y"


def _surveillance_pieces(
    gap: Interval, cusip9: str, documented: Sequence[tuple[Interval, _Support]], asof: AsOf,
    receipt: c.ValidationReceipt | None, issues: list[str],
) -> list[tuple[Interval, tuple[uuid.UUID, ...], dt.datetime]]:
    """Surveillance segments inside ``gap``: consecutive supported nondefault boundaries (a clean
    N-PORT N of this issue at the date, or a documentary nondefault segment containing it), a
    qualified receipt issued by K covering the whole interval, and no Y/invalid/arrears N-PORT
    evidence inside. Anything else stays unknown; blocking evidence under a covering receipt is
    reported as a typed issue."""
    if receipt is None or receipt.verdict != "qualified" or receipt.issued_at > asof.k:
        return []
    holdings = [
        o for o in asof.obs.values()
        if o.observation_kind == "nport_holding" and o.cusip9 == cusip9 and o.report_date is not None
        and gap[0] <= o.report_date <= gap[1] and asof.observation_problem(o.observation_id) is None
    ]
    blocked_dates = {o.report_date for o in holdings if _blocking(o)}
    clean: dict[dt.date, set[uuid.UUID]] = defaultdict(set)
    for o in holdings:
        if o.nport_is_default == "N" and o.report_date not in blocked_dates:
            clean[o.report_date].add(o.observation_id)  # type: ignore[index]
    boundary: dict[dt.date, set[uuid.UUID]] = {d: set(ids) for d, ids in clean.items()}
    for span, support in documented:
        if support.status == "nondefault_continuous":
            for d in (gap[0], gap[1]):
                if span[0] < d <= span[1]:
                    boundary.setdefault(d, set()).update(support.observation_ids)
    out = []
    dates = sorted(boundary)
    for lo, hi in pairwise(dates):
        if not receipt.surveils(True, lo, hi) or any(lo < d <= hi for d in blocked_dates):
            continue
        inside = {o.observation_id for o in holdings if lo < o.report_date <= hi}  # type: ignore[operator]
        ids = boundary[lo] | boundary[hi] | inside
        known = max([receipt.issued_at, *(asof.obs[i].public_available_at for i in ids)])
        if known <= asof.k:
            out.append(((lo, hi), c.sorted_uuids(ids), known))
    for d in sorted(blocked_dates):  # type: ignore[type-var]
        if gap[0] < d and receipt.surveils(True, d - dt.timedelta(days=1), d):
            issues.append(f"surveillance_blocked_by_default_evidence:{cusip9}:{d.isoformat()}")
    return out


def build_followup(
    panel_grid: Iterable[tuple[str, dt.date]],
    *,
    securities: Mapping[str, uuid.UUID],
    observations: Iterable[CreditObservation],
    episodes: Iterable[DefaultEpisode] = (),
    continuity: Iterable[ContinuityEvidence] = (),
    adjudications: Iterable[Adjudication] = (),
    links: Iterable[EventLink] = (),
    validation_receipt: c.ValidationReceipt | None = None,
    alias_spells: Mapping[str, uuid.UUID] | None = None,
    target_month: dt.date,
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str = "current_run",
    strict: bool = True,
) -> FollowUpResolution:
    """Continuous follow-up segments per issue spell as of ``K``.

    * Candidate intervals follow plan section 5.1: grid snapshot ``m < T`` covers
      ``(end(m), end(m+1)]``; a spell is a run of consecutive starts (a terminal start without a
      following panel row is kept). Missing intervening starts stay absent; every interval from an
      actual start is covered or censored explicitly (typed :data:`CENSORING_REASONS`).
    * The at-risk part excludes every episode's ``(lower, resolution]`` (whole prefix when
      prevalent). It is covered by canonically merged documentary support (disputes -> unknown +
      issue), then by surveillance between supported nondefault boundaries, else ``unknown``
      (basis ``none``, ``known_at = K``).
    * Every relied observation/link/adjudication is resolved as of K; review time counts only in
      current_run. Exchange-new aliases emit no survival rows.
    * Interim v1 decision rule: continuity/dispute decisions (``ContinuityEvidence.adjudication_ids``)
      count only as the effective admitting revision of an admitted episode of the same issue in
      ``episodes``; issuer-level scope follows :func:`scope_decision`. ``candidate`` records and
      standalone decisions fail closed (``candidate_is_not_a_decision``,
      ``standalone_decision_not_persistable``): the span stays ``unknown``.
    * ``strict`` raises :class:`ResolveError` listing every semantic issue.
    """
    k = _utc(knowledge_cutoff)
    _check_mode(knowledge_mode)
    t_end = c.month_end(target_month)
    asof = AsOf.build(k, knowledge_mode, observations, links, adjudications)
    aliases = dict(alias_spells or {})
    starts: dict[str, list[dt.date]] = defaultdict(list)
    for cusip9, month in panel_grid:
        if outcome_interval(month, target_month) is not None:
            starts[cusip9].append(c.month_key(month))
    by_security: dict[str, list[DefaultEpisode]] = defaultdict(list)
    episode_by_id: dict[uuid.UUID, DefaultEpisode] = {}
    for e in episodes:
        by_security[e.cusip9].append(e)
        episode_by_id[e.episode_id] = e
    issues: list[str] = []
    supports: dict[str, list[_Support]] = defaultdict(list)
    for ev in continuity:
        support = _continuity_support(ev, asof, episode_by_id, issues)
        if isinstance(support, str):
            issues.append(f"{support}:{ev.cusip9}")
        else:
            supports[ev.cusip9].append(support)
    followups: list[c.FollowUp] = []
    censoring: list[Censoring] = []
    for cusip9 in sorted(starts):
        security = securities.get(cusip9)
        if security is None:
            issues.append(f"security_id_missing:{cusip9}")
            continue
        runs = _runs(starts[cusip9])
        for index, run in enumerate(runs):
            spell_id = c.uuid5_of("bond_default_followup_spell", cusip9, run[0].isoformat())
            start = c.month_end(run[0])
            end = min(c.month_end(c.add_months(run[-1], 1)), t_end)
            if cusip9 in aliases:
                censoring.append(Censoring(cusip9, aliases[cusip9], start, "exchange_alias_absorbed"))
                continue
            holes: list[Interval] = []
            for e in by_security.get(cusip9, ()):
                hole = (e.onset_lower_exclusive or dt.date.min, e.resolution_date or dt.date.max)
                holes.append(hole)
                hit = _intersect((start, end), hole)
                if hit is not None:
                    censoring.append(Censoring(cusip9, spell_id, hit[0], "default_onset"))
            for window in _subtract((start, end), holes):
                followups.extend(_cover_window(cusip9, security, spell_id, window, supports[cusip9], asof,
                                               validation_receipt, issues))
            reason = "target_end" if end == t_end else "reentry_gap" if index + 1 < len(runs) else "panel_exit"
            censoring.append(Censoring(cusip9, spell_id, end, reason))
    followups.sort(key=lambda f: f.key())
    censoring.sort(key=lambda x: (x.cusip9, x.at, x.reason, str(x.spell_id)))
    unique_issues = tuple(sorted(set(issues)))
    if strict and unique_issues:
        raise ResolveError("followup_resolution_failed", unique_issues)
    return FollowUpResolution(tuple(followups), tuple(censoring), unique_issues)


def _segment(cusip9: str, security: uuid.UUID, spell_id: uuid.UUID, part: Interval, status: str, basis: str,
             evidence: Iterable[uuid.UUID], adjudication_ids: Iterable[uuid.UUID], known: dt.datetime) -> c.FollowUp:
    return c.FollowUp(
        security_id=security, spell_id=spell_id,
        segment_id=c.uuid5_of("bond_default_followup_segment", str(spell_id), part[0].isoformat(),
                              part[1].isoformat(), status),
        cusip9=cusip9, interval_start_exclusive=part[0], interval_end_inclusive=part[1], status=status,
        completeness_basis=basis, evidence_observation_ids=c.sorted_uuids(evidence),
        adjudication_ids=c.sorted_uuids(adjudication_ids), known_at=known,
    )


def _cover_window(
    cusip9: str, security: uuid.UUID, spell_id: uuid.UUID, window: Interval, supports: Sequence[_Support],
    asof: AsOf, receipt: c.ValidationReceipt | None, issues: list[str],
) -> list[c.FollowUp]:
    out: list[c.FollowUp] = []
    documented = _documentary_pieces(window, supports, asof, cusip9, issues)
    for part, s in documented:
        out.append(_segment(cusip9, security, spell_id, part, s.status, s.basis, s.observation_ids,
                            s.adjudication_ids, s.known))
    for gap in _subtract(window, [part for part, _s in documented]):
        surveilled = _surveillance_pieces(gap, cusip9, documented, asof, receipt, issues)
        for part, ids, known in surveilled:
            out.append(_segment(cusip9, security, spell_id, part, "nondefault_continuous", "surveillance_receipt",
                                ids, (), known))
        for rest in _subtract(gap, [part for part, _ids, _known in surveilled]):
            out.append(_segment(cusip9, security, spell_id, rest, "unknown", "none", (), (), asof.k))
    return out


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------
COVERAGE_EVENT_TYPES = ("all", "agency_issue_default", "bankruptcy", "default_state", "distressed_exchange",
                        "payment_default")
COVERAGE_STRATA = ("all", "HY", "IG", "unknown")
_DETAIL_SOURCES = frozenset(c.ENUMS["coverage_source"]) - {"all"}
_DETAIL_COHORTS = frozenset(c.ENUMS["exposure_cohort"]) - {"all"}
_IG = frozenset({"AAA", "AA", "A", "BBB"})
_HY = frozenset({"BB", "B", "CCC", "D"})
#: Exact cell key: (period_label, source, event_type, rating_stratum, exposure_cohort).
CellKey = tuple[str, str, str, str, str]
GridKey = tuple[str, dt.date]


def rating_stratum(bucket: str | None) -> str:
    return "IG" if bucket in _IG else "HY" if bucket in _HY else "unknown"


def _nearest_rank(values: Sequence[int], q: float) -> int:
    ordered = sorted(values)
    index = max(0, -(-int(q * 100) * len(ordered) // 100) - 1)
    return ordered[min(index, len(ordered) - 1)]


def interval_outcome(
    interval: Interval, cusip_episodes: Sequence[DefaultEpisode], segments: Sequence[Interval],
) -> str:
    """Whole-interval outcome of one exposure interval: ``nondefault`` when entirely inside
    non-unknown follow-up, ``default`` when entirely inside an episode's certain default span
    ``[upper, resolution]``, else ``unknown`` (potential onset windows of interval-uncertain or
    prevalent episodes stay uncertain mass)."""
    for e in cusip_episodes:
        certain = (e.onset_upper_inclusive - dt.timedelta(days=1), e.resolution_date or dt.date.max)
        if certain[0] <= interval[0] and interval[1] <= certain[1]:
            return "default"
    for e in cusip_episodes:
        window = (e.onset_lower_exclusive or dt.date.min, e.onset_upper_inclusive - dt.timedelta(days=1))
        if window[0] < window[1] and _intersect(interval, window) is not None:
            return "unknown"  # the onset may lie in or before this interval
    if interval_covered(interval, segments):
        return "nondefault"
    return "unknown"


def _start_key(e: DefaultEpisode) -> GridKey:
    """Grid start whose outcome interval ``(end(m), end(m+1)]`` contains the onset upper bound."""
    return e.cusip9, c.add_months(c.month_key(e.onset_upper_inclusive), -1)


def _members(values: Mapping[Any, Iterable[str]] | None, allowed: frozenset[str], label: str) -> dict[Any, frozenset[str]]:
    out: dict[Any, frozenset[str]] = {}
    for key, raw in (values or {}).items():
        items = frozenset([raw] if isinstance(raw, str) else raw)
        if not items <= allowed:
            raise ResolveError(f"{label}_invalid", (str(key),))
        out[key] = items
    return out


def build_coverage(
    panel_grid: Iterable[tuple[str, dt.date]],
    *,
    episodes: Iterable[DefaultEpisode],
    ratings: Iterable[c.RatingGridRow] = (),
    followups: Iterable[c.FollowUp] = (),
    validation_receipt: c.ValidationReceipt | None = None,
    independent_denominators: Mapping[CellKey, int] | None = None,
    unlinked_counts: Mapping[str, int] | None = None,
    source_frontiers: Mapping[str, dt.date | None],
    exposure_sources: Mapping[GridKey, Iterable[str]] | None = None,
    event_sources: Mapping[uuid.UUID, Iterable[str]] | None = None,
    exposure_cohorts: Mapping[GridKey, str] | None = None,
    adjudicated_negative_lags: Iterable[uuid.UUID] = (),
    disabled_event_types: Iterable[str] = ("agency_issue_default",),
    target_month: dt.date,
    knowledge_cutoff: dt.datetime,
) -> tuple[c.CoverageCell, ...]:
    """Coverage cells per ``year x source x event type x rating stratum x exposure cohort``.

    * Exposure units are grid starts ``m < T`` with outcome interval ``(end(m), end(m+1)]``
      (:func:`outcome_interval`); the period is the interval's year. A unit counts as observed
      only when its whole interval is resolved (:func:`interval_outcome`).
    * Stratum per start from its ``public_pit`` bucket (IG/HY, else ``unknown``); an event uses
      the start whose interval contains its onset upper bound. Events after T are outside.
    * ``source``/``exposure_cohort`` detail cells come only from explicit membership
      (``exposure_sources``/``event_sources``, ``exposure_cohorts``); every detail cell is emitted
      next to the ``all`` aggregates. Frontiers and denominators are keyed by the exact cell and
      never borrowed from an aggregate: a missing frontier is ``unavailable``.
    * ``qualified`` only with a qualified receipt issued by K, a positive denominator for the
      exact cell, the cell's source frontier covering the whole period, and no unadjudicated
      negative lag; otherwise ``partial`` (``not_applicable`` without exposure or when disabled).
    * Negative lags (evidence known before the onset upper bound) are excluded from the lag
      summaries and reported as ``negative_lag_pending_adjudication=<n>``.
    """
    k = _utc(knowledge_cutoff)
    t_end = c.month_end(target_month)
    denominators = dict(independent_denominators or {})
    unlinked = dict(unlinked_counts or {})
    disabled = set(disabled_event_types)
    settled_lags = set(adjudicated_negative_lags)
    unit_sources = _members(exposure_sources, _DETAIL_SOURCES, "exposure_source")
    ev_sources = _members(event_sources, _DETAIL_SOURCES, "event_source")
    cohorts = {key: next(iter(v)) for key, v in _members(
        {key: (value,) for key, value in (exposure_cohorts or {}).items()}, _DETAIL_COHORTS, "exposure_cohort").items()}
    receipt_ok = (
        validation_receipt is not None and validation_receipt.verdict == "qualified" and validation_receipt.issued_at <= k
    )
    strata: dict[GridKey, str] = {}
    for row in ratings:
        if row.view_kind == "public_pit":
            strata[(row.cusip_id, row.month)] = rating_stratum(row.bucket)
    units: dict[GridKey, Interval] = {}
    for cusip9, month in panel_grid:
        interval = outcome_interval(month, target_month)
        if interval is not None:
            units[(cusip9, c.month_key(month))] = interval
    all_episodes = [e for e in episodes if e.onset_upper_inclusive <= t_end]
    by_cusip: dict[str, list[DefaultEpisode]] = defaultdict(list)
    for e in all_episodes:
        by_cusip[e.cusip9].append(e)
    segments: dict[str, list[Interval]] = defaultdict(list)
    for f in followups:
        if f.status != "unknown":
            segments[f.cusip9].append((f.interval_start_exclusive, f.interval_end_inclusive))
    unknown_units = {key for key, iv in units.items() if interval_outcome(iv, by_cusip[key[0]], segments[key[0]]) == "unknown"}

    def unit_in(key: GridKey, source: str, stratum: str, cohort: str) -> bool:
        return (
            stratum in ("all", strata.get(key, "unknown"))
            and cohort in ("all", cohorts.get(key, "unknown"))
            and (source == "all" or source in unit_sources.get(key, frozenset()))
        )

    sources = ("all", *sorted({s for v in (*unit_sources.values(), *ev_sources.values()) for s in v}))
    cohort_labels = ("all", *sorted(set(cohorts.values())))
    years = sorted({iv[1].year for iv in units.values()} | {e.onset_upper_inclusive.year for e in all_episodes})
    cells: list[c.CoverageCell] = []
    for year in years:
        period = str(year)
        year_start, year_end = dt.date(year, 1, 1), min(dt.date(year, 12, 31), t_end)
        for source in sources:
            frontier = source_frontiers.get(source)
            for event_type in COVERAGE_EVENT_TYPES:
                for stratum in COVERAGE_STRATA:
                    for cohort in cohort_labels:
                        keys = [key for key, iv in units.items() if iv[1].year == year and unit_in(key, source, stratum, cohort)]
                        events = [
                            e for e in all_episodes
                            if e.onset_upper_inclusive.year == year and event_type in ("all", e.primary_type)
                            and unit_in(_start_key(e), "all", stratum, cohort)
                            and (source == "all" or source in ev_sources.get(e.episode_id, frozenset()))
                        ]
                        cells.append(_coverage_cell(
                            (period, source, event_type, stratum, cohort), keys, events, unknown_units, frontier,
                            (year_start, year_end), receipt_ok, validation_receipt, denominators.get(
                                (period, source, event_type, stratum, cohort)),
                            unlinked.get(period, 0) if (source, event_type, cohort) == ("all", "all", "all")
                            and stratum in ("all", "unknown") else 0,
                            event_type in disabled, settled_lags,
                        ))
    return tuple(sorted(cells, key=lambda x: x.key()))


def _coverage_cell(
    key: CellKey, keys: Sequence[GridKey], events: Sequence[DefaultEpisode], unknown_units: set[GridKey],
    frontier: dt.date | None, bounds: tuple[dt.date, dt.date], receipt_ok: bool,
    receipt: c.ValidationReceipt | None, reference: int | None, n_unlinked: int, disabled: bool,
    settled_lags: set[uuid.UUID],
) -> c.CoverageCell:
    period, source, event_type, stratum, cohort = key
    lags = {e.episode_id: (e.evidence_known_at.date() - e.onset_upper_inclusive).days for e in events}
    positive = [lag for lag in lags.values() if lag >= 0]
    pending = sum(1 for eid, lag in lags.items() if lag < 0 and eid not in settled_lags)
    count: int | None
    if not keys or disabled:
        state, basis, count = "not_applicable", "none", None
        rationale = "event stream disabled" if disabled else "no panel exposure in cell"
    elif frontier is None or frontier < bounds[0]:
        state, basis, count = "unavailable", "none", None
        rationale = "no source frontier for this cell" if frontier is None else "no source coverage of period"
    elif receipt_ok and reference and frontier >= bounds[1] and not pending:
        state, basis, count = "qualified", "independent_reference_enumeration", reference
        rationale = "qualified receipt and independent reference enumeration for the exact cell"
    else:
        state, basis, count = "partial", "panel_exposure", len({x for x, _m in keys})
        rationale = "public-source evidence without an exact-cell qualified denominator"
    if pending:
        rationale += f"; negative_lag_pending_adjudication={pending}"
    return c.CoverageCell(
        period_label=period, source=source, event_type=event_type, rating_stratum=stratum, exposure_cohort=cohort,
        state=state, denominator_basis=basis, denominator_count=count, exposed_issue_months=len(keys),
        event_count=len(events), unlinked_count=n_unlinked,
        date_uncertain_count=sum(1 for e in events if e.timing_class != "incident"),
        unknown_outcome_issue_months=sum(1 for x in keys if x in unknown_units),
        source_frontier=frontier,
        lag_p50_days=_nearest_rank(positive, 0.5) if positive else None,
        lag_p90_days=_nearest_rank(positive, 0.9) if positive else None,
        lag_max_days=max(positive) if positive else None,
        rationale=rationale,
        validation_receipt_digest=receipt.digest() if state == "qualified" and receipt else None,
    )
