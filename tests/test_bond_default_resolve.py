"""W2a: pure temporal/adjudication resolution (links, episodes, follow-up, coverage). SYNTHETIC data."""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.bonds.default_events import contracts as c
from src.bonds.default_events import resolve as r

HERE = Path(__file__).resolve().parent / "fixtures" / "bond_default_events" / "resolve"


def _load_builders():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location("bond_default_resolve_builders", HERE / "builders.py")
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


b = _load_builders()
syn_replace = b.syn.replace_row
UTC = dt.timezone.utc
K = b.K
A, B, C, D = b.cusip(41), b.cusip(42), b.cusip(43), b.cusip(44)


def _links(requests, observations, adjudications=(), *, k=K, mode="current_run"):  # type: ignore[no-untyped-def]
    return r.resolve_links(requests, observations=observations, adjudications=adjudications,
                           link_package=b.LINKS, knowledge_cutoff=k, knowledge_mode=mode)


# ---------------------------------------------------------------------------
# resolve_links
# ---------------------------------------------------------------------------
def test_nport_identity_admits_issue_link_with_max_known_time() -> None:
    o = b.nport("row-000001", A, dt.date(2026, 3, 31), "Y")
    late_identity = dt.datetime(2026, 9, 23, tzinfo=UTC)
    res = _links([b.request(o, A, "nport_cusip_identity", known=late_identity)], [o])
    (link,) = res.admitted
    assert link.status == "admitted" and link.cusip9 == A and link.affected_scope == "issue"
    assert link.link_known_at == max(o.public_available_at, late_identity)
    assert res.issues == ()


def test_nport_identity_contradicting_cusip_is_rejected() -> None:
    o = b.nport("row-000002", A, dt.date(2026, 3, 31), "Y")
    res = _links([b.request(o, B, "nport_cusip_identity")], [o])
    assert res.admitted == ()
    assert [x.status for x in res.links] == ["rejected"]
    assert res.issues[0].reason == "cusip_contradicts_observation"


@pytest.mark.parametrize("basis", ["cusip6_prefix", "issuer_cik_match", "lei_match", "name_match"])
def test_suspected_identity_bases_are_quarantined(basis: str) -> None:
    o = b.passage("p-susp", cusip9=None)
    res = _links([b.request(o, A, basis, scope="issuer_affected_obligation")], [o])
    assert res.admitted == () and [x.status for x in res.links] == ["quarantined"]
    assert res.issues[0].reason == f"suspected_identity_basis:{basis}"


def test_document_stated_cusip_admits_only_the_stated_issue() -> None:
    o = b.passage("p-stated", cusip9=A)
    res = _links([b.request(o, A, "document_stated_cusip"), b.request(o, B, "document_stated_cusip")], [o])
    assert [x.cusip9 for x in res.admitted] == [A]
    assert {i.reason for i in res.issues} == {"cusip_contradicts_observation"}


def test_issuer_level_evidence_never_propagates_without_adjudicated_scope() -> None:
    o = b.passage("p-issuer", cusip9=None)
    reqs = [b.request(o, x, "document_stated_cusip", scope="issuer_affected_obligation") for x in (A, B, C)]
    res = _links(reqs, [o])
    assert res.admitted == ()
    assert {x.status for x in res.links} == {"quarantined"}
    assert {i.reason for i in res.issues} == {"issuer_level_evidence_without_adjudicated_scope"}


STANDALONE = r.STANDALONE_DECISION_NOT_PERSISTABLE
CANDIDATE = r.CANDIDATE_IS_NOT_A_DECISION


def _cite(link_id):  # type: ignore[no-untyped-def]
    """Stand-in carrying only ``link_id`` (to cite a prospective admitted revision)."""
    return SimpleNamespace(link_id=link_id)


def _chain(reqs, obs, doc, cusip9, label, *, evidence=(), links=(), k=K, mode="current_run",  # type: ignore[no-untyped-def]
           at=b.ADJ_AT, prior_links=(), head_at=None):
    """Scope decided inside an episode's own chain: the scope record cites the quarantined proposal;
    the episode's accepting revision supersedes it and cites the admitted revision (the
    ``prospective_link_id`` reported while the decision was still standalone)."""
    proposal = next(x for x in _links(reqs, obs, k=k, mode=mode).links
                    if x.cusip9 == cusip9 and x.observation_id == doc.observation_id)
    subject = b.episode_id(label)
    scope = b.adjudicate(subject, "accepted_event", [doc, *evidence], [proposal, *links], at=at)
    standalone = r.resolve_links(reqs, observations=obs, adjudications=[scope], link_package=b.LINKS,
                                 knowledge_cutoff=k, knowledge_mode=mode, links=prior_links)
    found = [i for i in standalone.issues if i.cusip9 == cusip9 and i.observation_id == doc.observation_id]
    if not found or found[0].reason != STANDALONE:
        return standalone, scope, None  # the decision itself fails before the chain rule
    assert found[0].prospective_link_id is not None and standalone.admitted == ()
    head = b.adjudicate(subject, "accepted_event", [doc, *evidence], [_cite(found[0].prospective_link_id), *links],
                        at=head_at or at + dt.timedelta(minutes=10), supersedes=scope)
    resolved = r.resolve_links(reqs, observations=obs, adjudications=[scope, head], link_package=b.LINKS,
                               knowledge_cutoff=k, knowledge_mode=mode, links=prior_links)
    return resolved, scope, head


def test_adjudicated_scope_admits_only_the_cited_link_and_uses_adjudication_time() -> None:
    """Retain the baseline node id; public dependency time now excludes review time."""
    o = b.passage("p-scope", cusip9=None)
    reqs = [b.request(o, x, "adjudicated_issue_scope", scope="issuer_affected_obligation") for x in (A, B)]
    first = _links(reqs, [o])
    proposal_a = next(x for x in first.links if x.cusip9 == A)
    assert proposal_a.status == "quarantined" and first.admitted == ()
    second, _scope, head = _chain(reqs, [o], o, A, "scope")
    (admitted,) = second.admitted
    assert admitted.cusip9 == A and admitted.supersedes_link_id == proposal_a.link_id
    assert head is not None and admitted.link_id in head.link_ids and proposal_a.link_id not in head.link_ids
    assert admitted.link_known_at == max(o.public_available_at, b.LINK_AT)
    assert [i.reason for i in second.issues] == ["scope_adjudication_missing"]  # B stays quarantined


def test_adjudicated_scope_ignores_non_human_late_candidate_standalone_or_retracted_decisions() -> None:
    o = b.passage("p-scope2", cusip9=None)
    req = b.request(o, A, "adjudicated_issue_scope", scope="issuer_affected_obligation")
    proposal = _links([req], [o]).links[0]
    subject = b.episode_id("scope2")
    late = b.adjudicate(subject, "accepted_event", [o], [proposal], at=K + dt.timedelta(hours=1))
    assert _links([req], [o], [late]).admitted == ()
    engine = b.adjudicate(subject, "accepted_state", [o], [proposal], role="policy_rule_engine")
    assert _links([req], [o], [engine]).issues[0].reason == "scope_adjudication_not_human"
    pending = b.adjudicate(subject, "candidate", [o], [proposal], subject_kind="candidate")
    assert _links([req], [o], [pending]).issues[0].reason == CANDIDATE
    human = b.adjudicate(subject, "accepted_event", [o], [proposal])
    assert _links([req], [o], [human]).issues[0].reason == STANDALONE  # its own head: not persistable
    retract = b.adjudicate(subject, "retracted", [o], [proposal], supersedes=human, at=b.ADJ_AT + dt.timedelta(hours=1))
    res = _links([req], [o], [human, retract])
    assert res.admitted == () and res.issues[0].reason == "scope_adjudication_status_retracted"
    retract_bare = b.adjudicate(subject, "retracted", [o], [], supersedes=human, at=b.ADJ_AT + dt.timedelta(hours=1))
    assert _links([req], [o], [human, retract_bare]).issues[0].reason == "scope_adjudication_not_effective"
    # Chain head retracted after K is not known in the K snapshot; retracted by K it is.
    in_chain, scope, head = _chain([req], [o], o, A, "scope2b")
    assert len(in_chain.admitted) == 1 and head is not None
    head_retract = b.adjudicate(head.subject_id, "retracted", [], [], supersedes=head, at=b.ADJ_AT + dt.timedelta(hours=1))
    assert _links([req], [o], [scope, head, head_retract]).admitted == ()
    assert len(_links([req], [o], [scope, head, head_retract], k=b.ADJ_AT + dt.timedelta(minutes=30)).admitted) == 1


def test_reconstruction_link_time_excludes_present_day_adjudication() -> None:
    o = b.passage("p-scope3", cusip9=None)
    req = b.request(o, A, "adjudicated_issue_scope", scope="issuer_affected_obligation",
                    known=dt.datetime(2026, 5, 20, tzinfo=UTC))
    k_hist = dt.datetime(2026, 6, 1, tzinfo=UTC)
    res, _scope, _head = _chain([req], [o], o, A, "scope3", k=k_hist, mode="historical_reconstruction")
    (admitted,) = res.admitted
    assert admitted.link_known_at == dt.datetime(2026, 5, 20, tzinfo=UTC)


def _scope_with_support(support: c.CreditObservation, *, k=K, mode="current_run", extra_links=()):  # type: ignore[no-untyped-def]
    o = b.passage("p-scope4", cusip9=None)
    req = b.request(o, A, "adjudicated_issue_scope", scope="issuer_affected_obligation")
    res, scope, _head = _chain([req], [o, support], o, A, "scope4", evidence=(support,), links=extra_links, k=k,
                               mode=mode, prior_links=extra_links)
    return res, o, scope


def test_scope_decision_relies_on_full_dependency_set() -> None:
    # Identity passage (e.g. indenture exhibit) public after K: the decision's support is unusable.
    post_k = b.passage("p-ident-late", cusip9=None, public=K + dt.timedelta(days=1))
    res, _o, _s = _scope_with_support(post_k)
    assert res.admitted == () and res.issues[0].reason == "scope_dependency_unusable:observation_public_after_cutoff"
    # Superseded/retracted support is refused as well.
    original = b.passage("p-ident-orig", cusip9=None)
    retraction = b.passage("p-ident-retr", cusip9=None, revision="retraction", supersedes=original,
                           public=dt.datetime(2026, 9, 1, tzinfo=UTC))
    o = b.passage("p-scope5", cusip9=None)
    req = b.request(o, A, "adjudicated_issue_scope", scope="issuer_affected_obligation")
    proposal = _links([req], [o, original, retraction]).links[0]
    scope = b.adjudicate(b.episode_id("scope5"), "accepted_event", [o, original], [proposal])
    res2 = _links([req], [o, original, retraction], [scope])
    assert res2.admitted == () and res2.issues[0].reason == "scope_dependency_unusable:observation_superseded_or_retracted"
    # A cited support link missing from the inventory is refused.
    ghost = b.request(original, B, "document_stated_cusip")
    ghost_link = r._make_link(b.LINKS, ghost, status="admitted", known_at=b.LINK_AT, rationale="ghost")
    res3, _o, _s = _scope_with_support(b.passage("p-ident-ok", cusip9=None), extra_links=())
    assert len(res3.admitted) == 1
    o6 = b.passage("p-scope6", cusip9=None)
    req6 = b.request(o6, A, "adjudicated_issue_scope", scope="issuer_affected_obligation")
    proposal6 = _links([req6], [o6]).links[0]
    scope6 = b.adjudicate(b.episode_id("scope6"), "accepted_event", [o6], [proposal6, ghost_link])
    assert _links([req6], [o6], [scope6]).issues[0].reason == "scope_dependency_unusable:link_outside_inventory"


def test_scope_link_known_time_is_max_over_relied_set_and_mode_policy() -> None:
    support = b.passage("p-ident-sep", cusip9=None, public=dt.datetime(2026, 9, 21, 18, 0, tzinfo=UTC))
    res, o, _scope = _scope_with_support(support)
    (admitted,) = res.admitted
    assert admitted.link_known_at == max(o.public_available_at, b.LINK_AT, support.public_available_at)
    # Historical reconstruction: contemporaneous public support governs; present-day review excluded.
    hist, _o, _s = _scope_with_support(support, mode="historical_reconstruction")
    assert hist.admitted[0].link_known_at == max(o.public_available_at, b.LINK_AT, support.public_available_at)
    # current_run additionally requires relied observations ingested by K.
    not_ingested = b.passage("p-ident-ing", cusip9=None)
    not_ingested = syn_replace(not_ingested, first_seen_at=K + dt.timedelta(hours=2))
    res_ing, _o, _s = _scope_with_support(not_ingested)
    assert res_ing.issues[0].reason == "scope_dependency_unusable:observation_not_ingested_by_cutoff"
    hist_ing, _o, _s = _scope_with_support(not_ingested, mode="historical_reconstruction")
    assert len(hist_ing.admitted) == 1


def test_phase1_scope_review_cutoff_is_separate_from_public_link_time() -> None:
    o = b.passage("p-scope-review-boundary", cusip9=None)
    identity_known_at = dt.datetime(2026, 5, 20, tzinfo=UTC)
    req = b.request(
        o, A, "adjudicated_issue_scope", scope="issuer_affected_obligation", known=identity_known_at,
    )
    at_k, _scope, _head = _chain([req], [o], o, A, "scope-review-at-k", at=K, head_at=K)
    assert len(at_k.admitted) == 1
    assert at_k.admitted[0].link_known_at == max(o.public_available_at, identity_known_at)

    after_k, _scope, _head = _chain(
        [req], [o], o, A, "scope-review-after-k", at=K - dt.timedelta(minutes=1),
        head_at=K + dt.timedelta(seconds=1),
    )
    assert after_k.admitted == ()
    assert all(issue.reason != "link_known_after_cutoff" for issue in after_k.issues)

    historical, _scope, _head = _chain(
        [req], [o], o, A, "scope-review-historical", k=dt.datetime(2026, 6, 1, tzinfo=UTC),
        mode="historical_reconstruction", at=K, head_at=K,
    )
    assert historical.admitted[0].link_known_at == max(o.public_available_at, identity_known_at)


def test_unusable_observations_and_late_identity_emit_no_link() -> None:
    late = b.passage("p-late", cusip9=A, public=K + dt.timedelta(days=1))
    original = b.passage("p-orig", cusip9=A)
    retraction = b.passage("p-retract", cusip9=A, revision="retraction", supersedes=original,
                           public=dt.datetime(2026, 9, 1, tzinfo=UTC))
    ok = b.passage("p-ok", cusip9=A)
    reqs = [b.request(x, A, "document_stated_cusip") for x in (late, original, retraction)]
    reqs.append(b.request(ok, A, "document_stated_cusip", known=K + dt.timedelta(seconds=1)))
    res = _links(reqs, [late, original, retraction, ok])
    assert res.links == ()
    assert sorted(i.reason for i in res.issues) == [
        "link_known_after_cutoff", "observation_public_after_cutoff",
        "observation_superseded_or_retracted", "observation_superseded_or_retracted",
    ]


def test_link_resolution_is_deterministic_under_input_order() -> None:
    obs = [b.nport(f"row-00010{i}", x, dt.date(2026, 3, 31), "Y") for i, x in enumerate((A, B, C))]
    reqs = [b.request(o, o.cusip9, "nport_cusip_identity") for o in obs]
    assert _links(reqs, obs) == _links(list(reversed(reqs)), list(reversed(obs)))


# ---------------------------------------------------------------------------
# resolve_episodes
# ---------------------------------------------------------------------------
Q1, Q4, Q3_26 = dt.date(2025, 12, 31), dt.date(2025, 9, 30), dt.date(2026, 3, 31)


_AUTO_FRAMES = object()


class World:
    """Observations + resolved links for one synthetic scenario."""

    def __init__(self) -> None:
        self.obs: list[c.CreditObservation] = []
        self.reqs: list[r.LinkRequest] = []
        self.adjs: list[c.Adjudication] = []
        self.extra_links: list[c.EventLink] = []

    def all_links(self) -> tuple[c.EventLink, ...]:
        found = {x.link_id: x for x in (*self.links().links, *self.extra_links)}
        return tuple(found[k] for k in sorted(found, key=str))

    def dependency_frames(self, links=(), *, k=K, mode="current_run", **kw):  # type: ignore[no-untyped-def]
        link_rows = tuple(links) if links else self.all_links()
        eligible = r.adjudications_as_of(tuple(self.adjs), k, mode)
        return b.dependency_frames(
            observations=tuple(self.obs), event_links=link_rows, adjudications=eligible.values(), **kw,
        )

    def nport(self, cusip9: str, day: dt.date, flag: str, n: int = 1, **kw):  # type: ignore[no-untyped-def]
        o = b.nport(f"row-{cusip9[-4:]}{day:%y%m}{n}", cusip9, day, flag, series=f"S99999900{n}",
                    family=f"SYN-FAMILY-{n}", cik=f"999999991{n}", **kw)
        self.obs.append(o)
        self.reqs.append(b.request(o, cusip9, "nport_cusip_identity"))
        return o

    def doc(self, locator: str, cusip9: str | None, *, scope: str = "issue", link_cusips=None, **kw):  # type: ignore[no-untyped-def]
        o = b.passage(locator, cusip9=cusip9, **kw)
        self.obs.append(o)
        for x, s in ((cusip9, scope),) if link_cusips is None else link_cusips:
            self.reqs.append(b.request(o, x, "document_stated_cusip", scope=s))
        return o

    def links(self) -> r.LinkResolution:
        return _links(self.reqs, self.obs, self.adjs)

    def link(self, o: c.CreditObservation, cusip9: str) -> c.EventLink:
        return next(x for x in self.links().admitted if x.observation_id == o.observation_id and x.cusip9 == cusip9)

    def accept(self, label: str, status: str, evidence, cusip9: str, **kw):  # type: ignore[no-untyped-def]
        a = b.adjudicate(b.episode_id(label), status, evidence, [self.link(o, cusip9) for o in evidence], **kw)
        self.adjs.append(a)
        return a

    def resolve(self, *, proposals=(), claims=(), strict: bool = False, k=K, mode="current_run",
                frames=_AUTO_FRAMES, proposal_evidence=()):  # type: ignore[no-untyped-def]
        links = self.all_links()
        supplied_frames = self.dependency_frames(
            links, k=k, mode=mode, proposal_evidence=proposal_evidence,
        ) if frames is _AUTO_FRAMES else frames
        return r.resolve_episodes(
            tuple(self.adjs), observations=tuple(self.obs), links=links, packages=tuple(b.PACKAGES),
            state_proposals=tuple(proposals), claims=tuple(claims), knowledge_cutoff=k, knowledge_mode=mode,
            strict=strict, frames=supplied_frames,
        )


def _proposal(cusip9: str, y: list, n: list | None = None, **kw):  # type: ignore[no-untyped-def]
    from src.bonds.default_events.nport import StateProposal

    lower = n[0].report_date if n else None
    upper = y[0].report_date
    ids = c.sorted_uuids(o.observation_id for o in [*y, *(n or [])])
    values = {
        "cusip9": cusip9, "proposed_status": "accepted_state", "reviewer_role": "policy_rule_engine",
        "basis": "two_independent_families", "onset_lower_exclusive": lower, "onset_upper_inclusive": upper,
        "timing_class": c.derive_timing_class(lower, upper), "left_censored": lower is None,
        "evidence_observation_ids": ids,
        "evidence_known_at": max(o.public_available_at for o in [*y, *(n or [])]),
        "subsequent_y_dates": (), "credible_n_after_first_y": (), "conflict_dates": (), "candidate_y_dates": (),
        "onset_lower_evidence_ids": c.sorted_uuids(o.observation_id for o in n or []),
        "onset_upper_evidence_ids": c.sorted_uuids(o.observation_id for o in y),
    }
    values.update(kw)
    return StateProposal(**values)


def _state_world(prior_n: bool = True):  # type: ignore[no-untyped-def]
    w = World()
    n = [w.nport(A, Q1, "N", 1), w.nport(A, Q1, "N", 2)] if prior_n else []
    y = [w.nport(A, Q3_26, "Y", 1), w.nport(A, Q3_26, "Y", 2)]
    return w, y, n


def _state_claim(y: list, n: list | None = None, label: str = "state-A", **kw):  # type: ignore[no-untyped-def]
    """Explicit human ``default_state`` claim. Phase 1 admits N-PORT states only this way: a
    proposal-backed consensus state fails closed (``proposal_evidence_not_closed``) until persisted
    proposal/family evidence exists, and the resulting episode never carries the consensus flag."""
    return r.EpisodeClaim(
        b.episode_id(label), "default_state",
        onset_lower_exclusive=n[0].report_date if n else None, onset_upper_inclusive=y[0].report_date,
        onset_lower_evidence_ids=tuple(o.observation_id for o in n or ()), **kw,
    )


def test_accepted_state_uses_prior_n_interval_without_consensus_flag() -> None:
    w, y, n = _state_world()
    w.accept("state-A", "accepted_state", y + n, A)
    (e,) = w.resolve(claims=[_state_claim(y, n)], strict=True).episodes
    assert (e.onset_lower_exclusive, e.onset_upper_inclusive, e.timing_class) == (Q1, Q3_26, "interval_uncertain")
    assert e.primary_type == "default_state" and e.admission_status == "accepted_state"
    assert e.corroboration_flags == ()
    assert set(e.onset_lower_evidence_ids) == {o.observation_id for o in n}
    assert e.evidence_known_at == max(max(o.public_available_at for o in y + n), e.link_known_at)


def test_first_y_without_prior_n_is_prevalent_with_null_lower_bound() -> None:
    w, y, _ = _state_world(prior_n=False)
    w.accept("state-A", "accepted_state", y, A)
    (e,) = w.resolve(claims=[_state_claim(y)], strict=True).episodes
    assert e.onset_lower_exclusive is None and e.timing_class == "prevalent" and e.onset_lower_evidence_ids == ()


def test_subsequent_y_and_n_after_y_do_not_create_episodes_or_cure() -> None:
    w, y, _ = _state_world(prior_n=False)
    later_y = w.nport(A, dt.date(2026, 6, 30), "Y", 1)
    later_n = w.nport(A, dt.date(2026, 6, 30), "N", 2)
    engine = w.accept("state-A", "accepted_state", y, A, role="policy_rule_engine")
    prop = _proposal(A, y, subsequent_y_dates=(later_y.report_date,), credible_n_after_first_y=(later_n.report_date,))
    assert _reasons(w.resolve(proposals=[prop])) == ["proposal_evidence_not_closed"]  # phase 1: no proposal-backed state
    claim = _state_claim(y, resolution_date=later_n.report_date, resolution_refs=(later_n.observation_id,))
    assert [i.reason for i in w.resolve(claims=[claim]).issues] == ["human_review_required:explicit_state_claim"]
    w.adjs.append(b.adjudicate(engine.subject_id, "accepted_state", y, [w.link(o, A) for o in y], supersedes=engine,
                               at=b.ADJ_AT + dt.timedelta(hours=1)))
    # Later Y/N votes neither open another episode nor close this one.
    assert len(w.resolve(claims=[_state_claim(y)], strict=True).episodes) == 1
    res = w.resolve(claims=[claim])
    assert res.episodes == () and [i.reason for i in res.issues] == ["nport_n_is_not_cure_evidence"]


def test_proposal_alone_or_candidate_adjudication_never_self_approves() -> None:
    w, y, n = _state_world()
    assert w.resolve(proposals=[_proposal(A, y, n)], strict=True).episodes == ()
    w.accept("state-A", "candidate", y + n, A, role="extraction_proposer")
    res = w.resolve(proposals=[_proposal(A, y, n)], strict=True)
    assert res.episodes == () and res.stats["effective:candidate"] == 1


def test_state_adjudication_must_cover_proposal_evidence_and_conflicts_need_human() -> None:
    w, y, n = _state_world()
    w.accept("state-A", "accepted_state", y, A, role="policy_rule_engine")
    assert [i.reason for i in w.resolve(proposals=[_proposal(A, y, n)]).issues] == [
        "adjudication_does_not_cover_proposal_evidence"]
    w2, y2, n2 = _state_world()
    w2.accept("state-A", "accepted_state", y2 + n2, A, role="policy_rule_engine")
    conflicted = _proposal(A, y2, n2, conflict_dates=(dt.date(2025, 6, 30),))
    assert [i.reason for i in w2.resolve(proposals=[conflicted]).issues] == ["human_review_required:conflicting_sources"]
    with pytest.raises(r.ResolveError, match="human_review_required"):
        w2.resolve(proposals=[conflicted], strict=True)


def test_proposal_lineage_not_explained_by_persisted_closure_fails_closed() -> None:
    from src.bonds.default_events.nport import FamilyEvidence

    w, y, n = _state_world()
    w.accept("state-A", "accepted_state", y + n, A, role="policy_rule_engine")
    late = dt.datetime(2026, 9, 24, 0, 0, tzinfo=UTC)  # e.g. an N-CEN mapping known after every vote/link
    res = w.resolve(proposals=[_proposal(A, y, n, evidence_known_at=late)])
    assert res.episodes == () and _reasons(res) == ["proposal_lineage_not_persistable"]
    family = FamilyEvidence(registrant_cik="9999999911", family_id="SYN-FAMILY-1", evidence_ref="synthetic:ncen:1",
                            evidence_digest=c.digest_of({"ncen": 1}), valid_from=None, valid_to=None,
                            public_available_at=b.RETRIEVED_AT, known_at=b.RETRIEVED_AT)
    assert _reasons(w.resolve(proposals=[_proposal(A, y, n, family_evidence=(family,))])) == [
        "proposal_lineage_not_persistable"]
    corroborated = _proposal(A, y, n, corroboration_adjudication_ids=(b.episode_id("corr"),))
    with pytest.raises(r.ResolveError, match="proposal_lineage_not_persistable"):
        w.resolve(proposals=[corroborated], strict=True)
    # An explicit human claim does not rely on the proposal, but never carries its consensus flag.
    human = b.adjudicate(b.episode_id("state-A"), "accepted_state", y + n, [w.link(o, A) for o in y + n],
                         supersedes=w.adjs[0], at=b.ADJ_AT + dt.timedelta(hours=1))
    w.adjs.append(human)
    claim = r.EpisodeClaim(b.episode_id("state-A"), "default_state", onset_lower_exclusive=Q1,
                           onset_upper_inclusive=Q3_26, onset_lower_evidence_ids=tuple(o.observation_id for o in n))
    (e,) = w.resolve(proposals=[_proposal(A, y, n, family_evidence=(family,))], claims=[claim], strict=True).episodes
    assert e.corroboration_flags == ()


def _event_world(kind_facts: r.EventFacts, primary: str, *, effective=dt.date(2026, 5, 15)):  # type: ignore[no-untyped-def]
    w = World()
    d = w.doc("p-evt", B, effective=effective)
    w.accept("evt-B", "accepted_event", [d], B)
    claim = r.EpisodeClaim(b.episode_id("evt-B"), primary, onset_date=effective,
                           onset_lower_evidence_ids=(d.observation_id,), facts=kind_facts)
    return w, d, claim


@pytest.mark.parametrize(("facts", "reason"), [
    (r.EventFacts(acceleration="technical_covenant"), "technical_covenant_acceleration_nonqualifying"),
    (r.EventFacts(missed_payment=True, cured_within_grace=True), "cured_within_grace_nonqualifying"),
    (r.EventFacts(missed_payment=True), "inside_grace_payment_is_candidate"),
    (r.EventFacts(missed_payment=True, grace_period_expired_uncured=True, arrears_or_legal_deferral_only=True),
     "arrears_or_legal_deferral_unknown_never_default"),
    (r.EventFacts(missed_payment=True, grace_period_expired_uncured=True, pik_only=True), "pik_never_default"),
])
def test_payment_default_admission_gate(facts: r.EventFacts, reason: str) -> None:
    w, _, claim = _event_world(facts, "payment_default")
    res = w.resolve(claims=[claim])
    assert res.episodes == () and [i.reason for i in res.issues] == [reason]


def test_uncured_grace_end_payment_default_is_incident_event() -> None:
    w, _d, claim = _event_world(r.EventFacts(missed_payment=True, grace_period_expired_uncured=True), "payment_default")
    (e,) = w.resolve(claims=[claim], strict=True).episodes
    assert e.onset_date == dt.date(2026, 5, 15) and e.timing_class == "incident"
    assert e.corroboration_flags == ("edgar_document", "payment_default")


@pytest.mark.parametrize(("facts", "reason"), [
    (r.EventFacts(proceeding="plan_confirmation", debtor_verified=True, affected_obligation_verified=True),
     "bankruptcy_plan_confirmation_not_admissible"),
    (r.EventFacts(proceeding="bankruptcy_petition", debtor_verified=True),
     "bankruptcy_debtor_and_affected_obligation_required"),
    (r.EventFacts(proceeding="receivership", debtor_verified=True, affected_obligation_verified=True), None),
])
def test_bankruptcy_admission_gate(facts: r.EventFacts, reason: str | None) -> None:
    w, _, claim = _event_world(facts, "bankruptcy")
    res = w.resolve(claims=[claim])
    assert [i.reason for i in res.issues] == ([reason] if reason else [])
    assert len(res.episodes) == (0 if reason else 1)


COMPLETED = r.EventFacts(exchange_stage="completed", coercive_impairment=True)
X_DAY = dt.date(2026, 5, 15)


def _exchange_world(*, new_valid_from: dt.date = dt.date(2020, 1, 1)):  # type: ignore[no-untyped-def]
    w = World()
    old = w.doc("p-exch-old", B, effective=X_DAY, scope="exchange_old", document="exch-8k")
    new = b.passage("p-exch-new", cusip9=C, document="exch-8k")
    w.obs.append(new)
    w.reqs.append(b.request(new, C, "document_stated_cusip", scope="exchange_new", valid_from=new_valid_from))
    claim = r.EpisodeClaim(b.episode_id("exch-B"), "distressed_exchange", onset_date=X_DAY,
                           onset_lower_evidence_ids=(old.observation_id,), facts=COMPLETED)
    return w, old, new, claim


def _reason_list(w, claims):  # type: ignore[no-untyped-def]
    return [i.reason for i in w.resolve(claims=claims).issues]


def test_exchange_shared_document_hash_is_not_a_reviewed_pairing() -> None:
    w, old, _new, claim = _exchange_world()
    w.accept("exch-B", "accepted_event", [old], B)  # cites the old side only
    res = w.resolve(claims=[claim])
    assert res.episodes == () and res.alias_spells == {} and _reason_list(w, [claim]) == ["exchange_pairing_unreviewed"]
    proposed = r.EpisodeClaim(claim.episode_id, "distressed_exchange", onset_date=X_DAY,
                              onset_lower_evidence_ids=(old.observation_id,), facts=r.EventFacts(exchange_stage="proposed"))
    assert _reason_list(w, [proposed]) == ["proposed_exchange_is_candidate"]
    w2, _, claim2 = _event_world(COMPLETED, "distressed_exchange")
    assert _reason_list(w2, [claim2]) == ["exchange_old_new_links_required"]


def test_valid_exchange_pairing_fails_closed_as_not_persistable() -> None:
    w, old, new, claim = _exchange_world()
    pairing = b.adjudicate(claim.episode_id, "accepted_event", [old, new], [w.link(old, B), w.link(new, C)])
    w.adjs.append(pairing)
    res = w.resolve(claims=[claim])
    assert res.episodes == () and res.alias_spells == {}
    assert [i.reason for i in res.issues] == ["exchange_pairing_not_persistable"]
    with pytest.raises(r.ResolveError, match="exchange_pairing_not_persistable"):
        w.resolve(claims=[claim], strict=True)
    assert not r.w0_event_closure_persistable(B, b.security_id(B), [w.link(old, B), w.link(new, C)])


def test_exchange_pairing_checks_new_side_usability_validity_and_effectiveness() -> None:
    w, old, new, claim = _exchange_world(new_valid_from=dt.date(2026, 6, 1))
    w.adjs.append(b.adjudicate(claim.episode_id, "accepted_event", [old, new], [w.link(old, B), w.link(new, C)]))
    assert _reason_list(w, [claim]) == ["exchange_new_link_not_valid_at_exchange"]
    # New-side document retracted by K: the pairing's dependency is unusable.
    w2, old2, new2, claim2 = _exchange_world()
    new_link = w2.link(new2, C)
    retraction = b.passage("p-exch-new-retr", cusip9=C, revision="retraction", supersedes=new2,
                           public=dt.datetime(2026, 9, 23, tzinfo=UTC))
    w2.extra_links.append(new_link)
    w2.obs.append(retraction)
    w2.adjs.append(b.adjudicate(claim2.episode_id, "accepted_event", [old2, new2], [w2.link(old2, B), new_link]))
    assert _reason_list(w2, [claim2]) == ["exchange_pairing_dependency_unusable:observation_superseded_or_retracted"]
    # A separate pairing record that was later retracted is not effective.
    w3, old3, new3, claim3 = _exchange_world()
    w3.accept("exch-B", "accepted_event", [old3], B)
    pair = b.adjudicate(b.episode_id("pair"), "accepted_event", [old3, new3], [w3.link(old3, B), w3.link(new3, C)])
    retract = b.adjudicate(pair.subject_id, "retracted", [], [], supersedes=pair, at=b.ADJ_AT + dt.timedelta(hours=1))
    w3.adjs += [pair, retract]
    assert _reason_list(w3, [claim3]) == ["exchange_pairing_not_effective"]


def test_w0_rejects_new_side_link_in_event_closure() -> None:
    from src.bonds.default_events import publication as p

    w, old, new, _claim = _exchange_world()
    pay = r.EpisodeClaim(b.episode_id("x-pay"), "payment_default", onset_date=X_DAY, facts=PAY,
                         onset_lower_evidence_ids=(old.observation_id,))
    w.accept("x-pay", "accepted_event", [old], B)
    (event,) = w.resolve(claims=[pay], strict=True).episodes
    new_link = w.link(new, C)
    links = c.sorted_uuids([*event.link_ids, new_link.link_id])
    evidence = c.sorted_uuids([*event.evidence_observation_ids, new.observation_id])
    edges = {
        "evidence_observation_ids": evidence,
        "onset_lower_evidence_ids": event.onset_lower_evidence_ids,
        "link_ids": links,
        "adjudication_ids": event.adjudication_ids,
        "proposal_evidence_ids": (),
        "exchange_relation_ids": (),
    }
    frames = w.dependency_frames()
    strict_digest = p.event_dependency_digest(
        frames, knowledge_cutoff=K, knowledge_mode="current_run", **edges,
    )
    derived = p.derive_event_fields(
        frames, knowledge_cutoff=K, knowledge_mode="current_run", **edges,
    )
    assert derived["dependency_digest"] == strict_digest
    widened = syn_replace(event, **derived)
    grid = [(x, m) for x in (B, C) for m in MONTHS]
    with pytest.raises(c.ContractError, match="event_evidence_invalid"):
        p.check_bundle(_bundle(w, [widened], (), (), grid))


def test_overlapping_spells_rejected_but_independently_cured_new_episode_kept() -> None:
    w = World()
    d1 = w.doc("p-evt1", B, effective=dt.date(2026, 1, 15))
    cure = w.doc("p-cure", B, effective=dt.date(2026, 2, 1), kind="issuer_document_passage",
                 public=dt.datetime(2026, 2, 3, tzinfo=UTC))
    d2 = w.doc("p-evt2", B, effective=dt.date(2026, 5, 15))
    w.accept("evt-1", "accepted_event", [d1], B)
    w.accept("evt-2", "accepted_event", [d2], B)
    pay = r.EventFacts(missed_payment=True, grace_period_expired_uncured=True)

    def claim(label, doc, **kw):  # type: ignore[no-untyped-def]
        return r.EpisodeClaim(b.episode_id(label), "payment_default", onset_date=doc.effective_date,
                              onset_lower_evidence_ids=(doc.observation_id,), facts=pay, **kw)

    res = w.resolve(claims=[claim("evt-1", d1), claim("evt-2", d2)])
    assert len(res.episodes) == 1 and [i.reason for i in res.issues] == ["overlapping_episodes"]
    cured = claim("evt-1", d1, resolution_date=dt.date(2026, 2, 1), resolution_refs=(cure.observation_id,))
    res2 = w.resolve(claims=[cured, claim("evt-2", d2)], strict=True)
    assert len(res2.episodes) == 2
    first = next(e for e in res2.episodes if e.episode_id == b.episode_id("evt-1"))
    assert first.resolution_known_at == cure.public_available_at


PAY = r.EventFacts(missed_payment=True, grace_period_expired_uncured=True)


def _cure_world():  # type: ignore[no-untyped-def]
    w = World()
    d1 = w.doc("p-cw-evt", B, effective=dt.date(2026, 1, 15))
    w.accept("cw-1", "accepted_event", [d1], B)

    def resolve(refs, date=dt.date(2026, 2, 1)):  # type: ignore[no-untyped-def]
        claim = r.EpisodeClaim(b.episode_id("cw-1"), "payment_default", onset_date=d1.effective_date, facts=PAY,
                               onset_lower_evidence_ids=(d1.observation_id,), resolution_date=date,
                               resolution_refs=tuple(x.adjudication_id if isinstance(x, c.Adjudication)
                                                     else x.observation_id for x in refs))
        return w.resolve(claims=[claim])

    return w, d1, resolve


def _reasons(res) -> list[str]:  # type: ignore[no-untyped-def]
    return [i.reason for i in res.issues]


def _reasons_for(res, label: str) -> list[str]:  # type: ignore[no-untyped-def]
    return [i.reason for i in res.issues if i.subject_id == b.episode_id(label)]


def _revise_head(w: World, label: str, evidence, cusip9: str, **kw) -> c.Adjudication:  # type: ignore[no-untyped-def]
    """New admitting revision of the episode chain ``label`` (supersedes the current head)."""
    subject = b.episode_id(label)
    current = [a for a in w.adjs if a.subject_id == subject]
    superseded = {a.supersedes_adjudication_id for a in current}
    prior = next(a for a in current if a.adjudication_id not in superseded)
    head = b.adjudicate(subject, "accepted_event", evidence, [w.link(o, cusip9) for o in evidence], supersedes=prior,
                        at=prior.adjudicated_at + dt.timedelta(hours=1), **kw)
    w.adjs.append(head)
    return head


def test_cure_via_nport_n_is_refused_even_in_the_episode_chain() -> None:
    w, d1, resolve = _cure_world()
    later_n = w.nport(B, dt.date(2026, 3, 31), "N")
    record = b.adjudicate(b.episode_id("cw-cure-n"), "candidate", [later_n], [w.link(later_n, B)],
                          subject_kind="candidate")
    w.adjs.append(record)
    res = resolve([record], date=dt.date(2026, 3, 31))
    assert res.episodes == () and _reasons(res) == [CANDIDATE]
    head = _revise_head(w, "cw-1", [d1, later_n], B)  # the admitting event revision cites only N-PORT N
    res2 = resolve([head], date=dt.date(2026, 3, 31))
    assert res2.episodes == () and _reasons(res2) == ["nport_n_is_not_cure_evidence"]
    with pytest.raises(r.ResolveError, match="nport_n_is_not_cure_evidence"):
        w.resolve(claims=[r.EpisodeClaim(b.episode_id("cw-1"), "payment_default", onset_date=dt.date(2026, 1, 15),
                                         facts=PAY, onset_lower_evidence_ids=(d1.observation_id,),
                                         resolution_date=dt.date(2026, 3, 31),
                                         resolution_refs=(head.adjudication_id,))], strict=True)


def test_cure_document_of_another_obligation_or_date_is_refused() -> None:
    w, _d1, resolve = _cure_world()
    other = w.doc("p-cw-other", C, effective=dt.date(2026, 2, 1), kind="issuer_document_passage")
    assert _reasons(resolve([other])) == ["resolution_scope_mismatch"]
    wrong_day = w.doc("p-cw-day", B, effective=dt.date(2026, 2, 3), kind="issuer_document_passage")
    assert _reasons(resolve([wrong_day])) == ["resolution_date_unsupported"]
    issuer_level = w.doc("p-cw-issuer", None, effective=dt.date(2026, 2, 1), link_cusips=())
    assert _reasons(resolve([issuer_level])) == ["resolution_scope_not_established"]
    # Another episode chain's admitting record (here C's) is a standalone decision for this episode.
    foreign = w.accept("cw-other", "accepted_event", [other], C)
    assert _reasons_for(resolve([foreign]), "cw-1") == [STANDALONE]


def test_cure_adjudication_must_be_this_episodes_effective_admitting_revision() -> None:
    w, d1, resolve = _cure_world()
    original = w.adjs[0]
    cure = w.doc("p-cw-cure", B, effective=dt.date(2026, 2, 1), kind="issuer_document_passage",
                 public=dt.datetime(2026, 2, 3, tzinfo=UTC))
    candidate = b.adjudicate(b.episode_id("cw-cure"), "candidate", [cure], [w.link(cure, B)], subject_kind="candidate")
    w.adjs.append(candidate)
    assert _reasons(resolve([candidate])) == [CANDIDATE]
    standalone = b.adjudicate(b.episode_id("cw-cure-sa"), "accepted_event", [cure], [w.link(cure, B)])
    w.adjs.append(standalone)
    assert _reasons_for(resolve([standalone]), "cw-1") == [STANDALONE]
    w.adjs.remove(standalone)
    head = _revise_head(w, "cw-1", [d1, cure], B)
    ok = resolve([head])
    assert _reasons(ok) == [] and ok.episodes[0].resolution_known_at == max(
        d1.public_available_at, cure.public_available_at, w.link(d1, B).link_known_at, w.link(cure, B).link_known_at)
    assert _reasons(resolve([original])) == ["resolution_adjudication_not_effective"]  # superseded revision


def test_merged_event_types_within_one_episode_are_corroboration() -> None:
    w = World()
    y = [w.nport(B, Q3_26, "Y", 1), w.nport(B, Q3_26, "Y", 2)]
    d = w.doc("p-bk", B, effective=dt.date(2026, 3, 2))
    w.accept("merged-B", "accepted_event", [d, *y], B)
    claim = r.EpisodeClaim(b.episode_id("merged-B"), "bankruptcy", onset_date=dt.date(2026, 3, 2),
                           onset_lower_evidence_ids=(d.observation_id,), corroborating_types=("payment_default",),
                           facts=r.EventFacts(proceeding="bankruptcy_petition", debtor_verified=True,
                                              affected_obligation_verified=True))
    # Phase 1 never grants the N-PORT consensus flag (no persisted proposal/family closure).
    (e,) = w.resolve(claims=[claim], strict=True).episodes
    assert e.corroboration_flags == ("bankruptcy", "edgar_document", "payment_default")
    assert e.primary_type == "bankruptcy"


def test_arrears_and_pik_are_markers_never_episodes() -> None:
    w = World()
    w.nport(D, Q3_26, "N", 1, arrears="Y")
    w.nport(D, Q3_26, "N", 2, pik="Y")
    res = w.resolve(strict=True)
    assert res.episodes == ()
    assert [m.kind for m in res.markers] == ["arrears_or_legal_deferral_unknown", "paid_in_kind"]


def test_post_cutoff_adjudication_and_evidence_are_not_admitted() -> None:
    w, y, n = _state_world()
    w.accept("state-A", "accepted_state", y + n, A, role="policy_rule_engine", at=K + dt.timedelta(hours=1))
    res = w.resolve(proposals=[_proposal(A, y, n)], strict=True)
    assert res.episodes == () and res.adjudication_inventory == ()
    w2 = World()
    d = w2.doc("p-late-evt", B, effective=dt.date(2026, 5, 15))
    w2.accept("evt-B", "accepted_event", [d], B)
    claim = r.EpisodeClaim(b.episode_id("evt-B"), "payment_default", onset_date=dt.date(2026, 5, 15),
                           onset_lower_evidence_ids=(d.observation_id,),
                           facts=r.EventFacts(missed_payment=True, grace_period_expired_uncured=True))
    early_k = d.public_available_at - dt.timedelta(days=1)
    res2 = r.resolve_episodes(w2.adjs, observations=w2.obs, links=w2.links().links, claims=[claim],
                              knowledge_cutoff=early_k, knowledge_mode="historical_reconstruction", strict=False)
    assert res2.episodes == () and res2.issues[0].reason.startswith("adjudication_link_not_admitted")


# ---------------------------------------------------------------------------
# build_followup
# ---------------------------------------------------------------------------
T = dt.date(2026, 6, 1)
MONTHS = tuple(dt.date(2026, m, 1) for m in range(1, 7))
SECURITIES = {x: b.security_id(x) for x in (A, B, C, D)}


def _receipt(verdict: str = "qualified", window=(dt.date(2025, 12, 31), dt.date(2026, 6, 30))) -> c.ValidationReceipt:  # type: ignore[no-untyped-def]
    return c.ValidationReceipt(
        receipt_id=c.uuid5_of("resolve_receipt", verdict), verdict=verdict, scope="SYNTHETIC surveillance",
        evidence_digest=c.digest_of({"receipt": verdict}), reviewer_id="synthetic-validator",
        issued_at=dt.datetime(2026, 9, 24, 12, 0, tzinfo=UTC), positive_evidence_count=3 if verdict == "qualified" else 0,
        surveillance_start_exclusive=window[0] if window else None, surveillance_end_inclusive=window[1] if window else None,
    )


def _followup(grid, obs, strict=False, **kw):  # type: ignore[no-untyped-def]
    return r.build_followup(grid, securities=SECURITIES, observations=obs, target_month=T, knowledge_cutoff=K,
                            strict=strict, **kw)


def _spans(res, status=None):  # type: ignore[no-untyped-def]
    rows = sorted(res.followups, key=lambda f: (f.cusip9, f.interval_start_exclusive))
    return [(f.interval_start_exclusive, f.interval_end_inclusive, f.status) for f in rows if status in (None, f.status)]


JAN31, FEB28, MAR31, APR30, MAY31, JUN30 = (dt.date(2026, 1, 31), dt.date(2026, 2, 28), dt.date(2026, 3, 31),
                                          dt.date(2026, 4, 30), dt.date(2026, 5, 31), dt.date(2026, 6, 30))
C_GRID = [(C, m) for m in MONTHS]


def _cdoc(locator: str, **kw) -> c.CreditObservation:  # type: ignore[no-untyped-def]
    return b.passage(locator, cusip9=C, kind="issuer_document_passage", **kw)


def _cont(doc, start, end, status="nondefault_continuous", basis="issuer_trustee_confirmation", adjs=()):  # type: ignore[no-untyped-def]
    return r.ContinuityEvidence(C, start, end, status, basis, (doc.observation_id,),
                                tuple(a.adjudication_id for a in adjs))


def test_outcome_intervals_follow_plan_windows_and_keep_terminal_start() -> None:
    assert r.outcome_interval(dt.date(2026, 1, 1), T) == (JAN31, FEB28)
    assert r.outcome_interval(T, T) is None
    doc = _cdoc("p-fu-feb")
    res = _followup([(C, dt.date(2026, 1, 1))], [doc], continuity=[_cont(doc, JAN31, FEB28)])
    assert _spans(res) == [(JAN31, FEB28, "nondefault_continuous")]
    assert [x.reason for x in res.censoring] == ["panel_exit"]
    gap = _followup([(C, dt.date(2026, 1, 1)), (C, dt.date(2026, 3, 1))], [])
    assert _spans(gap) == [(JAN31, FEB28, "unknown"), (MAR31, APR30, "unknown")]  # Feb start absent
    assert [x.reason for x in gap.censoring] == ["reentry_gap", "panel_exit"]


def test_nport_point_n_alone_never_forms_continuous_segment() -> None:
    obs = [b.nport(f"row-fu{i:04d}", C, d, "N", series=f"S99999900{i}") for i, d in enumerate((MAR31, JUN30))]
    res = _followup(C_GRID, obs)
    assert _spans(res) == [(JAN31, JUN30, "unknown")] and res.followups[0].evidence_observation_ids == ()
    assert [x.reason for x in res.censoring] == ["target_end"]


def test_documentary_continuity_then_explicit_unknown_gap() -> None:
    doc = _cdoc("p-trustee", public=dt.datetime(2026, 7, 3, tzinfo=UTC))
    res = _followup(C_GRID, [doc], continuity=[_cont(doc, dt.date(2025, 12, 31), MAR31)])
    assert _spans(res) == [(JAN31, MAR31, "nondefault_continuous"), (MAR31, JUN30, "unknown")]
    assert res.followups[0].known_at == doc.public_available_at
    late = _cdoc("p-late-trustee", public=K + dt.timedelta(days=1))
    res2 = _followup(C_GRID, [late], continuity=[_cont(late, JAN31, JUN30)])
    assert _spans(res2) == [(JAN31, JUN30, "unknown")]
    assert res2.issues == (f"continuity_evidence_unusable:observation_public_after_cutoff:{C}",)
    n = b.nport("row-fu9999", C, Q3_26, "N")
    nport_only = r.ContinuityEvidence(C, JAN31, JUN30, "nondefault_continuous", "continuous_document", (n.observation_id,))
    assert _followup(C_GRID, [n], continuity=[nport_only]).issues == (f"nport_only_is_not_continuous_evidence:{C}",)


def _nc(day: dt.date, flag: str, n: int = 1, **kw) -> c.CreditObservation:  # type: ignore[no-untyped-def]
    return b.nport(f"row-sv{day:%m%d}{n}", C, day, flag, series=f"S99999900{n}", **kw)


def test_surveillance_needs_both_nondefault_boundaries_and_no_default_evidence_inside() -> None:
    receipt = _receipt()
    n_mar, n_jun = _nc(MAR31, "N"), _nc(JUN30, "N")
    valid = _followup(C_GRID, [n_mar, n_jun], validation_receipt=receipt)
    assert _spans(valid) == [(JAN31, MAR31, "unknown"), (MAR31, JUN30, "nondefault_continuous")]
    seg = next(f for f in valid.followups if f.status != "unknown")
    assert seg.completeness_basis == "surveillance_receipt" and valid.issues == ()
    assert set(seg.evidence_observation_ids) == {n_mar.observation_id, n_jun.observation_id}
    assert seg.known_at == max(receipt.issued_at, n_mar.public_available_at, n_jun.public_available_at)
    # Y only (the reproduction): never survival, and reported.
    y_only = _followup(C_GRID, [_nc(MAR31, "Y")], validation_receipt=receipt)
    assert _spans(y_only) == [(JAN31, JUN30, "unknown")]
    assert y_only.issues == (f"surveillance_blocked_by_default_evidence:{C}:2026-03-31",)
    with pytest.raises(r.ResolveError, match="surveillance_blocked_by_default_evidence"):
        _followup(C_GRID, [_nc(MAR31, "Y")], validation_receipt=receipt, strict=True)
    # Y/N conflict at the end boundary.
    conflict = _followup(C_GRID, [n_mar, n_jun, _nc(JUN30, "Y", 2)], validation_receipt=receipt)
    assert _spans(conflict, "nondefault_continuous") == [] and conflict.issues == (
        f"surveillance_blocked_by_default_evidence:{C}:2026-06-30",)
    # Missing start / end boundary.
    assert _spans(_followup(C_GRID, [n_jun], validation_receipt=receipt), "nondefault_continuous") == []
    assert _spans(_followup(C_GRID, [n_mar], validation_receipt=receipt), "nondefault_continuous") == []
    # Arrears/legal deferral inside and a partial receipt both block.
    arrears = _nc(MAY31, "N", 3, arrears="Y")
    assert _spans(_followup(C_GRID, [n_mar, n_jun, arrears], validation_receipt=receipt), "nondefault_continuous") == []
    assert _spans(_followup(C_GRID, [n_mar, n_jun], validation_receipt=_receipt("partial")), "nondefault_continuous") == []
    # A documentary segment supplies the start boundary.
    doc = _cdoc("p-sv-doc")
    mixed = _followup(C_GRID, [doc, n_jun], continuity=[_cont(doc, JAN31, MAR31)], validation_receipt=receipt)
    assert _spans(mixed) == [(JAN31, MAR31, "nondefault_continuous"), (MAR31, JUN30, "nondefault_continuous")]


def test_issuer_level_continuity_requires_established_scope_not_any_adjudication() -> None:
    issuer_doc = b.passage("p-fu-issuer", cusip9=None, kind="issuer_document_passage")
    unrelated = b.adjudicate(b.episode_id("fu-unrelated"), "retracted", [], [], subject_kind="candidate")
    ev = r.ContinuityEvidence(C, JAN31, JUN30, "nondefault_continuous", "issuer_trustee_confirmation",
                              (issuer_doc.observation_id,), (unrelated.adjudication_id,))
    res = _followup(C_GRID, [issuer_doc], continuity=[ev], adjudications=[unrelated])
    assert _spans(res) == [(JAN31, JUN30, "unknown")] and res.issues == (f"continuity_scope_not_established:{C}",)
    stated = _cdoc("p-fu-stated")
    ev2 = _cont(stated, JAN31, JUN30, adjs=[unrelated])
    res2 = _followup(C_GRID, [stated], continuity=[ev2], adjudications=[unrelated])
    assert _spans(res2) == [(JAN31, JUN30, "unknown")]
    assert res2.issues == (f"continuity_adjudication_unusable:not_effective:{C}",)


def test_historical_issuer_continuity_consistent_with_link_resolver() -> None:
    k_hist = dt.datetime(2026, 7, 1, tzinfo=UTC)
    doc = b.passage("p-fu-hist", cusip9=None, kind="issuer_document_passage", public=dt.datetime(2026, 5, 10, tzinfo=UTC))
    req = b.request(doc, C, "adjudicated_issue_scope", scope="issuer_affected_obligation",
                    known=dt.datetime(2026, 5, 12, tzinfo=UTC))
    # Present-day review inside the episode chain (scope record + accepting revision).
    links, scope, head = _chain([req], [doc], doc, C, "fu-hist-scope", k=k_hist, mode="historical_reconstruction")
    (admitted,) = links.admitted
    ev = r.ContinuityEvidence(C, JAN31, MAR31, "nondefault_continuous", "issuer_trustee_confirmation",
                              (doc.observation_id,))
    common = {"securities": SECURITIES, "observations": [doc], "continuity": [ev], "adjudications": [scope, head],
              "links": links.links, "target_month": T, "knowledge_cutoff": k_hist}
    hist = r.build_followup(C_GRID, knowledge_mode="historical_reconstruction", **common)
    seg = next(f for f in hist.followups if f.status == "nondefault_continuous")
    assert seg.known_at == admitted.link_known_at == dt.datetime(2026, 5, 12, tzinfo=UTC)
    current = r.build_followup(C_GRID, knowledge_mode="current_run", strict=False, **common)
    assert _spans(current, "nondefault_continuous") == []
    assert current.issues == (f"continuity_evidence_unusable:observation_not_ingested_by_cutoff:{C}",)


def _c_episode_chain(*docs: c.CreditObservation):  # type: ignore[no-untyped-def]
    """Admitted, cured payment-default episode of C whose accepting revision also cites ``docs``."""
    w = World()
    onset = w.doc("p-c-onset", C, effective=dt.date(2026, 1, 10))
    cure = w.doc("p-c-cure", C, effective=dt.date(2026, 1, 20), kind="issuer_document_passage")
    for d in docs:
        w.obs.append(d)
        w.reqs.append(b.request(d, C, "document_stated_cusip"))
    head = w.accept("c-ep", "accepted_event", [onset, *docs], C)
    claim = r.EpisodeClaim(b.episode_id("c-ep"), "payment_default", onset_date=dt.date(2026, 1, 10), facts=PAY,
                           onset_lower_evidence_ids=(onset.observation_id,), resolution_date=dt.date(2026, 1, 20),
                           resolution_refs=(cure.observation_id,))
    (episode,) = w.resolve(claims=[claim], strict=True).episodes
    return w, head, episode


def test_documentary_overlaps_merge_canonically_and_conflicts_are_disputes() -> None:
    d1, d2 = _cdoc("p-ov-1"), _cdoc("p-ov-2", public=dt.datetime(2026, 5, 20, tzinfo=UTC))
    same = [_cont(d1, JAN31, JUN30), _cont(d2, JAN31, JUN30)]
    forward = _followup(C_GRID, [d1, d2], continuity=same)
    backward = _followup(C_GRID, [d2, d1], continuity=list(reversed(same)))
    assert forward == backward
    (seg,) = forward.followups
    assert set(seg.evidence_observation_ids) == {d1.observation_id, d2.observation_id}
    overlap = [_cont(d1, JAN31, APR30, basis="continuous_document"), _cont(d2, MAR31, JUN30)]
    res = _followup(C_GRID, [d1, d2], continuity=overlap)
    assert res == _followup(C_GRID, [d2, d1], continuity=list(reversed(overlap)))
    assert [(f.interval_start_exclusive, f.completeness_basis) for f in sorted(
        res.followups, key=lambda f: f.interval_start_exclusive)] == [
        (JAN31, "continuous_document"), (MAR31, "issuer_trustee_confirmation"), (APR30, "issuer_trustee_confirmation")]
    conflict = [_cont(d1, JAN31, JUN30), _cont(d2, MAR31, JUN30, status="repaid")]
    disputed = _followup(C_GRID, [d1, d2], continuity=conflict)
    assert _spans(disputed) == [(JAN31, MAR31, "nondefault_continuous"), (MAR31, JUN30, "unknown")]
    assert disputed.issues == (f"continuity_status_dispute:{C}:2026-03-31:2026-06-30",)
    # A persistable dispute decision: the admitted episode's own accepting revision cites both documents.
    w, head, episode = _c_episode_chain(d1, d2)
    resolved_ev = [_cont(d1, JAN31, JUN30), _cont(d2, MAR31, JUN30, status="repaid", adjs=[head])]
    resolved = _followup(C_GRID, w.obs, continuity=resolved_ev, adjudications=w.adjs, links=w.all_links(),
                         episodes=[episode])
    assert _spans(resolved) == [(JAN31, MAR31, "nondefault_continuous"), (MAR31, JUN30, "repaid")]
    assert resolved.issues == ()
    assert resolved == _followup(C_GRID, list(reversed(w.obs)), continuity=list(reversed(resolved_ev)),
                                 adjudications=list(reversed(w.adjs)), links=w.all_links(), episodes=[episode])


def test_candidate_never_establishes_scope_continuity_dispute_resolution_or_cure() -> None:
    # Scope: a candidate citing the quarantined proposal admits nothing (links fail closed; strict raises).
    o = b.passage("p-cand-scope", cusip9=None, kind="issuer_document_passage")
    req = b.request(o, C, "adjudicated_issue_scope", scope="issuer_affected_obligation")
    proposal = _links([req], [o]).links[0]
    candidate = b.adjudicate(b.episode_id("cand-scope"), "candidate", [o], [proposal], subject_kind="candidate")
    scoped = _links([req], [o], [candidate])
    assert scoped.admitted == () and [i.reason for i in scoped.issues] == [CANDIDATE]
    with pytest.raises(r.ResolveError, match=CANDIDATE):
        r.resolve_links([req], observations=[o], adjudications=[candidate], link_package=b.LINKS,
                        knowledge_cutoff=K, strict=True)
    # Continuity: the issuer-level document needs scope; a candidate cannot provide it.
    ev = r.ContinuityEvidence(C, JAN31, JUN30, "nondefault_continuous", "issuer_trustee_confirmation",
                              (o.observation_id,))
    cont = _followup(C_GRID, [o], continuity=[ev], adjudications=[candidate], links=scoped.links)
    assert _spans(cont) == [(JAN31, JUN30, "unknown")] and cont.issues == (f"{CANDIDATE}:{C}",)
    with pytest.raises(r.ResolveError, match=CANDIDATE):
        _followup(C_GRID, [o], continuity=[ev], adjudications=[candidate], links=scoped.links, strict=True)
    # Dispute resolution: a candidate decision is ignored, so the conflict stays disputed.
    d1, d2 = _cdoc("p-cand-1"), _cdoc("p-cand-2", public=dt.datetime(2026, 5, 20, tzinfo=UTC))
    link = _admitted(d1)
    decision = b.adjudicate(b.episode_id("cand-dispute"), "candidate", [d1, d2], [link], subject_kind="candidate")
    conflict = [_cont(d1, JAN31, JUN30), _cont(d2, MAR31, JUN30, status="repaid", adjs=[decision])]
    disputed = _followup(C_GRID, [d1, d2], continuity=conflict, adjudications=[decision], links=[link])
    assert _spans(disputed) == [(JAN31, MAR31, "nondefault_continuous"), (MAR31, JUN30, "unknown")]
    assert disputed.issues == (f"{CANDIDATE}:{C}", f"continuity_status_dispute:{C}:2026-03-31:2026-06-30")
    # A standalone admitting decision (no persisted episode) is ignored the same way.
    standalone = b.adjudicate(b.episode_id("sa-dispute"), "accepted_event", [d1, d2], [link])
    conflict_sa = [_cont(d1, JAN31, JUN30), _cont(d2, MAR31, JUN30, status="repaid", adjs=[standalone])]
    disputed_sa = _followup(C_GRID, [d1, d2], continuity=conflict_sa, adjudications=[standalone], links=[link])
    assert _spans(disputed_sa, "repaid") == [] and f"{STANDALONE}:{C}" in disputed_sa.issues
    # Cure: a candidate reference never resolves an episode.
    w, _d1, resolve = _cure_world()
    cure = w.doc("p-cand-cure", B, effective=dt.date(2026, 2, 1), kind="issuer_document_passage")
    cure_candidate = b.adjudicate(b.episode_id("cand-cure"), "candidate", [cure], [w.link(cure, B)],
                                  subject_kind="candidate")
    w.adjs.append(cure_candidate)
    res = resolve([cure_candidate])
    assert res.episodes == () and _reasons(res) == [CANDIDATE]


def _admitted(doc: c.CreditObservation) -> c.EventLink:
    return _links([b.request(doc, C, "document_stated_cusip")], [doc]).admitted[0]


def test_episode_onset_clips_at_risk_and_reentry_and_exit_censoring() -> None:
    w, y, n = _state_world()
    w.accept("state-A", "accepted_state", y + n, A)
    episodes = w.resolve(claims=[_state_claim(y, n)], strict=True).episodes
    grid = [(A, dt.date(2025, 10, 1)), (A, dt.date(2025, 11, 1)), (A, dt.date(2025, 12, 1)), (A, dt.date(2026, 1, 1)),
            (B, dt.date(2026, 1, 1)), (B, dt.date(2026, 2, 1)), (B, dt.date(2026, 4, 1))]
    res = r.build_followup(grid, securities=SECURITIES, observations=w.obs, episodes=episodes, target_month=T,
                           knowledge_cutoff=K)
    a_segments = [f for f in res.followups if f.cusip9 == A]
    assert [(f.interval_start_exclusive, f.interval_end_inclusive) for f in a_segments] == [
        (dt.date(2025, 10, 31), Q1)]
    reasons = {(x.cusip9, x.reason) for x in res.censoring}
    assert reasons == {(A, "default_onset"), (A, "panel_exit"), (B, "reentry_gap"), (B, "panel_exit")}
    b_spells = {f.spell_id for f in res.followups if f.cusip9 == B}
    assert len(b_spells) == 2


def test_exchange_alias_emits_no_survival_rows() -> None:
    alias = c.uuid5_of("bond_default_exchange_alias_spell", "x")
    res = _followup(C_GRID, [], alias_spells={C: alias})
    assert res.followups == () and [(x.spell_id, x.reason) for x in res.censoring] == [(alias, "exchange_alias_absorbed")]


# ---------------------------------------------------------------------------
# build_coverage
# ---------------------------------------------------------------------------
def _rating(cusip9: str, month: dt.date, bucket: str | None) -> c.RatingGridRow:
    rated = bucket is not None
    return c.RatingGridRow(
        cusip_id=cusip9, month=month, view_kind="public_pit", bucket=bucket,
        state="observed" if rated else "missing", action_date=month if rated else None,
        public_known_at=dt.datetime(month.year, month.month, 2, tzinfo=UTC) if rated else None,
        agency_source_ids=(c.uuid5_of("synthetic_action", cusip9),) if rated else (),
        binding_link_ids=(),
        coverage_frontier=None, action_input_digest=c.digest_of({"a": cusip9}) if rated else None,
        default_overlay_episode_id=None,
    )


def _cell(cells, event_type: str, stratum: str, period: str = "2026", source: str = "all", cohort: str = "all"):  # type: ignore[no-untyped-def]
    return next(x for x in cells if (x.period_label, x.source, x.event_type, x.rating_stratum, x.exposure_cohort)
                == (period, source, event_type, stratum, cohort))


def _payment_episode():  # type: ignore[no-untyped-def]
    w, _d, claim = _event_world(r.EventFacts(missed_payment=True, grace_period_expired_uncured=True), "payment_default")
    return w.resolve(claims=[claim], strict=True).episodes


FRONTIER = {"all": JUN30}


def _cov(grid, **kw):  # type: ignore[no-untyped-def]
    args = {"episodes": (), "source_frontiers": FRONTIER, "target_month": T, "knowledge_cutoff": K, **kw}
    return r.build_coverage(grid, **args)


def test_coverage_strata_counts_and_partial_without_independent_denominator() -> None:
    grid = [(x, m) for x in (B, C) for m in MONTHS]
    ratings = [_rating(B, m, "B") for m in MONTHS] + [_rating(C, m, "BBB") for m in MONTHS]
    cells = _cov(grid, episodes=_payment_episode(), ratings=ratings, validation_receipt=_receipt())
    assert len(cells) == len(r.COVERAGE_EVENT_TYPES) * len(r.COVERAGE_STRATA)
    hy = _cell(cells, "payment_default", "HY")
    assert (hy.state, hy.denominator_basis, hy.denominator_count, hy.event_count, hy.exposed_issue_months) == (
        "partial", "panel_exposure", 1, 1, 5)  # starts Jan..May: (end(m), end(m+1)] within T
    assert hy.validation_receipt_digest is None and hy.lag_p50_days == 129  # link known 2026-09-21
    assert _cell(cells, "payment_default", "IG").event_count == 0
    assert _cell(cells, "distressed_exchange", "all").state == "partial"
    assert _cell(cells, "agency_issue_default", "all").state == "not_applicable"
    assert _cell(cells, "all", "unknown").state == "not_applicable"  # no unknown-stratum exposure


def test_coverage_qualified_only_with_receipt_denominator_and_frontier() -> None:
    grid = [(B, m) for m in MONTHS]
    key = ("2026", "all", "payment_default", "all", "all")
    args = {"episodes": _payment_episode()}
    q = _cell(_cov(grid, validation_receipt=_receipt(), independent_denominators={key: 4}, **args), "payment_default", "all")
    assert (q.state, q.denominator_basis, q.denominator_count) == ("qualified", "independent_reference_enumeration", 4)
    assert q.validation_receipt_digest == _receipt().digest()
    no_receipt = _cov(grid, independent_denominators={key: 4}, **args)
    partial_receipt = _cov(grid, validation_receipt=_receipt("partial"), independent_denominators={key: 4}, **args)
    short_frontier = _cov(grid, validation_receipt=_receipt(), independent_denominators={key: 4},
                          source_frontiers={"all": MAR31}, **args)
    for cells in (no_receipt, partial_receipt, short_frontier):
        assert _cell(cells, "payment_default", "all").state == "partial"
    unavailable = _cov(grid, source_frontiers={}, **args)
    assert {x.state for x in unavailable} <= {"unavailable", "not_applicable"}


def _unknown(cells) -> int:  # type: ignore[no-untyped-def]
    return _cell(cells, "all", "all").unknown_outcome_issue_months


def _seg(cusip9: str, start: dt.date, end: dt.date, status: str = "nondefault_continuous") -> c.FollowUp:
    spell = c.uuid5_of("cov_spell", cusip9)
    evidence = () if status == "unknown" else (c.uuid5_of("cov_doc", cusip9, start.isoformat()),)
    return r._segment(cusip9, b.security_id(cusip9), spell, (start, end), status,
                      "issuer_trustee_confirmation" if status != "unknown" else "none", evidence, (), b.ADJ_AT)


def test_coverage_counts_only_whole_interval_outcomes() -> None:
    grid = [(C, m) for m in MONTHS]
    # June-30-only reproduction: a segment touching the month end does not observe the interval.
    only_end = _cov(grid, followups=[_seg(C, dt.date(2026, 6, 29), JUN30)])
    assert _cell(only_end, "all", "all").exposed_issue_months == 5 and _unknown(only_end) == 5
    assert _unknown(_cov(grid, followups=[_seg(C, JAN31, JUN30)])) == 0
    assert _unknown(_cov(grid, followups=[_seg(C, JAN31, APR30), _seg(C, APR30, JUN30)])) == 0
    assert _unknown(_cov(grid, followups=[_seg(C, dt.date(2026, 2, 1), JUN30)])) == 1  # Feb interval partly uncovered
    # Explicit interval outcomes reconcile with the cell counts.
    segments = [(JAN31, MAR31), (APR30, JUN30)]
    outcomes = [r.interval_outcome(r.outcome_interval(m, T), [], segments) for m in MONTHS[:5]]
    assert outcomes == ["nondefault", "nondefault", "unknown", "nondefault", "nondefault"]
    assert _unknown(_cov(grid, followups=[_seg(C, *s) for s in segments])) == outcomes.count("unknown")


def test_coverage_keeps_potential_onset_windows_uncertain() -> None:
    (event,) = _payment_episode()
    grid = [(B, m) for m in MONTHS]
    prevalent = syn_replace(event, onset_date=None, onset_lower_exclusive=None, onset_upper_inclusive=MAR31,
                            onset_lower_evidence_ids=(), timing_class="prevalent")
    interval = syn_replace(event, onset_date=None, onset_lower_exclusive=JAN31, onset_upper_inclusive=MAR31,
                           timing_class="interval_uncertain")
    full = [_seg(B, JAN31, JUN30)]
    for e in (prevalent, interval):
        outcomes = [r.interval_outcome(r.outcome_interval(m, T), [e], [(JAN31, JUN30)]) for m in MONTHS[:5]]
        # Intervals after the onset upper bound are default; the onset window stays uncertain even
        # though (inconsistent) follow-up claims to cover it.
        assert outcomes == ["unknown", "unknown", "default", "default", "default"]
        assert _unknown(_cov(grid, episodes=[e], followups=full)) == outcomes.count("unknown") == 2


def test_coverage_source_and_cohort_detail_cells_use_exact_keys() -> None:
    (event,) = _payment_episode()
    grid = [(B, m) for m in MONTHS]
    units = {(B, m): ("sec_edgar",) for m in MONTHS[:5]}
    cohorts = {(B, m): "retained" for m in MONTHS[:5]}
    all_key = ("2026", "all", "payment_default", "all", "all")
    detail_key = ("2026", "sec_edgar", "payment_default", "all", "retained")
    cells = _cov(grid, episodes=[event], validation_receipt=_receipt(), exposure_sources=units,
                 event_sources={event.episode_id: ("sec_edgar",)}, exposure_cohorts=cohorts,
                 source_frontiers={"all": JUN30, "sec_edgar": JUN30}, independent_denominators={all_key: 4})
    assert {x.source for x in cells} == {"all", "sec_edgar"} and {x.exposure_cohort for x in cells} == {"all", "retained"}
    assert _cell(cells, "payment_default", "all").state == "qualified"
    detail = _cell(cells, "payment_default", "all", source="sec_edgar", cohort="retained")
    assert (detail.state, detail.event_count, detail.exposed_issue_months) == ("partial", 1, 5)  # not borrowed
    exact = _cov(grid, episodes=[event], validation_receipt=_receipt(), exposure_sources=units,
                 event_sources={event.episode_id: ("sec_edgar",)}, exposure_cohorts=cohorts,
                 source_frontiers={"all": JUN30, "sec_edgar": JUN30}, independent_denominators={detail_key: 2})
    assert _cell(exact, "payment_default", "all", source="sec_edgar", cohort="retained").state == "qualified"
    assert _cell(exact, "payment_default", "all").state == "partial"
    no_frontier = _cov(grid, episodes=[event], exposure_sources=units, event_sources={event.episode_id: ("sec_edgar",)})
    assert _cell(no_frontier, "payment_default", "all", source="sec_edgar").state == "unavailable"
    with pytest.raises(r.ResolveError, match="exposure_cohort_invalid"):
        _cov(grid, exposure_cohorts={(B, MONTHS[0]): "all"})


def test_negative_lags_are_excluded_reported_and_block_qualification() -> None:
    (event,) = _payment_episode()
    at = dt.datetime(2026, 3, 1, 12, 0, tzinfo=UTC)
    early = syn_replace(event, evidence_known_at=at, link_known_at=at)  # -75 days
    assert (early.evidence_known_at.date() - early.onset_upper_inclusive).days == -75
    grid = [(B, m) for m in MONTHS]
    key = ("2026", "all", "payment_default", "all", "all")
    cells = _cov(grid, episodes=[early], validation_receipt=_receipt(), independent_denominators={key: 4})
    cell = _cell(cells, "payment_default", "all")
    assert cell.state == "partial" and cell.lag_p50_days is None and cell.lag_max_days is None
    assert "negative_lag_pending_adjudication=1" in cell.rationale
    settled = _cov(grid, episodes=[early], validation_receipt=_receipt(), independent_denominators={key: 4},
                   adjudicated_negative_lags=[early.episode_id])
    assert _cell(settled, "payment_default", "all").state == "qualified"


# ---------------------------------------------------------------------------
# End to end: assemble_bundle + check_bundle
# ---------------------------------------------------------------------------
def _bundle(w: World, episodes, followups, coverage, grid, receipt=None, quality="partial", k=K,  # type: ignore[no-untyped-def]
            mode="current_run", adjudications=None):
    declarations = c.normalize_rating_declarations_record({
        "version": c.RATING_DECLARATIONS_VERSION,
        "rating_scopes": [],
        "uncleared_rating_sources": [],
    })
    ratings = [c.RatingGridRow(cusip_id=x, month=m, view_kind=v, bucket=None, state="missing", action_date=None,
                               public_known_at=None, agency_source_ids=(), binding_link_ids=(), coverage_frontier=None,
                               action_input_digest=None, default_overlay_episode_id=None)
               for x, m in grid for v in ("effective_audit", "public_pit")]
    return c.assemble_bundle(
        target_month=T, knowledge_cutoff=k, knowledge_mode=mode, build_scope="limited",
        quality_state=quality, code_digest=c.digest_of({"code": "resolve-test"}),
        panel_publication_id=c.uuid5_of("synthetic_panel", "resolve"), panel_grid=grid, issuer_mapping_digest=None,
        rating_declarations=declarations,
        rating_input_digest=c.rating_input_manifest_digest(declarations, b.PACKAGES), validation_receipt=receipt,
        source_packages=b.PACKAGES, observations=w.obs, event_links=w.all_links(),
        adjudications=w.adjs if adjudications is None else adjudications,
        events=episodes, followups=followups, exit_evidence=(), coverage=coverage, ratings=ratings,
        ncen_filings=(), family_contexts=(), family_evidence=(), proposal_evidence=(), exchange_relations=(),
    )


def _pipeline(w: World, grid, *, proposals=(), claims=(), continuity=(), receipt=None):  # type: ignore[no-untyped-def]
    from src.bonds.default_events import publication as p

    res = w.resolve(proposals=proposals, claims=claims, strict=True)
    fu = r.build_followup(grid, securities=SECURITIES, observations=w.obs, episodes=res.episodes,
                          continuity=continuity, adjudications=res.adjudication_inventory, links=w.all_links(),
                          validation_receipt=receipt, alias_spells=res.alias_spells, target_month=T,
                          knowledge_cutoff=K)
    cov = r.build_coverage(grid, episodes=res.episodes, followups=fu.followups, validation_receipt=receipt,
                           source_frontiers=FRONTIER, target_month=T, knowledge_cutoff=K)
    bundle = _bundle(w, res.episodes, fu.followups, cov, grid, receipt=receipt)
    p.check_bundle(bundle)
    return bundle, res


def _full_world():  # type: ignore[no-untyped-def]
    w, y, n = _state_world()
    w.accept("state-A", "accepted_state", y + n, A)
    d = w.doc("p-e2e", B, effective=dt.date(2026, 5, 15))
    w.accept("evt-B", "accepted_event", [d], B)
    claim = r.EpisodeClaim(b.episode_id("evt-B"), "payment_default", onset_date=dt.date(2026, 5, 15),
                           onset_lower_evidence_ids=(d.observation_id,),
                           facts=r.EventFacts(missed_payment=True, grace_period_expired_uncured=True))
    grid = [(x, m) for x in (A, B, C) for m in MONTHS]
    return w, y, n, d, claim, grid


def _rich_world():  # type: ignore[no-untyped-def]
    """Full world plus a cure on B, documentary + surveillance follow-up on C."""
    w, y, n, _d, claim, grid = _full_world()
    cure = w.doc("p-e2e-cure", B, effective=dt.date(2026, 6, 10), kind="issuer_document_passage",
                 public=dt.datetime(2026, 6, 11, tzinfo=UTC))
    cured = r.EpisodeClaim(claim.episode_id, claim.primary_type, onset_date=claim.onset_date, facts=claim.facts,
                           onset_lower_evidence_ids=claim.onset_lower_evidence_ids,
                           resolution_date=dt.date(2026, 6, 10), resolution_refs=(cure.observation_id,))
    trustee = w.doc("p-e2e-trustee", C, kind="issuer_document_passage", public=dt.datetime(2026, 4, 2, tzinfo=UTC))
    w.nport(C, JUN30, "N")
    continuity = [r.ContinuityEvidence(C, JAN31, MAR31, "nondefault_continuous", "issuer_trustee_confirmation",
                                       (trustee.observation_id,))]
    return w, y, n, cured, grid, continuity


def test_end_to_end_resolver_output_passes_check_bundle() -> None:
    w, y, n, cured, grid, continuity = _rich_world()
    bundle, res = _pipeline(w, grid, claims=[_state_claim(y, n), cured], continuity=continuity, receipt=_receipt())
    assert len(bundle.frames["events"]) == 2 and {e.cusip9 for e in res.episodes} == {A, B}
    c_rows = sorted((f for f in bundle.frames["followups"] if f.cusip9 == C), key=lambda f: f.interval_start_exclusive)
    assert [(f.interval_start_exclusive, f.completeness_basis) for f in c_rows] == [
        (JAN31, "issuer_trustee_confirmation"), (MAR31, "surveillance_receipt")]
    b_event = next(e for e in res.episodes if e.cusip9 == B)
    assert b_event.resolution_date == dt.date(2026, 6, 10)


def test_end_to_end_bundle_digest_is_permutation_invariant() -> None:
    def digest(reverse: bool) -> str:
        w, y, n, cured, grid, continuity = _rich_world()
        if reverse:
            w.obs.reverse()
            w.reqs.reverse()
            w.adjs.reverse()
            grid = list(reversed(grid))
            continuity = list(reversed(continuity))
        bundle, _res = _pipeline(w, grid, claims=[_state_claim(y, n), cured], continuity=continuity,
                                 receipt=_receipt())
        return bundle.bundle_digest()

    assert digest(False) == digest(True)


def test_end_to_end_rejects_missing_adjudication() -> None:
    from src.bonds.default_events import publication as p

    w, y, n, _d, claim, grid = _full_world()
    _bundle_ok, res = _pipeline(w, grid, claims=[_state_claim(y, n), claim])
    # The accepting adjudication of an event is dropped from the inventory: the bundle is refused.
    accepted = next(a for a in w.adjs if a.status == "accepted_event")
    w.adjs.remove(accepted)
    assert w.resolve(claims=[_state_claim(y, n), claim], strict=True).episodes != res.episodes
    with pytest.raises(c.ContractError, match="event_evidence_invalid"):
        p.check_bundle(_bundle(w, res.episodes, (), (), grid))


def test_end_to_end_rejects_post_cutoff_evidence() -> None:
    from src.bonds.default_events import publication as p

    w, y, n, d, claim, grid = _full_world()
    _bundle_ok, res = _pipeline(w, grid, claims=[_state_claim(y, n), claim])
    # Resolver output at K reused in a snapshot whose cutoff precedes the event evidence.
    early = d.public_available_at - dt.timedelta(hours=1)
    with pytest.raises(c.ContractError, match="event_evidence_invalid"):
        p.check_bundle(_bundle(w, res.episodes, (), (), grid, k=early, mode="historical_reconstruction"))
    # The resolver itself never admits it at that cutoff.
    at_early = r.resolve_episodes(w.adjs, observations=w.obs, links=w.all_links(),
                                  claims=[_state_claim(y, n), claim], knowledge_cutoff=early,
                                  knowledge_mode="historical_reconstruction", strict=False)
    assert at_early.episodes == ()


def test_end_to_end_rejects_issuer_wide_scope_without_adjudication() -> None:
    from src.bonds.default_events import publication as p

    w = World()
    issuer_doc = w.doc("p-issuer-wide", None, effective=dt.date(2026, 5, 15), link_cusips=())
    for x in (A, B, C):
        w.reqs.append(b.request(issuer_doc, x, "adjudicated_issue_scope", scope="issuer_affected_obligation"))
    assert w.links().admitted == ()
    quarantined = next(x for x in w.links().links if x.cusip9 == A)
    claim = r.EpisodeClaim(b.episode_id("wide"), "payment_default", onset_date=dt.date(2026, 5, 15),
                           onset_lower_evidence_ids=(issuer_doc.observation_id,), facts=PAY)
    pending = b.adjudicate(claim.episode_id, "candidate", [issuer_doc], [quarantined], subject_kind="candidate")
    w.adjs.append(pending)  # a pending (candidate) review decides no scope
    assert w.links().admitted == () and w.resolve(claims=[claim]).episodes == ()
    grid = [(x, m) for x in (A, B, C) for m in MONTHS]
    forged = b.adjudicate(b.episode_id("wide-forged"), "accepted_event", [issuer_doc], [quarantined])
    w.adjs.append(forged)
    # The resolver refuses an event over the quarantined proposal itself ...
    forged_claim = r.EpisodeClaim(forged.subject_id, "payment_default", onset_date=dt.date(2026, 5, 15),
                                  onset_lower_evidence_ids=(issuer_doc.observation_id,), facts=PAY)
    assert _reasons(w.resolve(claims=[forged_claim])) == ["adjudication_link_not_admitted_at_cutoff"]
    # A standalone admitting scope decision is not persistable: nothing is admitted, B/C never touched.
    assert w.links().admitted == ()
    assert [i.reason for i in w.links().issues if i.cusip9 == A] == [STANDALONE]
    # ... and a hand-built event over it is refused by check_bundle.
    edges = {
        "evidence_observation_ids": (issuer_doc.observation_id,),
        "onset_lower_evidence_ids": (issuer_doc.observation_id,),
        "link_ids": (quarantined.link_id,),
        "adjudication_ids": (forged.adjudication_id,),
        "proposal_evidence_ids": (),
        "exchange_relation_ids": (),
    }
    frames = w.dependency_frames()
    with pytest.raises(c.ContractError, match="dependency_unresolved"):
        p.event_dependency_digest(frames, knowledge_cutoff=K, knowledge_mode="current_run", **edges)

    # Preserve the bundle-level negative without inventing a digest for the unresolved closure:
    # mutate a valid test event after construction so its public identity contradicts its links.
    import copy

    valid_world, _valid_doc, valid_claim = _event_world(PAY, "payment_default")
    (event,) = valid_world.resolve(claims=[valid_claim], strict=True).episodes
    invalid_event = copy.copy(event)
    invalid_values = {
        "security_id": quarantined.security_id,
        "episode_id": forged.subject_id,
        "cusip9": A,
        "obligor_id": quarantined.obligor_id,
        "issuer_episode_id": c.uuid5_of("phase1-invalid-issuer-scope", str(forged.subject_id)),
        "evidence_observation_ids": (issuer_doc.observation_id,),
        "onset_lower_evidence_ids": (issuer_doc.observation_id,),
        "link_ids": (quarantined.link_id,),
        "adjudication_ids": (forged.adjudication_id,),
    }
    for name, value in invalid_values.items():
        object.__setattr__(invalid_event, name, value)
    with pytest.raises(c.ContractError, match="event_evidence_invalid"):
        p.check_bundle(_bundle(w, [invalid_event], (), (), grid))


def _scoped_episode_world(*, in_chain: bool = True):  # type: ignore[no-untyped-def]
    """Issuer-level notice; scope decided within the episode's own adjudication chain (or, with
    ``in_chain=False``, by a standalone decision that is its own head)."""
    w = World()
    notice = w.doc("p-scope-notice", None, effective=dt.date(2026, 2, 10), link_cusips=())
    for x in (A, D):
        w.reqs.append(b.request(notice, x, "adjudicated_issue_scope", scope="issuer_affected_obligation"))
    proposal = next(x for x in w.links().links if x.cusip9 == A)
    episode = b.episode_id("bk-A")
    scope = b.adjudicate(episode, "accepted_event", [notice], [proposal], at=dt.datetime(2026, 9, 21, 12, tzinfo=UTC))
    w.adjs.append(scope)
    prospective = next(i.prospective_link_id for i in w.links().issues if i.cusip9 == A and i.reason == STANDALONE)
    admitted = None
    if in_chain:
        w.adjs.append(b.adjudicate(episode, "accepted_event", [notice], [_cite(prospective)], supersedes=scope))
        admitted = w.link(notice, A)
        assert admitted.link_id == prospective
    claim = r.EpisodeClaim(episode, "bankruptcy", onset_date=dt.date(2026, 2, 10),
                           onset_lower_evidence_ids=(notice.observation_id,),
                           facts=r.EventFacts(proceeding="bankruptcy_petition", debtor_verified=True,
                                              affected_obligation_verified=True))
    return w, notice, proposal, admitted, claim


def test_scope_decided_in_episode_chain_is_refused_as_stale_chain_under_v2_closure() -> None:
    """The link resolver still admits an in-chain scope decision (the head cites the admitted
    revision), but the v2 event closure spans the whole adjudication chain and the chain's first
    revision cites the quarantined proposal that the admitted revision superseded. Publishing
    that closure would rely on a superseded link (``evidence_superseded``), so the resolver
    refuses it with a typed reason until an ``issue_scope``-based admission path exists."""
    w, _notice, proposal, admitted, claim = _scoped_episode_world()
    assert [x.link_id for x in w.links().admitted] == [admitted.link_id]  # D stays quarantined
    assert admitted.supersedes_link_id == proposal.link_id
    res = w.resolve(claims=[claim])
    assert res.episodes == () and _reasons(res) == ["adjudication_chain_cites_superseded_link"]
    with pytest.raises(r.ResolveError, match="adjudication_chain_cites_superseded_link"):
        w.resolve(claims=[claim], strict=True)


def test_standalone_scope_decision_fails_closed_in_the_resolver() -> None:
    """Interim v1 rule: a standalone admitting scope decision (its own head, no episode chain) is
    not persistable in W0 v1, so every resolver stage fails closed with a typed issue."""
    from src.bonds.default_events import publication as p

    w, notice, proposal, admitted, claim = _scoped_episode_world(in_chain=False)
    grid = [(x, m) for x in (A, D) for m in MONTHS]
    # Links: nothing admitted; typed issue with the revision the episode chain would have to cite.
    links = w.links()
    assert admitted is None and links.admitted == () and proposal.status == "quarantined"
    (issue,) = [i for i in links.issues if i.cusip9 == A]
    assert issue.reason == STANDALONE and issue.prospective_link_id is not None
    with pytest.raises(r.ResolveError, match=STANDALONE):
        r.resolve_links(w.reqs, observations=w.obs, adjudications=w.adjs, link_package=b.LINKS, knowledge_cutoff=K,
                        strict=True)
    # Follow-up: the issuer-level document cannot establish continuity for A -> unknown + typed issue.
    ev = r.ContinuityEvidence(A, JAN31, MAR31, "nondefault_continuous", "issuer_trustee_confirmation",
                              (notice.observation_id,))
    fu = r.build_followup(grid, securities=SECURITIES, observations=w.obs, continuity=[ev], adjudications=w.adjs,
                          links=w.all_links(), target_month=T, knowledge_cutoff=K, strict=False)
    assert all(f.status == "unknown" for f in fu.followups) and fu.issues == (f"{STANDALONE}:{A}",)
    with pytest.raises(r.ResolveError, match=STANDALONE):
        r.build_followup(grid, securities=SECURITIES, observations=w.obs, continuity=[ev], adjudications=w.adjs,
                         links=w.all_links(), target_month=T, knowledge_cutoff=K)
    # Episodes: the standalone record cites only the quarantined proposal -> not admitted (strict raises).
    res = w.resolve(claims=[claim])
    assert res.episodes == () and _reasons(res) == ["adjudication_link_not_admitted_at_cutoff"]
    with pytest.raises(r.ResolveError):
        w.resolve(claims=[claim], strict=True)
    # W0 v1 refuses to persist the decision without its event.
    with pytest.raises(c.ContractError, match="admitted_adjudication_without_event"):
        p.check_bundle(_bundle(w, (), (), (), grid))


def test_resolution_is_deterministic_in_fresh_processes() -> None:
    import subprocess
    import sys

    code = (
        "import importlib.util, sys; sys.argv=['x'];"
        "spec=importlib.util.spec_from_file_location('t', r'" + __file__ + "');"
        "m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m);"
        "w,y,n,cured,grid,cont=m._rich_world();"
        "bundle,_=m._pipeline(w,grid,claims=[m._state_claim(y,n),cured],continuity=cont,"
        "receipt=m._receipt());"
        "print(bundle.bundle_digest())"
    )
    root = Path(__file__).resolve().parents[1]
    digests = {
        subprocess.run([sys.executable, "-c", code], cwd=root, capture_output=True, text=True, check=True,
                       timeout=120).stdout.strip()
        for _ in range(2)
    }
    assert len(digests) == 1 and next(iter(digests)).startswith("sha256:")


# ---------------------------------------------------------------------------
# A3 v2 adapter phase 1: closed non-consensus construction only
# ---------------------------------------------------------------------------
def test_phase1_documentary_payment_round_trip_has_strict_nine_frame_closure() -> None:
    from src.bonds.default_events import publication as p

    w, doc, claim = _event_world(PAY, "payment_default")
    grid = [(B, month) for month in MONTHS]
    bundle, res = _pipeline(w, grid, claims=[claim])
    (event,) = res.episodes
    p.check_bundle(bundle)
    assert p.verify_schema(bundle).canonical_bytes() == bundle.canonical_bytes()
    strict_digest = p.event_dependency_digest(
        bundle.frames,
        knowledge_cutoff=K,
        knowledge_mode="current_run",
        evidence_observation_ids=event.evidence_observation_ids,
        onset_lower_evidence_ids=event.onset_lower_evidence_ids,
        link_ids=event.link_ids,
        adjudication_ids=event.adjudication_ids,
        proposal_evidence_ids=event.proposal_evidence_ids,
        exchange_relation_ids=event.exchange_relation_ids,
    )
    assert event.dependency_digest == strict_digest
    assert event.event_input_digest == c.DefaultEpisode.derive_input_digest(
        event.evidence_observation_ids,
        event.link_ids,
        event.adjudication_ids,
        event.proposal_evidence_ids,
        event.exchange_relation_ids,
        event.dependency_digest,
    )
    assert event.proposal_evidence_ids == event.exchange_relation_ids == ()
    assert bundle.frames["proposal_evidence"] == bundle.frames["exchange_relations"] == ()
    assert doc.package_id in {row.package_id for row in bundle.frames["source_packages"]}
    assert event.link_known_at <= event.evidence_known_at <= K


def test_phase1_explicit_human_state_ignores_incidental_transient_proposal() -> None:
    w, y, n = _state_world()
    w.accept("state-A", "accepted_state", y + n, A, role="human_reviewer")
    claim = r.EpisodeClaim(
        b.episode_id("state-A"),
        "default_state",
        onset_lower_exclusive=Q1,
        onset_upper_inclusive=Q3_26,
        onset_lower_evidence_ids=tuple(row.observation_id for row in n),
    )
    transient = _proposal(A, y, n)
    without = w.resolve(claims=[claim], strict=True)
    with_incidental = w.resolve(proposals=[transient], claims=[claim], strict=True)
    assert without.episodes == with_incidental.episodes
    (event,) = with_incidental.episodes
    assert "nport_consensus_state" not in event.corroboration_flags
    assert event.proposal_evidence_ids == ()
    grid = [(A, month) for month in MONTHS]
    bundle, _res = _pipeline(w, grid, proposals=[transient], claims=[claim])
    from src.bonds.default_events import publication as p

    p.check_bundle(bundle)


def test_phase1_missing_partial_and_incoherent_frames_are_typed_per_subject_refusals() -> None:
    w, doc, claim = _event_world(PAY, "payment_default")
    complete = w.dependency_frames()
    missing_key = dict(complete)
    missing_key.pop("family_contexts")
    duplicate = dict(complete)
    duplicate["observations"] = (*complete["observations"], doc)
    mismatch = dict(complete)
    mismatch["observations"] = ()
    missing_adjudication = dict(complete)
    missing_adjudication["adjudications"] = ()
    altered = dict(complete)
    altered["observations"] = (syn_replace(
        doc,
        first_seen_at=doc.first_seen_at + dt.timedelta(seconds=1),
    ),)
    cases = (
        (None, "contract:dependency_missing"),
        (missing_key, "contract:dependency_missing"),
        (duplicate, "contract:event_evidence_invalid"),
        (mismatch, "contract:event_evidence_invalid"),
        (missing_adjudication, "contract:event_evidence_invalid"),
        (altered, "contract:event_evidence_invalid"),
    )
    for frames, prefix in cases:
        result = w.resolve(claims=[claim], frames=frames)
        assert result.episodes == ()
        assert len(result.issues) == 1 and result.issues[0].reason.startswith(prefix)
    assert World().resolve(frames=None, strict=True).episodes == ()


def test_phase1_strict_dependency_check_runs_before_derived_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.bonds.default_events import publication as p

    w, doc, claim = _event_world(PAY, "payment_default")
    links = w.all_links()
    packages = tuple(row for row in b.PACKAGES if row.package_id != doc.package_id)
    frames = b.dependency_frames(
        source_packages=packages,
        observations=w.obs,
        event_links=links,
        adjudications=w.adjs,
    )
    derived_called = False

    def forbidden_derive(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal derived_called
        derived_called = True
        raise AssertionError("non-strict derivation ran before strict closure")

    monkeypatch.setattr(p, "derive_event_fields", forbidden_derive)
    result = r.resolve_episodes(
        tuple(w.adjs),
        observations=tuple(w.obs),
        links=links,
        packages=packages,
        claims=(claim,),
        knowledge_cutoff=K,
        strict=False,
        frames=frames,
    )
    assert result.episodes == ()
    assert result.issues[0].reason.startswith("contract:dependency_missing")
    assert not derived_called

    missing_ancestor = c.uuid5_of("phase1-missing-package-ancestor", "edgar")
    revised_owner = syn_replace(b.EDGAR, revision_of_package_id=missing_ancestor)
    revision_packages = tuple(
        revised_owner if row.package_id == b.EDGAR.package_id else row for row in b.PACKAGES
    )
    revision_frames = b.dependency_frames(
        source_packages=revision_packages,
        observations=w.obs,
        event_links=links,
        adjudications=w.adjs,
    )
    missing_ancestor_result = r.resolve_episodes(
        tuple(w.adjs),
        observations=tuple(w.obs),
        links=links,
        packages=revision_packages,
        claims=(claim,),
        knowledge_cutoff=K,
        strict=False,
        frames=revision_frames,
    )
    assert missing_ancestor_result.issues[0].reason.startswith("contract:dependency_missing")
    assert not derived_called


def test_phase1_missing_direct_evidence_and_link_rows_refuse_without_runtime_errors() -> None:
    w, _doc, claim = _event_world(PAY, "payment_default")
    links = w.all_links()
    no_observations = b.dependency_frames(event_links=links, adjudications=w.adjs)
    missing_observation = r.resolve_episodes(
        tuple(w.adjs), observations=(), links=links, packages=tuple(b.PACKAGES), claims=(claim,),
        knowledge_cutoff=K, frames=no_observations, strict=False,
    )
    assert _reasons(missing_observation) == ["adjudication_link_not_admitted_at_cutoff"]

    no_links = b.dependency_frames(observations=w.obs, adjudications=w.adjs)
    missing_link = r.resolve_episodes(
        tuple(w.adjs), observations=tuple(w.obs), links=(), packages=tuple(b.PACKAGES), claims=(claim,),
        knowledge_cutoff=K, frames=no_links, strict=False,
    )
    assert _reasons(missing_link) == ["adjudication_link_outside_inventory"]


def test_phase1_generators_permutations_and_relied_package_changes_are_deterministic() -> None:
    w, _doc, claim = _event_world(PAY, "payment_default")

    def resolve(reverse: bool, generators: bool, packages=tuple(b.PACKAGES)):  # type: ignore[no-untyped-def]
        observations = tuple(reversed(w.obs)) if reverse else tuple(w.obs)
        links = tuple(reversed(w.all_links())) if reverse else w.all_links()
        adjudications = tuple(reversed(w.adjs)) if reverse else tuple(w.adjs)
        package_rows = tuple(reversed(packages)) if reverse else tuple(packages)
        materialized = b.dependency_frames(
            source_packages=package_rows,
            observations=observations,
            event_links=links,
            adjudications=adjudications,
        )
        frame_rows = {
            name: (row for row in rows) if generators else rows
            for name, rows in materialized.items()
        }
        wrap = (lambda rows: (row for row in rows)) if generators else (lambda rows: rows)
        return r.resolve_episodes(
            wrap(adjudications),
            observations=wrap(observations),
            links=wrap(links),
            packages=wrap(package_rows),
            claims=wrap((claim,)),
            knowledge_cutoff=K,
            strict=True,
            frames=frame_rows,
        )

    expected = resolve(False, False)
    assert resolve(True, False).episodes == expected.episodes
    assert resolve(False, True).episodes == expected.episodes
    altered_package = syn_replace(b.EDGAR, retrieved_at=b.EDGAR.retrieved_at + dt.timedelta(seconds=1))
    altered_packages = tuple(altered_package if row.package_id == b.EDGAR.package_id else row for row in b.PACKAGES)
    altered = resolve(False, False, altered_packages)
    assert altered.episodes[0].dependency_digest != expected.episodes[0].dependency_digest
    assert altered.episodes[0].event_input_digest != expected.episodes[0].event_input_digest


def test_phase1_transitive_adjudication_support_binds_time_and_refuses_after_cutoff() -> None:
    def world_with_support(public_at: dt.datetime):
        w, doc, claim = _event_world(PAY, "payment_default")
        support = b.passage("phase1-transitive-support", cusip9=None, public=public_at)
        w.obs.append(support)
        direct_link = w.link(doc, B)
        original = b.adjudicate(
            claim.episode_id, "accepted_event", [doc, support], [direct_link], at=b.ADJ_AT,
        )
        head = b.adjudicate(
            claim.episode_id,
            "accepted_event",
            [doc],
            [direct_link],
            at=b.ADJ_AT + dt.timedelta(minutes=1),
            supersedes=original,
        )
        w.adjs[:] = [original, head]
        return w, claim

    support_at = K - dt.timedelta(hours=1)
    admitted_world, admitted_claim = world_with_support(support_at)
    (event,) = admitted_world.resolve(claims=[admitted_claim], strict=True).episodes
    assert event.evidence_known_at == support_at

    late_world, late_claim = world_with_support(K + dt.timedelta(seconds=1))
    late = late_world.resolve(claims=[late_claim])
    assert late.episodes == ()
    assert late.issues[0].reason == "evidence_known_after_cutoff"


def test_phase1_proposal_and_exchange_paths_remain_fail_closed() -> None:
    w, y, n = _state_world()
    transient = _proposal(A, y, n)
    w.accept("state-A", "accepted_state", y + n, A, role="policy_rule_engine")
    result = w.resolve(proposals=[transient])
    assert _reasons(result) == ["proposal_evidence_not_closed"]
    with pytest.raises(r.ResolveError, match="proposal_evidence_not_closed"):
        w.resolve(proposals=[transient], strict=True)

    persisted = c.ProposalEvidence.create(
        cusip9=A,
        proposed_status="accepted_state",
        basis="synthetic persisted proposal",
        onset_lower_exclusive=Q1,
        onset_upper_inclusive=Q3_26,
        onset_lower_evidence_ids=c.sorted_uuids(row.observation_id for row in n),
        onset_upper_evidence_ids=c.sorted_uuids(row.observation_id for row in y),
        evidence_observation_ids=c.sorted_uuids(row.observation_id for row in (*n, *y)),
        family_evidence_ids=(),
        corroboration_adjudication_ids=(),
        evidence_known_at=max(row.public_available_at for row in (*n, *y)),
        policy_digest=c.POLICY_DIGEST,
    )
    w2, y2, n2 = _state_world()
    w2.accept(
        "state-A", "accepted_state", y2 + n2, A, role="policy_rule_engine", proposals=(persisted,),
    )
    supplied = w2.resolve(
        proposals=[_proposal(A, y2, n2)], proposal_evidence=(persisted,),
    )
    assert _reasons(supplied) == ["proposal_lineage_not_persistable"]

    ancestry_world, ancestry_y, ancestry_n = _state_world()
    ancestry_links = [ancestry_world.link(row, A) for row in (*ancestry_n, *ancestry_y)]
    ancestry_subject = b.episode_id("state-A")
    ancestor = b.adjudicate(
        ancestry_subject,
        "accepted_state",
        [*ancestry_n, *ancestry_y],
        ancestry_links,
        proposals=(persisted,),
    )
    ancestry_head = b.adjudicate(
        ancestry_subject,
        "accepted_state",
        [*ancestry_n, *ancestry_y],
        ancestry_links,
        at=b.ADJ_AT + dt.timedelta(minutes=1),
        supersedes=ancestor,
    )
    ancestry_world.adjs[:] = [ancestor, ancestry_head]
    ancestry_claim = r.EpisodeClaim(
        ancestry_subject,
        "default_state",
        onset_lower_exclusive=Q1,
        onset_upper_inclusive=Q3_26,
        onset_lower_evidence_ids=tuple(row.observation_id for row in ancestry_n),
    )
    ancestry = ancestry_world.resolve(
        claims=[ancestry_claim], proposal_evidence=(persisted,),
    )
    assert _reasons(ancestry) == ["proposal_lineage_not_persistable"]

    precedence_world, precedence_doc, precedence_claim = _event_world(PAY, "payment_default")
    precedence_world.adjs[0] = b.adjudicate(
        precedence_claim.episode_id,
        "accepted_event",
        [precedence_doc],
        [precedence_world.link(precedence_doc, B)],
        proposals=(persisted,),
    )
    invalid_resolution = r.EpisodeClaim(
        precedence_claim.episode_id,
        precedence_claim.primary_type,
        onset_date=precedence_claim.onset_date,
        onset_lower_evidence_ids=precedence_claim.onset_lower_evidence_ids,
        resolution_date=precedence_claim.onset_date,
        facts=precedence_claim.facts,
    )
    assert _reasons(precedence_world.resolve(
        claims=[invalid_resolution], proposal_evidence=(persisted,),
    )) == ["resolution_date_without_evidence"]

    event_world, _doc, payment = _event_world(PAY, "payment_default")
    corroborating_exchange = r.EpisodeClaim(
        payment.episode_id,
        payment.primary_type,
        corroborating_types=("distressed_exchange",),
        onset_date=payment.onset_date,
        onset_lower_evidence_ids=payment.onset_lower_evidence_ids,
        facts=payment.facts,
    )
    assert _reasons(event_world.resolve(claims=[corroborating_exchange])) == [
        "exchange_pairing_not_persistable"
    ]
    invalid_exchange_resolution = r.EpisodeClaim(
        payment.episode_id,
        payment.primary_type,
        corroborating_types=("distressed_exchange",),
        onset_date=payment.onset_date,
        onset_lower_evidence_ids=payment.onset_lower_evidence_ids,
        resolution_date=payment.onset_date,
        facts=payment.facts,
    )
    assert _reasons(event_world.resolve(claims=[invalid_exchange_resolution])) == [
        "resolution_date_without_evidence"
    ]


def test_phase1_non_consensus_bundle_digest_is_stable_in_fresh_processes() -> None:
    import subprocess
    import sys

    code = (
        "import importlib.util, sys; sys.argv=['x'];"
        "spec=importlib.util.spec_from_file_location('t', r'" + __file__ + "');"
        "m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m);"
        "w,_doc,claim=m._event_world(m.PAY,'payment_default');"
        "bundle,_=m._pipeline(w,[(m.B,x) for x in m.MONTHS],claims=[claim]);"
        "print(bundle.bundle_digest())"
    )
    root = Path(__file__).resolve().parents[1]
    digests = {
        subprocess.run(
            [sys.executable, "-c", code], cwd=root, capture_output=True, text=True, check=True, timeout=120,
        ).stdout.strip()
        for _ in range(2)
    }
    assert len(digests) == 1 and next(iter(digests)).startswith("sha256:")
