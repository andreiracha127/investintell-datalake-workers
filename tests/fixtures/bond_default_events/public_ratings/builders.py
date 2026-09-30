"""SYNTHETIC builders for public_ratings tests (W2b).

No real issuer, CUSIP, agency, rating file or filing: agencies are ``SYNTHETIC-AGENCY-*``
and CUSIPs reuse the shared ``ZZ#SYN<nn>`` convention of ``synthetic.py``.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import uuid
from pathlib import Path

from src.bonds.default_events import contracts as c
from src.bonds.default_events import public_ratings as pr
from src.bonds.default_events import publication as pub

UTC = dt.timezone.utc
HERE = Path(__file__).resolve().parent


def _load_synthetic():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location("bond_default_synthetic_for_ratings", HERE.parent / "synthetic.py")
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


syn = _load_synthetic()
K = dt.datetime(2026, 9, 25, 0, 0, tzinfo=UTC)
RETRIEVED_AT = syn.RETRIEVED_AT
cusip = syn.synthetic_cusip
security_id = syn.security_id

AGENCY = "SYNTHETIC-AGENCY-1"
AGENCY_2 = "SYNTHETIC-AGENCY-2"
AGENCY_3 = "SYNTHETIC-AGENCY-3"
LONG_TERM, GLOBAL = "long_term", "global"
SCOPES = tuple(pr.RatingScope(a, LONG_TERM, GLOBAL) for a in (AGENCY, AGENCY_2, AGENCY_3))

Span = tuple[dt.date, dt.date] | None


def months(first: dt.date, count: int) -> list[dt.date]:
    return [c.add_months(first, i) for i in range(count)]


def grid(cusips: list[str], month_keys: list[dt.date]) -> list[tuple[str, dt.date]]:
    return [(x, m) for x in cusips for m in month_keys]


def agency_package(external_id: str, *, effective: Span = (dt.date(2020, 1, 1), dt.date(2026, 6, 30)),
                   public: Span = (dt.date(2020, 1, 1), dt.date(2026, 6, 30)),
                   first_public: dt.datetime = dt.datetime(2026, 7, 15, tzinfo=UTC)) -> c.SourcePackage:
    """Approved SYNTHETIC agency package with separate effective/public verified coverage."""
    e_start, e_end = effective if effective else (None, None)
    p_start, p_end = public if public else (None, None)
    return c.SourcePackage.create(
        source_family="agency_rocr_xbrl",
        external_id=external_id,
        content_sha256=syn._sha(f"{external_id}|content"),
        raw_sha256=syn._sha(f"{external_id}|raw"),
        header_sha256=None,
        member_sha256s={"primary": syn._sha(f"{external_id}|member")},
        official_url=None,
        accession_number=None,
        rights_state="approved",
        rights_ref="SYNTHETIC-AUTHORIZATION-RATINGS",
        parser_version="synthetic-parser-1",
        schema_version="synthetic-schema-1",
        retrieved_at=RETRIEVED_AT,
        first_verified_public_at=first_public,
        public_time_basis="rocr_file_creation",
        public_time_evidence="SYNTHETIC fixture: no real public-time evidence",
        source_coverage_start=e_start,
        source_coverage_end=e_end,
        public_coverage_start=p_start,
        public_coverage_end=p_end,
        effective_coverage_start=e_start,
        effective_coverage_end=e_end,
        raw_locator=f"synthetic/agency_rocr_xbrl/{external_id}.bin",
        revision_of_package_id=None,
        sec_run_id=None,
        sec_package_id=None,
    )


PKG = agency_package("SYNTHETIC-ROCR-RATINGS-1")
PKG_2 = agency_package("SYNTHETIC-ROCR-RATINGS-2")
PKG_3 = agency_package("SYNTHETIC-ROCR-RATINGS-3")
EDGAR = syn._package("sec_edgar_document", "SYNTHETIC-RATINGS-EDGAR", "public_government_record",
                     first_public=RETRIEVED_AT, basis="internal_record")
LINKS = syn.new_package("link_batch", "SYNTHETIC-RATINGS-LINKS")


def action(locator: str, cusip9: str | None, *, symbol: str | None = "BB+", rac: str | None = "AF",
           on: dt.date, public: dt.datetime, package: c.SourcePackage = PKG, agency: str = AGENCY,
           subject: str = "instrument", rating_type: str | None = LONG_TERM, scale: str | None = GLOBAL,
           first_seen: dt.datetime | None = None, revision: str = "original",
           supersedes: c.CreditObservation | None = None) -> c.CreditObservation:
    """One SYNTHETIC agency action (ROCR-style lexical fields)."""
    return syn._observation(
        package, locator, "agency_action",
        cusip_raw=cusip9, cusip9=cusip9, security_id=security_id(cusip9) if cusip9 else None,
        effective_date=on, date_precision="day",
        public_available_at=public, public_time_basis="rocr_file_creation",
        first_seen_at=first_seen if first_seen is not None else max(public, RETRIEVED_AT),
        agency_name=agency, agency_subject_kind=subject, agency_rating_type=rating_type,
        agency_scale=scale, agency_currency="USD", agency_rating_symbol=symbol,
        agency_action_classification=rac, agency_action_date=on, agency_file_creation_at=public,
        revision_kind=revision, supersedes_observation_id=supersedes.observation_id if supersedes else None,
    )


def at(day: dt.date, hour: int = 12) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def link(o: c.CreditObservation, cusip9: str, *, valid_from: dt.date = dt.date(2020, 1, 1),
         valid_to: dt.date | None = None, status: str = "admitted", scope: str = "issue",
         known: dt.datetime = dt.datetime(2026, 9, 21, 9, tzinfo=UTC),
         supersedes: c.EventLink | None = None) -> c.EventLink:
    refs = (f"synthetic:agency-instrument:{cusip9}",) if status == "admitted" else ()
    return c.EventLink.create(
        package_id=LINKS.package_id, observation_id=o.observation_id, security_id=security_id(cusip9),
        cusip9=cusip9, obligor_id=f"SYN-OBLIGOR-{cusip9[:6]}", affected_scope=scope,
        valid_from=valid_from, valid_to=valid_to, link_known_at=known,
        identity_evidence_refs=refs, identity_evidence_digest=c.digest_of(list(refs)),
        status=status, rationale="SYNTHETIC agency instrument link",
        supersedes_link_id=supersedes.link_id if supersedes else None,
    )


ISSUER_DOCS = syn._package("issuer_public_document", "SYNTHETIC-RATINGS-ISSUER-DOCS", "public_document_internal_use",
                           first_public=RETRIEVED_AT, basis="first_verified_retrieval")
ADJUDICATIONS = syn.new_package("adjudication_batch", "SYNTHETIC-RATINGS-ADJ")


def episode(cusip9: str, label: str, *, upper: dt.date, lower: dt.date | None = None,
            resolution: dt.date | None = None,
            known: dt.datetime = dt.datetime(2026, 9, 22, 12, tzinfo=UTC),
            resolution_known: dt.datetime | None = None) -> c.DefaultEpisode:
    """SYNTHETIC accepted episode (the resolver reads onset/resolution/known time).

    Its evidence passage, issue link and accepting adjudication are real W0 rows, and every
    derived field (knowledge times, ``dependency_digest``, ``event_input_digest``) comes from
    W0 ``publication.derive_event_fields`` over them; nothing is hashed by hand.
    """
    episode_id = c.uuid5_of("synthetic_rating_episode", label)
    admission = "accepted_state" if lower is None else "accepted_event"
    evidence = syn._observation(
        ISSUER_DOCS, f"rating-episode-{label}", "issuer_document_passage",
        public_available_at=known, public_time_basis="first_verified_retrieval", first_seen_at=known,
        document_quote=f"SYNTHETIC default notice {label}", document_location=f"notice-{label}.pdf#p1",
        document_sha256=syn._sha(f"rating-episode-{label}"),
    )
    issue_link = syn._link(LINKS, evidence, cusip9, f"SYN-OBLIGOR-{cusip9[:6]}", "issue", known)
    accepting = syn._adjudication(ADJUDICATIONS, episode_id, admission, "human_reviewer", known,
                                  [evidence], [issue_link])
    frames = {
        "source_packages": [ISSUER_DOCS, LINKS, ADJUDICATIONS], "observations": [evidence],
        "event_links": [issue_link], "adjudications": [accepting],
    }
    derived = pub.derive_event_fields(
        frames, knowledge_cutoff=K, knowledge_mode="historical_reconstruction",
        evidence_observation_ids=[evidence.observation_id],
        onset_lower_evidence_ids=[evidence.observation_id] if lower is not None else [],
        link_ids=[issue_link.link_id], adjudication_ids=[accepting.adjudication_id],
        proposal_evidence_ids=[], exchange_relation_ids=[],
    )
    return c.DefaultEpisode(
        security_id=security_id(cusip9), episode_id=episode_id,
        cusip9=cusip9, obligor_id=f"SYN-OBLIGOR-{cusip9[:6]}",
        issuer_episode_id=c.uuid5_of("synthetic_rating_issuer_episode", label),
        primary_type="default_state" if lower is None else "payment_default",
        # Explicit human state claim (no N-PORT proposal closure), evidenced by the issuer passage.
        corroboration_flags=("issuer_document",) if lower is None else ("issuer_document", "payment_default"),
        admission_status=admission,
        timing_class=c.derive_timing_class(lower, upper),
        onset_date=upper if lower is not None and lower == upper - dt.timedelta(days=1) else None,
        onset_lower_exclusive=lower, onset_upper_inclusive=upper,
        recognition_date=None,
        resolution_date=resolution,
        resolution_refs=(c.uuid5_of("synthetic_rating_resolution", label),) if resolution else (),
        resolution_known_at=(resolution_known or known) if resolution else None, alias_spell_id=None,
        **derived,
    )


def bench_cusip(number: int) -> str:
    """SYNTHETIC CUSIP9 for large grids (``ZZ#B<nnnn>`` + check digit)."""
    base = f"ZZ#B{number:04d}"
    return base + c.cusip_check_digit(base)


_DENSE_SYMBOLS = ("BB+", "BB", "BB-", "B+", "B", "BBB-", "Ba1", "B2")


def dense_history(n_cusips: int, n_months: int, n_actions: int,
                  first: dt.date = dt.date(2013, 1, 1)) -> tuple[list[tuple[str, dt.date]], list[c.CreditObservation]]:
    """Grid of ``n_cusips x n_months`` with ``n_actions`` spread-out direct actions per CUSIP."""
    month_keys = months(first, n_months)
    last_end = c.month_end(month_keys[-1])
    span = (last_end - first).days
    observations = []
    for i in range(n_cusips):
        cusip9 = bench_cusip(i)
        for j in range(n_actions):
            on = first + dt.timedelta(days=(j * span) // max(n_actions, 1) + i % 7)
            observations.append(action(
                f"dense-{i}-{j}", cusip9, symbol=_DENSE_SYMBOLS[(i + j) % len(_DENSE_SYMBOLS)],
                rac="AF" if j % 3 else "DG", on=on, public=at(on + dt.timedelta(days=1)),
                package=DENSE_PKG,
            ))
    return grid([bench_cusip(i) for i in range(n_cusips)], month_keys), observations


DENSE_PKG = agency_package("SYNTHETIC-ROCR-DENSE", effective=(dt.date(2000, 1, 1), dt.date(2026, 6, 30)),
                           public=(dt.date(2000, 1, 1), dt.date(2026, 6, 30)))


def resolve(grid_keys, *, packages=(PKG, PKG_2, PKG_3), observations=(), links=(), episodes=(),  # type: ignore[no-untyped-def]
            scopes=SCOPES, uncleared=(), k: dt.datetime = K, strict: bool = False, **kwargs):
    return pr.build_full_grid_ratings(
        grid_keys, knowledge_cutoff=k, packages=packages, observations=observations, links=links,
        episodes=episodes, rating_scopes=scopes, uncleared_sources=uncleared, strict=strict, **kwargs,
    )


def state_map(res, cusip9: str, view: str) -> dict[dt.date, tuple[str, str | None]]:  # type: ignore[no-untyped-def]
    """``month -> (state, bucket)`` of one CUSIP/view."""
    return {r.month: (r.state, r.bucket) for r in res.rows if r.cusip_id == cusip9 and r.view_kind == view}


def ids(*observations: c.CreditObservation) -> tuple[uuid.UUID, ...]:
    return c.sorted_uuids(o.observation_id for o in observations)
