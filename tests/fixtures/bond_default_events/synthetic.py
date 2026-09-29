"""Deterministic SYNTHETIC bond-credit bundles (no real issuer, CUSIP, CIK or filing).

Shared cross-repo fixtures: ``bundle_qualified.json`` and ``bundle_partial.json`` in this
directory are exactly ``write_fixtures()`` output (tests enforce equality). CUSIP9s use
the base ``ZZ#SYN<nn>`` plus a valid check digit, CIKs are ``99999999<nn>``, accessions
``9999999901-26-<n>``: all deliberately outside real identifier conventions.

Regenerate after any intentional contract/SQL change (also refreshes the SQL frame-spec
block and the policy/schema digests; ``--check`` verifies without writing)::

    .venv/Scripts/python.exe scripts/regen_bond_default_contracts.py
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import sys
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bonds.default_events import contracts as c
from src.bonds.default_events import public_ratings as pr
from src.bonds.default_events import publication as pub

UTC = dt.timezone.utc
HERE = Path(__file__).resolve().parent
TARGET_MONTH = dt.date(2026, 6, 1)
KNOWLEDGE_CUTOFF = dt.datetime(2026, 9, 25, 0, 0, tzinfo=UTC)
RETRIEVED_AT = dt.datetime(2026, 9, 20, 8, 0, tzinfo=UTC)
GRID_MONTHS = (dt.date(2026, 4, 1), dt.date(2026, 5, 1), dt.date(2026, 6, 1))


def _sha(label: str) -> str:
    return hashlib.sha256(f"synthetic|{label}".encode()).hexdigest()


def _digest(label: str) -> str:
    return "sha256:" + _sha(label)


def synthetic_cusip(number: int) -> str:
    base = f"ZZ#SYN{number:02d}"
    return base + c.cusip_check_digit(base)


CUSIP_A, CUSIP_B, CUSIP_C = synthetic_cusip(1), synthetic_cusip(2), synthetic_cusip(3)


def security_id(cusip: str) -> uuid.UUID:
    return c.uuid5_of("synthetic_security", cusip)


EPISODE_A = c.uuid5_of("synthetic_episode", "A-1")
EPISODE_B = c.uuid5_of("synthetic_episode", "B-1")
CANDIDATE_C = c.uuid5_of("synthetic_candidate", "C-1")


def _package(family: str, external_id: str, rights: str, *, rights_ref: str | None = None,
             accession: str | None = None, first_public: dt.datetime, basis: str,
             coverage: tuple[dt.date, dt.date] | None = None, retrieved_at: dt.datetime = RETRIEVED_AT,
             schema_version: str = "synthetic-schema-1") -> c.SourcePackage:
    start, end = coverage if coverage else (None, None)
    return c.SourcePackage.create(
        source_family=family,
        external_id=external_id,
        content_sha256=_sha(f"{external_id}|content"),
        raw_sha256=_sha(f"{external_id}|raw"),
        header_sha256=_sha(f"{external_id}|header") if accession else None,
        member_sha256s={"primary": _sha(f"{external_id}|member")},
        official_url=None,
        accession_number=accession,
        rights_state=rights,
        rights_ref=rights_ref,
        parser_version="synthetic-parser-1",
        schema_version=schema_version,
        retrieved_at=retrieved_at,
        first_verified_public_at=first_public,
        public_time_basis=basis,
        public_time_evidence="SYNTHETIC fixture: no real public-time evidence",
        source_coverage_start=start,
        source_coverage_end=end,
        public_coverage_start=start,
        public_coverage_end=end,
        effective_coverage_start=start,
        effective_coverage_end=end,
        raw_locator=f"synthetic/{family}/{external_id}.bin",
        revision_of_package_id=None,
        sec_run_id=None,
        sec_package_id=None,
    )


def new_package(family: str, external_id: str, rights: str = "internal_work_product") -> c.SourcePackage:
    """An extra package for test scenarios (defaults to an internal batch)."""
    return _package(family, external_id, rights, first_public=RETRIEVED_AT, basis="internal_record")


def _observation(package: c.SourcePackage, locator: str, kind: str, **fields: object) -> c.CreditObservation:
    base: dict[str, object] = {
        "package_id": package.package_id,
        "member_name": "primary",
        "row_locator": locator,
        "observation_kind": kind,
        "semantic_key": f"synthetic:{locator}",
        "accession_number": None,
        "holding_id": None,
        "cusip_raw": None,
        "cusip9": None,
        "isin_raw": None,
        "security_id": None,
        "issuer_cik": None,
        "registrant_cik": None,
        "series_id": None,
        "fund_family_id": None,
        "issuer_type_raw": None,
        "asset_category_raw": None,
        "report_date": None,
        "effective_date": None,
        "date_precision": "unknown",
        "effective_lower_exclusive": None,
        "effective_upper_inclusive": None,
        "acceptance_raw": None,
        "acceptance_at": None,
        "public_available_at": None,
        "public_time_basis": None,
        "first_seen_at": RETRIEVED_AT,
        "nport_is_default": None,
        "nport_arrears_or_deferral": None,
        "nport_paid_in_kind": None,
        "field_presence": {},
        "agency_name": None,
        "agency_subject_kind": None,
        "agency_rating_type": None,
        "agency_scale": None,
        "agency_currency": None,
        "agency_rating_symbol": None,
        "agency_action_classification": None,
        "agency_action_date": None,
        "agency_file_creation_at": None,
        "document_quote": None,
        "document_location": None,
        "document_sha256": None,
        "revision_kind": "original",
        "supersedes_observation_id": None,
    }
    base.update(fields)
    return c.CreditObservation.create(**base)


def _nport(package: c.SourcePackage, locator: str, cusip: str, series: str, family: str, cik: str,
           flag: str, acceptance_raw: str) -> c.CreditObservation:
    accepted = c.edgar_acceptance_to_utc(acceptance_raw)
    return _observation(
        package, locator, "nport_holding",
        accession_number=f"9999999901-26-{locator[-6:]}",
        holding_id=f"SYN-H-{locator}",
        cusip_raw=cusip,
        cusip9=cusip,
        security_id=security_id(cusip),
        registrant_cik=cik,
        series_id=series,
        fund_family_id=family,
        issuer_type_raw="CORP",
        asset_category_raw="DBT",
        report_date=dt.date(2026, 3, 31),
        acceptance_raw=acceptance_raw,
        acceptance_at=accepted,
        public_available_at=accepted,
        public_time_basis="edgar_acceptance_datetime",
        nport_is_default=flag,
        nport_arrears_or_deferral="N",
        nport_paid_in_kind="N",
        field_presence={
            "nport_arrears_or_deferral": "present",
            "nport_is_default": "present",
            "nport_paid_in_kind": "present",
        },
    )


def _link(package: c.SourcePackage, observation: c.CreditObservation, cusip: str, obligor: str,
          scope: str, known_at: dt.datetime, status: str = "admitted") -> c.EventLink:
    refs = (f"synthetic:indenture:{cusip}",) if status == "admitted" else ()
    return c.EventLink.create(
        package_id=package.package_id,
        observation_id=observation.observation_id,
        security_id=security_id(cusip),
        cusip9=cusip,
        obligor_id=obligor,
        affected_scope=scope,
        valid_from=dt.date(2020, 1, 1),
        valid_to=None,
        link_known_at=known_at,
        identity_evidence_refs=refs,
        identity_evidence_digest=c.digest_of(list(refs)),
        status=status,
        rationale="SYNTHETIC link fixture",
        supersedes_link_id=None,
    )


NCEN_PERIOD = dt.date(2025, 12, 31)
REPORT_DATE = dt.date(2026, 3, 31)
#: Registrant -> (voting series, adviser SEC file number, N-CEN acceptance raw, accession suffix).
NCEN_REGISTRANTS = {
    "9999999911": ("S999999001", "801-99911", "20260310101500", "000101"),
    # Registrant 12's N-CEN is accepted after its N-PORT vote became public (later family time).
    "9999999912": ("S999999002", "801-99912", "20260610101500", "000102"),
    "9999999913": ("S999999003", "801-99913", "20260310101500", "000103"),
}


def ncen_header(accession: str, accept_raw: str) -> c.SourcePackage:
    accepted = c.edgar_acceptance_to_utc(accept_raw)
    return _package("sec_ncen_acceptance_header", f"SYNTHETIC-NCEN-HDR-{accession}", "public_government_record",
                    accession=accession, first_public=accepted, basis="edgar_acceptance_datetime")


def ncen_filing(package: c.SourcePackage, header: c.SourcePackage, cik: str, series: tuple[str, ...],
                adviser_fn: str, accept_raw: str, *, period: dt.date = NCEN_PERIOD, form: str = "N-CEN",
                locator: str | None = None, family_answer: str | None = "N", family_name: str | None = None,
                advisers: tuple[dict, ...] | None = None, public_at: dt.datetime | None = None,  # type: ignore[type-arg]
                version_proofs: tuple[uuid.UUID, ...] = (), parse_status: str = "parsed",
                reasons: tuple[str, ...] = (), supersedes: uuid.UUID | None = None,
                underwriters: tuple[dict, ...] = ()) -> c.NcenFilingEvidence:  # type: ignore[type-arg]
    accepted = c.edgar_acceptance_to_utc(accept_raw)
    records = advisers if advisers is not None else tuple(
        {"series_id": s, "role": "adviser", "file_number_raw": adviser_fn, "crd_raw": None, "lei_raw": None}
        for s in series)
    fields = c.STRUCTURED_KINDS["advisers"][0]
    uw_fields = c.STRUCTURED_KINDS["underwriters"][0]
    return c.NcenFilingEvidence.create(
        package_id=package.package_id, accession_number=header.accession_number,
        row_locator=locator or f"ncen-{cik}", registrant_cik=cik, form_type=form, report_period_end=period,
        filing_date=dt.date(int(accept_raw[:4]), int(accept_raw[4:6]), int(accept_raw[6:8])),
        header_package_id=header.package_id, acceptance_raw=accept_raw, acceptance_at=accepted,
        public_available_at=public_at or accepted, first_seen_at=RETRIEVED_AT,
        public_time_basis="edgar_acceptance_datetime" if public_at is None else "first_verified_retrieval",
        version_evidence_filing_ids=c.sorted_uuids(version_proofs), parse_status=parse_status,
        reasons=c.sorted_texts(reasons), family_answer=family_answer, family_name_raw=family_name,
        reported_series_ids=c.sorted_texts(series),
        adviser_records=tuple(sorted(records, key=lambda r: c.structured_sort_key(fields, r))),
        underwriter_records=tuple(sorted(underwriters, key=lambda r: c.structured_sort_key(uw_fields, r))),
        supersedes_filing_evidence_id=supersedes,
    )


def ncen_inputs() -> tuple[list[c.SourcePackage], list[c.NcenFilingEvidence]]:
    """One N-CEN XML package plus a header package and a parsed filing per registrant."""
    ncen_pkg = _package("sec_ncen_public_xml", "SYNTHETIC-NCEN-2025", "public_government_record",
                        first_public=dt.datetime(2026, 3, 10, 14, 15, tzinfo=UTC), basis="archived_release_metadata")
    packages = [ncen_pkg]
    filings = []
    for cik, (series, fn, accept, suffix) in NCEN_REGISTRANTS.items():
        header = ncen_header(f"9999999901-26-{suffix}", accept)
        packages.append(header)
        filings.append(ncen_filing(ncen_pkg, header, cik, (series,), fn, accept))
    return packages, filings


def _adjudication(package: c.SourcePackage, subject: uuid.UUID, status: str, role: str,
                  at: dt.datetime, evidence: list[c.CreditObservation], links: list[c.EventLink],
                  *, subject_kind: str = "issue_episode",
                  supersedes: c.Adjudication | None = None,
                  proposals: tuple[c.ProposalEvidence, ...] = (),
                  support: tuple[dt.date | None, dt.date | None] = (None, None)) -> c.Adjudication:
    return c.Adjudication.create(
        package_id=package.package_id,
        subject_kind=subject_kind,
        subject_id=subject,
        status=status,
        supersedes_adjudication_id=supersedes.adjudication_id if supersedes else None,
        policy_digest=c.POLICY_DIGEST,
        reviewer_id=f"synthetic-{role}",
        reviewer_role=role,
        adjudicated_at=at,
        rationale=f"SYNTHETIC adjudication: {status}",
        evidence_observation_ids=c.sorted_uuids(o.observation_id for o in evidence),
        link_ids=c.sorted_uuids(x.link_id for x in links),
        proposal_evidence_ids=c.sorted_uuids(p.proposal_evidence_id for p in proposals),
        support_valid_from=support[0],
        support_valid_to=support[1],
    )


def _event(frames: dict, k: dt.datetime, mode: str, cusip: str, episode: uuid.UUID, primary: str,  # type: ignore[type-arg]
           flags: tuple[str, ...], admission: str,
           lower: dt.date | None, upper: dt.date, onset_date: dt.date | None,
           evidence: list[c.CreditObservation], links: list[c.EventLink],
           adjs: list[c.Adjudication], lower_evidence: tuple[c.CreditObservation, ...] = (),
           proposals: tuple[c.ProposalEvidence, ...] = (), relations: tuple[c.ExchangeRelation, ...] = (),
           ) -> c.DefaultEpisode:
    """Event whose knowledge times and digests are derived by the W0 builder API over ``frames``."""
    derived = pub.derive_event_fields(
        frames, knowledge_cutoff=k, knowledge_mode=mode,
        evidence_observation_ids=[o.observation_id for o in evidence],
        onset_lower_evidence_ids=[o.observation_id for o in lower_evidence],
        link_ids=[x.link_id for x in links], adjudication_ids=[a.adjudication_id for a in adjs],
        proposal_evidence_ids=[p.proposal_evidence_id for p in proposals],
        exchange_relation_ids=[r.relation_id for r in relations],
    )
    return c.DefaultEpisode(
        security_id=security_id(cusip),
        episode_id=episode,
        cusip9=cusip,
        obligor_id=f"SYNTHETIC-OBLIGOR-{cusip[-3:]}",
        issuer_episode_id=c.uuid5_of("synthetic_issuer_episode", str(episode)),
        primary_type=primary,
        corroboration_flags=c.sorted_texts(flags),
        admission_status=admission,
        timing_class=c.derive_timing_class(lower, upper),
        onset_date=onset_date,
        onset_lower_exclusive=lower,
        onset_upper_inclusive=upper,
        recognition_date=None,
        resolution_date=None,
        resolution_refs=(),
        resolution_known_at=None,
        alias_spell_id=c.DefaultEpisode.derive_alias_spell_id(episode) if relations else None,
        **derived,
    )


RATING_SCOPES = (pr.RatingScope("SYNTHETIC-AGENCY", "long_term", "global"),)
UNCLEARED_RATING_SOURCES = (pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "unverified"),)


def _receipt(packages: list[c.SourcePackage], declarations: dict[str, Any]) -> c.ValidationReceipt:
    """Positive receipt; it qualifies the rating input only when approved agency packages exist."""
    rating_packages = c.rating_package_digest(packages)
    return c.ValidationReceipt(
        receipt_id=c.uuid5_of("synthetic_receipt", "Q-1"),
        verdict="qualified",
        scope="SYNTHETIC completeness receipt for grid 2026-04..2026-06",
        evidence_digest=_digest("receipt-evidence"),
        reviewer_id="synthetic-validator",
        issued_at=dt.datetime(2026, 9, 24, 12, 0, tzinfo=UTC),
        positive_evidence_count=3,
        rating_input_digest=c.rating_input_manifest_digest(declarations, packages) if rating_packages else None,
        rating_package_digest=rating_packages,
    )


def build_bundle(
    *,
    quality_state: str = "qualified",
    target_month: dt.date = TARGET_MONTH,
    knowledge_cutoff: dt.datetime = KNOWLEDGE_CUTOFF,
    with_events: bool = True,
    with_receipt: bool = True,
    alter_output: bool = False,
    knowledge_mode: str = "current_run",
) -> c.CreditBundle:
    """Build one synthetic bundle; ``quality_state='qualified'`` is promotable.

    A non-default K/mode is built as the default bundle rebound to that K/mode, so several
    builds share one unchanged ledger (a K-scoped decision is re-decided in a new batch).
    """
    if (knowledge_cutoff, knowledge_mode) != (KNOWLEDGE_CUTOFF, "current_run") and with_events:
        base = build_bundle(quality_state=quality_state, target_month=target_month, with_events=with_events,
                            with_receipt=with_receipt, alter_output=alter_output)
        return reassemble(base, knowledge_cutoff=knowledge_cutoff, knowledge_mode=knowledge_mode)
    qualified = quality_state == "qualified"
    nport_pkg = _package(
        "sec_nport_dera", "SYNTHETIC-NPORT-2026Q1", "public_government_record",
        first_public=dt.datetime(2026, 6, 1, tzinfo=UTC), basis="archived_release_metadata",
        coverage=(dt.date(2026, 1, 1), dt.date(2026, 3, 31)),
    )
    edgar_pkg = _package(
        "sec_edgar_document", "SYNTHETIC-8K-A", "public_government_record",
        accession="9999999901-26-000011", first_public=dt.datetime(2026, 5, 18, 20, 30, 15, tzinfo=UTC),
        basis="edgar_acceptance_datetime",
    )
    issuer_pkg = _package(
        "issuer_public_document", "SYNTHETIC-ISSUER-C", "public_document_internal_use",
        first_public=dt.datetime(2026, 7, 3, 4, 0, tzinfo=UTC), basis="date_only_next_day_boundary",
    )
    link_pkg = _package(
        "link_batch", "SYNTHETIC-LINKS-1", "internal_work_product",
        first_public=RETRIEVED_AT, basis="internal_record",
    )
    adj_pkg = _package(
        "adjudication_batch", "SYNTHETIC-ADJ-1", "internal_work_product",
        first_public=RETRIEVED_AT, basis="internal_record",
    )
    packages = [nport_pkg, edgar_pkg, issuer_pkg, link_pkg, adj_pkg]

    nport_b1 = _nport(nport_pkg, "row-000001", CUSIP_B, "S999999001", "SYN-FAMILY-1", "9999999911", "Y", "20260520101500")
    nport_b2 = _nport(nport_pkg, "row-000002", CUSIP_B, "S999999002", "SYN-FAMILY-2", "9999999912", "Y", "20260521093000")
    nport_c = _nport(nport_pkg, "row-000003", CUSIP_C, "S999999003", "SYN-FAMILY-3", "9999999913", "N", "20260520101500")
    edgar_accept = "20260518163015"
    edgar_a = _observation(
        edgar_pkg, "item-2.04-p3", "edgar_passage",
        accession_number="9999999901-26-000011",
        issuer_cik="9999999901",
        effective_date=dt.date(2026, 5, 15),
        date_precision="day",
        acceptance_raw=edgar_accept,
        acceptance_at=c.edgar_acceptance_to_utc(edgar_accept),
        public_available_at=c.edgar_acceptance_to_utc(edgar_accept),
        public_time_basis="edgar_acceptance_datetime",
        document_quote="SYNTHETIC: interest due 2026-04-15 unpaid; 30-day grace period expired uncured on 2026-05-15.",
        document_location="ex99-1.htm#p3",
        document_sha256=_sha("edgar-a-document"),
    )
    issuer_c = _observation(
        issuer_pkg, "notice-p1", "issuer_document_passage",
        date_precision="interval",
        effective_lower_exclusive=dt.date(2026, 3, 31),
        effective_upper_inclusive=dt.date(2026, 6, 30),
        public_available_at=c.date_only_public_available_at(dt.date(2026, 7, 2), "America/New_York"),
        public_time_basis="date_only_next_day_boundary",
        document_quote="SYNTHETIC: all scheduled payments on the notes were made in full through 2026-06-30.",
        document_location="notice.pdf#p1",
        document_sha256=_sha("issuer-c-document"),
    )
    observations = [nport_b1, nport_b2, nport_c, edgar_a, issuer_c]

    agency_c = None
    if qualified:
        agency_pkg = _package(
            "agency_rocr_xbrl", "SYNTHETIC-ROCR-1", "approved", rights_ref="SYNTHETIC-AUTHORIZATION-0001",
            first_public=dt.datetime(2026, 4, 11, 12, 0, tzinfo=UTC), basis="rocr_file_creation",
            coverage=(dt.date(2026, 1, 1), dt.date(2026, 6, 30)),
        )
        packages.append(agency_pkg)
        agency_c = _observation(
            agency_pkg, "action-1", "agency_action",
            cusip_raw=CUSIP_C,
            cusip9=CUSIP_C,
            security_id=security_id(CUSIP_C),
            effective_date=dt.date(2026, 4, 10),
            date_precision="day",
            public_available_at=dt.datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            public_time_basis="rocr_file_creation",
            agency_name="SYNTHETIC-AGENCY",
            agency_subject_kind="instrument",
            agency_rating_type="long_term",
            agency_scale="global",
            agency_currency="USD",
            agency_rating_symbol="BB+",
            agency_action_classification="AF",
            agency_action_date=dt.date(2026, 4, 10),
            agency_file_creation_at=dt.datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
        )
        observations.append(agency_c)

    link_a = _link(link_pkg, edgar_a, CUSIP_A, "SYNTHETIC-OBLIGOR-A", "issuer_affected_obligation",
                   dt.datetime(2026, 9, 21, 9, 0, tzinfo=UTC))
    link_b1 = _link(link_pkg, nport_b1, CUSIP_B, "SYNTHETIC-OBLIGOR-B", "issue", dt.datetime(2026, 9, 21, 10, 0, tzinfo=UTC))
    link_b2 = _link(link_pkg, nport_b2, CUSIP_B, "SYNTHETIC-OBLIGOR-B", "issue", dt.datetime(2026, 9, 21, 10, 0, tzinfo=UTC))
    link_q = _link(link_pkg, nport_c, CUSIP_A, "SYNTHETIC-OBLIGOR-A", "issue",
                   dt.datetime(2026, 9, 21, 11, 0, tzinfo=UTC), status="quarantined")
    links = [link_a, link_b1, link_b2, link_q]

    ncen_packages, ncen_filings = ncen_inputs()
    packages += ncen_packages

    adj_day1 = dt.datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
    adj_day2 = dt.datetime(2026, 9, 22, 12, 0, tzinfo=UTC)
    disputed_c = _adjudication(adj_pkg, CANDIDATE_C, "disputed", "human_reviewer", adj_day2, [nport_c], [],
                               subject_kind="candidate")
    adjudications = [disputed_c]
    events: list[c.DefaultEpisode] = []
    contexts: tuple[c.FamilyContext, ...] = ()
    memberships: tuple[c.FamilyMembership, ...] = ()
    proposals: list[c.ProposalEvidence] = []
    frames: dict = {"source_packages": packages, "observations": observations, "event_links": links,  # type: ignore[type-arg]
                    "ncen_filings": ncen_filings}
    if with_events:
        # Full FE-1 context of the 2026-03-31 voter universe (CUSIP B and C voters) and the
        # independent-family W1 proposal it supports.
        contexts, memberships = pub.build_family_frames(
            frames, [REPORT_DATE], knowledge_cutoff=knowledge_cutoff, knowledge_mode=knowledge_mode)
        frames.update(family_contexts=contexts, family_evidence=memberships)
        proposal_b = pub.derive_state_proposal(frames, cusip9=CUSIP_B, onset_upper_inclusive=REPORT_DATE,
                                               knowledge_cutoff=knowledge_cutoff, knowledge_mode=knowledge_mode)
        proposals = [proposal_b]
        cand_a = _adjudication(adj_pkg, EPISODE_A, "candidate", "extraction_proposer", adj_day1, [edgar_a], [link_a])
        acc_a = _adjudication(adj_pkg, EPISODE_A, "accepted_event", "human_reviewer", adj_day2, [edgar_a], [link_a],
                              supersedes=cand_a)
        acc_b = _adjudication(adj_pkg, EPISODE_B, "accepted_state", "policy_rule_engine", adj_day2,
                              [nport_b1, nport_b2], [link_b1, link_b2], proposals=(proposal_b,))
        adjudications += [cand_a, acc_a, acc_b]
        frames.update(adjudications=adjudications, proposal_evidence=proposals)
        events = [
            _event(frames, knowledge_cutoff, knowledge_mode, CUSIP_A, EPISODE_A, "payment_default",
                   ("edgar_document", "payment_default"), "accepted_event",
                   dt.date(2026, 5, 14), dt.date(2026, 5, 15), dt.date(2026, 5, 15), [edgar_a], [link_a],
                   [cand_a, acc_a], lower_evidence=(edgar_a,)),
            _event(frames, knowledge_cutoff, knowledge_mode, CUSIP_B, EPISODE_B, "default_state",
                   ("nport_consensus_state",), "accepted_state", None, REPORT_DATE, None,
                   [nport_b1, nport_b2], [link_b1, link_b2], [acc_b], proposals=(proposal_b,)),
        ]

    followups = [
        c.FollowUp(
            security_id=security_id(CUSIP_C), spell_id=c.uuid5_of("synthetic_spell", "C-1"),
            segment_id=c.uuid5_of("synthetic_segment", "C-1-1"), cusip9=CUSIP_C,
            interval_start_exclusive=dt.date(2026, 3, 31), interval_end_inclusive=dt.date(2026, 6, 30),
            status="nondefault_continuous", completeness_basis="issuer_trustee_confirmation",
            evidence_observation_ids=c.sorted_uuids([issuer_c.observation_id, nport_c.observation_id]),
            adjudication_ids=(), known_at=issuer_c.public_available_at,
        ),
        c.FollowUp(
            security_id=security_id(CUSIP_A), spell_id=c.uuid5_of("synthetic_spell", "A-1"),
            segment_id=c.uuid5_of("synthetic_segment", "A-1-1"), cusip9=CUSIP_A,
            interval_start_exclusive=dt.date(2026, 3, 31), interval_end_inclusive=dt.date(2026, 5, 14),
            status="unknown", completeness_basis="none", evidence_observation_ids=(), adjudication_ids=(),
            known_at=adj_day2,
        ),
    ]
    exits = [
        c.ExitEvidence(
            security_id=security_id(CUSIP_C), last_panel_month=dt.date(2026, 6, 1), cusip9=CUSIP_C,
            primary_reason="matured", flags=("matured", "target_end_censored"), next_observed_month=None,
            gap_months=None, scheduled_maturity=dt.date(2026, 7, 15), proven_repayment_date=None,
            evidence_observation_ids=(), known_at=adj_day2,
        ),
        c.ExitEvidence(
            security_id=security_id(CUSIP_A), last_panel_month=dt.date(2026, 5, 1), cusip9=CUSIP_A,
            primary_reason="unknown", flags=("distressed_candidate", "unknown"), next_observed_month=None,
            gap_months=None, scheduled_maturity=None, proven_repayment_date=None,
            evidence_observation_ids=(), known_at=adj_day2,
        ),
    ]

    declarations = pr.rating_declarations_record(
        RATING_SCOPES,
        () if qualified else UNCLEARED_RATING_SOURCES,
    )
    rating_input_digest = c.rating_input_manifest_digest(declarations, packages)
    receipt = _receipt(packages, declarations) if with_receipt else None
    receipt_digest = receipt.digest() if (receipt and qualified) else None
    coverage = [
        c.CoverageCell(
            period_label="2026", source="all", event_type="all", rating_stratum="all", exposure_cohort="all",
            state="qualified" if qualified else "partial",
            denominator_basis="independent_reference_enumeration" if qualified else "panel_exposure",
            denominator_count=3, exposed_issue_months=9, event_count=len(events), unlinked_count=0,
            date_uncertain_count=0, unknown_outcome_issue_months=1, source_frontier=dt.date(2026, 6, 30),
            lag_p50_days=3, lag_p90_days=5, lag_max_days=125, rationale="SYNTHETIC coverage cell",
            validation_receipt_digest=receipt_digest,
        ),
        c.CoverageCell(
            period_label="2026", source="agency_rocr", event_type="agency_issue_default", rating_stratum="all",
            exposure_cohort="all", state="not_applicable", denominator_basis="none", denominator_count=None,
            exposed_issue_months=0, event_count=0, unlinked_count=0, date_uncertain_count=0,
            unknown_outcome_issue_months=0, source_frontier=None, lag_p50_days=None, lag_p90_days=None,
            lag_max_days=None, rationale="SYNTHETIC: optional agency stream not enabled for events",
            validation_receipt_digest=None,
        ),
    ]

    overlay_from = {CUSIP_A: dt.date(2026, 5, 1), CUSIP_B: dt.date(2026, 4, 1)} if with_events else {}
    overlay_episode = {CUSIP_A: EPISODE_A, CUSIP_B: EPISODE_B}
    ratings = []
    grid = [(cusip, month) for cusip in (CUSIP_A, CUSIP_B, CUSIP_C) for month in GRID_MONTHS]
    for cusip, month in grid:
        for view in ("effective_audit", "public_pit"):
            overlay = overlay_episode[cusip] if cusip in overlay_from and month >= overlay_from[cusip] else None
            if cusip == CUSIP_C and agency_c is not None:
                ratings.append(c.RatingGridRow(
                    cusip_id=cusip, month=month, view_kind=view, bucket="BB",
                    state="observed" if month == GRID_MONTHS[0] else "carried_verified",
                    action_date=dt.date(2026, 4, 10), public_known_at=agency_c.public_available_at,
                    agency_source_ids=(agency_c.observation_id,), binding_link_ids=(),
                    coverage_frontier=dt.date(2026, 6, 30),
                    action_input_digest=c.rating_action_input_digest(view, [agency_c], [agency_pkg], []),
                    default_overlay_episode_id=None,
                ))
            else:
                ratings.append(c.RatingGridRow(
                    cusip_id=cusip, month=month, view_kind=view, bucket=None,
                    state="missing" if qualified else "rights_unverified",
                    action_date=None, public_known_at=None, agency_source_ids=(), binding_link_ids=(),
                    coverage_frontier=None, action_input_digest=None, default_overlay_episode_id=overlay,
                ))
    if alter_output:
        # Same inputs (same fingerprint and id), different output bytes: replay collision.
        coverage[0] = replace_row(coverage[0], rationale="SYNTHETIC coverage cell (different bytes)")

    return c.assemble_bundle(
        target_month=target_month,
        knowledge_cutoff=knowledge_cutoff,
        knowledge_mode=knowledge_mode,
        build_scope="complete" if qualified else "limited",
        quality_state=quality_state,
        code_digest=_digest("code-v1"),
        panel_publication_id=c.uuid5_of("synthetic_panel", "2026-06"),
        panel_grid=grid,
        issuer_mapping_digest=_digest("a1-mapping") if qualified else None,
        rating_declarations=declarations,
        rating_input_digest=rating_input_digest,
        validation_receipt=receipt,
        source_packages=packages,
        observations=observations,
        event_links=links,
        adjudications=adjudications,
        events=events,
        followups=followups,
        exit_evidence=exits,
        coverage=coverage,
        ratings=ratings,
        ncen_filings=ncen_filings,
        family_contexts=contexts,
        family_evidence=memberships,
        proposal_evidence=proposals,
        exchange_relations=(),
    )


def replace_row(row, **changes):  # type: ignore[no-untyped-def]
    """Copy of a contract row with ``changes`` applied (re-validated)."""
    values = {name: getattr(row, name) for name, _ in row.SPEC}
    values.update(changes)
    return type(row)(**values)


def rebind_dependencies(frames: dict, k: dt.datetime, mode: str) -> dict:  # type: ignore[type-arg]
    """Rebuild the (R, K, mode)-scoped closure for a new cutoff/mode: contexts and memberships,
    accepted proposals (same CUSIP/bounds/corroborations), adjudications citing them and every
    event's derived fields. A context whose universe is not public by K is dropped (and a
    proposal that is no longer derivable is kept as-is, so validation reports it)."""
    fr = {name: list(rows) for name, rows in frames.items()}
    dates = sorted({ctx.report_date for ctx in fr["family_contexts"]})
    base = {name: fr[name] for name in ("source_packages", "observations", "event_links", "ncen_filings", "adjudications")}
    contexts, members = pub.build_family_frames(base, dates, knowledge_cutoff=k, knowledge_mode=mode)
    fr["family_contexts"], fr["family_evidence"] = list(contexts), list(members)
    proposal_map: dict[uuid.UUID, c.ProposalEvidence] = {}
    for p in fr["proposal_evidence"]:
        try:
            proposal_map[p.proposal_evidence_id] = pub.derive_state_proposal(
                fr, cusip9=p.cusip9, onset_upper_inclusive=p.onset_upper_inclusive,
                onset_lower_exclusive=p.onset_lower_exclusive,
                corroboration_adjudication_ids=p.corroboration_adjudication_ids, knowledge_cutoff=k,
                knowledge_mode=mode)
        except c.ContractError:
            proposal_map[p.proposal_evidence_id] = p
    fr["proposal_evidence"] = list(proposal_map.values())
    # Proposal ids are (R, K, mode)-scoped, so a decision citing one is re-decided in a new
    # append-only batch that supersedes the old record (the old batch stays unchanged).
    adj_map: dict[uuid.UUID, uuid.UUID] = {}
    superseded = {a.supersedes_adjudication_id for a in fr["adjudications"]}
    stale = [a for a in fr["adjudications"] if a.proposal_evidence_ids and a.adjudication_id not in superseded
             and any(proposal_map[p].proposal_evidence_id != p for p in a.proposal_evidence_ids)]
    if stale:
        batch = _package("adjudication_batch", f"SYNTHETIC-ADJ-REBIND-{c.ts_text(k)}-{mode}",
                         "internal_work_product", first_public=RETRIEVED_AT, basis="internal_record")
        if all(p.package_id != batch.package_id for p in fr["source_packages"]):
            fr["source_packages"].append(batch)
        for a in stale:
            new = _readjudicate(a, package_id=batch.package_id, supersedes_adjudication_id=a.adjudication_id,
                                adjudicated_at=a.adjudicated_at + dt.timedelta(minutes=1),
                                proposal_evidence_ids=c.sorted_uuids(
                                    proposal_map[p].proposal_evidence_id for p in a.proposal_evidence_ids))
            adj_map[a.adjudication_id] = new.adjudication_id
            fr["adjudications"].append(new)
    events = []
    for ev in fr["events"]:
        values = {n: getattr(ev, n) for n, _ in ev.SPEC}
        values["adjudication_ids"] = c.sorted_uuids(adj_map.get(a, a) for a in ev.adjudication_ids)
        values["proposal_evidence_ids"] = c.sorted_uuids(
            proposal_map[p].proposal_evidence_id for p in ev.proposal_evidence_ids)
        edges = ("evidence_observation_ids", "onset_lower_evidence_ids", "link_ids", "adjudication_ids",
                 "proposal_evidence_ids", "exchange_relation_ids")
        values.update(pub.derive_event_fields(fr, knowledge_cutoff=k, knowledge_mode=mode,
                                              **{name: values[name] for name in edges}))
        events.append(c.DefaultEpisode(**values))
    fr["events"] = events
    return fr


def reassemble(bundle: c.CreditBundle, *, frames: dict | None = None, rebind: bool = False, **manifest_changes):  # type: ignore[no-untyped-def]
    """Re-derive a bundle from ``bundle``'s inputs with frame/manifest overrides."""
    m = bundle.manifest
    fr = dict(bundle.frames)
    fr.update(frames or {})
    args = {
        "target_month": m["target_month"],
        "knowledge_cutoff": m["knowledge_cutoff"],
        "knowledge_mode": m["knowledge_mode"],
        "build_scope": m["build_scope"],
        "quality_state": m["quality_state"],
        "code_digest": m["code_digest"],
        "panel_publication_id": m["panel_publication_id"],
        "panel_grid": bundle.panel_grid,
        "issuer_mapping_digest": m["issuer_mapping_digest"],
        "rating_declarations": m["rating_declarations"],
        "rating_input_digest": m["rating_input_digest"],
        "validation_receipt": m.receipt(),
    }
    args.update(manifest_changes)
    if rebind or (args["knowledge_cutoff"], args["knowledge_mode"]) != (m["knowledge_cutoff"], m["knowledge_mode"]):
        fr = rebind_dependencies(fr, args["knowledge_cutoff"], args["knowledge_mode"])
    if "rating_input_digest" not in manifest_changes:
        args["rating_input_digest"] = c.rating_input_manifest_digest(
            args["rating_declarations"], fr["source_packages"])
    return c.assemble_bundle(
        **args,
        source_packages=fr["source_packages"],
        observations=fr["observations"],
        event_links=fr["event_links"],
        adjudications=fr["adjudications"],
        events=fr["events"],
        followups=fr["followups"],
        exit_evidence=fr["exit_evidence"],
        coverage=fr["coverage"],
        ratings=fr["ratings"],
        ncen_filings=fr["ncen_filings"],
        family_contexts=fr["family_contexts"],
        family_evidence=fr["family_evidence"],
        proposal_evidence=fr["proposal_evidence"],
        exchange_relations=fr["exchange_relations"],
    )


def rebind_receipt(bundle: c.CreditBundle, frames: dict | None = None, **receipt_changes) -> c.CreditBundle:  # type: ignore[type-arg]
    """Reassemble with the receipt re-bound to the (new) approved rating packages.

    ``receipt_changes`` override receipt fields afterwards; coverage cells that cite the
    receipt are re-pointed to the new receipt digest.
    """
    fr = {**bundle.frames, **(frames or {})}
    old = bundle.manifest.receipt()
    assert old is not None
    values = {name: getattr(old, name) for name, _ in old.SPEC}
    package_digest = c.rating_package_digest(fr["source_packages"])  # type: ignore[arg-type]
    values["rating_input_digest"] = (
        c.rating_input_manifest_digest(bundle.manifest["rating_declarations"], fr["source_packages"])
        if package_digest else None
    )
    values["rating_package_digest"] = package_digest
    values.update(receipt_changes)
    receipt = c.ValidationReceipt(**values)
    coverage = tuple(
        replace_row(x, validation_receipt_digest=receipt.digest()) if x.validation_receipt_digest else x
        for x in fr["coverage"]
    )
    return reassemble(bundle, frames={**(frames or {}), "coverage": coverage}, validation_receipt=receipt)


def _agency_inputs(bundle: c.CreditBundle) -> tuple[c.SourcePackage, c.CreditObservation]:
    package = next(p for p in bundle.frames["source_packages"] if p.source_family == "agency_rocr_xbrl")  # type: ignore[attr-defined]
    action = next(o for o in bundle.frames["observations"] if o.observation_kind == "agency_action")  # type: ignore[attr-defined]
    return package, action  # type: ignore[return-value]


def _all_missing(ratings) -> tuple:  # type: ignore[no-untyped-def,type-arg]
    """Every row ``missing`` with null bucket/provenance (default overlays kept)."""
    return tuple(
        replace_row(r, state="missing", bucket=None, action_date=None, public_known_at=None,
                    agency_source_ids=(), binding_link_ids=(), coverage_frontier=None, action_input_digest=None)
        for r in ratings
    )


def _swap_rows(bundle: c.CreditBundle, frame: str, changes: dict) -> tuple:  # type: ignore[type-arg]
    """``frame`` rows with ``changes[row] -> replacement`` applied (identity match)."""
    return tuple(changes.get(id(r), r) for r in bundle.frames[frame])


def _without_agency(bundle: c.CreditBundle) -> dict:  # type: ignore[type-arg]
    package, action = _agency_inputs(bundle)
    return {
        "source_packages": tuple(p for p in bundle.frames["source_packages"] if p is not package),
        "observations": tuple(o for o in bundle.frames["observations"] if o is not action),
        "ratings": _all_missing(bundle.frames["ratings"]),
    }


def _frontier_may(bundle: c.CreditBundle, *, row_frontier: dt.date | None) -> dict:  # type: ignore[type-arg]
    """Agency package verified only through 2026-05-31 (optionally re-stating row frontiers)."""
    package, _action = _agency_inputs(bundle)
    short = replace_row(
        package, public_coverage_end=dt.date(2026, 5, 31), effective_coverage_end=dt.date(2026, 5, 31),
        source_coverage_end=dt.date(2026, 5, 31),
    )
    frames = {"source_packages": _swap_rows(bundle, "source_packages", {id(package): short})}
    if row_frontier is not None:
        frames["ratings"] = tuple(
            replace_row(r, coverage_frontier=row_frontier) if r.agency_source_ids else r  # type: ignore[attr-defined]
            for r in bundle.frames["ratings"]
        )
    return frames


def _cusip_a_april_relies_on_c(bundle: c.CreditBundle) -> dict:  # type: ignore[type-arg]
    """CUSIP A's April public-PIT row relies on the CUSIP C agency action (another obligation)."""
    _package, action = _agency_inputs(bundle)
    row = next(r for r in bundle.frames["ratings"]  # type: ignore[attr-defined]
               if r.cusip_id == CUSIP_A and r.month == GRID_MONTHS[0] and r.view_kind == "public_pit")
    relied = replace_row(
        row, state="observed", bucket="BB", action_date=action.agency_action_date,
        public_known_at=action.public_available_at, agency_source_ids=(action.observation_id,),
        coverage_frontier=dt.date(2026, 6, 30),
        action_input_digest=c.digest_of({"action": str(action.observation_id)}),
    )
    return {"ratings": _swap_rows(bundle, "ratings", {id(row): relied})}


def _linked_to_cusip_a(bundle: c.CreditBundle, *, known_at: dt.datetime = dt.datetime(2026, 4, 20, 9, 30, tzinfo=UTC),
                       valid_to: dt.date | None = None, scope: str = "issue", view: str = "public_pit",
                       persist_binding: bool = True, digest_links: bool = True,
                       extra_known_at: dt.datetime | None = None) -> dict:  # type: ignore[type-arg]
    """CUSIP A's April ``view`` row relies on the CUSIP C action through a persisted binding link.

    The bucket and ``action_input_digest`` are derived exactly as W2b does; the knobs create
    §9 item 9 negatives (link known late, expired link, wrong scope, unpersisted binding).
    """
    package, action = _agency_inputs(bundle)
    link_pkg = next(p for p in bundle.frames["source_packages"] if p.source_family == "link_batch")  # type: ignore[attr-defined]
    link = _link(link_pkg, action, CUSIP_A, "SYNTHETIC-OBLIGOR-A", scope, known_at)  # type: ignore[arg-type]
    if valid_to is not None:
        values = {n: getattr(link, n) for n, _ in link.SPEC if n != "link_id"}
        link = c.EventLink.create(**{**values, "valid_to": valid_to})
    selected_links = [link]
    if extra_known_at is not None:
        values = {n: getattr(link, n) for n, _ in link.SPEC if n != "link_id"}
        selected_links.append(c.EventLink.create(**{
            **values,
            "valid_from": dt.date(2021, 1, 1),
            "link_known_at": extra_known_at,
        }))
    row = next(r for r in bundle.frames["ratings"]  # type: ignore[attr-defined]
               if r.cusip_id == CUSIP_A and r.month == GRID_MONTHS[0] and r.view_kind == view)
    known_at = max(action.public_available_at, *(item.link_known_at for item in selected_links))
    if view == "public_pit" and known_at >= c.next_month_boundary_utc(row.month):
        # Deliberately forged earlier scalar for the negative: the row constructor enforces its
        # scalar boundary, while publication validation must independently reject the late link.
        known_at = action.public_available_at
    relied = replace_row(
        row, state="observed", bucket="BB", action_date=action.agency_action_date,
        public_known_at=known_at, agency_source_ids=(action.observation_id,),
        binding_link_ids=c.sorted_uuids(item.link_id for item in selected_links) if persist_binding else (),
        coverage_frontier=dt.date(2026, 6, 30),
        action_input_digest=c.rating_action_input_digest(
            view, [action], [package], selected_links if digest_links else []),
    )
    return {"ratings": _swap_rows(bundle, "ratings", {id(row): relied}),
            "event_links": (*bundle.frames["event_links"], *selected_links)}


def _tampered_carried_row(bundle: c.CreditBundle, **changes: object) -> dict:  # type: ignore[type-arg]
    """A carried ``public_pit`` CUSIP C row with ``changes`` (enclosing identities rebuilt)."""
    row = next(r for r in bundle.frames["ratings"]  # type: ignore[attr-defined]
               if r.state == "carried_verified" and r.view_kind == "public_pit")
    return {"ratings": _swap_rows(bundle, "ratings", {id(row): replace_row(row, **changes)})}


def _late_publication(bundle: c.CreditBundle) -> dict:  # type: ignore[type-arg]
    """The relied agency action became public on 2026-09-24; the April rows keep April known-time."""
    _package, action = _agency_inputs(bundle)
    late = replace_row(action, public_available_at=dt.datetime(2026, 9, 24, 12, 0, tzinfo=UTC))
    return {"observations": _swap_rows(bundle, "observations", {id(action): late})}


def _action_date_shifted(bundle: c.CreditBundle) -> dict:  # type: ignore[type-arg]
    row = next(r for r in bundle.frames["ratings"] if r.state == "observed")  # type: ignore[attr-defined]
    shifted = replace_row(row, action_date=dt.date(2026, 4, 9))
    return {"ratings": _swap_rows(bundle, "ratings", {id(row): shifted})}


def _out_of_scope_rating(bundle: c.CreditBundle) -> dict:  # type: ignore[type-arg]
    package, action = _agency_inputs(bundle)
    changed = replace_row(action, agency_rating_type="short_term", agency_scale="national")
    return {
        "observations": _swap_rows(bundle, "observations", {id(action): changed}),
        "ratings": tuple(
            replace_row(
                row,
                action_input_digest=c.rating_action_input_digest(row.view_kind, [changed], [package], []),
            ) if action.observation_id in row.agency_source_ids else row
            for row in bundle.frames["ratings"]
        ),
    }


def _conflict_with_uncleared_declaration(bundle: c.CreditBundle) -> c.CreditBundle:
    package, action = _agency_inputs(bundle)
    changes = {
        name: getattr(action, name)
        for name, _kind in action.SPEC
        if name not in {"observation_id", "package_id", "member_name", "row_locator", "observation_kind", "semantic_key"}
    }
    conflict = _observation(
        package,
        "action-conflict",
        "agency_action",
        **{**changes, "agency_rating_symbol": "B+"},
    )
    declarations = pr.rating_declarations_record(RATING_SCOPES, UNCLEARED_RATING_SOURCES)
    panel_grid = tuple(key for key in bundle.panel_grid if key[0] == CUSIP_C)
    source = reassemble(
        bundle,
        frames={
            "observations": (*bundle.frames["observations"], conflict),
            "ratings": tuple(row for row in bundle.frames["ratings"] if row.cusip_id == CUSIP_C),
        },
        panel_grid=panel_grid,
        rating_declarations=declarations,
    )
    source = rebind_receipt(source)
    frames = source.frames
    resolved = pr.build_full_grid_ratings(
        source.panel_grid,
        knowledge_cutoff=source.manifest["knowledge_cutoff"],
        packages=frames["source_packages"],
        observations=frames["observations"],
        links=frames["event_links"],
        episodes=frames["events"],
        rating_scopes=RATING_SCOPES,
        uncleared_sources=UNCLEARED_RATING_SOURCES,
        strict=False,
    )
    return reassemble(source, frames={"ratings": resolved.rows})


def rating_regressions() -> dict[str, tuple[c.CreditBundle, str | None]]:
    """Rating-input qualification (finding 1) and public-PIT binding (finding 4) scenarios.

    Maps a scenario name to ``(bundle, expected reason)``; ``None`` means the bundle is
    valid and promotable. Shared by the in-memory and database suites.
    """
    q = build_bundle()
    empty = build_bundle(with_events=False)
    unsupported, invalid = "qualified_state_unsupported", "rating_row_invalid"
    uncleared = pr.rating_declarations_record(RATING_SCOPES, UNCLEARED_RATING_SOURCES)
    return {
        # Finding 1: all-missing grid with zero agency input (receipt kept / receipt re-derived).
        "zero_agency_all_missing": (reassemble(q, frames=_without_agency(q)), unsupported),
        "zero_agency_all_missing_no_events": (rebind_receipt(empty, _without_agency(empty)), unsupported),
        "receipt_not_bound_to_packages": (
            rebind_receipt(q, rating_package_digest=_digest("other-rating-packages")), unsupported),
        "receipt_not_bound_to_rating_input": (
            rebind_receipt(q, rating_input_digest=_digest("other-rating-input")), unsupported),
        "receipt_without_rating_qualification": (
            rebind_receipt(q, rating_input_digest=None, rating_package_digest=None), unsupported),
        "grid_month_beyond_coverage_frontier": (
            rebind_receipt(q, {**_frontier_may(q, row_frontier=None), "ratings": _all_missing(q.frames["ratings"])}),
            unsupported),
        "all_missing_under_qualified_input": (
            reassemble(q, frames={"ratings": _all_missing(q.frames["ratings"])}), None),
        "rating_input_declarations_mismatch": (
            reassemble(q, rating_declarations=uncleared, rating_input_digest=q.manifest["rating_input_digest"]),
            "rating_input_digest_mismatch"),
        "rating_declaration_coverage_state_mismatch": (
            reassemble(q, rating_declarations=uncleared), "rating_scope_invalid"),
        "rating_action_outside_declared_scope": (
            reassemble(q, frames=_out_of_scope_rating(q)), "rating_scope_invalid"),
        "conflict_missing_with_uncleared_declaration": (
            _conflict_with_uncleared_declaration(q), None),
        # Finding 4: rows bound to the relied action's obligation, time and coverage.
        "late_publication": (reassemble(q, frames=_late_publication(q)), invalid),
        "wrong_obligation": (reassemble(q, frames=_cusip_a_april_relies_on_c(q)), invalid),
        "linked_obligation": (reassemble(q, frames=_linked_to_cusip_a(q)), None),
        "multiple_binding_links_latest_known": (reassemble(q, frames=_linked_to_cusip_a(
            q, view="effective_audit", extra_known_at=dt.datetime(2026, 9, 21, 9, 30, tzinfo=UTC))), None),
        # §9 item 9 (W0 amendment 1): derived bucket/digest and persisted binding links.
        "bucket_tamper": (reassemble(q, frames=_tampered_carried_row(q, bucket="AAA")), invalid),
        "action_input_digest_tamper": (reassemble(q, frames=_tampered_carried_row(
            q, action_input_digest=_digest("unrelated-valid-digest"))), invalid),
        "binding_link_known_after_boundary": (reassemble(q, frames=_linked_to_cusip_a(
            q, known_at=dt.datetime(2026, 9, 21, 9, 30, tzinfo=UTC))), invalid),
        "binding_link_known_after_boundary_audit_view": (reassemble(q, frames=_linked_to_cusip_a(
            q, known_at=dt.datetime(2026, 9, 21, 9, 30, tzinfo=UTC), view="effective_audit")), None),
        "binding_link_known_after_cutoff_reconstruction": (reassemble(
            q,
            frames=_linked_to_cusip_a(
                q, known_at=KNOWLEDGE_CUTOFF + dt.timedelta(hours=1), view="effective_audit"),
            knowledge_mode="historical_reconstruction",
        ), invalid),
        "binding_link_expired_mid_month": (reassemble(q, frames=_linked_to_cusip_a(
            q, valid_to=dt.date(2026, 4, 15))), invalid),
        "binding_link_not_issue_scope": (reassemble(q, frames=_linked_to_cusip_a(
            q, scope="issuer_affected_obligation")), invalid),
        "binding_link_not_persisted": (reassemble(q, frames=_linked_to_cusip_a(
            q, persist_binding=False, digest_links=False)), invalid),
        "binding_link_not_in_digest": (reassemble(q, frames=_linked_to_cusip_a(q, digest_links=False)), invalid),
        "action_date_not_relied_action": (reassemble(q, frames=_action_date_shifted(q)), invalid),
        "carried_beyond_frontier": (
            rebind_receipt(q, _frontier_may(q, row_frontier=dt.date(2026, 5, 31))), invalid),
        "frontier_not_from_relied_package": (rebind_receipt(q, _frontier_may(q, row_frontier=None)), invalid),
        "valid_public_pit_binding": (q, None),
    }


def _variant(observation: c.CreditObservation, **changes: object) -> c.CreditObservation:
    """New observation (re-derived id) from ``observation`` with ``changes``."""
    values = {n: getattr(observation, n) for n, _ in observation.SPEC if n != "observation_id"}
    values.update(changes)
    return c.CreditObservation.create(**values)


def _readjudicate(adjudication: c.Adjudication, **changes: object) -> c.Adjudication:
    values = {n: getattr(adjudication, n) for n, _ in adjudication.SPEC if n != "adjudication_id"}
    values.update(changes)
    return c.Adjudication.create(**values)


def frames_with(bundle: c.CreditBundle, extra: tuple = (), drop: tuple = ()) -> dict:  # type: ignore[type-arg]
    """``bundle`` frames with ``extra`` rows added (replacing equal keys) and ``drop`` removed."""
    fr = {name: [r for r in rows if not any(r is d for d in drop)] for name, rows in bundle.frames.items()}
    for row in extra:
        fr[row.FRAME] = [r for r in fr[row.FRAME] if r.key() != row.key()] + [row]
    return fr


def _derive_event(bundle: c.CreditBundle, event: c.DefaultEpisode, extra: tuple = (), **changes: object) -> c.DefaultEpisode:  # type: ignore[type-arg]
    """``event`` with ``changes``; knowledge times, timing class and digests re-derived honestly
    by the W0 builder API over the bundle frames plus ``extra`` rows."""
    values = {n: getattr(event, n) for n, _ in event.SPEC}
    values.update(changes)
    edges = ("evidence_observation_ids", "onset_lower_evidence_ids", "link_ids", "adjudication_ids",
             "proposal_evidence_ids", "exchange_relation_ids")
    derived = pub.derive_event_fields(
        frames_with(bundle, extra), knowledge_cutoff=bundle.manifest["knowledge_cutoff"],
        knowledge_mode=bundle.manifest["knowledge_mode"], **{name: values[name] for name in edges},
    )
    values.update(
        derived,
        timing_class=c.derive_timing_class(values["onset_lower_exclusive"], values["onset_upper_inclusive"]),  # type: ignore[arg-type]
    )
    return c.DefaultEpisode(**values)


def _replaced(bundle: c.CreditBundle, frame: str, old: object, new: object) -> tuple:  # type: ignore[type-arg]
    return tuple(new if r is old else r for r in bundle.frames[frame])


def _pieces(bundle: c.CreditBundle) -> dict:  # type: ignore[type-arg]
    """Named rows of a :func:`build_bundle` result."""
    fr = bundle.frames
    obs = fr["observations"]
    return {
        "ev_a": next(e for e in fr["events"] if e.episode_id == EPISODE_A),  # type: ignore[attr-defined]
        "ev_b": next(e for e in fr["events"] if e.episode_id == EPISODE_B),  # type: ignore[attr-defined]
        "cand_a": next(a for a in fr["adjudications"] if a.subject_id == EPISODE_A and a.status == "candidate"),  # type: ignore[attr-defined]
        "acc_a": next(a for a in fr["adjudications"] if a.subject_id == EPISODE_A and a.status == "accepted_event"),  # type: ignore[attr-defined]
        "acc_b": next(a for a in fr["adjudications"] if a.subject_id == EPISODE_B),  # type: ignore[attr-defined]
        "edgar_a": next(o for o in obs if o.observation_kind == "edgar_passage"),  # type: ignore[attr-defined]
        "issuer_c": next(o for o in obs if o.observation_kind == "issuer_document_passage"),  # type: ignore[attr-defined]
        "nport_b": sorted((o for o in obs if o.cusip9 == CUSIP_B), key=lambda o: o.row_locator),  # type: ignore[attr-defined]
        "nport_c": next(o for o in obs if o.observation_kind == "nport_holding" and o.cusip9 == CUSIP_C),  # type: ignore[attr-defined]
        "link_a": next(x for x in fr["event_links"] if x.cusip9 == CUSIP_A and x.status == "admitted"),  # type: ignore[attr-defined]
        "links_b": [x for x in fr["event_links"] if x.cusip9 == CUSIP_B],  # type: ignore[attr-defined]
        "fu_c": next(f for f in fr["followups"] if f.cusip9 == CUSIP_C),  # type: ignore[attr-defined]
        "exit_c": next(x for x in fr["exit_evidence"] if x.cusip9 == CUSIP_C),  # type: ignore[attr-defined]
        "edgar_pkg": next(p for p in fr["source_packages"] if p.source_family == "sec_edgar_document"),  # type: ignore[attr-defined]
        "link_pkg": next(p for p in fr["source_packages"] if p.source_family == "link_batch"),  # type: ignore[attr-defined]
        "adj_pkg": next(p for p in fr["source_packages"] if p.external_id == "SYNTHETIC-ADJ-1"),  # type: ignore[attr-defined]
    }


RECONSTRUCTION_CUTOFF = dt.datetime(2026, 9, 23, 0, 0, tzinfo=UTC)


def _late_document(edgar_pkg: c.SourcePackage) -> c.CreditObservation:
    """A public EDGAR passage first public on 2026-09-24 12:00Z (after the Sep-23 reconstruction K)."""
    accept = "20260924080000"
    return _observation(
        edgar_pkg, "item-8.01-p1", "edgar_passage",
        accession_number="9999999901-26-000012", issuer_cik="9999999901",
        effective_date=dt.date(2026, 5, 15), date_precision="day",
        acceptance_raw=accept, acceptance_at=c.edgar_acceptance_to_utc(accept),
        public_available_at=c.edgar_acceptance_to_utc(accept), public_time_basis="edgar_acceptance_datetime",
        first_seen_at=dt.datetime(2026, 9, 24, 13, 0, tzinfo=UTC),
        document_quote="SYNTHETIC: later disclosure restating the 2026-05-15 payment default.",
        document_location="ex99-2.htm#p1", document_sha256=_sha("late-document"),
    )


def _dependency_closure_scenarios(q: c.CreditBundle) -> dict:  # type: ignore[type-arg]
    """Finding 5: event closure over the accepting adjudication; row known-times; resolution."""
    x = _pieces(q)
    ev_a, cand_a, acc_a = x["ev_a"], x["cand_a"], x["acc_a"]
    late = _late_document(x["edgar_pkg"])
    late_link = _link(x["link_pkg"], late, CUSIP_A, "SYNTHETIC-OBLIGOR-A", "issuer_affected_obligation",
                      dt.datetime(2026, 9, 22, 10, 0, tzinfo=UTC))
    acc_late = _readjudicate(acc_a, evidence_observation_ids=c.sorted_uuids(
        [x["edgar_a"].observation_id, late.observation_id]))
    adjs = (cand_a.adjudication_id, acc_late.adjudication_id)
    base = {"observations": (*q.frames["observations"], late),
            "adjudications": _replaced(q, "adjudications", acc_a, acc_late)}
    reconstruct = {"knowledge_cutoff": RECONSTRUCTION_CUTOFF, "knowledge_mode": "historical_reconstruction"}
    # Reviewer's case: accepting adjudication cites the Sep-24 document, the event omits it.
    unclosed = _derive_event(q, ev_a, (late, acc_late), adjudication_ids=adjs)
    closed = _derive_event(q, ev_a, (late, late_link, acc_late), adjudication_ids=adjs, link_ids=(
        x["link_a"].link_id, late_link.link_id), evidence_observation_ids=(x["edgar_a"].observation_id, late.observation_id))

    def event_a(event: c.DefaultEpisode, frames: dict | None = None) -> dict:  # type: ignore[type-arg]
        return {**(frames or {}), "events": _replaced(q, "events", ev_a, event)}

    fu_c, exit_c, issuer_c = x["fu_c"], x["exit_c"], x["issuer_c"]
    resolved = {"resolution_date": dt.date(2026, 6, 30), "resolution_refs": (issuer_c.observation_id,)}
    return {
        "reconstruction_valid": (reassemble(q, **reconstruct), None),
        "adjudication_cites_later_document": (
            reassemble(q, frames=event_a(unclosed, base), **reconstruct), "event_evidence_not_closed"),
        "closed_event_known_after_cutoff": (
            reassemble(q, frames=event_a(closed, {**base, "event_links": (*q.frames["event_links"], late_link)}),
                       **reconstruct), "event_evidence_invalid"),
        "followup_known_before_its_evidence": (reassemble(q, frames={"followups": _replaced(
            q, "followups", fu_c, replace_row(fu_c, known_at=dt.datetime(2026, 1, 15, tzinfo=UTC)))}),
            "followup_invalid"),
        "followup_adjudication_link_after_known_at": (reassemble(q, frames={"followups": _replaced(
            q, "followups", fu_c, replace_row(
                fu_c, security_id=x["ev_b"].security_id, cusip9=x["ev_b"].cusip9,
                adjudication_ids=(x["acc_b"].adjudication_id,)))}),
            "followup_invalid"),
        "followup_known_after_support": (reassemble(q, frames={"followups": _replaced(
            q, "followups", fu_c, replace_row(fu_c, known_at=dt.datetime(2026, 9, 22, 12, 0, tzinfo=UTC)))}), None),
        "resolution_public_after_cutoff": (reassemble(q, frames=event_a(replace_row(
            ev_a, resolution_date=dt.date(2026, 6, 30), resolution_refs=(late.observation_id,),
            resolution_known_at=late.public_available_at), {"observations": base["observations"]}),
            **reconstruct), "resolution_knowledge_invalid"),
        "resolution_known_at_understated": (reassemble(q, frames=event_a(replace_row(
            ev_a, **resolved, resolution_known_at=dt.datetime(2026, 6, 30, tzinfo=UTC)))),
            "resolution_knowledge_invalid"),
        "resolution_own_knowledge_time": (reassemble(q, frames=event_a(replace_row(
            ev_a, **resolved, resolution_known_at=issuer_c.public_available_at))), None),
        "repayment_evidence_after_known_at": (reassemble(q, frames={"exit_evidence": _replaced(
            q, "exit_evidence", exit_c, replace_row(
                exit_c, proven_repayment_date=dt.date(2026, 7, 15), evidence_observation_ids=(issuer_c.observation_id,),
                known_at=dt.datetime(2026, 7, 1, tzinfo=UTC)))}), "exit_evidence_invalid"),
        "repayment_evidence_public_by_known_at": (reassemble(q, frames={"exit_evidence": _replaced(
            q, "exit_evidence", exit_c, replace_row(
                exit_c, proven_repayment_date=dt.date(2026, 7, 15), evidence_observation_ids=(issuer_c.observation_id,),
                known_at=dt.datetime(2026, 9, 22, 12, 0, tzinfo=UTC)))}), None),
    }


SURVEILLANCE_WINDOW = {"surveillance_start_exclusive": dt.date(2026, 3, 31),
                       "surveillance_end_inclusive": dt.date(2026, 6, 30)}


def _surveillance_scenarios(q: c.CreditBundle) -> dict:  # type: ignore[type-arg]
    """Finding 5: a surveillance basis needs a qualified receipt whose scope covers issue and interval."""
    receipt_time = dt.datetime(2026, 9, 24, 12, 0, tzinfo=UTC)

    def surveilled(bundle: c.CreditBundle, **changes: object) -> dict:  # type: ignore[type-arg]
        fu_c = _pieces(bundle)["fu_c"]
        values = {"completeness_basis": "surveillance_receipt", "known_at": receipt_time,
                  "evidence_observation_ids": (_pieces(bundle)["nport_c"].observation_id,), **changes}
        return {"followups": _replaced(bundle, "followups", fu_c, replace_row(fu_c, **values))}

    outside = synthetic_cusip(9)
    p = build_bundle(quality_state="partial")
    return {
        "surveillance_covering_scope": (rebind_receipt(q, surveilled(q), **SURVEILLANCE_WINDOW), None),
        "surveillance_receipt_without_scope": (rebind_receipt(q, surveilled(q)), "followup_invalid"),
        "surveillance_scope_not_covering_interval": (rebind_receipt(
            q, surveilled(q), surveillance_start_exclusive=dt.date(2026, 3, 31),
            surveillance_end_inclusive=dt.date(2026, 5, 31)), "followup_invalid"),
        "surveillance_issue_outside_grid": (rebind_receipt(
            q, surveilled(q, cusip9=outside, security_id=security_id(outside)), **SURVEILLANCE_WINDOW),
            "followup_invalid"),
        "surveillance_receipt_after_known_at": (rebind_receipt(
            q, surveilled(q, known_at=dt.datetime(2026, 9, 1, tzinfo=UTC)), **SURVEILLANCE_WINDOW),
            "followup_invalid"),
        "surveillance_receipt_not_qualified": (rebind_receipt(
            p, surveilled(p), verdict="partial", **SURVEILLANCE_WINDOW), "followup_invalid"),
    }


def _onset_lower_scenarios(q: c.CreditBundle) -> dict:  # type: ignore[type-arg]
    """Finding 7: a lower onset bound needs prior-N / dating provenance cited at admission."""
    x = _pieces(q)
    ev_b, acc_b = x["ev_b"], x["acc_b"]
    b1, b2 = x["nport_b"]
    link_b1, link_b2 = x["links_b"]

    prior_date = dt.date(2026, 1, 31)
    old_proposal = next(iter(q.frames["proposal_evidence"]))

    def prior_n(accept: str = "20260220101500", cusip: str = CUSIP_B) -> tuple[c.CreditObservation, ...]:
        """Credible prior N: N votes of two independent registrants' series on 2026-01-31."""
        return tuple(
            _variant(
                b, row_locator=f"row-00000{4 + i}", semantic_key=f"synthetic:row-00000{4 + i}",
                holding_id=f"SYN-H-row-00000{4 + i}", accession_number=f"9999999901-26-00000{4 + i}",
                cusip_raw=cusip, cusip9=cusip, security_id=security_id(cusip), report_date=prior_date,
                nport_is_default="N", acceptance_raw=accept, acceptance_at=c.edgar_acceptance_to_utc(accept),
                public_available_at=c.edgar_acceptance_to_utc(accept),
            )
            for i, b in enumerate((b1, b2))
        )

    def with_prior(ns: tuple[c.CreditObservation, ...], *, lower: dt.date = prior_date,
                   adj_cites_lower: bool = True) -> c.CreditBundle:
        n_links = tuple(_link(x["link_pkg"], n, CUSIP_B, "SYNTHETIC-OBLIGOR-B", "issue",
                              dt.datetime(2026, 9, 21, 10, 0, tzinfo=UTC)) for n in ns)
        frames = frames_with(q, (*ns, *n_links))
        k, mode = q.manifest["knowledge_cutoff"], q.manifest["knowledge_mode"]
        contexts: tuple = ()  # type: ignore[type-arg]
        members: tuple = ()  # type: ignore[type-arg]
        proposal = old_proposal
        if all(n.public_available_at <= k for n in ns):
            contexts, members = pub.build_family_frames(frames, [prior_date], knowledge_cutoff=k, knowledge_mode=mode)
            frames = frames_with(q, (*ns, *n_links, *contexts, *members))
            try:
                proposal = pub.derive_state_proposal(frames, cusip9=CUSIP_B, onset_upper_inclusive=REPORT_DATE,
                                                     onset_lower_exclusive=prior_date, knowledge_cutoff=k,
                                                     knowledge_mode=mode)
            except c.ContractError:
                proposal = old_proposal  # N votes of another obligation: no credible prior N for B
        obs_ids = (b1.observation_id, b2.observation_id, *(n.observation_id for n in ns))
        link_ids = (link_b1.link_id, link_b2.link_id, *(nl.link_id for nl in n_links))
        acc = _readjudicate(
            acc_b, proposal_evidence_ids=(proposal.proposal_evidence_id,),
            evidence_observation_ids=c.sorted_uuids(obs_ids if adj_cites_lower else obs_ids[:2]),
            link_ids=c.sorted_uuids(link_ids if adj_cites_lower else link_ids[:2]))
        extra = (*ns, *n_links, *contexts, *members, proposal, acc)
        event = _derive_event(q, ev_b, extra, evidence_observation_ids=obs_ids, link_ids=link_ids,
                              adjudication_ids=(acc.adjudication_id,), onset_lower_exclusive=lower,
                              onset_lower_evidence_ids=tuple(n.observation_id for n in ns),
                              proposal_evidence_ids=(proposal.proposal_evidence_id,))
        return reassemble(q, frames={
            "observations": (*q.frames["observations"], *ns),
            "event_links": (*q.frames["event_links"], *n_links),
            "adjudications": _replaced(q, "adjudications", acc_b, acc),
            "family_contexts": (*q.frames["family_contexts"], *contexts),
            "family_evidence": (*q.frames["family_evidence"], *members),
            "proposal_evidence": (proposal,),
            "events": _replaced(q, "events", ev_b, event),
        })

    unsupported = "event_onset_lower_unsupported"
    omitted = _derive_event(q, ev_b, evidence_observation_ids=(b1.observation_id,), link_ids=(link_b1.link_id,))
    invented = _derive_event(q, ev_b, onset_lower_exclusive=dt.date(2026, 3, 30),
                             onset_lower_evidence_ids=(b1.observation_id,))
    return {
        "valid_dated_and_prevalent_null_lower": (q, None),
        "prior_n_lower_bound": (with_prior(prior_n()), None),
        "invented_lower_cites_y_state": (
            reassemble(q, frames={"events": _replaced(q, "events", ev_b, invented)}), unsupported),
        "prior_n_other_obligation": (with_prior(prior_n(cusip=CUSIP_C)), unsupported),
        "prior_n_other_date": (with_prior(prior_n(), lower=dt.date(2026, 2, 27)), unsupported),
        "prior_n_public_after_cutoff": (with_prior(prior_n(accept="20260926080000")), "event_evidence_invalid"),
        "prior_n_not_cited_by_adjudication": (with_prior(prior_n(), adj_cites_lower=False), unsupported),
        "consensus_observation_omitted": (
            reassemble(q, frames={"events": _replaced(q, "events", ev_b, omitted)}), "event_evidence_not_closed"),
    }


def evidence_closure_regressions() -> dict[str, tuple[c.CreditBundle, str | None]]:
    """Dependency closure (finding 5) and onset lower-bound provenance (finding 7) scenarios.

    Maps a scenario name to ``(bundle, expected reason)``; ``None`` means valid and
    promotable. Shared by the in-memory and database suites.
    """
    q = build_bundle()
    return {**_dependency_closure_scenarios(q), **_surveillance_scenarios(q), **_onset_lower_scenarios(q)}


#: Revision time between the reconstruction cutoff (Sep 23) and the default cutoff (Sep 25).
REVISION_AT = dt.datetime(2026, 9, 24, 9, 0, tzinfo=UTC)


def _link_revision(link: c.EventLink, *, status: str = "admitted",
                   rationale: str = "SYNTHETIC link revision") -> c.EventLink:
    """A revision of ``link`` known at :data:`REVISION_AT`."""
    values = {n: getattr(link, n) for n, _ in link.SPEC if n != "link_id"}
    refs = values["identity_evidence_refs"] if status == "admitted" else ()
    values.update(status=status, rationale=rationale, link_known_at=REVISION_AT, supersedes_link_id=link.link_id,
                  identity_evidence_refs=refs, identity_evidence_digest=c.digest_of(list(refs)))
    return c.EventLink.create(**values)


def _obs_revision(o: c.CreditObservation, locator: str, kind: str) -> c.CreditObservation:
    """A ``kind`` revision of ``o`` public at :data:`REVISION_AT` (first seen one hour later)."""
    changes: dict[str, object] = {
        "row_locator": locator, "semantic_key": f"synthetic:{locator}", "revision_kind": kind,
        "supersedes_observation_id": o.observation_id, "public_available_at": REVISION_AT,
        "first_seen_at": REVISION_AT + dt.timedelta(hours=1),
    }
    if o.acceptance_raw is not None:
        accept = "20260924050000"  # America/New_York -> 2026-09-24T09:00Z
        changes.update(acceptance_raw=accept, acceptance_at=c.edgar_acceptance_to_utc(accept))
    if o.holding_id is not None:
        changes["holding_id"] = f"SYN-H-{locator}"
    if o.agency_file_creation_at is not None:
        changes["agency_file_creation_at"] = REVISION_AT
    return _variant(o, **changes)


def revision_regressions() -> dict[str, tuple[c.CreditBundle, str | None]]:
    """Finding 6: link/observation revision chains resolved as of K.

    Maps a scenario name to ``(bundle, expected reason)``; ``None`` means valid and
    promotable. Shared by the in-memory and database suites.
    """
    q = build_bundle()
    x = _pieces(q)
    ev_a, cand_a, acc_a, link_a, edgar_a = x["ev_a"], x["cand_a"], x["acc_a"], x["link_a"], x["edgar_a"]
    reconstruct = {"knowledge_cutoff": RECONSTRUCTION_CUTOFF, "knowledge_mode": "historical_reconstruction"}
    links, observations = q.frames["event_links"], q.frames["observations"]
    rejected = {"event_links": (*links, _link_revision(link_a, status="rejected"))}
    # The event and its accepting adjudication move to the superseding admitted revision.
    renewed = _link_revision(link_a)
    cand_new = _readjudicate(cand_a, link_ids=(renewed.link_id,), adjudicated_at=REVISION_AT)
    acc_new = _readjudicate(
        acc_a, link_ids=(renewed.link_id,), supersedes_adjudication_id=cand_new.adjudication_id,
        adjudicated_at=REVISION_AT + dt.timedelta(hours=1),
    )
    ev_new = _derive_event(q, ev_a, (renewed, cand_new, acc_new), link_ids=(renewed.link_id,),
                           adjudication_ids=(cand_new.adjudication_id, acc_new.adjudication_id))
    moved = {"event_links": (*links, renewed),
             "adjudications": tuple(cand_new if a is cand_a else acc_new if a is acc_a else a
                                      for a in q.frames["adjudications"]),
             "events": _replaced(q, "events", ev_a, ev_new)}
    forked = {"event_links": (*links, renewed, _link_revision(link_a, rationale="SYNTHETIC competing revision"))}
    retracted = {"observations": (*observations, _obs_revision(edgar_a, "item-2.04-p3-retraction", "retraction"))}
    nport_fix = {"observations": (*observations, _obs_revision(x["nport_c"], "row-000003-corr", "correction"))}
    _package, action = _agency_inputs(q)
    action_fix = {"observations": (*observations, _obs_revision(action, "action-1-corr", "correction"))}
    stale = "evidence_superseded"
    return {
        # Reviewer's case: a Sep-24 rejected revision supersedes the link the event uses (K = Sep 25).
        "link_rejected_before_cutoff": (reassemble(q, frames=rejected), stale),
        "link_revision_after_cutoff": (reassemble(q, frames=rejected, **reconstruct), None),
        "relies_on_superseding_link_revision": (reassemble(q, frames=moved), None),
        "link_revision_fork": (reassemble(q, frames=forked), "revision_chain_fork"),
        "observation_retracted_before_cutoff": (reassemble(q, frames=retracted), stale),
        "observation_retraction_after_cutoff": (reassemble(q, frames=retracted, **reconstruct), None),
        # The corrected vote changes the FE-1 universe: the builder rebuilds the closure.
        "followup_evidence_corrected": (reassemble(q, frames=nport_fix, rebind=True), stale),
        "rating_action_corrected": (reassemble(q, frames=action_fix), stale),
    }


def observation_cycle_bundle() -> c.CreditBundle:
    """Two EDGAR corrections superseding each other (a revision cycle)."""
    edgar_a = _pieces(q := build_bundle())["edgar_a"]
    first = c.CreditObservation.derive_id(edgar_a.package_id, "primary", "cycle-1")
    second = _obs_revision(edgar_a, "cycle-2", "correction")
    second = _variant(second, supersedes_observation_id=first)
    first_row = _variant(_obs_revision(edgar_a, "cycle-1", "correction"), supersedes_observation_id=second.observation_id)
    assert first_row.observation_id == first
    return reassemble(q, frames={"observations": (*q.frames["observations"], first_row, second)})


FIXTURES = {
    "bundle_qualified.json": lambda: build_bundle(),
    "bundle_partial.json": lambda: build_bundle(quality_state="partial", with_receipt=False),
    "bundle_exchange.json": lambda: exchange_bundle(),
}
#: Retained pre-amendment W0 fixture (wire v1): never regenerated; v2 loaders must reject it.
#: v2 scenarios whose orphan rows the database refuses at insert time (link -> observation FK),
#: a stricter guard than validation; the in-memory suite checks their check_bundle reason.
V2_DB_INSERT_REFUSED = {
    "exchange_document_missing": "foreign key",
    # The ledger family guard refuses an agency action carried by a non-agency package.
    "corroboration_agency_source_without_rights": "cannot carry this row",
}
LEGACY_V1_FIXTURE = "legacy_v1_bundle_rejection.json"
LEGACY_V1_FIXTURE_SHA256 = "326ad0bde4511237fd958503b7828f0e50517bc204638c80f90699240a5f3c78"


# ---------------------------------------------------------------------------
# W0 amendment 1 (bundle v2) scenarios: exchange relations, N-CEN family closure,
# corroboration and evidence-only subjects (plan section 9 items 1-4)
# ---------------------------------------------------------------------------
CUSIP_NEW = "999999AD7"
EXCHANGE_DATE = dt.date(2026, 6, 10)
EXCHANGE_DOC_AT = dt.datetime(2026, 6, 10, 21, 0, tzinfo=UTC)
NEW_SIDE_LINK_AT = dt.datetime(2026, 9, 23, 15, 0, tzinfo=UTC)
PAIRING_REVIEW_AT = dt.datetime(2026, 9, 22, 14, 0, tzinfo=UTC)


def new_side_document(q: c.CreditBundle, *, retrieved_at: dt.datetime = RETRIEVED_AT,
                      accept: str = "20260612170000") -> tuple[c.SourcePackage, c.CreditObservation]:
    """A separate new-notes EDGAR document (own package) evidencing the exchange's new side."""
    edgar_a = _pieces(q)["edgar_a"]
    accession = "9999999901-26-000022"
    pkg = _package("sec_edgar_document", f"SYNTHETIC-NEW-NOTES-{accept}", "public_government_record",
                   accession=accession, first_public=c.edgar_acceptance_to_utc(accept),
                   basis="edgar_acceptance_datetime", retrieved_at=retrieved_at)
    doc = _variant(edgar_a, package_id=pkg.package_id, row_locator="new-notes", semantic_key="synthetic:new-notes",
                   accession_number=accession, acceptance_raw=accept, acceptance_at=c.edgar_acceptance_to_utc(accept),
                   public_available_at=c.edgar_acceptance_to_utc(accept), first_seen_at=retrieved_at,
                   effective_date=EXCHANGE_DATE, effective_lower_exclusive=None, effective_upper_inclusive=None,
                   document_quote="SYNTHETIC new notes issued in the completed exchange",
                   document_location="new-notes-p1")
    return pkg, doc


def _exchange_inputs(q: c.CreditBundle, *, new_cusip: str = CUSIP_NEW, new_link_at: dt.datetime = NEW_SIDE_LINK_AT,
                     old_valid_to: dt.date | None = None, new_valid_to: dt.date | None = None,
                     pair_status: str = "accepted_evidence", pair_role: str = "human_reviewer",
                     pair_review_at: dt.datetime = PAIRING_REVIEW_AT,
                     pair_support: tuple[tuple[c.CreditObservation, c.EventLink], ...] = (),
                     doc_accept: str = "20260610170000",
                     new_doc: tuple[c.SourcePackage, c.CreditObservation] | None = None,
                     episode: uuid.UUID = EPISODE_A) -> dict:  # type: ignore[type-arg]
    """Old CUSIP A payment default plus a completed distressed exchange into ``new_cusip``.

    Old-side evidence is public in May; the new-side link is known later (``new_link_at``),
    so the event's closure time must move to it (§9 item 1). With ``new_doc`` the new side is
    evidenced by its own document (and package) instead of the shared exchange document.
    """
    x = _pieces(q)
    doc_pkg = _package("sec_edgar_document", f"SYNTHETIC-8K-EXCHANGE-{doc_accept}", "public_government_record",
                       accession="9999999901-26-000021", first_public=c.edgar_acceptance_to_utc(doc_accept),
                       basis="edgar_acceptance_datetime")
    doc = _variant(x["edgar_a"], package_id=doc_pkg.package_id, row_locator="item-exchange", semantic_key="synthetic:exchange",
                   accession_number="9999999901-26-000021", acceptance_raw=doc_accept,
                   acceptance_at=c.edgar_acceptance_to_utc(doc_accept),
                   public_available_at=c.edgar_acceptance_to_utc(doc_accept),
                   effective_date=EXCHANGE_DATE, effective_lower_exclusive=None, effective_upper_inclusive=None,
                   document_quote="SYNTHETIC completed distressed exchange of the old notes into new notes",
                   document_location="item-8.01-p2")
    link_pkg = x["link_pkg"]
    new_side = doc if new_doc is None else new_doc[1]
    docs = [doc] if new_doc is None else [doc, new_doc[1]]
    old_link = _link(link_pkg, doc, CUSIP_A, "SYNTHETIC-OBLIGOR-A", "exchange_old", EXCHANGE_DOC_AT + dt.timedelta(hours=1))  # type: ignore[arg-type]
    new_link = _link(link_pkg, new_side, new_cusip, "SYNTHETIC-OBLIGOR-A", "exchange_new", new_link_at)  # type: ignore[arg-type]
    if old_valid_to is not None:
        old_link = c.EventLink.create(**{**{n: getattr(old_link, n) for n, _ in old_link.SPEC if n != "link_id"},
                                          "valid_to": old_valid_to})
    if new_valid_to is not None:
        new_link = c.EventLink.create(**{**{n: getattr(new_link, n) for n, _ in new_link.SPEC if n != "link_id"},
                                          "valid_to": new_valid_to})
    relation_id = c.ExchangeRelation.derive_id(security_id(CUSIP_A), security_id(new_cusip), episode)
    # Exclusive relation end from the new link's inclusive end; a link that ended before the
    # exchange date has no valid window, and the reviewer's (open) window is kept instead.
    valid_to = None if new_link.valid_to in (None, dt.date.max) else new_link.valid_to + dt.timedelta(days=1)
    if valid_to is not None and valid_to <= EXCHANGE_DATE:
        valid_to = None
    pair = _adjudication(x["adj_pkg"], relation_id, pair_status, pair_role, pair_review_at,
                         [*docs, *(o for o, _link_row in pair_support)],  # type: ignore[arg-type]
                         [old_link, new_link, *(link_row for _o, link_row in pair_support)], subject_kind="exchange_pairing",
                         support=(EXCHANGE_DATE, valid_to) if pair_status == "accepted_evidence" else (None, None))
    return {"doc_pkg": doc_pkg, "doc": doc, "docs": docs, "new_doc": new_doc, "old_link": old_link,
            "new_link": new_link, "pair": pair, "relation_id": relation_id, "valid_to": valid_to,
            "new_cusip": new_cusip}


def _relation(q: c.CreditBundle, e: dict, frames: dict, **changes: object) -> c.ExchangeRelation:  # type: ignore[type-arg]
    values: dict[str, object] = {
        "relation_id": e["relation_id"], "old_security_id": security_id(CUSIP_A), "old_cusip9": CUSIP_A,
        "new_security_id": security_id(e["new_cusip"]), "new_cusip9": e["new_cusip"], "episode_id": EPISODE_A,
        "alias_spell_id": c.DefaultEpisode.derive_alias_spell_id(EPISODE_A), "old_link_id": e["old_link"].link_id,
        "new_link_id": e["new_link"].link_id,
        "exchange_document_observation_ids": c.sorted_uuids(d.observation_id for d in e["docs"]),
        "pairing_adjudication_id": e["pair"].adjudication_id, "exchange_effective_date": EXCHANGE_DATE,
        "valid_from": EXCHANGE_DATE, "valid_to": e["valid_to"], "evidence_known_at": EXCHANGE_DOC_AT,
    }
    values.update(changes)
    draft = c.ExchangeRelation(**values)  # type: ignore[arg-type]
    if "evidence_known_at" in changes:
        return draft
    known = pub.exchange_relation_known_at(frames, draft,
                                           knowledge_cutoff=q.manifest["knowledge_cutoff"],
                                           knowledge_mode=q.manifest["knowledge_mode"])
    return replace_row(draft, evidence_known_at=known)


def exchange_bundle(q: c.CreditBundle | None = None, *, relation_changes: dict | None = None,  # type: ignore[type-arg]
                    keep_relation_on_event: bool = True, drop: tuple = (), extra_rows: tuple = (),  # type: ignore[type-arg]
                    extra_relations: tuple = (), frame_overrides: dict | None = None,  # type: ignore[type-arg]
                    manifest_changes: dict | None = None, **knobs: object) -> c.CreditBundle:  # type: ignore[type-arg]
    """CUSIP A payment default whose episode later completes a distressed exchange (corroboration
    flag, exchange at/after onset) with a persisted directional relation into ``CUSIP_NEW``."""
    q = q or build_bundle()
    e = _exchange_inputs(q, **knobs)  # type: ignore[arg-type]
    ev_a = next(ev for ev in q.frames["events"] if ev.primary_type == "payment_default")  # type: ignore[attr-defined]
    new_rows = () if e["new_doc"] is None else tuple(e["new_doc"])
    extra = (e["doc_pkg"], e["doc"], *new_rows, e["old_link"], e["new_link"], e["pair"], *extra_rows)
    frames = frames_with(q, extra)
    relation = _relation(q, e, frames, **(relation_changes or {}))
    relations = (relation, *extra_relations)
    flags = tuple(sorted({*ev_a.corroboration_flags, "distressed_exchange"}))  # type: ignore[attr-defined]
    event = _derive_event(q, ev_a, (*extra, *relations), corroboration_flags=flags,
                          exchange_relation_ids=tuple(r.relation_id for r in relations) if keep_relation_on_event else (),
                          alias_spell_id=c.DefaultEpisode.derive_alias_spell_id(EPISODE_A) if keep_relation_on_event else None)
    rows = {"source_packages": (*q.frames["source_packages"], e["doc_pkg"],
                                *(r for r in (*new_rows, *extra_rows) if isinstance(r, c.SourcePackage))),
            "observations": (*q.frames["observations"], e["doc"],
                             *(r for r in (*new_rows, *extra_rows) if isinstance(r, c.CreditObservation))),
            "event_links": (*q.frames["event_links"], e["old_link"], e["new_link"],
                            *(r for r in extra_rows if isinstance(r, c.EventLink))),
            "adjudications": (*q.frames["adjudications"], e["pair"],
                              *(r for r in extra_rows if isinstance(r, c.Adjudication))),
            "exchange_relations": relations,
            "events": _replaced(q, "events", ev_a, event)}
    for frame, row in drop:
        rows[frame] = tuple(r for r in rows.get(frame, q.frames[frame]) if r.key() != row.key())
    return reassemble(q, frames={**rows, **(frame_overrides or {})}, **(manifest_changes or {}))


def _exchange_negatives(q: c.CreditBundle) -> dict[str, tuple[c.CreditBundle, str | None]]:
    """§9 item 1: relation time closure, link validity, pairing decision and inventory."""
    ex = exchange_bundle(q)
    rel = next(iter(ex.frames["exchange_relations"]))
    e = _exchange_inputs(q)
    invalid, knowledge = "exchange_pairing_invalid", "dependency_knowledge_mismatch"
    ev = next(x for x in ex.frames["events"] if x.episode_id == EPISODE_A)  # type: ignore[attr-defined]
    # Scalar old-side times (t1) on the event while its closure reaches the later new side (t2).
    before = _pieces(q)["ev_a"]
    assert ev.link_known_at == NEW_SIDE_LINK_AT > before.link_known_at  # type: ignore[attr-defined]
    forged_event = replace_row(ev, evidence_known_at=before.evidence_known_at, link_known_at=before.link_known_at)
    late = dt.datetime(2026, 9, 26, tzinfo=UTC)
    return {
        "exchange_valid_later_new_side": (ex, None),
        "exchange_relation_forged_old_side_time": (
            exchange_bundle(q, relation_changes={"evidence_known_at": EXCHANGE_DOC_AT}), knowledge),
        "exchange_event_forged_old_side_time": (
            reassemble(ex, frames={"events": _replaced(ex, "events", ev, forged_event)}), "event_evidence_invalid"),
        "exchange_new_side_after_cutoff": (exchange_bundle(q, new_link_at=late), knowledge),
        "exchange_new_link_expired_before_exchange_date": (
            exchange_bundle(q, new_valid_to=dt.date(2026, 6, 9)), invalid),
        "exchange_pairing_candidate": (
            exchange_bundle(q, pair_status="candidate", pair_role="extraction_proposer"), invalid),
        "exchange_relation_wrong_new_cusip": (exchange_bundle(q, relation_changes={"new_cusip9": CUSIP_C}), invalid),
        "exchange_relation_missing": (exchange_bundle(q, drop=(("exchange_relations", rel),)), "adjudication_subject_invalid"),
        "exchange_pairing_missing": (exchange_bundle(q, drop=(("adjudications", e["pair"]),)), invalid),
        "exchange_document_missing": (exchange_bundle(q, drop=(("observations", e["doc"]),)),
                                      "link_observation_outside_inventory"),
    }


CUSIP_NEW2 = synthetic_cusip(9)
AFTER_CUTOFF = dt.datetime(2026, 9, 26, 8, 0, tzinfo=UTC)


def _exchange_item1_scenarios(q: c.CreditBundle) -> dict[str, tuple[c.CreditBundle, str | None]]:
    """§9 item 1 (continued): new-side retrieval/retraction/revision, missing package, two
    exchanges out of one document and a rejected episode that keeps its relation."""
    invalid = "exchange_pairing_invalid"
    new_doc = new_side_document(q)
    late_doc = new_side_document(q, retrieved_at=AFTER_CUTOFF)
    e = _exchange_inputs(q, new_doc=new_doc)
    reconstruct = {"knowledge_mode": "historical_reconstruction"}
    # Two exchanges described by one document: A -> NEW and A -> NEW2, each separately paired.
    e2 = _exchange_inputs(q, new_cusip=CUSIP_NEW2, new_link_at=NEW_SIDE_LINK_AT - dt.timedelta(hours=2))
    base_rows = (e["doc_pkg"], e["doc"], e["old_link"], e2["new_link"], e2["pair"])
    both = frames_with(q, (*base_rows, _exchange_inputs(q)["new_link"], _exchange_inputs(q)["pair"]))
    rel2 = _relation(q, e2, both)
    rel2_reused = _relation(q, e2, both, pairing_adjudication_id=_exchange_inputs(q)["pair"].adjudication_id)
    x = _pieces(q)
    ex = exchange_bundle(q)
    rejected = _readjudicate(x["acc_a"], status="nonqualifying", supersedes_adjudication_id=x["acc_a"].adjudication_id,
                             adjudicated_at=PAIRING_REVIEW_AT + dt.timedelta(hours=1),
                             rationale="SYNTHETIC reviewer rejects the payment-default episode")
    return {
        "exchange_new_side_retrieved_after_cutoff_current_run": (
            exchange_bundle(q, new_doc=late_doc), "source_lineage_mismatch"),
        "exchange_new_side_retrieved_after_cutoff_reconstruction": (
            exchange_bundle(q, new_doc=late_doc, manifest_changes=reconstruct), None),
        "exchange_new_side_document_valid": (exchange_bundle(q, new_doc=new_doc), None),
        "exchange_new_side_observation_retracted": (exchange_bundle(
            q, new_doc=new_doc, extra_rows=(_obs_revision(new_doc[1], "new-notes-retraction", "retraction"),)),
            invalid),
        "exchange_new_link_rejected_revision": (exchange_bundle(
            q, new_doc=new_doc, extra_rows=(_link_revision(e["new_link"], status="rejected"),)), invalid),
        "exchange_new_side_package_missing": (exchange_bundle(
            q, new_doc=new_doc, drop=(("source_packages", new_doc[0]), ("observations", new_doc[1]))),
            "link_observation_outside_inventory"),
        "exchange_two_pairs_one_document": (exchange_bundle(
            q, extra_rows=(e2["new_link"], e2["pair"]), extra_relations=(rel2,)), None),
        "exchange_second_pair_reuses_first_pairing": (exchange_bundle(
            q, extra_rows=(e2["new_link"],), extra_relations=(rel2_reused,)), invalid),
        "exchange_rejected_episode_keeps_relation": (reassemble(ex, frames={
            "adjudications": (*ex.frames["adjudications"], rejected),
            "events": tuple(ev for ev in ex.frames["events"] if ev.episode_id != EPISODE_A)}), invalid),  # type: ignore[attr-defined]
    }


CORROBORATION_B = c.uuid5_of("synthetic_corroboration", "B-1")
LOWER_DATE = dt.date(2026, 1, 31)


def state_b_bundle(q: c.CreditBundle, *, extra: tuple = (), dates: tuple = (REPORT_DATE,),  # type: ignore[type-arg]
                   lower: dt.date | None = None, cor: c.Adjudication | None = None,
                   forge_known: dt.datetime | None = None) -> c.CreditBundle:
    """CUSIP B state event rebuilt honestly over ``q`` plus ``extra`` rows: contexts of ``dates``,
    the W1 proposal (bounds, family edges, optional corroboration), the accepting decision citing
    it and the event. ``forge_known`` replaces only the published proposal's knowledge time."""
    x = _pieces(q)
    k, mode = q.manifest["knowledge_cutoff"], q.manifest["knowledge_mode"]
    fr = frames_with(q, (*extra, *((cor,) if cor is not None else ())))
    base = {n: fr[n] for n in ("source_packages", "observations", "event_links", "ncen_filings", "adjudications")}
    contexts, members = pub.build_family_frames(base, dates, knowledge_cutoff=k, knowledge_mode=mode)
    fr.update(family_contexts=list(contexts), family_evidence=list(members))
    cors = (cor.adjudication_id,) if cor is not None else ()
    proposal = pub.derive_state_proposal(fr, cusip9=CUSIP_B, onset_upper_inclusive=REPORT_DATE, onset_lower_exclusive=lower,
                                         corroboration_adjudication_ids=cors, knowledge_cutoff=k, knowledge_mode=mode)
    obs = {o.observation_id: o for o in fr["observations"]}
    lower_obs = [obs[i] for i in proposal.onset_lower_evidence_ids]
    lower_links = [ln for ln in fr["event_links"] if ln.cusip9 == CUSIP_B and ln.observation_id in proposal.onset_lower_evidence_ids]
    ev_obs = [*x["nport_b"], *lower_obs]
    ev_links = [*x["links_b"], *lower_links]
    acc = _readjudicate(x["acc_b"], proposal_evidence_ids=(proposal.proposal_evidence_id,),
                        evidence_observation_ids=c.sorted_uuids(o.observation_id for o in ev_obs),
                        link_ids=c.sorted_uuids(ln.link_id for ln in ev_links))
    fr["adjudications"] = [acc if a is x["acc_b"] else a for a in fr["adjudications"]]
    fr["proposal_evidence"] = [proposal]
    event = _derive_event(q, x["ev_b"], (*extra, *((cor,) if cor is not None else ()), *contexts, *members, proposal, acc),
                          evidence_observation_ids=tuple(o.observation_id for o in ev_obs),
                          link_ids=tuple(ln.link_id for ln in ev_links), adjudication_ids=(acc.adjudication_id,),
                          onset_lower_exclusive=lower, onset_lower_evidence_ids=proposal.onset_lower_evidence_ids,
                          proposal_evidence_ids=(proposal.proposal_evidence_id,))
    fr["events"] = [event if ev is x["ev_b"] else ev for ev in fr["events"]]
    if forge_known is not None:
        fr["proposal_evidence"] = [replace_row(proposal, evidence_known_at=forge_known)]
    fr.pop("publication_sources")
    return reassemble(q, frames=fr)


def _lower_only_family_rows(q: c.CreditBundle) -> tuple:  # type: ignore[type-arg]
    """Prior-N votes on 2026-01-31 from registrant 11 and a lower-only registrant 14 whose
    effective N-CEN is accepted on 2026-07-15 (after every upper-context input)."""
    x = _pieces(q)
    b1 = x["nport_b"][0]
    ncen_pkg = next(p for p in q.frames["source_packages"] if p.source_family == "sec_ncen_public_xml")  # type: ignore[attr-defined]
    header = ncen_header("9999999901-26-000104", "20260715101500")
    filing = ncen_filing(ncen_pkg, header, "9999999914", ("S999999004",), "801-99914", "20260715101500")
    accept = "20260220101500"
    votes = tuple(
        _variant(b1, row_locator=f"row-00000{6 + i}", semantic_key=f"synthetic:row-00000{6 + i}",
                 holding_id=f"SYN-H-row-00000{6 + i}", accession_number=f"9999999901-26-00000{6 + i}",
                 registrant_cik=cik, series_id=series, fund_family_id=f"SYNTHETIC-FAMILY-{cik}",
                 report_date=LOWER_DATE, nport_is_default="N", acceptance_raw=accept,
                 acceptance_at=c.edgar_acceptance_to_utc(accept), public_available_at=c.edgar_acceptance_to_utc(accept))
        for i, (cik, series) in enumerate((("9999999911", "S999999001"), ("9999999914", "S999999004"))))
    links = tuple(_link(x["link_pkg"], v, CUSIP_B, "SYNTHETIC-OBLIGOR-B", "issue",
                        dt.datetime(2026, 9, 21, 10, 0, tzinfo=UTC)) for v in votes)
    return (header, filing, *votes, *links)


def corroboration_rows(q: c.CreditBundle, *, cusip: str = CUSIP_B, support_from: dt.date = dt.date(2026, 3, 1),
                       review_at: dt.datetime = dt.datetime(2026, 9, 24, 16, 0, tzinfo=UTC),
                       evidence: c.CreditObservation | None = None) -> tuple[tuple, c.Adjudication]:  # type: ignore[type-arg]
    """Same-family N-CEN (registrant 12 shares registrant 11's adviser) plus a reviewed
    corroboration document public at t3 = 2026-09-24T12:00Z, linked to ``cusip``."""
    x = _pieces(q)
    ncen_pkg = next(p for p in q.frames["source_packages"] if p.source_family == "sec_ncen_public_xml")  # type: ignore[attr-defined]
    series, _fn, accept, suffix = NCEN_REGISTRANTS["9999999912"]
    header = next(p for p in q.frames["source_packages"]  # type: ignore[attr-defined]
                  if p.source_family == "sec_ncen_acceptance_header" and p.accession_number == f"9999999901-26-{suffix}")
    same_family = ncen_filing(ncen_pkg, header, "9999999912", (series,), NCEN_REGISTRANTS["9999999911"][1], accept)
    doc = evidence if evidence is not None else _late_document(x["edgar_pkg"])
    link = _link(x["link_pkg"], doc, cusip, "SYNTHETIC-OBLIGOR-B", "issue", dt.datetime(2026, 9, 24, 14, 0, tzinfo=UTC))
    cor = c.Adjudication.create(
        package_id=x["adj_pkg"].package_id, subject_kind="corroboration", subject_id=CORROBORATION_B,
        status="accepted_evidence", supersedes_adjudication_id=None, policy_digest=c.POLICY_DIGEST,
        reviewer_id="synthetic-reviewer", reviewer_role="human_reviewer", adjudicated_at=review_at,
        rationale="SYNTHETIC reviewed same-obligation corroboration of the default state",
        evidence_observation_ids=(doc.observation_id,), link_ids=(link.link_id,), proposal_evidence_ids=(),
        support_valid_from=support_from, support_valid_to=None,
    )
    return (same_family, doc, link), cor


def _item2_scenarios(q: c.CreditBundle) -> dict[str, tuple[c.CreditBundle, str | None]]:
    """§9 item 2: family time needed only for the lower N bound; corroboration public at t3."""
    lower_rows = _lower_only_family_rows(q)
    lower_ok = state_b_bundle(q, extra=lower_rows, dates=(LOWER_DATE, REPORT_DATE), lower=LOWER_DATE)
    upper_ctx = next(ctx for ctx in lower_ok.frames["family_contexts"] if ctx.report_date == REPORT_DATE)  # type: ignore[attr-defined]
    cor_rows, cor = corroboration_rows(q)
    cor_ok = state_b_bundle(q, extra=cor_rows, cor=cor)
    knowledge = "dependency_knowledge_mismatch"
    return {
        "lower_only_family_time_valid": (lower_ok, None),
        "lower_only_family_time_forged_to_upper_closure": (state_b_bundle(
            q, extra=lower_rows, dates=(LOWER_DATE, REPORT_DATE), lower=LOWER_DATE,
            forge_known=upper_ctx.evidence_known_at), knowledge),  # type: ignore[attr-defined]
        "corroboration_t3_valid": (cor_ok, None),
        "corroboration_t3_forged_to_family_time": (state_b_bundle(
            q, extra=cor_rows, cor=cor, forge_known=next(iter(cor_ok.frames["family_contexts"])).evidence_known_at),  # type: ignore[attr-defined]
            knowledge),
    }


def _item3_scenarios(q: c.CreditBundle) -> dict[str, tuple[c.CreditBundle, str | None]]:
    """§9 item 3 (continued): missing voting series, fake early DERA availability/acceptance,
    removal of an N voter or a disputed voter from a context."""
    selection = "family_selection_invalid"
    other = "S999999091"
    missing_series = {"reported_series_ids": (other,), "adviser_records": (
        {"series_id": other, "role": "adviser", "file_number_raw": NCEN_REGISTRANTS["9999999911"][1],
         "crd_raw": None, "lei_raw": None},)}
    # DERA quarterly release published 2026-04-01; its copy of registrant 11's accession claims the
    # 2026-03-10 acceptance as data availability (earlier than the artifact itself).
    dera_pkg = _package("sec_ncen_dera", "SYNTHETIC-NCEN-DERA-2026Q1-EARLY", "public_government_record",
                        first_public=dt.datetime(2026, 4, 1, tzinfo=UTC), basis="archived_release_metadata")
    # A sole DERA copy (released 2026-02-02) whose acceptance is forged before the header's.
    early_pkg = _package("sec_ncen_dera", "SYNTHETIC-NCEN-DERA-FORGED", "public_government_record",
                         first_public=dt.datetime(2026, 2, 2, tzinfo=UTC), basis="archived_release_metadata")
    forged = "20260202101500"
    future_accept = "20261001101500"
    future_accepted = c.edgar_acceptance_to_utc(future_accept)
    future_retrieved = dt.datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
    future_accession = "9999999901-26-000401"
    future_header = _package(
        "sec_ncen_acceptance_header", "SYNTHETIC-NCEN-HDR-FUTURE", "public_government_record",
        accession=future_accession, first_public=future_accepted,
        basis="edgar_acceptance_datetime", retrieved_at=future_retrieved,
    )
    future_archive = _package(
        "sec_ncen_dera", "SYNTHETIC-NCEN-DERA-FUTURE-ACCEPTANCE", "public_government_record",
        first_public=dt.datetime(2026, 9, 20, tzinfo=UTC), basis="archived_release_metadata",
        retrieved_at=future_retrieved,
    )
    lot2 = _variant(_pieces(q)["nport_c"], row_locator="lot2-000003", semantic_key="synthetic:lot2-000003",
                    holding_id="SYN-H-lot2-000003", nport_is_default="Y")
    disputed = reassemble(q, frames={"observations": (*q.frames["observations"], lot2)}, rebind=True)
    future_bundle = _with_filing(
        q, "9999999911", rebuild=False, drop_base=True, packages=(future_archive, future_header),
        package_id=future_archive.package_id, accession_number=future_accession,
        row_locator="dera-row-future-acceptance", header_package_id=future_header.package_id,
        acceptance_raw=future_accept, acceptance_at=future_accepted,
        public_time_basis="archived_release_metadata",
        public_available_at=future_archive.first_verified_public_at,
        first_seen_at=future_retrieved, filing_date=dt.date(2026, 10, 1))
    return {
        "effective_filing_missing_voting_series_stale_member": (_with_filing(
            q, "9999999911", rebuild=False, **missing_series), selection),  # type: ignore[arg-type]
        "effective_filing_missing_voting_series_rebuilt": (_with_filing(
            q, "9999999911", rebuild=True, **missing_series), "proposal_evidence_not_closed"),  # type: ignore[arg-type]
        "dera_copy_public_before_its_release": (_with_filing(
            q, "9999999911", rebuild=False, packages=(dera_pkg,), package_id=dera_pkg.package_id,
            row_locator="dera-row-early"), "ncen_provenance_invalid"),
        "dera_acceptance_forged_before_header": (_with_filing(
            q, "9999999911", rebuild=True, drop_base=True, packages=(early_pkg,), package_id=early_pkg.package_id,
            row_locator="dera-row-forged", acceptance_raw=forged, acceptance_at=c.edgar_acceptance_to_utc(forged),
            public_available_at=c.edgar_acceptance_to_utc(forged), filing_date=dt.date(2026, 2, 2)),
            "ncen_provenance_invalid"),
        "historical_filing_accepted_after_cutoff": (
            reassemble(future_bundle, knowledge_mode="historical_reconstruction"),
            "ncen_provenance_invalid"),
        "context_with_disputed_voter_valid": (disputed, None),
        "context_disputed_voter_removed": (_voter_subset(
            disputed, lambda o: o.registrant_cik == "9999999913"), "family_universe_not_closed"),
    }


def corroborated_b(q: c.CreditBundle, **cor_knobs: object) -> c.CreditBundle:
    """CUSIP B same-family state whose second support is a reviewed corroboration (``cor_knobs``
    are passed to :func:`corroboration_rows`)."""
    rows, cor = corroboration_rows(q, **cor_knobs)  # type: ignore[arg-type]
    return state_b_bundle(q, extra=rows, cor=cor)


def _item4_scenarios(q: c.CreditBundle) -> dict[str, tuple[c.CreditBundle, str | None]]:
    """§9 item 4 at bundle level: corroboration scope, applicability, source rights and review
    time (present-day review of historical public evidence)."""
    not_effective = "corroboration_not_effective"
    agency_pkg, agency = _agency_inputs(q)
    issuer_pkg = new_package("issuer_public_document", "SYNTHETIC-ISSUER-AGENCY-COPY", "public_document_internal_use")
    smuggled = _variant(agency, package_id=issuer_pkg.package_id, row_locator="agency-copy",
                        semantic_key="synthetic:agency-copy")
    rows, cor = corroboration_rows(q, evidence=smuggled)
    late_review = state_b_bundle(q, extra=corroboration_rows(q, review_at=AFTER_CUTOFF)[0],
                                 cor=corroboration_rows(q, review_at=AFTER_CUTOFF)[1])
    del agency_pkg
    return {
        "corroboration_other_issue": (corroborated_b(q, cusip=CUSIP_C), not_effective),
        "corroboration_inapplicable_date": (corroborated_b(q, support_from=dt.date(2026, 4, 15)), not_effective),
        "corroboration_agency_source_without_rights": (
            state_b_bundle(q, extra=(issuer_pkg, *rows), cor=cor), "observation_family_mismatch"),
        "corroboration_review_after_cutoff_current_run": (late_review, "current_run_input_after_cutoff"),
        "corroboration_review_after_cutoff_reconstruction": (
            reassemble(late_review, knowledge_mode="historical_reconstruction"), None),
    }


def _item9_scenarios(q: c.CreditBundle) -> dict[str, tuple[c.CreditBundle, str | None]]:
    """§9 item 9 (mode-aware ``_stale_as_of``) at bundle level: a correction of event A's
    evidence public by K but ingested after K. Reconstruction counts it (the event relies on a
    superseded row); current_run cannot hold an input ingested after K at all."""
    edgar_a = _pieces(q)["edgar_a"]
    k = q.manifest["knowledge_cutoff"]
    revision = _variant(_obs_revision(edgar_a, "item-2.04-p3-corr", "correction"), first_seen_at=k + dt.timedelta(hours=6))
    frames = {"observations": (*q.frames["observations"], revision)}
    return {
        "revision_ingested_after_cutoff_reconstruction": (
            reassemble(q, frames=frames, knowledge_mode="historical_reconstruction"), "evidence_superseded"),
        "revision_ingested_after_cutoff_current_run": (reassemble(q, frames=frames), "current_run_input_after_cutoff"),
    }


SCAN_DATE = dt.date(2026, 2, 28)
GROUP_C_LATE_AT = dt.datetime(2026, 9, 24, 14, 15, tzinfo=UTC)


def _group_c_scan_rows(q: c.CreditBundle) -> tuple:  # type: ignore[type-arg]
    """A single earlier Y voter whose incomplete context is known after the endpoint context."""
    x = _pieces(q)
    ncen_pkg = next(p for p in q.frames["source_packages"] if p.source_family == "sec_ncen_public_xml")  # type: ignore[attr-defined]
    accept = "20260924101500"
    accession = "9999999901-26-000501"
    header = _package(
        "sec_ncen_acceptance_header", "SYNTHETIC-NCEN-HDR-GROUP-C-SCAN", "public_government_record",
        accession=accession, first_public=GROUP_C_LATE_AT, basis="edgar_acceptance_datetime",
        retrieved_at=GROUP_C_LATE_AT + dt.timedelta(hours=1),
    )
    filing = ncen_filing(
        ncen_pkg, header, "9999999914", ("S999999004",), "801-99914", accept,
        locator="ncen-group-c-scan",
    )
    template = x["nport_b"][0]
    vote = _variant(
        template, row_locator="group-c-scan-y", semantic_key="synthetic:group-c-scan-y",
        holding_id="SYN-H-group-c-scan-y", accession_number="9999999901-26-000502",
        registrant_cik="9999999914", series_id="S999999004", fund_family_id="SYNTHETIC-FAMILY-9999999914",
        report_date=SCAN_DATE, nport_is_default="Y",
    )
    link = _link(
        x["link_pkg"], vote, CUSIP_B, "SYNTHETIC-OBLIGOR-B", "issue",
        dt.datetime(2026, 9, 21, 11, 0, tzinfo=UTC),
    )
    return header, filing, vote, link


def _group_c_scan_bundle(q: c.CreditBundle, *, forge_known: dt.datetime | None = None) -> c.CreditBundle:
    return state_b_bundle(
        q, extra=_group_c_scan_rows(q), dates=(SCAN_DATE, REPORT_DATE), forge_known=forge_known,
    )


def _group_c_pair_support(q: c.CreditBundle) -> tuple[c.SourcePackage, c.CreditObservation, c.EventLink]:
    x = _pieces(q)
    package = _package(
        "issuer_public_document", "SYNTHETIC-GROUP-C-PAIR-SUPPORT", "public_document_internal_use",
        first_public=dt.datetime(2026, 6, 11, 13, 0, tzinfo=UTC), basis="first_verified_retrieval",
    )
    support = _variant(
        x["issuer_c"], package_id=package.package_id, row_locator="group-c-pair-support",
        semantic_key="synthetic:group-c-pair-support", cusip_raw=CUSIP_A, cusip9=CUSIP_A,
        security_id=security_id(CUSIP_A), effective_date=EXCHANGE_DATE,
        public_available_at=package.first_verified_public_at,
        first_seen_at=package.retrieved_at,
        document_quote="SYNTHETIC independent support for the reviewed exchange pairing",
        document_location="group-c-pair-support-p1",
    )
    link = _link(
        x["link_pkg"], support, CUSIP_A, "SYNTHETIC-OBLIGOR-A", "issue",
        dt.datetime(2026, 9, 23, 18, 0, tzinfo=UTC),
    )
    return package, support, link


def _link_status(link: c.EventLink, status: str) -> c.EventLink:
    return c.EventLink.create(**{
        **{name: getattr(link, name) for name, _ in link.SPEC if name != "link_id"},
        "status": status,
    })


def _group_c_pair_bundle(q: c.CreditBundle, *, retraction_at: dt.datetime | None = None,
                         knowledge_cutoff: dt.datetime | None = None) -> c.CreditBundle:
    package, support, link = _group_c_pair_support(q)
    extra: tuple = (package, support, link)
    if retraction_at is not None:
        retraction = _variant(
            support, row_locator=f"group-c-pair-support-retraction-{retraction_at.date().isoformat()}",
            semantic_key=f"synthetic:group-c-pair-support-retraction:{retraction_at.isoformat()}",
            revision_kind="retraction", supersedes_observation_id=support.observation_id,
            public_available_at=retraction_at, first_seen_at=retraction_at + dt.timedelta(minutes=5),
        )
        extra = (*extra, retraction)
    changes = {} if knowledge_cutoff is None else {"knowledge_cutoff": knowledge_cutoff}
    if retraction_at is not None and retraction_at > changes.get("knowledge_cutoff", q.manifest["knowledge_cutoff"]):
        changes["knowledge_mode"] = "historical_reconstruction"
    return exchange_bundle(
        q, pair_support=((support, link),), extra_rows=extra, manifest_changes=changes,
    )


def _group_c_pair_decision_bundle(q: c.CreditBundle, *, revoked_at: dt.datetime,
                                  knowledge_cutoff: dt.datetime | None = None) -> c.CreditBundle:
    pair = _exchange_inputs(q)["pair"]
    revoked = _readjudicate(
        pair, status="retracted", supersedes_adjudication_id=pair.adjudication_id,
        adjudicated_at=revoked_at, support_valid_from=None, support_valid_to=None,
    )
    changes = {} if knowledge_cutoff is None else {"knowledge_cutoff": knowledge_cutoff}
    if revoked_at > changes.get("knowledge_cutoff", q.manifest["knowledge_cutoff"]):
        changes["knowledge_mode"] = "historical_reconstruction"
    return exchange_bundle(q, extra_rows=(revoked,), manifest_changes=changes)


def _group_c_missing_proposal_bundle(q: c.CreditBundle) -> c.CreditBundle:
    bundle = _group_c_scan_bundle(q)
    event = next(ev for ev in bundle.frames["events"] if ev.episode_id == EPISODE_B)  # type: ignore[attr-defined]
    accepted = next(a for a in bundle.frames["adjudications"]  # type: ignore[attr-defined]
                    if a.subject_id == EPISODE_B and a.status == "accepted_state")
    valid_proposal = next(iter(bundle.frames["proposal_evidence"]))
    missing = uuid.UUID(int=0x66)
    old = _readjudicate(
        accepted, proposal_evidence_ids=c.sorted_uuids((valid_proposal.proposal_evidence_id, missing)),
        adjudicated_at=accepted.adjudicated_at + dt.timedelta(minutes=1),
    )
    head = _readjudicate(
        old, proposal_evidence_ids=(valid_proposal.proposal_evidence_id,),
        supersedes_adjudication_id=old.adjudication_id,
        adjudicated_at=old.adjudicated_at + dt.timedelta(minutes=1),
    )
    adjudications = tuple(
        head if row is accepted else row for row in bundle.frames["adjudications"]
    ) + (old,)
    rebuilt = _derive_event(
        bundle, event, (old, head),
        adjudication_ids=(head.adjudication_id,),
    )
    followup = c.FollowUp(
        security_id=event.security_id, spell_id=event.episode_id,
        segment_id=c.uuid5_of("synthetic_group_c_missing_proposal", str(event.episode_id)), cusip9=event.cusip9,
        interval_start_exclusive=event.onset_upper_inclusive,
        interval_end_inclusive=event.onset_upper_inclusive + dt.timedelta(days=1),
        status="unknown", completeness_basis="none", evidence_observation_ids=(),
        adjudication_ids=(old.adjudication_id,), known_at=GROUP_C_LATE_AT,
    )
    return reassemble(bundle, frames={
        "adjudications": adjudications,
        "events": _replaced(bundle, "events", event, rebuilt),
        "followups": (*bundle.frames["followups"], followup),
    })


def _group_c_scope_cycle_bundle(q: c.CreditBundle) -> c.CreditBundle:
    package, first, first_link = _group_c_pair_support(q)
    first_link = _link_status(first_link, "quarantined")
    second = _variant(
        first, row_locator="group-c-pair-support-2", semantic_key="synthetic:group-c-pair-support-2",
        document_location="group-c-pair-support-p2",
    )
    second_link = _link_status(_link(
        _pieces(q)["link_pkg"], second, CUSIP_A, "SYNTHETIC-OBLIGOR-A", "issue",
        dt.datetime(2026, 9, 23, 18, 5, tzinfo=UTC),
    ), "quarantined")
    adj_pkg = _pieces(q)["adj_pkg"]
    evidence_ids = c.sorted_uuids((first.observation_id, second.observation_id))
    links = c.sorted_uuids((first_link.link_id, second_link.link_id))
    scope_a = c.Adjudication.create(
        package_id=adj_pkg.package_id, subject_kind="issue_scope", subject_id=first_link.link_id,
        status="accepted_evidence", supersedes_adjudication_id=None, policy_digest=c.POLICY_DIGEST,
        reviewer_id="synthetic-reviewer", reviewer_role="human_reviewer",
        adjudicated_at=dt.datetime(2026, 9, 23, 19, 0, tzinfo=UTC),
        rationale="SYNTHETIC group-C scope dependency A", evidence_observation_ids=evidence_ids,
        link_ids=links, proposal_evidence_ids=(), support_valid_from=None, support_valid_to=None,
    )
    scope_b = c.Adjudication.create(
        package_id=adj_pkg.package_id, subject_kind="issue_scope", subject_id=second_link.link_id,
        status="accepted_evidence", supersedes_adjudication_id=None, policy_digest=c.POLICY_DIGEST,
        reviewer_id="synthetic-reviewer", reviewer_role="human_reviewer",
        adjudicated_at=dt.datetime(2026, 9, 23, 19, 5, tzinfo=UTC),
        rationale="SYNTHETIC group-C scope dependency B", evidence_observation_ids=evidence_ids,
        link_ids=links, proposal_evidence_ids=(), support_valid_from=None, support_valid_to=None,
    )
    return exchange_bundle(
        q, pair_support=((first, first_link),),
        extra_rows=(package, first, second, first_link, second_link, scope_a, scope_b),
    )


def _group_c_late_raw_package_bundle(q: c.CreditBundle) -> c.CreditBundle:
    vote = _pieces(q)["nport_b"][0]
    package = next(p for p in q.frames["source_packages"] if p.package_id == vote.package_id)  # type: ignore[attr-defined]
    later = replace_row(
        package, first_verified_public_at=AFTER_CUTOFF,
        retrieved_at=AFTER_CUTOFF + dt.timedelta(hours=1),
    )
    packages = tuple(later if row is package else row for row in q.frames["source_packages"])
    return reassemble(q, frames={"source_packages": packages}, knowledge_mode="historical_reconstruction")


def _group_c_multihop(q: c.CreditBundle, *, shallow: bool, followup: bool) -> c.CreditBundle:
    bundle = _group_c_scan_bundle(q)
    proposal = next(iter(bundle.frames["proposal_evidence"]))
    event = next(ev for ev in bundle.frames["events"] if ev.episode_id == EPISODE_B)  # type: ignore[attr-defined]
    accepting = next(a for a in bundle.frames["adjudications"] if a.subject_id == EPISODE_B)  # type: ignore[attr-defined]
    direct = max(
        [next(o for o in bundle.frames["observations"] if o.observation_id == oid).public_available_at
         for oid in accepting.evidence_observation_ids]
        + [next(link for link in bundle.frames["event_links"] if link.link_id == lid).link_known_at
           for lid in accepting.link_ids]
    )
    known = direct if shallow else max(proposal.evidence_known_at, GROUP_C_LATE_AT)
    if followup:
        row = c.FollowUp(
            security_id=event.security_id, spell_id=event.episode_id,
            segment_id=c.uuid5_of("synthetic_group_c_followup", str(event.episode_id)), cusip9=event.cusip9,
            interval_start_exclusive=event.onset_upper_inclusive,
            interval_end_inclusive=event.onset_upper_inclusive + dt.timedelta(days=1),
            status="unknown", completeness_basis="none", evidence_observation_ids=(),
            adjudication_ids=(accepting.adjudication_id,), known_at=known,
        )
        return reassemble(bundle, frames={"followups": (*bundle.frames["followups"], row)})
    resolved = replace_row(
        event, resolution_date=event.onset_upper_inclusive + dt.timedelta(days=1),
        resolution_refs=(accepting.adjudication_id,), resolution_known_at=known,
    )
    return reassemble(bundle, frames={"events": _replaced(bundle, "events", event, resolved)})


def _group_c_scenarios(q: c.CreditBundle) -> dict[str, tuple[c.CreditBundle, str | None]]:
    scan = _group_c_scan_bundle(q)
    upper = next(ctx for ctx in scan.frames["family_contexts"] if ctx.report_date == REPORT_DATE)  # type: ignore[attr-defined]
    before = dt.datetime(2026, 9, 24, 9, 0, tzinfo=UTC)
    after = dt.datetime(2026, 9, 26, 9, 0, tzinfo=UTC)
    later_k = dt.datetime(2026, 9, 27, tzinfo=UTC)
    package, support, link = _group_c_pair_support(q)
    valid_pair = exchange_bundle(q, pair_support=((support, link),), extra_rows=(package, support, link))
    return {
        "group_c_scan_context_valid": (scan, None),
        "group_c_scan_context_forged_endpoint_time": (
            _group_c_scan_bundle(q, forge_known=upper.evidence_known_at), "dependency_knowledge_mismatch"),
        "group_c_resolution_multihop_valid": (_group_c_multihop(q, shallow=False, followup=False), None),
        "group_c_resolution_multihop_forged_shallow": (
            _group_c_multihop(q, shallow=True, followup=False), "resolution_knowledge_invalid"),
        "group_c_followup_multihop_forged_shallow": (
            _group_c_multihop(q, shallow=True, followup=True), "followup_invalid"),
        "group_c_pairing_extra_support_valid": (valid_pair, None),
        "group_c_pairing_support_retracted_before_cutoff": (
            _group_c_pair_bundle(q, retraction_at=before), "evidence_superseded"),
        "group_c_pairing_support_retraction_after_cutoff": (
            _group_c_pair_bundle(q, retraction_at=after), None),
        "group_c_pairing_support_retraction_visible_later": (
            _group_c_pair_bundle(q, retraction_at=after, knowledge_cutoff=later_k), "evidence_superseded"),
        "group_c_pairing_decision_revoked_before_cutoff": (
            _group_c_pair_decision_bundle(q, revoked_at=before), "exchange_pairing_invalid"),
        "group_c_pairing_decision_revoked_after_cutoff": (
            _group_c_pair_decision_bundle(q, revoked_at=after), None),
        "group_c_pairing_decision_revocation_visible_later": (
            _group_c_pair_decision_bundle(q, revoked_at=after, knowledge_cutoff=later_k),
            "exchange_pairing_invalid"),
        "group_c_pairing_accepted_after_cutoff_reconstruction": (
            exchange_bundle(q, pair_review_at=AFTER_CUTOFF,
                            manifest_changes={"knowledge_mode": "historical_reconstruction"}),
            "exchange_pairing_invalid"),
        "group_c_pairing_rejected_extra_support": (
            exchange_bundle(
                q, pair_support=((support, _link_status(link, "rejected")),),
                extra_rows=(package, support, _link_status(link, "rejected")),
            ),
            "exchange_pairing_invalid"),
        "group_c_missing_nested_proposal": (_group_c_missing_proposal_bundle(q), "dependency_missing"),
        "group_c_scope_dependency_cycle": (_group_c_scope_cycle_bundle(q), "dependency_cycle"),
        "group_c_late_raw_package_attested_observation": (_group_c_late_raw_package_bundle(q), None),
        "group_c_old_link_ends_before_successor": (
            exchange_bundle(q, old_valid_to=dt.date(2026, 6, 30), new_valid_to=dt.date(2026, 7, 31)),
            "exchange_pairing_invalid"),
        "group_c_successor_date_max": (
            exchange_bundle(q, new_valid_to=dt.date.max), "exchange_pairing_invalid"),
    }


def _issue_head(bundle: c.CreditBundle, event: c.DefaultEpisode) -> c.Adjudication:
    rows = {row.adjudication_id: row for row in bundle.frames["adjudications"]}  # type: ignore[attr-defined]
    return pub._effective_heads(rows)[("issue_episode", event.episode_id)]


def _event_from_frames(
    bundle: c.CreditBundle,
    event: c.DefaultEpisode,
    frames: dict,
    **changes: object,
) -> c.DefaultEpisode:  # type: ignore[type-arg]
    values = {name: getattr(event, name) for name, _ in event.SPEC}
    values.update(changes)
    edges = (
        "evidence_observation_ids", "onset_lower_evidence_ids", "link_ids", "adjudication_ids",
        "proposal_evidence_ids", "exchange_relation_ids",
    )
    values.update(pub.derive_event_fields(
        frames,
        knowledge_cutoff=bundle.manifest["knowledge_cutoff"],
        knowledge_mode=bundle.manifest["knowledge_mode"],
        **{name: values[name] for name in edges},
    ))
    values["timing_class"] = c.derive_timing_class(
        values["onset_lower_exclusive"], values["onset_upper_inclusive"],  # type: ignore[arg-type]
    )
    return c.DefaultEpisode(**values)


def _replace_issue_head(
    bundle: c.CreditBundle,
    event: c.DefaultEpisode,
    heads: tuple[c.Adjudication, ...],
    *,
    event_proposal_ids: tuple[uuid.UUID, ...] | None = None,
    proposal_rows: tuple[c.ProposalEvidence, ...] | None = None,
    flags: tuple[str, ...] | None = None,
    frame_overrides: dict | None = None,  # type: ignore[type-arg]
) -> c.CreditBundle:
    old = _issue_head(bundle, event)
    frames = {name: tuple(rows) for name, rows in bundle.frames.items()}
    frames["adjudications"] = tuple(
        row for row in bundle.frames["adjudications"] if row.adjudication_id != old.adjudication_id  # type: ignore[attr-defined]
    ) + heads
    if proposal_rows is not None:
        frames["proposal_evidence"] = proposal_rows
    frames.update(frame_overrides or {})
    adjudication_ids = c.sorted_uuids((
        *(row_id for row_id in event.adjudication_ids if row_id != old.adjudication_id),
        *(row.adjudication_id for row in heads),
    ))
    updated = _event_from_frames(
        bundle,
        event,
        frames,
        adjudication_ids=adjudication_ids,
        proposal_evidence_ids=(event.proposal_evidence_ids if event_proposal_ids is None else event_proposal_ids),
        corroboration_flags=(event.corroboration_flags if flags is None else flags),
    )
    frames["events"] = _replaced(bundle, "events", event, updated)
    return reassemble(bundle, frames=frames)


def _proposal_variant(proposal: c.ProposalEvidence, **changes: object) -> c.ProposalEvidence:
    values = {
        name: getattr(proposal, name)
        for name, _ in proposal.SPEC
        if name != "proposal_evidence_id"
    }
    values.update(changes)
    return c.ProposalEvidence.create(**values)


def _policy_state_without_proposal(q: c.CreditBundle, *, human: bool) -> c.CreditBundle:
    event = _pieces(q)["ev_b"]
    head = _issue_head(q, event)
    replacement = _readjudicate(
        head,
        reviewer_id="synthetic-human" if human else head.reviewer_id,
        reviewer_role="human_reviewer" if human else "policy_rule_engine",
        rationale="SYNTHETIC explicit state" if human else "SYNTHETIC proposal-less policy state",
        proposal_evidence_ids=(),
    )
    return _replace_issue_head(
        q,
        event,
        (replacement,),
        event_proposal_ids=(),
        proposal_rows=(),
        flags=(),
        frame_overrides={
            "family_contexts": (),
            "family_evidence": (),
            "ncen_filings": (),
        },
    )


def _missing_proposal_reference(q: c.CreditBundle) -> c.CreditBundle:
    event = _pieces(q)["ev_b"]
    old = _issue_head(q, event)
    missing = c.uuid5_of("synthetic_group_d_missing_proposal", str(event.episode_id))
    head = _readjudicate(old, proposal_evidence_ids=(missing,))
    adjudications = tuple(
        row for row in q.frames["adjudications"] if row.adjudication_id != old.adjudication_id  # type: ignore[attr-defined]
    ) + (head,)
    values = {name: getattr(event, name) for name, _ in event.SPEC}
    values.update(
        adjudication_ids=(head.adjudication_id,),
        proposal_evidence_ids=(missing,),
    )
    values["event_input_digest"] = c.DefaultEpisode.derive_input_digest(
        values["evidence_observation_ids"],
        values["link_ids"],
        values["adjudication_ids"],
        values["proposal_evidence_ids"],
        values["exchange_relation_ids"],
        values["dependency_digest"],
    )
    updated = c.DefaultEpisode(**values)
    return reassemble(q, frames={
        "adjudications": adjudications,
        "events": _replaced(q, "events", event, updated),
    })


def _superseded_issue_heads(
    q: c.CreditBundle,
    *,
    later_review: dt.datetime | None = None,
    negative_old: bool = False,
) -> tuple[c.CreditBundle, c.DefaultEpisode, c.Adjudication, c.Adjudication]:
    event = _pieces(q)["ev_a"]
    head = _issue_head(q, event)
    old = _readjudicate(
        head,
        status="retracted" if negative_old else head.status,
        rationale="SYNTHETIC superseded negative issue revision" if negative_old else "SYNTHETIC old admitting issue revision",
    )
    current = _readjudicate(
        head,
        supersedes_adjudication_id=old.adjudication_id,
        adjudicated_at=later_review or (head.adjudicated_at + dt.timedelta(minutes=1)),
        rationale="SYNTHETIC current admitting issue revision",
    )
    rebound = _replace_issue_head(q, event, (old, current))
    current_event = next(row for row in rebound.frames["events"] if row.episode_id == event.episode_id)  # type: ignore[attr-defined]
    return rebound, current_event, old, current


def _support_known_at(bundle: c.CreditBundle, adjudication_ids: tuple[uuid.UUID, ...]) -> dt.datetime:
    ix = pub._Index.build(
        bundle.frames,
        k=bundle.manifest["knowledge_cutoff"],
        mode=bundle.manifest["knowledge_mode"],
        policy_digest=bundle.manifest["policy_digest"],
    )
    return max(pub._support_times((), adjudication_ids, ix))


def _resolved_by(
    bundle: c.CreditBundle,
    event: c.DefaultEpisode,
    adjudication_ids: tuple[uuid.UUID, ...],
    *,
    known_at: dt.datetime | None = None,
) -> c.CreditBundle:
    resolved = replace_row(
        event,
        resolution_date=event.onset_upper_inclusive + dt.timedelta(days=1),
        resolution_refs=adjudication_ids,
        resolution_known_at=known_at or _support_known_at(bundle, adjudication_ids),
    )
    return reassemble(bundle, frames={"events": _replaced(bundle, "events", event, resolved)})


def _followup_by(
    bundle: c.CreditBundle,
    event: c.DefaultEpisode,
    adjudication_ids: tuple[uuid.UUID, ...],
    *,
    security: uuid.UUID | None = None,
    cusip: str | None = None,
    known_at: dt.datetime | None = None,
) -> c.CreditBundle:
    row = c.FollowUp(
        security_id=event.security_id if security is None else security,
        spell_id=event.episode_id,
        segment_id=c.uuid5_of(
            "synthetic_group_d_followup",
            str(event.episode_id),
            *(str(item) for item in adjudication_ids),
            str(security),
            str(cusip),
        ),
        cusip9=event.cusip9 if cusip is None else cusip,
        interval_start_exclusive=event.onset_upper_inclusive,
        interval_end_inclusive=event.onset_upper_inclusive + dt.timedelta(days=1),
        status="unknown",
        completeness_basis="none",
        evidence_observation_ids=(),
        adjudication_ids=c.sorted_uuids(adjudication_ids),
        known_at=known_at or _support_known_at(bundle, adjudication_ids),
    )
    return reassemble(bundle, frames={"followups": (*bundle.frames["followups"], row)})


def _evidence_only_authority(
    q: c.CreditBundle,
    kind: str,
    *,
    policy_digest: str = c.POLICY_DIGEST,
) -> tuple[c.CreditBundle, c.DefaultEpisode, c.Adjudication]:
    if kind == "exchange_pairing":
        bundle = exchange_bundle(q)
        event = next(row for row in bundle.frames["events"] if row.episode_id == EPISODE_A)  # type: ignore[attr-defined]
        decision = next(row for row in bundle.frames["adjudications"] if row.subject_kind == kind)  # type: ignore[attr-defined]
        return bundle, event, decision
    if kind == "corroboration":
        bundle = corroborated_b(q)
        event = next(row for row in bundle.frames["events"] if row.episode_id == EPISODE_B)  # type: ignore[attr-defined]
        decision = next(row for row in bundle.frames["adjudications"] if row.subject_kind == kind)  # type: ignore[attr-defined]
        return bundle, event, decision

    x = _pieces(q)
    if kind == "issue_scope":
        scoped = next(row for row in q.frames["event_links"] if row.status == "quarantined")  # type: ignore[attr-defined]
        evidence = x["nport_c"]
        subject_id = scoped.link_id
        link_ids = (scoped.link_id,)
    else:
        evidence = x["issuer_c"]
        subject_id = c.uuid5_of("synthetic_group_d_continuity", CUSIP_A)
        link_ids = ()
    decision = c.Adjudication.create(
        package_id=x["adj_pkg"].package_id,
        subject_kind=kind,
        subject_id=subject_id,
        status="accepted_evidence",
        supersedes_adjudication_id=None,
        policy_digest=policy_digest,
        reviewer_id="synthetic-reviewer",
        reviewer_role="human_reviewer",
        adjudicated_at=dt.datetime(2026, 9, 23, 15, 0, tzinfo=UTC),
        rationale=f"SYNTHETIC {kind} evidence-only decision",
        evidence_observation_ids=(evidence.observation_id,),
        link_ids=link_ids,
        proposal_evidence_ids=(),
        support_valid_from=None,
        support_valid_to=None,
    )
    bundle = reassemble(q, frames={"adjudications": (*q.frames["adjudications"], decision)})
    event = next(row for row in bundle.frames["events"] if row.episode_id == EPISODE_A)  # type: ignore[attr-defined]
    return bundle, event, decision


def group_d_regressions() -> dict[str, tuple[c.CreditBundle, str | None]]:
    """Final-review admission/cure scenarios shared by Python and PostgreSQL validators."""
    q = build_bundle()
    x = _pieces(q)
    event_a, event_b = x["ev_a"], x["ev_b"]
    head_a, head_b = _issue_head(q, event_a), _issue_head(q, event_b)
    proposal = next(iter(q.frames["proposal_evidence"]))
    candidate = _proposal_variant(
        proposal,
        proposed_status="candidate",
        onset_lower_exclusive=None,
        onset_upper_inclusive=None,
        onset_lower_evidence_ids=(),
        onset_upper_evidence_ids=(),
    )
    wrong_cusip = _proposal_variant(proposal, cusip9=CUSIP_C)
    old_proposal_less = _readjudicate(
        head_b,
        proposal_evidence_ids=(),
        adjudicated_at=head_b.adjudicated_at - dt.timedelta(minutes=1),
        rationale="SYNTHETIC superseded proposal-less state",
    )
    current_proposal = _readjudicate(
        head_b,
        supersedes_adjudication_id=old_proposal_less.adjudication_id,
        adjudicated_at=head_b.adjudicated_at,
        rationale="SYNTHETIC current proposal-bound state",
    )
    proposal_less_superseded = _replace_issue_head(q, event_b, (old_proposal_less, current_proposal))

    candidate_head = _readjudicate(head_b, proposal_evidence_ids=(candidate.proposal_evidence_id,))
    candidate_bundle = _replace_issue_head(
        q, event_b, (candidate_head,),
        event_proposal_ids=(candidate.proposal_evidence_id,), proposal_rows=(candidate,),
    )
    wrong_cusip_head = _readjudicate(head_b, proposal_evidence_ids=(wrong_cusip.proposal_evidence_id,))
    wrong_cusip_bundle = _replace_issue_head(
        q, event_b, (wrong_cusip_head,),
        event_proposal_ids=(wrong_cusip.proposal_evidence_id,), proposal_rows=(wrong_cusip,),
    )
    mismatched_event = _event_from_frames(
        q, event_b, dict(q.frames), proposal_evidence_ids=(), corroboration_flags=(),
    )
    array_mismatch = reassemble(q, frames={"events": _replaced(q, "events", event_b, mismatched_event)})
    wrong_episode = _event_from_frames(
        q,
        event_a,
        dict(q.frames),
        adjudication_ids=(head_b.adjudication_id,),
        proposal_evidence_ids=head_b.proposal_evidence_ids,
        corroboration_flags=c.sorted_texts((*event_a.corroboration_flags, "nport_consensus_state")),
    )
    omitted_head = _event_from_frames(
        q,
        event_a,
        dict(q.frames),
        adjudication_ids=(x["cand_a"].adjudication_id,),
    )

    superseded, superseded_event, old_head, current_head = _superseded_issue_heads(q)
    negative_chain, negative_event, negative_head, negative_current = _superseded_issue_heads(q, negative_old=True)
    historical = reassemble(q, knowledge_mode="historical_reconstruction")
    historical_chain, historical_event, historical_old, historical_current = _superseded_issue_heads(
        historical,
        later_review=historical.manifest["knowledge_cutoff"] + dt.timedelta(days=1),
    )
    unknown_resolution_ref = uuid.UUID("ffffffff-ffff-4fff-bfff-ffffffffffff")
    mixed_inventory_event = replace_row(
        event_a,
        resolution_date=event_a.onset_upper_inclusive + dt.timedelta(days=1),
        resolution_refs=c.sorted_uuids((head_b.adjudication_id, unknown_resolution_ref)),
        resolution_known_at=_support_known_at(q, (head_b.adjudication_id,)),
    )
    missing_nested = _group_c_missing_proposal_bundle(q)
    missing_id = uuid.UUID(int=0x66)
    missing_event = next(
        row for row in missing_nested.frames["events"] if row.episode_id == EPISODE_B  # type: ignore[attr-defined]
    )
    missing_head = next(
        row for row in missing_nested.frames["adjudications"]  # type: ignore[attr-defined]
        if missing_id in row.proposal_evidence_ids
    )
    missing_resolution = replace_row(
        missing_event,
        resolution_date=missing_event.onset_upper_inclusive + dt.timedelta(days=1),
        resolution_refs=(missing_head.adjudication_id,),
        resolution_known_at=GROUP_C_LATE_AT,
    )
    missing_nested_resolution = reassemble(missing_nested, frames={
        "events": _replaced(missing_nested, "events", missing_event, missing_resolution),
        "followups": tuple(
            row for row in missing_nested.frames["followups"]
            if missing_head.adjudication_id not in row.adjudication_ids  # type: ignore[attr-defined]
        ),
    })

    cases: dict[str, tuple[c.CreditBundle, str | None]] = {
        "group_d_policy_engine_proposal_less": (
            _policy_state_without_proposal(q, human=False), "proposal_evidence_not_closed"),
        "group_d_human_explicit_state_without_proposal_valid": (
            _policy_state_without_proposal(q, human=True), None),
        "group_d_genuine_proposal_state_valid": (q, None),
        "group_d_superseded_proposal_less_state_valid": (proposal_less_superseded, None),
        "group_d_proposal_missing": (_missing_proposal_reference(q), "proposal_evidence_not_closed"),
        "group_d_proposal_candidate": (candidate_bundle, "proposal_evidence_not_closed"),
        "group_d_proposal_wrong_cusip": (wrong_cusip_bundle, "proposal_evidence_not_closed"),
        "group_d_proposal_arrays_mismatch": (array_mismatch, "proposal_evidence_not_closed"),
        "group_d_event_wrong_episode_head": (
            reassemble(q, frames={"events": _replaced(q, "events", event_a, wrong_episode)}),
            "event_evidence_invalid"),
        "group_d_event_effective_head_omitted": (
            reassemble(q, frames={"events": _replaced(q, "events", event_a, omitted_head)}),
            "event_evidence_invalid"),
        "group_d_resolution_exact_human_head_valid": (
            _resolved_by(q, event_a, (head_a.adjudication_id,)), None),
        "group_d_resolution_exact_policy_head_valid": (
            _group_c_multihop(q, shallow=False, followup=False), None),
        "group_d_resolution_observation_only_valid": (
            _dependency_closure_scenarios(q)["resolution_own_knowledge_time"][0], None),
        "group_d_resolution_other_episode_head": (
            _resolved_by(q, event_a, (head_b.adjudication_id,)), "resolution_adjudication_invalid"),
        "group_d_resolution_inventory_precedes_authority": (
            reassemble(q, frames={
                "events": _replaced(q, "events", event_a, mixed_inventory_event),
            }),
            "resolution_ref_not_in_inventory"),
        "group_d_resolution_nested_dependency_precedes_authority": (
            missing_nested_resolution, "dependency_missing"),
        "group_d_resolution_candidate_revision": (
            _resolved_by(q, event_a, (x["cand_a"].adjudication_id,)), "resolution_adjudication_invalid"),
        "group_d_resolution_superseded_head": (
            _resolved_by(superseded, superseded_event, (old_head.adjudication_id,)),
            "resolution_adjudication_invalid"),
        "group_d_resolution_negative_revision": (
            _resolved_by(negative_chain, negative_event, (negative_head.adjudication_id,)),
            "resolution_adjudication_invalid"),
        "group_d_resolution_current_head_after_negative_valid": (
            _resolved_by(negative_chain, negative_event, (negative_current.adjudication_id,)), None),
        "group_d_resolution_mixed_valid_invalid": (
            _resolved_by(superseded, superseded_event,
                         (current_head.adjudication_id, old_head.adjudication_id)),
            "resolution_adjudication_invalid"),
        "group_d_followup_empty_array_valid": (q, None),
        "group_d_followup_matching_event_head_valid": (
            _followup_by(q, event_a, (head_a.adjudication_id,)), None),
        "group_d_followup_other_security": (
            _followup_by(q, event_a, (head_a.adjudication_id,),
                         security=security_id(CUSIP_C), cusip=CUSIP_C),
            "followup_adjudication_invalid"),
        "group_d_followup_same_security_wrong_cusip": (
            _followup_by(q, event_a, (head_a.adjudication_id,), cusip=CUSIP_B),
            "followup_adjudication_invalid"),
        "group_d_followup_superseded_head": (
            _followup_by(superseded, superseded_event, (old_head.adjudication_id,)),
            "followup_adjudication_invalid"),
        "group_d_followup_current_head_valid": (
            _followup_by(superseded, superseded_event, (current_head.adjudication_id,)), None),
        "group_d_historical_late_reviewed_head_valid": (historical_chain, None),
        "group_d_historical_old_head_not_resolution_authority": (
            _resolved_by(historical_chain, historical_event, (historical_old.adjudication_id,)),
            "resolution_adjudication_invalid"),
        "group_d_historical_late_current_head_resolution_valid": (
            _resolved_by(historical_chain, historical_event, (historical_current.adjudication_id,)), None),
    }

    for kind in ("exchange_pairing", "corroboration", "issue_scope", "followup_continuity"):
        authority, event, decision = _evidence_only_authority(q, kind)
        cases[f"group_d_resolution_evidence_only_{kind}"] = (
            _resolved_by(authority, event, (decision.adjudication_id,)),
            "resolution_adjudication_invalid",
        )
        cases[f"group_d_followup_evidence_only_{kind}"] = (
            _followup_by(authority, event, (decision.adjudication_id,)),
            "followup_adjudication_invalid",
        )

    wrong_policy_bundle, wrong_policy_event, wrong_policy = _evidence_only_authority(
        q,
        "followup_continuity",
        policy_digest="sha256:" + "9" * 64,
    )
    cases["group_d_resolution_wrong_policy"] = (
        _resolved_by(wrong_policy_bundle, wrong_policy_event, (wrong_policy.adjudication_id,)),
        "resolution_adjudication_invalid",
    )
    cases["group_d_followup_wrong_policy"] = (
        _followup_by(wrong_policy_bundle, wrong_policy_event, (wrong_policy.adjudication_id,)),
        "followup_adjudication_invalid",
    )
    cases["group_d_followup_mixed_valid_evidence_only"] = (
        _followup_by(
            wrong_policy_bundle,
            wrong_policy_event,
            (head_a.adjudication_id, wrong_policy.adjudication_id),
        ),
        "followup_adjudication_invalid",
    )
    cases["group_d_resolution_semantic_precedes_shallow_time"] = (
        _resolved_by(
            wrong_policy_bundle,
            wrong_policy_event,
            (wrong_policy.adjudication_id,),
            known_at=dt.datetime(2026, 1, 1, tzinfo=UTC),
        ),
        "resolution_adjudication_invalid",
    )
    cases["group_d_followup_semantic_precedes_support_time"] = (
        _followup_by(
            wrong_policy_bundle,
            wrong_policy_event,
            (wrong_policy.adjudication_id,),
            known_at=dt.datetime(2026, 1, 1, tzinfo=UTC),
        ),
        "followup_adjudication_invalid",
    )
    return cases


#: DB-only: scenarios whose omitted rows already exist in the ledger (seeded by a prior valid
#: prepare), so the SQL-negative path reaches bond_credit_validate instead of an insert FK.
def v2_db_seeds() -> dict[str, c.CreditBundle]:
    q = build_bundle()
    return {"exchange_new_side_package_missing": exchange_bundle(q, new_doc=new_side_document(q))}


def _family_frames_from(q: c.CreditBundle, fr: dict) -> dict:  # type: ignore[type-arg]
    """Closure rebuilt honestly from ``fr`` (the builder's view of the inventory)."""
    return rebind_dependencies(fr, q.manifest["knowledge_cutoff"], q.manifest["knowledge_mode"])


def _with_filing(q: c.CreditBundle, cik: str, *, rebuild: bool, packages: tuple = (), drop_base: bool = False,  # type: ignore[type-arg]
                 **changes: object) -> c.CreditBundle:
    """Replace (or add, when ``changes`` has a new ``row_locator``) ``cik``'s N-CEN filing;
    ``drop_base`` removes the original copy so the new row is the only one."""
    base = next(f for f in q.frames["ncen_filings"] if f.registrant_cik == cik)  # type: ignore[attr-defined]
    values = {n: getattr(base, n) for n, _ in base.SPEC if n not in ("filing_evidence_id", "projection_digest")}
    values.update(changes)
    filing = c.NcenFilingEvidence.create(**values)
    replace = drop_base or filing.filing_evidence_id == base.filing_evidence_id
    filings = tuple(f for f in q.frames["ncen_filings"] if not (replace and f is base)) + (filing,)
    fr = {name: list(rows) for name, rows in q.frames.items()}
    fr["ncen_filings"] = list(filings)
    fr["source_packages"] = [*fr["source_packages"], *packages]
    if rebuild:
        fr = _family_frames_from(q, fr)
    fr.pop("publication_sources")
    return reassemble(q, frames=fr)


def _family_negatives(q: c.CreditBundle) -> dict[str, tuple[c.CreditBundle, str | None]]:
    """§9 items 2-3: persisted FE-1 contexts, memberships, filings and proposal closure."""
    ctx = next(iter(q.frames["family_contexts"]))
    member = min(q.frames["family_evidence"], key=lambda m: m.registrant_cik)  # type: ignore[attr-defined]
    proposal = next(iter(q.frames["proposal_evidence"]))
    header = next(p for p in q.frames["source_packages"] if p.source_family == "sec_ncen_acceptance_header")  # type: ignore[attr-defined]
    votes = [o for o in q.frames["observations"] if o.observation_id in ctx.vote_observation_ids]  # type: ignore[attr-defined]
    vote_time = max(o.public_available_at for o in votes)
    fn_11 = NCEN_REGISTRANTS["9999999911"][1]
    bridge = tuple(
        {"series_id": NCEN_REGISTRANTS["9999999913"][0], "role": role, "file_number_raw": fn, "crd_raw": None,
         "lei_raw": None}
        for role, fn in (("adviser", NCEN_REGISTRANTS["9999999913"][1]), ("sub_adviser", fn_11),
                         ("sub_adviser", NCEN_REGISTRANTS["9999999912"][1])))
    ncen_pkg = next(p for p in q.frames["source_packages"] if p.source_family == "sec_ncen_public_xml")  # type: ignore[attr-defined]
    dera_pkg = _package("sec_ncen_dera", "SYNTHETIC-NCEN-DERA-2026Q1", "public_government_record",
                        first_public=dt.datetime(2026, 4, 1, tzinfo=UTC), basis="archived_release_metadata")
    index_pkg = _package("sec_edgar_index", "SYNTHETIC-EDGAR-INDEX-2026Q2", "public_government_record",
                         first_public=dt.datetime(2026, 7, 2, tzinfo=UTC), basis="archived_release_metadata")
    amend_header = ncen_header("9999999901-26-000201", "20260401101500")
    selection, universe = "family_selection_invalid", "family_universe_not_closed"
    return {
        # Item 2: scalar/opaque closure and omitted rows.
        "proposal_forged_vote_time": (reassemble(q, frames={"proposal_evidence": (
            replace_row(proposal, evidence_known_at=vote_time),)}), "dependency_knowledge_mismatch"),
        "context_forged_vote_time": (reassemble(q, frames={"family_contexts": (
            replace_row(ctx, evidence_known_at=vote_time),)}), "dependency_knowledge_mismatch"),
        "membership_omitted": (reassemble(q, frames={"family_evidence": tuple(
            m for m in q.frames["family_evidence"] if m is not member)}), universe),
        "context_omitted": (reassemble(q, frames={"family_contexts": ()}), universe),
        "selected_filing_omitted": (reassemble(q, frames={"ncen_filings": tuple(
            f for f in q.frames["ncen_filings"] if f.filing_evidence_id != member.selected_filing_id)}), selection),  # type: ignore[attr-defined]
        "filing_header_omitted": (reassemble(q, frames={"source_packages": tuple(
            p for p in q.frames["source_packages"] if p is not header)}), "ncen_provenance_invalid"),
        "context_n_voter_removed": (_voter_subset(q), universe),
        # Item 3: bridges, stale/ambiguous selections and amendments.
        "bridge_stale_components": (_with_filing(q, "9999999913", rebuild=False, adviser_records=bridge),
                                    "family_component_mismatch"),
        "bridge_rebuilt_no_consensus": (_with_filing(q, "9999999913", rebuild=True, adviser_records=bridge),
                                        "proposal_evidence_not_closed"),
        "effective_filing_older_than_15_months": (_with_filing(
            q, "9999999912", rebuild=False, report_period_end=dt.date(2024, 11, 30)), selection),
        "same_accession_conflicting_copy": (_with_filing(
            q, "9999999911", rebuild=False, packages=(dera_pkg,), package_id=dera_pkg.package_id,
            row_locator="dera-row-1", family_answer="Y", family_name_raw="SYNTHETIC CONFLICTING FAMILY",
            public_time_basis="archived_release_metadata", public_available_at=dera_pkg.first_verified_public_at),
            selection),
        "newer_indexed_filing_not_acquired": (_with_filing(
            q, "9999999911", rebuild=False, packages=(index_pkg,), package_id=index_pkg.package_id,
            accession_number="9999999901-26-000301", row_locator="index-row-1", parse_status="index_only",
            form_type=None, report_period_end=None, header_package_id=None, acceptance_raw=None,
            acceptance_at=None, filing_date=dt.date(2026, 7, 1),
            public_time_basis="date_only_next_day_boundary",
            public_available_at=c.date_only_public_available_at(dt.date(2026, 7, 1), "America/New_York"),
            family_answer=None, reported_series_ids=(), adviser_records=(), underwriter_records=()), selection),
        "amendment_semantics_unknown": (_with_filing(
            q, "9999999911", rebuild=False, packages=(amend_header,), accession_number="9999999901-26-000201",
            row_locator="ncen-a-9999999911", form_type="N-CEN/A", header_package_id=amend_header.package_id,
            acceptance_raw="20260401101500", acceptance_at=c.edgar_acceptance_to_utc("20260401101500"),
            public_available_at=c.edgar_acceptance_to_utc("20260401101500"), filing_date=dt.date(2026, 4, 1)),
            selection),
        "ncen_header_wrong_family": (_with_filing(
            q, "9999999911", rebuild=False, package_id=ncen_pkg.package_id, row_locator="ncen-9999999911",
            header_package_id=q.frames["source_packages"][0].package_id), "ncen_provenance_invalid"),  # type: ignore[attr-defined]
    }


def _voter_subset(q: c.CreditBundle, removed: Any = None) -> c.CreditBundle:
    """A context/membership/proposal closure built without some voters (default: CUSIP C's N
    voter), published with the full observation inventory."""
    ctx = next(iter(q.frames["family_contexts"]))
    removed = removed or (lambda o: o.cusip9 == CUSIP_C)
    gone = [o for o in q.frames["observations"]  # type: ignore[attr-defined]
            if o.observation_id in ctx.vote_observation_ids and removed(o)]  # type: ignore[attr-defined]
    fr = {name: [r for r in rows if not any(r is g for g in gone)] for name, rows in q.frames.items()}
    fr = _family_frames_from(q, fr)
    fr["observations"] = list(q.frames["observations"])
    fr.pop("publication_sources")
    return reassemble(q, frames=fr)


def v2_regressions() -> dict[str, tuple[c.CreditBundle, str | None]]:
    """W0 amendment 1 (§9 items 1-3) negative controls shared by the in-memory and DB suites.

    Maps a scenario name to ``(bundle, expected reason family)``; ``None`` is valid.
    """
    q = build_bundle()
    return {**_exchange_negatives(q), **_exchange_item1_scenarios(q), **_family_negatives(q), **_item2_scenarios(q), **_item3_scenarios(q),
            **_item4_scenarios(q), **_item9_scenarios(q), **_group_c_scenarios(q)}


def row_refusals() -> dict[str, tuple[Any, str]]:
    """Row-contract refusals (raised while constructing the offending row)."""
    q = build_bundle()
    ev_a = next(ev for ev in q.frames["events"] if ev.primary_type == "payment_default")  # type: ignore[attr-defined]
    base = {n: getattr(ev_a, n) for n, _ in ev_a.SPEC}
    x = _pieces(q)
    pair_values = {n: getattr(x["acc_a"], n) for n, _ in c.Adjudication.SPEC if n != "adjudication_id"}
    ncen = next(iter(q.frames["ncen_filings"]))
    ncen_values = {n: getattr(ncen, n) for n, _ in c.NcenFilingEvidence.SPEC}
    return {
        "ncen_filing_identity_not_derived": (
            lambda: c.NcenFilingEvidence(**{**ncen_values, "filing_evidence_id": uuid.uuid4()}),
            "filing_evidence_id:not_derived"),
        "ncen_acceptance_not_converted_from_raw": (
            lambda: c.NcenFilingEvidence(**{**ncen_values, "acceptance_raw": "20261101013000"}),
            "acceptance_at:not_converted_from_raw"),
        "ncen_date_only_boundary_not_derived": (
            lambda: c.NcenFilingEvidence(**{
                **ncen_values,
                "filing_date": dt.date(2026, 7, 2),
                "public_time_basis": "date_only_next_day_boundary",
                "public_available_at": dt.datetime(2026, 7, 3, 5, 0, tzinfo=UTC),
            }),
            "public_available_at:not_date_only_boundary"),
        "distressed_exchange_without_relation": (
            lambda: c.DefaultEpisode(**{**base, "corroboration_flags": ("distressed_exchange", "edgar_document",
                                                                        "payment_default")}),
            "exchange_relation_ids:required_iff_distressed_exchange"),
        "alias_spell_without_relation": (
            lambda: c.DefaultEpisode(**{**base, "alias_spell_id": c.DefaultEpisode.derive_alias_spell_id(EPISODE_A)}),
            "alias_spell_id:requires_relation_and_derivation"),
        "consensus_flag_without_proposal": (
            lambda: c.DefaultEpisode(**{**base, "corroboration_flags": ("nport_consensus_state", "payment_default")}),
            "proposal_evidence_ids:required_iff_nport_consensus_state"),
        "accepted_evidence_by_policy_engine": (
            lambda: c.Adjudication.create(**{**pair_values, "subject_kind": "corroboration", "status": "accepted_evidence",
                                             "reviewer_role": "policy_rule_engine", "supersedes_adjudication_id": None,
                                             "support_valid_from": dt.date(2026, 5, 15)}),
            "status:"),
        "accepted_evidence_on_issue_episode": (
            lambda: c.Adjudication.create(**{**pair_values, "status": "accepted_evidence",
                                             "supersedes_adjudication_id": None}),
            "subject_kind:accepted_evidence_requires_evidence_subject"),
        "issue_scope_not_citing_its_link": (
            lambda: c.Adjudication.create(**{**pair_values, "subject_kind": "issue_scope", "subject_id": EPISODE_B,
                                             "status": "accepted_evidence", "supersedes_adjudication_id": None}),
            "issue_scope:subject_link_not_cited"),
        "corroboration_without_window": (
            lambda: c.Adjudication.create(**{**pair_values, "subject_kind": "corroboration", "status": "accepted_evidence",
                                             "supersedes_adjudication_id": None}),
            "support_valid_from:required_iff_windowed_accepted_evidence"),
        # §9 item 4 cyclic proposal/corroboration dependency: a corroboration decision cannot cite
        # the proposal that relies on it (only issue-episode decisions carry proposal edges).
        "corroboration_citing_a_proposal_cycle": (
            lambda: c.Adjudication.create(**{**pair_values, "subject_kind": "corroboration", "subject_id": CORROBORATION_B,
                                             "status": "accepted_evidence", "supersedes_adjudication_id": None,
                                             "support_valid_from": dt.date(2026, 3, 1),
                                             "proposal_evidence_ids": tuple(p.proposal_evidence_id
                                                                            for p in q.frames["proposal_evidence"])}),  # type: ignore[attr-defined]
            "proposal_evidence_ids:only_for_issue_episode"),
        "relation_self_link": (
            lambda: c.ExchangeRelation(
                relation_id=c.ExchangeRelation.derive_id(security_id(CUSIP_A), security_id(CUSIP_A), EPISODE_A),
                old_security_id=security_id(CUSIP_A), old_cusip9=CUSIP_A, new_security_id=security_id(CUSIP_A),
                new_cusip9=CUSIP_A, episode_id=EPISODE_A,
                alias_spell_id=c.DefaultEpisode.derive_alias_spell_id(EPISODE_A),
                old_link_id=x["link_a"].link_id, new_link_id=x["link_a"].link_id,
                exchange_document_observation_ids=(x["edgar_a"].observation_id,),
                pairing_adjudication_id=x["acc_a"].adjudication_id, exchange_effective_date=EXCHANGE_DATE,
                valid_from=EXCHANGE_DATE, valid_to=None, evidence_known_at=EXCHANGE_DOC_AT),
            "exchange_relation:self_link"),
    }


def render(bundle: c.CreditBundle) -> bytes:
    return (json.dumps(bundle.to_json_obj(), indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def write_fixtures(directory: Path = HERE) -> dict[str, str]:
    digests = {}
    for name, factory in FIXTURES.items():
        bundle = factory()
        (directory / name).write_bytes(render(bundle))
        digests[name] = bundle.bundle_digest()
    return digests


if __name__ == "__main__":
    for fixture, bundle_digest in write_fixtures().items():
        print(fixture, bundle_digest)
