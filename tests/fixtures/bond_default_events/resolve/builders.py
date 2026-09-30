"""SYNTHETIC builders for resolve.py tests (W2a). No real issuers, CUSIPs or filings."""

from __future__ import annotations

import datetime as dt
import importlib.util
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from src.bonds.default_events import contracts as c
from src.bonds.default_events import resolve as r

UTC = dt.timezone.utc
HERE = Path(__file__).resolve().parent


def _load_synthetic():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location("bond_default_synthetic_for_resolve", HERE.parent / "synthetic.py")
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


syn = _load_synthetic()
K = dt.datetime(2026, 9, 25, 0, 0, tzinfo=UTC)
RETRIEVED_AT = syn.RETRIEVED_AT
ADJ_AT = dt.datetime(2026, 9, 22, 12, 0, tzinfo=UTC)
LINK_AT = dt.datetime(2026, 9, 21, 11, 0, tzinfo=UTC)
cusip = syn.synthetic_cusip
security_id = syn.security_id


def package(family: str, external_id: str) -> c.SourcePackage:
    rights = {
        "sec_nport_dera": "public_government_record",
        "sec_edgar_document": "public_government_record",
        "issuer_public_document": "public_document_internal_use",
        "court_public_document": "public_document_internal_use",
    }.get(family, "internal_work_product")
    return syn._package(family, external_id, rights, first_public=RETRIEVED_AT, basis="internal_record")


NPORT = package("sec_nport_dera", "RESOLVE-NPORT")
EDGAR = package("sec_edgar_document", "RESOLVE-EDGAR")
ISSUER = package("issuer_public_document", "RESOLVE-ISSUER")
LINKS = package("link_batch", "RESOLVE-LINKS")
ADJS = package("adjudication_batch", "RESOLVE-ADJ")
PACKAGES = [NPORT, EDGAR, ISSUER, LINKS, ADJS]


def acceptance_for(day: dt.date) -> str:
    return f"{day:%Y%m%d}101500"


def nport(locator: str, cusip9: str, report_date: dt.date, flag: str, *, series: str = "S999999001",
          family: str = "SYN-FAMILY-1", cik: str = "9999999911", accepted: dt.date | None = None,
          arrears: str = "N", pik: str = "N", first_seen: dt.datetime = RETRIEVED_AT) -> c.CreditObservation:
    raw = acceptance_for(accepted or report_date + dt.timedelta(days=55))
    at = c.edgar_acceptance_to_utc(raw)
    return syn._observation(
        NPORT, locator, "nport_holding",
        accession_number=f"9999999901-26-{locator[-6:]}", holding_id=f"SYN-H-{locator}",
        cusip_raw=cusip9, cusip9=cusip9, security_id=security_id(cusip9), registrant_cik=cik, series_id=series,
        fund_family_id=family, issuer_type_raw="CORP", asset_category_raw="DBT", report_date=report_date,
        acceptance_raw=raw, acceptance_at=at, public_available_at=at, public_time_basis="edgar_acceptance_datetime",
        first_seen_at=max(first_seen, at),
        nport_is_default=flag, nport_arrears_or_deferral=arrears, nport_paid_in_kind=pik,
        field_presence={"nport_arrears_or_deferral": "present", "nport_is_default": "present",
                        "nport_paid_in_kind": "present"},
    )


def passage(locator: str, *, cusip9: str | None = None, effective: dt.date | None = None,
            lower: dt.date | None = None, upper: dt.date | None = None,
            public: dt.datetime = dt.datetime(2026, 5, 18, 20, 30, tzinfo=UTC),
            kind: str = "edgar_passage", revision: str = "original",
            supersedes: c.CreditObservation | None = None, document: str | None = None) -> c.CreditObservation:
    pkg = EDGAR if kind == "edgar_passage" else ISSUER
    precision = "day" if effective else ("interval" if upper else "unknown")
    return syn._observation(
        pkg, locator, kind, cusip_raw=cusip9, cusip9=cusip9, effective_date=effective, date_precision=precision,
        effective_lower_exclusive=lower, effective_upper_inclusive=upper,
        public_available_at=public, public_time_basis="first_verified_retrieval",
        first_seen_at=max(public, RETRIEVED_AT),
        document_quote=f"SYNTHETIC passage {locator}", document_location=f"doc.htm#{locator}",
        document_sha256=syn._sha(f"doc|{document or locator}"), revision_kind=revision,
        supersedes_observation_id=supersedes.observation_id if supersedes else None,
    )


def request(o: c.CreditObservation, cusip9: str, basis: str, *, scope: str = "issue",
            known: dt.datetime = LINK_AT, valid_from: dt.date = dt.date(2020, 1, 1)) -> r.LinkRequest:
    return r.LinkRequest(
        observation_id=o.observation_id, cusip9=cusip9, security_id=security_id(cusip9),
        obligor_id=f"SYN-OBLIGOR-{cusip9[:6]}", affected_scope=scope, basis=basis,
        identity_evidence_refs=(f"synthetic:indenture:{cusip9}",), identity_known_at=known,
        valid_from=valid_from, valid_to=None, rationale="SYNTHETIC request",
    )


def adjudicate(subject: uuid.UUID, status: str, evidence: list[c.CreditObservation], links: list[c.EventLink],
               *, role: str = "human_reviewer", at: dt.datetime = ADJ_AT, subject_kind: str = "issue_episode",
               supersedes: c.Adjudication | None = None, proposals: tuple[c.ProposalEvidence, ...] = (),
               support: tuple[dt.date | None, dt.date | None] = (None, None)) -> c.Adjudication:
    return syn._adjudication(ADJS, subject, status, role, at, evidence, links, subject_kind=subject_kind,
                             supersedes=supersedes, proposals=proposals, support=support)


def dependency_frames(
    *,
    source_packages: Iterable[c.SourcePackage] = PACKAGES,
    observations: Iterable[c.CreditObservation] = (),
    event_links: Iterable[c.EventLink] = (),
    adjudications: Iterable[c.Adjudication] = (),
    ncen_filings: Iterable[c.NcenFilingEvidence] = (),
    family_contexts: Iterable[c.FamilyContext] = (),
    family_evidence: Iterable[c.FamilyMembership] = (),
    proposal_evidence: Iterable[c.ProposalEvidence] = (),
    exchange_relations: Iterable[c.ExchangeRelation] = (),
) -> dict[str, tuple[Any, ...]]:
    """The complete nine-frame dependency inventory required by phase-1 construction."""
    return {
        "source_packages": tuple(source_packages),
        "observations": tuple(observations),
        "event_links": tuple(event_links),
        "adjudications": tuple(adjudications),
        "ncen_filings": tuple(ncen_filings),
        "family_contexts": tuple(family_contexts),
        "family_evidence": tuple(family_evidence),
        "proposal_evidence": tuple(proposal_evidence),
        "exchange_relations": tuple(exchange_relations),
    }


def episode_id(label: str) -> uuid.UUID:
    return c.uuid5_of("resolve_test_episode", label)
