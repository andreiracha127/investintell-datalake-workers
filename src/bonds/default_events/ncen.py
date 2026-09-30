"""SEC Form N-CEN fund-family independence evidence (plan v1_8 §4.1 item 6, amendment FE-1).

Sources are the DERA N-CEN data sets (quarterly TSV packages) and EDGAR N-CEN
``primary_doc.xml`` (the DERA packages exclude schema-3.1 filings, so both are read). Both
are projected into typed :class:`NcenFiling` records carrying only the family-relevant items:
B.5 family of investment companies, C.9 (and D.12) advisers and sub-advisers, current and
terminated, and B.16 principal underwriters.

Rule FE-1 (declared, fixed; never tuned after a consensus or held-out result):

* The **effective N-CEN** of a voting registrant for report date ``R`` at cutoff ``K`` is the
  latest original-or-amended N-CEN whose report period end is ``<= R`` and no more than 15
  months before ``R``, and that was public (EDGAR acceptance, or the conservative date-only
  boundary) by ``K``. Older or absent => no evidence.
* Two registrants are **independent families** only if both effective N-CENs answer B.5,
  their normalized family names differ (or at least one is not part of a family and they
  are different registrants), their adviser + sub-adviser sets (current and terminated) are
  disjoint by SEC file number, CRD and non-``N/A`` LEI, and their principal underwriters are
  disjoint by file number/CRD when both report one. Any missing, ``N/A``-only or unparseable
  element => not independent.
* Implementation: per ``(R, K)``, registrants with complete effective evidence are merged into
  **family components** by the transitive closure (union-find) of the not-independent
  relation; consensus counts distinct components. Registrants lacking complete evidence get
  no family identity and cannot supply independent support.

Coordinator clarification FE-1a/FE-1b (plan v1_8 §4.2 item 6):

* **FE-1a universe.** For each ``(R, K)`` the closure runs over every registrant that votes Y
  or N on ``R`` (any vote carrying a Y or N lot: value Y, N or disputed, public by K) in
  the **complete** N-PORT packages, and has complete effective evidence. The universe exists
  only as a sealed :class:`VoteInventory` built by :func:`build_vote_inventory`, which
  streams every observation of the packages (no CUSIP filter). The accepting path
  (:func:`build_consensus_with_ncen`) takes that inventory plus the target votes (the CUSIPs
  being decided), which must be a subset of the inventory; a caller-assembled iterable is
  never a universe. The per-state closure over the Y voters of one ``(cusip9, R)`` is a
  non-accepting sensitivity only (:func:`diagnostic_per_state_components`).
* **FE-1b series completeness.** A registrant's evidence for ``R`` is complete only if its
  effective N-CEN lists every series with which it votes (Y or N lot) on ``R``; a voting
  series absent from it, or a vote without a valid EDGAR series ID, leaves the registrant
  without family identity for that date. Several N-CENs are never unioned.

Selection and timing (review findings P1, versioned by :data:`RULE_VERSION`):

* Copies of one accession (DERA, EDGAR XML, several of each) are checked for identity,
  form, status and content agreement before a canonical copy is chosen; any conflict
  quarantines the accession, independent of input order. A copy's form type that differs
  from the EDGAR index/header form is a typed conflict; the only exception is DERA labelling
  an ``NT N-CEN`` accession as ``N-CEN`` (no XML copy), which is excluded as ``NT N-CEN``.
* The conservative date-only public time is an **admission bound** only. Competing filings
  of one registrant and period are ordered only by exact EDGAR acceptance times; if the order
  would depend on a date-only bound, an equal or an unknown time, or a filing whose admission
  by K is uncertain, the selection is ``selection_order_unresolved``. Accession order is
  never used.
* Every selection dependency (the selected filing, same-period competitors, blocking or
  uncertain filings) is recorded with its evidenced knowledge time; these enter the
  universe digest and ``known_at``. A dependency whose time cannot be established voids the
  report date's family evidence (``dependency_time_unknown``).
* ``knowledge_mode``: ``historical_reconstruction`` relies on proven historic public
  availability (retrieval time stays visible but is never historic possession);
  ``current_run`` additionally requires every relied filing (and any acceptance header used
  for ordering) to be retrieved by K.

Amendment semantics (:data:`AMENDMENT_SEMANTICS_VERSION`). ``N-CEN/A`` is classified
``complete_replacement`` only when its schema version is covered by the evidence below;
otherwise ``unknown`` (``partial`` is reserved), which blocks the registrant for that date
(``amendment_semantics_unknown``). An amendment is never merged with its original.

* Form N-CEN (SEC 2846 (8/22), https://www.sec.gov/files/formn-cen.pdf, sha256
  ``515949a4…f5a9``), General Instruction C.2: "A registrant that files an amendment to a
  previously filed report must provide information in response to all required items of
  Form N-CEN, regardless of why the amendment is filed."
* EDGAR Form N-CEN XML Technical Specification versions 2.5 (September 2020), 3.0 (March
  2025) and 3.1 (June 2025): Table 1-1 lists ``N-CEN/A`` ("Amendment to Annual Report") as a
  submission type of the same schema; §3.4 "Mapping of Form N-CEN Submission Schemas to
  Submission Type" gives identical N-CEN and N-CEN/A applicability for every element
  except ``headerData/accessionNumber`` (not applicable to N-CEN, mandatory for N-CEN/A);
  ``eis_NCEN_Filer.xsd`` has one content model for both types (no type-conditional
  construct) and no partial-amendment indicator (no ``amendmentType``-like element).
* Schema-version coverage: only ``X0505`` is declared by the specification's own samples
  (version 3.1, ``N-CEN_Sample_N5.xml``/``N6.xml``). Other schema versions, and amendments
  without an EDGAR XML copy (DERA does not carry the schema version), are ``unknown``.
  ``schema-valid`` rests on EDGAR acceptance plus this module's namespace/vocabulary checks;
  no local XSD validation is performed.

Declared residuals (not linked by this rule): one firm acting as adviser of one registrant
and as principal underwriter of another (adviser and underwriter identities are compared
only within their own role); common control among separately registered advisers (Form
ADV relationships are not observed).

Family evidence for consensus is built privately inside :func:`build_consensus_with_ncen` at
the inventory's ``K`` (W1's :class:`~.nport.FamilyEvidence` cannot enforce ``K``); the public
:func:`family_evidence_for` returns diagnostic membership data only.
"""

from __future__ import annotations

import csv
import dataclasses
import datetime as dt
import hashlib
import heapq
import io
import json
import re
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from . import nport
from .contracts import CreditObservation, date_only_public_available_at
from .nport import (
    ConsensusResult,
    FamilyEvidence,
    FilingFamily,
    IndependentCorroboration,
    PackageIntegrityError,
    Vote,
    VoteKey,
    XmlSafetyError,
    ZipLimits,
    ZipSafetyError,
    build_consensus_states,
    inspect_zip,
    is_series_key,
    parse_dera_date,
    safe_xml_root,
    sha256_file,
)
from .sec_acquisition import (
    ARCHIVES_ROOT,
    AcceptanceHeader,
    FormIndexEntry,
    SecClient,
    normalize_cik,
)

RULE_VERSION = "bond_default_ncen_family_fe1ab_v3"
AMENDMENT_SEMANTICS_VERSION = "ncen_amendment_semantics_v1"
AMENDMENT_COMPLETE = "complete_replacement"
AMENDMENT_PARTIAL = "partial"
AMENDMENT_UNKNOWN = "unknown"
#: Schema versions whose ``N-CEN/A`` semantics are covered by the evidence above.
AMENDMENT_COVERED_SCHEMA_VERSIONS = frozenset({"X0505"})
KNOWLEDGE_CURRENT_RUN = "current_run"
KNOWLEDGE_HISTORICAL = "historical_reconstruction"
KNOWLEDGE_MODES = (KNOWLEDGE_CURRENT_RUN, KNOWLEDGE_HISTORICAL)
_NPORT_REVISION_MODE = {KNOWLEDGE_CURRENT_RUN: "current_run", KNOWLEDGE_HISTORICAL: "historical"}
#: FE-1a/FE-1b: a registrant "votes Y or N" through any vote carrying a Y or N lot.
UNIVERSE_VOTE_VALUES = frozenset({"Y", "N", "disputed"})
INCOMPLETE_SERIES_ABSENT = "voting_series_absent_from_effective_ncen"
INCOMPLETE_NO_SERIES_ID = "voting_without_series_id"
INCOMPLETE_NO_VOTES = "no_voting_series"
UNACQUIRED_FILING = "filing_not_acquired"
ORDER_UNRESOLVED = "selection_order_unresolved"
AMENDMENT_UNKNOWN_REASON = "amendment_semantics_unknown"
AMENDMENT_PARTIAL_REASON = "amendment_partial_not_mergeable"
DEPENDENCY_TIME_UNKNOWN = "dependency_time_unknown"
DERA_PARSER_VERSION = "bond_default_ncen_dera_v1"
XML_PARSER_VERSION = "bond_default_ncen_xml_v1"
NCEN_NAMESPACE = "http://www.sec.gov/edgar/ncen"
EFFECTIVE_WINDOW_MONTHS = 15
ORIGINAL_FORM = "N-CEN"
AMENDMENT_FORM = "N-CEN/A"
NCEN_FORMS = frozenset({ORIGINAL_FORM, AMENDMENT_FORM})
NT_FORM = "NT N-CEN"
EDGAR_TIMEZONE = "America/New_York"

PINNED_TABLES = ("SUBMISSION", "REGISTRANT", "FUND_REPORTED_INFO", "ADVISER", "PRINCIPAL_UNDERWRITER")
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "SUBMISSION": ("ACCESSION_NUMBER", "SUBMISSION_TYPE", "CIK", "FILING_DATE", "REPORT_ENDING_PERIOD"),
    "REGISTRANT": ("ACCESSION_NUMBER", "CIK", "IS_FAMILY_INVESTMENT_COMPANY", "FAMILY_INVESTMENT_COMPANY_NAME"),
    "FUND_REPORTED_INFO": ("FUND_ID", "ACCESSION_NUMBER", "SERIES_ID"),
    "ADVISER": ("FUND_ID", "ADVISER_TYPE", "FILE_NUM", "CRD_NUM", "ADVISER_LEI"),
    "PRINCIPAL_UNDERWRITER": ("ACCESSION_NUMBER", "FILE_NUM", "CRD_NUM", "UNDERWRITER_LEI"),
}
#: DERA ``ADVISER_TYPE`` (the data spells "Advisor"; the readme "Adviser") -> role.
ADVISER_ROLES: dict[str, str] = {
    "ADVISOR": "adviser",
    "ADVISER": "adviser",
    "SUBADVISOR": "sub_adviser",
    "SUBADVISER": "sub_adviser",
    "TERMINATED ADVISOR": "terminated_adviser",
    "TERMINATED ADVISER": "terminated_adviser",
    "TERMINATED SUBADVISOR": "terminated_sub_adviser",
    "TERMINATED SUBADVISER": "terminated_sub_adviser",
}
CURRENT_PRIMARY_ROLE = "adviser"
#: Trailing generic tokens stripped (repeatedly) from the normalized B.5 family name.
FAMILY_SUFFIXES = ("FAMILY", "COMPLEX", "GROUP", "FUNDS", "FUND", "TRUST")
#: Explicit missing-value sentinels for the B.5 family name (compared upper-cased, whitespace
#: collapsed, before normalization). A blank value is missing as well.
MISSING_VALUE_SENTINELS = frozenset({
    "N/A", "N.A.", "N.A", "NA", "N / A", "NONE", "NULL", "NIL", "-", "--", "---", "0",
    "NOT APPLICABLE", "NOT AVAILABLE", "UNKNOWN", "TBD",
})

#: EDGAR XML: fund container -> (list element, item element, id element local names).
XML_ADVISER_GROUPS: tuple[tuple[str, str, str, tuple[str, str, str]], ...] = (
    ("adviser", "investmentAdvisers", "investmentAdviser",
     ("investmentAdviserFileNo", "investmentAdviserCrdNo", "investmentAdviserLei")),
    ("terminated_adviser", "investmentAdvisersTerminated", "investmentAdviserTerminated",
     ("investAdviserTerminatedFileNo", "investAdviserTerminatedCrdNo", "investAdviserTerminatedLei")),
    ("sub_adviser", "subAdvisers", "subAdviser",
     ("subAdviserFileNo", "subAdviserCrdNo", "subAdviserLei")),
    ("terminated_sub_adviser", "subAdvisersTerminated", "subAdviserTerminated",
     ("subAdviserTerminatedFileNo", "subAdviserTerminatedCrdNo", "subAdviserTerminatedLei")),
)
XML_UNDERWRITER_IDS = (
    "principalUnderwriterFileNumber", "principalUnderwriterCrdNumber", "principalUnderwriterLei",
)
#: Every adviser/underwriter element name observed in schema X0201/X0303/X0404/X0505 filings.
#: Any other name matching ``advis``/``underwrit`` (e.g. an unmapped D.12 layout) quarantines.
KNOWN_ADVISER_ELEMENTS = frozenset({
    "investmentAdvisers", "investmentAdviser", "investmentAdviserName", "investmentAdviserFileNo",
    "investmentAdviserCrdNo", "investmentAdviserLei", "investmentAdviserRssdId",
    "investmentAdviserStateCountry", "investmentAdviserCountry", "investmentAdviserHired",
    "investmentAdviserStartDate", "isInvestmentAdviserHired", "investmentAdvisersTerminated",
    "investmentAdviserTerminated", "investmentAdviserTerminatedName", "investAdviserTerminatedFileNo",
    "investAdviserTerminatedCrdNo", "investAdviserTerminatedLei", "investAdviserTerminatedRssdId",
    "investAdviserTerminationDate", "investmentAdviserTerminatedStateCountry",
    "investmentAdviserTerminatedCountry",
    "subAdvisers", "subAdviser", "subAdviserName", "subAdviserFileNo", "subAdviserCrdNo",
    "subAdviserLei", "subAdviserRssdId", "isSubAdviserAffiliated", "subAdviserStateCountry",
    "subAdviserCountry", "isSubAdviserHired", "subAdviserHired", "subAdvisersTerminated",
    "subAdviserTerminated", "subAdviserTerminatedName", "subAdviserTerminatedFileNo",
    "subAdviserTerminatedCrdNo", "subAdviserTerminatedLei", "subAdviserTerminatedRssdId",
    "subAdviserTerminatedCountry", "subAdviserTerminatedStateCountry", "subAdviserTerminationDate",
})
KNOWN_UNDERWRITER_ELEMENTS = frozenset({
    "principalUnderwriters", "principalUnderwriter", "principalUnderwriterName",
    "principalUnderwriterFileNumber", "principalUnderwriterCrdNumber", "principalUnderwriterLei",
    "principalUnderwriterRssdId", "principalUnderWriterStateCountry", "principalUnderWriterCountry",
    "isPrincipalUnderwriterAffiliatedWithRegistrant", "isUnderwriterHiredOrTerminated",
})
#: Relationship records, their containers and identifiers that must all be consumed.
XML_TRACKED_RELATIONSHIP_ELEMENTS = frozenset(
    {"principalUnderwriters", "principalUnderwriter", *XML_UNDERWRITER_IDS}
    | {name for group in XML_ADVISER_GROUPS for name in (group[1], group[2], *group[3])}
)
_ADVISER_LIKE = re.compile(r"advis", re.IGNORECASE)
_UNDERWRITER_LIKE = re.compile(r"underwrit", re.IGNORECASE)

_ACCESSION = re.compile(r"\d{10}-\d{2}-\d{6}")
_SERIES = re.compile(r"S\d{9}")
_FILE_NUMBER = re.compile(r"(\d{1,3})-(\d{1,9})")
_CRD = re.compile(r"\d{1,12}")
_LEI = re.compile(r"[A-Z0-9]{20}")
_NON_ALNUM = re.compile(r"[^A-Z0-9]")
_TSV_HEADER_MAX = 1024 * 1024

UTC = dt.timezone.utc

NCEN_ZIP_LIMITS = ZipLimits(
    max_members=128,
    max_member_bytes=2 * 1024**3,
    max_total_bytes=8 * 1024**3,
    max_compression_ratio=200.0,
    max_header_bytes=_TSV_HEADER_MAX,
)


class NcenError(RuntimeError):
    """Fatal N-CEN processing error (never used for data-quality quarantine)."""


# ---------------------------------------------------------------------------
# Lexical normalization
# ---------------------------------------------------------------------------
def _clean(raw: str | None) -> str | None:
    if raw is None:
        return None
    text = raw.strip()
    return text or None


def normalize_file_number(raw: str | None) -> str | None:
    """SEC file number ``NNN-N+`` with the numeric part canonicalized (``801-00856`` ==
    ``801-856``); ``N/A``, blanks, zero serials and anything else are ``None``."""
    text = _clean(raw)
    if text is None:
        return None
    match = _FILE_NUMBER.fullmatch(text.replace(" ", ""))
    if match is None or int(match.group(2)) == 0:
        return None
    return f"{int(match.group(1))}-{int(match.group(2))}"


def normalize_crd(raw: str | None) -> str | None:
    """CRD number without leading zeros; non-digit, blank or zero values are ``None``."""
    text = _clean(raw)
    if text is None or _CRD.fullmatch(text) is None or int(text) == 0:
        return None
    return str(int(text))


def normalize_lei(raw: str | None) -> str | None:
    """20-character LEI (upper case); ``N/A``, all-zero and malformed values are ``None``."""
    text = _clean(raw)
    if text is None:
        return None
    text = text.upper()
    if _LEI.fullmatch(text) is None or set(text) == {"0"}:
        return None
    return text


def is_missing_value_sentinel(raw: str | None) -> bool:
    """``True`` for a blank value or an explicit missing-value sentinel
    (:data:`MISSING_VALUE_SENTINELS`, compared upper-cased with collapsed whitespace).
    Checked *before* family-name normalization, which would turn ``N/A`` into ``NA``."""
    text = _clean(raw)
    return text is None or " ".join(text.upper().split()) in MISSING_VALUE_SENTINELS


def normalize_family_name(raw: str | None) -> str | None:
    """B.5 family key: upper case, drop every non-alphanumeric, then strip trailing
    ``FAMILY|COMPLEX|GROUP|FUNDS|FUND|TRUST`` repeatedly. Empty result => ``None``.
    Callers must reject :func:`is_missing_value_sentinel` values first."""
    text = _clean(raw)
    if text is None:
        return None
    key = _NON_ALNUM.sub("", text.upper())
    stripped = True
    while stripped and key:
        stripped = False
        for suffix in FAMILY_SUFFIXES:
            if key.endswith(suffix):
                key = key[: -len(suffix)]
                stripped = True
                break
    return key or None


def family_key_of(answer: str | None, raw: str | None) -> str | None:
    """Normalized B.5 key of a ``Y`` answer; sentinels and blanks have no key."""
    if answer != "Y" or is_missing_value_sentinel(raw):
        return None
    return normalize_family_name(raw)


def months_before(day: dt.date, months: int) -> dt.date:
    """Calendar date ``months`` months before ``day`` (day-of-month clamped)."""
    index = day.year * 12 + (day.month - 1) - months
    year, month = divmod(index, 12)
    month += 1
    if month == 12:
        last = 31
    else:
        last = (dt.date(year, month + 1, 1) - dt.timedelta(days=1)).day
    return dt.date(year, month, min(day.day, last))


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _ts(value: dt.datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# ---------------------------------------------------------------------------
# Typed records
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AdviserRecord:
    """One C.9/D.12 adviser row (``role``: adviser, sub_adviser, terminated_*)."""

    role: str
    file_number: str | None
    crd: str | None
    lei: str | None
    raw: tuple[str | None, str | None, str | None]

    @property
    def tokens(self) -> frozenset[str]:
        out = set()
        if self.file_number is not None:
            out.add(f"FN:{self.file_number}")
        if self.crd is not None:
            out.add(f"CRD:{self.crd}")
        if self.lei is not None:
            out.add(f"LEI:{self.lei}")
        return frozenset(out)

    def projection(self) -> list[str]:
        return [self.role, self.file_number or "", self.crd or "", self.lei or ""]

    def sort_key(self) -> tuple[str, ...]:
        return (*self.projection(), *(r or "" for r in self.raw))


@dataclass(frozen=True)
class UnderwriterRecord:
    """One B.16 principal underwriter row; disjointness uses file number and CRD only."""

    file_number: str | None
    crd: str | None
    lei: str | None
    raw: tuple[str | None, str | None, str | None]

    @property
    def tokens(self) -> frozenset[str]:
        out = set()
        if self.file_number is not None:
            out.add(f"FN:{self.file_number}")
        if self.crd is not None:
            out.add(f"CRD:{self.crd}")
        return frozenset(out)

    def projection(self) -> list[str]:
        return [self.file_number or "", self.crd or "", self.lei or ""]

    def sort_key(self) -> tuple[str, ...]:
        return (*self.projection(), *(r or "" for r in self.raw))


@dataclass(frozen=True)
class NcenFund:
    """One Part C fund of a filing; ``series_id`` is ``None`` for series-less funds."""

    series_id: str | None
    advisers: tuple[AdviserRecord, ...]

    def projection(self) -> list[Any]:
        return [self.series_id or "", sorted(a.projection() for a in self.advisers)]


@dataclass(frozen=True)
class NcenFiling:
    """Family-relevant projection of one N-CEN accession from one source (or merged).

    Times are kept apart by meaning:

    * ``public_available_at`` is the **admission bound**: the exact EDGAR acceptance
      (``public_time_basis`` = ``edgar_acceptance``) or the conservative next-day boundary
      of the filing date (``date_only_conservative``). It decides whether a filing may be
      considered at K, never the order of competing filings.
    * ``acceptance_at`` is the exact EDGAR acceptance time (header only); competing
      filings of one registrant and period are ordered by it alone.
    * ``data_known_at`` is when this data version was publicly available (historic):
      the filing itself for EDGAR XML, the DERA package's verified public time (or, as a
      conservative fallback, its retrieval time) for DERA copies.
    * ``retrieved_at`` is operational possession (retrieval/ingestion by this system);
      it is visible in every mode but only ``current_run`` treats it as possession.
    """

    accession_number: str
    registrant_cik: str | None
    form_type: str | None
    form_type_source: str
    report_period_end: dt.date | None
    filing_date: dt.date | None
    public_available_at: dt.datetime | None
    public_time_basis: str | None
    data_known_at: dt.datetime | None
    source: str
    source_refs: tuple[str, ...]
    family_answer: str | None
    family_name_raw: str | None
    funds: tuple[NcenFund, ...]
    underwriters: tuple[UnderwriterRecord, ...]
    status: str
    reasons: tuple[str, ...] = ()
    schema_version: str | None = None
    retrieved_at: dt.datetime | None = None
    acceptance_at: dt.datetime | None = None
    #: Conservative date-only admission bound (next America/New_York day of the filing date).
    public_date_bound: dt.datetime | None = None
    #: When the acceptance header was retrieved (``current_run`` may use ``acceptance_at``
    #: only if the header was held by K).
    header_retrieved_at: dt.datetime | None = None

    @property
    def usable(self) -> bool:
        return self.status == "parsed"

    @property
    def is_placeholder(self) -> bool:
        """Indexed by EDGAR but no content acquired."""
        return "filing_content_missing" in self.reasons

    def exact_acceptance(self, mode: str, cutoff: dt.datetime) -> dt.datetime | None:
        """Exact EDGAR acceptance usable as ordering evidence in ``mode`` at ``cutoff``."""
        if self.acceptance_at is None:
            return None
        if mode == KNOWLEDGE_CURRENT_RUN and (self.header_retrieved_at is None or self.header_retrieved_at > cutoff):
            return None
        return self.acceptance_at

    def admission_bound(self, mode: str, cutoff: dt.datetime) -> dt.datetime | None:
        """Time from which the filing is provably public (exact acceptance, else the
        conservative date-only bound); ``None`` = unknown."""
        return self.exact_acceptance(mode, cutoff) or self.public_date_bound

    def earliest_public(self, mode: str, cutoff: dt.datetime) -> dt.datetime | None:
        """Earliest time the filing could have been public (start of its filing date)."""
        exact = self.exact_acceptance(mode, cutoff)
        if exact is not None:
            return exact
        if self.filing_date is None:
            return None
        return date_only_public_available_at(self.filing_date - dt.timedelta(days=1), EDGAR_TIMEZONE)

    def visible(self, mode: str, cutoff: dt.datetime) -> bool:
        """Admitted at ``cutoff``: provably public, and in ``current_run`` also held."""
        bound = self.admission_bound(mode, cutoff)
        if bound is None or bound > cutoff:
            return False
        return mode != KNOWLEDGE_CURRENT_RUN or (self.retrieved_at is not None and self.retrieved_at <= cutoff)

    def data_available(self, mode: str, cutoff: dt.datetime) -> bool:
        """This data version was publicly available (and in ``current_run`` held) by K."""
        if self.data_known_at is None or self.data_known_at > cutoff:
            return False
        return mode != KNOWLEDGE_CURRENT_RUN or (self.retrieved_at is not None and self.retrieved_at <= cutoff)

    def projection(self) -> dict[str, Any]:
        """Source-independent family-relevant content (normalized values)."""
        return {
            "accession_number": self.accession_number,
            "registrant_cik": self.registrant_cik,
            "report_period_end": None if self.report_period_end is None else self.report_period_end.isoformat(),
            "family_answer": self.family_answer,
            "family_key": family_key_of(self.family_answer, self.family_name_raw),
            "funds": sorted(f.projection() for f in self.funds),
            "underwriters": sorted(u.projection() for u in self.underwriters),
        }

    @property
    def projection_digest(self) -> str:
        return _sha(self.projection())

    def with_updates(self, **changes: Any) -> NcenFiling:
        return dataclasses.replace(self, **changes)


def _quarantined(filing: NcenFiling, *reasons: str) -> NcenFiling:
    return filing.with_updates(status="quarantined", reasons=tuple(sorted(set(filing.reasons) | set(reasons))))


# ---------------------------------------------------------------------------
# DERA N-CEN data set packages
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class NcenPackageResult:
    package_label: str
    zip_sha256: str
    status: str
    reasons: tuple[str, ...]
    filings: tuple[NcenFiling, ...]
    stats: Mapping[str, Any]
    retrieved_at: dt.datetime
    first_verified_public_at: dt.datetime


def dera_ncen_official_url(package_label: str) -> str:
    """Official DERA URL; 2024q1-2025q2 packages carry the ``_ncen_0`` suffix."""
    match = re.fullmatch(r"(\d{4})q([1-4])", package_label)
    if match is None:
        raise NcenError(f"package_label_invalid:{package_label}")
    year, quarter = int(match.group(1)), int(match.group(2))
    suffix = "_ncen_0" if (2024, 1) <= (year, quarter) <= (2025, 2) else "_ncen"
    return f"https://www.sec.gov/files/dera/data/form-n-cen-data-sets/{package_label}{suffix}.zip"


def _member_for(members: Mapping[str, zipfile.ZipInfo], table: str) -> zipfile.ZipInfo | None:
    wanted = f"{table}.TSV"
    found = [info for name, info in members.items() if name.rsplit("/", 1)[-1].upper() == wanted]
    return found[0] if len(found) == 1 else None


def _read_tsv(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> tuple[list[str], Iterator[list[str]]]:
    """Header (bounded) and raw rows of one DERA TSV member (no quoting, tab-delimited)."""
    with archive.open(info) as probe:
        first = probe.readline(_TSV_HEADER_MAX + 1)
    if len(first) > _TSV_HEADER_MAX or not first.endswith(b"\n"):
        raise ZipSafetyError(f"tsv_header_unbounded:{info.filename}")
    header = first.decode("utf-8").rstrip("\n").rstrip("\r").split("\t")

    def rows() -> Iterator[list[str]]:
        with archive.open(info) as handle:
            text = io.TextIOWrapper(handle, encoding="utf-8", newline="")
            reader = csv.reader(text, delimiter="\t", quoting=csv.QUOTE_NONE)
            next(reader)
            yield from reader

    return header, rows()


def _dera_rows(
    archive: zipfile.ZipFile, info: zipfile.ZipInfo, table: str, reasons: list[str]
) -> list[dict[str, str | None]] | None:
    header, rows = _read_tsv(archive, info)
    missing = [c for c in REQUIRED_COLUMNS[table] if c not in header]
    if missing:
        reasons.append(f"required_columns_missing:{table}:{','.join(missing)}")
        return None
    if len(set(header)) != len(header):
        reasons.append(f"duplicate_columns:{table}")
        return None
    out: list[dict[str, str | None]] = []
    for number, row in enumerate(rows, start=2):
        if len(row) != len(header):
            reasons.append(f"row_width_mismatch:{table}:{number}")
            return None
        out.append({name: _clean(value) for name, value in zip(header, row, strict=True)})
    return out


def parse_dera_ncen_package(
    zip_path: Path | str,
    *,
    expected_sha256: str,
    package_label: str,
    retrieved_at: dt.datetime,
    first_verified_public_at: dt.datetime | None = None,
    limits: ZipLimits | None = None,
) -> NcenPackageResult:
    """Parse one DERA N-CEN package into per-accession :class:`NcenFiling` records.

    The ZIP is hashed before it is opened (mismatch raises). A missing pinned table,
    missing required column, duplicate header or wrong-width row quarantines the whole
    package (no filings). Accession-level defects (CIK disagreement, duplicate registrant
    rows, unknown adviser type, orphan rows, unparseable dates) quarantine that accession.
    ``public_available_at`` is the conservative date-only boundary of ``FILING_DATE``
    (DERA has no acceptance time); ``data_known_at`` additionally waits for the package
    (``first_verified_public_at`` or, absent that, ``retrieved_at``).
    """
    limits = limits or NCEN_ZIP_LIMITS
    zip_path = Path(zip_path)
    zip_sha = sha256_file(zip_path)
    if zip_sha != expected_sha256:
        raise PackageIntegrityError(f"package_sha256_mismatch:{package_label}:{zip_sha}")
    retrieved = retrieved_at.astimezone(UTC)
    first_public = (first_verified_public_at or retrieved_at).astimezone(UTC)
    reasons: list[str] = []
    tables: dict[str, list[dict[str, str | None]]] = {}
    try:
        with zipfile.ZipFile(zip_path) as archive:
            members = inspect_zip(archive, limits)
            for table in PINNED_TABLES:
                info = _member_for(members, table)
                if info is None:
                    reasons.append(f"pinned_table_missing:{table}")
                    continue
                parsed = _dera_rows(archive, info, table, reasons)
                if parsed is not None:
                    tables[table] = parsed
    except zipfile.BadZipFile as exc:
        raise ZipSafetyError(f"zip_invalid:{package_label}") from exc
    except (UnicodeDecodeError, csv.Error) as exc:
        reasons.append(f"tsv_unparseable:{type(exc).__name__}")
    stats: dict[str, Any] = {"package_label": package_label}
    if reasons:
        return NcenPackageResult(package_label, zip_sha, "quarantined", tuple(sorted(reasons)), (), stats,
                                 retrieved, first_public)
    filings = _dera_filings(tables, package_label=package_label, zip_sha=zip_sha, known_at=first_public,
                            retrieved_at=retrieved, stats=stats)
    return NcenPackageResult(package_label, zip_sha, "parsed", (), filings, dict(sorted(stats.items())),
                             retrieved, first_public)


def _dera_filings(
    tables: Mapping[str, list[dict[str, str | None]]],
    *,
    package_label: str,
    zip_sha: str,
    known_at: dt.datetime,
    retrieved_at: dt.datetime,
    stats: dict[str, Any],
) -> tuple[NcenFiling, ...]:
    counts: Counter[str] = Counter()
    bad: dict[str, set[str]] = defaultdict(set)
    submissions: dict[str, dict[str, str | None]] = {}
    for row in tables["SUBMISSION"]:
        accession = row["ACCESSION_NUMBER"] or ""
        if not _ACCESSION.fullmatch(accession):
            counts["submission_accession_invalid"] += 1
            continue
        if accession in submissions:
            bad[accession].add("submission_duplicate")
        submissions[accession] = row
    registrants: dict[str, dict[str, str | None]] = {}
    for row in tables["REGISTRANT"]:
        accession = row["ACCESSION_NUMBER"] or ""
        if accession not in submissions:
            counts["registrant_orphan"] += 1
            continue
        if accession in registrants:
            bad[accession].add("registrant_duplicate")
        registrants[accession] = row
    fund_rows: dict[str, dict[str, str | None]] = {}
    funds_by_accession: dict[str, list[str]] = defaultdict(list)
    for row in tables["FUND_REPORTED_INFO"]:
        accession = row["ACCESSION_NUMBER"] or ""
        fund_id = row["FUND_ID"] or ""
        if accession not in submissions or not fund_id:
            counts["fund_orphan"] += 1
            continue
        if fund_id in fund_rows:
            bad[accession].add("fund_id_duplicate")
            continue
        fund_rows[fund_id] = row
        funds_by_accession[accession].append(fund_id)
    advisers: dict[str, list[AdviserRecord]] = defaultdict(list)
    for row in tables["ADVISER"]:
        fund_id = row["FUND_ID"] or ""
        fund = fund_rows.get(fund_id)
        if fund is None:
            counts["adviser_orphan"] += 1
            prefix = fund_id.split("_", 1)[0]
            if prefix in submissions:
                bad[prefix].add("adviser_orphan")
            continue
        role = ADVISER_ROLES.get((row["ADVISER_TYPE"] or "").upper())
        if role is None:
            bad[fund["ACCESSION_NUMBER"] or ""].add("adviser_type_unknown")
            continue
        raw = (row["FILE_NUM"], row["CRD_NUM"], row["ADVISER_LEI"])
        advisers[fund_id].append(AdviserRecord(
            role=role, file_number=normalize_file_number(raw[0]), crd=normalize_crd(raw[1]),
            lei=normalize_lei(raw[2]), raw=raw))
    underwriters: dict[str, list[UnderwriterRecord]] = defaultdict(list)
    for row in tables["PRINCIPAL_UNDERWRITER"]:
        accession = row["ACCESSION_NUMBER"] or ""
        if accession not in submissions:
            counts["underwriter_orphan"] += 1
            continue
        raw = (row["FILE_NUM"], row["CRD_NUM"], row["UNDERWRITER_LEI"])
        underwriters[accession].append(UnderwriterRecord(
            file_number=normalize_file_number(raw[0]), crd=normalize_crd(raw[1]), lei=normalize_lei(raw[2]),
            raw=raw))

    out: list[NcenFiling] = []
    for accession in sorted(submissions):
        sub = submissions[accession]
        reg = registrants.get(accession)
        reasons = set(bad.get(accession, ()))
        cik = normalize_cik(sub["CIK"])
        if cik is None:
            reasons.add("cik_invalid")
        if reg is None:
            reasons.add("registrant_missing")
        elif normalize_cik(reg["CIK"]) != cik:
            reasons.add("registrant_cik_mismatch")
        period = parse_dera_date(sub["REPORT_ENDING_PERIOD"])
        filed = parse_dera_date(sub["FILING_DATE"])
        if period is None:
            reasons.add("report_period_unparseable")
        if filed is None:
            reasons.add("filing_date_unparseable")
        answer_raw = (reg or {}).get("IS_FAMILY_INVESTMENT_COMPANY")
        answer = answer_raw.upper() if answer_raw else None
        if answer not in (None, "Y", "N"):
            reasons.add("family_answer_unparseable")
            answer = None
        funds = []
        for fund_id in sorted(funds_by_accession.get(accession, ())):
            series = fund_rows[fund_id]["SERIES_ID"]
            if series is not None and _SERIES.fullmatch(series) is None:
                reasons.add("series_id_invalid")
                series = None
            funds.append(NcenFund(series_id=series, advisers=tuple(
                sorted(advisers.get(fund_id, ()), key=AdviserRecord.sort_key))))
        public = None if filed is None else date_only_public_available_at(filed, EDGAR_TIMEZONE)
        filing = NcenFiling(
            accession_number=accession, registrant_cik=cik, form_type=sub["SUBMISSION_TYPE"],
            form_type_source="dera", report_period_end=period, filing_date=filed,
            public_available_at=public, public_time_basis=None if public is None else "date_only_conservative",
            data_known_at=None if public is None else max(public, known_at), source="dera",
            source_refs=(f"dera_ncen:{package_label}:sha256:{zip_sha}",),
            family_answer=answer, family_name_raw=(reg or {}).get("FAMILY_INVESTMENT_COMPANY_NAME"),
            funds=tuple(funds),
            underwriters=tuple(sorted(underwriters.get(accession, ()), key=UnderwriterRecord.sort_key)),
            status="parsed", schema_version=DERA_PARSER_VERSION, retrieved_at=retrieved_at,
        )
        if reasons:
            filing = _quarantined(filing, *reasons)
            counts["accessions_quarantined"] += 1
        out.append(filing)
        counts[f"form_{sub['SUBMISSION_TYPE']}"] += 1
    stats.update({"accessions": len(out), **dict(counts)})
    return tuple(out)


# ---------------------------------------------------------------------------
# EDGAR primary_doc.xml
# ---------------------------------------------------------------------------
def primary_doc_url(cik: str, accession_number: str) -> str:
    if not _ACCESSION.fullmatch(accession_number):
        raise NcenError(f"accession_invalid:{accession_number}")
    padded = normalize_cik(cik)
    if padded is None:
        raise NcenError(f"cik_invalid:{cik}")
    return f"{ARCHIVES_ROOT}/data/{int(padded)}/{accession_number.replace('-', '')}/primary_doc.xml"


class _XmlQuarantine(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _split(tag: str) -> tuple[str | None, str]:
    if tag.startswith("{"):
        namespace, _, local = tag[1:].partition("}")
        return namespace, local
    return None, tag


def _kids(element: ElementTree.Element, name: str) -> list[ElementTree.Element]:
    """Children with local name ``name``; any such child outside the N-CEN namespace
    quarantines the filing (a foreign-namespace look-alike is never read)."""
    found = []
    for child in element:
        if not isinstance(child.tag, str):
            continue
        namespace, local = _split(child.tag)
        if local != name:
            continue
        if namespace != NCEN_NAMESPACE:
            raise _XmlQuarantine(f"xml_namespace_mismatch:{name}")
        found.append(child)
    return found


def _kid(element: ElementTree.Element | None, name: str) -> ElementTree.Element | None:
    if element is None:
        return None
    found = _kids(element, name)
    if len(found) > 1:
        raise _XmlQuarantine(f"xml_element_repeated:{name}")
    return found[0] if found else None


def _text(element: ElementTree.Element | None, name: str) -> str | None:
    child = _kid(element, name)
    return None if child is None else _clean(child.text)


def _check_vocabulary(root: ElementTree.Element) -> None:
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue
        namespace, local = _split(element.tag)
        if _ADVISER_LIKE.search(local) and local not in KNOWN_ADVISER_ELEMENTS:
            raise _XmlQuarantine(f"xml_unmapped_adviser_element:{local}")
        if _UNDERWRITER_LIKE.search(local) and local not in KNOWN_UNDERWRITER_ELEMENTS:
            raise _XmlQuarantine(f"xml_unmapped_underwriter_element:{local}")
        if (local in KNOWN_ADVISER_ELEMENTS or local in KNOWN_UNDERWRITER_ELEMENTS) and namespace != NCEN_NAMESPACE:
            raise _XmlQuarantine(f"xml_namespace_mismatch:{local}")
        for attribute in element.attrib:
            attr_namespace, attr_local = _split(attribute)
            if attr_local in ("isRegistrantFamilyInvComp", "familyInvCompFullName", "reportEndingPeriod") and (
                attr_namespace is not None
            ):
                raise _XmlQuarantine(f"xml_attribute_namespace_mismatch:{attr_local}")


def _xml_family(registrant: ElementTree.Element) -> tuple[str | None, str | None]:
    """(answer, raw name) from ``registrantFamilyInvComp`` or the bare N element."""
    compound = _kid(registrant, "registrantFamilyInvComp")
    bare = _kid(registrant, "isRegistrantFamilyInvComp")
    answers: list[tuple[str | None, str | None]] = []
    if compound is not None:
        answers.append((_clean(compound.get("isRegistrantFamilyInvComp")), _clean(compound.get("familyInvCompFullName"))))
    if bare is not None:
        answers.append((_clean(bare.text), None))
    if not answers:
        return None, None
    if len(answers) > 1:
        raise _XmlQuarantine("xml_family_answer_repeated")
    answer, name = answers[0]
    answer = answer.upper() if answer else None
    if answer not in (None, "Y", "N"):
        raise _XmlQuarantine("xml_family_answer_unparseable")
    return answer, name


def parse_ncen_primary_doc(
    data: bytes,
    *,
    accession_number: str,
    source_url: str,
    retrieved_at: dt.datetime,
) -> NcenFiling:
    """Parse an EDGAR N-CEN ``primary_doc.xml`` into an :class:`NcenFiling`.

    XML safety is W1's :func:`~.nport.safe_xml_root` (UTF-8 only, DTD/entities refused,
    bounded size); every mapped element must be in the official N-CEN namespace and any
    adviser/underwriter element outside the known vocabulary quarantines the filing. The
    filing date/public time come from the EDGAR index or acceptance header at merge time.
    """
    if not _ACCESSION.fullmatch(accession_number):
        raise NcenError(f"accession_invalid:{accession_number}")
    digest = hashlib.sha256(data).hexdigest()
    base = NcenFiling(
        accession_number=accession_number, registrant_cik=None, form_type=None, form_type_source="edgar_xml",
        report_period_end=None, filing_date=None, public_available_at=None, public_time_basis=None,
        data_known_at=None, source="edgar_xml", source_refs=(f"edgar_xml:{source_url}:sha256:{digest}",),
        family_answer=None, family_name_raw=None, funds=(), underwriters=(), status="parsed",
        retrieved_at=retrieved_at.astimezone(UTC),
    )
    try:
        root = safe_xml_root(data)
    except XmlSafetyError as exc:
        return _quarantined(base, f"xml_unsafe:{str(exc).split(':', 1)[0]}")
    try:
        return _parse_xml_tree(root, base)
    except _XmlQuarantine as exc:
        return _quarantined(base, exc.reason)


def _parse_xml_tree(root: ElementTree.Element, base: NcenFiling) -> NcenFiling:
    namespace, local = _split(root.tag)
    if local != "edgarSubmission" or namespace != NCEN_NAMESPACE:
        raise _XmlQuarantine("xml_root_not_ncen")
    _check_vocabulary(root)
    reasons: set[str] = set()
    schema = _text(root, "schemaVersion")
    header = _kid(root, "headerData")
    form_type = _text(header, "submissionType")
    filer_cik = normalize_cik(
        _text(_kid(_kid(_kid(header, "filerInfo"), "filer"), "issuerCredentials"), "cik"))
    form = _kid(root, "formData")
    if form is None:
        raise _XmlQuarantine("xml_form_data_missing")
    general = _kid(form, "generalInfo")
    period_raw = None if general is None else _clean(general.get("reportEndingPeriod"))
    try:
        period = None if period_raw is None else dt.date.fromisoformat(period_raw)
    except ValueError:
        period = None
    if period is None:
        reasons.add("report_period_unparseable")
    registrant = _kid(form, "registrantInfo")
    if registrant is None:
        raise _XmlQuarantine("xml_registrant_info_missing")
    registrant_cik = normalize_cik(_text(registrant, "registrantCik"))
    cik = registrant_cik or filer_cik
    if cik is None:
        reasons.add("cik_invalid")
    elif filer_cik is not None and registrant_cik is not None and filer_cik != registrant_cik:
        reasons.add("registrant_cik_mismatch")
    answer, name = _xml_family(registrant)
    consumed: set[int] = set()

    def take(parent: ElementTree.Element, name_: str) -> str | None:
        child = _kid(parent, name_)
        if child is None:
            return None
        consumed.add(id(child))
        return _clean(child.text)

    underwriters = []
    uw_list = _kid(registrant, "principalUnderwriters")
    if uw_list is not None:
        consumed.add(id(uw_list))
    for item in [] if uw_list is None else _kids(uw_list, "principalUnderwriter"):
        consumed.add(id(item))
        raw = tuple(take(item, name_) for name_ in XML_UNDERWRITER_IDS)
        underwriters.append(UnderwriterRecord(
            file_number=normalize_file_number(raw[0]), crd=normalize_crd(raw[1]), lei=normalize_lei(raw[2]),
            raw=(raw[0], raw[1], raw[2])))
    funds = []
    series_info = _kid(form, "managementInvestmentQuestionSeriesInfo")
    for question in [] if series_info is None else _kids(series_info, "managementInvestmentQuestion"):
        series = _text(question, "mgmtInvSeriesId")
        if series is not None and _SERIES.fullmatch(series) is None:
            reasons.add("series_id_invalid")
            series = None
        advisers = []
        for role, list_name, item_name, id_names in XML_ADVISER_GROUPS:
            container = _kid(question, list_name)
            if container is not None:
                consumed.add(id(container))
            for item in [] if container is None else _kids(container, item_name):
                consumed.add(id(item))
                raw = tuple(take(item, name_) for name_ in id_names)
                advisers.append(AdviserRecord(
                    role=role, file_number=normalize_file_number(raw[0]), crd=normalize_crd(raw[1]),
                    lei=normalize_lei(raw[2]), raw=(raw[0], raw[1], raw[2])))
        funds.append(NcenFund(series_id=series, advisers=tuple(
            sorted(advisers, key=AdviserRecord.sort_key))))
    # Every relationship record and identifier present must have been read at its mapped
    # place; one under an unmapped wrapper (or misplaced) quarantines the filing.
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue
        local = _split(element.tag)[1]
        if local in XML_TRACKED_RELATIONSHIP_ELEMENTS and id(element) not in consumed:
            raise _XmlQuarantine(f"xml_relationship_node_unconsumed:{local}")
    filing = base.with_updates(
        registrant_cik=cik, form_type=form_type, report_period_end=period, family_answer=answer,
        family_name_raw=name, funds=tuple(sorted(funds, key=lambda f: _canonical(f.projection()))),
        underwriters=tuple(sorted(underwriters, key=UnderwriterRecord.sort_key)),
        schema_version=schema,
    )
    return _quarantined(filing, *reasons) if reasons else filing


def fetch_primary_doc(client: SecClient, cik: str, accession_number: str) -> NcenFiling:
    """Fetch (via the fair-access, cache-locked :class:`SecClient`) and parse one filing."""
    url = primary_doc_url(cik, accession_number)
    fetched = client.get(url)
    return parse_ncen_primary_doc(fetched.body, accession_number=accession_number, source_url=url,
                                  retrieved_at=fetched.fetched_at)


# ---------------------------------------------------------------------------
# Merge sources into one filing per accession
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class NcenFilingIndex:
    """Merged filings by registrant CIK (sorted), plus accessions excluded from selection."""

    by_cik: Mapping[str, tuple[NcenFiling, ...]]
    excluded: Mapping[str, str]
    stats: Mapping[str, Any]

    def filings(self) -> Iterator[NcenFiling]:
        for cik in sorted(self.by_cik):
            yield from self.by_cik[cik]


def merge_filings(
    filings: Iterable[NcenFiling],
    *,
    index_entries: Iterable[FormIndexEntry] = (),
    headers: Mapping[str, AcceptanceHeader] | None = None,
    index_retrieved_at: dt.datetime | None = None,
) -> NcenFilingIndex:
    """One :class:`NcenFiling` per accession from every DERA and EDGAR XML copy.

    All copies are validated together (status, registrant, form, filing date, schema
    version, family-relevant content) before a canonical copy is chosen, so the result does
    not depend on input order; any disagreement quarantines the accession. Form type and
    time come from the acceptance header when supplied, else the EDGAR form index, else the
    copies; a copy form that differs from the header/index form is ``form_type_conflict``,
    except an ``NT N-CEN`` accession that only DERA labels ``N-CEN`` (excluded as NT).
    An indexed N-CEN without any acquired copy becomes an unusable placeholder (it blocks
    selection instead of letting an older filing through). ``index_retrieved_at`` is when the
    index files were held (placeholder possession in ``current_run``).
    """
    headers = dict(headers or {})
    index: dict[str, set[FormIndexEntry]] = defaultdict(set)
    counts: Counter[str] = Counter()
    for entry in index_entries:
        index[entry.accession_number].add(entry)
    copies: dict[str, list[NcenFiling]] = defaultdict(list)
    for filing in filings:
        copies[filing.accession_number].append(filing)
    retrieved_index = None if index_retrieved_at is None else index_retrieved_at.astimezone(UTC)
    for accession, entries in index.items():
        if accession in copies or not any(e.form_type in NCEN_FORMS for e in entries):
            continue
        ciks = {e.cik for e in entries}
        copies[accession].append(NcenFiling(
            accession_number=accession, registrant_cik=next(iter(ciks)) if len(ciks) == 1 else None,
            form_type=None, form_type_source="edgar_index", report_period_end=None, filing_date=None,
            public_available_at=None, public_time_basis=None, data_known_at=None, source="edgar_index",
            source_refs=(f"edgar_index:{accession}",), family_answer=None, family_name_raw=None, funds=(),
            underwriters=(), status="quarantined", reasons=("filing_content_missing",),
            retrieved_at=retrieved_index,
        ))
        counts["index_placeholders"] += 1
    by_cik: dict[str, list[NcenFiling]] = defaultdict(list)
    excluded: dict[str, str] = {}
    for accession in sorted(copies):
        merged = _merge_copies(copies[accession], counts)
        merged = _apply_time(merged, copies[accession], tuple(sorted(index.get(accession, ()), key=repr)),
                             headers.get(accession), counts)
        if merged.form_type == NT_FORM:
            excluded[accession] = f"form_not_ncen:{merged.form_type}"
            counts["excluded_not_ncen"] += 1
            continue
        if merged.form_type not in NCEN_FORMS and merged.usable:
            excluded[accession] = f"form_not_ncen:{merged.form_type}"
            counts["excluded_not_ncen"] += 1
            continue
        if "copy_identity_conflict" in merged.reasons:
            # Fail closed: the conflicting accession blocks every registrant it may belong to.
            for claimed in sorted({c.registrant_cik for c in copies[accession] if c.registrant_cik is not None}):
                by_cik[claimed].append(merged.with_updates(registrant_cik=claimed))
            counts["merged_identity_conflict"] += 1
            continue
        if merged.registrant_cik is None:
            excluded[accession] = "cik_unknown"
            counts["excluded_cik_unknown"] += 1
            continue
        by_cik[merged.registrant_cik].append(merged)
        counts[f"merged_{merged.status}"] += 1
    ordered = {
        cik: tuple(sorted(items, key=lambda f: (f.report_period_end or dt.date.min, f.accession_number)))
        for cik, items in sorted(by_cik.items())
    }
    return NcenFilingIndex(ordered, dict(sorted(excluded.items())), dict(sorted(counts.items())))


def _copy_sort_key(copy: NcenFiling) -> tuple[Any, ...]:
    rank = {"edgar_xml": 0, "dera": 1}.get(copy.source, 2)
    return (rank, 0 if copy.usable else 1, copy.projection_digest, copy.source_refs, copy.reasons,
            copy.form_type or "", copy.registrant_cik or "")


def _merge_copies(copies: Sequence[NcenFiling], counts: Counter[str]) -> NcenFiling:
    ordered = sorted(copies, key=_copy_sort_key)
    usable = [c for c in ordered if c.usable]
    reasons: set[str] = set()
    for copy in ordered:
        reasons |= set(copy.reasons)
    if len(ordered) > 1 and len(usable) < len(ordered):
        reasons.add("source_copy_quarantined")
    ciks = {c.registrant_cik for c in ordered if c.registrant_cik is not None}
    forms = {c.form_type for c in ordered if c.form_type is not None}
    filed = {c.filing_date for c in ordered if c.filing_date is not None}
    schemas = {c.schema_version for c in ordered if c.source == "edgar_xml" and c.schema_version is not None}
    if len(ciks) > 1:
        reasons.add("copy_identity_conflict")
    if len(forms) > 1:
        reasons.add("copy_form_type_conflict")
    if len(filed) > 1:
        reasons.add("copy_filing_date_conflict")
    if len(schemas) > 1:
        reasons.add("copy_schema_version_conflict")
    if len({c.projection_digest for c in usable}) > 1:
        reasons.add("copy_projection_conflict")
    canonical = (usable or ordered)[0]
    if len(ordered) > 1:
        counts["accessions_with_multiple_copies"] += 1
    known = [c.data_known_at for c in ordered if c.data_known_at is not None]
    retrieved = [c.retrieved_at for c in ordered if c.retrieved_at is not None]
    merged = canonical.with_updates(
        source="+".join(sorted({c.source for c in ordered})),
        source_refs=tuple(sorted({r for c in ordered for r in c.source_refs})),
        registrant_cik=next(iter(ciks)) if len(ciks) == 1 else None,
        form_type=next(iter(forms)) if len(forms) == 1 else None,
        filing_date=next(iter(filed)) if len(filed) == 1 else None,
        schema_version=next(iter(schemas)) if len(schemas) == 1 else (None if schemas else canonical.schema_version),
        # Equal content: this data version was available from its earliest attested copy.
        data_known_at=min(known) if known else None,
        retrieved_at=min(retrieved) if retrieved else None,
        reasons=tuple(sorted(reasons)),
    )
    if reasons and merged.status == "parsed":
        counts["copy_conflicts"] += 1
        return _quarantined(merged, *reasons)
    return merged


def _apply_time(
    filing: NcenFiling,
    copies: Sequence[NcenFiling],
    entries: Sequence[FormIndexEntry],
    header: AcceptanceHeader | None,
    counts: Counter[str],
) -> NcenFiling:
    reasons: set[str] = set()
    has_xml = any(c.source == "edgar_xml" for c in copies)
    copy_forms = {c.form_type for c in copies if c.form_type is not None}
    dera_forms = {c.form_type for c in copies if c.source == "dera" and c.form_type is not None}
    filed = filing.filing_date
    if filing.registrant_cik is None and len({e.cik for e in entries}) == 1:
        # An unparseable copy still blocks its registrant (placed via the index CIK).
        filing = filing.with_updates(registrant_cik=entries[0].cik)
    matching = [e for e in entries if filing.registrant_cik is None or e.cik == filing.registrant_cik]
    if entries and not matching:
        reasons.add("index_cik_mismatch")
    authority: dict[str, set[str]] = defaultdict(set)
    index_dates = {e.date_filed for e in matching}
    for entry in matching:
        authority[entry.form_type].add("edgar_index")
    if len(index_dates) > 1:
        reasons.add("index_entry_conflict")
    elif index_dates:
        index_date = next(iter(index_dates))
        if filed is not None and index_date != filed:
            reasons.add("index_filing_date_mismatch")
        filed = index_date
    acceptance = None
    if header is not None:
        if header.accession_number != filing.accession_number:
            reasons.add("header_accession_mismatch")
        if filing.registrant_cik is not None and filing.registrant_cik not in header.filer_ciks:
            reasons.add("header_cik_mismatch")
        if filed is not None and header.filing_date is not None and header.filing_date != filed:
            reasons.add("header_filing_date_mismatch")
        if header.submission_type is not None:
            authority[header.submission_type].add("edgar_header")
        acceptance = header.acceptance_at.astimezone(UTC)
        filed = header.filing_date or filed
    form_type, form_source = filing.form_type, "copy"
    if len(authority) > 1:
        reasons.add("form_type_conflict")
        form_type = min(copy_forms & NCEN_FORMS, default=None) or min(authority)
    elif authority:
        (auth_form,) = authority
        form_source = "edgar_header" if "edgar_header" in authority[auth_form] else "edgar_index"
        if auth_form == NT_FORM and not has_xml and dera_forms == {ORIGINAL_FORM} and copy_forms == {ORIGINAL_FORM}:
            counts["nt_ncen_relabelled_by_dera"] += 1
            form_type = NT_FORM
        elif copy_forms and copy_forms != {auth_form} and not filing.is_placeholder:
            reasons.add("form_type_conflict")
            counts["form_type_conflict"] += 1
            # Keep an N-CEN form so the conflicting accession stays in place and blocks.
            form_type = min(copy_forms & NCEN_FORMS, default=None) or auth_form
        else:
            form_type = auth_form
    date_bound = None if filed is None else date_only_public_available_at(filed, EDGAR_TIMEZONE)
    public = acceptance or date_bound
    basis = "edgar_acceptance" if acceptance is not None else ("date_only_conservative" if date_bound else None)
    if public is None:
        reasons.add("public_time_unknown")
    known = filing.data_known_at
    if has_xml:
        # The XML copy is the filing itself: its data is available once the filing is public.
        known = public
    elif known is not None and public is not None:
        known = max(known, public)
    elif known is None:
        known = public if not filing.is_placeholder else None
    updated = filing.with_updates(
        form_type=form_type, form_type_source=form_source, filing_date=filed, public_available_at=public,
        public_time_basis=basis, data_known_at=known, acceptance_at=acceptance, public_date_bound=date_bound,
        header_retrieved_at=None if header is None else header.retrieved_at.astimezone(UTC),
    )
    if reasons:
        counts["time_or_identity_quarantine"] += 1
        return _quarantined(updated, *reasons)
    return updated


# ---------------------------------------------------------------------------
# Amendment semantics
# ---------------------------------------------------------------------------
def amendment_semantics(filing: NcenFiling) -> str | None:
    """``None`` for an original; for ``N-CEN/A``: ``complete_replacement`` when an EDGAR XML
    copy with a covered schema version exists (see module docstring), else ``unknown``."""
    if filing.form_type != AMENDMENT_FORM:
        return None
    has_xml = "edgar_xml" in filing.source.split("+")
    if has_xml and filing.schema_version in AMENDMENT_COVERED_SCHEMA_VERSIONS:
        return AMENDMENT_COMPLETE
    return AMENDMENT_UNKNOWN


# ---------------------------------------------------------------------------
# Effective N-CEN selection and completeness
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SelectionDependency:
    """One filing the selection outcome depends on, with its evidenced knowledge time
    (``None`` = cannot be established)."""

    accession_number: str
    role: str
    knowledge_time: dt.datetime | None

    def record(self) -> list[str | None]:
        return [self.accession_number, self.role, _ts(self.knowledge_time)]


@dataclass(frozen=True)
class EffectiveSelection:
    """Effective N-CEN of one registrant at ``(report_date, K, mode)``; ``reason`` is
    ``None`` exactly when ``filing`` is usable evidence. ``dependencies`` lists every filing
    the outcome relies on (selected, same-period competitors, blocking/uncertain filings)."""

    registrant_cik: str
    report_date: dt.date
    knowledge_cutoff: dt.datetime
    knowledge_mode: str
    filing: NcenFiling | None
    reason: str | None
    amendment_semantics: str | None = None
    dependencies: tuple[SelectionDependency, ...] = ()

    @property
    def time_established(self) -> bool:
        return all(d.knowledge_time is not None for d in self.dependencies)

    @property
    def knowledge_time(self) -> dt.datetime | None:
        times = [d.knowledge_time for d in self.dependencies if d.knowledge_time is not None]
        return max(times, default=None)


def _check_mode(mode: str) -> None:
    if mode not in KNOWLEDGE_MODES:
        raise NcenError(f"knowledge_mode_invalid:{mode}")


def _dependency_time(filing: NcenFiling, mode: str, cutoff: dt.datetime, *, selected: bool) -> dt.datetime | None:
    bound = filing.admission_bound(mode, cutoff)
    if bound is None:
        return None
    times = [bound]
    exact = filing.exact_acceptance(mode, cutoff)
    if exact is not None and mode == KNOWLEDGE_CURRENT_RUN and filing.header_retrieved_at is not None:
        times.append(filing.header_retrieved_at)
    if selected and filing.data_known_at is not None:
        times.append(filing.data_known_at)
    if mode == KNOWLEDGE_CURRENT_RUN:
        if filing.retrieved_at is None:
            return None
        times.append(filing.retrieved_at)
    return max(times)


def effective_filing(
    index: NcenFilingIndex,
    cik: str,
    report_date: dt.date,
    knowledge_cutoff: dt.datetime,
    *,
    knowledge_mode: str,
) -> EffectiveSelection:
    """FE-1 effective N-CEN: the N-CEN/N-CEN/A with the latest period end in
    ``[R - 15 months, R]`` admitted by ``K`` (see :meth:`NcenFiling.visible`).

    Competing filings of the latest period are ordered only by exact acceptance times.
    Never falls back past a quarantined pick, an unknown-semantics amendment, a data
    version not available by K, an unplaceable later filing, or a filing whose admission
    by K is uncertain; each case is a typed ``reason``.
    """
    _check_mode(knowledge_mode)
    padded = normalize_cik(cik)
    cutoff = knowledge_cutoff.astimezone(UTC)
    if padded is None:
        return EffectiveSelection(str(cik), report_date, cutoff, knowledge_mode, None, "cik_invalid")
    floor = months_before(report_date, EFFECTIVE_WINDOW_MONTHS)
    history = index.by_cik.get(padded, ())
    visible = [f for f in history if f.visible(knowledge_mode, cutoff)]
    in_window = [f for f in visible if f.report_period_end is not None and floor <= f.report_period_end <= report_date]
    top_period = max((f.report_period_end for f in in_window), default=None)
    top = [f for f in in_window if f.report_period_end == top_period]
    dependencies: list[SelectionDependency] = []
    reason: str | None = None
    pick: NcenFiling | None = None
    if len(top) == 1:
        pick = top[0]
    elif len(top) > 1:
        exact = [f.exact_acceptance(knowledge_mode, cutoff) for f in top]
        if any(t is None for t in exact) or len(set(exact)) != len(exact):
            reason = ORDER_UNRESOLVED
        else:
            pick = max(top, key=lambda f: f.exact_acceptance(knowledge_mode, cutoff))  # type: ignore[arg-type,return-value]
        for other in top:
            if other is not pick:
                dependencies.append(SelectionDependency(
                    other.accession_number, "same_period_competitor",
                    _dependency_time(other, knowledge_mode, cutoff, selected=False)))
    if pick is not None:
        dependencies.append(SelectionDependency(
            pick.accession_number, "selected", _dependency_time(pick, knowledge_mode, cutoff, selected=True)))

    # Filings that could supersede the pick but cannot be placed, or whose admission by K
    # depends on a date-only bound.
    horizon = date_only_public_available_at(top_period if top_period else floor, EDGAR_TIMEZONE)
    for other in history:
        if other in top:
            continue
        could_supersede = other.report_period_end is None or (
            floor <= other.report_period_end <= report_date
            and (top_period is None or other.report_period_end >= top_period)
        )
        if not could_supersede:
            continue
        if knowledge_mode == KNOWLEDGE_CURRENT_RUN and (other.retrieved_at is None or other.retrieved_at > cutoff):
            continue  # not held by the run at K: unknown to it
        bound = other.admission_bound(knowledge_mode, cutoff)
        if bound is None:
            dependencies.append(SelectionDependency(other.accession_number, "public_time_unknown", None))
            reason = reason or (UNACQUIRED_FILING if other.is_placeholder else "filing_public_time_unknown")
            continue
        if bound <= cutoff:
            if other.report_period_end is None and bound >= horizon:
                dependencies.append(SelectionDependency(
                    other.accession_number, "blocking_period_unknown",
                    _dependency_time(other, knowledge_mode, cutoff, selected=False)))
                reason = reason or (UNACQUIRED_FILING if other.is_placeholder else "filing_period_unknown")
            continue
        earliest = other.earliest_public(knowledge_mode, cutoff)
        if earliest is None or earliest <= cutoff:
            # May or may not have been public by K: the order depends on a date-only bound.
            dependencies.append(SelectionDependency(other.accession_number, "admission_uncertain", cutoff))
            reason = reason or ORDER_UNRESOLVED

    semantics = None if pick is None else amendment_semantics(pick)

    def done(why: str | None) -> EffectiveSelection:
        return EffectiveSelection(padded, report_date, cutoff, knowledge_mode, pick, why, semantics,
                                  tuple(sorted(dependencies, key=lambda d: (d.role, d.accession_number))))

    if reason is not None:
        return done(reason)
    if pick is None:
        stale = any(f.report_period_end is not None and f.report_period_end < floor for f in visible)
        return done("effective_filing_older_than_15_months" if stale else "no_effective_filing")
    if not pick.usable:
        return done("effective_filing_quarantined")
    if semantics == AMENDMENT_UNKNOWN:
        return done(AMENDMENT_UNKNOWN_REASON)
    if semantics == AMENDMENT_PARTIAL:
        return done(AMENDMENT_PARTIAL_REASON)
    if not pick.data_available(knowledge_mode, cutoff):
        return done("effective_data_not_known_at_cutoff")
    return done(None)


@dataclass(frozen=True)
class FamilyProfile:
    """FE-1 comparison inputs of one effective filing (``complete`` or the reasons not)."""

    registrant_cik: str
    accession_number: str
    complete: bool
    reasons: tuple[str, ...]
    family_answer: str | None
    family_key: str | None
    adviser_tokens: frozenset[str]
    underwriter_tokens: frozenset[str]


def family_profile(filing: NcenFiling) -> FamilyProfile:
    """Completeness: B.5 answered (a Y needs a name that is not blank, not a missing-value
    sentinel and normalizes non-empty); at least one fund; every fund has a current primary
    adviser; every adviser/sub-adviser row, current or terminated, has a valid file number,
    CRD or LEI; every reported principal underwriter has a valid file number or CRD."""
    reasons: set[str] = set()
    if not filing.usable:
        reasons.add("filing_quarantined")
    answer = filing.family_answer
    key = None
    if answer is None:
        reasons.add("b5_unanswered")
    elif answer == "Y":
        if _clean(filing.family_name_raw) is None:
            reasons.add("b5_family_name_missing")
        elif is_missing_value_sentinel(filing.family_name_raw):
            reasons.add("b5_family_name_sentinel")
        else:
            key = normalize_family_name(filing.family_name_raw)
            if key is None:
                reasons.add("b5_family_name_unparseable")
    if not filing.funds:
        reasons.add("no_funds")
    adviser_tokens: set[str] = set()
    for fund in filing.funds:
        if not any(a.role == CURRENT_PRIMARY_ROLE for a in fund.advisers):
            reasons.add("fund_without_current_adviser")
        for adviser in fund.advisers:
            if not adviser.tokens:
                reasons.add("adviser_without_identifier")
            adviser_tokens |= adviser.tokens
    underwriter_tokens: set[str] = set()
    for underwriter in filing.underwriters:
        if not underwriter.tokens:
            reasons.add("underwriter_without_identifier")
        underwriter_tokens |= underwriter.tokens
    return FamilyProfile(
        registrant_cik=filing.registrant_cik or "", accession_number=filing.accession_number,
        complete=not reasons, reasons=tuple(sorted(reasons)), family_answer=answer, family_key=key,
        adviser_tokens=frozenset(adviser_tokens), underwriter_tokens=frozenset(underwriter_tokens),
    )


def voting_series_gaps(filing: NcenFiling, fund_keys: Iterable[str]) -> tuple[str, ...]:
    """FE-1b: reasons the effective filing does not cover the registrant's voting series on
    the report date (empty => covered). Only this filing's funds count; no union."""
    keys = set(fund_keys)
    if not keys:
        return (INCOMPLETE_NO_VOTES,)
    listed = {fund.series_id for fund in filing.funds if fund.series_id is not None}
    gaps = set()
    for key in keys:
        if not is_series_key(key):
            gaps.add(INCOMPLETE_NO_SERIES_ID)
        elif key not in listed:
            gaps.add(INCOMPLETE_SERIES_ABSENT)
    return tuple(sorted(gaps))


def not_independent(a: FamilyProfile, b: FamilyProfile) -> bool:
    """FE-1 pairwise test (``True`` = not independent). Incomplete => not independent."""
    if not (a.complete and b.complete) or a.registrant_cik == b.registrant_cik:
        return True
    if a.family_answer == "Y" and b.family_answer == "Y" and a.family_key == b.family_key:
        return True
    if a.adviser_tokens & b.adviser_tokens:
        return True
    return bool(a.underwriter_tokens and b.underwriter_tokens and a.underwriter_tokens & b.underwriter_tokens)


# ---------------------------------------------------------------------------
# Family components (union-find closure of the not-independent relation)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FamilyComponent:
    component_id: str
    members: tuple[str, ...]
    accessions: tuple[str, ...]


@dataclass(frozen=True)
class ComponentResult:
    """Components among the complete registrants of one universe at ``(R, K, mode)``.

    ``knowledge_time`` is the latest evidenced time over every selection dependency of the
    universe (the structure depends on all of them); ``time_established`` is ``False`` when
    any dependency time cannot be established, in which case no family evidence is emitted
    for the date.
    """

    report_date: dt.date
    knowledge_cutoff: dt.datetime
    knowledge_mode: str
    universe: tuple[str, ...]
    voting_series: Mapping[str, tuple[str, ...]]
    components: tuple[FamilyComponent, ...]
    component_of: Mapping[str, str]
    profiles: Mapping[str, FamilyProfile]
    selections: Mapping[str, EffectiveSelection]
    incomplete: Mapping[str, tuple[str, ...]]
    public_available_at: dt.datetime | None
    knowledge_time: dt.datetime | None
    time_established: bool
    universe_digest: str

    @property
    def component_sizes(self) -> tuple[int, ...]:
        return tuple(sorted((len(c.members) for c in self.components), reverse=True))


def component_id_for(
    report_date: dt.date, knowledge_cutoff: dt.datetime, members: Iterable[str], *, knowledge_mode: str
) -> str:
    """Deterministic family-component identity (rule version, R, K, mode, sorted members)."""
    payload = [RULE_VERSION, report_date.isoformat(), _ts(knowledge_cutoff), knowledge_mode, sorted(members)]
    return "ncenfam:" + _sha(payload)[:32]


class _UnionFind:
    def __init__(self, items: Iterable[str]) -> None:
        self.parent = {item: item for item in items}

    def find(self, item: str) -> str:
        root = item
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[item] != root:
            self.parent[item], item = root, self.parent[item]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            low, high = sorted((ra, rb))
            self.parent[high] = low


def family_components(
    index: NcenFilingIndex,
    voters: Mapping[str, Iterable[str]],
    report_date: dt.date,
    knowledge_cutoff: dt.datetime,
    *,
    knowledge_mode: str,
) -> ComponentResult:
    """Merge the complete registrants of ``voters`` (CIK -> fund keys it votes with on
    ``report_date``) into FE-1 family components at ``(R, K, mode)``.

    Building block only (no family identity for acceptance): the accepting path fixes the
    FE-1a universe from a :class:`VoteInventory`.
    """
    _check_mode(knowledge_mode)
    cutoff = knowledge_cutoff.astimezone(UTC)
    voting: dict[str, set[str]] = defaultdict(set)
    for raw, keys in voters.items():
        padded = normalize_cik(raw)
        if padded is None:
            raise NcenError(f"voter_cik_invalid:{raw}")
        voting[padded].update(keys)
    universe = tuple(sorted(voting))
    selections = {cik: effective_filing(index, cik, report_date, cutoff, knowledge_mode=knowledge_mode)
                  for cik in universe}
    profiles: dict[str, FamilyProfile] = {}
    incomplete: dict[str, tuple[str, ...]] = {}
    for cik, chosen in selections.items():
        if chosen.reason is not None or chosen.filing is None:
            incomplete[cik] = (chosen.reason or "no_effective_filing",)
            continue
        profile = family_profile(chosen.filing)
        reasons = set(profile.reasons) | set(voting_series_gaps(chosen.filing, voting[cik]))
        if reasons:
            incomplete[cik] = tuple(sorted(reasons))
        else:
            profiles[cik] = profile
    finder = _UnionFind(profiles)
    owners: dict[tuple[str, str], str] = {}
    for cik in sorted(profiles):
        profile = profiles[cik]
        keys = [("adv", t) for t in profile.adviser_tokens] + [("uw", t) for t in profile.underwriter_tokens]
        if profile.family_answer == "Y" and profile.family_key is not None:
            keys.append(("name", profile.family_key))
        for key in keys:
            first = owners.setdefault(key, cik)
            if first != cik:
                finder.union(first, cik)
    groups: dict[str, list[str]] = defaultdict(list)
    for cik in profiles:
        groups[finder.find(cik)].append(cik)
    components = []
    component_of: dict[str, str] = {}
    for members in sorted((sorted(m) for m in groups.values()), key=lambda m: m[0]):
        cid = component_id_for(report_date, cutoff, members, knowledge_mode=knowledge_mode)
        components.append(FamilyComponent(cid, tuple(members),
                                          tuple(profiles[m].accession_number for m in members)))
        for member in members:
            component_of[member] = cid
    dependencies = [d for s in selections.values() for d in s.dependencies]
    established = all(d.knowledge_time is not None for d in dependencies)
    times = [d.knowledge_time for d in dependencies if d.knowledge_time is not None]
    public = [s.filing.admission_bound(knowledge_mode, cutoff) for s in selections.values() if s.filing is not None]
    series = {cik: tuple(sorted(voting[cik])) for cik in universe}
    digest = _sha([
        RULE_VERSION, AMENDMENT_SEMANTICS_VERSION, knowledge_mode,
        [[cik, list(series[cik]), None if s.filing is None else s.filing.accession_number,
          None if s.filing is None else s.filing.projection_digest, s.amendment_semantics,
          list(incomplete.get(cik, ())), [d.record() for d in s.dependencies]]
         for cik, s in sorted(selections.items())],
    ])
    return ComponentResult(
        report_date=report_date, knowledge_cutoff=cutoff, knowledge_mode=knowledge_mode, universe=universe,
        voting_series=series, components=tuple(components), component_of=dict(sorted(component_of.items())),
        profiles=dict(sorted(profiles.items())), selections=dict(sorted(selections.items())),
        incomplete=dict(sorted(incomplete.items())),
        public_available_at=max((p for p in public if p is not None), default=None),
        knowledge_time=max(times, default=None), time_established=established, universe_digest=digest,
    )


# ---------------------------------------------------------------------------
# FE-1a vote inventory (complete N-PORT packages)
# ---------------------------------------------------------------------------
_INVENTORY_SEAL = object()
INVENTORY_VERSION = "bond_default_ncen_vote_inventory_v1"


def _vote_fingerprint(vote: Vote) -> bytes:
    payload = [vote.accession_number, vote.fund_key, vote.registrant_cik, vote.cusip9, vote.report_date.isoformat(),
               vote.value, vote.y_lots, vote.n_lots, vote.nonvote_lots, vote.invalid_lots,
               _ts(vote.public_available_at), sorted(str(o) for o in vote.observation_ids)]
    return hashlib.blake2b(_canonical(payload), digest_size=16).digest()


@dataclass(frozen=True)
class InventorySource:
    """One complete N-PORT package the inventory streamed."""

    package_label: str
    zip_sha256: str
    package_id: str
    retrieved_at: dt.datetime
    first_verified_public_at: dt.datetime

    def record(self) -> list[str | None]:
        return [self.package_label, self.zip_sha256, self.package_id, _ts(self.retrieved_at),
                _ts(self.first_verified_public_at)]


@dataclass(frozen=True, eq=False)
class VoteInventory:
    """Sealed FE-1a universe: for each report date, every registrant with a Y, N or
    disputed vote public by K in the complete packages, with its voting fund keys.

    Only :func:`build_vote_inventory` can construct it. ``fingerprints`` covers every vote
    (any value) so target votes can be proven to be a subset.
    """

    knowledge_cutoff: dt.datetime
    knowledge_mode: str
    sources: tuple[InventorySource, ...]
    voters: Mapping[dt.date, Mapping[str, frozenset[str]]]
    known_at: Mapping[dt.date, dt.datetime]
    disputed_families: tuple[FilingFamily, ...]
    stats: Mapping[str, Any]
    digest: str
    _fingerprints: frozenset[bytes] = field(repr=False)
    _resolution: Any = field(repr=False)
    _seal: object = field(repr=False)

    def __post_init__(self) -> None:
        if self._seal is not _INVENTORY_SEAL:
            raise NcenError("vote_inventory_sealed:use_build_vote_inventory")

    def contains(self, vote: Vote) -> bool:
        return _vote_fingerprint(vote) in self._fingerprints

    def target_votes(self, packages: Sequence[nport.DeraPackageResult], cusips: Iterable[str], *,
                     acceptance_headers: Mapping[str, AcceptanceHeader] | None = None,
                     reconciliations: Iterable[nport.DeraReconciliation] = (),
                     batch_rows: int = 50_000) -> tuple[Vote, ...]:
        """Votes of ``cusips`` from the same packages and revision resolution."""
        _check_sources(self, packages)
        wanted = frozenset(cusips)
        streams = [nport.iter_package_observations(p, acceptance_headers=acceptance_headers,
                                                   reconciliations=reconciliations, cusips=wanted,
                                                   batch_rows=batch_rows) for p in packages]
        votes: list[Vote] = []
        builder = nport._PreparedVoteBuilder(self._resolution)
        for chunk in _accession_chunks(streams):
            votes.extend(builder.build(chunk).votes)
        return tuple(sorted(votes, key=lambda v: (v.cusip9, v.report_date, v.fund_key, v.accession_number)))


def _check_sources(inventory: VoteInventory, packages: Sequence[nport.DeraPackageResult]) -> None:
    given = sorted((p.package_label, p.zip_sha256) for p in packages)
    held = sorted((s.package_label, s.zip_sha256) for s in inventory.sources)
    if given != held:
        raise NcenError("inventory_packages_mismatch")


def _accession_chunks(streams: Sequence[Iterable[CreditObservation]]) -> Iterator[list[CreditObservation]]:
    """Group observations by accession across package streams sorted by accession."""
    def checked(stream: Iterable[CreditObservation]) -> Iterator[CreditObservation]:
        last = ""
        for observation in stream:
            accession = observation.accession_number or ""
            if accession < last:
                raise NcenError("package_stream_not_sorted_by_accession")
            last = accession
            yield observation

    merged = heapq.merge(*(checked(s) for s in streams), key=lambda o: o.accession_number or "")
    chunk: list[CreditObservation] = []
    current: str | None = None
    for observation in merged:
        accession = observation.accession_number or ""
        if chunk and accession != current:
            yield chunk
            chunk = []
        current = accession
        chunk.append(observation)
    if chunk:
        yield chunk


def build_vote_inventory(
    packages: Sequence[nport.DeraPackageResult],
    *,
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str,
    acceptance_headers: Mapping[str, AcceptanceHeader] | None = None,
    reconciliations: Iterable[nport.DeraReconciliation] = (),
    batch_rows: int = 50_000,
) -> VoteInventory:
    """Stream every observation of the complete parsed N-PORT ``packages`` (no CUSIP
    filter) through W1's revision resolution and vote builder at ``(K, mode)``, one
    accession at a time (bounded memory), and record the FE-1a universe."""
    _check_mode(knowledge_mode)
    cutoff = knowledge_cutoff.astimezone(UTC)
    if not packages:
        raise NcenError("inventory_requires_packages")
    for package in packages:
        if package.status != "parsed" or package.source_package is None:
            raise NcenError(f"inventory_package_not_parsed:{package.package_label}:{package.status}")
    labels = [p.package_label for p in packages]
    if len(set(labels)) != len(labels):
        raise NcenError("inventory_duplicate_package")
    reconciliations = tuple(reconciliations)
    filings = [f for p in packages for f in nport.accession_filings_from_result(
        p, acceptance_headers=acceptance_headers, reconciliations=reconciliations)]
    resolution = nport.resolve_accession_revisions(filings, knowledge_cutoff=cutoff,
                                                   mode=_NPORT_REVISION_MODE[knowledge_mode])
    builder = nport._PreparedVoteBuilder(resolution)
    streams = [nport.iter_package_observations(p, acceptance_headers=acceptance_headers,
                                               reconciliations=reconciliations, batch_rows=batch_rows)
               for p in packages]
    voters: dict[dt.date, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    latest: dict[dt.date, dt.datetime] = {}
    fingerprints: set[bytes] = set()
    counts: Counter[str] = Counter()
    for chunk in _accession_chunks(streams):
        counts["observations"] += len(chunk)
        vote_set = builder.build(chunk)
        for reason, number in vote_set.excluded_observations.items():
            counts[f"excluded:{reason}"] += number
        for vote in vote_set.votes:
            counts["votes"] += 1
            counts[f"votes:{vote.value}"] += 1
            fingerprints.add(_vote_fingerprint(vote))
            if vote.value not in UNIVERSE_VOTE_VALUES or vote.public_available_at > cutoff:
                continue
            cik = normalize_cik(vote.registrant_cik)
            if cik is None:
                counts["universe_vote_without_cik"] += 1
                continue
            voters[vote.report_date][cik].add(vote.fund_key)
            seen = latest.get(vote.report_date)
            if seen is None or vote.public_available_at > seen:
                latest[vote.report_date] = vote.public_available_at
    sources = tuple(sorted((InventorySource(
        package_label=p.package_label, zip_sha256=p.zip_sha256, package_id=str(p.source_package.package_id),  # type: ignore[union-attr]
        retrieved_at=p.source_package.retrieved_at, first_verified_public_at=p.source_package.first_verified_public_at,  # type: ignore[union-attr]
    ) for p in packages), key=lambda s: s.package_label))
    frozen_voters = {d: {c: frozenset(k) for c, k in sorted(m.items())} for d, m in sorted(voters.items())}
    digest = _sha([
        INVENTORY_VERSION, _ts(cutoff), knowledge_mode, [s.record() for s in sources],
        [[d.isoformat(), [[c, sorted(k)] for c, k in m.items()]] for d, m in frozen_voters.items()],
        [[d.isoformat(), _ts(t)] for d, t in sorted(latest.items())],
        hashlib.sha256(b"".join(sorted(fingerprints))).hexdigest(),
    ])
    counts["report_dates"] = len(frozen_voters)
    counts["universe_registrant_dates"] = sum(len(m) for m in frozen_voters.values())
    return VoteInventory(
        knowledge_cutoff=cutoff, knowledge_mode=knowledge_mode, sources=sources, voters=frozen_voters,
        known_at=dict(sorted(latest.items())), disputed_families=builder.disputed_families,
        stats=dict(sorted(counts.items())),
        digest=digest, _fingerprints=frozenset(fingerprints), _resolution=resolution, _seal=_INVENTORY_SEAL,
    )


def _require_inventory(inventory: object, cutoff: dt.datetime, mode: str) -> VoteInventory:
    if not isinstance(inventory, VoteInventory) or inventory._seal is not _INVENTORY_SEAL:
        raise NcenError("vote_inventory_required")
    if inventory.knowledge_cutoff != cutoff:
        raise NcenError("inventory_knowledge_cutoff_mismatch")
    if inventory.knowledge_mode != mode:
        raise NcenError("inventory_knowledge_mode_mismatch")
    return inventory


# ---------------------------------------------------------------------------
# Family evidence: diagnostic report (public) and W1 FamilyEvidence (private)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FamilyComponentsReport:
    """Diagnostic FE-1/FE-1a/FE-1b membership data at one ``(K, mode)`` for one inventory.

    Not consumable as family identity: it carries no :class:`~.nport.FamilyEvidence`.
    ``date_status`` is ``evidence`` or the typed reason no evidence exists for the date.
    """

    rule_version: str
    amendment_semantics_version: str
    knowledge_cutoff: dt.datetime
    knowledge_mode: str
    inventory_digest: str
    components: Mapping[dt.date, ComponentResult]
    date_status: Mapping[dt.date, str]
    stats: Mapping[str, Any]


def _components_report(index: NcenFilingIndex, inventory: VoteInventory) -> FamilyComponentsReport:
    cutoff, mode = inventory.knowledge_cutoff, inventory.knowledge_mode
    results: dict[dt.date, ComponentResult] = {}
    status: dict[dt.date, str] = {}
    counts: Counter[str] = Counter()
    for report_date, voters in inventory.voters.items():
        result = family_components(index, voters, report_date, cutoff, knowledge_mode=mode)
        results[report_date] = result
        counts["universe_registrants"] += len(result.universe)
        counts["complete_registrants"] += len(result.profiles)
        for reasons in result.incomplete.values():
            for reason in reasons:
                counts[f"incomplete:{reason}"] += 1
        if not result.time_established:
            status[report_date] = DEPENDENCY_TIME_UNKNOWN
        elif not result.components:
            status[report_date] = "no_complete_registrants"
        else:
            status[report_date] = "evidence"
        counts[f"date_status:{status[report_date]}"] += 1
    counts["largest_component_max"] = max((r.component_sizes[0] for r in results.values() if r.components),
                                          default=0)
    return FamilyComponentsReport(
        rule_version=RULE_VERSION, amendment_semantics_version=AMENDMENT_SEMANTICS_VERSION,
        knowledge_cutoff=cutoff, knowledge_mode=mode, inventory_digest=inventory.digest,
        components=results, date_status=dict(sorted(status.items())), stats=dict(sorted(counts.items())),
    )


def family_evidence_for(
    index: NcenFilingIndex, inventory: VoteInventory, *, knowledge_cutoff: dt.datetime, knowledge_mode: str
) -> FamilyComponentsReport:
    """Diagnostic FE-1a/FE-1b components for the inventory's universe (no FamilyEvidence)."""
    _check_mode(knowledge_mode)
    return _components_report(index, _require_inventory(inventory, knowledge_cutoff.astimezone(UTC),
                                                        knowledge_mode))


def _private_family_evidence(
    report: FamilyComponentsReport, inventory: VoteInventory
) -> dict[str, tuple[FamilyEvidence, ...]]:
    """W1 FamilyEvidence for dates with ``evidence`` status; used only at the same K."""
    cutoff = report.knowledge_cutoff
    by_cik: dict[str, list[FamilyEvidence]] = defaultdict(list)
    for report_date, result in report.components.items():
        if report.date_status[report_date] != "evidence" or result.public_available_at is None:
            continue
        known = max(t for t in (result.knowledge_time, result.public_available_at,
                                inventory.known_at.get(report_date)) if t is not None)
        if known > cutoff:
            raise NcenError(f"evidence_known_after_cutoff:{report_date}")
        for component in result.components:
            members = [[m, result.profiles[m].accession_number,
                        result.selections[m].filing.projection_digest]  # type: ignore[union-attr]
                       for m in component.members]
            for cik in component.members:
                accession = result.profiles[cik].accession_number
                digest = _sha({
                    "rule_version": RULE_VERSION, "report_date": report_date.isoformat(),
                    "knowledge_cutoff": _ts(cutoff), "knowledge_mode": report.knowledge_mode,
                    "registrant_cik": cik, "component_id": component.component_id, "members": members,
                    "voting_series": list(result.voting_series[cik]), "universe_digest": result.universe_digest,
                    "inventory_digest": report.inventory_digest,
                })
                by_cik[cik].append(FamilyEvidence(
                    registrant_cik=cik, family_id=component.component_id,
                    evidence_ref=(f"sec_ncen:{accession};rule:{RULE_VERSION};report_date:{report_date.isoformat()};"
                                  f"cutoff:{_ts(cutoff)};mode:{report.knowledge_mode};"
                                  f"component:{component.component_id}"),
                    evidence_digest=digest, valid_from=report_date,
                    valid_to=report_date + dt.timedelta(days=1), public_available_at=result.public_available_at,
                    known_at=known,
                ))
    return {cik: tuple(items) for cik, items in sorted(by_cik.items())}


def build_consensus_with_ncen(
    inventory: VoteInventory,
    index: NcenFilingIndex,
    *,
    target_votes: Iterable[Vote],
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str,
    corroborations: Iterable[IndependentCorroboration] = (),
    resolved_votes: Iterable[VoteKey] = (),
) -> tuple[ConsensusResult, FamilyComponentsReport]:
    """Accepting path: FE-1/FE-1a/FE-1b family evidence and ``nport.build_consensus_states``
    at one ``(K, mode)``. The universe is the sealed inventory (same K and mode); every
    target vote must belong to it; family evidence is built privately and never returned.
    Disputed accession families come from the inventory's revision resolution."""
    _check_mode(knowledge_mode)
    cutoff = knowledge_cutoff.astimezone(UTC)
    held = _require_inventory(inventory, cutoff, knowledge_mode)
    targets = tuple(target_votes)
    missing = sum(1 for vote in targets if not held.contains(vote))
    if missing:
        raise NcenError(f"target_votes_not_in_inventory:{missing}")
    report = _components_report(index, held)
    evidence = _private_family_evidence(report, held)
    result = build_consensus_states(
        targets, knowledge_cutoff=cutoff, family_evidence=evidence, corroborations=corroborations,
        resolved_votes=resolved_votes, disputed_families=held.disputed_families,
    )
    return result, report


# ---------------------------------------------------------------------------
# Non-accepting sensitivity: per-state closure (FE-1a)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PerStateComponentCount:
    """Closure over the Y voters of one ``(cusip9, R)``: counts only, no family identity."""

    cusip9: str
    report_date: dt.date
    y_registrants: int
    complete_registrants: int
    component_count: int
    component_sizes: tuple[int, ...]
    incomplete_reasons: Mapping[str, int]


@dataclass(frozen=True)
class PerStateDiagnostic:
    """NON-ACCEPTING sensitivity (FE-1a). Carries no component IDs and no
    :class:`~.nport.FamilyEvidence`; it can become primary only through an owner decision
    recorded before the held-out evaluation."""

    rule_version: str
    knowledge_cutoff: dt.datetime
    knowledge_mode: str
    accepting: bool
    states: tuple[PerStateComponentCount, ...]


def diagnostic_per_state_components(
    index: NcenFilingIndex,
    inventory: VoteInventory,
    target_votes: Iterable[Vote],
    *,
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str,
) -> PerStateDiagnostic:
    """Per-state closure sensitivity over the Y voters of each target ``(cusip9, R)``;
    FE-1b uses each registrant's voting series from the inventory. Never feeds consensus."""
    _check_mode(knowledge_mode)
    cutoff = knowledge_cutoff.astimezone(UTC)
    held = _require_inventory(inventory, cutoff, knowledge_mode)
    y_voters: dict[tuple[str, dt.date], set[str]] = defaultdict(set)
    for vote in target_votes:
        if not held.contains(vote):
            raise NcenError("target_votes_not_in_inventory:1")
        cik = normalize_cik(vote.registrant_cik)
        if vote.value == "Y" and vote.public_available_at <= cutoff and cik is not None:
            y_voters[(vote.cusip9, vote.report_date)].add(cik)
    states = []
    for (cusip, report_date), ciks in sorted(y_voters.items()):
        date_voters = held.voters.get(report_date, {})
        result = family_components(index, {c: date_voters.get(c, frozenset()) for c in ciks}, report_date, cutoff,
                                   knowledge_mode=knowledge_mode)
        reasons = Counter(r for rs in result.incomplete.values() for r in rs)
        states.append(PerStateComponentCount(
            cusip9=cusip, report_date=report_date, y_registrants=len(result.universe),
            complete_registrants=len(result.profiles), component_count=len(result.components),
            component_sizes=result.component_sizes, incomplete_reasons=dict(sorted(reasons.items())),
        ))
    return PerStateDiagnostic(rule_version=RULE_VERSION, knowledge_cutoff=cutoff, knowledge_mode=knowledge_mode,
                              accepting=False, states=tuple(states))


# === FE-1 purpose-labelled diagnostic core ==============================================
# Additive, source-independent and non-accepting. Raw sidecars, cohort loading and exports
# are deliberately outside this section and require a separately bound diagnostic lane.

DIAGNOSTIC_SCHEMA_VERSION = "ncen_purpose_diagnostics_v1"
DIAGNOSTIC_COHORT_VERSION = "ncen_diagnostic_cohort_v1"
DIAGNOSTIC_REPORTED_FAMILY_VERSION = "ncen_reported_family_v2"
DIAGNOSTIC_NAME_NORMALIZER_VERSION = "ncen_reported_name_key_v2"
DIAGNOSTIC_DEPENDENCE_VERSION = "ncen_reporting_dependence_block_v2"
DIAGNOSTIC_EDGE_VERSION = "ncen_provider_incidence_v2"
DIAGNOSTIC_SELECTION_VERSION = "ncen_diagnostic_selection_v2"
DIAGNOSTIC_ADMISSION_VERSION = "ncen_diagnostic_admission_v1"
DIAGNOSTIC_FOLD_PROTOCOL_VERSION = "ncen_temporal_incidence_union_v1"
DIAGNOSTIC_PURPOSE = "reporting_dependence_block"
DIAGNOSTIC_REPORTED_CLAIM = "reported_assertion_not_ultimate_control"

DIAGNOSTIC_PROVIDER_ROLES = frozenset({
    "current_primary",
    "current_sub",
    "terminated_primary",
    "terminated_sub",
    "underwriter",
})
DIAGNOSTIC_ROLES = frozenset({*DIAGNOSTIC_PROVIDER_ROLES, "b5"})
DIAGNOSTIC_IDENTIFIER_KINDS = ("FN", "CRD", "LEI")
_DIAGNOSTIC_CONTEXT = re.compile(r"^ncenctx:[0-9a-f]{64}$")
_DIAGNOSTIC_ROW_ID = re.compile(r"^ncenrow:[a-z_]+:[0-9a-f]{64}$")
_DIAGNOSTIC_HASH = re.compile(r"^[0-9a-f]{64}$")


def _diagnostic_canonical(value: Any) -> bytes:
    """Canonical JSON bytes for diagnostic identities; never reuse legacy ``_canonical``."""
    try:
        text = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise NcenError("diagnostic_value_not_canonical_json") from exc
    return text.encode("utf-8")


def _diagnostic_hash(value: Any) -> str:
    return hashlib.sha256(_diagnostic_canonical(value)).hexdigest()


def _diagnostic_id(prefix: str, value: Any) -> str:
    return f"{prefix}:{_diagnostic_hash(value)}"


def _diagnostic_nonempty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise NcenError(f"{field_name}_invalid")


def _diagnostic_normalized_cik(value: str) -> None:
    if normalize_cik(value) != value or len(value) != 10:
        raise NcenError("cik_not_normalized")


def _diagnostic_context_id_valid(value: str) -> None:
    if _DIAGNOSTIC_CONTEXT.fullmatch(value) is None:
        raise NcenError("context_id_invalid")


def _diagnostic_hash_valid(value: str, field_name: str) -> None:
    if _DIAGNOSTIC_HASH.fullmatch(value) is None:
        raise NcenError(f"{field_name}_invalid")


def _diagnostic_nonnegative_count(value: Any, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise NcenError(f"{field_name}_invalid")


def _diagnostic_reasons(values: tuple[str, ...]) -> None:
    if values != tuple(sorted(set(values))) or any(not value or value.strip() != value for value in values):
        raise NcenError("reasons_not_sorted_unique")


def _diagnostic_timestamp(value: dt.datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise NcenError("datetime_not_timezone_aware")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def normalize_reported_name_key(raw: str | None) -> str | None:
    """Canonical conservative B.5 key: NFC, uppercase, whitespace collapse, then NFC."""
    if is_missing_value_sentinel(raw):
        return None
    import unicodedata

    assert raw is not None
    upper = unicodedata.normalize("NFC", raw).upper()
    key = " ".join(upper.split())
    return unicodedata.normalize("NFC", key) or None


def _diagnostic_canonical_reported_name_key(name_key: str) -> str:
    """Accept only an uppercase key, allowing canonical-equivalent Unicode input."""
    _diagnostic_nonempty(name_key, "reported_name_key")
    import unicodedata

    canonical = normalize_reported_name_key(name_key)
    if (
        canonical is None
        or unicodedata.normalize("NFD", canonical) != unicodedata.normalize("NFD", name_key)
    ):
        raise NcenError("reported_name_key_not_normalized")
    return canonical


def diagnostic_provider_key_id(identifier_kind: str, identifier_value: str) -> str:
    if identifier_kind not in DIAGNOSTIC_IDENTIFIER_KINDS:
        raise NcenError("identifier_kind_invalid")
    normalizers = {"FN": normalize_file_number, "CRD": normalize_crd, "LEI": normalize_lei}
    if normalizers[identifier_kind](identifier_value) != identifier_value:
        raise NcenError("identifier_value_not_normalized")
    return _diagnostic_id("ncenkey", ["provider", identifier_kind, identifier_value])


def diagnostic_b5_key_id(name_key: str) -> str:
    canonical = _diagnostic_canonical_reported_name_key(name_key)
    return _diagnostic_id("ncenkey", ["b5", DIAGNOSTIC_NAME_NORMALIZER_VERSION, canonical])


@dataclass(frozen=True, slots=True)
class DiagnosticSourceRow:
    """One immutable source witness already extracted and attested by a future sidecar lane."""

    accession_number: str
    registrant_cik: str
    role: str
    locator: str
    series_id: str | None
    series_scope: str
    answer_raw: str | None = None
    name_raw: str | None = None
    file_number_raw: str | None = None
    crd_raw: str | None = None
    lei_raw: str | None = None
    attestation: str = "attested"
    reasons: tuple[str, ...] = ()
    uncertain_expansion_eligible: bool = False

    def __post_init__(self) -> None:
        _diagnostic_nonempty(self.accession_number, "accession_number")
        _diagnostic_normalized_cik(self.registrant_cik)
        if self.role not in DIAGNOSTIC_ROLES:
            raise NcenError("diagnostic_role_invalid")
        _diagnostic_nonempty(self.locator, "locator")
        if self.series_scope not in {"series", "registrant", "unresolved"}:
            raise NcenError("series_scope_invalid")
        if self.role in {"underwriter", "b5"}:
            if self.series_scope != "registrant" or self.series_id is not None:
                raise NcenError("registrant_role_scope_invalid")
        elif self.series_scope == "registrant":
            raise NcenError("adviser_registrant_scope_invalid")
        elif self.series_scope == "series":
            if self.series_id is None or not is_series_key(self.series_id):
                raise NcenError("series_id_not_normalized")
        elif self.series_id is not None:
            raise NcenError("unresolved_scope_has_series")
        if self.answer_raw is not None and not isinstance(self.answer_raw, str):
            raise NcenError("diagnostic_b5_answer_invalid")
        if self.role != "b5" and self.answer_raw is not None:
            raise NcenError("provider_row_has_b5_answer")
        if self.attestation not in {"attested", "uncertain"}:
            raise NcenError("attestation_invalid")
        _diagnostic_reasons(self.reasons)
        if self.attestation == "uncertain" and not self.reasons:
            raise NcenError("uncertain_row_missing_reason")
        if self.uncertain_expansion_eligible and self.attestation != "uncertain":
            raise NcenError("uncertain_expansion_on_attested_row")

    @property
    def source_row_id(self) -> str:
        return _diagnostic_id("ncenrow:source_row", self.identity_payload())

    def identity_payload(self) -> list[Any]:
        return [
            self.accession_number,
            self.registrant_cik,
            self.role,
            self.locator,
            self.series_id,
            self.series_scope,
            self.answer_raw,
            self.name_raw,
            self.file_number_raw,
            self.crd_raw,
            self.lei_raw,
            self.attestation,
            list(self.reasons),
            self.uncertain_expansion_eligible,
        ]

    def normalized_identifiers(self) -> tuple[tuple[str, str], ...]:
        if self.role == "b5":
            return ()
        normalized = (
            ("FN", normalize_file_number(self.file_number_raw)),
            ("CRD", normalize_crd(self.crd_raw)),
            ("LEI", normalize_lei(self.lei_raw)),
        )
        return tuple((kind, value) for kind, value in normalized if value is not None)


@dataclass(frozen=True, slots=True)
class DiagnosticSourceIndex:
    rows: tuple[DiagnosticSourceRow, ...]

    def __post_init__(self) -> None:
        ids = tuple(row.source_row_id for row in self.rows)
        if ids != tuple(sorted(set(ids))):
            raise NcenError("source_index_not_sorted_unique")

    @classmethod
    def from_rows(cls, rows: Iterable[DiagnosticSourceRow]) -> DiagnosticSourceIndex:
        by_id: dict[str, DiagnosticSourceRow] = {}
        for row in rows:
            existing = by_id.setdefault(row.source_row_id, row)
            if existing != row:
                raise NcenError("source_row_identity_conflict")
        return cls(tuple(by_id[key] for key in sorted(by_id)))


@dataclass(frozen=True, slots=True)
class DiagnosticSelection:
    cik: str
    accession_number: str
    rows: tuple[DiagnosticSourceRow, ...]
    evidence_state: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _diagnostic_normalized_cik(self.cik)
        _diagnostic_nonempty(self.accession_number, "accession_number")
        if self.evidence_state not in {"complete", "incomplete"}:
            raise NcenError("evidence_state_invalid")
        _diagnostic_reasons(self.reasons)
        if self.evidence_state == "complete" and self.reasons:
            raise NcenError("complete_node_has_reasons")
        if self.evidence_state == "incomplete" and not self.reasons:
            raise NcenError("incomplete_node_missing_reason")
        ids = tuple(row.source_row_id for row in self.rows)
        if ids != tuple(sorted(set(ids))):
            raise NcenError("selection_rows_not_sorted_unique")
        for row in self.rows:
            if row.registrant_cik != self.cik or row.accession_number != self.accession_number:
                raise NcenError("selection_source_identity_mismatch")

    @classmethod
    def from_rows(
        cls,
        *,
        cik: str,
        accession_number: str,
        rows: Iterable[DiagnosticSourceRow],
        evidence_state: str,
        reasons: tuple[str, ...],
    ) -> DiagnosticSelection:
        index = DiagnosticSourceIndex.from_rows(rows)
        return cls(cik, accession_number, index.rows, evidence_state, reasons)


@dataclass(frozen=True, slots=True)
class ReportedFamily:
    cik: str
    state: str
    answer: str | None
    name_raw: str | None
    name_key: str | None
    label_id: str | None
    source_row_ids: tuple[str, ...]
    normalizer: str
    reasons: tuple[str, ...]
    claim: str = DIAGNOSTIC_REPORTED_CLAIM

    def __post_init__(self) -> None:
        _diagnostic_normalized_cik(self.cik)
        if self.state not in {"declared_family", "standalone", "unknown"}:
            raise NcenError("reported_family_state_invalid")
        if self.answer not in {None, "Y", "N"}:
            raise NcenError("reported_family_answer_invalid")
        if self.source_row_ids != tuple(sorted(set(self.source_row_ids))):
            raise NcenError("reported_family_sources_not_sorted_unique")
        if any(_DIAGNOSTIC_ROW_ID.fullmatch(value) is None for value in self.source_row_ids):
            raise NcenError("reported_family_source_id_invalid")
        if self.normalizer != DIAGNOSTIC_NAME_NORMALIZER_VERSION:
            raise NcenError("reported_family_normalizer_invalid")
        _diagnostic_reasons(self.reasons)
        if self.claim != DIAGNOSTIC_REPORTED_CLAIM:
            raise NcenError("reported_family_claim_invalid")
        if self.state == "unknown":
            if self.label_id is not None or self.name_key is not None or not self.reasons:
                raise NcenError("reported_family_unknown_invalid")
        elif self.reasons or self.label_id is None:
            raise NcenError("reported_family_known_invalid")
        elif self.state == "declared_family":
            if self.answer != "Y" or self.name_key is None:
                raise NcenError("reported_family_declared_invalid")
            canonical_key = _diagnostic_canonical_reported_name_key(self.name_key)
            object.__setattr__(self, "name_key", canonical_key)
            expected_label = _diagnostic_id(
                "ncenreported",
                [DIAGNOSTIC_REPORTED_FAMILY_VERSION, DIAGNOSTIC_NAME_NORMALIZER_VERSION, canonical_key],
            )
            if self.label_id != expected_label:
                raise NcenError("reported_family_label_id_mismatch")
        elif (
            self.answer != "N"
            or self.name_raw is not None
            or self.name_key is not None
        ):
            raise NcenError("reported_family_standalone_invalid")
        else:
            expected_label = _diagnostic_id(
                "ncenstandalone", [DIAGNOSTIC_REPORTED_FAMILY_VERSION, self.cik]
            )
            if self.label_id != expected_label:
                raise NcenError("reported_family_label_id_mismatch")


def _unknown_reported_family(
    cik: str,
    source_row_ids: tuple[str, ...],
    *reasons: str,
    answer: str | None = None,
) -> ReportedFamily:
    return ReportedFamily(
        cik=cik,
        state="unknown",
        answer=answer,
        name_raw=None,
        name_key=None,
        label_id=None,
        source_row_ids=source_row_ids,
        normalizer=DIAGNOSTIC_NAME_NORMALIZER_VERSION,
        reasons=tuple(sorted(set(reasons))),
    )


def reported_family_for(selection: DiagnosticSelection) -> ReportedFamily:
    """Derive one reported assertion from per-copy B.5 witnesses, never provider edges."""
    rows = tuple(row for row in selection.rows if row.role == "b5")
    source_ids = tuple(row.source_row_id for row in rows)
    if not rows:
        return _unknown_reported_family(selection.cik, (), "diagnostic_b5_missing")
    if any(row.attestation != "attested" for row in rows):
        return _unknown_reported_family(selection.cik, source_ids, "diagnostic_b5_unattested")
    answers = {row.answer_raw for row in rows}
    if None in answers:
        return _unknown_reported_family(selection.cik, source_ids, "diagnostic_b5_answer_unavailable")
    if any(answer not in {"Y", "N"} for answer in answers):
        return _unknown_reported_family(selection.cik, source_ids, "diagnostic_b5_answer_unparseable")
    if len(answers) != 1:
        return _unknown_reported_family(selection.cik, source_ids, "diagnostic_b5_answer_conflict")
    answer = next(iter(answers))
    if answer == "N":
        if any(normalize_reported_name_key(row.name_raw) is not None for row in rows):
            return _unknown_reported_family(
                selection.cik, source_ids, "diagnostic_b5_answer_name_conflict", answer="N"
            )
        return ReportedFamily(
            cik=selection.cik,
            state="standalone",
            answer="N",
            name_raw=None,
            name_key=None,
            label_id=_diagnostic_id(
                "ncenstandalone", [DIAGNOSTIC_REPORTED_FAMILY_VERSION, selection.cik]
            ),
            source_row_ids=source_ids,
            normalizer=DIAGNOSTIC_NAME_NORMALIZER_VERSION,
            reasons=(),
        )
    keys = tuple(normalize_reported_name_key(row.name_raw) for row in rows)
    if any(key is None for key in keys):
        return _unknown_reported_family(
            selection.cik, source_ids, "diagnostic_b5_name_missing", answer="Y"
        )
    unique_keys = set(keys)
    if len(unique_keys) != 1:
        return _unknown_reported_family(
            selection.cik, source_ids, "diagnostic_b5_copy_key_conflict", answer="Y"
        )
    key = next(iter(unique_keys))
    assert key is not None
    raw_values = {row.name_raw for row in rows}
    return ReportedFamily(
        cik=selection.cik,
        state="declared_family",
        answer="Y",
        name_raw=next(iter(raw_values)) if len(raw_values) == 1 else None,
        name_key=key,
        label_id=_diagnostic_id(
            "ncenreported",
            [DIAGNOSTIC_REPORTED_FAMILY_VERSION, DIAGNOSTIC_NAME_NORMALIZER_VERSION, key],
        ),
        source_row_ids=source_ids,
        normalizer=DIAGNOSTIC_NAME_NORMALIZER_VERSION,
        reasons=(),
    )


@dataclass(frozen=True, slots=True)
class DependenceNode:
    context_id: str
    cik: str
    evidence_state: str
    reasons: tuple[str, ...]
    reported_family: ReportedFamily | None

    def __post_init__(self) -> None:
        _diagnostic_context_id_valid(self.context_id)
        _diagnostic_normalized_cik(self.cik)
        if self.evidence_state not in {"complete", "incomplete"}:
            raise NcenError("evidence_state_invalid")
        _diagnostic_reasons(self.reasons)
        if self.evidence_state == "complete" and self.reasons:
            raise NcenError("complete_node_has_reasons")
        if self.evidence_state == "incomplete" and not self.reasons:
            raise NcenError("incomplete_node_missing_reason")
        if self.reported_family is not None and self.reported_family.cik != self.cik:
            raise NcenError("node_reported_family_cik_mismatch")

    @property
    def independent_vote_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class TypedIncidence:
    context_id: str
    cik: str
    accession_number: str
    series_id: str | None
    series_scope: str
    kind: str
    role: str
    identifier_kind: str | None
    identifier_value: str | None
    key_id: str | None
    source_row_id: str
    attestation: str
    reasons: tuple[str, ...]
    uncertain_expansion_eligible: bool

    def __post_init__(self) -> None:
        _diagnostic_context_id_valid(self.context_id)
        _diagnostic_normalized_cik(self.cik)
        _diagnostic_nonempty(self.accession_number, "accession_number")
        if self.series_scope not in {"series", "registrant", "unresolved"}:
            raise NcenError("series_scope_invalid")
        if self.role in {"underwriter", "b5"}:
            if self.series_scope != "registrant" or self.series_id is not None:
                raise NcenError("registrant_role_scope_invalid")
        elif self.series_scope == "registrant":
            raise NcenError("adviser_registrant_scope_invalid")
        elif self.series_scope == "series":
            if self.series_id is None or not is_series_key(self.series_id):
                raise NcenError("series_id_not_normalized")
        elif self.series_id is not None:
            raise NcenError("unresolved_scope_has_series")
        if self.kind not in {"provider", "b5_declared_name"}:
            raise NcenError("incidence_kind_invalid")
        if self.role not in DIAGNOSTIC_ROLES:
            raise NcenError("diagnostic_role_invalid")
        if self.attestation not in {"attested", "uncertain"}:
            raise NcenError("attestation_invalid")
        _diagnostic_reasons(self.reasons)
        if _DIAGNOSTIC_ROW_ID.fullmatch(self.source_row_id) is None:
            raise NcenError("source_row_id_invalid")
        if self.uncertain_expansion_eligible and self.attestation != "uncertain":
            raise NcenError("uncertain_expansion_on_attested_incidence")
        if self.key_id is None:
            if (
                self.kind != "provider"
                or self.identifier_kind is not None
                or self.identifier_value is not None
                or self.attestation != "uncertain"
                or self.uncertain_expansion_eligible
                or not self.reasons
            ):
                raise NcenError("null_key_incidence_invalid")
        elif self.kind == "provider":
            if self.role not in DIAGNOSTIC_PROVIDER_ROLES or self.identifier_kind is None or self.identifier_value is None:
                raise NcenError("provider_incidence_invalid")
            if self.key_id != diagnostic_provider_key_id(self.identifier_kind, self.identifier_value):
                raise NcenError("provider_key_id_mismatch")
        else:
            if self.role != "b5" or self.identifier_kind != "name" or self.identifier_value is None:
                raise NcenError("b5_incidence_invalid")
            canonical_key = _diagnostic_canonical_reported_name_key(self.identifier_value)
            object.__setattr__(self, "identifier_value", canonical_key)
            if self.key_id != diagnostic_b5_key_id(canonical_key):
                raise NcenError("b5_key_id_mismatch")

    @classmethod
    def provider(
        cls,
        *,
        context_id: str,
        cik: str,
        accession_number: str,
        role: str,
        identifier_kind: str,
        identifier_value: str,
        source_row_id: str,
        series_id: str | None,
        series_scope: str,
        attestation: str = "attested",
        reasons: tuple[str, ...] = (),
        uncertain_expansion_eligible: bool = False,
    ) -> TypedIncidence:
        key_id = diagnostic_provider_key_id(identifier_kind, identifier_value)
        return cls(
            context_id,
            cik,
            accession_number,
            series_id,
            series_scope,
            "provider",
            role,
            identifier_kind,
            identifier_value,
            key_id,
            source_row_id,
            attestation,
            reasons,
            uncertain_expansion_eligible,
        )

    @property
    def incidence_id(self) -> str:
        return _diagnostic_id("ncenrow:incidence", self.identity_payload())

    def identity_payload(self) -> list[Any]:
        return [
            self.context_id,
            self.cik,
            self.accession_number,
            self.series_id,
            self.series_scope,
            self.kind,
            self.role,
            self.identifier_kind,
            self.identifier_value,
            self.key_id,
            self.source_row_id,
            self.attestation,
            list(self.reasons),
            self.uncertain_expansion_eligible,
        ]

    def structural_payload(self) -> list[Any]:
        return [self.cik, self.kind, self.role, self.key_id, self.series_id, self.series_scope]


def typed_incidences_for(
    selection: DiagnosticSelection,
    *,
    context_id: str,
    reported_family: ReportedFamily | None = None,
) -> tuple[TypedIncidence, ...]:
    """Project typed provider/B.5 incidences without treating names as provider edges."""
    _diagnostic_bound_selection_admission(selection)
    _diagnostic_context_id_valid(context_id)
    derived_family = reported_family_for(selection)
    if reported_family is not None and reported_family != derived_family:
        raise NcenError("reported_family_selection_mismatch")
    family = derived_family
    output: list[TypedIncidence] = []
    for source in selection.rows:
        if source.role == "b5":
            if family.state != "declared_family" or source.attestation != "attested":
                continue
            assert family.name_key is not None
            output.append(TypedIncidence(
                context_id=context_id,
                cik=selection.cik,
                accession_number=selection.accession_number,
                series_id=source.series_id,
                series_scope=source.series_scope,
                kind="b5_declared_name",
                role="b5",
                identifier_kind="name",
                identifier_value=family.name_key,
                key_id=diagnostic_b5_key_id(family.name_key),
                source_row_id=source.source_row_id,
                attestation="attested",
                reasons=source.reasons,
                uncertain_expansion_eligible=False,
            ))
            continue
        identifiers = source.normalized_identifiers()
        if not identifiers:
            reasons = tuple(sorted({*source.reasons, "diagnostic_provider_identifier_unavailable"}))
            output.append(TypedIncidence(
                context_id=context_id,
                cik=selection.cik,
                accession_number=selection.accession_number,
                series_id=source.series_id,
                series_scope=source.series_scope,
                kind="provider",
                role=source.role,
                identifier_kind=None,
                identifier_value=None,
                key_id=None,
                source_row_id=source.source_row_id,
                attestation="uncertain",
                reasons=reasons,
                uncertain_expansion_eligible=False,
            ))
            continue
        for identifier_kind, identifier_value in identifiers:
            output.append(TypedIncidence.provider(
                context_id=context_id,
                cik=selection.cik,
                accession_number=selection.accession_number,
                role=source.role,
                identifier_kind=identifier_kind,
                identifier_value=identifier_value,
                source_row_id=source.source_row_id,
                series_id=source.series_id,
                series_scope=source.series_scope,
                attestation=source.attestation,
                reasons=source.reasons,
                uncertain_expansion_eligible=source.uncertain_expansion_eligible,
            ))
    return tuple(sorted({item.incidence_id: item for item in output}.values(), key=lambda item: item.incidence_id))


@dataclass(frozen=True, slots=True)
class AblationSpec:
    ablation_id: str
    provider_roles: frozenset[str]
    include_b5: bool
    include_incomplete_edges: bool
    include_uncertain: bool

    def __post_init__(self) -> None:
        _diagnostic_nonempty(self.ablation_id, "ablation_id")
        if not self.provider_roles <= DIAGNOSTIC_PROVIDER_ROLES:
            raise NcenError("ablation_provider_role_invalid")


DIAGNOSTIC_ABLATIONS = (
    AblationSpec("full_observed", DIAGNOSTIC_PROVIDER_ROLES, True, True, False),
    AblationSpec("b5_only", frozenset(), True, True, False),
    AblationSpec("primary_only", frozenset({"current_primary"}), False, True, False),
    AblationSpec("sub_only", frozenset({"current_sub"}), False, True, False),
    AblationSpec(
        "terminated_only",
        frozenset({"terminated_primary", "terminated_sub"}),
        False,
        True,
        False,
    ),
    AblationSpec("underwriter_only", frozenset({"underwriter"}), False, True, False),
    AblationSpec("provider_only", DIAGNOSTIC_PROVIDER_ROLES, False, True, False),
    AblationSpec("full_without_incomplete_edges", DIAGNOSTIC_PROVIDER_ROLES, True, False, False),
    AblationSpec("uncertain_expanded", DIAGNOSTIC_PROVIDER_ROLES, True, True, True),
)


def ablation_spec(ablation_id: str) -> AblationSpec:
    for spec in DIAGNOSTIC_ABLATIONS:
        if spec.ablation_id == ablation_id:
            return spec
    raise NcenError("ablation_not_declared")


def _require_declared_ablation(spec: AblationSpec) -> None:
    if spec != ablation_spec(spec.ablation_id):
        raise NcenError("ablation_not_declared")


class _DiagnosticUnionFind:
    def __init__(self, members: Iterable[str] = ()) -> None:
        self.parent: dict[str, str] = {}
        self.size: dict[str, int] = {}
        self.minimum: dict[str, str] = {}
        for member in members:
            self.add(member)

    def add(self, member: str) -> None:
        if member not in self.parent:
            self.parent[member] = member
            self.size[member] = 1
            self.minimum[member] = member

    def find(self, member: str) -> str:
        parent = self.parent[member]
        while parent != self.parent[parent]:
            self.parent[parent] = self.parent[self.parent[parent]]
            parent = self.parent[parent]
        while member != parent:
            next_member = self.parent[member]
            self.parent[member] = parent
            member = next_member
        return parent

    def union(self, left: str, right: str) -> bool:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return False
        left_size = self.size[left_root]
        right_size = self.size[right_root]
        if left_size < right_size or (left_size == right_size and left_root > right_root):
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        self.size[left_root] += self.size.pop(right_root)
        self.minimum[left_root] = min(self.minimum[left_root], self.minimum.pop(right_root))
        return True

    def representative(self, member: str) -> str:
        return self.minimum[self.find(member)]


def snapshot_component_id(*, context_id: str, ablation_id: str, members: tuple[str, ...]) -> str:
    _diagnostic_context_id_valid(context_id)
    _diagnostic_nonempty(ablation_id, "ablation_id")
    if members != tuple(sorted(set(members))):
        raise NcenError("component_members_not_sorted_unique")
    return _diagnostic_id(
        "ncenblock",
        [DIAGNOSTIC_PURPOSE, DIAGNOSTIC_DEPENDENCE_VERSION, context_id, ablation_id, list(members)],
    )


def membership_track_id(*, ablation_id: str, members: tuple[str, ...]) -> str:
    _diagnostic_nonempty(ablation_id, "ablation_id")
    if members != tuple(sorted(set(members))):
        raise NcenError("component_members_not_sorted_unique")
    return _diagnostic_id(
        "ncentrack",
        [
            DIAGNOSTIC_PURPOSE,
            DIAGNOSTIC_DEPENDENCE_VERSION,
            DIAGNOSTIC_EDGE_VERSION,
            ablation_id,
            list(members),
        ],
    )


def observed_edge_version_id(incidences: Iterable[TypedIncidence]) -> str:
    structures = sorted(
        {tuple(item.structural_payload()) for item in incidences if item.attestation == "attested"},
        key=lambda item: _diagnostic_canonical(list(item)),
    )
    return _diagnostic_id(
        "ncenedges", [DIAGNOSTIC_EDGE_VERSION, [list(item) for item in structures]]
    )


@dataclass(frozen=True, slots=True)
class DependenceComponent:
    context_id: str
    ablation_id: str
    snapshot_component_id: str
    membership_track_id: str
    observed_edge_version_id: str
    evidence_digest: str
    members: tuple[str, ...]
    complete_count: int
    incomplete_count: int
    distinct_reported_y_keys: int
    has_unknown_dependence: bool
    independent_vote_count: None = None

    def __post_init__(self) -> None:
        _diagnostic_context_id_valid(self.context_id)
        if self.members != tuple(sorted(set(self.members))) or not self.members:
            raise NcenError("component_members_not_sorted_unique")
        for field_name in ("complete_count", "incomplete_count", "distinct_reported_y_keys"):
            _diagnostic_nonnegative_count(getattr(self, field_name), f"component_{field_name}")
        if self.complete_count + self.incomplete_count != len(self.members):
            raise NcenError("component_completeness_count_mismatch")
        if self.distinct_reported_y_keys > len(self.members):
            raise NcenError("component_reported_key_count_invalid")
        _diagnostic_hash_valid(self.evidence_digest, "component_evidence_digest")
        if self.snapshot_component_id != snapshot_component_id(
            context_id=self.context_id, ablation_id=self.ablation_id, members=self.members
        ):
            raise NcenError("snapshot_component_id_mismatch")
        if self.membership_track_id != membership_track_id(
            ablation_id=self.ablation_id, members=self.members
        ):
            raise NcenError("membership_track_id_mismatch")
        if re.fullmatch(r"ncenedges:[0-9a-f]{64}", self.observed_edge_version_id) is None:
            raise NcenError("observed_edge_version_id_invalid")
        if self.independent_vote_count is not None:
            raise NcenError("diagnostic_independent_vote_forbidden")

    @property
    def member_count(self) -> int:
        return len(self.members)


@dataclass(frozen=True, slots=True)
class KeyDegree:
    key_id: str
    kind: str
    distinct_registrants: int
    complete_registrants: int
    incomplete_registrants: int
    incidence_count: int

    def __post_init__(self) -> None:
        if re.fullmatch(r"ncenkey:[0-9a-f]{64}", self.key_id) is None:
            raise NcenError("key_degree_key_id_invalid")
        if self.kind not in {"provider", "b5_declared_name"}:
            raise NcenError("key_degree_kind_invalid")
        for field_name in (
            "distinct_registrants",
            "complete_registrants",
            "incomplete_registrants",
            "incidence_count",
        ):
            _diagnostic_nonnegative_count(getattr(self, field_name), f"key_degree_{field_name}")
        if self.distinct_registrants != self.complete_registrants + self.incomplete_registrants:
            raise NcenError("key_degree_completeness_mismatch")
        if self.incidence_count < self.distinct_registrants:
            raise NcenError("key_degree_count_invalid")


@dataclass(frozen=True, slots=True)
class SpanningUnion:
    key_id: str
    left_cik: str
    right_cik: str
    left_incidence_id: str
    right_incidence_id: str

    def __post_init__(self) -> None:
        if re.fullmatch(r"ncenkey:[0-9a-f]{64}", self.key_id) is None:
            raise NcenError("spanning_key_id_invalid")
        _diagnostic_normalized_cik(self.left_cik)
        _diagnostic_normalized_cik(self.right_cik)
        if self.left_cik == self.right_cik:
            raise NcenError("spanning_self_union")
        if any(
            re.fullmatch(r"ncenrow:incidence:[0-9a-f]{64}", value) is None
            for value in (self.left_incidence_id, self.right_incidence_id)
        ):
            raise NcenError("spanning_incidence_id_invalid")


@dataclass(frozen=True, slots=True)
class DependenceProjection:
    context_id: str
    ablation_id: str
    nodes: tuple[DependenceNode, ...]
    incidences: tuple[TypedIncidence, ...]
    enabled_incidences: tuple[TypedIncidence, ...]
    components: tuple[DependenceComponent, ...]
    key_degrees: tuple[KeyDegree, ...]
    spanning_unions: tuple[SpanningUnion, ...]
    union_attempts: int
    successful_unions: int
    uncertain_incidence_count: int
    excluded_incidence_count: int
    _admission: _DiagnosticContextAdmission | None = field(default=None, init=False, repr=False, compare=False)

    def __getstate__(self) -> list[Any]:
        return [None if item.name == "_admission" else getattr(self, item.name)
                for item in dataclasses.fields(self)]

    def __post_init__(self) -> None:
        _diagnostic_context_id_valid(self.context_id)
        ablation_spec(self.ablation_id)
        for field_name in (
            "union_attempts",
            "successful_unions",
            "uncertain_incidence_count",
            "excluded_incidence_count",
        ):
            _diagnostic_nonnegative_count(getattr(self, field_name), f"projection_{field_name}")
        node_ciks = tuple(node.cik for node in self.nodes)
        if node_ciks != tuple(sorted(set(node_ciks))):
            raise NcenError("projection_nodes_not_sorted_unique")
        if {cik for component in self.components for cik in component.members} != set(node_ciks):
            raise NcenError("projection_membership_mismatch")
        if len(self.spanning_unions) != self.successful_unions:
            raise NcenError("projection_spanning_count_mismatch")
        if self.successful_unions != len(self.nodes) - len(self.components):
            raise NcenError("projection_spanning_not_forest")
        if self.union_attempts < self.successful_unions:
            raise NcenError("projection_union_count_invalid")
        if self.union_attempts != sum(
            max(degree.distinct_registrants - 1, 0) for degree in self.key_degrees
        ):
            raise NcenError("projection_union_attempt_count_mismatch")
        if self.uncertain_incidence_count != sum(
            incidence.attestation == "uncertain" for incidence in self.enabled_incidences
        ):
            raise NcenError("projection_uncertain_count_mismatch")
        if self.excluded_incidence_count != len(self.incidences) - len(self.enabled_incidences):
            raise NcenError("projection_excluded_count_mismatch")

    @property
    def complete_total(self) -> int:
        return sum(component.complete_count for component in self.components)

    @property
    def complete_square_sum(self) -> int:
        return sum(component.complete_count**2 for component in self.components)

    @property
    def largest_all(self) -> int:
        return max((component.member_count for component in self.components), default=0)

    @property
    def largest_complete(self) -> int:
        return max((component.complete_count for component in self.components), default=0)

    def canonical_bytes(self) -> bytes:
        return _diagnostic_canonical({
            "ablation_id": self.ablation_id,
            "components": [
                {
                    "complete_count": item.complete_count,
                    "distinct_reported_y_keys": item.distinct_reported_y_keys,
                    "evidence_digest": item.evidence_digest,
                    "has_unknown_dependence": item.has_unknown_dependence,
                    "incomplete_count": item.incomplete_count,
                    "members": list(item.members),
                    "membership_track_id": item.membership_track_id,
                    "observed_edge_version_id": item.observed_edge_version_id,
                    "snapshot_component_id": item.snapshot_component_id,
                }
                for item in self.components
            ],
            "context_id": self.context_id,
            "enabled_incidence_ids": [item.incidence_id for item in self.enabled_incidences],
            "excluded_incidence_count": self.excluded_incidence_count,
            "incidence_ids": [item.incidence_id for item in self.incidences],
            "key_degrees": [dataclasses.asdict(item) for item in self.key_degrees],
            "nodes": [
                {
                    "cik": item.cik,
                    "evidence_state": item.evidence_state,
                    "reasons": list(item.reasons),
                    "reported_family_label": None
                    if item.reported_family is None
                    else item.reported_family.label_id,
                }
                for item in self.nodes
            ],
            "spanning_unions": [dataclasses.asdict(item) for item in self.spanning_unions],
            "successful_unions": self.successful_unions,
            "uncertain_incidence_count": self.uncertain_incidence_count,
            "union_attempts": self.union_attempts,
        })


def _incidence_enabled(
    incidence: TypedIncidence,
    node: DependenceNode,
    spec: AblationSpec,
) -> bool:
    if incidence.key_id is None:
        return False
    if incidence.kind == "b5_declared_name":
        if not spec.include_b5:
            return False
    elif incidence.role not in spec.provider_roles:
        return False
    if node.evidence_state == "incomplete" and not spec.include_incomplete_edges:
        return False
    if incidence.attestation == "attested":
        return True
    return spec.include_uncertain and incidence.uncertain_expansion_eligible


def project_dependence(
    nodes: Iterable[DependenceNode],
    incidences: Iterable[TypedIncidence],
    *,
    spec: AblationSpec,
    admission: DiagnosticContext | None = None,
) -> DependenceProjection:
    """Deterministic bipartite closure with O(incidences) union attempts, never a clique."""
    _require_declared_ablation(spec)
    bound = _diagnostic_bound_context_admission(admission)
    supplied_nodes = tuple(nodes)
    supplied_incidences = tuple(incidences)
    if any(type(item) is not DependenceNode for item in supplied_nodes) or any(
        type(item) is not TypedIncidence for item in supplied_incidences
    ):
        raise NcenError("diagnostic_graph_carrier_type_invalid")
    ordered_nodes = tuple(sorted(supplied_nodes, key=lambda item: item.cik))
    if not ordered_nodes:
        raise NcenError("diagnostic_nodes_empty")
    if len({node.cik for node in ordered_nodes}) != len(ordered_nodes):
        raise NcenError("diagnostic_node_duplicate")
    context_ids = {node.context_id for node in ordered_nodes}
    if len(context_ids) != 1:
        raise NcenError("diagnostic_context_mixed")
    context_id = next(iter(context_ids))
    if context_id != bound.context_id or _diagnostic_nodes_digest(ordered_nodes) != bound.nodes_digest:
        raise NcenError("diagnostic_graph_node_set_mismatch")
    by_cik = {node.cik: node for node in ordered_nodes}
    incidence_by_id: dict[str, TypedIncidence] = {}
    for incidence in supplied_incidences:
        if incidence.context_id != context_id:
            raise NcenError("incidence_context_mismatch")
        if incidence.cik not in by_cik:
            raise NcenError("incidence_node_missing")
        existing = incidence_by_id.setdefault(incidence.incidence_id, incidence)
        if existing != incidence:
            raise NcenError("incidence_identity_conflict")
    ordered_incidences = tuple(incidence_by_id[key] for key in sorted(incidence_by_id))
    if (
        len(supplied_incidences) != len(ordered_incidences)
        or tuple(item.incidence_id for item in ordered_incidences) != bound.incidence_ids
        or _diagnostic_incidences_digest(ordered_incidences) != bound.incidences_digest
    ):
        raise NcenError("diagnostic_graph_incidence_set_mismatch")
    ordered_nodes = bound.nodes
    ordered_incidences = bound.incidences
    enabled = tuple(
        incidence
        for incidence in ordered_incidences
        if _incidence_enabled(incidence, by_cik[incidence.cik], spec)
    )
    by_key: dict[str, list[TypedIncidence]] = defaultdict(list)
    for incidence in enabled:
        assert incidence.key_id is not None
        by_key[incidence.key_id].append(incidence)

    union = _DiagnosticUnionFind(by_cik)
    spanning: list[SpanningUnion] = []
    degrees: list[KeyDegree] = []
    union_attempts = 0
    successful_unions = 0
    for key_id in sorted(by_key):
        key_incidences = sorted(by_key[key_id], key=lambda item: (item.cik, item.incidence_id))
        witness_by_cik: dict[str, TypedIncidence] = {}
        for incidence in key_incidences:
            witness_by_cik.setdefault(incidence.cik, incidence)
        members = sorted(witness_by_cik)
        complete = sum(by_cik[cik].evidence_state == "complete" for cik in members)
        degrees.append(KeyDegree(
            key_id=key_id,
            kind=key_incidences[0].kind,
            distinct_registrants=len(members),
            complete_registrants=complete,
            incomplete_registrants=len(members) - complete,
            incidence_count=len(key_incidences),
        ))
        if len(members) < 2:
            continue
        left = members[0]
        for right in members[1:]:
            union_attempts += 1
            if not union.union(left, right):
                continue
            successful_unions += 1
            spanning.append(SpanningUnion(
                key_id=key_id,
                left_cik=left,
                right_cik=right,
                left_incidence_id=witness_by_cik[left].incidence_id,
                right_incidence_id=witness_by_cik[right].incidence_id,
            ))

    members_by_root: dict[str, list[str]] = defaultdict(list)
    for cik in by_cik:
        members_by_root[union.find(cik)].append(cik)
    incidences_by_root: dict[str, list[TypedIncidence]] = defaultdict(list)
    for incidence in enabled:
        incidences_by_root[union.find(incidence.cik)].append(incidence)
    uncertain_ciks = {
        incidence.cik for incidence in ordered_incidences if incidence.attestation == "uncertain"
    }
    components: list[DependenceComponent] = []
    for root, raw_members in members_by_root.items():
        members = tuple(sorted(raw_members))
        component_incidences = tuple(incidences_by_root[root])
        complete_count = sum(by_cik[cik].evidence_state == "complete" for cik in members)
        reported_keys = {
            node.reported_family.name_key
            for cik in members
            if (node := by_cik[cik]).reported_family is not None
            and node.reported_family.state == "declared_family"
            and node.reported_family.name_key is not None
        }
        components.append(DependenceComponent(
            context_id=context_id,
            ablation_id=spec.ablation_id,
            snapshot_component_id=snapshot_component_id(
                context_id=context_id, ablation_id=spec.ablation_id, members=members
            ),
            membership_track_id=membership_track_id(
                ablation_id=spec.ablation_id, members=members
            ),
            observed_edge_version_id=observed_edge_version_id(component_incidences),
            evidence_digest=_diagnostic_hash([
                context_id,
                spec.ablation_id,
                sorted(item.incidence_id for item in component_incidences),
            ]),
            members=members,
            complete_count=complete_count,
            incomplete_count=len(members) - complete_count,
            distinct_reported_y_keys=len(reported_keys),
            has_unknown_dependence=any(
                by_cik[cik].evidence_state == "incomplete" for cik in members
            ) or bool(set(members) & uncertain_ciks),
        ))
    components.sort(key=lambda item: item.members)
    spanning.sort(key=lambda item: (item.key_id, item.left_cik, item.right_cik))
    projection = DependenceProjection(
        context_id=context_id,
        ablation_id=spec.ablation_id,
        nodes=ordered_nodes,
        incidences=ordered_incidences,
        enabled_incidences=enabled,
        components=tuple(components),
        key_degrees=tuple(degrees),
        spanning_unions=tuple(spanning),
        union_attempts=union_attempts,
        successful_unions=successful_unions,
        uncertain_incidence_count=sum(item.attestation == "uncertain" for item in enabled),
        excluded_incidence_count=len(ordered_incidences) - len(enabled),
    )
    object.__setattr__(projection, "_admission", bound)
    return projection


def diagnostic_context_id(
    *,
    report_date: dt.date,
    knowledge_cutoff: dt.datetime,
    mode: str,
    inventory_digest: str,
    cohort_digest: str,
    ncen_evidence_digest: str,
    exclusion_ledger_digest: str,
) -> str:
    if not isinstance(report_date, dt.date) or isinstance(report_date, dt.datetime):
        raise NcenError("report_date_invalid")
    cutoff = _diagnostic_timestamp(knowledge_cutoff)
    if mode not in KNOWLEDGE_MODES:
        raise NcenError("knowledge_mode_invalid")
    _diagnostic_nonempty(inventory_digest, "inventory_digest")
    _diagnostic_hash_valid(cohort_digest, "cohort_digest")
    _diagnostic_hash_valid(ncen_evidence_digest, "ncen_evidence_digest")
    _diagnostic_hash_valid(exclusion_ledger_digest, "exclusion_ledger_digest")
    return _diagnostic_id(
        "ncenctx",
        [
            DIAGNOSTIC_SCHEMA_VERSION,
            {
                "reported_family": DIAGNOSTIC_REPORTED_FAMILY_VERSION,
                "reporting_dependence_block": DIAGNOSTIC_DEPENDENCE_VERSION,
            },
            DIAGNOSTIC_EDGE_VERSION,
            DIAGNOSTIC_SELECTION_VERSION,
            DIAGNOSTIC_NAME_NORMALIZER_VERSION,
            report_date.isoformat(),
            cutoff,
            mode,
            inventory_digest,
            cohort_digest,
            ncen_evidence_digest,
            exclusion_ledger_digest,
        ],
    )


def fold_scope_id(
    *,
    mode: str,
    ablation_id: str,
    contexts: tuple[tuple[dt.date, dt.datetime, str, str], ...],
) -> str:
    if mode not in KNOWLEDGE_MODES:
        raise NcenError("knowledge_mode_invalid")
    ablation_spec(ablation_id)
    if not contexts:
        raise NcenError("fold_contexts_empty")
    ordered = []
    previous: tuple[str, str] | None = None
    for report_date, cutoff, inventory_digest, context_id in contexts:
        _diagnostic_context_id_valid(context_id)
        _diagnostic_nonempty(inventory_digest, "inventory_digest")
        item = (report_date.isoformat(), _diagnostic_timestamp(cutoff))
        if previous is not None and item <= previous:
            raise NcenError("fold_contexts_not_strictly_ordered")
        previous = item
        ordered.append([item[0], item[1], inventory_digest, context_id])
    return _diagnostic_id(
        "ncenfoldscope",
        [
            DIAGNOSTIC_PURPOSE,
            DIAGNOSTIC_DEPENDENCE_VERSION,
            DIAGNOSTIC_EDGE_VERSION,
            mode,
            ablation_id,
            ordered,
            DIAGNOSTIC_FOLD_PROTOCOL_VERSION,
        ],
    )


def fold_group_id(*, scope_id: str, members: tuple[str, ...]) -> str:
    if re.fullmatch(r"ncenfoldscope:[0-9a-f]{64}", scope_id) is None:
        raise NcenError("fold_scope_id_invalid")
    if members != tuple(sorted(set(members))) or not members:
        raise NcenError("fold_members_not_sorted_unique")
    return _diagnostic_id("ncenfold", [scope_id, list(members)])


@dataclass(frozen=True, slots=True)
class DependenceSnapshot:
    report_date: dt.date
    knowledge_cutoff: dt.datetime
    mode: str
    inventory_digest: str
    projection: DependenceProjection

    def __post_init__(self) -> None:
        bound = _diagnostic_bound_projection_admission(self.projection)
        if (self.report_date != bound.report_date or self.knowledge_cutoff != bound.knowledge_cutoff
                or self.mode != bound.mode or self.inventory_digest != bound.inventory_digest):
            raise NcenError("diagnostic_projection_unadmitted")
        if not isinstance(self.report_date, dt.date) or isinstance(self.report_date, dt.datetime):
            raise NcenError("report_date_invalid")
        _diagnostic_timestamp(self.knowledge_cutoff)
        if self.mode not in KNOWLEDGE_MODES:
            raise NcenError("knowledge_mode_invalid")
        _diagnostic_nonempty(self.inventory_digest, "inventory_digest")
        _diagnostic_context_id_valid(self.projection.context_id)
        ablation_spec(self.projection.ablation_id)


@dataclass(frozen=True, slots=True)
class FoldContext:
    report_date: dt.date
    knowledge_cutoff: dt.datetime
    inventory_digest: str
    context_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.report_date, dt.date) or isinstance(self.report_date, dt.datetime):
            raise NcenError("report_date_invalid")
        _diagnostic_timestamp(self.knowledge_cutoff)
        _diagnostic_nonempty(self.inventory_digest, "inventory_digest")
        _diagnostic_context_id_valid(self.context_id)

    def identity_tuple(self) -> tuple[dt.date, dt.datetime, str, str]:
        return (self.report_date, self.knowledge_cutoff, self.inventory_digest, self.context_id)


@dataclass(frozen=True, slots=True)
class TemporalFoldScope:
    mode: str
    ablation_id: str
    contexts: tuple[FoldContext, ...]

    def __post_init__(self) -> None:
        if self.mode not in KNOWLEDGE_MODES:
            raise NcenError("knowledge_mode_invalid")
        ablation_spec(self.ablation_id)
        fold_scope_id(
            mode=self.mode,
            ablation_id=self.ablation_id,
            contexts=tuple(context.identity_tuple() for context in self.contexts),
        )

    @property
    def fold_scope_id(self) -> str:
        return fold_scope_id(
            mode=self.mode,
            ablation_id=self.ablation_id,
            contexts=tuple(context.identity_tuple() for context in self.contexts),
        )


@dataclass(frozen=True, slots=True)
class TemporalFoldGroup:
    fold_scope_id: str
    fold_group_id: str
    members: tuple[str, ...]
    has_unknown_dependence: bool
    usable_for_independence_claim: bool = False

    def __post_init__(self) -> None:
        if type(self.has_unknown_dependence) is not bool or type(
            self.usable_for_independence_claim
        ) is not bool:
            raise NcenError("fold_group_boolean_invalid")
        if self.usable_for_independence_claim:
            raise NcenError("fold_independence_claim_forbidden")
        if self.fold_group_id != fold_group_id(scope_id=self.fold_scope_id, members=self.members):
            raise NcenError("fold_group_id_mismatch")

    @property
    def member_count(self) -> int:
        return len(self.members)


@dataclass(frozen=True, slots=True)
class TemporalDependenceUnion:
    fold_scope_id: str
    groups: tuple[TemporalFoldGroup, ...]
    incidence_count: int
    union_attempts: int
    successful_unions: int

    def __post_init__(self) -> None:
        if re.fullmatch(r"ncenfoldscope:[0-9a-f]{64}", self.fold_scope_id) is None:
            raise NcenError("fold_scope_id_invalid")
        for field_name in ("incidence_count", "union_attempts", "successful_unions"):
            _diagnostic_nonnegative_count(getattr(self, field_name), f"temporal_{field_name}")
        if any(type(group) is not TemporalFoldGroup or group.fold_scope_id != self.fold_scope_id
               for group in self.groups):
            raise NcenError("temporal_fold_group_scope_mismatch")
        ordered_groups = tuple(sorted(self.groups, key=lambda group: group.members))
        if self.groups != ordered_groups:
            raise NcenError("temporal_fold_groups_not_sorted")
        members = tuple(member for group in self.groups for member in group.members)
        if len(members) != len(set(members)):
            raise NcenError("temporal_fold_membership_duplicate")
        if self.incidence_count != self.union_attempts:
            raise NcenError("temporal_union_attempt_count_mismatch")
        if self.successful_unions > self.union_attempts:
            raise NcenError("temporal_union_count_invalid")


def _diagnostic_fold_edge(attestation: str, key_id: str | None, enabled: bool) -> bool:
    return enabled and key_id is not None and attestation == "attested"


def build_temporal_dependence_union(
    snapshot_stream: Iterable[DependenceSnapshot],
    *,
    fold_scope: TemporalFoldScope,
) -> TemporalDependenceUnion:
    """Union CIK-key incidence edges over the complete declared context schedule."""
    union = _DiagnosticUnionFind()
    ciks: set[str] = set()
    tainted_ciks: set[str] = set()
    incidence_count = 0
    union_attempts = 0
    successful_unions = 0
    snapshots = iter(snapshot_stream)
    for expected_context in fold_scope.contexts:
        try:
            snapshot = next(snapshots)
        except StopIteration as exc:
            raise NcenError("fold_context_schedule_mismatch") from exc
        actual_context = FoldContext(
            snapshot.report_date,
            snapshot.knowledge_cutoff,
            snapshot.inventory_digest,
            snapshot.projection.context_id,
        )
        if actual_context != expected_context:
            raise NcenError("fold_context_schedule_mismatch")
        if (
            snapshot.mode != fold_scope.mode
            or snapshot.projection.ablation_id != fold_scope.ablation_id
        ):
            raise NcenError("fold_mode_or_ablation_mismatch")
        for node in snapshot.projection.nodes:
            vertex = f"cik:{node.cik}"
            union.add(vertex)
            ciks.add(node.cik)
            if node.evidence_state == "incomplete":
                tainted_ciks.add(node.cik)
        for incidence in snapshot.projection.enabled_incidences:
            if not _diagnostic_fold_edge(incidence.attestation, incidence.key_id, True):
                continue
            cik_vertex = f"cik:{incidence.cik}"
            key_vertex = f"key:{incidence.key_id}"
            union.add(cik_vertex)
            union.add(key_vertex)
            incidence_count += 1
            union_attempts += 1
            successful_unions += union.union(cik_vertex, key_vertex)
        for incidence in snapshot.projection.incidences:
            if incidence.attestation == "uncertain":
                tainted_ciks.add(incidence.cik)
    try:
        next(snapshots)
    except StopIteration:
        pass
    else:
        raise NcenError("fold_context_schedule_mismatch")

    by_root: dict[str, list[str]] = defaultdict(list)
    for cik in sorted(ciks):
        by_root[union.find(f"cik:{cik}")].append(cik)
    scope_id = fold_scope.fold_scope_id
    groups = []
    for raw_members in by_root.values():
        members = tuple(sorted(raw_members))
        groups.append(TemporalFoldGroup(
            fold_scope_id=scope_id,
            fold_group_id=fold_group_id(scope_id=scope_id, members=members),
            members=members,
            has_unknown_dependence=bool(set(members) & tainted_ciks),
        ))
    groups.sort(key=lambda item: item.members)
    return TemporalDependenceUnion(
        fold_scope_id=scope_id,
        groups=tuple(groups),
        incidence_count=incidence_count,
        union_attempts=union_attempts,
        successful_unions=successful_unions,
    )


@dataclass(frozen=True, slots=True)
class DependenceTransition:
    ablation_id: str
    from_context_id: str
    to_context_id: str
    from_component_id: str | None
    to_component_id: str | None
    intersection_count: int
    union_count: int
    kind: str

    def __post_init__(self) -> None:
        _diagnostic_nonnegative_count(self.intersection_count, "transition_intersection_count")
        _diagnostic_nonnegative_count(self.union_count, "transition_union_count")
        if self.kind not in {"overlap", "entry", "exit"}:
            raise NcenError("transition_kind_invalid")
        if self.intersection_count <= 0 or self.union_count < self.intersection_count:
            raise NcenError("transition_count_invalid")


def build_dependence_transitions(
    snapshots: Iterable[DependenceSnapshot],
) -> tuple[DependenceTransition, ...]:
    """All consecutive component overlaps plus cohort entries/exits using one CIK join."""
    iterator = iter(snapshots)
    try:
        previous = next(iterator)
    except StopIteration:
        return ()
    output: list[DependenceTransition] = []
    for current in iterator:
        if previous.mode != current.mode or previous.projection.ablation_id != current.projection.ablation_id:
            raise NcenError("transition_mode_or_ablation_mismatch")
        if (current.report_date, current.knowledge_cutoff) <= (
            previous.report_date,
            previous.knowledge_cutoff,
        ):
            raise NcenError("transition_context_order_invalid")
        previous_members = {
            component.snapshot_component_id: set(component.members)
            for component in previous.projection.components
        }
        current_members = {
            component.snapshot_component_id: set(component.members)
            for component in current.projection.components
        }
        previous_by_cik = {
            cik: component_id for component_id, members in previous_members.items() for cik in members
        }
        current_by_cik = {
            cik: component_id for component_id, members in current_members.items() for cik in members
        }
        intersections: Counter[tuple[str, str]] = Counter()
        for cik in previous_by_cik.keys() & current_by_cik.keys():
            intersections[(previous_by_cik[cik], current_by_cik[cik])] += 1
        for (left_id, right_id), count in sorted(intersections.items()):
            output.append(DependenceTransition(
                ablation_id=current.projection.ablation_id,
                from_context_id=previous.projection.context_id,
                to_context_id=current.projection.context_id,
                from_component_id=left_id,
                to_component_id=right_id,
                intersection_count=count,
                union_count=len(previous_members[left_id]) + len(current_members[right_id]) - count,
                kind="overlap",
            ))
        exited: Counter[str] = Counter(
            previous_by_cik[cik] for cik in previous_by_cik.keys() - current_by_cik.keys()
        )
        for component_id, count in sorted(exited.items()):
            output.append(DependenceTransition(
                ablation_id=current.projection.ablation_id,
                from_context_id=previous.projection.context_id,
                to_context_id=current.projection.context_id,
                from_component_id=component_id,
                to_component_id=None,
                intersection_count=count,
                union_count=len(previous_members[component_id]),
                kind="exit",
            ))
        entered: Counter[str] = Counter(
            current_by_cik[cik] for cik in current_by_cik.keys() - previous_by_cik.keys()
        )
        for component_id, count in sorted(entered.items()):
            output.append(DependenceTransition(
                ablation_id=current.projection.ablation_id,
                from_context_id=previous.projection.context_id,
                to_context_id=current.projection.context_id,
                from_component_id=None,
                to_component_id=component_id,
                intersection_count=count,
                union_count=len(current_members[component_id]),
                kind="entry",
            ))
        previous = current
    return tuple(output)


@dataclass(frozen=True, slots=True)
class BondIssuerGroupRef:
    purpose: str = "bond_issuer_group"
    state: str = "unavailable"
    reason: str = "light_a1_inventory_not_bound"
    authority: str = "Light A1"
    source_module: str = "backend/app/bond_optimizer/issuer_groups.py"
    inventory_digest: None = None
    policy_digest: None = None
    as_of: None = None
    temporal_qualification: str = "NOT_EVALUABLE"

    def __post_init__(self) -> None:
        expected = (
            "bond_issuer_group",
            "unavailable",
            "light_a1_inventory_not_bound",
            "Light A1",
            "backend/app/bond_optimizer/issuer_groups.py",
            None,
            None,
            None,
            "NOT_EVALUABLE",
        )
        actual = tuple(getattr(self, field.name) for field in dataclasses.fields(self))
        if actual != expected:
            raise NcenError("bond_issuer_group_ref_must_be_unavailable")


# === FE-1 diagnostic source sidecars and selection attestation ==========================
# This layer intentionally rebinds the three Stage 1 carrier names with strict additive
# fields. Their legacy constructor prefix and unbound identity payload stay unchanged.

#: v2 (F6): the quarantine ledger binds a concrete acquisition seal (``acquisition_root``) whose
#: exact derived membership must equal the declared ledger and boundary examples.
DIAGNOSTIC_SOURCE_MANIFEST_VERSION = "ncen_diagnostic_source_manifest_v2"
DIAGNOSTIC_QUARANTINE_LEDGER_VERSION = "ncen_diagnostic_quarantine_ledger_v1"
DIAGNOSTIC_SOURCE_ATTESTATION_VERSION = "ncen_diagnostic_source_attestation_v1"
DIAGNOSTIC_ACQUISITION_SCOPE_SHA256 = (
    "0ea76623093bde880b014dcd0e02c7338eb9a7c02f2d3ba2e1c43a748915e5fb"
)
DIAGNOSTIC_ACQUISITION_SHA256SUMS_SHA256 = (
    "c567c21a1d60895c3759a14976e13c3becf2f81410e9b3daf43a336e82d8dc91"
)
_DIAGNOSTIC_SOURCE_KINDS = frozenset({"synthetic", "dera", "edgar_xml", "header", "index"})
_DIAGNOSTIC_SOURCE_ROLES = frozenset({*DIAGNOSTIC_ROLES, "header", "index"})
_DIAGNOSTIC_QUARANTINE_CLASSES = frozenset({
    "absent_amended_accession",
    "schema_unavailable",
    "projection_conflict",
})
_DIAGNOSTIC_MANIFEST_KINDS = frozenset({"sealed_source", "synthetic_fixture"})
_DIAGNOSTIC_DERA_REQUIRED_COLUMNS = {
    **REQUIRED_COLUMNS,
    "ADVISER": (*REQUIRED_COLUMNS["ADVISER"], "ADVISER_NAME"),
    "PRINCIPAL_UNDERWRITER": (
        *REQUIRED_COLUMNS["PRINCIPAL_UNDERWRITER"],
        "UNDERWRITER_NAME",
    ),
}


def _diagnostic_optional_timestamp(value: dt.datetime | None) -> str | None:
    return None if value is None else _diagnostic_timestamp(value)


def _diagnostic_optional_date(value: dt.date | None) -> str | None:
    return None if value is None else value.isoformat()


def _diagnostic_parse_timestamp(value: Any, field_name: str) -> dt.datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise NcenError(f"{field_name}_invalid")
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise NcenError(f"{field_name}_invalid") from exc
    if _diagnostic_timestamp(parsed) != value:
        raise NcenError(f"{field_name}_invalid")
    return parsed


def _diagnostic_parse_date(value: Any, field_name: str) -> dt.date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise NcenError(f"{field_name}_invalid")
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise NcenError(f"{field_name}_invalid") from exc


def _diagnostic_exact_keys(value: Any, expected: set[str], context: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise NcenError(f"{context}_keys_invalid")
    return value


def _diagnostic_positive_int(value: Any, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise NcenError(f"{field_name}_invalid")
    return value


def _diagnostic_manifest_relative_path(root: Path, raw: Any) -> tuple[str, Path]:
    if not isinstance(raw, str) or not raw or "\\" in raw:
        raise NcenError("diagnostic_artifact_path_unsafe")
    relative = Path(raw)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise NcenError("diagnostic_artifact_path_unsafe")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise NcenError("diagnostic_artifact_symlink_forbidden")
    try:
        resolved = current.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
    except (FileNotFoundError, ValueError) as exc:
        raise NcenError("diagnostic_artifact_path_unsafe") from exc
    if not resolved.is_file():
        raise NcenError("diagnostic_artifact_not_regular_file")
    return relative.as_posix(), resolved


# --- F13a: verified artifact descriptors and one shared bounded resource monitor ------------
# The source loader never keeps a dictionary of raw artifact bytes. Each artifact is admitted
# through a held descriptor (hash and size against the external manifest pin, no retention);
# afterwards artifacts are loaded one at a time: headers and XML as bounded inline bytes that
# are dropped after their parse, DERA ZIPs as a private spool whose bytes are exactly the bytes
# re-hashed from the held source descriptor. Every read, parse and spill step checks one shared
# monitor. The 8 GiB hard cap is an in-process checkpoint, never a supervisor-enforced limit.
# F13b: DERA TSV members stream one bounded raw line at a time into one disposable per-package
# SQLite join store in the private spill root; accessions are projected one at a time from
# indexed lookups. No whole TSV table and no whole-package legacy parse is held in memory.

_DIAGNOSTIC_MIB = 1024 * 1024
_DIAGNOSTIC_GIB = 1024 * _DIAGNOSTIC_MIB
_DIAGNOSTIC_HARD_CAP_ENFORCEMENT = "in_process_checkpoints_only"
#: Work checked only at entry and exit (no interval checks inside), disclosed in every report.
_DIAGNOSTIC_UNMONITORED_SCOPES = (
    "read_diagnostic_acquisition_ledger_entry_exit_only",
    "frozen_dera_per_accession_projection_entry_exit_only",
    "source_index_issuance_entry_exit_only",
)


@dataclass(frozen=True, slots=True)
class DiagnosticResourceLimits:
    """Bounded loader resources; soft and hard RSS both refuse typed at checkpoints.

    ``hash_block_bytes`` bounds bytes between checks (at most 1 MiB) and ``rows_per_check``
    bounds streamed rows between checks (at most 1,024). Spill is one private ZIP spool plus one
    per-package join store at a time, together capped by ``spill_budget_bytes`` and
    ``spill_min_free_bytes`` of remaining capacity. ``tsv_line_max_bytes`` (at most 1 MiB)
    bounds one raw DERA TSV data line, terminator included; a longer line is refused, never
    truncated. ``join_cache_bytes`` (at most 64 MiB) caps the join store's page cache.
    """

    rss_soft_bytes: int = 7 * _DIAGNOSTIC_GIB + 512 * _DIAGNOSTIC_MIB
    rss_hard_bytes: int = 8 * _DIAGNOSTIC_GIB
    hash_block_bytes: int = _DIAGNOSTIC_MIB
    rows_per_check: int = 1024
    spill_budget_bytes: int = 8 * _DIAGNOSTIC_GIB
    spill_min_free_bytes: int = _DIAGNOSTIC_GIB
    manifest_max_bytes: int = 256 * _DIAGNOSTIC_MIB
    ledger_max_bytes: int = 256 * _DIAGNOSTIC_MIB
    #: Same bound as the acquisition reader's raw header role.
    header_max_bytes: int = 16 * _DIAGNOSTIC_MIB
    #: Same bound as W1's ``safe_xml_root``.
    xml_max_bytes: int = nport.MAX_XML_BYTES
    tsv_line_max_bytes: int = _DIAGNOSTIC_MIB
    join_cache_bytes: int = 8 * _DIAGNOSTIC_MIB
    spill_dir: Path | None = None

    def __post_init__(self) -> None:
        sizes = (
            self.rss_soft_bytes,
            self.rss_hard_bytes,
            self.hash_block_bytes,
            self.rows_per_check,
            self.spill_budget_bytes,
            self.manifest_max_bytes,
            self.ledger_max_bytes,
            self.header_max_bytes,
            self.xml_max_bytes,
            self.tsv_line_max_bytes,
            self.join_cache_bytes,
        )
        if (
            any(not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in sizes)
            or not isinstance(self.spill_min_free_bytes, int)
            or isinstance(self.spill_min_free_bytes, bool)
            or self.spill_min_free_bytes < 0
            or self.rss_soft_bytes > self.rss_hard_bytes
            or self.hash_block_bytes > _DIAGNOSTIC_MIB
            or self.rows_per_check > 1024
            or self.tsv_line_max_bytes > _DIAGNOSTIC_MIB
            or self.join_cache_bytes > 64 * _DIAGNOSTIC_MIB
        ):
            raise NcenError("diagnostic_resource_limits_invalid")
        if self.spill_dir is not None:
            spill = Path(self.spill_dir)
            if not spill.is_absolute():
                raise NcenError("diagnostic_resource_limits_invalid")
            object.__setattr__(self, "spill_dir", spill)

    def as_record(self) -> dict[str, Any]:
        return {
            "rss_soft_bytes": self.rss_soft_bytes,
            "rss_hard_bytes": self.rss_hard_bytes,
            "hash_block_bytes": self.hash_block_bytes,
            "rows_per_check": self.rows_per_check,
            "spill_budget_bytes": self.spill_budget_bytes,
            "spill_min_free_bytes": self.spill_min_free_bytes,
            "manifest_max_bytes": self.manifest_max_bytes,
            "ledger_max_bytes": self.ledger_max_bytes,
            "header_max_bytes": self.header_max_bytes,
            "xml_max_bytes": self.xml_max_bytes,
            "tsv_line_max_bytes": self.tsv_line_max_bytes,
            "join_cache_bytes": self.join_cache_bytes,
            "spill_dir": None if self.spill_dir is None else str(self.spill_dir),
        }


def _diagnostic_default_rss_probe() -> tuple[str, Any] | None:
    """Current-process resident set probe: Windows working set, Linux statm, else peak RSS."""
    import os
    import sys

    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class _Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        # A private library instance: never rebinds the shared ``ctypes.windll`` prototypes.
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        current_process = kernel32.GetCurrentProcess
        current_process.restype = wintypes.HANDLE
        current_process.argtypes = []
        query = kernel32.K32GetProcessMemoryInfo
        query.restype = wintypes.BOOL
        query.argtypes = [wintypes.HANDLE, ctypes.POINTER(_Counters), wintypes.DWORD]

        def windows_probe() -> int:
            counters = _Counters()
            counters.cb = ctypes.sizeof(_Counters)
            if not query(current_process(), ctypes.byref(counters), counters.cb):
                raise NcenError("diagnostic_resource_rss_probe_failed")
            return int(counters.WorkingSetSize)

        return "windows_working_set", windows_probe
    statm = Path("/proc/self/statm")
    if statm.is_file():
        page = int(os.sysconf("SC_PAGE_SIZE"))

        def statm_probe() -> int:
            return int(statm.read_bytes().split()[1]) * page

        return "linux_statm_resident", statm_probe
    try:
        import resource
    except ImportError:
        return None
    scale = 1 if sys.platform == "darwin" else 1024

    def peak_probe() -> int:
        # Peak resident size: a conservative upper bound of the current resident size.
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * scale

    return "getrusage_peak_resident", peak_probe


class DiagnosticResourceMonitor:
    """One shared, measured resource monitor for the diagnostic source loader.

    ``check`` reads the process RSS and refuses with ``diagnostic_resource_rss_hard_limit_
    exceeded:<phase>`` or ``..._soft_limit_exceeded:<phase>``; callers never drop rows or
    members to continue. The hard cap is enforced only at in-process checkpoints; no supervisor
    limit is claimed (``report()["supervisor_hard_cap"]`` is ``False``). ``rss_probe`` and
    ``disk_free_probe`` may be injected for deterministic refusal tests. Every counter is
    instrumented at the acquisition/release points, not asserted by construction.
    """

    def __init__(
        self,
        limits: DiagnosticResourceLimits | None = None,
        *,
        rss_probe: Any = None,
        disk_free_probe: Any = None,
    ) -> None:
        if limits is None:
            limits = DiagnosticResourceLimits()
        if not isinstance(limits, DiagnosticResourceLimits):
            raise TypeError("limits_must_be_DiagnosticResourceLimits")
        if rss_probe is None:
            resolved = _diagnostic_default_rss_probe()
            if resolved is None:
                raise NcenError("diagnostic_resource_rss_probe_unavailable")
            probe_name, rss_probe = resolved
        elif callable(rss_probe):
            probe_name = "injected"
        else:
            raise TypeError("rss_probe_must_be_callable")
        if disk_free_probe is not None and not callable(disk_free_probe):
            raise TypeError("disk_free_probe_must_be_callable")
        self.limits = limits
        self.phase: str | None = None
        self.artifact_id: str | None = None
        self._rss_probe = rss_probe
        self._probe_name = probe_name
        self._disk_free_probe = disk_free_probe
        self._phases: set[str] = set()
        self._log: list[dict[str, Any]] = []
        self._loaded: Counter[str] = Counter()
        self._resident: dict[str, int] = {}
        self._in_memory = 0
        self._bytes_since_check = 0
        self._rows_since_check = 0
        self._counters: Counter[str] = Counter()

    # -- checkpoints ---------------------------------------------------------------------------
    def check(self, phase: str, artifact_id: str | None = None) -> None:
        self.phase = phase
        if artifact_id is not None:
            self.artifact_id = artifact_id
        self._phases.add(phase)
        counters = self._counters
        counters["max_bytes_between_checks"] = max(counters["max_bytes_between_checks"], self._bytes_since_check)
        counters["max_rows_between_checks"] = max(counters["max_rows_between_checks"], self._rows_since_check)
        self._bytes_since_check = 0
        self._rows_since_check = 0
        rss = self._rss_probe()
        if not isinstance(rss, int) or isinstance(rss, bool) or rss < 0:
            raise NcenError("diagnostic_resource_rss_probe_invalid")
        counters["rss_checks"] += 1
        counters["rss_max_observed"] = max(counters["rss_max_observed"], rss)
        if rss > self.limits.rss_hard_bytes:
            raise NcenError(f"diagnostic_resource_rss_hard_limit_exceeded:{phase}")
        if rss > self.limits.rss_soft_bytes:
            raise NcenError(f"diagnostic_resource_rss_soft_limit_exceeded:{phase}")

    def bytes_read(self, count: int, phase: str) -> None:
        """Account one bounded read (at most ``hash_block_bytes``) and check."""
        self._counters["hash_blocks"] += 1
        self.bytes_processed(count, phase)

    def bytes_processed(self, count: int, phase: str) -> None:
        """Account streamed bytes and check without labeling non-hash rows as hash blocks."""
        self._bytes_since_check += count
        self._counters["bytes_streamed"] += count
        self.check(phase)

    def rows_read(self, count: int, phase: str) -> None:
        """Account streamed rows; check whenever ``rows_per_check`` rows have accumulated."""
        self._rows_since_check += count
        self._counters["rows_streamed"] += count
        if self._rows_since_check >= self.limits.rows_per_check:
            self.check(phase)

    def clear_artifact(self) -> None:
        self.artifact_id = None

    # -- handles, raw artifacts and spill ------------------------------------------------------
    def handle_opened(self) -> None:
        counters = self._counters
        counters["handles_opened"] += 1
        counters["handles_open_max"] = max(
            counters["handles_open_max"], counters["handles_opened"] - counters["handles_closed"]
        )

    def handle_closed(self) -> None:
        self._counters["handles_closed"] += 1

    def acquire_raw(self, artifact_id: str, size: int, *, in_memory: bool) -> None:
        """Enforce one raw artifact (inline bytes or its private spool) at a time."""
        if self._resident:
            raise NcenError("diagnostic_raw_artifact_overlap")
        self._resident[artifact_id] = size if in_memory else 0
        self._in_memory += self._resident[artifact_id]
        counters = self._counters
        counters["raw_artifacts_resident_max"] = max(counters["raw_artifacts_resident_max"], len(self._resident))
        counters["raw_bytes_in_memory_max"] = max(counters["raw_bytes_in_memory_max"], self._in_memory)

    def release_raw(self, artifact_id: str) -> None:
        size = self._resident.pop(artifact_id, None)
        if size is None:
            raise NcenError("diagnostic_raw_artifact_release_unknown")
        self._in_memory -= size

    def disk_free(self, path: Path) -> int:
        import shutil

        try:
            free = shutil.disk_usage(path).free if self._disk_free_probe is None else self._disk_free_probe(path)
        except OSError as exc:
            raise NcenError("diagnostic_spill_disk_probe_failed") from exc
        if not isinstance(free, int) or isinstance(free, bool) or free < 0:
            raise NcenError("diagnostic_spill_disk_probe_invalid")
        return free

    def spool_dir_created(self) -> None:
        self._counters["spool_dirs_created"] += 1

    def spool_dir_removed(self) -> None:
        self._counters["spool_dirs_removed"] += 1

    def spool_file_created(self) -> None:
        self._counters["spool_files_created"] += 1

    def spool_file_written(self, size: int) -> None:
        self._counters["spool_bytes_max"] = max(self._counters["spool_bytes_max"], size)

    def spool_file_removed(self) -> None:
        self._counters["spool_files_removed"] += 1

    # -- F13b: bounded TSV rows and the per-package join store ---------------------------------
    def tsv_line(self, size: int) -> None:
        self._counters["tsv_line_bytes_max"] = max(self._counters["tsv_line_bytes_max"], size)

    def tsv_rows_resident(self, count: int) -> None:
        """Record how many TSV-derived row records are resident in Python at once."""
        self._counters["tsv_rows_resident_max"] = max(self._counters["tsv_rows_resident_max"], count)

    def join_store_created(self) -> None:
        self._counters["join_stores_created"] += 1

    def join_store_removed(self) -> None:
        self._counters["join_stores_removed"] += 1

    def join_store_size(self, pages: int, page_size: int) -> None:
        counters = self._counters
        counters["join_store_pages_max"] = max(counters["join_store_pages_max"], pages)
        counters["join_store_bytes_max"] = max(counters["join_store_bytes_max"], pages * page_size)

    def join_plan_checked(self) -> None:
        self._counters["join_plans_checked"] += 1

    # -- provenance ---------------------------------------------------------------------------
    def descriptor_built(self) -> None:
        self._counters["descriptors_built"] += 1

    def log_artifact(self, stage: str, descriptor: _DiagnosticArtifactDescriptor) -> None:
        self._log.append({
            "stage": stage,
            "ordinal": descriptor.ordinal,
            "artifact_id": descriptor.artifact_id,
            "kind": descriptor.kind,
            "sha256": descriptor.sha256,
            "size": descriptor.size,
        })
        if stage == "load":
            self._loaded[descriptor.kind] += 1

    def released(self) -> bool:
        counters = self._counters
        return (
            not self._resident
            and counters["handles_opened"] == counters["handles_closed"]
            and counters["join_stores_created"] == counters["join_stores_removed"]
        )

    def report(self) -> dict[str, Any]:
        counters = self._counters
        names = (
            "rss_checks",
            "rss_max_observed",
            "bytes_streamed",
            "hash_blocks",
            "max_bytes_between_checks",
            "rows_streamed",
            "max_rows_between_checks",
            "descriptors_built",
            "raw_artifacts_resident_max",
            "raw_bytes_in_memory_max",
            "handles_opened",
            "handles_closed",
            "handles_open_max",
            "spool_files_created",
            "spool_files_removed",
            "spool_bytes_max",
            "spool_dirs_created",
            "spool_dirs_removed",
            "tsv_line_bytes_max",
            "tsv_rows_resident_max",
            "join_stores_created",
            "join_stores_removed",
            "join_store_pages_max",
            "join_store_bytes_max",
            "join_plans_checked",
        )
        return {
            **{name: counters[name] for name in names},
            "handles_open": counters["handles_opened"] - counters["handles_closed"],
            "raw_artifacts_resident": len(self._resident),
            "raw_bytes_in_memory": self._in_memory,
            "artifacts_loaded": dict(sorted(self._loaded.items())),
            "artifact_log": [dict(entry) for entry in self._log],
            "phases_checked": sorted(self._phases),
            "limits": self.limits.as_record(),
            "rss_probe": self._probe_name,
            "supervisor_hard_cap": False,
            "hard_cap_enforcement": _DIAGNOSTIC_HARD_CAP_ENFORCEMENT,
            "unmonitored_scopes": list(_DIAGNOSTIC_UNMONITORED_SCOPES),
        }


@dataclass(frozen=True, slots=True)
class _DiagnosticArtifactDescriptor:
    """One admitted manifest artifact: contained path plus its pinned hash, size and identity."""

    ordinal: int
    artifact: Mapping[str, Any]
    artifact_id: str
    kind: str
    path_label: str
    path: Path
    sha256: str
    size: int
    identity: tuple[int, int]


def _diagnostic_open_held(
    path: Path,
    label: str,
    monitor: DiagnosticResourceMonitor,
    *,
    identity: tuple[int, int] | None = None,
    buffered: bool = False,
) -> tuple[Any, tuple[int, int], int]:
    """Open one regular file through a held descriptor that must be the linked path itself."""
    import os
    import stat as stat_module

    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise NcenError(f"{label}_open_failed") from exc
    monitor.handle_opened()
    try:
        held = os.fstat(descriptor)
        linked = os.lstat(path)
        if not stat_module.S_ISREG(held.st_mode) or (held.st_ino, held.st_dev) != (linked.st_ino, linked.st_dev):
            raise NcenError(f"{label}_descriptor_mismatch")
        held_identity = (held.st_dev, held.st_ino)
        if identity is not None and held_identity != identity:
            raise NcenError(f"{label}_descriptor_changed")
        handle = os.fdopen(descriptor, "rb", buffering=-1 if buffered else 0)
    except BaseException:
        os.close(descriptor)
        monitor.handle_closed()
        raise
    return handle, held_identity, held.st_size


def _diagnostic_stream_file(
    path: Path,
    *,
    label: str,
    expected_sha256: str,
    expected_size: int,
    monitor: DiagnosticResourceMonitor,
    phase: str,
    identity: tuple[int, int] | None = None,
    sink: Any = None,
) -> tuple[int, int]:
    """Hash one file in bounded blocks from a held descriptor, handing the same blocks to ``sink``.

    Returns the held ``(st_dev, st_ino)``. The digest and size are checked after the stream, so
    a sink must treat its bytes as unverified until this function returns.
    """
    handle, held_identity, size = _diagnostic_open_held(path, label, monitor, identity=identity)
    digest = hashlib.sha256()
    total = 0
    try:
        if size != expected_size:
            raise NcenError(f"{label}_size_mismatch")
        while chunk := handle.read(monitor.limits.hash_block_bytes):
            total += len(chunk)
            if total > expected_size:
                raise NcenError(f"{label}_size_mismatch")
            digest.update(chunk)
            if sink is not None:
                sink(chunk)
            monitor.bytes_read(len(chunk), phase)
    finally:
        handle.close()
        monitor.handle_closed()
    if total != expected_size:
        raise NcenError(f"{label}_size_mismatch")
    if digest.hexdigest() != expected_sha256:
        raise NcenError(f"{label}_sha256_mismatch")
    return held_identity


def _diagnostic_verify_file(
    path: Path,
    *,
    expected_sha256: str,
    expected_size: int,
    label: str,
    monitor: DiagnosticResourceMonitor | None = None,
    max_bytes: int | None = None,
    phase: str = "control_read",
) -> bytes:
    """Bounded bytes of one pinned control file (manifest or ledger), hashed as read."""
    _diagnostic_hash_valid(expected_sha256, f"{label}_sha256")
    active = DiagnosticResourceMonitor() if monitor is None else monitor
    if max_bytes is not None and expected_size > max_bytes:
        raise NcenError(f"{label}_too_large")
    active.check(phase)
    chunks: list[bytes] = []
    _diagnostic_stream_file(
        path,
        label=label,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
        monitor=active,
        phase=phase,
        sink=chunks.append,
    )
    return b"".join(chunks)


def _diagnostic_admit_artifact(
    ordinal: int,
    artifact: Mapping[str, Any],
    path_label: str,
    path: Path,
    size: int,
    monitor: DiagnosticResourceMonitor,
) -> _DiagnosticArtifactDescriptor:
    """Preflight one artifact: hash and size against the pin through a held descriptor only."""
    sha256 = artifact["sha256"]
    if not isinstance(sha256, str):
        raise NcenError("diagnostic_artifact_sha256_invalid")
    _diagnostic_hash_valid(sha256, "diagnostic_artifact_sha256")
    kind = artifact["kind"]
    limits = monitor.limits
    if kind == "header" and size > limits.header_max_bytes:
        raise NcenError("diagnostic_artifact_inline_too_large:header")
    if kind == "edgar_xml" and size > limits.xml_max_bytes:
        raise NcenError("diagnostic_artifact_inline_too_large:edgar_xml")
    if kind == "dera_zip" and size > limits.spill_budget_bytes:
        raise NcenError("diagnostic_spill_budget_exceeded")
    monitor.check("preflight_hash", artifact["artifact_id"])
    identity = _diagnostic_stream_file(
        path,
        label="diagnostic_artifact",
        expected_sha256=sha256,
        expected_size=size,
        monitor=monitor,
        phase="preflight_hash",
    )
    descriptor = _DiagnosticArtifactDescriptor(
        ordinal, artifact, artifact["artifact_id"], kind, path_label, path, sha256, size, identity
    )
    monitor.descriptor_built()
    monitor.log_artifact("preflight", descriptor)
    monitor.clear_artifact()
    return descriptor


def _diagnostic_load_inline(
    descriptor: _DiagnosticArtifactDescriptor, monitor: DiagnosticResourceMonitor, phase: str
) -> bytes:
    """Re-hash one small artifact from its held descriptor and return exactly those bytes.

    The caller owns one raw-artifact slot until it calls ``monitor.release_raw``.
    """
    monitor.acquire_raw(descriptor.artifact_id, descriptor.size, in_memory=True)
    try:
        monitor.check("artifact_load", descriptor.artifact_id)
        chunks: list[bytes] = []
        _diagnostic_stream_file(
            descriptor.path,
            label="diagnostic_artifact",
            expected_sha256=descriptor.sha256,
            expected_size=descriptor.size,
            monitor=monitor,
            phase=phase,
            identity=descriptor.identity,
            sink=chunks.append,
        )
        data = b"".join(chunks)
        del chunks
    except BaseException:
        monitor.release_raw(descriptor.artifact_id)
        raise
    monitor.log_artifact("load", descriptor)
    return data


class _DiagnosticSpool:
    """One private spill directory holding at most one verified DERA ZIP copy at a time."""

    def __init__(self, monitor: DiagnosticResourceMonitor) -> None:
        self._monitor = monitor
        self._root: Path | None = None

    def _base(self) -> Path:
        import tempfile

        base = self._monitor.limits.spill_dir
        return Path(tempfile.gettempdir()) if base is None else base

    def _ensure_root(self, base: Path) -> Path:
        import tempfile

        if self._root is None:
            try:
                created = tempfile.mkdtemp(prefix="ncen-diagnostic-spool-", dir=str(base))
            except OSError as exc:
                raise NcenError("diagnostic_spill_io_failed") from exc
            self._root = Path(created)
            self._monitor.spool_dir_created()
        return self._root

    def write(self, descriptor: _DiagnosticArtifactDescriptor) -> Path:
        """Copy the held source stream into a new private spool; bytes equal the pinned hash."""
        import os

        monitor = self._monitor
        limits = monitor.limits
        if descriptor.size > limits.spill_budget_bytes:
            raise NcenError("diagnostic_spill_budget_exceeded")
        base = self._base()
        if monitor.disk_free(base) < descriptor.size + limits.spill_min_free_bytes:
            raise NcenError("diagnostic_spill_disk_insufficient")
        monitor.acquire_raw(descriptor.artifact_id, descriptor.size, in_memory=False)
        spool_path: Path | None = None
        try:
            monitor.check("artifact_load", descriptor.artifact_id)
            root = self._ensure_root(base)
            spool_path = root / f"artifact-{descriptor.ordinal:06d}.zip"
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
            try:
                spool_descriptor = os.open(spool_path, flags, 0o600)
            except OSError as exc:
                spool_path = None
                raise NcenError("diagnostic_spill_io_failed") from exc
            monitor.spool_file_created()
            monitor.handle_opened()
            written = 0
            try:

                def sink(chunk: bytes) -> None:
                    nonlocal written
                    view = memoryview(chunk)
                    while view:
                        try:
                            count = os.write(spool_descriptor, view)
                        except OSError as exc:
                            raise NcenError("diagnostic_spill_io_failed") from exc
                        view = view[count:]
                    written += len(chunk)
                    monitor.spool_file_written(written)

                _diagnostic_stream_file(
                    descriptor.path,
                    label="diagnostic_artifact",
                    expected_sha256=descriptor.sha256,
                    expected_size=descriptor.size,
                    monitor=monitor,
                    phase="spool_write",
                    identity=descriptor.identity,
                    sink=sink,
                )
            finally:
                os.close(spool_descriptor)
                monitor.handle_closed()
        except BaseException:
            try:
                self._remove(spool_path)
            finally:
                monitor.release_raw(descriptor.artifact_id)
            raise
        monitor.log_artifact("load", descriptor)
        return spool_path

    def _remove(self, spool_path: Path | None) -> None:
        import os

        if spool_path is None:
            return
        try:
            os.remove(spool_path)
        except OSError as exc:
            raise NcenError("diagnostic_spill_io_failed") from exc
        self._monitor.spool_file_removed()

    def discard(self, descriptor: _DiagnosticArtifactDescriptor, spool_path: Path) -> None:
        try:
            self._remove(spool_path)
        finally:
            self._monitor.release_raw(descriptor.artifact_id)

    def join_store_path(self, descriptor: _DiagnosticArtifactDescriptor) -> Path:
        """Path for one package's disposable join store inside this run's private spill root."""
        return self._ensure_root(self._base()) / f"join-{descriptor.ordinal:06d}.sqlite3"

    def close(self) -> None:
        import shutil

        if self._root is None:
            return
        root, self._root = self._root, None
        shutil.rmtree(root, ignore_errors=True)
        if root.exists():
            raise NcenError("diagnostic_spill_cleanup_failed")
        self._monitor.spool_dir_removed()


@dataclass(frozen=True, slots=True)
class DiagnosticSourceManifestPin:
    manifest_path: Path
    manifest_sha256: str
    manifest_size: int

    def __post_init__(self) -> None:
        path = Path(self.manifest_path)
        if path.is_symlink() or not path.is_absolute():
            raise NcenError("diagnostic_manifest_path_unsafe")
        _diagnostic_hash_valid(self.manifest_sha256, "diagnostic_manifest_sha256")
        _diagnostic_positive_int(self.manifest_size, "diagnostic_manifest_size")
        object.__setattr__(self, "manifest_path", path)


@dataclass(frozen=True, slots=True)
class DiagnosticSourceExclusion:
    accession_number: str
    registrant_cik: str | None
    form_type: str | None
    report_period_end: dt.date | None
    acceptance_at: dt.datetime | None
    classification: str
    reasons: tuple[str, ...]
    ledger_sha256: str
    ledger_size: int
    locator: str

    def __post_init__(self) -> None:
        if _ACCESSION.fullmatch(self.accession_number) is None:
            raise NcenError("diagnostic_exclusion_accession_invalid")
        if self.registrant_cik is not None:
            _diagnostic_normalized_cik(self.registrant_cik)
        if self.classification not in _DIAGNOSTIC_QUARANTINE_CLASSES:
            raise NcenError("diagnostic_quarantine_class_invalid")
        _diagnostic_reasons(self.reasons)
        if not self.reasons:
            raise NcenError("diagnostic_exclusion_reason_missing")
        _diagnostic_hash_valid(self.ledger_sha256, "diagnostic_ledger_sha256")
        _diagnostic_positive_int(self.ledger_size, "diagnostic_ledger_size")
        _diagnostic_nonempty(self.locator, "diagnostic_exclusion_locator")


@dataclass(frozen=True, slots=True)
class DiagnosticBoundaryExample:
    accession_number: str
    schema_version: str
    form_type: str
    acceptance_at: dt.datetime
    policy_state: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if _ACCESSION.fullmatch(self.accession_number) is None:
            raise NcenError("diagnostic_boundary_accession_invalid")
        if self.schema_version != "X0505" or self.form_type != AMENDMENT_FORM:
            raise NcenError("diagnostic_boundary_schema_invalid")
        _diagnostic_timestamp(self.acceptance_at)
        if self.policy_state != "example_only":
            raise NcenError("diagnostic_boundary_policy_invalid")
        _diagnostic_reasons(self.reasons)
        if not self.reasons:
            raise NcenError("diagnostic_boundary_reason_missing")


@dataclass(frozen=True, slots=True)
class DiagnosticSourceRow:  # noqa: F811 - append-only Stage 2a carrier extension
    accession_number: str
    registrant_cik: str
    role: str
    locator: str
    series_id: str | None
    series_scope: str
    answer_raw: str | None = None
    name_raw: str | None = None
    file_number_raw: str | None = None
    crd_raw: str | None = None
    lei_raw: str | None = None
    attestation: str = "attested"
    reasons: tuple[str, ...] = ()
    uncertain_expansion_eligible: bool = False
    source_kind: str = "synthetic"
    artifact_id: str | None = None
    artifact_path: str | None = None
    artifact_sha256: str | None = None
    artifact_size: int | None = None
    member_path: str | None = None
    raw_row_sha256: str | None = None
    supporting_rows: tuple[tuple[str, str], ...] = ()
    source_copy_id: str | None = None
    projection_digest: str | None = None
    schema_version: str | None = None
    form_type: str | None = None
    report_period_end: dt.date | None = None
    public_at: dt.datetime | None = None
    data_known_at: dt.datetime | None = None
    retrieved_at: dt.datetime | None = None
    acceptance_at: dt.datetime | None = None
    header_source_id: str | None = None
    name_state: str | None = None
    custody_state: str = "verified"

    def __post_init__(self) -> None:
        if _ACCESSION.fullmatch(self.accession_number) is None:
            raise NcenError("accession_number_invalid")
        _diagnostic_normalized_cik(self.registrant_cik)
        if self.role not in _DIAGNOSTIC_SOURCE_ROLES:
            raise NcenError("diagnostic_role_invalid")
        _diagnostic_nonempty(self.locator, "locator")
        if self.series_scope not in {"series", "registrant", "unresolved"}:
            raise NcenError("series_scope_invalid")
        if self.role in {"underwriter", "b5", "header", "index"}:
            if self.series_scope != "registrant" or self.series_id is not None:
                raise NcenError("registrant_role_scope_invalid")
        elif self.series_scope == "registrant":
            raise NcenError("adviser_registrant_scope_invalid")
        elif self.series_scope == "series":
            if self.series_id is None or not is_series_key(self.series_id):
                raise NcenError("series_id_not_normalized")
        elif self.series_id is not None:
            raise NcenError("unresolved_scope_has_series")
        if self.answer_raw is not None and not isinstance(self.answer_raw, str):
            raise NcenError("diagnostic_b5_answer_invalid")
        if self.role != "b5" and self.answer_raw is not None:
            raise NcenError("provider_row_has_b5_answer")
        if self.attestation not in {"attested", "uncertain"}:
            raise NcenError("attestation_invalid")
        _diagnostic_reasons(self.reasons)
        if self.attestation == "uncertain" and not self.reasons:
            raise NcenError("uncertain_row_missing_reason")
        if self.uncertain_expansion_eligible and self.attestation != "uncertain":
            raise NcenError("uncertain_expansion_on_attested_row")
        if self.source_kind not in _DIAGNOSTIC_SOURCE_KINDS:
            raise NcenError("diagnostic_source_kind_invalid")
        derived_name_state = "present" if self.name_raw not in {None, ""} else "absent"
        if self.name_state is None:
            object.__setattr__(self, "name_state", derived_name_state)
        elif self.name_state not in {"present", "absent", "unavailable"}:
            raise NcenError("diagnostic_name_state_invalid")
        if self.custody_state not in {"verified", "quarantined", "unavailable"}:
            raise NcenError("diagnostic_custody_state_invalid")
        for value in (self.public_at, self.data_known_at, self.retrieved_at, self.acceptance_at):
            if value is not None:
                _diagnostic_timestamp(value)
        if self.source_kind != "synthetic":
            if (
                self.artifact_id is None
                or self.artifact_path is None
                or self.artifact_sha256 is None
                or self.artifact_size is None
                or self.raw_row_sha256 is None
                or self.source_copy_id is None
            ):
                raise NcenError("diagnostic_bound_source_incomplete")
            _diagnostic_nonempty(self.artifact_id, "artifact_id")
            _diagnostic_nonempty(self.artifact_path, "artifact_path")
            _diagnostic_hash_valid(self.artifact_sha256, "artifact_sha256")
            _diagnostic_positive_int(self.artifact_size, "artifact_size")
            _diagnostic_hash_valid(self.raw_row_sha256, "raw_row_sha256")
            if not self.source_copy_id.startswith("ncencopy:"):
                raise NcenError("diagnostic_source_copy_id_invalid")
            _diagnostic_hash_valid(self.source_copy_id.split(":", 1)[1], "source_copy_id")
        if self.projection_digest is not None:
            _diagnostic_hash_valid(self.projection_digest, "projection_digest")
        if self.header_source_id is not None and _DIAGNOSTIC_ROW_ID.fullmatch(self.header_source_id) is None:
            raise NcenError("header_source_id_invalid")
        if self.supporting_rows != tuple(sorted(set(self.supporting_rows))):
            raise NcenError("supporting_rows_not_sorted_unique")
        for locator, digest in self.supporting_rows:
            _diagnostic_nonempty(locator, "supporting_locator")
            _diagnostic_hash_valid(digest, "supporting_row_sha256")

    @property
    def source_row_id(self) -> str:
        return _diagnostic_id("ncenrow:source_row", self.identity_payload())

    def identity_payload(self) -> list[Any]:
        legacy: list[Any] = [
            self.accession_number,
            self.registrant_cik,
            self.role,
            self.locator,
            self.series_id,
            self.series_scope,
            self.answer_raw,
            self.name_raw,
            self.file_number_raw,
            self.crd_raw,
            self.lei_raw,
            self.attestation,
            list(self.reasons),
            self.uncertain_expansion_eligible,
        ]
        if self.source_kind == "synthetic" and self.artifact_id is None:
            return legacy
        return [
            *legacy,
            DIAGNOSTIC_SOURCE_ATTESTATION_VERSION,
            self.source_kind,
            self.artifact_id,
            self.artifact_path,
            self.artifact_sha256,
            self.artifact_size,
            self.member_path,
            self.raw_row_sha256,
            [list(item) for item in self.supporting_rows],
            self.source_copy_id,
            self.projection_digest,
            self.schema_version,
            self.form_type,
            _diagnostic_optional_date(self.report_period_end),
            _diagnostic_optional_timestamp(self.public_at),
            _diagnostic_optional_timestamp(self.data_known_at),
            _diagnostic_optional_timestamp(self.retrieved_at),
            _diagnostic_optional_timestamp(self.acceptance_at),
            self.header_source_id,
            self.name_state,
            self.custody_state,
        ]

    def normalized_identifiers(self) -> tuple[tuple[str, str], ...]:
        if self.role not in DIAGNOSTIC_PROVIDER_ROLES:
            return ()
        normalized = (
            ("FN", normalize_file_number(self.file_number_raw)),
            ("CRD", normalize_crd(self.crd_raw)),
            ("LEI", normalize_lei(self.lei_raw)),
        )
        return tuple((kind, value) for kind, value in normalized if value is not None)


_DIAGNOSTIC_SOURCE_CUSTODY_VERSION = "ncen_diagnostic_source_custody_v1"
_DIAGNOSTIC_SOURCE_WORK_COUNTERS = (
    "index_passes",
    "row_ids_computed",
    "evidence_digest_builds",
    "lookup_builds",
    "accession_lookups",
    "accession_rows_visited",
    "quarantine_lookups",
    "quarantine_rows_visited",
)


class _DiagnosticSourceCustody:
    """Loader-issued custody of one verified :class:`DiagnosticSourceIndex` payload.

    Only :func:`read_diagnostic_source_rows` issues it, after verifying the pinned manifest,
    artifact, header and locator bytes; there is no constructor argument, module constant or
    serialized flag that confers it. It binds the exact row, exclusion and boundary tuples,
    the manifest identity and the row IDs, evidence digest and accession/CIK lookups computed
    once at load. It is immutable, cannot be pickled, and copies of an index never inherit it.

    This protects the supported API only: it is not a sandbox against arbitrary in-process
    reflection (``object.__new__``/``object.__setattr__``) or later OS-level file changes.
    Rows consumed by selection are re-identified against their load-time IDs.
    """

    __slots__ = (
        "_exclusions_by_accession",
        "_exclusions_by_cik",
        "_row_by_id",
        "_rows_by_accession",
        "_work",
        "acquisition_ledger",
        "boundary_examples",
        "custody_version",
        "evidence_digest",
        "exclusion_ledger_digest",
        "exclusions",
        "lane",
        "manifest_kind",
        "manifest_sha256",
        "manifest_size",
        "row_ids",
        "rows",
    )

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        # Factory-only: ``_diagnostic_issue_source_index`` is the sole creator.
        raise TypeError("diagnostic_source_custody_is_loader_issued")

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("diagnostic_source_custody_immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("diagnostic_source_custody_immutable")

    def __copy__(self) -> _DiagnosticSourceCustody:
        return self

    def __deepcopy__(self, memo: Any) -> _DiagnosticSourceCustody:
        return self

    def __reduce_ex__(self, protocol: Any) -> Any:
        raise TypeError("diagnostic_source_custody_not_serializable")

    def __reduce__(self) -> Any:
        raise TypeError("diagnostic_source_custody_not_serializable")

    @property
    def row_by_id(self) -> Mapping[str, DiagnosticSourceRow]:
        mapping: Mapping[str, DiagnosticSourceRow] = self._row_by_id
        return mapping

    def work_snapshot(self) -> dict[str, int]:
        return dict(self._work)

    def rows_with_ids_for(self, accession_number: str) -> tuple[tuple[DiagnosticSourceRow, str], ...]:
        """Rows of one accession in index order, re-identified against load-time IDs."""
        positions = self._rows_by_accession.get(accession_number, ())
        self._work["accession_lookups"] += 1
        self._work["accession_rows_visited"] += len(positions)
        output = []
        for position in positions:
            row = self.rows[position]
            row_id = self.row_ids[position]
            if row.source_row_id != row_id:
                raise NcenError("diagnostic_source_row_custody_mismatch")
            output.append((row, row_id))
        return tuple(output)

    def rows_for(self, accession_number: str) -> tuple[DiagnosticSourceRow, ...]:
        return tuple(row for row, _row_id in self.rows_with_ids_for(accession_number))

    def quarantine_candidates(self, accession_number: str, cik: str) -> tuple[DiagnosticSourceExclusion, ...]:
        """Quarantine records of the selected accession or registrant CIK, in index order."""
        positions = sorted({
            *self._exclusions_by_accession.get(accession_number, ()),
            *self._exclusions_by_cik.get(cik, ()),
        })
        self._work["quarantine_lookups"] += 1
        self._work["quarantine_rows_visited"] += len(positions)
        return tuple(self.exclusions[position] for position in positions)


@dataclass(frozen=True, slots=True)
class DiagnosticSourceIndex:  # noqa: F811 - append-only Stage 2a carrier extension
    """Source rows, quarantine exclusions and boundaries of one diagnostic source manifest.

    Public construction, :meth:`from_rows`, ``dataclasses.replace`` and copies yield a
    structural, *unbound* index: only :func:`read_diagnostic_source_rows` attaches loader
    custody, and every selection, snapshot and export entry point refuses an unbound index
    with ``diagnostic_source_index_unbound``.
    """

    rows: tuple[DiagnosticSourceRow, ...]
    exclusions: tuple[DiagnosticSourceExclusion, ...] = ()
    boundary_examples: tuple[DiagnosticBoundaryExample, ...] = ()
    manifest_sha256: str | None = None
    manifest_size: int | None = None
    manifest_kind: str | None = None
    evidence_digest: str | None = None
    _custody: _DiagnosticSourceCustody | None = field(
        default=None, init=False, repr=False, compare=False
    )

    def __getstate__(self) -> list[Any]:
        # Copies and serialized indexes are structural only; custody is never transferred.
        return [
            None if item.name == "_custody" else getattr(self, item.name)
            for item in dataclasses.fields(self)
        ]

    def __post_init__(self) -> None:
        ids = tuple(row.source_row_id for row in self.rows)
        if ids != tuple(sorted(set(ids))):
            raise NcenError("source_index_not_sorted_unique")
        exclusion_ids = tuple((item.accession_number, item.locator) for item in self.exclusions)
        if exclusion_ids != tuple(sorted(set(exclusion_ids))):
            raise NcenError("source_exclusions_not_sorted_unique")
        boundary_ids = tuple(item.accession_number for item in self.boundary_examples)
        if boundary_ids != tuple(sorted(set(boundary_ids))):
            raise NcenError("source_boundaries_not_sorted_unique")
        if self.manifest_sha256 is not None:
            _diagnostic_hash_valid(self.manifest_sha256, "diagnostic_manifest_sha256")
            if self.manifest_size is None:
                raise NcenError("diagnostic_manifest_size_missing")
            _diagnostic_positive_int(self.manifest_size, "diagnostic_manifest_size")
            if self.manifest_kind not in _DIAGNOSTIC_MANIFEST_KINDS:
                raise NcenError("diagnostic_manifest_kind_invalid")
        if self.evidence_digest is not None:
            _diagnostic_hash_valid(self.evidence_digest, "diagnostic_evidence_digest")

    @classmethod
    def from_rows(cls, rows: Iterable[DiagnosticSourceRow]) -> DiagnosticSourceIndex:
        by_id: dict[str, DiagnosticSourceRow] = {}
        for row in rows:
            existing = by_id.setdefault(row.source_row_id, row)
            if existing != row:
                raise NcenError("source_row_identity_conflict")
        return cls(tuple(by_id[key] for key in sorted(by_id)))

    def rows_for(self, accession_number: str) -> tuple[DiagnosticSourceRow, ...]:
        if self._custody is not None:
            return _diagnostic_bound_source_custody(self).rows_for(accession_number)
        return tuple(row for row in self.rows if row.accession_number == accession_number)

    def custody_lane(self) -> str:
        """Provenance lane of loader custody; raises for an unbound index."""
        lane: str = _diagnostic_bound_source_custody(self).lane
        return lane

    def work_counters(self) -> Mapping[str, int]:
        """Measured one-time load and per-lookup work of this verified handle."""
        import types

        return types.MappingProxyType(_diagnostic_bound_source_custody(self).work_snapshot())

    def exclusion_ledger_digest(self) -> str:
        """Versioned digest of the exact acquisition-derived exclusion and boundary payloads."""
        digest: str = _diagnostic_bound_source_custody(self).exclusion_ledger_digest
        return digest

    def acquisition_ledger(self) -> DiagnosticAcquisitionLedger:
        """The acquisition ledger derived from pinned custody at load; raises when unbound."""
        ledger: DiagnosticAcquisitionLedger = _diagnostic_bound_source_custody(self).acquisition_ledger
        return ledger


def _diagnostic_bound_source_custody(index: DiagnosticSourceIndex) -> _DiagnosticSourceCustody:
    """Return the loader custody of ``index`` or refuse it before any row is consumed.

    O(1): identity of the bound tuples and equality of manifest identity; no rehashing.
    """
    if type(index) is not DiagnosticSourceIndex:
        raise NcenError("diagnostic_source_index_unbound")
    custody = index._custody
    if (
        type(custody) is not _DiagnosticSourceCustody
        or index.rows is not custody.rows
        or index.exclusions is not custody.exclusions
        or index.boundary_examples is not custody.boundary_examples
        or index.manifest_sha256 != custody.manifest_sha256
        or index.manifest_size != custody.manifest_size
        or index.manifest_kind != custody.manifest_kind
        or index.evidence_digest != custody.evidence_digest
    ):
        raise NcenError("diagnostic_source_index_unbound")
    return custody


@dataclass(frozen=True, slots=True)
class DiagnosticSelection:  # noqa: F811 - append-only Stage 2a carrier extension
    cik: str
    accession_number: str | None
    rows: tuple[DiagnosticSourceRow, ...]
    evidence_state: str
    reasons: tuple[str, ...]
    selection_reason: str | None = None
    selected_projection_digest: str | None = None
    dependencies: tuple[SelectionDependency, ...] = ()
    knowledge_time: dt.datetime | None = None
    report_date: dt.date | None = None
    knowledge_cutoff: dt.datetime | None = None
    mode: str | None = None
    excluded_source_row_ids: tuple[str, ...] = ()
    _admission: _DiagnosticSelectionAdmission | None = field(default=None, init=False, repr=False, compare=False)

    def __getstate__(self) -> list[Any]:
        return [None if item.name == "_admission" else getattr(self, item.name)
                for item in dataclasses.fields(self)]

    def __post_init__(self) -> None:
        _diagnostic_normalized_cik(self.cik)
        if self.accession_number is not None and _ACCESSION.fullmatch(self.accession_number) is None:
            raise NcenError("accession_number_invalid")
        if self.evidence_state not in {"complete", "incomplete"}:
            raise NcenError("evidence_state_invalid")
        _diagnostic_reasons(self.reasons)
        if self.evidence_state == "complete" and (self.reasons or self.selection_reason is not None):
            raise NcenError("complete_node_has_reasons")
        if self.evidence_state == "incomplete" and not self.reasons:
            raise NcenError("incomplete_node_missing_reason")
        ids = tuple(row.source_row_id for row in self.rows)
        if ids != tuple(sorted(set(ids))):
            raise NcenError("selection_rows_not_sorted_unique")
        for row in self.rows:
            if row.role not in DIAGNOSTIC_ROLES:
                raise NcenError("selection_contains_nonrelationship_row")
            if row.registrant_cik != self.cik or row.accession_number != self.accession_number:
                raise NcenError("selection_source_identity_mismatch")
        if self.selected_projection_digest is not None:
            _diagnostic_hash_valid(self.selected_projection_digest, "selected_projection_digest")
        if self.knowledge_time is not None:
            _diagnostic_timestamp(self.knowledge_time)
        if self.knowledge_cutoff is not None:
            _diagnostic_timestamp(self.knowledge_cutoff)
        if self.mode is not None:
            _check_mode(self.mode)
        if self.excluded_source_row_ids != tuple(sorted(set(self.excluded_source_row_ids))):
            raise NcenError("excluded_source_rows_not_sorted_unique")

    @classmethod
    def from_rows(
        cls,
        *,
        cik: str,
        accession_number: str,
        rows: Iterable[DiagnosticSourceRow],
        evidence_state: str,
        reasons: tuple[str, ...],
    ) -> DiagnosticSelection:
        source_index = DiagnosticSourceIndex.from_rows(rows)
        return cls(cik, accession_number, source_index.rows, evidence_state, reasons)


_DIAGNOSTIC_ADMISSION_SEAL = object()
_DIAGNOSTIC_UNCERTAINTY_REASONS = frozenset({
    AMENDMENT_UNKNOWN_REASON, AMENDMENT_PARTIAL_REASON, ORDER_UNRESOLVED,
    "diagnostic_b5_copy_key_conflict", "diagnostic_provider_name_copy_conflict",
    "diagnostic_underwriter_lei_copy_conflict",
})


class _DiagnosticSelectionAdmission:
    __slots__ = ("_seal", "accession_number", "cik", "evidence_digest", "excluded_ids",
                 "fields", "knowledge_cutoff", "lane", "ledger_digest", "manifest_sha256",
                 "mode", "origin_ids", "report_date", "row_ids", "rows", "selection_digest")

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        raise TypeError("diagnostic_selection_admission_factory_only")

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("diagnostic_selection_admission_immutable")

    def __reduce_ex__(self, protocol: Any) -> Any:
        raise TypeError("diagnostic_selection_admission_not_serializable")


def _diagnostic_selection_payload(selection: DiagnosticSelection) -> list[Any]:
    return [
        DIAGNOSTIC_SELECTION_VERSION, selection.cik, selection.accession_number,
        [row.source_row_id for row in selection.rows], selection.evidence_state,
        list(selection.reasons), selection.selection_reason, selection.selected_projection_digest,
        [item.record() for item in selection.dependencies],
        _diagnostic_optional_timestamp(selection.knowledge_time),
        _diagnostic_optional_date(selection.report_date),
        _diagnostic_optional_timestamp(selection.knowledge_cutoff), selection.mode,
        list(selection.excluded_source_row_ids),
    ]


def _diagnostic_issue_selection(
    selection: DiagnosticSelection, *, lane: str, manifest_sha256: str,
    evidence_digest: str, ledger_digest: str, origin_ids: tuple[str, ...],
) -> DiagnosticSelection:
    admission = object.__new__(_DiagnosticSelectionAdmission)
    values = {
        "_seal": _DIAGNOSTIC_ADMISSION_SEAL, "lane": lane,
        "manifest_sha256": manifest_sha256, "evidence_digest": evidence_digest,
        "ledger_digest": ledger_digest, "cik": selection.cik,
        "accession_number": selection.accession_number, "report_date": selection.report_date,
        "knowledge_cutoff": selection.knowledge_cutoff, "mode": selection.mode,
        "rows": selection.rows, "row_ids": tuple(row.source_row_id for row in selection.rows),
        "origin_ids": origin_ids, "excluded_ids": selection.excluded_source_row_ids,
        "fields": tuple(getattr(selection, item.name) for item in dataclasses.fields(selection)
                        if item.name != "_admission"),
        "selection_digest": _diagnostic_hash([
            DIAGNOSTIC_ADMISSION_VERSION, lane, manifest_sha256, evidence_digest,
            ledger_digest, list(origin_ids), _diagnostic_selection_payload(selection),
        ]),
    }
    for name, value in values.items():
        object.__setattr__(admission, name, value)
    object.__setattr__(selection, "_admission", admission)
    return selection


def _diagnostic_bound_selection_admission(selection: DiagnosticSelection) -> _DiagnosticSelectionAdmission:
    if type(selection) is not DiagnosticSelection:
        raise NcenError("diagnostic_selection_unadmitted")
    bound = selection._admission
    if (
        type(bound) is not _DiagnosticSelectionAdmission
        or bound._seal is not _DIAGNOSTIC_ADMISSION_SEAL
        or selection.rows is not bound.rows
        or tuple(getattr(selection, item.name) for item in dataclasses.fields(selection)
                 if item.name != "_admission") != bound.fields
        or tuple(row.source_row_id for row in selection.rows) != bound.row_ids
        or selection.excluded_source_row_ids != bound.excluded_ids
        or selection.cik != bound.cik or selection.accession_number != bound.accession_number
        or selection.report_date != bound.report_date
        or selection.knowledge_cutoff != bound.knowledge_cutoff or selection.mode != bound.mode
        or _diagnostic_hash([
            DIAGNOSTIC_ADMISSION_VERSION, bound.lane, bound.manifest_sha256,
            bound.evidence_digest, bound.ledger_digest, list(bound.origin_ids),
            _diagnostic_selection_payload(selection),
        ]) != bound.selection_digest
    ):
        raise NcenError("diagnostic_selection_unadmitted")
    return bound


def _diagnostic_source_copy_id(kind: str, artifact_id: str, sha256: str, accession: str) -> str:
    return _diagnostic_id("ncencopy", [DIAGNOSTIC_SOURCE_ATTESTATION_VERSION, kind, artifact_id, sha256, accession])


def _diagnostic_row_locator(member: str, row_number: int, field_name: str) -> str:
    return f"{member}#data-row={row_number}#field={field_name}"


# --- F13b: bounded DERA TSV rows and one disposable per-package join store -----------------
# Each pinned member is hashed, then re-read one raw line at a time (at most
# ``tsv_line_max_bytes``, terminator included): the line keeps its exact 1-based data-row number
# and SHA-256, and only the columns the projection and the diagnostic rows read are spilled,
# raw, into a private SQLite file inside this run's spill root. Grammar is the legacy stream
# parser's accepted subset: one optional ``\r`` before ``\n``; any other CR, invalid UTF-8 or a
# csv error refuses the member. Accessions are then projected one at a time by the unchanged
# frozen ``_dera_filings`` over per-accession row sets that reproduce the whole-package parser's
# cross-accession attribution (first eligible FUND_ID owner, ``fund_id_duplicate`` and
# ``adviser_orphan`` charges). The store is local spill only: never a production database, an
# authority-bearing checkpoint or part of any identity.

_DIAGNOSTIC_JOIN_PAGE_SIZE = 4096
#: Smallest usable store (its empty schema takes 17 pages); a smaller remaining spill budget
#: refuses before the store exists.
_DIAGNOSTIC_JOIN_MIN_PAGES = 32
_DIAGNOSTIC_JOIN_TABLES = {
    "SUBMISSION": "sub",
    "REGISTRANT": "reg",
    "FUND_REPORTED_INFO": "fund",
    "ADVISER": "adv",
    "PRINCIPAL_UNDERWRITER": "uw",
}
#: Join keys: ``_clean(raw) or ""`` of one stored raw column each (the legacy parser's keys).
_DIAGNOSTIC_JOIN_KEYS: dict[str, tuple[tuple[str, str], ...]] = {
    "SUBMISSION": (("k_acc", "ACCESSION_NUMBER"),),
    "REGISTRANT": (("k_acc", "ACCESSION_NUMBER"),),
    "FUND_REPORTED_INFO": (("k_fund", "FUND_ID"), ("k_acc", "ACCESSION_NUMBER")),
    "ADVISER": (("k_fund", "FUND_ID"),),
    "PRINCIPAL_UNDERWRITER": (("k_acc", "ACCESSION_NUMBER"),),
}
#: Query-plan markers of temporary sort/hash structures or materialization: refused outright,
#: so every statement streams in rowid or index order through the capped page cache.
_DIAGNOSTIC_JOIN_UNBOUNDED_PLAN = ("TEMP B-TREE", "AUTOMATIC", "MATERIALIZE", "CO-ROUTINE", "BLOOM FILTER")


def _diagnostic_join_schema() -> tuple[str, ...]:
    statements: list[str] = []
    for table, name in _DIAGNOSTIC_JOIN_TABLES.items():
        keys = [key for key, _column in _DIAGNOSTIC_JOIN_KEYS[table]]
        columns = ", ".join(f'"{column}" TEXT NOT NULL' for column in _DIAGNOSTIC_DERA_REQUIRED_COLUMNS[table])
        key_columns = ", ".join(f"{key} TEXT NOT NULL" for key in keys)
        statements.append(
            f"CREATE TABLE {name} (line INTEGER PRIMARY KEY, digest TEXT NOT NULL, {key_columns}, {columns})"
        )
        statements.extend(f"CREATE INDEX {name}_{key} ON {name} ({key})" for key in keys)
    statements.extend((
        "CREATE TABLE fund_owner (k_fund TEXT PRIMARY KEY, line INTEGER NOT NULL, k_acc TEXT NOT NULL) WITHOUT ROWID",
        "CREATE INDEX fund_owner_k_acc ON fund_owner (k_acc)",
        "CREATE TABLE extra (k_acc TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY (k_acc, reason)) WITHOUT ROWID",
        (
            "CREATE TABLE proj (k_acc TEXT PRIMARY KEY, cik TEXT NOT NULL, digest TEXT NOT NULL, schema TEXT, "
            "form TEXT, period TEXT) WITHOUT ROWID"
        ),
        "CREATE TABLE quarantine (k_acc TEXT PRIMARY KEY, reasons TEXT NOT NULL) WITHOUT ROWID",
    ))
    return tuple(statements)


def _diagnostic_join_columns(table: str, alias: str = "") -> str:
    return ", ".join(f'{alias}"{column}"' for column in _DIAGNOSTIC_DERA_REQUIRED_COLUMNS[table])


class _DiagnosticJoinStore:
    """One disposable per-package SQLite join/sort store inside the run's private spill root.

    The file is created exclusively (an existing path is never reused or overwritten), capped at
    ``capacity_bytes`` through ``max_page_count`` and its page cache at ``join_cache_bytes``;
    there is no rollback journal, memory mapping or automatic index, temporary structures stay
    in memory under the RSS monitor, and every read statement's plan is refused unless it is
    driven by rowid or index order (no temporary B-tree, materialization or nested scan).
    SQLite failures refuse typed (``SQLITE_FULL`` as budget or disk exhaustion, anything else as
    ``diagnostic_spill_io_failed``); rows are never dropped. :meth:`close` deletes exactly its
    own file (and any SQLite sidecar of that exact name), on success and on failure.
    """

    def __init__(self, path: Path, monitor: DiagnosticResourceMonitor, *, capacity_bytes: int) -> None:
        import os
        import sqlite3

        self._monitor = monitor
        self._path = Path(path)
        self._conn: Any = None
        self._created = False
        self._plans: set[str] = set()
        self._max_pages = capacity_bytes // _DIAGNOSTIC_JOIN_PAGE_SIZE
        self._inserts: dict[str, tuple[str, tuple[int, ...]]] = {}
        for table, name in _DIAGNOSTIC_JOIN_TABLES.items():
            columns = _DIAGNOSTIC_DERA_REQUIRED_COLUMNS[table]
            keys = _DIAGNOSTIC_JOIN_KEYS[table]
            marks = ", ".join("?" for _ in range(2 + len(keys) + len(columns)))
            self._inserts[table] = (
                f"INSERT INTO {name} VALUES ({marks})",
                tuple(columns.index(column) for _key, column in keys),
            )
        if self._max_pages < _DIAGNOSTIC_JOIN_MIN_PAGES:
            raise NcenError("diagnostic_spill_budget_exceeded")
        if monitor.disk_free(self._path.parent) < monitor.limits.spill_min_free_bytes:
            raise NcenError("diagnostic_spill_disk_insufficient")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self._path, flags, 0o600)
        except OSError as exc:
            raise NcenError("diagnostic_spill_io_failed") from exc
        monitor.handle_opened()
        os.close(descriptor)
        monitor.handle_closed()
        self._created = True
        monitor.join_store_created()
        try:
            try:
                self._conn = sqlite3.connect(str(self._path), isolation_level=None)
            except sqlite3.Error as exc:
                raise NcenError("diagnostic_spill_io_failed") from exc
            monitor.handle_opened()
            for pragma in (
                f"PRAGMA page_size = {_DIAGNOSTIC_JOIN_PAGE_SIZE}",
                "PRAGMA journal_mode = OFF",
                "PRAGMA synchronous = OFF",
                "PRAGMA locking_mode = EXCLUSIVE",
                "PRAGMA temp_store = MEMORY",
                f"PRAGMA cache_size = {-(monitor.limits.join_cache_bytes // 1024)}",
                "PRAGMA mmap_size = 0",
                "PRAGMA automatic_index = OFF",
                f"PRAGMA max_page_count = {self._max_pages}",
            ):
                self._execute(pragma)
            for statement in _diagnostic_join_schema():
                self._execute(statement)
            # One open write transaction for the store's whole life: the store is discarded, not
            # committed, and dirty pages spill to its own file once the page cache is full.
            self._execute("BEGIN")
        except BaseException:
            self.close()
            raise

    def _typed(self, exc: Exception) -> NcenError:
        if getattr(exc, "sqlite_errorname", None) == "SQLITE_FULL":
            # SQLite reports both its page cap and a full device as SQLITE_FULL: free space at
            # or under the reserve is the device, anything else is this store's spill budget.
            if self._monitor.disk_free(self._path.parent) <= self._monitor.limits.spill_min_free_bytes:
                return NcenError("diagnostic_spill_disk_insufficient")
            return NcenError("diagnostic_spill_budget_exceeded")
        return NcenError("diagnostic_spill_io_failed")

    def _execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        import sqlite3

        if self._conn is None:
            raise NcenError("diagnostic_join_store_closed")
        try:
            return self._conn.execute(sql, params)
        except sqlite3.Error as exc:
            raise self._typed(exc) from exc

    def _fetchone(self, cursor: Any) -> Any:
        import sqlite3

        try:
            return cursor.fetchone()
        except sqlite3.Error as exc:
            raise self._typed(exc) from exc

    def _check_plan(self, sql: str, params: Sequence[Any]) -> None:
        if sql in self._plans:
            return
        cursor = self._execute("EXPLAIN QUERY PLAN " + sql, params)
        details: list[str] = []
        while (item := self._fetchone(cursor)) is not None:
            details.append(str(item[-1]))
        scans = sum(1 for detail in details if detail.startswith("SCAN "))
        if scans > 1 or any(marker in detail for detail in details for marker in _DIAGNOSTIC_JOIN_UNBOUNDED_PLAN):
            raise NcenError("diagnostic_join_plan_unbounded")
        self._plans.add(sql)
        self._monitor.join_plan_checked()

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        """One write statement (an insert into a store-owned table)."""
        self._execute(sql, params)

    def insert_row(self, table: str, line: int, digest: str, raw: Sequence[str]) -> None:
        sql, key_positions = self._inserts[table]
        keys = tuple(_clean(raw[position]) or "" for position in key_positions)
        self._execute(sql, (line, digest, *keys, *raw))

    def exists(self, sql: str, params: Sequence[Any]) -> bool:
        """Indexed lookup: whether ``sql`` yields a row."""
        self._check_plan(sql, params)
        cursor = self._execute(sql, params)
        try:
            return self._fetchone(cursor) is not None
        finally:
            cursor.close()

    def rows(self, sql: str, params: Sequence[Any] = (), *, phase: str) -> Iterator[tuple[Any, ...]]:
        """Stream a plan-checked query one row at a time, one monitor row per fetched row."""
        self._check_plan(sql, params)
        cursor = self._execute(sql, params)
        try:
            while (item := self._fetchone(cursor)) is not None:
                self._monitor.rows_read(1, phase)
                yield item
        finally:
            # A suspended iterator outliving close() must not touch the closed connection.
            if self._conn is not None:
                cursor.close()

    def capacity_check(self) -> None:
        """Measure the store's pages and refuse when free disk falls under the reserve."""
        cursor = self._execute("PRAGMA page_count")
        pages = int(self._fetchone(cursor)[0])
        self._monitor.join_store_size(pages, _DIAGNOSTIC_JOIN_PAGE_SIZE)
        if self._monitor.disk_free(self._path.parent) < self._monitor.limits.spill_min_free_bytes:
            raise NcenError("diagnostic_spill_disk_insufficient")

    def close(self) -> None:
        import os

        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            finally:
                self._monitor.handle_closed()
        if not self._created:
            return
        self._created = False
        for candidate in (self._path, *(Path(f"{self._path}{suffix}") for suffix in ("-journal", "-wal", "-shm"))):
            try:
                os.remove(candidate)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise NcenError("diagnostic_spill_cleanup_failed") from exc
        self._monitor.join_store_removed()


def _diagnostic_tsv_fields(raw_line: bytes, table: str, number: int) -> list[str]:
    """Fields of one raw DERA TSV data line (tab-delimited, no quoting), refusing typed."""
    try:
        text = raw_line.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NcenError(f"diagnostic_member_row_unparseable:{table}:{number}") from exc
    text = text.removesuffix("\n").removesuffix("\r")
    if "\r" in text:
        # The legacy stream parser would split the record here: never accept a different row.
        raise NcenError(f"diagnostic_member_row_unparseable:{table}:{number}")
    try:
        return next(csv.reader([text], delimiter="\t", quoting=csv.QUOTE_NONE))
    except csv.Error as exc:
        raise NcenError(f"diagnostic_member_row_unparseable:{table}:{number}") from exc


def _diagnostic_read_bounded_tsv_line(
    handle: Any,
    *,
    max_bytes: int,
    monitor: DiagnosticResourceMonitor,
) -> tuple[bytes | None, bool]:
    """Read one TSV line through genuinely bounded reads, checking immediately after each read.

    At most ``hash_block_bytes`` is requested/read between resource checkpoints. One extra byte
    beyond ``max_bytes`` distinguishes an oversized line from an exact-bound line ending at EOF;
    the caller chooses the typed error appropriate to a header or a data row.
    """
    raw_line = bytearray()
    while True:
        remaining = max_bytes + 1 - len(raw_line)
        request_size = min(remaining, monitor.limits.hash_block_bytes)
        chunk = handle.readline(request_size)
        if not chunk:
            return (bytes(raw_line), False) if raw_line else (None, False)
        monitor.bytes_processed(len(chunk), "zip_rows")
        raw_line.extend(chunk)
        if len(raw_line) > max_bytes:
            return bytes(raw_line), True
        if chunk.endswith(b"\n"):
            return bytes(raw_line), False


def _diagnostic_spill_zip_table(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    spec: Mapping[str, Any],
    table: str,
    *,
    monitor: DiagnosticResourceMonitor,
    store: _DiagnosticJoinStore,
) -> None:
    """Verify one pinned member, then stream its rows into ``store`` one bounded line at a time."""
    expected = _diagnostic_exact_keys(dict(spec), {"path", "sha256", "bytes", "header"}, "diagnostic_member")
    if expected["path"] != info.filename:
        raise NcenError("diagnostic_member_path_mismatch")
    expected_size = _diagnostic_positive_int(expected["bytes"], "diagnostic_member_size")
    if info.file_size != expected_size:
        raise NcenError("diagnostic_member_size_mismatch")
    expected_sha = expected["sha256"]
    _diagnostic_hash_valid(expected_sha, "diagnostic_member_sha256")
    digest = hashlib.sha256()
    monitor.check("zip_member_hash")
    with archive.open(info) as handle:
        while chunk := handle.read(monitor.limits.hash_block_bytes):
            digest.update(chunk)
            monitor.bytes_read(len(chunk), "zip_member_hash")
    if digest.hexdigest() != expected_sha:
        raise NcenError("diagnostic_member_sha256_mismatch")
    monitor.check("zip_rows")
    limits = monitor.limits
    columns = _DIAGNOSTIC_DERA_REQUIRED_COLUMNS[table]
    with archive.open(info) as handle:
        header_bytes, header_oversized = _diagnostic_read_bounded_tsv_line(
            handle, max_bytes=_TSV_HEADER_MAX, monitor=monitor
        )
        if header_bytes is None or header_oversized or not header_bytes.endswith(b"\n"):
            raise ZipSafetyError(f"tsv_header_unbounded:{info.filename}")
        monitor.tsv_line(len(header_bytes))
        try:
            header_text = header_bytes.decode("utf-8")[:-1]
        except UnicodeDecodeError as exc:
            raise NcenError(f"diagnostic_member_header_unparseable:{table}") from exc
        header_text = header_text.removesuffix("\r")
        if "\r" in header_text:
            raise NcenError(f"diagnostic_member_header_unparseable:{table}")
        try:
            header = tuple(next(csv.reader([header_text], delimiter="\t", quoting=csv.QUOTE_NONE)))
        except csv.Error as exc:
            raise NcenError(f"diagnostic_member_header_unparseable:{table}") from exc
        if not isinstance(expected["header"], list) or header != tuple(expected["header"]):
            raise NcenError("diagnostic_member_header_mismatch")
        if len(set(header)) != len(header):
            raise NcenError("diagnostic_member_duplicate_header")
        missing = set(columns) - set(header)
        if missing:
            raise NcenError(f"diagnostic_member_required_columns_missing:{table}")
        positions = tuple(header.index(column) for column in columns)
        width = len(header)
        line_max = limits.tsv_line_max_bytes
        number = 0
        monitor.tsv_rows_resident(1)
        while True:
            raw_line, oversized = _diagnostic_read_bounded_tsv_line(
                handle, max_bytes=line_max, monitor=monitor
            )
            if raw_line is None:
                break
            number += 1
            monitor.tsv_line(len(raw_line))
            if oversized:
                raise NcenError(f"diagnostic_member_row_oversized:{table}:{number}")
            fields = _diagnostic_tsv_fields(raw_line, table, number)
            if len(fields) != width:
                raise NcenError(f"diagnostic_member_row_width_mismatch:{table}:{number}")
            store.insert_row(table, number, hashlib.sha256(raw_line).hexdigest(), [fields[p] for p in positions])
            monitor.rows_read(1, "zip_rows")
            if number % limits.rows_per_check == 0:
                store.capacity_check()
    store.capacity_check()


def _diagnostic_common_row_fields(
    artifact: Mapping[str, Any],
    *,
    artifact_path: str,
    copy_id: str,
    projection_digest: str | None,
    filing: NcenFiling | None,
    header_source_id: str | None,
) -> dict[str, Any]:
    return {
        "artifact_id": artifact["artifact_id"],
        "artifact_path": artifact_path,
        "artifact_sha256": artifact["sha256"],
        "artifact_size": artifact["bytes"],
        "source_copy_id": copy_id,
        "projection_digest": projection_digest,
        "schema_version": None if filing is None else filing.schema_version,
        "form_type": None if filing is None else filing.form_type,
        "report_period_end": None if filing is None else filing.report_period_end,
        "public_at": _diagnostic_parse_timestamp(artifact["public_at"], "diagnostic_public_at"),
        "data_known_at": _diagnostic_parse_timestamp(artifact["data_known_at"], "diagnostic_data_known_at"),
        "retrieved_at": _diagnostic_parse_timestamp(artifact["retrieved_at"], "diagnostic_retrieved_at"),
        "acceptance_at": None if filing is None else filing.acceptance_at,
        "header_source_id": header_source_id,
        "custody_state": "verified",
    }


def _diagnostic_header_row(
    artifact: Mapping[str, Any], path_label: str, path: Path, data: bytes
) -> DiagnosticSourceRow:
    from .sec_acquisition import parse_acceptance_header

    accession = artifact["accession_number"]
    registrant = artifact["registrant_cik"]
    if not isinstance(accession, str) or _ACCESSION.fullmatch(accession) is None:
        raise NcenError("diagnostic_header_accession_invalid")
    _diagnostic_normalized_cik(registrant)
    retrieved = _diagnostic_parse_timestamp(artifact["retrieved_at"], "diagnostic_retrieved_at")
    assert retrieved is not None
    header = parse_acceptance_header(
        data,
        accession_number=accession,
        url=artifact["source_url"],
        document_sha256=artifact["sha256"],
        retrieved_at=retrieved,
    )
    if registrant not in header.filer_ciks:
        raise NcenError("diagnostic_header_cik_mismatch")
    public_at = _diagnostic_parse_timestamp(artifact["public_at"], "diagnostic_public_at")
    data_known = _diagnostic_parse_timestamp(artifact["data_known_at"], "diagnostic_data_known_at")
    if public_at != header.acceptance_at or data_known != header.acceptance_at:
        raise NcenError("diagnostic_header_time_mismatch")
    copy_id = _diagnostic_source_copy_id("header", artifact["artifact_id"], artifact["sha256"], accession)
    return DiagnosticSourceRow(
        accession,
        registrant,
        "header",
        "/SEC-HEADER[1]",
        None,
        "registrant",
        source_kind="header",
        artifact_id=artifact["artifact_id"],
        artifact_path=path_label,
        artifact_sha256=artifact["sha256"],
        artifact_size=artifact["bytes"],
        raw_row_sha256=header.header_sha256,
        source_copy_id=copy_id,
        form_type=header.submission_type,
        report_period_end=header.period,
        public_at=public_at,
        data_known_at=data_known,
        retrieved_at=retrieved,
        acceptance_at=header.acceptance_at,
        name_state="unavailable",
    )


def _diagnostic_dera_ingest(
    artifact: Mapping[str, Any],
    path: Path,
    store: _DiagnosticJoinStore,
    *,
    monitor: DiagnosticResourceMonitor,
) -> dict[str, str]:
    """Stream every pinned member of the verified DERA spool at ``path`` into ``store``.

    Returns the member path of each pinned table. Member, header, width and hash refusals are
    raised in table order, where the retired whole-table reader raised them.
    """
    member_specs = artifact["members"]
    if not isinstance(member_specs, list):
        raise NcenError("diagnostic_members_invalid")
    specs_by_path: dict[str, Mapping[str, Any]] = {}
    for item in member_specs:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise NcenError("diagnostic_member_invalid")
        if item["path"] in specs_by_path:
            raise NcenError("diagnostic_member_duplicate")
        specs_by_path[item["path"]] = item
    member_by_table: dict[str, str] = {}
    handle, _identity, _size = _diagnostic_open_held(path, "diagnostic_spool", monitor, buffered=True)
    try:
        with zipfile.ZipFile(handle) as archive:
            members = inspect_zip(archive, NCEN_ZIP_LIMITS)
            for table in PINNED_TABLES:
                info = _member_for(members, table)
                if info is None or info.filename not in specs_by_path:
                    raise NcenError(f"diagnostic_member_missing:{table}")
                member_by_table[table] = info.filename
                _diagnostic_spill_zip_table(
                    archive, info, specs_by_path[info.filename], table, monitor=monitor, store=store
                )
    finally:
        handle.close()
        monitor.handle_closed()
    if len(specs_by_path) != len(PINNED_TABLES) or set(specs_by_path) != set(member_by_table.values()):
        raise NcenError("diagnostic_member_allowlist_mismatch")
    return member_by_table


def _diagnostic_join_valid_accession(store: _DiagnosticJoinStore, key: str) -> bool:
    """``key`` is in the legacy parser's ``submissions``: well formed and present in SUBMISSION."""
    return _ACCESSION.fullmatch(key) is not None and store.exists(
        "SELECT 1 FROM sub WHERE k_acc = ? LIMIT 1", (key,)
    )


def _diagnostic_join_fund_owners(store: _DiagnosticJoinStore) -> None:
    """Legacy FUND_ID ownership: the first eligible row (file order) of each FUND_ID owns it.

    Eligible rows have a non-blank FUND_ID and a submitted accession (the legacy parser only
    counts the others). A later eligible row of another accession charges that accession
    ``fund_id_duplicate``; a later row of the owner's own accession stays in the owner's row set,
    where the frozen ``_dera_filings`` charges it exactly as the whole-package parser does.
    """
    current: str | None = None
    owner: str | None = None
    for k_fund, line, k_acc in store.rows("SELECT k_fund, line, k_acc FROM fund ORDER BY k_fund, line", phase="dera_parse"):
        if k_fund != current:
            current, owner = k_fund, None
        if not k_fund or not _diagnostic_join_valid_accession(store, k_acc):
            continue
        if owner is None:
            owner = k_acc
            store.execute("INSERT INTO fund_owner VALUES (?, ?, ?)", (k_fund, line, k_acc))
        elif k_acc != owner:
            store.execute("INSERT OR IGNORE INTO extra VALUES (?, ?)", (k_acc, "fund_id_duplicate"))


def _diagnostic_join_adviser_orphans(store: _DiagnosticJoinStore) -> None:
    """Legacy ``adviser_orphan``: an adviser FUND_ID that nobody owns charges its prefix accession."""
    current: str | None = None
    for (k_fund,) in store.rows("SELECT k_fund FROM adv ORDER BY k_fund", phase="dera_parse"):
        if k_fund == current:
            continue
        current = k_fund
        if store.exists("SELECT 1 FROM fund_owner WHERE k_fund = ?", (k_fund,)):
            continue
        prefix = k_fund.split("_", 1)[0]
        if _diagnostic_join_valid_accession(store, prefix):
            store.execute("INSERT OR IGNORE INTO extra VALUES (?, ?)", (prefix, "adviser_orphan"))


def _diagnostic_join_accession_tables(
    store: _DiagnosticJoinStore, accession: str
) -> dict[str, list[dict[str, str | None]]]:
    """One accession's rows exactly as the whole-package parser attributes them (cleaned values).

    Submission, registrant and underwriter rows carry the accession; fund rows are its own rows
    of FUND_IDs it owns; adviser rows are those of the FUND_IDs it owns, in file order per fund.
    """

    def fetch(table: str, sql: str, params: tuple[str, ...]) -> list[dict[str, str | None]]:
        columns = _DIAGNOSTIC_DERA_REQUIRED_COLUMNS[table]
        return [
            {name: _clean(value) for name, value in zip(columns, item, strict=True)}
            for item in store.rows(sql, params, phase="dera_parse")
        ]

    advisers: list[dict[str, str | None]] = []
    for (k_fund,) in store.rows(
        "SELECT k_fund FROM fund_owner WHERE k_acc = ? ORDER BY k_fund", (accession,), phase="dera_parse"
    ):
        advisers.extend(fetch(
            "ADVISER",
            f"SELECT {_diagnostic_join_columns('ADVISER')} FROM adv WHERE k_fund = ? ORDER BY line",
            (k_fund,),
        ))
    return {
        "SUBMISSION": fetch(
            "SUBMISSION",
            f"SELECT {_diagnostic_join_columns('SUBMISSION')} FROM sub WHERE k_acc = ? ORDER BY line",
            (accession,),
        ),
        "REGISTRANT": fetch(
            "REGISTRANT",
            f"SELECT {_diagnostic_join_columns('REGISTRANT')} FROM reg WHERE k_acc = ? ORDER BY line",
            (accession,),
        ),
        "FUND_REPORTED_INFO": fetch(
            "FUND_REPORTED_INFO",
            f"SELECT {_diagnostic_join_columns('FUND_REPORTED_INFO', 'f.')} FROM fund AS f "
            "CROSS JOIN fund_owner AS o WHERE f.k_acc = ? AND o.k_fund = f.k_fund AND o.k_acc = ? "
            "ORDER BY f.line",
            (accession, accession),
        ),
        "ADVISER": advisers,
        "PRINCIPAL_UNDERWRITER": fetch(
            "PRINCIPAL_UNDERWRITER",
            f"SELECT {_diagnostic_join_columns('PRINCIPAL_UNDERWRITER')} FROM uw WHERE k_acc = ? ORDER BY line",
            (accession,),
        ),
    }


def _diagnostic_dera_projections(
    store: _DiagnosticJoinStore,
    *,
    package_label: str,
    zip_sha256: str,
    retrieved_at: dt.datetime,
    first_verified_public_at: dt.datetime,
    monitor: DiagnosticResourceMonitor,
) -> Iterator[NcenFiling]:
    """Filings in accession order, each equal to :func:`parse_dera_ncen_package`'s (F13b).

    Every well-formed submitted accession is rebuilt by the frozen ``_dera_filings`` from its
    own row set only (one accession resident at a time), then charged the cross-accession reasons
    the whole-package parser charges it. The legacy package-level quarantines (missing table or
    column, duplicate header, wrong width, undecodable TSV) cannot reach here: ingestion refused
    them first. A projection field the row sets cannot supply stops the package.
    """
    retrieved = retrieved_at.astimezone(UTC)
    known_at = first_verified_public_at.astimezone(UTC)
    monitor.check("dera_parse")
    _diagnostic_join_fund_owners(store)
    _diagnostic_join_adviser_orphans(store)
    store.capacity_check()
    previous: str | None = None
    for (accession,) in store.rows("SELECT k_acc FROM sub ORDER BY k_acc", phase="dera_parse"):
        if accession == previous:
            continue
        previous = accession
        if _ACCESSION.fullmatch(accession) is None:
            continue
        monitor.check("dera_parse")
        tables = _diagnostic_join_accession_tables(store, accession)
        monitor.tsv_rows_resident(sum(len(items) for items in tables.values()))
        monitor.check("dera_parse")
        try:
            filings = _dera_filings(
                tables,
                package_label=package_label,
                zip_sha=zip_sha256,
                known_at=known_at,
                retrieved_at=retrieved,
                stats={},
            )
        except KeyError as exc:
            raise NcenError("diagnostic_dera_projection_field_unmapped") from exc
        monitor.check("dera_parse")
        del tables
        if len(filings) != 1 or filings[0].accession_number != accession:
            raise NcenError("diagnostic_dera_projection_field_unmapped")
        extras = tuple(
            reason
            for (reason,) in store.rows(
                "SELECT reason FROM extra WHERE k_acc = ? ORDER BY reason", (accession,), phase="dera_parse"
            )
        )
        yield _quarantined(filings[0], *extras) if extras else filings[0]
    monitor.check("dera_parse")


def _diagnostic_join_record(
    store: _DiagnosticJoinStore, filing: NcenFiling, *, monitor: DiagnosticResourceMonitor
) -> None:
    """Spill one projected filing: its quarantine reasons, or the fields the rows carry."""
    if not filing.usable:
        store.execute("INSERT INTO quarantine VALUES (?, ?)", (filing.accession_number, ",".join(filing.reasons)))
        return
    if filing.registrant_cik is None or filing.acceptance_at is not None:
        # A usable DERA projection always has a CIK and never an acceptance time.
        raise NcenError("diagnostic_dera_projection_field_unmapped")
    monitor.check("dera_parse")
    projection_digest = filing.projection_digest
    monitor.check("dera_parse")
    store.execute(
        "INSERT INTO proj VALUES (?, ?, ?, ?, ?, ?)",
        (
            filing.accession_number,
            filing.registrant_cik,
            projection_digest,
            filing.schema_version,
            filing.form_type,
            _diagnostic_optional_date(filing.report_period_end),
        ),
    )


def _diagnostic_join_refuse_repeats(
    store: _DiagnosticJoinStore, sql: str, reason: str, *, blank: bool = False, accession: bool = False
) -> None:
    """Refuse a repeated key in index order (and a blank or malformed accession key if asked)."""
    previous: str | None = None
    for (key,) in store.rows(sql, phase="dera_parse"):
        if key == previous or (blank and not key) or (accession and _ACCESSION.fullmatch(key) is None):
            raise NcenError(reason)
        previous = key


_DIAGNOSTIC_DERA_ROLE_MAP = {
    "adviser": "current_primary",
    "sub_adviser": "current_sub",
    "terminated_adviser": "terminated_primary",
    "terminated_sub_adviser": "terminated_sub",
}
_DIAGNOSTIC_JOIN_PROJECTION = "p.cik, p.digest, p.schema, p.form, p.period"


def _diagnostic_dera_fields(
    artifact: Mapping[str, Any],
    path_label: str,
    member: str,
    accession: str,
    projection: Sequence[Any],
    header_by_accession: Mapping[str, DiagnosticSourceRow],
) -> tuple[str, dict[str, Any]]:
    """Registrant CIK and shared row fields of one accession's DERA copy."""
    cik, digest, schema, form, period = projection
    if cik is None:
        raise NcenError("diagnostic_dera_source_identity_unavailable")
    header = header_by_accession.get(accession)
    copy_id = _diagnostic_source_copy_id("dera", artifact["artifact_id"], artifact["sha256"], accession)
    fields = _diagnostic_common_row_fields(
        artifact,
        artifact_path=path_label,
        copy_id=copy_id,
        projection_digest=digest,
        filing=None,
        header_source_id=None if header is None else header.source_row_id,
    )
    fields.update({
        "schema_version": schema,
        "form_type": form,
        "report_period_end": _diagnostic_parse_date(period, "diagnostic_report_period_end"),
        "source_kind": "dera",
        "member_path": member,
    })
    if header is not None:
        fields["acceptance_at"] = header.acceptance_at
    return cik, fields


def _diagnostic_dera_emit(
    store: _DiagnosticJoinStore,
    artifact: Mapping[str, Any],
    path_label: str,
    member_by_table: Mapping[str, str],
    header_by_accession: Mapping[str, DiagnosticSourceRow],
    *,
    monitor: DiagnosticResourceMonitor,
) -> tuple[DiagnosticSourceRow, ...]:
    """B.5 rows in accession order, then adviser and underwriter rows in file order."""
    output: list[DiagnosticSourceRow] = []
    registrant_member = member_by_table["REGISTRANT"]
    submission_member = member_by_table["SUBMISSION"]
    monitor.check("dera_rows")
    monitor.tsv_rows_resident(1)
    for line, digest, accession, raw_cik, answer, name, sub_line, sub_digest, *projection in store.rows(
        'SELECT r.line, r.digest, r.k_acc, r."CIK", r."IS_FAMILY_INVESTMENT_COMPANY", '
        f'r."FAMILY_INVESTMENT_COMPANY_NAME", s.line, s.digest, {_DIAGNOSTIC_JOIN_PROJECTION} '
        "FROM reg AS r LEFT JOIN sub AS s ON s.k_acc = r.k_acc LEFT JOIN proj AS p ON p.k_acc = r.k_acc "
        "ORDER BY r.k_acc",
        phase="dera_rows",
    ):
        if sub_line is None:
            raise NcenError("diagnostic_dera_source_identity_unavailable")
        cik, fields = _diagnostic_dera_fields(
            artifact, path_label, registrant_member, accession, projection, header_by_accession
        )
        if normalize_cik(raw_cik) != cik:
            raise NcenError("diagnostic_registrant_cik_mismatch")
        output.append(DiagnosticSourceRow(
            accession,
            cik,
            "b5",
            _diagnostic_row_locator(registrant_member, line, "IS_FAMILY_INVESTMENT_COMPANY"),
            None,
            "registrant",
            answer_raw=answer,
            name_raw=name,
            raw_row_sha256=digest,
            supporting_rows=((_diagnostic_row_locator(submission_member, sub_line, "ACCESSION_NUMBER"), sub_digest),),
            **fields,
        ))
    adviser_member = member_by_table["ADVISER"]
    fund_member = member_by_table["FUND_REPORTED_INFO"]
    for (
        line,
        digest,
        adviser_type,
        name,
        file_number,
        crd,
        lei,
        fund_line,
        fund_digest,
        accession,
        series_raw,
        *projection,
    ) in store.rows(
        'SELECT a.line, a.digest, a."ADVISER_TYPE", a."ADVISER_NAME", a."FILE_NUM", a."CRD_NUM", '
        f'a."ADVISER_LEI", f.line, f.digest, f.k_acc, f."SERIES_ID", {_DIAGNOSTIC_JOIN_PROJECTION} '
        "FROM adv AS a LEFT JOIN fund AS f ON f.k_fund = a.k_fund LEFT JOIN proj AS p ON p.k_acc = f.k_acc "
        "ORDER BY a.line",
        phase="dera_rows",
    ):
        if fund_line is None:
            raise NcenError("diagnostic_adviser_fund_join_missing")
        cik, fields = _diagnostic_dera_fields(
            artifact, path_label, adviser_member, accession, projection, header_by_accession
        )
        source_role = ADVISER_ROLES.get((_clean(adviser_type) or "").upper())
        if source_role is None:
            raise NcenError("diagnostic_adviser_role_unknown")
        series = _clean(series_raw)
        series_id = series if series is not None and is_series_key(series) else None
        output.append(DiagnosticSourceRow(
            accession,
            cik,
            _DIAGNOSTIC_DERA_ROLE_MAP[source_role],
            _diagnostic_row_locator(adviser_member, line, "ADVISER_NAME"),
            series_id,
            "series" if series_id is not None else "unresolved",
            name_raw=name,
            file_number_raw=file_number,
            crd_raw=crd,
            lei_raw=lei,
            raw_row_sha256=digest,
            supporting_rows=((_diagnostic_row_locator(fund_member, fund_line, "FUND_ID"), fund_digest),),
            **fields,
        ))
    underwriter_member = member_by_table["PRINCIPAL_UNDERWRITER"]
    for line, digest, accession, name, file_number, crd, lei, *projection in store.rows(
        'SELECT u.line, u.digest, u.k_acc, u."UNDERWRITER_NAME", u."FILE_NUM", u."CRD_NUM", '
        f'u."UNDERWRITER_LEI", {_DIAGNOSTIC_JOIN_PROJECTION} '
        "FROM uw AS u LEFT JOIN proj AS p ON p.k_acc = u.k_acc ORDER BY u.line",
        phase="dera_rows",
    ):
        cik, fields = _diagnostic_dera_fields(
            artifact, path_label, underwriter_member, accession, projection, header_by_accession
        )
        output.append(DiagnosticSourceRow(
            accession,
            cik,
            "underwriter",
            _diagnostic_row_locator(underwriter_member, line, "UNDERWRITER_NAME"),
            None,
            "registrant",
            name_raw=name,
            file_number_raw=file_number,
            crd_raw=crd,
            lei_raw=lei,
            raw_row_sha256=digest,
            **fields,
        ))
    return tuple(output)


def _diagnostic_dera_rows(
    artifact: Mapping[str, Any],
    path_label: str,
    path: Path,
    header_by_accession: Mapping[str, DiagnosticSourceRow],
    *,
    monitor: DiagnosticResourceMonitor,
    store_path: Path,
) -> tuple[DiagnosticSourceRow, ...]:
    """Rows of one DERA package read from ``path``, its verified private spool (F13a).

    F13b: the members stream into one disposable join store at ``store_path``, deleted before
    this returns or raises; no TSV table and no whole-package parse is ever resident. Accession
    projections equal :func:`parse_dera_ncen_package`'s and every refusal keeps the retired
    whole-table loader's order and message. Lines that loader let the legacy parser quarantine
    (oversized, stray CR, undecodable, csv-invalid) are refused earlier, typed, per member row.
    """
    zip_size = _diagnostic_positive_int(artifact["bytes"], "diagnostic_artifact_size")
    capacity = monitor.limits.spill_budget_bytes - zip_size
    store = _DiagnosticJoinStore(store_path, monitor, capacity_bytes=capacity)
    try:
        member_by_table = _diagnostic_dera_ingest(artifact, path, store, monitor=monitor)
        retrieved = _diagnostic_parse_timestamp(artifact["retrieved_at"], "diagnostic_retrieved_at")
        data_known = _diagnostic_parse_timestamp(artifact["data_known_at"], "diagnostic_data_known_at")
        assert retrieved is not None and data_known is not None
        for filing in _diagnostic_dera_projections(
            store,
            package_label=artifact["package_label"],
            zip_sha256=artifact["sha256"],
            retrieved_at=retrieved,
            first_verified_public_at=data_known,
            monitor=monitor,
        ):
            _diagnostic_join_record(store, filing, monitor=monitor)
        store.capacity_check()
        detail = ";".join(
            f"{accession}:{reasons}"
            for accession, reasons in store.rows(
                "SELECT k_acc, reasons FROM quarantine ORDER BY k_acc", phase="dera_parse"
            )
        )
        if detail:
            raise NcenError(f"diagnostic_dera_copy_quarantined:{detail}")
        _diagnostic_join_refuse_repeats(
            store,
            "SELECT k_acc FROM sub ORDER BY k_acc",
            "diagnostic_submission_duplicate_or_invalid",
            accession=True,
        )
        _diagnostic_join_refuse_repeats(store, "SELECT k_acc FROM reg ORDER BY k_acc", "diagnostic_registrant_duplicate")
        _diagnostic_join_refuse_repeats(
            store, "SELECT k_fund FROM fund ORDER BY k_fund", "diagnostic_fund_duplicate_or_invalid", blank=True
        )
        return _diagnostic_dera_emit(
            store, artifact, path_label, member_by_table, header_by_accession, monitor=monitor
        )
    finally:
        store.close()


def _diagnostic_xml_paths(root: ElementTree.Element) -> dict[int, str]:
    paths: dict[int, str] = {}

    def visit(node: ElementTree.Element, path: str) -> None:
        paths[id(node)] = path
        counts: Counter[str] = Counter()
        for child in list(node):
            if not isinstance(child.tag, str):
                continue
            counts[child.tag] += 1
            visit(child, f"{path}/{child.tag}[{counts[child.tag]}]")

    visit(root, f"/{root.tag}[1]")
    return paths


def _diagnostic_xml_rows(
    artifact: Mapping[str, Any],
    path_label: str,
    data: bytes,
    header_by_id: Mapping[str, DiagnosticSourceRow],
    *,
    monitor: DiagnosticResourceMonitor,
) -> tuple[DiagnosticSourceRow, ...]:
    accession = artifact["accession_number"]
    header = header_by_id.get(artifact["header_artifact_id"])
    if header is None or header.accession_number != accession:
        raise NcenError("diagnostic_xml_header_binding_missing")
    retrieved = _diagnostic_parse_timestamp(artifact["retrieved_at"], "diagnostic_retrieved_at")
    assert retrieved is not None
    monitor.check("xml_parse")
    filing = parse_ncen_primary_doc(
        data,
        accession_number=accession,
        source_url=artifact["source_url"],
        retrieved_at=retrieved,
    )
    if not filing.usable:
        raise NcenError(
            f"diagnostic_xml_copy_quarantined:{accession}:{','.join(filing.reasons)}"
        )
    if filing.registrant_cik is None:
        raise NcenError("diagnostic_xml_cik_unavailable")
    if filing.form_type != header.form_type:
        raise NcenError("diagnostic_xml_header_form_conflict")
    monitor.check("xml_parse")
    root = safe_xml_root(data)
    namespace, local = _split(root.tag)
    if namespace != NCEN_NAMESPACE or local != "edgarSubmission":
        raise NcenError("diagnostic_xml_namespace_invalid")
    _check_vocabulary(root)
    paths = _diagnostic_xml_paths(root)
    form = _kid(root, "formData")
    registrant = None if form is None else _kid(form, "registrantInfo")
    if form is None or registrant is None:
        raise NcenError("diagnostic_xml_structure_missing")
    copy_id = _diagnostic_source_copy_id("edgar_xml", artifact["artifact_id"], artifact["sha256"], accession)
    fields = _diagnostic_common_row_fields(
        artifact,
        artifact_path=path_label,
        copy_id=copy_id,
        projection_digest=filing.projection_digest,
        filing=filing,
        header_source_id=header.source_row_id,
    )
    fields.update({
        "source_kind": "edgar_xml",
        "raw_row_sha256": artifact["sha256"],
        "acceptance_at": header.acceptance_at,
    })
    output: list[DiagnosticSourceRow] = []
    compound = _kid(registrant, "registrantFamilyInvComp")
    bare = _kid(registrant, "isRegistrantFamilyInvComp")
    if compound is not None:
        output.append(DiagnosticSourceRow(
            accession,
            filing.registrant_cik,
            "b5",
            f"{paths[id(compound)]}/@isRegistrantFamilyInvComp",
            None,
            "registrant",
            answer_raw=compound.get("isRegistrantFamilyInvComp"),
            name_raw=compound.get("familyInvCompFullName"),
            **fields,
        ))
    elif bare is not None:
        output.append(DiagnosticSourceRow(
            accession,
            filing.registrant_cik,
            "b5",
            paths[id(bare)],
            None,
            "registrant",
            answer_raw=bare.text,
            **fields,
        ))
    role_map = {
        "adviser": ("current_primary", "investmentAdviserName"),
        "sub_adviser": ("current_sub", "subAdviserName"),
        "terminated_adviser": ("terminated_primary", "investmentAdviserTerminatedName"),
        "terminated_sub_adviser": ("terminated_sub", "subAdviserTerminatedName"),
    }
    series_info = _kid(form, "managementInvestmentQuestionSeriesInfo")
    for question in () if series_info is None else _kids(series_info, "managementInvestmentQuestion"):
        series = _text(question, "mgmtInvSeriesId")
        series_id = series if series is not None and is_series_key(series) else None
        for source_role, list_name, item_name, id_names in XML_ADVISER_GROUPS:
            container = _kid(question, list_name)
            for item in () if container is None else _kids(container, item_name):
                role, name_element = role_map[source_role]
                name_node = _kid(item, name_element)
                id_nodes = tuple(_kid(item, name) for name in id_names)
                raw_ids = tuple(None if node is None else node.text for node in id_nodes)
                output.append(DiagnosticSourceRow(
                    accession,
                    filing.registrant_cik,
                    role,
                    paths[id(name_node)] if name_node is not None else paths[id(item)],
                    series_id,
                    "series" if series_id is not None else "unresolved",
                    name_raw=None if name_node is None else name_node.text,
                    file_number_raw=raw_ids[0],
                    crd_raw=raw_ids[1],
                    lei_raw=raw_ids[2],
                    **fields,
                ))
    underwriters = _kid(registrant, "principalUnderwriters")
    for item in () if underwriters is None else _kids(underwriters, "principalUnderwriter"):
        name_node = _kid(item, "principalUnderwriterName")
        id_nodes = tuple(_kid(item, name) for name in XML_UNDERWRITER_IDS)
        raw_ids = tuple(None if node is None else node.text for node in id_nodes)
        output.append(DiagnosticSourceRow(
            accession,
            filing.registrant_cik,
            "underwriter",
            paths[id(name_node)] if name_node is not None else paths[id(item)],
            None,
            "registrant",
            name_raw=None if name_node is None else name_node.text,
            file_number_raw=raw_ids[0],
            crd_raw=raw_ids[1],
            lei_raw=raw_ids[2],
            **fields,
        ))
    return tuple(output)


# --- F6: exact acquisition ledger membership from pinned N-CEN/A acquisition custody --------
# The declared quarantine ledger and boundary examples are claims. The authority is the sealed
# acquisition run itself: its ``scope.json`` and ``SHA256SUMS`` bytes must match caller-supplied
# anchors, and every exclusion/boundary identity is re-derived from the sealed terminal rows and
# their raw header/XML evidence. Counts are secondary accounting only. This reader is read-only
# and custody-only: it never fetches, never runs acquisition/seal code, never reads Stage 1B,
# vote spools, candidates, holdings, ratings or outcomes, and never infers schema from dates.

DIAGNOSTIC_ACQUISITION_LEDGER_VERSION = "ncen_diagnostic_acquisition_ledger_v1"
DIAGNOSTIC_EXCLUSION_LEDGER_DIGEST_VERSION = "ncen_diagnostic_exclusion_ledger_digest_v1"
_DIAGNOSTIC_ACQUISITION_SCOPE_SCHEMA = "bond_ncen_amendment_acquisition_scope_v1"
_DIAGNOSTIC_ACQUISITION_EVIDENCE_SCHEMA = "bond_ncen_amendment_schema_evidence_v1"
_DIAGNOSTIC_ACQUISITION_COVERAGE_SCHEMA = "bond_ncen_amendment_acquisition_coverage_v1"
_DIAGNOSTIC_ACQUISITION_RECEIPT_SCHEMA = "bond_ncen_amendment_acquisition_receipt_v1"
_DIAGNOSTIC_ACQUISITION_CLASSIFICATION_CONTRACT = "observed_xml_only_no_acceptance_era_inference"
_DIAGNOSTIC_ACQUISITION_RECOGNIZED_SCHEMAS = frozenset({"X0101", "X0201", "X0303", "X0404", "X0505"})
#: Exhaustive versioned mapping from the exact sealed terminal reason set to a quarantine class.
#: Any other (unknown or multiple) reason set is a typed STOP; nothing is forced into a bucket.
_DIAGNOSTIC_ACQUISITION_CLASS_BY_REASONS: Mapping[tuple[str, ...], str] = {
    ("amended_accession_invalid",): "absent_amended_accession",
    ("schema_element_count:0",): "schema_unavailable",
    ("xml_dera_projection_conflict",): "projection_conflict",
}
#: Secondary accounting of the real sealed run; never a substitute for exact membership.
_DIAGNOSTIC_ACQUISITION_SEALED_CLASS_COUNTS: Mapping[str, int] = {
    "absent_amended_accession": 131,
    "schema_unavailable": 11,
    "projection_conflict": 3,
}
_DIAGNOSTIC_ACQUISITION_SEALED_BOUNDARIES = 128
_DIAGNOSTIC_BOUNDARY_POLICY_STATE = "example_only"
_DIAGNOSTIC_BOUNDARY_REASONS = ("diagnostic_not_admitted_by_example",)
_DIAGNOSTIC_ACQUISITION_SEAL_FILES = frozenset({"SHA256SUMS", "SHA256SUMS.receipt.json"})
_DIAGNOSTIC_ACQUISITION_SUMS_LINE = re.compile(r"([0-9a-f]{64})  ([^\r\n]+)")
_DIAGNOSTIC_ACQUISITION_TERMINAL = re.compile(r"terminal/(\d{10}-\d{2}-\d{6})\.json")
_DIAGNOSTIC_ACQUISITION_MAX_BYTES: Mapping[str, int] = {
    "sha256sums": 64 * 1024**2,
    "receipt": 1024**2,
    "scope": 256 * 1024**2,
    "scope_digest": 4096,
    "coverage": 16 * 1024**2,
    "schema_evidence": 512 * 1024**2,
    "terminal": 4 * 1024**2,
    "header_raw": 16 * 1024**2,
    "header_record": 16 * 1024**2,
    "raw_xml": 64 * 1024**2,
}
_DIAGNOSTIC_ACQUISITION_TERMINAL_KEYS = frozenset({
    "schema",
    "accession_number",
    "registrant_cik",
    "request_id",
    "attempt_ids",
    "boundary_anomaly",
    "classification_basis",
    "schema_observed",
    "schema_version",
    "schema_namespace",
    "schema_xpath",
    "raw_xml",
    "xml",
    "header",
    "dera",
    "projection_equality",
    "quarantine_reasons",
    "rule_refs",
    "terminal_status",
    "failure_kind",
})


@dataclass(frozen=True, slots=True)
class DiagnosticAcquisitionPin:
    """Caller-supplied external anchors of one sealed N-CEN/A acquisition run.

    The anchors are the actual SHA-256 of ``scope.json`` and ``SHA256SUMS``. A pin is an input,
    not a capability: trust comes only from bytes that hash to it. The ``sealed_source`` lane
    accepts exactly the real run anchors; the ``synthetic_fixture`` lane can never reuse them.
    """

    root: Path
    scope_sha256: str
    sha256sums_sha256: str
    lane: str

    def __post_init__(self) -> None:
        root = Path(self.root)
        if not root.is_absolute() or root.is_symlink():
            raise NcenError("diagnostic_acquisition_root_unsafe")
        for value, name in (
            (self.scope_sha256, "diagnostic_acquisition_scope_sha256"),
            (self.sha256sums_sha256, "diagnostic_acquisition_sha256sums_sha256"),
        ):
            if not isinstance(value, str):
                raise NcenError(f"{name}_invalid")
            _diagnostic_hash_valid(value, name)
        if self.lane not in _DIAGNOSTIC_MANIFEST_KINDS:
            raise NcenError("diagnostic_acquisition_lane_invalid")
        real = (
            self.scope_sha256 == DIAGNOSTIC_ACQUISITION_SCOPE_SHA256,
            self.sha256sums_sha256 == DIAGNOSTIC_ACQUISITION_SHA256SUMS_SHA256,
        )
        if self.lane == "sealed_source" and not all(real):
            raise NcenError("diagnostic_acquisition_sealed_anchor_mismatch")
        if self.lane == "synthetic_fixture" and any(real):
            raise NcenError("diagnostic_acquisition_synthetic_uses_real_anchor")
        object.__setattr__(self, "root", root)


def _diagnostic_acquisition_ref(value: Any, name: str) -> tuple[str, str, int]:
    if (
        not isinstance(value, tuple)
        or len(value) != 3
        or not isinstance(value[0], str)
        or not value[0]
        or not isinstance(value[1], str)
    ):
        raise NcenError(f"diagnostic_acquisition_{name}_ref_invalid")
    _diagnostic_hash_valid(value[1], f"diagnostic_acquisition_{name}_sha256")
    _diagnostic_positive_int(value[2], f"diagnostic_acquisition_{name}_bytes")
    return value


@dataclass(frozen=True, slots=True)
class DiagnosticAcquisitionRecord:
    """One exclusion or boundary identity with its sealed terminal/header/XML references."""

    ledger_role: str
    accession_number: str
    registrant_cik: str
    form_type: str
    report_period_end: dt.date | None
    acceptance_at: dt.datetime
    acceptance_raw: str
    classification: str | None
    reasons: tuple[str, ...]
    terminal_reasons: tuple[str, ...]
    terminal_status: str
    schema_observed: bool
    schema_version: str | None
    projection_equality: str
    xml_projection_digest: str
    dera_projection_digest: str
    request_id: str
    header_sha256: str
    terminal_ref: tuple[str, str, int]
    header_raw_ref: tuple[str, str, int]
    header_record_ref: tuple[str, str, int]
    raw_xml_ref: tuple[str, str, int]

    def __post_init__(self) -> None:
        if _ACCESSION.fullmatch(self.accession_number) is None:
            raise NcenError("diagnostic_acquisition_record_accession_invalid")
        _diagnostic_normalized_cik(self.registrant_cik)
        _diagnostic_timestamp(self.acceptance_at)
        _diagnostic_reasons(self.reasons)
        _diagnostic_reasons(self.terminal_reasons)
        if self.form_type != AMENDMENT_FORM or type(self.schema_observed) is not bool:
            raise NcenError("diagnostic_acquisition_record_form_invalid")
        if self.projection_equality not in {"equal", "conflict"}:
            raise NcenError("diagnostic_acquisition_record_projection_invalid")
        for value, name in (
            (self.xml_projection_digest, "xml_projection"),
            (self.dera_projection_digest, "dera_projection"),
            (self.header_sha256, "header"),
        ):
            _diagnostic_hash_valid(value, f"diagnostic_acquisition_{name}_sha256")
        if self.request_id != f"ncen-a:{self.accession_number}":
            raise NcenError("diagnostic_acquisition_record_request_invalid")
        for value, name in (
            (self.terminal_ref, "terminal"),
            (self.header_raw_ref, "header_raw"),
            (self.header_record_ref, "header_record"),
            (self.raw_xml_ref, "raw_xml"),
        ):
            _diagnostic_acquisition_ref(value, name)
        if self.ledger_role == "quarantine":
            expected = _DIAGNOSTIC_ACQUISITION_CLASS_BY_REASONS.get(self.terminal_reasons)
            if (
                expected is None
                or self.classification != expected
                or self.reasons != self.terminal_reasons
                or self.terminal_status != "quarantined"
            ):
                raise NcenError("diagnostic_acquisition_record_classification_invalid")
        elif self.ledger_role == "boundary":
            if (
                self.classification is not None
                or self.reasons != _DIAGNOSTIC_BOUNDARY_REASONS
                or self.terminal_reasons
                or self.terminal_status != "verified"
                or not self.schema_observed
                or self.schema_version != "X0505"
            ):
                raise NcenError("diagnostic_acquisition_record_boundary_invalid")
        else:
            raise NcenError("diagnostic_acquisition_record_role_invalid")

    def exclusion_identity(self) -> list[Any]:
        return [
            self.accession_number,
            self.registrant_cik,
            self.form_type,
            _diagnostic_optional_date(self.report_period_end),
            _diagnostic_timestamp(self.acceptance_at),
            self.classification,
            list(self.reasons),
        ]

    def boundary_identity(self) -> list[Any]:
        return [
            self.accession_number,
            self.schema_version,
            self.form_type,
            _diagnostic_timestamp(self.acceptance_at),
            _DIAGNOSTIC_BOUNDARY_POLICY_STATE,
            list(self.reasons),
        ]

    def payload(self) -> dict[str, Any]:
        def ref(value: tuple[str, str, int]) -> dict[str, Any]:
            return {"path": value[0], "sha256": value[1], "bytes": value[2]}

        return {
            "ledger_role": self.ledger_role,
            "accession_number": self.accession_number,
            "registrant_cik": self.registrant_cik,
            "form_type": self.form_type,
            "report_period_end": _diagnostic_optional_date(self.report_period_end),
            "acceptance_at": _diagnostic_timestamp(self.acceptance_at),
            "acceptance_raw": self.acceptance_raw,
            "classification": self.classification,
            "reasons": list(self.reasons),
            "terminal_reasons": list(self.terminal_reasons),
            "terminal_status": self.terminal_status,
            "schema_observed": self.schema_observed,
            "schema_version": self.schema_version,
            "projection_equality": self.projection_equality,
            "xml_projection_digest": self.xml_projection_digest,
            "dera_projection_digest": self.dera_projection_digest,
            "request_id": self.request_id,
            "header_sha256": self.header_sha256,
            "terminal": ref(self.terminal_ref),
            "header_raw": ref(self.header_raw_ref),
            "header_record": ref(self.header_record_ref),
            "raw_xml": ref(self.raw_xml_ref),
        }


def _diagnostic_acquisition_digest_object(
    lane: str,
    scope_sha256: str,
    sha256sums_sha256: str,
    exclusions: Sequence[DiagnosticAcquisitionRecord],
    boundaries: Sequence[DiagnosticAcquisitionRecord],
) -> dict[str, Any]:
    """Canonical versioned ledger object: anchors plus complete sorted payloads."""
    return {
        "schema_version": DIAGNOSTIC_EXCLUSION_LEDGER_DIGEST_VERSION,
        "derivation_version": DIAGNOSTIC_ACQUISITION_LEDGER_VERSION,
        "lane": lane,
        "acquisition": {"scope_sha256": scope_sha256, "sha256sums_sha256": sha256sums_sha256},
        "exclusions": [item.payload() for item in exclusions],
        "boundaries": [item.payload() for item in boundaries],
    }


@dataclass(frozen=True, slots=True)
class DiagnosticAcquisitionLedger:
    """Exact exclusion and boundary membership derived from one pinned acquisition seal.

    Derivation evidence, not source authority: only :func:`read_diagnostic_source_rows` binds
    a ledger it derived itself into loader custody. ``exclusion_ledger_digest`` is recomputed
    from the complete payloads on construction, so a replaced payload cannot keep its digest.
    """

    lane: str
    scope_sha256: str
    sha256sums_sha256: str
    exclusions: tuple[DiagnosticAcquisitionRecord, ...]
    boundaries: tuple[DiagnosticAcquisitionRecord, ...]
    requests: int
    verified_requests: int
    inventory_entries: int
    inventory_bytes: int
    inventory_entries_hashed: int
    rederived_requests: int
    opened_paths: tuple[str, ...]
    exclusion_ledger_digest: str
    derivation_version: str = DIAGNOSTIC_ACQUISITION_LEDGER_VERSION

    def __post_init__(self) -> None:
        if self.lane not in _DIAGNOSTIC_MANIFEST_KINDS:
            raise NcenError("diagnostic_acquisition_lane_invalid")
        if self.derivation_version != DIAGNOSTIC_ACQUISITION_LEDGER_VERSION:
            raise NcenError("diagnostic_acquisition_derivation_version_invalid")
        for records, role in ((self.exclusions, "quarantine"), (self.boundaries, "boundary")):
            if any(type(item) is not DiagnosticAcquisitionRecord or item.ledger_role != role for item in records):
                raise NcenError("diagnostic_acquisition_ledger_role_invalid")
            accessions = [item.accession_number for item in records]
            if accessions != sorted(set(accessions)):
                raise NcenError("diagnostic_acquisition_ledger_not_sorted_unique")
        for value in (
            self.requests,
            self.verified_requests,
            self.inventory_entries,
            self.inventory_bytes,
            self.inventory_entries_hashed,
            self.rederived_requests,
        ):
            if type(value) is not int or value < 0:
                raise NcenError("diagnostic_acquisition_ledger_count_invalid")
        expected = _diagnostic_hash(
            _diagnostic_acquisition_digest_object(
                self.lane, self.scope_sha256, self.sha256sums_sha256, self.exclusions, self.boundaries
            )
        )
        if self.exclusion_ledger_digest != expected:
            raise NcenError("diagnostic_acquisition_ledger_digest_mismatch")

    def digest_object(self) -> dict[str, Any]:
        return _diagnostic_acquisition_digest_object(
            self.lane, self.scope_sha256, self.sha256sums_sha256, self.exclusions, self.boundaries
        )

    def class_counts(self) -> dict[str, int]:
        return dict(sorted(Counter(item.classification or "" for item in self.exclusions).items()))


def _diagnostic_acquisition_root(root: Path) -> Path:
    try:
        resolved = root.resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise NcenError("diagnostic_acquisition_root_unavailable") from exc
    status = root.lstat()
    if root.is_symlink() or getattr(status, "st_file_attributes", 0) & 0x400 or not resolved.is_dir():
        raise NcenError("diagnostic_acquisition_root_unsafe")
    return resolved


def _diagnostic_acquisition_read(
    root: Path,
    relative: str,
    *,
    expected_sha256: str | None,
    role: str,
    opened: list[str],
) -> bytes:
    """Read one contained regular file through a held descriptor; hash and return those bytes."""
    import os
    import stat as stat_module

    label, path = _diagnostic_manifest_relative_path(root, relative)
    limit = _DIAGNOSTIC_ACQUISITION_MAX_BYTES[role]
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise NcenError(f"diagnostic_acquisition_open_failed:{label}") from exc
    digest = hashlib.sha256()
    chunks: list[bytes] = []
    try:
        held = os.fstat(descriptor)
        linked = os.lstat(path)
        if not stat_module.S_ISREG(held.st_mode) or (held.st_ino, held.st_dev) != (linked.st_ino, linked.st_dev):
            raise NcenError(f"diagnostic_acquisition_descriptor_mismatch:{label}")
        if held.st_size > limit:
            raise NcenError(f"diagnostic_acquisition_file_too_large:{label}")
        total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            total += len(chunk)
            if total > limit:
                raise NcenError(f"diagnostic_acquisition_file_too_large:{label}")
            digest.update(chunk)
            chunks.append(chunk)
    finally:
        os.close(descriptor)
    data = b"".join(chunks)
    if len(data) != held.st_size:
        raise NcenError(f"diagnostic_acquisition_size_changed:{label}")
    if expected_sha256 is not None and digest.hexdigest() != expected_sha256:
        raise NcenError(f"diagnostic_acquisition_sha256_mismatch:{label}")
    opened.append(label)
    return data


def _diagnostic_acquisition_hash_entry(root: Path, relative: str) -> tuple[str, int]:
    """Stream-hash one contained regular file through a held descriptor (no retention)."""
    import os
    import stat as stat_module

    label, path = _diagnostic_manifest_relative_path(root, relative)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    total = 0
    try:
        held = os.fstat(descriptor)
        linked = os.lstat(path)
        if not stat_module.S_ISREG(held.st_mode) or (held.st_ino, held.st_dev) != (linked.st_ino, linked.st_dev):
            raise NcenError(f"diagnostic_acquisition_descriptor_mismatch:{label}")
        while chunk := os.read(descriptor, 1024 * 1024):
            total += len(chunk)
            digest.update(chunk)
    finally:
        os.close(descriptor)
    if total != held.st_size:
        raise NcenError(f"diagnostic_acquisition_size_changed:{label}")
    return digest.hexdigest(), total


def _diagnostic_acquisition_json(data: bytes, label: str, *, canonical: bool) -> Any:
    """Strict JSON: UTF-8, duplicate keys refused, optionally the acquisition canonical bytes."""

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in items:
            if key in output:
                raise NcenError(f"diagnostic_acquisition_json_duplicate_key:{label}")
            output[key] = value
        return output

    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NcenError(f"diagnostic_acquisition_json_invalid:{label}") from exc
    if canonical:
        encoded = json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n"
        if encoded.encode("utf-8") != data:
            raise NcenError(f"diagnostic_acquisition_json_not_canonical:{label}")
    return value


def _diagnostic_acquisition_inventory(data: bytes) -> dict[str, str]:
    """Closed ``SHA256SUMS`` inventory: safe unique POSIX paths (case-folded) to digests."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NcenError("diagnostic_acquisition_inventory_invalid") from exc
    if not text.endswith("\n"):
        raise NcenError("diagnostic_acquisition_inventory_invalid")
    entries: dict[str, str] = {}
    folded: set[str] = set()
    for number, line in enumerate(text[:-1].split("\n"), start=1):
        match = _DIAGNOSTIC_ACQUISITION_SUMS_LINE.fullmatch(line)
        if match is None:
            raise NcenError(f"diagnostic_acquisition_inventory_line_invalid:{number}")
        digest, relative = match.groups()
        parts = relative.split("/")
        if "\\" in relative or relative.startswith("/") or any(part in {"", ".", ".."} for part in parts):
            raise NcenError(f"diagnostic_acquisition_inventory_path_unsafe:{number}")
        if relative in _DIAGNOSTIC_ACQUISITION_SEAL_FILES:
            raise NcenError("diagnostic_acquisition_inventory_lists_seal_file")
        key = relative.casefold()
        if relative in entries or key in folded:
            raise NcenError(f"diagnostic_acquisition_inventory_duplicate:{number}")
        entries[relative] = digest
        folded.add(key)
    return entries


def _diagnostic_acquisition_closed_files(root: Path) -> dict[str, int]:
    """Every regular file under ``root`` with its size; links, junctions and reparse refused."""
    import os

    files: dict[str, int] = {}
    pending: list[tuple[str, str]] = [("", str(root))]
    while pending:
        prefix, directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                relative = f"{prefix}{entry.name}"
                is_junction = bool(getattr(entry, "is_junction", lambda: False)())
                status = entry.stat(follow_symlinks=False)
                if entry.is_symlink() or is_junction or getattr(status, "st_file_attributes", 0) & 0x400:
                    raise NcenError(f"diagnostic_acquisition_link_forbidden:{relative}")
                if entry.is_dir(follow_symlinks=False):
                    pending.append((f"{relative}/", entry.path))
                elif entry.is_file(follow_symlinks=False):
                    files[relative] = status.st_size
                else:
                    raise NcenError(f"diagnostic_acquisition_non_regular_file:{relative}")
    return files


@dataclass(frozen=True, slots=True)
class _DiagnosticAcquisitionXmlFacts:
    schema_version: str | None
    schema_observed: bool
    form_type: str | None
    registrant_cik: str | None
    report_period_end: str | None
    amended_accession_number: str | None
    reasons: tuple[str, ...]


def _diagnostic_acquisition_inspect_xml(data: bytes) -> _DiagnosticAcquisitionXmlFacts:
    """Observed XML facts exactly as the acquisition recorded them (``inspect_xml``).

    Schema is only the single official namespaced ``schemaVersion`` child; no date inference.
    """
    try:
        root = safe_xml_root(data)
    except XmlSafetyError as exc:
        return _DiagnosticAcquisitionXmlFacts(
            None, False, None, None, None, None, (f"xml_unsafe:{str(exc).split(':', 1)[0]}",)
        )
    reasons: set[str] = set()
    if _split(str(root.tag)) != (NCEN_NAMESPACE, "edgarSubmission"):
        reasons.add("xml_root_not_ncen")
    direct = [element for element in root if isinstance(element.tag, str)]
    direct_ids = {id(element) for element in direct}
    official = [element for element in direct if _split(element.tag) == (NCEN_NAMESPACE, "schemaVersion")]
    misplaced = 0
    wrong_namespace = 0
    for element in root.iter():
        if element is root or id(element) in direct_ids or not isinstance(element.tag, str):
            continue
        namespace, local = _split(element.tag)
        if local == "schemaVersion":
            if namespace == NCEN_NAMESPACE:
                misplaced += 1
            else:
                wrong_namespace += 1
    for element in direct:
        namespace, local = _split(element.tag)
        if local == "schemaVersion" and namespace != NCEN_NAMESPACE:
            wrong_namespace += 1
    if misplaced:
        reasons.add(f"schema_element_misplaced:{misplaced}")
    if wrong_namespace:
        reasons.add(f"schema_wrong_namespace:{wrong_namespace}")
    if len(official) != 1:
        reasons.add(f"schema_element_count:{len(official)}")
    schema = None
    if len(official) == 1:
        schema = (official[0].text or "").strip() or None
        if schema is None:
            reasons.add("schema_empty")
        elif schema not in _DIAGNOSTIC_ACQUISITION_RECOGNIZED_SCHEMAS:
            reasons.add(f"schema_unrecognized:{schema}")

    def first(local_name: str, parent: ElementTree.Element | None) -> ElementTree.Element | None:
        if parent is None:
            return None
        tag = f"{{{NCEN_NAMESPACE}}}{local_name}"
        return next((child for child in parent if child.tag == tag), None)

    def text(local_name: str, parent: ElementTree.Element | None) -> str | None:
        child = first(local_name, parent)
        value = None if child is None else (child.text or "").strip()
        return value or None

    header_data = first("headerData", root)
    amended = text("accessionNumber", header_data)
    if amended is None or _ACCESSION.fullmatch(amended) is None:
        reasons.add("amended_accession_invalid")
    credentials = first("issuerCredentials", first("filer", first("filerInfo", header_data)))
    filer_cik = normalize_cik(text("cik", credentials))
    form_data = first("formData", root)
    registrant_cik = normalize_cik(text("registrantCik", first("registrantInfo", form_data))) or filer_cik
    general = first("generalInfo", form_data)
    period = None if general is None else (general.get("reportEndingPeriod") or "").strip() or None
    return _DiagnosticAcquisitionXmlFacts(
        schema,
        len(official) == 1 and schema is not None and wrong_namespace == 0 and misplaced == 0,
        text("submissionType", header_data),
        registrant_cik,
        period,
        amended,
        tuple(sorted(reasons)),
    )


def _diagnostic_acquisition_parse_time(value: Any, label: str) -> dt.datetime:
    parsed = _diagnostic_parse_timestamp(value, f"diagnostic_acquisition_{label}")
    if parsed is None:
        raise NcenError(f"diagnostic_acquisition_{label}_invalid")
    return parsed


def _diagnostic_acquisition_request(raw: Any) -> dict[str, Any]:
    request = _diagnostic_exact_keys(
        raw,
        {
            "accession_number",
            "boundary_anomaly",
            "dera",
            "filing_date",
            "header",
            "primary_doc_url",
            "registrant_cik",
            "request_id",
        },
        "diagnostic_acquisition_request",
    )
    accession = request["accession_number"]
    if not isinstance(accession, str) or _ACCESSION.fullmatch(accession) is None:
        raise NcenError("diagnostic_acquisition_request_accession_invalid")
    if not isinstance(request["registrant_cik"], str):
        raise NcenError("cik_not_normalized")
    _diagnostic_normalized_cik(request["registrant_cik"])
    if request["request_id"] != f"ncen-a:{accession}" or type(request["boundary_anomaly"]) is not bool:
        raise NcenError(f"diagnostic_acquisition_request_invalid:{accession}")
    header = _diagnostic_exact_keys(
        request["header"],
        {
            "acceptance_at",
            "acceptance_raw",
            "header_sha256",
            "raw_path",
            "raw_sha256",
            "record_path",
            "record_sha256",
            "retrieved_at",
            "url",
        },
        "diagnostic_acquisition_request_header",
    )
    for name in ("header_sha256", "raw_sha256", "record_sha256"):
        if not isinstance(header[name], str):
            raise NcenError(f"diagnostic_acquisition_request_{name}_invalid")
        _diagnostic_hash_valid(header[name], f"diagnostic_acquisition_request_{name}")
    _diagnostic_exact_keys(
        request["dera"],
        {"filing_date", "package", "package_label", "report_ending_period", "row_locator"},
        "diagnostic_acquisition_request_dera",
    )
    return request


def _diagnostic_acquisition_terminal(row: Any, request: Mapping[str, Any], inventory: Mapping[str, str]) -> dict[str, Any]:
    """Structural and request-identity checks of one sealed terminal row."""
    accession = request["accession_number"]
    if not isinstance(row, dict) or set(row) != _DIAGNOSTIC_ACQUISITION_TERMINAL_KEYS:
        raise NcenError(f"diagnostic_acquisition_terminal_keys_invalid:{accession}")
    expected_xpath = f"/{{{NCEN_NAMESPACE}}}edgarSubmission/{{{NCEN_NAMESPACE}}}schemaVersion"
    if (
        row["schema"] != _DIAGNOSTIC_ACQUISITION_EVIDENCE_SCHEMA
        or row["accession_number"] != accession
        or row["registrant_cik"] != request["registrant_cik"]
        or row["request_id"] != request["request_id"]
        or row["boundary_anomaly"] is not request["boundary_anomaly"]
        or row["schema_namespace"] != NCEN_NAMESPACE
        or row["schema_xpath"] != expected_xpath
    ):
        raise NcenError(f"diagnostic_acquisition_terminal_request_mismatch:{accession}")
    header = _diagnostic_exact_keys(
        row["header"],
        {"acceptance_at", "acceptance_raw", "document_sha256", "header_sha256", "locator", "retrieved_at", "url"},
        "diagnostic_acquisition_terminal_header",
    )
    request_header = request["header"]
    if (
        header["acceptance_at"] != request_header["acceptance_at"]
        or header["acceptance_raw"] != request_header["acceptance_raw"]
        or header["document_sha256"] != request_header["raw_sha256"]
        or header["header_sha256"] != request_header["header_sha256"]
        or header["retrieved_at"] != request_header["retrieved_at"]
        or header["url"] != request_header["url"]
        or not isinstance(header["locator"], str)
    ):
        raise NcenError(f"diagnostic_acquisition_terminal_header_mismatch:{accession}")
    dera = _diagnostic_exact_keys(
        row["dera"],
        {"package", "projection_digest", "row_locator", "source_refs"},
        "diagnostic_acquisition_terminal_dera",
    )
    if dera["package"] != request["dera"]["package"] or dera["row_locator"] != request["dera"]["row_locator"]:
        raise NcenError(f"diagnostic_acquisition_terminal_dera_mismatch:{accession}")
    reasons = row["quarantine_reasons"]
    if not isinstance(reasons, list) or any(not isinstance(value, str) for value in reasons):
        raise NcenError(f"diagnostic_acquisition_terminal_reasons_invalid:{accession}")
    if reasons != sorted(set(reasons)):
        raise NcenError(f"diagnostic_acquisition_terminal_reasons_invalid:{accession}")
    status = row["terminal_status"]
    if status not in {"verified", "quarantined"}:
        # Missing/transient/not-attempted terminals are not representable as exclusion classes.
        raise NcenError(f"diagnostic_acquisition_terminal_status_unaccounted:{accession}:{status}")
    if (status == "verified") != (not reasons) or row["failure_kind"] != (
        None if status == "verified" else "evidence_quarantine"
    ):
        raise NcenError(f"diagnostic_acquisition_terminal_status_inconsistent:{accession}")
    raw_xml = _diagnostic_exact_keys(
        row["raw_xml"],
        {"locator", "retrieved_at", "sha256", "size", "url"},
        "diagnostic_acquisition_terminal_raw_xml",
    )
    if not isinstance(raw_xml["locator"], str) or inventory.get(raw_xml["locator"]) != raw_xml["sha256"]:
        raise NcenError(f"diagnostic_acquisition_raw_xml_unbound:{accession}")
    _diagnostic_positive_int(raw_xml["size"], "diagnostic_acquisition_raw_xml_size")
    _diagnostic_exact_keys(
        row["xml"],
        {"amended_accession_number", "form_type", "registrant_cik", "report_period_end"},
        "diagnostic_acquisition_terminal_xml",
    )
    _diagnostic_exact_keys(
        row["rule_refs"],
        {"amendment_evidence_seal", "definitive_evidence_seal", "rule_version"},
        "diagnostic_acquisition_terminal_rule_refs",
    )
    if not isinstance(row["attempt_ids"], list) or type(row["schema_observed"]) is not bool:
        raise NcenError(f"diagnostic_acquisition_terminal_invalid:{accession}")
    return row


def _diagnostic_acquisition_record(
    root: Path,
    request: Mapping[str, Any],
    row: Mapping[str, Any],
    terminal_ref: tuple[str, str, int],
    inventory: Mapping[str, str],
    header_records_by_digest: Mapping[str, list[str]],
    opened: list[str],
) -> DiagnosticAcquisitionRecord | None:
    """Bind one terminal to its raw header, header record and XML bytes and re-derive it.

    The terminal's reasons, status, projection equality, schema and XML identity must equal a
    re-derivation from the raw evidence; its class comes from the exhaustive reason mapping.
    Returns ``None`` for a re-derived verified request that is not a boundary example.
    """
    from .sec_acquisition import SecHeaderError, parse_acceptance_header

    accession = request["accession_number"]
    registrant = request["registrant_cik"]
    header_meta = row["header"]
    locator = header_meta["locator"]
    if inventory.get(locator) != header_meta["document_sha256"]:
        raise NcenError(f"diagnostic_acquisition_header_unbound:{accession}")
    raw_header = _diagnostic_acquisition_read(
        root, locator, expected_sha256=inventory[locator], role="header_raw", opened=opened
    )
    retrieved = _diagnostic_acquisition_parse_time(header_meta["retrieved_at"], "header_retrieved_at")
    try:
        header = parse_acceptance_header(
            raw_header,
            accession_number=accession,
            url=header_meta["url"],
            document_sha256=header_meta["document_sha256"],
            retrieved_at=retrieved,
        )
    except (SecHeaderError, UnicodeDecodeError, ValueError) as exc:
        raise NcenError(f"diagnostic_acquisition_header_unparseable:{accession}") from exc
    if (
        header.header_sha256 != header_meta["header_sha256"]
        or header.acceptance_raw != header_meta["acceptance_raw"]
        or _diagnostic_timestamp(header.acceptance_at) != header_meta["acceptance_at"]
    ):
        raise NcenError(f"diagnostic_acquisition_header_evidence_mismatch:{accession}")
    record_paths = header_records_by_digest.get(request["header"]["record_sha256"], [])
    if len(record_paths) != 1:
        raise NcenError(f"diagnostic_acquisition_header_record_unresolved:{accession}")
    record_path = record_paths[0]
    record_bytes = _diagnostic_acquisition_read(
        root, record_path, expected_sha256=inventory[record_path], role="header_record", opened=opened
    )
    record = _diagnostic_acquisition_json(record_bytes, record_path, canonical=False)
    try:
        recorded = AcceptanceHeader.from_record(record)
    except (SecHeaderError, KeyError, TypeError, ValueError, AttributeError) as exc:
        raise NcenError(f"diagnostic_acquisition_header_record_invalid:{accession}") from exc
    if recorded != header:
        raise NcenError(f"diagnostic_acquisition_header_record_mismatch:{accession}")
    raw_meta = row["raw_xml"]
    xml = _diagnostic_acquisition_read(
        root, raw_meta["locator"], expected_sha256=raw_meta["sha256"], role="raw_xml", opened=opened
    )
    if len(xml) != raw_meta["size"]:
        raise NcenError(f"diagnostic_acquisition_raw_xml_size_mismatch:{accession}")
    facts = _diagnostic_acquisition_inspect_xml(xml)
    parsed = parse_ncen_primary_doc(
        xml,
        accession_number=accession,
        source_url=raw_meta["url"],
        retrieved_at=_diagnostic_acquisition_parse_time(raw_meta["retrieved_at"], "xml_retrieved_at"),
    )
    dera_digest = row["dera"]["projection_digest"]
    if not isinstance(dera_digest, str):
        raise NcenError(f"diagnostic_acquisition_dera_projection_invalid:{accession}")
    _diagnostic_hash_valid(dera_digest, "diagnostic_acquisition_dera_projection_digest")
    # Exact re-derivation of the acquisition's evidence rule from raw XML and header bytes.
    reasons = set(facts.reasons) | set(parsed.reasons)
    if parsed.status != "parsed":
        reasons.add("ncen_parser_quarantined")
    if parsed.schema_version != facts.schema_version:
        reasons.add("schema_parser_mismatch")
    if parsed.form_type != AMENDMENT_FORM or facts.form_type != AMENDMENT_FORM:
        reasons.add("xml_form_mismatch")
    if parsed.registrant_cik != registrant or facts.registrant_cik != registrant:
        reasons.add("xml_cik_mismatch")
    if parsed.report_period_end is None or facts.report_period_end is None:
        reasons.add("xml_report_period_missing")
    elif parsed.report_period_end.isoformat() != facts.report_period_end:
        reasons.add("xml_report_period_mismatch")
    if header.submission_type != parsed.form_type:
        reasons.add("xml_header_form_mismatch")
    if registrant not in header.filer_ciks:
        reasons.add("xml_header_cik_mismatch")
    xml_digest = parsed.projection_digest
    equality = "equal" if xml_digest == dera_digest else "conflict"
    if equality == "conflict":
        reasons.add("xml_dera_projection_conflict")
    derived_reasons = tuple(sorted(reasons))
    status = "quarantined" if derived_reasons else "verified"
    recorded_xml = {
        "amended_accession_number": facts.amended_accession_number,
        "form_type": facts.form_type,
        "registrant_cik": facts.registrant_cik,
        "report_period_end": facts.report_period_end,
    }
    if (
        list(derived_reasons) != row["quarantine_reasons"]
        or status != row["terminal_status"]
        or equality != row["projection_equality"]
        or facts.schema_observed is not row["schema_observed"]
        or facts.schema_version != row["schema_version"]
        or row["classification_basis"] != ("observed_xml" if facts.schema_observed else "unobserved")
        or recorded_xml != row["xml"]
    ):
        raise NcenError(f"diagnostic_acquisition_terminal_evidence_mismatch:{accession}")
    boundary = request["boundary_anomaly"]
    if status == "quarantined":
        if boundary:
            raise NcenError(f"diagnostic_acquisition_boundary_quarantined:{accession}")
        classification = _DIAGNOSTIC_ACQUISITION_CLASS_BY_REASONS.get(derived_reasons)
        if classification is None or (
            classification == "absent_amended_accession" and facts.amended_accession_number is not None
        ):
            # Unknown, multiple or merely malformed (present but invalid) evidence: STOP.
            raise NcenError(f"diagnostic_acquisition_classification_unknown:{accession}")
        role = "quarantine"
        ledger_reasons = derived_reasons
    else:
        if not boundary:
            return None
        if not facts.schema_observed or facts.schema_version != "X0505":
            # The acceptance boundary flag never supplies a schema; only observed XML does.
            raise NcenError(f"diagnostic_acquisition_boundary_schema_unobserved:{accession}")
        classification = None
        role = "boundary"
        ledger_reasons = _DIAGNOSTIC_BOUNDARY_REASONS
    try:
        period = None if facts.report_period_end is None else dt.date.fromisoformat(facts.report_period_end)
    except ValueError as exc:
        raise NcenError(f"diagnostic_acquisition_period_invalid:{accession}") from exc
    assert facts.form_type is not None
    return DiagnosticAcquisitionRecord(
        ledger_role=role,
        accession_number=accession,
        registrant_cik=registrant,
        form_type=facts.form_type,
        report_period_end=period,
        acceptance_at=header.acceptance_at,
        acceptance_raw=header.acceptance_raw,
        classification=classification,
        reasons=ledger_reasons,
        terminal_reasons=derived_reasons,
        terminal_status=status,
        schema_observed=facts.schema_observed,
        schema_version=facts.schema_version,
        projection_equality=equality,
        xml_projection_digest=xml_digest,
        dera_projection_digest=dera_digest,
        request_id=request["request_id"],
        header_sha256=header.header_sha256,
        terminal_ref=terminal_ref,
        header_raw_ref=(locator, inventory[locator], len(raw_header)),
        header_record_ref=(record_path, inventory[record_path], len(record_bytes)),
        raw_xml_ref=(raw_meta["locator"], raw_meta["sha256"], len(xml)),
    )


def read_diagnostic_acquisition_ledger(
    pin: DiagnosticAcquisitionPin,
    *,
    verify_full_inventory: bool = False,
) -> DiagnosticAcquisitionLedger:
    """Derive exact exclusion/boundary membership from one externally pinned acquisition seal.

    Read-only. Verifies the pinned ``SHA256SUMS`` and ``scope.json`` bytes, the closed file
    inventory (no links, junctions, reparse points, aliases or extras), the sealed receipt and
    coverage, every terminal (resolved from inventory entries, never guessed) against the scope
    request and ``schema_evidence.jsonl``; binds each quarantined or boundary terminal to its raw
    header, header record and XML bytes and re-derives it. ``verify_full_inventory`` additionally
    re-derives every verified request and stream-hashes every inventory entry. Counts are never
    used to establish membership.
    """
    if not isinstance(pin, DiagnosticAcquisitionPin):
        raise TypeError("pin_must_be_DiagnosticAcquisitionPin")
    root = _diagnostic_acquisition_root(pin.root)
    opened: list[str] = []
    sums = _diagnostic_acquisition_read(
        root, "SHA256SUMS", expected_sha256=pin.sha256sums_sha256, role="sha256sums", opened=opened
    )
    inventory = _diagnostic_acquisition_inventory(sums)
    files = _diagnostic_acquisition_closed_files(root)
    if set(files) != set(inventory) | _DIAGNOSTIC_ACQUISITION_SEAL_FILES:
        raise NcenError("diagnostic_acquisition_inventory_not_closed")
    inventory_bytes = sum(files[relative] for relative in inventory)
    if inventory.get("scope.json") != pin.scope_sha256:
        raise NcenError("diagnostic_acquisition_scope_sha256_mismatch")

    def entry(relative: str, role: str) -> bytes:
        digest = inventory.get(relative)
        if digest is None:
            raise NcenError(f"diagnostic_acquisition_entry_missing:{relative}")
        return _diagnostic_acquisition_read(root, relative, expected_sha256=digest, role=role, opened=opened)

    scope = _diagnostic_acquisition_json(entry("scope.json", "scope"), "scope.json", canonical=True)
    if entry("SCOPE.sha256", "scope_digest") != f"{pin.scope_sha256}  scope.json\n".encode("ascii"):
        raise NcenError("diagnostic_acquisition_scope_receipt_mismatch")
    coverage = _diagnostic_acquisition_json(entry("coverage.json", "coverage"), "coverage.json", canonical=True)
    evidence = entry("schema_evidence.jsonl", "schema_evidence")
    receipt = _diagnostic_exact_keys(
        _diagnostic_acquisition_json(
            _diagnostic_acquisition_read(
                root, "SHA256SUMS.receipt.json", expected_sha256=None, role="receipt", opened=opened
            ),
            "SHA256SUMS.receipt.json",
            canonical=True,
        ),
        {
            "coverage_sha256",
            "manifest_bytes",
            "manifest_entries",
            "schema",
            "schema_evidence_rows",
            "scope_sha256",
            "sealed_at",
            "sha256sums_sha256",
        },
        "diagnostic_acquisition_receipt",
    )
    if not isinstance(scope, dict) or not isinstance(scope.get("requests"), list):
        raise NcenError("diagnostic_acquisition_scope_invalid")
    if (
        scope.get("schema") != _DIAGNOSTIC_ACQUISITION_SCOPE_SCHEMA
        or scope.get("classification_contract") != _DIAGNOSTIC_ACQUISITION_CLASSIFICATION_CONTRACT
        or not isinstance(scope.get("cohort"), dict)
    ):
        raise NcenError("diagnostic_acquisition_scope_contract_invalid")
    requests = [_diagnostic_acquisition_request(item) for item in scope["requests"]]
    accessions = [item["accession_number"] for item in requests]
    if accessions != sorted(set(accessions)):
        raise NcenError("diagnostic_acquisition_scope_requests_not_sorted_unique")
    flagged = sum(1 for item in requests if item["boundary_anomaly"])
    cohort = scope["cohort"]
    if cohort.get("request_count") != len(requests) or cohort.get("boundary_anomaly_count") != flagged:
        raise NcenError("diagnostic_acquisition_scope_cohort_mismatch")
    if (
        receipt["schema"] != _DIAGNOSTIC_ACQUISITION_RECEIPT_SCHEMA
        or receipt["sha256sums_sha256"] != pin.sha256sums_sha256
        or receipt["scope_sha256"] != pin.scope_sha256
        or receipt["manifest_entries"] != len(inventory)
        or receipt["manifest_bytes"] != inventory_bytes
        or receipt["coverage_sha256"] != inventory.get("coverage.json")
        or receipt["schema_evidence_rows"] != len(requests)
    ):
        raise NcenError("diagnostic_acquisition_receipt_mismatch")
    if not evidence.endswith(b"\n") and evidence:
        raise NcenError("diagnostic_acquisition_schema_evidence_invalid")
    lines = [line + b"\n" for line in evidence.split(b"\n")[:-1]]
    if len(lines) != len(requests):
        raise NcenError("diagnostic_acquisition_schema_evidence_count_mismatch")
    terminals: dict[str, str] = {}
    for relative in inventory:
        if not relative.startswith("terminal/"):
            continue
        match = _DIAGNOSTIC_ACQUISITION_TERMINAL.fullmatch(relative)
        if match is None or match.group(1) in terminals:
            raise NcenError(f"diagnostic_acquisition_terminal_locator_invalid:{relative}")
        terminals[match.group(1)] = relative
    if set(terminals) != set(accessions):
        raise NcenError("diagnostic_acquisition_terminal_set_mismatch")
    header_records_by_digest: dict[str, list[str]] = defaultdict(list)
    for relative, digest in inventory.items():
        if relative.startswith("raw/header/") and relative.endswith(".json"):
            header_records_by_digest[digest].append(relative)
    exclusions: list[DiagnosticAcquisitionRecord] = []
    boundaries: list[DiagnosticAcquisitionRecord] = []
    statuses: Counter[str] = Counter()
    rederived = 0
    for request, line in zip(requests, lines, strict=True):
        accession = request["accession_number"]
        terminal_path = terminals[accession]
        terminal_bytes = entry(terminal_path, "terminal")
        if terminal_bytes != line:
            raise NcenError(f"diagnostic_acquisition_terminal_evidence_row_mismatch:{accession}")
        row = _diagnostic_acquisition_terminal(
            _diagnostic_acquisition_json(terminal_bytes, terminal_path, canonical=True), request, inventory
        )
        statuses[row["terminal_status"]] += 1
        non_member = row["terminal_status"] == "verified" and not request["boundary_anomaly"]
        if non_member:
            if row["projection_equality"] != "equal":
                raise NcenError(f"diagnostic_acquisition_terminal_status_inconsistent:{accession}")
            if not verify_full_inventory:
                # Terminal bytes are fixed by the pinned inventory; full mode re-derives them too.
                continue
        record = _diagnostic_acquisition_record(
            root,
            request,
            row,
            (terminal_path, inventory[terminal_path], len(terminal_bytes)),
            inventory,
            header_records_by_digest,
            opened,
        )
        rederived += 1
        if record is None:
            if not non_member:
                raise NcenError(f"diagnostic_acquisition_terminal_status_inconsistent:{accession}")
            continue
        (exclusions if record.ledger_role == "quarantine" else boundaries).append(record)
    boundary_anomalies = coverage.get("boundary_anomalies") if isinstance(coverage, dict) else None
    if (
        not isinstance(coverage, dict)
        or coverage.get("schema") != _DIAGNOSTIC_ACQUISITION_COVERAGE_SCHEMA
        or coverage.get("requested") != len(requests)
        or coverage.get("terminal_status_disjoint_total") != len(requests)
        or coverage.get("verified") != statuses["verified"]
        or coverage.get("quarantined") != statuses["quarantined"]
        or not isinstance(boundary_anomalies, dict)
        or boundary_anomalies.get("requested") != flagged
    ):
        raise NcenError("diagnostic_acquisition_coverage_mismatch")
    hashed = 0
    if verify_full_inventory:
        for relative, digest in sorted(inventory.items()):
            actual, size = _diagnostic_acquisition_hash_entry(root, relative)
            if actual != digest or size != files[relative]:
                raise NcenError(f"diagnostic_acquisition_sha256_mismatch:{relative}")
            hashed += 1
    ordered_exclusions = tuple(sorted(exclusions, key=lambda item: item.accession_number))
    ordered_boundaries = tuple(sorted(boundaries, key=lambda item: item.accession_number))
    return DiagnosticAcquisitionLedger(
        lane=pin.lane,
        scope_sha256=pin.scope_sha256,
        sha256sums_sha256=pin.sha256sums_sha256,
        exclusions=ordered_exclusions,
        boundaries=ordered_boundaries,
        requests=len(requests),
        verified_requests=statuses["verified"],
        inventory_entries=len(inventory),
        inventory_bytes=inventory_bytes,
        inventory_entries_hashed=hashed,
        rederived_requests=rederived,
        opened_paths=tuple(opened),
        exclusion_ledger_digest=_diagnostic_hash(
            _diagnostic_acquisition_digest_object(
                pin.lane, pin.scope_sha256, pin.sha256sums_sha256, ordered_exclusions, ordered_boundaries
            )
        ),
    )


def _diagnostic_acquisition_accounting(ledger: DiagnosticAcquisitionLedger) -> None:
    """Secondary accounting of the real sealed run (after exact membership, never instead)."""
    if ledger.class_counts() != dict(sorted(_DIAGNOSTIC_ACQUISITION_SEALED_CLASS_COUNTS.items())):
        raise NcenError("diagnostic_quarantine_coverage_mismatch")
    if len(ledger.boundaries) != _DIAGNOSTIC_ACQUISITION_SEALED_BOUNDARIES:
        raise NcenError("diagnostic_boundary_coverage_mismatch")


def _diagnostic_manifest_relative_dir(root: Path, raw: Any) -> Path:
    if not isinstance(raw, str) or not raw or "\\" in raw:
        raise NcenError("diagnostic_acquisition_root_unsafe")
    relative = Path(raw)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise NcenError("diagnostic_acquisition_root_unsafe")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise NcenError("diagnostic_acquisition_root_unsafe")
    try:
        resolved = current.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
    except (FileNotFoundError, ValueError) as exc:
        raise NcenError("diagnostic_acquisition_root_unsafe") from exc
    if not resolved.is_dir():
        raise NcenError("diagnostic_acquisition_root_unsafe")
    return current


def _diagnostic_resolve_acquisition(
    root: Path,
    spec: Mapping[str, Any],
    manifest_kind: str,
    trusted: DiagnosticAcquisitionPin | None,
) -> DiagnosticAcquisitionPin:
    """The acquisition pin whose derived ledger must equal the manifest's declared ledger.

    ``sealed_source`` requires a caller-supplied trusted pin (the real root is external and never
    named by the manifest). ``synthetic_fixture`` uses the trusted pin when supplied (it must equal
    the manifest's declared root and anchors); without one, the manifest-bound synthetic seal is
    used. That pin-less path exists only for the fixture lane, which never issues sealed-source
    authority, and only until trusted inputs are threaded through the export entry points (F7).
    """
    declared_root = spec["acquisition_root"]
    anchors = (spec["scope_sha256"], spec["sha256sums_sha256"])
    if manifest_kind == "sealed_source":
        if declared_root is not None:
            raise NcenError("diagnostic_acquisition_root_declared_for_sealed_source")
        if trusted is None:
            raise NcenError("diagnostic_acquisition_membership_unverified")
        resolved_declared = None
    else:
        resolved_declared = _diagnostic_manifest_relative_dir(root, declared_root)
        if trusted is None:
            return DiagnosticAcquisitionPin(resolved_declared.absolute(), *anchors, "synthetic_fixture")
    if not isinstance(trusted, DiagnosticAcquisitionPin):
        raise TypeError("trusted_acquisition_must_be_DiagnosticAcquisitionPin")
    if trusted.lane != manifest_kind:
        raise NcenError("diagnostic_acquisition_lane_mismatch")
    if (trusted.scope_sha256, trusted.sha256sums_sha256) != anchors:
        raise NcenError("diagnostic_acquisition_anchor_mismatch")
    if resolved_declared is not None and _diagnostic_acquisition_root(trusted.root) != resolved_declared.resolve(strict=True):
        raise NcenError("diagnostic_acquisition_root_mismatch")
    return trusted


def _diagnostic_compare_acquisition_ledger(
    ledger: DiagnosticAcquisitionLedger,
    exclusions: Sequence[DiagnosticSourceExclusion],
    boundaries: Sequence[DiagnosticBoundaryExample],
) -> None:
    """Declared ledger and boundary examples must equal the derived membership exactly."""
    derived_exclusions = sorted(_diagnostic_canonical(item.exclusion_identity()) for item in ledger.exclusions)
    declared_exclusions = sorted(
        _diagnostic_canonical([
            item.accession_number,
            item.registrant_cik,
            item.form_type,
            _diagnostic_optional_date(item.report_period_end),
            _diagnostic_optional_timestamp(item.acceptance_at),
            item.classification,
            list(item.reasons),
        ])
        for item in exclusions
    )
    if declared_exclusions != derived_exclusions:
        raise NcenError("diagnostic_acquisition_membership_mismatch:exclusions")
    derived_boundaries = sorted(_diagnostic_canonical(item.boundary_identity()) for item in ledger.boundaries)
    declared_boundaries = sorted(
        _diagnostic_canonical([
            item.accession_number,
            item.schema_version,
            item.form_type,
            _diagnostic_timestamp(item.acceptance_at),
            item.policy_state,
            list(item.reasons),
        ])
        for item in boundaries
    )
    if declared_boundaries != derived_boundaries:
        raise NcenError("diagnostic_acquisition_membership_mismatch:boundaries")


def _diagnostic_read_exclusions(
    root: Path, spec: Any, *, monitor: DiagnosticResourceMonitor
) -> tuple[DiagnosticSourceExclusion, ...]:
    item = _diagnostic_exact_keys(
        spec,
        {"path", "sha256", "bytes", "scope_sha256", "sha256sums_sha256", "acquisition_root"},
        "diagnostic_quarantine_ledger",
    )
    for name in ("scope_sha256", "sha256sums_sha256"):
        if not isinstance(item[name], str):
            raise NcenError("diagnostic_quarantine_seal_mismatch")
        _diagnostic_hash_valid(item[name], f"diagnostic_quarantine_{name}")
    path_label, path = _diagnostic_manifest_relative_path(root, item["path"])
    data = _diagnostic_verify_file(
        path,
        expected_sha256=item["sha256"],
        expected_size=_diagnostic_positive_int(item["bytes"], "diagnostic_ledger_size"),
        label="diagnostic_ledger",
        monitor=monitor,
        max_bytes=monitor.limits.ledger_max_bytes,
        phase="ledger_read",
    )
    try:
        payload = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NcenError("diagnostic_quarantine_ledger_invalid") from exc
    payload = _diagnostic_exact_keys(payload, {"schema_version", "records"}, "diagnostic_quarantine_ledger")
    if payload["schema_version"] != DIAGNOSTIC_QUARANTINE_LEDGER_VERSION or not isinstance(payload["records"], list):
        raise NcenError("diagnostic_quarantine_ledger_invalid")
    output = []
    for number, raw in enumerate(payload["records"], start=1):
        record = _diagnostic_exact_keys(
            raw,
            {"accession_number", "registrant_cik", "form_type", "report_period_end", "acceptance_at", "classification", "reasons"},
            "diagnostic_quarantine_record",
        )
        reasons = record["reasons"]
        if not isinstance(reasons, list) or any(not isinstance(value, str) for value in reasons):
            raise NcenError("diagnostic_quarantine_reasons_invalid")
        output.append(DiagnosticSourceExclusion(
            accession_number=record["accession_number"],
            registrant_cik=record["registrant_cik"],
            form_type=record["form_type"],
            report_period_end=_diagnostic_parse_date(record["report_period_end"], "diagnostic_quarantine_period"),
            acceptance_at=_diagnostic_parse_timestamp(record["acceptance_at"], "diagnostic_quarantine_acceptance"),
            classification=record["classification"],
            reasons=tuple(reasons),
            ledger_sha256=item["sha256"],
            ledger_size=item["bytes"],
            locator=f"{path_label}#record={number}",
        ))
    return tuple(sorted(output, key=lambda value: (value.accession_number, value.locator)))


def _diagnostic_read_boundaries(raw: Any) -> tuple[DiagnosticBoundaryExample, ...]:
    if not isinstance(raw, list):
        raise NcenError("diagnostic_boundary_examples_invalid")
    output = []
    for value in raw:
        item = _diagnostic_exact_keys(
            value,
            {"accession_number", "schema_version", "form_type", "acceptance_at", "policy_state", "reasons"},
            "diagnostic_boundary_example",
        )
        reasons = item["reasons"]
        if not isinstance(reasons, list) or any(not isinstance(reason, str) for reason in reasons):
            raise NcenError("diagnostic_boundary_reasons_invalid")
        acceptance = _diagnostic_parse_timestamp(item["acceptance_at"], "diagnostic_boundary_acceptance")
        assert acceptance is not None
        output.append(DiagnosticBoundaryExample(
            accession_number=item["accession_number"],
            schema_version=item["schema_version"],
            form_type=item["form_type"],
            acceptance_at=acceptance,
            policy_state=item["policy_state"],
            reasons=tuple(reasons),
        ))
    return tuple(sorted(output, key=lambda value: value.accession_number))


def _diagnostic_source_index_evidence_digest(
    manifest_sha256: str,
    rows: Sequence[DiagnosticSourceRow],
    exclusions: Sequence[DiagnosticSourceExclusion],
    boundaries: Sequence[DiagnosticBoundaryExample],
) -> str:
    return _diagnostic_source_index_evidence_digest_from_ids(
        manifest_sha256,
        [row.source_row_id for row in rows],
        exclusions,
        boundaries,
    )


def _diagnostic_source_index_evidence_digest_from_ids(
    manifest_sha256: str,
    row_ids: Sequence[str],
    exclusions: Sequence[DiagnosticSourceExclusion],
    boundaries: Sequence[DiagnosticBoundaryExample],
) -> str:
    return _diagnostic_hash([
        manifest_sha256,
        list(row_ids),
        [[item.accession_number, item.locator, item.classification] for item in exclusions],
        [item.accession_number for item in boundaries],
    ])


def _diagnostic_issue_source_index(
    *,
    manifest_kind: str,
    manifest_sha256: str,
    manifest_size: int,
    rows: Sequence[DiagnosticSourceRow],
    exclusions: tuple[DiagnosticSourceExclusion, ...],
    boundaries: tuple[DiagnosticBoundaryExample, ...],
    acquisition_ledger: DiagnosticAcquisitionLedger,
) -> DiagnosticSourceIndex:
    """Issue loader custody over rows already extracted from verified bytes (loader-only).

    Row IDs, the evidence digest and the accession/CIK lookups are computed exactly once here
    and stored immutably; consumers never rehash or rescan the whole index. The acquisition
    ledger must be the one the loader derived and compared for this manifest.
    """
    import types

    if type(acquisition_ledger) is not DiagnosticAcquisitionLedger or acquisition_ledger.lane != manifest_kind:
        raise NcenError("diagnostic_acquisition_membership_unverified")
    if manifest_kind != "synthetic_fixture":
        # Exact acquisition membership (F6) is verified at this point, but sealed-source authority
        # stays HOLD until the named trusted code/baseline pin roles (F7) exist; the fixture lane
        # is never promoted to it.
        raise NcenError("diagnostic_required_pins_unverified")
    work: Counter[str] = Counter({name: 0 for name in _DIAGNOSTIC_SOURCE_WORK_COUNTERS})
    work["index_passes"] += 1
    by_id: dict[str, DiagnosticSourceRow] = {}
    for row in rows:
        row_id = row.source_row_id
        work["row_ids_computed"] += 1
        existing = by_id.setdefault(row_id, row)
        if existing != row:
            raise NcenError("source_row_identity_conflict")
    row_ids = tuple(sorted(by_id))
    ordered = tuple(by_id[row_id] for row_id in row_ids)
    evidence_digest = _diagnostic_source_index_evidence_digest_from_ids(
        manifest_sha256, row_ids, exclusions, boundaries
    )
    work["evidence_digest_builds"] += 1
    rows_by_accession: dict[str, list[int]] = defaultdict(list)
    for position, row in enumerate(ordered):
        rows_by_accession[row.accession_number].append(position)
    exclusions_by_accession: dict[str, list[int]] = defaultdict(list)
    exclusions_by_cik: dict[str, list[int]] = defaultdict(list)
    for position, item in enumerate(exclusions):
        exclusions_by_accession[item.accession_number].append(position)
        if item.registrant_cik is not None:
            exclusions_by_cik[item.registrant_cik].append(position)
    work["lookup_builds"] += 1
    index = DiagnosticSourceIndex(
        ordered,
        exclusions,
        boundaries,
        manifest_sha256,
        manifest_size,
        manifest_kind,
        evidence_digest,
    )
    custody = object.__new__(_DiagnosticSourceCustody)
    payload: dict[str, Any] = {
        "lane": manifest_kind,
        "custody_version": _DIAGNOSTIC_SOURCE_CUSTODY_VERSION,
        "manifest_sha256": manifest_sha256,
        "manifest_size": manifest_size,
        "manifest_kind": manifest_kind,
        "evidence_digest": evidence_digest,
        "rows": index.rows,
        "row_ids": row_ids,
        "exclusions": index.exclusions,
        "boundary_examples": index.boundary_examples,
        "acquisition_ledger": acquisition_ledger,
        "exclusion_ledger_digest": acquisition_ledger.exclusion_ledger_digest,
        "_row_by_id": types.MappingProxyType(dict(zip(row_ids, ordered, strict=True))),
        "_rows_by_accession": types.MappingProxyType(
            {key: tuple(value) for key, value in rows_by_accession.items()}
        ),
        "_exclusions_by_accession": types.MappingProxyType(
            {key: tuple(value) for key, value in exclusions_by_accession.items()}
        ),
        "_exclusions_by_cik": types.MappingProxyType(
            {key: tuple(value) for key, value in exclusions_by_cik.items()}
        ),
        "_work": work,
    }
    for name, value in payload.items():
        object.__setattr__(custody, name, value)
    object.__setattr__(index, "_custody", custody)
    return index


def read_diagnostic_source_rows(
    source_manifest: DiagnosticSourceManifestPin,
    *,
    trusted_acquisition: DiagnosticAcquisitionPin | None = None,
    monitor: DiagnosticResourceMonitor | None = None,
) -> DiagnosticSourceIndex:
    """Read only exact files named by a cryptographically pinned sidecar manifest.

    The returned index is the only way to obtain loader custody. After the source artifacts are
    verified (F1), the declared quarantine ledger and boundary examples must equal, record for
    record, the membership derived from the pinned acquisition seal (F6); counts are secondary.
    ``sealed_source`` needs a caller-supplied ``trusted_acquisition`` pin: without it the loader
    fails closed with ``diagnostic_acquisition_membership_unverified`` before any artifact is
    read; with it, exact membership is verified and issuance still fails closed with
    ``diagnostic_required_pins_unverified`` (F7). Only ``synthetic_fixture`` is issued today.

    F13a: artifacts are preflighted as verified descriptors without retaining bytes, then loaded
    one at a time (bounded inline header/XML bytes or one private ZIP spool re-hashed from the
    held source descriptor). ``monitor`` (default: 7.5 GiB soft / 8 GiB in-process hard RSS)
    checks every read, spill and parse step and refuses typed; no row or member is dropped.
    F13b: DERA members stream as bounded rows into one disposable per-package join store next
    to that spool (deleted with it); accessions are projected one at a time.
    """
    if not isinstance(source_manifest, DiagnosticSourceManifestPin):
        raise TypeError("source_manifest_must_be_DiagnosticSourceManifestPin")
    if trusted_acquisition is not None and not isinstance(trusted_acquisition, DiagnosticAcquisitionPin):
        raise TypeError("trusted_acquisition_must_be_DiagnosticAcquisitionPin")
    if monitor is not None and not isinstance(monitor, DiagnosticResourceMonitor):
        raise TypeError("monitor_must_be_DiagnosticResourceMonitor")
    monitor = DiagnosticResourceMonitor() if monitor is None else monitor
    path = source_manifest.manifest_path
    data = _diagnostic_verify_file(
        path,
        expected_sha256=source_manifest.manifest_sha256,
        expected_size=source_manifest.manifest_size,
        label="diagnostic_manifest",
        monitor=monitor,
        max_bytes=monitor.limits.manifest_max_bytes,
        phase="manifest_read",
    )
    try:
        manifest = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NcenError("diagnostic_manifest_invalid") from exc
    del data
    manifest = _diagnostic_exact_keys(
        manifest,
        {"schema_version", "manifest_kind", "artifacts", "quarantine_ledger", "boundary_examples"},
        "diagnostic_manifest",
    )
    if manifest["schema_version"] != DIAGNOSTIC_SOURCE_MANIFEST_VERSION:
        raise NcenError("diagnostic_manifest_version_invalid")
    manifest_kind = manifest["manifest_kind"]
    if manifest_kind not in _DIAGNOSTIC_MANIFEST_KINDS or not isinstance(manifest["artifacts"], list):
        raise NcenError("diagnostic_manifest_kind_invalid")
    if manifest_kind == "sealed_source" and trusted_acquisition is None:
        # HOLD: real membership can only be derived from a caller-pinned acquisition seal.
        raise NcenError("diagnostic_acquisition_membership_unverified")
    root = path.parent.resolve(strict=True)
    descriptors: list[_DiagnosticArtifactDescriptor] = []
    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    common = {"artifact_id", "kind", "path", "sha256", "bytes", "retrieved_at", "public_at", "data_known_at"}
    extras = {
        "header": {"accession_number", "registrant_cik", "source_url"},
        "dera_zip": {"package_label", "members"},
        "edgar_xml": {"accession_number", "source_url", "header_artifact_id"},
    }
    for ordinal, raw in enumerate(manifest["artifacts"]):
        if not isinstance(raw, dict) or raw.get("kind") not in extras:
            raise NcenError("diagnostic_artifact_kind_invalid")
        artifact = _diagnostic_exact_keys(raw, common | extras[raw["kind"]], "diagnostic_artifact")
        artifact_id = artifact["artifact_id"]
        _diagnostic_nonempty(artifact_id, "artifact_id")
        if artifact_id in seen_ids:
            raise NcenError("diagnostic_artifact_id_duplicate")
        seen_ids.add(artifact_id)
        path_label, artifact_path = _diagnostic_manifest_relative_path(root, artifact["path"])
        if path_label in seen_paths:
            raise NcenError("diagnostic_artifact_path_duplicate")
        seen_paths.add(path_label)
        artifact_size = _diagnostic_positive_int(artifact["bytes"], "diagnostic_artifact_size")
        descriptors.append(
            _diagnostic_admit_artifact(ordinal, artifact, path_label, artifact_path, artifact_size, monitor)
        )
    header_by_id: dict[str, DiagnosticSourceRow] = {}
    header_by_accession: dict[str, DiagnosticSourceRow] = {}
    rows: list[DiagnosticSourceRow] = []
    spool = _DiagnosticSpool(monitor)
    try:
        for descriptor in descriptors:
            if descriptor.kind != "header":
                continue
            artifact_data = _diagnostic_load_inline(descriptor, monitor, "header_read")
            try:
                monitor.check("header_parse", descriptor.artifact_id)
                row = _diagnostic_header_row(
                    descriptor.artifact, descriptor.path_label, descriptor.path, artifact_data
                )
            finally:
                del artifact_data
                monitor.release_raw(descriptor.artifact_id)
                monitor.clear_artifact()
            if row.accession_number in header_by_accession:
                raise NcenError("diagnostic_header_duplicate")
            header_by_id[descriptor.artifact_id] = row
            header_by_accession[row.accession_number] = row
        rows.extend(header_by_id.values())
        for descriptor in descriptors:
            if descriptor.kind == "dera_zip":
                spool_path = spool.write(descriptor)
                try:
                    rows.extend(_diagnostic_dera_rows(
                        descriptor.artifact,
                        descriptor.path_label,
                        spool_path,
                        header_by_accession,
                        monitor=monitor,
                        store_path=spool.join_store_path(descriptor),
                    ))
                finally:
                    spool.discard(descriptor, spool_path)
                    monitor.clear_artifact()
            elif descriptor.kind == "edgar_xml":
                artifact_data = _diagnostic_load_inline(descriptor, monitor, "xml_read")
                try:
                    rows.extend(_diagnostic_xml_rows(
                        descriptor.artifact,
                        descriptor.path_label,
                        artifact_data,
                        header_by_id,
                        monitor=monitor,
                    ))
                finally:
                    del artifact_data
                    monitor.release_raw(descriptor.artifact_id)
                    monitor.clear_artifact()
    finally:
        spool.close()
    exclusions = _diagnostic_read_exclusions(root, manifest["quarantine_ledger"], monitor=monitor)
    boundaries = _diagnostic_read_boundaries(manifest["boundary_examples"])
    overlap = {row.accession_number for row in rows} & {item.accession_number for item in exclusions}
    if overlap:
        raise NcenError("diagnostic_quarantine_source_overlap")
    acquisition = _diagnostic_resolve_acquisition(
        root, manifest["quarantine_ledger"], manifest_kind, trusted_acquisition
    )
    monitor.check("acquisition_ledger")
    ledger = read_diagnostic_acquisition_ledger(acquisition)
    monitor.check("acquisition_ledger")
    _diagnostic_compare_acquisition_ledger(ledger, exclusions, boundaries)
    if manifest_kind == "sealed_source":
        _diagnostic_acquisition_accounting(ledger)
    monitor.check("issue_index")
    if not monitor.released():
        raise NcenError("diagnostic_resource_release_incomplete")
    index = _diagnostic_issue_source_index(
        manifest_kind=manifest_kind,
        manifest_sha256=source_manifest.manifest_sha256,
        manifest_size=source_manifest.manifest_size,
        rows=rows,
        exclusions=exclusions,
        boundaries=boundaries,
        acquisition_ledger=ledger,
    )
    # A refusal after issuance discards the index: no custody-bearing result escapes.
    monitor.check("issue_index")
    return index


def _diagnostic_copy_conflicts(rows: Sequence[DiagnosticSourceRow]) -> tuple[str, ...]:
    """Per-copy agreement over nullable tuples.

    Every sort uses the canonical JSON encoding as a total order: nulls stay JSON null,
    whole tuples keep role/series/identifier/name association and duplicates keep their
    multiplicity, so mixed FN-only/CRD-only/LEI-only or unresolved-series rows compare
    deterministically instead of raising on ``None`` versus ``str``.
    """
    by_copy: dict[str, list[DiagnosticSourceRow]] = defaultdict(list)
    for row in rows:
        if row.source_copy_id is not None:
            by_copy[row.source_copy_id].append(row)
    if len(by_copy) < 2:
        return ()
    reasons: set[str] = set()
    b5_values = []
    underwriter_relationships = []
    provider_name_maps = []
    for copy_rows in by_copy.values():
        b5 = [row for row in copy_rows if row.role == "b5"]
        b5_values.append(tuple(sorted(
            ((row.answer_raw, normalize_reported_name_key(row.name_raw)) for row in b5),
            key=_diagnostic_canonical,
        )))
        underwriter_relationships.append(tuple(sorted(
            (
                (
                    normalize_file_number(row.file_number_raw),
                    normalize_crd(row.crd_raw),
                    normalize_lei(row.lei_raw),
                )
                for row in copy_rows
                if row.role == "underwriter"
            ),
            key=_diagnostic_canonical,
        )))
        copy_names: dict[tuple[Any, ...], list[str]] = defaultdict(list)
        for row in copy_rows:
            if row.role not in DIAGNOSTIC_PROVIDER_ROLES or row.name_raw in {None, ""}:
                continue
            structural = (
                row.role,
                row.series_id,
                row.series_scope,
                normalize_file_number(row.file_number_raw),
                normalize_crd(row.crd_raw),
            )
            name_key = normalize_reported_name_key(row.name_raw)
            if name_key is not None:
                copy_names[structural].append(name_key)
        provider_name_maps.append(tuple(sorted(
            (
                (structural, tuple(sorted(names, key=_diagnostic_canonical)))
                for structural, names in copy_names.items()
            ),
            key=_diagnostic_canonical,
        )))
    if len(set(b5_values)) > 1:
        reasons.add("diagnostic_b5_copy_key_conflict")
    if len(set(underwriter_relationships)) > 1:
        reasons.add("diagnostic_underwriter_lei_copy_conflict")
    if len(set(provider_name_maps)) > 1:
        reasons.add("diagnostic_provider_name_copy_conflict")
    return tuple(sorted(reasons))


DIAGNOSTIC_AMENDMENT_SCHEMA_UNATTESTED = "diagnostic_amendment_schema_unattested"
DIAGNOSTIC_AMENDMENT_SCHEMA_CONFLICT = "diagnostic_amendment_schema_conflict"


def _diagnostic_amendment_schema_reason(
    filing: NcenFiling,
    header: DiagnosticSourceRow,
    cik: str,
    relationships: Sequence[DiagnosticSourceRow],
    eligible: Sequence[DiagnosticSourceRow],
) -> str | None:
    """Establish complete-replacement semantics of a selected ``N-CEN/A`` from eligible copies.

    The frozen merged filing may carry a covered schema inherited from an XML copy that is
    future, unheld or of unknown time at K/mode; merged fields never authorize eligible rows.
    Only an ``edgar_xml`` copy whose relationship rows are *all* eligible, bound to the attested
    header, identity- and projection-aligned with the selected filing and observing exactly the
    selected covered schema establishes the allowance. Schema-less DERA copies never do.

    Returns ``None`` when established, :data:`DIAGNOSTIC_AMENDMENT_SCHEMA_CONFLICT` when eligible
    XML copies observe differing schemas or a schema other than the selected one (fail closed),
    else :data:`DIAGNOSTIC_AMENDMENT_SCHEMA_UNATTESTED`. No era/date inference, no fallback.
    """
    observed = {
        row.schema_version
        for row in eligible
        if row.source_kind == "edgar_xml" and row.schema_version is not None
    }
    if len(observed) > 1 or (observed and observed != {filing.schema_version}):
        return DIAGNOSTIC_AMENDMENT_SCHEMA_CONFLICT
    if filing.schema_version not in AMENDMENT_COVERED_SCHEMA_VERSIONS:
        return DIAGNOSTIC_AMENDMENT_SCHEMA_UNATTESTED
    eligible_objects = {id(row) for row in eligible}
    header_id = header.source_row_id
    xml_copies: dict[str, list[DiagnosticSourceRow]] = defaultdict(list)
    for row in relationships:
        if row.source_kind == "edgar_xml" and row.source_copy_id is not None:
            xml_copies[row.source_copy_id].append(row)
    for copy_rows in xml_copies.values():
        if all(
            id(row) in eligible_objects
            and row.schema_version == filing.schema_version
            and row.registrant_cik == cik
            and row.header_source_id == header_id
            and row.form_type == filing.form_type
            and row.report_period_end == filing.report_period_end
            and row.acceptance_at == filing.acceptance_at
            and row.projection_digest == filing.projection_digest
            for row in copy_rows
        ):
            return None
    return DIAGNOSTIC_AMENDMENT_SCHEMA_UNATTESTED


def _diagnostic_quarantine_blocks(
    item: DiagnosticSourceExclusion,
    *,
    selected_accession: str,
    cik: str,
    report_date: dt.date,
    cutoff: dt.datetime,
    selected_period: dt.date | None,
    selected_acceptance: dt.datetime | None,
) -> bool:
    """Whether one quarantine record blocks the selected accession as of ``cutoff`` (pure).

    The exact rule of :func:`diagnostic_selection`: ordering uses only report period and exact
    acceptance time, never accession sequence or dates. Unknown values stay conservative. The
    predicate reads carrier values only; it confers no custody and consumes no source index.
    """
    if item.acceptance_at is not None and item.acceptance_at > cutoff:
        # Known acceptance after K is invisible as of K, including the selected accession.
        return False
    if item.accession_number == selected_accession:
        # K-eligible quarantine of the selected accession: no period, time or CIK cures it.
        return True
    if item.registrant_cik != cik:
        return False
    window_start = months_before(report_date, EFFECTIVE_WINDOW_MONTHS)
    period = item.report_period_end
    if period is None or selected_period is None:
        return period is None or window_start <= period <= report_date
    if not window_start <= period <= report_date or period < selected_period:
        return False
    if period > selected_period:
        return True
    # Same period: only an exactly ordered, strictly earlier quarantine is superseded.
    return (
        item.acceptance_at is None
        or selected_acceptance is None
        or item.acceptance_at >= selected_acceptance
    )


def _diagnostic_row_time_admissible(row: DiagnosticSourceRow, cutoff: dt.datetime, mode: str) -> bool:
    return (
        row.custody_state == "verified"
        and row.public_at is not None and row.public_at <= cutoff
        and row.data_known_at is not None and row.data_known_at <= cutoff
        and (mode != KNOWLEDGE_CURRENT_RUN or
             (row.retrieved_at is not None and row.retrieved_at <= cutoff))
    )


def _diagnostic_rebind_uncertain(
    rows: Sequence[DiagnosticSourceRow], reason: str,
) -> list[DiagnosticSourceRow]:
    if reason not in _DIAGNOSTIC_UNCERTAINTY_REASONS:
        raise NcenError("diagnostic_uncertainty_reason_invalid")
    return [dataclasses.replace(
        row, attestation="uncertain", reasons=tuple(sorted({*row.reasons, reason})),
        uncertain_expansion_eligible=True,
    ) for row in rows]


def _diagnostic_issue_selected(
    selected: DiagnosticSelection, *, index: NcenFilingIndex,
    custody: _DiagnosticSourceCustody, fund_keys: tuple[str, ...] | None,
    origin_ids: tuple[str, ...] = (),
) -> DiagnosticSelection:
    if fund_keys is not None:
        selected = _purpose_apply_voting_gaps(index, selected, fund_keys)
    return _diagnostic_issue_selection(
        selected, lane=custody.lane, manifest_sha256=custody.manifest_sha256,
        evidence_digest=custody.evidence_digest, ledger_digest=custody.exclusion_ledger_digest,
        origin_ids=origin_ids,
    )


def diagnostic_selection(
    index: NcenFilingIndex,
    source_index: DiagnosticSourceIndex,
    cik: str,
    report_date: dt.date,
    knowledge_cutoff: dt.datetime,
    *,
    mode: str,
    fund_keys: tuple[str, ...] | None = None,
) -> DiagnosticSelection:
    """Add exact source/copy/time attestation around the frozen effective selector.

    Requires loader custody (``diagnostic_source_index_unbound`` otherwise) and touches only
    the selected accession's rows and the quarantine records of that accession or CIK.
    """
    _check_mode(mode)
    cutoff = knowledge_cutoff
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise NcenError("datetime_not_timezone_aware")
    cutoff = cutoff.astimezone(UTC)
    custody = _diagnostic_bound_source_custody(source_index)
    effective = effective_filing(
        index,
        cik,
        report_date,
        cutoff,
        knowledge_mode=mode,
    )
    padded = normalize_cik(cik)
    if padded is None:
        padded = str(cik)
    filing = effective.filing
    base_reason = effective.reason
    accession = None if filing is None else filing.accession_number
    reasons: set[str] = set()
    if base_reason is not None:
        reasons.add(base_reason)
    if filing is None:
        return _diagnostic_issue_selected(DiagnosticSelection(
            padded,
            None,
            (),
            "incomplete",
            tuple(sorted(reasons or {"no_effective_filing"})),
            selection_reason=base_reason or "no_effective_filing",
            dependencies=effective.dependencies,
            knowledge_time=effective.knowledge_time,
            report_date=report_date,
            knowledge_cutoff=cutoff,
            mode=mode,
        ), index=index, custody=custody, fund_keys=fund_keys)
    assert accession is not None
    # Quarantine competitors are ordered only by recorded report period and exact acceptance
    # time; accession numbers and filing dates never imply order.  Non-blocking records stay
    # in the source index exclusions for audit.
    quarantined_competitors = [
        item
        for item in custody.quarantine_candidates(accession, padded)
        if _diagnostic_quarantine_blocks(
            item,
            selected_accession=accession,
            cik=padded,
            report_date=report_date,
            cutoff=cutoff,
            selected_period=filing.report_period_end,
            selected_acceptance=filing.acceptance_at,
        )
    ]
    if quarantined_competitors:
        reason = "diagnostic_quarantined_competitor"
        return _diagnostic_issue_selected(DiagnosticSelection(
            padded,
            accession,
            (),
            "incomplete",
            tuple(sorted({*reasons, reason})),
            selection_reason=reason,
            selected_projection_digest=filing.projection_digest,
            dependencies=effective.dependencies,
            knowledge_time=effective.knowledge_time,
            report_date=report_date,
            knowledge_cutoff=cutoff,
            mode=mode,
        ), index=index, custody=custody, fund_keys=fund_keys)
    source_rows = custody.rows_for(accession)
    headers = [row for row in source_rows if row.role == "header"]
    relationships = [row for row in source_rows if row.role in DIAGNOSTIC_ROLES]
    if len(headers) != 1 or filing.acceptance_at is None:
        reason = "filing_acceptance_unattested"
        return _diagnostic_issue_selected(DiagnosticSelection(
            padded,
            accession,
            (),
            "incomplete",
            tuple(sorted({*reasons, reason})),
            selection_reason=reason,
            selected_projection_digest=filing.projection_digest,
            dependencies=effective.dependencies,
            knowledge_time=effective.knowledge_time,
            report_date=report_date,
            knowledge_cutoff=cutoff,
            mode=mode,
        ), index=index, custody=custody, fund_keys=fund_keys)
    header = headers[0]
    if (
        header.acceptance_at is None
        or header.acceptance_at != filing.acceptance_at
        or header.form_type != filing.form_type
        or header.registrant_cik != padded
        or header.report_period_end != filing.report_period_end
    ):
        reason = "filing_acceptance_unattested"
        relationships = []
    elif header.acceptance_at > cutoff:
        reason = "filing_acceptance_after_cutoff"
        relationships = []
    elif mode == KNOWLEDGE_CURRENT_RUN and (header.retrieved_at is None or header.retrieved_at > cutoff):
        reason = "diagnostic_source_unheld"
        relationships = []
    else:
        reason = base_reason
    eligible: list[DiagnosticSourceRow] = []
    excluded_ids: list[str] = []
    projection_mismatch = False
    for row in relationships:
        if not _diagnostic_row_time_admissible(row, cutoff, mode):
            excluded_ids.append(row.source_row_id)
            continue
        identity_conflict = (
            row.registrant_cik != padded
            or row.header_source_id != header.source_row_id
            or row.form_type != filing.form_type
            or row.report_period_end != filing.report_period_end
            or row.acceptance_at != filing.acceptance_at
        )
        projection_mismatch = projection_mismatch or row.projection_digest != filing.projection_digest
        if identity_conflict:
            reason = "diagnostic_source_identity_conflict"
            reasons.add(reason)
        eligible.append(row)
    if relationships and not eligible:
        reason = reason or "diagnostic_source_unheld"
    if filing.data_known_at is None or filing.data_known_at > cutoff:
        reason = "effective_data_not_known_at_cutoff"
        excluded_ids.extend(row.source_row_id for row in eligible)
        eligible = []
    if mode == KNOWLEDGE_CURRENT_RUN and (filing.retrieved_at is None or filing.retrieved_at > cutoff):
        reason = "diagnostic_source_unheld"
        excluded_ids.extend(row.source_row_id for row in eligible)
        eligible = []
    uncertainty_reasons = _DIAGNOSTIC_UNCERTAINTY_REASONS
    conflicts = _diagnostic_copy_conflicts(eligible)
    reasons.update(conflicts)
    reason = reason or (conflicts[0] if conflicts else None)
    if projection_mismatch and not conflicts:
        reason = reason or "diagnostic_source_identity_conflict"
        reasons.add("diagnostic_source_identity_conflict")
    if effective.amendment_semantics == AMENDMENT_COMPLETE and eligible:
        # F3: the frozen allowance may come from merged fields of an ineligible XML copy;
        # re-establish it from eligible copies only. Earlier blocking reasons keep precedence.
        schema_reason = _diagnostic_amendment_schema_reason(filing, header, padded, relationships, eligible)
        if schema_reason == DIAGNOSTIC_AMENDMENT_SCHEMA_CONFLICT:
            reasons.add(schema_reason)
            if reason is None or reason in uncertainty_reasons:
                reason = schema_reason
        elif schema_reason is not None:
            # As of K only schema-less evidence is admissible: exactly the frozen unknown case.
            reasons.add(schema_reason)
            reason = reason or AMENDMENT_UNKNOWN_REASON
    if reason is not None:
        reasons.add(reason)
    origin_ids = tuple(sorted(row.source_row_id for row in eligible))
    if reason is not None and eligible:
        uncertainty_allowed = reason in uncertainty_reasons
        if uncertainty_allowed:
            eligible = _diagnostic_rebind_uncertain(eligible, reason)
        else:
            excluded_ids.extend(row.source_row_id for row in eligible)
            eligible = []
    if reason is None and not eligible:
        reason = "diagnostic_source_rows_unavailable"
        reasons.add(reason)
    profile = family_profile(filing)
    reasons.update(profile.reasons)
    state = "complete" if not reasons else "incomplete"
    ordered = DiagnosticSourceIndex.from_rows(eligible).rows
    return _diagnostic_issue_selected(DiagnosticSelection(
        padded,
        accession,
        ordered,
        state,
        tuple(sorted(reasons)),
        selection_reason=reason,
        selected_projection_digest=filing.projection_digest,
        dependencies=effective.dependencies,
        knowledge_time=effective.knowledge_time,
        report_date=report_date,
        knowledge_cutoff=cutoff,
        mode=mode,
        excluded_source_row_ids=tuple(sorted(set(excluded_ids))),
    ), index=index, custody=custody, fund_keys=fund_keys, origin_ids=origin_ids)


def diagnostic_fixture_selection(
    *, cik: str, accession_number: str, rows: Iterable[DiagnosticSourceRow],
    report_date: dt.date, knowledge_cutoff: dt.datetime, mode: str,
    reasons: tuple[str, ...] = (), selection_reason: str | None = None,
) -> DiagnosticSelection:
    """Admit raw, caller-owned rows to the non-exportable pure fixture lane only."""
    _diagnostic_normalized_cik(cik)
    _check_mode(mode)
    cutoff = knowledge_cutoff.astimezone(UTC) if knowledge_cutoff.tzinfo is not None else knowledge_cutoff
    _diagnostic_timestamp(cutoff)
    _diagnostic_reasons(reasons)
    raw = tuple(rows)
    if any(
        type(item) is not DiagnosticSourceRow or item.source_kind != "synthetic"
        or item.artifact_id is not None or item.source_copy_id is not None
        or item.role not in DIAGNOSTIC_ROLES or item.registrant_cik != cik
        or item.accession_number != accession_number or item.attestation != "attested"
        or item.reasons or item.uncertain_expansion_eligible
        for item in raw
    ):
        raise NcenError("diagnostic_fixture_raw_row_invalid")
    eligible = [item for item in raw if _diagnostic_row_time_admissible(item, cutoff, mode)]
    excluded = [item.source_row_id for item in raw if not _diagnostic_row_time_admissible(item, cutoff, mode)]
    origin_ids = tuple(sorted(item.source_row_id for item in eligible))
    if selection_reason is not None:
        if selection_reason in _DIAGNOSTIC_UNCERTAINTY_REASONS:
            eligible = _diagnostic_rebind_uncertain(eligible, selection_reason)
        else:
            excluded.extend(item.source_row_id for item in eligible)
            eligible = []
    all_reasons = tuple(sorted({*reasons, *((selection_reason,) if selection_reason else ())}))
    selection = DiagnosticSelection(
        cik, accession_number, DiagnosticSourceIndex.from_rows(eligible).rows,
        "incomplete" if all_reasons else "complete", all_reasons,
        selection_reason=selection_reason, report_date=report_date, knowledge_cutoff=cutoff,
        mode=mode, excluded_source_row_ids=tuple(sorted(set(excluded))),
    )
    payload_digest = _diagnostic_hash([item.source_row_id for item in raw])
    return _diagnostic_issue_selection(
        selection, lane="pure_fixture", manifest_sha256=payload_digest,
        evidence_digest=payload_digest, ledger_digest=_diagnostic_hash([]),
        origin_ids=origin_ids,
    )


def _diagnostic_nodes_digest(nodes: tuple[DependenceNode, ...]) -> str:
    return _diagnostic_hash([dataclasses.asdict(item) for item in nodes])


def _diagnostic_incidences_digest(incidences: tuple[TypedIncidence, ...]) -> str:
    return _diagnostic_hash([TypedIncidence.identity_payload(item) for item in incidences])


class _DiagnosticContextAdmission:
    __slots__ = ("_seal", "baseline_digest", "cohort_digest", "context_id", "incidence_ids",
                 "incidences", "incidences_digest", "inventory_digest", "knowledge_cutoff",
                 "lane", "ledger_digest", "mode", "ncen_evidence_digest", "nodes",
                 "nodes_digest", "report_date", "selection_digests", "selections")

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        raise TypeError("diagnostic_context_admission_factory_only")

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("diagnostic_context_admission_immutable")

    def __reduce_ex__(self, protocol: Any) -> Any:
        raise TypeError("diagnostic_context_admission_not_serializable")


@dataclass(frozen=True, slots=True)
class DiagnosticContext:
    context_id: str
    report_date: dt.date
    knowledge_cutoff: dt.datetime
    mode: str
    inventory_digest: str
    cohort_digest: str
    ncen_evidence_digest: str
    exclusion_ledger_digest: str
    baseline_digest: str | None
    selections: tuple[DiagnosticSelection, ...]
    reported_families: tuple[ReportedFamily, ...]
    nodes: tuple[DependenceNode, ...]
    incidences: tuple[TypedIncidence, ...]
    _admission: _DiagnosticContextAdmission | None = field(default=None, init=False, repr=False, compare=False)

    def __getstate__(self) -> list[Any]:
        return [None if item.name == "_admission" else getattr(self, item.name)
                for item in dataclasses.fields(self)]


def _diagnostic_bound_context_admission(context: DiagnosticContext) -> _DiagnosticContextAdmission:
    if type(context) is not DiagnosticContext:
        raise NcenError("diagnostic_graph_admission_missing")
    bound = context._admission
    if (
        type(bound) is not _DiagnosticContextAdmission or bound._seal is not _DIAGNOSTIC_ADMISSION_SEAL
        or context.selections is not bound.selections or context.nodes is not bound.nodes
        or context.incidences is not bound.incidences
        or context.context_id != bound.context_id or context.report_date != bound.report_date
        or context.knowledge_cutoff != bound.knowledge_cutoff or context.mode != bound.mode
        or context.inventory_digest != bound.inventory_digest
        or context.cohort_digest != bound.cohort_digest
        or context.ncen_evidence_digest != bound.ncen_evidence_digest
        or context.exclusion_ledger_digest != bound.ledger_digest
        or context.baseline_digest != bound.baseline_digest
        or tuple(_diagnostic_bound_selection_admission(item).selection_digest
                 for item in context.selections) != bound.selection_digests
        or _diagnostic_nodes_digest(context.nodes) != bound.nodes_digest
        or tuple(_diagnostic_id("ncenrow:incidence", TypedIncidence.identity_payload(item))
                 for item in context.incidences) != bound.incidence_ids
        or _diagnostic_incidences_digest(context.incidences) != bound.incidences_digest
    ):
        raise NcenError("diagnostic_graph_admission_missing")
    return bound


def _diagnostic_bound_projection_admission(projection: DependenceProjection) -> _DiagnosticContextAdmission:
    if type(projection) is not DependenceProjection:
        raise NcenError("diagnostic_projection_unadmitted")
    bound = projection._admission
    if (
        type(bound) is not _DiagnosticContextAdmission or bound._seal is not _DIAGNOSTIC_ADMISSION_SEAL
        or projection.context_id != bound.context_id or projection.nodes is not bound.nodes
        or projection.incidences is not bound.incidences
        or _diagnostic_nodes_digest(projection.nodes) != bound.nodes_digest
        or _diagnostic_incidences_digest(projection.incidences) != bound.incidences_digest
    ):
        raise NcenError("diagnostic_projection_unadmitted")
    return bound


def _diagnostic_build_context(
    selections: tuple[DiagnosticSelection, ...], *, report_date: dt.date,
    knowledge_cutoff: dt.datetime, mode: str, inventory_digest: str,
    cohort_digest: str, ncen_evidence_digest: str, exclusion_ledger_digest: str,
    baseline_digest: str | None = None,
) -> DiagnosticContext:
    _check_mode(mode)
    cutoff = knowledge_cutoff.astimezone(UTC) if knowledge_cutoff.tzinfo is not None else knowledge_cutoff
    _diagnostic_timestamp(cutoff)
    admissions = tuple(_diagnostic_bound_selection_admission(item) for item in selections)
    lanes = {item.lane for item in admissions}
    if len(lanes) != 1 or any(
        item.report_date != report_date or item.knowledge_cutoff != cutoff or item.mode != mode
        for item in admissions
    ):
        raise NcenError("diagnostic_graph_admission_missing")
    if not admissions or tuple(item.cik for item in selections) != tuple(sorted({item.cik for item in selections})):
        raise NcenError("diagnostic_context_ciks_not_sorted_unique")
    lane = admissions[0].lane
    if lane == "sealed_source":
        raise NcenError("diagnostic_required_pins_unverified")
    if lane not in {"pure_fixture", "synthetic_fixture"}:
        raise NcenError("diagnostic_graph_admission_missing")
    if lane == "synthetic_fixture" and any(
        item.evidence_digest != ncen_evidence_digest
        or item.ledger_digest != admissions[0].ledger_digest
        or item.manifest_sha256 != admissions[0].manifest_sha256 for item in admissions
    ):
        raise NcenError("diagnostic_graph_admission_missing")
    context_id = diagnostic_context_id(
        report_date=report_date, knowledge_cutoff=cutoff, mode=mode,
        inventory_digest=inventory_digest, cohort_digest=cohort_digest,
        ncen_evidence_digest=ncen_evidence_digest,
        exclusion_ledger_digest=exclusion_ledger_digest,
    )
    families = tuple(reported_family_for(item) for item in selections)
    nodes = tuple(DependenceNode(context_id, item.cik, item.evidence_state, item.reasons, family)
                  for item, family in zip(selections, families, strict=True))
    incidences = tuple(sorted((edge for item, family in zip(selections, families, strict=True)
                               for edge in typed_incidences_for(item, context_id=context_id, reported_family=family)),
                              key=lambda edge: edge.incidence_id))
    context = DiagnosticContext(context_id, report_date, cutoff, mode, inventory_digest,
                                cohort_digest, ncen_evidence_digest, exclusion_ledger_digest,
                                baseline_digest, selections, families, nodes, incidences)
    bound = object.__new__(_DiagnosticContextAdmission)
    values = {
        "_seal": _DIAGNOSTIC_ADMISSION_SEAL, "lane": lane, "context_id": context_id,
        "report_date": report_date, "knowledge_cutoff": cutoff, "mode": mode,
        "inventory_digest": inventory_digest, "cohort_digest": cohort_digest,
        "ncen_evidence_digest": ncen_evidence_digest, "ledger_digest": exclusion_ledger_digest,
        "baseline_digest": baseline_digest,
        "selection_digests": tuple(item.selection_digest for item in admissions),
        "selections": selections, "nodes": nodes, "incidences": incidences,
        "nodes_digest": _diagnostic_nodes_digest(nodes),
        "incidence_ids": tuple(item.incidence_id for item in incidences),
        "incidences_digest": _diagnostic_incidences_digest(incidences),
    }
    for name, value in values.items():
        object.__setattr__(bound, name, value)
    object.__setattr__(context, "_admission", bound)
    return context


def diagnostic_fixture_context(
    selections: tuple[DiagnosticSelection, ...], *, report_date: dt.date,
    knowledge_cutoff: dt.datetime, mode: str = KNOWLEDGE_HISTORICAL,
    inventory_digest: str = "inventory-a", cohort_digest: str = "2" * 64,
    ncen_evidence_digest: str = "3" * 64, exclusion_ledger_digest: str = "4" * 64,
) -> DiagnosticContext:
    if any(_diagnostic_bound_selection_admission(item).lane != "pure_fixture" for item in selections):
        raise NcenError("diagnostic_fixture_lane_not_exportable")
    return _diagnostic_build_context(
        selections, report_date=report_date, knowledge_cutoff=knowledge_cutoff, mode=mode,
        inventory_digest=inventory_digest, cohort_digest=cohort_digest,
        ncen_evidence_digest=ncen_evidence_digest, exclusion_ledger_digest=exclusion_ledger_digest,
    )


# === FE-1 diagnostic synthetic export and sealed envelope ===============================
# Additive, offline and outcome-blind.  Full-cohort replay, W0 persistence and admission
# remain separate gates; this section only consumes a sealed slim cohort or trusted checkpoint.

DIAGNOSTIC_COHORT_MANIFEST_VERSION = "ncen_diagnostic_cohort_manifest_v1"
DIAGNOSTIC_EXPORT_VERSION = "ncen_purpose_diagnostics_v2"
DIAGNOSTIC_DECLARATION_VERSION = "ncen_purpose_diagnostics_declaration_v2"
DIAGNOSTIC_MANIFEST_VERSION = "ncen_purpose_diagnostics_manifest_v2"
DIAGNOSTIC_PERFORMANCE_VERSION = "ncen_purpose_diagnostics_performance_v2"
DIAGNOSTIC_CHECKS_VERSION = "ncen_purpose_diagnostics_checks_v2"
DIAGNOSTIC_RECEIPT_VERSION = "ncen_purpose_diagnostics_receipt_v2"
DIAGNOSTIC_SELECTION_REPLAY_VERSION = "ncen_purpose_selection_replay_v1"
DIAGNOSTIC_MEMORY_LIMIT_BYTES = 8 * 1024**3
DIAGNOSTIC_MEMORY_SOFT_LIMIT_BYTES = 15 * 1024**3 // 2
DIAGNOSTIC_OUTCOME_INPUT_ALLOWLIST = (
    "ncen_filing_index",
    "ncen_source_manifest",
    "sealed_diagnostic_cohort",
    "source_quarantine_ledger",
)
DIAGNOSTIC_EXPORT_JSONL = (
    ("cohort.jsonl", "cohort_member"),
    ("sources.jsonl", "source_row"),
    ("exclusions.jsonl", "source_exclusion"),
    ("selections.jsonl", "selection_context"),
    ("contexts.jsonl", "context"),
    ("nodes.jsonl", "node"),
    ("reported_families.jsonl", "reported_family"),
    ("incidences.jsonl", "incidence"),
    ("components.jsonl", "component"),
    ("memberships.jsonl", "membership"),
    ("key_degrees.jsonl", "key_degree"),
    ("spanning.jsonl", "spanning_union"),
    ("summaries.jsonl", "summary"),
    ("transitions.jsonl", "transition"),
    ("fold_groups.jsonl", "fold_group"),
    ("fold_memberships.jsonl", "fold_membership"),
)
_DIAGNOSTIC_COHORT_SEAL = object()


# C1a pins are an external input, not an assertion issued by the diagnostic export.
_PURPOSE_SINGLETON_ROLES = frozenset({
    "runtime_code_manifest", "baseline_v3", "baseline_stage1", "baseline_stage2a",
    "baseline_stage2b", "diagnostic_baseline_checkpoint", "cohort_checkpoint",
    "cohort_derivation_receipt", "ncen_source_manifest", "ncen_index_manifest",
    "ncen_parsed_copies", "ncen_raw_audit_manifest", "acquisition_scope", "acquisition_sha256sums",
    "acquisition_ledger", "source_reconciliation", "synthetic_membership_definition",
    "synthetic_membership_inventory",
})
_PURPOSE_COLLECTION_ROLES = frozenset({
    "ncen_dera_artifacts", "ncen_xml_artifacts", "ncen_header_artifacts",
    "ncen_index_artifacts",
})
_PURPOSE_TRUST_ROLES = _PURPOSE_SINGLETON_ROLES | _PURPOSE_COLLECTION_ROLES
_PURPOSE_V3_SHA256 = "41521014f92ba1baf7ed15e4235005438e2c6bc52a6f47851bbb16bf184a9b29"
# Captured at module import; a file changed and repinned after import is not loaded code.
_PURPOSE_LOADED_CODE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


@dataclass(frozen=True, slots=True)
class DiagnosticTrustPin:
    manifest_path: Path
    manifest_sha256: str
    manifest_size: int


@dataclass(frozen=True, slots=True)
class DiagnosticCohortMember:
    report_date: dt.date
    cik: str
    fund_keys: tuple[str, ...]
    known_at: dt.datetime

    def __post_init__(self) -> None:
        if not isinstance(self.report_date, dt.date) or isinstance(self.report_date, dt.datetime):
            raise NcenError("diagnostic_cohort_report_date_invalid")
        _diagnostic_normalized_cik(self.cik)
        if self.fund_keys != tuple(sorted(set(self.fund_keys))) or not self.fund_keys:
            raise NcenError("diagnostic_cohort_fund_keys_invalid")
        if any(not isinstance(item, str) or not item for item in self.fund_keys):
            raise NcenError("diagnostic_cohort_fund_key_invalid")
        _diagnostic_timestamp(self.known_at)


def _diagnostic_inventory_source_payload(source: InventorySource) -> dict[str, Any]:
    _diagnostic_hash_valid(source.zip_sha256, "inventory_source_sha256")
    _diagnostic_nonempty(source.package_id, "inventory_package_id")
    _diagnostic_nonempty(source.package_label, "inventory_package_label")
    return {
        "first_verified_public_at": _diagnostic_timestamp(source.first_verified_public_at),
        "package_id": source.package_id,
        "package_label": source.package_label,
        "retrieved_at": _diagnostic_timestamp(source.retrieved_at),
        "zip_sha256": source.zip_sha256,
    }


def _diagnostic_cohort_digest(
    *,
    knowledge_cutoff: dt.datetime,
    knowledge_mode: str,
    inventory_digest: str,
    sources: tuple[InventorySource, ...],
    members: tuple[DiagnosticCohortMember, ...],
) -> str:
    return _diagnostic_hash(
        [
            DIAGNOSTIC_COHORT_VERSION,
            _diagnostic_timestamp(knowledge_cutoff),
            knowledge_mode,
            inventory_digest,
            [_diagnostic_inventory_source_payload(source) for source in sources],
            [
                [
                    member.report_date.isoformat(),
                    member.cik,
                    list(member.fund_keys),
                    _diagnostic_timestamp(member.known_at),
                ]
                for member in members
            ],
            {
                "derivation": "sealed_vote_inventory_full_universe",
                "full_cohort": True,
                "inventory_version": INVENTORY_VERSION,
                "outcome_fields_excluded": True,
            },
        ]
    )


@dataclass(frozen=True, slots=True)
class DiagnosticCohort:
    knowledge_cutoff: dt.datetime
    knowledge_mode: str
    inventory_digest: str
    sources: tuple[InventorySource, ...]
    members: tuple[DiagnosticCohortMember, ...]
    cohort_digest: str
    extraction_version: str
    derivation: str
    full_cohort: bool
    outcome_fields_excluded: bool
    _seal: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _DIAGNOSTIC_COHORT_SEAL:
            raise NcenError("diagnostic_cohort_unsealed")
        _diagnostic_timestamp(self.knowledge_cutoff)
        _check_mode(self.knowledge_mode)
        _diagnostic_nonempty(self.inventory_digest, "inventory_digest")
        if self.extraction_version != DIAGNOSTIC_COHORT_VERSION:
            raise NcenError("diagnostic_cohort_version_invalid")
        if self.derivation != "sealed_vote_inventory_full_universe":
            raise NcenError("diagnostic_cohort_derivation_invalid")
        if not self.full_cohort or not self.outcome_fields_excluded:
            raise NcenError("diagnostic_cohort_provenance_invalid")
        source_keys = tuple(source.package_label for source in self.sources)
        if source_keys != tuple(sorted(set(source_keys))) or not source_keys:
            raise NcenError("diagnostic_cohort_sources_invalid")
        member_keys = tuple((item.report_date, item.cik) for item in self.members)
        if member_keys != tuple(sorted(set(member_keys))) or not member_keys:
            raise NcenError("diagnostic_cohort_members_invalid")
        expected = _diagnostic_cohort_digest(
            knowledge_cutoff=self.knowledge_cutoff,
            knowledge_mode=self.knowledge_mode,
            inventory_digest=self.inventory_digest,
            sources=self.sources,
            members=self.members,
        )
        if self.cohort_digest != expected:
            raise NcenError("diagnostic_cohort_digest_mismatch")


@dataclass(frozen=True, slots=True)
class DiagnosticCohortManifestPin:
    manifest_path: Path
    manifest_sha256: str
    manifest_size: int
    sha256sums_sha256: str

    def __post_init__(self) -> None:
        if self.manifest_path.suffix.lower() in {".pkl", ".pickle"}:
            raise NcenError("diagnostic_cohort_pickle_forbidden")
        _diagnostic_hash_valid(self.manifest_sha256, "cohort_manifest_sha256")
        _diagnostic_positive_int(self.manifest_size, "cohort_manifest_size")
        _diagnostic_hash_valid(self.sha256sums_sha256, "cohort_sha256sums_sha256")


def diagnostic_cohort_from_inventory(inventory: VoteInventory) -> DiagnosticCohort:
    """Copy the complete FE-1a universe without votes, targets or fingerprints."""
    trusted = _require_inventory(
        inventory,
        inventory.knowledge_cutoff,
        inventory.knowledge_mode,
    )
    members = tuple(
        DiagnosticCohortMember(
            report_date=report_date,
            cik=cik,
            fund_keys=tuple(sorted(fund_keys)),
            known_at=trusted.known_at[report_date],
        )
        for report_date, voters in sorted(trusted.voters.items())
        for cik, fund_keys in sorted(voters.items())
    )
    sources = tuple(sorted(trusted.sources, key=lambda item: item.package_label))
    digest = _diagnostic_cohort_digest(
        knowledge_cutoff=trusted.knowledge_cutoff,
        knowledge_mode=trusted.knowledge_mode,
        inventory_digest=trusted.digest,
        sources=sources,
        members=members,
    )
    return DiagnosticCohort(
        knowledge_cutoff=trusted.knowledge_cutoff,
        knowledge_mode=trusted.knowledge_mode,
        inventory_digest=trusted.digest,
        sources=sources,
        members=members,
        cohort_digest=digest,
        extraction_version=DIAGNOSTIC_COHORT_VERSION,
        derivation="sealed_vote_inventory_full_universe",
        full_cohort=True,
        outcome_fields_excluded=True,
        _seal=_DIAGNOSTIC_COHORT_SEAL,
    )


def _purpose_no_duplicate_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise NcenError("diagnostic_json_duplicate_key")
        output[key] = value
    return output


def _purpose_json_load(raw: bytes, *, code: str) -> Any:
    if raw.startswith(b"\xef\xbb\xbf"):
        raise NcenError(f"{code}_bom_forbidden")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NcenError(f"{code}_utf8_invalid") from exc
    try:
        return json.loads(text, object_pairs_hook=_purpose_no_duplicate_object)
    except (json.JSONDecodeError, ValueError) as exc:
        if isinstance(exc, NcenError):
            raise
        raise NcenError(f"{code}_json_invalid") from exc


def _purpose_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _purpose_parse_timestamp(value: Any, *, code: str) -> dt.datetime:
    parsed = _diagnostic_parse_timestamp(value, code)
    if parsed is None:
        raise NcenError(f"{code}_required")
    return parsed


def _purpose_parse_date(value: Any, *, code: str) -> dt.date:
    parsed = _diagnostic_parse_date(value, code)
    if parsed is None:
        raise NcenError(f"{code}_required")
    return parsed


def _purpose_safe_path(root: Path, relative: str, *, code: str) -> Path:
    if (
        not isinstance(relative, str)
        or not relative
        or "\\" in relative
        or relative.startswith("/")
        or re.match(r"^[A-Za-z]:", relative)
    ):
        raise NcenError(f"{code}_path_unsafe")
    parts = Path(relative).parts
    if any(part in {"", ".", ".."} for part in parts):
        raise NcenError(f"{code}_path_unsafe")
    root_resolved = root.resolve()
    candidate = root.joinpath(*parts)
    current = root
    for part in parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise NcenError(f"{code}_symlink_forbidden")
    try:
        candidate.resolve().relative_to(root_resolved)
    except ValueError as exc:
        raise NcenError(f"{code}_path_escape") from exc
    return candidate


def _purpose_trusted_file(
    roots: Mapping[str, Path], pin: Mapping[str, Any], *, code: str,
) -> Path:
    import stat as stat_module

    root = roots.get(pin["root_id"])
    if root is None or not root.is_dir() or root.is_symlink():
        raise NcenError(code)
    relative = pin["path"]
    if (not isinstance(relative, str) or not relative or "\\" in relative
            or ":" in relative or any(not part or part in {".", ".."} for part in relative.split("/"))):
        raise NcenError(code)
    path = root
    for part in relative.split("/"):
        path = path / part
        try:
            status = path.lstat()
        except OSError as exc:
            raise NcenError(code) from exc
        if stat_module.S_ISLNK(status.st_mode) or getattr(status, "st_file_attributes", 0) & 0x400:
            raise NcenError(code)
    try:
        path.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (ValueError, OSError) as exc:
        raise NcenError(code) from exc
    if not stat_module.S_ISREG(status.st_mode):
        raise NcenError(code)
    return path


def _purpose_trust_bytes(
    roots: Mapping[str, Path], pin: Mapping[str, Any], *, code: str,
    monitor: DiagnosticResourceMonitor | None = None,
) -> bytes:
    path = _purpose_trusted_file(roots, pin, code=code)
    try:
        return _diagnostic_verify_file(
            path, expected_sha256=pin["sha256"], expected_size=pin["bytes"],
            label="diagnostic_trust_file", max_bytes=16 * 1024 * 1024,
            monitor=monitor,
        )
    except NcenError as exc:
        raise NcenError(code) from exc


def _purpose_trust_stream(
    roots: Mapping[str, Path], pin: Mapping[str, Any], *, code: str,
) -> None:
    path = _purpose_trusted_file(roots, pin, code=code)
    try:
        _diagnostic_stream_file(
            path, label="diagnostic_trust_file", expected_sha256=pin["sha256"],
            expected_size=pin["bytes"], monitor=DiagnosticResourceMonitor(), phase="trust_read",
        )
    except NcenError as exc:
        raise NcenError(code) from exc


def _purpose_trust_descriptor(value: Any, *, segment: bool = False) -> dict[str, Any]:
    keys = {"root_id", "path", "sha256", "bytes"}
    if segment:
        keys |= {"offset", "length"}
    if not isinstance(value, dict) or set(value) != keys:
        raise NcenError("diagnostic_pin_digest_invalid")
    pin = dict(value)
    if (not isinstance(pin["root_id"], str) or not pin["root_id"]
            or not isinstance(pin["path"], str) or not pin["path"]
            or "\\" in pin["path"] or ":" in pin["path"]
            or any(not part or part in {".", ".."} for part in pin["path"].split("/"))
            or not isinstance(pin["sha256"], str)
            or _DIAGNOSTIC_HASH.fullmatch(pin["sha256"]) is None
            or pin["sha256"] == "0" * 64
            or type(pin["bytes"]) is not int or pin["bytes"] < 0):
        raise NcenError("diagnostic_pin_digest_invalid")
    if segment and (type(pin["offset"]) is not int or pin["offset"] < 0
                    or type(pin["length"]) is not int or pin["length"] <= 0
                    or pin["offset"] + pin["length"] > pin["bytes"]):
        raise NcenError("diagnostic_pin_digest_invalid")
    return pin


def _purpose_trust_roles(value: Any) -> dict[str, tuple[dict[str, Any], ...]]:
    if not isinstance(value, list):
        raise NcenError("diagnostic_pin_role_missing")
    if any(not isinstance(item, dict) or not isinstance(item.get("role"), str) for item in value):
        raise NcenError("diagnostic_pin_digest_invalid")
    names = [item["role"] for item in value]
    for name in sorted(_PURPOSE_TRUST_ROLES - set(names)):
        raise NcenError(f"diagnostic_pin_role_missing:{name}")
    for name in sorted(set(names), key=str):
        if names.count(name) > 1:
            raise NcenError(f"diagnostic_pin_role_duplicate:{name}")
    for name in sorted(set(names) - _PURPOSE_TRUST_ROLES, key=str):
        raise NcenError(f"diagnostic_pin_role_unknown:{name}")
    if names != sorted(names):
        raise NcenError("diagnostic_pin_digest_invalid")
    roles: dict[str, tuple[dict[str, Any], ...]] = {}
    for item in value:
        if not isinstance(item, dict) or set(item) != {"role", "pins"} or not isinstance(item["pins"], list):
            raise NcenError("diagnostic_pin_digest_invalid")
        name = item["role"]
        pins = tuple(_purpose_trust_descriptor(pin, segment=name.startswith("baseline_"))
                     for pin in item["pins"])
        if name in _PURPOSE_SINGLETON_ROLES and len(pins) != 1:
            raise NcenError(f"diagnostic_pin_role_missing:{name}")
        identities = [(pin["root_id"], pin["path"]) for pin in pins]
        if identities != sorted(set(identities)):
            raise NcenError(f"diagnostic_pin_role_duplicate:{name}")
        roles[name] = pins
    return roles


_PURPOSE_MEMBERSHIP_DERIVATION = "ncen_synthetic_membership_derivation_v1"
_PURPOSE_MEMBERSHIP_BASIS = "externally_declared_finite_membership_fixture"
_PURPOSE_MEMBERSHIP_SEAL = object()


@dataclass(frozen=True, slots=True)
class DiagnosticSyntheticMembershipCheckpoint:
    report_dates: tuple[dt.date, ...]
    members: tuple[DiagnosticCohortMember, ...]
    definition_sha256: str
    inventory_sha256: str
    source_row_count: int
    included_source_row_count: int
    excluded_source_row_count: int
    _seal: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _PURPOSE_MEMBERSHIP_SEAL:
            raise NcenError("diagnostic_cohort_baseline_mismatch")

    def __reduce_ex__(self, protocol: int) -> Any:
        raise TypeError("diagnostic_membership_checkpoint_not_serializable")


def _purpose_membership_json(raw: bytes, *, lines: bool = False) -> Any:
    code = "diagnostic_membership_schema_invalid"
    if not raw.endswith(b"\n") and raw or b"\r" in raw or (not lines and not raw):
        raise NcenError(code)
    if lines:
        records = []
        for line in raw.split(b"\n")[:-1] if raw else ():
            if not line:
                raise NcenError(code)
            try:
                item = _purpose_json_load(line, code=code)
            except NcenError as exc:
                raise NcenError(code) from exc
            if _diagnostic_canonical(item) != line:
                raise NcenError(code)
            records.append(item)
        return records
    try:
        item = _purpose_json_load(raw[:-1], code=code)
    except NcenError as exc:
        raise NcenError(code) from exc
    if _diagnostic_canonical(item) + b"\n" != raw:
        raise NcenError(code)
    return item


def _purpose_membership_object(value: Any, keys: set[str], version: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys or value.get("schema_version") != version:
        raise NcenError("diagnostic_membership_schema_invalid")
    return value


def _purpose_membership_date(raw: Any) -> dt.date:
    try:
        value = dt.date.fromisoformat(raw) if isinstance(raw, str) else None
    except ValueError:
        value = None
    if value is None or value.isoformat() != raw:
        raise NcenError("diagnostic_membership_schema_invalid")
    return value


def _purpose_membership_time(raw: Any) -> dt.datetime | None:
    try:
        return _diagnostic_parse_timestamp(raw, "diagnostic_membership_schema")
    except NcenError as exc:
        raise NcenError("diagnostic_membership_schema_invalid") from exc


def _purpose_membership_descriptors(
    raw: Any, *, membership: bool, roots: Mapping[str, Path],
    seen_ids: set[str], seen_paths: set[tuple[str, str]], monitor: DiagnosticResourceMonitor,
) -> list[tuple[dict[str, Any], bytes]]:
    code = "diagnostic_membership_schema_invalid"
    if not isinstance(raw, list) or not raw:
        raise NcenError(code)
    keys = {"artifact_id", "root_id", "path", "sha256", "bytes", "retrieved_at"}
    if membership:
        keys |= {"rows", "public_at", "data_known_at"}
    result = []
    previous = ""
    for item in raw:
        if not isinstance(item, dict) or set(item) != keys:
            raise NcenError(code)
        artifact_id = item["artifact_id"]
        if not isinstance(artifact_id, str) or not artifact_id or artifact_id <= previous or artifact_id in seen_ids:
            raise NcenError("diagnostic_membership_row_duplicate")
        previous = artifact_id
        seen_ids.add(artifact_id)
        pin = {key: item[key] for key in ("root_id", "path", "sha256", "bytes")}
        try:
            _purpose_trust_descriptor(pin)
        except NcenError as exc:
            raise NcenError(code) from exc
        key = (item["root_id"].casefold(), item["path"].casefold())
        if key in seen_paths:
            raise NcenError("diagnostic_membership_row_duplicate")
        seen_paths.add(key)
        if membership and (type(item["rows"]) is not int or item["rows"] < 0):
            raise NcenError(code)
        for name in (("public_at", "data_known_at", "retrieved_at") if membership else ("retrieved_at",)):
            _purpose_membership_time(item[name])
        result.append((item, _purpose_trust_bytes(
            roots, pin, code="diagnostic_checkpoint_pin_mismatch", monitor=monitor,
        )))
    return result


def _purpose_read_synthetic_membership(
    roots: Mapping[str, Path], roles: Mapping[str, tuple[dict[str, Any], ...]],
    declaration: Mapping[str, Any], baseline: Mapping[str, Any],
) -> DiagnosticSyntheticMembershipCheckpoint:
    """Compare a finite externally pinned membership fixture to derived witnesses and cohort."""
    monitor = DiagnosticResourceMonitor()
    definition_pin = roles["synthetic_membership_definition"][0]
    inventory_pin = roles["synthetic_membership_inventory"][0]
    definition = _purpose_membership_object(
        _purpose_membership_json(_purpose_trust_bytes(
            roots, definition_pin, code="diagnostic_checkpoint_pin_mismatch", monitor=monitor,
        )),
        {"schema_version", "lane", "completeness_basis", "fixture_id", "K", "mode", "report_dates",
         "membership_artifacts", "header_artifacts", "derivation_version"},
        "ncen_synthetic_membership_definition_v1",
    )
    if (definition["lane"] != "synthetic_fixture"
            or definition["completeness_basis"] != _PURPOSE_MEMBERSHIP_BASIS
            or definition["derivation_version"] != _PURPOSE_MEMBERSHIP_DERIVATION
            or not isinstance(definition["fixture_id"], str) or not definition["fixture_id"]):
        raise NcenError("diagnostic_membership_schema_invalid")
    cutoff = _purpose_membership_time(definition["K"])
    if cutoff is None or not isinstance(definition["mode"], str) or definition["mode"] not in KNOWLEDGE_MODES:
        raise NcenError("diagnostic_membership_schema_invalid")
    dates_raw = definition["report_dates"]
    if (not isinstance(dates_raw, list) or not dates_raw
            or any(not isinstance(value, str) for value in dates_raw)
            or dates_raw != sorted(set(dates_raw))):
        raise NcenError("diagnostic_membership_schema_invalid")
    dates = tuple(_purpose_membership_date(value) for value in dates_raw)
    seen_ids: set[str] = set()
    seen_paths: set[tuple[str, str]] = set()
    membership_files = _purpose_membership_descriptors(
        definition["membership_artifacts"], membership=True, roots=roots,
        seen_ids=seen_ids, seen_paths=seen_paths, monitor=monitor,
    )
    header_files = _purpose_membership_descriptors(
        definition["header_artifacts"], membership=False, roots=roots,
        seen_ids=seen_ids, seen_paths=seen_paths, monitor=monitor,
    )
    header_by_id: dict[str, dict[str, Any]] = {}
    for descriptor, raw in header_files:
        header = _purpose_membership_object(
            _purpose_membership_json(raw),
            {"schema_version", "accession", "cik", "R", "form_type", "acceptance_at"},
            "ncen_synthetic_membership_header_v1",
        )
        if not isinstance(header["accession"], str) or _ACCESSION.fullmatch(header["accession"]) is None:
            raise NcenError("diagnostic_membership_schema_invalid")
        _purpose_membership_date(header["R"])
        _purpose_membership_time(header["acceptance_at"])
        header_by_id[descriptor["artifact_id"]] = header
    rows: list[tuple[dict[str, Any], dict[str, Any], int]] = []
    row_ids: set[str] = set()
    for descriptor, raw in membership_files:
        previous = ""
        parsed = _purpose_membership_json(raw, lines=True)
        if len(parsed) != descriptor["rows"]:
            raise NcenError("diagnostic_membership_schema_invalid")
        for line, value in enumerate(parsed, start=1):
            row = _purpose_membership_object(
                value,
                {"schema_version", "source_row_id", "R", "cik", "series_id", "accession", "header_artifact_id"},
                "ncen_synthetic_membership_source_row_v1",
            )
            row_id = row["source_row_id"]
            if not isinstance(row_id, str) or not row_id:
                raise NcenError("diagnostic_membership_schema_invalid")
            if row_id <= previous or row_id in row_ids:
                raise NcenError("diagnostic_membership_row_duplicate")
            previous = row_id
            row_ids.add(row_id)
            _purpose_membership_date(row["R"])
            if not isinstance(row["accession"], str) or _ACCESSION.fullmatch(row["accession"]) is None:
                raise NcenError("diagnostic_membership_schema_invalid")
            if not isinstance(row["header_artifact_id"], str):
                raise NcenError("diagnostic_membership_schema_invalid")
            header = header_by_id.get(row["header_artifact_id"])
            if header is None or any(row[key] != header[key] for key in ("accession", "cik", "R")):
                raise NcenError("diagnostic_membership_header_binding_mismatch")
            rows.append((row, descriptor, line))
    if not rows or {row["header_artifact_id"] for row, _, _ in rows} != set(header_by_id):
        raise NcenError("diagnostic_membership_header_binding_mismatch")

    # A redundant copy stays a distinct witness; only divergent headers or included revisions refuse.
    identities: dict[str, tuple[str, str, str, Any]] = {}
    for row, _descriptor, _line in sorted(rows, key=lambda item: item[0]["source_row_id"]):
        cik = row["cik"]
        if cik is None:
            raise NcenError("diagnostic_membership_identity_unknown")
        if (not isinstance(cik, str) or normalize_cik(cik) != cik or len(cik) != 10
                or (row["series_id"] is not None
                    and (not isinstance(row["series_id"], str)
                         or re.fullmatch(r"S[0-9]{9}", row["series_id"]) is None))):
            raise NcenError("diagnostic_membership_identity_invalid")
        header = header_by_id[row["header_artifact_id"]]
        form = header["form_type"]
        if form == "NPORT-P/A":
            raise NcenError("diagnostic_membership_revision_unsupported")
        if form != "NPORT-P":
            raise NcenError("diagnostic_membership_form_invalid")
        identity = row["accession"]
        signature = (header["cik"], header["R"], form, header["acceptance_at"])
        if identity in identities and identities[identity] != signature:
            raise NcenError("diagnostic_membership_copy_conflict")
        identities[identity] = signature

    inventory: list[dict[str, Any]] = []
    members_by_date: dict[dt.date, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    latest: dict[dt.date, dt.datetime] = {}
    accessions: dict[tuple[dt.date, str, str], str] = {}
    for row, descriptor, line in sorted(rows, key=lambda item: item[0]["source_row_id"]):
        header_id = row["header_artifact_id"]
        header = header_by_id[header_id]
        header_desc = next(item for item, _ in header_files if item["artifact_id"] == header_id)
        times = tuple(_purpose_membership_time(value) for value in (
            header["acceptance_at"], descriptor["public_at"], descriptor["data_known_at"],
            descriptor["retrieved_at"], header_desc["retrieved_at"],
        ))
        if any(value is None for value in times):
            raise NcenError("diagnostic_membership_time_unknown")
        acceptance, public, known, retrieved, header_retrieved = times
        assert acceptance is not None and public is not None and known is not None
        assert retrieved is not None and header_retrieved is not None
        p = max(acceptance, public, known)
        h = max(retrieved, header_retrieved)
        reasons = sorted([
            *(["public_after_cutoff"] if p > cutoff else []),
            *(["possession_after_cutoff"] if definition["mode"] == KNOWLEDGE_CURRENT_RUN and h > cutoff else []),
        ])
        fund_key = row["series_id"] if row["series_id"] is not None else f"cik:{row['cik']}"
        date = _purpose_membership_date(row["R"])
        if not reasons:
            key = (date, row["cik"], fund_key)
            if key in accessions and accessions[key] != row["accession"]:
                raise NcenError("diagnostic_membership_revision_unsupported")
            accessions[key] = row["accession"]
            members_by_date[date][row["cik"]].add(fund_key)
            latest[date] = max(latest.get(date, p), p)
        inventory.append({
            "schema_version": "ncen_synthetic_membership_inventory_row_v1",
            "source_row_id": row["source_row_id"], "artifact_id": descriptor["artifact_id"],
            "artifact_sha256": descriptor["sha256"], "source_line": line,
            "source_row_sha256": _diagnostic_hash(row), "header_artifact_id": header_id,
            "header_sha256": header_desc["sha256"], "R": row["R"], "cik": row["cik"],
            "series_id": row["series_id"], "fund_key": fund_key, "accession": row["accession"],
            "acceptance_at": header["acceptance_at"], "source_public_at": descriptor["public_at"],
            "data_known_at": descriptor["data_known_at"], "source_retrieved_at": descriptor["retrieved_at"],
            "header_retrieved_at": header_desc["retrieved_at"],
            "public_available_at": _diagnostic_timestamp(p), "possession_at": _diagnostic_timestamp(h),
            "disposition": "excluded" if reasons else "included", "reasons": reasons,
        })
    if {_purpose_membership_date(row["R"]) for row, _, _ in rows} - set(dates):
        raise NcenError("diagnostic_membership_date_universe_mismatch")
    if set(dates) - set(members_by_date):
        raise NcenError("diagnostic_membership_date_empty")
    pinned_inventory = _purpose_trust_bytes(
        roots, inventory_pin, code="diagnostic_checkpoint_pin_mismatch", monitor=monitor,
    )
    _purpose_membership_json(pinned_inventory, lines=True)
    if pinned_inventory != _purpose_jsonl_bytes(inventory):
        raise NcenError("diagnostic_membership_inventory_mismatch")
    members = tuple(
        DiagnosticCohortMember(date, cik, tuple(sorted(keys)), latest[date])
        for date, registrants in sorted(members_by_date.items())
        for cik, keys in sorted(registrants.items())
    )
    inventory_digest = declaration.get("inventory_digest")
    sources_raw = declaration.get("cohort_provenance")
    sources_raw = sources_raw.get("inventory_sources") if isinstance(sources_raw, dict) else None
    if not isinstance(inventory_digest, str) or not inventory_digest or not isinstance(sources_raw, list):
        raise NcenError("diagnostic_cohort_baseline_mismatch")
    sources: list[InventorySource] = []
    for source in sources_raw:
        if not isinstance(source, dict) or set(source) != {
                "package_label", "zip_sha256", "package_id", "retrieved_at", "first_verified_public_at"}:
            raise NcenError("diagnostic_cohort_baseline_mismatch")
        if (not isinstance(source["package_label"], str) or not source["package_label"]
                or not isinstance(source["package_id"], str) or not source["package_id"]
                or not isinstance(source["zip_sha256"], str)
                or _DIAGNOSTIC_HASH.fullmatch(source["zip_sha256"]) is None):
            raise NcenError("diagnostic_cohort_baseline_mismatch")
        retrieved = _purpose_membership_time(source["retrieved_at"])
        public = _purpose_membership_time(source["first_verified_public_at"])
        if retrieved is None or public is None:
            raise NcenError("diagnostic_cohort_baseline_mismatch")
        sources.append(InventorySource(
            source["package_label"], source["zip_sha256"], source["package_id"], retrieved, public,
        ))
    if (not sources or tuple(source.package_label for source in sources)
            != tuple(sorted({source.package_label for source in sources}))):
        raise NcenError("diagnostic_cohort_baseline_mismatch")
    try:
        cohort_digest = _diagnostic_cohort_digest(
            knowledge_cutoff=cutoff, knowledge_mode=definition["mode"],
            inventory_digest=inventory_digest, sources=tuple(sources), members=members,
        )
    except NcenError as exc:
        raise NcenError("diagnostic_cohort_baseline_mismatch") from exc
    cohort_pin = roles["cohort_checkpoint"][0]
    cohort_raw = _purpose_trust_bytes(roots, cohort_pin, code="diagnostic_checkpoint_pin_mismatch", monitor=monitor)
    cohort_rows = _purpose_membership_json(cohort_raw, lines=True)
    expected_rows = sorted(
        (_purpose_cohort_record(member, declaration["inventory_digest"]) for member in members),
        key=lambda item: item["record_id"],
    )
    receipt = _purpose_membership_json(_purpose_trust_bytes(
        roots, roles["cohort_derivation_receipt"][0],
        code="diagnostic_checkpoint_pin_mismatch", monitor=monitor,
    ))
    receipt_keys = {"schema_version", "lane", "cohort_derivation_version", "K", "mode",
                    "inventory_digest", "cohort_digest", "inventory_sources", "cohort_sha256",
                    "member_count", "fund_key_count", "derivation_code_manifest_sha256",
                    "completeness_basis", "membership_definition_sha256", "membership_inventory_sha256",
                    "membership_derivation_version", "report_dates", "source_row_count",
                    "included_source_row_count", "excluded_source_row_count"}
    if (not isinstance(receipt, dict) or set(receipt) != receipt_keys
            or _diagnostic_canonical(receipt) != _diagnostic_canonical({
                "schema_version": "ncen_diagnostic_cohort_derivation_v1", "lane": "synthetic_fixture",
                "cohort_derivation_version": DIAGNOSTIC_COHORT_VERSION,
                "K": definition["K"], "mode": definition["mode"],
                "inventory_digest": inventory_digest,
                "cohort_digest": cohort_digest,
                "inventory_sources": sources_raw,
                "cohort_sha256": cohort_pin["sha256"], "member_count": len(members),
                "fund_key_count": sum(len(member.fund_keys) for member in members),
                "derivation_code_manifest_sha256": roles["runtime_code_manifest"][0]["sha256"],
                "completeness_basis": _PURPOSE_MEMBERSHIP_BASIS,
                "membership_definition_sha256": definition_pin["sha256"],
                "membership_inventory_sha256": inventory_pin["sha256"],
                "membership_derivation_version": _PURPOSE_MEMBERSHIP_DERIVATION,
                "report_dates": dates_raw, "source_row_count": len(rows),
                "included_source_row_count": sum(row["disposition"] == "included" for row in inventory),
                "excluded_source_row_count": sum(row["disposition"] == "excluded" for row in inventory),
            })
            or declaration.get("cohort_digest") != cohort_digest
            or cohort_rows != expected_rows or cohort_raw != _purpose_jsonl_bytes(expected_rows)):
        raise NcenError("diagnostic_cohort_baseline_mismatch")
    contexts = declaration.get("contexts")
    if (not isinstance(contexts, list) or contexts != [
            {"R": date.isoformat(), "K": definition["K"], "mode": definition["mode"]} for date in dates]
            or baseline.get("contexts") != contexts):
        raise NcenError("diagnostic_context_coverage_mismatch")
    return DiagnosticSyntheticMembershipCheckpoint(
        dates, members, definition_pin["sha256"], inventory_pin["sha256"], len(rows),
        sum(row["disposition"] == "included" for row in inventory),
        sum(row["disposition"] == "excluded" for row in inventory), _PURPOSE_MEMBERSHIP_SEAL,
    )


def _purpose_segment_bytes(
    roots: Mapping[str, Path], pin: Mapping[str, Any], *, code: str,
) -> bytes:
    path = _purpose_trusted_file(roots, pin, code=code)
    monitor = DiagnosticResourceMonitor()
    try:
        handle, _identity, size = _diagnostic_open_held(path, "diagnostic_baseline", monitor)
        try:
            if size != pin["bytes"]:
                raise NcenError(code)
            handle.seek(pin["offset"])
            chunks = []
            remaining = pin["length"]
            digest = hashlib.sha256()
            while remaining:
                block = handle.read(min(remaining, monitor.limits.hash_block_bytes))
                if not block:
                    raise NcenError(code)
                remaining -= len(block)
                digest.update(block)
                chunks.append(block)
                monitor.bytes_read(len(block), "baseline_read")
            if digest.hexdigest() != pin["sha256"]:
                raise NcenError(code)
            return b"".join(chunks)
        finally:
            handle.close()
            monitor.handle_closed()
    except NcenError as exc:
        raise NcenError(code) from exc


from collections.abc import Callable


def _purpose_control_json(raw: bytes, validator: Callable[[Any], dict[str, Any]]) -> dict[str, Any]:
    try:
        value = _purpose_json_load(raw, code="diagnostic_pinned_input_invalid")
    except NcenError as exc:
        raise NcenError("diagnostic_pinned_input_invalid") from exc
    control = validator(value)
    if raw != _purpose_json_bytes(control):
        raise NcenError("diagnostic_pinned_input_invalid")
    return control


def _purpose_control_object(value: Any, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise NcenError("diagnostic_pinned_input_invalid")
    return value


def _purpose_control_digest(value: Any) -> None:
    if not isinstance(value, str) or _DIAGNOSTIC_HASH.fullmatch(value) is None:
        raise NcenError("diagnostic_pinned_input_invalid")


def _purpose_control_path(value: Any) -> None:
    if (not isinstance(value, str) or not value or "\\" in value or ":" in value
            or any(not part or part in {".", ".."} for part in value.split("/"))):
        raise NcenError("diagnostic_pinned_input_invalid")


def _purpose_control_pin(value: Any) -> None:
    try:
        _purpose_trust_descriptor(value)
    except NcenError as exc:
        raise NcenError("diagnostic_pinned_input_invalid") from exc


def _purpose_control_date(value: Any) -> None:
    try:
        parsed = dt.date.fromisoformat(value) if isinstance(value, str) else None
    except ValueError as exc:
        raise NcenError("diagnostic_pinned_input_invalid") from exc
    if parsed is None or parsed.isoformat() != value:
        raise NcenError("diagnostic_pinned_input_invalid")


def _purpose_control_time(value: Any) -> None:
    try:
        _purpose_parse_timestamp(value, code="diagnostic_pinned_input_invalid")
    except NcenError as exc:
        raise NcenError("diagnostic_pinned_input_invalid") from exc


def _validate_baseline_control(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or not isinstance(value.get("schema_version"), str):
        raise NcenError("diagnostic_pinned_input_invalid")
    if value["schema_version"] != "ncen_diagnostic_baseline_checkpoint_v2":
        raise NcenError("diagnostic_checkpoint_schema_unsupported")
    baseline = _purpose_control_object(value, {
        "schema_version", "lane", "purpose", "diagnostic_only", "contexts", "versions",
        "inventory_digest", "cohort_digest", "ncen_evidence_digest", "exclusion_ledger_digest",
        "files", "source_manifest_pin", "acquisition_pin",
    })
    if (baseline["lane"] != "synthetic_fixture"
            or baseline["purpose"] != "ncen_purpose_diagnostic_selection_replay"
            or baseline["diagnostic_only"] is not True):
        raise NcenError("diagnostic_pinned_input_invalid")
    contexts = baseline["contexts"]
    if not isinstance(contexts, list) or not contexts:
        raise NcenError("diagnostic_pinned_input_invalid")
    for item in contexts:
        context = _purpose_control_object(item, {"R", "K", "mode"})
        _purpose_control_date(context["R"])
        _purpose_control_time(context["K"])
        if not isinstance(context["mode"], str) or context["mode"] not in KNOWLEDGE_MODES:
            raise NcenError("diagnostic_pinned_input_invalid")
    if (contexts != sorted(contexts, key=lambda item: (item["R"], item["K"], item["mode"]))
            or len({(item["R"], item["K"], item["mode"]) for item in contexts}) != len(contexts)):
        raise NcenError("diagnostic_pinned_input_invalid")
    versions = _purpose_control_object(baseline["versions"], {
        "selector", "merge", "amendment", "cohort", "source", "admission", "normalizer", "edge", "fold",
    })
    if any(not isinstance(version, str) or not version for version in versions.values()):
        raise NcenError("diagnostic_pinned_input_invalid")
    if not isinstance(baseline["inventory_digest"], str) or not baseline["inventory_digest"]:
        raise NcenError("diagnostic_pinned_input_invalid")
    for name in ("cohort_digest", "ncen_evidence_digest", "exclusion_ledger_digest"):
        _purpose_control_digest(baseline[name])
    files = baseline["files"]
    if not isinstance(files, list) or len(files) != len(_PURPOSE_BASELINE_FILES):
        raise NcenError("diagnostic_pinned_input_invalid")
    for item in files:
        descriptor = _purpose_control_object(item, {"path", "sha256", "bytes", "rows"})
        _purpose_control_path(descriptor["path"])
        _purpose_control_digest(descriptor["sha256"])
        if (type(descriptor["bytes"]) is not int or descriptor["bytes"] < 0
                or type(descriptor["rows"]) is not int or descriptor["rows"] < 0):
            raise NcenError("diagnostic_pinned_input_invalid")
    _purpose_control_pin(baseline["source_manifest_pin"])
    acquisition = _purpose_control_object(baseline["acquisition_pin"], {"lane", "scope", "sha256sums"})
    if acquisition["lane"] != "synthetic_fixture":
        raise NcenError("diagnostic_pinned_input_invalid")
    _purpose_control_pin(acquisition["scope"])
    _purpose_control_pin(acquisition["sha256sums"])
    return baseline


def _validate_runtime_control(value: Any) -> dict[str, Any]:
    runtime = _purpose_control_object(value, {"schema_version", "files", "launcher", "dependencies"})
    if runtime["schema_version"] != "ncen_diagnostic_runtime_code_manifest_v1":
        raise NcenError("diagnostic_pinned_input_invalid")
    if not isinstance(runtime["files"], list) or not runtime["files"]:
        raise NcenError("diagnostic_pinned_input_invalid")
    for item in runtime["files"]:
        descriptor = _purpose_control_object(item, {"path", "sha256", "bytes"})
        _purpose_control_path(descriptor["path"])
        _purpose_control_digest(descriptor["sha256"])
        if type(descriptor["bytes"]) is not int or descriptor["bytes"] < 0:
            raise NcenError("diagnostic_pinned_input_invalid")
    _purpose_control_path(runtime["launcher"])
    if not isinstance(runtime["dependencies"], list):
        raise NcenError("diagnostic_pinned_input_invalid")
    for path in runtime["dependencies"]:
        _purpose_control_path(path)
    return runtime


def _validate_cohort_receipt_control(value: Any) -> dict[str, Any]:
    receipt = _purpose_control_object(value, {
        "schema_version", "lane", "cohort_derivation_version", "K", "mode", "inventory_digest",
        "cohort_digest", "inventory_sources", "cohort_sha256", "member_count", "fund_key_count",
        "derivation_code_manifest_sha256", "completeness_basis", "membership_definition_sha256",
        "membership_inventory_sha256", "membership_derivation_version", "report_dates", "source_row_count",
        "included_source_row_count", "excluded_source_row_count",
    })
    if (receipt["schema_version"] != "ncen_diagnostic_cohort_derivation_v1"
            or receipt["lane"] != "synthetic_fixture"
            or receipt["cohort_derivation_version"] != DIAGNOSTIC_COHORT_VERSION
            or receipt["completeness_basis"] != _PURPOSE_MEMBERSHIP_BASIS
            or receipt["membership_derivation_version"] != _PURPOSE_MEMBERSHIP_DERIVATION
            or not isinstance(receipt["inventory_digest"], str) or not receipt["inventory_digest"]
            or not isinstance(receipt["mode"], str) or receipt["mode"] not in KNOWLEDGE_MODES):
        raise NcenError("diagnostic_pinned_input_invalid")
    _purpose_control_time(receipt["K"])
    for name in ("cohort_digest", "cohort_sha256", "derivation_code_manifest_sha256",
                 "membership_definition_sha256", "membership_inventory_sha256"):
        _purpose_control_digest(receipt[name])
    for name in ("member_count", "fund_key_count", "source_row_count", "included_source_row_count",
                 "excluded_source_row_count"):
        if type(receipt[name]) is not int or receipt[name] < 0:
            raise NcenError("diagnostic_pinned_input_invalid")
    dates = receipt["report_dates"]
    if not isinstance(dates, list) or not dates:
        raise NcenError("diagnostic_pinned_input_invalid")
    for date in dates:
        _purpose_control_date(date)
    if dates != sorted(set(dates)):
        raise NcenError("diagnostic_pinned_input_invalid")
    sources = receipt["inventory_sources"]
    if not isinstance(sources, list) or not sources:
        raise NcenError("diagnostic_pinned_input_invalid")
    for item in sources:
        source = _purpose_control_object(item, {
            "package_label", "zip_sha256", "package_id", "retrieved_at", "first_verified_public_at",
        })
        if any(not isinstance(source[key], str) or not source[key] for key in ("package_label", "package_id")):
            raise NcenError("diagnostic_pinned_input_invalid")
        _purpose_control_digest(source["zip_sha256"])
        _purpose_control_time(source["retrieved_at"])
        _purpose_control_time(source["first_verified_public_at"])
    if [source["package_label"] for source in sources] != sorted({source["package_label"] for source in sources}):
        raise NcenError("diagnostic_pinned_input_invalid")
    return receipt


def _purpose_admit_trust(
    trusted_run: DiagnosticTrustPin | None, *, root: Path, code_root: Path,
    input_roots: Mapping[str, Path], declaration: Mapping[str, Any] | None,
    monitor: DiagnosticResourceMonitor | None = None,
) -> dict[str, Any]:
    """C1a input checks only; this never issues a complete export or C1 checkpoint."""
    if type(trusted_run) is not DiagnosticTrustPin:
        raise NcenError("diagnostic_trust_anchor_missing")
    trust_path = Path(trusted_run.manifest_path)
    if (not trust_path.is_absolute() or not isinstance(trusted_run.manifest_sha256, str)
            or _DIAGNOSTIC_HASH.fullmatch(trusted_run.manifest_sha256) is None
            or trusted_run.manifest_sha256 == "0" * 64
            or type(trusted_run.manifest_size) is not int or trusted_run.manifest_size <= 0):
        raise NcenError("diagnostic_trust_anchor_mismatch")
    try:
        trust_path.resolve(strict=True).relative_to(root.resolve())
    except ValueError:
        pass
    else:
        raise NcenError("diagnostic_trust_anchor_mismatch")
    trust_roots = {**input_roots, "code": code_root, "trust": trust_path.parent}
    if "code" in input_roots or "trust" in input_roots:
        raise NcenError("diagnostic_trust_anchor_mismatch")
    for admitted_root in trust_roots.values():
        try:
            resolved = admitted_root.resolve(strict=True)
            resolved.relative_to(root.resolve())
        except ValueError:
            pass
        except OSError as exc:
            raise NcenError("diagnostic_trust_anchor_mismatch") from exc
        else:
            raise NcenError("diagnostic_trust_anchor_mismatch")
        for component in (admitted_root, *admitted_root.parents):
            status = component.lstat()
            if component.is_symlink() or getattr(status, "st_file_attributes", 0) & 0x400:
                raise NcenError("diagnostic_trust_anchor_mismatch")
    try:
        data = _purpose_trust_bytes(trust_roots, {
            "root_id": "trust", "path": trust_path.name,
            "sha256": trusted_run.manifest_sha256, "bytes": trusted_run.manifest_size,
        }, code="diagnostic_trust_anchor_mismatch")
        trust = _purpose_json_load(data, code="diagnostic_trust_anchor_mismatch")
    except (OSError, NcenError) as exc:
        raise NcenError("diagnostic_trust_anchor_mismatch") from exc
    if (not isinstance(trust, dict) or set(trust) != {"schema_version", "lane", "roles", "logical_ledger"}
            or trust["schema_version"] != "ncen_diagnostic_trust_manifest_v3"
            or trust["lane"] not in _DIAGNOSTIC_MANIFEST_KINDS):
        raise NcenError("diagnostic_pin_digest_invalid")
    roles = _purpose_trust_roles(trust["roles"])
    logical = trust["logical_ledger"]
    if (not isinstance(logical, dict) or logical.get("version") != DIAGNOSTIC_EXCLUSION_LEDGER_DIGEST_VERSION
            or set(logical) != {"version", "digest"} or not isinstance(logical["digest"], str)
            or _DIAGNOSTIC_HASH.fullmatch(logical["digest"]) is None
            or logical["digest"] == "0" * 64):
        raise NcenError("diagnostic_pin_digest_invalid")
    if trust["lane"] != "synthetic_fixture":
        raise NcenError("diagnostic_required_pins_unverified")
    checkpoint_raw = _purpose_trust_bytes(
        trust_roots, roles["diagnostic_baseline_checkpoint"][0], code="diagnostic_checkpoint_pin_mismatch")
    runtime_raw = _purpose_trust_bytes(
        trust_roots, roles["runtime_code_manifest"][0], code="diagnostic_runtime_code_mismatch")
    receipt_raw = _purpose_trust_bytes(
        trust_roots, roles["cohort_derivation_receipt"][0], code="diagnostic_checkpoint_pin_mismatch")
    checkpoint = _purpose_control_json(checkpoint_raw, _validate_baseline_control)
    runtime = _purpose_control_json(runtime_raw, _validate_runtime_control)
    _purpose_control_json(receipt_raw, _validate_cohort_receipt_control)
    if checkpoint["lane"] != trust["lane"]:
        raise NcenError("diagnostic_lane_mismatch")
    if declaration is None or declaration.get("pin_roles") != trust["roles"]:
        raise NcenError("diagnostic_declared_pin_mismatch")
    if declaration.get("lane") != trust["lane"]:
        raise NcenError("diagnostic_lane_mismatch")
    code_files = runtime["files"]
    expected = {f"src/{path.relative_to(code_root / 'src').as_posix()}"
                for path in (code_root / "src").rglob("*.py")}
    expected.add(runtime["launcher"])
    expected.update(runtime["dependencies"])
    if (not runtime["launcher"] or runtime["launcher"].startswith("src/")
            or runtime["dependencies"] != sorted(set(runtime["dependencies"]))
            or "requirements.txt" not in runtime["dependencies"]
            or not {path for path in ("uv.lock", "poetry.lock") if (code_root / path).is_file()}.issubset(runtime["dependencies"])
            or {item["path"] for item in code_files} != expected
            or len(code_files) != len(expected)
            or code_files != sorted(code_files, key=lambda item: item["path"])):
        raise NcenError("diagnostic_runtime_code_mismatch")
    authorized_code = [{"path": item["path"], "sha256": item["sha256"]} for item in code_files]
    if declaration.get("source_code") != authorized_code:
        raise NcenError("diagnostic_declared_pin_mismatch")
    seals = [{"name": name, "sha256": roles[name][0]["sha256"]}
             for name in sorted(("baseline_v3", "baseline_stage1", "baseline_stage2a", "baseline_stage2b"))]
    if declaration.get("baseline_seals") != seals:
        raise NcenError("diagnostic_declared_pin_mismatch")
    excluded = {"runtime_code_manifest", "baseline_v3", "baseline_stage1", "baseline_stage2a", "baseline_stage2b"}
    artifacts = sorted(({key: pin[key] for key in ("root_id", "path", "sha256", "bytes")}
                        for name, pins in roles.items() if name not in excluded for pin in pins),
                       key=lambda pin: (pin["root_id"], pin["path"]))
    if declaration.get("input_artifacts") != artifacts:
        raise NcenError("diagnostic_declared_pin_mismatch")
    source_pin = roles["ncen_source_manifest"][0]
    if declaration.get("ncen_source_manifest") != {
        "sha256": source_pin["sha256"], "bytes": source_pin["bytes"],
        "kind": trust["lane"], "evidence_digest": declaration.get("ncen_evidence_digest"),
    }:
        raise NcenError("diagnostic_declared_pin_mismatch")
    scope, sums = (roles[name][0] for name in ("acquisition_scope", "acquisition_sha256sums"))
    if declaration.get("quarantine_seal") != {
        "scope_sha256": scope["sha256"], "sha256sums_sha256": sums["sha256"]}:
        raise NcenError("diagnostic_declared_pin_mismatch")
    loaded_code_bytes = None
    for item in code_files:
        verified = _purpose_trust_bytes(trust_roots, {"root_id": "code", **item},
                                        code="diagnostic_runtime_code_mismatch")
        if item["path"] == "src/bonds/default_events/ncen.py":
            loaded_code_bytes = verified
    loaded = Path(__file__).resolve(strict=True)
    if loaded != (code_root / "src/bonds/default_events/ncen.py").resolve(strict=True):
        raise NcenError("diagnostic_runtime_code_mismatch")
    import inspect

    caller = next((Path(frame.filename).resolve() for frame in inspect.stack()[1:]
                   if Path(frame.filename).resolve() != loaded), None)
    if caller != (code_root / runtime["launcher"]).resolve(strict=True):
        raise NcenError("diagnostic_runtime_code_mismatch")
    if loaded_code_bytes is None:
        raise NcenError("diagnostic_runtime_code_mismatch")
    source_bytes = loaded_code_bytes
    if hashlib.sha256(source_bytes).hexdigest() != _PURPOSE_LOADED_CODE_SHA256:
        raise NcenError("diagnostic_runtime_code_mismatch")
    markers = (
        b"\n\n# === FE-1 purpose-labelled diagnostic core",
        b"\n\n# === FE-1 diagnostic source sidecars and selection attestation",
        b"\n\n# === FE-1 diagnostic synthetic export and sealed envelope",
    )
    try:
        stage1, stage2a, stage2b = (source_bytes.index(marker) for marker in markers)
    except ValueError as exc:
        raise NcenError("diagnostic_baseline_seal_mismatch") from exc
    bounds = {"baseline_v3": (0, stage1), "baseline_stage1": (stage1, stage2a),
              "baseline_stage2a": (stage2a, stage2b)}
    for name, (start, end) in bounds.items():
        pin = roles[name][0]
        if (pin["root_id"] != "code" or pin["path"] != "src/bonds/default_events/ncen.py"
                or pin["offset"] != start or pin["length"] != end - start
                or _purpose_segment_bytes(trust_roots, pin, code="diagnostic_baseline_seal_mismatch")
                != source_bytes[start:end]):
            raise NcenError("diagnostic_baseline_seal_mismatch")
    if (stage1 != 95_275 or roles["baseline_v3"][0]["sha256"] != _PURPOSE_V3_SHA256):
        raise NcenError("diagnostic_baseline_seal_mismatch")
    previous = roles["baseline_stage2b"][0]
    _purpose_segment_bytes(trust_roots, previous, code="diagnostic_baseline_seal_mismatch")
    loader_roles = {"ncen_source_manifest", "ncen_raw_audit_manifest", "ncen_dera_artifacts", "ncen_xml_artifacts",
                    "ncen_header_artifacts", "acquisition_scope", "acquisition_sha256sums",
                    "acquisition_ledger"}
    for name in sorted(_PURPOSE_TRUST_ROLES - excluded - loader_roles):
        for pin in roles[name]:
            _purpose_trust_stream(trust_roots, pin, code="diagnostic_checkpoint_pin_mismatch")
    if (scope["root_id"] != sums["root_id"] or scope["path"] != "scope.json"
            or sums["path"] != "SHA256SUMS"):
        raise NcenError("diagnostic_pin_digest_invalid")
    acq_pin = DiagnosticAcquisitionPin(trust_roots[scope["root_id"]], scope["sha256"],
                                       sums["sha256"], trust["lane"])
    source_path = _purpose_trusted_file(trust_roots, source_pin, code="diagnostic_source_checkpoint_mismatch")
    source = read_diagnostic_source_rows(DiagnosticSourceManifestPin(
        source_path, source_pin["sha256"], source_pin["bytes"]), trusted_acquisition=acq_pin)
    custody = _diagnostic_bound_source_custody(source)
    manifest = _purpose_json_load(_purpose_trust_bytes(trust_roots, source_pin,
                                  code="diagnostic_source_checkpoint_mismatch"),
                                  code="diagnostic_source_checkpoint_mismatch")
    if (not isinstance(manifest, dict) or manifest.get("manifest_kind") != trust["lane"]
            or not isinstance(manifest.get("artifacts"), list)):
        raise NcenError("diagnostic_source_checkpoint_mismatch")
    for name, kind in (("ncen_dera_artifacts", "dera_zip"), ("ncen_xml_artifacts", "edgar_xml"),
                       ("ncen_header_artifacts", "header")):
        actual = {(pin["root_id"], pin["path"], pin["sha256"], pin["bytes"])
                  for pin in roles[name]}
        expected_members = {(source_pin["root_id"], item["path"], item["sha256"], item["bytes"])
                            for item in manifest["artifacts"] if item.get("kind") == kind}
        if actual != expected_members or len(actual) != len(roles[name]):
            raise NcenError("diagnostic_source_checkpoint_mismatch")
    index_pin = roles["ncen_index_manifest"][0]
    index = _purpose_json_load(_purpose_trust_bytes(trust_roots, index_pin,
                               code="diagnostic_checkpoint_pin_mismatch"), code="diagnostic_checkpoint_pin_mismatch")
    if (not isinstance(index, dict) or set(index) != {"schema_version", "artifacts"}
            or index["schema_version"] != "ncen_diagnostic_index_manifest_v1"
            or not isinstance(index["artifacts"], list)
            or any(not isinstance(item, dict) or set(item) != {"pin", "retrieved_at"}
                   or not isinstance(item["retrieved_at"], str)
                   or item["retrieved_at"] != _diagnostic_timestamp(
                       _purpose_parse_timestamp(item["retrieved_at"], code="diagnostic_index_time"))
                   for item in index["artifacts"])
            or [item["pin"] for item in index["artifacts"]] != list(roles["ncen_index_artifacts"])):
        raise NcenError("diagnostic_candidate_universe_mismatch")
    trust["membership_checkpoint"] = _purpose_read_synthetic_membership(
        trust_roots, roles, declaration, checkpoint,
    )
    ledger = custody.acquisition_ledger
    if (declaration.get("ncen_evidence_digest") != source.evidence_digest
            or declaration.get("ncen_source_manifest", {}).get("evidence_digest") != source.evidence_digest):
        raise NcenError("diagnostic_declared_pin_mismatch")
    if (roles["acquisition_ledger"][0]["sha256"] != hashlib.sha256(
            _diagnostic_canonical(ledger.digest_object())).hexdigest()
            or _purpose_trust_bytes(trust_roots, roles["acquisition_ledger"][0],
                                    code="diagnostic_declared_ledger_mismatch")
            != _diagnostic_canonical(ledger.digest_object())
            or any(value != ledger.exclusion_ledger_digest for value in (
                logical["digest"], checkpoint.get("exclusion_ledger_digest"),
                declaration.get("exclusion_ledger_digest")))
            or declaration.get("quarantine_seal") != {
                "scope_sha256": scope["sha256"], "sha256sums_sha256": sums["sha256"]}):
        raise NcenError("diagnostic_declared_ledger_mismatch")
    trust["raw_input_inventory"] = _purpose_enumerate_raw_inputs(
        trust_roots, roles, manifest, monitor=monitor,
    )
    raw_manifest = _purpose_raw_control(_purpose_trust_bytes(
        trust_roots, roles["ncen_raw_audit_manifest"][0], code="diagnostic_pinned_input_invalid",
        monitor=monitor,
    ))
    _purpose_q2_f1_subset(trust_roots, roles, raw_manifest["artifacts"], manifest,
                          trust["raw_input_inventory"],
                          DiagnosticResourceMonitor() if monitor is None else monitor)
    trust["candidate_snapshot"] = _purpose_read_candidate_snapshot(
        trust_roots, roles, declaration, checkpoint, manifest, source,
        trust["membership_checkpoint"], monitor=monitor,
    )
    trust.update(trusted_run=trusted_run, roles=roles, declaration=declaration,
                 runtime_code=runtime, checkpoint=checkpoint, source_index=source)
    return trust


def _purpose_preliminary_trust(
    trusted_run: DiagnosticTrustPin, *, root: Path,
    declaration: Mapping[str, Any],
) -> None:
    """Bounded public check only; Q3 candidate and raw-input admission is private."""
    if type(trusted_run) is not DiagnosticTrustPin:
        raise NcenError("diagnostic_trust_anchor_missing")
    path = Path(trusted_run.manifest_path)
    if (not path.is_absolute() or not isinstance(trusted_run.manifest_sha256, str)
            or _DIAGNOSTIC_HASH.fullmatch(trusted_run.manifest_sha256) is None
            or trusted_run.manifest_sha256 == "0" * 64
            or type(trusted_run.manifest_size) is not int or trusted_run.manifest_size <= 0
            or path.parent.is_symlink()):
        raise NcenError("diagnostic_trust_anchor_mismatch")
    try:
        path.resolve(strict=True).relative_to(root.resolve())
    except ValueError:
        pass
    except OSError as exc:
        raise NcenError("diagnostic_trust_anchor_mismatch") from exc
    else:
        raise NcenError("diagnostic_trust_anchor_mismatch")
    try:
        raw = _purpose_trust_bytes({"trust": path.parent}, {
            "root_id": "trust", "path": path.name,
            "sha256": trusted_run.manifest_sha256, "bytes": trusted_run.manifest_size,
        }, code="diagnostic_trust_anchor_mismatch")
        trust = _purpose_json_load(raw, code="diagnostic_trust_anchor_mismatch")
    except (OSError, NcenError) as exc:
        raise NcenError("diagnostic_trust_anchor_mismatch") from exc
    if (type(trust) is not dict or set(trust) != {"schema_version", "lane", "roles", "logical_ledger"}
            or trust["schema_version"] != "ncen_diagnostic_trust_manifest_v3"
            or trust["lane"] not in _DIAGNOSTIC_MANIFEST_KINDS):
        raise NcenError("diagnostic_pin_digest_invalid")
    if trust["lane"] != "synthetic_fixture":
        raise NcenError("diagnostic_required_pins_unverified")
    if declaration.get("trusted_run_manifest_sha256") != trusted_run.manifest_sha256:
        raise NcenError("diagnostic_declared_pin_mismatch")
    if declaration.get("lane") != trust["lane"]:
        raise NcenError("diagnostic_lane_mismatch")
    if declaration.get("pin_roles") != trust["roles"]:
        raise NcenError("diagnostic_declared_pin_mismatch")


def _purpose_preliminary_bytes(path: Path, *, code: str) -> bytes:
    """Capture one bounded control snapshot before interpreting its contents."""
    import stat as stat_module

    try:
        status = path.lstat()
    except OSError as exc:
        raise NcenError("diagnostic_export_incomplete") from exc
    if (not stat_module.S_ISREG(status.st_mode)
            or getattr(status, "st_file_attributes", 0) & 0x400
            or status.st_size > 16 * 1024 * 1024):
        raise NcenError(f"{code}_invalid")
    with path.open("rb") as handle:
        raw = handle.read(16 * 1024 * 1024 + 1)
    if len(raw) != status.st_size or len(raw) > 16 * 1024 * 1024:
        raise NcenError(f"{code}_invalid")
    return raw


def _purpose_preliminary_json(path: Path, *, code: str) -> Any:
    """Bound controls before the nonaccepting public envelope-version check."""
    return _purpose_json_load(_purpose_preliminary_bytes(path, code=code), code=code)


_PURPOSE_BASELINE_SEAL = object()  # Legacy private reader remains closed at C1_INCOMPLETE.
_PURPOSE_BASELINE_FILES = (
    ("cohort.jsonl", "cohort_checkpoint"),
    ("cohort_derivation.json", "cohort_derivation_receipt"),
    ("ncen_index_entries.jsonl", None),
    ("ncen_copies.jsonl", "ncen_parsed_copies"),
    ("source_reconciliation.json", "source_reconciliation"),
    ("raw_input_inventory.jsonl", None),
)


import threading  # Q3 additions must not change the frozen v3 prefix.
import weakref
from typing import NamedTuple


class DiagnosticBaselineCheckpoint:
    """Factory-issued identity; neither evidence nor admission is carried on this object."""

    __slots__ = ("_token", "__weakref__")  # noqa: RUF023 - Opaque carrier contract order.

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("diagnostic_baseline_checkpoint_factory_only")

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("diagnostic_baseline_checkpoint_immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("diagnostic_baseline_checkpoint_immutable")

    def __copy__(self) -> DiagnosticBaselineCheckpoint:
        raise TypeError("diagnostic_baseline_checkpoint_not_copyable")

    def __deepcopy__(self, memo: Any) -> DiagnosticBaselineCheckpoint:
        raise TypeError("diagnostic_baseline_checkpoint_not_copyable")

    def __reduce_ex__(self, protocol: int) -> Any:
        raise TypeError("diagnostic_baseline_checkpoint_not_serializable")

    @property
    def digest(self) -> str:
        return hashlib.sha256(_diagnostic_baseline_inspection(self)).hexdigest()

    @property
    def lane(self) -> str:
        return json.loads(_diagnostic_baseline_inspection(self))["lane"]


class _BaselineIssuance(NamedTuple):
    carrier_ref: weakref.ReferenceType[DiagnosticBaselineCheckpoint]
    token: object
    trusted_run_ref: DiagnosticTrustPin
    source_index_ref: DiagnosticSourceIndex
    source_custody_ref: _DiagnosticSourceCustody
    membership_ref: DiagnosticSyntheticMembershipCheckpoint
    trust_bytes: bytes
    source_binding_bytes: bytes
    membership_binding_bytes: bytes
    payload_bytes: bytes
    payload_digest: str


_DIAGNOSTIC_BASELINE_REGISTRY: dict[int, _BaselineIssuance] = {}
_DIAGNOSTIC_BASELINE_LOCK = threading.Lock()


def _diagnostic_baseline_cleanup(
    carrier_id: int, callback_weakref: weakref.ReferenceType[DiagnosticBaselineCheckpoint],
) -> None:
    with _DIAGNOSTIC_BASELINE_LOCK:
        current = _DIAGNOSTIC_BASELINE_REGISTRY.get(carrier_id)
        if current is not None and current.carrier_ref is callback_weakref:
            _DIAGNOSTIC_BASELINE_REGISTRY.pop(carrier_id, None)


def _diagnostic_baseline_value(value: Any) -> Any:
    """Freeze behavior-relevant dataclass state without copying private authority handles."""
    if isinstance(value, dt.datetime):
        return _diagnostic_timestamp(value)
    if isinstance(value, dt.date):
        return value.isoformat()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {item.name: _diagnostic_baseline_value(getattr(value, item.name))
                for item in dataclasses.fields(value) if not item.name.startswith("_")}
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise NcenError("diagnostic_baseline_unadmitted")
        return {key: _diagnostic_baseline_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_diagnostic_baseline_value(item) for item in value]
    if value is None or type(value) in (str, bool, int, float):
        return value
    raise NcenError("diagnostic_baseline_unadmitted")


def _diagnostic_baseline_trust_bytes(trust: DiagnosticTrustPin) -> bytes:
    return _diagnostic_canonical({"path": str(trust.manifest_path),
                                  "sha256": trust.manifest_sha256, "bytes": trust.manifest_size})


def _diagnostic_baseline_source_bytes(
    source: DiagnosticSourceIndex, custody: _DiagnosticSourceCustody,
) -> bytes:
    return _diagnostic_canonical({
        "source_index": _diagnostic_baseline_value(source),
        "custody": {
            "version": custody.custody_version, "lane": custody.lane,
            "manifest_sha256": custody.manifest_sha256, "manifest_size": custody.manifest_size,
            "manifest_kind": custody.manifest_kind, "evidence_digest": custody.evidence_digest,
            "exclusion_ledger_digest": custody.exclusion_ledger_digest,
            "rows": _diagnostic_baseline_value(custody.rows),
            "row_ids": _diagnostic_baseline_value(custody.row_ids),
            "exclusions": _diagnostic_baseline_value(custody.exclusions),
            "boundary_examples": _diagnostic_baseline_value(custody.boundary_examples),
            "row_by_id": _diagnostic_baseline_value(custody.row_by_id),
            "rows_by_accession": _diagnostic_baseline_value(custody._rows_by_accession),
            "exclusions_by_accession": _diagnostic_baseline_value(custody._exclusions_by_accession),
            "exclusions_by_cik": _diagnostic_baseline_value(custody._exclusions_by_cik),
            "acquisition_ledger": _diagnostic_baseline_value(custody.acquisition_ledger),
        },
    })


def _diagnostic_baseline_membership_bytes(
    membership: DiagnosticSyntheticMembershipCheckpoint,
    contexts: Any, roles: Any,
) -> bytes:
    if membership._seal is not _PURPOSE_MEMBERSHIP_SEAL:
        raise NcenError("diagnostic_baseline_unadmitted")
    return _diagnostic_canonical({"checkpoint": _diagnostic_baseline_value(membership),
                                  "contexts": contexts, "input_identities": roles})


def _diagnostic_baseline_payload(
    admitted: Mapping[str, Any], *, trust_bytes: bytes, source_bytes: bytes,
    membership_bytes: bytes,
) -> bytes:
    candidate = admitted["candidate_snapshot"]
    audit = candidate["audit"]
    return _diagnostic_canonical({
        "schema_version": "ncen_diagnostic_baseline_admission_v2", "lane": "synthetic_fixture",
        "trust": json.loads(trust_bytes), "source": json.loads(source_bytes),
        "membership": json.loads(membership_bytes),
        "roles": admitted["roles"], "runtime_code": admitted["runtime_code"],
        "checkpoint": admitted["checkpoint"],
        "contexts": [{"R": date.isoformat(), "K": _diagnostic_timestamp(cutoff), "mode": mode}
                     for date, cutoff, mode in candidate["contexts"]],
        "members": [_purpose_cohort_record(member, admitted["declaration"]["inventory_digest"])
                    for member in candidate["members"]],
        "raw_input_inventory": audit["raw_input_inventory"],
        "index_observations": audit["index_observations"],
        "copy_observations": audit["copy_observations"],
        "header_observations": audit["header_observations"],
        "reconciliation": audit["reconciliation"], "coverage": candidate["coverage"],
        "index_entries": [{**dataclasses.asdict(entry), "date_filed": entry.date_filed.isoformat()}
                          for entry in candidate["index_entries"]],
        "filings": [_purpose_baseline_filing(filing) for filing in candidate["filings"]],
        "headers": {key: value.to_record() for key, value in candidate["headers"].items()},
        "merged": {"by_cik": {cik: [_purpose_baseline_filing(filing) for filing in filings]
                              for cik, filings in candidate["merged"].by_cik.items()},
                   "excluded": candidate["merged"].excluded},
        "selections": [_diagnostic_selection_payload(selection)
                       for selection in candidate["selections"]],
    })


def _diagnostic_bound_baseline_admission(
    checkpoint: DiagnosticBaselineCheckpoint, *, trusted_run: DiagnosticTrustPin,
    source_index: DiagnosticSourceIndex | None = None,
    membership_checkpoint: DiagnosticSyntheticMembershipCheckpoint | None = None,
) -> dict[str, Any]:
    if type(checkpoint) is not DiagnosticBaselineCheckpoint:
        raise NcenError("diagnostic_baseline_unadmitted")
    with _DIAGNOSTIC_BASELINE_LOCK:
        record = _DIAGNOSTIC_BASELINE_REGISTRY.get(id(checkpoint))
        try:
            token = object.__getattribute__(checkpoint, "_token")
        except AttributeError as exc:
            raise NcenError("diagnostic_baseline_unadmitted") from exc
        if (record is None or record.carrier_ref() is not checkpoint or token is not record.token
                or trusted_run is not record.trusted_run_ref
                or (source_index is not None and source_index is not record.source_index_ref)
                or (membership_checkpoint is not None
                    and membership_checkpoint is not record.membership_ref)):
            raise NcenError("diagnostic_baseline_unadmitted")
    try:
        custody = _diagnostic_bound_source_custody(record.source_index_ref)
        trust_bytes = _diagnostic_baseline_trust_bytes(record.trusted_run_ref)
        source_bytes = _diagnostic_baseline_source_bytes(record.source_index_ref, custody)
        payload = json.loads(record.payload_bytes)
        membership_bytes = _diagnostic_baseline_membership_bytes(
            record.membership_ref, payload["checkpoint"]["contexts"], payload["roles"],
        )
    except (NcenError, AttributeError, TypeError, ValueError, KeyError) as exc:
        raise NcenError("diagnostic_baseline_unadmitted") from exc
    if (custody is not record.source_custody_ref
            or trust_bytes != record.trust_bytes
            or source_bytes != record.source_binding_bytes
            or membership_bytes != record.membership_binding_bytes
            or hashlib.sha256(record.payload_bytes).hexdigest() != record.payload_digest):
        raise NcenError("diagnostic_baseline_unadmitted")
    try:
        closed_keys = {
            "schema_version", "lane", "trust", "source", "membership", "roles",
            "runtime_code", "checkpoint", "contexts", "members", "raw_input_inventory",
            "index_observations", "copy_observations", "header_observations", "reconciliation",
            "coverage", "index_entries", "filings", "headers", "merged", "selections",
        }
        if (type(payload) is not dict or set(payload) != closed_keys
                or payload["schema_version"] != "ncen_diagnostic_baseline_admission_v2"
                or payload["lane"] != "synthetic_fixture"
                or _diagnostic_canonical(payload) != record.payload_bytes
                or _diagnostic_canonical(payload["trust"]) != record.trust_bytes
                or _diagnostic_canonical(payload["source"]) != record.source_binding_bytes
                or _diagnostic_canonical(payload["membership"]) != record.membership_binding_bytes
                or _diagnostic_canonical(payload["roles"])
                != _diagnostic_canonical(payload["membership"]["input_identities"])
                or _diagnostic_canonical(payload["contexts"])
                != _diagnostic_canonical(payload["membership"]["contexts"])):
            raise NcenError("diagnostic_baseline_unadmitted")
    except (NcenError, TypeError, ValueError, KeyError) as exc:
        raise NcenError("diagnostic_baseline_unadmitted") from exc
    with _DIAGNOSTIC_BASELINE_LOCK:
        if (_DIAGNOSTIC_BASELINE_REGISTRY.get(id(checkpoint)) is not record
                or object.__getattribute__(checkpoint, "_token") is not record.token):
            raise NcenError("diagnostic_baseline_unadmitted")
    return payload


def _diagnostic_baseline_inspection(checkpoint: DiagnosticBaselineCheckpoint) -> bytes:
    if type(checkpoint) is not DiagnosticBaselineCheckpoint:
        raise NcenError("diagnostic_baseline_unadmitted")
    with _DIAGNOSTIC_BASELINE_LOCK:
        record = _DIAGNOSTIC_BASELINE_REGISTRY.get(id(checkpoint))
        if record is None or record.carrier_ref() is not checkpoint:
            raise NcenError("diagnostic_baseline_unadmitted")
        trust = record.trusted_run_ref
    return _diagnostic_canonical(_diagnostic_bound_baseline_admission(checkpoint, trusted_run=trust))


def _purpose_baseline_filing(filing: NcenFiling) -> dict[str, Any]:
    def adviser(item: AdviserRecord) -> dict[str, Any]:
        return {"role": item.role, "file_number": item.file_number, "crd": item.crd,
                "lei": item.lei, "raw": list(item.raw)}

    def underwriter(item: UnderwriterRecord) -> dict[str, Any]:
        return {"file_number": item.file_number, "crd": item.crd, "lei": item.lei,
                "raw": list(item.raw)}

    return {
        "accession_number": filing.accession_number, "registrant_cik": filing.registrant_cik,
        "form_type": filing.form_type, "form_type_source": filing.form_type_source,
        "report_period_end": _diagnostic_optional_date(filing.report_period_end),
        "filing_date": _diagnostic_optional_date(filing.filing_date),
        "public_available_at": _diagnostic_optional_timestamp(filing.public_available_at),
        "public_time_basis": filing.public_time_basis,
        "data_known_at": _diagnostic_optional_timestamp(filing.data_known_at),
        "source": filing.source, "source_refs": list(filing.source_refs),
        "family_answer": filing.family_answer, "family_name_raw": filing.family_name_raw,
        "funds": [{"series_id": fund.series_id,
                   "advisers": [adviser(item) for item in fund.advisers]} for fund in filing.funds],
        "underwriters": [underwriter(item) for item in filing.underwriters],
        "status": filing.status, "reasons": list(filing.reasons),
        "schema_version": filing.schema_version,
        "retrieved_at": _diagnostic_optional_timestamp(filing.retrieved_at),
        "acceptance_at": _diagnostic_optional_timestamp(filing.acceptance_at),
        "public_date_bound": _diagnostic_optional_timestamp(filing.public_date_bound),
        "header_retrieved_at": _diagnostic_optional_timestamp(filing.header_retrieved_at),
    }


def _purpose_baseline_jsonl(raw: bytes, expected: list[dict[str, Any]], *, code: str) -> None:
    if (raw and not raw.endswith(b"\n")) or b"\r" in raw:
        raise NcenError(code)
    try:
        rows = [_purpose_json_load(line, code=code) for line in raw.splitlines()]
    except NcenError as exc:
        raise NcenError(code) from exc
    if rows != expected or raw != _purpose_jsonl_bytes(expected):
        raise NcenError(code)


def _purpose_read_candidate_snapshot(
    roots: Mapping[str, Path], roles: Mapping[str, tuple[dict[str, Any], ...]],
    declaration: Mapping[str, Any], baseline: Mapping[str, Any],
    manifest: Mapping[str, Any], sources: DiagnosticSourceIndex,
    membership: DiagnosticSyntheticMembershipCheckpoint,
    *, monitor: DiagnosticResourceMonitor | None = None,
) -> dict[str, Any]:
    """Verify Q1/Q2a/Q2b under external pins, without issuing an admitted C1 handle."""
    if not isinstance(baseline, dict):
        raise NcenError("diagnostic_checkpoint_schema_unsupported")
    descriptors = baseline.get("files")
    if (not isinstance(descriptors, list) or len(descriptors) != len(_PURPOSE_BASELINE_FILES)
            or any(not isinstance(descriptor, dict) for descriptor in descriptors)):
        raise NcenError("diagnostic_checkpoint_pin_mismatch")
    pins = [roles[role][0] if role is not None else {
        "root_id": roles["diagnostic_baseline_checkpoint"][0]["root_id"],
        "path": name, "sha256": descriptor.get("sha256"), "bytes": descriptor.get("bytes"),
    } for (name, role), descriptor in zip(_PURPOSE_BASELINE_FILES, descriptors, strict=True)]
    for descriptor, ((name, _role), pin) in zip(
        descriptors, zip(_PURPOSE_BASELINE_FILES, pins, strict=True), strict=True,
    ):
        if (not isinstance(descriptor, dict) or set(descriptor) != {"path", "sha256", "bytes", "rows"}
                or descriptor["path"] != name or descriptor["sha256"] != pin["sha256"]
                or descriptor["bytes"] != pin["bytes"] or type(descriptor["rows"]) is not int
                or descriptor["rows"] < 0 or (not name.endswith("jsonl") and descriptor["rows"] != 1)):
            raise NcenError("diagnostic_checkpoint_pin_mismatch")
        try:
            _purpose_trust_descriptor(pin)
        except NcenError as exc:
            raise NcenError("diagnostic_checkpoint_pin_mismatch") from exc
    expected_versions = {
        "selector": RULE_VERSION, "merge": RULE_VERSION,
        "amendment": AMENDMENT_SEMANTICS_VERSION, "cohort": DIAGNOSTIC_COHORT_VERSION,
        "source": DIAGNOSTIC_SOURCE_ATTESTATION_VERSION, "admission": DIAGNOSTIC_ADMISSION_VERSION,
        "normalizer": DIAGNOSTIC_NAME_NORMALIZER_VERSION,
        "edge": DIAGNOSTIC_EDGE_VERSION, "fold": DIAGNOSTIC_FOLD_PROTOCOL_VERSION,
    }
    if (set(baseline) != {"schema_version", "lane", "purpose", "diagnostic_only", "contexts",
                          "versions", "inventory_digest", "cohort_digest", "ncen_evidence_digest",
                          "exclusion_ledger_digest", "files", "source_manifest_pin", "acquisition_pin"}
            or baseline["schema_version"] != "ncen_diagnostic_baseline_checkpoint_v2"
            or baseline["lane"] != "synthetic_fixture"
            or baseline["purpose"] != "ncen_purpose_diagnostic_selection_replay"
            or baseline["diagnostic_only"] is not True
            or _diagnostic_canonical(baseline["versions"]) != _diagnostic_canonical(expected_versions)
            or baseline["contexts"] != declaration.get("contexts")
            or any(baseline[key] != declaration.get(key) for key in (
                "inventory_digest", "cohort_digest", "ncen_evidence_digest", "exclusion_ledger_digest"))
            or baseline["ncen_evidence_digest"] != sources.evidence_digest
            or _diagnostic_canonical(baseline["source_manifest_pin"]) != _diagnostic_canonical(
                roles["ncen_source_manifest"][0])
            or _diagnostic_canonical(baseline["acquisition_pin"]) != _diagnostic_canonical({
                "lane": "synthetic_fixture", "scope": roles["acquisition_scope"][0],
                "sha256sums": roles["acquisition_sha256sums"][0],
            })):
        raise NcenError("diagnostic_checkpoint_pin_mismatch")
    read = {name: _purpose_trust_bytes(roots, pin, code="diagnostic_checkpoint_pin_mismatch", monitor=monitor)
            for (name, _), pin in zip(_PURPOSE_BASELINE_FILES, pins, strict=True)}
    for descriptor in descriptors:
        if descriptor["rows"] != (len(read[descriptor["path"]].splitlines())
                                  if descriptor["path"].endswith("jsonl") else 1):
            raise NcenError("diagnostic_checkpoint_pin_mismatch")
    expected_cohort = sorted(
        (_purpose_cohort_record(member, declaration["inventory_digest"]) for member in membership.members),
        key=lambda row: row["record_id"],
    )
    _purpose_baseline_jsonl(read["cohort.jsonl"], expected_cohort,
                            code="diagnostic_cohort_baseline_mismatch")
    receipt = _purpose_json_load(read["cohort_derivation.json"], code="diagnostic_checkpoint_pin_mismatch")
    if (not isinstance(receipt, dict)
            or read["cohort_derivation.json"] != _diagnostic_canonical(receipt) + b"\n"
            or receipt.get("cohort_sha256") != pins[0]["sha256"]
            or receipt.get("cohort_digest") != baseline["cohort_digest"]
            or receipt.get("membership_definition_sha256") != roles["synthetic_membership_definition"][0]["sha256"]):
        raise NcenError("diagnostic_cohort_baseline_mismatch")
    contexts = tuple((_purpose_parse_date(item["R"], code="diagnostic_baseline_R"),
                      _purpose_parse_timestamp(item["K"], code="diagnostic_baseline_K"), item["mode"])
                     for item in baseline["contexts"])
    if (tuple(item[0] for item in contexts) != membership.report_dates
            or len({(item[1], item[2]) for item in contexts}) != 1):
        raise NcenError("diagnostic_context_coverage_mismatch")
    candidate = _purpose_q2_candidates(
        roots, roles, manifest, cohort_ciks={member.cik for member in membership.members}, monitor=monitor,
    )
    audit = candidate["audit"]
    identities: set[tuple[Any, ...]] = set()
    for line in read["raw_input_inventory.jsonl"].splitlines():
        row = _purpose_json_load(line, code="diagnostic_raw_membership_mismatch")
        if not isinstance(row, dict) or any(key not in row for key in (
            "root_id", "artifact_id", "container_member", "locator_kind", "locator",
        )):
            raise NcenError("diagnostic_raw_membership_mismatch")
        identity = tuple(row[key] for key in (
            "root_id", "artifact_id", "container_member", "locator_kind", "locator",
        ))
        if (not all(isinstance(value, str) for value in identity[:2] + identity[3:])
                or (identity[2] is not None and not isinstance(identity[2], str))):
            raise NcenError("diagnostic_pinned_input_invalid")
        if identity in identities:
            raise NcenError("diagnostic_raw_physical_duplicate")
        identities.add(identity)
    for path, key, code in (
        ("raw_input_inventory.jsonl", "raw_input_inventory", "diagnostic_raw_membership_mismatch"),
        ("ncen_index_entries.jsonl", "index_observations", "diagnostic_candidate_universe_mismatch"),
        ("ncen_copies.jsonl", "copy_observations", "diagnostic_source_checkpoint_mismatch"),
    ):
        _purpose_baseline_jsonl(read[path], audit[key], code=code)
    reconciliation = read["source_reconciliation.json"]
    if reconciliation != _diagnostic_canonical(audit["reconciliation"]) + b"\n":
        raise NcenError("diagnostic_candidate_universe_mismatch")
    for row in audit["copy_observations"]:
        if row["admission_disposition"] != "f1_admitted":
            continue
        filing = row["filing"]
        assert filing is not None
        source_rows = (item for item in sources.rows if item.artifact_id == row["artifact_id"]
                       and item.accession_number == filing["accession_number"]
                       and item.source_kind == row["source_kind"])
        expected_copy_id = _diagnostic_source_copy_id(row["source_kind"], row["artifact_id"],
                                                       row["raw_sha256"], filing["accession_number"])
        if any(item.source_copy_id != expected_copy_id
               or item.projection_digest != candidate["parser_results"][row["observation_id"]].projection_digest
               for item in source_rows):
            raise NcenError("diagnostic_source_checkpoint_mismatch")
    candidate["contexts"] = contexts
    candidate["members"] = membership.members
    candidate["selections"] = tuple(
        diagnostic_selection(candidate["merged"], sources, member.cik, report_date, cutoff,
                             mode=mode, fund_keys=member.fund_keys)
        for report_date, cutoff, mode in contexts
        for member in membership.members if member.report_date == report_date
    )
    candidate["input_identities"] = tuple(sorted(
        (f"{name}:{pin['root_id']}:{pin['path']}", pin["sha256"])
        for name, role_pins in roles.items() for pin in role_pins
    ))
    return candidate


def _purpose_baseline_time_status(value: Any, cutoff: dt.datetime) -> str:
    time = _diagnostic_parse_timestamp(value, "diagnostic_baseline_time")
    return "unknown" if time is None else "after_K" if time > cutoff else "by_K"


_PURPOSE_RAW_AUDIT_FIELDS = frozenset({
    "artifact_id", "kind", "root_id", "path", "sha256", "bytes", "retrieved_at",
    "public_at", "data_known_at", "package_label", "accession_claim", "cik_claim",
    "source_url", "header_artifact_ids", "acquisition_request_locators",
})
_PURPOSE_RAW_INPUT_VERSION = "ncen_diagnostic_raw_input_inventory_v1"


def _purpose_raw_control(
    data: bytes, *, canonical: bool = True, acquisition: bool = False,
) -> Any:
    try:
        value = _purpose_json_load(data, code="diagnostic_pinned_input_invalid")
        encoded = (json.dumps(value, sort_keys=True, ensure_ascii=True,
                              separators=(",", ":"), allow_nan=False).encode("utf-8")
                   if acquisition else _diagnostic_canonical(value))
        if canonical and encoded + b"\n" != data:
            raise NcenError("diagnostic_pinned_input_invalid")
    except (ValueError, TypeError, RecursionError, NcenError) as exc:
        raise NcenError("diagnostic_pinned_input_invalid") from exc
    return value


def _purpose_raw_file(
    roots: Mapping[str, Path], pin: Mapping[str, Any], monitor: DiagnosticResourceMonitor,
    limit: int,
) -> bytes:
    relative = pin["path"]
    if (not isinstance(relative, str) or not relative or "\\" in relative or ":" in relative
            or any(part in {"", ".", ".."} for part in relative.split("/"))):
        raise NcenError("diagnostic_artifact_path_unsafe")
    root = roots.get(pin["root_id"])
    if root is not None and any((root / "/".join(relative.split("/")[:number])).is_symlink()
                                for number in range(1, len(relative.split("/")) + 1)):
        raise NcenError("diagnostic_artifact_symlink_forbidden")
    path = _purpose_trusted_file(roots, pin, code="diagnostic_input_unavailable")
    try:
        return _diagnostic_verify_file(
            path, expected_sha256=pin["sha256"], expected_size=pin["bytes"],
            label="diagnostic_input", monitor=monitor, max_bytes=limit, phase="raw_audit_read",
        )
    except NcenError as exc:
        if str(exc) in {"diagnostic_input_size_mismatch", "diagnostic_input_sha256_mismatch"}:
            raise NcenError("diagnostic_input_bytes_mismatch") from exc
        raise


def _purpose_enumerate_raw_inputs(
    roots: Mapping[str, Path], roles: Mapping[str, tuple[dict[str, Any], ...]],
    source_manifest: Mapping[str, Any], *, monitor: DiagnosticResourceMonitor | None = None,
) -> tuple[dict[str, Any], ...]:
    """Inventory pinned physical input units, without granting F1 custody or baseline admission."""
    from .sec_acquisition import (
        _INDEX_LINE,
        SecHeaderError,
        SecIndexError,
        parse_acceptance_header,
        parse_form_index,
    )

    active = DiagnosticResourceMonitor() if monitor is None else monitor
    raw_pin = roles["ncen_raw_audit_manifest"][0]
    raw_manifest = _purpose_raw_control(_purpose_raw_file(
        roots, raw_pin, active, active.limits.manifest_max_bytes,
    ))
    if (not isinstance(raw_manifest, dict) or set(raw_manifest) != {
            "schema_version", "lane", "purpose", "artifacts", "index_manifest_pin",
            "acquisition_scope_pin", "acquisition_sha256sums_pin", "parser_code_manifest_sha256",
        } or raw_manifest["schema_version"] != "ncen_diagnostic_raw_audit_manifest_v1"
            or raw_manifest["lane"] != "synthetic_fixture"
            or raw_manifest["purpose"] != "complete_ncen_candidate_audit"
            or raw_manifest["index_manifest_pin"] != roles["ncen_index_manifest"][0]
            or raw_manifest["acquisition_scope_pin"] != roles["acquisition_scope"][0]
            or raw_manifest["acquisition_sha256sums_pin"] != roles["acquisition_sha256sums"][0]
            or raw_manifest["parser_code_manifest_sha256"] != roles["runtime_code_manifest"][0]["sha256"]
            or not isinstance(raw_manifest["artifacts"], list)):
        raise NcenError("diagnostic_pinned_input_invalid")

    artifacts = raw_manifest["artifacts"]
    ids: set[str] = set()
    paths: set[tuple[str, str]] = set()
    for item in artifacts:
        if not isinstance(item, dict) or set(item) != _PURPOSE_RAW_AUDIT_FIELDS:
            raise NcenError("diagnostic_pinned_input_invalid")
        if (not isinstance(item["artifact_id"], str) or not item["artifact_id"]
                or not isinstance(item["kind"], str)
                or item["kind"] not in {"dera_zip", "edgar_xml", "header"}
                or not isinstance(item["root_id"], str) or not item["root_id"]
                or not isinstance(item["path"], str) or not item["path"]):
            raise NcenError("diagnostic_pinned_input_invalid")
        if ("\\" in item["path"] or ":" in item["path"]
                or any(part in {"", ".", ".."} for part in item["path"].split("/"))):
            raise NcenError("diagnostic_artifact_path_unsafe")
        try:
            _purpose_trust_descriptor({key: item[key] for key in ("root_id", "path", "sha256", "bytes")})
        except NcenError as exc:
            raise NcenError("diagnostic_pinned_input_invalid") from exc
        for name in ("retrieved_at", "public_at", "data_known_at"):
            value = item[name]
            if value is not None:
                try:
                    if _diagnostic_timestamp(_purpose_parse_timestamp(value, code="diagnostic_raw_time")) != value:
                        raise NcenError("diagnostic_pinned_input_invalid")
                except NcenError as exc:
                    raise NcenError("diagnostic_pinned_input_invalid") from exc
        for name in ("package_label", "accession_claim", "cik_claim", "source_url"):
            if item[name] is not None and (not isinstance(item[name], str) or not item[name]):
                raise NcenError("diagnostic_pinned_input_invalid")
        if (item["accession_claim"] is not None
                and _ACCESSION.fullmatch(item["accession_claim"]) is None):
            raise NcenError("diagnostic_pinned_input_invalid")
        if item["cik_claim"] is not None:
            try:
                _diagnostic_normalized_cik(item["cik_claim"])
            except NcenError as exc:
                raise NcenError("diagnostic_pinned_input_invalid") from exc
        if (item["kind"] == "dera_zip") != (item["package_label"] is not None):
            raise NcenError("diagnostic_pinned_input_invalid")
        for name in ("header_artifact_ids", "acquisition_request_locators"):
            value = item[name]
            if (not isinstance(value, list) or not all(isinstance(entry, str) and entry for entry in value)
                    or value != sorted(set(value))):
                raise NcenError("diagnostic_pinned_input_invalid")
        key = item["root_id"], item["path"]
        if item["artifact_id"] in ids or key in paths:
            raise NcenError("diagnostic_pinned_input_invalid")
        ids.add(item["artifact_id"])
        paths.add(key)
    if [item["artifact_id"] for item in artifacts] != sorted(ids):
        raise NcenError("diagnostic_pinned_input_invalid")
    if any(header not in {item["artifact_id"] for item in artifacts if item["kind"] == "header"}
           for item in artifacts for header in item["header_artifact_ids"]):
        raise NcenError("diagnostic_pinned_input_invalid")

    # The F1 manifest remains an independent admission channel. Compare shared
    # metadata without deciding its maximal admissible subset (Q2).
    if not isinstance(source_manifest, dict) or not isinstance(source_manifest.get("artifacts"), list):
        raise NcenError("diagnostic_pinned_input_invalid")
    for item in source_manifest["artifacts"]:
        raw = next((entry for entry in artifacts if entry["artifact_id"] == item["artifact_id"]), None)
        if raw is None:
            raise NcenError("diagnostic_admission_subset_mismatch")
        shared = ("kind", "path", "sha256", "bytes", "retrieved_at", "public_at",
                  "data_known_at", "package_label", "source_url")
        if (raw["root_id"] != roles["ncen_source_manifest"][0]["root_id"]
                or any(raw[name] != item.get(name) for name in shared)
                or raw["accession_claim"] != item.get("accession_number")
                or raw["cik_claim"] != item.get("registrant_cik")
                or raw["header_artifact_ids"] != ([item["header_artifact_id"]]
                    if item.get("header_artifact_id") is not None else [])):
            raise NcenError("diagnostic_admission_subset_mismatch")

    rows: list[dict[str, Any]] = []
    physical_ids: set[str] = set()

    def emit(root_id: str, artifact_id: str, member: str | None, kind: str,
             locator: str, unit: bytes, unit_kind: str, disposition: str,
             reasons: list[str] | None = None, *, unit_sha256: str | None = None) -> None:
        identity = [root_id, artifact_id, member, kind, locator]
        physical_id = _diagnostic_id("raw_physical", identity)
        if physical_id in physical_ids:
            raise NcenError("diagnostic_raw_physical_duplicate")
        physical_ids.add(physical_id)
        rows.append({
            "schema_version": _PURPOSE_RAW_INPUT_VERSION, "physical_input_id": physical_id,
            "root_id": root_id, "artifact_id": artifact_id, "container_member": member,
            "locator_kind": kind, "locator": locator,
            "raw_unit_sha256": unit_sha256 or hashlib.sha256(unit).hexdigest(), "unit_kind": unit_kind,
            "disposition": disposition, "reason_codes": sorted(set(reasons or [])),
            "observation_ids": [],
            "ledger_locators": [locator] if artifact_id.startswith("acquisition:")
                                and disposition == "acquisition_evidence" else [],
        })
        active.rows_read(1, "raw_audit_rows")

    index = _purpose_raw_control(_purpose_raw_file(
        roots, roles["ncen_index_manifest"][0], active, active.limits.manifest_max_bytes,
    ))
    if (not isinstance(index, dict) or set(index) != {"schema_version", "artifacts"}
            or index["schema_version"] != "ncen_diagnostic_index_manifest_v1"
            or not isinstance(index["artifacts"], list)
            or any(not isinstance(item, dict) or set(item) != {"pin", "retrieved_at"}
                   or not isinstance(item["retrieved_at"], str) for item in index["artifacts"])
            or [item["pin"] for item in index["artifacts"]] != list(roles["ncen_index_artifacts"])
            or len(index["artifacts"]) != len(roles["ncen_index_artifacts"])):
        raise NcenError("diagnostic_pinned_input_invalid")
    for entry in index["artifacts"]:
        try:
            _purpose_trust_descriptor(entry["pin"])
            _purpose_parse_timestamp(entry["retrieved_at"], code="diagnostic_index_time")
        except NcenError as exc:
            raise NcenError("diagnostic_pinned_input_invalid") from exc
        pin = entry["pin"]
        data = _purpose_raw_file(roots, pin, active, active.limits.manifest_max_bytes)
        artifact_id = f"index:{pin['root_id']}:{pin['path']}"
        separator = False
        with io.BytesIO(data) as stream:
            number = 0
            while line := stream.readline(active.limits.tsv_line_max_bytes + 1):
                number += 1
                if len(line) > active.limits.tsv_line_max_bytes:
                    raise NcenError(f"diagnostic_index_line_oversized:{number}")
                active.bytes_processed(len(line), "raw_audit_index")
                text = line.removesuffix(b"\n").decode("latin-1").rstrip("\r")
                if re.fullmatch(r"-{20,}\s*", text):
                    separator = True
                if not separator or re.fullmatch(r"-{20,}\s*", text):
                    disposition, reasons = "structural_metadata", []
                elif not text.strip():
                    disposition, reasons = "structural_metadata", ["index_blank_line"]
                else:
                    matched = _INDEX_LINE.match(text.rstrip())
                    if matched is None:
                        disposition, reasons = "parser_quarantine", ["index_line_malformed"]
                    else:
                        try:
                            parsed = parse_form_index(b"Form Type\n" + b"-" * 20 + b"\n" + line)
                        except (SecIndexError, ValueError):
                            disposition, reasons = "parser_quarantine", ["index_line_malformed"]
                        else:
                            disposition, reasons = (
                                ("index_evidence", []) if parsed[0].form_type in {"N-CEN", "N-CEN/A"}
                                else ("out_of_scope_content", ["index_non_ncen_form"])
                            )
                emit(pin["root_id"], artifact_id, None, "physical_line", f"{pin['path']}:{number}",
                     line, "index_line", disposition, reasons)

    for artifact in artifacts:
        pin = {key: artifact[key] for key in ("root_id", "path", "sha256", "bytes")}
        kind = artifact["kind"]
        if kind != "dera_zip":
            limit = active.limits.xml_max_bytes if kind == "edgar_xml" else active.limits.header_max_bytes
            data = _purpose_raw_file(roots, pin, active, limit)
            disposition, reasons = "header_evidence", []
            if (kind == "header" and artifact["accession_claim"] is not None
                    and artifact["source_url"] is not None and artifact["retrieved_at"] is not None):
                try:
                    parse_acceptance_header(
                        data, accession_number=artifact["accession_claim"],
                        url=artifact["source_url"], document_sha256=artifact["sha256"],
                        retrieved_at=_purpose_parse_timestamp(
                            artifact["retrieved_at"], code="diagnostic_raw_time"),
                    )
                except (SecHeaderError, UnicodeDecodeError):
                    disposition, reasons = "parser_quarantine", ["header_unparseable"]
            if kind == "edgar_xml":
                try:
                    root = safe_xml_root(data)
                except XmlSafetyError as exc:
                    disposition, reasons = "parser_quarantine", [f"xml_unsafe:{str(exc).split(':', 1)[0]}"]
                else:
                    disposition = "structural_metadata"
                    if _split(root.tag) != (NCEN_NAMESPACE, "edgarSubmission"):
                        disposition, reasons = "parser_quarantine", ["xml_root_not_ncen"]
                    elif (artifact["accession_claim"] is not None and artifact["source_url"] is not None
                          and artifact["retrieved_at"] is not None):
                        parsed = parse_ncen_primary_doc(
                            data, accession_number=artifact["accession_claim"],
                            source_url=artifact["source_url"],
                            retrieved_at=_purpose_parse_timestamp(
                                artifact["retrieved_at"], code="diagnostic_raw_time"),
                        )
                        disposition = "parsed_evidence" if parsed.usable else "parser_quarantine"
                        reasons = [] if parsed.usable else list(parsed.reasons)
            emit(pin["root_id"], artifact["artifact_id"], None, "document", pin["path"],
                 data, kind, disposition, reasons)
            continue
        path = _purpose_trusted_file(roots, pin, code="diagnostic_input_unavailable")
        handle, _identity, size = _diagnostic_open_held(path, "diagnostic_input", active)
        try:
            if size != pin["bytes"]:
                raise NcenError("diagnostic_input_bytes_mismatch")
            digest = hashlib.sha256()
            while block := handle.read(active.limits.hash_block_bytes):
                digest.update(block)
                active.bytes_read(len(block), "raw_audit_read")
            if digest.hexdigest() != pin["sha256"]:
                raise NcenError("diagnostic_input_bytes_mismatch")
            if size > active.limits.spill_budget_bytes:
                raise NcenError("diagnostic_spill_budget_exceeded")
            handle.seek(0)
            try:
                with zipfile.ZipFile(handle) as archive:
                    members = inspect_zip(archive, NCEN_ZIP_LIMITS)
                    for info in sorted(archive.infolist(), key=lambda value: value.filename):
                        member = info.filename
                        if info.is_dir():
                            if info.file_size != 0:
                                raise ZipSafetyError(f"zip_directory_nonempty:{member}")
                            emit(pin["root_id"], artifact["artifact_id"], member, "zip_member",
                                 member, b"", "zip_directory", "structural_metadata")
                            continue
                        if info.file_size == 0:
                            nonconsumed = (not member.lower().endswith(".tsv")
                                           or member.rsplit("/", 1)[-1].rsplit(".", 1)[0].upper()
                                           not in PINNED_TABLES)
                            emit(pin["root_id"], artifact["artifact_id"], member, "zip_member",
                                 member, b"", "zip_empty_member",
                                 "out_of_scope_content" if nonconsumed else "parser_quarantine",
                                 ["zip_nonconsumed_auxiliary" if nonconsumed else "zip_member_empty"])
                            continue
                        table = member.rsplit("/", 1)[-1].rsplit(".", 1)[0].upper()
                        consumed = member.lower().endswith(".tsv") and table in PINNED_TABLES
                        if not member.lower().endswith(".tsv"):
                            digest_member = hashlib.sha256()
                            with archive.open(members[member]) as stream:
                                while block := stream.read(active.limits.hash_block_bytes):
                                    digest_member.update(block)
                                    active.bytes_processed(len(block), "raw_audit_zip")
                            emit(pin["root_id"], artifact["artifact_id"], member, "zip_member",
                                 member, b"", "zip_auxiliary", "out_of_scope_content",
                                 ["zip_nonconsumed_auxiliary"], unit_sha256=digest_member.hexdigest())
                            continue
                        with archive.open(members[member]) as stream:
                            header_width: int | None = None
                            number = 0
                            while True:
                                data, oversized = _diagnostic_read_bounded_tsv_line(
                                    stream, max_bytes=active.limits.tsv_line_max_bytes, monitor=active,
                                )
                                if data is None:
                                    break
                                number += 1
                                if oversized:
                                    raise NcenError(f"diagnostic_member_row_oversized:{table}:{number}")
                                disposition, reasons = "structural_metadata", []
                                if not consumed:
                                    disposition, reasons = "out_of_scope_content", ["zip_nonconsumed_table"]
                                elif number == 1 and not data.endswith(b"\n"):
                                    disposition, reasons = "parser_quarantine", ["tsv_header_unterminated"]
                                elif not data.strip():
                                    disposition, reasons = "parser_quarantine", ["tsv_blank_line"]
                                else:
                                    try:
                                        fields = _diagnostic_tsv_fields(data, table, number)
                                    except NcenError:
                                        disposition, reasons = "parser_quarantine", ["tsv_row_unparseable"]
                                    else:
                                        if number == 1:
                                            if (len(set(fields)) != len(fields)
                                                    or not set(REQUIRED_COLUMNS[table]).issubset(fields)):
                                                disposition, reasons = "parser_quarantine", ["tsv_header_invalid"]
                                            else:
                                                header_width = len(fields)
                                        elif header_width is None or len(fields) != header_width:
                                            disposition, reasons = "parser_quarantine", ["tsv_row_width_mismatch"]
                                        else:
                                            disposition = "parsed_evidence"
                                emit(pin["root_id"], artifact["artifact_id"], member, "zip_tsv_line",
                                     f"{member}:{number}", data, "tsv_line", disposition, reasons)
            except zipfile.BadZipFile as exc:
                raise ZipSafetyError("zip_invalid:raw_audit") from exc
        finally:
            handle.close()
            active.handle_closed()

    sums_pin = roles["acquisition_sha256sums"][0]
    sums = _purpose_raw_file(roots, sums_pin, active, active.limits.ledger_max_bytes)
    try:
        inventory = _diagnostic_acquisition_inventory(sums)
    except NcenError as exc:
        raise NcenError("diagnostic_pinned_input_invalid") from exc
    for number, line in enumerate(sums.splitlines(keepends=True), start=1):
        emit(sums_pin["root_id"], "acquisition:SHA256SUMS", None, "physical_line",
             f"SHA256SUMS:{number}", line, "sha256sums_line", "acquisition_evidence")
    scope_pin = roles["acquisition_scope"][0]
    if scope_pin["root_id"] != sums_pin["root_id"] or inventory.get(scope_pin["path"]) != scope_pin["sha256"]:
        raise NcenError("diagnostic_pinned_input_invalid")
    for relative, sha in sorted(inventory.items()):
        file_pin = {"root_id": sums_pin["root_id"], "path": relative,
                    "sha256": sha, "bytes": _purpose_trusted_file(
                        roots, {"root_id": sums_pin["root_id"], "path": relative},
                        code="diagnostic_input_unavailable").stat().st_size}
        data = _purpose_raw_file(roots, file_pin, active, active.limits.ledger_max_bytes)
        artifact_id = f"acquisition:{relative}"
        if relative.endswith(".json"):
            if relative.startswith("raw/"):
                _purpose_raw_control(data, canonical=False)
                emit(file_pin["root_id"], artifact_id, None, "document", relative, data,
                     "acquisition_document", "acquisition_evidence")
                continue
            value = _purpose_raw_control(data, acquisition=True)
            if not isinstance(value, (list, dict)):
                raise NcenError("diagnostic_pinned_input_invalid")
            if isinstance(value, list):
                parts = [(f"/{index}", item) for index, item in enumerate(value)]
            else:
                parts = []
                for key, item in sorted(value.items()):
                    escaped = key.replace("~", "~0").replace("/", "~1")
                    if isinstance(item, list) and item:
                        parts.extend((f"/{escaped}/{index}", entry) for index, entry in enumerate(item))
                    else:
                        parts.append((f"/{escaped}", item))
            if not parts:
                parts = [("", value)]
            for pointer, item in parts:
                emit(file_pin["root_id"], artifact_id, None, "json_pointer", pointer,
                     _diagnostic_canonical(item), "acquisition_json_record",
                     "structural_metadata" if relative == "scope.json"
                     and not pointer.startswith("/requests/") else "acquisition_evidence")
        elif relative.endswith(".jsonl"):
            for number, line in enumerate(data.splitlines(keepends=True), start=1):
                if not line.endswith(b"\n"):
                    raise NcenError("diagnostic_pinned_input_invalid")
                _purpose_raw_control(line, acquisition=True)
                emit(file_pin["root_id"], artifact_id, None, "physical_line", f"{relative}:{number}",
                     line, "acquisition_json_record", "acquisition_evidence")
        else:
            emit(file_pin["root_id"], artifact_id, None, "document", relative, data,
                 "acquisition_document", "acquisition_evidence")
    scope_locators = {row["locator"] for row in rows
                      if row["artifact_id"] == "acquisition:scope.json"
                      and row["locator"].startswith("/requests/")}
    if any(locator not in scope_locators for artifact in artifacts
           for locator in artifact["acquisition_request_locators"]):
        raise NcenError("diagnostic_pinned_input_invalid")
    return tuple(row for _position, row in sorted(
        enumerate(rows), key=lambda indexed: (
            indexed[1]["root_id"], indexed[1]["artifact_id"],
            indexed[1]["container_member"] or "", indexed[0],
        ),
    ))


def _purpose_q2_f1_subset(
    roots: Mapping[str, Path], roles: Mapping[str, tuple[dict[str, Any], ...]],
    raw_artifacts: Sequence[Mapping[str, Any]], source_manifest: Mapping[str, Any],
    physical: Sequence[Mapping[str, Any]], monitor: DiagnosticResourceMonitor,
) -> set[str]:
    """Prove whole-artifact F1 eligibility against the independently pinned raw package."""
    from .sec_acquisition import SecHeaderError

    source_pin = roles["ncen_source_manifest"][0]
    root_id = source_pin["root_id"]
    # These are F1 evidence refusals, not general exceptions or resource failures.
    dera_refusals = (
        "diagnostic_member_missing:", "diagnostic_member_allowlist_mismatch",
        "diagnostic_member_header_unparseable:", "diagnostic_member_duplicate_header",
        "diagnostic_member_required_columns_missing:", "diagnostic_member_row_width_mismatch:",
        "diagnostic_dera_copy_quarantined:", "diagnostic_submission_duplicate_or_invalid:",
        "diagnostic_registrant_duplicate", "diagnostic_fund_duplicate_or_invalid:",
        "diagnostic_member_row_unparseable:", "diagnostic_dera_source_identity_unavailable",
        "diagnostic_adviser_fund_join_missing", "diagnostic_adviser_role_unknown",
    )
    xml_refusals = ("diagnostic_xml_copy_quarantined:", "diagnostic_xml_cik_unavailable",
                    "diagnostic_xml_namespace_invalid", "diagnostic_xml_structure_missing")
    by_id = {item["artifact_id"]: item for item in raw_artifacts}
    f1_rows = source_manifest["artifacts"]
    if (not isinstance(f1_rows, list) or any(not isinstance(row, dict) for row in f1_rows)
            or len({row.get("artifact_id") for row in f1_rows}) != len(f1_rows)):
        raise NcenError("diagnostic_admission_subset_mismatch")
    expected: dict[str, dict[str, Any]] = {}
    f1_ids = {item["artifact_id"] for item in f1_rows}
    headers: dict[str, DiagnosticSourceRow] = {}
    header_by_id: dict[str, DiagnosticSourceRow] = {}
    for item in sorted(raw_artifacts, key=lambda entry: (
        entry["artifact_id"] not in f1_ids, entry["artifact_id"],
    )):
        if item["kind"] != "header" or item["root_id"] != root_id:
            continue
        if any(item[name] is None for name in (
                "accession_claim", "cik_claim", "source_url", "retrieved_at", "public_at", "data_known_at")):
            continue
        pin = {name: item[name] for name in ("root_id", "path", "sha256", "bytes")}
        data = _purpose_raw_file(roots, pin, monitor, monitor.limits.header_max_bytes)
        artifact = {**{key: item[key] for key in (
            "artifact_id", "kind", "path", "sha256", "bytes", "retrieved_at", "public_at",
            "data_known_at", "source_url")},
            "accession_number": item["accession_claim"], "registrant_cik": item["cik_claim"]}
        try:
            row = _diagnostic_header_row(artifact, item["path"],
                                         _purpose_trusted_file(roots, pin,
                                                               code="diagnostic_input_unavailable"), data)
        except SecHeaderError:
            continue
        header_by_id[item["artifact_id"]] = row
        if row.accession_number in headers:
            # The equivalent extra witness is audit-only; F1 still owns exactly one header.
            if item["artifact_id"] not in f1_ids:
                continue
            raise NcenError("diagnostic_header_duplicate")
        headers[row.accession_number] = row
        expected[item["artifact_id"]] = artifact

    for item in raw_artifacts:
        if item["kind"] == "header" or item["root_id"] != root_id:
            continue
        kind = item["kind"]
        if any(item[name] is None for name in ("retrieved_at", "public_at", "data_known_at")):
            continue
        pin = {name: item[name] for name in ("root_id", "path", "sha256", "bytes")}
        artifact = {key: item[key] for key in (
            "artifact_id", "kind", "path", "sha256", "bytes", "retrieved_at", "public_at",
            "data_known_at")}
        if kind == "edgar_xml":
            if item["accession_claim"] is None or item["source_url"] is None:
                continue
            if len(item["header_artifact_ids"]) != 1:
                raise NcenError("diagnostic_header_binding_missing")
            header_id = item["header_artifact_ids"][0]
            if header_id not in by_id or header_id not in header_by_id:
                raise NcenError("diagnostic_header_binding_missing")
            artifact.update(accession_number=item["accession_claim"],
                            source_url=item["source_url"], header_artifact_id=header_id)
            data = _purpose_raw_file(roots, pin, monitor, monitor.limits.xml_max_bytes)
            try:
                _diagnostic_xml_rows(artifact, item["path"], data, header_by_id, monitor=monitor)
            except NcenError as exc:
                if not str(exc).startswith(xml_refusals):
                    raise
                continue
        else:
            if item["package_label"] is None:
                continue
            data = _purpose_raw_file(roots, pin, monitor, monitor.limits.spill_budget_bytes)
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                members = inspect_zip(archive, NCEN_ZIP_LIMITS)
                specs = []
                for table in PINNED_TABLES:
                    info = _member_for(members, table)
                    if info is None:
                        break
                    with archive.open(info) as stream:
                        raw_header, oversized = _diagnostic_read_bounded_tsv_line(
                            stream, max_bytes=_TSV_HEADER_MAX, monitor=monitor,
                        )
                    if raw_header is None or oversized or not raw_header.endswith(b"\n"):
                        # Unbounded headers are safety failures, not audit-only evidence.
                        raise ZipSafetyError(f"tsv_header_unbounded:{info.filename}")
                    header = raw_header.decode("utf-8", errors="replace").rstrip("\n\r").split("\t")
                    digest = hashlib.sha256()
                    with archive.open(info) as stream:
                        while block := stream.read(monitor.limits.hash_block_bytes):
                            digest.update(block)
                            monitor.bytes_processed(len(block), "q2_f1_member_hash")
                    specs.append({"path": info.filename, "sha256": digest.hexdigest(),
                                  "bytes": info.file_size, "header": header})
                if len(specs) != len(PINNED_TABLES):
                    # Let unchanged F1 return its evidence-specific missing-table refusal.
                    specs = []
            del data
            artifact.update(package_label=item["package_label"],
                            members=sorted(specs, key=lambda row: row["path"]))
            descriptor = _diagnostic_admit_artifact(
                0, artifact, item["path"], _purpose_trusted_file(
                    roots, pin, code="diagnostic_input_unavailable"), item["bytes"], monitor,
            )
            spool = _DiagnosticSpool(monitor)
            try:
                path = spool.write(descriptor)
                try:
                    _diagnostic_dera_rows(artifact, item["path"], path, headers,
                                          monitor=monitor, store_path=spool.join_store_path(descriptor))
                except NcenError as exc:
                    if not str(exc).startswith(dera_refusals):
                        raise
                    continue
                finally:
                    spool.discard(descriptor, path)
                    monitor.clear_artifact()
            finally:
                spool.close()
        expected[item["artifact_id"]] = artifact

    # A package is indivisible: a quarantined copy/line excludes the entire original ZIP.
    quarantined = {row["artifact_id"] for row in physical
                   if row["disposition"] == "parser_quarantine"}
    if quarantined & expected.keys():
        raise NcenError("diagnostic_admission_subset_mismatch")
    actual = {row["artifact_id"]: row for row in f1_rows}
    if actual != expected or any(item["root_id"] != root_id for item in raw_artifacts
                                 if item["artifact_id"] in actual):
        raise NcenError("diagnostic_admission_subset_mismatch")
    return set(expected)


def _purpose_q1_observations(
    roots: Mapping[str, Path], roles: Mapping[str, tuple[dict[str, Any], ...]],
    source_manifest: Mapping[str, Any], *,
    acquisition_ledger: DiagnosticAcquisitionLedger | None = None,
    monitor: DiagnosticResourceMonitor | None = None,
    verify_f1_subset: bool = True,
    parser_results: dict[str, NcenFiling] | None = None,
    parsed_headers: dict[str, AcceptanceHeader] | None = None,
    parsed_index: dict[str, FormIndexEntry] | None = None,
) -> dict[str, Any]:
    """Reconcile Q1a's complete physical audit; do not admit a baseline or F1 subset."""
    from .sec_acquisition import (
        SecHeaderError,
        SecIndexError,
        parse_acceptance_header,
        parse_form_index,
    )

    active = DiagnosticResourceMonitor() if monitor is None else monitor
    if "ncen_source_manifest" in roles:
        pinned_source = _purpose_raw_control(_purpose_raw_file(
            roots, roles["ncen_source_manifest"][0], active, active.limits.manifest_max_bytes,
        ), canonical=False)
        if pinned_source != source_manifest:
            raise NcenError("diagnostic_raw_membership_mismatch")
    physical = [dict(row) for row in _purpose_enumerate_raw_inputs(
        roots, roles, source_manifest, monitor=active,
    )]
    by_artifact: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in physical:
        by_artifact[row["artifact_id"]].append(row)
    index_observations: list[dict[str, Any]] = []
    copy_observations: list[dict[str, Any]] = []
    header_observations: list[dict[str, Any]] = []
    raw_manifest = _purpose_raw_control(_purpose_raw_file(
        roots, roles["ncen_raw_audit_manifest"][0], active, active.limits.manifest_max_bytes,
    ))
    artifacts = raw_manifest["artifacts"]
    by_id = {item["artifact_id"]: item for item in artifacts}
    index_manifest = _purpose_raw_control(_purpose_raw_file(
        roots, roles["ncen_index_manifest"][0], active, active.limits.manifest_max_bytes,
    ))

    for descriptor in index_manifest["artifacts"]:
        pin = descriptor["pin"]
        artifact_id = f"index:{pin['root_id']}:{pin['path']}"
        raw = _purpose_raw_file(roots, pin, active, active.limits.manifest_max_bytes)
        lines = raw.splitlines(keepends=True)
        if len(lines) != len(by_artifact[artifact_id]):
            raise NcenError("diagnostic_raw_membership_mismatch")
        for physical_row, line in zip(by_artifact[artifact_id], lines, strict=True):
            entry = None
            if physical_row["disposition"] in {"index_evidence", "out_of_scope_content"}:
                try:
                    parsed = parse_form_index(b"Form Type\n" + b"-" * 20 + b"\n" + line)
                except (SecIndexError, ValueError) as exc:
                    raise NcenError("diagnostic_quarantine_disposition_mismatch") from exc
                if len(parsed) != 1:
                    raise NcenError("diagnostic_quarantine_disposition_mismatch")
                item = parsed[0]
                entry = {
                    "form_type": item.form_type, "company_name": item.company_name,
                    "cik": item.cik, "date_filed": item.date_filed.isoformat(),
                    "file_name": item.file_name, "accession_number": item.accession_number,
                }
            observation_id = _diagnostic_id("index_observation_v2", [physical_row["physical_input_id"]])
            if entry is not None and parsed_index is not None:
                parsed_index[observation_id] = item
            record = {
                "schema_version": "ncen_diagnostic_index_observation_v2",
                "observation_id": observation_id, "artifact_id": artifact_id,
                "physical_input_id": physical_row["physical_input_id"],
                "locator": physical_row["locator"], "index_retrieved_at": descriptor["retrieved_at"],
                "parse_disposition": physical_row["disposition"] if entry is None else "parsed",
                "parser_reasons": list(physical_row["reason_codes"]), "entry": entry,
                "equivalence_group_id": None if entry is None else _diagnostic_id("index_equivalence_v2", entry),
            }
            physical_row["observation_ids"] = [observation_id]
            index_observations.append(record)

    headers_by_accession: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in artifacts:
        if item["kind"] != "header":
            continue
        row = by_artifact[item["artifact_id"]][0]
        pin = {name: item[name] for name in ("root_id", "path", "sha256", "bytes")}
        data = _purpose_raw_file(roots, pin, active, active.limits.header_max_bytes)
        parsed_header = None
        reasons = list(row["reason_codes"])
        if all(item[name] is not None for name in ("accession_claim", "source_url", "retrieved_at")):
            try:
                parsed_header = parse_acceptance_header(
                    data, accession_number=item["accession_claim"], url=item["source_url"],
                    document_sha256=item["sha256"], retrieved_at=_purpose_parse_timestamp(
                        item["retrieved_at"], code="diagnostic_pinned_input_invalid"),
                )
            except (SecHeaderError, UnicodeDecodeError):
                reasons = ["header_unparseable"]
        else:
            reasons = ["header_binding_unavailable"]
        observation_id = _diagnostic_id("header_observation_v2", [row["physical_input_id"]])
        record = {
            "schema_version": "ncen_diagnostic_header_observation_v2",
            "observation_id": observation_id, "artifact_id": item["artifact_id"],
            "physical_input_id": row["physical_input_id"],
            "accession_claim": item["accession_claim"], "parse_state": (
                "parsed" if parsed_header is not None else "unparseable"),
            "parser_reasons": reasons, "header": None if parsed_header is None else parsed_header.to_record(),
            "representative_observation_id": None,
        }
        if parsed_header is not None and parsed_headers is not None:
            parsed_headers[observation_id] = parsed_header
        row["observation_ids"] = [observation_id]
        header_observations.append(record)
        if parsed_header is not None:
            headers_by_accession[parsed_header.accession_number].append(record)
    for candidates in headers_by_accession.values():
        # The frozen merger accepts one effective header per accession.
        relevant = lambda row: {key: row["header"][key] for key in (
            "accession_number", "acceptance_at",
            "submission_type", "filing_date", "period", "filer_ciks", "items", "retrieved_at",
        )}
        if any(relevant(row) != relevant(candidates[0]) for row in candidates[1:]):
            raise NcenError("diagnostic_header_conflict")
        representative = min(row["observation_id"] for row in candidates)
        for row in candidates:
            row["representative_observation_id"] = representative

    for item in artifacts:
        if item["kind"] == "header":
            continue
        artifact_rows = by_artifact[item["artifact_id"]]
        pin = {name: item[name] for name in ("root_id", "path", "sha256", "bytes")}
        filings: tuple[NcenFiling, ...] = ()
        package_reasons: list[str] = []
        if item["kind"] == "edgar_xml":
            if all(item[name] is not None for name in ("accession_claim", "source_url", "retrieved_at")):
                data = _purpose_raw_file(roots, pin, active, active.limits.xml_max_bytes)
                filings = (parse_ncen_primary_doc(
                    data, accession_number=item["accession_claim"], source_url=item["source_url"],
                    retrieved_at=_purpose_parse_timestamp(
                        item["retrieved_at"], code="diagnostic_pinned_input_invalid"),
                ),)
            else:
                package_reasons = ["xml_identity_unavailable"]
        else:
            # Consume original verified ZIP bytes. Never filter or rewrite a DERA ZIP.
            data = _purpose_raw_file(roots, pin, active, active.limits.spill_budget_bytes)
            tables: dict[str, list[dict[str, str | None]]] = {}
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as archive:
                    members = inspect_zip(archive, NCEN_ZIP_LIMITS)
                    for table in PINNED_TABLES:
                        info = _member_for(members, table)
                        if info is None:
                            package_reasons.append(f"pinned_table_missing:{table}")
                            continue
                        parsed = _dera_rows(archive, info, table, package_reasons)
                        if parsed is not None:
                            tables[table] = parsed
                if not package_reasons:
                    if item["retrieved_at"] is None or item["data_known_at"] is None:
                        package_reasons = ["dera_time_unavailable"]
                    else:
                        filings = _dera_filings(
                            tables, package_label=item["package_label"], zip_sha=item["sha256"],
                            known_at=_purpose_parse_timestamp(
                                item["data_known_at"], code="diagnostic_pinned_input_invalid"),
                            retrieved_at=_purpose_parse_timestamp(
                                item["retrieved_at"], code="diagnostic_pinned_input_invalid"), stats={},
                        )
            except (zipfile.BadZipFile, UnicodeDecodeError, csv.Error) as exc:
                package_reasons = [f"dera_unparseable:{type(exc).__name__}"]
        if not filings:
            package_reasons.extend(reason for row in artifact_rows for reason in row["reason_codes"])
        # Package-level dependencies preserve every physical row, including headers,
        # malformed/provider rows and orphan records that the frozen parser cannot assign.
        for filing in filings or (None,):
            accession = None if filing is None else filing.accession_number
            claims = sorted({value for value in (item["accession_claim"], accession) if value is not None})
            ciks = sorted({value for value in (
                item["cik_claim"], None if filing is None else filing.registrant_cik,
            ) if value is not None})
            headers = sorted({header["observation_id"] for header in header_observations
                              if header["artifact_id"] in item["header_artifact_ids"]
                              or (accession is not None and header["accession_claim"] == accession)})
            if any(name not in by_id for name in item["header_artifact_ids"]):
                raise NcenError("diagnostic_header_binding_missing")
            reasons = sorted(set(package_reasons if filing is None else filing.reasons))
            parse_state = ("unparseable" if filing is None else
                           "parsed" if filing.usable else "quarantined_partial")
            observation_id = _diagnostic_id("copy_observation_v2", [item["artifact_id"],
                                                               accession, [row["physical_input_id"]
                                                                           for row in artifact_rows]])
            if filing is not None and parser_results is not None:
                parser_results[observation_id] = filing
            copy_observations.append({
                "schema_version": "ncen_diagnostic_copy_observation_v2",
                "observation_id": observation_id, "artifact_id": item["artifact_id"],
                "source_kind": item["kind"], "raw_sha256": item["sha256"],
                "physical_input_ids": [row["physical_input_id"] for row in artifact_rows],
                "accession_claims": claims, "cik_claims": ciks,
                "header_observation_ids": headers,
                "acquisition_request_locators": list(item["acquisition_request_locators"]),
                "f6_exclusion_locators": [], "f6_boundary_locators": [],
                "parse_state": parse_state, "parser_reasons": reasons,
                "filing": None if filing is None else _purpose_baseline_filing(filing),
                "merge_input_kind": "parser_result" if filing is not None else "baseline_refusal",
                "admission_disposition": "audit_only",
            })
            for row in artifact_rows:
                row["observation_ids"].append(observation_id)

    acquired_claims = {accession for copy in copy_observations
                       for accession in copy["accession_claims"]}
    physical_by_id = {row["physical_input_id"]: row for row in physical}
    for index in index_observations:
        entry = index["entry"]
        if (entry is None or entry["form_type"] not in NCEN_FORMS
                or entry["accession_number"] in acquired_claims):
            continue
        witness = physical_by_id[index["physical_input_id"]]
        accession = entry["accession_number"]
        observation_id = _diagnostic_id("copy_observation_v2", ["index_only", index["observation_id"]])
        copy_observations.append({
            "schema_version": "ncen_diagnostic_copy_observation_v2",
            "observation_id": observation_id, "artifact_id": index["artifact_id"],
            "source_kind": "edgar_index", "raw_sha256": witness["raw_unit_sha256"],
            "physical_input_ids": [witness["physical_input_id"]],
            "accession_claims": [accession], "cik_claims": [entry["cik"]],
            "header_observation_ids": sorted(header["observation_id"] for header in header_observations
                                             if header["accession_claim"] == accession),
            "acquisition_request_locators": [], "f6_exclusion_locators": [],
            "f6_boundary_locators": [], "parse_state": "index_only", "parser_reasons": [],
            "filing": None, "merge_input_kind": "baseline_refusal",
            "admission_disposition": "audit_only",
        })
        witness["observation_ids"].append(observation_id)

    scope_pin = roles["acquisition_scope"][0]
    sums_pin = roles["acquisition_sha256sums"][0]
    scope = _purpose_raw_control(_purpose_raw_file(
        roots, scope_pin, active, active.limits.ledger_max_bytes,
    ), acquisition=True)
    if not isinstance(scope, dict) or not isinstance(scope.get("requests"), list):
        raise NcenError("diagnostic_pinned_input_invalid")
    if scope.get("schema") == "bond_ncen_amendment_acquisition_scope_v1":
        if "acquisition_ledger" not in roles:
            raise NcenError("diagnostic_pinned_input_invalid")
        derived = read_diagnostic_acquisition_ledger(DiagnosticAcquisitionPin(
            roots[scope_pin["root_id"]], scope_pin["sha256"], sums_pin["sha256"], "synthetic_fixture",
        ))
        if (acquisition_ledger is not None and acquisition_ledger != derived
                or _purpose_trust_bytes(roots, roles["acquisition_ledger"][0],
                                        code="diagnostic_declared_ledger_mismatch")
                != _diagnostic_canonical(derived.digest_object())):
            raise NcenError("diagnostic_declared_ledger_mismatch")
        acquisition_ledger = derived
    elif acquisition_ledger is not None:
        raise NcenError("diagnostic_pinned_input_invalid")
    exclusions = [] if acquisition_ledger is None else [item.payload() for item in acquisition_ledger.exclusions]
    boundaries = [] if acquisition_ledger is None else [item.payload() for item in acquisition_ledger.boundaries]
    for copy in copy_observations:
        copy["f6_exclusion_locators"] = sorted({f"/exclusions/{index}" for index, row in enumerate(exclusions)
                                                if row["accession_number"] in copy["accession_claims"]})
        copy["f6_boundary_locators"] = sorted({f"/boundaries/{index}" for index, row in enumerate(boundaries)
                                               if row["accession_number"] in copy["accession_claims"]})
    for row in physical:
        row["observation_ids"] = sorted(set(row["observation_ids"]))
    physical_order = {row["physical_input_id"]: position for position, row in enumerate(physical)}
    index_observations.sort(key=lambda row: (row["artifact_id"], physical_order[row["physical_input_id"]]))
    copy_observations.sort(key=lambda row: (row["artifact_id"], row["observation_id"]))
    header_observations.sort(key=lambda row: (row["artifact_id"], row["observation_id"]))
    scope_rows = [row for row in physical if row["artifact_id"] == "acquisition:scope.json"
                  and row["locator"].startswith("/requests/")]
    requests = []
    exclusions_by_accession = {row["accession_number"] for row in exclusions}
    boundaries_by_accession = {row["accession_number"] for row in boundaries}
    for index, request in enumerate(scope["requests"]):
        locator = f"/requests/{index}"
        row = next((item for item in scope_rows if item["locator"] == locator), None)
        if row is None or not isinstance(request, dict):
            raise NcenError("diagnostic_pinned_input_invalid")
        accession = request.get("accession_number")
        if not isinstance(accession, str) or _ACCESSION.fullmatch(accession) is None:
            disposition, reason = "baseline_refusal", "acquisition_request_accession_number_missing"
        elif acquisition_ledger is None:
            disposition, reason = "baseline_refusal", "f6_ledger_unavailable"
        elif accession in exclusions_by_accession:
            disposition, reason = "excluded", None
        elif accession in boundaries_by_accession:
            disposition, reason = "boundary", None
        else:
            disposition, reason = "verified", None
        requests.append({"physical_input_id": row["physical_input_id"], "locator": locator,
                         "accession_number": accession if isinstance(accession, str) else None,
                         "disposition": disposition, "refusal_reason": reason})
    request_by_id = {item["physical_input_id"]: item for item in requests}
    copies_by_id = {copy["observation_id"]: copy for copy in copy_observations}

    def f6_links(row: Mapping[str, Any], records: list[dict[str, Any]], role: str) -> list[str]:
        paths = []
        if row["artifact_id"].startswith("acquisition:"):
            paths.append(row["artifact_id"].removeprefix("acquisition:"))
        related = [copies_by_id[observation_id] for observation_id in row["observation_ids"]
                   if observation_id in copies_by_id]
        request = request_by_id.get(row["physical_input_id"])
        accessions = {claim for copy in related for claim in copy["accession_claims"]}
        if request is not None and request["accession_number"] is not None:
            accessions.add(request["accession_number"])
        return sorted({f"/{role}/{index}" for index, record in enumerate(records)
                       if record["accession_number"] in accessions
                       or any(ref["path"] in paths for ref in (
                           record["terminal"], record["header_raw"], record["header_record"],
                           record["raw_xml"],
                       ))})

    f1_verified = verify_f1_subset and "ncen_source_manifest" in roles
    f1_artifacts = (_purpose_q2_f1_subset(roots, roles, artifacts, source_manifest, physical, active)
                    if f1_verified else {item["artifact_id"] for item in source_manifest["artifacts"]})
    for copy in copy_observations:
        if f1_verified and copy["artifact_id"] in f1_artifacts and copy["parse_state"] == "parsed":
            copy["admission_disposition"] = "f1_admitted"
    reconciliation = {
        "schema_version": "ncen_diagnostic_source_reconciliation_v2",
        "physical_inputs": [
            {**{key: row[key] for key in (
                "physical_input_id", "disposition", "reason_codes", "observation_ids", "ledger_locators",
            )}, "f6_exclusion_locators": f6_links(row, exclusions, "exclusions"),
             "f6_boundary_locators": f6_links(row, boundaries, "boundaries"),
             "f1_subset_claim": ("f1_admitted" if f1_verified else "claimed_unverified_q2")
             if row["artifact_id"] in f1_artifacts
             else "not_claimed"}
            for row in physical
        ],
        "index_observation_ids": [row["observation_id"] for row in index_observations],
        "copy_observation_ids": [row["observation_id"] for row in copy_observations],
        "header_observation_ids": [row["observation_id"] for row in header_observations],
        "parser_quarantine": ([{"physical_input_id": row["physical_input_id"],
                                "reason_codes": row["reason_codes"]}
                               for row in physical if row["disposition"] == "parser_quarantine"]
                              + [{"observation_id": row["observation_id"],
                                  "reason_codes": row["parser_reasons"]}
                                 for row in copy_observations if row["parse_state"] in {
                                     "quarantined_partial", "unparseable",
                                 }]),
        "f6_exclusions": exclusions, "f6_boundaries": boundaries,
        "acquisition_requests": requests,
        "f1_subset_claim": {"artifact_ids": sorted(f1_artifacts),
                             "verification": "verified_q2a" if f1_verified else "unverified_q2"},
    }
    return {"raw_input_inventory": physical, "index_observations": index_observations,
            "copy_observations": copy_observations, "header_observations": header_observations,
            "reconciliation": reconciliation}


def _purpose_q2_candidates(
    roots: Mapping[str, Path], roles: Mapping[str, tuple[dict[str, Any], ...]],
    source_manifest: Mapping[str, Any], *, cohort_ciks: set[str],
    monitor: DiagnosticResourceMonitor | None = None,
) -> dict[str, Any]:
    """Construct a Q2b snapshot from actual parser carriers; it is not an admitted baseline."""
    active = DiagnosticResourceMonitor() if monitor is None else monitor
    parsed: dict[str, NcenFiling] = {}
    parsed_headers: dict[str, AcceptanceHeader] = {}
    parsed_index: dict[str, FormIndexEntry] = {}
    audit = _purpose_q1_observations(
        roots, roles, source_manifest, monitor=active,
        parser_results=parsed, parsed_headers=parsed_headers, parsed_index=parsed_index,
    )
    entries: list[FormIndexEntry] = []
    index_by_accession: dict[str, list[FormIndexEntry]] = defaultdict(list)
    retrievals: dict[str, set[dt.datetime]] = defaultdict(set)
    for row in audit["index_observations"]:
        value = row["entry"]
        if value is None:
            if row["parse_disposition"] == "parser_quarantine":
                # The broken physical line may be an otherwise relevant N-CEN.
                raise NcenError("diagnostic_candidate_identity_conflict")
            continue
        entry = parsed_index.get(row["observation_id"])
        if entry is None or value != {
            "form_type": entry.form_type, "company_name": entry.company_name,
            "cik": entry.cik, "date_filed": entry.date_filed.isoformat(),
            "file_name": entry.file_name, "accession_number": entry.accession_number,
        }:
            raise NcenError("diagnostic_candidate_identity_conflict")
        entries.append(entry)
        index_by_accession[entry.accession_number].append(entry)
        retrieval = _purpose_parse_timestamp(row["index_retrieved_at"], code="diagnostic_pinned_input_invalid")
        retrievals[entry.accession_number].add(retrieval)

    headers: dict[str, AcceptanceHeader] = {}
    for row in audit["header_observations"]:
        header = parsed_headers.get(row["observation_id"])
        if header is None:
            continue
        if row["header"] != header.to_record():
            raise NcenError("diagnostic_header_conflict")
        if row["representative_observation_id"] == row["observation_id"]:
            headers[header.accession_number] = header

    filings: list[NcenFiling] = []
    results_by_accession: dict[str, list[NcenFiling]] = defaultdict(list)
    for row in audit["copy_observations"]:
        filing = parsed.get(row["observation_id"])
        if filing is None:
            if row["filing"] is not None or row["parse_state"] not in {"index_only", "unparseable"}:
                raise NcenError("diagnostic_quarantine_disposition_mismatch")
            continue
        if (row["filing"] != _purpose_baseline_filing(filing)
                or row["parse_state"] != ("parsed" if filing.usable else "quarantined_partial")):
            raise NcenError("diagnostic_quarantine_disposition_mismatch")
        filings.append(filing)
        results_by_accession[filing.accession_number].append(filing)
        if row["accession_claims"] != [filing.accession_number] or (
            filing.registrant_cik is not None and any(
                claim != filing.registrant_cik for claim in row["cik_claims"]
            )
        ):
            raise NcenError("diagnostic_candidate_identity_conflict")
        if filing.registrant_cik is None and row["cik_claims"]:
            index_ciks = {entry.cik for entry in index_by_accession.get(filing.accession_number, ())}
            if not set(row["cik_claims"]) <= index_ciks:
                raise NcenError("diagnostic_candidate_identity_conflict")

    # The frozen merger has just one index possession time for placeholders.
    placeholder_times = set().union(*(
        retrievals[accession] for accession, candidates in index_by_accession.items()
        if accession not in results_by_accession and any(e.form_type in NCEN_FORMS for e in candidates)
    )) if index_by_accession else set()
    if len(placeholder_times) > 1:
        raise NcenError("diagnostic_index_possession_unrepresentable")
    merged = merge_filings(
        filings, index_entries=entries, headers=headers,
        index_retrieved_at=next(iter(placeholder_times)) if placeholder_times else None,
    )
    by_accession: dict[str, dict[str, NcenFiling]] = defaultdict(dict)
    for cik, candidates in merged.by_cik.items():
        for filing in candidates:
            by_accession[filing.accession_number][cik] = filing
    coverage: dict[str, dict[str, str | None]] = {}

    def classify(observation_id: str, accession: str, ciks: set[str], *,
                 placeholder: bool = False) -> None:
        candidates = by_accession.get(accession, {})
        if accession in merged.excluded:
            reason = merged.excluded[accession]
            if reason == "cik_unknown" and (not ciks or ciks & cohort_ciks):
                raise NcenError("diagnostic_candidate_identity_conflict")
            outcome = {"disposition": "frozen_exclusion", "accession": accession, "reason": reason}
        elif candidates:
            if (ciks and not ciks <= candidates.keys()
                    and (ciks | set(candidates)) & cohort_ciks):
                raise NcenError("diagnostic_candidate_identity_conflict")
            if placeholder and not any(f.is_placeholder for f in candidates.values()):
                raise NcenError("diagnostic_candidate_blocker_unrepresentable")
            disposition = "frozen_index_placeholder" if placeholder else "merged"
            outcome = {"disposition": disposition, "accession": accession, "reason": None}
        elif ciks and not ciks & cohort_ciks:
            outcome = {"disposition": "audit_out_of_scope", "accession": accession,
                       "reason": "registrant_not_in_complete_cohort"}
        else:
            raise NcenError("diagnostic_candidate_identity_conflict")
        coverage[observation_id] = outcome

    for row in audit["index_observations"]:
        entry = row["entry"]
        if entry is None:
            if row["parse_disposition"] == "structural_metadata":
                coverage[row["observation_id"]] = {
                    "disposition": "audit_out_of_scope", "accession": None,
                    "reason": "index_structural_metadata",
                }
            continue
        if entry["form_type"] not in NCEN_FORMS and entry["accession_number"] not in by_accession:
            coverage[row["observation_id"]] = {
                "disposition": "frozen_exclusion", "accession": entry["accession_number"],
                "reason": f"form_not_ncen:{entry['form_type']}",
            }
        else:
            classify(row["observation_id"], entry["accession_number"], {entry["cik"]},
                     placeholder=entry["accession_number"] not in results_by_accession)
    for row in audit["copy_observations"]:
        accessions = row["accession_claims"]
        ciks = set(row["cik_claims"])
        if len(accessions) != 1:
            if ciks and not ciks & cohort_ciks:
                coverage[row["observation_id"]] = {
                    "disposition": "audit_out_of_scope", "accession": None,
                    "reason": "registrant_not_in_complete_cohort",
                }
                continue
            raise NcenError("diagnostic_candidate_identity_conflict")
        accession = accessions[0]
        if row["parse_state"] == "unparseable" and results_by_accession.get(accession):
            raise NcenError("diagnostic_candidate_blocker_unrepresentable")
        if row["parse_state"] in {"unparseable", "index_only"}:
            bound = index_by_accession.get(accession, [])
            if not bound and (not ciks or ciks & cohort_ciks):
                raise NcenError("diagnostic_candidate_blocker_unrepresentable")
            if bound and ciks and not ciks <= {entry.cik for entry in bound}:
                raise NcenError("diagnostic_candidate_identity_conflict")
            ciks = ciks or {entry.cik for entry in bound}
        classify(row["observation_id"], accession, ciks,
                 placeholder=row["parse_state"] in {"unparseable", "index_only"})
        if coverage[row["observation_id"]]["disposition"] == "frozen_index_placeholder":
            row["merge_input_kind"] = "frozen_index_placeholder"
    for row in audit["header_observations"]:
        accession = row["accession_claim"]
        if accession is None or row["header"] is None:
            raise NcenError("diagnostic_header_binding_missing")
        if (accession not in index_by_accession and accession not in results_by_accession
                and accession not in merged.excluded):
            raise NcenError("diagnostic_header_binding_missing")
        classify(row["observation_id"], accession, set(row["header"]["filer_ciks"]))

    for family in ("f6_exclusions", "f6_boundaries"):
        for number, row in enumerate(audit["reconciliation"][family]):
            accession = row["accession_number"]
            classify(f"{family}:{number}", accession, {row["registrant_cik"]})
    scope = _purpose_raw_control(_purpose_raw_file(
        roots, roles["acquisition_scope"][0], active, active.limits.ledger_max_bytes,
    ), acquisition=True)
    for number, row in enumerate(audit["reconciliation"]["acquisition_requests"]):
        accession = row["accession_number"]
        if accession is None:
            raise NcenError("diagnostic_candidate_identity_conflict")
        request = scope["requests"][number]
        claimed_cik = request.get("registrant_cik") if isinstance(request, dict) else None
        if not isinstance(claimed_cik, str) or normalize_cik(claimed_cik) != claimed_cik:
            raise NcenError("diagnostic_candidate_identity_conflict")
        if accession in by_accession or accession in merged.excluded:
            classify(f"acquisition_requests:{number}", accession, {claimed_cik})
        elif claimed_cik not in cohort_ciks:
            coverage[f"acquisition_requests:{number}"] = {
                "disposition": "audit_out_of_scope", "accession": accession,
                "reason": "registrant_not_in_complete_cohort",
            }
        else:
            raise NcenError("diagnostic_candidate_blocker_unrepresentable")
    raw_manifest = _purpose_raw_control(_purpose_raw_file(
        roots, roles["ncen_raw_audit_manifest"][0], active, active.limits.manifest_max_bytes,
    ))
    dera_owners: dict[tuple[str, str], str] = {}
    for item in raw_manifest["artifacts"]:
        if item["kind"] != "dera_zip":
            continue
        data = _purpose_raw_file(roots, {key: item[key] for key in (
            "root_id", "path", "sha256", "bytes",
        )}, active, active.limits.spill_budget_bytes)
        fund_accessions: dict[str, str] = {}
        def rows_for(table: str, archive: zipfile.ZipFile,
                     members: Mapping[str, zipfile.ZipInfo],
                     reader: DiagnosticResourceMonitor) -> Iterator[tuple[str, int, dict[str, str]]]:
            info = _member_for(members, table)
            if info is None:
                return
            with archive.open(info) as stream:
                first, oversized = _diagnostic_read_bounded_tsv_line(
                    stream, max_bytes=reader.limits.tsv_line_max_bytes, monitor=reader,
                )
                if first is None or oversized:
                    raise NcenError("diagnostic_candidate_blocker_unrepresentable")
                header = first.decode("utf-8").rstrip("\r\n").split("\t")
                number = 1
                while True:
                    line, oversized = _diagnostic_read_bounded_tsv_line(
                        stream, max_bytes=reader.limits.tsv_line_max_bytes, monitor=reader,
                    )
                    if line is None:
                        break
                    number += 1
                    if oversized:
                        raise NcenError(f"diagnostic_member_row_oversized:{table}:{number}")
                    fields = line.decode("utf-8").rstrip("\r\n").split("\t")
                    if len(fields) == len(header) and len(set(header)) == len(header):
                        yield info.filename, number, dict(zip(header, fields, strict=True))

        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = inspect_zip(archive, NCEN_ZIP_LIMITS)
            for _member, _number, fields in rows_for("FUND_REPORTED_INFO", archive, members, active):
                fund_id, accession = fields.get("FUND_ID"), fields.get("ACCESSION_NUMBER")
                if fund_id and accession and _ACCESSION.fullmatch(accession):
                    if fund_id in fund_accessions and fund_accessions[fund_id] != accession:
                        raise NcenError("diagnostic_candidate_identity_conflict")
                    fund_accessions[fund_id] = accession
            for table in PINNED_TABLES:
                for member, number, fields in rows_for(table, archive, members, active):
                    accession = fields.get("ACCESSION_NUMBER") or fund_accessions.get(fields.get("FUND_ID", ""))
                    if accession is not None and _ACCESSION.fullmatch(accession):
                        dera_owners[item["artifact_id"], f"{member}:{number}"] = accession

    physical_claims = {row["physical_input_id"]: row
                       for row in audit["reconciliation"]["physical_inputs"]}
    request_by_accession = {row["accession_number"]: number for number, row in enumerate(
        audit["reconciliation"]["acquisition_requests"])}
    if len(request_by_accession) != len(audit["reconciliation"]["acquisition_requests"]):
        raise NcenError("diagnostic_candidate_identity_conflict")
    schema_evidence: list[dict[str, Any]] | None = None
    for row in audit["raw_input_inventory"]:
        linked = [coverage[observation_id] for observation_id in row["observation_ids"]
                  if observation_id in coverage]
        if row["observation_ids"] and len(linked) != len(row["observation_ids"]):
            raise NcenError("diagnostic_raw_membership_mismatch")
        if (row["disposition"] in {"structural_metadata", "out_of_scope_content"} and (
            row["unit_kind"].startswith("zip_") or (
                row["unit_kind"] == "tsv_line" and (
                    row["locator"].endswith(":1") or row["disposition"] == "out_of_scope_content"
                )
            )
        )):
            linked = []
        owner = dera_owners.get((row["artifact_id"], row["locator"]))
        if row["unit_kind"] == "tsv_line" and row["locator_kind"] == "zip_tsv_line":
            number = int(row["locator"].rsplit(":", 1)[1])
            if number > 1 and row["disposition"] != "out_of_scope_content":
                if owner is None:
                    raise NcenError("diagnostic_candidate_blocker_unrepresentable")
                linked = [value for value in linked if value["accession"] == owner]
                if not linked:
                    raise NcenError("diagnostic_candidate_blocker_unrepresentable")
        if row["disposition"] == "parser_quarantine" and not linked:
            raise NcenError("diagnostic_candidate_blocker_unrepresentable")
        choices = {(value["disposition"], value["accession"], value["reason"]) for value in linked}
        if len(choices) > 1:
            raise NcenError("diagnostic_candidate_blocker_unrepresentable")
        if choices:
            coverage[row["physical_input_id"]] = linked[0]
            continue
        claim = physical_claims[row["physical_input_id"]]
        f6 = [coverage[f"{family}:{int(locator.rsplit('/', 1)[1])}"]
              for family, key in (("f6_exclusions", "f6_exclusion_locators"),
                                  ("f6_boundaries", "f6_boundary_locators"))
              for locator in claim[key]]
        request = next((coverage[f"acquisition_requests:{number}"] for number, value in enumerate(
            audit["reconciliation"]["acquisition_requests"])
            if value["physical_input_id"] == row["physical_input_id"]), None)
        choices = {(value["disposition"], value["accession"], value["reason"]) for value in f6}
        if len(choices) > 1:
            raise NcenError("diagnostic_candidate_blocker_unrepresentable")
        if request is not None or f6:
            coverage[row["physical_input_id"]] = request or f6[0]
            continue
        if row["artifact_id"].startswith("acquisition:"):
            path = row["artifact_id"].removeprefix("acquisition:")
            accession_match = _ACCESSION.search(path)
            accession = accession_match.group() if accession_match is not None else None
            if path == "schema_evidence.jsonl":
                if schema_evidence is None:
                    sums = _purpose_raw_file(roots, roles["acquisition_sha256sums"][0],
                                             active, active.limits.ledger_max_bytes)
                    inventory = _diagnostic_acquisition_inventory(sums)
                    pin = {"root_id": roles["acquisition_scope"][0]["root_id"],
                           "path": path, "sha256": inventory[path],
                           "bytes": _purpose_trusted_file(roots, {
                               "root_id": roles["acquisition_scope"][0]["root_id"], "path": path,
                           }, code="diagnostic_input_unavailable").stat().st_size}
                    data = _purpose_raw_file(roots, pin, active, active.limits.ledger_max_bytes)
                    schema_evidence = [_purpose_raw_control(line, acquisition=True)
                                       for line in data.splitlines(keepends=True)]
                number = int(row["locator"].rsplit(":", 1)[1])
                if number < 1 or number > len(schema_evidence) or not isinstance(
                    schema_evidence[number - 1], dict
                ):
                    raise NcenError("diagnostic_pinned_input_invalid")
                accession = schema_evidence[number - 1].get("accession_number")
            if accession is not None:
                if accession not in request_by_accession:
                    raise NcenError("diagnostic_candidate_identity_conflict")
                coverage[row["physical_input_id"]] = coverage[
                    f"acquisition_requests:{request_by_accession[accession]}"]
                continue
            if path not in {"SHA256SUMS", "SCOPE.sha256", "scope.json", "coverage.json",
                            "SHA256SUMS.receipt.json", "quarantine.json", "boundary_examples.json"}:
                raise NcenError("diagnostic_candidate_blocker_unrepresentable")
            coverage[row["physical_input_id"]] = {
                "disposition": "audit_out_of_scope", "accession": None,
                "reason": "acquisition_control_record",
            }
        elif row["disposition"] in {"structural_metadata", "out_of_scope_content"}:
            coverage[row["physical_input_id"]] = {
                "disposition": "audit_out_of_scope", "accession": None,
                "reason": row["reason_codes"][0] if row["reason_codes"]
                          else f"parser_contract_{row['unit_kind']}",
            }
        else:
            raise NcenError("diagnostic_raw_membership_mismatch")
    return {"audit": audit, "index_entries": tuple(entries), "filings": tuple(filings),
            "headers": headers, "merged": merged, "coverage": coverage,
            "parser_results": parsed}


def _purpose_q1_verify_observations(
    roots: Mapping[str, Path], roles: Mapping[str, tuple[dict[str, Any], ...]],
    source_manifest: Mapping[str, Any], claimed: bytes, *, trusted_run: DiagnosticTrustPin,
    acquisition_ledger: DiagnosticAcquisitionLedger | None = None,
) -> dict[str, Any]:
    """Compare a claimed Q1 observation against freshly enumerated pinned inputs."""
    if not isinstance(trusted_run, DiagnosticTrustPin) or not trusted_run.manifest_path.is_absolute():
        raise NcenError("diagnostic_trust_anchor_missing")
    trust_root = trusted_run.manifest_path.parent
    trust_manifest = _purpose_raw_control(_purpose_trust_bytes({"trust": trust_root}, {
        "root_id": "trust", "path": trusted_run.manifest_path.name,
        "sha256": trusted_run.manifest_sha256, "bytes": trusted_run.manifest_size,
    }, code="diagnostic_trust_anchor_mismatch"))
    if (not isinstance(trust_manifest, dict)
            or set(trust_manifest) != {"schema_version", "lane", "roles", "logical_ledger"}
            or trust_manifest["schema_version"] != "ncen_diagnostic_trust_manifest_v3"
            or trust_manifest["lane"] != "synthetic_fixture"
            or _purpose_trust_roles(trust_manifest["roles"]) != roles):
        raise NcenError("diagnostic_declared_pin_mismatch")
    actual = _purpose_q1_observations(roots, roles, source_manifest,
                                      acquisition_ledger=acquisition_ledger)
    candidate = _purpose_raw_control(claimed)
    if not isinstance(candidate, dict) or set(candidate) != set(actual):
        raise NcenError("diagnostic_pinned_input_invalid")
    rows = candidate["raw_input_inventory"]
    if not isinstance(rows, list):
        raise NcenError("diagnostic_pinned_input_invalid")
    identities: set[tuple[Any, ...]] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != set(actual["raw_input_inventory"][0]):
            raise NcenError("diagnostic_pinned_input_invalid")
        identity = tuple(row[name] for name in (
            "root_id", "artifact_id", "container_member", "locator_kind", "locator",
        ))
        if not all(isinstance(value, str) for value in identity[:2] + identity[3:]) or (
            identity[2] is not None and not isinstance(identity[2], str)
        ):
            raise NcenError("diagnostic_pinned_input_invalid")
        if identity in identities:
            raise NcenError("diagnostic_raw_physical_duplicate")
        identities.add(identity)
    if len(rows) != len(actual["raw_input_inventory"]):
        raise NcenError("diagnostic_raw_membership_mismatch")
    for family in ("index_observations", "copy_observations", "header_observations"):
        if not isinstance(candidate[family], list) or len(candidate[family]) != len(actual[family]):
            raise NcenError("diagnostic_raw_membership_mismatch")
        if any(not isinstance(row, dict) or set(row) != set(expected)
               for row, expected in zip(candidate[family], actual[family], strict=True)):
            raise NcenError("diagnostic_pinned_input_invalid")
    if not isinstance(candidate["reconciliation"], dict):
        raise NcenError("diagnostic_pinned_input_invalid")
    if set(candidate["reconciliation"]) != set(actual["reconciliation"]):
        raise NcenError("diagnostic_pinned_input_invalid")
    for key in ("index_observation_ids", "copy_observation_ids", "header_observation_ids",
                "f6_exclusions", "f6_boundaries", "acquisition_requests", "f1_subset_claim"):
        if candidate["reconciliation"].get(key) != actual["reconciliation"][key]:
            raise NcenError("diagnostic_raw_membership_mismatch")
    if any(candidate["reconciliation"].get(key) != actual["reconciliation"][key]
           for key in ("physical_inputs", "parser_quarantine")):
        raise NcenError("diagnostic_quarantine_disposition_mismatch")
    if any(row.get("physical_input_id") != expected["physical_input_id"]
           or row.get("observation_ids") != expected["observation_ids"]
           for row, expected in zip(rows, actual["raw_input_inventory"], strict=True)):
        raise NcenError("diagnostic_raw_membership_mismatch")
    if _purpose_json_bytes(candidate) != _purpose_json_bytes(actual):
        raise NcenError("diagnostic_quarantine_disposition_mismatch")
    return actual


def _purpose_read_baseline_checkpoint(
    roots: Mapping[str, Path], roles: Mapping[str, tuple[dict[str, Any], ...]],
    declaration: Mapping[str, Any], baseline: Mapping[str, Any],
    manifest: Mapping[str, Any], sources: DiagnosticSourceIndex,
    membership: DiagnosticSyntheticMembershipCheckpoint,
    *, monitor: DiagnosticResourceMonitor | None = None,
) -> DiagnosticBaselineCheckpoint:
    """Q3 owns checkpoint admission; Q2b only verifies candidate snapshots."""
    raise NcenError("C1_INCOMPLETE")
    from .sec_acquisition import (
        SecHeaderError,
        SecIndexError,
        index_date_public_available_at,
        parse_acceptance_header,
        parse_form_index,
    )

    monitor = DiagnosticResourceMonitor() if monitor is None else monitor
    descriptors = baseline.get("files")
    if (not isinstance(descriptors, list) or len(descriptors) != len(_PURPOSE_BASELINE_FILES)
            or any(not isinstance(item, dict) for item in descriptors)):
        raise NcenError("diagnostic_checkpoint_pin_mismatch")
    file_pins = [roles[role][0] if role is not None else {
        "root_id": roles["diagnostic_baseline_checkpoint"][0]["root_id"],
        "path": name, "sha256": descriptor.get("sha256"), "bytes": descriptor.get("bytes"),
    } for (name, role), descriptor in zip(_PURPOSE_BASELINE_FILES, descriptors, strict=True)]
    if (any(not isinstance(row, dict) or set(row) != {"path", "sha256", "bytes", "rows"}
                   or row["path"] != name or row["sha256"] != pin["sha256"]
                   or row["bytes"] != pin["bytes"] or type(row["rows"]) is not int
                   or row["rows"] < 0 or (not name.endswith("jsonl") and row["rows"] != 1)
                   for row, ((name, _), pin) in zip(descriptors, zip(_PURPOSE_BASELINE_FILES, file_pins), strict=True))):
        raise NcenError("diagnostic_checkpoint_pin_mismatch")
    for pin in file_pins:
        try:
            _purpose_trust_descriptor(pin)
        except NcenError as exc:
            raise NcenError("diagnostic_checkpoint_pin_mismatch") from exc
    expected_versions = {
        "selector": RULE_VERSION, "merge": RULE_VERSION,
        "amendment": AMENDMENT_SEMANTICS_VERSION, "cohort": DIAGNOSTIC_COHORT_VERSION,
        "source": DIAGNOSTIC_SOURCE_ATTESTATION_VERSION,
        "admission": DIAGNOSTIC_ADMISSION_VERSION,
        "normalizer": DIAGNOSTIC_NAME_NORMALIZER_VERSION,
        "edge": DIAGNOSTIC_EDGE_VERSION, "fold": DIAGNOSTIC_FOLD_PROTOCOL_VERSION,
    }
    if (set(baseline) != {"schema_version", "lane", "purpose", "diagnostic_only", "contexts",
                          "versions", "inventory_digest", "cohort_digest", "ncen_evidence_digest",
                          "exclusion_ledger_digest", "files", "source_manifest_pin", "acquisition_pin"}
            or baseline["schema_version"] != "ncen_diagnostic_baseline_checkpoint_v2"
            or baseline["lane"] != "synthetic_fixture"
            or baseline["purpose"] != "ncen_purpose_diagnostic_selection_replay"
            or baseline["diagnostic_only"] is not True
            or baseline["versions"] != expected_versions
            or baseline["contexts"] != declaration.get("contexts")
            or any(baseline[key] != declaration.get(key) for key in (
                "inventory_digest", "cohort_digest", "ncen_evidence_digest", "exclusion_ledger_digest"))
            or baseline["ncen_evidence_digest"] != sources.evidence_digest
            or baseline["source_manifest_pin"] != roles["ncen_source_manifest"][0]
            or baseline["acquisition_pin"] != {
                "lane": "synthetic_fixture", "scope": roles["acquisition_scope"][0],
                "sha256sums": roles["acquisition_sha256sums"][0],
            }):
        raise NcenError("diagnostic_checkpoint_pin_mismatch")
    read = {name: _purpose_trust_bytes(roots, pin, code="diagnostic_checkpoint_pin_mismatch", monitor=monitor)
            for (name, _), pin in zip(_PURPOSE_BASELINE_FILES, file_pins, strict=True)}
    for descriptor in descriptors:
        if descriptor["rows"] != (len(read[descriptor["path"]].splitlines())
                                  if descriptor["path"].endswith("jsonl") else 1):
            raise NcenError("diagnostic_checkpoint_pin_mismatch")
    raw_rows = read["raw_input_inventory.jsonl"].splitlines(keepends=True)
    identities: set[tuple[Any, ...]] = set()
    for line in raw_rows:
        row = _purpose_json_load(line, code="diagnostic_raw_membership_mismatch")
        if not isinstance(row, dict) or set(row) != {
                "schema_version", "physical_input_id", "root_id", "artifact_id", "container_member",
                "locator_kind", "locator", "raw_unit_sha256", "unit_kind", "disposition", "reason_codes",
                "observation_ids", "ledger_locators",
            } or line != _diagnostic_canonical(row) + b"\n":
            raise NcenError("diagnostic_raw_membership_mismatch")
        if (not all(isinstance(row[name], str) for name in (
                "root_id", "artifact_id", "locator_kind", "locator", "physical_input_id"))
                or not isinstance(row["container_member"], (str, type(None)))):
            raise NcenError("diagnostic_pinned_input_invalid")
        identity = (row["root_id"], row["artifact_id"], row["container_member"],
                    row["locator_kind"], row["locator"])
        if identity in identities:
            raise NcenError("diagnostic_raw_physical_duplicate")
        identities.add(identity)
    _purpose_baseline_jsonl(read["raw_input_inventory.jsonl"], list(_purpose_enumerate_raw_inputs(
        roots, roles, manifest, monitor=monitor,
    )), code="diagnostic_raw_membership_mismatch")
    expected_cohort = sorted(
        (_purpose_cohort_record(item, declaration["inventory_digest"]) for item in membership.members),
        key=lambda row: row["record_id"],
    )
    _purpose_baseline_jsonl(read["cohort.jsonl"], expected_cohort,
                            code="diagnostic_cohort_baseline_mismatch")
    receipt = _purpose_json_load(read["cohort_derivation.json"], code="diagnostic_checkpoint_pin_mismatch")
    if read["cohort_derivation.json"] != _diagnostic_canonical(receipt) + b"\n":
        raise NcenError("diagnostic_checkpoint_pin_mismatch")
    if (receipt.get("cohort_sha256") != file_pins[0]["sha256"]
            or receipt.get("cohort_digest") != baseline["cohort_digest"]
            or receipt.get("membership_definition_sha256") != roles["synthetic_membership_definition"][0]["sha256"]):
        raise NcenError("diagnostic_cohort_baseline_mismatch")

    index_records: list[dict[str, Any]] = []
    entries: list[FormIndexEntry] = []
    index_manifest = _purpose_json_load(_purpose_trust_bytes(
        roots, roles["ncen_index_manifest"][0], code="diagnostic_checkpoint_pin_mismatch", monitor=monitor),
        code="diagnostic_checkpoint_pin_mismatch")
    for index_artifact in index_manifest["artifacts"]:
        pin = index_artifact["pin"]
        raw = _purpose_trust_bytes(roots, pin, code="diagnostic_checkpoint_pin_mismatch", monitor=monitor)
        try:
            parsed = parse_form_index(raw)
        except (SecIndexError, UnicodeError, ValueError) as exc:
            raise NcenError("diagnostic_candidate_universe_mismatch") from exc
        text = raw.decode("latin-1").splitlines()
        locations = [number for number, line in enumerate(text, start=1)
                     if number > next((i for i, value in enumerate(text, start=1)
                                       if re.fullmatch(r"-{20,}\s*", value)), len(text)) and line.strip()]
        if len(locations) != len(parsed):
            raise NcenError("diagnostic_candidate_universe_mismatch")
        artifact_id = f"index:{pin['root_id']}:{pin['path']}"
        # The artifact identity is its external role descriptor, not a value from the checkpoint.
        for location, entry in zip(locations, parsed, strict=True):
            entries.append(entry)
            index_records.append({
                "form_type": entry.form_type, "company_name": entry.company_name,
                "cik": entry.cik, "date_filed": entry.date_filed.isoformat(),
                "file_name": entry.file_name, "accession_number": entry.accession_number,
                "index_artifact_id": artifact_id,
                "index_record_locator": f"{pin['path']}:{location}",
                "index_retrieved_at": index_artifact["retrieved_at"],
            })
    _purpose_baseline_jsonl(read["ncen_index_entries.jsonl"], index_records,
                            code="diagnostic_candidate_universe_mismatch")
    if len({(row["index_artifact_id"], row["index_record_locator"]) for row in index_records}) != len(index_records):
        raise NcenError("diagnostic_candidate_universe_mismatch")
    if len(set(entries)) != len(entries):
        raise NcenError("diagnostic_candidate_universe_mismatch")

    cutoff = _purpose_parse_timestamp(baseline["contexts"][0]["K"], code="diagnostic_baseline_K")
    headers: dict[str, AcceptanceHeader] = {}
    header_records = []
    for artifact in manifest["artifacts"]:
        if artifact["kind"] != "header":
            continue
        pin = {"root_id": roles["ncen_source_manifest"][0]["root_id"],
               **{key: artifact[key] for key in ("path", "sha256", "bytes")}}
        raw = _purpose_trust_bytes(roots, pin, code="diagnostic_source_checkpoint_mismatch", monitor=monitor)
        try:
            header = parse_acceptance_header(
                raw, accession_number=artifact["accession_number"], url=artifact["source_url"],
                document_sha256=artifact["sha256"],
                retrieved_at=_purpose_parse_timestamp(artifact["retrieved_at"], code="diagnostic_header_time"),
            )
        except (SecHeaderError, ValueError) as exc:
            raise NcenError("diagnostic_source_checkpoint_mismatch") from exc
        if header.accession_number in headers:
            raise NcenError("diagnostic_candidate_universe_mismatch")
        headers[header.accession_number] = header
        header_records.append({"artifact_id": artifact["artifact_id"],
                               "accession_number": header.accession_number,
                               "header_sha256": header.header_sha256,
                               "public_disposition": _purpose_baseline_time_status(
                                   artifact["public_at"], cutoff),
                               "possession_disposition": _purpose_baseline_time_status(
                                   artifact["retrieved_at"], cutoff)})

    copies: list[dict[str, Any]] = []
    filings: list[NcenFiling] = []
    for artifact in manifest["artifacts"]:
        kind = artifact["kind"]
        if kind == "header":
            continue
        pin = {"root_id": roles["ncen_source_manifest"][0]["root_id"],
               **{key: artifact[key] for key in ("path", "sha256", "bytes")}}
        raw = _purpose_trust_bytes(roots, pin, code="diagnostic_source_checkpoint_mismatch", monitor=monitor)
        retrieved = _purpose_parse_timestamp(artifact["retrieved_at"], code="diagnostic_copy_time")
        if kind == "edgar_xml":
            parsed = (parse_ncen_primary_doc(raw, accession_number=artifact["accession_number"],
                                             source_url=artifact["source_url"], retrieved_at=retrieved),)
        elif kind == "dera_zip":
            tables: dict[str, list[dict[str, str | None]]] = {}
            reasons: list[str] = []
            try:
                with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                    # Synthetic-only full-field replay stays inside the bounded trust reader.
                    bound = 16 * 1024 * 1024
                    limits = ZipLimits(max_members=NCEN_ZIP_LIMITS.max_members,
                                       max_member_bytes=bound, max_total_bytes=bound,
                                       max_compression_ratio=NCEN_ZIP_LIMITS.max_compression_ratio,
                                       max_header_bytes=NCEN_ZIP_LIMITS.max_header_bytes)
                    zip_members = inspect_zip(archive, limits)
                    for table in PINNED_TABLES:
                        info = _member_for(zip_members, table)
                        if info is None:
                            raise NcenError("diagnostic_candidate_universe_mismatch")
                        rows = _dera_rows(archive, info, table, reasons)
                        if rows is None:
                            raise NcenError("diagnostic_candidate_universe_mismatch")
                        tables[table] = rows
                if reasons:
                    raise NcenError("diagnostic_candidate_universe_mismatch")
                parsed = _dera_filings(
                    tables, package_label=artifact["package_label"], zip_sha=artifact["sha256"],
                    known_at=_purpose_parse_timestamp(artifact["data_known_at"], code="diagnostic_copy_time"),
                    retrieved_at=retrieved, stats={},
                )
            except (zipfile.BadZipFile, ZipSafetyError, UnicodeError, csv.Error, KeyError) as exc:
                raise NcenError("diagnostic_candidate_universe_mismatch") from exc
        else:
            raise NcenError("diagnostic_candidate_universe_mismatch")
        for filing in parsed:
            if not filing.usable:
                raise NcenError("diagnostic_candidate_universe_mismatch")
            rows = [row for row in sources.rows if row.artifact_id == artifact["artifact_id"]
                    and row.accession_number == filing.accession_number and row.source_kind == kind]
            copy_id = _diagnostic_source_copy_id(kind, artifact["artifact_id"],
                                                 artifact["sha256"], filing.accession_number)
            if any(row.source_copy_id != copy_id or row.projection_digest != filing.projection_digest
                   for row in rows):
                raise NcenError("diagnostic_source_checkpoint_mismatch")
            header_id = artifact.get("header_artifact_id") if kind == "edgar_xml" else next(
                (item["artifact_id"] for item in manifest["artifacts"]
                 if item["kind"] == "header" and item["accession_number"] == filing.accession_number), None)
            header = headers.get(filing.accession_number)
            if header_id is not None and (header is None or next(
                    (item["artifact_id"] for item in manifest["artifacts"]
                     if item["kind"] == "header" and item["accession_number"] == filing.accession_number),
                    None) != header_id):
                raise NcenError("diagnostic_source_checkpoint_mismatch")
            copies.append({"copy_id": copy_id, "artifact_id": artifact["artifact_id"],
                           "source_kind": kind, "raw_sha256": artifact["sha256"],
                           "raw_locator": artifact["path"] if kind == "edgar_xml" else
                           f"{artifact['path']}:{filing.accession_number}",
                           "header_artifact_id": header_id,
                           "header_sha256": None if header_id is None or header is None else header.document_sha256,
                           "filing": _purpose_baseline_filing(filing)})
            filings.append(filing)
    copies.sort(key=lambda item: item["copy_id"])
    if len({item["copy_id"] for item in copies}) != len(copies):
        raise NcenError("diagnostic_candidate_universe_mismatch")
    _purpose_baseline_jsonl(read["ncen_copies.jsonl"], copies,
                            code="diagnostic_source_checkpoint_mismatch")

    ledger = sources.acquisition_ledger()
    exclusion_accessions = {item.accession_number for item in ledger.exclusions}
    boundary_accessions = {item.accession_number for item in ledger.boundaries}
    scope = _purpose_json_load(_purpose_trust_bytes(
        roots, roles["acquisition_scope"][0], code="diagnostic_declared_ledger_mismatch", monitor=monitor),
        code="diagnostic_declared_ledger_mismatch")
    cutoff = _purpose_parse_timestamp(baseline["contexts"][0]["K"], code="diagnostic_baseline_K")
    copy_accessions = {item["filing"]["accession_number"] for item in copies}
    reconciliation = {
        "schema_version": "ncen_diagnostic_source_reconciliation_v1",
        "index_records": [
            {**{key: row[key] for key in ("index_artifact_id", "index_record_locator", "accession_number")},
             "disposition": ("f6_excluded" if row["accession_number"] in exclusion_accessions
                             else "after_K" if index_date_public_available_at(entry) > cutoff
                             else "acquired" if row["accession_number"] in copy_accessions
                             else "missing_content"),
             "retrieval_disposition": ("after_K" if _purpose_parse_timestamp(
                 row["index_retrieved_at"], code="diagnostic_index_time") > cutoff else "by_K"),
             "period_status": "known" if row["accession_number"] in copy_accessions else "unknown"}
            for row, entry in zip(index_records, entries, strict=True)
        ],
        "copies": [
            {**{key: row[key] for key in ("copy_id", "artifact_id", "source_kind", "raw_sha256", "raw_locator")},
             "period_status": "unknown" if row["filing"]["report_period_end"] is None else "known",
             "public_disposition": _purpose_baseline_time_status(
                 next(item for item in manifest["artifacts"] if item["artifact_id"] == row["artifact_id"])["public_at"],
                 cutoff),
             "data_disposition": _purpose_baseline_time_status(
                 next(item for item in manifest["artifacts"] if item["artifact_id"] == row["artifact_id"])["data_known_at"],
                 cutoff),
             "possession_disposition": _purpose_baseline_time_status(row["filing"]["retrieved_at"], cutoff)}
            for row in copies
        ],
        "headers": sorted(header_records, key=lambda row: row["artifact_id"]),
        "parser_quarantine": [],
        "f6_exclusions": [item.payload() for item in ledger.exclusions],
        "f6_boundaries": [item.payload() for item in ledger.boundaries],
        "acquisition_records": [
            {"accession_number": item["accession_number"],
             "disposition": ("excluded" if item["accession_number"] in exclusion_accessions
                             else "boundary" if item["accession_number"] in boundary_accessions
                             else "verified")}
            for item in scope["requests"]
        ],
    }
    claimed = _purpose_json_load(read["source_reconciliation.json"],
                                 code="diagnostic_candidate_universe_mismatch")
    if (claimed != reconciliation
            or read["source_reconciliation.json"] != _diagnostic_canonical(reconciliation) + b"\n"):
        raise NcenError("diagnostic_candidate_universe_mismatch")
    contexts = tuple((_purpose_parse_date(item["R"], code="diagnostic_baseline_R"),
                      _purpose_parse_timestamp(item["K"], code="diagnostic_baseline_K"), item["mode"])
                     for item in baseline["contexts"])
    if (tuple(item[0] for item in contexts) != membership.report_dates
            or len({(item[1], item[2]) for item in contexts}) != 1):
        raise NcenError("diagnostic_context_coverage_mismatch")
    import types

    identities = tuple(sorted(
        (f"{name}:{pin['root_id']}:{pin['path']}", pin["sha256"])
        for name, pins in roles.items() for pin in pins
    )) + tuple((name, pin["sha256"]) for (name, _), pin in
               zip(_PURPOSE_BASELINE_FILES, file_pins, strict=True))
    return DiagnosticBaselineCheckpoint(contexts, membership.members, tuple(entries),
                                        tuple(filings), types.MappingProxyType(headers), sources,
                                        identities, _PURPOSE_BASELINE_SEAL)


def read_diagnostic_baseline_checkpoint(
    trust: DiagnosticTrustPin, *, code_root: Path,
    input_roots: Mapping[str, Path], monitor: DiagnosticResourceMonitor | None = None,
) -> DiagnosticBaselineCheckpoint:
    """Issue one synthetic-only checkpoint after complete externally pinned Q1/Q2 validation."""
    if monitor is not None and not isinstance(monitor, DiagnosticResourceMonitor):
        raise TypeError("monitor_must_be_DiagnosticResourceMonitor")
    if not isinstance(trust, DiagnosticTrustPin):
        raise NcenError("diagnostic_trust_anchor_missing")
    # The declaration is an audit echo. Reconstruct it from the externally pinned
    # files rather than accepting an export-selected cohort or candidate list.
    if not trust.manifest_path.is_absolute():
        raise NcenError("diagnostic_trust_anchor_mismatch")
    roots = {**input_roots, "code": code_root, "trust": trust.manifest_path.parent}
    manifest_bytes = _purpose_trust_bytes(roots, {
        "root_id": "trust", "path": trust.manifest_path.name,
        "sha256": trust.manifest_sha256, "bytes": trust.manifest_size,
    }, code="diagnostic_trust_anchor_mismatch", monitor=monitor)
    manifest = _purpose_json_load(manifest_bytes, code="diagnostic_trust_anchor_mismatch")
    if not isinstance(manifest, dict) or not isinstance(manifest.get("roles"), list):
        raise NcenError("diagnostic_pin_digest_invalid")
    if manifest.get("lane") != "synthetic_fixture":
        raise NcenError("diagnostic_required_pins_unverified")
    roles = _purpose_trust_roles(manifest["roles"])
    checkpoint_raw = _purpose_trust_bytes(
        roots, roles["diagnostic_baseline_checkpoint"][0], code="diagnostic_checkpoint_pin_mismatch",
        monitor=monitor)
    receipt_raw = _purpose_trust_bytes(
        roots, roles["cohort_derivation_receipt"][0], code="diagnostic_checkpoint_pin_mismatch",
        monitor=monitor)
    runtime_raw = _purpose_trust_bytes(
        roots, roles["runtime_code_manifest"][0], code="diagnostic_runtime_code_mismatch",
        monitor=monitor)
    checkpoint = _purpose_control_json(checkpoint_raw, _validate_baseline_control)
    runtime = _purpose_control_json(runtime_raw, _validate_runtime_control)
    receipt = _purpose_control_json(receipt_raw, _validate_cohort_receipt_control)
    excluded = {"runtime_code_manifest", "baseline_v3", "baseline_stage1",
                "baseline_stage2a", "baseline_stage2b"}
    declaration = {
        "pin_roles": manifest["roles"], "lane": manifest["lane"],
        "source_code": [{"path": item["path"], "sha256": item["sha256"]}
                        for item in runtime["files"]],
        "baseline_seals": [{"name": name, "sha256": roles[name][0]["sha256"]}
                           for name in sorted(excluded - {"runtime_code_manifest"})],
        "input_artifacts": sorted(
            ({key: pin[key] for key in ("root_id", "path", "sha256", "bytes")}
             for name, pins in roles.items() if name not in excluded for pin in pins),
            key=lambda item: (item["root_id"], item["path"])),
        "ncen_source_manifest": {"sha256": roles["ncen_source_manifest"][0]["sha256"],
                                 "bytes": roles["ncen_source_manifest"][0]["bytes"],
                                 "kind": "synthetic_fixture",
                                 "evidence_digest": checkpoint["ncen_evidence_digest"]},
        "ncen_evidence_digest": checkpoint["ncen_evidence_digest"],
        "quarantine_seal": {"scope_sha256": roles["acquisition_scope"][0]["sha256"],
                            "sha256sums_sha256": roles["acquisition_sha256sums"][0]["sha256"]},
        "exclusion_ledger_digest": checkpoint["exclusion_ledger_digest"],
        "cohort_provenance": {"inventory_sources": receipt["inventory_sources"]},
        "inventory_digest": checkpoint["inventory_digest"],
        "cohort_digest": checkpoint["cohort_digest"],
        "contexts": checkpoint["contexts"],
    }
    admitted = _purpose_admit_trust(
        trust, root=code_root / ".diagnostic-baseline-reader", code_root=code_root,
        input_roots=input_roots, declaration=declaration, monitor=monitor,
    )
    source = admitted["source_index"]
    custody = _diagnostic_bound_source_custody(source)
    membership = admitted["membership_checkpoint"]
    trust_bytes = _diagnostic_baseline_trust_bytes(trust)
    source_bytes = _diagnostic_baseline_source_bytes(source, custody)
    membership_bytes = _diagnostic_baseline_membership_bytes(
        membership, admitted["checkpoint"]["contexts"], admitted["roles"],
    )
    payload = _diagnostic_baseline_payload(
        admitted, trust_bytes=trust_bytes, source_bytes=source_bytes,
        membership_bytes=membership_bytes,
    )
    digest = hashlib.sha256(payload).hexdigest()
    result = object.__new__(DiagnosticBaselineCheckpoint)
    token = object()
    object.__setattr__(result, "_token", token)
    carrier_id = id(result)
    carrier_ref = weakref.ref(
        result, lambda callback_weakref, carrier_id=carrier_id: _diagnostic_baseline_cleanup(
            carrier_id, callback_weakref))
    entry = _BaselineIssuance(carrier_ref, token, trust, source, custody, membership,
                              trust_bytes, source_bytes, membership_bytes, payload, digest)
    with _DIAGNOSTIC_BASELINE_LOCK:
        _DIAGNOSTIC_BASELINE_REGISTRY[carrier_id] = entry
    return result


@dataclass(frozen=True, slots=True)
class ExpectedPurposeSelections:
    """Diagnostic result bytes, not a transferable baseline-admission capability."""

    canonical_bytes: bytes

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def records(self) -> dict[str, Any]:
        return json.loads(self.canonical_bytes)


def _purpose_replay_filing(record: Any) -> NcenFiling:
    """Invert the Q3 copy serializer without filling unknown or quarantined fields."""
    code = "diagnostic_source_checkpoint_mismatch"
    if type(record) is not dict or set(record) != {item.name for item in dataclasses.fields(NcenFiling)}:
        raise NcenError(code)

    def raw(value: Any) -> tuple[str | None, str | None, str | None]:
        if (type(value) is not list or len(value) != 3
                or any(item is not None and type(item) is not str for item in value)):
            raise NcenError(code)
        return tuple(value)  # type: ignore[return-value]

    def optional_date(value: Any) -> dt.date | None:
        return None if value is None else _purpose_parse_date(value, code=code)

    def optional_time(value: Any) -> dt.datetime | None:
        return None if value is None else _purpose_parse_timestamp(value, code=code)

    try:
        if (type(record["source_refs"]) is not list
                or any(type(item) is not str for item in record["source_refs"])
                or type(record["reasons"]) is not list
                or any(type(item) is not str for item in record["reasons"])
                or type(record["funds"]) is not list
                or type(record["underwriters"]) is not list):
            raise NcenError(code)
        funds = []
        for fund in record["funds"]:
            if type(fund) is not dict or set(fund) != {"series_id", "advisers"} or type(fund["advisers"]) is not list:
                raise NcenError(code)
            advisers = []
            for adviser in fund["advisers"]:
                if type(adviser) is not dict or set(adviser) != {"role", "file_number", "crd", "lei", "raw"}:
                    raise NcenError(code)
                advisers.append(AdviserRecord(adviser["role"], adviser["file_number"], adviser["crd"],
                                              adviser["lei"], raw(adviser["raw"])))
            funds.append(NcenFund(fund["series_id"], tuple(advisers)))
        underwriters = []
        for underwriter in record["underwriters"]:
            if type(underwriter) is not dict or set(underwriter) != {"file_number", "crd", "lei", "raw"}:
                raise NcenError(code)
            underwriters.append(UnderwriterRecord(underwriter["file_number"], underwriter["crd"],
                                                  underwriter["lei"], raw(underwriter["raw"])))
        filing = NcenFiling(
            accession_number=record["accession_number"], registrant_cik=record["registrant_cik"],
            form_type=record["form_type"], form_type_source=record["form_type_source"],
            report_period_end=optional_date(record["report_period_end"]),
            filing_date=optional_date(record["filing_date"]),
            public_available_at=optional_time(record["public_available_at"]),
            public_time_basis=record["public_time_basis"], data_known_at=optional_time(record["data_known_at"]),
            source=record["source"], source_refs=tuple(record["source_refs"]),
            family_answer=record["family_answer"], family_name_raw=record["family_name_raw"],
            funds=tuple(funds), underwriters=tuple(underwriters), status=record["status"],
            reasons=tuple(record["reasons"]), schema_version=record["schema_version"],
            retrieved_at=optional_time(record["retrieved_at"]), acceptance_at=optional_time(record["acceptance_at"]),
            public_date_bound=optional_time(record["public_date_bound"]),
            header_retrieved_at=optional_time(record["header_retrieved_at"]),
        )
        if _diagnostic_canonical(_purpose_baseline_filing(filing)) != _diagnostic_canonical(record):
            raise NcenError(code)
        return filing
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise NcenError(code) from exc


def replay_purpose_selections(
    baseline: DiagnosticBaselineCheckpoint, trusted_run: DiagnosticTrustPin, *,
    source_index: DiagnosticSourceIndex,
    membership_checkpoint: DiagnosticSyntheticMembershipCheckpoint,
    monitor: DiagnosticResourceMonitor | None = None,
    _snapshots: list[PurposeContextSnapshot] | None = None,
) -> ExpectedPurposeSelections:
    """Independently select every pinned synthetic member from Q3's complete candidates.

    The stored merged index and selections are consistency witnesses only. No exported
    row or caller-provided candidate subset participates in this replay.
    """
    if monitor is not None and not isinstance(monitor, DiagnosticResourceMonitor):
        raise TypeError("monitor_must_be_DiagnosticResourceMonitor")
    if source_index is None or membership_checkpoint is None:
        raise NcenError("diagnostic_baseline_unadmitted")
    if monitor is not None:
        monitor.check("selection_replay_start")
    payload = _diagnostic_bound_baseline_admission(
        baseline, trusted_run=trusted_run, source_index=source_index,
        membership_checkpoint=membership_checkpoint,
    )
    if monitor is not None:
        monitor.check("selection_replay_bound_baseline")
    if payload["lane"] != "synthetic_fixture":
        raise NcenError("diagnostic_baseline_unadmitted")
    index_fields = {item.name for item in dataclasses.fields(FormIndexEntry)}
    index_observations = payload["index_observations"]
    copy_observations = payload["copy_observations"]
    header_observations = payload["header_observations"]
    reconciliation = payload["reconciliation"]
    physical = payload["raw_input_inventory"]
    families = (
        ("physical_inputs", physical, "physical_input_id"),
        ("index_observation_ids", index_observations, "observation_id"),
        ("copy_observation_ids", copy_observations, "observation_id"),
        ("header_observation_ids", header_observations, "observation_id"),
    )
    for key, records, id_name in families:
        ids = [row[id_name] for row in records]
        reconciled = ([row[id_name] for row in reconciliation[key]]
                      if key == "physical_inputs" else reconciliation[key])
        if ids != reconciled or len(set(ids)) != len(ids):
            raise NcenError("diagnostic_raw_membership_mismatch")
    expected_coverage = {
        *(row["physical_input_id"] for row in physical),
        *(row["observation_id"] for group in (index_observations, copy_observations, header_observations)
          for row in group),
        *(f"{key}:{number}" for key in ("f6_exclusions", "f6_boundaries", "acquisition_requests")
          for number, _row in enumerate(reconciliation[key])),
    }
    if (set(payload["coverage"]) != expected_coverage
            or any(value.get("disposition") not in {
                "merged", "frozen_index_placeholder", "frozen_exclusion", "audit_out_of_scope",
            } for value in payload["coverage"].values())):
        raise NcenError("diagnostic_raw_membership_mismatch")
    if monitor is not None:
        monitor.rows_read(len(expected_coverage), "selection_replay_coverage")

    entries: list[FormIndexEntry] = []
    retrievals: dict[str, set[dt.datetime]] = defaultdict(set)
    for row in index_observations:
        if monitor is not None:
            monitor.rows_read(1, "selection_replay_index")
        value = row["entry"]
        if value is None:
            if row["parse_disposition"] == "parser_quarantine":
                raise NcenError("diagnostic_candidate_identity_conflict")
            continue
        if type(value) is not dict or set(value) != index_fields:
            raise NcenError("diagnostic_candidate_universe_mismatch")
        entry = FormIndexEntry(**{**value, "date_filed": _purpose_parse_date(
            value["date_filed"], code="diagnostic_candidate_universe_mismatch")})
        if {**dataclasses.asdict(entry), "date_filed": entry.date_filed.isoformat()} != value:
            raise NcenError("diagnostic_candidate_universe_mismatch")
        entries.append(entry)
        retrievals[entry.accession_number].add(_purpose_parse_timestamp(
            row["index_retrieved_at"], code="diagnostic_candidate_universe_mismatch"))
    if ([{**dataclasses.asdict(entry), "date_filed": entry.date_filed.isoformat()} for entry in entries]
            != payload["index_entries"]):
        raise NcenError("diagnostic_candidate_universe_mismatch")

    filings: list[NcenFiling] = []
    for row in copy_observations:
        if monitor is not None:
            monitor.rows_read(1, "selection_replay_copy")
        value = row["filing"]
        if value is None:
            if row["parse_state"] not in {"index_only", "unparseable"}:
                raise NcenError("diagnostic_quarantine_disposition_mismatch")
            continue
        filing = _purpose_replay_filing(value)
        if (row["parse_state"] != ("parsed" if filing.usable else "quarantined_partial")
                or row["accession_claims"] != [filing.accession_number]):
            raise NcenError("diagnostic_quarantine_disposition_mismatch")
        filings.append(filing)
    if [_purpose_baseline_filing(filing) for filing in filings] != payload["filings"]:
        raise NcenError("diagnostic_source_checkpoint_mismatch")

    from .sec_acquisition import SecHeaderError

    headers: dict[str, AcceptanceHeader] = {}
    for row in header_observations:
        if monitor is not None:
            monitor.rows_read(1, "selection_replay_header")
        value = row["header"]
        if value is None:
            if row["parse_state"] != "unparseable":
                raise NcenError("diagnostic_header_conflict")
            continue
        try:
            header = AcceptanceHeader.from_record(value)
        except (SecHeaderError, KeyError, TypeError, ValueError) as exc:
            raise NcenError("diagnostic_header_conflict") from exc
        if row["parse_state"] != "parsed" or header.accession_number != row["accession_claim"]:
            raise NcenError("diagnostic_header_conflict")
        if row["representative_observation_id"] == row["observation_id"]:
            if header.accession_number in headers:
                raise NcenError("diagnostic_header_conflict")
            headers[header.accession_number] = header
    if {key: item.to_record() for key, item in headers.items()} != payload["headers"]:
        raise NcenError("diagnostic_header_conflict")
    if monitor is not None:
        monitor.check("selection_replay_candidates")
    filing_accessions = {item.accession_number for item in filings}
    placeholder_times = set().union(*(
        retrievals[accession] for accession in retrievals
        if accession not in filing_accessions
        and any(entry.accession_number == accession and entry.form_type in NCEN_FORMS for entry in entries)
    )) if retrievals else set()
    if len(placeholder_times) > 1:
        raise NcenError("diagnostic_index_possession_unrepresentable")
    merged = merge_filings(
        filings, index_entries=entries, headers=headers,
        index_retrieved_at=next(iter(placeholder_times)) if placeholder_times else None,
    )
    merged_payload = {
        "by_cik": {cik: [_purpose_baseline_filing(item) for item in items]
                   for cik, items in merged.by_cik.items()},
        "excluded": merged.excluded,
    }
    if _diagnostic_canonical(merged_payload) != _diagnostic_canonical(payload["merged"]):
        raise NcenError("diagnostic_candidate_universe_mismatch")
    if monitor is not None:
        monitor.check("selection_replay_merge")

    members = membership_checkpoint.members
    checkpoint = payload["checkpoint"]
    if (payload["members"] != [_purpose_cohort_record(member, checkpoint["inventory_digest"])
                              for member in members]
            or len({(member.report_date, member.cik) for member in members}) != len(members)):
        raise NcenError("diagnostic_cohort_baseline_mismatch")
    context_dates = [item["R"] for item in payload["contexts"]]
    if (context_dates != [date.isoformat() for date in membership_checkpoint.report_dates]
            or payload["contexts"] != checkpoint["contexts"]):
        raise NcenError("diagnostic_context_coverage_mismatch")
    source_records, source_ids = _purpose_source_records(source_index)
    result: dict[str, Any] = {
        "schema_version": DIAGNOSTIC_SELECTION_REPLAY_VERSION, "lane": "synthetic_fixture",
        "baseline_digest": baseline.digest, "versions": checkpoint["versions"],
        "ablation_ids": [spec.ablation_id for spec in DIAGNOSTIC_ABLATIONS],
        "candidate_inventory": {
            "raw_input_inventory": physical, "index_observations": index_observations,
            "copy_observations": copy_observations, "header_observations": header_observations,
            "reconciliation": reconciliation, "coverage": payload["coverage"],
            "merged_exclusions": merged.excluded,
        },
        "sources": list(source_records),
        "global_exclusions": list(_purpose_exclusion_records((), source_index, source_ids)),
        "contexts": [],
    }
    replayed_selections: list[list[Any]] = []
    replayed_contexts: list[tuple[dt.date, dt.datetime, str, tuple[DiagnosticCohortMember, ...],
                                   tuple[DiagnosticSelection, ...], DiagnosticContext]] = []
    for raw_context in payload["contexts"]:
        if monitor is not None:
            monitor.check("selection_replay_context_start")
        date = _purpose_parse_date(raw_context["R"], code="diagnostic_context_coverage_mismatch")
        cutoff = _purpose_parse_timestamp(raw_context["K"], code="diagnostic_context_coverage_mismatch")
        mode = raw_context["mode"]
        group = tuple(member for member in members if member.report_date == date)
        if not group or tuple(member.cik for member in group) != tuple(sorted({member.cik for member in group})):
            raise NcenError("diagnostic_context_coverage_mismatch")
        selections = tuple(diagnostic_selection(
            merged, source_index, member.cik, date, cutoff, mode=mode, fund_keys=member.fund_keys,
        ) for member in group)
        replayed_selections.extend(_diagnostic_selection_payload(item) for item in selections)
        context = _diagnostic_build_context(
            selections, report_date=date, knowledge_cutoff=cutoff, mode=mode,
            inventory_digest=checkpoint["inventory_digest"], cohort_digest=checkpoint["cohort_digest"],
            ncen_evidence_digest=checkpoint["ncen_evidence_digest"],
            exclusion_ledger_digest=checkpoint["exclusion_ledger_digest"], baseline_digest=baseline.digest,
        )
        if _snapshots is not None:
            replayed_contexts.append((date, cutoff, mode, group, selections, context))
        contextual_ids = _purpose_contextual_source_ids(context, source_index, source_ids)
        dependency_times = [dependency.knowledge_time for selection in selections
                            for dependency in selection.dependencies]
        known = [member.known_at for member in group]
        if dependency_times and all(value is not None for value in dependency_times):
            known.extend(value for value in dependency_times if value is not None)
        established = bool(dependency_times) and all(value is not None for value in dependency_times)
        context_record = _purpose_record("context", {
            "context_id": context.context_id, "R": date.isoformat(), "K": _diagnostic_timestamp(cutoff),
            "mode": mode, "inventory_digest": checkpoint["inventory_digest"],
            "cohort_digest": checkpoint["cohort_digest"],
            "ncen_evidence_digest": checkpoint["ncen_evidence_digest"],
            "exclusion_ledger_digest": checkpoint["exclusion_ledger_digest"],
            "purpose_versions": {"reported_family": DIAGNOSTIC_REPORTED_FAMILY_VERSION,
                                 "reporting_dependence_block": DIAGNOSTIC_DEPENDENCE_VERSION},
            "edge_version": DIAGNOSTIC_EDGE_VERSION, "selection_version": DIAGNOSTIC_SELECTION_VERSION,
            "normalizer": DIAGNOSTIC_NAME_NORMALIZER_VERSION, "node_count": len(context.nodes),
            "dependency_time_established": established,
            "knowledge_time": _diagnostic_optional_timestamp(max(known)) if established else None,
            "diagnostic_only": True, "qualification": "NOT_EVALUABLE",
        })
        records = {
            "context_id": context.context_id, "R": date.isoformat(), "K": _diagnostic_timestamp(cutoff),
            "mode": mode, "context_record": context_record,
            "selections": [{
                "cik": member.cik, "fund_keys": list(member.fund_keys),
                "R": date.isoformat(), "K": _diagnostic_timestamp(cutoff), "mode": mode,
                "selected_accession": selection.accession_number,
                "selected_projection_digest": selection.selected_projection_digest,
                "knowledge_time": _diagnostic_optional_timestamp(selection.knowledge_time),
                "evidence_state": selection.evidence_state, "selection_reason": selection.selection_reason,
                "reasons": list(selection.reasons),
                "dependencies": [_purpose_dependency_payload(item) for item in selection.dependencies],
                "selection_payload": _diagnostic_selection_payload(selection),
                "origin_source_row_ids": [source_ids[item] for item in
                                          _diagnostic_bound_selection_admission(selection).origin_ids],
                "selected_source_row_ids": [contextual_ids[item.source_row_id] for item in selection.rows],
                "excluded_source_row_ids": [source_ids[item] for item in selection.excluded_source_row_ids],
            } for member, selection in zip(group, selections, strict=True)],
            "nodes": [_purpose_node_record(member, selection, node, context.incidences, contextual_ids)
                      for member, selection, node in zip(group, selections, context.nodes, strict=True)],
            "reported_families": [_purpose_family_record(context.context_id, family, contextual_ids)
                                  for family in context.reported_families],
            "incidences": [_purpose_incidence_record(item, contextual_ids) for item in context.incidences],
            "exclusions": list(_purpose_exclusion_records((context,), source_index, contextual_ids,
                                                         include_ledger=False)),
        }
        for name in ("nodes", "reported_families", "incidences", "exclusions"):
            records[name].sort(key=lambda row: row["record_id"])
        result["contexts"].append(records)
        if monitor is not None:
            monitor.rows_read(len(group) + len(context.incidences), "selection_replay_context")
            monitor.check("selection_replay_context_end")
    if replayed_selections != payload["selections"]:
        raise NcenError("diagnostic_selection_replay_mismatch")
    for date, cutoff, mode, group, selections, context in replayed_contexts:
        if monitor is not None:
            monitor.check("selection_replay_projection")
        projections = tuple(project_dependence(
            context.nodes, context.incidences, spec=spec, admission=context,
        ) for spec in DIAGNOSTIC_ABLATIONS)
        assert _snapshots is not None
        _snapshots.append(PurposeContextSnapshot(
            context_id=context.context_id, report_date=date, knowledge_cutoff=cutoff,
            mode=mode, inventory_digest=checkpoint["inventory_digest"],
            cohort_members=group, selections=selections,
            reported_families=context.reported_families, nodes=context.nodes,
            incidences=context.incidences, projections=projections,
        ))
    if monitor is not None:
        monitor.check("selection_replay_serialize")
    canonical = _diagnostic_canonical(result)
    if monitor is not None:
        monitor.check("selection_replay_complete")
    return ExpectedPurposeSelections(canonical)


def compare_purpose_selection_records(
    expected: ExpectedPurposeSelections, emitted: Mapping[str, Any],
) -> None:
    """Pure exact pre-graph comparison; local resealing cannot change the oracle."""
    try:
        if (type(expected) is not ExpectedPurposeSelections or type(emitted) is not dict
                or _diagnostic_canonical(emitted) != expected.canonical_bytes):
            raise NcenError("diagnostic_selection_replay_mismatch")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise NcenError("diagnostic_selection_replay_mismatch") from exc


_PURPOSE_SELECTION_INVENTORY_KEYS = frozenset({
    "schema_version", "lane", "baseline_digest", "versions", "ablation_ids",
    "candidate_inventory", "sources", "global_exclusions",
})
_PURPOSE_SELECTION_CONTEXT_KEYS = frozenset({
    "context_id", "R", "K", "mode", "context_record", "selections", "nodes",
    "reported_families", "incidences", "exclusions",
})
_PURPOSE_SELECTION_MEMBER_KEYS = frozenset({
    "cik", "fund_keys", "R", "K", "mode", "selected_accession",
    "selected_projection_digest", "knowledge_time", "evidence_state",
    "selection_reason", "reasons", "dependencies", "selection_payload",
    "origin_source_row_ids", "selected_source_row_ids", "excluded_source_row_ids",
})
_PURPOSE_CANDIDATE_INVENTORY_KEYS = frozenset({
    "raw_input_inventory", "index_observations", "copy_observations",
    "header_observations", "reconciliation", "coverage", "merged_exclusions",
})


def _purpose_selection_context_record(payload: Mapping[str, Any]) -> dict[str, Any]:
    """The v2 envelope hashes every byte of the complete C1b2a context payload."""
    if type(payload) is not dict or set(payload) != _PURPOSE_SELECTION_CONTEXT_KEYS:
        raise NcenError("diagnostic_selection_records_invalid")
    identity = {"schema_version": DIAGNOSTIC_EXPORT_VERSION,
                "record_type": "selection_context", **payload}
    return {**identity, "record_id": f"ncenrow:selection_context:{_diagnostic_hash(identity)}"}


def _purpose_serialize_selection_replay(
    expected: ExpectedPurposeSelections,
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    """Pure, lossless v2 transport; never use these emitted records as the oracle."""
    if type(expected) is not ExpectedPurposeSelections:
        raise NcenError("diagnostic_selection_records_invalid")
    replay = expected.records
    if type(replay) is not dict or set(replay) != _PURPOSE_SELECTION_INVENTORY_KEYS | {"contexts"}:
        raise NcenError("diagnostic_selection_records_invalid")
    inventory = {key: replay[key] for key in _PURPOSE_SELECTION_INVENTORY_KEYS}
    if type(replay["contexts"]) is not list:
        raise NcenError("diagnostic_selection_records_invalid")
    rows = tuple(_purpose_selection_context_record(context) for context in replay["contexts"])
    emitted = _purpose_reconstruct_emitted_replay(inventory, rows)
    compare_purpose_selection_records(expected, emitted)
    return inventory, rows


def _purpose_reconstruct_emitted_replay(
    inventory: Any, selection_rows: Any,
) -> dict[str, Any]:
    """Validate emitted fields and invert v2; no missing value comes from expected replay."""
    code = "diagnostic_selection_records_invalid"

    def obj(value: Any, keys: frozenset[str]) -> dict[str, Any]:
        if type(value) is not dict or set(value) != keys:
            raise NcenError(code)
        return value

    def text(value: Any) -> None:
        if type(value) is not str or not value:
            raise NcenError(code)

    def strings(value: Any, *, ordered: bool = True) -> None:
        if type(value) is not list or any(type(item) is not str or not item for item in value):
            raise NcenError(code)
        if len(value) != len(set(value)) or (ordered and value != sorted(value)):
            raise NcenError(code)

    def nested_record(value: Any, kind: str) -> None:
        try:
            _purpose_validate_record_shape(value, expected_type=kind)
        except (NcenError, AttributeError, KeyError, TypeError, ValueError) as exc:
            raise NcenError(code) from exc

    try:
        inv = obj(inventory, _PURPOSE_SELECTION_INVENTORY_KEYS)
        if inv["schema_version"] != DIAGNOSTIC_SELECTION_REPLAY_VERSION or inv["lane"] != "synthetic_fixture":
            raise NcenError(code)
        _diagnostic_hash_valid(inv["baseline_digest"], code)
        versions = obj(inv["versions"], frozenset({
            "selector", "merge", "amendment", "cohort", "source", "admission",
            "normalizer", "edge", "fold",
        }))
        for value in versions.values():
            text(value)
        if type(inv["ablation_ids"]) is not list or inv["ablation_ids"] != [
            spec.ablation_id for spec in DIAGNOSTIC_ABLATIONS
        ]:
            raise NcenError(code)
        candidate = obj(inv["candidate_inventory"], _PURPOSE_CANDIDATE_INVENTORY_KEYS)
        for key in ("raw_input_inventory", "index_observations", "copy_observations", "header_observations"):
            if type(candidate[key]) is not list or any(type(item) is not dict for item in candidate[key]):
                raise NcenError(code)
        for key, fields, identifier in (
            ("raw_input_inventory", frozenset({"schema_version", "physical_input_id", "root_id",
                "artifact_id", "container_member", "locator_kind", "locator", "raw_unit_sha256",
                "unit_kind", "disposition", "reason_codes", "observation_ids", "ledger_locators"}),
             "physical_input_id"),
            ("index_observations", frozenset({"schema_version", "observation_id", "artifact_id",
                "physical_input_id", "locator", "index_retrieved_at", "parse_disposition",
                "parser_reasons", "entry", "equivalence_group_id"}), "observation_id"),
            ("copy_observations", frozenset({"schema_version", "observation_id", "artifact_id",
                "source_kind", "raw_sha256", "physical_input_ids", "accession_claims", "cik_claims",
                "header_observation_ids", "acquisition_request_locators", "f6_exclusion_locators",
                "f6_boundary_locators", "parse_state", "parser_reasons", "filing", "merge_input_kind",
                "admission_disposition"}), "observation_id"),
            ("header_observations", frozenset({"schema_version", "observation_id", "artifact_id",
                "physical_input_id", "accession_claim", "parse_state", "parser_reasons", "header",
                "representative_observation_id"}), "observation_id"),
        ):
            ids = []
            for item in candidate[key]:
                obj(item, fields)
                text(item[identifier])
                expected_version = {
                    "raw_input_inventory": "ncen_diagnostic_raw_input_inventory_v1",
                    "index_observations": "ncen_diagnostic_index_observation_v2",
                    "copy_observations": "ncen_diagnostic_copy_observation_v2",
                    "header_observations": "ncen_diagnostic_header_observation_v2",
                }[key]
                if item["schema_version"] != expected_version:
                    raise NcenError(code)
                if key == "raw_input_inventory":
                    for field_name in ("root_id", "artifact_id", "locator_kind", "locator",
                                       "unit_kind", "disposition"):
                        text(item[field_name])
                    _diagnostic_hash_valid(item["raw_unit_sha256"], code)
                    if item["container_member"] is not None:
                        text(item["container_member"])
                    for field_name in ("reason_codes", "observation_ids", "ledger_locators"):
                        strings(item[field_name], ordered=False)
                elif key == "index_observations":
                    for field_name in ("artifact_id", "physical_input_id", "locator",
                                       "index_retrieved_at", "parse_disposition"):
                        text(item[field_name])
                    strings(item["parser_reasons"])
                    if item["entry"] is not None:
                        entry = obj(item["entry"], frozenset({
                            "form_type", "company_name", "cik", "date_filed", "file_name",
                            "accession_number",
                        }))
                        for field_name in entry:
                            text(entry[field_name])
                        _purpose_parse_date(entry["date_filed"], code=code)
                    if item["equivalence_group_id"] is not None:
                        text(item["equivalence_group_id"])
                elif key == "copy_observations":
                    for field_name in ("artifact_id", "source_kind", "parse_state",
                                       "merge_input_kind", "admission_disposition"):
                        text(item[field_name])
                    _diagnostic_hash_valid(item["raw_sha256"], code)
                    for field_name in ("physical_input_ids", "accession_claims", "cik_claims",
                                       "header_observation_ids", "acquisition_request_locators",
                                       "f6_exclusion_locators", "f6_boundary_locators", "parser_reasons"):
                        strings(item[field_name], ordered=False)
                    if item["filing"] is not None:
                        _purpose_replay_filing(item["filing"])
                else:
                    for field_name in ("artifact_id", "physical_input_id", "parse_state"):
                        text(item[field_name])
                    for field_name in ("accession_claim", "representative_observation_id"):
                        if item[field_name] is not None:
                            text(item[field_name])
                    strings(item["parser_reasons"])
                    if item["header"] is not None:
                        from .sec_acquisition import AcceptanceHeader, SecHeaderError

                        try:
                            parsed_header = AcceptanceHeader.from_record(item["header"])
                            if parsed_header.to_record() != item["header"]:
                                raise NcenError(code)
                        except (SecHeaderError, KeyError, TypeError, ValueError) as exc:
                            raise NcenError(code) from exc
                ids.append(item[identifier])
            if len(ids) != len(set(ids)):
                raise NcenError(code)
        if (type(candidate["coverage"]) is not dict or type(candidate["merged_exclusions"]) is not dict
                or any(type(key) is not str or not key or type(value) is not str
                       for key, value in candidate["merged_exclusions"].items())):
            raise NcenError(code)
        for value in candidate["coverage"].values():
            obj(value, frozenset({"disposition", "accession", "reason"}))
            text(value["disposition"])
            if value["reason"] is not None:
                text(value["reason"])
            if value["accession"] is not None:
                text(value["accession"])
        reconciliation = obj(candidate["reconciliation"], frozenset({
            "schema_version", "physical_inputs", "index_observation_ids", "copy_observation_ids",
            "header_observation_ids", "parser_quarantine", "f6_exclusions", "f6_boundaries",
            "acquisition_requests", "f1_subset_claim",
        }))
        if reconciliation["schema_version"] != "ncen_diagnostic_source_reconciliation_v2":
            raise NcenError(code)
        for name in ("index_observation_ids", "copy_observation_ids", "header_observation_ids"):
            strings(reconciliation[name], ordered=False)
        for name in ("physical_inputs", "parser_quarantine", "f6_exclusions",
                     "f6_boundaries", "acquisition_requests"):
            if type(reconciliation[name]) is not list or any(type(item) is not dict
                                                               for item in reconciliation[name]):
                raise NcenError(code)
        for item in reconciliation["physical_inputs"]:
            obj(item, frozenset({"physical_input_id", "disposition", "reason_codes", "observation_ids",
                                 "ledger_locators", "f6_exclusion_locators", "f6_boundary_locators",
                                 "f1_subset_claim"}))
            text(item["physical_input_id"])
            text(item["disposition"])
            text(item["f1_subset_claim"])
            for name in ("reason_codes", "observation_ids", "ledger_locators",
                         "f6_exclusion_locators", "f6_boundary_locators"):
                strings(item[name], ordered=False)
        for item in reconciliation["parser_quarantine"]:
            if set(item) not in ({"physical_input_id", "reason_codes"},
                                 {"observation_id", "reason_codes"}):
                raise NcenError(code)
            text(item.get("physical_input_id") or item.get("observation_id"))
            strings(item["reason_codes"])
        f6_fields = frozenset({
            "ledger_role", "accession_number", "registrant_cik", "form_type",
            "report_period_end", "acceptance_at", "acceptance_raw", "classification",
            "reasons", "terminal_reasons", "terminal_status", "schema_observed",
            "schema_version", "projection_equality", "xml_projection_digest",
            "dera_projection_digest", "request_id", "header_sha256", "terminal",
            "header_raw", "header_record", "raw_xml",
        })
        for name in ("f6_exclusions", "f6_boundaries"):
            for item in reconciliation[name]:
                obj(item, f6_fields)
                text(item["accession_number"])
                if type(item["schema_observed"]) is not bool:
                    raise NcenError(code)
                strings(item["reasons"])
                strings(item["terminal_reasons"])
                for ref in ("terminal", "header_raw", "header_record", "raw_xml"):
                    pinned = obj(item[ref], frozenset({"path", "sha256", "bytes"}))
                    text(pinned["path"])
                    _diagnostic_hash_valid(pinned["sha256"], code)
                    if type(pinned["bytes"]) is not int or pinned["bytes"] < 0:
                        raise NcenError(code)
        for item in reconciliation["acquisition_requests"]:
            obj(item, frozenset({"physical_input_id", "locator", "accession_number",
                                 "disposition", "refusal_reason"}))
            for name in ("physical_input_id", "locator", "disposition"):
                text(item[name])
            for name in ("accession_number", "refusal_reason"):
                if item[name] is not None:
                    text(item[name])
        obj(reconciliation["f1_subset_claim"], frozenset({"artifact_ids", "verification"}))
        strings(reconciliation["f1_subset_claim"]["artifact_ids"])
        text(reconciliation["f1_subset_claim"]["verification"])
        for name, kind in (("sources", "source_row"), ("global_exclusions", "source_exclusion")):
            if type(inv[name]) is not list:
                raise NcenError(code)
            for item in inv[name]:
                nested_record(item, kind)
        if type(selection_rows) not in (list, tuple) or not selection_rows:
            raise NcenError("diagnostic_context_coverage_mismatch")
        contexts: list[dict[str, Any]] = []
        seen_contexts: set[str] = set()
        seen_ids: set[str] = set()
        previous: tuple[str, str, str] | None = None
        for row in selection_rows:
            payload = obj(row, _PURPOSE_SELECTION_CONTEXT_KEYS | {
                "schema_version", "record_type", "record_id"})
            if payload != _purpose_selection_context_record({
                key: value for key, value in payload.items()
                if key not in {"schema_version", "record_type", "record_id"}
            }):
                raise NcenError(code)
            context_id = payload["context_id"]
            text(context_id)
            context_key = (payload["mode"], payload["R"], payload["K"])
            if context_id in seen_contexts or payload["record_id"] in seen_ids or (
                previous is not None and context_key <= previous
            ):
                raise NcenError("diagnostic_context_coverage_mismatch")
            seen_contexts.add(context_id)
            seen_ids.add(payload["record_id"])
            previous = context_key
            _purpose_parse_date(payload["R"], code=code)
            _purpose_parse_timestamp(payload["K"], code=code)
            _check_mode(payload["mode"])
            nested_record(payload["context_record"], "context")
            if any(payload["context_record"][key] != payload[key]
                   for key in ("context_id", "R", "K", "mode")):
                raise NcenError(code)
            selections = payload["selections"]
            if type(selections) is not list or not selections:
                raise NcenError("diagnostic_context_coverage_mismatch")
            ciks: list[str] = []
            for selected in selections:
                obj(selected, _PURPOSE_SELECTION_MEMBER_KEYS)
                _diagnostic_normalized_cik(selected["cik"])
                ciks.append(selected["cik"])
                if any(selected[key] != payload[key] for key in ("R", "K", "mode")):
                    raise NcenError(code)
                strings(selected["fund_keys"])
                if not selected["fund_keys"]:
                    raise NcenError(code)
                strings(selected["reasons"])
                for name in ("origin_source_row_ids", "selected_source_row_ids", "excluded_source_row_ids"):
                    strings(selected[name], ordered=False)
                if selected["evidence_state"] not in {"complete", "incomplete"}:
                    raise NcenError(code)
                for name in ("selected_accession", "selected_projection_digest", "knowledge_time",
                             "selection_reason"):
                    if selected[name] is not None:
                        text(selected[name])
                if type(selected["dependencies"]) is not list:
                    raise NcenError(code)
                for dependency in selected["dependencies"]:
                    obj(dependency, frozenset({"accession", "role", "knowledge_time"}))
                    text(dependency["accession"])
                    text(dependency["role"])
                    if dependency["knowledge_time"] is not None:
                        _purpose_parse_timestamp(dependency["knowledge_time"], code=code)
                projection = selected["selection_payload"]
                if type(projection) is not list or len(projection) != 14 or projection[0] != DIAGNOSTIC_SELECTION_VERSION:
                    raise NcenError(code)
                strings(projection[3], ordered=False)
                strings(projection[13], ordered=False)
                if (projection[1] != selected["cik"] or projection[2] != selected["selected_accession"]
                        or projection[4] != selected["evidence_state"] or projection[5] != selected["reasons"]
                        or projection[6] != selected["selection_reason"]
                        or projection[7] != selected["selected_projection_digest"]
                        or projection[8] != [[dependency["accession"], dependency["role"],
                                              dependency["knowledge_time"]]
                                             for dependency in selected["dependencies"]]
                        or projection[9] != selected["knowledge_time"]
                        or projection[10:13] != [selected["R"], selected["K"], selected["mode"]]):
                    raise NcenError(code)
            if ciks != sorted(set(ciks)):
                raise NcenError("diagnostic_context_coverage_mismatch")
            for name, kind in (("nodes", "node"), ("reported_families", "reported_family"),
                               ("incidences", "incidence"), ("exclusions", "source_exclusion")):
                if type(payload[name]) is not list:
                    raise NcenError(code)
                ids = []
                for item in payload[name]:
                    nested_record(item, kind)
                    if item["context_id"] != context_id:
                        raise NcenError(code)
                    ids.append(item["record_id"])
                if ids != sorted(set(ids)):
                    raise NcenError(code)
            if (sorted(item["cik"] for item in payload["nodes"]) != ciks
                    or sorted(item["cik"] for item in payload["reported_families"]) != ciks
                    or any(item["cik"] not in ciks for item in payload["incidences"])):
                raise NcenError("diagnostic_context_coverage_mismatch")
            contexts.append({key: payload[key] for key in _PURPOSE_SELECTION_CONTEXT_KEYS})
        return {**inv, "contexts": contexts}
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise NcenError(code) from exc


def _purpose_exact_object(value: Any, keys: frozenset[str], *, code: str) -> dict[str, Any]:
    if not isinstance(value, dict) or frozenset(value) != keys:
        raise NcenError(f"{code}_keys_invalid")
    return value


def _purpose_nonnegative(value: Any, *, code: str) -> int:
    if type(value) is not int or value < 0:
        raise NcenError(f"{code}_nonnegative_integer_required")
    return value


def _purpose_positive(value: Any, *, code: str) -> int:
    result = _purpose_nonnegative(value, code=code)
    if result == 0:
        raise NcenError(f"{code}_positive_integer_required")
    return result


def _purpose_sorted_strings(
    value: Any,
    *,
    code: str,
    allow_empty: bool = True,
) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise NcenError(f"{code}_string_array_invalid")
    output = tuple(value)
    if output != tuple(sorted(set(output))) or (not allow_empty and not output):
        raise NcenError(f"{code}_not_sorted_unique")
    return output


def _purpose_record(record_type: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    body = dict(payload)
    if any(key in body for key in ("schema_version", "record_type", "record_id")):
        raise NcenError("diagnostic_record_common_field_collision")
    identity = {
        "schema_version": DIAGNOSTIC_SCHEMA_VERSION,
        "record_type": record_type,
        **body,
    }
    return {
        **identity,
        "record_id": f"ncenrow:{record_type}:{_diagnostic_hash(identity)}",
    }


def _purpose_cohort_record(member: DiagnosticCohortMember, inventory_digest: str) -> dict[str, Any]:
    return _purpose_record(
        "cohort_member",
        {
            "inventory_digest": inventory_digest,
            "R": member.report_date.isoformat(),
            "cik": member.cik,
            "fund_keys": list(member.fund_keys),
            "known_at": _diagnostic_timestamp(member.known_at),
        },
    )


def load_diagnostic_cohort(
    path: str | Path,
    *,
    trusted_manifest: DiagnosticCohortManifestPin,
) -> DiagnosticCohort:
    """Load only a closed, externally pinned non-pickle slim full-cohort checkpoint."""
    root = Path(path)
    if root.suffix.lower() in {".pkl", ".pickle"}:
        raise NcenError("diagnostic_cohort_pickle_forbidden")
    if not root.is_dir() or root.is_symlink():
        raise NcenError("diagnostic_cohort_root_invalid")
    manifest_path = _purpose_safe_path(root, "manifest.json", code="diagnostic_cohort")
    sums_path = _purpose_safe_path(root, "SHA256SUMS", code="diagnostic_cohort")
    cohort_path = _purpose_safe_path(root, "cohort.jsonl", code="diagnostic_cohort")
    if trusted_manifest.manifest_path.resolve() != manifest_path.resolve():
        raise NcenError("diagnostic_cohort_manifest_path_mismatch")
    if manifest_path.stat().st_size != trusted_manifest.manifest_size:
        raise NcenError("diagnostic_cohort_manifest_size_mismatch")
    if _purpose_sha256(manifest_path) != trusted_manifest.manifest_sha256:
        raise NcenError("diagnostic_cohort_manifest_sha256_mismatch")
    if _purpose_sha256(sums_path) != trusted_manifest.sha256sums_sha256:
        raise NcenError("diagnostic_cohort_sha256sums_mismatch")
    names = sorted(item.name for item in root.iterdir())
    if names != ["SHA256SUMS", "cohort.jsonl", "manifest.json"]:
        raise NcenError("diagnostic_cohort_artifact_inventory_not_closed")
    manifest = _purpose_exact_object(
        _purpose_json_load(manifest_path.read_bytes(), code="diagnostic_cohort_manifest"),
        frozenset(
            {
                "schema_version",
                "status",
                "cohort_version",
                "knowledge_cutoff",
                "knowledge_mode",
                "inventory_digest",
                "cohort_digest",
                "inventory_sources",
                "full_cohort_provenance",
                "files",
                "outcome_fields_used",
            }
        ),
        code="diagnostic_cohort_manifest",
    )
    if (
        manifest["schema_version"] != DIAGNOSTIC_COHORT_MANIFEST_VERSION
        or manifest["status"] != "complete"
        or manifest["cohort_version"] != DIAGNOSTIC_COHORT_VERSION
        or manifest["outcome_fields_used"] is not False
    ):
        raise NcenError("diagnostic_cohort_manifest_not_trusted_complete")
    provenance = _purpose_exact_object(
        manifest["full_cohort_provenance"],
        frozenset(
            {
                "derivation",
                "full_cohort",
                "inventory_version",
                "sealed_inventory",
                "source_inventory_digest",
            }
        ),
        code="diagnostic_cohort_provenance",
    )
    if provenance != {
        "derivation": "sealed_vote_inventory_full_universe",
        "full_cohort": True,
        "inventory_version": INVENTORY_VERSION,
        "sealed_inventory": True,
        "source_inventory_digest": manifest["inventory_digest"],
    }:
        raise NcenError("diagnostic_cohort_provenance_invalid")
    files = manifest["files"]
    if not isinstance(files, list) or len(files) != 1:
        raise NcenError("diagnostic_cohort_files_invalid")
    file_record = _purpose_exact_object(
        files[0],
        frozenset({"path", "sha256", "bytes", "rows", "record_type"}),
        code="diagnostic_cohort_file",
    )
    if file_record["path"] != "cohort.jsonl" or file_record["record_type"] != "cohort_member":
        raise NcenError("diagnostic_cohort_file_invalid")
    _diagnostic_hash_valid(file_record["sha256"], "diagnostic_cohort_file_sha256")
    _purpose_nonnegative(file_record["bytes"], code="diagnostic_cohort_file_bytes")
    _purpose_positive(file_record["rows"], code="diagnostic_cohort_file_rows")
    if cohort_path.stat().st_size != file_record["bytes"] or _purpose_sha256(cohort_path) != file_record["sha256"]:
        raise NcenError("diagnostic_cohort_file_mismatch")
    sums_lines = sums_path.read_text(encoding="ascii").splitlines()
    expected_sums = [
        f"{file_record['sha256']}  cohort.jsonl",
        f"{trusted_manifest.manifest_sha256}  manifest.json",
    ]
    if sums_lines != expected_sums:
        raise NcenError("diagnostic_cohort_sha256sums_content_invalid")
    rows: list[dict[str, Any]] = []
    raw = cohort_path.read_bytes()
    if not raw.endswith(b"\n") or b"\r" in raw:
        raise NcenError("diagnostic_cohort_jsonl_encoding_invalid")
    for line in raw.splitlines():
        record = _purpose_json_load(line, code="diagnostic_cohort_row")
        record = _purpose_exact_object(
            record,
            frozenset(
                {
                    "schema_version",
                    "record_type",
                    "record_id",
                    "inventory_digest",
                    "R",
                    "cik",
                    "fund_keys",
                    "known_at",
                }
            ),
            code="diagnostic_cohort_row",
        )
        payload = {key: value for key, value in record.items() if key not in {"schema_version", "record_type", "record_id"}}
        if record != _purpose_record("cohort_member", payload):
            raise NcenError("diagnostic_cohort_record_id_mismatch")
        rows.append(record)
    if len(rows) != file_record["rows"] or rows != sorted(rows, key=lambda item: item["record_id"]):
        raise NcenError("diagnostic_cohort_rows_not_sorted_or_complete")
    sources: list[InventorySource] = []
    if not isinstance(manifest["inventory_sources"], list) or not manifest["inventory_sources"]:
        raise NcenError("diagnostic_cohort_sources_invalid")
    source_keys = frozenset(
        {"package_label", "zip_sha256", "package_id", "retrieved_at", "first_verified_public_at"}
    )
    for raw_source in manifest["inventory_sources"]:
        source = _purpose_exact_object(raw_source, source_keys, code="diagnostic_cohort_source")
        _diagnostic_nonempty(source["package_label"], "inventory_package_label")
        _diagnostic_nonempty(source["package_id"], "inventory_package_id")
        sources.append(
            InventorySource(
                package_label=source["package_label"],
                zip_sha256=source["zip_sha256"],
                package_id=source["package_id"],
                retrieved_at=_purpose_parse_timestamp(
                    source["retrieved_at"], code="inventory_source_retrieved_at"
                ),
                first_verified_public_at=_purpose_parse_timestamp(
                    source["first_verified_public_at"],
                    code="inventory_source_first_verified_public_at",
                ),
            )
        )
    members: list[DiagnosticCohortMember] = []
    for record in rows:
        if record["inventory_digest"] != manifest["inventory_digest"]:
            raise NcenError("diagnostic_cohort_inventory_digest_mismatch")
        _diagnostic_normalized_cik(record["cik"])
        members.append(
            DiagnosticCohortMember(
                report_date=_purpose_parse_date(record["R"], code="cohort_report_date"),
                cik=record["cik"],
                fund_keys=_purpose_sorted_strings(
                    record["fund_keys"], code="diagnostic_cohort_fund_keys", allow_empty=False
                ),
                known_at=_purpose_parse_timestamp(record["known_at"], code="cohort_known_at"),
            )
        )
    cohort = DiagnosticCohort(
        knowledge_cutoff=_purpose_parse_timestamp(
            manifest["knowledge_cutoff"], code="cohort_knowledge_cutoff"
        ),
        knowledge_mode=manifest["knowledge_mode"],
        inventory_digest=manifest["inventory_digest"],
        sources=tuple(sources),
        members=tuple(sorted(members, key=lambda item: (item.report_date, item.cik))),
        cohort_digest=manifest["cohort_digest"],
        extraction_version=DIAGNOSTIC_COHORT_VERSION,
        derivation="sealed_vote_inventory_full_universe",
        full_cohort=True,
        outcome_fields_excluded=True,
        _seal=_DIAGNOSTIC_COHORT_SEAL,
    )
    return cohort


@dataclass(frozen=True, slots=True)
class PurposeContextSnapshot:
    context_id: str
    report_date: dt.date
    knowledge_cutoff: dt.datetime
    mode: str
    inventory_digest: str
    cohort_members: tuple[DiagnosticCohortMember, ...]
    selections: tuple[DiagnosticSelection, ...]
    reported_families: tuple[ReportedFamily, ...]
    nodes: tuple[DependenceNode, ...]
    incidences: tuple[TypedIncidence, ...]
    projections: tuple[DependenceProjection, ...]

    def __post_init__(self) -> None:
        for projection in self.projections:
            bound = _diagnostic_bound_projection_admission(projection)
            if bound.lane == "pure_fixture":
                raise NcenError("diagnostic_fixture_lane_not_exportable")
        _diagnostic_context_id_valid(self.context_id)
        if tuple(item.cik for item in self.cohort_members) != tuple(item.cik for item in self.nodes):
            raise NcenError("purpose_snapshot_cohort_node_mismatch")
        if tuple(item.cik for item in self.selections) != tuple(item.cik for item in self.nodes):
            raise NcenError("purpose_snapshot_selection_node_mismatch")
        if tuple(item.cik for item in self.reported_families) != tuple(item.cik for item in self.nodes):
            raise NcenError("purpose_snapshot_family_node_mismatch")
        if tuple(item.ablation_id for item in self.projections) != tuple(
            spec.ablation_id for spec in DIAGNOSTIC_ABLATIONS
        ):
            raise NcenError("purpose_snapshot_ablations_incomplete")

    def dependence_snapshots(self) -> tuple[DependenceSnapshot, ...]:
        return tuple(
            DependenceSnapshot(
                report_date=self.report_date,
                knowledge_cutoff=self.knowledge_cutoff,
                mode=self.mode,
                inventory_digest=self.inventory_digest,
                projection=projection,
            )
            for projection in self.projections
        )


_PURPOSE_DECLARATION_KEYS = frozenset(
    {
        "schema_version",
        "diagnostic_schema_version",
        "cohort_derivation_version",
        "purpose_versions",
        "edge_version",
        "selection_version",
        "contexts",
        "inventory_digest",
        "cohort_digest",
        "cohort_provenance",
        "baseline_seals",
        "source_code",
        "input_artifacts",
        "ncen_source_manifest",
        "ncen_evidence_digest",
        "exclusion_ledger_digest",
        "quarantine_seal",
        "ablations",
        "fold_protocol",
        "normalizer",
        "normalization_rule",
        "outcome_input_allowlist",
        "limits",
        "sensitivity_stage",
        "predecessor_receipt_sha256",
        "diagnostic_only",
        "lane",
        "pin_roles",
        "trusted_run_manifest_sha256",
        "baseline_digest",
        "selection_replay_version",
        "selection_expected_digest",
        "acceptance_scope",
        "qualification",
    }
)
_PURPOSE_V2_BINDING_KEYS = frozenset({
    "trusted_run_manifest_sha256", "baseline_digest", "selection_replay_version",
    "selection_expected_digest", "acceptance_scope", "qualification",
})
_PURPOSE_NORMALIZATION_RULE = (
    "NFC+uppercase+unicode-whitespace-collapse+trim+NFC;"
    "preserve-punctuation-word-boundaries-legal-suffixes-accents"
)


def _purpose_v2_control_type(value: Any, shape: Any) -> None:
    """Check the closed JSON shape before semantic validators access its values."""
    if isinstance(shape, dict):
        if type(value) is not dict or set(value) != set(shape):
            raise NcenError("diagnostic_pinned_input_invalid")
        for name, child in shape.items():
            _purpose_v2_control_type(value[name], child)
    elif isinstance(shape, list):
        if type(value) is not list:
            raise NcenError("diagnostic_pinned_input_invalid")
        for item in value:
            _purpose_v2_control_type(item, shape[0])
    elif type(value) not in (shape if isinstance(shape, tuple) else (shape,)):
        raise NcenError("diagnostic_pinned_input_invalid")


_PURPOSE_V2_BINDING_TYPES = {name: str for name in _PURPOSE_V2_BINDING_KEYS}
_PURPOSE_V2_PIN_ROW = {"root_id": str, "path": str, "sha256": str, "bytes": int}
_PURPOSE_V2_CODE_ROW = {"path": str, "sha256": str}
_PURPOSE_V2_SEAL_ROW = {"name": str, "sha256": str}


def _purpose_v2_control_shape(value: Any, *, kind: str) -> None:
    if kind == "declaration":
        shape = {
            **_PURPOSE_V2_BINDING_TYPES,
            "schema_version": str, "diagnostic_schema_version": str,
            "cohort_derivation_version": str,
            "purpose_versions": {"reported_family": str, "reporting_dependence_block": str},
            "edge_version": str, "selection_version": str,
            "contexts": [{"R": str, "K": str, "mode": str}],
            "inventory_digest": str, "cohort_digest": str,
            "cohort_provenance": {
                "derivation": str, "full_cohort": bool, "inventory_version": str,
                "sealed_inventory": bool, "outcome_fields_excluded": bool,
                "knowledge_cutoff": str, "knowledge_mode": str,
                "inventory_sources": [{
                    "package_label": str, "zip_sha256": str, "package_id": str,
                    "retrieved_at": str, "first_verified_public_at": str,
                }],
            },
            "baseline_seals": [_PURPOSE_V2_SEAL_ROW],
            "source_code": [_PURPOSE_V2_CODE_ROW],
            "input_artifacts": [_PURPOSE_V2_PIN_ROW],
            "ncen_source_manifest": {
                "sha256": str, "bytes": int, "kind": str, "evidence_digest": str,
            },
            "ncen_evidence_digest": str, "exclusion_ledger_digest": str,
            "quarantine_seal": {"scope_sha256": str, "sha256sums_sha256": str},
            "ablations": [str], "fold_protocol": str, "normalizer": str,
            "normalization_rule": str, "outcome_input_allowlist": [str],
            "limits": {"memory_limit_bytes": int, "memory_soft_limit_bytes": int,
                       "max_contexts": int, "max_source_rows": int},
            "sensitivity_stage": str, "predecessor_receipt_sha256": (str, type(None)),
            "diagnostic_only": bool, "lane": str,
            "pin_roles": [{"role": str, "pins": list}],
        }
    elif kind == "manifest":
        shape = {
            **_PURPOSE_V2_BINDING_TYPES,
            "schema_version": str, "declaration_sha256": str,
            "baseline_seals": [_PURPOSE_V2_SEAL_ROW],
            "source_code": [_PURPOSE_V2_CODE_ROW],
            "runtime": {"python_version": str, "unicode_version": str, "platform": str},
            "input_artifacts": [_PURPOSE_V2_PIN_ROW],
            "files": [{"path": str, "sha256": str, "bytes": int,
                       "rows": int, "record_type": str}],
            "coverage": {"requested_contexts": int, "completed_contexts": int,
                         "missing_contexts": int, "requested_accessions": int,
                         "verified_accessions": int, "quarantined_accessions": int},
            "status": str, "reasons": [str], "issuer_group_ref_sha256": str,
            "outcome_inputs_used": bool, "diagnostic_only": bool,
        }
    elif kind == "checks":
        shape = {
            **_PURPOSE_V2_BINDING_TYPES, "schema_version": str,
            "checks": [{"name": str, "status": str, "details": str}],
            "validation_code_sha256": str, "status": str,
        }
    else:
        raise ValueError("unknown_v2_control_kind")
    _purpose_v2_control_type(value, shape)
    if kind == "declaration":
        for role in value["pin_roles"]:
            for pin in role["pins"]:
                fields = (dict(_PURPOSE_V2_PIN_ROW, offset=int, length=int)
                          if role["role"].startswith("baseline_") else _PURPOSE_V2_PIN_ROW)
                _purpose_v2_control_type(pin, fields)


def _purpose_relative_path_value(value: Any, *, code: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or value.startswith("/")
        or re.match(r"^[A-Za-z]:", value)
        or any(part in {"", ".", ".."} for part in Path(value).parts)
    ):
        raise NcenError(f"{code}_path_unsafe")
    return value


def _purpose_validate_pin_rows(
    value: Any,
    *,
    keys: frozenset[str],
    code: str,
    require_bytes: bool,
) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        raise NcenError(f"{code}_invalid")
    output: list[dict[str, Any]] = []
    for item in value:
        row = _purpose_exact_object(item, keys, code=code)
        _diagnostic_hash_valid(row["sha256"], f"{code}_sha256")
        if "path" in row:
            _purpose_relative_path_value(row["path"], code=code)
        if "name" in row:
            _diagnostic_nonempty(row["name"], f"{code}_name")
        if "root_id" in row:
            _diagnostic_nonempty(row["root_id"], f"{code}_root_id")
        if require_bytes:
            _purpose_positive(row["bytes"], code=f"{code}_bytes")
        output.append(row)
    order_key = "path" if "path" in keys else "name"
    ordered = sorted(output, key=lambda item: (item.get("root_id", ""), item[order_key]))
    identities = [(item.get("root_id", ""), item[order_key]) for item in ordered]
    if output != ordered or len(identities) != len(set(identities)):
        raise NcenError(f"{code}_not_sorted_unique")
    return tuple(output)


def _purpose_v2_bindings(value: Mapping[str, Any]) -> None:
    for name in ("trusted_run_manifest_sha256", "baseline_digest", "selection_expected_digest"):
        if type(value[name]) is not str:
            raise NcenError("diagnostic_pinned_input_invalid")
        _diagnostic_hash_valid(value[name], name)
        if value[name] == "0" * 64:
            raise NcenError("diagnostic_pinned_input_invalid")
    if (value["selection_replay_version"] != DIAGNOSTIC_SELECTION_REPLAY_VERSION
            or value["acceptance_scope"] != "synthetic_engineering_only"
            or value["qualification"] != "NOT_EVALUABLE"):
        raise NcenError("diagnostic_export_scope_invalid")


def _purpose_declaration(
    value: Mapping[str, Any] | dict[str, Any], *, legacy_fixture: bool = False,
) -> dict[str, Any]:
    version = value.get("schema_version") if isinstance(value, Mapping) else None
    historical = legacy_fixture and version == "ncen_purpose_diagnostics_declaration_v1"
    if not historical and version != DIAGNOSTIC_DECLARATION_VERSION:
        raise NcenError("diagnostic_export_schema_unsupported")
    if not historical:
        _purpose_v2_control_shape(dict(value), kind="declaration")
    keys = (_PURPOSE_DECLARATION_KEYS - _PURPOSE_V2_BINDING_KEYS - {"lane", "pin_roles"}
            if historical else _PURPOSE_DECLARATION_KEYS)
    declaration = _purpose_exact_object(dict(value), keys, code="declaration")
    if (
        declaration["schema_version"] != ("ncen_purpose_diagnostics_declaration_v1" if historical else DIAGNOSTIC_DECLARATION_VERSION)
        or declaration["diagnostic_schema_version"] != (DIAGNOSTIC_SCHEMA_VERSION if historical else DIAGNOSTIC_EXPORT_VERSION)
        or declaration["cohort_derivation_version"] != DIAGNOSTIC_COHORT_VERSION
        or declaration["edge_version"] != DIAGNOSTIC_EDGE_VERSION
        or declaration["selection_version"] != DIAGNOSTIC_SELECTION_VERSION
        or declaration["fold_protocol"] != DIAGNOSTIC_FOLD_PROTOCOL_VERSION
        or declaration["normalizer"] != DIAGNOSTIC_NAME_NORMALIZER_VERSION
        or declaration["normalization_rule"] != _PURPOSE_NORMALIZATION_RULE
        or declaration["diagnostic_only"] is not True
        or declaration["sensitivity_stage"] != "not_computed"
    ):
        raise NcenError("declaration_version_or_policy_invalid")
    if not historical:
        _purpose_v2_bindings(declaration)
        if declaration["lane"] not in _DIAGNOSTIC_MANIFEST_KINDS:
            raise NcenError("diagnostic_lane_mismatch")
        _purpose_trust_roles(declaration["pin_roles"])
        if not declaration["source_code"] or not declaration["baseline_seals"] or not declaration["input_artifacts"]:
            raise NcenError("diagnostic_declared_pin_mismatch")
    purpose_versions = _purpose_exact_object(
        declaration["purpose_versions"],
        frozenset({"reported_family", "reporting_dependence_block"}),
        code="declaration_purpose_versions",
    )
    if purpose_versions != {
        "reported_family": DIAGNOSTIC_REPORTED_FAMILY_VERSION,
        "reporting_dependence_block": DIAGNOSTIC_DEPENDENCE_VERSION,
    }:
        raise NcenError("declaration_purpose_versions_invalid")
    _diagnostic_nonempty(declaration["inventory_digest"], "declaration_inventory_digest")
    _diagnostic_hash_valid(declaration["cohort_digest"], "declaration_cohort_digest")
    _diagnostic_hash_valid(declaration["ncen_evidence_digest"], "declaration_ncen_evidence_digest")
    _diagnostic_hash_valid(
        declaration["exclusion_ledger_digest"], "declaration_exclusion_ledger_digest"
    )
    contexts = declaration["contexts"]
    if not isinstance(contexts, list) or not contexts:
        raise NcenError("declaration_contexts_invalid")
    context_keys = frozenset({"R", "K", "mode"})
    context_order: list[tuple[str, dt.date, dt.datetime]] = []
    for raw_context in contexts:
        context = _purpose_exact_object(raw_context, context_keys, code="declaration_context")
        report_date = _purpose_parse_date(context["R"], code="declaration_report_date")
        cutoff = _purpose_parse_timestamp(context["K"], code="declaration_knowledge_cutoff")
        _check_mode(context["mode"])
        if context["K"] != _diagnostic_timestamp(cutoff):
            raise NcenError("declaration_context_timestamp_not_canonical")
        context_order.append((context["mode"], report_date, cutoff))
    if context_order != sorted(set(context_order)):
        raise NcenError("declaration_contexts_not_sorted_unique")
    if tuple(declaration["ablations"]) != tuple(spec.ablation_id for spec in DIAGNOSTIC_ABLATIONS):
        raise NcenError("declaration_ablations_invalid")
    if tuple(declaration["outcome_input_allowlist"]) != DIAGNOSTIC_OUTCOME_INPUT_ALLOWLIST:
        raise NcenError("declaration_outcome_allowlist_invalid")
    cohort_provenance = _purpose_exact_object(
        declaration["cohort_provenance"],
        frozenset(
            {
                "derivation",
                "full_cohort",
                "inventory_version",
                "sealed_inventory",
                "outcome_fields_excluded",
                "knowledge_cutoff",
                "knowledge_mode",
                "inventory_sources",
            }
        ),
        code="declaration_cohort_provenance",
    )
    if (
        cohort_provenance["derivation"] != "sealed_vote_inventory_full_universe"
        or cohort_provenance["full_cohort"] is not True
        or cohort_provenance["inventory_version"] != INVENTORY_VERSION
        or cohort_provenance["sealed_inventory"] is not True
        or cohort_provenance["outcome_fields_excluded"] is not True
    ):
        raise NcenError("declaration_cohort_provenance_invalid")
    cutoff = _purpose_parse_timestamp(
        cohort_provenance["knowledge_cutoff"],
        code="declaration_cohort_knowledge_cutoff",
    )
    if cohort_provenance["knowledge_cutoff"] != _diagnostic_timestamp(cutoff):
        raise NcenError("declaration_cohort_cutoff_not_canonical")
    _check_mode(cohort_provenance["knowledge_mode"])
    if not isinstance(cohort_provenance["inventory_sources"], list) or not cohort_provenance[
        "inventory_sources"
    ]:
        raise NcenError("declaration_cohort_sources_invalid")
    source_keys = frozenset(
        {"package_label", "zip_sha256", "package_id", "retrieved_at", "first_verified_public_at"}
    )
    source_labels: list[str] = []
    for raw_source in cohort_provenance["inventory_sources"]:
        source = _purpose_exact_object(
            raw_source,
            source_keys,
            code="declaration_cohort_source",
        )
        _diagnostic_nonempty(source["package_label"], "declaration_cohort_package_label")
        _diagnostic_nonempty(source["package_id"], "declaration_cohort_package_id")
        _diagnostic_hash_valid(source["zip_sha256"], "declaration_cohort_source_sha256")
        _purpose_parse_timestamp(
            source["retrieved_at"], code="declaration_cohort_source_retrieved_at"
        )
        _purpose_parse_timestamp(
            source["first_verified_public_at"],
            code="declaration_cohort_source_first_verified_public_at",
        )
        source_labels.append(source["package_label"])
    if source_labels != sorted(set(source_labels)):
        raise NcenError("declaration_cohort_sources_not_sorted_unique")
    _purpose_validate_pin_rows(
        declaration["baseline_seals"],
        keys=frozenset({"name", "sha256"}),
        code="declaration_baseline_seal",
        require_bytes=False,
    )
    _purpose_validate_pin_rows(
        declaration["source_code"],
        keys=frozenset({"path", "sha256"}),
        code="declaration_source_code",
        require_bytes=False,
    )
    _purpose_validate_pin_rows(
        declaration["input_artifacts"],
        keys=frozenset({"root_id", "path", "sha256", "bytes"}),
        code="declaration_input_artifact",
        require_bytes=True,
    )
    source_manifest = _purpose_exact_object(
        declaration["ncen_source_manifest"],
        frozenset({"sha256", "bytes", "kind", "evidence_digest"}),
        code="declaration_source_manifest",
    )
    _diagnostic_hash_valid(source_manifest["sha256"], "declaration_source_manifest_sha256")
    _diagnostic_hash_valid(
        source_manifest["evidence_digest"], "declaration_source_manifest_evidence_digest"
    )
    _purpose_positive(source_manifest["bytes"], code="declaration_source_manifest_bytes")
    if source_manifest["kind"] not in _DIAGNOSTIC_MANIFEST_KINDS:
        raise NcenError("declaration_source_manifest_kind_invalid")
    quarantine = _purpose_exact_object(
        declaration["quarantine_seal"],
        frozenset({"scope_sha256", "sha256sums_sha256"}),
        code="declaration_quarantine_seal",
    )
    _diagnostic_hash_valid(quarantine["scope_sha256"], "declaration_quarantine_scope_sha256")
    _diagnostic_hash_valid(
        quarantine["sha256sums_sha256"], "declaration_quarantine_sha256sums_sha256"
    )
    limits = _purpose_exact_object(
        declaration["limits"],
        frozenset(
            {"memory_limit_bytes", "memory_soft_limit_bytes", "max_contexts", "max_source_rows"}
        ),
        code="declaration_limits",
    )
    if (
        limits["memory_limit_bytes"] != DIAGNOSTIC_MEMORY_LIMIT_BYTES
        or limits["memory_soft_limit_bytes"] != DIAGNOSTIC_MEMORY_SOFT_LIMIT_BYTES
    ):
        raise NcenError("declaration_memory_limits_invalid")
    _purpose_positive(limits["max_contexts"], code="declaration_max_contexts")
    _purpose_positive(limits["max_source_rows"], code="declaration_max_source_rows")
    if len(contexts) > limits["max_contexts"]:
        raise NcenError("declaration_context_limit_exceeded")
    predecessor = declaration["predecessor_receipt_sha256"]
    if not historical and predecessor is not None:
        raise NcenError("diagnostic_resume_not_supported")
    if predecessor is not None:
        _diagnostic_hash_valid(predecessor, "declaration_predecessor_receipt_sha256")
    forbidden_tokens = {
        "candidate_y",
        "consensus_y",
        "default_label",
        "target_vote",
        "ratings_input",
        "agency_outcome",
        "cusip_target",
    }
    encoded = _diagnostic_canonical(declaration).decode("utf-8").lower()
    if any(token in encoded for token in forbidden_tokens):
        raise NcenError("declaration_outcome_input_forbidden")
    return declaration


def _purpose_source_index_bound(sources: DiagnosticSourceIndex) -> _DiagnosticSourceCustody:
    """Snapshot/export boundary: loader custody or ``diagnostic_source_index_unbound``."""
    return _diagnostic_bound_source_custody(sources)


def _purpose_apply_voting_gaps(
    index: NcenFilingIndex,
    selection: DiagnosticSelection,
    fund_keys: tuple[str, ...],
) -> DiagnosticSelection:
    if selection.accession_number is None:
        return selection
    matches = tuple(
        filing
        for filing in index.by_cik.get(selection.cik, ())
        if filing.accession_number == selection.accession_number
    )
    if len(matches) != 1:
        raise NcenError("diagnostic_selected_filing_identity_ambiguous")
    gaps = voting_series_gaps(matches[0], fund_keys)
    if not gaps:
        return selection
    reasons = tuple(sorted({*selection.reasons, *gaps}))
    return dataclasses.replace(selection, evidence_state="incomplete", reasons=reasons)


def iter_purpose_snapshots(
    index: NcenFilingIndex,
    cohort: DiagnosticCohort,
    sources: DiagnosticSourceIndex,
    *,
    declaration: Mapping[str, Any],
) -> Iterator[PurposeContextSnapshot]:
    """Extract each declared context once, then project the fixed ablation registry."""
    # Historical v1 remains usable for pure fixture selection, never export certification.
    declared = _purpose_declaration(declaration, legacy_fixture=True)
    custody = _purpose_source_index_bound(sources)
    if cohort._seal is not _DIAGNOSTIC_COHORT_SEAL:
        raise NcenError("diagnostic_cohort_unsealed")
    if (
        declared["inventory_digest"] != cohort.inventory_digest
        or declared["cohort_digest"] != cohort.cohort_digest
    ):
        raise NcenError("declaration_cohort_binding_mismatch")
    if declared["cohort_provenance"] != {
        "derivation": cohort.derivation,
        "full_cohort": cohort.full_cohort,
        "inventory_version": INVENTORY_VERSION,
        "sealed_inventory": True,
        "outcome_fields_excluded": cohort.outcome_fields_excluded,
        "knowledge_cutoff": _diagnostic_timestamp(cohort.knowledge_cutoff),
        "knowledge_mode": cohort.knowledge_mode,
        "inventory_sources": [
            _diagnostic_inventory_source_payload(source) for source in cohort.sources
        ],
    }:
        raise NcenError("declaration_cohort_provenance_binding_mismatch")
    source_manifest = declared["ncen_source_manifest"]
    if source_manifest != {
        "sha256": custody.manifest_sha256,
        "bytes": custody.manifest_size,
        "kind": custody.manifest_kind,
        "evidence_digest": custody.evidence_digest,
    }:
        raise NcenError("declaration_source_manifest_binding_mismatch")
    if declared["ncen_evidence_digest"] != custody.evidence_digest:
        raise NcenError("declaration_ncen_evidence_digest_mismatch")
    # One pass over the compact cohort; member order within a date is preserved.
    grouped: dict[dt.date, list[DiagnosticCohortMember]] = defaultdict(list)
    for member in cohort.members:
        grouped[member.report_date].append(member)
    by_date: dict[dt.date, tuple[DiagnosticCohortMember, ...]] = {
        report_date: tuple(grouped[report_date]) for report_date in sorted(grouped)
    }
    for context in declared["contexts"]:
        report_date = _purpose_parse_date(context["R"], code="declaration_report_date")
        cutoff = _purpose_parse_timestamp(context["K"], code="declaration_knowledge_cutoff")
        mode = context["mode"]
        if cutoff != cohort.knowledge_cutoff or mode != cohort.knowledge_mode:
            raise NcenError("declaration_cohort_time_or_mode_mismatch")
        members = by_date.get(report_date, ())
        if not members:
            raise NcenError("declaration_context_missing_cohort_members")
        context_id = diagnostic_context_id(
            report_date=report_date,
            knowledge_cutoff=cutoff,
            mode=mode,
            inventory_digest=cohort.inventory_digest,
            cohort_digest=cohort.cohort_digest,
            ncen_evidence_digest=declared["ncen_evidence_digest"],
            exclusion_ledger_digest=declared["exclusion_ledger_digest"],
        )
        selections: list[DiagnosticSelection] = []
        for member in members:
            selected = diagnostic_selection(
                index,
                sources,
                member.cik,
                report_date,
                cutoff,
                mode=mode,
                fund_keys=member.fund_keys,
            )
            selections.append(selected)
        graph = _diagnostic_build_context(
            tuple(selections), report_date=report_date, knowledge_cutoff=cutoff, mode=mode,
            inventory_digest=cohort.inventory_digest, cohort_digest=cohort.cohort_digest,
            ncen_evidence_digest=declared["ncen_evidence_digest"],
            exclusion_ledger_digest=declared["exclusion_ledger_digest"],
        )
        if graph.context_id != context_id:
            raise NcenError("diagnostic_graph_admission_missing")
        ordered_nodes = graph.nodes
        ordered_incidences = graph.incidences
        projections = tuple(
            project_dependence(ordered_nodes, ordered_incidences, spec=spec, admission=graph)
            for spec in DIAGNOSTIC_ABLATIONS
        )
        yield PurposeContextSnapshot(
            context_id=context_id,
            report_date=report_date,
            knowledge_cutoff=cutoff,
            mode=mode,
            inventory_digest=cohort.inventory_digest,
            cohort_members=members,
            selections=tuple(selections),
            reported_families=graph.reported_families,
            nodes=ordered_nodes,
            incidences=ordered_incidences,
            projections=projections,
        )


def _purpose_identifiers(row: DiagnosticSourceRow) -> list[dict[str, str | None]]:
    if row.role not in DIAGNOSTIC_PROVIDER_ROLES:
        return []
    normalizers = {
        "FN": normalize_file_number,
        "CRD": normalize_crd,
        "LEI": normalize_lei,
    }
    raw_values = {
        "FN": row.file_number_raw,
        "CRD": row.crd_raw,
        "LEI": row.lei_raw,
    }
    return [
        {
            "kind": kind,
            "raw": raw_values[kind] or None,
            "normalized": normalizers[kind](raw_values[kind]),
        }
        for kind in DIAGNOSTIC_IDENTIFIER_KINDS
    ]


def _purpose_source_records(
    sources: DiagnosticSourceIndex,
) -> tuple[tuple[dict[str, Any], ...], dict[str, str]]:
    custody = _diagnostic_bound_source_custody(sources)
    if any(row.source_kind == "synthetic" for row in custody.rows):
        raise NcenError("purpose_export_synthetic_source_row_unbound")
    by_internal_id = custody.row_by_id
    header_map: dict[str, str] = {}
    records_by_internal: dict[str, dict[str, Any]] = {}

    def build(row: DiagnosticSourceRow, header_id: str | None) -> dict[str, Any]:
        assert row.artifact_sha256 is not None
        assert row.artifact_path is not None
        assert row.raw_row_sha256 is not None
        return _purpose_record(
            "source_row",
            {
                "accession": row.accession_number,
                "cik": row.registrant_cik,
                "source_kind": row.source_kind,
                "artifact_sha256": row.artifact_sha256,
                "artifact_path": row.artifact_path,
                "member_path": row.member_path,
                "locator": row.locator,
                "raw_row_sha256": row.raw_row_sha256,
                "projection_digest": row.projection_digest,
                "series_id": row.series_id,
                "series_scope": row.series_scope,
                "role": row.role,
                "name_raw": row.name_raw,
                "name_key": normalize_reported_name_key(row.name_raw),
                "name_state": row.name_state,
                "answer_raw": row.answer_raw,
                "identifiers": _purpose_identifiers(row),
                "public_at": _diagnostic_optional_timestamp(row.public_at),
                "data_known_at": _diagnostic_optional_timestamp(row.data_known_at),
                "retrieved_at": _diagnostic_optional_timestamp(row.retrieved_at),
                "header_source_id": header_id,
                "acceptance_at": _diagnostic_optional_timestamp(row.acceptance_at),
                "custody_state": row.custody_state,
                "reasons": list(row.reasons),
            },
        )

    for internal_id, row in by_internal_id.items():
        if row.header_source_id is not None:
            continue
        record = build(row, None)
        records_by_internal[internal_id] = record
        if row.role == "header":
            header_map[internal_id] = record["record_id"]
    for internal_id, row in by_internal_id.items():
        if row.header_source_id is None:
            continue
        header_id = header_map.get(row.header_source_id)
        if header_id is None:
            raise NcenError("purpose_export_orphan_header_source")
        records_by_internal[internal_id] = build(row, header_id)
    id_map = {
        internal_id: record["record_id"] for internal_id, record in records_by_internal.items()
    }
    return tuple(sorted(records_by_internal.values(), key=lambda item: item["record_id"])), id_map


def _purpose_context_record(snapshot: PurposeContextSnapshot) -> dict[str, Any]:
    dependencies = tuple(
        dependency
        for selection in snapshot.selections
        for dependency in selection.dependencies
    )
    established = bool(dependencies) and all(item.knowledge_time is not None for item in dependencies)
    knowledge_candidates = [member.known_at for member in snapshot.cohort_members]
    if established:
        knowledge_candidates.extend(
            item.knowledge_time for item in dependencies if item.knowledge_time is not None
        )
    return _purpose_record(
        "context",
        {
            "context_id": snapshot.context_id,
            "R": snapshot.report_date.isoformat(),
            "K": _diagnostic_timestamp(snapshot.knowledge_cutoff),
            "mode": snapshot.mode,
            "inventory_digest": snapshot.inventory_digest,
            "cohort_digest": "",  # rebound by _purpose_build_records
            "ncen_evidence_digest": "",  # rebound by _purpose_build_records
            "exclusion_ledger_digest": "",  # rebound by _purpose_build_records
            "purpose_versions": {
                "reported_family": DIAGNOSTIC_REPORTED_FAMILY_VERSION,
                "reporting_dependence_block": DIAGNOSTIC_DEPENDENCE_VERSION,
            },
            "edge_version": DIAGNOSTIC_EDGE_VERSION,
            "selection_version": DIAGNOSTIC_SELECTION_VERSION,
            "normalizer": DIAGNOSTIC_NAME_NORMALIZER_VERSION,
            "node_count": len(snapshot.nodes),
            "dependency_time_established": established,
            "knowledge_time": _diagnostic_optional_timestamp(max(knowledge_candidates))
            if established
            else None,
            "diagnostic_only": True,
            "qualification": "NOT_EVALUABLE",
        },
    )


def _purpose_rebound_record(record: dict[str, Any], **changes: Any) -> dict[str, Any]:
    payload = {
        key: value
        for key, value in record.items()
        if key not in {"schema_version", "record_type", "record_id"}
    }
    payload.update(changes)
    return _purpose_record(record["record_type"], payload)


def _purpose_dependency_payload(dependency: SelectionDependency) -> dict[str, Any]:
    return {
        "accession": dependency.accession_number,
        "role": dependency.role,
        "knowledge_time": _diagnostic_optional_timestamp(dependency.knowledge_time),
    }


def _purpose_node_record(
    member: DiagnosticCohortMember,
    selection: DiagnosticSelection,
    node: DependenceNode,
    incidences: tuple[TypedIncidence, ...],
    source_ids: Mapping[str, str],
) -> dict[str, Any]:
    return _purpose_record(
        "node",
        {
            "context_id": node.context_id,
            "cik": node.cik,
            "selected_accession": selection.accession_number,
            "selected_projection_digest": selection.selected_projection_digest,
            "voting_series": list(member.fund_keys),
            "evidence_state": node.evidence_state,
            "reasons": list(node.reasons),
            "selection_reason": selection.selection_reason,
            "source_row_ids": sorted(source_ids[item.source_row_id] for item in selection.rows),
            "dependencies": [
                _purpose_dependency_payload(item)
                for item in sorted(
                    selection.dependencies,
                    key=lambda item: (item.role, item.accession_number),
                )
            ],
            "has_unknown_dependence": node.evidence_state == "incomplete"
            or any(item.cik == node.cik and item.attestation == "uncertain" for item in incidences),
            "independent_vote_eligible": False,
        },
    )


def _purpose_family_record(
    context_id: str,
    family: ReportedFamily,
    source_ids: Mapping[str, str],
) -> dict[str, Any]:
    return _purpose_record(
        "reported_family",
        {
            "context_id": context_id,
            "purpose": "reported_family",
            "rule_version": DIAGNOSTIC_REPORTED_FAMILY_VERSION,
            "cik": family.cik,
            "state": family.state,
            "answer": family.answer,
            "name_raw": family.name_raw,
            "name_key": family.name_key,
            "label_id": family.label_id,
            "source_row_ids": sorted(source_ids[item] for item in family.source_row_ids),
            "normalizer": family.normalizer,
            "reasons": list(family.reasons),
            "claim": family.claim,
        },
    )


def _purpose_incidence_record(
    incidence: TypedIncidence,
    source_ids: Mapping[str, str],
) -> dict[str, Any]:
    return _purpose_record(
        "incidence",
        {
            "context_id": incidence.context_id,
            "purpose": "reporting_dependence_block",
            "cik": incidence.cik,
            "accession": incidence.accession_number,
            "series_id": incidence.series_id,
            "series_scope": incidence.series_scope,
            "kind": incidence.kind,
            "role": incidence.role,
            "identifier_kind": incidence.identifier_kind,
            "identifier_value": incidence.identifier_value,
            "key_id": incidence.key_id,
            "source_row_id": source_ids[incidence.source_row_id],
            "attestation": incidence.attestation,
            "reasons": list(incidence.reasons),
            "uncertain_expansion_eligible": incidence.uncertain_expansion_eligible,
        },
    )


_PURPOSE_CONTEXTUAL_SOURCE_FIELDS = frozenset(
    {"attestation", "reasons", "uncertain_expansion_eligible"}
)


def _purpose_identity_value(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return _diagnostic_timestamp(value)
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_purpose_identity_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _purpose_identity_value(item) for key, item in value.items()}
    return value


def _purpose_source_witness_key(row: DiagnosticSourceRow) -> bytes:
    return _diagnostic_canonical(
        {
            item.name: _purpose_identity_value(getattr(row, item.name))
            for item in dataclasses.fields(row)
            if item.name not in _PURPOSE_CONTEXTUAL_SOURCE_FIELDS
        }
    )


def _purpose_contextual_source_ids(
    snapshot: PurposeContextSnapshot,
    sources: DiagnosticSourceIndex,
    source_ids: Mapping[str, str],
) -> Mapping[str, str]:
    """Map context-modified selection rows back to their loader row, one accession at a time.

    Only rows whose contextual fields changed (uncertain attestation) need a witness match;
    candidates come from the same accession via the custody lookup, never a whole-index scan.
    """
    from collections import ChainMap

    custody = _diagnostic_bound_source_custody(sources)
    overlay: dict[str, str] = {}
    witnesses_by_accession: dict[str, dict[bytes, str]] = {}
    for selection in snapshot.selections:
        for source in selection.rows:
            source_id = source.source_row_id
            if source_id in source_ids or source_id in overlay:
                continue
            witnesses = witnesses_by_accession.get(source.accession_number)
            if witnesses is None:
                witnesses = {}
                for candidate, candidate_id in custody.rows_with_ids_for(source.accession_number):
                    key = _purpose_source_witness_key(candidate)
                    if key in witnesses:
                        raise NcenError("purpose_export_source_witness_ambiguous")
                    witnesses[key] = candidate_id
                witnesses_by_accession[source.accession_number] = witnesses
            base_id = witnesses.get(_purpose_source_witness_key(source))
            if base_id is None:
                raise NcenError("purpose_export_contextual_source_unbound")
            overlay[source_id] = source_ids[base_id]
    # Read-only overlay: callers only look IDs up, so the shared global map is never copied.
    return ChainMap(overlay, source_ids)  # type: ignore[arg-type]


def _purpose_excluded_incidence_count(
    snapshot: PurposeContextSnapshot,
    sources: DiagnosticSourceIndex,
) -> int:
    by_id = _diagnostic_bound_source_custody(sources).row_by_id
    excluded: set[tuple[str, str, str]] = set()
    for selection in snapshot.selections:
        for source_id in selection.excluded_source_row_ids:
            row = by_id.get(source_id)
            if row is None:
                raise NcenError("purpose_export_excluded_source_missing")
            if row.role == "b5":
                key = normalize_reported_name_key(row.name_raw)
                if row.answer_raw == "Y" and key is not None:
                    excluded.add((source_id, "name", key))
                continue
            for identifier_kind, identifier_value in row.normalized_identifiers():
                excluded.add((source_id, identifier_kind, identifier_value))
    return len(excluded)


def _purpose_exclusion_records(
    snapshots: tuple[PurposeContextSnapshot, ...],
    sources: DiagnosticSourceIndex,
    source_ids: Mapping[str, str],
    *,
    include_ledger: bool = True,
) -> tuple[dict[str, Any], ...]:
    """Context-free quarantine ledger records (optional) plus per-context exclusions."""
    custody = _diagnostic_bound_source_custody(sources)
    output: dict[str, dict[str, Any]] = {}
    for item in custody.exclusions if include_ledger else ():
        record = _purpose_record(
            "source_exclusion",
            {
                "context_id": None,
                "accession": item.accession_number,
                "source_row_ids": [],
                "state": "excluded_quarantine",
                "reasons": list(item.reasons),
            },
        )
        output[record["record_id"]] = record
    by_id = custody.row_by_id
    for snapshot in snapshots:
        for selection in snapshot.selections:
            for internal_id in selection.excluded_source_row_ids:
                row = by_id.get(internal_id)
                if row is None:
                    raise NcenError("purpose_export_excluded_source_missing")
                if row.custody_state != "verified":
                    state = "unavailable"
                elif (
                    row.public_at is None
                    or row.data_known_at is None
                    or row.public_at > snapshot.knowledge_cutoff
                    or row.data_known_at > snapshot.knowledge_cutoff
                ):
                    state = "excluded_after_cutoff"
                elif snapshot.mode == KNOWLEDGE_CURRENT_RUN and (
                    row.retrieved_at is None or row.retrieved_at > snapshot.knowledge_cutoff
                ):
                    state = "excluded_unheld"
                else:
                    state = "unavailable"
                reasons = tuple(
                    sorted(
                        {
                            *row.reasons,
                            selection.selection_reason or "diagnostic_source_unavailable",
                        }
                    )
                )
                record = _purpose_record(
                    "source_exclusion",
                    {
                        "context_id": snapshot.context_id,
                        "accession": row.accession_number,
                        "source_row_ids": [source_ids[internal_id]],
                        "state": state,
                        "reasons": list(reasons),
                    },
                )
                output[record["record_id"]] = record
    return tuple(sorted(output.values(), key=lambda item: item["record_id"]))


def _purpose_component_records(
    projection: DependenceProjection,
    incidence_records: Mapping[str, dict[str, Any]],
) -> tuple[
    tuple[dict[str, Any], ...],
    tuple[dict[str, Any], ...],
    tuple[dict[str, Any], ...],
    tuple[dict[str, Any], ...],
    dict[str, Any],
]:
    components: list[dict[str, Any]] = []
    memberships: list[dict[str, Any]] = []
    node_by_cik = {node.cik: node for node in projection.nodes}
    enabled_by_cik: dict[str, list[TypedIncidence]] = defaultdict(list)
    for incidence in projection.enabled_incidences:
        enabled_by_cik[incidence.cik].append(incidence)
    for component in projection.components:
        component_incidences = [
            incidence for cik in component.members for incidence in enabled_by_cik[cik]
        ]
        incident_ids = sorted(
            incidence_records[item.incidence_id]["record_id"]
            for item in component_incidences
        )
        components.append(
            _purpose_record(
                "component",
                {
                    "context_id": component.context_id,
                    "ablation_id": component.ablation_id,
                    "purpose": "reporting_dependence_block",
                    "snapshot_component_id": component.snapshot_component_id,
                    "membership_track_id": component.membership_track_id,
                    "observed_edge_version_id": component.observed_edge_version_id,
                    "evidence_digest": _diagnostic_hash(
                        [component.context_id, component.ablation_id, incident_ids]
                    ),
                    "member_count": component.member_count,
                    "complete_count": component.complete_count,
                    "incomplete_count": component.incomplete_count,
                    "distinct_reported_y_keys": component.distinct_reported_y_keys,
                    "has_unknown_dependence": component.has_unknown_dependence,
                    "independent_vote_count": None,
                },
            )
        )
        for cik in component.members:
            memberships.append(
                _purpose_record(
                    "membership",
                    {
                        "context_id": component.context_id,
                        "ablation_id": component.ablation_id,
                        "snapshot_component_id": component.snapshot_component_id,
                        "cik": cik,
                        "evidence_state": node_by_cik[cik].evidence_state,
                    },
                )
            )
    key_degrees = tuple(
        _purpose_record(
            "key_degree",
            {
                "context_id": projection.context_id,
                "ablation_id": projection.ablation_id,
                "key_id": item.key_id,
                "kind": item.kind,
                "distinct_registrants": item.distinct_registrants,
                "complete_registrants": item.complete_registrants,
                "incomplete_registrants": item.incomplete_registrants,
                "incidence_count": item.incidence_count,
            },
        )
        for item in projection.key_degrees
    )
    spanning = tuple(
        _purpose_record(
            "spanning_union",
            {
                "context_id": projection.context_id,
                "ablation_id": projection.ablation_id,
                "key_id": item.key_id,
                "left_cik": item.left_cik,
                "right_cik": item.right_cik,
                "left_incidence_id": incidence_records[item.left_incidence_id]["record_id"],
                "right_incidence_id": incidence_records[item.right_incidence_id]["record_id"],
            },
        )
        for item in projection.spanning_unions
    )
    summary = _purpose_record(
        "summary",
        {
            "context_id": projection.context_id,
            "ablation_id": projection.ablation_id,
            "node_count": len(projection.nodes),
            "complete_count": sum(node.evidence_state == "complete" for node in projection.nodes),
            "incomplete_count": sum(node.evidence_state == "incomplete" for node in projection.nodes),
            "component_count": len(projection.components),
            "largest_all": projection.largest_all,
            "largest_complete": projection.largest_complete,
            "complete_square_sum": projection.complete_square_sum,
            "complete_total": projection.complete_total,
            "uncertain_incidence_count": projection.uncertain_incidence_count,
            "excluded_incidence_count": 0,  # rebound by _purpose_build_records
        },
    )
    return (
        tuple(components),
        tuple(memberships),
        key_degrees,
        spanning,
        summary,
    )


def _purpose_transition_record(item: DependenceTransition) -> dict[str, Any]:
    return _purpose_record(
        "transition",
        {
            "ablation_id": item.ablation_id,
            "from_context_id": item.from_context_id,
            "to_context_id": item.to_context_id,
            "from_component_id": item.from_component_id,
            "to_component_id": item.to_component_id,
            "intersection_count": item.intersection_count,
            "union_count": item.union_count,
            "kind": item.kind,
        },
    )


def _purpose_fold_records(
    snapshots: tuple[PurposeContextSnapshot, ...],
) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]:
    groups: list[dict[str, Any]] = []
    memberships: list[dict[str, Any]] = []
    for spec in DIAGNOSTIC_ABLATIONS:
        stream = tuple(
            next(
                item
                for item in snapshot.dependence_snapshots()
                if item.projection.ablation_id == spec.ablation_id
            )
            for snapshot in snapshots
        )
        contexts = tuple(
            FoldContext(
                report_date=item.report_date,
                knowledge_cutoff=item.knowledge_cutoff,
                inventory_digest=item.inventory_digest,
                context_id=item.projection.context_id,
            )
            for item in stream
        )
        scope = TemporalFoldScope(
            mode=stream[0].mode,
            ablation_id=spec.ablation_id,
            contexts=contexts,
        )
        folded = build_temporal_dependence_union(stream, fold_scope=scope)
        for group in folded.groups:
            groups.append(
                _purpose_record(
                    "fold_group",
                    {
                        "fold_scope_id": folded.fold_scope_id,
                        "fold_group_id": group.fold_group_id,
                        "member_count": group.member_count,
                        "has_unknown_dependence": group.has_unknown_dependence,
                        "usable_for_independence_claim": False,
                    },
                )
            )
            for cik in group.members:
                memberships.append(
                    _purpose_record(
                        "fold_membership",
                        {
                            "fold_scope_id": folded.fold_scope_id,
                            "fold_group_id": group.fold_group_id,
                            "cik": cik,
                        },
                    )
                )
    return tuple(groups), tuple(memberships)


class _PurposeFoldAccumulator:
    def __init__(self, spec: AblationSpec) -> None:
        self.spec = spec
        self.mode: str | None = None
        self.contexts: list[FoldContext] = []
        self.parent: dict[str, str] = {}
        self.tainted: set[str] = set()
        self.ciks: set[str] = set()

    def _find(self, value: str) -> str:
        self.parent.setdefault(value, value)
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def _union(self, left: str, right: str) -> None:
        left_root = self._find(left)
        right_root = self._find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root

    def add(self, snapshot: DependenceSnapshot) -> None:
        if snapshot.projection.ablation_id != self.spec.ablation_id:
            raise NcenError("purpose_fold_accumulator_ablation_mismatch")
        if self.mode is None:
            self.mode = snapshot.mode
        elif self.mode != snapshot.mode:
            raise NcenError("purpose_fold_accumulator_mode_mismatch")
        self.contexts.append(
            FoldContext(
                report_date=snapshot.report_date,
                knowledge_cutoff=snapshot.knowledge_cutoff,
                inventory_digest=snapshot.inventory_digest,
                context_id=snapshot.projection.context_id,
            )
        )
        for node in snapshot.projection.nodes:
            self.ciks.add(node.cik)
            self._find(f"cik:{node.cik}")
            if node.evidence_state == "incomplete":
                self.tainted.add(node.cik)
        for incidence in snapshot.projection.incidences:
            if incidence.attestation == "uncertain":
                self.tainted.add(incidence.cik)
        for incidence in snapshot.projection.enabled_incidences:
            if not _diagnostic_fold_edge(incidence.attestation, incidence.key_id, True):
                continue
            self._union(f"cik:{incidence.cik}", f"key:{incidence.key_id}")

    def records(self) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]:
        if self.mode is None or not self.contexts:
            raise NcenError("purpose_fold_accumulator_empty")
        scope_id = fold_scope_id(
            mode=self.mode,
            ablation_id=self.spec.ablation_id,
            contexts=tuple(
                (
                    item.report_date,
                    item.knowledge_cutoff,
                    item.inventory_digest,
                    item.context_id,
                )
                for item in self.contexts
            ),
        )
        by_root: dict[str, list[str]] = defaultdict(list)
        for cik in sorted(self.ciks):
            by_root[self._find(f"cik:{cik}")].append(cik)
        groups: list[dict[str, Any]] = []
        memberships: list[dict[str, Any]] = []
        for raw_members in by_root.values():
            members = tuple(sorted(raw_members))
            group_id = fold_group_id(scope_id=scope_id, members=members)
            groups.append(
                _purpose_record(
                    "fold_group",
                    {
                        "fold_scope_id": scope_id,
                        "fold_group_id": group_id,
                        "member_count": len(members),
                        "has_unknown_dependence": bool(set(members) & self.tainted),
                        "usable_for_independence_claim": False,
                    },
                )
            )
            memberships.extend(
                _purpose_record(
                    "fold_membership",
                    {
                        "fold_scope_id": scope_id,
                        "fold_group_id": group_id,
                        "cik": cik,
                    },
                )
                for cik in members
            )
        return tuple(groups), tuple(memberships)


def _purpose_context_records(
    snapshot: PurposeContextSnapshot,
    *,
    cohort: DiagnosticCohort,
    sources: DiagnosticSourceIndex,
    declaration: Mapping[str, Any],
    source_ids: Mapping[str, str],
) -> tuple[dict[str, tuple[dict[str, Any], ...]], dict[str, int]]:
    for projection in snapshot.projections:
        bound = _diagnostic_bound_projection_admission(projection)
        if bound.lane == "pure_fixture":
            raise NcenError("diagnostic_fixture_lane_not_exportable")
    output: dict[str, list[dict[str, Any]]] = {
        path: [] for path, _record_type in DIAGNOSTIC_EXPORT_JSONL
    }
    contextual_source_ids = _purpose_contextual_source_ids(snapshot, sources, source_ids)
    context = _purpose_rebound_record(
        _purpose_context_record(snapshot),
        cohort_digest=cohort.cohort_digest,
        ncen_evidence_digest=declaration["ncen_evidence_digest"],
        exclusion_ledger_digest=declaration["exclusion_ledger_digest"],
    )
    output["contexts.jsonl"].append(context)
    incidence_records_by_internal: dict[str, dict[str, Any]] = {}
    for member, selection, family, node in zip(
        snapshot.cohort_members,
        snapshot.selections,
        snapshot.reported_families,
        snapshot.nodes,
        strict=True,
    ):
        output["nodes.jsonl"].append(
            _purpose_node_record(
                member,
                selection,
                node,
                snapshot.incidences,
                contextual_source_ids,
            )
        )
        output["reported_families.jsonl"].append(
            _purpose_family_record(snapshot.context_id, family, contextual_source_ids)
        )
    for incidence in snapshot.incidences:
        record = _purpose_incidence_record(incidence, contextual_source_ids)
        incidence_records_by_internal[incidence.incidence_id] = record
        output["incidences.jsonl"].append(record)
    excluded_count = _purpose_excluded_incidence_count(snapshot, sources)
    union_attempts = 0
    successful_unions = 0
    for projection in snapshot.projections:
        component_rows, membership_rows, degree_rows, spanning_rows, summary = (
            _purpose_component_records(projection, incidence_records_by_internal)
        )
        output["components.jsonl"].extend(component_rows)
        output["memberships.jsonl"].extend(membership_rows)
        output["key_degrees.jsonl"].extend(degree_rows)
        output["spanning.jsonl"].extend(spanning_rows)
        output["summaries.jsonl"].append(
            _purpose_rebound_record(summary, excluded_incidence_count=excluded_count)
        )
        union_attempts += projection.union_attempts
        successful_unions += projection.successful_unions
    output["exclusions.jsonl"].extend(
        item
        for item in _purpose_exclusion_records(
            (snapshot,), sources, contextual_source_ids, include_ledger=False
        )
        if item["context_id"] is not None
    )
    ablation_order = {
        spec.ablation_id: index for index, spec in enumerate(DIAGNOSTIC_ABLATIONS)
    }

    def row_order(item: Mapping[str, Any]) -> tuple[int, str]:
        ablation_id = item.get("ablation_id")
        return (
            -1 if ablation_id is None else ablation_order[ablation_id],
            item["record_id"],
        )

    for rows in output.values():
        rows.sort(key=row_order)
    return (
        {path: tuple(rows) for path, rows in output.items()},
        {
            "incidences_emitted": len(output["incidences.jsonl"]),
            "union_attempts": union_attempts,
            "successful_unions": successful_unions,
        },
    )


def _purpose_build_records(
    cohort: DiagnosticCohort,
    sources: DiagnosticSourceIndex,
    declaration: Mapping[str, Any],
    snapshots: tuple[PurposeContextSnapshot, ...],
) -> tuple[dict[str, tuple[dict[str, Any], ...]], dict[str, int]]:
    for snapshot in snapshots:
        for projection in snapshot.projections:
            bound = _diagnostic_bound_projection_admission(projection)
            if bound.lane == "pure_fixture":
                raise NcenError("diagnostic_fixture_lane_not_exportable")
    declared = _purpose_declaration(declaration)
    if len(snapshots) != len(declared["contexts"]):
        raise NcenError("purpose_export_context_count_mismatch")
    records: dict[str, list[dict[str, Any]]] = {
        path: [] for path, _record_type in DIAGNOSTIC_EXPORT_JSONL
    }
    source_records, source_ids = _purpose_source_records(sources)
    records["sources.jsonl"].extend(source_records)
    records["cohort.jsonl"].extend(
        _purpose_cohort_record(member, cohort.inventory_digest) for member in cohort.members
    )
    incidence_records_by_internal: dict[str, dict[str, Any]] = {}
    union_attempts = 0
    successful_unions = 0
    for snapshot in snapshots:
        contextual_source_ids = _purpose_contextual_source_ids(snapshot, sources, source_ids)
        context = _purpose_context_record(snapshot)
        context = _purpose_rebound_record(
            context,
            cohort_digest=cohort.cohort_digest,
            ncen_evidence_digest=declared["ncen_evidence_digest"],
            exclusion_ledger_digest=declared["exclusion_ledger_digest"],
        )
        records["contexts.jsonl"].append(context)
        for member, selection, family, node in zip(
            snapshot.cohort_members,
            snapshot.selections,
            snapshot.reported_families,
            snapshot.nodes,
            strict=True,
        ):
            records["nodes.jsonl"].append(
                _purpose_node_record(
                    member,
                    selection,
                    node,
                    snapshot.incidences,
                    contextual_source_ids,
                )
            )
            records["reported_families.jsonl"].append(
                _purpose_family_record(snapshot.context_id, family, contextual_source_ids)
            )
        for incidence in snapshot.incidences:
            record = _purpose_incidence_record(incidence, contextual_source_ids)
            incidence_records_by_internal[incidence.incidence_id] = record
            records["incidences.jsonl"].append(record)
        excluded_count = _purpose_excluded_incidence_count(snapshot, sources)
        for projection in snapshot.projections:
            component_rows, membership_rows, degree_rows, spanning_rows, summary = (
                _purpose_component_records(projection, incidence_records_by_internal)
            )
            records["components.jsonl"].extend(component_rows)
            records["memberships.jsonl"].extend(membership_rows)
            records["key_degrees.jsonl"].extend(degree_rows)
            records["spanning.jsonl"].extend(spanning_rows)
            records["summaries.jsonl"].append(
                _purpose_rebound_record(summary, excluded_incidence_count=excluded_count)
            )
            union_attempts += projection.union_attempts
            successful_unions += projection.successful_unions
    records["exclusions.jsonl"].extend(
        _purpose_exclusion_records(snapshots, sources, source_ids)
    )
    for spec in DIAGNOSTIC_ABLATIONS:
        stream = tuple(
            next(
                item
                for item in snapshot.dependence_snapshots()
                if item.projection.ablation_id == spec.ablation_id
            )
            for snapshot in snapshots
        )
        records["transitions.jsonl"].extend(
            _purpose_transition_record(item) for item in build_dependence_transitions(stream)
        )
    fold_groups, fold_memberships = _purpose_fold_records(snapshots)
    records["fold_groups.jsonl"].extend(fold_groups)
    records["fold_memberships.jsonl"].extend(fold_memberships)
    return (
        {path: tuple(rows) for path, rows in records.items()},
        {
            "incidences_emitted": len(records["incidences.jsonl"]),
            "union_attempts": union_attempts,
            "successful_unions": successful_unions,
        },
    )


def _purpose_context_order(
    declaration: Mapping[str, Any],
    records: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    declared = _purpose_declaration(declaration)
    context_rows = {
        (row["mode"], row["R"], row["K"]): row["context_id"]
        for row in records["contexts.jsonl"]
    }
    ordered_contexts: list[str] = []
    for item in declared["contexts"]:
        key = (item["mode"], item["R"], item["K"])
        if key not in context_rows:
            raise NcenError("purpose_export_declared_context_missing")
        ordered_contexts.append(context_rows[key])
    context_index = {value: index for index, value in enumerate(ordered_contexts)}
    ablation_index = {
        spec.ablation_id: index for index, spec in enumerate(DIAGNOSTIC_ABLATIONS)
    }
    report_index = {
        item["R"]: index
        for index, item in enumerate(declared["contexts"])
    }
    return context_index, ablation_index, report_index


def _purpose_sort_records(
    path: str,
    rows: Sequence[dict[str, Any]],
    *,
    context_index: Mapping[str, int],
    ablation_index: Mapping[str, int],
    report_index: Mapping[str, int],
) -> tuple[dict[str, Any], ...]:
    def key(row: dict[str, Any]) -> tuple[Any, ...]:
        if path == "cohort.jsonl":
            return (row["record_id"],)
        if path == "transitions.jsonl":
            return (
                context_index[row["from_context_id"]],
                context_index[row["to_context_id"]],
                ablation_index[row["ablation_id"]],
                row["record_id"],
            )
        if path in {"fold_groups.jsonl", "fold_memberships.jsonl"}:
            return (row["record_id"],)
        context_id = row.get("context_id")
        if context_id is None:
            return (-1, -1, row["record_id"])
        ablation_id = row.get("ablation_id")
        return (
            context_index[context_id],
            -1 if ablation_id is None else ablation_index[ablation_id],
            row["record_id"],
        )

    ordered = tuple(sorted(rows, key=key))
    ids = tuple(row["record_id"] for row in ordered)
    if len(ids) != len(set(ids)):
        raise NcenError(f"purpose_export_duplicate_record_id:{path}")
    return ordered


def _purpose_canonical_records(
    declaration: Mapping[str, Any],
    records: Mapping[str, Sequence[dict[str, Any]]],
) -> dict[str, tuple[dict[str, Any], ...]]:
    context_index, ablation_index, report_index = _purpose_context_order(
        declaration, records
    )
    return {
        path: _purpose_sort_records(
            path,
            rows,
            context_index=context_index,
            ablation_index=ablation_index,
            report_index=report_index,
        )
        for path, rows in records.items()
    }


def _purpose_json_bytes(value: Any) -> bytes:
    return _diagnostic_canonical(value) + b"\n"


def _purpose_jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return b"".join(_diagnostic_canonical(dict(row)) + b"\n" for row in rows)


def _purpose_issuer_group_ref() -> dict[str, Any]:
    return dataclasses.asdict(BondIssuerGroupRef())


_PURPOSE_PAYLOAD_KEYS: dict[str, frozenset[str]] = {
    "selection_context": _PURPOSE_SELECTION_CONTEXT_KEYS,
    "cohort_member": frozenset({"inventory_digest", "R", "cik", "fund_keys", "known_at"}),
    "source_row": frozenset(
        {
            "accession",
            "cik",
            "source_kind",
            "artifact_sha256",
            "artifact_path",
            "member_path",
            "locator",
            "raw_row_sha256",
            "projection_digest",
            "series_id",
            "series_scope",
            "role",
            "name_raw",
            "name_key",
            "name_state",
            "answer_raw",
            "identifiers",
            "public_at",
            "data_known_at",
            "retrieved_at",
            "header_source_id",
            "acceptance_at",
            "custody_state",
            "reasons",
        }
    ),
    "source_exclusion": frozenset(
        {"context_id", "accession", "source_row_ids", "state", "reasons"}
    ),
    "context": frozenset(
        {
            "context_id",
            "R",
            "K",
            "mode",
            "inventory_digest",
            "cohort_digest",
            "ncen_evidence_digest",
            "exclusion_ledger_digest",
            "purpose_versions",
            "edge_version",
            "selection_version",
            "normalizer",
            "node_count",
            "dependency_time_established",
            "knowledge_time",
            "diagnostic_only",
            "qualification",
        }
    ),
    "node": frozenset(
        {
            "context_id",
            "cik",
            "selected_accession",
            "selected_projection_digest",
            "voting_series",
            "evidence_state",
            "reasons",
            "selection_reason",
            "source_row_ids",
            "dependencies",
            "has_unknown_dependence",
            "independent_vote_eligible",
        }
    ),
    "reported_family": frozenset(
        {
            "context_id",
            "purpose",
            "rule_version",
            "cik",
            "state",
            "answer",
            "name_raw",
            "name_key",
            "label_id",
            "source_row_ids",
            "normalizer",
            "reasons",
            "claim",
        }
    ),
    "incidence": frozenset(
        {
            "context_id",
            "purpose",
            "cik",
            "accession",
            "series_id",
            "series_scope",
            "kind",
            "role",
            "identifier_kind",
            "identifier_value",
            "key_id",
            "source_row_id",
            "attestation",
            "reasons",
            "uncertain_expansion_eligible",
        }
    ),
    "component": frozenset(
        {
            "context_id",
            "ablation_id",
            "purpose",
            "snapshot_component_id",
            "membership_track_id",
            "observed_edge_version_id",
            "evidence_digest",
            "member_count",
            "complete_count",
            "incomplete_count",
            "distinct_reported_y_keys",
            "has_unknown_dependence",
            "independent_vote_count",
        }
    ),
    "membership": frozenset(
        {"context_id", "ablation_id", "snapshot_component_id", "cik", "evidence_state"}
    ),
    "key_degree": frozenset(
        {
            "context_id",
            "ablation_id",
            "key_id",
            "kind",
            "distinct_registrants",
            "complete_registrants",
            "incomplete_registrants",
            "incidence_count",
        }
    ),
    "spanning_union": frozenset(
        {
            "context_id",
            "ablation_id",
            "key_id",
            "left_cik",
            "right_cik",
            "left_incidence_id",
            "right_incidence_id",
        }
    ),
    "summary": frozenset(
        {
            "context_id",
            "ablation_id",
            "node_count",
            "complete_count",
            "incomplete_count",
            "component_count",
            "largest_all",
            "largest_complete",
            "complete_square_sum",
            "complete_total",
            "uncertain_incidence_count",
            "excluded_incidence_count",
        }
    ),
    "transition": frozenset(
        {
            "ablation_id",
            "from_context_id",
            "to_context_id",
            "from_component_id",
            "to_component_id",
            "intersection_count",
            "union_count",
            "kind",
        }
    ),
    "fold_group": frozenset(
        {
            "fold_scope_id",
            "fold_group_id",
            "member_count",
            "has_unknown_dependence",
            "usable_for_independence_claim",
        }
    ),
    "fold_membership": frozenset({"fold_scope_id", "fold_group_id", "cik"}),
}
_PURPOSE_COMMON_KEYS = frozenset({"schema_version", "record_type", "record_id"})


def _purpose_nullable_string(value: Any, *, code: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise NcenError(f"{code}_invalid")
    return value


def _purpose_nullable_hash(value: Any, *, code: str) -> str | None:
    if value is None:
        return None
    _diagnostic_hash_valid(value, code)
    return value


def _purpose_record_reasons(record: Mapping[str, Any]) -> tuple[str, ...]:
    return _purpose_sorted_strings(record["reasons"], code="diagnostic_reasons")


def _purpose_validate_record_shape(
    record: Any,
    *,
    expected_type: str,
) -> dict[str, Any]:
    if expected_type not in _PURPOSE_PAYLOAD_KEYS:
        raise NcenError("diagnostic_record_type_unknown")
    if expected_type == "selection_context":
        row = _purpose_exact_object(
            record, _PURPOSE_COMMON_KEYS | _PURPOSE_SELECTION_CONTEXT_KEYS,
            code="diagnostic_selection_records_invalid",
        )
        if row != _purpose_selection_context_record({
            key: value for key, value in row.items() if key not in _PURPOSE_COMMON_KEYS
        }):
            raise NcenError("diagnostic_selection_records_invalid")
        return row
    expected_keys = _PURPOSE_COMMON_KEYS | _PURPOSE_PAYLOAD_KEYS[expected_type]
    row = _purpose_exact_object(record, expected_keys, code=f"diagnostic_{expected_type}")
    if row["schema_version"] != DIAGNOSTIC_SCHEMA_VERSION or row["record_type"] != expected_type:
        raise NcenError(f"diagnostic_{expected_type}_common_fields_invalid")
    payload = {key: value for key, value in row.items() if key not in _PURPOSE_COMMON_KEYS}
    if row != _purpose_record(expected_type, payload):
        raise NcenError(f"diagnostic_{expected_type}_record_id_mismatch")
    validator = globals()[f"_purpose_validate_{expected_type}"]
    validator(row)
    return row


def _purpose_validate_cohort_member(row: Mapping[str, Any]) -> None:
    _diagnostic_nonempty(row["inventory_digest"], "cohort_inventory_digest")
    _purpose_parse_date(row["R"], code="cohort_report_date")
    _diagnostic_normalized_cik(row["cik"])
    _purpose_sorted_strings(row["fund_keys"], code="cohort_fund_keys", allow_empty=False)
    if row["known_at"] != _diagnostic_timestamp(
        _purpose_parse_timestamp(row["known_at"], code="cohort_known_at")
    ):
        raise NcenError("cohort_known_at_not_canonical")


def _purpose_validate_source_row(row: Mapping[str, Any]) -> None:
    if _ACCESSION.fullmatch(row["accession"]) is None:
        raise NcenError("source_accession_invalid")
    if row["cik"] is not None:
        _diagnostic_normalized_cik(row["cik"])
    if row["source_kind"] not in {"dera", "edgar_xml", "header", "index"}:
        raise NcenError("source_kind_invalid")
    _diagnostic_hash_valid(row["artifact_sha256"], "source_artifact_sha256")
    _purpose_relative_path_value(row["artifact_path"], code="source_artifact")
    if row["member_path"] is not None:
        _purpose_relative_path_value(row["member_path"], code="source_member")
    _diagnostic_nonempty(row["locator"], "source_locator")
    _diagnostic_hash_valid(row["raw_row_sha256"], "source_raw_row_sha256")
    _purpose_nullable_hash(row["projection_digest"], code="source_projection_digest")
    if row["series_scope"] not in {"series", "registrant", "unresolved"}:
        raise NcenError("source_series_scope_invalid")
    if row["role"] not in _DIAGNOSTIC_SOURCE_ROLES:
        raise NcenError("source_role_invalid")
    _purpose_nullable_string(row["name_raw"], code="source_name_raw")
    _purpose_nullable_string(row["name_key"], code="source_name_key")
    if row["name_state"] not in {"present", "absent", "unavailable"}:
        raise NcenError("source_name_state_invalid")
    _purpose_nullable_string(row["answer_raw"], code="source_answer_raw")
    identifiers = row["identifiers"]
    if row["role"] in DIAGNOSTIC_PROVIDER_ROLES:
        if not isinstance(identifiers, list) or [item.get("kind") for item in identifiers] != list(
            DIAGNOSTIC_IDENTIFIER_KINDS
        ):
            raise NcenError("source_identifiers_order_invalid")
        for item in identifiers:
            nested = _purpose_exact_object(
                item,
                frozenset({"kind", "raw", "normalized"}),
                code="source_identifier",
            )
            _purpose_nullable_string(nested["raw"], code="source_identifier_raw")
            _purpose_nullable_string(nested["normalized"], code="source_identifier_normalized")
    elif identifiers != []:
        raise NcenError("source_nonprovider_identifiers_forbidden")
    for field_name in (
        "public_at",
        "data_known_at",
        "retrieved_at",
        "acceptance_at",
    ):
        if row[field_name] is not None and row[field_name] != _diagnostic_timestamp(
            _purpose_parse_timestamp(row[field_name], code=f"source_{field_name}")
        ):
            raise NcenError(f"source_{field_name}_not_canonical")
    if row["header_source_id"] is not None and re.fullmatch(
        r"ncenrow:source_row:[0-9a-f]{64}", row["header_source_id"]
    ) is None:
        raise NcenError("source_header_id_invalid")
    if row["custody_state"] not in {"verified", "quarantined", "unavailable"}:
        raise NcenError("source_custody_state_invalid")
    _purpose_record_reasons(row)


def _purpose_validate_source_exclusion(row: Mapping[str, Any]) -> None:
    if row["context_id"] is not None:
        _diagnostic_context_id_valid(row["context_id"])
    if _ACCESSION.fullmatch(row["accession"]) is None:
        raise NcenError("source_exclusion_accession_invalid")
    _purpose_sorted_strings(row["source_row_ids"], code="source_exclusion_rows")
    if row["state"] not in {
        "excluded_quarantine",
        "excluded_after_cutoff",
        "excluded_unheld",
        "unavailable",
    }:
        raise NcenError("source_exclusion_state_invalid")
    _purpose_record_reasons(row)


def _purpose_validate_context(row: Mapping[str, Any]) -> None:
    _diagnostic_context_id_valid(row["context_id"])
    _purpose_parse_date(row["R"], code="context_report_date")
    if row["K"] != _diagnostic_timestamp(
        _purpose_parse_timestamp(row["K"], code="context_knowledge_cutoff")
    ):
        raise NcenError("context_cutoff_not_canonical")
    _check_mode(row["mode"])
    _diagnostic_nonempty(row["inventory_digest"], "context_inventory_digest")
    for field_name in ("cohort_digest", "ncen_evidence_digest", "exclusion_ledger_digest"):
        _diagnostic_hash_valid(row[field_name], f"context_{field_name}")
    purpose_versions = _purpose_exact_object(
        row["purpose_versions"],
        frozenset({"reported_family", "reporting_dependence_block"}),
        code="context_purpose_versions",
    )
    if purpose_versions != {
        "reported_family": DIAGNOSTIC_REPORTED_FAMILY_VERSION,
        "reporting_dependence_block": DIAGNOSTIC_DEPENDENCE_VERSION,
    }:
        raise NcenError("context_purpose_versions_invalid")
    if (
        row["edge_version"] != DIAGNOSTIC_EDGE_VERSION
        or row["selection_version"] != DIAGNOSTIC_SELECTION_VERSION
        or row["normalizer"] != DIAGNOSTIC_NAME_NORMALIZER_VERSION
        or row["diagnostic_only"] is not True
        or row["qualification"] != "NOT_EVALUABLE"
    ):
        raise NcenError("context_policy_invalid")
    _purpose_positive(row["node_count"], code="context_node_count")
    if not isinstance(row["dependency_time_established"], bool):
        raise NcenError("context_dependency_time_established_invalid")
    if row["knowledge_time"] is not None:
        _purpose_parse_timestamp(row["knowledge_time"], code="context_knowledge_time")
    if row["dependency_time_established"] != (row["knowledge_time"] is not None):
        raise NcenError("context_knowledge_time_state_mismatch")


def _purpose_validate_node(row: Mapping[str, Any]) -> None:
    _diagnostic_context_id_valid(row["context_id"])
    _diagnostic_normalized_cik(row["cik"])
    if row["selected_accession"] is not None and _ACCESSION.fullmatch(
        row["selected_accession"]
    ) is None:
        raise NcenError("node_accession_invalid")
    _purpose_nullable_hash(row["selected_projection_digest"], code="node_projection_digest")
    _purpose_sorted_strings(row["voting_series"], code="node_voting_series", allow_empty=False)
    if row["evidence_state"] not in {"complete", "incomplete"}:
        raise NcenError("node_evidence_state_invalid")
    reasons = _purpose_record_reasons(row)
    if (row["evidence_state"] == "complete") != (not reasons):
        raise NcenError("node_completeness_reasons_mismatch")
    _purpose_nullable_string(row["selection_reason"], code="node_selection_reason")
    _purpose_sorted_strings(row["source_row_ids"], code="node_source_rows")
    dependencies = row["dependencies"]
    if not isinstance(dependencies, list):
        raise NcenError("node_dependencies_invalid")
    dependency_order = []
    for raw in dependencies:
        item = _purpose_exact_object(
            raw,
            frozenset({"accession", "role", "knowledge_time"}),
            code="node_dependency",
        )
        if _ACCESSION.fullmatch(item["accession"]) is None:
            raise NcenError("node_dependency_accession_invalid")
        _diagnostic_nonempty(item["role"], "node_dependency_role")
        if item["knowledge_time"] is not None:
            _purpose_parse_timestamp(
                item["knowledge_time"], code="node_dependency_knowledge_time"
            )
        dependency_order.append((item["role"], item["accession"]))
    if dependency_order != sorted(set(dependency_order)):
        raise NcenError("node_dependencies_not_sorted_unique")
    if not isinstance(row["has_unknown_dependence"], bool):
        raise NcenError("node_unknown_dependence_invalid")
    if row["independent_vote_eligible"] is not False:
        raise NcenError("node_independent_vote_forbidden")


def _purpose_validate_reported_family(row: Mapping[str, Any]) -> None:
    _diagnostic_context_id_valid(row["context_id"])
    _diagnostic_normalized_cik(row["cik"])
    if (
        row["purpose"] != "reported_family"
        or row["rule_version"] != DIAGNOSTIC_REPORTED_FAMILY_VERSION
        or row["normalizer"] != DIAGNOSTIC_NAME_NORMALIZER_VERSION
        or row["claim"] != DIAGNOSTIC_REPORTED_CLAIM
    ):
        raise NcenError("reported_family_policy_invalid")
    if row["state"] not in {"declared_family", "standalone", "unknown"}:
        raise NcenError("reported_family_state_invalid")
    if row["answer"] not in {None, "Y", "N"}:
        raise NcenError("reported_family_answer_invalid")
    for field_name in ("name_raw", "name_key", "label_id"):
        _purpose_nullable_string(row[field_name], code=f"reported_family_{field_name}")
    _purpose_sorted_strings(row["source_row_ids"], code="reported_family_sources")
    reasons = _purpose_record_reasons(row)
    if row["state"] == "unknown":
        if row["label_id"] is not None or row["name_key"] is not None or not reasons:
            raise NcenError("reported_family_unknown_invalid")
    elif reasons:
        raise NcenError("reported_family_known_has_reasons")
    elif row["state"] == "declared_family":
        if row["answer"] != "Y" or row["name_key"] is None:
            raise NcenError("reported_family_declared_invalid")
        canonical_key = _diagnostic_canonical_reported_name_key(row["name_key"])
        if canonical_key != row["name_key"]:
            raise NcenError("reported_family_name_key_not_canonical")
        expected_label = _diagnostic_id(
            "ncenreported",
            [DIAGNOSTIC_REPORTED_FAMILY_VERSION, DIAGNOSTIC_NAME_NORMALIZER_VERSION, canonical_key],
        )
        if row["label_id"] != expected_label:
            raise NcenError("reported_family_label_id_mismatch")
    else:
        if row["answer"] != "N" or row["name_raw"] is not None or row["name_key"] is not None:
            raise NcenError("reported_family_standalone_invalid")
        expected_label = _diagnostic_id(
            "ncenstandalone", [DIAGNOSTIC_REPORTED_FAMILY_VERSION, row["cik"]]
        )
        if row["label_id"] != expected_label:
            raise NcenError("reported_family_label_id_mismatch")


def _purpose_validate_incidence(row: Mapping[str, Any]) -> None:
    _diagnostic_context_id_valid(row["context_id"])
    _diagnostic_normalized_cik(row["cik"])
    if _ACCESSION.fullmatch(row["accession"]) is None:
        raise NcenError("incidence_accession_invalid")
    if row["purpose"] != "reporting_dependence_block":
        raise NcenError("incidence_purpose_invalid")
    if row["series_scope"] not in {"series", "registrant", "unresolved"}:
        raise NcenError("incidence_series_scope_invalid")
    if row["kind"] not in {"provider", "b5_declared_name"} or row["role"] not in DIAGNOSTIC_ROLES:
        raise NcenError("incidence_kind_or_role_invalid")
    if row["identifier_kind"] not in {None, "FN", "CRD", "LEI", "name"}:
        raise NcenError("incidence_identifier_kind_invalid")
    _purpose_nullable_string(row["identifier_value"], code="incidence_identifier_value")
    _purpose_nullable_string(row["key_id"], code="incidence_key_id")
    if re.fullmatch(r"ncenrow:source_row:[0-9a-f]{64}", row["source_row_id"]) is None:
        raise NcenError("incidence_source_row_id_invalid")
    if row["attestation"] not in {"attested", "uncertain"}:
        raise NcenError("incidence_attestation_invalid")
    reasons = _purpose_record_reasons(row)
    if row["attestation"] == "uncertain" and not reasons:
        raise NcenError("incidence_uncertain_reason_missing")
    if not isinstance(row["uncertain_expansion_eligible"], bool):
        raise NcenError("incidence_uncertain_expansion_invalid")
    if row["key_id"] is None:
        if any(row[field] is not None for field in ("identifier_kind", "identifier_value")):
            raise NcenError("incidence_null_key_invalid")
    elif row["kind"] == "provider":
        if row["identifier_kind"] not in DIAGNOSTIC_IDENTIFIER_KINDS or row[
            "identifier_value"
        ] is None:
            raise NcenError("incidence_provider_identifier_invalid")
        if row["key_id"] != diagnostic_provider_key_id(
            row["identifier_kind"], row["identifier_value"]
        ):
            raise NcenError("incidence_provider_key_mismatch")
    else:
        if row["identifier_kind"] != "name" or row["identifier_value"] is None:
            raise NcenError("incidence_b5_identifier_invalid")
        canonical_key = _diagnostic_canonical_reported_name_key(row["identifier_value"])
        if canonical_key != row["identifier_value"]:
            raise NcenError("incidence_b5_identifier_not_canonical")
        if row["key_id"] != diagnostic_b5_key_id(canonical_key):
            raise NcenError("incidence_b5_key_mismatch")


def _purpose_validate_component(row: Mapping[str, Any]) -> None:
    _diagnostic_context_id_valid(row["context_id"])
    ablation_spec(row["ablation_id"])
    if row["purpose"] != "reporting_dependence_block":
        raise NcenError("component_purpose_invalid")
    for field_name, pattern in (
        ("snapshot_component_id", r"ncenblock:[0-9a-f]{64}"),
        ("membership_track_id", r"ncentrack:[0-9a-f]{64}"),
        ("observed_edge_version_id", r"ncenedges:[0-9a-f]{64}"),
    ):
        if re.fullmatch(pattern, row[field_name]) is None:
            raise NcenError(f"component_{field_name}_invalid")
    _diagnostic_hash_valid(row["evidence_digest"], "component_evidence_digest")
    _purpose_positive(row["member_count"], code="component_member_count")
    for field_name in (
        "complete_count",
        "incomplete_count",
        "distinct_reported_y_keys",
    ):
        _purpose_nonnegative(row[field_name], code=f"component_{field_name}")
    if row["complete_count"] + row["incomplete_count"] != row["member_count"]:
        raise NcenError("component_completeness_mismatch")
    if row["distinct_reported_y_keys"] > row["member_count"]:
        raise NcenError("component_reported_key_count_invalid")
    if not isinstance(row["has_unknown_dependence"], bool):
        raise NcenError("component_unknown_dependence_invalid")
    if row["independent_vote_count"] is not None:
        raise NcenError("component_independent_vote_forbidden")


def _purpose_validate_membership(row: Mapping[str, Any]) -> None:
    _diagnostic_context_id_valid(row["context_id"])
    ablation_spec(row["ablation_id"])
    if re.fullmatch(r"ncenblock:[0-9a-f]{64}", row["snapshot_component_id"]) is None:
        raise NcenError("membership_component_id_invalid")
    _diagnostic_normalized_cik(row["cik"])
    if row["evidence_state"] not in {"complete", "incomplete"}:
        raise NcenError("membership_evidence_state_invalid")


def _purpose_validate_key_degree(row: Mapping[str, Any]) -> None:
    _diagnostic_context_id_valid(row["context_id"])
    ablation_spec(row["ablation_id"])
    if re.fullmatch(r"ncenkey:[0-9a-f]{64}", row["key_id"]) is None:
        raise NcenError("key_degree_key_id_invalid")
    if row["kind"] not in {"provider", "b5_declared_name"}:
        raise NcenError("key_degree_kind_invalid")
    for field_name in (
        "distinct_registrants",
        "complete_registrants",
        "incomplete_registrants",
        "incidence_count",
    ):
        _purpose_nonnegative(row[field_name], code=f"key_degree_{field_name}")
    if row["complete_registrants"] + row["incomplete_registrants"] != row["distinct_registrants"]:
        raise NcenError("key_degree_completeness_mismatch")
    if row["incidence_count"] < row["distinct_registrants"]:
        raise NcenError("key_degree_incidence_count_invalid")


def _purpose_validate_spanning_union(row: Mapping[str, Any]) -> None:
    _diagnostic_context_id_valid(row["context_id"])
    ablation_spec(row["ablation_id"])
    if re.fullmatch(r"ncenkey:[0-9a-f]{64}", row["key_id"]) is None:
        raise NcenError("spanning_key_id_invalid")
    _diagnostic_normalized_cik(row["left_cik"])
    _diagnostic_normalized_cik(row["right_cik"])
    for field_name in ("left_incidence_id", "right_incidence_id"):
        if re.fullmatch(r"ncenrow:incidence:[0-9a-f]{64}", row[field_name]) is None:
            raise NcenError("spanning_incidence_id_invalid")


def _purpose_validate_summary(row: Mapping[str, Any]) -> None:
    _diagnostic_context_id_valid(row["context_id"])
    ablation_spec(row["ablation_id"])
    for field_name in _PURPOSE_PAYLOAD_KEYS["summary"] - {"context_id", "ablation_id"}:
        _purpose_nonnegative(row[field_name], code=f"summary_{field_name}")
    if row["complete_count"] + row["incomplete_count"] != row["node_count"]:
        raise NcenError("summary_completeness_mismatch")
    if (
        row["component_count"] > row["node_count"]
        or row["largest_all"] > row["node_count"]
        or (row["component_count"] == 0) != (row["largest_all"] == 0)
        or row["largest_complete"] > row["largest_all"]
        or row["complete_total"] > row["complete_count"]
        or row["largest_complete"] > row["complete_total"]
        or row["complete_square_sum"] < row["complete_total"]
        or row["complete_square_sum"] > row["complete_total"] * row["largest_complete"]
    ):
        raise NcenError("summary_aggregate_counts_invalid")


def _purpose_validate_transition(row: Mapping[str, Any]) -> None:
    ablation_spec(row["ablation_id"])
    _diagnostic_context_id_valid(row["from_context_id"])
    _diagnostic_context_id_valid(row["to_context_id"])
    for field_name in ("from_component_id", "to_component_id"):
        if row[field_name] is not None and re.fullmatch(
            r"ncenblock:[0-9a-f]{64}", row[field_name]
        ) is None:
            raise NcenError("transition_component_id_invalid")
    _purpose_positive(row["intersection_count"], code="transition_intersection_count")
    _purpose_positive(row["union_count"], code="transition_union_count")
    if row["union_count"] < row["intersection_count"] or row["kind"] not in {
        "overlap",
        "entry",
        "exit",
    }:
        raise NcenError("transition_counts_or_kind_invalid")


def _purpose_validate_fold_group(row: Mapping[str, Any]) -> None:
    if re.fullmatch(r"ncenfoldscope:[0-9a-f]{64}", row["fold_scope_id"]) is None:
        raise NcenError("fold_scope_id_invalid")
    if re.fullmatch(r"ncenfold:[0-9a-f]{64}", row["fold_group_id"]) is None:
        raise NcenError("fold_group_id_invalid")
    _purpose_positive(row["member_count"], code="fold_group_member_count")
    if not isinstance(row["has_unknown_dependence"], bool):
        raise NcenError("fold_group_unknown_dependence_invalid")
    if row["usable_for_independence_claim"] is not False:
        raise NcenError("fold_independence_claim_forbidden")


def _purpose_validate_fold_membership(row: Mapping[str, Any]) -> None:
    if re.fullmatch(r"ncenfoldscope:[0-9a-f]{64}", row["fold_scope_id"]) is None:
        raise NcenError("fold_scope_id_invalid")
    if re.fullmatch(r"ncenfold:[0-9a-f]{64}", row["fold_group_id"]) is None:
        raise NcenError("fold_group_id_invalid")
    _diagnostic_normalized_cik(row["cik"])


def _purpose_read_jsonl(path: Path, *, record_type: str) -> tuple[dict[str, Any], ...]:
    raw = path.read_bytes()
    if (raw and not raw.endswith(b"\n")) or b"\r" in raw:
        raise NcenError(f"diagnostic_jsonl_encoding_invalid:{path.name}")
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        parsed = _purpose_json_load(line, code=f"diagnostic_{record_type}")
        row = _purpose_validate_record_shape(parsed, expected_type=record_type)
        if line != _diagnostic_canonical(row):
            raise NcenError(f"diagnostic_jsonl_not_canonical:{path.name}")
        rows.append(row)
    ids = [row["record_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise NcenError(f"diagnostic_duplicate_record_id:{path.name}")
    return tuple(rows)


def _purpose_read_staged_jsonl(
    path: Path, *, record_type: str, pin: Mapping[str, Any], monitor: DiagnosticResourceMonitor,
) -> tuple[dict[str, Any], ...]:
    """Hash and parse each staged JSONL byte from the same open stream."""
    digest = hashlib.sha256()
    size = 0
    rows: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        for line in handle:
            digest.update(line)
            size += len(line)
            if not line.endswith(b"\n") or b"\r" in line:
                raise NcenError(f"diagnostic_jsonl_encoding_invalid:{path.name}")
            parsed = _purpose_json_load(line[:-1], code=f"diagnostic_{record_type}")
            row = _purpose_validate_record_shape(parsed, expected_type=record_type)
            if line[:-1] != _diagnostic_canonical(row):
                raise NcenError(f"diagnostic_jsonl_not_canonical:{path.name}")
            rows.append(row)
            monitor.rows_read(1, "purpose_readback_records")
    if size != pin["bytes"] or digest.hexdigest() != pin["sha256"]:
        raise NcenError("manifest_staged_file_mismatch")
    ids = [row["record_id"] for row in rows]
    if record_type != "selection_context" and len(ids) != len(set(ids)):
        raise NcenError(f"diagnostic_duplicate_record_id:{path.name}")
    return tuple(rows)


def _purpose_read_staged_json(path: Path, *, pin: Mapping[str, Any], code: str) -> Any:
    raw = _purpose_preliminary_bytes(path, code=code)
    if len(raw) != pin["bytes"] or hashlib.sha256(raw).hexdigest() != pin["sha256"]:
        raise NcenError("manifest_staged_file_mismatch")
    value = _purpose_json_load(raw, code=code)
    if raw != _purpose_json_bytes(value):
        raise NcenError(f"{code}_not_canonical")
    return value


def _purpose_incidence_enabled_record(
    incidence: Mapping[str, Any],
    node: Mapping[str, Any],
    spec: AblationSpec,
) -> bool:
    if incidence["key_id"] is None:
        return False
    if incidence["attestation"] == "uncertain":
        if not (spec.include_uncertain and incidence["uncertain_expansion_eligible"]):
            return False
    elif incidence["attestation"] != "attested":
        return False
    if node["evidence_state"] == "incomplete" and not spec.include_incomplete_edges:
        return False
    if incidence["kind"] == "b5_declared_name":
        return spec.include_b5
    return incidence["role"] in spec.provider_roles


def _purpose_partition(
    ciks: set[str],
    incidences: Sequence[Mapping[str, Any]],
) -> tuple[tuple[str, ...], ...]:
    adjacency = {cik: set() for cik in ciks}
    by_key: dict[str, set[str]] = defaultdict(set)
    for incidence in incidences:
        by_key[incidence["key_id"]].add(incidence["cik"])
    for members in by_key.values():
        ordered = sorted(members)
        if not ordered:
            continue
        anchor = ordered[0]
        for member in ordered[1:]:
            adjacency[anchor].add(member)
            adjacency[member].add(anchor)
    unseen = set(ciks)
    groups: list[tuple[str, ...]] = []
    while unseen:
        start = min(unseen)
        reached = {start}
        queue = [start]
        while queue:
            current = queue.pop()
            new = adjacency[current] - reached
            reached.update(new)
            queue.extend(new)
        unseen -= reached
        groups.append(tuple(sorted(reached)))
    return tuple(sorted(groups))


def _purpose_observed_edge_id(incidences: Sequence[Mapping[str, Any]]) -> str:
    structures = {
        (
            item["cik"],
            item["kind"],
            item["role"],
            item["key_id"],
            item["series_id"],
            item["series_scope"],
        )
        for item in incidences
        if item["attestation"] == "attested"
    }
    structural = [
        list(item)
        for item in sorted(
            structures,
            key=lambda item: _diagnostic_canonical(list(item)),
        )
    ]
    return _diagnostic_id("ncenedges", [DIAGNOSTIC_EDGE_VERSION, structural])


def _purpose_expected_spanning(
    ciks: set[str],
    incidences: Sequence[Mapping[str, Any]],
) -> set[tuple[str, str, str]]:
    parent = {cik: cik for cik in ciks}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    expected: set[tuple[str, str, str]] = set()
    by_key: dict[str, set[str]] = defaultdict(set)
    for incidence in incidences:
        by_key[incidence["key_id"]].add(incidence["cik"])
    for key_id in sorted(by_key):
        members = sorted(by_key[key_id])
        if len(members) < 2:
            continue
        left = members[0]
        for right in members[1:]:
            left_root = find(left)
            right_root = find(right)
            if left_root == right_root:
                continue
            parent[right_root] = left_root
            expected.add((key_id, left, right))
    return expected


def _purpose_expected_degree_records(
    *,
    context_id: str,
    ablation_id: str,
    incidences: Sequence[Mapping[str, Any]],
    nodes: Mapping[str, Mapping[str, Any]],
) -> set[bytes]:
    by_key: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for incidence in incidences:
        by_key[incidence["key_id"]].append(incidence)
    expected: set[bytes] = set()
    for key_id, key_incidences in by_key.items():
        ciks = {item["cik"] for item in key_incidences}
        complete = sum(nodes[cik]["evidence_state"] == "complete" for cik in ciks)
        expected.add(
            _diagnostic_canonical(
                _purpose_record(
                    "key_degree",
                    {
                        "context_id": context_id,
                        "ablation_id": ablation_id,
                        "key_id": key_id,
                        "kind": key_incidences[0]["kind"],
                        "distinct_registrants": len(ciks),
                        "complete_registrants": complete,
                        "incomplete_registrants": len(ciks) - complete,
                        "incidence_count": len(key_incidences),
                    },
                )
            )
        )
    return expected


def _purpose_expected_excluded_count(
    context_id: str,
    exclusion_rows: Sequence[Mapping[str, Any]],
    source_by_id: Mapping[str, Mapping[str, Any]],
) -> int:
    identities: set[tuple[str, str, str]] = set()
    for exclusion in exclusion_rows:
        if exclusion["context_id"] != context_id:
            continue
        for source_id in exclusion["source_row_ids"]:
            source = source_by_id[source_id]
            if source["role"] == "b5":
                if source["answer_raw"] == "Y" and source["name_key"] is not None:
                    identities.add((source_id, "name", source["name_key"]))
                continue
            for identifier in source["identifiers"]:
                if identifier["normalized"] is not None:
                    identities.add(
                        (source_id, identifier["kind"], identifier["normalized"])
                    )
    return len(identities)


def _purpose_unknown_family_payload(
    *,
    context_id: str,
    cik: str,
    source_ids: Sequence[str],
    reason: str,
    answer: str | None = None,
) -> dict[str, Any]:
    return {
        "context_id": context_id,
        "purpose": "reported_family",
        "rule_version": DIAGNOSTIC_REPORTED_FAMILY_VERSION,
        "cik": cik,
        "state": "unknown",
        "answer": answer,
        "name_raw": None,
        "name_key": None,
        "label_id": None,
        "source_row_ids": sorted(source_ids),
        "normalizer": DIAGNOSTIC_NAME_NORMALIZER_VERSION,
        "reasons": [reason],
        "claim": DIAGNOSTIC_REPORTED_CLAIM,
    }


def _purpose_expected_family_payload(
    *,
    context_id: str,
    cik: str,
    node: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    b5_rows = tuple(item for item in source_rows if item["role"] == "b5")
    source_ids = [item["record_id"] for item in b5_rows]
    if not b5_rows:
        return _purpose_unknown_family_payload(
            context_id=context_id,
            cik=cik,
            source_ids=(),
            reason="diagnostic_b5_missing",
        )
    uncertainty_reasons = {
        AMENDMENT_UNKNOWN_REASON,
        AMENDMENT_PARTIAL_REASON,
        ORDER_UNRESOLVED,
        "diagnostic_b5_copy_key_conflict",
        "diagnostic_provider_name_copy_conflict",
        "diagnostic_underwriter_lei_copy_conflict",
    }
    if node["selection_reason"] in uncertainty_reasons:
        return _purpose_unknown_family_payload(
            context_id=context_id,
            cik=cik,
            source_ids=source_ids,
            reason="diagnostic_b5_unattested",
        )
    answers = {item["answer_raw"] for item in b5_rows}
    if None in answers:
        return _purpose_unknown_family_payload(
            context_id=context_id,
            cik=cik,
            source_ids=source_ids,
            reason="diagnostic_b5_answer_unavailable",
        )
    if any(answer not in {"Y", "N"} for answer in answers):
        return _purpose_unknown_family_payload(
            context_id=context_id,
            cik=cik,
            source_ids=source_ids,
            reason="diagnostic_b5_answer_unparseable",
        )
    if len(answers) != 1:
        return _purpose_unknown_family_payload(
            context_id=context_id,
            cik=cik,
            source_ids=source_ids,
            reason="diagnostic_b5_answer_conflict",
        )
    answer = next(iter(answers))
    if answer == "N":
        if any(item["name_key"] is not None for item in b5_rows):
            return _purpose_unknown_family_payload(
                context_id=context_id,
                cik=cik,
                source_ids=source_ids,
                reason="diagnostic_b5_answer_name_conflict",
                answer="N",
            )
        return {
            "context_id": context_id,
            "purpose": "reported_family",
            "rule_version": DIAGNOSTIC_REPORTED_FAMILY_VERSION,
            "cik": cik,
            "state": "standalone",
            "answer": "N",
            "name_raw": None,
            "name_key": None,
            "label_id": _diagnostic_id(
                "ncenstandalone", [DIAGNOSTIC_REPORTED_FAMILY_VERSION, cik]
            ),
            "source_row_ids": sorted(source_ids),
            "normalizer": DIAGNOSTIC_NAME_NORMALIZER_VERSION,
            "reasons": [],
            "claim": DIAGNOSTIC_REPORTED_CLAIM,
        }
    keys = {item["name_key"] for item in b5_rows}
    if None in keys:
        return _purpose_unknown_family_payload(
            context_id=context_id,
            cik=cik,
            source_ids=source_ids,
            reason="diagnostic_b5_name_missing",
            answer="Y",
        )
    if len(keys) != 1:
        return _purpose_unknown_family_payload(
            context_id=context_id,
            cik=cik,
            source_ids=source_ids,
            reason="diagnostic_b5_copy_key_conflict",
            answer="Y",
        )
    name_key = next(iter(keys))
    raw_names = {item["name_raw"] for item in b5_rows}
    return {
        "context_id": context_id,
        "purpose": "reported_family",
        "rule_version": DIAGNOSTIC_REPORTED_FAMILY_VERSION,
        "cik": cik,
        "state": "declared_family",
        "answer": "Y",
        "name_raw": next(iter(raw_names)) if len(raw_names) == 1 else None,
        "name_key": name_key,
        "label_id": _diagnostic_id(
            "ncenreported",
            [
                DIAGNOSTIC_REPORTED_FAMILY_VERSION,
                DIAGNOSTIC_NAME_NORMALIZER_VERSION,
                name_key,
            ],
        ),
        "source_row_ids": sorted(source_ids),
        "normalizer": DIAGNOSTIC_NAME_NORMALIZER_VERSION,
        "reasons": [],
        "claim": DIAGNOSTIC_REPORTED_CLAIM,
    }


def _purpose_expected_incidence_records(
    *,
    context_id: str,
    node: Mapping[str, Any],
    family: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
) -> set[bytes]:
    uncertainty_reasons = {
        AMENDMENT_UNKNOWN_REASON,
        AMENDMENT_PARTIAL_REASON,
        ORDER_UNRESOLVED,
        "diagnostic_b5_copy_key_conflict",
        "diagnostic_provider_name_copy_conflict",
        "diagnostic_underwriter_lei_copy_conflict",
    }
    contextual_uncertainty = node["selection_reason"] in uncertainty_reasons
    output: set[bytes] = set()
    for source in source_rows:
        if source["role"] == "b5":
            if family["state"] != "declared_family" or contextual_uncertainty:
                continue
            payload = {
                "context_id": context_id,
                "purpose": "reporting_dependence_block",
                "cik": node["cik"],
                "accession": source["accession"],
                "series_id": source["series_id"],
                "series_scope": source["series_scope"],
                "kind": "b5_declared_name",
                "role": "b5",
                "identifier_kind": "name",
                "identifier_value": family["name_key"],
                "key_id": diagnostic_b5_key_id(family["name_key"]),
                "source_row_id": source["record_id"],
                "attestation": "attested",
                "reasons": list(source["reasons"]),
                "uncertain_expansion_eligible": False,
            }
            output.add(_diagnostic_canonical(_purpose_record("incidence", payload)))
            continue
        identifiers = [item for item in source["identifiers"] if item["normalized"] is not None]
        reasons = set(source["reasons"])
        if contextual_uncertainty:
            reasons.add(node["selection_reason"])
        if not identifiers:
            reasons.add("diagnostic_provider_identifier_unavailable")
            payload = {
                "context_id": context_id,
                "purpose": "reporting_dependence_block",
                "cik": node["cik"],
                "accession": source["accession"],
                "series_id": source["series_id"],
                "series_scope": source["series_scope"],
                "kind": "provider",
                "role": source["role"],
                "identifier_kind": None,
                "identifier_value": None,
                "key_id": None,
                "source_row_id": source["record_id"],
                "attestation": "uncertain",
                "reasons": sorted(reasons),
                "uncertain_expansion_eligible": False,
            }
            output.add(_diagnostic_canonical(_purpose_record("incidence", payload)))
            continue
        for identifier in identifiers:
            value = identifier["normalized"]
            payload = {
                "context_id": context_id,
                "purpose": "reporting_dependence_block",
                "cik": node["cik"],
                "accession": source["accession"],
                "series_id": source["series_id"],
                "series_scope": source["series_scope"],
                "kind": "provider",
                "role": source["role"],
                "identifier_kind": identifier["kind"],
                "identifier_value": value,
                "key_id": diagnostic_provider_key_id(identifier["kind"], value),
                "source_row_id": source["record_id"],
                "attestation": "uncertain" if contextual_uncertainty else "attested",
                "reasons": sorted(reasons),
                "uncertain_expansion_eligible": contextual_uncertainty,
            }
            output.add(_diagnostic_canonical(_purpose_record("incidence", payload)))
    return output


def _purpose_validate_context_projection(
    context: Mapping[str, Any],
    ablation_id: str,
    records: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    source_by_id: Mapping[str, Mapping[str, Any]],
    node_by_cik: Mapping[str, Mapping[str, Any]],
    family_by_cik: Mapping[str, Mapping[str, Any]],
    incidences: Sequence[Mapping[str, Any]],
) -> None:
    context_id = context["context_id"]
    spec = ablation_spec(ablation_id)
    enabled = tuple(
        item
        for item in incidences
        if _purpose_incidence_enabled_record(item, node_by_cik[item["cik"]], spec)
    )
    components = tuple(
        item
        for item in records["components.jsonl"]
        if item["context_id"] == context_id and item["ablation_id"] == ablation_id
    )
    memberships = tuple(
        item
        for item in records["memberships.jsonl"]
        if item["context_id"] == context_id and item["ablation_id"] == ablation_id
    )
    ciks = set(node_by_cik)
    member_ciks = [item["cik"] for item in memberships]
    if len(member_ciks) != len(set(member_ciks)) or set(member_ciks) != ciks:
        raise NcenError("diagnostic_membership_universe_mismatch")
    components_by_id = {item["snapshot_component_id"]: item for item in components}
    if len(components_by_id) != len(components):
        raise NcenError("diagnostic_component_duplicate")
    actual_groups: list[tuple[str, ...]] = []
    memberships_by_component: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for membership in memberships:
        memberships_by_component[membership["snapshot_component_id"]].append(membership)
    enabled_by_cik: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for incidence in enabled:
        enabled_by_cik[incidence["cik"]].append(incidence)
    uncertain_ciks = {
        item["cik"] for item in incidences if item["attestation"] == "uncertain"
    }
    for component_id, component in components_by_id.items():
        members = tuple(
            sorted(
                item["cik"] for item in memberships_by_component[component_id]
            )
        )
        if not members:
            raise NcenError("diagnostic_component_without_members")
        actual_groups.append(members)
        if component_id != snapshot_component_id(
            context_id=context_id,
            ablation_id=ablation_id,
            members=members,
        ):
            raise NcenError("diagnostic_component_id_replay_mismatch")
        if component["membership_track_id"] != membership_track_id(
            ablation_id=ablation_id,
            members=members,
        ):
            raise NcenError("diagnostic_track_id_replay_mismatch")
        component_incidences = tuple(
            item for cik in members for item in enabled_by_cik[cik]
        )
        if component["observed_edge_version_id"] != _purpose_observed_edge_id(
            component_incidences
        ):
            raise NcenError("diagnostic_observed_edge_id_replay_mismatch")
        incidence_ids = sorted(item["record_id"] for item in component_incidences)
        if component["evidence_digest"] != _diagnostic_hash(
            [context_id, ablation_id, incidence_ids]
        ):
            raise NcenError("diagnostic_component_evidence_digest_mismatch")
        complete = sum(node_by_cik[cik]["evidence_state"] == "complete" for cik in members)
        reported_keys = {
            family_by_cik[cik]["name_key"]
            for cik in members
            if family_by_cik[cik]["state"] == "declared_family"
        }
        expected_values = (
            len(members),
            complete,
            len(members) - complete,
            len(reported_keys),
            any(node_by_cik[cik]["evidence_state"] == "incomplete" for cik in members)
            or bool(set(members) & uncertain_ciks),
        )
        actual_values = (
            component["member_count"],
            component["complete_count"],
            component["incomplete_count"],
            component["distinct_reported_y_keys"],
            component["has_unknown_dependence"],
        )
        if actual_values != expected_values:
            raise NcenError("diagnostic_component_summary_mismatch")
        for membership in memberships_by_component[component_id]:
            if membership["evidence_state"] != node_by_cik[membership["cik"]][
                "evidence_state"
            ]:
                raise NcenError("diagnostic_membership_evidence_state_mismatch")
    expected_groups = _purpose_partition(ciks, enabled)
    if tuple(sorted(actual_groups)) != expected_groups:
        raise NcenError("diagnostic_partition_replay_mismatch")
    degrees = tuple(
        item
        for item in records["key_degrees.jsonl"]
        if item["context_id"] == context_id and item["ablation_id"] == ablation_id
    )
    expected_degrees = _purpose_expected_degree_records(
        context_id=context_id,
        ablation_id=ablation_id,
        incidences=enabled,
        nodes=node_by_cik,
    )
    if {_diagnostic_canonical(dict(item)) for item in degrees} != expected_degrees:
        raise NcenError("diagnostic_key_degree_replay_mismatch")
    incidence_by_id = {item["record_id"]: item for item in enabled}
    spanning = tuple(
        item
        for item in records["spanning.jsonl"]
        if item["context_id"] == context_id and item["ablation_id"] == ablation_id
    )
    actual_spanning = {
        (item["key_id"], item["left_cik"], item["right_cik"]) for item in spanning
    }
    expected_spanning = _purpose_expected_spanning(ciks, enabled)
    if actual_spanning != expected_spanning or len(spanning) != len(actual_spanning):
        raise NcenError("diagnostic_spanning_forest_replay_mismatch")
    for item in spanning:
        left = incidence_by_id.get(item["left_incidence_id"])
        right = incidence_by_id.get(item["right_incidence_id"])
        if (
            left is None
            or right is None
            or left["key_id"] != item["key_id"]
            or right["key_id"] != item["key_id"]
            or left["cik"] != item["left_cik"]
            or right["cik"] != item["right_cik"]
        ):
            raise NcenError("diagnostic_spanning_witness_invalid")
    if len(spanning) != len(ciks) - len(components):
        raise NcenError("diagnostic_spanning_count_mismatch")
    summaries = tuple(
        item
        for item in records["summaries.jsonl"]
        if item["context_id"] == context_id and item["ablation_id"] == ablation_id
    )
    if len(summaries) != 1:
        raise NcenError("diagnostic_summary_cardinality_invalid")
    summary = summaries[0]
    complete_counts = [item["complete_count"] for item in components]
    expected_summary = {
        "node_count": len(ciks),
        "complete_count": sum(node["evidence_state"] == "complete" for node in node_by_cik.values()),
        "incomplete_count": sum(
            node["evidence_state"] == "incomplete" for node in node_by_cik.values()
        ),
        "component_count": len(components),
        "largest_all": max((item["member_count"] for item in components), default=0),
        "largest_complete": max(complete_counts, default=0),
        "complete_square_sum": sum(value**2 for value in complete_counts),
        "complete_total": sum(complete_counts),
        "uncertain_incidence_count": sum(
            item["attestation"] == "uncertain" for item in enabled
        ),
        "excluded_incidence_count": _purpose_expected_excluded_count(
            context_id,
            records["exclusions.jsonl"],
            source_by_id,
        ),
    }
    if any(summary[key] != value for key, value in expected_summary.items()):
        raise NcenError("diagnostic_summary_reconciliation_mismatch")


def _purpose_expected_transitions(
    contexts: Sequence[Mapping[str, Any]],
    ablation_id: str,
    memberships: Sequence[Mapping[str, Any]],
) -> set[bytes]:
    from itertools import pairwise

    output: set[bytes] = set()
    for left_context, right_context in pairwise(contexts):
        left = {
            item["cik"]: item["snapshot_component_id"]
            for item in memberships
            if item["context_id"] == left_context["context_id"]
            and item["ablation_id"] == ablation_id
        }
        right = {
            item["cik"]: item["snapshot_component_id"]
            for item in memberships
            if item["context_id"] == right_context["context_id"]
            and item["ablation_id"] == ablation_id
        }
        left_sizes = Counter(left.values())
        right_sizes = Counter(right.values())
        intersections: Counter[tuple[str, str]] = Counter(
            (left[cik], right[cik]) for cik in left.keys() & right.keys()
        )
        for (left_id, right_id), count in intersections.items():
            output.add(
                _diagnostic_canonical(
                    _purpose_record(
                        "transition",
                        {
                            "ablation_id": ablation_id,
                            "from_context_id": left_context["context_id"],
                            "to_context_id": right_context["context_id"],
                            "from_component_id": left_id,
                            "to_component_id": right_id,
                            "intersection_count": count,
                            "union_count": left_sizes[left_id] + right_sizes[right_id] - count,
                            "kind": "overlap",
                        },
                    )
                )
            )
        for component_id, count in Counter(left[cik] for cik in left.keys() - right.keys()).items():
            output.add(
                _diagnostic_canonical(
                    _purpose_record(
                        "transition",
                        {
                            "ablation_id": ablation_id,
                            "from_context_id": left_context["context_id"],
                            "to_context_id": right_context["context_id"],
                            "from_component_id": component_id,
                            "to_component_id": None,
                            "intersection_count": count,
                            "union_count": left_sizes[component_id],
                            "kind": "exit",
                        },
                    )
                )
            )
        for component_id, count in Counter(right[cik] for cik in right.keys() - left.keys()).items():
            output.add(
                _diagnostic_canonical(
                    _purpose_record(
                        "transition",
                        {
                            "ablation_id": ablation_id,
                            "from_context_id": left_context["context_id"],
                            "to_context_id": right_context["context_id"],
                            "from_component_id": None,
                            "to_component_id": component_id,
                            "intersection_count": count,
                            "union_count": right_sizes[component_id],
                            "kind": "entry",
                        },
                    )
                )
            )
    return output


def _purpose_validate_temporal(
    contexts: Sequence[Mapping[str, Any]],
    records: Mapping[str, Sequence[Mapping[str, Any]]],
) -> None:
    nodes_by_context = {
        context["context_id"]: {
            item["cik"]: item
            for item in records["nodes.jsonl"]
            if item["context_id"] == context["context_id"]
        }
        for context in contexts
    }
    incidences_by_context = {
        context["context_id"]: tuple(
            item
            for item in records["incidences.jsonl"]
            if item["context_id"] == context["context_id"]
        )
        for context in contexts
    }
    actual_transitions = {
        _diagnostic_canonical(dict(item)) for item in records["transitions.jsonl"]
    }
    expected_transitions: set[bytes] = set()
    expected_groups: set[bytes] = set()
    expected_memberships: set[bytes] = set()
    for spec in DIAGNOSTIC_ABLATIONS:
        expected_transitions.update(
            _purpose_expected_transitions(
                contexts,
                spec.ablation_id,
                records["memberships.jsonl"],
            )
        )
        fold_contexts = tuple(
            (
                _purpose_parse_date(context["R"], code="fold_report_date"),
                _purpose_parse_timestamp(context["K"], code="fold_knowledge_cutoff"),
                context["inventory_digest"],
                context["context_id"],
            )
            for context in contexts
        )
        scope_id = fold_scope_id(
            mode=contexts[0]["mode"],
            ablation_id=spec.ablation_id,
            contexts=fold_contexts,
        )
        all_ciks = {
            cik for nodes in nodes_by_context.values() for cik in nodes
        }
        parent: dict[str, str] = {}

        def add(value: str, *, _parent: dict[str, str] = parent) -> None:
            _parent.setdefault(value, value)

        def find(
            value: str,
            *,
            _parent: dict[str, str] = parent,
            _add: Any = add,
        ) -> str:
            _add(value)
            while _parent[value] != value:
                _parent[value] = _parent[_parent[value]]
                value = _parent[value]
            return value

        def union(
            left: str,
            right: str,
            *,
            _parent: dict[str, str] = parent,
            _find: Any = find,
        ) -> None:
            left_root = _find(left)
            right_root = _find(right)
            if left_root != right_root:
                _parent[right_root] = left_root

        tainted: set[str] = set()
        for context in contexts:
            context_id = context["context_id"]
            nodes = nodes_by_context[context_id]
            for cik, node in nodes.items():
                add(f"cik:{cik}")
                if node["evidence_state"] == "incomplete":
                    tainted.add(cik)
            for incidence in incidences_by_context[context_id]:
                if incidence["attestation"] == "uncertain":
                    tainted.add(incidence["cik"])
                if _diagnostic_fold_edge(
                    incidence["attestation"], incidence["key_id"],
                    _purpose_incidence_enabled_record(incidence, nodes[incidence["cik"]], spec),
                ):
                    union(f"cik:{incidence['cik']}", f"key:{incidence['key_id']}")
        by_root: dict[str, list[str]] = defaultdict(list)
        for cik in sorted(all_ciks):
            by_root[find(f"cik:{cik}")].append(cik)
        for raw_members in by_root.values():
            members = tuple(sorted(raw_members))
            group_id = fold_group_id(scope_id=scope_id, members=members)
            expected_groups.add(
                _diagnostic_canonical(
                    _purpose_record(
                        "fold_group",
                        {
                            "fold_scope_id": scope_id,
                            "fold_group_id": group_id,
                            "member_count": len(members),
                            "has_unknown_dependence": bool(set(members) & tainted),
                            "usable_for_independence_claim": False,
                        },
                    )
                )
            )
            for cik in members:
                expected_memberships.add(
                    _diagnostic_canonical(
                        _purpose_record(
                            "fold_membership",
                            {
                                "fold_scope_id": scope_id,
                                "fold_group_id": group_id,
                                "cik": cik,
                            },
                        )
                    )
                )
    if actual_transitions != expected_transitions:
        raise NcenError("diagnostic_transition_replay_mismatch")
    if {
        _diagnostic_canonical(dict(item)) for item in records["fold_groups.jsonl"]
    } != expected_groups:
        raise NcenError("diagnostic_fold_group_replay_mismatch")
    if {
        _diagnostic_canonical(dict(item)) for item in records["fold_memberships.jsonl"]
    } != expected_memberships:
        raise NcenError("diagnostic_fold_membership_replay_mismatch")


def _purpose_reconstruct_cohort(
    declaration: Mapping[str, Any],
    cohort_rows: Sequence[Mapping[str, Any]],
) -> DiagnosticCohort:
    provenance = declaration["cohort_provenance"]
    sources = tuple(
        InventorySource(
            package_label=item["package_label"],
            zip_sha256=item["zip_sha256"],
            package_id=item["package_id"],
            retrieved_at=_purpose_parse_timestamp(
                item["retrieved_at"], code="cohort_source_retrieved_at"
            ),
            first_verified_public_at=_purpose_parse_timestamp(
                item["first_verified_public_at"],
                code="cohort_source_first_verified_public_at",
            ),
        )
        for item in provenance["inventory_sources"]
    )
    members = tuple(
        sorted(
            (
                DiagnosticCohortMember(
                    report_date=_purpose_parse_date(item["R"], code="cohort_report_date"),
                    cik=item["cik"],
                    fund_keys=tuple(item["fund_keys"]),
                    known_at=_purpose_parse_timestamp(
                        item["known_at"], code="cohort_known_at"
                    ),
                )
                for item in cohort_rows
            ),
            key=lambda item: (item.report_date, item.cik),
        )
    )
    if len(members) != len({(item.report_date, item.cik) for item in members}):
        raise NcenError("diagnostic_cohort_member_duplicate")
    return DiagnosticCohort(
        knowledge_cutoff=_purpose_parse_timestamp(
            provenance["knowledge_cutoff"], code="cohort_knowledge_cutoff"
        ),
        knowledge_mode=provenance["knowledge_mode"],
        inventory_digest=declaration["inventory_digest"],
        sources=sources,
        members=members,
        cohort_digest=declaration["cohort_digest"],
        extraction_version=DIAGNOSTIC_COHORT_VERSION,
        derivation=provenance["derivation"],
        full_cohort=provenance["full_cohort"],
        outcome_fields_excluded=provenance["outcome_fields_excluded"],
        _seal=_DIAGNOSTIC_COHORT_SEAL,
    )


def _purpose_validate_semantics(
    declaration: Mapping[str, Any],
    records: Mapping[str, Sequence[Mapping[str, Any]]],
) -> None:
    declared = _purpose_declaration(declaration)
    contexts = tuple(records["contexts.jsonl"])
    if len(contexts) != len(declared["contexts"]):
        raise NcenError("diagnostic_context_coverage_mismatch")
    source_by_id = {item["record_id"]: item for item in records["sources.jsonl"]}
    cohort = _purpose_reconstruct_cohort(declared, records["cohort.jsonl"])
    cohort_by_key = {
        (item.report_date.isoformat(), item.cik): item for item in cohort.members
    }
    for source in source_by_id.values():
        header_id = source["header_source_id"]
        if header_id is None:
            continue
        header = source_by_id.get(header_id)
        if (
            header is None
            or header["role"] != "header"
            or header["accession"] != source["accession"]
            or header["cik"] != source["cik"]
            or header["acceptance_at"] != source["acceptance_at"]
        ):
            raise NcenError("diagnostic_header_binding_invalid")
    for exclusion in records["exclusions.jsonl"]:
        if any(source_id not in source_by_id for source_id in exclusion["source_row_ids"]):
            raise NcenError("diagnostic_exclusion_orphan_source")
    cohort_by_date: dict[str, set[str]] = defaultdict(set)
    for member in records["cohort.jsonl"]:
        if member["inventory_digest"] != declared["inventory_digest"]:
            raise NcenError("diagnostic_cohort_inventory_binding_mismatch")
        cohort_by_date[member["R"]].add(member["cik"])
    expected_context_ids: list[str] = []
    for raw_context in declared["contexts"]:
        expected_context_ids.append(
            diagnostic_context_id(
                report_date=_purpose_parse_date(
                    raw_context["R"], code="context_report_date"
                ),
                knowledge_cutoff=_purpose_parse_timestamp(
                    raw_context["K"], code="context_knowledge_cutoff"
                ),
                mode=raw_context["mode"],
                inventory_digest=declared["inventory_digest"],
                cohort_digest=declared["cohort_digest"],
                ncen_evidence_digest=declared["ncen_evidence_digest"],
                exclusion_ledger_digest=declared["exclusion_ledger_digest"],
            )
        )
    if tuple(context["context_id"] for context in contexts) != tuple(expected_context_ids):
        raise NcenError("diagnostic_context_identity_or_order_mismatch")
    context_ids = set(expected_context_ids)
    for exclusion in records["exclusions.jsonl"]:
        if exclusion["context_id"] is not None and exclusion["context_id"] not in context_ids:
            raise NcenError("diagnostic_exclusion_orphan_context")
    for context in contexts:
        context_id = context["context_id"]
        if (
            context["inventory_digest"] != declared["inventory_digest"]
            or context["cohort_digest"] != declared["cohort_digest"]
            or context["ncen_evidence_digest"] != declared["ncen_evidence_digest"]
            or context["exclusion_ledger_digest"] != declared["exclusion_ledger_digest"]
        ):
            raise NcenError("diagnostic_context_declaration_binding_mismatch")
        nodes = tuple(
            item for item in records["nodes.jsonl"] if item["context_id"] == context_id
        )
        families = tuple(
            item
            for item in records["reported_families.jsonl"]
            if item["context_id"] == context_id
        )
        incidences = tuple(
            item
            for item in records["incidences.jsonl"]
            if item["context_id"] == context_id
        )
        node_by_cik = {item["cik"]: item for item in nodes}
        family_by_cik = {item["cik"]: item for item in families}
        expected_ciks = cohort_by_date[context["R"]]
        if (
            len(node_by_cik) != len(nodes)
            or len(family_by_cik) != len(families)
            or set(node_by_cik) != expected_ciks
            or set(family_by_cik) != expected_ciks
            or context["node_count"] != len(expected_ciks)
        ):
            raise NcenError("diagnostic_node_family_cohort_closure_mismatch")
        cutoff = _purpose_parse_timestamp(context["K"], code="context_knowledge_cutoff")
        if cutoff != cohort.knowledge_cutoff or context["mode"] != cohort.knowledge_mode:
            raise NcenError("diagnostic_context_cohort_time_mismatch")
        dependency_times: list[dt.datetime] = []
        dependency_established = True
        if not any(node["dependencies"] for node in nodes):
            dependency_established = False
        expected_incidence_records: set[bytes] = set()
        for node in nodes:
            cohort_member = cohort_by_key.get((context["R"], node["cik"]))
            if cohort_member is None or node["voting_series"] != list(cohort_member.fund_keys):
                raise NcenError("diagnostic_node_cohort_payload_mismatch")
            if cohort_member.known_at > cutoff:
                raise NcenError("diagnostic_cohort_known_after_cutoff")
            for source_id in node["source_row_ids"]:
                source = source_by_id.get(source_id)
                if source is None or source["cik"] != node["cik"]:
                    raise NcenError("diagnostic_node_orphan_source")
            selected_sources = [source_by_id[item] for item in node["source_row_ids"]]
            if any(source["role"] not in DIAGNOSTIC_ROLES for source in selected_sources):
                raise NcenError("diagnostic_node_contains_nonrelationship_source")
            if selected_sources:
                accessions = {source["accession"] for source in selected_sources}
                projections = {
                    source["projection_digest"]
                    for source in selected_sources
                    if source["projection_digest"] is not None
                }
                if accessions != {node["selected_accession"]} or len(projections) != 1:
                    raise NcenError("diagnostic_node_selection_source_mismatch")
                if node["selected_projection_digest"] != next(iter(projections)):
                    raise NcenError("diagnostic_node_projection_source_mismatch")
            elif node["selected_accession"] is None:
                if node["selected_projection_digest"] is not None:
                    raise NcenError("diagnostic_node_projection_without_selection")
            if node["selection_reason"] is not None and node["selection_reason"] not in node[
                "reasons"
            ]:
                raise NcenError("diagnostic_node_selection_reason_not_closed")
            selected_dependencies = [
                item
                for item in node["dependencies"]
                if item["role"] == "selected"
                and item["accession"] == node["selected_accession"]
            ]
            if node["selected_accession"] is not None and len(selected_dependencies) != 1:
                raise NcenError("diagnostic_node_selected_dependency_missing")
            for dependency in node["dependencies"]:
                if dependency["knowledge_time"] is None:
                    dependency_established = False
                    continue
                dependency_time = _purpose_parse_timestamp(
                    dependency["knowledge_time"], code="node_dependency_knowledge_time"
                )
                if dependency_time > cutoff:
                    raise NcenError("diagnostic_dependency_after_cutoff")
                dependency_times.append(dependency_time)
            family = family_by_cik[node["cik"]]
            expected_family = _purpose_record(
                "reported_family",
                _purpose_expected_family_payload(
                    context_id=context_id,
                    cik=node["cik"],
                    node=node,
                    source_rows=selected_sources,
                ),
            )
            if family != expected_family:
                raise NcenError("diagnostic_reported_family_derivation_mismatch")
            expected_incidence_records.update(
                _purpose_expected_incidence_records(
                    context_id=context_id,
                    node=node,
                    family=family,
                    source_rows=selected_sources,
                )
            )
        expected_context_knowledge = None
        if dependency_established:
            expected_context_knowledge = _diagnostic_timestamp(
                max(
                    [
                        cohort_by_key[(context["R"], cik)].known_at
                        for cik in sorted(expected_ciks)
                    ]
                    + dependency_times
                )
            )
        if (
            context["dependency_time_established"] != dependency_established
            or context["knowledge_time"] != expected_context_knowledge
        ):
            raise NcenError("diagnostic_context_knowledge_reconciliation_mismatch")
        for family in families:
            for source_id in family["source_row_ids"]:
                source = source_by_id.get(source_id)
                if source is None or source["cik"] != family["cik"]:
                    raise NcenError("diagnostic_family_orphan_source")
        for incidence in incidences:
            source = source_by_id.get(incidence["source_row_id"])
            if (
                incidence["cik"] not in node_by_cik
                or source is None
                or source["cik"] != incidence["cik"]
                or source["accession"] != incidence["accession"]
            ):
                raise NcenError("diagnostic_incidence_orphan_reference")
        if {_diagnostic_canonical(dict(item)) for item in incidences} != expected_incidence_records:
            raise NcenError("diagnostic_incidence_derivation_mismatch")
        for spec in DIAGNOSTIC_ABLATIONS:
            _purpose_validate_context_projection(
                context,
                spec.ablation_id,
                records,
                source_by_id=source_by_id,
                node_by_cik=node_by_cik,
                family_by_cik=family_by_cik,
                incidences=incidences,
            )
    _purpose_validate_temporal(contexts, records)


def _purpose_process_rss() -> int:
    try:
        import psutil  # type: ignore[import-not-found]

        return int(psutil.Process().memory_info().rss)
    except (ImportError, OSError):
        pass
    import os

    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        get_current_process = ctypes.windll.kernel32.GetCurrentProcess
        get_current_process.restype = wintypes.HANDLE
        get_process_memory_info = ctypes.windll.psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = (
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        )
        get_process_memory_info.restype = wintypes.BOOL
        process = get_current_process()
        if get_process_memory_info(
            process,
            ctypes.byref(counters),
            counters.cb,
        ):
            return int(counters.PeakWorkingSetSize)
    else:
        try:
            import resource
            import sys

            peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            return peak if sys.platform == "darwin" else peak * 1024
        except (ImportError, OSError):
            pass
    raise NcenError("purpose_export_memory_measurement_unavailable")


def _purpose_write_partial(path: Path, payload: bytes) -> None:
    import os

    if path.exists() or path.is_symlink():
        raise NcenError("purpose_export_partial_path_exists")
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _purpose_finalize_partial(path: Path) -> None:
    import os

    if path.suffix != ".partial":
        raise NcenError("purpose_export_partial_suffix_required")
    final = path.with_suffix("")
    if final.exists() or final.is_symlink():
        raise NcenError("purpose_export_final_path_exists")
    os.replace(path, final)


def _purpose_verify_declared_inputs(
    declaration: Mapping[str, Any],
    *,
    code_root: Path,
    input_roots: Mapping[str, Path],
) -> None:
    declared = _purpose_declaration(declaration)
    for pin in declared["source_code"]:
        path = _purpose_safe_path(code_root, pin["path"], code="source_code")
        if not path.is_file() or _purpose_sha256(path) != pin["sha256"]:
            raise NcenError("purpose_export_source_code_pin_mismatch")
    declared_root_ids = {item["root_id"] for item in declared["input_artifacts"]}
    if set(input_roots) != declared_root_ids:
        raise NcenError("purpose_export_input_roots_mismatch")
    for pin in declared["input_artifacts"]:
        root = Path(input_roots[pin["root_id"]])
        if not root.is_dir() or root.is_symlink():
            raise NcenError("purpose_export_input_root_invalid")
        path = _purpose_safe_path(root, pin["path"], code="input_artifact")
        if (
            not path.is_file()
            or path.stat().st_size != pin["bytes"]
            or _purpose_sha256(path) != pin["sha256"]
        ):
            raise NcenError("purpose_export_input_artifact_mismatch")


def _purpose_validate_raw_source_bindings(
    declaration: Mapping[str, Any],
    source_rows: Sequence[Mapping[str, Any]],
) -> None:
    pins_by_path: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for pin in declaration["input_artifacts"]:
        pins_by_path[pin["path"]].append(pin)
    for source in source_rows:
        matches = [
            pin
            for pin in pins_by_path.get(source["artifact_path"], ())
            if pin["sha256"] == source["artifact_sha256"]
        ]
        if len(matches) != 1:
            raise NcenError("purpose_export_source_artifact_binding_invalid")


def _purpose_reconstruct_declared_sources(
    declaration: Mapping[str, Any],
    *,
    input_roots: Mapping[str, Path],
) -> DiagnosticSourceIndex:
    source_manifest = declaration["ncen_source_manifest"]
    matches = [
        item
        for item in declaration["input_artifacts"]
        if item["sha256"] == source_manifest["sha256"]
        and item["bytes"] == source_manifest["bytes"]
    ]
    if len(matches) != 1:
        raise NcenError("purpose_export_source_manifest_artifact_ambiguous")
    manifest_pin = matches[0]
    manifest_path = _purpose_safe_path(
        input_roots[manifest_pin["root_id"]],
        manifest_pin["path"],
        code="source_manifest",
    )
    sources = read_diagnostic_source_rows(
        DiagnosticSourceManifestPin(
            manifest_path=manifest_path,
            manifest_sha256=source_manifest["sha256"],
            manifest_size=source_manifest["bytes"],
        )
    )
    if (
        sources.manifest_kind != source_manifest["kind"]
        or sources.evidence_digest != source_manifest["evidence_digest"]
        or sources.evidence_digest != declaration["ncen_evidence_digest"]
    ):
        raise NcenError("purpose_export_source_manifest_reconstruction_mismatch")
    return sources


def _purpose_validate_performance(value: Any) -> dict[str, Any]:
    keys = frozenset(
        {
            "schema_version",
            "started_at",
            "finished_at",
            "elapsed_seconds",
            "peak_rss_bytes",
            "memory_limit_bytes",
            "contexts_completed",
            "source_rows_read",
            "incidences_emitted",
            "union_attempts",
            "successful_unions",
            "raw_nport_parse_calls",
            "inventory_build_calls",
            "target_vote_calls",
            "stop_reason",
        }
    )
    performance = _purpose_exact_object(value, keys, code="performance")
    if performance["schema_version"] != DIAGNOSTIC_PERFORMANCE_VERSION:
        raise NcenError("performance_schema_version_invalid")
    started = _purpose_parse_timestamp(performance["started_at"], code="performance_started_at")
    finished = _purpose_parse_timestamp(
        performance["finished_at"], code="performance_finished_at"
    )
    if finished < started:
        raise NcenError("performance_time_order_invalid")
    elapsed = performance["elapsed_seconds"]
    if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or elapsed < 0:
        raise NcenError("performance_elapsed_invalid")
    for field_name in (
        "peak_rss_bytes",
        "memory_limit_bytes",
        "contexts_completed",
        "source_rows_read",
        "incidences_emitted",
        "union_attempts",
        "successful_unions",
        "raw_nport_parse_calls",
        "inventory_build_calls",
        "target_vote_calls",
    ):
        _purpose_nonnegative(performance[field_name], code=f"performance_{field_name}")
    if (
        performance["memory_limit_bytes"] != DIAGNOSTIC_MEMORY_LIMIT_BYTES
        or performance["peak_rss_bytes"] >= DIAGNOSTIC_MEMORY_LIMIT_BYTES
        or any(
            performance[field_name] != 0
            for field_name in (
                "raw_nport_parse_calls",
                "inventory_build_calls",
                "target_vote_calls",
            )
        )
        or performance["stop_reason"] is not None
    ):
        raise NcenError("performance_gate_invalid")
    return performance


def _purpose_validate_checks(value: Any) -> dict[str, Any]:
    _purpose_v2_control_shape(value, kind="checks")
    checks = _purpose_exact_object(
        value,
        frozenset({"schema_version", "checks", "validation_code_sha256", "status"})
        | _PURPOSE_V2_BINDING_KEYS,
        code="checks",
    )
    if checks["schema_version"] != DIAGNOSTIC_CHECKS_VERSION:
        raise NcenError("checks_schema_version_invalid")
    _purpose_v2_bindings(checks)
    if checks["status"] != "staged":
        raise NcenError("checks_status_invalid")
    _diagnostic_hash_valid(checks["validation_code_sha256"], "validation_code_sha256")
    if not isinstance(checks["checks"], list) or not checks["checks"]:
        raise NcenError("checks_list_invalid")
    names: list[str] = []
    for raw in checks["checks"]:
        item = _purpose_exact_object(
            raw,
            frozenset({"name", "status", "details"}),
            code="check",
        )
        _diagnostic_nonempty(item["name"], "check_name")
        _diagnostic_nonempty(item["details"], "check_details")
        if item["status"] != "passed":
            raise NcenError("check_not_passed")
        names.append(item["name"])
    if names != sorted(set(names)):
        raise NcenError("checks_not_sorted_unique")
    return checks


def _purpose_manifest(value: Any) -> dict[str, Any]:
    _purpose_v2_control_shape(value, kind="manifest")
    manifest = _purpose_exact_object(
        value,
        frozenset(
            {
                "schema_version",
                "declaration_sha256",
                "baseline_seals",
                "source_code",
                "runtime",
                "input_artifacts",
                "files",
                "coverage",
                "status",
                "reasons",
                "issuer_group_ref_sha256",
                "outcome_inputs_used",
                "diagnostic_only",
                "qualification",
            }
        ) | _PURPOSE_V2_BINDING_KEYS,
        code="manifest",
    )
    _purpose_v2_bindings(manifest)
    if (
        manifest["schema_version"] != DIAGNOSTIC_MANIFEST_VERSION
        or manifest["status"] != "staged"
        or manifest["reasons"] != []
        or manifest["outcome_inputs_used"] is not False
        or manifest["diagnostic_only"] is not True
        or manifest["qualification"] != "NOT_EVALUABLE"
    ):
        raise NcenError("manifest_complete_status_forged")
    _diagnostic_hash_valid(manifest["declaration_sha256"], "manifest_declaration_sha256")
    _diagnostic_hash_valid(
        manifest["issuer_group_ref_sha256"], "manifest_issuer_group_ref_sha256"
    )
    _purpose_validate_pin_rows(
        manifest["baseline_seals"],
        keys=frozenset({"name", "sha256"}),
        code="manifest_baseline_seal",
        require_bytes=False,
    )
    _purpose_validate_pin_rows(
        manifest["source_code"],
        keys=frozenset({"path", "sha256"}),
        code="manifest_source_code",
        require_bytes=False,
    )
    _purpose_validate_pin_rows(
        manifest["input_artifacts"],
        keys=frozenset({"root_id", "path", "sha256", "bytes"}),
        code="manifest_input_artifact",
        require_bytes=True,
    )
    expected_file_types = {**dict(DIAGNOSTIC_EXPORT_JSONL),
                           "selection_inventory.json": "selection_inventory",
                           "issuer_group_ref.json": "issuer_group_ref",
                           "performance.json": "performance", "checks.json": "checks"}
    files = manifest["files"]
    if type(files) is not list or len(files) != len(expected_file_types):
        raise NcenError("manifest_files_not_closed_or_sorted")
    paths = []
    for entry in files:
        item = _purpose_exact_object(entry, frozenset({"path", "sha256", "bytes", "rows", "record_type"}),
                                     code="manifest_file")
        path = item["path"]
        if path not in expected_file_types or item["record_type"] != expected_file_types[path]:
            raise NcenError("manifest_files_not_closed_or_sorted")
        _diagnostic_hash_valid(item["sha256"], "manifest_file_sha256")
        _purpose_nonnegative(item["bytes"], code="manifest_file_bytes")
        _purpose_nonnegative(item["rows"], code="manifest_file_rows")
        if (path == "selection_inventory.json" and item["rows"] != 1
                or path == "selections.jsonl" and item["rows"] == 0):
            raise NcenError("manifest_file_rows_invalid")
        paths.append(path)
    if paths != sorted(expected_file_types):
        raise NcenError("manifest_files_not_closed_or_sorted")
    runtime = _purpose_exact_object(
        manifest["runtime"],
        frozenset({"python_version", "unicode_version", "platform"}),
        code="manifest_runtime",
    )
    for field_name in runtime:
        _diagnostic_nonempty(runtime[field_name], f"manifest_runtime_{field_name}")
    coverage = _purpose_exact_object(
        manifest["coverage"],
        frozenset(
            {
                "requested_contexts",
                "completed_contexts",
                "missing_contexts",
                "requested_accessions",
                "verified_accessions",
                "quarantined_accessions",
            }
        ),
        code="manifest_coverage",
    )
    for field_name in coverage:
        _purpose_nonnegative(coverage[field_name], code=f"manifest_coverage_{field_name}")
    if (
        coverage["missing_contexts"] != 0
        or coverage["requested_contexts"] != coverage["completed_contexts"]
        or coverage["verified_accessions"] + coverage["quarantined_accessions"]
        != coverage["requested_accessions"]
    ):
        raise NcenError("manifest_coverage_not_complete")
    return manifest


def _purpose_receipt(value: Any) -> dict[str, Any]:
    receipt = _purpose_exact_object(
        value,
        frozenset(
            {
                "schema_version",
                "artifact_directory",
                "sha256sums_sha256",
                "manifest_sha256",
                "declaration_sha256",
                "status",
                "predecessor_receipt_sha256",
                "diagnostic_only",
                "qualification",
            }
        ) | _PURPOSE_V2_BINDING_KEYS,
        code="receipt",
    )
    _purpose_v2_bindings(receipt)
    if (
        receipt["schema_version"] != DIAGNOSTIC_RECEIPT_VERSION
        or receipt["status"] != "diagnostic_synthetic_complete"
        or receipt["diagnostic_only"] is not True
        or receipt["predecessor_receipt_sha256"] is not None
    ):
        raise NcenError("receipt_status_invalid")
    _diagnostic_nonempty(receipt["artifact_directory"], "receipt_artifact_directory")
    for field_name in ("sha256sums_sha256", "manifest_sha256", "declaration_sha256"):
        _diagnostic_hash_valid(receipt[field_name], f"receipt_{field_name}")
    if receipt["predecessor_receipt_sha256"] is not None:
        _diagnostic_hash_valid(
            receipt["predecessor_receipt_sha256"], "receipt_predecessor_sha256"
        )
    return receipt


def _purpose_expected_root_names() -> list[str]:
    return sorted(
        {
            *(path for path, _record_type in DIAGNOSTIC_EXPORT_JSONL),
            "selection_inventory.json",
            "issuer_group_ref.json",
            "performance.json",
            "checks.json",
            "declaration.json",
            "manifest.json",
            "SHA256SUMS",
        }
    )


def _purpose_validate_export_root(
    root: Path,
    *,
    receipt_path: Path | None,
    code_root: Path,
    input_roots: Mapping[str, Path],
    require_receipt: bool,
) -> dict[str, Any]:
    # C1b must replace self-consistency with independent selection replay before issuance.
    raise NcenError("C1_INCOMPLETE")
    if not root.is_dir() or root.is_symlink():
        raise NcenError("purpose_export_root_invalid")
    names = sorted(item.name for item in root.iterdir())
    if names != _purpose_expected_root_names():
        raise NcenError("purpose_export_artifact_inventory_not_closed")
    for name in names:
        path = _purpose_safe_path(root, name, code="purpose_export")
        if not path.is_file() or path.is_symlink():
            raise NcenError("purpose_export_non_regular_file")
    declaration_path = root / "declaration.json"
    declaration = _purpose_declaration(
        _purpose_json_load(declaration_path.read_bytes(), code="declaration")
    )
    if declaration_path.read_bytes() != _purpose_json_bytes(declaration):
        raise NcenError("declaration_not_canonical")
    _purpose_verify_declared_inputs(
        declaration,
        code_root=code_root,
        input_roots=input_roots,
    )
    manifest_path = root / "manifest.json"
    manifest = _purpose_manifest(
        _purpose_json_load(manifest_path.read_bytes(), code="manifest")
    )
    if manifest_path.read_bytes() != _purpose_json_bytes(manifest):
        raise NcenError("manifest_not_canonical")
    if (
        manifest["declaration_sha256"] != _purpose_sha256(declaration_path)
        or manifest["baseline_seals"] != declaration["baseline_seals"]
        or manifest["source_code"] != declaration["source_code"]
        or manifest["input_artifacts"] != declaration["input_artifacts"]
    ):
        raise NcenError("manifest_declaration_binding_mismatch")
    expected_file_types = dict(DIAGNOSTIC_EXPORT_JSONL)
    expected_file_types.update(
        {
            "selection_inventory.json": "selection_inventory",
            "issuer_group_ref.json": "issuer_group_ref",
            "performance.json": "performance",
            "checks.json": "checks",
        }
    )
    files = manifest["files"]
    if not isinstance(files, list):
        raise NcenError("manifest_files_invalid")
    parsed_file_rows: dict[str, dict[str, Any]] = {}
    for raw_file in files:
        item = _purpose_exact_object(
            raw_file,
            frozenset({"path", "sha256", "bytes", "rows", "record_type"}),
            code="manifest_file",
        )
        _purpose_relative_path_value(item["path"], code="manifest_file")
        _diagnostic_hash_valid(item["sha256"], "manifest_file_sha256")
        _purpose_nonnegative(item["bytes"], code="manifest_file_bytes")
        _purpose_nonnegative(item["rows"], code="manifest_file_rows")
        if item["path"] in parsed_file_rows:
            raise NcenError("manifest_duplicate_path")
        parsed_file_rows[item["path"]] = item
    if list(parsed_file_rows) != sorted(expected_file_types) or set(parsed_file_rows) != set(
        expected_file_types
    ):
        raise NcenError("manifest_files_not_closed_or_sorted")
    records: dict[str, tuple[dict[str, Any], ...]] = {}
    for path_name, record_type in DIAGNOSTIC_EXPORT_JSONL:
        path = root / path_name
        pin = parsed_file_rows[path_name]
        if (
            pin["record_type"] != record_type
            or path.stat().st_size != pin["bytes"]
            or _purpose_sha256(path) != pin["sha256"]
        ):
            raise NcenError("manifest_jsonl_file_mismatch")
        rows = _purpose_read_jsonl(path, record_type=record_type)
        if len(rows) != pin["rows"]:
            raise NcenError("manifest_jsonl_row_count_mismatch")
        records[path_name] = rows
    canonical_records = _purpose_canonical_records(declaration, records)
    if any(records[path] != canonical_records[path] for path in records):
        raise NcenError("purpose_export_record_order_invalid")
    issuer_path = root / "issuer_group_ref.json"
    issuer_ref = _purpose_json_load(issuer_path.read_bytes(), code="issuer_group_ref")
    if issuer_ref != _purpose_issuer_group_ref() or issuer_path.read_bytes() != _purpose_json_bytes(
        issuer_ref
    ):
        raise NcenError("issuer_group_ref_invalid")
    performance_path = root / "performance.json"
    performance = _purpose_validate_performance(
        _purpose_json_load(performance_path.read_bytes(), code="performance")
    )
    checks_path = root / "checks.json"
    checks = _purpose_validate_checks(
        _purpose_json_load(checks_path.read_bytes(), code="checks")
    )
    for path_name, value in (
        ("issuer_group_ref.json", issuer_ref),
        ("performance.json", performance),
        ("checks.json", checks),
    ):
        path = root / path_name
        pin = parsed_file_rows[path_name]
        if (
            pin["record_type"] != expected_file_types[path_name]
            or pin["rows"] != 1
            or path.stat().st_size != pin["bytes"]
            or _purpose_sha256(path) != pin["sha256"]
            or path.read_bytes() != _purpose_json_bytes(value)
        ):
            raise NcenError("manifest_json_file_mismatch")
    if manifest["issuer_group_ref_sha256"] != _purpose_sha256(issuer_path):
        raise NcenError("manifest_issuer_ref_mismatch")
    _purpose_validate_raw_source_bindings(declaration, records["sources.jsonl"])
    reconstructed_sources = _purpose_reconstruct_declared_sources(
        declaration,
        input_roots=input_roots,
    )
    reconstructed_rows, reconstructed_ids = _purpose_source_records(reconstructed_sources)
    if tuple(records["sources.jsonl"]) != reconstructed_rows:
        raise NcenError("purpose_export_source_row_derivation_mismatch")
    expected_quarantine = {
        _diagnostic_canonical(item)
        for item in _purpose_exclusion_records((), reconstructed_sources, reconstructed_ids)
    }
    actual_quarantine = {
        _diagnostic_canonical(dict(item))
        for item in records["exclusions.jsonl"]
        if item["context_id"] is None
    }
    if actual_quarantine != expected_quarantine:
        raise NcenError("purpose_export_quarantine_ledger_mismatch")
    _purpose_validate_semantics(declaration, records)
    coverage = manifest["coverage"]
    verified_accessions = {
        item["accession"]
        for item in records["sources.jsonl"]
        if item["custody_state"] == "verified"
    }
    quarantined_accessions = {
        item["accession"]
        for item in records["exclusions.jsonl"]
        if item["state"] == "excluded_quarantine"
    }
    if coverage != {
        "requested_contexts": len(declaration["contexts"]),
        "completed_contexts": len(records["contexts.jsonl"]),
        "missing_contexts": 0,
        "requested_accessions": len(verified_accessions | quarantined_accessions),
        "verified_accessions": len(verified_accessions),
        "quarantined_accessions": len(quarantined_accessions),
    }:
        raise NcenError("manifest_coverage_reconciliation_mismatch")
    if (
        performance["contexts_completed"] != len(records["contexts.jsonl"])
        or performance["source_rows_read"] != len(records["sources.jsonl"])
        or performance["incidences_emitted"] != len(records["incidences.jsonl"])
    ):
        raise NcenError("performance_count_reconciliation_mismatch")
    sums_paths = sorted({*parsed_file_rows, "declaration.json", "manifest.json"})
    expected_sums = "".join(
        f"{_purpose_sha256(root / path)}  {path}\n" for path in sums_paths
    ).encode("ascii")
    sums_path = root / "SHA256SUMS"
    if sums_path.read_bytes() != expected_sums:
        raise NcenError("purpose_export_sha256sums_invalid")
    if require_receipt:
        if receipt_path is None or receipt_path.parent.resolve() == root.resolve():
            raise NcenError("purpose_export_external_receipt_required")
        if not receipt_path.is_file() or receipt_path.is_symlink():
            raise NcenError("purpose_export_receipt_missing")
        receipt = _purpose_receipt(
            _purpose_json_load(receipt_path.read_bytes(), code="receipt")
        )
        if receipt_path.read_bytes() != _purpose_json_bytes(receipt):
            raise NcenError("receipt_not_canonical")
        if (
            receipt["artifact_directory"] != root.name
            or receipt["sha256sums_sha256"] != _purpose_sha256(sums_path)
            or receipt["manifest_sha256"] != _purpose_sha256(manifest_path)
            or receipt["declaration_sha256"] != _purpose_sha256(declaration_path)
            or receipt["predecessor_receipt_sha256"]
            != declaration["predecessor_receipt_sha256"]
        ):
            raise NcenError("receipt_envelope_binding_mismatch")
    return manifest


def validate_purpose_export(
    root: str | Path,
    *,
    trusted_run: DiagnosticTrustPin,
    code_root: str | Path,
    input_roots: Mapping[str, str | Path],
    receipt_path: str | Path | None = None,
) -> dict[str, Any]:
    """Check the public boundary without accepting an export before C2."""
    export_root = Path(root)
    if not export_root.is_dir() or export_root.is_symlink():
        raise NcenError("purpose_export_root_invalid")
    declaration_path = _purpose_safe_path(export_root, "declaration.json", code="purpose_export")
    declaration = _purpose_preliminary_json(declaration_path, code="declaration")
    if not isinstance(declaration, dict) or declaration.get("schema_version") != DIAGNOSTIC_DECLARATION_VERSION:
        raise NcenError("diagnostic_export_schema_unsupported")
    if declaration.get("predecessor_receipt_sha256") is not None:
        raise NcenError("diagnostic_resume_not_supported")
    versions = ((export_root / "manifest.json", DIAGNOSTIC_MANIFEST_VERSION),
                (export_root / "checks.json", DIAGNOSTIC_CHECKS_VERSION),
                (Path(receipt_path) if receipt_path is not None else export_root.parent / f"{export_root.name}.receipt.json",
                 DIAGNOSTIC_RECEIPT_VERSION))
    for path, version in versions:
        if path.exists():
            if path.is_symlink() or getattr(path.lstat(), "st_file_attributes", 0) & 0x400:
                raise NcenError("diagnostic_export_schema_unsupported")
            envelope = _purpose_preliminary_json(path, code="diagnostic_export_schema_unsupported")
            if not isinstance(envelope, dict) or envelope.get("schema_version") != version:
                raise NcenError("diagnostic_export_schema_unsupported")
    _purpose_preliminary_trust(trusted_run, root=export_root, declaration=declaration)
    raise NcenError("C1_INCOMPLETE")
    resolved_receipt = (
        Path(receipt_path)
        if receipt_path is not None
        else export_root.parent / f"{export_root.name}.receipt.json"
    )
    return _purpose_validate_export_root(
        export_root,
        receipt_path=resolved_receipt,
        code_root=Path(code_root) if code_root is not None else Path(__file__).resolve().parents[3],
        input_roots={key: Path(value) for key, value in (input_roots or {}).items()},
        require_receipt=True,
    )


@dataclass(frozen=True, slots=True)
class PurposeExportResult:
    root: Path
    receipt_path: Path
    declaration_sha256: str
    manifest_sha256: str
    sha256sums_sha256: str
    receipt_sha256: str


@dataclass(frozen=True, slots=True)
class StagedPurposeArtifact:
    root: Path
    declaration_sha256: str
    manifest_sha256: str
    sha256sums_sha256: str
    baseline_digest: str
    selection_expected_digest: str
    status: str = "staged"


def _stage_purpose_diagnostics_v2(
    *,
    trusted_run: DiagnosticTrustPin,
    declaration: Mapping[str, Any],
    staging_root: Path,
    code_root: Path,
    input_roots: Mapping[str, Path],
    monitor: DiagnosticResourceMonitor,
) -> StagedPurposeArtifact:
    """Write nonaccepting synthetic evidence from one freshly admitted Q3/C1b2a replay."""
    import os
    import platform
    import time
    import unicodedata

    if not isinstance(monitor, DiagnosticResourceMonitor):
        raise TypeError("monitor_must_be_DiagnosticResourceMonitor")
    output = Path(staging_root)
    if output.exists() or output.is_symlink():
        raise NcenError("purpose_export_directory_must_be_new")
    if not output.parent.is_dir():
        raise NcenError("purpose_export_parent_missing")
    for ancestor in (output.parent, *output.parent.parents):
        status = ancestor.lstat()
        if ancestor.is_symlink() or getattr(status, "st_file_attributes", 0) & 0x400:
            raise NcenError("purpose_export_parent_unsafe")
    if not isinstance(declaration, Mapping):
        raise NcenError("diagnostic_export_schema_unsupported")
    if declaration.get("lane") == "pure_fixture":
        raise NcenError("diagnostic_fixture_lane_not_exportable")
    if declaration.get("lane") == "sealed_source":
        raise NcenError("diagnostic_required_pins_unverified")
    if type(trusted_run) is not DiagnosticTrustPin:
        raise NcenError("diagnostic_trust_anchor_missing")
    declared = _purpose_declaration(declaration)
    monitor.check("purpose_stage_start")
    started_wall = dt.datetime.now(UTC)
    started_clock = time.perf_counter()

    # Q3 reloads the original F1 and membership handles, retaining their identity for C1b2a.
    baseline = read_diagnostic_baseline_checkpoint(
        trusted_run, code_root=Path(code_root), input_roots=input_roots, monitor=monitor,
    )
    issuance = _DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    source = issuance.source_index_ref
    membership = issuance.membership_ref
    admitted = _diagnostic_bound_baseline_admission(
        baseline, trusted_run=trusted_run, source_index=source,
        membership_checkpoint=membership,
    )
    roles = admitted["roles"]
    code_files = admitted["runtime_code"]["files"]
    excluded = {"runtime_code_manifest", "baseline_v3", "baseline_stage1", "baseline_stage2a",
                "baseline_stage2b"}
    authorized_artifacts = sorted(({
        key: pin[key] for key in ("root_id", "path", "sha256", "bytes")
    } for name, pins in roles.items() if name not in excluded for pin in pins),
        key=lambda pin: (pin["root_id"], pin["path"]))
    custody = _diagnostic_bound_source_custody(source)
    checkpoint = admitted["checkpoint"]
    if (declared["trusted_run_manifest_sha256"] != trusted_run.manifest_sha256
            or declared["lane"] != admitted["lane"]
            or declared["pin_roles"] != [
                {"role": name, "pins": pins} for name, pins in sorted(roles.items())]
            or declared["source_code"] != [
                {"path": item["path"], "sha256": item["sha256"]} for item in code_files]
            or declared["baseline_seals"] != [
                {"name": name, "sha256": roles[name][0]["sha256"]}
                for name in sorted(excluded - {"runtime_code_manifest"})]
            or declared["input_artifacts"] != authorized_artifacts
            or declared["ncen_source_manifest"] != {
                "sha256": custody.manifest_sha256, "bytes": custody.manifest_size,
                "kind": custody.manifest_kind, "evidence_digest": custody.evidence_digest}
            or declared["quarantine_seal"] != {
                "scope_sha256": roles["acquisition_scope"][0]["sha256"],
                "sha256sums_sha256": roles["acquisition_sha256sums"][0]["sha256"]}
            or any(declared[name] != checkpoint[name] for name in (
                "contexts", "inventory_digest", "cohort_digest", "ncen_evidence_digest",
                "exclusion_ledger_digest"))
            or declared["baseline_digest"] != baseline.digest):
        raise NcenError("diagnostic_declared_pin_mismatch")
    if len(source.rows) > declared["limits"]["max_source_rows"]:
        raise NcenError("purpose_export_source_row_limit_exceeded")

    snapshots: list[PurposeContextSnapshot] = []
    expected = replay_purpose_selections(
        baseline, trusted_run, source_index=source, membership_checkpoint=membership,
        monitor=monitor, _snapshots=snapshots,
    )
    if declared["selection_expected_digest"] != expected.digest:
        raise NcenError("diagnostic_selection_replay_mismatch")
    inventory, selection_rows = _purpose_serialize_selection_replay(expected)
    compare_purpose_selection_records(expected, _purpose_reconstruct_emitted_replay(
        inventory, selection_rows))
    cohort = _purpose_reconstruct_cohort(declared, admitted["members"])
    if cohort.members != membership.members or len(snapshots) != len(declared["contexts"]):
        raise NcenError("diagnostic_cohort_baseline_mismatch")
    raw_records, counters = _purpose_build_records(cohort, source, declared, tuple(snapshots))
    raw_records["selections.jsonl"] = selection_rows
    records = _purpose_canonical_records(declared, raw_records)
    replay = expected.records
    flattened = {
        "contexts.jsonl": [item["context_record"] for item in replay["contexts"]],
        "nodes.jsonl": [row for item in replay["contexts"] for row in item["nodes"]],
        "reported_families.jsonl": [row for item in replay["contexts"]
                                    for row in item["reported_families"]],
        "incidences.jsonl": [row for item in replay["contexts"] for row in item["incidences"]],
        "sources.jsonl": replay["sources"],
        "exclusions.jsonl": [*replay["global_exclusions"],
                             *(row for item in replay["contexts"] for row in item["exclusions"])],
    }
    for name, rows in flattened.items():
        if records[name] != _purpose_sort_records(
            name, rows, context_index={item["context_id"]: index for index, item in enumerate(replay["contexts"])},
            ablation_index={spec.ablation_id: index for index, spec in enumerate(DIAGNOSTIC_ABLATIONS)},
            report_index={item["R"]: index for index, item in enumerate(replay["contexts"])},
        ):
            raise NcenError("diagnostic_selection_replay_mismatch")
    monitor.check("purpose_stage_graph")
    _purpose_validate_semantics(declared, records)

    output.mkdir()
    declaration_bytes = _purpose_json_bytes(declared)
    _purpose_write_partial(output / "declaration.json.partial", declaration_bytes)
    if _purpose_sha256(output / "declaration.json.partial") != hashlib.sha256(declaration_bytes).hexdigest():
        raise NcenError("purpose_export_declaration_bytes_mismatch")
    _purpose_finalize_partial(output / "declaration.json.partial")
    monitor.check("purpose_stage_declaration")

    def write_data(name: str, payload: bytes) -> None:
        temporary = output / f"{name}.partial"
        _purpose_write_partial(temporary, payload)
        if (temporary.stat().st_size != len(payload)
                or _purpose_sha256(temporary) != hashlib.sha256(payload).hexdigest()):
            raise NcenError("purpose_export_staged_bytes_mismatch")
        _purpose_finalize_partial(temporary)
        monitor.check("purpose_stage_data_file")

    write_data("selection_inventory.json", _purpose_json_bytes(inventory))
    selection_partial = output / "selections.jsonl.partial"
    with selection_partial.open("xb") as handle:
        for row in selection_rows:
            payload = _diagnostic_canonical(row) + b"\n"
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            monitor.rows_read(1, "purpose_stage_selection_row")
            monitor.check("purpose_stage_selection_context")
    selection_bytes = _purpose_jsonl_bytes(selection_rows)
    if (selection_partial.stat().st_size != len(selection_bytes)
            or _purpose_sha256(selection_partial) != hashlib.sha256(selection_bytes).hexdigest()):
        raise NcenError("purpose_export_staged_bytes_mismatch")
    _purpose_finalize_partial(selection_partial)
    for name, _kind in DIAGNOSTIC_EXPORT_JSONL:
        if name != "selections.jsonl":
            write_data(name, _purpose_jsonl_bytes(records[name]))
    issuer_ref = _purpose_issuer_group_ref()
    write_data("issuer_group_ref.json", _purpose_json_bytes(issuer_ref))
    finished_wall = dt.datetime.now(UTC)
    monitor.check("purpose_stage_performance")
    performance = {
        "schema_version": DIAGNOSTIC_PERFORMANCE_VERSION,
        "started_at": _diagnostic_timestamp(started_wall),
        "finished_at": _diagnostic_timestamp(finished_wall),
        "elapsed_seconds": max(0.0, time.perf_counter() - started_clock),
        "peak_rss_bytes": monitor.report()["rss_max_observed"],
        "memory_limit_bytes": DIAGNOSTIC_MEMORY_LIMIT_BYTES,
        "contexts_completed": len(snapshots), "source_rows_read": len(source.rows),
        "incidences_emitted": counters["incidences_emitted"],
        "union_attempts": counters["union_attempts"],
        "successful_unions": counters["successful_unions"],
        "raw_nport_parse_calls": 0, "inventory_build_calls": 0, "target_vote_calls": 0,
        "stop_reason": None,
    }
    _purpose_validate_performance(performance)
    write_data("performance.json", _purpose_json_bytes(performance))
    bindings = {key: declared[key] for key in _PURPOSE_V2_BINDING_KEYS}
    checks = {
        "schema_version": DIAGNOSTIC_CHECKS_VERSION, **bindings,
        "status": "staged", "validation_code_sha256": _purpose_sha256(Path(__file__)),
        "checks": [
            {"name": "canonical_schema", "status": "passed", "details": "canonical v2 bytes staged"},
            {"name": "graph_derivation", "status": "passed", "details": "F4/F9-derived records checked before staging"},
            {"name": "pinned_selection_replay", "status": "passed", "details": "Q3/C1b2a same-handle replay compared"},
        ],
    }
    _purpose_validate_checks(checks)
    write_data("checks.json", _purpose_json_bytes(checks))
    file_types = {**dict(DIAGNOSTIC_EXPORT_JSONL), "selection_inventory.json": "selection_inventory",
                  "issuer_group_ref.json": "issuer_group_ref", "performance.json": "performance",
                  "checks.json": "checks"}
    file_rows = {name: len(records[name]) for name, _kind in DIAGNOSTIC_EXPORT_JSONL}
    file_rows.update({"selection_inventory.json": 1, "issuer_group_ref.json": 1,
                      "performance.json": 1, "checks.json": 1})
    verified_accessions = {row.accession_number for row in source.rows if row.custody_state == "verified"}
    quarantined_accessions = {row.accession_number for row in source.exclusions}
    manifest = {
        "schema_version": DIAGNOSTIC_MANIFEST_VERSION, **bindings,
        "declaration_sha256": _purpose_sha256(output / "declaration.json"),
        "baseline_seals": declared["baseline_seals"], "source_code": declared["source_code"],
        "runtime": {"python_version": platform.python_version(),
                    "unicode_version": unicodedata.unidata_version, "platform": platform.platform()},
        "input_artifacts": declared["input_artifacts"],
        "files": [{"path": name, "sha256": _purpose_sha256(output / name),
                   "bytes": (output / name).stat().st_size, "rows": file_rows[name],
                   "record_type": file_types[name]} for name in sorted(file_types)],
        "coverage": {"requested_contexts": len(declared["contexts"]),
                     "completed_contexts": len(snapshots), "missing_contexts": 0,
                     "requested_accessions": len(verified_accessions | quarantined_accessions),
                     "verified_accessions": len(verified_accessions),
                     "quarantined_accessions": len(quarantined_accessions)},
        "status": "staged", "reasons": [],
        "issuer_group_ref_sha256": _purpose_sha256(output / "issuer_group_ref.json"),
        "outcome_inputs_used": False, "diagnostic_only": True,
    }
    _purpose_manifest(manifest)
    write_data("manifest.json", _purpose_json_bytes(manifest))
    sum_paths = sorted({*file_types, "declaration.json", "manifest.json"})
    write_data("SHA256SUMS", "".join(
        f"{_purpose_sha256(output / name)}  {name}\n" for name in sum_paths).encode("ascii"))
    monitor.check("purpose_stage_complete")
    return StagedPurposeArtifact(
        root=output, declaration_sha256=manifest["declaration_sha256"],
        manifest_sha256=_purpose_sha256(output / "manifest.json"),
        sha256sums_sha256=_purpose_sha256(output / "SHA256SUMS"),
        baseline_digest=baseline.digest, selection_expected_digest=expected.digest,
    )


@dataclass(frozen=True, slots=True)
class SyntheticReadbackEvidence:
    declaration_sha256: str
    manifest_sha256: str
    sha256sums_sha256: str
    baseline_digest: str
    selection_replay_digest: str
    context_count: int
    source_count: int
    incidence_count: int
    status: str = "diagnostic_synthetic_readback_verified"
    acceptance_scope: str = "synthetic_engineering_only"
    publicly_accepted: bool = False
    qualification: str = "NOT_EVALUABLE"
    resource_qualification: str = "NOT_EVALUABLE"
    resume_qualification: str = "NOT_EVALUABLE"


def _validate_purpose_staging_v2(
    *,
    trusted_run: DiagnosticTrustPin,
    staging_root: Path,
    code_root: Path,
    input_roots: Mapping[str, Path],
    monitor: DiagnosticResourceMonitor,
) -> SyntheticReadbackEvidence:
    """Independently read a complete synthetic stage; never issue acceptance or a receipt."""
    import stat as stat_module

    if not isinstance(monitor, DiagnosticResourceMonitor):
        raise TypeError("monitor_must_be_DiagnosticResourceMonitor")
    root = Path(staging_root)
    if not root.is_dir():
        raise NcenError("diagnostic_export_incomplete")
    for ancestor in (root, *root.parents):
        status = ancestor.lstat()
        if (not stat_module.S_ISDIR(status.st_mode)
                or getattr(status, "st_file_attributes", 0) & 0x400):
            raise NcenError("purpose_export_root_invalid")
    monitor.check("purpose_readback_start")
    declaration_path = root / "declaration.json"
    for name in ("declaration.json", "manifest.json", "checks.json"):
        if (root / name).is_symlink():
            raise NcenError("purpose_export_non_regular_file")
    declaration_bytes = _purpose_preliminary_bytes(declaration_path, code="declaration")
    try:
        declaration_raw = _purpose_json_load(declaration_bytes, code="declaration")
    except NcenError as exc:
        raise NcenError("diagnostic_pinned_input_invalid") from exc
    if (type(declaration_raw) is not dict
            or type(declaration_raw.get("schema_version")) is not str):
        raise NcenError("diagnostic_pinned_input_invalid")
    if declaration_raw["schema_version"] != DIAGNOSTIC_DECLARATION_VERSION:
        raise NcenError("diagnostic_export_schema_unsupported")
    declared = _purpose_declaration(declaration_raw)
    if declaration_bytes != _purpose_json_bytes(declared):
        raise NcenError("declaration_not_canonical")
    declaration_sha = hashlib.sha256(declaration_bytes).hexdigest()
    manifest_path = root / "manifest.json"
    checks_path = root / "checks.json"
    manifest_bytes = _purpose_preliminary_bytes(manifest_path, code="manifest")
    checks_bytes = _purpose_preliminary_bytes(checks_path, code="checks")
    try:
        manifest_raw = _purpose_json_load(manifest_bytes, code="manifest")
        checks_raw = _purpose_json_load(checks_bytes, code="checks")
    except NcenError as exc:
        raise NcenError("diagnostic_pinned_input_invalid") from exc
    if (type(manifest_raw) is not dict or type(manifest_raw.get("schema_version")) is not str
            or type(checks_raw) is not dict or type(checks_raw.get("schema_version")) is not str):
        raise NcenError("diagnostic_pinned_input_invalid")
    if (manifest_raw["schema_version"] != DIAGNOSTIC_MANIFEST_VERSION
            or checks_raw["schema_version"] != DIAGNOSTIC_CHECKS_VERSION):
        raise NcenError("diagnostic_export_schema_unsupported")
    if type(manifest_raw.get("files")) is list:
        for item in manifest_raw["files"]:
            if type(item) is dict and isinstance(item.get("path"), str):
                _purpose_safe_path(root, item["path"], code="manifest_file")
    manifest = _purpose_manifest(manifest_raw)
    checks = _purpose_validate_checks(checks_raw)
    for raw, document, label in ((manifest_bytes, manifest, "manifest"),
                                 (checks_bytes, checks, "checks")):
        if raw != _purpose_json_bytes(document):
            raise NcenError(f"{label}_not_canonical")
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    if declared["predecessor_receipt_sha256"] is not None:
        raise NcenError("diagnostic_resume_not_supported")
    for name in _PURPOSE_V2_BINDING_KEYS:
        if manifest[name] != declared[name] or checks[name] != declared[name]:
            raise NcenError("diagnostic_declared_pin_mismatch")
    if (manifest["declaration_sha256"] != declaration_sha
            or manifest["baseline_seals"] != declared["baseline_seals"]
            or manifest["source_code"] != declared["source_code"]
            or manifest["input_artifacts"] != declared["input_artifacts"]):
        raise NcenError("diagnostic_declared_pin_mismatch")

    names = {item.name for item in root.iterdir()}
    if "selection_inventory.json" not in names:
        raise NcenError("diagnostic_selection_inventory_missing")
    if "selections.jsonl" not in names:
        raise NcenError("diagnostic_selection_records_missing")
    if names != set(_purpose_expected_root_names()):
        raise NcenError("purpose_export_artifact_inventory_not_closed")
    for name in names:
        path = _purpose_safe_path(root, name, code="purpose_export")
        status = path.lstat()
        if (not stat_module.S_ISREG(status.st_mode)
                or getattr(status, "st_file_attributes", 0) & 0x400):
            raise NcenError("purpose_export_non_regular_file")

    # Q3 is issued here, not carried from the staging process. Its F1 and
    # membership objects must be the same freshly loaded handles used by C1b2a.
    baseline = read_diagnostic_baseline_checkpoint(
        trusted_run, code_root=Path(code_root), input_roots=input_roots, monitor=monitor,
    )
    issuance = _DIAGNOSTIC_BASELINE_REGISTRY[id(baseline)]
    source = issuance.source_index_ref
    membership = issuance.membership_ref
    admitted = _diagnostic_bound_baseline_admission(
        baseline, trusted_run=trusted_run, source_index=source,
        membership_checkpoint=membership,
    )
    roles = admitted["roles"]
    code_files = admitted["runtime_code"]["files"]
    excluded = {"runtime_code_manifest", "baseline_v3", "baseline_stage1", "baseline_stage2a",
                "baseline_stage2b"}
    artifacts = sorted(({
        key: pin[key] for key in ("root_id", "path", "sha256", "bytes")
    } for name, pins in roles.items() if name not in excluded for pin in pins),
        key=lambda pin: (pin["root_id"], pin["path"]))
    custody = _diagnostic_bound_source_custody(source)
    checkpoint = admitted["checkpoint"]
    if (declared["trusted_run_manifest_sha256"] != trusted_run.manifest_sha256
            or declared["lane"] != admitted["lane"]
            or declared["pin_roles"] != [
                {"role": name, "pins": pins} for name, pins in sorted(roles.items())]
            or declared["source_code"] != [
                {"path": item["path"], "sha256": item["sha256"]} for item in code_files]
            or declared["baseline_seals"] != [
                {"name": name, "sha256": roles[name][0]["sha256"]}
                for name in sorted(excluded - {"runtime_code_manifest"})]
            or declared["input_artifacts"] != artifacts
            or declared["ncen_source_manifest"] != {
                "sha256": custody.manifest_sha256, "bytes": custody.manifest_size,
                "kind": custody.manifest_kind, "evidence_digest": custody.evidence_digest}
            or declared["quarantine_seal"] != {
                "scope_sha256": roles["acquisition_scope"][0]["sha256"],
                "sha256sums_sha256": roles["acquisition_sha256sums"][0]["sha256"]}
            or any(declared[name] != checkpoint[name] for name in (
                "contexts", "inventory_digest", "cohort_digest", "ncen_evidence_digest",
                "exclusion_ledger_digest"))
            or declared["baseline_digest"] != baseline.digest):
        raise NcenError("diagnostic_declared_pin_mismatch")
    if len(source.rows) > declared["limits"]["max_source_rows"]:
        raise NcenError("purpose_export_source_row_limit_exceeded")
    expected = replay_purpose_selections(
        baseline, trusted_run, source_index=source, membership_checkpoint=membership,
        monitor=monitor,
    )
    if declared["selection_expected_digest"] != expected.digest:
        raise NcenError("diagnostic_selection_replay_mismatch")
    if checks["validation_code_sha256"] != _purpose_sha256(Path(__file__)):
        raise NcenError("diagnostic_runtime_code_mismatch")

    file_pins = {item["path"]: item for item in manifest["files"]}
    for name, pin in file_pins.items():
        path = root / name
        if path.stat().st_size != pin["bytes"] or _purpose_sha256(path) != pin["sha256"]:
            raise NcenError("manifest_staged_file_mismatch")
        monitor.check("purpose_readback_file")
    sums_path = root / "SHA256SUMS"
    verified_hashes = {name: pin["sha256"] for name, pin in file_pins.items()}
    verified_hashes.update({"declaration.json": declaration_sha, "manifest.json": manifest_sha})
    sums = "".join(f"{verified_hashes[name]}  {name}\n"
                   for name in sorted(verified_hashes)).encode("ascii")
    sums_bytes = _purpose_preliminary_bytes(sums_path, code="sha256sums")
    if sums_bytes != sums:
        raise NcenError("purpose_export_sha256sums_invalid")
    sums_sha = hashlib.sha256(sums_bytes).hexdigest()

    inventory_path = root / "selection_inventory.json"
    try:
        inventory = _purpose_read_staged_json(
            inventory_path, pin=file_pins[inventory_path.name],
            code="diagnostic_selection_records_invalid")
    except NcenError as exc:
        if str(exc) == "manifest_staged_file_mismatch":
            raise
        raise NcenError("diagnostic_selection_records_invalid") from exc
    try:
        selection_rows = _purpose_read_staged_jsonl(
            root / "selections.jsonl", record_type="selection_context",
            pin=file_pins["selections.jsonl"], monitor=monitor,
        )
    except NcenError as exc:
        if str(exc) == "manifest_staged_file_mismatch":
            raise
        raise NcenError("diagnostic_selection_records_invalid") from exc
    if len(selection_rows) != file_pins["selections.jsonl"]["rows"]:
        raise NcenError("diagnostic_selection_records_invalid")
    try:
        emitted = _purpose_reconstruct_emitted_replay(inventory, selection_rows)
    except NcenError as exc:
        if str(exc) in {"diagnostic_selection_records_invalid", "diagnostic_context_coverage_mismatch"}:
            raise
        raise NcenError("diagnostic_selection_records_invalid") from exc
    expected_keys = [(item["R"], item["K"], item["mode"]) for item in expected.records["contexts"]]
    emitted_keys = [(item["R"], item["K"], item["mode"]) for item in emitted["contexts"]]
    expected_members = [(item.report_date.isoformat(), item.cik) for item in membership.members]
    emitted_members = [(item["R"], selected["cik"]) for item in emitted["contexts"]
                       for selected in item["selections"]]
    if emitted_keys != expected_keys or emitted_members != expected_members:
        raise NcenError("diagnostic_context_coverage_mismatch")
    compare_purpose_selection_records(expected, emitted)
    monitor.check("purpose_readback_selection_replay")

    records = {}
    for name, record_type in DIAGNOSTIC_EXPORT_JSONL:
        if name == "selections.jsonl":
            continue
        rows = _purpose_read_staged_jsonl(
            root / name, record_type=record_type, pin=file_pins[name], monitor=monitor)
        if len(rows) != file_pins[name]["rows"]:
            raise NcenError("manifest_jsonl_row_count_mismatch")
        records[name] = rows
    replay = emitted["contexts"]
    flattened = {
        "contexts.jsonl": [item["context_record"] for item in replay],
        "nodes.jsonl": [row for item in replay for row in item["nodes"]],
        "reported_families.jsonl": [row for item in replay for row in item["reported_families"]],
        "incidences.jsonl": [row for item in replay for row in item["incidences"]],
        "sources.jsonl": emitted["sources"],
        "exclusions.jsonl": [*emitted["global_exclusions"],
                             *(row for item in replay for row in item["exclusions"])],
    }
    context_index = {item["context_id"]: index for index, item in enumerate(replay)}
    ablation_index = {spec.ablation_id: index for index, spec in enumerate(DIAGNOSTIC_ABLATIONS)}
    report_index = {item["R"]: index for index, item in enumerate(replay)}
    for name, rows in flattened.items():
        if records[name] != _purpose_sort_records(
            name, rows, context_index=context_index, ablation_index=ablation_index,
            report_index=report_index,
        ):
            raise NcenError("diagnostic_selection_cross_file_mismatch")
    cohort = _purpose_reconstruct_cohort(declared, records["cohort.jsonl"])
    if cohort.members != membership.members:
        raise NcenError("diagnostic_cohort_baseline_mismatch")
    ordered = _purpose_canonical_records(declared, records)
    if any(records[name] != ordered[name] for name in records):
        raise NcenError("purpose_export_record_order_invalid")
    issuer_path = root / "issuer_group_ref.json"
    issuer_ref = _purpose_read_staged_json(
        issuer_path, pin=file_pins[issuer_path.name], code="issuer_group_ref")
    if issuer_ref != _purpose_issuer_group_ref():
        raise NcenError("issuer_group_ref_invalid")
    performance_path = root / "performance.json"
    performance = _purpose_validate_performance(
        _purpose_read_staged_json(
            performance_path, pin=file_pins[performance_path.name], code="performance"))
    for name in ("selection_inventory.json", "issuer_group_ref.json", "performance.json", "checks.json"):
        if file_pins[name]["rows"] != 1:
            raise NcenError("manifest_json_row_count_mismatch")
    if manifest["issuer_group_ref_sha256"] != file_pins[issuer_path.name]["sha256"]:
        raise NcenError("manifest_issuer_ref_mismatch")
    _purpose_validate_semantics(declared, records)
    verified = {row["accession"] for row in records["sources.jsonl"]
                if row["custody_state"] == "verified"}
    quarantined = {row["accession"] for row in records["exclusions.jsonl"]
                   if row["state"] == "excluded_quarantine"}
    if manifest["coverage"] != {
        "requested_contexts": len(declared["contexts"]),
        "completed_contexts": len(records["contexts.jsonl"]), "missing_contexts": 0,
        "requested_accessions": len(verified | quarantined),
        "verified_accessions": len(verified), "quarantined_accessions": len(quarantined),
    }:
        raise NcenError("manifest_coverage_reconciliation_mismatch")
    if (performance["contexts_completed"] != len(records["contexts.jsonl"])
            or performance["source_rows_read"] != len(source.rows)
            or performance["incidences_emitted"] != len(records["incidences.jsonl"])):
        raise NcenError("performance_count_reconciliation_mismatch")
    monitor.check("purpose_readback_complete")
    return SyntheticReadbackEvidence(
        declaration_sha256=declaration_sha,
        manifest_sha256=manifest_sha,
        sha256sums_sha256=sums_sha,
        baseline_digest=baseline.digest, selection_replay_digest=expected.digest,
        context_count=len(records["contexts.jsonl"]), source_count=len(source.rows),
        incidence_count=len(records["incidences.jsonl"]),
    )


def _purpose_validate_predecessor(
    predecessor_receipt: Path | None,
    expected_sha256: str | None,
    *,
    code_root: Path,
    input_roots: Mapping[str, Path],
) -> None:
    if expected_sha256 is None:
        if predecessor_receipt is not None:
            raise NcenError("purpose_export_unexpected_predecessor")
        return
    if predecessor_receipt is None or not predecessor_receipt.is_file() or predecessor_receipt.is_symlink():
        raise NcenError("purpose_export_predecessor_receipt_missing")
    if _purpose_sha256(predecessor_receipt) != expected_sha256:
        raise NcenError("purpose_export_predecessor_receipt_mismatch")
    receipt = _purpose_receipt(
        _purpose_json_load(predecessor_receipt.read_bytes(), code="predecessor_receipt")
    )
    if receipt["status"] != "complete":
        raise NcenError("purpose_export_predecessor_not_verified")
    predecessor_root = predecessor_receipt.parent / receipt["artifact_directory"]
    _purpose_validate_export_root(
        predecessor_root,
        receipt_path=predecessor_receipt,
        code_root=code_root,
        input_roots=input_roots,
        require_receipt=True,
    )


def write_purpose_diagnostics(
    *,
    trusted_run: DiagnosticTrustPin,
    declaration: Mapping[str, Any],
    output_root: str | Path,
    code_root: str | Path,
    input_roots: Mapping[str, str | Path],
    receipt_path: str | Path | None = None,
) -> PurposeExportResult:
    """Refuse public issuance before private readback and C2 resource verification."""
    if not isinstance(declaration, Mapping) or declaration.get("schema_version") != DIAGNOSTIC_DECLARATION_VERSION:
        raise NcenError("diagnostic_export_schema_unsupported")
    if declaration.get("predecessor_receipt_sha256") is not None:
        raise NcenError("diagnostic_resume_not_supported")
    output = Path(output_root)
    if output.exists() or output.is_symlink():
        raise NcenError("purpose_export_directory_must_be_new")
    if not output.parent.is_dir():
        raise NcenError("purpose_export_parent_missing")
    if receipt_path is not None and (Path(receipt_path).exists() or Path(receipt_path).is_symlink()):
        raise NcenError("purpose_export_receipt_must_be_new")
    _purpose_preliminary_trust(trusted_run, root=output, declaration=declaration)
    raise NcenError("C1_INCOMPLETE")
    # The old issuance body below is unreachable until b2/b3 replace it with
    # pinned private staging; it has no public candidate-bearing parameters.
    index: NcenFilingIndex = None  # type: ignore[assignment]
    cohort: DiagnosticCohort = None  # type: ignore[assignment]
    sources: DiagnosticSourceIndex = None  # type: ignore[assignment]
    predecessor_receipt: str | Path | None = None
    predecessor_input_roots: Mapping[str, str | Path] | None = None
    import contextlib
    import os
    import platform
    import time
    import unicodedata

    declared = _purpose_declaration(declaration)
    output = Path(output_root)
    external_receipt = (
        Path(receipt_path)
        if receipt_path is not None
        else output.parent / f"{output.name}.receipt.json"
    )
    if output.exists() or output.is_symlink():
        raise NcenError("purpose_export_directory_must_be_new")
    if external_receipt.exists() or external_receipt.is_symlink():
        raise NcenError("purpose_export_receipt_must_be_new")
    if external_receipt.parent.resolve() == output.resolve():
        raise NcenError("purpose_export_receipt_must_be_external")
    if not output.parent.is_dir() or not external_receipt.parent.is_dir():
        raise NcenError("purpose_export_parent_missing")
    resolved_code_root = Path(code_root)
    resolved_input_roots = {key: Path(value) for key, value in input_roots.items()}
    _purpose_validate_predecessor(
        Path(predecessor_receipt) if predecessor_receipt is not None else None,
        declared["predecessor_receipt_sha256"],
        code_root=resolved_code_root,
        input_roots={
            key: Path(value)
            for key, value in (
                predecessor_input_roots if predecessor_input_roots is not None else input_roots
            ).items()
        },
    )
    _purpose_verify_declared_inputs(
        declared,
        code_root=resolved_code_root,
        input_roots=resolved_input_roots,
    )
    _purpose_source_index_bound(sources)
    if len(sources.rows) > declared["limits"]["max_source_rows"]:
        raise NcenError("purpose_export_source_row_limit_exceeded")
    output.mkdir()
    declaration_path = output / "declaration.json"
    _purpose_write_partial(output / "declaration.json.partial", _purpose_json_bytes(declared))
    _purpose_finalize_partial(output / "declaration.json.partial")
    started_wall = dt.datetime.now(UTC)
    started_clock = time.perf_counter()
    peak_rss = _purpose_process_rss()
    source_records, source_ids = _purpose_source_records(sources)
    cohort_records = tuple(
        sorted(
            (_purpose_cohort_record(member, cohort.inventory_digest) for member in cohort.members),
            key=lambda item: item["record_id"],
        )
    )
    quarantine_records = _purpose_exclusion_records((), sources, source_ids)
    row_counts = {path: 0 for path, _record_type in DIAGNOSTIC_EXPORT_JSONL}
    counters = {"incidences_emitted": 0, "union_attempts": 0, "successful_unions": 0}
    contexts_completed = 0
    previous_by_ablation: dict[str, DependenceSnapshot] = {}
    fold_accumulators = {
        spec.ablation_id: _PurposeFoldAccumulator(spec) for spec in DIAGNOSTIC_ABLATIONS
    }

    with contextlib.ExitStack() as stack:
        handles = {
            path: stack.enter_context((output / f"{path}.partial").open("xb"))
            for path, _record_type in DIAGNOSTIC_EXPORT_JSONL
        }

        def emit(path: str, rows: Iterable[Mapping[str, Any]]) -> None:
            for row in rows:
                handles[path].write(_diagnostic_canonical(dict(row)) + b"\n")
                row_counts[path] += 1

        emit("sources.jsonl", source_records)
        emit("cohort.jsonl", cohort_records)
        emit("exclusions.jsonl", quarantine_records)
        for snapshot in iter_purpose_snapshots(
            index,
            cohort,
            sources,
            declaration=declared,
        ):
            context_records, context_counters = _purpose_context_records(
                snapshot,
                cohort=cohort,
                sources=sources,
                declaration=declared,
                source_ids=source_ids,
            )
            for path, rows in context_records.items():
                if path not in {
                    "sources.jsonl",
                    "cohort.jsonl",
                    "transitions.jsonl",
                    "fold_groups.jsonl",
                    "fold_memberships.jsonl",
                }:
                    emit(path, rows)
            transitions: list[dict[str, Any]] = []
            for dependence_snapshot in snapshot.dependence_snapshots():
                ablation_id = dependence_snapshot.projection.ablation_id
                previous = previous_by_ablation.get(ablation_id)
                if previous is not None:
                    transitions.extend(
                        _purpose_transition_record(item)
                        for item in build_dependence_transitions(
                            (previous, dependence_snapshot)
                        )
                    )
                previous_by_ablation[ablation_id] = dependence_snapshot
                fold_accumulators[ablation_id].add(dependence_snapshot)
            transitions.sort(
                key=lambda item: (
                    next(
                        index
                        for index, spec in enumerate(DIAGNOSTIC_ABLATIONS)
                        if spec.ablation_id == item["ablation_id"]
                    ),
                    item["record_id"],
                )
            )
            emit("transitions.jsonl", transitions)
            for key in counters:
                counters[key] += context_counters[key]
            contexts_completed += 1
            peak_rss = max(peak_rss, _purpose_process_rss())
            if peak_rss >= DIAGNOSTIC_MEMORY_SOFT_LIMIT_BYTES:
                raise NcenError("purpose_export_memory_soft_limit_exceeded")
        if contexts_completed != len(declared["contexts"]):
            raise NcenError("purpose_export_context_count_mismatch")
        fold_groups: list[dict[str, Any]] = []
        fold_memberships: list[dict[str, Any]] = []
        for spec in DIAGNOSTIC_ABLATIONS:
            groups, memberships = fold_accumulators[spec.ablation_id].records()
            fold_groups.extend(groups)
            fold_memberships.extend(memberships)
        emit("fold_groups.jsonl", sorted(fold_groups, key=lambda item: item["record_id"]))
        emit(
            "fold_memberships.jsonl",
            sorted(fold_memberships, key=lambda item: item["record_id"]),
        )
        for handle in handles.values():
            handle.flush()
            os.fsync(handle.fileno())
    records = {
        path_name: _purpose_read_jsonl(
            output / f"{path_name}.partial", record_type=record_type
        )
        for path_name, record_type in DIAGNOSTIC_EXPORT_JSONL
    }
    canonical_records = _purpose_canonical_records(declared, records)
    if any(records[path] != canonical_records[path] for path in records):
        raise NcenError("purpose_export_partial_record_order_invalid")
    _purpose_validate_semantics(declared, records)
    for path_name, _record_type in DIAGNOSTIC_EXPORT_JSONL:
        _purpose_finalize_partial(output / f"{path_name}.partial")
    issuer_ref = _purpose_issuer_group_ref()
    _purpose_write_partial(
        output / "issuer_group_ref.json.partial",
        _purpose_json_bytes(issuer_ref),
    )
    _purpose_finalize_partial(output / "issuer_group_ref.json.partial")
    finished_wall = dt.datetime.now(UTC)
    elapsed = max(0.0, time.perf_counter() - started_clock)
    peak_rss = max(peak_rss, _purpose_process_rss())
    performance = {
        "schema_version": DIAGNOSTIC_PERFORMANCE_VERSION,
        "started_at": _diagnostic_timestamp(started_wall),
        "finished_at": _diagnostic_timestamp(finished_wall),
        "elapsed_seconds": elapsed,
        "peak_rss_bytes": peak_rss,
        "memory_limit_bytes": DIAGNOSTIC_MEMORY_LIMIT_BYTES,
        "contexts_completed": contexts_completed,
        "source_rows_read": len(sources.rows),
        "incidences_emitted": counters["incidences_emitted"],
        "union_attempts": counters["union_attempts"],
        "successful_unions": counters["successful_unions"],
        "raw_nport_parse_calls": 0,
        "inventory_build_calls": 0,
        "target_vote_calls": 0,
        "stop_reason": None,
    }
    _purpose_validate_performance(performance)
    _purpose_write_partial(
        output / "performance.json.partial",
        _purpose_json_bytes(performance),
    )
    _purpose_finalize_partial(output / "performance.json.partial")
    validation_code_sha256 = _purpose_sha256(Path(__file__))
    checks = {
        "schema_version": DIAGNOSTIC_CHECKS_VERSION,
        "checks": [
            {
                "name": "canonical_schema",
                "status": "passed",
                "details": "all records decoded canonically with exact fields",
            },
            {
                "name": "closure_replay",
                "status": "passed",
                "details": "partition, forest, degree, summary and temporal closure reconciled",
            },
            {
                "name": "outcome_allowlist",
                "status": "passed",
                "details": "only declared outcome-free diagnostic inputs were used",
            },
            {
                "name": "source_bindings",
                "status": "passed",
                "details": "source files, rows and acceptance headers remained bound",
            },
        ],
        "validation_code_sha256": validation_code_sha256,
    }
    checks["checks"].sort(key=lambda item: item["name"])
    _purpose_validate_checks(checks)
    _purpose_write_partial(output / "checks.json.partial", _purpose_json_bytes(checks))
    _purpose_finalize_partial(output / "checks.json.partial")
    data_paths = sorted(
        {
            *(path for path, _record_type in DIAGNOSTIC_EXPORT_JSONL),
            "issuer_group_ref.json",
            "performance.json",
            "checks.json",
        }
    )
    record_types = dict(DIAGNOSTIC_EXPORT_JSONL)
    record_types.update(
        {
            "issuer_group_ref.json": "issuer_group_ref",
            "performance.json": "performance",
            "checks.json": "checks",
        }
    )
    row_counts.update(
        {"issuer_group_ref.json": 1, "performance.json": 1, "checks.json": 1}
    )
    verified_accessions = {
        row.accession_number for row in sources.rows if row.custody_state == "verified"
    }
    quarantined_accessions = {item.accession_number for item in sources.exclusions}
    manifest = {
        "schema_version": DIAGNOSTIC_MANIFEST_VERSION,
        "declaration_sha256": _purpose_sha256(declaration_path),
        "baseline_seals": declared["baseline_seals"],
        "source_code": declared["source_code"],
        "runtime": {
            "python_version": platform.python_version(),
            "unicode_version": unicodedata.unidata_version,
            "platform": platform.platform(),
        },
        "input_artifacts": declared["input_artifacts"],
        "files": [
            {
                "path": path,
                "sha256": _purpose_sha256(output / path),
                "bytes": (output / path).stat().st_size,
                "rows": row_counts[path],
                "record_type": record_types[path],
            }
            for path in data_paths
        ],
        "coverage": {
            "requested_contexts": len(declared["contexts"]),
            "completed_contexts": contexts_completed,
            "missing_contexts": 0,
            "requested_accessions": len(verified_accessions | quarantined_accessions),
            "verified_accessions": len(verified_accessions),
            "quarantined_accessions": len(quarantined_accessions),
        },
        "status": "complete",
        "reasons": [],
        "issuer_group_ref_sha256": _purpose_sha256(output / "issuer_group_ref.json"),
        "outcome_inputs_used": False,
        "diagnostic_only": True,
        "qualification": "NOT_EVALUABLE",
    }
    _purpose_manifest(manifest)
    _purpose_write_partial(output / "manifest.json.partial", _purpose_json_bytes(manifest))
    _purpose_finalize_partial(output / "manifest.json.partial")
    sums_paths = sorted({*data_paths, "declaration.json", "manifest.json"})
    sums = "".join(
        f"{_purpose_sha256(output / path)}  {path}\n" for path in sums_paths
    ).encode("ascii")
    _purpose_write_partial(output / "SHA256SUMS.partial", sums)
    _purpose_finalize_partial(output / "SHA256SUMS.partial")
    _purpose_validate_export_root(
        output,
        receipt_path=None,
        code_root=resolved_code_root,
        input_roots=resolved_input_roots,
        require_receipt=False,
    )
    receipt = {
        "schema_version": DIAGNOSTIC_RECEIPT_VERSION,
        "artifact_directory": output.name,
        "sha256sums_sha256": _purpose_sha256(output / "SHA256SUMS"),
        "manifest_sha256": _purpose_sha256(output / "manifest.json"),
        "declaration_sha256": _purpose_sha256(declaration_path),
        "status": "complete",
        "predecessor_receipt_sha256": declared["predecessor_receipt_sha256"],
        "diagnostic_only": True,
        "qualification": "NOT_EVALUABLE",
    }
    _purpose_receipt(receipt)
    receipt_bytes = _purpose_json_bytes(receipt)
    receipt_sha256 = hashlib.sha256(receipt_bytes).hexdigest()
    receipt_partial = external_receipt.with_name(f"{external_receipt.name}.partial")
    _purpose_write_partial(receipt_partial, receipt_bytes)
    _purpose_validate_export_root(
        output,
        receipt_path=receipt_partial,
        code_root=resolved_code_root,
        input_roots=resolved_input_roots,
        require_receipt=True,
    )
    os.replace(receipt_partial, external_receipt)
    return PurposeExportResult(
        root=output,
        receipt_path=external_receipt,
        declaration_sha256=receipt["declaration_sha256"],
        manifest_sha256=receipt["manifest_sha256"],
        sha256sums_sha256=receipt["sha256sums_sha256"],
        receipt_sha256=receipt_sha256,
    )


# === Timezone-awareness guards (post-freeze) =================================================
# The v3 core above and the FE-1 test suite are byte-pinned (``test_frozen_v3_and_prep1_unchanged``), so
# the naive-timestamp refusal is applied here, by rebinding the public entry points, instead of inside the
# frozen text. Module-level callers resolve these names at call time, so module-internal calls to a
# rebound *function* are guarded too; the ``NcenFiling`` methods are patched on the class and therefore
# guard every caller. Naive values are refused with ``NcenError("timestamp_not_timezone_aware:<name>")``
# (the N-PORT parser's code) BEFORE the wrapped body converts them with ``astimezone`` (which would read a
# naive value as local time).
#
# Exemptions (documented, deliberate): ``diagnostic_selection`` already refuses a naive cutoff itself
# (``datetime_not_timezone_aware``, before any conversion); ``diagnostic_fixture_selection`` and
# ``_diagnostic_build_context`` accept a naive ``knowledge_cutoff`` on purpose and reject it later with
# diagnostic-specific errors. They keep those codes and are not wrapped.
#
# FE-1 trust root: ``build_vote_inventory`` / ``VoteInventory.target_votes`` read ``DeraPackageResult``
# artifacts (Parquet + JSONL) WITHOUT an external sidecar pin. FE-1 inputs are pinned only by their sealed
# source manifests and the pinned package ZIP sha256 (``parse_dera_package`` re-verifies the ZIP; the
# derived artifacts are trusted as the sealed run's output). The externally pinned
# ``NportSourceInput.expected_result_sha256`` gate applies to ``nport.materialize_nport_inventory`` only.
def _aware(value: object, name: str) -> dt.datetime:
    """UTC-normalized ``value``; a naive input raises ``timestamp_not_timezone_aware:<name>``
    (same code as the N-PORT parser)."""
    try:
        return nport._require_aware_utc(value, name)
    except nport.NportError as exc:
        raise NcenError(str(exc)) from exc


def _guard_aware(function: Any, *names: str) -> Any:
    import functools
    import inspect

    parameters = list(inspect.signature(function).parameters.values())
    positional = {
        p.name: index for index, p in enumerate(parameters)
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    }
    missing = [name for name in names if name not in {p.name for p in parameters}]
    if missing:
        raise NcenError(f"guard_parameter_unknown:{function.__name__}:{missing}")
    where = tuple((name, positional.get(name)) for name in names)

    @functools.wraps(function)
    def guarded(*args: Any, **kwargs: Any) -> Any:
        for name, index in where:
            if name in kwargs:
                value = kwargs[name]
            elif index is not None and index < len(args):
                value = args[index]
            else:
                continue  # defaulted: nothing to check
            # Fast path: an aware datetime needs no further work; anything else (naive, non-datetime) is
            # checked (``None`` means "not supplied" for the optional parameters).
            if value is not None and not (
                isinstance(value, dt.datetime) and value.tzinfo is not None
                and value.tzinfo.utcoffset(value) is not None
            ):
                _aware(value, name)
        return function(*args, **kwargs)

    return guarded


for _name, _params in (
    ("parse_dera_ncen_package", ("retrieved_at", "first_verified_public_at")),
    ("parse_ncen_primary_doc", ("retrieved_at",)),
    ("merge_filings", ("index_retrieved_at",)),
    ("effective_filing", ("knowledge_cutoff",)),
    ("family_components", ("knowledge_cutoff",)),
    ("build_vote_inventory", ("knowledge_cutoff",)),
    ("family_evidence_for", ("knowledge_cutoff",)),
    ("build_consensus_with_ncen", ("knowledge_cutoff",)),
    ("diagnostic_per_state_components", ("knowledge_cutoff",)),
    ("_diagnostic_dera_projections", ("retrieved_at", "first_verified_public_at")),
):
    globals()[_name] = _guard_aware(globals()[_name], *_params)
for _method in ("visible", "data_available", "exact_acceptance", "admission_bound", "earliest_public"):
    setattr(NcenFiling, _method, _guard_aware(getattr(NcenFiling, _method), "cutoff"))
del _name, _params, _method
