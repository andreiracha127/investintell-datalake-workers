"""Diagnostic (coverage-only) release layer over a validated partial/limited bond-credit build.

This module mirrors ``schemas/bond_default_diagnostic_release_v1.sql``. The qualified
``bond_credit_current_pointer`` is never touched: a *diagnostic release* binds one validated,
``partial`` / ``limited`` publication to an allowlisted display projection
(``bond_default_events_display_v1``) and is elected through its own compare-and-set pointer.

Contract for callers (the worker, the source-bundle builder):

* ``DiagnosticProjection`` is a frozen dataclass; it is the *stored* part of the display response
  (everything §9.4 lists except the identity/timestamp fields the database adds on read).
* :func:`derive_projection` derives the projection from the persisted coverage rows exactly as
  ``bond_default_diag_derive`` does in SQL; ``prepare_diagnostic`` refuses any projection that is not
  identical to the SQL derivation, so a caller can neither add fields nor alter counts.
* Coverage ``rationale`` text carries the source-frontier provenance; build it with
  :func:`frontier_record` and :func:`coverage_rationale` (never hand-roll the JSON).
* DML entry points run in the caller's transaction: this module never commits, never installs
  implicitly and never opens a connection. ``install_diagnostic_schema`` is explicit.
"""

from __future__ import annotations

import datetime as dt
import itertools
import re
import uuid
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

from . import contracts as c
from .contracts import CreditBundle

DISPLAY_VERSION: Final = "bond_default_events_display_v1"
PRODUCT: Final = "bond_default_events_diagnostic_v1"
TIER: Final = "experimental_partial"
DISPLAY_MODE: Final = "coverage_only"
RATIONALE_FORMAT: Final = "bond_default_coverage_rationale_v1"
FRONTIER_MANIFEST_VERSION: Final = "bond_default_frontier_manifest_v1"
RELEASE_NAMESPACE: Final = (
    "https://investintell.local/contracts/bonds/bond_default_events_diagnostic_v1"
)
ERROR_PREFIX: Final = "bond_default_diagnostic:"
SQL_PATH = c.ROOT / "schemas" / "bond_default_diagnostic_release_v1.sql"
#: LF-normalized SHA-256 of the diagnostic SQL file (independent of the four-file ``c.sql_digest``).
DIAGNOSTIC_SQL_DIGEST: Final = (
    "sha256:3b97413930bd1497f504ca4041ad8be8c8f9a1db745c2f4751d398550d7a1480"
)

SOURCES: Final = ("agency_rocr", "sec_edgar", "sec_ncen", "sec_nport")
#: Allowed (source, inventory_kind) pairs of a source frontier.
FRONTIER_KINDS: Final = {
    "agency_rocr": ("agency_history",),
    "sec_edgar": ("census", "submissions"),
    "sec_ncen": ("dera_packages", "form_index"),
    "sec_nport": ("dera_packages",),
}
FRONTIER_STATES: Final = ("inventory_only", "unavailable")
#: (source, event_type) of the six coverage cells of every outcome month.
COVERAGE_CELLS: Final = (
    ("all", "all"),
    ("sec_nport", "default_state"),
    ("sec_edgar", "bankruptcy"),
    ("sec_edgar", "payment_default"),
    ("sec_edgar", "distressed_exchange"),
    ("agency_rocr", "agency_issue_default"),
)
EVENT_TYPES: Final = (
    "agency_issue_default",
    "bankruptcy",
    "default_state",
    "distressed_exchange",
    "payment_default",
)
TIMING_CLASSES: Final = ("incident", "interval_uncertain", "prevalent")
LIMITATION_CODES: Final = (
    "censoring_not_evaluated",
    "coverage_only",
    "inventory_not_ingested",
    "no_adjudicated_events",
    "not_for_expected_loss",
    "not_for_recommendation",
    "nport_consensus_c1_blocked",
    "nport_q3_unavailable_at_observation",
    "outcomes_unascertained",
    "rating_rights_unverified",
    "rating_source_unavailable",
    "reference_unreviewed",
    "unresolved_not_evaluated",
)
#: Limitations every v1 display carries; ``nport_q3_unavailable_at_observation`` is added only by a record.
BASE_LIMITATIONS: Final = tuple(
    code for code in LIMITATION_CODES if code != "nport_q3_unavailable_at_observation"
)
COUNTS_REASON_CODES: Final = ("censoring_not_evaluated", "unresolved_not_evaluated")

_TS = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z", re.ASCII
)
_DATE_TEXT = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", re.ASCII)
_PERIOD = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])", re.ASCII)
_BASIS = re.compile(r"[A-Za-z0-9 _.,:;()+=-]{1,200}")
_CODE = re.compile(r"[a-z0-9_]{1,64}")
_SOURCE_KEY = re.compile(r"[A-Za-z0-9_.:-]{1,128}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_REASON = re.compile(r"[a-z][a-z0-9_]{2,63}")
_INT_MAX = 2_147_483_647


class DiagnosticError(RuntimeError):
    """A diagnostic contract or database rule refused the operation (``reason`` is a bounded code)."""

    def __init__(
        self, reason: str, detail: str = "", sqlstate: str | None = None
    ) -> None:
        super().__init__(f"{reason}:{detail}" if detail else reason)
        self.reason = reason
        self.detail = detail
        self.sqlstate = sqlstate
        #: Sanitized per-check report attached by ``verify_installed_privileges``.
        self.report: dict[str, Any] | None = None


def _fail(reason: str, detail: str = "") -> DiagnosticError:
    return DiagnosticError(reason, detail)


# ---------------------------------------------------------------------------
# Canonical helpers
# ---------------------------------------------------------------------------
def _require_int(name: str, value: object, *, nullable: bool = False) -> int | None:
    if value is None and nullable:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= _INT_MAX
    ):
        raise _fail("projection_invalid", f"{name}:non_negative_int_expected")
    return value


def _require_ts(name: str, value: object) -> dt.datetime:
    if (
        not isinstance(value, dt.datetime)
        or value.tzinfo is None
        or value.utcoffset() != dt.timedelta(0)
    ):
        raise _fail("projection_invalid", f"{name}:utc_datetime_expected")
    return value.astimezone(dt.timezone.utc)


def _ts_text(value: dt.datetime) -> str:
    return c.ts_text(value)


def _parse_ts(name: str, text: object) -> dt.datetime:
    if not isinstance(text, str) or not _TS.fullmatch(text):
        raise _fail("projection_invalid", f"{name}:timestamp_text_expected")
    try:
        parsed = dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=dt.timezone.utc
        )
    except ValueError as exc:
        raise _fail("projection_invalid", f"{name}:timestamp_text_expected") from exc
    return parsed


def _parse_date(name: str, text: object, *, nullable: bool = False) -> dt.date | None:
    if text is None and nullable:
        return None
    if not isinstance(text, str) or not _DATE_TEXT.fullmatch(text):
        raise _fail("projection_invalid", f"{name}:date_text_expected")
    try:
        return dt.date.fromisoformat(text)
    except ValueError as exc:
        raise _fail("projection_invalid", f"{name}:date_text_expected") from exc


def _codes(name: str, values: Iterable[str]) -> tuple[str, ...]:
    """Known limitation codes, sorted and deduplicated (the canonical stored form)."""
    out = sorted(set(values))
    for code in out:
        if code not in LIMITATION_CODES:
            raise _fail("projection_invalid", f"{name}:unknown_code:{code}")
    return tuple(out)


def _sorted_codes_list(name: str, values: object) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)) or not all(
        isinstance(v, str) for v in values
    ):
        raise _fail("projection_invalid", f"{name}:list_of_codes_expected")
    codes = _codes(name, values)
    if list(codes) != list(values):
        raise _fail("projection_invalid", f"{name}:codes_must_be_sorted_unique")
    return codes


def _exact_keys(name: str, obj: object, keys: Iterable[str]) -> Mapping[str, Any]:
    expected = sorted(keys)
    if not isinstance(obj, Mapping) or sorted(obj) != expected:
        raise _fail("projection_invalid", f"{name}:keys")
    return obj


# ---------------------------------------------------------------------------
# Public projection types (field order = §9.4)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SourceFrontier:
    source: str
    inventory_kind: str
    frontier: dt.date | None
    observed_at: dt.datetime
    state: str
    filing_count: int | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.source not in SOURCES or self.inventory_kind not in FRONTIER_KINDS.get(
            self.source, ()
        ):
            raise _fail(
                "projection_invalid",
                f"source_frontier:source_kind:{self.source}/{self.inventory_kind}",
            )
        if self.state not in FRONTIER_STATES:
            raise _fail("projection_invalid", "source_frontier:state")
        if self.frontier is not None and not isinstance(self.frontier, dt.date):
            raise _fail("projection_invalid", "source_frontier:frontier")
        _require_int("filing_count", self.filing_count, nullable=True)
        object.__setattr__(
            self, "observed_at", _require_ts("observed_at", self.observed_at)
        )
        object.__setattr__(
            self,
            "reason_codes",
            _codes("source_frontier.reason_codes", self.reason_codes),
        )

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "inventory_kind": self.inventory_kind,
            "frontier": None if self.frontier is None else self.frontier.isoformat(),
            "observed_at": _ts_text(self.observed_at),
            "state": self.state,
            "filing_count": self.filing_count,
            "reason_codes": list(self.reason_codes),
        }

    @classmethod
    def from_json_obj(cls, obj: object) -> SourceFrontier:
        m = _exact_keys(
            "source_frontier",
            obj,
            (
                "source",
                "inventory_kind",
                "frontier",
                "observed_at",
                "state",
                "filing_count",
                "reason_codes",
            ),
        )
        return cls(
            source=m["source"],
            inventory_kind=m["inventory_kind"],
            frontier=_parse_date("frontier", m["frontier"], nullable=True),
            observed_at=_parse_ts("observed_at", m["observed_at"]),
            state=m["state"],
            filing_count=_require_int("filing_count", m["filing_count"], nullable=True),
            reason_codes=_sorted_codes_list(
                "source_frontier.reason_codes", m["reason_codes"]
            ),
        )


@dataclass(frozen=True)
class CoverageCellDisplay:
    period_label: str
    source: str
    event_type: str
    rating_stratum: str
    exposure_cohort: str
    state: str
    denominator_basis: str
    denominator_count: int
    exposed_issue_months: int
    event_count: int
    unlinked_count: int
    date_uncertain_count: int
    unknown_outcome_issue_months: int
    source_frontier: dt.date | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _PERIOD.fullmatch(self.period_label or ""):
            raise _fail("projection_invalid", "coverage:period_label")
        if (self.source, self.event_type) not in COVERAGE_CELLS:
            raise _fail(
                "projection_invalid",
                f"coverage:source_event:{self.source}/{self.event_type}",
            )
        if (
            self.rating_stratum,
            self.exposure_cohort,
            self.state,
            self.denominator_basis,
        ) != ("unknown", "all", "unavailable", "panel_exposure"):
            raise _fail("projection_invalid", "coverage:v1_constants")
        for name in (
            "denominator_count",
            "exposed_issue_months",
            "event_count",
            "unlinked_count",
            "date_uncertain_count",
            "unknown_outcome_issue_months",
        ):
            _require_int(name, getattr(self, name))
        if (self.event_count, self.unlinked_count, self.date_uncertain_count) != (
            0,
            0,
            0,
        ):
            raise _fail("projection_invalid", "coverage:events_not_zero")
        if not (
            self.denominator_count
            == self.exposed_issue_months
            == self.unknown_outcome_issue_months
        ):
            raise _fail(
                "projection_invalid", "coverage:denominator_exposure_unknown_mismatch"
            )
        if self.source_frontier is not None and not isinstance(
            self.source_frontier, dt.date
        ):
            raise _fail("projection_invalid", "coverage:source_frontier")
        object.__setattr__(
            self, "reason_codes", _codes("coverage.reason_codes", self.reason_codes)
        )

    def sort_key(self) -> tuple[str, str, str]:
        return (self.period_label, self.source, self.event_type)

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "period_label": self.period_label,
            "source": self.source,
            "event_type": self.event_type,
            "rating_stratum": self.rating_stratum,
            "exposure_cohort": self.exposure_cohort,
            "state": self.state,
            "denominator_basis": self.denominator_basis,
            "denominator_count": self.denominator_count,
            "exposed_issue_months": self.exposed_issue_months,
            "event_count": self.event_count,
            "unlinked_count": self.unlinked_count,
            "date_uncertain_count": self.date_uncertain_count,
            "unknown_outcome_issue_months": self.unknown_outcome_issue_months,
            "source_frontier": None
            if self.source_frontier is None
            else self.source_frontier.isoformat(),
            "reason_codes": list(self.reason_codes),
        }

    @classmethod
    def from_json_obj(cls, obj: object) -> CoverageCellDisplay:
        keys = (
            "period_label",
            "source",
            "event_type",
            "rating_stratum",
            "exposure_cohort",
            "state",
            "denominator_basis",
            "denominator_count",
            "exposed_issue_months",
            "event_count",
            "unlinked_count",
            "date_uncertain_count",
            "unknown_outcome_issue_months",
            "source_frontier",
            "reason_codes",
        )
        m = _exact_keys("coverage", obj, keys)
        return cls(
            period_label=m["period_label"],
            source=m["source"],
            event_type=m["event_type"],
            rating_stratum=m["rating_stratum"],
            exposure_cohort=m["exposure_cohort"],
            state=m["state"],
            denominator_basis=m["denominator_basis"],
            denominator_count=_require_int("denominator_count", m["denominator_count"]),  # type: ignore[arg-type]
            exposed_issue_months=_require_int(
                "exposed_issue_months", m["exposed_issue_months"]
            ),  # type: ignore[arg-type]
            event_count=_require_int("event_count", m["event_count"]),  # type: ignore[arg-type]
            unlinked_count=_require_int("unlinked_count", m["unlinked_count"]),  # type: ignore[arg-type]
            date_uncertain_count=_require_int(
                "date_uncertain_count", m["date_uncertain_count"]
            ),  # type: ignore[arg-type]
            unknown_outcome_issue_months=_require_int(  # type: ignore[arg-type]
                "unknown_outcome_issue_months", m["unknown_outcome_issue_months"]
            ),
            source_frontier=_parse_date(
                "source_frontier", m["source_frontier"], nullable=True
            ),
            reason_codes=_sorted_codes_list("coverage.reason_codes", m["reason_codes"]),
        )


@dataclass(frozen=True)
class AcceptedEventDisplay:
    """Future-compatible element shape; ``display_v1`` requires the collection to be empty."""

    cusip9: str
    event_type: str
    onset_date: dt.date | None
    onset_lower_exclusive: dt.date | None
    onset_upper_inclusive: dt.date
    timing_class: str

    def __post_init__(self) -> None:
        if (
            not c.is_valid_cusip9(self.cusip9)
            or self.event_type not in EVENT_TYPES
            or self.timing_class not in TIMING_CLASSES
        ):
            raise _fail("projection_invalid", "accepted_event:fields")

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "cusip9": self.cusip9,
            "event_type": self.event_type,
            "onset_date": None
            if self.onset_date is None
            else self.onset_date.isoformat(),
            "onset_lower_exclusive": None
            if self.onset_lower_exclusive is None
            else self.onset_lower_exclusive.isoformat(),
            "onset_upper_inclusive": self.onset_upper_inclusive.isoformat(),
            "timing_class": self.timing_class,
        }


@dataclass(frozen=True)
class DiagnosticCounts:
    panel_grid_keys: int
    candidate_issue_months: int
    unknown_outcome_issue_months: int
    accepted_events: int = 0
    unresolved_events: int | None = None
    censored_issue_months: int | None = None
    reason_codes: tuple[str, ...] = COUNTS_REASON_CODES

    def __post_init__(self) -> None:
        for name in (
            "panel_grid_keys",
            "candidate_issue_months",
            "unknown_outcome_issue_months",
        ):
            _require_int(name, getattr(self, name))
        if (
            type(self.accepted_events) is not int
            or self.accepted_events != 0
            or self.unresolved_events is not None
            or self.censored_issue_months is not None
        ):
            raise _fail("projection_invalid", "counts:v1_constants")
        if self.unknown_outcome_issue_months != self.candidate_issue_months:
            raise _fail(
                "projection_invalid", "counts:unknown_outcome_must_equal_candidate"
            )
        if tuple(self.reason_codes) != COUNTS_REASON_CODES:
            raise _fail("projection_invalid", "counts:reason_codes")
        object.__setattr__(self, "reason_codes", tuple(self.reason_codes))

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "panel_grid_keys": self.panel_grid_keys,
            "candidate_issue_months": self.candidate_issue_months,
            "accepted_events": self.accepted_events,
            "unknown_outcome_issue_months": self.unknown_outcome_issue_months,
            "unresolved_events": self.unresolved_events,
            "censored_issue_months": self.censored_issue_months,
            "reason_codes": list(self.reason_codes),
        }

    @classmethod
    def from_json_obj(cls, obj: object) -> DiagnosticCounts:
        m = _exact_keys(
            "counts",
            obj,
            (
                "panel_grid_keys",
                "candidate_issue_months",
                "accepted_events",
                "unknown_outcome_issue_months",
                "unresolved_events",
                "censored_issue_months",
                "reason_codes",
            ),
        )
        return cls(
            panel_grid_keys=_require_int("panel_grid_keys", m["panel_grid_keys"]),  # type: ignore[arg-type]
            candidate_issue_months=_require_int(
                "candidate_issue_months", m["candidate_issue_months"]
            ),  # type: ignore[arg-type]
            unknown_outcome_issue_months=_require_int(  # type: ignore[arg-type]
                "unknown_outcome_issue_months", m["unknown_outcome_issue_months"]
            ),
            accepted_events=m["accepted_events"],
            unresolved_events=m["unresolved_events"],
            censored_issue_months=m["censored_issue_months"],
            reason_codes=_sorted_codes_list("counts.reason_codes", m["reason_codes"]),
        )


@dataclass(frozen=True)
class DiagnosticProjection:
    """Stored display projection (§9.4 minus identity / timestamps, which the database adds on read).

    Construct with ``source_frontiers``, ``coverage``, ``counts``, ``limitations`` (and an empty
    ``accepted_events``); the v1 constants keep their defaults and are validated. Collections are
    normalized to canonical order (frontiers by source/kind; coverage by period/source/event type).
    """

    source_frontiers: tuple[SourceFrontier, ...]
    coverage: tuple[CoverageCellDisplay, ...]
    counts: DiagnosticCounts
    limitations: tuple[str, ...]
    accepted_events: tuple[AcceptedEventDisplay, ...] = ()
    schema_version: str = DISPLAY_VERSION
    product: str = PRODUCT
    tier: str = TIER
    display_mode: str = DISPLAY_MODE
    quality_state: str = "partial"
    build_scope: str = "limited"
    recommendation_eligible: bool = False

    def __post_init__(self) -> None:
        if self.recommendation_eligible is not False or (
            self.schema_version,
            self.product,
            self.tier,
            self.display_mode,
            self.quality_state,
            self.build_scope,
        ) != (
            DISPLAY_VERSION,
            PRODUCT,
            TIER,
            DISPLAY_MODE,
            "partial",
            "limited",
        ):
            raise _fail("diagnostic_contract_unsupported", "projection_constants")
        if self.accepted_events:
            raise _fail(
                "projection_invalid", "accepted_events_must_be_empty_in_display_v1"
            )
        frontiers = tuple(
            sorted(self.source_frontiers, key=lambda f: (f.source, f.inventory_kind))
        )
        if len({(f.source, f.inventory_kind) for f in frontiers}) != len(frontiers):
            raise _fail("projection_invalid", "source_frontiers:duplicate")
        cells = tuple(sorted(self.coverage, key=CoverageCellDisplay.sort_key))
        if len({cell.sort_key() for cell in cells}) != len(cells):
            raise _fail("projection_invalid", "coverage:duplicate")
        object.__setattr__(self, "source_frontiers", frontiers)
        object.__setattr__(self, "coverage", cells)
        object.__setattr__(self, "accepted_events", ())
        object.__setattr__(self, "limitations", _codes("limitations", self.limitations))

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "product": self.product,
            "tier": self.tier,
            "display_mode": self.display_mode,
            "quality_state": self.quality_state,
            "build_scope": self.build_scope,
            "recommendation_eligible": self.recommendation_eligible,
            "source_frontiers": [f.to_json_obj() for f in self.source_frontiers],
            "coverage": [cell.to_json_obj() for cell in self.coverage],
            "accepted_events": [],
            "counts": self.counts.to_json_obj(),
            "limitations": list(self.limitations),
        }

    def canonical_bytes(self) -> bytes:
        return c.canonical_json_bytes(self.to_json_obj())

    @property
    def digest(self) -> str:
        """``projection_digest`` (canonical JSON digest; SQL ``bond_credit_json_digest`` parity)."""
        return c.digest_of(self.to_json_obj())

    @classmethod
    def from_json_obj(cls, obj: object) -> DiagnosticProjection:
        keys = (
            "schema_version",
            "product",
            "tier",
            "display_mode",
            "quality_state",
            "build_scope",
            "recommendation_eligible",
            "source_frontiers",
            "coverage",
            "accepted_events",
            "counts",
            "limitations",
        )
        m = _exact_keys("projection", obj, keys)
        if (
            not isinstance(m["source_frontiers"], list)
            or not isinstance(m["coverage"], list)
            or not isinstance(m["accepted_events"], list)
            or not isinstance(m["limitations"], list)
        ):
            raise _fail("projection_invalid", "collections_expected")
        if m["accepted_events"]:
            raise _fail(
                "projection_invalid", "accepted_events_must_be_empty_in_display_v1"
            )
        return cls(
            source_frontiers=tuple(
                SourceFrontier.from_json_obj(x) for x in m["source_frontiers"]
            ),
            coverage=tuple(CoverageCellDisplay.from_json_obj(x) for x in m["coverage"]),
            counts=DiagnosticCounts.from_json_obj(m["counts"]),
            limitations=_sorted_codes_list("limitations", m["limitations"]),
            schema_version=m["schema_version"],
            product=m["product"],
            tier=m["tier"],
            display_mode=m["display_mode"],
            quality_state=m["quality_state"],
            build_scope=m["build_scope"],
            recommendation_eligible=m["recommendation_eligible"],
        )


# ---------------------------------------------------------------------------
# Frontier records and coverage rationale (embedded provenance)
# ---------------------------------------------------------------------------
def frontier_record(
    *,
    source: str,
    inventory_kind: str,
    frontier: dt.date | None,
    observed_at: dt.datetime,
    state: str,
    filing_count: int | None,
    reason_codes: Iterable[str] = (),
    basis: str,
    code: str,
    manifest_sha256: str,
    source_key: str,
) -> dict[str, Any]:
    """One canonical frontier record (``basis``/``code``/``manifest_sha256``/``source_key`` stay internal).

    ``manifest_sha256`` is the bare 64-hex SHA-256 of the sanitized source-frontier manifest the record
    was read from. None of the text fields may contain path separators.
    """
    if (
        source not in SOURCES
        or inventory_kind not in FRONTIER_KINDS[source]
        or state not in FRONTIER_STATES
    ):
        raise _fail(
            "frontier_invalid", f"source_kind_state:{source}/{inventory_kind}/{state}"
        )
    if not (
        _BASIS.fullmatch(basis)
        and _CODE.fullmatch(code)
        and _HEX64.fullmatch(manifest_sha256)
        and _SOURCE_KEY.fullmatch(source_key)
    ):
        raise _fail("frontier_invalid", "text_fields")
    if frontier is not None and not isinstance(frontier, dt.date):
        raise _fail("frontier_invalid", "frontier")
    _require_int("filing_count", filing_count, nullable=True)
    return {
        "basis": basis,
        "code": code,
        "filing_count": filing_count,
        "frontier": None if frontier is None else frontier.isoformat(),
        "inventory_kind": inventory_kind,
        "manifest_sha256": manifest_sha256,
        "observed_at": _ts_text(_require_ts("observed_at", observed_at)),
        "reason_codes": list(_codes("frontier.reason_codes", reason_codes)),
        "source": source,
        "source_key": source_key,
        "state": state,
    }


_RECORD_KEYS = (
    "basis",
    "code",
    "filing_count",
    "frontier",
    "inventory_kind",
    "manifest_sha256",
    "observed_at",
    "reason_codes",
    "source",
    "source_key",
    "state",
)


def _validated_record(record: object) -> dict[str, Any]:
    m = _exact_keys("frontier_record", record, _RECORD_KEYS)
    rebuilt = frontier_record(
        source=m["source"],
        inventory_kind=m["inventory_kind"],
        frontier=_parse_date("frontier", m["frontier"], nullable=True),
        observed_at=_parse_ts("observed_at", m["observed_at"]),
        state=m["state"],
        filing_count=m["filing_count"],
        reason_codes=_sorted_codes_list("frontier.reason_codes", m["reason_codes"]),
        basis=m["basis"],
        code=m["code"],
        manifest_sha256=m["manifest_sha256"],
        source_key=m["source_key"],
    )
    return rebuilt


def coverage_rationale(
    *, frontiers: Iterable[Mapping[str, Any]], reason_codes: Iterable[str] = ()
) -> str:
    """Canonical-JSON coverage ``rationale`` text carrying the given frontier records and cell reason codes."""
    records = sorted(
        (_validated_record(r) for r in frontiers),
        key=lambda r: (r["source"], r["inventory_kind"]),
    )
    if len(records) > 16:
        raise _fail("frontier_invalid", "too_many_records")
    payload = {
        "format": RATIONALE_FORMAT,
        "frontiers": records,
        "reason_codes": list(_codes("reason_codes", reason_codes)),
    }
    return c.canonical_json_bytes(payload).decode("utf-8")


def parse_rationale(text: str) -> tuple[tuple[dict[str, Any], ...], tuple[str, ...]]:
    """Validate a coverage rationale; returns ``(frontier records, cell reason codes)``."""
    try:
        payload = c.load_json_strict(text)
    except (ValueError, c.ContractError) as exc:
        raise _fail("coverage_invalid", "rationale_not_json_object") from exc
    m = _exact_keys("rationale", payload, ("format", "frontiers", "reason_codes"))
    if (
        m["format"] != RATIONALE_FORMAT
        or not isinstance(m["frontiers"], list)
        or len(m["frontiers"]) > 16
    ):
        raise _fail("coverage_invalid", "rationale_format")
    return (
        tuple(_validated_record(r) for r in m["frontiers"]),
        _sorted_codes_list("rationale.reason_codes", m["reason_codes"]),
    )


def frontier_manifest_digest(records: Iterable[Mapping[str, Any]]) -> str:
    """The ``source_frontier_manifest_digest`` bound to the embedded (deduplicated) frontier records."""
    ordered = sorted(
        (dict(r) for r in records), key=lambda r: (r["source"], r["inventory_kind"])
    )
    return c.digest_of({"version": FRONTIER_MANIFEST_VERSION, "records": ordered})


# ---------------------------------------------------------------------------
# Pure derivation (mirrors bond_default_diag_derive)
# ---------------------------------------------------------------------------
def frontier_records_of(
    coverage: Iterable[c.CoverageCell], *, knowledge_cutoff: dt.datetime
) -> tuple[dict[str, Any], ...]:
    """Distinct frontier records of every coverage rationale (sorted; consistent per source/kind)."""
    distinct: dict[str, dict[str, Any]] = {}
    for cell in coverage:
        records, _ = parse_rationale(cell.rationale)
        for record in records:
            distinct[c.canonical_json_bytes(record).decode("utf-8")] = record
    ordered = sorted(
        distinct.values(), key=lambda r: (r["source"], r["inventory_kind"])
    )
    if not ordered:
        raise _fail("coverage_invalid", "no_frontier_records")
    if len({(r["source"], r["inventory_kind"]) for r in ordered}) != len(ordered):
        raise _fail("coverage_invalid", "frontier_records_inconsistent")
    if any(
        _parse_ts("observed_at", r["observed_at"]) > knowledge_cutoff for r in ordered
    ):
        raise _fail("coverage_invalid", "frontier_observed_after_cutoff")
    return tuple(ordered)


def derive_projection(
    *,
    target_month: dt.date,
    knowledge_cutoff: dt.datetime,
    panel_grid_count: int,
    coverage: Iterable[c.CoverageCell],
    start_counts: Mapping[dt.date, int] | None = None,
) -> DiagnosticProjection:
    """Projection derived from coverage rows alone (no rating rows are iterated).

    ``start_counts`` (month -> panel start keys) is optional: when supplied, every cell's exposure is
    cross-checked against it, as the SQL derivation does against the persisted ratings frame.
    """
    cells = sorted(coverage, key=lambda cell: cell.key())
    if not cells:
        raise _fail("coverage_invalid", "frame_counts")
    rows: list[CoverageCellDisplay] = []
    reason_union: set[str] = set()
    for cell in cells:
        _, cell_codes = parse_rationale(cell.rationale)
        reason_union.update(cell_codes)
        if (cell.source, cell.event_type) not in COVERAGE_CELLS:
            raise _fail(
                "coverage_invalid",
                f"cell_shape:{cell.period_label}/{cell.source}/{cell.event_type}",
            )
        if not _PERIOD.fullmatch(cell.period_label):
            raise _fail("coverage_invalid", f"cell_shape:{cell.period_label}")
        if (
            (
                cell.rating_stratum,
                cell.exposure_cohort,
                cell.state,
                cell.denominator_basis,
            )
            != ("unknown", "all", "unavailable", "panel_exposure")
            or cell.denominator_count != cell.exposed_issue_months
            or (cell.event_count, cell.unlinked_count, cell.date_uncertain_count)
            != (0, 0, 0)
            or cell.unknown_outcome_issue_months != cell.exposed_issue_months
            or (cell.lag_p50_days, cell.lag_p90_days, cell.lag_max_days)
            != (None, None, None)
            or cell.validation_receipt_digest is not None
        ):
            raise _fail(
                "coverage_invalid",
                f"cell_shape:{cell.period_label}/{cell.source}/{cell.event_type}",
            )
        rows.append(
            CoverageCellDisplay(
                period_label=cell.period_label,
                source=cell.source,
                event_type=cell.event_type,
                rating_stratum=cell.rating_stratum,
                exposure_cohort=cell.exposure_cohort,
                state=cell.state,
                denominator_basis=cell.denominator_basis,
                denominator_count=cell.denominator_count or 0,
                exposed_issue_months=cell.exposed_issue_months,
                event_count=0,
                unlinked_count=0,
                date_uncertain_count=0,
                unknown_outcome_issue_months=cell.unknown_outcome_issue_months,
                source_frontier=cell.source_frontier,
                reason_codes=_codes("cell", parse_rationale(cell.rationale)[1]),
            )
        )
    periods = sorted({r.period_label for r in rows})
    if len(rows) != 6 * len(periods):
        raise _fail("coverage_invalid", "cells_per_period")
    months = [dt.date(int(p[:4]), int(p[5:7]), 1) for p in periods]
    if months[-1] != target_month:
        raise _fail("coverage_invalid", "last_period_not_target_month")
    if any(c.add_months(a, 1) != b for a, b in itertools.pairwise(months)):
        raise _fail("coverage_invalid", "periods_not_contiguous")
    if start_counts is not None:
        for row in rows:
            month = dt.date(int(row.period_label[:4]), int(row.period_label[5:7]), 1)
            if row.exposed_issue_months != start_counts.get(c.add_months(month, -1), 0):
                raise _fail(
                    "coverage_invalid",
                    f"exposure_not_panel_start_count:{row.period_label}",
                )
    candidate = sum(
        r.exposed_issue_months
        for r in rows
        if (r.source, r.event_type) == ("all", "all")
    )
    records = frontier_records_of(cells, knowledge_cutoff=knowledge_cutoff)
    if len({r["manifest_sha256"] for r in records}) != 1:
        raise _fail("coverage_invalid", "manifest_sha256_not_single")
    for cell in cells:
        label = f"{cell.period_label}/{cell.source}/{cell.event_type}"
        cell_records, _ = parse_rationale(cell.rationale)
        if cell.source == "all":
            if cell.source_frontier is not None:
                raise _fail("coverage_invalid", f"cell_frontier:{label}")
            continue
        own = {r["frontier"] for r in cell_records if r["frontier"] is not None}
        if (
            not cell_records
            or any(r["source"] != cell.source for r in cell_records)
            or (
                cell.source_frontier is not None
                and cell.source_frontier.isoformat() not in own
            )
        ):
            raise _fail("coverage_invalid", f"cell_frontier:{label}")
    frontiers = tuple(
        SourceFrontier.from_json_obj(
            {
                k: r[k]
                for k in (
                    "source",
                    "inventory_kind",
                    "frontier",
                    "observed_at",
                    "state",
                    "filing_count",
                    "reason_codes",
                )
            }
        )
        for r in records
    )
    limitations = (
        set(BASE_LIMITATIONS)
        | reason_union
        | {code for r in records for code in r["reason_codes"]}
    )
    return DiagnosticProjection(
        source_frontiers=frontiers,
        coverage=tuple(rows),
        counts=DiagnosticCounts(
            panel_grid_keys=panel_grid_count,
            candidate_issue_months=candidate,
            unknown_outcome_issue_months=candidate,
        ),
        limitations=tuple(sorted(limitations)),
    )


def derive_projection_from_bundle(bundle: CreditBundle) -> DiagnosticProjection:
    """Full check of a coverage-only bundle (empty frames, all-missing ratings, exposure = panel start keys)."""
    if any(
        bundle.frames[name]
        for name in (
            "source_packages",
            "publication_sources",
            "observations",
            "event_links",
            "adjudications",
            "ncen_filings",
            "events",
            "followups",
            "exit_evidence",
            "family_contexts",
            "family_evidence",
            "proposal_evidence",
            "exchange_relations",
        )
    ):
        raise _fail("coverage_invalid", "frames_not_empty")
    manifest = bundle.manifest
    ratings = bundle.frames["ratings"]
    if manifest["quality_state"] != "partial" or manifest["build_scope"] != "limited":
        raise _fail("publication_not_partial_limited")
    if len(ratings) != 2 * manifest["panel_grid_count"] or any(
        r.state != "missing"
        or r.bucket is not None
        or r.agency_source_ids
        or r.binding_link_ids  # type: ignore[attr-defined]
        or r.action_date is not None
        or r.public_known_at is not None  # type: ignore[attr-defined]
        or r.coverage_frontier is not None
        or r.action_input_digest is not None  # type: ignore[attr-defined]
        or r.default_overlay_episode_id is not None
        for r in ratings
    ):  # type: ignore[attr-defined]
        raise _fail("coverage_invalid", "ratings_not_all_missing")
    starts = Counter(r.month for r in ratings if r.view_kind == "public_pit")  # type: ignore[attr-defined]
    projection = derive_projection(
        target_month=manifest["target_month"],
        knowledge_cutoff=manifest["knowledge_cutoff"],
        panel_grid_count=manifest["panel_grid_count"],
        coverage=bundle.frames["coverage"],  # type: ignore[arg-type]
        start_counts=starts,
    )
    first = min(
        dt.date(int(p.period_label[:4]), int(p.period_label[5:7]), 1)
        for p in projection.coverage
    )
    if starts and (
        min(starts) < c.add_months(first, -1) or max(starts) > manifest["target_month"]
    ):
        raise _fail("coverage_invalid", "ratings_outside_window")
    at_target = starts.get(manifest["target_month"], 0)
    if (
        projection.counts.candidate_issue_months + at_target
        != manifest["panel_grid_count"]
    ):
        raise _fail("coverage_invalid", "grid_count_mismatch")
    return projection


# ---------------------------------------------------------------------------
# Release identity (parity with bond_default_diag_identity / _release_id_for)
# ---------------------------------------------------------------------------
def release_identity(
    *,
    publication_id: uuid.UUID,
    publication_fingerprint: str,
    target_month: dt.date,
    knowledge_cutoff: dt.datetime,
    policy_digest: str,
    contract_digest: str,
    source_frontier_manifest_digest: str,
    projection_digest: str,
    diagnostic_sql_digest: str,
) -> dict[str, str]:
    return {
        "namespace": RELEASE_NAMESPACE,
        "product": PRODUCT,
        "publication_id": str(publication_id),
        "publication_fingerprint": publication_fingerprint,
        "display_projection_version": DISPLAY_VERSION,
        "tier": TIER,
        "target_month": target_month.isoformat(),
        "knowledge_cutoff": _ts_text(knowledge_cutoff),
        "policy_digest": policy_digest,
        "contract_digest": contract_digest,
        "source_frontier_manifest_digest": source_frontier_manifest_digest,
        "projection_digest": projection_digest,
        "diagnostic_sql_digest": diagnostic_sql_digest,
    }


def release_id_for(identity: Mapping[str, str]) -> uuid.UUID:
    """Deterministic UUIDv5 (contract namespace) over the identity digest; no audit timestamps.

    Mirrored by ``bond_default_diag_release_id_for`` (``bond_credit_uuid5`` in SQL).
    """
    return c.uuid5_of("bond_default_diagnostic_release", c.digest_of(dict(identity)))


# ---------------------------------------------------------------------------
# SQL file, install and database API
# ---------------------------------------------------------------------------
def diagnostic_sql_digest(path: Any = SQL_PATH) -> str:
    """LF-normalized SHA-256 of the diagnostic SQL file (``sha256:`` encoding)."""
    return "sha256:" + c.sha256_hex(path.read_bytes().replace(b"\r\n", b"\n"))


def _record_installation(conn: Any, schema: str) -> bool:
    """Record the pinned digest as the schema's installation marker (idempotent; returns True when a row was added).

    A direct INSERT as the table owner (the installer): no function or grant exists for it. Nothing is
    recorded when the most recent marker already equals the digest, so a rerun is a no-op while a different
    (older or newer) pinned file installed later becomes the current marker.
    """
    from psycopg import sql

    table = sql.SQL("{}.bond_default_diagnostic_installations").format(
        sql.Identifier(schema)
    )
    with conn.transaction():
        conn.execute(
            "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(%s, 0))",
            ["bond_default_events_diagnostic_v1|install"],
        )
        inserted = conn.execute(
            sql.SQL(
                "INSERT INTO {t} (sql_digest) SELECT %s WHERE %s IS DISTINCT FROM "
                "(SELECT i.sql_digest FROM {t} i ORDER BY i.installed_at DESC, i.installation_id DESC LIMIT 1) "
                "RETURNING 1"
            ).format(t=table),
            [DIAGNOSTIC_SQL_DIGEST, DIAGNOSTIC_SQL_DIGEST],
        ).fetchone()
    return inserted is not None


def install_diagnostic_schema(conn: Any, *, schema: str) -> None:
    """Apply the diagnostic SQL (autocommit connection; the file owns BEGIN/COMMIT).

    Explicit operator / disposable-test use only: run it as the trusted table owner after the four
    ``install_schema`` files and after the administrator created ``bond_default_diagnostic_reader``.
    The pinned digest is checked before any statement runs. After the file ran, the pinned digest is recorded in
    ``bond_default_diagnostic_installations`` (idempotent); ``bond_default_prepare_diagnostic`` and the guard
    then refuse any release whose ``diagnostic_sql_digest`` differs from the most recent marker
    (``diagnostic_sql_pin_mismatch``), so a stale install and a newer worker (or the reverse) cannot interoperate.

    The four pinned files declare ``SET search_path FROM CURRENT``: they capture the *installing session's*
    search_path in ``proconfig``. ``install_schema`` therefore starts with ``SET search_path TO public,
    pg_temp`` (as does this function, explicitly and verified below); any other installer session would bake a
    different, hijackable search_path into every function. :func:`harden_installed_privileges` and
    :func:`verify_installed_privileges` refuse an installation whose functions do not carry exactly
    ``search_path=public, pg_temp``.
    """
    from psycopg import sql

    if schema != "public":
        raise ValueError("diagnostic_schema_must_be_public")
    if not conn.autocommit:
        raise ValueError("autocommit_connection_required_for_ddl")
    if diagnostic_sql_digest() != DIAGNOSTIC_SQL_DIGEST:
        raise DiagnosticError("diagnostic_sql_digest_not_pinned")
    conn.execute(
        sql.SQL("SET search_path TO {}, pg_temp").format(sql.Identifier(schema))
    )
    if conn.execute("SHOW search_path").fetchone()[0] != EXPECTED_SEARCH_PATH:
        raise DiagnosticError("install_search_path_not_pinned")
    conn.execute(SQL_PATH.read_text(encoding="utf-8"))
    _record_installation(conn, schema)


def _fn(schema: str, name: str) -> Any:
    from psycopg import sql

    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(name))


def _call(
    conn: Any, schema: str, name: str, params: Sequence[Any], casts: Sequence[str]
) -> Any:
    """``SELECT schema.name(%s::t, ...)`` in the caller's transaction; database refusals become DiagnosticError."""
    import psycopg
    from psycopg import sql

    placeholders = sql.SQL(", ").join(
        sql.SQL("%s::{}").format(sql.SQL(t)) for t in casts
    )
    statement = sql.SQL("SELECT {}({})").format(_fn(schema, name), placeholders)
    try:
        return conn.execute(statement, list(params)).fetchone()[0]
    except psycopg.Error as exc:
        message = (
            exc.diag.message_primary if exc.diag and exc.diag.message_primary else ""
        ).strip()
        if message.startswith(ERROR_PREFIX):
            detail = (exc.diag.message_detail or "") if exc.diag else ""
            raise DiagnosticError(
                message[len(ERROR_PREFIX) :], detail, getattr(exc, "sqlstate", None)
            ) from exc
        raise


def prepare_diagnostic(
    conn: Any,
    *,
    schema: str,
    publication_id: uuid.UUID,
    projection: DiagnosticProjection,
    source_frontier_manifest_digest: str,
) -> uuid.UUID:
    """Persist (or replay) the release of a validated partial/limited build; returns its deterministic id.

    ``source_frontier_manifest_digest`` must equal :func:`frontier_manifest_digest` of the frontier records
    embedded in the build's coverage rationales. No pointer is written and nothing is committed.
    """
    from psycopg.types.json import Jsonb

    if not isinstance(projection, DiagnosticProjection):
        raise TypeError("projection must be a DiagnosticProjection")
    released = _call(
        conn,
        schema,
        "bond_default_prepare_diagnostic",
        [
            publication_id,
            Jsonb(projection.to_json_obj()),
            source_frontier_manifest_digest,
            DIAGNOSTIC_SQL_DIGEST,
        ],
        ["uuid", "jsonb", "text", "text"],
    )
    return released if isinstance(released, uuid.UUID) else uuid.UUID(str(released))


def verify_diagnostic_report(
    conn: Any, *, schema: str, release_id: uuid.UUID
) -> dict[str, Any]:
    """Sanitized identity, digests, counts and projection of a release (writer / auditor; no raw rows)."""
    report = _call(
        conn, schema, "bond_default_verify_diagnostic", [release_id], ["uuid"]
    )
    return dict(report)


def verify_diagnostic(
    conn: Any, *, schema: str, release_id: uuid.UUID
) -> DiagnosticProjection:
    """Re-run every guard by ID (allowed before election) and return the verified projection."""
    return DiagnosticProjection.from_json_obj(
        verify_diagnostic_report(conn, schema=schema, release_id=release_id)[
            "projection"
        ]
    )


def promote_diagnostic(
    conn: Any,
    *,
    schema: str,
    release_id: uuid.UUID,
    expected_release_id: uuid.UUID | None,
) -> None:
    """Compare-and-set the diagnostic pointer (``None`` = pointer must be absent); never the qualified pointer."""
    _call(
        conn,
        schema,
        "bond_default_promote_diagnostic",
        [release_id, expected_release_id],
        ["uuid", "uuid"],
    )


def revoke_diagnostic(
    conn: Any, *, schema: str, release_id: uuid.UUID, reason_code: str
) -> None:
    """Append a revocation (idempotent for an identical replay); the next runtime read refuses the release."""
    if not _REASON.fullmatch(reason_code or ""):
        raise _fail("invalid_argument", "reason_code")
    _call(
        conn,
        schema,
        "bond_default_revoke_diagnostic",
        [release_id, reason_code],
        ["uuid", "text"],
    )


def read_current_diagnostic(conn: Any, *, schema: str) -> dict[str, Any]:
    """The runtime reader's complete §9.4 JSON (test / smoke helper; the API repository issues the same SELECT)."""
    return dict(_call(conn, schema, "bond_default_current_diagnostic_release", [], []))


# ---------------------------------------------------------------------------
# Installed-object privileges: harden (owner) and verify (read-only)
# ---------------------------------------------------------------------------
#: Every SQL file whose objects the hardening owns: the four pinned files plus the diagnostic file.
INSTALLED_SQL_PATHS: Final = (*c.SQL_PATHS, SQL_PATH)
#: The only non-owner grantees the installed objects may ever have.
INTENDED_ROLES: Final = (
    "bond_credit_writer",
    "bond_credit_reader",
    "bond_credit_auditor",
    "bond_default_diagnostic_reader",
)
EXPECTED_SEARCH_PATH: Final = "public, pg_temp"
EXPECTED_PROCONFIG: Final = [f"search_path={EXPECTED_SEARCH_PATH}"]
RUNTIME_READER_FUNCTION: Final = "bond_default_current_diagnostic_release"
API_FUNCTION_GRANTS: Final = {
    "bond_default_prepare_diagnostic": ("bond_credit_writer",),
    "bond_default_promote_diagnostic": ("bond_credit_writer",),
    "bond_default_revoke_diagnostic": ("bond_credit_writer",),
    "bond_default_verify_diagnostic": ("bond_credit_auditor", "bond_credit_writer"),
    RUNTIME_READER_FUNCTION: ("bond_default_diagnostic_reader",),
}
_IDENT = r"[a-z_][a-z0-9_]*"
_TABLE_DEF = re.compile(rf"^CREATE TABLE IF NOT EXISTS ({_IDENT})\b", re.MULTILINE)
_FUNCTION_DEF = re.compile(rf"^CREATE OR REPLACE FUNCTION ({_IDENT})\(", re.MULTILINE)
_GRANT_STMT = re.compile(r"^GRANT\b[^;]*;", re.MULTILINE)
_GRANT_SHAPE = re.compile(
    r"GRANT\s+[A-Z, ]+\s+ON\s+(?:FUNCTION\s+)?[a-z0-9_(),\[\] \n]+?\s+TO\s+"
    rf"({_IDENT}(?:\s*,\s*{_IDENT})*)\s*;",
    re.DOTALL,
)
_CAP = 25


@dataclass(frozen=True)
class InstalledManifest:
    """Exact names created by the installed SQL files, read from the files themselves."""

    tables: tuple[str, ...]
    functions: tuple[str, ...]
    grants: tuple[str, ...]

    def digest(self) -> str:
        return c.digest_of(
            {
                "tables": list(self.tables),
                "functions": list(self.functions),
                "grants": list(self.grants),
            }
        )


def installed_manifest(
    paths: Iterable[Any] = INSTALLED_SQL_PATHS,
) -> InstalledManifest:
    """Deterministic object manifest: ``CREATE TABLE IF NOT EXISTS`` / ``CREATE OR REPLACE FUNCTION``
    names and top-level ``GRANT`` statements of the four pinned files and the diagnostic file.

    Nothing is matched by pattern in the database: the exact name set comes from these files, so an
    unrelated ``bond_credit_*``-looking object in the same schema is never in scope.
    """
    tables: set[str] = set()
    functions: set[str] = set()
    grants: list[str] = []
    for path in paths:
        text = path.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
        tables.update(_TABLE_DEF.findall(text))
        functions.update(_FUNCTION_DEF.findall(text))
        for statement in _GRANT_STMT.findall(text):
            shape = _GRANT_SHAPE.fullmatch(statement)
            if shape is None or "GRANT OPTION" in statement.upper():
                raise _fail("privileges_manifest_invalid", statement[:60])
            if not {r.strip() for r in shape.group(1).split(",")} <= set(
                INTENDED_ROLES
            ):
                raise _fail("privileges_manifest_invalid", "unintended_grantee")
            grants.append(statement)
    if not tables or not functions:
        raise _fail("privileges_manifest_invalid", "empty")
    return InstalledManifest(
        tuple(sorted(tables)), tuple(sorted(functions)), tuple(grants)
    )


_GRANT_PARTS = re.compile(
    r"GRANT\s+(?P<privs>[A-Z, ]+?)\s+ON\s+(?P<fn>FUNCTION\s+)?(?P<objs>.+?)\s+TO\s+(?P<roles>[a-z_0-9, \n]+?)\s*;",
    re.DOTALL,
)


def _split_top_level(text: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    parts.append("".join(current).strip())
    return [" ".join(p.split()) for p in parts if p.strip()]


def _parse_grant(statement: str) -> tuple[tuple[str, ...], bool, list[str], list[str]]:
    m = _GRANT_PARTS.fullmatch(statement)
    if m is None:
        raise _fail("privileges_manifest_invalid", statement[:60])
    privileges = tuple(x.strip() for x in m["privs"].split(","))
    return (
        privileges,
        m["fn"] is not None,
        _split_top_level(m["objs"]),
        [r.strip() for r in m["roles"].split(",")],
    )


@dataclass(frozen=True)
class _Targets:
    schema: str
    owner_names: dict[str, str]  # display name -> owner
    relations: dict[int, tuple[str, str]]  # oid -> (display name, sql object kind)
    functions: dict[
        int, tuple[str, str, str]
    ]  # oid -> (display, proname, identity args)
    missing: tuple[str, ...]


def _ident(schema: str) -> Any:
    from psycopg import sql

    if schema != "public":
        raise ValueError("privileges_schema_must_be_public")
    return sql.Identifier(schema)


def _resolve_targets(conn: Any, schema: str, manifest: InstalledManifest) -> _Targets:
    """Resolve manifest names (exact) plus sequences owned by manifest tables to catalog oids."""
    owners: dict[str, str] = {}
    relations: dict[int, tuple[str, str]] = {}
    functions: dict[int, tuple[str, str, str]] = {}
    found_tables: set[str] = set()
    for oid, name, kind, owner in conn.execute(
        "SELECT c.oid::bigint, c.relname::text, c.relkind::text, pg_catalog.pg_get_userbyid(c.relowner)::text "
        "FROM pg_catalog.pg_class c WHERE c.relnamespace = %s::regnamespace "
        "AND c.relname = ANY(%s) ORDER BY 2",
        [schema, list(manifest.tables)],
    ).fetchall():
        if kind not in ("r", "p"):
            raise _fail("privileges_target_invalid", f"{name}:relkind:{kind}")
        relations[oid] = (name, "TABLE")
        owners[name] = owner
        found_tables.add(name)
    if relations:
        for oid, name, owner in conn.execute(
            "SELECT s.oid::bigint, s.relname::text, pg_catalog.pg_get_userbyid(s.relowner)::text "
            "FROM pg_catalog.pg_class s JOIN pg_catalog.pg_depend d ON d.objid = s.oid "
            "AND d.classid = 'pg_catalog.pg_class'::regclass AND d.refclassid = 'pg_catalog.pg_class'::regclass "
            "AND d.deptype IN ('a', 'i') WHERE s.relkind = 'S' AND d.refobjid = ANY(%s::oid[]) ORDER BY 2",
            [list(relations)],
        ).fetchall():
            relations[oid] = (name, "SEQUENCE")
            owners[name] = owner
    found_functions: set[str] = set()
    for oid, name, args, owner in conn.execute(
        "SELECT p.oid::bigint, p.proname::text, pg_catalog.pg_get_function_identity_arguments(p.oid), "
        "pg_catalog.pg_get_userbyid(p.proowner)::text FROM pg_catalog.pg_proc p "
        "WHERE p.pronamespace = %s::regnamespace AND p.proname = ANY(%s) ORDER BY 2, 3",
        [schema, list(manifest.functions)],
    ).fetchall():
        display = f"{name}({args})"
        functions[oid] = (display, name, args)
        owners[display] = owner
        found_functions.add(name)
    missing = tuple(
        sorted(
            [f"table:{t}" for t in set(manifest.tables) - found_tables]
            + [f"function:{f}" for f in set(manifest.functions) - found_functions]
        )
    )
    return _Targets(schema, owners, relations, functions, missing)


def _acl_rows(conn: Any, targets: _Targets) -> list[tuple[str, str, str, str]]:
    """``(kind, object, grantee, privilege)`` for every non-owner ACL entry of the targets."""
    rows: list[tuple[str, str, str, str]] = []
    if targets.relations:
        for oid, grantee, privilege in conn.execute(
            "SELECT c.oid::bigint, CASE a.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_catalog.pg_get_userbyid(a.grantee)::text END, "
            "a.privilege_type FROM pg_catalog.pg_class c CROSS JOIN LATERAL pg_catalog.aclexplode("
            "COALESCE(c.relacl, CASE WHEN c.relkind = 'S' THEN pg_catalog.acldefault('s', c.relowner) ELSE pg_catalog.acldefault('r', c.relowner) END)) a "
            "WHERE c.oid = ANY(%s::oid[]) AND a.grantee <> c.relowner",
            [list(targets.relations)],
        ).fetchall():
            name, kind = targets.relations[oid]
            rows.append((kind, name, grantee, privilege))
    if targets.functions:
        for oid, grantee, privilege in conn.execute(
            "SELECT p.oid::bigint, CASE a.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_catalog.pg_get_userbyid(a.grantee)::text END, "
            "a.privilege_type FROM pg_catalog.pg_proc p CROSS JOIN LATERAL pg_catalog.aclexplode("
            "COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))) a "
            "WHERE p.oid = ANY(%s::oid[]) AND a.grantee <> p.proowner",
            [list(targets.functions)],
        ).fetchall():
            rows.append(("FUNCTION", targets.functions[oid][0], grantee, privilege))
    return sorted(rows)


def _column_acl_rows(conn: Any, targets: _Targets) -> list[tuple[str, str, str]]:
    if not targets.relations:
        return []
    return sorted(
        (targets.relations[oid][0], grantee, privilege)
        for oid, grantee, privilege in conn.execute(
            "SELECT a.attrelid::bigint, CASE x.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_catalog.pg_get_userbyid(x.grantee)::text END, "
            "x.privilege_type FROM pg_catalog.pg_attribute a CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) x "
            "WHERE a.attrelid = ANY(%s::oid[]) AND a.attnum > 0 AND NOT a.attisdropped AND a.attacl IS NOT NULL "
            "AND x.grantee <> (SELECT relowner FROM pg_catalog.pg_class WHERE oid = a.attrelid)",
            [list(targets.relations)],
        ).fetchall()
    )


def _search_path_offenders(conn: Any, targets: _Targets) -> list[str]:
    """Manifest functions whose ``proconfig`` is not exactly ``search_path=public, pg_temp``."""
    if not targets.functions:
        return []
    return sorted(
        f"{name}:{config}"
        for name, config in conn.execute(
            "SELECT p.proname::text || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')', p.proconfig::text "
            "FROM pg_catalog.pg_proc p WHERE p.oid = ANY(%s::oid[]) AND p.proconfig IS DISTINCT FROM %s::text[]",
            [list(targets.functions), EXPECTED_PROCONFIG],
        ).fetchall()
    )


def _expected_intended_grants(
    conn: Any, schema: str, manifest: InstalledManifest, targets: _Targets
) -> tuple[set[tuple[str, str, str, str]], list[str]]:
    """``(kind, object, role, privilege)`` the SQL files' own GRANT statements define, resolved in the database."""
    expected: set[tuple[str, str, str, str]] = set()
    unresolved: list[str] = []
    table_names = {name for name, kind in targets.relations.values() if kind == "TABLE"}
    for statement in manifest.grants:
        privileges, is_function, objects, roles = _parse_grant(statement)
        for obj in objects:
            if is_function:
                oid = conn.execute(
                    "SELECT pg_catalog.to_regprocedure(%s)::oid::bigint",
                    [f"{schema}.{obj}"],
                ).fetchone()[0]
                if oid is None or oid not in targets.functions:
                    unresolved.append(obj)
                    continue
                name = targets.functions[oid][0]
                kind = "FUNCTION"
            else:
                if obj not in table_names:
                    unresolved.append(obj)
                    continue
                name, kind = obj, "TABLE"
            expected.update(
                (kind, name, role, priv) for role in roles for priv in privileges
            )
    return expected, unresolved


def harden_installed_privileges(conn: Any, *, schema: str = "public") -> dict[str, Any]:
    """Close default-privilege leaks on exactly the objects the installed SQL files created.

    Run as the installing owner (``worker_writer``) right after ``install_schema`` and
    :func:`install_diagnostic_schema`. In one transaction it

    1. resolves the exact object set from :func:`installed_manifest` (tables, sequences owned by those
       tables, functions by exact name) and fails loudly when an object is missing or is not owned by
       the current user;
    2. ``REVOKE ALL`` on every such table/sequence and function from every grantee that is neither the owner
       nor one of :data:`INTENDED_ROLES` (PUBLIC included), including column-level grants;
    3. re-executes the top-level ``GRANT`` statements of those SQL files (the intended grants, verbatim; they
       only name :data:`INTENDED_ROLES`) and reports any intended-role grant that was missing.

    It never touches an object outside the manifest, never changes the schema's default-privilege settings and is idempotent
    (a second run reports nothing revoked). Returns a sanitized report; ``revoked`` holds
    ``(object, grantee, privilege)`` tuples.
    """
    from psycopg import sql

    schema_ident = _ident(schema)
    if not conn.autocommit:
        raise ValueError("autocommit_connection_required_for_ddl")
    manifest = installed_manifest()
    with conn.transaction():
        conn.execute(
            sql.SQL("SET LOCAL search_path TO {}, pg_temp").format(schema_ident)
        )
        targets = _resolve_targets(conn, schema, manifest)
        if targets.missing:
            raise _fail("privileges_target_missing", ",".join(targets.missing[:_CAP]))
        current = conn.execute("SELECT current_user::text").fetchone()[0]
        foreign = sorted(
            name for name, owner in targets.owner_names.items() if owner != current
        )
        if foreign:
            raise _fail("privileges_not_owner", f"{current}:{','.join(foreign[:_CAP])}")
        wrong_config = _search_path_offenders(conn, targets)
        if wrong_config:
            raise _fail("privileges_search_path", ",".join(wrong_config[:_CAP]))
        before = _acl_rows(conn, targets)
        columns = _column_acl_rows(conn, targets)
        unintended = [row for row in before if row[2] not in INTENDED_ROLES]
        revoked: list[tuple[str, str, str]] = [
            (name, grantee, priv) for _k, name, grantee, priv in unintended
        ]
        revoked += [
            (f"{name}.(column)", grantee, priv)
            for name, grantee, priv in columns
            if grantee not in INTENDED_ROLES
        ]
        by_object: dict[tuple[str, str], set[str]] = {}
        for kind, name, grantee, _priv in unintended:
            by_object.setdefault((kind, name), set()).add(grantee)
        for name, grantee, _priv in columns:
            if grantee not in INTENDED_ROLES:
                by_object.setdefault(("TABLE", name), set()).add(grantee)
        function_args = {
            display: args for display, _n, args in targets.functions.values()
        }
        for (kind, name), grantees in sorted(by_object.items()):
            role_list = sql.SQL(", ").join(
                sql.SQL("PUBLIC") if g == "PUBLIC" else sql.Identifier(g)
                for g in sorted(grantees)
            )
            if kind == "FUNCTION":
                bare = name.split("(", 1)[0]
                target = sql.SQL("FUNCTION {}.{}({})").format(
                    schema_ident, sql.Identifier(bare), sql.SQL(function_args[name])
                )
            else:
                target = sql.SQL("{} {}.{}").format(
                    sql.SQL(kind), schema_ident, sql.Identifier(name)
                )
            conn.execute(sql.SQL("REVOKE ALL ON {} FROM {}").format(target, role_list))
        intended_before = {row for row in before if row[2] in INTENDED_ROLES}
        for statement in manifest.grants:
            conn.execute(statement)
        after = _acl_rows(conn, targets)
        added = sorted(
            {row for row in after if row[2] in INTENDED_ROLES} - intended_before
        )
        leftover = [r for r in after if r[2] not in INTENDED_ROLES] + [
            ("TABLE", n, g, p)
            for n, g, p in _column_acl_rows(conn, targets)
            if g not in INTENDED_ROLES
        ]
        if leftover:
            raise _fail("privileges_revoke_incomplete", str(leftover[:3]))
    return {
        "schema": schema,
        "owner": current,
        "manifest_digest": manifest.digest(),
        "tables": sum(1 for _n, k in targets.relations.values() if k == "TABLE"),
        "sequences": sum(1 for _n, k in targets.relations.values() if k == "SEQUENCE"),
        "functions": len(targets.functions),
        "revoked": sorted(set(revoked)),
        "intended_grants_added": [
            (name, grantee, priv) for _k, name, grantee, priv in added
        ],
    }


def _check(ok: bool, detail: Any = "") -> dict[str, Any]:
    return {"ok": bool(ok), "detail": detail}


def _default_acl_grantees(conn: Any, schema: str, skip: set[str]) -> list[str]:
    """Roles that ``pg_default_acl`` grants privileges to on new objects of ``schema`` (or globally)."""
    return sorted(
        {
            row[0]
            for row in conn.execute(
                "SELECT pg_catalog.pg_get_userbyid(a.grantee)::text FROM pg_catalog.pg_default_acl d "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(d.defaclacl) a "
                "WHERE d.defaclnamespace = ANY(ARRAY[%s::regnamespace::oid, 0::oid]) "
                "AND a.grantee <> 0 AND a.grantee <> d.defaclrole",
                [schema],
            ).fetchall()
        }
        - skip
    )


def verify_installed_privileges(
    conn: Any,
    *,
    schema: str = "public",
    runtime_role: str = "app_runtime",
    other_roles: Sequence[str] = (),
    require_empty_pointers: bool = True,
) -> dict[str, Any]:
    """Read-only §9.2 read-back of the installed objects; raises ``DiagnosticError`` on any failed check.

    Run as the installing owner or an administrator (it calls ``bond_credit_expected_pins()`` and counts the
    pointer tables). The roles whose reach is checked are ``runtime_role``, every explicit ``other_roles`` entry
    and every role that ``pg_default_acl`` grants privileges to on schema ``schema`` (so a new leaky default
    grantee is covered without being named); roles that do not exist are skipped except ``runtime_role``.
    The failure carries the complete report in ``.report``; a passing call returns it. ``evidence`` in the
    report lists the sha256 of ``pg_get_functiondef`` of every diagnostic function (informational only).
    """
    _ident(schema)
    manifest = installed_manifest()
    targets = _resolve_targets(conn, schema, manifest)
    checks: dict[str, dict[str, Any]] = {}
    checks["objects_present"] = _check(
        not targets.missing and bool(targets.relations) and bool(targets.functions),
        list(targets.missing[:_CAP]),
    )
    owners = sorted(set(targets.owner_names.values()))
    default_grantees = _default_acl_grantees(
        conn, schema, {*INTENDED_ROLES, *owners, runtime_role}
    )
    checked_roles = sorted({*other_roles, *default_grantees} - {runtime_role})
    checks["single_owner_not_runtime"] = _check(
        len(owners) == 1
        and runtime_role not in owners
        and not set(checked_roles) & set(owners),
        owners,
    )
    owner_flags = {
        row[0]: row[1:]
        for row in conn.execute(
            "SELECT rolname::text, rolsuper, rolcreaterole, rolcreatedb FROM pg_catalog.pg_roles WHERE rolname = ANY(%s)",
            [owners],
        ).fetchall()
    }
    checks["owner_not_privileged"] = _check(
        bool(owners)
        and all(owner_flags.get(o) == (False, False, False) for o in owners),
        {o: owner_flags.get(o) for o in owners},
    )
    schema_owner, db_owner = conn.execute(
        "SELECT pg_catalog.pg_get_userbyid(n.nspowner)::text, pg_catalog.pg_get_userbyid(d.datdba)::text "
        "FROM pg_catalog.pg_namespace n, pg_catalog.pg_database d "
        "WHERE n.nspname = %s AND d.datname = current_database()",
        [schema],
    ).fetchone()
    allowed_create = {*owners, schema_owner}
    if schema_owner == "pg_database_owner":
        allowed_create.add(db_owner)
    create_roles = [
        row[0]
        for row in conn.execute(
            "SELECT r.rolname::text FROM pg_catalog.pg_roles r "
            "WHERE NOT r.rolsuper AND pg_catalog.has_schema_privilege(r.oid, %s, 'CREATE') ORDER BY 1",
            [schema],
        ).fetchall()
        if row[0] not in allowed_create
    ]
    public_create = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n CROSS JOIN LATERAL pg_catalog.aclexplode("
        "COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))) a "
        "WHERE n.nspname = %s AND a.grantee = 0 AND a.privilege_type = 'CREATE')",
        [schema],
    ).fetchone()[0]
    checks["schema_create_restricted"] = _check(
        not create_roles and not public_create,
        {"roles": create_roles[:_CAP], "public": bool(public_create)},
    )

    roles = {
        row[0]: row[1:]
        for row in conn.execute(
            "SELECT rolname::text, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb FROM pg_catalog.pg_roles "
            "WHERE rolname = ANY(%s)",
            [[*INTENDED_ROLES, runtime_role, *checked_roles]],
        ).fetchall()
    }
    checks["group_roles_nologin"] = _check(
        all(roles.get(r) == (False, False, False, False) for r in INTENDED_ROLES),
        [r for r in INTENDED_ROLES if roles.get(r) != (False, False, False, False)],
    )
    membership: dict[str, bool] = {}
    if runtime_role in roles:
        for role in (*INTENDED_ROLES, *owners):
            membership[role] = bool(
                conn.execute(
                    "SELECT pg_catalog.pg_has_role(%s, %s, 'member')",
                    [runtime_role, role],
                ).fetchone()[0]
            )
    checks["runtime_membership"] = _check(
        runtime_role in roles
        and membership.get("bond_default_diagnostic_reader") is True
        and not any(
            membership.get(r, True)
            for r in (
                "bond_credit_reader",
                "bond_credit_auditor",
                "bond_credit_writer",
                *owners,
            )
        ),
        {
            "role_present": runtime_role in roles,
            "member_of": sorted(r for r, v in membership.items() if v),
        },
    )

    acl = _acl_rows(conn, targets)
    column_acl = _column_acl_rows(conn, targets)
    unintended = [f"{n}:{g}:{p}" for _k, n, g, p in acl if g not in INTENDED_ROLES] + [
        f"{n}.(column):{g}:{p}" for n, g, p in column_acl if g not in INTENDED_ROLES
    ]
    checks["no_public_or_unintended_grants"] = _check(not unintended, unintended[:_CAP])
    expected, unresolved = _expected_intended_grants(conn, schema, manifest, targets)
    actual = {(kind, n, g, p) for kind, n, g, p in acl if g in INTENDED_ROLES}
    missing_grants = [f"{k}:{n}:{g}:{p}" for k, n, g, p in sorted(expected - actual)]
    extra_grants = [f"{k}:{n}:{g}:{p}" for k, n, g, p in sorted(actual - expected)] + [
        f"{n}.(column):{g}:{p}" for n, g, p in column_acl if g in INTENDED_ROLES
    ]
    checks["intended_grants_exact"] = _check(
        not unresolved and not missing_grants and not extra_grants,
        {
            "unresolved": unresolved[:_CAP],
            "missing": missing_grants[:_CAP],
            "extra": extra_grants[:_CAP],
        },
    )

    exact_bad = []
    for name, expected in API_FUNCTION_GRANTS.items():
        grantees = sorted(
            g
            for kind, n, g, p in acl
            if kind == "FUNCTION" and n.startswith(f"{name}(") and p == "EXECUTE"
        )
        if grantees != sorted(expected):
            exact_bad.append(f"{name}:{grantees}")
    checks["api_function_grants_exact"] = _check(not exact_bad, exact_bad)
    diag_tables = [
        n
        for n, k in targets.relations.values()
        if k == "TABLE" and n.startswith("bond_default_diagnostic_")
    ]
    table_bad = [
        f"{n}:{g}"
        for kind, n, g, _p in acl
        if kind == "TABLE"
        and n in diag_tables
        and g not in ("bond_credit_writer", "bond_credit_auditor")
    ] + [
        f"{n}:{g}:{p}"
        for kind, n, g, p in acl
        if kind == "TABLE" and n in diag_tables and p != "SELECT"
    ]
    checks["diagnostic_tables_select_only"] = _check(
        len(diag_tables) == 4 and not table_bad, table_bad
    )

    matrix_bad: list[str] = []
    for role in (runtime_role, *checked_roles):
        if role not in roles:
            if role == runtime_role:
                matrix_bad.append(f"{role}:absent")
            continue
        for oid, (name, kind) in targets.relations.items():
            privileges = (
                (
                    "SELECT",
                    "INSERT",
                    "UPDATE",
                    "DELETE",
                    "TRUNCATE",
                    "REFERENCES",
                    "TRIGGER",
                )
                if kind == "TABLE"
                else ("USAGE", "SELECT", "UPDATE")
            )
            for privilege in privileges:
                function = (
                    "has_table_privilege"
                    if kind == "TABLE"
                    else "has_sequence_privilege"
                )
                if conn.execute(
                    f"SELECT pg_catalog.{function}(%s, %s::oid, %s)",
                    [role, oid, privilege],
                ).fetchone()[0]:
                    matrix_bad.append(f"{role}:{name}:{privilege}")
            if (
                kind == "TABLE"
                and conn.execute(
                    "SELECT pg_catalog.has_any_column_privilege(%s, %s::oid, 'SELECT,INSERT,UPDATE,REFERENCES')",
                    [role, oid],
                ).fetchone()[0]
            ):
                matrix_bad.append(f"{role}:{name}:column")
        for oid, (display, name, _args) in targets.functions.items():
            allowed = role == runtime_role and name == RUNTIME_READER_FUNCTION
            has = conn.execute(
                "SELECT pg_catalog.has_function_privilege(%s, %s::oid, 'EXECUTE')",
                [role, oid],
            ).fetchone()[0]
            if bool(has) != allowed:
                matrix_bad.append(f"{role}:{display}:EXECUTE={has}")
    checks["runtime_privilege_matrix"] = _check(not matrix_bad, matrix_bad[:_CAP])

    definer_bad: list[str] = []
    present_api: set[str] = set()
    diag_names = {f for f in manifest.functions if f.startswith("bond_default_")}
    if targets.functions:
        for name, secdef in conn.execute(
            "SELECT p.proname::text, p.prosecdef FROM pg_catalog.pg_proc p WHERE p.oid = ANY(%s::oid[])",
            [list(targets.functions)],
        ).fetchall():
            if name not in diag_names:
                continue
            present_api.update({name} & set(API_FUNCTION_GRANTS))
            if bool(secdef) != (name in API_FUNCTION_GRANTS):
                definer_bad.append(f"{name}:security_definer={secdef}")
    checks["definer_set_exact"] = _check(
        present_api == set(API_FUNCTION_GRANTS) and not definer_bad, definer_bad[:_CAP]
    )
    path_bad = _search_path_offenders(conn, targets)
    checks["search_path_pinned_all_functions"] = _check(not path_bad, path_bad[:_CAP])

    pins_detail: Any = "not_executable"
    pins_ok = False
    pins_oid = next(
        (
            oid
            for oid, (_d, n, _a) in targets.functions.items()
            if n == "bond_credit_expected_pins"
        ),
        None,
    )
    if (
        pins_oid is not None
        and conn.execute(
            "SELECT pg_catalog.has_function_privilege(current_user, %s::oid, 'EXECUTE')",
            [pins_oid],
        ).fetchone()[0]
    ):
        from psycopg import sql

        row = conn.execute(
            sql.SQL(
                "SELECT contract_version, policy_digest, contract_digest, family_rule_version, rating_resolver_id "
                "FROM {}.bond_credit_expected_pins()"
            ).format(sql.Identifier(schema))
        ).fetchone()
        pins_ok = tuple(row) == (
            c.CONTRACT_VERSION,
            c.POLICY_DIGEST,
            c.SCHEMA_DIGEST,
            c.FAMILY_RULE_VERSION,
            c.RATING_RESOLVER_ID,
        )
        pins_detail = "match" if pins_ok else "mismatch"
    checks["contract_pins_match"] = _check(pins_ok, pins_detail)
    checks["diagnostic_sql_digest_pinned"] = _check(
        diagnostic_sql_digest() == DIAGNOSTIC_SQL_DIGEST,
        "pinned" if diagnostic_sql_digest() == DIAGNOSTIC_SQL_DIGEST else "drift",
    )
    marker: Any = "unreadable"
    if conn.execute(
        "SELECT CASE WHEN pg_catalog.to_regclass(%s) IS NULL THEN NULL "
        "ELSE pg_catalog.has_table_privilege(current_user, pg_catalog.to_regclass(%s), 'SELECT') END",
        [f"{schema}.bond_default_diagnostic_installations"] * 2,
    ).fetchone()[0]:
        marker = conn.execute(
            "SELECT sql_digest FROM public.bond_default_diagnostic_installations "
            "ORDER BY installed_at DESC, installation_id DESC LIMIT 1"
        ).fetchone()
        marker = marker[0] if marker else "none"
    checks["installation_marker_current"] = _check(
        marker == DIAGNOSTIC_SQL_DIGEST,
        {"recorded": marker, "pinned": DIAGNOSTIC_SQL_DIGEST},
    )
    if require_empty_pointers:
        from psycopg import sql

        counts: dict[str, Any] = {}
        for table in ("bond_credit_current_pointer", "bond_default_diagnostic_pointer"):
            readable = conn.execute(
                "SELECT CASE WHEN pg_catalog.to_regclass(%s) IS NULL THEN NULL "
                "ELSE pg_catalog.has_table_privilege(current_user, pg_catalog.to_regclass(%s), 'SELECT') END",
                [f"{schema}.{table}"] * 2,
            ).fetchone()[0]
            if readable is None:
                counts[table] = "missing"
            elif readable:
                counts[table] = conn.execute(
                    sql.SQL("SELECT count(*) FROM {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                ).fetchone()[0]
            else:
                counts[table] = "unreadable"
        checks["pointers_empty"] = _check(all(v == 0 for v in counts.values()), counts)
    evidence: dict[str, str] = {}
    for oid, (display, name, _args) in sorted(
        targets.functions.items(), key=lambda kv: kv[1][0]
    ):
        if name in diag_names:
            definition = conn.execute(
                "SELECT pg_catalog.pg_get_functiondef(%s::oid)", [oid]
            ).fetchone()[0]
            evidence[display] = c.sha256_hex(definition.encode("utf-8"))
    report = {
        "ok": all(v["ok"] for v in checks.values()),
        "schema": schema,
        "checked_roles": [runtime_role, *checked_roles],
        "checks": checks,
        "evidence": {"function_definition_sha256": evidence},
    }
    if not report["ok"]:
        error = _fail(
            "privileges_unverified",
            ",".join(k for k, v in checks.items() if not v["ok"]),
        )
        error.report = report
        raise error
    return report


__all__ = [
    "API_FUNCTION_GRANTS",
    "BASE_LIMITATIONS",
    "COVERAGE_CELLS",
    "DIAGNOSTIC_SQL_DIGEST",
    "DISPLAY_VERSION",
    "INSTALLED_SQL_PATHS",
    "INTENDED_ROLES",
    "LIMITATION_CODES",
    "PRODUCT",
    "AcceptedEventDisplay",
    "CoverageCellDisplay",
    "DiagnosticCounts",
    "DiagnosticError",
    "DiagnosticProjection",
    "InstalledManifest",
    "SourceFrontier",
    "coverage_rationale",
    "derive_projection",
    "derive_projection_from_bundle",
    "diagnostic_sql_digest",
    "frontier_manifest_digest",
    "frontier_record",
    "frontier_records_of",
    "harden_installed_privileges",
    "install_diagnostic_schema",
    "installed_manifest",
    "parse_rationale",
    "prepare_diagnostic",
    "promote_diagnostic",
    "read_current_diagnostic",
    "release_id_for",
    "release_identity",
    "revoke_diagnostic",
    "verify_diagnostic",
    "verify_diagnostic_report",
    "verify_installed_privileges",
]
