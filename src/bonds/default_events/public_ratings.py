"""Full-grid public rating resolution for bond credit evidence (W2b).

``build_full_grid_ratings`` emits one W0 :class:`~.contracts.RatingGridRow` for every
``(cusip, month, view_kind)`` of the supplied historical panel grid, whatever the issue's
eligibility, so :func:`publication.check_bundle` can bind the ratings frame to the pinned
grid. Everything here is pure and deterministic: no database, network, clock or
environment reads; the knowledge cutoff ``K`` is passed in, and the output does not
depend on input order.

Rules (implementation plan section 4.3, policy ``rating_policy``):

* Inputs are indexed by identity first: identical duplicates collapse, while one ID
  with two different bodies is a hard error (``input_id_collision:<frame>``).
* Only ``agency_action`` rows of rights-approved agency packages
  (:func:`~.contracts.is_approved_rating_package`) enter revision processing. A revision
  counts only when it is itself knowable at ``K`` (``current_run``: public and ingested by
  ``K``; ``historical_reconstruction``: proven public by ``K``); two counted revisions of
  one record are a hard fork error. Usable actions are then restricted to the declared
  long-term global instrument scope (:class:`RatingScope`). Issuer-level SD/RD/D never
  rates an instrument.
* An action naming a CUSIP binds that CUSIP. A CUSIP-less action binds through the
  selected revision (as of ``K``) of an admitted ``issue``-scope link that is valid at the
  month-end snapshot. ``effective_audit`` needs that link known by ``K``; ``public_pit``
  needs it known before the next month boundary, otherwise the key is ``pit_unverified``.
* ``public_pit``: the latest action effective by month-end must be proven public before
  the next month boundary (UTC); present-day ``K`` never backdates availability.
  ``effective_audit``: effective dates known by ``K`` (retrospective; never an allocation
  or start-rating input).
* A rating or withdrawal holds only while one relied package's verified coverage for the
  view spans from the action to the month end; otherwise the row is ``stale``.
* Withdrawals (RAC ``WD``/``WE``/``WO``/``WR``, symbols ``WD``/``WR``/``NR``) give
  ``withdrawn``; RAC ``WD`` is never bucket ``D``. Bucket ``D`` only from an
  instrument-level ``D`` symbol; Moody's ``C``/``Ca``/``Caa`` map to ``CCC``.
* Same-day actions of one agency whose raw semantics (agency, type, scale, symbol, action
  class) differ have no winner, even when they share a bucket; unmappable symbols and a
  multi-agency bucket disagreement also keep the key unrated. Each is a typed issue.
* State precedence: ``pit_unverified`` > ``stale`` > ``missing`` (typed problem) > agreed
  ``withdrawn``/rated state. Problems are reported whatever state wins.
* Agency material that exists but is not rights-cleared is declared as
  :class:`UnclearedRatingSource` (never read) and turns otherwise evidence-free keys
  into ``rights_unverified``; without any agency evidence the state is ``missing``.
* The default overlay references an accepted episode known by ``K`` whose onset upper
  bound is on or before the key's month end and that is not resolved by it (resolution
  known by ``K``); it is the same in both views and never changes the agency bucket.

Bundle v2: every row with relied actions persists the selected ``binding_link_ids``
(sorted; ``public_pit`` only links known before the boundary), its bucket comes from W0
``classify_rating_action`` and its ``action_input_digest`` from W0
``rating_action_input_digest``, which ``check_bundle`` and SQL recompute.
:func:`rating_declarations_record` is the canonical form of the local declarations.

Resolution is one sorted sweep per ``(cusip, agency)`` over that CUSIP's grid months:
``O((months + bindings) log bindings)`` rather than a rescan of every action per month.
"""

from __future__ import annotations

import datetime as dt
import heapq
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable, Hashable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from . import contracts as c
from .contracts import (
    CreditObservation,
    DefaultEpisode,
    EventLink,
    RatingGridRow,
    SourcePackage,
)

__all__ = [
    "DECLARATIONS_VERSION",
    "INPUT_MANIFEST_VERSION",
    "KNOWLEDGE_MODES",
    "RESOLVER_ID",
    "RatingIssue",
    "RatingResolution",
    "RatingResolveError",
    "RatingScope",
    "UnclearedRatingSource",
    "action_semantics",
    "build_full_grid_ratings",
    "classify_agency_action",
    "rating_declarations_digest",
    "rating_declarations_from_record",
    "rating_declarations_record",
    "rating_input_manifest_digest",
    "rating_input_manifest_record",
]

#: Identity of these resolution rules (W0 pin); bound into every ``action_input_digest``.
RESOLVER_ID = c.RATING_RESOLVER_ID
#: Version tag of :func:`rating_declarations_record`.
DECLARATIONS_VERSION = c.RATING_DECLARATIONS_VERSION
INPUT_MANIFEST_VERSION = c.RATING_INPUT_MANIFEST_VERSION
KNOWLEDGE_MODES = tuple(c.ENUMS["knowledge_mode"])


class RatingResolveError(ValueError):
    """Fail-loud rating resolution error; ``issues`` carries every typed reason found."""

    def __init__(self, code: str, issues: Iterable[object] = ()) -> None:
        self.code = code
        self.issues = tuple(issues)
        detail = "; ".join(str(i) for i in self.issues[:20])
        super().__init__(f"{code}: {detail}" if detail else code)


_RATED = "rated"
_WITHDRAWN = "withdrawn"
_UNMAPPED = "unmapped"
_CLEAN_STATES = frozenset({"observed", "carried_verified", "withdrawn"})
#: ASCII-only trimming/case folding, the normalization of W0 ``classify_rating_action``.
_ASCII_WS = " \t\n\r\f\v"
_ASCII_UPPER = str.maketrans("abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def classify_agency_action(o: CreditObservation) -> tuple[str, str | None]:
    """``(kind, bucket)`` of an agency action: ``rated``/``withdrawn``/``unmapped``.

    Exactly W0 :func:`~.contracts.classify_rating_action` (the rule ``check_bundle`` and SQL
    recompute): a withdrawal RAC (``WD``/``WE``/``WO``/``WR``) or withdrawal symbol
    (``WD``/``WR``/``NR``) wins, so RAC ``WD`` never becomes ``D``; only exact long-term
    global symbols map (Moody's ``C``/``Ca``/``Caa`` -> ``CCC``); issuer ``SD``/``RD``,
    watch/provisional markers and most short-term symbols are ``unmapped``. Short-term
    ``B``/``C`` look like long-term symbols, so rating type/scale are first filtered by a
    declared :class:`RatingScope`.
    """
    return c.classify_rating_action(o)


def action_semantics(o: CreditObservation) -> tuple[str, str | None, str | None, str, str]:
    """Governed raw meaning of an agency action: agency, type, scale, symbol, action class.

    Two unsequenced same-day actions conflict when these differ, even if they map to one
    bucket (``BB+`` vs ``BB-``, affirmation vs upgrade). Type and scale keep their exact raw
    values (as :class:`RatingScope` matches them); symbol and action class use the ASCII
    normalization of :func:`classify_agency_action`.
    """
    return (
        o.agency_name or "",
        o.agency_rating_type,
        o.agency_scale,
        (o.agency_rating_symbol or "").strip(_ASCII_WS),
        (o.agency_action_classification or "").strip(_ASCII_WS).translate(_ASCII_UPPER),
    )


# ---------------------------------------------------------------------------
# Public input/result types
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RatingScope:
    """Declared raw vocabulary of long-term, global-scale instrument ratings.

    Exact raw ``(agency_name, agency_rating_type, agency_scale)`` values of approved
    agency observations that denote such ratings (declared by the governed input
    manifest). Undeclared type/scale values (short-term, national scale, ...) are
    excluded, so a short-term ``B`` or ``C`` can never be read as a long-term bucket.
    """

    agency_name: str
    rating_type: str | None
    scale: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.agency_name, str) or not self.agency_name.strip():
            raise RatingResolveError("rating_scope:agency_name_required")
        for value in (self.rating_type, self.scale):
            if value is not None and not isinstance(value, str):
                raise RatingResolveError("rating_scope:raw_values_must_be_text", (repr(value),))

    def to_record(self) -> dict[str, str | None]:
        """Canonical declaration record (exact raw values, explicit nulls)."""
        return {"agency_name": self.agency_name, "rating_type": self.rating_type, "scale": self.scale}


@dataclass(frozen=True)
class UnclearedRatingSource:
    """Agency rating material known to exist but not rights-cleared; never read.

    ``coverage_start``/``coverage_end`` (dates, both or neither; neither = unbounded)
    bound the months it may speak to. Keys it covers that have no approved evidence
    become ``rights_unverified`` instead of ``missing``.
    """

    source_ref: str
    rights_state: str
    coverage_start: dt.date | None = None
    coverage_end: dt.date | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.source_ref, str) or not self.source_ref.strip():
            raise RatingResolveError("uncleared_source:source_ref_required")
        if self.rights_state not in c.ENUMS["rights_state"] or self.rights_state == "approved":
            raise RatingResolveError(
                "uncleared_source:rights_state_must_be_known_and_not_approved", (self.source_ref,)
            )
        for bound in (self.coverage_start, self.coverage_end):
            if bound is not None and (isinstance(bound, dt.datetime) or not isinstance(bound, dt.date)):
                raise RatingResolveError("uncleared_source:coverage_dates_expected", (self.source_ref,))
        if (self.coverage_start is None) != (self.coverage_end is None):
            raise RatingResolveError("uncleared_source:coverage_both_or_neither", (self.source_ref,))
        if self.coverage_start is not None and self.coverage_end is not None and (
            self.coverage_start > self.coverage_end
        ):
            raise RatingResolveError("uncleared_source:coverage_not_increasing", (self.source_ref,))

    def covers(self, month: dt.date) -> bool:
        if self.coverage_start is None or self.coverage_end is None:
            return True
        return self.coverage_start <= c.month_end(month) and month <= self.coverage_end

    def to_record(self) -> dict[str, str | None]:
        """Canonical declaration record (ISO dates, explicit nulls)."""
        return {
            "source_ref": self.source_ref,
            "rights_state": self.rights_state,
            "coverage_start": None if self.coverage_start is None else self.coverage_start.isoformat(),
            "coverage_end": None if self.coverage_end is None else self.coverage_end.isoformat(),
        }


def rating_declarations_record(
    rating_scopes: Iterable[RatingScope], uncleared_sources: Iterable[UnclearedRatingSource],
) -> dict[str, Any]:
    """Deterministic canonical form of the integrator-local rating declarations.

    Both complete lists, each sorted by canonical JSON bytes; identical duplicates collapse,
    conflicting duplicates (same ``source_ref``, different body) raise
    ``input_id_collision:uncleared_sources`` exactly as :func:`build_full_grid_ratings`
    does, and empty lists stay explicit. Independent of input order. The order-4 integrator
    embeds this object in ``rating_input_manifest_v2`` (plan amendment 1, section 4.9).
    """
    scopes = _index("rating_scopes", rating_scopes, RatingScope,
                    lambda s: (s.agency_name, s.rating_type, s.scale), lambda s: s)
    uncleared = _index("uncleared_sources", uncleared_sources, UnclearedRatingSource,
                       lambda s: s.source_ref, lambda s: s)

    def ordered(records: Iterable[dict[str, str | None]]) -> list[dict[str, str | None]]:
        return sorted(records, key=c.canonical_json_bytes)

    return c.normalize_rating_declarations_record({
        "version": DECLARATIONS_VERSION,
        "rating_scopes": ordered(s.to_record() for s in scopes.values()),
        "uncleared_rating_sources": ordered(s.to_record() for s in uncleared.values()),
    })


def rating_declarations_digest(
    rating_scopes: Iterable[RatingScope], uncleared_sources: Iterable[UnclearedRatingSource],
) -> str:
    """W0 ``digest_of`` of :func:`rating_declarations_record`."""
    return c.digest_of(rating_declarations_record(rating_scopes, uncleared_sources))


def rating_declarations_from_record(
    record: Mapping[str, Any],
) -> tuple[tuple[RatingScope, ...], tuple[UnclearedRatingSource, ...]]:
    """Decode the exact canonical declaration record; non-canonical bytes fail closed."""
    try:
        normalized = c.normalize_rating_declarations_record(record)
    except c.ContractError as exc:
        raise RatingResolveError("rating_declarations_invalid", (str(exc),)) from exc
    scopes = tuple(RatingScope(**item) for item in normalized["rating_scopes"])
    uncleared = tuple(
        UnclearedRatingSource(
            source_ref=item["source_ref"],
            rights_state=item["rights_state"],
            coverage_start=None if item["coverage_start"] is None else dt.date.fromisoformat(item["coverage_start"]),
            coverage_end=None if item["coverage_end"] is None else dt.date.fromisoformat(item["coverage_end"]),
        )
        for item in normalized["uncleared_rating_sources"]
    )
    return scopes, uncleared


def rating_input_manifest_record(
    rating_scopes: Iterable[RatingScope],
    uncleared_sources: Iterable[UnclearedRatingSource],
    packages: Iterable[SourcePackage],
) -> dict[str, Any]:
    """Versioned governed rating-input manifest used by W0 publication validation."""
    declarations = rating_declarations_record(rating_scopes, uncleared_sources)
    return c.rating_input_manifest_record(declarations, packages)


def rating_input_manifest_digest(
    rating_scopes: Iterable[RatingScope],
    uncleared_sources: Iterable[UnclearedRatingSource],
    packages: Iterable[SourcePackage],
) -> str:
    """Canonical digest of :func:`rating_input_manifest_record`."""
    return c.digest_of(rating_input_manifest_record(rating_scopes, uncleared_sources, packages))


@dataclass(frozen=True)
class RatingIssue:
    """Typed reason a key stayed unrated despite evidence, or an input defect."""

    reason: str
    cusip_id: str | None = None
    month: dt.date | None = None
    view_kind: str | None = None
    observation_ids: tuple[uuid.UUID, ...] = ()
    detail: str = ""

    def sort_key(self) -> tuple[str, ...]:
        return (
            self.cusip_id or "",
            self.month.isoformat() if self.month else "",
            self.view_kind or "",
            self.reason,
            self.detail,
            ",".join(str(x) for x in self.observation_ids),
        )

    def __str__(self) -> str:
        where = f"{self.cusip_id or '-'}|{self.month.isoformat() if self.month else '-'}|{self.view_kind or '-'}"
        return f"{self.reason}:{where}" + (f":{self.detail}" if self.detail else "")


@dataclass(frozen=True)
class RatingResolution:
    #: exactly one row per (grid key, view), sorted by row key.
    rows: tuple[RatingGridRow, ...]
    issues: tuple[RatingIssue, ...]
    stats: Mapping[str, int] = field(default_factory=dict)

    def by_key(self) -> dict[tuple[str, dt.date, str], RatingGridRow]:
        return {(r.cusip_id, r.month, r.view_kind): r for r in self.rows}


# ---------------------------------------------------------------------------
# Internal evidence model
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class _Action:
    observation: CreditObservation
    package: SourcePackage
    agency: str
    semantics: tuple[str | None, ...]
    kind: str
    bucket: str | None
    action_date: dt.date
    public_at: dt.datetime

    @property
    def observation_id(self) -> uuid.UUID:
        return self.observation.observation_id

    @property
    def order(self) -> tuple[dt.date, str]:
        return self.action_date, str(self.observation.observation_id)


@dataclass(frozen=True)
class _Item:
    """One binding of an action to a CUSIP, active on grid months ``[start, stop]``.

    ``link`` is ``None`` when the action names the CUSIP itself.
    """

    action: _Action
    link: EventLink | None
    start: dt.date
    stop: dt.date | None

    @property
    def ident(self) -> tuple[str, str]:
        return str(self.action.observation_id), str(self.link.link_id) if self.link else ""


@dataclass(frozen=True)
class _RowHash:
    """Stand-in exposing a precomputed ``row_sha256`` to W0 ``rating_action_input_digest``
    (which reads nothing else), so each input row is hashed once per resolution."""

    sha: str

    def row_sha256(self) -> str:
        return self.sha


_ROW_ID_ATTR: dict[type, str] = {
    CreditObservation: "observation_id", SourcePackage: "package_id", EventLink: "link_id",
}


#: An action with the binding links active at the month end (``None`` = names the CUSIP).
_Bound = tuple[_Action, "tuple[EventLink, ...] | None"]
_Problem = tuple[str, "tuple[_Bound, ...]"]


@dataclass(frozen=True)
class _Outcome:
    state: str
    bucket: str | None = None
    relied: tuple[_Bound, ...] = ()
    #: ``(reason, bound actions)`` that kept (or would keep) the key unrated.
    problems: tuple[_Problem, ...] = ()


def _utc(value: dt.datetime) -> dt.datetime:
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise RatingResolveError("knowledge_cutoff:timezone_required")
    return value.astimezone(dt.timezone.utc)


# ---------------------------------------------------------------------------
# Input indexing and revision selection
# ---------------------------------------------------------------------------
def _index(
    frame: str,
    items: Iterable[Any],
    kind: type,
    key: Callable[[Any], Hashable],
    body: Callable[[Any], Hashable],
) -> dict[Any, Any]:
    """Index ``items`` by identity; identical duplicates collapse, divergent bodies fail.

    Raised regardless of ``strict`` and independently of input order: every colliding key
    is listed, sorted.
    """
    if isinstance(items, (str, bytes)):
        raise RatingResolveError(f"input_type_invalid:{frame}", (type(items).__name__,))
    first: dict[Any, Any] = {}
    repeated: dict[Any, list[Any]] = defaultdict(list)
    wrong: set[str] = set()
    for item in items:
        if not isinstance(item, kind):
            wrong.add(type(item).__name__)
            continue
        ident = key(item)
        if ident in first:
            repeated[ident].append(item)
        else:
            first[ident] = item
    if wrong:
        raise RatingResolveError(f"input_type_invalid:{frame}", sorted(wrong))
    # Bodies are compared only for repeated identities (cheap in the common case).
    collisions = sorted(
        str(ident) for ident, more in repeated.items()
        if len({body(first[ident]), *(body(x) for x in more)}) > 1
    )
    if collisions:
        raise RatingResolveError(f"input_id_collision:{frame}", collisions)
    return first


def _row_body(row: Any) -> str:
    return row.row_sha256()  # type: ignore[no-any-return]


def _require_acyclic(rows: Mapping[uuid.UUID, Any], parent_of: Callable[[Any], uuid.UUID | None], frame: str) -> None:
    done: set[uuid.UUID] = set()
    for start in sorted(rows, key=str):
        path: list[uuid.UUID] = []
        on_path: set[uuid.UUID] = set()
        node: uuid.UUID | None = start
        while node is not None and node in rows and node not in done:
            if node in on_path:
                raise RatingResolveError(f"{frame}_supersession_cycle", (str(node),))
            on_path.add(node)
            path.append(node)
            node = parent_of(rows[node])
        done.update(path)


def _superseded(
    rows: Mapping[uuid.UUID, Any],
    parent_of: Callable[[Any], uuid.UUID | None],
    admissible: Iterable[uuid.UUID],
    frame: str,
) -> set[uuid.UUID]:
    """Records superseded by an admissible revision; two admissible revisions fork."""
    children: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    for rid in admissible:
        parent = parent_of(rows[rid])
        if parent is not None:
            children[parent].append(rid)
    forks = sorted(str(parent) for parent, kids in children.items() if len(kids) > 1)
    if forks:
        raise RatingResolveError(f"{frame}_revision_fork", forks)
    return set(children)


def _knowledge_reason(o: CreditObservation, k: dt.datetime, mode: str) -> str | None:
    """``None`` when ``o`` is knowable at ``k`` under ``mode`` (evidence or revision)."""
    if o.public_available_at > k:
        return "observation_public_after_cutoff"
    if mode == "current_run" and o.first_seen_at > k:
        return "observation_not_ingested_by_cutoff"
    return None


def _grid(panel_grid: Iterable[tuple[str, dt.date]]) -> dict[str, list[dt.date]]:
    """Grid months per CUSIP, both sorted (row order equals W0 ``RatingGridRow.key()`` order)."""
    keys: dict[str, set[dt.date]] = defaultdict(set)
    for item in panel_grid:
        try:
            cusip, month = item
        except (TypeError, ValueError) as exc:
            raise RatingResolveError("panel_grid:pair_expected", (repr(item),)) from exc
        if isinstance(month, dt.datetime) or not isinstance(month, dt.date) or month.day != 1:
            raise RatingResolveError("panel_grid:month_key_expected", (repr(month),))
        if cusip not in keys and not (isinstance(cusip, str) and c.is_valid_cusip9(cusip)):
            raise RatingResolveError("panel_grid:invalid_cusip9", (repr(cusip),))
        keys[cusip].add(month)
    return {cusip: sorted(months) for cusip, months in sorted(keys.items())}


def _views(views: Iterable[str]) -> list[str]:
    if isinstance(views, str):
        raise RatingResolveError("views:iterable_of_view_kinds_expected")
    chosen = sorted(set(views))
    if not chosen or not set(chosen) <= set(c.ENUMS["view_kind"]):
        raise RatingResolveError("views:invalid", tuple(chosen))
    return chosen


def _link_months(action_date: dt.date, link: EventLink) -> tuple[dt.date, dt.date | None] | None:
    """Grid months whose month-end snapshot the link binds and the action is effective by.

    Month ``m`` qualifies when ``valid_from <= end(m) <= valid_to`` and
    ``action_date <= end(m)``.
    """
    start = c.month_key(max(action_date, link.valid_from))
    if link.valid_to is None:
        return start, None
    last = c.month_key(link.valid_to)
    stop = last if link.valid_to == c.month_end(last) else c.add_months(last, -1)
    return (start, stop) if stop >= start else None


# ---------------------------------------------------------------------------
# Sweep and per-key evaluation
# ---------------------------------------------------------------------------
def _sweep(items: Sequence[_Item], months: Sequence[dt.date], steps: Counter[str]) -> Iterator[tuple[_Bound, ...] | None]:
    """Latest effective same-day group bound at each month end, in ``months`` order."""
    ordered = sorted(items, key=lambda it: (it.start, it.ident))
    active: dict[dt.date, dict[tuple[str, str], _Item]] = {}
    groups: dict[dt.date, tuple[_Bound, ...]] = {}
    dates: list[int] = []
    expiring: list[tuple[dt.date, tuple[str, str], _Item]] = []
    i = 0
    for month in months:
        while i < len(ordered) and ordered[i].start <= month:
            item = ordered[i]
            i += 1
            day = item.action.action_date
            members = active.setdefault(day, {})
            if not members:
                heapq.heappush(dates, -day.toordinal())
            members[item.ident] = item
            groups.pop(day, None)
            if item.stop is not None:
                heapq.heappush(expiring, (item.stop, item.ident, item))
            steps["sweep_steps"] += 1
        while expiring and expiring[0][0] < month:
            _stop, ident, item = heapq.heappop(expiring)
            day = item.action.action_date
            del active[day][ident]
            groups.pop(day, None)
            steps["sweep_steps"] += 1
        while dates and not active.get(dt.date.fromordinal(-dates[0])):
            heapq.heappop(dates)
            steps["sweep_steps"] += 1
        steps["sweep_steps"] += 1
        if not dates:
            yield None
            continue
        latest = dt.date.fromordinal(-dates[0])
        group = groups.get(latest)
        if group is None:
            group = groups[latest] = _group(active[latest].values())
        yield group


def _group(items: Iterable[_Item]) -> tuple[_Bound, ...]:
    by_action: dict[uuid.UUID, tuple[_Action, list[EventLink] | None]] = {}
    for item in items:
        entry = by_action.setdefault(item.action.observation_id, (item.action, None if item.link is None else []))
        if item.link is not None and entry[1] is not None:
            entry[1].append(item.link)
    return tuple(
        (action, None if links is None else tuple(sorted(links, key=lambda x: str(x.link_id))))
        for action, links in sorted(by_action.values(), key=lambda pair: pair[0].order)
    )


def _anchor(action: _Action, view: str) -> dt.date:
    if view == "public_pit":
        return action.public_at.astimezone(dt.timezone.utc).date()
    return action.action_date


def _coverage_verified(relied: Sequence[_Bound], view: str, end: dt.date) -> bool:
    """One relied package's verified coverage spans from the action to the month end.

    Every relied package must declare its coverage for the view (``check_bundle`` states
    the row frontier from all of them).
    """
    spans = [c.rating_coverage(a.package, view) for a, _links in relied]
    if any(start is None or frontier is None for start, frontier in spans):
        return False
    return any(
        start <= _anchor(a, view) and frontier >= end  # type: ignore[operator]
        for (a, _links), (start, frontier) in zip(relied, spans)
    )


def _evaluate(group: tuple[_Bound, ...], month: dt.date, end: dt.date, boundary: dt.datetime, view: str) -> _Outcome:
    """State of one agency's latest same-day group for a key.

    Precedence: ``pit_unverified`` > ``stale`` > ``missing`` (problem) > clean state; the
    problems of the group are reported in every case.
    """
    problems: tuple[_Problem, ...] = ()
    if len({a.semantics for a, _links in group}) > 1:
        problems = (("same_day_conflict", group),)
    elif group[0][0].kind == _UNMAPPED:
        problems = (("rating_symbol_unmapped", group),)
    relied = group
    if view == "public_pit":
        known: list[_Bound] = []
        late: set[tuple[str | None, ...]] = set()
        for action, links in group:
            pit_links = None if links is None else tuple(x for x in links if x.link_known_at < boundary)
            if action.public_at < boundary and (pit_links is None or pit_links):
                known.append((action, pit_links))
            else:
                late.add(action.semantics)
        if not known or not late <= {a.semantics for a, _links in known}:
            return _Outcome("pit_unverified", problems=problems)
        relied = tuple(known)
    if not _coverage_verified(relied, view, end):
        return _Outcome("stale", relied=relied, problems=problems)
    if problems:
        return _Outcome("missing", problems=problems)
    action = relied[0][0]
    if action.kind == _WITHDRAWN:
        return _Outcome("withdrawn", relied=relied)
    state = "observed" if c.month_key(action.action_date) == month else "carried_verified"
    return _Outcome(state, bucket=action.bucket, relied=relied)


def _combine(outcomes: Sequence[_Outcome], month: dt.date) -> _Outcome:
    """Across agencies; no composite rule is authorized, so only agreement rates a key.

    Every agency's problems accumulate independently; a bucket/withdrawal disagreement
    among agencies with a clean state is one more problem. State precedence is the same
    as :func:`_evaluate`.
    """
    if len(outcomes) == 1:
        return outcomes[0]  # already resolved with the same precedence by _evaluate
    problems = tuple(p for o in outcomes for p in o.problems)
    clean = [o for o in outcomes if o.state in _CLEAN_STATES]
    if len({(o.state == "withdrawn", o.bucket) for o in clean}) > 1:
        problems += (("multi_agency_composite_undefined", tuple(b for o in clean for b in o.relied)),)
    if any(o.state == "pit_unverified" for o in outcomes):
        return _Outcome("pit_unverified", problems=problems)
    stale = [o for o in outcomes if o.state == "stale"]
    if stale:
        return _Outcome("stale", relied=tuple(b for o in stale for b in o.relied), problems=problems)
    if problems:
        return _Outcome("missing", problems=problems)
    relied = tuple(b for o in clean for b in o.relied)
    if clean[0].state == "withdrawn":
        return _Outcome("withdrawn", relied=relied)
    latest = max(a.action_date for a, _links in relied)
    state = "observed" if c.month_key(latest) == month else "carried_verified"
    return _Outcome(state, bucket=clean[0].bucket, relied=relied)


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------
def build_full_grid_ratings(
    panel_grid: Iterable[tuple[str, dt.date]],
    *,
    knowledge_cutoff: dt.datetime,
    packages: Iterable[SourcePackage] = (),
    observations: Iterable[CreditObservation] = (),
    links: Iterable[EventLink] = (),
    episodes: Iterable[DefaultEpisode] = (),
    rating_scopes: Iterable[RatingScope] = (),
    uncleared_sources: Iterable[UnclearedRatingSource] = (),
    views: Iterable[str] = c.ENUMS["view_kind"],
    knowledge_mode: str = "current_run",
    strict: bool = True,
) -> RatingResolution:
    """Resolve every ``(cusip, month)`` of ``panel_grid`` for each requested view as of ``K``.

    ``panel_grid`` is the complete historical panel grid (excluded issues and the
    boundary start month included; never filtered by eligibility). ``packages`` and
    ``observations`` are the bundle input inventory (non-agency rows are ignored);
    ``links`` bind CUSIP-less instrument actions (admitted ``issue`` scope, selected
    revision as of ``K``); ``episodes`` are the accepted :class:`DefaultEpisode` rows of
    the same bundle (default overlay). The output has exactly one row per grid key and
    view. Input identity collisions, revision forks/cycles and malformed arguments always
    raise :class:`RatingResolveError`; ``strict`` additionally raises on any typed issue,
    otherwise affected keys stay unrated and the issues are returned.
    """
    k = _utc(knowledge_cutoff)
    if knowledge_mode not in KNOWLEDGE_MODES:
        raise RatingResolveError(f"knowledge_mode:invalid:{knowledge_mode}")
    grid = _grid(panel_grid)
    view_list = _views(views)
    pkgs = _index("packages", packages, SourcePackage, lambda p: p.package_id, _row_body)
    obs = _index("observations", observations, CreditObservation, lambda o: o.observation_id, _row_body)
    all_links = _index("links", links, EventLink, lambda x: x.link_id, _row_body)
    all_episodes = _index("episodes", episodes, DefaultEpisode, lambda e: (e.security_id, e.episode_id), _row_body)
    scopes = _index("rating_scopes", rating_scopes, RatingScope,
                    lambda s: (s.agency_name, s.rating_type, s.scale), lambda s: s)
    uncleared = _index("uncleared_sources", uncleared_sources, UnclearedRatingSource,
                       lambda s: s.source_ref, lambda s: s)
    declared_agencies = {agency for agency, _type, _scale in scopes}

    issues: list[RatingIssue] = []
    stats: Counter[str] = Counter()
    uncleared_cusips: set[str] = set()

    # 1. Package eligibility, then revision processing among eligible agency actions.
    eligible: dict[uuid.UUID, CreditObservation] = {}
    for oid in sorted(obs, key=str):
        o = obs[oid]
        if o.observation_kind != "agency_action":
            continue
        package = pkgs.get(o.package_id)
        if package is None or not c.is_approved_rating_package(package):
            reason = "agency_action_package_outside_inventory" if package is None else (
                "agency_action_rights_not_approved"
            )
            issues.append(RatingIssue(reason, cusip_id=o.cusip9, observation_ids=(oid,)))
            if o.cusip9 is not None:
                uncleared_cusips.add(o.cusip9)
            continue
        eligible[oid] = o

    def obs_parent(o: CreditObservation) -> uuid.UUID | None:
        return o.supersedes_observation_id

    _require_acyclic(eligible, obs_parent, "observation")
    unknown = {oid: _knowledge_reason(o, k, knowledge_mode) for oid, o in eligible.items()}
    superseded = _superseded(eligible, obs_parent, (oid for oid, r in unknown.items() if r is None), "observation")

    # 2. Link revision selection (as of K) for links of eligible actions. A revision of such a
    #    link supersedes it whatever observation the revision names (as ``check_bundle`` counts).
    agency_links = {lid: x for lid, x in all_links.items() if x.observation_id in eligible}
    revision_scope = {
        **agency_links,
        **{lid: x for lid, x in all_links.items() if x.supersedes_link_id in agency_links},
    }

    def link_parent(x: EventLink) -> uuid.UUID | None:
        return x.supersedes_link_id

    _require_acyclic(revision_scope, link_parent, "link")
    superseded_links = _superseded(
        revision_scope, link_parent, (lid for lid, x in revision_scope.items() if x.link_known_at <= k), "link",
    )
    known_links = [lid for lid, x in agency_links.items() if x.link_known_at <= k]
    bindings: dict[uuid.UUID, dict[str, list[EventLink]]] = defaultdict(lambda: defaultdict(list))
    for lid in sorted(known_links, key=str):
        x = agency_links[lid]
        if lid in superseded_links:
            stats["links:superseded"] += 1
        elif x.status != "admitted" or x.affected_scope != "issue":
            stats["links:not_admitted_issue_scope"] += 1
        else:
            bindings[x.observation_id][x.cusip9].append(x)
    if len(agency_links) > len(known_links):
        stats["links:known_after_cutoff"] += len(agency_links) - len(known_links)

    # 3. Usable, in-scope instrument actions -> CUSIP bindings.
    items: dict[str, dict[str, list[_Item]]] = defaultdict(lambda: defaultdict(list))
    out_of_scope: Counter[str] = Counter()
    for oid in sorted(eligible, key=str):
        o = eligible[oid]
        reason = unknown[oid] or (
            "observation_superseded" if oid in superseded
            else "observation_retraction" if o.revision_kind == "retraction" else None
        )
        if reason:
            stats[f"excluded:{reason}"] += 1
            continue
        if o.agency_subject_kind != "instrument":
            stats["excluded:issuer_level_action"] += 1
            continue
        agency = o.agency_name or ""
        if (agency, o.agency_rating_type, o.agency_scale) not in scopes:
            out_of_scope[agency] += 1
            stats["excluded:rating_scope"] += 1
            continue
        kind, bucket = classify_agency_action(o)
        action = _Action(
            observation=o, package=pkgs[o.package_id], agency=agency, semantics=action_semantics(o),
            kind=kind, bucket=bucket, action_date=o.agency_action_date,  # type: ignore[arg-type]
            public_at=o.public_available_at,
        )
        if o.cusip9 is not None:
            items[o.cusip9][agency].append(_Item(action, None, c.month_key(action.action_date), None))
        elif oid in bindings:
            for cusip, cusip_links in sorted(bindings[oid].items()):
                for x in cusip_links:
                    span = _link_months(action.action_date, x)
                    if span is not None:
                        items[cusip][agency].append(_Item(action, x, *span))
        else:
            stats["excluded:instrument_action_without_cusip_or_link"] += 1
            continue
        stats["agency_actions:usable"] += 1
    for agency, count in sorted(out_of_scope.items()):
        if agency not in declared_agencies:
            issues.append(RatingIssue("rating_scope_undeclared", detail=f"agency={agency};actions={count}"))

    # 4. Default overlay episodes.
    episodes_by_cusip: dict[str, list[DefaultEpisode]] = defaultdict(list)
    for ekey in sorted(all_episodes, key=str):
        e = all_episodes[ekey]
        if e.evidence_known_at > k:
            issues.append(RatingIssue("episode_known_after_cutoff", cusip_id=e.cusip9, detail=str(e.episode_id)))
            continue
        episodes_by_cusip[e.cusip9].append(e)
    for cusip_episodes in episodes_by_cusip.values():
        cusip_episodes.sort(key=lambda e: (e.onset_upper_inclusive, str(e.episode_id)))

    def overlay(cusip: str, month: dt.date, end: dt.date) -> uuid.UUID | None:
        hits = [
            e for e in episodes_by_cusip.get(cusip, ())
            if e.onset_upper_inclusive <= end and not (
                e.resolution_known_at is not None and e.resolution_known_at <= k
                and e.resolution_date is not None and e.resolution_date <= end
            )
        ]
        if len(hits) > 1:
            issues.append(RatingIssue(
                "overlapping_default_episodes", cusip_id=cusip, month=month,
                detail=",".join(str(e.episode_id) for e in hits),
            ))
        return hits[0].episode_id if hits else None

    # 5. Row provenance (cached per view and relied set). ``binding_link_ids`` are the selected
    #    link revisions that bind the relied actions at the month end (``public_pit``: only
    #    those known before the boundary); the digest is W0 ``rating_action_input_digest``.
    provenance: dict[tuple[str, frozenset[uuid.UUID], frozenset[uuid.UUID]], tuple[Any, ...]] = {}
    row_hashes: dict[tuple[type, uuid.UUID], _RowHash] = {}

    def _hashed(row: CreditObservation | SourcePackage | EventLink) -> Any:
        """Row hash computed once per (deduplicated) input identity for the W0 digest helper."""
        ident = (type(row), getattr(row, _ROW_ID_ATTR[type(row)]))
        proxy = row_hashes.get(ident)
        if proxy is None:
            proxy = row_hashes[ident] = _RowHash(row.row_sha256())
        return proxy

    def provenance_of(view: str, relied: tuple[_Bound, ...]) -> tuple[Any, ...]:
        cache_key = (
            view,
            frozenset(a.observation_id for a, _links in relied),
            frozenset(x.link_id for _a, links in relied for x in (links or ())),
        )
        cached = provenance.get(cache_key)
        if cached is not None:
            return cached
        actions = {a.observation_id: a for a, _links in relied}
        bound_links = {x.link_id: x for _a, links in relied for x in (links or ())}
        relied_packages = {a.package.package_id: a.package for a in actions.values()}
        frontiers = [c.rating_coverage(a.package, view)[1] for a in actions.values()]
        cached = provenance[cache_key] = (
            max(a.action_date for a in actions.values()),
            max([
                *(a.public_at for a in actions.values()),
                *(x.link_known_at for x in bound_links.values()),
            ]),
            c.sorted_uuids(actions),
            c.sorted_uuids(bound_links),
            None if any(f is None for f in frontiers) else max(frontiers),  # type: ignore[type-var]
            c.rating_action_input_digest(
                view,
                [_hashed(a.observation) for a in actions.values()],
                [_hashed(p) for p in relied_packages.values()],
                [_hashed(x) for x in bound_links.values()],
            ),
        )
        return cached

    # 6. One sweep per (cusip, agency) over the CUSIP's grid months.
    uncleared_list = list(uncleared.values())
    all_months = sorted({m for months in grid.values() for m in months})
    ends = {m: c.month_end(m) for m in all_months}
    boundaries = {m: c.next_month_boundary_utc(m) for m in all_months}
    uncleared_months = {m for m in all_months if any(s.covers(m) for s in uncleared_list)}
    row_states: Counter[tuple[str, str]] = Counter()
    rows: list[RatingGridRow] = []
    for cusip, months in grid.items():
        agencies = items.get(cusip, {})
        sweeps = {agency: list(_sweep(agency_items, months, stats)) for agency, agency_items in sorted(agencies.items())}
        for index, month in enumerate(months):
            end = ends[month]
            boundary = boundaries[month]
            default_overlay = overlay(cusip, month, end)
            groups = [found[index] for found in sweeps.values() if found[index] is not None]
            for view in view_list:
                if groups:
                    outcome = _combine([_evaluate(g, month, end, boundary, view) for g in groups], month)  # type: ignore[arg-type]
                else:
                    unverified = cusip in uncleared_cusips or month in uncleared_months
                    outcome = _Outcome("rights_unverified" if unverified else "missing")
                for reason, bound in outcome.problems:
                    issues.append(RatingIssue(
                        reason, cusip_id=cusip, month=month, view_kind=view,
                        observation_ids=c.sorted_uuids(a.observation_id for a, _links in bound),
                        detail="actions=" + ",".join(sorted({
                            "/".join("<null>" if part is None else part for part in a.semantics)
                            for a, _links in bound
                        })),
                    ))
                if outcome.relied:
                    action_date, known_at, source_ids, link_ids, frontier, digest = provenance_of(
                        view, outcome.relied)
                else:
                    action_date = known_at = frontier = digest = None
                    source_ids = link_ids = ()
                rows.append(RatingGridRow(
                    cusip_id=cusip,
                    month=month,
                    view_kind=view,
                    bucket=outcome.bucket if outcome.state in c.RATED_STATES else None,
                    state=outcome.state,
                    action_date=action_date,
                    public_known_at=known_at,
                    agency_source_ids=source_ids,
                    binding_link_ids=link_ids,
                    coverage_frontier=frontier,
                    action_input_digest=digest,
                    default_overlay_episode_id=default_overlay,
                ))
                row_states[(view, outcome.state)] += 1

    # Rows are emitted in (cusip, month, view) order, i.e. already sorted by row key.
    for (view, state), count in row_states.items():
        stats[f"rows:{view}:{state}"] += count
    issues.sort(key=lambda i: i.sort_key())
    stats["grid_keys"] = sum(len(months) for months in grid.values())
    stats["rows"] = len(rows)
    stats["issues"] = len(issues)
    if strict and issues:
        raise RatingResolveError("rating_resolution_failed", issues)
    return RatingResolution(rows=tuple(rows), issues=tuple(issues), stats=dict(sorted(stats.items())))
