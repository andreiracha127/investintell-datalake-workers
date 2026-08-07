"""Security-master materializer: point-in-time bond identity + observed terms.

Two concerns live here:

* A **pure**, DB-free resolver (``resolve_securities``) that folds immutable
  identity/terms observations into deterministic securities.  It reuses the
  pilot's lossless ``normalize_cusip9`` (placeholders and synthetic prefixes are
  rejected upstream, never repaired) and qualifies ISINs with the same
  no-repair discipline.  Conflicting evidence for one identity is surfaced as an
  explicit ``ambiguous`` state with the conflicting values recorded — never an
  arbitrary winner.

* The **publication** wiring that lands one complete ``bond_security_v1``
  snapshot (securities + point-in-time aliases) through the shared
  ``sec_derived_publications`` protocol (prepared -> validated -> current
  pointer), pinned by a product-salted input fingerprint so reruns are
  idempotent and partial builds can never become current.

Identity key (spec §1), documented once here and mirrored in
``schemas/bond_security_v1.sql``:

    security_id = uuid5(NAMESPACE_BOND_SECURITY, identity_key)

where ``identity_key`` is the FIRST qualified identifier in this precedence:

    1. ``"cusip9:" + <lossless normalized CUSIP9>``   (directly observed CUSIP9)
    2. ``"cusip9:" + <CUSIP9 anchored from a US/CA ISIN>``  (positional NSIN,
       chars 3-11, re-qualified through normalize_cusip9 — not a repair)
    3. ``"isin:" + <qualified ISIN>``                 (non-US/CA ISIN, or a US/CA
       ISIN whose embedded NSIN normalize_cusip9 rejects)

Because a US/CA ISIN embeds its CUSIP9, anchoring makes the key ``cusip9:``-based
regardless of which field first carried the identifier.  So the ``security_id``
is stable across the ISIN-only -> CUSIP9 arrival transition, and one CUSIP9-bearing
instrument never splits into two securities within or across snapshots.  (A
security identified only by a non-US/CA ISIN keeps a ``isin:`` key, stable as long
as that ISIN is its identifier — this resolver does not claim to unify an
identity across a change of identifier scheme.)  The key never depends on the
as-of date, source run, or observed terms.  Observations whose only identifier is
a placeholder/synthetic/invalid value carry no fabricated key and are reported as
rejected rather than published.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID, uuid5

import psycopg
from psycopg.types.json import Jsonb

from src.bonds import issuer_consensus
from src.bonds.identifiers import normalize_cusip9
from src.bonds.states import IdentifierState

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_security_v1.sql"
REFERENCE_TERMS_SCHEMA_PATH = ROOT / "schemas" / "bond_reference_terms.sql"
PRODUCT = "bond_security_v1"
METHODOLOGY_VERSION = "bond_security_v1"

# Deterministic namespace for security_id (distinct constant; do not reuse).
NAMESPACE_BOND_SECURITY = UUID("b0d5ec00-0000-5000-a000-626f6e647331")
# Deterministic namespace for the publication identity.
_NAMESPACE_PUBLICATION = UUID("b0d5ec00-0000-5000-a000-7075626c6931")

# Summary term fields projected onto the published row.  Tri-state text fields
# default to 'not_reported' when never observed; the rest default to NULL.  No
# field is ever fabricated — absence is represented honestly.
_TRISTATE_TERMS = ("is_144a", "secured")
_SCALAR_TERMS = (
    "issuer_name",
    "currency",
    "coupon_type",
    "coupon_rate",
    "maturity_date",
    "seniority",
    "day_count",
    "settlement_convention",
)
_SCHEDULE_TERMS = ("coupon_schedule", "call_schedule", "put_schedule")

# ISIN placeholders rejected outright (mirrors the CUSIP placeholder discipline).
_ISIN_PLACEHOLDERS = frozenset(
    {"", "N/A", "NA", "NONE", "NULL", "UNKNOWN", "000000000000", "XXXXXXXXXXXX"}
)


# ---------------------------------------------------------------------------
# Pure resolver
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SecurityObservation:
    """One immutable identity/terms observation as landed in the DB."""

    observation_id: str
    observation_date: date
    cusip9_input: object = None
    isin_input: object = None
    issuer_name: object = None
    currency: object = None
    coupon_type: object = None
    coupon_rate: object = None
    maturity_date: object = None
    seniority: object = None
    secured: object = None
    is_144a: object = None
    day_count: object = None
    settlement_convention: object = None
    coupon_schedule: object = None
    call_schedule: object = None
    put_schedule: object = None
    source_lineage: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SecurityAlias:
    alias_kind: str  # 'cusip9' | 'isin'
    alias_value: str
    valid_from: date
    valid_to: date | None
    source_lineage: dict[str, Any]


@dataclass(frozen=True)
class ResolvedSecurity:
    security_id: UUID
    identity_key: str
    identity_state: str  # 'resolved' | 'ambiguous'
    identity_reason_code: str | None
    measured_terms: dict[str, Any]
    terms: dict[str, Any]
    identity_evidence: dict[str, Any]
    aliases: tuple[SecurityAlias, ...]
    contributing_observation_ids: tuple[str, ...]
    # The qualified CUSIP9 this identity resolves to (None for an ISIN-only
    # identity). Carried so issuer attribution can scope its vote by the exact
    # CUSIP9 and, on fallback, by the 6-character issuer prefix.
    cusip9: str | None = None
    # Reported issuer spellings for THIS security with their holding-lot vote
    # weight: the raw input to ``issuer_consensus``. Never published as-is.
    issuer_votes: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class RejectedObservation:
    observation_id: str
    cusip9_state: str
    isin_state: str
    reason_code: str


@dataclass(frozen=True)
class ResolutionResult:
    securities: tuple[ResolvedSecurity, ...]
    rejected: tuple[RejectedObservation, ...]


def qualify_isin(value: object) -> tuple[str | None, str]:
    """Qualify an ISIN losslessly (trim/upper only); never repair or invent.

    Returns ``(normalized_or_None, state)`` where state is one of
    ``valid_isin`` / ``blank`` / ``placeholder`` / ``invalid_format``.
    """
    if value is None:
        return None, "blank"
    if not isinstance(value, str):
        return None, "invalid_format"
    normalized = value.strip().upper()
    if not normalized:
        return None, "blank"
    if normalized in _ISIN_PLACEHOLDERS:
        return None, "placeholder"
    if (
        len(normalized) != 12
        or not normalized.isascii()
        or not normalized.isalnum()
        or not normalized[:2].isalpha()
        or not normalized[2:].isalnum()
    ):
        return None, "invalid_format"
    return normalized, "valid_isin"


def _anchor_cusip9_from_isin(isin_value: str) -> str | None:
    """CUSIP-anchor a US/CA ISIN: extract the positional NSIN (chars 3-11) and
    pass it through the lossless ``normalize_cusip9`` gate.

    Positional extraction is NOT a repair — a US/CA ISIN embeds its CUSIP9
    literally as its NSIN.  ``normalize_cusip9`` stays the sole gate: if it
    rejects the extracted value (placeholder/synthetic/invalid), no CUSIP9 is
    anchored and the caller keeps ``isin:`` keying.  This makes ``identity_key``
    ``cusip9:``-anchored regardless of which field first carried the identifier,
    so the ``security_id`` is stable across the ISIN-only -> CUSIP9 arrival
    transition (and one instrument never splits into two securities).
    """
    if isin_value[:2] not in ("US", "CA"):
        return None
    anchored = normalize_cusip9(isin_value[2:11])
    if anchored.state is IdentifierState.VALID_CUSIP9 and anchored.normalized_cusip9 is not None:
        return anchored.normalized_cusip9
    return None


def _identity_key(observation: SecurityObservation) -> tuple[str | None, str, str, str | None, str]:
    """Return (identity_key, cusip9_state, isin_state, qualified_cusip9, qualified_isin).

    Precedence: a directly observed valid CUSIP9 wins; otherwise a US/CA ISIN is
    CUSIP-anchored; otherwise a qualified ISIN keys ``isin:``; otherwise the
    observation carries no identity and is rejected.
    """
    cusip = normalize_cusip9(observation.cusip9_input)
    isin_value, isin_state = qualify_isin(observation.isin_input)
    qualified_cusip: str | None = None
    if cusip.state is IdentifierState.VALID_CUSIP9 and cusip.normalized_cusip9 is not None:
        qualified_cusip = cusip.normalized_cusip9
    elif isin_value is not None:
        qualified_cusip = _anchor_cusip9_from_isin(isin_value)
    if qualified_cusip is not None:
        return f"cusip9:{qualified_cusip}", cusip.state.value, isin_state, qualified_cusip, isin_value
    if isin_value is not None:
        return f"isin:{isin_value}", cusip.state.value, isin_state, None, isin_value
    return None, cusip.state.value, isin_state, None, isin_value


def _normalized_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _windows_from_timeline(timeline: Sequence[tuple[date, str]]) -> list[tuple[str, date, date | None]]:
    """Collapse a sorted (date, value) timeline into PIT windows.

    Consecutive equal values coalesce; a window closes (``valid_to``) at the
    first date of the next distinct value, so an alias's window closes exactly
    when it is superseded.  The final run stays open (``valid_to = None``).
    """
    runs: list[tuple[str, date]] = []
    for observation_date, value in timeline:
        if runs and runs[-1][0] == value:
            continue
        runs.append((value, observation_date))
    windows: list[tuple[str, date, date | None]] = []
    for idx, (value, start) in enumerate(runs):
        end = runs[idx + 1][1] if idx + 1 < len(runs) else None
        windows.append((value, start, end))
    return windows


def _resolve_terms(observations: list[SecurityObservation]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Latest-non-null-observation wins per term; absent terms stay honest."""
    ordered = sorted(observations, key=lambda o: (o.observation_date, o.observation_id))
    measured: dict[str, Any] = {}
    for name in (*_SCALAR_TERMS, *_TRISTATE_TERMS, *_SCHEDULE_TERMS):
        resolved: Any = None
        for obs in ordered:
            candidate = getattr(obs, name)
            if candidate is not None:
                resolved = candidate
        measured[name] = resolved
    for name in _TRISTATE_TERMS:
        if measured[name] is None:
            measured[name] = "not_reported"
    summary = {name: measured[name] for name in (*_SCALAR_TERMS, *_TRISTATE_TERMS)}
    return summary, measured


def _windows_overlap(left: SecurityAlias, right: SecurityAlias) -> bool:
    return (
        (right.valid_to is None or left.valid_from < right.valid_to)
        and (left.valid_to is None or right.valid_from < left.valid_to)
    )


def _withhold_cross_identity_alias_collisions(
    securities: list[ResolvedSecurity],
) -> list[ResolvedSecurity]:
    """Withhold PIT aliases that overlap across distinct security identities.

    The security rows remain available with explicit ambiguity evidence, while
    reverse lookup can never multiply a source holding through a shared alias.
    Non-overlapping historical reassignment remains representable.
    """
    by_alias: dict[tuple[str, str], list[tuple[UUID, SecurityAlias]]] = {}
    for security in securities:
        for alias in security.aliases:
            by_alias.setdefault((alias.alias_kind, alias.alias_value), []).append(
                (security.security_id, alias)
            )

    colliding: set[tuple[UUID, str, str, date, date | None]] = set()
    for candidates in by_alias.values():
        for index, (left_id, left) in enumerate(candidates):
            for right_id, right in candidates[index + 1 :]:
                if left_id != right_id and _windows_overlap(left, right):
                    colliding.add(
                        (left_id, left.alias_kind, left.alias_value, left.valid_from, left.valid_to)
                    )
                    colliding.add(
                        (right_id, right.alias_kind, right.alias_value, right.valid_from, right.valid_to)
                    )

    if not colliding:
        return securities

    resolved: list[ResolvedSecurity] = []
    for security in securities:
        withheld = [
            alias
            for alias in security.aliases
            if (
                security.security_id,
                alias.alias_kind,
                alias.alias_value,
                alias.valid_from,
                alias.valid_to,
            )
            in colliding
        ]
        if not withheld:
            resolved.append(security)
            continue
        evidence = dict(security.identity_evidence)
        conflicts = dict(evidence.get("conflicts", {}))
        conflicts["cross_identity_alias"] = sorted(
            {f"{alias.alias_kind}:{alias.alias_value}" for alias in withheld}
        )
        evidence["conflicts"] = conflicts
        evidence["withheld_aliases"] = [
            {
                "alias_kind": alias.alias_kind,
                "alias_value": alias.alias_value,
                "valid_from": alias.valid_from.isoformat(),
                "valid_to": alias.valid_to.isoformat() if alias.valid_to else None,
            }
            for alias in withheld
        ]
        resolved.append(
            replace(
                security,
                identity_state="ambiguous",
                identity_reason_code="cross_identity_alias_collision",
                identity_evidence=evidence,
                aliases=tuple(alias for alias in security.aliases if alias not in withheld),
            )
        )
    return resolved


def resolve_securities(observations: Iterable[SecurityObservation]) -> ResolutionResult:
    """Fold identity/terms observations into deterministic securities + aliases."""
    groups: dict[str, list[SecurityObservation]] = {}
    keyed_cusip: dict[str, str | None] = {}
    keyed_isin: dict[str, dict[str, str | None]] = {}
    rejected: list[RejectedObservation] = []

    for obs in observations:
        key, cusip_state, isin_state, qualified_cusip, qualified_isin = _identity_key(obs)
        if key is None:
            rejected.append(
                RejectedObservation(
                    observation_id=str(obs.observation_id),
                    cusip9_state=cusip_state,
                    isin_state=isin_state,
                    reason_code="no_qualified_identifier",
                )
            )
            continue
        groups.setdefault(key, []).append(obs)
        keyed_cusip[str(obs.observation_id)] = qualified_cusip
        keyed_isin.setdefault(key, {})[str(obs.observation_id)] = qualified_isin

    securities: list[ResolvedSecurity] = []
    for key in sorted(groups):
        members = groups[key]
        security_id = uuid5(NAMESPACE_BOND_SECURITY, key)

        # Per-date evidence for point-in-time conflict detection.
        isin_by_date: dict[date, set[str]] = {}
        issuer_by_date: dict[date, set[str]] = {}
        cusip_by_date: dict[date, set[str]] = {}
        # One vote per contributing observation (= one N-PORT holding lot), which
        # is exactly the grain the issuer-consensus coverage was measured at.
        issuer_votes: Counter[str] = Counter()
        for obs in members:
            oid = str(obs.observation_id)
            isin_v = keyed_isin[key].get(oid)
            if isin_v:
                isin_by_date.setdefault(obs.observation_date, set()).add(isin_v)
            issuer_v = _normalized_text(obs.issuer_name)
            if issuer_v:
                issuer_by_date.setdefault(obs.observation_date, set()).add(issuer_v)
                issuer_votes[issuer_v] += 1
            cusip_v = keyed_cusip.get(oid)
            if cusip_v:
                cusip_by_date.setdefault(obs.observation_date, set()).add(cusip_v)

        isin_conflict = any(len(vs) > 1 for vs in isin_by_date.values())
        # Reported issuer spellings VARY (case, punctuation, corporate suffixes,
        # an appended coupon/maturity) across the funds that hold one bond. That
        # variance is reporting noise about a NAME, never competing evidence about
        # WHICH INSTRUMENT this is, so it no longer flips identity_state.
        # Measured 2026-08-07: treating it as an identity conflict marked 9,688 of
        # 10,073 curated securities 'ambiguous' and nulled their issuer, while the
        # underlying CUSIP9/ISIN evidence was a singleton in every one of them.
        # The name is resolved downstream by ``issuer_consensus`` (fail-closed,
        # abstains rather than guessing); until then it stays honestly NULL.
        issuer_variance = any(len(vs) > 1 for vs in issuer_by_date.values())
        conflicts: dict[str, list[str]] = {}
        if isin_conflict:
            conflicts["isin"] = sorted({v for vs in isin_by_date.values() for v in vs})

        reason = "conflicting_isin_evidence" if isin_conflict else None
        identity_state = "ambiguous" if reason else "resolved"

        aliases: list[SecurityAlias] = []
        lineage = {"engine": "security_master", "identity_key": key}
        # cusip9 identity alias (single open window) when the key is CUSIP9-based.
        if cusip_by_date:
            cusip_timeline = sorted((d, next(iter(vs))) for d, vs in cusip_by_date.items())
            for value, vfrom, vto in _windows_from_timeline(cusip_timeline):
                aliases.append(SecurityAlias("cusip9", value, vfrom, vto, dict(lineage)))
        # ISIN aliases: emitted with PIT windows unless the ISIN evidence itself
        # conflicts (then no arbitrary winner — withheld, recorded as evidence).
        if not isin_conflict and isin_by_date:
            isin_timeline = sorted((d, next(iter(vs))) for d, vs in isin_by_date.items())
            for value, vfrom, vto in _windows_from_timeline(isin_timeline):
                aliases.append(SecurityAlias("isin", value, vfrom, vto, dict(lineage)))

        summary, terms = _resolve_terms(members)
        # On an ambiguous identity, never expose a "chosen" value for a summary
        # term that itself conflicts: null it out (the full conflicting set lives
        # in identity_evidence.conflicts) so a naive consumer sees no winner.
        for conflicting_term in conflicts:
            if conflicting_term in summary:
                summary[conflicting_term] = None
            if conflicting_term in terms:
                terms[conflicting_term] = None
        if issuer_variance:
            # More than one reported spelling and no consensus applied yet: the
            # latest-non-null pick would be an arbitrary winner, so publish
            # NOTHING here. ``attribute_issuers`` decides (or abstains) next.
            summary["issuer_name"] = None
            terms["issuer_name"] = None
        evidence = {
            "contributing_observation_ids": sorted(str(o.observation_id) for o in members),
            "observation_dates": sorted({o.observation_date.isoformat() for o in members}),
            "distinct_cusip9": sorted({v for vs in cusip_by_date.values() for v in vs}),
            "distinct_isin": sorted({v for vs in isin_by_date.values() for v in vs}),
            "distinct_issuer_name": sorted({v for vs in issuer_by_date.values() for v in vs}),
            "issuer_name_variance": issuer_variance,
            "conflicts": conflicts,
        }
        identity_cusip9 = key[len("cusip9:"):] if key.startswith("cusip9:") else None
        securities.append(
            ResolvedSecurity(
                security_id=security_id,
                identity_key=key,
                identity_state=identity_state,
                identity_reason_code=reason,
                measured_terms=summary,
                terms=terms,
                identity_evidence=evidence,
                aliases=tuple(aliases),
                contributing_observation_ids=tuple(sorted(str(o.observation_id) for o in members)),
                cusip9=identity_cusip9,
                issuer_votes=tuple(sorted(issuer_votes.items())),
            )
        )

    securities = _withhold_cross_identity_alias_collisions(securities)
    return ResolutionResult(securities=tuple(securities), rejected=tuple(rejected))


# ---------------------------------------------------------------------------
# Issuer attribution overlay (pure; see src/bonds/issuer_consensus.py)
# ---------------------------------------------------------------------------
def attribute_issuers(
    securities: Sequence[ResolvedSecurity],
    *,
    lei_by_cusip9: Mapping[str, Iterable[str]] | None = None,
) -> tuple[ResolvedSecurity, ...]:
    """Fill ``issuer_name`` by layered reported-name consensus, or abstain.

    Layer 1 votes on the security's OWN reported spellings — the exact-identity
    scope, which for a CUSIP9-keyed identity is the exact CUSIP9 (an ISIN-keyed
    identity uses the same own-security scope and has no CUSIP6 fallback, since
    it has no CUSIP prefix to fall back to). Only when layer 1 abstains does
    layer 2 widen the vote to every security sharing the 6-character CUSIP
    prefix — that INFERS across securities, so it also carries the vehicle-name
    veto. A security whose vote is split, or whose reported LEIs disagree, ends
    with a NULL issuer and the reason recorded. The verdict and its evidence
    always land in ``identity_evidence['issuer_attribution']``, attributed or not.

    Every security goes through the rule, including one every filer already
    spells identically: a unanimous vote is simply a single cluster at share 1.0.
    Routing it through anyway is what lets the LEI veto reach a name that filers
    agree on but attach to two different legal entities.

    ``lei_by_cusip9`` carries the reported legal-entity identifiers per CUSIP9.
    It is OPTIONAL: with no LEI evidence the rule degrades to name consensus
    alone (an honest weakening, never a fabricated agreement).
    """
    leis = {str(k): tuple(v) for k, v in (lei_by_cusip9 or {}).items()}

    votes_by_cusip6: dict[str, Counter[str]] = {}
    for security in securities:
        if not security.cusip9:
            continue
        prefix = security.cusip9[:6]
        bucket = votes_by_cusip6.setdefault(prefix, Counter())
        for name, count in security.issuer_votes:
            bucket[name] += count

    # The CUSIP6 LEI scope spans EVERY reported CUSIP9 under the prefix, not only
    # the ones this batch happens to publish. Narrowing it to the published
    # universe would hide a disagreement carried by a sibling security and make
    # the fallback layer quietly more permissive than the rule it was measured
    # under. Fail-closed means the veto sees everything the filings say.
    lei_by_cusip6: dict[str, set[str]] = {}
    prefixes = set(votes_by_cusip6)
    for cusip9, values in leis.items():
        prefix = cusip9[:6]
        if prefix in prefixes:
            lei_by_cusip6.setdefault(prefix, set()).update(values)

    resolved: list[ResolvedSecurity] = []
    for security in securities:
        # Consensus owns the name outright — including for a security every filer
        # already spells identically. A unanimous vote simply resolves to one
        # cluster at share 1.0, and routing it through the same rule is what lets
        # the LEI veto reach it: filers can agree on the NAME while reporting two
        # different legal entities, and that disagreement must still abstain.
        verdict = issuer_consensus.resolve_issuer(
            dict(security.issuer_votes),
            layer=issuer_consensus.ATTRIBUTION_CUSIP9,
            leis=leis.get(security.cusip9 or "", ()),
        )
        if not verdict.attributed and security.cusip9:
            prefix = security.cusip9[:6]
            verdict = issuer_consensus.resolve_issuer(
                dict(votes_by_cusip6.get(prefix, Counter())),
                layer=issuer_consensus.ATTRIBUTION_CUSIP6,
                leis=lei_by_cusip6.get(prefix, set()),
            )
        resolved.append(_with_issuer(security, verdict))
    return tuple(resolved)


def _with_issuer(security: ResolvedSecurity, verdict: issuer_consensus.IssuerVerdict) -> ResolvedSecurity:
    """Write the verdict onto the security — including an ABSTENTION.

    An abstention actively CLEARS any name the term resolution had picked up. The
    consensus is the only authority on this field, so "the rule declined to name
    it" must beat "an earlier pass happened to have a value"; otherwise a
    multi-LEI veto would be silently overridden by the very value it rejected.
    """
    evidence = dict(security.identity_evidence)
    evidence["issuer_attribution"] = {
        "attribution": verdict.attribution, **verdict.evidence,
    }
    summary = dict(security.measured_terms)
    terms = dict(security.terms)
    summary["issuer_name"] = verdict.issuer_name
    terms["issuer_name"] = verdict.issuer_name
    return replace(
        security, measured_terms=summary, terms=terms, identity_evidence=evidence,
    )


# ---------------------------------------------------------------------------
# Vendor reference terms overlay (fills ABSENCE only; never overrides the chain)
# ---------------------------------------------------------------------------
# The published universe reports coupon and maturity almost completely (measured
# 2026-08-07 on the 10,073 curated securities: 8 missing coupons, 0 missing
# maturities) but reports NO seniority, secured flag, callability or amount
# outstanding at all. Those come from a neutral reference table
# (``bond_reference_terms``), recorded as basis ``vendor_reference`` — a neutral
# label, because no serving copy ever names a source.
_REFERENCE_SCALARS = ("coupon_rate", "coupon_type", "maturity_date", "seniority", "day_count")
_REFERENCE_TRISTATE = ("secured",)
_REFERENCE_TERMS_ONLY = ("callable", "amount_outstanding_mm", "payment_frequency")

REFERENCE_BASIS = "vendor_reference"


def apply_reference_terms(
    securities: Sequence[ResolvedSecurity],
    reference: Mapping[str, Mapping[str, Any]] | None,
) -> tuple[ResolvedSecurity, ...]:
    """Fill terms the chain does NOT carry from the neutral reference table.

    Strictly gap-filling: a field the published observations already resolved is
    never touched, and a tri-state at ``not_reported`` counts as absent. Fields
    the reference itself does not carry stay absent — no default, no guess. Every
    filled field is listed under ``identity_evidence['reference_terms']``.
    """
    if not reference:
        return tuple(securities)
    resolved: list[ResolvedSecurity] = []
    for security in securities:
        row = reference.get(security.cusip9 or "")
        if not row:
            resolved.append(security)
            continue
        summary = dict(security.measured_terms)
        terms = dict(security.terms)
        filled: list[str] = []
        for name in _REFERENCE_SCALARS:
            if summary.get(name) is None and row.get(name) is not None:
                summary[name] = row[name]
                terms[name] = row[name]
                filled.append(name)
        for name in _REFERENCE_TRISTATE:
            if summary.get(name) in (None, "not_reported") and row.get(name) is not None:
                summary[name] = row[name]
                terms[name] = row[name]
                filled.append(name)
        for name in _REFERENCE_TERMS_ONLY:
            if terms.get(name) is None and row.get(name) is not None:
                terms[name] = row[name]
                filled.append(name)
        if not filled:
            resolved.append(security)
            continue
        evidence = dict(security.identity_evidence)
        evidence["reference_terms"] = {"basis": REFERENCE_BASIS, "fields": sorted(filled)}
        resolved.append(replace(
            security, measured_terms=summary, terms=terms, identity_evidence=evidence,
        ))
    return tuple(resolved)


# ---------------------------------------------------------------------------
# Publication wiring (sec_derived_publications protocol)
# ---------------------------------------------------------------------------
def install_schema(conn: psycopg.Connection) -> None:
    """Apply the publication protocol + bond_security_v1 DDL idempotently."""
    with conn.cursor() as cur:
        cur.execute((ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8"))
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
        cur.execute(REFERENCE_TERMS_SCHEMA_PATH.read_text(encoding="utf-8"))


def _relation_exists(conn: psycopg.Connection, name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s)", (name,)).fetchone()
    return bool(row and row[0] is not None)


def load_issuer_lei(conn: psycopg.Connection) -> dict[str, list[str]]:
    """Reported legal-entity identifiers per CUSIP9 (empty when unavailable).

    The LEI is the second half of the fail-closed issuer rule: more than one
    reported LEI in a vote scope means the reported legal entities genuinely
    disagree and the attribution abstains. It lives only on the N-PORT holding
    grain, not on ``bond_security_observation``, so it is read here. A missing
    relation degrades the rule to name consensus alone — honestly weaker, never
    a fabricated agreement.
    """
    if not _relation_exists(conn, "sec_nport_holdings_v2_current"):
        return {}
    rows = conn.execute(
        "SELECT DISTINCT upper(btrim(cusip)) AS cusip9, btrim(issuer_lei) AS lei "
        "FROM sec_nport_holdings_v2_current "
        "WHERE nullif(btrim(coalesce(cusip,'')),'') IS NOT NULL "
        "  AND nullif(btrim(coalesce(issuer_lei,'')),'') IS NOT NULL"
    ).fetchall()
    out: dict[str, list[str]] = {}
    for cusip9, lei in rows:
        out.setdefault(cusip9, []).append(lei)
    return out


def load_reference_terms(conn: psycopg.Connection) -> dict[str, dict[str, Any]]:
    """The neutral reference terms keyed by CUSIP9 (empty when unavailable)."""
    if not _relation_exists(conn, "bond_reference_terms"):
        return {}
    rows = conn.execute(
        "SELECT cusip9, coupon_rate, coupon_type, maturity_date, seniority, secured, "
        "day_count, payment_frequency, callable, amount_outstanding_mm "
        "FROM bond_reference_terms"
    ).fetchall()
    keys = ("coupon_rate", "coupon_type", "maturity_date", "seniority", "secured",
            "day_count", "payment_frequency", "callable", "amount_outstanding_mm")
    numeric = {"coupon_rate", "amount_outstanding_mm"}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        # DB numerics arrive as Decimal, which json.dumps cannot encode and which
        # would blow up when the enriched terms are written back as jsonb.
        out[row[0]] = {
            key: (float(value) if key in numeric and value is not None else value)
            for key, value in zip(keys, row[1:])
        }
    return out


def publication_id_for(as_of: date, code_revision: str) -> UUID:
    return uuid5(_NAMESPACE_PUBLICATION, f"{PRODUCT}|{as_of.isoformat()}|{code_revision}")


def _load_observations(conn: psycopg.Connection, as_of: date) -> list[SecurityObservation]:
    rows = conn.execute(
        "SELECT observation_id, observation_date, cusip9_input, isin_input, issuer_name, currency, "
        "coupon_type, coupon_rate, maturity_date, seniority, secured, is_144a, day_count, "
        "settlement_convention, coupon_schedule, call_schedule, put_schedule, source_lineage "
        "FROM bond_security_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    result: list[SecurityObservation] = []
    for r in rows:
        coupon_rate = None if r[7] is None else str(r[7])
        result.append(
            SecurityObservation(
                observation_id=str(r[0]), observation_date=r[1], cusip9_input=r[2], isin_input=r[3],
                issuer_name=r[4], currency=r[5], coupon_type=r[6], coupon_rate=coupon_rate,
                maturity_date=r[8], seniority=r[9], secured=r[10], is_144a=r[11], day_count=r[12],
                settlement_convention=r[13], coupon_schedule=r[14], call_schedule=r[15],
                put_schedule=r[16], source_lineage=r[17] or {},
            )
        )
    return result


def _input_fingerprint(
    as_of: date,
    observations: Sequence[SecurityObservation],
    *,
    lei_by_cusip9: Mapping[str, Iterable[str]] | None = None,
    reference_terms: Mapping[str, Mapping[str, Any]] | None = None,
) -> str:
    """Digest of every input the snapshot is built from (identity + enrichment).

    The enrichment inputs are digested too, so a build pinned to one reference
    cohort cannot be silently completed against another one. They are appended
    AFTER the observation block and omitted entirely when empty, so a build with
    no enrichment available keeps its historical fingerprint byte-for-byte.
    """
    parts = [f"{PRODUCT}|{as_of.isoformat()}"]
    for obs in sorted(observations, key=lambda o: str(o.observation_id)):
        parts.append(
            "|".join(
                str(x)
                for x in (
                    obs.observation_id, obs.observation_date.isoformat(),
                    obs.cusip9_input, obs.isin_input,
                )
            )
        )
    if lei_by_cusip9:
        parts.append("lei")
        for cusip9 in sorted(lei_by_cusip9):
            parts.append(f"{cusip9}|{'|'.join(sorted(str(v) for v in lei_by_cusip9[cusip9]))}")
    if reference_terms:
        parts.append("reference_terms")
        for cusip9 in sorted(reference_terms):
            row = reference_terms[cusip9]
            parts.append(
                cusip9 + "|" + "|".join(f"{k}={row[k]!r}" for k in sorted(row))
            )
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _coupon_rate_numeric(value: object) -> object:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def materialize(
    conn: psycopg.Connection,
    *,
    as_of: date,
    source_run_id: UUID,
    source_package_id: UUID,
    code_revision: str,
) -> dict[str, Any]:
    """Prepare -> build -> validate -> current, idempotently, for one as_of.

    A partial/failed build never becomes current: the snapshot rows are written
    only while the publication is 'prepared', the pin is verified before the
    validate step, and the current pointer only advances after the publication
    is validated (the publication protocol's fail-closed guards enforce this).
    """
    publication_id = publication_id_for(as_of, code_revision)
    observations = _load_observations(conn, as_of)
    # Enrichment inputs are read BEFORE the fingerprint so the build pin digests
    # what the snapshot was actually built from, not only the identity inputs.
    lei_by_cusip9 = load_issuer_lei(conn)
    reference_terms = load_reference_terms(conn)
    fingerprint = _input_fingerprint(
        as_of, observations, lei_by_cusip9=lei_by_cusip9, reference_terms=reference_terms,
    )

    existing = conn.execute(
        "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
        (publication_id,),
    ).fetchone()
    if existing is None:
        version = conn.execute(
            "SELECT COALESCE(max(publication_version),0)+1 FROM sec_derived_publications WHERE product=%s",
            (PRODUCT,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO sec_derived_publications"
            "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint) "
            "VALUES(%s,%s,%s,%s,%s,%s)",
            (publication_id, PRODUCT, version, source_run_id, source_package_id, fingerprint),
        )
        lifecycle = "prepared"
    else:
        lifecycle = existing[0]

    result = resolve_securities(observations)
    # Overlay order matters: attribute the issuer from the REPORTED evidence
    # first, then gap-fill the remaining terms from the neutral reference. The
    # reference carries no issuer name at all, so the two never compete.
    securities = attribute_issuers(result.securities, lei_by_cusip9=lei_by_cusip9)
    securities = apply_reference_terms(securities, reference_terms)
    result = ResolutionResult(securities=securities, rejected=result.rejected)
    attributed = sum(
        1 for sec in result.securities if sec.measured_terms.get("issuer_name")
    )
    if lifecycle == "prepared":
        conn.execute(
            "INSERT INTO bond_security_v1_builds"
            "(publication_id,input_fingerprint,as_of_date,observation_input_count) "
            "VALUES(%s,%s,%s,%s) ON CONFLICT (publication_id) DO NOTHING",
            (publication_id, fingerprint, as_of, len(observations)),
        )
        pinned = conn.execute(
            "SELECT input_fingerprint, as_of_date FROM bond_security_v1_builds WHERE publication_id=%s",
            (publication_id,),
        ).fetchone()
        if pinned[0] != fingerprint:
            raise RuntimeError(
                f"bond_security_v1 publication already pinned to fingerprint {pinned[0]}"
            )
        if pinned[1] != as_of:
            raise RuntimeError(
                f"bond_security_v1 publication already pinned to as_of {pinned[1]}"
            )
        for sec in result.securities:
            conn.execute(
                "INSERT INTO bond_security_v1"
                "(publication_id,source_run_id,security_id,identity_key,identity_state,identity_reason_code,"
                " issuer_name,currency,coupon_type,coupon_rate,maturity_date,seniority,secured,is_144a,"
                " day_count,settlement_convention,terms,identity_evidence,measured_at,provenance) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (publication_id,security_id) DO NOTHING",
                (
                    publication_id, source_run_id, sec.security_id, sec.identity_key,
                    sec.identity_state, sec.identity_reason_code,
                    sec.measured_terms["issuer_name"], sec.measured_terms["currency"],
                    sec.measured_terms["coupon_type"], _coupon_rate_numeric(sec.measured_terms["coupon_rate"]),
                    sec.measured_terms["maturity_date"], sec.measured_terms["seniority"],
                    sec.measured_terms["secured"], sec.measured_terms["is_144a"],
                    sec.measured_terms["day_count"], sec.measured_terms["settlement_convention"],
                    Jsonb(_jsonable(sec.terms)), Jsonb(sec.identity_evidence), as_of,
                    Jsonb({"resolver": "security_master", "methodology_version": METHODOLOGY_VERSION}),
                ),
            )
            for alias in sec.aliases:
                conn.execute(
                    "INSERT INTO bond_security_alias_v1"
                    "(publication_id,security_id,alias_kind,alias_value,valid_from,valid_to,source_lineage) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (publication_id,security_id,alias_kind,alias_value,valid_from) DO NOTHING",
                    (
                        publication_id, sec.security_id, alias.alias_kind, alias.alias_value,
                        alias.valid_from, alias.valid_to, Jsonb(alias.source_lineage),
                    ),
                )
        conn.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))

    current = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s", (PRODUCT,)
    ).fetchone()
    if current is None or current[0] != publication_id:
        conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (PRODUCT, publication_id))

    return {
        "product": PRODUCT,
        "publication_id": str(publication_id),
        "as_of": as_of.isoformat(),
        "securities": len(result.securities),
        "rejected": len(result.rejected),
        "issuer_attributed": attributed,
        "issuer_abstained": len(result.securities) - attributed,
        "reference_terms_rows": len(reference_terms),
        "lei_cusip9s": len(lei_by_cusip9),
        "state": "current",
    }


def _jsonable(payload: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in payload.items():
        out[key] = value.isoformat() if isinstance(value, date) else value
    return out
