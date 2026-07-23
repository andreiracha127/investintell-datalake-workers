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
from dataclasses import dataclass, field, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID, uuid5

import psycopg
from psycopg.types.json import Jsonb

from src.bonds.identifiers import normalize_cusip9
from src.bonds.states import IdentifierState

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_security_v1.sql"
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
        for obs in members:
            oid = str(obs.observation_id)
            isin_v = keyed_isin[key].get(oid)
            if isin_v:
                isin_by_date.setdefault(obs.observation_date, set()).add(isin_v)
            issuer_v = _normalized_text(obs.issuer_name)
            if issuer_v:
                issuer_by_date.setdefault(obs.observation_date, set()).add(issuer_v)
            cusip_v = keyed_cusip.get(oid)
            if cusip_v:
                cusip_by_date.setdefault(obs.observation_date, set()).add(cusip_v)

        isin_conflict = any(len(vs) > 1 for vs in isin_by_date.values())
        issuer_conflict = any(len(vs) > 1 for vs in issuer_by_date.values())
        conflicts: dict[str, list[str]] = {}
        if isin_conflict:
            conflicts["isin"] = sorted({v for vs in isin_by_date.values() for v in vs})
        if issuer_conflict:
            conflicts["issuer_name"] = sorted({v for vs in issuer_by_date.values() for v in vs})

        if isin_conflict and issuer_conflict:
            reason = "conflicting_identity_evidence"
        elif isin_conflict:
            reason = "conflicting_isin_evidence"
        elif issuer_conflict:
            reason = "conflicting_issuer_evidence"
        else:
            reason = None
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
        evidence = {
            "contributing_observation_ids": sorted(str(o.observation_id) for o in members),
            "observation_dates": sorted({o.observation_date.isoformat() for o in members}),
            "distinct_cusip9": sorted({v for vs in cusip_by_date.values() for v in vs}),
            "distinct_isin": sorted({v for vs in isin_by_date.values() for v in vs}),
            "distinct_issuer_name": sorted({v for vs in issuer_by_date.values() for v in vs}),
            "conflicts": conflicts,
        }
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
            )
        )

    securities = _withhold_cross_identity_alias_collisions(securities)
    return ResolutionResult(securities=tuple(securities), rejected=tuple(rejected))


# ---------------------------------------------------------------------------
# Publication wiring (sec_derived_publications protocol)
# ---------------------------------------------------------------------------
def install_schema(conn: psycopg.Connection) -> None:
    """Apply the publication protocol + bond_security_v1 DDL idempotently."""
    with conn.cursor() as cur:
        cur.execute((ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8"))
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


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


def _input_fingerprint(as_of: date, observations: Sequence[SecurityObservation]) -> str:
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
    fingerprint = _input_fingerprint(as_of, observations)

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
        "state": "current",
    }


def _jsonable(payload: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in payload.items():
        out[key] = value.isoformat() if isinstance(value, date) else value
    return out
