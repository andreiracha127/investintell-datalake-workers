"""Pure contracts for the mixed_quant_v1 publication.

These functions never touch a database. They encode the binding identity and
income rules the publication worker relies on:

* Stable internal UUIDs for identity; CUSIP/ISIN/ticker are aliases with
  validity intervals, never identity.
* Collisions (the same alias observed for evidence that does not provably tie
  the observations together) are preserved as separate unresolved records.
* Observed income only: no inferred yield metrics (YTM/YTW/OAS/Z-spread/price/
  yield) may be recorded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
import math
from typing import Any, Iterable, Mapping
import uuid

# Reuse the governed class-factor quality vocabulary from the ported offline
# runner so the publication and the runner cannot drift apart.
from src.workers.sec_class_factors import _QUALITY as CLASS_FACTOR_QUALITY

# Fixed namespace so uuid5 identities are reproducible across runs and machines.
NAMESPACE_INSTRUMENT = uuid.UUID("6f9619ff-8b86-d011-b42d-00cf4fc964ff")

ALIAS_TYPES = frozenset({"cusip", "isin", "ticker"})
INSTRUMENT_TYPES = frozenset({"fund", "equity", "bond"})

# Fields that would encode an inferred yield/price and must never reach income.
INFERRED_YIELD_FIELDS = frozenset({
    "ytm", "yield_to_maturity", "ytw", "yield_to_worst", "oas",
    "option_adjusted_spread", "z_spread", "zspread", "g_spread", "i_spread",
    "spread", "yield", "current_yield", "price", "clean_price", "dirty_price",
    "duration", "modified_duration", "convexity",
})

_REQUIRED_INCOME_FIELDS = ("event_date", "cash_amount", "currency", "event_type")

# Fixed vocabulary of named bond factor inputs. A factor absent from observed
# data is published as an ABSENT coverage state, never as a fabricated value.
NAMED_BOND_FACTORS = ("credit", "curve", "duration", "inflation", "liquidity")

# Observed look-through vs return-estimated (regression/latent) exposures.
MEASUREMENT_TYPES = frozenset({"observed", "estimated"})


class ContractError(ValueError):
    """Raised when a value violates a mixed_quant_v1 contract."""


@dataclass(frozen=True)
class AliasRecord:
    alias_type: str
    alias_value: str
    valid_from: date
    valid_to: date | None
    source_lineage: dict[str, Any]


@dataclass(frozen=True)
class IdentityObservation:
    """One point-in-time observation binding an alias to an instrument."""

    observation_id: uuid.UUID
    instrument_type: str
    currency: str
    alias_type: str
    alias_value: str
    valid_from: date
    observed_at: datetime
    source_lineage: dict[str, Any]
    valid_to: date | None = None
    issuer_id: str | None = None
    security_id: str | None = None
    # Deterministic evidence that provably ties observations to one identity.
    # Absent => the observation stays unresolved and is never merged on alias.
    deterministic_key: str | None = None

    def __post_init__(self) -> None:
        if self.alias_type not in ALIAS_TYPES:
            raise ContractError(f"unknown alias_type {self.alias_type!r}")
        if self.instrument_type not in INSTRUMENT_TYPES:
            raise ContractError(f"unknown instrument_type {self.instrument_type!r}")
        if not self.alias_value:
            raise ContractError("alias_value must be non-empty")
        require_lineage(self.source_lineage)


@dataclass(frozen=True)
class ResolvedInstrument:
    instrument_id: uuid.UUID
    instrument_type: str
    currency: str
    issuer_id: str | None
    security_id: str | None
    unresolved: bool
    aliases: tuple[AliasRecord, ...]
    coverage: dict[str, Any] = field(default_factory=dict)

    @property
    def validity(self) -> tuple[date | None, date | None]:
        """Half-open [lower, upper) hull of the instrument's alias intervals."""
        lower = min((a.valid_from for a in self.aliases), default=None)
        uppers = [a.valid_to for a in self.aliases]
        upper = None if any(u is None for u in uppers) else max(uppers, default=None)
        return (lower, upper)


def require_lineage(source_lineage: Any) -> dict[str, Any]:
    """Every published value must resolve to its observation via lineage."""
    if not isinstance(source_lineage, Mapping) or not source_lineage:
        raise ContractError("source_lineage must be a non-empty object")
    return dict(source_lineage)


def mint_instrument_id(deterministic_key: str) -> uuid.UUID:
    """Stable internal UUID for an identity given its deterministic evidence."""
    if not deterministic_key:
        raise ContractError("deterministic_key must be non-empty")
    return uuid.uuid5(NAMESPACE_INSTRUMENT, deterministic_key)


def _group_key(obs: IdentityObservation) -> tuple[bool, str]:
    """(unresolved?, stable grouping key).

    Deterministic evidence merges observations; otherwise each observation is
    its own unresolved instrument keyed on its stable observation_id so reruns
    reproduce the same identity.
    """
    if obs.deterministic_key:
        return (False, obs.deterministic_key)
    return (True, f"unresolved:{obs.observation_id}")


def build_alias_history(observations: Iterable[IdentityObservation]) -> tuple[AliasRecord, ...]:
    """Fold observations of one instrument into alias validity intervals.

    Within an alias_type, when the alias_value changes over time the earlier
    open interval is closed at the successor's valid_from.
    """
    records: list[AliasRecord] = []
    by_type: dict[str, list[IdentityObservation]] = {}
    for obs in observations:
        by_type.setdefault(obs.alias_type, []).append(obs)

    for alias_type, group in by_type.items():
        # Collapse repeated observations of the same value to its earliest start
        # and the lineage of that first sighting.
        earliest: dict[str, IdentityObservation] = {}
        explicit_to: dict[str, date | None] = {}
        for obs in sorted(group, key=lambda o: (o.valid_from, o.observed_at)):
            if obs.alias_value not in earliest:
                earliest[obs.alias_value] = obs
                explicit_to[obs.alias_value] = obs.valid_to
            elif obs.valid_to is not None:
                prior = explicit_to[obs.alias_value]
                explicit_to[obs.alias_value] = obs.valid_to if prior is None else max(prior, obs.valid_to)

        ordered = sorted(earliest.values(), key=lambda o: o.valid_from)
        for idx, obs in enumerate(ordered):
            valid_to = explicit_to[obs.alias_value]
            if valid_to is None and idx + 1 < len(ordered):
                valid_to = ordered[idx + 1].valid_from
            records.append(AliasRecord(
                alias_type=alias_type,
                alias_value=obs.alias_value,
                valid_from=obs.valid_from,
                valid_to=valid_to,
                source_lineage=dict(obs.source_lineage),
            ))
    return tuple(sorted(records, key=lambda r: (r.alias_type, r.valid_from, r.alias_value)))


def resolve_identities(observations: Iterable[IdentityObservation]) -> list[ResolvedInstrument]:
    """Group observations into stable instrument identities.

    Deterministic evidence merges; collisions without such evidence are kept as
    separate unresolved instruments.
    """
    grouped: dict[str, list[IdentityObservation]] = {}
    unresolved_flag: dict[str, bool] = {}
    for obs in observations:
        unresolved, key = _group_key(obs)
        grouped.setdefault(key, []).append(obs)
        unresolved_flag[key] = unresolved

    resolved: list[ResolvedInstrument] = []
    for key, group in grouped.items():
        head = group[0]
        unresolved = unresolved_flag[key]
        instrument_id = mint_instrument_id(key if unresolved else head.deterministic_key)
        aliases = build_alias_history(group)
        coverage = {
            "observation_count": len(group),
            "alias_count": len(aliases),
            "resolution": "unresolved" if unresolved else "deterministic",
        }
        resolved.append(ResolvedInstrument(
            instrument_id=instrument_id,
            instrument_type=head.instrument_type,
            currency=head.currency,
            issuer_id=head.issuer_id,
            security_id=head.security_id,
            unresolved=unresolved,
            aliases=aliases,
            coverage=coverage,
        ))
    resolved.sort(key=lambda inst: str(inst.instrument_id))
    return resolved


def validate_income_event(event: Mapping[str, Any]) -> None:
    """Reject inferred yield metrics and enforce observed cash-event fields."""
    banned = INFERRED_YIELD_FIELDS.intersection(k.lower() for k in event)
    if banned:
        raise ContractError(f"income event carries inferred yield fields: {sorted(banned)}")
    missing = [f for f in _REQUIRED_INCOME_FIELDS if event.get(f) in (None, "")]
    if missing:
        raise ContractError(f"income event missing observed fields: {missing}")
    require_lineage(event.get("source_lineage"))


def _finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def validate_class_factor_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one governed class-factor (return-estimated) exposure row.

    The regression itself is computed by the governed offline runner; here we
    only guarantee the carried result is well-formed (finite value, a known
    measurement type and quality state, a method, and lineage) so the publication
    faithfully transports it without inventing anything.
    """
    factor = row.get("factor")
    if not isinstance(factor, str) or not factor:
        raise ContractError("class factor requires a non-empty factor name")
    if not _finite_number(row.get("value")):
        raise ContractError(f"class factor {factor!r} value must be finite")
    method = row.get("method")
    if not isinstance(method, str) or not method:
        raise ContractError(f"class factor {factor!r} requires a method")
    if row.get("measurement_type") not in MEASUREMENT_TYPES:
        raise ContractError(f"class factor {factor!r} measurement_type must be one of {sorted(MEASUREMENT_TYPES)}")
    if row.get("quality_status") not in CLASS_FACTOR_QUALITY:
        raise ContractError(f"class factor {factor!r} quality_status must be one of {sorted(CLASS_FACTOR_QUALITY)}")
    flags = row.get("quality_flags", [])
    if not isinstance(flags, (list, tuple)) or any(not isinstance(f, str) or not f for f in flags):
        raise ContractError(f"class factor {factor!r} quality_flags must be non-empty strings")
    evidence = row.get("evidence", {})
    if not isinstance(evidence, Mapping):
        raise ContractError(f"class factor {factor!r} evidence must be an object")
    require_lineage(row.get("source_lineage"))
    return {
        "factor": factor,
        "value": float(row["value"]),
        "method": method,
        "measurement_type": row["measurement_type"],
        "quality_status": row["quality_status"],
        "quality_flags": list(flags),
        "evidence": dict(evidence),
        "source_lineage": dict(row["source_lineage"]),
    }


def validate_bond_factor_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one observed named bond factor input.

    Only the five named factors are admissible and the value must be a finite
    observation with lineage; nothing is ever inferred or fabricated.
    """
    factor = row.get("factor")
    if factor not in NAMED_BOND_FACTORS:
        raise ContractError(f"bond factor {factor!r} not in named vocabulary {NAMED_BOND_FACTORS}")
    if not _finite_number(row.get("value")):
        raise ContractError(f"bond factor {factor!r} value must be finite")
    method = row.get("method")
    if not isinstance(method, str) or not method:
        raise ContractError(f"bond factor {factor!r} requires a method")
    require_lineage(row.get("source_lineage"))
    return {
        "factor": factor,
        "value": float(row["value"]),
        "method": method,
        "source_lineage": dict(row["source_lineage"]),
    }
