"""Exact composite-triple debt classification.

Ported from the bond pilot's ``DebtMapping.classify`` with the capability /
signing / approval-provenance machinery removed (Increment 2 Global Constraint
#7 — only the algorithm enters the production library). The decision is an exact
lookup on the ``(issuer_category, asset_class, instrument_structure)`` triple;
there is no aliasing, normalization, fuzzy matching, or inferred decision.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping, Sequence, cast

from .errors import BondError
from .states import DebtState


_RULE_FIELDS = ("issuer_category", "asset_class", "instrument_structure")
_DECISIONS = {
    "eligible_debt": DebtState.DEBT_LIKE_ELIGIBLE,
    "non_debt_excluded": DebtState.INELIGIBLE_NON_DEBT,
}


def _invalid() -> BondError:
    return BondError("invalid_debt_mapping")


def _normalize_rules(value: object) -> tuple[tuple[str, str, str, str], ...]:
    """Validate rules given as dicts or 4-tuples; reject duplicate composite keys."""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise _invalid()
    output: list[tuple[str, str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for rule in value:
        if isinstance(rule, Mapping):
            if set(rule) != {*_RULE_FIELDS, "decision"}:
                raise _invalid()
            components = tuple(rule[name] for name in _RULE_FIELDS)
            decision = rule["decision"]
        elif isinstance(rule, (tuple, list)) and len(rule) == 4:
            components = tuple(rule[:3])
            decision = rule[3]
        else:
            raise _invalid()
        if any(not isinstance(part, str) or part == "" for part in components) or decision not in _DECISIONS:
            raise _invalid()
        key: tuple[str, str, str] = components  # type: ignore[assignment]
        if key in seen:
            raise BondError("duplicate_debt_rule", {"key": key})
        seen.add(key)
        output.append((*key, decision))  # type: ignore[arg-type]
    return tuple(output)


@dataclass(frozen=True)
class DebtMapping:
    """Exact triple lookup with no aliases, normalization, or inferred decisions."""

    rules: Sequence[object]
    _lookup: Mapping[tuple[str, str, str], DebtState] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        normalized = _normalize_rules(self.rules)
        object.__setattr__(self, "rules", normalized)
        object.__setattr__(
            self, "_lookup", MappingProxyType({rule[:3]: _DECISIONS[rule[3]] for rule in normalized})
        )

    def classify(self, issuer_category: object, asset_class: object, instrument_structure: object) -> DebtState:
        components = (issuer_category, asset_class, instrument_structure)
        if any(component is None or component == "" for component in components):
            return DebtState.MISSING_CATEGORY
        if not all(isinstance(component, str) for component in components):
            return DebtState.AMBIGUOUS_CATEGORY
        key = cast("tuple[str, str, str]", components)
        return self._lookup.get(key, DebtState.AMBIGUOUS_CATEGORY)
