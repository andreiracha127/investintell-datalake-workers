"""Immutable in-memory contracts for the pure Plan C allocator core."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, Mapping

import numpy as np
from numpy.typing import NDArray

from .errors import AllocatorIssue

FloatArray = NDArray[np.float64]
DecisionMode = Literal["category_equal_weight", "instrument"]


def _readonly_array(value: object, *, ndim: int | None = None) -> FloatArray:
    array = np.array(value, dtype=np.float64, copy=True)
    if ndim is not None and array.ndim != ndim:
        raise ValueError(f"expected a {ndim}-D array, got shape {array.shape}")
    array.setflags(write=False)
    return array


@dataclass(frozen=True)
class Instrument:
    instrument_id: str
    label: str
    category_id: str
    sleeve_id: str
    returns: FloatArray

    def __post_init__(self) -> None:
        object.__setattr__(self, "returns", _readonly_array(self.returns, ndim=1))


@dataclass(frozen=True)
class SleeveBand:
    sleeve_id: str
    lo: float
    hi: float


@dataclass(frozen=True)
class BlockBudget:
    indices: tuple[int, ...]
    lo: float
    hi: float
    label: str


@dataclass(frozen=True)
class LinearConstraint:
    coef: FloatArray
    lo: float | None
    hi: float | None
    label: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "coef", _readonly_array(self.coef, ndim=1))


@dataclass(frozen=True)
class AllocatorUniverse:
    instruments: tuple[Instrument, ...]
    return_dates: tuple[str, ...]
    sleeve_ids: tuple[str, ...]
    mapping_version: str
    decision_mode: DecisionMode = "category_equal_weight"


@dataclass(frozen=True)
class AllocatorPolicy:
    sleeve_bands: tuple[SleeveBand, ...]
    cvar_alpha: float
    cvar_limit: float
    instrument_cap: float | None = None
    instrument_floor: float | None = None
    risk_asset_sleeves: frozenset[str] = frozenset()
    risk_assets_cap: float | None = None
    defensive_sleeves: frozenset[str] = frozenset()
    defensive_floor: float | None = None
    instrument_linear_constraints: tuple[LinearConstraint, ...] = ()
    strict_missing_sleeves: bool = False


@dataclass(frozen=True)
class Tolerances:
    sum: float = 1e-6
    weight: float = 1e-6
    constraint: float = 1e-6
    cvar: float = 1e-4


@dataclass(frozen=True)
class CompiledProblem:
    category_ids: tuple[str, ...]
    category_sleeve_ids: tuple[str, ...]
    sleeve_ids: tuple[str, ...]
    instrument_ids: tuple[str, ...]
    instrument_labels: tuple[str, ...]
    S: FloatArray
    M: FloatArray
    daily_returns: FloatArray
    category_returns: FloatArray
    return_dates: tuple[str, ...]
    blocks: tuple[BlockBudget, ...]
    linear_constraints: tuple[LinearConstraint, ...]
    cvar_alpha: float
    cvar_limit: float
    min_weight: float | None
    tolerances: Tolerances
    as_of: str
    mapping_version: str
    signature: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "S", _readonly_array(self.S, ndim=2))
        object.__setattr__(self, "M", _readonly_array(self.M, ndim=2))
        object.__setattr__(
            self, "daily_returns", _readonly_array(self.daily_returns, ndim=2)
        )
        object.__setattr__(
            self, "category_returns", _readonly_array(self.category_returns, ndim=2)
        )


@dataclass(frozen=True)
class CompileResult:
    problem: CompiledProblem | None
    issues: tuple[AllocatorIssue, ...] = ()

    @property
    def ok(self) -> bool:
        return self.problem is not None and not self.issues


@dataclass(frozen=True)
class PreflightResult:
    issues: tuple[AllocatorIssue, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.issues


@dataclass(frozen=True)
class SolveResult:
    x: FloatArray
    status: str
    objective: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "x", _readonly_array(self.x, ndim=1))


@dataclass(frozen=True)
class VerificationResult:
    x: FloatArray
    y: FloatArray
    sleeve_weights: Mapping[str, float] = field(default_factory=dict)
    realized_cvar: float | None = None
    issues: tuple[AllocatorIssue, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "x", _readonly_array(self.x, ndim=1))
        object.__setattr__(self, "y", _readonly_array(self.y, ndim=1))
        object.__setattr__(
            self, "sleeve_weights", MappingProxyType(dict(self.sleeve_weights))
        )

    @property
    def ok(self) -> bool:
        return not self.issues
