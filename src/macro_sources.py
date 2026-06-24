# src/macro_sources.py
"""Macro source registry for the macro quadrant (model_version macro_quadrant_us_v1).

A LEAN, auditable basket: 3-5 families per axis, point-in-time reconstructible via
ALFRED. Weights/direction/transform here are SEEDS to be calibrated in A3 (against
abstention/flip/vintage-stability — never against return), not final parameters.
The same dataclass also describes daily market sources (revision_policy='none'),
but A1 only populates the macro basket.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

SOURCE_SPEC_VERSION = "macro_quadrant_us_v1.0"


@dataclass(frozen=True)
class MacroSourceSpec:
    source_id: str
    series_id: str
    axis: Literal["growth", "inflation"]
    family: str
    transform_id: str
    direction: Literal[-1, 1]
    weight: float
    cadence: Literal["daily", "weekly", "monthly", "quarterly"]
    release_calendar_id: str | None
    revision_policy: Literal["none", "vintage"]
    grace_period: timedelta
    hard_max_age: timedelta
    critical: bool
    minimum_history: int
    source_spec_version: str = SOURCE_SPEC_VERSION


def _macro(series_id, axis, family, weight, *, direction=1, transform="yoy",
           cadence="monthly", critical=True):
    return MacroSourceSpec(
        source_id=f"alfred:{series_id}", series_id=series_id, axis=axis, family=family,
        transform_id=transform, direction=direction, weight=weight, cadence=cadence,
        release_calendar_id=None, revision_policy="vintage",
        grace_period=timedelta(days=7), hard_max_age=timedelta(days=45),
        critical=critical, minimum_history=24,
    )


SEED_SOURCES: tuple[MacroSourceSpec, ...] = (
    # growth axis (4 families)
    _macro("INDPRO", "growth", "activity_production", 0.25),
    _macro("PCEC96", "growth", "real_consumption", 0.25),
    _macro("PAYEMS", "growth", "labor", 0.25),
    _macro("ACOGNO", "growth", "new_orders_leading", 0.25),
    # inflation axis (4 families)
    _macro("CPILFESL", "inflation", "core_inflation", 0.30),
    _macro("PPIFIS", "inflation", "upstream_prices", 0.25),
    _macro("AHETPI", "inflation", "wages", 0.25),
    _macro("MICH", "inflation", "inflation_expectations", 0.20),
)


def axis_weights(axis: str) -> dict[str, float]:
    """Per-axis weights normalized to sum 1 (over series_id)."""
    specs = [s for s in SEED_SOURCES if s.axis == axis]
    total = sum(abs(s.weight) for s in specs)
    if total <= 0:
        raise ValueError(f"axis {axis}: non-positive weight total")
    return {s.series_id: s.weight / total for s in specs}
