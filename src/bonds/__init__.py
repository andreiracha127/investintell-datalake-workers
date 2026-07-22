"""Pure, DB-free bond identity / matching / classification library.

Mined from the TRACE 144A bond pilot as production algorithms only: identifier
qualification, exact composite debt classification, observed-panel daily-key
state derivation, and no-look-ahead as-of matching with the full ``MatchState``
precedence and isolated ``latest`` / ``fund_asof`` lanes. None of the pilot's
capability / signing / approval / artifact / workflow machinery is present, and
the package imports no database driver or connection module.
"""

from __future__ import annotations

from .debt_mapping import DebtMapping
from .errors import BondError
from .identifiers import NormalizedCusip, normalize_cusip9
from .matching import (
    CrossSeriesSummary,
    HoldingRecord,
    MatchResult,
    Observation,
    ObservationIndex,
    SeriesMetric,
    WeightState,
    classify_weight,
    compute_cross_series_summary,
    compute_series_metrics,
    match_holding,
    match_holdings_asof,
    validate_match_categories,
)
from .panel_states import ObservedPanel, PanelBuildResult, build_observed_panel_rows
from .states import DebtState, FieldState, IdentifierState, MatchState

__all__ = [
    "BondError",
    "FieldState",
    "IdentifierState",
    "DebtState",
    "MatchState",
    "NormalizedCusip",
    "normalize_cusip9",
    "DebtMapping",
    "PanelBuildResult",
    "ObservedPanel",
    "build_observed_panel_rows",
    "HoldingRecord",
    "Observation",
    "MatchResult",
    "ObservationIndex",
    "SeriesMetric",
    "CrossSeriesSummary",
    "WeightState",
    "classify_weight",
    "match_holding",
    "match_holdings_asof",
    "validate_match_categories",
    "compute_series_metrics",
    "compute_cross_series_summary",
]
