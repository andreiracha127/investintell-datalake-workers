"""Per-axis score: standardize each series (economic transform -> robust-z over
distinct point-in-time vintages), then a weighted sum across an axis's series
(freeze v1 §4 s_a = Σ w_k·z_k).

Owner decision A — TWO stages, NO universal yoy/level: the per-family
economic_transform_id extracts the impulse and the universal standardizer_id
('robust_z_10y_distinct_vintages_v1') makes axes comparable. A series without
enough data for its declared transform standardizes to None (a MISSING input);
coverage (Task 5) penalizes the gap — the score itself is never silently halved
because axis_score renormalizes the weights over the AVAILABLE subset. The
market-implied worker bypasses this module's standardizer (its 126d window return
is already comparable) and feeds axis_score directly.
"""
from __future__ import annotations

import datetime as _dt

from src.macro_sources import MacroSourceSpec
from src.macro_transforms import economic_transform, standardize


def standardized_latest(
    spec: MacroSourceSpec,
    series: dict[_dt.date, float],
    as_of: _dt.date,
    *,
    window_years: int = 10,
) -> float | None:
    """Latest standardized impulse for one macro series at/<= ``as_of``.

    1. economic_transform(spec.economic_transform_id, series, neutral_level=...).
    2. Restrict to transformed periods <= as_of within the trailing window_years.
    3. standardize(spec.standardizer_id, distinct history, latest value).

    Returns None when there is no transformed period <= as_of, or when the robust
    scale is undefined (the caller treats None as a missing input, not a zero).
    """
    transformed = economic_transform(
        spec.economic_transform_id, series, neutral_level=spec.neutral_level)
    cutoff = _dt.date(as_of.year - window_years, as_of.month, 1)
    eligible = [p for p in transformed if cutoff <= p <= as_of]
    if not eligible:
        return None
    latest_period = max(eligible)
    history = [transformed[p] for p in eligible]
    return standardize(spec.standardizer_id, history, transformed[latest_period])


def axis_score(
    weights: dict[str, float], z_by_series: dict[str, float | None]
) -> tuple[float | None, dict[str, float]]:
    """Weighted axis score over the AVAILABLE series.

    Renormalizes the supplied weights over the series with a non-None z, so a
    missing input shifts mass to its peers rather than shrinking the score.
    Returns (score, {series_id: w_k·z_k}); score is None when nothing is available.
    """
    available = {sid: z for sid, z in z_by_series.items()
                 if z is not None and sid in weights}
    total = sum(abs(weights[sid]) for sid in available)
    if total <= 0.0:
        return None, {}
    contributions: dict[str, float] = {}
    score = 0.0
    for sid, z in available.items():
        w = weights[sid] / total
        contrib = w * z
        contributions[sid] = contrib
        score += contrib
    return score, contributions
