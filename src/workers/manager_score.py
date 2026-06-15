"""Equity composite manager_score — pure scoring math (no I/O).

Ported from the legacy allocation engine's scoring_service.py
(``_normalize_with_provenance`` lines 274-287, ``_peaked_score`` lines 266-271,
``_resolve_sharpe_input`` lines 307-334, ``_compute_equity_score`` bounds lines
720-742) and trimmed to the four legacy-weighted risk components that are
derivable from a fund_risk_metrics row:

    return_consistency   <- return_1y                       [-0.20, 0.40]
    risk_adjusted_return <- sharpe_cf (robust) else sharpe_1y [-1.0, 3.0]
    drawdown_control     <- max_drawdown_1y                 [-0.50, 0.0]
    information_ratio    <- information_ratio_1y            [-1.0, 2.0]

The legacy composite (``_DEFAULT_SCORING_WEIGHTS`` lines 94-101) also weights
``flows_momentum`` (0.10) and ``fee_efficiency`` (0.10); fund_risk_metrics
carries NEITHER a flows signal NOR an expense ratio, so both are dropped and
the remaining four risk weights (0.20/0.25/0.20/0.15, sum 0.80) are
renormalized /0.80 to sum to 1.0. There is no separate "robust_sharpe"
component — the robust Sharpe (Cornish-Fisher ``sharpe_cf``, falling back to
``sharpe_1y``) IS the input to ``risk_adjusted_return``, exactly as legacy
``_resolve_sharpe_input`` resolves it with use_robust_sharpe=True.

Missing inputs get a peer-median opacity penalty (peer_median - 5, floored at
0) so opaque/short-history funds rank below transparent peers with mediocre
metrics, exactly as the legacy engine does.

This module is EQUITY-ONLY: callers must gate on asset_class == 'equity'
before invoking. FI/cash/alternatives have their own (un-ported) models.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

# Legacy risk weights (scoring_service._DEFAULT_SCORING_WEIGHTS lines 94-101)
# minus flows_momentum (0.10) and fee_efficiency (0.10), renormalized /0.80.
#   return_consistency   0.20 / 0.80 = 0.25
#   risk_adjusted_return 0.25 / 0.80 = 0.3125
#   drawdown_control     0.20 / 0.80 = 0.25
#   information_ratio    0.15 / 0.80 = 0.1875   (sum = 1.0)
EQUITY_MANAGER_SCORE_WEIGHTS: dict[str, float] = {
    "return_consistency": 0.25,
    "risk_adjusted_return": 0.3125,
    "drawdown_control": 0.25,
    "information_ratio": 0.1875,
}


@dataclass(frozen=True, slots=True)
class ManagerScoreResult:
    """Composite 0-100 manager_score with provenance."""

    score: float
    components: dict[str, float] = field(default_factory=dict)
    degraded: bool = False
    degraded_components: list[str] = field(default_factory=list)


def normalize_with_provenance(
    value: float | None,
    min_val: float,
    max_val: float,
    peer_median: float | None = None,
) -> tuple[float, bool]:
    """Normalize value to 0-100; returns (score, was_synthesized).

    Direct port of scoring_service._normalize_with_provenance. Missing/non-finite
    -> (peer_median - 5, True) when a peer_median is given (opacity penalty,
    floored at 0), else (45.0, True). A degenerate range (max == min) returns
    (50.0, False).
    """
    if value is None or not math.isfinite(value):
        if peer_median is not None:
            return max(0.0, min(100.0, peer_median - 5.0)), True
        return 45.0, True
    if max_val == min_val:
        return 50.0, False
    return max(0.0, min(100.0, (value - min_val) / (max_val - min_val) * 100.0)), False


def peaked_score(value: float | None, target: float, half_range: float) -> float:
    """100 at value==target, decays linearly to 0 at |value-target| >= half_range.

    Direct port of scoring_service._peaked_score. Missing/non-finite -> 45.0
    (neutral-below-midpoint, legacy convention). Retained for parity with the
    legacy helper set; not used by the four-component equity composite below.
    """
    if value is None or not math.isfinite(value):
        return 45.0
    distance = abs(value - target)
    return max(0.0, 100.0 * (1.0 - distance / half_range))


def _resolve_sharpe(metrics: dict[str, float | None]) -> float | None:
    """Robust Sharpe: sharpe_cf preferred, fall back to sharpe_1y when absent.

    Mirrors scoring_service._resolve_sharpe_input (lines 307-334) with the
    use_robust_sharpe path active.
    """
    cf = metrics.get("sharpe_cf")
    if cf is not None and math.isfinite(float(cf)):
        return float(cf)
    s1 = metrics.get("sharpe_1y")
    return float(s1) if s1 is not None and math.isfinite(float(s1)) else None


def compute_equity_manager_score(
    metrics: dict[str, float | None],
    peer_medians: dict[str, float] | None = None,
) -> ManagerScoreResult:
    """Composite equity manager_score from a fund_risk_metrics-shaped dict.

    ``metrics`` keys consumed: return_1y, sharpe_1y, sharpe_cf,
    max_drawdown_1y, information_ratio_1y. ``peer_medians`` maps component
    name -> peer-median sub-score (0-100) used for the opacity penalty on
    missing inputs.
    """
    pm = peer_medians or {}
    components: dict[str, float] = {}
    synthesized: list[str] = []

    def _num(key: str) -> float | None:
        v = metrics.get(key)
        return float(v) if v is not None and math.isfinite(float(v)) else None

    # return_consistency: trailing 1y return, [-0.20, 0.40].
    val, synth = normalize_with_provenance(
        _num("return_1y"), -0.20, 0.40, pm.get("return_consistency")
    )
    components["return_consistency"] = round(val, 2)
    if synth:
        synthesized.append("return_consistency")

    # risk_adjusted_return: robust Sharpe (cf preferred, sharpe_1y fallback),
    # [-1.0, 3.0]. Synthesized only when BOTH sharpe inputs are missing.
    sharpe = _resolve_sharpe(metrics)
    val, synth = normalize_with_provenance(
        sharpe, -1.0, 3.0, pm.get("risk_adjusted_return")
    )
    components["risk_adjusted_return"] = round(val, 2)
    if synth:
        synthesized.append("risk_adjusted_return")

    # drawdown_control: max_drawdown_1y (negative fraction), [-0.50, 0.0].
    val, synth = normalize_with_provenance(
        _num("max_drawdown_1y"), -0.50, 0.0, pm.get("drawdown_control")
    )
    components["drawdown_control"] = round(val, 2)
    if synth:
        synthesized.append("drawdown_control")

    # information_ratio: [-1.0, 2.0].
    val, synth = normalize_with_provenance(
        _num("information_ratio_1y"), -1.0, 2.0, pm.get("information_ratio")
    )
    components["information_ratio"] = round(val, 2)
    if synth:
        synthesized.append("information_ratio")

    score = sum(components[k] * w for k, w in EQUITY_MANAGER_SCORE_WEIGHTS.items())
    # Match legacy: only flag degraded for components with positive weight.
    weighted_synth = [
        name for name in synthesized
        if EQUITY_MANAGER_SCORE_WEIGHTS.get(name, 0.0) > 0.0
    ]
    return ManagerScoreResult(
        score=round(score, 2),
        components=components,
        degraded=len(weighted_synth) > 0,
        degraded_components=weighted_synth,
    )
