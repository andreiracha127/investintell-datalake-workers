"""NAV glitch sanitizer — repair transient near-zero round-trip prints (Bug 2).

Tiingo occasionally prints a spurious near-zero NAV that round-trips against its
neighbours (e.g. PAAA 19.66 -> 0.02 -> 19.68). Such a print produces an
impossible |log return| (>2.7x/day) and, downstream, a negative multiplier under
prod(1+r). This module detects TRANSIENT dips against a robust local level and
repairs them by log-linear interpolation BEFORE return_1d is computed.

It does NOT invent data: a fund that is genuinely dead (sustained near-zero) or
has a persistent scale step (NAV reported in the wrong units) is FLAGGED for the
eligibility column, not repaired.

Semantics of ``glitch_count``: the number of transient round-trip dips DETECTED
and repaired in this series (one per repaired point). The eligibility flag (the
risk_metrics worker) re-derives the POST-repair residual impossible-print count
from the cleaned series — a fully repaired series has zero residual.
"""

from __future__ import annotations

import datetime as _dt
import math
import statistics
from dataclasses import dataclass

LOW_RATIO = 0.2          # nav < ref * LOW_RATIO => candidate transient dip
WINDOW = 5               # centered window (half=2 each side) for the local median
SCALE_STEP_RATIO = 10.0  # persistent >=10x level shift that never reverts = scale change
GLITCH_LOG = 1.0         # |log return| above this is "impossible" (>2.7x/day)
_DEAD_FRACTION = 0.5     # >= this fraction of points near-zero => dead fund


@dataclass
class SanitizeResult:
    nav: list[float | None]
    repaired: list[bool]
    glitch_count: int
    dead: bool
    scale_step: bool


def _local_ref(values: list[float], i: int) -> float | None:
    """Median of the centered WINDOW excluding index i (positive values only)."""
    half = WINDOW // 2
    lo, hi = max(0, i - half), min(len(values), i + half + 1)
    neigh = [values[j] for j in range(lo, hi) if j != i and values[j] > 0]
    return statistics.median(neigh) if neigh else None


def sanitize_nav_series(
    series: list[tuple[_dt.date, float | None]],
) -> SanitizeResult:
    ordered = sorted(series)
    navs: list[float | None] = [v for _d, v in ordered]
    n = len(navs)
    repaired = [False] * n
    if n == 0:
        return SanitizeResult([], [], 0, False, False)

    positives = [v for v in navs if v is not None and v > 0]
    if not positives:
        return SanitizeResult(navs, repaired, 0, False, False)

    # Dead: a large fraction of the series sits near-zero relative to the series
    # peak. Tiingo's glitches are LOW (near-zero prints), never spuriously high,
    # so the peak is a safe "normal level" reference here — unlike the overall
    # median, which collapses to near-zero precisely when the fund IS dead.
    peak = max(positives)
    near_zero = sum(1 for v in positives if v < peak * LOW_RATIO)
    dead = near_zero >= _DEAD_FRACTION * len(positives)

    # Scale step: a persistent level shift >= SCALE_STEP_RATIO between the first
    # and last thirds that does not revert (median-of-thirds ratio).
    scale_step = False
    if len(positives) >= 6:
        third = len(positives) // 3
        head = statistics.median(positives[:third])
        tail = statistics.median(positives[-third:])
        if head > 0 and tail > 0:
            ratio = max(head / tail, tail / head)
            scale_step = ratio >= SCALE_STEP_RATIO

    glitch_count = 0
    if not dead and not scale_step:
        for i in range(n):
            v = navs[i]
            if v is None or v <= 0:
                continue
            ref = _local_ref([x if x is not None else 0.0 for x in navs], i)
            if ref is not None and ref > 0 and v < ref * LOW_RATIO:
                # transient dip — interpolate from nearest valid non-dip neighbours
                left = _nearest(navs, i, -1, ref)
                right = _nearest(navs, i, +1, ref)
                navs[i] = _interp(left, right, ref)
                repaired[i] = True
                glitch_count += 1

    return SanitizeResult(navs, repaired, glitch_count, dead, scale_step)


def _nearest(navs, i, step, ref):
    j = i + step
    while 0 <= j < len(navs):
        v = navs[j]
        if v is not None and v >= ref * LOW_RATIO:
            return v
        j += step
    return None


def _interp(left, right, ref):
    if left is not None and right is not None:
        return math.exp((math.log(left) + math.log(right)) / 2.0)
    return left if left is not None else (right if right is not None else ref)
