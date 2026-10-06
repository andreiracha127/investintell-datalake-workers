"""Coverage alarm for the current-daily NAV publication.

A readiness snapshot can publish correctly while admitting almost no funds:
until 2026-10-06 two legacy catalog defects left 65% of the fund cohort UNKNOWN
and missing NAV lineage left zero funds admissible, and nothing failed. The
chain now measures two shares after every publication and fails the run (the
publication itself stays) when either falls below its floor:

* ``active_share`` = ACTIVE lifecycle evidence / readiness cohort
  (``funds_profile_mv``) — identity and catalog health;
* ``ready_share`` = admissible / ACTIVE — NAV ingestion and lineage health.

Floors come from ``NAV_COVERAGE_MIN_ACTIVE_SHARE`` and
``NAV_COVERAGE_MIN_READY_SHARE`` (fractions in [0, 1]); an unset variable uses
the default, a malformed one is a configuration error raised before any work.
"""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from typing import Any

DEFAULT_MIN_ACTIVE_SHARE = 0.85
DEFAULT_MIN_READY_SHARE = 0.80
ENV_MIN_ACTIVE_SHARE = "NAV_COVERAGE_MIN_ACTIVE_SHARE"
ENV_MIN_READY_SHARE = "NAV_COVERAGE_MIN_READY_SHARE"


def _floor(env: Mapping[str, str], name: str, default: float) -> float:
    raw = (env.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name}={raw!r} is not a number") from None
    if not math.isfinite(value) or not 0.0 <= value <= 1.0:
        raise ValueError(f"{name}={raw!r} must be a fraction between 0 and 1")
    return value


def floors_from_env(env: Mapping[str, str] | None = None) -> tuple[float, float]:
    """``(min_active_share, min_ready_share)``; raises on a malformed value."""
    env = os.environ if env is None else env
    return (
        _floor(env, ENV_MIN_ACTIVE_SHARE, DEFAULT_MIN_ACTIVE_SHARE),
        _floor(env, ENV_MIN_READY_SHARE, DEFAULT_MIN_READY_SHARE),
    )


def assess_coverage(
    snapshot: Mapping[str, Any], *, min_active_share: float, min_ready_share: float
) -> dict[str, Any]:
    """Coverage of one published readiness snapshot; ``alarm`` when below a floor.

    Missing counts are themselves an alarm: a publication that cannot say how
    many funds it admitted must not paint the run green.
    """
    counts = {
        key: snapshot.get(key) for key in ("instrument_count", "active_count", "ready_count")
    }
    result: dict[str, Any] = {
        **counts,
        "min_active_share": min_active_share,
        "min_ready_share": min_ready_share,
    }
    if any(not isinstance(value, int) or value < 0 for value in counts.values()):
        return {**result, "active_share": None, "ready_share": None, "alarm": True,
                "breaches": ["COVERAGE_COUNTS_MISSING"]}
    cohort, active, ready = (
        counts["instrument_count"], counts["active_count"], counts["ready_count"]
    )
    active_share = active / cohort if cohort else 0.0
    ready_share = ready / active if active else 0.0
    breaches = []
    if active_share < min_active_share:
        breaches.append("ACTIVE_SHARE_BELOW_FLOOR")
    if ready_share < min_ready_share:
        breaches.append("READY_SHARE_BELOW_FLOOR")
    return {
        **result,
        "active_share": round(active_share, 4),
        "ready_share": round(ready_share, 4),
        "alarm": bool(breaches),
        "breaches": breaches,
    }
