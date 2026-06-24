from __future__ import annotations

import datetime as dt
import uuid as _uuid

from src.quadrant_snapshot import (
    REGIME_SNAPSHOT_NAMESPACE,
    AxisDiagnostics,
    QuadrantSnapshot,
    effective_status,
    make_snapshot_id,
)


def _axis(sign=1) -> AxisDiagnostics:
    return AxisDiagnostics(
        score=0.3, sign=sign, internal_sign=sign, candidate_confidence=0.9, margin=0.3,
        uncertainty_raw=0.1, uncertainty_adjusted=0.1,
    )


def _valid_snapshot(stale_after: dt.datetime) -> QuadrantSnapshot:
    av = dt.datetime(2024, 3, 5, tzinfo=dt.timezone.utc)
    sid = make_snapshot_id("macro_quadrant_us_v1", dt.date(2024, 3, 4),
                           "abcdef0123456789", None)
    return QuadrantSnapshot(
        snapshot_id=sid, previous_snapshot_id=None,
        quadrant="expansion", candidate_quadrant="expansion",
        candidate_confidence=0.88, growth=_axis(1), inflation=_axis(1),
        coverage_quality=1.0, freshness_quality=1.0, source_health_quality=1.0,
        transition_pending=False, transition_reason=None,
        as_of=dt.date(2024, 3, 4), available_at=av, computed_at=av,
        data_stale_after=stale_after, pipeline_stale_after=stale_after,
        stale_after=stale_after, status_at_compute="valid",
        model_version="macro_quadrant_us_v1",
        confidence_model_version="confidence_v1.0",
        confidence_method="rolling_score_mad_distinct_vintages_v1",
        source_vintage_hash="abcdef0123456789",
    )


def test_snapshot_id_is_deterministic_uuid5() -> None:
    sid = make_snapshot_id("macro_quadrant_us_v1", dt.date(2024, 3, 4),
                           "abcdef0123456789", None)
    # uuid5 over the canonical "|"-joined key with GENESIS for a null predecessor.
    expect = str(_uuid.uuid5(
        REGIME_SNAPSHOT_NAMESPACE,
        "macro_quadrant_us_v1|2024-03-04|abcdef0123456789|GENESIS"))
    assert sid == expect
    # same inputs + same predecessor -> same id (idempotent daily recompute)
    assert sid == make_snapshot_id("macro_quadrant_us_v1", dt.date(2024, 3, 4),
                                   "abcdef0123456789", None)
    # a DIFFERENT predecessor yields a DIFFERENT id (latched chain identity)
    other = make_snapshot_id("macro_quadrant_us_v1", dt.date(2024, 3, 4),
                             "abcdef0123456789", sid)
    assert other != sid


def test_effective_status_valid_before_stale() -> None:
    far = dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc)
    snap = _valid_snapshot(far)
    now = dt.datetime(2024, 3, 10, tzinfo=dt.timezone.utc)
    assert effective_status(snap, now) == "valid"


def test_effective_status_becomes_stale_after_stale_after() -> None:
    cutoff = dt.datetime(2024, 3, 8, tzinfo=dt.timezone.utc)
    snap = _valid_snapshot(cutoff)
    now = dt.datetime(2024, 3, 8, tzinfo=dt.timezone.utc)  # now >= stale_after
    assert effective_status(snap, now) == "stale"


def test_effective_status_passthrough_when_not_valid() -> None:
    snap = _valid_snapshot(dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc))
    low = snap.__class__(**{**snap.__dict__, "status_at_compute": "low_confidence",
                            "quadrant": None})
    now = dt.datetime(2024, 5, 1, tzinfo=dt.timezone.utc)  # well past stale_after
    # non-valid statuses are never relabelled to 'stale'
    assert effective_status(low, now) == "low_confidence"


def test_axis_diagnostics_allows_all_none() -> None:
    a = AxisDiagnostics(score=None, sign=None, internal_sign=None,
                        candidate_confidence=None, margin=None,
                        uncertainty_raw=None, uncertainty_adjusted=None)
    assert a.sign is None and a.internal_sign is None
