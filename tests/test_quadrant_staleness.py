# tests/test_quadrant_staleness.py
from __future__ import annotations

import datetime as dt

from src.quadrant_staleness import (
    add_business_days,
    available_at_snapshot,
    compute_stale_after,
    source_deadlines,
    source_expiry,
)

UTC = dt.timezone.utc


def test_add_business_days_skips_weekend() -> None:
    fri = dt.datetime(2024, 3, 1, 12, 0, tzinfo=UTC)  # Friday
    assert add_business_days(fri, 1) == dt.datetime(2024, 3, 4, 12, 0, tzinfo=UTC)  # Mon
    assert add_business_days(fri, 2) == dt.datetime(2024, 3, 5, 12, 0, tzinfo=UTC)  # Tue


def test_available_at_is_max_of_computed_and_inputs() -> None:
    computed = dt.datetime(2024, 3, 5, tzinfo=UTC)
    inputs = [dt.datetime(2024, 3, 4, tzinfo=UTC), dt.datetime(2024, 3, 6, tzinfo=UTC)]
    assert available_at_snapshot(computed, inputs) == dt.datetime(2024, 3, 6, tzinfo=UTC)


def test_available_at_falls_back_to_computed_when_no_inputs() -> None:
    computed = dt.datetime(2024, 3, 5, tzinfo=UTC)
    assert available_at_snapshot(computed, []) == computed


def test_source_deadlines_soft_and_hard() -> None:
    av = dt.datetime(2024, 3, 1, tzinfo=UTC)
    nxt = dt.datetime(2024, 3, 20, tzinfo=UTC)
    # soft = release + grace(7d) = Mar 27.
    # hard = min(available+hard_max(45d)=Apr 15, soft+decay(14d)=Apr 10) = Apr 10.
    soft, hard = source_deadlines(av, nxt, dt.timedelta(days=7),
                                  dt.timedelta(days=45), dt.timedelta(days=14))
    assert soft == dt.datetime(2024, 3, 27, tzinfo=UTC)
    assert hard == dt.datetime(2024, 4, 10, tzinfo=UTC)


def test_source_expiry_equals_hard_deadline() -> None:
    av = dt.datetime(2024, 3, 1, tzinfo=UTC)
    nxt = dt.datetime(2024, 3, 20, tzinfo=UTC)
    # source_expiry == hard_deadline of source_deadlines (binds data_stale_after).
    e = source_expiry(av, nxt, dt.timedelta(days=7), dt.timedelta(days=45),
                      dt.timedelta(days=14))
    _, hard = source_deadlines(av, nxt, dt.timedelta(days=7),
                               dt.timedelta(days=45), dt.timedelta(days=14))
    assert e == hard == dt.datetime(2024, 4, 10, tzinfo=UTC)


def test_compute_stale_after_is_min_of_data_and_pipeline() -> None:
    computed = dt.datetime(2024, 3, 1, 9, 0, tzinfo=UTC)  # Friday
    data_exp = dt.datetime(2024, 3, 20, tzinfo=UTC)       # far
    data, pipeline, stale = compute_stale_after(computed, [data_exp])
    # pipeline = computed + 2 business days = Tue Mar 5
    assert pipeline == dt.datetime(2024, 3, 5, 9, 0, tzinfo=UTC)
    assert data == data_exp
    assert stale == pipeline  # pipeline is the binding (earlier) one here
