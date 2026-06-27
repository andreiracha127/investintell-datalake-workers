# src/quadrant_staleness.py
"""available_at (§8) and stale_after (§9) + the freshness soft/hard split (owner
decision D).

available_at_snapshot = max(computed_at, max_j available_at_j) over required inputs.
Per critical source j (owner decision D):
  soft_deadline_j = next_expected_release_j + grace_j
  hard_deadline_j = min(last_available_at_j + hard_max_age_j,
                        soft_deadline_j + freshness_decay_window_j)
freshness_value (Task 5) is 1 before soft, decays linearly to 0 between soft and
hard, and is a HARD gate (-> stale) at/after hard. source_expiry_j == hard_deadline_j
binds data_stale_after = min over critical expiries.
pipeline_stale_after = computed_at + 2 business days. stale_after = min(both); the
two components are persisted SEPARATELY.

Business days = Mon-Fri count, NO holiday calendar in v1 (a documented A3
calibration point — exchange holidays would tighten the market model's 3-bd
hard_max_age; for the macro monthly basket the 45-calendar-day hard_max dominates).
"""
import datetime as _dt


def add_business_days(start: _dt.datetime, n: int) -> _dt.datetime:
    """``start`` plus ``n`` business days (Mon-Fri), preserving time-of-day."""
    current = start
    added = 0
    while added < n:
        current = current + _dt.timedelta(days=1)
        if current.weekday() < 5:  # 0=Mon .. 4=Fri
            added += 1
    return current


def available_at_snapshot(
    computed_at: _dt.datetime, input_available_ats: list[_dt.datetime]
) -> _dt.datetime:
    """§8: max(computed_at, max_j available_at_j). Falls back to computed_at."""
    if not input_available_ats:
        return computed_at
    return max([computed_at, *input_available_ats])


def source_deadlines(
    last_available_at: _dt.datetime,
    next_expected_release: _dt.datetime,
    grace: _dt.timedelta,
    hard_max_age: _dt.timedelta,
    freshness_decay_window: _dt.timedelta,
) -> tuple[_dt.datetime, _dt.datetime]:
    """Owner decision D — (soft_deadline, hard_deadline) for one source.

    soft = next_expected_release + grace.
    hard = min(last_available_at + hard_max_age, soft + freshness_decay_window).
    These feed freshness_value (Task 5) and bind data_stale_after (hard).
    """
    soft = next_expected_release + grace
    hard = min(last_available_at + hard_max_age, soft + freshness_decay_window)
    return soft, hard


def source_expiry(
    last_available_at: _dt.datetime,
    next_expected_release: _dt.datetime,
    grace: _dt.timedelta,
    hard_max_age: _dt.timedelta,
    freshness_decay_window: _dt.timedelta,
) -> _dt.datetime:
    """§9 expiry == the hard_deadline (owner decision D)."""
    _, hard = source_deadlines(
        last_available_at, next_expected_release, grace, hard_max_age,
        freshness_decay_window)
    return hard


def compute_stale_after(
    computed_at: _dt.datetime, critical_expiries: list[_dt.datetime]
) -> tuple[_dt.datetime, _dt.datetime, _dt.datetime]:
    """§9: (data_stale_after, pipeline_stale_after, stale_after).

    Requires at least one critical expiry. pipeline = computed_at + 2 business days.
    """
    if not critical_expiries:
        raise ValueError("at least one critical source expiry is required")
    data_stale_after = min(critical_expiries)
    pipeline_stale_after = add_business_days(computed_at, 2)
    stale_after = min(data_stale_after, pipeline_stale_after)
    return data_stale_after, pipeline_stale_after, stale_after
