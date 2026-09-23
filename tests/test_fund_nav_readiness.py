"""Synthetic daily NAV contract, no external provider and no calendar inference."""

from __future__ import annotations

import datetime as dt
import uuid
from contextlib import contextmanager

import pytest

from src.workers._tiingo import NavObservation
from src.workers._nav_sanitize import REPAIRED_NAV_KINDS
from src.workers import nav_current_daily_chain as chain
from src.workers.fund_nav_readiness import assess_instrument, sample_id
from src.workers.instrument_ingestion import build_rows

HOLIDAY = dt.date(2026, 9, 7)


def _grid(end: dt.date = dt.date(2026, 9, 8)) -> list[dt.date]:
    out = []
    d = end
    while len(out) < 401:
        if d.weekday() < 5 and d != HOLIDAY:
            out.append(d)
        d -= dt.timedelta(days=1)
    return list(reversed(out))


def _case():
    grid = _grid()
    policy = {
        "policy_id": "synthetic",
        "policy_version": "v1",
        "policy_hash": "f" * 64,
        "calendar_id": "NYSE-TEST",
        "calendar_version": "v1",
        "calendar_source": "fixture-closed-sessions",
        "required_nav_kind": "adjusted",
        "required_return_semantics": "observed_interval_log_ratio",
        "modeling_currency": "USD",
    }
    observations = tuple(
        NavObservation(d, round(100 + i * 0.01, 6), "adjusted")
        for i, d in enumerate(grid)
    )
    rows = build_rows(
        observations,
        [(uuid.uuid4(), "USD")],
        calendar={d: ("NYSE-TEST", "v1", "fixture-closed-sessions") for d in grid},
    )
    lifecycle = {
        "evidence_id": uuid.uuid4(),
        "fund_status": "ACTIVE",
        "valuation_frequency": "daily",
        "identity_verified": True,
        "return_basis_verified": True,
        "currency_verified": True,
        "known_at": dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
        "effective_at": dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
    }
    feature = {
        "risk_run_id": uuid.uuid4(),
        "input_fingerprint": "a" * 64,
        "calc_date": grid[-1],
        "feature_as_of": grid[-1],
        "input_max_date": grid[-1],
        "exclusion_reason": None,
    }
    attempt = {"run_id": uuid.uuid4(), "status": "success_new"}
    return policy, grid, rows, lifecycle, feature, attempt


def _assess(
    policy,
    grid,
    rows,
    lifecycle,
    feature,
    attempt,
    *,
    last=None,
    closed=None,
    input_matches=True,
    hold=False,
    revision_verified=True,
):
    return assess_instrument(
        "fund-1",
        policy,
        grid,
        rows,
        grid[-1] if last is None else last,
        grid[-1] if closed is None else closed,
        lifecycle,
        attempt,
        feature,
        input_matches,
        reexpression_hold=hold,
        revision_source_verified=revision_verified,
    )


def test_exact_401_endpoints_400_returns_over_weekend_and_holiday():
    policy, grid, rows, lifecycle, feature, attempt = _case()
    assert grid[-2:] == [dt.date(2026, 9, 4), dt.date(2026, 9, 8)]
    assert rows[-1]["return_start_date"] == grid[-2]
    result = _assess(policy, grid, rows, lifecycle, feature, attempt)
    assert result["admissible"] and result["reason_code"] is None
    assert result["observed_levels_count"] == 401
    assert result["admissible_returns_count"] == 400
    assert result["missed_due_sessions"] == 0
    assert sample_id(policy, grid) == sample_id(policy, list(grid))


@pytest.mark.parametrize("frequency", ["weekly", "monthly", "unknown"])
def test_no_resampling_of_nondaily_fund(frequency):
    p, g, r, lifecycle, f, a = _case()
    lifecycle["valuation_frequency"] = frequency
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "UNSUPPORTED_VALUATION_FREQUENCY"
    )


def test_current_gap_is_never_dropped_or_forward_filled():
    p, g, r, lifecycle, f, a = _case()
    missing = g[-20]
    r = [row for row in r if row["nav_date"] != missing]
    result = _assess(p, g, r, lifecycle, f, a)
    assert result["reason_code"] == "RETURN_INTERVAL_INCOMPATIBLE"
    assert result["missing_session_count"] == 1
    assert result["missed_due_sessions"] == 0
    assert len(g) == 401


def test_400_levels_are_only_399_returns():
    p, g, r, lifecycle, f, a = _case()
    result = _assess(p, g, r[1:], lifecycle, f, a)
    assert result["reason_code"] == "RETURN_INTERVAL_INCOMPATIBLE"
    assert result["observed_levels_count"] == 400
    assert not result["admissible"]


def test_29_march_15_to_18_vs_one_march_14_to_18_is_not_a_daily_cohort():
    p, g, r, lifecycle, f, a = _case()
    grid = _grid(dt.date(2024, 3, 18))
    assert grid[-3:] == [
        dt.date(2024, 3, 14),
        dt.date(2024, 3, 15),
        dt.date(2024, 3, 18),
    ]
    r = build_rows(
        tuple(
            NavObservation(day, round(100 + i * 0.01, 6), "adjusted")
            for i, day in enumerate(grid)
        ),
        [(uuid.uuid4(), "USD")],
        calendar={
            day: (p["calendar_id"], p["calendar_version"], p["calendar_source"])
            for day in grid
        },
    )
    f["calc_date"] = f["feature_as_of"] = f["input_max_date"] = grid[-1]
    assert all(_assess(p, grid, r, lifecycle, f, a)["admissible"] for _ in range(29))
    r[-1]["return_start_date"] = grid[-3]
    mismatched = _assess(p, grid, r, lifecycle, f, a)
    assert mismatched["admissible"] is False
    assert mismatched["reason_code"] == "RETURN_INTERVAL_INCOMPATIBLE"


def test_inactive_fresh_unknown_active_stale_and_future():
    p, g, r, lifecycle, f, a = _case()
    lifecycle["fund_status"] = "INACTIVE"
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "INACTIVE_FUND"
    lifecycle["fund_status"] = "UNKNOWN"
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "UNKNOWN_FUND_STATUS"
    lifecycle["fund_status"] = "ACTIVE"
    assert (
        _assess(p, g, r[:-1], lifecycle, f, a, last=g[-2])["reason_code"] == "NAV_STALE"
    )
    assert (
        _assess(p, g, r, lifecycle, f, a, last=g[-1] + dt.timedelta(days=1))[
            "reason_code"
        ]
        == "NAV_DATA_UNAVAILABLE"
    )


def test_extra_observed_nav_is_allowed_only_through_pinned_closed_session():
    p, g, r, lifecycle, f, a = _case()
    next_closed = g[-1] + dt.timedelta(days=1)
    assert _assess(p, g, r, lifecycle, f, a, last=next_closed, closed=next_closed)[
        "admissible"
    ]
    assert (
        _assess(p, g, r, lifecycle, f, a, last=next_closed, closed=g[-1])["reason_code"]
        == "NAV_DATA_UNAVAILABLE"
    )


def test_no_attempt_and_provider_error_are_not_success():
    p, g, r, lifecycle, f, a = _case()
    assert _assess(p, g, r, lifecycle, f, None)["reason_code"] == "NAV_DATA_UNAVAILABLE"
    a["status"] = "transient_error"
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "NAV_DATA_UNAVAILABLE"


def test_source_boundary_repair_and_predecessor_mismatch_are_rejected():
    p, g, r, lifecycle, f, a = _case()
    r[-1]["source"] = "yahoo"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "RETURN_INTERVAL_INCOMPATIBLE"
    )


@pytest.mark.parametrize("kind", sorted(REPAIRED_NAV_KINDS))
def test_all_legacy_and_current_repair_kinds_fail_current_daily(kind):
    p, g, r, lifecycle, f, a = _case()
    r[-2]["nav_repair_kind"] = kind
    r[-1]["return_uses_repaired_nav"] = True
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "RETURN_INTERVAL_INCOMPATIBLE"
    )


def test_adjusted_overlap_hold_remains_incompatible_even_after_all_returns_recomputed():
    p, g, r, lifecycle, f, a = _case()
    assert _assess(p, g, r, lifecycle, f, a)["admissible"]
    assert _assess(p, g, r, lifecycle, f, a, hold=True)["reason_code"] == (
        "RETURN_INTERVAL_INCOMPATIBLE"
    )


def test_unattributed_nav_revision_requires_completed_source_run():
    p, g, r, lifecycle, f, a = _case()
    assert _assess(p, g, r, lifecycle, f, a, revision_verified=False)[
        "reason_code"
    ] == ("NAV_DATA_UNAVAILABLE")
    r[-1]["source"] = "tiingo"
    r[-1]["nav_repair_kind"] = "centered_interpolation_v1"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "RETURN_INTERVAL_INCOMPATIBLE"
    )
    r[-1]["nav_repair_kind"] = "none"
    r[-1]["return_start_date"] = g[-3]
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "RETURN_INTERVAL_INCOMPATIBLE"
    )


def test_risk_input_older_than_current_or_modified_after_calculation():
    p, g, r, lifecycle, f, a = _case()
    f["input_max_date"] = g[-2]
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"] == "RETURN_SAMPLE_NOT_CURRENT"
    )
    f["input_max_date"] = g[-1]
    assert (
        _assess(p, g, r, lifecycle, f, a, input_matches=False)["reason_code"]
        == "RETURN_SAMPLE_NOT_CURRENT"
    )


def test_raw_unknown_or_unproven_identity_never_admissible():
    p, g, r, lifecycle, f, a = _case()
    lifecycle["return_basis_verified"] = False
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "NAV_DATA_UNAVAILABLE"
    lifecycle["return_basis_verified"] = True
    r[-1]["source_nav_kind"] = "raw"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    )
    r[-1]["source_nav_kind"] = "unknown"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    )


def test_non_usd_without_fx_and_wrong_return_type_are_not_ready():
    p, g, r, lifecycle, f, a = _case()
    r[-1]["currency"] = "EUR"
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "NAV_DATA_UNAVAILABLE"
    r[-1]["currency"] = "USD"
    r[-1]["return_type"] = "arithmetic"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    )


def test_ordered_chain_publishes_only_after_coverage_and_risk(monkeypatch):
    events = []

    class Guard:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    @contextmanager
    def lock(*_args):
        yield True

    monkeypatch.setattr(chain, "connect", lambda *_args: Guard())
    monkeypatch.setattr(chain, "advisory_lock", lock)
    monkeypatch.setattr(
        chain, "_due_session", lambda *_args: events.append("policy") or "2026-09-08"
    )
    monkeypatch.setattr(
        chain.matview_refresh,
        "_refresh_all",
        lambda *_args: events.append("coverage") or ["fund_nav_coverage_mv"],
    )

    def ingest(*_args, **_kwargs):
        events.append("ingest")
        assert _kwargs["target_session"] == dt.date(2026, 9, 8)
        return {"ingestion_run_id": "ingested"}

    def risk(*_args, **_kwargs):
        events.append("risk")
        return {"risk_run_id": "risk", "mv_refreshed": True}

    def publish(*_args):
        events.append("readiness")
        return {
            "state": "complete",
            "published": True,
            "run_id": "readiness",
            "sample_id": "s" * 64,
            "ready_count": 1,
        }

    stats = chain.run(
        "unused", ingestion_runner=ingest, risk_runner=risk, readiness_runner=publish
    )
    assert events == ["policy", "ingest", "coverage", "risk", "readiness"]
    assert stats["published"] and stats["readiness_run_id"] == "readiness"

    events.clear()

    def stale_risk(*_args, **_kwargs):
        events.append("risk")
        return {"mv_refreshed": False}

    with pytest.raises(RuntimeError, match="risk publication incomplete"):
        chain.run(
            "unused",
            ingestion_runner=ingest,
            risk_runner=stale_risk,
            readiness_runner=publish,
        )
    assert events == ["policy", "ingest", "coverage", "risk"]
