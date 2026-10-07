"""The fund chain certifies broad rolling cohorts, not a singleton max date."""

from __future__ import annotations

import datetime as dt

import pytest

from src.workers import _fund_pipeline_freshness as freshness

NOW = dt.date(2026, 10, 7)
LOADED = dt.datetime(2026, 10, 6, 12, tzinfo=dt.UTC)


def _row(series, day, count=4, loaded=LOADED, equity=True):
    return (series, dt.date.fromisoformat(day), count, loaded, equity)


def _cohort(size=100):
    raw = [_row(f"S{i}", "2026-07-31") for i in range(size)]
    prior = [(f"S{i}", dt.date(2026, 4, 30), 4) for i in range(size)]
    return freshness.source_from_rows(
        raw, prior, raw_max=dt.date(2026, 7, 31), today=NOW,
        policy=freshness.Policy(min_full_month_series=1),
    )


def test_staggered_quarterly_filers_form_one_valid_rolling_cohort():
    raw = [
        _row(f"S{i}", day)
        for begin, end, day in [
            (0, 2505, "2026-05-31"),
            (2505, 9531, "2026-06-30"),
            (9531, 13725, "2026-07-31"),
        ]
        for i in range(begin, end)
    ]
    raw.append(_row("S0", "2026-08-31"))
    prior = [(f"S{i}", dt.date(2026, 4, 30), 4) for i in range(13725)]
    source = freshness.source_from_rows(raw, prior, raw_max=dt.date(2026, 8, 31), today=NOW)
    assert source.verdict["alarm"] is False
    assert source.as_of == dt.date(2026, 7, 31)
    assert source.raw_max == dt.date(2026, 8, 31)
    assert len(source.series) == 13725
    assert source.series["S0"].report_date == dt.date(2026, 5, 31)


def test_new_series_cannot_mask_the_loss_of_previous_series():
    raw = [_row(f"S{i}", "2026-07-31") for i in range(80)]
    raw += [_row(f"N{i}", "2026-07-31") for i in range(30)]
    prior = [(f"S{i}", dt.date(2026, 4, 30), 4) for i in range(100)]
    source = freshness.source_from_rows(
        raw, prior, raw_max=dt.date(2026, 7, 31), today=NOW,
        policy=freshness.Policy(min_full_month_series=1),
    )
    assert source.verdict["alarm"] is True
    assert "SOURCE_COHORT_RETENTION_BELOW_FLOOR" in source.verdict["breaches"]


def test_old_full_cohort_is_stale_even_with_a_new_singleton():
    raw = [_row(f"S{i}", "2026-04-30") for i in range(1000)]
    raw.append(_row("S0", "2026-08-31"))
    prior = [(f"S{i}", dt.date(2026, 1, 31), 4) for i in range(1000)]
    source = freshness.source_from_rows(raw, prior, raw_max=dt.date(2026, 8, 31), today=NOW)
    assert "SOURCE_REPORT_STALE" in source.verdict["breaches"]


@pytest.mark.parametrize("matches,alarm", [(89, True), (90, False)])
def test_derived_coverage_pairs_the_latest_report_of_each_same_series(matches, alarm):
    source = _cohort()
    rows = [
        freshness.Observation(f"S{i}", dt.date(2026, 7, 31), 4, LOADED)
        for i in range(matches)
    ]
    rows += [
        freshness.Observation(f"S{i}", dt.date(2026, 4, 30), 4, LOADED)
        for i in range(matches, 100)
    ]
    verdict = freshness.assess_stage("cagg", source, rows, require_counts=True)
    assert verdict["alarm"] is alarm
    assert verdict["matched_series_count"] == matches


def test_matching_dates_with_missing_holdings_still_fail():
    source = _cohort(1)
    rows = [freshness.Observation("S0", source.as_of, 3, LOADED)]
    assert freshness.assess_stage("cagg", source, rows, require_counts=True)["alarm"]


def test_existing_characteristics_must_have_been_computed_after_source_arrived():
    source = _cohort(1)
    rows = [freshness.Observation("S0", source.as_of, None, LOADED - dt.timedelta(days=1))]
    assert freshness.assess_stage("characteristics", source, rows, require_computed=True)["alarm"]


def test_empty_source_never_passes():
    source = freshness.source_from_rows([], [], raw_max=None, today=NOW)
    assert source.verdict["alarm"] is True


def test_a_future_raw_report_is_invalid_even_when_the_known_cohort_is_fresh():
    source = _cohort()
    raw = [_row(sid, str(row.report_date)) for sid, row in source.series.items()]
    prior = [(sid, dt.date(2026, 4, 30), 4) for sid in source.series]
    invalid = freshness.source_from_rows(
        raw, prior, raw_max=NOW + dt.timedelta(days=1), today=NOW,
        policy=freshness.Policy(min_full_month_series=1),
    )
    assert invalid.verdict["alarm"] is True
    assert "SOURCE_REPORT_FUTURE" in invalid.verdict["breaches"]


def test_fresh_summary_without_exposures_cannot_certify_the_lookthrough():
    source = _cohort(1)
    row = freshness.Observation("S0", source.as_of, 4, LOADED,
                                 oldest_report_date=source.as_of, exposures_present=False)
    verdict = freshness.assess_stage("lookthrough", source, [row], require_exposures=True)
    assert verdict["alarm"] is True


def test_mapping_repairs_are_rebuilt_within_the_documented_seven_day_window():
    source = _cohort(1)
    row = freshness.Observation("S0", source.as_of, 4,
                                 dt.datetime.now(dt.UTC) - dt.timedelta(days=8),
                                 oldest_report_date=source.as_of, exposures_present=True)
    verdict = freshness.assess_stage("lookthrough", source, [row], require_recent_computation=True)
    assert verdict["alarm"] is True
    assert verdict["max_computation_age_days"] == 7


def test_old_expanded_child_reports_cannot_certify_a_fresh_root():
    source = _cohort(1)
    row = freshness.Observation("S0", source.as_of, 4, LOADED,
                                 oldest_report_date=dt.date(2026, 1, 31), exposures_present=True)
    verdict = freshness.assess_stage("lookthrough", source, [row], require_chain_freshness=True)
    assert verdict["alarm"] is True
