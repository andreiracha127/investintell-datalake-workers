"""The independent monitor detects a classifier that never ran or published."""

from __future__ import annotations

import datetime as dt

import pytest

from src.workers import _fund_pipeline_freshness as freshness
from src.workers import fund_pipeline_health as health

NOW = dt.datetime(2026, 10, 7, 10, tzinfo=dt.UTC)
SOURCE_LOAD = NOW - dt.timedelta(days=1)


def _source():
    return freshness.SourceCohort(
        dt.date(2026, 7, 31), dt.date(2026, 8, 31), dt.date(2026, 5, 1),
        series={"S1": freshness.Observation("S1", dt.date(2026, 7, 31), 4, SOURCE_LOAD)},
        verdict={"alarm": False},
    )


@pytest.mark.parametrize(
    ("row", "breach"),
    [
        (None, "CLASSIFICATION_COMPLETED_RUN_MISSING"),
        (("run", NOW.date(), NOW - dt.timedelta(hours=37), 5000), "CLASSIFICATION_RUN_STALE"),
        (("run", NOW.date(), NOW - dt.timedelta(hours=25), 5000), "CLASSIFICATION_BEFORE_SOURCE_LOAD"),
        (("run", NOW.date(), NOW - dt.timedelta(hours=2), 0), "CLASSIFICATION_EMPTY"),
        (("run", dt.date(2026, 6, 30), NOW - dt.timedelta(hours=2), 5000),
         "CLASSIFICATION_BEFORE_SOURCE_REPORT"),
    ],
)
def test_missing_stale_or_empty_classification_is_an_alarm(row, breach):
    verdict = health.assess_classification(row, _source(), now=NOW)
    assert verdict["alarm"] is True
    assert breach in verdict["breaches"]


def test_recent_nonempty_classification_on_current_source_is_healthy():
    row = ("run", NOW.date(), NOW - dt.timedelta(hours=2), 5000)
    verdict = health.assess_classification(row, _source(), now=NOW)
    assert verdict["alarm"] is False
    assert verdict["max_completion_age_hours"] == 36


def test_a_newer_sparse_raw_load_cannot_renew_health_through_the_broad_anchor():
    source = _source()
    source.latest_loaded_at = NOW - dt.timedelta(hours=1)
    row = ("run", NOW.date(), NOW - dt.timedelta(hours=2), 5000)
    verdict = health.assess_classification(row, source, now=NOW)
    assert "CLASSIFICATION_BEFORE_SOURCE_LOAD" in verdict["breaches"]


def test_sparse_loaded_raw_ahead_of_derived_outputs_still_blocks_health():
    verdict = health.assess_watermarks(dt.date(2026, 8, 31), {
        "cagg": dt.date(2026, 8, 31), "characteristics": dt.date(2026, 8, 31),
        "lookthrough": dt.date(2026, 7, 31),
    }, today=NOW.date())
    assert verdict["alarm"] is True
    assert verdict["breaches"] == ["LOOKTHROUGH_BEHIND_LOADED_RAW"]


def test_aligned_loaded_raw_watermarks_are_healthy():
    verdict = health.assess_watermarks(dt.date(2026, 7, 31), {
        stage: dt.date(2026, 7, 31) for stage in ("cagg", "characteristics", "lookthrough")
    }, today=NOW.date())
    assert verdict["alarm"] is False


def test_health_is_read_only_and_aggregates_dependency_failures(monkeypatch):
    statements = []

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def cursor(self):
            return self

        def execute(self, sql, *_args):
            statements.append(sql)

    monkeypatch.setattr(health, "connect", lambda *_a: Conn())
    monkeypatch.setattr(health.freshness, "read_source_cohort", lambda *_a, **_k: _source())
    monkeypatch.setattr(health.freshness, "probe_stage", lambda _c, _s, stage: {
        "stage": stage, "alarm": stage == "lookthrough", "breaches": ["OUTPUT_STALE"],
    })
    monkeypatch.setattr(health, "_latest_classification", lambda *_a: None)
    monkeypatch.setattr(health, "_input_watermarks", lambda *_a: {
        stage: dt.date(2026, 7, 31) for stage in ("cagg", "characteristics", "lookthrough")
    })
    stats = health.run("unused", now=NOW)
    assert statements[0].endswith("READ ONLY")
    assert stats["state"] == "failed"
    assert stats["freshness"]["alarm"] is True
    assert "lookthrough" in stats["freshness"]["failed_stages"]
    assert "classification" in stats["freshness"]["failed_stages"]
