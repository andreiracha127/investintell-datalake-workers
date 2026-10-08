"""The independent monitor detects a classifier that never ran or published."""

from __future__ import annotations

import datetime as dt
import tomllib
from pathlib import Path

import pytest

from src.workers import _fund_pipeline_freshness as freshness
from src.workers import fund_pipeline_health as health

NOW = dt.datetime(2026, 10, 7, 10, tzinfo=dt.UTC)
SOURCE_LOAD = NOW - dt.timedelta(days=1)


def _source(loaded=SOURCE_LOAD):
    return freshness.SourceCohort(
        dt.date(2026, 7, 31), dt.date(2026, 8, 31), dt.date(2026, 5, 1),
        series={"S1": freshness.Observation("S1", dt.date(2026, 7, 31), 4, loaded)},
        verdict={"alarm": False},
    )


def _run(completed, fund_count=5000, as_of=NOW.date()):
    return ("run", as_of, completed - dt.timedelta(minutes=20), completed, fund_count)


@pytest.mark.parametrize(
    ("row", "breach"),
    [
        (None, "CLASSIFICATION_COMPLETED_RUN_MISSING"),
        (_run(NOW - dt.timedelta(hours=37)), "CLASSIFICATION_RUN_STALE"),
        (_run(NOW - dt.timedelta(hours=25)), "CLASSIFICATION_BEFORE_SOURCE_LOAD"),
        (_run(NOW - dt.timedelta(hours=1), fund_count=0), "CLASSIFICATION_EMPTY"),
        (_run(NOW - dt.timedelta(hours=1), as_of=dt.date(2026, 6, 30)),
         "CLASSIFICATION_BEFORE_SOURCE_REPORT"),
    ],
)
def test_missing_stale_or_empty_classification_is_an_alarm(row, breach):
    verdict = health.assess_classification(row, _source(), now=NOW)
    assert verdict["alarm"] is True
    assert breach in verdict["breaches"]


def test_recent_nonempty_classification_on_current_source_is_healthy():
    verdict = health.assess_classification(_run(NOW - dt.timedelta(hours=1)), _source(), now=NOW)
    assert verdict["alarm"] is False


@pytest.mark.parametrize(
    ("now", "started", "alarm"),
    [
        # The 09:00 check after a missed 08:00 run: yesterday's run is ~25 h old.
        (dt.datetime(2026, 10, 7, 9, tzinfo=dt.UTC), dt.datetime(2026, 10, 6, 8, tzinfo=dt.UTC), True),
        (dt.datetime(2026, 10, 7, 9, tzinfo=dt.UTC), dt.datetime(2026, 10, 7, 8, 0, 40, tzinfo=dt.UTC), False),
        # Before the completion window ends, yesterday's slot is still the expected one.
        (dt.datetime(2026, 10, 7, 8, 30, tzinfo=dt.UTC), dt.datetime(2026, 10, 6, 8, tzinfo=dt.UTC), False),
    ],
)
def test_health_requires_the_most_recent_scheduled_classifier_run(now, started, alarm):
    row = ("run", now.date(), started, started + dt.timedelta(minutes=20), 5000)
    verdict = health.assess_classification(row, _source(now - dt.timedelta(days=3)), now=now)
    assert verdict["alarm"] is alarm
    assert ("CLASSIFICATION_RUN_STALE" in verdict["breaches"]) is alarm


def test_health_cron_runs_after_the_classifier_completion_window():
    toml = Path(__file__).resolve().parents[1] / "railway.fund-pipeline-health.toml"
    minute, hour, *_ = tomllib.loads(toml.read_text(encoding="utf-8"))["deploy"]["cronSchedule"].split()
    due = dt.datetime.combine(NOW.date(), health.CLASSIFIER_START_UTC) + health.CLASSIFIER_COMPLETION_WINDOW
    assert dt.time(int(hour), int(minute)) >= due.time()


def test_a_newer_sparse_raw_load_cannot_renew_health_through_the_broad_anchor():
    source = _source()
    source.latest_loaded_at = NOW - dt.timedelta(hours=1)
    row = _run(NOW - dt.timedelta(hours=1, minutes=10))
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
