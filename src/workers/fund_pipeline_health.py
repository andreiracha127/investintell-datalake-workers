"""Independent read-only monitor; a missed classifier cron cannot stay green.

Run at 09:00 UTC after the classifier's 08:00 window. All source/derived
cohorts obey the same freshness policy as the producer chain. The classifier
must have a nonempty completed run within 36 hours and after the current source
load. Unchanged characteristics/lookthrough are validated against source
lineage; they do not require an expensive daily recomputation heartbeat.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from src.db import connect
from src.workers import _fund_pipeline_freshness as freshness

MAX_COMPLETION_AGE_HOURS = 36


def _latest_classification(conn: Any) -> tuple | None:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT id::text, as_of_date, completed_at, fund_count
               FROM public.fund_classification_runs
               WHERE status = 'completed'
               ORDER BY completed_at DESC NULLS LAST, started_at DESC LIMIT 1"""
        )
        return cur.fetchone()


def _input_watermarks(conn: Any) -> dict[str, dt.date | None]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT (SELECT max(report_day) FROM public.cagg_nport_series_profile),
                      (SELECT max(as_of) FROM public.equity_characteristics_monthly),
                      (SELECT max(report_date) FROM public.nport_lookthrough_summary)"""
        )
        return dict(zip(("cagg", "characteristics", "lookthrough"), cur.fetchone()))


def assess_watermarks(
    raw_max: dt.date | None, watermarks: dict[str, dt.date | None], *, today: dt.date,
) -> dict[str, Any]:
    breaches = []
    if raw_max is None:
        breaches.append("SOURCE_REPORT_WATERMARK_MISSING")
    for stage, day in watermarks.items():
        if day is None:
            breaches.append(f"{stage.upper()}_REPORT_WATERMARK_MISSING")
        elif day > today:
            breaches.append(f"{stage.upper()}_REPORT_FUTURE")
        elif raw_max is not None and day < raw_max:
            breaches.append(f"{stage.upper()}_BEHIND_LOADED_RAW")
    return {
        "stage": "classification_inputs", "raw_max_report_date": str(raw_max) if raw_max else None,
        "input_watermarks": {stage: str(day) if day else None for stage, day in watermarks.items()},
        "alarm": bool(breaches), "breaches": breaches,
    }


def assess_classification(
    row: tuple | None, source: freshness.SourceCohort, *, now: dt.datetime,
) -> dict[str, Any]:
    breaches = []
    run_id = as_of = completed = fund_count = age = None
    if row is None:
        breaches.append("CLASSIFICATION_COMPLETED_RUN_MISSING")
    else:
        run_id, as_of, completed, fund_count = row
        if completed is None or completed.tzinfo is None:
            breaches.append("CLASSIFICATION_COMPLETED_AT_MISSING")
        else:
            age = (now - completed).total_seconds() / 3600
            if age > MAX_COMPLETION_AGE_HOURS or age < 0:
                breaches.append("CLASSIFICATION_RUN_STALE")
            source_loaded = source.latest_loaded_at or max(
                (row.computed_at for row in source.series.values() if row.computed_at is not None),
                default=None,
            )
            if source_loaded is None or completed < source_loaded:
                breaches.append("CLASSIFICATION_BEFORE_SOURCE_LOAD")
        if as_of is None or not 0 <= (now.date() - as_of).days <= source.policy.max_report_age_days:
            breaches.append("CLASSIFICATION_AS_OF_STALE")
        elif any(day is not None and as_of < day for day in (source.as_of, source.raw_max)):
            breaches.append("CLASSIFICATION_BEFORE_SOURCE_REPORT")
        if not isinstance(fund_count, int) or fund_count <= 0:
            breaches.append("CLASSIFICATION_EMPTY")
    return {
        "stage": "classification", "run_id": run_id, "as_of_date": str(as_of) if as_of else None,
        "completed_at": str(completed) if completed else None, "fund_count": fund_count,
        "completion_age_hours": age, "max_completion_age_hours": MAX_COMPLETION_AGE_HOURS,
        "alarm": bool(breaches), "breaches": breaches,
    }


def run(
    dsn: str, *, calc_date: str | None = None, limit: int | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    if calc_date is not None or limit is not None:
        raise ValueError("fund_pipeline_health is a live full-cohort check; replay/limit is unsupported")
    reference_time = now or dt.datetime.now(dt.UTC)
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        source = freshness.read_source_cohort(conn, today=reference_time.date())
        stages = [source.verdict]
        if not source.verdict["alarm"]:
            for stage in ("cagg", "characteristics", "lookthrough"):
                stages.append(freshness.probe_stage(conn, source, stage))
        stages.append(assess_watermarks(source.raw_max, _input_watermarks(conn), today=reference_time.date()))
        row = _latest_classification(conn)
        stages.append(assess_classification(row, source, now=now or dt.datetime.now(dt.UTC)))
    failed = [stage["stage"] for stage in stages if stage["alarm"]]
    return {
        "state": "failed" if failed else "healthy", "source_as_of": str(source.as_of),
        "freshness": {"alarm": bool(failed), "failed_stages": failed, "stages": stages},
    }
