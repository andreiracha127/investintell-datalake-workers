"""Worker-only freshness contract for the legacy fund-classification inputs.

N-PORT filers have staggered fiscal quarters. A month is meaningful when it has
at least 1,000 distinct series; the newest meaningful month anchors a rolling
three-report-month cohort. A newer singleton cannot advance that anchor.
The anchor may be at most 120 UTC calendar days old. At least 90% of the prior
three-month cohort must remain, and derived stages must cover at least 90% of
the same series at their own latest source report. New series cannot compensate
for lost old series. These checks never run on an API request path.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Policy:
    max_report_age_days: int = 120
    min_full_month_series: int = 1000
    min_coverage: float = 0.90
    max_chain_report_age_days: int = 180
    max_lookthrough_computation_age_days: int = 7


@dataclass(frozen=True)
class Observation:
    series_id: str
    report_date: dt.date | None
    n_holdings: int | None = None
    computed_at: dt.datetime | None = None
    equity: bool = False
    oldest_report_date: dt.date | None = None
    exposures_present: bool = False
    # Set for instrument-grain outputs (characteristics): every instrument mapped
    # to a series needs its own matching row; a sibling share class cannot cover it.
    instrument_id: str | None = None


@dataclass
class SourceCohort:
    as_of: dt.date | None
    raw_max: dt.date | None
    start: dt.date | None
    series: dict[str, Observation] = field(default_factory=dict)
    verdict: dict[str, Any] = field(default_factory=dict)
    policy: Policy = field(default_factory=Policy)
    latest_loaded_at: dt.datetime | None = None
    # Newest load in the raw tail through today, ignoring any replay cutoff.
    # With the global raw_max it is the live watermark: a cohort read capped at
    # an old anchor cannot see a newer month, this can.
    live_loaded_at: dt.datetime | None = None

    @property
    def signature(self) -> str:
        rows = [
            (key, str(row.report_date), row.n_holdings, str(row.computed_at))
            for key, row in sorted(self.series.items())
        ]
        return hashlib.sha256(json.dumps(rows).encode()).hexdigest()

    @property
    def load_watermark(self) -> tuple[str, str]:
        return str(self.raw_max), str(self.live_loaded_at)


class FundPipelineBlocked(RuntimeError):
    def __init__(self, verdict: dict[str, Any]):
        self.verdict = verdict
        super().__init__(", ".join(verdict.get("breaches", ["FUND_PIPELINE_BLOCKED"])))


def month_start(day: dt.date, offset: int = 0) -> dt.date:
    index = day.year * 12 + day.month - 1 + offset
    return dt.date(index // 12, index % 12 + 1, 1)


def utc_today() -> dt.date:
    return dt.datetime.now(dt.UTC).date()


def source_from_rows(
    raw_rows: list[tuple], profile_rows: list[tuple], *, raw_max: dt.date | None,
    today: dt.date, policy: Policy | None = None, live_loaded_at: dt.datetime | None = None,
) -> SourceCohort:
    policy = policy or Policy()
    months: dict[dt.date, set[str]] = {}
    for sid, day, *_ in raw_rows:
        if day <= today:
            months.setdefault(month_start(day), set()).add(str(sid))
    meaningful = [month for month, series in months.items()
                  if len(series) >= policy.min_full_month_series]
    anchor_month = max(meaningful, default=None)
    anchor = max((row[1] for row in raw_rows if anchor_month == month_start(row[1]) and row[1] <= today),
                 default=None)
    start = month_start(anchor, -2) if anchor else None
    current: dict[str, Observation] = {}
    if start and anchor:
        for sid, day, count, loaded_at, equity in raw_rows:
            sid = str(sid)
            if start <= day <= anchor and (sid not in current or current[sid].report_date < day):
                current[sid] = Observation(sid, day, int(count), loaded_at, bool(equity))
    previous_start = month_start(start, -3) if start else None
    previous = {str(sid) for sid, day, *_ in profile_rows
                if previous_start and previous_start <= day < start}
    retained = len(previous.intersection(current))
    retention = retained / len(previous) if previous else None
    age = (today - anchor).days if anchor else None
    breaches = []
    if raw_max is not None and raw_max > today:
        breaches.append("SOURCE_REPORT_FUTURE")
    if anchor is None:
        breaches.append("SOURCE_MEANINGFUL_COHORT_MISSING")
    elif age > policy.max_report_age_days:
        breaches.append("SOURCE_REPORT_STALE")
    if not previous:
        breaches.append("SOURCE_BASELINE_MISSING")
    elif retention < policy.min_coverage:
        breaches.append("SOURCE_COHORT_RETENTION_BELOW_FLOOR")
    verdict = {
        "stage": "nport", "source_as_of": str(anchor) if anchor else None,
        "raw_max_report_date": str(raw_max) if raw_max else None,
        "report_age_days": age, "max_report_age_days": policy.max_report_age_days,
        "cohort_report_months": 3, "min_full_month_series": policy.min_full_month_series,
        "min_coverage": policy.min_coverage, "expected_series_count": len(current),
        "previous_series_count": len(previous), "retained_series_count": retained,
        "retention_share": retention, "alarm": bool(breaches), "breaches": breaches,
        "last_good_preserved": True,
    }
    latest_loaded_at = max((row[3] for row in raw_rows if row[3] is not None), default=None)
    return SourceCohort(anchor, raw_max, start, current, verdict, policy, latest_loaded_at,
                        live_loaded_at or latest_loaded_at)


def read_source_cohort(
    conn: Any, *, cutoff: dt.date | None = None, today: dt.date | None = None,
) -> SourceCohort:
    """Bound the raw scan; use the small cagg for the older baseline.

    The raw tail begins two report months before the 120-day age boundary, so
    the oldest still-admissible anchor's complete rolling quarter is included.
    A replay cutoff never changes the UTC wall clock used to judge staleness.
    The tail is always scanned through today: a cutoff bounds the cohort, never
    the live load watermark.
    """
    today = today or utc_today()
    cutoff = min(cutoff or today, today)
    start = month_start(today - dt.timedelta(days=Policy().max_report_age_days), -2)
    with conn.cursor() as cur:
        cur.execute("SELECT max(report_date) FROM public.sec_nport_holdings")
        raw_max = cur.fetchone()[0]
        cur.execute(
            """SELECT series_id, report_day, n_holdings
               FROM public.cagg_nport_series_profile
               WHERE report_day >= %s AND report_day <= %s""",
            (month_start(start, -3), cutoff),
        )
        profiles = cur.fetchall()
        cur.execute(
            """SELECT series_id, report_date, count(*), max(created_at),
                      bool_or(asset_class IN ('EC', 'EP') AND market_value > 0)
               FROM public.sec_nport_holdings
               WHERE report_date >= %s AND report_date <= %s
               GROUP BY series_id, report_date""",
            (start, today),
        )
        tail = cur.fetchall()
    return source_from_rows(
        [row for row in tail if row[1] <= cutoff], profiles, raw_max=raw_max, today=today,
        live_loaded_at=max((row[3] for row in tail if row[3] is not None), default=None),
    )


def require(verdict: dict[str, Any]) -> dict[str, Any]:
    if verdict.get("alarm"):
        raise FundPipelineBlocked(verdict)
    return verdict


def require_unchanged(stage: str, before: SourceCohort, after: SourceCohort) -> None:
    """Refuse promotion if the cohort or the live raw watermark moved during a build."""
    if after.signature == before.signature and after.load_watermark == before.load_watermark:
        return
    raise FundPipelineBlocked({
        "stage": stage, "alarm": True, "breaches": ["SOURCE_CHANGED_DURING_BUILD"],
        "source_as_of": str(before.as_of),
        "load_watermark_before": list(before.load_watermark),
        "load_watermark_after": list(after.load_watermark),
        "last_good_preserved": True,
    })


def assess_stage(
    stage: str, source: SourceCohort, rows: list[Observation], *,
    expected_series: set[str] | None = None, require_counts: bool = False,
    require_computed: bool = False,
    require_chain_freshness: bool = False,
    require_exposures: bool = False, require_recent_computation: bool = False,
) -> dict[str, Any]:
    expected = set(source.series) if expected_series is None else expected_series
    observed: dict[str, list[Observation]] = {}
    for row in rows:
        observed.setdefault(row.series_id, []).append(row)
    def matches(row: Observation, wanted: Observation) -> bool:
        if row.report_date != wanted.report_date:
            return False
        if require_counts and row.n_holdings != wanted.n_holdings:
            return False
        if require_computed and (row.computed_at is None or wanted.computed_at is None
                                 or row.computed_at < wanted.computed_at):
            return False
        if require_chain_freshness and (
            row.oldest_report_date is None
            or row.oldest_report_date < utc_today() - dt.timedelta(days=source.policy.max_chain_report_age_days)
            or row.oldest_report_date > row.report_date
        ):
            return False
        if require_exposures and not row.exposures_present:
            return False
        return not require_recent_computation or (
            row.computed_at is not None and row.computed_at >= dt.datetime.now(dt.UTC)
            - dt.timedelta(days=source.policy.max_lookthrough_computation_age_days)
        )

    matched = set()
    for sid in expected:
        units: dict[str | None, list[Observation]] = {}
        for row in observed.get(sid, []):
            units.setdefault(row.instrument_id, []).append(row)
        if units and all(any(matches(row, source.series[sid]) for row in unit)
                         for unit in units.values()):
            matched.add(sid)
    share = len(matched) / len(expected) if expected else 0.0
    breaches = []
    if not expected:
        breaches.append("ELIGIBLE_COHORT_EMPTY")
    elif share < source.policy.min_coverage:
        breaches.append("DERIVED_COHORT_COVERAGE_BELOW_FLOOR")
    return {
        "stage": stage, "source_as_of": str(source.as_of) if source.as_of else None,
        "expected_series_count": len(expected), "matched_series_count": len(matched),
        "coverage_share": share, "min_coverage": source.policy.min_coverage,
        "mismatch_sample": sorted(expected - matched)[:20],
        "alarm": bool(breaches), "breaches": breaches, "last_good_preserved": True,
        "max_computation_age_days": source.policy.max_lookthrough_computation_age_days
        if require_recent_computation else None,
    }


def probe_stage(conn: Any, source: SourceCohort, stage: str) -> dict[str, Any]:
    if stage == "cagg":
        with conn.cursor() as cur:
            cur.execute(
                """SELECT series_id, report_day, n_holdings
                   FROM public.cagg_nport_series_profile
                   WHERE report_day >= %s AND report_day <= %s""",
                (source.start, source.as_of),
            )
            rows = [Observation(str(sid), day, int(count)) for sid, day, count in cur.fetchall()]
        return assess_stage(stage, source, rows, require_counts=True)
    if stage == "characteristics":
        with conn.cursor() as cur:
            cur.execute(
                """WITH funds AS (
                     SELECT i.instrument_id, COALESCE(NULLIF(ii.sec_series_id, ''),
                         NULLIF(i.attributes->>'sec_series_id', ''),
                         NULLIF(i.attributes->>'series_id', '')) AS series_id
                     FROM public.instruments_universe i
                     LEFT JOIN public.instrument_identity ii USING (instrument_id)
                   ) SELECT f.series_id, f.instrument_id::text, e.as_of, e.computed_at
                   FROM funds f LEFT JOIN public.equity_characteristics_monthly e
                     ON e.instrument_id = f.instrument_id AND e.as_of >= %s AND e.as_of <= %s
                   WHERE f.series_id = ANY(%s::text[])""",
                (source.start, source.as_of, [sid for sid, row in source.series.items() if row.equity]),
            )
            rows = [Observation(str(sid), day, computed_at=computed, instrument_id=iid)
                    for sid, iid, day, computed in cur.fetchall()]
        return assess_stage(stage, source, rows, expected_series={row.series_id for row in rows},
                            require_computed=True)
    if stage == "lookthrough":
        with conn.cursor() as cur:
            cur.execute(
                """SELECT s.series_id, s.report_date, s.n_holdings, s.computed_at, s.oldest_report_date,
                          EXISTS (SELECT 1 FROM public.nport_lookthrough_exposures e
                                  WHERE e.series_id = s.series_id AND e.report_date = s.report_date)
                   FROM public.nport_lookthrough_summary s
                   WHERE s.report_date >= %s AND s.report_date <= %s""",
                (source.start, source.as_of),
            )
            rows = [Observation(str(sid), day, count, computed, oldest_report_date=oldest,
                                exposures_present=present)
                    for sid, day, count, computed, oldest, present in cur.fetchall()]
        return assess_stage(stage, source, rows, require_counts=True, require_computed=True,
                            require_chain_freshness=True, require_exposures=True,
                            require_recent_computation=True)
    raise ValueError(f"unknown fund pipeline stage: {stage}")
