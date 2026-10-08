"""Retry the legacy fund input chain in dependency order, without daily rebuilds.

The monthly loader is owned by PR #153 and is deliberately not imported here.
Committed, verified holdings are the boundary: source -> owner-run cagg policy
-> characteristics -> atomic look-through. An asynchronous cagg request is only
accepted work, never freshness proof. A bounded wait that expires is red; the
next retry resumes. Existing unchanged derived cohorts are cheap worker no-ops.
"""

from __future__ import annotations

import datetime as dt
import time
from typing import Any, Callable

from src.db import LOCK_NPORT_CLASSIFICATION_INPUTS_CHAIN, advisory_lock, connect
from src.workers import _fund_pipeline_freshness as freshness
from src.workers import characteristics, nport_lookthrough

POLL_ATTEMPTS = 6
POLL_INTERVAL_SECONDS = 2


def _blocked(reason: str, stages: list[dict], **details: Any) -> dict[str, Any]:
    return {"state": "blocked", "reason": reason, "stages": stages,
            "last_good_preserved": True, **details}


def run(
    dsn: str, *, calc_date: str | None = None, limit: int | None = None,
    characteristics_runner: Callable[..., dict] = characteristics.run,
    lookthrough_runner: Callable[..., dict] = nport_lookthrough.run,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    stages: list[dict] = []
    if limit is not None:
        return _blocked("partial_cohort_cannot_publish", stages)
    cutoff = dt.date.fromisoformat(calc_date) if calc_date else None
    with connect(dsn, autocommit=True) as guard:
        with advisory_lock(guard, LOCK_NPORT_CLASSIFICATION_INPUTS_CHAIN) as got:
            if not got:
                return {"status": "lock_busy", "last_good_preserved": True}
            stage = "nport"
            try:
                source = freshness.read_source_cohort(guard, cutoff=cutoff)
                stages.append(source.verdict)
                freshness.require(source.verdict)
                stage = "cagg"
                cagg = freshness.probe_stage(guard, source, stage)
                if cagg["alarm"]:
                    # Autocommit makes the advanced policy next_start visible
                    # to the scheduler BEFORE any poll. Runtime never CALLs
                    # the owner-only Timescale refresh procedure itself.
                    job_id = guard.execute(
                        "SELECT public.request_nport_series_profile_refresh()"
                    ).fetchone()[0]
                    stages.append({"stage": "cagg_request", "job_id": job_id, "requested": True})
                    for attempt in range(POLL_ATTEMPTS):
                        if attempt:
                            sleeper(POLL_INTERVAL_SECONDS)
                        cagg = freshness.probe_stage(guard, source, stage)
                        if not cagg["alarm"]:
                            break
                stages.append(cagg)
                if cagg["alarm"]:
                    return _blocked("cagg_refresh_pending", stages, freshness=cagg)
                stage = "characteristics"
                result = characteristics_runner(dsn, calc_date=calc_date, source=source)
                stages.append({"stage": stage, "result": result})
                if result.get("status") not in {"succeeded", "current"}:
                    return _blocked("characteristics_not_complete", stages)
                freshness.require(freshness.probe_stage(guard, source, stage))
                stage = "lookthrough"
                result = lookthrough_runner(dsn, calc_date=str(source.as_of), source=source)
                stages.append({"stage": stage, "result": result})
                if result.get("status") not in {"complete", "current"}:
                    return _blocked("lookthrough_not_complete", stages)
                final = freshness.require(freshness.probe_stage(guard, source, stage))
                latest = freshness.read_source_cohort(guard, cutoff=cutoff)
                freshness.require(latest.verdict)
                if (latest.signature != source.signature
                        or latest.load_watermark != source.load_watermark):
                    return _blocked("source_changed_during_chain", stages)
                return {"state": "complete", "source_as_of": str(source.as_of),
                        "source_signature": source.signature, "stages": stages, "freshness": final}
            except freshness.FundPipelineBlocked as exc:
                return _blocked(f"{stage}_not_fresh", stages, freshness=exc.verdict)
            except Exception as exc:
                return _blocked(f"{stage}_failed", stages, error_type=type(exc).__name__)
