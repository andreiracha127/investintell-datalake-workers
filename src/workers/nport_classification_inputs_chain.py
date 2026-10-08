"""Retry the legacy fund input chain in dependency order, without daily rebuilds.

The monthly loader (``nport_secapi_monthly``) is a separate lane and is
deliberately not imported here; it shares only the governed cagg request in
``_fund_pipeline_freshness``. Committed, verified holdings are the boundary:
source -> owner-run cagg policy -> characteristics -> atomic look-through. An
asynchronous cagg request is only accepted work, never freshness proof. A
bounded wait that expires is red; the next retry resumes. Existing unchanged
derived cohorts are cheap worker no-ops.
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
BASELINE_BREACHES = {"SOURCE_BASELINE_MISSING", "SOURCE_COHORT_RETENTION_BELOW_FLOOR"}


def _require_raw(source):
    breaches = set(source.verdict.get("breaches", [])) - BASELINE_BREACHES
    if breaches:
        freshness.require({**source.verdict, "alarm": True, "breaches": sorted(breaches)})


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
                _require_raw(source)
                stage = "cagg"
                cagg = freshness.probe_stage(guard, source, stage)
                if cagg["alarm"] or source.verdict["alarm"]:
                    job_id = freshness.request_profile_refresh(guard)
                    stages.append({"stage": "cagg_request", "job_id": job_id, "requested": True})

                    def aligned():
                        nonlocal source
                        latest = freshness.read_source_cohort(guard, cutoff=cutoff)
                        _require_raw(latest)
                        freshness.require_unchanged("cagg", source, latest)
                        source = latest
                        verdict = freshness.probe_stage(guard, source, "cagg")
                        return verdict, not verdict["alarm"] and not source.verdict["alarm"]

                    cagg, _polls = freshness.poll_alignment(
                        aligned, attempts=POLL_ATTEMPTS, interval=POLL_INTERVAL_SECONDS,
                        sleeper=sleeper,
                    )
                stages[0] = source.verdict
                stages.append(cagg)
                if cagg["alarm"]:
                    return _blocked("cagg_refresh_pending", stages, freshness=cagg)
                freshness.require(source.verdict)
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
                latest = freshness.read_source_cohort(guard, cutoff=cutoff)
                freshness.require(latest.verdict)
                if (latest.signature != source.signature
                        or latest.load_watermark != source.load_watermark):
                    return _blocked("source_changed_during_chain", stages)
                # Auxiliary repairs can invalidate an earlier stage while a
                # later stage builds, even when the raw watermark is unchanged.
                for stage in ("cagg", "characteristics", "lookthrough"):
                    final = freshness.require(freshness.probe_stage(guard, latest, stage))
                return {"state": "complete", "source_as_of": str(source.as_of),
                        "source_signature": source.signature, "stages": stages, "freshness": final}
            except freshness.FundPipelineBlocked as exc:
                return _blocked(f"{stage}_not_fresh", stages, freshness=exc.verdict)
            except Exception as exc:
                return _blocked(f"{stage}_failed", stages, error_type=type(exc).__name__)
