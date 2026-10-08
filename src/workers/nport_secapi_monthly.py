"""Monthly sec-api.io top-up of ``sec_nport_holdings``.

WHY THIS LANE EXISTS
--------------------
``sec_nport_holdings`` was only ever written by hand, from DERA quarterly
packages and, once, from sec-api monthly containers converted by a script that
never reached a repository. Nothing ran after 2026-08-06: by 2026-10 the table
had full coverage only through report_date 2026-04-30, and Light's
``fund-classification`` (180-day report-age rule) stopped publishing on
2026-08-27. This lane is the recurring version of that manual load.

WHAT ONE RUN DOES (as of ``calc_date``, month M)
-----------------------------------------------
1. Download the ``form-nport`` containers for filing months M-3..M with the
   ``sec_api`` SDK (``tools.nport_secapi.download``). M is still filling.
2. Convert them (``tools.nport_secapi.convert``, DERA key policy): one CSV per
   report_date, one filing per (report_date, series_id), newest wins.
3. Target the report_dates from the start of M-5 to the end of M-3. N-PORT turns
   public ~60 days after the period, so the newest of them (M-3) has its main
   publication month (M-1) complete, and each report_date is revisited by three
   consecutive runs while its late filers trickle in. Earlier dates are left to
   an operator: a late filing for them would decompress a cold chunk.
4. For each target date with series the table does not have yet: the value
   checks of ``tools.nport_secapi.validate`` on its CSV, then the loader's
   ``--dry-run`` with the load's own arguments (it reads the table, writes
   nothing, and refuses whatever the load or its verify would reject), then
   ``nport_parallel_load --new-series-only`` scoped to that one date. ``--new-series-only`` is what makes revisiting safe:
   a series already loaded is never touched, so an amendment cannot graft new
   keys onto the original filing's rows the way ``ON CONFLICT DO NOTHING`` alone
   would. The loader verifies actual rows before committing the transaction.
5. Once the loads have committed, request the owner-run refresh of
   ``cagg_nport_series_profile`` once, through
   ``public.request_nport_series_profile_refresh()``. The aggregate belongs to
   ``postgres`` and this lane runs as ``worker_writer``, so it never CALLs
   ``refresh_continuous_aggregate`` itself. The request is committed before
   polling, and its job id is a receipt, not completion. The lane then polls,
   within a bound, until each accepted date's profile holds every committed
   series and the shared cohort check of ``_fund_pipeline_freshness`` passes.
   Rejected loads roll back, so the policy cannot materialize rejected rows. A
   no-new-series revisit requests again for a profile an earlier run left stale.

One report_date per loader invocation, as the identifier-coverage runbook
requires (``docs/runbooks/nport-identifier-coverage.md``): a transaction over
several dates holds ``backend_xmin`` long enough to stall the global VACUUM.

NO CRON IS CONFIGURED. ``railway.nport-secapi-monthly.toml`` documents the
proposed schedule; attaching it to a service is an operator decision.

Env: ``SEC_API_IO_KEY`` (required), ``DATABASE_URL``. Optional
``NPORT_SECAPI_CACHE_DIR`` keeps the containers between runs (re-fetched when
the remote size or ``updatedAt`` changes); without it they go to a temp dir that
is removed afterwards.
``WORKER_CALC_DATE`` pins M; ``WORKER_LIMIT`` caps how many report_dates load.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict``. ``state`` is
``ok`` (profile alignment confirmed), ``noop`` (no new series or refresh
needed), ``blocked`` (``reason == "cagg_refresh_pending"``: requested, not
confirmed within the bound; the next run requests again) or ``failed`` (also
when the containers convert to no complete target date at all, or the request
raised); ``status == "lock_busy"`` when another run holds the lock.
``stats["cagg_refresh"]`` records the request: report dates, ``job_id``,
``aligned``, ``polls`` and the last evidence. ``run_worker`` exits non-zero on
``blocked``, ``failed`` and ``lock_busy``.
"""

from __future__ import annotations

import csv
import datetime as dt
import logging
import os
import shutil
import tempfile
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from src.db import LOCK_NPORT_SECAPI_MONTHLY, advisory_lock, connect
from src.workers import _fund_pipeline_freshness as freshness
from tools.nport_dera import nport_parallel_load as loader
from tools.nport_secapi import convert as converter
from tools.nport_secapi import download as downloader
from tools.nport_secapi import validate as validator

LOGGER = logging.getLogger(__name__)

CAGG = "cagg_nport_series_profile"
ENV_CACHE_DIR = "NPORT_SECAPI_CACHE_DIR"
#: Filing-month containers fetched per run: M-3..M.
CONTAINER_LOOKBACK = 3
#: Report months targeted per run: M-5..M-3.
REPORT_MONTHS_FROM, REPORT_MONTHS_TO = 5, 3
#: Bounded wait for the owner-run refresh after a request, about five minutes.
#: Re-aggregating dates that just gained millions of rows outlasts the input
#: chain's 12-second wait, which only expects an incremental refresh.
CAGG_POLL_ATTEMPTS = 30
CAGG_POLL_INTERVAL_SECONDS = 10


def _month(d: dt.date, delta: int) -> dt.date:
    idx = d.year * 12 + (d.month - 1) + delta
    return dt.date(idx // 12, idx % 12 + 1, 1)


@dataclass(frozen=True)
class Plan:
    container_from: str
    container_to: str
    partial_months: frozenset[str]
    report_date_from: str
    report_date_to: str


def plan_for(today: dt.date) -> Plan:
    """The containers and report_date window one run covers, as of ``today``."""
    current = _month(today, 0)
    first_report = _month(today, -REPORT_MONTHS_FROM)
    last_report = _month(today, -REPORT_MONTHS_TO + 1) - dt.timedelta(days=1)
    return Plan(
        container_from=_month(today, -CONTAINER_LOOKBACK).strftime("%Y-%m"),
        container_to=current.strftime("%Y-%m"),
        partial_months=frozenset({current.strftime("%Y-%m")}),
        report_date_from=first_report.isoformat(),
        report_date_to=last_report.isoformat(),
    )


def _csv_series(path: Path) -> set[str]:
    with open(path, encoding="utf-8", newline="") as fh:
        return {row["series_id"] for row in csv.DictReader(fh)}


def existing_series(dsn: str, report_date: str) -> set[str]:
    with connect(dsn) as conn:
        rows = conn.execute(
            "SELECT DISTINCT series_id FROM sec_nport_holdings WHERE report_date = %s",
            (report_date,),
        ).fetchall()
    return {r[0] for r in rows}


def load_report_date(dsn: str, seed_dir: Path, report_date: str) -> int:
    """The loader's dry run over exactly the load's arguments, then the load. Returns the exit code.

    Same argv, so the plan is the load's own: the series the table already holds
    are left out of the new-cohort contract. ISIN must pass both that cohort's
    contract and the whole-date post-load check before commit.
    """
    args = ["--seed-dir", str(seed_dir), "--only", f"{report_date}.csv", "--only-report-dates", report_date,
            "--dsn", dsn, "--workers", "1", "--skip-matview", "--new-series-only", "--secapi"]
    rc = loader.main([*args, "--dry-run"])
    if rc != 0:
        return rc
    return loader.main(args)


def report_date_counts(dsn: str, report_dates: list[str]) -> dict[str, dict[str, int]]:
    with connect(dsn) as conn:
        rows = conn.execute(
            "SELECT report_date::text, count(*), count(DISTINCT series_id) FROM sec_nport_holdings "
            "WHERE report_date = ANY(%s::date[]) GROUP BY 1",
            (report_dates,),
        ).fetchall()
    return {r[0]: {"rows": r[1], "series": r[2]} for r in rows}


def cagg_series_counts(conn: Any, report_dates: list[str]) -> dict[str, int]:
    """Profile rows per report date: one per (series, day) bucket."""
    rows = conn.execute(
        f"SELECT report_day::text, count(*) FROM {CAGG} WHERE report_day = ANY(%s::date[]) GROUP BY 1",
        (report_dates,),
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def cagg_needs_refresh(dsn: str, report_date: str, series_count: int) -> bool:
    """Detect a profile still stale after an earlier verified commit."""
    with connect(dsn) as conn:
        return cagg_series_counts(conn, [report_date]).get(report_date, 0) != series_count


def request_cagg_refresh(
    dsn: str, series: dict[str, int], *, sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Request the owner-run profile refresh once, then poll its alignment.

    ``series`` maps each report date to its committed distinct-series count.
    Alignment needs every date's profile to hold exactly that many series (the
    lane's own writes, including a handful of late filers a 90% floor would
    hide) and the shared cohort check the input chain applies. A job id alone
    never confirms anything.
    """
    dates = sorted(series)
    record: dict[str, Any] = {"report_dates": dates, "requested": False, "job_id": None,
                              "aligned": False}
    cutoff = dt.date.fromisoformat(dates[-1])

    def aligned(conn: Any) -> tuple[dict[str, Any], bool]:
        counts = cagg_series_counts(conn, dates)
        pending = [rd for rd in dates if counts.get(rd, 0) != series[rd]]
        if pending:
            return {"aligned": False, "pending_report_dates": pending}, False
        source = freshness.read_source_cohort(conn, cutoff=cutoff)
        verdict = freshness.probe_stage(conn, source, "cagg")
        ok = not verdict["alarm"]
        return {"aligned": ok, "pending_report_dates": [], "freshness": verdict}, ok

    try:
        # Autocommit: the request is committed before the first poll, and no
        # poll holds a snapshot open across the owner job's refresh.
        with connect(dsn, autocommit=True) as conn:
            record["job_id"] = freshness.request_profile_refresh(conn)
            record["requested"] = True
            evidence, record["polls"] = freshness.poll_alignment(
                lambda: aligned(conn), attempts=CAGG_POLL_ATTEMPTS,
                interval=CAGG_POLL_INTERVAL_SECONDS, sleeper=sleeper,
            )
    except Exception as exc:
        record["error"] = downloader.scrub(f"{type(exc).__name__}: {exc}")
        LOGGER.error("nport_secapi_monthly: cagg refresh request failed: %s", record["error"])
        return record
    record.update(evidence)
    if not record["aligned"]:
        LOGGER.error("nport_secapi_monthly: cagg refresh %s requested, alignment not confirmed "
                     "after %s polls", record["job_id"], record["polls"])
    return record


def _run_locked(
    dsn: str, today: dt.date, limit: int | None, workdir: Path, api_key: str,
    *, sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    plan = plan_for(today)
    fetched = downloader.download_months(plan.container_from, plan.container_to, str(workdir), api_key=api_key)
    paths = [c["path"] for c in fetched["containers"]]
    seed_dir = workdir / "seed"
    if seed_dir.exists():
        shutil.rmtree(seed_dir)
    manifest = converter.convert(
        paths, str(seed_dir), min_report_date=plan.report_date_from,
        partial_months=set(plan.partial_months),
    )
    stats: dict[str, Any] = {
        "plan": plan.__dict__ | {"partial_months": sorted(plan.partial_months)},
        "bytes_transferred": fetched["bytes_transferred"],
        "containers": [c["key"] for c in fetched["containers"]],
        "report_dates": {},
        "outside_window": sorted(
            [rd for rd in manifest["report_dates"] if not plan.report_date_from <= rd <= plan.report_date_to]
            + list(manifest["excluded_report_dates"])
        ),
    }
    targets = sorted(rd for rd in manifest["report_dates"] if plan.report_date_from <= rd <= plan.report_date_to)
    loaded: list[str] = []
    refresh_ready: list[str] = []
    failed: set[str] = set()
    for rd in targets:
        entry: dict[str, Any] = {"csv_rows": manifest["report_dates"][rd]["rows"],
                                 "csv_series": manifest["report_dates"][rd]["series"]}
        stats["report_dates"][rd] = entry
        if manifest["report_dates"][rd]["partial"]:
            entry["result"] = "partial"  # cannot happen inside the window; refuse rather than assume
            failed.add(rd)
            continue
        csv_series = _csv_series(seed_dir / f"{rd}.csv")
        if not csv_series or len(csv_series) != manifest["report_dates"][rd]["series"]:
            # The converter counted filings whose holdings it could not emit:
            # an upstream shape change, not a date with nothing new.
            entry["result"] = "failed"
            entry["reason"] = f"CSV holds {len(csv_series)} series, the manifest {manifest['report_dates'][rd]['series']}"
            failed.add(rd)
            LOGGER.error("nport_secapi_monthly: report_date %s: %s", rd, entry["reason"])
            continue
        existing = existing_series(dsn, rd)
        new = csv_series - existing
        entry["new_series"] = len(new)
        if not new:
            entry["result"] = "no_new_series"
            if cagg_needs_refresh(dsn, rd, len(existing)):
                _, bad = loader.verify_isin_fill(dsn, [rd])
                if bad:
                    entry['result'] = 'failed'
                    entry['reason'] = 'existing date fails ISIN verification'
                    failed.add(rd)
                else:
                    entry['cagg_stale'] = True
                    refresh_ready.append(rd)
            continue
        if limit is not None and len(loaded) + len(failed) >= limit:
            entry["result"] = "deferred_by_limit"
            continue
        # The value checks the manual workflow runs before a load (pct_of_nav
        # sums, units, foreign dates), over the series the load will insert;
        # the loader repeats the contract and whole-date ISIN check against its
        # table-aware dry run, then verifies actual inserted rows before commit.
        try:
            # Existing high-ISIN series cannot dilute a defective new cohort.
            # The loader also judges the full post-load date, including old rows.
            problems = validator.verdict(
                validator.profile_csv(str(seed_dir / f"{rd}.csv"), only_series=new),
            )
        except Exception as exc:
            entry['result'] = 'failed'
            entry['reason'] = downloader.scrub(f'{type(exc).__name__}: {exc}')
            failed.add(rd)
            continue
        if problems:
            entry["result"] = "failed"
            entry["validation"] = problems
            failed.add(rd)
            LOGGER.error("nport_secapi_monthly: report_date %s failed validation: %s", rd, problems)
            continue
        try:
            rc = load_report_date(dsn, seed_dir, rd)
        except Exception as exc:
            entry['result'] = 'failed'
            entry['reason'] = downloader.scrub(f'{type(exc).__name__}: {exc}')
            failed.add(rd)
            LOGGER.error('nport_secapi_monthly: report_date %s failed: %s', rd, entry['reason'])
            continue
        entry["loader_exit"] = rc
        if rc == 0:
            entry["result"] = "loaded"
            loaded.append(rd)
            refresh_ready.append(rd)
        else:
            # Rejected transactions commit no rows. Maintenance can fail after
            # a verified commit; a later run requests a refresh for its stale cagg.
            entry["result"] = "failed"
            failed.add(rd)
            LOGGER.error("nport_secapi_monthly: report_date %s loader exit %s", rd, rc)
    refresh: dict[str, Any] = {}
    if refresh_ready:
        # Every load above has committed (rejected ones rolled back). One
        # request covers them all: the owner policy refreshes the whole
        # invalidated window, whatever dates the lane loaded.
        table_after = report_date_counts(dsn, refresh_ready)
        for rd, counts in table_after.items():
            stats["report_dates"][rd]["table_after"] = counts
        refresh = request_cagg_refresh(
            dsn, {rd: table_after.get(rd, {}).get("series", 0) for rd in refresh_ready},
            sleeper=sleeper,
        )
        stats["cagg_refresh"] = refresh
    if not any(not manifest["report_dates"][rd]["partial"] for rd in targets):
        # N-PORT always has filings for M-5..M-3 in M-3..M: nothing to judge
        # means the containers or the converter broke, not that nothing is new.
        stats["reason"] = "conversion produced no complete report_date in the window"
    if failed or "reason" in stats or refresh.get("error"):
        stats["state"] = "failed"
    elif refresh and not refresh["aligned"]:
        stats["state"], stats["reason"] = "blocked", "cagg_refresh_pending"
    else:
        stats["state"] = "ok" if refresh_ready else "noop"
    return stats


def run(
    dsn: str, *, calc_date: str | None = None, limit: int | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    api_key = os.getenv("SEC_API_IO_KEY", "").strip()
    if not api_key:
        return {"state": "failed", "reason": "SEC_API_IO_KEY is not set"}
    today = dt.date.fromisoformat(calc_date) if calc_date else dt.datetime.now(dt.UTC).date()
    cache = os.getenv(ENV_CACHE_DIR, "").strip()
    workdir = Path(cache) if cache else Path(tempfile.mkdtemp(prefix="nport-secapi-"))
    workdir.mkdir(parents=True, exist_ok=True)
    try:
        with connect(dsn, autocommit=True) as lock_conn, advisory_lock(lock_conn, LOCK_NPORT_SECAPI_MONTHLY) as got:
            if not got:
                return {"status": "lock_busy"}
            return _run_locked(dsn, today, limit, workdir, api_key, sleeper=sleeper)
    except Exception as exc:  # the key rides in sec-api URLs; never let it reach the log
        LOGGER.error("nport_secapi_monthly failed: %s", downloader.scrub(traceback.format_exc()))
        raise RuntimeError(downloader.scrub(f"{type(exc).__name__}: {exc}")) from None
    finally:
        if not cache:
            shutil.rmtree(workdir, ignore_errors=True)
