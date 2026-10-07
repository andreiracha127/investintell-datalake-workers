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
5. ``CALL refresh_continuous_aggregate('cagg_nport_series_profile', ...)`` over
   each run of accepted dates that no failed date interrupts. Rejected loads
   roll back, so neither explicit refreshes nor the independent 6-hour policy
   can materialize rejected rows. A no-new-series revisit also repairs a stale
   cagg left by a previous refresh failure.

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
``ok``, ``noop`` (no new series or refresh needed) or ``failed``
(also when the containers convert to no complete target date at all);
``status == "lock_busy"`` when
another run holds the lock. ``run_worker`` exits non-zero on the last two.
"""

from __future__ import annotations

import csv
import datetime as dt
import logging
import os
import shutil
import tempfile
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg import sql

from src.db import LOCK_NPORT_SECAPI_MONTHLY, advisory_lock, connect
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
    are left out of it, and its ISIN verdict is the post-load verify's, over the
    whole date.
    """
    args = ["--seed-dir", str(seed_dir), "--only", f"{report_date}.csv", "--only-report-dates", report_date,
            "--dsn", dsn, "--workers", "1", "--skip-matview", "--new-series-only", "--secapi"]
    rc = loader.main([*args, "--dry-run"])
    if rc != 0:
        return rc
    return loader.main(args)


def refresh_ranges(loaded: list[str], failed: set[str]) -> list[tuple[str, str]]:
    """``[start, end)`` per maximal run of ``loaded`` dates with no ``failed`` date inside.

    Failed dates stay out of explicit refreshes; rejected loads roll back.
    """
    ranges: list[tuple[str, str]] = []
    run: list[str] = []
    for rd in sorted(set(loaded) | failed):
        if rd in failed:
            if run:
                ranges.append((run[0], run[-1]))
            run = []
        else:
            run.append(rd)
    if run:
        ranges.append((run[0], run[-1]))
    return [(lo, (dt.date.fromisoformat(hi) + dt.timedelta(days=1)).isoformat()) for lo, hi in ranges]


def refresh_cagg(dsn: str, start: str, end: str) -> None:
    """Re-materialize the cagg over [start, end). A procedure: no transaction block."""
    with connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CALL refresh_continuous_aggregate({}, {}::date, {}::date)").format(
            sql.Literal(CAGG), sql.Literal(start), sql.Literal(end),
        ))


def report_date_counts(dsn: str, report_dates: list[str]) -> dict[str, dict[str, int]]:
    with connect(dsn) as conn:
        rows = conn.execute(
            "SELECT report_date::text, count(*), count(DISTINCT series_id) FROM sec_nport_holdings "
            "WHERE report_date = ANY(%s::date[]) GROUP BY 1",
            (report_dates,),
        ).fetchall()
    return {r[0]: {"rows": r[1], "series": r[2]} for r in rows}


def cagg_needs_refresh(dsn: str, report_date: str, series_count: int) -> bool:
    """Recover a refresh that failed after an earlier verified commit."""
    with connect(dsn) as conn:
        count = conn.execute(
            f'SELECT count(*) FROM {CAGG} WHERE report_day = %s::date', (report_date,),
        ).fetchone()[0]
    return count != series_count


def _run_locked(dsn: str, today: dt.date, limit: int | None, workdir: Path, api_key: str) -> dict[str, Any]:
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
        # the loader's dry run checks loadability and ISIN fill, not values.
        try:
            # Percentage/MV/malformed checks apply to the actual new cohort;
            # the loader judges ISIN over the full post-load date, including old rows.
            problems = validator.verdict(
                validator.profile_csv(str(seed_dir / f"{rd}.csv"), only_series=new), include_isin=False,
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
            # a verified commit; a later run recovers its stale cagg.
            entry["result"] = "failed"
            failed.add(rd)
            LOGGER.error("nport_secapi_monthly: report_date %s loader exit %s", rd, rc)
    if refresh_ready:
        stats["cagg_refreshed"] = []
        for lo, hi in refresh_ranges(refresh_ready, failed):
            try:
                refresh_cagg(dsn, lo, hi)
                stats["cagg_refreshed"].append([lo, hi])
            except Exception as exc:
                stats.setdefault('cagg_refresh_failed', []).append({
                    'range': [lo, hi], 'reason': downloader.scrub(f'{type(exc).__name__}: {exc}'),
                })
        for rd, counts in report_date_counts(dsn, refresh_ready).items():
            stats["report_dates"][rd]["table_after"] = counts
    if not any(not manifest["report_dates"][rd]["partial"] for rd in targets):
        # N-PORT always has filings for M-5..M-3 in M-3..M: nothing to judge
        # means the containers or the converter broke, not that nothing is new.
        stats["reason"] = "conversion produced no complete report_date in the window"
    stats["state"] = ('failed' if failed or 'reason' in stats or stats.get('cagg_refresh_failed')
                      else ('ok' if refresh_ready else 'noop'))
    return stats


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
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
            return _run_locked(dsn, today, limit, workdir, api_key)
    except Exception as exc:  # the key rides in sec-api URLs; never let it reach the log
        LOGGER.error("nport_secapi_monthly failed: %s", downloader.scrub(traceback.format_exc()))
        raise RuntimeError(downloader.scrub(f"{type(exc).__name__}: {exc}")) from None
    finally:
        if not cache:
            shutil.rmtree(workdir, ignore_errors=True)
