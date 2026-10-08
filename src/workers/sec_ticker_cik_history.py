"""Recurring top-up of the SEC cover-page ticker -> (CIK, class) history.

The bulk history (2009 onward) is loaded once by
``scripts/load_sec_ticker_cik_history.py``. This worker keeps it current from
the same public sources (docs/runbooks/sec-ticker-cik-history.md):

1. Take ``LOCK_SEC_TICKER_CIK_HISTORY`` (900_368); a second run reports
   ``status: lock_busy``. Refuse when the governed schema is missing.
2. Read the DERA Financial Statement and Notes listing. Every listed package
   whose name is not in ``sec_ticker_cik_packages`` is loaded, oldest first,
   and so is the newest listed package when its size differs from the recorded
   one (a republished month). Each package is downloaded, loaded in its own
   transaction and deleted again unless ``SEC_TICKER_CACHE_DIR`` keeps it, so a
   run never needs more than one package of disk. ``WORKER_LIMIT`` caps the
   packages per run (the backlog resumes next run).
3. Re-fetch the EDGAR form indexes of the calc date's quarter and the one before
   (an index keeps growing until its quarter closes) and load their
   deregistration/delisting rows.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict``. ``state`` is ``ok``
(something new was loaded) or ``noop``; an error propagates and ``run_worker``
exits non-zero.

Env: ``WORKER=sec_ticker_cik_history``, ``DATABASE_URL``. Optional
``SEC_TICKER_CACHE_DIR`` (a mounted volume keeps the zips), ``WORKER_LIMIT``,
``WORKER_CALC_DATE``.
"""

from __future__ import annotations

import datetime as dt
import os
import shutil
import tempfile
from pathlib import Path

from scripts import load_sec_ticker_cik_history as history
from src.db import LOCK_SEC_TICKER_CIK_HISTORY, advisory_lock, connect


def _quarters(calc_date: dt.date) -> list[tuple[int, int]]:
    year, quarter = calc_date.year, (calc_date.month - 1) // 3 + 1
    previous = (year - 1, 4) if quarter == 1 else (year, quarter - 1)
    return [previous, (year, quarter)]


def _remote_size(client, url: str) -> int | None:
    response = client.head(url)
    response.raise_for_status()
    length = response.headers.get("content-length")
    return int(length) if length else None


def _packages_to_load(conn, client, urls: list[str]) -> list[str]:
    recorded = dict(conn.execute(
        "SELECT source_package, package_bytes FROM sec_ticker_cik_packages"
    ).fetchall())
    names = {url: url.rsplit("/", 1)[1] for url in urls}
    listed = sorted(
        (url for url in urls if history.PACKAGE_RE.match(names[url])),
        key=lambda url: history.package_sort_key(Path(names[url])),
    )
    todo = [url for url in listed if names[url] not in recorded]
    if listed and listed[-1] not in todo:
        newest = listed[-1]
        if _remote_size(client, newest) not in (None, recorded[names[newest]]):
            todo.append(newest)
    return todo


def run(
    dsn: str | None = None,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
    client=None,
    cache_dir: Path | None = None,
) -> dict:
    as_of = dt.date.fromisoformat(calc_date) if calc_date else dt.date.today()
    cache = cache_dir or (
        Path(os.environ["SEC_TICKER_CACHE_DIR"]) if os.getenv("SEC_TICKER_CACHE_DIR") else None
    )
    workdir = cache or Path(tempfile.mkdtemp(prefix="sec_ticker_"))
    workdir.mkdir(parents=True, exist_ok=True)
    owns_client = client is None
    client = client or history.sec_client()
    stats: dict = {"calc_date": as_of.isoformat(), "packages": [], "form_indexes": []}
    try:
        with connect(dsn, autocommit=True) as conn, advisory_lock(
            conn, LOCK_SEC_TICKER_CIK_HISTORY
        ) as got:
            if not got:
                return {**stats, "status": "lock_busy", "state": "noop"}
            history.require_schema(conn)
            todo = _packages_to_load(conn, client, history.list_package_urls(client))
            stats["backlog"] = len(todo)
            for url in todo[:limit] if limit else todo:
                target = workdir / url.rsplit("/", 1)[1]
                if not target.exists():
                    history.fetch_package(client, url, target)
                result = history.parse_package(target)
                stats["packages"].append({**result.stats(), **history.load_package(conn, result)})
                if cache is None:
                    target.unlink()
            for year, quarter in _quarters(as_of):
                target = workdir / f"{year}QTR{quarter}.form.gz"
                history.fetch_form_index(client, year, quarter, target)
                stats["form_indexes"].append(history.load_form_index(conn, target))
                if cache is None:
                    target.unlink()
    finally:
        if owns_client:
            client.close()
        if cache is None:
            shutil.rmtree(workdir, ignore_errors=True)
    changed = any(p["inserted"] or p["updated"] or p["removed"] for p in stats["packages"]) or any(
        i["inserted"] or i["updated"] for i in stats["form_indexes"]
    )
    stats["state"] = "ok" if changed else "noop"
    return stats
