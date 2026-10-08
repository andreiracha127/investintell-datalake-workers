"""Recurring top-up of the SEC cover-page ticker -> (CIK, class) history.

The bulk history (2009 onward) is loaded once by
``scripts/load_sec_ticker_cik_history.py``. This worker keeps it current from
the same public sources (docs/runbooks/sec-ticker-cik-history.md):

1. Take ``LOCK_SEC_TICKER_CIK_HISTORY`` (900_368); a second run reports
   ``status: lock_busy``. Refuse when the governed schema is missing.
2. Read the DERA Financial Statement and Notes listing. Every listed package
   whose name is not in ``sec_ticker_cik_packages`` is loaded, oldest first,
   and so is the newest listed package when its size differs from the recorded
   one (a republished month; it is downloaded again even when a cached copy
   exists, and a cached copy of any package is replaced when its size differs
   from the remote one). Each package is reconciled in its own transaction:
   facts it no longer carries (and no other package does) are retired, never
   deleted. The zip is deleted again unless ``SEC_TICKER_CACHE_DIR`` keeps it,
   so a run never needs more than one package of disk. ``WORKER_LIMIT`` caps
   the packages per run (the backlog resumes next run). DERA consolidates the
   monthly packages of a quarter into ``YYYYqN`` after about a year: loading the
   quarterly supersedes them (their facts retire unless a current package
   carries them), and a listed monthly package whose quarterly is loaded is
   skipped.
3. Re-fetch the EDGAR form indexes of the calc date's quarter and the one before
   (an index keeps growing until its quarter closes) and reconcile their
   registration end/start rows. The end filings of CIKs with cover data are
   read for the class they concern (at most 10 requests per second), kept under
   ``<cache>/event-docs``.
4. Re-derive the class of end events read by another parser version, or not
   read yet, as corrections.

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


def _packages_to_load(conn, client, urls: list[str]) -> list[tuple[str, bool]]:
    """(url, republished) of every package to load, oldest first. A monthly
    package whose quarterly is loaded (current) or listed is not queued: the
    quarterly supersedes it, so it never takes a WORKER_LIMIT slot."""
    recorded = dict(conn.execute(
        "SELECT source_package, package_bytes FROM sec_ticker_cik_packages"
    ).fetchall())
    quarters = {name.split("_", 1)[0] for (name,) in conn.execute(
        "SELECT source_package FROM sec_ticker_cik_packages WHERE superseded_by IS NULL"
    ).fetchall() if history.quarter_months(name)}
    names = {url: url.rsplit("/", 1)[1] for url in urls}
    quarters |= {names[url].split("_", 1)[0] for url in urls
                 if history.PACKAGE_RE.match(names[url]) and history.quarter_months(names[url])}
    listed = sorted(
        (url for url in urls if history.PACKAGE_RE.match(names[url])),
        key=lambda url: history.package_sort_key(Path(names[url])),
    )
    todo = [(url, False) for url in listed if names[url] not in recorded
            and history.covering_quarter(names[url]) not in quarters]
    if listed and names[listed[-1]] in recorded:
        newest = listed[-1]
        if _remote_size(client, newest) not in (None, recorded[names[newest]]):
            todo.append((newest, True))
    return todo


def _needs_download(client, url: str, target: Path, *, republished: bool) -> bool:
    """A republished package is always fetched again; a cached one only if it differs."""
    if republished or not target.exists():
        return True
    remote = _remote_size(client, url)
    return remote is not None and remote != target.stat().st_size


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
    documents = history.EventDocuments(workdir / "event-docs", client)
    try:
        with connect(dsn, autocommit=True) as conn, advisory_lock(
            conn, LOCK_SEC_TICKER_CIK_HISTORY
        ) as got:
            if not got:
                return {**stats, "status": "lock_busy", "state": "noop"}
            history.require_schema(conn)
            todo = _packages_to_load(conn, client, history.list_package_urls(client))
            stats["backlog"] = len(todo)
            for url, republished in todo[:limit] if limit else todo:
                name = url.rsplit("/", 1)[1]
                quarterly = history.superseded_by(conn, name)
                if quarterly is not None:
                    stats["packages"].append({"package": name,
                                              "skipped": f"superseded by {quarterly}"})
                    continue
                target = workdir / name
                if _needs_download(client, url, target, republished=republished):
                    history.fetch_package(client, url, target)
                result = history.parse_package(target)
                stats["packages"].append({
                    **result.stats(), **history.load_package(conn, result),
                    **history.supersede_monthly_packages(conn, name),
                })
                if cache is None:
                    target.unlink()
            for year, quarter in _quarters(as_of):
                target = workdir / f"{year}QTR{quarter}.form.gz"
                history.fetch_form_index(client, year, quarter, target)
                stats["form_indexes"].append(
                    history.load_form_index(conn, target, documents=documents))
                if cache is None:
                    target.unlink()
            stats["event_classes"] = history.derive_event_classes(conn, documents)
            stats["filings_fetched"] = documents.fetched
            stats["filings_failed"] = documents.failed
    finally:
        if owns_client:
            client.close()
        if cache is None:
            shutil.rmtree(workdir, ignore_errors=True)
    changed = any(
        p.get(key) for p in stats["packages"]
        for key in ("inserted", "retired", "shares_inserted", "shares_retired",
                    "superseded_retired", "superseded_shares_retired")
    ) or any(i["inserted"] or i["retired"] for i in stats["form_indexes"]) or bool(
        stats.get("event_classes", {}).get("derived"))
    stats["state"] = "ok" if changed else "noop"
    return stats
