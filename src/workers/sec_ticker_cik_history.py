"""Recurring top-up of the SEC cover-page ticker -> (CIK, class) history.

The bulk history (2009 onward) is loaded once by
``scripts/load_sec_ticker_cik_history.py``. This worker keeps it current from
the same public sources (docs/runbooks/sec-ticker-cik-history.md):

1. Take ``LOCK_SEC_TICKER_CIK_HISTORY`` (900_368); a second run reports
   ``status: lock_busy``. Refuse when the governed schema is missing. Finish
   any monthly supersession a stopped run left (resume_supersession).
2. Read the DERA Financial Statement and Notes listing. Every listed package
   whose name is not in ``sec_ticker_cik_packages`` is loaded, oldest first,
   and so is every loaded package still listed that was republished (the SEC
   updated 2010q1-2013q4 in 2024): its size, else the SEC's ETag or
   Last-Modified compared with the recorded ones, else the SHA-256 of a fresh
   download differs. A download whose digest matches records its validators, so
   a package recorded without them is downloaded once, not every run. Every
   package loaded is downloaded by the run that loads it (a cached copy is never
   trusted), and recorded with the validators of that download in the load's
   own transaction. Each package is reconciled in its own transaction: facts it
   no longer carries (and no other package does) are retired, never deleted.
   The zip is deleted again unless ``SEC_TICKER_CACHE_DIR`` keeps it, and so is
   every download of the republication check (a republished package is fetched
   again when it loads), so a run never needs more than one package of disk.
   ``WORKER_LIMIT`` caps the packages per run (the backlog resumes next run). DERA consolidates the
   monthly packages of a quarter into ``YYYYqN`` after about a year: loading the
   quarterly supersedes them (their facts retire unless a current package
   carries them), and a listed monthly package whose quarterly is loaded is
   skipped.
3. Re-fetch the EDGAR form indexes of the calc date's quarter and the one before
   (an index keeps growing until its quarter closes) and reconcile their
   registration end/start rows. The end filings of CIKs with cover data are
   read for the class they concern, and their Forms 8-A for the class they
   register (at most 10 requests per second), kept under ``<cache>/event-docs``;
   an event already read by the current parser version is carried, not fetched.
4. Re-derive the class of end and 8-A events read by another parser version, or
   not read yet, as parser corrections (dated by the filing, the old reading
   retired as never true). Steps 3 and 4 share one budget of
   ``EVENT_REDERIVE_LIMIT`` filings fetched a run: the index pass reads what it
   can (an event it cannot read keeps its earlier reading), the latest filed
   events of the backlog use the rest, and the remainder waits for later runs.

A change of the package parser (``FSN_PARSER_VERSION``) is not applied by this
worker, which reloads a package only when the SEC republishes it: the operator
re-derives the loaded packages with the loader from the workstation cache
(docs/runbooks/sec-ticker-cik-history.md, Re-derivation).

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
import time
from pathlib import Path

from scripts import load_sec_ticker_cik_history as history
from src.db import LOCK_SEC_TICKER_CIK_HISTORY, advisory_lock, connect


# Filings one run fetches, for the index pass and the backlog of events read by
# another parser version together (about ten minutes of requests at most 10 per
# second, and of the order of 150 MB of filings in the run's temporary
# directory). An event parser
# change is re-derived by the operator from the workstation cache before the
# worker runs (docs/runbooks/sec-ticker-cik-history.md); without that, the
# backlog drains over later runs instead of one run fetching every filing.
EVENT_REDERIVE_LIMIT = 2000


def _quarters(calc_date: dt.date) -> list[tuple[int, int]]:
    year, quarter = calc_date.year, (calc_date.month - 1) // 3 + 1
    previous = (year - 1, 4) if quarter == 1 else (year, quarter - 1)
    return [previous, (year, quarter)]


Validators = tuple[str | None, str | None]


def _remote(client, url: str) -> tuple[int | None, str | None, str | None]:
    """(size, ETag, Last-Modified) the SEC reports for a package (HEAD, spaced to
    stay within 10 requests per second)."""
    time.sleep(history.FILING_SPACING_S)
    response = client.head(url)
    response.raise_for_status()
    length = response.headers.get("content-length")
    return (int(length) if length else None, response.headers.get("etag"),
            response.headers.get("last-modified"))


def _republished(conn, client, url: str, target: Path,
                 stored: tuple) -> tuple[bool, Validators | None]:
    """Whether a listed package differs from its loaded version: by size, else by
    the SEC's ETag or Last-Modified when both sides have it, else by the SHA-256
    of a fresh download (left at ``target``; its validators are returned). When
    that download matches the loaded version, its validators are recorded, so the
    next run compares them instead of downloading again."""
    size, sha256, etag, last_modified = stored
    remote_size, remote_etag, remote_modified = _remote(client, url)
    if remote_size is not None and remote_size != size:
        return True, None
    if remote_etag and etag:
        return remote_etag != etag, None
    if remote_modified and last_modified:
        return remote_modified != last_modified, None
    validators = history.fetch_package(client, url, target)
    if history._sha256(target) != sha256:
        return True, validators
    history.record_remote_validators(conn, target.name, etag=validators[0],
                                     last_modified=validators[1])
    return False, None


def _packages_to_load(
    conn, client, urls: list[str], workdir: Path, *, keep: bool = True,
) -> tuple[list[tuple[str, bool]], dict[str, Validators]]:
    """(url, republished) of every package to load, oldest first, and the
    validators of the republished packages this check downloaded into the
    persistent cache ``workdir`` (they are loaded from there). Without ``keep``
    (no persistent cache) every digest-check download is deleted at once, whatever
    it shows: the run holds one package at a time, and a republished package is
    downloaded again when it loads, within WORKER_LIMIT.
    New: listed and not loaded. Republished: loaded, current (not superseded) and
    listed, and different from the loaded version (every such package is
    checked, not only the newest). A monthly package whose quarterly is loaded
    (current) or listed is not queued: the quarterly supersedes it, so it never
    takes a WORKER_LIMIT slot."""
    recorded = {name for (name,) in conn.execute(
        "SELECT source_package FROM sec_ticker_cik_packages").fetchall()}
    current = {row[0]: row[1:] for row in conn.execute(
        "SELECT source_package, package_bytes, package_sha256, remote_etag, "
        "remote_last_modified FROM sec_ticker_cik_packages WHERE superseded_by IS NULL"
    ).fetchall()}
    quarters = {name.split("_", 1)[0] for name in current if history.quarter_months(name)}
    names = {url: url.rsplit("/", 1)[1] for url in urls}
    quarters |= {names[url].split("_", 1)[0] for url in urls
                 if history.PACKAGE_RE.match(names[url]) and history.quarter_months(names[url])}
    listed = [url for url in urls if history.PACKAGE_RE.match(names[url])]
    todo = [(url, False) for url in listed if names[url] not in recorded
            and history.covering_quarter(names[url]) not in quarters]
    fetched: dict[str, Validators] = {}
    for url in listed:
        name = names[url]
        if name not in current or history.covering_quarter(name) in quarters:
            continue  # not loaded, or about to be superseded by its quarterly
        target = workdir / name
        republished, validators = _republished(conn, client, url, target, current[name])
        if republished:
            todo.append((url, True))
            if keep and validators is not None:
                fetched[name] = validators
        if not keep:
            target.unlink(missing_ok=True)
    todo.sort(key=lambda item: history.package_sort_key(Path(names[item[0]])))
    return todo, fetched


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
    documents = history.EventDocuments(workdir / "event-docs", client,
                                       budget=EVENT_REDERIVE_LIMIT)
    try:
        with connect(dsn, autocommit=True) as conn, advisory_lock(
            conn, LOCK_SEC_TICKER_CIK_HISTORY
        ) as got:
            if not got:
                return {**stats, "status": "lock_busy", "state": "noop"}
            history.prepare_session(conn)
            history.require_schema(conn)
            stats["resumed_supersession"] = history.resume_supersession(conn)
            todo, fetched = _packages_to_load(conn, client, history.list_package_urls(client),
                                              workdir, keep=cache is not None)
            stats["backlog"] = len(todo)
            stats["republished"] = sorted(url.rsplit("/", 1)[1] for url, again in todo if again)
            for url, _ in todo[:limit] if limit else todo:
                name = url.rsplit("/", 1)[1]
                quarterly = history.superseded_by(conn, name)
                if quarterly is not None:
                    stats["packages"].append({"package": name,
                                              "skipped": f"superseded by {quarterly}"})
                    continue
                target = workdir / name
                # The bytes loaded are always those this run downloaded, so the
                # validators recorded with them are theirs.
                validators = fetched.get(name) or history.fetch_package(client, url, target)
                result = history.parse_package(target)
                stats["packages"].append({
                    **result.stats(), **history.load_package(conn, result, validators=validators),
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
            stats["event_classes"] = history.derive_event_classes(
                conn, documents, limit=documents.remaining)
            stats["filings_fetched"] = documents.fetched
            stats["filings_failed"] = documents.failed
            stats["filings_rejected"] = documents.rejected
            stats["filings_deferred"] = documents.deferred
    finally:
        if owns_client:
            client.close()
        if cache is None:
            shutil.rmtree(workdir, ignore_errors=True)
    changed = any(
        p.get(key) for p in [*stats["packages"], *stats.get("resumed_supersession", [])]
        for key in ("inserted", "retired", "shares_inserted", "shares_retired",
                    "superseded_retired", "superseded_shares_retired")
    ) or any(i["inserted"] or i["retired"] for i in stats["form_indexes"]) or bool(
        stats.get("event_classes", {}).get("derived"))
    stats["state"] = "ok" if changed else "noop"
    return stats
