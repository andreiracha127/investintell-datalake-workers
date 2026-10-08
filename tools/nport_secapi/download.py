r"""Download monthly ``form-nport`` containers from sec-api.io with the ``sec_api`` SDK.

The dataset is partitioned by FILING month (``2026/2026-07.jsonl.gz`` holds the
NPORT-P filings made public in July 2026) and refreshed daily, so the current
month's container keeps growing until the month ends.

``sec_api.Datasets.download`` is all-or-nothing (94 containers, ~7.5 GB for
``form-nport``). This module lists the containers through the SDK, requires
every requested month to be listed, and fetches each one through the SDK's own
atomic ``.tmp``-then-rename writer. A container already on disk is reused only
when its size and the remote ``updatedAt`` recorded beside it (``<file>.meta.json``)
both still match, so a re-run is a cheap sync, and a grown partial month or a
same-size republication is fetched again. The SDK retries three times over
~3 s; transient failures here get a longer backoff on top.

The API key travels as a ``?token=`` query parameter, so any exception text
that carries a URL carries the key. Everything printed goes through ``scrub``.

Usage:
  python -m tools.nport_secapi.download --from 2026-07 --to 2026-10 \
      --out E:\tmp-deploy\sec-cache\nport-secapi [--dry-run]

The key is read from ``SEC_API_IO_KEY`` (or ``SEC_API_KEY``), else from
``--dotenv`` (e.g. ``E:\investintell-light\backend\.env``).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

DATASET = "form-nport"
_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})")
_TOKEN_RE = re.compile(r"(token=)[^&\s'\"]+", re.IGNORECASE)
_HEX64_RE = re.compile(r"\b[0-9a-f]{64}\b", re.IGNORECASE)
_TRANSIENT = re.compile(r"\b(429|50[0-4])\b|timed? ?out|connection|reset", re.IGNORECASE)


def scrub(text: str) -> str:
    """Remove the API key from anything about to be logged."""
    return _HEX64_RE.sub("***", _TOKEN_RE.sub(r"\1***", str(text)))


def month_of(key: str) -> str | None:
    """``2026/2026-07.jsonl.gz`` -> ``2026-07``.

    Anchored on the basename: an unanchored ``(\\d{4})[-/](\\d{2})`` reads the
    ``2026/20`` folder prefix as the month ``2026-20`` and selects nothing.
    """
    match = _MONTH_RE.match(key.rsplit("/", 1)[-1])
    return f"{match.group(1)}-{match.group(2)}" if match else None


def load_api_key(dotenv: str | None = None) -> str:
    for var in ("SEC_API_IO_KEY", "SEC_API_KEY"):
        value = os.environ.get(var, "").strip()
        if value:
            return value
    if dotenv and Path(dotenv).is_file():
        for raw in Path(dotenv).read_text(encoding="utf-8", errors="replace").splitlines():
            name, sep, value = raw.strip().partition("=")
            if sep and name.strip() in ("SEC_API_IO_KEY", "SEC_API_KEY"):
                value = value.strip().strip("'\"")
                if value:
                    return value
    raise SystemExit("no sec-api key: set SEC_API_IO_KEY or pass --dotenv")


def _datasets(api_key: str) -> Any:
    try:
        from sec_api import Datasets
    except ModuleNotFoundError as exc:  # pragma: no cover - environment guard
        raise SystemExit(
            "the sec-api SDK is not installed in this interpreter (pip install sec-api==1.0.36; "
            "on the operator box it lives in `py -3.13`)"
        ) from exc
    return Datasets(api_key=api_key)


def months_between(month_from: str, month_to: str) -> list[str]:
    """Every ``YYYY-MM`` from ``month_from`` to ``month_to``, inclusive."""
    def index(month: str) -> int:
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}", month):
            raise ValueError(f"invalid month {month!r}; expected YYYY-MM")
        try:
            date = dt.date.fromisoformat(month + "-01")
        except ValueError:
            raise ValueError(f"invalid calendar month {month!r}") from None
        return date.year * 12 + date.month - 1

    first, last = index(month_from), index(month_to)
    if first > last:
        raise ValueError(f"reversed month window {month_from}..{month_to}")
    return [f"{i // 12:04d}-{i % 12 + 1:02d}" for i in range(first, last + 1)]


def _meta_path(dest: Path) -> Path:
    return dest.with_name(dest.name + ".meta.json")


def _cached(dest: Path, size: Any, updated_at: Any) -> bool:
    """True when ``dest`` is the remote revision: same size and same recorded ``updatedAt``."""
    if not dest.exists() or (size is not None and dest.stat().st_size != size):
        return False
    try:
        meta = json.loads(_meta_path(dest).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(meta, dict) and meta.get("size") == size and meta.get("updatedAt") == updated_at


def select_containers(containers: list[dict], month_from: str, month_to: str) -> list[dict]:
    picked = []
    for container in containers:
        month = month_of(container.get("key", ""))
        if month is not None and month_from <= month <= month_to:
            picked.append({**container, "month": month})
    return sorted(picked, key=lambda c: c["month"])


class _IncompleteDownload(RuntimeError):
    """A successful HTTP response that did not contain the advertised bytes."""


def _retry(
    operation: Callable[[], Any],
    description: str,
    *,
    attempts: int,
    sleep: Callable[[float], None],
    log: Callable[[str], None],
    retry_all: bool = False,
) -> Any:
    delay = 5.0
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:  # the SDK raises bare Exception; URLs can carry the key
            message = scrub(exc)
            transient = retry_all or isinstance(exc, _IncompleteDownload) or _TRANSIENT.search(message)
            if attempt == attempts or not transient:
                raise RuntimeError(f"{description} failed: {message}") from None
            log(f"  {description}: retryable failure ({message[:120]}); retry {attempt}/{attempts - 1} "
                f"in {delay:.0f}s")
            sleep(delay)
            delay = min(delay * 2, 60.0)


def download_months(
    month_from: str,
    month_to: str,
    out_dir: str,
    *,
    api_key: str | None = None,
    datasets: Any = None,
    dry_run: bool = False,
    attempts: int = 5,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = lambda msg: print(scrub(msg), file=sys.stderr, flush=True),
) -> dict:
    """Fetch the ``form-nport`` containers for ``month_from..month_to`` (inclusive).

    Returns ``{"containers": [...], "bytes_transferred": n, "bytes_planned": n}``
    where each container entry carries its local ``path``, the remote ``size``
    and ``updatedAt``, and whether bytes were actually ``transferred``.
    """
    requested = months_between(month_from, month_to)
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    ds = datasets if datasets is not None else _datasets(api_key or "")
    # The SDK masks some transient detail failures as "Dataset not found".
    # This dataset name is fixed, so retry listing errors before rejecting it.
    detail = _retry(lambda: ds.get_dataset_details(DATASET), f"{DATASET}: dataset listing",
                    attempts=attempts, sleep=sleep, log=log, retry_all=True)
    selected = select_containers(detail.get("containers") or [], month_from, month_to)
    missing = sorted(set(requested) - {c["month"] for c in selected})
    if missing or not selected:
        # A partial listing would convert an incomplete window and report it clean.
        raise RuntimeError(f"{DATASET}: no container listed for month(s) {missing or [month_from, month_to]}")
    planned = sum(int(c.get("size") or 0) for c in selected)
    log(f"{DATASET}: {len(selected)} container(s) {month_from}..{month_to}, {planned / 1e6:.1f} MB remote")

    results = []
    transferred = 0
    for container in selected:
        dest = Path(out_dir) / DATASET / container["key"]
        size = container.get("size")
        present = _cached(dest, size, container.get("updatedAt"))
        entry = {
            "month": container["month"], "key": container["key"], "path": str(dest),
            "size": size, "updatedAt": container.get("updatedAt"), "transferred": False,
        }
        if dry_run or present:
            log(f"  {container['key']:<24} {(size or 0) / 1e6:9.1f} MB  {'present' if present else 'would fetch'}")
            results.append(entry)
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        # The SDK skips an existing file by size alone. Fetch to a unique sibling
        # instead, then replace only verified bytes; a failed resync retains the
        # previous cache and its revision metadata on Windows as well as Linux.
        with tempfile.TemporaryDirectory(dir=dest.parent, prefix=f".{dest.name}.") as stage_dir:
            staged = Path(stage_dir) / dest.name

            def fetch() -> int:
                staged.unlink(missing_ok=True)  # never reuse a short response from an earlier attempt
                ds._download_file(container["downloadUrl"], str(staged), expected_size=size)
                got = staged.stat().st_size
                if size is not None and got != size:
                    raise _IncompleteDownload(f"size {got} != remote {size}")
                return got

            got = _retry(fetch, f"{container['key']}: download", attempts=attempts, sleep=sleep, log=log)
            metadata = Path(stage_dir) / "meta.json"
            metadata.write_text(json.dumps({"size": size, "updatedAt": container.get("updatedAt")}),
                                encoding="utf-8")
            staged.replace(dest)
            # Record the revision after publishing verified bytes. A crash
            # between the two replacements forces a refetch on the next run.
            metadata.replace(_meta_path(dest))
        transferred += got
        entry["transferred"] = True
        log(f"  {container['key']:<24} {got / 1e6:9.1f} MB  fetched")
        results.append(entry)
    summary = {"containers": results, "bytes_transferred": transferred, "bytes_planned": planned}
    if not dry_run:
        manifest = Path(out_dir) / DATASET / "download_manifest.json"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    return summary


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="month_from", required=True, help="first container month, YYYY-MM")
    ap.add_argument("--to", dest="month_to", required=True, help="last container month, YYYY-MM (inclusive)")
    ap.add_argument("--out", required=True, help="cache root; files land in <out>/form-nport/<yyyy>/<yyyy-mm>.jsonl.gz")
    ap.add_argument("--dotenv", default=None, help="dotenv file holding SEC_API_IO_KEY")
    ap.add_argument("--dry-run", action="store_true", help="list what would be fetched, transfer nothing")
    args = ap.parse_args(argv)
    try:
        months_between(args.month_from, args.month_to)
    except ValueError as exc:
        ap.error(str(exc))
    summary = download_months(
        args.month_from, args.month_to, args.out,
        api_key=load_api_key(args.dotenv), dry_run=args.dry_run,
    )
    print(scrub(f"transferred {summary['bytes_transferred'] / 1e6:.1f} MB of {summary['bytes_planned'] / 1e6:.1f} MB"),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
