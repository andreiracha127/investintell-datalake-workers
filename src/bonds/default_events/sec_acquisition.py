"""Identified, rate-limited, cached SEC EDGAR acquisition for bond default evidence.

Responsibilities (implementation contract §3.1/§3.3): identified requests, one
process-wide <=4 requests/second budget shared by every client and thread,
bounded retries (HTTP 403 stops all further requests, HTTP 429 honours
``Retry-After`` with at most three retries), a content-addressed on-disk cache,
EDGAR index parsing and acceptance-header metadata fetched once per accession.

This module holds no model, label or admission logic. Knowledge-time
conversion is delegated to :func:`contracts.edgar_acceptance_to_utc`.
"""

from __future__ import annotations

import datetime as dt
import email.utils
import hashlib
import json
import os
import re
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Self
from urllib.parse import urlsplit

from .contracts import date_only_public_available_at, edgar_acceptance_to_utc

USER_AGENT = "InvestIntell research admin@investintell.com"
MAX_REQUESTS_PER_SECOND = 4.0
MAX_RETRIES = 3
#: A demanded wait longer than this stops the job instead of sleeping unbounded.
MAX_RETRY_AFTER_SECONDS = 120.0
DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 60.0
MAX_REDIRECTS = 3
SEC_HOSTS = frozenset({"www.sec.gov", "sec.gov", "data.sec.gov"})
RETRYABLE_STATUSES = frozenset({500, 502, 503, 504})
ARCHIVES_ROOT = "https://www.sec.gov/Archives/edgar"
PUBLIC_NPORT_TYPES = frozenset({"NPORT-P", "NPORT-P/A"})

_ACCESSION = re.compile(r"\d{10}-\d{2}-\d{6}")
_HEADER_LINE = re.compile(r"<([A-Z0-9][A-Z0-9-]*)>([^\r\n]*)")
_INDEX_LINE = re.compile(
    r"^(?P<form>\S.*?)\s{2,}(?P<company>\S.*?)\s{2,}(?P<cik>\d{1,10})\s+"
    r"(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<file>edgar/\S+)$"
)
_INDEX_ACCESSION = re.compile(r"(\d{10}-\d{2}-\d{6})\.txt$")

UTC = dt.timezone.utc


class SecAcquisitionError(RuntimeError):
    """Base class for SEC acquisition failures."""


class SecAccessDenied(SecAcquisitionError):
    """HTTP 403: SEC refused access. All further requests of the budget stop."""


class SecRateLimited(SecAcquisitionError):
    """HTTP 429 persisted after the bounded retries (or demanded an unbounded wait)."""


class SecNotFound(SecAcquisitionError):
    """HTTP 404 for the requested SEC resource."""


class SecResponseError(SecAcquisitionError):
    """Any other non-success response, transport failure or oversized body."""


class SecHeaderError(SecAcquisitionError):
    """An EDGAR header document is malformed or does not match the accession."""


class SecIndexError(SecAcquisitionError):
    """An EDGAR form index line or layout is malformed."""


# ---------------------------------------------------------------------------
# Shared request budget
# ---------------------------------------------------------------------------
class RequestBudget:
    """Thread-safe request pacing shared by every :class:`SecClient` of a process.

    Each request start is *granted* under the lock at the actual clock time, only when
    that time is at least ``1/max_per_second`` after the previous grant and not before any
    ``Retry-After`` deadline. A caller that must wait computes the wait under the lock,
    sleeps outside it and rechecks - so late wake-ups never create bursts and a
    :meth:`defer` issued while callers are waiting applies to them. No half-open
    one-second window therefore contains more than ``max_per_second`` grants, however many
    threads or clients share the budget. The client invokes the transport immediately after
    the grant. A 403 stops the budget permanently. Cross-process exclusion is provided by
    :class:`CacheDirLock` (one acquiring process per cache directory).
    """

    def __init__(
        self,
        max_per_second: float = MAX_REQUESTS_PER_SECOND,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not 0 < max_per_second <= MAX_REQUESTS_PER_SECOND:
            raise ValueError("max_per_second must be in (0, 4]")
        self.interval = 1.0 / max_per_second
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._last_start: float | None = None
        self._blocked_until: float | None = None
        self._stop_reason: str | None = None
        #: Recent granted start times (bounded; diagnostics and tests).
        self.starts: deque[float] = deque(maxlen=10_000)

    @property
    def stopped(self) -> str | None:
        return self._stop_reason

    def stop(self, reason: str) -> None:
        with self._lock:
            if self._stop_reason is None:
                self._stop_reason = reason

    def check(self) -> None:
        if self._stop_reason is not None:
            raise SecAccessDenied(f"sec_budget_stopped:{self._stop_reason}")

    def defer(self, seconds: float) -> None:
        """No start is granted before ``now + seconds`` (applies to already-waiting callers)."""
        with self._lock:
            earliest = self._clock() + max(0.0, seconds)
            if self._blocked_until is None or self._blocked_until < earliest:
                self._blocked_until = earliest

    def acquire(self) -> float:
        """Wait until a start is permitted, grant it at the actual time and return that time."""
        while True:
            self.check()
            with self._lock:
                if self._stop_reason is not None:
                    raise SecAccessDenied(f"sec_budget_stopped:{self._stop_reason}")
                now = self._clock()
                earliest = now
                if self._last_start is not None:
                    earliest = max(earliest, self._last_start + self.interval)
                if self._blocked_until is not None:
                    earliest = max(earliest, self._blocked_until)
                if now >= earliest:
                    self._last_start = now
                    self.starts.append(now)
                    return now
                wait = earliest - now
            self._sleep(wait)


_SHARED_BUDGET = RequestBudget()


def shared_budget() -> RequestBudget:
    """The single process-wide budget used by default by every client."""
    return _SHARED_BUDGET


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes
    url: str


Transport = Callable[[str, Mapping[str, str], int], HttpResponse]


class TransportFailure(SecAcquisitionError):
    """A retryable transport-level failure (connection, timeout)."""


def httpx_transport(timeout: float = DEFAULT_TIMEOUT_SECONDS) -> Transport:
    """Default transport: streamed ``httpx`` GET that never follows redirects itself."""
    import httpx

    client = httpx.Client(timeout=timeout, follow_redirects=False)

    def send(url: str, headers: Mapping[str, str], max_bytes: int) -> HttpResponse:
        try:
            with client.stream("GET", url, headers=dict(headers)) as response:
                chunks: list[bytes] = []
                size = 0
                if response.status_code == 200:
                    for chunk in response.iter_bytes():
                        size += len(chunk)
                        if size > max_bytes:
                            raise SecResponseError(f"response_too_large:{url}")
                        chunks.append(chunk)
                return HttpResponse(
                    status=response.status_code,
                    headers={k.lower(): v for k, v in response.headers.items()},
                    body=b"".join(chunks),
                    url=url,
                )
        except httpx.TransportError as exc:
            raise TransportFailure(f"transport:{type(exc).__name__}") from exc

    return send


def parse_retry_after(value: str | None, now: dt.datetime) -> float | None:
    """Seconds to wait from a ``Retry-After`` header (delta-seconds or HTTP-date)."""
    if value is None:
        return None
    text = value.strip()
    if re.fullmatch(r"\d+", text):
        return float(int(text))
    try:
        when = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - now).total_seconds())


def _check_sec_url(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname not in SEC_HOSTS:
        raise SecResponseError(f"non_sec_url_refused:{url}")


# ---------------------------------------------------------------------------
# Content-addressed cache and client
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FetchResult:
    url: str
    status: int
    sha256: str
    size: int
    content_type: str | None
    body: bytes
    fetched_at: dt.datetime
    from_cache: bool


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


class ContentCache:
    """Objects stored by SHA-256; URL entries point at objects and are verified on read."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _object_path(self, sha: str) -> Path:
        return self.root / "objects" / sha[:2] / sha

    def _url_path(self, url: str) -> Path:
        return self.root / "urls" / (hashlib.sha256(url.encode("utf-8")).hexdigest() + ".json")

    def put(self, url: str, body: bytes, *, content_type: str | None, fetched_at: dt.datetime) -> str:
        sha = hashlib.sha256(body).hexdigest()
        obj = self._object_path(sha)
        if not obj.exists():
            _atomic_write(obj, body)
        meta = {
            "content_type": content_type,
            "fetched_at": fetched_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "sha256": sha,
            "size": len(body),
            "url": url,
        }
        _atomic_write(self._url_path(url), json.dumps(meta, sort_keys=True).encode("utf-8"))
        return sha

    def get(self, url: str) -> FetchResult | None:
        meta_path = self._url_path(url)
        if not meta_path.exists():
            return None
        meta = json.loads(meta_path.read_bytes())
        if meta.get("url") != url:
            return None
        obj = self._object_path(meta["sha256"])
        if not obj.exists():
            return None
        body = obj.read_bytes()
        if hashlib.sha256(body).hexdigest() != meta["sha256"]:
            raise SecResponseError(f"cache_object_corrupt:{meta['sha256']}")
        fetched = dt.datetime.strptime(meta["fetched_at"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
        return FetchResult(
            url=url, status=200, sha256=meta["sha256"], size=len(body),
            content_type=meta.get("content_type"), body=body, fetched_at=fetched, from_cache=True,
        )


class SecCacheLocked(SecAcquisitionError):
    """Another process already owns this SEC cache directory."""


class CacheDirLock:
    """Exclusive OS-level lock on ``<cache>/.sec_acquisition.lock`` for one process.

    Acquisition is single-process per cache directory: the lock is held for the owning
    clients' lifetime, a second *process* fails loud with :class:`SecCacheLocked`, and
    clients inside the owning process share it (reference-counted).
    """

    LOCK_NAME = ".sec_acquisition.lock"
    _registry: ClassVar[dict[str, CacheDirLock]] = {}
    _registry_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, key: str, handle: Any) -> None:
        self.key = key
        self._handle = handle
        self._refs = 1

    @classmethod
    def acquire(cls, cache_dir: Path) -> CacheDirLock:
        cache_dir.mkdir(parents=True, exist_ok=True)
        path = (cache_dir / cls.LOCK_NAME).resolve()
        key = os.path.normcase(str(path))
        with cls._registry_lock:
            held = cls._registry.get(key)
            if held is not None:
                held._refs += 1
                return held
            handle = open(path, "a+b")  # noqa: SIM115 - held for the lock lifetime
            try:
                cls._os_lock(handle)
            except OSError as exc:
                handle.close()
                raise SecCacheLocked(f"sec_cache_dir_locked_by_another_process:{cache_dir}") from exc
            lock = cls(key, handle)
            cls._registry[key] = lock
            return lock

    @staticmethod
    def _os_lock(handle: Any) -> None:
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def release(self) -> None:
        with self._registry_lock:
            self._refs -= 1
            if self._refs > 0 or self._handle is None:
                return
            try:
                self._handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                self._handle.close()
                self._handle = None
                self._registry.pop(self.key, None)


class SecClient:
    """Identified SEC GET client with shared pacing, bounded retries and a CAS cache.

    Owns its cache directory exclusively per process (:class:`CacheDirLock`); call
    :meth:`close` or use it as a context manager to release the lock.
    """

    def __init__(
        self,
        cache_dir: Path,
        *,
        transport: Transport | None = None,
        budget: RequestBudget | None = None,
        user_agent: str = USER_AGENT,
        max_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        max_retries: int = MAX_RETRIES,
        max_retry_after: float = MAX_RETRY_AFTER_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], dt.datetime] = lambda: dt.datetime.now(UTC),
        lock_cache_dir: bool = True,
    ) -> None:
        if "@" not in user_agent or not user_agent.strip():
            raise ValueError("SEC user agent must identify a contact address")
        if not 0 <= max_retries <= MAX_RETRIES:
            raise ValueError("max_retries must be within [0, 3]")
        #: Exclusive per-process ownership of the cache directory (and thus of the SEC
        #: budget used with it); ``lock_cache_dir=False`` is for offline unit tests only.
        self.cache_lock: CacheDirLock | None = CacheDirLock.acquire(Path(cache_dir)) if lock_cache_dir else None
        self.cache = ContentCache(cache_dir)
        self._transport = transport if transport is not None else httpx_transport()
        self.budget = budget if budget is not None else shared_budget()
        self.user_agent = user_agent
        self.max_bytes = max_bytes
        self.max_retries = max_retries
        self.max_retry_after = max_retry_after
        self._sleep = sleep
        self._now = now
        self.network_requests = 0

    def close(self) -> None:
        if self.cache_lock is not None:
            self.cache_lock.release()
            self.cache_lock = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _headers(self) -> dict[str, str]:
        return {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}

    def get(self, url: str, *, use_cache: bool = True) -> FetchResult:
        _check_sec_url(url)
        if use_cache:
            cached = self.cache.get(url)
            if cached is not None:
                return cached
        target = url
        retries = 0
        redirects = 0
        while True:
            self.budget.acquire()
            self.network_requests += 1
            try:
                response = self._transport(target, self._headers(), self.max_bytes)
            except TransportFailure as exc:
                if retries >= self.max_retries:
                    raise SecResponseError(f"transport_retries_exhausted:{url}") from exc
                retries += 1
                self._sleep(float(2**retries))
                continue
            status = response.status
            if status == 200:
                if len(response.body) > self.max_bytes:
                    raise SecResponseError(f"response_too_large:{url}")
                fetched_at = self._now()
                content_type = response.headers.get("content-type")
                sha = self.cache.put(url, response.body, content_type=content_type, fetched_at=fetched_at)
                return FetchResult(
                    url=url, status=200, sha256=sha, size=len(response.body),
                    content_type=content_type, body=response.body, fetched_at=fetched_at,
                    from_cache=False,
                )
            if status == 403:
                self.budget.stop(f"403:{url}")
                raise SecAccessDenied(f"sec_403:{url}")
            if status == 429:
                wait = parse_retry_after(response.headers.get("retry-after"), self._now())
                wait = float(2**retries) if wait is None else wait
                if retries >= self.max_retries or wait > self.max_retry_after:
                    raise SecRateLimited(f"sec_429:{url}:retries={retries}:wait={wait}")
                retries += 1
                self.budget.defer(wait)
                continue
            if status in RETRYABLE_STATUSES:
                if retries >= self.max_retries:
                    raise SecResponseError(f"sec_{status}_retries_exhausted:{url}")
                retries += 1
                self._sleep(float(2**retries))
                continue
            if status in (301, 302, 303, 307, 308):
                location = response.headers.get("location")
                if location is None or redirects >= MAX_REDIRECTS:
                    raise SecResponseError(f"sec_redirect_refused:{url}")
                if location.startswith("/"):
                    parts = urlsplit(target)
                    location = f"{parts.scheme}://{parts.netloc}{location}"
                _check_sec_url(location)
                redirects += 1
                target = location
                continue
            if status == 404:
                raise SecNotFound(f"sec_404:{url}")
            raise SecResponseError(f"sec_{status}:{url}")


# ---------------------------------------------------------------------------
# Acceptance headers
# ---------------------------------------------------------------------------
def normalize_cik(value: object) -> str | None:
    """Ten-digit zero-padded CIK, or ``None`` when the raw value is not a CIK."""
    if value is None:
        return None
    text = str(value).strip()
    if not re.fullmatch(r"\d{1,10}", text) or int(text) == 0:
        return None
    return text.zfill(10)


def accession_header_url(cik: str, accession_number: str) -> str:
    """URL of the public ``-index-headers.html`` document of one accession."""
    if not _ACCESSION.fullmatch(accession_number):
        raise SecHeaderError(f"accession_invalid:{accession_number}")
    padded = normalize_cik(cik)
    if padded is None:
        raise SecHeaderError(f"cik_invalid:{cik}")
    folder = accession_number.replace("-", "")
    return f"{ARCHIVES_ROOT}/data/{int(padded)}/{folder}/{accession_number}-index-headers.html"


def _parse_yyyymmdd(value: str | None) -> dt.date | None:
    if value is None or not re.fullmatch(r"\d{8}", value):
        return None
    try:
        return dt.date(int(value[:4]), int(value[4:6]), int(value[6:]))
    except ValueError:
        return None


def extract_sec_header(document: bytes) -> str:
    """The raw ``<SEC-HEADER>`` SGML block (LF line endings) of a header document.

    Accepts a ``.hdr.sgml`` file or an ``-index-headers.html`` page (whose HTML
    comment carries the unescaped block). Exactly one unescaped block is required.
    """
    text = document.decode("utf-8", errors="strict").replace("\r\n", "\n")
    starts = [m.start() for m in re.finditer(r"<SEC-HEADER>", text)]
    ends = [m.end() for m in re.finditer(r"</SEC-HEADER>", text)]
    if len(starts) != 1 or len(ends) != 1 or ends[0] <= starts[0]:
        raise SecHeaderError(f"sec_header_block_count:starts={len(starts)}:ends={len(ends)}")
    return text[starts[0]:ends[0]]


@dataclass(frozen=True)
class AcceptanceHeader:
    """Parsed EDGAR submission header of one accession (raw text and hash kept).

    ``filer_ciks`` holds every ``<CIK>`` in the header (filer, subject company and
    filed-by sections), zero-padded and sorted.
    """

    accession_number: str
    url: str
    document_sha256: str
    header_text: str
    header_sha256: str
    acceptance_raw: str
    acceptance_at: dt.datetime
    submission_type: str | None
    filing_date: dt.date | None
    period: dt.date | None
    filer_ciks: tuple[str, ...]
    items: tuple[str, ...]
    retrieved_at: dt.datetime

    @property
    def is_public_nport(self) -> bool:
        return self.submission_type in PUBLIC_NPORT_TYPES

    def to_record(self) -> dict[str, Any]:
        return {
            "acceptance_at": self.acceptance_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "acceptance_raw": self.acceptance_raw,
            "accession_number": self.accession_number,
            "document_sha256": self.document_sha256,
            "filer_ciks": list(self.filer_ciks),
            "filing_date": None if self.filing_date is None else self.filing_date.isoformat(),
            "header_sha256": self.header_sha256,
            "header_text": self.header_text,
            "items": list(self.items),
            "period": None if self.period is None else self.period.isoformat(),
            "retrieved_at": self.retrieved_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "submission_type": self.submission_type,
            "url": self.url,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> AcceptanceHeader:
        header = parse_acceptance_header(
            record["header_text"].encode("utf-8"),
            accession_number=record["accession_number"],
            url=record["url"],
            document_sha256=record["document_sha256"],
            retrieved_at=dt.datetime.strptime(record["retrieved_at"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC),
        )
        if header.to_record() != dict(record):
            raise SecHeaderError(f"header_record_inconsistent:{record['accession_number']}")
        return header


def parse_acceptance_header(
    document: bytes,
    *,
    accession_number: str,
    url: str,
    document_sha256: str,
    retrieved_at: dt.datetime,
) -> AcceptanceHeader:
    """Parse the SEC header block; America/New_York acceptance -> UTC via the contract helper."""
    block = extract_sec_header(document)
    fields: dict[str, list[str]] = {}
    for match in _HEADER_LINE.finditer(block):
        fields.setdefault(match.group(1), []).append(match.group(2).strip())
    accessions = fields.get("ACCESSION-NUMBER", [])
    if accessions != [accession_number]:
        raise SecHeaderError(f"accession_mismatch:{accession_number}:{accessions}")
    acceptance = fields.get("ACCEPTANCE-DATETIME", [])
    if len(acceptance) != 1:
        raise SecHeaderError(f"acceptance_datetime_count:{len(acceptance)}")
    try:
        acceptance_at = edgar_acceptance_to_utc(acceptance[0])
    except ValueError as exc:
        raise SecHeaderError(f"acceptance_datetime_invalid:{acceptance[0]}") from exc
    types = fields.get("TYPE", [])
    ciks = sorted({c for c in (normalize_cik(v) for v in fields.get("CIK", [])) if c is not None})
    return AcceptanceHeader(
        accession_number=accession_number,
        url=url,
        document_sha256=document_sha256,
        header_text=block,
        header_sha256=hashlib.sha256(block.encode("utf-8")).hexdigest(),
        acceptance_raw=acceptance[0],
        acceptance_at=acceptance_at,
        submission_type=types[0] if len(types) == 1 else None,
        filing_date=_parse_yyyymmdd((fields.get("FILING-DATE") or [None])[0]),
        period=_parse_yyyymmdd((fields.get("PERIOD") or [None])[0]),
        filer_ciks=tuple(ciks),
        items=tuple(fields.get("ITEMS", [])),
        retrieved_at=retrieved_at.astimezone(UTC),
    )


class AcceptanceHeaderStore:
    """Acceptance headers fetched at most once per accession and persisted as JSON."""

    def __init__(self, client: SecClient, directory: Path) -> None:
        self.client = client
        self.directory = Path(directory)
        self._lock = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    def _path(self, accession_number: str) -> Path:
        return self.directory / f"{accession_number}.json"

    def cached(self, accession_number: str) -> AcceptanceHeader | None:
        path = self._path(accession_number)
        if not path.exists():
            return None
        return AcceptanceHeader.from_record(json.loads(path.read_bytes()))

    def get(self, accession_number: str, cik: str) -> AcceptanceHeader:
        if not _ACCESSION.fullmatch(accession_number):
            raise SecHeaderError(f"accession_invalid:{accession_number}")
        with self._lock:
            lock = self._locks.setdefault(accession_number, threading.Lock())
        with lock:
            existing = self.cached(accession_number)
            if existing is not None:
                return existing
            url = accession_header_url(cik, accession_number)
            fetched = self.client.get(url)
            header = parse_acceptance_header(
                fetched.body,
                accession_number=accession_number,
                url=url,
                document_sha256=fetched.sha256,
                retrieved_at=fetched.fetched_at,
            )
            _atomic_write(
                self._path(accession_number),
                json.dumps(header.to_record(), sort_keys=True, ensure_ascii=False).encode("utf-8"),
            )
            return header


# ---------------------------------------------------------------------------
# EDGAR form indexes
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FormIndexEntry:
    form_type: str
    company_name: str
    cik: str
    date_filed: dt.date
    file_name: str
    accession_number: str


def index_date_public_available_at(entry: FormIndexEntry) -> dt.datetime:
    """Conservative knowledge time from a date-only EDGAR index row.

    EDGAR dates are America/New_York calendar dates; date-only evidence becomes public
    at the *next* day boundary in that timezone (policy ``date_only_public_evidence``),
    never at midnight or noon of the filing date. Prefer the accession header.
    """
    return date_only_public_available_at(entry.date_filed, "America/New_York")


def full_index_url(year: int, quarter: int) -> str:
    if quarter not in (1, 2, 3, 4) or year < 1993:
        raise SecIndexError(f"index_period_invalid:{year}Q{quarter}")
    return f"{ARCHIVES_ROOT}/full-index/{year}/QTR{quarter}/form.idx"


def daily_index_url(day: dt.date) -> str:
    quarter = (day.month - 1) // 3 + 1
    return f"{ARCHIVES_ROOT}/daily-index/{day.year}/QTR{quarter}/form.{day:%Y%m%d}.idx"


def parse_form_index(data: bytes, *, forms: Iterable[str] | None = None) -> list[FormIndexEntry]:
    """Parse an EDGAR ``form.idx`` (full or daily). Malformed data lines raise.

    Columns are resolved from the right (file name, date, CIK) because data rows
    do not follow the header's column offsets; form types may contain single
    spaces (``SC 13D``) but never runs of two spaces.
    """
    wanted = None if forms is None else frozenset(forms)
    text = data.decode("latin-1")
    lines = text.split("\n")
    try:
        start = next(i for i, line in enumerate(lines) if re.fullmatch(r"-{20,}\s*", line)) + 1
    except StopIteration as exc:
        raise SecIndexError("form_index_separator_missing") from exc
    header = lines[start - 2] if start >= 2 else ""
    if not header.startswith("Form Type"):
        raise SecIndexError("form_index_header_missing")
    entries: list[FormIndexEntry] = []
    for number, raw in enumerate(lines[start:], start=start + 1):
        line = raw.rstrip()
        if not line:
            continue
        match = _INDEX_LINE.match(line)
        if match is None:
            raise SecIndexError(f"form_index_line_malformed:{number}")
        form = match.group("form").strip()
        if wanted is not None and form not in wanted:
            continue
        accession = _INDEX_ACCESSION.search(match.group("file"))
        if accession is None:
            raise SecIndexError(f"form_index_file_name_malformed:{number}")
        entries.append(
            FormIndexEntry(
                form_type=form,
                company_name=match.group("company").strip(),
                cik=str(int(match.group("cik"))).zfill(10),
                date_filed=dt.date.fromisoformat(match.group("date")),
                file_name=match.group("file"),
                accession_number=accession.group(1),
            )
        )
    return entries
