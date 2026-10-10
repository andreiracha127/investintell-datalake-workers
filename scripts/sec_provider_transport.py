"""Machine-shared SEC provider permits and credential-safe urllib transport.

State contains bucket names and timestamps only, never keys or request URLs.
"""
from __future__ import annotations

import math
import os
import http.client
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

PAID_HOSTS = frozenset({"api.sec-api.io", "archive.sec-api.io", "edgar-mirror.sec-api.io"})


def safe_url(url: str) -> str:
    """Diagnostic URL without userinfo, query credentials or fragments."""
    parsed = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.hostname or "", parsed.path, "", ""))


def scrub(value: object, secrets=()) -> str:
    result = str(value)
    for secret in secrets:
        if secret:
            result = result.replace(secret, "[REDACTED]")
            result = result.replace(urllib.parse.quote(secret, safe=""), "[REDACTED]")
    return result


def filing_download_url(canonical_url: str, api_key: str = "") -> str:
    parsed = urllib.parse.urlsplit(canonical_url)
    host = (parsed.hostname or "").lower().rstrip(".")
    if api_key and (host == "sec.gov" or host.endswith(".sec.gov")) and parsed.path.startswith("/Archives/edgar/data/"):
        return "https://edgar-mirror.sec-api.io/" + parsed.path.split("/data/", 1)[1]
    return canonical_url


def _default_state_path() -> Path:
    configured = os.environ.get("SEC_PROVIDER_STATE_PATH")
    if configured:
        return Path(configured)
    if os.name == "nt":
        return Path(os.environ.get("PROGRAMDATA", "C:/ProgramData")) / "Investintell" / "sec-provider.sqlite3"
    return Path("/tmp/investintell-sec-provider.sqlite3")


class ProviderAuthError(Exception):
    """A provider refused the request (401/403): credentials, plan or access.

    Deliberately not a RuntimeError, ValueError or OSError, so loader fallbacks
    and recovery paths cannot absorb it; the run stops instead of retrying the
    same request elsewhere.
    """


class ProviderScheduler:
    """SQLite transactions coordinate independent processes on one machine.

    Limits map bucket names to (request count, rolling window seconds). Every
    reservation is made atomically in all applicable buckets. Waiting callers
    recheck cooldown and policy on waking rather than prebooking future slots.
    """

    def __init__(self, state_path=None, limits=None, *, clock=time.time, sleep=time.sleep, max_wait=600.0):
        self.state_path = Path(state_path) if state_path is not None else _default_state_path()
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.clock, self.sleep, self.max_wait = clock, sleep, float(max_wait)
        def setting(name, default):
            return float(os.environ.get("SEC_PROVIDER_" + name, default))
        self.limits = {
            "government": (min(10, setting("GOVERNMENT_RPS", 10)), 1.0),
            "mirror": (setting("MIRROR_RPS", 150), 1.0),
            "archive": (setting("ARCHIVE_RPS", 40), 1.0),
            "downloads": (setting("DOWNLOAD_BUDGET", 15000), setting("DOWNLOAD_WINDOW", 300)),
            "api_host": (setting("API_HOST_RPS", 40), 1.0),
            "query": (setting("QUERY_RPS", 2), 1.0),
            "fulltext": (setting("FULLTEXT_RPS", 20), 1.0),
            "other": (2, 1.0),
        }
        self.limits.update(limits or {})
        if any(not math.isfinite(count) or not math.isfinite(window) or count < 1 or window <= 0 for count, window in self.limits.values()):
            raise ValueError("Provider budgets must have count >= 1 and window > 0")
        if self.limits["government"][0] > 10 or self.limits["government"][1] < 1:
            raise ValueError("Government budget cannot exceed ten requests per second")
        for bucket, ceiling in (("mirror", 200), ("archive", 50)):
            if self.limits[bucket][0] > ceiling or self.limits[bucket][1] < 1:
                raise ValueError(f"{bucket} budget exceeds documented technical ceiling")
        if not math.isfinite(self.max_wait) or self.max_wait <= 0:
            raise ValueError("Provider maximum wait must be positive")
        with self._connect() as connection:
            connection.executescript("CREATE TABLE IF NOT EXISTS permits (bucket TEXT NOT NULL, at REAL NOT NULL); CREATE INDEX IF NOT EXISTS permits_bucket_at ON permits(bucket, at); CREATE TABLE IF NOT EXISTS cooldowns (bucket TEXT PRIMARY KEY, until_at REAL NOT NULL); CREATE TABLE IF NOT EXISTS policies (bucket TEXT PRIMARY KEY, count INTEGER NOT NULL, window REAL NOT NULL);")

    def _connect(self):
        return sqlite3.connect(str(self.state_path), timeout=5)

    @staticmethod
    def buckets(url, product=None):
        parsed = urllib.parse.urlsplit(url)
        host = (parsed.hostname or "").lower().rstrip(".")
        if host == "sec.gov" or host.endswith(".sec.gov"):
            return ("government",)
        if host == "edgar-mirror.sec-api.io":
            return ("mirror", "downloads")
        if host == "archive.sec-api.io":
            return ("archive", "downloads")
        if host == "api.sec-api.io":
            if parsed.path.startswith("/datasets"):
                return ("api_host", "archive", "downloads")
            selected = product or ("fulltext" if "full-text" in parsed.path or "fulltext" in parsed.path else "downloads" if "download" in parsed.path or "bulk" in parsed.path else "query")
            if selected not in {"downloads", "query", "fulltext"}:
                raise ValueError("Unknown SEC API product")
            return ("api_host", selected)
        return ("other",)

    def acquire(self, url, product=None):
        buckets = self.buckets(url, product)
        started = self.clock()
        while True:
            delay = 0.0
            try:
                with self._connect() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    # Sample time only while holding the lock: a timestamp taken
                    # before a contended BEGIN would be recorded in the past and
                    # let the next caller expire a permit for a request just sent.
                    now = self.clock()
                    for bucket in buckets:
                        count, window = self.limits[bucket]
                        # A stricter participant persists the shared ceiling;
                        # later processes cannot accidentally relax it.
                        connection.execute("INSERT INTO policies VALUES (?, ?, ?) ON CONFLICT(bucket) DO UPDATE SET count=min(count, excluded.count), window=max(window, excluded.window)", (bucket, int(count), window))
                        count, window = connection.execute("SELECT count, window FROM policies WHERE bucket=?", (bucket,)).fetchone()
                        connection.execute("DELETE FROM permits WHERE bucket=? AND at<=?", (bucket, now-window))
                        cooldown = connection.execute("SELECT until_at FROM cooldowns WHERE bucket=?", (bucket,)).fetchone()
                        if cooldown:
                            delay = max(delay, cooldown[0]-now)
                        total, latest = connection.execute("SELECT count(*), max(at) FROM permits WHERE bucket=?", (bucket,)).fetchone()
                        if total >= count:
                            oldest = connection.execute("SELECT at FROM permits WHERE bucket=? ORDER BY at LIMIT 1 OFFSET ?", (bucket, total-count)).fetchone()[0]
                            delay = max(delay, oldest+window-now)
                        # Pace the short windows as well as enforcing counts.
                        if window <= 1 and latest is not None:
                            delay = max(delay, latest+window/count-now)
                    if delay <= 0:
                        connection.executemany("INSERT INTO permits VALUES (?, ?)", [(bucket, now) for bucket in buckets])
                        return now
            except sqlite3.Error:
                raise RuntimeError("SEC provider coordination state unavailable") from None
            if now-started+delay > self.max_wait:
                raise RuntimeError("SEC provider permit wait exceeded configured bound")
            self.sleep(max(0.001, delay))

    reserve = acquire

    def cooldown(self, url, retry_after=None, product=None, *, fallback=1.0):
        now = self.clock()
        try:
            delay = float(retry_after)
        except (ValueError, TypeError):
            try:
                stamp = parsedate_to_datetime(str(retry_after))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                delay = stamp.timestamp()-now
            except (ValueError, TypeError, OverflowError):
                delay = fallback
        delay = max(0.0, delay)
        # Persist the full Retry-After. A very long pause fails bounded acquire
        # rather than shortening the provider's requested cooldown.
        if not math.isfinite(delay):
            delay = fallback
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.executemany("INSERT INTO cooldowns VALUES (?, ?) ON CONFLICT(bucket) DO UPDATE SET until_at=max(until_at, excluded.until_at)", [(bucket, now+delay) for bucket in self.buckets(url, product)])
        except sqlite3.Error:
            raise RuntimeError("SEC provider cooldown state unavailable") from None
        return delay


class _GovernedRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self, scheduler, product):
        self.scheduler, self.product = scheduler, product

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        old, new = urllib.parse.urlsplit(req.full_url), urllib.parse.urlsplit(newurl)
        if new.scheme not in {"http", "https"}:
            raise RuntimeError("SEC provider redirect scheme refused")
        credentials = any(name.lower() == "authorization" for name in req.headers) or bool(old.query)
        if credentials and (old.scheme, old.hostname, old.port) != (new.scheme, new.hostname, new.port):
            raise RuntimeError("SEC provider credential redirect refused")
        self.scheduler.acquire(newurl, self.product if old.hostname == new.hostname else None)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class ProviderTransport:
    def __init__(self, user_agent, api_key="", scheduler=None):
        if not str(user_agent).strip():
            raise ValueError("SEC requests require a declared User-Agent")
        self.user_agent, self.api_key = user_agent, api_key
        self.scheduler = scheduler or ProviderScheduler()

    def open(self, url, *, data=None, method=None, headers=None, authenticated=None,
             product=None, timeout=90, max_attempts=5, retry_statuses=frozenset()):
        if not 1 <= max_attempts <= 10:
            raise ValueError("SEC retries must be bounded between one and ten attempts")
        parsed = urllib.parse.urlsplit(url)
        authenticated = parsed.hostname in PAID_HOSTS if authenticated is None else authenticated
        outgoing = {**(headers or {}), "User-Agent": self.user_agent}
        if authenticated:
            if parsed.hostname not in PAID_HOSTS or parsed.scheme != "https" or not self.api_key:
                raise ValueError("SEC API authentication requires a trusted HTTPS endpoint and key")
            outgoing["Authorization"] = self.api_key
        elif any(name.lower() == "authorization" for name in outgoing):
            raise ValueError("Explicit authorization must use authenticated SEC transport")
        for attempt in range(max_attempts):
            self.scheduler.acquire(url, product)
            try:
                request = urllib.request.Request(url, data=data, headers=outgoing, method=method)
                opener = urllib.request.build_opener(_GovernedRedirect(self.scheduler, product))
                return opener.open(request, timeout=timeout)
            except urllib.error.HTTPError as exc:
                status = exc.code
                retry_url = exc.url or url
                if status == 429:
                    self.scheduler.cooldown(retry_url, exc.headers.get("Retry-After"), product=product if urllib.parse.urlsplit(retry_url).hostname == parsed.hostname else None, fallback=min(2**attempt, 16))
                exc.close()
                if status in {401, 403} and authenticated:
                    raise ProviderAuthError(f"SEC provider HTTP {status}: {scrub(safe_url(url), (self.api_key,))}") from None
                if attempt == max_attempts-1 or (status not in {408, 429, 500, 502, 503, 504} and status not in retry_statuses):
                    raise RuntimeError(f"SEC provider HTTP {status}: {scrub(safe_url(url), (self.api_key,))}") from None
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException):
                if attempt == max_attempts-1:
                    raise RuntimeError(f"SEC provider transport exhausted: {scrub(safe_url(url), (self.api_key,))}") from None
            except ValueError:
                raise RuntimeError("SEC provider request URL invalid") from None
            self.scheduler.sleep(min(2**attempt, 16))
        raise AssertionError("unreachable")
