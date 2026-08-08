"""Shared Finnhub bond client for the live daily feed.

Standalone reimplementation of the essentials, in this repo's provider-client
idiom (cf. ``_tiingo.py`` / ``_openfigi.py``): one module, no cross-repository
import, every trap the serving repository's client already paid for carried over
as CODE, not as a comment.

Four endpoints:

  ``GET /bond/profile?cusip=``    CUSIP -> ISIN. Note the response's own
                                  ``cusip`` field may be null even when the
                                  query was by CUSIP; ``isin`` is the one that
                                  matters.
  ``GET /bond/price?isin=&from=&to=``
                                  Daily candles. The live response carries
                                  ``{s, t, c, y}`` ONLY -- close and yield --
                                  despite the documented OHLCV candle shape.
                                  ``c`` is clean percent of par, ``y`` is a
                                  yield in PERCENT.
  ``GET /bond/tick?isin=&date=&exchange=&format=json``
                                  Columnar tick arrays. TWO traps, both
                                  root-caused 2026-08-07: the endpoint
                                  **302-redirects to tick.finnhub.io**, so the
                                  transport MUST follow redirects; and without
                                  ``format=json`` the redirect target serves CSV
                                  carrying a Go-fmt bug (``%!d(float64=...)``).
                                  Tick responses carry NO rate-limit headers, so
                                  pacing there is the base sleep plus 429
                                  backoff and nothing else.
  ``GET /bond/yield-curve?code=`` One tenor's full history, newest first, as
                                  ``{code, data:[{d, v}]}`` with ``v`` in
                                  PERCENT.

Rate posture is HEADER-DRIVEN where headers exist: the API reports
``X-Ratelimit-Limit`` / ``-Remaining`` / ``-Reset`` on price and profile
responses, and when the window is nearly spent the client sleeps to the reset
instant instead of hammering into 429s. Measured sustained throughput with the
default pacing is ~190 CUSIPs/minute.

There is deliberately NO on-disk cache here (the serving repository's client has
one because it drives interactive rebuilds from a workstation). This worker runs
in a container with no persistent volume, and its cache is the DATABASE: the
watermark makes every run a delta, so a re-run costs a small window, not a
re-download.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

BASE_URL = "https://finnhub.io/api/v1"
MAX_RETRIES = 6
BACKOFF_BASE_S = 2.0
BACKOFF_CAP_S = 120.0
DEFAULT_TIMEOUT_S = 45.0
#: ~190 CUSIPs/min sustained, which is what the measured budget supports.
DEFAULT_BASE_SLEEP_S = 0.15

#: Consecutive transient exhaustions after which a sweep gives up rather than
#: spending its whole window on a provider that is down. The run then reports
#: ``aborted`` and exits non-zero (run_worker's budget contract), so a truncated
#: sweep is never painted green.
#:
#: Costed against the backoff ladder: one exhausted logical request now spends
#: 2+4+8+16+32+64 = 126s sleeping (the seventh attempt no longer sleeps -- see
#: ``_get_json``), so the worst case before an outage is REPORTED is 25 x 126s
#: ~= 52min of backoff, against ~102min while the trailing 120s sleep was still
#: charged per request. Wall clock adds the connect/read timeouts on top. This
#: constant is the lever if that window still has to shrink.
MAX_CONSECUTIVE_FAILURES = 25


class FinnhubTransientError(RuntimeError):
    """Retries exhausted on 429/5xx/network for one logical request."""


class FinnhubConfigError(RuntimeError):
    """A non-retryable 4xx: wrong key, missing entitlement, bad parameter."""


class FinnhubProfileError(RuntimeError):
    """A successful profile HTTP response with no usable profile object."""


class FinnhubClient:
    """Throttled, retrying access to the Finnhub bond endpoints.

    ``opener`` is any callable ``(url, timeout) -> response`` whose response
    exposes ``status``/``getcode()``, ``headers`` and ``read()``. Tests inject a
    fake; production uses :mod:`urllib` with redirects followed (the default
    opener follows them, which the tick endpoint requires).
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_sleep_s: float = DEFAULT_BASE_SLEEP_S,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        opener: Any | None = None,
        sleep: Any = time.sleep,
        clock: Any = time.time,
    ) -> None:
        if not api_key:
            raise FinnhubConfigError("FINNHUB_API_KEY is empty")
        self._api_key = api_key
        self._base_sleep_s = base_sleep_s
        self._timeout_s = timeout_s
        self._opener = opener
        self._sleep = sleep
        self._clock = clock
        self.http_calls = 0
        self.retries = 0
        self.throttle_sleeps = 0
        self.errors: dict[str, int] = {}

    # ---------------------------------------------------------------- API ---

    def profile_by_cusip(self, cusip: str) -> dict[str, Any]:
        payload = self._get_json("/bond/profile", {"cusip": cusip})
        profile = payload.get("profile", payload) if isinstance(payload, dict) else None
        if not isinstance(profile, dict) or not profile:
            raise FinnhubProfileError("empty_profile")
        return profile

    def daily_candles(self, isin: str, from_ts: int, to_ts: int) -> dict[str, Any]:
        payload = self._get_json(
            "/bond/price", {"isin": isin, "from": from_ts, "to": to_ts}
        )
        return payload if isinstance(payload, dict) else {}

    def yield_curve(self, code: str) -> dict[str, Any]:
        payload = self._get_json("/bond/yield-curve", {"code": code})
        return payload if isinstance(payload, dict) else {}

    #: Columnar array keys merged across tick pages.
    TICK_ARRAY_KEYS = ("t", "p", "v", "si", "y", "ats", "cp", "rp", "c")
    #: Internal metadata retained so an HTTP-200 failure is not normalized into
    #: the same empty arrays as a valid zero-trade day.
    TICK_PAYLOAD_STATE = "__finnhub_payload_state"

    def ticks(
        self, isin: str, day: str, *, exchange: str = "trace", limit: int = 25_000
    ) -> dict[str, Any]:
        """Merged tick arrays for one ISIN and one ``YYYY-MM-DD``.

        Pages are merged via ``skip`` until ``total`` is reached (a liquid bond
        prints tens of trades a day, so one page is the norm). A day without
        trading comes back with an empty ``t``.
        """
        merged: dict[str, Any] = {key: [] for key in self.TICK_ARRAY_KEYS}
        skip = 0
        while True:
            payload = self._get_json(
                "/bond/tick",
                {
                    "isin": isin, "date": day, "limit": limit, "skip": skip,
                    "exchange": exchange,
                    # format=json is load-bearing: the redirect target's CSV
                    # carries a Go-fmt bug that corrupts numeric fields.
                    "format": "json",
                },
            )
            if not isinstance(payload, dict):
                merged[self.TICK_PAYLOAD_STATE] = "malformed_payload"
                return merged
            if not payload:
                merged[self.TICK_PAYLOAD_STATE] = "api_empty"
                return merged
            if payload.get("error") is not None or payload.get("s") in {"error", "no_data"}:
                merged[self.TICK_PAYLOAD_STATE] = "api_error"
                return merged
            page = payload.get("t")
            total = payload.get("total")
            if not isinstance(page, list) or (
                total is not None and (not isinstance(total, int) or total < 0)
            ):
                merged[self.TICK_PAYLOAD_STATE] = "malformed_payload"
                return merged
            if not page:
                merged[self.TICK_PAYLOAD_STATE] = (
                    "valid_zero_trades"
                    if skip == 0 and total in {None, 0}
                    else "malformed_payload"
                )
                return merged
            for key in self.TICK_ARRAY_KEYS:
                values = payload.get(key)
                if isinstance(values, list):
                    merged[key].extend(values)
            if total is None:
                # The live endpoint currently omits ``total`` and returns a
                # single page with ``skip`` plus columnar arrays. A short page
                # is complete; a full page cannot prove it was not truncated.
                merged[self.TICK_PAYLOAD_STATE] = (
                    "ok" if len(page) < limit else "malformed_payload"
                )
                return merged
            if len(merged["t"]) >= total:
                merged[self.TICK_PAYLOAD_STATE] = "ok"
                return merged
            if len(page) < limit:
                merged[self.TICK_PAYLOAD_STATE] = "malformed_payload"
                return merged
            skip += len(page)

    # ---------------------------------------------------------- internals ---

    def _count_error(self, kind: str) -> None:
        self.errors[kind] = self.errors.get(kind, 0) + 1

    def stats(self) -> dict[str, Any]:
        return {
            "http_calls": self.http_calls,
            "retries": self.retries,
            "throttle_sleeps": self.throttle_sleeps,
            "errors": dict(self.errors),
        }

    def _open(self, url: str) -> Any:
        if self._opener is not None:
            return self._opener(url, self._timeout_s)
        request = urllib.request.Request(
            url, headers={"User-Agent": "investintell-bond-live-daily/1"}
        )
        # urllib's default opener follows 3xx, which /bond/tick requires.
        return urllib.request.urlopen(request, timeout=self._timeout_s)

    @staticmethod
    def _read_body(response: Any) -> tuple[Any, Any]:
        """Drain and close one response; whatever it raises is the caller's.

        Called from inside :meth:`_get_json`'s retrying scope, so a body that
        dies mid-stream is retried and, when retries run out, surfaces as a
        :class:`FinnhubTransientError` like any other network failure. The
        socket is closed on both paths -- a retried attempt must not leak one.
        """
        try:
            body = response.read()
            headers = getattr(response, "headers", {}) or {}
        finally:
            closer = getattr(response, "close", None)
            if callable(closer):
                closer()
        return body, headers

    def _get_json(self, endpoint: str, params: dict[str, Any]) -> Any:
        """One logical GET with throttle, retries and exponential backoff.

        Raises :class:`FinnhubTransientError` when retries are exhausted (the
        caller records the unit as failed-transient and moves on -- nothing is
        persisted, so the next run's watermark retries it) and
        :class:`FinnhubConfigError` on a non-retryable 4xx, which is an operator
        problem and must stop the sweep rather than be counted as missing data.
        """
        url = f"{BASE_URL}{endpoint}?" + urllib.parse.urlencode(
            dict(params, token=self._api_key)
        )
        delay = BACKOFF_BASE_S
        last_error = "unknown"
        for attempt in range(MAX_RETRIES + 1):
            if attempt > 0:
                self.retries += 1
            # The retry budget is checked BEFORE every backoff below, never
            # after. On the last attempt the loop exits and raises immediately,
            # so a sleep there buys no further try: it is dead time added to the
            # abort the caller is already waiting for -- up to the 120s cap (or
            # a provider-dictated Retry-After) per failed logical request, times
            # MAX_CONSECUTIVE_FAILURES before an outage is reported. It changes
            # neither how many attempts are made nor how an error is classified.
            retries_left = attempt < MAX_RETRIES
            try:
                response = self._open(url)
                # A call whose body then dies still burned quota.
                self.http_calls += 1
                # The read is INSIDE this scope on purpose: a socket that dies
                # while the body is being consumed is exactly as transient as
                # one that dies at connect time. Outside, it escaped as a raw
                # timeout/OSError past loaders that only catch
                # FinnhubTransientError and killed the whole unsupervised sweep.
                body, headers = self._read_body(response)
            except urllib.error.HTTPError as exc:
                self.http_calls += 1
                status = exc.code
                if status == 429:
                    self._count_error("http_429")
                    last_error = "http_429"
                    if retries_left:
                        self._sleep(self._retry_after_s(exc, min(delay, BACKOFF_CAP_S)))
                    delay *= 2
                    continue
                if status >= 500:
                    self._count_error(f"http_{status}")
                    last_error = f"http_{status}"
                    if retries_left:
                        self._sleep(min(delay, BACKOFF_CAP_S))
                    delay *= 2
                    continue
                self._count_error(f"http_{status}")
                raise FinnhubConfigError(f"Finnhub {endpoint} returned HTTP {status}")
            except Exception as exc:  # network layer, at connect OR mid-body
                self._count_error("network")
                last_error = f"network: {type(exc).__name__}"
                if retries_left:
                    self._sleep(min(delay, BACKOFF_CAP_S))
                delay *= 2
                continue

            self._respect_rate_headers(headers)
            try:
                return json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
            except (ValueError, AttributeError):
                self._count_error("bad_json")
                last_error = "bad_json"
                if retries_left:
                    self._sleep(min(delay, BACKOFF_CAP_S))
                delay *= 2
                continue
        raise FinnhubTransientError(f"retries exhausted for {endpoint}: {last_error}")

    def _respect_rate_headers(self, headers: Any) -> None:
        """Base pacing sleep; sleep to the reset instant when nearly drained."""
        remaining, reset = self._rate_headers(headers)
        if remaining is not None and remaining <= 2 and reset:
            wait = max(0.0, reset - self._clock()) + 1.0
            if wait > 0:
                self.throttle_sleeps += 1
                self._sleep(min(wait, BACKOFF_CAP_S))
                return
        if self._base_sleep_s:
            self._sleep(self._base_sleep_s)

    @staticmethod
    def _rate_headers(headers: Any) -> tuple[int | None, int]:
        getter = getattr(headers, "get", None)
        if not callable(getter):
            return None, 0
        try:
            remaining: int | None = int(getter("X-Ratelimit-Remaining"))
        except (TypeError, ValueError):
            remaining = None
        try:
            reset = int(getter("X-Ratelimit-Reset"))
        except (TypeError, ValueError):
            reset = 0
        return remaining, reset

    def _retry_after_s(self, exc: Any, fallback: float) -> float:
        headers = getattr(exc, "headers", {}) or {}
        getter = getattr(headers, "get", None)
        if callable(getter):
            retry_after = getter("Retry-After")
            if retry_after:
                try:
                    return max(1.0, float(retry_after))
                except (TypeError, ValueError):
                    pass
            _, reset = self._rate_headers(headers)
            if reset:
                return max(1.0, reset - self._clock() + 1.0)
        return fallback


def client_from_env(**kwargs: Any) -> FinnhubClient:
    """Build a client from ``FINNHUB_API_KEY``; raise when it is absent.

    Raising (rather than returning None) keeps the "no key" case from looking
    like "no data": the worker catches it and REPORTS ``no_api_key``, which is a
    configuration fault an operator must see, not an empty day.
    """
    key = (os.getenv("FINNHUB_API_KEY") or "").strip()
    if not key:
        raise FinnhubConfigError("FINNHUB_API_KEY is not set")
    return FinnhubClient(key, **kwargs)
