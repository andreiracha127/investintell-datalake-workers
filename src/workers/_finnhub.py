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
import threading
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
#: Floor between requests when the provider has not told us its limit yet.
#: Kept as the historical value so a lane that never sees a rate header (the
#: tick endpoint serves none) behaves exactly as it always did until a header
#: from another endpoint teaches the client the real budget.
DEFAULT_BASE_SLEEP_S = 0.15

#: Fraction of the provider's reported limit this client will actually spend.
#: Not 1.0: the reset window is a boundary we cannot see the inside of, and the
#: tick lane runs blind (no rate headers), so it inherits whatever pace the
#: header-bearing lanes learned. 0.85 of 300/min = 255/min.
RATE_SAFETY_MARGIN = 0.85

#: Requests the guard keeps in reserve on top of whatever is in flight. A slot
#: is an AUTHORISATION to emit, and between authorising and emitting there is no
#: lock -- there cannot be one, the call is network I/O and holding a lock across
#: it would serialise the concurrency this design exists for. So a hold can
#: always land after a caller was cleared, and no amount of re-checking closes
#: that window. What closes the RISK is refusing to authorise while the budget
#: can still absorb everyone outstanding: the guard fires at
#: ``GUARD_BASE_MARGIN + in flight``, so when it does, every already-cleared
#: caller still has quota to spend. The window stops mattering rather than being
#: (impossibly) closed.
GUARD_BASE_MARGIN = 2

#: Pacing is measured from the START of the previous request, not from the end
#: of it. The distinction is the whole point: a sweep is one bond at a time, so
#: wall clock is (latency + pace) per call, and sleeping a fixed amount AFTER
#: each response charges the provider's latency on top of an interval that
#: already accounts for it. Measured 2026-08-18 in production: 10.208 tick calls
#: took 4180s -- 0.41s each, i.e. 146/min -- against a ceiling of 300/min read
#: from X-Ratelimit-Limit on the live API the same day. The old constant above
#: assumed 190/min, which was never the budget; the gap was latency being paid
#: twice. Anchoring the interval at the request start hands that gap back.

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
        #: Learned from X-Ratelimit-Limit; None until an endpoint reports one.
        self._limit_per_min: int | None = None
        #: The shared emission schedule: the next instant a request may be
        #: ISSUED. Callers reserve a slot and leave the following one for
        #: whoever comes next, so N in flight spend the budget at the same rate
        #: one caller would -- they just stop waiting on each other's latency.
        self._next_slot_at: float | None = None
        #: A hard floor on emission, set by the near-drained guard. Separate
        #: from the schedule because a caller that reserved its slot BEFORE the
        #: budget ran out is already sleeping on a wait computed while things
        #: looked fine: moving the schedule alone would not reach it, and it
        #: would wake into a spent budget. Re-checked after every sleep.
        self._barrier_at: float = 0.0
        #: Callers cleared to emit but not yet done. See ``GUARD_BASE_MARGIN``.
        self._in_flight: int = 0
        self._slot_lock = threading.Lock()
        self.http_calls = 0
        self.retries = 0
        self.throttle_sleeps = 0
        self.errors: dict[str, int] = {}

    # ---------------------------------------------------------------- API ---

    def profile_by_cusip(self, cusip: str) -> dict[str, Any]:
        payload = self._get_json("/bond/profile", {"cusip": cusip})
        if isinstance(payload, dict) and payload.get("error") is not None:
            raise FinnhubProfileError("provider_error")
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
            in_flight = False
            try:
                # Returns holding ONE authorisation, already counted; it
                # covers this request even if a reset lands while it is on the
                # wire, and the ``finally`` below hands it back.
                self._await_slot()
                in_flight = True
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
            finally:
                # Every exit from the attempt releases the authorisation --
                # success, typed HTTP error, dead socket. A leak here would
                # inflate the guard's margin permanently and make it fire ever
                # earlier, throttling a healthy client into uselessness.
                if in_flight:
                    self._leave_flight()

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
        """Sleep to the reset when nearly drained; otherwise pace the interval.

        The near-drained guard is unchanged and comes first: a budget about to
        run out is a hard stop, not a pace. Everything else is spacing, and it
        is measured from when the LAST REQUEST WAS ISSUED, so a slow response
        has already paid part (or all) of the interval by the time we get here.
        """
        remaining, reset = self._rate_headers(headers)
        limit = self._limit_header(headers)
        if limit:
            self._limit_per_min = limit
        if self._hold_if_drained(remaining, reset):
            # The schedule is already held at this point -- the decision and the
            # block were one act -- so this thread only has its own wait left to
            # pay. Callers in flight queue behind the same barrier.
            wait = max(0.0, reset - self._clock()) + 1.0
            if wait > 0:
                self.throttle_sleeps += 1
                self._sleep(min(wait, BACKOFF_CAP_S))

    def _target_interval_s(self) -> float:
        """Seconds between request STARTS: the provider's budget, or the floor."""
        if self._limit_per_min and self._limit_per_min > 0:
            return 60.0 / (self._limit_per_min * RATE_SAFETY_MARGIN)
        return self._base_sleep_s

    def _guard_margin(self) -> int:
        """How much budget must remain before the guard stops authorising."""
        with self._slot_lock:
            return GUARD_BASE_MARGIN + self._in_flight

    def _hold_if_drained(self, remaining: int | None, reset: int) -> bool:
        """Decide AND block in one critical section; report whether it blocked.

        Reading the margin, releasing the lock, and only then holding leaves an
        interval in which other callers take slots the margin just computed
        never accounted for -- the transition into the blocked state would be
        announced after the fact rather than made. Evaluating against the live
        count and applying the hold before the lock is released is what makes
        "we are drained" and "nobody else is authorised" the same instant.
        """
        if remaining is None or not reset:
            return False
        instant = reset + 1.0
        with self._slot_lock:
            if remaining > GUARD_BASE_MARGIN + self._in_flight:
                return False
            if self._next_slot_at is None or instant > self._next_slot_at:
                self._next_slot_at = instant
            if instant > self._barrier_at:
                self._barrier_at = instant
            return True

    def _leave_flight(self) -> None:
        with self._slot_lock:
            if self._in_flight > 0:
                self._in_flight -= 1

    def _reserve_slot(self) -> float:
        """Claim the next emission instant; return the wait it implies.

        Reserving BEFORE the request (rather than sleeping after the response)
        is what lets the provider's latency count towards the interval instead
        of being charged on top of it, and it is also what makes concurrency
        safe: the schedule -- not "when did I last call" -- is the shared state,
        so two callers can never claim the same instant. With one caller in
        flight this degenerates exactly to the sequential interval, which is
        what keeps the un-parallelised lanes (curve, profile) unchanged.
        """
        interval = self._target_interval_s()
        with self._slot_lock:
            now = self._clock()
            # The standing hold is part of the arithmetic, not a check beside
            # it. A separate check is two steps -- decide, then claim -- and a
            # reset arriving between them hands out a slot inside the very
            # window the guard declared spent. Folded in here, no slot can ever
            # predate the barrier, whatever order the callers arrive in.
            floor = max(now, self._barrier_at)
            slot = floor if self._next_slot_at is None else max(floor, self._next_slot_at)
            self._next_slot_at = slot + interval
            # Authorising IS counting, under the SAME lock. Tallying afterwards
            # leaves a gap in which another thread computes the guard's margin
            # without seeing this caller -- an undercount of exactly the thing
            # the margin exists to cover. The caller gives it back via
            # ``_leave_flight`` when it emits, fails, or abandons the slot.
            self._in_flight += 1
        return max(0.0, slot - now)

    def _hold_until(self, instant: float) -> None:
        """Push the shared schedule out to ``instant`` (never pull it in).

        The near-drained guard uses this so a budget that is spent stops EVERY
        caller, not just the one that happened to read the header. A thread
        sleeping alone while the others emit is a guard in name only.
        """
        with self._slot_lock:
            if self._next_slot_at is None or instant > self._next_slot_at:
                self._next_slot_at = instant
            if instant > self._barrier_at:
                self._barrier_at = instant

    def _await_slot(self) -> None:
        """Wait for this caller's slot, and for any hold placed while it waited.

        The re-check is the whole point: reservation and emission are separated
        by a sleep, and the near-drained guard fires in between often enough to
        matter -- it fires exactly when several callers are in flight against a
        budget about to run out. Each iteration requires a STRICTLY later
        barrier than the one already waited out, so a schedule under constant
        holds still converges instead of spinning.
        """
        while True:
            # Re-reserved on every pass, and that is the point: the barrier is
            # ONE shared instant, so everyone parked on it wakes together. A
            # caller that emitted straight after the wait would turn the reset
            # -- the moment the budget is most fragile -- into a burst of N
            # simultaneous requests, which is the very 429 the hold just spent a
            # minute avoiding. Claiming a fresh slot on the way out spaces the
            # resumption at the normal interval instead.
            wait = self._reserve_slot()
            if wait > 0:
                self._sleep(wait)
            with self._slot_lock:
                barrier = self._barrier_at
            if self._clock() >= barrier:
                return
            # A hold landed while this caller slept: give the authorisation back
            # before trying again, or the abandoned slot would inflate the
            # margin for as long as the sweep runs.
            self._leave_flight()
            # A hold landed while this caller slept. Loop rather than emit: the
            # reservation above already accounts for the barrier, so the next
            # pass waits it out and comes back paced. There is deliberately NO
            # "I have waited for this barrier before" shortcut -- that would
            # trust the clock to be monotonic, and the default one is
            # ``time.time``, which an NTP step can walk backwards.

    @staticmethod
    def _limit_header(headers: Any) -> int | None:
        getter = getattr(headers, "get", None)
        if not callable(getter):
            return None
        try:
            return int(getter("X-Ratelimit-Limit"))
        except (TypeError, ValueError):
            return None

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
