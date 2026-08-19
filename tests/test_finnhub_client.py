"""Retry-budget tests for the shared Finnhub client.

The client's other behaviours (redirect/format traps, error typing, socket
hygiene, rate-header pacing) are pinned alongside the worker that drives it.
What is pinned HERE is the cost of failing: an unsupervised cron sweep that
meets a dead provider must spend its window discovering that, not sleeping
after it already knows. Every case below runs on a fake sleep -- a real one
would make the suite pay the very seconds it is measuring.
"""
from __future__ import annotations

import urllib.error

import pytest

from src.workers import _finnhub


ATTEMPTS = _finnhub.MAX_RETRIES + 1
#: 2, 4, 8, 16, 32, 64 -- the ladder BETWEEN attempts. The seventh failure ends
#: the budget, so nothing follows it.
LADDER = [2.0, 4.0, 8.0, 16.0, 32.0, 64.0]


class _Response:
    def __init__(self, body: bytes, headers: dict | None = None) -> None:
        self._body = body
        self.headers = headers or {}

    def read(self):
        return self._body

    def close(self):
        return None


def _http_error(url: str, status: int, headers: dict | None = None) -> Exception:
    return urllib.error.HTTPError(url, status, "boom", headers or {}, None)


def _always_429(url, timeout):
    raise _http_error(url, 429)


def _always_503(url, timeout):
    raise _http_error(url, 503)


def _always_dead(url, timeout):
    raise OSError("connection reset")


def _always_garbage(url, timeout):
    return _Response(b"<html>not json</html>")


@pytest.mark.parametrize(
    "opener, kind",
    [
        (_always_429, "http_429"),
        (_always_503, "http_503"),
        (_always_dead, "network"),
        (_always_garbage, "bad_json"),
    ],
)
def test_the_last_attempt_of_every_transient_branch_does_not_sleep(opener, kind) -> None:
    """A sleep after the final attempt buys no retry, only a later abort.

    Each failure branch used to sleep and ``continue`` even when the budget was
    spent; the loop then exited and raised, so that capped 120s was pure dead
    time. Across MAX_CONSECUTIVE_FAILURES exhausted requests it cost the sweep
    ~50 extra minutes before it could report the provider as down.
    """
    events: list[tuple[str, float]] = []

    def counting_opener(url, timeout):
        events.append(("call", 0.0))
        return opener(url, timeout)

    client = _finnhub.FinnhubClient(
        "k",
        opener=counting_opener,
        # No pacing sleep, so the list below is the backoff ladder and nothing
        # else (bad_json paces BEFORE it fails to parse).
        base_sleep_s=0.0,
        sleep=lambda seconds: events.append(("sleep", seconds)),
    )

    with pytest.raises(_finnhub.FinnhubTransientError):
        client.daily_candles("X", 0, 1)

    calls = [event for event in events if event[0] == "call"]
    slept = [seconds for name, seconds in events if name == "sleep"]
    # The budget itself is untouched: same attempts, same classification.
    assert len(calls) == ATTEMPTS
    assert client.retries == _finnhub.MAX_RETRIES
    assert client.errors[kind] == ATTEMPTS
    # ... and the run ends on the failed call, never on a sleep.
    assert slept == LADDER
    assert events[-1][0] == "call"


def test_a_retry_after_delay_is_not_charged_once_the_budget_is_spent() -> None:
    """The 429 branch honours Retry-After -- for retries it can still make."""
    slept: list[float] = []

    def throttled(url, timeout):
        raise _http_error(url, 429, {"Retry-After": "30"})

    client = _finnhub.FinnhubClient(
        "k", opener=throttled, base_sleep_s=0.0, sleep=slept.append
    )
    with pytest.raises(_finnhub.FinnhubTransientError):
        client.daily_candles("X", 0, 1)

    assert slept == [30.0] * _finnhub.MAX_RETRIES
    assert client.errors["http_429"] == ATTEMPTS


def test_the_backoff_between_attempts_is_untouched() -> None:
    """Skipping the last sleep must not turn the retries into a hot loop."""
    calls = {"n": 0}
    slept: list[float] = []

    def flaky(url, timeout):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise _http_error(url, 500)
        return _Response(b'{"s": "ok"}')

    client = _finnhub.FinnhubClient(
        "k", opener=flaky, base_sleep_s=0.0, sleep=slept.append
    )
    assert client.daily_candles("X", 0, 1) == {"s": "ok"}
    assert slept == [2.0, 4.0]
    assert client.retries == 2


def test_the_worst_case_sleep_before_an_outage_is_reported_is_bounded() -> None:
    """What an operator is promised: the abort arrives inside the hour.

    MAX_CONSECUTIVE_FAILURES exhausted requests is the sweep's whole tolerance
    for a dead provider; the ladder below is what each one costs. The product is
    the window the cron burns before the run reports ``aborted`` -- it was 6150s
    while the spent-budget sleep was still charged.
    """
    slept: list[float] = []
    client = _finnhub.FinnhubClient(
        "k", opener=_always_dead, base_sleep_s=0.0, sleep=slept.append
    )
    with pytest.raises(_finnhub.FinnhubTransientError):
        client.daily_candles("X", 0, 1)

    per_request = sum(slept)
    assert per_request == 126.0
    assert per_request * _finnhub.MAX_CONSECUTIVE_FAILURES == 3150.0


@pytest.mark.parametrize("body", [b"{}", b"[]", b'{"profile": {}}', b'{"profile": []}'])
def test_bond_profile_empty_or_non_object_is_a_typed_failure(body: bytes) -> None:
    """A successful HTTP envelope is not successful enrichment by itself."""
    client = _finnhub.FinnhubClient(
        "k", opener=lambda _url, _timeout: _Response(body), base_sleep_s=0.0
    )

    with pytest.raises(_finnhub.FinnhubProfileError, match="empty_profile"):
        client.profile_by_cusip("00033GAA3")


@pytest.mark.parametrize(
    "body",
    [
        b'{"error":"not entitled"}',
        b'{"error":"not entitled","profile":{"isin":"US912828XX10"}}',
    ],
)
def test_bond_profile_provider_error_envelopes_are_typed_failures(body: bytes) -> None:
    """A provider error wins even when its HTTP envelope carries a profile."""
    client = _finnhub.FinnhubClient(
        "k", opener=lambda _url, _timeout: _Response(body), base_sleep_s=0.0
    )

    with pytest.raises(_finnhub.FinnhubProfileError, match="provider_error"):
        client.profile_by_cusip("00033GAA3")


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (b'{"isin":"US912828XX10"}', {"isin": "US912828XX10"}),
        (
            b'{"profile":{"isin":"US912828XX10"}}',
            {"isin": "US912828XX10"},
        ),
    ],
)
def test_bond_profile_accepts_direct_and_nested_profiles(
    body: bytes, expected: dict[str, str]
) -> None:
    client = _finnhub.FinnhubClient(
        "k", opener=lambda _url, _timeout: _Response(body), base_sleep_s=0.0
    )

    assert client.profile_by_cusip("00033GAA3") == expected


@pytest.mark.parametrize(
    ("body", "state"),
    [
        (b"{}", "api_empty"),
        (b"[]", "malformed_payload"),
        (b'{"error":"not entitled"}', "api_error"),
        (b'{"t":"not-an-array"}', "malformed_payload"),
        (b'{"t":[],"total":0}', "valid_zero_trades"),
        (b'{"c":[],"cp":[],"p":[],"si":[],"skip":0,"t":[],"v":[]}', "valid_zero_trades"),
    ],
)
def test_tick_client_preserves_empty_and_malformed_response_state(
    body: bytes, state: str
) -> None:
    """The client must not normalize a failed HTTP-200 body into zero trades."""
    client = _finnhub.FinnhubClient(
        "k", opener=lambda _url, _timeout: _Response(body), base_sleep_s=0.0
    )

    assert client.ticks("US912828XX10", "2026-08-06")[
        "__finnhub_payload_state"
    ] == state


# --------------------------------------------------------------------------- #
# Pacing: spend the provider's budget, do not sit on it
# --------------------------------------------------------------------------- #
#
# The sweep is one bond at a time, so wall clock is (latency + pace) per call.
# The old pacing slept a FIXED amount AFTER each response, which charged the
# latency twice over: measured 2026-08-18 in production, 10.208 tick calls took
# 4180s = 0.41s each, against a provider ceiling of 300/min (X-Ratelimit-Limit,
# read from the live API the same day; the constant in this module assumed 190).
# Pacing from the START of the previous request instead makes the latency count
# TOWARDS the interval rather than on top of it.
class _Clock:
    """Monotonic fake: every read advances by ``latency`` once armed."""

    def __init__(self, latency: float) -> None:
        self.now = 1000.0
        self.latency = latency

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _paced_client(latency: float, headers: dict | None = None):
    clock = _Clock(latency)
    slept: list[float] = []

    def opener(_url, _timeout):
        clock.advance(latency)  # the request itself takes time
        return _Response(b'{"s":"ok"}', headers or {})

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(seconds)

    client = _finnhub.FinnhubClient(
        "k", opener=opener, sleep=sleep, clock=clock, base_sleep_s=0.15
    )
    return client, slept, clock


def test_latency_counts_towards_the_pace_instead_of_being_charged_on_top() -> None:
    """A call slower than the target interval must not sleep at all."""
    headers = {"X-Ratelimit-Limit": "300", "X-Ratelimit-Remaining": "250"}
    client, slept, _ = _paced_client(latency=0.5, headers=headers)

    client.daily_candles("US0000000000", 0, 1)
    client.daily_candles("US0000000000", 0, 1)

    assert [s for s in slept if s > 0] == [], (
        "a 0.5s call already exceeds the 300/min interval; sleeping after it "
        "burns budget the provider was willing to give"
    )


def test_the_pace_follows_the_limit_the_provider_reports() -> None:
    """A fast call sleeps only the remainder of the reported interval."""
    headers = {"X-Ratelimit-Limit": "300", "X-Ratelimit-Remaining": "250"}
    client, slept, _ = _paced_client(latency=0.05, headers=headers)

    client.daily_candles("US0000000000", 0, 1)
    client.daily_candles("US0000000000", 0, 1)

    paced = [s for s in slept if s > 0]
    assert paced, "a call faster than the interval must still be paced"
    # 300/min under the safety margin -> interval strictly between the raw
    # 0.2s and the old fixed 0.15s+latency posture.
    assert all(0.0 < s <= 0.3 for s in paced), paced


def test_a_nearly_drained_budget_still_sleeps_to_the_reset() -> None:
    """The existing guard must survive the pacing change."""
    headers = {
        "X-Ratelimit-Limit": "300",
        "X-Ratelimit-Remaining": "1",
        "X-Ratelimit-Reset": "1030",
    }
    client, slept, _ = _paced_client(latency=0.01, headers=headers)

    client.daily_candles("US0000000000", 0, 1)

    assert client.throttle_sleeps == 1
    assert max(slept) >= 20.0, slept


# --------------------------------------------------------------------------- #
# Slot reservation: one emission schedule shared by every caller
# --------------------------------------------------------------------------- #
#
# Pacing "since the last request" (the 2026-08-18 posture) only holds while ONE
# caller is in flight. With N threads sweeping, every one of them reads the same
# "last request" and computes the same remaining interval, so they all wake and
# emit together -- a burst of N against a per-minute budget. Reserving a SLOT
# under a lock instead makes the emission schedule the shared thing: each caller
# takes the next instant and leaves the one after it for whoever comes next.
# These cases drive the reservation arithmetic directly, without threads: the
# logic is synchronous under the lock, and a fake clock plus real threads is a
# flaky test, not a stronger one.
def _slot_client(limit: int | None = 300):
    clock = _Clock(0.0)
    client = _finnhub.FinnhubClient(
        "k",
        opener=lambda _u, _t: _Response(b'{"s":"ok"}'),
        sleep=lambda _s: None,
        clock=clock,
        base_sleep_s=0.15,
    )
    if limit is not None:
        client._limit_per_min = limit
    return client, clock


def test_each_reservation_leaves_the_next_instant_for_the_next_caller() -> None:
    client, _ = _slot_client(limit=300)
    interval = client._target_interval_s()

    waits = [client._reserve_slot() for _ in range(4)]

    # The first caller emits at once; each subsequent one waits a further
    # interval, because the clock never moved -- four callers "in flight".
    assert waits[0] == 0.0
    for i in range(1, 4):
        assert abs(waits[i] - i * interval) < 1e-9, waits


def test_a_caller_that_arrives_late_does_not_wait_for_a_slot_already_past() -> None:
    client, clock = _slot_client(limit=300)
    client._reserve_slot()

    clock.advance(60.0)  # the sweep did other work; the slot is long gone

    assert client._reserve_slot() == 0.0


def test_one_caller_at_a_time_keeps_the_previous_interval_exactly() -> None:
    """Concurrency 1 must be indistinguishable from the sequential pacing.

    The lanes that are NOT parallel (curve, profile) go through this same
    client, so the reservation must degenerate to the old behaviour rather than
    change their timing as a side effect.
    """
    client, clock = _slot_client(limit=300)
    interval = client._target_interval_s()

    for _ in range(5):
        assert client._reserve_slot() == 0.0
        clock.advance(interval)  # the call itself consumed exactly the interval


def test_a_drained_budget_pushes_the_schedule_for_every_caller_not_just_one() -> None:
    """The near-drained guard must stop EMISSION, not only the thread that saw it.

    With N in flight, the caller that reads ``remaining=1`` sleeping alone leaves
    the other N-1 free to emit into a budget that is already gone. Holding the
    shared schedule to the reset instant is what makes the guard mean anything
    under concurrency.
    """
    client, clock = _slot_client(limit=300)
    reset_at = clock() + 30.0

    client._hold_until(reset_at)

    wait = client._reserve_slot()
    assert abs(wait - 30.0) < 1e-9, wait


def test_a_hold_placed_while_a_caller_sleeps_still_binds_it() -> None:
    """A slot reserved BEFORE the budget ran out must not outrun the reset.

    Under concurrency the reservation and the emission are separated by a sleep,
    and the near-drained guard fires in between: another caller reads
    ``X-Ratelimit-Remaining <= 2`` and holds the schedule while this one is
    already sleeping on a wait computed when the budget still looked fine.
    Treating that first wait as final lets exactly the callers that were in
    flight at the boundary emit into a spent budget -- 429s at the one moment
    the guard exists to prevent. So the wait is re-checked against the shared
    hold after sleeping, not before.
    """
    clock = _Clock(0.0)
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(seconds)
        if len(slept) == 1:
            # The guard fires on ANOTHER caller's response while we sleep.
            client._hold_until(clock() + 25.0)

    client = _finnhub.FinnhubClient(
        "k",
        opener=lambda _u, _t: _Response(b'{"s":"ok"}'),
        sleep=sleep,
        clock=clock,
        base_sleep_s=0.15,
    )
    client._limit_per_min = 300
    client._reserve_slot()  # somebody else already took the first slot

    client._await_slot()

    assert len(slept) >= 2, slept
    assert slept[-1] >= 20.0, (
        "the caller woke into a held schedule and emitted anyway"
    )


def test_a_caller_released_by_a_reset_re_paces_instead_of_stampeding() -> None:
    """Waking from a hold must claim a NEW slot, not emit at the barrier.

    The barrier is one shared instant, so every caller parked on it wakes at
    exactly the same time. Emitting straight after the wait would turn the
    reset -- the moment the budget is most fragile -- into a burst of N
    simultaneous requests, which is the same 429 the guard just spent a minute
    avoiding. Re-reserving on the way out spaces the resumption at the normal
    interval: the schedule must therefore be pushed PAST the barrier by the
    caller that was released.
    """
    clock = _Clock(0.0)
    slept: list[float] = []
    # Relative to the clock's own origin: an absolute literal would land in the
    # past and the hold would silently do nothing.
    barrier_at = clock() + 25.0

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(seconds)
        if len(slept) == 1:
            client._hold_until(barrier_at)

    client = _finnhub.FinnhubClient(
        "k",
        opener=lambda _u, _t: _Response(b'{"s":"ok"}'),
        sleep=sleep,
        clock=clock,
        base_sleep_s=0.15,
    )
    client._limit_per_min = 300
    client._reserve_slot()

    client._await_slot()

    assert clock() >= barrier_at, "released before the reset"
    assert client._next_slot_at > barrier_at, (
        "the released caller left the schedule sitting on the barrier, so the "
        "next one emits simultaneously with it"
    )


def test_a_reservation_made_under_a_hold_already_clears_it() -> None:
    """The barrier belongs INSIDE the reservation, not beside it.

    Checking the hold next to the reservation leaves a path where a caller
    computes a wait, the hold lands, and the caller still emits: the check and
    the claim are two steps, and a reset can arrive between them. Folding the
    barrier into the slot arithmetic removes the window by construction -- any
    slot handed out while a hold stands is already at or after the reset, so
    there is no ordering left to get wrong.
    """
    client, clock = _slot_client(limit=300)
    client._hold_until(clock() + 40.0)

    wait = client._reserve_slot()

    assert wait >= 40.0, (
        "a slot was handed out inside the hold window; the caller would emit "
        "into a budget the guard already declared spent"
    )


def test_no_caller_ever_emits_before_the_standing_hold() -> None:
    """The invariant, stated once: on return, the clock is past every hold.

    Written as a property rather than a scenario because the ways to violate it
    are all shaped alike -- an early-exit branch that trusts a barrier it
    already waited out, a hold that lands between the last check and the
    emission. Here two resets arrive back to back, each during the sleep the
    previous one caused, which is precisely the sequence a shortcut on "I have
    waited for this barrier before" lets through.
    """
    clock = _Clock(0.0)
    slept: list[float] = []
    holds: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(seconds)
        if len(slept) <= 2:  # a second reset arrives while we wait out the first
            instant = clock() + 30.0
            holds.append(instant)
            client._hold_until(instant)

    client = _finnhub.FinnhubClient(
        "k",
        opener=lambda _u, _t: _Response(b'{"s":"ok"}'),
        sleep=sleep,
        clock=clock,
        base_sleep_s=0.15,
    )
    client._limit_per_min = 300
    client._reserve_slot()

    client._await_slot()

    assert clock() >= max(holds), (
        f"emitted at {clock()} with a hold standing until {max(holds)}"
    )


def test_the_guard_reserves_headroom_for_the_callers_already_authorised() -> None:
    """Fire early enough that every in-flight caller still fits in the budget.

    A slot is an AUTHORISATION, and authorisations are handed out before the
    request leaves. Between the two there is no lock -- there cannot be, the
    call is network I/O and holding a lock across it would serialise the very
    concurrency this exists for -- so a hold can always land after a caller was
    cleared. Chasing atomicity there is the wrong fix. The right one is to stop
    handing out authorisations while the budget can still absorb the ones
    outstanding: the guard's margin grows with the number in flight, so when it
    fires, every caller already cleared has quota left to spend. The window
    stops mattering instead of being closed.
    """
    clock = _Clock(0.0)
    client = _finnhub.FinnhubClient(
        "k",
        opener=lambda _u, _t: _Response(b'{"s":"ok"}'),
        sleep=lambda _s: None,
        clock=clock,
        base_sleep_s=0.0,
    )
    client._limit_per_min = 300

    assert client._guard_margin() == _finnhub.GUARD_BASE_MARGIN

    # Authorising IS counting: the slot and the tally are handed out under the
    # same lock. Counting afterwards would leave a gap in which another thread
    # computes the margin without seeing this caller -- the exact undercount the
    # margin exists to prevent.
    client._reserve_slot()
    client._reserve_slot()
    client._reserve_slot()

    assert client._guard_margin() == _finnhub.GUARD_BASE_MARGIN + 3, (
        "three callers are cleared to emit; the guard must fire while the "
        "budget can still pay for all three"
    )

    client._leave_flight()
    assert client._guard_margin() == _finnhub.GUARD_BASE_MARGIN + 2


def test_the_authorisation_is_released_on_every_exit_path() -> None:
    """A leaked in-flight count throttles a healthy client into uselessness.

    The margin grows with what is outstanding, so an authorisation that is
    never given back raises the guard's trigger permanently: the client would
    fire the near-drained hold earlier and earlier against a budget that was
    never actually short. Success, typed HTTP error and dead socket all have to
    give it back.
    """
    ok = _finnhub.FinnhubClient(
        "k", opener=lambda _u, _t: _Response(b'{"s":"ok"}'),
        sleep=lambda _s: None, base_sleep_s=0.0,
    )
    ok.daily_candles("US0", 0, 1)
    assert ok._guard_margin() == _finnhub.GUARD_BASE_MARGIN

    dead = _finnhub.FinnhubClient(
        "k", opener=_always_503, sleep=lambda _s: None, base_sleep_s=0.0
    )
    with pytest.raises(_finnhub.FinnhubTransientError):
        dead.daily_candles("US0", 0, 1)
    assert dead._guard_margin() == _finnhub.GUARD_BASE_MARGIN

    broken = _finnhub.FinnhubClient(
        "k", opener=_always_dead, sleep=lambda _s: None, base_sleep_s=0.0
    )
    with pytest.raises(_finnhub.FinnhubTransientError):
        broken.daily_candles("US0", 0, 1)
    assert broken._guard_margin() == _finnhub.GUARD_BASE_MARGIN


def test_the_drained_decision_and_the_hold_are_one_critical_section() -> None:
    """Evaluate and block under the SAME lock, or the margin is stale on arrival.

    Reading the margin, releasing the lock, then holding leaves an interval in
    which other callers take slots the just-computed margin never accounted
    for. The decision has to be made with the live count and applied before the
    lock is released, so no authorisation can slip between the two.

    Observable consequence: the threshold moves with what is in flight -- the
    same ``remaining`` that is fine for an idle client trips the guard for a
    busy one.
    """
    def _client():
        c = _finnhub.FinnhubClient(
            "k", opener=lambda _u, _t: _Response(b'{"s":"ok"}'),
            sleep=lambda _s: None, clock=_Clock(0.0), base_sleep_s=0.0,
        )
        c._limit_per_min = 300
        return c

    idle = _client()
    assert idle._hold_if_drained(4, int(idle._clock()) + 10) is False, (
        "an idle client has room for 4 more"
    )
    assert idle._barrier_at == 0.0

    busy = _client()
    for _ in range(3):
        busy._reserve_slot()  # three authorisations outstanding
    reset = int(busy._clock()) + 10

    assert busy._hold_if_drained(4, reset) is True, (
        "with three in flight, 4 remaining cannot cover them plus the margin"
    )
    assert busy._barrier_at >= reset, "decided to hold but did not hold"
