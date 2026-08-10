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
