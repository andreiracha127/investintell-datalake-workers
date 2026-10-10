"""Bounded Yahoo validation uses the existing fallback transport and parser."""

import datetime as dt
from unittest.mock import Mock

import pytest

from src.workers import _fallback_nav as fb
from src.workers._tiingo import TiingoDeadlineExceeded


DAY = dt.date(2026, 9, 25)


@pytest.mark.parametrize("outcome,status", [
    (429, "rate_limited"), (503, "transient_error"),
    (RuntimeError("transport failure"), "transient_error"),
])
def test_yahoo_single_attempt_never_retries_or_sleeps(monkeypatch, outcome, status):
    monkeypatch.setenv("EODHD_API_KEY", "must-not-be-used")
    client = fb.FallbackNav(eodhd_key="")
    http = Mock()
    if isinstance(outcome, Exception):
        http.get.side_effect = outcome
    else:
        http.get.return_value.status_code = outcome
    monkeypatch.setattr(client, "_http", lambda: http)
    sleep = Mock()
    monkeypatch.setattr(fb.time, "sleep", sleep)
    _, provider, attempts = client.fetch_observations(
        "TEST", DAY, DAY, max_attempts=1, remaining=lambda: 2.5)
    assert provider is None
    assert client.providers == ("yahoo",)
    assert [(p, r.status) for p, r in attempts] == [
        ("eodhd", "not_configured"), ("yahoo", status)]
    http.get.assert_called_once()
    assert http.get.call_args.kwargs["timeout"] == 2.5
    assert "/v8/finance/chart/TEST" in http.get.call_args.args[0]
    sleep.assert_not_called()


@pytest.mark.parametrize("remaining", [(0.0,), (1.0, 0.0)])
def test_yahoo_deadline_checked_before_pacing_and_request(monkeypatch, remaining):
    client = fb.FallbackNav(eodhd_key="")
    http = Mock()
    monkeypatch.setattr(client, "_http", lambda: http)
    times = iter(remaining)
    with pytest.raises(TiingoDeadlineExceeded):
        client.fetch_observations("TEST", DAY, DAY, max_attempts=1,
                                  remaining=lambda: next(times))
    http.get.assert_not_called()


def test_yahoo_token_wait_honors_deadline(monkeypatch):
    client = fb.FallbackNav(eodhd_key="")
    bucket = Mock()
    bucket.acquire.side_effect = TiingoDeadlineExceeded("pacing budget")
    client._yahoo_bucket = bucket
    http = Mock()
    monkeypatch.setattr(client, "_http", lambda: http)
    with pytest.raises(TiingoDeadlineExceeded):
        client.fetch_observations("TEST", DAY, DAY, max_attempts=1,
                                  remaining=lambda: 0.25)
    bucket.acquire.assert_called_once_with(max_wait=0.25)
    http.get.assert_not_called()


def test_yahoo_bounded_fetch_preserves_typed_observations(monkeypatch):
    client = fb.FallbackNav(eodhd_key="")
    http = Mock()
    http.get.return_value.status_code = 200
    http.get.return_value.json.return_value = {"chart": {"result": [{
        "timestamp": [1790294400, 1790380800],
        "indicators": {"quote": [{"close": [101.0, 102.0]}],
                       "adjclose": [{"adjclose": [100.0, None]}]},
    }]}}
    monkeypatch.setattr(client, "_http", lambda: http)
    result, provider, attempts = client.fetch_observations(
        "TEST", DAY, DAY + dt.timedelta(days=1), max_attempts=1,
        remaining=lambda: 60.0)
    assert provider == "yahoo"
    assert [(o.price, o.kind) for o in result.observations] == [
        (100.0, "adjusted"), (102.0, "raw")]
    assert attempts[-1] == ("yahoo", result)
    assert http.get.call_args.kwargs["timeout"] == 30.0


def test_fallback_default_retry_policy_is_unchanged(monkeypatch):
    client = fb.FallbackNav(eodhd_key="")
    http = Mock()
    http.get.return_value.status_code = 503
    monkeypatch.setattr(client, "_http", lambda: http)
    sleep = Mock()
    monkeypatch.setattr(fb.time, "sleep", sleep)
    client.fetch_observations("TEST", DAY, DAY)
    assert http.get.call_count == 2
    assert [c.args[0] for c in sleep.call_args_list] == [1.0, 4.0]
    assert "timeout" not in http.get.call_args.kwargs
