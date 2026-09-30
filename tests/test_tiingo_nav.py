"""Typed NAV provider outcomes; older OHLCV/list callers retain their wrappers."""

from __future__ import annotations

import datetime as dt
import sys
from types import SimpleNamespace

from src.workers._tiingo import TiingoClient, parse_nav_observations


def test_tiingo_adjusted_raw_and_null_bars():
    series = parse_nav_observations(
        [
            {"date": "2026-06-08T00:00:00Z", "close": 10, "adjClose": 9.9},
            {"date": "2026-06-09T00:00:00Z", "close": 11, "adjClose": None},
            {"date": "2026-06-10T00:00:00Z", "close": None},
        ],
        source="tiingo",
    )
    assert [o.kind for o in series] == ["adjusted", "raw", "unknown"]
    assert [o.price for o in series] == [9.9, 11.0, None]


class _Bucket:
    refill_rate = 1.0

    def acquire(self):
        pass


class _Response:
    def __init__(self, code, payload=None):
        self.status_code = code
        self.payload = payload

    def json(self):
        return self.payload


def _client(monkeypatch):
    # The lean disposable DB image intentionally has no HTTP dependencies.
    transport = SimpleNamespace(get=None, close=lambda: None)
    monkeypatch.setitem(
        sys.modules, "httpx", SimpleNamespace(Client=lambda **_kw: transport)
    )
    return TiingoClient(key="synthetic", bucket=_Bucket()), transport


def test_not_found_not_empty_and_no_secret_error_text(monkeypatch):
    client, transport = _client(monkeypatch)
    transport.get = lambda *_a, **_kw: _Response(404)
    result = client.fetch_daily_observations(
        "X", dt.date(2026, 6, 8), dt.date(2026, 6, 9)
    )
    assert result.status == "not_found" and not result.observations
    assert client.fetch_daily_bars("X", dt.date(2026, 6, 8)) == []
    client.close()


def test_successful_http_empty_and_malformed_are_distinct(monkeypatch):
    client, transport = _client(monkeypatch)
    response = {"value": _Response(200, [])}
    transport.get = lambda *_a, **_kw: response["value"]
    assert client.fetch_daily_observations(
        "X", dt.date(2026, 6, 8), dt.date(2026, 6, 9)
    ).status == ("empty")
    response["value"] = _Response(200, {"error": "PRIVATE_RESPONSE_BODY"})
    result = client.fetch_daily_observations(
        "X", dt.date(2026, 6, 8), dt.date(2026, 6, 9)
    )
    assert result.status == "invalid_payload" and "PRIVATE_RESPONSE_BODY" not in repr(
        result
    )
    client.close()


def test_network_failure_is_typed_without_exception_content(monkeypatch):
    client, transport = _client(monkeypatch)
    transport.get = lambda *_a, **_kw: (_ for _ in ()).throw(
        ConnectionError("private_url")
    )
    monkeypatch.setattr("src.workers._tiingo._RETRY_SLEEPS", (0, 0, 0))
    monkeypatch.setattr("src.workers._tiingo.time.sleep", lambda *_: None)
    result = client.fetch_daily_observations(
        "X", dt.date(2026, 6, 8), dt.date(2026, 6, 9)
    )
    assert result.status == "transient_error" and "private_url" not in repr(result)
    client.close()
