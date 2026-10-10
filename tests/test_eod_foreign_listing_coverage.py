"""DB-free tests for the warmer's W1c foreign-listing full-history source.

The database behaviour (W1c selection, instruments seeding, cold starts,
truncated backfill, compressed chunks, resume) is covered on TimescaleDB in
``test_eod_foreign_listing_coverage_db.py``. These pin the pure rules and the
Tiingo outcome mapping without a network or a database.
"""

from __future__ import annotations

import datetime as dt
import re

import pytest

from src.workers import eod_prices_warmer as w
from src.workers._tiingo import TiingoClient

D = dt.date


# ──────────────────────────────────────────────────────────────────────────────
# Source shape
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("symbol", ["TSM", "ASML", "NVO", "SONY", "BRK-B", "BNRE-A", "Q"])
def test_us_listing_ticker_shape_accepts_exchange_tickers(symbol):
    assert re.fullmatch(w.US_LISTING_TICKER_PATTERN, symbol)


@pytest.mark.parametrize(
    "symbol",
    # home-market codes, note lines, preferred/warrant suffixes, too long
    ["SANB11", "VIVT3", "CPLE6", "AMX22", "BCS23A", "NM-PG", "ALLG-WS", "LEV-WSA", "ABCDEF"],
)
def test_us_listing_ticker_shape_rejects_non_exchange_symbols(symbol):
    assert not re.fullmatch(w.US_LISTING_TICKER_PATTERN, symbol)






# ──────────────────────────────────────────────────────────────────────────────
# Cold start and backfill windows
# ──────────────────────────────────────────────────────────────────────────────
AS_OF = D(2026, 10, 9)


def test_window_for_a_ticker_without_rows_is_tiingo_start_to_as_of():
    assert w.history_window(D(1997, 10, 9), None, AS_OF) == (D(1997, 10, 9), AS_OF)
    # The screener cold start would have been only 745 days.
    assert AS_OF - dt.timedelta(days=w.NEW_TICKER_LOOKBACK_DAYS) > D(1997, 10, 9)


def test_window_for_truncated_rows_stops_the_day_before_the_first_row():
    assert w.history_window(D(2005, 8, 5), D(2024, 6, 11), AS_OF) == (
        D(2005, 8, 5), D(2024, 6, 10))


def test_rows_reaching_the_start_within_tolerance_are_complete():
    # startDate on a Saturday; the first bar is the following Monday.
    assert w.history_window(D(2005, 8, 6), D(2005, 8, 8), AS_OF) is None
    assert w.history_window(D(2005, 8, 5), D(2005, 8, 5), AS_OF) is None
    assert w.history_window(
        D(2005, 8, 5), D(2005, 8, 5) + dt.timedelta(days=w.HISTORY_START_TOLERANCE_DAYS + 1),
        AS_OF) is not None


def _task(ticker="X", *, instrument=None, min_date=None):
    return w.HistoryTask(ticker, instrument, min_date)


FULL = {"name": "X Co", "exchange_code": "NYSE",
        "tiingo_start_date": D(2001, 1, 2), "tiingo_end_date": D(2026, 10, 9)}


def test_meta_is_needed_for_a_missing_or_incomplete_instruments_row():
    assert _task().needs_meta
    assert _task(instrument={**FULL, "exchange_code": None}).needs_meta
    assert _task(instrument={**FULL, "tiingo_start_date": None}).needs_meta
    assert _task(instrument={**FULL, "tiingo_end_date": None}).needs_meta
    # A NULL name alone does not cost a request.
    assert not _task(instrument={**FULL, "name": None}).needs_meta


def test_classification_used_by_the_preview():
    assert w.classify_history_task(_task()) == "new"
    assert w.classify_history_task(_task(instrument=FULL)) == "new"
    assert w.classify_history_task(
        _task(instrument={**FULL, "tiingo_start_date": None}, min_date=D(2024, 6, 11))
    ) == "needs_meta"
    assert w.classify_history_task(_task(instrument=FULL, min_date=D(2024, 6, 11))) == "truncated"
    assert w.classify_history_task(_task(instrument=FULL, min_date=D(2001, 1, 2))) == "complete"


# ──────────────────────────────────────────────────────────────────────────────
# Per-run cap
# ──────────────────────────────────────────────────────────────────────────────
def test_history_cap_default_env_explicit_and_limit(monkeypatch):
    monkeypatch.delenv(w.HISTORY_LIMIT_ENV, raising=False)
    assert w.history_cap(None, None) == w.HISTORY_TICKERS_PER_RUN
    # Production ring cap (WORKER_LIMIT=2000) leaves the default in force.
    assert w.history_cap(None, 2000) == w.HISTORY_TICKERS_PER_RUN
    # A smoke run never fetches more history than its own limit.
    assert w.history_cap(None, 5) == 5
    monkeypatch.setenv(w.HISTORY_LIMIT_ENV, "40")
    assert w.history_cap(None, None) == 40
    assert w.history_cap(7, None) == 7
    monkeypatch.setenv(w.HISTORY_LIMIT_ENV, "0")
    assert w.history_cap(None, None) == 0  # kill switch


@pytest.mark.parametrize("raw", ["ten", "-1"])
def test_history_cap_rejects_bad_values(monkeypatch, raw):
    monkeypatch.setenv(w.HISTORY_LIMIT_ENV, raw)
    with pytest.raises(ValueError):
        w.history_cap(None, None)


def test_history_phase_requests_stay_inside_the_shared_budget():
    from src.workers._tiingo import TIINGO_MAX_REQUESTS_PER_HOUR

    # meta + one price request per ticker, on the same paced bucket
    assert 2 * w.HISTORY_TICKERS_PER_RUN < TIINGO_MAX_REQUESTS_PER_HOUR // 10
    assert w.FETCH_RATE_PER_S * 3600 <= TIINGO_MAX_REQUESTS_PER_HOUR






# ──────────────────────────────────────────────────────────────────────────────
# Tiingo outcome mapping (fake responses, no network)
# ──────────────────────────────────────────────────────────────────────────────
class _Resp:
    def __init__(self, status_code, payload=None, *, bad_json=False):
        self.status_code = status_code
        self._payload = payload
        self._bad_json = bad_json

    def json(self):
        if self._bad_json:
            raise ValueError("not json")
        return self._payload


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr("src.workers._tiingo.time.sleep", lambda s: None)
    c = TiingoClient(key="test")
    yield c
    c.close()


META = {"ticker": "tsm", "name": "Taiwan Semiconductor", "exchangeCode": "NYSE",
        "startDate": "1997-10-09", "endDate": "2026-10-09", "description": "..."}


@pytest.mark.parametrize(
    ("responses", "expected"),
    [
        ([_Resp(200, META)], ("found", META)),
        ([_Resp(404, {"detail": "Not found."})], ("not_found", None)),
        ([_Resp(400, {"detail": "bad"})], ("invalid_payload", None)),
        ([_Resp(200, ["a", "list"])], ("invalid_payload", None)),
        ([_Resp(200, bad_json=True)], ("invalid_payload", None)),
        ([_Resp(503), _Resp(503), _Resp(503)], ("transient_error", None)),
        ([_Resp(503), _Resp(200, META)], ("found", META)),
    ],
)
def test_fetch_meta_result_keeps_unknown_apart_from_failures(client, responses, expected):
    seq = iter(responses)
    client._client.get = lambda *a, **k: next(seq)  # type: ignore[assignment]
    assert client.fetch_meta_result("TSM") == expected
    # The legacy accessor keeps its contract: payload or None.
    seq = iter(responses)
    assert client.fetch_meta("TSM") == expected[1]


def test_fetch_daily_bars_result_tells_empty_from_failure(client):
    client._client.get = lambda *a, **k: _Resp(200, [])  # type: ignore[assignment]
    assert client.fetch_daily_bars_result("TSM", D(1997, 10, 9), AS_OF) == ("empty", [])
    client._client.get = lambda *a, **k: _Resp(404)  # type: ignore[assignment]
    assert client.fetch_daily_bars_result("TSM", D(1997, 10, 9), AS_OF) == ("not_found", [])
    client._client.get = lambda *a, **k: _Resp(500)  # type: ignore[assignment]
    assert client.fetch_daily_bars_result("TSM", D(1997, 10, 9), AS_OF) == ("transient_error", [])
    bar = {"date": "1997-10-09T00:00:00.000Z"}
    client._client.get = lambda *a, **k: _Resp(200, [bar])  # type: ignore[assignment]
    assert client.fetch_daily_bars_result("TSM", D(1997, 10, 9), AS_OF) == ("success_new", [bar])
