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
# Verification pass (rule: one full-range check before history_complete)
# ──────────────────────────────────────────────────────────────────────────────
AS_OF = D(2026, 10, 9)
START = D(2026, 8, 3)


def _bdays(a, b):
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += dt.timedelta(days=1)


def _row(day, close, adj):
    # build_eod_rows layout: ticker, date, open, high, low, close, volume,
    # adj_open, adj_high, adj_low, adj_close, adj_volume, div_cash, split_factor
    return ("X", day, close, close, close, close, 100, adj, adj, adj, adj, 100, 0.0, 1.0)


def _px(day):
    return 20.0 + day.toordinal() % 50 / 10


def _fetched(a=START, b=AS_OF, factor=1.0, skip=()):
    return [_row(d, _px(d), _px(d) * factor) for d in _bdays(a, b) if d not in skip]


def _stored(a, b, factor=1.0, skip=()):
    return {d: {"close": _px(d), "adj_open": _px(d) * factor, "adj_high": _px(d) * factor,
                "adj_low": _px(d) * factor, "adj_close": _px(d) * factor}
            for d in _bdays(a, b) if d not in skip}


def test_a_ticker_without_rows_loads_the_whole_range():
    check = w.verify_history(_fetched(), {}, start=START)
    assert check.verdict == "load"
    assert [r[1] for r in check.missing] == list(_bdays(START, AS_OF))


def test_rows_reaching_the_start_are_not_complete_until_verified():
    """Re-gate P1 #1: 500 of 1,200 prefix rows committed by an older writer,
    then the stored 2024-2026 tail. min(date) reaches startDate, yet 700 dates
    are missing: the pass inserts them instead of declaring completion."""
    start, tail = D(2020, 1, 1), D(2024, 1, 2)
    prefix = list(_bdays(start, tail - dt.timedelta(days=1)))
    stored = {**_stored(prefix[0], prefix[499]), **_stored(tail, AS_OF)}
    check = w.verify_history(_fetched(start, AS_OF), stored, start=start)
    assert check.verdict == "load"
    assert [r[1] for r in check.missing] == prefix[500:]


def test_complete_store_is_raw_verified_without_an_adjusted_check():
    # A dividend moved Tiingo's adjusted basis, but nothing is missing: no insert,
    # so no seam can be created.
    check = w.verify_history(_fetched(factor=0.99), _stored(START, AS_OF), start=START)
    assert check.verdict == "complete"
    assert check.missing == ()


def test_interior_gaps_are_loaded_like_a_prefix():
    gap = {D(2026, 9, 1), D(2026, 9, 2)}
    check = w.verify_history(_fetched(), _stored(START, AS_OF, skip=gap), start=START)
    assert check.verdict == "load"
    assert {r[1] for r in check.missing} == gap


def test_any_raw_close_difference_fails_closed():
    stored = _stored(D(2026, 9, 1), AS_OF)
    stored[D(2026, 10, 7)]["close"] *= 2      # a late date, not just the first sessions
    check = w.verify_history(_fetched(), stored, start=START)
    assert check.verdict == "conflict"
    assert check.detail == "raw_differs: ratio=0.500000 on 1/29 sessions"


def test_adjusted_basis_must_match_on_all_common_dates_to_insert():
    # Only the LAST stored session moved; a first-ten check would have passed.
    stored = _stored(D(2026, 9, 1), AS_OF)
    stored[AS_OF] = {k: v * 2 if k != "close" else v for k, v in stored[AS_OF].items()}
    check = w.verify_history(_fetched(), stored, start=START)
    assert check.verdict == "rebase"
    assert check.detail == "adjusted_moved: ratio=0.500000 on 1/29 sessions"


def test_two_for_one_split_after_the_stored_rows_is_a_rebase():
    check = w.verify_history(_fetched(factor=0.5), _stored(D(2026, 9, 1), AS_OF), start=START)
    assert (check.verdict, check.detail) == (
        "rebase", "adjusted_moved: ratio=0.500000 on 29/29 sessions")


def test_a_response_omitting_stored_boundary_sessions_cannot_insert():
    """Re-gate P1 #2: stored Sep 21–Oct 7 at factor 1.0, Oct 8–9 refreshed at
    0.5. The response carries the prefix and Oct 8–9 at 0.5 but omits Sep 21–
    Oct 7, so only matching sessions are common. It must not insert a +103% seam."""
    boundary = set(_bdays(D(2026, 9, 21), D(2026, 10, 7)))
    stored = {**_stored(D(2026, 9, 21), D(2026, 10, 7), factor=1.0),
              **_stored(D(2026, 10, 8), AS_OF, factor=0.5)}
    fetched = _fetched(factor=0.5, skip=boundary)
    check = w.verify_history(fetched, stored, start=START)
    assert check.verdict == "incomplete"
    assert check.detail == "stored_sessions_missing=13 first=2026-09-21"


def test_an_empty_or_late_starting_response_is_incomplete():
    assert w.verify_history([], _stored(START, AS_OF), start=START).detail == "empty_window"
    late = w.verify_history(_fetched(D(2026, 9, 1)), {}, start=START)
    assert (late.verdict, late.detail) == ("incomplete", "starts_after_start_date: first=2026-09-01")
    # A startDate on a weekend is not "late".
    assert w.verify_history(_fetched(D(2026, 8, 3)), {}, start=D(2026, 8, 1)).verdict == "load"


def test_stored_rows_outside_the_fetched_range_are_ignored():
    stored = {**_stored(D(2020, 1, 1), D(2020, 1, 31)), **_stored(START, AS_OF)}
    assert w.verify_history(_fetched(), stored, start=START).verdict == "complete"


def test_float_noise_within_tolerance_matches():
    fetched = [_row(d, _px(d), _px(d) * (1 + 5e-8)) for d in _bdays(START, AS_OF)]
    stored = _stored(START, AS_OF, skip={START})
    assert w.verify_history(fetched, stored, start=START).verdict == "load"


def test_preview_classification():
    assert w.classify_history_task(w.HistoryTask("X", None, None)) == "new"
    assert w.classify_history_task(w.HistoryTask("X", None, D(2024, 6, 11))) == "existing"


# ──────────────────────────────────────────────────────────────────────────────
# Ring admission and retry backoff
# ──────────────────────────────────────────────────────────────────────────────
def test_ring_excludes_only_covered_tickers_without_rows():
    marks = {"DONE": AS_OF, "PART": AS_OF, "RBAS": AS_OF}
    assert w.ring_excluded(["DONE", "PART", "RBAS", "COLD"], marks) == frozenset({"COLD"})


def test_retry_backoff_doubles_and_is_capped():
    now = dt.datetime(2026, 10, 10, tzinfo=dt.UTC)
    hours = [(w.retry_after(now, n) - now) / dt.timedelta(hours=1) for n in range(1, 7)]
    assert hours == [12, 24, 48, 96, 168, 168]


# ──────────────────────────────────────────────────────────────────────────────
# Per-run cap
# ──────────────────────────────────────────────────────────────────────────────
def test_history_cap_default_env_explicit_and_limit(monkeypatch):
    monkeypatch.delenv(w.HISTORY_LIMIT_ENV, raising=False)
    assert w.history_cap(None, None) == w.HISTORY_TICKERS_PER_RUN == 25
    # Production ring cap (WORKER_LIMIT=2000) leaves the default in force.
    assert w.history_cap(None, 2000) == w.HISTORY_TICKERS_PER_RUN
    # A smoke run never fetches more history than its own limit.
    assert w.history_cap(None, 5) == 5
    # Staged rollout: start small, raise to the steady state.
    monkeypatch.setenv(w.HISTORY_LIMIT_ENV, "25")
    assert w.history_cap(None, 2300) == 25
    monkeypatch.setenv(w.HISTORY_LIMIT_ENV, "250")
    assert w.history_cap(None, 2300) == 250
    assert w.history_cap(7, None) == 7
    monkeypatch.setenv(w.HISTORY_LIMIT_ENV, "0")
    assert w.history_cap(None, None) == 0  # kill switch


@pytest.mark.parametrize("raw", ["ten", "-1"])
def test_history_cap_rejects_bad_values(monkeypatch, raw):
    monkeypatch.setenv(w.HISTORY_LIMIT_ENV, raw)
    with pytest.raises(ValueError):
        w.history_cap(None, None)


def test_history_phase_http_attempts_stay_inside_the_shared_budget():
    from src.workers._tiingo import _RETRY_SLEEPS, TIINGO_MAX_REQUESTS_PER_HOUR

    # Two logical requests per ticker (meta + one window), each up to the
    # client's full retry ladder, at the 250 steady-state cap.
    worst_case_http = 250 * 2 * len(_RETRY_SLEEPS)
    assert worst_case_http == 1500
    assert worst_case_http < TIINGO_MAX_REQUESTS_PER_HOUR // 5
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
        ([_Resp(429), _Resp(429), _Resp(429)], ("rate_limited", None)),
        ([_Resp(429), _Resp(503), _Resp(503)], ("transient_error", None)),
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
