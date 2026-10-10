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
# Verification pass: obligations come from the request and the store
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


def _verify(fetched, stored, start=START, end=AS_OF):
    return w.verify_history(fetched, stored, start=start, end=end)


def test_the_interval_is_fixed_before_the_fetch():
    stored = _stored(D(2026, 9, 1), AS_OF)
    # meta endDate earlier than the stored tail: the tail still has to be covered
    assert w.verification_interval(START, D(2026, 9, 18), stored, AS_OF) == (START, AS_OF)
    assert w.verification_interval(START, AS_OF, {}, AS_OF) == (START, AS_OF)
    assert w.verification_interval(START, None, {}, AS_OF) == (START, AS_OF)


def test_a_ticker_without_rows_loads_the_whole_interval():
    check = _verify(_fetched(), {})
    assert check.verdict == "load"
    assert [r[1] for r in check.missing] == list(_bdays(START, AS_OF))


def test_rows_reaching_the_start_are_not_complete_until_verified():
    """Re-gate 1, P1 #1: 500 of 1,200 prefix rows committed by an older writer,
    then the stored tail. min(date) reaches startDate, yet 700 dates are
    missing: the pass inserts them instead of declaring completion."""
    start, tail = D(2020, 1, 1), D(2024, 1, 2)
    prefix = list(_bdays(start, tail - dt.timedelta(days=1)))
    stored = {**_stored(prefix[0], prefix[499]), **_stored(tail, AS_OF)}
    check = _verify(_fetched(start, AS_OF), stored, start=start)
    assert check.verdict == "load"
    assert [r[1] for r in check.missing] == prefix[500:]


def test_complete_store_is_raw_verified_without_an_adjusted_check():
    # A dividend moved Tiingo's adjusted basis, but nothing is missing: no insert.
    check = _verify(_fetched(factor=0.99), _stored(START, AS_OF))
    assert check.verdict == "complete"
    assert check.missing == ()


def test_interior_gaps_are_loaded_like_a_prefix():
    gap = {D(2026, 9, 1), D(2026, 9, 2)}
    check = _verify(_fetched(), _stored(START, AS_OF, skip=gap))
    assert check.verdict == "load"
    assert {r[1] for r in check.missing} == gap


def test_any_raw_close_difference_fails_closed():
    stored = _stored(D(2026, 9, 1), AS_OF)
    stored[D(2026, 10, 7)]["close"] *= 2
    check = _verify(_fetched(), stored)
    assert (check.verdict, check.detail) == (
        "conflict", "raw_differs: ratio=0.500000 on 1/29 sessions")


def test_a_raw_conflict_is_reported_even_when_a_session_is_omitted():
    """Re-gate 2, P3: a known raw conflict wins over a retryable omission."""
    stored = _stored(D(2026, 9, 1), AS_OF)
    stored[D(2026, 10, 7)]["close"] *= 2
    check = _verify(_fetched(skip={D(2026, 9, 15)}), stored)
    assert check.verdict == "conflict"


def test_adjusted_basis_must_match_on_all_shared_dates_to_insert():
    stored = _stored(D(2026, 9, 1), AS_OF)
    stored[AS_OF] = {k: v * 2 if k != "close" else v for k, v in stored[AS_OF].items()}
    check = _verify(_fetched(), stored)
    assert (check.verdict, check.detail) == (
        "rebase", "adjusted_moved: ratio=0.500000 on 1/29 sessions")


def test_two_for_one_split_after_the_stored_rows_is_a_rebase():
    check = _verify(_fetched(factor=0.5), _stored(D(2026, 9, 1), AS_OF))
    assert (check.verdict, check.detail) == (
        "rebase", "adjusted_moved: ratio=0.500000 on 29/29 sessions")


def test_a_response_omitting_stored_boundary_sessions_cannot_insert():
    """Re-gate 1, P1 #2: Sep 21-Oct 7 omitted, Oct 8-9 returned."""
    boundary = set(_bdays(D(2026, 9, 21), D(2026, 10, 7)))
    stored = {**_stored(D(2026, 9, 21), D(2026, 10, 7), factor=1.0),
              **_stored(D(2026, 10, 8), AS_OF, factor=0.5)}
    check = _verify(_fetched(factor=0.5, skip=boundary), stored)
    assert (check.verdict, check.detail) == (
        "incomplete", "stored_sessions_missing=13 first=2026-09-21")


def test_a_truncated_response_cannot_drop_the_stored_tail_from_its_obligations():
    """Re-gate 2, P1 (i): meta start Aug 3; the response holds only Aug 3-Sep 18
    at factor 0.5; stored Sep 21-Oct 9 stays at 1.0. No shared session, so an
    adjusted check would pass vacuously and insert a +103% seam."""
    stored = _stored(D(2026, 9, 21), AS_OF, factor=1.0)
    _, end = w.verification_interval(START, AS_OF, stored, AS_OF)
    check = _verify(_fetched(START, D(2026, 9, 18), factor=0.5), stored, end=end)
    assert (check.verdict, check.detail) == ("incomplete", "no_shared_sessions")
    assert check.missing == ()


def test_a_truncated_response_is_not_a_verified_complete_series():
    """Re-gate 2, P1 (ii): the full Aug 3-Oct 9 series is stored, the response
    ends at Sep 18. Not 'verified: 35 sessions, 0 missing'."""
    check = _verify(_fetched(START, D(2026, 9, 18)), _stored(START, AS_OF))
    assert check.verdict == "incomplete"
    assert check.detail == ("ends_before_interval_end: last=2026-09-18 end=2026-10-09"
                            " stored_max=2026-10-09")


def test_a_cold_response_must_reach_the_interval_end():
    check = _verify(_fetched(START, D(2026, 9, 18)), {})
    assert (check.verdict, check.detail) == (
        "incomplete", "ends_before_interval_end: last=2026-09-18 end=2026-10-09")


def test_stored_rows_outside_the_provider_range_fail_closed():
    """Re-gate 2, P1 (c): rows before a startDate Tiingo has since advanced are
    not silently exempted."""
    stored = {**_stored(D(2026, 7, 1), D(2026, 7, 31)), **_stored(START, AS_OF)}
    check = _verify(_fetched(), stored)
    assert check.verdict == "conflict"
    assert check.detail.startswith(
        "stored_outside_provider_range: 23 stored sessions outside 2026-08-03..2026-10-09")


def test_bars_outside_the_request_are_incomplete():
    check = _verify(_fetched(D(2026, 7, 27)), {})
    assert check.detail.startswith("bars_outside_request")


def test_an_empty_or_late_starting_response_is_incomplete():
    assert _verify([], _stored(START, AS_OF)).detail == "empty_window"
    late = _verify(_fetched(D(2026, 9, 1)), {})
    assert (late.verdict, late.detail) == (
        "incomplete", "starts_after_start_date: first=2026-09-01 sessions_skipped=21")
    # A startDate on a weekend is not "late".
    assert _verify(_fetched(D(2026, 8, 3)), {}, start=D(2026, 8, 1)).verdict == "load"


def test_coverage_tolerance_is_sessions_not_calendar_days():
    """Codex 4237494727: a Monday startDate whose first bar is that Friday
    skipped four real sessions; calendar-day slack would have let it pass."""
    monday, friday = D(2026, 8, 3), D(2026, 8, 7)
    check = _verify(_fetched(friday), {}, start=monday)
    assert (check.verdict, check.detail) == (
        "incomplete", "starts_after_start_date: first=2026-08-07 sessions_skipped=4")
    # A holiday is not a session: Labor Day startDate, first bar the Tuesday.
    labor_day = D(2026, 9, 7)
    assert w.sessions_between(labor_day, labor_day) == 0
    assert _verify(_fetched(D(2026, 9, 8)), {}, start=labor_day).verdict == "load"
    # At the end: an interval ending on Labor Day is covered by Friday's bar...
    assert _verify(_fetched(START, D(2026, 9, 4)), {}, end=labor_day).verdict == "load"
    # ...but not when a session follows the last bar.
    assert _verify(_fetched(START, D(2026, 9, 3)), {}, end=labor_day).detail == (
        "ends_before_interval_end: last=2026-09-03 end=2026-09-07")


def test_float_noise_within_tolerance_matches():
    fetched = [_row(d, _px(d), _px(d) * (1 + 5e-8)) for d in _bdays(START, AS_OF)]
    stored = _stored(START, AS_OF, skip={D(2026, 9, 1)})
    assert _verify(fetched, stored).verdict == "load"


def test_preview_classification():
    assert w.classify_history_task(w.HistoryTask("X", None, None)) == "new"
    assert w.classify_history_task(w.HistoryTask("X", None, D(2024, 6, 11))) == "existing"


# ──────────────────────────────────────────────────────────────────────────────
# Malformed elements
# ──────────────────────────────────────────────────────────────────────────────
GOOD_BAR = {"date": "2026-10-09T00:00:00.000Z", "open": 1.0, "high": 1.0, "low": 1.0,
            "close": 1.0, "volume": 10, "adjOpen": 1.0, "adjHigh": 1.0, "adjLow": 1.0,
            "adjClose": 1.0, "adjVolume": 10, "divCash": 0.0, "splitFactor": 1.0}


@pytest.mark.parametrize("element", [
    None, "bad", 7, ["x"], {**GOOD_BAR, "date": None}, {**GOOD_BAR, "date": "not-a-date"},
    {**GOOD_BAR, "close": "1.0"}, {**GOOD_BAR, "volume": True}, {**GOOD_BAR, "adjClose": None},
    {**GOOD_BAR, "close": float("nan")}, {**GOOD_BAR, "close": "N/A"},
    {**GOOD_BAR, "high": float("inf")}, {**GOOD_BAR, "adjLow": -float("inf")},
    {**GOOD_BAR, "low": -1.0}, {**GOOD_BAR, "adjOpen": 0.0}, {**GOOD_BAR, "volume": -5},
    {**GOOD_BAR, "divCash": -0.1}, {**GOOD_BAR, "splitFactor": 0.0},
])
def test_build_eod_rows_drops_every_non_conforming_element(element):
    assert w.build_eod_rows("X", [GOOD_BAR, element]) == w.build_eod_rows("X", [GOOD_BAR])
    assert len(w.build_eod_rows("X", [GOOD_BAR])) == 1


# ──────────────────────────────────────────────────────────────────────────────
# Ring admission and retry backoff
# ──────────────────────────────────────────────────────────────────────────────
def test_ring_excludes_only_zero_row_tickers_this_run_will_load():
    marks = {"DONE": AS_OF, "PART": AS_OF, "RBAS": AS_OF}
    batch = frozenset({"PART", "COLD"})
    assert w.ring_excluded(["DONE", "PART", "RBAS", "COLD", "LATER"], marks, batch) == {"COLD"}
    # Cap 0 (no batch): nothing is excluded, the ring is exactly as before.
    assert w.ring_excluded(["DONE", "COLD"], marks, frozenset()) == frozenset()


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


@pytest.mark.parametrize("body", [[None], ["bad"], [{"date": "2026-10-09"}, None]])
def test_fetch_daily_bars_result_rejects_non_object_elements(client, body):
    client._client.get = lambda *a, **k: _Resp(200, body)  # type: ignore[assignment]
    assert client.fetch_daily_bars_result("TSM", D(1997, 10, 9), AS_OF) == ("invalid_payload", [])


def test_no_api_key_sends_nothing(client):
    client._key = ""
    client._client.get = lambda *a, **k: pytest.fail("no request without a key")  # type: ignore[assignment]
    assert client.fetch_meta_result("TSM") == ("not_configured", None)
    assert client.fetch_daily_bars_result("TSM", D(1997, 10, 9), AS_OF) == ("not_configured", [])


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
