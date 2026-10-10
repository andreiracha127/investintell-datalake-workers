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
    assert w.history_fetch_window(D(1997, 10, 9), None, AS_OF) == (D(1997, 10, 9), AS_OF)
    # The screener cold start would have been only 745 days.
    assert AS_OF - dt.timedelta(days=w.NEW_TICKER_LOOKBACK_DAYS) > D(1997, 10, 9)


def test_window_for_truncated_rows_runs_into_the_first_stored_sessions():
    first = D(2024, 6, 11)
    start, end = w.history_fetch_window(D(2005, 8, 5), first, AS_OF)
    assert start == D(2005, 8, 5)
    assert end == first + dt.timedelta(days=w.OVERLAP_CALENDAR_DAYS)
    # 21 calendar days hold at least OVERLAP_SESSIONS sessions even with holidays.
    assert w.OVERLAP_CALENDAR_DAYS >= w.OVERLAP_SESSIONS * 7 // 5 + 4
    # Never past the run date.
    assert w.history_fetch_window(D(2005, 8, 5), AS_OF - dt.timedelta(days=3), AS_OF)[1] == AS_OF


def test_rows_reaching_the_start_within_tolerance_need_no_window():
    # startDate on a Saturday; the first bar is the following Monday.
    assert w.history_fetch_window(D(2005, 8, 6), D(2005, 8, 8), AS_OF) is None
    assert w.history_fetch_window(D(2005, 8, 5), D(2005, 8, 5), AS_OF) is None
    assert w.history_fetch_window(
        D(2005, 8, 5), D(2005, 8, 5) + dt.timedelta(days=w.HISTORY_START_TOLERANCE_DAYS + 1),
        AS_OF) is not None


def test_preview_classification():
    assert w.classify_history_task(w.HistoryTask("X", None, None)) == "new"
    assert w.classify_history_task(w.HistoryTask("X", None, D(2024, 6, 11))) == "existing"


# ──────────────────────────────────────────────────────────────────────────────
# Adjustment basis at the junction with stored rows
# ──────────────────────────────────────────────────────────────────────────────
def _row(day, close, adj):
    # build_eod_rows layout: ticker, date, open, high, low, close, volume,
    # adj_open, adj_high, adj_low, adj_close, adj_volume, div_cash, split_factor
    return ("X", day, close, close, close, close, 100, adj, adj, adj, adj, 100, 0.0, 1.0)


def _stored(day, close, adj):
    return {"close": close, "adj_open": adj, "adj_high": adj, "adj_low": adj, "adj_close": adj}


SESSIONS = [D(2024, 6, 11) + dt.timedelta(days=i) for i in range(14)]


def test_same_basis_matches():
    fetched = [_row(d, 100.0, 97.5) for d in SESSIONS]
    stored = {d: _stored(d, 100.0, 97.5) for d in SESSIONS}
    assert w.compare_basis(fetched, stored) == w.BasisCheck("match", 10, 1.0)


def test_two_for_one_split_after_the_stored_rows_is_a_moved_basis():
    """The gate's case: rows stored before a 2:1 split with adj = raw = 100;
    Tiingo now reports raw 100 (pre-split) but adjClose 50 for the same dates.
    Joining a prefix on the new basis would fabricate a +100% return."""
    fetched = [_row(d, 100.0, 50.0) for d in SESSIONS]
    stored = {d: _stored(d, 100.0, 100.0) for d in SESSIONS}
    check = w.compare_basis(fetched, stored)
    assert check.verdict == "adjusted_moved"
    assert check.ratio == pytest.approx(0.5)
    assert check.detail() == "adjusted_moved: ratio=0.500000 over 10 sessions"


def test_a_dividend_sized_move_is_also_caught():
    fetched = [_row(d, 100.0, 98.0) for d in SESSIONS]
    stored = {d: _stored(d, 100.0, 98.0 / 0.99) for d in SESSIONS}
    check = w.compare_basis(fetched, stored)
    assert (check.verdict, round(check.ratio, 6)) == ("adjusted_moved", 0.99)


def test_raw_close_difference_is_a_conflict_not_a_rebase():
    fetched = [_row(d, 40.0, 40.0) for d in SESSIONS]
    stored = {d: _stored(d, 100.0, 100.0) for d in SESSIONS}
    check = w.compare_basis(fetched, stored)
    assert (check.verdict, check.ratio) == ("raw_differs", pytest.approx(0.4))


def test_float_noise_within_tolerance_matches():
    fetched = [_row(d, 100.0, 97.5 * (1 + 5e-8)) for d in SESSIONS]
    stored = {d: _stored(d, 100.0, 97.5) for d in SESSIONS}
    assert w.compare_basis(fetched, stored).verdict == "match"


def test_only_the_first_stored_sessions_are_compared():
    fetched = [_row(d, 100.0, 97.5 if i < 10 else 50.0) for i, d in enumerate(SESSIONS)]
    stored = {d: _stored(d, 100.0, 97.5) for d in SESSIONS}
    assert w.compare_basis(fetched, stored).verdict == "match"


def test_no_common_session_cannot_be_judged():
    fetched = [_row(d, 100.0, 97.5) for d in SESSIONS[:3]]
    stored = {d: _stored(d, 100.0, 97.5) for d in SESSIONS[5:]}
    assert w.compare_basis(fetched, stored) == w.BasisCheck("no_overlap", 0, None)


# ──────────────────────────────────────────────────────────────────────────────
# Ring admission comes from the recorded status
# ──────────────────────────────────────────────────────────────────────────────
def test_ring_admits_covered_tickers_on_status_not_rows():
    marks = {t: D(2026, 10, 8) for t in ("DONE", "PART", "SCRN", "RBAS", "INCP")}
    status = {
        "DONE": {"status": w.STATUS_COMPLETE},
        "RBAS": {"status": w.STATUS_REBASE},
        "INCP": {"status": w.STATUS_INCOMPLETE},
    }
    excluded = w.ring_excluded(
        ["DONE", "PART", "COLD", "SCRN", "SCLD", "RBAS", "INCP"], status,
        ring_owned=frozenset({"SCRN", "SCLD"}), watermarks=marks)
    # Rows without a complete status (an interrupted or never-run load) are not
    # admission; nor is a retryable failure.
    assert excluded == frozenset({"PART", "COLD", "SCLD", "INCP"})


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
