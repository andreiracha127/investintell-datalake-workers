"""DB-free tests for the warmer's W1c foreign-listing full-history source.

The database behaviour (W1c selection, instruments seeding, cold starts,
truncated backfill, compressed chunks, resume) is covered on TimescaleDB in
``test_eod_foreign_listing_coverage_db.py``. These pin the pure rules and the
Tiingo outcome mapping without a network or a database.
"""

from __future__ import annotations

import datetime as dt
import math
import re

import pytest

import exchange_calendars as xcals

from src.workers import eod_history_validation as v
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
# validate_series: the whole-ticker rules (pure)
# Invariant: a certifying verdict (complete_nothing_to_insert / load) only for
# a complete, coherent, correctly bounded series for the fixed interval.
# ──────────────────────────────────────────────────────────────────────────────
_XNYS = xcals.get_calendar("XNYS", start="1900-01-01")
CAL = v.xnys_calendar()
START, END = D(2026, 8, 3), D(2026, 10, 9)      # 49 XNYS sessions (Labor Day excluded)
AS_OF = END


def sessions(a=START, b=END):
    return [ts.date() for ts in _XNYS.sessions_in_range(a.isoformat(), b.isoformat())]


def _px(day):
    return 20.0 + day.toordinal() % 50 / 10


def bar(day, *, factor=1.0, close=None, **over):
    px = close or _px(day)
    b = {"date": f"{day.isoformat()}T00:00:00.000Z", "open": px, "high": px * 1.01,
         "low": px * 0.99, "close": px, "volume": 1000, "adjOpen": px * factor,
         "adjHigh": px * 1.01 * factor, "adjLow": px * 0.99 * factor, "adjClose": px * factor,
         "adjVolume": 1000.0, "divCash": 0.0, "splitFactor": 1.0}
    b.update(over)
    return b


def series(a=START, b=END, *, skip=(), factor=1.0):
    return [bar(d, factor=factor) for d in sessions(a, b) if d not in skip]


def stored_of(bars):
    """eod_prices rows as the warmer reads them: every STORED_FIELDS column."""
    return {v.parse_bar_date(b["date"]): {col: b[key] for col, key in v.EOD_FIELDS}
            for b in bars}


def V(bars, stored=None, start=START, end=END, gaps=frozenset()):
    return v.validate_series("X", (start, end), bars, stored or {}, CAL, gaps)


def nothing_written(verdict, status=v.VERDICT_INCOMPLETE):
    assert verdict.status == status, verdict
    assert verdict.rows == ()


def test_the_interval_is_fixed_before_the_fetch():
    stored = stored_of(series(D(2026, 9, 1)))
    assert w.verification_interval(START, D(2026, 9, 18), stored, AS_OF) == (START, AS_OF)
    assert w.verification_interval(START, None, {}, AS_OF) == (START, AS_OF)


def test_gate4_interval_starts_at_the_certified_calendar_domain():
    """A Tiingo startDate before 1970 is floored at CALENDAR_SUPPORTED_FROM;
    stored rows before it are out of scope for the end as well."""
    assert v.CALENDAR_SUPPORTED_FROM == D(1970, 1, 1)
    assert w.verification_interval(D(1962, 1, 2), AS_OF, {}, AS_OF) == (D(1970, 1, 1), AS_OF)
    assert w.verification_interval(D(1962, 1, 2), None, {D(1965, 6, 1): {}}, AS_OF) == (
        D(1970, 1, 1), AS_OF)
    assert w.verification_interval(D(1973, 5, 3), D(1990, 1, 2), {}, AS_OF) == (
        D(1973, 5, 3), D(1990, 1, 2))


def test_gate4_an_interval_floored_at_1970_is_inside_the_calendar():
    """The library's first session is 1970-01-02 and it refuses a range that
    starts before it; the floored interval must still validate."""
    assert CAL.sessions(D(1970, 1, 1), D(1970, 1, 9)) == frozenset(sessions(D(1970, 1, 2), D(1970, 1, 9)))
    assert CAL.sessions(D(1970, 1, 1), D(1970, 1, 1)) == frozenset()
    old = [bar(d) for d in sessions(D(1970, 1, 2), D(1970, 2, 27))]
    verdict = V(old, start=D(1970, 1, 1), end=D(1970, 2, 27))
    assert verdict.status == v.VERDICT_LOAD and verdict.rows[0][1] == D(1970, 1, 2)
    for start in (D(1969, 12, 31), D(1899, 12, 1)):
        assert V(old, start=start, end=D(1970, 2, 27)).reason.startswith("interval_outside_calendar")


def test_historical_interval_is_capped_at_as_of():
    """Codex 4237640082: nothing after as_of is in scope."""
    as_of = D(2020, 6, 30)
    stored = {**stored_of(series(D(2020, 6, 1), as_of)), **stored_of(series(D(2026, 9, 1)))}
    assert w.verification_interval(D(1997, 10, 9), AS_OF, stored, as_of) == (D(1997, 10, 9), as_of)
    assert w.verification_interval(D(1997, 10, 9), D(2018, 3, 1), {}, as_of) == (
        D(1997, 10, 9), D(2018, 3, 1))


def test_a_ticker_without_rows_loads_exactly_the_interval_sessions():
    verdict = V(series())
    assert verdict.status == v.VERDICT_LOAD
    assert [r[1] for r in verdict.rows] == sessions() and len(verdict.rows) == 49


# The gate's findings, at the validator ------------------------------------------
@pytest.mark.parametrize("stored_dates", [[], [START, END], [END]])
def test_gate3_finding1_endpoint_only_response_is_never_complete(stored_dates):
    provider = [bar(START), bar(END)]
    stored = stored_of([bar(d) for d in stored_dates])
    verdict = V(provider, stored)
    nothing_written(verdict)
    assert verdict.reason.startswith("sessions_missing=47")


def test_gate3_finding1_one_interior_session_absent_from_both():
    missing = D(2026, 9, 15)
    verdict = V(series(skip={missing}), stored_of([bar(END)]))
    nothing_written(verdict)
    assert verdict.reason == "sessions_missing=1 first=2026-09-15"


def test_gate3_finding1_a_bar_on_a_sunday_is_rejected():
    verdict = V(series() + [bar(D(2026, 8, 9))])
    nothing_written(verdict)
    assert verdict.reason == "off_session_bar: 1 first=2026-08-09"


def test_gate3_finding1_the_calendar_guard_applies_to_the_interval_itself():
    for start in (D(1899, 12, 1), D(1969, 12, 31)):
        verdict = V([bar(start), bar(END)], start=start)
        nothing_written(verdict)
        assert verdict.reason.startswith("interval_outside_calendar")


@pytest.mark.parametrize("over", [
    {"high": 1.0, "low": 100.0, "adjHigh": 1.0, "adjLow": 100.0},            # gate repro
    {"open": 20.0, "high": 10.0, "low": 30.0, "close": 25.0,
     "adjOpen": 1.0, "adjHigh": 900.0, "adjLow": 80.0, "adjClose": 250.0},  # independent case
])
def test_gate3_finding3_incoherent_bars_are_unusable(over):
    provider = series()
    provider[10] = {**provider[10], **over}
    verdict = V(provider)
    nothing_written(verdict)
    assert verdict.reason.startswith("unusable_bar")


def test_gate3_finding3_adjustment_factor_must_agree_within_a_bar():
    provider = series()
    provider[5] = {**provider[5], "adjLow": provider[5]["adjLow"] * 0.95}   # 5% off, order intact
    assert V(provider).reason == "unusable_bar: adjustment_factor at index 5"
    # Tiingo's 3-decimal rounding of adjusted fields stays inside the tolerance.
    provider = series()
    provider[5] = {**provider[5], "adjHigh": round(provider[5]["adjHigh"], 3) + 0.0005}
    assert V(provider).status == v.VERDICT_LOAD


def test_gate3_finding4_a_malformed_date_is_never_truncated():
    provider = series()
    provider[5] = {**provider[5], "date": "2026-08-10garbage"}
    verdict = V(provider)
    nothing_written(verdict)
    assert verdict.reason == "unusable_bar: malformed_date at index 5"


def test_gate3_finding5_a_mixed_adjusted_basis_is_never_complete():
    stored = stored_of(series())
    last = stored[END]
    stored[END] = {k: (x / 2 if k.startswith("adj") else x) for k, x in last.items()}
    verdict = V(series(), stored)
    nothing_written(verdict, v.VERDICT_REBASE)
    assert verdict.reason == "adjusted_moved: ratio=2.000000 on 1/49 sessions"


# Battery: provider responses ------------------------------------------------------
@pytest.mark.parametrize(("first", "last"), [
    (START, D(2026, 9, 18)), (D(2026, 9, 1), END), (D(2026, 8, 17), D(2026, 9, 25))])
def test_battery_a_a_partial_slice_writes_nothing(first, last):
    for stored in ({}, stored_of(series(D(2026, 8, 17), D(2026, 9, 25)))):
        nothing_written(V(series(first, last), stored))


def test_battery_b_duplicates_unsorted_and_outside_dates():
    provider = series()
    assert V(provider + [dict(provider[3])]).reason == "duplicate_dates=1"
    shuffled = provider[1::2] + provider[::2]
    assert V(shuffled) == V(provider)
    assert V(provider + [bar(D(2026, 7, 31))]).reason == "bars_outside_request: first=2026-07-31"
    assert V(provider + [bar(D(2026, 10, 12))]).reason == "bars_outside_request: first=2026-10-12"


def test_battery_c_an_empty_list_or_non_list_body_writes_nothing():
    nothing_written(V([]))
    nothing_written(V([], stored_of(series(D(2026, 9, 1)))))
    nothing_written(V({"detail": "x"}))


def test_battery_d_raw_rebased_is_a_conflict_adjusted_rebased_is_a_rebase():
    stored = stored_of(series())
    raw_moved = [{**b, "open": b["open"] * 2, "high": b["high"] * 2, "low": b["low"] * 2,
                  "close": b["close"] * 2} for b in series()]
    nothing_written(V(raw_moved, stored), v.VERDICT_CONFLICT)
    nothing_written(V(series(factor=0.98), stored), v.VERDICT_REBASE)


@pytest.mark.parametrize(("field", "rel", "status"), [
    ("close", 0.9e-6, v.VERDICT_COMPLETE), ("close", 1.1e-6, v.VERDICT_CONFLICT),
    ("open", 1.1e-6, v.VERDICT_CONFLICT),
    ("adj_close", 0.9e-6, v.VERDICT_COMPLETE), ("adj_close", 1.1e-6, v.VERDICT_REBASE),
    ("adj_low", 1.1e-6, v.VERDICT_REBASE),
])
def test_battery_e_stored_comparison_tolerance_boundary(field, rel, status):
    stored = stored_of(series())
    stored[D(2026, 9, 15)][field] *= 1 + rel
    assert V(series(), stored).status == status


def test_battery_f_unserved_stored_row_and_two_gaps():
    stored = stored_of(series())
    verdict = V(series(skip={D(2026, 9, 23)}), stored)
    nothing_written(verdict)
    assert verdict.reason == "stored_sessions_missing=1 first=2026-09-23"
    gaps = {D(2026, 8, 12), D(2026, 8, 13), D(2026, 9, 29)}
    verdict = V(series(), stored_of(series(skip=gaps)))
    assert verdict.status == v.VERDICT_LOAD
    assert {r[1] for r in verdict.rows} == gaps


def test_battery_f_stored_rows_outside_or_off_session_fail_closed():
    stored = {**stored_of(series()), **stored_of(series(D(2026, 7, 27), D(2026, 7, 31)))}
    assert V(series(), stored).reason.startswith("stored_outside_provider_range: 5")
    stored = {**stored_of(series()), **stored_of([bar(D(2026, 9, 7))])}   # Labor Day
    assert V(series(), stored).reason == "stored_off_session: 1 first=2026-09-07"


def test_battery_g_weekends_and_holidays_at_both_ends():
    labor_day, good_friday = D(2026, 9, 7), D(2026, 4, 3)
    assert V(series(D(2026, 8, 3)), start=D(2026, 8, 1)).status == v.VERDICT_LOAD
    assert V(series(D(2026, 9, 8), END), start=labor_day).status == v.VERDICT_LOAD
    assert V(series(START, D(2026, 9, 4)), end=labor_day).status == v.VERDICT_LOAD
    assert V(series(D(2026, 3, 2), D(2026, 4, 2)), start=D(2026, 3, 2),
             end=good_friday).status == v.VERDICT_LOAD
    assert V(series(D(2026, 8, 4)), start=START).reason == "sessions_missing=1 first=2026-08-03"
    assert V(series(START, D(2026, 10, 8))).reason == "sessions_missing=1 first=2026-10-09"
    # A Monday startDate whose first bar is that Friday skipped four sessions.
    assert V(series(D(2026, 8, 7))).reason == "sessions_missing=4 first=2026-08-03"


def test_battery_g_calendar_reaches_the_oldest_histories_and_never_clamps():
    assert CAL.sessions(D(1974, 7, 26), D(1974, 7, 28)) == {D(1974, 7, 26)}   # SONY
    assert len(CAL.sessions(D(1982, 1, 4), D(1982, 1, 8))) == 5               # NVO
    assert CAL.sessions(D(1985, 9, 27), D(1985, 9, 27)) == frozenset()        # Hurricane Gloria
    old = [bar(d) for d in sessions(D(1974, 7, 26), D(1974, 8, 30))]
    assert V(old, start=D(1974, 7, 26), end=D(1974, 8, 30)).status == v.VERDICT_LOAD
    for first in (D(1899, 12, 1), D(1969, 12, 31)):
        with pytest.raises(ValueError, match="outside XNYS"):
            CAL.sessions(first, D(1970, 1, 5))


def test_evidenced_gaps_are_an_explicit_input_and_empty_by_default():
    hole = D(2026, 9, 15)
    assert V(series(skip={hole})).reason == "sessions_missing=1 first=2026-09-15"
    assert V(series(skip={hole}), gaps=frozenset({hole})).status == v.VERDICT_LOAD


def test_the_verdict_names_the_snapshot_it_judged():
    stored = stored_of(series(D(2026, 9, 1)))
    verdict = V(series(), stored)
    assert verdict.digest == v.stored_digest(stored)
    assert v.STORED_FIELDS == ("open", "high", "low", "close", "volume", "adj_open", "adj_high",
                               "adj_low", "adj_close", "adj_volume", "div_cash", "split_factor")
    for col in v.STORED_FIELDS:                       # the whole stored row, every column
        changed = {**stored, END: {**stored[END], col: stored[END][col] + 0.5}}
        assert v.stored_digest(changed) != verdict.digest, col


# Gate 4: retained stored bars, numeric safety, exact edges --------------------------
FLAT = {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1000,
        "adjOpen": 100.0, "adjHigh": 100.0, "adjLow": 100.0, "adjClose": 100.0,
        "adjVolume": 1000.0, "divCash": 0.0, "splitFactor": 1.0}


def one(day=END, **over):
    return {"date": day.isoformat(), **FLAT, **over}


def V1(provider_bar, stored_row=None):
    stored = {END: stored_row} if stored_row is not None else {}
    return v.validate_series("X", (END, END), [provider_bar], stored, CAL)


@pytest.mark.parametrize(("col", "value", "problem"), [
    ("high", 99.99995, "ohlc_order"),           # gate: within 1e-6 of the provider, incoherent
    ("close", math.inf, "non_numeric_close"), ("adj_close", math.inf, "non_numeric_adjClose"),
    ("high", math.inf, "non_numeric_high"), ("open", math.nan, "non_numeric_open"),
    ("volume", -1, "negative"), ("adj_volume", -1.0, "negative"), ("div_cash", -0.5, "negative"),
    ("split_factor", 0.0, "non_positive"), ("adj_low", None, "non_numeric_adjLow"),
    ("close", 2e7, "value_out_of_bounds"), ("volume", 10**14, "value_out_of_bounds"),
])
def test_gate4_every_retained_stored_bar_is_validated(col, value, problem):
    verdict = V1(one(), {**stored_of([one()])[END], col: value})
    nothing_written(verdict, v.VERDICT_CONFLICT)
    assert verdict.reason == f"stored_bar_invalid: {problem} on {END}"


def test_gate4_extreme_magnitudes_never_reach_a_float_quotient():
    gate = one(open=1e-308, high=1e-308, low=1e-308, close=1e-308,
               adjOpen=1e307, adjHigh=1e308, adjLow=1e307, adjClose=1e308)
    assert V1(gate).reason == "unusable_bar: value_out_of_bounds at index 0"
    # in bounds, but adj/raw overflows a float: only exact arithmetic sees the 10x
    overflow = one(open=1e-308, high=1e-308, low=1e-308, close=1e-308,
                   adjOpen=1e6, adjHigh=1e7, adjLow=1e6, adjClose=1e7)
    assert V1(overflow).reason == "unusable_bar: adjustment_factor at index 0"
    coherent = one(open=1e-308, high=1e-308, low=1e-308, close=1e-308,
                   adjOpen=1e7, adjHigh=1e7, adjLow=1e7, adjClose=1e7)
    assert V1(coherent).status == v.VERDICT_LOAD
    assert V1(one(volume=10**400)).reason == "unusable_bar: non_numeric_volume at index 0"


@pytest.mark.parametrize(("over", "ok"), [
    ({k: 1e7 for k in v._PRICES}, True),
    ({k: math.nextafter(1e7, math.inf) for k in v._PRICES}, False),
    ({"volume": 10**13, "adjVolume": 1e13}, True), ({"volume": 10**13 + 1}, False),
    ({"adjVolume": math.nextafter(1e13, math.inf)}, False),
    ({"divCash": 1e7}, True), ({"divCash": math.nextafter(1e7, math.inf)}, False),
    ({"splitFactor": 1e4}, True), ({"splitFactor": math.nextafter(1e4, math.inf)}, False),
])
def test_gate4_magnitude_bounds_are_inclusive(over, ok):
    verdict = V1(one(**over))
    if ok:
        assert verdict.status == v.VERDICT_LOAD
    else:
        assert verdict.reason == "unusable_bar: value_out_of_bounds at index 0"


@pytest.mark.parametrize(("adj_open", "status"), [
    (101.0, v.VERDICT_LOAD), (math.nextafter(101.0, 0), v.VERDICT_LOAD),
    (math.nextafter(101.0, math.inf), v.VERDICT_INCOMPLETE),
    (99.0, v.VERDICT_LOAD), (math.nextafter(99.0, 0), v.VERDICT_INCOMPLETE),
])
def test_gate4_factor_tolerance_edge_is_exact(adj_open, status):
    """|adjOpen/open - adjClose/close| == 1% of adjClose/close is accepted; one
    ulp beyond is not."""
    bar_ = one(high=200.0, low=50.0, adjHigh=200.0, adjLow=50.0, adjOpen=adj_open)
    assert V1(bar_).status == status


@pytest.mark.parametrize(("stored_close", "status"), [
    (999999.0, v.VERDICT_COMPLETE), (math.nextafter(999999.0, math.inf), v.VERDICT_COMPLETE),
    (math.nextafter(999999.0, 0), v.VERDICT_CONFLICT),
])
def test_gate4_stored_tolerance_edge_is_exact(stored_close, status):
    """|stored - provider| == 1e-6 * max(|stored|, |provider|) is accepted."""
    provider = one(open=1e6, high=2e6, low=5e5, close=1e6,
                   adjOpen=1e6, adjHigh=2e6, adjLow=5e5, adjClose=1e6)
    stored = {**stored_of([provider])[END], "close": stored_close}
    assert V1(provider, stored).status == status


def test_preview_classification():
    assert w.classify_history_task(w.HistoryTask("X", None, None)) == "new"
    assert w.classify_history_task(w.HistoryTask("X", None, D(2024, 6, 11))) == "existing"


# ──────────────────────────────────────────────────────────────────────────────
# Ring path value filter (build_eod_rows) and retry backoff
# ──────────────────────────────────────────────────────────────────────────────
GOOD_BAR = {"date": "2026-10-09T00:00:00.000Z", "open": 1.0, "high": 1.0, "low": 1.0,
            "close": 1.0, "volume": 10, "adjOpen": 1.0, "adjHigh": 1.0, "adjLow": 1.0,
            "adjClose": 1.0, "adjVolume": 10, "divCash": 0.0, "splitFactor": 1.0}


@pytest.mark.parametrize("element", [
    None, "bad", 7, ["x"], {**GOOD_BAR, "date": None}, {**GOOD_BAR, "date": "not-a-date"},
    {**GOOD_BAR, "close": "1.0"}, {**GOOD_BAR, "volume": True}, {**GOOD_BAR, "adjClose": None},
    {**GOOD_BAR, "close": float("nan")}, {**GOOD_BAR, "close": "N/A"},
    {**GOOD_BAR, "high": float("inf")}, {**GOOD_BAR, "low": -1.0}, {**GOOD_BAR, "volume": -5},
    {**GOOD_BAR, "splitFactor": 0.0},
])
def test_ring_rows_drop_every_non_conforming_element(element):
    assert w.build_eod_rows("X", [GOOD_BAR, element]) == w.build_eod_rows("X", [GOOD_BAR])


def test_retry_backoff_doubles_and_is_capped():
    now = dt.datetime(2026, 10, 10, tzinfo=dt.UTC)
    hours = [(w.retry_after(now, n) - now) / dt.timedelta(hours=1) for n in range(1, 7)]
    assert hours == [12, 24, 48, 96, 168, 168]


def test_completion_is_written_only_by_promote():
    """Static half of the single-promotion-path rule (the dynamic half runs the
    real entrypoint against a database): the history insert statement and the
    history_complete write appear in exactly one function, promote()."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(w))
    users: dict[str, set[str]] = {"EOD_HISTORY_INSERT_SQL": set(), "STATUS_COMPLETE": set()}
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef):
            for node in ast.walk(fn):
                if isinstance(node, ast.Name) and node.id in users:
                    users[node.id].add(fn.name)
    assert users["EOD_HISTORY_INSERT_SQL"] == {"promote"}
    # record_ticker_status only to refuse it; the plan only to read it.
    assert users["STATUS_COMPLETE"] == {"promote", "record_ticker_status", "plan_foreign_history"}
    with pytest.raises(ValueError, match="only by promote"):
        w.record_ticker_status(None, "X", w.STATUS_COMPLETE)
    with pytest.raises(ValueError, match="certifying verdict"):
        w.promote(None, "X", v.Verdict(v.VERDICT_INCOMPLETE, "x"), through=END, history_start=START)


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
