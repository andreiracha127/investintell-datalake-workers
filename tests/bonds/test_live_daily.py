"""DB-free tests for the live daily feed's transforms (src/bonds/live_daily.py).

No connection, no network, no clock: every function here is total over provider
payloads, so the whole suite runs in the no-DSN CI job.

What is actually being pinned:
  * UNITS -- the one place a 100x seam can enter (yield percent -> fraction);
  * ABSENCE -- an unreported value must stay absent, never become 0;
  * the accrued correction inside the yield solve, which is the difference
    between a right answer and a plausible wrong one;
  * the empirical bid/ask side semantics and the refusal to invent a spread on a
    one-sided day;
  * determinism of the tick cohort and of same-day supersession;
  * the WINDOW -- both ends of it, on both the candle and the curve fold. The
    upper bound is the one that carries blast radius: ``max(day)`` of the
    observation table anchors the as-of of both downstream publications, so a
    candle dated past the request would date the product into the future and jam
    every later publication as a regression; and on the curve, whose response is
    the provider's whole history rather than a windowed request, it is what
    keeps a replay from advancing the table past the day it asked for.
"""
from __future__ import annotations

import datetime as _dt

import pytest

from src.bonds import live_daily


# --------------------------------------------------------------------------- #
# Yield solving
# --------------------------------------------------------------------------- #
def test_solved_yield_round_trips_the_pricing_identity() -> None:
    """A price generated AT a known yield must solve back to that yield.

    The generator is the same dirty-price identity the solver inverts, so this
    is a true round trip rather than a comparison against a hand-typed constant.
    """
    coupon, periods, frac = 5.0, 10.0, 1.0
    for expected in (0.01, 0.0425, 0.09, 0.25):
        dirty = live_daily._bullet_dirty_price(expected, coupon, periods, frac)
        clean = dirty - (coupon / 2.0) * (1.0 - frac)
        solved = live_daily.analytical_ytm(clean, coupon, periods / 2.0)
        assert solved is not None
        assert solved == pytest.approx(expected, abs=1e-6)


def test_accrued_is_added_before_solving_not_after() -> None:
    """Mid-period, ignoring accrued biases the solved yield -- measurably.

    A high-coupon bond halfway through its coupon period accrues ~half a
    coupon. Solving the SAME clean price with and without that correction has to
    land on different yields, or the correction is not being applied at all.
    """
    coupon, periods, frac = 8.0, 6.0, 0.5
    true_yield = 0.05
    dirty = live_daily._bullet_dirty_price(true_yield, coupon, periods, frac)
    clean = dirty - (coupon / 2.0) * (1.0 - frac)

    corrected = live_daily.analytical_ytm(clean, coupon, (periods - 1.0 + frac) / 2.0)
    assert corrected is not None
    assert corrected == pytest.approx(true_yield, abs=1e-4)

    # What the naive solve would have produced: the clean price treated as dirty.
    naive_lo, naive_hi = -0.5, 5.0
    for _ in range(80):
        mid = (naive_lo + naive_hi) / 2.0
        if live_daily._bullet_dirty_price(mid, coupon, periods, frac) > clean:
            naive_lo = mid
        else:
            naive_hi = mid
    naive = (naive_lo + naive_hi) / 2.0
    assert abs(naive - true_yield) > 1e-3, "the accrued correction would be a no-op"


@pytest.mark.parametrize(
    "price, coupon, years",
    [
        (None, 5.0, 4.0),          # no price
        (98.0, None, 4.0),         # no coupon
        (98.0, 5.0, None),         # no maturity
        (98.0, 5.0, 0.0),          # matured
        (98.0, 5.0, -1.0),         # past maturity
        (0.0, 5.0, 4.0),           # non-positive price
        (float("nan"), 5.0, 4.0),  # non-finite
    ],
)
def test_unsupportable_inputs_yield_absence_never_a_number(price, coupon, years) -> None:
    assert live_daily.analytical_ytm(price, coupon, years) is None


def test_a_price_outside_the_bracket_is_refused_not_clamped() -> None:
    """A price the bracket cannot reach returns None.

    Clamping to an endpoint would publish -50% or +500% as if it had been
    solved -- a fabricated yield wearing a solved yield's clothes.

    A cent for a 30-year 1% bond implies a yield far above the bracket's 500%
    ceiling; the bracket is deliberately wide (a distressed bond in the governed
    history reaches ~390%), so the refusal has to be probed beyond it.
    """
    assert live_daily.analytical_ytm(0.01, 1.0, 30.0) is None
    # ... and the solver still answers INSIDE the bracket, including deeply
    # negative yields, rather than refusing anything unusual.
    assert live_daily.analytical_ytm(10_000.0, 1.0, 30.0) is not None


# --------------------------------------------------------------------------- #
# Candles
# --------------------------------------------------------------------------- #
DAY_1 = _dt.date(2026, 8, 5)
DAY_2 = _dt.date(2026, 8, 6)


def _candles(**overrides):
    payload = {
        "s": "ok",
        "t": [live_daily.to_epoch(DAY_1), live_daily.to_epoch(DAY_2)],
        "c": [98.25, 98.50],
        "y": [4.058191, 4.040360],
    }
    payload.update(overrides)
    return payload


def test_reported_yield_is_converted_from_percent_to_fraction() -> None:
    rows = live_daily.candle_rows("912828XX1", _candles()).rows
    assert [r.day for r in rows] == [DAY_1, DAY_2]
    assert rows[0].price == pytest.approx(98.25)
    # The single unit boundary in the whole feed.
    assert rows[0].ytm == pytest.approx(0.04058191)
    assert rows[0].ytm_basis == live_daily.BASIS_REPORTED
    assert rows[0].source == "finnhub" and rows[0].source_rank == 1
    assert rows[0].price_type == "trade" and rows[0].accrued == "clean"


def test_volume_is_absent_not_zero() -> None:
    """The provider sends no volume; a 0 would read as "traded nothing"."""
    rows = live_daily.candle_rows("912828XX1", _candles()).rows
    assert all(row.volume is None for row in rows)


def test_a_day_without_a_reported_yield_is_solved_and_labelled_computed() -> None:
    rows = live_daily.candle_rows(
        "912828XX1", _candles(y=[None, None]),
        coupon_pct=4.0, maturity_date=_dt.date(2031, 8, 6),
    ).rows
    assert [r.ytm_basis for r in rows] == [live_daily.BASIS_COMPUTED] * 2
    assert all(0.0 < r.ytm < 0.20 for r in rows)


def test_a_day_whose_yield_cannot_be_solved_still_keeps_its_price() -> None:
    """Dropping the row would punch a hole in the PRICE series to protect a yield."""
    rows = live_daily.candle_rows("912828XX1", _candles(y=[None, None])).rows
    assert len(rows) == 2
    assert all(r.ytm is None and r.ytm_basis is None for r in rows)
    assert all(r.price is not None for r in rows)


def test_days_without_a_usable_price_are_dropped() -> None:
    fold = live_daily.candle_rows("912828XX1", _candles(c=[None, 0.0]))
    assert fold.rows == []
    # Dropped for want of a price, not for falling outside the window.
    assert fold.dropped_after_window == 0


def test_not_before_bounds_the_delta_inclusively() -> None:
    fold = live_daily.candle_rows("912828XX1", _candles(), not_before=DAY_2)
    assert [r.day for r in fold.rows] == [DAY_2]
    # The lower side is expected provider over-return, so it stays SILENT.
    assert fold.dropped_after_window == 0


# --------------------------------------------------------------------------- #
# The upper bound of the requested window.
#
# ``max(day)`` of ``bond_observation_daily`` is the as-of anchor of BOTH
# downstream publications, so a single future-dated candle does not merely add a
# bad row: it dates the product into the future and then makes every legitimate
# later publication look like an as-of regression, jamming the chain until
# someone deletes the row by hand in production. These pin the refusal.
# --------------------------------------------------------------------------- #
FUTURE_DAY = _dt.date(2027, 1, 4)


def _candles_with_a_future_element():
    """An otherwise-good response carrying one future-dated element."""
    return _candles(
        t=[live_daily.to_epoch(DAY_1), live_daily.to_epoch(DAY_2),
           live_daily.to_epoch(FUTURE_DAY)],
        c=[98.25, 98.50, 97.00],
        y=[4.058191, 4.040360, 4.200000],
    )


def test_a_candle_past_the_requested_window_cannot_move_the_anchor() -> None:
    """The thread's case: a good response with one future-dated element.

    Before the bound existed this returned three rows and ``max(day)`` was
    2027-01-04 -- the value both publications would have anchored on.
    """
    fold = live_daily.candle_rows(
        "912828XX1", _candles_with_a_future_element(),
        not_before=DAY_1, not_after=DAY_2,
    )
    assert [r.day for r in fold.rows] == [DAY_1, DAY_2]
    assert max(r.day for r in fold.rows) == DAY_2
    assert FUTURE_DAY not in {r.day for r in fold.rows}
    # ... and the refusal is REPORTED, not silent: a provider that starts
    # emitting garbage has to show up in the stage report on the day it starts.
    assert fold.dropped_after_window == 1


def test_the_last_day_of_the_window_is_kept_the_bound_is_inclusive() -> None:
    """``day == not_after`` is the day a daily run exists to collect."""
    fold = live_daily.candle_rows(
        "912828XX1", _candles(), not_before=DAY_1, not_after=DAY_2
    )
    assert [r.day for r in fold.rows] == [DAY_1, DAY_2]
    assert fold.dropped_after_window == 0


def test_an_unbounded_call_still_keeps_everything() -> None:
    """``not_after=None`` is unbounded, symmetric with ``not_before``."""
    fold = live_daily.candle_rows("912828XX1", _candles_with_a_future_element())
    assert [r.day for r in fold.rows] == [DAY_1, DAY_2, FUTURE_DAY]
    assert fold.dropped_after_window == 0


def test_a_future_candle_is_refused_before_its_price_is_even_looked_at() -> None:
    """Out-of-window is decided on the DAY, whatever the element carries.

    Ordering the price check first would let a future candle with a null price
    be dropped as "no price" and vanish from the count -- the provider's clock
    going wrong would then be invisible until the day it also sent a price.
    """
    payload = _candles(
        t=[live_daily.to_epoch(DAY_1), live_daily.to_epoch(FUTURE_DAY)],
        c=[98.25, None], y=[4.0, None],
    )
    fold = live_daily.candle_rows("912828XX1", payload, not_after=DAY_1)
    assert [r.day for r in fold.rows] == [DAY_1]
    assert fold.dropped_after_window == 1


def test_a_repeated_day_in_one_payload_supersedes_rather_than_conflicts() -> None:
    """Two rows for one day would break the single-statement upsert."""
    payload = _candles(
        t=[live_daily.to_epoch(DAY_1), live_daily.to_epoch(DAY_1)],
        c=[98.25, 99.75], y=[4.0, 3.9],
    )
    rows = live_daily.candle_rows("912828XX1", payload).rows
    assert len(rows) == 1
    assert rows[0].price == pytest.approx(99.75)  # last element wins


def test_a_no_data_payload_is_empty_not_an_error() -> None:
    assert live_daily.candle_rows("912828XX1", {"s": "no_data"}).rows == []
    assert live_daily.candle_rows("912828XX1", None).rows == []


def test_the_row_tuple_matches_the_declared_column_protocol() -> None:
    rows = live_daily.candle_rows("912828XX1", _candles()).rows
    assert len(rows[0].as_tuple()) == len(live_daily.OBSERVATION_COLUMNS)


def test_the_day_is_the_utc_calendar_day_of_the_stamp() -> None:
    assert live_daily.day_from_timestamp(live_daily.to_epoch(DAY_2)) == DAY_2
    # Late in the UTC day still resolves to that same day, never the next one.
    assert live_daily.day_from_timestamp(live_daily.to_epoch(DAY_2) + 86_399) == DAY_2


# --------------------------------------------------------------------------- #
# Yield curve
# --------------------------------------------------------------------------- #
def test_curve_points_keep_percent_and_trim_to_the_watermark() -> None:
    payload = {"code": "10y", "data": [
        {"d": "2026-08-06", "v": 4.69},
        {"d": "2026-08-05", "v": 4.66},
        {"d": "2026-08-04", "v": 4.61},
    ]}
    points = live_daily.curve_points("10y", payload, not_before=_dt.date(2026, 8, 5))
    assert [(p.day, p.yield_pct) for p in points] == [
        (_dt.date(2026, 8, 5), 4.66), (_dt.date(2026, 8, 6), 4.69),
    ]
    # A curve tenor stays in PERCENT -- deliberately not the ytm convention.
    assert points[-1].yield_pct == 4.69
    assert len(points[0].as_tuple()) == len(live_daily.CURVE_COLUMNS)


def test_curve_points_are_bounded_above_by_the_requested_day() -> None:
    """A replay must load its session, not every session the provider has since.

    The curve is the one lane the provider is never asked a WINDOW for: one call
    returns the tenor's whole history, and the fold is the only place a ceiling
    can be applied. Without it a replay of an old day upserts every later point
    too, so ``bond_yield_curve_daily`` advances past the day being replayed with
    rates from sessions the replay is not loading.

    Before the bound existed this returned all four points and the worker
    reported ``latest_day`` as 2026-08-06 on a run whose prices stopped in May.
    """
    replay = _dt.date(2026, 5, 15)
    payload = {"data": [
        {"d": "2026-05-14", "v": 4.40},
        {"d": "2026-05-15", "v": 4.42},
        {"d": "2026-06-15", "v": 4.71},   # real sessions -- just not this run's
        {"d": "2026-08-06", "v": 4.69},
    ]}
    points = live_daily.curve_points("10y", payload, not_after=replay)
    assert [p.day.isoformat() for p in points] == ["2026-05-14", "2026-05-15"]
    # The requested day itself is KEPT: it is the day the run exists to load.
    assert points[-1].day == replay


def test_the_curve_bound_is_optional_and_composes_with_the_watermark() -> None:
    """``not_after=None`` is unbounded, symmetric with ``not_before``.

    And the two bounds are applied together rather than either/or: the watermark
    opens the window from below (so a lagging tenor recovers its gap) while the
    requested date closes it from above.
    """
    payload = {"data": [
        {"d": "2026-08-03", "v": 4.60}, {"d": "2026-08-05", "v": 4.66},
        {"d": "2026-08-07", "v": 4.70},
    ]}
    assert len(live_daily.curve_points("10y", payload)) == 3
    both = live_daily.curve_points(
        "10y", payload,
        not_before=_dt.date(2026, 8, 5), not_after=_dt.date(2026, 8, 5),
    )
    assert [p.day.isoformat() for p in both] == ["2026-08-05"]
    # An INVERTED window (a tenor whose watermark is already past the replay
    # date) yields nothing rather than raising -- a real state on a replay.
    assert live_daily.curve_points(
        "10y", payload,
        not_before=_dt.date(2026, 8, 7), not_after=_dt.date(2026, 8, 5),
    ) == []


def test_malformed_curve_items_are_skipped_not_fatal() -> None:
    payload = {"data": [{"d": "nope", "v": 1.0}, {"d": "2026-08-06"}, "junk",
                        {"d": "2026-08-06", "v": 4.69}]}
    points = live_daily.curve_points("10y", payload)
    assert [(p.day, p.yield_pct) for p in points] == [(_dt.date(2026, 8, 6), 4.69)]


# --------------------------------------------------------------------------- #
# Ticks
# --------------------------------------------------------------------------- #
def test_side_two_is_the_ask_and_the_spread_is_relative_bps() -> None:
    ticks = {
        "t": [1, 2, 3, 4],
        "p": [99.0, 101.0, 99.0, 101.0],
        "v": [1000, 2000, 1000, 2000],
        "si": [1, 2, 1, 2],
        "y": [4.0, 4.1, 4.0, 4.1],
    }
    agg = live_daily.aggregate_ticks("912828XX1", DAY_2, ticks)
    assert agg is not None
    assert agg.bid_price_median == pytest.approx(99.0)
    assert agg.ask_price_median == pytest.approx(101.0)
    assert agg.bid_ask_bps == pytest.approx((101.0 - 99.0) / 100.0 * 10_000.0)
    assert agg.trade_count == 4
    assert agg.par_volume == pytest.approx(6000.0)
    assert len(agg.as_tuple()) == len(live_daily.TICK_COLUMNS)


def test_a_one_sided_day_reports_no_spread_rather_than_a_zero() -> None:
    ticks = {"t": [1, 2], "p": [99.0, 99.5], "si": [1, 1], "v": [10, 10]}
    agg = live_daily.aggregate_ticks("912828XX1", DAY_2, ticks)
    assert agg is not None
    assert agg.bid_ask_bps is None, "a fabricated 0 would read as a frictionless bond"
    assert agg.ask_price_median is None


def test_crossing_medians_are_kept_not_clipped() -> None:
    ticks = {"t": [1, 2], "p": [101.0, 99.0], "si": [1, 2], "v": [10, 10]}
    agg = live_daily.aggregate_ticks("912828XX1", DAY_2, ticks)
    assert agg is not None and agg.bid_ask_bps < 0


def test_an_untraded_day_produces_no_row_at_all() -> None:
    assert live_daily.aggregate_ticks("912828XX1", DAY_2, {"t": []}) is None
    assert live_daily.aggregate_ticks("912828XX1", DAY_2, None) is None
    # Prices the tape did not price are not trades.
    assert live_daily.aggregate_ticks(
        "912828XX1", DAY_2, {"t": [1], "p": [None]}
    ) is None


def test_absent_volume_stays_absent() -> None:
    agg = live_daily.aggregate_ticks(
        "912828XX1", DAY_2, {"t": [1], "p": [99.0], "si": [1]}
    )
    assert agg is not None and agg.par_volume is None


# --------------------------------------------------------------------------- #
# Window + cohort arithmetic
# --------------------------------------------------------------------------- #
def test_the_window_re_reads_the_watermark_day() -> None:
    """A revised close must be picked up, not frozen at its first print.

    The window opens the day BEFORE the watermark because the provider's
    ``from`` is EXCLUSIVE. Measured 2026-08-18 against the live API, same ``to``,
    only ``from`` varied, for a bond whose last point is 2026-08-17:

        from=2026-08-17 -> {"s":"no_data"}
        from=2026-08-01 -> {"s":"ok", ... t[-1]=2026-08-17}

    An inclusive ``from`` was the standing assumption, and it made the watermark
    day unreachable: once the lane caught up, every window it asked for started
    on the one day the provider would never return, so EVERY bond came back
    empty. That is what dressed a caught-up lane as ``candles_failed`` -- whose
    own premise is that a re-read of an already-loaded day must come back -- and
    aborted the run, leaving the panel deferred. Opening one day earlier puts the
    watermark day back INSIDE the window, so the re-read works and the failure
    signal means what it says again.
    """
    start, end = live_daily.fetch_window(_dt.date(2026, 8, 5), _dt.date(2026, 8, 7))
    assert (start, end) == (_dt.date(2026, 8, 4), _dt.date(2026, 8, 7))


def test_a_caught_up_lane_still_asks_for_a_day_the_provider_can_answer() -> None:
    """The regression that produced the incident, pinned as its own case.

    Watermark == today is the caught-up state a healthy daily lane sits in for
    most of every day. The window must still open before it, or the request is
    the empty one by construction.
    """
    today = _dt.date(2026, 8, 17)
    start, end = live_daily.fetch_window(today, today)
    assert start < today, "a caught-up lane would ask the provider for nothing"
    assert end == today


def test_a_cold_table_asks_for_a_small_window_not_the_whole_history() -> None:
    start, end = live_daily.fetch_window(None, _dt.date(2026, 8, 7), cold_start_days=30)
    assert (end - start).days == 30


def test_a_watermark_ahead_of_today_never_inverts_the_window() -> None:
    start, end = live_daily.fetch_window(_dt.date(2026, 9, 1), _dt.date(2026, 8, 7))
    assert start == end == _dt.date(2026, 8, 7)


def test_the_window_never_asks_past_the_execution_date() -> None:
    """``to`` is ALWAYS today -- the second barrier behind ``not_after``.

    ``candle_rows`` is clock-free by design (it runs in the no-DSN CI job), so
    its upper bound can only be as good as what the caller passes. What makes
    ``not_after=to`` an EXECUTION-DATE ceiling rather than just a parameter is
    this invariant: no watermark, past, present, future, or absent, can make
    ``fetch_window`` ask for a day beyond ``today``. Pinned here so an edit that
    widens ``to`` -- "ask a day ahead, the provider clips anyway" -- fails
    instead of quietly re-opening the future-day hole.
    """
    today = _dt.date(2026, 8, 7)
    watermarks = [
        None,                      # cold table
        _dt.date(2020, 1, 1),      # far behind
        today - _dt.timedelta(days=1),
        today,                     # caught up
        _dt.date(2027, 6, 30),     # already ahead of today
    ]
    for watermark in watermarks:
        start, end = live_daily.fetch_window(watermark, today)
        assert end == today, watermark
        assert start <= end, watermark


def test_previous_business_day_skips_the_weekend() -> None:
    assert live_daily.previous_business_day(_dt.date(2026, 8, 10)) == _dt.date(2026, 8, 7)
    assert live_daily.previous_business_day(_dt.date(2026, 8, 7)) == _dt.date(2026, 8, 6)


def test_the_tick_cohort_is_deterministic_on_ties() -> None:
    rows = [("BBB000001", 10), ("AAA000001", 10), ("CCC000001", 99)]
    assert live_daily.rank_by_activity(rows, 3) == [
        "CCC000001", "AAA000001", "BBB000001",
    ]
    assert live_daily.rank_by_activity(rows, 1) == ["CCC000001"]
    assert live_daily.rank_by_activity(rows, 0) == []
