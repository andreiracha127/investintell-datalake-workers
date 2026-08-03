"""Formula-level acceptance for the open_macro v4.0-rev engine (M-COMP4).

These tests exercise the three layers in isolation: the dial-0 identity, the
inertness of ALERT inside `contained`, freshness measured in native publication
periods, the mandate invariants, and the non-idempotence of the barbell.

The end-to-end reproduction of the signed ledger lives in
``test_open_macro_v4_golden.py``.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from harness.direct_activation import credit_guard as guard
from harness.phase0q import book_router as books
from harness.phase0q import sleeve as pinned_sleeve
from harness.phase0q import v4_replay as replay
from src import fiscal_state as fiscal


# --------------------------------------------------------------------------- #
# 1. The dial-0 identity                                                       #
# --------------------------------------------------------------------------- #

def test_amplitude_zero_compression_is_the_center_exactly() -> None:
    """`compressed_0(q) == CENTER` for all four quadrants, to the last bit.

    This is the whole v4 amendment stated as arithmetic: inside `contained` the
    quadrant is a published diagnostic and allocates nothing. Not "close to" the
    center — the SAME float, otherwise a quadrant flip would still move money."""
    for quadrant in books.QUADRANTS:
        book = books.compressed(quadrant, books.CONTAINED_AMPLITUDE)
        assert book == books.CENTER, f"compressed_0({quadrant}) drifted from CENTER"
        for ticker in books.UNIVERSE:
            assert book[ticker] - books.CENTER[ticker] == 0.0


def test_amplitude_zero_book_ids_still_record_the_quadrant_that_was_read() -> None:
    """The weights collapse; the AUDIT TRAIL must not. Four different ids, one book."""
    ids = set()
    for quadrant in books.QUADRANTS:
        book, book_id = books.l2_book("contained", quadrant)
        ids.add(book_id)
        assert book == books.CENTER
    assert ids == {f"compressed_0({q})" for q in books.QUADRANTS}


def test_center_is_the_cross_quadrant_mean_of_the_pinned_baselines() -> None:
    """The CENTER is DERIVED from the hash-pinned sleeve, never restated by hand."""
    for ticker in books.CORE_TICKERS:
        expected = sum(
            pinned_sleeve.PER_QUADRANT_BASELINE_WEIGHTS[
                pinned_sleeve.QUADRANT_TO_KEY[q]].get(ticker, 0.0)
            for q in books.QUADRANTS) / 4.0
        assert books.CENTER[ticker] == expected
    for ticker in books.CREDIT_TICKERS:
        assert books.CENTER[ticker] == 0.0


# --------------------------------------------------------------------------- #
# 2. ALERT inside `contained` is inert                                         #
# --------------------------------------------------------------------------- #

def test_alert_inside_contained_returns_the_center_exactly() -> None:
    """Under amplitude 0 the ALERT blend is `blend(CENTER, CENTER)` — the identity.

    Max |delta| must be exactly 0.0. The guard is armed and the portfolio does not
    move; the formulation freeze states that in normative text because reading
    "degrades to ALERT" as "the portfolio is defended" is the available mistake."""
    for quadrant in (*books.QUADRANTS, None):
        off_book, _ = books.compose("contained", "off", quadrant)
        alert_book, alert_id = books.compose("contained", "alert", quadrant)
        assert alert_id.startswith("alert50:")
        deltas = [abs(alert_book[t] - books.CENTER[t]) for t in books.UNIVERSE]
        assert max(deltas) == 0.0
        assert alert_book == off_book == books.CENTER


def test_alert_outside_contained_does_move_the_book() -> None:
    """The inertness is a property of amplitude 0 in `contained`, not of ALERT."""
    off_book, _ = books.compose("dominance", "off", None)
    alert_book, _ = books.compose("dominance", "alert", None)
    assert max(abs(alert_book[t] - off_book[t]) for t in books.UNIVERSE) > 0.0
    for ticker in books.UNIVERSE:
        assert alert_book[ticker] == pytest.approx(
            0.5 * off_book[ticker] + 0.5 * books.CENTER[ticker], abs=0.0, rel=0.0)


# --------------------------------------------------------------------------- #
# 3. Freshness in NATIVE publication periods                                   #
# --------------------------------------------------------------------------- #

CALIBRATION_MOMENT = pd.Timestamp("2026-08-03")
CALIBRATION_LAST_OBS = pd.Timestamp("2026-04-01")


def test_same_label_age_gives_opposite_freshness_verdicts() -> None:
    """The two pinned calibration points, asserted against each other.

    Both arms carry a 2026-04 observation and both are read on 2026-08-03. M2SL is
    monthly with a 1-month lag, so the 2026-05 print was due 2026-06-30 and the
    2026-06 print was due 2026-07-31: two deadlines passed, nothing arrived, STALE.
    SUBLPDCILSLGNQ is quarterly with a 2-month lag, so the next period (2026-07) is
    not due until 2026-09-30: nothing is late, FRESH.

    Same label age, opposite verdicts. A "months old" threshold cannot produce this
    pair, which is why the rule is written in native periods."""
    m2 = guard.arm_freshness([CALIBRATION_LAST_OBS], CALIBRATION_MOMENT, guard.M2_CLOCK)
    sloos = guard.arm_freshness([CALIBRATION_LAST_OBS], CALIBRATION_MOMENT,
                                guard.SLOOS_CLOCK)

    assert m2.missing_periods == 2
    assert m2.stale is True
    assert sloos.missing_periods == 0
    assert sloos.stale is False

    assert m2.last_observation == sloos.last_observation == CALIBRATION_LAST_OBS
    assert m2.stale is not sloos.stale


def test_the_two_deadlines_that_make_m2sl_stale_are_named() -> None:
    """Not just the count — WHICH releases are late, so the verdict is auditable."""
    assert guard.M2_CLOCK.deadline(pd.Timestamp("2026-05-01")) == pd.Timestamp("2026-06-30")
    assert guard.M2_CLOCK.deadline(pd.Timestamp("2026-06-01")) == pd.Timestamp("2026-07-31")
    assert guard.M2_CLOCK.deadline(pd.Timestamp("2026-07-01")) == pd.Timestamp("2026-08-31")
    # the third one has NOT passed on 2026-08-03, which is why the count is 2 and not 3
    assert guard.M2_CLOCK.deadline(pd.Timestamp("2026-07-01")) > CALIBRATION_MOMENT
    assert guard.SLOOS_CLOCK.deadline(pd.Timestamp("2026-07-01")) == pd.Timestamp("2026-09-30")
    assert guard.SLOOS_CLOCK.deadline(pd.Timestamp("2026-07-01")) > CALIBRATION_MOMENT


def test_a_source_with_no_visible_observation_is_stale_not_fresh() -> None:
    """Nothing to be late relative to is not the same as up to date."""
    verdict = guard.arm_freshness([pd.Timestamp("2026-04-01")],
                                  pd.Timestamp("1999-12-31"), guard.M2_CLOCK)
    assert verdict.last_observation is None
    assert verdict.stale is True
    with pytest.raises(ValueError):
        guard.missing_periods(None, CALIBRATION_MOMENT, guard.M2_CLOCK)


def test_coverage_tokens_name_which_arm_went_dark() -> None:
    assert guard.coverage_token(True, True) == guard.COVERAGE_FULL
    assert guard.coverage_token(False, True) == guard.COVERAGE_PARTIAL_A
    assert guard.coverage_token(True, False) == guard.COVERAGE_PARTIAL_B
    assert guard.coverage_token(False, False) == guard.COVERAGE_BLIND


def _guard_run(m2_raw: pd.Series, index: pd.DatetimeIndex,
               sloos_raw: pd.Series, l1: pd.DataFrame,
               stress: pd.Series) -> pd.DataFrame:
    return guard.build_guard(
        sloos_raw=sloos_raw, m2_raw=m2_raw, index=index,
        fiscal_state=l1["fiscal_state"], fiscal_state_age_m=l1["fiscal_state_age_m"],
        stress_confirmed=stress)


def test_stale_arm_b_raises_partial_b_while_the_book_does_not_move() -> None:
    """The token is emitted even though NOTHING in the portfolio changes.

    2016-01 is `contained` with arm A firing, so the guard is ALERT and — under
    amplitude 0 — the book is the CENTER. Stall M2SL two months and arm B goes
    stale: the coverage token must go `full` -> `partial_b` while the guard level
    AND the weights stay bit-identical.

    Asserted TOGETHER on purpose. The temptation is to suppress a token that does
    not move money; that is exactly the month where a silent loss of coverage would
    be invisible."""
    index = replay.decision_index()
    raw = {sid: replay.load_macro_series(sid) for sid in replay.MACRO_SERIES}
    l1 = fiscal.fiscal_panel(raw["MTSDS133FMS"], raw["GDP"], index)
    monthly = replay.month_end_prices(replay.load_price_frame())
    stress = guard.stress_confirmed_series(monthly["SPY"], index)

    target = pd.Timestamp("2016-01-31")
    m2_full = raw["M2SL"]
    m2_stalled = m2_full[m2_full.index <= pd.Timestamp("2015-11-01")]

    fresh = _guard_run(m2_full, index, raw["SUBLPDCILSLGNQ"], l1, stress)
    stalled = _guard_run(m2_stalled, index, raw["SUBLPDCILSLGNQ"], l1, stress)

    # the token moves ...
    assert fresh.at[target, "guard_coverage"] == guard.COVERAGE_FULL
    assert stalled.at[target, "guard_coverage"] == guard.COVERAGE_PARTIAL_B
    assert bool(fresh.at[target, "arm_b_fresh"]) is True
    assert bool(stalled.at[target, "arm_b_fresh"]) is False
    assert bool(stalled.at[target, "guard_blind"]) is False   # arm A still sees

    # ... and the guard level and the book do NOT.
    assert fresh.at[target, "guard_level"] == stalled.at[target, "guard_level"] == "alert"
    quadrant = replay.quadrant_series(
        replay.load_decision_chain(),
        replay.proxy_quadrant(raw["CFNAI"], raw["CPIAUCSL"], index),
        index).at[target, "quadrant"]
    fresh_book, fresh_id = books.compose(
        l1.at[target, "fiscal_state"], fresh.at[target, "guard_level"], quadrant)
    stalled_book, stalled_id = books.compose(
        l1.at[target, "fiscal_state"], stalled.at[target, "guard_level"], quadrant)
    assert fresh_id == stalled_id
    assert max(abs(fresh_book[t] - stalled_book[t]) for t in books.UNIVERSE) == 0.0
    assert fresh_book == books.CENTER


def test_a_stale_arm_votes_false_it_is_never_carried_forward() -> None:
    """A stale arm contributes False to the ALERT — not its last known reading."""
    index = pd.date_range("2019-01-31", "2021-12-31", freq="ME")
    clock = guard.M2_CLOCK
    dates = pd.date_range("2018-01-01", "2020-06-01", freq="MS")
    fresh_flags = guard.arm_fresh_series(dates, index, clock)
    # the series stops publishing after 2020-06; the July print is due 2020-08-31
    assert bool(fresh_flags.loc[pd.Timestamp("2020-07-31")]) is True
    assert bool(fresh_flags.loc[pd.Timestamp("2020-08-31")]) is False
    assert bool(fresh_flags.loc[pd.Timestamp("2021-12-31")]) is False


# --------------------------------------------------------------------------- #
# 7. Mandate invariants and the barbell trap                                    #
# --------------------------------------------------------------------------- #

def test_every_emitted_book_passes_the_invariants_without_enforcement() -> None:
    """The dissolution proof, part 1.

    Every book the signed configuration can emit — the CENTER, the four
    `compressed_0(q)` ids, the barbelled dominance book, the SEVERE book and every
    ALERT blend of those — satisfies sum == 1, risk <= 0.65 and defensive >= 0.20
    on its own arithmetic. Nothing is scaled toward a bound, nothing is dropped and
    nothing is renormalized to get there."""
    emitted = books.emitted_books()
    assert len(emitted) == 13
    for book_id, book in emitted.items():
        books.check_invariants(book, book_id)
        assert set(book) == set(books.UNIVERSE)
        assert sum(book.values()) == pytest.approx(1.0, abs=1e-9)
        assert sum(book[t] for t in books.RISK_ASSETS) <= books.RISK_CAP
        assert sum(book[t] for t in books.DEFENSIVE_ASSETS) >= books.DEFENSIVE_FLOOR


def _book(**weights: float) -> dict[str, float]:
    return {t: float(weights.get(t, 0.0)) for t in books.UNIVERSE}


def test_the_invariant_check_refuses_a_bad_book_it_does_not_repair_it() -> None:
    over_risk = _book(SPY=0.50, DBC=0.20, TLT=0.10, SHY=0.10, TIP=0.10)
    assert sum(over_risk.values()) == pytest.approx(1.0, abs=1e-12)
    with pytest.raises(books.BookInvariantError, match="risk cap"):
        books.check_invariants(over_risk, "over_risk")

    under_defensive = _book(SPY=0.40, DBC=0.20, GLD=0.25, TLT=0.05, SHY=0.05, TIP=0.05)
    assert sum(under_defensive.values()) == pytest.approx(1.0, abs=1e-12)
    with pytest.raises(books.BookInvariantError, match="defensive floor"):
        books.check_invariants(under_defensive, "under_defensive")

    unbalanced = dict(books.CENTER)
    unbalanced["SHY"] += 0.01
    with pytest.raises(books.BookInvariantError, match="sum"):
        books.check_invariants(unbalanced, "unbalanced")

    negative = dict(books.CENTER)
    negative["TLT"] -= 0.30
    negative["SPY"] += 0.30
    with pytest.raises(books.BookInvariantError, match="negative"):
        books.check_invariants(negative, "negative")


def test_pure_compression_equals_the_pinned_sleeve_book_at_lambda_50() -> None:
    """The dissolution proof, part 2: at λ = 0.5 the two constructions AGREE.

    `sleeve.compressed_quadrant_weights` drops `w <= 0` and renormalizes; the v4
    compression does neither. At λ = 0.5 both of those steps are IDENTITIES — every
    core weight stays strictly positive and the moved vector already sums to 1 — so
    the two books are the same book.

    They are not, however, bit-identical: `base + f*(mean - base)` and
    `f*base + (1-f)*center` associate the floats differently and TLT lands 1 ULP
    apart (1.39e-17) in `expansion` and `slowdown`. Recorded rather than papered
    over — the tolerance here is 1 ULP and nothing wider, and the ledger comparison
    that actually matters is still zero-tolerance because it never mixes the two
    constructions."""
    keys = list(pinned_sleeve.PER_QUADRANT_BASELINE_WEIGHTS)
    mean = {t: sum(pinned_sleeve.PER_QUADRANT_BASELINE_WEIGHTS[k].get(t, 0.0)
                   for k in keys) / len(keys)
            for t in pinned_sleeve.SLEEVE_TICKERS}
    pinned = pinned_sleeve.compressed_quadrant_weights(0.5)
    for quadrant in books.QUADRANTS:
        base = pinned_sleeve.PER_QUADRANT_BASELINE_WEIGHTS[
            pinned_sleeve.QUADRANT_TO_KEY[quadrant]]
        moved = {t: base.get(t, 0.0) + 0.5 * (mean[t] - base.get(t, 0.0))
                 for t in pinned_sleeve.SLEEVE_TICKERS}
        # the `w > 0` filter is an identity here ...
        assert all(w > 0.0 for w in moved.values())
        # ... and so is the renormalization.
        assert sum(moved.values()) == 1.0

        mine = books.compressed(quadrant, 0.5)
        theirs = pinned[pinned_sleeve.QUADRANT_TO_KEY[quadrant]]
        assert sorted(theirs) == sorted(books.CORE_TICKERS)
        for ticker in books.CORE_TICKERS:
            delta = abs(mine[ticker] - theirs[ticker])
            assert delta <= math.ulp(max(abs(mine[ticker]), abs(theirs[ticker]))), (
                f"{quadrant}/{ticker}: {delta!r} is more than one ULP apart")


def test_the_pinned_drop_and_renormalize_would_delete_the_barbell_destination() -> None:
    """The dissolution proof, part 3: WHY the pinned machinery cannot be reused.

    The pinned book keeps only strictly positive weights. On the v4 universe the
    credit legs start at zero, so a drop-and-renormalize pass deletes LQD — the
    barbell's own destination — and HYG before the barbell could ever fill them.
    The v4 books are emitted over a FIXED vector for exactly this reason."""
    dropped = pinned_sleeve._renormalize(
        {t: w for t, w in books.CENTER.items() if w > 0.0})
    assert "LQD" not in dropped
    assert "HYG" not in dropped
    assert set(books.CENTER) - set(dropped) == set(books.CREDIT_TICKERS)
    # ... and the barbell needs that column to exist.
    assert books.DOMINANCE_BOOK["LQD"] > 0.0


def test_the_signed_dial_never_computes_a_compressed_25_book() -> None:
    """Only λ = 0.5 and amplitude 0 are reachable, so the grid's most compressed
    cell — where a drop-and-renormalize would actually bite — is never built."""
    reachable = {books.DOMINANCE_COMPRESSION, books.SEVERE_COMPRESSION,
                 books.CONTAINED_AMPLITUDE}
    assert reachable == {0.5, 0.0}
    assert all("compressed_25" not in book_id for book_id in books.emitted_books())


def test_applying_the_barbell_twice_does_not_reproduce_the_signed_book() -> None:
    """The measured trap: `bb_transform` is not idempotent.

    Pre-barbelling the dominance book and then running the hook again re-splits the
    SHY leg the first pass created. The signed book is the ONE-pass result; the
    two-pass result is 0.093 away on SHY and LQD — and, measured here, it is not
    merely a different book: it BREAKS the defensive floor. The double pass moves
    TLT+SHY+TIP to 0.1395 against a floor of 0.20, so the mandate invariant catches
    the trap even without the reference weights to compare against."""
    once = books.DOMINANCE_BOOK
    twice = books.bb_transform(once)
    assert twice != once
    delta = max(abs(twice[t] - once[t]) for t in books.UNIVERSE)
    assert delta == pytest.approx(0.093, abs=1e-12)
    assert once["SHY"] == 0.23249999999999998
    assert once["LQD"] == 0.15500000000000003
    assert twice["SHY"] == pytest.approx(0.1395, abs=1e-12)
    assert twice["LQD"] == pytest.approx(0.2480, abs=1e-12)
    # the sleeve TOTAL is preserved by both passes, which is why the error is silent
    # in the weight sum ...
    assert sum(twice.values()) == pytest.approx(1.0, abs=1e-9)
    # ... and loud in the mandate.
    assert sum(twice[t] for t in books.DEFENSIVE_ASSETS) == pytest.approx(
        0.1395, abs=1e-12)
    with pytest.raises(books.BookInvariantError, match="defensive floor"):
        books.check_invariants(twice, "double_barbell")


def test_the_signed_dominance_book_is_the_expansion_c50_barbell() -> None:
    expected = {"SPY": 0.39375, "TLT": 0.0, "TIP": 0.0, "GLD": 0.10625,
                "DBC": 0.1125, "SHY": 0.2325, "LQD": 0.155, "HYG": 0.0}
    for ticker, value in expected.items():
        assert books.DOMINANCE_BOOK[ticker] == pytest.approx(value, abs=1e-16)


# --------------------------------------------------------------------------- #
# L1 — the hysteresis band                                                     #
# --------------------------------------------------------------------------- #

def _routed(values: list[float]) -> pd.DataFrame:
    index = pd.date_range("2000-01-31", periods=len(values), freq="ME")
    return fiscal.route(pd.Series(values, index=index, dtype="float64"))


def test_the_hysteresis_band_holds_the_state_it_does_not_chatter() -> None:
    """Inside [4.0, 5.0] the router CARRIES whatever is in force and flags it."""
    out = _routed([3.0, 4.5, 5.5, 4.5, 4.0, 5.0, 3.9])
    assert list(out["fiscal_state"]) == [
        "contained", "contained", "dominance", "dominance", "dominance",
        "dominance", "contained"]
    assert list(out["fiscal_boundary"]) == [
        False, True, False, True, True, True, False]


def test_the_cold_start_picks_a_side_only_on_the_first_finite_reading() -> None:
    out = _routed([float("nan"), float("nan"), 6.0, 4.5])
    assert out["fiscal_state"].iloc[0] is None
    assert out["fiscal_state"].iloc[1] is None
    assert list(out["fiscal_state"])[2:] == ["dominance", "dominance"]
    assert list(out["fiscal_state_age_m"]) == [0, 0, 1, 2]

    cold_contained = _routed([5.0])
    assert cold_contained["fiscal_state"].iloc[0] == "contained"  # 5.0 is not > 5.0


def test_a_non_finite_reading_abstains_and_resets_the_age() -> None:
    out = _routed([3.0, 3.0, float("nan"), 3.0])
    assert list(out["fiscal_state_age_m"]) == [1, 2, 0, 1]
    assert out["fiscal_state"].iloc[2] is None


def test_a_month_without_a_fiscal_state_gets_no_book_at_all() -> None:
    book, book_id = books.compose(None, "off", "expansion")
    assert book is None and book_id is None


def test_the_thresholds_refuse_an_unordered_band() -> None:
    with pytest.raises(ValueError, match="hysteresis"):
        fiscal.FiscalThresholds(enter=4.0, exit=5.0)


def test_the_pit_convention_stamps_a_release_not_the_reference_month() -> None:
    """A quarterly input lagged 3 months reads as a step function between stamps."""
    index = pd.date_range("2020-01-31", "2020-12-31", freq="ME")
    quarterly = pd.Series([100.0, 200.0],
                          index=pd.DatetimeIndex(["2020-01-01", "2020-04-01"]))
    pit = fiscal.pit_monthly(quarterly, index, 3)
    assert np.isnan(pit.loc[pd.Timestamp("2020-03-31")])
    assert pit.loc[pd.Timestamp("2020-04-30")] == 100.0
    assert pit.loc[pd.Timestamp("2020-06-30")] == 100.0
    assert pit.loc[pd.Timestamp("2020-07-31")] == 200.0
