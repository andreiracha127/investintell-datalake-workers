"""L3 — the credit guard of the open_macro **v4.0-rev** regime engine (M-COMP4).

WHAT THIS LAYER IS
------------------
Layer 3 is a compression guard, not a forecast. It watches two independent
readings of credit/liquidity conditions and, when either fires, it compresses the
portfolio toward the neutral CENTER (ALERT) or — only under the conditions in A3
and A8 below — replaces it wholesale with the defensive book (SEVERE).

    arm A   z( SUBLPDCILSLGNQ , 54 months, min 36 ) > 0.5
    arm B   M2SL year-over-year (%) > 12

``SUBLPDCILSLGNQ`` is the SLOOS *net percentage of banks tightening standards on
C&I loans to large and middle-market firms* — the BANK DECISION series. It is not
``DRTSCILM`` and not a spread: the decision to lend is the detector; the price of
credit is coincident and would hand out risk-ON in 2007-09.

    ALERT  = arm_a OR arm_b OR guard_blind

FRESHNESS IS MEASURED IN NATIVE PUBLICATION PERIODS, NOT IN MONTHS
------------------------------------------------------------------
The two arms publish on different clocks, so a single "months old" threshold gives
opposite verdicts for the same label age. Each arm declares its native frequency
and its publication lag, and the deadline of a period is

    deadline(p) = month_end( start_of(p) + lag_months )

    missing(arm, t) = #{ p : p > last_observation ∧ deadline(p) <= t }

An arm is STALE iff ``missing >= 1`` — at least one release whose deadline has
passed never arrived. Measured calibration (2026-08-03), the pair that makes the
point:

    M2SL             last obs 2026-04, monthly,   lag 1  ->  missing 2  ->  STALE
    SUBLPDCILSLGNQ   last obs 2026-04, quarterly, lag 2  ->  missing 0  ->  FRESH

Same label age, opposite verdicts, and a month-count rule cannot express that.

A STALE ARM CONTRIBUTES ``False`` TO THE ALERT and raises a coverage token
(``partial_a`` / ``partial_b``). BOTH stale is ``guard_blind``: the guard cannot
see, so it compresses conservatively — ALERT — and says so (``blind``).

The token is emitted even when the book does not move. Under amplitude 0 an ALERT
inside ``contained`` returns the CENTER, which is the book it already held: the
guard is INERT there. That is exactly when a silent degradation would be
invisible, so the coverage token is never suppressed on the grounds that the
weights did not change.

EQUIVALENCE WITH THE SIGNED ENGINE
----------------------------------
``m4_core.build_states`` carries a single ``guard_blind = sloos_age_m > 5 or NaN``
month-count rule. Over the golden decision window (2006-12..2026-05) the SLOOS
mirror is gapless and quarterly, so its PIT age cycles 0/1/2 and never reaches
either bound: ``guard_blind`` is False in all 234 months under BOTH rules, and the
golden end-to-end replay proves the two rules agree there byte for byte. They do
NOT agree before 1990-04, where SLOOS has no data at all: the month-count rule
calls that blind (and therefore ALERT, and therefore a SEVERE candidate), while
the per-arm rule sees M2SL publishing normally and only drops arm A. The per-arm
rule is the v4 formulation; the month-count rule is what it replaces.

A3 / A8
-------
A3  A SEVERE candidate additionally requires ``fiscal_state == contained`` with
    ``fiscal_state_age_m >= 3``. Fiscal dominance never takes the SEVERE book.
A8  ``persist='confirm'``, K=3. The ENTRY rule is unchanged; a month that meets it
    is a CANDIDATE. ``severe_run_age`` counts the current maximal consecutive
    candidate stretch. The candidate is EMITTED as SEVERE iff
    ``run_age <= K or stress_confirmed``; otherwise it DEGRADES to ALERT and
    re-escalates the moment stress confirms.
    ``stress_confirmed(t)``: SPY month-end adjusted close at least 8% below its
    trailing 12-month month-end maximum (window t-11..t, 12 observations required).

This module is PURE and NON-PINNED. It imports nothing hash-pinned and writes
nothing; it is a formula, and the governance surface lives in the formulation
freeze artifact.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

# ─────────────────────────── the signed constants ──────────────────────────
# Explicit FRED series ids, never aliases.
SLOOS_SERIES_ID = "SUBLPDCILSLGNQ"
M2_SERIES_ID = "M2SL"

SLOOS_PIT_LAG_MONTHS = 2
SLOOS_Z_WINDOW_MONTHS = 54
SLOOS_Z_MIN_PERIODS = 36
SLOOS_Z_THRESHOLD = 0.5

M2_PIT_LAG_MONTHS = 1
M2_YOY_LOOKBACK_MONTHS = 12
M2_YOY_THRESHOLD = 12.0

SEVERE_MIN_CONTAINED_AGE_M = 3
SEVERE_PERSIST_MODE = "confirm"
SEVERE_PERSIST_K = 3

STRESS_TICKER = "SPY"
STRESS_WINDOW_MONTHS = 12
STRESS_DRAWDOWN_X = -0.08

GUARD_OFF = "off"
GUARD_ALERT = "alert"
GUARD_SEVERE = "severe"

COVERAGE_FULL = "full"
COVERAGE_PARTIAL_A = "partial_a"
COVERAGE_PARTIAL_B = "partial_b"
COVERAGE_BLIND = "blind"

MONTHLY = "monthly"
QUARTERLY = "quarterly"
_MONTHS_PER_PERIOD = {MONTHLY: 1, QUARTERLY: 3}


# ──────────────────── freshness in native publication periods ──────────────

@dataclass(frozen=True)
class PublicationClock:
    """How one arm's source publishes: native period length + publication lag.

    ``frequency`` is ``'monthly'`` or ``'quarterly'``; a period is identified by
    its START date (FRED's convention for both series here). ``lag_months`` is the
    number of months after the period start by whose month-end the release is due.
    """

    series_id: str
    frequency: str
    lag_months: int

    def __post_init__(self) -> None:
        if self.frequency not in _MONTHS_PER_PERIOD:
            raise ValueError(
                f"{self.series_id}: unknown native frequency {self.frequency!r} "
                f"(expected one of {sorted(_MONTHS_PER_PERIOD)})")
        if self.lag_months < 0:
            raise ValueError(f"{self.series_id}: negative publication lag")

    @property
    def months_per_period(self) -> int:
        return _MONTHS_PER_PERIOD[self.frequency]

    def period_start(self, moment: pd.Timestamp) -> pd.Timestamp:
        """Snap ``moment`` down to the start of the native period containing it."""
        ts = pd.Timestamp(moment)
        if self.frequency == QUARTERLY:
            month = 1 + 3 * ((ts.month - 1) // 3)
            return pd.Timestamp(year=ts.year, month=month, day=1)
        return pd.Timestamp(year=ts.year, month=ts.month, day=1)

    def next_period(self, period_start: pd.Timestamp) -> pd.Timestamp:
        return pd.Timestamp(period_start) + pd.DateOffset(months=self.months_per_period)

    def deadline(self, period_start: pd.Timestamp) -> pd.Timestamp:
        """Month-end of ``period_start + lag_months`` — when the release is due."""
        return (pd.Timestamp(period_start)
                + pd.DateOffset(months=self.lag_months)
                + pd.offsets.MonthEnd(0))


SLOOS_CLOCK = PublicationClock(SLOOS_SERIES_ID, QUARTERLY, SLOOS_PIT_LAG_MONTHS)
M2_CLOCK = PublicationClock(M2_SERIES_ID, MONTHLY, M2_PIT_LAG_MONTHS)


@dataclass(frozen=True)
class ArmFreshness:
    """The verdict for one arm at one evaluation moment."""

    series_id: str
    frequency: str
    lag_months: int
    last_observation: pd.Timestamp | None
    evaluated_at: pd.Timestamp
    missing_periods: int
    stale: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "series_id": self.series_id,
            "frequency": self.frequency,
            "lag_months": self.lag_months,
            "last_observation": (None if self.last_observation is None
                                 else f"{self.last_observation:%Y-%m-%d}"),
            "evaluated_at": f"{self.evaluated_at:%Y-%m-%d}",
            "missing_periods": self.missing_periods,
            "stale": self.stale,
        }


def missing_periods(last_observation: pd.Timestamp | None,
                    evaluated_at: pd.Timestamp,
                    clock: PublicationClock) -> int:
    """``#{ p : p > last_observation ∧ deadline(p) <= evaluated_at }``.

    Requires an observation to count from. A source with NO observation at all has
    nothing to be late relative to; :func:`arm_freshness` calls that stale by
    inspection rather than inventing an epoch, so this function fails loud instead
    of returning a fabricated count.
    """
    if last_observation is None:
        raise ValueError("missing_periods needs a last observation; a source with "
                         "no observation at all is stale by inspection")
    deadline_moment = pd.Timestamp(evaluated_at)
    period = clock.next_period(clock.period_start(last_observation))
    count = 0
    while clock.deadline(period) <= deadline_moment:
        count += 1
        period = clock.next_period(period)
    return count


def arm_freshness(observation_dates, evaluated_at, clock: PublicationClock) -> ArmFreshness:
    """Freshness of one arm at ``evaluated_at``, in the arm's NATIVE periods.

    ``observation_dates`` is the arm's raw observation index (period starts). Only
    observations dated on or before ``evaluated_at`` are visible — a replay must
    not read a print that had not been stamped yet. An arm with no visible
    observation is stale with ``missing_periods = 0``: there is nothing to be late
    relative to, and calling that "fresh" would be the exact lie the token exists
    to prevent.
    """
    moment = pd.Timestamp(evaluated_at)
    visible = [pd.Timestamp(d) for d in observation_dates if pd.Timestamp(d) <= moment]
    if not visible:
        return ArmFreshness(clock.series_id, clock.frequency, clock.lag_months,
                            None, moment, 0, True)
    last = max(visible)
    n_missing = missing_periods(last, moment, clock)
    return ArmFreshness(clock.series_id, clock.frequency, clock.lag_months,
                        last, moment, n_missing, n_missing >= 1)


def arm_fresh_series(observation_dates, index: pd.DatetimeIndex,
                     clock: PublicationClock) -> pd.Series:
    """``arm_freshness`` evaluated at every month-end of ``index`` (True == fresh).

    Walked forward once rather than recomputed per month: the visible-observation
    frontier and the deadline frontier are both monotone in the evaluation moment.
    """
    dates = sorted(pd.Timestamp(d) for d in observation_dates)
    out: list[bool] = []
    cursor = 0
    last: pd.Timestamp | None = None
    for moment in index:
        while cursor < len(dates) and dates[cursor] <= moment:
            last = dates[cursor]
            cursor += 1
        if last is None:
            out.append(False)
            continue
        out.append(missing_periods(last, moment, clock) == 0)
    return pd.Series(out, index=index, dtype=bool)


def coverage_token(arm_a_fresh: bool, arm_b_fresh: bool) -> str:
    if arm_a_fresh and arm_b_fresh:
        return COVERAGE_FULL
    if arm_a_fresh:
        return COVERAGE_PARTIAL_B
    if arm_b_fresh:
        return COVERAGE_PARTIAL_A
    return COVERAGE_BLIND


# ──────────────────────────── the two arm sensors ──────────────────────────

def sloos_z(sloos_pit: pd.Series,
            window_months: int = SLOOS_Z_WINDOW_MONTHS,
            min_periods: int = SLOOS_Z_MIN_PERIODS) -> pd.Series:
    """Rolling z-score of the PIT SLOOS panel, CURRENT MONTH INCLUDED.

    The window is trailing and inclusive (``t-53..t``); ``min_periods`` months are
    required before any z is emitted, so the first ~3 years read NaN rather than a
    z built on four observations."""
    rolling = sloos_pit.rolling(window_months, min_periods=min_periods)
    return (sloos_pit - rolling.mean()) / rolling.std()


def m2_yoy(m2_raw: pd.Series, index: pd.DatetimeIndex,
           lookback_months: int = M2_YOY_LOOKBACK_MONTHS) -> pd.Series:
    """M2SL year-over-year in percent, shifted one month for the publication lag.

    Month-end resampled (``last``), NaN months dropped before the reindex so a hole
    in the mirror reads as a hole and not as a carried level, then the YoY is
    shifted by ``M2_PIT_LAG_MONTHS`` — at month-end ``t`` the reading is the YoY
    computed through month ``t-1``, which is what was published by then."""
    monthly = m2_raw.resample("ME").last().dropna().reindex(index)
    return (100.0 * (monthly / monthly.shift(lookback_months) - 1.0)).shift(
        M2_PIT_LAG_MONTHS)


def stress_confirmed_series(spy_month_end: pd.Series, index: pd.DatetimeIndex,
                            x: float = STRESS_DRAWDOWN_X,
                            window: int = STRESS_WINDOW_MONTHS) -> pd.Series:
    """A8's price confirmation: SPY at or below ``x`` from its trailing 12m max.

    ``x`` is negative. A window with fewer than ``window`` observations yields
    False — the confirmation cannot be manufactured out of a short history."""
    s = spy_month_end.reindex(index)
    rolling_max = s.rolling(window, min_periods=window).max()
    return ((s / rolling_max - 1.0) <= x).fillna(False).astype(bool)


def stress_drawdown_series(spy_month_end: pd.Series, index: pd.DatetimeIndex,
                           window: int = STRESS_WINDOW_MONTHS) -> pd.Series:
    """The drawdown itself, published as the audit trail behind the boolean."""
    s = spy_month_end.reindex(index)
    return s / s.rolling(window, min_periods=window).max() - 1.0


# ────────────────────────────── the guard state ────────────────────────────

def build_guard(
    *,
    sloos_raw: pd.Series,
    m2_raw: pd.Series,
    index: pd.DatetimeIndex,
    fiscal_state: pd.Series,
    fiscal_state_age_m: pd.Series,
    stress_confirmed: pd.Series | None = None,
    z_threshold: float = SLOOS_Z_THRESHOLD,
    m2_threshold: float = M2_YOY_THRESHOLD,
    severe_min_contained_age: int = SEVERE_MIN_CONTAINED_AGE_M,
    persist_k: int = SEVERE_PERSIST_K,
) -> pd.DataFrame:
    """The full L3 frame over ``index``.

    Columns, in the order the ledger publishes them:

    ``sloos_z``            arm A sensor (NaN until ``min_periods`` is satisfied)
    ``sloos_age_m``        DIAGNOSTIC calendar age of the PIT stamp
    ``m2_yoy``             arm B sensor
    ``arm_a_fresh`` / ``arm_b_fresh``   native-period freshness per arm
    ``arm_a`` / ``arm_b``  the arm verdicts AFTER the stale gate (a stale arm is
                           False, never NaN, never optimistically carried)
    ``guard_blind``        both arms stale
    ``guard_coverage``     ``full`` | ``partial_a`` | ``partial_b`` | ``blind``
    ``alert``              ``arm_a or arm_b or guard_blind``
    ``severe_candidate``   A3: alert AND contained AND contained-age >= 3
    ``severe_run_age``     length of the current consecutive candidate stretch
    ``stress_confirmed``   A8's price confirmation
    ``severe_emit``        A8: candidate AND (run_age <= K OR stress confirmed)
    ``severe_degraded``    candidate that did NOT emit — the guard DISARMED the
                           portfolio and only price re-arms it
    ``guard_level``        ``off`` | ``alert`` | ``severe``
    """
    from src import fiscal_state as _fiscal   # PIT helpers; pure, non-pinned

    frame = pd.DataFrame(index=index)
    sloos_pit = _fiscal.pit_monthly(sloos_raw, index, SLOOS_PIT_LAG_MONTHS)
    frame["sloos_z"] = sloos_z(sloos_pit)
    frame["sloos_age_m"] = _fiscal.observation_age_months(
        sloos_raw, index, SLOOS_PIT_LAG_MONTHS)
    frame["m2_yoy"] = m2_yoy(m2_raw, index)

    fresh_a = arm_fresh_series(sloos_raw.index, index, SLOOS_CLOCK)
    fresh_b = arm_fresh_series(m2_raw.index, index, M2_CLOCK)
    frame["arm_a_fresh"] = fresh_a
    frame["arm_b_fresh"] = fresh_b

    raw_a = (frame["sloos_z"] > z_threshold).fillna(False).astype(bool)
    raw_b = (frame["m2_yoy"] > m2_threshold).fillna(False).astype(bool)
    frame["arm_a_raw"] = raw_a
    frame["arm_b_raw"] = raw_b
    frame["arm_a"] = raw_a & fresh_a
    frame["arm_b"] = raw_b & fresh_b
    frame["guard_blind"] = ~fresh_a & ~fresh_b
    frame["guard_coverage"] = [coverage_token(a, b) for a, b in zip(fresh_a, fresh_b)]
    frame["alert"] = frame["arm_a"] | frame["arm_b"] | frame["guard_blind"]

    contained_ok = (fiscal_state.reindex(index).eq(_fiscal.CONTAINED)
                    & (fiscal_state_age_m.reindex(index) >= severe_min_contained_age))
    candidate = (frame["alert"] & contained_ok).astype(bool)
    frame["severe_candidate"] = candidate

    run_ages: list[int] = []
    run = 0
    for flag in candidate.to_numpy():
        run = run + 1 if flag else 0
        run_ages.append(run)
    frame["severe_run_age"] = run_ages

    if stress_confirmed is None:
        confirmed = pd.Series(False, index=index, dtype=bool)
    else:
        confirmed = stress_confirmed.reindex(index).fillna(False).astype(bool)
    frame["stress_confirmed"] = confirmed

    run_array = np.asarray(run_ages)
    emit = candidate.to_numpy() & ((run_array <= persist_k) | confirmed.to_numpy())
    frame["severe_emit"] = emit
    frame["severe_degraded"] = candidate.to_numpy() & ~emit
    frame["guard_level"] = np.where(
        ~frame["alert"].to_numpy(), GUARD_OFF,
        np.where(emit, GUARD_SEVERE, GUARD_ALERT))
    return frame
