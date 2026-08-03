"""L1 — the fiscal router of the open_macro **v4.0-rev** regime engine (M-COMP4).

WHAT THIS LAYER IS
------------------
Layer 1 answers one question and only one: *is the fiscal stance DOMINANT or
CONTAINED?*  It reads a single observable — the rolling-12-month federal deficit
as a percent of nominal GDP — and runs it through a hysteresis band so the
answer cannot chatter around a threshold.

    deficit_gdp(t) = -( rolling_12m_sum(MTSDS133FMS)(t) / 1000 ) / GDP(t) * 100

``MTSDS133FMS`` (Monthly Treasury Statement, federal surplus/deficit) is a
NEGATIVE number in a deficit month and is quoted in $ millions; ``GDP`` is
nominal, quarterly, in $ billions.  The division by 1000 converts the summed
millions to billions, and the leading minus turns "surplus" into "deficit", so a
positive ``deficit_gdp`` is a deficit worth that many percent of GDP.

POINT-IN-TIME
-------------
Both inputs are lagged to what a decision maker knew at the CLOSE of month-end
``t``: MTS by 1 month, GDP by 3 months.  The convention is the one the signed
M-COMP4 engine uses (``m4_core._pit``): stamp each observation at
``month_end(obs_date + lag)``, drop duplicate stamps keeping the last, then
reindex onto the monthly decision index and forward-fill.  Nothing here
reconstructs release vintages — this is a publication-lag shift, and the
formulation-freeze artifact says so explicitly.

HYSTERESIS
----------
``> 5.0`` enters DOMINANCE, ``< 4.0`` returns to CONTAINED, and the closed band
``[4.0, 5.0]`` HOLDS whatever state is already in force (and raises the
``fiscal_boundary`` flag, so a book taken inside the band is auditable as such).
Cold start: the first month with a finite reading picks ``dominance`` iff it is
above the entry threshold, ``contained`` otherwise — there is no "unknown" state
once the panel is finite, and there is no state at all before it.

``fiscal_state_age_m`` counts how many consecutive months the CURRENT state has
been in force (1 on the month it is entered).  L3 consumes it: a SEVERE
candidate requires ``contained`` with age >= 3 (amendment A3).

This module is PURE and NON-PINNED: it holds no DB handle, no clock and no
governance switch, and it deliberately does not import any hash-pinned module.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

# ─────────────────────────── the signed constants ──────────────────────────
# Explicit FRED series ids. Never aliases: `GDP` is the NOMINAL level series,
# NOT the real-growth series `A191RL1Q225SBEA`.
MTS_SERIES_ID = "MTSDS133FMS"
GDP_SERIES_ID = "GDP"

MTS_PIT_LAG_MONTHS = 1
GDP_PIT_LAG_MONTHS = 3

# MTS is $ millions, GDP is $ billions.
MTS_MILLIONS_PER_GDP_UNIT = 1000.0
DEFICIT_WINDOW_MONTHS = 12

FISCAL_ENTER = 5.0   # deficit_gdp strictly above this ENTERS dominance
FISCAL_EXIT = 4.0    # deficit_gdp strictly below this RETURNS to contained

DOMINANCE = "dominance"
CONTAINED = "contained"


@dataclass(frozen=True)
class FiscalThresholds:
    """The hysteresis band. Frozen; the signed configuration is the default."""

    enter: float = FISCAL_ENTER
    exit: float = FISCAL_EXIT

    def __post_init__(self) -> None:
        if not self.exit < self.enter:
            raise ValueError(
                f"fiscal hysteresis needs exit < enter, got exit={self.exit!r} "
                f"enter={self.enter!r} (a band that is not strictly ordered is not "
                "hysteresis, it is a threshold with a sign error)")


SIGNED_THRESHOLDS = FiscalThresholds()


# ───────────────────────────── PIT panel ───────────────────────────────────

def pit_monthly(series: pd.Series, index: pd.DatetimeIndex,
                lag_months: int) -> pd.Series:
    """Stamp ``series`` at ``month_end(obs_date + lag_months)``, then ffill onto
    ``index``.

    This is the M-COMP4 ``_pit`` convention verbatim: duplicate stamps keep the
    LAST observation, the stamped series is sorted, and the reindex forward-fills
    so a quarterly input reads as a step function between releases. Months before
    the first stamp stay NaN — the panel refuses to invent a reading.
    """
    if len(series) == 0:
        return pd.Series(np.nan, index=index, dtype="float64")
    stamped = pd.Series(
        series.to_numpy(dtype="float64"),
        index=pd.DatetimeIndex(
            [d + pd.DateOffset(months=lag_months) + pd.offsets.MonthEnd(0)
             for d in series.index]),
    )
    stamped = stamped[~stamped.index.duplicated(keep="last")].sort_index()
    return stamped.reindex(index).ffill()


def pit_stamp(series: pd.Series, index: pd.DatetimeIndex,
              lag_months: int) -> pd.Series:
    """The PIT *stamp* behind each ffilled reading — the release month-end whose
    value is in force at each month of ``index``. Used to publish an observation
    age as a diagnostic; the freshness VERDICT is L3's, in native publication
    periods, not a month count (see ``credit_guard``)."""
    if len(series) == 0:
        return pd.Series(pd.NaT, index=index, dtype="datetime64[ns]")
    stamps = pd.DatetimeIndex(
        [d + pd.DateOffset(months=lag_months) + pd.offsets.MonthEnd(0)
         for d in series.index])
    s = pd.Series(stamps, index=stamps)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s.reindex(index).ffill()


def observation_age_months(series: pd.Series, index: pd.DatetimeIndex,
                           lag_months: int) -> pd.Series:
    """Calendar months between each month of ``index`` and the PIT stamp in force.

    DIAGNOSTIC ONLY. It is published so a reader can see how old a reading is; it
    is NOT the staleness test (a quarterly series read monthly is 0/1/2 months old
    by construction and that says nothing about whether a release was missed)."""
    stamps = pit_stamp(series, index, lag_months)
    age = pd.Series(np.nan, index=index, dtype="float64")
    ok = stamps.notna()
    age[ok] = [(a.year - b.year) * 12 + (a.month - b.month)
               for a, b in zip(index[ok.to_numpy()], stamps[ok])]
    return age


def deficit_to_gdp(mts: pd.Series, gdp: pd.Series,
                   index: pd.DatetimeIndex) -> pd.Series:
    """The L1 observable: rolling-12m federal deficit as a percent of nominal GDP.

    ``mts`` and ``gdp`` are the RAW series (obs_date index, native frequency);
    the PIT lags are applied here so a caller cannot forget them.
    """
    gdp_pit = pit_monthly(gdp, index, GDP_PIT_LAG_MONTHS)
    mts_pit = pit_monthly(mts, index, MTS_PIT_LAG_MONTHS)
    rolling = mts_pit.rolling(DEFICIT_WINDOW_MONTHS).sum()
    return -(rolling / MTS_MILLIONS_PER_GDP_UNIT) / gdp_pit * 100.0


# ─────────────────────────── the state machine ─────────────────────────────

def route(deficit_gdp: pd.Series,
          thresholds: FiscalThresholds = SIGNED_THRESHOLDS) -> pd.DataFrame:
    """Run the hysteresis band over ``deficit_gdp``.

    Returns a frame indexed like the input with three columns:

    ``fiscal_state``          ``'dominance'`` | ``'contained'`` | ``None``
    ``fiscal_state_age_m``    consecutive months the current state has held (1 on
                              the month of entry; 0 while there is no state)
    ``fiscal_boundary``       True inside the closed band ``[exit, enter]`` — the
                              months where the state is CARRIED rather than chosen

    A non-finite reading emits ``None`` and RESETS the age to 0 without deciding
    anything: the router abstains rather than guessing, and the book composer
    refuses to publish a book for a stateless month.
    """
    states: list[str | None] = []
    current: str | None = None
    for value in deficit_gdp.to_numpy(dtype="float64"):
        if not np.isfinite(value):
            states.append(None)
            continue
        if current is None:
            current = DOMINANCE if value > thresholds.enter else CONTAINED
        elif value > thresholds.enter:
            current = DOMINANCE
        elif value < thresholds.exit:
            current = CONTAINED
        states.append(current)

    ages: list[int] = []
    previous: str | None = None
    run = 0
    for state in states:
        if state is None:
            ages.append(0)
            previous, run = None, 0
            continue
        run = run + 1 if state == previous else 1
        previous = state
        ages.append(run)

    out = pd.DataFrame(index=deficit_gdp.index)
    out["deficit_gdp"] = deficit_gdp
    out["fiscal_state"] = pd.Series(states, index=deficit_gdp.index, dtype="object")
    out["fiscal_state_age_m"] = ages
    out["fiscal_boundary"] = (
        (deficit_gdp >= thresholds.exit) & (deficit_gdp <= thresholds.enter))
    return out


def fiscal_panel(mts: pd.Series, gdp: pd.Series, index: pd.DatetimeIndex,
                 thresholds: FiscalThresholds = SIGNED_THRESHOLDS) -> pd.DataFrame:
    """End-to-end L1: raw MTS + raw GDP -> deficit_gdp -> routed state frame."""
    return route(deficit_to_gdp(mts, gdp, index), thresholds)


def state_transitions(fiscal_state: pd.Series) -> list[dict[str, object]]:
    """Every month where the state CHANGES, as ``{date, from, to}`` records.

    The cold start (``None -> contained|dominance``) is a transition too and is
    reported as such; the fiscal ledger in the formulation freeze is exactly this
    list over the decision window."""
    out: list[dict[str, object]] = []
    unset = object()
    previous: object = unset
    for date, state in fiscal_state.items():
        value = state if isinstance(state, str) else None
        if previous is unset:
            previous = value
            if value is not None:
                # the series opens already in a state: that IS the entry the
                # window sees, and hiding it would understate the ledger.
                out.append({"date": date, "from": None, "to": value})
            continue
        if value != previous:
            out.append({"date": date, "from": previous, "to": value})
            previous = value
    return out
