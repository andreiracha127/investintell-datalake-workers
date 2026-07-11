"""TDD suite for the Tranche W1 regime-timeline metrics + their runner integration.

Covers:
  * regime_timeline_metrics: counts, abstention streak, carry age, same-quadrant run
    over the carry-filled consumable chain, quadrant mix, rolling fresh-valid rates,
    and the fail-loud duplicate-month guard;
  * upside_capture_by_calendar_year: per-year strategy/SPY return, spy_up gate,
    division guards;
  * the runner ALWAYS attaches a ``timeline`` block to the gate report + run dict.

Network-free, DB-free (the unit tests use lightweight fake decisions; the runner
integration test replays the committed pack over a short fast window).
"""

from __future__ import annotations

import datetime as dt

import pytest

from harness.phase0q import metrics, runner, sleeve

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACK_DIR = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"


class _FakeDecision:
    """A DecisionRow-shaped stand-in (as_of / status / quadrant / validity)."""

    def __init__(self, as_of: dt.date, quadrant: str | None, status: str):
        self.as_of = as_of
        self.quadrant = quadrant
        self.status = status

    def has_valid_quadrant(self) -> bool:
        return self.status == "valid" and self.quadrant is not None


def _month_end(year: int, month: int) -> dt.date:
    if month == 12:
        return dt.date(year, 12, 31)
    return dt.date(year, month + 1, 1) - dt.timedelta(days=1)


def _chain(*specs):
    """Build a monthly chain from (year, month, quadrant_or_None, status) specs."""
    return [_FakeDecision(_month_end(y, m), q, s) for (y, m, q, s) in specs]


# --------------------------------------------------------------------------- #
# regime_timeline_metrics                                                     #
# --------------------------------------------------------------------------- #

def test_timeline_counts_streaks_carry_age_and_same_quadrant_run():
    # fresh -> 4 gated -> fresh (same quadrant) -> flip -> hold flip.
    chain = _chain(
        (2021, 1, "expansion", "valid"),
        (2021, 2, "expansion", "low_confidence"),   # carry age 1
        (2021, 3, "expansion", "low_confidence"),   # carry age 2
        (2021, 4, "expansion", "low_confidence"),   # carry age 3
        (2021, 5, "expansion", "low_confidence"),   # carry age 4 (streak 4)
        (2021, 6, "expansion", "valid"),            # fresh resets; consumable still expansion
        (2021, 7, "contraction", "valid"),          # quadrant flips
        (2021, 8, "contraction", "valid"),
    )
    m = metrics.regime_timeline_metrics(chain)
    assert m["n_months"] == 8
    assert m["n_valid"] == 4
    assert m["n_low_confidence"] == 4
    assert m["max_abstention_streak_months"] == 4
    assert m["max_carry_age_months"] == 4
    # consumable quadrants: expansion x6 (01..06), contraction x2 (07,08).
    assert m["max_same_quadrant_run_months"] == 6
    assert m["quadrant_mix"] == {"recovery": 0, "expansion": 2,
                                 "slowdown": 0, "contraction": 2}
    assert m["fresh_valid_rate"]["global"] == pytest.approx(0.5)
    # n < window -> denominator is n, so all rolling rates equal the global rate.
    assert m["fresh_valid_rate"]["rolling_12m"] == pytest.approx(0.5)
    assert m["fresh_valid_rate"]["rolling_36m"] == pytest.approx(0.5)
    assert m["first_as_of"] == "2021-01-31"
    assert m["last_as_of"] == "2021-08-31"


def test_timeline_carry_age_ignores_leading_abstention_before_first_seed():
    """Abstention BEFORE the first valid decision is not carry (no seed to carry): it
    counts toward the abstention streak but not toward carry age."""
    chain = _chain(
        (2020, 1, None, "low_confidence"),   # streak 1, no carry (no seed yet)
        (2020, 2, None, "low_confidence"),   # streak 2
        (2020, 3, "recovery", "valid"),      # first seed
        (2020, 4, None, "low_confidence"),   # carry age 1
    )
    m = metrics.regime_timeline_metrics(chain)
    assert m["max_abstention_streak_months"] == 2
    assert m["max_carry_age_months"] == 1
    assert m["n_valid"] == 1


def test_timeline_rolling_rate_uses_trailing_window():
    # 40 months: first 20 valid, last 20 abstaining -> trailing 12/24/36 differ.
    specs = []
    for k in range(40):
        y, mo = 2018 + k // 12, k % 12 + 1
        if k < 20:
            specs.append((y, mo, "expansion", "valid"))
        else:
            specs.append((y, mo, None, "low_confidence"))
    m = metrics.regime_timeline_metrics(_chain(*specs))
    assert m["n_months"] == 40
    assert m["fresh_valid_rate"]["global"] == pytest.approx(20 / 40)
    # trailing 12 are all abstaining.
    assert m["fresh_valid_rate"]["rolling_12m"] == pytest.approx(0.0)
    # trailing 24 -> 4 valid of 24.
    assert m["fresh_valid_rate"]["rolling_24m"] == pytest.approx(4 / 24)
    # trailing 36 -> 16 valid of 36.
    assert m["fresh_valid_rate"]["rolling_36m"] == pytest.approx(16 / 36)


def test_timeline_fails_loud_on_duplicate_month():
    chain = _chain(
        (2021, 1, "expansion", "valid"),
        (2021, 1, "contraction", "valid"),  # duplicate as_of
    )
    with pytest.raises(ValueError, match="duplicate decision month"):
        metrics.regime_timeline_metrics(chain)


def test_timeline_out_of_order_input_is_sorted_not_double_counted():
    chain = _chain(
        (2021, 3, "expansion", "valid"),
        (2021, 1, "expansion", "valid"),
        (2021, 2, None, "low_confidence"),
    )
    m = metrics.regime_timeline_metrics(chain)
    assert m["first_as_of"] == "2021-01-31"
    assert m["last_as_of"] == "2021-03-31"
    assert m["max_carry_age_months"] == 1  # 2021-02 carries the 2021-01 seed


# --------------------------------------------------------------------------- #
# upside_capture_by_calendar_year                                            #
# --------------------------------------------------------------------------- #

def test_upside_capture_judges_only_up_years_with_division_guard():
    strategy = [(dt.date(2021, 1, 4), 1.0), (dt.date(2021, 12, 31), 1.10),
                (dt.date(2022, 1, 3), 1.10), (dt.date(2022, 12, 30), 1.045)]
    spy = [(dt.date(2021, 1, 4), 100.0), (dt.date(2021, 12, 31), 120.0),
           (dt.date(2022, 1, 3), 120.0), (dt.date(2022, 12, 30), 108.0)]
    out = metrics.upside_capture_by_calendar_year(strategy, spy)
    assert out["2021"]["spy_up"] is True
    assert out["2021"]["strategy_return"] == pytest.approx(0.10)
    assert out["2021"]["spy_return"] == pytest.approx(0.20)
    assert out["2021"]["upside_capture"] == pytest.approx(0.5)
    # SPY fell in 2022 -> not an upside-capture year; capture is None (guarded).
    assert out["2022"]["spy_up"] is False
    assert out["2022"]["upside_capture"] is None


def test_upside_capture_skips_years_with_too_few_points():
    strategy = [(dt.date(2021, 6, 1), 1.0)]  # single point -> no return
    spy = [(dt.date(2021, 6, 1), 100.0)]
    assert metrics.upside_capture_by_calendar_year(strategy, spy) == {}


def test_upside_capture_flat_spy_year_is_not_up():
    strategy = [(dt.date(2021, 1, 4), 1.0), (dt.date(2021, 12, 31), 1.05)]
    spy = [(dt.date(2021, 1, 4), 100.0), (dt.date(2021, 12, 31), 100.0)]  # 0% -> not up
    out = metrics.upside_capture_by_calendar_year(strategy, spy)
    assert out["2021"]["spy_up"] is False
    assert out["2021"]["upside_capture"] is None


# --------------------------------------------------------------------------- #
# Runner integration: timeline block ALWAYS present                          #
# --------------------------------------------------------------------------- #

def _fast_config():
    return runner.RunConfig(
        run_id="phase0q-timeline-test-0000",
        started_at="2026-07-11T00:00:00+00:00",
        finished_at="2026-07-11T00:00:01+00:00",
        harness_commit="0" * 40,
        candidates=(runner.SCENARIO_CANDIDATES[0],),
        cost_grid=(0, 5),
        primary_window=(dt.date(2019, 6, 1), dt.date(2020, 12, 31)),
        stress_windows=(
            {"window_id": "COVID_2020", "start": dt.date(2020, 2, 15),
             "end": dt.date(2020, 4, 30), "coverage": "full_basket"},
        ),
    )


@pytest.fixture(scope="module")
def fast_run():
    return runner.run_harness(PACK_DIR, _fast_config())


def test_gate_report_always_carries_timeline_block(fast_run):
    tl = fast_run["gate_report"]["timeline"]
    assert tl["reference_candidate_id"] == "baseline_current"
    m = tl["regime_timeline_metrics"]
    assert m["n_months"] > 0
    assert m["n_valid"] + m["n_low_confidence"] <= m["n_months"]
    assert set(m["quadrant_mix"]) == {"recovery", "expansion", "slowdown", "contraction"}
    assert 0.0 <= m["fresh_valid_rate"]["global"] <= 1.0
    assert isinstance(m["max_carry_age_months"], int)
    # upside capture is present (may be empty if no complete year) and well-formed.
    uc = tl["upside_capture_by_calendar_year"]
    for year, entry in uc.items():
        assert set(entry) == {"strategy_return", "spy_return", "spy_up", "upside_capture"}
        if entry["spy_up"]:
            assert entry["upside_capture"] is not None
        else:
            assert entry["upside_capture"] is None


def test_timeline_block_also_surfaced_on_run_dict(fast_run):
    assert fast_run["timeline"]["regime_timeline_metrics"]["n_months"] == \
        fast_run["gate_report"]["timeline"]["regime_timeline_metrics"]["n_months"]


def test_timeline_block_is_deterministic(fast_run):
    run2 = runner.run_harness(PACK_DIR, _fast_config())
    assert runner.canonical_json(fast_run["gate_report"]["timeline"]) == \
        runner.canonical_json(run2["gate_report"]["timeline"])
