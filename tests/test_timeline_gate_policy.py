"""TDD suite for the Tranche W2 proposed timeline-gate policy + advisory judge.

Covers:
  * the policy artifact is PROPOSED (never self-ratified): status
    proposed_not_ratified, ratified_by/decision_date null, self_ratification prohibited;
  * judge_timeline_gates min/max direction logic and the bull-year upside gate;
  * advisory vs gating MODES (proposed -> advisory/enforced=False; a ratified policy
    -> gating/enforced=True);
  * the runner attaches the judgment advisory-only and does NOT let it enter
    gates_overall_base_cost.

Network-free, DB-free for the policy + judge unit tests; the wiring test replays the
committed pack over a short fast window.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from harness.phase0q import runner

ROOT = Path(__file__).resolve().parents[1]
PACK_DIR = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"
POLICY_PATH = (ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_005"
               / "timeline_gate_policy.json")


def _block(rolling_36m, abstention, carry_age, same_run, upside=None):
    return {
        "regime_timeline_metrics": {
            "fresh_valid_rate": {"global": rolling_36m, "rolling_12m": rolling_36m,
                                 "rolling_24m": rolling_36m, "rolling_36m": rolling_36m},
            "max_abstention_streak_months": abstention,
            "max_carry_age_months": carry_age,
            "max_same_quadrant_run_months": same_run,
        },
        "upside_capture_by_calendar_year": upside or {},
    }


# --------------------------------------------------------------------------- #
# The artifact is RATIFIED by the quant_owner (2026-07-11) — recorded, not     #
# self-ratified: the ratification was ordered by the owner and this artifact   #
# records it with the same fields the phase0q_003 amendment convention uses.   #
# --------------------------------------------------------------------------- #

def test_policy_artifact_is_ratified_by_quant_owner():
    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    assert policy["artifact_type"] == "phase0q_timeline_gate_policy"
    assert policy["status"] == "ratified"
    assert policy["ratified_by"] == "quant_owner"
    assert policy["ratified_by_name"] == "Andrei Rachadel"
    assert policy["decision_date"] == "2026-07-11"
    # the self-ratification BAN stays on record: ratification came from the owner.
    assert policy["governance"]["self_ratification"] == "prohibited"
    assert policy["governance"]["A5"] == "blocked"
    assert policy["governance"]["runtime_activation"] is False
    # the five gate bounds are EXACTLY as proposed (ratified unchanged).
    assert policy["gates"] == {
        "min_fresh_valid_rate_36m": 0.40,
        "max_abstention_streak_months": 6,
        "max_carry_age_months": 3,
        "max_same_quadrant_run_months": 12,
        "min_upside_capture_bull_year": 0.35,
    }
    assert policy["gate_parameters"]["bull_year_spy_return_threshold"] == 0.15
    # it amends the phase0q_003 stress-gate amendment.
    assert "phase0q_003" in policy["amends"]["target"]


def test_policy_loads_via_runner_default_path():
    policy = runner.load_timeline_gate_policy()
    assert policy is not None
    assert policy["status"] == "ratified"


# --------------------------------------------------------------------------- #
# Advisory vs gating modes                                                    #
# --------------------------------------------------------------------------- #

def test_judge_is_gating_for_the_committed_ratified_policy():
    policy = runner.load_timeline_gate_policy()
    j = runner.judge_timeline_gates(_block(0.5, 3, 3, 10), policy)
    assert j["mode"] == "gating"
    assert j["gates_enforced"] is True
    assert j["policy_status"] == "ratified"


def test_judge_is_advisory_for_an_unratified_policy():
    proposed = {**runner.load_timeline_gate_policy(),
                "status": "proposed_not_ratified"}
    j = runner.judge_timeline_gates(_block(0.5, 3, 3, 10), proposed)
    assert j["mode"] == "advisory"
    assert j["gates_enforced"] is False
    assert j["policy_status"] == "proposed_not_ratified"


def test_judge_policy_absent_blocks_nothing():
    j = runner.judge_timeline_gates(_block(0.5, 3, 3, 10), None)
    assert j["policy_status"] == "policy_absent"
    assert j["gates_enforced"] is False
    assert j["overall_go"] is None


# --------------------------------------------------------------------------- #
# Gate direction + bull-year upside logic                                     #
# --------------------------------------------------------------------------- #

def test_all_gates_pass_when_within_bounds():
    policy = runner.load_timeline_gate_policy()
    j = runner.judge_timeline_gates(_block(0.50, 4, 3, 12), policy)
    for gate in ("min_fresh_valid_rate_36m", "max_abstention_streak_months",
                 "max_carry_age_months", "max_same_quadrant_run_months"):
        assert j["per_gate"][gate]["go"] is True, gate
    assert j["overall_go"] is True


@pytest.mark.parametrize("block,failing_gate", [
    (_block(0.30, 3, 3, 10), "min_fresh_valid_rate_36m"),   # 0.30 < 0.40
    (_block(0.50, 7, 3, 10), "max_abstention_streak_months"),  # 7 > 6
    (_block(0.50, 3, 4, 10), "max_carry_age_months"),        # 4 > 3
    (_block(0.50, 3, 3, 13), "max_same_quadrant_run_months"),  # 13 > 12
])
def test_each_gate_fails_when_out_of_bounds(block, failing_gate):
    policy = runner.load_timeline_gate_policy()
    j = runner.judge_timeline_gates(block, policy)
    assert j["per_gate"][failing_gate]["go"] is False
    assert j["overall_go"] is False


def test_bull_year_upside_gate_judges_only_up_years():
    policy = runner.load_timeline_gate_policy()
    # 2021 SPY +25% (bull), captured only 0.20 (< 0.35) -> fail; 2020 SPY +10% (not bull).
    upside = {
        "2020": {"spy_return": 0.10, "upside_capture": 0.10, "spy_up": True,
                 "strategy_return": 0.01, "full_year_coverage": True},
        "2021": {"spy_return": 0.25, "upside_capture": 0.20, "spy_up": True,
                 "strategy_return": 0.05, "full_year_coverage": True},
    }
    j = runner.judge_timeline_gates(_block(0.5, 3, 3, 10, upside), policy)
    g = j["per_gate"]["min_upside_capture_bull_year"]
    assert g["applicable"] is True
    assert g["bull_years"] == ["2021"]  # only the > +15% year is judged
    assert g["measured"] == pytest.approx(0.20)
    assert g["go"] is False


def test_bull_year_upside_gate_not_applicable_without_a_bull_year():
    policy = runner.load_timeline_gate_policy()
    upside = {"2022": {"spy_return": 0.05, "upside_capture": 0.9, "spy_up": True,
                       "strategy_return": 0.045, "full_year_coverage": True}}
    j = runner.judge_timeline_gates(_block(0.5, 3, 3, 10, upside), policy)
    g = j["per_gate"]["min_upside_capture_bull_year"]
    assert g["applicable"] is False
    assert g["go"] is True  # nothing to judge; does not vacuously fail


def test_bull_year_upside_gate_skips_partial_years():
    """A PARTIAL year whose partial-period SPY return clears the bull threshold must
    NOT be enforced as a bull year: the judge only considers full-calendar-year
    coverage, and surfaces what it excluded."""
    policy = runner.load_timeline_gate_policy()
    upside = {
        # partial (mid-year start): +18% over June..December — above the threshold,
        # but not a calendar-year figure. Its capture is None by construction.
        "2019": {"spy_return": 0.18, "upside_capture": None, "spy_up": True,
                 "strategy_return": 0.08, "full_year_coverage": False,
                 "coverage_reason": "starts_after_january"},
        # full bull year, healthy capture -> the only judged year, passes.
        "2021": {"spy_return": 0.25, "upside_capture": 0.60, "spy_up": True,
                 "strategy_return": 0.15, "full_year_coverage": True},
    }
    j = runner.judge_timeline_gates(_block(0.5, 3, 3, 10, upside), policy)
    g = j["per_gate"]["min_upside_capture_bull_year"]
    assert g["bull_years"] == ["2021"]         # 2019 excluded despite +18%
    assert g["excluded_partial_years"] == ["2019"]
    assert g["measured"] == pytest.approx(0.60)
    assert g["go"] is True


def test_bull_year_upside_gate_only_partial_years_is_not_applicable():
    policy = runner.load_timeline_gate_policy()
    upside = {"2019": {"spy_return": 0.18, "upside_capture": None, "spy_up": True,
                       "strategy_return": 0.08, "full_year_coverage": False,
                       "coverage_reason": "starts_after_january"}}
    j = runner.judge_timeline_gates(_block(0.5, 3, 3, 10, upside), policy)
    g = j["per_gate"]["min_upside_capture_bull_year"]
    assert g["applicable"] is False
    assert g["go"] is True
    assert g["excluded_partial_years"] == ["2019"]


# --------------------------------------------------------------------------- #
# Runner wiring: advisory-only, never in gates_overall_base_cost              #
# --------------------------------------------------------------------------- #

def _fast_config():
    return runner.RunConfig(
        run_id="phase0q-w2-test-0000",
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


def test_ratified_policy_makes_timeline_a_blocking_overall_gate(fast_run):
    """With the committed policy RATIFIED, the timeline judgment is GATING and is
    surfaced as a distinct ``timeline`` entry in gates_overall_base_cost — a real
    go/no_go, never a crash. The five metric gates are untouched."""
    report = fast_run["gate_report"]
    judgment = report["timeline"]["gate_judgment"]
    assert judgment["mode"] == "gating"
    assert judgment["gates_enforced"] is True
    assert judgment["policy_status"] == "ratified"
    # distinct blocking entry alongside the five metric gates.
    assert set(report["gates_overall_base_cost"]) == {
        "turnover", "drawdown", "volatility", "stress_windows", "out_of_sample",
        "timeline"}
    tl_gate = report["gates_overall_base_cost"]["timeline"]
    assert tl_gate["go_no_go"] == ("go" if judgment["overall_go"] else "no_go")
    assert tl_gate["policy_status"] == "ratified"
    assert tl_gate["phase0q_id"] == "open_macro_v03_phase0q_005"
    # per-gate judgments carry measured values (an honest no_go, not a crash).
    for gate, entry in judgment["per_gate"].items():
        assert "measured" in entry and "bound" in entry and "go" in entry
    # run-report governance pins remain untouched (the run itself is still
    # measured-pending-cloud-leg; ratifying the POLICY approves no RUN).
    assert report["approved"] is False
    assert report["governance"]["A5"] == "blocked"


def test_unratified_policy_stays_advisory_and_out_of_overall_gates(tmp_path, monkeypatch):
    """The gating flip is driven ONLY by the artifact's ratified status: with an
    unratified policy at the configured path, the judgment is advisory and the
    timeline entry never enters gates_overall_base_cost (the pre-ratification
    behaviour, kept testable)."""
    proposed = {**json.loads(POLICY_PATH.read_text(encoding="utf-8")),
                "status": "proposed_not_ratified"}
    path = tmp_path / "timeline_gate_policy.proposed.json"
    path.write_text(json.dumps(proposed), encoding="utf-8")
    monkeypatch.setattr(runner, "TIMELINE_GATE_POLICY_PATH", path)
    run = runner.run_harness(PACK_DIR, _fast_config())
    report = run["gate_report"]
    judgment = report["timeline"]["gate_judgment"]
    assert judgment["mode"] == "advisory"
    assert judgment["gates_enforced"] is False
    assert set(report["gates_overall_base_cost"]) == {
        "turnover", "drawdown", "volatility", "stress_windows", "out_of_sample"}
