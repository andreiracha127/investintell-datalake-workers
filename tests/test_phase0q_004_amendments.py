"""Pins for artifacts/quant/open_macro_v03_phase0q_004 (OOS semantics + sleeve decision).

The phase0q_004 package records two quant_owner decisions (2026-07-03) and the
re-judgment they produce, derived ENTIRELY from already-committed evidence:

  * the OOS stability statistic becomes a stress-overlap jackknife (at most ONE fold —
    the argmax deviation — excluded, and only when it overlaps a GO full_series stress
    window); bounds unchanged;
  * compressed_50 becomes the candidate sleeve (baseline_100 retained as reference);
  * quantitative_gate_judgment.phase0q_004 judges the compressed_50 sleeve on the
    committed grid + the amended OOS semantics, superseding phase0q_003 traceably.

The strongest pin here is REGENERATION: each committed artifact must equal, byte for
byte, what the committed generator derives from the committed sources. Everything stays
candidate_not_approved / A5 blocked; Task 2 stays blocked pending the threshold sign-off.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from harness.phase0q import generate_phase0q_004 as gen

ROOT = Path(__file__).resolve().parents[1]
P004 = ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_004"

REQUIRED_ARTIFACTS = {
    "oos_dispersion_semantics_amendment.json",
    "sleeve_selection_record.json",
    "quantitative_gate_judgment.phase0q_004.json",
}


def _json(name: str) -> dict[str, Any]:
    payload = json.loads((P004 / name).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


@pytest.fixture(scope="module")
def sources():
    fold_report = gen._load(gen.GRID_DIR / "oos_fold_report.json")
    grid = gen._load(gen.GRID_DIR / "grid_results.json")
    stress_windows = gen.go_stress_windows()
    return fold_report, grid, stress_windows


def test_required_phase0q_004_artifacts_exist() -> None:
    missing = [n for n in sorted(REQUIRED_ARTIFACTS) if not (P004 / n).is_file()]
    assert missing == []
    extra = sorted(p.name for p in P004.rglob("*") if p.is_file()
                   and p.name not in REQUIRED_ARTIFACTS)
    assert extra == []


def test_committed_artifacts_equal_regeneration_from_committed_sources(sources) -> None:
    fold_report, grid, stress_windows = sources
    amendment = gen.build_amendment(fold_report, stress_windows)
    assert _json("oos_dispersion_semantics_amendment.json") == amendment
    assert _json("sleeve_selection_record.json") == gen.build_sleeve_record(grid)
    assert _json("quantitative_gate_judgment.phase0q_004.json") == gen.build_judgment(
        grid, amendment)


def test_jackknife_excludes_at_most_the_single_argmax_stress_overlap_fold(sources) -> None:
    fold_report, _grid, stress_windows = sources
    amendment = _json("oos_dispersion_semantics_amendment.json")

    assert amendment["bounds_changed"] is False
    assert amendment["bounds"] == {
        "max_fold_mdd_deviation": 0.08,
        "max_fold_volatility_deviation": 0.05,
        "fold_absolute_max_drawdown": 0.25,
        "fold_absolute_max_volatility": 0.12,
    }
    considered = {w["window_id"] for w in amendment["go_stress_windows_considered"]}
    assert considered == {"COVID_2020", "INFLATION_SHOCK_2022", "SVB_2023", "Q4_2018"}

    for variant, measured in amendment["re_measured"].items():
        for stat_key in ("mdd_stability", "volatility_stability"):
            stat = measured[stat_key]
            folds = fold_report["variants"][variant]["folds"]
            # never more than one exclusion, and only ever the argmax fold
            if stat["excluded_fold_index"] is not None:
                assert stat["excluded_fold_index"] == stat["all_folds"]["argmax_fold_index"]
                assert stat["eligible_folds"]["count"] == len(folds) - 1
                assert stat["excluded_fold_stress_overlap"], variant
            else:
                assert stat["eligible_folds"]["count"] == len(folds)
        # 2022 inflation shock is the known overlap on both statistics, both sleeves
        assert measured["mdd_stability"]["excluded_fold_stress_overlap"] == [
            "INFLATION_SHOCK_2022"]
        assert measured["verdict"] == "go_candidate"


def test_sleeve_selection_matches_committed_grid_numbers() -> None:
    record = _json("sleeve_selection_record.json")
    grid = gen._load(gen.GRID_DIR / "grid_results.json")

    assert record["selected_sleeve"] == "compressed_50"
    assert record["reference_sleeve_retained"] == "baseline_100"
    assert record["decided_by"] == "Andrei Rachadel"
    assert record["decided_by_role"] == "quant_owner"
    assert record["approved"] is False

    for variant in ("baseline_100", "compressed_50"):
        assert record["comparison_at_base_cost"][variant] == (
            grid["variants"][variant]["cost_grid"]["by_cost_bps"]["5"])
    assert record["candidate_stress_windows"] == (
        grid["variants"]["compressed_50"]["stress_windows"])


def test_judgment_is_go_candidate_on_compressed_50_and_supersedes_003() -> None:
    judgment = _json("quantitative_gate_judgment.phase0q_004.json")

    assert judgment["judged_sleeve"] == "compressed_50"
    assert judgment["overall_recommendation"] == "go_candidate"
    assert judgment["approved"] is False
    assert judgment["approval_required_from"] == "quant_owner"
    assert judgment["status"] == "candidate_not_approved"
    assert "phase0q_003" in judgment["supersedes"]
    assert "BLOCKED" in judgment["task2_gate_effect"]
    assert judgment["execution_legs"] == {
        "local_python_pure": "complete",
        "qc_research_object_store": "reproduced",
    }

    gates = judgment["gates"]
    assert gates["turnover"]["verdict"] == "pass_candidate_under_reference_sleeve_policy"
    assert gates["turnover"]["measured"] == 1.027076034777
    assert gates["turnover"]["measured"] <= gates["turnover"][
        "reference_sleeve_turnover_candidate_bound"]
    assert gates["drawdown"]["verdict"] == "go"
    assert gates["drawdown"]["measured"] == 0.15095367724
    assert gates["volatility"]["verdict"] == "go"
    assert gates["volatility"]["measured"] == 0.073803795385
    assert gates["stress"]["verdict"] == "go"
    assert all(w["go"] is True for w in gates["stress"]["windows"].values())
    assert set(gates["stress"]["windows"]) == {
        "COVID_2020", "INFLATION_SHOCK_2022", "SVB_2023", "Q4_2018"}
    assert gates["out_of_sample"]["verdict"] == "go_candidate"


FORBIDDEN_TRUE_KEYS = ("runtime_activation", "activation_allowed", "allocator_publish",
                       "official_result", "freeze_ready", "approved")


def _walk(node):
    if isinstance(node, dict):
        for key, value in node.items():
            yield key, value
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def test_phase0q_004_artifacts_keep_activation_blocked() -> None:
    for path in sorted(P004.rglob("*.json")):
        for key, value in _walk(json.loads(path.read_text(encoding="utf-8"))):
            if key in FORBIDDEN_TRUE_KEYS:
                assert value is not True, f"{path.name}: {key} must never be true"
            if key == "A5":
                assert str(value).strip().lower() == "blocked"
            if key == "db_write_mode":
                assert str(value).strip().lower() == "none"
