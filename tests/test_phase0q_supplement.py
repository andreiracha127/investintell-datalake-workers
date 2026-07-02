from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SUPP_ROOT = ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_002"

REQUIRED_ARTIFACTS = {
    "phase0q_002_manifest.json",
    "reference_sleeve_proposal.json",
    "harness_window_policy.json",
}

SLEEVE_TICKERS = {"SPY", "TLT", "TIP", "GLD", "DBC", "SHY"}
GROWTH_SERIES = {"INDPRO", "PCEC96", "PAYEMS", "ACOGNO"}
INFLATION_SERIES = {"CPILFESL", "PPIFIS", "AHETPI", "MICH"}


def _json(name: str) -> dict[str, Any]:
    payload = json.loads((SUPP_ROOT / name).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_required_phase0q_supplement_artifacts_exist() -> None:
    missing = [name for name in sorted(REQUIRED_ARTIFACTS) if not (SUPP_ROOT / name).is_file()]
    assert missing == []


def test_supplement_manifest_is_candidate_only_and_keeps_activation_blocked() -> None:
    manifest = _json("phase0q_002_manifest.json")

    assert manifest["phase0q_supplement_id"] == "open_macro_v03_phase0q_002"
    assert manifest["phase0q_id"] == "open_macro_v03_phase0q_001"
    assert manifest["A5"] == "blocked"
    assert manifest["status"] == "candidate_not_approved"
    assert manifest["runtime_activation"] is False
    assert manifest["activation_allowed"] is False
    assert manifest["freeze_ready"] is False
    assert manifest["official_result"] is False
    assert manifest["allocator_publish"] is False
    assert manifest["db_write_mode"] == "none"
    assert manifest["production_endpoint_activation"] == "none"


def test_reference_sleeve_weights_are_complete_and_constraint_consistent() -> None:
    sleeve = _json("reference_sleeve_proposal.json")

    assert sleeve["approved"] is False
    assert sleeve["approval_required_from"] == "quant_owner"
    assert sleeve["status"] == "candidate_not_approved"
    assert {inst["ticker"] for inst in sleeve["instruments"]} == SLEEVE_TICKERS
    assert sleeve["cost_model"]["one_way_cost_bps"] > 0

    constraints = sleeve["constraint_baselines"]
    risk_assets = set(constraints["risk_assets_definition"])
    defensive_assets = set(constraints["defensive_assets_definition"])
    risk_cap = constraints["risk_cap_baseline"]
    defensive_floor = constraints["defensive_floor_baseline"]
    assert 0 < defensive_floor < risk_cap < 1

    quadrants = sleeve["per_quadrant_baseline_weights"]
    assert set(quadrants) == {
        "Q1_growth_up_inflation_down",
        "Q2_growth_up_inflation_up",
        "Q3_growth_down_inflation_up",
        "Q4_growth_down_inflation_down",
    }
    for name, weights in quadrants.items():
        assert set(weights) == SLEEVE_TICKERS, name
        assert math.isclose(sum(weights.values()), 1.0, abs_tol=1e-9), name
        assert all(w >= 0 for w in weights.values()), name
        risk_exposure = sum(weights[t] for t in risk_assets)
        defensive_exposure = sum(weights[t] for t in defensive_assets)
        assert risk_exposure <= risk_cap, f"{name}: risk {risk_exposure} > cap {risk_cap}"
        assert defensive_exposure >= defensive_floor, (
            f"{name}: defensive {defensive_exposure} < floor {defensive_floor}"
        )


def test_window_policy_pins_pit_authority_and_measured_windows() -> None:
    policy = _json("harness_window_policy.json")

    authority = policy["decision_input_authority"]
    assert authority["table"] == "macro_observation_vintage"
    assert set(authority["series_growth"]) == GROWTH_SERIES
    assert set(authority["series_inflation"]) == INFLATION_SERIES

    windows = policy["windows"]
    assert windows["primary_full_basket"]["start"] == "2014-03-01"
    assert windows["primary_full_basket"]["end"] == "2026-06-30"
    assert windows["primary_full_basket"]["approx_years"] > 12

    coverage = policy["measured_vintage_coverage"]
    assert coverage["per_series_first_vintage"]["PPIFIS"] == "2014-02-19"
    assert coverage["access_mode"] == "read_only_select"

    classification = policy["stress_window_classification"]
    assert set(classification["full_basket"]) == {"COVID_2020", "INFLATION_SHOCK_2022", "SVB_2023", "Q4_2018"}
    assert set(classification["reduced_coverage"]) == {"GFC_2008", "TAPER_2013"}

    oos = policy["out_of_sample"]
    assert oos["method"] == "rolling_walk_forward"
    assert oos["expected_folds_in_primary_window"] == 9
    assert "supersedes" not in policy or policy.get("status") == "candidate_not_approved"
    assert "correction_note" in policy and len(policy["correction_note"]) > 100


def test_phase0q_supplement_contains_no_activation_or_approval_markers() -> None:
    forbidden = (
        "runtime_activation=true",
        "activation_allowed=true",
        "freeze_ready=true",
        "official_result=true",
        '"runtime_activation": true',
        '"activation_allowed": true',
        '"freeze_ready": true',
        '"official_result": true',
        '"approved": true',
        "A5=unblocked",
    )
    for path in sorted(SUPP_ROOT.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for marker in forbidden:
            assert marker not in text, f"{path.name} contains {marker}"
