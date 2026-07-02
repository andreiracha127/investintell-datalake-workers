from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PHASE0Q_ROOT = ROOT / "artifacts" / "quant" / "open_macro_v03_phase0q_001"

REQUIRED_ARTIFACTS = {
    "phase0q_manifest.json",
    "data_discovery_report.json",
    "metric_definitions.json",
    "stress_oos_policy.json",
    "scenario_grid.json",
    "threshold_candidate_report.json",
    "quantitative_gate_report.candidate.json",
    "lean_harness_spec.md",
    "phase0q_report.md",
}

REQUIRED_GATES = {"turnover", "drawdown", "volatility", "stress_windows", "out_of_sample"}
ALLOWED_RECOMMENDATIONS = {"go_candidate", "no_go_pending_data", "no_go_pending_metric_harness"}
REQUIRED_PROFILES = ["conservative", "base", "aggressive"]
MAX_TYPE_THRESHOLD_KEYS = (
    "max_one_way_turnover_annualized",
    "max_drawdown",
    "max_annualized_volatility",
    "max_fold_volatility_deviation",
    "max_fold_mdd_deviation",
)


def _json(name: str) -> dict[str, Any]:
    payload = json.loads((PHASE0Q_ROOT / name).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_required_phase0q_artifacts_exist() -> None:
    missing = [name for name in sorted(REQUIRED_ARTIFACTS) if not (PHASE0Q_ROOT / name).is_file()]
    assert missing == []


def test_phase0q_manifest_is_candidate_only_and_keeps_activation_blocked() -> None:
    manifest = _json("phase0q_manifest.json")

    assert manifest["phase0q_id"] == "open_macro_v03_phase0q_001"
    assert manifest["A3"] == "open_macro_v03"
    assert manifest["A5"] == "blocked"
    assert manifest["status"] == "candidate_not_approved"
    assert manifest["recommendation"] in ALLOWED_RECOMMENDATIONS
    assert manifest["runtime_activation"] is False
    assert manifest["activation_allowed"] is False
    assert manifest["freeze_ready"] is False
    assert manifest["official_result"] is False
    assert manifest["allocator_publish"] is False
    assert manifest["feature_flag_default"] is False
    assert manifest["db_write_mode"] == "none"
    assert manifest["production_endpoint_activation"] == "none"
    assert len(manifest["controlled_activation_proposal_001_merge_commit"]) == 40


def test_candidate_gate_report_blocks_all_gates_pending_harness() -> None:
    report = _json("quantitative_gate_report.candidate.json")
    gates = {gate["gate"]: gate for gate in report["gates"]}

    assert set(gates) == REQUIRED_GATES
    assert report["approved"] is False
    assert report["status"] == "candidate_not_approved"
    assert report["overall_recommendation"] in ALLOWED_RECOMMENDATIONS
    assert report["overall_recommendation"] != "go_candidate"
    for gate_id, gate in gates.items():
        assert gate["status"] == "no_go_pending_metric_harness", gate_id
        assert gate["missing"], gate_id
        assert gate["evidence_refs"], gate_id
    assert "BLOCKED" in report["task2_gate_effect"]


def test_threshold_candidates_are_unapproved_positive_and_profile_monotonic() -> None:
    report = _json("threshold_candidate_report.json")
    profiles = {entry["profile"]: entry for entry in report["profiles"]}

    assert report["approved"] is False
    assert report["approval_required_from"] == "quant_owner"
    assert report["status"] == "candidate_not_approved"
    assert list(profiles) == REQUIRED_PROFILES
    assert report["recommended_profile_for_first_activation"] in profiles

    for name, entry in profiles.items():
        for key in MAX_TYPE_THRESHOLD_KEYS:
            assert isinstance(entry[key], (int, float)) and entry[key] > 0, f"{name}.{key}"
        assert entry["min_worst_5d_return"] < 0, name
        assert len(entry["derivation"].strip()) >= 30, name

    for key in MAX_TYPE_THRESHOLD_KEYS:
        assert profiles["conservative"][key] <= profiles["base"][key] <= profiles["aggressive"][key], key
    assert (
        profiles["conservative"]["min_worst_5d_return"]
        >= profiles["base"]["min_worst_5d_return"]
        >= profiles["aggressive"]["min_worst_5d_return"]
    )


def test_data_discovery_pins_real_findings() -> None:
    discovery = _json("data_discovery_report.json")
    conclusion = discovery["conclusion"]

    assert conclusion["db_history_sufficient_for_quant_gates"] is True
    assert conclusion["repo_pack_sufficient_for_quant_gates"] is False
    assert conclusion["historical_certified_input_pack_exists"] is False
    assert conclusion["metric_harness_exists"] is False
    assert discovery["db_findings"]["access_mode"] == "read_only_select"
    assert discovery["db_findings"]["service_id"] == "t83f4np6x4"
    tables = {t["table"]: t for t in discovery["db_findings"]["tables"]}
    assert tables["eod_prices"]["min_date"] == "1962-01-02"
    assert tables["macro_data"]["min_date"] == "1959-01-01"
    assert "baseline_distance" in discovery["harness_findings"]["turnover_proxy_definition"]
    assert "MUST NOT" in discovery["harness_findings"]["turnover_proxy_definition"]


def test_metric_definitions_disqualify_turnover_proxy() -> None:
    definitions = _json("metric_definitions.json")

    assert definitions["status"] == "candidate_not_approved"
    turnover = definitions["metrics"]["turnover"]
    assert "MUST NOT" in turnover["explicit_prohibition"]
    assert set(definitions["metrics"]) == {
        "turnover",
        "max_drawdown",
        "annualized_volatility",
        "stress_window_behavior",
        "out_of_sample_stability",
    }


def test_stress_oos_policy_has_real_windows_and_folds() -> None:
    policy = _json("stress_oos_policy.json")
    windows = {w["window_id"]: w for w in policy["stress_windows"]}

    assert {"COVID_2020", "INFLATION_SHOCK_2022", "SVB_2023", "Q4_2018"} <= set(windows)
    for window in windows.values():
        assert window["start_date"] < window["end_date"]
        assert window["coverage"] in {"full_series", "reduced_coverage"}
    full_series = [w for w in windows.values() if w["coverage"] == "full_series"]
    assert len(full_series) >= 4
    oos = policy["out_of_sample"]
    assert oos["method"] == "rolling_walk_forward"
    assert oos["train_months"] > 0 and oos["test_months"] > 0
    assert policy["stress_acceptance"]["decision_coverage_min"] == 1.0


def test_phase0q_artifacts_contain_no_activation_or_approval_markers() -> None:
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
        '"status": "go"',
    )
    for path in sorted(PHASE0Q_ROOT.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for marker in forbidden:
            assert marker not in text, f"{path.name} contains {marker}"
