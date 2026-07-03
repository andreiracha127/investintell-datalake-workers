from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DARK_ROOT = ROOT / "artifacts" / "a5" / "open_macro_v03_dark_launch_001"

REQUIRED_ARTIFACTS = {
    "dark_launch_manifest.json",
    "review_closure_record.json",
    "owners_assignment_record.json",
    "monitoring_thresholds_record.json",
    "refreshed_observability_metrics.json",
    "rollback_dry_run_record.json",
    "kill_switch_dry_run_record.json",
    "evidence_refresh_manifest.json",
    "no_activation_guard_report.json",
    "dark_launch_report.md",
}

PLACEHOLDERS = {"", "TODO", "TBD", "placeholder", "<pending>", "unassigned"}


def _json(name: str) -> dict[str, Any]:
    payload = json.loads((DARK_ROOT / name).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_required_dark_launch_artifacts_exist() -> None:
    missing = [name for name in sorted(REQUIRED_ARTIFACTS) if not (DARK_ROOT / name).is_file()]
    assert missing == []


REQUIRED_OWNER_ROLES = {
    "technical_owner",
    "quant_owner",
    "risk_owner",
    "operations_owner",
    "product_portfolio_owner",
    "final_approver",
}


def test_owners_assignment_names_every_role() -> None:
    owners = _json("owners_assignment_record.json")
    assignments = {entry["role"]: entry for entry in owners["assignments"]}

    assert set(assignments) == REQUIRED_OWNER_ROLES
    for entry in assignments.values():
        assert entry["owner"] not in PLACEHOLDERS
        assert entry["assigned_date"] not in PLACEHOLDERS
    assert owners["owners_real_names_recorded"] is True
    assert owners["activation_approvals_recorded"] is False


def test_rollback_and_kill_switch_dry_runs_are_recorded() -> None:
    for name, plan_ref in (
        ("rollback_dry_run_record.json", "rollback_execution_plan.md"),
        ("kill_switch_dry_run_record.json", "kill_switch_plan.json"),
    ):
        record = _json(name)
        assert record["status"] == "completed"
        assert record["operator"] not in PLACEHOLDERS
        assert record["date"] not in PLACEHOLDERS
        assert record["plan_reference"].endswith(plan_ref)
        assert record["steps_executed"]
        for step in record["steps_executed"]:
            assert step["outcome"] == "pass"
        assert record["runtime_activation"] is False
        assert record["activation_allowed"] is False


def test_dry_run_records_equal_regeneration_by_the_committed_executor() -> None:
    """The committed records must be exactly what the fail-loud executor verifies and
    writes today — a hand-edited record that no verification produced cannot survive."""
    from harness.dark_launch import execute_dry_runs as executor

    assert _json("rollback_dry_run_record.json") == executor.build_rollback_record()
    assert _json("kill_switch_dry_run_record.json") == executor.build_kill_switch_record()


def test_monitoring_thresholds_are_measured_and_complete() -> None:
    metrics = _json("refreshed_observability_metrics.json")
    thresholds = _json("monitoring_thresholds_record.json")

    assert metrics["measured"] is True
    assert metrics["latency_p95_ms"] > 0
    assert metrics["memory_peak_bytes"] > 0
    assert metrics["error_rate"] >= 0
    assert metrics["retry_rate"] >= 0

    slos = {slo["id"]: slo for slo in thresholds["slos"]}
    assert set(slos) == {"latency_slo", "memory_slo", "error_rate_slo", "retry_rate_slo"}
    for slo in slos.values():
        assert isinstance(slo["threshold"], (int, float))
        assert slo["status"] == "defined"
        assert slo["derivation"] not in PLACEHOLDERS
    assert slos["latency_slo"]["threshold"] >= metrics["latency_p95_ms"]
    assert slos["memory_slo"]["threshold"] >= metrics["memory_peak_bytes"]


def test_observability_round_really_measured_the_matrix() -> None:
    """The refreshed metrics must come from a real 8+8 host+container round: both legs
    present, every run sample recorded, aggregates equal to the recomputed per-leg
    worst case, and the SLOs equal to the plan's ceil(1.5 x measured) rule."""
    import math

    metrics = _json("refreshed_observability_metrics.json")
    thresholds = _json("monitoring_thresholds_record.json")

    assert metrics["environment_matrix"] == ["container", "host"]
    per_leg = metrics["per_leg"]
    for leg in ("host", "container"):
        assert per_leg[leg]["runs"] == 8
        assert len(per_leg[leg]["wall_ms_all_runs"]) == 8
        assert len(per_leg[leg]["memory_peak_bytes_all_runs"]) == 8
        assert per_leg[leg]["failed_runs"] == 0
        assert per_leg[leg]["memory_peak_bytes"] == max(
            per_leg[leg]["memory_peak_bytes_all_runs"])
        walls = sorted(per_leg[leg]["wall_ms_all_runs"])
        assert per_leg[leg]["latency_p95_ms"] == walls[
            max(0, math.ceil(0.95 * len(walls)) - 1)]
    assert metrics["latency_p95_ms"] == max(
        per_leg[leg]["latency_p95_ms"] for leg in per_leg)
    assert metrics["memory_peak_bytes"] == max(
        per_leg[leg]["memory_peak_bytes"] for leg in per_leg)
    assert metrics["runs_total"] == 16
    assert metrics["error_rate"] == 0.0

    slos = {slo["id"]: slo for slo in thresholds["slos"]}
    assert slos["latency_slo"]["threshold"] == math.ceil(1.5 * metrics["latency_p95_ms"])
    assert slos["memory_slo"]["threshold"] == math.ceil(1.5 * metrics["memory_peak_bytes"])
    assert slos["error_rate_slo"]["threshold"] == 0.0
    assert slos["retry_rate_slo"]["threshold"] == 0.0


from datetime import date
import re

DECISIONS = {"go", "no_go"}
REQUIRED_REVIEW_DOMAINS = {"technical", "quantitative", "risk", "operations"}
REQUIRED_QUANT_GATES = {"turnover", "drawdown", "volatility", "stress_windows", "out_of_sample"}
REQUIRED_TECHNICAL_GATES = {
    "artifact_immutability_reviewed",
    "no_formula_input_calibration_contract_changes",
    "no_pre_stage4_activation_surface",
    "guard_tests_reviewed",
}
REQUIRED_OPERATIONS_GATES = {
    "rollback_procedure_reviewed",
    "kill_switch_procedure_reviewed",
    "monitoring_thresholds_reviewed",
    "no_productive_side_effects_reviewed",
}
_TEMPLATE_PREFIXES = ("REAL_", "PATH_TO_")


def _assert_not_template_token(value: str, *, field: str) -> None:
    # Precise prefix match, not a broad all-caps rule, so legit ids like "GFC_2008" survive.
    assert not value.startswith(_TEMPLATE_PREFIXES), f"{field} is a template placeholder: {value!r}"


def _assert_iso_date(value: str, *, field: str) -> None:
    assert isinstance(value, str), field
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", value), field
    date.fromisoformat(value)


def _assert_non_placeholder_text(value: object, *, field: str, min_len: int = 8) -> None:
    assert isinstance(value, str), field
    assert value not in PLACEHOLDERS, field
    assert not value.startswith("<") and not value.endswith(">"), field
    _assert_not_template_token(value, field=field)
    assert len(value.strip()) >= min_len, field


def _assert_evidence_refs(refs: object, *, field: str) -> None:
    assert isinstance(refs, list) and refs, field
    for ref in refs:
        assert isinstance(ref, dict), field
        _assert_non_placeholder_text(ref.get("path"), field=f"{field}.path", min_len=10)
        _assert_non_placeholder_text(ref.get("note"), field=f"{field}.note", min_len=12)
        sha256 = ref.get("sha256")
        if sha256 is not None:
            assert isinstance(sha256, str) and re.fullmatch(r"[0-9a-f]{64}", sha256), field


def _assert_domain_review(review: dict[str, Any], *, expected_review: str) -> None:
    assert review["review"] == expected_review
    assert review["decision"] in DECISIONS
    assert review["decision"] == "go"  # positive path only; no_go stops before Phase 1
    _assert_non_placeholder_text(review["reviewer"], field=f"{expected_review}.reviewer")
    _assert_iso_date(review["date"], field=f"{expected_review}.date")
    _assert_non_placeholder_text(review["evidence_note"], field=f"{expected_review}.evidence_note", min_len=30)
    _assert_evidence_refs(review["evidence_refs"], field=f"{expected_review}.evidence_refs")


def _assert_gate_decision(gate: dict[str, Any], *, field: str) -> None:
    assert gate["decision"] in DECISIONS
    assert gate["decision"] == "go"  # positive path only
    _assert_iso_date(gate["date"], field=f"{field}.date")
    _assert_non_placeholder_text(gate["evidence_note"], field=f"{field}.evidence_note", min_len=30)
    _assert_evidence_refs(gate["evidence_refs"], field=f"{field}.evidence_refs")


def _assert_bounded_metric(measurements: dict[str, Any], observed_key: str, max_key: str, *, gate: str) -> None:
    observed, max_allowed = measurements[observed_key], measurements[max_key]
    assert isinstance(observed, (int, float)) and isinstance(max_allowed, (int, float)), gate
    assert max_allowed > 0, f"{gate}.{max_key} must be a real positive threshold, not a template 0.0"
    assert observed <= max_allowed, f"{gate}: observed {observed} exceeds max_allowed {max_allowed}"


def test_review_closure_records_go_no_go_date_and_evidence_for_all_domains() -> None:
    closure = _json("review_closure_record.json")
    reviews = {review["review"]: review for review in closure["reviews"]}

    assert set(reviews) == REQUIRED_REVIEW_DOMAINS
    for review_id in sorted(REQUIRED_REVIEW_DOMAINS):
        _assert_domain_review(reviews[review_id], expected_review=review_id)

    assert closure["all_reviews_recorded"] is True
    assert closure["all_reviews_go"] is True
    assert closure["final_decision"] == "go"
    assert closure["activation_path_status"] == "eligible_for_dark_launch_pr"
    assert closure["activation_allowed"] is False
    assert closure["runtime_activation"] is False


def test_quantitative_review_closes_all_pending_quant_gates() -> None:
    closure = _json("review_closure_record.json")
    reviews = {review["review"]: review for review in closure["reviews"]}
    gates = {gate["gate"]: gate for gate in reviews["quantitative"]["quantitative_gates"]}

    assert set(gates) == REQUIRED_QUANT_GATES
    for gate_id in sorted(REQUIRED_QUANT_GATES):
        gate = gates[gate_id]
        _assert_gate_decision(gate, field=f"quantitative.{gate_id}")
        _assert_non_placeholder_text(
            gate["acceptance_criteria"], field=f"quantitative.{gate_id}.acceptance_criteria", min_len=20
        )
        assert isinstance(gate["measurements"], dict) and gate["measurements"], gate_id

    _assert_bounded_metric(
        gates["turnover"]["measurements"], "observed_turnover", "max_allowed_turnover", gate="turnover"
    )
    _assert_bounded_metric(
        gates["drawdown"]["measurements"], "observed_max_drawdown", "max_allowed_drawdown", gate="drawdown"
    )
    _assert_bounded_metric(
        gates["volatility"]["measurements"], "observed_volatility", "max_allowed_volatility", gate="volatility"
    )

    stress_windows = gates["stress_windows"]["measurements"].get("windows")
    assert isinstance(stress_windows, list) and stress_windows
    for window in stress_windows:
        _assert_non_placeholder_text(window.get("window_id"), field="stress_windows.window_id", min_len=3)
        _assert_iso_date(window["start_date"], field="stress_windows.start_date")
        _assert_iso_date(window["end_date"], field="stress_windows.end_date")
        assert window["decision"] == "go"

    oos = gates["out_of_sample"]["measurements"]
    _assert_iso_date(oos["start_date"], field="out_of_sample.start_date")
    _assert_iso_date(oos["end_date"], field="out_of_sample.end_date")
    assert isinstance(oos.get("metrics"), dict) and oos["metrics"]


def test_technical_risk_and_operations_reviews_have_domain_gate_detail() -> None:
    closure = _json("review_closure_record.json")
    reviews = {review["review"]: review for review in closure["reviews"]}

    technical_gates = {gate["gate"]: gate for gate in reviews["technical"]["domain_gates"]}
    assert REQUIRED_TECHNICAL_GATES.issubset(set(technical_gates))
    for gate_id, gate in technical_gates.items():
        _assert_gate_decision(gate, field=f"technical.{gate_id}")

    risk = reviews["risk"]
    assert risk["risk_items_total"] == 13
    assert risk["risk_items_open"] == 0
    assert risk["risk_items_status_counts"]["unresolved"] == 0
    assert risk["risk_items_status_counts"]["activation_blocking"] == 0
    _assert_evidence_refs(risk["risk_register_refs"], field="risk.risk_register_refs")

    operations_gates = {gate["gate"]: gate for gate in reviews["operations"]["domain_gates"]}
    assert REQUIRED_OPERATIONS_GATES.issubset(set(operations_gates))
    for gate_id, gate in operations_gates.items():
        _assert_gate_decision(gate, field=f"operations.{gate_id}")


def test_evidence_refresh_pins_upstream_hashes() -> None:
    refresh = _json("evidence_refresh_manifest.json")

    assert refresh["a5_preflight_001_merge_commit"] == "10602998fda56d0d265e69314ee333a307923e51"
    assert len(refresh["controlled_activation_proposal_001_merge_commit"]) == 40
    assert refresh["hashes_reverified"] is True
    assert refresh["stale_artifacts_found"] == 0
    for entry in refresh["verified_bundles"]:
        assert entry["sha256"] not in PLACEHOLDERS
        assert len(entry["sha256"]) == 64


def test_dark_launch_artifacts_contain_no_activation_markers() -> None:
    from tests.test_controlled_activation_proposal import (
        FORBIDDEN_AUTOMATIC_ACTIVATION_COMMANDS,
    )

    for path in sorted(DARK_ROOT.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for marker in (
            "runtime_activation=true",
            "activation_allowed=true",
            "freeze_ready=true",
            "official_result=true",
            '"runtime_activation": true',
            '"activation_allowed": true',
            '"freeze_ready": true',
            '"official_result": true',
            "A5=unblocked",
            *FORBIDDEN_AUTOMATIC_ACTIVATION_COMMANDS,
        ):
            assert marker not in text, f"{path.name} contains {marker}"


def test_readiness_artifacts_equal_regeneration_by_the_committed_builder() -> None:
    """Signature-bearing artifacts must be exactly what the committed builder derives
    from committed evidence — a hand-edited closure or resolution cannot survive."""
    from harness.dark_launch import build_readiness_artifacts as builder

    assert _json("risk_register_resolution_record.json") == builder.build_risk_resolution()
    assert _json("review_closure_record.json") == builder.build_review_closure()
    assert _json("evidence_refresh_manifest.json") == builder.build_evidence_refresh()
    assert _json("no_activation_guard_report.json") == builder.build_guard_report()


def test_dark_launch_manifest_keeps_activation_blocked() -> None:
    manifest = _json("dark_launch_manifest.json")

    assert manifest["dark_launch_id"] == "open_macro_v03_dark_launch_001"
    assert manifest["controlled_activation_proposal_id"] == "open_macro_v03_controlled_activation_proposal_001"
    assert manifest["A3"] == "open_macro_v03"
    assert manifest["A4"] == "controlled_activation_proposal_prepared"
    assert manifest["target_state_after_this_pr"] == "dark_launch_ready"
    assert manifest["A5"] == "blocked"
    assert manifest["current_stage"] == "stage_1_dark_launch"
    assert manifest["runtime_activation"] is False
    assert manifest["activation_allowed"] is False
    assert manifest["freeze_ready"] is False
    assert manifest["official_result"] is False
    assert manifest["allocator_publish"] is False
    assert manifest["feature_flag_default"] is False
    assert manifest["db_write_mode"] == "none"
    assert manifest["production_endpoint_activation"] == "none"
    assert manifest["allowed_side_effects"] == []
    assert len(manifest["controlled_activation_proposal_001_merge_commit"]) == 40
