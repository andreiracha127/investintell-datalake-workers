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
