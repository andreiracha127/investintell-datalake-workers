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
