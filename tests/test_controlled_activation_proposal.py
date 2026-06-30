from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
PROPOSAL_ROOT = ROOT / "artifacts" / "a5" / "open_macro_v03_controlled_activation_proposal_001"

REQUIRED_ARTIFACTS = {
    "controlled_activation_proposal_manifest.json",
    "technical_review_record.json",
    "quantitative_review_record.json",
    "risk_review_record.json",
    "operations_review_record.json",
    "go_no_go_matrix.json",
    "approval_matrix.json",
    "staged_rollout_plan.json",
    "feature_flag_activation_policy.json",
    "monitoring_enforcement_policy.json",
    "kill_switch_plan.json",
    "rollback_execution_plan.md",
    "production_activation_checklist.json",
    "controlled_activation_proposal.md",
    "unresolved_risks_register.json",
    "evidence_map.json",
    "no_activation_guard_report.json",
}

FORBIDDEN_TRUE_FIELDS = {
    "runtime_activation",
    "freeze_ready",
    "activation_allowed",
    "official_result",
    "allocator_publish",
    "allow_allocator_publish",
    "feature_flag_default",
    "default",
    "production_default",
}

PLACEHOLDERS = {"", "TODO", "TBD", "placeholder", "<pending>"}


def _reject_duplicate_json_object_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise ValueError(f"duplicate JSON object key {key!r}")
        payload[key] = value
    return payload


def _reject_non_finite_json_constant(constant: str) -> None:
    raise ValueError(f"non-finite JSON constant {constant!r}")


def _loads_json(text: str) -> Any:
    return json.loads(
        text,
        object_pairs_hook=_reject_duplicate_json_object_keys,
        parse_constant=_reject_non_finite_json_constant,
    )


def _load_json(path: Path) -> Any:
    return _loads_json(path.read_text(encoding="utf-8"))


def _json(name: str) -> dict[str, Any]:
    payload = _load_json(PROPOSAL_ROOT / name)
    assert isinstance(payload, dict)
    return payload


def test_proposal_json_loader_rejects_duplicate_activation_keys() -> None:
    with pytest.raises(ValueError, match="duplicate JSON object key 'runtime_activation'"):
        _loads_json('{"runtime_activation": true, "runtime_activation": false}')


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_proposal_json_loader_rejects_non_finite_constants(constant: str) -> None:
    with pytest.raises(ValueError, match="non-finite JSON constant"):
        _loads_json(f'{{"value": {constant}}}')


def _text(name: str) -> str:
    return (PROPOSAL_ROOT / name).read_text(encoding="utf-8")


def _walk_json(value: Any) -> list[Any]:
    if isinstance(value, dict):
        values: list[Any] = []
        for key, item in value.items():
            values.append(key)
            values.extend(_walk_json(item))
        return values
    if isinstance(value, list):
        values = []
        for item in value:
            values.extend(_walk_json(item))
        return values
    return [value]


def test_required_controlled_activation_proposal_artifacts_exist() -> None:
    missing = [name for name in sorted(REQUIRED_ARTIFACTS) if not (PROPOSAL_ROOT / name).is_file()]

    assert missing == []


def test_controlled_activation_proposal_manifest_is_proposal_only() -> None:
    manifest = _json("controlled_activation_proposal_manifest.json")

    assert manifest["controlled_activation_proposal_id"] == "open_macro_v03_controlled_activation_proposal_001"
    assert manifest["status"] == "proposal_candidate"
    assert manifest["strategy"] == "open_macro_v03"
    assert manifest["a5_preflight_id"] == "open_macro_v03_a5_preflight_001"
    assert manifest["a5_preflight_001_merge_commit"] == "10602998fda56d0d265e69314ee333a307923e51"
    assert manifest["controlled_shadow_id"] == "open_macro_v03_controlled_shadow_001"
    assert manifest["calibration_id"] == "open_macro_v03_calibration_001"
    assert manifest["input_pack_id"] == "open_macro_v03_certified_input_pack_001"
    assert manifest["runtime_skeleton_id"] == "open_macro_v03_runtime_skeleton_001"
    assert manifest["external_executor_handshake_id"] == "open_macro_v03_external_executor_handshake_001"
    assert manifest["A3"] == "open_macro_v03"
    assert manifest["A4"] == "controlled_activation_proposal_prepared"
    assert manifest["A5"] == "blocked"
    assert manifest["runtime_activation"] is False
    assert manifest["freeze_ready"] is False
    assert manifest["activation_allowed"] is False
    assert manifest["activation_requested"] is True
    assert manifest["official_result"] is False
    assert manifest["allocator_publish"] is False
    assert manifest["db_write_mode"] == "none"
    assert manifest["production_endpoint_activation"] == "none"
    assert manifest["feature_flag_default"] is False
    assert manifest["proposal_only"] is True
    assert manifest["activation_effect_in_this_pr"] == "none"


def test_go_no_go_matrix_never_allows_a5_activation_in_this_pr() -> None:
    matrix = _json("go_no_go_matrix.json")

    assert matrix["proposal_only"] is True
    assert matrix["a5_activation"] == "no_go"
    assert matrix["final_decision"] == "no_go_pending_review"
    assert matrix["gates"]["a5_activation"] == "no_go"
    assert matrix["runtime_activation"] is False
    assert matrix["freeze_ready"] is False
    assert matrix["activation_allowed"] is False
    assert matrix["official_result"] is False
    for gate in ("technical_review", "quantitative_review", "risk_review", "operations_review"):
        assert matrix["gates"][gate] == "pending"


def test_approval_matrix_does_not_accept_invented_owners() -> None:
    approvals = _json("approval_matrix.json")

    assert approvals["owners_real_names_recorded"] is False
    assert approvals["activation_allowed"] is False
    for approval in approvals["approvals"]:
        assert approval["owner"] in {None, "unassigned"}
        assert approval["approval_status"] == "pending"
        assert approval["approval_evidence"] is None
        assert approval["timestamp"] is None
        assert approval["blocking"] is True


def test_formal_reviews_are_required_for_future_activation() -> None:
    technical = _json("technical_review_record.json")
    quantitative = _json("quantitative_review_record.json")
    risk = _json("risk_review_record.json")
    operations = _json("operations_review_record.json")

    technical_items = {item["id"]: item for item in technical["items"]}
    operations_items = {item["id"]: item for item in operations["items"]}

    assert technical["technical_review_recorded"] is False
    assert technical_items["technical_review_recorded"]["status"] == "pending"
    assert technical_items["technical_review_recorded"]["blocking"] is True
    assert quantitative["quantitative_review_recorded"] is False
    assert quantitative["reviewer_decision"] == "pending"
    assert risk["sign_off_status"] == "pending"
    assert operations["operations_review_recorded"] is False
    assert operations_items["operations_review_recorded"]["status"] == "pending"
    assert operations_items["operations_review_recorded"]["blocking"] is True


def test_evidence_map_has_no_empty_placeholders_and_records_required_reads() -> None:
    evidence = _json("evidence_map.json")

    assert evidence["runtime_activation"] is False
    assert evidence["activation_allowed"] is False
    assert evidence["files_read"]
    assert evidence["digests_found"]["a5_preflight_001_merge_commit"] == "10602998fda56d0d265e69314ee333a307923e51"
    assert "artifacts/a5/open_macro_v03_a5_preflight_001/a5_preflight_manifest.json" in evidence["files_read"]
    assert "tests/test_a5_preflight_readiness.py" in evidence["files_read"]
    assert "scripts/contract_bundle.py" in evidence["files_read"]
    assert evidence["items_blocking"]
    assert evidence["gaps"]

    for value in _walk_json(evidence):
        if isinstance(value, str):
            assert value.strip() == value
            assert value not in PLACEHOLDERS


def test_no_activation_guard_report_blocks_runtime_freeze_activation_and_official_result() -> None:
    report = _json("no_activation_guard_report.json")

    assert report["status"] == "pass_no_activation_effect"
    assert report["runtime_activation"] is False
    assert report["freeze_ready"] is False
    assert report["activation_allowed"] is False
    assert report["official_result"] is False
    assert report["production_endpoint_activation"] == "none"
    assert {check["status"] for check in report["checks"]} == {"pass"}


def test_new_proposal_artifacts_do_not_contain_forbidden_activation_true_values() -> None:
    violations: list[str] = []
    for path in PROPOSAL_ROOT.rglob("*"):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        forbidden_snippets = (
            '"runtime_activation": true',
            '"freeze_ready": true',
            '"activation_allowed": true',
            '"official_result": true',
            '"allow_allocator_publish": true',
            '"feature_flag_default": true',
            "runtime_activation: true",
            "freeze_ready: true",
            "activation_allowed: true",
            "activation_requested: true",
            "official_result: true",
            "allocator_publish: true",
            "allow_allocator_publish: true",
            "feature_flag_default: true",
            "approve_controlled_activation: true",
            "runtime_activation_allowed: true",
            "A5_unblocked: true",
            "runtime_activation=true",
            "freeze_ready=true",
            "activation_allowed=true",
            "official_result=true",
        )
        for snippet in forbidden_snippets:
            if snippet in text:
                violations.append(f"{path.relative_to(ROOT)} contains {snippet}")
        if path.suffix == ".json":
            _load_json(path)

    assert violations == []


def test_json_activation_fields_remain_false_or_none_for_proposal_package() -> None:
    for path in PROPOSAL_ROOT.glob("*.json"):
        payload = _load_json(path)
        if path.name == "controlled_activation_proposal_manifest.json":
            assert payload["activation_requested"] is True
        for key, value in _iter_json_items(payload):
            if key in FORBIDDEN_TRUE_FIELDS:
                assert value is not True, f"{path.name} has {key}=true"
            if key == "production_endpoint_activation":
                assert value == "none"


def _iter_json_items(value: Any) -> list[tuple[str, Any]]:
    if isinstance(value, dict):
        items: list[tuple[str, Any]] = []
        for key, item in value.items():
            items.append((key, item))
            items.extend(_iter_json_items(item))
        return items
    if isinstance(value, list):
        items = []
        for item in value:
            items.extend(_iter_json_items(item))
        return items
    return []


def test_activation_runbook_and_proposal_do_not_contain_automatic_activation_command() -> None:
    combined = "\n".join(
        [
            _text("controlled_activation_proposal.md"),
            _text("rollback_execution_plan.md"),
            (ROOT / "artifacts" / "a5" / "open_macro_v03_a5_preflight_001" / "activation_runbook.md").read_text(encoding="utf-8"),
        ]
    )

    forbidden_commands = (
        "railway up --detach",
        "kubectl apply",
        "open_macro_v03_runtime_activation=true",
        "set open_macro_v03_runtime_activation true",
        "activation_allowed=true",
        "freeze_ready=true",
    )
    for command in forbidden_commands:
        assert command not in combined


def test_staged_rollout_current_stage_is_proposal_only_with_no_productive_side_effects() -> None:
    plan = _json("staged_rollout_plan.json")

    assert plan["current_stage"] == "stage_0_proposal_only"
    assert plan["runtime_activation"] is False
    assert plan["activation_allowed"] is False
    stage_zero = {stage["id"]: stage for stage in plan["stages"]}["stage_0_proposal_only"]
    assert stage_zero["allowed_side_effects"] == ["documentation artifacts", "governance tests"]


def test_monitoring_and_kill_switch_keep_activation_blocked_when_pending() -> None:
    monitoring = _json("monitoring_enforcement_policy.json")
    kill_switch = _json("kill_switch_plan.json")
    checklist = _json("production_activation_checklist.json")
    checks = {check["id"]: check for check in checklist["checks"]}

    assert monitoring["runtime_activation"] is False
    assert monitoring["activation_allowed"] is False
    assert any(slo["status"] == "pending" for slo in monitoring["slos"])
    assert kill_switch["runtime_activation"] is False
    assert kill_switch["activation_allowed"] is False
    assert kill_switch["test_status"] == "pending_operator_dry_run"
    assert kill_switch["owner"] == "unassigned"
    assert checks["kill_switch_dry_run"]["status"] == "pending"
    assert checks["kill_switch_dry_run"]["blocking"] is True
