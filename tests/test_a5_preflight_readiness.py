from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
A5_ROOT = ROOT / "artifacts" / "a5" / "open_macro_v03_a5_preflight_001"
DOC = ROOT / "docs" / "a5" / "open_macro_v03_a5_preflight_001.md"
GITHUB_ACTIONS_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"

REQUIRED_A5_ARTIFACTS = {
    "a5_preflight_manifest.json",
    "evidence_index.json",
    "promotion_gate_matrix.json",
    "technical_review_checklist.json",
    "quantitative_review_checklist.json",
    "risk_review_checklist.json",
    "operations_review_checklist.json",
    "production_readiness_checklist.json",
    "feature_flag_policy.json",
    "monitoring_slo_policy.json",
    "activation_runbook.md",
    "rollback_runbook.md",
    "go_no_go_memo.md",
    "unresolved_risks_register.json",
}

ALLOWED_CHECKLIST_STATUSES = {"pass", "fail", "pending", "not_applicable"}
FORBIDDEN_ACTIVATION_STRINGS = (
    "runtime_activation" + "=true",
    "freeze_ready" + "=true",
    "official_result" + "=true",
    "A5=un" + "blocked",
    "A5 un" + "blocked",
)


def _json(name: str) -> dict[str, Any]:
    return json.loads((A5_ROOT / name).read_text(encoding="utf-8"))


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _git_show_bytes(source_commit: str, artifact_path: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{source_commit}:{artifact_path}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    assert result.returncode == 0, f"{artifact_path} is unavailable at {source_commit}"
    return result.stdout


def _walk_values(value: Any) -> list[Any]:
    if isinstance(value, dict):
        values: list[Any] = []
        for key, item in value.items():
            values.append(key)
            values.extend(_walk_values(item))
        return values
    if isinstance(value, list):
        values = []
        for item in value:
            values.extend(_walk_values(item))
        return values
    return [value]


def test_required_a5_preflight_artifacts_exist() -> None:
    missing = [name for name in sorted(REQUIRED_A5_ARTIFACTS) if not (A5_ROOT / name).is_file()]

    assert missing == []
    assert DOC.is_file()


def test_a5_preflight_manifest_keeps_governance_inert() -> None:
    manifest = _json("a5_preflight_manifest.json")

    assert manifest["a5_preflight_id"] == "open_macro_v03_a5_preflight_001"
    assert manifest["status"] == "readiness_candidate"
    assert manifest["strategy"] == "open_macro_v03"
    assert manifest["controlled_shadow_id"] == "open_macro_v03_controlled_shadow_001"
    assert manifest["controlled_shadow_001_merge_commit"] == "6fb22079542d2fae5fd63f2088a41f76b8bde8c9"
    assert manifest["A3"] == "open_macro_v03"
    assert manifest["A4"] == "controlled_shadow_validated"
    assert manifest["target_state_after_this_pr"] == "A5_preflight_readiness_prepared"
    assert manifest["A5"] == "blocked"
    assert manifest["runtime_activation"] is False
    assert manifest["freeze_ready"] is False
    assert manifest["activation_allowed"] is False
    assert manifest["activation_requested"] is False
    assert manifest["official_result"] is False
    assert manifest["allocator_impact"] == "none"
    assert manifest["db_write_mode"] == "none"
    assert manifest["backend_execution"] == "none"
    assert manifest["production_endpoint_activation"] == "none"
    assert manifest["formula_changes"] == "none"
    assert manifest["input_pack_changes"] == "none"
    assert manifest["calibration_pack_changes"] == "none"
    assert manifest["contract_v1_changes"] == "none"


def test_evidence_index_references_existing_artifacts_without_placeholders() -> None:
    evidence = _json("evidence_index.json")
    items = evidence["items"]

    assert evidence["overall_status"] == "present_with_missing_optional"
    assert evidence["blocking"] is False
    assert evidence["critical_missing_count"] == 0
    assert evidence["missing_blocking_count"] == 0
    assert evidence["essential_evidence_complete"] is True
    assert {item["evidence_id"] for item in items if item["status"] == "missing_optional"} == {
        "input_pack_certified_manifest_expected_artifact_path"
    }
    assert [item for item in items if item["status"] == "missing_blocking"] == []

    required_ids = {
        "input_pack_certified_manifest_effective",
        "calibration_manifest",
        "calibration_config",
        "calibration_output_manifest",
        "shadow_readiness_manifest",
        "shadow_pilot_manifest",
        "external_executor_handshake_manifest",
        "runtime_skeleton_manifest",
        "controlled_shadow_manifest",
        "controlled_shadow_reproducibility_report",
        "controlled_shadow_no_side_effects_report",
        "contract_bundle_manifest",
    }
    assert required_ids.issubset({item["evidence_id"] for item in items})

    for item in items:
        assert item["evidence_id"]
        assert item["status"] in {"present", "missing_optional", "missing_blocking"}
        if item["status"] == "present":
            assert item["verified"] is True
            assert item["artifact_path"]
            assert (ROOT / item["artifact_path"]).is_file()
            assert item["sha256"] and len(item["sha256"]) == 64
            artifact_bytes = _git_show_bytes(item["source_commit"], item["artifact_path"])
            assert hashlib.sha256(artifact_bytes).hexdigest() == item["sha256"]
        else:
            assert item["blocking"] is False
            assert item["missing_reason"]

    for value in _walk_values(evidence):
        if isinstance(value, str):
            assert value.strip() == value
            assert value not in {"", "TODO", "TBD", "<pending>", "placeholder"}


def test_post_merge_validation_block_is_controlled_shadow_green() -> None:
    validation = _json("evidence_index.json")["post_merge_validation"]

    assert validation == {
        "A5": "blocked",
        "aggregate_shadow_calibration_gate": "628 passed",
        "contract_bundle_verify": "ok",
        "controlled_shadow_id": "open_macro_v03_controlled_shadow_001",
        "controlled_shadow_tests": "122 passed",
        "freeze_ready": False,
        "official_result": False,
        "remote_ci": "PASS",
        "remote_ci_commit": "6fb22079542d2fae5fd63f2088a41f76b8bde8c9",
        "repeatability_matrix": {
            "host_run_count": 4,
            "mismatch_count": 0,
            "ok": True,
            "run_count": 8,
        },
        "runtime_activation": False,
        "validated": True,
        "verify_calibration_artifacts": "ok",
        "verify_input_pack": "ok",
    }


def test_promotion_matrix_requires_reviews_before_activation() -> None:
    matrix = _json("promotion_gate_matrix.json")

    assert matrix["A5"] == "blocked"
    assert matrix["runtime_activation"] is False
    assert matrix["freeze_ready"] is False
    assert matrix["official_result"] is False
    assert matrix["activation_allowed"] is False
    assert matrix["separate_activation_pr_required"] is True

    technical_ids = {gate["id"] for gate in matrix["technical_gates"]}
    assert {
        "input_pack_verified",
        "calibration_verified",
        "controlled_shadow_verified",
        "repeatability_mismatch_count_zero",
        "no_backend_docker_subprocess",
    }.issubset(technical_ids)

    risk_gates = {gate["id"]: gate for gate in matrix["risk_governance_gates"]}
    for gate_id in (
        "technical_review_recorded",
        "quantitative_review_recorded",
        "risk_review_recorded",
        "operations_review_recorded",
    ):
        assert risk_gates[gate_id]["status"] == "pending"
        assert risk_gates[gate_id]["blocking"] is True
    assert risk_gates["activation_allowed_false"]["status"] == "pass"
    assert risk_gates["freeze_ready_false"]["status"] == "pass"


def test_checklists_have_required_shape_and_pending_human_reviews() -> None:
    checklists = [
        _json("technical_review_checklist.json"),
        _json("quantitative_review_checklist.json"),
        _json("risk_review_checklist.json"),
        _json("operations_review_checklist.json"),
        _json("production_readiness_checklist.json"),
    ]

    for checklist in checklists:
        assert checklist["A5"] == "blocked"
        assert checklist["runtime_activation"] is False
        assert checklist["items"]
        for item in checklist["items"]:
            assert set(item) == {
                "id",
                "description",
                "status",
                "evidence_path",
                "blocking",
                "owner_or_reviewer",
                "notes",
            }
            assert item["status"] in ALLOWED_CHECKLIST_STATUSES
            assert item["evidence_path"]
            assert isinstance(item["blocking"], bool)
            assert item["owner_or_reviewer"]

    assert _json("technical_review_checklist.json")["technical_review_recorded"] is False
    assert _json("quantitative_review_checklist.json")["technical_and_quantitative_review_recorded"] is False
    assert _json("quantitative_review_checklist.json")["quantitative_reviewer_signoff"] == "pending"
    assert _json("risk_review_checklist.json")["risk_reviewer_signoff"] == "pending"
    assert _json("operations_review_checklist.json")["operations_review_recorded"] is False


def test_feature_flag_policy_defaults_false_and_forbids_activation_here() -> None:
    policy = _json("feature_flag_policy.json")

    assert policy["flag_name"] == "open_macro_v03_runtime_activation"
    assert policy["default"] is False
    assert policy["production_default"] is False
    assert policy["allowed_environments"] == []
    assert policy["blast_radius"] == 0
    assert policy["require_explicit_approval"] is True
    assert policy["require_rollback_plan"] is True
    assert policy["require_monitoring"] is True
    assert policy["require_shadow_review"] is True
    assert policy["activation_allowed"] is False
    assert policy["activation_allowed_in_this_pr"] is False


def test_monitoring_slo_policy_covers_required_detectors() -> None:
    policy = _json("monitoring_slo_policy.json")
    ids = {slo["id"] for slo in policy["slos"]}

    assert policy["runtime_activation"] is False
    assert policy["escalation_owner"] == "quant-engine-governance"
    assert {
        "latency_slo",
        "memory_slo",
        "error_retry_slo",
        "divergence_slo",
        "stale_artifact_detection",
        "missing_output_detection",
        "allocator_publish_attempt_detection",
        "db_write_attempt_detection",
        "runtime_activation_attempt_detection",
        "production_endpoint_activation_detection",
    }.issubset(ids)
    assert policy["alert_severity"]["allocator_publish_attempt_detection"] == "critical"
    assert policy["alert_severity"]["db_write_attempt_detection"] == "critical"


def test_activation_and_rollback_runbooks_are_inert() -> None:
    activation = _text(A5_ROOT / "activation_runbook.md")
    rollback = _text(A5_ROOT / "rollback_runbook.md")

    assert "Status: preparatory only" in activation
    assert "does not execute activation" in activation
    assert "Default: `false`." in activation
    assert "Keep `open_macro_v03_runtime_activation=false`." in rollback
    assert "Confirm A5 remains blocked." in rollback
    assert "Prevent Allocator Publish" in rollback
    assert "Verify no official result was published." in rollback
    assert "Verify no productive DB write occurred." in rollback

    for text in (activation, rollback):
        for forbidden in FORBIDDEN_ACTIVATION_STRINGS:
            assert forbidden not in text


def test_unresolved_risks_are_explicit_and_blocking() -> None:
    register = _json("unresolved_risks_register.json")
    risks = {risk["id"]: risk for risk in register["risks"]}

    assert register["A5"] == "blocked"
    assert register["runtime_activation"] is False
    for risk_id in (
        "technical_review_pending",
        "quantitative_review_pending",
        "risk_review_pending",
        "operations_review_pending",
    ):
        assert risks[risk_id]["status"] == "pending"
        assert risks[risk_id]["blocking"] is True
        assert risks[risk_id]["owner_or_reviewer"]


def test_a5_artifacts_do_not_introduce_activation_markers() -> None:
    for path in A5_ROOT.rglob("*"):
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            for forbidden in FORBIDDEN_ACTIVATION_STRINGS:
                assert forbidden not in text, f"{forbidden} found in {path}"


def test_no_formula_input_calibration_or_contract_files_changed_in_branch() -> None:
    result = subprocess.run(
        ["git", "diff", "--name-only", "origin/main...HEAD"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    changed = [line for line in result.stdout.splitlines() if line]

    forbidden_prefixes = (
        "contracts/quant-engine/v1/",
        "fixtures/input_packs/",
        "artifacts/input_packs/",
        "artifacts/calibration/open_macro_v03_calibration_001/",
        "src/workers/",
    )
    assert [path for path in changed if path.startswith(forbidden_prefixes)] == []


def test_docs_and_memo_record_final_non_activation_state() -> None:
    doc = _text(DOC)
    memo = _text(A5_ROOT / "go_no_go_memo.md")
    report = _text(A5_ROOT / "a5_preflight_readiness_report.md")

    for text in (doc, memo, report):
        assert "A5" in text
        assert "`blocked`" in text or "`A5=blocked`" in text
        assert "runtime_activation" in text
        assert "`false`" in text or "=false`" in text
        assert "controlled activation proposal PR" in text
    assert "A5 continua bloqueado" in doc


def test_github_actions_includes_a5_preflight_governance_test() -> None:
    text = _text(GITHUB_ACTIONS_WORKFLOW)

    assert "pull_request:" in text
    assert "tests/test_a5_preflight_readiness.py" in text
