from __future__ import annotations

import json
import hashlib
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
A5_ROOT = ROOT / "artifacts" / "a5" / "open_macro_v03_a5_preflight_001"
DOC = ROOT / "docs" / "a5" / "open_macro_v03_a5_preflight_001.md"
GITHUB_ACTIONS_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def _json(name: str) -> dict:
    return json.loads((A5_ROOT / name).read_text(encoding="utf-8"))


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _committed_or_normalized_file_bytes(source_commit: str, artifact_path: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{source_commit}:{artifact_path}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode == 0:
        return result.stdout
    commit_exists = subprocess.run(
        ["git", "cat-file", "-e", f"{source_commit}^{{commit}}"],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0
    assert not commit_exists, f"{artifact_path} is unavailable at source_commit {source_commit}"
    return (ROOT / artifact_path).read_bytes().replace(b"\r\n", b"\n")


def test_a5_preflight_manifest_keeps_governance_inert() -> None:
    manifest = _json("a5_preflight_manifest.json")

    assert manifest["a5_preflight_id"] == "open_macro_v03_a5_preflight_001"
    assert manifest["status"] == "readiness_candidate"
    assert manifest["strategy"] == "open_macro_v03"
    assert manifest["post_shadow_planning_merge_commit"] == "c99aa5814a7051fb652b319ba34adaad23cb14e6"
    assert manifest["A3"] == "open_macro_v03"
    assert manifest["A4"] == "a5_preflight_readiness_prepared"
    assert manifest["A5"] == "blocked"
    assert manifest["runtime_activation"] is False
    assert manifest["freeze_ready"] is False
    assert manifest["official_result"] is False
    assert manifest["allocator_impact"] == "none"
    assert manifest["db_write_mode"] == "none"
    assert manifest["production_endpoint_activation"] == "none"
    assert manifest["production_impact"] == "none"
    assert manifest["formula_changes"] == "none"
    assert manifest["input_pack_changes"] == "none"
    assert manifest["calibration_pack_changes"] == "none"
    assert manifest["contract_v1_changes"] == "none"
    assert manifest["controlled_activation"] == "not_started"
    assert manifest["decision_required_before_A5"] is True


def test_evidence_index_allows_only_nonblocking_optional_missing() -> None:
    evidence = _json("evidence_index.json")

    assert evidence["overall_status"] == "present_with_missing_optional"
    assert evidence["blocking"] is False
    assert evidence["missing_blocking_count"] == 0
    assert evidence["critical_missing_count"] == 0
    items = evidence["items"]
    missing_blocking = [item for item in items if item["status"] == "missing_blocking"]
    assert missing_blocking == []

    missing_optional = [item for item in items if item["status"] == "missing_optional"]
    assert {item["evidence_id"] for item in missing_optional} == {
        "input_pack_certified_manifest_expected_artifact_path"
    }
    for item in items:
        if item["status"] == "present":
            assert item["verified"] is True
            assert item["sha256"] and len(item["sha256"]) == 64
            assert (ROOT / item["artifact_path"]).is_file()
            artifact_bytes = _committed_or_normalized_file_bytes(item["source_commit"], item["artifact_path"])
            assert hashlib.sha256(artifact_bytes).hexdigest() == item["sha256"]


def test_promotion_l4_cannot_pass_or_activate_a5() -> None:
    matrix = _json("promotion_gate_matrix.json")

    assert matrix["A5"] == "blocked"
    assert matrix["runtime_activation"] is False
    l4 = next(level for level in matrix["levels"] if level["level"] == "L4")
    assert l4["status"] == "blocked"
    assert l4["activation_allowed"] is False
    assert all(criterion["status"] != "pass" for criterion in l4["criteria"])
    assert matrix["separate_activation_pr_required"] is True


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
    assert policy["activation_allowed_in_this_pr"] is False


def test_decision_template_does_not_approve_a5_by_default() -> None:
    text = _text(A5_ROOT / "a5_decision_record_template.md")

    assert "approve_controlled_activation: false" in text
    assert "runtime_activation_allowed: false" in text
    assert "freeze_ready: false" in text
    assert "A5_unblocked: false" in text
    assert "decision_status: pending" in text


def test_runbooks_exist_but_activation_runbook_is_not_executed() -> None:
    activation = _text(A5_ROOT / "production_activation_runbook.md")
    rollback = _text(A5_ROOT / "rollback_runbook.md")

    assert "Este runbook é preparatório. Não autoriza A5. Não ativa runtime." in activation
    assert "Status: not executed." in activation
    assert "Activation is not allowed in this PR." in activation
    assert "Keep `open_macro_v03_runtime_activation=false`." in rollback
    assert "Confirm A5 remains blocked." in rollback
    assert "Prevent allocator publish." in rollback
    assert "Prevent official DB writes." in rollback


def test_review_checklists_keep_human_signoffs_pending() -> None:
    technical = _json("technical_review_checklist.json")
    quantitative = _json("quantitative_review_checklist.json")
    risk = _json("risk_review_checklist.json")

    assert technical["technical_review_recorded"] is False
    assert quantitative["technical_and_quantitative_review_recorded"] is False
    assert quantitative["quantitative_reviewer_signoff"] == "pending"
    assert risk["risk_reviewer_signoff"] == "pending"
    assert technical["A5"] == quantitative["A5"] == risk["A5"] == "blocked"
    assert technical["runtime_activation"] is False
    assert quantitative["runtime_activation"] is False
    assert risk["runtime_activation"] is False


def test_checklist_evidence_ids_are_declared_in_index() -> None:
    evidence_ids = {item["evidence_id"] for item in _json("evidence_index.json")["items"]}

    for checklist_name in (
        "technical_review_checklist.json",
        "quantitative_review_checklist.json",
        "risk_review_checklist.json",
    ):
        checklist = _json(checklist_name)
        for item in checklist["items"]:
            assert set(item["evidence_ids"]).issubset(evidence_ids)


def test_quantitative_window_checks_cite_calibration_config() -> None:
    evidence_ids = {item["evidence_id"] for item in _json("evidence_index.json")["items"]}
    quantitative = _json("quantitative_review_checklist.json")
    items = {item["id"]: item for item in quantitative["items"]}
    calibration_config = json.loads(
        (ROOT / "artifacts/calibration/open_macro_v03_calibration_001/calibration_config.json").read_text(
            encoding="utf-8"
        )
    )

    assert "calibration_config" in evidence_ids
    assert items["out_of_sample_evidence_present"]["evidence_ids"] == ["calibration_config"]
    assert items["stress_windows_evidence_present"]["evidence_ids"] == ["calibration_config"]
    assert calibration_config["windows"]["out_of_sample"] == {
        "end": "2026-06-26",
        "start": "2026-06-26",
    }
    assert any(
        window["name"] == "p0_latest_macro_rates"
        for window in calibration_config["windows"]["stress"]
    )


def test_docs_report_final_state_and_recommendation() -> None:
    doc = _text(DOC)
    report = _text(A5_ROOT / "a5_preflight_readiness_report.md")

    for text in (doc, report):
        assert "A5" in text
        assert "`blocked`" in text
        assert "runtime_activation" in text
        assert "`false`" in text
        assert "Preparar Controlled Shadow Execution / Runtime Integration Skeleton" in text
        assert "A5 continua bloqueado" in text
    assert "Expected `artifacts/input_packs/open_macro_v03_certified_input_pack_001/manifest.json` is absent" in report


def test_github_actions_includes_a5_preflight_governance_test() -> None:
    text = _text(GITHUB_ACTIONS_WORKFLOW)

    assert "pull_request:" in text
    assert "tests/test_a5_preflight_readiness.py" in text
