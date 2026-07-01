from __future__ import annotations

import json
import math
import re
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
    "runtime_activation_allowed",
    "runtime_activation_attempt",
    "freeze_ready",
    "activation_allowed",
    "activation_allowed_in_this_pr",
    "activation_requested",
    "approve_controlled_activation",
    "A5_unblocked",
    "official_result",
    "official_result_published",
    "allow_db_write",
    "db_write_official",
    "official_db_write_attempt",
    "productive_db_received_official_result",
    "allocator_publish",
    "allocator_publish_attempt",
    "allocator_received_output",
    "allow_allocator_publish",
    "production_endpoint_activation_attempt",
    "production_endpoint_activated",
    "backend_executes_engine",
    "backend_executes_docker",
    "backend_executes_subprocess",
    "docker_execution_from_backend",
    "backend_executes_engine_attempt",
    "backend_executes_docker_attempt",
    "backend_executes_subprocess_attempt",
    "docker_execution_from_backend_attempt",
    "feature_flag_default",
    "default",
    "production_default",
}

A5_BLOCKED_STATUS_FIELDS = {"A5", "a5_status"}

REQUIRED_TRUE_FIELDS = {
    "proposal_only",
    "runtime_activation_false",
    "A5_blocked",
    "freeze_ready_false",
    "official_result_false",
    "allow_db_write_false",
}

REQUIRED_NONE_FIELDS = {
    "activation_effect_in_this_pr",
    "allocator_impact",
    "backend_execution",
    "db_write_mode",
    "production_endpoint_activation",
    "production_impact",
    "formula_changes",
    "input_pack_changes",
    "calibration_pack_changes",
    "contract_v1_changes",
}

SIDE_EFFECT_ATTEMPT_FAILURE_CLASSES = {
    "runtime_activation_attempt",
    "official_db_write_attempt",
    "allocator_publish_attempt",
    "production_endpoint_activation_attempt",
    "backend_executes_engine_attempt",
    "backend_executes_docker_attempt",
    "backend_executes_subprocess_attempt",
    "docker_execution_from_backend_attempt",
}

FORBIDDEN_ALLOWED_CHECK_IDS = {
    "runtime_activation",
    "official_result",
    "db_write",
    "official_db_write",
    "allocator_publish",
    "production_endpoint_activation",
    "backend_engine_execution",
    "backend_docker_execution",
    "backend_subprocess_execution",
    "docker_execution_from_backend",
}

FORBIDDEN_TEXT_JSON_TRUE_FIELDS = tuple(sorted(FORBIDDEN_TRUE_FIELDS - {"activation_requested"}))

REQUIRED_APPROVAL_ROLES = {
    "technical_owner",
    "quant_owner",
    "risk_owner",
    "operations_owner",
    "product_portfolio_owner",
    "final_approver",
}

FORBIDDEN_AUTOMATIC_ACTIVATION_COMMANDS = (
    "railway up --detach",
    "kubectl apply",
    "open_macro_v03_runtime_activation=true",
    "set open_macro_v03_runtime_activation true",
    "activation_allowed=true",
    "freeze_ready=true",
)

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


def _reject_non_finite_json_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite JSON number {value!r}")
    return parsed


def _loads_json(text: str) -> Any:
    return json.loads(
        text,
        object_pairs_hook=_reject_duplicate_json_object_keys,
        parse_constant=_reject_non_finite_json_constant,
        parse_float=_reject_non_finite_json_float,
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


@pytest.mark.parametrize("number", ["1e9999", "-1e9999"])
def test_proposal_json_loader_rejects_float_overflow(number: str) -> None:
    with pytest.raises(ValueError, match="non-finite JSON number"):
        _loads_json(f'{{"value": {number}}}')


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
    for gate in (
        "technical_review",
        "quantitative_review",
        "risk_review",
        "operations_review",
        "production_readiness",
        "rollback_readiness",
        "monitoring_readiness",
    ):
        assert matrix["gates"][gate] == "pending"


def test_approval_matrix_does_not_accept_invented_owners() -> None:
    approvals = _json("approval_matrix.json")
    approval_entries = approvals["approvals"]
    approval_roles = [approval["role"] for approval in approval_entries]

    assert approvals["owners_real_names_recorded"] is False
    assert approvals["activation_allowed"] is False
    assert approval_entries
    assert set(approval_roles) == REQUIRED_APPROVAL_ROLES
    assert len(approval_roles) == len(REQUIRED_APPROVAL_ROLES)
    for approval in approval_entries:
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
    assert "lacunas" not in evidence
    assert evidence["gaps"]

    checklist = _json("production_activation_checklist.json")
    checklist_ids = {check["id"] for check in checklist["checks"]}
    assert set(evidence["items_blocking"]) <= checklist_ids
    assert "kill_switch_dry_run" in evidence["items_blocking"]
    assert "monitoring_thresholds_complete" in evidence["items_blocking"]
    assert "kill_switch_test" not in evidence["items_blocking"]
    assert "monitoring_thresholds" not in evidence["items_blocking"]

    for value in _walk_json(evidence):
        if isinstance(value, str):
            assert value.strip() == value
            assert value not in PLACEHOLDERS


def test_no_activation_guard_report_blocks_runtime_freeze_activation_and_official_result() -> None:
    report = _json("no_activation_guard_report.json")
    required_check_ids = {
        "runtime_activation_true_absent",
        "freeze_ready_true_absent",
        "activation_allowed_true_absent",
        "official_result_true_absent",
        "allow_allocator_publish_true_absent",
        "production_endpoint_activation_non_none_absent",
        "feature_flag_default_true_absent",
        "automatic_activation_script_absent",
        "productive_endpoint_absent",
        "runtime_productive_change_absent",
    }

    assert report["status"] == "pass_no_activation_effect"
    assert report["runtime_activation"] is False
    assert report["freeze_ready"] is False
    assert report["activation_allowed"] is False
    assert report["official_result"] is False
    assert report["production_endpoint_activation"] == "none"
    assert {check["id"] for check in report["checks"]} == required_check_ids
    assert {check["status"] for check in report["checks"]} == {"pass"}


def test_feature_flag_activation_policy_pins_rollout_envelope_closed() -> None:
    policy = _json("feature_flag_activation_policy.json")

    assert policy["allowed_environments"] == []
    assert policy["max_rollout_percentage"] == 0
    assert policy["who_can_change"] == []


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
            '"official_result_published": true',
            '"allow_db_write": true',
            '"db_write_official": true',
            '"official_db_write_attempt": true',
            '"productive_db_received_official_result": true',
            '"allocator_received_output": true',
            '"backend_executes_engine": true',
            '"backend_executes_docker": true',
            '"backend_executes_subprocess": true',
            '"docker_execution_from_backend": true',
            '"runtime_activation_attempt": true',
            '"allocator_publish_attempt": true',
            '"production_endpoint_activation_attempt": true',
            '"backend_executes_engine_attempt": true',
            '"backend_executes_docker_attempt": true',
            '"backend_executes_subprocess_attempt": true',
            '"docker_execution_from_backend_attempt": true',
            '"allow_allocator_publish": true',
            '"production_endpoint_activated": true',
            '"feature_flag_default": true',
            "runtime_activation: true",
            "freeze_ready: true",
            "activation_allowed: true",
            "activation_requested: true",
            "official_result: true",
            "official_result_published: true",
            "allow_db_write: true",
            "db_write_official: true",
            "official_db_write_attempt: true",
            "productive_db_received_official_result: true",
            "backend_executes_engine: true",
            "backend_executes_docker: true",
            "backend_executes_subprocess: true",
            "docker_execution_from_backend: true",
            "runtime_activation_attempt: true",
            "allocator_publish_attempt: true",
            "production_endpoint_activation_attempt: true",
            "backend_executes_engine_attempt: true",
            "backend_executes_docker_attempt: true",
            "backend_executes_subprocess_attempt: true",
            "docker_execution_from_backend_attempt: true",
            "allocator_publish: true",
            "allocator_received_output: true",
            "allow_allocator_publish: true",
            "production_endpoint_activated: true",
            "feature_flag_default: true",
            "approve_controlled_activation: true",
            "runtime_activation_allowed: true",
            "A5_unblocked: true",
            "runtime_activation=true",
            "freeze_ready=true",
            "activation_allowed=true",
            "official_result=true",
            "official_result_published=true",
            "allow_db_write=true",
            "db_write_official=true",
            "official_db_write_attempt=true",
            "productive_db_received_official_result=true",
            "backend_executes_engine=true",
            "backend_executes_docker=true",
            "backend_executes_subprocess=true",
            "docker_execution_from_backend=true",
            "runtime_activation_attempt=true",
            "allocator_publish_attempt=true",
            "production_endpoint_activation_attempt=true",
            "backend_executes_engine_attempt=true",
            "backend_executes_docker_attempt=true",
            "backend_executes_subprocess_attempt=true",
            "docker_execution_from_backend_attempt=true",
            "allocator_received_output=true",
            "production_endpoint_activated=true",
            "db_write_mode=productive",
            "production_endpoint_activation=live",
            "production_endpoint_activation=enabled",
            "production_endpoint_activation=public",
            "production_endpoint_activation: live",
            "production_endpoint_activation: enabled",
            "production_endpoint_activation: public",
            '"production_endpoint_activation": "live"',
            '"production_endpoint_activation": "enabled"',
            '"production_endpoint_activation": "public"',
        )
        for snippet in forbidden_snippets:
            if re.search(rf"(?<![A-Za-z0-9_]){re.escape(snippet)}(?![A-Za-z0-9_])", text):
                violations.append(f"{path.relative_to(PROPOSAL_ROOT)} contains {snippet}")
        for field in FORBIDDEN_TEXT_JSON_TRUE_FIELDS:
            if re.search(rf'"{re.escape(field)}"\s*:\s*true\b', text):
                violations.append(f"{path.relative_to(PROPOSAL_ROOT)} contains {field}=true")
            if re.search(rf"(?<![A-Za-z0-9_]){re.escape(field)}\s*[:=]\s*true\b", text):
                violations.append(f"{path.relative_to(PROPOSAL_ROOT)} contains {field}=true")
        for field in A5_BLOCKED_STATUS_FIELDS:
            for match in re.finditer(rf'"?{re.escape(field)}"?\s*[:=]\s*"?([A-Za-z0-9_-]+)"?', text):
                if match.group(1) != "blocked":
                    violations.append(f"{path.relative_to(PROPOSAL_ROOT)} contains {field}={match.group(1)}")
        for field in REQUIRED_TRUE_FIELDS:
            for match in re.finditer(rf'"?{re.escape(field)}"?\s*[:=]\s*"?([A-Za-z0-9_-]+)"?', text):
                if match.group(1) != "true":
                    violations.append(
                        f"{path.relative_to(PROPOSAL_ROOT)} contains {field}={match.group(1)!r}; expected true"
                    )
        for field in REQUIRED_NONE_FIELDS:
            for match in re.finditer(rf'"?{re.escape(field)}"?\s*[:=]\s*"?([A-Za-z0-9_-]+)"?', text):
                if match.group(1) != "none":
                    violations.append(
                        f"{path.relative_to(PROPOSAL_ROOT)} contains {field}={match.group(1)!r}; expected none"
                    )
        for match in re.finditer(r'"?failure_class"?\s*[:=]\s*"?([A-Za-z0-9_-]+)"?', text):
            if match.group(1) in SIDE_EFFECT_ATTEMPT_FAILURE_CLASSES:
                violations.append(f"{path.relative_to(PROPOSAL_ROOT)} contains failure_class={match.group(1)!r}")
        for match in re.finditer(r'"?side_effect_attempt_count"?\s*[:=]\s*"?([0-9]+)"?', text):
            if int(match.group(1)) != 0:
                violations.append(f"{path.relative_to(PROPOSAL_ROOT)} contains side_effect_attempt_count={match.group(1)}")
        for match in re.finditer(
            r'"?side_effect_attempt_evidence_sha256"?\s*[:=]\s*"?([^"\s,}]+)"?', text
        ):
            if match.group(1) not in {"null", "none", "None"}:
                violations.append(
                    f"{path.relative_to(PROPOSAL_ROOT)} contains side_effect_attempt_evidence_sha256"
                )
        if path.suffix == ".json":
            _load_json(path)

    assert violations == []


def test_json_activation_fields_remain_false_or_none_for_proposal_package() -> None:
    for path in PROPOSAL_ROOT.rglob("*.json"):
        payload = _load_json(path)
        _assert_no_forbidden_allowed_check_rows(path, payload)
        if path.name == "controlled_activation_proposal_manifest.json":
            assert payload["activation_requested"] is True
        for item_path, key, value in _iter_json_items(payload):
            if key in A5_BLOCKED_STATUS_FIELDS:
                assert value == "blocked", (
                    f"{path.relative_to(PROPOSAL_ROOT)} has {key}={value!r}; expected blocked"
                )
            if key == "activation_requested":
                is_top_level_manifest_activation_request = (
                    path.name == "controlled_activation_proposal_manifest.json"
                    and item_path == ("activation_requested",)
                )
                if is_top_level_manifest_activation_request:
                    assert value is True, (
                        f"{path.relative_to(PROPOSAL_ROOT)} has activation_requested={value!r}; expected true"
                    )
                    continue
                else:
                    assert value is False or value is None, (
                        f"{path.relative_to(PROPOSAL_ROOT)} has activation_requested={value!r}; "
                        "only controlled_activation_proposal_manifest.json may request activation"
                    )
            if key in FORBIDDEN_TRUE_FIELDS:
                assert value is False or value is None, (
                    f"{path.relative_to(PROPOSAL_ROOT)} has {key}={value!r}; expected false or null"
                )
            if key in REQUIRED_TRUE_FIELDS:
                assert value is True, (
                    f"{path.relative_to(PROPOSAL_ROOT)} has {key}={value!r}; expected true"
                )
            if key in REQUIRED_NONE_FIELDS:
                assert value == "none", (
                    f"{path.relative_to(PROPOSAL_ROOT)} has {key}={value!r}; expected none"
                )
            if key == "failure_class":
                assert value not in SIDE_EFFECT_ATTEMPT_FAILURE_CLASSES, (
                    f"{path.relative_to(PROPOSAL_ROOT)} has failure_class={value!r}"
                )
            if key == "side_effect_attempt_count":
                assert value is None or (type(value) is int and value == 0), (
                    f"{path.relative_to(PROPOSAL_ROOT)} has side_effect_attempt_count={value!r}; "
                    "expected absent, null, or 0"
                )
            if key == "side_effect_attempt_evidence_sha256":
                assert value is None, (
                    f"{path.relative_to(PROPOSAL_ROOT)} has side_effect_attempt_evidence_sha256={value!r}; "
                    "expected absent or null"
                )


def _assert_no_forbidden_allowed_check_rows(path: Path, value: Any) -> None:
    if isinstance(value, dict):
        check_id = value.get("id")
        if check_id in FORBIDDEN_ALLOWED_CHECK_IDS and value.get("allowed") is True:
            raise AssertionError(
                f"{path.relative_to(PROPOSAL_ROOT)} has check id={check_id!r} allowed=True; "
                "expected false or absent"
            )
        for item in value.values():
            _assert_no_forbidden_allowed_check_rows(path, item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_forbidden_allowed_check_rows(path, item)


def test_json_activation_guard_checks_nested_compact_json_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "compact.json").write_text('{"runtime_activation":true}\n', encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=r"nested.*compact\.json.*runtime_activation=True"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [("A5", "unblocked"), ("a5_status", "unblocked"), ("a5_status", "cleared")],
)
def test_json_activation_guard_rejects_nested_a5_non_blocked_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, bad_value: str
) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "compact.json").write_text(
        json.dumps({"controls": [{field: bad_value}]}) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=rf"nested.*compact\.json.*{field}={bad_value!r}"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


def test_json_activation_guard_rejects_nested_activation_requested_outside_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "compact.json").write_text('{"controls":[{"activation_requested":true}]}\n', encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=r"nested.*compact\.json.*activation_requested=True"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


def test_json_activation_guard_rejects_nested_activation_requested_inside_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "controlled_activation_proposal_manifest.json").write_text(
        '{"activation_requested":true,"controls":[{"activation_requested":true}]}\n',
        encoding="utf-8",
    )

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(
        AssertionError,
        match=r"controlled_activation_proposal_manifest\.json.*activation_requested=True",
    ):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


@pytest.mark.parametrize(
    "field",
    ["runtime_activation", "activation_allowed_in_this_pr", "db_write_official", "official_result_published"],
)
def test_text_activation_guard_rejects_compact_json_activation_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    (tmp_path / "proposal.md").write_text(
        f'embedded compact snippet {{"{field}":true}}\n',
        encoding="utf-8",
    )

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=rf"{field}=true"):
        test_new_proposal_artifacts_do_not_contain_forbidden_activation_true_values()


@pytest.mark.parametrize(
    ("field", "snippet"),
    [
        ("default", "default: true"),
        ("default", "default=true"),
        ("production_default", "production_default: true"),
        ("production_default", "production_default=true"),
        ("activation_allowed_in_this_pr", "activation_allowed_in_this_pr: true"),
        ("activation_allowed_in_this_pr", "activation_allowed_in_this_pr=true"),
    ],
)
def test_text_activation_guard_rejects_forbidden_plain_text_true_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, snippet: str
) -> None:
    (tmp_path / "runbook.md").write_text(f"{snippet}\n", encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=rf"{field}=true"):
        test_new_proposal_artifacts_do_not_contain_forbidden_activation_true_values()


def test_json_activation_field_guard_rejects_string_truth_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "string_truth.json"
    path.write_text('{"runtime_activation":"true","nested":[{"activation_allowed":"true"}]}\n', encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match="runtime_activation='true'"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


@pytest.mark.parametrize(
    "field",
    [
        "runtime_activation_allowed",
        "activation_allowed_in_this_pr",
        "approve_controlled_activation",
        "A5_unblocked",
        "db_write_official",
        "official_result_published",
        "allocator_received_output",
        "productive_db_received_official_result",
        "production_endpoint_activated",
    ],
)
def test_json_activation_guard_rejects_compact_json_activation_alias_true_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    path = tmp_path / "aliases.json"
    path.write_text(json.dumps({"nested": [{field: True}]}) + "\n", encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=rf"{field}=True"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ({"failure_class": "runtime_activation_attempt"}, "failure_class='runtime_activation_attempt'"),
        ({"side_effect_attempt_count": 1}, "side_effect_attempt_count=1"),
        ({"side_effect_attempt_count": False}, "side_effect_attempt_count=False"),
        ({"side_effect_attempt_evidence_sha256": "a" * 64}, "side_effect_attempt_evidence_sha256"),
    ],
)
def test_json_activation_guard_rejects_side_effect_attempt_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, payload: dict[str, Any], match: str
) -> None:
    path = tmp_path / "side_effect_attempt.json"
    path.write_text(json.dumps({"nested": [payload]}) + "\n", encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=match):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


def test_json_activation_guard_allows_zero_or_null_side_effect_attempt_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "side_effect_attempt_zero.json"
    path.write_text(
        json.dumps(
            {
                "side_effect_attempt_count": 0,
                "side_effect_attempt_evidence_sha256": None,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    test_json_activation_fields_remain_false_or_none_for_proposal_package()


def test_activation_guards_reject_backend_execution_and_attempt_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "backend.json").write_text(
        '{"nested":[{"backend_executes_docker":true}]}\n',
        encoding="utf-8",
    )

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match="backend_executes_docker=True"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()

    (tmp_path / "backend.json").write_text(
        '{"nested":[{"docker_execution_from_backend":true}]}\n',
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="docker_execution_from_backend=True"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()

    (tmp_path / "backend.json").unlink()
    (tmp_path / "backend.md").write_text(
        "backend_executes_subprocess_attempt=true\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="backend_executes_subprocess_attempt=true"):
        test_new_proposal_artifacts_do_not_contain_forbidden_activation_true_values()

    (tmp_path / "backend.md").write_text(
        "docker_execution_from_backend_attempt=true\n",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="docker_execution_from_backend_attempt=true"):
        test_new_proposal_artifacts_do_not_contain_forbidden_activation_true_values()


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("db_write_mode", "official"),
        ("db_write_mode", "read_write"),
        ("db_write_mode", "none_or_artifact_only"),
        ("db_write_mode", None),
        ("allocator_impact", "publish"),
        ("backend_execution", "docker"),
        ("production_impact", "live"),
    ],
)
def test_json_activation_guard_requires_side_effect_pins_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, bad_value: object
) -> None:
    path = tmp_path / "side_effect_pin.json"
    path.write_text(json.dumps({"nested": [{field: bad_value}]}) + "\n", encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=rf"{field}={bad_value!r}"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


@pytest.mark.parametrize("check_id", sorted(FORBIDDEN_ALLOWED_CHECK_IDS))
def test_json_activation_guard_rejects_forbidden_allowed_check_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, check_id: str
) -> None:
    path = tmp_path / "no_side_effects_report.json"
    path.write_text(
        json.dumps({"checks": [{"id": check_id, "allowed": True, "status": "pass"}]}) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=rf"id={check_id!r} allowed=True"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


def test_json_activation_guard_requires_proposal_only_true(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "proposal_only.json"
    path.write_text(json.dumps({"nested": [{"proposal_only": False}]}) + "\n", encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=r"proposal_only=False.*expected true"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


@pytest.mark.parametrize(
    "field",
    ["formula_changes", "input_pack_changes", "calibration_pack_changes", "contract_v1_changes"],
)
def test_json_activation_guard_requires_proposal_change_fields_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    path = tmp_path / "change_field.json"
    path.write_text(json.dumps({"nested": [{field: "changed"}]}) + "\n", encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=rf"{field}='changed'.*expected none"):
        test_json_activation_fields_remain_false_or_none_for_proposal_package()


def test_text_activation_guard_rejects_production_endpoint_activation_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "runbook.md").write_text(
        "production_endpoint_activation=live\nproduction_endpoint_activation: enabled\n",
        encoding="utf-8",
    )

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match="production_endpoint_activation=live"):
        test_new_proposal_artifacts_do_not_contain_forbidden_activation_true_values()


@pytest.mark.parametrize(
    ("marker", "match"),
    [
        ("a5_status=cleared", "a5_status=cleared"),
        ('"a5_status": "unblocked"', "a5_status=unblocked"),
        ("db_write_official=true", "db_write_official=true"),
        ("official_result_published=true", "official_result_published=true"),
        ("allocator_received_output=true", "allocator_received_output=true"),
        ("productive_db_received_official_result=true", "productive_db_received_official_result=true"),
        ("production_endpoint_activated=true", "production_endpoint_activated=true"),
        ("failure_class=runtime_activation_attempt", "failure_class='runtime_activation_attempt'"),
        ("side_effect_attempt_count=1", "side_effect_attempt_count=1"),
        (f"side_effect_attempt_evidence_sha256={'a' * 64}", "side_effect_attempt_evidence_sha256"),
        ("contract_v1_changes=changed", "contract_v1_changes='changed'"),
        ("allocator_impact=publish", "allocator_impact='publish'"),
        ("backend_execution=docker", "backend_execution='docker'"),
        ("production_impact: live", "production_impact='live'"),
        ('{"production_impact":"enabled"}', "production_impact='enabled'"),
        ("proposal_only=false", "proposal_only='false'"),
        ("activation_effect_in_this_pr=runtime", "activation_effect_in_this_pr='runtime'"),
    ],
)
def test_text_activation_guard_rejects_side_effect_and_scope_alias_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, marker: str, match: str
) -> None:
    (tmp_path / "proposal.md").write_text(f"{marker}\n", encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=re.escape(match)):
        test_new_proposal_artifacts_do_not_contain_forbidden_activation_true_values()


def _iter_json_items(
    value: Any, prefix: tuple[str | int, ...] = ()
) -> list[tuple[tuple[str | int, ...], str, Any]]:
    if isinstance(value, dict):
        items: list[tuple[tuple[str | int, ...], str, Any]] = []
        for key, item in value.items():
            item_path = (*prefix, key)
            items.append((item_path, key, item))
            items.extend(_iter_json_items(item, item_path))
        return items
    if isinstance(value, list):
        items = []
        for index, item in enumerate(value):
            items.extend(_iter_json_items(item, (*prefix, index)))
        return items
    return []


def test_activation_runbook_and_proposal_do_not_contain_automatic_activation_command() -> None:
    violations: list[str] = []
    for path in sorted(PROPOSAL_ROOT.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for command in FORBIDDEN_AUTOMATIC_ACTIVATION_COMMANDS:
            if command in text:
                violations.append(f"{path.relative_to(PROPOSAL_ROOT)} contains {command}")

    assert violations == []


def test_automatic_activation_command_guard_scans_nested_proposal_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "future_activation.md").write_text("kubectl apply -f prod.yaml\n", encoding="utf-8")

    monkeypatch.setitem(globals(), "PROPOSAL_ROOT", tmp_path)

    with pytest.raises(AssertionError, match=r"nested.*future_activation\.md.*kubectl apply"):
        test_activation_runbook_and_proposal_do_not_contain_automatic_activation_command()


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
    required_blocking_check_ids = {
        "technical_review_recorded",
        "quantitative_review_recorded",
        "risk_review_recorded",
        "operations_review_recorded",
        "approval_matrix_complete",
        "rollback_dry_run",
        "kill_switch_dry_run",
        "monitoring_thresholds_complete",
    }

    assert required_blocking_check_ids <= checks.keys()
    for check_id in required_blocking_check_ids:
        assert checks[check_id]["status"] == "pending"
        assert checks[check_id]["blocking"] is True

    assert monitoring["runtime_activation"] is False
    assert monitoring["activation_allowed"] is False
    assert any(slo["status"] == "pending" for slo in monitoring["slos"])
    critical_detector_ids = {
        "db_write_attempt_alert",
        "allocator_publish_attempt_alert",
        "runtime_activation_attempt_alert",
        "production_endpoint_activation_attempt_alert",
    }
    monitoring_slos_by_id = {slo["id"]: slo for slo in monitoring["slos"]}

    assert critical_detector_ids.issubset(monitoring_slos_by_id)
    for detector_id in critical_detector_ids:
        detector = monitoring_slos_by_id[detector_id]
        assert detector["threshold"] == 0
        assert detector["status"] == "defined"
        assert detector["alert_severity"] == "critical"

    pending_threshold_slos = {
        "latency_slo": "controlled_shadow_latency_p95_ms",
        "memory_slo": "controlled_shadow_memory_peak_bytes",
        "error_rate_slo": "error_rate",
        "retry_rate_slo": "retry_rate",
    }
    assert pending_threshold_slos.keys() <= monitoring_slos_by_id.keys()
    for slo_id, metric in pending_threshold_slos.items():
        slo = monitoring_slos_by_id[slo_id]
        assert slo["metric"] == metric
        assert slo["threshold"] is None
        assert slo["status"] == "pending"

    assert kill_switch["runtime_activation"] is False
    assert kill_switch["activation_allowed"] is False
    assert kill_switch["test_status"] == "pending_operator_dry_run"
    assert kill_switch["owner"] == "unassigned"
    assert "Confirm production_endpoint_activation remains none." in kill_switch["validation_steps"]
