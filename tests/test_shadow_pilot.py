from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path

import jsonschema
import pytest

from src import shadow_pilot as sp
from src.input_packs.hashing import file_sha256

ROOT = Path(__file__).resolve().parents[1]
CALIBRATION_RUN_MATRIX = json.loads(
    (ROOT / "artifacts" / "calibration" / sp.CALIBRATION_ID / "run_matrix.json").read_text(encoding="utf-8")
)
CALIBRATION_MANIFEST = json.loads(
    (ROOT / "artifacts" / "calibration" / sp.CALIBRATION_ID / "calibration_manifest.json").read_text(encoding="utf-8")
)


def _envelope() -> dict:
    return sp.build_shadow_job_envelope(CALIBRATION_RUN_MATRIX)


def _result() -> dict:
    envelope = _envelope()
    return sp.build_shadow_result_manifest(
        envelope=envelope,
        invariant_report=_invariant_report(),
        reproducibility_report={"ok": True},
        output_manifest_hash="a" * 64,
        invariant_hash="b" * 64,
        baseline_hash="c" * 64,
        reproducibility_hash="d" * 64,
        started_at=sp.dt.datetime(2026, 6, 28, 12, 0, tzinfo=sp.dt.UTC),
        finished_at=sp.dt.datetime(2026, 6, 28, 12, 0, 1, tzinfo=sp.dt.UTC),
    )


def _output_manifest_with_logs() -> dict:
    return {
        "artifacts": [
            {"path": rel, "sha256": "a" * 64, "bytes": 1}
            for rel in sorted(sp.PILOT_RELATIVE_OUTPUTS)
        ],
        "unexpected_outputs": [],
    }


def _invariant_report(*, ok: bool = True) -> dict:
    return {
        "ok": ok,
        "checks": {
            "runtime_activation_false": True,
            "allow_db_write_false": True,
            "allow_allocator_publish_false": True,
        },
    }


def test_shadow_pilot_envelope_validates_pinned_provenance() -> None:
    envelope = _envelope()
    sp.validate_shadow_job_envelope(envelope, root=ROOT)

    for field, bad_value in (
        ("input_pack_sha256", "0" * 64),
        ("calibration_config_sha256", "1" * 64),
        ("contract_bundle_sha256", "2" * 64),
        ("engine_commit", "3" * 40),
        ("runtime_activation", True),
        ("allow_db_write", True),
        ("allow_allocator_publish", True),
        ("output_artifact_uri", "db://official/results/open_macro_v03"),
    ):
        bad = dict(envelope)
        bad[field] = bad_value
        with pytest.raises(jsonschema.ValidationError):
            sp.validate_shadow_job_envelope(bad, root=ROOT)


def test_shadow_result_manifest_rejects_divergent_or_retryable_success() -> None:
    result = _result()
    sp.validate_shadow_result_manifest(result, root=ROOT)

    divergent = deepcopy(result)
    divergent["divergence_summary"]["mismatch_count"] = 1
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_shadow_result_manifest(divergent, root=ROOT)

    retryable = deepcopy(result)
    retryable["retryable"] = True
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_shadow_result_manifest(retryable, root=ROOT)


def test_shadow_result_manifest_rejects_inconsistent_duration_window() -> None:
    result = _result()

    zero_window = deepcopy(result)
    zero_window["finished_at"] = zero_window["started_at"]
    with pytest.raises(jsonschema.ValidationError, match="finished_at"):
        sp.validate_shadow_result_manifest(zero_window, root=ROOT)

    bad_duration = deepcopy(result)
    bad_duration["duration_ms"] = 1
    with pytest.raises(jsonschema.ValidationError, match="duration_ms"):
        sp.validate_shadow_result_manifest(bad_duration, root=ROOT)


def test_side_effect_attempt_result_is_rejected_and_non_retryable() -> None:
    result = _result()
    attempt = {
        key: value
        for key, value in result.items()
        if key
        not in {
            "output_manifest_sha256",
            "invariant_report_sha256",
            "baseline_comparison_sha256",
            "reproducibility_report_sha256",
            "materiality_summary",
            "divergence_summary",
            "memory_peak_bytes",
            "cpu_time_ms",
        }
    }
    attempt.update(
        {
            "status": "rejected",
            "failure_class": "allocator_publish_attempt",
            "retryable": False,
            "side_effect_attempt_evidence_sha256": "e" * 64,
            "side_effect_attempt_count": 1,
        }
    )
    sp.validate_shadow_result_manifest(attempt, root=ROOT)

    attempt["retryable"] = True
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_shadow_result_manifest(attempt, root=ROOT)


def test_baseline_comparison_thresholds_are_enforced() -> None:
    policy = sp.load_policy(ROOT)
    comparison = sp.build_baseline_comparison(policy)
    assert sp.evaluate_baseline_comparison(comparison, policy)["status"] == "pass"

    hard = deepcopy(comparison)
    hard["materiality_summary"]["max_relative_delta_pct"] = 2.0
    hard_eval = sp.evaluate_baseline_comparison(hard, policy)
    assert hard_eval["status"] == "rejected"
    assert "hard_relative_delta_exceeded" in hard_eval["rejection_rules_triggered"]

    review = deepcopy(comparison)
    review["materiality_summary"]["max_relative_delta_pct"] = 0.5
    review_eval = sp.evaluate_baseline_comparison(review, policy)
    assert review_eval["status"] == "review_required"
    assert review_eval["material_divergence"] is True


def test_baseline_comparison_policy_rejects_contract_v1_change_key() -> None:
    policy = sp.load_policy(ROOT)
    comparison = sp.build_baseline_comparison(policy)
    comparison["forbidden_effects"]["contract_v1_change_without_new_bundle"] = "changed"

    evaluation = sp.evaluate_baseline_comparison(comparison, policy)

    assert evaluation["status"] == "rejected"
    assert "contract_v1_change_without_new_bundle" in evaluation["rejection_rules_triggered"]


def test_reproducibility_report_requires_exact_run_matrix_membership() -> None:
    matrix = deepcopy(CALIBRATION_RUN_MATRIX)
    old_label = matrix["comparison_evidence"]["labels"][0]
    new_label = "host_jobs2_r0"
    matrix["comparison_evidence"]["labels"][0] = new_label
    matrix["hashes"][new_label] = matrix["hashes"].pop(old_label)

    report = sp.build_reproducibility_report(matrix, sp.build_shadow_job_envelope(matrix), CALIBRATION_MANIFEST)

    assert report["ok"] is False
    assert old_label in report["missing"]
    assert new_label in report["unexpected"]

    wrong_count = deepcopy(CALIBRATION_RUN_MATRIX)
    wrong_count["comparison_evidence"]["run_count"] = 7
    count_report = sp.build_reproducibility_report(
        wrong_count,
        sp.build_shadow_job_envelope(wrong_count),
        CALIBRATION_MANIFEST,
    )
    assert count_report["ok"] is False


def test_reproducibility_report_recomputes_per_run_hash_equality() -> None:
    matrix = deepcopy(CALIBRATION_RUN_MATRIX)
    label = matrix["comparison_evidence"]["labels"][0]
    matrix["hashes"][label]["output_manifest_sha256"] = "0" * 64

    report = sp.build_reproducibility_report(matrix, sp.build_shadow_job_envelope(matrix), CALIBRATION_MANIFEST)

    assert report["ok"] is False
    assert label in report["run_hash_mismatches"]


def test_reproducibility_report_requires_isolated_execution_fields() -> None:
    for field, bad_value in (
        ("network", "bridge"),
        ("db_access", True),
        ("input_pack_mount", "read_write"),
        ("path_independence", False),
    ):
        matrix = deepcopy(CALIBRATION_RUN_MATRIX)
        matrix["comparison_evidence"][field] = bad_value

        report = sp.build_reproducibility_report(matrix, sp.build_shadow_job_envelope(matrix), CALIBRATION_MANIFEST)

        assert report["ok"] is False


def test_reproducibility_report_gates_image_provenance() -> None:
    report = sp.build_reproducibility_report(CALIBRATION_RUN_MATRIX, _envelope(), CALIBRATION_MANIFEST)
    assert report["ok"] is True
    assert report["docker_image_id"] == CALIBRATION_MANIFEST["engine_image_id"]
    assert report["docker_image_digest"] == CALIBRATION_MANIFEST["engine_image_digest"]
    assert report["docker_image_provenance_ok"] is True

    bad_id = deepcopy(CALIBRATION_RUN_MATRIX)
    bad_id["comparison_evidence"]["docker_image_id"] = "sha256:" + "0" * 64
    bad_id_report = sp.build_reproducibility_report(bad_id, sp.build_shadow_job_envelope(bad_id), CALIBRATION_MANIFEST)
    assert bad_id_report["ok"] is False
    assert bad_id_report["docker_image_provenance_ok"] is False

    digest_required = deepcopy(CALIBRATION_MANIFEST)
    digest_required["engine_image_digest"] = sp.RAILWAY_IMAGE_DIGEST
    missing_digest_report = sp.build_reproducibility_report(CALIBRATION_RUN_MATRIX, _envelope(), digest_required)
    assert missing_digest_report["ok"] is False
    assert missing_digest_report["docker_image_provenance_ok"] is False


def test_acceptance_report_blocks_when_invariant_report_is_red() -> None:
    policy = sp.load_policy(ROOT)
    baseline = sp.build_baseline_comparison(policy)
    reproducibility = sp.build_reproducibility_report(CALIBRATION_RUN_MATRIX, _envelope(), CALIBRATION_MANIFEST)

    report = sp.build_acceptance_report(
        policy=policy,
        output_manifest=_output_manifest_with_logs(),
        invariant_report=_invariant_report(ok=False),
        baseline_comparison=baseline,
        reproducibility_report=reproducibility,
        expected_run_fingerprint=_envelope()["run_fingerprint"],
    )

    rule = next(rule for rule in report["rules"] if rule["id"] == "invariant_failures_zero")
    assert rule["status"] == "fail"
    assert rule["blocking"] is True
    assert report["status"] == "artifact_gate_failed"


def test_acceptance_report_derives_forbidden_side_effect_rules() -> None:
    policy = sp.load_policy(ROOT)
    baseline = sp.build_baseline_comparison(policy)
    baseline["forbidden_effects"]["allocator_publish_attempt"] = True
    baseline["evaluation"] = sp.evaluate_baseline_comparison(baseline, policy)
    baseline["status"] = baseline["evaluation"]["status"]
    reproducibility = sp.build_reproducibility_report(CALIBRATION_RUN_MATRIX, _envelope(), CALIBRATION_MANIFEST)

    report = sp.build_acceptance_report(
        policy=policy,
        output_manifest=_output_manifest_with_logs(),
        invariant_report=_invariant_report(),
        baseline_comparison=baseline,
        reproducibility_report=reproducibility,
        expected_run_fingerprint=_envelope()["run_fingerprint"],
    )

    rule = next(rule for rule in report["rules"] if rule["id"] == "no_allocator_publish_attempt")
    assert rule["status"] == "fail"
    assert rule["blocking"] is True
    assert report["status"] == "artifact_gate_failed"


def test_acceptance_report_blocks_unexpected_outputs_and_fingerprint_mismatch() -> None:
    policy = sp.load_policy(ROOT)
    baseline = sp.build_baseline_comparison(policy)
    reproducibility = sp.build_reproducibility_report(CALIBRATION_RUN_MATRIX, _envelope(), CALIBRATION_MANIFEST)

    unexpected_report = sp.build_acceptance_report(
        policy=policy,
        output_manifest={
            "artifacts": [{"path": "logs/shadow_pilot.log"}, {"path": "logs/executor.log"}],
            "unexpected_outputs": ["stale.json"],
        },
        invariant_report=_invariant_report(),
        baseline_comparison=baseline,
        reproducibility_report=reproducibility,
        expected_run_fingerprint=_envelope()["run_fingerprint"],
    )
    unexpected_rule = next(rule for rule in unexpected_report["rules"] if rule["id"] == "no_unexpected_outputs")
    assert unexpected_rule["status"] == "fail"
    assert unexpected_report["status"] == "artifact_gate_failed"

    fingerprint_report = sp.build_acceptance_report(
        policy=policy,
        output_manifest=_output_manifest_with_logs(),
        invariant_report=_invariant_report(),
        baseline_comparison=baseline,
        reproducibility_report=reproducibility,
        expected_run_fingerprint="0" * 64,
    )
    fingerprint_rule = next(rule for rule in fingerprint_report["rules"] if rule["id"] == "run_fingerprint_consistent")
    assert fingerprint_rule["status"] == "fail"
    assert fingerprint_report["status"] == "artifact_gate_failed"


def test_acceptance_report_requires_full_output_manifest() -> None:
    policy = sp.load_policy(ROOT)
    baseline = sp.build_baseline_comparison(policy)
    reproducibility = sp.build_reproducibility_report(CALIBRATION_RUN_MATRIX, _envelope(), CALIBRATION_MANIFEST)
    logs_only = {
        "artifacts": [{"path": "logs/shadow_pilot.log"}, {"path": "logs/executor.log"}],
        "unexpected_outputs": [],
    }

    report = sp.build_acceptance_report(
        policy=policy,
        output_manifest=logs_only,
        invariant_report=_invariant_report(),
        baseline_comparison=baseline,
        reproducibility_report=reproducibility,
        expected_run_fingerprint=_envelope()["run_fingerprint"],
    )

    required_rule = next(rule for rule in report["rules"] if rule["id"] == "all_required_outputs_present")
    complete_rule = next(rule for rule in report["rules"] if rule["id"] == "output_manifest_complete")
    assert required_rule["status"] == "fail"
    assert complete_rule["status"] == "fail"
    assert report["status"] == "artifact_gate_failed"


def test_acceptance_report_blocks_baseline_rejection_status() -> None:
    policy = sp.load_policy(ROOT)
    baseline = sp.build_baseline_comparison(policy)
    baseline["divergence_summary"]["missing_outputs"] = 1
    baseline["evaluation"] = sp.evaluate_baseline_comparison(baseline, policy)
    baseline["status"] = baseline["evaluation"]["status"]
    reproducibility = sp.build_reproducibility_report(CALIBRATION_RUN_MATRIX, _envelope(), CALIBRATION_MANIFEST)

    report = sp.build_acceptance_report(
        policy=policy,
        output_manifest=_output_manifest_with_logs(),
        invariant_report=_invariant_report(),
        baseline_comparison=baseline,
        reproducibility_report=reproducibility,
        expected_run_fingerprint=_envelope()["run_fingerprint"],
    )

    required_rule = next(rule for rule in report["rules"] if rule["id"] == "all_required_outputs_present")
    assert baseline["status"] == "rejected"
    assert "missing_output" in baseline["evaluation"]["rejection_rules_triggered"]
    assert required_rule["status"] == "fail"
    assert report["status"] == "artifact_gate_failed"


def test_output_manifest_requires_hash_and_byte_metadata() -> None:
    metadata_missing = {
        "artifacts": [{"path": rel} for rel in sorted(sp.PILOT_RELATIVE_OUTPUTS)],
        "unexpected_outputs": [],
    }

    assert sp.output_manifest_has_required_outputs(metadata_missing) is False


def test_output_manifest_checks_current_file_metadata(tmp_path: Path) -> None:
    for rel in sp.PILOT_RELATIVE_OUTPUTS:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".json":
            path.write_text(json.dumps({"path": rel}) + "\n", encoding="utf-8")
        else:
            path.write_text(f"{rel}\n", encoding="utf-8")

    output_manifest = sp.build_pilot_output_manifest(tmp_path)
    assert sp.output_manifest_has_required_outputs(output_manifest, tmp_path) is True

    bad_hash = deepcopy(output_manifest)
    bad_hash["artifacts"][0]["sha256"] = "0" * 64
    assert sp.output_manifest_has_required_outputs(bad_hash, tmp_path) is False

    bad_size = deepcopy(output_manifest)
    bad_size["artifacts"][0]["bytes"] += 1
    assert sp.output_manifest_has_required_outputs(bad_size, tmp_path) is False


def test_output_manifest_requires_shadow_and_executor_logs(tmp_path: Path) -> None:
    for rel in sp.PILOT_RELATIVE_OUTPUTS - {"logs/executor.log"}:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("ok\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing required output artifact"):
        sp.build_pilot_output_manifest(tmp_path)


def test_output_manifest_rejects_unexpected_artifacts(tmp_path: Path) -> None:
    for rel in sp.PILOT_RELATIVE_OUTPUTS:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("ok\n", encoding="utf-8")
    (tmp_path / "stale.json").write_text("old\n", encoding="utf-8")

    with pytest.raises(ValueError, match="unexpected output artifact"):
        sp.build_pilot_output_manifest(tmp_path)


def test_result_manifest_requires_green_invariant_and_reproducibility_reports() -> None:
    envelope = _envelope()
    kwargs = {
        "envelope": envelope,
        "output_manifest_hash": "a" * 64,
        "invariant_hash": "b" * 64,
        "baseline_hash": "c" * 64,
        "reproducibility_hash": "d" * 64,
        "started_at": sp.dt.datetime(2026, 6, 28, 12, 0, tzinfo=sp.dt.UTC),
        "finished_at": sp.dt.datetime(2026, 6, 28, 12, 0, 1, tzinfo=sp.dt.UTC),
    }

    with pytest.raises(ValueError, match="red invariant"):
        sp.build_shadow_result_manifest(
            **kwargs,
            invariant_report=_invariant_report(ok=False),
            reproducibility_report={"ok": True},
        )

    with pytest.raises(ValueError, match="red reproducibility"):
        sp.build_shadow_result_manifest(
            **kwargs,
            invariant_report=_invariant_report(),
            reproducibility_report={"ok": False},
        )


def test_observability_evidence_contains_required_structured_fields() -> None:
    envelope = _envelope()
    result = _result()
    evidence = sp.build_observability_evidence(
        envelope=envelope,
        result=result,
        output_manifest_hash="a" * 64,
        invariant_hash="b" * 64,
        baseline_hash="c" * 64,
    )
    required = {
        "shadow_id",
        "calibration_id",
        "request_id",
        "correlation_id",
        "execution_id",
        "run_fingerprint",
        "status",
        "started_at",
        "finished_at",
        "input_pack_sha256",
        "engine_commit",
        "engine_image_digest",
        "output_artifact_uri",
        "output_manifest_sha256",
        "invariant_report_sha256",
        "baseline_comparison_sha256",
        "duration_ms",
        "memory_peak_bytes",
        "cpu_time_ms",
        "failure_class",
        "retry_count",
        "runtime_activation",
        "allow_db_write",
        "allow_allocator_publish",
        "production_endpoint_activation",
        "official_result",
    }
    assert required.issubset(evidence)
    assert evidence["output_artifact_uri"] == envelope["output_artifact_uri"]
    assert evidence["engine_image_digest"] == result["engine_image_digest"]
    assert evidence["runtime_activation"] is False
    assert evidence["allow_db_write"] is False
    assert evidence["allow_allocator_publish"] is False
    assert evidence["production_endpoint_activation"] == "none"
    assert evidence["official_result"] is False


def test_readiness_manifest_requires_all_inert_fields() -> None:
    readiness = json.loads((ROOT / "artifacts" / "shadow" / sp.SHADOW_ID / "shadow_manifest.json").read_text(encoding="utf-8"))
    sp.validate_shadow_readiness_manifest_is_inert(readiness)

    for field, bad_value in (
        ("official_result", True),
        ("allocator_impact", "publish"),
        ("db_write_mode", "productive"),
        ("production_endpoint_activation", "shadow"),
        ("formula_changes", "changed"),
        ("input_pack_changes", "changed"),
        ("calibration_pack_changes", "changed"),
        ("contract_v1_changes", "changed"),
    ):
        bad = deepcopy(readiness)
        bad[field] = bad_value
        with pytest.raises(ValueError, match=field):
            sp.validate_shadow_readiness_manifest_is_inert(bad)


def test_calibration_run_matrix_hash_is_pinned(tmp_path: Path) -> None:
    source_dir = ROOT / "artifacts" / "calibration" / sp.CALIBRATION_ID
    calibration_dir = tmp_path / "artifacts" / "calibration" / sp.CALIBRATION_ID
    calibration_dir.mkdir(parents=True)
    for name in ("calibration_manifest.json", "calibration_config.json", "run_matrix.json"):
        (calibration_dir / name).write_text((source_dir / name).read_text(encoding="utf-8"), encoding="utf-8")

    sp.validate_calibration_artifact_hashes(tmp_path)
    (calibration_dir / "run_matrix.json").write_text('{"ok": true}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="run_matrix_sha256"):
        sp.validate_calibration_artifact_hashes(tmp_path)

    manifest = json.loads((calibration_dir / "calibration_manifest.json").read_text(encoding="utf-8"))
    manifest["run_matrix_sha256"] = file_sha256(calibration_dir / "run_matrix.json")
    (calibration_dir / "calibration_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="run_matrix_sha256"):
        sp.validate_calibration_artifact_hashes(tmp_path)


def test_output_isolation_rejects_dangling_symlink_and_outside_write(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.mkdir()
    try:
        os.symlink(tmp_path / "missing-target", out / "dangling")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with pytest.raises(ValueError, match="symlink"):
        sp.reject_symlinks(out)

    with pytest.raises(ValueError, match="outside output dir"):
        sp.ensure_child(tmp_path / "elsewhere.json", out)


def test_shadow_pilot_runner_generates_valid_artifact_bundle(tmp_path: Path) -> None:
    manifest = sp.run_shadow_pilot(
        output_dir=tmp_path / sp.SHADOW_PILOT_ID,
        shadow_readiness_merge_commit="a644bbd72e530ffa5555e41a2553639332b65902",
        shadow_pilot_branch_base_commit="a644bbd72e530ffa5555e41a2553639332b65902",
        allow_external_output_dir=True,
    )

    out = tmp_path / sp.SHADOW_PILOT_ID
    expected = {
        "shadow_pilot_manifest.json",
        "shadow_job_envelope.json",
        "shadow_result_manifest.json",
        "output_manifest.json",
        "invariant_report.json",
        "baseline_comparison.json",
        "reproducibility_report.json",
        "observability_evidence.json",
        "rollback_evidence.json",
        "acceptance_report.json",
        "pilot_execution_report.md",
        "logs/shadow_pilot.log",
        "logs/executor.log",
    }
    assert expected.issubset({p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()})
    assert manifest["runtime_activation"] is False
    assert manifest["A5"] == "blocked"
    assert manifest["freeze_ready"] is False

    result = json.loads((out / "shadow_result_manifest.json").read_text(encoding="utf-8"))
    sp.validate_shadow_result_manifest(result, root=ROOT)
    acceptance = json.loads((out / "acceptance_report.json").read_text(encoding="utf-8"))
    assert len(acceptance["rules"]) == 18
    review_rule = next(rule for rule in acceptance["rules"] if rule["id"] == "technical_and_quantitative_review_recorded")
    assert review_rule["status"] == "pending"
    assert review_rule["blocking"] is True
    assert manifest["output_manifest_sha256"] == file_sha256(out / "output_manifest.json")
    invariant = json.loads((out / "invariant_report.json").read_text(encoding="utf-8"))
    assert invariant["ok"] is True


def test_committed_shadow_pilot_artifacts_validate() -> None:
    out = ROOT / "artifacts" / "shadow" / sp.SHADOW_PILOT_ID
    envelope = json.loads((out / "shadow_job_envelope.json").read_text(encoding="utf-8"))
    result = json.loads((out / "shadow_result_manifest.json").read_text(encoding="utf-8"))
    manifest = json.loads((out / "shadow_pilot_manifest.json").read_text(encoding="utf-8"))
    output_manifest = json.loads((out / "output_manifest.json").read_text(encoding="utf-8"))
    invariant = json.loads((out / "invariant_report.json").read_text(encoding="utf-8"))
    observability = json.loads((out / "observability_evidence.json").read_text(encoding="utf-8"))

    sp.validate_shadow_job_envelope(envelope, root=ROOT)
    sp.validate_shadow_result_manifest(result, root=ROOT)
    assert invariant["ok"] is True
    assert sp.output_manifest_has_required_logs(output_manifest)
    assert sp.output_manifest_has_required_outputs(output_manifest, out)
    assert observability["output_artifact_uri"] == envelope["output_artifact_uri"]
    assert observability["engine_image_digest"] == result["engine_image_digest"]
    assert manifest["output_manifest_sha256"] == file_sha256(out / "output_manifest.json")
    assert manifest["A5"] == "blocked"
    assert manifest["runtime_activation"] is False
    assert manifest["freeze_ready"] is False
    assert manifest["official_result"] is False


def test_railway_ci_runs_shadow_pilot_gate() -> None:
    text = (ROOT / "docker" / "railway-ci" / "Dockerfile").read_text(encoding="utf-8")

    assert (
        "COPY artifacts/shadow/open_macro_v03_shadow_pilot_001 "
        "/app/artifacts/shadow/open_macro_v03_shadow_pilot_001"
    ) in text
    assert "tests/test_shadow_pilot.py" in text
    assert "src/shadow_pilot.py" in text
