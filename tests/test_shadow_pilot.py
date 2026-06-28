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


def _envelope() -> dict:
    return sp.build_shadow_job_envelope(CALIBRATION_RUN_MATRIX)


def _result() -> dict:
    envelope = _envelope()
    return sp.build_shadow_result_manifest(
        envelope=envelope,
        output_manifest_hash="a" * 64,
        invariant_hash="b" * 64,
        baseline_hash="c" * 64,
        reproducibility_hash="d" * 64,
        started_at=sp.dt.datetime(2026, 6, 28, 12, 0, tzinfo=sp.dt.UTC),
        finished_at=sp.dt.datetime(2026, 6, 28, 12, 0, 1, tzinfo=sp.dt.UTC),
    )


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


def test_output_manifest_requires_shadow_and_executor_logs(tmp_path: Path) -> None:
    for rel in sp.PILOT_RELATIVE_OUTPUTS - {"logs/executor.log"}:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("ok\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing required output artifact"):
        sp.build_pilot_output_manifest(tmp_path)


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


def test_committed_shadow_pilot_artifacts_validate() -> None:
    out = ROOT / "artifacts" / "shadow" / sp.SHADOW_PILOT_ID
    envelope = json.loads((out / "shadow_job_envelope.json").read_text(encoding="utf-8"))
    result = json.loads((out / "shadow_result_manifest.json").read_text(encoding="utf-8"))
    manifest = json.loads((out / "shadow_pilot_manifest.json").read_text(encoding="utf-8"))
    output_manifest = json.loads((out / "output_manifest.json").read_text(encoding="utf-8"))

    sp.validate_shadow_job_envelope(envelope, root=ROOT)
    sp.validate_shadow_result_manifest(result, root=ROOT)
    assert sp.output_manifest_has_required_logs(output_manifest)
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
