"""Defensive-binding hardening tests for the shadow pilot evidence gates.

These cover the systemic defect surfaced across many Codex review threads on
``src/shadow_pilot.py``: trust surfaces that accept externally-derived evidence
must verify identity/binding, require evidence fields to be present, reject
malformed markers (``0 == False``), and reject non-finite numbers before a
green verdict. Each test exercises a public validator/builder with adversarial
input, matching the repository's existing ``test_*_rejects_*`` contract.
"""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from src import shadow_pilot as sp

ROOT = Path(__file__).resolve().parents[1]
CALIBRATION_RUN_MATRIX = json.loads(
    (ROOT / "artifacts" / "calibration" / sp.CALIBRATION_ID / "run_matrix.json").read_text(encoding="utf-8")
)
CALIBRATION_MANIFEST = json.loads(
    (ROOT / "artifacts" / "calibration" / sp.CALIBRATION_ID / "calibration_manifest.json").read_text(encoding="utf-8")
)
_START = sp.dt.datetime(2026, 6, 28, 12, 0, tzinfo=sp.dt.UTC)
_FINISH = sp.dt.datetime(2026, 6, 28, 12, 0, 1, tzinfo=sp.dt.UTC)


def _policy() -> dict:
    return sp.load_policy(ROOT)


def _envelope() -> dict:
    return sp.build_shadow_job_envelope(CALIBRATION_RUN_MATRIX)


def _baseline() -> dict:
    return sp.build_baseline_comparison(_policy())


def _reproducibility(envelope: dict | None = None, *, ok: bool = True) -> dict:
    envelope = envelope or _envelope()
    return {
        "ok": ok,
        "run_fingerprint": envelope["run_fingerprint"],
        "shadow_id": sp.SHADOW_ID,
        "shadow_pilot_id": sp.SHADOW_PILOT_ID,
        "calibration_id": sp.CALIBRATION_ID,
    }


def _invariant(*, ok: bool = True) -> dict:
    return {
        "ok": ok,
        "shadow_id": sp.SHADOW_ID,
        "shadow_pilot_id": sp.SHADOW_PILOT_ID,
        "calibration_id": sp.CALIBRATION_ID,
        "checks": {
            "runtime_activation_false": True,
            "allow_db_write_false": True,
            "allow_allocator_publish_false": True,
        },
    }


def _output_manifest() -> dict:
    return {
        "artifact_type": "shadow_pilot_output_manifest",
        "shadow_pilot_id": sp.SHADOW_PILOT_ID,
        "shadow_id": sp.SHADOW_ID,
        "status": "succeeded",
        "artifacts": [
            {"path": rel, "sha256": "a" * 64, "bytes": 1}
            for rel in sorted(sp.PILOT_RELATIVE_OUTPUTS)
        ],
        "unexpected_outputs": [],
    }


def _result(
    envelope: dict,
    baseline: dict,
    repro: dict,
    policy: dict,
    *,
    invariant: dict | None = None,
    calibration_run_matrix: dict | None = None,
) -> dict:
    return sp.build_shadow_result_manifest(
        envelope=envelope,
        invariant_report=invariant or _invariant(),
        baseline_comparison=baseline,
        policy=policy,
        reproducibility_report=repro,
        calibration_run_matrix=calibration_run_matrix or CALIBRATION_RUN_MATRIX,
        output_manifest_hash="a" * 64,
        invariant_hash="b" * 64,
        baseline_hash="c" * 64,
        reproducibility_hash="d" * 64,
        started_at=_START,
        finished_at=_FINISH,
    )


# ---- Thread 1: baseline identity / binding ----
@pytest.mark.parametrize("field", ["shadow_id", "shadow_pilot_id", "calibration_id", "policy_id"])
def test_evaluate_baseline_rejects_foreign_identity(field: str) -> None:
    policy = _policy()
    comparison = _baseline()
    comparison[field] = "foreign_value"
    with pytest.raises(ValueError):
        sp.evaluate_baseline_comparison(comparison, policy)


# ---- Thread 2 / B2: required materiality & divergence fields (no permissive default) ----
@pytest.mark.parametrize("field", ["max_relative_delta_pct", "return_metric_delta_pct", "latency_p95_regression_pct"])
def test_evaluate_baseline_requires_materiality_fields(field: str) -> None:
    policy = _policy()
    comparison = _baseline()
    del comparison["materiality_summary"][field]
    with pytest.raises(ValueError):
        sp.evaluate_baseline_comparison(comparison, policy)


def test_evaluate_baseline_requires_divergence_counters() -> None:
    policy = _policy()
    comparison = _baseline()
    del comparison["divergence_summary"]["mismatch_count"]
    with pytest.raises(ValueError):
        sp.evaluate_baseline_comparison(comparison, policy)


# ---- Thread 3: forbidden-change markers must not fold 0 into False ----
@pytest.mark.parametrize("bad_value", [0, 0.0, False, None, "changed"])
def test_evaluate_baseline_rejects_non_none_forbidden_change_marker(bad_value: object) -> None:
    policy = _policy()
    comparison = _baseline()
    comparison["forbidden_effects"]["formula_change"] = bad_value
    evaluation = sp.evaluate_baseline_comparison(comparison, policy)
    assert evaluation["status"] == "rejected"
    assert "formula_change" in evaluation["rejection_rules_triggered"]


# ---- Thread 6: explicit false attestation required for side-effect attempts ----
def test_evaluate_baseline_requires_explicit_false_attempt() -> None:
    policy = _policy()
    comparison = _baseline()
    del comparison["forbidden_effects"]["allocator_publish_attempt"]
    evaluation = sp.evaluate_baseline_comparison(comparison, policy)
    assert evaluation["status"] == "rejected"
    assert "allocator_publish_attempt" in evaluation["rejection_rules_triggered"]


def test_acceptance_requires_explicit_false_attempt() -> None:
    policy = _policy()
    comparison = _baseline()
    del comparison["forbidden_effects"]["allocator_publish_attempt"]
    report = sp.build_acceptance_report(
        policy=policy,
        output_manifest=_output_manifest(),
        invariant_report=_invariant(),
        baseline_comparison=comparison,
        reproducibility_report=sp.build_reproducibility_report(
            CALIBRATION_RUN_MATRIX, _envelope(), CALIBRATION_MANIFEST
        ),
        expected_run_fingerprint=_envelope()["run_fingerprint"],
    )
    rule = next(r for r in report["rules"] if r["id"] == "no_allocator_publish_attempt")
    assert rule["status"] == "fail"
    assert report["status"] == "artifact_gate_failed"


# ---- Thread 4: readiness identity & state pins ----
@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("shadow_id", "other_shadow"),
        ("status", "frozen"),
        ("A3", "other_strategy"),
        ("A4", "shadow_pilot_validated"),
        ("execution_status", "succeeded"),
    ],
)
def test_readiness_requires_state_and_identity_pins(field: str, bad: str) -> None:
    readiness = json.loads(
        (ROOT / "artifacts" / "shadow" / sp.SHADOW_ID / "shadow_manifest.json").read_text(encoding="utf-8")
    )
    readiness[field] = bad
    with pytest.raises(ValueError, match=field):
        sp.validate_shadow_readiness_manifest_is_inert(readiness)


# ---- Thread 5: reproducibility identity before success ----
@pytest.mark.parametrize("field", ["shadow_id", "shadow_pilot_id", "calibration_id"])
def test_result_requires_reproducibility_identity(field: str) -> None:
    policy = _policy()
    envelope = _envelope()
    repro = _reproducibility(envelope)
    repro[field] = "foreign_value"
    with pytest.raises(ValueError):
        _result(envelope, _baseline(), repro, policy)


@pytest.mark.parametrize("field", ["shadow_id", "shadow_pilot_id", "calibration_id"])
def test_acceptance_requires_reproducibility_identity(field: str) -> None:
    policy = _policy()
    repro = _reproducibility()
    repro[field] = "foreign_value"
    with pytest.raises(ValueError):
        sp.build_acceptance_report(
            policy=policy,
            output_manifest=_output_manifest(),
            invariant_report=_invariant(),
            baseline_comparison=_baseline(),
            reproducibility_report=repro,
            expected_run_fingerprint=_envelope()["run_fingerprint"],
        )


# ---- Thread 7: non-finite numbers must not survive into a succeeded result ----
def test_validate_result_rejects_non_finite_materiality() -> None:
    policy = _policy()
    envelope = _envelope()
    result = _result(envelope, _baseline(), _reproducibility(envelope), policy)
    # material_divergence=True satisfies the schema's conditional so the only thing
    # standing between a NaN metric and a succeeded result is an explicit finiteness
    # check (NaN is an unbounded "number" instance JSON Schema accepts).
    result["materiality_summary"]["material_divergence"] = True
    result["materiality_summary"]["return_metric_delta_pct"] = float("nan")
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_shadow_result_manifest(result, root=ROOT)


# ---- Thread 8: output_artifact_uri pinned to this pilot, not just regex ----
def test_validate_result_pins_output_artifact_uri() -> None:
    policy = _policy()
    envelope = _envelope()
    result = _result(envelope, _baseline(), _reproducibility(envelope), policy)
    result["output_artifact_uri"] = f"artifact://shadow/{sp.SHADOW_ID}/some_other_pilot"
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_shadow_result_manifest(result, root=ROOT)


def test_build_result_rejects_misbound_envelope_uri() -> None:
    policy = _policy()
    envelope = _envelope()
    envelope["output_artifact_uri"] = f"artifact://shadow/{sp.SHADOW_ID}/some_other_pilot"
    with pytest.raises(ValueError):
        _result(envelope, _baseline(), _reproducibility(envelope), policy)


# ---- Missing schemas: data-layer hardening for the unschema'd evidence artifacts ----
def _committed(name: str) -> dict:
    return json.loads(
        (ROOT / "artifacts" / "shadow" / sp.SHADOW_PILOT_ID / name).read_text(encoding="utf-8")
    )


def test_baseline_comparison_schema_validates_committed_and_rejects_drift() -> None:
    sp.validate_baseline_comparison(_committed("baseline_comparison.json"), root=ROOT)

    foreign = _committed("baseline_comparison.json")
    foreign["shadow_id"] = "other_shadow"
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_baseline_comparison(foreign, root=ROOT)

    missing = _committed("baseline_comparison.json")
    del missing["materiality_summary"]["return_metric_delta_pct"]
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_baseline_comparison(missing, root=ROOT)

    numeric_marker = _committed("baseline_comparison.json")
    numeric_marker["forbidden_effects"]["formula_change"] = 0
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_baseline_comparison(numeric_marker, root=ROOT)


def test_reproducibility_report_schema_validates_committed_and_rejects_drift() -> None:
    sp.validate_reproducibility_report(_committed("reproducibility_report.json"), root=ROOT)

    foreign = _committed("reproducibility_report.json")
    foreign["shadow_pilot_id"] = "other_pilot"
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_reproducibility_report(foreign, root=ROOT)


def test_output_manifest_schema_validates_committed_and_rejects_drift() -> None:
    sp.validate_pilot_output_manifest(_committed("output_manifest.json"), root=ROOT)

    bad_status = _committed("output_manifest.json")
    bad_status["status"] = "failed"
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_pilot_output_manifest(bad_status, root=ROOT)


# ---- A12 (new finding): invariant report re-evaluates baseline, ignores stale status ----
def test_invariant_report_reevaluates_stale_baseline_status(tmp_path: Path) -> None:
    policy = _policy()
    out = tmp_path / sp.SHADOW_PILOT_ID
    (out / "logs").mkdir(parents=True)
    (out / "logs" / "shadow_pilot.log").write_text("x\n", encoding="utf-8")
    (out / "logs" / "executor.log").write_text("x\n", encoding="utf-8")
    baseline = _baseline()
    baseline["divergence_summary"]["mismatch_count"] = 5
    baseline["status"] = "pass"  # stale green status on a dirty body
    report = sp.build_invariant_report(
        output_dir=out,
        envelope=_envelope(),
        baseline_comparison=baseline,
        reproducibility_report=_reproducibility(),
        policy=policy,
    )
    assert report["checks"]["baseline_comparison_pass"] is False


# ---- Review 4588225478 #1: invariant-report identity must be bound before trusting ok ----
@pytest.mark.parametrize("field", ["shadow_id", "shadow_pilot_id", "calibration_id"])
def test_result_requires_invariant_identity(field: str) -> None:
    policy = _policy()
    envelope = _envelope()
    invariant = _invariant()
    invariant[field] = "foreign_value"
    with pytest.raises(ValueError):
        _result(envelope, _baseline(), _reproducibility(envelope), policy, invariant=invariant)


@pytest.mark.parametrize("field", ["shadow_id", "shadow_pilot_id", "calibration_id"])
def test_acceptance_requires_invariant_identity(field: str) -> None:
    policy = _policy()
    invariant = _invariant()
    invariant[field] = "foreign_value"
    with pytest.raises(ValueError):
        sp.build_acceptance_report(
            policy=policy,
            output_manifest=_output_manifest(),
            invariant_report=invariant,
            baseline_comparison=_baseline(),
            reproducibility_report=_reproducibility(),
            expected_run_fingerprint=_envelope()["run_fingerprint"],
        )


# ---- Review 4588225478 #2: pin the result engine image digest ----
def test_validate_result_pins_engine_image_digest() -> None:
    policy = _policy()
    envelope = _envelope()
    result = _result(envelope, _baseline(), _reproducibility(envelope), policy)
    result["engine_image_digest"] = "sha256:" + "f" * 64
    with pytest.raises(jsonschema.ValidationError):
        sp.validate_shadow_result_manifest(result, root=ROOT)


# ---- Review 4588225478 #3: derive no-db invariant from executor evidence ----
def test_invariant_no_db_access_derived_from_executor_log(tmp_path: Path) -> None:
    policy = _policy()
    out = tmp_path / sp.SHADOW_PILOT_ID
    (out / "logs").mkdir(parents=True)
    (out / "logs" / "shadow_pilot.log").write_text(
        "shadow_pilot_id=open_macro_v03_shadow_pilot_001 runtime_activation=false "
        "allow_db_write=false allow_allocator_publish=false production_endpoint_activation=none\n",
        encoding="utf-8",
    )
    # Tampered executor log claiming DB access.
    (out / "logs" / "executor.log").write_text(
        "isolated_external_executor_no_productive_runtime_docker network=none db_access=true "
        "input_pack_mount=read_only source_tree_writes=false\n",
        encoding="utf-8",
    )
    report = sp.build_invariant_report(
        output_dir=out,
        envelope=_envelope(),
        baseline_comparison=_baseline(),
        reproducibility_report=_reproducibility(),
        policy=policy,
    )
    assert report["checks"]["no_db_access"] is False


# ---- Review 4588225478 #4: recompute the expected run fingerprint from the matrix ----
def test_result_recomputes_run_fingerprint_from_matrix() -> None:
    policy = _policy()
    envelope = _envelope()
    bogus = "0" * 64
    envelope["run_fingerprint"] = bogus
    repro = _reproducibility(envelope)  # self-consistent with the bogus fingerprint
    with pytest.raises(ValueError):
        _result(envelope, _baseline(), repro, policy)


def test_acceptance_recomputes_run_fingerprint_from_matrix() -> None:
    policy = _policy()
    bogus = "0" * 64
    envelope = _envelope()
    envelope["run_fingerprint"] = bogus
    repro = _reproducibility(envelope)
    with pytest.raises(ValueError):
        sp.build_acceptance_report(
            policy=policy,
            output_manifest=_output_manifest(),
            invariant_report=_invariant(),
            baseline_comparison=_baseline(),
            reproducibility_report=repro,
            expected_run_fingerprint=bogus,
            calibration_run_matrix=CALIBRATION_RUN_MATRIX,
        )
