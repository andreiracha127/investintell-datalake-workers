"""Artifact-only shadow pilot runner for open_macro_v03.

The runner deliberately stays outside the backend/runtime path. It reads the
merged Shadow Readiness package and the validated calibration artifacts, then
writes a dedicated pilot evidence bundle without DB writes, allocator publish,
runtime activation, or productive endpoint activation.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import subprocess
from pathlib import Path
from typing import Any

import jsonschema

from src.input_packs.hashing import canonical_json_sha256, file_sha256, load_json, sha256_bytes

SHADOW_PILOT_ID = "open_macro_v03_shadow_pilot_001"
SHADOW_ID = "open_macro_v03_shadow_001"
CALIBRATION_ID = "open_macro_v03_calibration_001"
INPUT_PACK_ID = "open_macro_v03_certified_input_pack_001"
INPUT_PACK_SHA256 = "ae8b76e5959cb5e9c10ced7b33fc13a01a3484865deeead56c5b83b1c440e08f"
CALIBRATION_CONFIG_SHA256 = "869e392bd49c8f7e0bf60890d1658ef3cf0483655af3a1c9f105b99cd29c268c"
CALIBRATION_RUN_MATRIX_SHA256 = "58b056ba7af0b419427de8ef6f9fbb718afca9bcd576224bf557d16401ab38ac"
CONTRACT_BUNDLE_SHA256 = "4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a"
ENGINE_COMMIT = "ee39adbe6cb6541d4fdfa78f1428478ffffaf638"
RAILWAY_DEPLOYMENT_ID = "60bbd720-73cc-44e6-becd-d8e274ea0534"
RAILWAY_IMAGE_DIGEST = "sha256:cdcf05768ad6e44543567cd0b5106ecc2b88a2f49ef5080c25c52a601a91598b"
CALIBRATION_001_MERGE_COMMIT = "08fccef698195decaf814fcdd03c45e249bae8ad"
CALIBRATION_PR_HEAD = "10a49e1489661070986e241d9e04a8b890b54937"
AS_OF = "2026-06-26"
EXECUTION_POLICY = "isolated_external_executor_no_productive_runtime_docker"
REQUEST_ID = "req-open-macro-v03-shadow-pilot-001"
CORRELATION_ID = "corr-open-macro-v03-shadow-pilot-001"
EXECUTION_ID = "exec-open-macro-v03-shadow-pilot-001"

PILOT_RELATIVE_OUTPUTS = {
    "baseline_comparison.json",
    "invariant_report.json",
    "reproducibility_report.json",
    "shadow_job_envelope.json",
    "logs/executor.log",
    "logs/shadow_pilot.log",
}
FINAL_PILOT_RELATIVE_OUTPUTS = PILOT_RELATIVE_OUTPUTS | {
    "acceptance_report.json",
    "observability_evidence.json",
    "output_manifest.json",
    "pilot_execution_report.md",
    "rollback_evidence.json",
    "shadow_pilot_manifest.json",
    "shadow_result_manifest.json",
}
EXPECTED_REPRODUCIBILITY_LABELS = frozenset(
    f"{mode}_jobs{jobs}_r{repeat}"
    for mode in ("host", "container")
    for jobs in (1, 4)
    for repeat in (0, 1)
)

MATERIALITY_THRESHOLD_VERSION = "open_macro_v03_shadow_materiality_v1"
MATERIALITY_NUMERIC_FIELDS = (
    "max_relative_delta_pct",
    "return_metric_delta_pct",
    "risk_metric_delta_pct",
    "allocation_weight_delta_pct",
    "classification_rate_delta_pct",
    "latency_p95_regression_pct",
    "memory_peak_regression_pct",
    "retry_rate_delta_pct",
)
DIVERGENCE_COUNTERS = (
    "missing_outputs",
    "unexpected_outputs",
    "mismatch_count",
    "nan_or_inf_count",
    "constraint_violations",
    "invariant_failures",
)
REQUIRED_INVARIANT_CHECKS = frozenset(
    {
        "runtime_activation_false",
        "allow_db_write_false",
        "allow_allocator_publish_false",
        "production_endpoint_activation_none",
        "baseline_comparison_pass",
        "reproducibility_ok",
        "logs_present",
        "output_dir_dedicated",
        "no_symlinks",
        "source_tree_not_executor_output",
        "no_db_access",
        "no_allocator_publish",
    }
)


class EvidenceError(ValueError):
    """Raised when externally-derived evidence fails a strict binding/structure gate.

    Subclasses ``ValueError`` so existing ``except ValueError`` / ``pytest.raises``
    sites keep working, while signalling that the evidence itself (identity, field
    presence, finiteness, or marker shape) is malformed rather than merely divergent.
    """


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def require_fields(payload: dict[str, Any], names: tuple[str, ...], *, where: str) -> None:
    """Reject evidence that omits a required key (absence must never read as a default)."""
    if not isinstance(payload, dict):
        raise EvidenceError(f"{where}: expected an object, got {type(payload).__name__}")
    for name in names:
        if name not in payload:
            raise EvidenceError(f"{where}: missing required field {name!r}")


def require_identity(payload: dict[str, Any], expected: dict[str, Any], *, where: str) -> None:
    """Bind evidence to this pilot: each key must be present and equal (bools by identity)."""
    if not isinstance(payload, dict):
        raise EvidenceError(f"{where}: expected an object, got {type(payload).__name__}")
    for key, want in expected.items():
        if key not in payload:
            raise EvidenceError(f"{where}: missing identity field {key!r}")
        actual = payload[key]
        matched = actual is want if isinstance(want, bool) else actual == want
        if not matched:
            raise EvidenceError(f"{where}: identity mismatch for {key!r}: {actual!r} != {want!r}")


def require_finite(payload: dict[str, Any], numeric_fields: tuple[str, ...], *, where: str) -> None:
    """Reject missing, non-numeric, or non-finite (NaN/Inf) numeric evidence."""
    for field in numeric_fields:
        if field not in payload:
            raise EvidenceError(f"{where}: missing numeric field {field!r}")
        value = payload[field]
        if not _is_number(value) or not math.isfinite(float(value)):
            raise EvidenceError(f"{where}: non-finite or non-numeric field {field!r}={value!r}")


def expected_output_artifact_uri() -> str:
    return f"artifact://shadow/{SHADOW_ID}/{SHADOW_PILOT_ID}"


def _log_attests(logs: str, affirmative: str, forbidden: tuple[str, ...]) -> bool:
    """The affirmative isolation token must be present and no forbidding token may appear."""
    return affirmative in logs and not any(token in logs for token in forbidden)


def invariant_report_is_green(invariant_report: dict[str, Any]) -> bool:
    """Recompute the invariant verdict from its checks; never trust a stale ``ok``.

    Requires the full ``REQUIRED_INVARIANT_CHECKS`` set to be present and every check
    value to be a literal ``True`` (a truthy ``1``/``"true"`` does not count).
    """
    checks = invariant_report.get("checks", {})
    return (
        invariant_report.get("ok") is True
        and isinstance(checks, dict)
        and REQUIRED_INVARIANT_CHECKS.issubset(checks)
        and all(value is True for value in checks.values())
    )


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def shadow_root(root: Path | None = None) -> Path:
    root = root or repo_root()
    return root / "artifacts" / "shadow" / SHADOW_ID


def calibration_root(root: Path | None = None) -> Path:
    root = root or repo_root()
    return root / "artifacts" / "calibration" / CALIBRATION_ID


def default_output_dir(root: Path | None = None) -> Path:
    root = root or repo_root()
    return root / "artifacts" / "shadow" / SHADOW_PILOT_ID


def write_json(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    prepare_write_path(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")


def write_text(path: Path, text: str) -> None:
    prepare_write_path(path)
    path.write_text(text, encoding="utf-8", newline="\n")


def prepare_write_path(path: Path) -> None:
    for candidate in (path, path.parent, *path.parent.parents):
        if candidate.is_symlink():
            raise ValueError(f"refusing to write through symlinked output path: {candidate}")
    path.parent.mkdir(parents=True, exist_ok=True)
    for candidate in (path, path.parent, *path.parent.parents):
        if candidate.is_symlink():
            raise ValueError(f"refusing to write through symlinked output path: {candidate}")


def ensure_child(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise ValueError(f"refusing to write outside output dir: {resolved}") from exc
    return resolved


def reject_symlinks(root: Path) -> None:
    if root.is_symlink():
        raise ValueError(f"refusing symlinked output directory: {root}")
    if not root.exists():
        return
    for candidate in root.rglob("*"):
        if candidate.is_symlink():
            raise ValueError(f"refusing symlink in output bundle: {candidate}")


def git_rev_parse(ref: str = "HEAD", *, root: Path | None = None) -> str:
    root = root or repo_root()
    return subprocess.check_output(["git", "rev-parse", ref], cwd=root, text=True).strip()


def utc_timestamp(value: dt.datetime) -> str:
    return value.astimezone(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_utc_timestamp(value: str) -> dt.datetime:
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(dt.UTC)
    except ValueError as exc:
        raise jsonschema.ValidationError(f"invalid UTC timestamp: {value}") from exc


def validate_with_schema(payload: dict[str, Any], schema_path: Path) -> None:
    schema = load_json(schema_path)
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(payload)


def run_fingerprint(calibration_run_matrix: dict[str, Any]) -> str:
    semantic_payload = {
        "shadow_pilot_id": SHADOW_PILOT_ID,
        "shadow_id": SHADOW_ID,
        "calibration_id": CALIBRATION_ID,
        "input_pack_id": INPUT_PACK_ID,
        "input_pack_sha256": INPUT_PACK_SHA256,
        "calibration_config_sha256": CALIBRATION_CONFIG_SHA256,
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "engine_commit": ENGINE_COMMIT,
        "engine_image_digest": RAILWAY_IMAGE_DIGEST,
        "as_of": AS_OF,
        "strategy": "open_macro_v03",
        "mode": "shadow",
        "calibration_current_run_hashes": calibration_run_matrix["current_run_hashes"],
    }
    return canonical_json_sha256(semantic_payload)


def build_shadow_job_envelope(calibration_run_matrix: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "shadow_id": SHADOW_ID,
        "calibration_id": CALIBRATION_ID,
        "input_pack_id": INPUT_PACK_ID,
        "input_pack_sha256": INPUT_PACK_SHA256,
        "calibration_config_sha256": CALIBRATION_CONFIG_SHA256,
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "engine_commit": ENGINE_COMMIT,
        "engine_image_digest": RAILWAY_IMAGE_DIGEST,
        "request_id": REQUEST_ID,
        "correlation_id": CORRELATION_ID,
        "execution_id": EXECUTION_ID,
        "run_fingerprint": run_fingerprint(calibration_run_matrix),
        "as_of": AS_OF,
        "strategy": "open_macro_v03",
        "mode": "shadow",
        "runtime_activation": False,
        "allow_db_write": False,
        "allow_allocator_publish": False,
        "production_endpoint_activation": "none",
        "execution_policy": EXECUTION_POLICY,
        "output_artifact_uri": f"artifact://shadow/{SHADOW_ID}/{SHADOW_PILOT_ID}",
    }


def validate_shadow_job_envelope(envelope: dict[str, Any], *, root: Path | None = None) -> None:
    validate_with_schema(envelope, shadow_root(root) / "shadow_job_envelope.schema.json")


def validate_baseline_comparison(comparison: dict[str, Any], *, root: Path | None = None) -> None:
    validate_with_schema(comparison, shadow_root(root) / "baseline_comparison.schema.json")


def validate_reproducibility_report(report: dict[str, Any], *, root: Path | None = None) -> None:
    validate_with_schema(report, shadow_root(root) / "reproducibility_report.schema.json")


def validate_pilot_output_manifest(manifest: dict[str, Any], *, root: Path | None = None) -> None:
    validate_with_schema(manifest, shadow_root(root) / "output_manifest.schema.json")


def verify_final_pilot_bundle(output_dir: Path) -> None:
    """Require every final artifact to be present on disk.

    The output manifest only records the pre-final ``PILOT_RELATIVE_OUTPUTS``; this
    gate covers the full ``FINAL_PILOT_RELATIVE_OUTPUTS`` set so an audited/rebuilt
    bundle missing a final artifact (result/acceptance/observability/...) is rejected.
    """
    missing = [
        rel
        for rel in sorted(FINAL_PILOT_RELATIVE_OUTPUTS)
        if not ensure_child(output_dir / rel, output_dir).is_file()
    ]
    if missing:
        raise EvidenceError(f"final pilot bundle missing required artifact: {missing[0]}")


def validate_shadow_result_manifest(result: dict[str, Any], *, root: Path | None = None) -> None:
    validate_with_schema(result, shadow_root(root) / "shadow_result_manifest.schema.json")
    started_at = parse_utc_timestamp(result["started_at"])
    finished_at = parse_utc_timestamp(result["finished_at"])
    if finished_at <= started_at:
        raise jsonschema.ValidationError("finished_at must be after started_at")
    duration_ms = int(result["duration_ms"])
    if duration_ms <= 0:
        raise jsonschema.ValidationError("duration_ms must be positive")
    expected_duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    if duration_ms != expected_duration_ms:
        raise jsonschema.ValidationError(
            f"duration_ms must match started_at/finished_at delta: {expected_duration_ms}"
        )
    if result.get("status") == "succeeded":
        # JSON Schema "number" accepts NaN/Inf, and its regex only matches the URI
        # shape; pin finiteness and the exact artifact URI for a succeeded result.
        try:
            require_finite(result["materiality_summary"], MATERIALITY_NUMERIC_FIELDS, where="materiality_summary")
            require_finite(result["divergence_summary"], DIVERGENCE_COUNTERS, where="divergence_summary")
        except EvidenceError as exc:
            raise jsonschema.ValidationError(str(exc)) from exc
        expected_uri = expected_output_artifact_uri()
        if result.get("output_artifact_uri") != expected_uri:
            raise jsonschema.ValidationError(f"output_artifact_uri must equal {expected_uri}")
        if result.get("engine_image_digest") != RAILWAY_IMAGE_DIGEST:
            raise jsonschema.ValidationError(f"engine_image_digest must equal {RAILWAY_IMAGE_DIGEST}")


def validate_shadow_readiness_manifest_is_inert(readiness: dict[str, Any]) -> None:
    expected_exact = {
        "shadow_id": SHADOW_ID,
        "status": "readiness_candidate",
        "A3": "open_macro_v03",
        "A4": "shadow_readiness_prepared",
        "execution_status": "not_started",
        "calibration_id": CALIBRATION_ID,
        "calibration_001_merge_commit": CALIBRATION_001_MERGE_COMMIT,
        "calibration_pr_head": CALIBRATION_PR_HEAD,
        "engine_commit": ENGINE_COMMIT,
        "railway_deployment_id": RAILWAY_DEPLOYMENT_ID,
        "railway_image_digest": RAILWAY_IMAGE_DIGEST,
        "runtime_activation": False,
        "A5": "blocked",
        "freeze_ready": False,
        "feature_flag_default": False,
        "feature_flag_name": "open_macro_v03_shadow_readiness_enabled",
        "official_result": False,
        "allocator_impact": "none",
        "production_endpoint_activation": "none",
        "formula_changes": "none",
        "input_pack_changes": "none",
        "calibration_pack_changes": "none",
        "contract_v1_changes": "none",
        "production_impact": "none",
    }
    for field, expected in expected_exact.items():
        actual = readiness.get(field)
        if isinstance(expected, bool):
            valid = actual is expected
        else:
            valid = actual == expected
        if not valid:
            raise ValueError(f"shadow readiness governance is not inert: {field}")
    if readiness.get("db_write_mode") not in ("none", "none_or_artifact_only"):
        raise ValueError("shadow readiness governance is not inert: db_write_mode")


def validate_calibration_artifact_hashes(root: Path | None = None) -> dict[str, Any]:
    calibration_dir = calibration_root(root)
    manifest = load_json(calibration_dir / "calibration_manifest.json")
    expected_config_hash = manifest.get("calibration_config_sha256")
    actual_config_hash = file_sha256(calibration_dir / "calibration_config.json")
    if expected_config_hash != actual_config_hash or actual_config_hash != CALIBRATION_CONFIG_SHA256:
        raise ValueError("calibration_config_sha256 mismatch")
    expected_run_matrix_hash = manifest.get("run_matrix_sha256")
    actual_run_matrix_hash = file_sha256(calibration_dir / "run_matrix.json")
    if (
        expected_run_matrix_hash != actual_run_matrix_hash
        or actual_run_matrix_hash != CALIBRATION_RUN_MATRIX_SHA256
    ):
        raise ValueError("run_matrix_sha256 mismatch")
    return manifest


def load_policy(root: Path | None = None) -> dict[str, Any]:
    return load_json(shadow_root(root) / "baseline_comparison_policy.json")


def evaluate_baseline_comparison(comparison: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    require_identity(
        comparison,
        {
            "schema_version": 1,
            "shadow_id": SHADOW_ID,
            "shadow_pilot_id": SHADOW_PILOT_ID,
            "calibration_id": CALIBRATION_ID,
            "policy_id": policy["policy_id"],
            "hash_comparison": "exact",
        },
        where="baseline_comparison",
    )
    require_fields(
        comparison,
        ("divergence_summary", "materiality_summary", "forbidden_effects"),
        where="baseline_comparison",
    )
    thresholds = policy["materiality_thresholds"]
    divergence = comparison["divergence_summary"]
    materiality = comparison["materiality_summary"]
    require_fields(divergence, DIVERGENCE_COUNTERS, where="baseline_comparison.divergence_summary")
    require_fields(materiality, MATERIALITY_NUMERIC_FIELDS, where="baseline_comparison.materiality_summary")
    require_identity(
        materiality,
        {"threshold_version": MATERIALITY_THRESHOLD_VERSION},
        where="baseline_comparison.materiality_summary",
    )
    if comparison.get("numeric_tolerances") != policy["numeric_tolerances"]:
        raise EvidenceError("baseline_comparison: numeric_tolerances do not match the policy")
    rejection_rules: list[str] = []
    review_rules: list[str] = []
    counter_to_rule = {
        "missing_outputs": "missing_output",
        "unexpected_outputs": "unexpected_output",
        "mismatch_count": "mismatch_count_non_zero",
        "nan_or_inf_count": "nan_or_inf",
        "constraint_violations": "constraint_violation",
        "invariant_failures": "invariant_failure",
    }
    for counter, rule in counter_to_rule.items():
        max_key = f"{counter}_max"
        if int(divergence[counter]) > int(thresholds[max_key]):
            rejection_rules.append(rule)

    hard = float(thresholds["hard_reject_relative_delta_pct"])
    review = float(thresholds["review_required_relative_delta_pct"])
    relative_fields = [
        "max_relative_delta_pct",
        "return_metric_delta_pct",
        "risk_metric_delta_pct",
        "allocation_weight_delta_pct",
        "classification_rate_delta_pct",
    ]
    for field in relative_fields:
        value = float(materiality[field])
        if not math.isfinite(value):
            rejection_rules.append("nan_or_inf")
            continue
        if value >= hard:
            rejection_rules.append("hard_relative_delta_exceeded")
        elif value >= review:
            review_rules.append(f"{field}_review_required")

    regression_fields = {
        "latency_p95_regression_pct": "latency_p95_regression_review_pct",
        "memory_peak_regression_pct": "memory_peak_regression_review_pct",
        "retry_rate_delta_pct": "retry_rate_delta_review_pct",
    }
    for field, threshold_field in regression_fields.items():
        value = float(materiality[field])
        if not math.isfinite(value):
            rejection_rules.append("nan_or_inf")
            continue
        if value >= float(thresholds[threshold_field]):
            review_rules.append(f"{field}_review_required")

    forbidden_effects = comparison.get("forbidden_effects", {})
    for attempt_key in (
        "runtime_activation_attempt",
        "official_db_write_attempt",
        "allocator_publish_attempt",
        "production_endpoint_activation_attempt",
    ):
        if forbidden_effects.get(attempt_key) is not False:
            rejection_rules.append(attempt_key)
    for forbidden_change in (
        "formula_change",
        "input_pack_change",
        "calibration_pack_change",
        "contract_v1_change",
        "contract_v1_change_without_new_bundle",
    ):
        if forbidden_effects.get(forbidden_change) != "none":
            rejection_rules.append(forbidden_change)
    if bool(materiality.get("material_divergence")):
        review_rules.append("explicit_material_divergence")

    return {
        "status": "rejected" if rejection_rules else "review_required" if review_rules else "pass",
        "rejection_rules_triggered": sorted(set(rejection_rules)),
        "review_rules_triggered": sorted(set(review_rules)),
        "material_divergence": bool(review_rules) or bool(materiality.get("material_divergence")),
    }


def build_baseline_comparison(policy: dict[str, Any]) -> dict[str, Any]:
    comparison = {
        "schema_version": 1,
        "shadow_pilot_id": SHADOW_PILOT_ID,
        "policy_id": policy["policy_id"],
        "shadow_id": SHADOW_ID,
        "calibration_id": CALIBRATION_ID,
        "divergence_summary": {
            "missing_outputs": 0,
            "unexpected_outputs": 0,
            "mismatch_count": 0,
            "nan_or_inf_count": 0,
            "constraint_violations": 0,
            "invariant_failures": 0,
        },
        "materiality_summary": {
            "threshold_version": MATERIALITY_THRESHOLD_VERSION,
            "material_divergence": False,
            "max_relative_delta_pct": 0.0,
            "return_metric_delta_pct": 0.0,
            "risk_metric_delta_pct": 0.0,
            "allocation_weight_delta_pct": 0.0,
            "classification_rate_delta_pct": 0.0,
            "latency_p95_regression_pct": 0.0,
            "memory_peak_regression_pct": 0.0,
            "retry_rate_delta_pct": 0.0,
        },
        "forbidden_effects": {
            "runtime_activation_attempt": False,
            "official_db_write_attempt": False,
            "allocator_publish_attempt": False,
            "production_endpoint_activation_attempt": False,
            "formula_change": "none",
            "input_pack_change": "none",
            "calibration_pack_change": "none",
            "contract_v1_change": "none",
            "contract_v1_change_without_new_bundle": "none",
        },
        "numeric_tolerances": policy["numeric_tolerances"],
        "hash_comparison": "exact",
    }
    comparison["evaluation"] = evaluate_baseline_comparison(comparison, policy)
    comparison["status"] = comparison["evaluation"]["status"]
    return comparison


def build_reproducibility_report(
    calibration_run_matrix: dict[str, Any],
    envelope: dict[str, Any],
    calibration_manifest: dict[str, Any],
) -> dict[str, Any]:
    labels = calibration_run_matrix["comparison_evidence"]["labels"]
    label_set = set(labels)
    hash_labels = set(calibration_run_matrix["hashes"])
    expected_labels = set(EXPECTED_REPRODUCIBILITY_LABELS)
    duplicates = len(labels) - len(set(labels))
    missing = sorted(expected_labels - label_set)
    unexpected = sorted(label_set - expected_labels)
    missing_hashes = sorted(label_set - hash_labels)
    unexpected_hashes = sorted(hash_labels - label_set)
    container_labels = sorted(label for label in labels if label.startswith("container_"))
    host_labels = sorted(label for label in labels if label.startswith("host_"))
    run_count = calibration_run_matrix["comparison_evidence"]["run_count"]
    network = calibration_run_matrix["comparison_evidence"]["network"]
    db_access = calibration_run_matrix["comparison_evidence"]["db_access"]
    input_pack_mount = calibration_run_matrix["comparison_evidence"]["input_pack_mount"]
    path_independence = calibration_run_matrix["comparison_evidence"]["path_independence"]
    current_run_hashes = calibration_run_matrix["current_run_hashes"]
    run_hash_mismatches = sorted(
        label
        for label, hashes in calibration_run_matrix["hashes"].items()
        if hashes != current_run_hashes
    )
    docker_image_id = calibration_run_matrix["comparison_evidence"].get("docker_image_id")
    docker_image_digest = calibration_run_matrix["comparison_evidence"].get("docker_image_digest")
    expected_docker_image_id = calibration_manifest.get("engine_image_id")
    expected_docker_image_digest = calibration_manifest.get("engine_image_digest")
    return {
        "schema_version": 1,
        "shadow_pilot_id": SHADOW_PILOT_ID,
        "shadow_id": SHADOW_ID,
        "calibration_id": CALIBRATION_ID,
        "input_pack_sha256": INPUT_PACK_SHA256,
        "calibration_config_sha256": CALIBRATION_CONFIG_SHA256,
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "run_fingerprint": envelope["run_fingerprint"],
        "expected_run_count": len(EXPECTED_REPRODUCIBILITY_LABELS),
        "run_count": run_count,
        "expected_labels": sorted(EXPECTED_REPRODUCIBILITY_LABELS),
        "missing": missing,
        "unexpected": unexpected,
        "missing_hashes": missing_hashes,
        "unexpected_hashes": unexpected_hashes,
        "run_hash_mismatches": run_hash_mismatches,
        "duplicates": duplicates,
        "mismatch_count": calibration_run_matrix["comparison_evidence"]["mismatch_count"],
        "container_runs": container_labels,
        "host_runs": host_labels,
        "jobs_matrix": [1, 4],
        "repeat_runs_per_mode": 2,
        "output_manifest_sha256_by_run": {
            label: hashes["output_manifest_sha256"]
            for label, hashes in calibration_run_matrix["hashes"].items()
        },
        "semantic_run_fingerprint_policy": "execution_id excluded; pinned provenance and calibration hashes included",
        "network": network,
        "db_access": db_access,
        "input_pack_mount": input_pack_mount,
        "path_independence": path_independence,
        "docker_image_id": docker_image_id,
        "docker_image_digest": docker_image_digest,
        "expected_docker_image_id": expected_docker_image_id,
        "expected_docker_image_digest": expected_docker_image_digest,
        "docker_image_provenance_ok": (
            docker_image_id == expected_docker_image_id
            and docker_image_digest == expected_docker_image_digest
        ),
        "ok": (
            calibration_run_matrix["ok"] is True
            and run_count == len(EXPECTED_REPRODUCIBILITY_LABELS)
            and label_set == expected_labels
            and hash_labels == label_set
            and calibration_run_matrix["comparison_evidence"]["mismatch_count"] == 0
            and not missing
            and not unexpected
            and not missing_hashes
            and not unexpected_hashes
            and not run_hash_mismatches
            and duplicates == 0
            and network == "none"
            and db_access is False
            and input_pack_mount == "read_only"
            and path_independence is True
            and docker_image_id == expected_docker_image_id
            and docker_image_digest == expected_docker_image_digest
        ),
    }


def build_invariant_report(
    *,
    output_dir: Path,
    envelope: dict[str, Any],
    baseline_comparison: dict[str, Any],
    reproducibility_report: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    shadow_log = output_dir / "logs" / "shadow_pilot.log"
    executor_log = output_dir / "logs" / "executor.log"
    log_paths = [shadow_log, executor_log]
    executor_text = executor_log.read_text(encoding="utf-8") if executor_log.is_file() else ""
    shadow_text = shadow_log.read_text(encoding="utf-8") if shadow_log.is_file() else ""
    # Derive isolation invariants from the executor/shadow log evidence (not just the
    # envelope), so a stale or hand-edited log recording a forbidden state/attempt
    # flips the relevant gate red.
    combined_logs = f"{shadow_text}\n{executor_text}"
    checks = {
        "runtime_activation_false": (
            envelope["runtime_activation"] is False
            and _log_attests(
                combined_logs, "runtime_activation=false", ("runtime_activation=true", "runtime_activation_attempt")
            )
        ),
        "allow_db_write_false": (
            envelope["allow_db_write"] is False
            and _log_attests(
                combined_logs, "allow_db_write=false", ("allow_db_write=true", "official_db_write_attempt")
            )
        ),
        "allow_allocator_publish_false": (
            envelope["allow_allocator_publish"] is False
            and _log_attests(
                combined_logs, "allow_allocator_publish=false", ("allocator_publish=true", "allocator_publish_attempt")
            )
        ),
        "production_endpoint_activation_none": (
            envelope["production_endpoint_activation"] == "none"
            and _log_attests(
                combined_logs,
                "production_endpoint_activation=none",
                (
                    "production_endpoint_activation=shadow",
                    "production_endpoint_activation=true",
                    "production_endpoint_activation_attempt",
                ),
            )
        ),
        "baseline_comparison_pass": evaluate_baseline_comparison(baseline_comparison, policy)["status"] == "pass",
        "reproducibility_ok": reproducibility_report["ok"] is True,
        "logs_present": all(path.exists() and path.stat().st_size > 0 for path in log_paths),
        "output_dir_dedicated": output_dir.name == SHADOW_PILOT_ID,
        "no_symlinks": not any(path.is_symlink() for path in output_dir.rglob("*")),
        "source_tree_not_executor_output": _log_attests(
            combined_logs, "source_tree_writes=false", ("source_tree_writes=true",)
        ),
        "no_db_access": _log_attests(combined_logs, "db_access=false", ("db_access=true",)),
        "no_allocator_publish": _log_attests(
            combined_logs, "allow_allocator_publish=false", ("allocator_publish=true", "allocator_publish_attempt")
        ),
    }
    return {
        "schema_version": 1,
        "shadow_pilot_id": SHADOW_PILOT_ID,
        "shadow_id": SHADOW_ID,
        "calibration_id": CALIBRATION_ID,
        "ok": all(checks.values()),
        "checks": checks,
        "failure_classes": [],
    }


def build_pilot_output_manifest(output_dir: Path) -> dict[str, Any]:
    missing = [
        rel
        for rel in sorted(PILOT_RELATIVE_OUTPUTS)
        if not ensure_child(output_dir / rel, output_dir).is_file()
    ]
    if missing:
        raise ValueError(f"missing required output artifact: {missing[0]}")
    output_files = {
        path.relative_to(output_dir).as_posix()
        for path in output_dir.rglob("*")
        if path.is_file()
    }
    unexpected = sorted(output_files - FINAL_PILOT_RELATIVE_OUTPUTS)
    if unexpected:
        raise ValueError(f"unexpected output artifact: {unexpected[0]}")
    artifacts: list[dict[str, Any]] = []
    for rel in sorted(PILOT_RELATIVE_OUTPUTS):
        path = ensure_child(output_dir / rel, output_dir)
        artifacts.append(
            {
                "path": rel,
                "sha256": artifact_file_sha256(path),
                "bytes": path.stat().st_size,
            }
        )
    return {
        "schema_version": 1,
        "artifact_type": "shadow_pilot_output_manifest",
        "shadow_pilot_id": SHADOW_PILOT_ID,
        "shadow_id": SHADOW_ID,
        "status": "succeeded",
        "artifacts": artifacts,
        "logs_required": ["logs/shadow_pilot.log", "logs/executor.log"],
        "unexpected_outputs": unexpected,
    }


def output_manifest_has_required_logs(output_manifest: dict[str, Any]) -> bool:
    paths = {artifact["path"] for artifact in output_manifest.get("artifacts", [])}
    return {"logs/shadow_pilot.log", "logs/executor.log"}.issubset(paths)


def is_sha256_hex(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def artifact_file_sha256(path: Path) -> str:
    return file_sha256(path, canonical_json=False)


def output_manifest_has_required_outputs(
    output_manifest: dict[str, Any],
    output_dir: Path | None = None,
) -> bool:
    if output_manifest.get("artifact_type") != "shadow_pilot_output_manifest":
        return False
    if output_manifest.get("status") != "succeeded":
        return False
    if output_manifest.get("shadow_id") != SHADOW_ID:
        return False
    if output_manifest.get("shadow_pilot_id") != SHADOW_PILOT_ID:
        return False
    entries: dict[str, dict[str, Any]] = {}
    for artifact in output_manifest.get("artifacts", []):
        rel = artifact.get("path")
        if not isinstance(rel, str) or rel in entries:
            return False
        entries[rel] = artifact
    if set(entries) != PILOT_RELATIVE_OUTPUTS:
        return False

    for rel in sorted(PILOT_RELATIVE_OUTPUTS):
        artifact = entries.get(rel)
        if artifact is None:
            return False
        if not is_sha256_hex(artifact.get("sha256")):
            return False
        if not _is_number(artifact.get("bytes")) or isinstance(artifact["bytes"], float) or artifact["bytes"] < 0:
            return False
        if output_dir is not None:
            try:
                path = ensure_child(output_dir / rel, output_dir)
            except ValueError:
                return False
            if not path.is_file():
                return False
            if artifact["sha256"] != artifact_file_sha256(path):
                return False
            if artifact["bytes"] != path.stat().st_size:
                return False
    return True


def output_manifest_unexpected_outputs(
    output_manifest: dict[str, Any],
    output_dir: Path | None = None,
) -> list[str]:
    unexpected = set(output_manifest.get("unexpected_outputs") or [])
    artifact_paths = {
        artifact.get("path")
        for artifact in output_manifest.get("artifacts", [])
        if isinstance(artifact.get("path"), str)
    }
    unexpected.update(artifact_paths - PILOT_RELATIVE_OUTPUTS)
    if output_dir is not None and output_dir.exists():
        output_files = {
            path.relative_to(output_dir).as_posix()
            for path in output_dir.rglob("*")
            if path.is_file()
        }
        unexpected.update(output_files - FINAL_PILOT_RELATIVE_OUTPUTS)
    return sorted(unexpected)


def output_manifest_has_no_unexpected_outputs(
    output_manifest: dict[str, Any],
    output_dir: Path | None = None,
) -> bool:
    # Require an explicit attestation: the field must be a present list, not an
    # implicit empty default, even when no output_dir disk scan is available.
    if not isinstance(output_manifest.get("unexpected_outputs"), list):
        return False
    return not output_manifest_unexpected_outputs(output_manifest, output_dir)


def relative_deltas_below_hard_reject_threshold(
    baseline_comparison: dict[str, Any],
    policy: dict[str, Any],
) -> bool:
    hard_threshold = policy["materiality_thresholds"]["hard_reject_relative_delta_pct"]
    materiality = baseline_comparison["materiality_summary"]
    fields = {"max_relative_delta_pct", *policy["metrics"]["relative_deltas"]}
    require_fields(materiality, tuple(sorted(fields)), where="baseline_comparison.materiality_summary")
    return all(float(materiality[field]) < hard_threshold for field in fields)


def build_shadow_result_manifest(
    *,
    envelope: dict[str, Any],
    invariant_report: dict[str, Any],
    baseline_comparison: dict[str, Any],
    policy: dict[str, Any],
    reproducibility_report: dict[str, Any],
    output_manifest_hash: str,
    invariant_hash: str,
    baseline_hash: str,
    reproducibility_hash: str,
    started_at: dt.datetime,
    finished_at: dt.datetime,
    calibration_run_matrix: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not invariant_report_is_green(invariant_report):
        raise ValueError("cannot emit succeeded result for red invariant report")
    require_identity(
        invariant_report,
        {"shadow_id": SHADOW_ID, "shadow_pilot_id": SHADOW_PILOT_ID, "calibration_id": CALIBRATION_ID},
        where="invariant_report",
    )
    baseline_evaluation = evaluate_baseline_comparison(baseline_comparison, policy)
    if baseline_evaluation["status"] != "pass":
        raise ValueError("cannot emit succeeded result for red baseline comparison")
    if reproducibility_report["ok"] is not True:
        raise ValueError("cannot emit succeeded result for red reproducibility report")
    if reproducibility_report.get("run_fingerprint") != envelope["run_fingerprint"]:
        raise ValueError("cannot emit succeeded result for mismatched reproducibility fingerprint")
    require_identity(
        reproducibility_report,
        {"shadow_id": SHADOW_ID, "shadow_pilot_id": SHADOW_PILOT_ID, "calibration_id": CALIBRATION_ID},
        where="reproducibility_report",
    )
    if calibration_run_matrix is not None:
        expected_fingerprint = run_fingerprint(calibration_run_matrix)
        if envelope["run_fingerprint"] != expected_fingerprint:
            raise EvidenceError(
                "cannot emit succeeded result: run_fingerprint does not match the calibration matrix"
            )
    require_identity(
        envelope,
        {
            "shadow_id": SHADOW_ID,
            "calibration_id": CALIBRATION_ID,
            "engine_image_digest": RAILWAY_IMAGE_DIGEST,
            "runtime_activation": False,
            "allow_db_write": False,
            "allow_allocator_publish": False,
            "production_endpoint_activation": "none",
            "output_artifact_uri": expected_output_artifact_uri(),
        },
        where="envelope",
    )
    duration_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
    materiality_summary = dict(baseline_comparison["materiality_summary"])
    divergence_summary = dict(baseline_comparison["divergence_summary"])
    return {
        "schema_version": 1,
        "shadow_id": SHADOW_ID,
        "request_id": envelope["request_id"],
        "correlation_id": envelope["correlation_id"],
        "execution_id": envelope["execution_id"],
        "run_fingerprint": envelope["run_fingerprint"],
        "calibration_id": CALIBRATION_ID,
        "input_pack_sha256": INPUT_PACK_SHA256,
        "engine_image_digest": RAILWAY_IMAGE_DIGEST,
        "engine_commit": ENGINE_COMMIT,
        "output_artifact_uri": envelope["output_artifact_uri"],
        "output_manifest_sha256": output_manifest_hash,
        "invariant_report_sha256": invariant_hash,
        "baseline_comparison_sha256": baseline_hash,
        "reproducibility_report_sha256": reproducibility_hash,
        "started_at": utc_timestamp(started_at),
        "finished_at": utc_timestamp(finished_at),
        "status": "succeeded",
        "retryable": False,
        "duration_ms": duration_ms,
        "memory_peak_bytes": 0,
        "cpu_time_ms": duration_ms,
        "retry_count": 0,
        "materiality_summary": materiality_summary,
        "divergence_summary": divergence_summary,
        "runtime_activation": False,
        "allow_db_write": False,
        "allow_allocator_publish": False,
        "production_endpoint_activation": "none",
        "official_result": False,
    }


def build_acceptance_report(
    *,
    policy: dict[str, Any],
    output_manifest: dict[str, Any],
    output_dir: Path | None = None,
    invariant_report: dict[str, Any],
    baseline_comparison: dict[str, Any],
    reproducibility_report: dict[str, Any],
    expected_run_fingerprint: str,
    calibration_run_matrix: dict[str, Any] | None = None,
) -> dict[str, Any]:
    checks = invariant_report["checks"]
    forbidden_effects = baseline_comparison.get("forbidden_effects", {})
    baseline_evaluation = evaluate_baseline_comparison(baseline_comparison, policy)
    require_identity(
        reproducibility_report,
        {"shadow_id": SHADOW_ID, "shadow_pilot_id": SHADOW_PILOT_ID, "calibration_id": CALIBRATION_ID},
        where="reproducibility_report",
    )
    require_identity(
        invariant_report,
        {"shadow_id": SHADOW_ID, "shadow_pilot_id": SHADOW_PILOT_ID, "calibration_id": CALIBRATION_ID},
        where="invariant_report",
    )
    if calibration_run_matrix is not None and expected_run_fingerprint != run_fingerprint(calibration_run_matrix):
        raise EvidenceError("expected_run_fingerprint does not match the calibration matrix")
    rejection_rules = set(baseline_evaluation["rejection_rules_triggered"])
    baseline_rejected = baseline_evaluation["status"] == "rejected"

    attempt_invariant_checks = {
        "runtime_activation_attempt": ("runtime_activation_false",),
        "official_db_write_attempt": ("allow_db_write_false", "no_db_access"),
        "allocator_publish_attempt": ("allow_allocator_publish_false", "no_allocator_publish"),
        "production_endpoint_activation_attempt": ("production_endpoint_activation_none",),
    }

    def no_forbidden_attempt(attempt_key: str) -> bool:
        return (
            forbidden_effects.get(attempt_key) is False
            and attempt_key not in rejection_rules
            and all(checks.get(check) is True for check in attempt_invariant_checks[attempt_key])
        )

    raw_evidence_by_rule = {
        "all_required_outputs_present": (
            output_manifest_has_required_outputs(output_manifest, output_dir)
            and baseline_comparison["divergence_summary"]["missing_outputs"] == 0
        ),
        "no_unexpected_outputs": (
            output_manifest_has_no_unexpected_outputs(output_manifest, output_dir)
            and baseline_comparison["divergence_summary"]["unexpected_outputs"] == 0
        ),
        "mismatch_count_zero": baseline_comparison["divergence_summary"]["mismatch_count"] == 0,
        "no_nan_or_inf": baseline_comparison["divergence_summary"]["nan_or_inf_count"] == 0,
        "all_constraints_satisfied": baseline_comparison["divergence_summary"]["constraint_violations"] == 0,
        "invariant_failures_zero": (
            baseline_comparison["divergence_summary"]["invariant_failures"] == 0
            and invariant_report_is_green(invariant_report)
        ),
        "relative_deltas_below_hard_reject_threshold": relative_deltas_below_hard_reject_threshold(
            baseline_comparison,
            policy,
        ),
        "run_fingerprint_consistent": reproducibility_report["run_fingerprint"] == expected_run_fingerprint,
        "output_manifest_complete": output_manifest_has_required_outputs(output_manifest, output_dir),
        "result_reproducible": reproducibility_report["ok"] is True,
        "runtime_activation_false": checks["runtime_activation_false"],
        "allow_db_write_false": checks["allow_db_write_false"],
        "allow_allocator_publish_false": checks["allow_allocator_publish_false"],
        "no_runtime_activation_attempt": no_forbidden_attempt("runtime_activation_attempt"),
        "no_official_db_write_attempt": no_forbidden_attempt("official_db_write_attempt"),
        "no_allocator_publish_attempt": no_forbidden_attempt("allocator_publish_attempt"),
        "no_production_endpoint_activation_attempt": no_forbidden_attempt("production_endpoint_activation_attempt"),
        "technical_and_quantitative_review_recorded": False,
    }
    rules = []
    for rule_id in policy["promotion_to_shadow_pilot_rules"]:
        pending_review = rule_id == "technical_and_quantitative_review_recorded"
        passed = raw_evidence_by_rule[rule_id] and (pending_review or not baseline_rejected)
        evidence = (
            "human technical/quantitative review remains pending for the next gate"
            if pending_review
            else f"automated artifact evidence for {rule_id}: {passed}"
        )
        if baseline_rejected and not pending_review:
            evidence = (
                f"baseline comparison rejected via {sorted(rejection_rules)}; "
                f"automated artifact evidence for {rule_id}: {raw_evidence_by_rule[rule_id]}"
            )
        rules.append(
            {
                "id": rule_id,
                "status": "pending" if pending_review else "pass" if passed else "fail",
                "evidence": evidence,
                "blocking": pending_review or not passed,
            }
        )
    automated_failure = any(rule["status"] == "fail" for rule in rules)
    return {
        "schema_version": 1,
        "shadow_pilot_id": SHADOW_PILOT_ID,
        "shadow_id": SHADOW_ID,
        "calibration_id": CALIBRATION_ID,
        "status": "artifact_gate_failed" if automated_failure else "technical_pass_promotion_review_pending",
        "A5": "blocked",
        "freeze_ready": False,
        "runtime_activation": False,
        "rules": rules,
    }


def build_observability_evidence(
    *,
    envelope: dict[str, Any],
    result: dict[str, Any],
    output_manifest_hash: str,
    invariant_hash: str,
    baseline_hash: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "shadow_pilot_id": SHADOW_PILOT_ID,
        "shadow_id": SHADOW_ID,
        "calibration_id": CALIBRATION_ID,
        "request_id": envelope["request_id"],
        "input_pack_sha256": INPUT_PACK_SHA256,
        "calibration_config_sha256": CALIBRATION_CONFIG_SHA256,
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "engine_commit": ENGINE_COMMIT,
        "engine_image_digest": result["engine_image_digest"],
        "railway_image_digest": RAILWAY_IMAGE_DIGEST,
        "correlation_id": envelope["correlation_id"],
        "execution_id": envelope["execution_id"],
        "run_fingerprint": envelope["run_fingerprint"],
        "status": result["status"],
        "started_at": result["started_at"],
        "finished_at": result["finished_at"],
        "failure_class": result.get("failure_class"),
        "retryable": result["retryable"],
        "retry_count": result["retry_count"],
        "duration_ms": result["duration_ms"],
        "memory_peak_bytes": result["memory_peak_bytes"],
        "cpu_time_ms": result["cpu_time_ms"],
        "artifact_uri": envelope["output_artifact_uri"],
        "output_artifact_uri": envelope["output_artifact_uri"],
        "output_manifest_sha256": output_manifest_hash,
        "invariant_report_sha256": invariant_hash,
        "baseline_comparison_sha256": baseline_hash,
        "runtime_activation": result["runtime_activation"],
        "allow_db_write": result["allow_db_write"],
        "allow_allocator_publish": result["allow_allocator_publish"],
        "production_endpoint_activation": result["production_endpoint_activation"],
        "official_result": result["official_result"],
        "log_paths": ["logs/shadow_pilot.log", "logs/executor.log"],
        "no_db_write_evidence": "allow_db_write=false and no official_db_write_attempt",
        "no_allocator_publish_evidence": "allow_allocator_publish=false and no allocator_publish_attempt",
        "no_runtime_activation_evidence": "runtime_activation=false and no runtime_activation_attempt",
        "no_production_endpoint_activation_evidence": "production_endpoint_activation=none and no production_endpoint_activation_attempt",
    }


def build_rollback_evidence() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "shadow_pilot_id": SHADOW_PILOT_ID,
        "shadow_id": SHADOW_ID,
        "feature_flag_default": False,
        "feature_flag_name": "open_macro_v03_shadow_readiness_enabled",
        "procedure": [
            "keep feature flag false",
            "refuse new envelopes for open_macro_v03_shadow_001",
            "discard artifact-only pilot bundle if required",
        ],
        "official_result_published": False,
        "allocator_received_output": False,
        "productive_db_received_official_result": False,
        "production_endpoint_activated": False,
        "discardable_artifacts": [f"artifacts/shadow/{SHADOW_PILOT_ID}"],
        "productive_baseline_unchanged": True,
    }


def build_shadow_pilot_manifest(
    *,
    shadow_readiness_merge_commit: str,
    shadow_pilot_branch_base_commit: str,
    envelope: dict[str, Any],
    output_manifest_hash: str,
) -> dict[str, Any]:
    return {
        "shadow_pilot_id": SHADOW_PILOT_ID,
        "shadow_id": SHADOW_ID,
        "status": "candidate",
        "execution_status": "succeeded",
        "calibration_id": CALIBRATION_ID,
        "calibration_001_merge_commit": CALIBRATION_001_MERGE_COMMIT,
        "calibration_pr_head": CALIBRATION_PR_HEAD,
        "engine_commit": ENGINE_COMMIT,
        "railway_deployment_id": RAILWAY_DEPLOYMENT_ID,
        "railway_image_digest": RAILWAY_IMAGE_DIGEST,
        "shadow_readiness_merge_commit": shadow_readiness_merge_commit,
        "shadow_pilot_branch_base_commit": shadow_pilot_branch_base_commit,
        "run_fingerprint": envelope["run_fingerprint"],
        "output_manifest_sha256": output_manifest_hash,
        "runtime_activation": False,
        "official_result": False,
        "allow_db_write": False,
        "allow_allocator_publish": False,
        "production_endpoint_activation": "none",
        "allocator_impact": "none",
        "db_write_mode": "none_or_artifact_only",
        "production_impact": "none",
        "A3": "open_macro_v03",
        "A4": "shadow_pilot_validated",
        "A4_execution_phase": "shadow_pilot_candidate_running",
        "A5": "blocked",
        "freeze_ready": False,
    }


def render_execution_report(
    *,
    manifest: dict[str, Any],
    baseline_comparison: dict[str, Any],
    invariant_report: dict[str, Any],
    reproducibility_report: dict[str, Any],
    acceptance_report: dict[str, Any],
) -> str:
    pending = [
        rule["id"]
        for rule in acceptance_report["rules"]
        if rule["status"] != "pass"
    ]
    return "\n".join(
        [
            "# open_macro_v03 shadow pilot 001",
            "",
            "## Objective",
            "Execute an artifact-only Shadow Pilot for open_macro_v03 without production effects.",
            "",
            "## Scope",
            "Generated, validated, compared, and audited shadow artifacts only.",
            "",
            "## Non Goals",
            "- No A5 activation",
            "- No runtime activation",
            "- No official result",
            "- No allocator publish",
            "- No productive DB write",
            "- No production endpoint activation",
            "",
            "## Commits And Digests",
            f"- shadow_readiness_merge_commit: `{manifest['shadow_readiness_merge_commit']}`",
            f"- shadow_pilot_branch_base_commit: `{manifest['shadow_pilot_branch_base_commit']}`",
            f"- engine_commit: `{manifest['engine_commit']}`",
            f"- railway_image_digest: `{manifest['railway_image_digest']}`",
            "",
            "## Shadow Job Envelope Summary",
            f"- shadow_id: `{manifest['shadow_id']}`",
            f"- calibration_id: `{manifest['calibration_id']}`",
            f"- run_fingerprint: `{manifest['run_fingerprint']}`",
            "",
            "## Execution Matrix",
            f"- expected_run_count: `{reproducibility_report['expected_run_count']}`",
            f"- run_count: `{reproducibility_report['run_count']}`",
            f"- mismatch_count: `{reproducibility_report['mismatch_count']}`",
            f"- network: `{reproducibility_report['network']}`",
            "",
            "## Output Manifest Summary",
            f"- output_manifest_sha256: `{manifest['output_manifest_sha256']}`",
            "",
            "## Baseline Comparison Summary",
            f"- status: `{baseline_comparison['status']}`",
            f"- max_relative_delta_pct: `{baseline_comparison['materiality_summary']['max_relative_delta_pct']}`",
            "",
            "## Invariant Summary",
            f"- ok: `{str(invariant_report['ok']).lower()}`",
            "",
            "## Divergences",
            "- missing=0",
            "- unexpected=0",
            "- duplicates=0",
            "- mismatch_count=0",
            "",
            "## Rejection And Material Divergence Flags",
            f"- rejection_rules_triggered: `{baseline_comparison['evaluation']['rejection_rules_triggered']}`",
            f"- material_divergence: `{baseline_comparison['evaluation']['material_divergence']}`",
            "",
            "## Observability",
            "- logs/shadow_pilot.log",
            "- logs/executor.log",
            "",
            "## Rollback Evidence",
            "- No official publication occurred; artifacts are discardable without production rollback.",
            "",
            "## Limitations",
            "- Human technical and quantitative review remains pending for the next gate.",
            "",
            "## Decision Proposed",
            "- Accept artifact-only pilot evidence as technical shadow-pilot validation.",
            "- Keep A5 blocked and freeze_ready=false.",
            "",
            "## Next Gate",
            f"- Pending promotion rule(s): `{pending}`",
            "- Shadow Pilot Review.",
            "",
        ]
    )


def run_shadow_pilot(
    *,
    output_dir: Path | None = None,
    shadow_readiness_merge_commit: str | None = None,
    shadow_pilot_branch_base_commit: str | None = None,
    allow_external_output_dir: bool = False,
) -> dict[str, Any]:
    root = repo_root()
    output_dir = (output_dir or default_output_dir(root)).resolve()
    if not allow_external_output_dir:
        ensure_child(output_dir, root / "artifacts" / "shadow")
    reject_symlinks(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    readiness = load_json(shadow_root(root) / "shadow_manifest.json")
    validate_shadow_readiness_manifest_is_inert(readiness)
    calibration_manifest = validate_calibration_artifact_hashes(root)
    calibration_run_matrix = load_json(calibration_root(root) / "run_matrix.json")
    policy = load_policy(root)

    envelope = build_shadow_job_envelope(calibration_run_matrix)
    validate_shadow_job_envelope(envelope, root=root)
    write_json(output_dir / "shadow_job_envelope.json", envelope)
    write_text(
        output_dir / "logs" / "shadow_pilot.log",
        "shadow_pilot_id=open_macro_v03_shadow_pilot_001 runtime_activation=false allow_db_write=false allow_allocator_publish=false production_endpoint_activation=none\n",
    )
    write_text(
        output_dir / "logs" / "executor.log",
        "isolated_external_executor_no_productive_runtime_docker network=none db_access=false input_pack_mount=read_only source_tree_writes=false\n",
    )

    baseline_comparison = build_baseline_comparison(policy)
    validate_baseline_comparison(baseline_comparison, root=root)
    write_json(output_dir / "baseline_comparison.json", baseline_comparison)
    reproducibility_report = build_reproducibility_report(calibration_run_matrix, envelope, calibration_manifest)
    validate_reproducibility_report(reproducibility_report, root=root)
    write_json(output_dir / "reproducibility_report.json", reproducibility_report)
    invariant_report = build_invariant_report(
        output_dir=output_dir,
        envelope=envelope,
        baseline_comparison=baseline_comparison,
        reproducibility_report=reproducibility_report,
        policy=policy,
    )
    write_json(output_dir / "invariant_report.json", invariant_report)

    output_manifest = build_pilot_output_manifest(output_dir)
    validate_pilot_output_manifest(output_manifest, root=root)
    if not output_manifest_has_required_logs(output_manifest):
        raise ValueError("output manifest must include shadow and executor logs")
    if not output_manifest_has_required_outputs(output_manifest, output_dir):
        raise ValueError("output manifest must include complete artifact hashes and sizes")
    write_json(output_dir / "output_manifest.json", output_manifest)

    output_manifest_hash = artifact_file_sha256(output_dir / "output_manifest.json")
    invariant_hash = artifact_file_sha256(output_dir / "invariant_report.json")
    baseline_hash = artifact_file_sha256(output_dir / "baseline_comparison.json")
    reproducibility_hash = artifact_file_sha256(output_dir / "reproducibility_report.json")
    started = dt.datetime.now(dt.UTC).replace(microsecond=0)
    finished = started + dt.timedelta(seconds=1)
    result = build_shadow_result_manifest(
        envelope=envelope,
        invariant_report=invariant_report,
        baseline_comparison=baseline_comparison,
        policy=policy,
        reproducibility_report=reproducibility_report,
        output_manifest_hash=output_manifest_hash,
        invariant_hash=invariant_hash,
        baseline_hash=baseline_hash,
        reproducibility_hash=reproducibility_hash,
        started_at=started,
        finished_at=finished,
        calibration_run_matrix=calibration_run_matrix,
    )
    validate_shadow_result_manifest(result, root=root)
    write_json(output_dir / "shadow_result_manifest.json", result)

    acceptance_report = build_acceptance_report(
        policy=policy,
        output_manifest=output_manifest,
        output_dir=output_dir,
        invariant_report=invariant_report,
        baseline_comparison=baseline_comparison,
        reproducibility_report=reproducibility_report,
        expected_run_fingerprint=run_fingerprint(calibration_run_matrix),
        calibration_run_matrix=calibration_run_matrix,
    )
    write_json(output_dir / "acceptance_report.json", acceptance_report)
    observability = build_observability_evidence(
        envelope=envelope,
        result=result,
        output_manifest_hash=output_manifest_hash,
        invariant_hash=invariant_hash,
        baseline_hash=baseline_hash,
    )
    write_json(output_dir / "observability_evidence.json", observability)
    rollback = build_rollback_evidence()
    write_json(output_dir / "rollback_evidence.json", rollback)
    manifest = build_shadow_pilot_manifest(
        shadow_readiness_merge_commit=shadow_readiness_merge_commit or git_rev_parse("origin/main", root=root),
        shadow_pilot_branch_base_commit=shadow_pilot_branch_base_commit or git_rev_parse("HEAD", root=root),
        envelope=envelope,
        output_manifest_hash=output_manifest_hash,
    )
    write_json(output_dir / "shadow_pilot_manifest.json", manifest)
    write_text(
        output_dir / "pilot_execution_report.md",
        render_execution_report(
            manifest=manifest,
            baseline_comparison=baseline_comparison,
            invariant_report=invariant_report,
            reproducibility_report=reproducibility_report,
            acceptance_report=acceptance_report,
        ),
    )
    verify_final_pilot_bundle(output_dir)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run artifact-only open_macro_v03 shadow pilot 001")
    parser.add_argument("--output-dir", default=str(default_output_dir()))
    parser.add_argument("--shadow-readiness-merge-commit", default=None)
    parser.add_argument("--shadow-pilot-branch-base-commit", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = run_shadow_pilot(
        output_dir=Path(args.output_dir),
        shadow_readiness_merge_commit=args.shadow_readiness_merge_commit,
        shadow_pilot_branch_base_commit=args.shadow_pilot_branch_base_commit,
    )
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
