"""A3 parity runner."""

from __future__ import annotations

import uuid
from typing import Any

from investintell_quant_core.hashing.canonical import stable_hash

from investintell_quant_engine._paths import ensure_repo_paths
from investintell_quant_engine.preflight import validate_offline_request, validate_runtime_disabled

ensure_repo_paths()

import qc_a3_core as qc

PARENT_HASH_FIELDS = ("l2_macro_logical_hash", "revision_uncertainty_logical_hash")


def a3_input_bundle_logical_hash(parent_hashes: dict[str, str]) -> str:
    return stable_hash(
        {
            "schema_version": 1,
            "artifact_type": "a3_qc_parity_input_bundle",
            "parent_hashes": {field: parent_hashes[field] for field in PARENT_HASH_FIELDS},
        }
    )


def _validate_expected_hash(*, name: str, expected: str | None, actual: str) -> None:
    if expected is not None and expected != actual:
        raise ValueError(f"a3 parity request {name} mismatch: expected {expected}, got {actual}")


def _derive_request_pin_hashes(
    config: qc.A3ParityConfig,
    *,
    include_config_catalog_hash: bool,
) -> dict[str, Any]:
    _feature_manifest, _l2_path, l2_hash, _l2_records = qc.load_l2_macro_for_config(config)
    uncertainty_manifest, uncertainty_hash, _uncertainty_rows = (
        qc.load_revision_uncertainty_for_config(config)
    )
    parent_uncertainty_l2 = (uncertainty_manifest.get("parent_hashes") or {}).get(
        "l2_macro_logical_hash"
    )
    if parent_uncertainty_l2 is not None and str(parent_uncertainty_l2) != l2_hash:
        raise ValueError(
            "a3 parity request revision_uncertainty parent mismatch: "
            f"expected {l2_hash}, got {parent_uncertainty_l2}"
        )
    parent_hashes = {
        "l2_macro_logical_hash": l2_hash,
        "revision_uncertainty_logical_hash": uncertainty_hash,
    }
    result: dict[str, Any] = {
        "input_bundle_logical_hash": a3_input_bundle_logical_hash(parent_hashes),
        "expected_parent_hashes": parent_hashes,
    }
    if include_config_catalog_hash:
        _a31, _catalog_a31_hash, _normalized_catalog, catalog_hash = qc.load_a31_from_catalog(
            config_catalog=config.config_catalog,
            l2_macro_logical_hash=l2_hash,
            a31_name=config.a31_name,
        )
        result["config_catalog_hash"] = catalog_hash
    return result


def validate_request_pins(
    config: qc.A3ParityConfig,
    *,
    expected_input_bundle_logical_hash: str | None = None,
    expected_config_catalog_hash: str | None = None,
    expected_parent_hashes: dict[str, str | None] | None = None,
) -> None:
    expected_parent_hashes = expected_parent_hashes or {}
    if (
        expected_input_bundle_logical_hash is None
        and expected_config_catalog_hash is None
        and not any(expected_parent_hashes.get(field) for field in PARENT_HASH_FIELDS)
    ):
        return
    actual = _derive_request_pin_hashes(
        config,
        include_config_catalog_hash=expected_config_catalog_hash is not None,
    )
    _validate_expected_hash(
        name="input_bundle_logical_hash",
        expected=expected_input_bundle_logical_hash,
        actual=actual["input_bundle_logical_hash"],
    )
    if expected_config_catalog_hash is not None:
        _validate_expected_hash(
            name="config_catalog_hash",
            expected=expected_config_catalog_hash,
            actual=actual["config_catalog_hash"],
        )
    actual_parent_hashes = actual["expected_parent_hashes"]
    for field in PARENT_HASH_FIELDS:
        _validate_expected_hash(
            name=f"expected_parent_hashes.{field}",
            expected=expected_parent_hashes.get(field),
            actual=actual_parent_hashes[field],
        )


def run_parity_job(
    config: qc.A3ParityConfig,
    *,
    job_id: str | None = None,
    jobs: int = 1,
    offline: bool = True,
    expected_input_bundle_logical_hash: str | None = None,
    expected_config_catalog_hash: str | None = None,
    expected_parent_hashes: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    validate_offline_request(offline=offline, jobs=jobs)
    validate_request_pins(
        config,
        expected_input_bundle_logical_hash=expected_input_bundle_logical_hash,
        expected_config_catalog_hash=expected_config_catalog_hash,
        expected_parent_hashes=expected_parent_hashes,
    )
    report = qc.run_parity(config)
    validate_runtime_disabled(report)
    return result_from_report(
        report,
        job_id=job_id or str(uuid.uuid4()),
        jobs=jobs,
        artifact_prefix=str(config.output_dir),
    )


def result_from_report(
    report: dict[str, Any],
    *,
    job_id: str,
    jobs: int,
    artifact_prefix: str,
) -> dict[str, Any]:
    comparison = report.get("comparison") or {}
    status = "succeeded" if comparison.get("status") == "passed" else "failed"
    output_hashes = {
        "runtime_replay_logical_hash": report["runtime_replay_logical_hash"],
        "counterfactual_replay_logical_hash": report["counterfactual_replay_logical_hash"],
        "metrics_canonical_logical_hash": report["metrics_canonical_logical_hash"],
        "metrics_raw_sha256": report["metrics_raw_sha256"],
        "model_evaluation_hash": report["model_evaluation_hash"],
    }
    fingerprint_payload = {
        "schema_version": 1,
        "job_type": "a3_qc_parity",
        "a31_config_hash": report["a31_config_hash"],
        "a32_config_hash": report["a32_config_hash"],
        "parent_hashes": {
            "l2_macro_logical_hash": (report.get("parent_hashes") or {}).get(
                "l2_macro_logical_hash"
            ),
            "revision_uncertainty_logical_hash": (report.get("parent_hashes") or {}).get(
                "revision_uncertainty_logical_hash"
            ),
        },
        "output_logical_hashes": output_hashes,
        "runtime_activation": False,
    }
    return {
        "schema_version": 1,
        "job_type": "a3_qc_parity",
        "job_id": job_id,
        "execution_id": report["execution_id"],
        "run_fingerprint": stable_hash(fingerprint_payload),
        "status": status,
        "classification": comparison.get("status"),
        "jobs": jobs,
        "artifact_prefix": artifact_prefix,
        "output_logical_hashes": output_hashes,
        "errors": [] if status == "succeeded" else comparison.get("mismatches", []),
        "runtime_activation": False,
        "a3_status": "open_macro_v03",
        "a4_status": report.get("a4_status"),
        "a5_status": report.get("a5_status"),
    }

