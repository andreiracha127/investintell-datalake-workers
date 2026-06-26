"""A3 parity runner."""

from __future__ import annotations

import uuid
from typing import Any

from investintell_quant_core.hashing.canonical import stable_hash

from investintell_quant_engine._paths import ensure_repo_paths
from investintell_quant_engine.preflight import validate_offline_request, validate_runtime_disabled

ensure_repo_paths()

import qc_a3_core as qc


def run_parity_job(
    config: qc.A3ParityConfig,
    *,
    job_id: str | None = None,
    jobs: int = 1,
    offline: bool = True,
) -> dict[str, Any]:
    validate_offline_request(offline=offline, jobs=jobs)
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

