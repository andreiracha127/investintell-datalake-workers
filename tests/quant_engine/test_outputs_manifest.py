from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))
sys.path.insert(0, str(ROOT))

from investintell_quant_engine.outputs_manifest import (
    NON_SEMANTIC_FIELDS,
    OPERATIONAL_FIELDS,
    VOLATILE_FIELDS,
    build_outputs_manifest,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _job_result(*, execution_id: str, job_id: str, prefix: str, fingerprint: str) -> dict:
    return {
        "schema_version": 1,
        "job_type": "a3_qc_parity",
        "execution_id": execution_id,
        "job_id": job_id,
        "artifact_prefix": prefix,
        "created_at": "2026-06-26T00:00:00+00:00",
        "run_fingerprint": fingerprint,
        "status": "succeeded",
        "output_logical_hashes": {"metrics": "abc"},
    }


def test_raw_manifest_lists_all_files_with_sha_and_bytes(tmp_path):
    _write_json(tmp_path / "metrics.json", {"a": 1})
    (tmp_path / "blob.bin").write_bytes(b"\x00\x01\x02")
    manifest = build_outputs_manifest(tmp_path, status="succeeded")
    paths = [a["path"] for a in manifest["artifacts"]]
    assert paths == ["blob.bin", "metrics.json"]  # sorted
    assert manifest["status"] == "succeeded"
    for a in manifest["artifacts"]:
        assert len(a["sha256"]) == 64
        assert a["bytes"] > 0


def test_canonical_manifest_ignores_volatile_fields(tmp_path):
    run1 = tmp_path / "run1"
    run2 = tmp_path / "run2"
    _write_json(
        run1 / "job_result.json",
        _job_result(execution_id="exec-1", job_id="job-1", prefix="/out/a", fingerprint="FP"),
    )
    _write_json(
        run2 / "job_result.json",
        _job_result(execution_id="exec-2", job_id="job-2", prefix="/out/b", fingerprint="FP"),
    )

    raw1 = build_outputs_manifest(run1, canonical=False)
    raw2 = build_outputs_manifest(run2, canonical=False)
    assert raw1["artifacts"][0]["sha256"] != raw2["artifacts"][0]["sha256"]

    can1 = build_outputs_manifest(run1, canonical=True)
    can2 = build_outputs_manifest(run2, canonical=True)
    assert can1["artifacts"][0]["sha256"] == can2["artifacts"][0]["sha256"]


def test_canonical_manifest_detects_semantic_difference(tmp_path):
    run1 = tmp_path / "run1"
    run2 = tmp_path / "run2"
    _write_json(
        run1 / "job_result.json",
        _job_result(execution_id="exec-1", job_id="job-1", prefix="/out/a", fingerprint="FP-1"),
    )
    _write_json(
        run2 / "job_result.json",
        _job_result(execution_id="exec-1", job_id="job-1", prefix="/out/a", fingerprint="FP-2"),
    )
    can1 = build_outputs_manifest(run1, canonical=True)
    can2 = build_outputs_manifest(run2, canonical=True)
    assert can1["artifacts"][0]["sha256"] != can2["artifacts"][0]["sha256"]


def test_paths_are_relative_and_posix(tmp_path):
    _write_json(tmp_path / "nested" / "deep" / "result.json", {"x": 1})
    manifest = build_outputs_manifest(tmp_path)
    assert manifest["artifacts"][0]["path"] == "nested/deep/result.json"


def test_volatile_fields_cover_known_nondeterministic_keys():
    for key in ("execution_id", "job_id", "created_at", "started_at", "finished_at", "environment", "artifact_prefix"):
        assert key in VOLATILE_FIELDS


def test_canonical_manifest_ignores_operational_jobs_field(tmp_path):
    # jobs=1 vs jobs=4 is the operational knob the determinism test varies. It is
    # echoed in the envelope but is not part of the semantic result, so the
    # canonical digest must ignore it (else cross-jobs determinism never proves).
    run1 = tmp_path / "r1"
    run4 = tmp_path / "r4"
    base = _job_result(execution_id="e", job_id="j", prefix="/o", fingerprint="FP")
    _write_json(run1 / "engine_manifest.json", {**base, "jobs": 1})
    _write_json(run4 / "engine_manifest.json", {**base, "jobs": 4})
    can1 = build_outputs_manifest(run1, canonical=True)
    can4 = build_outputs_manifest(run4, canonical=True)
    assert can1["artifacts"][0]["sha256"] == can4["artifacts"][0]["sha256"]


def test_non_semantic_fields_is_union_of_volatile_and_operational():
    assert NON_SEMANTIC_FIELDS == VOLATILE_FIELDS | OPERATIONAL_FIELDS
    assert "jobs" in OPERATIONAL_FIELDS
    assert "jobs" not in VOLATILE_FIELDS
