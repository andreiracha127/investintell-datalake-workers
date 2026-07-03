"""Pins for artifacts/quant/open_macro_v03_reproducibility_001 (closed matrix evidence).

The consolidated reproducibility package commits, byte-for-byte, what the driver wrote on
the SUCCESSFUL cloud run (backtest efd8c9cc..., QC project 33679769): the completed
consolidated report and the reconstructed cloud verdict, plus a provenance record that
sha256-pins both files. These tests freeze that evidence: any post-commit edit to the
report/verdict bytes breaks the provenance pins, and no file in the package may carry an
activation or approval marker — the closed matrix is reproducibility evidence only.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPRO_ROOT = ROOT / "artifacts" / "quant" / "open_macro_v03_reproducibility_001"

REQUIRED_ARTIFACTS = {
    "consolidated_reproducibility_report.json",
    "phase0q_cloud_verdict.json",
    "provenance.json",
}

# The measured local leg hash (artifacts/quant/open_macro_v03_metric_evidence_001) that the
# cloud leg must equal — the single equality that closes the reproducibility matrix.
EXPECTED_LEG_HASH = "83e6733e9f849c68cd12082b47d72c67a2f2c7242341618dbbe2f0ea76870b60"
EXPECTED_RUN_FINGERPRINT = "6850a1e361ded96fcf84a74a0f78b5a8db962be79320f388e83c2778eafec671"
EXPECTED_FULL_VERDICT_SHA256 = "7ff8b16c58213f53a31ade980a83a9e16eb4b25208d514866df580d379f43f2b"
EXPECTED_BACKTEST_ID = "efd8c9cc19855e2d75344979c9f068d0"
EXPECTED_HARNESS_COMMIT = "68b07e810bc28665fedd85c6acd3ea5770b4b099"
EXPECTED_INPUT_PACK_SHA256 = "23a639781853bd53e37eb44359c30a613bc3c82a9dfc5a65c9b5b81f1d04d337"
EXPECTED_CONTRACT_BUNDLE_SHA256 = "db85c58968becd890d49d0a022b54b9493449e8c9ff444c88da10678c5d6f53b"


def _json(name: str) -> dict[str, Any]:
    payload = json.loads((REPRO_ROOT / name).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_required_reproducibility_artifacts_exist() -> None:
    missing = [name for name in sorted(REQUIRED_ARTIFACTS) if not (REPRO_ROOT / name).is_file()]
    assert missing == []


def test_consolidated_report_closes_the_matrix_with_zero_mismatches() -> None:
    report = _json("consolidated_reproducibility_report.json")

    assert report["artifact_type"] == "phase0q_cloud_consolidated_reproducibility_report"
    assert report["verdict"] == "reproduced"
    assert report["reproduced"] is True
    assert report["comparison"]["all_hashes_match"] is True
    assert report["comparison"]["mismatch_count"] == 0

    matrix = report["reproducibility_matrix"]
    assert matrix["legs"] == ["local_python_pure", "qc_research_object_store"]
    assert matrix["local_python_pure"]["logical_hash"] == EXPECTED_LEG_HASH
    assert matrix["qc_research_object_store"]["logical_hash"] == EXPECTED_LEG_HASH
    assert matrix["qc_research_object_store"]["status"] == "reproduced"

    assert report["run_fingerprint"] == EXPECTED_RUN_FINGERPRINT
    assert report["harness_commit"] == EXPECTED_HARNESS_COMMIT
    assert report["input_pack_sha256"] == EXPECTED_INPUT_PACK_SHA256
    assert report["contract_bundle_sha256"] == EXPECTED_CONTRACT_BUNDLE_SHA256
    assert report["qc_project_id"] == 33679769

    # every per-metric hash must individually match between legs
    for metric, entry in report["comparison"]["output_logical_hashes"].items():
        assert entry["match"] is True, metric
        assert entry["actual"] == entry["expected"], metric


def test_consolidated_report_keeps_activation_blocked() -> None:
    gov = _json("consolidated_reproducibility_report.json")["governance"]

    assert gov["A5"] == "blocked"
    assert gov["status"] == "candidate_not_approved"
    assert gov["classification"] == "metric_evidence_only"
    assert gov["approved"] is False
    assert gov["runtime_activation"] is False
    assert gov["activation_allowed"] is False
    assert gov["allocator_publish"] is False
    assert gov["official_result"] is False
    assert gov["freeze_ready"] is False
    assert gov["db_write_mode"] == "none"
    assert gov["production_endpoint_activation"] == "none"


def test_cloud_verdict_is_reproduced_and_honest_about_unarchived_full_verdict() -> None:
    verdict = _json("phase0q_cloud_verdict.json")

    assert verdict["verdict"] == "reproduced"
    assert verdict["reproduced"] is True
    assert verdict["execution_backend"] == "quantconnect_cloud_backtest"
    assert verdict["execution_legs"]["qc_research_object_store"]["logical_hash"] == EXPECTED_LEG_HASH
    assert verdict["run_fingerprint"] == EXPECTED_RUN_FINGERPRINT

    stats = verdict["runtime_statistics"]
    assert stats["phase0q_verdict"] == "reproduced"
    assert stats["phase0q_mismatch_count"] == "0"
    assert stats["phase0q_cloud_leg_hash"] == stats["phase0q_expected_leg_hash"] == EXPECTED_LEG_HASH

    # quota reality, reported honestly: the full verdict was NOT archived, so no store key
    # may be advertised; the full verdict is pinned by sha256 via the chunked-log fallback.
    assert stats["phase0q_fullverdict_saved"] == "false"
    assert verdict["full_verdict_object_store_key"] is None
    assert verdict["full_verdict_sha256"] == EXPECTED_FULL_VERDICT_SHA256


def test_provenance_pins_the_successful_run_and_the_exact_file_bytes() -> None:
    provenance = _json("provenance.json")

    run = provenance["successful_run"]
    assert run["backtest_id"] == EXPECTED_BACKTEST_ID
    assert run["verdict"] == "reproduced"
    assert run["mismatch_count"] == 0
    assert run["full_verdict_archived"] is False
    assert run["full_verdict_sha256"] == EXPECTED_FULL_VERDICT_SHA256
    assert provenance["qc_project_id"] == 33679769

    pins = provenance["pins"]
    assert pins["harness_commit"] == EXPECTED_HARNESS_COMMIT
    assert pins["input_pack_sha256"] == EXPECTED_INPUT_PACK_SHA256
    assert pins["contract_bundle_sha256"] == EXPECTED_CONTRACT_BUNDLE_SHA256

    # immutability: the committed report/verdict bytes must equal what the driver wrote on
    # the successful run — any post-commit edit breaks these pins.
    for name, expected_sha in provenance["file_sha256"].items():
        actual = hashlib.sha256((REPRO_ROOT / name).read_bytes()).hexdigest()
        assert actual == expected_sha, f"{name} bytes diverge from provenance pin"


def test_reproducibility_artifacts_contain_no_activation_or_approval_markers() -> None:
    forbidden = (
        "runtime_activation=true",
        "activation_allowed=true",
        "freeze_ready=true",
        "official_result=true",
        '"runtime_activation": true',
        '"activation_allowed": true',
        '"freeze_ready": true',
        '"official_result": true',
        '"approved": true',
        "A5=unblocked",
        '"status": "go"',
    )
    for path in sorted(REPRO_ROOT.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for marker in forbidden:
            assert marker not in text, f"{path.name} contains {marker}"
