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

import pytest

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

# The historically committed local leg (metric_evidence_001) predates the ratified
# fold-seeding fixes; the live-head harness (68b07e81) recomputes the leg as
# EXPECTED_LEG_HASH. The provenance record must state that relation explicitly.
EVIDENCE_001_LEG_HASH = "ae7094c76c36d02c2f269e1cdc7868c78a7c8835939f79a1bb0d4c5c925bc993"

# Independent byte pins over every file in the package, provenance itself included —
# constants here (not read from the package) so a coordinated edit of report/verdict
# AND provenance.json cannot silently refresh the evidence.
EXPECTED_FILE_SHA256 = {
    "consolidated_reproducibility_report.json":
        "7e4a49ec39bab36a62bfe1065b0a55599ded04724b221db19d653d29e633b871",
    "phase0q_cloud_verdict.json":
        "c98f2e870c304d24f5666b7bc75877c453ed5ce635df34b2390e779ac530310a",
    "provenance.json":
        "bc698ed10d4362335534f5dd36a4d6da6c4891cd1a44ea5e4493e8a3acaf1f35",
}

# Two DISTINCT manifests, both pinned: the expected-results manifest is the comparison
# target (pinned identically in the cloud_leg_manifest object table AS OF the run);
# the object-store manifest is the per-object root of trust baked into the uploaded
# main.py.
EXPECTED_RESULTS_MANIFEST_SHA256 = (
    "018d4d6f2774c2ec3b9c328aa05074f4badf00b6db0dfeb2278e26a4ccd642fd")
OBJECT_STORE_MANIFEST_SHA256 = (
    "74a27d7596255c5d0e18c0abe82d839894a0238b3eac1a69fdafde41ad43156c")

# The evidence ratification commit: the last commit of this byte-frozen package. The
# cross-artifact check below reads the cloud_leg_manifest git blob AT THIS COMMIT (the
# manifest as uploaded for backtest efd8c9cc...), never the mutable working tree.
EVIDENCE_RATIFICATION_COMMIT = "7bb85088bf2fc541f5e11a5bebe88c1802332adf"


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
    assert pins["expected_results_manifest_sha256"] == EXPECTED_RESULTS_MANIFEST_SHA256
    assert pins["object_store_manifest_sha256"] == OBJECT_STORE_MANIFEST_SHA256

    # cross-artifact consistency: the expected-results pin must equal the cloud-leg
    # bundle's object-table pin AS OF THE SUCCESSFUL RUN — the trail an auditor
    # follows. Read the cloud_leg_manifest git BLOB at the evidence ratification
    # commit, never the mutable working tree (the same principle as the stage-B
    # evidence freeze): the working-tree manifest legitimately tracks HEAD and is
    # re-prepared whenever the shipped closure evolves (e.g. the phase0q_005
    # ratification changed runner.py/metrics.py and shipped the policy artifact),
    # while this historical record can neither drift nor be forced to chase
    # re-measurements.
    import subprocess
    blob = subprocess.check_output(
        ["git", "cat-file", "blob",
         f"{EVIDENCE_RATIFICATION_COMMIT}:artifacts/quant/"
         "open_macro_v03_cloud_leg_001/cloud_leg_manifest.json"],
        cwd=ROOT)
    cloud_leg = json.loads(blob.decode("utf-8"))
    bundle_pin = cloud_leg["object_key_sha_table"]["expected_results_manifest.json"]["content_sha256"]
    assert bundle_pin == EXPECTED_RESULTS_MANIFEST_SHA256

    # immutability: every file in the package (provenance included) must match the
    # CONSTANT pins above — a coordinated edit of the evidence files plus
    # provenance.json cannot silently refresh the package.
    # recursive listing: a file smuggled into any subdirectory must break the pin set.
    committed = sorted(
        str(p.relative_to(REPRO_ROOT)).replace("\\", "/")
        for p in REPRO_ROOT.rglob("*") if p.is_file())
    assert committed == sorted(EXPECTED_FILE_SHA256)
    for name, expected_sha in EXPECTED_FILE_SHA256.items():
        actual = hashlib.sha256((REPRO_ROOT / name).read_bytes()).hexdigest()
        assert actual == expected_sha, f"{name} bytes diverge from the constant pin"
    # internal consistency: the provenance's own table must agree with the constants
    # for the two driver-written files it pins.
    for name, sha in provenance["file_sha256"].items():
        assert EXPECTED_FILE_SHA256[name] == sha, f"provenance pin for {name} diverges"


def test_provenance_states_the_local_leg_derivation_honestly() -> None:
    derivation = _json("provenance.json")["local_leg_derivation"]

    assert derivation["local_leg_hash"] == EXPECTED_LEG_HASH
    assert derivation["expected_manifest_sha256"] == EXPECTED_RESULTS_MANIFEST_SHA256
    assert EXPECTED_HARNESS_COMMIT in derivation["computed_from"]
    # the record must name the historically committed leg it does NOT reproduce, so the
    # closed matrix can never be read as reproducing evidence_001's committed bytes.
    assert EVIDENCE_001_LEG_HASH in derivation["relation_to_metric_evidence_001"]
    assert "source" in derivation["driver_report_source_field_note"]


FORBIDDEN_TRUE_KEYS = ("runtime_activation", "activation_allowed", "allocator_publish",
                       "official_result", "freeze_ready", "approved")


def _walk(node):
    if isinstance(node, dict):
        for key, value in node.items():
            yield key, value
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _assert_governance(payload) -> None:
    for key, value in _walk(payload):
        if key in FORBIDDEN_TRUE_KEYS:
            assert value is not True, f"{key} must never be true"
        if key == "A5":
            assert str(value).strip().lower() == "blocked"
        if key == "db_write_mode":
            assert str(value).strip().lower() == "none"


def test_governance_walker_catches_minified_markers() -> None:
    # formatting-independent by construction: a minified true flag must be caught.
    with pytest.raises(AssertionError):
        _assert_governance(json.loads('{"nested":[{"approved":true}]}'))
    with pytest.raises(AssertionError):
        _assert_governance(json.loads('{"A5":"unblocked"}'))


def test_reproducibility_artifacts_contain_no_activation_or_approval_markers() -> None:
    for path in sorted(REPRO_ROOT.rglob("*.json")):
        _assert_governance(json.loads(path.read_text(encoding="utf-8")))
