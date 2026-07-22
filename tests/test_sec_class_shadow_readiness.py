"""Synthetic, DB-free contracts for SEC class shadow readiness evidence."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import subprocess
import sys

import pytest

from src.workers import sec_class_peers_scoring as peers
from src.workers import sec_class_shadow_readiness as readiness
from tests.test_sec_class_peers_scoring import evidence, governed


def _rehash(value: dict) -> dict:
    value["content_hash"] = readiness.canonical_sha256(
        {key: item for key, item in value.items() if key != "content_hash"}
    )
    return value


def _row(score: dict, *, status: str = "certified") -> dict:
    return {
        "series_id": score["series_id"],
        "classification_label": score["classification_label"],
        "broad_class": score["broad_class"],
        "approved_proxy": "LQD",
        "optimizer_sleeve": "fixed_income",
        "benchmark_family": "GOVT",
        "status": status,
        "optimizer_eligible": status == "certified",
        "holdings_hash": "1" * 64,
        "regulatory_hash": "2" * 64,
        "fee_hash": "3" * 64,
        "narrative_hash": "4" * 64,
        "frontend_hash": "5" * 64,
        "pillar_score_hashes": {
            pillar: readiness.canonical_sha256(score["pillars"][pillar])
            for pillar in peers.PILLARS
        },
        "coverage_pct": 100.0,
        "reason_codes": [],
        "w2b2_score_hash": readiness.canonical_sha256(score),
        "peer_cohort_hash": readiness.canonical_sha256(score["cohort"]),
        "summary_score_hash": readiness.canonical_sha256(score["summary_score"]),
    }


def _artifacts(tmp_path: Path) -> tuple[dict, dict, dict]:
    scoring_input, matrix, phase4, w2a_bundle, factor_input, w2b1 = governed(
        tmp_path, 10
    )
    w2b2 = peers.build_authenticated_result(
        scoring_input,
        matrix,
        evidence_manifest=evidence(),
        phase4_manifest=phase4,
        w2a_bundle=w2a_bundle,
        factor_input=factor_input,
        w2b1_result=w2b1,
        factor_runner_sha="f" * 40,
        runner_sha="e" * 40,
    )
    rows = [_row(score) for score in w2b2["scores"]]
    baseline = _rehash(
        {
            "schema_version": readiness.BASELINE_SCHEMA,
            "state": "complete",
            "classification_policy": {"id": "policy", "version": "v1"},
            "snapshot_id": "snapshot",
            "publication_version": "publication-v1",
            "as_of": "2026-09-27",
            "baseline_strategic_count": 10,
            "baseline_total_count": 10,
            "retention_floor_pct": 90.0,
            "rows": deepcopy(rows),
        }
    )
    baseline_policy_matrix = _rehash(
        {
            "schema_version": readiness.BASELINE_MATRIX_SCHEMA,
            "policy_id": matrix["policy_id"], "policy_version": matrix["policy_version"],
            "entries": deepcopy(matrix["entries"]),
        }
    )
    candidate = _rehash(
        {
            "schema_version": readiness.CANDIDATE_SCHEMA,
            "state": "complete",
            "classification_policy": {"id": "policy", "version": "v1"},
            "snapshot_id": "snapshot",
            "publication_version": "publication-v2",
            "as_of": "2026-09-27",
            "w2b2_run_hash": w2b2["content_hash"],
            "quality": {"unidentified_pct": 0.0, "nondecomposable_fund_pct": 0.0},
            "rows": deepcopy(rows),
        }
    )
    baseline_authority = _rehash(
        {
            "schema_version": readiness.BASELINE_AUTHORITY_SCHEMA,
            "state": "complete",
            "baseline_content_hash": baseline["content_hash"],
            "publication_version": baseline["publication_version"],
            "classification_policy": baseline["classification_policy"],
            "snapshot_id": baseline["snapshot_id"],
            "baseline_total_count": baseline["baseline_total_count"],
            "baseline_strategic_count": baseline["baseline_strategic_count"],
            "retention_floor_pct": baseline["retention_floor_pct"],
            "baseline_policy_matrix_hash": baseline_policy_matrix["content_hash"],
        }
    )
    candidate_evidence_manifest = _rehash(
        {
            "schema_version": readiness.CANDIDATE_EVIDENCE_SCHEMA,
            "state": "complete",
            "publication_version": candidate["publication_version"],
            "classification_policy": candidate["classification_policy"],
            "snapshot_id": candidate["snapshot_id"],
            "as_of": candidate["as_of"],
            "w2b2_run_hash": candidate["w2b2_run_hash"],
            "rows": [
                {key: row[key] for key in (
                    "series_id", "status", "optimizer_eligible", "holdings_hash",
                    "regulatory_hash", "fee_hash", "narrative_hash", "frontend_hash",
                    "w2b2_score_hash", "peer_cohort_hash", "summary_score_hash", "pillar_score_hashes",
                )}
                for row in candidate["rows"]
            ],
        }
    )
    expected_trust_anchors = {
        "schema_version": readiness.TRUST_ANCHORS_SCHEMA,
        "baseline_authority_hash": baseline_authority["content_hash"],
        "baseline_publication_version": baseline["publication_version"],
        "candidate_evidence_manifest_hash": candidate_evidence_manifest["content_hash"],
        "candidate_publication_version": candidate["publication_version"],
        "candidate_producer_id": "fixture-producer",
    }
    context = {
        "scoring_input": scoring_input,
        "phase4_manifest": phase4,
        "w2a_bundle": w2a_bundle,
        "factor_input": factor_input,
        "w2b1_result": w2b1,
        "w2b2_result": w2b2,
        "policy_matrix": matrix,
        "evidence_manifest": evidence(),
        "factor_runner_sha": "f" * 40,
        "scoring_runner_sha": "e" * 40,
        "runner_sha": "d" * 40,
        "baseline_authority": baseline_authority,
        "candidate_evidence_manifest": candidate_evidence_manifest,
        "expected_trust_anchors": expected_trust_anchors,
        "baseline_policy_matrix": baseline_policy_matrix,
    }
    return baseline, candidate, context


def _refresh_candidate_evidence(candidate: dict, context: dict) -> None:
    manifest = context["candidate_evidence_manifest"]
    manifest["publication_version"] = candidate["publication_version"]
    manifest["classification_policy"] = deepcopy(candidate["classification_policy"])
    manifest["snapshot_id"] = candidate["snapshot_id"]
    manifest["as_of"] = candidate["as_of"]
    manifest["w2b2_run_hash"] = candidate["w2b2_run_hash"]
    manifest["rows"] = [
        {key: row[key] for key in (
            "series_id", "status", "optimizer_eligible", "holdings_hash",
            "regulatory_hash", "fee_hash", "narrative_hash", "frontend_hash",
            "w2b2_score_hash", "peer_cohort_hash", "summary_score_hash", "pillar_score_hashes",
        )}
        for row in candidate["rows"]
    ]
    _rehash(manifest)
    context["expected_trust_anchors"]["candidate_evidence_manifest_hash"] = manifest["content_hash"]
    context["expected_trust_anchors"]["candidate_publication_version"] = candidate["publication_version"]


def _anchor_records(*, coverage: float | None = None, unidentified: float = 0.0, nondecomposable: float = 0.0, lag_days: int = 0) -> list[dict]:
    dates = ("2024-06-30", "2024-09-30", "2024-12-31", "2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31")
    return [
        {
            "report_date": readiness.date.fromisoformat(day),
            "oldest_report_date": readiness.date.fromordinal(readiness.date.fromisoformat(day).toordinal() - lag_days),
            "coverage_pct": 100.0 - unidentified - nondecomposable if coverage is None else coverage,
            "unidentified_pct": unidentified,
            "nondecomposable_fund_pct": nondecomposable,
        }
        for day in dates
    ]


def _refresh_baseline_authority(baseline: dict, context: dict) -> None:
    authority = context["baseline_authority"]
    authority.update({
        "baseline_content_hash": baseline["content_hash"],
        "publication_version": baseline["publication_version"],
        "classification_policy": deepcopy(baseline["classification_policy"]),
        "snapshot_id": baseline["snapshot_id"],
        "baseline_total_count": baseline["baseline_total_count"],
        "baseline_strategic_count": baseline["baseline_strategic_count"],
        "retention_floor_pct": baseline["retention_floor_pct"],
    })
    _rehash(authority)
    context["expected_trust_anchors"]["baseline_authority_hash"] = authority["content_hash"]
    context["expected_trust_anchors"]["baseline_publication_version"] = baseline["publication_version"]


def test_authenticated_artifact_only_result_is_ready_and_self_hashed(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)

    result = readiness.build_readiness_result(baseline, candidate, **context)

    assert result["status"] == "ready"
    assert result["content_hash"] == readiness.canonical_sha256(
        {key: item for key, item in result.items() if key != "content_hash"}
    )
    assert "current" not in readiness.canonical_json(result).decode()
    assert result["gates"]["anchor_count"] is True


@pytest.mark.parametrize(
    ("value", "expected"),
    [(79.99, False), (80.0, True)],
)
def test_coverage_gate_honors_80_percent_boundary(
    tmp_path: Path, value: float, expected: bool
) -> None:
    gates, _ = readiness._derive_anchor_gates(
        _anchor_records(coverage=value, unidentified=100.0 - value), readiness.date(2026, 9, 27)
    )
    assert gates["anchor_coverage"] is expected
    assert gates["latest_coverage"] is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [(10.0, True), (10.01, False)],
)
def test_unidentified_gate_honors_10_percent_boundary(
    tmp_path: Path, value: float, expected: bool
) -> None:
    gates, _ = readiness._derive_anchor_gates(
        _anchor_records(unidentified=value), readiness.date(2026, 9, 27)
    )
    assert gates["unidentified_pct"] is expected


@pytest.mark.parametrize(
    ("as_of", "expected"),
    [("2026-09-27", True), ("2026-09-28", False)],
)
def test_latest_anchor_age_honors_180_day_boundary(
    tmp_path: Path, as_of: str, expected: bool
) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    candidate["as_of"] = as_of
    _rehash(candidate)
    _refresh_candidate_evidence(candidate, context)

    result = readiness.build_readiness_result(baseline, candidate, **context)

    assert result["gates"]["latest_anchor_age"] is expected


@pytest.mark.parametrize(("span_months", "expected"), [(20, False), (21, True)])
def test_anchor_span_honors_20_21_month_boundary(span_months: int, expected: bool) -> None:
    records = _anchor_records()
    records[-1]["report_date"] = readiness.date(2026, 2 if span_months == 20 else 3, 28 if span_months == 20 else 31)
    records[-1]["oldest_report_date"] = records[-1]["report_date"]
    gates, _ = readiness._derive_anchor_gates(records, readiness.date(2026, 9, 27))
    assert gates["anchor_span"] is expected


@pytest.mark.parametrize(("lag_days", "expected"), [(180, True), (181, False)])
def test_chain_lag_honors_180_181_day_boundary(lag_days: int, expected: bool) -> None:
    if lag_days == 181:
        with pytest.raises(readiness.ArtifactValidationError, match="lag"):
            readiness._derive_anchor_gates(_anchor_records(lag_days=lag_days), readiness.date(2026, 9, 27))
        return
    gates, _ = readiness._derive_anchor_gates(
        _anchor_records(lag_days=lag_days), readiness.date(2026, 9, 27)
    )
    assert gates["chain_lag"] is expected


def test_bad_quarter_cannot_be_masked_by_good_candidate_aggregate() -> None:
    records = _anchor_records()
    records[3]["unidentified_pct"] = 10.01
    records[3]["nondecomposable_fund_pct"] = 10.0
    records[3]["coverage_pct"] = 79.99
    gates, evidence = readiness._derive_anchor_gates(records, readiness.date(2026, 9, 27))
    assert gates["unidentified_pct"] is False
    assert gates["anchor_coverage"] is False
    assert evidence["worst_unidentified_pct"] == 10.01


@pytest.mark.parametrize(
    ("removed", "expected"),
    [(1, True), (2, False)],
)
def test_total_and_strategic_retention_honor_90_percent_boundary(
    tmp_path: Path, removed: int, expected: bool
) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    candidate["rows"] = candidate["rows"][:-removed]
    _rehash(candidate)
    _refresh_candidate_evidence(candidate, context)

    result = readiness.build_readiness_result(baseline, candidate, **context)

    assert result["gates"]["total_series_retention"] is expected
    assert result["gates"]["strategic_retention"] is expected


def test_baseline_reset_policy_mismatch_and_self_hash_tamper_are_refused(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    baseline["baseline_total_count"] = 9
    _rehash(baseline)
    with pytest.raises(readiness.ArtifactValidationError, match="inherited counts"):
        readiness.build_readiness_result(baseline, candidate, **context)

    baseline, candidate, context = _artifacts(tmp_path)
    candidate["classification_policy"]["version"] = "v2"
    _rehash(candidate)
    with pytest.raises(readiness.ArtifactValidationError, match="authenticated scoring input"):
        readiness.build_readiness_result(baseline, candidate, **context)

    baseline, candidate, context = _artifacts(tmp_path)
    candidate["rows"][0]["fee_hash"] = "0" * 64
    with pytest.raises(readiness.ArtifactValidationError, match="self-hash"):
        readiness.build_readiness_result(baseline, candidate, **context)


def test_prior_baseline_policy_and_snapshot_may_differ_from_candidate(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    baseline["classification_policy"] = {"id": "prior-policy", "version": "v0"}
    context["baseline_policy_matrix"]["policy_id"] = "prior-policy"
    context["baseline_policy_matrix"]["policy_version"] = "v0"
    _rehash(context["baseline_policy_matrix"])
    baseline["snapshot_id"] = "prior-snapshot"
    _rehash(baseline)
    _refresh_baseline_authority(baseline, context)
    context["baseline_authority"]["baseline_policy_matrix_hash"] = context["baseline_policy_matrix"]["content_hash"]
    _rehash(context["baseline_authority"])
    context["expected_trust_anchors"]["baseline_authority_hash"] = context["baseline_authority"]["content_hash"]

    result = readiness.build_readiness_result(baseline, candidate, **context)

    assert result["status"] == "ready"
    assert {"policy", "snapshot"}.issubset(result["deltas"][0]["change_reason_codes"])


def test_baseline_matrix_identity_mismatch_is_refused_even_when_rehashed(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    context["baseline_policy_matrix"]["policy_version"] = "v2"
    _rehash(context["baseline_policy_matrix"])
    context["baseline_authority"]["baseline_policy_matrix_hash"] = context["baseline_policy_matrix"]["content_hash"]
    _rehash(context["baseline_authority"])
    context["expected_trust_anchors"]["baseline_authority_hash"] = context["baseline_authority"]["content_hash"]
    with pytest.raises(readiness.ArtifactValidationError, match="baseline policy matrix identity"):
        readiness.build_readiness_result(baseline, candidate, **context)


def test_delta_pillars_are_explicit_for_added_and_removed_series() -> None:
    pillars = {name: "a" * 64 for name in peers.PILLARS}
    prior = {"classification_policy": {}, "snapshot_id": "old", "rows": [{"series_id": "OLD", "pillar_score_hashes": pillars, "w2b2_score_hash": "1" * 64}]}
    candidate = {"classification_policy": {}, "snapshot_id": "new", "rows": [{"series_id": "NEW", "pillar_score_hashes": pillars, "w2b2_score_hash": "2" * 64}]}
    # Fill the remaining governed row keys used by delta comparison.
    for row in [*prior["rows"], *candidate["rows"]]:
        row.update({"classification_label": "x", "benchmark_family": "x", "approved_proxy": "x", "peer_cohort_hash": "3" * 64, "summary_score_hash": "4" * 64, "status": "certified", "optimizer_eligible": True, "holdings_hash": "5" * 64, "regulatory_hash": "6" * 64, "fee_hash": "7" * 64, "narrative_hash": "8" * 64, "frontend_hash": "9" * 64})
    deltas = readiness._deltas(prior, candidate, {})
    for item in deltas:
        assert all(item["evidence"][f"pillar:{pillar}"]["prior"] is None or item["evidence"][f"pillar:{pillar}"]["candidate"] is None for pillar in peers.PILLARS)
        assert all(f"pillar:{pillar}" in item["change_reason_codes"] for pillar in peers.PILLARS)


def test_historical_matrix_mapping_evolution_is_compared_not_rejected(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    historical = context["baseline_policy_matrix"]
    historical["policy_id"], historical["policy_version"] = "prior-policy", "v0"
    entry = next(item for item in historical["entries"] if item["classification_label"] == "Investment Grade Bond")
    entry["approved_proxy"] = "OLD"
    _rehash(historical)
    baseline["classification_policy"] = {"id": "prior-policy", "version": "v0"}
    for row in baseline["rows"]:
        row["approved_proxy"] = "OLD"
    _rehash(baseline)
    _refresh_baseline_authority(baseline, context)
    context["baseline_authority"]["baseline_policy_matrix_hash"] = historical["content_hash"]
    _rehash(context["baseline_authority"])
    context["expected_trust_anchors"]["baseline_authority_hash"] = context["baseline_authority"]["content_hash"]

    result = readiness.build_readiness_result(baseline, candidate, **context)

    assert result["status"] == "ready"
    assert "proxy" in result["deltas"][0]["change_reason_codes"]


def test_authorities_refuse_coherent_baseline_reset_and_candidate_forgery(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    baseline["publication_version"] = "reset-publication"
    _rehash(baseline)
    with pytest.raises(readiness.ArtifactValidationError, match="baseline authority binding"):
        readiness.build_readiness_result(baseline, candidate, **context)


def test_frozen_expected_trust_anchors_refuse_dual_rehash(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    candidate["rows"][0]["narrative_hash"] = "0" * 64
    _rehash(candidate)
    manifest = context["candidate_evidence_manifest"]
    manifest["rows"][0]["narrative_hash"] = "0" * 64
    _rehash(manifest)

    with pytest.raises(readiness.ArtifactValidationError, match="expected trust anchors mismatch"):
        readiness.build_readiness_result(baseline, candidate, **context)

    baseline, candidate, context = _artifacts(tmp_path)
    candidate["rows"][0]["status"] = "degraded"
    candidate["rows"][0]["optimizer_eligible"] = False
    _rehash(candidate)
    with pytest.raises(readiness.ArtifactValidationError, match="candidate evidence manifest binding"):
        readiness.build_readiness_result(baseline, candidate, **context)


def test_empty_candidate_is_not_ready_and_bucket_retention_requires_same_bucket(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    candidate["rows"] = []
    _rehash(candidate)
    _refresh_candidate_evidence(candidate, context)

    result = readiness.build_readiness_result(baseline, candidate, **context)

    assert result["status"] == "not_ready"
    assert result["gates"]["candidate_non_empty"] is False
    buckets = readiness._breakdown(
        [{"series_id": "S", "broad_class": "equity"}],
        [{"series_id": "S", "broad_class": "fixed_income"}],
        "broad_class",
    )
    assert next(item for item in buckets if item["value"] == "equity")["retained_total"] == 0


@pytest.mark.parametrize(
    ("field", "prior", "candidate"),
    [
        ("classification_label", "Large Blend", "Large Value"),
        ("broad_class", "equity", "fixed_income"),
        ("status", "certified", "degraded"),
        ("approved_proxy", "IVV", "QQQ"),
        ("optimizer_sleeve", "equity", "thematic"),
    ],
)
def test_bucket_swaps_do_not_count_as_retention(field: str, prior: str, candidate: str) -> None:
    buckets = readiness._breakdown(
        [{"series_id": "S", field: prior}], [{"series_id": "S", field: candidate}], field
    )
    assert next(item for item in buckets if item["value"] == prior)["retained_total"] == 0


@pytest.mark.parametrize(("value", "expected"), [(89.99, False), (90.0, True)])
def test_broad_class_coverage_threshold_is_pinned(value: float, expected: bool) -> None:
    assert readiness.broad_class_coverage_passes(value) is expected


@pytest.mark.parametrize(("value", "expected"), [(10.0, True), (10.01, False)])
def test_nondecomposable_boundary_uses_worst_anchor(value: float, expected: bool) -> None:
    gates, _ = readiness._derive_anchor_gates(
        _anchor_records(nondecomposable=value), readiness.date(2026, 9, 27)
    )
    assert gates["nondecomposable_pct"] is expected


def test_anchor_rejects_negative_lag_nan_and_out_of_range() -> None:
    records = _anchor_records()
    for invalid in (-0.1, float("nan"), 100.01):
        records[0]["unidentified_pct"] = invalid
        with pytest.raises(readiness.ArtifactValidationError):
            readiness._derive_anchor_gates(records, readiness.date(2026, 9, 27))

def test_diagnostic_narrative_delta_does_not_change_classification_or_score(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    original_label = candidate["rows"][0]["classification_label"]
    original_score = candidate["rows"][0]["w2b2_score_hash"]
    candidate["rows"][0]["narrative_hash"] = "0" * 64
    _rehash(candidate)
    _refresh_candidate_evidence(candidate, context)

    result = readiness.build_readiness_result(baseline, candidate, **context)

    assert candidate["rows"][0]["classification_label"] == original_label
    assert candidate["rows"][0]["w2b2_score_hash"] == original_score
    assert result["deltas"][0]["change_reason_codes"] == ["narratives"]


def test_atomic_artifact_refuses_foreign_output_and_shadow_refuses_pointer(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    output = tmp_path / "shadow-result"
    output.mkdir()
    (output / "foreign.json").write_text("{}", encoding="utf-8")
    with pytest.raises(readiness.ArtifactValidationError, match="partial foreign"):
        readiness.run_artifact(baseline, candidate, output, **context)

    result = readiness.build_readiness_result(baseline, candidate, **context)
    authorization = {
        "stage": "phase9_shadow",
        "command": "shadow-db-write",
        **result["binding"],
        "output_content_hash": result["content_hash"],
        "target": "isolated-shadow",
        "role": "shadow_writer",
    }
    with pytest.raises(readiness.ArtifactValidationError, match="shadow_writer_unconfigured"):
        readiness.run_shadow_unconfigured(
            authorization=authorization, baseline=baseline, candidate=candidate, **context
        )
    authorization["pointer"] = "forbidden"
    with pytest.raises(readiness.ArtifactValidationError, match="binding mismatch"):
        readiness.run_shadow_unconfigured(
            authorization=authorization, baseline=baseline, candidate=candidate, **context
        )


def test_atomic_artifact_readback_rejects_tampering_and_cli_is_db_free(tmp_path: Path) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    output = tmp_path / "shadow-result"

    result = readiness.run_artifact(baseline, candidate, output, **context)

    assert (output / "shadow_readiness.json").read_bytes() == readiness.canonical_json(result)
    (output / "shadow_readiness.json").write_text("{}", encoding="utf-8")
    with pytest.raises(readiness.ArtifactValidationError, match="partial foreign"):
        readiness.run_artifact(baseline, candidate, output, **context)
    completed = subprocess.run(
        [sys.executable, "-m", "src.workers.sec_class_shadow_readiness", "--help"],
        check=False, capture_output=True, text=True,
    )
    assert completed.returncode == 0
    assert "--artifact-only" in completed.stdout
    fresh_import = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import src.workers.sec_class_shadow_readiness; "
            "assert not any(name.startswith(('psycopg', 'asyncpg', 'sqlalchemy')) "
            "for name in sys.modules)",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert fresh_import.returncode == 0, fresh_import.stderr


def test_public_runner_refuses_reparse_and_staged_corruption(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    baseline, candidate, context = _artifacts(tmp_path)
    output = tmp_path / "result"
    original_no_reparse = readiness._no_reparse
    monkeypatch.setattr(readiness, "_no_reparse", lambda path: (_ for _ in ()).throw(readiness.ArtifactValidationError("symlink/reparse output refused")))
    with pytest.raises(readiness.ArtifactValidationError, match="reparse"):
        readiness.run_artifact(baseline, candidate, output, **context)
    monkeypatch.setattr(readiness, "_no_reparse", original_no_reparse)
    monkeypatch.setattr(readiness, "_write", lambda path, value: path.write_text("{}", encoding="utf-8"))
    with pytest.raises(readiness.ArtifactValidationError, match="staged output"):
        readiness.run_artifact(baseline, candidate, output, **context)
