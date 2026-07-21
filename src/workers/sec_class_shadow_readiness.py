"""DB-free, artifact-only SEC class shadow reclassification readiness evidence.

This module deliberately has no database, environment, network, or deployment
surface.  It authenticates the upstream W2B evidence from supplied immutable
artifacts and can only emit a canonical readiness report.
"""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import stat
import tempfile
from typing import Any, Mapping

from src.workers import sec_class_factors as factors
from src.workers import sec_class_peers_scoring as peers
from src.workers import nport_v2_lookthrough as w2a


ArtifactValidationError = factors.ArtifactValidationError
BASELINE_SCHEMA = "sec_class_shadow_baseline/v1"
CANDIDATE_SCHEMA = "sec_class_shadow_candidate/v1"
RUN_SCHEMA = "sec_class_shadow_readiness/v1"
BASELINE_AUTHORITY_SCHEMA = "sec_class_shadow_baseline_authority/v1"
BASELINE_MATRIX_SCHEMA = "sec_class_shadow_baseline_policy_matrix/v1"
CANDIDATE_EVIDENCE_SCHEMA = "sec_class_shadow_candidate_evidence/v1"
TRUST_ANCHORS_SCHEMA = "sec_class_shadow_expected_anchors/v1"
_PILLARS = peers.PILLARS
_BROAD = {entry[1] for entry in peers.APPROVED_POLICY_MATRIX}
_STATUSES = {"certified", "degraded", "insufficient", "unavailable"}
_HASH_FIELDS = (
    "holdings_hash",
    "regulatory_hash",
    "fee_hash",
    "narrative_hash",
    "frontend_hash",
    "w2b2_score_hash",
    "peer_cohort_hash",
    "summary_score_hash",
)
_ROW_FIELDS = {
    "series_id",
    "classification_label",
    "broad_class",
    "approved_proxy",
    "optimizer_sleeve",
    "benchmark_family",
    "status",
    "optimizer_eligible",
    *_HASH_FIELDS,
    "coverage_pct",
    "reason_codes",
    "pillar_score_hashes",
}


def canonical_json(value: Any) -> bytes:
    """Return the deterministic JSON encoding used for every identity."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _sha(value: Any, where: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value.lower())
    ):
        raise ArtifactValidationError(f"{where} must be SHA-256")
    return value


def _git(value: Any, where: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(char not in "0123456789abcdef" for char in value.lower())
    ):
        raise ArtifactValidationError(f"{where} must be Git SHA")
    return value


def _day(value: Any, where: str) -> date:
    if not isinstance(value, str):
        raise ArtifactValidationError(f"{where} must be ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ArtifactValidationError(f"{where} must be ISO date") from error


def _finite_pct(value: Any, where: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
        or value > 100
    ):
        raise ArtifactValidationError(f"{where} must be a percentage")
    return float(value)


def _hash_ok(value: Mapping[str, Any], where: str) -> None:
    body = dict(value)
    claimed = body.pop("content_hash", None)
    if claimed != canonical_sha256(body):
        raise ArtifactValidationError(f"{where} self-hash mismatch")


def _policy(value: Any, where: str) -> dict[str, str]:
    if (
        not isinstance(value, dict)
        or set(value) != {"id", "version"}
        or not isinstance(value.get("id"), str)
        or not value["id"]
        or not isinstance(value.get("version"), str)
        or not value["version"]
    ):
        raise ArtifactValidationError(f"{where} policy schema mismatch")
    return {"id": value["id"], "version": value["version"]}


def _row(value: Any, matrix: Mapping[str, tuple[str, str, str, str]], where: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != _ROW_FIELDS:
        raise ArtifactValidationError(f"{where} row schema mismatch")
    if not isinstance(value["series_id"], str) or not value["series_id"]:
        raise ArtifactValidationError(f"{where} series_id required")
    label = value["classification_label"]
    if not isinstance(label, str) or label not in matrix:
        raise ArtifactValidationError(f"{where} classification label is not governed")
    broad, proxy, sleeve, benchmark = matrix[label]
    if (
        value["broad_class"] != broad
        or value["approved_proxy"] != proxy
        or value["optimizer_sleeve"] != sleeve
        or value["benchmark_family"] != benchmark
        or broad not in _BROAD
    ):
        raise ArtifactValidationError(f"{where} classification matrix closure mismatch")
    if value["status"] not in _STATUSES or not isinstance(value["optimizer_eligible"], bool):
        raise ArtifactValidationError(f"{where} status/eligibility invalid")
    if value["optimizer_eligible"] and value["status"] != "certified":
        raise ArtifactValidationError(f"{where} optimizer eligibility requires certified status")
    for field in _HASH_FIELDS:
        _sha(value[field], f"{where} {field}")
    if (
        not isinstance(value["pillar_score_hashes"], dict)
        or set(value["pillar_score_hashes"]) != set(_PILLARS)
    ):
        raise ArtifactValidationError(f"{where} pillar score hash schema mismatch")
    for pillar, item in value["pillar_score_hashes"].items():
        _sha(item, f"{where} {pillar} pillar score hash")
    _finite_pct(value["coverage_pct"], f"{where} coverage")
    if (
        not isinstance(value["reason_codes"], list)
        or any(not isinstance(item, str) or not item for item in value["reason_codes"])
        or value["reason_codes"] != sorted(set(value["reason_codes"]))
    ):
        raise ArtifactValidationError(f"{where} reason codes must be sorted and unique")
    return value


def _rows(value: Any, matrix: Mapping[str, tuple[str, str, str, str]], where: str, *, allow_empty: bool = False) -> list[dict[str, Any]]:
    if not isinstance(value, list) or (not value and not allow_empty):
        raise ArtifactValidationError(f"{where} rows required")
    rows = [_row(item, matrix, where) for item in value]
    ids = [item["series_id"] for item in rows]
    if ids != sorted(ids) or len(set(ids)) != len(ids):
        raise ArtifactValidationError(f"{where} series_id must be strictly ordered and unique")
    return rows


def _matrix(policy_matrix: dict[str, Any]) -> dict[str, tuple[str, str, str, str]]:
    result = peers.validate_policy_matrix(policy_matrix)
    if len(result) != 45:
        raise ArtifactValidationError("45-label matrix required")
    return result


def _baseline_matrix(value: dict[str, Any]) -> dict[str, tuple[str, str, str, str]]:
    required = {"schema_version", "policy_id", "policy_version", "entries", "content_hash"}
    if not isinstance(value, dict) or set(value) != required or value.get("schema_version") != BASELINE_MATRIX_SCHEMA:
        raise ArtifactValidationError("baseline policy matrix schema mismatch")
    _hash_ok(value, "baseline policy matrix")
    if not isinstance(value["policy_id"], str) or not value["policy_id"] or not isinstance(value["policy_version"], str) or not value["policy_version"] or not isinstance(value["entries"], list):
        raise ArtifactValidationError("baseline policy matrix identity mismatch")
    fields = {"classification_label", "broad_class", "approved_proxy", "optimizer_sleeve", "benchmark_family"}
    if any(not isinstance(item, dict) or set(item) != fields for item in value["entries"]):
        raise ArtifactValidationError("baseline policy matrix entry schema mismatch")
    result = {item["classification_label"]: (item["broad_class"], item["approved_proxy"], item["optimizer_sleeve"], item["benchmark_family"]) for item in value["entries"]}
    if len(result) != len(value["entries"]):
        raise ArtifactValidationError("baseline policy matrix duplicate labels")
    return result


def validate_baseline(
    baseline: dict[str, Any], baseline_policy_matrix: dict[str, Any]
) -> dict[str, Any]:
    """Validate the inherited, self-hashed immutable complete baseline."""
    matrix = _baseline_matrix(baseline_policy_matrix)
    required = {
        "schema_version", "state", "classification_policy", "snapshot_id",
        "publication_version", "as_of", "baseline_strategic_count",
        "baseline_total_count", "retention_floor_pct", "rows", "content_hash",
    }
    if (
        not isinstance(baseline, dict)
        or set(baseline) != required
        or baseline.get("schema_version") != BASELINE_SCHEMA
        or baseline.get("state") != "complete"
    ):
        raise ArtifactValidationError("baseline schema mismatch")
    _hash_ok(baseline, "baseline")
    _policy(baseline["classification_policy"], "baseline")
    if baseline["classification_policy"] != {"id": baseline_policy_matrix["policy_id"], "version": baseline_policy_matrix["policy_version"]}:
        raise ArtifactValidationError("baseline policy matrix identity mismatch")
    if not isinstance(baseline["snapshot_id"], str) or not baseline["snapshot_id"]:
        raise ArtifactValidationError("baseline snapshot identity required")
    if not isinstance(baseline["publication_version"], str) or not baseline["publication_version"]:
        raise ArtifactValidationError("baseline publication identity required")
    _day(baseline["as_of"], "baseline as_of")
    if type(baseline["baseline_total_count"]) is not int or baseline["baseline_total_count"] < 1:
        raise ArtifactValidationError("baseline total count invalid")
    if type(baseline["baseline_strategic_count"]) is not int or baseline["baseline_strategic_count"] < 0:
        raise ArtifactValidationError("baseline strategic count invalid")
    if _finite_pct(baseline["retention_floor_pct"], "baseline retention floor") != 90.0:
        raise ArtifactValidationError("baseline retention floor must remain 90")
    rows = _rows(baseline["rows"], matrix, "baseline")
    strategic = sum(row["optimizer_eligible"] for row in rows)
    if baseline["baseline_total_count"] != len(rows) or baseline["baseline_strategic_count"] != strategic:
        raise ArtifactValidationError("baseline inherited counts do not match rows")
    return baseline


def validate_baseline_authority(authority: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    required = {
        "schema_version", "state", "baseline_content_hash", "publication_version",
        "classification_policy", "snapshot_id", "baseline_total_count",
        "baseline_strategic_count", "retention_floor_pct", "content_hash",
        "baseline_policy_matrix_hash",
    }
    if not isinstance(authority, dict) or set(authority) != required or authority.get("schema_version") != BASELINE_AUTHORITY_SCHEMA or authority.get("state") != "complete":
        raise ArtifactValidationError("baseline authority schema mismatch")
    _hash_ok(authority, "baseline authority")
    _policy(authority["classification_policy"], "baseline authority")
    for field in ("baseline_content_hash", "baseline_policy_matrix_hash"):
        _sha(authority[field], f"baseline authority {field}")
    for field in ("publication_version", "snapshot_id"):
        if not isinstance(authority[field], str) or not authority[field]:
            raise ArtifactValidationError("baseline authority identity mismatch")
    if authority["baseline_content_hash"] != baseline["content_hash"] or any(
        authority[field] != baseline[field]
        for field in ("publication_version", "classification_policy", "snapshot_id", "baseline_total_count", "baseline_strategic_count", "retention_floor_pct")
    ):
        raise ArtifactValidationError("baseline authority binding mismatch")
    return authority


def validate_candidate(
    candidate: dict[str, Any], policy_matrix: dict[str, Any], *, w2b2_result: dict[str, Any], scoring_input: dict[str, Any]
) -> dict[str, Any]:
    """Validate a complete candidate and bind every row to authenticated W2B2 evidence."""
    matrix = _matrix(policy_matrix)
    required = {
        "schema_version", "state", "classification_policy", "snapshot_id",
        "publication_version", "as_of", "w2b2_run_hash", "quality", "rows", "content_hash",
    }
    if (
        not isinstance(candidate, dict)
        or set(candidate) != required
        or candidate.get("schema_version") != CANDIDATE_SCHEMA
        or candidate.get("state") != "complete"
    ):
        raise ArtifactValidationError("candidate schema mismatch")
    _hash_ok(candidate, "candidate")
    _policy(candidate["classification_policy"], "candidate")
    if not isinstance(candidate["snapshot_id"], str) or not candidate["snapshot_id"]:
        raise ArtifactValidationError("candidate snapshot identity required")
    if not isinstance(candidate["publication_version"], str) or not candidate["publication_version"]:
        raise ArtifactValidationError("candidate publication identity required")
    _day(candidate["as_of"], "candidate as_of")
    if candidate["classification_policy"] != {"id": policy_matrix["policy_id"], "version": policy_matrix["policy_version"]} or candidate["snapshot_id"] != scoring_input["snapshot"]["snapshot_id"]:
        raise ArtifactValidationError("candidate identity does not match authenticated scoring input")
    _sha(candidate["w2b2_run_hash"], "candidate W2B2 run hash")
    if (
        not isinstance(candidate["quality"], dict)
        or set(candidate["quality"]) != {"unidentified_pct", "nondecomposable_fund_pct"}
    ):
        raise ArtifactValidationError("candidate quality schema mismatch")
    _finite_pct(candidate["quality"]["unidentified_pct"], "candidate unidentified")
    _finite_pct(candidate["quality"]["nondecomposable_fund_pct"], "candidate nondecomposable")
    if candidate["w2b2_run_hash"] != w2b2_result.get("content_hash"):
        raise ArtifactValidationError("candidate W2B2 binding mismatch")
    rows = _rows(candidate["rows"], matrix, "candidate", allow_empty=True)
    scores = {score["series_id"]: score for score in w2b2_result["scores"]}
    if not {row["series_id"] for row in rows}.issubset(scores):
        raise ArtifactValidationError("candidate rows do not close W2B2 series")
    for row in rows:
        score = scores[row["series_id"]]
        if (
            row["w2b2_score_hash"] != canonical_sha256(score)
            or row["peer_cohort_hash"] != canonical_sha256(score["cohort"])
            or row["summary_score_hash"] != canonical_sha256(score["summary_score"])
            or row["classification_label"] != score["classification_label"]
            or row["broad_class"] != score["broad_class"]
            or row["pillar_score_hashes"]
            != {pillar: canonical_sha256(score["pillars"][pillar]) for pillar in _PILLARS}
        ):
            raise ArtifactValidationError("candidate W2B2 row binding mismatch")
    return candidate


def validate_candidate_evidence_manifest(manifest: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    required = {"schema_version", "state", "publication_version", "classification_policy", "snapshot_id", "as_of", "w2b2_run_hash", "rows", "content_hash"}
    if not isinstance(manifest, dict) or set(manifest) != required or manifest.get("schema_version") != CANDIDATE_EVIDENCE_SCHEMA or manifest.get("state") != "complete":
        raise ArtifactValidationError("candidate evidence manifest schema mismatch")
    _hash_ok(manifest, "candidate evidence manifest")
    _policy(manifest["classification_policy"], "candidate evidence manifest")
    if any(manifest[field] != candidate[field] for field in ("publication_version", "classification_policy", "snapshot_id", "as_of", "w2b2_run_hash")):
        raise ArtifactValidationError("candidate evidence manifest identity mismatch")
    expected_fields = {"series_id", "status", "optimizer_eligible", "holdings_hash", "regulatory_hash", "fee_hash", "narrative_hash", "frontend_hash", "w2b2_score_hash", "peer_cohort_hash", "summary_score_hash", "pillar_score_hashes"}
    if not isinstance(manifest["rows"], list) or any(not isinstance(row, dict) or set(row) != expected_fields for row in manifest["rows"]):
        raise ArtifactValidationError("candidate evidence manifest row schema mismatch")
    claimed = {row["series_id"]: row for row in manifest["rows"]}
    if len(claimed) != len(manifest["rows"]) or claimed != {row["series_id"]: {field: row[field] for field in expected_fields} for row in candidate["rows"]}:
        raise ArtifactValidationError("candidate evidence manifest binding mismatch")
    return manifest


def validate_expected_trust_anchors(value: dict[str, Any], authority: dict[str, Any], manifest: dict[str, Any], baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    required = {"schema_version", "baseline_authority_hash", "baseline_publication_version", "candidate_evidence_manifest_hash", "candidate_publication_version", "candidate_producer_id"}
    if not isinstance(value, dict) or set(value) != required or value.get("schema_version") != TRUST_ANCHORS_SCHEMA:
        raise ArtifactValidationError("expected trust anchors schema mismatch")
    for field in ("baseline_authority_hash", "candidate_evidence_manifest_hash"):
        _sha(value[field], f"expected trust anchor {field}")
    if not isinstance(value["candidate_producer_id"], str) or not value["candidate_producer_id"]:
        raise ArtifactValidationError("expected trust anchor provenance missing")
    if value["baseline_authority_hash"] != authority["content_hash"] or value["candidate_evidence_manifest_hash"] != manifest["content_hash"] or value["baseline_publication_version"] != baseline["publication_version"] or value["candidate_publication_version"] != candidate["publication_version"]:
        raise ArtifactValidationError("expected trust anchors mismatch")
    return value


def _authenticate_w2b2(
    *, scoring_input: dict[str, Any], policy_matrix: dict[str, Any], evidence_manifest: dict[str, Any],
    phase4_manifest: dict[str, Any], w2a_bundle: Path | str, factor_input: dict[str, Any],
    w2b1_result: dict[str, Any], w2b2_result: dict[str, Any], factor_runner_sha: str,
    scoring_runner_sha: str,
) -> dict[str, Any]:
    _git(factor_runner_sha, "W2B1 runner SHA")
    _git(scoring_runner_sha, "W2B2 runner SHA")
    return peers.validate_authenticated_result(
        w2b2_result, scoring_input, policy_matrix,
        evidence_manifest=evidence_manifest, phase4_manifest=phase4_manifest,
        w2a_bundle=w2a_bundle, factor_input=factor_input, w2b1_result=w2b1_result,
        factor_runner_sha=factor_runner_sha, runner_sha=scoring_runner_sha,
    )


def _pct(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else 100.0 * numerator / denominator


def _at_least(value: float | None, threshold: float) -> bool:
    return value is not None and value >= threshold


def broad_class_coverage_passes(value: float | None) -> bool:
    """Pinned 90% threshold; integrated coverage is baseline-family retention."""
    return _at_least(value, 90.0)


def _breakdown(baseline_rows: list[dict[str, Any]], candidate_rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    values = sorted({str(row[key]) for row in [*baseline_rows, *candidate_rows]})
    result = []
    for value in values:
        prior = [row for row in baseline_rows if str(row[key]) == value]
        candidate = [row for row in candidate_rows if str(row[key]) == value]
        prior_ids, candidate_ids = {row["series_id"] for row in prior}, {row["series_id"] for row in candidate}
        retained_ids = prior_ids & candidate_ids
        result.append({
            "value": value, "prior_total": len(prior), "candidate_total": len(candidate),
            "retained_total": len(retained_ids), "entrants": len(candidate_ids - prior_ids),
            "exits": len(prior_ids - candidate_ids), "retention_pct": _pct(len(retained_ids), len(prior)),
        })
    return result


def _deltas(baseline: dict[str, Any], candidate: dict[str, Any], scores: Mapping[str, Any]) -> list[dict[str, Any]]:
    baseline_rows, candidate_rows = baseline["rows"], candidate["rows"]
    old = {row["series_id"]: row for row in baseline_rows}
    new = {row["series_id"]: row for row in candidate_rows}
    output = []
    for series_id in sorted(set(old) | set(new)):
        prior, candidate_row = old.get(series_id), new.get(series_id)
        changes: list[str] = []
        evidence: dict[str, dict[str, Any]] = {}
        for name, field in (
            ("classification", "classification_label"), ("benchmark", "benchmark_family"),
            ("proxy", "approved_proxy"), ("peer_cohort", "peer_cohort_hash"), ("summary_score", "summary_score_hash"), ("status", "status"), ("optimizer_eligibility", "optimizer_eligible"),
            ("holdings", "holdings_hash"), ("regulatory", "regulatory_hash"),
            ("fees", "fee_hash"), ("narratives", "narrative_hash"), ("frontend", "frontend_hash"),
        ):
            before = None if prior is None else prior[field]
            after = None if candidate_row is None else candidate_row[field]
            if before != after:
                changes.append(name)
            evidence[name] = {"prior": before, "candidate": after}
        if prior is not None and candidate_row is not None and prior["w2b2_score_hash"] != candidate_row["w2b2_score_hash"]:
            changes.append("w2b2_score")
        evidence["w2b2_score"] = {"prior": None if prior is None else prior["w2b2_score_hash"], "candidate": None if candidate_row is None else candidate_row["w2b2_score_hash"], "authenticated_score_present": series_id in scores}
        for pillar in _PILLARS:
            prior_pillar = None if prior is None else prior["pillar_score_hashes"][pillar]
            candidate_pillar = None if candidate_row is None else candidate_row["pillar_score_hashes"][pillar]
            if prior_pillar != candidate_pillar:
                changes.append(f"pillar:{pillar}")
            evidence[f"pillar:{pillar}"] = {"prior": prior_pillar, "candidate": candidate_pillar}
        output.append({
            "series_id": series_id,
            "change_reason_codes": sorted(changes + (["policy"] if baseline["classification_policy"] != candidate["classification_policy"] else []) + (["snapshot"] if baseline["snapshot_id"] != candidate["snapshot_id"] else [])),
            "prior_evidence_hash": None if prior is None else prior["w2b2_score_hash"],
            "candidate_evidence_hash": None if candidate_row is None else candidate_row["w2b2_score_hash"],
            "evidence": evidence,
        })
    return output


def _trusted_anchor_evidence(phase4_manifest: dict[str, Any], w2a_bundle: Path | str, candidate_as_of: date) -> tuple[dict[str, bool], dict[str, Any]]:
    """Derive P9 anchor gates solely from W2A-authenticated series summaries.

    Coverage is pinned as `100 - unidentified_pct - nondecomposable_fund_pct`
    per summary; an anchor uses its worst series and the run uses its worst anchor.
    """
    aggregate = factors._w2a(phase4_manifest, Path(w2a_bundle))
    outputs = w2a._read_prior_complete_anchor_outputs(Path(w2a_bundle), aggregate["anchors"])
    records: list[dict[str, Any]] = []
    for output in outputs:
        report = _day(output["anchor"]["report_date"], "W2A anchor report_date")
        summaries = [row["summary"] for row in output["series"]]
        if not summaries:
            raise ArtifactValidationError("W2A anchor has no trusted summaries")
        normalized = []
        for summary in summaries:
            unidentified_value = _finite_pct(summary["unidentified_pct"], "W2A unidentified")
            nondecomposable_value = _finite_pct(summary["nondecomposable_fund_pct"], "W2A nondecomposable")
            coverage_value = 100.0 - unidentified_value - nondecomposable_value
            if coverage_value < 0 or coverage_value > 100:
                raise ArtifactValidationError("W2A derived coverage invalid")
            oldest_value = _day(summary["oldest_report_date"], "W2A oldest report_date")
            if oldest_value > report or (report - oldest_value).days > 180:
                raise ArtifactValidationError("W2A oldest report date/lag invalid")
            normalized.append((unidentified_value, nondecomposable_value, coverage_value, oldest_value))
        unidentified = max(item[0] for item in normalized)
        nondecomposable = max(item[1] for item in normalized)
        coverage = min(item[2] for item in normalized)
        oldest = min(item[3] for item in normalized)
        records.append({"report_date": report, "oldest_report_date": oldest, "coverage_pct": coverage, "unidentified_pct": unidentified, "nondecomposable_fund_pct": nondecomposable})
    return _derive_anchor_gates(records, candidate_as_of)


def _derive_anchor_gates(records: list[dict[str, Any]], candidate_as_of: date) -> tuple[dict[str, bool], dict[str, Any]]:
    """Apply the pinned readiness formula to W2A-derived anchor records."""
    if not records:
        return {"anchor_count": False, "anchor_span": False, "latest_anchor_age": False, "anchor_coverage": False, "latest_coverage": False, "chain_lag": False, "unidentified_pct": False, "nondecomposable_pct": False}, {"anchors": []}
    for item in records:
        unidentified = _finite_pct(item["unidentified_pct"], "W2A unidentified")
        nondecomposable = _finite_pct(item["nondecomposable_fund_pct"], "W2A nondecomposable")
        if _finite_pct(item["coverage_pct"], "W2A coverage") != 100.0 - unidentified - nondecomposable:
            raise ArtifactValidationError("W2A derived coverage invalid")
        if item["oldest_report_date"] > item["report_date"] or (item["report_date"] - item["oldest_report_date"]).days > 180:
            raise ArtifactValidationError("W2A oldest report date/lag invalid")
    days = [item["report_date"] for item in records]
    months = (days[-1].year - days[0].year) * 12 + days[-1].month - days[0].month
    latest = records[-1]
    evidence = {
        "anchors": [{key: (value.isoformat() if isinstance(value, date) else value) for key, value in item.items()} for item in records],
        "worst_coverage_pct": min(item["coverage_pct"] for item in records),
        "worst_unidentified_pct": max(item["unidentified_pct"] for item in records),
        "worst_nondecomposable_fund_pct": max(item["nondecomposable_fund_pct"] for item in records),
        "worst_chain_lag_days": max((item["report_date"] - item["oldest_report_date"]).days for item in records),
    }
    return {
        "anchor_count": len(records) == 8,
        "anchor_span": months >= 21,
        "latest_anchor_age": 0 <= (candidate_as_of - latest["report_date"]).days <= 180,
        "anchor_coverage": evidence["worst_coverage_pct"] >= 80.0,
        "latest_coverage": latest["coverage_pct"] >= 80.0,
        "chain_lag": evidence["worst_chain_lag_days"] <= 180,
        "unidentified_pct": evidence["worst_unidentified_pct"] <= 10.0,
        "nondecomposable_pct": evidence["worst_nondecomposable_fund_pct"] <= 10.0,
    }, evidence


def build_readiness_result(
    baseline: dict[str, Any], candidate: dict[str, Any], *, scoring_input: dict[str, Any],
    policy_matrix: dict[str, Any], evidence_manifest: dict[str, Any], phase4_manifest: dict[str, Any],
    w2a_bundle: Path | str, factor_input: dict[str, Any], w2b1_result: dict[str, Any],
    w2b2_result: dict[str, Any], factor_runner_sha: str, scoring_runner_sha: str, runner_sha: str,
    baseline_authority: dict[str, Any], candidate_evidence_manifest: dict[str, Any], expected_trust_anchors: dict[str, Any], baseline_policy_matrix: dict[str, Any],
) -> dict[str, Any]:
    """Build deterministic readiness evidence after full upstream authentication."""
    _git(runner_sha, "W2B3 runner SHA")
    authenticated = _authenticate_w2b2(
        scoring_input=scoring_input, policy_matrix=policy_matrix, evidence_manifest=evidence_manifest,
        phase4_manifest=phase4_manifest, w2a_bundle=w2a_bundle, factor_input=factor_input,
        w2b1_result=w2b1_result, w2b2_result=w2b2_result, factor_runner_sha=factor_runner_sha,
        scoring_runner_sha=scoring_runner_sha,
    )
    inherited = validate_baseline(baseline, baseline_policy_matrix)
    authority = validate_baseline_authority(baseline_authority, inherited)
    if authority["baseline_policy_matrix_hash"] != baseline_policy_matrix["content_hash"]:
        raise ArtifactValidationError("baseline authority historical matrix mismatch")
    proposed = validate_candidate(candidate, policy_matrix, w2b2_result=authenticated, scoring_input=scoring_input)
    candidate_evidence = validate_candidate_evidence_manifest(candidate_evidence_manifest, proposed)
    trust = validate_expected_trust_anchors(expected_trust_anchors, authority, candidate_evidence, inherited, proposed)
    baseline_rows, candidate_rows = inherited["rows"], proposed["rows"]
    total_retention = _pct(len({row["series_id"] for row in baseline_rows} & {row["series_id"] for row in candidate_rows}), inherited["baseline_total_count"])
    strategic_retained = sum(
        row["optimizer_eligible"] and row["series_id"] in {item["series_id"] for item in candidate_rows}
        and next(item for item in candidate_rows if item["series_id"] == row["series_id"])["optimizer_eligible"]
        for row in baseline_rows
    )
    strategic_retention = _pct(strategic_retained, inherited["baseline_strategic_count"])
    baseline_broad = {row["broad_class"] for row in baseline_rows}
    candidate_coverage = _pct(len(baseline_broad & {row["broad_class"] for row in candidate_rows}), len(baseline_broad))
    gates, anchor_evidence = _trusted_anchor_evidence(
        phase4_manifest, w2a_bundle, _day(proposed["as_of"], "candidate as_of")
    )
    gates.update({
        "candidate_non_empty": bool(candidate_rows),
        "broad_class_coverage": broad_class_coverage_passes(candidate_coverage),
        "total_series_retention": _at_least(total_retention, inherited["retention_floor_pct"]),
        "strategic_retention": _at_least(strategic_retention, 90.0),
        "w2b2_complete": authenticated.get("state") == "complete" and all(
            pillar["status"] != "insufficient" and pillar["score"] is not None
            for score in authenticated["scores"] for pillar in score["pillars"].values()
            if pillar["status"] != "not_applicable"
        ),
        "artifact_consistency": True,
    })
    failed = sorted(name for name, passed in gates.items() if not passed)
    scores = {score["series_id"]: score for score in authenticated["scores"]}
    result = {
        "schema_version": RUN_SCHEMA,
        "state": "complete",
        "status": "ready" if not failed else "not_ready",
        "reason_codes": [f"gate_failed:{name}" for name in failed],
        "binding": {
            "baseline_hash": inherited["content_hash"], "baseline_authority_hash": authority["content_hash"],
            "baseline_policy_matrix_hash": baseline_policy_matrix["content_hash"],
            "candidate_hash": proposed["content_hash"], "candidate_evidence_manifest_hash": candidate_evidence["content_hash"],
            "expected_trust_anchors_hash": canonical_sha256(trust), "candidate_producer_id": trust["candidate_producer_id"],
            "phase4_manifest_hash": factors.canonical_sha256(phase4_manifest),
            "w2a_run_hash": authenticated["binding"]["w2a_run_hash"],
            "factor_input_hash": factors.canonical_sha256(factor_input),
            "w2b1_run_hash": w2b1_result["content_hash"], "w2b2_run_hash": authenticated["content_hash"],
            "matrix_hash": peers.canonical_sha256(policy_matrix),
            "evidence_manifest_hash": peers.canonical_sha256(evidence_manifest),
            "factor_runner_sha": factor_runner_sha, "scoring_runner_sha": scoring_runner_sha,
            "runner_sha": runner_sha,
        },
        "gates": gates,
        "metrics": {
            "total_retention_pct": total_retention, "strategic_retention_pct": strategic_retention,
            "candidate_broad_class_coverage_pct": candidate_coverage,
            "worst_unidentified_pct": anchor_evidence["worst_unidentified_pct"],
            "worst_nondecomposable_fund_pct": anchor_evidence["worst_nondecomposable_fund_pct"],
            "worst_anchor_coverage_pct": anchor_evidence["worst_coverage_pct"],
            "worst_chain_lag_days": anchor_evidence["worst_chain_lag_days"],
        },
        "deltas": _deltas(inherited, proposed, scores),
        "anchor_evidence": anchor_evidence,
        "breakdowns": {
            "broad_class": _breakdown(baseline_rows, candidate_rows, "broad_class"),
            "strategic_label": _breakdown(baseline_rows, candidate_rows, "classification_label"),
            "status": _breakdown(baseline_rows, candidate_rows, "status"),
            "proxy_eligibility": _breakdown(baseline_rows, candidate_rows, "approved_proxy"),
            "optimizer_sleeve": _breakdown(baseline_rows, candidate_rows, "optimizer_sleeve"),
        },
    }
    result["content_hash"] = canonical_sha256(result)
    return result


def validate_authenticated_result(result: dict[str, Any], baseline: dict[str, Any], candidate: dict[str, Any], **context: Any) -> dict[str, Any]:
    """Consumer-facing validation by rebuilding the complete expected result."""
    expected = build_readiness_result(baseline, candidate, **context)
    if result != expected:
        raise ArtifactValidationError("W2B3 artifact does not match governed inputs")
    return result


def _no_reparse(path: Path) -> None:
    try:
        item = os.lstat(path)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(item.st_mode) or bool(getattr(item, "st_file_attributes", 0) & 0x400):
        raise ArtifactValidationError("symlink/reparse output refused")


def _external(path: Path) -> Path:
    probe = path
    while probe != probe.parent:
        _no_reparse(probe)
        probe = probe.parent
    resolved = path.resolve()
    for root in (Path(__file__).resolve().parents[2], Path("E:/Edgard/nport"), Path("E:/Edgard/ncen"), Path("E:/Edgard/RR1"), Path("E:/Edgard/13-F")):
        try:
            resolved.relative_to(root.resolve())
            raise ArtifactValidationError("output must be external to Git/source roots")
        except ValueError:
            pass
    return resolved


def _read(path: Path) -> dict[str, Any]:
    _no_reparse(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ArtifactValidationError("explicit JSON artifact could not be read") from error
    if not isinstance(value, dict):
        raise ArtifactValidationError("explicit JSON artifact must be object")
    return value


def _write(path: Path, value: dict[str, Any]) -> None:
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(canonical_json(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        if os.path.exists(temporary):
            os.unlink(temporary)
        raise


def _fsync_directory(path: Path) -> None:
    """Persist a completed staging directory before and after promotion."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def run_artifact(baseline: dict[str, Any], candidate: dict[str, Any], output_dir: Path | str, **context: Any) -> dict[str, Any]:
    result = build_readiness_result(baseline, candidate, **context)
    destination = _external(Path(output_dir))
    if destination.exists():
        child = destination / "shadow_readiness.json"
        _no_reparse(destination)
        _no_reparse(child)
        if not destination.is_dir() or {item.name for item in destination.iterdir()} != {"shadow_readiness.json"} or not child.is_file() or child.read_bytes() != canonical_json(result):
            raise ArtifactValidationError("partial foreign mixed or noncanonical output")
        return validate_authenticated_result(_read(child), baseline, candidate, **context)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.stage-", dir=destination.parent))
    try:
        child = staging / "shadow_readiness.json"
        _write(child, result)
        _no_reparse(staging)
        _no_reparse(child)
        if {item.name for item in staging.iterdir()} != {"shadow_readiness.json"} or child.read_bytes() != canonical_json(result):
            raise ArtifactValidationError("staged output is partial or noncanonical")
        validate_authenticated_result(_read(child), baseline, candidate, **context)
        _fsync_directory(staging)
        os.replace(staging, destination)
        _fsync_directory(destination.parent)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return result


def run_shadow_unconfigured(*, authorization: dict[str, Any], baseline: dict[str, Any], candidate: dict[str, Any], **context: Any) -> None:
    result = build_readiness_result(baseline, candidate, **context)
    expected = {
        "stage": "phase9_shadow", "command": "shadow-db-write", **result["binding"],
        "output_content_hash": result["content_hash"], "target": "isolated-shadow", "role": "shadow_writer",
    }
    if not isinstance(authorization, dict) or set(authorization) != set(expected) or authorization != expected or any(word in key for key in authorization for word in ("pointer", "current", "provider")):
        raise ArtifactValidationError("shadow authorization binding mismatch")
    raise ArtifactValidationError("shadow_writer_unconfigured")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run artifact-only SEC class shadow readiness evidence")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--artifact-only", action="store_true")
    modes.add_argument("--shadow-db-write", action="store_true")
    for name in ("phase4-manifest", "w2a-bundle", "factor-input", "w2b1-run", "scoring-input", "policy-matrix", "baseline-policy-matrix", "evidence-manifest", "w2b2-run", "baseline", "baseline-authority", "candidate", "candidate-evidence-manifest", "expected-trust-anchors"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--authorization-record", type=Path)
    parser.add_argument("--factor-runner-sha", required=True)
    parser.add_argument("--scoring-runner-sha", required=True)
    parser.add_argument("--runner-sha", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        context = {
            "scoring_input": _read(args.scoring_input), "policy_matrix": _read(args.policy_matrix),
            "evidence_manifest": _read(args.evidence_manifest), "phase4_manifest": _read(args.phase4_manifest),
            "w2a_bundle": args.w2a_bundle, "factor_input": _read(args.factor_input),
            "w2b1_result": _read(args.w2b1_run), "w2b2_result": _read(args.w2b2_run),
            "factor_runner_sha": args.factor_runner_sha, "scoring_runner_sha": args.scoring_runner_sha, "runner_sha": args.runner_sha,
            "baseline_authority": _read(args.baseline_authority), "candidate_evidence_manifest": _read(args.candidate_evidence_manifest),
            "expected_trust_anchors": _read(args.expected_trust_anchors),
            "baseline_policy_matrix": _read(args.baseline_policy_matrix),
        }
        baseline, candidate = _read(args.baseline), _read(args.candidate)
        if args.artifact_only:
            if args.output_dir is None:
                raise ArtifactValidationError("artifact-only mode requires --output-dir")
            print(canonical_json(run_artifact(baseline, candidate, args.output_dir, **context)).decode())
            return 0
        if args.authorization_record is None:
            raise ArtifactValidationError("shadow mode requires --authorization-record")
        run_shadow_unconfigured(authorization=_read(args.authorization_record), baseline=baseline, candidate=candidate, **context)
    except ArtifactValidationError as error:
        print(str(error))
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
