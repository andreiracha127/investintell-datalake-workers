"""A3 metric canonicalization and comparison policies."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any

from investintell_quant_core.hashing.canonical import logical_payload_hash, logical_records_hash

FLOAT_TOLERANCE = 1e-12
FLOAT_REL_TOLERANCE = 1e-12
METRIC_HASH_FLOAT_DECIMALS = 12
METRICS_HASH_POLICY_VERSION = "qc_a3_metrics_float_canonical_v1"
BUNDLE_EVALUATION_HASH_POLICY_VERSION = "qc_a3_parity_bundle_v1"
QC_A3_BRIDGE_SCHEMA_VERSION = 1


def compare_rows(
    fold: str,
    actual: dict[str, Any],
    expected: dict[str, Any],
) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    for key in sorted(set(actual) | set(expected)):
        if key not in actual or key not in expected:
            mismatches.append({"fold": fold, "field": key, "issue": "missing_field"})
            continue
        lhs = actual[key]
        rhs = expected[key]
        if lhs is None and rhs is None:
            continue
        if values_equivalent(lhs, rhs):
            continue
        if is_number(lhs) and is_number(rhs):
            mismatches.append({
                "fold": fold,
                "field": key,
                "actual": lhs,
                "expected": rhs,
                "abs_diff": abs(float(lhs) - float(rhs)),
            })
        else:
            mismatches.append({
                "fold": fold,
                "field": key,
                "actual": normalize_scalar(lhs),
                "expected": normalize_scalar(rhs),
            })
    return mismatches


def float_diff_summary(
    actual_rows: list[dict[str, Any]],
    expected_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    actual_by_fold = {str(row.get("fold")): row for row in actual_rows}
    diffs: list[dict[str, Any]] = []
    for expected in expected_rows:
        fold = str(expected.get("fold"))
        actual = actual_by_fold.get(fold)
        if actual is None:
            continue
        for key in sorted(set(actual) & set(expected)):
            collect_float_diffs(
                diffs,
                fold=fold,
                field=key,
                actual=actual[key],
                expected=expected[key],
            )
    max_abs_diff = max((item["abs_diff"] for item in diffs), default=0.0)
    max_rel_diff = max((item["rel_diff"] for item in diffs), default=0.0)
    differing_fields = sorted({item["field"] for item in diffs})
    return {
        "float_abs_tolerance": FLOAT_TOLERANCE,
        "float_rel_tolerance": FLOAT_REL_TOLERANCE,
        "max_abs_diff": max_abs_diff,
        "max_rel_diff": max_rel_diff,
        "differing_float_fields": differing_fields,
    }


def collect_float_diffs(
    diffs: list[dict[str, Any]],
    *,
    fold: str,
    field: str,
    actual: Any,
    expected: Any,
) -> None:
    actual_json = parse_json_scalar(actual)
    expected_json = parse_json_scalar(expected)
    if actual_json is not None and expected_json is not None:
        collect_json_float_diffs(
            diffs,
            fold=fold,
            field=field,
            actual=actual_json,
            expected=expected_json,
        )
        return
    if is_number(actual) and is_number(expected):
        append_float_diff(diffs, fold=fold, field=field, actual=float(actual), expected=float(expected))


def collect_json_float_diffs(
    diffs: list[dict[str, Any]],
    *,
    fold: str,
    field: str,
    actual: Any,
    expected: Any,
) -> None:
    if is_number(actual) and is_number(expected):
        append_float_diff(diffs, fold=fold, field=field, actual=float(actual), expected=float(expected))
        return
    if isinstance(actual, dict) and isinstance(expected, dict):
        for key in sorted(set(actual) & set(expected)):
            collect_json_float_diffs(
                diffs,
                fold=fold,
                field=f"{field}.{key}",
                actual=actual[key],
                expected=expected[key],
            )
        return
    if isinstance(actual, list) and isinstance(expected, list):
        for index, (left, right) in enumerate(zip(actual, expected)):
            collect_json_float_diffs(
                diffs,
                fold=fold,
                field=f"{field}[{index}]",
                actual=left,
                expected=right,
            )


def append_float_diff(
    diffs: list[dict[str, Any]],
    *,
    fold: str,
    field: str,
    actual: float,
    expected: float,
) -> None:
    abs_diff = abs(actual - expected)
    if abs_diff == 0:
        return
    denominator = max(abs(expected), FLOAT_TOLERANCE)
    diffs.append({
        "fold": fold,
        "field": field,
        "actual": actual,
        "expected": expected,
        "abs_diff": abs_diff,
        "rel_diff": abs_diff / denominator,
    })


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def values_equivalent(lhs: Any, rhs: Any) -> bool:
    if lhs is None and rhs is None:
        return True
    if is_number(lhs) and is_number(rhs):
        return math.isclose(
            float(lhs),
            float(rhs),
            rel_tol=FLOAT_REL_TOLERANCE,
            abs_tol=FLOAT_TOLERANCE,
        )
    lhs_json = parse_json_scalar(lhs)
    rhs_json = parse_json_scalar(rhs)
    if lhs_json is not None and rhs_json is not None:
        return json_values_equivalent(lhs_json, rhs_json)
    return normalize_scalar(lhs) == normalize_scalar(rhs)


def parse_json_scalar(value: Any) -> Any | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text[0] not in "[{":
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def json_values_equivalent(lhs: Any, rhs: Any) -> bool:
    if is_number(lhs) and is_number(rhs):
        return math.isclose(
            float(lhs),
            float(rhs),
            rel_tol=FLOAT_REL_TOLERANCE,
            abs_tol=FLOAT_TOLERANCE,
        )
    if isinstance(lhs, dict) and isinstance(rhs, dict):
        if set(lhs) != set(rhs):
            return False
        return all(json_values_equivalent(lhs[key], rhs[key]) for key in lhs)
    if isinstance(lhs, list) and isinstance(rhs, list):
        if len(lhs) != len(rhs):
            return False
        return all(json_values_equivalent(left, right) for left, right in zip(lhs, rhs))
    return normalize_scalar(lhs) == normalize_scalar(rhs)


def normalize_scalar(value: Any) -> Any:
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return value.item()
        except (AttributeError, TypeError, ValueError):
            return value
    return value


def canonical_metric_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {key: canonical_metric_value(value) for key, value in row.items()}
        for row in rows
    ]


def canonical_metric_value(value: Any) -> Any:
    if is_number(value):
        rounded = round(float(value), METRIC_HASH_FLOAT_DECIMALS)
        return 0.0 if rounded == 0 else rounded
    parsed = parse_json_scalar(value)
    if parsed is not None:
        return json.dumps(
            canonical_metric_value(parsed),
            sort_keys=True,
            separators=(",", ":"),
        )
    if isinstance(value, dict):
        return {
            str(key): canonical_metric_value(item)
            for key, item in sorted(value.items())
        }
    if isinstance(value, list):
        return [canonical_metric_value(item) for item in value]
    return normalize_scalar(value)


def metric_rows_logical_hash(rows: list[dict[str, Any]]) -> str:
    return logical_records_hash(canonical_metric_rows(rows))


def metric_rows_raw_sha256(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        rows,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def metrics_hash_policy_payload(
    rows: list[dict[str, Any]],
    comparison: dict[str, Any],
) -> dict[str, Any]:
    return {
        "metrics_hash_policy_version": METRICS_HASH_POLICY_VERSION,
        "metrics_raw_sha256": metric_rows_raw_sha256(rows),
        "metrics_canonical_logical_hash": metric_rows_logical_hash(rows),
        "float_abs_tolerance": FLOAT_TOLERANCE,
        "float_rel_tolerance": FLOAT_REL_TOLERANCE,
        "max_abs_diff": comparison.get("max_abs_diff"),
        "max_rel_diff": comparison.get("max_rel_diff"),
        "differing_float_fields": comparison.get("differing_float_fields", []),
    }


def bundle_evaluation_hash(
    *,
    worker_commit: str | None,
    result: dict[str, Any],
    metrics_policy: dict[str, Any],
) -> str:
    return logical_payload_hash({
        "policy_version": BUNDLE_EVALUATION_HASH_POLICY_VERSION,
        "schema_version": QC_A3_BRIDGE_SCHEMA_VERSION,
        "worker_commit": worker_commit,
        "model_evaluation_hash": result["evaluation_hash"],
        "a31_config_hash": result["a31_hash"],
        "a32_config_hash": result["a32_hash"],
        "parent_l2_macro_logical_hash": result["l2_hash"],
        "revision_uncertainty_logical_hash": result["uncertainty_hash"],
        "metrics_hash_policy_version": metrics_policy["metrics_hash_policy_version"],
        "metrics_canonical_logical_hash": metrics_policy["metrics_canonical_logical_hash"],
    })[:24]

