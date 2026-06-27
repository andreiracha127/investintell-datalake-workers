import argparse
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid
import zipfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_A31_NAME = "G2-CREDIT6040-15-SURVEY05"
DEFAULT_A32_NAME = "A32-G0.35-I0.35-X0.10-C0.60-D1.25"
OBJECT_STORE_BASE_PREFIX = "investintell/a3/qc-a3-parity"
OBJECT_STORE_PREFIX = OBJECT_STORE_BASE_PREFIX
QC_A3_BRIDGE_SCHEMA_VERSION = 1
FLOAT_TOLERANCE = 1e-12
FLOAT_REL_TOLERANCE = 1e-12
METRIC_HASH_FLOAT_DECIMALS = 12
METRICS_HASH_POLICY_VERSION = "qc_a3_metrics_float_canonical_v1"
BUNDLE_EVALUATION_HASH_POLICY_VERSION = "qc_a3_parity_bundle_v1"
OBJECT_STORE_UPLOAD_FILE_KEYS = {
    "feature_manifest": "manifests/feature_manifest.json",
    "revision_uncertainty_manifest": "manifests/revision_uncertainty_manifest.json",
    "config_catalog_normalized": "manifests/config_catalog.normalized.json",
    "selected_a31_config": "manifests/selected_a31_config.json",
    "selected_a32_config": "manifests/selected_a32_config.json",
    "l3_manifest": "manifests/l3_manifest.json",
    "macro_l2_union_numeric": "panels/macro_l2_union_numeric.npz",
    "revision_uncertainty_numeric": "panels/revision_uncertainty_numeric.npz",
    "expected_runtime_replay": "expected/macro_runtime_replay.csv.gz",
    "expected_counterfactual_replay": "expected/macro_counterfactual_replay.csv.gz",
    "expected_metric_rows": "expected/macro_metric_rows.json",
}
OBJECT_STORE_SOURCE_FILE_KEYS = {
    "calibration_harness_source": "code/calibration_harness.py.gz",
}


@dataclass(frozen=True)
class A3ParityConfig:
    feature_manifest: Path
    revision_uncertainty_manifest: Path
    config_catalog: Path
    a32_grid_dir: Path
    output_dir: Path
    expected_v03_grid_dir: Path | None = None
    macro_l2_npz: Path | None = None
    revision_uncertainty_npz: Path | None = None
    a31_name: str = DEFAULT_A31_NAME
    a32_name: str = DEFAULT_A32_NAME
    worker_commit: str | None = None


def require_harness():
    try:
        from src import calibration_harness as harness
    except ImportError as exc:  # pragma: no cover - exercised in QC if project files are absent
        raise RuntimeError(
            "qc_a3_core.py must run with the Investintell worker project files "
            "available so it can import src.calibration_harness. In QC Research, "
            "call materialize_harness_source_from_manifest(...) before run_parity."
        ) from exc
    return harness


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def load_a31_from_catalog(
    *,
    config_catalog: Path,
    l2_macro_logical_hash: str,
    a31_name: str,
) -> tuple[Any, str, dict[str, Any], str]:
    harness = require_harness()
    payload = harness.read_catalog_payload(config_catalog)
    normalized, catalog_hash = harness.normalize_a31_catalog(
        payload,
        l2_macro_logical_hash=l2_macro_logical_hash,
        source_path=config_catalog,
    )
    matches = [
        item for item in normalized["configs"]
        if item["config"]["name"] == a31_name
    ]
    if len(matches) != 1:
        raise ValueError(f"A31 config is not unique in catalog: {a31_name}")
    item = matches[0]
    return harness.A31Config(**item["config"]), str(item["a31_config_hash"]), normalized, catalog_hash


def load_l2_macro_for_config(config: A3ParityConfig) -> tuple[dict[str, Any], Path, str, list[dict[str, Any]]]:
    harness = require_harness()
    if config.macro_l2_npz is None:
        return harness.load_l2_macro_from_feature_manifest(config.feature_manifest)
    manifest = read_json(config.feature_manifest)
    validate_feature_manifest_contract(manifest)
    macro_meta = manifest.get("macro_feature_primitives") or {}
    expected_hash = str(macro_meta.get("logical_hash") or "")
    if not expected_hash:
        raise ValueError("feature_manifest is missing macro_feature_primitives.logical_hash")
    expected_count = int(macro_meta.get("row_count") or 0)
    rows = read_npz_records(config.macro_l2_npz)
    actual_hash = harness.logical_records_hash(rows)
    harness.validate_parent_hash("QC A3 NPZ L2 macro_feature_primitives", actual_hash, expected_hash)
    if expected_count and len(rows) != expected_count:
        raise ValueError("macro_feature_primitives NPZ row_count mismatch")
    return manifest, config.macro_l2_npz, actual_hash, rows


def validate_feature_manifest_contract(manifest: dict[str, Any]) -> None:
    if manifest.get("parameter_independent") is not True:
        raise ValueError("feature_manifest must be parameter_independent=true")
    if manifest.get("counterfactual_runtime_allowed") is not False:
        raise ValueError("feature_manifest must forbid counterfactual runtime use")
    roles = manifest.get("selection_roles") or {}
    if roles.get("latest") != "pit_runtime_candidate":
        raise ValueError("feature_manifest.latest must be pit_runtime_candidate")
    if roles.get("first_release") != "revised_vintage_counterfactual":
        raise ValueError("feature_manifest.first_release must be revised_vintage_counterfactual")


def load_revision_uncertainty_for_config(
    config: A3ParityConfig,
) -> tuple[dict[str, Any], str, list[dict[str, Any]]]:
    harness = require_harness()
    if config.revision_uncertainty_npz is None:
        return harness.load_revision_uncertainty_from_manifest(
            config.revision_uncertainty_manifest
        )
    manifest = read_json(config.revision_uncertainty_manifest)
    expected_hash = str(manifest.get("logical_hash") or "")
    if not expected_hash:
        raise ValueError("revision uncertainty manifest missing logical_hash")
    expected_count = int(manifest.get("row_count") or 0)
    rows = read_npz_records(config.revision_uncertainty_npz)
    actual_hash = harness.logical_records_hash(rows)
    harness.validate_parent_hash("QC A3 NPZ revision_uncertainty_primitives", actual_hash, expected_hash)
    if expected_count and len(rows) != expected_count:
        raise ValueError("revision_uncertainty_primitives NPZ row_count mismatch")
    return manifest, actual_hash, rows


def compute_a3_case(config: A3ParityConfig) -> dict[str, Any]:
    harness = require_harness()
    feature_manifest, l2_path, l2_hash, l2_records = load_l2_macro_for_config(config)
    uncertainty_manifest, uncertainty_hash, uncertainty_rows = (
        load_revision_uncertainty_for_config(config)
    )
    parent_uncertainty_l2 = (
        uncertainty_manifest.get("parent_hashes") or {}
    ).get("l2_macro_logical_hash")
    harness.validate_parent_hash("QC A3 uncertainty parent L2", str(parent_uncertainty_l2), l2_hash)

    a31, catalog_a31_hash, normalized_catalog, catalog_hash = load_a31_from_catalog(
        config_catalog=config.config_catalog,
        l2_macro_logical_hash=l2_hash,
        a31_name=config.a31_name,
    )
    uncertainty_by_key = harness.revision_uncertainty_keyed(uncertainty_rows)
    l3_rows, contribution_rows, l3_manifest = harness.build_l3_score_panel(
        l2_records,
        a31,
        l2_macro_logical_hash=l2_hash,
        expected_l2_macro_logical_hash=l2_hash,
        revision_uncertainty_by_key=uncertainty_by_key,
        revision_uncertainty_logical_hash=uncertainty_hash,
    )
    a31_hash = str(l3_manifest["a31_config_hash"])
    harness.validate_parent_hash("QC A3 catalog A31 hash", catalog_a31_hash, a31_hash)
    a32 = load_a32_config(config.a32_grid_dir, config.a32_name)
    a32_hash = harness.a32_config_hash(a32)
    evaluation_hash = harness.evaluation_hash(a31_hash, a32_hash)

    runtime, _ = harness.run_l4_state_machine(l3_rows, a32, selection_mode="latest")
    counterfactual, _ = harness.run_l4_state_machine(
        l3_rows,
        a32,
        selection_mode="first_release",
    )
    metrics_full = harness.build_macro_metrics(
        runtime,
        first_release_replay=counterfactual,
    )
    classification = harness.classify_a32_grid_result(metrics_full)
    metric_rows = harness.evaluation_metric_rows(
        runtime,
        counterfactual,
        a31,
        a32,
        a31_hash,
        a32_hash,
        evaluation_hash,
        classification,
    )
    comparison = compare_expected_metrics(config, metric_rows)
    return {
        "feature_manifest": feature_manifest,
        "l2_path": l2_path,
        "l2_hash": l2_hash,
        "l2_row_count": len(l2_records),
        "uncertainty_manifest": uncertainty_manifest,
        "uncertainty_hash": uncertainty_hash,
        "uncertainty_row_count": len(uncertainty_rows),
        "normalized_catalog": normalized_catalog,
        "catalog_hash": catalog_hash,
        "a31": a31,
        "a31_hash": a31_hash,
        "a32": a32,
        "a32_hash": a32_hash,
        "evaluation_hash": evaluation_hash,
        "l3_rows": l3_rows,
        "l3_contribution_rows": contribution_rows,
        "l3_manifest": l3_manifest,
        "runtime_rows": runtime,
        "counterfactual_rows": counterfactual,
        "metric_rows": metric_rows,
        "classification": classification,
        "comparison": comparison,
    }


def load_a32_config(a32_dir: Path, a32_name: str) -> Any:
    harness = require_harness()
    grid_path = a32_dir / "a32_configs.parquet"
    if grid_path.exists():
        return harness.load_a32_config_from_grid(a32_dir, a32_name)
    selected_path = a32_dir / "selected_a32_config.json"
    if selected_path.exists():
        payload = read_json(selected_path)
        if payload.get("name") != a32_name:
            raise ValueError(f"selected A32 config is {payload.get('name')}, expected {a32_name}")
        return harness.A32Config(**payload)
    raise FileNotFoundError(
        f"expected {grid_path} or {selected_path} for A32 config loading"
    )


def compare_expected_metrics(
    config: A3ParityConfig,
    actual_metric_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if config.expected_v03_grid_dir is None:
        return {
            "enabled": False,
            "status": "skipped",
            "reason": "expected_v03_grid_dir_not_provided",
        }
    harness = require_harness()
    expected_path = first_existing_metrics_path(config.expected_v03_grid_dir)
    if expected_path is None:
        return {
            "enabled": False,
            "status": "skipped",
            "reason": f"no supported metrics parquet found in {config.expected_v03_grid_dir}",
        }
    expected_rows = read_expected_metric_rows(expected_path, harness)
    expected = [
        row for row in expected_rows
        if row.get("a31_config_name") == config.a31_name
        and row.get("a32_config_name") == config.a32_name
    ]
    if not expected:
        return {
            "enabled": True,
            "status": "failed",
            "reason": "expected metrics row not found",
        }
    actual_by_fold = metric_rows_by_fold(actual_metric_rows)
    expected_by_fold = metric_rows_by_fold(expected)
    actual_folds = set(actual_by_fold)
    expected_folds = set(expected_by_fold)
    missing_folds = sorted(expected_folds - actual_folds)
    unexpected_folds = sorted(actual_folds - expected_folds)
    duplicate_actual_folds = duplicate_metric_folds(actual_by_fold)
    duplicate_expected_folds = duplicate_metric_folds(expected_by_fold)
    mismatches: list[dict[str, Any]] = []
    for fold in missing_folds:
        mismatches.append({"fold": fold, "field": "<row>", "issue": "missing_actual"})
    for fold in unexpected_folds:
        mismatches.append({"fold": fold, "field": "<row>", "issue": "unexpected_actual"})
    for fold in duplicate_actual_folds:
        mismatches.append({"fold": fold, "field": "<row>", "issue": "duplicate_actual"})
    for fold in duplicate_expected_folds:
        mismatches.append({"fold": fold, "field": "<row>", "issue": "duplicate_expected"})
    non_comparable = set(
        missing_folds + unexpected_folds + duplicate_actual_folds + duplicate_expected_folds
    )
    for fold in sorted((actual_folds & expected_folds) - non_comparable):
        mismatches.extend(compare_rows(fold, actual_by_fold[fold][0], expected_by_fold[fold][0]))
    float_diffs = float_diff_summary(actual_metric_rows, expected)
    return {
        "enabled": True,
        "status": "passed" if not mismatches else "failed",
        "expected_metrics_path": str(expected_path),
        "actual_rows": len(actual_metric_rows),
        "expected_rows": len(expected),
        "actual_folds": sorted(actual_folds),
        "expected_folds": sorted(expected_folds),
        "missing_folds": missing_folds,
        "unexpected_folds": unexpected_folds,
        "duplicate_actual_folds": duplicate_actual_folds,
        "duplicate_expected_folds": duplicate_expected_folds,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:50],
        **float_diffs,
    }


def metric_rows_by_fold(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_fold: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_fold.setdefault(str(row["fold"]), []).append(row)
    return by_fold


def duplicate_metric_folds(rows_by_fold: dict[str, list[dict[str, Any]]]) -> list[str]:
    return sorted(fold for fold, rows in rows_by_fold.items() if len(rows) > 1)


def first_existing_metrics_path(base_dir: Path) -> Path | None:
    for name in (
        "a31_v03_grid_metrics.parquet",
        "a32_grid_metrics.parquet",
        "a31_grid_metrics.parquet",
    ):
        path = base_dir / name
        if path.exists():
            return path
    for name in (
        "expected/macro_metric_rows.json",
        "macro_metric_rows.json",
    ):
        path = base_dir / name
        if path.exists():
            return path
    return None


def read_expected_metric_rows(path: Path, harness: Any) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        payload = read_json(path)
        rows = payload.get("rows")
        if not isinstance(rows, list):
            raise ValueError(f"Expected metric rows JSON at {path} must contain a rows array")
        return rows
    return harness.read_parquet_records(path)


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


def metric_rows_logical_hash(rows: list[dict[str, Any]]) -> str:
    harness = require_harness()
    return harness.logical_records_hash(canonical_metric_rows(rows))


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
    harness = require_harness()
    return harness.logical_payload_hash({
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


def read_npz_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        import numpy as np
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - QC Research includes numpy
        raise RuntimeError("numpy and pandas are required to read QC NPZ panels") from exc
    with np.load(path, allow_pickle=False) as payload:
        columns = [str(column) for column in payload["_columns"].tolist()]
        if not columns:
            return []
        frame = pd.DataFrame({column: payload[column] for column in columns})
    for column in frame.columns:
        if pd.api.types.is_string_dtype(frame[column]):
            frame[column] = frame[column].replace("", pd.NA)
    frame = frame.where(pd.notna(frame), None)
    return [
        {str(key): npz_cell(value) for key, value in record.items()}
        for record in frame.to_dict("records")
    ]


def npz_cell(value: Any) -> Any:
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except (AttributeError, TypeError, ValueError):
            pass
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return value or None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def materialize_harness_source_from_manifest(
    manifest: dict[str, Any],
    object_store_path_resolver,
    *,
    project_root: Path | None = None,
) -> Path:
    source_files = manifest.get("source_files") or {}
    item = source_files.get("calibration_harness_source")
    if not item:
        return Path("src") / "calibration_harness.py"
    source_path = object_store_path_resolver(str(item["object_store_key"]))
    if source_path is None:
        source_path = Path(manifest.get("_local_bundle_dir", ".")) / str(item["relative_path"])
    source_path = Path(source_path)
    expected_sha = str(item["content_sha256"])
    actual_sha = file_sha256(source_path)
    if actual_sha != expected_sha:
        raise ValueError(
            f"calibration_harness source SHA mismatch: {actual_sha} != {expected_sha}"
        )
    root = project_root or Path.cwd()
    target = root / "src" / "calibration_harness.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(source_path, "rt", encoding="utf-8") as handle:
        target.write_text(handle.read(), encoding="utf-8")
    db_stub = root / "src" / "db.py"
    if not db_stub.exists():
        db_stub.write_text(
            '"""Offline DB stub for QC A3 parity."""\n'
            "def resolve_dsn(dsn=None):\n"
            "    raise RuntimeError('QC A3 parity is offline-only; database access is forbidden')\n\n"
            "def connect(dsn=None, *, autocommit=False):\n"
            "    raise RuntimeError('QC A3 parity is offline-only; database access is forbidden')\n",
            encoding="utf-8",
        )
    return target


def run_parity(config: A3ParityConfig) -> dict[str, Any]:
    started = dt.datetime.now(dt.UTC)
    result = compute_a3_case(config)
    finished = dt.datetime.now(dt.UTC)
    report = parity_report(config, result, started, finished)
    write_json(config.output_dir / "qc_a3_parity_report.json", report)
    return report


def export_bundle(config: A3ParityConfig) -> dict[str, Any]:
    started = dt.datetime.now(dt.UTC)
    result = compute_a3_case(config)
    output_dir = config.output_dir
    manifests_dir = output_dir / "manifests"
    expected_dir = output_dir / "expected"
    panels_dir = output_dir / "panels"
    code_dir = output_dir / "code"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    expected_dir.mkdir(parents=True, exist_ok=True)
    panels_dir.mkdir(parents=True, exist_ok=True)
    code_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy2(config.feature_manifest, manifests_dir / "feature_manifest.json")
    shutil.copy2(
        config.revision_uncertainty_manifest,
        manifests_dir / "revision_uncertainty_manifest.json",
    )
    revision_uncertainty_parquet = (
        config.revision_uncertainty_manifest.parent
        / "revision_uncertainty_primitives.parquet"
    )
    write_json(
        manifests_dir / "config_catalog.normalized.json",
        result["normalized_catalog"],
    )
    write_json(manifests_dir / "selected_a31_config.json", asdict(result["a31"]))
    write_json(manifests_dir / "selected_a32_config.json", asdict(result["a32"]))
    write_json(manifests_dir / "l3_manifest.json", result["l3_manifest"])

    write_csv_gzip(expected_dir / "macro_runtime_replay.csv.gz", result["runtime_rows"])
    write_csv_gzip(
        expected_dir / "macro_counterfactual_replay.csv.gz",
        result["counterfactual_rows"],
    )
    write_json(expected_dir / "macro_metric_rows.json", {"rows": result["metric_rows"]})

    l2_panel_path = panels_dir / "macro_l2_union_numeric.npz"
    uncertainty_panel_path = panels_dir / "revision_uncertainty_numeric.npz"
    export_numeric_panel_npz(Path(result["l2_path"]), l2_panel_path)
    export_numeric_panel_npz(revision_uncertainty_parquet, uncertainty_panel_path)
    write_gzip_text(
        Path("src") / "calibration_harness.py",
        code_dir / "calibration_harness.py.gz",
    )

    finished = dt.datetime.now(dt.UTC)
    object_store_manifest = object_store_manifest_payload(
        config,
        result,
        started=started,
        finished=finished,
        l2_panel_path=l2_panel_path,
        uncertainty_panel_path=uncertainty_panel_path,
    )
    manifest_path = output_dir / "object_store_manifest.json"
    write_json(manifest_path, object_store_manifest)

    parity = parity_report(config, result, started, finished)
    write_json(output_dir / "qc_a3_parity_report.json", parity)
    return {
        "status": "ok" if parity["comparison"]["status"] in {"passed", "skipped"} else "failed",
        "output_dir": str(output_dir),
        "object_store_manifest": str(manifest_path),
        "runtime_hash": object_store_manifest["expected"]["macro_runtime_replay_logical_hash"],
        "counterfactual_hash": object_store_manifest["expected"][
            "macro_counterfactual_replay_logical_hash"
        ],
        "metrics_hash": object_store_manifest["expected"]["macro_metric_rows_logical_hash"],
        "comparison": parity["comparison"],
    }


def write_csv_gzip(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with deterministic_gzip_text_writer(path) as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: csv_cell(row.get(key)) for key in fieldnames})


@contextmanager
def deterministic_gzip_text_writer(path: Path):
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="") as handle:
                yield handle


def csv_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return value.item()
        except (AttributeError, TypeError, ValueError):
            return value
    return value


def export_numeric_panel_npz(source_parquet: Path, target_npz: Path) -> None:
    try:
        import numpy as np
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - project requirements include both
        raise RuntimeError("numpy and pandas are required to export QC NPZ panels") from exc
    frame = pd.read_parquet(source_parquet)
    arrays: dict[str, Any] = {}
    for column in frame.columns:
        series = frame[column]
        if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series):
            arrays[column] = series.to_numpy()
        else:
            arrays[column] = series.astype("string").fillna("").to_numpy(dtype=str)
    arrays["_columns"] = np.array(sorted(arrays), dtype=str)
    target_npz.parent.mkdir(parents=True, exist_ok=True)
    write_npz_canonical(target_npz, arrays)


def write_npz_canonical(path: Path, arrays: dict[str, Any]) -> None:
    try:
        import numpy as np
    except ImportError as exc:  # pragma: no cover - project requirements include numpy
        raise RuntimeError("numpy is required to write QC NPZ panels") from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for name in sorted(arrays):
            buffer = io.BytesIO()
            np.lib.format.write_array(buffer, canonical_npz_array(np.asarray(arrays[name])), allow_pickle=False)
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o600 << 16
            zf.writestr(info, buffer.getvalue(), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)


def canonical_npz_array(array: Any) -> Any:
    try:
        import numpy as np
    except ImportError as exc:  # pragma: no cover - project requirements include numpy
        raise RuntimeError("numpy is required to canonicalize QC NPZ arrays") from exc
    arr = np.ascontiguousarray(array)
    if arr.dtype.kind in {"i", "u", "f", "c"} and arr.dtype.byteorder not in {"<", "|"}:
        arr = arr.astype(arr.dtype.newbyteorder("<"), copy=False)
    return arr


def write_gzip_text(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    text = source.read_text(encoding="utf-8")
    with deterministic_gzip_text_writer(target) as handle:
        handle.write(text)


def immutable_object_store_prefix(worker_commit: str | None, evaluation_hash: str) -> str:
    commit_part = (worker_commit or "unknown")[:7]
    return f"{OBJECT_STORE_BASE_PREFIX}/{commit_part}/{evaluation_hash}"


def store_key(relative_path: str, *, prefix: str | None = None) -> str:
    root = prefix or OBJECT_STORE_PREFIX
    return f"{root}/{relative_path}".replace("\\", "/")


def object_file_metadata(
    bundle_dir: Path,
    prefix: str,
    *,
    logical_hashes: dict[str, str],
) -> dict[str, dict[str, Any]]:
    files: dict[str, dict[str, Any]] = {}
    for name, relative_path in OBJECT_STORE_UPLOAD_FILE_KEYS.items():
        path = bundle_dir / relative_path
        if not path.exists():
            raise FileNotFoundError(path)
        files[name] = {
            "relative_path": relative_path,
            "object_store_key": store_key(relative_path, prefix=prefix),
            "file_size_bytes": path.stat().st_size,
            "content_sha256": file_sha256(path),
            "logical_hash": logical_hashes.get(relative_path),
        }
    return files


def source_file_metadata(bundle_dir: Path, prefix: str) -> dict[str, dict[str, Any]]:
    files: dict[str, dict[str, Any]] = {}
    for name, relative_path in OBJECT_STORE_SOURCE_FILE_KEYS.items():
        path = bundle_dir / relative_path
        if not path.exists():
            raise FileNotFoundError(path)
        files[name] = {
            "relative_path": relative_path,
            "object_store_key": store_key(relative_path, prefix=prefix),
            "file_size_bytes": path.stat().st_size,
            "content_sha256": file_sha256(path),
            "logical_hash": file_sha256(path),
        }
    return files


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def object_store_manifest_payload(
    config: A3ParityConfig,
    result: dict[str, Any],
    *,
    started: dt.datetime,
    finished: dt.datetime,
    l2_panel_path: Path,
    uncertainty_panel_path: Path,
) -> dict[str, Any]:
    harness = require_harness()
    runtime_hash = harness.logical_records_hash(result["runtime_rows"])
    counterfactual_hash = harness.logical_records_hash(result["counterfactual_rows"])
    metrics_policy = metrics_hash_policy_payload(result["metric_rows"], result["comparison"])
    metric_hash = metrics_policy["metrics_canonical_logical_hash"]
    worker_commit = config.worker_commit or current_git_commit()
    evaluation_hash = bundle_evaluation_hash(
        worker_commit=worker_commit,
        result=result,
        metrics_policy=metrics_policy,
    )
    immutable_prefix = immutable_object_store_prefix(worker_commit, evaluation_hash)
    bundle_dir = l2_panel_path.parents[1]
    object_files = object_file_metadata(
        bundle_dir,
        immutable_prefix,
        logical_hashes={
            "manifests/feature_manifest.json": harness.logical_payload_hash(
                read_json(bundle_dir / "manifests" / "feature_manifest.json")
            ),
            "manifests/revision_uncertainty_manifest.json": harness.logical_payload_hash(
                read_json(bundle_dir / "manifests" / "revision_uncertainty_manifest.json")
            ),
            "manifests/config_catalog.normalized.json": result["catalog_hash"],
            "manifests/selected_a31_config.json": result["a31_hash"],
            "manifests/selected_a32_config.json": result["a32_hash"],
            "manifests/l3_manifest.json": result["l3_manifest"]["logical_hash"],
            "panels/macro_l2_union_numeric.npz": result["l2_hash"],
            "panels/revision_uncertainty_numeric.npz": result["uncertainty_hash"],
            "expected/macro_runtime_replay.csv.gz": runtime_hash,
            "expected/macro_counterfactual_replay.csv.gz": counterfactual_hash,
            "expected/macro_metric_rows.json": metric_hash,
        },
    )
    source_files = source_file_metadata(bundle_dir, immutable_prefix)
    return {
        "schema_version": QC_A3_BRIDGE_SCHEMA_VERSION,
        "artifact_type": "qc_a3_parity_object_store_manifest",
        "execution_id": str(uuid.uuid4()),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "uploaded_at": None,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "worker_commit": worker_commit,
        "git_dirty": bool(current_git_dirty()),
        "qc_organization_id": qc_organization_id(),
        "bridge_scope": "qc_research_parity_only",
        "runtime_activation": False,
        "a3_status": "open_macro_v03",
        "a4_status": "harness_ready_provisional_A3",
        "a5_status": "blocked",
        "pit_source_contract": (
            "Investintell ALFRED/vintages and available_at remain source of truth; "
            "QC FRED/History must not feed the core A3 parity path."
        ),
        "forbidden_uses": [
            "QC FRED for core A3 vintage reconstruction",
            "return objective for A3 parameter selection",
            "runtime publication or activation",
            "A4 parameter selection before A3 scope decision",
        ],
        "evaluation_hash_policy_version": BUNDLE_EVALUATION_HASH_POLICY_VERSION,
        "evaluation_hash": evaluation_hash,
        "model_evaluation_hash": result["evaluation_hash"],
        "selected": {
            "a31_config_name": config.a31_name,
            "a31_config_hash": result["a31_hash"],
            "a32_config_name": config.a32_name,
            "a32_config_hash": result["a32_hash"],
            "model_evaluation_hash": result["evaluation_hash"],
            "bundle_evaluation_hash": evaluation_hash,
        },
        "parent_hashes": {
            "l2_macro_logical_hash": result["l2_hash"],
            "revision_uncertainty_logical_hash": result["uncertainty_hash"],
            "config_catalog_hash": result["catalog_hash"],
        },
        "object_store_base_prefix": OBJECT_STORE_BASE_PREFIX,
        "object_store_prefix": immutable_prefix,
        "object_store_prefix_immutable": immutable_prefix,
        "object_store_manifest_key": store_key("object_store_manifest.json", prefix=immutable_prefix),
        "bundle_size_bytes": sum(
            item["file_size_bytes"]
            for item in [*object_files.values(), *source_files.values()]
        ),
        "file_count": len(object_files) + len(source_files),
        "object_files": object_files,
        "source_files": source_files,
        "upload_policy": {
            "upload_only": ["npz", "json", "csv.gz", "py.gz"],
            "parquet_upload_allowed": False,
            "reason": (
                "first QC Research parity test avoids pyarrow and avoids "
                "duplicating Parquets; py.gz is deterministic harness source "
                "materialized in Research because QC source files have size limits"
            ),
        },
        "object_store_keys": {
            "feature_manifest": store_key("manifests/feature_manifest.json", prefix=immutable_prefix),
            "revision_uncertainty_manifest": store_key(
                "manifests/revision_uncertainty_manifest.json", prefix=immutable_prefix
            ),
            "config_catalog_normalized": store_key(
                "manifests/config_catalog.normalized.json", prefix=immutable_prefix
            ),
            "selected_a31_config": store_key("manifests/selected_a31_config.json", prefix=immutable_prefix),
            "selected_a32_config": store_key("manifests/selected_a32_config.json", prefix=immutable_prefix),
            "l3_manifest": store_key("manifests/l3_manifest.json", prefix=immutable_prefix),
            "macro_l2_union_numeric": store_key(
                "panels/macro_l2_union_numeric.npz", prefix=immutable_prefix
            ),
            "revision_uncertainty_numeric": store_key(
                "panels/revision_uncertainty_numeric.npz", prefix=immutable_prefix
            ),
            "expected_runtime_replay": store_key(
                "expected/macro_runtime_replay.csv.gz", prefix=immutable_prefix
            ),
            "expected_counterfactual_replay": store_key(
                "expected/macro_counterfactual_replay.csv.gz", prefix=immutable_prefix
            ),
            "expected_metric_rows": store_key(
                "expected/macro_metric_rows.json", prefix=immutable_prefix
            ),
            "calibration_harness_source": store_key(
                "code/calibration_harness.py.gz", prefix=immutable_prefix
            ),
        },
        "local_files": {
            name: item["relative_path"] for name, item in object_files.items()
        },
        "expected": {
            "macro_runtime_replay_logical_hash": runtime_hash,
            "macro_counterfactual_replay_logical_hash": counterfactual_hash,
            "macro_metric_rows_logical_hash": metric_hash,
            "macro_metric_rows_raw_sha256": metrics_policy["metrics_raw_sha256"],
            "macro_metric_rows_canonical_logical_hash": metric_hash,
            "metrics_hash_policy_version": metrics_policy["metrics_hash_policy_version"],
            "metric_row_count": len(result["metric_rows"]),
            "runtime_row_count": len(result["runtime_rows"]),
            "counterfactual_row_count": len(result["counterfactual_rows"]),
        },
        **metrics_policy,
        "comparison": result["comparison"],
        "qc_notes": {
            "research_node": "R8-16 CPU recommended for larger grids",
            "hmm_challenger": "diagnostic market challenger only; not A3 replacement",
            "backtest_use": "A4 and Book B only after A3 freeze/scope decision",
        },
    }


def parity_report(
    config: A3ParityConfig,
    result: dict[str, Any],
    started: dt.datetime,
    finished: dt.datetime,
) -> dict[str, Any]:
    harness = require_harness()
    metrics_policy = metrics_hash_policy_payload(result["metric_rows"], result["comparison"])
    worker_commit = config.worker_commit or current_git_commit()
    evaluation_hash = bundle_evaluation_hash(
        worker_commit=worker_commit,
        result=result,
        metrics_policy=metrics_policy,
    )
    return {
        "schema_version": QC_A3_BRIDGE_SCHEMA_VERSION,
        "artifact_type": "qc_a3_parity_report",
        "execution_id": str(uuid.uuid4()),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "worker_commit": worker_commit,
        "git_dirty": bool(current_git_dirty()),
        "a31_config_name": config.a31_name,
        "a31_config_hash": result["a31_hash"],
        "a32_config_name": config.a32_name,
        "a32_config_hash": result["a32_hash"],
        "evaluation_hash_policy_version": BUNDLE_EVALUATION_HASH_POLICY_VERSION,
        "evaluation_hash": evaluation_hash,
        "model_evaluation_hash": result["evaluation_hash"],
        "parent_hashes": {
            "l2_macro_logical_hash": result["l2_hash"],
            "revision_uncertainty_logical_hash": result["uncertainty_hash"],
            "config_catalog_hash": result["catalog_hash"],
        },
        "runtime_replay_logical_hash": harness.logical_records_hash(result["runtime_rows"]),
        "counterfactual_replay_logical_hash": harness.logical_records_hash(
            result["counterfactual_rows"]
        ),
        "metric_rows_logical_hash": metrics_policy["metrics_canonical_logical_hash"],
        "metrics_raw_sha256": metrics_policy["metrics_raw_sha256"],
        "metrics_canonical_logical_hash": metrics_policy["metrics_canonical_logical_hash"],
        "metrics_hash_policy_version": metrics_policy["metrics_hash_policy_version"],
        "float_abs_tolerance": metrics_policy["float_abs_tolerance"],
        "float_rel_tolerance": metrics_policy["float_rel_tolerance"],
        "max_abs_diff": metrics_policy["max_abs_diff"],
        "max_rel_diff": metrics_policy["max_rel_diff"],
        "differing_float_fields": metrics_policy["differing_float_fields"],
        "metric_row_count": len(result["metric_rows"]),
        "runtime_row_count": len(result["runtime_rows"]),
        "counterfactual_row_count": len(result["counterfactual_rows"]),
        "comparison": result["comparison"],
        "freeze_ready": False,
        "runtime_activation": False,
        "a4_status": "harness_ready_provisional_A3",
        "a5_status": "blocked",
    }


def current_git_commit() -> str | None:
    try:
        import subprocess

        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None


def current_git_dirty() -> str:
    try:
        import subprocess

        return subprocess.check_output(
            ["git", "status", "--porcelain"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return ""


def lean_text(args: list[str]) -> str:
    return subprocess.check_output(
        ["lean", *args],
        text=True,
        stderr=subprocess.STDOUT,
    ).strip()


def qc_organization_id() -> str | None:
    if os.environ.get("QC_ORGANIZATION_ID"):
        return os.environ["QC_ORGANIZATION_ID"]
    lean_config = Path("lean.json")
    if lean_config.exists():
        payload = read_json(lean_config)
        organization_id = payload.get("organization-id") or payload.get(
            "job-organization-id"
        )
        if organization_id:
            return str(organization_id)
    return None


def upload_object_store_bundle(bundle_dir: Path) -> dict[str, Any]:
    manifest_path = bundle_dir / "object_store_manifest.json"
    manifest = read_json(manifest_path)
    if manifest.get("runtime_activation") is not False:
        raise ValueError("refusing to upload a bundle with runtime_activation=true")
    if manifest.get("upload_policy", {}).get("parquet_upload_allowed") is not False:
        raise ValueError("refusing to upload unless parquet_upload_allowed=false")
    object_files = manifest.get("object_files") or {}
    if not object_files:
        raise ValueError("object_store_manifest.json has no object_files")
    source_files = manifest.get("source_files") or {}

    started = dt.datetime.now(dt.UTC)
    manifest["uploaded_at"] = started.isoformat()
    manifest["qc_organization_id"] = (
        manifest.get("qc_organization_id") or qc_organization_id()
    )
    try:
        manifest["qc_authenticated_user"] = lean_text(["whoami"])
    except Exception as exc:
        raise RuntimeError("lean whoami failed; run lean login before uploading") from exc
    write_json(manifest_path, manifest)

    uploads: list[dict[str, Any]] = []
    for name, item in sorted({**object_files, **source_files}.items()):
        relative_path = Path(str(item["relative_path"]))
        local_path = bundle_dir / relative_path
        key = str(item["object_store_key"])
        command_started = time.perf_counter()
        output = lean_text(["cloud", "object-store", "set", key, str(local_path)])
        uploads.append({
            "name": name,
            "object_store_key": key,
            "relative_path": str(relative_path).replace("\\", "/"),
            "file_size_bytes": local_path.stat().st_size,
            "content_sha256": file_sha256(local_path),
            "elapsed_seconds": time.perf_counter() - command_started,
            "lean_output": output,
        })

    manifest_key = str(manifest["object_store_manifest_key"])
    command_started = time.perf_counter()
    manifest_output = lean_text([
        "cloud",
        "object-store",
        "set",
        manifest_key,
        str(manifest_path),
    ])
    list_output = lean_text([
        "cloud",
        "object-store",
        "list",
        str(manifest["object_store_prefix_immutable"]),
    ])
    finished = dt.datetime.now(dt.UTC)
    report = {
        "schema_version": QC_A3_BRIDGE_SCHEMA_VERSION,
        "artifact_type": "qc_a3_object_store_upload_report",
        "status": "uploaded",
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_seconds": (finished - started).total_seconds(),
        "object_store_prefix_immutable": manifest["object_store_prefix_immutable"],
        "object_store_manifest_key": manifest_key,
        "object_count": len(uploads),
        "data_object_count": len(object_files),
        "source_object_count": len(source_files),
        "bundle_size_bytes": manifest["bundle_size_bytes"],
        "qc_organization_id": manifest.get("qc_organization_id"),
        "qc_authenticated_user": manifest.get("qc_authenticated_user"),
        "uploads": uploads,
        "manifest_upload": {
            "object_store_key": manifest_key,
            "file_size_bytes": manifest_path.stat().st_size,
            "content_sha256": file_sha256(manifest_path),
            "elapsed_seconds": time.perf_counter() - command_started,
            "lean_output": manifest_output,
        },
        "list_output": list_output,
    }
    results_dir = bundle_dir / "results"
    write_json(results_dir / "qc_object_store_upload_report.json", report)
    return report


def parse_args(argv: list[str]) -> tuple[str, A3ParityConfig | dict[str, Any]]:
    parser = argparse.ArgumentParser(description="QC Research parity bridge for A3")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("export-bundle", "run-parity"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--feature-manifest", required=True)
        cmd.add_argument("--revision-uncertainty-manifest", required=True)
        cmd.add_argument("--config-catalog", required=True)
        cmd.add_argument("--a32-grid-dir", required=True)
        cmd.add_argument("--output-dir", required=True)
        cmd.add_argument("--expected-v03-grid-dir")
        cmd.add_argument("--macro-l2-npz")
        cmd.add_argument("--revision-uncertainty-npz")
        cmd.add_argument("--a31-name", default=DEFAULT_A31_NAME)
        cmd.add_argument("--a32-name", default=DEFAULT_A32_NAME)
        cmd.add_argument("--worker-commit")
    upload = sub.add_parser("upload-object-store")
    upload.add_argument("--bundle-dir", required=True)
    args = parser.parse_args(argv)
    if args.command == "upload-object-store":
        return args.command, {"bundle_dir": Path(args.bundle_dir)}
    return args.command, A3ParityConfig(
        feature_manifest=Path(args.feature_manifest),
        revision_uncertainty_manifest=Path(args.revision_uncertainty_manifest),
        config_catalog=Path(args.config_catalog),
        a32_grid_dir=Path(args.a32_grid_dir),
        output_dir=Path(args.output_dir),
        expected_v03_grid_dir=(
            Path(args.expected_v03_grid_dir) if args.expected_v03_grid_dir else None
        ),
        macro_l2_npz=Path(args.macro_l2_npz) if args.macro_l2_npz else None,
        revision_uncertainty_npz=(
            Path(args.revision_uncertainty_npz)
            if args.revision_uncertainty_npz else None
        ),
        a31_name=args.a31_name,
        a32_name=args.a32_name,
        worker_commit=args.worker_commit,
    )


def main(argv: list[str] | None = None) -> int:
    command, config = parse_args(sys.argv[1:] if argv is None else argv)
    if command == "export-bundle":
        assert isinstance(config, A3ParityConfig)
        result = export_bundle(config)
    elif command == "run-parity":
        assert isinstance(config, A3ParityConfig)
        result = run_parity(config)
    elif command == "upload-object-store":
        assert isinstance(config, dict)
        result = upload_object_store_bundle(config["bundle_dir"])
    else:  # pragma: no cover
        raise ValueError(command)
    print(json.dumps(result, sort_keys=True))
    return 1 if command_result_failed(result) else 0


def command_result_failed(result: dict[str, Any]) -> bool:
    if result.get("status") == "failed":
        return True
    comparison = result.get("comparison")
    return isinstance(comparison, dict) and comparison.get("status") == "failed"


if __name__ == "__main__":
    raise SystemExit(main())
