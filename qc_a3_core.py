from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import json
import os
import shutil
import socket
import sys
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_A31_NAME = "G2-CREDIT6040-15-SURVEY05"
DEFAULT_A32_NAME = "A32-G0.35-I0.35-X0.10-C0.60-D1.25"
OBJECT_STORE_PREFIX = "investintell/a3/qc-a3-parity"
QC_A3_BRIDGE_SCHEMA_VERSION = 1
FLOAT_TOLERANCE = 1e-12


@dataclass(frozen=True)
class A3ParityConfig:
    feature_manifest: Path
    revision_uncertainty_manifest: Path
    config_catalog: Path
    a32_grid_dir: Path
    output_dir: Path
    expected_v03_grid_dir: Path | None = None
    a31_name: str = DEFAULT_A31_NAME
    a32_name: str = DEFAULT_A32_NAME
    worker_commit: str | None = None


def require_harness():
    try:
        from src import calibration_harness as harness
    except ImportError as exc:  # pragma: no cover - exercised in QC if project files are absent
        raise RuntimeError(
            "qc_a3_core.py must run with the Investintell worker project files "
            "available so it can import src.calibration_harness."
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


def compute_a3_case(config: A3ParityConfig) -> dict[str, Any]:
    harness = require_harness()
    feature_manifest, l2_path, l2_hash, l2_records = (
        harness.load_l2_macro_from_feature_manifest(config.feature_manifest)
    )
    uncertainty_manifest, uncertainty_hash, uncertainty_rows = (
        harness.load_revision_uncertainty_from_manifest(config.revision_uncertainty_manifest)
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
    expected_rows = harness.read_parquet_records(expected_path)
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
    actual_by_fold = {str(row["fold"]): row for row in actual_metric_rows}
    expected_by_fold = {str(row["fold"]): row for row in expected}
    mismatches: list[dict[str, Any]] = []
    for fold, expected_row in sorted(expected_by_fold.items()):
        actual_row = actual_by_fold.get(fold)
        if actual_row is None:
            mismatches.append({"fold": fold, "field": "<row>", "issue": "missing_actual"})
            continue
        mismatches.extend(compare_rows(fold, actual_row, expected_row))
    return {
        "enabled": True,
        "status": "passed" if not mismatches else "failed",
        "expected_metrics_path": str(expected_path),
        "actual_rows": len(actual_metric_rows),
        "expected_rows": len(expected),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:50],
    }


def first_existing_metrics_path(base_dir: Path) -> Path | None:
    for name in (
        "a31_v03_grid_metrics.parquet",
        "a32_grid_metrics.parquet",
        "a31_grid_metrics.parquet",
    ):
        path = base_dir / name
        if path.exists():
            return path
    return None


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
        if is_number(lhs) and is_number(rhs):
            if abs(float(lhs) - float(rhs)) > FLOAT_TOLERANCE:
                mismatches.append({
                    "fold": fold,
                    "field": key,
                    "actual": lhs,
                    "expected": rhs,
                    "abs_diff": abs(float(lhs) - float(rhs)),
                })
        elif normalize_scalar(lhs) != normalize_scalar(rhs):
            mismatches.append({
                "fold": fold,
                "field": key,
                "actual": normalize_scalar(lhs),
                "expected": normalize_scalar(rhs),
            })
    return mismatches


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def normalize_scalar(value: Any) -> Any:
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return value.item()
        except (AttributeError, TypeError, ValueError):
            return value
    return value


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
    manifests_dir.mkdir(parents=True, exist_ok=True)
    expected_dir.mkdir(parents=True, exist_ok=True)
    panels_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy2(config.feature_manifest, manifests_dir / "feature_manifest.json")
    shutil.copy2(result["l2_path"], manifests_dir / Path(result["l2_path"]).name)
    shutil.copy2(
        config.revision_uncertainty_manifest,
        manifests_dir / "revision_uncertainty_manifest.json",
    )
    revision_uncertainty_parquet = (
        config.revision_uncertainty_manifest.parent
        / "revision_uncertainty_primitives.parquet"
    )
    shutil.copy2(
        revision_uncertainty_parquet,
        manifests_dir / "revision_uncertainty_primitives.parquet",
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
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: csv_cell(row.get(key)) for key in fieldnames})


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
        elif column in {
            "business_date",
            "selection_mode",
            "selection_role",
            "series_id",
            "family_id",
            "axis_id",
            "entity_level",
            "entity_id",
        }:
            arrays[column] = series.astype("string").fillna("").to_numpy(dtype=str)
    arrays["_columns"] = np.array(sorted(arrays), dtype=str)
    target_npz.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(target_npz, **arrays)


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
    metric_hash = harness.logical_records_hash(result["metric_rows"])
    return {
        "schema_version": QC_A3_BRIDGE_SCHEMA_VERSION,
        "artifact_type": "qc_a3_parity_object_store_manifest",
        "execution_id": str(uuid.uuid4()),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "worker_commit": config.worker_commit or current_git_commit(),
        "git_dirty": bool(current_git_dirty()),
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
        "selected": {
            "a31_config_name": config.a31_name,
            "a31_config_hash": result["a31_hash"],
            "a32_config_name": config.a32_name,
            "a32_config_hash": result["a32_hash"],
            "evaluation_hash": result["evaluation_hash"],
        },
        "parent_hashes": {
            "l2_macro_logical_hash": result["l2_hash"],
            "revision_uncertainty_logical_hash": result["uncertainty_hash"],
            "config_catalog_hash": result["catalog_hash"],
        },
        "object_store_prefix": OBJECT_STORE_PREFIX,
        "object_store_keys": {
            "feature_manifest": store_key("manifests/feature_manifest.json"),
            "revision_uncertainty_manifest": store_key(
                "manifests/revision_uncertainty_manifest.json"
            ),
            "config_catalog_normalized": store_key(
                "manifests/config_catalog.normalized.json"
            ),
            "selected_a31_config": store_key("manifests/selected_a31_config.json"),
            "selected_a32_config": store_key("manifests/selected_a32_config.json"),
            "l3_manifest": store_key("manifests/l3_manifest.json"),
            "macro_l2_union_parquet": store_key(
                f"manifests/{Path(result['l2_path']).name}"
            ),
            "revision_uncertainty_parquet": store_key(
                "manifests/revision_uncertainty_primitives.parquet"
            ),
            "macro_l2_union_numeric": store_key("panels/macro_l2_union_numeric.npz"),
            "revision_uncertainty_numeric": store_key(
                "panels/revision_uncertainty_numeric.npz"
            ),
            "expected_runtime_replay": store_key("expected/macro_runtime_replay.csv.gz"),
            "expected_counterfactual_replay": store_key(
                "expected/macro_counterfactual_replay.csv.gz"
            ),
            "expected_metric_rows": store_key("expected/macro_metric_rows.json"),
        },
        "local_files": {
            "macro_l2_union_parquet": str(
                Path("manifests") / Path(result["l2_path"]).name
            ),
            "revision_uncertainty_parquet": str(
                Path("manifests") / "revision_uncertainty_primitives.parquet"
            ),
            "macro_l2_union_numeric": str(l2_panel_path),
            "revision_uncertainty_numeric": str(uncertainty_panel_path),
        },
        "expected": {
            "macro_runtime_replay_logical_hash": runtime_hash,
            "macro_counterfactual_replay_logical_hash": counterfactual_hash,
            "macro_metric_rows_logical_hash": metric_hash,
            "metric_row_count": len(result["metric_rows"]),
            "runtime_row_count": len(result["runtime_rows"]),
            "counterfactual_row_count": len(result["counterfactual_rows"]),
        },
        "comparison": result["comparison"],
        "qc_notes": {
            "research_node": "R8-16 CPU recommended for larger grids",
            "hmm_challenger": "diagnostic market challenger only; not A3 replacement",
            "backtest_use": "A4 and Book B only after A3 freeze/scope decision",
        },
    }


def store_key(relative_path: str) -> str:
    return f"{OBJECT_STORE_PREFIX}/{relative_path}".replace("\\", "/")


def parity_report(
    config: A3ParityConfig,
    result: dict[str, Any],
    started: dt.datetime,
    finished: dt.datetime,
) -> dict[str, Any]:
    harness = require_harness()
    return {
        "schema_version": QC_A3_BRIDGE_SCHEMA_VERSION,
        "artifact_type": "qc_a3_parity_report",
        "execution_id": str(uuid.uuid4()),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "worker_commit": config.worker_commit or current_git_commit(),
        "git_dirty": bool(current_git_dirty()),
        "a31_config_name": config.a31_name,
        "a31_config_hash": result["a31_hash"],
        "a32_config_name": config.a32_name,
        "a32_config_hash": result["a32_hash"],
        "evaluation_hash": result["evaluation_hash"],
        "parent_hashes": {
            "l2_macro_logical_hash": result["l2_hash"],
            "revision_uncertainty_logical_hash": result["uncertainty_hash"],
            "config_catalog_hash": result["catalog_hash"],
        },
        "runtime_replay_logical_hash": harness.logical_records_hash(result["runtime_rows"]),
        "counterfactual_replay_logical_hash": harness.logical_records_hash(
            result["counterfactual_rows"]
        ),
        "metric_rows_logical_hash": harness.logical_records_hash(result["metric_rows"]),
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


def parse_args(argv: list[str]) -> tuple[str, A3ParityConfig]:
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
        cmd.add_argument("--a31-name", default=DEFAULT_A31_NAME)
        cmd.add_argument("--a32-name", default=DEFAULT_A32_NAME)
        cmd.add_argument("--worker-commit")
    args = parser.parse_args(argv)
    return args.command, A3ParityConfig(
        feature_manifest=Path(args.feature_manifest),
        revision_uncertainty_manifest=Path(args.revision_uncertainty_manifest),
        config_catalog=Path(args.config_catalog),
        a32_grid_dir=Path(args.a32_grid_dir),
        output_dir=Path(args.output_dir),
        expected_v03_grid_dir=(
            Path(args.expected_v03_grid_dir) if args.expected_v03_grid_dir else None
        ),
        a31_name=args.a31_name,
        a32_name=args.a32_name,
        worker_commit=args.worker_commit,
    )


def main(argv: list[str] | None = None) -> int:
    command, config = parse_args(sys.argv[1:] if argv is None else argv)
    if command == "export-bundle":
        result = export_bundle(config)
    elif command == "run-parity":
        result = run_parity(config)
    else:  # pragma: no cover
        raise ValueError(command)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("status") != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
