"""Worker module for parallel microgrid execution.

Each worker loads L2/uncertainty data from local NPZ files (warm load)
and runs one A31 config with the fixed A32 config.
"""
from __future__ import annotations

import sys
import time
import tracemalloc
from pathlib import Path
from typing import Any


def run_config_worker(args: dict[str, Any]) -> dict[str, Any]:
    """Run one A31 config in a worker process.

    Loads data from local files (not Object Store) per the requirement:
    'Carregue o bundle do Object Store uma vez para armazenamento
    efêmero do node e faça os workers consumirem os arquivos locais.'
    """
    a31_name = args["a31_name"]
    a32_name = args["a32_name"]
    feature_manifest_path = Path(args["feature_manifest_path"])
    revision_uncertainty_manifest_path = Path(args["revision_uncertainty_manifest_path"])
    config_catalog_path = Path(args["config_catalog_path"])
    macro_l2_npz_path = Path(args["macro_l2_npz_path"])
    revision_uncertainty_npz_path = Path(args["revision_uncertainty_npz_path"])
    worker_commit = args["worker_commit"]
    project_root = Path(args.get("project_root", "."))

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from src import calibration_harness as harness
    from qc_a3_core import (
        A3ParityConfig,
        load_a32_config,
        load_l2_macro_for_config,
        load_revision_uncertainty_for_config,
        read_npz_records,
    )

    timings: dict[str, float] = {}
    tracemalloc.start()
    t0 = time.perf_counter()

    # Warm load from local disk
    t_warm = time.perf_counter()
    l2_records = read_npz_records(macro_l2_npz_path)
    uncertainty_rows = read_npz_records(revision_uncertainty_npz_path)
    timings["object_store_warm_load"] = time.perf_counter() - t_warm

    # Bundle decode
    t_decode = time.perf_counter()
    cfg = A3ParityConfig(
        feature_manifest=feature_manifest_path,
        revision_uncertainty_manifest=revision_uncertainty_manifest_path,
        config_catalog=config_catalog_path,
        a32_grid_dir=feature_manifest_path.parent,
        output_dir=Path("results"),
        macro_l2_npz=macro_l2_npz_path,
        revision_uncertainty_npz=revision_uncertainty_npz_path,
        a31_name=a31_name,
        a32_name=a32_name,
        worker_commit=worker_commit,
    )
    _, l2_hash, _ = load_l2_macro_for_config(cfg)
    _, uncertainty_hash, _ = load_revision_uncertainty_for_config(cfg)
    uncertainty_by_key = harness.revision_uncertainty_keyed(uncertainty_rows)
    timings["bundle_decode"] = time.perf_counter() - t_decode

    # Load catalog
    catalog_payload = harness.read_catalog_payload(config_catalog_path)
    normalized_catalog, _ = harness.normalize_a31_catalog(
        catalog_payload,
        l2_macro_logical_hash=l2_hash,
        source_path=config_catalog_path,
    )

    # Load A31 config
    a31_item = [
        item for item in normalized_catalog["configs"]
        if item["config"]["name"] == a31_name
    ][0]
    a31 = harness.A31Config(**a31_item["config"])
    a31_hash = str(a31_item["a31_config_hash"])

    # compute_l3
    t_l3 = time.perf_counter()
    l3_rows, contribution_rows, l3_manifest = harness.build_l3_score_panel(
        l2_records,
        a31,
        l2_macro_logical_hash=l2_hash,
        expected_l2_macro_logical_hash=l2_hash,
        revision_uncertainty_by_key=uncertainty_by_key,
        revision_uncertainty_logical_hash=uncertainty_hash,
    )
    timings["compute_l3"] = time.perf_counter() - t_l3

    # Load A32
    a32 = load_a32_config(feature_manifest_path.parent, a32_name)
    a32_hash = harness.a32_config_hash(a32)
    eval_hash = harness.evaluation_hash(a31_hash, a32_hash)

    # run_l4
    t_l4 = time.perf_counter()
    runtime, runtime_meta = harness.run_l4_state_machine(
        l3_rows, a32, selection_mode="latest"
    )
    counterfactual, cf_meta = harness.run_l4_state_machine(
        l3_rows, a32, selection_mode="first_release"
    )
    timings["run_l4"] = time.perf_counter() - t_l4

    # compute_metrics
    t_metrics = time.perf_counter()
    metrics_full = harness.build_macro_metrics(
        runtime, first_release_replay=counterfactual
    )
    classification = harness.classify_a32_grid_result(metrics_full)
    metric_rows = harness.evaluation_metric_rows(
        runtime, counterfactual, a31, a32,
        a31_hash, a32_hash, eval_hash, classification,
    )
    timings["compute_metrics"] = time.perf_counter() - t_metrics

    # Hashes
    runtime_hash = harness.logical_records_hash(runtime)
    counterfactual_hash = harness.logical_records_hash(counterfactual)

    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    timings["total"] = time.perf_counter() - t0

    metrics_by_fold = {row["fold"]: row for row in metric_rows}

    return {
        "a31_config_name": a31_name,
        "a31_config_hash": a31_hash,
        "a32_config_name": a32_name,
        "a32_config_hash": a32_hash,
        "evaluation_hash": eval_hash,
        "classification": classification,
        "runtime_row_count": len(runtime),
        "counterfactual_row_count": len(counterfactual),
        "metric_row_count": len(metric_rows),
        "runtime_replay_logical_hash": runtime_hash,
        "counterfactual_replay_logical_hash": counterfactual_hash,
        "metrics_by_fold": metrics_by_fold,
        "metrics_full": metrics_full,
        "timings": timings,
        "peak_memory_bytes": peak_bytes,
    }
