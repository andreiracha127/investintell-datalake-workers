"""Task 4 executor: measured observability round for the open_macro_v03 candidate.

Usage (from the repo root):

    python -m harness.dark_launch.measure_observability [--runs 8] [--skip-container]
    python -m harness.dark_launch.measure_observability --emit-records <raw_results.json>

Runs the candidate's REAL heavy compute — the ``a3_qc_parity`` quant-engine job (the
same G0 case ``scripts/repeatability_matrix.py`` pins, fixtures from the combo root) —
N times per leg on the host+container matrix of ``open_macro_v03_controlled_shadow_001``,
measuring each run in-process via ``measure_child.py`` (wall clock + OS peak memory of
the very process that computed). No prior run of this pipeline ever measured anything:
the shadow/handshake manifests carry fixed 1s/0-byte pins, which is exactly the gap
Task 4 closes.

Fail-loud: a failed job aborts the round (an error becomes a REAL nonzero error_rate
only if the operations_owner decides to accept it — the default is to stop and fix).
SLO derivation (stage plan Task 4): latency_slo = ceil(1.5 x measured p95),
memory_slo = ceil(1.5 x measured peak); error/retry SLOs are the operations_owner's
zero-tolerance decision. Governance: measurement only; A5 stays blocked.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DARK = ROOT / "artifacts" / "a5" / "open_macro_v03_dark_launch_001"
DEFAULT_COMBO_ROOT = "E:/investintell-datalake-workers-combo"
IMAGE = "investintell-quant-engine:local"
MEASURE_CHILD = ROOT / "harness" / "dark_launch" / "measure_child.py"
EXECUTION_DATE = "2026-07-03"
DEFAULT_RUNS_PER_LEG = 8


def _load_repeatability_module():
    spec = importlib.util.spec_from_file_location(
        "repeatability_matrix", ROOT / "scripts" / "repeatability_matrix.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# the code surfaces whose state determines what the measured compute actually ran
COMPUTE_TREES = ("harness/", "packages/", "services/", "scripts/", "qc_a3_core.py", "src/")


def _worker_commit() -> str:
    """HEAD, but ONLY from a clean compute tree — a dirty worktree would pin the
    measurement to a commit that did not contain the code that actually ran."""
    status = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT,
                            capture_output=True, text=True, check=True).stdout
    dirty = []
    for line in status.splitlines():
        # a rename/copy line carries BOTH paths ("R  old -> new"); a rename INTO a
        # compute tree must count as dirty, so test every path on the line.
        paths = line[3:].split(" -> ")
        if any(p.strip('"').replace("\\", "/").startswith(COMPUTE_TREES)
               for p in paths):
            dirty.append(line)
    if dirty:
        raise RuntimeError(
            "refusing to measure from a dirty compute tree (worker_commit would be "
            f"unreproducible): {dirty}")
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True,
                          text=True, check=True).stdout.strip()


def _compute_tree_hashes(commit: str) -> dict[str, str]:
    """Git tree/blob hashes of the compute surfaces at the measured commit — reachable
    provenance even if the measuring commit itself later falls out of ancestry."""
    hashes = {}
    for tree in COMPUTE_TREES:
        rel = tree.rstrip("/")
        obj = subprocess.run(["git", "rev-parse", f"{commit}:{rel}"], cwd=ROOT,
                             capture_output=True, text=True, check=True).stdout.strip()
        hashes[rel] = obj
    return hashes


def _image_id() -> str:
    """Immutable content id of the local engine image the container leg executes."""
    result = subprocess.run(["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"engine image {IMAGE} not present; build it first "
                           f"(docker build -f docker/quant-engine/Dockerfile -t {IMAGE} .)")
    return result.stdout.strip()


def _run_host(rm, out_dir: Path, worker_commit: str, combo_root: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = out_dir / "measure_metrics.json"
    args = rm._cli_args(rm.G0, input_root=str(combo_root).replace("\\", "/"),
                        output_dir=str(out_dir), jobs=1, worker_commit=worker_commit)
    sep = ";" if sys.platform == "win32" else ":"
    env_path = sep.join([str(ROOT),
                         str(ROOT / "packages" / "investintell_quant_core" / "src"),
                         str(ROOT / "services" / "quant_engine" / "src")])
    import os
    env = dict(os.environ, PYTHONPATH=env_path)
    subprocess.run([sys.executable, str(MEASURE_CHILD), str(metrics_path), *args],
                   check=True, env=env, capture_output=True, cwd=ROOT)
    return json.loads(metrics_path.read_text(encoding="utf-8"))


def _run_container(rm, out_dir: Path, worker_commit: str, combo_root: Path) -> dict[str, Any]:
    rm._prepare_container_output_dir(out_dir)
    args = rm._cli_args(rm.G0, input_root="/input/combo", output_dir="/outputs",
                        jobs=1, worker_commit=worker_commit)
    docker_base = rm._container_docker_base(combo_root=combo_root, out_dir=out_dir)
    harness_dir = (ROOT / "harness" / "dark_launch").resolve()
    subprocess.run(
        ["docker", "run", "--rm", *docker_base,
         "--mount", f"type=bind,src={harness_dir},dst=/input/measure,readonly",
         "--entrypoint", "python", IMAGE,
         "/input/measure/measure_child.py", "/outputs/measure_metrics.json", *args],
        check=True, capture_output=True)
    return json.loads((out_dir / "measure_metrics.json").read_text(encoding="utf-8"))


def _verify_job_result(out_dir: Path) -> dict[str, Any]:
    result = json.loads((out_dir / "job_result.json").read_text(encoding="utf-8"))
    if result["status"] != "succeeded" or result["classification"] != "passed":
        raise RuntimeError(f"{out_dir}: job did not succeed cleanly: "
                           f"{result['status']}/{result['classification']}")
    if result.get("errors"):
        raise RuntimeError(f"{out_dir}: job reported errors: {result['errors']}")
    if result.get("a5_status") != "blocked" or result.get("runtime_activation") is not False:
        raise RuntimeError(f"{out_dir}: governance fields unexpected")
    return {"run_fingerprint": result["run_fingerprint"],
            "output_logical_hashes": result["output_logical_hashes"]}


def measure(runs_per_leg: int, *, skip_container: bool, work_root: Path,
            combo_root: Path) -> dict[str, Any]:
    rm = _load_repeatability_module()
    worker_commit = _worker_commit()
    image_id = None if skip_container else _image_id()
    legs: dict[str, list[dict[str, Any]]] = {}
    fingerprints: dict[str, Any] = {}

    for leg, runner in (("host", _run_host), ("container", _run_container)):
        if leg == "container" and skip_container:
            continue
        samples = []
        for index in range(runs_per_leg):
            out_dir = work_root / f"{leg}-r{index}"
            metrics = runner(rm, out_dir, worker_commit, combo_root)
            if metrics["exit_code"] != 0:
                raise RuntimeError(f"{leg} run {index}: nonzero exit {metrics['exit_code']}")
            identity = _verify_job_result(out_dir)
            # determinism across the whole round: every run of every leg must produce
            # the IDENTICAL fingerprint/output hashes, or the measurement is void.
            if not fingerprints:
                fingerprints = identity
            elif identity != fingerprints:
                raise RuntimeError(f"{leg} run {index}: job identity diverged from the "
                                   f"round's fingerprint - non-deterministic compute")
            samples.append(metrics)
            print(f"{leg} run {index}: {metrics['wall_ms']:.0f} ms, "
                  f"{metrics['memory_peak_bytes'] / 1e6:.0f} MB peak")
        legs[leg] = samples

    return {
        "job_type": "a3_qc_parity",
        "case": "G0 (V03-G0-CONTROL, scripts/repeatability_matrix.py pins)",
        "jobs": 1,
        "worker_commit": worker_commit,
        "compute_tree_hashes": _compute_tree_hashes(worker_commit),
        "engine_image_id": image_id,
        "combo_root": str(combo_root).replace("\\", "/"),
        "job_identity": fingerprints,
        "runs_per_leg": runs_per_leg,
        "legs": legs,
    }


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[index]


def build_records(raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    legs = raw["legs"]
    per_leg = {}
    for leg, samples in legs.items():
        walls = [s["wall_ms"] for s in samples]
        peaks = [s["memory_peak_bytes"] for s in samples]
        per_leg[leg] = {
            "runs": len(samples),
            "platform": samples[0]["platform"],
            "wall_ms_all_runs": [round(w, 3) for w in walls],
            "latency_p95_ms": round(_p95(walls), 3),
            "memory_peak_bytes_all_runs": peaks,
            "memory_peak_bytes": max(peaks),
            "failed_runs": sum(1 for s in samples if s["exit_code"] != 0),
        }
    total_runs = sum(leg["runs"] for leg in per_leg.values())
    failed = sum(leg["failed_runs"] for leg in per_leg.values())
    latency_p95 = max(leg["latency_p95_ms"] for leg in per_leg.values())
    memory_peak = max(leg["memory_peak_bytes"] for leg in per_leg.values())

    metrics_record = {
        "artifact_type": "dark_launch_refreshed_observability_metrics",
        "schema_version": 1,
        "dark_launch_id": "open_macro_v03_dark_launch_001",
        "measured": True,
        "measurement_date": EXECUTION_DATE,
        "harness": "harness/dark_launch/measure_observability.py + measure_child.py "
                   "(in-process wall clock + OS peak memory of the computing process)",
        "measured_compute": {
            "job_type": raw["job_type"],
            "case": raw["case"],
            "jobs": raw["jobs"],
            "worker_commit": raw["worker_commit"],
            "compute_tree_hashes": raw["compute_tree_hashes"],
            "engine_image_id": raw["engine_image_id"],
            "combo_root": raw["combo_root"],
            "job_identity": raw["job_identity"],
            "determinism_note": "every run of every leg produced the identical "
                                "run_fingerprint and output logical hashes (enforced "
                                "fail-loud by the harness), so the thresholds certify "
                                "exactly one input bundle and one compute",
        },
        "environment_matrix": sorted(per_leg),
        "runs_total": total_runs,
        "per_leg": per_leg,
        "latency_p95_ms": latency_p95,
        "memory_peak_bytes": memory_peak,
        "error_rate": failed / total_runs,
        "retry_rate": 0.0,
        "retry_rate_note": "the harness performs no retries; every run is a first attempt "
                           "and all succeeded (a failed run aborts the round, fail-loud)",
        "prior_evidence_note": "no prior measurement existed: the controlled_shadow_001 / "
                               "handshake manifests carry fixed pins (duration_ms=1000, "
                               "memory_peak_bytes=0) from artifact-only validators",
        "runtime_activation": False,
        "activation_allowed": False,
    }

    def _slo(slo_id: str, metric: str, threshold, derivation: str) -> dict[str, Any]:
        return {"id": slo_id, "metric": metric, "threshold": threshold,
                "status": "defined", "derivation": derivation}

    latency_slo = int(math.ceil(1.5 * latency_p95))
    memory_slo = int(math.ceil(1.5 * memory_peak))
    thresholds_record = {
        "artifact_type": "dark_launch_monitoring_thresholds_record",
        "schema_version": 1,
        "dark_launch_id": "open_macro_v03_dark_launch_001",
        "decision_date": EXECUTION_DATE,
        "derived_from": "refreshed_observability_metrics.json (this directory)",
        "slos": [
            _slo("latency_slo", "candidate_compute_latency_p95_ms", latency_slo,
                 f"ceil(1.5 x measured p95 {latency_p95} ms) per the stage plan Task 4 "
                 f"rule; worst leg of the host+container matrix"),
            _slo("memory_slo", "candidate_compute_memory_peak_bytes", memory_slo,
                 f"ceil(1.5 x measured peak {memory_peak} bytes) per the stage plan "
                 f"Task 4 rule; worst leg of the host+container matrix"),
            _slo("error_rate_slo", "candidate_compute_error_rate", 0.0,
                 "operations_owner zero-tolerance decision: any job error alerts and "
                 "blocks; measured error_rate over the round was 0.0"),
            _slo("retry_rate_slo", "candidate_compute_retry_rate", 0.0,
                 "operations_owner zero-tolerance decision: the pipeline performs no "
                 "silent retries; any retry alerts; measured retry_rate was 0.0"),
        ],
        "zero_threshold_attempt_detectors_note": "db_write_attempt_alert / "
            "allocator_publish_attempt_alert / runtime_activation_attempt_alert / "
            "production_endpoint_activation_attempt_alert stay as defined in "
            "monitoring_enforcement_policy.json (not duplicated here, per the plan)",
        "runtime_activation": False,
        "activation_allowed": False,
    }
    return metrics_record, thresholds_record


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8", newline="\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS_PER_LEG)
    parser.add_argument("--combo-root", default=DEFAULT_COMBO_ROOT,
                        help="root holding the G0 fixture directories")
    parser.add_argument("--skip-container", action="store_true")
    parser.add_argument("--work-root", default=str(ROOT / "_tmp_observability_round"))
    parser.add_argument("--raw-out", default=str(ROOT / "_tmp_observability_round" / "raw_results.json"))
    parser.add_argument("--emit-records", metavar="RAW_JSON",
                        help="skip measuring; build the two records from a raw results file")
    args = parser.parse_args(argv)

    if args.emit_records:
        raw = json.loads(Path(args.emit_records).read_text(encoding="utf-8"))
    else:
        raw = measure(args.runs, skip_container=args.skip_container,
                      work_root=Path(args.work_root),
                      combo_root=Path(args.combo_root))
        raw_path = Path(args.raw_out)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        _write(raw_path, raw)
        print(f"raw results -> {raw_path}")

    metrics_record, thresholds_record = build_records(raw)
    _write(DARK / "refreshed_observability_metrics.json", metrics_record)
    _write(DARK / "monitoring_thresholds_record.json", thresholds_record)
    print(f"latency_p95_ms={metrics_record['latency_p95_ms']} "
          f"memory_peak_bytes={metrics_record['memory_peak_bytes']} "
          f"error_rate={metrics_record['error_rate']}")
    print("records written (measurement only; A5 blocked)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
