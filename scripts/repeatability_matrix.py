#!/usr/bin/env python3
"""Determinism repeatability matrix for the quant-engine.

Runs the same case across host vs container and jobs=1 vs jobs=4, repeated N
times, builds a canonical outputs manifest per run (volatile fields stripped),
and compares every run against a baseline with the closed comparator. Emits a
matrix report and exits non-zero on any divergence.

This realizes the report's determinism gate: "Comparacao bit a bit de outputs
entre jobs=1 e jobs=4" and "Matriz host versus container".

Example:
    python scripts/repeatability_matrix.py --with-container --repetitions 3 \
        --report _tmp_repeatability_matrix/report.json
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))
sys.path.insert(0, str(ROOT))

from investintell_quant_engine.outputs_manifest import build_outputs_manifest
from investintell_quant_engine.repeatability import compare_run_group

# G0 case: input paths are relative to the combo root, taken from the golden
# manifest's source_artifacts. They are joined to either the host combo root or
# the in-container mount point.
G0 = {
    "name": "G0",
    "a31_name": "V03-G0-CONTROL",
    "inputs": {
        "feature-manifest": "_tmp_qc_a3_parity_1138754_cloud_20260625/manifests/feature_manifest.json",
        "revision-uncertainty-manifest": "_tmp_qc_a3_parity_1138754_cloud_20260625/manifests/revision_uncertainty_manifest.json",
        "config-catalog": "_tmp_qc_a3_parity_1138754_cloud_20260625/manifests/config_catalog.normalized.json",
        "a32-grid-dir": "_tmp_a32_grid_selected_4827ce4_20260625",
        "expected-v03-grid-dir": "_tmp_a31_v03_revision_robust_g1_e6a72c3_20260625",
        "macro-l2-npz": "_tmp_qc_a3_parity_1138754_cloud_20260625/panels/macro_l2_union_numeric.npz",
        "revision-uncertainty-npz": "_tmp_qc_a3_parity_1138754_cloud_20260625/panels/revision_uncertainty_numeric.npz",
    },
}


def _cli_args(case, *, input_root: str, output_dir: str, jobs: int, worker_commit: str) -> list[str]:
    args: list[str] = ["run-parity"]
    for flag, rel in case["inputs"].items():
        args += [f"--{flag}", f"{input_root}/{rel}"]
    args += [
        "--a31-name", case["a31_name"],
        "--output-dir", output_dir,
        "--jobs", str(jobs),
        # Provenance is injected by the dispatcher, not read from ambient git.
        # The container has no .git, so relying on current_git_commit() would
        # make the bundle evaluation_hash environment-dependent (Finding F1b).
        "--worker-commit", worker_commit,
        "--result-json", f"{output_dir}/job_result.json",
        "--manifest-json", f"{output_dir}/engine_manifest.json",
    ]
    return args


def _run_host(case, *, combo_root: Path, out_dir: Path, jobs: int, worker_commit: str) -> None:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    env_path = (
        f"{ROOT}{';' if sys.platform == 'win32' else ':'}"
        f"{ROOT / 'packages' / 'investintell_quant_core' / 'src'}"
        f"{';' if sys.platform == 'win32' else ':'}"
        f"{ROOT / 'services' / 'quant_engine' / 'src'}"
    )
    import os

    env = dict(os.environ, PYTHONPATH=env_path)
    args = _cli_args(
        case, input_root=str(combo_root), output_dir=str(out_dir), jobs=jobs,
        worker_commit=worker_commit,
    )
    subprocess.run(
        [sys.executable, "-m", "investintell_quant_engine.cli", *args],
        check=True,
        env=env,
        capture_output=True,
    )


def _run_container(case, *, combo_root: Path, out_dir: Path, jobs: int, image: str, worker_commit: str) -> None:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    args = _cli_args(
        case, input_root="/input/combo", output_dir="/outputs", jobs=jobs,
        worker_commit=worker_commit,
    )
    docker_cmd = [
        "docker", "run", "--rm", "--network", "none",
        "--mount", f"type=bind,src={combo_root},dst=/input/combo,readonly",
        "--mount", f"type=bind,src={out_dir},dst=/outputs",
        image, *args,
    ]
    subprocess.run(docker_cmd, check=True, capture_output=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--combo-root", default=r"E:\investintell-datalake-workers-combo")
    parser.add_argument("--workdir", default=str(ROOT / "_tmp_repeatability_matrix"))
    parser.add_argument("--image", default="investintell-quant-engine:local")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--jobs", type=int, nargs="+", default=[1, 4])
    parser.add_argument(
        "--environments",
        nargs="+",
        choices=["host", "container"],
        default=["host", "container"],
        help="Which execution environments to run. Defaults to the full "
        "host-vs-container gate; pass e.g. '--environments host' to opt down.",
    )
    parser.add_argument("--min-runs", type=int, default=4)
    parser.add_argument(
        "--worker-commit",
        default=None,
        help="Provenance commit injected into every run (defaults to the worktree HEAD).",
    )
    parser.add_argument("--report", default=str(ROOT / "_tmp_repeatability_matrix" / "report.json"))
    args = parser.parse_args(argv)

    combo_root = Path(args.combo_root)
    workdir = Path(args.workdir)
    worker_commit = args.worker_commit or subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    print(f"[provenance] worker_commit={worker_commit}")
    runs: list[tuple[str, dict]] = []

    runners = {"host": _run_host, "container": _run_container}
    plan = [(env, runners[env]) for env in args.environments]

    for env_name, runner in plan:
        for jobs in args.jobs:
            for rep in range(args.repetitions):
                label = f"{env_name}-jobs{jobs}-r{rep}"
                out_dir = workdir / "runs" / label
                kwargs = dict(
                    combo_root=combo_root, out_dir=out_dir, jobs=jobs,
                    worker_commit=worker_commit,
                )
                if env_name == "container":
                    kwargs["image"] = args.image
                runner(G0, **kwargs)
                manifest = build_outputs_manifest(out_dir, status="succeeded", canonical=True)
                runs.append((label, manifest))
                print(f"[run] {label}: {len(manifest['artifacts'])} artifacts")

    verdict = compare_run_group(runs, min_runs=args.min_runs)
    report = {
        "case": G0["name"],
        "labels": [label for label, _ in runs],
        "verdict": verdict,
    }
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(
        f"\nmismatch_count={verdict['mismatch_count']} "
        f"run_count={verdict['run_count']} "
        f"sufficient={verdict['sufficient']} ok={verdict['ok']}"
    )
    if verdict["divergent"]:
        print("DIVERGENT:", verdict["divergent"])
    return 0 if verdict["ok"] and verdict["sufficient"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
