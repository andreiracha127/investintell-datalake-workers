"""Command line interface for the offline quant engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .bundle_io import parity_config_from_args
from .manifests import engine_manifest
from .outputs_manifest import build_outputs_manifest
from .runners.parity import run_parity_job


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Investintell offline quant engine")
    sub = parser.add_subparsers(dest="command", required=True)
    parity = sub.add_parser("run-parity")
    parity.add_argument("--feature-manifest", required=True)
    parity.add_argument("--revision-uncertainty-manifest", required=True)
    parity.add_argument("--config-catalog", required=True)
    parity.add_argument("--a32-grid-dir", required=True)
    parity.add_argument("--output-dir", required=True)
    parity.add_argument("--expected-v03-grid-dir")
    parity.add_argument("--macro-l2-npz")
    parity.add_argument("--revision-uncertainty-npz")
    parity.add_argument("--a31-name", default="G2-CREDIT6040-15-SURVEY05")
    parity.add_argument("--a32-name", default="A32-G0.35-I0.35-X0.10-C0.60-D1.25")
    parity.add_argument("--worker-commit")
    parity.add_argument("--job-id")
    parity.add_argument("--jobs", type=int, default=1)
    parity.add_argument("--result-json")
    parity.add_argument("--manifest-json")
    parity.add_argument(
        "--outputs-manifest",
        help="Write a closed manifest of every artifact in --output-dir to this path.",
    )
    parity.add_argument(
        "--outputs-manifest-canonical",
        action="store_true",
        help="Strip volatile fields (ids, timestamps, env) before hashing artifacts.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    if args.command != "run-parity":  # pragma: no cover
        parser.error(f"unsupported command {args.command}")
    config = parity_config_from_args(args)
    result = run_parity_job(
        config,
        job_id=args.job_id,
        jobs=args.jobs,
        offline=True,
    )
    manifest = engine_manifest(job_type="a3_qc_parity", jobs=args.jobs, offline=True)
    if args.result_json:
        write_json(Path(args.result_json), result)
    if args.manifest_json:
        write_json(Path(args.manifest_json), manifest)
    if args.outputs_manifest:
        # Exclude the manifest target from the walk so neither the manifest being
        # written nor a stale copy from a previous run (when the target lives
        # inside --output-dir) is folded in. Either would break repeatability.
        outputs = build_outputs_manifest(
            config.output_dir,
            status=result.get("status", "failed"),
            canonical=args.outputs_manifest_canonical,
            exclude=[Path(args.outputs_manifest)],
        )
        write_json(Path(args.outputs_manifest), outputs)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("status") == "succeeded" else 1


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())

