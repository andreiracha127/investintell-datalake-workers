"""Bundle argument adapters for quant-engine runners."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from ._paths import ensure_repo_paths

ensure_repo_paths()

import qc_a3_core as qc


def parity_config_from_args(args: Namespace) -> qc.A3ParityConfig:
    return qc.A3ParityConfig(
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
            if args.revision_uncertainty_npz
            else None
        ),
        a31_name=args.a31_name,
        a32_name=args.a32_name,
        worker_commit=args.worker_commit,
    )

