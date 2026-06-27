from __future__ import annotations

import argparse
import json
from pathlib import Path

from src import calibration_candidate as cc
from src.input_packs.hashing import canonical_json_sha256, file_sha256, load_json


ROOT = Path(__file__).resolve().parents[1]
GOLDEN_PACK = ROOT / "fixtures" / "input_packs" / "golden" / "certified_input_pack"


def _summary() -> dict[str, str]:
    manifest = load_json(GOLDEN_PACK / "manifest.json")
    return {
        "input_pack_sha256": manifest["input_pack_sha256"],
        "source_snapshot_sha256": canonical_json_sha256(
            {
                "raw_snapshot_sha256": manifest["raw_snapshot_sha256"],
                "canonical_snapshot_sha256": manifest["canonical_snapshot_sha256"],
            }
        ),
        "contract_bundle_sha256": manifest["contract_bundle_sha256"],
        "builder_commit": manifest["builder_commit"],
    }


def _args(output_dir: Path, *, jobs: int = 1) -> argparse.Namespace:
    summary = _summary()
    return argparse.Namespace(
        input_pack=str(GOLDEN_PACK),
        output_dir=str(output_dir),
        input_pack_sha256=summary["input_pack_sha256"],
        source_snapshot_sha256=summary["source_snapshot_sha256"],
        contract_bundle_sha256=summary["contract_bundle_sha256"],
        input_pack_p0_merge_commit="5" * 40,
        calibration_branch_base_commit="5" * 40,
        engine_commit="5" * 40,
        builder_commit=summary["builder_commit"],
        builder_code_sha256=None,
        engine_image_digest=None,
        engine_image_id="sha256:" + "1" * 64,
        docker_context_sha256="2" * 64,
        dockerfile_sha256="3" * 64,
        jobs=jobs,
        network="none",
        db_access=False,
        input_pack_mount="read_only",
        evidence_json=None,
    )


def test_calibration_candidate_generates_required_artifacts(tmp_path: Path) -> None:
    manifest = cc.run_calibration(_args(tmp_path))

    assert manifest["calibration_id"] == cc.CALIBRATION_ID
    assert manifest["runtime_activation"] is False
    assert manifest["A5"] == "blocked"
    assert manifest["freeze_ready"] is False
    assert manifest["status"] == "candidate"

    expected = {
        "calibration_manifest.json",
        "calibration_config.json",
        "parameter_grid.json",
        "selected_parameters.json",
        "rejected_candidates.json",
        "run_matrix.json",
        "output_manifest.json",
        "metrics_manifest.json",
        "invariant_report.json",
        "baseline_comparison.json",
        "reproducibility_report.json",
        "calibration_report.md",
        "logs/calibration.log",
    }
    assert expected.issubset({p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file()})

    selected = json.loads((tmp_path / "selected_parameters.json").read_text(encoding="utf-8"))
    rejected = json.loads((tmp_path / "rejected_candidates.json").read_text(encoding="utf-8"))
    invariant = json.loads((tmp_path / "invariant_report.json").read_text(encoding="utf-8"))
    assert selected["selected_candidate_id"] == "baseline_current"
    assert selected["final_approval_allowed"] is False
    assert rejected["rejected_count"] == 4
    assert invariant["ok"] is True
    assert invariant["checks"]["db_access"] is True
    assert invariant["checks"]["network_access"] is True


def test_calibration_candidate_is_jobs_invariant(tmp_path: Path) -> None:
    one = tmp_path / "jobs1"
    four = tmp_path / "jobs4"
    cc.run_calibration(_args(one, jobs=1))
    cc.run_calibration(_args(four, jobs=4))

    stable_files = [
        "selected_parameters.json",
        "rejected_candidates.json",
        "metrics_manifest.json",
        "invariant_report.json",
        "baseline_comparison.json",
        "reproducibility_report.json",
        "run_matrix.json",
    ]
    for rel in stable_files:
        assert file_sha256(one / rel) == file_sha256(four / rel)
