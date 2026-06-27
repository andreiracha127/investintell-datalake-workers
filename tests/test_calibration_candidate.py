from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

from src import calibration_candidate as cc
from src.input_packs.hashing import canonical_json_sha256, file_sha256, load_json


ROOT = Path(__file__).resolve().parents[1]
GOLDEN_PACK = ROOT / "fixtures" / "input_packs" / "golden" / "certified_input_pack"


def _engine_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return "5" * 40


def _dockerfile_sha256(engine_commit: str) -> str:
    try:
        payload = subprocess.check_output(["git", "show", f"{engine_commit}:docker/quant-engine/Dockerfile"], cwd=ROOT)
        return hashlib.sha256(payload).hexdigest()
    except (OSError, subprocess.CalledProcessError):
        return "3" * 64


def _docker_context_sha256(engine_commit: str) -> str:
    try:
        return cc.committed_docker_context_sha256(engine_commit)
    except (OSError, subprocess.CalledProcessError, ValueError):
        return "2" * 64


def _git_identity_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("GIT_AUTHOR_NAME", "Investintell CI")
    env.setdefault("GIT_AUTHOR_EMAIL", "ci@investintell.local")
    env.setdefault("GIT_COMMITTER_NAME", "Investintell CI")
    env.setdefault("GIT_COMMITTER_EMAIL", "ci@investintell.local")
    return env


def _commit_tree(treeish: str, message: str) -> str:
    return subprocess.check_output(
        ["git", "commit-tree", treeish, "-m", message],
        cwd=ROOT,
        env=_git_identity_env(),
        text=True,
    ).strip()


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


def _commit_contains_pack(commit: str, summary: dict[str, str]) -> bool:
    try:
        payload = subprocess.check_output(
            ["git", "show", f"{commit}:fixtures/input_packs/golden/certified_input_pack/manifest.json"],
            cwd=ROOT,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return False
    return json.loads(payload).get("input_pack_sha256") == summary["input_pack_sha256"]


def _input_pack_p0_merge_commit(summary: dict[str, str]) -> str:
    for ref in ("origin/main", "main", "HEAD", summary["builder_commit"]):
        try:
            commit = subprocess.check_output(["git", "rev-parse", ref], cwd=ROOT, text=True).strip()
        except (OSError, subprocess.CalledProcessError):
            continue
        if _commit_contains_pack(commit, summary):
            return commit
    return summary["builder_commit"]


def _args(output_dir: Path, *, jobs: int = 1, evidence_json: str | None = None) -> argparse.Namespace:
    summary = _summary()
    engine_commit = _engine_commit()
    input_pack_p0_merge_commit = _input_pack_p0_merge_commit(summary)
    return argparse.Namespace(
        input_pack=str(GOLDEN_PACK),
        output_dir=str(output_dir),
        input_pack_sha256=summary["input_pack_sha256"],
        source_snapshot_sha256=summary["source_snapshot_sha256"],
        contract_bundle_sha256=summary["contract_bundle_sha256"],
        input_pack_p0_merge_commit=input_pack_p0_merge_commit,
        calibration_branch_base_commit=input_pack_p0_merge_commit,
        engine_commit=engine_commit,
        builder_commit=summary["builder_commit"],
        builder_code_sha256=None,
        engine_image_digest=None,
        engine_image_id="sha256:" + "1" * 64,
        docker_context_sha256=_docker_context_sha256(engine_commit),
        dockerfile_sha256=_dockerfile_sha256(engine_commit),
        jobs=jobs,
        network="none",
        db_access=False,
        input_pack_mount="read_only",
        evidence_json=evidence_json,
    )


def _write_evidence(
    path: Path,
    hashes: dict[str, str],
    *,
    labels: list[str] | None = None,
    include_isolation: bool = True,
    docker_image_id: str | None = "sha256:" + "1" * 64,
    path_independence: bool | None = True,
) -> Path:
    labels = labels or sorted(cc.REQUIRED_MATRIX_LABELS)
    payload = {
        "schema_version": 1,
        "calibration_id": cc.CALIBRATION_ID,
        "base_label": "host_jobs1_r0",
        "labels": labels,
        "comparisons": {
            f"host_jobs1_r0_vs_{label}": {"ok": True, "mismatched": [], "hashes": hashes} for label in labels
        },
        "run_count": len(labels),
        "mismatch_count": 0,
        "ok": True,
    }
    if include_isolation:
        payload.update({"network": "none", "db_access": False, "input_pack_mount": "read_only"})
    if docker_image_id is not None:
        payload["docker_image_id"] = docker_image_id
    if path_independence is not None:
        payload["path_independence"] = path_independence
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


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


def test_run_matrix_requires_external_evidence(tmp_path: Path) -> None:
    cc.run_calibration(_args(tmp_path))

    run_matrix = json.loads((tmp_path / "run_matrix.json").read_text(encoding="utf-8"))
    reproducibility = json.loads((tmp_path / "reproducibility_report.json").read_text(encoding="utf-8"))
    assert run_matrix["evidence_required"] is True
    assert run_matrix["comparison_evidence"] is None
    assert run_matrix["hashes"] == {}
    assert run_matrix["ok"] is False
    assert reproducibility["evidence_ok"] is False


def test_run_matrix_accepts_independent_evidence(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    cc.run_calibration(_args(probe))
    probe_matrix = json.loads((probe / "run_matrix.json").read_text(encoding="utf-8"))
    evidence = _write_evidence(tmp_path / "matrix_evidence.json", probe_matrix["current_run_hashes"])

    cc.run_calibration(_args(tmp_path / "out", evidence_json=str(evidence)))

    run_matrix = json.loads((tmp_path / "out" / "run_matrix.json").read_text(encoding="utf-8"))
    reproducibility = json.loads((tmp_path / "out" / "reproducibility_report.json").read_text(encoding="utf-8"))
    manifest = json.loads((tmp_path / "out" / "calibration_manifest.json").read_text(encoding="utf-8"))
    assert run_matrix["ok"] is True
    assert set(run_matrix["hashes"]) == cc.REQUIRED_MATRIX_LABELS
    assert set(reproducibility["jobs_1_hashes"]) == {
        "host_jobs1_r0",
        "host_jobs1_r1",
        "container_jobs1_r0",
        "container_jobs1_r1",
    }
    assert set(reproducibility["jobs_4_hashes"]) == {
        "host_jobs4_r0",
        "host_jobs4_r1",
        "container_jobs4_r0",
        "container_jobs4_r1",
    }
    assert reproducibility["evidence_ok"] is True
    assert manifest["run_matrix_sha256"] == file_sha256(tmp_path / "out" / "run_matrix.json")
    assert manifest["reproducibility_report_sha256"] == file_sha256(tmp_path / "out" / "reproducibility_report.json")


def test_run_matrix_rejects_stale_evidence_hashes(tmp_path: Path) -> None:
    stale_hashes = {
        "selected_parameters_sha256": "a" * 64,
        "rejected_candidates_sha256": "b" * 64,
        "metrics_manifest_sha256": "c" * 64,
        "invariant_report_sha256": "d" * 64,
        "baseline_comparison_sha256": "e" * 64,
        "output_manifest_sha256": "f" * 64,
    }
    evidence = _write_evidence(tmp_path / "stale_evidence.json", stale_hashes)

    cc.run_calibration(_args(tmp_path / "out", evidence_json=str(evidence)))

    run_matrix = json.loads((tmp_path / "out" / "run_matrix.json").read_text(encoding="utf-8"))
    reproducibility = json.loads((tmp_path / "out" / "reproducibility_report.json").read_text(encoding="utf-8"))
    assert run_matrix["ok"] is False
    assert run_matrix["hashes"] == {}
    assert reproducibility["evidence_ok"] is False


def test_run_matrix_rejects_missing_jobs_coverage(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    cc.run_calibration(_args(probe))
    probe_matrix = json.loads((probe / "run_matrix.json").read_text(encoding="utf-8"))
    evidence = _write_evidence(
        tmp_path / "jobs1_only_evidence.json",
        probe_matrix["current_run_hashes"],
        labels=["host_jobs1_r0", "host_jobs1_r1", "container_jobs1_r0", "container_jobs1_r1"],
    )

    cc.run_calibration(_args(tmp_path / "out", evidence_json=str(evidence)))

    run_matrix = json.loads((tmp_path / "out" / "run_matrix.json").read_text(encoding="utf-8"))
    reproducibility = json.loads((tmp_path / "out" / "reproducibility_report.json").read_text(encoding="utf-8"))
    assert run_matrix["ok"] is False
    assert run_matrix["hashes"] == {}
    assert reproducibility["jobs_4_hashes"] == {}
    assert reproducibility["evidence_ok"] is False


def test_run_matrix_rejects_missing_isolation_metadata(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    cc.run_calibration(_args(probe))
    probe_matrix = json.loads((probe / "run_matrix.json").read_text(encoding="utf-8"))
    evidence = _write_evidence(
        tmp_path / "legacy_evidence.json",
        probe_matrix["current_run_hashes"],
        include_isolation=False,
    )

    cc.run_calibration(_args(tmp_path / "out", evidence_json=str(evidence)))

    run_matrix = json.loads((tmp_path / "out" / "run_matrix.json").read_text(encoding="utf-8"))
    reproducibility = json.loads((tmp_path / "out" / "reproducibility_report.json").read_text(encoding="utf-8"))
    assert run_matrix["ok"] is False
    assert run_matrix["hashes"] == {}
    assert reproducibility["evidence_ok"] is False


def test_run_matrix_rejects_different_image_id(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    cc.run_calibration(_args(probe))
    probe_matrix = json.loads((probe / "run_matrix.json").read_text(encoding="utf-8"))
    evidence = _write_evidence(
        tmp_path / "wrong_image_evidence.json",
        probe_matrix["current_run_hashes"],
        docker_image_id="sha256:" + "9" * 64,
    )

    cc.run_calibration(_args(tmp_path / "out", evidence_json=str(evidence)))

    run_matrix = json.loads((tmp_path / "out" / "run_matrix.json").read_text(encoding="utf-8"))
    reproducibility = json.loads((tmp_path / "out" / "reproducibility_report.json").read_text(encoding="utf-8"))
    assert run_matrix["ok"] is False
    assert run_matrix["hashes"] == {}
    assert reproducibility["evidence_ok"] is False


def test_run_matrix_requires_path_independence_evidence(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    cc.run_calibration(_args(probe))
    probe_matrix = json.loads((probe / "run_matrix.json").read_text(encoding="utf-8"))
    evidence = _write_evidence(
        tmp_path / "path_dependent_evidence.json",
        probe_matrix["current_run_hashes"],
        path_independence=False,
    )

    cc.run_calibration(_args(tmp_path / "out", evidence_json=str(evidence)))

    run_matrix = json.loads((tmp_path / "out" / "run_matrix.json").read_text(encoding="utf-8"))
    reproducibility = json.loads((tmp_path / "out" / "reproducibility_report.json").read_text(encoding="utf-8"))
    assert run_matrix["ok"] is False
    assert reproducibility["path_independence"] is False
    assert reproducibility["evidence_ok"] is False


def test_invariant_ok_tracks_failed_checks(tmp_path: Path) -> None:
    report = cc.build_invariant_report(
        output_dir=tmp_path,
        generated_files=["missing.json"],
        config={"constraints": {"institutional_limits": {"status": "explicitly_unset"}}},
        candidate_rows=[{"candidate_id": "x", "weights_sum": 1.0, "objective_value": 0.0}],
        network="bridge",
        db_access=True,
        input_pack_mount="read_write",
    )

    assert report["ok"] is False
    assert report["checks"]["outputs_complete"] is False
    assert report["checks"]["db_access"] is False
    assert report["checks"]["network_access"] is False
    assert report["checks"]["input_pack_read_only"] is False


def test_run_calibration_enforces_runtime_guards_for_direct_calls(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.db_access = True
    with pytest.raises(ValueError, match="db_access must remain false"):
        cc.run_calibration(args)

    args = _args(tmp_path)
    args.network = "bridge"
    with pytest.raises(ValueError, match="network must be none"):
        cc.run_calibration(args)

    args = _args(tmp_path)
    args.input_pack_mount = "read_write"
    with pytest.raises(ValueError, match="input pack mount must be read_only"):
        cc.run_calibration(args)


def test_run_calibration_rejects_output_inside_input_pack() -> None:
    args = _args(GOLDEN_PACK)
    with pytest.raises(ValueError, match="output_dir must not be inside the certified input pack"):
        cc.run_calibration(args)

    args = _args(GOLDEN_PACK / "calibration-output")
    with pytest.raises(ValueError, match="output_dir must not be inside the certified input pack"):
        cc.run_calibration(args)


def test_run_calibration_rejects_symlinked_output(tmp_path: Path) -> None:
    outside = tmp_path / "outside.json"
    outside.write_text("outside\n", encoding="utf-8")
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    try:
        os.symlink(outside, output_dir / "selected_parameters.json")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with pytest.raises(ValueError, match="symlinked output path"):
        cc.run_calibration(_args(output_dir))
    assert outside.read_text(encoding="utf-8") == "outside\n"


def test_builder_commit_override_must_match_verified_pack(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.builder_commit = "9" * 40

    with pytest.raises(ValueError, match="builder_commit mismatch"):
        cc.run_calibration(args)


def test_input_pack_merge_commit_must_match_verified_pack(tmp_path: Path) -> None:
    if not (ROOT / ".git").exists():
        pytest.skip("git checkout metadata unavailable")
    empty_tree = subprocess.check_output(["git", "mktree"], cwd=ROOT, input="", text=True).strip()
    empty_commit = _commit_tree(empty_tree, "empty stale input pack commit")
    args = _args(tmp_path)
    args.input_pack_p0_merge_commit = empty_commit

    with pytest.raises(ValueError, match="does not contain the certified pack manifest"):
        cc.run_calibration(args)


def test_calibration_branch_base_commit_must_be_checkoutable(tmp_path: Path) -> None:
    if not (ROOT / ".git").exists():
        pytest.skip("git checkout metadata unavailable")
    args = _args(tmp_path)
    args.calibration_branch_base_commit = "9" * 40

    with pytest.raises(ValueError, match="calibration_branch_base_commit is not a checkoutable commit"):
        cc.run_calibration(args)


def test_engine_commit_is_required(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.engine_commit = None

    with pytest.raises(ValueError, match="engine_commit must be provided explicitly"):
        cc.run_calibration(args)


def test_engine_commit_must_be_well_formed(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.engine_commit = "not-a-commit"

    with pytest.raises(ValueError, match="40-character git commit SHA"):
        cc.run_calibration(args)


def test_engine_commit_must_exist_when_git_checkout_available(tmp_path: Path) -> None:
    if not (ROOT / ".git").exists():
        pytest.skip("git checkout metadata unavailable")
    args = _args(tmp_path)
    args.engine_commit = "9" * 40

    with pytest.raises(ValueError, match="not a checkoutable commit"):
        cc.run_calibration(args)


def test_engine_commit_must_be_reachable_from_current_head(tmp_path: Path) -> None:
    if not (ROOT / ".git").exists():
        pytest.skip("git checkout metadata unavailable")
    commit = _commit_tree("HEAD^{tree}", "unreachable calibration engine")
    args = _args(tmp_path)
    args.engine_commit = commit
    args.docker_context_sha256 = _docker_context_sha256(commit)
    args.dockerfile_sha256 = _dockerfile_sha256(commit)

    with pytest.raises(ValueError, match="ancestor of HEAD"):
        cc.run_calibration(args)


def test_docker_context_sha256_must_match_committed_context(tmp_path: Path) -> None:
    if not (ROOT / ".git").exists():
        pytest.skip("git checkout metadata unavailable")
    args = _args(tmp_path)
    args.docker_context_sha256 = "9" * 64
    if args.docker_context_sha256 == _docker_context_sha256(args.engine_commit):
        args.docker_context_sha256 = "8" * 64

    with pytest.raises(ValueError, match="docker_context_sha256 mismatch"):
        cc.run_calibration(args)


def test_dockerfile_sha256_must_match_committed_blob(tmp_path: Path) -> None:
    if not (ROOT / ".git").exists():
        pytest.skip("git checkout metadata unavailable")
    args = _args(tmp_path)
    args.dockerfile_sha256 = hashlib.sha256((ROOT / "docker" / "quant-engine" / "Dockerfile").read_bytes()).hexdigest()
    if args.dockerfile_sha256 == _dockerfile_sha256(args.engine_commit):
        args.dockerfile_sha256 = "9" * 64

    with pytest.raises(ValueError, match="dockerfile_sha256 mismatch"):
        cc.run_calibration(args)


def test_output_manifest_excludes_stale_files_and_records_disk_size(tmp_path: Path) -> None:
    stale = tmp_path / "stale_debug.json"
    stale.write_text('{"leftover": true}\n', encoding="utf-8")

    cc.run_calibration(_args(tmp_path))

    manifest = json.loads((tmp_path / "output_manifest.json").read_text(encoding="utf-8"))
    paths = {entry["path"] for entry in manifest["artifacts"]}
    assert "stale_debug.json" not in paths
    assert "run_matrix.json" not in paths
    assert "reproducibility_report.json" not in paths
    for entry in manifest["artifacts"]:
        assert entry["bytes"] == (tmp_path / entry["path"]).stat().st_size


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
