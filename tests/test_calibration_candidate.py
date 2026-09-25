from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from src import calibration_candidate as cc
from src.input_packs.hashing import canonical_json_sha256, file_sha256, load_json


ROOT = Path(__file__).resolve().parents[1]
GOLDEN_PACK = ROOT / "fixtures" / "input_packs" / "golden" / "certified_input_pack"
GOLDEN_MANIFEST_GIT_PATH = f"{GOLDEN_PACK.relative_to(ROOT).as_posix()}/manifest.json"
AMBIENT_MAIN_REFS = ("origin/main", "main", "refs/remotes/origin/main", "refs/heads/main")


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
            ["git", "show", f"{commit}:{GOLDEN_MANIFEST_GIT_PATH}"],
            cwd=ROOT,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return False
    manifest = json.loads(payload)
    return (
        manifest.get("input_pack_sha256") == summary["input_pack_sha256"]
        and manifest.get("builder_commit") == summary["builder_commit"]
    )


def _input_pack_p0_merge_commit(summary: dict[str, str]) -> str:
    # Synthetic fixture provenance, not the historical production P0 merge.
    commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", "HEAD^{commit}"], cwd=ROOT, text=True
    ).strip()
    if not _commit_contains_pack(commit, summary):
        raise AssertionError("HEAD does not contain the matching certified test pack")
    return commit


def _git_stdout(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def _is_ancestor(commit: str, descendant: str) -> bool:
    result = subprocess.run(["git", "merge-base", "--is-ancestor", commit, descendant], cwd=ROOT, check=False)
    if result.returncode not in (0, 1):
        raise AssertionError(f"git merge-base --is-ancestor failed with exit {result.returncode}")
    return result.returncode == 0


def _manifest_variant_bytes(variant: str) -> bytes | None:
    manifest = json.loads(_git_stdout("show", f"HEAD:{GOLDEN_MANIFEST_GIT_PATH}"))
    if variant == "wrong_builder":
        manifest["builder_commit"] = "f" * 40
    elif variant == "wrong_input_pack_sha256":
        manifest["input_pack_sha256"] = "0" * 64
    elif variant == "missing_manifest":
        return None
    elif variant == "malformed_manifest":
        return b'{"input_pack_sha256": \n'
    else:
        raise AssertionError(f"unknown manifest variant: {variant}")
    return (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _same_tree_commit_with_manifest(tmp_path: Path, manifest: bytes | None, message: str) -> str:
    """Parentless commit of HEAD's tree whose certified manifest is replaced or removed.

    A private index file keeps the repository index, working tree and refs untouched;
    only unreachable objects are written, like the other synthetic commits here.
    """
    env = _git_identity_env()
    env["GIT_INDEX_FILE"] = str(tmp_path / "synthetic-manifest.index")
    subprocess.check_call(["git", "read-tree", "HEAD^{tree}"], cwd=ROOT, env=env)
    if manifest is None:
        subprocess.check_call(["git", "update-index", "--force-remove", GOLDEN_MANIFEST_GIT_PATH], cwd=ROOT, env=env)
    else:
        blob = subprocess.check_output(["git", "hash-object", "-w", "--stdin"], cwd=ROOT, input=manifest)
        subprocess.check_call(
            ["git", "update-index", "--cacheinfo", f"100644,{blob.decode('ascii').strip()},{GOLDEN_MANIFEST_GIT_PATH}"],
            cwd=ROOT,
            env=env,
        )
    tree = subprocess.check_output(["git", "write-tree"], cwd=ROOT, env=env, text=True).strip()
    return _commit_tree(tree, message)


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
    base_label: str = "host_jobs1_r0",
    include_isolation: bool = True,
    docker_image_digest: str | None = None,
    docker_image_id: str | None = "sha256:" + "1" * 64,
    path_independence: bool | None = True,
) -> Path:
    labels = labels or sorted(cc.REQUIRED_MATRIX_LABELS)
    payload = {
        "schema_version": 1,
        "calibration_id": cc.CALIBRATION_ID,
        "base_label": base_label,
        "labels": labels,
        "comparisons": {f"{base_label}_vs_{label}": {"ok": True, "mismatched": [], "hashes": hashes} for label in labels},
        "run_count": len(labels),
        "mismatch_count": 0,
        "ok": True,
    }
    if include_isolation:
        payload.update({"network": "none", "db_access": False, "input_pack_mount": "read_only"})
    if docker_image_digest is not None:
        payload["docker_image_digest"] = docker_image_digest
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


def test_run_matrix_validates_digest_and_image_id_together(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    cc.run_calibration(_args(probe))
    probe_matrix = json.loads((probe / "run_matrix.json").read_text(encoding="utf-8"))
    evidence = _write_evidence(
        tmp_path / "wrong_image_pair_evidence.json",
        probe_matrix["current_run_hashes"],
        docker_image_digest="sha256:" + "2" * 64,
        docker_image_id="sha256:" + "9" * 64,
    )
    args = _args(tmp_path / "out", evidence_json=str(evidence))
    args.engine_image_digest = "sha256:" + "2" * 64

    cc.run_calibration(args)

    run_matrix = json.loads((tmp_path / "out" / "run_matrix.json").read_text(encoding="utf-8"))
    reproducibility = json.loads((tmp_path / "out" / "reproducibility_report.json").read_text(encoding="utf-8"))
    assert run_matrix["ok"] is False
    assert run_matrix["hashes"] == {}
    assert reproducibility["evidence_ok"] is False


def test_run_matrix_requires_required_base_label(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    cc.run_calibration(_args(probe))
    probe_matrix = json.loads((probe / "run_matrix.json").read_text(encoding="utf-8"))
    evidence = _write_evidence(
        tmp_path / "unlisted_base_evidence.json",
        probe_matrix["current_run_hashes"],
        base_label="manual_baseline",
    )

    cc.run_calibration(_args(tmp_path / "out", evidence_json=str(evidence)))

    run_matrix = json.loads((tmp_path / "out" / "run_matrix.json").read_text(encoding="utf-8"))
    reproducibility = json.loads((tmp_path / "out" / "reproducibility_report.json").read_text(encoding="utf-8"))
    assert run_matrix["ok"] is False
    assert run_matrix["hashes"] == {}
    assert reproducibility["evidence_ok"] is False


def test_engine_image_identifiers_must_be_sha256_prefixed(tmp_path: Path) -> None:
    args = _args(tmp_path / "bad-image-id")
    args.engine_image_id = "local-image-placeholder"

    with pytest.raises(ValueError, match="engine_image_id must be a sha256:<64 hex> digest"):
        cc.run_calibration(args)

    args = _args(tmp_path / "bad-image-digest")
    args.engine_image_digest = "not-a-digest"

    with pytest.raises(ValueError, match="engine_image_digest must be a sha256:<64 hex> digest"):
        cc.run_calibration(args)


def test_engine_image_identifier_is_required(tmp_path: Path) -> None:
    args = _args(tmp_path / "missing-image-provenance")
    args.engine_image_digest = None
    args.engine_image_id = None

    with pytest.raises(ValueError, match="engine_image_digest or engine_image_id must be provided"):
        cc.run_calibration(args)


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


def test_run_calibration_rejects_dangling_symlinked_output(tmp_path: Path) -> None:
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    outside = outside_dir / "selected_parameters.json"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    try:
        os.symlink(outside, output_dir / "selected_parameters.json")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with pytest.raises(ValueError, match="symlinked output path"):
        cc.run_calibration(_args(output_dir))
    assert not outside.exists()


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

    expected = f"input_pack_p0_merge_commit does not contain the certified pack manifest: {empty_commit}"
    with pytest.raises(ValueError, match=rf"^{re.escape(expected)}$"):
        cc.run_calibration(args)


def test_calibration_branch_base_commit_must_be_checkoutable(tmp_path: Path) -> None:
    if not (ROOT / ".git").exists():
        pytest.skip("git checkout metadata unavailable")
    args = _args(tmp_path)
    args.calibration_branch_base_commit = "9" * 40

    with pytest.raises(ValueError, match="calibration_branch_base_commit is not a checkoutable commit"):
        cc.run_calibration(args)


def test_calibration_branch_base_commit_must_be_ancestor_of_head(tmp_path: Path) -> None:
    summary = _summary()
    orphan = _commit_tree("HEAD^{tree}", "same-tree orphan calibration branch base")
    # Identical tree and a matching committed pack are intentionally insufficient.
    assert _git_stdout("rev-parse", f"{orphan}^{{tree}}") == _git_stdout("rev-parse", "HEAD^{tree}")
    assert _commit_contains_pack(orphan, summary)
    assert not _is_ancestor(orphan, "HEAD")
    args = _args(tmp_path)
    # Orphan in BOTH fields: P0-to-base holds reflexively, so only base-to-HEAD can reject.
    args.input_pack_p0_merge_commit = orphan
    args.calibration_branch_base_commit = orphan

    expected = f"calibration_branch_base_commit must be an ancestor of HEAD: {orphan}"
    with pytest.raises(ValueError, match=rf"^{re.escape(expected)}$"):
        cc.run_calibration(args)


def test_input_pack_p0_merge_commit_must_be_ancestor_of_calibration_base(tmp_path: Path) -> None:
    summary = _summary()
    head = _git_stdout("rev-parse", "--verify", "HEAD^{commit}")
    orphan = _commit_tree("HEAD^{tree}", "same-tree orphan input pack P0 merge")
    assert _git_stdout("rev-parse", f"{orphan}^{{tree}}") == _git_stdout("rev-parse", "HEAD^{tree}")
    assert _commit_contains_pack(orphan, summary)
    assert not _is_ancestor(orphan, head)
    args = _args(tmp_path)
    assert args.calibration_branch_base_commit == head
    args.input_pack_p0_merge_commit = orphan

    expected = f"calibration_branch_base_commit must be an ancestor of {head}: {orphan}"
    with pytest.raises(ValueError, match=rf"^{re.escape(expected)}$"):
        cc.run_calibration(args)


@pytest.mark.parametrize("field_name", ["input_pack_p0_merge_commit", "calibration_branch_base_commit"])
@pytest.mark.parametrize(
    ("variant", "error", "gate_message"),
    [
        ("wrong_input_pack_sha256", ValueError, "does not contain the verified input pack"),
        ("wrong_builder", ValueError, "builder provenance mismatch"),
        ("missing_manifest", ValueError, "does not contain the certified pack manifest"),
        ("malformed_manifest", json.JSONDecodeError, None),
    ],
)
def test_pack_commits_must_carry_the_verified_pack_identity(
    tmp_path: Path,
    field_name: str,
    variant: str,
    error: type[Exception],
    gate_message: str | None,
) -> None:
    commit = _same_tree_commit_with_manifest(
        tmp_path, _manifest_variant_bytes(variant), f"synthetic pack commit with {variant}"
    )
    args = _args(tmp_path / "out")
    setattr(args, field_name, commit)

    match = rf"^{re.escape(f'{field_name} {gate_message}')}" if gate_message else None
    with pytest.raises(error, match=match) as raised:
        cc.run_calibration(args)
    assert type(raised.value) is error


def test_engine_commit_is_required(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.engine_commit = None

    with pytest.raises(ValueError, match="engine_commit must be provided explicitly"):
        cc.run_calibration(args)


def test_engine_commit_must_be_well_formed(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.engine_commit = "not-a-commit"

    with pytest.raises(ValueError, match="^engine_commit must be a 40-character git commit SHA$"):
        cc.run_calibration(args)


def test_engine_commit_must_exist_when_git_checkout_available(tmp_path: Path) -> None:
    if not (ROOT / ".git").exists():
        pytest.skip("git checkout metadata unavailable")
    args = _args(tmp_path)
    args.engine_commit = "9" * 40

    expected = f"engine_commit is not a checkoutable commit in this repository: {'9' * 40}"
    with pytest.raises(ValueError, match=rf"^{re.escape(expected)}$"):
        cc.run_calibration(args)


def test_engine_commit_must_be_reachable_from_current_head(tmp_path: Path) -> None:
    if not (ROOT / ".git").exists():
        pytest.skip("git checkout metadata unavailable")
    commit = _commit_tree("HEAD^{tree}", "unreachable calibration engine")
    args = _args(tmp_path)
    args.engine_commit = commit
    args.docker_context_sha256 = _docker_context_sha256(commit)
    args.dockerfile_sha256 = _dockerfile_sha256(commit)

    # Field-specific: the calibration-base ancestry gate must not satisfy this test.
    expected = f"engine_commit must be an ancestor of HEAD: {commit}"
    with pytest.raises(ValueError, match=rf"^{re.escape(expected)}$"):
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


def _names_ref(argv: list[str], ref: str) -> bool:
    return any(arg == ref or arg.startswith((f"{ref}:", f"{ref}^", f"{ref}~")) for arg in argv)


def test_fixture_p0_commit_is_verified_head_and_real_ancestor(tmp_path: Path) -> None:
    summary = _summary()
    head = _git_stdout("rev-parse", "--verify", "HEAD^{commit}")

    selected = _input_pack_p0_merge_commit(summary)

    assert selected == head
    committed = json.loads(_git_stdout("show", f"{selected}:{GOLDEN_MANIFEST_GIT_PATH}"))
    assert committed["input_pack_sha256"] == summary["input_pack_sha256"]
    assert committed["builder_commit"] == summary["builder_commit"]
    assert _is_ancestor(selected, "HEAD")
    args = _args(tmp_path)
    assert args.input_pack_p0_merge_commit == args.calibration_branch_base_commit == args.engine_commit == head


@pytest.mark.parametrize("ambient_main", ["nonancestor_orphan_with_pack", "absent"])
def test_fixture_p0_commit_never_consults_ambient_main(monkeypatch: pytest.MonkeyPatch, ambient_main: str) -> None:
    summary = _summary()
    head = _git_stdout("rev-parse", "--verify", "HEAD^{commit}")
    orphan = _commit_tree("HEAD^{tree}", "ambient main that is not an ancestor of HEAD")
    # The orphan carries the matching committed pack, so a ref scan would have accepted it.
    assert _commit_contains_pack(orphan, summary)
    assert not _is_ancestor(orphan, head)

    real_check_output = subprocess.check_output
    calls: list[list[str]] = []

    def fake_check_output(cmd: list[str], *args: object, **kwargs: object) -> object:
        argv = [str(part) for part in cmd]
        calls.append(argv)
        if argv[:2] == ["git", "rev-parse"] and any(_names_ref(argv[2:], ref) for ref in AMBIENT_MAIN_REFS):
            if ambient_main == "absent":
                raise subprocess.CalledProcessError(128, argv)
            return orphan + "\n"
        return real_check_output(cmd, *args, **kwargs)

    monkeypatch.setattr(subprocess, "check_output", fake_check_output)
    for ref in AMBIENT_MAIN_REFS:
        if ambient_main == "absent":
            with pytest.raises(subprocess.CalledProcessError):
                subprocess.check_output(["git", "rev-parse", ref], cwd=ROOT, text=True)
        else:
            assert subprocess.check_output(["git", "rev-parse", ref], cwd=ROOT, text=True).strip() == orphan
    calls.clear()

    selected = _input_pack_p0_merge_commit(summary)

    assert selected == head
    assert selected not in {orphan, summary["builder_commit"]}
    assert calls and all(argv[0] == "git" for argv in calls)
    for ref in (*AMBIENT_MAIN_REFS, summary["builder_commit"]):
        assert not any(_names_ref(argv, ref) for argv in calls), ref


@pytest.mark.parametrize(
    "variant", ["wrong_builder", "wrong_input_pack_sha256", "missing_manifest", "malformed_manifest"]
)
def test_fixture_p0_commit_rejects_head_without_matching_pack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    summary = _summary()
    head = _git_stdout("rev-parse", "--verify", "HEAD^{commit}")
    commit = _same_tree_commit_with_manifest(
        tmp_path, _manifest_variant_bytes(variant), f"synthetic HEAD with {variant}"
    )
    assert _commit_contains_pack(head, summary) is True
    if variant == "malformed_manifest":
        with pytest.raises(json.JSONDecodeError):
            _commit_contains_pack(commit, summary)
    else:
        assert _commit_contains_pack(commit, summary) is False

    real_check_output = subprocess.check_output

    def fake_check_output(cmd: list[str], *args: object, **kwargs: object) -> object:
        if [str(part) for part in cmd] == ["git", "rev-parse", "--verify", "HEAD^{commit}"]:
            return commit + "\n"
        return real_check_output(cmd, *args, **kwargs)

    monkeypatch.setattr(subprocess, "check_output", fake_check_output)

    if variant == "malformed_manifest":
        with pytest.raises(json.JSONDecodeError):
            _input_pack_p0_merge_commit(summary)
    else:
        with pytest.raises(AssertionError, match="^HEAD does not contain the matching certified test pack$"):
            _input_pack_p0_merge_commit(summary)


@pytest.mark.parametrize(
    "make_failure",
    [
        pytest.param(
            lambda: subprocess.CalledProcessError(128, ["git", "rev-parse", "--verify", "HEAD^{commit}"]),
            id="unresolvable_head",
        ),
        pytest.param(lambda: FileNotFoundError("git"), id="git_unavailable"),
    ],
)
def test_fixture_p0_commit_fails_loudly_when_head_cannot_be_resolved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, make_failure: Callable[[], Exception]
) -> None:
    real_check_output = subprocess.check_output
    failure_type = type(make_failure())

    def fake_check_output(cmd: list[str], *args: object, **kwargs: object) -> object:
        if [str(part) for part in cmd][:3] == ["git", "rev-parse", "--verify"]:
            raise make_failure()
        return real_check_output(cmd, *args, **kwargs)

    monkeypatch.setattr(subprocess, "check_output", fake_check_output)

    # No skip, fallback or fabricated builder commit: the fixture itself must fail.
    with pytest.raises(failure_type):
        _input_pack_p0_merge_commit(_summary())
    with pytest.raises(failure_type):
        _args(tmp_path)
