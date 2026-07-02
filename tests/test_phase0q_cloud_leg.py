"""Tests for the phase0q cloud-leg PREPARATION package (build-only).

Everything here is pure construction / mocks: ZERO network calls and ZERO ``lean``
executions. The bundle is built ONCE per session (the build runs the deterministic
local harness, which is the slow part) and reused across tests.

Coverage:
  * bundle determinism (byte-identical rebuild),
  * drift refusal (a tampered temp source copy makes the build refuse),
  * shipped-source equivalence (gzip contents byte-match the git HEAD blobs),
  * upload-plan correctness (manifest LAST, keys match the bundle, nothing executed),
  * governance markers (whitespace-tolerant + recursive JSON walk),
  * immutability pins of the three committed evidence dirs (001 / 002 / grid_001),
  * notebook JSON well-formed + contains the db-stub and hash-comparison cells.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from harness.phase0q_cloud import bundle as bundle_mod
from harness.phase0q_cloud import upload_plan as upload_plan_mod

ROOT = Path(__file__).resolve().parents[1]
CLOUD_PKG = ROOT / "harness" / "phase0q_cloud"
ARTIFACT_DIR = ROOT / "artifacts" / "quant" / "open_macro_v03_cloud_leg_001"
NOTEBOOK = CLOUD_PKG / "phase0q_cloud_leg.ipynb"

# A real 40-char commit SHA (the pack/contract provenance target); any valid hex works.
HARNESS_COMMIT = "68b07e810bc28665fedd85c6acd3ea5770b4b099"


# --------------------------------------------------------------------------- #
# Session bundle build (deterministic; no network / no lean)                  #
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="session")
def built_bundle(tmp_path_factory) -> Path:
    bundle_dir = tmp_path_factory.mktemp("phase0q_cloud_bundle")
    summary = bundle_mod.build_bundle(bundle_dir, HARNESS_COMMIT)
    assert summary["status"] == "prepared_pending_upload"
    return bundle_dir


@pytest.fixture(scope="session")
def bundle_manifest(built_bundle) -> dict:
    return json.loads((built_bundle / "object_store_manifest.json").read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# Determinism                                                                 #
# --------------------------------------------------------------------------- #

def _tree_hashes(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in sorted(root.rglob("*")):
        if p.is_file() and "_pack_root" not in p.parts:
            out[p.relative_to(root).as_posix()] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def test_bundle_rebuild_is_byte_identical(built_bundle, tmp_path):
    rebuild = tmp_path / "rebuild"
    bundle_mod.build_bundle(rebuild, HARNESS_COMMIT)
    a, b = _tree_hashes(built_bundle), _tree_hashes(rebuild)
    assert set(a) == set(b)
    diffs = [k for k in a if a[k] != b[k]]
    assert diffs == [], f"non-deterministic bundle files: {diffs}"


def test_bundle_manifest_prefix_and_key(bundle_manifest):
    prefix = bundle_manifest["object_store_prefix_immutable"]
    assert prefix == (
        f"investintell/open_macro_v03/phase0q/{HARNESS_COMMIT}/"
        f"{bundle_manifest['input_pack_sha256']}"
    )
    assert bundle_manifest["object_store_manifest_key"] == f"{prefix}/object_store_manifest.json"
    assert bundle_manifest["qc_project_id"] == 33679769


def test_bundle_build_invalid_commit_rejected(tmp_path):
    with pytest.raises(ValueError, match="40-char"):
        bundle_mod.build_bundle(tmp_path / "bad", "not-a-sha")


# --------------------------------------------------------------------------- #
# Drift refusal                                                               #
# --------------------------------------------------------------------------- #

def test_drift_refusal_on_tampered_source(monkeypatch, tmp_path):
    """A shipped source whose working-tree bytes differ from git HEAD must abort the
    build. We simulate the tamper by monkeypatching the working-tree read for one
    shipped file to return mutated bytes; the git HEAD blob is unchanged, so the build
    must refuse. (No repo file is modified.)"""
    target_rel = "harness/phase0q/runner.py"
    real_read_bytes = Path.read_bytes

    def fake_read_bytes(self: Path):
        if self.as_posix().endswith(target_rel):
            return real_read_bytes(self) + b"\n# TAMPER\n"
        return real_read_bytes(self)

    monkeypatch.setattr(Path, "read_bytes", fake_read_bytes)
    with pytest.raises(RuntimeError, match="drift refusal"):
        bundle_mod.build_bundle(tmp_path / "drift", HARNESS_COMMIT)


def test_drift_refusal_helper_matches_head():
    # A clean shipped file passes drift refusal and returns LF-normalized HEAD bytes.
    data = bundle_mod.read_source_with_drift_refusal("harness/phase0q/runner.py")
    head = subprocess.check_output(
        ["git", "cat-file", "blob", "HEAD:harness/phase0q/runner.py"], cwd=ROOT
    ).replace(b"\r\n", b"\n")
    assert data == head


# --------------------------------------------------------------------------- #
# Shipped-source equivalence (gzip == git HEAD blob)                          #
# --------------------------------------------------------------------------- #

def test_shipped_gzip_sources_match_git_head(built_bundle, bundle_manifest):
    groups = (
        bundle_manifest["harness_sources"],
        bundle_manifest["src_sources"],
        bundle_manifest["quant_core_sources"],
    )
    checked = 0
    for group in groups:
        for entry in group:
            gz_path = built_bundle / entry["relative_path"]
            got = gzip.decompress(gz_path.read_bytes())
            assert hashlib.sha256(got).hexdigest() == entry["plaintext_sha256"]
            source_path = entry.get("source_path")
            if source_path is None:
                # the fail-loud db stub has no repo file; assert its content refuses.
                assert b"offline-only" in got and b"LOCK_REGIME_QUADRANT" in got
                continue
            head = subprocess.check_output(
                ["git", "cat-file", "blob", f"HEAD:{source_path}"], cwd=ROOT
            ).replace(b"\r\n", b"\n")
            assert got == head, f"shipped {source_path} != git HEAD blob"
            checked += 1
    assert checked >= 20  # harness + src + quant_core closure


def test_pack_tables_present_and_full_tree_shipped(built_bundle, bundle_manifest):
    for entry in bundle_manifest["pack_canonical_tables"]:
        assert (built_bundle / entry["relative_path"]).is_file()
    # the whole pack tree ships (manifest.json etc.), not just the two tables.
    pack_objects = [rel for rel in bundle_manifest["object_files"] if rel.startswith("pack/")]
    assert any(rel.endswith("manifest.json") for rel in pack_objects)
    assert any(rel.endswith("data/canonical/eod_prices.json") for rel in pack_objects)


# --------------------------------------------------------------------------- #
# Upload plan correctness (pure construction; nothing executed)               #
# --------------------------------------------------------------------------- #

def test_upload_plan_manifest_last_and_keys_match(built_bundle, bundle_manifest):
    summary = upload_plan_mod.emit(built_bundle)
    assert summary["executed"] is False
    assert summary["manifest_uploaded_last"] is True
    assert summary["any_on_disk_drift"] is False

    plan = json.loads((built_bundle / "upload_plan.json").read_text(encoding="utf-8"))
    assert plan["executed"] is False
    assert plan["lean_invocations"] == 0
    assert plan["network_calls"] == 0

    cmds = plan["ordered_upload_commands"]
    # manifest is LAST, everything before it is a bundle object.
    assert cmds[-1]["is_manifest"] is True
    assert cmds[-1]["object_store_key"] == bundle_manifest["object_store_manifest_key"]
    object_cmds = cmds[:-1]
    assert len(object_cmds) == len(bundle_manifest["object_files"])
    manifest_keys = {item["object_store_key"] for item in bundle_manifest["object_files"].values()}
    assert {c["object_store_key"] for c in object_cmds} == manifest_keys
    # every command is a `lean cloud object-store set` construction, none executed.
    for c in cmds:
        assert c["argv"][:4] == ["lean", "cloud", "object-store", "set"]


def test_upload_plan_shell_script_orders_manifest_last(built_bundle):
    upload_plan_mod.emit(built_bundle)
    script = (built_bundle / "upload_plan.sh").read_text(encoding="utf-8")
    assert "manifest LAST" in script
    # the manifest key line must appear after every object key line.
    manifest_key_line = script.rindex("object_store_manifest.json")
    first_code_line = script.index("lean cloud object-store set")
    assert manifest_key_line > first_code_line


def test_upload_plan_does_not_invoke_lean(monkeypatch, built_bundle):
    """Belt-and-suspenders: emitting the plan must never call subprocess/lean."""
    def boom(*a, **k):  # pragma: no cover - only fires on regression
        raise AssertionError("upload_plan must not execute any subprocess")

    monkeypatch.setattr(subprocess, "run", boom)
    monkeypatch.setattr(subprocess, "check_output", boom)
    monkeypatch.setattr(subprocess, "check_call", boom)
    monkeypatch.setattr(subprocess, "Popen", boom)
    summary = upload_plan_mod.emit(built_bundle)
    assert summary["executed"] is False


# --------------------------------------------------------------------------- #
# fetch_results (pure construction; no network)                               #
# --------------------------------------------------------------------------- #

def test_fetch_results_matches_and_mismatches(built_bundle, tmp_path):
    from harness.phase0q_cloud import fetch_results

    expected = json.loads(
        (built_bundle / "expected_results_manifest.json").read_text(encoding="utf-8"))
    # a matching verdict → reproduced
    good_verdict = {
        "run_fingerprint": expected["run_fingerprint"],
        "output_logical_hashes": expected["output_logical_hashes"],
        "execution_legs": {"qc_research_object_store": {
            "logical_hash": expected["execution_legs"]["local_python_pure"]["logical_hash"]}},
    }
    vpath = tmp_path / "verdict.json"
    vpath.write_text(json.dumps(good_verdict), encoding="utf-8")
    out = tmp_path / "consolidated.json"
    summary = fetch_results.complete_report(vpath, built_bundle / "expected_results_manifest.json", out)
    assert summary["reproduced"] is True and summary["mismatch_count"] == 0
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["verdict"] == "reproduced"
    assert report["governance"]["A5"] == "blocked"

    # a drifted verdict → not reproduced
    bad_verdict = dict(good_verdict)
    bad_verdict["run_fingerprint"] = "0" * 64
    vpath.write_text(json.dumps(bad_verdict), encoding="utf-8")
    summary2 = fetch_results.complete_report(vpath, built_bundle / "expected_results_manifest.json", out)
    assert summary2["reproduced"] is False and summary2["mismatch_count"] >= 1


# --------------------------------------------------------------------------- #
# Governance markers (whitespace-tolerant + recursive JSON walk)              #
# --------------------------------------------------------------------------- #

FORBIDDEN_TRUE = ("runtime_activation", "activation_allowed", "allocator_publish",
                  "official_result", "freeze_ready", "approved")


def _walk(node):
    if isinstance(node, dict):
        for k, v in node.items():
            yield k, v
            yield from _walk(v)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _assert_governance(payload):
    for key, value in _walk(payload):
        if key in FORBIDDEN_TRUE:
            assert value is not True, f"{key} must never be true"
        if key == "A5":
            assert str(value).strip().lower() == "blocked"
        if key == "db_write_mode":
            assert str(value).strip().lower() == "none"


@pytest.mark.parametrize("name", [
    "cloud_leg_manifest.json",
    "consolidated_reproducibility_report.json",
])
def test_committed_artifact_governance_markers(name):
    payload = json.loads((ARTIFACT_DIR / name).read_text(encoding="utf-8"))
    _assert_governance(payload)
    gov = payload["governance"]
    assert gov["A5"] == "blocked"
    assert gov["status"] == "candidate_not_approved"
    assert gov["approved"] is False
    assert gov["db_write_mode"] == "none"


def test_cloud_leg_manifest_status_and_table(bundle_manifest):
    manifest = json.loads((ARTIFACT_DIR / "cloud_leg_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "prepared_pending_upload"
    assert manifest["qc_project_id"] == 33679769
    # the committed object-key/sha table matches the freshly built bundle's manifest.
    table = manifest["object_key_sha_table"]
    live = {rel: item["content_sha256"] for rel, item in bundle_manifest["object_files"].items()}
    assert {rel: v["content_sha256"] for rel, v in table.items()} == live


def test_consolidated_report_cloud_side_pending():
    report = json.loads(
        (ARTIFACT_DIR / "consolidated_reproducibility_report.json").read_text(encoding="utf-8"))
    assert report["status"] == "pending_cloud_leg"
    matrix = report["reproducibility_matrix"]
    assert matrix["local_python_pure"]["logical_hash"]  # local filled
    assert matrix["qc_research_object_store"]["logical_hash"] is None  # cloud null/pending
    assert report["reproduced"] is None
    assert report["verdict"] == "pending"


def test_build_only_manifest_declares_no_network_or_lean(bundle_manifest):
    policy = bundle_manifest["upload_policy"]
    assert policy["network_calls_during_build"] == 0
    assert policy["lean_invocations_during_build"] == 0
    assert policy["manifest_uploaded_last"] is True


# --------------------------------------------------------------------------- #
# Immutability of the three committed evidence dirs                           #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("evidence_rel, required", [
    ("artifacts/quant/open_macro_v03_metric_evidence_001", (
        "metric_backtest_result.json", "quantitative_gate_report.measured.json")),
    ("artifacts/quant/open_macro_v03_metric_evidence_002", ("evidence_index.json",)),
    ("artifacts/quant/open_macro_v03_compression_grid_001", (
        "compression_grid_manifest.json", "grid_results.json")),
])
def test_evidence_dirs_unmodified_vs_git_head(evidence_rel, required):
    """The cloud-leg prep must not touch any existing evidence artifact: every file in
    the three dirs must be byte-identical to its committed git HEAD blob."""
    evidence_dir = ROOT / evidence_rel
    for name in required:
        assert (evidence_dir / name).is_file(), f"missing {evidence_rel}/{name}"
    files = subprocess.check_output(
        ["git", "ls-files", evidence_rel], cwd=ROOT, text=True).split()
    assert files, f"no committed files under {evidence_rel}"
    for rel in files:
        head = subprocess.check_output(
            ["git", "cat-file", "blob", f"HEAD:{rel}"], cwd=ROOT)
        working = (ROOT / rel).read_bytes()
        assert working.replace(b"\r\n", b"\n") == head.replace(b"\r\n", b"\n"), (
            f"{rel} differs from git HEAD (evidence dir must be immutable)")


# --------------------------------------------------------------------------- #
# Notebook JSON well-formed + required cells                                  #
# --------------------------------------------------------------------------- #

def test_notebook_json_wellformed_and_cells():
    nb = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    assert nb["nbformat"] == 4
    code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
    assert code_cells, "notebook has no code cells"
    all_src = "\n".join("".join(c["source"]) for c in nb["cells"])

    # db-stub cell: materializes + verifies the fail-loud offline db stub.
    assert "src/db.py.gz" in all_src or "code/src/db.py" in all_src
    assert "db stub must refuse" in all_src
    assert "offline-only" in all_src

    # hash-comparison cell: compares recomputed hashes to expected + emits verdict.
    assert "output_logical_hashes" in all_src
    assert "all_hashes_match" in all_src
    assert "phase0q_cloud_verdict.json" in all_src

    # drift refusal on object pull.
    assert "drift refusal" in all_src

    # every code cell must parse.
    import ast
    for c in code_cells:
        ast.parse("".join(c["source"]))


def test_qc_project_workspace_scaffolding():
    qc = CLOUD_PKG / "qc_project"
    config = json.loads((qc / "config.json").read_text(encoding="utf-8"))
    assert config["cloud-id"] == 33679769
    assert config["algorithm-language"] == "Python"
    main_py = (qc / "main.py").read_text(encoding="utf-8")
    assert "raise RuntimeError" in main_py  # placeholder refuses to run as a backtest
    assert (qc / "phase0q_cloud_leg.ipynb").is_file()
    # the notebook copy must match the package notebook byte-for-byte.
    assert (qc / "phase0q_cloud_leg.ipynb").read_bytes() == NOTEBOOK.read_bytes()
