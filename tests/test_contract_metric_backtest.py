"""PR-A contract tests: additive `open_macro_v03_metric_backtest` job type.

Delivery model (quant_owner decision, Option 3 / new-version-dir): the released
`contracts/quant-engine/v1/` bundle is IMMUTABLE and stays live for every
historical governance guard (controlled_shadow immutable-input pins, runtime
skeleton, handshake, pack-001, calibration-001). The new job type ships in a
NEW versioned bundle dir `contracts/quant-engine/v2/` that is a byte-superset
of v1 (same schemas plus the metric_backtest `oneOf` variants, `$id` bumped to
/v2/) with its own manifest.json.

Governance: the new job type is evidence-only. Its result schema pins, via
`const`, that nothing is activated (`runtime_activation: false`,
`a5_status: "blocked"`, `official_result: false`, `allocator_publish: false`,
`db_write: "none"`, `production_endpoint_activation: "none"`,
`classification: "metric_evidence_only"`). The request pins `offline: true`.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[1]
V1 = ROOT / "contracts" / "quant-engine" / "v1"
V2 = ROOT / "contracts" / "quant-engine" / "v2"

# The commit this PR is based on (main@5db964f). The v1 contract bundle must
# remain byte-identical to this commit; the historical bundle sha is
# reconstructed from it exactly as `contract_bundle.py` computes it.
BASE_COMMIT = "5db964f"

# The bundle sha pinned by the historical calibration/pack-001/shadow/handshake
# evidence AND still live in contracts/quant-engine/v1/manifest.json.
PINNED_HISTORICAL_BUNDLE_SHA256 = (
    "sha256:4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a"
)

DELTA_REPORT = (
    ROOT
    / "artifacts"
    / "contracts"
    / "open_macro_v03_contract_delta_001"
    / "contract_delta_report.json"
)

REQUEST_FIXTURE = V2 / "fixtures" / "valid" / "job-request.metric-backtest.json"
RESULT_FIXTURE = V2 / "fixtures" / "valid" / "job-result.metric-backtest.json"


# --------------------------------------------------------------------------- #
# Helpers mirroring services/quant_engine/.../contract_bundle.py exactly.
# --------------------------------------------------------------------------- #
def _git_show_bytes(commit: str, repo_relpath: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{commit}:{repo_relpath}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    assert result.returncode == 0, f"{repo_relpath} unavailable at {commit}"
    return result.stdout


def _git_ls_tree(commit: str, repo_relpath: str) -> list[str]:
    result = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", commit, "--", repo_relpath],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
        text=True,
    )
    assert result.returncode == 0, f"{repo_relpath} unavailable at {commit}"
    return [line for line in result.stdout.splitlines() if line]


def _bundle_sha256(files: list[dict[str, str]]) -> str:
    """Reproduce contract_bundle.bundle_sha256 independently."""
    canonical = json.dumps(
        sorted(
            ({"path": f["path"], "sha256": f["sha256"]} for f in files),
            key=lambda x: x["path"],
        ),
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _recompute_live_bundle_sha(bundle_dir: Path) -> str:
    """Recompute a live bundle sha over a bundle dir exactly as
    scripts/contract_bundle.py does (schemas + fixtures, manifest excluded)."""
    members = sorted(
        p
        for p in [*bundle_dir.glob("*.schema.json"), *bundle_dir.glob("fixtures/**/*.json")]
        if p.is_file() and p.name != "manifest.json"
    )
    files = [
        {
            "path": p.relative_to(bundle_dir).as_posix(),
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
        }
        for p in members
    ]
    return _bundle_sha256(files)


def _recompute_old_bundle_sha_from_git() -> str:
    manifest = json.loads(
        _git_show_bytes(BASE_COMMIT, "contracts/quant-engine/v1/manifest.json")
    )
    files = []
    for entry in manifest["files"]:
        raw = _git_show_bytes(BASE_COMMIT, f"contracts/quant-engine/v1/{entry['path']}")
        files.append({"path": entry["path"], "sha256": hashlib.sha256(raw).hexdigest()})
    return _bundle_sha256(files)


def _schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# Test 1 — historical v1 bundle: pinned sha re-derivable AND live v1 tree
# byte-identical to BASE_COMMIT (proves zero mutation of the released bundle).
# --------------------------------------------------------------------------- #
def test_historical_pack_validates_against_pinned_historical_bundle() -> None:
    # (a) The OLD bundle, reconstructed from BASE_COMMIT via `git show` and
    # hashed the same way contract_bundle.py hashes it, equals the pinned sha.
    assert _recompute_old_bundle_sha_from_git() == PINNED_HISTORICAL_BUNDLE_SHA256

    # (b) The LIVE v1 tree still carries the pinned sha (v1 remains the live
    # bundle for all historical governance guards).
    live_manifest = json.loads((V1 / "manifest.json").read_text(encoding="utf-8"))
    assert live_manifest["bundle_sha256"] == PINNED_HISTORICAL_BUNDLE_SHA256
    assert _recompute_live_bundle_sha(V1) == PINNED_HISTORICAL_BUNDLE_SHA256

    # (c) Strengthened: the ENTIRE live v1 tree is identical to BASE_COMMIT —
    # same file set (nothing added or removed, including untracked/ignored
    # strays) and identical content per git (`git diff` respects the
    # repository's own line-ending normalization on Windows checkouts).
    committed = _git_ls_tree(BASE_COMMIT, "contracts/quant-engine/v1")
    assert committed, "expected v1 files at BASE_COMMIT"
    live = sorted(
        p.relative_to(ROOT).as_posix() for p in V1.rglob("*") if p.is_file()
    )
    assert live == sorted(committed), "live v1 file set drifted from BASE_COMMIT"
    diff = subprocess.run(
        ["git", "diff", "--name-only", BASE_COMMIT, "--", "contracts/quant-engine/v1/"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
        text=True,
    )
    assert diff.returncode == 0
    assert diff.stdout.strip() == "", (
        f"live v1 tree differs from {BASE_COMMIT}: {diff.stdout.strip()}"
    )
    # And the bundle-relevant members (schemas + fixtures, the hashed set) are
    # raw-byte identical to the committed blobs.
    for rel in committed:
        if rel.endswith(".schema.json") or "/fixtures/" in rel or rel.endswith("manifest.json"):
            assert (ROOT / rel).read_bytes() == _git_show_bytes(BASE_COMMIT, rel), (
                f"{rel} is not byte-identical to {BASE_COMMIT}"
            )


# --------------------------------------------------------------------------- #
# Test 2 — new request/result validate against the v2 schemas; live v2 bundle
# sha matches contract_delta_report.json.
# --------------------------------------------------------------------------- #
def test_metric_backtest_request_and_result_validate_against_new_bundle() -> None:
    request = json.loads(REQUEST_FIXTURE.read_text(encoding="utf-8"))
    result = json.loads(RESULT_FIXTURE.read_text(encoding="utf-8"))

    # Fixtures validate against the whole (oneOf) v2 schema — exactly one
    # variant matches, which must be the new metric_backtest variant.
    jsonschema.validate(request, _schema(V2 / "job-request.schema.json"))
    jsonschema.validate(result, _schema(V2 / "job-result.schema.json"))

    assert request["job_type"] == "open_macro_v03_metric_backtest"
    assert result["job_type"] == "open_macro_v03_metric_backtest"

    # The delta report records the v2 delivery and both bundle shas.
    report = json.loads(DELTA_REPORT.read_text(encoding="utf-8"))
    live_v2 = _recompute_live_bundle_sha(V2)
    assert report["delivery"] == "new_version_dir_v2"
    assert report["version_dir"] == "contracts/quant-engine/v2/"
    assert report["new_bundle_sha256"] == live_v2
    assert report["old_bundle_sha256"] == PINNED_HISTORICAL_BUNDLE_SHA256

    # The v2 manifest is internally consistent with the live recomputation.
    v2_manifest = json.loads((V2 / "manifest.json").read_text(encoding="utf-8"))
    assert v2_manifest["bundle_sha256"] == live_v2
    assert live_v2 != PINNED_HISTORICAL_BUNDLE_SHA256

    # Schema identity hygiene: v2 schemas carry /v2/ $id URLs.
    for name in (
        "job-request.schema.json",
        "job-result.schema.json",
        "engine-manifest.schema.json",
    ):
        assert "/v2/" in _schema(V2 / name)["$id"], f"{name} $id not bumped to /v2/"


# --------------------------------------------------------------------------- #
# Test 3 — evidence-only enforcement (parametrized negative cases, v2 schemas).
# --------------------------------------------------------------------------- #
RESULT_MUTATIONS = [
    ("runtime_activation", True),
    ("a5_status", "active"),
    ("a5_status", "unblocked"),
    ("official_result", True),
    ("allocator_publish", True),
    ("db_write", "productive"),
    ("production_endpoint_activation", "live"),
    ("classification", "productive_result"),
]


@pytest.mark.parametrize("field,bad_value", RESULT_MUTATIONS, ids=lambda v: str(v))
def test_metric_backtest_is_evidence_only_result(field: str, bad_value) -> None:
    result = json.loads(RESULT_FIXTURE.read_text(encoding="utf-8"))
    result[field] = bad_value
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(result, _schema(V2 / "job-result.schema.json"))


def test_metric_backtest_is_evidence_only_request_offline_false() -> None:
    request = json.loads(REQUEST_FIXTURE.read_text(encoding="utf-8"))
    request["offline"] = False
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(request, _schema(V2 / "job-request.schema.json"))


# --------------------------------------------------------------------------- #
# Test 4 — v2 is a strict superset of v1: every v1 `$defs` subtree appears in
# v2 deep-equal, and v2 only ADDS the metric_backtest variants. (Live-v1-
# unchanged is already proven byte-for-byte by test 1; this pins the superset
# relation between the two versions.)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "schema_file,expected_added",
    [
        ("job-request.schema.json", {"open_macro_v03_metric_backtest_request"}),
        ("job-result.schema.json", {"open_macro_v03_metric_backtest_result"}),
    ],
)
def test_existing_contract_defs_byte_unchanged(schema_file: str, expected_added: set[str]) -> None:
    v1_schema = json.loads(
        _git_show_bytes(BASE_COMMIT, f"contracts/quant-engine/v1/{schema_file}")
    )
    v2_schema = _schema(V2 / schema_file)

    v1_defs = v1_schema["$defs"]
    v2_defs = v2_schema["$defs"]

    # Every v1 def is present in v2 and deep-equal (canonical JSON comparison
    # guarantees structural byte-equality of the subtree).
    for name, subtree in v1_defs.items():
        assert name in v2_defs, f"v1 $def {name} missing from v2 {schema_file}"
        assert json.dumps(v2_defs[name], sort_keys=True) == json.dumps(
            subtree, sort_keys=True
        ), f"v1 $def {name} changed in v2 {schema_file}"

    # v2 adds exactly the metric_backtest variant, nothing else.
    assert set(v2_defs) - set(v1_defs) == expected_added

    # oneOf: v2 keeps every v1 variant and adds exactly the new one.
    v1_refs = {v["$ref"] for v in v1_schema["oneOf"]}
    v2_refs = {v["$ref"] for v in v2_schema["oneOf"]}
    assert v1_refs <= v2_refs, f"v1 oneOf variant dropped in v2 {schema_file}"
    assert v2_refs - v1_refs == {f"#/$defs/{name}" for name in expected_added}
