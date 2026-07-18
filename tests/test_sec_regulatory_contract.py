from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "verify_sec_regulatory_contract.py"
BUNDLE = ROOT / "contracts" / "sec-regulatory" / "v1"


def _copy_bundle(tmp_path: Path) -> Path:
    target = tmp_path / "contracts" / "sec-regulatory" / "v1"
    shutil.copytree(BUNDLE, target)
    return target


def _verify(bundle: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--bundle-root", str(bundle)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _manifest(bundle: Path) -> dict:
    return json.loads((bundle / "worker-equivalence-manifest.json").read_text(encoding="utf-8"))


def test_verifier_accepts_the_pinned_worker_contract_bundle() -> None:
    result = _verify(BUNDLE)

    assert result.returncode == 0, result.stderr
    assert "verified 34 mirrored files" in result.stdout


@pytest.mark.parametrize(
    "mutation",
    ["tamper", "missing", "extra", "provenance-drift"],
)
def test_verifier_fails_closed_for_bundle_drift(tmp_path: Path, mutation: str) -> None:
    bundle = _copy_bundle(tmp_path)

    if mutation == "tamper":
        target = bundle / "CHANGELOG.md"
        target.write_bytes(target.read_bytes() + b"\nTAMPERED\n")
    elif mutation == "missing":
        (bundle / "manifest.json").unlink()
    elif mutation == "extra":
        (bundle / "unexpected-worker-file.txt").write_text("not mirrored", encoding="utf-8")
    else:
        provenance_path = bundle / "worker-provenance.json"
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        provenance["source_commit"] = "0" * 40
        provenance_path.write_text(
            json.dumps(provenance, indent=2) + "\n", encoding="utf-8"
        )

    result = _verify(bundle)

    assert result.returncode != 0
    assert "SEC regulatory contract verification failed:" in result.stderr


def test_verifier_rejects_duplicate_equivalence_mapping(tmp_path: Path) -> None:
    bundle = _copy_bundle(tmp_path)
    manifest_path = bundle / "worker-equivalence-manifest.json"
    manifest = _manifest(bundle)
    manifest["files"].append(manifest["files"][0].copy())
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    result = _verify(bundle)

    assert result.returncode != 0
    assert "duplicate mapping" in result.stderr
