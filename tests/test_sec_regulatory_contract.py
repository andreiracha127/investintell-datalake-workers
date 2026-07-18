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
    assert "verified 36 mirrored files" in result.stdout


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


def test_semantic_verifier_rejects_candidate_key_flag_drift(tmp_path: Path) -> None:
    from scripts.verify_sec_regulatory_contract import (
        ContractVerificationError, _verify_source_table_semantics,
    )

    bundle = _copy_bundle(tmp_path)
    path = bundle / "source-tables" / "rr1.json"
    contract = json.loads(path.read_text(encoding="utf-8"))
    txt = next(
        table for table in contract["schema_variants"][0]["tables"]
        if table["source_file"] == "txt.tsv"
    )
    next(column for column in txt["columns"] if column["name"] == "series")["candidate_key"] = False
    path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(ContractVerificationError, match="key flags drifted"):
        _verify_source_table_semantics(bundle)


def _minimal_source_contract() -> dict:
    return {
        "contract_version": "test-v1",
        "family": "nport",
        "schema_variants": [{
            "metadata_sha256": "a" * 64,
            "tables": [{
                "source_file": "SUBMISSION.tsv",
                "raw_target": "nport_submission_raw",
                "candidate_primary_key": ["ACCESSION_NUMBER"],
                "logical_parents": [],
                "columns": [{
                    "name": "ACCESSION_NUMBER",
                    "parsing_policy": "text_preserve_lexical",
                    "required": True,
                    "candidate_key": True,
                    "datatype": {"base": "string", "maxLength": 20},
                    "raw_target": "nport_submission_raw",
                }],
            }],
        }],
    }


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ("policy", "política de parsing"),
        ("target", "raw_target divergente"),
        ("candidate-key", "candidate_primary_key diverge"),
        ("unknown-parent", "logical_parents fora"),
        ("duplicate-variant", "metadata_sha256 duplicado"),
    ],
)
def test_contract_loader_rejects_internal_contract_drift(
    tmp_path: Path, mutation: str, match: str,
) -> None:
    from src.sec_regulatory.contracts import ContractError, load_source_table_contract

    document = _minimal_source_contract()
    table = document["schema_variants"][0]["tables"][0]
    if mutation == "policy":
        table["columns"][0]["parsing_policy"] = "guess_silently"
    elif mutation == "target":
        table["columns"][0]["raw_target"] = "nport_other_raw"
    elif mutation == "candidate-key":
        table["candidate_primary_key"] = []
    elif mutation == "unknown-parent":
        table["logical_parents"] = ["MISSING.tsv"]
    else:
        document["schema_variants"].append(document["schema_variants"][0].copy())
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ContractError, match=match):
        load_source_table_contract(path, family="nport", metadata_sha256="a" * 64)


def test_contract_loader_accepts_every_corrected_frozen_family_and_variant() -> None:
    from src.sec_regulatory.contracts import load_source_table_contract

    for path in sorted((BUNDLE / "source-tables").glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        for variant in document["schema_variants"]:
            contract = load_source_table_contract(
                path,
                family=document["family"],
                metadata_sha256=variant["metadata_sha256"],
            )
            assert contract.metadata_sha256 == variant["metadata_sha256"]


def test_rr1_txt_contract_uses_the_merged_figure_7_key() -> None:
    from src.sec_regulatory.contracts import load_source_table_contract

    path = BUNDLE / "source-tables" / "rr1.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    expected = (
        "adsh", "tag", "version", "ddate", "series", "class",
        "measure", "document", "otherdims", "iprx",
    )
    assert len(document["schema_variants"]) == 6
    for variant in document["schema_variants"]:
        contract = load_source_table_contract(
            path,
            family="rr1",
            metadata_sha256=variant["metadata_sha256"],
        )
        txt = contract.table_for_filename("txt.tsv")
        assert txt.candidate_key == expected
        assert {column.name for column in txt.columns if column.candidate_key} == set(expected)
