from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.input_packs.build import P0_INPUT_PACK_ID, build_pack, main
from src.input_packs.verifier import verify_pack

ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = ROOT / "fixtures" / "input_packs" / "p0_sources" / "open_macro_v03"


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_build_cli_creates_verified_open_macro_v03_pack(tmp_path: Path) -> None:
    output = tmp_path / "open_macro_v03_2026-06-26"

    result = main(
        [
            "--profile",
            "open_macro_v03",
            "--as-of",
            "2026-06-26",
            "--source-dir",
            str(SOURCE_DIR),
            "--output",
            str(output),
        ]
    )

    assert result == 0
    manifest = _json(output / "manifest.json")
    verification = verify_pack(output)
    assert verification["ok"] is True
    assert manifest["input_pack_id"] == P0_INPUT_PACK_ID
    assert manifest["runtime_activation"] is False
    assert (output / "reports" / "certification_summary.json").is_file()


def test_build_is_deterministic_and_path_independent(tmp_path: Path) -> None:
    first = build_pack(
        profile="open_macro_v03",
        as_of="2026-06-26",
        source_dir=SOURCE_DIR,
        output=tmp_path / "first" / "pack",
    )
    second = build_pack(
        profile="open_macro_v03",
        as_of="2026-06-26",
        source_dir=SOURCE_DIR,
        output=tmp_path / "second" / "nested" / "pack",
    )

    assert first["input_pack_sha256"] == second["input_pack_sha256"]
    assert first["source_snapshot_sha256"] == second["source_snapshot_sha256"]


def test_as_of_changes_pack_hash(tmp_path: Path) -> None:
    earlier = build_pack(
        profile="open_macro_v03",
        as_of="2026-06-25",
        source_dir=SOURCE_DIR,
        output=tmp_path / "earlier",
    )
    later = build_pack(
        profile="open_macro_v03",
        as_of="2026-06-26",
        source_dir=SOURCE_DIR,
        output=tmp_path / "later",
    )

    assert earlier["input_pack_sha256"] != later["input_pack_sha256"]
    assert earlier["source_snapshot_sha256"] != later["source_snapshot_sha256"]


def test_builder_rejects_unsupported_profile(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported input pack profile"):
        build_pack(
            profile="fund_risk_metrics",
            as_of="2026-06-26",
            source_dir=SOURCE_DIR,
            output=tmp_path / "pack",
        )


def test_p0_pack_does_not_use_derived_tables_as_official_inputs(tmp_path: Path) -> None:
    build_pack(
        profile="open_macro_v03",
        as_of="2026-06-26",
        source_dir=SOURCE_DIR,
        output=tmp_path / "pack",
    )
    report = _json(tmp_path / "pack" / "reports" / "certification_summary.json")
    official = set(report["official_source_tables"])
    banned = set(report["excluded_as_official_inputs"])

    assert official.isdisjoint(banned)
    assert "fund_risk_metrics" not in official


def test_builder_contract_bundle_matches_worker_contract_manifest(tmp_path: Path) -> None:
    build_pack(
        profile="open_macro_v03",
        as_of="2026-06-26",
        source_dir=SOURCE_DIR,
        output=tmp_path / "pack",
    )
    pack_manifest = _json(tmp_path / "pack" / "manifest.json")
    contract_manifest = _json(ROOT / "contracts" / "quant-engine" / "v1" / "manifest.json")

    assert pack_manifest["contract_bundle_sha256"] == contract_manifest["bundle_sha256"].removeprefix("sha256:")
