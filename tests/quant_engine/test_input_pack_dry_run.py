from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))
sys.path.insert(0, str(ROOT))

from investintell_quant_engine.cli import main
from investintell_quant_engine.runners.input_pack import run_input_pack_dry_run
from src.input_packs.build import build_pack

SOURCE_DIR = ROOT / "fixtures" / "input_packs" / "p0_sources" / "open_macro_v03"


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _build_pack(tmp_path: Path) -> Path:
    output = tmp_path / "pack"
    build_pack(
        profile="open_macro_v03",
        as_of="2026-06-26",
        source_dir=SOURCE_DIR,
        output=output,
    )
    return output


def test_dry_run_consumes_verified_pack_without_runtime_activation(tmp_path: Path) -> None:
    pack = _build_pack(tmp_path)

    result = run_input_pack_dry_run(pack, jobs=4)

    assert result["status"] == "succeeded"
    assert result["classification"] == "input_pack_verified"
    assert result["runtime_activation"] is False
    assert result["freeze_ready"] is False
    assert result["a3_status"] == "open_macro_v03"
    assert result["a4_status"] == "input_pack_certified_for_calibration"
    assert result["a5_status"] == "blocked"
    assert result["input_pack_sha256"] == _json(pack / "manifest.json")["input_pack_sha256"]


def test_dry_run_rejects_invalid_pack(tmp_path: Path) -> None:
    pack = _build_pack(tmp_path)
    rows = _json(pack / "data" / "derived" / "fund_nav_return_features.json")
    rows[0]["value"] = 123
    (pack / "data" / "derived" / "fund_nav_return_features.json").write_text(
        json.dumps(rows, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid certified input pack"):
        run_input_pack_dry_run(pack)


def test_dry_run_cli_outputs_are_canonical_jobs_independent(tmp_path: Path) -> None:
    pack = _build_pack(tmp_path)
    first = tmp_path / "jobs1"
    second = tmp_path / "jobs4"

    assert main(
        [
            "dry-run-input-pack",
            "--input-pack",
            str(pack),
            "--output-dir",
            str(first),
            "--jobs",
            "1",
            "--result-json",
            str(first / "job_result.json"),
            "--manifest-json",
            str(first / "engine_manifest.json"),
            "--outputs-manifest",
            str(first / "outputs_manifest.json"),
            "--outputs-manifest-canonical",
        ]
    ) == 0
    assert main(
        [
            "dry-run-input-pack",
            "--input-pack",
            str(pack),
            "--output-dir",
            str(second),
            "--jobs",
            "4",
            "--result-json",
            str(second / "job_result.json"),
            "--manifest-json",
            str(second / "engine_manifest.json"),
            "--outputs-manifest",
            str(second / "outputs_manifest.json"),
            "--outputs-manifest-canonical",
        ]
    ) == 0

    assert _json(first / "job_result.json") == _json(second / "job_result.json")
    first_manifest = _json(first / "outputs_manifest.json")
    second_manifest = _json(second / "outputs_manifest.json")
    assert [
        (artifact["path"], artifact["sha256"])
        for artifact in first_manifest["artifacts"]
    ] == [
        (artifact["path"], artifact["sha256"])
        for artifact in second_manifest["artifacts"]
    ]


def test_dry_run_runner_has_no_db_or_network_connector_imports() -> None:
    text = (ROOT / "services" / "quant_engine" / "src" / "investintell_quant_engine" / "runners" / "input_pack.py").read_text(
        encoding="utf-8"
    ).lower()
    for forbidden in ("psycopg", "database_url", "src.db", "requests", "httpx", "socket"):
        assert forbidden not in text
