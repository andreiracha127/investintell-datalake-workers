from __future__ import annotations

from pathlib import Path

import qc_a3_core as qc


def test_store_key_uses_stable_object_store_prefix() -> None:
    assert qc.store_key("manifests\\feature_manifest.json") == (
        "investintell/a3/qc-a3-parity/manifests/feature_manifest.json"
    )


def test_compare_rows_allows_tiny_float_differences() -> None:
    mismatches = qc.compare_rows(
        "full",
        {"value": 1.0 + 5e-13, "status": "ok"},
        {"value": 1.0, "status": "ok"},
    )

    assert mismatches == []


def test_compare_rows_reports_categorical_and_float_mismatches() -> None:
    mismatches = qc.compare_rows(
        "full",
        {"value": 1.0 + 1e-6, "status": "ok"},
        {"value": 1.0, "status": "failed"},
    )

    assert {item["field"] for item in mismatches} == {"value", "status"}


def test_parse_args_defaults_to_selected_a3_candidate(tmp_path: Path) -> None:
    command, config = qc.parse_args([
        "run-parity",
        "--feature-manifest",
        str(tmp_path / "feature_manifest.json"),
        "--revision-uncertainty-manifest",
        str(tmp_path / "revision_uncertainty_manifest.json"),
        "--config-catalog",
        str(tmp_path / "catalog.json"),
        "--a32-grid-dir",
        str(tmp_path / "a32"),
        "--output-dir",
        str(tmp_path / "out"),
    ])

    assert command == "run-parity"
    assert config.a31_name == qc.DEFAULT_A31_NAME
    assert config.a32_name == qc.DEFAULT_A32_NAME


def test_compare_expected_metrics_skips_when_reference_dir_missing(tmp_path: Path) -> None:
    config = qc.A3ParityConfig(
        feature_manifest=tmp_path / "feature_manifest.json",
        revision_uncertainty_manifest=tmp_path / "revision_uncertainty_manifest.json",
        config_catalog=tmp_path / "catalog.json",
        a32_grid_dir=tmp_path / "a32",
        output_dir=tmp_path / "out",
        expected_v03_grid_dir=tmp_path / "missing",
    )

    comparison = qc.compare_expected_metrics(config, [])

    assert comparison["enabled"] is False
    assert comparison["status"] == "skipped"


def test_require_harness_imports_worker_module() -> None:
    harness = qc.require_harness()

    assert hasattr(harness, "build_l3_score_panel")


def test_load_a32_config_from_exported_selected_json(tmp_path: Path) -> None:
    harness = qc.require_harness()
    a32 = harness.reference_a32_config(name="A32-TEST")
    qc.write_json(tmp_path / "selected_a32_config.json", harness.asdict(a32))

    loaded = qc.load_a32_config(tmp_path, "A32-TEST")

    assert loaded == a32
