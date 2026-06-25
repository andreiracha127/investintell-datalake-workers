from __future__ import annotations

import gzip
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import qc_a3_core as qc


def test_store_key_uses_stable_object_store_prefix() -> None:
    assert qc.store_key("manifests\\feature_manifest.json") == (
        "investintell/a3/qc-a3-parity/manifests/feature_manifest.json"
    )


def test_immutable_object_store_prefix_includes_commit_and_evaluation() -> None:
    assert qc.immutable_object_store_prefix(
        "25375bbd23d7eb99210914ad6702bf2d080a27ce",
        "10198d7603036c3327ac9e67",
    ) == "investintell/a3/qc-a3-parity/25375bb/10198d7603036c3327ac9e67"


def test_compare_rows_allows_tiny_float_differences() -> None:
    mismatches = qc.compare_rows(
        "full",
        {"value": 1.0 + 5e-13, "status": "ok"},
        {"value": 1.0, "status": "ok"},
    )

    assert mismatches == []


def test_compare_rows_allows_tiny_json_float_differences() -> None:
    mismatches = qc.compare_rows(
        "full",
        {"distribution": '{"max": 1.0000000000001, "min": 0.0}'},
        {"distribution": '{"max": 1.0, "min": 0.0}'},
    )

    assert mismatches == []


def test_metric_rows_logical_hash_canonicalizes_float_noise() -> None:
    left = [{"fold": "full", "value": 0.39246263518212093}]
    right = [{"fold": "full", "value": 0.39246263518212104}]

    assert qc.metric_rows_logical_hash(left) == qc.metric_rows_logical_hash(right)
    assert qc.metric_rows_raw_sha256(left) != qc.metric_rows_raw_sha256(right)


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


def test_parse_args_accepts_qc_npz_inputs(tmp_path: Path) -> None:
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
        "--macro-l2-npz",
        str(tmp_path / "l2.npz"),
        "--revision-uncertainty-npz",
        str(tmp_path / "uncertainty.npz"),
    ])

    assert command == "run-parity"
    assert isinstance(config, qc.A3ParityConfig)
    assert config.macro_l2_npz == tmp_path / "l2.npz"
    assert config.revision_uncertainty_npz == tmp_path / "uncertainty.npz"


def test_parse_args_accepts_upload_bundle_dir(tmp_path: Path) -> None:
    command, payload = qc.parse_args([
        "upload-object-store",
        "--bundle-dir",
        str(tmp_path / "bundle"),
    ])

    assert command == "upload-object-store"
    assert payload == {"bundle_dir": tmp_path / "bundle"}


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


def test_npz_export_round_trips_records_with_logical_hash(tmp_path: Path) -> None:
    pd = pytest.importorskip("pandas")
    harness = qc.require_harness()
    records = [
        {
            "business_date": "2026-06-24",
            "selection_mode": "latest",
            "series_id": "ICSA",
            "value": 1.5,
            "missing_float": None,
            "missing_string": None,
            "flag": True,
        },
        {
            "business_date": "2026-06-25",
            "selection_mode": "first_release",
            "series_id": "DRTSCILM",
            "value": None,
            "missing_float": 2.0,
            "missing_string": "ok",
            "flag": False,
        },
    ]
    parquet_path = tmp_path / "panel.parquet"
    npz_path = tmp_path / "panel.npz"
    pd.DataFrame(records).to_parquet(parquet_path, index=False)

    qc.export_numeric_panel_npz(parquet_path, npz_path)

    assert harness.logical_records_hash(qc.read_npz_records(npz_path)) == (
        harness.logical_records_hash(harness.read_parquet_records(parquet_path))
    )


def test_materialize_harness_source_from_manifest_verifies_sha(tmp_path: Path) -> None:
    source_dir = tmp_path / "bundle" / "code"
    source_dir.mkdir(parents=True)
    source_path = source_dir / "calibration_harness.py.gz"
    with gzip.open(source_path, "wt", encoding="utf-8") as handle:
        handle.write("VALUE = 1\n")
    manifest = {
        "_local_bundle_dir": str(tmp_path / "bundle"),
        "source_files": {
            "calibration_harness_source": {
                "relative_path": "code/calibration_harness.py.gz",
                "object_store_key": "unused",
                "content_sha256": qc.file_sha256(source_path),
            }
        },
    }

    target = qc.materialize_harness_source_from_manifest(
        manifest,
        lambda _key: None,
        project_root=tmp_path / "project",
    )

    assert target.read_text(encoding="utf-8") == "VALUE = 1\n"
    assert (tmp_path / "project" / "src" / "db.py").exists()
