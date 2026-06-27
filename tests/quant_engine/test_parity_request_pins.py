from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))
sys.path.insert(0, str(ROOT))

import investintell_quant_engine.cli as engine_cli
from investintell_quant_engine.runners import parity as parity_runner
from investintell_quant_engine.runners.parity import a3_input_bundle_logical_hash, run_parity_job


def _config(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        config_catalog=tmp_path / "config_catalog.json",
        output_dir=tmp_path / "out",
        a31_name="A31",
    )


def _report() -> dict:
    return {
        "execution_id": "exec-1",
        "a31_config_hash": "4b263d560be163fb131d9fdf",
        "a32_config_hash": "40823b2a6aba9b998109e23e",
        "parent_hashes": {
            "l2_macro_logical_hash": "l2-logical-hash",
            "revision_uncertainty_logical_hash": "uncertainty-logical-hash",
        },
        "runtime_replay_logical_hash": "0" * 64,
        "counterfactual_replay_logical_hash": "1" * 64,
        "metrics_canonical_logical_hash": "2" * 64,
        "metrics_raw_sha256": "3" * 64,
        "model_evaluation_hash": "model-hash",
        "comparison": {"status": "passed"},
        "runtime_activation": False,
        "freeze_ready": False,
        "a4_status": "harness_ready_provisional_A3",
        "a5_status": "blocked",
    }


def _patch_pin_sources(monkeypatch: pytest.MonkeyPatch, *, catalog_hash: str = "catalog-hash") -> None:
    monkeypatch.setattr(
        parity_runner.qc,
        "load_l2_macro_for_config",
        lambda config: ({}, Path("l2.parquet"), "l2-logical-hash", []),
    )
    monkeypatch.setattr(
        parity_runner.qc,
        "load_revision_uncertainty_for_config",
        lambda config: (
            {"parent_hashes": {"l2_macro_logical_hash": "l2-logical-hash"}},
            "uncertainty-logical-hash",
            [],
        ),
    )
    monkeypatch.setattr(
        parity_runner.qc,
        "load_a31_from_catalog",
        lambda **kwargs: (object(), "a31-hash", {}, catalog_hash),
    )


def test_run_parity_rejects_request_pin_mismatch_before_running_parity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_pin_sources(monkeypatch)

    def fail_if_called(config):
        raise AssertionError("qc.run_parity should not run after a request pin mismatch")

    monkeypatch.setattr(parity_runner.qc, "run_parity", fail_if_called)

    with pytest.raises(ValueError, match="expected_parent_hashes.l2_macro_logical_hash mismatch"):
        run_parity_job(
            _config(tmp_path),
            expected_parent_hashes={"l2_macro_logical_hash": "other-l2-pin"},
        )


def test_run_parity_accepts_matching_request_pins(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_pin_sources(monkeypatch)
    monkeypatch.setattr(parity_runner.qc, "run_parity", lambda config: _report())
    parent_hashes = {
        "l2_macro_logical_hash": "l2-logical-hash",
        "revision_uncertainty_logical_hash": "uncertainty-logical-hash",
    }

    result = run_parity_job(
        _config(tmp_path),
        expected_input_bundle_logical_hash=a3_input_bundle_logical_hash(parent_hashes),
        expected_config_catalog_hash="catalog-hash",
        expected_parent_hashes=parent_hashes,
    )

    assert result["status"] == "succeeded"
    assert result["runtime_activation"] is False


def test_run_parity_accepts_schema_valid_request_pin_prefixes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_pin_sources(monkeypatch, catalog_hash="catalog-hash-value")
    monkeypatch.setattr(parity_runner.qc, "run_parity", lambda config: _report())
    parent_hashes = {
        "l2_macro_logical_hash": "l2-logical-hash",
        "revision_uncertainty_logical_hash": "uncertainty-logical-hash",
    }

    result = run_parity_job(
        _config(tmp_path),
        expected_input_bundle_logical_hash=a3_input_bundle_logical_hash(parent_hashes)[:16],
        expected_config_catalog_hash="catalog-hash",
        expected_parent_hashes={
            "l2_macro_logical_hash": "l2-logical-ha",
            "revision_uncertainty_logical_hash": "uncertainty-",
        },
    )

    assert result["status"] == "succeeded"


def test_run_parity_cli_threads_request_pins_to_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict = {}
    monkeypatch.setattr(engine_cli, "parity_config_from_args", lambda args: _config(tmp_path))

    def fake_run_parity_job(config, **kwargs):
        captured.update(kwargs)
        return _report() | {
            "schema_version": 1,
            "job_type": "a3_qc_parity",
            "job_id": "job-1",
            "run_fingerprint": "fingerprint",
            "status": "succeeded",
            "classification": "passed",
            "jobs": kwargs["jobs"],
            "artifact_prefix": str(tmp_path / "out"),
            "output_logical_hashes": {
                "runtime_replay_logical_hash": "0" * 64,
                "counterfactual_replay_logical_hash": "1" * 64,
                "metrics_canonical_logical_hash": "2" * 64,
                "metrics_raw_sha256": "3" * 64,
                "model_evaluation_hash": "model-hash",
            },
            "errors": [],
            "a3_status": "open_macro_v03",
        }

    monkeypatch.setattr(engine_cli, "run_parity_job", fake_run_parity_job)

    assert engine_cli.main(
        [
            "run-parity",
            "--feature-manifest",
            "feature.json",
            "--revision-uncertainty-manifest",
            "uncertainty.json",
            "--config-catalog",
            "catalog.json",
            "--a32-grid-dir",
            "grid",
            "--output-dir",
            str(tmp_path / "out"),
            "--input-bundle-logical-hash",
            "input-bundle-hash",
            "--config-catalog-hash",
            "catalog-hash",
            "--expected-l2-macro-logical-hash",
            "l2-logical-hash",
            "--expected-revision-uncertainty-logical-hash",
            "uncertainty-logical-hash",
            "--jobs",
            "4",
        ]
    ) == 0

    assert captured["expected_input_bundle_logical_hash"] == "input-bundle-hash"
    assert captured["expected_config_catalog_hash"] == "catalog-hash"
    assert captured["expected_parent_hashes"] == {
        "l2_macro_logical_hash": "l2-logical-hash",
        "revision_uncertainty_logical_hash": "uncertainty-logical-hash",
    }
    assert captured["jobs"] == 4
