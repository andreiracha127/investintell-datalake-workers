from __future__ import annotations

import copy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))
sys.path.insert(0, str(ROOT))

from investintell_quant_engine.runners.parity import result_from_report


def _report() -> dict:
    return {
        "execution_id": "exec-1",
        "a31_config_hash": "4b263d560be163fb131d9fdf",
        "a32_config_hash": "40823b2a6aba9b998109e23e",
        "parent_hashes": {
            "l2_macro_logical_hash": "9d46",
            "revision_uncertainty_logical_hash": "bc4b",
        },
        "runtime_replay_logical_hash": "de46",
        "counterfactual_replay_logical_hash": "0238",
        "metrics_canonical_logical_hash": "7001",
        "metrics_raw_sha256": "a896",
        "model_evaluation_hash": "1019",
        "comparison": {"status": "passed"},
        "runtime_activation": False,
        "freeze_ready": False,
        "a4_status": "harness_ready_provisional_A3",
        "a5_status": "blocked",
    }


def test_run_fingerprint_is_independent_of_execution_id_and_jobs() -> None:
    first = _report()
    second = copy.deepcopy(first)
    second["execution_id"] = "exec-2"

    left = result_from_report(first, job_id="job", jobs=1, artifact_prefix="out1")
    right = result_from_report(second, job_id="job", jobs=4, artifact_prefix="out2")

    assert left["execution_id"] != right["execution_id"]
    assert left["run_fingerprint"] == right["run_fingerprint"]
    assert left["runtime_activation"] is False

