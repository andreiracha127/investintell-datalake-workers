from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))

from investintell_quant_core.a3 import metrics
from investintell_quant_core.contracts.manifests import validate_feature_manifest_contract
from investintell_quant_core.hashing.canonical import logical_payload_hash, logical_records_hash
from src import calibration_harness as ch


def test_logical_records_hash_matches_legacy_harness() -> None:
    np = pytest.importorskip("numpy")
    rows = [
        {
            "business_date": dt.date(2026, 6, 25),
            "nested": {"b": 2, "a": np.float64(1.23456789012345)},
            "value": float("nan"),
        },
        {
            "business_date": dt.date(2026, 6, 24),
            "nested": {"a": 1, "b": 2},
            "value": np.float64(0.0),
        },
    ]

    assert logical_records_hash(rows) == ch.logical_records_hash(rows)


def test_logical_payload_hash_matches_legacy_harness() -> None:
    payload = {
        "policy_version": "qc_a3_parity_bundle_v1",
        "runtime_activation": False,
        "timestamp": dt.datetime(2026, 6, 26, 0, 0, tzinfo=dt.timezone.utc),
    }

    assert logical_payload_hash(payload) == ch.logical_payload_hash(payload)


def test_metric_hash_policy_canonicalizes_float_noise() -> None:
    left = [{"fold": "full", "value": 0.39246263518212093}]
    right = [{"fold": "full", "value": 0.39246263518212104}]

    assert metrics.metric_rows_logical_hash(left) == metrics.metric_rows_logical_hash(right)
    assert metrics.metric_rows_raw_sha256(left) != metrics.metric_rows_raw_sha256(right)


def test_feature_manifest_contract_rejects_counterfactual_runtime() -> None:
    manifest = {
        "parameter_independent": True,
        "counterfactual_runtime_allowed": True,
        "selection_roles": {
            "latest": "pit_runtime_candidate",
            "first_release": "revised_vintage_counterfactual",
        },
    }

    with pytest.raises(ValueError, match="counterfactual runtime"):
        validate_feature_manifest_contract(manifest)

