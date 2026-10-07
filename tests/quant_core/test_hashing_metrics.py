from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "packages" / "investintell_quant_core" / "src"))

from investintell_quant_core.a3 import metrics
from investintell_quant_core.contracts.manifests import validate_feature_manifest_contract
from investintell_quant_core.hashing.canonical import (
    logical_payload_hash,
    logical_records_hash,
    normalize_logical_value,
)
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


def test_negative_zero_canonicalizes_to_zero() -> None:
    assert logical_payload_hash({"x": -0.0}) == logical_payload_hash({"x": 0.0})
    assert logical_records_hash([{"x": -0.0}]) == logical_records_hash([{"x": 0.0}])
    # The legacy harness canonicalizer follows the same convention.
    assert logical_records_hash([{"x": -0.0}]) == ch.logical_records_hash([{"x": -0.0}])


def test_nan_field_is_distinct_from_a_missing_field() -> None:
    nan_rows = [{"x": float("nan")}, {"x": 1.0}]
    missing_rows = [{}, {"x": 1.0}]
    null_rows = [{"x": None}, {"x": 1.0}]

    assert logical_records_hash(nan_rows) != logical_records_hash(missing_rows)
    assert logical_records_hash(nan_rows) != logical_records_hash(null_rows)
    assert logical_records_hash(nan_rows) == logical_records_hash(
        [{"x": float("nan")}, {"x": 1.0}]
    )
    assert logical_records_hash(nan_rows) == ch.logical_records_hash(nan_rows)


def test_non_finite_floats_are_tagged_and_never_collide_with_strings() -> None:
    nan, inf = float("nan"), float("inf")
    assert normalize_logical_value(nan) == {"$float": "NaN"}
    assert normalize_logical_value(inf) == {"$float": "Infinity"}
    assert normalize_logical_value(-inf) == {"$float": "-Infinity"}

    for payload, lookalike in (
        ({"x": nan}, {"x": "NaN"}),
        ({"x": nan}, {"x": "nan"}),
        ({"x": nan}, {"x": None}),
        ({"x": inf}, {"x": "inf"}),
        ({"x": inf}, {"x": "Infinity"}),
        ({"x": -inf}, {"x": "-inf"}),
        ({"x": inf}, {"x": -inf}),
        ({"x": nan}, {"x": inf}),
    ):
        assert logical_payload_hash(payload) != logical_payload_hash(lookalike)
        # The harness mirror agrees on both sides of every pair.
        assert logical_payload_hash(payload) == ch.logical_payload_hash(payload)
        assert logical_payload_hash(lookalike) == ch.logical_payload_hash(lookalike)
    # The tag is strict JSON: the harness writes normalized payloads to files.
    assert "NaN" in json.dumps(normalize_logical_value({"x": nan}), allow_nan=False)


def test_reserved_non_finite_tag_key_is_refused_on_input() -> None:
    forged = {"x": {"$float": "NaN"}}
    for hasher in (logical_payload_hash, ch.logical_payload_hash):
        with pytest.raises(ValueError, match="reserved"):
            hasher(forged)
    with pytest.raises(ValueError, match="reserved"):
        logical_records_hash([forged])


def test_metrics_hash_policy_is_versioned_for_the_v2_canonicalizer() -> None:
    assert metrics.METRICS_HASH_POLICY_VERSION == "qc_a3_metrics_float_canonical_v2"
    policy = metrics.metrics_hash_policy_payload([{"fold": "full", "value": float("nan")}], {})
    assert policy["metrics_hash_policy_version"] == "qc_a3_metrics_float_canonical_v2"
    # The v1 digest of the same row (NaN hashed as null) is not reproduced.
    assert policy["metrics_canonical_logical_hash"] != logical_records_hash(
        [{"fold": "full", "value": None}]
    )
    # The bundle evaluation hash takes the policy identifier as an input, so a
    # v1 report and a v2 report over identical metrics cannot share a digest.
    assert metrics.BUNDLE_EVALUATION_HASH_POLICY_VERSION == "qc_a3_parity_bundle_v1"


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

