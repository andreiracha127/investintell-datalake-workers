"""Pure admission and CLI tests for the frozen-artifact loader."""
from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import tomllib

from src.bonds import implied_rating as policy
from src.bonds import implied_rating_artifact_loader as loader
from src.bonds.implied_rating_materializer import publication_id_for

ROOT = Path(__file__).resolve().parents[1]
PANEL_ID = "11111111-1111-4111-8111-111111111111"
PARENT_ID = "22222222-2222-4222-8222-222222222222"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rows() -> list[dict[str, Any]]:
    buckets = policy.BUCKET_ORDER
    market_levels = {
        0: None,
        1: 0.1,
        2: 1.0 / 3.0,
        3: np.nextafter(0.0, 1.0),
        4: np.finfo(np.float64).max,
    }
    rows: list[dict[str, Any]] = []
    for index, bucket in enumerate(buckets):
        witnessed = bucket not in {"WITHDRAWN", "NOT_RATED"}
        confirmed = bucket == "D"
        rows.append({
            "month": pd.Timestamp("2026-06-01"),
            "cusip_id": f"{index:09d}",
            "implied_bucket": bucket,
            "spread_norm_log": float(index) if witnessed else np.nan,
            "neutralized_score": (-0.0 if index == 0 else index + 0.125) if witnessed else np.nan,
            "market_level_l": market_levels.get(index, 0.0),
            "witnessed": witnessed,
            "carry_months": 0 if witnessed or bucket == "NOT_RATED" else 4,
            "spell_id": 1,
            "d_candidate": bucket == "B",
            "d_confirmed": confirmed,
            "d_event_month": pd.Timestamp("2026-06-01") if confirmed else pd.NaT,
            "recovery_observed": 12.5 if confirmed else np.nan,
            "censoring": (
                "default_absorbing" if confirmed else
                "withdrawal_absorbing" if bucket == "WITHDRAWN" else "none"
            ),
            "policy_version": policy.POLICY_VERSION,
            "policy_digest": policy.POLICY_DIGEST,
        })
    rows.extend((
        {
            **rows[0], "month": pd.Timestamp("2026-07-01"), "cusip_id": "000000010",
            "spread_norm_log": np.nan, "neutralized_score": np.nan,
            "market_level_l": 0.0, "witnessed": False, "carry_months": 1,
        },
        {
            **rows[7], "month": pd.Timestamp("2026-07-01"), "cusip_id": "000000011",
            "spread_norm_log": np.nan, "neutralized_score": np.nan,
            "market_level_l": 0.0, "witnessed": False, "carry_months": 1,
        },
        {
            **rows[9], "month": pd.Timestamp("2026-08-01"), "cusip_id": "000000012",
        },
    ))
    return rows


def _frame() -> pd.DataFrame:
    frame = pd.DataFrame(_rows(), columns=policy.PUBLICATION_COLUMNS)
    frame["month"] = pd.to_datetime(frame["month"])
    frame["d_event_month"] = pd.to_datetime(frame["d_event_month"])
    for name in ("witnessed", "d_candidate", "d_confirmed"):
        frame[name] = frame[name].astype(bool)
    for name in ("carry_months", "spell_id"):
        frame[name] = frame[name].astype("int64")
    for name in ("spread_norm_log", "neutralized_score", "market_level_l", "recovery_observed"):
        frame[name] = frame[name].astype("float64")
    return frame


def _table(frame: pd.DataFrame | None = None) -> pa.Table:
    frame = _frame() if frame is None else frame
    arrays = [
        pa.array(frame["month"], type=pa.timestamp("us"), from_pandas=True),
        pa.array(frame["cusip_id"], type=pa.large_string(), from_pandas=True),
        pa.array(frame["implied_bucket"], type=pa.large_string(), from_pandas=True),
        pa.array(frame["spread_norm_log"], type=pa.float64(), from_pandas=True),
        pa.array(frame["neutralized_score"], type=pa.float64(), from_pandas=True),
        pa.array(frame["market_level_l"], type=pa.float64(), from_pandas=True),
        pa.array(frame["witnessed"], type=pa.bool_(), from_pandas=True),
        pa.array(frame["carry_months"], type=pa.int64(), from_pandas=True),
        pa.array(frame["spell_id"], type=pa.int64(), from_pandas=True),
        pa.array(frame["d_candidate"], type=pa.bool_(), from_pandas=True),
        pa.array(frame["d_confirmed"], type=pa.bool_(), from_pandas=True),
        pa.array(frame["d_event_month"], type=pa.timestamp("ms"), from_pandas=True),
        pa.array(frame["recovery_observed"], type=pa.float64(), from_pandas=True),
        pa.array(frame["censoring"], type=pa.large_string(), from_pandas=True),
        pa.array(frame["policy_version"], type=pa.large_string(), from_pandas=True),
        pa.array(frame["policy_digest"], type=pa.large_string(), from_pandas=True),
    ]
    schema = pa.schema([
        pa.field(name, array.type, nullable=True)
        for name, array in zip(policy.PUBLICATION_COLUMNS, arrays, strict=True)
    ])
    return pa.Table.from_arrays(arrays, schema=schema)


def _json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


def _pin(label: str, root: Path, relative: str) -> dict[str, Any]:
    path = root / relative
    return {
        "label": label,
        "relative_path": relative,
        "size_bytes": path.stat().st_size,
        "sha256": _sha(path),
    }


def _runtime_versions() -> dict[str, str]:
    return {
        name: importlib.metadata.version(name)
        for name in ("numpy", "pandas", "pyarrow", "psycopg", "psycopg-binary")
    }


def _fixture(tmp_path: Path) -> tuple[Path, bytes, loader.FrozenArtifactContract]:
    artifact_root = tmp_path / "artifact"
    parquet_path = artifact_root / "round002_cache" / "baseline_implied_rows.parquet"
    parquet_path.parent.mkdir(parents=True)
    pq.write_table(_table(), parquet_path, row_group_size=4, compression="zstd")

    frame = _frame()
    digest = policy.rows_digest(frame)
    histogram = {bucket: int((frame["implied_bucket"] == bucket).sum()) for bucket in policy.BUCKET_ORDER}
    module_sha = _sha(ROOT / "src" / "bonds" / "implied_rating.py")
    artifact_sha = _sha(parquet_path)
    build = {
        "anchor_repr": "-0.5", "anchor_resolved": -0.5,
        "default_counts": {"d_candidate_rows": 1, "d_confirmed_rows": 2},
        "git": {"head": "fixture-revision"},
        "last_closed_month": "2026-08-01",
        "parquet": {"bytes": parquet_path.stat().st_size, "sha256": artifact_sha},
        "producer": {
            "module_sha256": module_sha, "policy_digest": policy.POLICY_DIGEST,
            "policy_version": policy.POLICY_VERSION,
        },
        "roundtrip_digest_equal": True, "rows": len(frame),
        "rows_digest": digest, "rows_digest_after_parquet_roundtrip": digest,
        "snapshot": {
            "rows": 99, "sha256": "7" * 64,
            "month_min": "2026-06-01", "month_max": "2026-08-01",
        },
    }
    export = {
        "source_pointer": PANEL_ID, "source_parent": PARENT_ID,
        "publication": {
            "publication_id": PANEL_ID, "parent_publication_id": PARENT_ID,
            "status": "validated", "config_hash": "1863d3d5fa3a0edf",
            "code_revision": "fixture-panel-repair",
        },
        "pointer_checks": {"pointer_unchanged": True, "child_validated": True},
    }
    gates = {
        "arm": "baseline",
        "data": {
            "rows_total": len(frame), "witnessed_rows_total": 8,
            "implied_bucket_distribution_total": histogram,
        },
        "d_inventory": {"d_confirmed_rows_total": 2, "d_candidate_rows_total": 1},
        "protocol": {"policy_version": policy.POLICY_VERSION, "policy_digest": policy.POLICY_DIGEST},
    }
    _json(artifact_root / "round002_cache" / "baseline_build_manifest_a.json", build)
    _json(artifact_root / "export_manifest.json", export)
    _json(artifact_root / "round002_cache" / "gate_results_baseline.json", gates)
    pins = [
        _pin("build", artifact_root, "round002_cache/baseline_build_manifest_a.json"),
        _pin("export", artifact_root, "export_manifest.json"),
        _pin("gates", artifact_root, "round002_cache/gate_results_baseline.json"),
    ]
    fingerprint = "1" * 64
    publication_id = publication_id_for(policy.POLICY_DIGEST, "fixture-revision", fingerprint)
    identity = {
        "schema_version": loader.IDENTITY_SCHEMA,
        "snapshot": {"relative_path": "snapshot.csv", "bytes": 1, "rows": 99, "sha256": "7" * 64},
        "parser": {
            "source_sha256": "8" * 64, "function_source_sha256": "6" * 64,
            "python": "fixture",
            "pandas": importlib.metadata.version("pandas"),
            "numpy": importlib.metadata.version("numpy"),
            "authority": "historical_round_002_csv_pandas",
            "type_sensitivity": "fixture type-sensitive boundary",
        },
        "producer_module_sha256": module_sha,
        "materializer_module_sha256": _sha(ROOT / "src" / "bonds" / "implied_rating_materializer.py"),
        "input_fingerprint": fingerprint,
        "publication_id": publication_id,
        "build_fingerprint": loader.build_fingerprint(
            policy.POLICY_DIGEST, "fixture-revision", fingerprint
        ),
        "stopped_run_evidence_sha256": "9" * 64,
        "artifact_sha256": artifact_sha,
        "build_manifest_sha256": pins[0]["sha256"],
        "export_manifest_sha256": pins[1]["sha256"],
        "gate_manifest_sha256": pins[2]["sha256"],
        "original_receipt_sha256": "4" * 64,
        "evidence_manifest_sha256": "5" * 64,
        "authority": "historical_round_002_csv_pandas",
        "type_sensitivity": "fixture type-sensitive boundary",
    }
    _json(artifact_root / "input_identity_receipt.json", identity)
    receipt_pin = _pin("identity", artifact_root, "input_identity_receipt.json")
    contract_dict = {
        "schema_version": loader.CONTRACT_SCHEMA,
        "status": "ready",
        "artifact": {
            "label": "artifact", "relative_path": "round002_cache/baseline_implied_rows.parquet",
            "size_bytes": parquet_path.stat().st_size, "sha256": artifact_sha,
        },
        "manifests": pins,
        "producer": {
            "revision": "fixture-revision", "policy_version": policy.POLICY_VERSION,
            "policy_digest": policy.POLICY_DIGEST, "manifest_module_sha256": module_sha,
            "sources": [{
                "relative_path": "src/bonds/implied_rating.py", "historical_sha256": module_sha,
                "accepted_runtime_sha256": [module_sha],
            }],
        },
        "arrow_schema": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in _table().schema
        ],
        "expected": {
            "row_count": len(frame), "first_month": "2026-06-01", "last_month": "2026-08-01",
            "month_count": 3, "unique_key_count": len(frame), "witnessed_count": 8,
            "d_confirmed_count": 2, "d_candidate_count": 1,
            "histogram": histogram, "rows_digest": digest,
        },
        "anchor": {"value": -0.5, "repr": "-0.5"},
        "parent": {
            "publication_id": PANEL_ID, "parent_publication_id": PARENT_ID,
            "config_hash": "1863d3d5fa3a0edf", "code_revision": "fixture-panel-repair",
            "first_month": "2026-06-01", "last_closed_month": "2026-08-01",
            "open_month": "2026-09-01", "declared_counts": [1, 1, 1, 1],
            "repair_contract": "fixture-unit-repair",
            "source_sha256_keys": [
                "bond_monthly_returns.parquet", "bond_panel_live.parquet",
                "bond_ratings_pit.parquet", "universe_snapshots_live.parquet",
            ],
            "snapshot_source_sha256": "a" * 64, "header_sha256": "b" * 64,
        },
        "identity": {
            "input_fingerprint": fingerprint, "publication_id": publication_id,
            "receipt": receipt_pin, "stopped_run_evidence_sha256": "9" * 64,
        },
        "runtime": {
            "python_minor": f"{sys.version_info.major}.{sys.version_info.minor}",
            "distributions": _runtime_versions(), "base_image": "fixture@sha256:" + "c" * 64,
            "connect_timeout_seconds": loader.CONNECT_TIMEOUT_SECONDS,
            "lock_timeout_ms": loader.LOCK_TIMEOUT_MS,
            "statement_timeout_ms": loader.SQL_TIMEOUT_MS,
            "idle_transaction_timeout_ms": loader.IDLE_TRANSACTION_TIMEOUT_MS,
            "supervised_process_cap_seconds": loader.SUPERVISED_PROCESS_CAP_SECONDS,
            "timeout_decision": loader.TIMEOUT_DECISION,
        },
        "admission_limits": {
            "max_manifest_bytes": 1_000_000, "max_row_groups": 4, "max_rows": 100,
            "max_decoded_bytes": 10_000_000, "max_footer_bytes": 1_000_000,
            "max_string_bytes": 128,
        },
    }
    raw = json.dumps(contract_dict, sort_keys=True, separators=(",", ":")).encode()
    return artifact_root, raw, loader._parse_contract(raw)


def _approve_release_context(evidence: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Provision a synthetic approval for the context as currently written.

    This stands in for the protected deployment setting only; it is never evidence
    of production review authentication.  Tests that tamper afterwards must NOT
    call this again, so the fixed approval keeps refusing the tampered bytes.
    """
    approved = _sha(evidence / "release-context.json")
    monkeypatch.setenv(loader.APPROVED_RELEASE_CONTEXT_ENV, approved)
    return approved


def _write_release_context(
    evidence: Path, contract: loader.FrozenArtifactContract, root: Path = ROOT
) -> dict[str, Any]:
    evidence.mkdir(parents=True, exist_ok=True)
    source_sha = {
        relative: _sha(root / relative)
        for relative in loader._RELEASE_SOURCE_PATHS
    }
    runtime_root = root / "docker" / "bond-implied-artifact-loader"
    document = {
        "schema_version": "bond_market_implied_rating_artifact_release/1",
        "target": "production",
        "loader_commit": "1" * 40,
        "loader_tree": "2" * 40,
        "review_changeset_sha256": "3" * 64,
        "review_report_sha256": "4" * 64,
        "context_archive_sha256": "5" * 64,
        "inventory_sha256": "6" * 64,
        "exclusion_evidence_sha256": "7" * 64,
        "runtime_base_image": contract.runtime.base_image,
        "requirements_lock_sha256": _sha(runtime_root / "requirements.lock"),
        "dockerfile_sha256": _sha(runtime_root / "Dockerfile"),
        "railway_toml_sha256": _sha(runtime_root / "railway.toml"),
        "source_sha256": source_sha,
    }
    _json(evidence / "release-context.json", document)
    return document


def test_checked_in_contract_binds_verified_identity_receipt() -> None:
    contract = loader.load_frozen_contract()
    assert contract.status is loader.ContractStatus.READY
    assert contract.identity is not None
    assert contract.identity.input_fingerprint == (
        "620760bf4de0584d44138d880ff57bf87d3ffa24ef205074d0b004d8874b9867"
    )
    assert contract.identity.publication_id == "bc13a5e4-7f1a-54fa-8a5b-68862df4020b"
    # Production C/H header digest from the read-only capture receipt
    # 53edcee77d317e7236b380a2e7f9d97416875a6d9350266399c2e6d4db806ab3.
    assert contract.parent.header_sha256 == (
        "faf310864329a7dbe43e7ea3dcd1a55f043683914945bac0b16f6e9bfa982c69"
    )
    loader._require_ready(contract)
    receipt, evidence = loader._read_pinned_json(
        loader.ROOT, contract.identity.receipt, contract.limits
    )
    assert evidence.sha256 == contract.identity.receipt.sha256
    assert receipt["materializer_module_sha256"] == _sha(
        ROOT / "src" / "bonds" / "implied_rating_materializer.py"
    )
    assert loader._verify_sources(contract)
    assert contract.runtime.statement_timeout_ms == 7_200_000
    assert contract.runtime.idle_transaction_timeout_ms == 7_200_000
    assert contract.runtime.lock_timeout_ms == 5_000
    assert contract.runtime.connect_timeout_seconds == 15
    assert contract.runtime.supervised_process_cap_seconds == 9_000
    assert "rollback 1991s and apply 1229s" in contract.runtime.timeout_decision


def test_complete_fixture_is_admitted_exactly(tmp_path: Path) -> None:
    root, raw, contract = _fixture(tmp_path)
    artifact = loader._load_verified_artifact(
        root, contract=contract, contract_sha256=hashlib.sha256(raw).hexdigest()
    )
    assert artifact.summary.row_count == 13
    assert artifact.summary.unique_key_count == 13
    assert artifact.summary.witnessed_count == 8
    assert artifact.summary.d_confirmed_count == 2
    assert artifact.table["cusip_id"][0].as_py() == "000000000"
    assert artifact.publication.publication_id == contract.identity.publication_id


@pytest.mark.parametrize("mutation", ["flip", "truncate", "append"])
def test_artifact_byte_changes_fail_before_decode(tmp_path: Path, mutation: str) -> None:
    root, raw, contract = _fixture(tmp_path)
    path = root / contract.artifact.relative_path
    data = bytearray(path.read_bytes())
    if mutation == "flip":
        data[len(data) // 2] ^= 1
    elif mutation == "truncate":
        data = data[:-1]
    else:
        data.extend(b"x")
    path.write_bytes(data)
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._load_verified_artifact(
            root, contract=contract, contract_sha256=hashlib.sha256(raw).hexdigest()
        )
    assert exc.value.code == loader.ErrorCode.FILE_HASH_MISMATCH.value


def test_symlink_artifact_is_refused(tmp_path: Path) -> None:
    root, _, contract = _fixture(tmp_path)
    path = root / contract.artifact.relative_path
    target = root / "real.parquet"
    path.replace(target)
    try:
        path.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._read_parquet(root, contract)
    assert exc.value.code == loader.ErrorCode.FILE_UNSAFE.value


@pytest.mark.parametrize(
    "raw",
    [
        b'{"schema_version":"x","schema_version":"y"}',
        b'{"value":NaN}',
        b'{"value":Infinity}',
        b"\xff",
    ],
)
def test_strict_json_rejects_duplicate_nonfinite_and_invalid_utf8(raw: bytes) -> None:
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._strict_json(raw, label="test")
    assert exc.value.code == loader.ErrorCode.JSON_INVALID.value


def test_contract_rejects_boolean_count_and_partial_identity(tmp_path: Path) -> None:
    _, raw, _ = _fixture(tmp_path)
    document = json.loads(raw)
    document["expected"]["row_count"] = True
    with pytest.raises(loader.ArtifactLoaderError):
        loader._parse_contract(json.dumps(document).encode())
    document = json.loads(raw)
    document["identity"].pop("publication_id")
    with pytest.raises(loader.ArtifactLoaderError):
        loader._parse_contract(json.dumps(document).encode())


def test_identity_uuid_mismatch_is_refused(tmp_path: Path) -> None:
    _, raw, _ = _fixture(tmp_path)
    document = json.loads(raw)
    document["identity"]["publication_id"] = "33333333-3333-4333-8333-333333333333"
    contract = loader._parse_contract(json.dumps(document).encode())
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._require_ready(contract)
    assert exc.value.code == loader.ErrorCode.IDENTITY_MISMATCH.value


@pytest.mark.parametrize(
    "transform",
    [
        lambda table: table.drop(["market_level_l"]),
        lambda table: table.append_column("extra", pa.array([1] * table.num_rows, type=pa.int64())),
        lambda table: table.select(list(reversed(range(table.num_columns)))),
        lambda table: table.set_column(7, "carry_months", pa.array([0] * table.num_rows, type=pa.int32())),
        lambda table: table.set_column(
            0, "month", pa.array(_frame()["month"], type=pa.timestamp("us", tz="UTC"))
        ),
    ],
)
def test_exact_arrow_schema_is_required(tmp_path: Path, transform: Any) -> None:
    _, _, contract = _fixture(tmp_path)
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._verify_arrow_schema(transform(_table()).schema, contract)
    assert exc.value.code == loader.ErrorCode.ARROW_SCHEMA_MISMATCH.value


def test_valid_nan_is_not_confused_with_arrow_null(tmp_path: Path) -> None:
    _, _, contract = _fixture(tmp_path)
    table = _table()
    values = table["spread_norm_log"].combine_chunks().to_pylist()
    values[0] = float("nan")
    array = pa.array(values, type=pa.float64(), from_pandas=False)
    bad = table.set_column(3, "spread_norm_log", array)
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._admit_arrow(bad, contract)
    assert exc.value.code == loader.ErrorCode.ROW_INVALID.value
    assert table["spread_norm_log"].null_count == 5
    loader._admit_arrow(table, contract)


def test_required_arrow_null_and_runtime_mismatch_are_typed(tmp_path: Path) -> None:
    _, _, contract = _fixture(tmp_path)
    table = _table()
    witnessed = table["witnessed"].combine_chunks().to_pylist()
    witnessed[0] = None
    bad = table.set_column(6, "witnessed", pa.array(witnessed, type=pa.bool_()))
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._admit_arrow(bad, contract)
    assert exc.value.code == loader.ErrorCode.ROW_INVALID.value
    assert exc.value.details == {"field": "row.required_null.witnessed"}
    wrong_runtime = replace(
        contract,
        runtime=replace(contract.runtime, python_minor="0.0"),
    )
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._verify_runtime(wrong_runtime)
    assert exc.value.code == loader.ErrorCode.RUNTIME_MISMATCH.value


def test_runtime_source_pin_and_resource_bounds_fail_closed(tmp_path: Path) -> None:
    root, _, contract = _fixture(tmp_path)
    bad_source = replace(
        contract,
        sources=(replace(contract.sources[0], accepted_runtime_sha256=("0" * 64,)),),
    )
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._verify_sources(bad_source)
    assert exc.value.code == loader.ErrorCode.SOURCE_MISMATCH.value
    tiny_footer = replace(
        contract,
        limits=replace(contract.limits, max_footer_bytes=1),
    )
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._read_parquet(root, tiny_footer)
    assert exc.value.code == loader.ErrorCode.RESOURCE_LIMIT.value


def test_oversized_utf8_value_is_refused_before_pandas_conversion(tmp_path: Path) -> None:
    _, _, contract = _fixture(tmp_path)
    bounded = replace(contract, limits=replace(contract.limits, max_string_bytes=8))
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._admit_arrow(_table(), bounded)
    assert exc.value.code == loader.ErrorCode.RESOURCE_LIMIT.value


@pytest.mark.parametrize(
    ("field", "value", "code_field"),
    [
        ("cusip_id", "bad", "row.cusip_id"),
        ("month", pd.Timestamp("2026-06-02"), "row.month"),
        ("carry_months", -1, "row.carry_months"),
        ("spell_id", 0, "row.spell_id"),
        ("policy_version", "wrong", "row.policy_version"),
        ("censoring", "wrong", "row.censoring"),
        ("recovery_observed", -1.0, "row.recovery"),
    ],
)
def test_row_domain_failures_are_typed(
    tmp_path: Path, field: str, value: Any, code_field: str
) -> None:
    _, _, contract = _fixture(tmp_path)
    frame = _frame()
    frame.loc[0, field] = value
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._validate_frame(frame, contract, compare_expected=False)
    assert exc.value.details == {"field": code_field}


def test_duplicate_key_and_month_hole_are_refused(tmp_path: Path) -> None:
    _, _, contract = _fixture(tmp_path)
    duplicate = _frame()
    duplicate.loc[1, ["month", "cusip_id"]] = duplicate.loc[0, ["month", "cusip_id"]]
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._validate_frame(duplicate, contract, compare_expected=False)
    assert exc.value.code == loader.ErrorCode.ROW_KEY_MISMATCH.value
    missing = _frame().loc[lambda frame: frame["month"] != pd.Timestamp("2026-07-01")]
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._validate_frame(missing, contract, compare_expected=False)
    assert exc.value.details == {"field": "row.month_window"}


def test_histogram_and_logical_digest_drift_are_distinct(tmp_path: Path) -> None:
    _, _, contract = _fixture(tmp_path)
    wrong_bucket = _frame()
    wrong_bucket.loc[0, "implied_bucket"] = "AA"
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._validate_frame(wrong_bucket, contract)
    assert exc.value.code == loader.ErrorCode.ROW_SUMMARY_MISMATCH.value
    wrong_value = _frame()
    wrong_value.loc[0, "neutralized_score"] = 99.0
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._validate_frame(wrong_value, contract)
    assert exc.value.code == loader.ErrorCode.ROW_DIGEST_MISMATCH.value


def test_manifest_tuple_drift_and_receipt_source_drift_fail(tmp_path: Path) -> None:
    root, _, contract = _fixture(tmp_path)
    documents = {
        pin.label: loader._read_pinned_json(root, pin, contract.limits)[0]
        for pin in contract.manifests
    }
    documents["build"]["anchor_repr"] = "-0.4"
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._verify_manifests(contract, documents)
    assert exc.value.code == loader.ErrorCode.MANIFEST_MISMATCH.value
    documents["build"]["anchor_repr"] = "-0.5"
    identity = loader._read_pinned_json(root, contract.identity.receipt, contract.limits)[0]
    identity["producer_module_sha256"] = "f" * 64
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._verify_identity_receipt(
            identity,
            contract,
            {pin.label: pin for pin in contract.manifests},
            documents,
        )
    assert exc.value.code == loader.ErrorCode.IDENTITY_MISMATCH.value


def _load_cli() -> ModuleType:
    path = ROOT / "scripts" / "load_bond_market_implied_rating_artifact.py"
    spec = importlib.util.spec_from_file_location("artifact_loader_cli", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cli_default_verify_only_never_resolves_dsn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    cli = _load_cli()
    monkeypatch.setattr(
        cli.loader,
        "load_verified_artifact",
        lambda _: (_ for _ in ()).throw(
            loader.ArtifactLoaderError(loader.ErrorCode.MISSING_IDENTITY, field="identity")
        ),
    )
    monkeypatch.setattr(cli, "resolve_dsn", lambda: pytest.fail("DSN must not be read"))
    code = cli.main([
        "--artifact-root", str(tmp_path), "--evidence-dir", str(tmp_path / "evidence")
    ])
    assert code == 3
    stderr = capsys.readouterr().err
    assert "missing_identity" in stderr
    receipts = list((tmp_path / "evidence").glob("*-failure-*.json"))
    assert len(receipts) == 1
    assert '"mode":"verify-only"' in receipts[0].read_text(encoding="utf-8")


def test_cli_missing_dsn_is_sanitized_and_receipted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = _load_cli()
    root, raw, contract = _fixture(tmp_path)
    artifact = loader._load_verified_artifact(
        root, contract=contract, contract_sha256=hashlib.sha256(raw).hexdigest()
    )
    monkeypatch.setattr(cli.loader, "load_verified_artifact", lambda _: artifact)
    monkeypatch.setattr(cli.loader, "_contract_from_artifact", lambda _: contract)
    for name in (
        "DATABASE_URL", "DB_TLS_CA_PEM", "DB_TLS_CERT_PEM", "DB_TLS_KEY_PEM"
    ):
        monkeypatch.delenv(name, raising=False)
    evidence = tmp_path / "dsn-evidence"
    code = cli.main([
        "--artifact-root", str(root), "--evidence-dir", str(evidence), "--dry-run"
    ])
    assert code == 3
    stderr = capsys.readouterr().err
    assert json.loads(stderr) == {"code": "configuration_error", "state": "refused"}
    receipt = json.loads(next(evidence.glob("*-failure-*.json")).read_text())
    assert receipt["code"] == "configuration_error"
    assert receipt["failure_phase"] == "connect"
    assert receipt["transaction_outcome"] == "not_started"
    assert "DATABASE_URL" not in stderr


def test_cli_missing_artifact_file_is_sanitized_and_receipted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cli = _load_cli()
    evidence = tmp_path / "file-evidence"
    missing = tmp_path / "sensitive-customer-path"
    code = cli.main([
        "--artifact-root", str(missing), "--evidence-dir", str(evidence), "--verify-only"
    ])
    assert code == 3
    stderr = capsys.readouterr().err
    assert json.loads(stderr) == {"code": "file_unsafe", "state": "refused"}
    assert str(missing) not in stderr
    receipt = json.loads(next(evidence.glob("*-failure-*.json")).read_text())
    assert receipt["failure_phase"] == "offline_verify"
    assert str(missing) not in json.dumps(receipt)


def test_cli_missing_runtime_contract_is_sanitized_and_receipted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = _load_cli()
    missing_contract = tmp_path / "private-contract-name.json"
    monkeypatch.setattr(cli.loader, "CONTRACT_PATH", missing_contract)
    evidence = tmp_path / "contract-evidence"
    code = cli.main([
        "--artifact-root", str(tmp_path), "--evidence-dir", str(evidence), "--verify-only"
    ])
    assert code == 3
    stderr = capsys.readouterr().err
    assert json.loads(stderr) == {"code": "contract_invalid", "state": "refused"}
    assert str(missing_contract) not in stderr
    receipt = json.loads(next(evidence.glob("*-failure-*.json")).read_text())
    assert receipt["failure_phase"] == "offline_verify"
    assert str(missing_contract) not in json.dumps(receipt)


def test_cli_missing_release_context_is_sanitized_before_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = _load_cli()
    root, raw, contract = _fixture(tmp_path)
    artifact = loader._load_verified_artifact(
        root, contract=contract, contract_sha256=hashlib.sha256(raw).hexdigest()
    )
    monkeypatch.setattr(cli.loader, "load_verified_artifact", lambda _: artifact)
    monkeypatch.setattr(cli.loader, "_contract_from_artifact", lambda _: contract)
    monkeypatch.setattr(cli, "resolve_dsn", lambda: pytest.fail("DB must not be opened"))
    evidence = tmp_path / "release-evidence"
    code = cli.main([
        "--artifact-root", str(root), "--evidence-dir", str(evidence), "--apply"
    ])
    assert code == 3
    stderr = capsys.readouterr().err
    assert json.loads(stderr) == {"code": "receipt_failure", "state": "refused"}
    receipt = json.loads(next(evidence.glob("*-failure-*.json")).read_text())
    assert receipt["failure_phase"] == "release_context"
    assert receipt["schema_installed"] is None
    assert receipt["transaction_outcome"] == "not_started"


def test_cli_committed_evidence_failure_is_recovery_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = _load_cli()
    root, raw, contract = _fixture(tmp_path)
    artifact = loader._load_verified_artifact(
        root, contract=contract, contract_sha256=hashlib.sha256(raw).hexdigest()
    )
    monkeypatch.setattr(cli.loader, "load_verified_artifact", lambda _: artifact)

    def fail_after_commit(*_: object, **__: object) -> loader.OperationResult:
        raise loader.ArtifactLoaderError(
            loader.ErrorCode.COMMITTED_EVIDENCE_INCOMPLETE,
            field="receipt.final_readback",
            receipt_written=False,
            phase="final_receipt",
            schema_installed=True,
            transaction_outcome="committed",
            outcome="recovery_required",
        )

    monkeypatch.setattr(cli.loader, "publish_verified_artifact", fail_after_commit)
    monkeypatch.setattr(
        cli.loader,
        "persist_failure_receipt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            loader.ArtifactLoaderError(
                loader.ErrorCode.RECEIPT_FAILURE, field="receipt.unavailable"
            )
        ),
    )
    code = cli.main([
        "--artifact-root", str(root),
        "--evidence-dir", str(tmp_path / "recovery-evidence"),
        "--apply",
    ])
    assert code == 5
    assert json.loads(capsys.readouterr().err) == {
        "code": "committed_evidence_incomplete",
        "state": "recovery_required",
    }


@pytest.mark.parametrize("flag", ["--dsn", "--force", "--contract", "--revision", "--app"])
def test_cli_has_no_override_or_abbreviated_flags(flag: str, tmp_path: Path) -> None:
    cli = _load_cli()
    with pytest.raises(SystemExit) as exc:
        cli.main([
            "--artifact-root", str(tmp_path), "--evidence-dir", str(tmp_path), flag, "x"
        ])
    assert exc.value.code == 2


def test_receipts_are_immutable_and_sanitized(tmp_path: Path) -> None:
    first = loader._persist_receipt(tmp_path, phase="test", payload=b'{"safe":true}')
    second = loader._persist_receipt(tmp_path, phase="test", payload=b'{"safe":true}')
    assert first.basename != second.basename
    assert len(list(tmp_path.glob("*.json"))) == 2
    assert "password" not in (tmp_path / first.basename).read_text().lower()


def test_apply_requires_reviewed_release_context_before_database(tmp_path: Path) -> None:
    _, _, contract = _fixture(tmp_path)
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._load_release_evidence(tmp_path / "missing", contract)
    assert exc.value.code == loader.ErrorCode.RECEIPT_FAILURE.value


def test_complete_release_context_verifies_runtime_image_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, contract = _fixture(tmp_path)
    evidence = tmp_path / "evidence"
    document = _write_release_context(evidence, contract)
    _approve_release_context(evidence, monkeypatch)
    release = loader._load_release_evidence(evidence, contract)
    assert release.loader_commit == "1" * 40
    assert release.release_context_sha256 == _sha(evidence / "release-context.json")
    assert release.railway_toml_sha256 == document["railway_toml_sha256"]


# Independent literal: the release inventory must be exactly this set.  It is not
# derived from the loader constant, the Dockerfile or an import trace, so a change
# to any one of them has to be reconciled with the others by review.
_EXPECTED_RELEASE_SOURCES = frozenset({
    "src/__init__.py",
    "src/db.py",
    "src/bonds/__init__.py",
    "src/bonds/cashflows.py",
    "src/bonds/debt_mapping.py",
    "src/bonds/errors.py",
    "src/bonds/identifiers.py",
    "src/bonds/implied_rating.py",
    "src/bonds/implied_rating_artifact_loader.py",
    "src/bonds/implied_rating_materializer.py",
    "src/bonds/matching.py",
    "src/bonds/oas.py",
    "src/bonds/panel_states.py",
    "src/bonds/pricing.py",
    "src/bonds/states.py",
    "scripts/__init__.py",
    "scripts/load_bond_market_implied_rating_artifact.py",
    "schemas/sec_derived_publications.sql",
    "schemas/bond_market_implied_rating_v1.sql",
    "contracts/bond_market_implied_rating_round002_artifact.json",
    "contracts/bond_market_implied_rating_round002_input_identity.json",
})
_RUNTIME_DIR = "docker/bond-implied-artifact-loader"
_RUNTIME_FILES = frozenset({
    f"{_RUNTIME_DIR}/Dockerfile",
    f"{_RUNTIME_DIR}/requirements.lock",
    f"{_RUNTIME_DIR}/railway.toml",
})
_BOOTSTRAP = f"{_RUNTIME_DIR}/bootstrap_evidence.py"


def _dockerfile_copy_sources() -> set[str]:
    text = (ROOT / _RUNTIME_DIR / "Dockerfile").read_text(encoding="utf-8")
    logical = re.sub(r"\\\n", " ", text)
    sources: set[str] = set()
    for line in logical.splitlines():
        tokens = line.split()
        if tokens and tokens[0] == "COPY":
            assert not any(token.startswith("--") for token in tokens[1:]), line
            sources.update(tokens[1:-1])
    return sources


def test_release_inventory_is_the_exact_reviewed_set() -> None:
    assert len(loader._RELEASE_SOURCE_PATHS) == len(set(loader._RELEASE_SOURCE_PATHS))
    assert set(loader._RELEASE_SOURCE_PATHS) == _EXPECTED_RELEASE_SOURCES
    for relative in _EXPECTED_RELEASE_SOURCES:
        assert (ROOT / relative).is_file(), relative


def test_release_inventory_equals_every_copied_application_file() -> None:
    sources = _dockerfile_copy_sources()
    # Everything the image copies is either inventory, a runtime file bound by its
    # own release-context digest, the digest-pinned bootstrap, or the artifact.
    assert sources - _RUNTIME_FILES - {_BOOTSTRAP, "artifact/"} == set(
        loader._RELEASE_SOURCE_PATHS
    )
    assert _RUNTIME_FILES | {_BOOTSTRAP} <= sources


def test_release_inventory_covers_fresh_process_cli_import_closure() -> None:
    probe = (
        "import sys\n"
        "from pathlib import Path\n"
        "root = Path(sys.argv[1]).resolve()\n"
        "sys.path.insert(0, str(root))\n"
        "import scripts.load_bond_market_implied_rating_artifact\n"
        "found = set()\n"
        "for module in list(sys.modules.values()):\n"
        "    origin = getattr(module, '__file__', None)\n"
        "    if origin and Path(origin).resolve().is_relative_to(root):\n"
        "        found.add(Path(origin).resolve().relative_to(root).as_posix())\n"
        "print('\\n'.join(sorted(found)))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-B", "-c", probe, str(ROOT)],
        capture_output=True, text=True, timeout=120, check=True,
    )
    loaded = set(completed.stdout.split())
    python_inventory = {
        relative for relative in loader._RELEASE_SOURCE_PATHS if relative.endswith(".py")
    }
    # The eager import graph (src/bonds/__init__.py pulls the pure bond modules) is
    # exactly the Python inventory: nothing loaded is unhashed, nothing hashed is dead.
    assert loaded == python_inventory
    assert "src/bonds/matching.py" in loaded and "src/__init__.py" in loaded


def _release_tree(tmp_path: Path) -> Path:
    root = tmp_path / "release-root"
    for relative in (*loader._RELEASE_SOURCE_PATHS, *_RUNTIME_FILES, _BOOTSTRAP):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    return root


@pytest.mark.parametrize("relative", sorted(_EXPECTED_RELEASE_SOURCES))
def test_release_context_refuses_every_changed_inventory_file(
    relative: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, contract = _fixture(tmp_path)
    root = _release_tree(tmp_path)
    evidence = tmp_path / "evidence"
    _write_release_context(evidence, contract, root)
    _approve_release_context(evidence, monkeypatch)
    monkeypatch.setattr(loader, "ROOT", root)
    assert loader._load_release_evidence(evidence, contract).loader_commit == "1" * 40
    with (root / relative).open("ab") as handle:
        handle.write(b"\n# drift\n")
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._load_release_evidence(evidence, contract)
    assert exc.value.code == loader.ErrorCode.RECEIPT_FAILURE.value
    assert exc.value.details.get("field") == f"release_context.source.{relative}"


@pytest.mark.parametrize("change", ["missing", "extra"])
def test_release_context_inventory_keys_must_match_exactly(
    change: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Even a context the deployer approved as-is is refused when its inventory
    # does not cover exactly the reviewed release closure.
    _, _, contract = _fixture(tmp_path)
    evidence = tmp_path / "evidence"
    document = _write_release_context(evidence, contract)
    if change == "missing":
        document["source_sha256"].pop("src/bonds/matching.py")
    else:
        document["source_sha256"]["src/workers/bond_market_implied_rating.py"] = "0" * 64
    _json(evidence / "release-context.json", document)
    _approve_release_context(evidence, monkeypatch)
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._load_release_evidence(evidence, contract)
    assert exc.value.details.get("field") == "release_context.source_inventory"


def test_release_context_binds_bootstrap_wrapper_to_dockerfile_literal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, contract = _fixture(tmp_path)
    root = _release_tree(tmp_path)
    evidence = tmp_path / "evidence"
    _write_release_context(evidence, contract, root)
    _approve_release_context(evidence, monkeypatch)
    monkeypatch.setattr(loader, "ROOT", root)
    loader._load_release_evidence(evidence, contract)
    with (root / _BOOTSTRAP).open("ab") as handle:
        handle.write(b"\n")
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._load_release_evidence(evidence, contract)
    assert exc.value.code == loader.ErrorCode.RECEIPT_FAILURE.value
    assert exc.value.details.get("field") == "release_context.bootstrap_sha256"
    # A Dockerfile without exactly one reviewed literal is refused even when the
    # release context was built from it.
    shutil.copyfile(ROOT / _BOOTSTRAP, root / _BOOTSTRAP)
    dockerfile = root / _RUNTIME_DIR / "Dockerfile"
    dockerfile.write_bytes(dockerfile.read_bytes().replace(b"sha256sum -c -", b"true"))
    _write_release_context(evidence, contract, root)
    # Approved as a new context on purpose: the literal rule refuses independently.
    _approve_release_context(evidence, monkeypatch)
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._load_release_evidence(evidence, contract)
    assert exc.value.details.get("field") == "release_context.bootstrap_literal"


# --- r4081728349: independently approved release-context trust root -------------------


def _approval_refused(evidence: Path, contract: loader.FrozenArtifactContract) -> None:
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._load_release_evidence(evidence, contract)
    assert exc.value.code == loader.ErrorCode.RECEIPT_FAILURE.value
    assert exc.value.details == {"field": "release_context.approval"}


def test_release_context_approval_is_absent_by_default_and_never_file_derived(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, contract = _fixture(tmp_path)
    evidence = tmp_path / "evidence"
    _write_release_context(evidence, contract)
    monkeypatch.delenv(loader.APPROVED_RELEASE_CONTEXT_ENV, raising=False)
    _approval_refused(evidence, contract)
    # Absent approval refuses even before the context is opened.
    _approval_refused(tmp_path / "no-such-evidence", contract)
    source = (ROOT / "src" / "bonds" / "implied_rating_artifact_loader.py").read_text()
    assert source.count(loader.APPROVED_RELEASE_CONTEXT_ENV) == 1
    for path in (
        ROOT / "docker" / "bond-implied-artifact-loader" / "Dockerfile",
        ROOT / "docker" / "bond-implied-artifact-loader" / "railway.toml",
        ROOT / "scripts" / "load_bond_market_implied_rating_artifact.py",
    ):
        assert loader.APPROVED_RELEASE_CONTEXT_ENV not in path.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "value",
    ["", "B" * 64, "b" * 63, "b" * 65, "b" * 64 + "\n", " " + "b" * 64, "g" * 64],
)
def test_malformed_release_context_approval_refuses(
    value: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, contract = _fixture(tmp_path)
    evidence = tmp_path / "evidence"
    _write_release_context(evidence, contract)
    genuine = _approve_release_context(evidence, monkeypatch)
    assert loader._load_release_evidence(evidence, contract).release_context_sha256 == genuine
    # Case, surrounding whitespace or a trailing newline around the genuine hash is
    # still malformed; so is any other value.
    for candidate in (value, value.replace("b" * 64, genuine).replace("B" * 64, genuine.upper())):
        if candidate == genuine:
            continue
        monkeypatch.setenv(loader.APPROVED_RELEASE_CONTEXT_ENV, candidate)
        _approval_refused(evidence, contract)


_SELF_ATTESTED_FIELDS = (
    "loader_commit", "loader_tree", "review_changeset_sha256", "review_report_sha256",
    "context_archive_sha256", "inventory_sha256", "exclusion_evidence_sha256",
)


@pytest.mark.parametrize("field", _SELF_ATTESTED_FIELDS)
def test_fixed_approval_refuses_any_changed_self_attested_field(
    field: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, contract = _fixture(tmp_path)
    evidence = tmp_path / "evidence"
    document = _write_release_context(evidence, contract)
    _approve_release_context(evidence, monkeypatch)
    loader._load_release_evidence(evidence, contract)
    width = 40 if field in {"loader_commit", "loader_tree"} else 64
    document[field] = "e" * width  # syntactically valid, different value
    _json(evidence / "release-context.json", document)
    _approval_refused(evidence, contract)


def test_fixed_approval_refuses_changed_source_with_matching_self_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, contract = _fixture(tmp_path)
    root = _release_tree(tmp_path)
    evidence = tmp_path / "evidence"
    _write_release_context(evidence, contract, root)
    _approve_release_context(evidence, monkeypatch)
    monkeypatch.setattr(loader, "ROOT", root)
    loader._load_release_evidence(evidence, contract)
    target = root / "src" / "bonds" / "implied_rating_artifact_loader.py"
    with target.open("ab") as handle:
        handle.write(b"\n# attacker change\n")
    # The attacker also rewrites the context so every self-attested hash matches.
    _write_release_context(evidence, contract, root)
    _approval_refused(evidence, contract)


@pytest.mark.parametrize("variant", ["reordered", "whitespace", "trailing_newline"])
def test_approval_pins_raw_bytes_not_json_meaning(
    variant: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, contract = _fixture(tmp_path)
    evidence = tmp_path / "evidence"
    document = _write_release_context(evidence, contract)
    _approve_release_context(evidence, monkeypatch)
    path = evidence / "release-context.json"
    if variant == "reordered":
        raw = json.dumps(dict(reversed(list(document.items())))).encode()
    elif variant == "whitespace":
        raw = json.dumps(document, indent=4, sort_keys=True).encode()
    else:
        raw = path.read_bytes() + b"\n"
    assert json.loads(raw) == document
    path.write_bytes(raw)
    _approval_refused(evidence, contract)


def test_publish_refuses_unapproved_context_before_receipt_or_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, contract = _genuine(tmp_path, monkeypatch)
    evidence = tmp_path / "evidence"
    document = _write_release_context(evidence, contract)
    _approve_release_context(evidence, monkeypatch)
    document["review_report_sha256"] = "e" * 64
    _json(evidence / "release-context.json", document)
    _NoDatabase.calls = 0
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader.publish_verified_artifact(
            artifact, evidence_dir=evidence, connection_factory=_NoDatabase()
        )
    assert exc.value.details == {"field": "release_context.approval"}
    assert _NoDatabase.calls == 0
    names = [path.name for path in evidence.glob("*.json") if path.name != "release-context.json"]
    assert not [name for name in names if "-pre-apply-" in name]


# --- r4081728318: durable receipt directory entry -------------------------------------

requires_directory_fsync = pytest.mark.skipif(
    not all(hasattr(os, name) for name in ("O_DIRECTORY", "O_NOFOLLOW")),
    reason="receipt durability requires POSIX directory descriptors",
)


@pytest.mark.parametrize("missing", ["O_DIRECTORY", "open_dir_fd", "mkdir_dir_fd"])
def test_receipts_refuse_where_directory_fsync_is_unsupported(
    missing: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if missing == "O_DIRECTORY":
        monkeypatch.delattr(loader.os, "O_DIRECTORY", raising=False)
    else:
        unsupported = loader.os.open if missing == "open_dir_fd" else loader.os.mkdir
        monkeypatch.setattr(
            loader.os, "supports_dir_fd",
            {function for function in loader.os.supports_dir_fd if function is not unsupported},
        )
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._persist_receipt(tmp_path, phase="test", payload=b"{}")
    assert exc.value.details == {"field": "receipt.directory_unsupported"}
    assert not list(tmp_path.glob("*.json"))


class _FsSpy:
    """Record the durability-relevant syscalls and optionally fault one of them.

    fsync kinds are ``file``, ``dir`` (the evidence directory) and ``parent`` (the
    directory holding the evidence directory's entry, identified by device/inode).
    ``fail`` is read at call time, so a test can clear it between attempts.
    """

    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        fail: str | None = None,
        *,
        evidence: Path,
    ) -> None:
        self.events: list[tuple[str, str]] = []
        self.open_fds: set[int] = set()
        self.fail = fail
        self.fsync_count = 0
        kinds: dict[int, str] = {}

        def fsync_kind(fd: int) -> str:
            kind = kinds.get(fd, "dir")
            if kind == "dir" and os.path.samestat(os.fstat(fd), os.stat(evidence.parent)):
                return "parent"
            return kind

        def spy_open(path: str, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
            kind = "dir" if flags & os.O_DIRECTORY else "file"
            if self.fail == f"open_{kind}":
                raise OSError("injected open failure")
            fd = os.open(path, flags, mode, dir_fd=dir_fd)
            kinds[fd] = kind
            self.open_fds.add(fd)
            self.events.append(("open", kind))
            return fd

        def spy_close(fd: int) -> None:
            self.open_fds.discard(fd)
            os.close(fd)

        def spy_fsync(fd: int) -> None:
            kind = fsync_kind(fd)
            self.fsync_count += 1
            self.events.append(("fsync", kind))
            if self.fail == f"fsync_{kind}":
                raise OSError("injected fsync failure")
            os.fsync(fd)

        def spy_read(fd: int, size: int) -> bytes:
            data = os.read(fd, size)
            return b"tampered" if self.fail == "readback" else data

        def spy_mkdir(name: str, mode: int = 0o777, *, dir_fd: int | None = None) -> None:
            self.events.append(("mkdir", name))
            os.mkdir(name, mode, dir_fd=dir_fd)

        monkeypatch.setattr(loader._DurableFs, "open", staticmethod(spy_open))
        monkeypatch.setattr(loader._DurableFs, "close", staticmethod(spy_close))
        monkeypatch.setattr(loader._DurableFs, "fsync", staticmethod(spy_fsync))
        monkeypatch.setattr(loader._DurableFs, "read", staticmethod(spy_read))
        monkeypatch.setattr(loader._DurableFs, "mkdir", staticmethod(spy_mkdir))


@requires_directory_fsync
def test_receipt_fsyncs_file_then_directory_before_returning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spy = _FsSpy(monkeypatch, evidence=tmp_path)
    ref = loader._persist_receipt(tmp_path, phase="test", payload=b'{"x":1}')
    fsyncs = [event for event in spy.events if event[0] == "fsync"]
    # A local (same-device) evidence directory has its own entry synced into its
    # parent on every admission, before the receipt file is even created; the file
    # is then synced before the directory entry that names it.
    assert fsyncs == [("fsync", "parent"), ("fsync", "file"), ("fsync", "dir")]
    assert spy.events.index(("fsync", "parent")) < spy.events.index(("open", "file"))
    assert spy.events.index(("fsync", "file")) < spy.events.index(("fsync", "dir"))
    assert spy.open_fds == set()
    path = tmp_path / ref.basename
    assert path.read_bytes() == b'{"x":1}'
    assert oct(path.stat().st_mode & 0o777) == "0o600"


@requires_directory_fsync
@pytest.mark.parametrize(
    "fault", ["open_dir", "open_file", "fsync_parent", "fsync_file", "fsync_dir", "readback"]
)
def test_receipt_durability_faults_are_typed_and_close_descriptors(
    fault: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spy = _FsSpy(monkeypatch, fail=fault, evidence=tmp_path)
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._persist_receipt(tmp_path, phase="test", payload=b'{"x":1}')
    assert exc.value.code == loader.ErrorCode.RECEIPT_FAILURE.value
    assert exc.value.details == {
        "field": "receipt.readback" if fault == "readback" else "receipt.write"
    }
    assert spy.open_fds == set()
    if fault == "fsync_parent":
        assert ("open", "file") not in spy.events
        assert not list(tmp_path.glob("*.json"))


@requires_directory_fsync
def test_receipt_creates_single_missing_leaf_and_fsyncs_its_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    leaf = tmp_path / "evidence"
    spy = _FsSpy(monkeypatch, evidence=leaf)
    loader._persist_receipt(leaf, phase="test", payload=b"{}")
    mkdir_at = spy.events.index(("mkdir", "evidence"))
    parent_sync_at = spy.events.index(("fsync", "parent"))
    assert mkdir_at < parent_sync_at < spy.events.index(("open", "file"))
    assert spy.events.count(("fsync", "parent")) == 1
    assert oct(leaf.stat().st_mode & 0o777) == "0o700"
    assert spy.open_fds == set()


@requires_directory_fsync
def test_receipt_missing_leaf_parent_fsync_failure_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    leaf = tmp_path / "evidence"
    spy = _FsSpy(monkeypatch, fail="fsync_parent", evidence=leaf)
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._persist_receipt(leaf, phase="test", payload=b"{}")
    assert exc.value.details == {"field": "receipt.write"}
    assert spy.events[-1] == ("fsync", "parent")
    assert ("open", "file") not in spy.events
    assert not list(leaf.glob("*.json"))
    assert spy.open_fds == set()


def _started_at() -> str:
    return datetime.now(timezone.utc).isoformat()


@requires_directory_fsync
def test_leftover_leaf_refuses_every_retry_until_its_parent_entry_is_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    leaf = tmp_path / "evidence"
    spy = _FsSpy(monkeypatch, fail="fsync_parent", evidence=leaf)
    with pytest.raises(loader.ArtifactLoaderError) as first:
        loader._persist_receipt(leaf, phase="pre-apply", payload=b"{}")
    assert first.value.details == {"field": "receipt.write"}
    # mkdir succeeded, so the leaf is left behind -- but its entry is not durable.
    assert leaf.is_dir() and not list(leaf.iterdir())

    def best_effort() -> loader.ArtifactLoaderError:
        return loader._best_effort_failure_receipt(
            first.value, leaf, operation_id="op", operation_started_at_utc=_started_at(),
            mode="apply", publication_id=None,
        )

    # The immediate best-effort failure receipt on the same path must not treat
    # the leftover leaf as durable either.
    spy.events.clear()
    assert best_effort().receipt_written is False
    assert ("mkdir", "evidence") not in spy.events
    assert spy.events[-1] == ("fsync", "parent")
    assert ("open", "file") not in spy.events
    # Every later retry on the same path refuses before any receipt file exists.
    for _ in range(2):
        spy.events.clear()
        with pytest.raises(loader.ArtifactLoaderError) as retry:
            loader._persist_receipt(leaf, phase="pre-apply", payload=b"{}")
        assert retry.value.code == loader.ErrorCode.RECEIPT_FAILURE.value
        assert retry.value.details == {"field": "receipt.write"}
        assert ("mkdir", "evidence") not in spy.events
        assert spy.events[-1] == ("fsync", "parent")
        assert ("open", "file") not in spy.events
        assert not list(leaf.iterdir())
    assert spy.open_fds == set()
    # Once the parent sync succeeds the same leftover leaf is admitted.
    spy.fail = None
    spy.events.clear()
    ref = loader._persist_receipt(leaf, phase="pre-apply", payload=b'{"x":1}')
    assert spy.events.index(("fsync", "parent")) < spy.events.index(("open", "file"))
    assert spy.events.index(("fsync", "file")) < spy.events.index(("fsync", "dir"))
    assert [path.name for path in leaf.iterdir()] == [ref.basename]
    assert best_effort().receipt_written is True
    assert len(list(leaf.glob("*-failure-*.json"))) == 1
    assert spy.open_fds == set()


@requires_directory_fsync
def test_public_failure_receipt_on_leftover_leaf_refuses_until_parent_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, _ = _genuine(tmp_path, monkeypatch)
    monkeypatch.delenv(loader.APPROVED_RELEASE_CONTEXT_ENV, raising=False)
    leaf = tmp_path / "evidence"
    spy = _FsSpy(monkeypatch, fail="fsync_parent", evidence=leaf)
    _NoDatabase.calls = 0
    # The first attempt creates the leaf inside its immediate best-effort failure
    # receipt; the second finds that leftover leaf.  Both must refuse the receipt.
    for attempt in range(2):
        spy.events.clear()
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader.publish_verified_artifact(
                artifact, evidence_dir=leaf, connection_factory=_NoDatabase()
            )
        assert exc.value.details == {"field": "release_context.approval"}
        assert exc.value.receipt_written is False
        assert (("mkdir", "evidence") in spy.events) is (attempt == 0)
        assert spy.events[-1] == ("fsync", "parent")
        assert leaf.is_dir() and not list(leaf.iterdir())
    spy.fail = None
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader.publish_verified_artifact(
            artifact, evidence_dir=leaf, connection_factory=_NoDatabase()
        )
    assert exc.value.receipt_written is True
    [receipt] = leaf.glob("*-failure-*.json")
    document = json.loads(receipt.read_text(encoding="utf-8"))
    assert document["code"] == loader.ErrorCode.RECEIPT_FAILURE.value
    assert document["transaction_outcome"] == "not_started"
    assert _NoDatabase.calls == 0
    assert spy.open_fds == set()


@requires_directory_fsync
def test_premounted_evidence_root_does_not_depend_on_its_parent_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Production /evidence is a pre-mounted volume root on its own device: its entry
    # is never created here, so the parent is not synced (and cannot block it).
    leaf = tmp_path / "evidence"
    leaf.mkdir()
    leaf_stat = os.stat(leaf)

    def fstat(fd: int) -> os.stat_result:
        result = os.fstat(fd)
        if not os.path.samestat(result, leaf_stat):
            return result
        fields = list(result[:10])
        fields[2] = result.st_dev + 1  # st_dev: another mounted filesystem
        return os.stat_result(fields)

    monkeypatch.setattr(loader._DurableFs, "fstat", staticmethod(fstat))
    spy = _FsSpy(monkeypatch, fail="fsync_parent", evidence=leaf)
    ref = loader._persist_receipt(leaf, phase="test", payload=b'{"x":1}')
    fsyncs = [event for event in spy.events if event[0] == "fsync"]
    assert fsyncs == [("fsync", "file"), ("fsync", "dir")]
    assert (leaf / ref.basename).read_bytes() == b'{"x":1}'
    assert spy.open_fds == set()


@requires_directory_fsync
@pytest.mark.parametrize("name", ["/", "."])
def test_receipt_refuses_evidence_path_without_a_leaf_name(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._persist_receipt(Path(name), phase="test", payload=b"{}")
    assert exc.value.details == {"field": "receipt.directory"}
    assert not list(tmp_path.glob("*.json"))


@requires_directory_fsync
def test_receipt_refuses_missing_ancestor_chain_and_symlinked_directory(
    tmp_path: Path,
) -> None:
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._persist_receipt(tmp_path / "a" / "b", phase="test", payload=b"{}")
    assert exc.value.details == {"field": "receipt.directory_parent"}
    assert not (tmp_path / "a").exists()
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(loader.ArtifactLoaderError):
        loader._persist_receipt(link, phase="test", payload=b"{}")
    assert not list(real.iterdir())


# --- r4081728306: full content binding in read-only paths -----------------------------


def _swapped_content_artifact(artifact: loader.VerifiedArtifact) -> loader.VerifiedArtifact:
    """Same schema/count/nulls/histogram/metadata, one valid score changed."""
    frame = _frame()
    frame.loc[1, "neutralized_score"] = frame.loc[1, "neutralized_score"] + 1.0
    swapped = _table(frame)
    assert swapped.schema == artifact.table.schema
    assert swapped.num_rows == artifact.table.num_rows
    assert [swapped[name].null_count for name in swapped.column_names] == [
        artifact.table[name].null_count for name in artifact.table.column_names
    ]
    return replace(artifact, table=swapped)


@pytest.mark.parametrize("entrypoint", ["dry_run", "recover"])
def test_public_read_only_entrypoints_bind_full_content_before_database(
    entrypoint: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, _ = _genuine(tmp_path, monkeypatch)
    swapped = _swapped_content_artifact(artifact)
    operation = (
        loader.dry_run_verified_artifact if entrypoint == "dry_run"
        else loader.recover_published_artifact
    )
    payloads: list[int] = []
    monkeypatch.setattr(
        loader, "publication_row_tuples", lambda *a, **k: payloads.append(1) or []
    )
    evidence = tmp_path / "content-evidence"
    evidence.mkdir()
    _NoDatabase.calls = 0
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        operation(swapped, evidence_dir=evidence, connection_factory=_NoDatabase())
    assert exc.value.code == loader.ErrorCode.ROW_DIGEST_MISMATCH.value
    assert exc.value.phase == "artifact_binding"
    assert exc.value.transaction_outcome == "not_started"
    assert _NoDatabase.calls == 0
    assert payloads == []
    receipts = list(evidence.glob("*.json"))
    assert all("-failure-" in path.name for path in receipts)
    if receipts:
        receipt = json.loads(receipts[0].read_text(encoding="utf-8"))
        assert receipt["failure_phase"] == "artifact_binding"
        assert receipt["publication_id"] is None


def test_private_read_only_seam_binds_full_content_before_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, contract = _genuine(tmp_path, monkeypatch)
    swapped = _swapped_content_artifact(artifact)
    _NoDatabase.calls = 0
    for require_published in (False, True):
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._read_only_operation(
                swapped, contract=contract, connection_factory=_NoDatabase(),
                require_published=require_published,
            )
        assert exc.value.code == loader.ErrorCode.ROW_DIGEST_MISMATCH.value
    assert _NoDatabase.calls == 0


def test_genuine_read_only_admission_builds_no_insert_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, _ = _genuine(tmp_path, monkeypatch)
    payloads: list[int] = []
    monkeypatch.setattr(
        loader, "publication_row_tuples", lambda *a, **k: payloads.append(1) or []
    )
    contract, admitted = loader._admit_read_only_operation_artifact(artifact)
    assert admitted.publication == loader._publication_from_contract(contract)
    assert payloads == []


def test_ready_contract_requires_parent_header_digest(tmp_path: Path) -> None:
    _, raw, contract = _fixture(tmp_path)
    assert contract.parent.header_sha256 == "b" * 64
    document = json.loads(raw)
    document["parent"]["header_sha256"] = None
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._parse_contract(json.dumps(document).encode())
    assert exc.value.code == loader.ErrorCode.CONTRACT_INVALID.value
    assert exc.value.details.get("field") == "parent.header_sha256"
    document["parent"].pop("header_sha256")
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._parse_contract(json.dumps(document).encode())
    assert exc.value.details.get("field") == "parent.header_sha256"
    for invalid in ("B" * 64, "b" * 63, 7):
        document["parent"]["header_sha256"] = invalid
        with pytest.raises(loader.ArtifactLoaderError):
            loader._parse_contract(json.dumps(document).encode())
    unpinned = replace(contract, parent=replace(contract.parent, header_sha256=None))
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._require_ready(unpinned)
    assert exc.value.details.get("field") == "parent.header_sha256"


def test_identity_pending_contract_may_leave_parent_header_open(tmp_path: Path) -> None:
    _, raw, _ = _fixture(tmp_path)
    document = json.loads(raw)
    document["status"] = "identity_pending"
    document["identity"] = None
    document["parent"]["header_sha256"] = None
    contract = loader._parse_contract(json.dumps(document).encode())
    assert contract.parent.header_sha256 is None
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._require_ready(contract)
    assert exc.value.code == loader.ErrorCode.MISSING_IDENTITY.value


class _NoDatabase:
    """Connection factory and connection stand-in that fails on any database use."""

    calls = 0

    def __call__(self) -> Any:
        type(self).calls += 1
        raise AssertionError("database connection opened")

    def execute(self, *_: object, **__: object) -> Any:
        type(self).calls += 1
        raise AssertionError("database statement executed")


def _genuine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[loader.VerifiedArtifact, loader.FrozenArtifactContract]:
    root, raw, contract = _fixture(tmp_path)
    contract_path = tmp_path / "checked-in-contract.json"
    contract_path.write_bytes(raw)
    monkeypatch.setattr(loader, "CONTRACT_PATH", contract_path)
    artifact = loader._load_verified_artifact(
        root, contract=contract, contract_sha256=hashlib.sha256(raw).hexdigest()
    )
    return artifact, contract


_FORGED_PUBLICATIONS: tuple[tuple[str, Any], ...] = (
    ("publication_id", "33333333-3333-4333-8333-333333333333"),
    ("panel_publication_id", PARENT_ID),
    ("policy_version", "forged-policy"),
    ("policy_digest", "0" * 64),
    ("code_revision", "forged-revision"),
    ("panel_last_closed_month", date(2026, 7, 1)),
    ("panel_last_closed_month", datetime(2026, 8, 1)),
    ("first_month", date(2026, 5, 1)),
    ("last_month", date(2026, 9, 1)),
    ("input_fingerprint", "2" * 64),
    ("l_anchor", -0.25),
    ("l_anchor", np.float64(-0.5)),
    ("rows_digest", "3" * 64),
    ("d_confirmed_count", 3),
    ("d_candidate_count", True),
    ("row_count", 13.0),
)


@pytest.mark.parametrize(
    ("field", "value"), _FORGED_PUBLICATIONS,
    ids=[f"{name}-{type(value).__name__}" for name, value in _FORGED_PUBLICATIONS],
)
@pytest.mark.parametrize("entrypoint", ["publish", "dry_run", "recover"])
def test_public_entrypoints_refuse_forged_publication_before_database(
    entrypoint: str, field: str, value: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, _ = _genuine(tmp_path, monkeypatch)
    forged = replace(artifact, publication=replace(artifact.publication, **{field: value}))
    no_database = _NoDatabase()
    _NoDatabase.calls = 0
    function = {
        "publish": loader.publish_verified_artifact,
        "dry_run": loader.dry_run_verified_artifact,
        "recover": loader.recover_published_artifact,
    }[entrypoint]
    evidence = tmp_path / "evidence"
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        function(forged, evidence_dir=evidence, connection_factory=no_database)
    assert exc.value.code == loader.ErrorCode.IDENTITY_MISMATCH.value
    assert exc.value.details.get("field") == f"publication.{field}"
    assert _NoDatabase.calls == 0
    for receipt in evidence.glob("*.json") if evidence.exists() else ():
        document = json.loads(receipt.read_text(encoding="utf-8"))
        assert document.get("publication_id") != value
        assert "verified" != document.get("outcome")


@pytest.mark.parametrize("entrypoint", ["dry_run", "recover"])
@pytest.mark.parametrize("forgery", ["publication_id", "contract_sha256"])
def test_read_only_admission_failure_writes_one_untrusted_identity_free_receipt(
    entrypoint: str, forgery: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, _ = _genuine(tmp_path, monkeypatch)
    forged_id = "33333333-3333-4333-8333-333333333333"
    forged = (
        replace(artifact, publication=replace(artifact.publication, publication_id=forged_id))
        if forgery == "publication_id"
        else replace(artifact, contract_sha256="0" * 64)
    )
    operation = (
        loader.dry_run_verified_artifact if entrypoint == "dry_run"
        else loader.recover_published_artifact
    )
    evidence = tmp_path / "admission-evidence"
    _NoDatabase.calls = 0
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        operation(
            forged, evidence_dir=evidence, connection_factory=_NoDatabase(),
            operation_id="admission-test", operation_started_at_utc="2026-09-23T00:00:00+00:00",
        )
    assert exc.value.code == (
        loader.ErrorCode.IDENTITY_MISMATCH.value if forgery == "publication_id"
        else loader.ErrorCode.CONTRACT_INVALID.value
    )
    assert exc.value.phase == "artifact_binding"
    assert exc.value.transaction_outcome == "not_started"
    assert exc.value.receipt_written is True
    assert _NoDatabase.calls == 0
    receipts = list(evidence.glob("*.json"))
    assert len(receipts) == 1 and "-failure-" in receipts[0].name
    receipt = json.loads(receipts[0].read_text(encoding="utf-8"))
    assert receipt["phase"] == "failure"
    assert receipt["operation_id"] == "admission-test"
    assert receipt["mode"] == ("dry-run" if entrypoint == "dry_run" else "verify-published")
    assert receipt["code"] == exc.value.code
    assert receipt["failure_phase"] == "artifact_binding"
    assert receipt["transaction_outcome"] == "not_started"
    assert receipt["schema_installed"] is False
    assert receipt["publication_id"] is None
    assert forged_id not in receipts[0].read_text(encoding="utf-8")
    assert receipt["outcome"] == "failed"


@pytest.mark.parametrize("mode", ["--dry-run", "--verify-published"])
def test_cli_does_not_duplicate_admission_failure_receipt(
    mode: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact, _ = _genuine(tmp_path, monkeypatch)
    forged_id = "33333333-3333-4333-8333-333333333333"
    forged = replace(
        artifact, publication=replace(artifact.publication, publication_id=forged_id)
    )
    cli = _load_cli()
    monkeypatch.setattr(cli.loader, "load_verified_artifact", lambda _: forged)
    monkeypatch.setattr(cli, "resolve_dsn", lambda: pytest.fail("DB must not be opened"))
    evidence = tmp_path / "cli-admission-evidence"
    code = cli.main([
        "--artifact-root", str(tmp_path), "--evidence-dir", str(evidence), mode
    ])
    assert code == 3
    assert json.loads(capsys.readouterr().err) == {
        "code": "identity_mismatch", "state": "refused"
    }
    receipts = list(evidence.glob("*.json"))
    assert len(receipts) == 1 and "-failure-" in receipts[0].name
    assert json.loads(receipts[0].read_text(encoding="utf-8"))["publication_id"] is None
    assert forged_id not in receipts[0].read_text(encoding="utf-8")


def _forged_artifacts(
    artifact: loader.VerifiedArtifact,
) -> list[tuple[str, loader.VerifiedArtifact]]:
    summary = artifact.summary
    histogram = tuple(
        (bucket, count + 1 if index == 0 else count)
        for index, (bucket, count) in enumerate(summary.histogram)
    )
    nulls = tuple(
        (name, count + 1 if index == 0 else count)
        for index, (name, count) in enumerate(summary.null_counts)
    )
    files = artifact.files
    return [
        ("artifact.summary.row_count", replace(artifact, summary=replace(summary, row_count=12))),
        ("artifact.summary.month_count", replace(artifact, summary=replace(summary, month_count=2))),
        (
            "artifact.summary.witnessed_count",
            replace(artifact, summary=replace(summary, witnessed_count=8.0)),
        ),
        ("artifact.summary.histogram", replace(artifact, summary=replace(summary, histogram=histogram))),
        (
            "artifact.summary.rows_digest",
            replace(artifact, summary=replace(summary, rows_digest="0" * 64)),
        ),
        (
            "artifact.summary.null_counts",
            replace(artifact, summary=replace(summary, null_counts=nulls)),
        ),
        ("artifact.files", replace(artifact, files=files[:-1])),
        ("artifact.files", replace(artifact, files=list(files))),
        (
            f"artifact.files.{len(files) - 1}",
            replace(artifact, files=(*files[:-1], replace(files[-1], sha256="0" * 64))),
        ),
        (
            "artifact.table.row_count",
            replace(artifact, table=artifact.table.slice(0, artifact.table.num_rows - 1)),
        ),
        ("artifact.summary", replace(artifact, summary=object())),
        ("artifact.table", replace(artifact, table=object())),
    ]


@pytest.mark.parametrize("index", range(12))
def test_forged_summary_files_or_table_are_refused_before_database(
    index: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, contract = _genuine(tmp_path, monkeypatch)
    field, forged = _forged_artifacts(artifact)[index]
    _NoDatabase.calls = 0
    for function in (
        loader.publish_verified_artifact,
        loader.dry_run_verified_artifact,
        loader.recover_published_artifact,
    ):
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            function(forged, evidence_dir=tmp_path / "evidence", connection_factory=_NoDatabase())
        assert exc.value.code == loader.ErrorCode.IDENTITY_MISMATCH.value
        assert exc.value.details.get("field") == field
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._read_only_operation(
            forged, contract=contract, connection_factory=_NoDatabase(), require_published=False
        )
    assert exc.value.details.get("field") == field
    assert _NoDatabase.calls == 0


def test_forged_contract_digest_and_artifact_type_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, _ = _genuine(tmp_path, monkeypatch)
    _NoDatabase.calls = 0
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader.dry_run_verified_artifact(
            replace(artifact, contract_sha256="0" * 64),
            evidence_dir=tmp_path / "evidence", connection_factory=_NoDatabase(),
        )
    assert exc.value.code == loader.ErrorCode.CONTRACT_INVALID.value
    assert exc.value.details.get("field") == "contract.changed"
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader.recover_published_artifact(
            replace(artifact, publication=object()),
            evidence_dir=tmp_path / "evidence", connection_factory=_NoDatabase(),
        )
    assert exc.value.details.get("field") == "publication.type"
    assert _NoDatabase.calls == 0


def test_private_publish_refuses_forged_metadata_before_receipt_or_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, contract = _genuine(tmp_path, monkeypatch)
    forged = replace(
        artifact, publication=replace(artifact.publication, l_anchor=-0.25)
    )
    evidence = tmp_path / "evidence"
    _NoDatabase.calls = 0
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._publish_verified_artifact(
            forged, contract=contract, connection_factory=_NoDatabase(), evidence_dir=evidence
        )
    assert exc.value.details.get("field") == "publication.l_anchor"
    assert _NoDatabase.calls == 0
    receipts = list(evidence.glob("*.json"))
    assert len(receipts) == 1 and "-failure-" in receipts[0].name
    receipt = json.loads(receipts[0].read_text(encoding="utf-8"))
    assert receipt["failure_phase"] == "artifact_binding"
    assert receipt["transaction_outcome"] == "not_started"
    assert receipt["publication_id"] == contract.identity.publication_id


def test_private_publish_refuses_swapped_table_content_before_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, contract = _genuine(tmp_path, monkeypatch)
    frame = _frame()
    frame.loc[0, "neutralized_score"] = 99.0
    swapped = replace(artifact, table=_table(frame))
    evidence = tmp_path / "evidence"
    _NoDatabase.calls = 0
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._publish_verified_artifact(
            swapped, contract=contract, connection_factory=_NoDatabase(), evidence_dir=evidence
        )
    assert exc.value.code == loader.ErrorCode.ROW_DIGEST_MISMATCH.value
    assert _NoDatabase.calls == 0
    receipt = json.loads(next(evidence.glob("*-failure-*.json")).read_text(encoding="utf-8"))
    assert receipt["failure_phase"] == "payload"
    assert receipt["schema_installed"] is False
    assert not list(evidence.glob("*pre-apply*"))


def test_public_stored_verification_refuses_forged_publication_before_queries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, _ = _genuine(tmp_path, monkeypatch)
    _NoDatabase.calls = 0
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader.verify_stored_publication(
            _NoDatabase(),  # type: ignore[arg-type]
            replace(artifact.publication, rows_digest="0" * 64),
            require_current=False,
        )
    assert exc.value.details.get("field") == "publication.rows_digest"
    assert _NoDatabase.calls == 0


def test_strict_metadata_equality_refuses_loose_python_equality() -> None:
    assert loader._strict_same(-0.5, -0.5)
    assert loader._strict_same(date(2026, 8, 1), date(2026, 8, 1))
    assert loader._strict_same((("A", 1),), (("A", 1),))
    for actual, expected in (
        (True, 1), (1.0, 1), (np.float64(-0.5), -0.5), (-0.0, 0.0),
        (datetime(2026, 8, 1), date(2026, 8, 1)), ((("A", True),), (("A", 1),)),
        ([("A", 1)], (("A", 1),)), ((("A", 1),), (("A", 1), ("B", 2))),
    ):
        assert not loader._strict_same(actual, expected), (actual, expected)


def test_loader_never_mutates_privileges() -> None:
    source = (ROOT / "src" / "bonds" / "implied_rating_artifact_loader.py").read_text()
    # SQL in the loader is upper-case; any privilege-changing statement would appear here.
    for forbidden in ("GRANT ", "REVOKE ", "ALTER DEFAULT PRIVILEGES", "OWNER TO", "SET ROLE"):
        assert forbidden not in source, forbidden


_OWNER, _PUBLIC, _RUNTIME, _OTHER = 16385, 0, 16400, 16500


@pytest.mark.parametrize(
    ("grantor", "grantee_oid", "grantee", "privilege", "grantable", "admitted"),
    [
        (_OWNER, _OWNER, "worker_writer", "EXECUTE", False, True),
        (_OWNER, _PUBLIC, None, "EXECUTE", False, True),
        (_OWNER, _RUNTIME, "app_runtime", "EXECUTE", False, True),
        (_OWNER, _OTHER, "app_analytics_ro", "EXECUTE", False, False),
        (_OWNER, _OTHER, "unknown_role", "EXECUTE", False, False),
        (_OWNER, _OTHER, None, "EXECUTE", False, False),
        (_OWNER, _RUNTIME, "app_runtime", "EXECUTE", True, False),
        (_OWNER, _PUBLIC, None, "EXECUTE", True, False),
        (_OWNER, _OWNER, "worker_writer", "EXECUTE", True, False),
        (_RUNTIME, _PUBLIC, None, "EXECUTE", False, False),
        (_OTHER, _OWNER, "worker_writer", "EXECUTE", False, False),
        (_OWNER, _RUNTIME, "app_runtime", "USAGE", False, False),
    ],
)
def test_function_acl_item_rule(
    grantor: int, grantee_oid: int, grantee: str | None, privilege: str, grantable: bool,
    admitted: bool,
) -> None:
    assert loader._function_acl_item_admitted(
        _OWNER, grantor, grantee_oid, grantee, privilege, grantable
    ) is admitted


def _plpgsql_body(source: str, name: str) -> str:
    start = source.index(f"CREATE OR REPLACE FUNCTION {name}()")
    body_start = source.index("AS $$", start) + len("AS $$")
    return source[body_start:source.index("$$;", body_start)]


def test_rr1_pins_are_bound_to_the_authentic_rr1_sql() -> None:
    source = (ROOT / "schemas" / "rr1_fee_profiles.sql").read_text(encoding="utf-8")
    for signature, contract in loader._RR1_FUNCTION_CONTRACTS.items():
        body = _plpgsql_body(source, signature.split("(")[0])
        assert hashlib.sha256(body.encode()).hexdigest() == contract[0], signature
        assert contract[4:7] == ("trigger", "plpgsql", "v")
        assert contract[8] is False and contract[13] is None and contract[14] == "worker_writer"
    assert {key: value[:3] for key, value in loader._RR1_TRIGGER_CONTRACTS.items()} == {
        ("rr1_fee_profile_current_pointer_guard", "sec_derived_current_pointers"): (
            "rr1_fee_profile_current_pointer_guard()", 31, "O",
        ),
        ("rr1_fee_profile_publication_validation_guard", "sec_derived_publications"): (
            "rr1_fee_profile_publication_validation_guard()", 19, "O",
        ),
    }
    # Admission is exact-name, never a prefix: the pair is exactly two triggers
    # and two functions, disjoint from the baseline contract.
    assert len(loader._RR1_FUNCTION_CONTRACTS) == 2
    assert not set(loader._RR1_TRIGGER_CONTRACTS) & set(loader._SHARED_TRIGGER_CONTRACTS)
    assert loader._PRODUCTION_REQUIRED_PROFILE == loader.PROFILE_RR1
    assert loader._PRODUCTION_REQUIRED_ROLES == ("worker_writer", "app_runtime", "app_analytics_ro")


def _sql_statements(path: str) -> str:
    return (ROOT / "schemas" / path).read_text(encoding="utf-8")


def test_product_sql_revokes_exactly_inherited_writes_on_its_five_relations() -> None:
    text = _sql_statements("bond_market_implied_rating_v1.sql")
    block = text[text.index("DO $$\nDECLARE\n    write_privileges"):]
    assert "WHERE rolname = 'app_runtime'" in block
    assert "'INSERT, UPDATE, DELETE, MAINTAIN'" in block and "'INSERT, UPDATE, DELETE'" in block
    for relation in loader._PRODUCT_SCHEMA_OBJECTS:
        assert f"|| '{relation}" in block, relation
    assert block.count("|| 'bond_market_implied_rating_") == 5
    assert "FROM app_runtime RESTRICT" in block
    for forbidden in ("CREATE ROLE", "GRANT ", "REVOKE ALL", "SELECT,", "CASCADE", "EXCEPTION"):
        assert forbidden not in block, forbidden


def test_shared_sql_revoke_is_fresh_install_only_and_exact() -> None:
    text = _sql_statements("sec_derived_publications.sql")
    marker = text.index("'sec_derived.fresh_ledger_tables'")
    assert marker < text.index("CREATE TABLE IF NOT EXISTS sec_derived_publications")
    block = text[text.rindex("DO $$"):]
    assert "ledger_table = ANY(fresh_tables)" in block
    assert "'REVOKE %s ON TABLE %I FROM app_runtime RESTRICT'" in block
    for relation in loader._SHARED_SCHEMA_OBJECTS:
        assert f"'{relation}'" in block
    for forbidden in ("CREATE ROLE", "GRANT ", "REVOKE ALL", "CASCADE", "EXCEPTION"):
        assert forbidden not in block, forbidden


@pytest.mark.parametrize("entrypoint", ["publish", "dry_run", "recover"])
def test_public_entrypoints_pin_the_production_envelope(
    entrypoint: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, contract = _genuine(tmp_path, monkeypatch)
    captured: dict[str, Any] = {}

    class _Stop(Exception):
        pass

    def capture(*_: object, **kwargs: Any) -> None:
        captured.update(kwargs)
        raise _Stop

    if entrypoint == "publish":
        monkeypatch.setattr(loader, "_load_release_evidence", lambda *_: None)
        monkeypatch.setattr(loader, "_publish_verified_artifact", capture)
        function = loader.publish_verified_artifact
    else:
        monkeypatch.setattr(loader, "_read_only_operation", capture)
        function = (
            loader.dry_run_verified_artifact if entrypoint == "dry_run"
            else loader.recover_published_artifact
        )
    with pytest.raises(_Stop):
        function(artifact, evidence_dir=tmp_path / "evidence", connection_factory=_NoDatabase())
    assert captured["required_profile"] == loader.PROFILE_RR1
    assert captured["required_roles"] == loader._PRODUCTION_REQUIRED_ROLES


def test_genuine_artifact_rebinds_to_contract_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, contract = _genuine(tmp_path, monkeypatch)
    bound = loader._bind_verified_artifact(artifact, contract)
    assert bound.publication == loader._publication_from_contract(contract)
    assert bound.summary is artifact.summary and bound.table is artifact.table
    assert loader._artifact_payload(bound, contract)


def test_railway_runtime_is_private_verify_only_and_never_restarts() -> None:
    path = ROOT / "docker" / "bond-implied-artifact-loader" / "railway.toml"
    document = tomllib.loads(path.read_text(encoding="utf-8"))
    assert set(document) == {"build", "deploy"}
    assert document["build"] == {
        "builder": "DOCKERFILE",
        "dockerfilePath": "docker/bond-implied-artifact-loader/Dockerfile",
    }
    assert document["deploy"] == {
        "startCommand": (
            "/usr/local/bin/python -I -S "
            "/app/docker/bond-implied-artifact-loader/bootstrap_evidence.py "
            "timeout --signal=TERM --kill-after=30s 9000s "
            "python -m scripts.load_bond_market_implied_rating_artifact "
            "--artifact-root /artifact --evidence-dir /evidence --verify-only"
        ),
        "restartPolicyType": "never",
    }
    text = path.read_text(encoding="utf-8")
    assert "cronSchedule" not in text
    assert "domain" not in document["deploy"]
    assert "9000 seconds" in text
    dockerfile = (
        ROOT / "docker" / "bond-implied-artifact-loader" / "Dockerfile"
    ).read_text(encoding="utf-8")
    assert "docker/bond-implied-artifact-loader/railway.toml" in dockerfile
    assert 'CMD ["timeout", "--signal=TERM", "--kill-after=30s", "9000s"' in dockerfile


def test_receipt_write_failure_is_typed(tmp_path: Path) -> None:
    not_a_directory = tmp_path / "evidence"
    not_a_directory.write_text("occupied", encoding="utf-8")
    with pytest.raises(loader.ArtifactLoaderError) as exc:
        loader._persist_receipt(not_a_directory, phase="test", payload=b"{}")
    assert exc.value.code == loader.ErrorCode.RECEIPT_FAILURE.value


def test_receipt_phases_can_share_one_operation_id(tmp_path: Path) -> None:
    root, raw, contract = _fixture(tmp_path)
    artifact = loader._load_verified_artifact(
        root, contract=contract, contract_sha256=hashlib.sha256(raw).hexdigest()
    )
    operation_id = "33333333-3333-4333-8333-333333333333"
    pre = json.loads(loader._receipt_payload(
        phase="pre-apply", artifact=artifact, outcome="verified", stored=None,
        operation_id=operation_id, contract=contract,
    ))
    post = json.loads(loader._receipt_payload(
        phase="readback", artifact=artifact, outcome="verified", stored=None,
        operation_id=operation_id, contract=contract,
    ))
    assert pre["operation_id"] == post["operation_id"] == operation_id
    assert pre["runtime"]["distributions"]["pyarrow"] == "25.0.0"


def test_loader_has_no_rebuild_worker_or_hand_written_publication_dml() -> None:
    source = (ROOT / "src" / "bonds" / "implied_rating_artifact_loader.py").read_text()
    assert "src.workers" not in source
    assert "build_publication_rows" not in source
    assert "backfill_bond_market_implied_rating" not in source
    for forbidden in (
        "INSERT INTO bond_market_implied_rating_v1",
        "UPDATE bond_market_implied_rating_v1",
        "DELETE FROM bond_market_implied_rating_v1",
        "COPY bond_market_implied_rating_v1",
    ):
        assert forbidden not in source
