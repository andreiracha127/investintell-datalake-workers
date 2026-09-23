"""Pure admission and CLI tests for the frozen-artifact loader."""
from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import sys
from dataclasses import replace
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


def _write_release_context(
    evidence: Path, contract: loader.FrozenArtifactContract
) -> dict[str, Any]:
    evidence.mkdir(parents=True, exist_ok=True)
    source_sha = {
        relative: _sha(ROOT / relative)
        for relative in loader._RELEASE_SOURCE_PATHS
    }
    runtime_root = ROOT / "docker" / "bond-implied-artifact-loader"
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


def test_complete_release_context_verifies_runtime_image_layout(tmp_path: Path) -> None:
    _, _, contract = _fixture(tmp_path)
    evidence = tmp_path / "evidence"
    document = _write_release_context(evidence, contract)
    release = loader._load_release_evidence(evidence, contract)
    assert release.loader_commit == "1" * 40
    assert release.release_context_sha256 == _sha(evidence / "release-context.json")
    assert release.railway_toml_sha256 == document["railway_toml_sha256"]


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
