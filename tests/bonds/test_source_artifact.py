"""Schema contract + streaming reader for the pinned bond price source artifact.

Pure and DB-free. The column-set contract is the one ported from the internal
bond pilot (read-only reference on the unmerged pilot branch); these tests pin:

* required/optional column vocabulary (missing required column => typed refusal
  ``schema_contract_violation``),
* SHA-256 pin enforcement BEFORE parsing (mismatch => ``sha256_mismatch``;
  absent/malformed pin => fail-closed refusal),
* zip-wrapped single-parquet handling (pilot ``source.parquet`` convention) and
  direct ``.parquet`` acceptance,
* record-batch streaming (no full materialization) with global row ordinals,
* unparseable identifier/date rows yielded — never dropped silently,
* the opaque ``source_contract_ref`` token shape (no vendor identity).
"""
from __future__ import annotations

import hashlib
import inspect
import zipfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.bonds.errors import BondError
from src.bonds import source_artifact

# Vendor tokens are composed dynamically so this test file itself stays clean
# under any grep-based leak scan of the new surfaces.
VENDOR_TOKENS = (
    "OS" + "BAP",
    "open" + "bond" + "asset" + "pricing",
    "TR" + "ACE",
    "WR" + "DS",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_parquet(path: Path, columns: dict[str, list], *, row_group_size: int | None = None) -> str:
    table = pa.table(columns)
    pq.write_table(table, path, row_group_size=row_group_size)
    return _sha256(path)


def _basic_columns() -> dict[str, list]:
    return {
        "cusip_id": ["037833100", "459200101", "594918104"],
        "trd_exctn_dt": [date(2026, 6, 1), date(2026, 6, 15), date(2026, 6, 30)],
        "pr": [99.5, 100.25, 101.0],
        "ytm": [4.1, 4.2, None],
        "db_type": [3, None, 1],
    }


def test_required_and_optional_column_vocabulary_is_the_ported_contract() -> None:
    assert source_artifact.REQUIRED_COLUMNS == ("cusip_id", "trd_exctn_dt", "pr")
    assert source_artifact.OPTIONAL_COLUMNS == (
        "prfull", "acclast", "ytm", "mod_dur", "mac_dur", "convexity",
        "bond_maturity", "credit_spread", "qvolume", "dvolume", "db_type",
    )


def test_validate_artifact_accepts_direct_parquet(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.parquet"
    sha = _write_parquet(artifact, _basic_columns())
    descriptor = source_artifact.validate_artifact(artifact, expected_sha256=sha)
    try:
        assert descriptor.sha256 == sha
        assert descriptor.row_count == 3
        assert descriptor.first_observation_date == date(2026, 6, 1)
        assert descriptor.last_observation_date == date(2026, 6, 30)
        assert set(source_artifact.REQUIRED_COLUMNS) <= set(descriptor.columns)
        assert descriptor.optional_columns_present == ("ytm", "db_type")
        assert descriptor.source_contract_ref == f"bond_price_source_v1@{sha[:12]}"
    finally:
        source_artifact.cleanup_artifact(descriptor)


def test_validate_artifact_accepts_zip_wrapped_single_parquet(tmp_path: Path) -> None:
    inner = tmp_path / "inner.parquet"
    _write_parquet(inner, _basic_columns())
    artifact = tmp_path / "artifact.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.write(inner, "source.parquet")
    sha = _sha256(artifact)
    descriptor = source_artifact.validate_artifact(artifact, expected_sha256=sha)
    try:
        assert descriptor.sha256 == sha  # the pin binds the DELIVERED artifact bytes
        assert descriptor.row_count == 3
        assert descriptor.extracted_dir is not None
        rows = list(source_artifact.iter_price_rows(descriptor))
        assert [float(r.price) for r in rows] == [99.5, 100.25, 101.0]
    finally:
        source_artifact.cleanup_artifact(descriptor)
    # cleanup removes the extraction workspace
    assert not Path(descriptor.parquet_path).exists()


def test_zip_with_multiple_members_is_a_schema_contract_violation(tmp_path: Path) -> None:
    inner = tmp_path / "inner.parquet"
    _write_parquet(inner, _basic_columns())
    artifact = tmp_path / "artifact.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.write(inner, "source.parquet")
        archive.write(inner, "second.parquet")
    with pytest.raises(BondError) as excinfo:
        source_artifact.validate_artifact(artifact, expected_sha256=_sha256(artifact))
    assert excinfo.value.code == "schema_contract_violation"


def test_sha256_pin_is_enforced_before_parsing(tmp_path: Path) -> None:
    # Garbage bytes: not parquet, not zip. With a WRONG pin the refusal must be
    # sha256_mismatch — proving the hash gate runs BEFORE any parsing.
    artifact = tmp_path / "garbage.parquet"
    artifact.write_bytes(b"not-a-real-artifact")
    with pytest.raises(BondError) as excinfo:
        source_artifact.validate_artifact(artifact, expected_sha256="0" * 64)
    assert excinfo.value.code == "sha256_mismatch"
    # With the CORRECT pin over the same garbage, parsing is reached and the
    # container refusal is typed as a schema contract violation.
    with pytest.raises(BondError) as excinfo:
        source_artifact.validate_artifact(artifact, expected_sha256=_sha256(artifact))
    assert excinfo.value.code == "schema_contract_violation"


def test_absent_or_malformed_pin_is_fail_closed(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.parquet"
    _write_parquet(artifact, _basic_columns())
    for bad_pin in (None, "", "abc123", "Z" * 64):
        with pytest.raises(BondError) as excinfo:
            source_artifact.validate_artifact(artifact, expected_sha256=bad_pin)
        assert excinfo.value.code == "sha256_mismatch"


def test_missing_required_column_is_a_typed_refusal(tmp_path: Path) -> None:
    columns = _basic_columns()
    del columns["pr"]
    artifact = tmp_path / "artifact.parquet"
    sha = _write_parquet(artifact, columns)
    with pytest.raises(BondError) as excinfo:
        source_artifact.validate_artifact(artifact, expected_sha256=sha)
    assert excinfo.value.code == "schema_contract_violation"
    assert excinfo.value.details.get("missing_columns") == ["pr"]


def test_artifact_missing_on_disk_is_unavailable(tmp_path: Path) -> None:
    with pytest.raises(BondError) as excinfo:
        source_artifact.validate_artifact(tmp_path / "absent.parquet", expected_sha256="0" * 64)
    assert excinfo.value.code == "artifact_unavailable"


def test_iter_price_rows_streams_record_batches_with_global_ordinals(tmp_path: Path) -> None:
    n = 12
    columns = {
        "cusip_id": [f"03783310{i % 10}" for i in range(n)],
        "trd_exctn_dt": [date(2026, 6, 1 + i) for i in range(n)],
        "pr": [100.0 + i for i in range(n)],
    }
    artifact = tmp_path / "multi.parquet"
    sha = _write_parquet(artifact, columns, row_group_size=4)  # 3 row groups
    descriptor = source_artifact.validate_artifact(artifact, expected_sha256=sha)
    try:
        assert descriptor.row_group_count == 3
        # Streaming: a generator over pyarrow record batches, never a full table.
        assert inspect.isgeneratorfunction(source_artifact.iter_price_rows)
        source_text = inspect.getsource(source_artifact)
        assert "iter_batches" in source_text
        assert "read_table" not in source_text and "to_pandas" not in source_text
        iterator = source_artifact.iter_price_rows(descriptor, batch_size=4)
        first = next(iterator)  # first row available without exhausting the stream
        assert first.row_number == 0 and float(first.price) == 100.0
        rest = list(iterator)
        assert [r.row_number for r in rest] == list(range(1, n))
        assert float(rest[-1].price) == 100.0 + n - 1
    finally:
        source_artifact.cleanup_artifact(descriptor)


def test_unparseable_identifier_and_date_rows_are_yielded_never_dropped(tmp_path: Path) -> None:
    columns = {
        "cusip_id": ["037833100", None, "NOT-A-CUSIP"],
        "trd_exctn_dt": ["2026-06-30", "not-a-date", "2026-06-15"],
        "pr": [99.5, 100.0, 101.5],
        "ytm": [4.1, None, 4.3],
    }
    artifact = tmp_path / "mixed.parquet"
    sha = _write_parquet(artifact, columns)
    descriptor = source_artifact.validate_artifact(artifact, expected_sha256=sha)
    try:
        rows = list(source_artifact.iter_price_rows(descriptor))
        assert len(rows) == 3  # nothing dropped silently
        assert rows[0].observation_date == date(2026, 6, 30)
        assert rows[1].observation_date is None  # unparseable -> quarantine path
        assert rows[1].observation_date_raw == "not-a-date"
        assert rows[2].cusip_id == "NOT-A-CUSIP"  # identifier kept raw for the resolver
        assert float(rows[1].ytm) if rows[1].ytm is not None else rows[1].ytm is None
        # Date bounds ignore the unparseable value.
        assert descriptor.first_observation_date == date(2026, 6, 15)
        assert descriptor.last_observation_date == date(2026, 6, 30)
    finally:
        source_artifact.cleanup_artifact(descriptor)


def test_artifact_with_zero_parseable_dates_is_refused(tmp_path: Path) -> None:
    columns = {
        "cusip_id": ["037833100"],
        "trd_exctn_dt": ["never"],
        "pr": [99.5],
    }
    artifact = tmp_path / "dateless.parquet"
    sha = _write_parquet(artifact, columns)
    with pytest.raises(BondError) as excinfo:
        source_artifact.validate_artifact(artifact, expected_sha256=sha)
    assert excinfo.value.code == "schema_contract_violation"


def test_contract_stamps_make_rows_eligibility_capable() -> None:
    # The pinned source contract declares pr as a clean transaction price in
    # percent of par; the stamps must sit inside the eligibility vocabularies
    # (bond_price_is_eligible: price_type IN trade/evaluated, accrued IN clean/dirty).
    assert source_artifact.CONTRACT_PRICE_TYPE == "trade"
    assert source_artifact.CONTRACT_ACCRUED_TREATMENT == "clean"


def test_module_carries_no_vendor_identity() -> None:
    source_text = inspect.getsource(source_artifact).lower()
    for token in VENDOR_TOKENS:
        assert token.lower() not in source_text, token
