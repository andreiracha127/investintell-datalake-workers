from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.bond_pilot.contracts import FieldState, IdentifierState, PilotError
from src.bond_pilot.identifiers import normalize_cusip9
from src.bond_pilot.panel import build_observed_panel


def _write_source(path: Path, columns: dict[str, list[object]]) -> None:
    pq.write_table(pa.table(columns), path)


def _read_output(path: Path) -> dict[str, list[object]]:
    return pq.read_table(path).to_pydict()


@pytest.mark.parametrize(
    ("value", "state", "normalized", "transformation"),
    [
        ("  123abc789  ", IdentifierState.VALID_CUSIP9, "123ABC789", "trim_upper"),
        ("123ABC789", IdentifierState.VALID_CUSIP9, "123ABC789", "identity"),
        (None, IdentifierState.BLANK, None, "rejected"),
        (" ", IdentifierState.BLANK, None, "rejected"),
        ("000000000", IdentifierState.PLACEHOLDER, None, "rejected"),
        ("N/A", IdentifierState.PLACEHOLDER, None, "rejected"),
        ("IS:123456", IdentifierState.SYNTHETIC, None, "rejected"),
        ("123-45678", IdentifierState.INVALID_FORMAT, None, "rejected"),
    ],
)
def test_normalize_cusip9_is_reversible_and_explicit(
    value: object, state: IdentifierState, normalized: str | None, transformation: str
) -> None:
    result = normalize_cusip9(value)

    assert result.original_value == value
    assert result.state is state
    assert result.normalized_cusip9 == normalized
    assert result.transformation == transformation


def test_build_panel_preserves_rows_columns_and_marks_ambiguity(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    output = tmp_path / "panel.parquet"
    _write_source(
        source,
        {
            "unknown_column": ["keep-a", "keep-b", "keep-c", "keep-d"],
            "cusip_id": [" 123abc789 ", "123ABC789", "987654321", "N/A"],
            "trd_exctn_dt": ["2024-01-02", "2024-01-02T09:00:00", "bad", None],
            "pr": [101.5, float("nan"), None, float("inf")],
            "prfull": [None, 10.0, float("nan"), 12.0],
            "bond_maturity": ["2030-01-01", "bad", None, "2031-02-03T00:00:00"],
            "db_type": [1.0, 1.5, None, float("nan")],
        },
    )

    result = build_observed_panel(source, output, ["123abc789", "N/A"], batch_size=1)
    actual = _read_output(output)

    assert actual["unknown_column"] == ["keep-a", "keep-b", "keep-c", "keep-d"]
    assert actual["source_row_number"] == [0, 1, 2, 3]
    assert actual["normalized_cusip9"] == ["123ABC789", "123ABC789", "987654321", None]
    assert actual["cusip_state"] == ["valid_cusip9", "valid_cusip9", "valid_cusip9", "placeholder"]
    assert actual["observation_date"] == ["2024-01-02", "2024-01-02", None, None]
    assert actual["observation_date_state"] == ["present", "present", "invalid", "null"]
    assert actual["pr_state"] == ["present", "invalid", "null", "invalid"]
    assert actual["prfull_state"] == ["null", "present", "invalid", "present"]
    assert actual["bond_maturity_state"] == ["present", "invalid", "null", "present"]
    assert actual["db_type_state"] == ["present", "invalid", "null", "invalid"]
    assert actual["ytm_state"] == ["not_in_schema"] * 4
    assert actual["daily_key_state"] == [
        "duplicate_in_matching_cohort",
        "duplicate_in_matching_cohort",
        "invalid_key",
        "invalid_key",
    ]
    assert result.input_rows == 4
    assert result.output_rows == 4
    assert result.cohort_valid_cusip_count == 1
    assert result.cohort_duplicate_key_count == 1
    assert result.cohort_duplicate_row_count == 2
    assert result.global_uniqueness_proven is False


def test_duplicate_outside_cohort_is_preserved_but_not_counted(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    output = tmp_path / "panel.parquet"
    _write_source(
        source,
        {
            "cusip_id": ["999999991", "999999991"],
            "trd_exctn_dt": ["2024-02-01", "2024-02-01"],
            "pr": [1.0, 2.0],
        },
    )

    result = build_observed_panel(source, output, ["123456789"], batch_size=1)

    assert _read_output(output)["daily_key_state"] == ["not_in_matching_cohort"] * 2
    assert result.cohort_duplicate_key_count == 0
    assert result.cohort_duplicate_row_count == 0


def test_missing_schema_columns_collision_and_cleanup(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    output = tmp_path / "panel.parquet"
    _write_source(source, {"cusip_id": ["123456789"], "trd_exctn_dt": ["2024-01-01"]})

    build_observed_panel(source, output, ["123456789"])
    actual = _read_output(output)
    assert actual["pr_state"] == [FieldState.NOT_IN_SCHEMA]

    with pytest.raises(PilotError, match="already_exists"):
        build_observed_panel(source, output, ["123456789"])
    assert not list(tmp_path.glob("*.partial"))
    assert not list(tmp_path.glob("*.sqlite*"))
