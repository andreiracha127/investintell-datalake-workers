"""Pure daily_key_state / field-state derivation over in-memory row batches.

Ported from the pilot's observed-panel construction with the Parquet/streaming
I/O and capability machinery stripped out (pure function over row dicts).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Mapping, Sequence

import pytest

from src.bonds.errors import BondError
from src.bonds.panel_states import ObservedPanel, build_observed_panel_rows
from src.bonds.states import FieldState


def _col(panel: ObservedPanel, key: str) -> list[object]:
    return [row[key] for row in panel.rows]


def _rows(*dicts: Mapping[str, object]) -> Sequence[Mapping[str, object]]:
    return list(dicts)


def test_non_string_cusips_are_not_cohort_keys_or_source_identifiers() -> None:
    panel = build_observed_panel_rows(
        _rows({"cusip_id": 123456789, "trd_exctn_dt": "2024-01-01", "pr": 1.0}),
        [123456789, Decimal("123456789")],
    )
    assert _col(panel, "normalized_cusip9") == [None]
    assert _col(panel, "cusip_state") == ["invalid_format"]
    assert _col(panel, "daily_key_state") == ["invalid_key"]
    assert panel.result.cohort_valid_cusip_count == 0


def test_build_panel_preserves_rows_columns_and_marks_ambiguity() -> None:
    panel = build_observed_panel_rows(
        _rows(
            {"unknown_column": "keep-a", "cusip_id": " 123abc789 ", "trd_exctn_dt": "2024-01-02", "pr": 101.5, "prfull": None, "bond_maturity": "2030-01-01", "db_type": 1.0},
            {"unknown_column": "keep-b", "cusip_id": "123ABC789", "trd_exctn_dt": "2024-01-02T09:00:00", "pr": float("nan"), "prfull": 10.0, "bond_maturity": "bad", "db_type": 1.5},
            {"unknown_column": "keep-c", "cusip_id": "987654321", "trd_exctn_dt": "bad", "pr": None, "prfull": float("nan"), "bond_maturity": None, "db_type": None},
            {"unknown_column": "keep-d", "cusip_id": "N/A", "trd_exctn_dt": None, "pr": float("inf"), "prfull": 12.0, "bond_maturity": "2031-02-03T00:00:00", "db_type": float("nan")},
        ),
        ["123abc789", "N/A"],
    )

    assert _col(panel, "unknown_column") == ["keep-a", "keep-b", "keep-c", "keep-d"]
    assert _col(panel, "source_row_number") == [0, 1, 2, 3]
    assert _col(panel, "normalized_cusip9") == ["123ABC789", "123ABC789", "987654321", None]
    assert _col(panel, "cusip_state") == ["valid_cusip9", "valid_cusip9", "valid_cusip9", "placeholder"]
    assert _col(panel, "observation_date") == ["2024-01-02", "2024-01-02", None, None]
    assert _col(panel, "observation_date_state") == ["present", "present", "invalid", "null"]
    assert _col(panel, "pr_state") == ["present", "invalid", "null", "invalid"]
    assert _col(panel, "prfull_state") == ["null", "present", "invalid", "present"]
    assert _col(panel, "bond_maturity_state") == ["present", "invalid", "null", "present"]
    assert _col(panel, "db_type_state") == ["present", "invalid", "null", "invalid"]
    assert _col(panel, "ytm_state") == ["not_in_schema"] * 4
    assert _col(panel, "daily_key_state") == [
        "duplicate_in_matching_cohort",
        "duplicate_in_matching_cohort",
        "invalid_key",
        "invalid_key",
    ]
    assert panel.result.input_rows == 4
    assert panel.result.output_rows == 4
    assert panel.result.cohort_valid_cusip_count == 1
    assert panel.result.cohort_duplicate_key_count == 1
    assert panel.result.cohort_duplicate_row_count == 2
    assert panel.result.global_uniqueness_proven is False


def test_offset_timestamp_uses_parsed_calendar_date_without_tz_conversion() -> None:
    panel = build_observed_panel_rows(
        _rows({"cusip_id": "123456789", "trd_exctn_dt": "2024-01-02T00:30:00+01:00", "pr": 1.0}),
        ["123456789"],
    )
    assert _col(panel, "observation_date") == ["2024-01-02"]


def test_duplicate_outside_cohort_is_preserved_but_not_counted() -> None:
    panel = build_observed_panel_rows(
        _rows(
            {"cusip_id": "999999991", "trd_exctn_dt": "2024-02-01", "pr": 1.0},
            {"cusip_id": "999999991", "trd_exctn_dt": "2024-02-01", "pr": 2.0},
        ),
        ["123456789"],
    )
    assert _col(panel, "daily_key_state") == ["not_in_matching_cohort"] * 2
    assert panel.result.cohort_duplicate_key_count == 0
    assert panel.result.cohort_duplicate_row_count == 0


def test_unique_in_matching_cohort_for_singleton_cohort_key() -> None:
    panel = build_observed_panel_rows(
        _rows({"cusip_id": "123456789", "trd_exctn_dt": "2024-01-02", "pr": 1.0}),
        ["123456789"],
    )
    assert _col(panel, "daily_key_state") == ["unique_in_matching_cohort"]
    assert panel.result.cohort_duplicate_key_count == 0


def test_missing_optional_columns_are_not_in_schema() -> None:
    panel = build_observed_panel_rows(
        _rows({"cusip_id": "123456789", "trd_exctn_dt": "2024-01-01"}),
        ["123456789"],
    )
    assert _col(panel, "pr_state") == [FieldState.NOT_IN_SCHEMA.value]
    assert _col(panel, "ytm_state") == [FieldState.NOT_IN_SCHEMA.value]


def test_missing_required_columns_raise() -> None:
    with pytest.raises(BondError, match="missing_required_columns"):
        build_observed_panel_rows(_rows({"cusip_id": "123456789"}), ["123456789"])


def test_derived_column_collision_is_rejected() -> None:
    with pytest.raises(BondError, match="derived_column_collision"):
        build_observed_panel_rows(
            _rows({"cusip_id": "123456789", "trd_exctn_dt": "2024-01-01", "normalized_cusip9": "x"}),
            ["123456789"],
        )
