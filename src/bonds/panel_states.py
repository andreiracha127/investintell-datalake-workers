"""Pure observed-panel state derivation over in-memory row batches.

Ported from the bond pilot's ``build_observed_panel`` with the Parquet reader,
streaming writer, and secure-filesystem capability machinery removed. The daily
key state (``unique_in_matching_cohort`` / ``duplicate_in_matching_cohort`` /
``not_in_matching_cohort`` / ``invalid_key``) and every per-field state carry
the exact same semantics as the pilot — this is a pure function over row dicts.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import math
from numbers import Real
import re
from typing import Iterable, Mapping

from .errors import BondError
from .identifiers import normalize_cusip9
from .states import FieldState, IdentifierState


REQUIRED_FIELDS = ("cusip_id", "trd_exctn_dt", "pr")
OPTIONAL_FIELDS = (
    "prfull",
    "acclast",
    "ytm",
    "mod_dur",
    "mac_dur",
    "convexity",
    "bond_maturity",
    "credit_spread",
    "qvolume",
    "dvolume",
    "db_type",
)
_DERIVED_COLUMNS = (
    "normalized_cusip9",
    "cusip_state",
    "cusip_transformation",
    "observation_date",
    "observation_date_state",
    "source_row_number",
    "daily_key_state",
)
_STATE_COLUMNS = tuple(f"{field}_state" for field in (*REQUIRED_FIELDS, *OPTIONAL_FIELDS))
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T")


@dataclass(frozen=True)
class PanelBuildResult:
    input_rows: int
    output_rows: int
    cohort_valid_cusip_count: int
    cohort_duplicate_key_count: int
    cohort_duplicate_row_count: int
    checked_scope: str
    global_uniqueness_proven: bool = False

    def to_mapping(self) -> dict[str, object]:
        return {
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "cohort_valid_cusip_count": self.cohort_valid_cusip_count,
            "cohort_duplicate_key_count": self.cohort_duplicate_key_count,
            "cohort_duplicate_row_count": self.cohort_duplicate_row_count,
            "checked_scope": self.checked_scope,
            "global_uniqueness_proven": self.global_uniqueness_proven,
        }


@dataclass(frozen=True)
class ObservedPanel:
    rows: tuple[Mapping[str, object], ...]
    result: PanelBuildResult


def _parse_date(value: object) -> tuple[str | None, FieldState]:
    if value is None:
        return None, FieldState.NULL
    if isinstance(value, datetime):
        return value.date().isoformat(), FieldState.PRESENT
    if isinstance(value, date):
        return value.isoformat(), FieldState.PRESENT
    if not isinstance(value, str):
        return None, FieldState.INVALID
    try:
        if _ISO_DATE.fullmatch(value):
            return date.fromisoformat(value).isoformat(), FieldState.PRESENT
        if _ISO_DATETIME.match(value):
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat(), FieldState.PRESENT
    except ValueError:
        pass
    return None, FieldState.INVALID


def _numeric_state(value: object) -> FieldState:
    if value is None:
        return FieldState.NULL
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        return FieldState.INVALID
    try:
        return FieldState.PRESENT if math.isfinite(float(value)) else FieldState.INVALID
    except (OverflowError, ValueError):
        return FieldState.INVALID


def _integral_numeric_state(value: object) -> FieldState:
    state = _numeric_state(value)
    if state is not FieldState.PRESENT:
        return state
    try:
        return FieldState.PRESENT if Decimal(str(value)) % 1 == 0 else FieldState.INVALID
    except Exception:
        return FieldState.INVALID


def _field_state(field: str, value: object) -> FieldState:
    if field == "cusip_id":
        identifier = normalize_cusip9(value)
        if identifier.state is IdentifierState.BLANK:
            return FieldState.NULL
        return FieldState.PRESENT if identifier.state is IdentifierState.VALID_CUSIP9 else FieldState.INVALID
    if field == "trd_exctn_dt" or field == "bond_maturity":
        return _parse_date(value)[1]
    if field == "db_type":
        return _integral_numeric_state(value)
    return _numeric_state(value)


def _normalize_cohort(values: Iterable[object]) -> set[str]:
    return {
        result.normalized_cusip9
        for value in values
        if (result := normalize_cusip9(value)).state is IdentifierState.VALID_CUSIP9
        and result.normalized_cusip9 is not None
    }


def build_observed_panel_rows(
    source_rows: Iterable[Mapping[str, object]], cohort_cusips: Iterable[object]
) -> ObservedPanel:
    """Derive identity/date/daily-key/field states for a batch of source rows.

    ``source_rows`` are raw observation rows (each at least ``cusip_id`` and
    ``trd_exctn_dt``). The schema is the union of keys across the batch; a field
    absent from the schema is reported ``not_in_schema`` while a field present
    but ``None`` is reported ``null`` — matching the pilot exactly.
    """
    rows = [dict(row) for row in source_rows]
    source_columns: set[str] = set()
    for row in rows:
        source_columns.update(row)

    missing = [field for field in ("cusip_id", "trd_exctn_dt") if field not in source_columns]
    if missing:
        raise BondError("missing_required_columns", {"columns": missing})
    collisions = sorted(source_columns.intersection((*_DERIVED_COLUMNS, *_STATE_COLUMNS)))
    if collisions:
        raise BondError("derived_column_collision", {"columns": collisions})

    cohort = _normalize_cohort(cohort_cusips)

    # First pass: count cohort (cusip, day) keys so uniqueness is a batch fact.
    key_counts: Counter[tuple[str, str]] = Counter()
    for row in rows:
        cusip = normalize_cusip9(row.get("cusip_id")).normalized_cusip9
        day, date_state = _parse_date(row.get("trd_exctn_dt"))
        if cusip in cohort and day is not None and date_state is FieldState.PRESENT:
            key_counts[(cusip, day)] += 1

    # Second pass: emit source columns plus the derived state columns.
    output: list[dict[str, object]] = []
    for source_row_number, row in enumerate(rows):
        identifier = normalize_cusip9(row.get("cusip_id"))
        day, date_state = _parse_date(row.get("trd_exctn_dt"))
        cusip = identifier.normalized_cusip9

        if cusip is None or day is None or date_state is not FieldState.PRESENT:
            daily_key_state = "invalid_key"
        elif cusip not in cohort:
            daily_key_state = "not_in_matching_cohort"
        elif key_counts[(cusip, day)] == 1:
            daily_key_state = "unique_in_matching_cohort"
        else:
            daily_key_state = "duplicate_in_matching_cohort"

        derived: dict[str, object] = dict(row)
        derived["normalized_cusip9"] = identifier.normalized_cusip9
        derived["cusip_state"] = identifier.state.value
        derived["cusip_transformation"] = identifier.transformation
        derived["observation_date"] = day
        derived["observation_date_state"] = date_state.value
        derived["source_row_number"] = source_row_number
        derived["daily_key_state"] = daily_key_state
        for field in (*REQUIRED_FIELDS, *OPTIONAL_FIELDS):
            if field not in source_columns:
                state = FieldState.NOT_IN_SCHEMA
            else:
                state = _field_state(field, row.get(field))
            derived[f"{field}_state"] = state.value
        output.append(derived)

    duplicate_key_count = sum(1 for count in key_counts.values() if count > 1)
    duplicate_row_count = sum(count for count in key_counts.values() if count > 1)
    result = PanelBuildResult(
        input_rows=len(rows),
        output_rows=len(rows),
        cohort_valid_cusip_count=len(cohort),
        cohort_duplicate_key_count=duplicate_key_count,
        cohort_duplicate_row_count=duplicate_row_count,
        checked_scope="matching_cohort_cusip_date_keys",
    )
    return ObservedPanel(tuple(output), result)
