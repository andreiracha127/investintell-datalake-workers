"""Bounded-memory observed-panel construction with explicit ambiguity states."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import math
from numbers import Real
from pathlib import Path
import re
import sqlite3
from typing import Iterable
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from .artifacts import commit_partial, partial_path
from .contracts import FieldState, IdentifierState, PilotError
from .identifiers import normalize_cusip9


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
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T")
_PARQUET_WRITER_FACTORY = pq.ParquetWriter


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


def _derived_schema(source_schema: pa.Schema) -> pa.Schema:
    field_names = set(source_schema.names)
    state_columns = tuple(f"{field}_state" for field in (*REQUIRED_FIELDS, *OPTIONAL_FIELDS))
    derived_names = (*_DERIVED_COLUMNS, *state_columns)
    collisions = sorted(field_names.intersection(derived_names))
    if collisions:
        raise PilotError("derived_column_collision", {"columns": collisions})
    fields = list(source_schema)
    fields.extend(
        (
            pa.field("normalized_cusip9", pa.string()),
            pa.field("cusip_state", pa.string()),
            pa.field("cusip_transformation", pa.string()),
            pa.field("observation_date", pa.string()),
            pa.field("observation_date_state", pa.string()),
            pa.field("source_row_number", pa.int64()),
            pa.field("daily_key_state", pa.string()),
        )
    )
    fields.extend(pa.field(name, pa.string()) for name in state_columns)
    return pa.schema(fields, metadata=source_schema.metadata)


def _normalize_cohort(values: Iterable[object]) -> set[str]:
    return {
        result.normalized_cusip9
        for value in values
        if (result := normalize_cusip9(value)).state is IdentifierState.VALID_CUSIP9
        and result.normalized_cusip9 is not None
    }


def _temp_sqlite_path(output_path: Path) -> Path:
    return output_path.parent / f".{output_path.name}.{uuid4().hex}.sqlite"


def _count_matching_keys(parquet: pq.ParquetFile, cohort: set[str], database: sqlite3.Connection, batch_size: int) -> None:
    database.execute("CREATE TABLE daily_keys (cusip TEXT NOT NULL, day TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY (cusip, day))")
    if not cohort:
        return
    for batch in parquet.iter_batches(columns=["cusip_id", "trd_exctn_dt"], batch_size=batch_size):
        cusips = batch.column(0).to_pylist()
        dates = batch.column(1).to_pylist()
        rows: list[tuple[str, str]] = []
        for raw_cusip, raw_date in zip(cusips, dates, strict=True):
            cusip = normalize_cusip9(raw_cusip).normalized_cusip9
            day, date_state = _parse_date(raw_date)
            if cusip in cohort and day is not None and date_state is FieldState.PRESENT:
                rows.append((cusip, day))
        if rows:
            database.executemany(
                "INSERT INTO daily_keys (cusip, day, count) VALUES (?, ?, 1) "
                "ON CONFLICT(cusip, day) DO UPDATE SET count = count + 1",
                rows,
            )
    database.commit()


def _key_count(database: sqlite3.Connection, cusip: str, day: str) -> int:
    row = database.execute("SELECT count FROM daily_keys WHERE cusip = ? AND day = ?", (cusip, day)).fetchone()
    return 0 if row is None else int(row[0])


def _cleanup(writer: object | None, partial: Path, sqlite_path: Path) -> None:
    """Attempt every cleanup step without masking the operational failure."""
    if writer is not None:
        try:
            writer.close()  # type: ignore[attr-defined]
        except Exception:
            pass
    for path in (partial, sqlite_path):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass


def _append_derived(
    batch: pa.RecordBatch,
    source_columns: set[str],
    cohort: set[str],
    database: sqlite3.Connection,
    row_start: int,
) -> pa.RecordBatch:
    values = {name: batch.column(batch.schema.get_field_index(name)).to_pylist() for name in source_columns}
    identifiers = [normalize_cusip9(value) for value in values["cusip_id"]]
    dates = [_parse_date(value) for value in values["trd_exctn_dt"]]
    result = batch
    result = result.append_column("normalized_cusip9", pa.array([value.normalized_cusip9 for value in identifiers], type=pa.string()))
    result = result.append_column("cusip_state", pa.array([value.state.value for value in identifiers], type=pa.string()))
    result = result.append_column("cusip_transformation", pa.array([value.transformation for value in identifiers], type=pa.string()))
    result = result.append_column("observation_date", pa.array([value[0] for value in dates], type=pa.string()))
    result = result.append_column("observation_date_state", pa.array([value[1].value for value in dates], type=pa.string()))
    result = result.append_column("source_row_number", pa.array(range(row_start, row_start + batch.num_rows), type=pa.int64()))

    daily_states: list[str] = []
    for identifier, (day, date_state) in zip(identifiers, dates, strict=True):
        if identifier.normalized_cusip9 is None or day is None or date_state is not FieldState.PRESENT:
            daily_states.append("invalid_key")
        elif identifier.normalized_cusip9 not in cohort:
            daily_states.append("not_in_matching_cohort")
        elif _key_count(database, identifier.normalized_cusip9, day) == 1:
            daily_states.append("unique_in_matching_cohort")
        else:
            daily_states.append("duplicate_in_matching_cohort")
    result = result.append_column("daily_key_state", pa.array(daily_states, type=pa.string()))

    for field in (*REQUIRED_FIELDS, *OPTIONAL_FIELDS):
        if field not in source_columns:
            states = [FieldState.NOT_IN_SCHEMA.value] * batch.num_rows
        else:
            states = [_field_state(field, value).value for value in values[field]]
        result = result.append_column(f"{field}_state", pa.array(states, type=pa.string()))
    return result


def build_observed_panel(
    source_parquet: str | Path,
    output_path: str | Path,
    cohort_cusips: Iterable[object],
    batch_size: int = 65_536,
) -> PanelBuildResult:
    """Stream an observed panel without repairing, selecting, or averaging source rows."""
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
        raise PilotError("invalid_batch_size", {"batch_size": batch_size})
    source_path = Path(source_parquet)
    final_path = Path(output_path)
    if final_path.exists():
        raise PilotError("already_exists", {"path": str(final_path)})
    cohort = _normalize_cohort(cohort_cusips)
    partial = partial_path(final_path)
    sqlite_path = _temp_sqlite_path(final_path)
    writer: pq.ParquetWriter | None = None
    input_rows = 0
    try:
        with pq.ParquetFile(source_path) as parquet:
            source_columns = set(parquet.schema_arrow.names)
            missing = [field for field in ("cusip_id", "trd_exctn_dt") if field not in source_columns]
            if missing:
                raise PilotError("missing_required_columns", {"columns": missing})
            schema = _derived_schema(parquet.schema_arrow)
            database = sqlite3.connect(sqlite_path)
            try:
                _count_matching_keys(parquet, cohort, database, batch_size)
                writer = _PARQUET_WRITER_FACTORY(partial, schema)
                for batch in parquet.iter_batches(batch_size=batch_size):
                    writer.write_batch(_append_derived(batch, source_columns, cohort, database, input_rows))
                    input_rows += batch.num_rows
                writer.close()
                writer = None
                duplicate_key_count, duplicate_row_count = database.execute(
                    "SELECT COUNT(*), COALESCE(SUM(count), 0) FROM daily_keys WHERE count > 1"
                ).fetchone()
            finally:
                database.close()
        commit_partial(partial, final_path)
        return PanelBuildResult(
            input_rows=input_rows,
            output_rows=input_rows,
            cohort_valid_cusip_count=len(cohort),
            cohort_duplicate_key_count=int(duplicate_key_count),
            cohort_duplicate_row_count=int(duplicate_row_count),
            checked_scope="matching_cohort_cusip_date_keys",
        )
    finally:
        _cleanup(writer, partial, sqlite_path)
