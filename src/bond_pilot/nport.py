"""Offline N-PORT fixture loading for the pre-backfill bond-pilot run."""

from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .artifacts import sha256_file
from .contracts import PilotError
from .matching import HoldingRecord


_SCHEMA_VERSION = "nport-fixture-v1"
_REQUIRED_FIELDS = (
    "publication_id", "accession_number", "holding_id", "source_run_id", "report_date", "filing_date",
    "series_id", "class_id", "instrument_id", "issuer_category", "cusip", "signed_market_value",
    "signed_pct_of_nav", "currency",
)
_PHYSICAL_LINEAGE_FIELDS = ("publication_id", "accession_number", "holding_id", "source_run_id", "series_id", "instrument_id")


def _read_fixture(path: Path) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PilotError("nport_invalid_fixture") from exc
    if not isinstance(value, dict):
        raise PilotError("nport_invalid_fixture")
    if value.get("schema_version") != _SCHEMA_VERSION:
        raise PilotError("nport_invalid_schema_version")
    if value.get("phase4_state") != "pre_backfill":
        raise PilotError("nport_invalid_phase4_state")
    return value


def _nonempty(value: object, field: str, row_number: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PilotError("nport_missing_lineage", {"field": field, "row": row_number})
    return value


def _iso_date(value: object, field: str, row_number: int) -> str:
    if not isinstance(value, str):
        raise PilotError("nport_invalid_date", {"field": field, "row": row_number})
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise PilotError("nport_invalid_date", {"field": field, "row": row_number}) from exc
    if parsed.isoformat() != value:
        raise PilotError("nport_invalid_date", {"field": field, "row": row_number})
    return value


def _frozen_raw(value: Mapping[str, object]) -> Mapping[str, object]:
    def freeze(item: object) -> object:
        if isinstance(item, Mapping):
            return MappingProxyType({str(key): freeze(child) for key, child in item.items()})
        if isinstance(item, list):
            return tuple(freeze(child) for child in item)
        return deepcopy(item)
    return MappingProxyType({str(key): freeze(item) for key, item in value.items()})


def load_fixture_holdings(path: str | Path, max_rows: int = 10_000) -> tuple[HoldingRecord, ...]:
    """Load a bounded, deliberately offline N-PORT fixture without DB access."""
    if not isinstance(max_rows, int) or isinstance(max_rows, bool) or max_rows < 0:
        raise PilotError("nport_invalid_row_limit")
    value = _read_fixture(Path(path))
    rows = value.get("holdings")
    if not isinstance(rows, list):
        raise PilotError("nport_invalid_holdings")
    effective_limit = min(max_rows, 10_000)
    if len(rows) > effective_limit:
        raise PilotError("nport_row_limit_exceeded", {"max_rows": effective_limit, "row_count": len(rows)})
    holdings: list[HoldingRecord] = []
    lots: set[tuple[str, str]] = set()
    for row_number, row in enumerate(rows):
        if not isinstance(row, dict):
            raise PilotError("nport_invalid_holding_row", {"row": row_number})
        missing = [field for field in _REQUIRED_FIELDS if field not in row]
        if missing:
            raise PilotError("nport_missing_field", {"field": missing[0], "row": row_number})
        lineage = {field: _nonempty(row[field], field, row_number) for field in _PHYSICAL_LINEAGE_FIELDS}
        report_date = _iso_date(row["report_date"], "report_date", row_number)
        filing_date = _iso_date(row["filing_date"], "filing_date", row_number)
        lot = (lineage["accession_number"], lineage["holding_id"])
        if lot in lots:
            raise PilotError("nport_duplicate_lot", {"accession_number": lot[0], "holding_id": lot[1]})
        lots.add(lot)
        holdings.append(HoldingRecord(
            publication_id=lineage["publication_id"], accession_number=lineage["accession_number"], holding_id=lineage["holding_id"],
            source_run_id=lineage["source_run_id"], report_date=report_date, filing_date=filing_date, series_id=lineage["series_id"],
            class_id=row["class_id"], instrument_id=lineage["instrument_id"], issuer_category=row["issuer_category"], original_cusip=row["cusip"],
            signed_market_value=row["signed_market_value"], signed_pct_of_nav=row["signed_pct_of_nav"], currency=row["currency"], raw_values=_frozen_raw(row),
        ))
    return tuple(holdings)


def fixture_manifest(path: str | Path, holdings: tuple[HoldingRecord, ...]) -> dict[str, object]:
    """Return immutable-run provenance derived from the raw fixture bytes."""
    fixture_path = Path(path)
    return {
        "schema_version": "nport-extract-manifest-v1", "extraction_mode": "fixture", "phase4_state": "pre_backfill",
        "representative_post_backfill": False, "db_reads": 0, "db_writes": 0, "fixture_path": str(fixture_path),
        "fixture_sha256": sha256_file(fixture_path), "row_count": len(holdings),
        "distinct_lot_count": len({(row.accession_number, row.holding_id) for row in holdings}),
        "distinct_series_count": len({row.series_id for row in holdings}), "distinct_accession_count": len({row.accession_number for row in holdings}),
        "lineage_fields_present": all(all(getattr(row, field) for field in _PHYSICAL_LINEAGE_FIELDS) for row in holdings),
    }
