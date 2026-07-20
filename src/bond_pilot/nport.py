"""Offline, bounded N-PORT fixture loading for the pre-backfill pilot."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .artifacts import read_secure_local_file
from .contracts import PilotError
from .matching import HoldingRecord


MAX_FIXTURE_BYTES = 64 * 1024 * 1024
_SCHEMA_VERSION = "nport-fixture-v1"
_REQUIRED_FIELDS = ("publication_id", "accession_number", "holding_id", "source_run_id", "report_date", "filing_date", "series_id", "class_id", "instrument_id", "issuer_category", "asset_class", "instrument_structure", "cusip", "signed_market_value", "signed_pct_of_nav", "currency")
_PHYSICAL_LINEAGE_FIELDS = ("publication_id", "accession_number", "holding_id", "source_run_id", "series_id", "instrument_id")


@dataclass(frozen=True)
class FixtureLoadResult:
    fixture_path: str
    fixture_sha256: str
    holdings: tuple[HoldingRecord, ...]
    row_count: int
    distinct_lot_count: int
    distinct_series_count: int
    distinct_accession_count: int

    def manifest(self) -> dict[str, object]:
        return {
            "schema_version": "nport-extract-manifest-v1", "extraction_mode": "fixture", "phase4_state": "pre_backfill",
            "representative_post_backfill": False, "db_reads": 0, "db_writes": 0, "fixture_path": self.fixture_path,
            "fixture_sha256": self.fixture_sha256, "row_count": self.row_count, "distinct_lot_count": self.distinct_lot_count,
            "distinct_series_count": self.distinct_series_count, "distinct_accession_count": self.distinct_accession_count,
            "lineage_fields_present": True,
        }


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> object:
    raise ValueError("non-finite JSON constant")


def _read_fixture_bytes(path: str | Path) -> tuple[Path, bytes]:
    return read_secure_local_file(
        path,
        max_bytes=MAX_FIXTURE_BYTES,
        error_code="nport_invalid_fixture",
        too_large_code="nport_fixture_too_large",
    )


def _parse_rows(raw: bytes, max_rows: int) -> list[object]:
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise PilotError("nport_invalid_fixture") from exc
    if not isinstance(value, dict):
        raise PilotError("nport_invalid_fixture")
    if value.get("schema_version") != _SCHEMA_VERSION:
        raise PilotError("nport_invalid_schema_version")
    if value.get("phase4_state") != "pre_backfill":
        raise PilotError("nport_invalid_phase4_state")
    rows = value.get("holdings")
    if not isinstance(rows, list):
        raise PilotError("nport_invalid_holdings")
    effective_limit = min(max_rows, 10_000)
    if len(rows) > effective_limit:
        raise PilotError("nport_row_limit_exceeded", {"max_rows": effective_limit, "row_count": len(rows)})
    return rows


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


def _holdings_from_rows(rows: list[object]) -> tuple[HoldingRecord, ...]:
    holdings: list[HoldingRecord] = []
    lots: set[tuple[str, str]] = set()
    for row_number, row in enumerate(rows):
        if not isinstance(row, dict):
            raise PilotError("nport_invalid_holding_row", {"row": row_number})
        missing = [field for field in _REQUIRED_FIELDS if field not in row]
        if missing:
            raise PilotError("nport_missing_field", {"field": missing[0], "row": row_number})
        lineage = {field: _nonempty(row[field], field, row_number) for field in _PHYSICAL_LINEAGE_FIELDS}
        report_date, filing_date = _iso_date(row["report_date"], "report_date", row_number), _iso_date(row["filing_date"], "filing_date", row_number)
        lot = (lineage["accession_number"], lineage["holding_id"])
        if lot in lots:
            raise PilotError("nport_duplicate_lot", {"accession_number": lot[0], "holding_id": lot[1]})
        lots.add(lot)
        holdings.append(HoldingRecord(publication_id=lineage["publication_id"], accession_number=lineage["accession_number"], holding_id=lineage["holding_id"], source_run_id=lineage["source_run_id"], report_date=report_date, filing_date=filing_date, series_id=lineage["series_id"], class_id=row["class_id"], instrument_id=lineage["instrument_id"], issuer_category=row["issuer_category"], asset_class=row["asset_class"], instrument_structure=row["instrument_structure"], original_cusip=row["cusip"], signed_market_value=row["signed_market_value"], signed_pct_of_nav=row["signed_pct_of_nav"], currency=row["currency"], raw_values=_frozen_raw(row)))
    return tuple(holdings)


def load_fixture_result(path: str | Path, max_rows: int = 10_000) -> FixtureLoadResult:
    """Read, hash, parse, and materialize one regular fixture file exactly once."""
    if not isinstance(max_rows, int) or isinstance(max_rows, bool) or max_rows < 0:
        raise PilotError("nport_invalid_row_limit")
    fixture_path, raw = _read_fixture_bytes(path)
    holdings = _holdings_from_rows(_parse_rows(raw, max_rows))
    return FixtureLoadResult(str(fixture_path), hashlib.sha256(raw).hexdigest(), holdings, len(holdings), len({(row.accession_number, row.holding_id) for row in holdings}), len({row.series_id for row in holdings}), len({row.accession_number for row in holdings}))


def load_fixture_holdings(path: str | Path, max_rows: int = 10_000) -> tuple[HoldingRecord, ...]:
    return load_fixture_result(path, max_rows).holdings


def fixture_manifest(path: str | Path, holdings: tuple[HoldingRecord, ...]) -> dict[str, object]:
    result = load_fixture_result(path)
    if result.holdings != holdings:
        raise PilotError("fixture_manifest_mismatch")
    return result.manifest()
