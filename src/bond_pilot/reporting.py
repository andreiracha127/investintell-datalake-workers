"""Collision-safe internal-only evidence output for the bond-pilot fixture."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from enum import Enum
import math
from pathlib import Path
from typing import Iterable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .artifacts import canonical_json_bytes, commit_partial, partial_path, replace_checkpoint, write_checksums, write_json_once, write_text_once
from .contracts import PilotError, SourceApproval, SourceCandidate
from .debt_mapping import DebtMapping
from .matching import CrossSeriesSummary, MatchResult, Observation, SeriesMetric
from .panel import PanelBuildResult


def _plain(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {item.name: _plain(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _json(value: object) -> str:
    return canonical_json_bytes(_plain(value)).decode("utf-8").rstrip("\n")


def _finite(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


_MATCH_SCHEMA = pa.schema([
    pa.field("publication_id", pa.string()), pa.field("accession_number", pa.string()), pa.field("holding_id", pa.string()), pa.field("source_run_id", pa.string()),
    pa.field("report_date", pa.string()), pa.field("filing_date", pa.string()), pa.field("series_id", pa.string()), pa.field("class_id", pa.string()),
    pa.field("instrument_id", pa.string()), pa.field("issuer_category", pa.string()), pa.field("original_cusip", pa.string()), pa.field("normalized_cusip9", pa.string()),
    pa.field("signed_market_value_raw", pa.string()), pa.field("signed_pct_of_nav_raw", pa.string()), pa.field("currency", pa.string()), pa.field("raw_values_json", pa.string()),
    pa.field("debt_state", pa.string()), pa.field("state", pa.string()), pa.field("observation_date", pa.string()), pa.field("observation_age_days", pa.int64()),
    pa.field("is_144a", pa.bool_()), pa.field("observation_price", pa.float64()), pa.field("observations_json", pa.string()),
])
_METRIC_SCHEMA = pa.schema([
    pa.field("series_id", pa.string()), pa.field("report_date", pa.string()), pa.field("publication_id", pa.string()), pa.field("source_run_id", pa.string()),
    pa.field("state_counts_json", pa.string()), pa.field("denominator_diagnostics_json", pa.string()), pa.field("denominator_weight", pa.float64()),
    pa.field("numerator_weight", pa.float64()), pa.field("nav_ratio", pa.float64()), pa.field("eligible_market_value_by_currency_json", pa.string()),
    pa.field("matched_market_value_by_currency_json", pa.string()), pa.field("market_value_diagnostics_json", pa.string()),
])
_LATEST_SCHEMA = pa.schema([
    pa.field("cusip", pa.string()), pa.field("observation_date", pa.string()), pa.field("source_row_number", pa.int64()), pa.field("price", pa.float64()),
    pa.field("price_raw", pa.string()), pa.field("price_state", pa.string()), pa.field("ytm_json", pa.string()), pa.field("db_type", pa.string()),
    pa.field("db_type_state", pa.string()), pa.field("daily_key_state", pa.string()),
], metadata={b"historical_input": b"false", b"internal_only": b"true"})


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _match_rows(matches: Iterable[MatchResult], mapping: DebtMapping) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for match in matches:
        holding = match.holding
        rows.append({
            "publication_id": _text(holding.publication_id), "accession_number": _text(holding.accession_number), "holding_id": _text(holding.holding_id),
            "source_run_id": _text(holding.source_run_id), "report_date": _text(holding.report_date), "filing_date": _text(holding.filing_date),
            "series_id": _text(holding.series_id), "class_id": _text(holding.class_id), "instrument_id": _text(holding.instrument_id), "issuer_category": _text(holding.issuer_category),
            "original_cusip": _text(holding.original_cusip), "normalized_cusip9": match.normalized_cusip9, "signed_market_value_raw": _json(holding.signed_market_value),
            "signed_pct_of_nav_raw": _json(holding.signed_pct_of_nav), "currency": _text(holding.currency), "raw_values_json": _json(holding.raw_values or {}),
            "debt_state": mapping.classify(holding.issuer_category).value, "state": match.state.value, "observation_date": match.observation_date,
            "observation_age_days": match.observation_age_days, "is_144a": match.is_144a,
            "observation_price": _finite(match.observations[0].price) if match.observations else None, "observations_json": _json(match.observations),
        })
    return rows


def _metric_rows(metrics: Iterable[SeriesMetric]) -> list[dict[str, object]]:
    return [{
        "series_id": _text(metric.series_id), "report_date": _text(metric.report_date), "publication_id": _text(metric.publication_id), "source_run_id": _text(metric.source_run_id),
        "state_counts_json": _json(metric.state_counts), "denominator_diagnostics_json": _json(metric.denominator_diagnostics), "denominator_weight": _finite(metric.denominator_weight),
        "numerator_weight": _finite(metric.numerator_weight), "nav_ratio": _finite(metric.nav_ratio), "eligible_market_value_by_currency_json": _json(_known_currencies(metric.eligible_market_value_by_currency)),
        "matched_market_value_by_currency_json": _json(_known_currencies(metric.matched_market_value_by_currency)), "market_value_diagnostics_json": _json(metric.market_value_diagnostics),
    } for metric in metrics]


def _latest_rows(observations: Iterable[Observation]) -> list[dict[str, object]]:
    return [{"cusip": row.cusip, "observation_date": row.observation_date, "source_row_number": row.source_row_number, "price": _finite(row.price), "price_raw": _json(row.price), "price_state": _text(row.price_state), "ytm_json": _json(row.ytm), "db_type": _text(row.db_type), "db_type_state": _text(row.db_type_state), "daily_key_state": _text(row.daily_key_state)} for row in observations]


def _known_currencies(values: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in values.items() if key != "UNKNOWN"}


def _write_parquet_once(path: Path, schema: pa.Schema, rows: list[dict[str, object]]) -> Path:
    if path.exists():
        raise PilotError("already_exists", {"path": str(path)})
    partial = partial_path(path)
    try:
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, partial)
        return commit_partial(partial, path)
    except Exception:
        partial.unlink(missing_ok=True)
        raise


def _copy_panel_once(source: Path, destination: Path) -> Path:
    if source.resolve() == destination.resolve():
        if not source.is_file():
            raise PilotError("missing_panel", {"path": str(source)})
        return destination
    if destination.exists():
        raise PilotError("already_exists", {"path": str(destination)})
    partial = partial_path(destination)
    try:
        with source.open("rb") as input_file, partial.open("xb") as output_file:
            for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                output_file.write(chunk)
        return commit_partial(partial, destination)
    except Exception:
        partial.unlink(missing_ok=True)
        raise


def write_internal_reports(*, run_dir: str | Path, source_candidate: SourceCandidate, source_approval: SourceApproval, debt_mapping: DebtMapping, mapping_provenance: Mapping[str, object], nport_manifest: Mapping[str, object], panel_result: PanelBuildResult, panel_path: str | Path, matches: Iterable[MatchResult], series_metrics: Iterable[SeriesMetric], cross_series_summary: CrossSeriesSummary, latest_observations: Iterable[Observation], calibration_report: Mapping[str, object], checkpoint: Mapping[str, object] | None = None) -> Mapping[str, Path]:
    """Emit a complete local evidence pack; this intentionally contains unredacted provenance."""
    root = Path(run_dir)
    root.mkdir(parents=True, exist_ok=True)
    match_values = tuple(matches)
    metric_values = tuple(series_metrics)
    latest_values = tuple(latest_observations)
    match_rows = _match_rows(match_values, debt_mapping)
    metric_rows = _metric_rows(metric_values)
    latest_rows = _latest_rows(latest_values)
    source_manifest = {**source_candidate.to_json_mapping(), "approval": source_approval.to_json_mapping(), "internal_only": True}
    calibration = {**dict(calibration_report), "status": "not_started", "phase4_state": "pre_backfill", "representative_post_backfill": False, "db_reads": 0, "db_writes": 0}
    quality = {
        "internal_only": True, "source": source_manifest, "mapping": {**debt_mapping.to_mapping(), "provenance": _plain(mapping_provenance)},
        "nport": _plain(nport_manifest), "panel": _plain(panel_result), "state_counts": {key: sum(1 for row in match_rows if row["state"] == key) for key in sorted({str(row["state"]) for row in match_rows})},
        "invalid_weight_diagnostics": {"by_series": [_plain(metric.denominator_diagnostics) for metric in metric_values]},
        "market_diagnostics": {"by_series": [_plain(metric.market_value_diagnostics) for metric in metric_values], "currency_values_no_fx": [_plain(_known_currencies(metric.eligible_market_value_by_currency)) for metric in metric_values]},
        "cross_series": _plain(cross_series_summary), "latest_lane": {"historical_input": False}, "db_reads": nport_manifest.get("db_reads", 0), "db_writes": nport_manifest.get("db_writes", 0), "representative": False,
    }
    paths: dict[str, Path] = {}
    paths["source_manifest"] = write_json_once(root / "source-manifest.json", source_manifest)
    paths["nport_extract_manifest"] = write_json_once(root / "nport-extract-manifest.json", _plain(nport_manifest))
    paths["calibration_report"] = write_json_once(root / "calibration-report.json", calibration)
    paths["bond_observed_daily"] = _copy_panel_once(Path(panel_path), root / "bond-observed-daily.parquet")
    paths["fund_asof_match"] = _write_parquet_once(root / "fund-asof-match.parquet", _MATCH_SCHEMA, match_rows)
    paths["fund_series_metrics"] = _write_parquet_once(root / "fund-series-metrics.parquet", _METRIC_SCHEMA, metric_rows)
    paths["bond_latest"] = _write_parquet_once(root / "bond-latest.parquet", _LATEST_SCHEMA, latest_rows)
    paths["quality_summary"] = write_json_once(root / "quality-summary.json", quality)
    report = "# Bond pilot internal report\n\nInternal-only local/offline/no-write fixture run. Phase state: pre-backfill; representative: false. Full internal provenance is expected. Latest lane is isolated (`historical_input:false`). No frontend, API, or production claim.\n"
    paths["pilot_report"] = write_text_once(root / "pilot-report.md", report)
    if checkpoint is not None or calibration_report:
        paths["checkpoint"] = replace_checkpoint(root / "checkpoint.json", canonical_json_bytes(_plain(checkpoint or {"calibration_attempted": True})))
    paths["checksums"] = write_checksums(root)
    return paths
