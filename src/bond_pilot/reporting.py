"""Atomic internal-only evidence output for the pre-backfill bond-pilot."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from enum import Enum
import math
from pathlib import Path
import re
import shutil
from typing import Iterable, Mapping
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from .artifacts import canonical_json_bytes, commit_partial, partial_path, replace_checkpoint, write_checksums, write_json_once, write_text_once
from .contracts import PilotError, SourceApproval, SourceCandidate
from .debt_mapping import DebtMapping
from .matching import CrossSeriesSummary, MatchResult, Observation, SeriesMetric, compute_cross_series_summary, compute_series_metrics, validate_match_categories
from .nport import FixtureLoadResult, load_fixture_result
from .panel import PanelBuildResult
from .source_artifact import _path_lexists, _publish_directory_no_replace


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MATCH_SCHEMA = pa.schema([
    pa.field("publication_id", pa.string()), pa.field("accession_number", pa.string()), pa.field("holding_id", pa.string()), pa.field("source_run_id", pa.string()),
    pa.field("report_date", pa.string()), pa.field("filing_date", pa.string()), pa.field("series_id", pa.string()), pa.field("class_id", pa.string()),
    pa.field("instrument_id", pa.string()), pa.field("issuer_category", pa.string()), pa.field("asset_class", pa.string()), pa.field("instrument_structure", pa.string()), pa.field("original_cusip", pa.string()), pa.field("normalized_cusip9", pa.string()),
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
    if value is None or isinstance(value, bool):
        return None
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return None
    return candidate if math.isfinite(candidate) else None


def _text(value: object) -> str | None:
    if value is None:
        return None
    return str(value.value) if isinstance(value, Enum) else str(value)


def _known_currencies(values: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in values.items() if key != "UNKNOWN"}


def _match_rows(matches: Iterable[tuple[MatchResult, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for match, debt_state in matches:
        holding = match.holding
        rows.append({
            "publication_id": _text(holding.publication_id), "accession_number": _text(holding.accession_number), "holding_id": _text(holding.holding_id), "source_run_id": _text(holding.source_run_id),
            "report_date": _text(holding.report_date), "filing_date": _text(holding.filing_date), "series_id": _text(holding.series_id), "class_id": _text(holding.class_id),
            "instrument_id": _text(holding.instrument_id), "issuer_category": _text(holding.issuer_category), "asset_class": _text(holding.asset_class), "instrument_structure": _text(holding.instrument_structure), "original_cusip": _text(holding.original_cusip), "normalized_cusip9": match.normalized_cusip9,
            "signed_market_value_raw": _json(holding.signed_market_value), "signed_pct_of_nav_raw": _json(holding.signed_pct_of_nav), "currency": _text(holding.currency), "raw_values_json": _json(holding.raw_values or {}),
            "debt_state": debt_state.value, "state": match.state.value, "observation_date": match.observation_date, "observation_age_days": match.observation_age_days,
            "is_144a": match.is_144a, "observation_price": _finite(match.observations[0].price) if match.observations else None, "observations_json": _json(match.observations),
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


def _write_parquet_once(path: Path, schema: pa.Schema, rows: list[dict[str, object]]) -> Path:
    partial = partial_path(path)
    try:
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), partial)
        return commit_partial(partial, path)
    except Exception:
        partial.unlink(missing_ok=True)
        raise


def _copy_panel_once(source: Path, destination: Path) -> Path:
    partial = partial_path(destination)
    try:
        with source.open("rb") as input_file, partial.open("xb") as output_file:
            for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                output_file.write(chunk)
        return commit_partial(partial, destination)
    except Exception:
        partial.unlink(missing_ok=True)
        raise


def _create_reporting_attempt(run_dir: Path) -> Path:
    if _path_lexists(run_dir):
        raise PilotError("already_exists", {"path": str(run_dir)})
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(3):
        attempt = run_dir.parent / f".{run_dir.name}.reporting-{uuid4().hex}.partial-dir"
        try:
            attempt.mkdir()
        except FileExistsError:
            continue
        return attempt
    raise PilotError("attempt_directory_collision", {"path": str(run_dir)})


def _publish_reporting_directory(attempt: Path, run_dir: Path) -> None:
    """Adapter around the established no-replace directory publisher."""
    _publish_directory_no_replace(attempt, run_dir)


def _valid_sha(value: object) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def _validate_nport_manifest(manifest: Mapping[str, object], match_count: int, metric_count: int) -> FixtureLoadResult:
    required = {"schema_version", "extraction_mode", "phase4_state", "representative_post_backfill", "db_reads", "db_writes", "fixture_path", "fixture_sha256", "row_count", "distinct_lot_count", "distinct_series_count", "distinct_accession_count", "lineage_fields_present"}
    if not isinstance(manifest, Mapping) or set(manifest) != required:
        raise PilotError("invalid_nport_manifest")
    valid = manifest.get("schema_version") == "nport-extract-manifest-v1" and manifest.get("extraction_mode") == "fixture" and manifest.get("phase4_state") == "pre_backfill" and manifest.get("representative_post_backfill") is False and manifest.get("db_reads") == 0 and manifest.get("db_writes") == 0 and isinstance(manifest.get("fixture_path"), str) and bool(manifest["fixture_path"]) and _valid_sha(manifest.get("fixture_sha256")) and manifest.get("lineage_fields_present") is True
    counts = [manifest.get(key) for key in ("row_count", "distinct_lot_count", "distinct_series_count", "distinct_accession_count")]
    if not valid or any(not isinstance(count, int) or isinstance(count, bool) or count < 0 or count > 10_000 for count in counts):
        raise PilotError("invalid_nport_manifest")
    row_count = manifest["row_count"]
    if row_count == 0:
        if any(count != 0 for count in counts[1:]) or match_count != 0 or metric_count != 0:
            raise PilotError("invalid_nport_manifest")
    elif any(count <= 0 or count > row_count for count in counts[1:]) or match_count > row_count or metric_count > manifest["distinct_series_count"]:
        raise PilotError("invalid_nport_manifest")
    try:
        loaded = load_fixture_result(manifest["fixture_path"])
    except PilotError as exc:
        raise PilotError("invalid_nport_manifest") from exc
    if dict(manifest) != loaded.manifest():
        raise PilotError("invalid_nport_manifest")
    return loaded


def _fixture_lot_key(holding: object) -> tuple[object, ...]:
    return tuple(getattr(holding, field) for field in ("publication_id", "accession_number", "holding_id", "source_run_id", "series_id", "instrument_id"))


def _validate_fixture_coverage(matches: tuple[MatchResult, ...], fixture: FixtureLoadResult) -> None:
    fixture_by_lot = {_fixture_lot_key(holding): holding for holding in fixture.holdings}
    if len(fixture_by_lot) != fixture.row_count or len(matches) != fixture.row_count:
        raise PilotError("report_fixture_mismatch")
    match_by_lot: dict[tuple[object, ...], object] = {}
    for match in matches:
        lot = _fixture_lot_key(match.holding)
        if lot in match_by_lot:
            raise PilotError("report_fixture_mismatch")
        match_by_lot[lot] = match.holding
    if set(match_by_lot) != set(fixture_by_lot) or any(match_by_lot[key] != fixture_by_lot[key] for key in fixture_by_lot):
        raise PilotError("report_fixture_mismatch")


def _validate_mapping_provenance(value: Mapping[str, object], mapping: DebtMapping) -> dict[str, object]:
    if not isinstance(value, Mapping) or value.get("schema_version") != "mapping-provenance-v2" or value.get("mapping_contract") != "composite-exact-v2" or value.get("mapping_version") != mapping.mapping_version or value.get("scope") != mapping.scope or value.get("mapping_sha256") != mapping.mapping_sha256:
        raise PilotError("invalid_mapping_provenance")
    if mapping.scope == "synthetic_fixture_only":
        expected = {"schema_version", "mapping_contract", "mapping_version", "scope", "mapping_sha256", "approval_state", "approval_reference"}
        if set(value) != expected or value.get("approval_state") != "synthetic_fixture_only" or value.get("approval_reference") != "synthetic_fixture_only":
            raise PilotError("invalid_mapping_provenance")
    elif mapping.scope == "approved_external":
        expected = {"schema_version", "mapping_contract", "mapping_version", "scope", "mapping_sha256", "observed_composite_values_sha256", "approval_state", "approval_reference"}
        if set(value) != expected or value.get("observed_composite_values_sha256") != mapping.observed_composite_values_sha256 or value.get("approval_state") != "approved" or value.get("approval_reference") != mapping.approval_sha256:
            raise PilotError("invalid_mapping_provenance")
    else:
        raise PilotError("invalid_mapping_provenance")
    return dict(_plain(value))


def _mapping_evidence(mapping: DebtMapping, provenance: Mapping[str, object]) -> dict[str, object]:
    """Expose only contract pins; decision-table values never enter diagnostics."""
    return {"schema_version": mapping.schema_version, "mapping_version": mapping.mapping_version, "scope": mapping.scope, "mapping_sha256": mapping.mapping_sha256, "classification_fields": ["issuer_category", "asset_class", "instrument_structure"], "mapping_contract": provenance["mapping_contract"], "approval_reference": provenance["approval_reference"]}


def _panel_is_inside_run(panel_path: Path, run_dir: Path) -> bool:
    panel = panel_path.resolve()
    root = run_dir.resolve()
    return panel == root or root in panel.parents


def write_internal_reports(*, run_dir: str | Path, source_candidate: SourceCandidate, source_approval: SourceApproval, debt_mapping: DebtMapping, mapping_provenance: Mapping[str, object], nport_manifest: Mapping[str, object], panel_result: PanelBuildResult, panel_path: str | Path, matches: Iterable[MatchResult], series_metrics: Iterable[SeriesMetric], cross_series_summary: CrossSeriesSummary, latest_observations: Iterable[Observation], calibration_report: Mapping[str, object], checkpoint: Mapping[str, object] | None = None) -> Mapping[str, Path]:
    """Build a complete, unredacted internal evidence pack and publish it once."""
    source_approval.validate_for(source_candidate)
    root = Path(run_dir)
    panel = Path(panel_path)
    if _panel_is_inside_run(panel, root):
        raise PilotError("panel_path_inside_run_dir", {"path": str(panel)})
    if _path_lexists(root):
        raise PilotError("already_exists", {"path": str(root)})
    if not panel.is_file():
        raise PilotError("missing_panel", {"path": str(panel)})
    match_values, metric_values, latest_values = tuple(matches), tuple(series_metrics), tuple(latest_observations)
    validated_matches = validate_match_categories(match_values, debt_mapping)
    fixture = _validate_nport_manifest(nport_manifest, len(match_values), len(metric_values))
    _validate_fixture_coverage(match_values, fixture)
    manifest = fixture.manifest()
    provenance = _validate_mapping_provenance(mapping_provenance, debt_mapping)
    recomputed_metrics = compute_series_metrics(match_values, debt_mapping)
    if metric_values != recomputed_metrics:
        raise PilotError("report_metrics_mismatch")
    if cross_series_summary != compute_cross_series_summary(recomputed_metrics):
        raise PilotError("report_summary_mismatch")
    match_rows, metric_rows, latest_rows = _match_rows(validated_matches), _metric_rows(metric_values), _latest_rows(latest_values)
    source_manifest = {**source_candidate.to_json_mapping(), "approval": source_approval.to_json_mapping(), "internal_only": True}
    calibration = {**dict(calibration_report), "status": "not_started", "phase4_state": "pre_backfill", "representative_post_backfill": False, "db_reads": 0, "db_writes": 0}
    dispositions = {"eligible": 0, "missing_category": 0, "ambiguous_category": 0, "non_debt_excluded": 0}
    for _match, debt_state in validated_matches:
        if debt_state.value == "ineligible_non_debt":
            dispositions["non_debt_excluded"] += 1
        elif debt_state.value in {"missing_category", "ambiguous_category"}:
            dispositions[debt_state.value] += 1
        else:
            dispositions["eligible"] += 1
    quality = {"internal_only": True, "source": source_manifest, "mapping": _mapping_evidence(debt_mapping, provenance), "nport": manifest, "panel": _plain(panel_result), "state_counts": {key: sum(1 for row in match_rows if row["state"] == key) for key in sorted({str(row["state"]) for row in match_rows})}, "disposition_counts": dispositions, "invalid_weight_diagnostics": {"by_series": [_plain(metric.denominator_diagnostics) for metric in metric_values]}, "market_diagnostics": {"by_series": [_plain(metric.market_value_diagnostics) for metric in metric_values], "currency_values_no_fx": [_plain(_known_currencies(metric.eligible_market_value_by_currency)) for metric in metric_values]}, "cross_series": _plain(cross_series_summary), "latest_lane": {"historical_input": False}, "db_reads": 0, "db_writes": 0, "representative": False}
    report = "# Bond pilot internal report\n\nInternal-only local/offline/no-write fixture run. Phase state: pre-backfill; representative: false. Full internal provenance is expected. Latest lane is isolated (`historical_input:false`). No frontend, API, or production claim.\n"
    attempt = _create_reporting_attempt(root)
    try:
        paths: dict[str, Path] = {}
        paths["source_manifest"] = write_json_once(attempt / "source-manifest.json", source_manifest)
        paths["nport_extract_manifest"] = write_json_once(attempt / "nport-extract-manifest.json", manifest)
        paths["calibration_report"] = write_json_once(attempt / "calibration-report.json", calibration)
        paths["bond_observed_daily"] = _copy_panel_once(panel, attempt / "bond-observed-daily.parquet")
        paths["fund_asof_match"] = _write_parquet_once(attempt / "fund-asof-match.parquet", _MATCH_SCHEMA, match_rows)
        paths["fund_series_metrics"] = _write_parquet_once(attempt / "fund-series-metrics.parquet", _METRIC_SCHEMA, metric_rows)
        paths["bond_latest"] = _write_parquet_once(attempt / "bond-latest.parquet", _LATEST_SCHEMA, latest_rows)
        paths["quality_summary"] = write_json_once(attempt / "quality-summary.json", quality)
        paths["pilot_report"] = write_text_once(attempt / "pilot-report.md", report)
        if checkpoint is not None:
            paths["checkpoint"] = replace_checkpoint(attempt / "checkpoint.json", canonical_json_bytes(_plain(checkpoint)))
        paths["checksums"] = write_checksums(attempt)
        _publish_reporting_directory(attempt, root)
    except Exception:
        shutil.rmtree(attempt, ignore_errors=True)
        raise
    return {key: root / path.name for key, path in paths.items()}
