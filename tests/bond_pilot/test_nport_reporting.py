from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.bond_pilot.contracts import MatchState, PilotError, SourceApproval, SourceCandidate
from src.bond_pilot.debt_mapping import load_fixture_debt_mapping
from src.bond_pilot.matching import CrossSeriesSummary, MatchResult, Observation, SeriesMetric
from src.bond_pilot.nport import fixture_manifest, load_fixture_holdings
from src.bond_pilot.panel import PanelBuildResult
from src.bond_pilot.reporting import write_internal_reports


def _row(*, holding_id: str = "lot-1", instrument_id: str = "instrument-1", weight: object = "not-a-number") -> dict[str, object]:
    return {
        "publication_id": "publication-1", "accession_number": "0000000000-24-000001", "holding_id": holding_id,
        "source_run_id": "source-run-1", "report_date": "2024-03-31", "filing_date": "2024-04-15",
        "series_id": "series-1", "class_id": None, "instrument_id": instrument_id, "issuer_category": "fixture_debt",
        "cusip": "123456789", "signed_market_value": "100.25", "signed_pct_of_nav": weight, "currency": "USD",
    }


def _fixture(path: Path, rows: list[object]) -> Path:
    path.write_text(json.dumps({"schema_version": "nport-fixture-v1", "phase4_state": "pre_backfill", "holdings": rows}), encoding="utf-8")
    return path


def _candidate() -> SourceCandidate:
    return SourceCandidate("source-candidate-v1", "file:///internal/source.zip", "source.zip", "source.parquet", 10, "a" * 64, "inner/source.parquet", 9, "b" * 64, ("cusip_id", "trd_exctn_dt", "pr"), (), 1, 1, "2024-01-01", "2024-12-31", "matching_cohort")


def _approval() -> SourceApproval:
    return SourceApproval("source-approval-v1", "file:///internal/source.zip", "a" * 64, "b" * 64, "2024-12-31", "internal terms record", True, False, "reviewer", "2026-07-19T12:00:00Z")


def _mapping(tmp_path: Path):
    path = tmp_path / "mapping.json"
    path.write_text(json.dumps({"schema_version": "debt-mapping-test-v1", "mapping_version": "synthetic-test-v1", "scope": "synthetic_fixture_only", "categories": {"fixture_debt": "debt_like_eligible", "fixture_non_debt": "ineligible_non_debt", "fixture_ambiguous": "ambiguous_category"}}), encoding="utf-8")
    return load_fixture_debt_mapping(path)


def test_fixture_rejects_over_cap_before_converting_rows(tmp_path: Path) -> None:
    path = _fixture(tmp_path / "over.json", [_row(holding_id=str(index)) for index in range(10_001)])
    with pytest.raises(PilotError, match="nport_row_limit_exceeded"):
        load_fixture_holdings(path)
    with pytest.raises(PilotError, match="nport_row_limit_exceeded"):
        load_fixture_holdings(path, max_rows=20_000)


def test_fixture_accepts_10000_and_preserves_lineage_and_invalid_raw_weight(tmp_path: Path) -> None:
    path = _fixture(tmp_path / "cap.json", [_row(holding_id=str(index)) for index in range(10_000)])
    holdings = load_fixture_holdings(path)
    assert len(holdings) == 10_000
    assert holdings[0].publication_id == "publication-1"
    assert holdings[0].original_cusip == "123456789"
    assert holdings[0].signed_pct_of_nav == "not-a-number"
    assert holdings[0].raw_values["signed_pct_of_nav"] == "not-a-number"
    with pytest.raises(TypeError):
        holdings[0].raw_values["changed"] = True  # type: ignore[index]


def test_fixture_rejects_duplicate_lot_but_keeps_distinct_lots(tmp_path: Path) -> None:
    duplicate = _fixture(tmp_path / "duplicate.json", [_row(), _row()])
    with pytest.raises(PilotError, match="nport_duplicate_lot"):
        load_fixture_holdings(duplicate)
    distinct = _fixture(tmp_path / "distinct.json", [_row(holding_id="lot-1"), _row(holding_id="lot-2")])
    assert [item.holding_id for item in load_fixture_holdings(distinct)] == ["lot-1", "lot-2"]


def test_fixture_manifest_has_raw_hash_and_prebackfill_flags(tmp_path: Path) -> None:
    path = _fixture(tmp_path / "fixture.json", [_row()])
    holdings = load_fixture_holdings(path)
    manifest = fixture_manifest(path, holdings)
    assert manifest["fixture_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert manifest["phase4_state"] == "pre_backfill"
    assert manifest["representative_post_backfill"] is False
    assert manifest["db_reads"] == manifest["db_writes"] == 0
    assert manifest["lineage_fields_present"] is True


def test_reports_write_explicit_schemas_internal_provenance_and_no_enum_repr(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path / "fixture.json", [_row()])
    holding = load_fixture_holdings(fixture)[0]
    panel_path = tmp_path / "source-panel.parquet"
    pq.write_table(pa.table({"normalized_cusip9": ["123456789"]}), panel_path)
    match = MatchResult(holding, MatchState.MATCHED, "123456789", "2024-03-29", (Observation("123456789", "2024-03-29", 7, 101.5, "present", None, "T", "present", "unique"),), 2, False)
    metric = SeriesMetric("series-1", "2024-03-31", "publication-1", "source-run-1", {"matched": 1}, {"non_numeric": 1}, None, None, None, {"USD": 100.25}, {"USD": 100.25}, {})
    summary = CrossSeriesSummary("nav_match_ratio", None, None, None, 0, 1, {"zero_valid_denominator": 1})
    reports = write_internal_reports(
        run_dir=tmp_path / "run", source_candidate=_candidate(), source_approval=_approval(), debt_mapping=_mapping(tmp_path), mapping_provenance={"approval_hash": "c" * 64}, nport_manifest=fixture_manifest(fixture, (holding,)), panel_result=PanelBuildResult(1, 1, 1, 0, 0, "matching_cohort"), panel_path=panel_path, matches=(match,), series_metrics=(metric,), cross_series_summary=summary, latest_observations=match.observations, calibration_report={}, checkpoint={"attempted": True},
    )
    expected = {"source-manifest.json", "nport-extract-manifest.json", "calibration-report.json", "bond-observed-daily.parquet", "fund-asof-match.parquet", "fund-series-metrics.parquet", "bond-latest.parquet", "quality-summary.json", "pilot-report.md", "checksums.sha256", "checkpoint.json"}
    assert {path.name for path in reports.values()} == expected
    assert pq.read_table(reports["fund_asof_match"]).schema == pq.read_table(reports["fund_asof_match"]).schema
    match_row = pq.read_table(reports["fund_asof_match"]).to_pylist()[0]
    assert match_row["state"] == "matched"
    assert "MatchState" not in str(match_row)
    quality = json.loads(reports["quality_summary"].read_text(encoding="utf-8"))
    assert quality["internal_only"] is True
    assert quality["latest_lane"]["historical_input"] is False
    assert quality["source"]["source_locator"] == "file:///internal/source.zip"
    assert (tmp_path / "run" / "bond-observed-daily.parquet").read_bytes() == panel_path.read_bytes()
    assert "bond-latest.parquet" in reports["checksums"].read_text(encoding="utf-8")


def test_reports_empty_schemas_collision_and_nonfinite_json_rejection(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path / "fixture.json", [_row(weight=1.0)])
    holding = load_fixture_holdings(fixture)[0]
    panel_path = tmp_path / "panel.parquet"
    pq.write_table(pa.table({"normalized_cusip9": []}), panel_path)
    common = dict(run_dir=tmp_path / "run", source_candidate=_candidate(), source_approval=_approval(), debt_mapping=_mapping(tmp_path), mapping_provenance={}, nport_manifest=fixture_manifest(fixture, (holding,)), panel_result=PanelBuildResult(0, 0, 0, 0, 0, "scope"), panel_path=panel_path, matches=(), series_metrics=(), cross_series_summary=CrossSeriesSummary("nav_match_ratio", None, None, None, 0, 0, {}), latest_observations=(), calibration_report={})
    reports = write_internal_reports(**common)
    assert pq.read_table(reports["fund_asof_match"]).num_rows == 0
    assert pq.read_table(reports["fund_series_metrics"]).num_rows == 0
    with pytest.raises(PilotError, match="already_exists"):
        write_internal_reports(**common)
    bad = dict(common)
    bad["run_dir"] = tmp_path / "bad"
    bad["mapping_provenance"] = {"bad": float("nan")}
    with pytest.raises(ValueError):
        write_internal_reports(**bad)


def test_reports_do_not_emit_unknown_currency_aggregate(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path / "fixture.json", [_row(weight=1.0)])
    holding = load_fixture_holdings(fixture)[0]
    panel_path = tmp_path / "panel.parquet"
    pq.write_table(pa.table({"normalized_cusip9": []}), panel_path)
    metric = SeriesMetric("series-1", "2024-03-31", "publication-1", "source-run-1", {}, {}, 1.0, 1.0, 1.0, {"UNKNOWN": 99.0, "USD": 1.0}, {"UNKNOWN": 99.0, "USD": 1.0}, {})
    reports = write_internal_reports(run_dir=tmp_path / "run", source_candidate=_candidate(), source_approval=_approval(), debt_mapping=_mapping(tmp_path), mapping_provenance={}, nport_manifest=fixture_manifest(fixture, (holding,)), panel_result=PanelBuildResult(0, 0, 0, 0, 0, "scope"), panel_path=panel_path, matches=(), series_metrics=(metric,), cross_series_summary=CrossSeriesSummary("nav_match_ratio", 1.0, 1.0, 1.0, 1, 0, {}), latest_observations=(), calibration_report={})
    row = pq.read_table(reports["fund_series_metrics"]).to_pylist()[0]
    assert json.loads(row["eligible_market_value_by_currency_json"]) == {"USD": 1.0}
    quality = json.loads(reports["quality_summary"].read_text(encoding="utf-8"))
    assert quality["market_diagnostics"]["currency_values_no_fx"] == [{"USD": 1.0}]
