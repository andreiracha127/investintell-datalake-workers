from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.bond_pilot.contracts import DebtState, MatchState, PilotError
from src.bond_pilot import matching
from src.bond_pilot.debt_mapping import DebtMapping, load_approved_debt_mapping, load_fixture_debt_mapping
from src.bond_pilot.matching import (
    HoldingRecord,
    ObservationIndex,
    classify_weight,
    compute_cross_series_summary,
    compute_series_metrics,
    match_holding,
)


def _holding(**changes: object) -> HoldingRecord:
    values: dict[str, object] = {
        "publication_id": "publication-1",
        "accession_number": "accession-1",
        "holding_id": "holding-1",
        "source_run_id": "run-1",
        "report_date": "2024-01-15",
        "filing_date": "2024-01-20",
        "series_id": "series-a",
        "class_id": "class-a",
        "instrument_id": "instrument-1",
        "issuer_category": "fixture_debt",
        "original_cusip": "123456789",
        "signed_market_value": 100.0,
        "signed_pct_of_nav": 10.0,
        "currency": "USD",
    }
    values.update(changes)
    return HoldingRecord(**values)


def _write_panel(path: Path, rows: list[dict[str, object]]) -> None:
    columns = {key: [row.get(key) for row in rows] for key in rows[0]}
    pq.write_table(pa.table(columns), path)


@pytest.fixture
def mapping() -> object:
    return load_fixture_debt_mapping(Path(__file__).parent / "fixtures" / "debt-mapping-test-v1.json")


def test_fixture_mapping_is_strictly_synthetic_and_classifies_exact_values(mapping: object) -> None:
    assert mapping.classify("fixture_debt") is DebtState.DEBT_LIKE_ELIGIBLE
    assert mapping.classify(" fixture_debt ") is DebtState.AMBIGUOUS_CATEGORY
    assert mapping.classify(None) is DebtState.MISSING_CATEGORY
    assert mapping.classify(" ") is DebtState.MISSING_CATEGORY
    assert mapping.classify("unseen") is DebtState.AMBIGUOUS_CATEGORY


def test_debt_mapping_cannot_bypass_loader_provenance() -> None:
    with pytest.raises(PilotError, match="debt_mapping_unapproved"):
        DebtMapping("debt-mapping-test-v1", "synthetic-test-v1", "synthetic_fixture_only", {"fixture_debt": "debt_like_eligible"})


def test_mapping_loader_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    duplicate_top_level = tmp_path / "duplicate-top.json"
    duplicate_top_level.write_text(
        '{"schema_version":"debt-mapping-test-v1","schema_version":"debt-mapping-test-v1","mapping_version":"synthetic-test-v1","scope":"synthetic_fixture_only","categories":{"fixture_debt":"debt_like_eligible","fixture_non_debt":"ineligible_non_debt","fixture_ambiguous":"ambiguous_category"}}',
        encoding="utf-8",
    )
    duplicate_category = tmp_path / "duplicate-category.json"
    duplicate_category.write_text(
        '{"schema_version":"debt-mapping-test-v1","mapping_version":"synthetic-test-v1","scope":"synthetic_fixture_only","categories":{"fixture_debt":"debt_like_eligible","fixture_debt":"debt_like_eligible","fixture_non_debt":"ineligible_non_debt","fixture_ambiguous":"ambiguous_category"}}',
        encoding="utf-8",
    )
    for path in (duplicate_top_level, duplicate_category):
        with pytest.raises(PilotError, match="debt_mapping_unapproved"):
            load_fixture_debt_mapping(path)


def test_real_mapping_requires_external_approved_hash_bound_files(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.json"
    mapping_payload = {
        "schema_version": "debt-mapping-v1",
        "mapping_version": "calibration-1",
        "observed_values_sha256": "a" * 64,
        "categories": {"observed-value": "debt_like_eligible"},
    }
    mapping_path.write_text(json.dumps(mapping_payload), encoding="utf-8")
    with pytest.raises(PilotError, match="debt_mapping_unapproved"):
        load_approved_debt_mapping(mapping_path, tmp_path / "missing.json")

    approval = {
        "schema_version": "debt-mapping-approval-v1",
        "mapping_sha256": hashlib.sha256(mapping_path.read_bytes()).hexdigest(),
        "observed_values_sha256": "a" * 64,
        "evidence": [{"reference": "ticket-1", "sha256": "b" * 64}],
        "approved_by": "reviewer",
        "approved_at": "2024-01-01T12:00:00Z",
    }
    approval_path = tmp_path / "approval.json"
    approval_path.write_text(json.dumps(approval), encoding="utf-8")
    loaded = load_approved_debt_mapping(mapping_path, approval_path)
    assert loaded.classify("observed-value") is DebtState.DEBT_LIKE_ELIGIBLE
    assert loaded.approval_sha256 == hashlib.sha256(approval_path.read_bytes()).hexdigest()
    assert loaded.approval_manifest["approved_by"] == "reviewer"
    assert loaded.approval_canonical_json == (json.dumps(approval, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    assert loaded.to_mapping()["approval"]["approved_at"] == "2024-01-01T12:00:00Z"

    approval["mapping_sha256"] = "0" * 64
    approval_path.write_text(json.dumps(approval), encoding="utf-8")
    assert loaded.to_mapping()["approval"]["mapping_sha256"] != "0" * 64
    with pytest.raises(PilotError, match="debt_mapping_unapproved"):
        load_approved_debt_mapping(mapping_path, approval_path)


def test_observation_index_asof_preserves_duplicates_and_latest_is_independent(tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    index = tmp_path / "index.sqlite"
    _write_panel(
        panel,
        [
            {"normalized_cusip9": "123456789", "observation_date": "2024-01-10", "source_row_number": 7, "pr": 99.0, "pr_state": "present", "ytm": 1.0, "db_type": 3, "daily_key_state": "duplicate_in_matching_cohort"},
            {"normalized_cusip9": "123456789", "observation_date": "2024-01-10", "source_row_number": 8, "pr": 100.0, "pr_state": "present", "ytm": 2.0, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
            {"normalized_cusip9": "123456789", "observation_date": "2024-01-20", "source_row_number": 9, "pr": 101.0, "pr_state": "present", "ytm": 3.0, "db_type": 1, "daily_key_state": "unique_in_matching_cohort"},
        ],
    )
    with ObservationIndex.build(panel, index, ["123456789"]) as observations:
        assert [row.source_row_number for row in observations.lookup_asof("123456789", "2024-01-15")] == [7, 8]
        assert [row.source_row_number for row in observations.latest_rows()] == [9]
        assert observations.is_universe_member("123456789")
    with pytest.raises(PilotError, match="already_exists"):
        ObservationIndex.build(panel, index, ["123456789"])


def test_observation_index_preserves_competing_final_during_publish_race(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    panel = tmp_path / "panel.parquet"
    index = tmp_path / "index.sqlite"
    _write_panel(panel, [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 3, "daily_key_state": "unique_in_matching_cohort"}])
    original_commit = matching.commit_partial

    def competing_commit(partial: Path, final: Path) -> Path:
        final.write_text("competing-index", encoding="utf-8")
        return original_commit(partial, final)

    monkeypatch.setattr(matching, "commit_partial", competing_commit)
    with pytest.raises(PilotError, match="already_exists"):
        ObservationIndex.build(panel, index, ["123456789"])
    assert index.read_text(encoding="utf-8") == "competing-index"
    assert not list(tmp_path.glob(".index.sqlite.*.partial*"))


def test_observation_index_derives_universe_from_valid_panel_rows(tmp_path: Path, mapping: object) -> None:
    panel = tmp_path / "panel.parquet"
    index = tmp_path / "index.sqlite"
    _write_panel(
        panel,
        [
            {"normalized_cusip9": "555555555", "observation_date": "2024-01-20", "source_row_number": 1, "pr": None, "pr_state": "null", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"},
            {"normalized_cusip9": "666666666", "observation_date": "invalid-date", "observation_date_state": "invalid", "source_row_number": 2, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "invalid_key"},
        ],
    )
    with ObservationIndex.build(panel, index, ["987654321", "555555555", "666666666"]) as observations:
        assert not observations.is_universe_member("987654321")
        assert observations.is_universe_member("555555555")
        assert not observations.is_universe_member("666666666")
        absent = match_holding(_holding(original_cusip="987654321"), mapping, observations, "2024-01-01", "2024-01-31")
        future_only = match_holding(_holding(original_cusip="555555555"), mapping, observations, "2024-01-01", "2024-01-31")
    assert absent.state is MatchState.UNMATCHED_NO_CUSIP
    assert future_only.state is MatchState.UNMATCHED_NO_PRIOR_OBSERVATION


def test_historical_matcher_rejects_latest_lane_output(tmp_path: Path, mapping: object) -> None:
    panel = tmp_path / "panel.parquet"
    index = tmp_path / "index.sqlite"
    _write_panel(panel, [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"}])
    with ObservationIndex.build(panel, index, ["123456789"]) as observations:
        with pytest.raises(PilotError, match="invalid_observation_index"):
            match_holding(_holding(), mapping, observations.latest_rows(), "2024-01-01", "2024-01-31")  # type: ignore[arg-type]


def test_historical_matcher_rejects_duck_typed_mapping(tmp_path: Path) -> None:
    class DuckMapping:
        def classify(self, value: object) -> DebtState:
            return DebtState.DEBT_LIKE_ELIGIBLE

    panel = tmp_path / "panel.parquet"
    index = tmp_path / "index.sqlite"
    _write_panel(panel, [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"}])
    with ObservationIndex.build(panel, index, ["123456789"]) as observations:
        with pytest.raises(PilotError, match="debt_mapping_unapproved"):
            match_holding(_holding(), DuckMapping(), observations, "2024-01-01", "2024-01-31")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("holding_changes", "expected"),
    [
        ({"issuer_category": "fixture_non_debt"}, MatchState.INELIGIBLE_NON_DEBT),
        ({"issuer_category": "fixture_ambiguous"}, MatchState.AMBIGUOUS_CATEGORY),
        ({"issuer_category": None}, MatchState.MISSING_CATEGORY),
        ({"original_cusip": ""}, MatchState.INVALID_IDENTIFIER),
        ({"report_date": "2023-12-31"}, MatchState.OUTSIDE_WINDOW_BEFORE_SOURCE),
        ({"report_date": "2024-02-01"}, MatchState.OUTSIDE_WINDOW_AFTER_CUTOFF),
        ({"original_cusip": "987654321"}, MatchState.UNMATCHED_NO_CUSIP),
    ],
)
def test_match_state_precedence_before_observation(
    tmp_path: Path, mapping: object, holding_changes: dict[str, object], expected: MatchState
) -> None:
    panel = tmp_path / "panel.parquet"
    index = tmp_path / "index.sqlite"
    _write_panel(panel, [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 3, "daily_key_state": "unique_in_matching_cohort"}])
    with ObservationIndex.build(panel, index, ["123456789"]) as observations:
        result = match_holding(_holding(**holding_changes), mapping, observations, "2024-01-01", "2024-01-31")
    assert result.state is expected


def test_required_precedence_collisions_and_asof_states(tmp_path: Path, mapping: object) -> None:
    panel = tmp_path / "panel.parquet"
    index = tmp_path / "index.sqlite"
    _write_panel(
        panel,
        [
            {"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 3, "daily_key_state": "unique_in_matching_cohort"},
            {"normalized_cusip9": "222222222", "observation_date": "2024-01-01", "source_row_number": 2, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
            {"normalized_cusip9": "222222222", "observation_date": "2024-01-01", "source_row_number": 3, "pr": 101.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
            {"normalized_cusip9": "333333333", "observation_date": "2023-12-16", "source_row_number": 4, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
            {"normalized_cusip9": "333333333", "observation_date": "2023-12-16", "source_row_number": 5, "pr": 101.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
            {"normalized_cusip9": "444444444", "observation_date": "2023-12-16", "source_row_number": 6, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "unique_in_matching_cohort"},
            {"normalized_cusip9": "555555555", "observation_date": "2024-01-20", "source_row_number": 7, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "unique_in_matching_cohort"},
        ],
    )
    with ObservationIndex.build(panel, index, ["123456789", "222222222", "333333333", "444444444", "555555555"]) as observations:
        def state(**changes: object) -> MatchState:
            return match_holding(_holding(**changes), mapping, observations, "2024-01-01", "2024-01-31").state

        assert state(original_cusip="", report_date="2023-12-31") is MatchState.INVALID_IDENTIFIER
        assert state(original_cusip="987654321", report_date="2023-12-31") is MatchState.OUTSIDE_WINDOW_BEFORE_SOURCE
        assert state(original_cusip="987654321") is MatchState.UNMATCHED_NO_CUSIP
        assert state(original_cusip="555555555", report_date="2024-01-15") is MatchState.UNMATCHED_NO_PRIOR_OBSERVATION
        assert state(original_cusip="222222222", report_date="2024-01-11") is MatchState.UNAVAILABLE_AMBIGUOUS
        assert state(original_cusip="333333333", report_date="2024-01-16") is MatchState.STALE
        matched = match_holding(_holding(original_cusip="123456789"), mapping, observations, "2024-01-01", "2024-01-31")
        assert matched.state is MatchState.MATCHED
        assert matched.is_144a is None
        assert state(original_cusip="444444444", report_date="2024-01-15") is MatchState.MATCHED


def test_metrics_keep_series_and_currencies_separate(mapping: object, tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    index = tmp_path / "index.sqlite"
    _write_panel(panel, [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"}])
    with ObservationIndex.build(panel, index, ["123456789"]) as observations:
        matches = [
            match_holding(_holding(holding_id="a", signed_pct_of_nav=10.0, signed_market_value=10.0), mapping, observations, "2024-01-01", "2024-01-31"),
            match_holding(_holding(holding_id="b", original_cusip="987654321", signed_pct_of_nav=20.0, signed_market_value=20.0, currency="EUR"), mapping, observations, "2024-01-01", "2024-01-31"),
            match_holding(_holding(holding_id="c", signed_pct_of_nav=-5.0, signed_market_value=float("nan"), currency=None), mapping, observations, "2024-01-01", "2024-01-31"),
            match_holding(_holding(holding_id="d", signed_pct_of_nav=None, signed_market_value=7.0, currency=None), mapping, observations, "2024-01-01", "2024-01-31"),
            match_holding(_holding(holding_id="e", series_id="series-b", signed_pct_of_nav=30.0, signed_market_value=30.0), mapping, observations, "2024-01-01", "2024-01-31"),
        ]
    metrics = compute_series_metrics(matches)
    by_series = {metric.series_id: metric for metric in metrics}
    first = by_series["series-a"]
    assert first.nav_ratio == pytest.approx(1 / 3)
    assert first.denominator_diagnostics["negative"] == 1
    assert first.denominator_diagnostics["null"] == 1
    assert first.eligible_market_value_by_currency == {"USD": 10.0, "EUR": 20.0}
    assert first.matched_market_value_by_currency == {"USD": 10.0}
    assert first.market_value_diagnostics["missing_currency"] == 2
    assert by_series["series-b"].nav_ratio == 1.0
    summary = compute_cross_series_summary(metrics)
    assert summary.to_mapping() == {
        "metric": "nav_match_ratio",
        "p25": pytest.approx(0.5),
        "median": pytest.approx(2 / 3),
        "p75": pytest.approx(5 / 6),
        "count": 2,
        "excluded_count": 0,
        "excluded_reasons": {},
    }


@pytest.mark.parametrize(
    ("value", "state"), [(None, "null"), ("x", "non_numeric"), (math.inf, "non_finite"), (-1, "negative"), (0, "valid")],
)
def test_weight_states(value: object, state: str) -> None:
    assert classify_weight(value).value == state


@pytest.mark.parametrize(
    ("value", "state", "expected"),
    [(3, "present", True), (3.0, "present", True), (2, "present", False), (1.5, "present", None), (float("nan"), "present", None), (True, "present", None), (3, "invalid", None)],
)
def test_144a_requires_present_integral_db_type(value: object, state: str, expected: bool | None) -> None:
    observation = matching.Observation("123456789", "2024-01-01", 1, 1.0, "present", None, value, state, "unique")
    assert matching._is_144a(observation) is expected


def test_overflow_is_diagnostic_and_never_serialized_as_non_finite(mapping: object, tmp_path: Path) -> None:
    panel = tmp_path / "panel.parquet"
    index = tmp_path / "index.sqlite"
    _write_panel(panel, [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"}])
    with ObservationIndex.build(panel, index, ["123456789"]) as observations:
        rows = [
            match_holding(_holding(holding_id=str(number), signed_pct_of_nav=1e308, signed_market_value=1e308), mapping, observations, "2024-01-01", "2024-01-31")
            for number in range(2)
        ]
    metric = compute_series_metrics(rows)[0]
    assert metric.nav_ratio is None
    assert metric.denominator_diagnostics["aggregate_non_finite"] >= 1
    assert metric.eligible_market_value_by_currency == {}
    assert metric.matched_market_value_by_currency == {}
    assert compute_cross_series_summary((metric,)).excluded_reasons == {"aggregate_non_finite": 1}
    assert "inf" not in json.dumps(metric.to_mapping()).lower()
    assert "nan" not in json.dumps(metric.to_mapping()).lower()
