"""No-look-ahead as-of matching, lane isolation, and fund-level metrics.

Ported from the pilot's matching module with the Parquet/capability I/O
replaced by an in-memory row source (SQLite in-memory index preserved).
"""

from __future__ import annotations

import json
import math
from typing import Iterable, Mapping

import pytest

from src.bonds import matching
from src.bonds.debt_mapping import DebtMapping
from src.bonds.errors import BondError
from src.bonds.matching import (
    HoldingRecord,
    MatchResult,
    Observation,
    ObservationIndex,
    classify_weight,
    compute_cross_series_summary,
    compute_series_metrics,
    match_holding,
)
from src.bonds.states import DebtState, MatchState


_FIXTURE_RULES = (
    ("fixture_debt", "fixture_asset", "fixture_structure", "eligible_debt"),
    ("fixture_non_debt", "fixture_asset", "fixture_structure", "non_debt_excluded"),
    ("synthetic_mbs", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
    ("synthetic_abs", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
    ("synthetic_clo", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
    ("synthetic_loan", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
    ("synthetic_repo", "synthetic_asset", "synthetic_structure", "non_debt_excluded"),
)


@pytest.fixture
def mapping() -> DebtMapping:
    return DebtMapping(rules=_FIXTURE_RULES)


def _index(rows: Iterable[Mapping[str, object]], cohort: Iterable[object]) -> ObservationIndex:
    return ObservationIndex.from_rows(rows, cohort)


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
        "asset_class": "fixture_asset",
        "instrument_structure": "fixture_structure",
        "original_cusip": "123456789",
        "signed_market_value": 100.0,
        "signed_pct_of_nav": 10.0,
        "currency": "USD",
    }
    values.update(changes)
    return HoldingRecord(**values)


def test_observation_index_asof_preserves_duplicates_and_latest_is_independent() -> None:
    rows = [
        {"normalized_cusip9": "123456789", "observation_date": "2024-01-10", "source_row_number": 7, "pr": 99.0, "pr_state": "present", "ytm": 1.0, "db_type": 3, "daily_key_state": "duplicate_in_matching_cohort"},
        {"normalized_cusip9": "123456789", "observation_date": "2024-01-10", "source_row_number": 8, "pr": 100.0, "pr_state": "present", "ytm": 2.0, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
        {"normalized_cusip9": "123456789", "observation_date": "2024-01-20", "source_row_number": 9, "pr": 101.0, "pr_state": "present", "ytm": 3.0, "db_type": 1, "daily_key_state": "unique_in_matching_cohort"},
    ]
    with _index(rows, ["123456789"]) as observations:
        assert [row.source_row_number for row in observations.lookup_asof("123456789", "2024-01-15")] == [7, 8]
        assert [row.source_row_number for row in observations.latest_rows()] == [9]
        assert observations.is_universe_member("123456789")


def test_observation_index_derives_universe_from_valid_rows(mapping: DebtMapping) -> None:
    rows = [
        {"normalized_cusip9": "555555555", "observation_date": "2024-01-20", "source_row_number": 1, "pr": None, "pr_state": "null", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"},
        {"normalized_cusip9": "666666666", "observation_date": "invalid-date", "observation_date_state": "invalid", "source_row_number": 2, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "invalid_key"},
    ]
    with _index(rows, ["987654321", "555555555", "666666666"]) as observations:
        assert not observations.is_universe_member("987654321")
        assert observations.is_universe_member("555555555")
        assert not observations.is_universe_member("666666666")
        absent = match_holding(_holding(original_cusip="987654321"), mapping, observations, "2024-01-01", "2024-01-31")
        future_only = match_holding(_holding(original_cusip="555555555"), mapping, observations, "2024-01-01", "2024-01-31")
    assert absent.state is MatchState.UNMATCHED_NO_CUSIP
    assert future_only.state is MatchState.UNMATCHED_NO_PRIOR_OBSERVATION


def test_historical_matcher_rejects_latest_lane_output(mapping: DebtMapping) -> None:
    """MANDATORY: the informative `latest` lane is never accepted by the PIT matcher."""
    rows = [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"}]
    with _index(rows, ["123456789"]) as observations:
        with pytest.raises(BondError, match="invalid_observation_index"):
            match_holding(_holding(), mapping, observations.latest_rows(), "2024-01-01", "2024-01-31")  # type: ignore[arg-type]


def test_historical_matcher_rejects_duck_typed_mapping() -> None:
    class DuckMapping:
        def classify(self, value: object) -> DebtState:
            return DebtState.DEBT_LIKE_ELIGIBLE

    rows = [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"}]
    with _index(rows, ["123456789"]) as observations:
        with pytest.raises(BondError, match="invalid_debt_mapping"):
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
    mapping: DebtMapping, holding_changes: dict[str, object], expected: MatchState
) -> None:
    rows = [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 3, "daily_key_state": "unique_in_matching_cohort"}]
    with _index(rows, ["123456789"]) as observations:
        result = match_holding(_holding(**holding_changes), mapping, observations, "2024-01-01", "2024-01-31")
    assert result.state is expected


def test_required_precedence_collisions_and_asof_states(mapping: DebtMapping) -> None:
    rows = [
        {"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 3, "daily_key_state": "unique_in_matching_cohort"},
        {"normalized_cusip9": "222222222", "observation_date": "2024-01-01", "source_row_number": 2, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
        {"normalized_cusip9": "222222222", "observation_date": "2024-01-01", "source_row_number": 3, "pr": 101.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
        {"normalized_cusip9": "333333333", "observation_date": "2023-12-16", "source_row_number": 4, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
        {"normalized_cusip9": "333333333", "observation_date": "2023-12-16", "source_row_number": 5, "pr": 101.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
        {"normalized_cusip9": "444444444", "observation_date": "2023-12-16", "source_row_number": 6, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "unique_in_matching_cohort"},
        {"normalized_cusip9": "555555555", "observation_date": "2024-01-20", "source_row_number": 7, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "unique_in_matching_cohort"},
    ]
    with _index(rows, ["123456789", "222222222", "333333333", "444444444", "555555555"]) as observations:
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


def test_matched_144a_flag_requires_present_integral_db_type(mapping: DebtMapping) -> None:
    rows = [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 3, "db_type_state": "present", "daily_key_state": "unique_in_matching_cohort"}]
    with _index(rows, ["123456789"]) as observations:
        matched = match_holding(_holding(), mapping, observations, "2024-01-01", "2024-01-31")
    assert matched.state is MatchState.MATCHED
    assert matched.is_144a is True


def test_ambiguous_asof_never_arbitrary_pick(mapping: DebtMapping) -> None:
    rows = [
        {"normalized_cusip9": "222222222", "observation_date": "2024-01-14", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
        {"normalized_cusip9": "222222222", "observation_date": "2024-01-14", "source_row_number": 2, "pr": 101.0, "pr_state": "present", "ytm": None, "db_type": 1, "daily_key_state": "duplicate_in_matching_cohort"},
    ]
    with _index(rows, ["222222222"]) as observations:
        result = match_holding(_holding(original_cusip="222222222"), mapping, observations, "2024-01-01", "2024-01-31")
    assert result.state is MatchState.UNAVAILABLE_AMBIGUOUS
    assert [row.source_row_number for row in result.observations] == [1, 2]


@pytest.mark.parametrize(
    ("changes", "forged_state"),
    [
        ({"issuer_category": "fixture_non_debt"}, MatchState.MATCHED),
        ({"issuer_category": None}, MatchState.MATCHED),
        ({"issuer_category": "unknown"}, MatchState.MATCHED),
        ({"asset_class": 7}, MatchState.MATCHED),
        ({}, MatchState.MISSING_CATEGORY),
    ],
)
def test_metrics_reject_forged_category_state_before_any_denominator(
    mapping: DebtMapping, changes: dict[str, object], forged_state: MatchState
) -> None:
    forged = MatchResult(_holding(**changes), forged_state, "123456789")
    with pytest.raises(BondError, match="inconsistent_category_state"):
        compute_series_metrics((forged,), mapping)


@pytest.mark.parametrize(
    ("value", "state"), [(None, "null"), ("x", "non_numeric"), (math.inf, "non_finite"), (-1, "negative"), (0, "valid")]
)
def test_weight_states(value: object, state: str) -> None:
    assert classify_weight(value).value == state


@pytest.mark.parametrize(
    ("value", "state", "expected"),
    [(3, "present", True), (3.0, "present", True), (2, "present", False), (1.5, "present", None), (float("nan"), "present", None), (True, "present", None), (3, "invalid", None)],
)
def test_144a_requires_present_integral_db_type(value: object, state: str, expected: bool | None) -> None:
    observation = Observation("123456789", "2024-01-01", 1, 1.0, "present", None, value, state, "unique")
    assert matching._is_144a(observation) is expected


def test_metrics_keep_series_and_currencies_separate(mapping: DebtMapping) -> None:
    rows = [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"}]
    with _index(rows, ["123456789"]) as observations:
        matches = [
            match_holding(_holding(holding_id="a", signed_pct_of_nav=10.0, signed_market_value=10.0), mapping, observations, "2024-01-01", "2024-01-31"),
            match_holding(_holding(holding_id="b", original_cusip="987654321", signed_pct_of_nav=20.0, signed_market_value=20.0, currency="EUR"), mapping, observations, "2024-01-01", "2024-01-31"),
            match_holding(_holding(holding_id="c", signed_pct_of_nav=-5.0, signed_market_value=float("nan"), currency=None), mapping, observations, "2024-01-01", "2024-01-31"),
            match_holding(_holding(holding_id="d", signed_pct_of_nav=None, signed_market_value=7.0, currency=None), mapping, observations, "2024-01-01", "2024-01-31"),
            match_holding(_holding(holding_id="e", series_id="series-b", signed_pct_of_nav=30.0, signed_market_value=30.0), mapping, observations, "2024-01-01", "2024-01-31"),
        ]
    metrics = compute_series_metrics(matches, mapping)
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


def test_overflow_is_diagnostic_and_never_serialized_as_non_finite(mapping: DebtMapping) -> None:
    rows = [{"normalized_cusip9": "123456789", "observation_date": "2024-01-15", "source_row_number": 1, "pr": 100.0, "pr_state": "present", "ytm": None, "db_type": None, "daily_key_state": "unique_in_matching_cohort"}]
    with _index(rows, ["123456789"]) as observations:
        matches = [
            match_holding(_holding(holding_id=str(number), signed_pct_of_nav=1e308, signed_market_value=1e308), mapping, observations, "2024-01-01", "2024-01-31")
            for number in range(2)
        ]
    metric = compute_series_metrics(matches, mapping)[0]
    assert metric.nav_ratio is None
    assert metric.denominator_diagnostics["aggregate_non_finite"] >= 1
    assert metric.eligible_market_value_by_currency == {}
    assert metric.matched_market_value_by_currency == {}
    assert compute_cross_series_summary((metric,)).excluded_reasons == {"aggregate_non_finite": 1}
    assert "inf" not in json.dumps(metric.to_mapping()).lower()
    assert "nan" not in json.dumps(metric.to_mapping()).lower()
