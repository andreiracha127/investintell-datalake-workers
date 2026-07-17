from __future__ import annotations

import copy
import datetime as dt

import pytest

from src.workers import ipca_production_gate as gate


THRESHOLDS = {
    "expected_k": 6,
    "min_oos_r_squared": 0.05,
    "min_catalog_coverage": 0.985,
    "min_specific_variance_coverage": 0.95,
    "max_visible_null_t_stat_ratio": 0.001,
    "max_visible_null_r_squared_ratio": 0.001,
    "extreme_beta_abs": 10.0,
    "max_visible_extreme_beta_ratio": 0.001,
    "max_characteristics_age_days": 210,
    "max_fit_age_days": 45,
}


def _healthy_snapshot() -> dict:
    dates = [
        "2019-10-01",
        "2021-01-01",
        "2022-01-01",
        "2023-01-01",
        "2024-01-01",
        "2026-02-01",
    ]
    return {
        "fit_id": "fit-6",
        "fit_date": dt.date(2026, 7, 17),
        "created_date": dt.date(2026, 7, 17),
        "universe_hash": "universe",
        "k_factors": 6,
        "gamma": [[0.1] * 6 for _ in range(6)],
        "factor_returns": {"dates": dates, "values": [[0.01] * 6 for _ in range(6)]},
        "oos_r_squared": 0.069,
        "converged": True,
        "sample_start": dt.date(2019, 10, 1),
        "sample_end": dt.date(2026, 2, 1),
        "n_observations": 91_325,
        "n_instruments": 4_804,
        "feature_names": gate.EXPECTED_FEATURES,
        "degraded": False,
        "production_fit": True,
        "gamma_drift": 0.001,
        "drift_alert": False,
        "exposure_rows": 69_504,
        "exposure_instruments": 11_584,
        "exposure_factors": 6,
        "min_factor_index": 1,
        "max_factor_index": 6,
        "incomplete_exposure_rows": 0,
        "visible_null_t_stat_instruments": 1,
        "visible_null_r_squared_instruments": 1,
        "visible_extreme_beta_instruments": 5,
        "mv_rows": 69_504,
        "mv_instruments": 11_584,
        "mv_fits": 1,
        "mv_fit_id": "fit-6",
        "mv_factors": 6,
        "catalog_instruments": 7_128,
        "covered_instruments": 7_108,
        "specific_variance_rows": 4_656,
        "specific_variance_instruments": 4_656,
        "nonpositive_specific_variances": 0,
        "current_date": dt.date(2026, 7, 17),
        "characteristics_max_as_of": dt.date(2026, 1, 31),
        "characteristics_last_computed": dt.date(2026, 7, 17),
    }


def test_healthy_snapshot_passes_with_bounded_warnings() -> None:
    result = gate.evaluate_snapshot(
        _healthy_snapshot(), expected_fit_id="fit-6", thresholds=THRESHOLDS
    )
    assert result["status"] == "succeeded"
    assert result["catalog_coverage"] == pytest.approx(7_108 / 7_128)
    assert result["quality_warnings"] == [
        "visible_null_t_stat_below_gate",
        "visible_null_r_squared_below_gate",
        "visible_extreme_beta_below_gate",
        "catalog_coverage_below_100_percent",
    ]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"mv_fit_id": "stale-fit"}, "materialized view"),
        ({"covered_instruments": 7_000}, "catalog factor coverage"),
        ({"visible_extreme_beta_instruments": 20}, "extreme beta"),
        ({"visible_null_r_squared_instruments": 20}, "null R-squared"),
        ({"drift_alert": True}, "drift alert"),
        ({"characteristics_max_as_of": dt.date(2025, 1, 31)}, "stale"),
    ],
)
def test_gate_rejects_production_blockers(change: dict, message: str) -> None:
    snapshot = copy.deepcopy(_healthy_snapshot())
    snapshot.update(change)
    with pytest.raises(RuntimeError, match=message):
        gate.evaluate_snapshot(
            snapshot, expected_fit_id="fit-6", thresholds=THRESHOLDS
        )


def test_gate_rejects_unexpected_fit_id() -> None:
    with pytest.raises(RuntimeError, match="expected wanted-fit"):
        gate.evaluate_snapshot(
            _healthy_snapshot(), expected_fit_id="wanted-fit", thresholds=THRESHOLDS
        )


@pytest.mark.parametrize("field", ["oos_r_squared", "gamma_drift"])
def test_gate_rejects_non_finite_fit_metrics(field: str) -> None:
    snapshot = _healthy_snapshot()
    snapshot[field] = float("nan")
    with pytest.raises(RuntimeError):
        gate.evaluate_snapshot(snapshot, thresholds=THRESHOLDS)


def test_candidate_can_pass_before_atomic_activation() -> None:
    snapshot = _healthy_snapshot()
    snapshot["production_fit"] = False
    snapshot["mv_fit_id"] = "previous-fit"
    result = gate.evaluate_snapshot(
        snapshot,
        expected_fit_id="fit-6",
        thresholds=THRESHOLDS,
        require_mv_sync=False,
        require_production_fit=False,
    )
    assert result["fit_id"] == "fit-6"


def test_gate_requires_characteristics_from_governed_run() -> None:
    snapshot = _healthy_snapshot()
    minimum = dt.datetime(2026, 7, 17, 12, tzinfo=dt.UTC)
    snapshot["characteristics_last_computed"] = minimum - dt.timedelta(seconds=1)
    with pytest.raises(RuntimeError, match="governed run"):
        gate.evaluate_snapshot(
            snapshot,
            thresholds=THRESHOLDS,
            min_characteristics_computed_at=minimum,
        )
