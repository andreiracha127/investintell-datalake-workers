"""Focused contracts for the read-only bond-panel parity worker."""
from __future__ import annotations

import contextlib
from datetime import date

import numpy as np
import pandas as pd
import pytest

from src.workers import bond_panel_parity as parity


def _cusips(n: int, *, offset: int = 0) -> list[str]:
    return [f"{offset + index:09d}" for index in range(n)]


def _snapshot(
    month: pd.Timestamp,
    *,
    n: int | None = None,
    offset: int = 0,
    ytm: float = 0.05,
    mod_dur: float = 4.0,
    eligibility_state: str = "included",
    eligibility_reason: object = "eligible",
) -> pd.DataFrame:
    size = parity.MIN_MONTH_ROWS if n is None else n
    cusips = _cusips(size, offset=offset)
    return pd.DataFrame({
        "cusip_id": cusips,
        "month": month,
        "issuer_id": [f"ISSUER-{cusip}" for cusip in cusips],
        "eligibility_state": eligibility_state,
        "eligibility_reason": eligibility_reason,
        "ytm": ytm,
        "mod_dur": mod_dur,
        "maturity_years": 4.0,
        "bond_maturity": 4.0,
        "spread_final": 0.01,
        "spread_final_bps": 100.0,
        "spread_definition": parity.SPREAD_DEFINITION,
        "source_lineage": [
            {"daily_observations": "bond_observation_daily"}
            for _ in cusips
        ],
    })


def _rv(
    month: pd.Timestamp,
    *,
    n: int | None = None,
    offset: int = 0,
    signal_shift: float = 0.0,
) -> pd.DataFrame:
    size = parity.MIN_MONTH_ROWS if n is None else n
    raw = np.arange(size, dtype=float)
    signal = (raw - raw.mean()) / raw.std(ddof=0)
    return pd.DataFrame({
        "cusip_id": _cusips(size, offset=offset),
        "month": month,
        "spread_bps": 100.0,
        "fitted_bps": 100.0 - signal,
        "residual_bps": signal,
        "rv_signal": signal + signal_shift,
        "spread_definition": parity.SPREAD_DEFINITION,
        "source_lineage": [
            {"daily_observations": "bond_observation_daily"}
            for _ in range(size)
        ],
    })


def _fit_diagnostics(
    month: pd.Timestamp,
    *,
    n: int | None = None,
    skipped: bool = False,
) -> pd.DataFrame:
    size = parity.MIN_MONTH_ROWS if n is None else n
    return pd.DataFrame({
        "month": [month],
        "n": [size],
        "r2": [0.5],
        "max_vif_continuous": [1.0],
        "skipped": [skipped],
    })


def _monthly_result(
    month: pd.Timestamp,
    state: str,
    comparable: bool,
    *,
    aborted: bool = False,
    reason: str | None = None,
) -> dict[str, object]:
    return {
        "month": month.date().isoformat(),
        "state": state,
        "comparable": comparable,
        "aborted": aborted,
        "reason": reason,
    }


def _compare_fixture(
    month: pd.Timestamp,
    *,
    frozen_snapshot: pd.DataFrame | None = None,
    frozen_rv: pd.DataFrame | None = None,
    rebuilt_snapshot: pd.DataFrame | None = None,
    rebuilt_rv: pd.DataFrame | None = None,
    reference_keys: pd.Series | None = None,
    fit_diagnostics: pd.DataFrame | None = None,
) -> dict[str, object]:
    rebuilt = _snapshot(month) if rebuilt_snapshot is None else rebuilt_snapshot
    return parity._compare_month(
        month,
        _snapshot(month) if frozen_snapshot is None else frozen_snapshot,
        _rv(month) if frozen_rv is None else frozen_rv,
        rebuilt,
        _rv(month) if rebuilt_rv is None else rebuilt_rv,
        input_max_day=parity._month_end(month),
        fit_as_of=month,
        monthly_curve=_curve(month),
        reference_keys=(
            rebuilt["cusip_id"] if reference_keys is None else reference_keys
        ),
        fit_diagnostics=(
            _fit_diagnostics(month)
            if fit_diagnostics is None
            else fit_diagnostics
        ),
    )


def test_rv_structure_accepts_finite_standardized_fit() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._rv_structure(
        _rv(month),
        _snapshot(month),
        _fit_diagnostics(month),
        month,
    )

    assert result["passed"] is True
    assert result["fit_row_count"] == parity.MIN_MONTH_ROWS
    assert abs(result["rv_mean"]) <= parity.RV_MEAN_TOLERANCE
    assert abs(result["rv_population_std"] - 1) <= parity.RV_STD_TOLERANCE


def test_rv_structure_rejects_permuted_rv_signal_even_when_moments_survive() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt_rv = _rv(month)
    rebuilt_rv["rv_signal"] = rebuilt_rv["rv_signal"].iloc[::-1].to_numpy()

    result = _compare_fixture(month, rebuilt_rv=rebuilt_rv)

    assert result["state"] == "parity_failed"
    assert result["rv_structure"]["gates"]["rv_signal_matches_residual_zscore"] is False
    assert result["rv_structure"]["max_rv_signal_error"] > 0.0


def test_rv_structure_rejects_residual_identity_corruption() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt_rv = _rv(month)
    rebuilt_rv.loc[0, "residual_bps"] += 1.0

    result = _compare_fixture(month, rebuilt_rv=rebuilt_rv)

    assert result["state"] == "parity_failed"
    assert result["rv_structure"]["gates"]["residual_matches_spread_minus_fitted"] is False
    assert result["rv_structure"]["max_residual_identity_error"] == pytest.approx(1.0)


def test_rv_structure_rejects_rekeyed_rv_tuples_that_preserve_marginal_moments() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt_snapshot = _snapshot(month)
    spread_bps = np.arange(parity.MIN_MONTH_ROWS, dtype=float) + 100.0
    rebuilt_snapshot["spread_final"] = spread_bps / 10_000.0
    rebuilt_snapshot["spread_final_bps"] = spread_bps
    rebuilt_rv = _rv(month)
    rebuilt_rv["spread_bps"] = spread_bps
    rebuilt_rv["fitted_bps"] = spread_bps - rebuilt_rv["residual_bps"]
    rebuilt_rv[["spread_bps", "fitted_bps", "residual_bps", "rv_signal"]] = (
        rebuilt_rv[["spread_bps", "fitted_bps", "residual_bps", "rv_signal"]]
        .iloc[::-1]
        .to_numpy()
    )

    result = _compare_fixture(
        month, rebuilt_snapshot=rebuilt_snapshot, rebuilt_rv=rebuilt_rv
    )

    assert result["state"] == "parity_failed"
    assert result["rv_structure"]["gates"]["spread_matches_included_snapshot"] is False
    assert result["rv_structure"]["max_snapshot_spread_error"] > 0.0


def test_rv_structure_rejects_empty_output() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._rv_structure(
        _rv(month).iloc[0:0], _snapshot(month), _fit_diagnostics(month, n=0), month
    )

    assert result["passed"] is False
    assert result["gates"]["rebuilt_rv_nonempty"] is False


def test_rv_structure_rejects_absent_required_column() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._rv_structure(
        _rv(month).drop(columns="rv_signal"),
        _snapshot(month),
        _fit_diagnostics(month),
        month,
    )

    assert result["passed"] is False
    assert result["gates"]["required_columns_present"] is False


@pytest.mark.parametrize("column", ["rv_signal", "residual_bps"])
def test_rv_structure_rejects_nonfinite_values(column: str) -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt_rv = _rv(month)
    rebuilt_rv.loc[0, column] = np.inf

    result = parity._rv_structure(
        rebuilt_rv, _snapshot(month), _fit_diagnostics(month), month
    )

    assert result["passed"] is False
    assert result["gates"]["rv_values_finite"] is False


@pytest.mark.parametrize("key", [None, "   "])
def test_rv_structure_rejects_null_or_blank_key(key: object) -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt_rv = _rv(month)
    rebuilt_rv.loc[0, "cusip_id"] = key

    result = parity._rv_structure(
        rebuilt_rv, _snapshot(month), _fit_diagnostics(month), month
    )

    assert result["passed"] is False
    assert result["gates"]["rv_keys_valid"] is False


def test_rv_structure_rejects_duplicate_key() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt_rv = _rv(month)
    rebuilt_rv.loc[1, "cusip_id"] = rebuilt_rv.loc[0, "cusip_id"]

    result = parity._rv_structure(
        rebuilt_rv, _snapshot(month), _fit_diagnostics(month), month
    )

    assert result["passed"] is False
    assert result["gates"]["rv_keys_unique"] is False


def test_rv_structure_rejects_key_outside_rebuilt_included_cohort() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._rv_structure(
        _rv(month, offset=1), _snapshot(month), _fit_diagnostics(month), month
    )

    assert result["passed"] is False
    assert result["gates"]["rv_keys_subset_of_included"] is False


@pytest.mark.parametrize("value", [pd.Timestamp("2025-02-01"), pd.NaT])
def test_rv_structure_rejects_wrong_or_null_rv_month(value: pd.Timestamp) -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt_rv = _rv(month)
    rebuilt_rv.loc[0, "month"] = value

    result = parity._rv_structure(
        rebuilt_rv, _snapshot(month), _fit_diagnostics(month), month
    )

    assert result["passed"] is False
    assert result["gates"]["rv_month_exact"] is False


@pytest.mark.parametrize("diagnostics", [
    pd.DataFrame(),
    pd.concat([
        _fit_diagnostics(pd.Timestamp("2025-01-01")),
        _fit_diagnostics(pd.Timestamp("2025-01-01")),
    ], ignore_index=True),
    _fit_diagnostics(pd.Timestamp("2025-02-01")),
])
def test_rv_structure_rejects_absent_multiple_or_wrong_month_diagnostics(
    diagnostics: pd.DataFrame,
) -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._rv_structure(_rv(month), _snapshot(month), diagnostics, month)

    assert result["passed"] is False
    assert result["gates"]["fit_diagnostics_valid"] is False


def test_rv_structure_rejects_unparseable_diagnostic_month() -> None:
    month = pd.Timestamp("2025-01-01")
    diagnostics = _fit_diagnostics(month)
    diagnostics["month"] = pd.Series(["not-a-date"], dtype="object")

    result = parity._rv_structure(_rv(month), _snapshot(month), diagnostics, month)

    assert result["passed"] is False
    assert result["gates"]["fit_diagnostics_valid"] is False


def test_rv_structure_rejects_skipped_fit() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._rv_structure(
        _rv(month), _snapshot(month), _fit_diagnostics(month, skipped=True), month
    )

    assert result["passed"] is False
    assert result["gates"]["fit_diagnostics_valid"] is False


@pytest.mark.parametrize("n", [None, 300.5])
def test_rv_structure_rejects_missing_or_nonintegral_fit_count(n: float | None) -> None:
    month = pd.Timestamp("2025-01-01")
    diagnostics = _fit_diagnostics(month)
    diagnostics["n"] = pd.Series([n], dtype="object")

    result = parity._rv_structure(_rv(month), _snapshot(month), diagnostics, month)

    assert result["passed"] is False
    assert result["gates"]["fit_diagnostics_valid"] is False


def test_rv_structure_rejects_fit_count_mismatch() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._rv_structure(
        _rv(month), _snapshot(month), _fit_diagnostics(month, n=299), month
    )

    assert result["passed"] is False
    assert result["gates"]["row_count_matches_fit"] is False


def test_rv_structure_rejects_off_center_signal() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._rv_structure(
        _rv(month, signal_shift=0.01), _snapshot(month), _fit_diagnostics(month), month
    )

    assert result["passed"] is False
    assert result["gates"]["rv_mean_centered"] is False


def test_rv_structure_rejects_nonunit_population_standard_deviation() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt_rv = _rv(month)
    rebuilt_rv["rv_signal"] *= 2

    result = parity._rv_structure(
        rebuilt_rv, _snapshot(month), _fit_diagnostics(month), month
    )

    assert result["passed"] is False
    assert result["gates"]["rv_population_std_unit"] is False


def _curve(month: pd.Timestamp, *, rate: float = 0.04) -> pd.DataFrame:
    return pd.DataFrame({"DGS3": [rate], "DGS5": [rate]}, index=[month])


def test_reference_accounting_accepts_exact_snapshot_with_typed_exclusion() -> None:
    month = pd.Timestamp("2025-01-01")
    included = _snapshot(month, n=2)
    excluded = _snapshot(
        month,
        n=1,
        offset=2,
        eligibility_state="excluded",
        eligibility_reason="illiquid",
    )
    rebuilt = pd.concat([included, excluded], ignore_index=True)

    result = parity._reference_accounting(
        pd.Series([" 000000000 ", "000000001", "000000002"]),
        rebuilt,
    )

    assert result["passed"] is True
    assert result["reference_size"] == 3
    assert result["included_size"] == 2
    assert result["excluded_size"] == 1
    assert result["exclusion_counts"] == {"illiquid": 1}


@pytest.mark.parametrize(
    ("keys", "gate"),
    [
        (pd.Series(["000000000", pd.NA]), "reference_keys_valid"),
        (pd.Series(["000000000", "   "]), "reference_keys_valid"),
        (pd.Series(["000000000", " 000000000 "]), "reference_keys_unique"),
    ],
)
def test_reference_accounting_rejects_invalid_source(
    keys: pd.Series,
    gate: str,
) -> None:
    result = parity._reference_accounting(
        keys,
        _snapshot(pd.Timestamp("2025-01-01"), n=1),
    )
    assert result["passed"] is False
    assert result["gates"][gate] is False


def test_reference_accounting_reports_missing_and_unexpected_rebuilt_keys() -> None:
    result = parity._reference_accounting(
        pd.Series(["000000000", "000000001"]),
        _snapshot(pd.Timestamp("2025-01-01"), n=1, offset=2),
    )

    assert result["gates"]["exact_reference_key_set"] is False
    assert result["missing_reference_key_count"] == 2
    assert result["unexpected_rebuilt_key_count"] == 1
    assert result["missing_reference_keys"] == ["000000000", "000000001"]
    assert result["unexpected_rebuilt_keys"] == ["000000002"]


@pytest.mark.parametrize(
    ("cusip_id", "gate"),
    [
        (["000000000", "000000000"], "rebuilt_keys_unique"),
        (["000000000", pd.NA], "rebuilt_keys_valid"),
        (["000000000", "   "], "rebuilt_keys_valid"),
    ],
)
def test_reference_accounting_rejects_duplicate_or_invalid_rebuilt_keys(
    cusip_id: list[object],
    gate: str,
) -> None:
    rebuilt = _snapshot(pd.Timestamp("2025-01-01"), n=2)
    rebuilt["cusip_id"] = cusip_id

    result = parity._reference_accounting(pd.Series(cusip_id), rebuilt)

    assert result["passed"] is False
    assert result["gates"][gate] is False


def test_reference_accounting_rejects_unrecognized_eligibility_state() -> None:
    rebuilt = _snapshot(pd.Timestamp("2025-01-01"), n=1, eligibility_state="pending")

    result = parity._reference_accounting(pd.Series(["000000000"]), rebuilt)

    assert result["passed"] is False
    assert result["gates"]["eligibility_states_recognized"] is False


def test_reference_accounting_rejects_excluded_blank_reason() -> None:
    rebuilt = _snapshot(
        pd.Timestamp("2025-01-01"),
        n=1,
        eligibility_state="excluded",
        eligibility_reason="   ",
    )

    result = parity._reference_accounting(pd.Series(["000000000"]), rebuilt)

    assert result["passed"] is False
    assert result["gates"]["excluded_reasons_typed"] is False


def test_reference_accounting_rejects_missing_issuer_id_column() -> None:
    rebuilt = _snapshot(pd.Timestamp("2025-01-01"), n=1).drop(columns="issuer_id")

    result = parity._reference_accounting(pd.Series(["000000000"]), rebuilt)

    assert result["passed"] is False
    assert result["gates"]["included_identity_present"] is False


@pytest.mark.parametrize("issuer_id", [pd.NA, "   "])
def test_reference_accounting_rejects_invalid_included_issuer_identity(issuer_id: object) -> None:
    rebuilt = _snapshot(pd.Timestamp("2025-01-01"), n=1)
    rebuilt["issuer_id"] = issuer_id

    result = parity._reference_accounting(pd.Series(["000000000"]), rebuilt)

    assert result["passed"] is False
    assert result["gates"]["included_identity_present"] is False


def test_compare_month_passes_exact_rebuild_and_records_all_gates() -> None:
    month = pd.Timestamp("2025-01-01")

    result = _compare_fixture(month)

    assert result["state"] == "parity_passed"
    assert result["aborted"] is False
    assert result["matched_bonds"] == parity.MIN_MONTH_ROWS
    assert result["reference_accounting"]["passed"] is True
    assert result["formula_parity"]["passed"] is True
    assert result["rv_structure"]["passed"] is True
    assert result["spread_definition"] == "ytm_minus_interpolated_dgs"
    assert result["walk_forward"]["fit_as_of"] == "2025-01-01"


def test_compare_month_fails_ytm_threshold_and_empty_frozen_month() -> None:
    month = pd.Timestamp("2025-01-01")
    failed = _compare_fixture(month, rebuilt_snapshot=_snapshot(month, ytm=0.051))
    empty = _compare_fixture(month, frozen_snapshot=pd.DataFrame())

    assert failed["state"] == "parity_failed"
    assert "ytm_abs_bps" in failed["failed_gates"]
    assert empty["reason"] == "gate_failed"
    assert empty["aborted"] is True


def test_compare_month_checks_spread_against_interpolated_curve_not_only_bps() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._compare_month(
        month, _snapshot(month), _rv(month), _snapshot(month), _rv(month),
        input_max_day=date(2025, 1, 31), fit_as_of=month,
        monthly_curve=_curve(month, rate=0.03),
        reference_keys=_snapshot(month)["cusip_id"],
        fit_diagnostics=_fit_diagnostics(month),
    )

    assert result["state"] == "parity_failed"
    assert "spread_numeric_semantics" in result["failed_gates"]


def test_compare_month_records_historical_rv_surface_only_as_diagnostic() -> None:
    month = pd.Timestamp("2025-01-01")
    frozen_rv = _rv(month, n=30)

    result = _compare_fixture(month, frozen_rv=frozen_rv)

    assert result["state"] == "parity_passed"
    assert result["diagnostics"]["rv_abs"]["frozen_size"] == 30
    assert result["diagnostics"]["rv_abs"]["rebuilt_size"] == parity.MIN_MONTH_ROWS


def test_compare_month_records_historical_rv_key_overlap_only_as_diagnostic() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt_ids = _cusips(parity.MIN_MONTH_ROWS)
    frozen_ids = [*rebuilt_ids[:98], "OTHER001", "OTHER002"]
    frozen_rv = _rv(month, n=100)
    frozen_rv["cusip_id"] = frozen_ids

    result = _compare_fixture(month, frozen_rv=frozen_rv)

    assert result["state"] == "parity_passed"
    assert result["diagnostics"]["rv_abs"]["matched_coverage"] == 0.98


def test_compare_month_measures_snapshot_gates_when_rebuilt_rv_is_empty() -> None:
    month = pd.Timestamp("2025-01-01")

    result = _compare_fixture(month, rebuilt_rv=pd.DataFrame())

    assert result["state"] == "parity_failed"
    assert result["diagnostics"]["rv_abs"]["rebuilt_size"] == 0
    assert result["formula_parity"]["metrics"]["ytm_abs_bps"]["median"] == 0
    assert "rebuilt_rv_nonempty" in result["failed_gates"]


def test_compare_month_reports_noncomparable_when_common_cohort_is_below_minimum() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt = _snapshot(month, n=10, offset=200)

    result = _compare_fixture(
        month,
        frozen_snapshot=_snapshot(month, n=10, offset=100),
        frozen_rv=_rv(month, n=10, offset=100),
        rebuilt_snapshot=rebuilt,
        rebuilt_rv=pd.DataFrame(),
    )

    assert result["state"] == "parity_not_comparable"
    assert result["aborted"] is False
    assert result["matched_bonds"] == 0
    assert result["formula_parity"]["evaluated"] is False


def test_compare_month_reports_noncomparable_when_every_reference_key_is_excluded() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt = _snapshot(
        month,
        n=10,
        offset=200,
        eligibility_state="excluded",
        eligibility_reason="missing_currency",
    )

    result = _compare_fixture(
        month,
        frozen_snapshot=_snapshot(month, n=10, offset=100),
        frozen_rv=_rv(month, n=10, offset=100),
        rebuilt_snapshot=rebuilt,
        rebuilt_rv=pd.DataFrame(),
    )

    assert result["state"] == "parity_not_comparable"
    assert result["aborted"] is False
    assert result["reference_accounting"]["passed"] is True
    assert result["reference_accounting"]["included_size"] == 0
    assert result["reference_accounting"]["excluded_size"] == 10
    assert result["hard_gates"]["spread_numeric_semantics"] is True
    assert result["spread_semantics"]["rebuilt"] == {
        "rows": 0,
        "max_abs_error": None,
        "max_bps_conversion_error": None,
    }


def test_compare_month_passes_with_historical_membership_drift_as_diagnostic() -> None:
    month = pd.Timestamp("2025-01-01")

    result = _compare_fixture(
        month,
        frozen_snapshot=_snapshot(month, n=350),
        frozen_rv=_rv(month, n=350),
    )

    assert result["state"] == "parity_passed"
    assert result["diagnostics"]["membership"]["frozen_included_size"] == 350
    assert result["diagnostics"]["membership"]["rebuilt_included_size"] == 300
    assert result["diagnostics"]["membership"]["symmetric_difference_size"] == 50


def test_compare_month_fails_reference_accounting_even_when_noncomparable() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt = _snapshot(month, n=10)

    result = _compare_fixture(
        month,
        frozen_snapshot=_snapshot(month, n=10, offset=100),
        rebuilt_snapshot=rebuilt,
        reference_keys=rebuilt["cusip_id"].iloc[1:],
    )

    assert result["state"] == "parity_failed"
    assert result["aborted"] is True
    assert "exact_reference_key_set" in result["failed_gates"]
    assert result["formula_parity"]["evaluated"] is False


def test_compare_month_fails_hard_gate_even_when_noncomparable() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt = _snapshot(month, n=10)
    rebuilt["spread_definition"] = "wrong"

    result = _compare_fixture(
        month,
        frozen_snapshot=_snapshot(month, n=10, offset=100),
        rebuilt_snapshot=rebuilt,
    )

    assert result["state"] == "parity_failed"
    assert result["aborted"] is True
    assert "spread_definition" in result["failed_gates"]


def test_compare_month_empty_frozen_rv_is_nonblocking_diagnostic() -> None:
    month = pd.Timestamp("2025-01-01")

    result = _compare_fixture(month, frozen_rv=pd.DataFrame())

    assert result["state"] == "parity_passed"
    assert result["diagnostics"]["rv_abs"]["unavailable_reason"] == "frozen_rv_empty"
    assert result["diagnostics"]["rv_abs"]["metrics"] == {
        "median": None, "p90": None, "p99": None,
    }


def test_compare_month_reports_frozen_rv_shift_only_as_diagnostic() -> None:
    month = pd.Timestamp("2025-01-01")

    result = _compare_fixture(month, frozen_rv=_rv(month, signal_shift=2.0))

    assert result["state"] == "parity_passed"
    assert result["diagnostics"]["rv_abs"]["metrics"] == {
        "median": 2.0, "p90": 2.0, "p99": 2.0,
    }


@pytest.mark.parametrize(
    ("gate", "frozen_snapshot", "rebuilt_snapshot"),
    [
        ("ytm_abs_bps", None, _snapshot(pd.Timestamp("2025-01-01"), ytm=0.051)),
        ("duration_abs_years", None, _snapshot(pd.Timestamp("2025-01-01"), mod_dur=5.5)),
        ("duration_relative", None, _snapshot(pd.Timestamp("2025-01-01"), mod_dur=8.0)),
        (
            "spread_abs_bps",
            _snapshot(pd.Timestamp("2025-01-01"), ytm=0.06).assign(
                spread_final=0.02, spread_final_bps=200.0
            ),
            None,
        ),
    ],
)
def test_compare_month_formula_gate_blocks_comparable_month(
    gate: str,
    frozen_snapshot: pd.DataFrame | None,
    rebuilt_snapshot: pd.DataFrame | None,
) -> None:
    month = pd.Timestamp("2025-01-01")

    result = _compare_fixture(
        month,
        frozen_snapshot=frozen_snapshot,
        rebuilt_snapshot=rebuilt_snapshot,
    )

    assert result["state"] == "parity_failed"
    assert result["formula_parity"]["evaluated"] is True
    assert gate in result["failed_gates"]


def test_compare_month_walk_forward_gate_blocks_comparable_month() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._compare_month(
        month, _snapshot(month), _rv(month), _snapshot(month), _rv(month),
        input_max_day=date(2025, 2, 1), fit_as_of=month,
        monthly_curve=_curve(month), reference_keys=_snapshot(month)["cusip_id"],
        fit_diagnostics=_fit_diagnostics(month),
    )

    assert result["state"] == "parity_failed"
    assert "walk_forward" in result["failed_gates"]


@pytest.mark.parametrize(
    ("rebuilt_rv", "fit_diagnostics", "gate"),
    [
        (pd.DataFrame(), None, "rebuilt_rv_nonempty"),
        (_rv(pd.Timestamp("2025-01-01")).drop(columns="spread_definition"), None, "rebuilt_rv_spread_definition"),
        (_rv(pd.Timestamp("2025-01-01")).assign(spread_definition="wrong"), None, "rebuilt_rv_spread_definition"),
        (_rv(pd.Timestamp("2025-01-01")).drop(columns="residual_bps"), None, "required_columns_present"),
        (_rv(pd.Timestamp("2025-01-01")).assign(rv_signal=np.nan), None, "rv_values_finite"),
        (_rv(pd.Timestamp("2025-01-01")).assign(cusip_id=""), None, "rv_keys_valid"),
        (pd.concat([_rv(pd.Timestamp("2025-01-01")).iloc[:1], _rv(pd.Timestamp("2025-01-01"))], ignore_index=True), _fit_diagnostics(pd.Timestamp("2025-01-01"), n=301), "rv_keys_unique"),
        (_rv(pd.Timestamp("2025-01-01"), offset=500), None, "rv_keys_subset_of_included"),
        (_rv(pd.Timestamp("2025-01-01")).assign(month=pd.Timestamp("2025-02-01")), None, "rv_month_exact"),
        (_rv(pd.Timestamp("2025-01-01")), _fit_diagnostics(pd.Timestamp("2025-01-01"), skipped=True), "fit_diagnostics_valid"),
        (_rv(pd.Timestamp("2025-01-01")), _fit_diagnostics(pd.Timestamp("2025-01-01"), n=299), "row_count_matches_fit"),
        (_rv(pd.Timestamp("2025-01-01")).assign(rv_signal=0.1), None, "rv_mean_centered"),
        (_rv(pd.Timestamp("2025-01-01")).assign(rv_signal=np.arange(parity.MIN_MONTH_ROWS, dtype=float) - (parity.MIN_MONTH_ROWS - 1) / 2), None, "rv_population_std_unit"),
    ],
)
def test_compare_month_rebuilt_rv_structure_failure_blocks_comparable_month(
    rebuilt_rv: pd.DataFrame,
    fit_diagnostics: pd.DataFrame | None,
    gate: str,
) -> None:
    month = pd.Timestamp("2025-01-01")

    result = _compare_fixture(
        month,
        rebuilt_rv=rebuilt_rv,
        fit_diagnostics=(
            _fit_diagnostics(month) if fit_diagnostics is None else fit_diagnostics
        ),
    )

    assert result["state"] == "parity_failed"
    assert result["aborted"] is True
    assert result["rv_structure"]["passed"] is False
    assert result["rv_structure"]["gates"][gate] is False
    assert gate in result["failed_gates"]


def test_overall_passes_one_noncomparable_and_one_comparable() -> None:
    result = parity._overall_verdict([
        _monthly_result(parity.PARITY_MONTHS[0], "parity_not_comparable", False),
        _monthly_result(parity.PARITY_MONTHS[1], "parity_passed", True),
    ])

    assert result["state"] == "parity_passed"
    assert result["aborted"] is False
    assert result["reason"] is None
    assert result["counts"] == {
        "failed_months": 0,
        "comparable_passed_months": 1,
        "noncomparable_months": 1,
    }
    assert all(result["gates"].values())
    assert result["failure_reasons"] == {}


def test_overall_passes_when_an_exact_fully_excluded_month_is_noncomparable() -> None:
    month = parity.PARITY_MONTHS[0]
    rebuilt = _snapshot(
        month,
        n=10,
        offset=200,
        eligibility_state="excluded",
        eligibility_reason="missing_currency",
    )
    noncomparable = _compare_fixture(
        month,
        frozen_snapshot=_snapshot(month, n=10, offset=100),
        frozen_rv=_rv(month, n=10, offset=100),
        rebuilt_snapshot=rebuilt,
        rebuilt_rv=pd.DataFrame(),
    )

    result = parity._overall_verdict([
        noncomparable,
        _monthly_result(parity.PARITY_MONTHS[1], "parity_passed", True),
    ])

    assert result["state"] == "parity_passed"
    assert result["aborted"] is False
    assert result["counts"] == {
        "failed_months": 0,
        "comparable_passed_months": 1,
        "noncomparable_months": 1,
    }


def test_overall_fails_without_comparable_month() -> None:
    result = parity._overall_verdict([
        _monthly_result(parity.PARITY_MONTHS[0], "parity_not_comparable", False),
        _monthly_result(parity.PARITY_MONTHS[1], "parity_not_comparable", False),
    ])

    assert result["state"] == "parity_failed"
    assert result["reason"] == "no_comparable_month"
    assert result["aborted"] is True
    assert result["counts"] == {
        "failed_months": 0,
        "comparable_passed_months": 0,
        "noncomparable_months": 2,
    }
    assert result["gates"]["at_least_one_comparable_month"] is False
    assert result["gates"]["monthly_contract_valid"] is True
    assert result["failure_reasons"] == {}


def test_overall_monthly_failure_blocks_a_comparable_pass() -> None:
    result = parity._overall_verdict([
        _monthly_result(parity.PARITY_MONTHS[0], "parity_passed", True),
        _monthly_result(
            parity.PARITY_MONTHS[1], "parity_failed", False,
            aborted=True, reason="gate_failed",
        ),
    ])

    assert result["state"] == "parity_failed"
    assert result["reason"] == "monthly_parity_failure"
    assert result["aborted"] is True
    assert result["counts"] == {
        "failed_months": 1,
        "comparable_passed_months": 1,
        "noncomparable_months": 0,
    }
    assert result["gates"]["all_months_nonblocking"] is False
    assert result["failure_reasons"] == {"gate_failed": 1}


@pytest.mark.parametrize(
    "record",
    [
        _monthly_result(parity.PARITY_MONTHS[0], "parity_passed", True, aborted=True),
        _monthly_result(parity.PARITY_MONTHS[0], "unknown_state", True),
    ],
)
def test_overall_fails_closed_for_invalid_monthly_state_contract(
    record: dict[str, object],
) -> None:
    result = parity._overall_verdict([
        record,
        _monthly_result(parity.PARITY_MONTHS[1], "parity_passed", True),
    ])

    assert result["state"] == "parity_failed"
    assert result["reason"] == "monthly_contract_failure"
    assert result["aborted"] is True
    assert result["gates"]["monthly_contract_valid"] is False
    assert result["invalid_month_results"]


@pytest.mark.parametrize(
    "records",
    [
        [_monthly_result(parity.PARITY_MONTHS[0], "parity_passed", True)],
        [
            _monthly_result(parity.PARITY_MONTHS[0], "parity_passed", True),
            _monthly_result(parity.PARITY_MONTHS[0], "parity_passed", True),
        ],
        [
            _monthly_result(parity.PARITY_MONTHS[0], "parity_passed", True),
            _monthly_result(pd.Timestamp("2025-02-01"), "parity_passed", True),
        ],
    ],
)
def test_overall_fails_closed_for_invalid_declared_month_coverage(
    records: list[dict[str, object]],
) -> None:
    result = parity._overall_verdict(records)

    assert result["state"] == "parity_failed"
    assert result["reason"] == "monthly_contract_failure"
    assert result["gates"]["declared_months_exactly_once"] is False
    assert result["month_declaration"]["missing"] or result["month_declaration"]["duplicates"] or result["month_declaration"]["unexpected"]


def test_run_aggregates_one_noncomparable_and_one_comparable_pass(monkeypatch) -> None:
    monkeypatch.setattr(parity, "config_hash", lambda: parity.PANEL_CONFIG_HASH)
    monkeypatch.setenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", "approved-snapshot")
    reference_keys = pd.Series(_cusips(parity.MIN_MONTH_ROWS))
    fit_diagnostics = _fit_diagnostics(parity.PARITY_MONTHS[0])
    comparisons = iter([
        _monthly_result(parity.PARITY_MONTHS[0], "parity_not_comparable", False),
        _monthly_result(parity.PARITY_MONTHS[1], "parity_passed", True),
    ])

    class Connection:
        def execute(self, _statement, _params=()):
            return type("Result", (), {"fetchone": lambda self: (
                parity.BASE_PUBLICATION_ID,
                parity.PANEL_CONFIG_HASH,
                parity.BASE_INPUT_FINGERPRINT,
                "validated",
            )})()

    monkeypatch.setattr(parity, "connect", lambda _dsn: contextlib.nullcontext(Connection()))
    monkeypatch.setattr(parity, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(parity, "_frozen_snapshot", lambda *_args: pd.DataFrame())
    monkeypatch.setattr(parity, "_frozen_rv", lambda *_args: pd.DataFrame())
    monkeypatch.setattr(
        parity,
        "_rebuild_month",
        lambda _conn, month, _structural_publication_id, **_kwargs: (
            pd.DataFrame(), pd.DataFrame(), None, month, pd.DataFrame(), {},
            reference_keys, fit_diagnostics,
        ),
    )

    def compare_month(*_args, reference_keys, fit_diagnostics, **_kwargs):
        pd.testing.assert_series_equal(reference_keys, pd.Series(_cusips(parity.MIN_MONTH_ROWS)))
        assert fit_diagnostics is not None
        return next(comparisons)

    monkeypatch.setattr(parity, "_compare_month", compare_month)

    outcome = parity.run("postgresql://example")

    assert outcome["state"] == "parity_passed"
    assert outcome["aborted"] is False
    assert outcome["counts"] == {
        "failed_months": 0,
        "comparable_passed_months": 1,
        "noncomparable_months": 1,
    }


def test_run_accepts_only_the_exact_authorized_repaired_root(monkeypatch) -> None:
    monkeypatch.setattr(parity, "config_hash", lambda: parity.PANEL_CONFIG_HASH)
    monkeypatch.setenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", "approved-snapshot")
    frozen_publication_ids: list[str] = []
    structural_publication_ids: list[str] = []

    class Connection:
        def execute(self, _statement, _params=()):
            return type("Result", (), {"fetchone": lambda self: (
                parity.REPAIRED_BASE_PUBLICATION_ID,
                parity.PANEL_CONFIG_HASH,
                parity.REPAIRED_BASE_INPUT_FINGERPRINT,
                "validated",
            )})()

    monkeypatch.setattr(parity, "connect", lambda _dsn: contextlib.nullcontext(Connection()))
    monkeypatch.setattr(parity, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(
        parity,
        "_frozen_snapshot",
        lambda _conn, publication_id, _month: frozen_publication_ids.append(publication_id) or pd.DataFrame(),
    )
    monkeypatch.setattr(parity, "_frozen_rv", lambda _conn, _publication_id, _month: pd.DataFrame())
    monkeypatch.setattr(
        parity,
        "_rebuild_month",
        lambda _conn, month, structural_publication_id, **_kwargs: (
            structural_publication_ids.append(structural_publication_id) or pd.DataFrame(),
            pd.DataFrame(), None, month, pd.DataFrame(), {}, pd.Series(dtype="string"), pd.DataFrame(),
        ),
    )
    monkeypatch.setattr(
        parity,
        "_compare_month",
        lambda month, *_args, **_kwargs: _monthly_result(month, "parity_passed", True),
    )

    outcome = parity.run("postgresql://example")

    assert outcome["state"] == "parity_passed"
    assert frozen_publication_ids == [parity.REPAIRED_BASE_PUBLICATION_ID] * len(parity.PARITY_MONTHS)
    assert structural_publication_ids == [parity.REPAIRED_BASE_PUBLICATION_ID] * len(parity.PARITY_MONTHS)


def test_run_requires_mapping_snapshot_before_rebuilding_authorized_root(monkeypatch) -> None:
    monkeypatch.setattr(parity, "config_hash", lambda: parity.PANEL_CONFIG_HASH)
    monkeypatch.delenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", raising=False)

    class Connection:
        def execute(self, _statement, _params=()):
            return type("Result", (), {"fetchone": lambda self: (
                parity.REPAIRED_BASE_PUBLICATION_ID,
                parity.PANEL_CONFIG_HASH,
                parity.REPAIRED_BASE_INPUT_FINGERPRINT,
                "validated",
            )})()

    monkeypatch.setattr(parity, "connect", lambda _dsn: contextlib.nullcontext(Connection()))
    monkeypatch.setattr(parity, "resolve_dsn", lambda dsn: dsn)
    monkeypatch.setattr(parity, "_rebuild_month", lambda *_args, **_kwargs: pytest.fail("must not rebuild"))

    assert parity.run("postgresql://example") == {
        "state": "parity_failed",
        "reason": "distribution_mapping_snapshot_id_absent",
        "aborted": True,
        "months": [],
    }


def test_run_retires_legacy_144a_parity_for_the_reg_s_config_without_connecting(monkeypatch) -> None:
    monkeypatch.setattr(parity, "connect", lambda _dsn: (_ for _ in ()).throw(AssertionError("no DB")))

    outcome = parity.run("postgresql://example")

    assert outcome == {
        "state": "parity_failed",
        "reason": "legacy_rule_144a_parity_not_applicable_to_reg_s",
        "aborted": True,
        "months": [],
    }


def test_run_refuses_unknown_config_mismatch_without_connecting(monkeypatch) -> None:
    monkeypatch.setattr(parity, "config_hash", lambda: "wrong")
    monkeypatch.setattr(parity, "connect", lambda _dsn: (_ for _ in ()).throw(AssertionError("no DB")))

    outcome = parity.run("postgresql://example")

    assert outcome == {
        "state": "parity_failed", "reason": "config_hash_mismatch", "aborted": True,
        "months": [],
    }


def test_rebuild_exposes_reference_and_fit_evidence(monkeypatch) -> None:
    month = pd.Timestamp("2025-01-01")
    as_of = date(2025, 1, 31)
    reference_frame = pd.DataFrame({"cusip9": _cusips(parity.MIN_MONTH_ROWS)})
    construction_frames: list[pd.DataFrame] = []
    loader_kwargs: list[dict[str, object]] = []

    def load_inputs(*_args, **kwargs):
        loader_kwargs.append(kwargs)
        return ({
            "daily_observations": pd.DataFrame({"day": [as_of]}),
            "monthly_curve": pd.DataFrame({
                "day": [as_of, as_of], "tenor": ["3y", "5y"], "yield_pct": [4.0, 4.0],
            }),
            "resolved_issuer_sector": reference_frame,
        }, {"source": "verified"})

    monkeypatch.setattr(parity.bond_panel, "_load_inputs", load_inputs)

    def build_panel(**inputs):
        construction_frames.append(inputs["resolved_issuer_sector"])
        return _snapshot(month).assign(
            coupon_pct=5.0,
            maturity_date=pd.Timestamp("2030-01-01"),
            amt_outstanding_k=100.0,
            reason_code="quoted",
            rating_bucket="A",
            rating_as_of_month=month,
            rating_state="static_current",
            rating_reason="static_rating_current",
            rating_staleness_months=0,
        )

    monkeypatch.setattr(parity, "build_db_monthly_panel", build_panel)
    monkeypatch.setattr(
        parity,
        "build_snapshots",
        lambda frame, ratings_pit=None: (frame.copy(), pd.DataFrame(columns=frame.columns)),
    )
    monkeypatch.setattr(
        parity,
        "fit_all_months",
        lambda frame, *, as_of: (_rv(as_of), _fit_diagnostics(as_of, n=len(frame))),
    )

    (
        _rebuilt_snapshot,
        rebuilt_rv,
        _max_day,
        _fit_as_of,
        _monthly_curve,
        _input_exclusions,
        reference_keys,
        fit_diagnostics,
    ) = parity._rebuild_month(object(), month, mapping_snapshot_id="approved-snapshot")

    assert len(construction_frames) == 1
    assert loader_kwargs == [{
        "mapping_snapshot_id": "approved-snapshot",
        "structural_publication_id": parity.BASE_PUBLICATION_ID,
        "structural_month": month.date(),
    }]
    assert construction_frames[0] is reference_frame
    pd.testing.assert_series_equal(reference_keys, reference_frame["cusip9"])
    assert rebuilt_rv["residual_bps"].equals(_rv(month)["residual_bps"])
    assert fit_diagnostics.loc[0, "n"] == len(rebuilt_rv)


def test_run_uses_exact_clock_and_issues_no_writes(monkeypatch) -> None:
    monkeypatch.setattr(parity, "config_hash", lambda: parity.PANEL_CONFIG_HASH)
    monkeypatch.setenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID", "approved-snapshot")
    calls: list[tuple[pd.Timestamp, pd.Timestamp, date]] = []
    structural_calls: list[dict[str, object]] = []
    fit_calls: list[pd.Timestamp] = []
    sql: list[str] = []
    month_rows = {month: (_snapshot(month), _rv(month)) for month in parity.PARITY_MONTHS}

    class Connection:
        def execute(self, statement, _params=()):
            sql.append(statement)
            return type("Result", (), {"fetchone": lambda self: (
                parity.BASE_PUBLICATION_ID,
                parity.PANEL_CONFIG_HASH,
                parity.BASE_INPUT_FINGERPRINT,
                "validated",
            )})()

    monkeypatch.setattr(parity, "connect", lambda _dsn: contextlib.nullcontext(Connection()))
    monkeypatch.setattr(parity, "resolve_dsn", lambda dsn: dsn)
    def frame(_conn, statement, params=()):
        sql.append(statement)
        return month_rows[pd.Timestamp(params[-1])][0 if "snapshot" in statement else 1].copy()

    monkeypatch.setattr(parity, "_frame", frame)
    def load_inputs(_conn, closed, opened, as_of, **kwargs):
        calls.append((closed, opened, as_of))
        structural_calls.append(kwargs)
        return ({
        "daily_observations": pd.DataFrame({"day": [as_of]}),
        "monthly_curve": pd.DataFrame({
            "day": [as_of, as_of], "tenor": ["3y", "5y"], "yield_pct": [4.0, 4.0],
        }),
        "static_rating_mapping": pd.DataFrame({
            "cusip9": ["AAA"],
            "rating_as_of_month": [pd.Timestamp("2026-07-01")],
        }),
        "resolved_issuer_sector": pd.DataFrame({"cusip9": _cusips(parity.MIN_MONTH_ROWS)}),
        }, {"x": "x"})

    monkeypatch.setattr(parity.bond_panel, "_load_inputs", load_inputs)
    monkeypatch.setattr(parity, "build_db_monthly_panel", lambda **_kwargs: _snapshot(_kwargs["months"][0]).assign(coupon_pct=5.0, maturity_date=pd.Timestamp("2030-01-01"), reason_code="quoted", rating_bucket="A", rating_as_of_month=pd.Timestamp("2025-01-01"), rating_state="static_current", rating_reason="static_rating_current", rating_staleness_months=0))
    monkeypatch.setattr(parity, "build_snapshots", lambda frame, ratings_pit=None: (frame.copy(), pd.DataFrame(columns=frame.columns)))
    monkeypatch.setattr(parity, "fit_all_months", lambda frame, *, as_of: fit_calls.append(as_of) or (
        _rv(as_of),
        _fit_diagnostics(as_of),
    ))

    outcome = parity.run("postgresql://example")

    assert outcome["state"] == "parity_passed", outcome
    assert outcome["aborted"] is False
    assert outcome["counts"] == {
        "failed_months": 0,
        "comparable_passed_months": 2,
        "noncomparable_months": 0,
    }
    assert calls == [
        (pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01"), date(2025, 1, 31)),
        (pd.Timestamp("2026-06-01"), pd.Timestamp("2026-06-01"), date(2026, 6, 30)),
    ]
    assert structural_calls == [
        {
            "mapping_snapshot_id": "approved-snapshot",
            "structural_publication_id": parity.BASE_PUBLICATION_ID,
            "structural_month": date(2025, 1, 1),
        },
        {
            "mapping_snapshot_id": "approved-snapshot",
            "structural_publication_id": parity.BASE_PUBLICATION_ID,
            "structural_month": date(2026, 6, 1),
        },
    ]
    assert fit_calls == list(parity.PARITY_MONTHS)
    assert all(
        month_result["walk_forward"]["input_exclusions"] == {"static_rating_after_month": 1}
        for month_result in outcome["months"]
    )
    assert any(statement == "SET TRANSACTION READ ONLY" for statement in sql)
    assert all("bond_panel_current_" not in statement for statement in sql)
    assert any("FROM bond_panel_snapshot WHERE publication_id" in statement for statement in sql)
    assert any("FROM bond_panel_rv_signal WHERE publication_id" in statement for statement in sql)
    assert (
        "SELECT cusip_id, month, eligibility_state, eligibility_reason, ytm, mod_dur, "
        "maturity_years, spread_final, spread_final_bps, spread_definition, source_lineage "
        "FROM bond_panel_snapshot WHERE publication_id = %s AND month = %s"
    ) in sql
    assert (
        "SELECT cusip_id, month, rv_signal, spread_definition, source_lineage "
        "FROM bond_panel_rv_signal WHERE publication_id = %s AND month = %s"
    ) in sql
    assert all(not statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "ALTER", "CREATE")) for statement in sql)


def test_run_refuses_a_different_current_publication_identity(monkeypatch) -> None:
    monkeypatch.setattr(parity, "config_hash", lambda: parity.PANEL_CONFIG_HASH)
    class Connection:
        def execute(self, statement, _params=()):
            row = (
                "different-publication",
                parity.PANEL_CONFIG_HASH,
                parity.BASE_INPUT_FINGERPRINT,
                "validated",
            )
            return type("Result", (), {"fetchone": lambda self: row})()

    monkeypatch.setattr(parity, "connect", lambda _dsn: contextlib.nullcontext(Connection()))
    monkeypatch.setattr(parity, "resolve_dsn", lambda dsn: dsn)

    outcome = parity.run("postgresql://example")

    assert outcome["state"] == "parity_failed"
    assert outcome["reason"] == "current_publication_id_mismatch"
    assert outcome["aborted"] is True


def test_run_refuses_a_different_base_fingerprint(monkeypatch) -> None:
    monkeypatch.setattr(parity, "config_hash", lambda: parity.PANEL_CONFIG_HASH)
    class Connection:
        def execute(self, statement, _params=()):
            row = (
                parity.BASE_PUBLICATION_ID,
                parity.PANEL_CONFIG_HASH,
                "0" * 64,
                "validated",
            )
            return type("Result", (), {"fetchone": lambda self: row})()

    monkeypatch.setattr(parity, "connect", lambda _dsn: contextlib.nullcontext(Connection()))
    monkeypatch.setattr(parity, "resolve_dsn", lambda dsn: dsn)

    outcome = parity.run("postgresql://example")

    assert outcome["state"] == "parity_failed"
    assert outcome["reason"] == "current_publication_fingerprint_mismatch"
