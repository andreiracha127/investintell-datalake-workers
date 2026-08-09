"""Read-only reproducibility check for the two frozen bond-panel months.

This is deliberately a one-shot Railway worker: it reads the publication currently
pointed at by ``bond_panel_v1``, rebuilds the two ratified months through Stage 6's
runtime path, and reports an all-gates parity verdict.  It never materializes facts
or moves a publication pointer.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from src.bonds.panel_config import config_hash
from src.bonds.panel_resolvers import (
    MIN_MONTH_ROWS,
    SPREAD_WINSOR_BPS,
    compute_spread,
    monthly_treasury_curve,
)
from src.db import connect, resolve_dsn
from src.workers import bond_panel

PANEL_CONFIG_HASH = "0c0d78a866bc1090"
BASE_PUBLICATION_ID = "92740098-1571-559d-9fb3-119de8321754"
BASE_INPUT_FINGERPRINT = "5a7af9e1adaed315e9940293cf3e9e789ca6350993688d58ab3e759cee37a3cb"
PARITY_MONTHS = (pd.Timestamp("2025-01-01"), pd.Timestamp("2026-06-01"))
SPREAD_DEFINITION = "ytm_minus_interpolated_dgs"
RECOGNIZED_ELIGIBILITY_STATES = frozenset({"included", "excluded"})
RV_MEAN_TOLERANCE = 1e-10
RV_STD_TOLERANCE = 1e-10
RV_STRUCTURE_TOLERANCE = 1e-10

# Aliases make the runtime seams explicit and let focused tests exercise the
# orchestration without replacing the Stage 6 module itself.
_frame = bond_panel._frame
build_db_monthly_panel = bond_panel.build_db_monthly_panel
build_snapshots = bond_panel.build_snapshots
fit_all_months = bond_panel.fit_all_months


def _failure(reason: str, months: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {"state": "parity_failed", "reason": reason, "aborted": True, "months": months or []}


def _next_month(month: pd.Timestamp) -> pd.Timestamp:
    return month + pd.offsets.MonthBegin(1)


def _month_end(month: pd.Timestamp) -> date:
    return (month + pd.offsets.MonthEnd(1)).date()


def _current_publication(conn: Any) -> tuple[str, str, str, str] | None:
    row = conn.execute(
        "SELECT p.publication_id::text, p.config_hash, p.input_fingerprint, "
        "p.publication_status "
        "FROM bond_panel_app_pointer pointer "
        "JOIN bond_panel_publications p ON p.publication_id = pointer.publication_id "
        "WHERE pointer.product = 'bond_panel_v1'"
    ).fetchone()
    return None if row is None else tuple(str(value) for value in row)


def _frozen_snapshot(conn: Any, month: pd.Timestamp) -> pd.DataFrame:
    frame = _frame(
        conn,
        "SELECT cusip_id, month, eligibility_state, eligibility_reason, ytm, mod_dur, "
        "maturity_years, spread_final, spread_final_bps, spread_definition, source_lineage "
        "FROM bond_panel_snapshot WHERE publication_id = %s AND month = %s",
        (BASE_PUBLICATION_ID, month.date()),
    )
    if "month" in frame:
        frame["month"] = pd.to_datetime(frame["month"])
    return frame


def _frozen_rv(conn: Any, month: pd.Timestamp) -> pd.DataFrame:
    frame = _frame(
        conn,
        "SELECT cusip_id, month, rv_signal, spread_definition, source_lineage "
        "FROM bond_panel_rv_signal WHERE publication_id = %s AND month = %s",
        (BASE_PUBLICATION_ID, month.date()),
    )
    if "month" in frame:
        frame["month"] = pd.to_datetime(frame["month"])
    return frame


def _input_max_day(inputs: dict[str, pd.DataFrame]) -> date | None:
    days: list[pd.Timestamp] = []
    for frame in inputs.values():
        for column in ("day", "month", "rating_as_of_month"):
            if column in frame:
                values = pd.to_datetime(frame[column], errors="coerce").dropna()
                days.extend(values.tolist())
    return max(days).date() if days else None


def _filter_future_static_ratings(
    inputs: dict[str, pd.DataFrame], as_of: date,
) -> tuple[dict[str, pd.DataFrame], dict[str, int]]:
    """Remove static ratings that were not available at t, with typed evidence."""
    filtered = dict(inputs)
    ratings = filtered.get("static_rating_mapping")
    if ratings is None or "rating_as_of_month" not in ratings:
        return filtered, {"static_rating_after_month": 0}
    observed_at = pd.to_datetime(ratings["rating_as_of_month"], errors="coerce")
    future = observed_at > pd.Timestamp(as_of)
    filtered["static_rating_mapping"] = ratings.loc[~future].copy()
    return filtered, {"static_rating_after_month": int(future.sum())}


def _normalized_monthly_curve(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"tenor", "day", "yield_pct"}
    if frame.empty or not required.issubset(frame):
        raise ValueError("monthly_curve_untyped_or_empty")
    curve = frame.copy()
    tenor_map = {
        "1y": "DGS1", "2y": "DGS2", "3y": "DGS3", "5y": "DGS5",
        "7y": "DGS7", "10y": "DGS10", "20y": "DGS20", "30y": "DGS30",
    }
    curve["tenor"] = curve["tenor"].astype(str).str.lower().str.strip().map(tenor_map)
    curve = curve.dropna(subset=["tenor"])
    daily = curve.pivot_table(
        index=pd.to_datetime(curve["day"]),
        columns="tenor",
        values="yield_pct",
        aggfunc="median",
    )
    return monthly_treasury_curve(daily)


def _rebuild_month(
    conn: Any, month: pd.Timestamp,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    date | None,
    pd.Timestamp,
    pd.DataFrame,
    dict[str, int],
    pd.Series,
    pd.DataFrame,
]:
    """Use precisely the Stage 6 input loader and resolver seams for one month."""
    as_of = _month_end(month)
    # Parity is a one-month walk-forward reconstruction. Passing t+1 to the
    # daily loader would admit its historical monthly-liquidity row even though
    # Stage 6 later fits only t; pin both loader month arguments to t instead.
    inputs, lineage = bond_panel._load_inputs(
        conn,
        month,
        month,
        as_of,
        structural_publication_id=BASE_PUBLICATION_ID,
        structural_month=month.date(),
    )
    reference_frame = inputs["resolved_issuer_sector"].copy()
    reference_column = next(
        (name for name in ("cusip9", "cusip_id") if name in reference_frame),
        None,
    )
    if reference_column is None:
        raise ValueError("reference_cusip_column_missing")
    reference_keys = reference_frame[reference_column].copy()
    inputs, input_exclusions = _filter_future_static_ratings(inputs, as_of)
    if not lineage or any(not value for value in lineage.values()):
        raise ValueError("input_lineage_missing")
    max_day = _input_max_day(inputs)
    if max_day is None:
        raise ValueError("input_day_missing")
    if max_day > as_of:
        raise ValueError("input_day_after_month_end")
    normalized_curve = _normalized_monthly_curve(inputs["monthly_curve"])
    panel = build_db_monthly_panel(**inputs, months=[month])
    if panel.empty:
        raise ValueError("panel_rebuild_empty")
    # These are the exact Stage 6 terms/liquidity annotations before snapshots.
    panel["issuer_identity_state"] = panel["issuer_identity_state"].fillna("unresolved") if "issuer_identity_state" in panel else "unresolved"
    panel["liquidity_reason"] = panel["reason_code"].fillna("monthly_liquidity_absent") if "reason_code" in panel else "monthly_liquidity_absent"
    terms_present = panel.get("coupon_pct", pd.Series(index=panel.index, dtype=float)).notna() & panel.get("maturity_date", pd.Series(index=panel.index, dtype=object)).notna() & panel.get("amt_outstanding_k", pd.Series(index=panel.index, dtype=float)).notna()
    panel["terms_source"] = terms_present.map({True: "bond_reference_terms", False: "terms_missing"})
    panel["terms_reason"] = terms_present.map({True: "terms_present", False: "terms_missing"})
    panel["spread_source"] = panel["spread_final"].notna().map({True: "computed", False: "missing_curve"})
    ratings = panel[["cusip_id", "month", "rating_bucket", "rating_as_of_month", "rating_state", "rating_reason", "rating_staleness_months"]].copy()
    panel_without_ratings = panel.drop(columns=[column for column in ratings.columns if column in panel and column not in {"cusip_id", "month"}])
    snapshots, exclusions = build_snapshots(panel_without_ratings, ratings_pit=ratings)
    if not exclusions.empty:
        exclusions = exclusions.merge(ratings, on=["cusip_id", "month"], how="left")
    snapshot = pd.concat([snapshots, exclusions], ignore_index=True).sort_values(["month", "cusip_id"])
    snapshot["month"] = pd.to_datetime(snapshot["month"])
    rebuilt_snapshot = snapshot[snapshot["month"].eq(month)].reset_index(drop=True)
    included = snapshots[snapshots["month"].eq(month)]
    signals, fit_diagnostics = fit_all_months(included, as_of=month)
    if not signals.empty:
        signals = signals.merge(
            included,
            on=["cusip_id", "month"],
            how="inner",
            validate="one_to_one",
            suffixes=("", "_snapshot"),
        )
    rebuilt_rv = signals[signals["month"].eq(month)].reset_index(drop=True) if not signals.empty else signals
    if not rebuilt_rv.empty:
        rebuilt_rv["month"] = pd.to_datetime(rebuilt_rv["month"])
    return (
        rebuilt_snapshot,
        rebuilt_rv,
        max_day,
        month,
        normalized_curve,
        input_exclusions,
        reference_keys,
        fit_diagnostics,
    )


def _valid_lineage(frame: pd.DataFrame) -> bool:
    return "source_lineage" in frame and all(isinstance(value, dict) and bool(value) for value in frame["source_lineage"])


def _typed_exclusions(frame: pd.DataFrame) -> float:
    if frame.empty or not {"eligibility_state", "eligibility_reason"}.issubset(frame):
        return 0.0
    excluded = frame[frame["eligibility_state"].eq("excluded")]
    if excluded.empty:
        return 1.0
    return float(excluded["eligibility_reason"].map(lambda value: isinstance(value, str) and bool(value.strip())).mean())


def _normalized_keys(values: pd.Series) -> pd.Series:
    normalized = values.astype("string").str.strip().str.upper()
    return normalized.mask(normalized.eq(""))


def _rv_structure(
    rebuilt_rv: pd.DataFrame,
    rebuilt_included: pd.DataFrame,
    fit_diagnostics: pd.DataFrame,
    month: pd.Timestamp,
) -> dict[str, Any]:
    required = {
        "cusip_id", "month", "spread_bps", "fitted_bps", "residual_bps",
        "rv_signal",
    }
    required_present = required.issubset(rebuilt_rv.columns)
    rebuilt_rv_spread_definition_ok = bool(
        "spread_definition" in rebuilt_rv
        and rebuilt_rv["spread_definition"].notna().all()
        and rebuilt_rv["spread_definition"].eq(SPREAD_DEFINITION).all()
    )
    rv_keys = (
        _normalized_keys(rebuilt_rv["cusip_id"])
        if "cusip_id" in rebuilt_rv
        else pd.Series(pd.NA, index=rebuilt_rv.index, dtype="string")
    )
    included_keys = (
        _normalized_keys(rebuilt_included["cusip_id"])
        if "cusip_id" in rebuilt_included
        else pd.Series(pd.NA, index=rebuilt_included.index, dtype="string")
    )
    included_key_set = set(included_keys.dropna().tolist())
    included_snapshot_typed = {"cusip_id", "spread_final"}.issubset(
        rebuilt_included.columns
    )
    included_keys_unique = bool(
        included_keys.notna().all()
        and not included_keys.dropna().duplicated().any()
    )
    diagnostic_rows = (
        fit_diagnostics.loc[
            pd.to_datetime(fit_diagnostics["month"], errors="coerce").eq(month)
        ]
        if "month" in fit_diagnostics
        else fit_diagnostics.iloc[0:0]
    )
    raw_fit_count = (
        pd.to_numeric(
            pd.Series([diagnostic_rows.iloc[0]["n"]]),
            errors="coerce",
        ).iloc[0]
        if len(diagnostic_rows) == 1 and "n" in diagnostic_rows
        else np.nan
    )
    fit_count_valid = bool(
        pd.notna(raw_fit_count)
        and np.isfinite(float(raw_fit_count))
        and float(raw_fit_count).is_integer()
        and float(raw_fit_count) >= 0
    )
    skipped_value = (
        diagnostic_rows.iloc[0]["skipped"]
        if len(diagnostic_rows) == 1 and "skipped" in diagnostic_rows
        else None
    )
    diagnostic_valid = (
        len(diagnostic_rows) == 1
        and fit_count_valid
        and isinstance(skipped_value, (bool, np.bool_))
        and not bool(skipped_value)
    )
    fit_count = int(raw_fit_count) if diagnostic_valid else None
    numeric = (
        rebuilt_rv[["spread_bps", "fitted_bps", "residual_bps", "rv_signal"]].apply(
            pd.to_numeric, errors="coerce"
        )
        if required_present
        else pd.DataFrame()
    )
    finite = bool(
        required_present
        and len(numeric) == len(rebuilt_rv)
        and np.isfinite(numeric.to_numpy(dtype=float)).all()
    )
    rv_mean = float(numeric["rv_signal"].mean()) if finite and len(numeric) else None
    rv_std = float(numeric["rv_signal"].std(ddof=0)) if finite and len(numeric) else None
    residual_identity_error = (
        (numeric["residual_bps"] - (numeric["spread_bps"] - numeric["fitted_bps"])).abs()
        if finite
        else pd.Series(dtype=float)
    )
    residual_std = (
        float(numeric["residual_bps"].std(ddof=0)) if finite and len(numeric) else None
    )
    expected_signal = (
        (numeric["residual_bps"] - numeric["residual_bps"].mean()) / residual_std
        if residual_std is not None and np.isfinite(residual_std) and residual_std > 0.0
        else pd.Series(np.nan, index=numeric.index, dtype=float)
    )
    rv_signal_error = (
        (numeric["rv_signal"] - expected_signal).abs()
        if finite
        else pd.Series(dtype=float)
    )
    snapshot_spread_error = pd.Series(dtype=float)
    snapshot_binding_complete = False
    if (
        required_present
        and included_snapshot_typed
        and included_keys_unique
        and rv_keys.notna().all()
        and not rv_keys.dropna().duplicated().any()
    ):
        rv_binding = pd.DataFrame({
            "cusip_id": rv_keys,
            "spread_bps": numeric["spread_bps"],
        })
        included_binding = pd.DataFrame({
            "cusip_id": included_keys,
            "spread_final": pd.to_numeric(
                rebuilt_included["spread_final"], errors="coerce"
            ),
        })
        bound = rv_binding.merge(
            included_binding,
            on="cusip_id",
            how="left",
            validate="one_to_one",
        )
        expected_spread = (bound["spread_final"] * 10_000.0).clip(
            *SPREAD_WINSOR_BPS
        )
        snapshot_spread_error = (bound["spread_bps"] - expected_spread).abs()
        snapshot_binding_complete = bool(
            len(bound) == len(rebuilt_rv)
            and np.isfinite(expected_spread.to_numpy(dtype=float)).all()
        )

    def max_error(error: pd.Series) -> float | None:
        return (
            float(error.max())
            if len(error) and np.isfinite(error.to_numpy(dtype=float)).all()
            else None
        )

    max_residual_identity_error = max_error(residual_identity_error)
    max_rv_signal_error = max_error(rv_signal_error)
    max_snapshot_spread_error = max_error(snapshot_spread_error)
    gates = {
        "rebuilt_rv_nonempty": bool(len(rebuilt_rv)),
        "required_columns_present": required_present,
        "rebuilt_rv_spread_definition": rebuilt_rv_spread_definition_ok,
        "rv_keys_valid": bool(rv_keys.notna().all()),
        "rv_keys_unique": bool(not rv_keys.dropna().duplicated().any()),
        "rv_keys_subset_of_included": set(rv_keys.dropna()).issubset(included_key_set),
        "rv_month_exact": bool(
            required_present
            and pd.to_datetime(rebuilt_rv["month"], errors="coerce").eq(month).all()
        ),
        "fit_diagnostics_valid": diagnostic_valid,
        "row_count_matches_fit": fit_count == len(rebuilt_rv),
        "rv_values_finite": finite,
        "rv_mean_centered": rv_mean is not None and abs(rv_mean) <= RV_MEAN_TOLERANCE,
        "rv_population_std_unit": rv_std is not None and abs(rv_std - 1) <= RV_STD_TOLERANCE,
        "residual_matches_spread_minus_fitted": (
            max_residual_identity_error is not None
            and max_residual_identity_error <= RV_STRUCTURE_TOLERANCE
        ),
        "rv_signal_matches_residual_zscore": (
            max_rv_signal_error is not None
            and max_rv_signal_error <= RV_STRUCTURE_TOLERANCE
        ),
        "spread_matches_included_snapshot": (
            snapshot_binding_complete
            and max_snapshot_spread_error is not None
            and max_snapshot_spread_error <= RV_STRUCTURE_TOLERANCE
        ),
    }
    return {
        "passed": bool(all(gates.values())),
        "gates": gates,
        "row_count": int(len(rebuilt_rv)),
        "fit_row_count": fit_count,
        "included_row_count": int(len(rebuilt_included)),
        "rv_mean": rv_mean,
        "rv_population_std": rv_std,
        "max_residual_identity_error": max_residual_identity_error,
        "max_rv_signal_error": max_rv_signal_error,
        "max_snapshot_spread_error": max_snapshot_spread_error,
    }


def _reference_accounting(
    reference_keys: pd.Series,
    rebuilt_snapshot: pd.DataFrame,
) -> dict[str, Any]:
    reference = _normalized_keys(reference_keys)
    rebuilt = (
        _normalized_keys(rebuilt_snapshot["cusip_id"])
        if "cusip_id" in rebuilt_snapshot
        else pd.Series(pd.NA, index=rebuilt_snapshot.index, dtype="string")
    )
    states = rebuilt_snapshot.get(
        "eligibility_state",
        pd.Series(pd.NA, index=rebuilt_snapshot.index, dtype="string"),
    ).astype("string")
    reasons = rebuilt_snapshot.get(
        "eligibility_reason",
        pd.Series(pd.NA, index=rebuilt_snapshot.index, dtype="string"),
    ).astype("string").str.strip()
    identities = rebuilt_snapshot.get(
        "issuer_id",
        pd.Series(pd.NA, index=rebuilt_snapshot.index, dtype="string"),
    ).astype("string").str.strip()

    valid_reference = reference.dropna()
    valid_rebuilt = rebuilt.dropna()
    reference_set = set(valid_reference.tolist())
    rebuilt_set = set(valid_rebuilt.tolist())
    included = states.eq("included")
    excluded = states.eq("excluded")
    typed_exclusions = (~excluded) | (reasons.notna() & reasons.ne(""))
    identified_included = (~included) | (identities.notna() & identities.ne(""))
    gates = {
        "reference_nonempty": bool(len(valid_reference)),
        "reference_keys_valid": bool(reference.notna().all()),
        "reference_keys_unique": bool(not valid_reference.duplicated().any()),
        "rebuilt_keys_valid": bool(rebuilt.notna().all()),
        "rebuilt_keys_unique": bool(not valid_rebuilt.duplicated().any()),
        "exact_reference_key_set": reference_set == rebuilt_set,
        "eligibility_states_recognized": bool(
            states.notna().all()
            and states.isin(RECOGNIZED_ELIGIBILITY_STATES).all()
        ),
        "excluded_reasons_typed": bool(typed_exclusions.all()),
        "included_identity_present": bool(identified_included.all()),
    }
    exclusion_counts = {
        str(reason): int(count)
        for reason, count in reasons.loc[excluded & reasons.notna() & reasons.ne("")]
        .value_counts()
        .sort_index()
        .items()
    }
    return {
        "passed": bool(all(gates.values())),
        "gates": gates,
        "reference_source_rows": int(len(reference)),
        "reference_size": int(len(reference_set)),
        "rebuilt_size": int(len(rebuilt)),
        "included_size": int(included.sum()),
        "excluded_size": int(excluded.sum()),
        "exclusion_counts": exclusion_counts,
        "invalid_reference_key_rows": int(reference.isna().sum()),
        "invalid_rebuilt_key_rows": int(rebuilt.isna().sum()),
        "duplicate_reference_key_rows": int(valid_reference.duplicated(keep=False).sum()),
        "duplicate_rebuilt_key_rows": int(valid_rebuilt.duplicated(keep=False).sum()),
        "missing_reference_key_count": int(len(reference_set - rebuilt_set)),
        "unexpected_rebuilt_key_count": int(len(rebuilt_set - reference_set)),
        "missing_reference_keys": sorted(reference_set - rebuilt_set)[:50],
        "unexpected_rebuilt_keys": sorted(rebuilt_set - reference_set)[:50],
    }


def _quantile_gate(
    values: pd.Series, limits: tuple[float, float, float],
) -> tuple[dict[str, float | None], bool]:
    if values.empty or values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        return {"median": None, "p90": None, "p99": None}, False
    stats = {"median": float(values.quantile(.5)), "p90": float(values.quantile(.9)), "p99": float(values.quantile(.99))}
    return stats, all(stats[key] <= limit for key, limit in zip(("median", "p90", "p99"), limits))


def _spread_semantics(
    frame: pd.DataFrame, monthly_curve: pd.DataFrame,
) -> tuple[dict[str, float | int | None], bool]:
    maturity = "bond_maturity" if "bond_maturity" in frame else "maturity_years"
    required = {"month", "ytm", "spread_final", "spread_final_bps", maturity}
    if not required.issubset(frame):
        return {"rows": int(len(frame)), "max_abs_error": None, "max_bps_conversion_error": None}, False
    if frame.empty:
        return {"rows": 0, "max_abs_error": None, "max_bps_conversion_error": None}, True
    candidate = frame[["month", "ytm", "spread_final", "spread_final_bps", maturity]].copy()
    candidate = candidate.rename(columns={maturity: "bond_maturity"}).reset_index(drop=True)
    expected = pd.to_numeric(compute_spread(candidate, monthly_curve), errors="coerce")
    observed = pd.to_numeric(candidate["spread_final"], errors="coerce")
    observed_bps = pd.to_numeric(candidate["spread_final_bps"], errors="coerce")
    finite = (
        expected.notna().all()
        and observed.notna().all()
        and observed_bps.notna().all()
        and np.isfinite(expected.to_numpy(dtype=float)).all()
        and np.isfinite(observed.to_numpy(dtype=float)).all()
        and np.isfinite(observed_bps.to_numpy(dtype=float)).all()
    )
    if not finite:
        return {"rows": int(len(candidate)), "max_abs_error": None, "max_bps_conversion_error": None}, False
    semantic_error = (observed - expected).abs()
    conversion_error = (observed * 10_000 - observed_bps).abs()
    evidence: dict[str, float | int | None] = {
        "rows": int(len(candidate)),
        "max_abs_error": float(semantic_error.max()),
        "max_bps_conversion_error": float(conversion_error.max()),
    }
    return evidence, bool(
        (semantic_error <= 1e-10).all() and (conversion_error <= 1e-10).all()
    )


def _compare_month(
    month: pd.Timestamp,
    frozen_snapshot: pd.DataFrame,
    frozen_rv: pd.DataFrame,
    rebuilt_snapshot: pd.DataFrame,
    rebuilt_rv: pd.DataFrame,
    *,
    input_max_day: date | None,
    fit_as_of: pd.Timestamp,
    monthly_curve: pd.DataFrame,
    reference_keys: pd.Series,
    fit_diagnostics: pd.DataFrame,
    input_exclusions: dict[str, int] | None = None,
) -> dict[str, object]:
    """Return separately accounted, comparable, and diagnostic monthly parity evidence."""
    label = month.date().isoformat()
    snapshot_fields = {
        "cusip_id", "month", "eligibility_state", "eligibility_reason",
        "ytm", "mod_dur", "spread_final", "spread_final_bps",
        "spread_definition",
    }
    frozen_snapshot_typed = snapshot_fields.issubset(frozen_snapshot)
    rebuilt_snapshot_typed = snapshot_fields.issubset(rebuilt_snapshot)
    frozen_rv_fields = {"cusip_id", "month", "rv_signal", "spread_definition"}
    frozen_rv_typed = frozen_rv.empty or frozen_rv_fields.issubset(frozen_rv)

    frozen_included = (
        frozen_snapshot.loc[
            frozen_snapshot["eligibility_state"].eq("included")
        ].copy()
        if frozen_snapshot_typed
        else pd.DataFrame(columns=["cusip_id", "month"])
    )
    rebuilt_included = (
        rebuilt_snapshot.loc[
            rebuilt_snapshot["eligibility_state"].eq("included")
        ].copy()
        if rebuilt_snapshot_typed
        else pd.DataFrame(columns=["cusip_id", "month"])
    )
    duplicate_snapshot_keys = bool(
        frozen_included.duplicated(["cusip_id", "month"]).any()
        or rebuilt_included.duplicated(["cusip_id", "month"]).any()
    )
    if duplicate_snapshot_keys:
        common_snapshot = pd.DataFrame()
    else:
        common_snapshot = frozen_included.merge(
            rebuilt_included,
            on=["cusip_id", "month"],
            suffixes=("_frozen", "_rebuilt"),
            validate="one_to_one",
        )

    frozen_n = int(len(frozen_included))
    rebuilt_n = int(len(rebuilt_included))
    matched_bonds = int(len(common_snapshot))
    frozen_keys = set(_normalized_keys(frozen_included.get("cusip_id", pd.Series(dtype="string"))).dropna())
    rebuilt_keys = set(_normalized_keys(rebuilt_included.get("cusip_id", pd.Series(dtype="string"))).dropna())
    universe_limit = max(25, .005 * max(frozen_n, rebuilt_n))
    membership = {
        "frozen_included_size": frozen_n,
        "rebuilt_included_size": rebuilt_n,
        "common_size": matched_bonds,
        "frozen_overlap_ratio": matched_bonds / frozen_n if frozen_n else 0.0,
        "rebuilt_overlap_ratio": matched_bonds / rebuilt_n if rebuilt_n else 0.0,
        "symmetric_difference_size": int(len(frozen_keys ^ rebuilt_keys)),
        "universe_delta": abs(frozen_n - rebuilt_n),
        "universe_delta_limit": universe_limit,
    }
    comparable = matched_bonds >= MIN_MONTH_ROWS
    reference_accounting = _reference_accounting(reference_keys, rebuilt_snapshot)

    frozen_rv_present = not frozen_rv.empty
    frozen_lineage_ok = (
        _valid_lineage(frozen_snapshot)
        and (not frozen_rv_present or _valid_lineage(frozen_rv))
    )
    frozen_typed_exclusions = _typed_exclusions(frozen_snapshot)
    rebuilt_typed_exclusions = _typed_exclusions(rebuilt_snapshot)
    frozen_spread_definition_ok = (
        frozen_snapshot_typed
        and set(frozen_snapshot["spread_definition"].dropna()) == {SPREAD_DEFINITION}
    )
    rebuilt_spread_definition_ok = (
        rebuilt_snapshot_typed
        and set(rebuilt_snapshot["spread_definition"].dropna()) == {SPREAD_DEFINITION}
    )
    frozen_rv_spread_definition_ok = (
        not frozen_rv_present
        or (
            frozen_rv_typed
            and set(frozen_rv["spread_definition"].dropna()) == {SPREAD_DEFINITION}
        )
    )
    frozen_semantics, frozen_semantics_ok = _spread_semantics(
        frozen_included, monthly_curve
    )
    rebuilt_semantics, rebuilt_semantics_ok = _spread_semantics(
        rebuilt_included, monthly_curve
    )
    walk_forward_ok = (
        input_max_day is not None
        and input_max_day <= _month_end(month)
        and fit_as_of == month
    )
    hard_gates = {
        "frozen_snapshot_nonempty": not frozen_snapshot.empty,
        "rebuilt_snapshot_nonempty": not rebuilt_snapshot.empty,
        "snapshot_types": frozen_snapshot_typed and rebuilt_snapshot_typed,
        "frozen_rv_types": frozen_rv_typed,
        "frozen_lineage": frozen_lineage_ok,
        "unique_universe_keys": not duplicate_snapshot_keys,
        "typed_exclusions": (
            frozen_typed_exclusions == 1.0 and rebuilt_typed_exclusions == 1.0
        ),
        "spread_definition": (
            frozen_spread_definition_ok
            and rebuilt_spread_definition_ok
            and frozen_rv_spread_definition_ok
        ),
        "spread_numeric_semantics": frozen_semantics_ok and rebuilt_semantics_ok,
        "walk_forward": walk_forward_ok,
    }

    formula_metrics: dict[str, dict[str, float | None]] = {}
    formula_gates: dict[str, bool] = {}
    if comparable:
        metric_specs = {
            "ytm_abs_bps": ("ytm", "ytm", 10_000.0, (1.0, 5.0, 25.0)),
            "duration_abs_years": ("mod_dur", "mod_dur", 1.0, (.10, .50, 1.0)),
            "spread_abs_bps": (
                "spread_final_bps", "spread_final_bps", 1.0, (5.0, 25.0, 75.0)
            ),
        }
        for gate, (frozen_key, rebuilt_key, scale, limits) in metric_specs.items():
            delta = (
                pd.to_numeric(
                    common_snapshot[f"{frozen_key}_frozen"], errors="coerce"
                )
                - pd.to_numeric(
                    common_snapshot[f"{rebuilt_key}_rebuilt"], errors="coerce"
                )
            ).abs() * scale
            formula_metrics[gate], formula_gates[gate] = _quantile_gate(delta, limits)
        relative = (
            pd.to_numeric(common_snapshot["mod_dur_frozen"], errors="coerce")
            - pd.to_numeric(common_snapshot["mod_dur_rebuilt"], errors="coerce")
        ).abs() / pd.to_numeric(
            common_snapshot["mod_dur_frozen"], errors="coerce"
        ).abs().clip(lower=1e-12)
        formula_metrics["duration_relative"], formula_gates["duration_relative"] = (
            _quantile_gate(relative, (.02, .10, float("inf")))
        )
    formula_parity = {
        "evaluated": comparable,
        "passed": bool(all(formula_gates.values())) if comparable else None,
        "metrics": formula_metrics,
        "gates": formula_gates,
    }

    rv_structure = _rv_structure(
        rebuilt_rv, rebuilt_included, fit_diagnostics, month
    )
    rebuilt_rv_n = int(len(rebuilt_rv))
    if not frozen_rv_present:
        rv_metrics = {"median": None, "p90": None, "p99": None}
        rv_unavailable_reason = "frozen_rv_empty"
        common_rv_n = 0
    elif not frozen_rv_typed:
        rv_metrics = {"median": None, "p90": None, "p99": None}
        rv_unavailable_reason = "frozen_rv_untyped"
        common_rv_n = 0
    elif rebuilt_rv.empty or not frozen_rv_fields.issubset(rebuilt_rv):
        rv_metrics = {"median": None, "p90": None, "p99": None}
        rv_unavailable_reason = "rebuilt_rv_empty_or_untyped"
        common_rv_n = 0
    else:
        rv_common = frozen_rv.merge(
            rebuilt_rv,
            on=["cusip_id", "month"],
            suffixes=("_frozen", "_rebuilt"),
        )
        common_rv_n = int(len(rv_common))
        rv_metrics, _ = _quantile_gate(
            (
                pd.to_numeric(rv_common["rv_signal_frozen"], errors="coerce")
                - pd.to_numeric(rv_common["rv_signal_rebuilt"], errors="coerce")
            ).abs(),
            (.05, .25, .75),
        )
        rv_unavailable_reason = (
            "no_common_rv_keys" if rv_common.empty else None
        )
    rv_abs = {
        "frozen_size": int(len(frozen_rv)),
        "rebuilt_size": rebuilt_rv_n,
        "common_size": common_rv_n,
        "frozen_overlap_ratio": common_rv_n / len(frozen_rv) if len(frozen_rv) else 0.0,
        "rebuilt_overlap_ratio": common_rv_n / rebuilt_rv_n if rebuilt_rv_n else 0.0,
        "matched_coverage": (
            common_rv_n / min(len(frozen_rv), rebuilt_rv_n)
            if min(len(frozen_rv), rebuilt_rv_n)
            else 0.0
        ),
        "metrics": rv_metrics,
        "unavailable_reason": rv_unavailable_reason,
    }

    blocking_failure = (
        not reference_accounting["passed"]
        or not all(hard_gates.values())
        or (
            comparable
            and (
                formula_parity["passed"] is not True
                or not rv_structure["passed"]
            )
        )
    )
    if blocking_failure:
        state, aborted, reason = "parity_failed", True, "gate_failed"
    elif comparable:
        state, aborted, reason = "parity_passed", False, None
    else:
        state, aborted, reason = "parity_not_comparable", False, "not_comparable"

    failed_gates = [
        name
        for name, passed in reference_accounting["gates"].items()
        if not passed
    ]
    failed_gates.extend(
        name for name, passed in hard_gates.items() if not passed
    )
    if comparable:
        failed_gates.extend(
            name for name, passed in formula_gates.items() if not passed
        )
        failed_gates.extend(
            name for name, passed in rv_structure["gates"].items() if not passed
        )

    return {
        "month": label,
        "state": state,
        "reason": reason,
        "aborted": aborted,
        "matched_bonds": matched_bonds,
        "comparable": comparable,
        "reference_accounting": reference_accounting,
        "hard_gates": hard_gates,
        "formula_parity": formula_parity,
        "rv_structure": rv_structure,
        "diagnostics": {
            "membership": membership,
            "rv_abs": rv_abs,
        },
        "frozen_universe_size": frozen_n,
        "rebuilt_universe_size": rebuilt_n,
        "matched_coverage": membership["rebuilt_overlap_ratio"],
        "frozen_rv_size": int(len(frozen_rv)),
        "rebuilt_rv_size": rebuilt_rv_n,
        "rv_matched_coverage": rv_abs["matched_coverage"],
        "typed_exclusions": {
            "frozen": frozen_typed_exclusions,
            "rebuilt": rebuilt_typed_exclusions,
        },
        "spread_definition": SPREAD_DEFINITION,
        "spread_semantics": {
            "frozen": frozen_semantics,
            "rebuilt": rebuilt_semantics,
        },
        "walk_forward": {
            "max_input_day": input_max_day.isoformat() if input_max_day else None,
            "calendar_month_end": _month_end(month).isoformat(),
            "fit_as_of": fit_as_of.date().isoformat(),
            "input_exclusions": input_exclusions
            or {"static_rating_after_month": 0},
        },
        "failed_gates": failed_gates,
    }


def _overall_verdict(month_results: list[dict[str, Any]]) -> dict[str, Any]:
    """Fail closed unless every declared month has one legal parity result."""
    declared_months = [month.date().isoformat() for month in PARITY_MONTHS]
    observed_months: list[str] = []
    invalid_month_results: list[dict[str, object]] = []
    legal_results: list[dict[str, Any]] = []
    for index, result in enumerate(month_results):
        month = result.get("month")
        state = result.get("state")
        comparable = result.get("comparable")
        aborted = result.get("aborted")
        legal_tuple = (
            (state == "parity_passed" and comparable is True and aborted is False)
            or (
                state == "parity_not_comparable"
                and comparable is False
                and aborted is False
            )
            or (
                state == "parity_failed"
                and isinstance(comparable, bool)
                and aborted is True
            )
        )
        canonical_month = month if isinstance(month, str) else None
        if canonical_month is not None:
            observed_months.append(canonical_month)
        violations: list[str] = []
        if not legal_tuple:
            violations.append("illegal_state_tuple")
        if canonical_month is None:
            violations.append("month_not_canonical_iso_date")
        if violations:
            invalid_month_results.append({
                "index": index,
                "month": canonical_month,
                "state": state if isinstance(state, str) else None,
                "violations": violations,
            })
        else:
            legal_results.append(result)

    duplicates = sorted({
        month for month in observed_months if observed_months.count(month) > 1
    })
    unexpected = sorted(set(observed_months) - set(declared_months))
    missing = sorted(set(declared_months) - set(observed_months))
    declaration_exact = not missing and not duplicates and not unexpected
    failed = [
        result for result in legal_results
        if result.get("state") == "parity_failed"
    ]
    comparable_passed = [
        result for result in legal_results
        if result.get("state") == "parity_passed" and result.get("comparable") is True
    ]
    noncomparable = [
        result for result in legal_results
        if result.get("state") == "parity_not_comparable"
    ]
    failure_reasons: dict[str, int] = {}
    for result in failed:
        reason = result.get("reason")
        key = reason if isinstance(reason, str) and reason else "unspecified_monthly_failure"
        failure_reasons[key] = failure_reasons.get(key, 0) + 1

    gates = {
        "monthly_contract_valid": not invalid_month_results,
        "declared_months_exactly_once": declaration_exact,
        "all_months_nonblocking": not failed,
        "at_least_one_comparable_month": bool(comparable_passed),
        "all_comparable_months_passed": all(
            result.get("state") == "parity_passed" and result.get("aborted") is False
            for result in month_results
            if result.get("comparable") is True
        ),
    }
    if failed:
        state, reason, aborted = "parity_failed", "monthly_parity_failure", True
    elif not gates["monthly_contract_valid"] or not gates["declared_months_exactly_once"]:
        state, reason, aborted = "parity_failed", "monthly_contract_failure", True
    elif not comparable_passed:
        state, reason, aborted = "parity_failed", "no_comparable_month", True
    elif not all(gates.values()):
        state, reason, aborted = "parity_failed", "overall_gate_failure", True
    else:
        state, reason, aborted = "parity_passed", None, False
    return {
        "state": state,
        "reason": reason,
        "aborted": aborted,
        "counts": {
            "failed_months": len(failed),
            "comparable_passed_months": len(comparable_passed),
            "noncomparable_months": len(noncomparable),
        },
        "gates": gates,
        "failure_reasons": failure_reasons,
        "invalid_month_results": invalid_month_results,
        "month_declaration": {
            "declared": declared_months,
            "observed": observed_months,
            "missing": missing,
            "duplicates": duplicates,
            "unexpected": unexpected,
        },
    }


def run(dsn: str | None = None) -> dict[str, object]:
    """Run the two-month, DB-only parity gate without modifying any relation."""
    if config_hash() != PANEL_CONFIG_HASH:
        return _failure("config_hash_mismatch")
    results: list[dict[str, object]] = []
    with connect(resolve_dsn(dsn)) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        current = _current_publication(conn)
        if current is None:
            return _failure("current_publication_missing")
        if current[0] != BASE_PUBLICATION_ID:
            return _failure("current_publication_id_mismatch")
        if current[1] != PANEL_CONFIG_HASH:
            return _failure("current_publication_config_mismatch")
        if current[2] != BASE_INPUT_FINGERPRINT:
            return _failure("current_publication_fingerprint_mismatch")
        if current[3] != "validated":
            return _failure("current_publication_status_mismatch")
        for month in PARITY_MONTHS:
            frozen_snapshot, frozen_rv = _frozen_snapshot(conn, month), _frozen_rv(conn, month)
            try:
                (
                    rebuilt_snapshot,
                    rebuilt_rv,
                    max_day,
                    fit_as_of,
                    monthly_curve,
                    input_exclusions,
                    _reference_keys,
                    _fit_diagnostics,
                ) = _rebuild_month(conn, month)
            except (KeyError, TypeError, ValueError) as exc:
                return _failure(f"rebuild_error:{exc}", results)
            results.append(_compare_month(
                month,
                frozen_snapshot,
                frozen_rv,
                rebuilt_snapshot,
                rebuilt_rv,
                input_max_day=max_day,
                fit_as_of=fit_as_of,
                monthly_curve=monthly_curve,
                reference_keys=_reference_keys,
                fit_diagnostics=_fit_diagnostics,
                input_exclusions=input_exclusions,
            ))
    overall = _overall_verdict(results)
    return {**overall, "months": results}
