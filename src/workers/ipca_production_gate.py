"""Fail-closed production gate for the persisted six-factor IPCA surface."""

from __future__ import annotations

import math
import os
from datetime import date, datetime
from typing import Any

import numpy as np

from src.db import connect

EXPECTED_FEATURES = [
    "size_log_mkt_cap",
    "book_to_market",
    "mom_12_1",
    "quality_roa",
    "investment_growth",
    "profitability_gross",
]


def _env_float(name: str, default: float) -> float:
    value = float(os.getenv(name, default))
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a finite non-negative number")
    return value


def _env_int(name: str, default: int) -> int:
    value = int(os.getenv(name, default))
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _thresholds() -> dict[str, float | int]:
    return {
        "expected_k": _env_int("IPCA_EXPECTED_K", 6),
        "min_oos_r_squared": _env_float("IPCA_MIN_OOS_R2", 0.05),
        "min_catalog_coverage": _env_float("IPCA_MIN_CATALOG_COVERAGE", 0.995),
        "min_specific_variance_coverage": _env_float(
            "IPCA_MIN_SPECIFIC_VARIANCE_COVERAGE", 0.95
        ),
        "max_visible_null_t_stat_ratio": _env_float(
            "IPCA_MAX_VISIBLE_NULL_T_STAT_RATIO", 0.001
        ),
        "extreme_beta_abs": _env_float("IPCA_EXTREME_BETA_ABS", 10.0),
        "max_visible_extreme_beta_ratio": _env_float(
            "IPCA_MAX_VISIBLE_EXTREME_BETA_RATIO", 0.001
        ),
        "max_characteristics_age_days": _env_int(
            "IPCA_MAX_CHARACTERISTICS_AGE_DAYS", 210
        ),
        "max_fit_age_days": _env_int("IPCA_MAX_FIT_AGE_DAYS", 45),
    }


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _month_after(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _load_snapshot(
    conn: Any,
    *,
    extreme_beta_abs: float,
    expected_fit_id: str | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fit_id, fit_date, created_at::date, universe_hash, k_factors,
                   gamma_loadings, factor_returns, oos_r_squared, converged,
                   sample_start, sample_end, n_observations, n_instruments,
                   feature_names, degraded, production_fit,
                   gamma_drift_vs_prior, drift_alert
            FROM factor_model_fits
            WHERE engine = 'ipca'
              AND asset_class = 'Equity'
              AND (%s::uuid IS NULL OR fit_id = %s::uuid)
            ORDER BY fit_date DESC, created_at DESC
            LIMIT 1
            """,
            (expected_fit_id, expected_fit_id),
        )
        fit = cur.fetchone()
    if fit is None:
        raise RuntimeError("no eligible IPCA production fit")

    (
        fit_id,
        fit_date,
        created_date,
        universe_hash,
        k_factors,
        gamma,
        factor_returns,
        oos_r_squared,
        converged,
        sample_start,
        sample_end,
        n_observations,
        n_instruments,
        feature_names,
        degraded,
        production_fit,
        gamma_drift,
        drift_alert,
    ) = fit

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) AS rows,
                   count(DISTINCT x.instrument_id) AS instruments,
                   count(DISTINCT x.factor_index) AS factors,
                   min(x.factor_index) AS min_factor_index,
                   max(x.factor_index) AS max_factor_index,
                   count(*) FILTER (
                       WHERE x.beta IS NULL OR x.n_observations IS NULL
                          OR x.r_squared IS NULL
                   ) AS incomplete_rows,
                   count(DISTINCT x.instrument_id) FILTER (
                       WHERE x.t_stat IS NULL AND listed.instrument_id IS NOT NULL
                   ) AS visible_null_t_stat_instruments,
                   count(DISTINCT x.instrument_id) FILTER (
                       WHERE abs(x.beta) > %s AND listed.instrument_id IS NOT NULL
                   ) AS visible_extreme_beta_instruments
            FROM fund_factor_exposures x
            LEFT JOIN funds_list_mv listed
              ON listed.instrument_id = x.instrument_id
            WHERE x.fit_id = %s AND x.organization_id IS NULL
            """,
            (extreme_beta_abs, fit_id),
        )
        exposure = cur.fetchone()

        cur.execute(
            """
            SELECT count(*) AS rows,
                   count(DISTINCT instrument_id) AS instruments,
                   count(DISTINCT fit_id) AS fits,
                   min(fit_id::text) AS fit_id,
                   count(DISTINCT factor_index) AS factors
            FROM fund_factor_exposures_latest_mv
            """
        )
        materialized = cur.fetchone()

        cur.execute(
            """
            SELECT count(*) AS catalog_instruments,
                   count(*) FILTER (
                       WHERE EXISTS (
                           SELECT 1
                           FROM fund_factor_exposures x
                           WHERE x.instrument_id = listed.instrument_id
                             AND x.fit_id = %s::uuid
                             AND x.organization_id IS NULL
                       )
                   ) AS covered_instruments
            FROM funds_list_mv listed
            """,
            (fit_id,),
        )
        catalog = cur.fetchone()

        cur.execute(
            """
            SELECT count(*) AS rows,
                   count(DISTINCT instrument_id) AS instruments,
                   count(*) FILTER (WHERE variance_monthly <= 0) AS nonpositive
            FROM factor_model_specific_variances
            WHERE fit_id = %s
            """,
            (fit_id,),
        )
        specific = cur.fetchone()

        cur.execute(
            """
            SELECT current_date,
                   max(as_of),
                   max(computed_at)
            FROM equity_characteristics_monthly
            """
        )
        freshness = cur.fetchone()

    return {
        "fit_id": str(fit_id),
        "fit_date": fit_date,
        "created_date": created_date,
        "universe_hash": universe_hash,
        "k_factors": int(k_factors),
        "gamma": gamma,
        "factor_returns": factor_returns,
        "oos_r_squared": float(oos_r_squared) if oos_r_squared is not None else None,
        "converged": bool(converged),
        "sample_start": sample_start,
        "sample_end": sample_end,
        "n_observations": int(n_observations or 0),
        "n_instruments": int(n_instruments or 0),
        "feature_names": feature_names,
        "degraded": bool(degraded),
        "production_fit": bool(production_fit),
        "gamma_drift": float(gamma_drift) if gamma_drift is not None else None,
        "drift_alert": drift_alert,
        "exposure_rows": int(exposure[0]),
        "exposure_instruments": int(exposure[1]),
        "exposure_factors": int(exposure[2] or 0),
        "min_factor_index": exposure[3],
        "max_factor_index": exposure[4],
        "incomplete_exposure_rows": int(exposure[5]),
        "visible_null_t_stat_instruments": int(exposure[6]),
        "visible_extreme_beta_instruments": int(exposure[7]),
        "mv_rows": int(materialized[0]),
        "mv_instruments": int(materialized[1]),
        "mv_fits": int(materialized[2]),
        "mv_fit_id": materialized[3],
        "mv_factors": int(materialized[4] or 0),
        "catalog_instruments": int(catalog[0]),
        "covered_instruments": int(catalog[1]),
        "specific_variance_rows": int(specific[0]),
        "specific_variance_instruments": int(specific[1]),
        "nonpositive_specific_variances": int(specific[2]),
        "current_date": freshness[0],
        "characteristics_max_as_of": freshness[1],
        "characteristics_last_computed": freshness[2],
    }


def evaluate_snapshot(
    snapshot: dict[str, Any],
    *,
    expected_fit_id: str | None = None,
    thresholds: dict[str, float | int] | None = None,
    require_mv_sync: bool = True,
    require_production_fit: bool = True,
    min_characteristics_computed_at: datetime | None = None,
) -> dict[str, Any]:
    limits = thresholds or _thresholds()
    expected_k = int(limits["expected_k"])
    errors: list[str] = []
    warnings: list[str] = []

    if expected_fit_id is not None and snapshot["fit_id"] != expected_fit_id:
        errors.append(
            f"latest fit_id {snapshot['fit_id']} != expected {expected_fit_id}"
        )
    if snapshot["k_factors"] != expected_k:
        errors.append(f"k_factors {snapshot['k_factors']} != {expected_k}")
    if not snapshot["converged"] or snapshot["degraded"]:
        errors.append("latest fit is not eligible or converged")
    if require_production_fit and not snapshot["production_fit"]:
        errors.append("latest fit is not activated for production")
    oos_r_squared = snapshot["oos_r_squared"]
    if (
        oos_r_squared is None
        or not math.isfinite(oos_r_squared)
        or oos_r_squared < float(limits["min_oos_r_squared"])
    ):
        errors.append("OOS R-squared is below the production threshold")

    gamma = np.asarray(snapshot["gamma"], dtype=float)
    returns = snapshot["factor_returns"] or {}
    factor_values = np.asarray(returns.get("values", []), dtype=float)
    factor_dates = list(returns.get("dates", []))
    try:
        parsed_factor_dates = [date.fromisoformat(str(value)[:10]) for value in factor_dates]
    except ValueError:
        parsed_factor_dates = []
    if gamma.shape != (len(EXPECTED_FEATURES), expected_k) or not np.isfinite(gamma).all():
        errors.append(f"invalid Gamma shape or values: {gamma.shape}")
    if (
        factor_values.shape != (expected_k, len(factor_dates))
        or not factor_dates
        or factor_dates != sorted(set(factor_dates))
        or not np.isfinite(factor_values).all()
        or not parsed_factor_dates
        or parsed_factor_dates[0] != snapshot["sample_start"]
        or parsed_factor_dates[-1] != snapshot["sample_end"]
    ):
        errors.append("invalid factor return matrix or dates")
    if list(snapshot["feature_names"] or []) != EXPECTED_FEATURES:
        errors.append("feature_names do not match the certified characteristic order")

    gamma_drift = snapshot["gamma_drift"]
    if gamma_drift is None or not math.isfinite(gamma_drift) or gamma_drift < 0:
        errors.append("gamma drift was not persisted for the latest fit")
    if snapshot["drift_alert"] is not False:
        errors.append("gamma drift alert is not explicitly false")

    expected_rows = snapshot["exposure_instruments"] * expected_k
    if snapshot["exposure_rows"] != expected_rows or snapshot["exposure_rows"] == 0:
        errors.append("fund exposure row count is not instruments x K")
    if (
        snapshot["exposure_factors"] != expected_k
        or snapshot["min_factor_index"] != 1
        or snapshot["max_factor_index"] != expected_k
    ):
        errors.append("fund exposure factor indexes are incomplete")
    if snapshot["incomplete_exposure_rows"]:
        errors.append("fund exposures contain null beta, observations, or R-squared")

    if require_mv_sync:
        if (
            snapshot["mv_fits"] != 1
            or snapshot["mv_fit_id"] != snapshot["fit_id"]
            or snapshot["mv_rows"] != snapshot["exposure_rows"]
            or snapshot["mv_factors"] != expected_k
        ):
            errors.append("materialized view is not synchronized to the latest fit_id")

    catalog_coverage = _ratio(
        snapshot["covered_instruments"], snapshot["catalog_instruments"]
    )
    if catalog_coverage < float(limits["min_catalog_coverage"]):
        errors.append("visible catalog factor coverage is below threshold")

    specific_coverage = _ratio(
        snapshot["specific_variance_instruments"], snapshot["n_instruments"]
    )
    if specific_coverage < float(limits["min_specific_variance_coverage"]):
        errors.append("specific variance coverage is below threshold")
    if snapshot["nonpositive_specific_variances"]:
        errors.append("specific variances contain non-positive values")

    null_t_ratio = _ratio(
        snapshot["visible_null_t_stat_instruments"], snapshot["catalog_instruments"]
    )
    if null_t_ratio > float(limits["max_visible_null_t_stat_ratio"]):
        errors.append("visible null t-stat instrument ratio is above threshold")
    elif snapshot["visible_null_t_stat_instruments"]:
        warnings.append("visible_null_t_stat_below_gate")

    extreme_beta_ratio = _ratio(
        snapshot["visible_extreme_beta_instruments"], snapshot["catalog_instruments"]
    )
    if extreme_beta_ratio > float(limits["max_visible_extreme_beta_ratio"]):
        errors.append("visible extreme beta instrument ratio is above threshold")
    elif snapshot["visible_extreme_beta_instruments"]:
        warnings.append("visible_extreme_beta_below_gate")

    current_date = snapshot["current_date"]
    characteristics_as_of = snapshot["characteristics_max_as_of"]
    characteristics_computed_at = snapshot["characteristics_last_computed"]
    if characteristics_as_of is None:
        errors.append("equity characteristics have no as_of anchor")
    else:
        characteristics_age = (current_date - characteristics_as_of).days
        if characteristics_age > int(limits["max_characteristics_age_days"]):
            errors.append("equity characteristics anchor is stale")
        if snapshot["sample_end"] != _month_after(characteristics_as_of):
            errors.append("fit sample_end is not the month after the characteristics anchor")
    if (
        min_characteristics_computed_at is not None
        and (
            characteristics_computed_at is None
            or characteristics_computed_at < min_characteristics_computed_at
        )
    ):
        errors.append("characteristics were not recomputed in this governed run")

    if (current_date - snapshot["created_date"]).days > int(limits["max_fit_age_days"]):
        errors.append("production fit is stale")

    if snapshot["covered_instruments"] < snapshot["catalog_instruments"]:
        warnings.append("catalog_coverage_below_100_percent")

    metrics = {
        "fit_id": snapshot["fit_id"],
        "k_factors": snapshot["k_factors"],
        "oos_r_squared": snapshot["oos_r_squared"],
        "gamma_drift": snapshot["gamma_drift"],
        "exposure_rows": snapshot["exposure_rows"],
        "exposure_instruments": snapshot["exposure_instruments"],
        "catalog_coverage": catalog_coverage,
        "specific_variance_coverage": specific_coverage,
        "visible_null_t_stat_ratio": null_t_ratio,
        "visible_extreme_beta_ratio": extreme_beta_ratio,
        "sample_end": snapshot["sample_end"],
        "characteristics_max_as_of": characteristics_as_of,
        "quality_warnings": warnings,
    }
    if errors:
        raise RuntimeError("IPCA production gate failed: " + "; ".join(errors))
    return {"status": "succeeded", **metrics}


def run(
    dsn: str,
    *,
    expected_fit_id: str | None = None,
    activate: bool = False,
    min_characteristics_computed_at: datetime | None = None,
) -> dict[str, Any]:
    limits = _thresholds()
    with connect(dsn) as conn:
        snapshot = _load_snapshot(
            conn,
            extreme_beta_abs=float(limits["extreme_beta_abs"]),
            expected_fit_id=expected_fit_id,
        )
        result = evaluate_snapshot(
            snapshot,
            expected_fit_id=expected_fit_id,
            thresholds=limits,
            require_mv_sync=not activate,
            require_production_fit=not activate,
            min_characteristics_computed_at=min_characteristics_computed_at,
        )
        if not activate:
            return result

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE factor_model_fits SET production_fit = TRUE "
                "WHERE fit_id = %s::uuid",
                (snapshot["fit_id"],),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"failed to activate IPCA fit {snapshot['fit_id']}")
            cur.execute("REFRESH MATERIALIZED VIEW fund_factor_exposures_latest_mv")

        activated = _load_snapshot(
            conn,
            extreme_beta_abs=float(limits["extreme_beta_abs"]),
            expected_fit_id=snapshot["fit_id"],
        )
        result = evaluate_snapshot(
            activated,
            expected_fit_id=snapshot["fit_id"],
            thresholds=limits,
            require_mv_sync=True,
            require_production_fit=True,
            min_characteristics_computed_at=min_characteristics_computed_at,
        )
        conn.commit()
        return {**result, "activated": True}
