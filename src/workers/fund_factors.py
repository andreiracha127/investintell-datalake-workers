"""fund_factors — OLS de exposições de fatores por fundo (db-first do A1).

Para cada fundo: retornos mensais do NAV (resample mensal de nav_timeseries →
pct_change) regredidos por OLS contra factor_model_fits.factor_returns (fit IPCA
mais recente). Produz beta/t_stat/significância por fator. Upsert idempotente em
fund_factor_exposures; depois REFRESH … CONCURRENTLY fund_factor_exposures_latest_mv
em conexão autocommit FORA do advisory lock (padrão risk_metrics).
"""
from __future__ import annotations

import datetime as _dt
import math

import numpy as np

from src.db import LOCK_FUND_FACTORS, advisory_lock, connect

_SIG = ((2.58, "***"), (1.96, "**"), (1.65, "*"))
_NUMERIC_14_8_MAX = 999_999.99999999


def _significance(t_stat: float | None) -> str | None:
    if t_stat is None or math.isnan(t_stat):
        return None
    level = abs(t_stat)
    for threshold, mark in _SIG:
        if level >= threshold:
            return mark
    return None


def _numeric_14_8(value: float | None) -> float | None:
    """Normalize a value for the fund_factor_exposures NUMERIC(14,8) contract."""
    if value is None:
        return None
    normalized = float(value)
    if not math.isfinite(normalized):
        return None
    normalized = round(normalized, 8)
    if abs(normalized) > _NUMERIC_14_8_MAX:
        return None
    return normalized


def _storage_values(row: dict) -> tuple[float | None, float | None, str | None]:
    beta = _numeric_14_8(row["beta"])
    t_stat = _numeric_14_8(row["t_stat"]) if beta is not None else None
    return beta, t_stat, _significance(t_stat)


def ols_factor_exposures(y: np.ndarray, x: np.ndarray) -> list[dict]:
    """OLS de y (Nx1) sobre x (NxK) com intercepto. Retorna uma linha por fator
    (exclui o intercepto): {"factor","beta","t_stat","significance"}.
    Espelha _ols_market_sensitivities (lstsq, SE de sigma2·(XᵀX)⁻¹, dof=N−(K+1)).
    """
    y = np.asarray(y, dtype=float)
    x = np.asarray(x, dtype=float)
    if x.ndim != 2 or len(y) < max(10, x.shape[1] + 2):
        return []
    x_design = np.column_stack([np.ones(len(x)), x])
    beta, *_ = np.linalg.lstsq(x_design, y, rcond=None)
    residuals = y - x_design @ beta
    ss_res = float(residuals @ residuals)
    centered = y - float(np.mean(y))
    ss_tot = float(centered @ centered)
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0.0 else None
    dof = len(y) - x_design.shape[1]
    if dof <= 0:
        t_stats = np.full(beta.shape, np.nan)
    else:
        sigma2 = float((residuals @ residuals) / dof)
        cov = sigma2 * np.linalg.pinv(x_design.T @ x_design)
        se = np.sqrt(np.diag(cov))
        t_stats = np.divide(beta, se, out=np.full(beta.shape, np.nan), where=se > 0)
    out: list[dict] = []
    for idx in range(1, x_design.shape[1]):  # pula o intercepto
        t = float(t_stats[idx])
        t = None if math.isnan(t) else t
        out.append({
            "factor": f"Factor {idx}",
            "factor_index": idx,
            "beta": float(beta[idx]),
            "t_stat": t,
            "significance": _significance(t),
            "n_observations": int(len(y)),
            "r_squared": r_squared,
        })
    return out


_UPSERT = """
INSERT INTO fund_factor_exposures
    (instrument_id, factor, factor_index, as_of, fit_id, beta, t_stat,
     significance, n_observations, r_squared, organization_id)
VALUES (%(iid)s, %(factor)s, %(factor_index)s, %(as_of)s, %(fit_id)s,
        %(beta)s, %(t_stat)s, %(sig)s, %(n_observations)s, %(r_squared)s, NULL)
ON CONFLICT (instrument_id, factor, as_of, organization_id) DO UPDATE SET
    factor_index = EXCLUDED.factor_index, fit_id = EXCLUDED.fit_id,
    beta = EXCLUDED.beta, t_stat = EXCLUDED.t_stat,
    significance = EXCLUDED.significance,
    n_observations = EXCLUDED.n_observations,
    r_squared = EXCLUDED.r_squared, computed_at = now()
"""

_DELETE_STALE_FIT = """
DELETE FROM fund_factor_exposures
WHERE instrument_id = %(iid)s
  AND as_of = %(as_of)s
  AND organization_id IS NULL
  AND fit_id IS DISTINCT FROM %(fit_id)s
"""


def _refresh_latest_mv(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_factor_exposures_latest_mv"
            )


def _latest_factor_matrix(
    conn,
    calc_date: _dt.date | None = None,
) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fit_id, fit_date, sample_start, sample_end, k_factors,
                   factor_returns
            FROM factor_model_fits
            WHERE engine = 'ipca'
              AND asset_class = 'Equity'
              AND converged IS TRUE
              AND degraded IS FALSE
              AND production_fit IS TRUE
              AND (%s::date IS NULL OR fit_date <= %s::date)
            ORDER BY fit_date DESC, created_at DESC
            LIMIT 1
            """,
            (calc_date, calc_date),
        )
        row = cur.fetchone()
    if row is None or not isinstance(row[5], dict):
        return None
    fit_id, fit_date, sample_start, sample_end, k_factors, payload = row
    try:
        dates = [_dt.date.fromisoformat(d[:10]) for d in payload.get("dates", [])]
    except (TypeError, ValueError):
        return None
    values = payload.get("values", [])
    if (
        not dates
        or len(values) != int(k_factors)
        or dates != sorted(set(dates))
        or any(not isinstance(value, list) or len(value) != len(dates) for value in values)
    ):
        return None
    matrix = np.asarray(values, dtype=float).T
    if matrix.shape != (len(dates), int(k_factors)) or not np.isfinite(matrix).all():
        return None
    return {
        "fit_id": fit_id,
        "fit_date": fit_date,
        "sample_start": sample_start or dates[0],
        "sample_end": sample_end or dates[-1],
        "k_factors": int(k_factors),
        "dates": dates,
        "matrix": matrix,
    }


def _fund_monthly_returns(conn, iid, factor_dates: list[_dt.date]) -> np.ndarray:
    """Retornos mensais do fundo alinhados às datas dos fatores (month-end)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date_trunc('month', nav_date)::date AS m, "
            "       (array_agg(nav ORDER BY nav_date DESC))[1] AS last_nav "
            "FROM nav_timeseries WHERE instrument_id = %s AND nav IS NOT NULL "
            "GROUP BY 1 ORDER BY 1",
            (iid,),
        )
        rows = cur.fetchall()
    by_month = {r[0]: float(r[1]) for r in rows}
    months = sorted(by_month)
    rets: dict[_dt.date, float] = {}
    for prev, cur_m in zip(months, months[1:]):
        is_consecutive = (
            cur_m.year * 12 + cur_m.month
            == prev.year * 12 + prev.month + 1
        )
        if is_consecutive and by_month[prev]:
            rets[cur_m] = by_month[cur_m] / by_month[prev] - 1.0
    aligned = []
    for d in factor_dates:
        key = d.replace(day=1)
        aligned.append(rets.get(key, np.nan))
    return np.asarray(aligned, dtype=float)


def _fund_ids(conn, limit) -> list:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT instrument_id FROM nav_timeseries"
            + (" LIMIT %s" if limit else ""),
            ((limit,) if limit else None),
        )
        return [r[0] for r in cur.fetchall()]


def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
    as_of: str | None = None,
) -> dict:
    processed = upserted = 0
    out_date: _dt.date | None = None
    cutoff = _dt.date.fromisoformat(calc_date) if calc_date else None
    fit: dict | None = None
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FUND_FACTORS) as got:
            if not got:
                return {"processed": 0, "upserted": 0, "skipped": "lock_busy"}
            fit = _latest_factor_matrix(conn, cutoff)
            out_date = (
                _dt.date.fromisoformat(as_of)
                if as_of
                else (fit["sample_end"] if fit else None)
            )
            if fit is not None and out_date is not None:
                fdates = fit["dates"]
                fmatrix = fit["matrix"]
                for iid in _fund_ids(conn, limit):
                    y = _fund_monthly_returns(conn, iid, fdates)
                    mask = np.isfinite(y) & np.isfinite(fmatrix).all(axis=1)
                    if mask.sum() < max(10, fmatrix.shape[1] + 2):
                        continue
                    processed += 1
                    rows = ols_factor_exposures(y[mask], fmatrix[mask])
                    with conn.cursor() as cur:
                        cur.execute(
                            _DELETE_STALE_FIT,
                            {
                                "iid": iid,
                                "as_of": out_date,
                                "fit_id": fit["fit_id"],
                            },
                        )
                    for r in rows:
                        beta, t_stat, significance = _storage_values(r)
                        with conn.cursor() as cur:
                            cur.execute(_UPSERT, {
                                "iid": iid,
                                "factor": r["factor"],
                                "factor_index": r["factor_index"],
                                "as_of": out_date,
                                "fit_id": fit["fit_id"],
                                "beta": beta,
                                "t_stat": t_stat,
                                "sig": significance,
                                "n_observations": r["n_observations"],
                                "r_squared": r["r_squared"],
                            })
                        upserted += 1
                conn.commit()
    result = {
        "processed": processed,
        "upserted": upserted,
        "as_of": out_date.isoformat() if out_date else None,
        "fit_id": str(fit["fit_id"]) if fit else None,
        "k_factors": fit["k_factors"] if fit else None,
    }
    if limit is not None:
        result["mv_refreshed"] = False
        result["mv_refresh_reason"] = "limited_run"
        return result
    try:
        _refresh_latest_mv(dsn)
        result["mv_refreshed"] = True
    except Exception as exc:  # noqa: BLE001
        result["mv_refreshed"] = False
        result["mv_refresh_error"] = str(exc)
    return result
