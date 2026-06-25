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


def _significance(t_stat: float | None) -> str | None:
    if t_stat is None or math.isnan(t_stat):
        return None
    level = abs(t_stat)
    for threshold, mark in _SIG:
        if level >= threshold:
            return mark
    return None


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
            "beta": float(beta[idx]),
            "t_stat": t,
            "significance": _significance(t),
        })
    return out


_UPSERT = """
INSERT INTO fund_factor_exposures
    (instrument_id, factor, as_of, beta, t_stat, significance, organization_id)
VALUES (%(iid)s, %(factor)s, %(as_of)s, %(beta)s, %(t_stat)s, %(sig)s, NULL)
ON CONFLICT (instrument_id, factor, as_of, organization_id) DO UPDATE SET
    beta = EXCLUDED.beta, t_stat = EXCLUDED.t_stat,
    significance = EXCLUDED.significance, computed_at = now()
"""


def _refresh_latest_mv(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_factor_exposures_latest_mv"
            )


def _latest_factor_matrix(conn) -> tuple[_dt.date | None, list[_dt.date], np.ndarray]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT fit_date, factor_returns FROM factor_model_fits "
            "WHERE engine = 'ipca' ORDER BY fit_date DESC, created_at DESC LIMIT 1"
        )
        row = cur.fetchone()
    if row is None or not isinstance(row[1], dict):
        return None, [], np.empty((0, 0))
    fit_date, payload = row
    dates = [_dt.date.fromisoformat(d[:10]) for d in payload.get("dates", [])]
    values = payload.get("values", [])
    if not dates or not values:
        return fit_date, [], np.empty((0, 0))
    cols = [np.asarray(v, dtype=float) for v in values if len(v) == len(dates)]
    matrix = np.column_stack(cols) if cols else np.empty((len(dates), 0))
    return fit_date, dates, matrix


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
        if by_month[prev]:
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


def run(dsn: str, *, as_of: str | None = None, limit: int | None = None) -> dict:
    processed = upserted = 0
    fit_date: _dt.date | None = None
    out_date: _dt.date | None = None
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FUND_FACTORS) as got:
            if not got:
                return {"processed": 0, "upserted": 0, "skipped": "lock_busy"}
            fit_date, fdates, fmatrix = _latest_factor_matrix(conn)
            out_date = _dt.date.fromisoformat(as_of) if as_of else (fit_date or _dt.date.today())
            if fdates and fmatrix.size:
                for iid in _fund_ids(conn, limit):
                    y = _fund_monthly_returns(conn, iid, fdates)
                    mask = ~np.isnan(y)
                    if mask.sum() < max(10, fmatrix.shape[1] + 2):
                        continue
                    processed += 1
                    rows = ols_factor_exposures(y[mask], fmatrix[mask])
                    for r in rows:
                        with conn.cursor() as cur:
                            cur.execute(_UPSERT, {
                                "iid": iid, "factor": r["factor"], "as_of": out_date,
                                "beta": r["beta"], "t_stat": r["t_stat"], "sig": r["significance"],
                            })
                        upserted += 1
                conn.commit()
    result = {"processed": processed, "upserted": upserted,
              "as_of": (out_date.isoformat() if (fit_date or as_of) and out_date else None)}
    try:
        _refresh_latest_mv(dsn)
        result["mv_refreshed"] = True
    except Exception as exc:  # noqa: BLE001
        result["mv_refreshed"] = False
        result["mv_refresh_error"] = str(exc)
    return result
