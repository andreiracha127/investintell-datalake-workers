"""Recurring NAV momentum owner for ``fund_risk_metrics``."""

from __future__ import annotations

import datetime as _dt
from typing import Any

import numpy as np

from src.db import LOCK_MOMENTUM_METRICS, advisory_lock, connect

MOMENTUM_COLUMNS = (
    "dtw_drift_score",
    "rsi_14",
    "bb_position",
    "nav_momentum_score",
    "flow_momentum_score",
    "blended_momentum_score",
)


def _clip(value: float | None, lo: float, hi: float) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    return float(max(lo, min(hi, value)))


def _score_between(value: float | None, lo: float, hi: float) -> float | None:
    value = _clip(value, lo, hi)
    if value is None or hi == lo:
        return None
    return round((value - lo) / (hi - lo) * 100.0, 6)


def rsi_14(nav: np.ndarray) -> float | None:
    """Classic 14-period RSI over the latest NAV observations."""

    if len(nav) < 15:
        return None
    deltas = np.diff(nav[-15:])
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = float(np.mean(gains))
    avg_loss = float(np.mean(losses))
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 6)


def bollinger_position(nav: np.ndarray, window: int = 20) -> float | None:
    """Latest NAV position inside the 20-day Bollinger band.

    0 is at the lower band, 0.5 at the moving average, and 1 at the upper band.
    Values outside 0..1 are preserved to flag breakouts.
    """

    if len(nav) < window:
        return None
    frame = nav[-window:]
    mean = float(np.mean(frame))
    std = float(np.std(frame, ddof=1))
    if std == 0 or not np.isfinite(std):
        return 0.5
    lower = mean - 2.0 * std
    upper = mean + 2.0 * std
    return round((float(nav[-1]) - lower) / (upper - lower), 6)


def dtw_drift_score(nav: np.ndarray, window: int = 63) -> float | None:
    """Trend-path stability score, 0..100.

    This is a cheap deterministic proxy for dynamic-time-warping drift: compare
    the latest log-NAV path to the straight line connecting its endpoints and
    penalize path residuals relative to path amplitude.
    """

    if len(nav) < 20:
        return None
    frame = nav[-min(window, len(nav)):]
    if np.any(frame <= 0):
        return None
    y = np.log(frame / frame[0])
    x = np.linspace(0.0, float(y[-1]), len(y))
    residual = float(np.sqrt(np.mean((y - x) ** 2)))
    amplitude = max(float(np.std(y, ddof=1)), abs(float(y[-1])), 1e-6)
    score = 100.0 * (1.0 - min(residual / amplitude, 1.0))
    return round(score, 6)


def compute_nav_momentum(nav_values: list[float]) -> dict[str, float | None]:
    nav = np.asarray(nav_values, dtype=float)
    nav = nav[np.isfinite(nav)]
    if len(nav) < 20:
        return {c: None for c in MOMENTUM_COLUMNS}

    rsi = rsi_14(nav)
    bb = bollinger_position(nav)
    drift = dtw_drift_score(nav)
    lookback = min(63, len(nav) - 1)
    ret = float(nav[-1] / nav[-1 - lookback] - 1.0) if nav[-1 - lookback] else None
    ret_score = _score_between(ret, -0.20, 0.20)
    bb_score = _score_between(bb, 0.0, 1.0)

    weighted = [
        (ret_score, 0.35),
        (rsi, 0.25),
        (bb_score, 0.20),
        (drift, 0.20),
    ]
    num = sum(score * weight for score, weight in weighted if score is not None)
    den = sum(weight for score, weight in weighted if score is not None)
    nav_score = round(num / den, 6) if den else None
    return {
        "dtw_drift_score": drift,
        "rsi_14": rsi,
        "bb_position": bb,
        "nav_momentum_score": nav_score,
        "flow_momentum_score": None,
        "blended_momentum_score": nav_score,
    }


def _resolve_calc_date(conn, calc_date: str | None) -> _dt.date:
    if calc_date:
        return _dt.date.fromisoformat(calc_date)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(calc_date) FROM fund_risk_metrics "
            "WHERE organization_id IS NULL"
        )
        cdate = cur.fetchone()[0]
    if cdate is None:
        raise RuntimeError("fund_risk_metrics has no global rows")
    return cdate


def _target_instruments(conn, calc_date: _dt.date, limit: int | None) -> list[Any]:
    sql = """
        SELECT instrument_id
        FROM fund_risk_metrics
        WHERE calc_date = %s AND organization_id IS NULL
        ORDER BY instrument_id
    """
    params: list[Any] = [calc_date]
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [r[0] for r in cur.fetchall()]


def _fetch_nav(conn, instrument_id: Any, calc_date: _dt.date) -> list[float]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT nav FROM (
                SELECT nav_date, nav
                FROM nav_timeseries
                WHERE instrument_id = %s
                  AND nav_date <= %s
                  AND nav IS NOT NULL
                ORDER BY nav_date DESC
                LIMIT 260
            ) s
            ORDER BY nav_date
            """,
            (instrument_id, calc_date),
        )
        return [float(r[0]) for r in cur.fetchall()]


def _upsert(conn, calc_date: _dt.date, rows: list[tuple]) -> int:
    if not rows:
        return 0
    cols = ", ".join(MOMENTUM_COLUMNS)
    placeholders = ", ".join(["%s"] * (3 + len(MOMENTUM_COLUMNS)))
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in MOMENTUM_COLUMNS)
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO fund_risk_metrics
                (instrument_id, calc_date, organization_id, {cols})
            VALUES ({placeholders})
            ON CONFLICT (instrument_id, calc_date, organization_id)
            DO UPDATE SET {update_clause}
            """,
            rows,
        )
    return len(rows)


def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
) -> dict:
    """Update momentum columns for one risk calc date."""

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_MOMENTUM_METRICS) as got:
            if not got:
                return {"processed": 0, "upserted": 0, "skipped": "lock_busy"}
            cdate = _resolve_calc_date(conn, calc_date)
            targets = _target_instruments(conn, cdate, limit)
            rows = []
            for instrument_id in targets:
                metrics = compute_nav_momentum(_fetch_nav(conn, instrument_id, cdate))
                if metrics["nav_momentum_score"] is None:
                    continue
                rows.append(
                    (
                        instrument_id,
                        cdate,
                        None,
                        *(metrics[c] for c in MOMENTUM_COLUMNS),
                    )
                )
            upserted = _upsert(conn, cdate, rows)
            conn.commit()
    return {"processed": len(targets), "upserted": upserted, "calc_date": cdate}

