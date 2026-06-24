"""Recurring NAV, daily-flow proxy, and N-PORT flow confirmation momentum."""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
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
    "flow_momentum_as_of",
    "flow_momentum_observation_count",
    "nport_flow_momentum_score",
    "nport_flow_as_of",
    "nport_flow_staleness_days",
    "nport_flow_observation_count",
)


@dataclass(frozen=True)
class NavAumPoint:
    nav_date: _dt.date
    nav: float
    aum_usd: float | None


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


def compute_nav_momentum(nav_values: list[float]) -> dict[str, Any]:
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
        "flow_momentum_as_of": None,
        "flow_momentum_observation_count": None,
        "nport_flow_momentum_score": None,
        "nport_flow_as_of": None,
        "nport_flow_staleness_days": None,
        "nport_flow_observation_count": None,
    }


def _score_flow_pct_slope(
    flow_pct_assets: list[float],
    *,
    max_points: int,
    min_points: int,
    slope_scale: float,
) -> float | None:
    flows = np.asarray(flow_pct_assets, dtype=float)
    flows = flows[np.isfinite(flows)]
    if len(flows) < min_points:
        return None
    tail = flows[-min(max_points, len(flows)):]
    cumulative = np.cumsum(tail)
    if len(cumulative) < min_points:
        return None
    slope = float(np.polyfit(np.arange(len(cumulative)), cumulative, 1)[0])
    score = 50.0 + 50.0 * float(np.tanh(slope / slope_scale))
    return round(max(0.0, min(100.0, score)), 6)


def compute_daily_flow_pct(nav_aum_points: list[NavAumPoint]) -> list[float]:
    """Estimate daily external flows from AUM changes net of NAV return.

    ``external_flow_t = AUM_t - AUM_{t-1} * NAV_t / NAV_{t-1}``

    This keeps the optimizer on a fresh daily signal while separating market/NAV
    performance from subscriptions and redemptions as far as the NAV+AUM series
    allows. Extreme one-day values are clipped to damp splits, mergers, and
    stale AUM jumps.
    """

    flows: list[float] = []
    points = [p for p in nav_aum_points if p.aum_usd is not None and p.nav > 0 and p.aum_usd > 0]
    for prev, cur in zip(points, points[1:]):
        if prev.nav <= 0 or prev.aum_usd is None or cur.aum_usd is None or prev.aum_usd <= 0:
            continue
        expected_aum = prev.aum_usd * (cur.nav / prev.nav)
        external_flow = cur.aum_usd - expected_aum
        flows.append(float(max(-0.25, min(0.25, external_flow / prev.aum_usd))))
    return flows


def compute_daily_flow_momentum(flow_pct_assets: list[float]) -> float | None:
    """Score fresh daily flow proxy, normalized by prior-day assets."""

    return _score_flow_pct_slope(
        flow_pct_assets,
        max_points=63,
        min_points=10,
        slope_scale=0.0025,
    )


def compute_nport_flow_momentum(flow_pct_assets: list[float]) -> float | None:
    """Score reported N-PORT net flows, normalized by assets, on 0..100.

    The input is monthly ``net_flow / net_assets`` from reported N-PORT sales,
    reinvestments, and redemptions. It is a slower confirmation/reviewer signal,
    not the optimizer's fresh flow input.
    """

    return _score_flow_pct_slope(
        flow_pct_assets,
        max_points=12,
        min_points=3,
        slope_scale=0.02,
    )


def blend_momentum_scores(
    nav_score: float | None,
    flow_score: float | None,
) -> float | None:
    if nav_score is not None and flow_score is not None:
        return round(0.5 * nav_score + 0.5 * flow_score, 6)
    if flow_score is not None:
        return flow_score
    return nav_score


def compute_momentum(
    nav_aum_points: list[NavAumPoint],
    nport_flow_pct_assets: list[float],
    *,
    nport_as_of: _dt.date | None = None,
    calc_date: _dt.date | None = None,
) -> dict[str, Any]:
    nav_values = [p.nav for p in nav_aum_points]
    metrics = compute_nav_momentum(nav_values)
    daily_flow_pct = compute_daily_flow_pct(nav_aum_points)
    flow_score = compute_daily_flow_momentum(daily_flow_pct)
    nport_score = compute_nport_flow_momentum(nport_flow_pct_assets)

    metrics["flow_momentum_score"] = flow_score
    metrics["blended_momentum_score"] = blend_momentum_scores(
        metrics["nav_momentum_score"],
        flow_score,
    )
    flow_as_of = next((p.nav_date for p in reversed(nav_aum_points) if p.aum_usd is not None), None)
    metrics["flow_momentum_as_of"] = flow_as_of
    metrics["flow_momentum_observation_count"] = len(daily_flow_pct)
    metrics["nport_flow_momentum_score"] = nport_score
    metrics["nport_flow_as_of"] = nport_as_of
    metrics["nport_flow_staleness_days"] = (
        (calc_date - nport_as_of).days
        if calc_date is not None and nport_as_of is not None
        else None
    )
    metrics["nport_flow_observation_count"] = len(nport_flow_pct_assets)
    return metrics


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


def _target_instruments(conn, calc_date: _dt.date, limit: int | None) -> list[tuple[Any, str | None]]:
    sql = """
        SELECT frm.instrument_id, f.series_id
        FROM fund_risk_metrics frm
        LEFT JOIN funds_v f ON f.instrument_id = frm.instrument_id
        WHERE frm.calc_date = %s AND frm.organization_id IS NULL
        ORDER BY frm.instrument_id
    """
    params: list[Any] = [calc_date]
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [(r[0], r[1]) for r in cur.fetchall()]


def _fetch_nav_aum(conn, instrument_id: Any, calc_date: _dt.date) -> list[NavAumPoint]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT nav_date, nav, aum_usd FROM (
                SELECT nav_date, nav, aum_usd
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
        return [
            NavAumPoint(r[0], float(r[1]), float(r[2]) if r[2] is not None else None)
            for r in cur.fetchall()
        ]


def _fetch_nport_flow_pct_assets(
    conn,
    series_id: str | None,
    calc_date: _dt.date,
) -> tuple[list[float], _dt.date | None]:
    if not series_id:
        return [], None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT flow_month_end, net_flow_pct_assets
            FROM (
                SELECT DISTINCT ON (flow_month_end)
                    flow_month_end,
                    net_flow_pct_assets
                FROM sec_nport_fund_monthly_flows
                WHERE series_id = %s
                  AND flow_month_end <= %s
                  AND net_flow_pct_assets IS NOT NULL
                ORDER BY flow_month_end, filing_date DESC NULLS LAST, accession_number DESC
            ) deduped
            ORDER BY flow_month_end DESC
            LIMIT 24
            """,
            (series_id, calc_date),
        )
        rows = cur.fetchall()
    values = [float(r[1]) for r in rows]
    as_of = rows[0][0] if rows else None
    values.reverse()
    return values, as_of


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


def _refresh_read_models(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY fund_risk_latest_mv")
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY funds_list_mv")


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
            for instrument_id, series_id in targets:
                nport_flows, nport_as_of = _fetch_nport_flow_pct_assets(conn, series_id, cdate)
                metrics = compute_momentum(
                    _fetch_nav_aum(conn, instrument_id, cdate),
                    nport_flows,
                    nport_as_of=nport_as_of,
                    calc_date=cdate,
                )
                if metrics["blended_momentum_score"] is None:
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
    _refresh_read_models(dsn)
    return {"processed": len(targets), "upserted": upserted, "calc_date": cdate}
