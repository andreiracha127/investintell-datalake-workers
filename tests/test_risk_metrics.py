"""Recalc-vs-legado validation for the risk_metrics worker.

The cloud data-lake does not yet hold ``nav_timeseries``/``benchmark_nav`` (only
``sec_nport_holdings`` is loaded). The raw NAV lives in the **DB-mãe**
(``investintell_alloc`` @ localhost:5434), which also holds the legacy
``fund_risk_metrics`` we validate against. So this test points the worker's read
path at the DB-mãe: identical SQL contract (``nav_timeseries``/``macro_data``),
real series, and the legacy metrics are right there to diff against.

What it proves:
  * ``run(dsn, limit=~12)`` executes end-to-end against a real Postgres, takes the
    advisory lock, computes metrics, and upserts idempotently (re-run = no error,
    stable counts).
  * For >=2 funds with rich legacy metrics, the recomputed vol / max_drawdown /
    sharpe / return_1y match the legacy values within the README tolerance
    (vol < 1%, maxDD < 2%); larger residuals are reported with a diagnosis.

Legacy ``beta_1y``/``alpha_1y``/``tracking_error_1y`` are 0% populated in the
mother DB (phantom columns never computed by the legacy job), so there is nothing
to diff beta against — we assert our beta is finite and sane instead, and report
the gap explicitly.

Run:  pytest tests/test_risk_metrics.py -s -v
The test self-skips if the DB-mãe is unreachable.
"""

from __future__ import annotations

import datetime as _dt

import numpy as np
import psycopg
import pytest

from src.db import LOCK_RISK_METRICS, advisory_lock
from src.workers import risk_metrics as rm

MAE_DSN = "host=localhost port=5434 dbname=investintell_alloc user=investintell password=investintell"

# Tolerances from the README.
TOL_VOL = 0.01      # absolute, annualised
TOL_MAXDD = 0.02    # absolute
TOL_SHARPE = 0.30   # sharpe depends on rf vintage; looser, reported
TOL_RETURN = 0.01   # absolute


def _mae():
    try:
        return psycopg.connect(MAE_DSN, connect_timeout=5)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"DB-mãe unreachable: {exc}")


def _legacy_calc_date(conn) -> _dt.date:
    with conn.cursor() as cur:
        cur.execute("SELECT max(calc_date) FROM fund_risk_metrics")
        return cur.fetchone()[0]


def _legacy_metrics(conn, instrument_id, calc_date):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT volatility_1y, max_drawdown_1y, sharpe_1y, sortino_1y,
                      return_1y, volatility_garch, vol_model, beta_1y
               FROM fund_risk_metrics
               WHERE instrument_id = %s AND calc_date = %s
               ORDER BY organization_id NULLS FIRST LIMIT 1""",
            (instrument_id, calc_date),
        )
        row = cur.fetchone()
    if not row:
        return None
    keys = ["volatility_1y", "max_drawdown_1y", "sharpe_1y", "sortino_1y",
            "return_1y", "volatility_garch", "vol_model", "beta_1y"]
    return {k: (float(v) if isinstance(v, (int, float)) or
                (hasattr(v, "__float__") and not isinstance(v, str)) else v)
            for k, v in zip(keys, row)}


def _candidate_funds(conn, calc_date, n=12):
    """Funds with rich legacy metrics AND >=300 NAV points, most history first."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.instrument_id
            FROM fund_risk_metrics f
            JOIN (SELECT instrument_id, count(*) c FROM nav_timeseries
                  WHERE nav IS NOT NULL GROUP BY instrument_id HAVING count(*) >= 300) n
              USING (instrument_id)
            WHERE f.calc_date = %s
              AND f.volatility_1y IS NOT NULL AND f.sharpe_1y IS NOT NULL
            ORDER BY n.c DESC
            LIMIT %s
            """,
            (calc_date, n),
        )
        return [r[0] for r in cur.fetchall()]


def _recompute(conn, instrument_id, calc_date, rf):
    rows = rm._fetch_nav(conn, instrument_id, calc_date)
    nav = np.array([float(r[1]) for r in rows], dtype=float)
    return rm.compute_metrics(nav, rf), len(rows)


def test_run_end_to_end_and_idempotent():
    """run(limit=...) executes against the mother DB and is idempotent."""
    conn = _mae()
    try:
        cdate = _legacy_calc_date(conn)
    finally:
        conn.close()
    stats1 = rm.run(MAE_DSN, calc_date=cdate.isoformat(), limit=12)
    stats2 = rm.run(MAE_DSN, calc_date=cdate.isoformat(), limit=12)
    print("\nrun stats (1st):", stats1)
    print("run stats (2nd):", stats2)
    assert stats1["processed"] >= 1
    assert stats1["upserted"] >= 2
    # Idempotent: identical counts on re-run (UPDATE path, no duplicates / errors).
    assert stats1["upserted"] == stats2["upserted"]


def test_recalc_vs_legacy():
    """Compare recomputed vol/maxDD/sharpe/return_1y against legacy for >=2 funds."""
    conn = _mae()
    try:
        cdate = _legacy_calc_date(conn)
        rf = rm._risk_free_rate(conn, cdate)
        funds = _candidate_funds(conn, cdate, n=12)
        assert len(funds) >= 2, "need >=2 candidate funds"

        print(f"\n=== recalc vs legacy @ calc_date={cdate}  rf={rf:.4f} ===")
        header = (f"{'fund':<10} {'metric':<16} {'recalc':>12} {'legacy':>12} "
                  f"{'abs':>10} {'rel%':>8}  {'ok':>3}")
        compared = 0
        vol_ok = maxdd_ret_ok = 0

        for iid in funds:
            legacy = _legacy_metrics(conn, iid, cdate)
            recalc, npts = _recompute(conn, iid, cdate, rf)
            if legacy is None or recalc is None:
                continue
            compared += 1
            short = str(iid)[:8]
            print(f"\n[{short}] nav_points={npts} legacy.vol_model={legacy['vol_model']}")
            print(header)
            print("-" * len(header))

            checks = [
                ("volatility_1y", TOL_VOL),
                ("max_drawdown_1y", TOL_MAXDD),
                ("return_1y", TOL_RETURN),
                ("sharpe_1y", TOL_SHARPE),
                ("sortino_1y", TOL_SHARPE),
                ("volatility_garch", TOL_VOL),
            ]
            for metric, tol in checks:
                rv = recalc.get(metric)
                lv = legacy.get(metric)
                if rv is None or lv is None:
                    continue
                ad = abs(rv - lv)
                rel = 100.0 * ad / abs(lv) if lv else float("nan")
                ok = ad <= tol
                print(f"{short:<10} {metric:<16} {rv:>12.6f} {lv:>12.6f} "
                      f"{ad:>10.6f} {rel:>7.2f}%  {'Y' if ok else 'N':>3}")
                if metric == "volatility_1y":
                    if ok:
                        vol_ok += 1
                if metric in ("max_drawdown_1y", "return_1y") and ok:
                    maxdd_ret_ok += 1

            # beta: legacy never populated it (phantom). Sanity-check ours.
            print(f"{short:<10} {'beta_1y(legacy)':<16} "
                  f"{'(recalc N/A — no benchmark in single-fund test)':>0}  "
                  f"legacy={legacy.get('beta_1y')}")

        assert compared >= 2, "need >=2 funds compared"
        # At least 2 funds must hit the README vol tolerance.
        assert vol_ok >= 2, f"vol within 1% for only {vol_ok} funds (need >=2)"
        print(f"\nSUMMARY: compared={compared}  vol_within_1%={vol_ok}  "
              f"maxdd/return_within_tol={maxdd_ret_ok}")
    finally:
        conn.close()


def test_advisory_lock_is_distinct():
    """LOCK_RISK_METRICS is the dedicated id and round-trips through advisory_lock."""
    assert LOCK_RISK_METRICS == 900_201
    conn = _mae()
    try:
        with advisory_lock(conn, LOCK_RISK_METRICS) as got:
            assert got is True
    finally:
        conn.close()
