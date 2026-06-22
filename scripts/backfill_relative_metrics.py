"""backfill_relative_metrics.py — recompute the 8 benchmark-relative metrics over
the FULL history of fund_risk_metrics against the proxy-ETF benchmark.

Why surgical: the daily worker only writes the latest calc_date; these metrics
were never historically populated (and the old benchmark was wrong). This job
recomputes ONLY the 8 relative columns and UPDATEs the existing rows — it never
touches CVaR/returns/GARCH already validated in production.

Columns written: beta_1y, alpha_1y, tracking_error_1y, information_ratio_1y,
upside_capture_1y, downside_capture_1y, equity_correlation_252d, crisis_alpha_score.

Reuses the worker's pure math (regression_metrics / equity_correlation /
crisis_alpha) and benchmark resolution (_fetch_fund_benchmarks) so values match
the daily run exactly. Benchmark return series come from eod_prices.adj_close.

Efficiency: ETF return series loaded ONCE; NAV bulk-read per shard; per (fund,
calc_date) we slice the in-memory series (no per-date DB round-trips).

Usage:
  DATABASE_URL=postgresql://...cloud...  python scripts/backfill_relative_metrics.py \
      --workers 12 --out artifacts/relative_metrics_backfill.csv [--limit-funds N] [--seed [--dry-run]]
"""
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as _dt
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.workers import risk_metrics as rm  # noqa: E402

REL_COLS = [
    "beta_1y", "alpha_1y", "tracking_error_1y", "information_ratio_1y",
    "upside_capture_1y", "downside_capture_1y", "equity_correlation_252d",
    "crisis_alpha_score",
]
OUT_COLS = ["instrument_id", "calc_date", *REL_COLS]

# Lookback windows (calendar days) for slicing the in-memory series per calc_date.
_REG_WINDOW_DAYS = 600          # 252 sessions + slack (regression / capture / corr)
_CRISIS_WINDOW_DAYS = 11 * 366  # long multi-crisis window for crisis_alpha


def _dsn() -> str:
    raw = os.environ.get("DATABASE_URL", "")
    if not raw:
        sys.exit("DATABASE_URL not set")
    return raw.replace("postgresql+asyncpg://", "postgresql://")


# ── shared, read-once inputs ──────────────────────────────────────────────────
def _load_bench_series(conn) -> dict[str, list[tuple[_dt.date, float]]]:
    """ticker → [(date, simple ret)] over FULL history from eod_prices."""
    fund_bm = rm._fetch_fund_benchmarks(conn)
    tickers = sorted(set(fund_bm.values()) | {rm.EQUITY_BENCHMARK_KEY})
    with conn.cursor() as cur:
        cur.execute(
            """SELECT upper(ticker), date, adj_close FROM eod_prices
               WHERE upper(ticker) = ANY(%s) AND adj_close IS NOT NULL
               ORDER BY upper(ticker), date""",
            (tickers,),
        )
        rows = cur.fetchall()
    out: dict[str, list[tuple[_dt.date, float]]] = {}
    prev_tk = None
    prev_px = None
    for tk, d, px in rows:
        px = float(px)
        if tk != prev_tk:
            prev_tk, prev_px = tk, px
            out.setdefault(tk, [])
            continue
        if prev_px and prev_px > 0:
            out[tk].append((d, px / prev_px - 1.0))
        prev_px = px
    return out


def _load_rf_series(conn) -> list[tuple[_dt.date, float]]:
    """DFF as decimal, ascending — for as-of lookup per calc_date."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT obs_date, value FROM macro_data WHERE series_id='DFF' ORDER BY obs_date"
        )
        return [(d, float(v) / 100.0) for d, v in cur.fetchall() if v is not None]


def _load_targets(conn, limit_funds: int | None) -> list[tuple[str, list[_dt.date]]]:
    """[(instrument_id, [calc_dates ascending])] for base rows."""
    sql = """
        SELECT instrument_id, array_agg(calc_date ORDER BY calc_date) AS dates
        FROM fund_risk_metrics WHERE organization_id IS NULL
        GROUP BY instrument_id
    """
    if limit_funds:
        sql += f" LIMIT {int(limit_funds)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return [(str(iid), dates) for iid, dates in cur.fetchall()]


# ── slicing helpers (module-level, picklable) ─────────────────────────────────
def _rf_asof(rf_dates: list[_dt.date], rf_series: list[tuple[_dt.date, float]], d: _dt.date) -> float:
    i = bisect.bisect_right(rf_dates, d)
    return rf_series[i - 1][1] if i > 0 else rm.RISK_FREE_FALLBACK


def _slice(dates: list[_dt.date], series: list[tuple[_dt.date, float]],
           lo: _dt.date, hi: _dt.date) -> list[tuple[_dt.date, float]]:
    """series points with lo < date <= hi (dates is the parallel date array)."""
    a = bisect.bisect_right(dates, lo)
    b = bisect.bisect_right(dates, hi)
    return series[a:b]


def _process_shard(
    dsn: str,
    fund_ids: list[str],
    fund_dates: dict[str, list[str]],
    fund_bm: dict[str, str],
    bench_series: dict[str, list[tuple[_dt.date, float]]],
    rf_series: list[tuple[_dt.date, float]],
) -> list[list]:
    """Compute relative metrics for a shard of funds; return CSV-ready rows."""
    rf_dates = [d for d, _ in rf_series]
    # Pre-extract date arrays of each bench series for fast slicing.
    bench_dates = {tk: [d for d, _ in s] for tk, s in bench_series.items()}
    eq_key = rm.EQUITY_BENCHMARK_KEY
    eq_series = bench_series.get(eq_key, [])
    eq_dates = bench_dates.get(eq_key, [])

    out_rows: list[list] = []
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT instrument_id, nav_date, nav FROM nav_timeseries
               WHERE instrument_id = ANY(%s) AND nav IS NOT NULL
               ORDER BY instrument_id, nav_date""",
            (fund_ids,),
        )
        nav_by_fund: dict[str, list[tuple[_dt.date, float]]] = {}
        for iid, d, nav in cur.fetchall():
            nav_by_fund.setdefault(str(iid), []).append((d, float(nav)))

    for iid in fund_ids:
        nav_rows = nav_by_fund.get(iid)
        if not nav_rows or len(nav_rows) < 22:
            continue
        fund_ret = rm.dated_simple_returns(nav_rows)
        if not fund_ret:
            continue
        fund_dates_arr = [d for d, _ in fund_ret]
        tk = fund_bm.get(iid)
        bench = bench_series.get(tk) if tk else None
        bench_d = bench_dates.get(tk) if tk else None

        for cd_iso in fund_dates[iid]:
            cd = _dt.date.fromisoformat(cd_iso)
            rf = _rf_asof(rf_dates, rf_series, cd)
            lo_reg = cd - _dt.timedelta(days=_REG_WINDOW_DAYS)
            lo_cri = cd - _dt.timedelta(days=_CRISIS_WINDOW_DAYS)
            f_reg = _slice(fund_dates_arr, fund_ret, lo_reg, cd)
            row_vals: dict[str, float | None] = {}
            if bench is not None and len(f_reg) >= rm.TRADING_DAYS:
                b_reg = _slice(bench_d, bench, lo_reg, cd)
                row_vals.update(rm.regression_metrics(f_reg, b_reg, rf))
            if eq_series:
                eq_reg = _slice(eq_dates, eq_series, lo_reg, cd)
                ec = rm.equity_correlation(f_reg, eq_reg)
                if ec is not None:
                    row_vals["equity_correlation_252d"] = ec
                f_cri = _slice(fund_dates_arr, fund_ret, lo_cri, cd)
                eq_cri = _slice(eq_dates, eq_series, lo_cri, cd)
                ca = rm.crisis_alpha(f_cri, eq_cri)
                if ca is not None:
                    row_vals["crisis_alpha_score"] = ca
            if any(row_vals.get(c) is not None for c in REL_COLS):
                out_rows.append([iid, cd_iso, *[row_vals.get(c) for c in REL_COLS]])
    return out_rows


# ── seed ──────────────────────────────────────────────────────────────────────
_STAGE_COLS = (
    "instrument_id uuid, calc_date date,"
    "beta_1y numeric, alpha_1y numeric, tracking_error_1y numeric,"
    "information_ratio_1y numeric, upside_capture_1y numeric, downside_capture_1y numeric,"
    "equity_correlation_252d numeric, crisis_alpha_score numeric"
)
_UPDATE_SQL = """
UPDATE fund_risk_metrics m SET
    beta_1y = s.beta_1y, alpha_1y = s.alpha_1y,
    tracking_error_1y = s.tracking_error_1y, information_ratio_1y = s.information_ratio_1y,
    upside_capture_1y = s.upside_capture_1y, downside_capture_1y = s.downside_capture_1y,
    equity_correlation_252d = s.equity_correlation_252d, crisis_alpha_score = s.crisis_alpha_score
FROM _rel_stage s
WHERE m.instrument_id = s.instrument_id AND m.calc_date = s.calc_date
  AND m.organization_id IS NULL;
"""
_COVERAGE_SQL = """
SELECT count(*) FILTER (WHERE beta_1y IS NOT NULL),
       count(*) FILTER (WHERE equity_correlation_252d IS NOT NULL),
       count(*) FILTER (WHERE crisis_alpha_score IS NOT NULL)
FROM fund_risk_metrics WHERE organization_id IS NULL
"""


def seed(dsn: str, csv_path: str, dry_run: bool) -> None:
    cols = ", ".join(OUT_COLS)
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_COVERAGE_SQL)
        b0, e0, c0 = cur.fetchone()
        print(f"[before] beta={b0} eqcorr={e0} crisis={c0}", flush=True)
        cur.execute(f"CREATE TEMP TABLE _rel_stage ({_STAGE_COLS}) ON COMMIT DROP")
        n = 0
        with open(csv_path, encoding="utf-8") as fh:
            next(fh)
            with cur.copy(f"COPY _rel_stage ({cols}) FROM STDIN WITH (FORMAT csv)") as cp:
                for line in fh:
                    cp.write(line)
                    n += 1
        print(f"[stage] copied {n} rows", flush=True)
        cur.execute(_UPDATE_SQL)
        print(f"[update] {cur.rowcount} rows", flush=True)
        cur.execute(_COVERAGE_SQL)
        b1, e1, c1 = cur.fetchone()
        print(f"[after] beta={b1} (+{b1-b0}) eqcorr={e1} (+{e1-e0}) crisis={c1} (+{c1-c0})", flush=True)
        if dry_run:
            conn.rollback()
            print("[dry-run] rolled back", flush=True)
        else:
            conn.commit()
            print("[commit] done", flush=True)


def _shard(items: list, n: int) -> list[list]:
    return [items[i::n] for i in range(n)] if items else []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="artifacts/relative_metrics_backfill.csv")
    ap.add_argument("--workers", type=int, default=min(12, (os.cpu_count() or 4)))
    ap.add_argument("--limit-funds", type=int, default=None)
    ap.add_argument("--seed", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    dsn = _dsn()

    if not args.seed or not os.path.exists(args.out) or args.limit_funds:
        print("[load] shared inputs from cloud ...", flush=True)
        with psycopg.connect(dsn) as conn:
            bench_series = _load_bench_series(conn)
            rf_series = _load_rf_series(conn)
            fund_bm = rm._fetch_fund_benchmarks(conn)
            targets = _load_targets(conn, args.limit_funds)
        print(f"[load] {len(bench_series)} bench series, {len(fund_bm)} fund→ticker, "
              f"{len(targets)} target funds, {len(rf_series)} rf points", flush=True)

        fund_ids = [iid for iid, _ in targets]
        fund_dates = {iid: [d.isoformat() for d in dates] for iid, dates in targets}
        n_workers = max(1, min(args.workers, len(fund_ids) or 1))
        shards = _shard(fund_ids, n_workers)
        all_rows: list[list] = []
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futs = [
                pool.submit(_process_shard, dsn, sh,
                            {i: fund_dates[i] for i in sh},
                            {i: fund_bm[i] for i in sh if i in fund_bm},
                            bench_series, rf_series)
                for sh in shards if sh
            ]
            for fut in as_completed(futs):
                all_rows.extend(fut.result())
        print(f"[compute] {len(all_rows)} (fund,date) rows with ≥1 metric", flush=True)

        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(OUT_COLS)
            w.writerows(all_rows)
        print(f"[out] wrote {len(all_rows)} rows → {args.out}", flush=True)

    if args.seed:
        seed(dsn, args.out, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
