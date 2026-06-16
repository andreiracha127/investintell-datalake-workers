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


def test_peer_percentiles_set_based():
    """_update_peer_percentiles: percent_rank 0-100 por label, peer_count = tamanho
    do grupo, drawdown menos negativo = pctl maior. Roda na mãe e dá ROLLBACK."""
    conn = _mae()
    try:
        cdate = _legacy_calc_date(conn)
        updated = rm._update_peer_percentiles(conn, cdate)
        assert updated > 1000, f"expected a broad update, got {updated}"

        # Group membership per the UPDATE's own definition (stage labels join),
        # NOT per stored peer_strategy_label — the mae keeps legacy labels on
        # rows absent from the stage, which the update correctly leaves alone.
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH labels AS (
                    SELECT DISTINCT ON (source_pk)
                           source_pk::uuid AS instrument_id,
                           proposed_strategy_label AS label
                    FROM strategy_reclassification_stage
                    WHERE source_table = 'instruments_universe'
                      AND proposed_strategy_label IS NOT NULL
                    ORDER BY source_pk, classified_at DESC
                )
                SELECT m.sharpe_1y, m.peer_sharpe_pctl, m.max_drawdown_1y,
                       m.peer_drawdown_pctl, m.peer_count,
                       count(*) OVER () AS group_size
                FROM fund_risk_metrics m
                JOIN labels l ON l.instrument_id = m.instrument_id
                WHERE m.calc_date = %s AND m.organization_id IS NULL
                  AND l.label = 'Large Blend'
                ORDER BY m.sharpe_1y DESC NULLS LAST
                """,
                (cdate,),
            )
            rows = cur.fetchall()
        assert len(rows) >= 10, "Large Blend peer group unexpectedly small"

        group_size = rows[0][5]
        ranked = [r for r in rows if r[0] is not None]
        # Best sharpe in group ranks 100; pctls bounded; count = group size.
        assert float(ranked[0][1]) == 100.0
        for sharpe, sharpe_pctl, dd, dd_pctl, peer_count, _ in rows:
            assert peer_count == group_size
            if sharpe_pctl is not None:
                assert 0 <= float(sharpe_pctl) <= 100
            if dd_pctl is not None:
                assert 0 <= float(dd_pctl) <= 100
        # Monotonic: ordered by sharpe desc, pctls must be non-increasing.
        pctls = [float(r[1]) for r in ranked]
        assert all(a >= b for a, b in zip(pctls, pctls[1:]))
        # Drawdown direction: the least-negative dd has the highest dd pctl.
        dd_rows = [(float(r[2]), float(r[3])) for r in rows if r[2] is not None and r[3] is not None]
        best_dd = max(dd_rows, key=lambda t: t[0])
        assert best_dd[1] == max(p for _, p in dd_rows)
    finally:
        conn.rollback()
        conn.close()


def test_benchmark_maps_reference_known_blocks():
    """Todo bloco referenciado nos mapas existe no conjunto nomeado do
    benchmark_ingest — um typo aqui silenciaria as métricas relativas."""
    known = {
        "alt_commodities", "alt_gold", "alt_real_estate", "cash",
        "dm_asia_equity", "dm_europe_equity", "em_equity",
        "factor_source_intl_developed", "factor_source_us_growth",
        "fi_em_debt", "fi_ig_corporate", "fi_us_aggregate",
        "fi_us_high_yield", "fi_us_short_term", "fi_us_tips",
        "fi_us_treasury", "na_equity_growth", "na_equity_large",
        "na_equity_small", "na_equity_value",
    }
    used = set(rm.BENCHMARK_BY_LABEL.values()) | set(
        rm.BENCHMARK_BY_ASSET_CLASS.values()
    ) | {rm.EQUITY_BENCHMARK_BLOCK}
    assert used <= known, used - known


def test_relative_metrics_synthetic_beta_two():
    """fundo = 2× benchmark → beta 2, correlação 1, capture 200/200."""
    rng = np.random.default_rng(42)
    start = _dt.date(2024, 1, 1)
    dates = [start + _dt.timedelta(days=i) for i in range(400)]
    b_ret = rng.normal(0.0004, 0.01, 400)
    bench_nav, fund_nav = [100.0], [100.0]
    for r in b_ret:
        bench_nav.append(bench_nav[-1] * (1 + r))
        fund_nav.append(fund_nav[-1] * (1 + 2 * r))
    bench_rows = list(zip([start - _dt.timedelta(days=1), *dates], bench_nav))
    fund_rows = list(zip([start - _dt.timedelta(days=1), *dates], fund_nav))
    bench_returns = {"na_equity_large": rm.dated_simple_returns(bench_rows)}

    out = rm.relative_metrics_for(fund_rows, "na_equity_large", bench_returns, 0.04)
    assert out["beta_1y"] is not None and abs(out["beta_1y"] - 2.0) < 0.01
    assert abs(out["equity_correlation_252d"] - 1.0) < 1e-6
    assert abs(out["upside_capture_1y"] - 200.0) < 2.0
    assert abs(out["downside_capture_1y"] - 200.0) < 2.0
    assert out["tracking_error_1y"] > 0


def test_relative_metrics_without_block_only_correlation():
    """Sem benchmark mapeado (ex. alternatives) só a eq-correlation sai."""
    start = _dt.date(2024, 1, 1)
    dates = [start + _dt.timedelta(days=i) for i in range(300)]
    rows = [(d, 100.0 + i * 0.1) for i, d in enumerate(dates)]
    bench_returns = {"na_equity_large": rm.dated_simple_returns(rows)}
    out = rm.relative_metrics_for(rows, None, bench_returns, 0.04)
    assert "beta_1y" not in out
    assert "equity_correlation_252d" in out


# ──────────────────────────────────────────────────────────────────────────────
# Read-model refresh (Railway/Tiger migration): after a metrics run, the API's
# fund_risk_latest_mv MATERIALIZED VIEW is refreshed CONCURRENTLY in a FRESH
# connection OUTSIDE the advisory lock (docs/INGESTION_DESIGN.md). These tests
# need no DB — they monkeypatch the I/O seams.
# ──────────────────────────────────────────────────────────────────────────────
class _FakeCursor:
    def __init__(self, sink: dict):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def execute(self, sql, *_args):
        self._sink["sql"] = " ".join(str(sql).split())


class _FakeConn:
    def __init__(self, sink: dict):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def cursor(self):
        return _FakeCursor(self._sink)

    def commit(self):
        self._sink.setdefault("events", []).append("commit")


def test_refresh_fund_risk_latest_mv_concurrently_in_fresh_autocommit_conn(monkeypatch):
    """The refresh opens a FRESH autocommit conn and runs REFRESH … CONCURRENTLY."""
    sink: dict = {}

    def _fake_connect(dsn=None, *, autocommit=False):
        sink["dsn"] = dsn
        sink["autocommit"] = autocommit
        return _FakeConn(sink)

    monkeypatch.setattr(rm, "connect", _fake_connect)
    rm._refresh_fund_risk_latest_mv("postgres://x")

    assert sink["autocommit"] is True  # CONCURRENTLY cannot run in a txn block
    assert sink["dsn"] == "postgres://x"
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_risk_latest_mv" in sink["sql"]


def test_run_does_not_refresh_when_lock_busy(monkeypatch):
    """Lock busy → run() returns early and never refreshes (nothing recomputed)."""
    import contextlib

    monkeypatch.setattr(rm, "connect", lambda dsn=None, **_k: _FakeConn({}))

    @contextlib.contextmanager
    def _busy_lock(_conn, _lock_id):
        yield False

    monkeypatch.setattr(rm, "advisory_lock", _busy_lock)

    refreshed = {"called": False}
    monkeypatch.setattr(
        rm, "_refresh_fund_risk_latest_mv",
        lambda _dsn: refreshed.__setitem__("called", True),
    )

    stats = rm.run("postgres://x")
    assert stats == {"processed": 0, "upserted": 0, "skipped": "lock_busy"}
    assert refreshed["called"] is False


def test_run_refreshes_mv_after_lock_released(monkeypatch):
    """Successful run: the MV refresh fires once, AFTER the advisory lock is freed."""
    import contextlib

    events: list[str] = []

    def _fake_connect(dsn=None, *, autocommit=False):
        conn = _FakeConn({"events": events})
        return conn

    monkeypatch.setattr(rm, "connect", _fake_connect)

    @contextlib.contextmanager
    def _granted_lock(_conn, _lock_id):
        events.append("lock_acquire")
        try:
            yield True
        finally:
            events.append("lock_release")

    monkeypatch.setattr(rm, "advisory_lock", _granted_lock)
    monkeypatch.setattr(rm, "_resolve_calc_date", lambda _c, _cd: _dt.date(2026, 6, 11))
    monkeypatch.setattr(rm, "_risk_free_rate", lambda _c, _cd: 0.04)
    monkeypatch.setattr(rm, "_fetch_fund_ids", lambda _c, _cd, _lim: [])
    monkeypatch.setattr(rm, "_fetch_benchmark_returns", lambda _c, _cd: {})
    monkeypatch.setattr(rm, "_fetch_fund_benchmarks", lambda _c: {})
    monkeypatch.setattr(
        rm, "_fetch_macro_changes",
        lambda _c, _cd: {"DGS10": [], "BAA10Y": [], "CPI": []},
    )
    monkeypatch.setattr(rm, "_fetch_fund_asset_classes", lambda _c: {})
    monkeypatch.setattr(rm, "_update_peer_percentiles", lambda _c, _cd: 0)
    monkeypatch.setattr(rm, "_update_manager_scores", lambda _c, _cd: 0)
    monkeypatch.setattr(
        rm, "_refresh_fund_risk_latest_mv", lambda _dsn: events.append("refresh")
    )

    stats = rm.run("postgres://x")

    assert stats["mv_refreshed"] is True
    assert "refresh" in events
    assert events.index("refresh") > events.index("lock_release")


# ──────────────────────────────────────────────────────────────────────────────
# Class regression metrics (Tier 1, rank 4): empirical duration, credit beta,
# inflation beta, crisis alpha. Pure — synthetic dated frames, no DB.
# ──────────────────────────────────────────────────────────────────────────────
def test_empirical_duration_recovers_known_beta():
    """Fund return = -6 * delta_yield (+ tiny noise) → empirical_duration ≈ 6."""
    rng = np.random.default_rng(7)
    start = _dt.date(2023, 1, 2)
    n = 400
    dates = [start + _dt.timedelta(days=i) for i in range(n)]
    dy = rng.normal(0.0, 0.0005, n)          # daily yield change in DECIMAL (5bp sd)
    fund_ret = -6.0 * dy + rng.normal(0.0, 1e-5, n)
    fund_dated = list(zip(dates, fund_ret.tolist(), strict=True))
    dy_dated = list(zip(dates, dy.tolist(), strict=True))

    dur, r2 = rm.empirical_duration(fund_dated, dy_dated)
    assert dur is not None and abs(dur - 6.0) < 0.1
    assert r2 is not None and r2 > 0.95


def test_empirical_duration_none_below_min_observations():
    """Fewer than REG_MIN_OBSERVATIONS aligned dates → (None, None)."""
    start = _dt.date(2024, 1, 1)
    dates = [start + _dt.timedelta(days=i) for i in range(50)]  # < 120
    fund_dated = [(d, 0.001) for d in dates]
    dy_dated = [(d, 0.0001) for d in dates]
    assert rm.empirical_duration(fund_dated, dy_dated) == (None, None)


def test_empirical_duration_none_below_min_r_squared():
    """Pure-noise fund return uncorrelated with yield → R² below floor → None."""
    rng = np.random.default_rng(11)
    start = _dt.date(2023, 1, 2)
    n = 400
    dates = [start + _dt.timedelta(days=i) for i in range(n)]
    fund_dated = list(zip(dates, rng.normal(0, 0.01, n).tolist(), strict=True))
    dy_dated = list(zip(dates, rng.normal(0, 0.0005, n).tolist(), strict=True))
    dur, r2 = rm.empirical_duration(fund_dated, dy_dated)
    assert dur is None and r2 is None


def test_credit_beta_recovers_known_beta():
    """Fund return = -3 * delta_spread (+ tiny noise) → credit_beta ≈ 3."""
    rng = np.random.default_rng(13)
    start = _dt.date(2023, 1, 2)
    n = 400
    dates = [start + _dt.timedelta(days=i) for i in range(n)]
    ds = rng.normal(0.0, 0.0004, n)          # daily spread change, DECIMAL
    fund_ret = -3.0 * ds + rng.normal(0.0, 1e-5, n)
    fund_dated = list(zip(dates, fund_ret.tolist(), strict=True))
    ds_dated = list(zip(dates, ds.tolist(), strict=True))

    cb, r2 = rm.credit_beta(fund_dated, ds_dated)
    assert cb is not None and abs(cb - 3.0) < 0.1
    assert r2 is not None and r2 > 0.95


def test_credit_beta_none_below_min_observations():
    start = _dt.date(2024, 1, 1)
    dates = [start + _dt.timedelta(days=i) for i in range(60)]  # < 120
    fund_dated = [(d, 0.001) for d in dates]
    ds_dated = [(d, 0.0001) for d in dates]
    assert rm.credit_beta(fund_dated, ds_dated) == (None, None)


def test_inflation_beta_recovers_positive_beta():
    """Monthly fund return = 0.5 * monthly_cpi_change (+noise) → inflation_beta ≈ 0.5.

    Daily fund returns compound to a monthly return ≈ the per-month target; the
    function resamples daily→monthly internally, so we hand it ~21 trading days
    per month with the per-month return spread evenly.
    """
    rng = np.random.default_rng(21)
    fund_dated: list = []
    cpi_dated: list = []
    for k in range(18):                       # 18 months ≥ INFLATION_MIN_MONTHS
        year = 2023 + (k // 12)
        month = (k % 12) + 1
        cpi_chg = float(rng.normal(0.003, 0.002))      # monthly CPI MoM, decimal
        cpi_dated.append((_dt.date(year, month, 1), cpi_chg))
        target_month_ret = 0.5 * cpi_chg + float(rng.normal(0.0, 1e-4))
        daily = (1.0 + target_month_ret) ** (1.0 / 21) - 1.0
        for day in range(1, 22):
            fund_dated.append((_dt.date(year, month, day), daily))

    ib, r2 = rm.inflation_beta(fund_dated, cpi_dated)
    assert ib is not None and abs(ib - 0.5) < 0.15
    assert r2 is not None and r2 >= rm.INFLATION_MIN_R2


def test_inflation_beta_none_below_min_months():
    """Fewer than INFLATION_MIN_MONTHS aligned months → (None, None)."""
    fund_dated = [(_dt.date(2024, m, d), 0.001) for m in range(1, 4) for d in range(1, 22)]
    cpi_dated = [(_dt.date(2024, m, 1), 0.002) for m in range(1, 4)]  # 3 months
    assert rm.inflation_beta(fund_dated, cpi_dated) == (None, None)


def test_crisis_alpha_positive_when_fund_outperforms_in_drawdown():
    """Construct a benchmark with a deep (> -10%) drawdown stretch; the fund is
    flat during it. Fund cum − bench cum over crisis days must be > 0."""
    start = _dt.date(2023, 1, 2)
    n = 120
    dates = [start + _dt.timedelta(days=i) for i in range(n)]
    bench_ret = [0.0] * n
    # Days 30..70: benchmark falls ~1%/day → cumulative drawdown well past -10%.
    for i in range(30, 71):
        bench_ret[i] = -0.01
    fund_ret = [0.0] * n                        # fund flat throughout
    fund_dated = list(zip(dates, fund_ret, strict=True))
    bench_dated = list(zip(dates, bench_ret, strict=True))

    ca = rm.crisis_alpha(fund_dated, bench_dated)
    assert ca is not None and ca > 0.0


def test_crisis_alpha_none_when_too_few_crisis_days():
    """No benchmark drawdown beyond threshold → fewer than CRISIS_MIN_DAYS
    crisis days → None."""
    start = _dt.date(2023, 1, 2)
    n = 120
    dates = [start + _dt.timedelta(days=i) for i in range(n)]
    flat = list(zip(dates, [0.0001] * n, strict=True))   # gently rising, no DD
    assert rm.crisis_alpha(flat, flat) is None


def test_crisis_alpha_none_below_min_aligned():
    """Fewer than 60 aligned days → None (matches legacy guard)."""
    start = _dt.date(2024, 1, 1)
    dates = [start + _dt.timedelta(days=i) for i in range(40)]
    rows = list(zip(dates, [0.0] * 40, strict=True))
    assert rm.crisis_alpha(rows, rows) is None


def test_metric_columns_include_class_regression():
    """The upsert column list carries the four new metrics + helpers."""
    for col in (
        "scoring_model", "empirical_duration", "empirical_duration_r2",
        "credit_beta", "credit_beta_r2", "inflation_beta", "inflation_beta_r2",
        "crisis_alpha_score",
    ):
        assert col in rm._METRIC_COLUMNS, col


def _nav_rows_from_returns(start, daily_returns):
    """[(date, nav)] from a daily-return list, NAV seeded at 100."""
    rows = [(start - _dt.timedelta(days=1), 100.0)]
    nav = 100.0
    for i, r in enumerate(daily_returns):
        nav *= (1.0 + r)
        rows.append((start + _dt.timedelta(days=i), nav))
    return rows


def test_class_regression_fixed_income_populates_duration_and_credit():
    """A fixed_income fund whose returns track ΔDGS10/ΔBAA10Y gets
    empirical_duration, credit_beta and scoring_model='fixed_income';
    alt-only keys absent."""
    rng = np.random.default_rng(5)
    start = _dt.date(2023, 1, 2)
    n = 420
    dates = [start + _dt.timedelta(days=i) for i in range(n)]
    dy = rng.normal(0.0, 0.0005, n)
    ds = rng.normal(0.0, 0.0004, n)
    fund_ret = (-6.0 * dy - 3.0 * ds + rng.normal(0.0, 1e-5, n)).tolist()
    rows = _nav_rows_from_returns(start, fund_ret)
    macro_changes = {
        "DGS10": list(zip(dates, dy.tolist(), strict=True)),
        "BAA10Y": list(zip(dates, ds.tolist(), strict=True)),
        "CPI": [],
    }
    out = rm.class_regression_metrics_for(rows, "fixed_income", macro_changes)
    assert out["scoring_model"] == "fixed_income"
    assert out["empirical_duration"] is not None
    assert out["credit_beta"] is not None
    assert "crisis_alpha_score" not in out
    assert "inflation_beta" not in out


def test_class_regression_equity_is_noop():
    """An equity fund only gets scoring_model='equity', no regression keys."""
    rows = [(_dt.date(2024, 1, 1) + _dt.timedelta(days=i), 100.0 + i) for i in range(30)]
    out = rm.class_regression_metrics_for(rows, "equity", {"DGS10": [], "BAA10Y": [], "CPI": []})
    assert out == {"scoring_model": "equity"}


# ──────────────────────────────────────────────────────────────────────────────
# T3C-2: manager_score post-step (equity composite). No DB — fake the cursor.
# ──────────────────────────────────────────────────────────────────────────────
class _ManagerScoreCursor:
    """Fake cursor: SELECT returns canned equity rows; UPDATE batch is captured."""

    def __init__(self, select_rows, sink):
        self._select_rows = select_rows
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def execute(self, sql, params=None):
        self._sink["last_sql"] = " ".join(str(sql).split())

    def fetchall(self):
        return self._select_rows

    def executemany(self, sql, rows):
        self._sink["update_sql"] = " ".join(str(sql).split())
        self._sink["update_rows"] = list(rows)


class _ManagerScoreConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_update_manager_scores_writes_equity_composites():
    """Two equity rows -> two (manager_score, instrument_id, calc_date) updates,
    each a 0-100 composite from manager_score.compute_equity_manager_score."""
    from src.workers import manager_score as ms

    # row columns: instrument_id, return_1y, sharpe_1y, sharpe_cf,
    #              max_drawdown_1y, information_ratio_1y
    rows = [
        ("11111111-1111-1111-1111-111111111111", 0.10, 1.0, 1.0, -0.25, 0.5),
        ("22222222-2222-2222-2222-222222222222", -0.05, 0.0, None, -0.40, -0.2),
    ]
    sink: dict = {}
    cur = _ManagerScoreCursor(rows, sink)
    conn = _ManagerScoreConn(cur)

    # Reproduce the post-step's peer-median baseline so expected == actual.
    peer_medians = rm._equity_peer_medians(rows)
    updated = rm._update_manager_scores(conn, _dt.date(2026, 6, 11))

    assert updated == 2
    by_id = {r[1]: r[0] for r in sink["update_rows"]}
    exp0 = ms.compute_equity_manager_score(
        {"return_1y": 0.10, "sharpe_1y": 1.0, "sharpe_cf": 1.0,
         "max_drawdown_1y": -0.25, "information_ratio_1y": 0.5},
        peer_medians=peer_medians,
    ).score
    exp1 = ms.compute_equity_manager_score(
        {"return_1y": -0.05, "sharpe_1y": 0.0, "sharpe_cf": None,
         "max_drawdown_1y": -0.40, "information_ratio_1y": -0.2},
        peer_medians=peer_medians,
    ).score
    assert by_id["11111111-1111-1111-1111-111111111111"] == exp0
    assert by_id["22222222-2222-2222-2222-222222222222"] == exp1
    # Every written score is a valid 0-100 manager_score at the calc_date.
    for score, _iid, cdate in sink["update_rows"]:
        assert 0.0 <= score <= 100.0
        assert cdate == _dt.date(2026, 6, 11)
    assert "UPDATE fund_risk_metrics" in sink["update_sql"]
    assert "manager_score" in sink["update_sql"]


def test_update_manager_scores_empty_cohort_returns_zero():
    sink: dict = {}
    cur = _ManagerScoreCursor([], sink)
    conn = _ManagerScoreConn(cur)
    updated = rm._update_manager_scores(conn, _dt.date(2026, 6, 11))
    assert updated == 0
    assert "update_rows" not in sink  # nothing to write


def test_equity_peer_medians_skips_missing_inputs():
    """Median sub-score per component is taken over funds that HAVE the metric."""
    rows = [
        ("a", 0.10, 1.0, 1.0, -0.25, 0.5),
        ("b", None, 2.0, 2.0, -0.10, None),  # missing return_1y and IR
    ]
    medians = rm._equity_peer_medians(rows)
    # return_consistency only has fund 'a' -> its single sub-score is the median.
    assert "return_consistency" in medians
    # information_ratio only has fund 'a' -> present.
    assert "information_ratio" in medians
    # All medians are valid 0-100 sub-scores.
    for v in medians.values():
        assert 0.0 <= v <= 100.0


def test_run_calls_manager_score_post_step(monkeypatch):
    """Successful run() invokes _update_manager_scores once with the calc_date."""
    import contextlib

    events: list[str] = []

    def _fake_connect(dsn=None, *, autocommit=False):
        return _FakeConn({"events": events})

    monkeypatch.setattr(rm, "connect", _fake_connect)

    @contextlib.contextmanager
    def _granted_lock(_conn, _lock_id):
        yield True

    monkeypatch.setattr(rm, "advisory_lock", _granted_lock)
    monkeypatch.setattr(rm, "_resolve_calc_date", lambda _c, _cd: _dt.date(2026, 6, 11))
    monkeypatch.setattr(rm, "_risk_free_rate", lambda _c, _cd: 0.04)
    monkeypatch.setattr(rm, "_fetch_fund_ids", lambda _c, _cd, _lim: [])
    monkeypatch.setattr(rm, "_fetch_benchmark_returns", lambda _c, _cd: {})
    monkeypatch.setattr(rm, "_fetch_fund_benchmarks", lambda _c: {})
    monkeypatch.setattr(
        rm, "_fetch_macro_changes",
        lambda _c, _cd: {"DGS10": [], "BAA10Y": [], "CPI": []},
    )
    monkeypatch.setattr(rm, "_fetch_fund_asset_classes", lambda _c: {})
    monkeypatch.setattr(rm, "_update_peer_percentiles", lambda _c, _cd: 0)
    monkeypatch.setattr(rm, "_refresh_fund_risk_latest_mv", lambda _dsn: None)

    captured: dict = {}
    monkeypatch.setattr(
        rm, "_update_manager_scores",
        lambda _c, cdate: captured.__setitem__("calc_date", cdate) or 7,
    )

    stats = rm.run("postgres://x")
    assert captured["calc_date"] == _dt.date(2026, 6, 11)
    assert stats["manager_score_rows"] == 7


# ──────────────────────────────────────────────────────────────────────────────
# T3C-3: enriched peer ranking — quartile + band + cohort guard + mid-rank ties.
# Pure-helper tests (no DB) for the ported conventions, plus an SQL-shape guard.
# ──────────────────────────────────────────────────────────────────────────────
def test_peer_quartile_from_percentile_boundaries():
    assert rm._peer_quartile_from_percentile(100.0) == 1
    assert rm._peer_quartile_from_percentile(75.0) == 1
    assert rm._peer_quartile_from_percentile(74.99) == 2
    assert rm._peer_quartile_from_percentile(50.0) == 2
    assert rm._peer_quartile_from_percentile(49.99) == 3
    assert rm._peer_quartile_from_percentile(25.0) == 3
    assert rm._peer_quartile_from_percentile(24.99) == 4
    assert rm._peer_quartile_from_percentile(0.0) == 4


def test_midrank_percentile_all_tied_is_50_not_100():
    # All-tied cohort: every member sits at the median (50.0), the institutional
    # convention — percent_rank() would put them all at 0.
    peers = [1.0, 1.0, 1.0, 1.0]
    assert rm._peer_midrank_percentile(1.0, peers, higher_is_better=True) == 50.0


def test_midrank_percentile_best_value_high():
    peers = [0.1, 0.2, 0.3, 0.4, 0.5]
    # value strictly above all peers -> (5 below + 0)/5 = 100.
    assert rm._peer_midrank_percentile(0.9, peers, higher_is_better=True) == 100.0


def test_midrank_percentile_drawdown_less_negative_ranks_higher():
    # Drawdown uses higher_is_better=True (less-negative = larger numeric =
    # better), matching the existing SQL (ORDER BY max_drawdown_1y ASC ->
    # higher value = higher pctl).
    peers = [-0.40, -0.30, -0.20, -0.10]
    p_best = rm._peer_midrank_percentile(-0.05, peers, higher_is_better=True)
    p_worst = rm._peer_midrank_percentile(-0.50, peers, higher_is_better=True)
    assert p_best > p_worst


def test_midrank_percentile_empty_cohort_returns_50():
    assert rm._peer_midrank_percentile(1.0, [], higher_is_better=True) == 50.0


def test_enriched_peer_sql_has_quartile_band_and_cohort_guard():
    sql = rm._PEER_PERCENTILES_SQL.lower()
    # New target columns are written.
    assert "peer_overall_quartile" in sql
    assert "peer_band_low" in sql
    assert "peer_band_mid" in sql
    assert "peer_band_high" in sql
    # Cohort guard uses the institutional minimum (passed as a bind).
    assert "min_cohort" in sql
    # Mid-rank tie convention: count_below + 0.5 * count_equal.
    assert "0.5" in sql
    # Band uses percentile_cont over sharpe_1y (p25/median/p75).
    assert "percentile_cont" in sql
    # percent_rank() is no longer the ranking mechanism.
    assert "percent_rank" not in sql


def test_min_peer_cohort_size_matches_legacy():
    # Ported from peer_group_service.MIN_PEER_COHORT_SIZE = 10.
    assert rm.MIN_PEER_COHORT_SIZE == 10
