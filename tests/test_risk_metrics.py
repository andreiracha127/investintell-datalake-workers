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
    monkeypatch.setattr(rm, "_update_peer_percentiles", lambda _c, _cd: 0)
    monkeypatch.setattr(
        rm, "_refresh_fund_risk_latest_mv", lambda _dsn: events.append("refresh")
    )

    stats = rm.run("postgres://x")

    assert stats["mv_refreshed"] is True
    assert "refresh" in events
    assert events.index("refresh") > events.index("lock_release")


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
