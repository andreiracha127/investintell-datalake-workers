"""risk_metrics worker — recompute fund risk metrics from raw NAV.

Standalone reimplementation of the legacy ``risk_calc`` worker. Reads **raw
series** (``nav_timeseries``, ``benchmark_nav``) from the data-lake and writes
the rich ``fund_risk_metrics`` table back, plus a thin ``sec_mmf_metrics`` pass
projected from ``sec_nport_holdings`` is intentionally out of scope here — that
table is fed by the N-MFP ingestion worker; we only (re)create its schema and
upsert when the source columns are present.

Recipe (README §"Receita validada", proven on Lean 2026-06-11):

  returns        = nav[t]/nav[t-1] - 1          (arithmetic, return-type agnostic)
  return_Ny      = nav[-1]/nav[-window] - 1
  volatility_1y  = std(ret, ddof=1) * sqrt(252)
  volatility_garch = GARCH(1,1) 1-step-ahead annualised, else EWMA(0.94)
  max_drawdown   = min(cumprod(1+ret)/cummax - 1)
  sharpe         = (mean(excess)/std(excess,ddof=1)) * sqrt(252)
  sortino        = mean(excess) / TDD * sqrt(252)   (TDD = sqrt(mean(min(excess,0)^2)))
  beta/alpha/TE/IR = OLS of fund excess vs benchmark excess (date-aligned)
  VaR/CVaR 95    = Rockafellar-Uryasev empirical estimator (return-space, negative)
  CVaR 99/99.9   = EVT POT-GPD on the loss tail

All windows are deterministic (252 trading days/year, ddof=1). ``calc_date`` is a
parameter — no implicit ``date.today()`` in window logic.

Contract:  run(dsn, *, calc_date=None, limit=None) -> {"processed", "upserted"}
"""

from __future__ import annotations

import datetime as _dt
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any

import numpy as np

from src.db import LOCK_RISK_METRICS, advisory_lock, connect

TRADING_DAYS = 252

# Internal parallelism: saturate the box's CPUs (CPU-bound GARCH/EVT per fund),
# but cap so concurrent cloud connections (one per child, NAV read + upsert) stay
# bounded. 24 == Railway vCPU count and a safe ceiling for the cloud pool.
MAX_WORKERS_CAP = 24
RISK_FREE_FALLBACK = 0.04
MIN_ANNUALIZED_VOL = 0.01
MAX_DAILY_RETURN_ABS = 0.5
NUMERIC_10_6_MAX = 9999.999999

# Rolling windows (trading days) for VaR/CVaR/returns.
WINDOWS = {"1m": 21, "3m": 63, "6m": 126, "12m": 252}

# ── columns we populate (must exist in schemas/risk_metrics.sql) ──────────────
_METRIC_COLUMNS = [
    "cvar_95_1m", "cvar_95_3m", "cvar_95_6m", "cvar_95_12m",
    "var_95_1m", "var_95_3m", "var_95_6m", "var_95_12m",
    "return_1m", "return_3m", "return_6m", "return_1y",
    "return_3y_ann", "return_5y_ann", "return_10y_ann",
    "volatility_1y", "volatility_garch", "vol_model",
    "max_drawdown_1y", "max_drawdown_3y",
    "sharpe_1y", "sharpe_3y", "sortino_1y", "calmar_ratio_3y",
    "alpha_1y", "beta_1y", "tracking_error_1y", "information_ratio_1y",
    "upside_capture_1y", "downside_capture_1y", "equity_correlation_252d",
    "sharpe_cf", "sharpe_cf_skew", "sharpe_cf_kurt",
    "sharpe_cf_ci_lower", "sharpe_cf_ci_upper",
    "cvar_99_evt", "cvar_999_evt", "evt_xi_shape",
    "empirical_duration", "credit_beta",
    "fed_funds_rate_at_calc", "data_quality_flags",
]


# ──────────────────────────────────────────────────────────────────────────────
# Pure-math primitives
# ──────────────────────────────────────────────────────────────────────────────
def _clip(value: float | None, decimals: int = 6) -> float | None:
    if value is None:
        return None
    v = float(value)
    if not np.isfinite(v):
        return None
    v = max(-NUMERIC_10_6_MAX, min(NUMERIC_10_6_MAX, v))
    return round(v, decimals)


def returns_from_nav(nav: np.ndarray) -> np.ndarray:
    """Arithmetic daily returns from a NAV price path (return-type agnostic)."""
    nav = np.asarray(nav, dtype=float)
    prev = nav[:-1]
    with np.errstate(divide="ignore", invalid="ignore"):
        ret = nav[1:] / prev - 1.0
    ret = ret[np.isfinite(ret)]
    # Reject physically-impossible single-day moves (unadjusted corp actions).
    return ret[np.abs(ret) <= MAX_DAILY_RETURN_ABS]


def cum_return(nav: np.ndarray, window: int) -> float | None:
    if len(nav) <= window:
        return None
    base = nav[-1 - window]
    if base == 0 or not np.isfinite(base):
        return None
    return float(nav[-1] / base - 1.0)


def annualized_return(nav: np.ndarray, years: int) -> float | None:
    window = years * TRADING_DAYS
    if len(nav) <= window:
        return None
    base = nav[-1 - window]
    if base <= 0:
        return None
    total = nav[-1] / base
    if total <= 0:
        return -1.0
    return float(total ** (1.0 / years) - 1.0)


def volatility(ret: np.ndarray, days: int) -> float | None:
    if len(ret) < days:
        return None
    return float(np.std(ret[-days:], ddof=1) * np.sqrt(TRADING_DAYS))


def max_drawdown(ret: np.ndarray, days: int) -> float | None:
    if len(ret) < days:
        return None
    cum = np.cumprod(1.0 + ret[-days:])
    running_max = np.maximum.accumulate(cum)
    return float(np.min(cum / running_max - 1.0))


def sharpe(ret: np.ndarray, days: int, rf: float) -> float | None:
    if len(ret) < days:
        return None
    excess = ret[-days:] - rf / TRADING_DAYS
    vol = float(np.std(excess, ddof=1))
    if vol == 0 or not np.isfinite(vol):
        return None
    if vol * np.sqrt(TRADING_DAYS) < MIN_ANNUALIZED_VOL:
        return None
    return float(np.mean(excess) / vol * np.sqrt(TRADING_DAYS))


def sortino(ret: np.ndarray, days: int, rf: float) -> float | None:
    if len(ret) < days:
        return None
    excess = ret[-days:] - rf / TRADING_DAYS
    shortfall = np.minimum(excess, 0.0)
    tdd = float(np.sqrt(np.mean(shortfall**2)))
    if tdd == 0 or not np.isfinite(tdd):
        return None
    if tdd * np.sqrt(TRADING_DAYS) < MIN_ANNUALIZED_VOL:
        return None
    return float(np.mean(excess) / tdd * np.sqrt(TRADING_DAYS))


def cvar_var_95(ret: np.ndarray, confidence: float = 0.95) -> tuple[float, float]:
    """Rockafellar-Uryasev empirical CVaR/VaR (return-space, negative losses)."""
    if len(ret) < 5:
        return float("nan"), float("nan")
    losses = -ret
    var_loss = float(np.quantile(losses, confidence, method="higher"))
    u = np.maximum(losses - var_loss, 0.0)
    cvar_loss = var_loss + u.sum() / ((1.0 - confidence) * losses.size)
    return -cvar_loss, -var_loss


def garch_or_ewma(ret: np.ndarray) -> tuple[float | None, str | None]:
    """GARCH(1,1) zero-mean 1-step-ahead annualised vol; EWMA(0.94) fallback."""
    arr = np.asarray(ret, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    # Guard: enough points and a NON-DEGENERATE, non-explosive return series.
    # A near-constant series (std -> 0) makes GARCH's log-likelihood divide by
    # zero (floods logs with benign RuntimeWarnings and yields NaN vol); those
    # funds fall through to EWMA instead.
    if len(arr) >= 100 and 1e-6 < float(np.std(arr)) <= 0.5:
        try:
            import warnings as _warnings

            from arch import arch_model

            # arch/numpy emit benign RuntimeWarnings while fitting hard series;
            # the convergence_flag + finiteness checks below are the real gate.
            with _warnings.catch_warnings(), np.errstate(all="ignore"):
                _warnings.simplefilter("ignore")
                model = arch_model(arr * 100.0, vol="GARCH", p=1, q=1,
                                   mean="Zero", rescale=False)
                res = model.fit(disp="off", show_warning=False)
                if res.convergence_flag == 0:
                    params = {str(k): float(v) for k, v in res.params.to_dict().items()}
                    persistence = params["alpha[1]"] + params["beta[1]"]
                    fc = res.forecast(horizon=1)
                    var_1 = float(fc.variance.values[-1, 0])
                    if np.isfinite(var_1) and var_1 >= 0 and persistence < 1.0 - 1e-6:
                        daily = np.sqrt(var_1) / 100.0
                        return float(daily * np.sqrt(TRADING_DAYS)), "GARCH(1,1)"
        except Exception:
            pass
    # EWMA(0.94) RiskMetrics fallback.
    if len(arr) < 20:
        return None, None
    variance = float(np.var(arr))
    for r in arr:
        variance = 0.94 * variance + 0.06 * (r * r)
    return float(np.sqrt(variance) * np.sqrt(TRADING_DAYS)), "EWMA_0.94"


def cornish_fisher_sharpe(ret: np.ndarray, rf: float) -> dict[str, float | None]:
    """Cornish-Fisher skew/kurtosis-adjusted Sharpe with Opdyke 95% CI."""
    out: dict[str, float | None] = {
        "sharpe_cf": None, "sharpe_cf_skew": None, "sharpe_cf_kurt": None,
        "sharpe_cf_ci_lower": None, "sharpe_cf_ci_upper": None,
    }
    window = ret[-(3 * TRADING_DAYS):] if len(ret) >= 3 * TRADING_DAYS else ret
    n = len(window)
    if n < 30:
        return out
    excess = window - rf / TRADING_DAYS
    mu = float(np.mean(excess))
    sd = float(np.std(excess, ddof=1))
    if sd == 0 or not np.isfinite(sd):
        return out
    sr = mu / sd  # per-period
    skew = float(np.mean(((excess - mu) / sd) ** 3))
    kurt = float(np.mean(((excess - mu) / sd) ** 4) - 3.0)
    out["sharpe_cf_skew"] = _clip(skew)
    out["sharpe_cf_kurt"] = _clip(kurt)
    # Cornish-Fisher expansion of the Sharpe ratio (annualised).
    sr_cf = sr * (1.0 + (skew / 6.0) * sr - ((kurt) / 24.0) * sr * sr)
    sr_cf_ann = sr_cf * np.sqrt(TRADING_DAYS)
    # Monotonicity guard (legacy null-out).
    if not np.isfinite(sr_cf_ann) or abs(sr_cf_ann) > 1e3:
        return out
    # Opdyke (2007) asymptotic SE of the Sharpe ratio.
    se = np.sqrt((1.0 + 0.5 * sr * sr - skew * sr + (kurt / 4.0) * sr * sr) / n)
    se_ann = se * np.sqrt(TRADING_DAYS)
    out["sharpe_cf"] = _clip(sr_cf_ann)
    out["sharpe_cf_ci_lower"] = _clip(sr_cf_ann - 1.96 * se_ann)
    out["sharpe_cf_ci_upper"] = _clip(sr_cf_ann + 1.96 * se_ann)
    return out


def evt_tail(ret: np.ndarray) -> dict[str, float | None]:
    """EVT POT-GPD on the loss tail → CVaR 99 / 99.9 and shape xi."""
    out: dict[str, float | None] = {
        "cvar_99_evt": None, "cvar_999_evt": None, "evt_xi_shape": None,
    }
    arr = np.asarray(ret, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) < 100:
        return out
    losses = -arr
    losses = losses[losses > 0]
    if len(losses) < 30:
        return out
    try:
        from scipy.stats import genpareto
    except Exception:
        return out
    # Peaks-over-threshold at the 90th percentile (drop to 85th if too few).
    for q in (0.90, 0.85):
        u = float(np.quantile(losses, q))
        exceed = losses[losses > u] - u
        if len(exceed) >= 20:
            break
    else:
        return out
    n, n_u = len(losses), len(exceed)
    try:
        xi, _loc, beta = genpareto.fit(exceed, floc=0.0)
    except Exception:
        return out
    if beta <= 0 or not np.isfinite(xi):
        return out
    out["evt_xi_shape"] = _clip(float(xi))

    def _var_cvar(p: float) -> tuple[float, float]:
        # GPD tail quantile (McNeil-Frey closed form).
        ratio = (n / n_u) * (1.0 - p)
        var = u + (beta / xi) * (ratio ** (-xi) - 1.0) if abs(xi) > 1e-8 \
            else u - beta * np.log(ratio)
        if xi < 1.0:
            cvar = var / (1.0 - xi) + (beta - xi * u) / (1.0 - xi)
        else:
            cvar = var  # heavy tail: ES undefined, fall back to VaR
        return var, cvar

    _, c99 = _var_cvar(0.99)
    _, c999 = _var_cvar(0.999)
    # Return-space (negative losses).
    out["cvar_99_evt"] = _clip(-float(c99))
    out["cvar_999_evt"] = _clip(-float(c999))
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Benchmark wiring (2026-06-12) — liga regression_metrics ao benchmark_nav.
# A função existia desde o port mas nunca foi chamada: faltava o mapa
# fundo→benchmark e o fetch das séries. Mapa em duas camadas:
#   1. strategy label (stage) → bloco nomeado do benchmark_ingest;
#   2. fallback instruments_universe.asset_class → bloco amplo.
# Labels sem benchmark defensável (Balanced, Target Date, Convertibles…)
# caem no fallback do asset_class; alternatives sem mapa ficam sem métricas
# relativas — exceto equity_correlation_252d, sempre vs EQUITY_BENCHMARK_BLOCK.
# ──────────────────────────────────────────────────────────────────────────────
EQUITY_BENCHMARK_BLOCK = "na_equity_large"

# Capture ratios além disso indicam denominador degenerado (benchmark ~flat
# no subconjunto de dias) — sem significado; e a coluna é numeric(8,4).
CAPTURE_LIMIT = 500.0

BENCHMARK_BY_LABEL: dict[str, str] = {
    "Asian Equity": "dm_asia_equity",
    "Asset-Backed Securities": "fi_us_aggregate",
    "Cash Equivalent": "cash",
    "Commodities": "alt_commodities",
    "ESG/Sustainable Bond": "fi_us_aggregate",
    "ESG/Sustainable Equity": "na_equity_large",
    "Emerging Markets Debt": "fi_em_debt",
    "Emerging Markets Equity": "em_equity",
    "European Equity": "dm_europe_equity",
    "Global Equity": "na_equity_large",
    "Government Bond": "fi_us_treasury",
    "High Yield Bond": "fi_us_high_yield",
    "Inflation-Linked Bond": "fi_us_tips",
    "Intermediate-Term Bond": "fi_us_aggregate",
    "International Equity": "factor_source_intl_developed",
    "Investment Grade Bond": "fi_ig_corporate",
    "Large Blend": "na_equity_large",
    "Large Growth": "na_equity_growth",
    "Large Value": "na_equity_value",
    "Long/Short Equity": "na_equity_large",
    "Mid Blend": "na_equity_large",
    "Mid Growth": "na_equity_growth",
    "Mid Value": "na_equity_value",
    "Mortgage-Backed Securities": "fi_us_aggregate",
    "Municipal Bond": "fi_us_aggregate",
    "Precious Metals": "alt_gold",
    "Private Credit": "fi_us_high_yield",
    "Real Estate": "alt_real_estate",
    "Sector Equity": "na_equity_large",
    "Small Blend": "na_equity_small",
    "Small Growth": "na_equity_small",
    "Small Value": "na_equity_small",
    "Structured Credit": "fi_us_aggregate",
}

BENCHMARK_BY_ASSET_CLASS: dict[str, str] = {
    "equity": "na_equity_large",
    "fixed_income": "fi_us_aggregate",
    "cash": "cash",
}

# Janela de busca: 252 sessões + folga p/ feriados/lacunas de NAV.
_BENCH_LOOKBACK_DAYS = 600

_FUND_BENCHMARKS_SQL = """
WITH labels AS (
    SELECT DISTINCT ON (source_pk)
           source_pk::uuid AS instrument_id,
           proposed_strategy_label AS label
    FROM strategy_reclassification_stage
    WHERE source_table = 'instruments_universe'
      AND proposed_strategy_label IS NOT NULL
    ORDER BY source_pk, classified_at DESC
)
SELECT iu.instrument_id, l.label, iu.asset_class
FROM instruments_universe iu
LEFT JOIN labels l ON l.instrument_id = iu.instrument_id
"""


def _fetch_fund_benchmarks(conn) -> dict[str, str]:
    """instrument_id(str) → benchmark block_id (label map, asset-class fallback)."""
    out: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(_FUND_BENCHMARKS_SQL)
        for iid, label, asset_class in cur.fetchall():
            block = BENCHMARK_BY_LABEL.get(label or "") or BENCHMARK_BY_ASSET_CLASS.get(
                asset_class or ""
            )
            if block:
                out[str(iid)] = block
    return out


def _fetch_benchmark_returns(
    conn, calc_date: _dt.date
) -> dict[str, list[tuple[_dt.date, float]]]:
    """block_id → [(date, simple daily return)] dos NAVs do benchmark_nav.

    Retornos simples recomputados do próprio NAV (não de return_1d, que é
    log) para casar com a convenção dos retornos de fundo no worker.
    """
    blocks = sorted(
        set(BENCHMARK_BY_LABEL.values())
        | set(BENCHMARK_BY_ASSET_CLASS.values())
        | {EQUITY_BENCHMARK_BLOCK}
    )
    with conn.cursor() as cur:
        cur.execute(
            """SELECT block_id, nav_date, nav FROM benchmark_nav
               WHERE block_id = ANY(%s) AND nav_date <= %s AND nav_date > %s
               ORDER BY block_id, nav_date""",
            (blocks, calc_date, calc_date - _dt.timedelta(days=_BENCH_LOOKBACK_DAYS)),
        )
        rows = cur.fetchall()
    out: dict[str, list[tuple[_dt.date, float]]] = {}
    prev_block: str | None = None
    prev_nav: float | None = None
    for block, d, nav in rows:
        nav = float(nav)
        if block != prev_block:
            prev_block, prev_nav = block, nav
            out.setdefault(block, [])
            continue
        if prev_nav and prev_nav > 0:
            out[block].append((d, nav / prev_nav - 1.0))
        prev_nav = nav
    return out


def dated_simple_returns(
    rows: list[tuple[_dt.date, Any]],
) -> list[tuple[_dt.date, float]]:
    """(date, nav) rows (ascending) → [(date, simple daily return)]."""
    out: list[tuple[_dt.date, float]] = []
    prev: float | None = None
    for d, nav in rows:
        nav = float(nav)
        if prev is not None and prev > 0:
            out.append((d, nav / prev - 1.0))
        prev = nav
    return out


def equity_correlation(
    fund_ret_dated: list[tuple[_dt.date, float]],
    eq_bench_dated: list[tuple[_dt.date, float]],
    days: int = TRADING_DAYS,
) -> float | None:
    """corr(fundo, benchmark de equity) nas últimas ``days`` sessões em comum."""
    bench_map = dict(eq_bench_dated)
    pairs = [(f, bench_map[d]) for d, f in fund_ret_dated if d in bench_map]
    if len(pairs) < days:
        return None
    pairs = pairs[-days:]
    f = np.array([p[0] for p in pairs])
    b = np.array([p[1] for p in pairs])
    if float(np.std(f, ddof=1)) == 0 or float(np.std(b, ddof=1)) == 0:
        return None
    return _clip(float(np.corrcoef(f, b)[0, 1]), 4)


def regression_metrics(
    fund_ret_dated: list[tuple[_dt.date, float]],
    bench_ret_dated: list[tuple[_dt.date, float]],
    rf: float,
    days: int = TRADING_DAYS,
) -> dict[str, float | None]:
    """OLS of fund excess returns vs benchmark excess returns (date-aligned)."""
    out: dict[str, float | None] = {
        "beta_1y": None, "alpha_1y": None,
        "tracking_error_1y": None, "information_ratio_1y": None,
        "upside_capture_1y": None, "downside_capture_1y": None,
    }
    bench_map = dict(bench_ret_dated)
    pairs = [(f, bench_map[d]) for d, f in fund_ret_dated if d in bench_map]
    if len(pairs) < days:
        return out
    pairs = pairs[-days:]
    f = np.array([p[0] for p in pairs])
    b = np.array([p[1] for p in pairs])
    rf_d = rf / TRADING_DAYS
    # beta/alpha via OLS on raw returns; alpha annualised.
    bvar = float(np.var(b, ddof=1))
    if bvar == 0 or not np.isfinite(bvar):
        return out
    beta = float(np.cov(f, b, ddof=1)[0, 1] / bvar)
    alpha_daily = float(np.mean(f) - rf_d) - beta * float(np.mean(b) - rf_d)
    out["beta_1y"] = _clip(beta)
    out["alpha_1y"] = _clip(alpha_daily * TRADING_DAYS)
    # Tracking error & information ratio on active return.
    active = f - b
    te = float(np.std(active, ddof=1) * np.sqrt(TRADING_DAYS))
    out["tracking_error_1y"] = _clip(te)
    if te >= MIN_ANNUALIZED_VOL:
        out["information_ratio_1y"] = _clip(float(np.mean(active)) * TRADING_DAYS / te)
    # Up/down capture (geometric), vs benchmark up/down days. Acima de
    # |CAPTURE_LIMIT| o denominador é degenerado (benchmark ~flat no
    # subconjunto): estatisticamente sem significado E estoura o
    # numeric(8,4) da coluna — vira None, nunca um número absurdo.
    up = b > 0
    down = b < 0
    if up.sum() >= 5 and (1.0 + b[up]).prod() > 0:
        fc = float(np.prod(1.0 + f[up]) ** (1.0 / up.sum()) - 1.0)
        bc = float(np.prod(1.0 + b[up]) ** (1.0 / up.sum()) - 1.0)
        if bc != 0 and abs(100.0 * fc / bc) <= CAPTURE_LIMIT:
            out["upside_capture_1y"] = _clip(100.0 * fc / bc, 4)
    if down.sum() >= 5 and (1.0 + b[down]).prod() > 0:
        fc = float(np.prod(1.0 + f[down]) ** (1.0 / down.sum()) - 1.0)
        bc = float(np.prod(1.0 + b[down]) ** (1.0 / down.sum()) - 1.0)
        if bc != 0 and abs(100.0 * fc / bc) <= CAPTURE_LIMIT:
            out["downside_capture_1y"] = _clip(100.0 * fc / bc, 4)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Per-fund metric assembly (pure — no I/O)
# ──────────────────────────────────────────────────────────────────────────────
def compute_metrics(
    nav: np.ndarray,
    rf: float,
    *,
    rejected: int = 0,
) -> dict[str, Any] | None:
    """All risk metrics for one fund from its NAV price path. Pure computation."""
    nav = np.asarray(nav, dtype=float)
    nav = nav[np.isfinite(nav) & (nav > 0)]
    ret = returns_from_nav(nav)
    if len(ret) < 21:
        return None

    m: dict[str, Any] = {}

    for label, days in WINDOWS.items():
        if len(ret) >= days:
            cvar, var = cvar_var_95(ret[-days:])
            m[f"cvar_95_{label}"] = _clip(cvar)
            m[f"var_95_{label}"] = _clip(var)

    m["return_1m"] = _clip(cum_return(nav, 21))
    m["return_3m"] = _clip(cum_return(nav, 63))
    m["return_6m"] = _clip(cum_return(nav, 126))
    m["return_1y"] = _clip(cum_return(nav, 252))
    m["return_3y_ann"] = _clip(annualized_return(nav, 3))
    m["return_5y_ann"] = _clip(annualized_return(nav, 5), 8)
    m["return_10y_ann"] = _clip(annualized_return(nav, 10), 8)

    m["volatility_1y"] = _clip(volatility(ret, 252))
    m["max_drawdown_1y"] = _clip(max_drawdown(ret, 252))
    m["max_drawdown_3y"] = _clip(max_drawdown(ret, 3 * 252))

    m["sharpe_1y"] = _clip(sharpe(ret, 252, rf))
    m["sharpe_3y"] = _clip(sharpe(ret, 3 * 252, rf))
    m["sortino_1y"] = _clip(sortino(ret, 252, rf))

    mdd3 = m.get("max_drawdown_3y")
    r3 = m.get("return_3y_ann")
    if mdd3 is not None and r3 is not None and mdd3 < 0:
        m["calmar_ratio_3y"] = _clip(r3 / abs(mdd3), 4)

    gvol, gmodel = garch_or_ewma(ret)
    m["volatility_garch"] = _clip(gvol)
    m["vol_model"] = gmodel

    m.update(cornish_fisher_sharpe(ret, rf))
    m.update(evt_tail(ret))

    m["fed_funds_rate_at_calc"] = _clip(rf, 4)
    m["data_quality_flags"] = (
        {"return_rejected_count": rejected} if rejected else {}
    )
    return m


# ──────────────────────────────────────────────────────────────────────────────
# I/O helpers
# ──────────────────────────────────────────────────────────────────────────────
def _resolve_calc_date(conn, calc_date: str | None) -> _dt.date:
    if calc_date:
        return _dt.date.fromisoformat(calc_date)
    with conn.cursor() as cur:
        cur.execute("SELECT max(nav_date) FROM nav_timeseries")
        row = cur.fetchone()
    return row[0] if row and row[0] else _dt.date.today()


def _fetch_fund_ids(conn, calc_date: _dt.date, limit: int | None) -> list:
    sql = """
        SELECT instrument_id
        FROM nav_timeseries
        WHERE nav_date <= %s AND nav IS NOT NULL
        GROUP BY instrument_id
        HAVING count(*) >= 21
        ORDER BY count(*) DESC
    """
    params: list[Any] = [calc_date]
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [r[0] for r in cur.fetchall()]


def _fetch_nav(conn, instrument_id, calc_date: _dt.date, lookback_years: int = 11):
    start = calc_date - _dt.timedelta(days=lookback_years * 366)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT nav_date, nav FROM nav_timeseries
               WHERE instrument_id = %s AND nav_date <= %s AND nav_date >= %s
                 AND nav IS NOT NULL
               ORDER BY nav_date""",
            (instrument_id, calc_date, start),
        )
        rows = cur.fetchall()
    return rows


def _risk_free_rate(conn, calc_date: _dt.date) -> float:
    """Fed Funds (DFF) as of calc_date from macro_data; fallback 4%."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT value FROM macro_data
                   WHERE series_id = 'DFF' AND obs_date <= %s
                   ORDER BY obs_date DESC LIMIT 1""",
                (calc_date,),
            )
            row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0]) / 100.0
    except Exception:
        pass
    return RISK_FREE_FALLBACK


# ── Fixed-income style regressions (ported from the legacy quant engine) ──────
# Empirical duration = −beta of fund daily returns on Δ DGS10 (10y Treasury
# yield); credit beta = −beta on Δ BAA10Y (Baa−10y credit spread). OLS over the
# date-aligned overlap, last FI_REG_WINDOW obs, kept only when the fit clears
# FI_MIN_R2 — funds with no rate/credit sensitivity (e.g. pure equity) fail the
# R² gate and report None.
FI_YIELD_SERIES = "DGS10"
FI_CREDIT_SERIES = "BAA10Y"
FI_MIN_OBS = 120  # ~6 months of daily data
FI_REG_WINDOW = 504  # 2 years (2 × 252)
FI_MIN_R2 = 0.05
_MACRO_LOOKBACK_DAYS = 1000  # > FI_REG_WINDOW sessions + holiday/gap slack


def _fetch_macro_changes(
    conn, calc_date: _dt.date, lookback_days: int = _MACRO_LOOKBACK_DAYS
) -> dict[str, dict[_dt.date, float]]:
    """{series_id: {date: daily Δ}} for the FI factor series from macro_data.

    The FI regressions use the first difference of the level series (DGS10 yield,
    BAA10Y spread). Read ONCE in the main process and passed to the shards (like
    the benchmark returns), so children never re-fetch shared data.
    """
    out: dict[str, dict[_dt.date, float]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """SELECT series_id, obs_date, value FROM macro_data
               WHERE series_id = ANY(%s) AND obs_date <= %s AND obs_date > %s
               ORDER BY series_id, obs_date""",
            (
                [FI_YIELD_SERIES, FI_CREDIT_SERIES],
                calc_date,
                calc_date - _dt.timedelta(days=lookback_days),
            ),
        )
        rows = cur.fetchall()
    prev_series: str | None = None
    prev_val: float | None = None
    for series, d, val in rows:
        val = float(val)
        if series != prev_series:
            prev_series, prev_val = series, val
            out.setdefault(series, {})
            continue
        if prev_val is not None:
            out[series][d] = val - prev_val
        prev_val = val
    return out


def _ols_beta_r2(y: np.ndarray, x: np.ndarray) -> tuple[float, float]:
    """OLS ``y = a + b·x`` → ``(beta, r_squared)`` (legacy FI convention)."""
    mat = np.column_stack([np.ones(len(x)), x])
    coeffs = np.linalg.lstsq(mat, y, rcond=None)[0]
    beta = float(coeffs[1])
    resid = y - mat @ coeffs
    ss_res = float(np.sum(resid**2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return beta, r2


def _fi_factor_beta(
    fund_by_date: dict[_dt.date, float], change_by_date: dict[_dt.date, float]
) -> float | None:
    """−beta of fund returns on a factor's daily change; None if weak/insufficient."""
    common = sorted(fund_by_date.keys() & change_by_date.keys())
    if len(common) < FI_MIN_OBS:
        return None
    common = common[-FI_REG_WINDOW:]
    y = np.array([fund_by_date[d] for d in common], dtype=float)
    x = np.array([change_by_date[d] for d in common], dtype=float)
    if not np.isfinite(y).all() or not np.isfinite(x).all():
        return None
    if float(np.var(x)) == 0.0:
        return None
    beta, r2 = _ols_beta_r2(y, x)
    if r2 < FI_MIN_R2:
        return None
    return -beta


def fi_style_metrics(
    fund_ret_dated: list[tuple[_dt.date, float]],
    macro_changes: dict[str, dict[_dt.date, float]],
) -> dict[str, float | None]:
    """empirical_duration (vs Δ DGS10) and credit_beta (vs Δ BAA10Y)."""
    out: dict[str, float | None] = {"empirical_duration": None, "credit_beta": None}
    if not macro_changes:
        return out
    fund_by_date = dict(fund_ret_dated)
    yld = macro_changes.get(FI_YIELD_SERIES)
    crd = macro_changes.get(FI_CREDIT_SERIES)
    if yld:
        out["empirical_duration"] = _clip(_fi_factor_beta(fund_by_date, yld), 6)
    if crd:
        out["credit_beta"] = _clip(_fi_factor_beta(fund_by_date, crd), 6)
    return out


def relative_metrics_for(
    rows: list,
    fund_block: str | None,
    bench_returns: dict[str, list[tuple[_dt.date, float]]],
    rf: float,
    macro_changes: dict[str, dict[_dt.date, float]] | None = None,
) -> dict[str, float | None]:
    """Métricas benchmark-relativas de um fundo a partir dos NAV rows.

    ``rows`` é a saída de ``_fetch_nav`` ((date, nav) ascendente). Sem bloco
    mapeado, só a correlação com o benchmark de equity é computada. Com
    ``macro_changes``, também computa as regressões FI (duration/credit).
    """
    fund_ret = dated_simple_returns([(r[0], r[1]) for r in rows])
    out: dict[str, float | None] = {}
    if fund_block and fund_block in bench_returns:
        out.update(regression_metrics(fund_ret, bench_returns[fund_block], rf))
    eq = bench_returns.get(EQUITY_BENCHMARK_BLOCK)
    if eq:
        out["equity_correlation_252d"] = equity_correlation(fund_ret, eq)
    if macro_changes:
        out.update(fi_style_metrics(fund_ret, macro_changes))
    return out


def _upsert(conn, instrument_id, calc_date: _dt.date, metrics: dict[str, Any]) -> None:
    import json

    cols = ["instrument_id", "calc_date", "organization_id"]
    vals: list[Any] = [instrument_id, calc_date, None]
    for c in _METRIC_COLUMNS:
        if c not in metrics:
            continue
        cols.append(c)
        v = metrics[c]
        vals.append(json.dumps(v) if c == "data_quality_flags" else v)
    placeholders = ", ".join(["%s"] * len(cols))
    update_cols = [c for c in cols if c not in ("instrument_id", "calc_date", "organization_id")]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO fund_risk_metrics ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT (instrument_id, calc_date, organization_id) DO UPDATE SET {set_clause}"
    )
    with conn.cursor() as cur:
        cur.execute(sql, vals)


# ──────────────────────────────────────────────────────────────────────────────
# Sharded execution (process-level parallelism)
# ──────────────────────────────────────────────────────────────────────────────
def _resolve_max_workers() -> int:
    """min(cpu_count, 24): 24 on Railway, 32→24 local; bounds the cloud pool."""
    return min(os.cpu_count() or 4, MAX_WORKERS_CAP)


def _process_shard(
    dsn: str,
    calc_date_iso: str,
    rf: float,
    fund_ids: list,
    fund_benchmarks: dict[str, str],
    bench_returns: dict[str, list[tuple[_dt.date, float]]],
    macro_changes: dict[str, dict[_dt.date, float]] | None = None,
) -> tuple[int, int]:
    """Worker entrypoint for a child process — picklable, module-level.

    Opens its OWN connection (never shared across processes), computes and
    upserts its shard in its own transaction, and returns (processed, upserted).
    Does NOT take the advisory lock — the lock is held once by the main process
    for the whole run. ``rf``, ``calc_date``, the fund→benchmark map and the
    benchmark return series are passed in (read once in main) so children
    never re-fetch shared data.
    """
    cdate = _dt.date.fromisoformat(calc_date_iso)
    processed = 0
    upserted = 0
    with connect(dsn) as conn:
        for iid in fund_ids:
            rows = _fetch_nav(conn, iid, cdate)
            if len(rows) < 22:
                continue
            nav = np.array([float(r[1]) for r in rows], dtype=float)
            metrics = compute_metrics(nav, rf)
            processed += 1
            if metrics is None:
                continue
            metrics.update(
                relative_metrics_for(
                    rows, fund_benchmarks.get(str(iid)), bench_returns, rf, macro_changes
                )
            )
            _upsert(conn, iid, cdate, metrics)
            upserted += 1
        conn.commit()
    return processed, upserted


def _shard(fund_ids: list, n_shards: int) -> list[list]:
    """Round-robin fund_ids into ``n_shards`` balanced buckets.

    Round-robin (not contiguous chunks) keeps each shard's NAV-history mix
    similar — funds are ordered by history length, so chunking would pile the
    heavy (long-history, slow-GARCH) funds into the first shards.
    """
    shards: list[list] = [[] for _ in range(n_shards)]
    for i, iid in enumerate(fund_ids):
        shards[i % n_shards].append(iid)
    return [s for s in shards if s]


# Peer/scoring layer — percent_rank (0–100) dentro do peer_strategy_label.
# Semântica verificada empiricamente contra a DB-mãe (2026-06-12): ASC para
# sharpe/sortino/return (maior = melhor) e ASC para max_drawdown (valores
# negativos; menos negativo = melhor → pctl maior). peer_count = tamanho do
# grupo no calc_date. Labels: último proposed_strategy_label por instrumento
# em strategy_reclassification_stage (réplica cloud). Fundos sem label ficam
# com peer_* NULL. manager_score/elite_flag NÃO são computados aqui (modelo
# de scoring do allocation ainda não portado).
_PEER_PERCENTILES_SQL = """
WITH labels AS (
    SELECT DISTINCT ON (source_pk)
           source_pk::uuid AS instrument_id,
           proposed_strategy_label AS label
    FROM strategy_reclassification_stage
    WHERE source_table = 'instruments_universe'
      AND proposed_strategy_label IS NOT NULL
    ORDER BY source_pk, classified_at DESC
),
latest AS (
    SELECT m.instrument_id, m.sharpe_1y, m.sortino_1y, m.return_1y,
           m.max_drawdown_1y, l.label
    FROM fund_risk_metrics m
    JOIN labels l ON l.instrument_id = m.instrument_id
    WHERE m.calc_date = %(calc_date)s AND m.organization_id IS NULL
),
counts AS (
    SELECT label, count(*) AS peer_count FROM latest GROUP BY label
),
sharpe AS (
    SELECT instrument_id, round((percent_rank() OVER (
        PARTITION BY label ORDER BY sharpe_1y))::numeric * 100, 2) AS p
    FROM latest WHERE sharpe_1y IS NOT NULL
),
sortino AS (
    SELECT instrument_id, round((percent_rank() OVER (
        PARTITION BY label ORDER BY sortino_1y))::numeric * 100, 2) AS p
    FROM latest WHERE sortino_1y IS NOT NULL
),
ret AS (
    SELECT instrument_id, round((percent_rank() OVER (
        PARTITION BY label ORDER BY return_1y))::numeric * 100, 2) AS p
    FROM latest WHERE return_1y IS NOT NULL
),
dd AS (
    SELECT instrument_id, round((percent_rank() OVER (
        PARTITION BY label ORDER BY max_drawdown_1y))::numeric * 100, 2) AS p
    FROM latest WHERE max_drawdown_1y IS NOT NULL
)
UPDATE fund_risk_metrics m
SET peer_strategy_label = lt.label,
    peer_sharpe_pctl    = s.p,
    peer_sortino_pctl   = so.p,
    peer_return_pctl    = r.p,
    peer_drawdown_pctl  = d.p,
    peer_count          = c.peer_count
FROM latest lt
JOIN counts c ON c.label = lt.label
LEFT JOIN sharpe s ON s.instrument_id = lt.instrument_id
LEFT JOIN sortino so ON so.instrument_id = lt.instrument_id
LEFT JOIN ret r ON r.instrument_id = lt.instrument_id
LEFT JOIN dd d ON d.instrument_id = lt.instrument_id
WHERE m.instrument_id = lt.instrument_id
  AND m.calc_date = %(calc_date)s
  AND m.organization_id IS NULL
"""


def _update_peer_percentiles(conn, calc_date: _dt.date) -> int:
    """Set-based peer-percentile refresh for one calc_date; returns rows updated.

    Does NOT commit — the caller owns the transaction (run() commits; tests
    roll back).
    """
    with conn.cursor() as cur:
        cur.execute(_PEER_PERCENTILES_SQL, {"calc_date": calc_date})
        return cur.rowcount


def _refresh_fund_risk_latest_mv(dsn: str) -> None:
    """Refresh the API read-model MV (``fund_risk_latest_mv``) after a run.

    The Light's FastAPI serves the fund catalogue from this MATERIALIZED VIEW
    over ``fund_risk_metrics`` (latest calc per fund); it is stale until
    refreshed. Per docs/INGESTION_DESIGN.md, matview refreshes run
    ``REFRESH … CONCURRENTLY`` in a **fresh connection outside the advisory
    lock**: CONCURRENTLY cannot run inside a transaction block (needs
    autocommit) and requires the MV's UNIQUE index (``fund_risk_latest_mv_pk``)
    — both hold here. Called by ``run()`` only after the lock is released.
    """
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY fund_risk_latest_mv")


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
    serial: bool = False,
) -> dict:
    """Recompute fund_risk_metrics from raw NAV and upsert into the cloud.

    The MAIN process takes the LOCK_RISK_METRICS advisory lock ONCE, resolves
    ``calc_date``, reads the shared risk-free rate, and lists the target funds.
    It then splits the funds into shards and dispatches them to a pool of worker
    processes (``min(cpu_count, 24)``), each opening its OWN connection and
    upserting its shard idempotently. Results are identical to the serial path —
    same math, just distributed. Pass ``serial=True`` to force the single-process
    path (used for benchmarking / equivalence checks).

    Returns ``{"processed", "upserted", "calc_date", "workers"}``.
    """
    # The MAIN process holds the advisory lock for the WHOLE run (children never
    # lock). We dispatch the pool INSIDE this context so the lock spans the run.
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_RISK_METRICS) as got:
            if not got:
                return {"processed": 0, "upserted": 0, "skipped": "lock_busy"}

            cdate = _resolve_calc_date(conn, calc_date)
            rf = _risk_free_rate(conn, cdate)
            fund_ids = _fetch_fund_ids(conn, cdate, limit)
            cdate_iso = cdate.isoformat()
            # Benchmark wiring: lido UMA vez no main e passado aos shards.
            bench_returns = _fetch_benchmark_returns(conn, cdate)
            fund_benchmarks = _fetch_fund_benchmarks(conn)
            # FI factor changes (Δ DGS10 / Δ BAA10Y) — read once, shared like benches.
            macro_changes = _fetch_macro_changes(conn, cdate)

            n_workers = 1 if serial else min(_resolve_max_workers(), len(fund_ids) or 1)

            # Serial path (single process) — for benchmarking / equivalence.
            if n_workers <= 1:
                processed = upserted = 0
                for iid in fund_ids:
                    rows = _fetch_nav(conn, iid, cdate)
                    if len(rows) < 22:
                        continue
                    nav = np.array([float(r[1]) for r in rows], dtype=float)
                    metrics = compute_metrics(nav, rf)
                    processed += 1
                    if metrics is None:
                        continue
                    metrics.update(
                        relative_metrics_for(
                            rows,
                            fund_benchmarks.get(str(iid)),
                            bench_returns,
                            rf,
                            macro_changes,
                        )
                    )
                    _upsert(conn, iid, cdate, metrics)
                    upserted += 1
                peers = _update_peer_percentiles(conn, cdate)
                conn.commit()
                result = {
                    "processed": processed,
                    "upserted": upserted,
                    "peer_rows": peers,
                    "calc_date": cdate_iso,
                    "workers": 1,
                }
            else:
                # Parallel path: shard funds, dispatch to a process pool. Each
                # child opens its own connection and commits its own shard.
                shards = _shard(fund_ids, n_workers)
                processed = upserted = 0
                with ProcessPoolExecutor(max_workers=n_workers) as pool:
                    futures = [
                        pool.submit(
                            _process_shard,
                            dsn,
                            cdate_iso,
                            rf,
                            shard,
                            {str(i): b for i in shard if (b := fund_benchmarks.get(str(i)))},
                            bench_returns,
                            macro_changes,
                        )
                        for shard in shards
                    ]
                    for fut in as_completed(futures):
                        p, u = fut.result()
                        processed += p
                        upserted += u

                # Children committed their shards; rank peers over the full set.
                peers = _update_peer_percentiles(conn, cdate)
                conn.commit()

                result = {
                    "processed": processed,
                    "upserted": upserted,
                    "peer_rows": peers,
                    "calc_date": cdate_iso,
                    "workers": n_workers,
                }

    # Lock released and the main connection is closed. Refresh the API read-model
    # MV in a FRESH autocommit connection, OUTSIDE the advisory lock. The metrics
    # are already committed and idempotent, so a refresh hiccup must not discard
    # them — surface it in the stats (printed as JSON by the runner) instead of
    # silently swallowing or failing the committed run.
    try:
        _refresh_fund_risk_latest_mv(dsn)
        result["mv_refreshed"] = True
    except Exception as exc:  # noqa: BLE001 — surface, don't discard committed work
        result["mv_refreshed"] = False
        result["mv_refresh_error"] = str(exc)
    return result
