"""factor_model — worker IPCA (Instrumented Principal Components Analysis).

Tabela destino : factor_model_fits  (GLOBAL, sem RLS)
Advisory lock  : LOCK_FACTOR_MODEL (900_203)
Frequência     : trimestral / on-demand
Idempotente    : sim — upsert por chave natural
                 (engine, asset_class, universe_hash, fit_date).

==============================================================================
MODELO IPCA (Kelly, Pruitt & Su 2019) — implementação STANDALONE
==============================================================================
O IPCA modela o retorno do ativo i no período t como exposição a K fatores
latentes f_t, onde os *betas* (loadings) NÃO são livres mas instrumentados
pelas características observáveis do ativo z_{i,t-1} (L características) via uma
matriz de mapeamento Gamma (L x K), comum a todos os ativos e estável no tempo:

        r_{i,t} = beta_{i,t-1}' f_t + e_{i,t}
        beta_{i,t-1} = Gamma' z_{i,t-1}          (Gamma: L x K)
   =>   r_{i,t} = (z_{i,t-1}' Gamma) f_t + e_{i,t}

Estimação por ALS (alternating least squares) minimizando
   sum_t || r_t - Z_t Gamma f_t ||^2
alternando entre dois passos fechados (managed-portfolio / GMM form, KP-S 2019):

  Passo-F (dado Gamma) — corte transversal de cada período t:
      beta_t = Z_t Gamma                  (N_t x K)
      f_t = (beta_t' beta_t)^{-1} beta_t' r_t        (mínimos quadrados)

  Passo-Gamma (dado todos os f_t) — sistema linear vetorizado em vec(Gamma):
      Para cada t, x_{i,t} = z_{i,t-1} ⊗ f_t  (L*K vetor). Empilhando todos os
      (i,t):  vec(Gamma) = ( sum_{i,t} x x' )^{-1} ( sum_{i,t} x r_{i,t} ).
      Equivalente à forma de produto de Kronecker de KP-S:
          A = sum_t (Z_t'Z_t) ⊗ (f_t f_t');  b = sum_t vec(Z_t' r_t f_t')
          vec(Gamma) = A^{-1} b

  Identificação: Gamma e F são identificados a menos de rotação/escala. Após a
  convergência normalizamos para a convenção KP-S: colunas de Gamma
  ortonormais (Gamma'Gamma = I_K via QR) e os fatores rotacionados de modo que
  cov(F) seja diagonal decrescente (autovetores de F F'). Isto torna o fit
  comparável entre runs (drift) — sem alterar o subespaço ajustado nem o R².

Convergência: ||Gamma_new - Gamma_old||_F < tol (após realinhar sinal das
colunas) ou max_iter atingido.

R² in-sample      : 1 - SS_res / SS_tot sobre todos os (i,t).
R² out-of-sample  : validação walk-forward (treino expansível / teste à frente).
   Para cada janela, ajusta Gamma no treino, estima f_t no teste resolvendo o
   corte transversal com o Gamma de TREINO (fatores não vazam: são reestimados
   a partir das características de teste e do próprio Gamma), e mede
   1 - SS_res/SS_tot fora da amostra. Reporta a média das janelas.

Dependência canônica das características: a versão recalculada de
`equity_characteristics_monthly` no TimescaleDB Cloud (em reconstrução por
outro agente). Enquanto a versão cloud não existir, lê do DB-mãe legado
(localhost:5434) — ver `_load_panel`. Os retornos vêm de `nav_timeseries`
(retornos compostos mensais a partir do NAV bruto), nunca de tabelas de métrica.
==============================================================================
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from src.db import LOCK_FACTOR_MODEL, advisory_lock, connect

ENGINE = "ipca"
ASSET_CLASS = "Equity"

# Características-instrumento (ordem fixa = ordem das linhas de gamma_loadings).
CHARS_COLS = [
    "size_log_mkt_cap",
    "book_to_market",
    "mom_12_1",
    "quality_roa",
    "investment_growth",
    "profitability_gross",
]

# DB-mãe legado (fallback de características enquanto o cloud não tem a tabela
# recalculada). Read-only, apenas para o caminho de teste/validação.
_LEGACY_DSN = (
    "host=localhost port=5434 dbname=investintell_alloc "
    "user=investintell password=investintell"
)


# --------------------------------------------------------------------------- #
# Pré-processamento (KP-S 2019): rank cross-seccional -> [-0.5, +0.5]
# --------------------------------------------------------------------------- #
def rank_transform(chars: pd.DataFrame) -> pd.DataFrame:
    """Rank transversal por período, reescalado a [-0.5, +0.5].

    Ranqueia cada característica dentro de cada corte transversal (nível de
    tempo do MultiIndex (instrument_id, month)) e reescala. Robusto a outliers
    (book_to_market, investment_growth). Per-period => sem vazamento entre
    janelas de treino/teste separadas por data.
    """
    return chars.groupby(level="month").transform(lambda g: g.rank(pct=True) - 0.5)


# --------------------------------------------------------------------------- #
# Núcleo ALS do IPCA (standalone, numpy puro)
# --------------------------------------------------------------------------- #
def _panel_arrays(
    chars: pd.DataFrame, returns: pd.Series
) -> tuple[list[np.ndarray], list[np.ndarray], list[Any]]:
    """Quebra o painel (MultiIndex instrument×month) em listas por período.

    Retorna (Zs, rs, dates): Zs[t] é N_t x L, rs[t] é N_t, dates[t] o mês.
    """
    Zs: list[np.ndarray] = []
    rs: list[np.ndarray] = []
    dates: list[Any] = []
    for dt, sub in chars.groupby(level="month"):
        idx = sub.index
        Zs.append(np.asarray(sub.values, dtype=np.float64))
        rs.append(np.asarray(returns.loc[idx].values, dtype=np.float64))
        dates.append(dt)
    return Zs, rs, dates


def _estimate_factors(Z: np.ndarray, r: np.ndarray, gamma: np.ndarray) -> np.ndarray:
    """Passo-F: f_t = (beta'beta)^{-1} beta' r  com beta = Z @ gamma."""
    beta = Z @ gamma  # N_t x K
    a = beta.T @ beta  # K x K
    b = beta.T @ r  # K
    # Resolve via lstsq para robustez quando a é quase-singular.
    f, *_ = np.linalg.lstsq(a, b, rcond=None)
    return f


def _estimate_gamma(
    Zs: list[np.ndarray], rs: list[np.ndarray], fs: list[np.ndarray], L: int, K: int
) -> np.ndarray:
    """Passo-Gamma vetorizado (forma de Kronecker KP-S).

    A = sum_t (Z_t'Z_t) ⊗ (f_t f_t');  b = sum_t vec(Z_t' r_t f_t')
    vec(Gamma) = A^{-1} b.  Retorna Gamma (L x K).
    """
    A = np.zeros((L * K, L * K), dtype=np.float64)
    b = np.zeros(L * K, dtype=np.float64)
    for Z, r, f in zip(Zs, rs, fs):
        ZtZ = Z.T @ Z  # L x L
        ff = np.outer(f, f)  # K x K
        A += np.kron(ZtZ, ff)
        # vec(Z' r f') : Z'r é L, f' é K -> outer -> L x K, vec (Fortran/column).
        Ztr = Z.T @ r  # L
        b += np.kron(Ztr, f)  # L*K, consistente com kron acima
    vec_gamma, *_ = np.linalg.lstsq(A, b, rcond=None)
    # vec usa ordem de Kronecker (linha-bloco L, sub-bloco K) -> reshape L x K.
    return vec_gamma.reshape(L, K)


def _normalize(gamma: np.ndarray, fs: list[np.ndarray]) -> tuple[np.ndarray, list[np.ndarray]]:
    """Convenção de identificação KP-S: Gamma'Gamma = I_K, cov(F) diagonal desc.

    1) QR de Gamma -> colunas ortonormais; absorve R nos fatores.
    2) Rotaciona pelo autovetores de F F' (PCA dos fatores) p/ ordenar variância.
    Não altera o subespaço/ajuste (Gamma f = (Gamma Q)(Q' f)).
    """
    K = gamma.shape[1]
    F = np.column_stack(fs)  # K x T
    # Passo 1: ortonormaliza Gamma.
    Q, R = np.linalg.qr(gamma)  # Q: L x K ortonormal, R: K x K
    gamma_o = Q
    F = R @ F  # fatores absorvem R
    # Passo 2: rotaciona para diagonalizar cov dos fatores (variância desc).
    cov = F @ F.T  # K x K
    w, V = np.linalg.eigh(cov)
    order = np.argsort(w)[::-1]
    V = V[:, order]
    gamma_o = gamma_o @ V
    F = V.T @ F
    # Convenção de sinal: primeira entrada não-nula de cada coluna de Gamma >= 0.
    for k in range(K):
        col = gamma_o[:, k]
        nz = col[np.abs(col) > 1e-12]
        if nz.size and nz[0] < 0:
            gamma_o[:, k] *= -1
            F[k, :] *= -1
    fs_norm = [F[:, t] for t in range(F.shape[1])]
    return gamma_o, fs_norm


def fit_ipca(
    chars: pd.DataFrame,
    returns: pd.Series,
    K: int,
    *,
    max_iter: int = 200,
    tol: float = 1e-6,
) -> dict[str, Any]:
    """Ajusta o IPCA por ALS. Painel: MultiIndex (instrument_id, month).

    Retorna dict com gamma (L x K), factor_returns (K x T), dates (lista),
    r_squared, converged, n_iterations.
    """
    Zs, rs, dates = _panel_arrays(chars, returns)
    L = chars.shape[1]
    T = len(Zs)

    # Inicialização de Gamma: PCA das características-instrumento agregadas.
    # Z_all (sum Z'Z) autovetores dominantes -> chute estável e determinístico.
    ZtZ_all = sum(Z.T @ Z for Z in Zs)
    w, V = np.linalg.eigh(ZtZ_all)
    gamma = V[:, np.argsort(w)[::-1][:K]].copy()  # L x K

    converged = False
    n_iter = 0
    for n_iter in range(1, max_iter + 1):
        fs = [_estimate_factors(Z, r, gamma) for Z, r in zip(Zs, rs)]
        gamma_new = _estimate_gamma(Zs, rs, fs, L, K)
        # Realinha sinal coluna-a-coluna antes de medir a mudança.
        for k in range(K):
            if np.dot(gamma_new[:, k], gamma[:, k]) < 0:
                gamma_new[:, k] *= -1
        delta = np.linalg.norm(gamma_new - gamma)
        gamma = gamma_new
        if delta < tol:
            converged = True
            break

    # Fatores finais e normalização de identificação.
    fs = [_estimate_factors(Z, r, gamma) for Z, r in zip(Zs, rs)]
    gamma, fs = _normalize(gamma, fs)

    # R² in-sample.
    ss_res = 0.0
    ss_tot = 0.0
    for Z, r, f in zip(Zs, rs, fs):
        pred = (Z @ gamma) @ f
        ss_res += float(np.sum((r - pred) ** 2))
        ss_tot += float(np.sum(r**2))
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    factor_returns = np.column_stack(fs) if fs else np.zeros((K, 0))  # K x T
    return {
        "gamma": gamma,
        "factor_returns": factor_returns,
        "dates": dates,
        "K": K,
        "r_squared": r_squared,
        "converged": converged,
        "n_iterations": n_iter,
        "T": T,
    }


def oos_r_squared(
    chars: pd.DataFrame,
    returns: pd.Series,
    K: int,
    *,
    min_train: int = 24,
    test_window: int = 12,
    max_iter: int = 100,
) -> float | None:
    """R² out-of-sample por walk-forward expansível.

    Treina Gamma em [0, i); estima f_t por corte transversal NO teste usando o
    Gamma de treino (fatores reestimados, não vazados) e mede R² fora-da-amostra.
    Retorna a média das janelas, ou None se não houver dados suficientes.
    """
    all_dates = sorted(chars.index.get_level_values("month").unique())
    n = len(all_dates)
    if n < min_train + test_window:
        return None

    scores: list[float] = []
    i = min_train
    while i + test_window <= n:
        train_dates = set(all_dates[:i])
        test_dates = all_dates[i : i + test_window]

        tr_mask = chars.index.get_level_values("month").isin(train_dates)
        tr_chars = chars[tr_mask]
        tr_ret = returns[tr_mask]
        if tr_chars.empty:
            i += test_window
            continue

        fit = fit_ipca(tr_chars, tr_ret, K, max_iter=max_iter)
        gamma = fit["gamma"]

        ss_res = 0.0
        ss_tot = 0.0
        for dt in test_dates:
            m = chars.index.get_level_values("month") == dt
            Z = np.asarray(chars[m].values, dtype=np.float64)
            r = np.asarray(returns[m].values, dtype=np.float64)
            if r.size == 0:
                continue
            f = _estimate_factors(Z, r, gamma)
            pred = (Z @ gamma) @ f
            ss_res += float(np.sum((r - pred) ** 2))
            ss_tot += float(np.sum(r**2))
        if ss_tot > 0:
            scores.append(1.0 - ss_res / ss_tot)
        i += test_window

    return float(np.mean(scores)) if scores else None


# --------------------------------------------------------------------------- #
# Carregamento do painel
# --------------------------------------------------------------------------- #
_PANEL_SQL = """
SELECT
    e.instrument_id::text AS instrument_id,
    date_trunc('month', e.as_of)::date AS month,
    e.size_log_mkt_cap, e.book_to_market, e.mom_12_1,
    e.quality_roa, e.investment_growth, e.profitability_gross,
    -- retorno mensal composto a partir do NAV bruto (nav_open -> nav_close)
    (m.nav_close / NULLIF(m.nav_open, 0) - 1.0) AS monthly_return
FROM equity_characteristics_monthly e
JOIN nav_monthly_returns_agg m
  ON m.instrument_id = e.instrument_id
 AND m.month = date_trunc('month', e.as_of)::date
WHERE e.as_of <= %(asof)s
"""

# Versão cloud-canônica (quando as tabelas recalculadas existirem): retornos
# direto de nav_timeseries (NAV bruto), características da versão recalculada.
_PANEL_SQL_CLOUD = """
WITH monthly AS (
    SELECT
        instrument_id,
        date_trunc('month', nav_date)::date AS month,
        (last(nav, nav_date) / NULLIF(first(nav, nav_date), 0) - 1.0) AS monthly_return
    FROM nav_timeseries
    WHERE nav_date <= %(asof)s
    GROUP BY instrument_id, date_trunc('month', nav_date)
)
SELECT
    e.instrument_id::text AS instrument_id,
    date_trunc('month', e.as_of)::date AS month,
    e.size_log_mkt_cap, e.book_to_market, e.mom_12_1,
    e.quality_roa, e.investment_growth, e.profitability_gross,
    mo.monthly_return
FROM equity_characteristics_monthly e
JOIN monthly mo
  ON mo.instrument_id = e.instrument_id
 AND mo.month = date_trunc('month', e.as_of)::date
WHERE e.as_of <= %(asof)s
"""


def _has_table(conn: Any, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (name,))
        return cur.fetchone()[0] is not None


def _load_panel(conn: Any, asof: date) -> pd.DataFrame:
    """Carrega o painel retornos×características.

    Preferência: tabelas recalculadas no cloud (fonte canônica). Fallback:
    DB-mãe legado (localhost:5434) enquanto o cloud não tiver
    equity_characteristics_monthly recalculada.
    """
    if _has_table(conn, "equity_characteristics_monthly") and _has_table(
        conn, "nav_timeseries"
    ):
        with conn.cursor() as cur:
            cur.execute(_PANEL_SQL_CLOUD, {"asof": asof})
            cols = [c.name for c in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)

    # Fallback legado read-only.
    import psycopg

    with psycopg.connect(_LEGACY_DSN) as legacy, legacy.cursor() as cur:
        cur.execute(_PANEL_SQL, {"asof": asof})
        cols = [c.name for c in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _build_panel(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Limpa, indexa e rank-transforma. Retorna (chars, returns) alinhados."""
    for col in CHARS_COLS + ["monthly_return"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["month"] = pd.to_datetime(df["month"])
    df = df.dropna(subset=CHARS_COLS + ["monthly_return"])
    # Deduplica (instrument_id, month): a fonte pode trazer mais de uma linha
    # por par (ex.: múltiplas classes/observações de NAV). Mantém a última —
    # o painel IPCA exige no máximo 1 obs por ativo×período.
    df = df.drop_duplicates(subset=["instrument_id", "month"], keep="last")
    df = df.set_index(["instrument_id", "month"]).sort_index()
    chars = rank_transform(df[CHARS_COLS])
    returns = df["monthly_return"]
    return chars, returns


def _universe_hash(chars: pd.DataFrame) -> str:
    ids = sorted(chars.index.get_level_values("instrument_id").unique().tolist())
    return hashlib.md5(",".join(ids).encode()).hexdigest()[:16]


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
    k_factors: int = 1,
    min_panel_dates: int = 12,
) -> dict[str, Any]:
    """Recalcula o fit IPCA e faz upsert idempotente em factor_model_fits.

    Args:
        dsn: DSN do cloud (destino + fonte canônica).
        calc_date: data de cálculo (YYYY-MM-DD); default = hoje. Determinística.
        limit: nº máx. de instrumentos no universo (amostragem p/ runs leves).
        k_factors: K do IPCA (default 1, como o legado).
        min_panel_dates: nº mín. de meses distintos p/ ajustar.

    Returns:
        {processed, upserted, ...stats}.
    """
    asof = (
        datetime.strptime(calc_date, "%Y-%m-%d").date() if calc_date else date.today()
    )

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FACTOR_MODEL) as got:
            if not got:
                return {"status": "skipped", "reason": "lock_held", "processed": 0, "upserted": 0}

            raw = _load_panel(conn, asof)
            if raw.empty:
                return {"status": "no_data", "processed": 0, "upserted": 0}

            chars, returns = _build_panel(raw)

            if limit is not None:
                keep = sorted(chars.index.get_level_values("instrument_id").unique())[:limit]
                m = chars.index.get_level_values("instrument_id").isin(keep)
                chars, returns = chars[m], returns[m]

            n_dates = chars.index.get_level_values("month").nunique()
            n_inst = chars.index.get_level_values("instrument_id").nunique()
            if n_dates < min_panel_dates:
                return {
                    "status": "skipped",
                    "reason": "panel_too_small",
                    "n_dates": int(n_dates),
                    "n_instruments": int(n_inst),
                    "processed": int(len(chars)),
                    "upserted": 0,
                }

            fit = fit_ipca(chars, returns, k_factors)
            oos = oos_r_squared(chars, returns, k_factors)
            uhash = _universe_hash(chars)

            _upsert(conn, asof, uhash, fit, oos)
            conn.commit()

            return {
                "status": "succeeded",
                "processed": int(len(chars)),
                "upserted": 1,
                "k_factors": fit["K"],
                "n_instruments": int(n_inst),
                "n_dates": int(n_dates),
                "r_squared": fit["r_squared"],
                "oos_r_squared": oos,
                "converged": fit["converged"],
                "n_iterations": fit["n_iterations"],
                "universe_hash": uhash,
            }


def _upsert(conn: Any, asof: date, uhash: str, fit: dict[str, Any], oos: float | None) -> None:
    """Upsert idempotente por chave natural (engine, asset_class, hash, date)."""
    gamma_loadings = fit["gamma"].tolist()  # L x K
    dates_str = [
        (d.date() if isinstance(d, datetime) else pd.Timestamp(d).date()).isoformat()
        for d in fit["dates"]
    ]
    factor_returns_json = {
        "dates": dates_str,
        "values": fit["factor_returns"].tolist(),  # K x T
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO factor_model_fits (
                fit_id, engine, fit_date, universe_hash, asset_class, k_factors,
                gamma_loadings, factor_returns, oos_r_squared, converged, n_iterations
            ) VALUES (
                gen_random_uuid(), %(engine)s, %(fit_date)s, %(hash)s, %(asset_class)s,
                %(k)s, %(gamma)s::jsonb, %(f_returns)s::jsonb, %(oos)s, %(conv)s, %(n_iter)s
            )
            ON CONFLICT (engine, asset_class, universe_hash, fit_date)
            DO UPDATE SET
                k_factors      = EXCLUDED.k_factors,
                gamma_loadings = EXCLUDED.gamma_loadings,
                factor_returns = EXCLUDED.factor_returns,
                oos_r_squared  = EXCLUDED.oos_r_squared,
                converged      = EXCLUDED.converged,
                n_iterations   = EXCLUDED.n_iterations,
                created_at     = now()
            """,
            {
                "engine": ENGINE,
                "fit_date": asof,
                "hash": uhash,
                "asset_class": ASSET_CLASS,
                "k": fit["K"],
                "gamma": json.dumps(gamma_loadings),
                "f_returns": json.dumps(factor_returns_json),
                "oos": float(oos) if oos is not None else None,
                "conv": bool(fit["converged"]),
                "n_iter": int(fit["n_iterations"]),
            },
        )
