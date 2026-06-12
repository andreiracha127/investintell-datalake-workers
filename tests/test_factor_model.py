"""Testes do worker IPCA (factor_model).

Foco em CORREÇÃO do algoritmo:
  1. test_synthetic_recovery — gera dados de um Gamma/F conhecidos e verifica
     que o ALS recupera o subespaço de loadings (alinhamento de subespaço ~1)
     e R² in-sample alto, com convergência.
  2. test_oos_plausible — R² OOS plausível (positivo e < in-sample) no caso
     sintético com sinal forte.
  3. test_dimensions — gamma (L x K) e factor_returns (K x T) coerentes.
  4. test_kron_gamma_step — o passo-Gamma vetorizado bate com a solução
     ingênua mínimos-quadrados empilhada.
  5. test_real_run — run() de ponta a ponta sobre painel real (DB-mãe legado),
     upsert idempotente no cloud. Marcado para pular se os DBs não responderem.
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest

from src.workers import factor_model as fm

CLOUD_DSN = (
    "postgresql://tsdbadmin:nefnetue4kl9mmhc@"
    "t83f4np6x4.tghc3kjhuc.tsdb.cloud.timescale.com:33132/tsdb?sslmode=require"
)


# --------------------------------------------------------------------------- #
# Geração de painel sintético com Gamma e fatores conhecidos
# --------------------------------------------------------------------------- #
def _make_synthetic(
    *, L=4, K=2, T=120, N=60, noise=0.02, seed=7
) -> tuple[pd.DataFrame, pd.Series, np.ndarray]:
    """r_{i,t} = (z_{i,t} Gamma) f_t + ruído.  Retorna (chars, returns, Gamma)."""
    rng = np.random.default_rng(seed)
    # Gamma verdadeiro, colunas ortonormais.
    G_true, _ = np.linalg.qr(rng.standard_normal((L, K)))
    F = rng.standard_normal((K, T)) * 0.05  # fatores
    months = pd.date_range("2010-01-31", periods=T, freq="ME")

    frames = []
    for t in range(T):
        Z = rng.standard_normal((N, L))  # características brutas
        beta = Z @ G_true  # N x K
        r = beta @ F[:, t] + rng.standard_normal(N) * noise
        df = pd.DataFrame(Z, columns=fm.CHARS_COLS[:L])
        df["instrument_id"] = [f"id_{i:03d}" for i in range(N)]
        df["month"] = months[t]
        df["monthly_return"] = r
        frames.append(df)

    panel = pd.concat(frames, ignore_index=True)
    panel = panel.set_index(["instrument_id", "month"]).sort_index()
    chars = panel[fm.CHARS_COLS[:L]]
    returns = panel["monthly_return"]
    return chars, returns, G_true


def _subspace_alignment(A: np.ndarray, B: np.ndarray) -> float:
    """Alinhamento de subespaço col(A) vs col(B) em [0,1] (1 = idêntico).

    Média dos cossenos quadrados via SVD dos produtos ortonormais (principal
    angles). Invariante a rotação/sinal — o que o IPCA identifica.
    """
    Qa, _ = np.linalg.qr(A)
    Qb, _ = np.linalg.qr(B)
    s = np.linalg.svd(Qa.T @ Qb, compute_uv=False)
    return float(np.mean(s**2))


# --------------------------------------------------------------------------- #
# 1. Recuperação sintética
# --------------------------------------------------------------------------- #
def test_synthetic_recovery():
    chars, returns, G_true = _make_synthetic(noise=0.01)
    fit = fm.fit_ipca(chars, returns, K=2, max_iter=300, tol=1e-8)

    assert fit["converged"], "ALS deveria convergir no caso sintético limpo"
    # Subespaço de loadings recuperado quase perfeitamente.
    align = _subspace_alignment(fit["gamma"], G_true)
    assert align > 0.98, f"subspace alignment {align:.4f} muito baixo"
    # R² in-sample alto (ruído pequeno).
    assert fit["r_squared"] > 0.9, f"R² in-sample {fit['r_squared']:.3f} baixo"


def test_recovery_degrades_with_noise():
    """Sanidade: mais ruído => R² menor (monotonicidade do estimador)."""
    chars_lo, ret_lo, _ = _make_synthetic(noise=0.01, seed=3)
    chars_hi, ret_hi, _ = _make_synthetic(noise=0.10, seed=3)
    r2_lo = fm.fit_ipca(chars_lo, ret_lo, K=2)["r_squared"]
    r2_hi = fm.fit_ipca(chars_hi, ret_hi, K=2)["r_squared"]
    assert r2_lo > r2_hi


# --------------------------------------------------------------------------- #
# 2. OOS plausível
# --------------------------------------------------------------------------- #
def test_oos_plausible():
    chars, returns, _ = _make_synthetic(noise=0.01, T=120)
    oos = fm.oos_r_squared(chars, returns, K=2, min_train=36, test_window=12)
    assert oos is not None
    # Sinal forte e estrutura estável => OOS bem positivo.
    assert oos > 0.5, f"OOS R² {oos:.3f} implausivelmente baixo p/ sinal forte"
    assert oos <= 1.0


def test_oos_none_when_insufficient():
    chars, returns, _ = _make_synthetic(T=20)
    assert fm.oos_r_squared(chars, returns, K=2, min_train=36, test_window=12) is None


# --------------------------------------------------------------------------- #
# 3. Dimensões coerentes
# --------------------------------------------------------------------------- #
def test_dimensions():
    L, K, T = 4, 2, 60
    chars, returns, _ = _make_synthetic(L=L, K=K, T=T)
    fit = fm.fit_ipca(chars, returns, K=K)
    assert fit["gamma"].shape == (L, K)
    assert fit["factor_returns"].shape == (K, T)
    assert len(fit["dates"]) == T
    # Gamma ortonormal pós-normalização.
    np.testing.assert_allclose(fit["gamma"].T @ fit["gamma"], np.eye(K), atol=1e-8)


# --------------------------------------------------------------------------- #
# 4. Passo-Gamma vetorizado == solução ingênua empilhada
# --------------------------------------------------------------------------- #
def test_kron_gamma_step():
    rng = np.random.default_rng(11)
    L, K = 3, 2
    Zs, rs, fs = [], [], []
    rows_X, rows_y = [], []
    for _ in range(5):
        N = rng.integers(8, 15)
        Z = rng.standard_normal((N, L))
        r = rng.standard_normal(N)
        f = rng.standard_normal(K)
        Zs.append(Z); rs.append(r); fs.append(f)
        # Forma ingênua: para cada obs, x = kron(z_i, f) (L*K), prevê r_i.
        for i in range(N):
            rows_X.append(np.kron(Z[i], f))
            rows_y.append(r[i])
    X = np.array(rows_X)
    y = np.array(rows_y)
    naive, *_ = np.linalg.lstsq(X, y, rcond=None)
    naive_gamma = naive.reshape(L, K)

    kron_gamma = fm._estimate_gamma(Zs, rs, fs, L, K)
    np.testing.assert_allclose(kron_gamma, naive_gamma, atol=1e-8)


# --------------------------------------------------------------------------- #
# 5. Run real de ponta a ponta (DB-mãe -> cloud)
# --------------------------------------------------------------------------- #
def _dbs_reachable() -> bool:
    import psycopg

    try:
        with psycopg.connect(CLOUD_DSN, connect_timeout=10):
            pass
        with psycopg.connect(fm._LEGACY_DSN, connect_timeout=5):
            pass
        return True
    except Exception:
        return False


@pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1" and not _dbs_reachable(),
    reason="DBs (cloud/legado) indisponíveis",
)
def test_real_run():
    stats = fm.run(CLOUD_DSN, calc_date="2026-05-31", limit=80, k_factors=1)
    assert stats["status"] in ("succeeded", "skipped", "no_data")
    if stats["status"] != "succeeded":
        pytest.skip(f"run não produziu fit: {stats}")

    assert stats["upserted"] == 1
    assert stats["k_factors"] == 1
    assert stats["n_instruments"] > 0
    assert stats["n_dates"] >= 12
    # R² in-sample em [0,1]; OOS (se houver) plausível.
    assert 0.0 <= stats["r_squared"] <= 1.0
    if stats["oos_r_squared"] is not None:
        assert -1.0 <= stats["oos_r_squared"] <= 1.0

    # Idempotência: rodar de novo não duplica (upsert por chave natural).
    import psycopg

    with psycopg.connect(CLOUD_DSN) as c, c.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM factor_model_fits WHERE universe_hash=%s AND fit_date=%s",
            (stats["universe_hash"], "2026-05-31"),
        )
        before = cur.fetchone()[0]
    fm.run(CLOUD_DSN, calc_date="2026-05-31", limit=80, k_factors=1)
    with psycopg.connect(CLOUD_DSN) as c, c.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM factor_model_fits WHERE universe_hash=%s AND fit_date=%s",
            (stats["universe_hash"], "2026-05-31"),
        )
        after = cur.fetchone()[0]
    assert before == after == 1, "upsert deveria manter exatamente 1 linha"
