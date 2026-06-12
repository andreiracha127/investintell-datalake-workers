"""Tests for the credit_regime worker (Frente B — detector de stress de crédito).

Replica EXATA da mecânica validada no backtest QC
(`2026-06-11-macro-regime-backtest.md`, projeto MacroRegimeHYOnly,
backtest `856a7e9f643a8c44501456e6a328cd86`, Sharpe 0,481 / DD 25,7%):

  ratio = HYG_adjclose / IEF_adjclose
  janela móvel das 1260 observações ANTERIORES (exclui o dia corrente)
  p20 = sorted(janela)[min(n-1, int(0.20*(n-1)))], exige n >= 252
  risk_off  ⇔  p20 existe e ratio < p20   (binário, sem estado caution)

A histerese é estrutural: o percentil é móvel sobre uma janela que passa a
conter os próprios períodos de stress. O composite legado foi REFUTADO pelo
backtest — este worker implementa apenas o sinal de crédito.

Pure-engine tests run anywhere. The integration test fetches real Tiingo
prices and upserts into the cloud — it validates the detector against os
episódios históricos conhecidos do backtest (Lehman 2008 e COVID 2020
risk_off; 2022 sem disparo) and self-skips without credentials.
"""

from __future__ import annotations

import datetime as _dt
import os
import pathlib

import psycopg
import pytest

from src.db import LOCK_CREDIT_REGIME, advisory_lock
from src.workers import credit_regime as cr

D0 = _dt.date(2020, 1, 1)


def _days(n: int, start: _dt.date = D0) -> list[_dt.date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += _dt.timedelta(days=1)
    return out


def _series(dates, prices):
    return list(zip(dates, prices))


# ──────────────────────────────────────────────────────────────────────────────
# Percentil — replica exata do backtest
# ──────────────────────────────────────────────────────────────────────────────
def test_percentile20_matches_backtest_indexing():
    # backtest: vals = sorted(window); idx = min(n-1, int(0.20 * (n-1)))
    window = [float(v) for v in range(1260, 0, -1)]  # 1260..1, fora de ordem
    # n=1260 → idx = int(0.20*1259) = 251 → sorted[251] = 252
    assert cr.percentile_20(window) == 252.0
    # n=252 (mínimo exato) → idx = int(0.20*251) = 50 → sorted[50] = 51
    assert cr.percentile_20([float(v) for v in range(1, 253)]) == 51.0


def test_percentile20_requires_min_obs():
    assert cr.percentile_20(list(range(251))) is None  # < 252 → warmup


# ──────────────────────────────────────────────────────────────────────────────
# compute_regime — mecânica do detector
# ──────────────────────────────────────────────────────────────────────────────
def test_warmup_is_risk_on_with_null_threshold():
    dates = _days(100)
    rows = cr.compute_regime(
        _series(dates, [80.0] * 100), _series(dates, [100.0] * 100)
    )
    assert len(rows) == 100
    assert all(r["state"] == "risk_on" for r in rows)
    assert all(r["p20_5y"] is None for r in rows)
    assert all(r["flip"] is False for r in rows)


def test_stress_episode_flips_to_risk_off_and_back():
    n = 400
    dates = _days(n)
    ief = [100.0] * n
    # 300 dias estáveis (ratio 0.80), queda para 0.60 por 50 dias (stress),
    # recuperação a 0.80 — a comparação do detector é estrita (<), então o
    # platô constante nunca dispara sozinho
    hyg = [80.0] * 300 + [60.0] * 50 + [80.0] * 50
    rows = cr.compute_regime(_series(dates, hyg), _series(dates, ief))
    states = {r["regime_date"]: r["state"] for r in rows}
    assert states[dates[299]] == "risk_on"   # antes do stress
    assert states[dates[310]] == "risk_off"  # dentro do stress
    assert states[dates[390]] == "risk_on"   # recuperado
    flips = [r for r in rows if r["flip"]]
    assert len(flips) == 2  # entra e sai uma vez
    # provenance carregada nas linhas
    stress_row = next(r for r in rows if r["regime_date"] == dates[310])
    assert stress_row["ratio"] == pytest.approx(0.60)
    assert stress_row["p20_5y"] is not None
    assert stress_row["hyg_close"] == pytest.approx(60.0)


def test_window_excludes_current_day():
    """O p20 usa só as observações ANTERIORES (como o backtest: testa antes
    de fazer append). Um crash num único dia deve ser avaliado contra a
    janela antiga — e dispara."""
    n = 253
    dates = _days(n)
    ief = [100.0] * n
    hyg = [80.0] * 252 + [50.0]  # crash no último dia
    rows = cr.compute_regime(_series(dates, hyg), _series(dates, ief))
    last = rows[-1]
    # janela anterior é constante 0.80 → p20 = 0.80 > 0.50 → risk_off
    assert last["state"] == "risk_off"
    assert last["p20_5y"] == pytest.approx(0.80)


def test_dates_align_inner_join_and_ignore_missing_prices():
    dates = _days(300)
    hyg = _series(dates, [80.0] * 300)
    # IEF sem os primeiros 10 dias e com um None no meio
    ief_prices: list[float | None] = [100.0] * 290
    ief_prices[100] = None
    ief = _series(dates[10:], ief_prices)
    rows = cr.compute_regime(hyg, ief)
    row_dates = [r["regime_date"] for r in rows]
    assert row_dates[0] == dates[10]      # interseção
    assert dates[110] not in row_dates    # dia com preço None fora
    assert len(rows) == 289


def test_deterministic_full_recompute():
    dates = _days(300)
    hyg = _series(dates, [80.0 - 0.05 * i for i in range(300)])
    ief = _series(dates, [100.0] * 300)
    assert cr.compute_regime(hyg, ief) == cr.compute_regime(hyg, ief)


# ──────────────────────────────────────────────────────────────────────────────
# Integração — Tiingo real + cloud (self-skip)
# ──────────────────────────────────────────────────────────────────────────────
def _env() -> dict[str, str]:
    env_file = pathlib.Path(__file__).resolve().parents[1] / ".env"
    out: dict[str, str] = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"')
    out.update({k: v for k, v in os.environ.items()
                if k in ("DATABASE_URL", "TIINGO_API_KEY")})
    return out


def test_advisory_lock_is_distinct():
    assert LOCK_CREDIT_REGIME == 900_205


def test_run_real_history_validates_known_episodes():
    """Roda fim-a-fim (Tiingo + cloud) e confere os episódios do backtest:
    risk_off contínuo no Lehman (jul/2008→abr/2009) e no COVID
    (mar→jun/2020); 2022 sem disparo (≈ B&H). Idempotente."""
    env = _env()
    if not env.get("DATABASE_URL") or not env.get("TIINGO_API_KEY"):
        pytest.skip("DATABASE_URL / TIINGO_API_KEY not configured")
    os.environ.setdefault("TIINGO_API_KEY", env["TIINGO_API_KEY"])
    dsn = env["DATABASE_URL"]
    try:
        conn = psycopg.connect(dsn, connect_timeout=10)
        conn.close()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"cloud unreachable: {exc}")

    stats1 = cr.run(dsn)
    print("\nrun stats:", stats1)
    assert stats1["days"] > 4_000          # ~18y de pregões desde 2007
    assert stats1["upserted"] == stats1["days"]
    assert stats1["state"] in ("risk_on", "risk_off")

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        with advisory_lock(conn, LOCK_CREDIT_REGIME) as got:
            assert got is True
        # Lehman: out/2008 em risk_off
        cur.execute("""SELECT state, count(*) FROM credit_regime_daily
                       WHERE regime_date BETWEEN '2008-09-15' AND '2009-03-31'
                       GROUP BY state ORDER BY 2 DESC""")
        lehman = dict(cur.fetchall())
        print("Lehman window:", lehman)
        assert lehman.get("risk_off", 0) > 0.9 * sum(lehman.values())
        # COVID: abr/2020 em risk_off
        cur.execute("""SELECT state FROM credit_regime_daily
                       WHERE regime_date BETWEEN '2020-03-20' AND '2020-04-30'""")
        covid = [r[0] for r in cur.fetchall()]
        assert covid and all(s == "risk_off" for s in covid)
        # 2022: o sinal NÃO dispara (tolerância p/ vintage de dados Tiingo)
        cur.execute("""SELECT count(*) FILTER (WHERE state='risk_off'), count(*)
                       FROM credit_regime_daily
                       WHERE regime_date BETWEEN '2022-01-01' AND '2022-12-31'""")
        off_2022, total_2022 = cur.fetchone()
        print(f"2022: {off_2022}/{total_2022} risk_off")
        assert off_2022 <= 0.05 * total_2022

    # idempotência: re-run produz as mesmas contagens
    stats2 = cr.run(dsn)
    assert stats2["upserted"] == stats1["upserted"]
