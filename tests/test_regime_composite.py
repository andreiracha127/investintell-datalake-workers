"""Tests for the regime_composite worker (vote2of3 — Frente B, evolução do detector).

Materializa o ensemble por votos validado no backtest
(`2026-06-12-regime-detector-alternatives-backtest.md`, RegimeAltVote,
Sharpe 0,549 / DD 25,3% / 16 flips): risk_off ⇔ ≥2 votos entre

  credit : HYG/IEF ajustado < p20 móvel 5y (lido de credit_regime_daily)
  trend  : SPY fechamento mensal < SMA de 10 fechamentos mensais (Faber)
  nfci   : Chicago Fed NFCI > 0 entra, < −0,05 sai (histerese)

Estados binários (sem caution — o composite por score foi refutado). A engine
pura espelha 1:1 a mecânica diária do backtest (sinais lentos carregados adiante).
"""

from __future__ import annotations

import datetime as _dt

from src.workers import regime_composite as rc


def _spy_months(prices: list[float], start_year: int = 2018) -> list[tuple[_dt.date, float]]:
    """Uma observação por mês (dia 15), preço = month-end close daquele mês."""
    out = []
    y, m = start_year, 1
    for p in prices:
        out.append((_dt.date(y, m, 15), p))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


# ──────────────────────────────────────────────────────────────────────────────
# trend: SPY mensal < SMA(10 mensais), avaliado na virada do mês
# ──────────────────────────────────────────────────────────────────────────────
def test_trend_warmup_under_10_months_is_inactive():
    spy = _spy_months([100.0 + i for i in range(8)])  # só 8 meses
    active = rc.trend_active_by_month(spy)
    assert all(v is False for v in active.values())


def test_trend_rising_market_is_inactive():
    # 13 meses subindo: o último fechamento mensal fica ACIMA da SMA10 → trend off
    spy = _spy_months([100.0 + i for i in range(13)])
    active = rc.trend_active_by_month(spy)
    # mês de índice 10/11/12 (≥10 meses completos antes) avaliáveis → False
    assert active[(2018, 11)] is False
    assert active[(2018, 12)] is False


def test_trend_falling_market_is_active():
    spy = _spy_months([130.0 - i for i in range(13)])  # caindo
    active = rc.trend_active_by_month(spy)
    assert active[(2018, 11)] is True
    assert active[(2018, 12)] is True


def test_trend_uses_only_completed_prior_months():
    # 10 meses planos em 100, depois um pico no 11º; o trend do 11º mês usa os 10
    # meses ANTERIORES (todos 100) → 100 < 100? não → False (não enxerga o próprio mês)
    spy = _spy_months([100.0] * 10 + [50.0, 100.0])
    active = rc.trend_active_by_month(spy)
    assert active[(2018, 11)] is False  # usa os 10 meses de 100, não o pico do próprio mês


# ──────────────────────────────────────────────────────────────────────────────
# nfci: > 0 entra, < −0,05 sai (histerese)
# ──────────────────────────────────────────────────────────────────────────────
def test_nfci_hysteresis_enter_hold_exit_reenter():
    obs = [
        (_dt.date(2020, 1, 1), 0.50),   # > 0 → entra
        (_dt.date(2020, 1, 8), -0.02),  # ≥ −0,05 → segura (histerese)
        (_dt.date(2020, 1, 15), -0.10),  # < −0,05 → sai
        (_dt.date(2020, 1, 22), 0.10),  # > 0 → reentra
    ]
    states = rc.nfci_states(obs)
    assert [s[2] for s in states] == [True, True, False, True]
    # carrega valor + data para proveniência/forward-fill
    assert states[0][0] == _dt.date(2020, 1, 1)
    assert states[2][1] == -0.10


def test_nfci_below_entry_stays_inactive():
    obs = [(_dt.date(2020, 1, 1), -0.5), (_dt.date(2020, 1, 8), -0.01)]
    # nunca > 0 → nunca entra
    assert [s[2] for s in rc.nfci_states(obs)] == [False, False]


# ──────────────────────────────────────────────────────────────────────────────
# compose: ≥2 votos → risk_off, com forward-fill dos sinais lentos
# ──────────────────────────────────────────────────────────────────────────────
def _credit(date, ratio, p20):
    return {"regime_date": date, "ratio": ratio, "p20_5y": p20}


def test_compose_two_votes_is_risk_off():
    d = _dt.date(2021, 6, 1)
    credit = [_credit(d, 0.70, 0.80)]  # ratio < p20 → credit vote True
    trend = {(2021, 6): True}          # trend vote True
    nfci = [(_dt.date(2021, 1, 1), -0.2, False)]  # nfci False
    rows = rc.compose(credit, trend, nfci)
    r = rows[0]
    assert r["credit_vote"] is True and r["trend_vote"] is True and r["nfci_vote"] is False
    assert r["vote_count"] == 2
    assert r["state"] == "risk_off"


def test_compose_one_vote_is_risk_on():
    d = _dt.date(2021, 6, 1)
    credit = [_credit(d, 0.70, 0.80)]  # só o credit
    rows = rc.compose(credit, {(2021, 6): False}, [(_dt.date(2021, 1, 1), 0.0, False)])
    assert rows[0]["vote_count"] == 1
    assert rows[0]["state"] == "risk_on"


def test_compose_credit_vote_needs_threshold():
    d = _dt.date(2021, 6, 1)
    # p20 None (warmup) → credit vote nunca dispara
    rows = rc.compose([_credit(d, 0.70, None)], {}, [])
    assert rows[0]["credit_vote"] is False
    assert rows[0]["state"] == "risk_on"


def test_compose_nfci_forward_fills_and_flags_flips():
    dates = [_dt.date(2021, 3, 1), _dt.date(2021, 6, 1), _dt.date(2021, 9, 1)]
    credit = [_credit(dates[0], 0.7, 0.8),   # credit True
              _credit(dates[1], 0.9, 0.8),   # credit False
              _credit(dates[2], 0.7, 0.8)]   # credit True
    trend = {(2021, 3): True, (2021, 6): False, (2021, 9): True}
    # nfci entra em risk-off em fev e fica (forward-fill para todas as datas)
    nfci = [(_dt.date(2021, 2, 1), 0.3, True)]
    rows = rc.compose(credit, trend, nfci)
    # mar: credit+trend+nfci = 3 → risk_off
    assert rows[0]["vote_count"] == 3 and rows[0]["state"] == "risk_off"
    # jun: trend False, credit False, nfci True (forward-fill) = 1 → risk_on (flip)
    assert rows[1]["vote_count"] == 1 and rows[1]["state"] == "risk_on"
    assert rows[1]["flip"] is True
    # set: credit+trend+nfci = 3 → risk_off (flip de novo)
    assert rows[2]["state"] == "risk_off" and rows[2]["flip"] is True
    # proveniência do nfci forward-filled
    assert rows[1]["nfci"] == 0.3


def test_compose_first_row_flip_is_false_when_risk_on():
    d = _dt.date(2021, 6, 1)
    rows = rc.compose([_credit(d, 0.9, 0.8)], {}, [])
    assert rows[0]["state"] == "risk_on" and rows[0]["flip"] is False


# ──────────────────────────────────────────────────────────────────────────────
# Integração — credit_regime_daily (cloud) + Tiingo SPY + FRED NFCI (self-skip)
# ──────────────────────────────────────────────────────────────────────────────
import os  # noqa: E402
import pathlib  # noqa: E402

import psycopg  # noqa: E402
import pytest  # noqa: E402


def _env() -> dict[str, str]:
    env_file = pathlib.Path(__file__).resolve().parents[1] / ".env"
    out: dict[str, str] = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"')
    out.update({k: v for k, v in os.environ.items()
                if k in ("DATABASE_URL", "TIINGO_API_KEY", "FRED_API_KEY")})
    return out


def test_run_real_history_reproduces_vote2of3():
    """Roda fim-a-fim (credit_regime_daily + Tiingo + FRED) e confere o caráter do
    vote2of3 validado: ~16 flips, risk_off em GFC e COVID, NEUTRO em 2022. Idempotente.
    Requer credit_regime_daily já materializado no cloud."""
    env = _env()
    for key in ("DATABASE_URL", "TIINGO_API_KEY", "FRED_API_KEY"):
        if not env.get(key):
            pytest.skip(f"{key} not configured")
    os.environ.setdefault("TIINGO_API_KEY", env["TIINGO_API_KEY"])
    os.environ.setdefault("FRED_API_KEY", env["FRED_API_KEY"])
    dsn = env["DATABASE_URL"]
    try:
        psycopg.connect(dsn, connect_timeout=10).close()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"cloud unreachable: {exc}")

    stats1 = rc.run(dsn)
    print("\nrun stats:", stats1)
    assert stats1["days"] > 4_000
    assert stats1["upserted"] == stats1["days"]
    assert stats1["state"] in ("risk_on", "risk_off")
    assert 8 <= stats1["flips"] <= 30  # ~16 no backtest; tolera vintage Tiingo/NFCI

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("""SELECT count(*) FILTER (WHERE state='risk_off'), count(*)
                       FROM regime_composite_daily
                       WHERE regime_date BETWEEN '2008-09-15' AND '2009-03-31'""")
        gfc_off, gfc_tot = cur.fetchone()
        assert gfc_off > 0.9 * gfc_tot                       # GFC defensivo
        cur.execute("""SELECT count(*) FROM regime_composite_daily
                       WHERE state='risk_off'
                         AND regime_date BETWEEN '2020-03-09' AND '2020-05-29'""")
        assert cur.fetchone()[0] > 0                          # COVID dispara
        cur.execute("""SELECT count(*) FILTER (WHERE state='risk_off'), count(*)
                       FROM regime_composite_daily
                       WHERE regime_date BETWEEN '2022-01-01' AND '2022-12-31'""")
        off22, tot22 = cur.fetchone()
        assert off22 <= 0.05 * tot22                          # 2022 neutro (vote2of3)

    stats2 = rc.run(dsn)
    assert stats2["upserted"] == stats1["upserted"]           # idempotente
