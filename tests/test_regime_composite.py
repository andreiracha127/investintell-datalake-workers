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

import pytest

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
        (_dt.date(2020, 1, 3), 0.50),    # sex 03/01: > 0 → entra
        (_dt.date(2020, 1, 10), -0.02),  # ≥ −0,05 → segura (histerese)
        (_dt.date(2020, 1, 17), -0.10),  # < −0,05 → sai
        (_dt.date(2020, 1, 24), 0.10),   # > 0 → reentra
    ]
    states = rc.nfci_states(obs)
    assert [s[2] for s in states] == [True, True, False, True]
    # carrega valor + carimbo de DIVULGAÇÃO (sex 03/01 → qua 08/01/2020) para
    # proveniência/forward-fill — nunca a data de observação
    assert states[0][0] == _dt.date(2020, 1, 8)
    # sex 17/01 → semana do MLK Day (seg 20/01/2020) → quinta 23/01
    assert states[2][0] == _dt.date(2020, 1, 23)
    assert states[2][1] == -0.10
    # a regra de carimbo é injetável (identidade = carimbo na própria observação)
    raw = rc.nfci_states(obs, release_date=lambda d: d)
    assert [s[0] for s in raw] == [o[0] for o in obs]
    assert [s[2] for s in raw] == [True, True, False, True]


def test_nfci_below_entry_stays_inactive():
    obs = [(_dt.date(2020, 1, 3), -0.5), (_dt.date(2020, 1, 10), -0.01)]
    # nunca > 0 → nunca entra
    assert [s[2] for s in rc.nfci_states(obs)] == [False, False]


@pytest.mark.parametrize("obs, release, why", [
    (_dt.date(2020, 3, 13), _dt.date(2020, 3, 18), "semana normal: a quarta seguinte"),
    (_dt.date(2025, 1, 17), _dt.date(2025, 1, 23), "MLK Day seg 2025-01-20 → quinta"),
    (_dt.date(2025, 11, 7), _dt.date(2025, 11, 13), "Veterans Day ter 2025-11-11 → quinta"),
    (_dt.date(2024, 12, 27), _dt.date(2025, 1, 2), "Ano Novo na própria quarta 2025-01-01 → quinta"),
    (_dt.date(2024, 6, 14), _dt.date(2024, 6, 20), "Juneteenth qua 2024-06-19 → quinta"),
    (_dt.date(2022, 6, 17), _dt.date(2022, 6, 23), "Juneteenth dom 2022-06-19, observado seg 20 → quinta"),
    (_dt.date(2025, 11, 21), _dt.date(2025, 11, 26), "Thanksgiving qui 2025-11-27 NÃO atrasa a quarta"),
    (_dt.date(2025, 7, 4), _dt.date(2025, 7, 9), "feriado na própria sexta de referência NÃO atrasa"),
    (_dt.date(2026, 7, 3), _dt.date(2026, 7, 8), "4 de julho no sábado 2026-07-04 (não movido) NÃO atrasa"),
])
def test_nfci_release_date_follows_the_chicago_fed_schedule(obs, release, why):
    """Regra oficial (chicagofed.org/research/data/nfci/current-data): quarta 8h30 ET
    cobrindo até a sexta anterior; quinta quando um feriado federal cai na quarta ou
    antes na mesma semana. Feriados fora de seg–qua não atrasam nada."""
    assert obs.weekday() == rc.NFCI_REFERENCE_WEEKDAY, why
    assert rc.nfci_release_date(obs) == release, why
    assert release.weekday() in (rc.NFCI_RELEASE_WEEKDAY, rc.NFCI_RELEASE_WEEKDAY + 1), why


def test_nfci_release_date_anchors_on_the_reference_week():
    # o FRED carimba sextas; um input fora da sexta é levado à sexta da SUA semana
    assert rc.nfci_release_date(_dt.date(2020, 3, 12)) == _dt.date(2020, 3, 18)  # qui → sex 13 → qua 18
    assert rc.nfci_release_date(_dt.date(2020, 3, 9)) == _dt.date(2020, 3, 18)   # seg → sex 13 → qua 18
    assert rc.nfci_release_date(_dt.date(2020, 3, 14)) == _dt.date(2020, 3, 25)  # sáb → sex 20 → qua 25


def test_nfci_vote_waits_for_the_wednesday_release():
    """Regressão MR-1 (auditoria quant 2026-10-07): a observação NFCI de sex
    2020-03-13 (1.2 > 0 → entra) só é divulgada na qua 2020-03-18 às 8h30 ET. Com
    crédito e tendência inativos, o voto NFCI é o único que varia: False em 13, 16 e
    17/03 (inclusive a data de observação), True a partir da PRÓPRIA quarta 18/03 —
    a linha de d é uma decisão de fechamento de d — e carregado adiante."""
    days = [_dt.date(2020, 3, d) for d in (13, 16, 17, 18, 19, 20)]
    credit = [_credit(d, 0.9, 0.8) for d in days]      # ratio > p20 → credit False
    trend = {(2020, 3): False}
    nfci = rc.nfci_states([(_dt.date(2020, 3, 13), 1.2)])
    assert nfci == [(_dt.date(2020, 3, 18), 1.2, True)]
    rows = rc.compose(credit, trend, nfci)
    by_date = {r["regime_date"]: r for r in rows}
    for d in days[:3]:  # 13/03 (obs), 16 e 17/03: ainda não divulgado
        assert by_date[d]["nfci_vote"] is False, d
        assert by_date[d]["vote_count"] == 0 and by_date[d]["nfci"] is None
    for d in days[3:]:  # qua 18/03 (divulgação) em diante: forward-fill
        assert by_date[d]["nfci_vote"] is True, d
        assert by_date[d]["vote_count"] == 1 and by_date[d]["nfci"] == 1.2
    assert all(r["credit_vote"] is False and r["trend_vote"] is False for r in rows)
    assert all(r["state"] == "risk_on" for r in rows)  # 1 voto nunca decide sozinho


def test_nfci_vote_waits_one_more_day_in_a_holiday_week():
    """Semana com feriado federal (MLK Day seg 2025-01-20): a observação de sex
    2025-01-17 só é divulgada na QUINTA 23/01 — a quarta 22/01 ainda não vota."""
    days = [_dt.date(2025, 1, d) for d in (17, 21, 22, 23, 24)]
    credit = [_credit(d, 0.9, 0.8) for d in days]
    nfci = rc.nfci_states([(_dt.date(2025, 1, 17), 0.8)])
    assert nfci == [(_dt.date(2025, 1, 23), 0.8, True)]
    rows = rc.compose(credit, {(2025, 1): False}, nfci)
    votes = {r["regime_date"]: r["nfci_vote"] for r in rows}
    assert votes == {_dt.date(2025, 1, 17): False, _dt.date(2025, 1, 21): False,
                     _dt.date(2025, 1, 22): False, _dt.date(2025, 1, 23): True,
                     _dt.date(2025, 1, 24): True}


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
