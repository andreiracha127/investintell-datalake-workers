"""regime_composite worker — detector vote2of3 (Frente B, evolução do detector).

Materializa o ensemble por VOTOS validado no backtest QC
(``2026-06-12-regime-detector-alternatives-backtest.md``, projeto RegimeAltVote,
backtest ``7ecef2e31f1fa4c98b7c5cc732b1f259``: Sharpe 0,549 / DD 25,3% / CAGR
12,30% / ~16 flips — bate o credit-only em TODAS as métricas e fica neutro em
2022). Confirmado em 2026-06-13 (`2026-06-13-credit-regime-hysteresis-lowdrawdown.md`).

  risk_off  ⇔  ≥ 2 votos entre:
    credit : HYG/IEF ajustado < p20 móvel 5y      (lido de credit_regime_daily)
    trend  : SPY fechamento mensal < SMA 10 meses (Faber; preços Tiingo)
    nfci   : Chicago Fed NFCI > 0 entra / < −0,05 sai  (histerese; FRED)

Decisões herdadas do estudo (não opcionais):
  * Estados BINÁRIOS (risk_on|risk_off) — sem caution. O composite por score
    ponderado (legado) foi REFUTADO; a força do vote2of3 é a CONFIRMAÇÃO CRUZADA
    entre sinais individualmente defensáveis, não a agregação por peso.
  * O voto de crédito é lido de ``credit_regime_daily`` (mantido INTACTO; é 1 dos
    votos) como ``ratio < p20_5y`` — o sinal binário p20 validado, independente da
    histerese opcional do worker credit_regime.
  * Sinais lentos (trend mensal, nfci semanal) são carregados adiante (forward-fill)
    sobre as datas diárias de pregão, como o backtest avalia a cada bar.
  * NFCI vem do FRED com histórico pleno (o lake macro_data só guarda ~10 anos).
    Caveat conhecido: NFCI é revisado ex-post (viés de revisão; teto otimista) — no
    composite ele é 1 voto entre 3, nunca decide sozinho.

Contract:  run(dsn, *, calc_date=None, limit=None)
           -> {"days", "upserted", "state", "vote_count", "flips", "last_flip",
               "calc_date"}
"""

from __future__ import annotations

import datetime as _dt
import os
from typing import Any

from src.db import LOCK_REGIME_COMPOSITE, advisory_lock, connect

SPY_TICKER = "SPY"
HISTORY_START = _dt.date(2007, 1, 1)   # casa o set_start_date do backtest
SMA_MONTHS = 10                        # Faber: SMA de 10 fechamentos mensais
NFCI_ENTER = 0.0                       # NFCI > 0 entra em risk-off
NFCI_EXIT = -0.05                      # NFCI < −0,05 sai (histerese)
FRED_OBS_URL = "https://api.stlouisfed.org/fred/series/observations"
INSERT_CHUNK = 1_000


# ──────────────────────────────────────────────────────────────────────────────
# Pure engine — replica do backtest, sem I/O
# ──────────────────────────────────────────────────────────────────────────────
def month_end_closes(
    spy_daily: list[tuple[_dt.date, float | None]],
) -> dict[tuple[int, int], tuple[_dt.date, float]]:
    """(ano, mês) → (data, close) do ÚLTIMO pregão de cada mês (preço > 0)."""
    me: dict[tuple[int, int], tuple[_dt.date, float]] = {}
    for d, px in spy_daily:
        if px is None or px <= 0:
            continue
        key = (d.year, d.month)
        if key not in me or d > me[key][0]:
            me[key] = (d, px)
    return me


def trend_active_by_month(
    spy_daily: list[tuple[_dt.date, float | None]],
    *,
    sma_months: int = SMA_MONTHS,
) -> dict[tuple[int, int], bool]:
    """trend_active por mês: o sinal de cada mês M usa só os fechamentos dos meses
    COMPLETOS anteriores (≥ sma_months) — espelha o backtest, que fixa o trend no
    primeiro bar do mês a partir dos meses já fechados (não enxerga o próprio mês).
    """
    me = month_end_closes(spy_daily)
    months = sorted(me.keys())
    closes = [me[m][1] for m in months]
    active: dict[tuple[int, int], bool] = {}
    for i, m in enumerate(months):
        prior = closes[:i]  # meses estritamente anteriores a M (completos)
        if len(prior) >= sma_months:
            window = prior[-sma_months:]
            active[m] = window[-1] < sum(window) / sma_months
        else:
            active[m] = False
    return active


def nfci_states(
    nfci_obs: list[tuple[_dt.date, float]],
) -> list[tuple[_dt.date, float, bool]]:
    """Estado do voto NFCI por observação, com histerese (> 0 entra, < −0,05 sai).
    Retorna (data, valor, ativo) em ordem — o consumidor faz forward-fill."""
    out: list[tuple[_dt.date, float, bool]] = []
    active = False
    for d, v in sorted(nfci_obs, key=lambda t: t[0]):
        active = (v >= NFCI_EXIT) if active else (v > NFCI_ENTER)
        out.append((d, v, active))
    return out


def compose(
    credit_rows: list[dict[str, Any]],
    trend_by_month: dict[tuple[int, int], bool],
    nfci: list[tuple[_dt.date, float, bool]],
) -> list[dict[str, Any]]:
    """Série diária de regime por votos. ``credit_rows`` = linhas de
    credit_regime_daily ({regime_date, ratio, p20_5y}); trend/nfci são carregados
    adiante (forward-fill) sobre cada data de crédito. risk_off ⇔ ≥ 2 votos.
    ``flip`` marca a 1ª observação de cada novo estado (estado inicial: risk_on).
    """
    nfci_sorted = sorted(nfci, key=lambda t: t[0])
    rows: list[dict[str, Any]] = []
    prev_state = "risk_on"
    j = 0
    cur_nfci_val: float | None = None
    cur_nfci_active = False
    for cr in sorted(credit_rows, key=lambda r: r["regime_date"]):
        d = cr["regime_date"]
        while j < len(nfci_sorted) and nfci_sorted[j][0] <= d:
            cur_nfci_val = nfci_sorted[j][1]
            cur_nfci_active = nfci_sorted[j][2]
            j += 1
        ratio = cr.get("ratio")
        p20 = cr.get("p20_5y")
        credit_vote = p20 is not None and ratio is not None and ratio < p20
        trend_vote = bool(trend_by_month.get((d.year, d.month), False))
        nfci_vote = bool(cur_nfci_active)
        vote_count = int(credit_vote) + int(trend_vote) + int(nfci_vote)
        state = "risk_off" if vote_count >= 2 else "risk_on"
        rows.append({
            "regime_date": d,
            "state": state,
            "credit_vote": credit_vote,
            "trend_vote": trend_vote,
            "nfci_vote": nfci_vote,
            "vote_count": vote_count,
            "ratio": round(ratio, 8) if ratio is not None else None,
            "p20_5y": round(p20, 8) if p20 is not None else None,
            "nfci": round(cur_nfci_val, 4) if cur_nfci_val is not None else None,
            "flip": state != prev_state,
        })
        prev_state = state
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# I/O
# ──────────────────────────────────────────────────────────────────────────────
def ensure_schema(conn) -> None:
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "schemas", "regime_composite.sql",
    )
    with open(sql_path, encoding="utf-8") as fh:
        conn.execute(fh.read())
    conn.commit()


def _fetch_credit_daily(conn) -> list[dict[str, Any]]:
    """Voto de crédito: ratio + p20_5y materializados pelo worker credit_regime."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT regime_date, ratio, p20_5y FROM credit_regime_daily "
            "ORDER BY regime_date"
        )
        return [
            {"regime_date": d,
             "ratio": float(r) if r is not None else None,
             "p20_5y": float(p) if p is not None else None}
            for d, r, p in cur.fetchall()
        ]


def _fetch_spy(calc_date: _dt.date | None) -> list[tuple[_dt.date, float | None]]:
    """Closes ajustados de SPY na Tiingo (sinal de tendência mensal)."""
    from src.workers._tiingo import TiingoClient

    with TiingoClient() as client:
        spy = client.fetch_daily_prices(SPY_TICKER, HISTORY_START, calc_date)
    if not spy:
        raise RuntimeError("Tiingo returned empty SPY history")
    return spy


def _fetch_nfci(calc_date: _dt.date | None) -> list[tuple[_dt.date, float]]:
    """NFCI (Chicago Fed) com histórico pleno via FRED (semanal)."""
    import httpx

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise RuntimeError("FRED_API_KEY not set")
    params = {
        "series_id": "NFCI", "api_key": api_key, "file_type": "json",
        "sort_order": "asc", "observation_start": HISTORY_START.isoformat(),
    }
    if calc_date:
        params["observation_end"] = calc_date.isoformat()
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(FRED_OBS_URL, params=params)
        resp.raise_for_status()
        payload = resp.json()
    out: list[tuple[_dt.date, float]] = []
    for o in payload.get("observations", []):
        s = str(o.get("value", "")).strip()
        if s in (".", "", "NaN", "nan", "null", "None"):
            continue
        try:
            v = float(s)
        except (TypeError, ValueError):
            continue
        out.append((_dt.date.fromisoformat(o["date"]), v))
    if not out:
        raise RuntimeError("FRED returned no NFCI observations")
    return out


def _upsert(conn, rows: list[dict[str, Any]]) -> int:
    cols = ("regime_date", "state", "credit_vote", "trend_vote", "nfci_vote",
            "vote_count", "ratio", "p20_5y", "nfci", "flip")
    update = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "regime_date")
    sql = (
        f"INSERT INTO regime_composite_daily ({', '.join(cols)}, computed_at) "
        f"VALUES ({', '.join(['%s'] * len(cols))}, now()) "
        f"ON CONFLICT (regime_date) DO UPDATE SET {update}, computed_at = now()"
    )
    upserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), INSERT_CHUNK):
            chunk = rows[start:start + INSERT_CHUNK]
            cur.executemany(sql, [tuple(r[c] for c in cols) for r in chunk])
            upserted += len(chunk)
    return upserted


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,  # aceito por contrato; sem efeito (série única)
) -> dict:
    """Recompute the full vote2of3 composite regime and upsert to the cloud."""
    cdate = _dt.date.fromisoformat(calc_date) if calc_date else None
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_REGIME_COMPOSITE) as got:
            if not got:
                return {"days": 0, "upserted": 0, "skipped": "lock_busy"}

            ensure_schema(conn)
            credit = _fetch_credit_daily(conn)
            if not credit:
                raise RuntimeError(
                    "credit_regime_daily is empty — run the credit_regime worker first"
                )
            spy = _fetch_spy(cdate)
            nfci = _fetch_nfci(cdate)
            rows = compose(credit, trend_active_by_month(spy), nfci_states(nfci))
            upserted = _upsert(conn, rows)
            conn.commit()

    flips = [r for r in rows if r["flip"]]
    last = rows[-1]
    return {
        "days": len(rows),
        "upserted": upserted,
        "state": last["state"],
        "vote_count": last["vote_count"],
        "flips": len(flips),
        "last_flip": flips[-1]["regime_date"].isoformat() if flips else None,
        "calc_date": last["regime_date"].isoformat(),
    }
