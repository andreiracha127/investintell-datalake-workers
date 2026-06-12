"""credit_regime worker — detector binário de stress de crédito (Frente B).

Replica EXATA da mecânica validada no backtest QC (doc
``2026-06-11-macro-regime-backtest.md``, projeto MacroRegimeHYOnly, backtest
``856a7e9f643a8c44501456e6a328cd86``: Sharpe 0,481 / max DD 25,7% vs 0,418 /
55,0% do buy-and-hold, 46 flips em 19 anos):

  ratio        = HYG_adjclose / IEF_adjclose          (proxy de spread HY)
  janela móvel = últimas 1260 observações ANTERIORES  (~5 anos; exclui hoje)
  p20          = sorted(janela)[min(n-1, int(0.20*(n-1)))], exige n >= 252
  estado       = risk_off  ⇔  p20 existe e ratio < p20      (senão risk_on)

Decisões herdadas do backtest (não opcionais):
  * O sinal é BINÁRIO — não existe "caution". O composite legado de 4 sinais
    foi REFUTADO (Sharpe 0,353 < B&H; 2022 pior que B&H) e NÃO é consumido.
  * A histerese é estrutural: o percentil móvel passa a conter os próprios
    períodos de stress, elevando a barra de re-entrada e suavizando a saída.
  * Fonte = preços Tiingo ajustados (HYG existe desde 2007-04 → sinal vivo a
    partir de ~abr/2008). As séries FRED ICE BofA (BAML*) viraram janela
    rolling ~3 anos e NÃO são usadas aqui.
  * A série INTEIRA é recomputada a cada run: closes ajustados mudam
    retroativamente quando HYG/IEF distribuem dividendos (mensal no HYG) —
    upsert incremental produziria thresholds inconsistentes.

Contract:  run(dsn, *, calc_date=None, limit=None)
           -> {"days", "upserted", "state", "flips", "last_flip", "calc_date"}
"""

from __future__ import annotations

import datetime as _dt
import os
from typing import Any

from src.db import LOCK_CREDIT_REGIME, advisory_lock, connect

HYG_TICKER = "HYG"
IEF_TICKER = "IEF"
HISTORY_START = _dt.date(2007, 1, 1)   # HYG inception 2007-04
WINDOW_MAX = 1260                       # ~5 anos de pregões
MIN_OBS = 252                           # mínimo p/ percentil (1 ano)
PCTL = 0.20
INSERT_CHUNK = 1_000


# ──────────────────────────────────────────────────────────────────────────────
# Pure engine — replica do backtest, sem I/O
# ──────────────────────────────────────────────────────────────────────────────
def percentile_20(window: list[float]) -> float | None:
    """p20 com a indexação EXATA do backtest; None durante o warmup."""
    n = len(window)
    if n < MIN_OBS:
        return None
    vals = sorted(window)
    idx = min(n - 1, int(PCTL * (n - 1)))
    return vals[idx]


def compute_regime(
    hyg: list[tuple[_dt.date, float | None]],
    ief: list[tuple[_dt.date, float | None]],
) -> list[dict[str, Any]]:
    """Série diária de regime a partir dos closes ajustados de HYG e IEF.

    Inner join por data (só dias com AMBOS os preços presentes e > 0). Para
    cada dia, o p20 é avaliado sobre a janela das observações ANTERIORES
    (como o backtest: testa antes do append) e só então o ratio do dia entra
    na janela. ``flip`` marca a primeira observação de cada novo estado.
    """
    ief_map = {d: p for d, p in ief if p is not None and p > 0}
    aligned = [
        (d, p, ief_map[d])
        for d, p in hyg
        if p is not None and p > 0 and d in ief_map
    ]
    aligned.sort(key=lambda t: t[0])

    rows: list[dict[str, Any]] = []
    window: list[float] = []
    prev_state = "risk_on"
    for day, hyg_close, ief_close in aligned:
        ratio = hyg_close / ief_close
        p20 = percentile_20(window)
        state = "risk_off" if (p20 is not None and ratio < p20) else "risk_on"
        rows.append({
            "regime_date": day,
            "state": state,
            "hyg_close": round(hyg_close, 6),
            "ief_close": round(ief_close, 6),
            "ratio": round(ratio, 8),
            "p20_5y": round(p20, 8) if p20 is not None else None,
            "n_window": len(window),
            "flip": state != prev_state,
        })
        prev_state = state
        window.append(ratio)
        if len(window) > WINDOW_MAX:
            window.pop(0)
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# I/O
# ──────────────────────────────────────────────────────────────────────────────
def ensure_schema(conn) -> None:
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "schemas", "credit_regime.sql",
    )
    with open(sql_path, encoding="utf-8") as fh:
        conn.execute(fh.read())
    conn.commit()


def _fetch_prices(calc_date: _dt.date | None):
    """História completa de HYG e IEF na Tiingo (closes ajustados).

    Sempre a série inteira: o adjClose é re-baseado retroativamente a cada
    distribuição, então qualquer fetch incremental misturaria bases. Fail
    loud quando uma das séries vier vazia — sem preço não há detector.
    """
    from src.workers._tiingo import TiingoClient

    with TiingoClient() as client:
        hyg = client.fetch_daily_prices(HYG_TICKER, HISTORY_START, calc_date)
        ief = client.fetch_daily_prices(IEF_TICKER, HISTORY_START, calc_date)
    if not hyg or not ief:
        raise RuntimeError(
            f"Tiingo returned empty history (HYG={len(hyg)}, IEF={len(ief)})"
        )
    return hyg, ief


def _upsert(conn, rows: list[dict[str, Any]]) -> int:
    cols = ("regime_date", "state", "hyg_close", "ief_close", "ratio",
            "p20_5y", "n_window", "flip")
    update = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "regime_date")
    sql = (
        f"INSERT INTO credit_regime_daily ({', '.join(cols)}, computed_at) "
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
    """Recompute the full HYG/IEF credit-stress regime and upsert to the cloud."""
    cdate = _dt.date.fromisoformat(calc_date) if calc_date else None
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_CREDIT_REGIME) as got:
            if not got:
                return {"days": 0, "upserted": 0, "skipped": "lock_busy"}

            ensure_schema(conn)
            hyg, ief = _fetch_prices(cdate)
            rows = compute_regime(hyg, ief)
            if not rows:
                raise RuntimeError("no aligned HYG/IEF observations")
            upserted = _upsert(conn, rows)
            conn.commit()

    flips = [r for r in rows if r["flip"]]
    return {
        "days": len(rows),
        "upserted": upserted,
        "state": rows[-1]["state"],
        "flips": len(flips),
        "last_flip": flips[-1]["regime_date"].isoformat() if flips else None,
        "calc_date": rows[-1]["regime_date"].isoformat(),
    }
