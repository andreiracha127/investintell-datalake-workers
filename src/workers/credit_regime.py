"""credit_regime worker — detector binário de stress de crédito (Frente B).

Replica EXATA da mecânica validada no backtest QC (doc
``2026-06-11-macro-regime-backtest.md``, projeto MacroRegimeHYOnly, backtest
``856a7e9f643a8c44501456e6a328cd86``: Sharpe 0,481 / max DD 25,7% vs 0,418 /
55,0% do buy-and-hold, 46 flips em 19 anos):

  ratio        = HYG_adjclose / IEF_adjclose          (proxy de spread HY)
  janela móvel = últimas 1260 observações ANTERIORES  (~5 anos; exclui hoje)
  p20          = sorted(janela)[min(n-1, int(0.20*(n-1)))], exige n >= 252
  estado       = risk_off  ⇔  p20 existe e ratio < p20      (senão risk_on)

Decisões herdadas do backtest (defaults — preservam o sinal validado):
  * O estado materializado é BINÁRIO (risk_on|risk_off) — não existe "caution".
    O composite legado de 4 sinais foi REFUTADO (Sharpe 0,353 < B&H; 2022 pior
    que B&H) e NÃO é consumido.
  * A histerese estrutural permanece: o percentil móvel passa a conter os
    próprios períodos de stress, elevando a barra de re-entrada.
  * Fonte = preços Tiingo ajustados (HYG existe desde 2007-04 → sinal vivo a
    partir de ~abr/2008). As séries FRED ICE BofA (BAML*) viraram janela
    rolling ~3 anos e NÃO são usadas aqui.
  * A série INTEIRA é recomputada a cada run: closes ajustados mudam
    retroativamente quando HYG/IEF distribuem dividendos (mensal no HYG) —
    upsert incremental produziria thresholds inconsistentes.

Melhorias configuráveis (via env — sem deploy de código), aditivas ao default:
  * Histerese assimétrica (flip-control): CREDIT_REGIME_ENTRY_PCTL (entra em
    risk_off quando ratio < p_entry) e CREDIT_REGIME_EXIT_PCTL (volta a risk_on
    só quando recupera ≥ p_exit). exit == entry (default) = detector binário
    validado; exit > entry corta o whipsaw. Único componente do legado com valor
    comprovado (sem histerese: flips 131→289, Sharpe −0,012, DD +1,5 p.p.).
  * stress_score 0–100 graduado (CREDIT_REGIME_STRESS_CALM_PCTL/PANIC_PCTL):
    insumo do modo "low-drawdown" (score sem amplificação) consumido pelo Light.
    Materializado por linha; não altera o estado binário.

Contract:  run(dsn, *, calc_date=None, limit=None)
           -> {"days", "upserted", "state", "stress_score", "entry_pctl",
               "exit_pctl", "flips", "last_flip", "calc_date"}
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
PCTL = 0.20                             # p20 do detector binário validado
INSERT_CHUNK = 1_000


def _env_float(name: str, default: float) -> float:
    """Lê um float de env; trata ausência/vazio como default (sem deploy)."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


# ── Flip-control configurável (histerese assimétrica) ─────────────────────────
# Banda de ENTRADA (entra em risk_off quando ratio < p_entry) e banda de SAÍDA
# (volta a risk_on quando ratio recupera ≥ p_exit). Default exit == entry =>
# detector binário validado (46 flips, Sharpe 0,481), inalterado. Defina
# CREDIT_REGIME_EXIT_PCTL > entry (ex. 0,25) para ligar a histerese anti-whipsaw
# SEM deploy de código — só variável de ambiente.
ENTRY_PCTL = _env_float("CREDIT_REGIME_ENTRY_PCTL", PCTL)
EXIT_PCTL = _env_float("CREDIT_REGIME_EXIT_PCTL", ENTRY_PCTL)

# ── Score graduado (insumo do modo low-drawdown consumido pelo Light) ─────────
# stress_score 0–100 por ramp linear do rank do ratio na janela: rank ≥ calm =>
# 0 (sem stress), rank ≤ panic => 100 (cauda baixa). Espelha o sub-score de
# crédito do legado sem amplificação (ramp(1−percentil, 0,50→0,95)).
STRESS_CALM_PCTL = _env_float("CREDIT_REGIME_STRESS_CALM_PCTL", 0.50)
STRESS_PANIC_PCTL = _env_float("CREDIT_REGIME_STRESS_PANIC_PCTL", 0.05)


# ──────────────────────────────────────────────────────────────────────────────
# Pure engine — replica do backtest, sem I/O
# ──────────────────────────────────────────────────────────────────────────────
def percentile(window: list[float], q: float) -> float | None:
    """Percentil q∈(0,1) com a indexação EXATA do backtest; None no warmup."""
    n = len(window)
    if n < MIN_OBS:
        return None
    vals = sorted(window)
    idx = min(n - 1, int(q * (n - 1)))
    return vals[idx]


def percentile_20(window: list[float]) -> float | None:
    """p20 do detector binário validado — atalho de ``percentile(window, 0.20)``."""
    return percentile(window, PCTL)


def next_state(
    prev_state: str,
    ratio: float,
    p_entry: float | None,
    p_exit: float | None,
) -> str:
    """Estado do dia com histerese assimétrica (utilitário de flip-control).

    Entra em ``risk_off`` quando o ratio rompe a banda de ENTRADA (p_entry, ex.
    p20). Só retorna a ``risk_on`` após recuperar acima da banda de SAÍDA
    (p_exit ≥ p_entry, ex. p25): exigir o cruzamento da banda mais alta evita o
    whipsaw em torno de um único limiar (sem ela os flips explodem — 131→289 no
    composite legado). Com p_entry == p_exit a regra colapsa no detector binário
    sem memória (``risk_off ⇔ ratio < p``), preservando o sinal validado.
    """
    if prev_state == "risk_off":
        # mantém o sinal até a recuperação material; sem banda (warmup) não há
        # como sustentar risk_off, então limpa.
        if p_exit is None:
            return "risk_on"
        return "risk_on" if ratio >= p_exit else "risk_off"
    if p_entry is None:
        return "risk_on"
    return "risk_off" if ratio < p_entry else "risk_on"


def stress_score(
    ratio: float,
    window: list[float],
    *,
    calm: float = STRESS_CALM_PCTL,
    panic: float = STRESS_PANIC_PCTL,
) -> float | None:
    """Score graduado 0–100 da posição do ratio na janela (modo low-drawdown).

    ``rank`` = fração da janela ≤ ratio (CDF empírica). O stress sobe quando o
    ratio CAI: ramp linear do rank ``calm`` (sem stress → 0) ao rank ``panic``
    (cauda baixa → 100), clampado em [0, 100]. Espelha o sub-score de crédito do
    legado sem amplificação. None no warmup (< MIN_OBS), como o percentil. A
    janela aqui são as observações ANTERIORES (igual à avaliação do percentil).
    """
    n = len(window)
    if n < MIN_OBS:
        return None
    span = calm - panic
    if span <= 0:
        return 0.0
    rank = sum(1 for v in window if v <= ratio) / n
    frac = (calm - rank) / span
    return 100.0 * min(1.0, max(0.0, frac))


def compute_regime(
    hyg: list[tuple[_dt.date, float | None]],
    ief: list[tuple[_dt.date, float | None]],
    *,
    entry_pctl: float = ENTRY_PCTL,
    exit_pctl: float = EXIT_PCTL,
    calm: float = STRESS_CALM_PCTL,
    panic: float = STRESS_PANIC_PCTL,
) -> list[dict[str, Any]]:
    """Série diária de regime a partir dos closes ajustados de HYG e IEF.

    Inner join por data (só dias com AMBOS os preços presentes e > 0). Para
    cada dia, as bandas de percentil e o ``stress_score`` são avaliados sobre a
    janela das observações ANTERIORES (como o backtest: testa antes do append) e
    só então o ratio do dia entra na janela. O estado segue a histerese
    assimétrica (``next_state``): com ``exit_pctl == entry_pctl`` (default) é o
    detector binário validado; com ``exit_pctl > entry_pctl`` liga o anti-whipsaw.
    ``flip`` marca a primeira observação de cada novo estado.

    Colunas derivadas por linha: ``p20_5y`` = banda de entrada (p20 default),
    ``p_exit_5y`` = banda de saída, ``stress_score`` = score graduado 0–100.
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
        p_entry = percentile(window, entry_pctl)
        p_exit = percentile(window, exit_pctl)
        state = next_state(prev_state, ratio, p_entry, p_exit)
        score = stress_score(ratio, window, calm=calm, panic=panic)
        rows.append({
            "regime_date": day,
            "state": state,
            "hyg_close": round(hyg_close, 6),
            "ief_close": round(ief_close, 6),
            "ratio": round(ratio, 8),
            "p20_5y": round(p_entry, 8) if p_entry is not None else None,
            "p_exit_5y": round(p_exit, 8) if p_exit is not None else None,
            "stress_score": round(score, 3) if score is not None else None,
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
            "p20_5y", "p_exit_5y", "stress_score", "n_window", "flip")
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
    last = rows[-1]
    return {
        "days": len(rows),
        "upserted": upserted,
        "state": last["state"],
        "stress_score": last["stress_score"],
        "entry_pctl": ENTRY_PCTL,
        "exit_pctl": EXIT_PCTL,
        "flips": len(flips),
        "last_flip": flips[-1]["regime_date"].isoformat() if flips else None,
        "calc_date": last["regime_date"].isoformat(),
    }
