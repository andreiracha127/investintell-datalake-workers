# investintell-datalake-workers

Workers **standalone** que recalculam métricas do data-lake a partir das **séries
brutas**, escrevendo de volta no **TimescaleDB Cloud** (`Investintell-Prod`).
Deploy-alvo: **Railway** (um serviço por worker, cron-scheduled).

> **Por que este repositório existe.** As métricas pré-calculadas no DB-mãe
> (`fund_risk_metrics`, `*_characteristics_monthly`, `factor_model_fits`, …) foram
> geradas por scripts em que o dono **não confia**. Aqui reconstruímos apenas os
> workers **saudáveis e auditáveis**, isolados do monólito `investintell-allocation`,
> derivando 100% das séries brutas. Só o que vive aqui vai para o Railway.

## Princípios

1. **Standalone.** Nada importa de `investintell-allocation`. Dependências mínimas
   (`psycopg`, `numpy`, `pandas`, `arch`, `scipy`). O worker antigo serve só de
   **referência de leitura** — reimplementamos, não copiamos a maquinaria do monólito.
2. **Fonte = séries brutas no cloud.** Lê `nav_timeseries`, `sec_nport_holdings`,
   `benchmark_nav`, `macro_data`, `esma_nav_history`, etc. (já no data-lake).
   **Nunca** lê tabelas de métricas como entrada.
3. **Idempotente.** Cada run abre transação própria, usa `INSERT … ON CONFLICT`
   ou upsert por chave natural, e um **advisory lock** dedicado por worker.
4. **Auditável.** Cada métrica tem fórmula documentada e um teste que a recalcula
   sobre dados conhecidos e compara com tolerância explícita.
5. **Reprodutível.** Sem `Date.now()` implícito em lógica de janela: a data de
   cálculo (`calc_date`) é parâmetro; janelas são determinísticas (252d, `ddof=1`,
   retornos aritméticos).

## Contrato de um worker

Cada worker é um módulo em `src/workers/<nome>.py` expondo:

```python
def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Recalcula a(s) métrica(s) e faz upsert no cloud. Retorna stats {processed, upserted}."""
```

Acompanha:
- `schemas/<nome>.sql` — DDL idempotente da(s) tabela(s) de destino (extraído do
  DB-mãe e ajustado; hypertable quando aplicável).
- `tests/test_<nome>.py` — recalcula ≥1 caso real e compara com tolerância.

Conexão: `src/db.py::connect(dsn)` (psycopg3). DSN vem de `DATABASE_URL` (env).
Advisory lock: `src/db.py::advisory_lock(conn, lock_id)`.

## Workers planejados

| Worker (`src/workers/`) | Tabela(s) destino | Fonte bruta | Referência (monólito, só leitura) |
|---|---|---|---|
| `risk_metrics.py` | `fund_risk_metrics`, `sec_mmf_metrics` | `nav_timeseries`, `benchmark_nav` | `backend/app/jobs/workers/risk_calc.py` |
| `characteristics.py` | `company_characteristics_monthly`, `equity_characteristics_monthly` | `sec_nport_holdings`, `nav_timeseries` | `company_characteristics_compute.py`, `fund_characteristics_aggregator.py` |
| `factor_model.py` | `factor_model_fits` | retornos (`nav_timeseries`) + características | `ipca_estimation.py` |

### Ingestão (Tier 1 do design — `docs/INGESTION_DESIGN.md`)

| Worker (`src/workers/`) | Tabela(s) destino | Fonte externa | Lock | Segredos |
|---|---|---|---|---|
| `macro_ingestion.py` | `macro_data`, `macro_regional_snapshots` | FRED (~92 séries + derivadas `YIELD_CURVE_10Y2Y`/`CPI_YOY`) | 900_320 | `FRED_API_KEY` |
| `treasury_ingestion.py` | `treasury_data` | US Treasury Fiscal Data (5 endpoints, `RATE_/DEBT_/AUCTION_/FX_/INTEREST_`) | 900_324 | — |
| `benchmark_ingest.py` | `benchmark_nav` | Tiingo (ETFs benchmark por bloco; NaN ≤5%) | 900_332 | `TIINGO_API_KEY` |
| `instrument_ingestion.py` | `nav_timeseries` | Tiingo (sweep stale-only priorizado por AUM; universo completo/run) + fallback UCITS EODHD→Yahoo (`_fallback_nav.py`) | 900_331 | `TIINGO_API_KEY`, `EODHD_API_KEY` (opcional) |

## Receita validada — risk metrics (prova Lean, 2026-06-11)

Recalcular de `nav_timeseries.nav` (retornos aritméticos, `return_type='arithmetic'`):

| Métrica | Fórmula |
|---|---|
| `return_Ny` | `nav[-1]/nav[-window] - 1` |
| `volatility_1y` | `std(ret, ddof=1) * sqrt(252)`; **GARCH(1,1)** quando `vol_model` indicar (lib `arch`) |
| `max_drawdown_1y` | `min(nav/nav.cummax() - 1)` |
| `sharpe_1y` | `(mean(ret)*252 - rf) / (std(ret)*sqrt(252))` |
| `sortino` | idem com downside std |
| `beta/alpha/tracking_error/IR` | regressão dos retornos do fundo vs `benchmark_nav` |
| `VaR/CVaR 95` | quantil empírico / EVT na cauda |

Tolerância: vol < 1%, maxDD < 2% vs DB legado (resíduo = vintage de dados + estimador).
Para **100% de acurácia**: mesma NAV até `calc_date`, GARCH(1,1) quando aplicável,
`ddof=1`, 252 dias, retornos aritméticos.

## Conexão (alvos)

- **Cloud (destino + fonte bruta):** `DATABASE_URL` → serviço Tiger `Investintell-Prod`.
- **DB-mãe (referência/validação, read-only):** Postgres em `localhost:5434`
  (`investintell`/`investintell`/`investintell_alloc`) — para comparar contra os
  valores legados durante os testes.

## Deploy (Railway)

Um serviço por worker, comando `python -m src.run <worker>`, agendado por cron.
`railway.toml` e o runner `src/run.py` definem o ponto de entrada. Segredos via
variáveis de ambiente do Railway (`DATABASE_URL`). Nunca commitar credenciais.
