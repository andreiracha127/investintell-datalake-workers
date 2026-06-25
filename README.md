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

## QC A3 parity pilot

O piloto `qc-a3-parity` usa QuantConnect Research apenas como infraestrutura
secundária de pesquisa. O core A3 continua consumindo painéis PIT imutáveis do
Investintell; o notebook não usa QC FRED, não usa retornos como objetivo de A3 e
mantém `runtime_activation=false`, `A4=harness_ready_provisional_A3` e
`A5=blocked`.

Bundle local aprovado para o primeiro teste cloud:

```powershell
python qc_a3_core.py export-bundle `
  --feature-manifest _tmp_qc_a3_parity_25375bb_20260625\manifests\feature_manifest.json `
  --revision-uncertainty-manifest _tmp_qc_a3_parity_25375bb_20260625\manifests\revision_uncertainty_manifest.json `
  --config-catalog _tmp_qc_a3_parity_25375bb_20260625\manifests\config_catalog.normalized.json `
  --a32-grid-dir _tmp_qc_a3_parity_25375bb_20260625\manifests `
  --output-dir _tmp_qc_a3_parity_25375bb_10198d_cloud_20260625 `
  --expected-v03-grid-dir _tmp_a31_v03_revision_robust_g1_e6a72c3_20260625 `
  --a31-name V03-G0-CONTROL `
  --a32-name A32-G0.35-I0.35-X0.10-C0.60-D1.25 `
  --worker-commit 25375bbd23d7eb99210914ad6702bf2d080a27ce
```

O manifest publica somente JSON, NPZ e CSV gzip no prefixo imutável:

```text
investintell/a3/qc-a3-parity/25375bb/10198d7603036c3327ac9e67/
```

O projeto cloud materializa `src/calibration_harness.py` a partir de
`code/calibration_harness.py.gz` no mesmo prefixo, com SHA-256 verificado, porque
o QC Cloud limita o tamanho de arquivos fonte individuais.

Upload via Lean CLI:

```powershell
lean login
lean whoami
python qc_a3_core.py upload-object-store `
  --bundle-dir _tmp_qc_a3_parity_25375bb_10198d_cloud_20260625
lean cloud object-store list investintell/a3/qc-a3-parity/25375bb
```

O notebook cloud deve gravar `results/qc_cloud_parity_report.json` e
`results/qc_cloud_environment.json`, com hashes lógicos iguais aos do bundle e
`mismatch_count=0`.

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
| `nport_lookthrough.py` | `nport_lookthrough_exposures`, `nport_lookthrough_summary` | `sec_nport_holdings` (96M) + catálogo (`sec_cusip_ticker_map`, `sec_fund_classes`, `sec_etfs`, `instrument_identity`, `instruments_universe`) + `cagg_nport_series_profile` (coverage **copiado**, nunca recalculado) | frente C do doc de research 2026-06-11 (ADENDO §6) |
| `credit_regime.py` | `credit_regime_daily` | Tiingo (closes ajustados HYG/IEF — série inteira por run, ajustes são retroativos) | frente B re-escopada; replica do backtest QC `856a7e9f…` (Sharpe 0,481 / DD 25,7%) — ratio HYG/IEF < p20 móvel 5y (mín. 252 obs), binário risk_on/risk_off; composite legado REFUTADO. Lock 900_205 |

### Look-through (frente C) — modelo

Expansão recursiva (profundidade máx. 2, guarda de ciclo por ancestrais), peso
composto `w = (pct_parent/100) × pct_child`; dimensões **issuer (CUSIP-6)**,
**asset_class**, **sector**, **currency**, separando exposição **direta ×
indireta**. Σpct > 100 (derivativos/alavancagem) **nunca** é renormalizado;
sinais (shorts) preservados. Residual explícito no summary:
`nondecomposable_fund_pct` (fundo casado sem dados / ciclo / limite de
profundidade), `derivatives_gross_pct`/`derivatives_net_pct` (asset_class
N-PORT `D*` exceto `DBT`) e `unidentified_pct` (chaves sintéticas
`LE:`/`H:`/`CIK:`). Staleness em cadeia: `oldest_report_date` = report mais
antigo usado na expansão. Aresta FoF: CUSIP-9 → ticker
(`sec_cusip_ticker_map`) → série (`sec_fund_classes`/`sec_etfs`), mais
`instrument_identity` e isin (`IS:<isin>` casa via isin e via CUSIP-9 embutido
em ISIN US; `LE:`/`H:`/`CIK:` nunca casam). Lock `900_204`. O Light consome as
duas tabelas materializadas direto (DB-first) — nenhum cálculo em request path.

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
