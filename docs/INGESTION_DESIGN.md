# Ingestion Workers — Design

> **Status: DESIGN ONLY.** This document specifies the *raw-series ingestion* workers
> for the Investintell data-lake. No production worker here is implemented yet; the
> calculation workers (`risk_metrics`, `characteristics`, `factor_model`) already live
> in `src/workers/`. This file is the blueprint for adding the ingestion layer next to
> them, under the same standalone contract (README §"Contrato de um worker").

## 0. Scope & boundary

The ~45 global raw series were **replicated once** from the mother DB
(`investintell-allocation`) into **TimescaleDB Cloud** (Tiger `Investintell-Prod`,
service_id `t83f4np6x4`). They are now **stale snapshots**. We need **continuous
ingestion workers** that pull *new* data from the **external sources** and upsert into
the cloud, deployed standalone on **Railway** in this repo.

- **In scope:** raw-series ingestion (SEC, ESMA/UCITS, macro/econ, market/identity).
- **Out of scope:** the metric workers already here; any write to the mother DB; any
  new schema in production (we ship idempotent DDL, but the owner applies it).
- **Reference only:** the monolith workers in
  `E:\investintell-allocation\backend\app\jobs\workers\` and providers in
  `…\backend\data_providers\`. We *reimplement* standalone — never import the monolith
  (README §Princípios 1). Each section below cites the real monolith worker, its
  source, conflict key, and rate-limit posture so the standalone rewrite is faithful.

### What changes vs. the monolith
| Concern | Monolith | This repo (standalone) |
|---|---|---|
| Runtime | async SQLAlchemy + RLS + Redis gates | sync `psycopg3` (`src/db.py`), no RLS (all targets are global) |
| Rate limiting | Redis sliding-window `ExternalProviderGate` | in-process token bucket (no Redis on Railway) |
| Schedule | one dispatcher (`app.workers.cli`) | **one Railway service per worker**, cron-scheduled (README §Deploy) |
| Entry | registry coroutine | `run(dsn, *, calc_date=None, limit=None) -> dict` |
| Lock range | 43, 900_0xx, 900_1xx | **900_3xx** (ingestion band; metrics use 900_201–203) |

---

## 1. Master table — every ingestion worker

Frequencies are *cron suggestions*; the real cadence is set per Railway service.
"Conflict key" = the natural key that makes the upsert idempotent (copy it verbatim
from the monolith — it is the only thing keeping re-runs safe). Lock IDs are proposed
in the **900_3xx** band (distinct per worker; none collide with metrics 900_201–203 or
the monolith's 43/900_0xx/900_1xx).

### 1A. SEC (EDGAR) workers

| Worker | External source | Target table(s) | Cron | Method | Idempotency / upsert | Lock | Rate-limit / risk |
|---|---|---|---|---|---|---|---|
| `sec_adv_ingestion` | SEC FOIA monthly ADV ZIP (`sec.gov/files/investment/data/...ia{MMDDYY}.zip`, **DD non-deterministic** → try candidate dates) | `sec_managers`, `sec_manager_funds`, `sec_entity_links` | monthly | bulk ZIP/CSV download | `sec_managers` upsert by CRD; `sec_entity_links` `ON CONFLICT (manager_crd, related_cik, relationship) DO NOTHING` | 900_301 | EDGAR 8 req/s + mandatory User-Agent; ADV URL date guessing is brittle |
| `nport_fund_discovery` | EFTS full-text `efts.sec.gov/LATEST/search-index` (`"NPORT-P"`, 100/page, 10k cap) + per-CIK `data.sec.gov/submissions/CIK{cik}.json` | `sec_registered_funds`, `sec_fund_classes` | weekly | scrape + per-CIK header fetch, AUM≥$50M filter | `ON CONFLICT (cik) DO UPDATE` (COALESCE-preserving so it never nulls ticker/series); classes `ON CONFLICT (cik, series_id, class_id)`; staleness `data_fetched_at < now()-35d` | 900_302 | EFTS partial results → **merge with existing DB CIKs**; `sleep(0.15)` between pages |
| `nport_ingestion` (per-CIK) | EDGAR per-CIK via `edgartools` `Company(cik).get_filings(form="NPORT-P").xml()` | `sec_nport_holdings` (hypertable), updates `sec_registered_funds.last_nport_date` | monthly | incremental per-CIK API parse, AUM-sorted ≤200/run, `last_nport_date < now()-35d` | `ON CONFLICT (report_date, series_id, cusip) DO NOTHING`; **drops series-less rows** | 900_303 | needs `edgar.set_identity()`; 35-day staleness gate |
| `nport_bulk_load` (**preferred high-volume**) | Local DERA quarterly dataset dir (`E:\Edgard\*_nport` / `F:\EDGAR FILES\2026q1_nport` TSVs) → parse → parallel COPY | `sec_nport_holdings` | quarterly (after DERA publish, ~2mo lag) | local-file parse + `ThreadPoolExecutor` COPY into TEMP stage → `INSERT…SELECT` | same `ON CONFLICT (report_date, series_id, cusip) DO NOTHING`; **keeps CUSIP-less rows via synthetic key** (`IS:{isin}`/`LE:{lei}`/`H:{holding_id}`), series-less → `CIK:{cik}` | none (parallel by design) | Timescale: must `decompress_chunk` + drop compression policy before load, re-add after (`scripts/nport_parallel_load.py` `prep()`/`finalize()`) |
| `sec_xbrl_facts_ingestion` | **Local** SEC companyfacts dump (`CIK{0000000000}.json`) | `sec_xbrl_facts` (106M, hypertable) | on dump refresh (quarterly) | parallel local-file parse → asyncpg/psycopg COPY → `INSERT…SELECT` | `ON CONFLICT (cik, taxonomy, concept, unit, period_end, accn) DO NOTHING` (no watermark, pure conflict key) | 900_304 | huge; needs a **direct (non-pooled) DSN**; no network |
| `sec_13f_ingestion` | EDGAR per-CIK via `edgartools` (13F-HR) | `sec_13f_holdings` (hypertable), `sec_13f_diffs` | quarterly | per-CIK API; universe `sec_managers WHERE aum_total ≥ $100M` | service upsert + diff of two latest `report_date`s | 900_305 | **depends on `sec_adv_ingestion`** (needs `sec_managers`) |
| `form345_ingestion` | `sec.gov/files/structureddata/data/form-345/{YYYY}q{N}_form345.zip` | `sec_insider_transactions` | quarterly | bulk ZIP → TSV join (`SUBMISSION`/`REPORTINGOWNER`/`NONDERIV_TRANS`) | `ON CONFLICT (accession_number, trans_sk) DO UPDATE` (true upsert) | 900_306 | independent; self-parsing |
| `nport_ticker_resolution` | OpenFIGI `api.openfigi.com/v3/mapping` (`BASE_TICKER`) | `sec_registered_funds.ticker` (UPDATE) | weekly (after discovery) | batched OpenFIGI, ≤500 funds/run | naturally idempotent — only touches `ticker IS NULL` | 900_307 | OpenFIGI free 25/min vs keyed 250/min |
| `nport_cusip_enrichment` | OpenFIGI (CUSIP→ticker) + Tiingo `fundamentals/meta` (ticker→GICS/SIC) | `sec_cusip_ticker_map`, propagates `sec_nport_holdings.sector` | weekly (after holdings) | two-phase API enrichment | Phase A `ON CONFLICT (cusip) DO UPDATE`; Phase B `UPDATE … WHERE ticker=`; 90-day `tiingo_meta_fetched_at` skip | 900_308 | OpenFIGI free tier is the real bottleneck; scrub Tiingo `"Field not available…"` sentinel before INTEGER cast |

> **Tables not yet covered by an existing worker but in the cloud:** `sec_etfs`,
> `sec_bdcs`, `sec_money_market_funds`, `sec_registered_funds` (the N-CEN/N-MFP/BDC
> slices) are populated in the monolith by `sec_bulk_ingestion.py`, which **scrapes the
> DERA landing pages** for the newest ZIP and delegates to `scripts/seed_*` parsers.
> Standalone equivalent = `sec_bulk_ingestion` worker (quarterly, lock 900_309) that
> downloads the N-CEN/N-MFP/BDC ZIPs and upserts. `sec_mmf_metrics` is fed from the
> N-MFP slice (the metric worker only creates its schema, per `risk_metrics.py` docstring).

### 1B. ESMA / UCITS workers

| Worker | External source | Target table(s) | Cron | Method | Idempotency / upsert | Lock | Rate-limit / risk |
|---|---|---|---|---|---|---|---|
| `firds_ucits_security_sync` | ESMA FIRDS: Solr discovery (`registers.esma.europa.eu/solr/esma_registers_firds_files`) + direct ZIP (`firds.esma.europa.eu/firds/FULINS_C_{date}_01of01.zip`, 50–150 MB) | `esma_securities` | daily ~03:30 UTC | bulk ZIP stream-parse XML, filtered by known LEIs from `esma_funds` | `ON CONFLICT (isin) DO UPDATE … last_seen_at=now(), is_active=true`; staleness sets `is_active=false` for rows with `last_seen_at < run_started_at` — **use DB `now()`, not host clock** (monolith has a documented skew bug here) | 900_310 | requires `esma_funds` populated first (loads `known_leis`) |
| `esma_ingestion` | ESMA Solr funds register (`…/solr/esma_registers_funds_cbdif/select`, paginated 1000) + OpenFIGI for ISIN→ticker | `esma_managers`, `esma_funds`, `esma_isin_ticker_map` | weekly | Solr scrape (funds+managers one pass) + OpenFIGI | `esma_managers ON CONFLICT (esma_id)`, `esma_funds ON CONFLICT (lei)` (**dedup by LEI in Python first** — PG rejects dup keys in one INSERT), `esma_isin_ticker_map ON CONFLICT (isin)`; per-batch commit | 900_311 | ESMA 4 req/s; reads `esma_securities` for real share-class ISINs → run **after** FIRDS |
| `esma_aum_sync` | **Yahoo Finance** (`yfinance` `Ticker.info["totalAssets"]` + FX `<CCY>USD=X`) | `instruments_universe.attributes` (`aum_usd`, `aum_native`, …) | weekly | per-ticker yfinance in thread pool, `aum_fetched_at < now()-7d` | 7-day TTL skip; degraded fetch **preserves last good AUM**; per-row `SAVEPOINT` | 900_312 | yfinance unofficial; circuit-opens after 10 consecutive failures |

### 1C. Macro / econ workers

| Worker | External source | Target table(s) | Cron | Method | Idempotency / upsert | Lock | Rate-limit / risk |
|---|---|---|---|---|---|---|---|
| `macro_ingestion` | **FRED** REST (`api.stlouisfed.org/fred`, ~45 series US/EU/Asia/EM) | `macro_data`, `macro_snapshots` (`macro_regional_snapshots`) | daily | incremental API, 10y lookback, concurrent domain batches | snapshot `ON CONFLICT (as_of_date)`; series `ON CONFLICT (series_id, obs_date)`; dedup-by-PK then chunk 2000; also writes derived `YIELD_CURVE_10Y2Y`, `CPI_YOY` | 900_320 | FRED free key; reads `bis_statistics`/`imf_weo_forecasts` for enrichment (degrade to None) → run BIS/IMF first |
| `imf_ingestion` | IMF SDMX 2.1 (`api.imf.org/external/sdmx/2.1`; `IMF.RES,WEO` + `IFS` + `BOP`) | `imf_weo_forecasts`, `imf_high_frequency` (hypertables) | quarterly (WEO Apr+Oct) | SDMX-CSV API | WEO `ON CONFLICT (country_code, indicator, year, period)`; HF `ON CONFLICT (country_code, indicator, frequency, obs_date, dataset)` | 900_321 | independent |
| `bis_ingestion` | BIS SDMX (`stats.bis.org/api/v1/data/{dataset}/Q.{ref_area}?format=csv`; credit-gap, DSR, property) | `bis_statistics` (hypertable) | quarterly | incremental CSV API | `ON CONFLICT (country_code, indicator, period)` (indicator may be `name__dim1__dim2`) | 900_322 | independent; feeds macro |
| `ofr_ingestion` | OFR Hedge Fund Monitor (`data.financialresearch.gov/hf/v1`; leverage P5/50/95, GAV/NAV, SCOOS, FICC repo) | `ofr_hedge_fund_data` (hypertable) | quarterly (Form PF cadence) | incremental API, 5y lookback, per-category try/except | `ON CONFLICT (obs_date, series_id)`; series_id normalized `OFR_<…>` ≤80 chars | 900_323 | 5 req/s; 401 auth path logged |
| `treasury_ingestion` | US Treasury Fiscal Data (`api.fiscaldata.treasury.gov/...`; avg rates, debt-to-penny, auctions, FX, interest) | `treasury_data` (hypertable) | daily/monthly mix | paginated API, 365d lookback, 5 endpoints concurrent | `ON CONFLICT (obs_date, series_id)`; prefixes `RATE_/DEBT_/AUCTION_/FX_/INTEREST_`; auctions carry `metadata_json` (bid_to_cover) | 900_324 | 5 req/s token bucket |

### 1D. Market / identity workers

| Worker | External source | Target table(s) | Cron | Method | Idempotency / upsert | Lock | Rate-limit / risk |
|---|---|---|---|---|---|---|---|
| `universe_sync` | **DB-internal** joins from SEC/ESMA catalog tables + one SEC GET (`sec.gov/files/company_tickers_mf.json`) | `instruments_universe` (7 phases) | weekly | bulk SQL `INSERT…SELECT…ON CONFLICT (ticker)` | `ON CONFLICT (ticker) DO UPDATE` merging `attributes || EXCLUDED.attributes`; post-sync `_deactivate_no_nav` (is_active=false where no `nav_timeseries`) | 900_330 | **must run after** SEC-bulk/ESMA/FIRDS and **before** NAV/identity; the deactivate step needs a 2nd pass after NAV |
| `instrument_ingestion` (NAV) | **Tiingo** batch history | `nav_timeseries` (27.4M, hypertable) | daily | batch download all active+ticker instruments, ~15y, dedup one call per unique ticker | `ON CONFLICT (instrument_id, nav_date) DO UPDATE`; chunk 500, per-chunk commit | 900_331 | **Tiingo ~130 req/h real** (see §5); ~416 no-ticker + ~1,442 UCITS `.L/.PA/.SW` return empty (provider gap) |
| `benchmark_ingest` | **Tiingo** (benchmark tickers from `allocation_blocks.benchmark_ticker`) | `benchmark_nav` (compressed hypertable) | daily | batch ~15y, NaN-ratio ≤5% validation | `ON CONFLICT (block_id, nav_date)`; chunk 200; **upsert only safe on recent uncompressed chunks** | 900_332 | shares the Tiingo budget with NAV |
| `tiingo_enrichment` | **Tiingo** meta (`tiingo/daily/{ticker}`: description/start/end) | `instruments_universe.attributes` (JSONB) | weekly | per-ticker ~2 req/s, commit every 50 | 30-day TTL; ordered by `instrument_id` so aborted runs resume | 900_333 | Tiingo exposes **no `X-RateLimit-*` headers** — only signal is 429; breaker on 30 consecutive 429s |
| `identity_resolver` | 6 sources: local `company_tickers.json`, SEC mutual-fund JSON, ESMA DB, ADV DB, OpenFIGI, `sec_cusip_ticker_map` | `instrument_identity` (per-field provenance) | weekly | per-field authority merge (`FIELD_AUTHORITY`) | targets missing / `last_resolved_at` NULL or >30d; stamp only on full success | 900_334 | OpenFIGI batch 100, 25 req/6s; after `universe_sync` |
| `live_price_poll` | Yahoo `fast_info` | **Redis only** (not a DB table) | continuous daemon | 60s loop | last-writer-wins | 900_335 | **Out of scope for the data-lake** — writes Redis, not the cloud; document and skip unless an SSE bridge is added |

> `factor_model_fits` is a *derived* table produced by the existing `factor_model`
> metric worker, **not** an ingestion target.

---

## 2. Grouping & dependency order (DAG)

Run order matters because several workers read tables another worker populates.

```
SEC chain:
  sec_bulk_ingestion ─┐
  sec_adv_ingestion ──┼─► sec_13f_ingestion        (managers ≥$100M)
                      └─► nport_fund_discovery ─► nport_ingestion ─► nport_cusip_enrichment
                                              └─► nport_ticker_resolution
  sec_xbrl_facts_ingestion        (independent, local dump)
  form345_ingestion               (independent)
  nport_bulk_load                 (independent of discovery — local DERA dataset)

ESMA chain:
  esma_ingestion (seeds esma_funds/LEIs) ─► firds_ucits_security_sync (needs LEIs)
       ▲                                        │
       └──────── re-reads esma_securities ◄─────┘   (2-pass: funds → FIRDS → ticker phase)

Macro (mostly independent, but macro reads BIS/IMF):
  bis_ingestion ─┐
  imf_ingestion ─┼─► macro_ingestion
  ofr_ingestion  │   (treasury independent)
  treasury_ingestion

Market/identity (the consumer-critical path):
  [SEC catalog tables] + [ESMA tables] ─► universe_sync (pass 1)
        ─► instrument_ingestion (NAV)  ──► universe_sync (pass 2: deactivate-no-NAV)
        ─► tiingo_enrichment
        ─► esma_aum_sync
        ─► identity_resolver
  benchmark_ingest  (independent of universe; driven by allocation_blocks)
```

**Hard ordering rules**
1. `sec_adv_ingestion` → `sec_13f_ingestion` (13F filters on `sec_managers.aum_total`).
2. `nport_fund_discovery` → `nport_ingestion` / `nport_ticker_resolution`.
3. `nport_ingestion`/`nport_bulk_load` → `nport_cusip_enrichment` (needs raw-sector holdings).
4. `esma_ingestion` → `firds_ucits_security_sync` (FIRDS filters by known LEIs).
5. SEC catalog + ESMA tables → `universe_sync` → NAV/identity/enrichment.
6. `bis_ingestion` + `imf_ingestion` → `macro_ingestion` (enrichment; degrades to None).
7. NAV (`instrument_ingestion`) feeds the **second** `universe_sync` pass that
   deactivates tickers with no NAV — schedule universe_sync slightly after NAV, or run
   it twice.

On Railway, ordering is enforced by **cron offsets** (e.g. discovery 02:00, ingestion
03:00) plus each worker's advisory lock + idempotency making accidental overlap safe.

---

## 3. Where the Lean CLI fits (empirically verified)

I probed the running research container `lean_cli_4f9998347d154a86b91a5b560a445296`
(`quantconnect/research:latest`) by reflecting over the loaded `QuantConnect.DataSource.*`
assemblies (bootstrap `from AlgorithmImports import *`, headless via
`PYTHONNET_RUNTIME=coreclr` + the Launcher `runtimeconfig.json` from
`/Lean/Launcher/bin/Debug`). **Datasource DLLs present:**

```
BenzingaNews BitcoinMetadata BrainSentiment CBOE CoinGecko CryptoCoarseFundamental
CryptoSlamNFTSales EODHD ExtractAlpha FearGreedIndex FRED KavoutCompositeFactorBundle
NasdaqDataLink QuiverQuant RegalyticsArticles SEC SmartInsiderIntentionsTransactions
TiingoNews USDAFruitAndVegetables USEnergy USTreasury VIXCentralContango
```

Confirmed relevant types and series groups (verified, not from memory):

- **`QuantConnect.DataSource.Fred`** — exposes named constant groups:
  `OECDRecessionIndicators` (per-country recession bands, e.g.
  `AUSTRALIA_FROM_PEAK_THROUGH_THE_TROUGH`), `CommercialPaper` (AA financial/
  non-financial/asset-backed rates), `CentralBankInterventions`, `ICEBofAML`
  (EM corporate OAS / effective-yield / total-return credit-spread series),
  `LIBOR`, `Wilshire`, `TradeWeightedIndexes`, `CBOE`. This is the **same FRED
  upstream** our `macro_ingestion` already hits.
- **`QuantConnect.DataSource.USTreasuryYieldCurveRate`** — the daily Treasury yield
  curve (overlaps our `treasury_data RATE_*`).
- **`QuantConnect.DataSource.USEnergy`** — EIA energy series.
- **`QuantConnect.DataSource.SEC`** — `SECReport10K/10Q/8K` + submission/filer metadata
  (parsed filings, *not* N-PORT/13F/XBRL-facts).
- **`QuantConnect.DataSource.CBOE`** — VIX & index history; **`VIXCentralContango`** for
  the term structure.
- **`QuantConnect.DataSource.NasdaqDataLink`** (Quandl) — generic keyed connector.

### Verdict per series — primary vs cross-check vs irrelevant

| Lean datasource | Our series | Role |
|---|---|---|
| `Fred` (OECDRecession, ICEBofAML, CommercialPaper, LIBOR) | `macro_data` | **Cross-check / gap-fill.** Same FRED upstream, so not a new primary, but Lean's curated constant enums are a clean **validation oracle** and a backfill path for credit-spread / recession-band series we may not pull today. |
| `USTreasuryYieldCurveRate` | `treasury_data` (`RATE_*`) | **Cross-check.** Validate our Fiscal-Data yield curve against Lean's. |
| `CBOE` / `VIXCentralContango` | (new) macro risk regime input | **Candidate primary** for a VIX/contango series feeding regime detection — not currently a raw table. |
| `USEnergy` | (none) | Optional new macro series; low priority. |
| `SEC` (10K/10Q/8K) | `sec_xbrl_facts` | **Irrelevant as a source** — our XBRL facts come from the SEC companyfacts bulk dump (106M rows); Lean's parsed-report objects don't replace that. Possible qualitative cross-reference only. |
| `nav_timeseries` / `benchmark_nav` | prices | **Not from Lean** — Lean's bundled equity price data is US-centric and not the fund/UCITS NAV universe; Tiingo stays primary. Lean *could* validate US-listed benchmark NAVs (SPY etc.) as a sanity check. |
| ESMA / IMF / BIS / OFR | EU/UCITS, IMF, BIS, OFR | **Irrelevant** — no Lean equivalent. |

**Recommendation:** treat Lean as a **validation/backfill oracle for FRED + Treasury +
CBOE**, not as a primary ingestion path. A small optional `lean_crosscheck` job (run in
the research container, write a discrepancy report — *not* a Railway worker) can compare
`macro_data`/`treasury_data` against Lean's curated series. CBOE/VIX-contango is the one
genuinely *new* primary series worth adding if regime detection needs it.

---

## 4. Implementation pattern (README contract)

Each ingestion worker mirrors the metric-worker shape already in `src/workers/`
(see `risk_metrics.py`): pure helpers + a thin `run()` that owns the transaction, takes
its advisory lock, and upserts idempotently.

```python
# src/workers/<worker>.py
def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Ingest new rows from <source> and upsert into <table>. Returns stats."""
    fetched = upserted = 0
    with connect(dsn) as conn:                      # src.db.connect (psycopg3)
        with advisory_lock(conn, LOCK_<WORKER>) as got:   # 900_3xx
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}
            watermark = _max_date(conn)             # incremental cursor (see below)
            rows = _fetch_source(watermark, limit)  # rate-limited external call
            upserted = _upsert(conn, rows)          # INSERT … ON CONFLICT … (chunked)
            fetched = len(rows)
            conn.commit()
    return {"fetched": fetched, "upserted": upserted, "watermark": str(watermark)}
```

Accompanying files (README §"Contrato"): `schemas/<worker>.sql` (idempotent DDL,
`CREATE TABLE IF NOT EXISTS` + `create_hypertable(..., if_not_exists => TRUE)` where the
target is a hypertable) and `tests/test_<worker>.py` (parse a tiny real fixture, upsert
into a temp/throwaway schema, assert the conflict key + a re-run is a no-op).

### Incremental (watermark) vs full-refresh

| Pattern | When | How |
|---|---|---|
| **Watermark by `max(date)`** | append-only time series: `macro_data`, `treasury_data`, `bis_statistics`, `imf_*`, `ofr_hedge_fund_data`, `nav_timeseries`, `benchmark_nav` | `SELECT max(obs_date) FROM <t> [WHERE series_id=…]`; fetch source only `> watermark` (or watermark − small overlap to catch revisions). Upsert covers revised points via `DO UPDATE`. |
| **Per-entity staleness gate** | per-CIK / per-fund pulls: `nport_fund_discovery` (`data_fetched_at`), `nport_ingestion` (`last_nport_date`), `tiingo_enrichment` (30d), `esma_aum_sync` (7d), `identity_resolver` (30d) | `WHERE last_seen < now() - INTERVAL '<n> days' ORDER BY size DESC LIMIT <cap>` — bounds each run, naturally resumable. |
| **Seen-set / last_seen flip** | `firds_ucits_security_sync` | upsert sets `last_seen_at = now(), is_active=true`; rows untouched this run flip `is_active=false`. **Use DB `now()`** (capture `run_started_at` from the DB, not the host — monolith skew bug). |
| **Full refresh (conflict-key only)** | bulk dumps with no reliable date cursor: `sec_xbrl_facts` (companyfacts), `nport_bulk_load` (DERA quarter), `form345` (quarterly ZIP) | reprocess the whole file; `ON CONFLICT DO NOTHING` makes re-runs cheap. No watermark. |

### Idempotency rules (non-negotiable)
- **Dedup the batch in Python before INSERT** — Postgres rejects two rows with the same
  conflict key in one statement (monolith does this for `macro_data`, `esma_funds`).
- **Chunk** upserts (200–2000) with **per-chunk commit** for fault isolation.
- **Hypertable + compression caveat:** `benchmark_nav` and `sec_nport_holdings` are
  compressed; upserts only land on recent uncompressed chunks. The bulk N-PORT loader
  drops the compression policy + `decompress_chunk` before COPY and re-adds after
  (`scripts/nport_parallel_load.py` `prep()`/`finalize()`). Replicate for any backfill
  that targets old chunks.
- **Matview refreshes** (`mv_nport_sector_attribution`, screener MVs) run
  `REFRESH … CONCURRENTLY` in a **fresh connection outside the advisory lock**.
- **Lock band:** ingestion = **900_3xx**; register each new id in `src/db.py` next to
  `LOCK_RISK_METRICS=900_201` etc. (proposed ids in §1's "Lock" column).

### Railway packaging
Reuse `src/run_worker.py` (one service, `WORKER=<name>`, shared `DATABASE_URL`) and
`railway.toml` (one service per worker, per-service cron). Extend the `run_worker`
allow-list comment to include the ingestion names. Secrets (FRED key, Tiingo token,
OpenFIGI key, SEC User-Agent) via Railway env vars — never committed (§5).

---

## 5. Risks & open decisions for the owner

1. **Tiingo rate limit.** ~~Project memory: real ~130 req/h~~ **REVISED
   2026-06-12 (empirical):** 150 requests in 2.9s with zero 429s — the account
   honours the Power-tier 10k req/h budget. The "~130 req/h" memory was wrong
   (possibly an old key or the News endpoint, which 403s). NAV for ~6.1k active
   tickers via `instrument_ingestion` uses **batch history (one call per unique
   ticker)** — so it is *number-of-tickers* bound, not rows, and the **full
   universe fits one daily run** (~6.1k req, paced 2.5 req/s ≈ 40 min). The
   stale-only watermark sweep stays (it keeps re-runs cheap); the original
   staggering decisions below are moot. **UCITS gap RESOLVED 2026-06-12:**
   `instrument_ingestion` falls through to `src/workers/_fallback_nav.py`
   (EODHD when `EODHD_API_KEY` is set → Yahoo chart API otherwise) for any
   ticker Tiingo returns empty — Yahoo is the proven provider for this slice
   (it fed the pre-existing 622k `source='yahoo'` rows). Rows carry the real
   provider in `source`; option (d) below is implemented, with EODHD as a
   drop-in upgrade once a key exists:
   - **Cadence:** daily full sweep is likely infeasible at 130 req/h (~9k tickers / 130
     ≈ 70 h). Options: (a) **stagger** — only refresh tickers whose `max(nav_date)` is
     stale, prioritised by AUM/usage; (b) split the universe across multiple Railway
     cron slots through the week; (c) buy a higher Tiingo tier; (d) a second provider
     (EODHD is bundled in Lean) for the ~1,442 UCITS European tickers Tiingo returns
     empty for. **Owner call: tier vs. staggering vs. second provider.**
   - No `X-RateLimit-*` headers exist — the only safe signal is **429**; copy the
     monolith's "30 consecutive 429 → abort cleanly, resume next cycle" breaker.
2. **SEC N-PORT: bulk vs per-CIK.** Two coexisting strategies, same table/conflict key.
   Bulk DERA load (`nport_bulk_load`) is full-universe, keeps CUSIP-less/series-less
   holdings (synthetic keys), but is **manual/local-file** (depends on `E:\Edgard\*_nport`
   existing on the box) and quarterly. Per-CIK (`nport_ingestion`) is incremental and
   API-driven but caps ≤200 funds/run and **drops series-less rows**. **Decision:** run
   bulk quarterly as the backbone, per-CIK as the between-quarters top-up? And — does
   Railway have access to the local DERA files, or do we need to host/download them?
3. **Local-file dependencies on Railway.** `sec_xbrl_facts` (companyfacts dump) and
   `nport_bulk_load` (DERA TSVs) read **local disk** in the monolith. Railway containers
   are ephemeral — these need either (a) a download step from SEC into the container, or
   (b) an object-store stage (S3/Tiger volume). **Owner decision: where do the bulk
   dumps live for the Railway workers?**
4. **Secrets on Railway.** Need: `DATABASE_URL`, FRED key, Tiingo token, OpenFIGI key
   (free 25/min vs keyed 250/min — keyed strongly recommended), SEC User-Agent string
   (mandatory), optional OFR auth. Confirm the keyed OpenFIGI plan (the free tier is the
   real bottleneck for ticker/CUSIP enrichment).
5. **`live_price_poll` is Redis-only, not a data-lake table.** It is *not* an ingestion
   target for the cloud. Recommend leaving it in the monolith (or a future SSE service)
   and excluding it from this repo unless the owner wants live prices persisted.
6. **Compression / decompression on writes.** Backfills into old hypertable chunks
   (`benchmark_nav`, `sec_nport_holdings`) require dropping/re-adding the compression
   policy — a privileged, somewhat heavy operation against the production cloud. Confirm
   the worker DB role can `decompress_chunk` / `add_compression_policy`.
7. **ADV URL date guessing & ESMA endpoint drift.** `sec_adv_ingestion` guesses a
   non-deterministic filename date; ESMA Solr/FIRDS URLs change format periodically.
   These are the most fragile sources — add explicit alerting on zero-row runs.
8. **Lean cross-check is research-only.** It runs in the QC container (coreclr headless),
   not on Railway. Decide whether the discrepancy report is worth a scheduled research
   job or stays ad-hoc.

---

## 6. Priority roadmap

Ordered by *consumption unlocked* (what the app/metrics actually need) vs. effort.

### Tier 1 — unblocks the most (build first)
1. **`macro_ingestion` (FRED)** — daily, simple REST, free key, feeds `risk_metrics`
   risk-free rate (`macro_data DFF`) and regime inputs. Lowest effort, high leverage.
2. **`instrument_ingestion` (NAV, Tiingo)** — the spine of `nav_timeseries`, which every
   metric worker reads. Highest value, but gated by the Tiingo rate decision (§5.1) —
   start with the **stale-only, AUM-prioritised** sweep.
3. **`benchmark_ingest` (Tiingo)** — `benchmark_nav` feeds beta/alpha/TE/IR in
   `risk_metrics`. Small universe, cheap.
4. **`treasury_ingestion`** — `treasury_data` (yield curve / rates); cheap, validates
   against Lean.

### Tier 2 — depth for screening & characteristics
5. **`nport_bulk_load`** (backbone) + **`nport_fund_discovery`** — `sec_nport_holdings`
   feeds `characteristics`/`factor_model`. Resolve the local-file/Railway question (§5.3).
6. **`universe_sync`** — keeps `instruments_universe` coherent; must precede NAV's
   active-set logic. Mostly DB-internal SQL, low external risk.
7. **`sec_adv_ingestion` → `sec_13f_ingestion`** — managers + 13F holdings.
8. **`esma_ingestion` → `firds_ucits_security_sync`** — the UCITS side of the universe.

### Tier 3 — enrichment & lower-frequency
9. `nport_ticker_resolution`, `nport_cusip_enrichment` (OpenFIGI/Tiingo sectors).
10. `tiingo_enrichment`, `esma_aum_sync`, `identity_resolver`.
11. `imf_ingestion`, `bis_ingestion`, `ofr_ingestion` (quarterly macro depth).
12. `sec_xbrl_facts_ingestion` (106M; valuable for fundamentals but heavy — do once the
    bulk-file hosting story is settled).

### Optional / deferred
- `form345_ingestion` (insider sentiment) — independent, nice-to-have.
- `sec_bulk_ingestion` (N-CEN/N-MFP/BDC slices) — needed for ETF/MMF/BDC catalog depth.
- `lean_crosscheck` — research-only validation oracle (§3).
- `live_price_poll` — excluded unless live prices are persisted (§5.5).

---

## Appendix — advisory lock ids proposed (900_3xx ingestion band)

| Worker | Lock | Worker | Lock |
|---|---|---|---|
| sec_adv_ingestion | 900_301 | esma_aum_sync | 900_312 |
| nport_fund_discovery | 900_302 | macro_ingestion | 900_320 |
| nport_ingestion | 900_303 | imf_ingestion | 900_321 |
| sec_xbrl_facts_ingestion | 900_304 | bis_ingestion | 900_322 |
| sec_13f_ingestion | 900_305 | ofr_ingestion | 900_323 |
| form345_ingestion | 900_306 | treasury_ingestion | 900_324 |
| nport_ticker_resolution | 900_307 | universe_sync | 900_330 |
| nport_cusip_enrichment | 900_308 | instrument_ingestion | 900_331 |
| sec_bulk_ingestion | 900_309 | benchmark_ingest | 900_332 |
| firds_ucits_security_sync | 900_310 | tiingo_enrichment | 900_333 |
| esma_ingestion | 900_311 | identity_resolver | 900_334 |
| | | live_price_poll (if used) | 900_335 |

> `nport_bulk_load` intentionally takes **no advisory lock** (parallel COPY workers must
> not serialize; idempotency via the conflict key is the only guard) — mirrors
> `scripts/nport_parallel_load.py`.

Reserved/in-use elsewhere (do **not** reuse): metrics 900_201–203; monolith 43,
900_004/010/011/012/014/015/018/021/022/023/024/050/051/060/061/070/100/110/111/125/126.
