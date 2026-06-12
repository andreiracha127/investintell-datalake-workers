-- characteristics.sql — destination tables for src/workers/characteristics.py
--
-- Reproduced 1:1 from the legacy DB-mãe (investintell_alloc @ localhost:5434):
--   \d company_characteristics_monthly  → PK (cik, period_end), hypertable on period_end
--   \d equity_characteristics_monthly   → PK (instrument_id, as_of), hypertable on as_of
--
-- Idempotent: safe to run repeatedly (CREATE TABLE IF NOT EXISTS + guarded
-- create_hypertable). NUMERIC scales match the legacy schema exactly so a
-- recompute round-trips without silent truncation.
--
-- Apply to cloud:
--   psql "$DATABASE_URL" -f schemas/characteristics.sql

-- ---------------------------------------------------------------------------
-- Layer 1 — company (issuer) fundamentals, one row per (cik, fiscal period).
-- Source of truth in the legacy pipeline: sec_xbrl_facts (us-gaap / dei).
-- Money amounts are full NUMERIC (no scale) because they are raw USD figures;
-- derived ratios are unconstrained NUMERIC in the legacy table as well.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS company_characteristics_monthly (
    cik                 BIGINT      NOT NULL,
    period_end          DATE        NOT NULL,
    fp                  TEXT,
    book_equity         NUMERIC,
    total_assets        NUMERIC,
    net_income_ttm      NUMERIC,
    revenue             NUMERIC,
    cost_of_revenue     NUMERIC,
    gross_profit        NUMERIC,
    capex_ttm           NUMERIC,
    ppe_prior           NUMERIC,
    shares_outstanding  NUMERIC,
    quality_roa         NUMERIC,
    investment_growth   NUMERIC,
    profitability_gross NUMERIC,
    source_filing_date  DATE,
    source_accn         TEXT,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cik, period_end)
);

-- ---------------------------------------------------------------------------
-- Layer 2 — fund/equity-instrument characteristics, one row per (instrument, date).
-- Derived by aggregating Layer-1 company chars over the fund's N-PORT equity
-- holdings (asset_class IN ('EC','EP')) plus the fund's own NAV for momentum.
-- All six chars are NUMERIC(10,4) in the legacy schema → round to 4 dp.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS equity_characteristics_monthly (
    instrument_id       UUID         NOT NULL,
    ticker              TEXT         NOT NULL,
    as_of               DATE         NOT NULL,
    size_log_mkt_cap    NUMERIC(10,4),
    book_to_market      NUMERIC(10,4),
    mom_12_1            NUMERIC(10,4),
    quality_roa         NUMERIC(10,4),
    investment_growth   NUMERIC(10,4),
    profitability_gross NUMERIC(10,4),
    source_filing_date  DATE,
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, as_of)
);

-- ---------------------------------------------------------------------------
-- Hypertables (TimescaleDB). Partition on the date column that is part of the
-- PK so the unique constraint is satisfied. Guarded: if_not_exists + a probe
-- so re-runs and non-Timescale targets do not error.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'company_characteristics_monthly', 'period_end',
            chunk_time_interval => INTERVAL '365 days',
            if_not_exists => TRUE, migrate_data => TRUE
        );
        PERFORM create_hypertable(
            'equity_characteristics_monthly', 'as_of',
            chunk_time_interval => INTERVAL '365 days',
            if_not_exists => TRUE, migrate_data => TRUE
        );
    END IF;
END $$;
