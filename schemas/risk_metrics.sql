-- risk_metrics worker — destination tables (idempotent DDL)
-- Extracted from the DB-mãe (investintell_alloc @ localhost:5434) and adjusted
-- for the standalone data-lake. Both tables are TimescaleDB hypertables in the
-- mother DB; we recreate them as hypertables here when the timescaledb
-- extension is available, otherwise plain tables (fully functional for upsert).
--
-- Apply against the cloud:  psql "$DATABASE_URL" -f schemas/risk_metrics.sql
--
-- Conventions reproduced from the legacy schema:
--   * fund_risk_metrics  : numeric(10,6) for most ratios; unique natural key
--     (instrument_id, calc_date, organization_id) with NULLS NOT DISTINCT so a
--     global (org-less) row upserts cleanly.
--   * sec_mmf_metrics    : PK (metric_date, series_id, class_id).

-- ─────────────────────────────────────────────────────────────────────────────
-- fund_risk_metrics
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fund_risk_metrics (
    instrument_id                    uuid          NOT NULL,
    calc_date                        date          NOT NULL,
    organization_id                  uuid,

    -- VaR / CVaR (empirical, 95%) per rolling window
    cvar_95_1m                       numeric(10,6),
    cvar_95_3m                       numeric(10,6),
    cvar_95_6m                       numeric(10,6),
    cvar_95_12m                      numeric(10,6),
    var_95_1m                        numeric(10,6),
    var_95_3m                        numeric(10,6),
    var_95_6m                        numeric(10,6),
    var_95_12m                       numeric(10,6),

    -- Cumulative / annualized returns
    return_1m                        numeric(10,6),
    return_3m                        numeric(10,6),
    return_6m                        numeric(10,6),
    return_1y                        numeric(10,6),
    return_3y_ann                    numeric(10,6),
    return_5y_ann                    numeric(12,8),
    return_10y_ann                   numeric(12,8),

    -- Volatility / drawdown
    volatility_1y                    numeric(10,6),
    volatility_garch                 numeric(10,6),
    vol_model                        varchar,
    max_drawdown_1y                  numeric(10,6),
    max_drawdown_3y                  numeric(10,6),

    -- Risk-adjusted ratios
    sharpe_1y                        numeric(10,6),
    sharpe_3y                        numeric(10,6),
    sortino_1y                       numeric(10,6),
    calmar_ratio_3y                  numeric(8,4),

    -- Benchmark-relative (regression vs benchmark_nav)
    alpha_1y                         numeric(10,6),
    beta_1y                          numeric(10,6),
    tracking_error_1y                numeric(10,6),
    information_ratio_1y             numeric(10,6),
    upside_capture_1y                numeric(8,4),
    downside_capture_1y              numeric(8,4),

    -- Cornish-Fisher robust Sharpe
    sharpe_cf                        numeric(10,6),
    sharpe_cf_skew                   numeric(10,6),
    sharpe_cf_kurt                   numeric(10,6),
    sharpe_cf_ci_lower               numeric(10,6),
    sharpe_cf_ci_upper               numeric(10,6),

    -- EVT extreme tail (POT-GPD)
    cvar_99_evt                      numeric(12,6),
    cvar_999_evt                     numeric(12,6),
    evt_xi_shape                     numeric(12,6),

    -- Provenance / quality
    fed_funds_rate_at_calc           numeric(8,4),
    data_quality_flags               jsonb DEFAULT '{}'::jsonb,

    CONSTRAINT ux_fund_risk_metrics_pk
        UNIQUE NULLS NOT DISTINCT (instrument_id, calc_date, organization_id)
);

CREATE INDEX IF NOT EXISTS fund_risk_metrics_calc_date_idx
    ON fund_risk_metrics USING btree (calc_date DESC);

-- Promote to hypertable on calc_date (7-day chunks, matching the mother DB).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'fund_risk_metrics', 'calc_date',
            chunk_time_interval => INTERVAL '7 days',
            if_not_exists => TRUE,
            migrate_data => TRUE
        );
    END IF;
END$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- sec_mmf_metrics  (money-market fund liquidity / yield, from N-MFP filings)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sec_mmf_metrics (
    metric_date                      date         NOT NULL,
    series_id                        varchar      NOT NULL,
    class_id                         varchar      NOT NULL,
    accession_number                 varchar      NOT NULL,
    seven_day_net_yield              numeric(8,4),
    daily_gross_subscriptions        numeric(20,2),
    daily_gross_redemptions          numeric(20,2),
    pct_daily_liquid                 numeric(8,4),
    pct_weekly_liquid                numeric(8,4),
    total_daily_liquid_assets        numeric(20,2),
    total_weekly_liquid_assets       numeric(20,2),
    CONSTRAINT sec_mmf_metrics_pkey PRIMARY KEY (metric_date, series_id, class_id)
);

CREATE INDEX IF NOT EXISTS sec_mmf_metrics_metric_date_idx
    ON sec_mmf_metrics USING btree (metric_date DESC);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'sec_mmf_metrics', 'metric_date',
            chunk_time_interval => INTERVAL '30 days',
            if_not_exists => TRUE,
            migrate_data => TRUE
        );
    END IF;
END$$;
