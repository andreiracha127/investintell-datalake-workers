-- risk_metrics worker destination tables and latest read model.
--
-- Apply against the cloud:  psql "$DATABASE_URL" -f schemas/risk_metrics.sql
--
-- Column ownership:
--   risk_metrics.py        : daily NAV risk, benchmark-relative metrics,
--                            FI/alternatives regressions, NAV quality,
--                            peer percentiles/bands, equity manager_score.
--   active_share_metrics.py: N-PORT holdings overlap, benchmark ids,
--                            report dates and report-age snapshot fields.
--   momentum_metrics.py    : deterministic NAV momentum columns.
--   reserved/latent        : not exposed by fund_risk_latest_mv until a product
--                            owner and public contract exist. MMF columns may
--                            exist in Tiger as latent dump artifacts but are
--                            intentionally not managed by this schema.

-- The live MV depends on base-table column types, and funds_list_mv depends on
-- the live MV. Drop/recreate both around this parity DDL so type corrections do
-- not hit SQLSTATE 0A000.
DROP MATERIALIZED VIEW IF EXISTS funds_list_mv;
DROP MATERIALIZED VIEW IF EXISTS fund_risk_latest_mv;

CREATE TABLE IF NOT EXISTS fund_risk_metrics (
    instrument_id uuid NOT NULL,
    calc_date date NOT NULL,
    organization_id uuid,

    -- owner: risk_metrics.py / base risk
    cvar_95_1m numeric(10,6),
    cvar_95_3m numeric(10,6),
    cvar_95_6m numeric(10,6),
    cvar_95_12m numeric(10,6),
    var_95_1m numeric(10,6),
    var_95_3m numeric(10,6),
    var_95_6m numeric(10,6),
    var_95_12m numeric(10,6),
    return_1m numeric(10,6),
    return_3m numeric(10,6),
    return_6m numeric(10,6),
    return_1y numeric(10,6),
    return_3y_ann numeric(10,6),
    return_5y_ann numeric(12,8),
    return_10y_ann numeric(12,8),
    volatility_1y numeric(10,6),
    volatility_garch numeric(10,6),
    vol_model varchar,
    max_drawdown_1y numeric(10,6),
    max_drawdown_3y numeric(10,6),
    sharpe_1y numeric(10,6),
    sharpe_3y numeric(10,6),
    sortino_1y numeric(10,6),
    calmar_ratio_3y numeric(8,4),

    -- owner: risk_metrics.py / benchmark-relative
    alpha_1y numeric(10,6),
    beta_1y numeric(10,6),
    tracking_error_1y numeric(10,6),
    information_ratio_1y numeric(10,6),
    upside_capture_1y numeric(8,4),
    downside_capture_1y numeric(8,4),

    -- owner: risk_metrics.py / robust Sharpe and EVT
    sharpe_cf numeric(10,6),
    sharpe_cf_skew numeric(10,6),
    sharpe_cf_kurt numeric(10,6),
    sharpe_cf_ci_lower numeric(10,6),
    sharpe_cf_ci_upper numeric(10,6),
    cvar_99_evt numeric(12,6),
    cvar_999_evt numeric(12,6),
    evt_xi_shape numeric(12,6),
    fed_funds_rate_at_calc numeric(8,4),
    data_quality_flags jsonb DEFAULT '{}'::jsonb,

    -- owner: risk_metrics.py / set-based peer and equity scoring post-steps
    peer_strategy_label text,
    peer_sharpe_pctl numeric(5,2),
    peer_sortino_pctl numeric(5,2),
    peer_return_pctl numeric(5,2),
    peer_drawdown_pctl numeric(5,2),
    peer_count integer,
    manager_score numeric(5,2),

    -- owner: reserved
    elite_flag boolean,

    -- owner: risk_metrics.py / class regressions and NAV quality
    equity_correlation_252d numeric(6,4),
    empirical_duration numeric(10,6),
    credit_beta numeric(10,6),
    crisis_alpha_score numeric(10,6),
    inflation_beta numeric(10,6),

    -- owner: active_share_metrics.py / N-PORT snapshot
    active_share_normalized numeric(10,6),
    overlap_normalized numeric(10,6),
    overlap_nav_raw numeric(10,6),
    fund_cusip_coverage_nav numeric(10,6),
    benchmark_cusip_coverage_nav numeric(10,6),
    n_fund_holdings integer,
    n_benchmark_holdings integer,
    n_common_holdings integer,
    n_fund_only integer,
    n_benchmark_only integer,
    holdings_jaccard numeric(10,6),
    fund_report_age_days integer,
    benchmark_report_age_days integer,
    report_date_gap_days integer,
    active_share_benchmark_instrument_id uuid,
    active_share_benchmark_series_id text,
    active_share_fund_report_date date,
    active_share_benchmark_report_date date,

    -- owner: reserved
    score_components jsonb,

    -- owner: momentum_metrics.py
    dtw_drift_score numeric,
    rsi_14 numeric,
    bb_position numeric,
    nav_momentum_score numeric,
    flow_momentum_score numeric,
    blended_momentum_score numeric,

    -- owner: reserved
    cvar_95_conditional numeric,
    elite_rank_within_strategy smallint,
    elite_target_count_per_strategy smallint,

    -- owner: risk_metrics.py / class-regression quality
    empirical_duration_r2 numeric,
    credit_beta_r2 numeric,

    -- owner: reserved
    yield_proxy_12m numeric,
    duration_adj_drawdown_1y numeric,

    -- owner: risk_metrics.py / model provenance
    scoring_model varchar,

    -- owner: risk_metrics.py / alternatives regression quality and peer bands
    inflation_beta_r2 numeric,
    peer_overall_quartile smallint,
    peer_band_low numeric,
    peer_band_mid numeric,
    peer_band_high numeric,
    nav_quality_ok boolean,
    nav_glitch_count integer,

    CONSTRAINT ux_fund_risk_metrics_pk
        UNIQUE NULLS NOT DISTINCT (instrument_id, calc_date, organization_id)
);

-- Existing deployments may have an older table. Keep the additive path explicit
-- so the repo schema declares every live Tiger column.
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_strategy_label text;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_sharpe_pctl numeric(5,2);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_sortino_pctl numeric(5,2);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_return_pctl numeric(5,2);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_drawdown_pctl numeric(5,2);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_count integer;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS manager_score numeric(5,2);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS elite_flag boolean;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS equity_correlation_252d numeric(6,4);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS empirical_duration numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS credit_beta numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS crisis_alpha_score numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS inflation_beta numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS active_share_normalized numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS overlap_normalized numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS overlap_nav_raw numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS fund_cusip_coverage_nav numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS benchmark_cusip_coverage_nav numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS n_fund_holdings integer;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS n_benchmark_holdings integer;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS n_common_holdings integer;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS n_fund_only integer;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS n_benchmark_only integer;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS holdings_jaccard numeric(10,6);
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS fund_report_age_days integer;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS benchmark_report_age_days integer;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS report_date_gap_days integer;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS active_share_benchmark_instrument_id uuid;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS active_share_benchmark_series_id text;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS active_share_fund_report_date date;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS active_share_benchmark_report_date date;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS score_components jsonb;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS dtw_drift_score numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS rsi_14 numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS bb_position numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS nav_momentum_score numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS flow_momentum_score numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS blended_momentum_score numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS cvar_95_conditional numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS elite_rank_within_strategy smallint;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS elite_target_count_per_strategy smallint;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS empirical_duration_r2 numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS credit_beta_r2 numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS yield_proxy_12m numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS duration_adj_drawdown_1y numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS scoring_model varchar;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS inflation_beta_r2 numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_overall_quartile smallint;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_band_low numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_band_mid numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS peer_band_high numeric;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS nav_quality_ok boolean;
ALTER TABLE fund_risk_metrics ADD COLUMN IF NOT EXISTS nav_glitch_count integer;

-- Type parity corrections. The MV was dropped above so these are view-safe.
ALTER TABLE fund_risk_metrics ALTER COLUMN empirical_duration TYPE numeric(10,6) USING empirical_duration::numeric(10,6);
ALTER TABLE fund_risk_metrics ALTER COLUMN credit_beta TYPE numeric(10,6) USING credit_beta::numeric(10,6);
ALTER TABLE fund_risk_metrics ALTER COLUMN inflation_beta TYPE numeric(10,6) USING inflation_beta::numeric(10,6);
ALTER TABLE fund_risk_metrics ALTER COLUMN empirical_duration_r2 TYPE numeric USING empirical_duration_r2::numeric;
ALTER TABLE fund_risk_metrics ALTER COLUMN credit_beta_r2 TYPE numeric USING credit_beta_r2::numeric;
ALTER TABLE fund_risk_metrics ALTER COLUMN inflation_beta_r2 TYPE numeric USING inflation_beta_r2::numeric;
ALTER TABLE fund_risk_metrics ALTER COLUMN scoring_model TYPE varchar USING scoring_model::varchar;

CREATE INDEX IF NOT EXISTS fund_risk_metrics_calc_date_idx
    ON fund_risk_metrics USING btree (calc_date DESC);

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

-- Latest API read model. Exposes only recurring-worker columns plus the
-- active-share snapshot family with explicit report-date/age metadata.
CREATE MATERIALIZED VIEW fund_risk_latest_mv AS
SELECT DISTINCT ON (instrument_id)
    instrument_id,
    calc_date,
    return_1m,
    return_3m,
    return_1y,
    return_3y_ann,
    return_5y_ann,
    volatility_1y,
    volatility_garch,
    vol_model,
    max_drawdown_1y,
    max_drawdown_3y,
    sharpe_1y,
    sharpe_3y,
    sortino_1y,
    calmar_ratio_3y,
    alpha_1y,
    beta_1y,
    information_ratio_1y,
    tracking_error_1y,
    var_95_1m,
    cvar_95_1m,
    cvar_95_12m,
    cvar_99_evt,
    cvar_999_evt,
    evt_xi_shape,
    peer_sharpe_pctl,
    peer_sortino_pctl,
    peer_return_pctl,
    peer_drawdown_pctl,
    peer_overall_quartile,
    peer_band_low,
    peer_band_mid,
    peer_band_high,
    manager_score,
    downside_capture_1y,
    upside_capture_1y,
    equity_correlation_252d,
    peer_strategy_label,
    peer_count,
    empirical_duration,
    empirical_duration_r2,
    credit_beta,
    credit_beta_r2,
    inflation_beta,
    inflation_beta_r2,
    crisis_alpha_score,
    scoring_model,
    active_share_normalized,
    overlap_normalized,
    overlap_nav_raw,
    fund_cusip_coverage_nav,
    benchmark_cusip_coverage_nav,
    n_fund_holdings,
    n_benchmark_holdings,
    n_common_holdings,
    n_fund_only,
    n_benchmark_only,
    holdings_jaccard,
    fund_report_age_days,
    benchmark_report_age_days,
    report_date_gap_days,
    active_share_benchmark_instrument_id,
    active_share_benchmark_series_id,
    active_share_fund_report_date,
    active_share_benchmark_report_date,
    dtw_drift_score,
    rsi_14,
    bb_position,
    nav_momentum_score,
    flow_momentum_score,
    blended_momentum_score,
    nav_quality_ok,
    nav_glitch_count
FROM fund_risk_metrics
WHERE organization_id IS NULL
ORDER BY instrument_id, calc_date DESC;

CREATE UNIQUE INDEX IF NOT EXISTS fund_risk_latest_mv_pk
    ON fund_risk_latest_mv (instrument_id);

-- Funds list read model depends on fund_risk_latest_mv in Tiger. Keep the
-- existing list contract view-aware, but do not re-source reserved risk fields
-- from fund_risk_latest_mv. elite_flag is retained as an explicit null
-- compatibility column until a scoring owner/product contract exists.
CREATE MATERIALIZED VIEW funds_list_mv AS
WITH nav_staleness AS (
    SELECT max(nav_timeseries.nav_date) AS source_nav_max_date
    FROM nav_timeseries
)
SELECT
    f.instrument_id,
    f.series_id,
    f.ticker,
    f.name,
    f.fund_type,
    f.strategy_label,
    f.asset_class,
    f.is_index,
    f.expense_ratio,
    f.aum_usd,
    f.inception_date,
    r.calc_date,
    ns.source_nav_max_date,
    r.return_1m,
    r.return_3m,
    r.return_1y,
    r.return_3y_ann,
    r.return_5y_ann,
    r.volatility_1y,
    r.max_drawdown_1y,
    r.max_drawdown_3y,
    r.sharpe_1y,
    r.sharpe_3y,
    r.sortino_1y,
    r.calmar_ratio_3y,
    r.alpha_1y,
    r.beta_1y,
    r.information_ratio_1y,
    r.tracking_error_1y,
    r.var_95_1m,
    r.cvar_95_1m,
    r.cvar_95_12m,
    r.cvar_99_evt,
    r.peer_strategy_label,
    r.peer_sharpe_pctl,
    r.peer_sortino_pctl,
    r.peer_return_pctl,
    r.peer_drawdown_pctl,
    r.peer_count,
    r.manager_score,
    r.blended_momentum_score,
    NULL::boolean AS elite_flag,
    r.downside_capture_1y,
    r.upside_capture_1y,
    r.equity_correlation_252d
FROM funds_v f
LEFT JOIN fund_risk_latest_mv r ON r.instrument_id = f.instrument_id
CROSS JOIN nav_staleness ns
WHERE f.strategy_label IS DISTINCT FROM 'Unclassified'::text;

CREATE UNIQUE INDEX IF NOT EXISTS funds_list_mv_pk
    ON funds_list_mv (instrument_id);
CREATE INDEX IF NOT EXISTS funds_list_mv_filters_idx
    ON funds_list_mv (fund_type, asset_class, strategy_label);
CREATE INDEX IF NOT EXISTS funds_list_mv_risk_filters_idx
    ON funds_list_mv (return_1y, volatility_1y, max_drawdown_1y);
CREATE INDEX IF NOT EXISTS funds_list_mv_ticker_sort_idx
    ON funds_list_mv (ticker, instrument_id);
CREATE INDEX IF NOT EXISTS funds_list_mv_name_sort_idx
    ON funds_list_mv (name, instrument_id);
CREATE INDEX IF NOT EXISTS funds_list_mv_aum_sort_idx
    ON funds_list_mv (aum_usd DESC NULLS LAST, ticker, instrument_id);
CREATE INDEX IF NOT EXISTS funds_list_mv_sharpe_sort_idx
    ON funds_list_mv (sharpe_1y DESC NULLS LAST, ticker, instrument_id);
