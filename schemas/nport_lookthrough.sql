-- nport_lookthrough worker — destination tables (idempotent DDL)
-- Materializes recursive look-through exposures computed from sec_nport_holdings
-- (96M rows, cloud). One row per (series, report, dimension, key); a companion
-- summary row per (series, report) carries the explicit residual buckets and
-- chain staleness. The Light app consumes these tables directly (DB-first) —
-- no look-through math ever runs in a request path.
--
-- Apply against the cloud:  psql "$DATABASE_URL" -f schemas/nport_lookthrough.sql
--
-- Conventions:
--   * pct columns are in percentage points of the PARENT series NAV, sign
--     preserved (shorts negative). Σpct > 100 is legitimate (derivatives /
--     leverage) and is NEVER renormalized.
--   * dimension ∈ ('issuer','asset_class','sector','currency').
--     issuer keys: CUSIP-6 for real/embedded CUSIPs, the synthetic key itself
--     ('IS:…','LE:…','H:…','CIK:…') otherwise; categorical NULLs → 'UNKNOWN'.
--   * coverage_pct in the summary is COPIED from cagg_nport_series_profile
--     (never recomputed here).

-- ─────────────────────────────────────────────────────────────────────────────
-- nport_lookthrough_exposures
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nport_lookthrough_exposures (
    series_id        text          NOT NULL,
    report_date      date          NOT NULL,
    dimension        text          NOT NULL,
    key              text          NOT NULL,
    label            text,
    direct_pct       numeric(14,6) NOT NULL DEFAULT 0,
    indirect_pct     numeric(14,6) NOT NULL DEFAULT 0,
    computed_at      timestamptz   NOT NULL DEFAULT now(),

    CONSTRAINT ux_nport_lookthrough_exposures
        UNIQUE (series_id, report_date, dimension, key)
);

CREATE INDEX IF NOT EXISTS nport_lookthrough_exposures_series_idx
    ON nport_lookthrough_exposures USING btree (series_id, report_date DESC);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'nport_lookthrough_exposures', 'report_date',
            chunk_time_interval => INTERVAL '1 month',
            if_not_exists => TRUE,
            migrate_data => TRUE
        );
    END IF;
END$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- nport_lookthrough_summary  (residual explícito + staleness em cadeia)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nport_lookthrough_summary (
    series_id                 text          NOT NULL,
    report_date               date          NOT NULL,

    -- totais (pontos percentuais do NAV da série-mãe; nunca renormalizados)
    sum_pct_total             numeric(14,6),
    direct_pct                numeric(14,6),
    indirect_pct              numeric(14,6),

    -- residual explícito
    expanded_fund_pct         numeric(14,6),  -- posições-fundo substituídas pelo look-through
    nondecomposable_fund_pct  numeric(14,6),  -- fundo casado sem dados / ciclo / limite de profundidade
    derivatives_gross_pct     numeric(14,6),  -- Σ|pct| das classes derivativas (D*, exceto DBT)
    derivatives_net_pct       numeric(14,6),  -- Σpct  das classes derivativas
    unidentified_pct          numeric(14,6),  -- chaves sintéticas LE:/H:/CIK: (não-identificáveis)

    -- proveniência / qualidade
    coverage_pct              numeric(16,6),  -- copiado de cagg_nport_series_profile
    n_holdings                integer,
    n_children_expanded       integer,
    oldest_report_date        date,           -- staleness em cadeia (report mais antigo usado)
    computed_at               timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT ux_nport_lookthrough_summary
        UNIQUE (series_id, report_date)
);

CREATE INDEX IF NOT EXISTS nport_lookthrough_summary_series_idx
    ON nport_lookthrough_summary USING btree (series_id, report_date DESC);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'nport_lookthrough_summary', 'report_date',
            chunk_time_interval => INTERVAL '1 month',
            if_not_exists => TRUE,
            migrate_data => TRUE
        );
    END IF;
END$$;
