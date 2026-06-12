-- macro_ingestion worker — target tables (idempotent DDL).
-- Extracted from the cloud data-lake (Tiger Investintell-Prod); both tables
-- already exist in production as compressed hypertables. This file documents
-- the contract and bootstraps fresh environments.

CREATE TABLE IF NOT EXISTS macro_data (
    series_id   VARCHAR(30)    NOT NULL,
    obs_date    DATE           NOT NULL,
    value       NUMERIC(24,6)  NOT NULL,
    source      VARCHAR(30)    DEFAULT 'fred',
    is_derived  BOOLEAN        NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    created_by  VARCHAR,
    updated_by  VARCHAR,
    PRIMARY KEY (series_id, obs_date)
);

SELECT create_hypertable('macro_data', 'obs_date',
                         chunk_time_interval => INTERVAL '1 month',
                         if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS macro_regional_snapshots (
    id          UUID        NOT NULL DEFAULT gen_random_uuid(),
    as_of_date  DATE        NOT NULL,
    data_json   JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now(),
    created_by  VARCHAR,
    updated_by  VARCHAR,
    PRIMARY KEY (as_of_date, id)
);

-- The upsert conflict target: one snapshot per as-of date.
CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_regional_snapshots_as_of_date
    ON macro_regional_snapshots (as_of_date);

SELECT create_hypertable('macro_regional_snapshots', 'as_of_date',
                         chunk_time_interval => INTERVAL '1 month',
                         if_not_exists => TRUE);
