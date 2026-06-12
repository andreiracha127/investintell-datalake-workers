-- treasury_ingestion worker — target table (idempotent DDL).
-- Extracted from the cloud data-lake (Tiger Investintell-Prod); the table
-- already exists in production as a compressed hypertable.

CREATE TABLE IF NOT EXISTS treasury_data (
    obs_date      DATE           NOT NULL,
    series_id     VARCHAR(80)    NOT NULL,
    value         NUMERIC(24,6),
    source        VARCHAR(40)    NOT NULL DEFAULT 'treasury_api',
    metadata_json JSONB,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (obs_date, series_id)
);

SELECT create_hypertable('treasury_data', 'obs_date',
                         chunk_time_interval => INTERVAL '1 month',
                         if_not_exists => TRUE);
