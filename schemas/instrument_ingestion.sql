-- instrument_ingestion worker — target table (idempotent DDL).
-- nav_timeseries already exists in the cloud (27.4M rows) as a compressed
-- hypertable; instruments_universe (read-only source of the active universe)
-- is owned by universe_sync and not created here.

CREATE TABLE IF NOT EXISTS nav_timeseries (
    instrument_id UUID          NOT NULL,
    nav_date      DATE          NOT NULL,
    nav           NUMERIC(18,6),
    return_1d     NUMERIC(12,8),
    aum_usd       NUMERIC(18,2),
    currency      VARCHAR(3),
    source        VARCHAR(30)   DEFAULT 'tiingo',
    return_type   VARCHAR(10)   NOT NULL DEFAULT 'arithmetic',
    PRIMARY KEY (instrument_id, nav_date)
);

SELECT create_hypertable('nav_timeseries', 'nav_date',
                         chunk_time_interval => INTERVAL '1 month',
                         if_not_exists => TRUE);
