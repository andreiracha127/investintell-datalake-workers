-- benchmark_ingest worker — target table + block→ticker reference (idempotent DDL).
-- benchmark_nav already exists in the cloud as a compressed hypertable.
-- allocation_blocks does NOT exist in the cloud: the worker falls back to its
-- embedded DEFAULT_BLOCK_TICKERS map. Applying this DDL (and seeding it from
-- the mother DB) lets the owner override the embedded map without a deploy.

CREATE TABLE IF NOT EXISTS benchmark_nav (
    block_id    VARCHAR(80)   NOT NULL,
    nav_date    DATE          NOT NULL,
    nav         NUMERIC(18,6) NOT NULL,
    return_1d   NUMERIC(12,8),
    return_type VARCHAR(10)   NOT NULL DEFAULT 'log',
    source      VARCHAR(30)   NOT NULL DEFAULT 'tiingo',
    created_at  TIMESTAMPTZ   DEFAULT now(),
    updated_at  TIMESTAMPTZ   DEFAULT now(),
    PRIMARY KEY (block_id, nav_date)
);

SELECT create_hypertable('benchmark_nav', 'nav_date',
                         chunk_time_interval => INTERVAL '1 month',
                         if_not_exists => TRUE);

-- Optional override for the embedded block→ticker map (mother-DB slice).
CREATE TABLE IF NOT EXISTS allocation_blocks (
    block_id         VARCHAR(80) PRIMARY KEY,
    benchmark_ticker VARCHAR(20),
    is_active        BOOLEAN NOT NULL DEFAULT true,
    display_name     VARCHAR,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
