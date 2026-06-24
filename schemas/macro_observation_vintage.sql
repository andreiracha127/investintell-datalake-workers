-- macro_vintage worker — point-in-time vintage store for the macro quadrant.
-- One row per REAL revision: vintages whose value did not change are compressed
-- away upstream. Coexists with macro_data (latest-revision); never overwrites.
CREATE TABLE IF NOT EXISTS macro_observation_vintage (
    series_id           VARCHAR(30)   NOT NULL,
    observation_period  DATE          NOT NULL,   -- the economic date (obs date)
    vintage_date        DATE          NOT NULL,   -- ALFRED realtime date the value first appeared
    value               NUMERIC(24,6) NOT NULL,
    available_at        TIMESTAMPTZ   NOT NULL,   -- when the value became knowable (vintage_date 00:00 UTC)
    revision_number     INTEGER       NOT NULL,   -- 0 = first print, 1,2,... per (series_id, observation_period)
    source              VARCHAR(30)   NOT NULL DEFAULT 'alfred',
    source_spec_version VARCHAR(40)   NOT NULL,
    ingested_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, observation_period, vintage_date)
);

SELECT create_hypertable('macro_observation_vintage', 'observation_period',
                         chunk_time_interval => INTERVAL '1 year',
                         if_not_exists => TRUE);

-- Point-in-time read: per series, walk back from a decision time over available_at.
CREATE INDEX IF NOT EXISTS idx_mov_pit
    ON macro_observation_vintage (series_id, available_at DESC, observation_period DESC);
