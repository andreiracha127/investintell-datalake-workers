-- Immutable direct-input captures for newly published live Open Macro v04 decisions.
-- Historical/bootstrap decisions are intentionally never backfilled from macro_data.
CREATE TABLE IF NOT EXISTS open_macro_v04_decision_input_captures (
    as_of                    date        NOT NULL,
    series_id                text        NOT NULL CHECK (series_id IN (
        'MTSDS133FMS', 'GDP', 'SUBLPDCILSLGNQ', 'M2SL'
    )),
    series_digest_sha256     char(64)    NOT NULL
        CHECK (series_digest_sha256 ~ '^[0-9a-f]{64}$'),
    row_count                integer     NOT NULL CHECK (row_count > 0),
    min_obs_date             date        NOT NULL,
    max_obs_date             date        NOT NULL,
    producer_run_id          text        NOT NULL CHECK (producer_run_id <> ''),
    global_input_digest_sha256 char(64)  NOT NULL
        CHECK (global_input_digest_sha256 ~ '^[0-9a-f]{64}$'),
    captured_at              timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT open_macro_v04_decision_input_captures_pkey
        PRIMARY KEY (as_of, series_id),
    CONSTRAINT open_macro_v04_decision_input_captures_month_end CHECK (
        as_of = (date_trunc('month', as_of::timestamp)
                 + interval '1 month' - interval '1 day')::date
    ),
    CONSTRAINT open_macro_v04_decision_input_captures_horizon CHECK (
        min_obs_date <= max_obs_date AND max_obs_date <= as_of
    ),
    CONSTRAINT open_macro_v04_decision_input_captures_decision_fk FOREIGN KEY (as_of)
        REFERENCES open_macro_v04_decisions (as_of) ON DELETE RESTRICT
);

CREATE OR REPLACE FUNCTION open_macro_v04_decision_input_captures_reject_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'open_macro_v04_decision_input_captures is append-only';
END;
$$;

DROP TRIGGER IF EXISTS open_macro_v04_decision_input_captures_reject_mutation
    ON open_macro_v04_decision_input_captures;
CREATE TRIGGER open_macro_v04_decision_input_captures_reject_mutation
BEFORE UPDATE OR DELETE ON open_macro_v04_decision_input_captures
FOR EACH ROW EXECUTE FUNCTION open_macro_v04_decision_input_captures_reject_mutation();

REVOKE ALL ON TABLE open_macro_v04_decision_input_captures FROM PUBLIC;
REVOKE ALL ON FUNCTION open_macro_v04_decision_input_captures_reject_mutation() FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker_writer') THEN
        ALTER TABLE open_macro_v04_decision_input_captures OWNER TO worker_writer;
        ALTER FUNCTION open_macro_v04_decision_input_captures_reject_mutation()
            OWNER TO worker_writer;
        GRANT SELECT, INSERT ON TABLE open_macro_v04_decision_input_captures
            TO worker_writer;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_runtime') THEN
        REVOKE ALL ON TABLE open_macro_v04_decision_input_captures FROM app_runtime;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_analytics_ro') THEN
        REVOKE ALL ON TABLE open_macro_v04_decision_input_captures FROM app_analytics_ro;
    END IF;
END $$;
