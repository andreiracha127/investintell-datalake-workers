-- open_macro_v03 daily consumable REGIME DECISION output (Stage B direct activation).
-- Flat table (NOT a hypertable): exactly one row per as_of business day (PK).
-- The sanctioned backend read path filters valid_status = 'valid' AND honours
-- valid_until freshness; a kill-switch abort stamps valid_status = 'invalidated'.
CREATE TABLE IF NOT EXISTS open_macro_v03_decisions (
    as_of                DATE          PRIMARY KEY,
    quadrant             TEXT          NOT NULL
        CHECK (quadrant IN ('recovery', 'expansion', 'slowdown', 'contraction')),
    decision_validity    TEXT          NOT NULL
        CHECK (decision_validity IN ('fresh', 'carried')),
    carry_seed_as_of     DATE          NOT NULL CHECK (carry_seed_as_of <= as_of),
    candidate_confidence NUMERIC
        CHECK (candidate_confidence IS NULL
               OR (candidate_confidence >= 0 AND candidate_confidence <= 1)),
    coverage_quality     NUMERIC       NOT NULL,
    growth_score         NUMERIC,
    inflation_score      NUMERIC,
    input_vintage_sha256 CHAR(64)      NOT NULL,
    input_prices_sha256  CHAR(64)      NOT NULL,
    pack_v2_sha256       CHAR(64)      NOT NULL,
    module_pins_sha256   CHAR(64)      NOT NULL,
    judgment_ref         TEXT          NOT NULL,
    threshold_ref        TEXT          NOT NULL,
    code_commit          CHAR(40)      NOT NULL,
    run_id               TEXT          NOT NULL,
    publish_state        TEXT          NOT NULL DEFAULT 'published'
        CHECK (publish_state IN ('publishing', 'published')),
    valid_status         TEXT          NOT NULL DEFAULT 'valid'
        CHECK (valid_status IN ('valid', 'invalidated')),
    -- next business day 14:00 UTC. Business days are DELIBERATELY Mon-Fri without
    -- a market-holiday calendar: on holidays the worker publishes carried and
    -- renews this horizon (no reader blackout).
    valid_until          TIMESTAMPTZ   NOT NULL,
    invalidated_at       TIMESTAMPTZ,
    invalidated_reason   TEXT,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    -- invalidation timestamp is present iff the row is invalidated
    CONSTRAINT open_macro_v03_decisions_invalidation_consistent
        CHECK ((valid_status = 'invalidated') = (invalidated_at IS NOT NULL)),
    -- fresh iff the seed is today; carried iff the seed predates today
    CONSTRAINT open_macro_v03_decisions_validity_seed
        CHECK ((decision_validity = 'fresh'   AND carry_seed_as_of = as_of)
            OR (decision_validity = 'carried' AND carry_seed_as_of < as_of))
);

-- Reader index: latest valid rows by freshness horizon.
CREATE INDEX IF NOT EXISTS idx_open_macro_v03_decisions_valid
    ON open_macro_v03_decisions (valid_status, valid_until DESC);
