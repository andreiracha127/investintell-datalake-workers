-- open_macro_v03 daily consumable ALLOCATION output (Stage B direct activation).
-- Flat table (NOT a hypertable): exactly one row per as_of business day (PK), FK to
-- the decision row so an allocation never exists without its regime decision. The
-- allocation IS the product; the regime decision is its input.
CREATE TABLE IF NOT EXISTS open_macro_v03_allocations (
    as_of                   DATE        PRIMARY KEY
        REFERENCES open_macro_v03_decisions (as_of),
    book                    TEXT        NOT NULL DEFAULT 'compressed_50'
        CHECK (book = 'compressed_50'),
    w_spy                   NUMERIC     NOT NULL CHECK (w_spy >= 0 AND w_spy <= 1),
    w_tlt                   NUMERIC     NOT NULL CHECK (w_tlt >= 0 AND w_tlt <= 1),
    w_tip                   NUMERIC     NOT NULL CHECK (w_tip >= 0 AND w_tip <= 1),
    w_gld                   NUMERIC     NOT NULL CHECK (w_gld >= 0 AND w_gld <= 1),
    w_dbc                   NUMERIC     NOT NULL CHECK (w_dbc >= 0 AND w_dbc <= 1),
    w_shy                   NUMERIC     NOT NULL CHECK (w_shy >= 0 AND w_shy <= 1),
    risk_assets_weight      NUMERIC     NOT NULL,
    defensive_assets_weight NUMERIC     NOT NULL,
    risk_cap                NUMERIC     NOT NULL DEFAULT 0.65,
    defensive_floor         NUMERIC     NOT NULL DEFAULT 0.20,
    priced_at               DATE        NOT NULL,
    input_prices_sha256     CHAR(64)    NOT NULL,
    pack_v2_sha256          CHAR(64)    NOT NULL,
    module_pins_sha256      CHAR(64)    NOT NULL,
    code_commit             CHAR(40)    NOT NULL,
    run_id                  TEXT        NOT NULL,
    publish_state           TEXT        NOT NULL DEFAULT 'published'
        CHECK (publish_state IN ('publishing', 'published')),
    valid_status            TEXT        NOT NULL DEFAULT 'valid'
        CHECK (valid_status IN ('valid', 'invalidated')),
    -- next business day 14:00 UTC. Mon-Fri calendar without market holidays is a
    -- DELIBERATE decision (see open_macro_v03_decisions.sql / worker docstring).
    valid_until             TIMESTAMPTZ NOT NULL,
    invalidated_at          TIMESTAMPTZ,
    invalidated_reason      TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- invalidation timestamp is present iff the row is invalidated
    CONSTRAINT open_macro_v03_allocations_invalidation_consistent
        CHECK ((valid_status = 'invalidated') = (invalidated_at IS NOT NULL)),
    -- the six sleeve weights sum to 1
    CONSTRAINT open_macro_v03_allocations_weights_sum
        CHECK (abs(w_spy + w_tlt + w_tip + w_gld + w_dbc + w_shy - 1) < 1e-9),
    -- risk assets (SPY+DBC) stay at or under the risk cap
    CONSTRAINT open_macro_v03_allocations_risk_cap
        CHECK (risk_assets_weight <= risk_cap + 1e-9),
    -- defensive assets (TLT+SHY+TIP) stay at or above the defensive floor
    CONSTRAINT open_macro_v03_allocations_defensive_floor
        CHECK (defensive_assets_weight >= defensive_floor - 1e-9)
);

-- Reader index: latest valid rows by freshness horizon.
CREATE INDEX IF NOT EXISTS idx_open_macro_v03_allocations_valid
    ON open_macro_v03_allocations (valid_status, valid_until DESC);
