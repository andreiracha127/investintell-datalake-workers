-- open_macro_v03 staleness-block LEDGER (Stage B direct activation).
-- Durable, IMMUTABLE ledger: one row per business day on which the worker REFUSED
-- to publish a decision/allocation because the inputs breached the staleness SLO.
-- A block is a POSITIVE, replayable record (input hashes + reason) so the Stage C
-- verifier and the missing_output_slo can tell an INTENTIONAL block apart from a
-- silent worker exit. No lifecycle columns: a ledger entry is never invalidated.
CREATE TABLE IF NOT EXISTS open_macro_v03_staleness_blocks (
    as_of                DATE        PRIMARY KEY,
    reason               TEXT        NOT NULL,
    stale_detail         JSONB       NOT NULL,
    input_vintage_sha256 CHAR(64)    NOT NULL,
    input_prices_sha256  CHAR(64)    NOT NULL,
    pack_v2_sha256       CHAR(64)    NOT NULL,
    module_pins_sha256   CHAR(64)    NOT NULL,
    code_commit          CHAR(40)    NOT NULL,
    run_id               TEXT        NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
