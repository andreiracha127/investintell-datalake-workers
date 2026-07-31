-- open_macro_v03 staleness-block RESOLUTION ledger (append-only).
--
-- WHY: schemas/open_macro_v03_staleness_blocks.sql is an IMMUTABLE ledger — a block
-- is never edited or deleted. publish() refuses a day that carries a block and says
-- the day "requires explicit operator resolution". Until this table existed there was
-- NO sanctioned path to emit that resolution: the 2026-07-17 incident was cleared by
-- an operator with an ad-hoc SQL session over SSH. This ledger IS that path, and it
-- keeps the honesty scaffold intact: nothing is mutated, the block stays verbatim, and
-- every clearance is a POSITIVE, replayable record carrying its proof of re-freshness.
--
-- Two append-only event kinds, one row each, never updated:
--   'resolved'    the operator path (`python -m src.workers.open_macro_v03
--                 resolve-staleness --as-of ... --resolved-by ... --reason ...`).
--                 The worker RE-READS the inputs for that as_of and recomputes the
--                 staleness report under the same advisory lock; the event is written
--                 ONLY when the report is breach-free, and ``freshness_proof`` carries
--                 the per-source timestamps/ages against the thresholds that made it
--                 pass. An operator cannot assert freshness the data does not show.
--   'superseded'  emitted by the writer itself when a fresh run actually publishes the
--                 day (one per as_of, the first publication). It closes the loop: the
--                 block was real, it was resolved, and the resolution produced output.
--
-- Both events pin the hashes of the ORIGINAL block (block_*) and of the inputs read at
-- event time, so a reader can prove the inputs actually moved between block and
-- clearance.
CREATE TABLE IF NOT EXISTS open_macro_v03_staleness_resolutions (
    resolution_id              UUID        PRIMARY KEY,
    as_of                      DATE        NOT NULL
        REFERENCES open_macro_v03_staleness_blocks(as_of),
    resolution_state           TEXT        NOT NULL,
    resolved_by                TEXT        NOT NULL,
    reason                     TEXT        NOT NULL,
    freshness_proof            JSONB       NOT NULL,
    block_run_id               TEXT        NOT NULL,
    block_input_vintage_sha256 CHAR(64)    NOT NULL,
    block_input_prices_sha256  CHAR(64)    NOT NULL,
    input_vintage_sha256       CHAR(64)    NOT NULL,
    input_prices_sha256        CHAR(64)    NOT NULL,
    pack_v2_sha256             CHAR(64)    NOT NULL,
    module_pins_sha256         CHAR(64)    NOT NULL,
    code_commit                CHAR(40)    NOT NULL,
    run_id                     TEXT        NOT NULL,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT open_macro_v03_staleness_resolutions_state_check
        CHECK (resolution_state IN ('resolved', 'superseded')),
    CONSTRAINT open_macro_v03_staleness_resolutions_proof_object
        CHECK (jsonb_typeof(freshness_proof) = 'object')
);

-- Lookup pattern of both readers (publish() and the resolve CLI): newest event for a day.
CREATE INDEX IF NOT EXISTS open_macro_v03_staleness_resolutions_as_of_idx
    ON open_macro_v03_staleness_resolutions (as_of, created_at DESC);
