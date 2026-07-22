-- Daily publication chain bookkeeping (Increment 2, Task 6; frozen spec §5).
--
-- The chain ORCHESTRATES the existing, review-approved publication workers; these
-- tables are the chain's own run/stage ledger. They record, per eligible
-- source-day, one deterministic run identity, a per-stage checkpoint (so a
-- restart is idempotent and resumes mid-chain), the input watermarks observed,
-- and exactly ONE run summary. Promotion history is kept so a current pointer can
-- be rolled back to its prior target.
--
-- No production source is read or written here: this is orchestration state only.

-- One row per (chain, source_day, code_revision, config_version). run_id is the
-- deterministic uuid5 of that tuple (chain_run_id_for), so a replay resolves to
-- the same run and never forks a second row.
CREATE TABLE IF NOT EXISTS bond_daily_chain_runs (
    run_id            uuid PRIMARY KEY,
    chain             text NOT NULL CHECK (chain <> ''),
    source_day        date NOT NULL,
    code_revision     text NOT NULL,
    config_version    text NOT NULL,
    status            text NOT NULL DEFAULT 'running'
                          CHECK (status IN ('running', 'completed', 'failed')),
    input_watermarks  jsonb NOT NULL DEFAULT '{}'::jsonb,
    summary           jsonb,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    UNIQUE (chain, source_day, code_revision, config_version)
);

-- One checkpoint row per stage of a run. A stage is executed at most once to a
-- terminal per-stage status; on restart a row already at 'succeeded' or 'skipped'
-- is honoured and NOT re-executed. 'failed' aborts the run before promotion.
--   reason         : for 'skipped' one of the allowed skip reasons
--                    (dark_no_source | input_unchanged); for 'failed' the cause.
--   classification : for 'failed' whether the failure was 'transient' (retried
--                    then exhausted) or 'terminal' (fail-closed immediately).
CREATE TABLE IF NOT EXISTS bond_daily_chain_stage_runs (
    run_id          uuid NOT NULL REFERENCES bond_daily_chain_runs(run_id) ON DELETE CASCADE,
    stage           text NOT NULL CHECK (stage <> ''),
    stage_order     integer NOT NULL CHECK (stage_order >= 0),
    status          text NOT NULL CHECK (status IN ('succeeded', 'skipped', 'failed')),
    reason          text,
    classification  text CHECK (classification IS NULL OR classification IN ('transient', 'terminal')),
    attempts        integer NOT NULL DEFAULT 1 CHECK (attempts >= 1),
    detail          jsonb NOT NULL DEFAULT '{}'::jsonb,
    watermarks      jsonb,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    -- A skipped stage MUST carry a reason (a required stage skipped without an
    -- allowed reason is recorded as failed, never skipped); a failed stage MUST
    -- carry a classification.
    CHECK (status <> 'skipped' OR reason IS NOT NULL),
    CHECK (status <> 'failed' OR classification IS NOT NULL),
    PRIMARY KEY (run_id, stage)
);

-- Pointer-promotion history so a rollback can restore the prior current pointer.
-- Every time the chain promotes a product it records the publication it made
-- current and the publication that was current immediately before, giving
-- rollback_pointer() a deterministic prior target to restore.
CREATE TABLE IF NOT EXISTS bond_daily_chain_promotions (
    promotion_id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                   uuid REFERENCES bond_daily_chain_runs(run_id) ON DELETE SET NULL,
    product                  text NOT NULL CHECK (product <> ''),
    action                   text NOT NULL DEFAULT 'promote' CHECK (action IN ('promote', 'rollback')),
    publication_id           uuid,
    previous_publication_id  uuid,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS bond_daily_chain_promotions_product_idx
    ON bond_daily_chain_promotions (product, promotion_id DESC);
