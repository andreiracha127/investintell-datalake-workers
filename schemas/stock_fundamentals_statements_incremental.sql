-- Incremental, worker-owned companion for the app's
-- stock_fundamentals_statements_mv.  This is deliberately additive: it never
-- drops, replaces, or redirects the existing materialized view.  A separate,
-- reviewed consumer cutover may choose this compatible relation later.
--
-- LIKE takes the exact public column types from the existing MV at first
-- installation.  That prevents a second hand-maintained type declaration from
-- drifting from the app contract while still leaving the MV untouched.
CREATE TABLE IF NOT EXISTS stock_fundamentals_statements_incremental
    (LIKE stock_fundamentals_statements_mv INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

CREATE UNIQUE INDEX IF NOT EXISTS stock_fundamentals_statements_incremental_pk
    ON stock_fundamentals_statements_incremental (ticker, freq, period_end);
CREATE INDEX IF NOT EXISTS stock_fundamentals_statements_incremental_ticker_idx
    ON stock_fundamentals_statements_incremental (ticker);

COMMENT ON TABLE stock_fundamentals_statements_incremental IS
    'Worker-owned incremental companion to stock_fundamentals_statements_mv. '
    'The existing MV remains the consumer contract until an explicit cutover.';

-- One row per canonical input fact identity.  The fingerprint includes its
-- value and filing attributes, so an in-place source correction is detected
-- even when its filing/accession identity is unchanged.
CREATE TABLE IF NOT EXISTS stock_fundamentals_statement_fact_watermarks (
    fact_identity text PRIMARY KEY,
    fact_fingerprint text NOT NULL,
    cik bigint NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS stock_fundamentals_statement_fact_watermarks_cik_idx
    ON stock_fundamentals_statement_fact_watermarks (cik);

-- Invalid source dates are retained as evidence but are never allowed into the
-- materializer or its watermark.  This specifically blocks historical parser
-- failures such as a period date in year 6016 from creating absurd chunks.
CREATE TABLE IF NOT EXISTS stock_fundamentals_statement_fact_quarantine (
    fact_identity text PRIMARY KEY,
    cik bigint NOT NULL,
    accession text,
    period_start date,
    period_end date,
    reason_code text NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now()
);

-- Successful/no-op runs are a compact operational ledger.  State/data writes
-- and this row commit together; a failed scoped rebuild rolls all of them back
-- rather than advancing a watermark past unmaterialized source facts.
CREATE TABLE IF NOT EXISTS stock_fundamentals_statement_runs (
    run_id uuid PRIMARY KEY,
    mode text NOT NULL CHECK (mode IN ('incremental', 'rebuild')),
    changed_facts integer NOT NULL CHECK (changed_facts >= 0),
    affected_ciks integer NOT NULL CHECK (affected_ciks >= 0),
    rows_deleted integer NOT NULL CHECK (rows_deleted >= 0),
    rows_upserted integer NOT NULL CHECK (rows_upserted >= 0),
    quarantined_facts integer NOT NULL CHECK (quarantined_facts >= 0),
    completed_at timestamptz NOT NULL DEFAULT now()
);
