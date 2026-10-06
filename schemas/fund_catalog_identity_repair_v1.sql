-- Append-only receipt ledger of scripts/repair_fund_catalog_identity_v1.py.
--
-- One ``runs`` row per committed apply or rollback, and one ``receipts`` row
-- per catalog row that run changed, carrying the exact before/after values of
-- every column it wrote (``updated_at`` and ``identity_sources`` included).
-- The receipts are the rollback record: ``--rollback <run_id>`` restores the
-- before-values byte for byte, guarded by a compare-and-swap on the after
-- values, and appends its own run + receipts. Nothing here is ever updated or
-- deleted (row triggers) or truncated (statement trigger).
--
-- Idempotent: applied inside the repair transaction on every apply/rollback.

CREATE TABLE IF NOT EXISTS fund_catalog_identity_repair_runs (
    run_id            uuid PRIMARY KEY,
    kind              text NOT NULL CHECK (kind IN ('apply', 'rollback')),
    repair_version    text NOT NULL,
    plan_sha256       text NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    rolls_back_run_id uuid REFERENCES fund_catalog_identity_repair_runs (run_id),
    decision_at       timestamptz NOT NULL,
    committed_by      text NOT NULL DEFAULT current_user,
    created_at        timestamptz NOT NULL DEFAULT now(),
    counts            jsonb NOT NULL CHECK (jsonb_typeof(counts) = 'object'),
    CHECK ((kind = 'rollback') = (rolls_back_run_id IS NOT NULL))
);

-- At most one rollback per apply run.
CREATE UNIQUE INDEX IF NOT EXISTS fund_catalog_identity_repair_runs_rollback_key
    ON fund_catalog_identity_repair_runs (rolls_back_run_id)
    WHERE rolls_back_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS fund_catalog_identity_repair_receipts (
    run_id        uuid NOT NULL REFERENCES fund_catalog_identity_repair_runs (run_id),
    relation      text NOT NULL
                  CHECK (relation IN ('instruments_universe', 'instrument_identity')),
    instrument_id uuid NOT NULL,
    rule          text NOT NULL,
    before_values jsonb NOT NULL CHECK (jsonb_typeof(before_values) = 'object'),
    after_values  jsonb NOT NULL CHECK (jsonb_typeof(after_values) = 'object'),
    PRIMARY KEY (run_id, relation, instrument_id)
);

CREATE INDEX IF NOT EXISTS fund_catalog_identity_repair_receipts_instrument_idx
    ON fund_catalog_identity_repair_receipts (instrument_id);

CREATE OR REPLACE FUNCTION fund_catalog_identity_repair_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

CREATE OR REPLACE TRIGGER fund_catalog_identity_repair_runs_append_only
BEFORE UPDATE OR DELETE ON fund_catalog_identity_repair_runs
FOR EACH ROW EXECUTE FUNCTION fund_catalog_identity_repair_append_only();

CREATE OR REPLACE TRIGGER fund_catalog_identity_repair_runs_no_truncate
BEFORE TRUNCATE ON fund_catalog_identity_repair_runs
FOR EACH STATEMENT EXECUTE FUNCTION fund_catalog_identity_repair_append_only();

CREATE OR REPLACE TRIGGER fund_catalog_identity_repair_receipts_append_only
BEFORE UPDATE OR DELETE ON fund_catalog_identity_repair_receipts
FOR EACH ROW EXECUTE FUNCTION fund_catalog_identity_repair_append_only();

CREATE OR REPLACE TRIGGER fund_catalog_identity_repair_receipts_no_truncate
BEFORE TRUNCATE ON fund_catalog_identity_repair_receipts
FOR EACH STATEMENT EXECUTE FUNCTION fund_catalog_identity_repair_append_only();
