-- Publication-versioned N-PORT holdings sidecar.  This does not alter the
-- legacy sec_nport_holdings relation, whose key can collapse DERA source lots.

CREATE TABLE IF NOT EXISTS sec_nport_instrument_class_bridge (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    accession_number text NOT NULL CHECK (accession_number <> ''),
    holding_id text NOT NULL CHECK (holding_id <> ''),
    -- Security identity is optional.  A resolved holding can be attributed to a
    -- series without a resolved security identity or share class.
    instrument_id text CHECK (instrument_id <> ''),
    series_id text,
    class_id text,
    valid_from date NOT NULL,
    valid_to date,
    resolution_state text NOT NULL CHECK (resolution_state IN ('resolved', 'ambiguous', 'orphan')),
    source_candidate_key_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (publication_id, accession_number, holding_id),
    CHECK (valid_to IS NULL OR valid_to >= valid_from),
    CONSTRAINT sec_nport_instrument_class_bridge_resolved_series_check
        CHECK (resolution_state <> 'resolved' OR NULLIF(series_id, '') IS NOT NULL)
);

ALTER TABLE sec_nport_instrument_class_bridge
    ADD COLUMN IF NOT EXISTS source_candidate_key_evidence jsonb NOT NULL DEFAULT '{}'::jsonb;

-- Upgrade the already-created, empty V2 schema from the original DDL.  The
-- production caller guards this DDL with an explicit empty-table check; this
-- file intentionally does not attempt a populated-table migration.
ALTER TABLE sec_nport_instrument_class_bridge
    ALTER COLUMN instrument_id DROP NOT NULL;

ALTER TABLE sec_nport_instrument_class_bridge
    DROP CONSTRAINT IF EXISTS sec_nport_instrument_class_bridge_check1;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'sec_nport_instrument_class_bridge'::regclass
          AND conname = 'sec_nport_instrument_class_bridge_resolved_series_check'
    ) THEN
        ALTER TABLE sec_nport_instrument_class_bridge
            ADD CONSTRAINT sec_nport_instrument_class_bridge_resolved_series_check
            CHECK (resolution_state <> 'resolved' OR NULLIF(series_id, '') IS NOT NULL);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS sec_nport_instrument_class_bridge_instrument_validity_idx
    ON sec_nport_instrument_class_bridge (instrument_id, valid_from, valid_to);

CREATE TABLE IF NOT EXISTS sec_nport_holdings_v2 (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    accession_number text NOT NULL CHECK (accession_number <> ''),
    holding_id text NOT NULL CHECK (holding_id <> ''),
    source_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    report_date date NOT NULL,
    filing_date date NOT NULL,
    source_series_id text,
    issuer_name text,
    issuer_category text,
    cusip text,
    isin text,
    issuer_lei text,
    signed_market_value numeric,
    signed_pct_of_nav numeric,
    payoff_profile text,
    source_typed_projection jsonb NOT NULL,
    PRIMARY KEY (publication_id, accession_number, holding_id),
    FOREIGN KEY (publication_id, accession_number, holding_id)
        REFERENCES sec_nport_instrument_class_bridge(publication_id, accession_number, holding_id)
        ON DELETE RESTRICT
);

ALTER TABLE sec_nport_holdings_v2
    ADD COLUMN IF NOT EXISTS issuer_category text;

CREATE INDEX IF NOT EXISTS sec_nport_holdings_v2_publication_report_idx
    ON sec_nport_holdings_v2 (publication_id, report_date DESC, source_series_id);

-- Rows are append-only while the parent publication is prepared. Validation
-- freezes the complete publication atomically.
CREATE OR REPLACE FUNCTION sec_nport_holdings_v2_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_product text;
        parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION '% is immutable after insert', TG_TABLE_NAME;
    END IF;

    SELECT product, lifecycle_state INTO parent_product, parent_state
    FROM sec_derived_publications
    WHERE publication_id=NEW.publication_id
    FOR UPDATE;

    IF parent_product IS NULL THEN
        RAISE EXCEPTION 'N-PORT v2 row requires an existing parent publication';
    END IF;
    IF parent_product <> 'sec_nport_holdings_v2' OR parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-PORT v2 rows may only be inserted into a prepared sec_nport_holdings_v2 publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS sec_nport_instrument_class_bridge_write_guard
    ON sec_nport_instrument_class_bridge;
CREATE TRIGGER sec_nport_instrument_class_bridge_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON sec_nport_instrument_class_bridge
FOR EACH ROW EXECUTE FUNCTION sec_nport_holdings_v2_write_guard();

DROP TRIGGER IF EXISTS sec_nport_holdings_v2_write_guard
    ON sec_nport_holdings_v2;
CREATE TRIGGER sec_nport_holdings_v2_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON sec_nport_holdings_v2
FOR EACH ROW EXECUTE FUNCTION sec_nport_holdings_v2_write_guard();

-- Unresolved bridge rows remain inspectable here, but are deliberately absent
-- from the current publishable view below.
CREATE OR REPLACE VIEW sec_nport_holdings_v2_bridge_status AS
SELECT h.publication_id, h.accession_number, h.holding_id, h.source_run_id,
       h.report_date, h.filing_date, h.source_series_id, h.issuer_name,
       h.issuer_category, h.cusip, h.isin, h.issuer_lei,
       h.signed_market_value, h.signed_pct_of_nav, h.payoff_profile,
       b.instrument_id, b.series_id, b.class_id, b.valid_from, b.valid_to,
       b.resolution_state, b.source_candidate_key_evidence
FROM sec_nport_holdings_v2 h
JOIN sec_nport_instrument_class_bridge b
  ON (b.publication_id,b.accession_number,b.holding_id)
   =(h.publication_id,h.accession_number,h.holding_id);

CREATE OR REPLACE VIEW sec_nport_holdings_v2_current AS
SELECT h.publication_id, h.accession_number, h.holding_id, h.source_run_id,
       h.report_date, h.filing_date, b.series_id, b.class_id, b.instrument_id,
       h.source_series_id, h.issuer_name, h.issuer_category, h.cusip, h.isin,
       h.issuer_lei, h.signed_market_value, h.signed_pct_of_nav, h.payoff_profile,
       h.source_typed_projection
FROM sec_current_derived_publications p
JOIN sec_nport_holdings_v2 h ON h.publication_id=p.publication_id
JOIN sec_nport_instrument_class_bridge b
  ON (b.publication_id,b.accession_number,b.holding_id)
   =(h.publication_id,h.accession_number,h.holding_id)
WHERE p.product='sec_nport_holdings_v2'
  AND b.resolution_state='resolved'
  AND h.report_date >= b.valid_from
  AND (b.valid_to IS NULL OR h.report_date <= b.valid_to);
