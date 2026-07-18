-- Publication-versioned N-PORT holdings sidecar.  This does not alter the
-- legacy sec_nport_holdings relation, whose key can collapse DERA source lots.

CREATE TABLE IF NOT EXISTS sec_nport_instrument_class_bridge (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    accession_number text NOT NULL CHECK (accession_number <> ''),
    holding_id text NOT NULL CHECK (holding_id <> ''),
    instrument_id text NOT NULL CHECK (instrument_id <> ''),
    series_id text,
    class_id text,
    valid_from date NOT NULL,
    valid_to date,
    resolution_state text NOT NULL CHECK (resolution_state IN ('resolved', 'ambiguous', 'orphan')),
    source_candidate_key_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (publication_id, accession_number, holding_id),
    CHECK (valid_to IS NULL OR valid_to >= valid_from),
    CHECK (resolution_state <> 'resolved'
           OR (NULLIF(series_id, '') IS NOT NULL AND NULLIF(class_id, '') IS NOT NULL))
);

ALTER TABLE sec_nport_instrument_class_bridge
    ADD COLUMN IF NOT EXISTS source_candidate_key_evidence jsonb NOT NULL DEFAULT '{}'::jsonb;

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
ALTER TABLE sec_nport_holdings_v2
    ADD COLUMN IF NOT EXISTS source_typed_projection jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS sec_nport_holdings_v2_publication_report_idx
    ON sec_nport_holdings_v2 (publication_id, report_date DESC, source_series_id);

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
