-- Effective N-PORT submissions are a derived, fail-closed serving surface.
-- Raw DERA rows remain immutable and are never replaced by this selection.

CREATE OR REPLACE VIEW nport_effective_filing_candidates AS
WITH typed_submissions AS (
    SELECT s.raw_row_id, s.ingestion_run_id,
           s.typed_projection->>'ACCESSION_NUMBER' AS accession_number,
           s.typed_projection->>'SUB_TYPE' AS form,
           (s.typed_projection->>'FILING_DATE')::date AS filing_date,
           COALESCE((s.typed_projection->>'REPORT_DATE')::date,
                    (s.typed_projection->>'REPORT_ENDING_PERIOD')::date) AS report_date,
           i.typed_projection->>'SERIES_ID' AS series_id
    FROM nport_raw_rows s
    JOIN sec_validated_raw_runs sv ON sv.run_id=s.ingestion_run_id
    JOIN nport_raw_rows i ON i.ingestion_run_id=s.ingestion_run_id
                         AND i.source_table='FUND_REPORTED_INFO.tsv'
                         AND i.parse_status='typed'
                         AND i.accession_number=s.accession_number
    JOIN sec_validated_raw_runs iv ON iv.run_id=i.ingestion_run_id
    WHERE s.source_table='SUBMISSION.tsv' AND s.parse_status='typed'
), eligible AS (
    SELECT *, CASE WHEN form='NPORT-P/A' THEN 1 ELSE 0 END AS is_amendment
    FROM typed_submissions
    WHERE form IN ('NPORT-P', 'NPORT-P/A')
      AND NULLIF(accession_number, '') IS NOT NULL
      AND NULLIF(series_id, '') IS NOT NULL
      AND filing_date IS NOT NULL AND report_date IS NOT NULL
)
SELECT raw_row_id, ingestion_run_id, accession_number, series_id, form, filing_date,
       report_date, is_amendment
FROM eligible;

CREATE OR REPLACE VIEW nport_effective_filing_selection AS
WITH ranked AS (
    SELECT c.*,
           dense_rank() OVER (
               PARTITION BY series_id, report_date
               ORDER BY is_amendment DESC, filing_date DESC
           ) AS precedence_rank,
           row_number() OVER (
               PARTITION BY series_id, report_date
               ORDER BY is_amendment DESC, filing_date DESC, accession_number DESC, raw_row_id DESC
           ) AS deterministic_order
    FROM nport_effective_filing_candidates c
), assessed AS (
    SELECT ranked.*,
           count(*) FILTER (WHERE precedence_rank=1) OVER (
               PARTITION BY series_id, report_date
           ) AS winning_candidate_count
    FROM ranked
)
SELECT raw_row_id, ingestion_run_id, accession_number, series_id, form, filing_date,
       report_date, is_amendment, precedence_rank, deterministic_order,
       winning_candidate_count,
       CASE WHEN precedence_rank=1 AND winning_candidate_count=1
            THEN 'publishable' ELSE 'ambiguous' END AS selection_state
FROM assessed;

CREATE OR REPLACE VIEW nport_effective_filings AS
SELECT raw_row_id, ingestion_run_id, accession_number, series_id, form, filing_date,
       report_date, is_amendment
FROM nport_effective_filing_selection
WHERE selection_state='publishable';

-- This view intentionally retains source lots: duplicate CUSIPs are not an identity key.
CREATE OR REPLACE VIEW nport_current_holding_candidates AS
SELECT e.accession_number, h.holding_id, e.ingestion_run_id, e.series_id,
       e.report_date, e.filing_date, e.form,
       h.typed_projection->>'ISSUER_NAME' AS issuer_name,
       h.typed_projection->>'ISSUER_CUSIP' AS raw_cusip,
       h.typed_projection->>'ISSUER_LEI' AS raw_issuer_lei,
       h.typed_projection->>'ISSUER_TYPE' AS issuer_category,
       (h.typed_projection->>'CURRENCY_VALUE')::numeric AS signed_market_value,
       (h.typed_projection->>'PERCENTAGE')::numeric AS signed_pct_of_nav,
       h.typed_projection->>'PAYOFF_PROFILE' AS payoff_profile,
       h.typed_projection AS holding_typed_projection
FROM nport_effective_filings e
JOIN nport_raw_rows h ON h.ingestion_run_id=e.ingestion_run_id
                     AND h.source_table='FUND_REPORTED_HOLDING.tsv'
                     AND h.parse_status='typed'
                     AND h.accession_number=e.accession_number
JOIN sec_validated_raw_runs hv ON hv.run_id=h.ingestion_run_id
WHERE NULLIF(h.holding_id, '') IS NOT NULL;

CREATE OR REPLACE VIEW nport_current_holdings AS
SELECT accession_number, holding_id, ingestion_run_id, series_id, report_date, filing_date,
       form, issuer_name, raw_cusip, raw_issuer_lei, issuer_category,
       signed_market_value, signed_pct_of_nav, payoff_profile, holding_typed_projection
FROM nport_current_holding_candidates;
