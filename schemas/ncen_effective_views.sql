-- Effective N-CEN selection: only typed rows from a validated raw-run surface.
CREATE OR REPLACE VIEW ncen_effective_filing_candidates AS
WITH typed_submissions AS (
    SELECT r.raw_row_id, r.ingestion_run_id, r.typed_projection,
           r.typed_projection->>'ACCESSION_NUMBER' AS accession_number,
           r.typed_projection->>'CIK' AS registrant_cik,
           r.typed_projection->>'SUBMISSION_TYPE' AS form,
           (r.typed_projection->>'FILING_DATE')::date AS filing_date,
           (r.typed_projection->>'REPORT_ENDING_PERIOD')::date AS effective_date
    FROM ncen_raw_v2_rows r
    JOIN sec_validated_raw_runs v ON v.run_id=r.ingestion_run_id
    WHERE r.source_table='SUBMISSION.tsv' AND r.parse_status='typed'
), eligible AS (
    SELECT *, NULL::timestamptz AS accepted_at,
           CASE WHEN form='N-CEN/A' THEN 1 ELSE 0 END AS is_amendment
    FROM typed_submissions
    -- NT N-CEN and NT N-CEN/A notice forms cannot win an effective filing.
    WHERE form IN ('N-CEN','N-CEN/A')
      AND accession_number IS NOT NULL AND registrant_cik IS NOT NULL
      AND filing_date IS NOT NULL AND effective_date IS NOT NULL
), lineage AS (
    SELECT registrant_cik,effective_date,
           count(DISTINCT accession_number) FILTER (WHERE is_amendment=0) AS base_accession_count,
           min(accession_number) FILTER (WHERE is_amendment=0) AS sole_base_candidate
    FROM eligible
    GROUP BY registrant_cik,effective_date
), with_lineage AS (
    SELECT e.*, l.base_accession_count,
           CASE WHEN l.base_accession_count=1 THEN l.sole_base_candidate END AS base_accession_number
    FROM eligible e
    JOIN lineage l USING(registrant_cik,effective_date)
)
SELECT raw_row_id, ingestion_run_id, accession_number, registrant_cik, form, filing_date,
       accepted_at, effective_date, is_amendment,
       CASE WHEN is_amendment=1 THEN base_accession_number END AS amends_accession_number,
       base_accession_count
FROM with_lineage;

CREATE OR REPLACE VIEW ncen_effective_filing_selection AS
WITH ranked AS (
    SELECT c.raw_row_id,c.ingestion_run_id,c.accession_number,c.registrant_cik,c.form,c.filing_date,
           c.accepted_at,c.effective_date,c.is_amendment,c.amends_accession_number,
           dense_rank() OVER (
        PARTITION BY registrant_cik,effective_date
        ORDER BY is_amendment DESC, filing_date DESC, accepted_at DESC NULLS LAST
           ) AS precedence_rank,
           c.base_accession_count
    FROM ncen_effective_filing_candidates c
), assessed AS (
    SELECT ranked.raw_row_id,ranked.ingestion_run_id,ranked.accession_number,ranked.registrant_cik,
           ranked.form,ranked.filing_date,ranked.accepted_at,ranked.effective_date,ranked.is_amendment,
           ranked.amends_accession_number,ranked.precedence_rank,
           count(*) FILTER (WHERE precedence_rank=1) OVER (
               PARTITION BY registrant_cik,effective_date
           ) AS winning_candidate_count,
           ranked.base_accession_count
    FROM ranked
)
SELECT raw_row_id,ingestion_run_id,accession_number,registrant_cik,form,filing_date,
       accepted_at,effective_date,is_amendment,amends_accession_number,precedence_rank,winning_candidate_count,
       CASE WHEN precedence_rank=1 AND winning_candidate_count=1
                 AND (is_amendment=0 OR base_accession_count=1)
            THEN 'publishable' ELSE 'ambiguous' END AS selection_state,
       base_accession_count
FROM assessed;

CREATE OR REPLACE VIEW ncen_effective_filings AS
SELECT raw_row_id, ingestion_run_id, accession_number, registrant_cik, form, filing_date,
       accepted_at, effective_date, is_amendment, amends_accession_number, base_accession_count
FROM ncen_effective_filing_selection
WHERE selection_state='publishable';
