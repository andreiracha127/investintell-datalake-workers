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
    WHERE r.source_table='SUBMISSION.tsv'
      AND (
          r.parse_status='typed'
          OR (
              -- Historical N-CEN contracts accidentally classified this Y/N
              -- indicator as a date. Keep the exception fail-closed: admit only
              -- the single known parser issue and preserve every other
              -- quarantined row as ineligible.
              r.parse_status='quarantined'
              AND jsonb_array_length(to_jsonb(r)->'parse_errors')=1
              AND to_jsonb(r)->'parse_errors'
                    @> '[{"code":"invalid_date","column_name":"IS_REPORT_PERIOD_LT_12MONTH"}]'::jsonb
              AND to_jsonb(r)->'original_lexical_row'->>'IS_REPORT_PERIOD_LT_12MONTH' IN ('Y','N')
          )
      )
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

-- ---------------------------------------------------------------------------
-- FUND_REPORTED_INFO admissibility (the same parser defect, two more columns)
-- ---------------------------------------------------------------------------
-- IS_REPORT_PERIOD_LT_12MONTH above is not the only Y/N indicator the frozen
-- N-CEN contract typed as a date.  FUND_REPORTED_INFO.tsv carries two siblings:
-- IS_COLLATERAL_LIQUIDATED (contracts/sec-regulatory/v1/source-tables/ncen.json
-- :3305) and IS_TARGET_DATE.  Measured on the datalake 2026-08-07 they are the
-- ONLY two columns that quarantine a FUND_REPORTED_INFO row -- 26.038 of 59.756
-- rows (43,6%): IS_COLLATERAL_LIQUIDATED 23.494, IS_TARGET_DATE 3.014, both
-- with code `invalid_date`, over lexical values 'N' 22.999, '' 2.544, 'Y' 25.
--
-- Effect on the served surface: the N-CEN provider-network profiles covered
-- 3.660 of the 6.872 series of the fund_peer_groups_v1 2026-06-30 anchor
-- (53,3%).  Admitting these quarantined rows -- and nothing else -- takes that
-- to 6.872/6.872 (100,0%).
--
-- The carve-out keeps the ratified fail-closed shape: a quarantined row is
-- admissible only when EVERY one of its parse errors is one of the two known
-- defects AND both lexical values are inside the authorized code domain.  The
-- single-error `jsonb_array_length(...)=1 AND @> [...]` form above does not
-- generalise to two columns (a row can legitimately trip both), so the
-- equivalent is written as "no error outside the allowed set".  Any other
-- column, any other code, or any value outside the domain keeps the row
-- quarantined, exactly as before.  The domain is ('Y','N','') rather than the
-- ('Y','N') used for SUBMISSION because blank is a real reported state here and
-- ncen_derived_common's ncen_tristate_flag() already maps it to `not_reported`
-- -- it is never coerced to the negative.
--
-- No view is created for this predicate.  Its only consumer is
-- build_ncen_provider_network_profiles(), whose DDL left this repo with the
-- fourteen dossier profile products in #65 and lives on in the datalake; the
-- installed function body carries the block below verbatim, so the contract
-- stays a mirror.  Two sites inside that function read it: the _ncen_pn_sf
-- selected-fund temp table, and the fund-identity guard inlined there (the
-- shared ncen_assert_effective_fund_identity() reads the typed-only surface and
-- is left untouched for the other, orphaned, N-CEN builders).
--
--   f.source_table = 'FUND_REPORTED_INFO.tsv'
--   AND (
--       f.parse_status = 'typed'
--       OR (
--           f.parse_status = 'quarantined'
--           AND NOT EXISTS (
--               SELECT 1 FROM jsonb_array_elements(f.parse_errors) err
--               WHERE NOT (err->>'code' = 'invalid_date'
--                          AND err->>'column_name' IN ('IS_COLLATERAL_LIQUIDATED','IS_TARGET_DATE'))
--           )
--           AND COALESCE(f.original_lexical_row->>'IS_COLLATERAL_LIQUIDATED','') IN ('Y','N','')
--           AND COALESCE(f.original_lexical_row->>'IS_TARGET_DATE','') IN ('Y','N','')
--       )
--   )
--
-- One consequence correction rides along, because the carve-out cannot ship
-- without it.  ncen_assert_effective_fund_identity() aborts a build when an
-- effective filing exposes zero FUND_REPORTED_INFO rows.  The SUBMISSION
-- carve-out above admits UIT/separate-account registrants (N-4, N-6, S-6) that
-- report on Part E and file no FUND_REPORTED_INFO at all: 2.946 of the 13.581
-- effective filings, contributing 0 series to the anchor and 0 rows to the
-- profiles (the builder's join is an inner one, so they never produced output).
-- A plain re-run therefore raises today, carve-out or not.  The guard inlined
-- in the builder keeps both teeth that are about identity -- no NULL FUND_ID,
-- no duplicate FUND_ID per filing -- over the admissible rows, and scopes them
-- to filings that actually report funds.  A filing that reports funds but has
-- no admissible row still fails closed.
