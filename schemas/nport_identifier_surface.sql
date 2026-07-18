-- Identifier normalization is a read-only derived surface.  Empty and known
-- placeholder values never become relationship keys, while their presence stays visible.

CREATE OR REPLACE VIEW nport_identifier_surface AS
WITH identifier_rows AS (
    SELECT m.accession_number, m.holding_id,
           i.typed_projection->>'IDENTIFIER_ISIN' AS raw_isin,
           COALESCE((i.candidate_key_evidence->>'complete')::boolean, false) AS candidate_key_complete
    FROM nport_holding_accession_map m
    JOIN nport_raw_rows i ON i.ingestion_run_id=m.ingestion_run_id
                         AND i.source_table='IDENTIFIERS.tsv'
                         AND i.parse_status='typed'
                         AND i.holding_id=m.holding_id
    JOIN sec_validated_raw_runs v ON v.run_id=i.ingestion_run_id
), normalized_identifier_rows AS (
    SELECT accession_number, holding_id, raw_isin, candidate_key_complete,
           upper(regexp_replace(COALESCE(raw_isin, ''), '[[:space:]]+', '', 'g')) AS normalized_isin
    FROM identifier_rows
), identifier_rollup AS (
    SELECT accession_number, holding_id,
           array_remove(array_agg(DISTINCT normalized_isin) FILTER (
               WHERE candidate_key_complete
                 AND normalized_isin ~ '^[A-Z]{2}[A-Z0-9]{9}[0-9]$'
           ), NULL) AS normalized_isins,
           bool_or(NOT candidate_key_complete) AS incomplete_identifier_candidate_key_evidence,
           bool_or(raw_isin IS NOT NULL AND btrim(raw_isin) <> ''
                   AND normalized_isin IN ('N/A','NA','NONE','NULL','UNKNOWN','-')) AS isin_placeholder
    FROM normalized_identifier_rows
    GROUP BY accession_number, holding_id
), normalized_holdings AS (
    SELECT h.*,
           upper(regexp_replace(COALESCE(h.raw_cusip, ''), '[[:space:]]+', '', 'g')) AS normalized_cusip_candidate,
           upper(regexp_replace(COALESCE(h.raw_issuer_lei, ''), '[[:space:]]+', '', 'g')) AS normalized_lei_candidate
    FROM nport_current_holdings h
)
SELECT h.accession_number, h.holding_id, h.ingestion_run_id, h.series_id,
       h.report_date, h.filing_date,
       CASE WHEN h.normalized_cusip_candidate ~ '^[A-Z0-9]{9}$'
            THEN h.normalized_cusip_candidate END AS cusip,
       CASE WHEN h.normalized_lei_candidate ~ '^[A-Z0-9]{20}$'
            THEN h.normalized_lei_candidate END AS issuer_lei,
       COALESCE(r.normalized_isins, ARRAY[]::text[]) AS isins,
       cardinality(COALESCE(r.normalized_isins, ARRAY[]::text[])) AS isin_count,
       cardinality(COALESCE(r.normalized_isins, ARRAY[]::text[])) > 1 AS isin_conflict,
       h.normalized_cusip_candidate IN ('N/A','NA','NONE','NULL','UNKNOWN','-') AS cusip_placeholder,
       h.normalized_lei_candidate IN ('N/A','NA','NONE','NULL','UNKNOWN','-') AS lei_placeholder,
       COALESCE(r.isin_placeholder, false) AS isin_placeholder,
       COALESCE(r.incomplete_identifier_candidate_key_evidence, false)
           AS incomplete_identifier_candidate_key_evidence,
       h.raw_cusip, h.raw_issuer_lei
FROM normalized_holdings h
LEFT JOIN identifier_rollup r USING(accession_number, holding_id);
