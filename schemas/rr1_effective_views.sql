-- Effective RR1 facts retain the report context; tie resolution is intentionally fail-closed.
CREATE OR REPLACE VIEW rr1_effective_fact_candidates AS
SELECT f.raw_row_id, f.ingestion_run_id, f.source_table, f.adsh AS accession_number,
       f.tag, f.version, (f.typed_projection->>'ddate')::date AS data_date,
       f.series AS series_id, f.class AS class_id, f.measure AS measure_id,
       f.document AS document_id, f.otherdims AS dimensions, f.iprx AS occurrence,
       f.typed_projection AS fact_typed_projection,
       (s.typed_projection->>'effdate')::date AS effective_date,
       (s.typed_projection->>'accepted')::timestamp AS accepted_at,
       (s.typed_projection->>'filed')::date AS filed_date,
       s.typed_projection->>'form' AS form
FROM rr1_raw_v2_rows f
JOIN sec_validated_raw_runs fv ON fv.run_id=f.ingestion_run_id
JOIN rr1_raw_v2_rows s ON s.ingestion_run_id=f.ingestion_run_id AND s.source_table='sub.tsv'
                         AND s.adsh=f.adsh AND s.parse_status='typed'
JOIN sec_validated_raw_runs sv ON sv.run_id=s.ingestion_run_id
WHERE f.source_table IN ('num.tsv','txt.tsv') AND f.parse_status='typed'
  AND f.typed_projection->>'ddate' IS NOT NULL
  AND s.typed_projection->>'effdate' IS NOT NULL
  AND s.typed_projection->>'accepted' IS NOT NULL
  AND s.typed_projection->>'filed' IS NOT NULL
  AND COALESCE(s.typed_projection->>'form','') NOT IN ('NT N-1A','NT N-1A/A');

CREATE OR REPLACE VIEW rr1_effective_fact_selection AS
WITH ranked AS (
    SELECT c.*, dense_rank() OVER (
        PARTITION BY source_table,tag,version,data_date,series_id,class_id,measure_id,document_id,dimensions,occurrence
        ORDER BY effective_date DESC, accepted_at DESC, filed_date DESC
    ) AS precedence_rank
    FROM rr1_effective_fact_candidates c
), assessed AS (
    SELECT ranked.*, count(*) FILTER (WHERE precedence_rank=1) OVER (
        PARTITION BY source_table,tag,version,data_date,series_id,class_id,measure_id,document_id,dimensions,occurrence
    ) AS winning_candidate_count
    FROM ranked
)
SELECT *, CASE WHEN precedence_rank=1 AND winning_candidate_count=1 THEN 'publishable' ELSE 'ambiguous' END AS selection_state
FROM assessed;

CREATE OR REPLACE VIEW rr1_effective_facts AS
SELECT raw_row_id, ingestion_run_id, source_table, accession_number, tag, version, data_date,
       series_id, class_id, measure_id, document_id, dimensions, occurrence, fact_typed_projection,
       effective_date, accepted_at, filed_date, form
FROM rr1_effective_fact_selection
WHERE selection_state='publishable';
