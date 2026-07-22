-- N-CEN Phase 1 V2: lossless physical rows; no V0 table is read or mutated.
CREATE TABLE IF NOT EXISTS ncen_raw_v2_rows (
 raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 ingestion_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
 source_file_id uuid NOT NULL, source_row_number bigint NOT NULL CHECK(source_row_number>=2),
 source_sha256 char(64) NOT NULL CHECK(source_sha256 ~ '^[0-9a-f]{64}$'),
 loaded_at timestamptz NOT NULL DEFAULT now(), parser_version text NOT NULL CHECK(parser_version<>''),
 source_table text NOT NULL CHECK(source_table ~ '^[A-Z0-9_]+[.]tsv$'),
 original_lexical_row jsonb NOT NULL, typed_projection jsonb NOT NULL,
 parse_status text NOT NULL CHECK(parse_status IN ('typed','quarantined','rejected')),
 parse_errors jsonb NOT NULL DEFAULT '[]'::jsonb, candidate_key_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
 accession_number text, fund_id text, director_seqnum text, line_of_credit_seqnum text, valuation_method_change_seqnum text, entity_key text, parent_key text,
 FOREIGN KEY(ingestion_run_id,source_file_id) REFERENCES sec_source_files(run_id,source_file_id) ON DELETE RESTRICT,
 UNIQUE(source_file_id,source_row_number)
);
ALTER TABLE ncen_raw_v2_rows ADD COLUMN IF NOT EXISTS fund_id text;
ALTER TABLE ncen_raw_v2_rows ADD COLUMN IF NOT EXISTS director_seqnum text;
ALTER TABLE ncen_raw_v2_rows ADD COLUMN IF NOT EXISTS line_of_credit_seqnum text;
ALTER TABLE ncen_raw_v2_rows ADD COLUMN IF NOT EXISTS valuation_method_change_seqnum text;
ALTER TABLE ncen_raw_v2_rows ADD COLUMN IF NOT EXISTS entity_key text;
ALTER TABLE ncen_raw_v2_rows ADD COLUMN IF NOT EXISTS parent_key text;
CREATE INDEX IF NOT EXISTS ncen_raw_v2_rows_run_table_idx ON ncen_raw_v2_rows(ingestion_run_id,source_table,source_file_id,source_row_number);
CREATE INDEX IF NOT EXISTS ncen_raw_v2_rows_parent_idx ON ncen_raw_v2_rows(ingestion_run_id,source_table,accession_number,fund_id,director_seqnum,line_of_credit_seqnum,valuation_method_change_seqnum);
CREATE INDEX IF NOT EXISTS ncen_raw_v2_rows_entity_idx ON ncen_raw_v2_rows(ingestion_run_id,source_table,entity_key);

-- A selected metadata SHA has exactly the frozen 53-table catalog.  The rows
-- are installed from the checked-in contract, never inferred from a package.
CREATE TABLE IF NOT EXISTS ncen_contract_tables (
 metadata_sha256 char(64) NOT NULL CHECK(metadata_sha256 ~ '^[0-9a-f]{64}$'),
 source_table text NOT NULL, raw_target text NOT NULL, logical_parents text[] NOT NULL,
 candidate_key text[] NOT NULL, headers text[] NOT NULL DEFAULT ARRAY[]::text[],
 column_contract jsonb NOT NULL DEFAULT '[]'::jsonb, PRIMARY KEY(metadata_sha256,source_table),
 UNIQUE(metadata_sha256,raw_target)
);
ALTER TABLE ncen_contract_tables ADD COLUMN IF NOT EXISTS headers text[] NOT NULL DEFAULT ARRAY[]::text[];
ALTER TABLE ncen_contract_tables ADD COLUMN IF NOT EXISTS column_contract jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE OR REPLACE FUNCTION ncen_contract_catalog_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'N-CEN contract catalog is immutable'; END $$;

CREATE OR REPLACE FUNCTION ncen_validate_raw_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM new_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS (
  SELECT 1 FROM new_rows n JOIN sec_ingestion_runs r ON r.run_id=n.ingestion_run_id
  LEFT JOIN sec_source_files f ON (f.run_id,f.source_file_id)=(n.ingestion_run_id,n.source_file_id)
  LEFT JOIN sec_source_packages p ON p.run_id=n.ingestion_run_id AND p.source_family='ncen'
  LEFT JOIN ncen_contract_tables c ON c.metadata_sha256=p.metadata_sha256 AND c.source_table=n.source_table
  WHERE r.source_family<>'ncen' OR f.source_file_id IS NULL OR p.package_id IS NULL OR c.source_table IS NULL
   OR n.source_sha256<>f.sha256 OR n.parser_version<>r.parser_version OR n.source_table<>f.relative_path
   OR jsonb_typeof(n.original_lexical_row)<>'object' OR jsonb_typeof(n.typed_projection)<>'object'
   OR jsonb_typeof(n.parse_errors)<>'array' OR jsonb_typeof(n.candidate_key_evidence)<>'object'
 ) THEN RAISE EXCEPTION 'invalid N-CEN V2 raw provenance or JSON shape'; END IF;
 RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS ncen_raw_v2_rows_provenance_insert ON ncen_raw_v2_rows;
CREATE TRIGGER ncen_raw_v2_rows_provenance_insert AFTER INSERT ON ncen_raw_v2_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION ncen_validate_raw_statement();
DROP TRIGGER IF EXISTS ncen_raw_v2_rows_provenance_update ON ncen_raw_v2_rows;
CREATE TRIGGER ncen_raw_v2_rows_provenance_update AFTER UPDATE ON ncen_raw_v2_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION ncen_validate_raw_statement();

CREATE OR REPLACE FUNCTION ncen_lock_raw_insert_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM new_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM sec_ingestion_runs WHERE run_id IN(SELECT ingestion_run_id FROM new_rows) AND raw_validated_at IS NOT NULL) THEN RAISE EXCEPTION 'N-CEN V2 raw rows immutable'; END IF; RETURN NULL; END $$;
CREATE OR REPLACE FUNCTION ncen_lock_raw_update_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN(SELECT ingestion_run_id FROM old_rows UNION SELECT ingestion_run_id FROM new_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM sec_ingestion_runs WHERE run_id IN(SELECT ingestion_run_id FROM old_rows UNION SELECT ingestion_run_id FROM new_rows) AND raw_validated_at IS NOT NULL) THEN RAISE EXCEPTION 'N-CEN V2 raw rows immutable'; END IF; RETURN NULL; END $$;
CREATE OR REPLACE FUNCTION ncen_lock_raw_delete_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN(SELECT ingestion_run_id FROM old_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM sec_ingestion_runs WHERE run_id IN(SELECT ingestion_run_id FROM old_rows) AND raw_validated_at IS NOT NULL) THEN RAISE EXCEPTION 'N-CEN V2 raw rows immutable'; END IF; RETURN NULL; END $$;
DROP TRIGGER IF EXISTS ncen_raw_v2_rows_lock_insert ON ncen_raw_v2_rows;
CREATE TRIGGER ncen_raw_v2_rows_lock_insert AFTER INSERT ON ncen_raw_v2_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION ncen_lock_raw_insert_statement();
DROP TRIGGER IF EXISTS ncen_raw_v2_rows_lock_update ON ncen_raw_v2_rows;
CREATE TRIGGER ncen_raw_v2_rows_lock_update AFTER UPDATE ON ncen_raw_v2_rows REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION ncen_lock_raw_update_statement();
DROP TRIGGER IF EXISTS ncen_raw_v2_rows_lock_delete ON ncen_raw_v2_rows;
CREATE TRIGGER ncen_raw_v2_rows_lock_delete AFTER DELETE ON ncen_raw_v2_rows REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION ncen_lock_raw_delete_statement();

CREATE OR REPLACE FUNCTION ncen_raw_run_reconciles(target_run_id uuid) RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$
DECLARE package_metadata char(64); table_count integer;
BEGIN
 SELECT p.metadata_sha256 INTO package_metadata FROM sec_source_packages p WHERE p.run_id=target_run_id AND p.source_family='ncen';
 SELECT count(*) INTO table_count FROM ncen_contract_tables WHERE metadata_sha256=package_metadata;
 IF package_metadata IS NULL OR table_count<>53 OR (SELECT count(*) FROM sec_table_reconciliations WHERE run_id=target_run_id)<>53 THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM sec_table_reconciliations t LEFT JOIN ncen_contract_tables c ON c.metadata_sha256=package_metadata AND c.source_table=t.table_name WHERE t.run_id=target_run_id AND c.source_table IS NULL) THEN RETURN false; END IF;
 IF EXISTS(
  SELECT 1 FROM ncen_contract_tables c LEFT JOIN sec_table_reconciliations t ON t.run_id=target_run_id AND t.table_name=c.source_table
  LEFT JOIN sec_source_files f ON f.source_file_id=t.source_file_id AND f.run_id=target_run_id
  LEFT JOIN LATERAL(SELECT count(DISTINCT r.raw_row_id) n,count(DISTINCT r.raw_row_id) FILTER(WHERE r.parse_status='typed') typed,count(DISTINCT r.source_file_id::text||':'||r.source_row_number) FILTER(WHERE i.status='quarantined') quarantined,count(DISTINCT r.source_file_id::text||':'||r.source_row_number) FILTER(WHERE i.status='rejected') rejected FROM ncen_raw_v2_rows r LEFT JOIN sec_row_issues i ON i.source_file_id=r.source_file_id AND i.source_row_number=r.source_row_number WHERE r.ingestion_run_id=target_run_id AND r.source_table=c.source_table) a ON true
  WHERE t.reconciliation_id IS NULL OR f.source_file_id IS NULL OR f.state<>'accounted' OR t.state<>'accounted'
   OR t.expected_count<>a.n OR t.source_count<>a.n OR t.lexical_count<>a.n OR t.typed_success_count<>a.typed OR t.quarantine_count<>a.quarantined OR t.reject_count<>a.rejected
 ) THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM sec_source_files f LEFT JOIN ncen_contract_tables c ON c.metadata_sha256=package_metadata AND c.source_table=f.relative_path WHERE f.run_id=target_run_id AND (c.source_table IS NULL AND f.relative_path<>'ncen_metadata.json' OR f.relative_path='ncen_metadata.json' AND (f.expected_count<>0 OR f.data_count<>0 OR f.lexical_count<>0 OR f.typed_success_count<>0 OR f.quarantine_count<>0 OR f.reject_count<>0))) THEN RETURN false; END IF;
 -- Recompute lexical typing, issue evidence, and candidate evidence from the
 -- immutable contract catalog.  Nothing written by the Python loader is
 -- treated as authoritative.
 IF EXISTS(
   SELECT 1 FROM ncen_raw_v2_rows r JOIN ncen_contract_tables c ON c.metadata_sha256=package_metadata AND c.source_table=r.source_table
   LEFT JOIN LATERAL (
     SELECT COALESCE(jsonb_agg(jsonb_build_object('column_name',i.column_name,'code',i.typed_error_code,'raw_value',i.raw_lexical_value,'detail',i.typed_error_detail) ORDER BY i.issue_sequence),'[]'::jsonb) errors
     FROM sec_row_issues i WHERE i.source_file_id=r.source_file_id AND i.source_row_number=r.source_row_number
   ) issues ON true
   LEFT JOIN LATERAL (
     SELECT COALESCE(jsonb_agg(r.original_lexical_row -> key ORDER BY ord),'[]'::jsonb) values
     FROM unnest(c.candidate_key) WITH ORDINALITY AS keys(key,ord)
   ) key_values ON true
   LEFT JOIN LATERAL (
     SELECT jsonb_object_agg(col->>'name', CASE
       WHEN COALESCE((r.original_lexical_row->>(col->>'name')),'')='' THEN 'null'::jsonb
       WHEN col->>'parsing_policy'='text_preserve_lexical' AND length((r.original_lexical_row->>(col->>'name'))) <= COALESCE((col->'datatype'->>'maxLength')::integer,2147483647) THEN to_jsonb((r.original_lexical_row->>(col->>'name')))
       WHEN col->>'parsing_policy'='decimal_preserve_lexical' AND (r.original_lexical_row->>(col->>'name')) ~ '^[+-]?(([0-9]+([.][0-9]*)?)|([.][0-9]+))([eE][+-]?[0-9]+)?$' AND lower((r.original_lexical_row->>(col->>'name'))) NOT IN ('nan','infinity','-infinity') THEN to_jsonb(((r.original_lexical_row->>(col->>'name'))::numeric::text))
       WHEN col->>'parsing_policy'='date_field_specific_fail_preserve_lexical' AND (r.original_lexical_row->>(col->>'name')) ~ '^[0-9]{2}-[A-Za-z]{3}-[0-9]{4}$' AND upper(to_char(to_date((r.original_lexical_row->>(col->>'name')),'DD-Mon-YYYY'),'DD-Mon-YYYY'))=upper((r.original_lexical_row->>(col->>'name'))) THEN to_jsonb(to_char(to_date((r.original_lexical_row->>(col->>'name')),'DD-Mon-YYYY'),'YYYY-MM-DD'))
       WHEN col->>'parsing_policy'='date_field_specific_fail_preserve_lexical' AND (r.original_lexical_row->>(col->>'name')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND to_char(to_date((r.original_lexical_row->>(col->>'name')),'YYYY-MM-DD'),'YYYY-MM-DD')=(r.original_lexical_row->>(col->>'name')) THEN to_jsonb(to_char(to_date((r.original_lexical_row->>(col->>'name')),'YYYY-MM-DD'),'YYYY-MM-DD'))
       ELSE 'null'::jsonb END ORDER BY ord) typed,
       COALESCE(jsonb_agg(jsonb_build_object('column_name',col->>'name','code',CASE WHEN COALESCE((r.original_lexical_row->>(col->>'name')),'')='' AND COALESCE((col->>'required')::boolean,false) THEN 'required_blank' WHEN col->>'parsing_policy'='text_preserve_lexical' THEN 'invalid_text' WHEN col->>'parsing_policy'='decimal_preserve_lexical' THEN CASE WHEN lower((r.original_lexical_row->>(col->>'name'))) IN ('nan','infinity','-infinity') THEN 'invalid_decimal' ELSE 'invalid_decimal' END WHEN col->>'parsing_policy'='date_field_specific_fail_preserve_lexical' THEN 'invalid_date' ELSE 'invalid_value' END,'raw_value',(r.original_lexical_row->>(col->>'name')),'detail',CASE WHEN COALESCE((r.original_lexical_row->>(col->>'name')),'')='' AND COALESCE((col->>'required')::boolean,false) THEN 'campo obrigatório vazio' WHEN col->>'parsing_policy'='text_preserve_lexical' THEN 'texto excede maxLength='||(col->'datatype'->>'maxLength') WHEN col->>'parsing_policy'='decimal_preserve_lexical' THEN CASE WHEN lower((r.original_lexical_row->>(col->>'name'))) IN ('nan','infinity','-infinity') THEN 'decimal não finito' ELSE 'decimal inválido' END WHEN col->>'parsing_policy'='date_field_specific_fail_preserve_lexical' THEN 'data inválida para o campo' ELSE 'política de parsing não reconhecida: '||(col->>'parsing_policy') END) ORDER BY ord) FILTER (WHERE (COALESCE((r.original_lexical_row->>(col->>'name')),'')='' AND COALESCE((col->>'required')::boolean,false)) OR (COALESCE((r.original_lexical_row->>(col->>'name')),'')<>'' AND col->>'parsing_policy'='text_preserve_lexical' AND length((r.original_lexical_row->>(col->>'name'))) > COALESCE((col->'datatype'->>'maxLength')::integer,2147483647)) OR (COALESCE((r.original_lexical_row->>(col->>'name')),'')<>'' AND col->>'parsing_policy'='decimal_preserve_lexical' AND NOT ((r.original_lexical_row->>(col->>'name')) ~ '^[+-]?(([0-9]+([.][0-9]*)?)|([.][0-9]+))([eE][+-]?[0-9]+)?$' AND lower((r.original_lexical_row->>(col->>'name'))) NOT IN ('nan','infinity','-infinity'))) OR (COALESCE((r.original_lexical_row->>(col->>'name')),'')<>'' AND col->>'parsing_policy'='date_field_specific_fail_preserve_lexical' AND NOT (((r.original_lexical_row->>(col->>'name')) ~ '^[0-9]{2}-[A-Za-z]{3}-[0-9]{4}$' AND upper(to_char(to_date((r.original_lexical_row->>(col->>'name')),'DD-Mon-YYYY'),'DD-Mon-YYYY'))=upper((r.original_lexical_row->>(col->>'name')))) OR ((r.original_lexical_row->>(col->>'name')) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND to_char(to_date((r.original_lexical_row->>(col->>'name')),'YYYY-MM-DD'),'YYYY-MM-DD')=(r.original_lexical_row->>(col->>'name')))))),'[]'::jsonb) errors
       FROM jsonb_array_elements(c.column_contract) WITH ORDINALITY AS columns(col,ord)
   ) expected ON true
   LEFT JOIN LATERAL (
     SELECT COALESCE(jsonb_agg(r.original_lexical_row->>key ORDER BY ord),'[]'::jsonb) values
     FROM unnest(c.candidate_key) WITH ORDINALITY AS keys(key,ord)
   ) key_values_text ON true
   WHERE r.ingestion_run_id=target_run_id AND (
     c.headers=ARRAY[]::text[] OR EXISTS(((SELECT key FROM (SELECT jsonb_object_keys(r.original_lexical_row)::text AS key) lexical_keys) EXCEPT (SELECT unnest(c.headers))))
     OR (SELECT count(*) FROM jsonb_object_keys(r.original_lexical_row))<>cardinality(c.headers)
     OR (SELECT count(*) FROM jsonb_object_keys(r.typed_projection))<>cardinality(c.headers)
     OR r.typed_projection IS DISTINCT FROM expected.typed
     OR r.parse_errors IS DISTINCT FROM expected.errors
     OR r.parse_errors IS DISTINCT FROM issues.errors
     OR r.parse_status IS DISTINCT FROM CASE WHEN expected.errors='[]'::jsonb THEN 'typed' ELSE 'quarantined' END
     OR r.candidate_key_evidence IS DISTINCT FROM jsonb_build_object('columns',to_jsonb(c.candidate_key),'complete',cardinality(c.candidate_key)>0 AND NOT EXISTS(SELECT 1 FROM unnest(c.candidate_key) key WHERE COALESCE(r.original_lexical_row->>key,'')='') AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(expected.errors) issue WHERE issue->>'column_name'=ANY(c.candidate_key)),'values',CASE WHEN cardinality(c.candidate_key)>0 AND NOT EXISTS(SELECT 1 FROM unnest(c.candidate_key) key WHERE COALESCE(r.original_lexical_row->>key,'')='') AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(expected.errors) issue WHERE issue->>'column_name'=ANY(c.candidate_key)) THEN key_values_text.values ELSE 'null'::jsonb END)
   )
 ) THEN RETURN false; END IF;
 -- Recompute the relationship identity from lexical evidence.  The source
 -- row identity remains (source_file_id, source_row_number), so duplicate
 -- candidate values remain legitimate evidence instead of a false uniqueness
 -- constraint.
 IF EXISTS(
   SELECT 1 FROM ncen_raw_v2_rows r JOIN ncen_contract_tables c ON c.metadata_sha256=package_metadata AND c.source_table=r.source_table
   WHERE r.ingestion_run_id=target_run_id AND (
     r.entity_key IS DISTINCT FROM CASE r.source_table
       WHEN 'FUND_REPORTED_INFO.tsv' THEN CASE WHEN COALESCE(r.original_lexical_row->>'FUND_ID','')='' THEN NULL ELSE 'fund:'||(r.original_lexical_row->>'FUND_ID') END
       WHEN 'DIRECTOR.tsv' THEN CASE WHEN COALESCE(r.original_lexical_row->>'ACCESSION_NUMBER','')='' OR COALESCE(r.original_lexical_row->>'DIRECTOR_SEQNUM','')='' THEN NULL ELSE 'director:'||(r.original_lexical_row->>'ACCESSION_NUMBER')||':'||(r.original_lexical_row->>'DIRECTOR_SEQNUM') END
       WHEN 'LINE_OF_CREDIT_DETAIL.tsv' THEN CASE WHEN COALESCE(r.original_lexical_row->>'FUND_ID','')='' OR COALESCE(r.original_lexical_row->>'LINE_OF_CREDIT_SEQNUM','')='' THEN NULL ELSE 'credit:'||(r.original_lexical_row->>'FUND_ID')||':'||(r.original_lexical_row->>'LINE_OF_CREDIT_SEQNUM') END
       WHEN 'VALUATION_METHOD_CHANGE.tsv' THEN CASE WHEN COALESCE(r.original_lexical_row->>'ACCESSION_NUMBER','')='' OR COALESCE(r.original_lexical_row->>'VALUATION_METHOD_CHANGE_SEQNUM','')='' THEN NULL ELSE 'valuation:'||(r.original_lexical_row->>'ACCESSION_NUMBER')||':'||(r.original_lexical_row->>'VALUATION_METHOD_CHANGE_SEQNUM') END
       ELSE CASE WHEN COALESCE(r.original_lexical_row->>'ACCESSION_NUMBER','')='' THEN NULL ELSE 'accession:'||(r.original_lexical_row->>'ACCESSION_NUMBER') END END
     OR r.parent_key IS DISTINCT FROM CASE WHEN cardinality(c.logical_parents)=0 THEN NULL WHEN c.logical_parents[1]='FUND_REPORTED_INFO.tsv' THEN CASE WHEN COALESCE(r.original_lexical_row->>'FUND_ID','')='' THEN NULL ELSE 'fund:'||(r.original_lexical_row->>'FUND_ID') END WHEN c.logical_parents[1]='DIRECTOR.tsv' THEN CASE WHEN COALESCE(r.original_lexical_row->>'ACCESSION_NUMBER','')='' OR COALESCE(r.original_lexical_row->>'DIRECTOR_SEQNUM','')='' THEN NULL ELSE 'director:'||(r.original_lexical_row->>'ACCESSION_NUMBER')||':'||(r.original_lexical_row->>'DIRECTOR_SEQNUM') END WHEN c.logical_parents[1]='LINE_OF_CREDIT_DETAIL.tsv' THEN CASE WHEN COALESCE(r.original_lexical_row->>'FUND_ID','')='' OR COALESCE(r.original_lexical_row->>'LINE_OF_CREDIT_SEQNUM','')='' THEN NULL ELSE 'credit:'||(r.original_lexical_row->>'FUND_ID')||':'||(r.original_lexical_row->>'LINE_OF_CREDIT_SEQNUM') END WHEN c.logical_parents[1]='VALUATION_METHOD_CHANGE.tsv' THEN CASE WHEN COALESCE(r.original_lexical_row->>'ACCESSION_NUMBER','')='' OR COALESCE(r.original_lexical_row->>'VALUATION_METHOD_CHANGE_SEQNUM','')='' THEN NULL ELSE 'valuation:'||(r.original_lexical_row->>'ACCESSION_NUMBER')||':'||(r.original_lexical_row->>'VALUATION_METHOD_CHANGE_SEQNUM') END ELSE CASE WHEN COALESCE(r.original_lexical_row->>'ACCESSION_NUMBER','')='' THEN NULL ELSE 'accession:'||(r.original_lexical_row->>'ACCESSION_NUMBER') END END
   )
 ) THEN RETURN false; END IF;
 -- Every declared parent resolves exactly once using all shared structural
 -- identifiers.  This preserves fund/director/provider/event child grains
 -- without pivoting and rejects both orphan and ambiguous child references.
 IF EXISTS(
   SELECT 1 FROM ncen_raw_v2_rows child
   JOIN ncen_contract_tables c ON c.metadata_sha256=package_metadata AND c.source_table=child.source_table
   JOIN LATERAL unnest(c.logical_parents) parent_name ON true
   LEFT JOIN LATERAL (SELECT count(*) n FROM ncen_raw_v2_rows parent WHERE parent.ingestion_run_id=target_run_id AND parent.source_table=parent_name AND parent.entity_key=child.parent_key) resolved ON true
   WHERE child.ingestion_run_id=target_run_id AND (child.parent_key IS NULL OR resolved.n<>1)
 ) THEN RETURN false; END IF;
 RETURN true;
END $$;
