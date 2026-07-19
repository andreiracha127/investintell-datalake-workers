-- RR1 V2: physical lexical evidence and typed projections remain unpivoted.
CREATE TABLE IF NOT EXISTS rr1_raw_v2_rows (
 raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 ingestion_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
 source_file_id uuid NOT NULL, source_row_number bigint NOT NULL CHECK(source_row_number >= 2),
 source_sha256 char(64) NOT NULL CHECK(source_sha256 ~ '^[0-9a-f]{64}$'),
 loaded_at timestamptz NOT NULL DEFAULT now(), parser_version text NOT NULL CHECK(parser_version <> ''),
 source_table text NOT NULL CHECK(source_table IN ('sub.tsv','tag.tsv','lab.tsv','cal.tsv','num.tsv','txt.tsv')),
 original_lexical_row jsonb NOT NULL, typed_projection jsonb NOT NULL,
 parse_status text NOT NULL CHECK(parse_status IN ('typed','quarantined','rejected')),
 parse_errors jsonb NOT NULL DEFAULT '[]'::jsonb, candidate_key_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
 adsh text, tag text, version text, ddate text, series text, class text, measure text, document text, otherdims text, iprx text,
 FOREIGN KEY(ingestion_run_id,source_file_id) REFERENCES sec_source_files(run_id,source_file_id) ON DELETE RESTRICT,
 UNIQUE(source_file_id,source_row_number)
);
CREATE INDEX IF NOT EXISTS rr1_raw_v2_rows_run_table_idx ON rr1_raw_v2_rows(ingestion_run_id,source_table,source_file_id,source_row_number);
CREATE INDEX IF NOT EXISTS rr1_raw_v2_rows_adsh_idx ON rr1_raw_v2_rows(ingestion_run_id,adsh);
CREATE INDEX IF NOT EXISTS rr1_raw_v2_rows_tag_idx ON rr1_raw_v2_rows(ingestion_run_id,tag,version);
CREATE INDEX IF NOT EXISTS rr1_raw_v2_rows_table_tag_idx ON rr1_raw_v2_rows(ingestion_run_id,source_table,tag,version);

CREATE TABLE IF NOT EXISTS rr1_contract_tables (
 metadata_sha256 char(64) NOT NULL CHECK(metadata_sha256 ~ '^[0-9a-f]{64}$'), source_table text NOT NULL,
 raw_target text NOT NULL, logical_parents text[] NOT NULL, candidate_key text[] NOT NULL, headers text[] NOT NULL,
 column_specs jsonb NOT NULL DEFAULT '{}'::jsonb,
 PRIMARY KEY(metadata_sha256,source_table), UNIQUE(metadata_sha256,raw_target)
);
ALTER TABLE rr1_contract_tables ADD COLUMN IF NOT EXISTS column_specs jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE OR REPLACE FUNCTION rr1_contract_catalog_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.column_specs='{}'::jsonb AND NEW.metadata_sha256=OLD.metadata_sha256
    AND NEW.source_table=OLD.source_table AND NEW.raw_target=OLD.raw_target
    AND NEW.logical_parents=OLD.logical_parents AND NEW.candidate_key=OLD.candidate_key
    AND NEW.headers=OLD.headers THEN RETURN NEW; END IF;
 RAISE EXCEPTION 'RR1 contract catalog is immutable';
END $$;

CREATE OR REPLACE FUNCTION rr1_expected_row_evidence(
 lexical jsonb, headers text[], specs jsonb
) RETURNS TABLE(typed jsonb, errors jsonb) LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE key text; spec jsonb; raw text; policy text; datatype jsonb; detail text; code text;
        invalid boolean; decimal_value numeric; parsed_date date; parsed_timestamp timestamp;
BEGIN
 typed := '{}'::jsonb; errors := '[]'::jsonb;
 FOREACH key IN ARRAY headers LOOP
  spec := specs->key; datatype := spec->'datatype'; raw := lexical->>key; policy := spec->>'parsing_policy';
  invalid := false; code := NULL; detail := NULL;
  IF raw='' THEN
   typed := typed || jsonb_build_object(key, NULL);
   IF COALESCE((spec->>'required')::boolean, false) THEN
    errors := errors || jsonb_build_array(jsonb_build_object('column_name',key,'code','required_blank','raw_value',raw,'detail','campo obrigatório vazio'));
   END IF;
   CONTINUE;
  END IF;
  IF datatype ? 'minLength' AND length(raw)<(datatype->>'minLength')::integer THEN
   invalid := true; code := CASE policy WHEN 'decimal_preserve_lexical' THEN 'invalid_decimal' WHEN 'date_field_specific_fail_preserve_lexical' THEN 'invalid_date' ELSE 'invalid_text' END; detail := 'valor abaixo de minLength=' || (datatype->>'minLength');
  ELSIF datatype ? 'maxLength' AND length(raw)>(datatype->>'maxLength')::integer THEN
   invalid := true; code := CASE policy WHEN 'decimal_preserve_lexical' THEN 'invalid_decimal' WHEN 'date_field_specific_fail_preserve_lexical' THEN 'invalid_date' ELSE 'invalid_text' END; detail := 'valor excede maxLength=' || (datatype->>'maxLength');
  ELSIF policy='text_preserve_lexical' THEN
   typed := typed || jsonb_build_object(key, raw);
  ELSIF policy='decimal_preserve_lexical' THEN
   IF raw !~ '^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$' THEN
    invalid := true; code := 'invalid_decimal'; detail := 'decimal com léxico inválido';
   ELSE
    decimal_value := raw::numeric;
    IF datatype ? 'minInclusive' AND decimal_value<(datatype->>'minInclusive')::numeric THEN
     invalid := true; code := 'invalid_decimal'; detail := 'decimal abaixo de minInclusive=' || (datatype->>'minInclusive');
    ELSIF datatype ? 'maxInclusive' AND decimal_value>(datatype->>'maxInclusive')::numeric THEN
     invalid := true; code := 'invalid_decimal'; detail := 'decimal acima de maxInclusive=' || (datatype->>'maxInclusive');
    ELSE typed := typed || jsonb_build_object(key, raw);
    END IF;
   END IF;
  ELSIF policy='date_field_specific_fail_preserve_lexical' THEN
   IF key='accepted' THEN
    IF raw !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{1,6}$' THEN
     invalid := true; code := 'invalid_date'; detail := 'timestamp accepted inválido';
    ELSE
     BEGIN
      parsed_timestamp := raw::timestamp;
     EXCEPTION WHEN SQLSTATE '22007' OR SQLSTATE '22008' THEN
      invalid := true; code := 'invalid_date'; detail := 'timestamp accepted inválido';
     END;
     IF NOT invalid THEN
      IF extract(year FROM parsed_timestamp) NOT BETWEEN 1900 AND 2027 THEN
       invalid := true; code := 'invalid_date'; detail := 'ano RR1 fora da faixa aceita';
      ELSE typed := typed || jsonb_build_object(key, CASE WHEN date_trunc('second', parsed_timestamp)=parsed_timestamp
        THEN to_char(parsed_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE to_char(parsed_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.US') END);
      END IF;
     END IF;
    END IF;
   ELSIF raw ~ '^\d{8}$' THEN
    parsed_date := NULL;
    BEGIN
     parsed_date := to_date(raw, 'YYYYMMDD');
     IF to_char(parsed_date, 'YYYYMMDD')<>raw THEN parsed_date := NULL; END IF;
    EXCEPTION WHEN SQLSTATE '22007' OR SQLSTATE '22008' THEN
     parsed_date := NULL;
    END;
    IF parsed_date IS NULL THEN
     BEGIN
      parsed_date := to_date(raw, 'DDMMYYYY');
      IF to_char(parsed_date, 'DDMMYYYY')<>raw THEN parsed_date := NULL; END IF;
     EXCEPTION WHEN SQLSTATE '22007' OR SQLSTATE '22008' THEN
      parsed_date := NULL;
     END;
    END IF;
    IF parsed_date IS NULL THEN invalid := true; code := 'invalid_date'; detail := 'data inválida para o campo';
    ELSIF extract(year FROM parsed_date) NOT BETWEEN 1900 AND 2027 THEN invalid := true; code := 'invalid_date'; detail := 'ano RR1 fora da faixa aceita';
    ELSIF key='ddate' AND (parsed_date + 1) <> date_trunc('month', parsed_date + interval '1 month')::date THEN invalid := true; code := 'invalid_date'; detail := 'data RR1 não é fim de mês';
    ELSE typed := typed || jsonb_build_object(key, to_char(parsed_date, 'YYYY-MM-DD')); END IF;
   ELSIF raw ~ '^\d{4}-\d{2}-\d{2}$' THEN
    BEGIN
     parsed_date := raw::date;
    EXCEPTION WHEN SQLSTATE '22007' OR SQLSTATE '22008' THEN
     invalid := true; code := 'invalid_date'; detail := 'data inválida para o campo';
    END;
    IF NOT invalid THEN
     IF extract(year FROM parsed_date) NOT BETWEEN 1900 AND 2027 THEN invalid := true; code := 'invalid_date'; detail := 'ano RR1 fora da faixa aceita';
     ELSIF key='ddate' AND (parsed_date + 1) <> date_trunc('month', parsed_date + interval '1 month')::date THEN invalid := true; code := 'invalid_date'; detail := 'data RR1 não é fim de mês';
     ELSE typed := typed || jsonb_build_object(key, to_char(parsed_date, 'YYYY-MM-DD')); END IF;
    END IF;
   ELSE invalid := true; code := 'invalid_date'; detail := 'data inválida para o campo';
   END IF;
  ELSE invalid := true; code := 'invalid_value'; detail := 'política de parsing não reconhecida: ' || policy;
  END IF;
  IF invalid THEN
   typed := typed || jsonb_build_object(key, NULL);
   errors := errors || jsonb_build_array(jsonb_build_object('column_name',key,'code',code,'raw_value',raw,'detail',detail));
  END IF;
 END LOOP;
 RETURN NEXT;
END $$;

CREATE OR REPLACE FUNCTION rr1_row_evidence_is_valid(n rr1_raw_v2_rows, c rr1_contract_tables) RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE expected record; key_complete boolean;
BEGIN
 SELECT * INTO expected FROM rr1_expected_row_evidence(n.original_lexical_row,c.headers,c.column_specs);
 key_complete := NOT EXISTS(SELECT 1 FROM unnest(c.candidate_key) key WHERE COALESCE(n.original_lexical_row->>key,'')='')
   AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(expected.errors) x WHERE x->>'column_name'=ANY(c.candidate_key));
 RETURN n.typed_projection=expected.typed AND n.parse_errors=expected.errors
   AND n.parse_status=CASE WHEN expected.errors='[]'::jsonb THEN 'typed' ELSE 'quarantined' END
   AND n.candidate_key_evidence IS NOT DISTINCT FROM jsonb_build_object(
     'columns', to_jsonb(c.candidate_key),
     'complete', key_complete,
     'values', CASE WHEN key_complete THEN to_jsonb(ARRAY(SELECT n.original_lexical_row->>key FROM unnest(c.candidate_key) key)) ELSE 'null'::jsonb END
   )
   AND n.adsh IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'adsh','')
   AND n.tag IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'tag','')
   AND n.version IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'version','')
   AND n.ddate IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'ddate','')
   AND n.series IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'series','')
   AND n.class IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'class','')
   AND n.measure IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'measure','')
   AND n.document IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'document','')
   AND n.otherdims IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'otherdims','')
   AND n.iprx IS NOT DISTINCT FROM NULLIF(n.original_lexical_row->>'iprx','');
END $$;

CREATE OR REPLACE FUNCTION rr1_validate_raw_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM new_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS (
  SELECT 1 FROM new_rows n JOIN sec_ingestion_runs r ON r.run_id=n.ingestion_run_id
  LEFT JOIN sec_source_files f ON (f.run_id,f.source_file_id)=(n.ingestion_run_id,n.source_file_id)
  LEFT JOIN sec_source_packages p ON p.run_id=n.ingestion_run_id AND p.source_family='rr1'
  LEFT JOIN rr1_contract_tables c ON c.metadata_sha256=p.metadata_sha256 AND c.source_table=n.source_table
  WHERE r.source_family<>'rr1' OR f.source_file_id IS NULL OR p.package_id IS NULL OR c.source_table IS NULL
    OR n.source_sha256<>f.sha256 OR n.parser_version<>r.parser_version OR n.source_table<>f.relative_path
    OR jsonb_typeof(n.original_lexical_row)<>'object' OR jsonb_typeof(n.typed_projection)<>'object'
    OR jsonb_typeof(n.parse_errors)<>'array' OR jsonb_typeof(n.candidate_key_evidence)<>'object'
    OR ARRAY(SELECT jsonb_object_keys(n.original_lexical_row) ORDER BY 1)<>ARRAY(SELECT unnest(c.headers) ORDER BY 1)
    OR ARRAY(SELECT jsonb_object_keys(n.typed_projection) ORDER BY 1)<>ARRAY(SELECT unnest(c.headers) ORDER BY 1)
    OR NOT rr1_row_evidence_is_valid(n,c)
 ) THEN RAISE EXCEPTION 'invalid RR1 V2 raw provenance, lexical, typed, or candidate evidence'; END IF;
 RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS rr1_raw_v2_rows_provenance_insert ON rr1_raw_v2_rows;
CREATE TRIGGER rr1_raw_v2_rows_provenance_insert AFTER INSERT ON rr1_raw_v2_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION rr1_validate_raw_statement();
DROP TRIGGER IF EXISTS rr1_raw_v2_rows_provenance_update ON rr1_raw_v2_rows;
CREATE TRIGGER rr1_raw_v2_rows_provenance_update AFTER UPDATE ON rr1_raw_v2_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION rr1_validate_raw_statement();

CREATE OR REPLACE FUNCTION rr1_lock_raw_insert_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM new_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM sec_ingestion_runs WHERE run_id IN(SELECT ingestion_run_id FROM new_rows) AND raw_validated_at IS NOT NULL) THEN RAISE EXCEPTION 'RR1 V2 raw rows immutable'; END IF; RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS rr1_raw_v2_rows_lock_insert ON rr1_raw_v2_rows;
CREATE TRIGGER rr1_raw_v2_rows_lock_insert AFTER INSERT ON rr1_raw_v2_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION rr1_lock_raw_insert_statement();
CREATE OR REPLACE FUNCTION rr1_lock_raw_update_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM old_rows UNION SELECT ingestion_run_id FROM new_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM sec_ingestion_runs WHERE run_id IN(SELECT ingestion_run_id FROM old_rows UNION SELECT ingestion_run_id FROM new_rows) AND raw_validated_at IS NOT NULL) THEN RAISE EXCEPTION 'RR1 V2 raw rows immutable'; END IF; RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS rr1_raw_v2_rows_lock_update ON rr1_raw_v2_rows;
CREATE TRIGGER rr1_raw_v2_rows_lock_update AFTER UPDATE ON rr1_raw_v2_rows REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION rr1_lock_raw_update_statement();
CREATE OR REPLACE FUNCTION rr1_lock_raw_delete_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM old_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM sec_ingestion_runs WHERE run_id IN(SELECT ingestion_run_id FROM old_rows) AND raw_validated_at IS NOT NULL) THEN RAISE EXCEPTION 'RR1 V2 raw rows immutable'; END IF; RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS rr1_raw_v2_rows_lock_delete ON rr1_raw_v2_rows;
CREATE TRIGGER rr1_raw_v2_rows_lock_delete AFTER DELETE ON rr1_raw_v2_rows REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION rr1_lock_raw_delete_statement();

CREATE OR REPLACE FUNCTION rr1_raw_run_reconciles(target_run_id uuid) RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$
DECLARE package_metadata char(64); table_count integer;
BEGIN
 SELECT p.metadata_sha256 INTO package_metadata FROM sec_source_packages p WHERE p.run_id=target_run_id AND p.source_family='rr1';
 SELECT count(*) INTO table_count FROM rr1_contract_tables WHERE metadata_sha256=package_metadata;
 IF package_metadata IS NULL OR table_count<>6 OR (SELECT count(*) FROM sec_table_reconciliations WHERE run_id=target_run_id)<>6 THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM rr1_contract_tables c LEFT JOIN sec_table_reconciliations t ON t.run_id=target_run_id AND t.table_name=c.source_table LEFT JOIN sec_source_files f ON f.run_id=target_run_id AND f.source_file_id=t.source_file_id LEFT JOIN LATERAL(SELECT count(*) n,count(*) FILTER(WHERE parse_status='typed') typed,count(*) FILTER(WHERE parse_status='quarantined') quarantined,count(*) FILTER(WHERE parse_status='rejected') rejected FROM rr1_raw_v2_rows r WHERE r.ingestion_run_id=target_run_id AND r.source_table=c.source_table) a ON true WHERE t.reconciliation_id IS NULL OR f.source_file_id IS NULL OR f.state<>'accounted' OR t.state<>'accounted' OR t.expected_count<>a.n OR t.source_count<>a.n OR t.lexical_count<>a.n OR t.typed_success_count<>a.typed OR t.quarantine_count<>a.quarantined OR t.reject_count<>a.rejected) THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM sec_source_files f LEFT JOIN rr1_contract_tables c ON c.metadata_sha256=package_metadata AND c.source_table=f.relative_path WHERE f.run_id=target_run_id AND c.source_table IS NULL AND f.relative_path NOT LIKE '%-metadata.json' AND f.relative_path<>'readme.htm') THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM rr1_raw_v2_rows r JOIN rr1_contract_tables c ON c.metadata_sha256=package_metadata AND c.source_table=r.source_table WHERE r.ingestion_run_id=target_run_id AND NOT rr1_row_evidence_is_valid(r,c)) THEN RETURN false; END IF;
 -- Invalid dates are quarantined by the parser; current fact candidates are typed only.
 IF EXISTS(SELECT 1 FROM rr1_raw_v2_rows WHERE ingestion_run_id=target_run_id AND source_table IN ('num.tsv','txt.tsv') AND parse_status='typed' AND (typed_projection->>'ddate') IS NULL) THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM rr1_raw_v2_rows child JOIN rr1_contract_tables c ON c.metadata_sha256=package_metadata AND c.source_table=child.source_table WHERE child.ingestion_run_id=target_run_id AND 'sub.tsv'=ANY(c.logical_parents) AND NOT EXISTS(SELECT 1 FROM rr1_raw_v2_rows parent WHERE parent.ingestion_run_id=target_run_id AND parent.source_table='sub.tsv' AND parent.adsh=child.adsh)) THEN RETURN false; END IF;
 -- lab.tsv may label taxonomy tags which are not materialized in this package's
 -- tag.tsv; it is not a tag-row referential child.  Numeric/text facts are.
 -- Compare the distinct pairs as sets: a correlated anti-join here is quadratic
 -- on real RR1 deliveries even though the parent table is small.
 IF EXISTS(
   SELECT 1 FROM (
     SELECT DISTINCT child.tag,child.version FROM rr1_raw_v2_rows child
     WHERE child.ingestion_run_id=target_run_id AND child.source_table IN ('num.tsv','txt.tsv')
     EXCEPT
     SELECT parent.tag,parent.version FROM rr1_raw_v2_rows parent
     WHERE parent.ingestion_run_id=target_run_id AND parent.source_table='tag.tsv'
   ) missing_tag
 ) THEN RETURN false; END IF;
 -- CAL has two explicit tag/version links rather than a tag/version context.
 IF EXISTS(SELECT 1 FROM rr1_raw_v2_rows child WHERE child.ingestion_run_id=target_run_id AND child.source_table='cal.tsv' AND (NOT EXISTS(SELECT 1 FROM rr1_raw_v2_rows parent WHERE parent.ingestion_run_id=target_run_id AND parent.source_table='tag.tsv' AND parent.tag=child.original_lexical_row->>'ptag' AND parent.version=child.original_lexical_row->>'pversion') OR NOT EXISTS(SELECT 1 FROM rr1_raw_v2_rows parent WHERE parent.ingestion_run_id=target_run_id AND parent.source_table='tag.tsv' AND parent.tag=child.original_lexical_row->>'ctag' AND parent.version=child.original_lexical_row->>'cversion'))) THEN RETURN false; END IF;
 RETURN true;
END $$;

-- Every user-schema routine must opt out of PostgreSQL's default PUBLIC EXECUTE.
REVOKE ALL ON FUNCTION rr1_contract_catalog_immutable() FROM PUBLIC;
REVOKE ALL ON FUNCTION rr1_expected_row_evidence(jsonb,text[],jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION rr1_row_evidence_is_valid(rr1_raw_v2_rows,rr1_contract_tables) FROM PUBLIC;
REVOKE ALL ON FUNCTION rr1_validate_raw_statement() FROM PUBLIC;
REVOKE ALL ON FUNCTION rr1_lock_raw_insert_statement() FROM PUBLIC;
REVOKE ALL ON FUNCTION rr1_lock_raw_update_statement() FROM PUBLIC;
REVOKE ALL ON FUNCTION rr1_lock_raw_delete_statement() FROM PUBLIC;
REVOKE ALL ON FUNCTION rr1_raw_run_reconciles(uuid) FROM PUBLIC;
