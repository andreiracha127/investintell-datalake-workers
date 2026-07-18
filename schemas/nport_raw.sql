-- Camada bruta N-PORT: uma linha física imutável por linha lexical de TSV.
-- Pré-requisito: schemas/sec_source_manifests.sql.

CREATE TABLE IF NOT EXISTS nport_raw_rows (
    raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ingestion_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    source_file_id uuid NOT NULL,
    source_row_number bigint NOT NULL CHECK (source_row_number >= 2),
    source_sha256 char(64) NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    loaded_at timestamptz NOT NULL DEFAULT now(),
    parser_version text NOT NULL CHECK (parser_version <> ''),
    source_table text NOT NULL CHECK (source_table ~ '^[A-Z0-9_]+[.]tsv$'),
    original_lexical_row jsonb NOT NULL,
    typed_projection jsonb NOT NULL,
    parse_status text NOT NULL CHECK (parse_status IN ('typed', 'quarantined', 'rejected')),
    parse_errors jsonb NOT NULL DEFAULT '[]'::jsonb,
    candidate_key_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    accession_number text,
    holding_id text,
    FOREIGN KEY (ingestion_run_id, source_file_id)
        REFERENCES sec_source_files(run_id, source_file_id) ON DELETE RESTRICT,
    UNIQUE (source_file_id, source_row_number)
);

CREATE TABLE IF NOT EXISTS nport_holding_accession_map (
    ingestion_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    holding_id text NOT NULL CHECK (holding_id <> ''),
    accession_number text NOT NULL CHECK (accession_number <> ''),
    source_file_id uuid NOT NULL,
    source_row_number bigint NOT NULL,
    PRIMARY KEY (ingestion_run_id, holding_id),
    FOREIGN KEY (ingestion_run_id, source_file_id)
        REFERENCES sec_source_files(run_id, source_file_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS nport_raw_rows_run_table_idx
    ON nport_raw_rows (ingestion_run_id, source_table, source_file_id, source_row_number);
CREATE INDEX IF NOT EXISTS nport_raw_rows_run_holding_idx
    ON nport_raw_rows (ingestion_run_id, holding_id) WHERE holding_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS nport_raw_rows_run_file_row_uq
    ON nport_raw_rows (ingestion_run_id, source_file_id, source_row_number);
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'nport_holding_accession_map'::regclass
          AND conname = 'nport_holding_map_raw_row_fk'
    ) THEN
        ALTER TABLE nport_holding_accession_map
        ADD CONSTRAINT nport_holding_map_raw_row_fk
        FOREIGN KEY (ingestion_run_id, source_file_id, source_row_number)
        REFERENCES nport_raw_rows (ingestion_run_id, source_file_id, source_row_number)
        ON DELETE RESTRICT;
    END IF;
END $$;

-- Catálogo DB do contrato fechado, com a semântica suficiente para validar
-- relações sem confiar no processo Python.  Os VALUES são uma projeção do
-- contrato versionado; o teste Python faz a paridade completa contra o JSON.
CREATE TABLE IF NOT EXISTS nport_contract_tables (source_table text PRIMARY KEY);
ALTER TABLE nport_contract_tables ADD COLUMN IF NOT EXISTS raw_target text;
ALTER TABLE nport_contract_tables ADD COLUMN IF NOT EXISTS logical_parents text[];
ALTER TABLE nport_contract_tables ADD COLUMN IF NOT EXISTS candidate_key text[];
ALTER TABLE nport_contract_tables ADD COLUMN IF NOT EXISTS columns text[];
ALTER TABLE nport_contract_tables ADD COLUMN IF NOT EXISTS required_columns text[];
ALTER TABLE nport_contract_tables ADD COLUMN IF NOT EXISTS column_specs jsonb;
ALTER TABLE nport_contract_tables ADD COLUMN IF NOT EXISTS table_ordinal integer;
ALTER TABLE nport_contract_tables ALTER COLUMN columns DROP NOT NULL;
ALTER TABLE nport_contract_tables ALTER COLUMN required_columns DROP NOT NULL;
ALTER TABLE nport_contract_tables ALTER COLUMN column_specs DROP NOT NULL;
ALTER TABLE nport_contract_tables ALTER COLUMN table_ordinal DROP NOT NULL;
DROP TRIGGER IF EXISTS nport_contract_tables_immutable ON nport_contract_tables;
INSERT INTO nport_contract_tables(source_table, raw_target, logical_parents, candidate_key) VALUES
 ('BORROWER.tsv','nport_borrower_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER','BORROWER_ID']),
 ('BORROW_AGGREGATE.tsv','nport_borrow_aggregate_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER','BORROW_AGGREGATE_ID']),
 ('CONVERTIBLE_SECURITY_CURRENCY.tsv','nport_convertible_security_currency_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','CONVERTIBLE_SECURITY_ID']),
 ('DEBT_SECURITY.tsv','nport_debt_security_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY[]::text[]),
 ('DEBT_SECURITY_REF_INSTRUMENT.tsv','nport_debt_security_ref_instrument_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','DEBT_SECURITY_REF_ID']),
 ('DERIVATIVE_COUNTERPARTY.tsv','nport_derivative_counterparty_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','DERIVATIVE_COUNTERPARTY_ID']),
 ('DESC_REF_INDEX_BASKET.tsv','nport_desc_ref_index_basket_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID']),
 ('DESC_REF_INDEX_COMPONENT.tsv','nport_desc_ref_index_component_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','DESC_REF_INDEX_COMPONENT_ID']),
 ('DESC_REF_OTHER.tsv','nport_desc_ref_other_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','DESC_REF_OTHER_ID']),
 ('EXPLANATORY_NOTE.tsv','nport_explanatory_note_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER','EXPLANATORY_NOTE_ID']),
 ('FLOATING_RATE_RESET_TENOR.tsv','nport_floating_rate_reset_tenor_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','RATE_RESET_TENOR_ID']),
 ('FUND_REPORTED_HOLDING.tsv','nport_fund_reported_holding_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER','HOLDING_ID']),
 ('FUND_REPORTED_INFO.tsv','nport_fund_reported_info_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER']),
 ('FUND_VAR_INFO.tsv','nport_fund_var_info_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER']),
 ('FUT_FWD_NONFOREIGNCUR_CONTRACT.tsv','nport_fut_fwd_nonforeigncur_contract_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID']),
 ('FWD_FOREIGNCUR_CONTRACT_SWAP.tsv','nport_fwd_foreigncur_contract_swap_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID']),
 ('IDENTIFIERS.tsv','nport_identifiers_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','IDENTIFIERS_ID']),
 ('INTEREST_RATE_RISK.tsv','nport_interest_rate_risk_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER','INTEREST_RATE_RISK_ID']),
 ('MONTHLY_RETURN_CAT_INSTRUMENT.tsv','nport_monthly_return_cat_instrument_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER','ASSET_CAT','INSTRUMENT_KIND']),
 ('MONTHLY_TOTAL_RETURN.tsv','nport_monthly_total_return_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER','MONTHLY_TOTAL_RETURN_ID']),
 ('NONFOREIGN_EXCHANGE_SWAP.tsv','nport_nonforeign_exchange_swap_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID']),
 ('OTHER_DERIV.tsv','nport_other_deriv_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID']),
 ('OTHER_DERIV_NOTIONAL_AMOUNT.tsv','nport_other_deriv_notional_amount_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','OTHER_DERIV_NOTIONAL_AMOUNT_ID']),
 ('REGISTRANT.tsv','nport_registrant_raw',ARRAY['SUBMISSION.tsv'],ARRAY['ACCESSION_NUMBER']),
 ('REPURCHASE_AGREEMENT.tsv','nport_repurchase_agreement_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID']),
 ('REPURCHASE_COLLATERAL.tsv','nport_repurchase_collateral_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','REPURCHASE_COLLATERAL_ID']),
 ('REPURCHASE_COUNTERPARTY.tsv','nport_repurchase_counterparty_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID','REPURCHASE_COUNTERPARTY_ID']),
 ('SECURITIES_LENDING.tsv','nport_securities_lending_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID']),
 ('SUBMISSION.tsv','nport_submission_raw',ARRAY[]::text[],ARRAY['ACCESSION_NUMBER']),
 ('SWAPTION_OPTION_WARNT_DERIV.tsv','nport_swaption_option_warnt_deriv_raw',ARRAY['FUND_REPORTED_HOLDING.tsv'],ARRAY['HOLDING_ID'])
ON CONFLICT (source_table) DO UPDATE SET raw_target = EXCLUDED.raw_target,
    logical_parents = EXCLUDED.logical_parents, candidate_key = EXCLUDED.candidate_key;
ALTER TABLE nport_contract_tables ALTER COLUMN raw_target SET NOT NULL;
ALTER TABLE nport_contract_tables ALTER COLUMN logical_parents SET NOT NULL;
ALTER TABLE nport_contract_tables ALTER COLUMN candidate_key SET NOT NULL;
CREATE OR REPLACE FUNCTION nport_contract_catalog_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  RAISE EXCEPTION 'N-PORT contract catalog is immutable';
END $$;
CREATE TRIGGER nport_contract_tables_immutable BEFORE INSERT OR UPDATE OR DELETE
ON nport_contract_tables FOR EACH ROW EXECUTE FUNCTION nport_contract_catalog_immutable();
REVOKE ALL ON nport_contract_tables FROM PUBLIC;

CREATE OR REPLACE FUNCTION nport_contract_catalog_payload()
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public AS $$
  SELECT jsonb_agg(jsonb_build_object(
    'table_ordinal', table_ordinal,
    'source_table', source_table,
    'raw_target', raw_target,
    'logical_parents', to_jsonb(logical_parents),
    'candidate_key', to_jsonb(candidate_key),
    'columns', to_jsonb(columns),
    'required_columns', to_jsonb(required_columns),
    'column_specs', column_specs
  ) ORDER BY table_ordinal)
  FROM public.nport_contract_tables;
$$;

CREATE OR REPLACE FUNCTION nport_contract_catalog_sha256()
RETURNS text LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public AS $$
  SELECT encode(sha256(convert_to(public.nport_contract_catalog_payload()::text, 'UTF8')), 'hex');
$$;

CREATE OR REPLACE FUNCTION nport_install_contract_catalog(payload jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE item jsonb; actual_sha text;
BEGIN
  actual_sha := encode(sha256(convert_to(payload::text, 'UTF8')), 'hex');
  IF jsonb_typeof(payload) <> 'array' OR jsonb_array_length(payload) <> 30
     OR actual_sha <> '688dc665eced0360b812e9b119c7e8fcd40c444cbdbe7a745dcdca90e662c896' THEN
    RAISE EXCEPTION 'N-PORT contract catalog payload is not canonical';
  END IF;
  EXECUTE 'ALTER TABLE public.nport_contract_tables DISABLE TRIGGER nport_contract_tables_immutable';
  FOR item IN SELECT value FROM jsonb_array_elements(payload) LOOP
    UPDATE public.nport_contract_tables SET
      table_ordinal=(item->>'table_ordinal')::integer,
      raw_target=item->>'raw_target',
      logical_parents=ARRAY(SELECT jsonb_array_elements_text(item->'logical_parents')),
      candidate_key=ARRAY(SELECT jsonb_array_elements_text(item->'candidate_key')),
      columns=ARRAY(SELECT jsonb_array_elements_text(item->'columns')),
      required_columns=ARRAY(SELECT jsonb_array_elements_text(item->'required_columns')),
      column_specs=item->'column_specs'
    WHERE source_table=item->>'source_table';
    IF NOT FOUND THEN RAISE EXCEPTION 'unknown N-PORT contract table'; END IF;
  END LOOP;
  IF EXISTS (SELECT 1 FROM public.nport_contract_tables
             WHERE table_ordinal IS NULL OR columns IS NULL OR required_columns IS NULL OR column_specs IS NULL)
     OR (SELECT count(DISTINCT table_ordinal) FROM public.nport_contract_tables) <> 30 THEN
    RAISE EXCEPTION 'incomplete N-PORT contract catalog';
  END IF;
  EXECUTE 'ALTER TABLE public.nport_contract_tables ENABLE TRIGGER nport_contract_tables_immutable';
  ALTER TABLE public.nport_contract_tables ALTER COLUMN columns SET NOT NULL;
  ALTER TABLE public.nport_contract_tables ALTER COLUMN required_columns SET NOT NULL;
  ALTER TABLE public.nport_contract_tables ALTER COLUMN column_specs SET NOT NULL;
  ALTER TABLE public.nport_contract_tables ALTER COLUMN table_ordinal SET NOT NULL;
END $$;
REVOKE ALL ON FUNCTION nport_install_contract_catalog(jsonb) FROM PUBLIC;

-- This independently derives the governed parser result. Direct SQL never
-- supplies trusted typed or error evidence.
CREATE OR REPLACE FUNCTION nport_expected_row(original jsonb, specs jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE spec jsonb; name text; raw text; policy text; typed jsonb := '{}'::jsonb;
        errors jsonb := '[]'::jsonb; error_code text; detail text; value jsonb;
        matched text[]; integer_part text; fraction_part text; exponent_text text;
        exponent_value integer; coefficient text; significant text;
        integral_digits integer; fractional_digits integer; max_length integer;
        expected_numeric numeric; expected_date date;
BEGIN
  IF jsonb_typeof(original) <> 'object' OR jsonb_typeof(specs) <> 'array' THEN RETURN NULL; END IF;
  FOR spec IN SELECT element.item FROM jsonb_array_elements(specs) AS element(item) LOOP
    name := spec->>'name'; raw := original->>name; policy := spec->>'parsing_policy';
    error_code := NULL; detail := NULL; value := 'null'::jsonb;
    IF name IS NULL OR raw IS NULL OR jsonb_typeof(original->name) <> 'string' THEN RETURN NULL; END IF;
    IF raw = '' THEN
      IF COALESCE((spec->>'required')::boolean, false) THEN
        error_code := 'required_blank'; detail := 'campo obrigatório vazio';
      END IF;
    ELSIF policy = 'text_preserve_lexical' THEN
      max_length := NULLIF(spec->'datatype'->>'maxLength', '')::integer;
      IF max_length IS NOT NULL AND length(raw) > max_length THEN
        error_code := 'invalid_text'; detail := format('texto excede maxLength=%s', max_length);
      ELSE value := to_jsonb(raw); END IF;
    ELSIF policy = 'decimal_preserve_lexical' THEN
      matched := regexp_match(raw, '^[+-]?(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-]?\d+))?$');
      IF matched IS NULL THEN
        error_code := 'invalid_decimal'; detail := 'decimal inválido';
      ELSE
        integer_part := COALESCE(matched[1], '');
        fraction_part := CASE WHEN matched[1] IS NOT NULL THEN COALESCE(matched[2], '') ELSE COALESCE(matched[3], '') END;
        exponent_text := COALESCE(matched[4], '0');
        IF length(ltrim(exponent_text, '+-0')) > 6 THEN
          error_code := 'decimal_out_of_domain'; detail := 'decimal fora do domínio governado';
        ELSE
          exponent_value := exponent_text::integer;
          coefficient := integer_part || fraction_part;
          significant := regexp_replace(coefficient, '^0+', '');
          integral_digits := CASE WHEN significant = '' THEN 0
                                  ELSE greatest(length(integer_part) + exponent_value - (length(coefficient) - length(significant)), 0) END;
          fractional_digits := greatest(length(fraction_part) - exponent_value, 0);
          IF integral_digits > 131072 OR fractional_digits > 16383 THEN
            error_code := 'decimal_out_of_domain'; detail := 'decimal fora do domínio governado';
          ELSE
            BEGIN
              expected_numeric := raw::numeric;
              value := to_jsonb(expected_numeric::text);
            EXCEPTION WHEN OTHERS THEN
              error_code := 'invalid_decimal'; detail := 'decimal inválido';
            END;
          END IF;
        END IF;
      END IF;
    ELSIF policy = 'date_field_specific_fail_preserve_lexical' THEN
      BEGIN
        IF raw ~ '^\d{2}-[A-Z]{3}-\d{4}$' THEN
          expected_date := make_date(
            substring(raw,8,4)::integer,
            CASE substring(raw,4,3)
              WHEN 'JAN' THEN 1 WHEN 'FEB' THEN 2 WHEN 'MAR' THEN 3 WHEN 'APR' THEN 4
              WHEN 'MAY' THEN 5 WHEN 'JUN' THEN 6 WHEN 'JUL' THEN 7 WHEN 'AUG' THEN 8
              WHEN 'SEP' THEN 9 WHEN 'OCT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEC' THEN 12
              ELSE 0 END,
            substring(raw,1,2)::integer);
        ELSIF raw ~ '^\d{4}-\d{2}-\d{2}$' THEN
          expected_date := make_date(substring(raw,1,4)::integer, substring(raw,6,2)::integer, substring(raw,9,2)::integer);
        ELSE RAISE EXCEPTION 'invalid date'; END IF;
        value := to_jsonb(to_char(expected_date, 'YYYY-MM-DD'));
      EXCEPTION WHEN OTHERS THEN
        error_code := 'invalid_date'; detail := 'data inválida para o campo';
      END;
    ELSE RETURN NULL;
    END IF;
    typed := typed || jsonb_build_object(name, value);
    IF error_code IS NOT NULL THEN
      errors := errors || jsonb_build_array(jsonb_build_object(
        'column_name', name, 'code', error_code, 'raw_value', raw, 'detail', detail));
    END IF;
  END LOOP;
  RETURN jsonb_build_object(
    'typed_projection', typed,
    'parse_errors', errors,
    'parse_status', CASE WHEN jsonb_array_length(errors)=0 THEN 'typed' ELSE 'quarantined' END);
END $$;

CREATE OR REPLACE FUNCTION nport_validate_raw_statement()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM new_rows) ORDER BY r.run_id FOR UPDATE;
  IF EXISTS (
    SELECT 1 FROM new_rows n
    LEFT JOIN sec_source_files f ON (f.run_id,f.source_file_id)=(n.ingestion_run_id,n.source_file_id)
    JOIN sec_ingestion_runs r ON r.run_id=n.ingestion_run_id
    LEFT JOIN nport_contract_tables c ON c.source_table=n.source_table
    WHERE r.source_family <> 'nport' OR f.source_file_id IS NULL OR n.source_sha256 <> f.sha256
       OR n.parser_version <> r.parser_version OR n.source_table <> f.relative_path OR c.source_table IS NULL
       OR c.columns IS NULL OR c.required_columns IS NULL
       OR jsonb_typeof(n.original_lexical_row) <> 'object'
       OR EXISTS (SELECT 1 FROM jsonb_each(n.original_lexical_row) lexical_value
                  WHERE jsonb_typeof(lexical_value.value) <> 'string')
       OR EXISTS (SELECT 1 FROM jsonb_object_keys(n.original_lexical_row) k WHERE NOT k = ANY(c.columns))
       OR EXISTS (SELECT 1 FROM unnest(c.columns) k WHERE NOT n.original_lexical_row ? k)
       OR jsonb_typeof(n.typed_projection) <> 'object'
       OR EXISTS (SELECT 1 FROM jsonb_object_keys(n.typed_projection) k WHERE NOT k = ANY(c.columns))
       OR EXISTS (SELECT 1 FROM unnest(c.columns) k WHERE NOT n.typed_projection ? k)
       OR jsonb_typeof(n.parse_errors) <> 'array' OR jsonb_typeof(n.candidate_key_evidence) <> 'object'
       OR n.candidate_key_evidence->'columns' IS DISTINCT FROM to_jsonb(c.candidate_key)
       OR n.candidate_key_evidence->'values' IS DISTINCT FROM to_jsonb(ARRAY(SELECT n.original_lexical_row->>k FROM unnest(c.candidate_key) k))
       OR COALESCE((n.candidate_key_evidence->>'complete')::boolean, false)
            IS DISTINCT FROM (cardinality(c.candidate_key) > 0 AND NOT EXISTS (
                SELECT 1 FROM unnest(c.candidate_key) k WHERE n.original_lexical_row->>k = ''
            ) AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(n.parse_errors) e WHERE e->>'column_name' = ANY(c.candidate_key)
            ))
       OR (n.original_lexical_row ? 'ACCESSION_NUMBER'
           AND n.accession_number IS DISTINCT FROM NULLIF(n.original_lexical_row->>'ACCESSION_NUMBER', ''))
       OR (n.original_lexical_row ? 'HOLDING_ID'
           AND n.holding_id IS DISTINCT FROM NULLIF(n.original_lexical_row->>'HOLDING_ID', ''))
  ) THEN
    RAISE EXCEPTION 'invalid N-PORT raw provenance or JSON shape';
  END IF;
  RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS nport_raw_rows_provenance ON nport_raw_rows;
CREATE TRIGGER nport_raw_rows_provenance AFTER INSERT ON nport_raw_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION nport_validate_raw_statement();
DROP TRIGGER IF EXISTS nport_raw_rows_provenance_update ON nport_raw_rows;
CREATE TRIGGER nport_raw_rows_provenance_update AFTER UPDATE ON nport_raw_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION nport_validate_raw_statement();

CREATE OR REPLACE FUNCTION nport_raw_run_reconciles(target_run_id uuid)
RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$
BEGIN
  -- Catalog and reconciliation coverage are exact in both directions.
  IF (SELECT count(*) FROM nport_contract_tables) <> 30
     OR nport_contract_catalog_sha256() <> '688dc665eced0360b812e9b119c7e8fcd40c444cbdbe7a745dcdca90e662c896'
     OR EXISTS (
       SELECT 1 FROM nport_contract_tables c
       WHERE c.table_ordinal NOT BETWEEN 1 AND 30
          OR jsonb_typeof(c.column_specs) <> 'array'
          OR jsonb_array_length(c.column_specs) <> cardinality(c.columns)
          OR ARRAY(SELECT spec->>'name' FROM jsonb_array_elements(c.column_specs) spec) <> c.columns
          OR (SELECT count(DISTINCT spec->>'name') FROM jsonb_array_elements(c.column_specs) spec) <> cardinality(c.columns)
          OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(c.column_specs) spec
            WHERE spec->>'parsing_policy' NOT IN (
              'text_preserve_lexical','decimal_preserve_lexical','date_field_specific_fail_preserve_lexical'
            ) OR jsonb_typeof(spec->'datatype') <> 'object'
              OR jsonb_typeof(spec->'required') <> 'boolean'
              OR jsonb_typeof(spec->'datatype'->'base') <> 'string'
          )
     )
     OR (SELECT count(*) FROM sec_table_reconciliations WHERE run_id = target_run_id) <> 30
     OR EXISTS (SELECT 1 FROM sec_table_reconciliations t LEFT JOIN nport_contract_tables c ON c.source_table = t.table_name WHERE t.run_id = target_run_id AND c.source_table IS NULL)
     OR EXISTS (
       SELECT 1 FROM sec_source_files f
       LEFT JOIN nport_contract_tables c ON c.source_table = f.relative_path
       WHERE f.run_id = target_run_id
         AND (c.source_table IS NULL AND f.relative_path <> 'nport_metadata.json'
              OR f.relative_path = 'nport_metadata.json' AND (
                   f.expected_count <> 0 OR f.data_count <> 0 OR f.lexical_count <> 0
                   OR f.typed_success_count <> 0 OR f.quarantine_count <> 0 OR f.reject_count <> 0
              ))
     ) THEN
    RETURN false;
  END IF;
  -- Every physical TSV must be accounted by its own reconciliation. Metadata
  -- may account only declared-absent tables, never a real TSV under another
  -- table name.
  IF EXISTS (
    SELECT 1 FROM sec_source_files f
    WHERE f.run_id = target_run_id AND f.relative_path <> 'nport_metadata.json'
      AND NOT EXISTS (SELECT 1 FROM sec_table_reconciliations t
                      WHERE t.run_id=f.run_id AND t.source_file_id=f.source_file_id
                        AND t.table_name=f.relative_path)
  ) OR EXISTS (
    SELECT 1 FROM sec_table_reconciliations t
    JOIN sec_source_files f ON f.run_id=t.run_id AND f.source_file_id=t.source_file_id
    WHERE t.run_id=target_run_id AND f.relative_path <> 'nport_metadata.json'
      AND t.table_name <> f.relative_path
  ) THEN RETURN false; END IF;

  -- Reconciliations, physical raw rows, and actual issue rows must describe
  -- the same facts; supplied counters are never trusted on their own.
  IF EXISTS (
    SELECT 1
    FROM nport_contract_tables c
    LEFT JOIN sec_table_reconciliations t ON t.run_id = target_run_id AND t.table_name = c.source_table
    LEFT JOIN sec_source_files f ON f.run_id = target_run_id AND f.source_file_id = t.source_file_id
    LEFT JOIN LATERAL (
      SELECT count(DISTINCT r.raw_row_id) AS physical_count,
             count(DISTINCT r.raw_row_id) FILTER (WHERE r.parse_status = 'typed') AS typed_count,
             count(DISTINCT r.source_row_number) FILTER (WHERE i.status = 'quarantined') AS quarantined_count,
             count(DISTINCT r.source_row_number) FILTER (WHERE i.status = 'rejected') AS rejected_count
      FROM nport_raw_rows r
      LEFT JOIN sec_row_issues i ON i.source_file_id = r.source_file_id
        AND i.source_row_number = r.source_row_number
      WHERE r.ingestion_run_id = target_run_id AND r.source_table = c.source_table
    ) actual ON true
    WHERE f.source_file_id IS NULL OR t.reconciliation_id IS NULL
      OR f.state <> 'accounted' OR t.state <> 'accounted'
      OR ((actual.physical_count > 0 OR c.source_table = 'SUBMISSION.tsv') AND f.relative_path <> c.source_table)
      OR (f.relative_path = c.source_table AND (f.expected_count <> actual.physical_count OR f.data_count <> actual.physical_count
          OR f.lexical_count <> actual.physical_count OR f.typed_success_count <> actual.typed_count
          OR f.quarantine_count <> actual.quarantined_count OR f.reject_count <> actual.rejected_count))
      OR t.expected_count <> actual.physical_count OR t.source_count <> actual.physical_count
      OR t.lexical_count <> actual.physical_count OR t.typed_success_count <> actual.typed_count
      OR t.quarantine_count <> actual.quarantined_count OR t.reject_count <> actual.rejected_count
  ) THEN RETURN false; END IF;

  -- Every raw row remains tied to the run's declared source file and frozen contract.
  IF EXISTS (
    SELECT 1 FROM nport_raw_rows r
    LEFT JOIN sec_source_files f ON (f.run_id, f.source_file_id) = (r.ingestion_run_id, r.source_file_id)
    LEFT JOIN sec_ingestion_runs run ON run.run_id = r.ingestion_run_id
    LEFT JOIN nport_contract_tables c ON c.source_table = r.source_table
    WHERE r.ingestion_run_id = target_run_id AND (f.source_file_id IS NULL OR c.source_table IS NULL
      OR run.source_family <> 'nport' OR r.source_sha256 <> f.sha256 OR r.parser_version <> run.parser_version
      OR r.source_table <> f.relative_path)
  ) THEN RETURN false; END IF;

  -- The typed projection is a governed derivation, never caller-supplied
  -- evidence. Validate every persisted value against its lexical source.
  IF EXISTS (
    SELECT 1 FROM nport_raw_rows r
    JOIN nport_contract_tables c ON c.source_table = r.source_table
    CROSS JOIN LATERAL (SELECT nport_expected_row(r.original_lexical_row, c.column_specs) AS row) expected
    WHERE r.ingestion_run_id = target_run_id
      AND (c.column_specs IS NULL
           OR jsonb_array_length(c.column_specs) <> cardinality(c.columns)
           OR expected.row IS NULL
           OR r.typed_projection IS DISTINCT FROM expected.row->'typed_projection'
           OR r.parse_errors IS DISTINCT FROM expected.row->'parse_errors'
           OR r.parse_status IS DISTINCT FROM expected.row->>'parse_status')
  ) THEN RETURN false; END IF;

  -- Disposition is checked only at publication, after COPY and issue rows have
  -- both landed. This avoids an ordering dependency inside a bounded batch.
  IF EXISTS (
    SELECT 1
    FROM nport_raw_rows r
    JOIN nport_contract_tables c ON c.source_table = r.source_table
    WHERE r.ingestion_run_id = target_run_id
      AND (
        (r.parse_status = 'typed' AND (
          r.parse_errors <> '[]'::jsonb OR EXISTS (
            SELECT 1 FROM sec_row_issues i
            WHERE i.source_file_id = r.source_file_id
              AND i.source_row_number = r.source_row_number
          )
        ))
        OR (r.parse_status IN ('quarantined', 'rejected') AND (
          r.parse_errors = '[]'::jsonb
          OR NOT EXISTS (
            SELECT 1 FROM sec_row_issues i
            WHERE i.source_file_id = r.source_file_id
              AND i.source_row_number = r.source_row_number
              AND i.status = r.parse_status
          )
          OR EXISTS (
            SELECT 1 FROM sec_row_issues i
            WHERE i.source_file_id = r.source_file_id
              AND i.source_row_number = r.source_row_number
              AND i.status <> r.parse_status
          )
          OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(r.parse_errors) e
            WHERE r.typed_projection->(e->>'column_name') <> 'null'::jsonb
          )
          OR r.parse_errors IS DISTINCT FROM COALESCE((
            SELECT jsonb_agg(
              jsonb_build_object(
                'column_name', i.column_name,
                'code', i.typed_error_code,
                'raw_value', i.raw_lexical_value,
                'detail', i.typed_error_detail
              ) ORDER BY i.issue_sequence
            )
            FROM sec_row_issues i
            WHERE i.source_file_id = r.source_file_id
              AND i.source_row_number = r.source_row_number
              AND i.status = r.parse_status
          ), '[]'::jsonb)
        ))
        OR EXISTS (
          SELECT 1 FROM unnest(c.required_columns) required_column
          WHERE COALESCE(r.original_lexical_row->>required_column, '') = ''
            AND NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements(r.parse_errors) e
              WHERE e->>'column_name' = required_column
                AND e->>'code' = 'required_blank'
            )
        )
        OR EXISTS (
          SELECT 1 FROM jsonb_array_elements(r.parse_errors) e
          WHERE e->>'code' = 'required_blank'
            AND COALESCE(r.original_lexical_row->>(e->>'column_name'), '') <> ''
        )
      )
  ) THEN RETURN false; END IF;

  -- Candidate keys are required only where the frozen contract declares one;
  -- their evidence shape and uniqueness are both checked inside the database.
  IF EXISTS (
    SELECT 1 FROM nport_raw_rows r JOIN nport_contract_tables c ON c.source_table = r.source_table
    WHERE r.ingestion_run_id = target_run_id AND cardinality(c.candidate_key) > 0
      AND (r.candidate_key_evidence->'columns' <> to_jsonb(c.candidate_key)
           OR COALESCE((r.candidate_key_evidence->>'complete')::boolean, false) IS NOT TRUE
           OR jsonb_array_length(COALESCE(r.candidate_key_evidence->'values', '[]'::jsonb)) <> cardinality(c.candidate_key))
  ) OR EXISTS (
    SELECT 1 FROM nport_raw_rows r JOIN nport_contract_tables c ON c.source_table = r.source_table
    WHERE r.ingestion_run_id = target_run_id AND cardinality(c.candidate_key) > 0
    GROUP BY r.source_table, r.candidate_key_evidence->'values'
    HAVING count(*) > 1
  ) THEN RETURN false; END IF;

  -- SUBMISSION is the unique root; all declared children must resolve to it.
  IF EXISTS (
    SELECT 1 FROM nport_raw_rows r JOIN nport_contract_tables c ON c.source_table = r.source_table
    LEFT JOIN (
      SELECT accession_number, count(*) AS n FROM nport_raw_rows
      WHERE ingestion_run_id = target_run_id AND source_table = 'SUBMISSION.tsv'
        AND typed_projection->>'ACCESSION_NUMBER' IS NOT NULL
      GROUP BY accession_number
    ) s ON s.accession_number = r.accession_number
    WHERE r.ingestion_run_id = target_run_id AND 'SUBMISSION.tsv' = ANY(c.logical_parents)
      AND (r.accession_number IS NULL OR s.n IS DISTINCT FROM 1)
  ) THEN RETURN false; END IF;

  -- The holding map is the sole parent resolution table and must be an exact,
  -- one-to-one projection of FUND_REPORTED_HOLDING rows.
  IF EXISTS (
    SELECT holding_id FROM nport_raw_rows
    WHERE ingestion_run_id = target_run_id AND source_table = 'FUND_REPORTED_HOLDING.tsv'
      AND holding_id IS NOT NULL
    GROUP BY holding_id HAVING count(DISTINCT accession_number) <> 1
  ) OR EXISTS (
    (SELECT holding_id, accession_number, source_file_id, source_row_number FROM nport_raw_rows
     WHERE ingestion_run_id = target_run_id AND source_table = 'FUND_REPORTED_HOLDING.tsv'
       AND holding_id IS NOT NULL AND accession_number IS NOT NULL
     EXCEPT
     SELECT holding_id, accession_number, source_file_id, source_row_number FROM nport_holding_accession_map
     WHERE ingestion_run_id = target_run_id)
    UNION ALL
    (SELECT holding_id, accession_number, source_file_id, source_row_number FROM nport_holding_accession_map
     WHERE ingestion_run_id = target_run_id
     EXCEPT
     SELECT holding_id, accession_number, source_file_id, source_row_number FROM nport_raw_rows
     WHERE ingestion_run_id = target_run_id AND source_table = 'FUND_REPORTED_HOLDING.tsv')
  ) OR EXISTS (
    SELECT 1 FROM nport_raw_rows r JOIN nport_contract_tables c ON c.source_table = r.source_table
    LEFT JOIN nport_holding_accession_map m ON m.ingestion_run_id = r.ingestion_run_id AND m.holding_id = r.holding_id
    WHERE r.ingestion_run_id = target_run_id AND 'FUND_REPORTED_HOLDING.tsv' = ANY(c.logical_parents)
      AND (r.holding_id IS NULL OR m.holding_id IS NULL OR r.accession_number IS DISTINCT FROM m.accession_number)
  ) THEN RETURN false; END IF;
  RETURN true;
END $$;

DROP TRIGGER IF EXISTS nport_raw_publication_gate ON sec_ingestion_runs;

-- COPY escreve batches de 1.000 linhas: a guarda é por statement, nunca um
-- SELECT/FOR UPDATE por linha física.
CREATE OR REPLACE FUNCTION nport_lock_raw_insert_statement()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM new_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS (SELECT 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM new_rows) AND r.raw_validated_at IS NOT NULL) THEN RAISE EXCEPTION 'N-PORT raw rows immutable'; END IF;
 RETURN NULL; END $$;
CREATE OR REPLACE FUNCTION nport_lock_raw_update_statement()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM old_rows UNION SELECT ingestion_run_id FROM new_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS (SELECT 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM old_rows UNION SELECT ingestion_run_id FROM new_rows) AND r.raw_validated_at IS NOT NULL) THEN RAISE EXCEPTION 'N-PORT raw rows immutable'; END IF;
 RETURN NULL; END $$;
CREATE OR REPLACE FUNCTION nport_lock_raw_delete_statement()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM old_rows) ORDER BY r.run_id FOR UPDATE;
 IF EXISTS (SELECT 1 FROM sec_ingestion_runs r WHERE r.run_id IN (SELECT ingestion_run_id FROM old_rows) AND r.raw_validated_at IS NOT NULL) THEN RAISE EXCEPTION 'N-PORT raw rows immutable'; END IF;
 RETURN NULL; END $$;
DROP TRIGGER IF EXISTS nport_raw_rows_immutable ON nport_raw_rows;
DROP TRIGGER IF EXISTS nport_raw_rows_lock_insert ON nport_raw_rows;
CREATE TRIGGER nport_raw_rows_lock_insert AFTER INSERT ON nport_raw_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION nport_lock_raw_insert_statement();
DROP TRIGGER IF EXISTS nport_raw_rows_lock_update ON nport_raw_rows;
CREATE TRIGGER nport_raw_rows_lock_update AFTER UPDATE ON nport_raw_rows REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION nport_lock_raw_update_statement();
DROP TRIGGER IF EXISTS nport_raw_rows_lock_delete ON nport_raw_rows;
CREATE TRIGGER nport_raw_rows_lock_delete AFTER DELETE ON nport_raw_rows REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION nport_lock_raw_delete_statement();
CREATE OR REPLACE FUNCTION nport_lock_holding_map_insert_statement()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  PERFORM 1 FROM sec_ingestion_runs WHERE run_id IN (SELECT ingestion_run_id FROM new_rows) ORDER BY run_id FOR UPDATE;
  IF EXISTS (SELECT 1 FROM sec_ingestion_runs WHERE run_id IN (SELECT ingestion_run_id FROM new_rows) AND raw_validated_at IS NOT NULL) THEN
    RAISE EXCEPTION 'N-PORT holding map is immutable after raw validation';
  END IF; RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION nport_lock_holding_map_update_statement()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  PERFORM 1 FROM sec_ingestion_runs WHERE run_id IN (SELECT ingestion_run_id FROM old_rows UNION SELECT ingestion_run_id FROM new_rows) ORDER BY run_id FOR UPDATE;
  IF EXISTS (SELECT 1 FROM sec_ingestion_runs WHERE run_id IN (SELECT ingestion_run_id FROM old_rows UNION SELECT ingestion_run_id FROM new_rows) AND raw_validated_at IS NOT NULL) THEN
    RAISE EXCEPTION 'N-PORT holding map is immutable after raw validation';
  END IF; RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION nport_lock_holding_map_delete_statement()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  PERFORM 1 FROM sec_ingestion_runs WHERE run_id IN (SELECT ingestion_run_id FROM old_rows) ORDER BY run_id FOR UPDATE;
  IF EXISTS (SELECT 1 FROM sec_ingestion_runs WHERE run_id IN (SELECT ingestion_run_id FROM old_rows) AND raw_validated_at IS NOT NULL) THEN
    RAISE EXCEPTION 'N-PORT holding map is immutable after raw validation';
  END IF; RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS nport_holding_accession_map_immutable ON nport_holding_accession_map;
DROP TRIGGER IF EXISTS nport_holding_map_lock_insert ON nport_holding_accession_map;
CREATE TRIGGER nport_holding_map_lock_insert AFTER INSERT ON nport_holding_accession_map
REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION nport_lock_holding_map_insert_statement();
DROP TRIGGER IF EXISTS nport_holding_map_lock_update ON nport_holding_accession_map;
CREATE TRIGGER nport_holding_map_lock_update AFTER UPDATE ON nport_holding_accession_map
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION nport_lock_holding_map_update_statement();
DROP TRIGGER IF EXISTS nport_holding_map_lock_delete ON nport_holding_accession_map;
CREATE TRIGGER nport_holding_map_lock_delete AFTER DELETE ON nport_holding_accession_map
REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION nport_lock_holding_map_delete_statement();

-- Superfícies de contrato: cada raw_target guarda a identidade e o payload
-- comum, sem pivotar ou perder coluna lexical. As views só exibem runs validados.
DO $$
DECLARE target text;
BEGIN
    FOREACH target IN ARRAY ARRAY[
        'nport_borrower_raw', 'nport_borrow_aggregate_raw',
        'nport_convertible_security_currency_raw', 'nport_debt_security_raw',
        'nport_debt_security_ref_instrument_raw', 'nport_derivative_counterparty_raw',
        'nport_desc_ref_index_basket_raw', 'nport_desc_ref_index_component_raw',
        'nport_desc_ref_other_raw', 'nport_explanatory_note_raw',
        'nport_floating_rate_reset_tenor_raw', 'nport_fund_reported_holding_raw',
        'nport_fund_reported_info_raw', 'nport_fund_var_info_raw',
        'nport_fut_fwd_nonforeigncur_contract_raw', 'nport_fwd_foreigncur_contract_swap_raw',
        'nport_identifiers_raw', 'nport_interest_rate_risk_raw',
        'nport_monthly_return_cat_instrument_raw', 'nport_monthly_total_return_raw',
        'nport_nonforeign_exchange_swap_raw', 'nport_other_deriv_raw',
        'nport_other_deriv_notional_amount_raw', 'nport_registrant_raw',
        'nport_repurchase_agreement_raw', 'nport_repurchase_collateral_raw',
        'nport_repurchase_counterparty_raw', 'nport_securities_lending_raw',
        'nport_submission_raw', 'nport_swaption_option_warnt_deriv_raw'
    ] LOOP
        EXECUTE format(
            'CREATE OR REPLACE VIEW %I AS
             SELECT r.* FROM nport_raw_rows r
             JOIN sec_validated_raw_runs v ON v.run_id = r.ingestion_run_id
             WHERE r.source_table = %L',
            target, upper(regexp_replace(substr(target, 7, length(target) - 10), '_', '_', 'g')) || '.tsv'
        );
    END LOOP;
END;
$$;
