-- Append-only RenderApi fallback overlay for terminal FormNportApi N-PORT rows.
--
-- This schema never changes v1 evidence.  A v2 row is admissible only when its
-- v1 recovery row is already terminal_error with form_nport_exact_zero.

BEGIN;

CREATE OR REPLACE FUNCTION nport_fixed_income_secapi_fallback_document_id_v2(
    p_publication_id uuid, p_run_id uuid, p_accession_number text
) RETURNS uuid LANGUAGE sql IMMUTABLE STRICT AS $$
    WITH digest AS (
        SELECT md5('nport-secapi-render-fallback-v2|' || p_publication_id::text
                   || '|' || p_run_id::text || '|' || p_accession_number) AS value
    )
    SELECT (substr(value, 1, 8) || '-' || substr(value, 9, 4) || '-5'
            || substr(value, 14, 3) || '-8' || substr(value, 17, 3) || '-'
            || substr(value, 20, 12))::uuid
      FROM digest
$$;

CREATE TABLE IF NOT EXISTS nport_fixed_income_secapi_fallback_manifest_v2 (
    source_holdings_publication_id uuid NOT NULL,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL CHECK (accession_number <> ''),
    source_document_id uuid NOT NULL,
    fallback_reason text NOT NULL DEFAULT 'form_nport_exact_zero'
        CHECK (fallback_reason = 'form_nport_exact_zero'),
    provider text NOT NULL DEFAULT 'sec-api.io' CHECK (provider = 'sec-api.io'),
    api_chain text[] NOT NULL DEFAULT ARRAY['FormNportApi','QueryApi','RenderApi']::text[]
        CHECK (api_chain = ARRAY['FormNportApi','QueryApi','RenderApi']::text[]),
    parser_version text NOT NULL CHECK (parser_version <> ''),
    resolver_version text NOT NULL CHECK (resolver_version <> ''),
    form_type text NOT NULL DEFAULT 'NPORT-P' CHECK (form_type = 'NPORT-P'),
    document_name text NOT NULL DEFAULT 'primary_doc.xml' CHECK (document_name = 'primary_doc.xml'),
    document_url text NOT NULL CHECK (
        document_url ~ ('^https://www[.]sec[.]gov/Archives/edgar/data/[0-9]+/'
            || replace(accession_number, '-', '') || '/primary_doc[.]xml$')
    ),
    form_nport_query text NOT NULL CHECK (
        form_nport_query = 'accessionNo:"' || accession_number || '"'
    ),
    form_nport_result_count integer NOT NULL CHECK (form_nport_result_count = 0),
    form_nport_response_sha256 char(64) NOT NULL CHECK (form_nport_response_sha256 ~ '^[0-9a-f]{64}$'),
    query_response_sha256 char(64) NOT NULL CHECK (query_response_sha256 ~ '^[0-9a-f]{64}$'),
    render_raw_sha256 char(64) NOT NULL CHECK (render_raw_sha256 ~ '^[0-9a-f]{64}$'),
    compact_payload_sha256 char(64) NOT NULL CHECK (compact_payload_sha256 ~ '^[0-9a-f]{64}$'),
    status text NOT NULL DEFAULT 'success' CHECK (status = 'success'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_holdings_publication_id, source_run_id, accession_number),
    FOREIGN KEY (source_holdings_publication_id, source_run_id, accession_number)
        REFERENCES nport_fixed_income_secapi_recovery_v1
        (source_holdings_publication_id, source_run_id, accession_number) ON DELETE RESTRICT
);

-- Upgrade an already-installed, still-empty v2 overlay without rewriting it.
ALTER TABLE nport_fixed_income_secapi_fallback_manifest_v2
    ADD COLUMN IF NOT EXISTS form_nport_query text NOT NULL DEFAULT '';
ALTER TABLE nport_fixed_income_secapi_fallback_manifest_v2
    ALTER COLUMN form_nport_query DROP DEFAULT;
ALTER TABLE nport_fixed_income_secapi_fallback_manifest_v2
    ADD COLUMN IF NOT EXISTS form_nport_result_count integer NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS nport_fixed_income_secapi_fallback_fund_info_v2 (
    source_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_holdings_publication_id uuid NOT NULL,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL CHECK (accession_number <> ''),
    source_document_id uuid NOT NULL,
    parser_version text NOT NULL CHECK (parser_version <> ''),
    resolver_version text NOT NULL CHECK (resolver_version <> ''),
    compact_payload_sha256 char(64) NOT NULL CHECK (compact_payload_sha256 ~ '^[0-9a-f]{64}$'),
    projection_sha256 char(64) NOT NULL CHECK (projection_sha256 ~ '^[0-9a-f]{64}$'),
    compact_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    presence_map jsonb NOT NULL DEFAULT '{}'::jsonb,
    cur_metric_state text NOT NULL CHECK (cur_metric_state IN ('present','empty','missing','null')),
    cur_metric_count integer NOT NULL CHECK (cur_metric_count >= 0),
    total_assets numeric, total_liabilities numeric, net_assets numeric,
    borrowing_pay_within_1yr numeric, ctrld_companies_pay_within_1yr numeric,
    other_affilia_pay_within_1yr numeric, other_pay_within_1yr numeric,
    borrowing_pay_after_1yr numeric, ctrld_companies_pay_after_1yr numeric,
    other_affilia_pay_after_1yr numeric, other_pay_after_1yr numeric,
    delayed_delivery numeric, standby_commitment numeric, cash_not_rptd_in_c_or_d numeric,
    credit_spread_3mon_invest numeric, credit_spread_1yr_invest numeric,
    credit_spread_5yr_invest numeric, credit_spread_10yr_invest numeric,
    credit_spread_30yr_invest numeric, credit_spread_3mon_noninvest numeric,
    credit_spread_1yr_noninvest numeric, credit_spread_5yr_noninvest numeric,
    credit_spread_10yr_noninvest numeric, credit_spread_30yr_noninvest numeric,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_holdings_publication_id, source_run_id, accession_number),
    FOREIGN KEY (source_holdings_publication_id, source_run_id, accession_number)
        REFERENCES nport_fixed_income_secapi_fallback_manifest_v2
        (source_holdings_publication_id, source_run_id, accession_number) ON DELETE RESTRICT,
    CHECK (nport_fixed_income_secapi_compact_json(compact_payload)),
    CHECK (nport_fixed_income_secapi_compact_json(presence_map)),
    CHECK ((cur_metric_state = 'present' AND cur_metric_count > 0)
        OR (cur_metric_state IN ('empty','missing','null') AND cur_metric_count = 0))
);

CREATE TABLE IF NOT EXISTS nport_fixed_income_secapi_fallback_rate_risk_v2 (
    source_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_holdings_publication_id uuid NOT NULL,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL CHECK (accession_number <> ''),
    source_document_id uuid NOT NULL,
    parser_version text NOT NULL CHECK (parser_version <> ''),
    resolver_version text NOT NULL CHECK (resolver_version <> ''),
    provider_ordinal bigint NOT NULL CHECK (provider_ordinal >= 0),
    provider_rate_risk_id text NOT NULL CHECK (provider_rate_risk_id <> ''),
    currency_code text NOT NULL CHECK (currency_code <> ''),
    compact_payload_sha256 char(64) NOT NULL CHECK (compact_payload_sha256 ~ '^[0-9a-f]{64}$'),
    projection_sha256 char(64) NOT NULL CHECK (projection_sha256 ~ '^[0-9a-f]{64}$'),
    compact_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    presence_map jsonb NOT NULL DEFAULT '{}'::jsonb,
    dv01_3mon numeric, dv01_1yr numeric, dv01_5yr numeric, dv01_10yr numeric, dv01_30yr numeric,
    dv100_3mon numeric, dv100_1yr numeric, dv100_5yr numeric, dv100_10yr numeric, dv100_30yr numeric,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_holdings_publication_id, source_run_id, accession_number, provider_ordinal),
    FOREIGN KEY (source_holdings_publication_id, source_run_id, accession_number)
        REFERENCES nport_fixed_income_secapi_fallback_manifest_v2
        (source_holdings_publication_id, source_run_id, accession_number) ON DELETE RESTRICT,
    CHECK (nport_fixed_income_secapi_compact_json(compact_payload)),
    CHECK (nport_fixed_income_secapi_compact_json(presence_map))
);

CREATE OR REPLACE FUNCTION nport_fixed_income_secapi_fallback_write_guard_v2()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v1_status text;
    manifest nport_fixed_income_secapi_fallback_manifest_v2%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'SEC API fallback overlay evidence is immutable';
    END IF;
    IF TG_TABLE_NAME = 'nport_fixed_income_secapi_fallback_manifest_v2' THEN
        SELECT status INTO v1_status
          FROM nport_fixed_income_secapi_recovery_v1
         WHERE (source_holdings_publication_id, source_run_id, accession_number) =
               (NEW.source_holdings_publication_id, NEW.source_run_id, NEW.accession_number);
        IF v1_status IS DISTINCT FROM 'terminal_error' THEN
            RAISE EXCEPTION 'SEC API fallback overlay requires a v1 terminal recovery row';
        END IF;
        IF NEW.source_document_id IS DISTINCT FROM nport_fixed_income_secapi_fallback_document_id_v2(
            NEW.source_holdings_publication_id, NEW.source_run_id, NEW.accession_number
        ) THEN
            RAISE EXCEPTION 'SEC API fallback source document id is not deterministic';
        END IF;
        IF NEW.document_url !~ ('^https://www[.]sec[.]gov/Archives/edgar/data/[0-9]+/'
            || replace(NEW.accession_number, '-', '') || '/primary_doc[.]xml$') THEN
            RAISE EXCEPTION 'SEC API fallback document URL does not match accession';
        END IF;
        IF NEW.form_nport_query IS DISTINCT FROM
           ('accessionNo:"' || NEW.accession_number || '"')
           OR NEW.form_nport_result_count IS DISTINCT FROM 0 THEN
            RAISE EXCEPTION 'SEC API fallback requires immutable FormNport exact-zero evidence';
        END IF;
        RETURN NEW;
    END IF;

    SELECT * INTO manifest
      FROM nport_fixed_income_secapi_fallback_manifest_v2
     WHERE (source_holdings_publication_id, source_run_id, accession_number) =
           (NEW.source_holdings_publication_id, NEW.source_run_id, NEW.accession_number);
    IF manifest.status IS DISTINCT FROM 'success' THEN
        RAISE EXCEPTION 'SEC API fallback projection requires a successful manifest';
    END IF;
    IF (NEW.source_document_id, NEW.parser_version, NEW.resolver_version, NEW.compact_payload_sha256)
       IS DISTINCT FROM
       (manifest.source_document_id, manifest.parser_version, manifest.resolver_version, manifest.compact_payload_sha256) THEN
        RAISE EXCEPTION 'SEC API fallback projection provenance or hash does not match manifest';
    END IF;
    RETURN NEW;
END $$;

DO $$
DECLARE target text;
BEGIN
    FOREACH target IN ARRAY ARRAY[
        'nport_fixed_income_secapi_fallback_manifest_v2',
        'nport_fixed_income_secapi_fallback_fund_info_v2',
        'nport_fixed_income_secapi_fallback_rate_risk_v2'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
             WHERE tgrelid = to_regclass(target)
               AND tgname = 'nport_fixed_income_secapi_fallback_write_guard_v2'
               AND NOT tgisinternal
        ) THEN
            EXECUTE format('CREATE TRIGGER nport_fixed_income_secapi_fallback_write_guard_v2 '
                || 'BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW '
                || 'EXECUTE FUNCTION nport_fixed_income_secapi_fallback_write_guard_v2()', target);
        END IF;
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS nport_fixed_income_secapi_fallback_manifest_scope_idx
    ON nport_fixed_income_secapi_fallback_manifest_v2
    (source_holdings_publication_id, source_run_id, accession_number);

CREATE OR REPLACE FUNCTION nport_fixed_income_secapi_fallback_scope_ready_v2(
    p_publication_id uuid, p_run_id uuid,
    p_v1_extractor_version text DEFAULT 'nport-secapi-fixed-income/v1',
    p_v2_parser_version text DEFAULT 'nport-secapi-fixed-income/v2',
    p_resolver_version text DEFAULT 'secapi-query-render/v1'
) RETURNS jsonb LANGUAGE sql STABLE AS $$
WITH expected AS (
    SELECT DISTINCT accession_number FROM nport_holdings_snapshot_identity_v1
     WHERE publication_id = p_publication_id AND accession_number IS NOT NULL
), v1 AS (
    SELECT * FROM nport_fixed_income_secapi_recovery_v1
     WHERE source_holdings_publication_id = p_publication_id AND source_run_id = p_run_id
), v2 AS (
    SELECT * FROM nport_fixed_income_secapi_fallback_manifest_v2
     WHERE source_holdings_publication_id = p_publication_id AND source_run_id = p_run_id
), selected AS (
    SELECT e.accession_number,
           CASE WHEN r.status = 'success' AND r.extractor_version = p_v1_extractor_version THEN 'v1'
                WHEN r.status = 'terminal_error' AND m.status = 'success'
                 AND m.parser_version = p_v2_parser_version AND m.resolver_version = p_resolver_version THEN 'v2'
           END AS selected_version
      FROM expected e
      LEFT JOIN v1 r USING (accession_number)
      LEFT JOIN v2 m USING (accession_number)
), counts AS (
    SELECT
      (SELECT count(*) FROM expected) AS expected_count,
      (SELECT count(*) FROM selected WHERE selected_version = 'v1') AS selected_v1_count,
      (SELECT count(*) FROM selected WHERE selected_version = 'v2') AS selected_v2_count,
      (SELECT count(*) FROM expected e LEFT JOIN v1 r USING (accession_number) WHERE r.accession_number IS NULL) AS missing_v1_count,
      (SELECT count(*) FROM expected e JOIN v1 r USING (accession_number) WHERE r.status = 'terminal_error' AND NOT EXISTS (
          SELECT 1 FROM v2 m WHERE m.accession_number=e.accession_number AND m.status='success'
            AND m.parser_version=p_v2_parser_version AND m.resolver_version=p_resolver_version
      )) AS missing_fallback_count,
      (SELECT count(*) FROM expected e JOIN v1 r USING (accession_number)
        WHERE r.status NOT IN ('success','terminal_error')) AS nonterminal_v1_count,
      (SELECT count(*) FROM v1 r LEFT JOIN expected e USING (accession_number) WHERE e.accession_number IS NULL) AS unexpected_v1_count,
      (SELECT count(*) FROM v2 m LEFT JOIN expected e USING (accession_number) WHERE e.accession_number IS NULL) AS unexpected_v2_count,
      (SELECT count(*) FROM expected e JOIN v1 r USING (accession_number) WHERE r.status='success'
        AND r.extractor_version IS DISTINCT FROM p_v1_extractor_version)
       + (SELECT count(*) FROM expected e JOIN v2 m USING (accession_number) WHERE m.status='success'
        AND (m.parser_version IS DISTINCT FROM p_v2_parser_version OR m.resolver_version IS DISTINCT FROM p_resolver_version))
        AS active_parameter_mismatch_count,
      (SELECT count(*) FROM selected s LEFT JOIN nport_fixed_income_secapi_fund_info_v1 f
        ON s.selected_version='v1' AND (f.source_holdings_publication_id,f.source_run_id,f.accession_number)=(p_publication_id,p_run_id,s.accession_number)
        LEFT JOIN nport_fixed_income_secapi_fallback_fund_info_v2 g
        ON s.selected_version='v2' AND (g.source_holdings_publication_id,g.source_run_id,g.accession_number)=(p_publication_id,p_run_id,s.accession_number)
        WHERE s.selected_version IS NOT NULL AND f.accession_number IS NULL AND g.accession_number IS NULL) AS missing_fund_count,
      (SELECT COALESCE(sum(CASE WHEN s.selected_version='v1' THEN f.cur_metric_count ELSE g.cur_metric_count END),0)
        FROM selected s LEFT JOIN nport_fixed_income_secapi_fund_info_v1 f
          ON s.selected_version='v1' AND (f.source_holdings_publication_id,f.source_run_id,f.accession_number)=(p_publication_id,p_run_id,s.accession_number)
        LEFT JOIN nport_fixed_income_secapi_fallback_fund_info_v2 g
          ON s.selected_version='v2' AND (g.source_holdings_publication_id,g.source_run_id,g.accession_number)=(p_publication_id,p_run_id,s.accession_number)) AS declared_cur_metric_count,
      (SELECT count(*) FROM nport_fixed_income_secapi_rate_risk_v1 q JOIN selected s USING(accession_number)
        WHERE s.selected_version='v1' AND q.source_holdings_publication_id=p_publication_id AND q.source_run_id=p_run_id)
       + (SELECT count(*) FROM nport_fixed_income_secapi_fallback_rate_risk_v2 q JOIN selected s USING(accession_number)
        WHERE s.selected_version='v2' AND q.source_holdings_publication_id=p_publication_id AND q.source_run_id=p_run_id) AS rate_row_count
)
SELECT jsonb_build_object(
    'ready', expected_count > 0 AND selected_v1_count + selected_v2_count = expected_count
       AND missing_v1_count=0 AND missing_fallback_count=0 AND nonterminal_v1_count=0
       AND unexpected_v1_count=0 AND unexpected_v2_count=0 AND active_parameter_mismatch_count=0
       AND missing_fund_count=0 AND declared_cur_metric_count=rate_row_count,
    'expected_count',expected_count,'selected_v1_count',selected_v1_count,'selected_v2_count',selected_v2_count,
    'missing_v1_count',missing_v1_count,'missing_fallback_count',missing_fallback_count,
    'nonterminal_v1_count',nonterminal_v1_count,'unexpected_v1_count',unexpected_v1_count,
    'unexpected_v2_count',unexpected_v2_count,'active_parameter_mismatch_count',active_parameter_mismatch_count,
    'missing_fund_count',missing_fund_count,'declared_cur_metric_count',declared_cur_metric_count,
    'rate_row_count',rate_row_count
) FROM counts
$$;

CREATE OR REPLACE FUNCTION nport_fixed_income_fund_info_source_v2(
    p_publication_id uuid, p_run_id uuid, p_source_kind text,
    p_v1_extractor_version text DEFAULT 'nport-secapi-fixed-income/v1',
    p_v2_parser_version text DEFAULT 'nport-secapi-fixed-income/v2',
    p_resolver_version text DEFAULT 'secapi-query-render/v1'
) RETURNS TABLE (source_row_id bigint, source_document_id uuid, source_row_number bigint,
                 accession_number text, typed_projection jsonb, source_kind text, payload_sha256 char(64))
LANGUAGE plpgsql STABLE AS $$
DECLARE readiness jsonb;
BEGIN
    IF p_source_kind <> 'sec_api' THEN
        RAISE EXCEPTION 'unknown N-PORT fixed-income source kind: %', p_source_kind;
    END IF;
    readiness := nport_fixed_income_secapi_fallback_scope_ready_v2(p_publication_id,p_run_id,p_v1_extractor_version,p_v2_parser_version,p_resolver_version);
    IF NOT COALESCE((readiness->>'ready')::boolean, false) THEN RAISE EXCEPTION 'SEC API fallback scope is not ready: %', readiness; END IF;
    RETURN QUERY
    SELECT f.source_row_id,f.source_document_id,f.source_row_number,f.accession_number,
           jsonb_strip_nulls(jsonb_build_object(
             'TOTAL_ASSETS',f.total_assets,'TOTAL_LIABILITIES',f.total_liabilities,'NET_ASSETS',f.net_assets,
             'BORROWING_PAY_WITHIN_1YR',f.borrowing_pay_within_1yr,'CTRLD_COMPANIES_PAY_WITHIN_1YR',f.ctrld_companies_pay_within_1yr,
             'OTHER_AFFILIA_PAY_WITHIN_1YR',f.other_affilia_pay_within_1yr,'OTHER_PAY_WITHIN_1YR',f.other_pay_within_1yr,
             'BORROWING_PAY_AFTER_1YR',f.borrowing_pay_after_1yr,'CTRLD_COMPANIES_PAY_AFTER_1YR',f.ctrld_companies_pay_after_1yr,
             'OTHER_AFFILIA_PAY_AFTER_1YR',f.other_affilia_pay_after_1yr,'OTHER_PAY_AFTER_1YR',f.other_pay_after_1yr,
             'DELAYED_DELIVERY',f.delayed_delivery,'STANDBY_COMMITMENT',f.standby_commitment,'CASH_NOT_RPTD_IN_C_OR_D',f.cash_not_rptd_in_c_or_d,
             'CREDIT_SPREAD_3MON_INVEST',f.credit_spread_3mon_invest,'CREDIT_SPREAD_1YR_INVEST',f.credit_spread_1yr_invest,
             'CREDIT_SPREAD_5YR_INVEST',f.credit_spread_5yr_invest,'CREDIT_SPREAD_10YR_INVEST',f.credit_spread_10yr_invest,
             'CREDIT_SPREAD_30YR_INVEST',f.credit_spread_30yr_invest,'CREDIT_SPREAD_3MON_NONINVEST',f.credit_spread_3mon_noninvest,
             'CREDIT_SPREAD_1YR_NONINVEST',f.credit_spread_1yr_noninvest,'CREDIT_SPREAD_5YR_NONINVEST',f.credit_spread_5yr_noninvest,
             'CREDIT_SPREAD_10YR_NONINVEST',f.credit_spread_10yr_noninvest,'CREDIT_SPREAD_30YR_NONINVEST',f.credit_spread_30yr_noninvest)),
           'sec_api'::text,f.payload_sha256
      FROM nport_fixed_income_secapi_fund_info_v1 f JOIN nport_fixed_income_secapi_recovery_v1 r
        ON (r.source_holdings_publication_id,r.source_run_id,r.accession_number)=(f.source_holdings_publication_id,f.source_run_id,f.accession_number)
     WHERE f.source_holdings_publication_id=p_publication_id AND f.source_run_id=p_run_id
       AND r.status='success' AND r.extractor_version=p_v1_extractor_version
    UNION ALL
    SELECT f.source_row_id,f.source_document_id,0::bigint,f.accession_number,
           jsonb_strip_nulls(jsonb_build_object(
             'TOTAL_ASSETS',f.total_assets,'TOTAL_LIABILITIES',f.total_liabilities,'NET_ASSETS',f.net_assets,
             'BORROWING_PAY_WITHIN_1YR',f.borrowing_pay_within_1yr,'CTRLD_COMPANIES_PAY_WITHIN_1YR',f.ctrld_companies_pay_within_1yr,
             'OTHER_AFFILIA_PAY_WITHIN_1YR',f.other_affilia_pay_within_1yr,'OTHER_PAY_WITHIN_1YR',f.other_pay_within_1yr,
             'BORROWING_PAY_AFTER_1YR',f.borrowing_pay_after_1yr,'CTRLD_COMPANIES_PAY_AFTER_1YR',f.ctrld_companies_pay_after_1yr,
             'OTHER_AFFILIA_PAY_AFTER_1YR',f.other_affilia_pay_after_1yr,'OTHER_PAY_AFTER_1YR',f.other_pay_after_1yr,
             'DELAYED_DELIVERY',f.delayed_delivery,'STANDBY_COMMITMENT',f.standby_commitment,'CASH_NOT_RPTD_IN_C_OR_D',f.cash_not_rptd_in_c_or_d,
             'CREDIT_SPREAD_3MON_INVEST',f.credit_spread_3mon_invest,'CREDIT_SPREAD_1YR_INVEST',f.credit_spread_1yr_invest,
             'CREDIT_SPREAD_5YR_INVEST',f.credit_spread_5yr_invest,'CREDIT_SPREAD_10YR_INVEST',f.credit_spread_10yr_invest,
             'CREDIT_SPREAD_30YR_INVEST',f.credit_spread_30yr_invest,'CREDIT_SPREAD_3MON_NONINVEST',f.credit_spread_3mon_noninvest,
             'CREDIT_SPREAD_1YR_NONINVEST',f.credit_spread_1yr_noninvest,'CREDIT_SPREAD_5YR_NONINVEST',f.credit_spread_5yr_noninvest,
             'CREDIT_SPREAD_10YR_NONINVEST',f.credit_spread_10yr_noninvest,'CREDIT_SPREAD_30YR_NONINVEST',f.credit_spread_30yr_noninvest)),
           'sec_api'::text,f.compact_payload_sha256
      FROM nport_fixed_income_secapi_fallback_fund_info_v2 f JOIN nport_fixed_income_secapi_fallback_manifest_v2 m
        ON (m.source_holdings_publication_id,m.source_run_id,m.accession_number)=(f.source_holdings_publication_id,f.source_run_id,f.accession_number)
     WHERE f.source_holdings_publication_id=p_publication_id AND f.source_run_id=p_run_id
       AND m.parser_version=p_v2_parser_version AND m.resolver_version=p_resolver_version;
END $$;

CREATE OR REPLACE FUNCTION nport_fixed_income_rate_risk_source_v2(
    p_publication_id uuid, p_run_id uuid, p_source_kind text,
    p_v1_extractor_version text DEFAULT 'nport-secapi-fixed-income/v1',
    p_v2_parser_version text DEFAULT 'nport-secapi-fixed-income/v2',
    p_resolver_version text DEFAULT 'secapi-query-render/v1'
) RETURNS TABLE (source_row_id bigint, source_document_id uuid, source_row_number bigint,
                 accession_number text, typed_projection jsonb, source_kind text, payload_sha256 char(64))
LANGUAGE plpgsql STABLE AS $$
DECLARE readiness jsonb;
BEGIN
    IF p_source_kind <> 'sec_api' THEN RAISE EXCEPTION 'unknown N-PORT fixed-income source kind: %', p_source_kind; END IF;
    readiness := nport_fixed_income_secapi_fallback_scope_ready_v2(p_publication_id,p_run_id,p_v1_extractor_version,p_v2_parser_version,p_resolver_version);
    IF NOT COALESCE((readiness->>'ready')::boolean, false) THEN RAISE EXCEPTION 'SEC API fallback scope is not ready: %', readiness; END IF;
    RETURN QUERY
    SELECT q.source_row_id,q.source_document_id,q.source_row_number,q.accession_number,
           jsonb_strip_nulls(jsonb_build_object('INTEREST_RATE_RISK_ID',q.provider_rate_risk_id,'CURRENCY_CODE',q.currency_code,
             'INTRST_RATE_CHANGE_3MON_DV01',q.dv01_3mon,'INTRST_RATE_CHANGE_1YR_DV01',q.dv01_1yr,
             'INTRST_RATE_CHANGE_5YR_DV01',q.dv01_5yr,'INTRST_RATE_CHANGE_10YR_DV01',q.dv01_10yr,
             'INTRST_RATE_CHANGE_30YR_DV01',q.dv01_30yr,'INTRST_RATE_CHANGE_3MON_DV100',q.dv100_3mon,
             'INTRST_RATE_CHANGE_1YR_DV100',q.dv100_1yr,'INTRST_RATE_CHANGE_5YR_DV100',q.dv100_5yr,
             'INTRST_RATE_CHANGE_10YR_DV100',q.dv100_10yr,'INTRST_RATE_CHANGE_30YR_DV100',q.dv100_30yr)),
           'sec_api'::text,q.payload_sha256
      FROM nport_fixed_income_secapi_rate_risk_v1 q JOIN nport_fixed_income_secapi_recovery_v1 r
        ON (r.source_holdings_publication_id,r.source_run_id,r.accession_number)=(q.source_holdings_publication_id,q.source_run_id,q.accession_number)
     WHERE q.source_holdings_publication_id=p_publication_id AND q.source_run_id=p_run_id AND r.status='success' AND r.extractor_version=p_v1_extractor_version
    UNION ALL
    SELECT q.source_row_id,q.source_document_id,(q.provider_ordinal + 1)::bigint,q.accession_number,
           jsonb_strip_nulls(jsonb_build_object('INTEREST_RATE_RISK_ID',q.provider_rate_risk_id,'CURRENCY_CODE',q.currency_code,
             'INTRST_RATE_CHANGE_3MON_DV01',q.dv01_3mon,'INTRST_RATE_CHANGE_1YR_DV01',q.dv01_1yr,
             'INTRST_RATE_CHANGE_5YR_DV01',q.dv01_5yr,'INTRST_RATE_CHANGE_10YR_DV01',q.dv01_10yr,
             'INTRST_RATE_CHANGE_30YR_DV01',q.dv01_30yr,'INTRST_RATE_CHANGE_3MON_DV100',q.dv100_3mon,
             'INTRST_RATE_CHANGE_1YR_DV100',q.dv100_1yr,'INTRST_RATE_CHANGE_5YR_DV100',q.dv100_5yr,
             'INTRST_RATE_CHANGE_10YR_DV100',q.dv100_10yr,'INTRST_RATE_CHANGE_30YR_DV100',q.dv100_30yr)),
           'sec_api'::text,q.compact_payload_sha256
      FROM nport_fixed_income_secapi_fallback_rate_risk_v2 q JOIN nport_fixed_income_secapi_fallback_manifest_v2 m
        ON (m.source_holdings_publication_id,m.source_run_id,m.accession_number)=(q.source_holdings_publication_id,q.source_run_id,q.accession_number)
     WHERE q.source_holdings_publication_id=p_publication_id AND q.source_run_id=p_run_id AND m.parser_version=p_v2_parser_version AND m.resolver_version=p_resolver_version;
END $$;

CREATE TABLE IF NOT EXISTS nport_fixed_income_secapi_fallback_schema_versions (
    schema_name text PRIMARY KEY,
    schema_version text NOT NULL CHECK (schema_version = 'v2'),
    installed_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO nport_fixed_income_secapi_fallback_schema_versions(schema_name, schema_version)
VALUES ('nport_fixed_income_secapi_fallback', 'v2') ON CONFLICT (schema_name) DO NOTHING;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM nport_fixed_income_secapi_fallback_schema_versions
                   WHERE schema_name='nport_fixed_income_secapi_fallback' AND schema_version='v2') THEN
        RAISE EXCEPTION 'unexpected SEC API fallback schema version sentinel';
    END IF;
END $$;

COMMIT;
