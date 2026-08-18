-- Native, compact SEC API evidence for N-PORT fixed-income fields.
--
-- This is deliberately a sidecar: expected accession membership comes only
-- from nport_holdings_snapshot_identity_v1, never from holdings or raw rows.

BEGIN;

CREATE OR REPLACE FUNCTION nport_fixed_income_secapi_compact_json(value jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
    SELECT value IS NOT NULL
       AND jsonb_typeof(value) = 'object'
       AND NOT jsonb_path_exists(value, '$.**.invstOrSecs')
       AND NOT jsonb_path_exists(value, '$.**.invstOrSec')
       AND NOT jsonb_path_exists(value, '$.**.investments')
       AND NOT jsonb_path_exists(value, '$.**.holdings')
       AND position('<EDGARSUBMISSION' IN upper(value::text)) = 0
       AND position('<!DOCTYPE' IN upper(value::text)) = 0
       AND position('<!ENTITY' IN upper(value::text)) = 0
$$;

CREATE TABLE IF NOT EXISTS nport_fixed_income_secapi_recovery_v1 (
    source_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_holdings_publication_id uuid NOT NULL,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL CHECK (accession_number <> ''),
    source_document_id uuid NOT NULL,
    source_row_number bigint NOT NULL CHECK (source_row_number >= 0),
    provider text NOT NULL DEFAULT 'sec-api.io' CHECK (provider = 'sec-api.io'),
    api text NOT NULL DEFAULT 'FormNportApi' CHECK (api = 'FormNportApi'),
    source_format text NOT NULL DEFAULT 'sec_api_form_nport_json'
        CHECK (source_format = 'sec_api_form_nport_json'),
    extractor_version text NOT NULL CHECK (extractor_version <> ''),
    status text NOT NULL CHECK (status IN ('pending','success','retryable_error','terminal_error','conflict')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    provider_http_status integer,
    payload_sha256 char(64) CHECK (payload_sha256 IS NULL OR payload_sha256 ~ '^[0-9a-f]{64}$'),
    provider_response_sha256 char(64) CHECK (provider_response_sha256 IS NULL OR provider_response_sha256 ~ '^[0-9a-f]{64}$'),
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_holdings_publication_id, source_run_id, accession_number),
    CHECK (jsonb_typeof(provider_metadata) = 'object'),
    CONSTRAINT nport_fi_secapi_success_hashes_check CHECK (
        status <> 'success' OR (payload_sha256 IS NOT NULL AND provider_response_sha256 IS NOT NULL)
    )
);
ALTER TABLE nport_fixed_income_secapi_recovery_v1
    ALTER COLUMN payload_sha256 DROP NOT NULL,
    ALTER COLUMN provider_response_sha256 DROP NOT NULL;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='nport_fixed_income_secapi_recovery_v1'::regclass
          AND conname='nport_fi_secapi_success_hashes_check'
    ) THEN
        ALTER TABLE nport_fixed_income_secapi_recovery_v1
        ADD CONSTRAINT nport_fi_secapi_success_hashes_check CHECK (
            status <> 'success' OR (payload_sha256 IS NOT NULL AND provider_response_sha256 IS NOT NULL)
        );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS nport_fixed_income_secapi_fund_info_v1 (
    source_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_holdings_publication_id uuid NOT NULL,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL CHECK (accession_number <> ''),
    source_document_id uuid NOT NULL,
    source_row_number bigint NOT NULL CHECK (source_row_number >= 0),
    provider text NOT NULL DEFAULT 'sec-api.io' CHECK (provider = 'sec-api.io'),
    api text NOT NULL DEFAULT 'FormNportApi' CHECK (api = 'FormNportApi'),
    source_format text NOT NULL DEFAULT 'sec_api_form_nport_json'
        CHECK (source_format = 'sec_api_form_nport_json'),
    extractor_version text NOT NULL DEFAULT 'secapi-sidecar-v1' CHECK (extractor_version <> ''),
    payload_sha256 char(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    projection_sha256 char(64) NOT NULL CHECK (projection_sha256 ~ '^[0-9a-f]{64}$'),
    compact_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    presence_map jsonb NOT NULL DEFAULT '{}'::jsonb,
    cur_metric_state text NOT NULL CHECK (cur_metric_state IN ('present','empty','missing','null')),
    cur_metric_count integer NOT NULL CHECK (cur_metric_count >= 0),
    total_assets numeric,
    total_liabilities numeric,
    net_assets numeric,
    borrowing_pay_within_1yr numeric,
    ctrld_companies_pay_within_1yr numeric,
    other_affilia_pay_within_1yr numeric,
    other_pay_within_1yr numeric,
    borrowing_pay_after_1yr numeric,
    ctrld_companies_pay_after_1yr numeric,
    other_affilia_pay_after_1yr numeric,
    other_pay_after_1yr numeric,
    delayed_delivery numeric,
    standby_commitment numeric,
    cash_not_rptd_in_c_or_d numeric,
    credit_spread_3mon_invest numeric,
    credit_spread_1yr_invest numeric,
    credit_spread_5yr_invest numeric,
    credit_spread_10yr_invest numeric,
    credit_spread_30yr_invest numeric,
    credit_spread_3mon_noninvest numeric,
    credit_spread_1yr_noninvest numeric,
    credit_spread_5yr_noninvest numeric,
    credit_spread_10yr_noninvest numeric,
    credit_spread_30yr_noninvest numeric,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_holdings_publication_id, source_run_id, accession_number),
    FOREIGN KEY (source_holdings_publication_id, source_run_id, accession_number)
        REFERENCES nport_fixed_income_secapi_recovery_v1
        (source_holdings_publication_id, source_run_id, accession_number) ON DELETE RESTRICT,
    CHECK (nport_fixed_income_secapi_compact_json(compact_payload)),
    CHECK (nport_fixed_income_secapi_compact_json(presence_map)),
    CHECK ((cur_metric_state = 'present' AND cur_metric_count > 0)
        OR (cur_metric_state IN ('empty','missing','null') AND cur_metric_count = 0))
);

CREATE TABLE IF NOT EXISTS nport_fixed_income_secapi_rate_risk_v1 (
    source_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_holdings_publication_id uuid NOT NULL,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL CHECK (accession_number <> ''),
    source_document_id uuid NOT NULL,
    source_row_number bigint NOT NULL CHECK (source_row_number >= 0),
    provider_ordinal bigint NOT NULL CHECK (provider_ordinal >= 0),
    provider_rate_risk_id text NOT NULL CHECK (provider_rate_risk_id <> ''),
    provider text NOT NULL DEFAULT 'sec-api.io' CHECK (provider = 'sec-api.io'),
    api text NOT NULL DEFAULT 'FormNportApi' CHECK (api = 'FormNportApi'),
    source_format text NOT NULL DEFAULT 'sec_api_form_nport_json'
        CHECK (source_format = 'sec_api_form_nport_json'),
    extractor_version text NOT NULL DEFAULT 'secapi-sidecar-v1' CHECK (extractor_version <> ''),
    currency_code text NOT NULL CHECK (currency_code <> ''),
    payload_sha256 char(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    projection_sha256 char(64) NOT NULL CHECK (projection_sha256 ~ '^[0-9a-f]{64}$'),
    compact_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    presence_map jsonb NOT NULL DEFAULT '{}'::jsonb,
    dv01_3mon numeric, dv01_1yr numeric, dv01_5yr numeric, dv01_10yr numeric, dv01_30yr numeric,
    dv100_3mon numeric, dv100_1yr numeric, dv100_5yr numeric, dv100_10yr numeric, dv100_30yr numeric,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_holdings_publication_id, source_run_id, accession_number, provider_ordinal),
    FOREIGN KEY (source_holdings_publication_id, source_run_id, accession_number)
        REFERENCES nport_fixed_income_secapi_recovery_v1
        (source_holdings_publication_id, source_run_id, accession_number) ON DELETE RESTRICT,
    CHECK (nport_fixed_income_secapi_compact_json(compact_payload)),
    CHECK (nport_fixed_income_secapi_compact_json(presence_map))
);

CREATE INDEX IF NOT EXISTS nport_fixed_income_secapi_recovery_scope_status_idx
    ON nport_fixed_income_secapi_recovery_v1
    (source_holdings_publication_id, source_run_id, status, accession_number);
CREATE INDEX IF NOT EXISTS nport_fixed_income_secapi_fund_scope_accession_idx
    ON nport_fixed_income_secapi_fund_info_v1
    (source_holdings_publication_id, source_run_id, accession_number);
CREATE INDEX IF NOT EXISTS nport_fixed_income_secapi_rate_scope_accession_idx
    ON nport_fixed_income_secapi_rate_risk_v1
    (source_holdings_publication_id, source_run_id, accession_number, provider_ordinal);

CREATE OR REPLACE FUNCTION nport_fixed_income_secapi_sidecar_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    recovery_status text;
    recovery_document_id uuid;
    recovery_extractor_version text;
    recovery_payload_sha256 char(64);
BEGIN
    IF TG_TABLE_NAME = 'nport_fixed_income_secapi_recovery_v1' THEN
        IF TG_OP = 'DELETE' THEN
            IF OLD.status IN ('success','terminal_error','conflict') THEN
                RAISE EXCEPTION 'terminal SEC API recovery evidence is immutable';
            END IF;
            RETURN OLD;
        ELSIF TG_OP = 'UPDATE' THEN
            IF (NEW.source_holdings_publication_id, NEW.source_run_id, NEW.accession_number,
                NEW.source_document_id, NEW.source_row_number, NEW.provider, NEW.api,
                NEW.source_format, NEW.extractor_version, NEW.created_at)
               IS DISTINCT FROM
               (OLD.source_holdings_publication_id, OLD.source_run_id, OLD.accession_number,
                OLD.source_document_id, OLD.source_row_number, OLD.provider, OLD.api,
                OLD.source_format, OLD.extractor_version, OLD.created_at) THEN
                RAISE EXCEPTION 'SEC API recovery provenance is immutable';
            END IF;
            IF OLD.status IN ('success','terminal_error','conflict') THEN
                RAISE EXCEPTION 'terminal SEC API recovery evidence is immutable';
            END IF;
            IF NEW.attempt_count <= OLD.attempt_count THEN
                RAISE EXCEPTION 'SEC API recovery updates must advance attempt_count';
            END IF;
            IF NEW.status NOT IN ('success','retryable_error','terminal_error','conflict') THEN
                RAISE EXCEPTION 'invalid SEC API recovery transition from % to %', OLD.status, NEW.status;
            END IF;
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'SEC API sidecar evidence is immutable';
    END IF;
    SELECT status, source_document_id, extractor_version, payload_sha256
      INTO recovery_status, recovery_document_id, recovery_extractor_version,
           recovery_payload_sha256
      FROM nport_fixed_income_secapi_recovery_v1
     WHERE source_holdings_publication_id = NEW.source_holdings_publication_id
       AND source_run_id = NEW.source_run_id
       AND accession_number = NEW.accession_number;
    IF recovery_status IS DISTINCT FROM 'success' THEN
        RAISE EXCEPTION 'SEC API sidecar evidence requires a successful recovery manifest';
    END IF;
    IF (NEW.source_document_id, NEW.extractor_version, NEW.payload_sha256)
       IS DISTINCT FROM
       (recovery_document_id, recovery_extractor_version, recovery_payload_sha256) THEN
        RAISE EXCEPTION 'SEC API sidecar provenance does not match its recovery manifest';
    END IF;
    RETURN NEW;
END $$;

DO $$
DECLARE target text;
BEGIN
    FOREACH target IN ARRAY ARRAY[
        'nport_fixed_income_secapi_recovery_v1',
        'nport_fixed_income_secapi_fund_info_v1',
        'nport_fixed_income_secapi_rate_risk_v1'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
             WHERE tgrelid = to_regclass(target)
               AND tgname = 'nport_fixed_income_secapi_sidecar_write_guard'
               AND NOT tgisinternal
        ) THEN
            EXECUTE format('CREATE TRIGGER nport_fixed_income_secapi_sidecar_write_guard '
                || 'BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW '
                || 'EXECUTE FUNCTION nport_fixed_income_secapi_sidecar_write_guard()', target);
        END IF;
    END LOOP;
END $$;

DROP FUNCTION IF EXISTS nport_fixed_income_secapi_scope_ready(uuid, uuid);
CREATE OR REPLACE FUNCTION nport_fixed_income_secapi_scope_ready(
    p_publication_id uuid, p_run_id uuid,
    p_extractor_version text DEFAULT 'nport-secapi-fixed-income/v1'
) RETURNS jsonb LANGUAGE sql STABLE AS $$
WITH expected AS (
    SELECT DISTINCT accession_number
      FROM nport_holdings_snapshot_identity_v1
     WHERE publication_id = p_publication_id
       AND accession_number IS NOT NULL
), recovery AS (
    SELECT accession_number, status, source_document_id, extractor_version, payload_sha256
      FROM nport_fixed_income_secapi_recovery_v1
     WHERE source_holdings_publication_id = p_publication_id
       AND source_run_id = p_run_id
), fund AS (
    SELECT accession_number, cur_metric_count, cur_metric_state,
           source_document_id, extractor_version, payload_sha256
      FROM nport_fixed_income_secapi_fund_info_v1
     WHERE source_holdings_publication_id = p_publication_id
       AND source_run_id = p_run_id
), rates AS (
    SELECT accession_number, source_document_id, extractor_version, payload_sha256
      FROM nport_fixed_income_secapi_rate_risk_v1
     WHERE source_holdings_publication_id = p_publication_id
       AND source_run_id = p_run_id
), counts AS (
    SELECT
      (SELECT count(*) FROM expected) AS expected_count,
      (SELECT count(*) FROM recovery r JOIN expected e USING (accession_number)
        WHERE r.status = 'success') AS successful_recovery_count,
      (SELECT count(*) FROM expected e LEFT JOIN recovery r USING (accession_number)
        WHERE r.status IS DISTINCT FROM 'success') AS missing_success_count,
      (SELECT count(*) FROM recovery r LEFT JOIN expected e USING (accession_number)
        WHERE e.accession_number IS NULL) AS unexpected_recovery_count,
      (SELECT count(*) FROM recovery WHERE status <> 'success') AS non_success_recovery_count,
      (SELECT count(*) FROM expected e JOIN recovery r USING (accession_number)
        WHERE r.extractor_version IS DISTINCT FROM p_extractor_version) AS extractor_mismatch_count,
      (SELECT count(*) FROM expected e JOIN fund f USING (accession_number)) AS fund_count,
      (SELECT count(*) FROM expected e LEFT JOIN fund f USING (accession_number)
        WHERE f.accession_number IS NULL) AS missing_fund_count,
      (SELECT COALESCE(sum(f.cur_metric_count), 0) FROM expected e JOIN fund f USING (accession_number))
        AS declared_cur_metric_count,
      (SELECT count(*) FROM rates r JOIN expected e USING (accession_number)) AS rate_row_count,
      (SELECT count(*) FROM rates r LEFT JOIN expected e USING (accession_number)
        WHERE e.accession_number IS NULL) AS unexpected_rate_count,
      (SELECT count(*) FROM expected e JOIN fund f USING (accession_number)
        WHERE f.cur_metric_state IN ('empty','missing','null') AND f.cur_metric_count <> 0) AS invalid_zero_rate_declaration_count,
      (SELECT count(*) FROM (
          SELECT f.accession_number
            FROM fund f JOIN recovery r USING (accession_number)
           WHERE r.status IS DISTINCT FROM 'success'
              OR (f.source_document_id, f.extractor_version, f.payload_sha256)
                 IS DISTINCT FROM
                 (r.source_document_id, r.extractor_version, r.payload_sha256)
          UNION ALL
          SELECT q.accession_number
            FROM rates q JOIN recovery r USING (accession_number)
           WHERE r.status IS DISTINCT FROM 'success'
              OR (q.source_document_id, q.extractor_version, q.payload_sha256)
                 IS DISTINCT FROM
                 (r.source_document_id, r.extractor_version, r.payload_sha256)
      ) mismatches) AS provenance_mismatch_count
)
SELECT jsonb_build_object(
    'ready', expected_count > 0
        AND missing_success_count = 0 AND unexpected_recovery_count = 0
        AND non_success_recovery_count = 0 AND missing_fund_count = 0
        AND extractor_mismatch_count = 0
        AND unexpected_rate_count = 0
        AND declared_cur_metric_count = rate_row_count
        AND invalid_zero_rate_declaration_count = 0
        AND provenance_mismatch_count = 0,
    'expected_count', expected_count,
    'successful_recovery_count', successful_recovery_count,
    'missing_success_count', missing_success_count,
    'unexpected_recovery_count', unexpected_recovery_count,
    'non_success_recovery_count', non_success_recovery_count,
    'extractor_mismatch_count', extractor_mismatch_count,
    'fund_count', fund_count,
    'missing_fund_count', missing_fund_count,
    'declared_cur_metric_count', declared_cur_metric_count,
    'rate_row_count', rate_row_count,
    'unexpected_rate_count', unexpected_rate_count,
    'invalid_zero_rate_declaration_count', invalid_zero_rate_declaration_count,
    'provenance_mismatch_count', provenance_mismatch_count
)
FROM counts
$$;

CREATE OR REPLACE FUNCTION nport_fixed_income_fund_info_source_v1(
    p_publication_id uuid, p_run_id uuid, p_source_kind text
) RETURNS TABLE (
    source_row_id bigint, source_document_id uuid, source_row_number bigint,
    accession_number text, typed_projection jsonb, source_kind text, payload_sha256 char(64)
) LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF p_source_kind = 'sec_api' THEN
        RETURN QUERY
        SELECT f.source_row_id, f.source_document_id, f.source_row_number, f.accession_number,
               jsonb_strip_nulls(jsonb_build_object(
                   'TOTAL_ASSETS', f.total_assets, 'TOTAL_LIABILITIES', f.total_liabilities,
                   'NET_ASSETS', f.net_assets,
                   'BORROWING_PAY_WITHIN_1YR', f.borrowing_pay_within_1yr,
                   'CTRLD_COMPANIES_PAY_WITHIN_1YR', f.ctrld_companies_pay_within_1yr,
                   'OTHER_AFFILIA_PAY_WITHIN_1YR', f.other_affilia_pay_within_1yr,
                   'OTHER_PAY_WITHIN_1YR', f.other_pay_within_1yr,
                   'BORROWING_PAY_AFTER_1YR', f.borrowing_pay_after_1yr,
                   'CTRLD_COMPANIES_PAY_AFTER_1YR', f.ctrld_companies_pay_after_1yr,
                   'OTHER_AFFILIA_PAY_AFTER_1YR', f.other_affilia_pay_after_1yr,
                   'OTHER_PAY_AFTER_1YR', f.other_pay_after_1yr,
                   'DELAYED_DELIVERY', f.delayed_delivery, 'STANDBY_COMMITMENT', f.standby_commitment,
                   'CASH_NOT_RPTD_IN_C_OR_D', f.cash_not_rptd_in_c_or_d,
                   'CREDIT_SPREAD_3MON_INVEST', f.credit_spread_3mon_invest,
                   'CREDIT_SPREAD_1YR_INVEST', f.credit_spread_1yr_invest,
                   'CREDIT_SPREAD_5YR_INVEST', f.credit_spread_5yr_invest,
                   'CREDIT_SPREAD_10YR_INVEST', f.credit_spread_10yr_invest,
                   'CREDIT_SPREAD_30YR_INVEST', f.credit_spread_30yr_invest,
                   'CREDIT_SPREAD_3MON_NONINVEST', f.credit_spread_3mon_noninvest,
                   'CREDIT_SPREAD_1YR_NONINVEST', f.credit_spread_1yr_noninvest,
                   'CREDIT_SPREAD_5YR_NONINVEST', f.credit_spread_5yr_noninvest,
                   'CREDIT_SPREAD_10YR_NONINVEST', f.credit_spread_10yr_noninvest,
                   'CREDIT_SPREAD_30YR_NONINVEST', f.credit_spread_30yr_noninvest
               )), 'sec_api'::text, f.payload_sha256
          FROM nport_fixed_income_secapi_fund_info_v1 f
          JOIN nport_fixed_income_secapi_recovery_v1 r
            ON (r.source_holdings_publication_id, r.source_run_id, r.accession_number) =
               (f.source_holdings_publication_id, f.source_run_id, f.accession_number)
         WHERE f.source_holdings_publication_id = p_publication_id
           AND f.source_run_id = p_run_id AND r.status = 'success';
    ELSIF p_source_kind = 'dera_raw' THEN
        IF to_regclass('nport_fund_reported_info_raw') IS NULL THEN RETURN; END IF;
        RETURN QUERY EXECUTE
            'SELECT raw_row_id, source_file_id, source_row_number, accession_number, typed_projection, ''dera_raw'', source_sha256 FROM nport_fund_reported_info_raw WHERE ingestion_run_id = $1'
            USING p_run_id;
    ELSE
        RAISE EXCEPTION 'unknown N-PORT fixed-income source kind: %', p_source_kind;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION nport_fixed_income_rate_risk_source_v1(
    p_publication_id uuid, p_run_id uuid, p_source_kind text
) RETURNS TABLE (
    source_row_id bigint, source_document_id uuid, source_row_number bigint,
    accession_number text, typed_projection jsonb, source_kind text, payload_sha256 char(64)
) LANGUAGE plpgsql STABLE AS $$
BEGIN
    IF p_source_kind = 'sec_api' THEN
        RETURN QUERY
        SELECT r.source_row_id, r.source_document_id, r.source_row_number, r.accession_number,
               jsonb_strip_nulls(jsonb_build_object(
                   'INTEREST_RATE_RISK_ID', r.provider_rate_risk_id, 'CURRENCY_CODE', r.currency_code,
                   'INTRST_RATE_CHANGE_3MON_DV01', r.dv01_3mon,
                   'INTRST_RATE_CHANGE_1YR_DV01', r.dv01_1yr,
                   'INTRST_RATE_CHANGE_5YR_DV01', r.dv01_5yr,
                   'INTRST_RATE_CHANGE_10YR_DV01', r.dv01_10yr,
                   'INTRST_RATE_CHANGE_30YR_DV01', r.dv01_30yr,
                   'INTRST_RATE_CHANGE_3MON_DV100', r.dv100_3mon,
                   'INTRST_RATE_CHANGE_1YR_DV100', r.dv100_1yr,
                   'INTRST_RATE_CHANGE_5YR_DV100', r.dv100_5yr,
                   'INTRST_RATE_CHANGE_10YR_DV100', r.dv100_10yr,
                   'INTRST_RATE_CHANGE_30YR_DV100', r.dv100_30yr
               )), 'sec_api'::text, r.payload_sha256
          FROM nport_fixed_income_secapi_rate_risk_v1 r
          JOIN nport_fixed_income_secapi_recovery_v1 m
            ON (m.source_holdings_publication_id, m.source_run_id, m.accession_number) =
               (r.source_holdings_publication_id, r.source_run_id, r.accession_number)
         WHERE r.source_holdings_publication_id = p_publication_id
           AND r.source_run_id = p_run_id AND m.status = 'success';
    ELSIF p_source_kind = 'dera_raw' THEN
        IF to_regclass('nport_interest_rate_risk_raw') IS NULL THEN RETURN; END IF;
        RETURN QUERY EXECUTE
            'SELECT raw_row_id, source_file_id, source_row_number, accession_number, typed_projection, ''dera_raw'', source_sha256 FROM nport_interest_rate_risk_raw WHERE ingestion_run_id = $1'
            USING p_run_id;
    ELSE
        RAISE EXCEPTION 'unknown N-PORT fixed-income source kind: %', p_source_kind;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS nport_fixed_income_secapi_schema_versions (
    schema_name text PRIMARY KEY,
    schema_version text NOT NULL,
    installed_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO nport_fixed_income_secapi_schema_versions(schema_name, schema_version)
VALUES ('nport_fixed_income_secapi_sidecars', 'v2')
ON CONFLICT (schema_name) DO UPDATE
SET schema_version=EXCLUDED.schema_version, installed_at=now();

COMMIT;
