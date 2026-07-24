-- Immutable N-PORT fixed-income snapshot.  The only input is the current,
-- publication-versioned V2 holdings surface.  `ASSET_CAT='DBT'` is preserved
-- as the source literal; debt detail comes only from the official
-- `DEBT_SECURITY` typed-projection evidence.

CREATE TABLE IF NOT EXISTS nport_fixed_income_features (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    methodology_version text NOT NULL DEFAULT 'nport_fixed_income_features_v1',
    series_id text NOT NULL,
    report_date date NOT NULL,
    measured_at date NOT NULL,
    status text NOT NULL CHECK (status IN ('certified','degraded','insufficient','unavailable')),
    reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    position_count integer NOT NULL CHECK (position_count >= 0),
    debt_position_count integer NOT NULL CHECK (debt_position_count >= 0),
    debt_extension_position_count integer NOT NULL CHECK (debt_extension_position_count >= 0),
    unknown_debt_market_value_position_count integer NOT NULL DEFAULT 0 CHECK (unknown_debt_market_value_position_count >= 0),
    debt_signed_market_value numeric,
    debt_gross_market_value numeric,
    debt_nav_signed_pct numeric,
    debt_nav_gross_pct numeric,
    debt_market_value_coverage numeric CHECK (debt_market_value_coverage IS NULL OR debt_market_value_coverage BETWEEN 0 AND 1),
    debt_extension_coverage numeric CHECK (debt_extension_coverage IS NULL OR debt_extension_coverage BETWEEN 0 AND 1),
    coupon_weighted_average numeric,
    coupon_market_value_coverage numeric CHECK (coupon_market_value_coverage IS NULL OR coupon_market_value_coverage BETWEEN 0 AND 1),
    coupon_type_mix jsonb,
    maturity_market_value_coverage numeric CHECK (maturity_market_value_coverage IS NULL OR maturity_market_value_coverage BETWEEN 0 AND 1),
    maturity_ladder jsonb,
    maturity_weighted_mean_years numeric,
    maturity_weighted_p25_years numeric,
    maturity_weighted_median_years numeric,
    maturity_weighted_p75_years numeric,
    maturity_statistics_market_value_coverage numeric CHECK (maturity_statistics_market_value_coverage IS NULL OR maturity_statistics_market_value_coverage BETWEEN 0 AND 1),
    identifier_market_value_coverage numeric CHECK (identifier_market_value_coverage IS NULL OR identifier_market_value_coverage BETWEEN 0 AND 1),
    report_age_days integer NOT NULL CHECK (report_age_days >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, series_id, report_date),
    CHECK (methodology_version = 'nport_fixed_income_features_v1'),
    CHECK (jsonb_typeof(reason_codes) = 'array'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object')
);

ALTER TABLE nport_fixed_income_features
    ADD COLUMN IF NOT EXISTS unknown_debt_market_value_position_count integer NOT NULL DEFAULT 0
    CHECK (unknown_debt_market_value_position_count >= 0);

ALTER TABLE nport_fixed_income_features
    ADD COLUMN IF NOT EXISTS maturity_weighted_mean_years numeric,
    ADD COLUMN IF NOT EXISTS maturity_weighted_p25_years numeric,
    ADD COLUMN IF NOT EXISTS maturity_weighted_median_years numeric,
    ADD COLUMN IF NOT EXISTS maturity_weighted_p75_years numeric,
    ADD COLUMN IF NOT EXISTS maturity_statistics_market_value_coverage numeric
    CHECK (maturity_statistics_market_value_coverage IS NULL OR maturity_statistics_market_value_coverage BETWEEN 0 AND 1);

-- One immutable build identity per target publication prevents an idempotent
-- rerun from silently mixing source snapshots or measurement dates.
CREATE TABLE IF NOT EXISTS nport_fixed_income_feature_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    as_of_date date NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS nport_fixed_income_features_source_idx
    ON nport_fixed_income_features (source_holdings_publication_id, series_id, report_date DESC);

-- Governed N-PORT facts beyond the holding projection.  Each relation is
-- publication-versioned and carries the pinned raw-run/accession identity.
CREATE TABLE IF NOT EXISTS nport_fixed_income_key_rate_sensitivities_v2 (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    series_id text NOT NULL, report_date date NOT NULL, accession_number text NOT NULL,
    source_raw_row_id bigint NOT NULL, source_file_id uuid NOT NULL, source_row_number bigint NOT NULL,
    interest_rate_risk_id text NOT NULL, currency_code text, tenor text NOT NULL CHECK (tenor IN ('3mon','1yr','5yr','10yr','30yr')),
    sensitivity text NOT NULL CHECK (sensitivity IN ('dv01','dv100')),
    raw_value numeric, normalized_to_net_assets numeric,
    net_assets_denominator numeric, unit text NOT NULL, sign_semantics text NOT NULL,
    methodology_version text NOT NULL DEFAULT 'nport_fixed_income_features_v2',
    PRIMARY KEY (publication_id,series_id,report_date,accession_number,source_raw_row_id,tenor,sensitivity)
);
CREATE TABLE IF NOT EXISTS nport_fixed_income_credit_spread_sensitivities_v2 (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    series_id text NOT NULL, report_date date NOT NULL, accession_number text NOT NULL,
    source_raw_row_id bigint NOT NULL, source_file_id uuid NOT NULL, source_row_number bigint NOT NULL,
    investment_bucket text NOT NULL CHECK (investment_bucket IN ('investment','noninvestment')),
    tenor text NOT NULL CHECK (tenor IN ('3mon','1yr','5yr','10yr','30yr')),
    raw_value numeric, normalized_to_net_assets numeric, net_assets_denominator numeric,
    measurement_type text NOT NULL DEFAULT 'reported_credit_spread_sensitivity', unit text NOT NULL,
    sign_semantics text NOT NULL, methodology_version text NOT NULL DEFAULT 'nport_fixed_income_features_v2',
    PRIMARY KEY (publication_id,series_id,report_date,accession_number,source_raw_row_id,investment_bucket,tenor)
);
CREATE TABLE IF NOT EXISTS nport_fixed_income_balance_sheet_primitives_v2 (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    series_id text NOT NULL, report_date date NOT NULL, accession_number text NOT NULL,
    source_raw_row_id bigint NOT NULL, source_file_id uuid NOT NULL, source_row_number bigint NOT NULL,
    primitive_key text NOT NULL, raw_value numeric, net_assets_denominator numeric,
    normalized_to_net_assets numeric, unit text NOT NULL, sign_semantics text NOT NULL,
    methodology_version text NOT NULL DEFAULT 'nport_fixed_income_features_v2',
    PRIMARY KEY (publication_id,series_id,report_date,accession_number,source_raw_row_id,primitive_key)
);
CREATE TABLE IF NOT EXISTS nport_fixed_income_debt_flag_features_v2 (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    series_id text NOT NULL, report_date date NOT NULL, flag_key text NOT NULL,
    reported_state text NOT NULL CHECK (reported_state IN ('reported_true','reported_false','not_reported')),
    true_gross_market_value numeric, known_gross_market_value numeric, gross_market_value_denominator numeric,
    unknown_gross_market_value_position_count integer NOT NULL DEFAULT 0 CHECK (unknown_gross_market_value_position_count >= 0),
    gross_market_value_coverage numeric CHECK (gross_market_value_coverage IS NULL OR gross_market_value_coverage BETWEEN 0 AND 1), methodology_version text NOT NULL DEFAULT 'nport_fixed_income_features_v2',
    PRIMARY KEY (publication_id,series_id,report_date,flag_key)
);
ALTER TABLE nport_fixed_income_debt_flag_features_v2
    ADD COLUMN IF NOT EXISTS unknown_gross_market_value_position_count integer NOT NULL DEFAULT 0
    CHECK (unknown_gross_market_value_position_count >= 0),
    ADD COLUMN IF NOT EXISTS gross_market_value_coverage numeric
    CHECK (gross_market_value_coverage IS NULL OR gross_market_value_coverage BETWEEN 0 AND 1);
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='nport_fixed_income_debt_flag_features_v2'::regclass AND conname='nport_fi_debt_flags_unknown_gross_check') THEN
    ALTER TABLE nport_fixed_income_debt_flag_features_v2 ADD CONSTRAINT nport_fi_debt_flags_unknown_gross_check CHECK (unknown_gross_market_value_position_count >= 0);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='nport_fixed_income_debt_flag_features_v2'::regclass AND conname='nport_fi_debt_flags_coverage_check') THEN
    ALTER TABLE nport_fixed_income_debt_flag_features_v2 ADD CONSTRAINT nport_fi_debt_flags_coverage_check CHECK (gross_market_value_coverage IS NULL OR gross_market_value_coverage BETWEEN 0 AND 1);
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS nport_fixed_income_repo_lending_primitives_v2 (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    series_id text NOT NULL, report_date date NOT NULL, accession_number text NOT NULL,
    source_raw_row_id bigint NOT NULL, source_file_id uuid NOT NULL, source_row_number bigint NOT NULL,
    holding_id text NOT NULL DEFAULT '', source_child_id text, primitive_type text NOT NULL, raw_value numeric, currency_code text,
    counterparty_key text NOT NULL, counterparty_name text, counterparty_lei text, raw_value_unit text NOT NULL, sign_semantics text NOT NULL,
    methodology_version text NOT NULL DEFAULT 'nport_fixed_income_features_v2',
    PRIMARY KEY (publication_id,series_id,report_date,accession_number,source_raw_row_id,primitive_type)
);
CREATE TABLE IF NOT EXISTS nport_fixed_income_repo_lending_reported_flags_v2 (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    series_id text NOT NULL, report_date date NOT NULL, accession_number text NOT NULL, holding_id text NOT NULL,
    source_raw_row_id bigint, source_file_id uuid, source_row_number bigint, source_identity_key text NOT NULL,
    flag_key text NOT NULL, reported_state text NOT NULL CHECK (reported_state IN ('reported_true','reported_false','not_reported')),
    methodology_version text NOT NULL DEFAULT 'nport_fixed_income_features_v2',
    PRIMARY KEY (publication_id,series_id,report_date,accession_number,holding_id,source_identity_key,flag_key)
);
CREATE TABLE IF NOT EXISTS nport_fixed_income_metric_coverage_v2 (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    series_id text NOT NULL, report_date date NOT NULL, accession_number text NOT NULL,
    source_raw_row_id bigint, source_file_id uuid, source_row_number bigint,
    source_identity_key text NOT NULL,
    metric_family text NOT NULL, metric_key text NOT NULL,
    numerator numeric NOT NULL, denominator numeric NOT NULL, denominator_unit text NOT NULL,
    coverage_ratio numeric NOT NULL CHECK (coverage_ratio BETWEEN 0 AND 1),
    availability_state text NOT NULL CHECK (availability_state IN ('source_row_absent','field_missing_or_invalid','reported_numeric')),
    missing_reason text, exclusions jsonb NOT NULL DEFAULT '[]'::jsonb,
    methodology_version text NOT NULL DEFAULT 'nport_fixed_income_features_v2',
    PRIMARY KEY (publication_id,series_id,report_date,accession_number,source_identity_key,metric_family,metric_key)
);

CREATE OR REPLACE FUNCTION nport_fixed_income_safe_numeric(value text)
RETURNS numeric LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF value IS NULL OR value !~ '^-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$' THEN RETURN NULL; END IF;
    RETURN value::numeric;
END $$;

CREATE OR REPLACE FUNCTION nport_fixed_income_feature_build_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        source_is_valid boolean;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id
      AND product = 'nport_fixed_income_features_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature build identity requires a prepared fixed-income publication';
    END IF;
    SELECT sec_derived_publication_is_validated(
        NEW.source_holdings_publication_id, 'sec_nport_holdings_v2'
    ) INTO source_is_valid;
    IF NOT source_is_valid THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature build identity requires a validated sec_nport_holdings_v2 source';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS nport_fixed_income_feature_build_write_guard ON nport_fixed_income_feature_builds;
CREATE TRIGGER nport_fixed_income_feature_build_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON nport_fixed_income_feature_builds
FOR EACH ROW EXECUTE FUNCTION nport_fixed_income_feature_build_write_guard();

CREATE OR REPLACE FUNCTION nport_fixed_income_features_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_source uuid;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature row is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id
      AND product = 'nport_fixed_income_features_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature row requires a prepared fixed-income publication';
    END IF;
    SELECT source_holdings_publication_id, as_of_date INTO pinned_source, pinned_as_of
    FROM nport_fixed_income_feature_builds
    WHERE publication_id = NEW.publication_id;
    IF pinned_source IS DISTINCT FROM NEW.source_holdings_publication_id
       OR pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature row requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS nport_fixed_income_features_write_guard ON nport_fixed_income_features;
CREATE TRIGGER nport_fixed_income_features_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON nport_fixed_income_features
FOR EACH ROW EXECUTE FUNCTION nport_fixed_income_features_write_guard();

CREATE OR REPLACE FUNCTION nport_fixed_income_v2_fact_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE pinned_source uuid; pinned_run uuid; pinned_as_of date; parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'N-PORT fixed-income v2 fact row is immutable'; END IF;
    SELECT lifecycle_state INTO parent_state FROM sec_derived_publications
      WHERE publication_id=NEW.publication_id AND product='nport_fixed_income_features_v1' FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN RAISE EXCEPTION 'N-PORT fixed-income v2 fact requires a prepared publication'; END IF;
    SELECT b.source_holdings_publication_id,p.source_run_id,b.as_of_date INTO pinned_source,pinned_run,pinned_as_of
      FROM nport_fixed_income_feature_builds b JOIN sec_derived_publications p ON p.publication_id=b.source_holdings_publication_id
      WHERE b.publication_id=NEW.publication_id;
    IF NEW.source_holdings_publication_id IS DISTINCT FROM pinned_source OR NEW.source_run_id IS DISTINCT FROM pinned_run THEN
        RAISE EXCEPTION 'N-PORT fixed-income v2 fact requires the pinned source publication and run';
    END IF;
    IF NEW.report_date > pinned_as_of THEN RAISE EXCEPTION 'N-PORT fixed-income v2 fact report date exceeds build as_of_date'; END IF;
    RETURN NEW;
END $$;

DO $$ DECLARE target text; BEGIN
  FOREACH target IN ARRAY ARRAY['nport_fixed_income_key_rate_sensitivities_v2','nport_fixed_income_credit_spread_sensitivities_v2','nport_fixed_income_balance_sheet_primitives_v2','nport_fixed_income_debt_flag_features_v2','nport_fixed_income_repo_lending_primitives_v2','nport_fixed_income_repo_lending_reported_flags_v2','nport_fixed_income_metric_coverage_v2'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS nport_fixed_income_v2_fact_write_guard ON %I', target);
    EXECUTE format('CREATE TRIGGER nport_fixed_income_v2_fact_write_guard BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION nport_fixed_income_v2_fact_write_guard()', target);
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION nport_fixed_income_safe_date(value text)
RETURNS date LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF value IS NULL OR value !~ '^\d{4}-\d{2}-\d{2}$' THEN
        RETURN NULL;
    END IF;
    RETURN value::date;
EXCEPTION
    WHEN datetime_field_overflow OR invalid_datetime_format THEN RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION build_nport_fixed_income_features(
    target_publication_id uuid,
    as_of_date date
) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    parent_state text;
    source_publication_id uuid;
    source_run_id uuid;
    pinned_source_publication_id uuid;
    pinned_as_of_date date;
    existing_source_publication_id uuid;
    existing_as_of_date date;
    existing_source_count integer;
    existing_as_of_count integer;
    inserted_count integer;
BEGIN
    IF as_of_date IS NULL THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id
      AND product = 'nport_fixed_income_features_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature build requires a prepared fixed-income publication';
    END IF;

    SELECT c.publication_id INTO source_publication_id
    FROM sec_derived_current_pointers c
    JOIN sec_derived_publications p ON p.publication_id = c.publication_id
    WHERE c.product = 'sec_nport_holdings_v2'
      AND p.product = 'sec_nport_holdings_v2'
      AND p.lifecycle_state = 'validated'
    FOR SHARE OF c;
    IF source_publication_id IS NULL THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature build requires a current sec_nport_holdings_v2 publication';
    END IF;
    SELECT p.source_run_id INTO source_run_id FROM sec_derived_publications p WHERE p.publication_id=source_publication_id;
    IF source_run_id IS NULL THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature build requires a source run pinned by holdings publication';
    END IF;

    -- Fail closed if this DDL is applied over a still-prepared publication
    -- containing rows created before the build-identity table existed.
    SELECT count(DISTINCT source_holdings_publication_id),
           count(DISTINCT measured_at),
           (array_agg(DISTINCT source_holdings_publication_id))[1],
           min(measured_at)
      INTO existing_source_count, existing_as_of_count,
           existing_source_publication_id, existing_as_of_date
    FROM nport_fixed_income_features
    WHERE publication_id = target_publication_id;
    IF existing_source_count > 1 OR existing_as_of_count > 1 THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature publication already contains mixed build identities';
    END IF;
    IF existing_as_of_count = 1 AND existing_as_of_date IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature publication is already pinned to as_of_date %', existing_as_of_date;
    END IF;
    IF existing_source_count = 1 AND existing_source_publication_id IS DISTINCT FROM source_publication_id THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature publication is already pinned to source publication %', existing_source_publication_id;
    END IF;

    INSERT INTO nport_fixed_income_feature_builds
        (publication_id, source_holdings_publication_id, as_of_date)
    VALUES (target_publication_id, source_publication_id, as_of_date)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT build.source_holdings_publication_id, build.as_of_date
      INTO pinned_source_publication_id, pinned_as_of_date
    FROM nport_fixed_income_feature_builds build
    WHERE build.publication_id = target_publication_id
    FOR UPDATE;
    IF pinned_as_of_date IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature publication is already pinned to as_of_date %', pinned_as_of_date;
    END IF;
    IF pinned_source_publication_id IS DISTINCT FROM source_publication_id THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature publication is already pinned to source publication %', pinned_source_publication_id;
    END IF;

    WITH positions AS (
        SELECT h.*, b.series_id, b.class_id, b.instrument_id,
               upper(btrim(COALESCE(h.source_typed_projection ->> 'ASSET_CAT', ''))) AS asset_cat,
               CASE WHEN jsonb_typeof(h.source_typed_projection -> 'DEBT_SECURITY') = 'object'
                    THEN h.source_typed_projection -> 'DEBT_SECURITY' END AS debt_evidence
        FROM sec_nport_holdings_v2 h
        JOIN sec_nport_instrument_class_bridge b
          ON (b.publication_id,b.accession_number,b.holding_id)
           =(h.publication_id,h.accession_number,h.holding_id)
        WHERE h.publication_id = source_publication_id
          AND h.report_date <= as_of_date
          AND b.resolution_state = 'resolved'
          AND h.report_date >= b.valid_from
          AND (b.valid_to IS NULL OR h.report_date <= b.valid_to)
    ), debt AS (
        SELECT *,
               abs(signed_market_value) AS market_value_weight,
               CASE WHEN debt_evidence IS NOT NULL THEN abs(signed_market_value) END AS extension_weight,
               lower(btrim(COALESCE(debt_evidence ->> 'COUPON_TYPE', ''))) AS coupon_type,
               CASE WHEN COALESCE(debt_evidence ->> 'ANNUALIZED_RATE', '') ~ '^-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$'
                    THEN (debt_evidence ->> 'ANNUALIZED_RATE')::numeric END AS annualized_rate,
               nport_fixed_income_safe_date(debt_evidence ->> 'MATURITY_DATE') AS maturity_date
        FROM positions
        WHERE asset_cat = 'DBT'
    ), aggregates AS (
        SELECT series_id, report_date,
               count(*) FILTER (WHERE asset_cat = 'DBT')::integer AS debt_position_count,
               count(*) FILTER (WHERE asset_cat = 'DBT' AND debt_evidence IS NOT NULL)::integer AS debt_extension_position_count,
               count(*) FILTER (WHERE asset_cat = 'DBT' AND signed_market_value IS NULL)::integer AS unknown_debt_market_value_position_count,
               count(*) FILTER (WHERE asset_cat = 'DBT' AND debt_evidence IS NOT NULL
                                AND (NULLIF(btrim(cusip),'') IS NOT NULL OR NULLIF(btrim(isin),'') IS NOT NULL OR NULLIF(btrim(issuer_lei),'') IS NOT NULL))::integer AS identified_debt_position_count,
               sum(signed_market_value) FILTER (WHERE asset_cat = 'DBT') AS debt_signed_market_value,
               sum(abs(signed_market_value)) FILTER (WHERE asset_cat = 'DBT') AS debt_gross_market_value,
               sum(signed_pct_of_nav) FILTER (WHERE asset_cat = 'DBT') AS debt_nav_signed_pct,
               sum(abs(signed_pct_of_nav)) FILTER (WHERE asset_cat = 'DBT') AS debt_nav_gross_pct,
               sum(market_value_weight) AS debt_market_value_denominator,
               sum(extension_weight) AS extension_market_value,
               sum(CASE WHEN annualized_rate IS NOT NULL THEN extension_weight ELSE 0 END) AS coupon_market_value,
               sum(CASE WHEN maturity_date IS NOT NULL THEN extension_weight ELSE 0 END) AS maturity_market_value,
               sum(CASE WHEN debt_evidence IS NOT NULL
                              AND (NULLIF(btrim(cusip),'') IS NOT NULL OR NULLIF(btrim(isin),'') IS NOT NULL OR NULLIF(btrim(issuer_lei),'') IS NOT NULL)
                        THEN extension_weight ELSE 0 END) AS identifier_market_value,
               sum(CASE WHEN coupon_type = 'fixed' THEN extension_weight ELSE 0 END) AS coupon_fixed_weight,
               sum(CASE WHEN coupon_type = 'floating' THEN extension_weight ELSE 0 END) AS coupon_floating_weight,
               sum(CASE WHEN coupon_type = 'variable' THEN extension_weight ELSE 0 END) AS coupon_variable_weight,
               sum(CASE WHEN coupon_type = 'none' THEN extension_weight ELSE 0 END) AS coupon_zero_or_none_weight,
               sum(CASE WHEN coupon_type NOT IN ('fixed','floating','variable','none') THEN extension_weight ELSE 0 END) AS coupon_unknown_weight,
               sum(CASE WHEN maturity_date < report_date + interval '1 year' THEN extension_weight ELSE 0 END) AS maturity_lt_1y_weight,
               sum(CASE WHEN maturity_date >= report_date + interval '1 year' AND maturity_date < report_date + interval '3 years' THEN extension_weight ELSE 0 END) AS maturity_1_3y_weight,
               sum(CASE WHEN maturity_date >= report_date + interval '3 years' AND maturity_date < report_date + interval '5 years' THEN extension_weight ELSE 0 END) AS maturity_3_5y_weight,
               sum(CASE WHEN maturity_date >= report_date + interval '5 years' AND maturity_date < report_date + interval '10 years' THEN extension_weight ELSE 0 END) AS maturity_5_10y_weight,
               sum(CASE WHEN maturity_date >= report_date + interval '10 years' AND maturity_date < report_date + interval '20 years' THEN extension_weight ELSE 0 END) AS maturity_10_20y_weight,
               sum(CASE WHEN maturity_date >= report_date + interval '20 years' THEN extension_weight ELSE 0 END) AS maturity_20y_plus_weight,
               sum(CASE WHEN maturity_date IS NULL THEN extension_weight ELSE 0 END) AS maturity_perpetual_or_missing_weight,
               sum(annualized_rate * extension_weight) AS coupon_weighted_sum
        FROM debt
        GROUP BY series_id, report_date
    ), maturity_values AS (
        SELECT series_id, report_date, extension_weight,
               (maturity_date - report_date)::numeric / 365.25 AS maturity_years
        FROM debt
        WHERE maturity_date IS NOT NULL AND extension_weight IS NOT NULL AND extension_weight > 0
    ), maturity_ranked AS (
        SELECT *, sum(extension_weight) OVER (PARTITION BY series_id, report_date ORDER BY maturity_years) AS cumulative_weight,
               sum(extension_weight) OVER (PARTITION BY series_id, report_date) AS total_weight
        FROM maturity_values
    ), maturity_statistics AS (
        SELECT series_id, report_date,
               sum(maturity_years * extension_weight) / NULLIF(sum(extension_weight), 0) AS mean_years,
               min(maturity_years) FILTER (WHERE cumulative_weight >= total_weight * 0.25) AS p25_years,
               min(maturity_years) FILTER (WHERE cumulative_weight >= total_weight * 0.50) AS median_years,
               min(maturity_years) FILTER (WHERE cumulative_weight >= total_weight * 0.75) AS p75_years
        FROM maturity_ranked
        GROUP BY series_id, report_date
    ), position_counts AS (
        SELECT series_id, report_date, count(*)::integer AS position_count
        FROM positions
        GROUP BY series_id, report_date
    ), computed AS (
        SELECT p.series_id, p.report_date, p.position_count,
               COALESCE(a.debt_position_count, 0) AS debt_position_count,
               COALESCE(a.debt_extension_position_count, 0) AS debt_extension_position_count,
               COALESCE(a.unknown_debt_market_value_position_count, 0) AS unknown_debt_market_value_position_count,
               CASE WHEN COALESCE(a.unknown_debt_market_value_position_count, 0) = 0 THEN a.debt_signed_market_value END AS debt_signed_market_value,
               CASE WHEN COALESCE(a.unknown_debt_market_value_position_count, 0) = 0 THEN a.debt_gross_market_value END AS debt_gross_market_value,
               a.debt_nav_signed_pct, a.debt_nav_gross_pct,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.debt_market_value_denominator > 0 THEN a.extension_market_value / a.debt_market_value_denominator END AS debt_market_value_coverage,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.debt_market_value_denominator > 0 THEN a.extension_market_value / a.debt_market_value_denominator END AS debt_extension_coverage,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.coupon_market_value > 0 THEN a.coupon_weighted_sum / a.coupon_market_value END AS coupon_weighted_average,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN a.coupon_market_value / a.extension_market_value END AS coupon_market_value_coverage,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN jsonb_build_object(
                   'fixed', a.coupon_fixed_weight / a.extension_market_value,
                   'floating', a.coupon_floating_weight / a.extension_market_value,
                   'variable', a.coupon_variable_weight / a.extension_market_value,
                   'zero_or_none', a.coupon_zero_or_none_weight / a.extension_market_value,
                   'unknown', a.coupon_unknown_weight / a.extension_market_value) END AS coupon_type_mix,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN a.maturity_market_value / a.extension_market_value END AS maturity_market_value_coverage,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN jsonb_build_object(
                   'lt_1y', a.maturity_lt_1y_weight / a.extension_market_value,
                   '1_3y', a.maturity_1_3y_weight / a.extension_market_value,
                   '3_5y', a.maturity_3_5y_weight / a.extension_market_value,
                   '5_10y', a.maturity_5_10y_weight / a.extension_market_value,
                   '10_20y', a.maturity_10_20y_weight / a.extension_market_value,
                   '20y_plus', a.maturity_20y_plus_weight / a.extension_market_value,
                   'perpetual_or_missing', a.maturity_perpetual_or_missing_weight / a.extension_market_value) END AS maturity_ladder,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN s.mean_years END AS maturity_weighted_mean_years,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN s.p25_years END AS maturity_weighted_p25_years,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN s.median_years END AS maturity_weighted_median_years,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN s.p75_years END AS maturity_weighted_p75_years,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN a.maturity_market_value / a.extension_market_value END AS maturity_statistics_market_value_coverage,
               CASE WHEN a.unknown_debt_market_value_position_count = 0 AND a.extension_market_value > 0 THEN a.identifier_market_value / a.extension_market_value END AS identifier_market_value_coverage,
               GREATEST(0, as_of_date - p.report_date) AS report_age_days
        FROM position_counts p
        LEFT JOIN aggregates a USING (series_id, report_date)
        LEFT JOIN maturity_statistics s USING (series_id, report_date)
    )
    INSERT INTO nport_fixed_income_features (
        publication_id, source_holdings_publication_id, series_id, report_date, measured_at,
        status, reason_codes, provenance, coverage, position_count, debt_position_count, debt_extension_position_count,
        unknown_debt_market_value_position_count,
        debt_signed_market_value, debt_gross_market_value, debt_nav_signed_pct, debt_nav_gross_pct,
        debt_market_value_coverage, debt_extension_coverage, coupon_weighted_average, coupon_market_value_coverage,
        coupon_type_mix, maturity_market_value_coverage, maturity_ladder,
        maturity_weighted_mean_years, maturity_weighted_p25_years, maturity_weighted_median_years, maturity_weighted_p75_years,
        maturity_statistics_market_value_coverage, identifier_market_value_coverage, report_age_days
    )
    SELECT target_publication_id, source_publication_id, c.series_id, c.report_date, as_of_date,
           CASE
             WHEN c.debt_position_count = 0 THEN 'unavailable'
             WHEN c.debt_extension_position_count = 0 THEN 'unavailable'
             WHEN c.unknown_debt_market_value_position_count > 0 THEN 'insufficient'
             WHEN c.debt_market_value_coverage IS NULL OR c.debt_market_value_coverage < 0.70 OR c.report_age_days > 180 THEN 'insufficient'
             WHEN c.debt_market_value_coverage < 0.90 THEN 'degraded'
             ELSE 'certified'
           END,
           to_jsonb(array_remove(ARRAY[
             CASE WHEN c.debt_position_count = 0 THEN 'no_explicit_dbt_positions' END,
             CASE WHEN c.debt_position_count > 0 AND c.debt_extension_position_count = 0 THEN 'debt_extension_evidence_absent' END,
             CASE WHEN c.unknown_debt_market_value_position_count > 0 THEN 'unknown_debt_market_value' END,
             CASE WHEN c.debt_position_count > 0 AND c.debt_extension_position_count > 0
                            AND c.unknown_debt_market_value_position_count = 0
                            AND c.debt_market_value_coverage IS NULL THEN 'debt_market_value_denominator_unavailable' END,
             CASE WHEN c.debt_market_value_coverage IS NOT NULL AND c.debt_market_value_coverage < 0.70 THEN 'debt_market_value_coverage_below_0_70' END,
             CASE WHEN c.debt_market_value_coverage >= 0.70 AND c.debt_market_value_coverage < 0.90 THEN 'debt_market_value_coverage_below_0_90' END,
             CASE WHEN c.report_age_days > 180 THEN 'report_age_exceeds_180_days' END,
             CASE WHEN c.debt_extension_position_count > 0 AND c.unknown_debt_market_value_position_count = 0
                            AND (c.coupon_market_value_coverage IS NULL OR c.coupon_market_value_coverage < 1) THEN 'coupon_not_fully_reported' END,
             CASE WHEN c.debt_extension_position_count > 0 AND c.unknown_debt_market_value_position_count = 0
                            AND (c.maturity_market_value_coverage IS NULL OR c.maturity_market_value_coverage < 1) THEN 'maturity_not_fully_reported' END
           ], NULL)),
           jsonb_build_object(
             'source_surface', 'sec_nport_holdings_v2_current',
             'source_holdings_publication_id', source_publication_id,
             'source_asset_category_field', 'ASSET_CAT',
             'source_asset_category_literal', 'DBT',
             'source_debt_extension', 'DEBT_SECURITY',
             'debt_market_value_coverage_denominator', 'sum(abs(signed_market_value)) for ASSET_CAT=DBT positions',
             'coupon_and_maturity_coverage_denominator', 'debt-extension market value',
             'report_age_as_of_date', as_of_date),
           jsonb_build_object(
             'debt_market_value', c.debt_market_value_coverage,
             'debt_extension', c.debt_extension_coverage,
             'coupon_eligible_market_value', c.coupon_market_value_coverage,
             'maturity_market_value', c.maturity_market_value_coverage,
             'identifier_market_value', c.identifier_market_value_coverage,
             'unknown_market_value_position_count', c.unknown_debt_market_value_position_count,
             'debt_market_value_denominator_semantics', 'sum(abs(signed_market_value)) for ASSET_CAT=DBT positions'),
           c.position_count, c.debt_position_count, c.debt_extension_position_count,
           c.unknown_debt_market_value_position_count,
           c.debt_signed_market_value, c.debt_gross_market_value, c.debt_nav_signed_pct, c.debt_nav_gross_pct,
           c.debt_market_value_coverage, c.debt_extension_coverage, c.coupon_weighted_average, c.coupon_market_value_coverage,
           c.coupon_type_mix, c.maturity_market_value_coverage, c.maturity_ladder,
           c.maturity_weighted_mean_years, c.maturity_weighted_p25_years, c.maturity_weighted_median_years, c.maturity_weighted_p75_years,
           c.maturity_statistics_market_value_coverage, c.identifier_market_value_coverage, c.report_age_days
    FROM computed c
    ON CONFLICT (publication_id, series_id, report_date) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;

    -- The snapshot filings CTE always binds validated raw views to the exact
    -- source_run_id pinned by sec_nport_holdings_v2, never a current raw run.
    WITH snapshot_filings AS (
      SELECT DISTINCT h.accession_number,b.series_id,h.report_date
      FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b
        ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
      WHERE h.publication_id=source_publication_id AND b.resolution_state='resolved'
        AND h.report_date<=as_of_date AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), fund_info AS (
      SELECT r.accession_number, nport_fixed_income_safe_numeric(r.typed_projection->>'NET_ASSETS') AS net_assets
      FROM nport_fund_reported_info_raw r
      WHERE r.ingestion_run_id=source_run_id
    ), rate_values AS (
      SELECT s.series_id,s.report_date,s.accession_number,r.raw_row_id source_raw_row_id,r.source_file_id,r.source_row_number,COALESCE(NULLIF(r.typed_projection->>'INTEREST_RATE_RISK_ID',''),'not_reported') AS interest_rate_risk_id,COALESCE(NULLIF(r.typed_projection->>'CURRENCY_CODE',''),'not_reported') AS currency_code,f.net_assets,
             v.tenor,v.sensitivity,nport_fixed_income_safe_numeric(v.value) AS raw_value
      FROM snapshot_filings s JOIN nport_interest_rate_risk_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number
      LEFT JOIN fund_info f ON f.accession_number=s.accession_number
      CROSS JOIN LATERAL (VALUES
        ('3mon','dv01',r.typed_projection->>'INTRST_RATE_CHANGE_3MON_DV01'),('1yr','dv01',r.typed_projection->>'INTRST_RATE_CHANGE_1YR_DV01'),('5yr','dv01',r.typed_projection->>'INTRST_RATE_CHANGE_5YR_DV01'),('10yr','dv01',r.typed_projection->>'INTRST_RATE_CHANGE_10YR_DV01'),('30yr','dv01',r.typed_projection->>'INTRST_RATE_CHANGE_30YR_DV01'),
        ('3mon','dv100',r.typed_projection->>'INTRST_RATE_CHANGE_3MON_DV100'),('1yr','dv100',r.typed_projection->>'INTRST_RATE_CHANGE_1YR_DV100'),('5yr','dv100',r.typed_projection->>'INTRST_RATE_CHANGE_5YR_DV100'),('10yr','dv100',r.typed_projection->>'INTRST_RATE_CHANGE_10YR_DV100'),('30yr','dv100',r.typed_projection->>'INTRST_RATE_CHANGE_30YR_DV100')
      ) v(tenor,sensitivity,value)
    ) INSERT INTO nport_fixed_income_key_rate_sensitivities_v2
      (publication_id,source_holdings_publication_id,source_run_id,series_id,report_date,accession_number,source_raw_row_id,source_file_id,source_row_number,interest_rate_risk_id,currency_code,tenor,sensitivity,raw_value,normalized_to_net_assets,net_assets_denominator,unit,sign_semantics)
    SELECT target_publication_id,source_publication_id,source_run_id,series_id,report_date,accession_number,source_raw_row_id,source_file_id,source_row_number,interest_rate_risk_id,currency_code,tenor,sensitivity,raw_value,
           CASE WHEN net_assets IS NOT NULL AND net_assets<>0 THEN raw_value/net_assets END,net_assets,
           CASE sensitivity WHEN 'dv01' THEN 'reported_currency_value_per_1bp' ELSE 'reported_currency_value_per_100bp' END,'reported_signed'
    FROM rate_values WHERE raw_value IS NOT NULL ON CONFLICT DO NOTHING;

    WITH snapshot_filings AS (
      SELECT DISTINCT h.accession_number,b.series_id,h.report_date FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
      WHERE h.publication_id=source_publication_id AND b.resolution_state='resolved' AND h.report_date<=as_of_date AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), values_rows AS (
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,v.metric_key,nport_fixed_income_safe_numeric(v.raw_value) parsed_value,
        CASE WHEN r.raw_row_id IS NULL THEN 'source_row_absent' WHEN nport_fixed_income_safe_numeric(v.raw_value) IS NULL THEN 'field_missing_or_invalid' ELSE 'reported_numeric' END availability_state
      FROM snapshot_filings s LEFT JOIN nport_interest_rate_risk_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number
      CROSS JOIN LATERAL (VALUES
        ('key_rate.dv01.3mon',r.typed_projection->>'INTRST_RATE_CHANGE_3MON_DV01'),('key_rate.dv01.1yr',r.typed_projection->>'INTRST_RATE_CHANGE_1YR_DV01'),('key_rate.dv01.5yr',r.typed_projection->>'INTRST_RATE_CHANGE_5YR_DV01'),('key_rate.dv01.10yr',r.typed_projection->>'INTRST_RATE_CHANGE_10YR_DV01'),('key_rate.dv01.30yr',r.typed_projection->>'INTRST_RATE_CHANGE_30YR_DV01'),
        ('key_rate.dv100.3mon',r.typed_projection->>'INTRST_RATE_CHANGE_3MON_DV100'),('key_rate.dv100.1yr',r.typed_projection->>'INTRST_RATE_CHANGE_1YR_DV100'),('key_rate.dv100.5yr',r.typed_projection->>'INTRST_RATE_CHANGE_5YR_DV100'),('key_rate.dv100.10yr',r.typed_projection->>'INTRST_RATE_CHANGE_10YR_DV100'),('key_rate.dv100.30yr',r.typed_projection->>'INTRST_RATE_CHANGE_30YR_DV100')
      ) v(metric_key,raw_value)
    ) INSERT INTO nport_fixed_income_metric_coverage_v2
      (publication_id,source_holdings_publication_id,source_run_id,series_id,report_date,accession_number,source_raw_row_id,source_file_id,source_row_number,source_identity_key,metric_family,metric_key,numerator,denominator,denominator_unit,coverage_ratio,availability_state,missing_reason,exclusions)
    SELECT target_publication_id,source_publication_id,source_run_id,series_id,report_date,accession_number,raw_row_id,source_file_id,source_row_number,
      COALESCE('raw:'||raw_row_id::text,'absent:nport_interest_rate_risk_raw:'||accession_number),'key_rate_sensitivity',metric_key,
      CASE WHEN parsed_value IS NULL THEN 0 ELSE 1 END,1,'source_row_and_named_field',CASE WHEN parsed_value IS NULL THEN 0 ELSE 1 END,availability_state,
      CASE availability_state WHEN 'source_row_absent' THEN 'no_pinned_raw_source_row' WHEN 'field_missing_or_invalid' THEN 'named_field_missing_or_invalid' END,'[]'::jsonb
    FROM values_rows ON CONFLICT DO NOTHING;

    -- Coverage is a row-level provenance surface: an absent extension has a
    -- deterministic absent key, while every actual raw row retains all three
    -- physical identity fields.  Numeric zero is reported_numeric, never null.
    WITH snapshot_filings AS (
      SELECT DISTINCT h.accession_number,b.series_id,h.report_date FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
      WHERE h.publication_id=source_publication_id AND b.resolution_state='resolved' AND h.report_date<=as_of_date AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), values_rows AS (
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,'credit_spread_sensitivity' family,v.metric_key,v.raw_value
      FROM snapshot_filings s LEFT JOIN nport_fund_reported_info_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number
      CROSS JOIN LATERAL (VALUES ('credit_spread.investment.3mon',r.typed_projection->>'CREDIT_SPREAD_3MON_INVEST'),('credit_spread.noninvestment.3mon',r.typed_projection->>'CREDIT_SPREAD_3MON_NONINVEST'),('credit_spread.investment.1yr',r.typed_projection->>'CREDIT_SPREAD_1YR_INVEST'),('credit_spread.noninvestment.1yr',r.typed_projection->>'CREDIT_SPREAD_1YR_NONINVEST'),('credit_spread.investment.5yr',r.typed_projection->>'CREDIT_SPREAD_5YR_INVEST'),('credit_spread.noninvestment.5yr',r.typed_projection->>'CREDIT_SPREAD_5YR_NONINVEST'),('credit_spread.investment.10yr',r.typed_projection->>'CREDIT_SPREAD_10YR_INVEST'),('credit_spread.noninvestment.10yr',r.typed_projection->>'CREDIT_SPREAD_10YR_NONINVEST'),('credit_spread.investment.30yr',r.typed_projection->>'CREDIT_SPREAD_30YR_INVEST'),('credit_spread.noninvestment.30yr',r.typed_projection->>'CREDIT_SPREAD_30YR_NONINVEST')) v(metric_key,raw_value)
      UNION ALL
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,'balance_sheet',v.metric_key,v.raw_value
      FROM snapshot_filings s LEFT JOIN nport_fund_reported_info_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number
      CROSS JOIN LATERAL (VALUES ('balance.net_assets',r.typed_projection->>'NET_ASSETS'),('balance.total_assets',r.typed_projection->>'TOTAL_ASSETS'),('balance.total_liabilities',r.typed_projection->>'TOTAL_LIABILITIES'),('balance.borrowing_pay_within_1yr',r.typed_projection->>'BORROWING_PAY_WITHIN_1YR'),('balance.controlled_companies_pay_within_1yr',r.typed_projection->>'CTRLD_COMPANIES_PAY_WITHIN_1YR'),('balance.other_affiliates_pay_within_1yr',r.typed_projection->>'OTHER_AFFILIA_PAY_WITHIN_1YR'),('balance.other_pay_within_1yr',r.typed_projection->>'OTHER_PAY_WITHIN_1YR'),('balance.borrowing_pay_after_1yr',r.typed_projection->>'BORROWING_PAY_AFTER_1YR'),('balance.controlled_companies_pay_after_1yr',r.typed_projection->>'CTRLD_COMPANIES_PAY_AFTER_1YR'),('balance.other_affiliates_pay_after_1yr',r.typed_projection->>'OTHER_AFFILIA_PAY_AFTER_1YR'),('balance.other_pay_after_1yr',r.typed_projection->>'OTHER_PAY_AFTER_1YR'),('balance.standby_commitment',r.typed_projection->>'STANDBY_COMMITMENT'),('balance.delayed_delivery',r.typed_projection->>'DELAYED_DELIVERY'),('balance.cash_not_reported_in_c_or_d',r.typed_projection->>'CASH_NOT_RPTD_IN_C_OR_D')) v(metric_key,raw_value)
    ) INSERT INTO nport_fixed_income_metric_coverage_v2
      (publication_id,source_holdings_publication_id,source_run_id,series_id,report_date,accession_number,source_raw_row_id,source_file_id,source_row_number,source_identity_key,metric_family,metric_key,numerator,denominator,denominator_unit,coverage_ratio,availability_state,missing_reason,exclusions)
    SELECT target_publication_id,source_publication_id,source_run_id,series_id,report_date,accession_number,raw_row_id,source_file_id,source_row_number,COALESCE('raw:'||raw_row_id::text,'absent:nport_fund_reported_info_raw:'||accession_number),family,metric_key,
      CASE WHEN nport_fixed_income_safe_numeric(raw_value) IS NULL THEN 0 ELSE 1 END,1,'source_row_and_named_field',CASE WHEN nport_fixed_income_safe_numeric(raw_value) IS NULL THEN 0 ELSE 1 END,
      CASE WHEN raw_row_id IS NULL THEN 'source_row_absent' WHEN nport_fixed_income_safe_numeric(raw_value) IS NULL THEN 'field_missing_or_invalid' ELSE 'reported_numeric' END,
      CASE WHEN raw_row_id IS NULL THEN 'no_pinned_raw_source_row' WHEN nport_fixed_income_safe_numeric(raw_value) IS NULL THEN 'named_field_missing_or_invalid' END,'[]'::jsonb
    FROM values_rows ON CONFLICT DO NOTHING;

    WITH snapshot_filings AS (
      SELECT DISTINCT h.accession_number,b.series_id,h.report_date FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
      WHERE h.publication_id=source_publication_id AND b.resolution_state='resolved' AND h.report_date<=as_of_date
        AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), source_rows AS (
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,nport_fixed_income_safe_numeric(r.typed_projection->>'NET_ASSETS') AS net_assets,r.typed_projection FROM snapshot_filings s JOIN nport_fund_reported_info_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number
    ), values_rows AS (
      SELECT s.*,v.bucket,v.tenor,nport_fixed_income_safe_numeric(v.value) raw_value FROM source_rows s CROSS JOIN LATERAL (VALUES
        ('investment','3mon',s.typed_projection->>'CREDIT_SPREAD_3MON_INVEST'),('noninvestment','3mon',s.typed_projection->>'CREDIT_SPREAD_3MON_NONINVEST'),
        ('investment','1yr',s.typed_projection->>'CREDIT_SPREAD_1YR_INVEST'),('noninvestment','1yr',s.typed_projection->>'CREDIT_SPREAD_1YR_NONINVEST'),
        ('investment','5yr',s.typed_projection->>'CREDIT_SPREAD_5YR_INVEST'),('noninvestment','5yr',s.typed_projection->>'CREDIT_SPREAD_5YR_NONINVEST'),
        ('investment','10yr',s.typed_projection->>'CREDIT_SPREAD_10YR_INVEST'),('noninvestment','10yr',s.typed_projection->>'CREDIT_SPREAD_10YR_NONINVEST'),
        ('investment','30yr',s.typed_projection->>'CREDIT_SPREAD_30YR_INVEST'),('noninvestment','30yr',s.typed_projection->>'CREDIT_SPREAD_30YR_NONINVEST')
      ) v(bucket,tenor,value)
    ) INSERT INTO nport_fixed_income_credit_spread_sensitivities_v2
      (publication_id,source_holdings_publication_id,source_run_id,series_id,report_date,accession_number,source_raw_row_id,source_file_id,source_row_number,investment_bucket,tenor,raw_value,normalized_to_net_assets,net_assets_denominator,unit,sign_semantics)
    SELECT target_publication_id,source_publication_id,source_run_id,series_id,report_date,accession_number,raw_row_id,source_file_id,source_row_number,bucket,tenor,raw_value,
      CASE WHEN net_assets IS NOT NULL AND net_assets<>0 THEN raw_value/net_assets END,net_assets,'reported_currency_value','reported_signed'
    FROM values_rows ON CONFLICT DO NOTHING;

    WITH snapshot_filings AS (
      SELECT DISTINCT h.accession_number,b.series_id,h.report_date FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
      WHERE h.publication_id=source_publication_id AND b.resolution_state='resolved' AND h.report_date<=as_of_date
        AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), source_rows AS (
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,nport_fixed_income_safe_numeric(r.typed_projection->>'NET_ASSETS') net_assets,r.typed_projection FROM snapshot_filings s JOIN nport_fund_reported_info_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number
    ), values_rows AS (
      SELECT s.*,v.key,nport_fixed_income_safe_numeric(v.value) raw_value FROM source_rows s CROSS JOIN LATERAL (VALUES
        ('net_assets',s.typed_projection->>'NET_ASSETS'),('total_assets',s.typed_projection->>'TOTAL_ASSETS'),('total_liabilities',s.typed_projection->>'TOTAL_LIABILITIES'),('borrowing_pay_within_1yr',s.typed_projection->>'BORROWING_PAY_WITHIN_1YR'),('controlled_companies_pay_within_1yr',s.typed_projection->>'CTRLD_COMPANIES_PAY_WITHIN_1YR'),('other_affiliates_pay_within_1yr',s.typed_projection->>'OTHER_AFFILIA_PAY_WITHIN_1YR'),('other_pay_within_1yr',s.typed_projection->>'OTHER_PAY_WITHIN_1YR'),('borrowing_pay_after_1yr',s.typed_projection->>'BORROWING_PAY_AFTER_1YR'),('controlled_companies_pay_after_1yr',s.typed_projection->>'CTRLD_COMPANIES_PAY_AFTER_1YR'),('other_affiliates_pay_after_1yr',s.typed_projection->>'OTHER_AFFILIA_PAY_AFTER_1YR'),('other_pay_after_1yr',s.typed_projection->>'OTHER_PAY_AFTER_1YR'),('standby_commitment',s.typed_projection->>'STANDBY_COMMITMENT'),('delayed_delivery',s.typed_projection->>'DELAYED_DELIVERY'),('cash_not_reported_in_c_or_d',s.typed_projection->>'CASH_NOT_RPTD_IN_C_OR_D')
      ) v(key,value)
    ) INSERT INTO nport_fixed_income_balance_sheet_primitives_v2
      (publication_id,source_holdings_publication_id,source_run_id,series_id,report_date,accession_number,source_raw_row_id,source_file_id,source_row_number,primitive_key,raw_value,net_assets_denominator,normalized_to_net_assets,unit,sign_semantics)
    SELECT target_publication_id,source_publication_id,source_run_id,series_id,report_date,accession_number,raw_row_id,source_file_id,source_row_number,key,raw_value,net_assets,
      CASE WHEN net_assets IS NOT NULL AND net_assets<>0 THEN raw_value/net_assets END,'reported_currency_value','reported_signed'
    FROM values_rows ON CONFLICT DO NOTHING;

    WITH positions AS (
      SELECT b.series_id,h.report_date,h.source_typed_projection->'DEBT_SECURITY' debt,abs(h.signed_market_value) gross
      FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
      WHERE h.publication_id=source_publication_id AND b.resolution_state='resolved' AND h.report_date<=as_of_date
        AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
        AND h.source_typed_projection->>'ASSET_CAT'='DBT'
    ), expanded AS (
      SELECT p.*,v.flag_key,upper(btrim(COALESCE(v.raw_state,''))) normalized_state FROM positions p CROSS JOIN LATERAL (VALUES
        ('is_default',p.debt->>'IS_DEFAULT'),('interest_payment_reported',p.debt->>'ARE_ANY_INTEREST_PAYMENT'),('interest_paid_state',p.debt->>'IS_ANY_PORTION_INTEREST_PAID'),('mandatory_convertible',p.debt->>'IS_CONVTIBLE_MANDATORY'),('contingent_convertible',p.debt->>'IS_CONVTIBLE_CONTINGENT')
      ) v(flag_key,raw_state)
    ) INSERT INTO nport_fixed_income_debt_flag_features_v2
      (publication_id,source_holdings_publication_id,source_run_id,series_id,report_date,flag_key,reported_state,true_gross_market_value,known_gross_market_value,gross_market_value_denominator,unknown_gross_market_value_position_count,gross_market_value_coverage)
    SELECT target_publication_id,source_publication_id,source_run_id,series_id,report_date,flag_key,
      CASE WHEN count(*) FILTER (WHERE normalized_state IN ('Y','YES','TRUE','N','NO','FALSE'))=0 THEN 'not_reported'
           WHEN count(*) FILTER (WHERE normalized_state NOT IN ('Y','YES','TRUE','N','NO','FALSE'))>0 THEN 'not_reported'
           WHEN count(*) FILTER (WHERE normalized_state IN ('Y','YES','TRUE'))>0 THEN 'reported_true' ELSE 'reported_false' END,
      sum(gross) FILTER (WHERE normalized_state IN ('Y','YES','TRUE')),
      sum(gross) FILTER (WHERE normalized_state IN ('Y','YES','TRUE','N','NO','FALSE')),
      sum(gross),count(*) FILTER (WHERE gross IS NULL)::integer,
      CASE WHEN count(*) FILTER (WHERE gross IS NULL)>0 THEN NULL ELSE COALESCE(sum(gross) FILTER (WHERE normalized_state IN ('Y','YES','TRUE','N','NO','FALSE')),0)/NULLIF(sum(gross),0) END
    FROM expanded GROUP BY series_id,report_date,flag_key ON CONFLICT DO NOTHING;

    WITH snapshot_holdings AS (
      SELECT h.accession_number,h.holding_id,b.series_id,h.report_date FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
      WHERE h.publication_id=source_publication_id AND b.resolution_state='resolved' AND h.report_date<=as_of_date
        AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), raw_flags AS (
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,COALESCE('raw:'||r.raw_row_id::text,'absent:nport_securities_lending_raw:'||s.accession_number||':'||s.holding_id) source_identity_key,v.flag_key,upper(btrim(COALESCE(v.raw_state,''))) normalized_state
      FROM snapshot_holdings s LEFT JOIN nport_securities_lending_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      CROSS JOIN LATERAL (VALUES ('is_cash_collateral',r.typed_projection->>'IS_CASH_COLLATERAL'),('is_non_cash_collateral',r.typed_projection->>'IS_NON_CASH_COLLATERAL'),('is_loan_by_fund',r.typed_projection->>'IS_LOAN_BY_FUND')) v(flag_key,raw_state)
    ), flags AS (
      SELECT series_id,report_date,accession_number,holding_id,raw_row_id,source_file_id,source_row_number,source_identity_key,flag_key,
        CASE WHEN count(*) FILTER (WHERE normalized_state IN ('Y','YES','TRUE'))=count(*) AND count(*)>0 THEN 'reported_true'
             WHEN count(*) FILTER (WHERE normalized_state IN ('N','NO','FALSE'))=count(*) AND count(*)>0 THEN 'reported_false'
             ELSE 'not_reported' END AS reported_state
      FROM raw_flags GROUP BY series_id,report_date,accession_number,holding_id,raw_row_id,source_file_id,source_row_number,source_identity_key,flag_key
    ) INSERT INTO nport_fixed_income_repo_lending_reported_flags_v2
      (publication_id,source_holdings_publication_id,source_run_id,series_id,report_date,accession_number,holding_id,source_raw_row_id,source_file_id,source_row_number,source_identity_key,flag_key,reported_state)
    SELECT target_publication_id,source_publication_id,source_run_id,series_id,report_date,accession_number,holding_id,raw_row_id,source_file_id,source_row_number,source_identity_key,flag_key,reported_state
    FROM flags ON CONFLICT DO NOTHING;

    WITH snapshot_holdings AS (
      SELECT h.accession_number,h.holding_id,b.series_id,h.report_date FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
      WHERE h.publication_id=source_publication_id AND b.resolution_state='resolved' AND h.report_date<=as_of_date
        AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), raw_values AS (
      SELECT s.series_id,s.report_date,s.accession_number,s.holding_id,r.raw_row_id,r.source_file_id,r.source_row_number,NULLIF(r.typed_projection->>'REPURCHASE_COUNTERPARTY_ID','') source_child_id,'repurchase_counterparty' primitive_type,NULL::numeric raw_value,NULL::text currency_code,r.typed_projection->>'NAME' counterparty_name,r.typed_projection->>'LEI' counterparty_lei FROM snapshot_holdings s JOIN nport_repurchase_counterparty_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      UNION ALL SELECT s.series_id,s.report_date,s.accession_number,s.holding_id,r.raw_row_id,r.source_file_id,r.source_row_number,NULL,'repurchase_rate',nport_fixed_income_safe_numeric(r.typed_projection->>'REPURCHASE_RATE'),NULL,NULL,NULL FROM snapshot_holdings s JOIN nport_repurchase_agreement_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      UNION ALL SELECT s.series_id,s.report_date,s.accession_number,s.holding_id,r.raw_row_id,r.source_file_id,r.source_row_number,NULLIF(r.typed_projection->>'REPURCHASE_COLLATERAL_ID',''),'repurchase_collateral_principal_amount',nport_fixed_income_safe_numeric(r.typed_projection->>'PRINCIPAL_AMOUNT'),r.typed_projection->>'PRINCIPAL_CURRENCY_CODE',NULL,NULL FROM snapshot_holdings s JOIN nport_repurchase_collateral_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      UNION ALL SELECT s.series_id,s.report_date,s.accession_number,s.holding_id,r.raw_row_id,r.source_file_id,r.source_row_number,NULLIF(r.typed_projection->>'REPURCHASE_COLLATERAL_ID',''),'repurchase_collateral_value',nport_fixed_income_safe_numeric(r.typed_projection->>'COLLATERAL_AMOUNT'),r.typed_projection->>'COLLATERAL_CURRENCY_CODE',NULL,NULL FROM snapshot_holdings s JOIN nport_repurchase_collateral_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      UNION ALL SELECT s.series_id,s.report_date,s.accession_number,s.holding_id,r.raw_row_id,r.source_file_id,r.source_row_number,NULLIF(r.typed_projection->>'SECURITIES_LENDING_ID',''),'securities_lending_loan_value',nport_fixed_income_safe_numeric(r.typed_projection->>'LOAN_VALUE'),NULL,NULL,NULL FROM snapshot_holdings s JOIN nport_securities_lending_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      UNION ALL SELECT s.series_id,s.report_date,s.accession_number,s.holding_id,r.raw_row_id,r.source_file_id,r.source_row_number,NULLIF(r.typed_projection->>'SECURITIES_LENDING_ID',''),'securities_lending_cash_collateral',nport_fixed_income_safe_numeric(r.typed_projection->>'CASH_COLLATERAL_AMOUNT'),NULL,NULL,NULL FROM snapshot_holdings s JOIN nport_securities_lending_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      UNION ALL SELECT s.series_id,s.report_date,s.accession_number,s.holding_id,r.raw_row_id,r.source_file_id,r.source_row_number,NULLIF(r.typed_projection->>'SECURITIES_LENDING_ID',''),'securities_lending_non_cash_collateral',nport_fixed_income_safe_numeric(r.typed_projection->>'NON_CASH_COLLATERAL_VALUE'),NULL,NULL,NULL FROM snapshot_holdings s JOIN nport_securities_lending_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      UNION ALL SELECT s.series_id,s.report_date,s.accession_number,NULL,r.raw_row_id,r.source_file_id,r.source_row_number,NULLIF(r.typed_projection->>'BORROW_AGGREGATE_ID',''),'borrow_aggregate_amount',nport_fixed_income_safe_numeric(r.typed_projection->>'AMOUNT'),NULL,NULL,NULL FROM (SELECT DISTINCT accession_number,series_id,report_date FROM snapshot_holdings) s JOIN nport_borrow_aggregate_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number
      UNION ALL SELECT s.series_id,s.report_date,s.accession_number,NULL,r.raw_row_id,r.source_file_id,r.source_row_number,NULLIF(r.typed_projection->>'BORROW_AGGREGATE_ID',''),'borrow_aggregate_collateral',nport_fixed_income_safe_numeric(r.typed_projection->>'COLLATERAL'),NULL,NULL,NULL FROM (SELECT DISTINCT accession_number,series_id,report_date FROM snapshot_holdings) s JOIN nport_borrow_aggregate_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number
    ) INSERT INTO nport_fixed_income_repo_lending_primitives_v2
      (publication_id,source_holdings_publication_id,source_run_id,series_id,report_date,accession_number,source_raw_row_id,source_file_id,source_row_number,holding_id,source_child_id,primitive_type,raw_value,currency_code,counterparty_key,counterparty_name,counterparty_lei,raw_value_unit,sign_semantics)
    SELECT target_publication_id,source_publication_id,source_run_id,series_id,report_date,accession_number,raw_row_id,source_file_id,source_row_number,COALESCE(holding_id,''),source_child_id,primitive_type,raw_value,currency_code,
      COALESCE('lei:'||NULLIF(counterparty_lei,''),'name:'||NULLIF(counterparty_name,''),'raw:'||raw_row_id::text),counterparty_name,counterparty_lei,
      CASE WHEN primitive_type='repurchase_rate' THEN 'reported_rate' ELSE 'reported_currency_value' END,'reported_signed'
    FROM raw_values ON CONFLICT DO NOTHING;

    WITH snapshot_holdings AS (
      SELECT h.accession_number,h.holding_id,b.series_id,h.report_date FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
      WHERE h.publication_id=source_publication_id AND b.resolution_state='resolved' AND h.report_date<=as_of_date AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), values_rows AS (
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,'repo_lending' family,'repo.repurchase_rate' metric_key,r.typed_projection->>'REPURCHASE_RATE' raw_value,'nport_repurchase_agreement_raw' source_relation
      FROM snapshot_holdings s LEFT JOIN nport_repurchase_agreement_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      UNION ALL
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,'repo_lending',v.metric_key,v.raw_value,'nport_repurchase_collateral_raw'
      FROM snapshot_holdings s LEFT JOIN nport_repurchase_collateral_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      CROSS JOIN LATERAL (VALUES ('repo.repurchase_collateral_principal_amount',r.typed_projection->>'PRINCIPAL_AMOUNT'),('repo.repurchase_collateral_value',r.typed_projection->>'COLLATERAL_AMOUNT')) v(metric_key,raw_value)
      UNION ALL
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,'repo_lending' family,'repo.repurchase_counterparty' metric_key,COALESCE(NULLIF(r.typed_projection->>'NAME',''),NULLIF(r.typed_projection->>'LEI','')) raw_value,'nport_repurchase_counterparty_raw' source_relation
      FROM snapshot_holdings s LEFT JOIN nport_repurchase_counterparty_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      UNION ALL
      SELECT s.*,r.raw_row_id,r.source_file_id,r.source_row_number,'repo_lending',v.metric_key,v.raw_value,'nport_securities_lending_raw'
      FROM snapshot_holdings s LEFT JOIN nport_securities_lending_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number AND r.holding_id=s.holding_id
      CROSS JOIN LATERAL (VALUES ('repo.securities_lending_loan_value',r.typed_projection->>'LOAN_VALUE'),('repo.securities_lending_cash_collateral',r.typed_projection->>'CASH_COLLATERAL_AMOUNT'),('repo.securities_lending_non_cash_collateral',r.typed_projection->>'NON_CASH_COLLATERAL_VALUE'),('repo.is_cash_collateral',r.typed_projection->>'IS_CASH_COLLATERAL'),('repo.is_non_cash_collateral',r.typed_projection->>'IS_NON_CASH_COLLATERAL'),('repo.is_loan_by_fund',r.typed_projection->>'IS_LOAN_BY_FUND')) v(metric_key,raw_value)
      UNION ALL
      SELECT s.accession_number,NULL::text holding_id,s.series_id,s.report_date,r.raw_row_id,r.source_file_id,r.source_row_number,'repo_lending',v.metric_key,v.raw_value,'nport_borrow_aggregate_raw'
      FROM (SELECT DISTINCT accession_number,series_id,report_date FROM snapshot_holdings) s LEFT JOIN nport_borrow_aggregate_raw r ON r.ingestion_run_id=source_run_id AND r.accession_number=s.accession_number
      CROSS JOIN LATERAL (VALUES ('repo.borrow_aggregate_amount',r.typed_projection->>'AMOUNT'),('repo.borrow_aggregate_collateral',r.typed_projection->>'COLLATERAL')) v(metric_key,raw_value)
    ), evaluated AS (
      SELECT *, CASE
        WHEN raw_row_id IS NULL THEN 'source_row_absent'
        WHEN raw_value IS NULL THEN 'field_missing_or_invalid'
        WHEN metric_key LIKE 'repo.is_%' AND upper(btrim(raw_value)) IN ('Y','YES','TRUE','N','NO','FALSE') THEN 'reported_numeric'
        WHEN metric_key LIKE 'repo.is_%' THEN 'field_missing_or_invalid'
        WHEN metric_key <> 'repo.repurchase_counterparty' AND nport_fixed_income_safe_numeric(raw_value) IS NULL THEN 'field_missing_or_invalid'
        ELSE 'reported_numeric'
      END AS availability_state
      FROM values_rows
    ) INSERT INTO nport_fixed_income_metric_coverage_v2
      (publication_id,source_holdings_publication_id,source_run_id,series_id,report_date,accession_number,source_raw_row_id,source_file_id,source_row_number,source_identity_key,metric_family,metric_key,numerator,denominator,denominator_unit,coverage_ratio,availability_state,missing_reason,exclusions)
    SELECT target_publication_id,source_publication_id,source_run_id,series_id,report_date,accession_number,raw_row_id,source_file_id,source_row_number,COALESCE('raw:'||raw_row_id::text,'absent:'||source_relation||':'||accession_number||':'||COALESCE(holding_id,'')),family,metric_key,
      CASE WHEN availability_state='reported_numeric' THEN 1 ELSE 0 END,1,'source_row_and_named_field',
      CASE WHEN availability_state='reported_numeric' THEN 1 ELSE 0 END,availability_state,
      CASE availability_state WHEN 'source_row_absent' THEN 'no_pinned_raw_source_row' WHEN 'field_missing_or_invalid' THEN 'named_field_missing_or_invalid' END,'[]'::jsonb
    FROM evaluated ON CONFLICT DO NOTHING;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_nport_fixed_income_features AS
SELECT f.*
FROM sec_derived_current_pointers c
JOIN nport_fixed_income_features f ON f.publication_id = c.publication_id
WHERE c.product = 'nport_fixed_income_features_v1';

CREATE OR REPLACE VIEW sec_current_nport_fixed_income_key_rate_sensitivities_v2 AS
SELECT f.* FROM sec_derived_current_pointers c JOIN nport_fixed_income_key_rate_sensitivities_v2 f ON f.publication_id=c.publication_id WHERE c.product='nport_fixed_income_features_v1';
CREATE OR REPLACE VIEW sec_current_nport_fixed_income_credit_spread_sensitivities_v2 AS
SELECT f.* FROM sec_derived_current_pointers c JOIN nport_fixed_income_credit_spread_sensitivities_v2 f ON f.publication_id=c.publication_id WHERE c.product='nport_fixed_income_features_v1';
CREATE OR REPLACE VIEW sec_current_nport_fixed_income_balance_sheet_primitives_v2 AS
SELECT f.* FROM sec_derived_current_pointers c JOIN nport_fixed_income_balance_sheet_primitives_v2 f ON f.publication_id=c.publication_id WHERE c.product='nport_fixed_income_features_v1';
CREATE OR REPLACE VIEW sec_current_nport_fixed_income_debt_flag_features_v2 AS
SELECT f.* FROM sec_derived_current_pointers c JOIN nport_fixed_income_debt_flag_features_v2 f ON f.publication_id=c.publication_id WHERE c.product='nport_fixed_income_features_v1';
CREATE OR REPLACE VIEW sec_current_nport_fixed_income_repo_lending_primitives_v2 AS
SELECT f.* FROM sec_derived_current_pointers c JOIN nport_fixed_income_repo_lending_primitives_v2 f ON f.publication_id=c.publication_id WHERE c.product='nport_fixed_income_features_v1';
CREATE OR REPLACE VIEW sec_current_nport_fixed_income_repo_lending_reported_flags_v2 AS
SELECT f.* FROM sec_derived_current_pointers c JOIN nport_fixed_income_repo_lending_reported_flags_v2 f ON f.publication_id=c.publication_id WHERE c.product='nport_fixed_income_features_v1';
CREATE OR REPLACE VIEW sec_current_nport_fixed_income_metric_coverage_v2 AS
SELECT f.* FROM sec_derived_current_pointers c JOIN nport_fixed_income_metric_coverage_v2 f ON f.publication_id=c.publication_id WHERE c.product='nport_fixed_income_features_v1';
