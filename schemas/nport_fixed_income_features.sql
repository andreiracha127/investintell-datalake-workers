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

-- Immutable audit attestation for a locally-computed publication.  Payload
-- bytes are retained outside PostgreSQL; this table pins their canonical
-- manifest and blocks a target UUID from being silently republished.
CREATE TABLE IF NOT EXISTS nport_fixed_income_publication_manifests (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    manifest_sha256 char(64) NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    manifest jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (jsonb_typeof(manifest) = 'object')
);

CREATE OR REPLACE FUNCTION nport_fixed_income_manifest_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-PORT fixed-income publication manifest is immutable';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM sec_derived_publications
        WHERE publication_id = NEW.publication_id
          AND product = 'nport_fixed_income_features_v1'
          AND lifecycle_state = 'prepared'
    ) THEN
        RAISE EXCEPTION 'N-PORT fixed-income publication manifest requires a prepared publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS nport_fixed_income_manifest_write_guard
    ON nport_fixed_income_publication_manifests;
CREATE TRIGGER nport_fixed_income_manifest_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON nport_fixed_income_publication_manifests
FOR EACH ROW EXECUTE FUNCTION nport_fixed_income_manifest_write_guard();

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

-- --------------------------------------------------------------------------- --
-- Storage closure: the O(1) proof that a published relation set is intact.
--
-- The row guards above reject every UPDATE and DELETE outright and admit an
-- INSERT only while the parent publication is 'prepared'.  Once a publication is
-- validated its eight relations are therefore FROZEN by the database, not by
-- convention.  A closure row records the per-relation counts observed while the
-- publication was already validated; because nothing can write afterwards, that
-- observation stays true forever, and an idempotent replay can re-prove storage
-- by reading ONE indexed row instead of running eight full counts -- one of them
-- over nport_fixed_income_metric_coverage_v2 (45.6M rows in production).
--
-- TRUNCATE bypasses row-level triggers, so it is forbidden explicitly below:
-- otherwise it would be the one write that could invalidate a closure silently.
-- --------------------------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS nport_fixed_income_publication_closures (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    manifest_sha256 char(64) NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    relation_counts jsonb NOT NULL CHECK (jsonb_typeof(relation_counts) = 'object'),
    verified_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION nport_fixed_income_closure_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE recorded_manifest char(64);
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-PORT fixed-income publication closure is immutable';
    END IF;
    -- A closure may only be recorded once the publication is validated: before
    -- that the row guards still admit inserts, so a count observed earlier would
    -- not be a closure at all.
    IF NOT sec_derived_publication_is_validated(NEW.publication_id, 'nport_fixed_income_features_v1') THEN
        RAISE EXCEPTION 'N-PORT fixed-income publication closure requires a validated publication';
    END IF;
    -- The closure attests THE manifest that was published, never a free-standing
    -- hash: a closure whose manifest differs from the recorded one would let a
    -- replay of a different artifact skip verification.
    SELECT manifest_sha256 INTO recorded_manifest
    FROM nport_fixed_income_publication_manifests WHERE publication_id = NEW.publication_id;
    IF recorded_manifest IS DISTINCT FROM NEW.manifest_sha256 THEN
        RAISE EXCEPTION 'N-PORT fixed-income publication closure manifest does not match the published manifest';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS nport_fixed_income_closure_write_guard
    ON nport_fixed_income_publication_closures;
CREATE TRIGGER nport_fixed_income_closure_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON nport_fixed_income_publication_closures
FOR EACH ROW EXECUTE FUNCTION nport_fixed_income_closure_write_guard();

CREATE OR REPLACE FUNCTION nport_fixed_income_truncate_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'N-PORT fixed-income publication relations cannot be truncated (%)', TG_TABLE_NAME;
END $$;

DO $$ DECLARE target text; BEGIN
  FOREACH target IN ARRAY ARRAY['nport_fixed_income_features','nport_fixed_income_key_rate_sensitivities_v2','nport_fixed_income_credit_spread_sensitivities_v2','nport_fixed_income_balance_sheet_primitives_v2','nport_fixed_income_debt_flag_features_v2','nport_fixed_income_repo_lending_primitives_v2','nport_fixed_income_repo_lending_reported_flags_v2','nport_fixed_income_metric_coverage_v2'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS nport_fixed_income_truncate_guard ON %I', target);
    EXECUTE format('CREATE TRIGGER nport_fixed_income_truncate_guard BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION nport_fixed_income_truncate_guard()', target);
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

-- build_nport_fixed_income_features() is NOT defined here.
--
-- It used to be a stub that raised 0A000 ("local fixed-income materializer is
-- required"), which made this product impossible to publish from any deployed
-- runtime: the only writer was a human running a separately-attested local
-- PostgreSQL. The real builder lives in the sha256-pinned resource
-- src/nport/sql/nport_fixed_income_features_builder.sql and is installed by the
-- registered worker src/workers/nport_fixed_income_serving.py (see
-- src.nport.fixed_income_local_materializer.install_builder), which verifies the
-- pin before executing it.
--
-- Deliberately absent rather than re-stubbed: a re-run of this DDL over a live
-- database must not silently replace the real builder with a raise.  Every
-- integrity guard the stub was standing in for (prepared->validated->current
-- lifecycle, build-identity pinning, per-relation write guards, immutable
-- manifest) is enforced by the triggers above and by the builder itself.

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
