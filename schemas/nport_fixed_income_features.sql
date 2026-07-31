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

-- The legacy oracle is a guarded local-only SQL resource under src/nport/sql/local_only.
-- Production installation always ends with this definition:
-- heavy joins/aggregates are intentionally forbidden in PostgreSQL.
CREATE OR REPLACE FUNCTION build_nport_fixed_income_features(
    target_publication_id uuid,
    as_of_date date
) RETURNS integer LANGUAGE plpgsql AS $$
BEGIN
    RAISE SQLSTATE '0A000' USING
        MESSAGE = 'local fixed-income materializer is required; server-side builder is unsupported',
        HINT = 'Extract immutable inputs and run scripts/materialize_nport_fixed_income_local.py.';
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

-- Cobertura e gravada por posicao: um snapshot carrega ~173k linhas para 46
-- chaves de metrica, quase todas marcadores de ausencia. Servir esse grao por
-- requisicao custou 36 s e ~493 MB de I/O com cache frio, alem do statement
-- timeout do datalake. O rollup guarda as mesmas 46 figuras por snapshot.
--
-- Regra unica da agregacao: combinar apenas o que concorda. Denominadores em
-- unidades diferentes nao sao somaveis, e uma metrica nesse estado fica sem
-- cobertura reportada em vez de virar um numero sem significado.
CREATE MATERIALIZED VIEW IF NOT EXISTS nport_fixed_income_metric_coverage_snapshot_v1 AS
SELECT c.publication_id,
       c.source_holdings_publication_id,
       c.series_id,
       c.report_date,
       c.accession_number,
       c.metric_family,
       c.metric_key,
       CASE WHEN min(c.denominator_unit)=max(c.denominator_unit) THEN sum(c.numerator) END AS numerator,
       CASE WHEN min(c.denominator_unit)=max(c.denominator_unit) THEN sum(c.denominator) END AS denominator,
       CASE WHEN min(c.denominator_unit)=max(c.denominator_unit) THEN min(c.denominator_unit) END AS denominator_unit,
       CASE WHEN min(c.denominator_unit)=max(c.denominator_unit) AND sum(c.denominator)>0
            THEN sum(c.numerator)/sum(c.denominator) END AS coverage_ratio,
       CASE WHEN min(c.availability_state)=max(c.availability_state) THEN min(c.availability_state) END AS availability_state,
       CASE WHEN min(c.methodology_version)=max(c.methodology_version) THEN min(c.methodology_version) END AS methodology_version,
       CASE WHEN min(c.exclusions::text)=max(c.exclusions::text) THEN min(c.exclusions::text)::jsonb ELSE '[]'::jsonb END AS exclusions,
       count(*) AS source_row_count
FROM nport_fixed_income_metric_coverage_v2 c
GROUP BY c.publication_id,c.source_holdings_publication_id,c.series_id,
         c.report_date,c.accession_number,c.metric_family,c.metric_key;

-- Unico: exigido pelo REFRESH ... CONCURRENTLY que o materializador dispara
-- logo apos mover o ponteiro corrente.
CREATE UNIQUE INDEX IF NOT EXISTS nport_fi_metric_coverage_snapshot_v1_pk
  ON nport_fixed_income_metric_coverage_snapshot_v1
  (publication_id,series_id,report_date,accession_number,metric_family,metric_key);

-- O predicado que a camada de leitura usa de fato.
CREATE INDEX IF NOT EXISTS nport_fi_metric_coverage_snapshot_v1_serving_idx
  ON nport_fixed_income_metric_coverage_snapshot_v1
  (source_holdings_publication_id,series_id,report_date,accession_number);

CREATE OR REPLACE VIEW sec_current_nport_fixed_income_metric_coverage_snapshot_v1 AS
SELECT s.* FROM sec_derived_current_pointers c JOIN nport_fixed_income_metric_coverage_snapshot_v1 s ON s.publication_id=c.publication_id WHERE c.product='nport_fixed_income_features_v1';
