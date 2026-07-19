-- Immutable, publication-versioned N-PORT composition snapshot.  Reads only
-- the exact V2 holdings sidecar plus its resolved class bridge; legacy/raw
-- holdings and enrichment surfaces are deliberately outside this product.

CREATE TABLE IF NOT EXISTS nport_composition_features (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    methodology_version text NOT NULL DEFAULT 'nport_composition_features_v1',
    series_id text NOT NULL,
    report_date date NOT NULL,
    measured_at date NOT NULL,
    status text NOT NULL CHECK (status IN ('certified','degraded','insufficient','unavailable')),
    reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    position_count integer NOT NULL CHECK (position_count >= 0),
    unknown_market_value_position_count integer NOT NULL DEFAULT 0 CHECK (unknown_market_value_position_count >= 0),
    unknown_nav_position_count integer NOT NULL DEFAULT 0 CHECK (unknown_nav_position_count >= 0),
    signed_market_value numeric,
    gross_market_value numeric,
    signed_nav_pct numeric,
    gross_nav_pct numeric,
    signed_nav_residual_pct numeric,
    gross_nav_residual_pct numeric,
    identifier_market_value_coverage numeric CHECK (identifier_market_value_coverage IS NULL OR identifier_market_value_coverage BETWEEN 0 AND 1),
    issuer_category_market_value_coverage numeric CHECK (issuer_category_market_value_coverage IS NULL OR issuer_category_market_value_coverage BETWEEN 0 AND 1),
    payoff_profile_market_value_coverage numeric CHECK (payoff_profile_market_value_coverage IS NULL OR payoff_profile_market_value_coverage BETWEEN 0 AND 1),
    issuer_market_value_coverage numeric CHECK (issuer_market_value_coverage IS NULL OR issuer_market_value_coverage BETWEEN 0 AND 1),
    security_identity_market_value_coverage numeric CHECK (security_identity_market_value_coverage IS NULL OR security_identity_market_value_coverage BETWEEN 0 AND 1),
    top_5_gross_market_value_share numeric CHECK (top_5_gross_market_value_share IS NULL OR top_5_gross_market_value_share BETWEEN 0 AND 1),
    top_10_gross_market_value_share numeric CHECK (top_10_gross_market_value_share IS NULL OR top_10_gross_market_value_share BETWEEN 0 AND 1),
    issuer_hhi numeric CHECK (issuer_hhi IS NULL OR issuer_hhi BETWEEN 0 AND 1),
    issuer_effective_position_count numeric,
    security_hhi numeric CHECK (security_hhi IS NULL OR security_hhi BETWEEN 0 AND 1),
    security_effective_position_count numeric,
    report_age_days integer NOT NULL CHECK (report_age_days >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, series_id, report_date),
    CHECK (methodology_version = 'nport_composition_features_v1'),
    CHECK (jsonb_typeof(reason_codes) = 'array'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object')
);

CREATE TABLE IF NOT EXISTS nport_composition_dimension_features (
    publication_id uuid NOT NULL,
    series_id text NOT NULL,
    report_date date NOT NULL,
    dimension_type text NOT NULL CHECK (dimension_type IN ('issuer_category','payoff_profile','identifier_availability')),
    dimension_key text NOT NULL CHECK (dimension_key <> ''),
    position_count integer NOT NULL CHECK (position_count >= 0),
    unknown_market_value_position_count integer NOT NULL DEFAULT 0 CHECK (unknown_market_value_position_count >= 0),
    signed_market_value numeric,
    gross_market_value numeric,
    signed_market_value_share numeric,
    gross_market_value_share numeric CHECK (gross_market_value_share IS NULL OR gross_market_value_share BETWEEN 0 AND 1),
    PRIMARY KEY (publication_id, series_id, report_date, dimension_type, dimension_key),
    FOREIGN KEY (publication_id, series_id, report_date)
        REFERENCES nport_composition_features(publication_id, series_id, report_date) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS nport_composition_feature_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_holdings_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    as_of_date date NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS nport_composition_features_source_idx
    ON nport_composition_features (source_holdings_publication_id, series_id, report_date DESC);

CREATE OR REPLACE FUNCTION nport_composition_feature_build_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        source_is_valid boolean;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-PORT composition feature build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id
      AND product = 'nport_composition_features_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'N-PORT composition feature build identity requires a prepared composition publication';
    END IF;
    SELECT sec_derived_publication_is_validated(NEW.source_holdings_publication_id, 'sec_nport_holdings_v2')
      INTO source_is_valid;
    IF NOT source_is_valid THEN
        RAISE EXCEPTION 'N-PORT composition feature build identity requires a validated sec_nport_holdings_v2 source';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS nport_composition_feature_build_write_guard ON nport_composition_feature_builds;
CREATE TRIGGER nport_composition_feature_build_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON nport_composition_feature_builds
FOR EACH ROW EXECUTE FUNCTION nport_composition_feature_build_write_guard();

CREATE OR REPLACE FUNCTION nport_composition_features_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_source uuid;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-PORT composition feature row is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id
      AND product = 'nport_composition_features_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'N-PORT composition feature row requires a prepared composition publication';
    END IF;
    SELECT source_holdings_publication_id, as_of_date INTO pinned_source, pinned_as_of
    FROM nport_composition_feature_builds WHERE publication_id = NEW.publication_id;
    IF pinned_source IS DISTINCT FROM NEW.source_holdings_publication_id
       OR pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-PORT composition feature row requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS nport_composition_features_write_guard ON nport_composition_features;
CREATE TRIGGER nport_composition_features_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON nport_composition_features
FOR EACH ROW EXECUTE FUNCTION nport_composition_features_write_guard();

CREATE OR REPLACE FUNCTION nport_composition_dimension_features_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-PORT composition dimension row is immutable';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM nport_composition_features f
        JOIN sec_derived_publications p ON p.publication_id=f.publication_id
        WHERE (f.publication_id,f.series_id,f.report_date)=(NEW.publication_id,NEW.series_id,NEW.report_date)
          AND p.product='nport_composition_features_v1' AND p.lifecycle_state='prepared'
    ) THEN
        RAISE EXCEPTION 'N-PORT composition dimension row requires its prepared composition summary';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS nport_composition_dimension_features_write_guard ON nport_composition_dimension_features;
CREATE TRIGGER nport_composition_dimension_features_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON nport_composition_dimension_features
FOR EACH ROW EXECUTE FUNCTION nport_composition_dimension_features_write_guard();

CREATE OR REPLACE FUNCTION build_nport_composition_features(
    target_publication_id uuid,
    as_of_date date
) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    parent_state text;
    source_publication_id uuid;
    pinned_source_publication_id uuid;
    pinned_as_of_date date;
    inserted_count integer;
BEGIN
    IF as_of_date IS NULL THEN
        RAISE EXCEPTION 'N-PORT composition feature build requires an as_of_date';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id=target_publication_id AND product='nport_composition_features_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'N-PORT composition feature build requires a prepared composition publication';
    END IF;

    SELECT c.publication_id INTO source_publication_id
    FROM sec_derived_current_pointers c
    JOIN sec_derived_publications p ON p.publication_id=c.publication_id
    WHERE c.product='sec_nport_holdings_v2'
      AND p.product='sec_nport_holdings_v2' AND p.lifecycle_state='validated'
    FOR SHARE OF c;
    IF source_publication_id IS NULL THEN
        RAISE EXCEPTION 'N-PORT composition feature build requires a current sec_nport_holdings_v2 publication';
    END IF;

    INSERT INTO nport_composition_feature_builds (publication_id,source_holdings_publication_id,as_of_date)
    VALUES (target_publication_id,source_publication_id,as_of_date)
    ON CONFLICT (publication_id) DO NOTHING;
    SELECT b.source_holdings_publication_id,b.as_of_date INTO pinned_source_publication_id,pinned_as_of_date
    FROM nport_composition_feature_builds b WHERE b.publication_id=target_publication_id FOR UPDATE;
    IF pinned_as_of_date IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-PORT composition feature publication is already pinned to as_of_date %', pinned_as_of_date;
    END IF;
    IF pinned_source_publication_id IS DISTINCT FROM source_publication_id THEN
        RAISE EXCEPTION 'N-PORT composition feature publication is already pinned to source publication %', pinned_source_publication_id;
    END IF;

    WITH positions AS (
        SELECT b.series_id,h.report_date,h.holding_id,h.signed_market_value,h.signed_pct_of_nav,
               NULLIF(btrim(h.issuer_name),'') AS issuer_key,
               NULLIF(btrim(h.issuer_category),'') AS issuer_category,
               NULLIF(btrim(h.payoff_profile),'') AS payoff_profile,
               (NULLIF(btrim(h.cusip),'') IS NOT NULL OR NULLIF(btrim(h.isin),'') IS NOT NULL
                    OR NULLIF(btrim(h.issuer_lei),'') IS NOT NULL) AS identifier_available,
               NULLIF(btrim(b.instrument_id),'') AS security_identity_key
        FROM sec_nport_holdings_v2 h
        JOIN sec_nport_instrument_class_bridge b
          ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
        WHERE h.publication_id=source_publication_id AND h.report_date<=as_of_date
          AND b.resolution_state='resolved' AND h.report_date>=b.valid_from
          AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), aggregate_rows AS (
        SELECT series_id,report_date,count(*)::integer AS position_count,
               count(*) FILTER (WHERE signed_market_value IS NULL)::integer AS unknown_market_value_position_count,
               count(*) FILTER (WHERE signed_pct_of_nav IS NULL)::integer AS unknown_nav_position_count,
               sum(signed_market_value) AS signed_market_value,
               sum(abs(signed_market_value)) AS gross_market_value,
               sum(signed_pct_of_nav) AS signed_nav_pct,
               sum(abs(signed_pct_of_nav)) AS gross_nav_pct,
               sum(abs(signed_market_value)) FILTER (WHERE identifier_available) AS identifier_market_value,
               sum(abs(signed_market_value)) FILTER (WHERE issuer_category IS NOT NULL) AS issuer_category_market_value,
               sum(abs(signed_market_value)) FILTER (WHERE payoff_profile IS NOT NULL) AS payoff_profile_market_value,
               sum(abs(signed_market_value)) FILTER (WHERE issuer_key IS NOT NULL) AS issuer_market_value,
               sum(abs(signed_market_value)) FILTER (WHERE security_identity_key IS NOT NULL) AS security_identity_market_value
        FROM positions GROUP BY series_id,report_date
    ), ranked_positions AS (
        SELECT series_id,report_date,abs(signed_market_value) AS gross_weight,
               row_number() OVER (PARTITION BY series_id,report_date ORDER BY abs(signed_market_value) DESC,holding_id) AS rank
        FROM positions WHERE signed_market_value IS NOT NULL
    ), top_n AS (
        SELECT series_id,report_date,
               sum(gross_weight) FILTER (WHERE rank<=5) AS top_5_weight,
               sum(gross_weight) FILTER (WHERE rank<=10) AS top_10_weight
        FROM ranked_positions GROUP BY series_id,report_date
    ), issuer_weights AS (
        SELECT series_id,report_date,issuer_key,sum(abs(signed_market_value)) AS gross_weight
        FROM positions WHERE signed_market_value IS NOT NULL AND issuer_key IS NOT NULL
        GROUP BY series_id,report_date,issuer_key
    ), issuer_concentration AS (
        SELECT a.series_id,a.report_date,
               sum(power(i.gross_weight/a.gross_market_value,2)) AS hhi
        FROM aggregate_rows a JOIN issuer_weights i USING(series_id,report_date)
        WHERE a.unknown_market_value_position_count=0 AND a.gross_market_value>0
          AND a.issuer_market_value=a.gross_market_value
        GROUP BY a.series_id,a.report_date
    ), security_weights AS (
        SELECT series_id,report_date,security_identity_key,sum(abs(signed_market_value)) AS gross_weight
        FROM positions WHERE signed_market_value IS NOT NULL AND security_identity_key IS NOT NULL
        GROUP BY series_id,report_date,security_identity_key
    ), security_concentration AS (
        SELECT a.series_id,a.report_date,
               sum(power(s.gross_weight/a.gross_market_value,2)) AS hhi
        FROM aggregate_rows a JOIN security_weights s USING(series_id,report_date)
        WHERE a.unknown_market_value_position_count=0 AND a.gross_market_value>0
          AND a.security_identity_market_value=a.gross_market_value
        GROUP BY a.series_id,a.report_date
    ), computed AS (
        SELECT a.*,
               CASE WHEN a.unknown_market_value_position_count=0 AND a.gross_market_value>0 THEN a.identifier_market_value/a.gross_market_value END AS identifier_coverage,
               CASE WHEN a.unknown_market_value_position_count=0 AND a.gross_market_value>0 THEN a.issuer_category_market_value/a.gross_market_value END AS issuer_category_coverage,
               CASE WHEN a.unknown_market_value_position_count=0 AND a.gross_market_value>0 THEN a.payoff_profile_market_value/a.gross_market_value END AS payoff_profile_coverage,
               CASE WHEN a.unknown_market_value_position_count=0 AND a.gross_market_value>0 THEN a.issuer_market_value/a.gross_market_value END AS issuer_coverage,
               CASE WHEN a.unknown_market_value_position_count=0 AND a.gross_market_value>0 THEN a.security_identity_market_value/a.gross_market_value END AS security_identity_coverage,
               CASE WHEN a.unknown_market_value_position_count=0 THEN a.signed_market_value END AS certified_signed_market_value,
               CASE WHEN a.unknown_market_value_position_count=0 THEN a.gross_market_value END AS certified_gross_market_value,
               CASE WHEN a.unknown_nav_position_count=0 THEN a.signed_nav_pct END AS certified_signed_nav_pct,
               CASE WHEN a.unknown_nav_position_count=0 THEN a.gross_nav_pct END AS certified_gross_nav_pct,
               CASE WHEN a.unknown_nav_position_count=0 THEN 100-a.signed_nav_pct END AS signed_nav_residual_pct,
               CASE WHEN a.unknown_nav_position_count=0 THEN 100-a.gross_nav_pct END AS gross_nav_residual_pct,
               CASE WHEN a.unknown_market_value_position_count=0 AND a.gross_market_value>0 THEN t.top_5_weight/a.gross_market_value END AS top_5_share,
               CASE WHEN a.unknown_market_value_position_count=0 AND a.gross_market_value>0 THEN t.top_10_weight/a.gross_market_value END AS top_10_share,
               i.hhi AS issuer_hhi,s.hhi AS security_hhi,
               GREATEST(0,as_of_date-a.report_date) AS report_age_days
        FROM aggregate_rows a
        LEFT JOIN top_n t USING(series_id,report_date)
        LEFT JOIN issuer_concentration i USING(series_id,report_date)
        LEFT JOIN security_concentration s USING(series_id,report_date)
    )
    INSERT INTO nport_composition_features (
        publication_id,source_holdings_publication_id,series_id,report_date,measured_at,status,reason_codes,provenance,coverage,
        position_count,unknown_market_value_position_count,unknown_nav_position_count,signed_market_value,gross_market_value,
        signed_nav_pct,gross_nav_pct,signed_nav_residual_pct,gross_nav_residual_pct,identifier_market_value_coverage,
        issuer_category_market_value_coverage,payoff_profile_market_value_coverage,issuer_market_value_coverage,security_identity_market_value_coverage,
        top_5_gross_market_value_share,top_10_gross_market_value_share,issuer_hhi,issuer_effective_position_count,
        security_hhi,security_effective_position_count,report_age_days)
    SELECT target_publication_id,source_publication_id,c.series_id,c.report_date,as_of_date,
           CASE WHEN c.unknown_market_value_position_count>0 OR c.gross_market_value IS NULL OR c.gross_market_value<=0
                          OR c.identifier_coverage IS NULL OR c.issuer_category_coverage IS NULL OR c.payoff_profile_coverage IS NULL OR c.issuer_coverage IS NULL
                          OR c.identifier_coverage<0.70 OR c.issuer_category_coverage<0.70 OR c.payoff_profile_coverage<0.70 OR c.issuer_coverage<0.70
                          OR c.report_age_days>180 THEN 'insufficient'
                WHEN c.identifier_coverage<0.90 OR c.issuer_category_coverage<0.90 OR c.payoff_profile_coverage<0.90 OR c.issuer_coverage<0.90 THEN 'degraded'
                ELSE 'certified' END,
           to_jsonb(array_remove(ARRAY[
              CASE WHEN c.unknown_market_value_position_count>0 THEN 'unknown_market_value' END,
              CASE WHEN c.unknown_nav_position_count>0 THEN 'unknown_nav_pct' END,
              CASE WHEN c.unknown_market_value_position_count=0 AND (c.gross_market_value IS NULL OR c.gross_market_value<=0) THEN 'market_value_denominator_unavailable' END,
              CASE WHEN c.identifier_coverage IS NULL THEN 'identifier_coverage_unavailable' END,
              CASE WHEN c.issuer_category_coverage IS NULL THEN 'issuer_category_coverage_unavailable' END,
              CASE WHEN c.payoff_profile_coverage IS NULL THEN 'payoff_profile_coverage_unavailable' END,
              CASE WHEN c.issuer_coverage IS NULL THEN 'issuer_coverage_unavailable' END,
              CASE WHEN c.identifier_coverage IS NOT NULL AND c.identifier_coverage<0.70 THEN 'identifier_coverage_below_0_70' END,
              CASE WHEN c.identifier_coverage>=0.70 AND c.identifier_coverage<0.90 THEN 'identifier_coverage_below_0_90' END,
              CASE WHEN c.issuer_category_coverage IS NOT NULL AND c.issuer_category_coverage<0.70 THEN 'issuer_category_coverage_below_0_70' END,
              CASE WHEN c.issuer_category_coverage>=0.70 AND c.issuer_category_coverage<0.90 THEN 'issuer_category_coverage_below_0_90' END,
              CASE WHEN c.payoff_profile_coverage IS NOT NULL AND c.payoff_profile_coverage<0.70 THEN 'payoff_profile_coverage_below_0_70' END,
              CASE WHEN c.payoff_profile_coverage>=0.70 AND c.payoff_profile_coverage<0.90 THEN 'payoff_profile_coverage_below_0_90' END,
              CASE WHEN c.issuer_coverage IS NOT NULL AND c.issuer_coverage<0.70 THEN 'issuer_coverage_below_0_70' END,
              CASE WHEN c.issuer_coverage>=0.70 AND c.issuer_coverage<0.90 THEN 'issuer_coverage_below_0_90' END,
              CASE WHEN c.issuer_hhi IS NULL AND c.issuer_coverage<1 THEN 'issuer_coverage_incomplete' END,
              CASE WHEN c.security_identity_coverage IS NULL THEN 'security_identity_coverage_unavailable' END,
              CASE WHEN c.security_hhi IS NULL AND c.security_identity_coverage<1 THEN 'security_identity_coverage_incomplete' END,
              CASE WHEN c.report_age_days>180 THEN 'report_age_exceeds_180_days' END
           ],NULL)),
           jsonb_build_object('source_surface','sec_nport_holdings_v2','source_holdings_publication_id',source_publication_id,
                              'source_fields',jsonb_build_array('issuer_category','payoff_profile','cusip','isin','issuer_lei','signed_market_value','signed_pct_of_nav','instrument_id'),
                              'security_identity_field','resolved bridge instrument_id',
                              'market_value_denominator','sum(abs(signed_market_value))','report_age_as_of_date',as_of_date),
           jsonb_build_object('identifier_market_value',c.identifier_coverage,'issuer_category_market_value',c.issuer_category_coverage,
                              'payoff_profile_market_value',c.payoff_profile_coverage,'issuer_market_value',c.issuer_coverage,
                              'security_identity_market_value',c.security_identity_coverage,
                              'unknown_market_value_position_count',c.unknown_market_value_position_count,'unknown_nav_position_count',c.unknown_nav_position_count),
           c.position_count,c.unknown_market_value_position_count,c.unknown_nav_position_count,c.certified_signed_market_value,c.certified_gross_market_value,
           c.certified_signed_nav_pct,c.certified_gross_nav_pct,c.signed_nav_residual_pct,c.gross_nav_residual_pct,c.identifier_coverage,
           c.issuer_category_coverage,c.payoff_profile_coverage,c.issuer_coverage,c.security_identity_coverage,c.top_5_share,c.top_10_share,c.issuer_hhi,
           CASE WHEN c.issuer_hhi>0 THEN 1/c.issuer_hhi END,c.security_hhi,CASE WHEN c.security_hhi>0 THEN 1/c.security_hhi END,c.report_age_days
    FROM computed c
    ON CONFLICT (publication_id,series_id,report_date) DO NOTHING;
    GET DIAGNOSTICS inserted_count = ROW_COUNT;

    WITH positions AS (
        SELECT b.series_id,h.report_date,h.signed_market_value,
               NULLIF(btrim(h.issuer_category),'') AS issuer_category,
               NULLIF(btrim(h.payoff_profile),'') AS payoff_profile,
               CASE WHEN NULLIF(btrim(h.cusip),'') IS NOT NULL OR NULLIF(btrim(h.isin),'') IS NOT NULL OR NULLIF(btrim(h.issuer_lei),'') IS NOT NULL
                    THEN 'available' ELSE 'unavailable' END AS identifier_availability
        FROM sec_nport_holdings_v2 h JOIN sec_nport_instrument_class_bridge b
          ON (b.publication_id,b.accession_number,b.holding_id)=(h.publication_id,h.accession_number,h.holding_id)
        WHERE h.publication_id=source_publication_id AND h.report_date<=as_of_date AND b.resolution_state='resolved'
          AND h.report_date>=b.valid_from AND (b.valid_to IS NULL OR h.report_date<=b.valid_to)
    ), dimensioned AS (
        SELECT series_id,report_date,'issuer_category'::text AS dimension_type,COALESCE(issuer_category,'unknown') AS dimension_key,signed_market_value FROM positions
        UNION ALL SELECT series_id,report_date,'payoff_profile',COALESCE(payoff_profile,'unknown'),signed_market_value FROM positions
        UNION ALL SELECT series_id,report_date,'identifier_availability',identifier_availability,signed_market_value FROM positions
    ), grouped AS (
        SELECT series_id,report_date,dimension_type,dimension_key,count(*)::integer AS position_count,
               count(*) FILTER (WHERE signed_market_value IS NULL)::integer AS unknown_market_value_position_count,
               sum(signed_market_value) AS signed_market_value,sum(abs(signed_market_value)) AS gross_market_value
        FROM dimensioned GROUP BY series_id,report_date,dimension_type,dimension_key
    )
    INSERT INTO nport_composition_dimension_features (
        publication_id,series_id,report_date,dimension_type,dimension_key,position_count,unknown_market_value_position_count,
        signed_market_value,gross_market_value,signed_market_value_share,gross_market_value_share)
    SELECT target_publication_id,g.series_id,g.report_date,g.dimension_type,g.dimension_key,g.position_count,g.unknown_market_value_position_count,
           CASE WHEN g.unknown_market_value_position_count=0 THEN g.signed_market_value END,
           CASE WHEN g.unknown_market_value_position_count=0 THEN g.gross_market_value END,
           CASE WHEN g.unknown_market_value_position_count=0 AND f.signed_market_value IS NOT NULL AND f.signed_market_value<>0 THEN g.signed_market_value/f.signed_market_value END,
           CASE WHEN g.unknown_market_value_position_count=0 AND f.gross_market_value IS NOT NULL AND f.gross_market_value>0 THEN g.gross_market_value/f.gross_market_value END
    FROM grouped g JOIN nport_composition_features f
      ON (f.publication_id,f.series_id,f.report_date)=(target_publication_id,g.series_id,g.report_date)
    ON CONFLICT (publication_id,series_id,report_date,dimension_type,dimension_key) DO NOTHING;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_nport_composition_features AS
SELECT f.*
FROM sec_derived_current_pointers c
JOIN nport_composition_features f ON f.publication_id=c.publication_id
WHERE c.product='nport_composition_features_v1';

CREATE OR REPLACE VIEW sec_current_nport_composition_dimension_features AS
SELECT d.*
FROM sec_derived_current_pointers c
JOIN nport_composition_dimension_features d ON d.publication_id=c.publication_id
WHERE c.product='nport_composition_features_v1';
