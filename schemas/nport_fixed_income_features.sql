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
    identifier_market_value_coverage numeric CHECK (identifier_market_value_coverage IS NULL OR identifier_market_value_coverage BETWEEN 0 AND 1),
    report_age_days integer NOT NULL CHECK (report_age_days >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, series_id, report_date),
    CHECK (methodology_version = 'nport_fixed_income_features_v1'),
    CHECK (jsonb_typeof(reason_codes) = 'array'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object')
);

CREATE INDEX IF NOT EXISTS nport_fixed_income_features_source_idx
    ON nport_fixed_income_features (source_holdings_publication_id, series_id, report_date DESC);

CREATE OR REPLACE FUNCTION nport_fixed_income_features_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature row is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id
      AND product = 'nport_fixed_income_features_v1'
    FOR UPDATE;
    IF parent_state = 'prepared' THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'N-PORT fixed-income feature row requires a prepared fixed-income publication';
END $$;

DROP TRIGGER IF EXISTS nport_fixed_income_features_write_guard ON nport_fixed_income_features;
CREATE TRIGGER nport_fixed_income_features_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON nport_fixed_income_features
FOR EACH ROW EXECUTE FUNCTION nport_fixed_income_features_write_guard();

CREATE OR REPLACE FUNCTION build_nport_fixed_income_features(
    target_publication_id uuid,
    as_of_date date
) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    parent_state text;
    source_publication_id uuid;
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

    SELECT publication_id INTO source_publication_id
    FROM sec_current_derived_publications
    WHERE product = 'sec_nport_holdings_v2';
    IF source_publication_id IS NULL THEN
        RAISE EXCEPTION 'N-PORT fixed-income feature build requires a current sec_nport_holdings_v2 publication';
    END IF;

    WITH positions AS (
        SELECT h.*,
               upper(btrim(COALESCE(h.source_typed_projection ->> 'ASSET_CAT', ''))) AS asset_cat,
               CASE WHEN jsonb_typeof(h.source_typed_projection -> 'DEBT_SECURITY') = 'object'
                    THEN h.source_typed_projection -> 'DEBT_SECURITY' END AS debt_evidence
        FROM sec_nport_holdings_v2_current h
        WHERE h.publication_id = source_publication_id
          AND h.report_date <= as_of_date
    ), debt AS (
        SELECT *,
               abs(COALESCE(signed_market_value, 0)) AS market_value_weight,
               CASE WHEN debt_evidence IS NOT NULL THEN abs(COALESCE(signed_market_value, 0)) ELSE 0 END AS extension_weight,
               lower(btrim(COALESCE(debt_evidence ->> 'COUPON_TYPE', ''))) AS coupon_type,
               CASE WHEN COALESCE(debt_evidence ->> 'ANNUALIZED_RATE', '') ~ '^-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$'
                    THEN (debt_evidence ->> 'ANNUALIZED_RATE')::numeric END AS annualized_rate,
               CASE WHEN COALESCE(debt_evidence ->> 'MATURITY_DATE', '') ~ '^\d{4}-\d{2}-\d{2}$'
                    THEN (debt_evidence ->> 'MATURITY_DATE')::date END AS maturity_date
        FROM positions
        WHERE asset_cat = 'DBT'
    ), aggregates AS (
        SELECT series_id, report_date,
               count(*) FILTER (WHERE asset_cat = 'DBT')::integer AS debt_position_count,
               count(*) FILTER (WHERE asset_cat = 'DBT' AND debt_evidence IS NOT NULL)::integer AS debt_extension_position_count,
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
    ), position_counts AS (
        SELECT series_id, report_date, count(*)::integer AS position_count
        FROM positions
        GROUP BY series_id, report_date
    ), computed AS (
        SELECT p.series_id, p.report_date, p.position_count,
               COALESCE(a.debt_position_count, 0) AS debt_position_count,
               COALESCE(a.debt_extension_position_count, 0) AS debt_extension_position_count,
               a.debt_signed_market_value, a.debt_gross_market_value, a.debt_nav_signed_pct, a.debt_nav_gross_pct,
               CASE WHEN a.debt_market_value_denominator > 0 THEN a.extension_market_value / a.debt_market_value_denominator END AS debt_market_value_coverage,
               CASE WHEN a.debt_market_value_denominator > 0 THEN a.extension_market_value / a.debt_market_value_denominator END AS debt_extension_coverage,
               CASE WHEN a.coupon_market_value > 0 THEN a.coupon_weighted_sum / a.coupon_market_value END AS coupon_weighted_average,
               CASE WHEN a.extension_market_value > 0 THEN a.coupon_market_value / a.extension_market_value END AS coupon_market_value_coverage,
               CASE WHEN a.extension_market_value > 0 THEN jsonb_build_object(
                   'fixed', a.coupon_fixed_weight / a.extension_market_value,
                   'floating', a.coupon_floating_weight / a.extension_market_value,
                   'variable', a.coupon_variable_weight / a.extension_market_value,
                   'zero_or_none', a.coupon_zero_or_none_weight / a.extension_market_value,
                   'unknown', a.coupon_unknown_weight / a.extension_market_value) END AS coupon_type_mix,
               CASE WHEN a.extension_market_value > 0 THEN a.maturity_market_value / a.extension_market_value END AS maturity_market_value_coverage,
               CASE WHEN a.extension_market_value > 0 THEN jsonb_build_object(
                   'lt_1y', a.maturity_lt_1y_weight / a.extension_market_value,
                   '1_3y', a.maturity_1_3y_weight / a.extension_market_value,
                   '3_5y', a.maturity_3_5y_weight / a.extension_market_value,
                   '5_10y', a.maturity_5_10y_weight / a.extension_market_value,
                   '10_20y', a.maturity_10_20y_weight / a.extension_market_value,
                   '20y_plus', a.maturity_20y_plus_weight / a.extension_market_value,
                   'perpetual_or_missing', a.maturity_perpetual_or_missing_weight / a.extension_market_value) END AS maturity_ladder,
               CASE WHEN a.extension_market_value > 0 THEN a.identifier_market_value / a.extension_market_value END AS identifier_market_value_coverage,
               GREATEST(0, as_of_date - p.report_date) AS report_age_days
        FROM position_counts p
        LEFT JOIN aggregates a USING (series_id, report_date)
    )
    INSERT INTO nport_fixed_income_features (
        publication_id, source_holdings_publication_id, series_id, report_date, measured_at,
        status, reason_codes, provenance, coverage, position_count, debt_position_count, debt_extension_position_count,
        debt_signed_market_value, debt_gross_market_value, debt_nav_signed_pct, debt_nav_gross_pct,
        debt_market_value_coverage, debt_extension_coverage, coupon_weighted_average, coupon_market_value_coverage,
        coupon_type_mix, maturity_market_value_coverage, maturity_ladder, identifier_market_value_coverage, report_age_days
    )
    SELECT target_publication_id, source_publication_id, c.series_id, c.report_date, as_of_date,
           CASE
             WHEN c.debt_position_count = 0 THEN 'unavailable'
             WHEN c.debt_market_value_coverage IS NULL OR c.debt_market_value_coverage < 0.70 OR c.report_age_days > 180 THEN 'insufficient'
             WHEN c.debt_market_value_coverage < 0.90 THEN 'degraded'
             ELSE 'certified'
           END,
           to_jsonb(array_remove(ARRAY[
             CASE WHEN c.debt_position_count = 0 THEN 'no_explicit_dbt_positions' END,
             CASE WHEN c.debt_position_count > 0 AND c.debt_market_value_coverage IS NULL THEN 'debt_market_value_denominator_unavailable' END,
             CASE WHEN c.debt_market_value_coverage IS NOT NULL AND c.debt_market_value_coverage < 0.70 THEN 'debt_market_value_coverage_below_0_70' END,
             CASE WHEN c.debt_market_value_coverage >= 0.70 AND c.debt_market_value_coverage < 0.90 THEN 'debt_market_value_coverage_below_0_90' END,
             CASE WHEN c.report_age_days > 180 THEN 'report_age_exceeds_180_days' END,
             CASE WHEN c.coupon_market_value_coverage IS NULL OR c.coupon_market_value_coverage < 1 THEN 'coupon_not_fully_reported' END,
             CASE WHEN c.maturity_market_value_coverage IS NULL OR c.maturity_market_value_coverage < 1 THEN 'maturity_not_fully_reported' END
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
             'debt_market_value_denominator_semantics', 'sum(abs(signed_market_value)) for ASSET_CAT=DBT positions'),
           c.position_count, c.debt_position_count, c.debt_extension_position_count,
           c.debt_signed_market_value, c.debt_gross_market_value, c.debt_nav_signed_pct, c.debt_nav_gross_pct,
           c.debt_market_value_coverage, c.debt_extension_coverage, c.coupon_weighted_average, c.coupon_market_value_coverage,
           c.coupon_type_mix, c.maturity_market_value_coverage, c.maturity_ladder, c.identifier_market_value_coverage, c.report_age_days
    FROM computed c
    ON CONFLICT (publication_id, series_id, report_date) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_nport_fixed_income_features AS
SELECT f.*
FROM sec_derived_current_pointers c
JOIN nport_fixed_income_features f ON f.publication_id = c.publication_id
WHERE c.product = 'nport_fixed_income_features_v1';
