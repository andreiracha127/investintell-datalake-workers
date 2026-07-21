-- Immutable N-CEN closed-end / interval-fund profiles.  Grain is one row per
-- (publication, source run, accession, FUND_ID).  Families cover the closed-end
-- Part F disclosures: market price / NAV per share, primary and secondary
-- offerings, rights offerings, security repurchases, long-term debt default,
-- dividends in arrears, and material security modifications; interval funds
-- (a closed-end subtype) are surfaced by their own flag and share the repurchase
-- families.  Rights-offering, debt-default, and arrears children are
-- PRE-AGGREGATED at their own grain before being folded onto the fund (Global
-- Constraint 4); a guard fails closed if a fold ever multiplied the fund grain.
--
-- Applicability: REGISTRANT.INVESTMENT_COMPANY_TYPE is an OPEN domain in the
-- frozen SEC dictionary (values are not enumerated), so a closed-end
-- classification code is never fabricated.  A fund's closed-end family is
-- applicable when it answers any Part F question (Y or N) or reports market/NAV
-- per share, or is flagged as an interval fund; an open-end fund that answers
-- none is not_applicable.  Premium/discount is derived via ncen_safe_ratio so a
-- missing market price or a zero/missing NAV yields NULL -- never a synthetic 0.

CREATE TABLE IF NOT EXISTS ncen_closed_end_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    effective_date date NOT NULL,
    form text NOT NULL,
    is_amendment integer NOT NULL CHECK (is_amendment IN (0, 1)),
    registrant_cik text NOT NULL,
    fund_id text NOT NULL,
    series_id text,
    methodology_version text NOT NULL DEFAULT 'ncen_closed_end_v1',
    measured_at date NOT NULL,
    closed_end_state text NOT NULL CHECK (closed_end_state IN ('available','unavailable','not_applicable')),
    closed_end_reason_code text,
    closed_end jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, fund_id),
    CHECK (methodology_version = 'ncen_closed_end_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((closed_end_state = 'available') = (closed_end IS NOT NULL AND closed_end_reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS ncen_closed_end_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ncen_closed_end_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN closed-end build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_closed_end_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN closed-end build identity requires a prepared closed-end publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_closed_end_build_guard ON ncen_closed_end_builds;
CREATE TRIGGER ncen_closed_end_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_closed_end_builds
FOR EACH ROW EXECUTE FUNCTION ncen_closed_end_build_guard();

CREATE OR REPLACE FUNCTION ncen_closed_end_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN closed-end profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_closed_end_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN closed-end profile requires a prepared closed-end publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM ncen_closed_end_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-CEN closed-end profile requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_closed_end_write_guard ON ncen_closed_end_profiles;
CREATE TRIGGER ncen_closed_end_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_closed_end_profiles
FOR EACH ROW EXECUTE FUNCTION ncen_closed_end_write_guard();

CREATE OR REPLACE FUNCTION build_ncen_closed_end_profiles(
    target_publication_id uuid,
    as_of_date date
) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    parent_state text;
    computed_fingerprint char(64);
    selected_count integer;
    pinned_fingerprint char(64);
    pinned_as_of date;
    inserted_count integer;
BEGIN
    IF as_of_date IS NULL THEN
        RAISE EXCEPTION 'N-CEN closed-end build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'ncen_closed_end_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN closed-end build requires a prepared closed-end publication';
    END IF;

    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || effective_date::text || ':' || form,
                '|' ORDER BY ingestion_run_id, accession_number, effective_date, form
            ), '')) || md5('ncen_closed_end_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM ncen_effective_filings
    WHERE effective_date <= as_of_date;

    PERFORM ncen_assert_effective_fund_identity(as_of_date);

    INSERT INTO ncen_closed_end_builds
        (publication_id, input_fingerprint, as_of_date, effective_input_count)
    VALUES (target_publication_id, computed_fingerprint, as_of_date, selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM ncen_closed_end_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-CEN closed-end publication is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'N-CEN closed-end publication is already pinned to effective-input fingerprint %', pinned_fingerprint;
    END IF;

    DROP TABLE IF EXISTS _ncen_ce_sf;
    DROP TABLE IF EXISTS _ncen_ce_children;
    DROP TABLE IF EXISTS _ncen_ce_child_agg;

    CREATE TEMP TABLE _ncen_ce_sf ON COMMIT DROP AS
    SELECT e.ingestion_run_id AS source_run_id, e.accession_number, e.effective_date, e.form, e.is_amendment,
           e.registrant_cik, e.raw_row_id AS submission_raw_row_id,
           f.fund_id, f.raw_row_id AS fund_raw_row_id, f.typed_projection AS fund_evidence,
           NULLIF(btrim(f.typed_projection->>'SERIES_ID'),'') AS series_id,
           NULLIF(btrim(f.typed_projection->>'MARKET_PRICE_PER_SHARE'),'')::numeric AS market_price_per_share,
           NULLIF(btrim(f.typed_projection->>'NAV_PER_SHARE'),'')::numeric AS nav_per_share,
           (
               ncen_tristate_flag(f.typed_projection->>'DID_MAKE_RIGHTS_OFFERING') <> 'not_reported'
               OR ncen_tristate_flag(f.typed_projection->>'DID_MAKE_SECOND_OFFERING') <> 'not_reported'
               OR ncen_tristate_flag(f.typed_projection->>'DID_REPURCHASE_SECURITY') <> 'not_reported'
               OR ncen_tristate_flag(f.typed_projection->>'IS_LONG_TERM_DEBT_DEFAULT') <> 'not_reported'
               OR ncen_tristate_flag(f.typed_projection->>'IS_ACCUM_DIVIDEND_IN_ARREARS') <> 'not_reported'
               OR ncen_tristate_flag(f.typed_projection->>'IS_SECURITY_MAT_MODIFIED') <> 'not_reported'
               OR NULLIF(btrim(f.typed_projection->>'MARKET_PRICE_PER_SHARE'),'') IS NOT NULL
               OR NULLIF(btrim(f.typed_projection->>'NAV_PER_SHARE'),'') IS NOT NULL
               OR ncen_tristate_flag(f.typed_projection->>'IS_INTERVAL') = 'true'
           ) AS is_closed_end
    FROM ncen_effective_filings e
    JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                           AND f.source_table='FUND_REPORTED_INFO.tsv'
                           AND f.parse_status='typed'
                           AND f.accession_number=e.accession_number
    WHERE e.effective_date <= as_of_date;

    -- Part F children pre-aggregated at their own grain (each raw child row is a
    -- distinct offering/default/arrears record).  Every candidate carries the
    -- raw row it came from so a fan-out can be detected.
    CREATE TEMP TABLE _ncen_ce_children ON COMMIT DROP AS
    WITH candidates AS (
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'rights' AS relation,
               jsonb_build_object(
                   'common', ncen_tristate_flag(r.typed_projection->>'IS_RIGHTS_OFFER_COMMON'),
                   'preferred', ncen_tristate_flag(r.typed_projection->>'IS_RIGHTS_OFFER_PREFERRED'),
                   'warrants', ncen_tristate_flag(r.typed_projection->>'IS_RIGHTS_OFFER_WARRANTS'),
                   'convertibles', ncen_tristate_flag(r.typed_projection->>'IS_RIGHTS_OFFER_CONVERTIBLES'),
                   'bonds', ncen_tristate_flag(r.typed_projection->>'IS_RIGHTS_OFFER_BONDS'),
                   'other', ncen_tristate_flag(r.typed_projection->>'IS_RIGHTS_OFFER_OTHER'),
                   'description', NULLIF(btrim(r.typed_projection->>'RIGHTS_OFFER_DESC'),''),
                   'pct_participation_primary', NULLIF(btrim(r.typed_projection->>'PCT_PARTCI_PRIMARY_OFFERING'),'')::numeric
               ) AS payload, r.source_table, r.raw_row_id
        FROM _ncen_ce_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='RIGHTS_OFFERING_FUND.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'debt_default',
               jsonb_build_object(
                   'nature', NULLIF(btrim(r.typed_projection->>'DEFAULT_NATURE'),''),
                   'default_date', NULLIF(btrim(r.typed_projection->>'DEFAULT_DATE'),''),
                   'amount_per_1000', NULLIF(btrim(r.typed_projection->>'DEFAULT_AMNT_PER_1000'),'')::numeric,
                   'total_amount', NULLIF(btrim(r.typed_projection->>'TOTAL_DEFAULT_AMNT'),'')::numeric
               ), r.source_table, r.raw_row_id
        FROM _ncen_ce_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='LONGTERM_DEBT_DEFAULT.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'arrears',
               jsonb_build_object(
                   'issue_title', NULLIF(btrim(r.typed_projection->>'ISSUE_TITLE'),''),
                   'amount_per_share_in_arrear', NULLIF(btrim(r.typed_projection->>'AMOUNT_PER_SHARE_IN_ARREAR'),'')::numeric
               ), r.source_table, r.raw_row_id
        FROM _ncen_ce_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='DIVIDENDS_IN_ARREAR.tsv' AND r.parse_status='typed'
    )
    SELECT source_run_id, accession_number, fund_id, relation, payload, source_table, raw_row_id
    FROM candidates;

    -- Hard failure if pre-aggregation ever multiplied the fund grain.
    IF EXISTS (
        SELECT 1 FROM _ncen_ce_children
        GROUP BY source_run_id, accession_number, fund_id, source_table, raw_row_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'N-CEN closed-end row multiplication detected';
    END IF;

    CREATE TEMP TABLE _ncen_ce_child_agg ON COMMIT DROP AS
    SELECT source_run_id, accession_number, fund_id,
           COALESCE(jsonb_agg(payload ORDER BY raw_row_id) FILTER (WHERE relation='rights'), '[]'::jsonb) AS rights_offerings,
           COALESCE(jsonb_agg(payload ORDER BY raw_row_id) FILTER (WHERE relation='debt_default'), '[]'::jsonb) AS debt_defaults,
           COALESCE(jsonb_agg(payload ORDER BY raw_row_id) FILTER (WHERE relation='arrears'), '[]'::jsonb) AS arrears,
           count(*) FILTER (WHERE relation='rights') AS rights_count,
           count(*) FILTER (WHERE relation='debt_default') AS debt_default_count,
           count(*) FILTER (WHERE relation='arrears') AS arrears_count
    FROM _ncen_ce_children
    GROUP BY source_run_id, accession_number, fund_id;

    INSERT INTO ncen_closed_end_profiles(
        publication_id,source_run_id,accession_number,effective_date,form,is_amendment,registrant_cik,fund_id,series_id,
        measured_at,closed_end_state,closed_end_reason_code,closed_end,provenance,coverage
    )
    SELECT target_publication_id, sf.source_run_id, sf.accession_number, sf.effective_date, sf.form, sf.is_amendment,
           sf.registrant_cik, sf.fund_id, sf.series_id, as_of_date,
           CASE WHEN sf.is_closed_end THEN 'available' ELSE 'not_applicable' END,
           CASE WHEN sf.is_closed_end THEN NULL ELSE 'fund_is_not_closed_end' END,
           CASE WHEN sf.is_closed_end THEN jsonb_build_object(
               'market_valuation', jsonb_build_object(
                   'market_price_per_share', sf.market_price_per_share,
                   'nav_per_share', sf.nav_per_share),
               'offerings', jsonb_build_object(
                   'made_rights_offering', ncen_tristate_flag(sf.fund_evidence->>'DID_MAKE_RIGHTS_OFFERING'),
                   'made_secondary_offering', ncen_tristate_flag(sf.fund_evidence->>'DID_MAKE_SECOND_OFFERING'),
                   'secondary_offering_securities', jsonb_build_object(
                       'common', ncen_tristate_flag(sf.fund_evidence->>'IS_SECONDARY_COMMON'),
                       'preferred', ncen_tristate_flag(sf.fund_evidence->>'IS_SECONDARY_PREFERRED'),
                       'warrants', ncen_tristate_flag(sf.fund_evidence->>'IS_SECONDARY_WARRANTS'),
                       'convertibles', ncen_tristate_flag(sf.fund_evidence->>'IS_SECONDARY_CONVERTIBLES'),
                       'bonds', ncen_tristate_flag(sf.fund_evidence->>'IS_SECONDARY_BONDS'),
                       'other', ncen_tristate_flag(sf.fund_evidence->>'IS_SECONDARY_OTHER'),
                       'other_description', NULLIF(btrim(sf.fund_evidence->>'OTHER_SECONDARY_DESC'),'')),
                   'rights_offerings', COALESCE(ca.rights_offerings, '[]'::jsonb)),
               'repurchases', jsonb_build_object(
                   'repurchased_security', ncen_tristate_flag(sf.fund_evidence->>'DID_REPURCHASE_SECURITY'),
                   'repurchase_securities', jsonb_build_object(
                       'common', ncen_tristate_flag(sf.fund_evidence->>'IS_REPUR_COMMON'),
                       'preferred', ncen_tristate_flag(sf.fund_evidence->>'IS_REPUR_PREFERRED'),
                       'warrants', ncen_tristate_flag(sf.fund_evidence->>'IS_REPUR_WARRANTS'),
                       'convertibles', ncen_tristate_flag(sf.fund_evidence->>'IS_REPUR_CONVERTIBLES'),
                       'bonds', ncen_tristate_flag(sf.fund_evidence->>'IS_REPUR_BONDS'),
                       'other', ncen_tristate_flag(sf.fund_evidence->>'IS_REPUR_OTHER'),
                       'other_description', NULLIF(btrim(sf.fund_evidence->>'OTHER_REPUR_DESC'),''))),
               'debt_default', jsonb_build_object(
                   'has_long_term_debt_default', ncen_tristate_flag(sf.fund_evidence->>'IS_LONG_TERM_DEBT_DEFAULT'),
                   'details', COALESCE(ca.debt_defaults, '[]'::jsonb)),
               'arrears', jsonb_build_object(
                   'has_accumulated_dividends_in_arrears', ncen_tristate_flag(sf.fund_evidence->>'IS_ACCUM_DIVIDEND_IN_ARREARS'),
                   'details', COALESCE(ca.arrears, '[]'::jsonb)),
               'security_modification', jsonb_build_object(
                   'has_material_modification', ncen_tristate_flag(sf.fund_evidence->>'IS_SECURITY_MAT_MODIFIED')),
               'interval_fund', jsonb_build_object(
                   'is_interval', ncen_tristate_flag(sf.fund_evidence->>'IS_INTERVAL')),
               'derived', jsonb_build_object(
                   'premium_discount_ratio', ncen_safe_ratio(sf.market_price_per_share - sf.nav_per_share, sf.nav_per_share),
                   'rights_offering_count', COALESCE(ca.rights_count, 0),
                   'debt_default_count', COALESCE(ca.debt_default_count, 0),
                   'arrears_count', COALESCE(ca.arrears_count, 0))
           ) END,
           jsonb_build_object('effective_selection_view','ncen_effective_filings','registrant_cik',sf.registrant_cik,
                              'submission_raw_row_id',sf.submission_raw_row_id,'fund_raw_row_id',sf.fund_raw_row_id,
                              'source_run_id',sf.source_run_id,'accession_number',sf.accession_number,
                              'fund_source_table','FUND_REPORTED_INFO.tsv'),
           jsonb_build_object('is_closed_end', sf.is_closed_end,
                              'rights_offering_count', COALESCE(ca.rights_count, 0),
                              'debt_default_count', COALESCE(ca.debt_default_count, 0),
                              'arrears_count', COALESCE(ca.arrears_count, 0))
    FROM _ncen_ce_sf sf
    LEFT JOIN _ncen_ce_child_agg ca USING (source_run_id, accession_number, fund_id)
    ON CONFLICT (publication_id,source_run_id,accession_number,fund_id) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_ncen_closed_end_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN ncen_closed_end_profiles p ON p.publication_id=c.publication_id
WHERE c.product='ncen_closed_end_v1';
