-- Immutable N-CEN ETF primary-market profiles.  Grain is one row per
-- (publication, source run, accession, FUND_ID).  Families: authorized
-- participants (stable identifiers, purchase/redemption values), creation-unit
-- size, cash/in-kind mix, purchase/redeem fees, collateral requirement, tracking
-- difference/variability (before/after fee), and affiliated/exclusive index
-- flags.  Authorized-participant children are PRE-AGGREGATED at their own grain
-- before being folded onto the fund (Global Constraint 4); a guard fails closed
-- if a fold ever multiplied the fund grain.  A fund that is not an ETF publishes
-- the whole family as not_applicable (never a synthetic zero); an ETF that filed
-- no ETF primary-market detail is unavailable.

CREATE TABLE IF NOT EXISTS ncen_etf_primary_market_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    effective_date date NOT NULL,
    form text NOT NULL,
    is_amendment integer NOT NULL CHECK (is_amendment IN (0, 1)),
    registrant_cik text NOT NULL,
    fund_id text NOT NULL,
    series_id text,
    methodology_version text NOT NULL DEFAULT 'ncen_etf_primary_market_v1',
    measured_at date NOT NULL,
    etf_primary_market_state text NOT NULL CHECK (etf_primary_market_state IN ('available','unavailable','not_applicable')),
    etf_primary_market_reason_code text,
    etf_primary_market jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, fund_id),
    CHECK (methodology_version = 'ncen_etf_primary_market_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((etf_primary_market_state = 'available') = (etf_primary_market IS NOT NULL AND etf_primary_market_reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS ncen_etf_primary_market_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ncen_etf_primary_market_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN ETF primary-market build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_etf_primary_market_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN ETF primary-market build identity requires a prepared ETF primary-market publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_etf_primary_market_build_guard ON ncen_etf_primary_market_builds;
CREATE TRIGGER ncen_etf_primary_market_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_etf_primary_market_builds
FOR EACH ROW EXECUTE FUNCTION ncen_etf_primary_market_build_guard();

CREATE OR REPLACE FUNCTION ncen_etf_primary_market_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN ETF primary market is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_etf_primary_market_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN ETF primary market requires a prepared ETF primary-market publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM ncen_etf_primary_market_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-CEN ETF primary market requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_etf_primary_market_write_guard ON ncen_etf_primary_market_profiles;
CREATE TRIGGER ncen_etf_primary_market_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_etf_primary_market_profiles
FOR EACH ROW EXECUTE FUNCTION ncen_etf_primary_market_write_guard();

CREATE OR REPLACE FUNCTION build_ncen_etf_primary_market_profiles(
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
        RAISE EXCEPTION 'N-CEN ETF primary-market build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'ncen_etf_primary_market_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN ETF primary-market build requires a prepared ETF primary-market publication';
    END IF;

    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || effective_date::text || ':' || form,
                '|' ORDER BY ingestion_run_id, accession_number, effective_date, form
            ), '')) || md5('ncen_etf_primary_market_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM ncen_effective_filings
    WHERE effective_date <= as_of_date;

    PERFORM ncen_assert_effective_fund_identity(as_of_date);

    INSERT INTO ncen_etf_primary_market_builds
        (publication_id, input_fingerprint, as_of_date, effective_input_count)
    VALUES (target_publication_id, computed_fingerprint, as_of_date, selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM ncen_etf_primary_market_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-CEN ETF primary-market publication is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'N-CEN ETF primary-market publication is already pinned to effective-input fingerprint %', pinned_fingerprint;
    END IF;

    DROP TABLE IF EXISTS _ncen_etf_sf;
    DROP TABLE IF EXISTS _ncen_etf_aps;
    DROP TABLE IF EXISTS _ncen_etf_ap_metrics;

    -- Selected funds with the (at most one) ETF primary-market detail row.
    CREATE TEMP TABLE _ncen_etf_sf ON COMMIT DROP AS
    SELECT e.ingestion_run_id AS source_run_id, e.accession_number, e.effective_date, e.form, e.is_amendment,
           e.registrant_cik, e.raw_row_id AS submission_raw_row_id,
           f.fund_id, f.raw_row_id AS fund_raw_row_id, f.typed_projection AS fund_evidence,
           NULLIF(btrim(f.typed_projection->>'SERIES_ID'),'') AS series_id,
           d.raw_row_id AS etf_raw_row_id, d.typed_projection AS etf_evidence,
           (ncen_tristate_flag(f.typed_projection->>'IS_ETF')='true'
            OR ncen_tristate_flag(f.typed_projection->>'IS_ETMF')='true'
            OR d.raw_row_id IS NOT NULL) AS is_etf
    FROM ncen_effective_filings e
    JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                           AND f.source_table='FUND_REPORTED_INFO.tsv'
                           AND f.parse_status='typed'
                           AND f.accession_number=e.accession_number
    LEFT JOIN ncen_raw_v2_rows d ON d.ingestion_run_id=e.ingestion_run_id
                                AND d.source_table='ETF.tsv' AND d.parse_status='typed'
                                AND d.fund_id=f.fund_id
    WHERE e.effective_date <= as_of_date;

    -- Authorized participants: each row carries a stable identity; identity-less
    -- rows key on their own raw row so they still contribute value and count.
    CREATE TEMP TABLE _ncen_etf_aps ON COMMIT DROP AS
    SELECT sf.source_run_id, sf.accession_number, sf.fund_id, r.raw_row_id,
           ncen_provider_identity(r.typed_projection->>'PARTICIPANT_LEI', r.typed_projection->>'CRD_NUM', NULL,
                                  r.typed_projection->>'PARTICIPANT_NAME') AS identity,
           lower(btrim(r.typed_projection->>'PARTICIPANT_NAME')) AS display_name,
           NULLIF(btrim(r.typed_projection->>'FILE_NUM'),'') AS file_num,
           NULLIF(btrim(r.typed_projection->>'PURCHASE_VALUE'),'')::numeric AS purchase_value,
           NULLIF(btrim(r.typed_projection->>'REDEEM_VALUE'),'')::numeric AS redeem_value
    FROM _ncen_etf_sf sf
    JOIN ncen_raw_v2_rows r ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
                           AND r.source_table='AUTHORIZED_PARTICIPANT.tsv' AND r.parse_status='typed'
    WHERE sf.is_etf;

    -- Hard failure if the AP fold ever multiplied the fund grain.
    IF EXISTS (
        SELECT 1 FROM _ncen_etf_aps
        GROUP BY source_run_id, accession_number, fund_id, raw_row_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'N-CEN ETF primary market row multiplication detected';
    END IF;

    CREATE TEMP TABLE _ncen_etf_ap_metrics ON COMMIT DROP AS
    WITH entity AS (
        SELECT source_run_id, accession_number, fund_id,
               COALESCE(identity->>'identifier_value', 'row:' || raw_row_id) AS entity_key,
               sum(COALESCE(purchase_value,0) + COALESCE(redeem_value,0)) AS entity_value
        FROM _ncen_etf_aps GROUP BY 1,2,3,4
    ), hhi AS (
        SELECT source_run_id, accession_number, fund_id,
               ncen_safe_ratio(sum(power(entity_value,2)), power(sum(entity_value),2)) AS ap_concentration_hhi
        FROM entity GROUP BY 1,2,3
    ), agg AS (
        SELECT source_run_id, accession_number, fund_id,
               jsonb_agg(jsonb_build_object(
                   'identifier_kind', identity->>'identifier_kind', 'identifier_value', identity->>'identifier_value',
                   'display_name', display_name, 'file_num', file_num,
                   'purchase_value', purchase_value, 'redeem_value', redeem_value) ORDER BY raw_row_id) AS aps,
               count(*) AS ap_count,
               sum(purchase_value) AS total_purchase,
               sum(redeem_value) AS total_redeem,
               -- Task 1d (flag-as-data-gap; honesty principle): TRUE when >=1 AP row did
               -- NOT report BOTH legs -- one leg missing (purchase XOR redeem) OR BOTH legs
               -- missing (a listed AP with no reported purchase/redemption activity at all).
               -- A both-NULL AP row carries no activity signal; leaving it unflagged would
               -- read as "covered" while the aggregate net silently omits that participant.
               -- The row is still retained in ap_count (never silently excluded, so the
               -- disclosed roster stays faithful); the serving degrades and drops the
               -- untrustworthy net when this flag is true.
               bool_or(NOT (purchase_value IS NOT NULL AND redeem_value IS NOT NULL)) AS leg_incomplete
        FROM _ncen_etf_aps GROUP BY 1,2,3
    )
    SELECT a.source_run_id, a.accession_number, a.fund_id, a.aps, a.ap_count,
           a.total_purchase, a.total_redeem, a.leg_incomplete, h.ap_concentration_hhi
    FROM agg a JOIN hhi h USING (source_run_id, accession_number, fund_id);

    INSERT INTO ncen_etf_primary_market_profiles(
        publication_id,source_run_id,accession_number,effective_date,form,is_amendment,registrant_cik,fund_id,series_id,
        measured_at,etf_primary_market_state,etf_primary_market_reason_code,etf_primary_market,provenance,coverage
    )
    SELECT target_publication_id, sf.source_run_id, sf.accession_number, sf.effective_date, sf.form, sf.is_amendment,
           sf.registrant_cik, sf.fund_id, sf.series_id, as_of_date,
           CASE WHEN NOT sf.is_etf THEN 'not_applicable'
                WHEN sf.etf_evidence IS NULL THEN 'unavailable'
                ELSE 'available' END,
           CASE WHEN NOT sf.is_etf THEN 'fund_is_not_etf'
                WHEN sf.etf_evidence IS NULL THEN 'etf_primary_market_not_reported'
                ELSE NULL END,
           CASE WHEN sf.is_etf AND sf.etf_evidence IS NOT NULL THEN jsonb_build_object(
               'creation_unit_shares', NULLIF(btrim(sf.etf_evidence->>'NUM_SHARES_PER_CREATION_UNIT'),'')::numeric,
               'is_collateral_required', ncen_tristate_flag(sf.etf_evidence->>'IS_COLLATERAL_REQUIRED'),
               'is_fund_in_kind_etf', ncen_tristate_flag(sf.etf_evidence->>'IS_FUND_IN_KIND_ETF'),
               'cash_in_kind_mix', jsonb_build_object(
                    'purchased_avg_pct_cash', NULLIF(btrim(sf.etf_evidence->>'PURCHASED_AVG_PCT_CASH'),'')::numeric,
                    'purchased_stdv_pct_cash', NULLIF(btrim(sf.etf_evidence->>'PURCHASED_STDV_PCT_CASH'),'')::numeric,
                    'purchased_avg_pct_non_cash', NULLIF(btrim(sf.etf_evidence->>'PURCHASED_AVG_PCT_NON_CASH'),'')::numeric,
                    'purchased_stdv_pct_non_cash', NULLIF(btrim(sf.etf_evidence->>'PURCHASED_STDV_PCT_NON_CASH'),'')::numeric,
                    'redeemed_avg_pct_cash', NULLIF(btrim(sf.etf_evidence->>'REDEEMED_AVG_PCT_CASH'),'')::numeric,
                    'redeemed_stdv_pct_cash', NULLIF(btrim(sf.etf_evidence->>'REDEEMED_STDV_PCT_CASH'),'')::numeric,
                    'redeemed_avg_pct_non_cash', NULLIF(btrim(sf.etf_evidence->>'REDEEMED_AVG_PCT_NON_CASH'),'')::numeric,
                    'redeemed_stdv_pct_non_cash', NULLIF(btrim(sf.etf_evidence->>'REDEEMED_STDV_PCT_NON_CASH'),'')::numeric),
               'fees', jsonb_build_object(
                    'purchase_avg_fee_per_unit', NULLIF(btrim(sf.etf_evidence->>'PURCH_AVG_FEE_PER_UNIT'),'')::numeric,
                    'purchase_avg_fee_same_day', NULLIF(btrim(sf.etf_evidence->>'PURCH_AVG_FEE_SAME_DAY'),'')::numeric,
                    'purchase_avg_fee_percentage', NULLIF(btrim(sf.etf_evidence->>'PURCH_AVG_FEE_PERCENTAGE'),'')::numeric,
                    'purchase_avg_fee_cash_per_unit', NULLIF(btrim(sf.etf_evidence->>'PURCH_AVG_FEE_CASH_PER_UNIT'),'')::numeric,
                    'purchase_avg_fee_cash_same_day', NULLIF(btrim(sf.etf_evidence->>'PURCH_AVG_FEE_CASH_SAME_DAY'),'')::numeric,
                    'purchase_avg_fee_cash_percentage', NULLIF(btrim(sf.etf_evidence->>'PURCH_AVG_FEE_CASH_PERCENTAGE'),'')::numeric,
                    'redeem_avg_fee_per_unit', NULLIF(btrim(sf.etf_evidence->>'REDEEM_AVG_FEE_PER_UNIT'),'')::numeric,
                    'redeem_avg_fee_same_day', NULLIF(btrim(sf.etf_evidence->>'REDEEM_AVG_FEE_SAME_DAY'),'')::numeric,
                    'redeem_avg_fee_percentage', NULLIF(btrim(sf.etf_evidence->>'REDEEM_AVG_FEE_PERCENTAGE'),'')::numeric,
                    'redeem_avg_fee_cash_per_unit', NULLIF(btrim(sf.etf_evidence->>'REDEEM_AVG_FEE_CASH_PER_UNIT'),'')::numeric,
                    'redeem_avg_fee_cash_same_day', NULLIF(btrim(sf.etf_evidence->>'REDEEM_AVG_FEE_CASH_SAME_DAY'),'')::numeric,
                    'redeem_avg_fee_cash_percentage', NULLIF(btrim(sf.etf_evidence->>'REDEEM_AVG_FEE_CASH_PERCENTAGE'),'')::numeric),
               'index_flags', jsonb_build_object(
                    'index_affiliated', ncen_tristate_flag(sf.fund_evidence->>'IS_INDEX_AFFILIATED'),
                    'index_exclusive', ncen_tristate_flag(sf.fund_evidence->>'IS_INDEX_EXCLUSIVE'),
                    'performance_tracked_affiliated_person', ncen_tristate_flag(sf.etf_evidence->>'IS_PERF_TRACKED_AFFILIA_PERSON'),
                    'performance_tracked_exclusively', ncen_tristate_flag(sf.etf_evidence->>'IS_PERF_TRACKED_EXCLUSIVELY')),
               'tracking', jsonb_build_object(
                    'difference_before_fee', NULLIF(btrim(sf.etf_evidence->>'ANNUAL_DIFF_B4_FEE_EXPENSE'),'')::numeric,
                    'difference_after_fee', NULLIF(btrim(sf.etf_evidence->>'ANNUAL_DIFF_AFTER_FEE_EXPENSE'),'')::numeric,
                    'stdev_before_fee', NULLIF(btrim(sf.etf_evidence->>'ANNUAL_STDV_B4_FEE_EXPENSE'),'')::numeric,
                    'stdev_after_fee', NULLIF(btrim(sf.etf_evidence->>'ANNUAL_STDV_AFTER_FEE_EXPENSE'),'')::numeric),
               'authorized_participants', COALESCE(m.aps, '[]'::jsonb),
               'derived', jsonb_build_object(
                    'authorized_participant_count', COALESCE(m.ap_count, 0),
                    'ap_concentration_hhi', m.ap_concentration_hhi,
                    -- Net flow is legitimate ONLY when BOTH aggregate legs are present.
                    -- A missing leg is NEVER coerced to 0 (Task 3's original sin) -> NULL.
                    'net_primary_market_flow',
                        CASE WHEN m.total_purchase IS NOT NULL AND m.total_redeem IS NOT NULL
                             THEN m.total_purchase - m.total_redeem ELSE NULL END,
                    -- Data-quality flag: >=1 AP row did not report both legs -- one leg
                    -- missing, or both legs missing (see agg CTE, Task 1d).  The serving
                    -- layer degrades and drops the net when this is true.
                    'leg_incomplete', COALESCE(m.leg_incomplete, false),
                    'fee_attributable_tracking_drag',
                        CASE WHEN NULLIF(btrim(sf.etf_evidence->>'ANNUAL_DIFF_AFTER_FEE_EXPENSE'),'') IS NOT NULL
                              AND NULLIF(btrim(sf.etf_evidence->>'ANNUAL_DIFF_B4_FEE_EXPENSE'),'') IS NOT NULL
                             THEN round((sf.etf_evidence->>'ANNUAL_DIFF_AFTER_FEE_EXPENSE')::numeric
                                        - (sf.etf_evidence->>'ANNUAL_DIFF_B4_FEE_EXPENSE')::numeric, 6)
                             ELSE NULL END)
           ) END,
           jsonb_build_object('effective_selection_view','ncen_effective_filings','registrant_cik',sf.registrant_cik,
                              'submission_raw_row_id',sf.submission_raw_row_id,'fund_raw_row_id',sf.fund_raw_row_id,
                              'etf_detail_raw_row_id',sf.etf_raw_row_id,
                              'source_run_id',sf.source_run_id,'accession_number',sf.accession_number,
                              'fund_source_table','FUND_REPORTED_INFO.tsv','etf_source_table','ETF.tsv'),
           jsonb_build_object('is_etf', sf.is_etf,
                              'etf_detail_present', (sf.etf_evidence IS NOT NULL),
                              'authorized_participant_count', COALESCE(m.ap_count, 0))
    FROM _ncen_etf_sf sf
    LEFT JOIN _ncen_etf_ap_metrics m USING (source_run_id, accession_number, fund_id)
    ON CONFLICT (publication_id,source_run_id,accession_number,fund_id) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_ncen_etf_primary_market_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN ncen_etf_primary_market_profiles p ON p.publication_id=c.publication_id
WHERE c.product='ncen_etf_primary_market_v1';
