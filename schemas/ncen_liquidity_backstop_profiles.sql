-- Immutable N-CEN liquidity-backstop profiles.  Grain is one row per
-- (publication, source run, accession, FUND_ID).  Families: lines of credit
-- (size/type/use/days) with participating institutions and shared CREDIT_USER
-- rows, interfund lending/borrowing, and swing-pricing flags.  Every child grain
-- (facility, institution, credit user, interfund detail) is PRE-AGGREGATED at its
-- own grain before being folded onto the fund (Global Constraint 4); a guard
-- fails closed if a fold ever multiplied the fund grain.  Inapplicable values are
-- never published as zero: a fund with no line of credit reports the LOC family
-- as not_applicable with NULL derived metrics, distinct from an existing-but-
-- unused line whose utilization is a real reported 0.

CREATE TABLE IF NOT EXISTS ncen_liquidity_backstop_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    effective_date date NOT NULL,
    form text NOT NULL,
    is_amendment integer NOT NULL CHECK (is_amendment IN (0, 1)),
    registrant_cik text NOT NULL,
    fund_id text NOT NULL,
    series_id text,
    methodology_version text NOT NULL DEFAULT 'ncen_liquidity_backstop_v1',
    measured_at date NOT NULL,
    liquidity_backstop_state text NOT NULL CHECK (liquidity_backstop_state IN ('available','unavailable','not_applicable')),
    liquidity_backstop_reason_code text,
    liquidity_backstop jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, fund_id),
    CHECK (methodology_version = 'ncen_liquidity_backstop_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((liquidity_backstop_state = 'available') = (liquidity_backstop IS NOT NULL AND liquidity_backstop_reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS ncen_liquidity_backstop_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ncen_liquidity_backstop_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN liquidity-backstop build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_liquidity_backstop_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN liquidity-backstop build identity requires a prepared liquidity-backstop publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_liquidity_backstop_build_guard ON ncen_liquidity_backstop_builds;
CREATE TRIGGER ncen_liquidity_backstop_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_liquidity_backstop_builds
FOR EACH ROW EXECUTE FUNCTION ncen_liquidity_backstop_build_guard();

CREATE OR REPLACE FUNCTION ncen_liquidity_backstop_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN liquidity backstop is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_liquidity_backstop_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN liquidity backstop requires a prepared liquidity-backstop publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM ncen_liquidity_backstop_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-CEN liquidity backstop requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_liquidity_backstop_write_guard ON ncen_liquidity_backstop_profiles;
CREATE TRIGGER ncen_liquidity_backstop_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_liquidity_backstop_profiles
FOR EACH ROW EXECUTE FUNCTION ncen_liquidity_backstop_write_guard();

CREATE OR REPLACE FUNCTION build_ncen_liquidity_backstop_profiles(
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
        RAISE EXCEPTION 'N-CEN liquidity-backstop build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'ncen_liquidity_backstop_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN liquidity-backstop build requires a prepared liquidity-backstop publication';
    END IF;

    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || effective_date::text || ':' || form,
                '|' ORDER BY ingestion_run_id, accession_number, effective_date, form
            ), '')) || md5('ncen_liquidity_backstop_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM ncen_effective_filings
    WHERE effective_date <= as_of_date;

    PERFORM ncen_assert_effective_fund_identity(as_of_date);

    INSERT INTO ncen_liquidity_backstop_builds
        (publication_id, input_fingerprint, as_of_date, effective_input_count)
    VALUES (target_publication_id, computed_fingerprint, as_of_date, selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM ncen_liquidity_backstop_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-CEN liquidity-backstop publication is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'N-CEN liquidity-backstop publication is already pinned to effective-input fingerprint %', pinned_fingerprint;
    END IF;

    DROP TABLE IF EXISTS _ncen_lb_sf;
    DROP TABLE IF EXISTS _ncen_lb_facilities;
    DROP TABLE IF EXISTS _ncen_lb_loc;
    DROP TABLE IF EXISTS _ncen_lb_interfund;

    -- Selected funds (one row per fund inside an effective filing).
    CREATE TEMP TABLE _ncen_lb_sf ON COMMIT DROP AS
    SELECT e.ingestion_run_id AS source_run_id, e.accession_number, e.effective_date, e.form, e.is_amendment,
           e.registrant_cik, e.raw_row_id AS submission_raw_row_id,
           f.fund_id, f.raw_row_id AS fund_raw_row_id, f.typed_projection AS fund_evidence,
           NULLIF(btrim(f.typed_projection->>'SERIES_ID'),'') AS series_id,
           NULLIF(btrim(f.typed_projection->>'MONTHLY_AVG_NET_ASSETS'),'')::numeric AS monthly_avg_net_assets
    FROM ncen_effective_filings e
    JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                           AND f.source_table='FUND_REPORTED_INFO.tsv'
                           AND f.parse_status='typed'
                           AND f.accession_number=e.accession_number
    WHERE e.effective_date <= as_of_date;

    -- One row per line-of-credit facility, with its participating institutions
    -- and shared credit users pre-aggregated (nested) at the facility grain.
    CREATE TEMP TABLE _ncen_lb_facilities ON COMMIT DROP AS
    SELECT sf.source_run_id, sf.accession_number, sf.fund_id, d.raw_row_id AS facility_raw_row_id,
           NULLIF(btrim(d.typed_projection->>'LINE_OF_CREDIT_SEQNUM'),'') AS seqnum,
           ncen_tristate_flag(d.typed_projection->>'IS_CREDIT_LINE_COMMITTED') AS is_committed,
           NULLIF(btrim(d.typed_projection->>'CREDIT_TYPE'),'') AS credit_type,
           NULLIF(btrim(d.typed_projection->>'LINE_OF_CREDIT_SIZE'),'')::numeric AS size,
           ncen_tristate_flag(d.typed_projection->>'IS_CREDIT_LINE_USED') AS is_used,
           NULLIF(btrim(d.typed_projection->>'AVERAGE_CREDIT_LINE_USED'),'')::numeric AS average_used,
           NULLIF(btrim(d.typed_projection->>'DAYS_CREDIT_USED'),'')::numeric AS days_used,
           inst.names AS institutions, COALESCE(array_length(inst.names,1),0) AS institution_count,
           usr.users AS credit_users, usr.n AS credit_user_count
    FROM _ncen_lb_sf sf
    JOIN ncen_raw_v2_rows d ON d.ingestion_run_id=sf.source_run_id AND d.fund_id=sf.fund_id
                           AND d.source_table='LINE_OF_CREDIT_DETAIL.tsv' AND d.parse_status='typed'
    CROSS JOIN LATERAL (
        SELECT COALESCE(array_agg(NULLIF(btrim(i.typed_projection->>'CREDIT_INSTITUTION_NAME'),'') ORDER BY i.raw_row_id)
                        FILTER (WHERE NULLIF(btrim(i.typed_projection->>'CREDIT_INSTITUTION_NAME'),'') IS NOT NULL),
                        ARRAY[]::text[]) AS names
        FROM ncen_raw_v2_rows i
        WHERE i.ingestion_run_id=sf.source_run_id AND i.fund_id=sf.fund_id
          AND i.source_table='LINE_OF_CREDIT_INSTITUTION.tsv' AND i.parse_status='typed'
          AND NULLIF(btrim(i.typed_projection->>'LINE_OF_CREDIT_SEQNUM'),'')
              IS NOT DISTINCT FROM NULLIF(btrim(d.typed_projection->>'LINE_OF_CREDIT_SEQNUM'),'')
    ) inst
    CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'fund_name', NULLIF(btrim(u.typed_projection->>'FUND_NAME'),''),
                    'sec_file_num', NULLIF(btrim(u.typed_projection->>'SEC_FILE_NUM'),''))
                    ORDER BY u.raw_row_id), '[]'::jsonb) AS users,
               count(*) AS n
        FROM ncen_raw_v2_rows u
        WHERE u.ingestion_run_id=sf.source_run_id AND u.fund_id=sf.fund_id
          AND u.source_table='CREDIT_USER.tsv' AND u.parse_status='typed'
          AND NULLIF(btrim(u.typed_projection->>'LINE_OF_CREDIT_SEQNUM'),'')
              IS NOT DISTINCT FROM NULLIF(btrim(d.typed_projection->>'LINE_OF_CREDIT_SEQNUM'),'')
    ) usr;

    -- Hard failure if a fold ever multiplied a fund's facility grain.
    IF EXISTS (
        SELECT 1 FROM _ncen_lb_facilities
        GROUP BY source_run_id, accession_number, fund_id, facility_raw_row_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'N-CEN liquidity backstop row multiplication detected';
    END IF;

    CREATE TEMP TABLE _ncen_lb_loc ON COMMIT DROP AS
    SELECT source_run_id, accession_number, fund_id,
           jsonb_agg(jsonb_build_object(
               'seqnum', seqnum, 'is_committed', is_committed, 'credit_type', credit_type,
               'size', size, 'is_used', is_used, 'average_used', average_used, 'days_used', days_used,
               'participating_institutions', to_jsonb(institutions), 'credit_users', credit_users,
               'utilization', ncen_safe_ratio(average_used, size)
           ) ORDER BY seqnum, facility_raw_row_id) AS facilities,
           count(*) AS facility_count,
           sum(size) AS total_size,
           sum(average_used) AS total_average_used,
           max(days_used) AS max_days_used,
           sum(institution_count) AS participating_institution_count,
           sum(credit_user_count) AS shared_credit_user_count
    FROM _ncen_lb_facilities
    GROUP BY source_run_id, accession_number, fund_id;

    -- Interfund lending/borrowing detail aggregated at the fund grain.
    CREATE TEMP TABLE _ncen_lb_interfund ON COMMIT DROP AS
    SELECT sf.source_run_id, sf.accession_number, sf.fund_id,
           lend.loan_average AS lending_loan_average, lend.days_outstanding AS lending_days, lend.n AS lending_rows,
           borr.loan_average AS borrowing_loan_average, borr.days_outstanding AS borrowing_days, borr.n AS borrowing_rows
    FROM _ncen_lb_sf sf
    CROSS JOIN LATERAL (
        SELECT sum(NULLIF(btrim(l.typed_projection->>'LENDING_LOAN_AVERAGE'),'')::numeric) AS loan_average,
               max(NULLIF(btrim(l.typed_projection->>'LENDING_DAYS_OUTSTANDING'),'')::numeric) AS days_outstanding,
               count(*) AS n
        FROM ncen_raw_v2_rows l
        WHERE l.ingestion_run_id=sf.source_run_id AND l.fund_id=sf.fund_id
          AND l.source_table='INTER_FUND_LENDING_DETAIL.tsv' AND l.parse_status='typed'
    ) lend
    CROSS JOIN LATERAL (
        SELECT sum(NULLIF(btrim(b.typed_projection->>'BORROWING_LOAN_AVERAGE'),'')::numeric) AS loan_average,
               max(NULLIF(btrim(b.typed_projection->>'BORROWING_DAYS_OUTSTANDING'),'')::numeric) AS days_outstanding,
               count(*) AS n
        FROM ncen_raw_v2_rows b
        WHERE b.ingestion_run_id=sf.source_run_id AND b.fund_id=sf.fund_id
          AND b.source_table='INTER_FUND_BORROWING_DETAIL.tsv' AND b.parse_status='typed'
    ) borr;

    INSERT INTO ncen_liquidity_backstop_profiles(
        publication_id,source_run_id,accession_number,effective_date,form,is_amendment,registrant_cik,fund_id,series_id,
        measured_at,liquidity_backstop_state,liquidity_backstop_reason_code,liquidity_backstop,provenance,coverage
    )
    SELECT target_publication_id, sf.source_run_id, sf.accession_number, sf.effective_date, sf.form, sf.is_amendment,
           sf.registrant_cik, sf.fund_id, sf.series_id, as_of_date,
           CASE WHEN flags.reported > 0 THEN 'available' ELSE 'unavailable' END,
           CASE WHEN flags.reported > 0 THEN NULL ELSE 'liquidity_backstop_not_reported' END,
           CASE WHEN flags.reported > 0 THEN jsonb_build_object(
               'lines_of_credit', jsonb_build_object(
                    'state', CASE ncen_tristate_flag(sf.fund_evidence->>'HAS_LINE_OF_CREDIT')
                                  WHEN 'true' THEN 'available' WHEN 'false' THEN 'not_applicable'
                                  ELSE 'unavailable' END,
                    'facilities', COALESCE(loc.facilities, '[]'::jsonb),
                    'derived', jsonb_build_object(
                        'facility_count', COALESCE(loc.facility_count, 0),
                        'total_size', loc.total_size,
                        'total_average_used', loc.total_average_used,
                        'aggregate_utilization', ncen_safe_ratio(loc.total_average_used, loc.total_size),
                        'max_days_used', loc.max_days_used,
                        'participating_institution_count', COALESCE(loc.participating_institution_count, 0),
                        'shared_credit_user_count', COALESCE(loc.shared_credit_user_count, 0))
               ),
               'interfund', jsonb_build_object(
                    'lending', jsonb_build_object(
                        'state', CASE ncen_tristate_flag(sf.fund_evidence->>'HAS_INTERFUND_LENDING')
                                      WHEN 'true' THEN 'available' WHEN 'false' THEN 'not_applicable'
                                      ELSE 'unavailable' END,
                        'loan_average', itf.lending_loan_average, 'days_outstanding', itf.lending_days),
                    'borrowing', jsonb_build_object(
                        'state', CASE ncen_tristate_flag(sf.fund_evidence->>'HAS_INTERFUND_BORROWING')
                                      WHEN 'true' THEN 'available' WHEN 'false' THEN 'not_applicable'
                                      ELSE 'unavailable' END,
                        'loan_average', itf.borrowing_loan_average, 'days_outstanding', itf.borrowing_days),
                    'derived', jsonb_build_object(
                        'interfund_borrowing_intensity', ncen_safe_ratio(itf.borrowing_loan_average, sf.monthly_avg_net_assets),
                        'interfund_lending_intensity', ncen_safe_ratio(itf.lending_loan_average, sf.monthly_avg_net_assets))
               ),
               'swing_pricing', jsonb_build_object(
                    'has_swing_pricing', ncen_tristate_flag(sf.fund_evidence->>'HAS_SWING_PRICING'),
                    'swing_factor_upper_limit', NULLIF(btrim(sf.fund_evidence->>'SWING_FACTOR_UPPER_LIMIT'),'')::numeric)
           ) END,
           jsonb_build_object('effective_selection_view','ncen_effective_filings','registrant_cik',sf.registrant_cik,
                              'submission_raw_row_id',sf.submission_raw_row_id,'fund_raw_row_id',sf.fund_raw_row_id,
                              'source_run_id',sf.source_run_id,'accession_number',sf.accession_number,
                              'fund_source_table','FUND_REPORTED_INFO.tsv'),
           jsonb_build_object('flags_reported', flags.reported,
                              'loc_facility_count', COALESCE(loc.facility_count, 0),
                              'participating_institution_count', COALESCE(loc.participating_institution_count, 0),
                              'shared_credit_user_count', COALESCE(loc.shared_credit_user_count, 0),
                              'interfund_lending_rows', COALESCE(itf.lending_rows, 0),
                              'interfund_borrowing_rows', COALESCE(itf.borrowing_rows, 0))
    FROM _ncen_lb_sf sf
    LEFT JOIN _ncen_lb_loc loc USING (source_run_id, accession_number, fund_id)
    LEFT JOIN _ncen_lb_interfund itf USING (source_run_id, accession_number, fund_id)
    CROSS JOIN LATERAL (
        SELECT (CASE WHEN ncen_tristate_flag(sf.fund_evidence->>'HAS_LINE_OF_CREDIT') <> 'not_reported' THEN 1 ELSE 0 END
              + CASE WHEN ncen_tristate_flag(sf.fund_evidence->>'HAS_INTERFUND_LENDING') <> 'not_reported' THEN 1 ELSE 0 END
              + CASE WHEN ncen_tristate_flag(sf.fund_evidence->>'HAS_INTERFUND_BORROWING') <> 'not_reported' THEN 1 ELSE 0 END
              + CASE WHEN ncen_tristate_flag(sf.fund_evidence->>'HAS_SWING_PRICING') <> 'not_reported' THEN 1 ELSE 0 END) AS reported
    ) flags
    ON CONFLICT (publication_id,source_run_id,accession_number,fund_id) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_ncen_liquidity_backstop_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN ncen_liquidity_backstop_profiles p ON p.publication_id=c.publication_id
WHERE c.product='ncen_liquidity_backstop_v1';
