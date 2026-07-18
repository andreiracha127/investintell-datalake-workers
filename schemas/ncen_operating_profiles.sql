-- Immutable N-CEN operating profiles.  This bounded builder consumes only the
-- publishable amendment-aware selection and typed V2 rows while the target
-- publication is prepared; the current view never reaches back into raw rows.

CREATE TABLE IF NOT EXISTS ncen_operating_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    effective_date date NOT NULL,
    form text NOT NULL,
    is_amendment integer NOT NULL CHECK (is_amendment IN (0, 1)),
    fund_id text NOT NULL,
    series_id text,
    methodology_version text NOT NULL DEFAULT 'ncen_operating_profile_v1',
    measured_at date NOT NULL,
    service_providers_state text NOT NULL CHECK (service_providers_state IN ('available','unavailable','not_applicable')),
    service_providers_reason_code text,
    provider_children jsonb NOT NULL DEFAULT '[]'::jsonb,
    securities_lending_state text NOT NULL CHECK (securities_lending_state IN ('available','unavailable','not_applicable')),
    securities_lending_reason_code text,
    securities_lending jsonb,
    liquidity_backstop_state text NOT NULL CHECK (liquidity_backstop_state IN ('available','unavailable','not_applicable')),
    liquidity_backstop_reason_code text,
    liquidity_backstop jsonb,
    etf_primary_market_state text NOT NULL CHECK (etf_primary_market_state IN ('available','unavailable','not_applicable')),
    etf_primary_market_reason_code text,
    etf_primary_market jsonb,
    fund_structure_state text NOT NULL CHECK (fund_structure_state IN ('available','unavailable','not_applicable')),
    fund_structure_reason_code text,
    fund_structure jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, fund_id),
    CHECK (methodology_version = 'ncen_operating_profile_v1'),
    CHECK (jsonb_typeof(provider_children) = 'array'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((service_providers_state = 'available') = (jsonb_array_length(provider_children) > 0)),
    CHECK ((service_providers_state = 'available') = (service_providers_reason_code IS NULL)),
    CHECK ((securities_lending_state = 'available') = (securities_lending IS NOT NULL AND securities_lending_reason_code IS NULL)),
    CHECK ((liquidity_backstop_state = 'available') = (liquidity_backstop IS NOT NULL AND liquidity_backstop_reason_code IS NULL)),
    CHECK ((etf_primary_market_state = 'available') = (etf_primary_market IS NOT NULL AND etf_primary_market_reason_code IS NULL)),
    CHECK ((fund_structure_state = 'available') = (fund_structure IS NOT NULL AND fund_structure_reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS ncen_operating_profile_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ncen_operating_profile_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN operating-profile build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id
      AND product = 'ncen_operating_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN operating-profile build identity requires a prepared N-CEN operating-profile publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_operating_profile_build_guard ON ncen_operating_profile_builds;
CREATE TRIGGER ncen_operating_profile_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_operating_profile_builds
FOR EACH ROW EXECUTE FUNCTION ncen_operating_profile_build_guard();

CREATE OR REPLACE FUNCTION ncen_operating_profile_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN operating profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id
      AND product = 'ncen_operating_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN operating profile requires a prepared N-CEN operating-profile publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of
    FROM ncen_operating_profile_builds
    WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-CEN operating profile requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_operating_profile_write_guard ON ncen_operating_profiles;
CREATE TRIGGER ncen_operating_profile_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_operating_profiles
FOR EACH ROW EXECUTE FUNCTION ncen_operating_profile_write_guard();

CREATE OR REPLACE FUNCTION build_ncen_operating_profiles(
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
        RAISE EXCEPTION 'N-CEN operating-profile build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id
      AND product = 'ncen_operating_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN operating-profile build requires a prepared N-CEN operating-profile publication';
    END IF;

    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || effective_date::text || ':' || form,
                '|' ORDER BY ingestion_run_id, accession_number, effective_date, form
            ), '')) || md5('ncen_operating_profile_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM ncen_effective_filings
    WHERE effective_date <= as_of_date;

    -- Each effective filing must expose exactly one typed official FUND_ID
    -- parent for every profile source row.  No CIK-derived identifiers exist.
    IF EXISTS (
        SELECT 1
        FROM ncen_effective_filings e
        LEFT JOIN ncen_raw_v2_rows f
          ON f.ingestion_run_id = e.ingestion_run_id
         AND f.source_table = 'FUND_REPORTED_INFO.tsv'
         AND f.parse_status = 'typed'
         AND f.accession_number = e.accession_number
        WHERE e.effective_date <= as_of_date
        GROUP BY e.ingestion_run_id, e.accession_number
        HAVING count(f.raw_row_id) = 0
            OR count(f.raw_row_id) FILTER (WHERE NULLIF(btrim(f.fund_id), '') IS NULL) > 0
    ) THEN
        RAISE EXCEPTION 'missing N-CEN fund identity for effective filing';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM ncen_effective_filings e
        JOIN ncen_raw_v2_rows f
          ON f.ingestion_run_id = e.ingestion_run_id
         AND f.source_table = 'FUND_REPORTED_INFO.tsv'
         AND f.parse_status = 'typed'
         AND f.accession_number = e.accession_number
        WHERE e.effective_date <= as_of_date
        GROUP BY e.ingestion_run_id, e.accession_number, f.fund_id
        HAVING count(*) <> 1
    ) THEN
        RAISE EXCEPTION 'ambiguous N-CEN fund identity for effective filing';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM ncen_effective_filings e
        JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                               AND f.source_table='FUND_REPORTED_INFO.tsv'
                               AND f.parse_status='typed'
                               AND f.accession_number=e.accession_number
        JOIN ncen_raw_v2_rows d ON d.ingestion_run_id=e.ingestion_run_id
                               AND d.source_table='LINE_OF_CREDIT_DETAIL.tsv'
                               AND d.parse_status='typed'
                               AND d.fund_id=f.fund_id
        WHERE e.effective_date <= as_of_date
        GROUP BY d.ingestion_run_id, d.fund_id, d.typed_projection->>'LINE_OF_CREDIT_SEQNUM'
        HAVING count(*) <> 1 OR NULLIF(btrim(min(d.typed_projection->>'LINE_OF_CREDIT_SEQNUM')), '') IS NULL
    ) THEN
        RAISE EXCEPTION 'conflicting N-CEN credit-facility child key';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM ncen_effective_filings e
        JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                               AND f.source_table='FUND_REPORTED_INFO.tsv'
                               AND f.parse_status='typed'
                               AND f.accession_number=e.accession_number
        JOIN ncen_raw_v2_rows etf ON etf.ingestion_run_id=e.ingestion_run_id
                                 AND etf.source_table='ETF.tsv'
                                 AND etf.parse_status='typed'
                                 AND etf.fund_id=f.fund_id
        WHERE e.effective_date <= as_of_date
        GROUP BY etf.ingestion_run_id, etf.fund_id
        HAVING count(*) <> 1
    ) THEN
        RAISE EXCEPTION 'conflicting N-CEN ETF child key';
    END IF;

    INSERT INTO ncen_operating_profile_builds
        (publication_id, input_fingerprint, as_of_date, effective_input_count)
    VALUES (target_publication_id, computed_fingerprint, as_of_date, selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM ncen_operating_profile_builds b
    WHERE b.publication_id = target_publication_id
    FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-CEN operating-profile publication is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'N-CEN operating-profile publication is already pinned to effective-input fingerprint %', pinned_fingerprint;
    END IF;

    WITH selected_funds AS (
        SELECT e.ingestion_run_id AS source_run_id, e.accession_number, e.effective_date, e.form, e.is_amendment,
               e.registrant_cik, e.raw_row_id AS effective_raw_row_id,
               f.fund_id, f.typed_projection AS fund_evidence
        FROM ncen_effective_filings e
        JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                               AND f.source_table='FUND_REPORTED_INFO.tsv'
                               AND f.parse_status='typed'
                               AND f.accession_number=e.accession_number
        WHERE e.effective_date <= as_of_date
    )
    INSERT INTO ncen_operating_profiles(
        publication_id,source_run_id,accession_number,effective_date,form,is_amendment,fund_id,series_id,measured_at,
        service_providers_state,service_providers_reason_code,provider_children,
        securities_lending_state,securities_lending_reason_code,securities_lending,
        liquidity_backstop_state,liquidity_backstop_reason_code,liquidity_backstop,
        etf_primary_market_state,etf_primary_market_reason_code,etf_primary_market,
        fund_structure_state,fund_structure_reason_code,fund_structure,provenance,coverage
    )
    SELECT target_publication_id,s.source_run_id,s.accession_number,s.effective_date,s.form,s.is_amendment,
           s.fund_id,NULLIF(btrim(s.fund_evidence->>'SERIES_ID'),''),as_of_date,
           CASE WHEN jsonb_array_length(providers.children)>0 THEN 'available' ELSE 'unavailable' END,
           CASE WHEN jsonb_array_length(providers.children)>0 THEN NULL ELSE 'service_provider_not_reported' END,
           providers.children,
           CASE WHEN NULLIF(btrim(s.fund_evidence->>'IS_SEC_LENDING_AUTHORIZED'),'') IS NOT NULL THEN 'available' ELSE 'unavailable' END,
           CASE WHEN NULLIF(btrim(s.fund_evidence->>'IS_SEC_LENDING_AUTHORIZED'),'') IS NOT NULL THEN NULL ELSE 'securities_lending_not_reported' END,
           CASE WHEN NULLIF(btrim(s.fund_evidence->>'IS_SEC_LENDING_AUTHORIZED'),'') IS NOT NULL THEN
                jsonb_build_object('fund_evidence',jsonb_build_object(
                    'IS_SEC_LENDING_AUTHORIZED',s.fund_evidence->'IS_SEC_LENDING_AUTHORIZED',
                    'DID_LEND_SECURITIES',s.fund_evidence->'DID_LEND_SECURITIES',
                    'AVG_VALUE_SEC_LOAN',s.fund_evidence->'AVG_VALUE_SEC_LOAN',
                    'NET_INCOME_SEC_LENDING',s.fund_evidence->'NET_INCOME_SEC_LENDING'
                ),'indemnity_provider_children',lending.children) END,
           CASE WHEN NULLIF(btrim(s.fund_evidence->>'HAS_LINE_OF_CREDIT'),'') IS NOT NULL THEN 'available' ELSE 'unavailable' END,
           CASE WHEN NULLIF(btrim(s.fund_evidence->>'HAS_LINE_OF_CREDIT'),'') IS NOT NULL THEN NULL ELSE 'liquidity_backstop_not_reported' END,
           CASE WHEN NULLIF(btrim(s.fund_evidence->>'HAS_LINE_OF_CREDIT'),'') IS NOT NULL THEN
                jsonb_build_object('fund_evidence',jsonb_build_object('HAS_LINE_OF_CREDIT',s.fund_evidence->'HAS_LINE_OF_CREDIT'),
                                   'credit_facilities',credit.children,'credit_institutions',institutions.children) END,
           CASE WHEN jsonb_typeof(s.fund_evidence->'IS_ETF')='boolean' AND NOT (s.fund_evidence->>'IS_ETF')::boolean THEN 'not_applicable'
                WHEN jsonb_typeof(s.fund_evidence->'IS_ETF')='boolean' AND (s.fund_evidence->>'IS_ETF')::boolean
                     AND jsonb_array_length(etf.children)>0 THEN 'available'
                ELSE 'unavailable' END,
           CASE WHEN jsonb_typeof(s.fund_evidence->'IS_ETF')='boolean' AND (s.fund_evidence->>'IS_ETF')::boolean
                     AND jsonb_array_length(etf.children)>0 THEN NULL
                WHEN jsonb_typeof(s.fund_evidence->'IS_ETF')='boolean' AND NOT (s.fund_evidence->>'IS_ETF')::boolean THEN 'fund_not_reported_as_etf'
                WHEN jsonb_typeof(s.fund_evidence->'IS_ETF')='boolean' AND (s.fund_evidence->>'IS_ETF')::boolean THEN 'etf_primary_market_not_reported'
                ELSE 'etf_status_not_reported' END,
           CASE WHEN jsonb_typeof(s.fund_evidence->'IS_ETF')='boolean' AND (s.fund_evidence->>'IS_ETF')::boolean
                     AND jsonb_array_length(etf.children)>0 THEN
                jsonb_build_object('etf_children',etf.children,'authorized_participants',participants.children) END,
           CASE WHEN jsonb_typeof(s.fund_evidence->'IS_ETF')='boolean'
                      OR jsonb_typeof(s.fund_evidence->'IS_INTERVAL')='boolean'
                      OR jsonb_typeof(s.fund_evidence->'IS_SECONDARY_COMMON')='boolean' THEN 'available' ELSE 'unavailable' END,
           CASE WHEN jsonb_typeof(s.fund_evidence->'IS_ETF')='boolean'
                      OR jsonb_typeof(s.fund_evidence->'IS_INTERVAL')='boolean'
                      OR jsonb_typeof(s.fund_evidence->'IS_SECONDARY_COMMON')='boolean' THEN NULL ELSE 'fund_structure_not_reported' END,
           CASE WHEN jsonb_typeof(s.fund_evidence->'IS_ETF')='boolean'
                      OR jsonb_typeof(s.fund_evidence->'IS_INTERVAL')='boolean'
                      OR jsonb_typeof(s.fund_evidence->'IS_SECONDARY_COMMON')='boolean' THEN
                jsonb_build_object('IS_ETF',s.fund_evidence->'IS_ETF','IS_ETMF',s.fund_evidence->'IS_ETMF',
                                   'IS_INTERVAL',s.fund_evidence->'IS_INTERVAL','IS_FUND_OF_FUND',s.fund_evidence->'IS_FUND_OF_FUND',
                                   'IS_MASTER_FEEDER',s.fund_evidence->'IS_MASTER_FEEDER','IS_MONEY_MARKET',s.fund_evidence->'IS_MONEY_MARKET',
                                   'IS_SECONDARY_COMMON',s.fund_evidence->'IS_SECONDARY_COMMON',
                                   'IS_SECONDARY_PREFERRED',s.fund_evidence->'IS_SECONDARY_PREFERRED') END,
           jsonb_build_object('effective_selection_view','ncen_effective_filings','effective_raw_row_id',s.effective_raw_row_id,
                              'registrant_cik',s.registrant_cik,'source_run_id',s.source_run_id,'accession_number',s.accession_number,
                              'fund_source_table','FUND_REPORTED_INFO.tsv'),
           jsonb_build_object('provider_child_count',jsonb_array_length(providers.children),
                              'credit_facility_child_count',jsonb_array_length(credit.children),
                              'etf_child_count',jsonb_array_length(etf.children),
                              'authorized_participant_child_count',jsonb_array_length(participants.children))
    FROM selected_funds s
    CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(jsonb_build_object('source_table',r.source_table,'raw_row_id',r.raw_row_id,'evidence',r.typed_projection)
                                       ORDER BY r.source_table,r.raw_row_id),'[]'::jsonb) AS children
        FROM ncen_raw_v2_rows r WHERE r.ingestion_run_id=s.source_run_id AND r.fund_id=s.fund_id
          AND r.parse_status='typed' AND r.source_table IN ('ADVISER.tsv','ADMIN.tsv','SEC_LENDING_IDEMNITY_PROVIDER.tsv')
    ) providers
    CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(jsonb_build_object('source_table',r.source_table,'raw_row_id',r.raw_row_id,'evidence',r.typed_projection)
                                       ORDER BY r.raw_row_id),'[]'::jsonb) AS children
        FROM ncen_raw_v2_rows r WHERE r.ingestion_run_id=s.source_run_id AND r.fund_id=s.fund_id
          AND r.parse_status='typed' AND r.source_table='SEC_LENDING_IDEMNITY_PROVIDER.tsv'
    ) lending
    CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(jsonb_build_object('source_table',r.source_table,'raw_row_id',r.raw_row_id,'evidence',r.typed_projection)
                                       ORDER BY r.raw_row_id),'[]'::jsonb) AS children
        FROM ncen_raw_v2_rows r WHERE r.ingestion_run_id=s.source_run_id AND r.fund_id=s.fund_id
          AND r.parse_status='typed' AND r.source_table='LINE_OF_CREDIT_DETAIL.tsv'
    ) credit
    CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(jsonb_build_object('source_table',r.source_table,'raw_row_id',r.raw_row_id,'evidence',r.typed_projection)
                                       ORDER BY r.raw_row_id),'[]'::jsonb) AS children
        FROM ncen_raw_v2_rows r WHERE r.ingestion_run_id=s.source_run_id AND r.fund_id=s.fund_id
          AND r.parse_status='typed' AND r.source_table='LINE_OF_CREDIT_INSTITUTION.tsv'
    ) institutions
    CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(jsonb_build_object('source_table',r.source_table,'raw_row_id',r.raw_row_id,'evidence',r.typed_projection)
                                       ORDER BY r.raw_row_id),'[]'::jsonb) AS children
        FROM ncen_raw_v2_rows r WHERE r.ingestion_run_id=s.source_run_id AND r.fund_id=s.fund_id
          AND r.parse_status='typed' AND r.source_table='ETF.tsv'
    ) etf
    CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(jsonb_build_object('source_table',r.source_table,'raw_row_id',r.raw_row_id,'evidence',r.typed_projection)
                                       ORDER BY r.raw_row_id),'[]'::jsonb) AS children
        FROM ncen_raw_v2_rows r WHERE r.ingestion_run_id=s.source_run_id AND r.fund_id=s.fund_id
          AND r.parse_status='typed' AND r.source_table='AUTHORIZED_PARTICIPANT.tsv'
    ) participants
    ON CONFLICT (publication_id,source_run_id,accession_number,fund_id) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_ncen_operating_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN ncen_operating_profiles p ON p.publication_id=c.publication_id
WHERE c.product='ncen_operating_profile_v1';
