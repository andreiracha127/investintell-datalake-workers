-- Immutable N-CEN securities-lending profiles.  Grain is one row per
-- (publication, source run, accession, FUND_ID).  Families: authorization,
-- realized activity, average value on loan, net income, agent/indemnity/
-- collateral-manager relations, collateral liquidation, adverse impact.  Agent,
-- indemnity-provider, and collateral-manager children are PRE-AGGREGATED at their
-- own grain before being folded onto the fund (Global Constraint 4); a guard
-- fails closed if a fold ever multiplied the fund grain.  When lending is not
-- authorized the whole family is not_applicable -- net income and value on loan
-- are NEVER published as a synthetic zero; an authorized-but-inactive fund keeps
-- those metrics absent (NULL), not zero.

CREATE TABLE IF NOT EXISTS ncen_securities_lending_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    effective_date date NOT NULL,
    form text NOT NULL,
    is_amendment integer NOT NULL CHECK (is_amendment IN (0, 1)),
    registrant_cik text NOT NULL,
    fund_id text NOT NULL,
    series_id text,
    methodology_version text NOT NULL DEFAULT 'ncen_securities_lending_v1',
    measured_at date NOT NULL,
    securities_lending_state text NOT NULL CHECK (securities_lending_state IN ('available','unavailable','not_applicable')),
    securities_lending_reason_code text,
    securities_lending jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, fund_id),
    CHECK (methodology_version = 'ncen_securities_lending_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((securities_lending_state = 'available') = (securities_lending IS NOT NULL AND securities_lending_reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS ncen_securities_lending_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ncen_securities_lending_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN securities-lending build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_securities_lending_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN securities-lending build identity requires a prepared securities-lending publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_securities_lending_build_guard ON ncen_securities_lending_builds;
CREATE TRIGGER ncen_securities_lending_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_securities_lending_builds
FOR EACH ROW EXECUTE FUNCTION ncen_securities_lending_build_guard();

CREATE OR REPLACE FUNCTION ncen_securities_lending_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN securities lending is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_securities_lending_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN securities lending requires a prepared securities-lending publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM ncen_securities_lending_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-CEN securities lending requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_securities_lending_write_guard ON ncen_securities_lending_profiles;
CREATE TRIGGER ncen_securities_lending_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_securities_lending_profiles
FOR EACH ROW EXECUTE FUNCTION ncen_securities_lending_write_guard();

CREATE OR REPLACE FUNCTION build_ncen_securities_lending_profiles(
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
        RAISE EXCEPTION 'N-CEN securities-lending build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'ncen_securities_lending_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN securities-lending build requires a prepared securities-lending publication';
    END IF;

    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || effective_date::text || ':' || form,
                '|' ORDER BY ingestion_run_id, accession_number, effective_date, form
            ), '')) || md5('ncen_securities_lending_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM ncen_effective_filings
    WHERE effective_date <= as_of_date;

    PERFORM ncen_assert_effective_fund_identity(as_of_date);

    INSERT INTO ncen_securities_lending_builds
        (publication_id, input_fingerprint, as_of_date, effective_input_count)
    VALUES (target_publication_id, computed_fingerprint, as_of_date, selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM ncen_securities_lending_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-CEN securities-lending publication is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'N-CEN securities-lending publication is already pinned to effective-input fingerprint %', pinned_fingerprint;
    END IF;

    DROP TABLE IF EXISTS _ncen_sl_sf;
    DROP TABLE IF EXISTS _ncen_sl_children;
    DROP TABLE IF EXISTS _ncen_sl_relations;

    CREATE TEMP TABLE _ncen_sl_sf ON COMMIT DROP AS
    SELECT e.ingestion_run_id AS source_run_id, e.accession_number, e.effective_date, e.form, e.is_amendment,
           e.registrant_cik, e.raw_row_id AS submission_raw_row_id,
           f.fund_id, f.raw_row_id AS fund_raw_row_id, f.typed_projection AS fund_evidence,
           NULLIF(btrim(f.typed_projection->>'SERIES_ID'),'') AS series_id,
           ncen_tristate_flag(f.typed_projection->>'IS_SEC_LENDING_AUTHORIZED') AS authorization,
           NULLIF(btrim(f.typed_projection->>'AVG_VALUE_SEC_LOAN'),'')::numeric AS average_on_loan,
           NULLIF(btrim(f.typed_projection->>'NET_INCOME_SEC_LENDING'),'')::numeric AS net_income,
           NULLIF(btrim(f.typed_projection->>'MONTHLY_AVG_NET_ASSETS'),'')::numeric AS monthly_avg_net_assets
    FROM ncen_effective_filings e
    JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                           AND f.source_table='FUND_REPORTED_INFO.tsv'
                           AND f.parse_status='typed'
                           AND f.accession_number=e.accession_number
    WHERE e.effective_date <= as_of_date;

    -- Lending relation children pre-aggregated at their own grain (agent per
    -- SECURITY_LENDING_SEQNUM, indemnity provider per sequence, collateral
    -- manager per fund).  Each candidate carries the raw row it came from.
    CREATE TEMP TABLE _ncen_sl_children ON COMMIT DROP AS
    WITH candidates AS (
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'agent' AS relation,
               ncen_provider_identity(r.typed_projection->>'SECURITIES_AGENT_LEI', NULL, NULL, r.typed_projection->>'SECURITIES_AGENT_NAME') AS identity,
               lower(btrim(r.typed_projection->>'SECURITIES_AGENT_NAME')) AS display_name,
               jsonb_build_object(
                   'seqnum', NULLIF(btrim(r.typed_projection->>'SECURITY_LENDING_SEQNUM'),''),
                   'is_affiliated', ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'),
                   'provides_indemnity', ncen_tristate_flag(r.typed_projection->>'SECURITY_AGENT_IDEMNITY'),
                   'has_indemnification_rights', ncen_tristate_flag(r.typed_projection->>'DID_INDEMNIFICATION_RIGHTS')) AS attributes,
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED') AS is_affiliated,
               r.source_table, r.raw_row_id
        FROM _ncen_sl_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='SECURITY_LENDING.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'indemnity_provider',
               ncen_provider_identity(r.typed_projection->>'INDEMNITY_PROVIDER_LEI', NULL, NULL, r.typed_projection->>'INDEMNITY_PROVIDER_NAME'),
               lower(btrim(r.typed_projection->>'INDEMNITY_PROVIDER_NAME')),
               jsonb_build_object('seqnum', NULLIF(btrim(r.typed_projection->>'SECURITY_LENDING_SEQNUM'),'')),
               'not_reported', r.source_table, r.raw_row_id
        FROM _ncen_sl_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='SEC_LENDING_IDEMNITY_PROVIDER.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'collateral_manager',
               ncen_provider_identity(r.typed_projection->>'COLLATERAL_MANAGER_LEI', NULL, NULL, r.typed_projection->>'COLLATERAL_MANAGER_NAME'),
               lower(btrim(r.typed_projection->>'COLLATERAL_MANAGER_NAME')),
               jsonb_build_object(
                   'is_affiliated', ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'),
                   'is_affiliated_with_fund', ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED_WITH_FUND')),
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'),
               r.source_table, r.raw_row_id
        FROM _ncen_sl_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='COLLATERAL_MANAGER.tsv' AND r.parse_status='typed'
    )
    SELECT source_run_id, accession_number, fund_id, relation,
           identity->>'identifier_kind' AS identifier_kind, identity->>'identifier_value' AS identifier_value,
           display_name, attributes, is_affiliated, source_table, raw_row_id
    FROM candidates
    WHERE identity IS NOT NULL;

    -- Hard failure if pre-aggregation ever multiplied the fund grain.
    IF EXISTS (
        SELECT 1 FROM _ncen_sl_children
        GROUP BY source_run_id, accession_number, fund_id, source_table, raw_row_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'N-CEN securities lending row multiplication detected';
    END IF;

    CREATE TEMP TABLE _ncen_sl_relations ON COMMIT DROP AS
    SELECT source_run_id, accession_number, fund_id,
           COALESCE(jsonb_agg(jsonb_build_object(
               'identifier_kind', identifier_kind, 'identifier_value', identifier_value,
               'display_name', display_name, 'is_affiliated', is_affiliated, 'attributes', attributes)
               ORDER BY relation, raw_row_id) FILTER (WHERE relation='agent'), '[]'::jsonb) AS agents,
           COALESCE(jsonb_agg(jsonb_build_object(
               'identifier_kind', identifier_kind, 'identifier_value', identifier_value,
               'display_name', display_name, 'attributes', attributes)
               ORDER BY relation, raw_row_id) FILTER (WHERE relation='indemnity_provider'), '[]'::jsonb) AS indemnity_providers,
           COALESCE(jsonb_agg(jsonb_build_object(
               'identifier_kind', identifier_kind, 'identifier_value', identifier_value,
               'display_name', display_name, 'is_affiliated', is_affiliated, 'attributes', attributes)
               ORDER BY relation, raw_row_id) FILTER (WHERE relation='collateral_manager'), '[]'::jsonb) AS collateral_managers,
           count(*) FILTER (WHERE relation='agent') AS agent_count,
           count(*) FILTER (WHERE relation='indemnity_provider') AS indemnity_provider_count,
           count(*) FILTER (WHERE relation='collateral_manager') AS collateral_manager_count
    FROM _ncen_sl_children
    GROUP BY source_run_id, accession_number, fund_id;

    INSERT INTO ncen_securities_lending_profiles(
        publication_id,source_run_id,accession_number,effective_date,form,is_amendment,registrant_cik,fund_id,series_id,
        measured_at,securities_lending_state,securities_lending_reason_code,securities_lending,provenance,coverage
    )
    SELECT target_publication_id, sf.source_run_id, sf.accession_number, sf.effective_date, sf.form, sf.is_amendment,
           sf.registrant_cik, sf.fund_id, sf.series_id, as_of_date,
           CASE sf.authorization WHEN 'true' THEN 'available' WHEN 'false' THEN 'not_applicable' ELSE 'unavailable' END,
           CASE sf.authorization WHEN 'true' THEN NULL WHEN 'false' THEN 'securities_lending_not_authorized'
                ELSE 'securities_lending_authorization_not_reported' END,
           CASE WHEN sf.authorization = 'true' THEN jsonb_build_object(
               'authorization', sf.authorization,
               'activity', ncen_tristate_flag(sf.fund_evidence->>'DID_LEND_SECURITIES'),
               'average_on_loan', sf.average_on_loan,
               'net_income', sf.net_income,
               'collateral_liquidation', ncen_tristate_flag(sf.fund_evidence->>'IS_COLLATERAL_LIQUIDATED'),
               'adverse_impact', ncen_tristate_flag(sf.fund_evidence->>'IS_IMPACTED_ADVERSELY'),
               'cash_collateral_fee_paid', ncen_tristate_flag(sf.fund_evidence->>'IS_PYMNT_CASH_COLLATERAL_FEE'),
               'indemnification_fee_paid', ncen_tristate_flag(sf.fund_evidence->>'IS_PYMNT_INDEMNI_FEE'),
               'agents', COALESCE(rel.agents, '[]'::jsonb),
               'indemnity_providers', COALESCE(rel.indemnity_providers, '[]'::jsonb),
               'collateral_managers', COALESCE(rel.collateral_managers, '[]'::jsonb),
               'derived', jsonb_build_object(
                    'lending_yield', ncen_safe_ratio(sf.net_income, sf.average_on_loan),
                    'lending_intensity', ncen_safe_ratio(sf.average_on_loan, sf.monthly_avg_net_assets),
                    'agent_count', COALESCE(rel.agent_count, 0),
                    'indemnity_provider_count', COALESCE(rel.indemnity_provider_count, 0),
                    'collateral_manager_count', COALESCE(rel.collateral_manager_count, 0))
           ) END,
           jsonb_build_object('effective_selection_view','ncen_effective_filings','registrant_cik',sf.registrant_cik,
                              'submission_raw_row_id',sf.submission_raw_row_id,'fund_raw_row_id',sf.fund_raw_row_id,
                              'source_run_id',sf.source_run_id,'accession_number',sf.accession_number,
                              'fund_source_table','FUND_REPORTED_INFO.tsv'),
           jsonb_build_object('authorization', sf.authorization,
                              'agent_count', COALESCE(rel.agent_count, 0),
                              'indemnity_provider_count', COALESCE(rel.indemnity_provider_count, 0),
                              'collateral_manager_count', COALESCE(rel.collateral_manager_count, 0))
    FROM _ncen_sl_sf sf
    LEFT JOIN _ncen_sl_relations rel USING (source_run_id, accession_number, fund_id)
    ON CONFLICT (publication_id,source_run_id,accession_number,fund_id) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_ncen_securities_lending_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN ncen_securities_lending_profiles p ON p.publication_id=c.publication_id
WHERE c.product='ncen_securities_lending_v1';
