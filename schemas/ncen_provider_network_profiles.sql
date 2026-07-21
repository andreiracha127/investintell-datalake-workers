-- Immutable N-CEN provider-network profiles.  Grain is one row per
-- (publication, source run, accession, FUND_ID).  Provider children (adviser,
-- subadviser, custodian, pricing service, transfer agent, administrator,
-- shareholder-servicing agent, public accountant, underwriter, broker,
-- collateral manager, securities-lending agent) are PRE-AGGREGATED at their own
-- grain before being folded onto the fund (Global Constraint 4); a one-to-many
-- provider join can never multiply the fund grain, and a guard fails closed if
-- it ever did.  Identifiers normalize LEI/CRD/PCAOB-first with name as fallback.

CREATE TABLE IF NOT EXISTS ncen_provider_network_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    effective_date date NOT NULL,
    form text NOT NULL,
    is_amendment integer NOT NULL CHECK (is_amendment IN (0, 1)),
    registrant_cik text NOT NULL,
    fund_id text NOT NULL,
    series_id text,
    methodology_version text NOT NULL DEFAULT 'ncen_provider_network_v1',
    measured_at date NOT NULL,
    provider_network_state text NOT NULL CHECK (provider_network_state IN ('available','unavailable','not_applicable')),
    provider_network_reason_code text,
    provider_network jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, fund_id),
    CHECK (methodology_version = 'ncen_provider_network_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((provider_network_state = 'available') = (provider_network IS NOT NULL AND provider_network_reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS ncen_provider_network_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ncen_provider_network_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN provider-network build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_provider_network_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN provider-network build identity requires a prepared provider-network publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_provider_network_build_guard ON ncen_provider_network_builds;
CREATE TRIGGER ncen_provider_network_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_provider_network_builds
FOR EACH ROW EXECUTE FUNCTION ncen_provider_network_build_guard();

CREATE OR REPLACE FUNCTION ncen_provider_network_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN provider network is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_provider_network_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN provider network requires a prepared provider-network publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM ncen_provider_network_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-CEN provider network requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_provider_network_write_guard ON ncen_provider_network_profiles;
CREATE TRIGGER ncen_provider_network_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_provider_network_profiles
FOR EACH ROW EXECUTE FUNCTION ncen_provider_network_write_guard();

CREATE OR REPLACE FUNCTION build_ncen_provider_network_profiles(
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
        RAISE EXCEPTION 'N-CEN provider-network build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'ncen_provider_network_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN provider-network build requires a prepared provider-network publication';
    END IF;

    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || effective_date::text || ':' || form,
                '|' ORDER BY ingestion_run_id, accession_number, effective_date, form
            ), '')) || md5('ncen_provider_network_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM ncen_effective_filings
    WHERE effective_date <= as_of_date;

    PERFORM ncen_assert_effective_fund_identity(as_of_date);

    INSERT INTO ncen_provider_network_builds
        (publication_id, input_fingerprint, as_of_date, effective_input_count)
    VALUES (target_publication_id, computed_fingerprint, as_of_date, selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM ncen_provider_network_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-CEN provider-network publication is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'N-CEN provider-network publication is already pinned to effective-input fingerprint %', pinned_fingerprint;
    END IF;

    DROP TABLE IF EXISTS _ncen_pn_sf;
    DROP TABLE IF EXISTS _ncen_pn_children;
    DROP TABLE IF EXISTS _ncen_pn_metrics;

    -- Selected funds (one row per fund inside an effective filing).
    CREATE TEMP TABLE _ncen_pn_sf ON COMMIT DROP AS
    SELECT e.ingestion_run_id AS source_run_id, e.accession_number, e.effective_date, e.form, e.is_amendment,
           e.registrant_cik, f.fund_id, NULLIF(btrim(f.typed_projection->>'SERIES_ID'),'') AS series_id
    FROM ncen_effective_filings e
    JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                           AND f.source_table='FUND_REPORTED_INFO.tsv'
                           AND f.parse_status='typed'
                           AND f.accession_number=e.accession_number
    WHERE e.effective_date <= as_of_date;

    -- Provider children pre-aggregated at their own (fund x provider) grain.
    -- Fund-level roles resolve on FUND_ID; registrant-level roles (public
    -- accountant, underwriter) resolve on ACCESSION_NUMBER and attach to every
    -- fund of the filing as an aggregated array -- never a multiplying row join.
    CREATE TEMP TABLE _ncen_pn_children ON COMMIT DROP AS
    WITH candidates AS (
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id,
               CASE WHEN upper(COALESCE(r.typed_projection->>'ADVISER_TYPE','')) IN ('SUB-ADVISER','SUBADVISER','SUB ADVISER')
                    THEN 'subadviser' ELSE 'adviser' END AS role,
               ncen_provider_identity(r.typed_projection->>'ADVISER_LEI', r.typed_projection->>'CRD_NUM', NULL, r.typed_projection->>'ADVISER_NAME') AS identity,
               lower(btrim(r.typed_projection->>'ADVISER_NAME')) AS display_name,
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED') AS is_affiliated,
               r.source_table, r.raw_row_id, r.typed_projection AS evidence
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='ADVISER.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'custodian',
               ncen_provider_identity(r.typed_projection->>'CUSTODIAN_LEI', NULL, NULL, r.typed_projection->>'CUSTODIAN_NAME'),
               lower(btrim(r.typed_projection->>'CUSTODIAN_NAME')),
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'), r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='CUSTODIAN.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'pricing_service',
               ncen_provider_identity(r.typed_projection->>'PRICING_SERVICE_LEI', NULL, NULL, r.typed_projection->>'PRICING_SERVICE_NAME'),
               lower(btrim(r.typed_projection->>'PRICING_SERVICE_NAME')),
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'), r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='PRICING_SERVICE.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'transfer_agent',
               ncen_provider_identity(r.typed_projection->>'TRANSFERAGENT_LEI', NULL, NULL, r.typed_projection->>'TRANSFERAGENT_NAME'),
               lower(btrim(r.typed_projection->>'TRANSFERAGENT_NAME')),
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'), r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='TRANSFER_AGENT.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'administrator',
               ncen_provider_identity(r.typed_projection->>'ADMIN_LEI', NULL, NULL, r.typed_projection->>'ADMIN_NAME'),
               lower(btrim(r.typed_projection->>'ADMIN_NAME')),
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'), r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='ADMIN.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'shareholder_servicing',
               ncen_provider_identity(r.typed_projection->>'AGENT_LEI', NULL, NULL, r.typed_projection->>'AGENT_NAME'),
               lower(btrim(r.typed_projection->>'AGENT_NAME')),
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'), r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='SHAREHOLDER_SERVICING_AGENT.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'broker',
               ncen_provider_identity(r.typed_projection->>'BROKER_LEI', r.typed_projection->>'CRD_NUM', NULL, r.typed_projection->>'BROKER_NAME'),
               lower(btrim(r.typed_projection->>'BROKER_NAME')),
               'not_reported', r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='BROKER.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'collateral_manager',
               ncen_provider_identity(r.typed_projection->>'COLLATERAL_MANAGER_LEI', NULL, NULL, r.typed_projection->>'COLLATERAL_MANAGER_NAME'),
               lower(btrim(r.typed_projection->>'COLLATERAL_MANAGER_NAME')),
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'), r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='COLLATERAL_MANAGER.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'securities_lending_agent',
               ncen_provider_identity(r.typed_projection->>'SECURITIES_AGENT_LEI', NULL, NULL, r.typed_projection->>'SECURITIES_AGENT_NAME'),
               lower(btrim(r.typed_projection->>'SECURITIES_AGENT_NAME')),
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'), r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='SECURITY_LENDING.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'public_accountant',
               ncen_provider_identity(r.typed_projection->>'PUB_ACCOUNTANT_LEI', NULL, r.typed_projection->>'PCAOB_NUM', r.typed_projection->>'PUB_ACCOUNTANT_NAME'),
               lower(btrim(r.typed_projection->>'PUB_ACCOUNTANT_NAME')),
               'not_reported', r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.accession_number=sf.accession_number
         AND r.source_table='PUBLIC_ACCOUNTANT.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'underwriter',
               ncen_provider_identity(r.typed_projection->>'UNDERWRITER_LEI', r.typed_projection->>'CRD_NUM', NULL, r.typed_projection->>'UNDERWRITER_NAME'),
               lower(btrim(r.typed_projection->>'UNDERWRITER_NAME')),
               ncen_tristate_flag(r.typed_projection->>'IS_AFFILIATED'), r.source_table, r.raw_row_id, r.typed_projection
        FROM _ncen_pn_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.accession_number=sf.accession_number
         AND r.source_table='PRINCIPAL_UNDERWRITER.tsv' AND r.parse_status='typed'
    )
    SELECT source_run_id, accession_number, fund_id, role,
           identity->>'identifier_kind' AS identifier_kind,
           identity->>'identifier_value' AS identifier_value,
           display_name, is_affiliated, source_table, raw_row_id, evidence
    FROM candidates
    WHERE identity IS NOT NULL;

    -- Hard failure if pre-aggregation ever multiplied the fund grain: no
    -- provider child may appear twice under one fund.
    IF EXISTS (
        SELECT 1 FROM _ncen_pn_children
        GROUP BY source_run_id, accession_number, fund_id, source_table, raw_row_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'N-CEN provider network row multiplication detected';
    END IF;

    CREATE TEMP TABLE _ncen_pn_metrics ON COMMIT DROP AS
    WITH entity_counts AS (
        SELECT source_run_id, accession_number, fund_id, identifier_value, count(*) AS c
        FROM _ncen_pn_children GROUP BY 1,2,3,4
    ), conflicts AS (
        SELECT source_run_id, accession_number, fund_id,
               jsonb_agg(jsonb_build_object('conflict_kind',kind,'key',key) ORDER BY kind, key) AS evidence,
               count(*) AS n
        FROM (
            SELECT source_run_id, accession_number, fund_id,
                   'display_name_maps_to_multiple_identifiers' AS kind, display_name AS key
            FROM _ncen_pn_children GROUP BY 1,2,3, display_name HAVING count(DISTINCT identifier_value) > 1
            UNION ALL
            SELECT source_run_id, accession_number, fund_id,
                   'identifier_maps_to_multiple_display_names', identifier_value
            FROM _ncen_pn_children GROUP BY 1,2,3, identifier_value HAVING count(DISTINCT display_name) > 1
        ) u GROUP BY 1,2,3
    ), hhi AS (
        SELECT source_run_id, accession_number, fund_id,
               round(sum(power(c::numeric,2)) / power(sum(c),2), 6) AS concentration_hhi
        FROM entity_counts GROUP BY 1,2,3
    ), agg AS (
        SELECT source_run_id, accession_number, fund_id,
               jsonb_agg(jsonb_build_object('role',role,'identifier_kind',identifier_kind,
                    'identifier_value',identifier_value,'display_name',display_name,
                    'is_affiliated',is_affiliated,'source_table',source_table,
                    'raw_row_id',raw_row_id,'evidence',evidence)
                    ORDER BY role, source_table, raw_row_id) AS providers,
               jsonb_agg(DISTINCT role) AS roles,
               count(*) AS total,
               count(*) FILTER (WHERE is_affiliated='true') AS affiliated,
               count(DISTINCT identifier_value) AS distinct_entities
        FROM _ncen_pn_children GROUP BY 1,2,3
    )
    SELECT a.source_run_id, a.accession_number, a.fund_id, a.providers, a.roles, a.total,
           a.affiliated, a.distinct_entities, h.concentration_hhi,
           COALESCE(c.evidence,'[]'::jsonb) AS conflict_evidence, COALESCE(c.n,0) AS conflict_count
    FROM agg a
    JOIN hhi h USING (source_run_id, accession_number, fund_id)
    LEFT JOIN conflicts c USING (source_run_id, accession_number, fund_id);

    INSERT INTO ncen_provider_network_profiles(
        publication_id,source_run_id,accession_number,effective_date,form,is_amendment,registrant_cik,fund_id,series_id,
        measured_at,provider_network_state,provider_network_reason_code,provider_network,provenance,coverage
    )
    SELECT target_publication_id, sf.source_run_id, sf.accession_number, sf.effective_date, sf.form, sf.is_amendment,
           sf.registrant_cik, sf.fund_id, sf.series_id, as_of_date,
           CASE WHEN COALESCE(m.total,0) > 0 THEN 'available' ELSE 'unavailable' END,
           CASE WHEN COALESCE(m.total,0) > 0 THEN NULL ELSE 'provider_network_not_reported' END,
           CASE WHEN COALESCE(m.total,0) > 0 THEN jsonb_build_object(
                'providers', m.providers,
                'derived', jsonb_build_object(
                    'total_provider_count', m.total,
                    'affiliated_provider_count', m.affiliated,
                    'affiliated_service_exposure', round(m.affiliated::numeric / m.total, 6),
                    'distinct_entity_count', m.distinct_entities,
                    'provider_concentration_hhi', m.concentration_hhi,
                    'identifier_conflict_count', m.conflict_count
                ),
                'identifier_conflicts', m.conflict_evidence
           ) END,
           jsonb_build_object('effective_selection_view','ncen_effective_filings','registrant_cik',sf.registrant_cik,
                              'source_run_id',sf.source_run_id,'accession_number',sf.accession_number,
                              'fund_source_table','FUND_REPORTED_INFO.tsv'),
           jsonb_build_object('provider_child_count',COALESCE(m.total,0),
                              'distinct_entity_count',COALESCE(m.distinct_entities,0),
                              'roles_present',COALESCE(m.roles,'[]'::jsonb))
    FROM _ncen_pn_sf sf
    LEFT JOIN _ncen_pn_metrics m USING (source_run_id, accession_number, fund_id)
    ON CONFLICT (publication_id,source_run_id,accession_number,fund_id) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_ncen_provider_network_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN ncen_provider_network_profiles p ON p.publication_id=c.publication_id
WHERE c.product='ncen_provider_network_v1';
