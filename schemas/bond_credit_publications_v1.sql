-- bond_credit_publications_v1: immutable bond-credit publication ledger, many-source
-- lineage, singleton current pointer, append-only revocations, and the
-- validate / promote / revoke / read functions (implementation plan section 3.3).
--
-- Reuses the protocol semantics of sec_derived_publications (prepared ->
-- validated -> atomic pointer) without its storage: no fake SEC run/package
-- foreign key. Apply after bond_default_sources_v1 with
-- `SET search_path TO <schema>, pg_temp`; bond_credit_validate reads the event
-- and rating tables installed by the next two files (late-bound PL/pgSQL).
-- bond_credit_validate recomputes every row hash (bond_credit_row_v1 encoding), frame
-- digest, the validation-receipt digest, the fingerprint and the UUIDv8 publication id
-- from stored columns with the built-in sha256(); supplied hashes are never trusted.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

DO $$
BEGIN
    IF pg_catalog.to_regclass('bond_default_source_package') IS NULL THEN
        RAISE EXCEPTION 'bond_credit_publications_v1: apply bond_default_sources_v1 first (same schema)';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_frame_digest(hashes text[]) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT 'sha256:' || pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
        COALESCE((SELECT pg_catalog.string_agg(s.h, E'\n' ORDER BY s.h)
                  FROM (SELECT h COLLATE "C" AS h FROM pg_catalog.unnest(hashes) h) s), ''),
        'UTF8')), 'hex')
$$;

CREATE OR REPLACE FUNCTION bond_credit_id_digest(ids uuid[]) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT bond_credit_frame_digest(ARRAY(SELECT u::text FROM pg_catalog.unnest(ids) u))
$$;

-- ---------------------------------------------------------------------------
-- Row encoding bond_credit_row_v1 (byte-identical to contracts.row_encoding).
-- Fields in fixed column order; each value length-prefixed and type-tagged:
-- null=N, string=S<utf8 bytes>:<text>, integer=I<len>:<decimal>,
-- boolean=B<len>:true|false, array=A<count>:<elements>, object=M<count>:<S-key><value>
-- with keys in UTF-8 byte order. timestamptz columns ('t:' spec entries) are encoded
-- as S of UTC YYYY-MM-DDTHH:MM:SS.ffffffZ; dates arrive as ISO text from to_jsonb.
-- Stored row_sha256 values are never trusted: validation recomputes them.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_credit_encoded_scalar(tag text, body text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT tag || pg_catalog.octet_length(pg_catalog.convert_to(body, 'UTF8'))::text || ':' || body
$$;

CREATE OR REPLACE FUNCTION bond_credit_value_encoding(v jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
DECLARE
    kind text := pg_catalog.jsonb_typeof(v);
BEGIN
    IF v IS NULL OR kind = 'null' THEN
        RETURN 'N';
    ELSIF kind = 'string' THEN
        RETURN bond_credit_encoded_scalar('S', v #>> '{}');
    ELSIF kind = 'number' THEN
        IF (v #>> '{}') !~ '^-?(0|[1-9][0-9]*)$' THEN
            RAISE EXCEPTION 'bond_credit_row_encoding:non_integer_number';
        END IF;
        RETURN bond_credit_encoded_scalar('I', v #>> '{}');
    ELSIF kind = 'boolean' THEN
        RETURN bond_credit_encoded_scalar('B', v #>> '{}');
    ELSIF kind = 'array' THEN
        RETURN 'A' || pg_catalog.jsonb_array_length(v)::text || ':'
            || COALESCE((SELECT pg_catalog.string_agg(bond_credit_value_encoding(e.value), '' ORDER BY e.ordinality)
                         FROM pg_catalog.jsonb_array_elements(v) WITH ORDINALITY AS e(value, ordinality)), '');
    END IF;
    RETURN 'M' || (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(v))::text || ':'
        || COALESCE((SELECT pg_catalog.string_agg(bond_credit_encoded_scalar('S', e.key) || bond_credit_value_encoding(e.value),
                                                  '' ORDER BY e.key COLLATE "C")
                     FROM pg_catalog.jsonb_each(v) AS e(key, value)), '');
END $$;

-- Canonical JSON used by Python ``digest_of``: UTF-8, sorted object keys, compact separators,
-- array order preserved, and integer-only numbers. This is distinct from row-value encoding.
CREATE OR REPLACE FUNCTION bond_credit_canonical_json(v jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
DECLARE
    kind text := pg_catalog.jsonb_typeof(v);
BEGIN
    IF v IS NULL OR kind = 'null' THEN
        RETURN 'null';
    ELSIF kind = 'string' THEN
        RETURN pg_catalog.to_jsonb(v #>> '{}')::text;
    ELSIF kind = 'number' THEN
        IF (v #>> '{}') !~ '^-?(0|[1-9][0-9]*)$' THEN
            RAISE EXCEPTION 'bond_credit_canonical_json:non_integer_number';
        END IF;
        RETURN v #>> '{}';
    ELSIF kind = 'boolean' THEN
        RETURN v #>> '{}';
    ELSIF kind = 'array' THEN
        RETURN '[' || COALESCE((
            SELECT pg_catalog.string_agg(bond_credit_canonical_json(e.value), ',' ORDER BY e.ordinality)
            FROM pg_catalog.jsonb_array_elements(v) WITH ORDINALITY AS e(value, ordinality)
        ), '') || ']';
    ELSIF kind = 'object' THEN
        RETURN '{' || COALESCE((
            SELECT pg_catalog.string_agg(
                pg_catalog.to_jsonb(e.key)::text || ':' || bond_credit_canonical_json(e.value),
                ',' ORDER BY e.key COLLATE "C")
            FROM pg_catalog.jsonb_each(v) AS e(key, value)
        ), '') || '}';
    END IF;
    RAISE EXCEPTION 'bond_credit_canonical_json:unsupported_type:%', kind;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_json_digest(v jsonb) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT 'sha256:' || pg_catalog.encode(pg_catalog.sha256(
        pg_catalog.convert_to(bond_credit_canonical_json(v), 'UTF8')), 'hex')
$$;

CREATE OR REPLACE FUNCTION bond_credit_rating_declarations_valid(d jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
DECLARE
    item jsonb;
    start_text text;
    end_text text;
    keys text[];
    sorted_keys text[];
BEGIN
    IF d IS NULL OR pg_catalog.jsonb_typeof(d) <> 'object' THEN
        RETURN false;
    END IF;
    IF (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(d)) <> 3
       OR NOT (d ?& ARRAY['version', 'rating_scopes', 'uncleared_rating_sources'])
       OR pg_catalog.jsonb_typeof(d -> 'version') <> 'string'
       OR d ->> 'version' IS DISTINCT FROM 'bond_rating_declarations_v1'
       OR pg_catalog.jsonb_typeof(d -> 'rating_scopes') <> 'array'
       OR pg_catalog.jsonb_typeof(d -> 'uncleared_rating_sources') <> 'array' THEN
        RETURN false;
    END IF;

    FOR item IN SELECT value FROM pg_catalog.jsonb_array_elements(d -> 'rating_scopes') LOOP
        IF pg_catalog.jsonb_typeof(item) <> 'object' THEN
            RETURN false;
        END IF;
        IF (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(item)) <> 3
           OR NOT (item ?& ARRAY['agency_name', 'rating_type', 'scale'])
           OR pg_catalog.jsonb_typeof(item -> 'agency_name') <> 'string'
           OR COALESCE(item ->> 'agency_name', '') !~ '\S'
           OR pg_catalog.jsonb_typeof(item -> 'rating_type') NOT IN ('string', 'null')
           OR pg_catalog.jsonb_typeof(item -> 'scale') NOT IN ('string', 'null') THEN
            RETURN false;
        END IF;
    END LOOP;
    keys := ARRAY(SELECT bond_credit_canonical_json(value)
                  FROM pg_catalog.jsonb_array_elements(d -> 'rating_scopes') WITH ORDINALITY e(value, ordinality)
                  ORDER BY ordinality);
    sorted_keys := ARRAY(SELECT DISTINCT bond_credit_canonical_json(value) COLLATE "C"
                         FROM pg_catalog.jsonb_array_elements(d -> 'rating_scopes') e(value)
                         ORDER BY 1);
    IF keys IS DISTINCT FROM sorted_keys THEN
        RETURN false;
    END IF;

    FOR item IN SELECT value FROM pg_catalog.jsonb_array_elements(d -> 'uncleared_rating_sources') LOOP
        IF pg_catalog.jsonb_typeof(item) <> 'object' THEN
            RETURN false;
        END IF;
        IF (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(item)) <> 4
           OR NOT (item ?& ARRAY['source_ref', 'rights_state', 'coverage_start', 'coverage_end'])
           OR pg_catalog.jsonb_typeof(item -> 'source_ref') <> 'string'
           OR COALESCE(item ->> 'source_ref', '') !~ '\S'
           OR pg_catalog.jsonb_typeof(item -> 'rights_state') <> 'string'
           OR ((item ->> 'rights_state') = ANY (ARRAY[
               'denied', 'internal_work_product', 'public_document_internal_use',
               'public_government_record', 'unverified'])) IS NOT TRUE
           OR pg_catalog.jsonb_typeof(item -> 'coverage_start') NOT IN ('string', 'null')
           OR pg_catalog.jsonb_typeof(item -> 'coverage_end') NOT IN ('string', 'null') THEN
            RETURN false;
        END IF;
        start_text := item ->> 'coverage_start';
        end_text := item ->> 'coverage_end';
        IF (start_text IS NULL) <> (end_text IS NULL) THEN
            RETURN false;
        END IF;
        IF start_text IS NOT NULL THEN
            IF start_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR end_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                RETURN false;
            END IF;
            BEGIN
                IF pg_catalog.to_char(start_text::date, 'YYYY-MM-DD') <> start_text
                   OR pg_catalog.to_char(end_text::date, 'YYYY-MM-DD') <> end_text
                   OR start_text::date > end_text::date THEN
                    RETURN false;
                END IF;
            EXCEPTION WHEN datetime_field_overflow OR invalid_datetime_format THEN
                RETURN false;
            END;
        END IF;
    END LOOP;
    keys := ARRAY(SELECT bond_credit_canonical_json(value)
                  FROM pg_catalog.jsonb_array_elements(d -> 'uncleared_rating_sources')
                       WITH ORDINALITY e(value, ordinality)
                  ORDER BY ordinality);
    sorted_keys := ARRAY(SELECT DISTINCT bond_credit_canonical_json(value) COLLATE "C"
                         FROM pg_catalog.jsonb_array_elements(d -> 'uncleared_rating_sources') e(value)
                         ORDER BY 1);
    IF keys IS DISTINCT FROM sorted_keys OR (
        SELECT pg_catalog.count(*) <> pg_catalog.count(DISTINCT value ->> 'source_ref')
        FROM pg_catalog.jsonb_array_elements(d -> 'uncleared_rating_sources') e(value)
    ) THEN
        RETURN false;
    END IF;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_rating_scope_declared(
    d jsonb, agency text, rating_type text, scale text
) RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(d -> 'rating_scopes') e(value)
        WHERE e.value ->> 'agency_name' IS NOT DISTINCT FROM agency
          AND e.value ->> 'rating_type' IS NOT DISTINCT FROM rating_type
          AND e.value ->> 'scale' IS NOT DISTINCT FROM scale)
$$;

CREATE OR REPLACE FUNCTION bond_credit_rating_uncleared_covers(d jsonb, month_key date)
RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(d -> 'uncleared_rating_sources') e(value)
        WHERE (e.value ->> 'coverage_start') IS NULL
           OR ((e.value ->> 'coverage_start')::date <= ((month_key + interval '1 month')::date - 1)
               AND month_key <= (e.value ->> 'coverage_end')::date))
$$;

CREATE OR REPLACE FUNCTION bond_credit_row_sha256(r jsonb, spec text[]) RETURNS text
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    item text;
    col text;
    encoded text := '';
BEGIN
    IF spec IS NULL OR cardinality(spec) = 0 THEN
        RAISE EXCEPTION 'bond_credit_row_encoding:empty_spec';
    END IF;
    FOREACH item IN ARRAY spec LOOP
        col := pg_catalog.substr(item, 3);
        IF NOT (r ? col) THEN
            RAISE EXCEPTION 'bond_credit_row_encoding:missing_column:%', col;
        END IF;
        IF pg_catalog.left(item, 2) = 't:' AND pg_catalog.jsonb_typeof(r -> col) = 'string' THEN
            encoded := encoded || bond_credit_encoded_scalar('S', pg_catalog.to_char(
                (r ->> col)::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
        ELSIF pg_catalog.left(item, 2) IN ('t:', 'j:') THEN
            encoded := encoded || bond_credit_value_encoding(r -> col);
        ELSE
            RAISE EXCEPTION 'bond_credit_row_encoding:bad_spec_entry:%', item;
        END IF;
    END LOOP;
    RETURN pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(encoded, 'UTF8')), 'hex');
END $$;

-- Column specs mirrored from the Python row SPECs (contracts.sql_row_spec; tested equal).
CREATE OR REPLACE FUNCTION bond_credit_frame_spec(frame text) RETURNS text[]
LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
BEGIN
    RETURN CASE frame
-- BEGIN GENERATED FRAME SPECS
        WHEN 'adjudications' THEN ARRAY['j:adjudication_id', 'j:package_id', 'j:subject_kind', 'j:subject_id', 'j:status', 'j:supersedes_adjudication_id', 'j:policy_digest', 'j:reviewer_id', 'j:reviewer_role', 't:adjudicated_at', 'j:rationale', 'j:evidence_observation_ids', 'j:link_ids', 'j:proposal_evidence_ids', 'j:support_valid_from', 'j:support_valid_to']::text[]
        WHEN 'coverage' THEN ARRAY['j:period_label', 'j:source', 'j:event_type', 'j:rating_stratum', 'j:exposure_cohort', 'j:state', 'j:denominator_basis', 'j:denominator_count', 'j:exposed_issue_months', 'j:event_count', 'j:unlinked_count', 'j:date_uncertain_count', 'j:unknown_outcome_issue_months', 'j:source_frontier', 'j:lag_p50_days', 'j:lag_p90_days', 'j:lag_max_days', 'j:rationale', 'j:validation_receipt_digest']::text[]
        WHEN 'event_links' THEN ARRAY['j:link_id', 'j:package_id', 'j:observation_id', 'j:security_id', 'j:cusip9', 'j:obligor_id', 'j:affected_scope', 'j:valid_from', 'j:valid_to', 't:link_known_at', 'j:identity_evidence_refs', 'j:identity_evidence_digest', 'j:status', 'j:rationale', 'j:supersedes_link_id']::text[]
        WHEN 'events' THEN ARRAY['j:security_id', 'j:episode_id', 'j:cusip9', 'j:obligor_id', 'j:issuer_episode_id', 'j:primary_type', 'j:corroboration_flags', 'j:admission_status', 'j:timing_class', 'j:onset_date', 'j:onset_lower_exclusive', 'j:onset_upper_inclusive', 'j:onset_lower_evidence_ids', 'j:recognition_date', 't:evidence_known_at', 't:link_known_at', 'j:evidence_observation_ids', 'j:link_ids', 'j:adjudication_ids', 'j:resolution_date', 'j:resolution_refs', 't:resolution_known_at', 'j:alias_spell_id', 'j:proposal_evidence_ids', 'j:exchange_relation_ids', 'j:dependency_digest', 'j:event_input_digest']::text[]
        WHEN 'exchange_relations' THEN ARRAY['j:relation_id', 'j:old_security_id', 'j:old_cusip9', 'j:new_security_id', 'j:new_cusip9', 'j:episode_id', 'j:alias_spell_id', 'j:old_link_id', 'j:new_link_id', 'j:exchange_document_observation_ids', 'j:pairing_adjudication_id', 'j:exchange_effective_date', 'j:valid_from', 'j:valid_to', 't:evidence_known_at']::text[]
        WHEN 'exit_evidence' THEN ARRAY['j:security_id', 'j:last_panel_month', 'j:cusip9', 'j:primary_reason', 'j:flags', 'j:next_observed_month', 'j:gap_months', 'j:scheduled_maturity', 'j:proven_repayment_date', 'j:evidence_observation_ids', 't:known_at']::text[]
        WHEN 'family_contexts' THEN ARRAY['j:context_id', 'j:report_date', 't:knowledge_cutoff', 'j:rule_version', 'j:vote_observation_ids', 'j:selection_filing_ids', 'j:index_package_ids', 'j:universe_digest', 'j:membership_digest', 't:evidence_known_at']::text[]
        WHEN 'family_evidence' THEN ARRAY['j:family_evidence_id', 'j:context_id', 'j:registrant_cik', 'j:voting_series_ids', 'j:vote_observation_ids', 'j:selected_filing_id', 'j:blocking_filing_ids', 'j:state', 'j:reasons', 'j:component_id', 'j:valid_from', 'j:valid_to']::text[]
        WHEN 'fingerprint' THEN ARRAY['j:product', 'j:contract_version', 'j:target_month', 't:knowledge_cutoff', 'j:knowledge_mode', 'j:build_scope', 'j:policy_digest', 'j:contract_digest', 'j:sql_digest', 'j:code_digest', 'j:panel_publication_id', 'j:panel_grid_digest', 'j:panel_grid_count', 'j:issuer_mapping_digest', 'j:rating_declarations', 'j:rating_declarations_digest', 'j:rating_input_digest', 'j:source_manifest_digest', 'j:package_inventory_digest', 'j:observation_inventory_digest', 'j:link_inventory_digest', 'j:adjudication_inventory_digest', 'j:ncen_filing_inventory_digest', 'j:validation_digest', 'j:family_contexts_digest', 'j:family_evidence_digest', 'j:proposal_evidence_digest', 'j:exchange_relations_digest']::text[]
        WHEN 'followups' THEN ARRAY['j:security_id', 'j:spell_id', 'j:segment_id', 'j:cusip9', 'j:interval_start_exclusive', 'j:interval_end_inclusive', 'j:status', 'j:completeness_basis', 'j:evidence_observation_ids', 'j:adjudication_ids', 't:known_at']::text[]
        WHEN 'ncen_filings' THEN ARRAY['j:filing_evidence_id', 'j:package_id', 'j:accession_number', 'j:row_locator', 'j:registrant_cik', 'j:form_type', 'j:report_period_end', 'j:filing_date', 'j:header_package_id', 'j:acceptance_raw', 't:acceptance_at', 't:public_available_at', 't:first_seen_at', 'j:public_time_basis', 'j:version_evidence_filing_ids', 'j:parse_status', 'j:reasons', 'j:family_answer', 'j:family_name_raw', 'j:reported_series_ids', 'j:adviser_records', 'j:underwriter_records', 'j:projection_digest', 'j:supersedes_filing_evidence_id']::text[]
        WHEN 'observations' THEN ARRAY['j:observation_id', 'j:package_id', 'j:member_name', 'j:row_locator', 'j:observation_kind', 'j:semantic_key', 'j:accession_number', 'j:holding_id', 'j:cusip_raw', 'j:cusip9', 'j:isin_raw', 'j:security_id', 'j:issuer_cik', 'j:registrant_cik', 'j:series_id', 'j:fund_family_id', 'j:issuer_type_raw', 'j:asset_category_raw', 'j:report_date', 'j:effective_date', 'j:date_precision', 'j:effective_lower_exclusive', 'j:effective_upper_inclusive', 'j:acceptance_raw', 't:acceptance_at', 't:public_available_at', 'j:public_time_basis', 't:first_seen_at', 'j:nport_is_default', 'j:nport_arrears_or_deferral', 'j:nport_paid_in_kind', 'j:field_presence', 'j:agency_name', 'j:agency_subject_kind', 'j:agency_rating_type', 'j:agency_scale', 'j:agency_currency', 'j:agency_rating_symbol', 'j:agency_action_classification', 'j:agency_action_date', 't:agency_file_creation_at', 'j:document_quote', 'j:document_location', 'j:document_sha256', 'j:revision_kind', 'j:supersedes_observation_id']::text[]
        WHEN 'proposal_evidence' THEN ARRAY['j:proposal_evidence_id', 'j:cusip9', 'j:proposed_status', 'j:basis', 'j:onset_lower_exclusive', 'j:onset_upper_inclusive', 'j:onset_lower_evidence_ids', 'j:onset_upper_evidence_ids', 'j:evidence_observation_ids', 'j:family_evidence_ids', 'j:corroboration_adjudication_ids', 't:evidence_known_at', 'j:policy_digest']::text[]
        WHEN 'publication_sources' THEN ARRAY['j:package_id', 'j:role', 'j:package_content_sha256', 'j:row_count', 'j:row_inventory_digest']::text[]
        WHEN 'ratings' THEN ARRAY['j:cusip_id', 'j:month', 'j:view_kind', 'j:bucket', 'j:state', 'j:action_date', 't:public_known_at', 'j:agency_source_ids', 'j:binding_link_ids', 'j:coverage_frontier', 'j:action_input_digest', 'j:default_overlay_episode_id']::text[]
        WHEN 'source_packages' THEN ARRAY['j:package_id', 'j:source_family', 'j:external_id', 'j:content_sha256', 'j:raw_sha256', 'j:header_sha256', 'j:member_sha256s', 'j:official_url', 'j:accession_number', 'j:rights_state', 'j:rights_ref', 'j:parser_version', 'j:schema_version', 't:retrieved_at', 't:first_verified_public_at', 'j:public_time_basis', 'j:public_time_evidence', 'j:source_coverage_start', 'j:source_coverage_end', 'j:public_coverage_start', 'j:public_coverage_end', 'j:effective_coverage_start', 'j:effective_coverage_end', 'j:raw_locator', 'j:revision_of_package_id', 'j:sec_run_id', 'j:sec_package_id']::text[]
        WHEN 'validation_receipt' THEN ARRAY['j:receipt_id', 'j:verdict', 'j:scope', 'j:evidence_digest', 'j:reviewer_id', 't:issued_at', 'j:positive_evidence_count', 'j:rating_input_digest', 'j:rating_package_digest', 'j:surveillance_start_exclusive', 'j:surveillance_end_inclusive']::text[]
-- END GENERATED FRAME SPECS
    END;
END $$;

-- Contract identity SQL enforces (generated by scripts/regen_bond_default_contracts.py):
-- bond_credit_validate refuses any publication whose contract version, policy digest or
-- contract digest differs, so SQL never accepts a writer-selected contract identity.
CREATE OR REPLACE FUNCTION bond_credit_expected_pins(
    OUT contract_version text, OUT policy_digest text, OUT contract_digest text,
    OUT family_rule_version text, OUT rating_resolver_id text)
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
-- BEGIN GENERATED CONTRACT PINS
    SELECT 'bond_default_event_bundle_v2'::text, 'sha256:f0aea3d0d86daa874c237adf38de634151f18c0ef2cff5ab69e7d67588bc5662'::text, 'sha256:7b48e744ecb5e75e8865c6284d285462dcb9f11ca7ffa15dda1a375cd27f337f'::text,
           'bond_default_ncen_family_fe1ab_v3'::text, 'bond_public_ratings_v1'::text
-- END GENERATED CONTRACT PINS
$$;

CREATE OR REPLACE FUNCTION bond_credit_rating_input_digest(
    declarations jsonb, package_digest text
) RETURNS text LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT bond_credit_json_digest(pg_catalog.jsonb_build_object(
        'version', 'rating_input_manifest_v2',
        'resolver_id', (SELECT x.rating_resolver_id FROM bond_credit_expected_pins() x),
        'rating_declarations', declarations,
        'rating_declarations_digest', bond_credit_json_digest(declarations),
        'rating_package_digest', package_digest))
$$;

-- Recomputes the row hash from the stored columns; a stored row_sha256 that differs fails.
CREATE OR REPLACE FUNCTION bond_credit_checked_row_sha256(frame text, r jsonb) RETURNS text
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    spec text[] := bond_credit_frame_spec(frame);
    computed text;
BEGIN
    IF spec IS NULL THEN
        RAISE EXCEPTION 'bond_credit_row_encoding:unknown_frame:%', frame;
    END IF;
    computed := bond_credit_row_sha256(r, spec);
    IF (r ->> 'row_sha256') IS DISTINCT FROM computed THEN
        RAISE EXCEPTION 'bond_credit_validate:row_hash_mismatch:%', frame;
    END IF;
    RETURN computed;
END $$;

-- Fingerprint over the stored manifest fields (contracts.fingerprint_digest).
CREATE OR REPLACE FUNCTION bond_credit_fingerprint_digest(p jsonb) RETURNS text
LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT 'sha256:' || bond_credit_row_sha256(p, bond_credit_frame_spec('fingerprint'))
$$;

-- Validation-receipt digest over the stored validation_* fields (ValidationReceipt.digest).
CREATE OR REPLACE FUNCTION bond_credit_validation_digest(p jsonb) RETURNS text
LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT CASE WHEN p -> 'validation_receipt_id' IS NULL OR p -> 'validation_receipt_id' = 'null'::jsonb
                THEN NULL
                ELSE 'sha256:' || bond_credit_row_sha256(pg_catalog.jsonb_build_object(
                    'receipt_id', p -> 'validation_receipt_id', 'verdict', p -> 'validation_verdict',
                    'scope', p -> 'validation_scope', 'evidence_digest', p -> 'validation_evidence_digest',
                    'reviewer_id', p -> 'validation_reviewer_id', 'issued_at', p -> 'validation_issued_at',
                    'positive_evidence_count', p -> 'validation_positive_evidence_count',
                    'rating_input_digest', p -> 'validation_rating_input_digest',
                    'rating_package_digest', p -> 'validation_rating_package_digest',
                    'surveillance_start_exclusive', p -> 'validation_surveillance_start_exclusive',
                    'surveillance_end_inclusive', p -> 'validation_surveillance_end_inclusive'),
                    bond_credit_frame_spec('validation_receipt')) END
$$;

-- RFC 9562 UUIDv8 from the first 128 bits of the fingerprint sha256 (contracts.publication_id_for).
CREATE OR REPLACE FUNCTION bond_credit_publication_id_for(fingerprint text) RETURNS uuid
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT (pg_catalog.substr(h, 1, 12) || '8' || pg_catalog.substr(h, 14, 3)
            || pg_catalog.translate(pg_catalog.substr(h, 17, 1), '0123456789abcdef', '89ab89ab89ab89ab')
            || pg_catalog.substr(h, 18, 15))::uuid
    FROM (SELECT pg_catalog.substr(fingerprint, 8, 32) AS h) s
    WHERE fingerprint ~ '^sha256:[0-9a-f]{64}$'
$$;

-- ---------------------------------------------------------------------------
-- Publications
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_credit_publications (
    publication_id uuid PRIMARY KEY,
    publication_version integer NOT NULL UNIQUE CHECK (publication_version > 0),
    product text NOT NULL CHECK (product = 'bond_credit_evidence_v1'),
    contract_version text NOT NULL CHECK (contract_version = 'bond_default_event_bundle_v2'),
    fingerprint_digest text NOT NULL UNIQUE CHECK (fingerprint_digest ~ '^sha256:[0-9a-f]{64}$'),
    target_month date NOT NULL CHECK (EXTRACT(day FROM target_month) = 1),
    knowledge_cutoff timestamptz NOT NULL,
    knowledge_mode text NOT NULL CHECK (knowledge_mode IN ('current_run', 'historical_reconstruction')),
    build_scope text NOT NULL CHECK (build_scope IN ('complete', 'limited')),
    quality_state text NOT NULL CHECK (quality_state IN ('partial', 'qualified', 'unavailable')),
    policy_digest text NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    contract_digest text NOT NULL CHECK (contract_digest ~ '^sha256:[0-9a-f]{64}$'),
    sql_digest text NOT NULL CHECK (sql_digest ~ '^sha256:[0-9a-f]{64}$'),
    code_digest text NOT NULL CHECK (code_digest ~ '^sha256:[0-9a-f]{64}$'),
    panel_publication_id uuid NOT NULL,
    panel_grid_digest text NOT NULL CHECK (panel_grid_digest ~ '^sha256:[0-9a-f]{64}$'),
    panel_grid_count integer NOT NULL CHECK (panel_grid_count >= 0),
    issuer_mapping_digest text CHECK (issuer_mapping_digest ~ '^sha256:[0-9a-f]{64}$'),
    rating_declarations jsonb NOT NULL CHECK (pg_catalog.jsonb_typeof(rating_declarations) = 'object'),
    rating_declarations_digest text NOT NULL CHECK (rating_declarations_digest ~ '^sha256:[0-9a-f]{64}$'),
    rating_input_digest text NOT NULL CHECK (rating_input_digest ~ '^sha256:[0-9a-f]{64}$'),
    source_manifest_digest text NOT NULL CHECK (source_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    package_inventory_digest text NOT NULL CHECK (package_inventory_digest ~ '^sha256:[0-9a-f]{64}$'),
    observation_inventory_digest text NOT NULL CHECK (observation_inventory_digest ~ '^sha256:[0-9a-f]{64}$'),
    link_inventory_digest text NOT NULL CHECK (link_inventory_digest ~ '^sha256:[0-9a-f]{64}$'),
    adjudication_inventory_digest text NOT NULL CHECK (adjudication_inventory_digest ~ '^sha256:[0-9a-f]{64}$'),
    ncen_filing_inventory_digest text NOT NULL CHECK (ncen_filing_inventory_digest ~ '^sha256:[0-9a-f]{64}$'),
    validation_receipt_id uuid,
    validation_verdict text CHECK (validation_verdict IN ('partial', 'qualified', 'unavailable')),
    validation_scope text CHECK (validation_scope ~ '\S'),
    validation_evidence_digest text CHECK (validation_evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    validation_reviewer_id text CHECK (validation_reviewer_id ~ '\S'),
    validation_issued_at timestamptz,
    validation_positive_evidence_count integer CHECK (validation_positive_evidence_count >= 0),
    -- Rating-input qualification carried by the receipt (both or neither, receipt required).
    validation_rating_input_digest text CHECK (validation_rating_input_digest ~ '^sha256:[0-9a-f]{64}$'),
    validation_rating_package_digest text CHECK (validation_rating_package_digest ~ '^sha256:[0-9a-f]{64}$'),
    -- Structured surveillance scope: every panel-grid CUSIP over (start, end] (both or neither).
    validation_surveillance_start_exclusive date,
    validation_surveillance_end_inclusive date,
    validation_digest text CHECK (validation_digest ~ '^sha256:[0-9a-f]{64}$'),
    sources_count integer NOT NULL CHECK (sources_count >= 0),
    sources_digest text NOT NULL CHECK (sources_digest ~ '^sha256:[0-9a-f]{64}$'),
    events_count integer NOT NULL CHECK (events_count >= 0),
    events_digest text NOT NULL CHECK (events_digest ~ '^sha256:[0-9a-f]{64}$'),
    followups_count integer NOT NULL CHECK (followups_count >= 0),
    followups_digest text NOT NULL CHECK (followups_digest ~ '^sha256:[0-9a-f]{64}$'),
    exits_count integer NOT NULL CHECK (exits_count >= 0),
    exits_digest text NOT NULL CHECK (exits_digest ~ '^sha256:[0-9a-f]{64}$'),
    coverage_count integer NOT NULL CHECK (coverage_count >= 0),
    coverage_digest text NOT NULL CHECK (coverage_digest ~ '^sha256:[0-9a-f]{64}$'),
    ratings_count integer NOT NULL CHECK (ratings_count >= 0),
    ratings_digest text NOT NULL CHECK (ratings_digest ~ '^sha256:[0-9a-f]{64}$'),
    family_contexts_count integer NOT NULL CHECK (family_contexts_count >= 0),
    family_contexts_digest text NOT NULL CHECK (family_contexts_digest ~ '^sha256:[0-9a-f]{64}$'),
    family_evidence_count integer NOT NULL CHECK (family_evidence_count >= 0),
    family_evidence_digest text NOT NULL CHECK (family_evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    proposal_evidence_count integer NOT NULL CHECK (proposal_evidence_count >= 0),
    proposal_evidence_digest text NOT NULL CHECK (proposal_evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    exchange_relations_count integer NOT NULL CHECK (exchange_relations_count >= 0),
    exchange_relations_digest text NOT NULL CHECK (exchange_relations_digest ~ '^sha256:[0-9a-f]{64}$'),
    lifecycle_state text NOT NULL DEFAULT 'prepared' CHECK (lifecycle_state IN ('prepared', 'validated')),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    validated_at timestamptz,
    CHECK ((lifecycle_state = 'prepared') = (validated_at IS NULL)),
    CHECK (num_nonnulls(validation_receipt_id, validation_verdict, validation_scope,
                        validation_evidence_digest, validation_reviewer_id, validation_issued_at,
                        validation_positive_evidence_count, validation_digest) IN (0, 8)),
    CHECK (num_nonnulls(validation_rating_input_digest, validation_rating_package_digest) IN (0, 2)),
    CHECK (validation_rating_input_digest IS NULL OR validation_receipt_id IS NOT NULL),
    CHECK (num_nonnulls(validation_surveillance_start_exclusive, validation_surveillance_end_inclusive) IN (0, 2)),
    CHECK (validation_surveillance_start_exclusive IS NULL OR validation_receipt_id IS NOT NULL),
    CHECK (validation_surveillance_start_exclusive < validation_surveillance_end_inclusive),
    CHECK (validation_verdict IS DISTINCT FROM 'qualified' OR validation_positive_evidence_count > 0),
    -- Qualified requires a positive validation receipt and a complete build; zero rows never imply it.
    CHECK (quality_state <> 'qualified'
           OR (validation_verdict IS NOT NULL AND validation_verdict = 'qualified'
               AND build_scope = 'complete' AND issuer_mapping_digest IS NOT NULL)),
    CHECK (source_manifest_digest = sources_digest)
);

CREATE TABLE IF NOT EXISTS bond_credit_publication_sources (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    package_id uuid NOT NULL REFERENCES bond_default_source_package(package_id),
    role text NOT NULL CHECK (role IN (
        'adjudication_inventory', 'agency_evidence', 'document_evidence', 'edgar_evidence',
        'link_inventory', 'ncen_family_evidence', 'nport_evidence')),
    package_content_sha256 char(64) NOT NULL CHECK (package_content_sha256 ~ '^[0-9a-f]{64}$'),
    row_count integer NOT NULL CHECK (row_count >= 0),
    row_inventory_digest text NOT NULL CHECK (row_inventory_digest ~ '^sha256:[0-9a-f]{64}$'),
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, package_id)
);
CREATE INDEX IF NOT EXISTS bond_credit_publication_sources_package_idx
    ON bond_credit_publication_sources (package_id);

CREATE TABLE IF NOT EXISTS bond_credit_current_pointer (
    product text PRIMARY KEY CHECK (product = 'bond_credit_evidence_v1'),
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS bond_credit_publication_revocations (
    publication_id uuid PRIMARY KEY REFERENCES bond_credit_publications(publication_id),
    reason text NOT NULL CHECK (reason ~ '\S'),
    evidence_digest text NOT NULL CHECK (evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    revoked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked_by name NOT NULL DEFAULT session_user
);

-- Transaction-scoped capability tokens; only SECURITY DEFINER functions write them.
CREATE TABLE IF NOT EXISTS bond_credit_lifecycle_tokens (
    publication_id uuid PRIMARY KEY,
    backend_pid integer NOT NULL,
    xact_id bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS bond_credit_pointer_tokens (
    product text PRIMARY KEY,
    backend_pid integer NOT NULL,
    xact_id bigint NOT NULL
);
REVOKE ALL ON bond_credit_lifecycle_tokens, bond_credit_pointer_tokens FROM PUBLIC;

-- ---------------------------------------------------------------------------
-- Guards
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_credit_publication_insert_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    -- Monotonic version under a product-scoped lock; lifecycle always starts prepared.
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bond_credit_evidence_v1|version', 0));
    SELECT COALESCE(max(publication_version), 0) + 1 INTO NEW.publication_version FROM bond_credit_publications;
    NEW.lifecycle_state := 'prepared';
    NEW.validated_at := NULL;
    NEW.prepared_at := pg_catalog.clock_timestamp();
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_publication_update_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.lifecycle_state = 'prepared' AND NEW.lifecycle_state = 'validated'
       AND NEW.validated_at IS NOT NULL
       AND (pg_catalog.to_jsonb(NEW) - 'lifecycle_state' - 'validated_at')
           = (pg_catalog.to_jsonb(OLD) - 'lifecycle_state' - 'validated_at')
       AND EXISTS (SELECT 1 FROM bond_credit_lifecycle_tokens t
                   WHERE t.publication_id = OLD.publication_id
                     AND t.backend_pid = pg_catalog.pg_backend_pid()
                     AND t.xact_id = pg_catalog.txid_current()) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'bond_credit_publications is immutable: % refused', TG_OP;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_child_insert_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    state text;
BEGIN
    SELECT p.lifecycle_state INTO state FROM bond_credit_publications p
    WHERE p.publication_id = NEW.publication_id FOR SHARE;
    IF state IS NULL THEN
        RAISE EXCEPTION '%: publication % does not exist', TG_TABLE_NAME, NEW.publication_id;
    END IF;
    IF state <> 'prepared' THEN
        RAISE EXCEPTION '%: rows may only be inserted while publication % is prepared',
            TG_TABLE_NAME, NEW.publication_id;
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_publication_source_seal() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    -- Same package-scoped lock as bond_credit_ledger_family_guard: sealing and appending serialize.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('bond_credit_package|' || NEW.package_id::text, 0));
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_pointer_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    pointer_product text := COALESCE(NEW.product, OLD.product);
BEGIN
    IF TG_OP = 'DELETE' OR NOT EXISTS (
        SELECT 1 FROM bond_credit_pointer_tokens t
        WHERE t.product = pointer_product AND t.backend_pid = pg_catalog.pg_backend_pid()
          AND t.xact_id = pg_catalog.txid_current()) THEN
        RAISE EXCEPTION 'bond_credit_current_pointer is managed only by bond_credit_promote';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_credit_publications_insert_guard ON bond_credit_publications;
CREATE TRIGGER bond_credit_publications_insert_guard BEFORE INSERT ON bond_credit_publications
FOR EACH ROW EXECUTE FUNCTION bond_credit_publication_insert_guard();
DROP TRIGGER IF EXISTS bond_credit_publications_update_guard ON bond_credit_publications;
CREATE TRIGGER bond_credit_publications_update_guard BEFORE UPDATE OR DELETE ON bond_credit_publications
FOR EACH ROW EXECUTE FUNCTION bond_credit_publication_update_guard();
DROP TRIGGER IF EXISTS bond_credit_publications_no_truncate ON bond_credit_publications;
CREATE TRIGGER bond_credit_publications_no_truncate BEFORE TRUNCATE ON bond_credit_publications
FOR EACH STATEMENT EXECUTE FUNCTION bond_credit_append_only();

DROP TRIGGER IF EXISTS bond_credit_publication_sources_insert_guard ON bond_credit_publication_sources;
CREATE TRIGGER bond_credit_publication_sources_insert_guard BEFORE INSERT ON bond_credit_publication_sources
FOR EACH ROW EXECUTE FUNCTION bond_credit_child_insert_guard();
DROP TRIGGER IF EXISTS bond_credit_publication_sources_seal ON bond_credit_publication_sources;
CREATE TRIGGER bond_credit_publication_sources_seal BEFORE INSERT ON bond_credit_publication_sources
FOR EACH ROW EXECUTE FUNCTION bond_credit_publication_source_seal();
DROP TRIGGER IF EXISTS bond_credit_publication_sources_append_only ON bond_credit_publication_sources;
CREATE TRIGGER bond_credit_publication_sources_append_only BEFORE UPDATE OR DELETE ON bond_credit_publication_sources
FOR EACH ROW EXECUTE FUNCTION bond_credit_append_only();
DROP TRIGGER IF EXISTS bond_credit_publication_sources_no_truncate ON bond_credit_publication_sources;
CREATE TRIGGER bond_credit_publication_sources_no_truncate BEFORE TRUNCATE ON bond_credit_publication_sources
FOR EACH STATEMENT EXECUTE FUNCTION bond_credit_append_only();

DROP TRIGGER IF EXISTS bond_credit_current_pointer_guard ON bond_credit_current_pointer;
CREATE TRIGGER bond_credit_current_pointer_guard BEFORE INSERT OR UPDATE OR DELETE ON bond_credit_current_pointer
FOR EACH ROW EXECUTE FUNCTION bond_credit_pointer_guard();
DROP TRIGGER IF EXISTS bond_credit_current_pointer_no_truncate ON bond_credit_current_pointer;
CREATE TRIGGER bond_credit_current_pointer_no_truncate BEFORE TRUNCATE ON bond_credit_current_pointer
FOR EACH STATEMENT EXECUTE FUNCTION bond_credit_append_only();

DROP TRIGGER IF EXISTS bond_credit_publication_revocations_append_only ON bond_credit_publication_revocations;
CREATE TRIGGER bond_credit_publication_revocations_append_only BEFORE UPDATE OR DELETE ON bond_credit_publication_revocations
FOR EACH ROW EXECUTE FUNCTION bond_credit_append_only();
DROP TRIGGER IF EXISTS bond_credit_publication_revocations_no_truncate ON bond_credit_publication_revocations;
CREATE TRIGGER bond_credit_publication_revocations_no_truncate BEFORE TRUNCATE ON bond_credit_publication_revocations
FOR EACH STATEMENT EXECUTE FUNCTION bond_credit_append_only();

-- ---------------------------------------------------------------------------
-- Validation (one transaction; raises on the first violated rule)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_credit_assert_frame(caller text, frame text, expected_count integer,
                                                    expected_digest text, hashes text[])
RETURNS void LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
BEGIN
    IF COALESCE(cardinality(hashes), 0) <> expected_count
       OR bond_credit_frame_digest(COALESCE(hashes, '{}')) <> expected_digest THEN
        RAISE EXCEPTION '%:frame_mismatch:%', caller, frame;
    END IF;
END $$;

-- Output frame row counts and digests over row hashes recomputed from the stored columns
-- (a stored row_sha256 that differs from its recomputation fails). Run by validation and
-- again by promotion, immediately before the pointer moves.
CREATE OR REPLACE FUNCTION bond_credit_assert_output_frames(caller text, pub bond_credit_publications)
RETURNS void LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_assert_frame(caller, 'publication_sources', pub.sources_count, pub.sources_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('publication_sources', pg_catalog.to_jsonb(t))
              FROM bond_credit_publication_sources t WHERE t.publication_id = pub.publication_id));
    PERFORM bond_credit_assert_frame(caller, 'events', pub.events_count, pub.events_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('events', pg_catalog.to_jsonb(t))
              FROM bond_default_event_v1 t WHERE t.publication_id = pub.publication_id));
    PERFORM bond_credit_assert_frame(caller, 'followups', pub.followups_count, pub.followups_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('followups', pg_catalog.to_jsonb(t))
              FROM bond_default_followup_v1 t WHERE t.publication_id = pub.publication_id));
    PERFORM bond_credit_assert_frame(caller, 'exit_evidence', pub.exits_count, pub.exits_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('exit_evidence', pg_catalog.to_jsonb(t))
              FROM bond_default_exit_evidence_v1 t WHERE t.publication_id = pub.publication_id));
    PERFORM bond_credit_assert_frame(caller, 'coverage', pub.coverage_count, pub.coverage_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('coverage', pg_catalog.to_jsonb(t))
              FROM bond_default_coverage_v1 t WHERE t.publication_id = pub.publication_id));
    PERFORM bond_credit_assert_frame(caller, 'ratings', pub.ratings_count, pub.ratings_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('ratings', pg_catalog.to_jsonb(t))
              FROM bond_rating_history_public_v1 t WHERE t.publication_id = pub.publication_id));
    PERFORM bond_credit_assert_frame(caller, 'family_contexts', pub.family_contexts_count, pub.family_contexts_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('family_contexts', pg_catalog.to_jsonb(t))
              FROM bond_default_family_context_v2 t WHERE t.publication_id = pub.publication_id));
    PERFORM bond_credit_assert_frame(caller, 'family_evidence', pub.family_evidence_count, pub.family_evidence_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('family_evidence', pg_catalog.to_jsonb(t))
              FROM bond_default_family_evidence_v2 t WHERE t.publication_id = pub.publication_id));
    PERFORM bond_credit_assert_frame(caller, 'proposal_evidence', pub.proposal_evidence_count,
        pub.proposal_evidence_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('proposal_evidence', pg_catalog.to_jsonb(t))
              FROM bond_default_proposal_evidence_v2 t WHERE t.publication_id = pub.publication_id));
    PERFORM bond_credit_assert_frame(caller, 'exchange_relations', pub.exchange_relations_count,
        pub.exchange_relations_digest,
        ARRAY(SELECT bond_credit_checked_row_sha256('exchange_relations', pg_catalog.to_jsonb(t))
              FROM bond_default_exchange_relation_v2 t WHERE t.publication_id = pub.publication_id));
END $$;

-- First record (by id) whose supersedes chain returns to itself, or NULL (acyclic).
CREATE OR REPLACE FUNCTION bond_credit_revision_cycle(ids uuid[], parents uuid[]) RETURNS uuid
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    WITH RECURSIVE inv AS (
        SELECT e.id, e.parent FROM ROWS FROM (pg_catalog.unnest(ids), pg_catalog.unnest(parents)) AS e(id, parent)
    ), walk(start_id, cur_id, depth) AS (
        SELECT inv.id, inv.parent, 1 FROM inv WHERE inv.parent IS NOT NULL
        UNION ALL
        SELECT walk.start_id, inv.parent, walk.depth + 1
        FROM walk JOIN inv ON inv.id = walk.cur_id
        WHERE inv.parent IS NOT NULL AND walk.cur_id <> walk.start_id
          AND walk.depth <= pg_catalog.cardinality(ids)
    )
    SELECT walk.start_id FROM walk WHERE walk.cur_id = walk.start_id ORDER BY walk.start_id LIMIT 1
$$;

-- ===========================================================================
-- W0 amendment 1 (bundle v2): persisted dependency closure. Every function below mirrors a
-- named Python rule in src/bonds/default_events/publication.py / contracts.py; the validator
-- recomputes contexts, proposals, relations, times and digests from the persisted inventory.
-- ===========================================================================
CREATE OR REPLACE FUNCTION bond_credit_ts_text(t timestamptz) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT pg_catalog.to_char(t AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
$$;

-- 'sha256:<hex>' of UTF-8 text (canonical JSON built by the caller) / of a value encoding.
CREATE OR REPLACE FUNCTION bond_credit_text_digest(body text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT 'sha256:' || pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(body, 'UTF8')), 'hex')
$$;
CREATE OR REPLACE FUNCTION bond_credit_encoded_digest(v jsonb) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT bond_credit_text_digest(bond_credit_value_encoding(v))
$$;
-- Compact canonical JSON array of strings in C order (contracts.canonical_json_bytes).
CREATE OR REPLACE FUNCTION bond_credit_json_texts(v text[]) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT '[' || COALESCE((SELECT pg_catalog.string_agg(pg_catalog.to_json(x)::text, ',' ORDER BY x COLLATE "C")
                            FROM pg_catalog.unnest(v) x), '') || ']'
$$;

-- N-CEN lexical normalization (contracts.ncen_*; ASCII-only case folding).
CREATE OR REPLACE FUNCTION bond_credit_ascii_upper(v text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT pg_catalog.translate(v, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')
$$;
CREATE OR REPLACE FUNCTION bond_credit_ncen_clean(raw text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT NULLIF(pg_catalog.btrim(raw, E' \t\n\r' || pg_catalog.chr(11) || pg_catalog.chr(12)), '')
$$;
CREATE OR REPLACE FUNCTION bond_credit_ncen_file_number(raw text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT CASE WHEN m IS NULL OR m[2]::numeric = 0 THEN NULL
                ELSE (m[1]::numeric)::text || '-' || (m[2]::numeric)::text END
    FROM (SELECT pg_catalog.regexp_match(pg_catalog.replace(bond_credit_ncen_clean(raw), ' ', ''),
                                         '^([0-9]{1,3})-([0-9]{1,9})$') AS m) s
$$;
CREATE OR REPLACE FUNCTION bond_credit_ncen_crd(raw text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT CASE WHEN c ~ '^[0-9]{1,12}$' AND c::numeric <> 0 THEN (c::numeric)::text END
    FROM (SELECT bond_credit_ncen_clean(raw) AS c) s
$$;
CREATE OR REPLACE FUNCTION bond_credit_ncen_lei(raw text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT CASE WHEN u ~ '^[A-Z0-9]{20}$' AND u !~ '^0+$' THEN u END
    FROM (SELECT bond_credit_ascii_upper(bond_credit_ncen_clean(raw)) AS u) s
$$;
CREATE OR REPLACE FUNCTION bond_credit_ncen_is_sentinel(raw text) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT bond_credit_ncen_clean(raw) IS NULL OR pg_catalog.array_to_string(ARRAY(
               SELECT p FROM pg_catalog.regexp_split_to_table(
                   bond_credit_ascii_upper(bond_credit_ncen_clean(raw)), '[ \t\n\r\f\v]+') p WHERE p <> ''), ' ')
           = ANY (ARRAY['N/A', 'N.A.', 'N.A', 'NA', 'N / A', 'NONE', 'NULL', 'NIL', '-', '--', '---', '0',
                        'NOT APPLICABLE', 'NOT AVAILABLE', 'UNKNOWN', 'TBD'])
$$;
CREATE OR REPLACE FUNCTION bond_credit_ncen_family_name_key(raw text) RETURNS text
LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
DECLARE
    key text := pg_catalog.regexp_replace(bond_credit_ascii_upper(bond_credit_ncen_clean(raw)), '[^A-Z0-9]', '', 'g');
    suffix text;
    stripped boolean := true;
BEGIN
    WHILE stripped AND COALESCE(key, '') <> '' LOOP
        stripped := false;
        FOREACH suffix IN ARRAY ARRAY['FAMILY', 'COMPLEX', 'GROUP', 'FUNDS', 'FUND', 'TRUST'] LOOP
            IF pg_catalog.right(key, pg_catalog.length(suffix)) = suffix THEN
                key := pg_catalog.left(key, pg_catalog.length(key) - pg_catalog.length(suffix));
                stripped := true;
                EXIT;
            END IF;
        END LOOP;
    END LOOP;
    RETURN NULLIF(key, '');
END $$;
CREATE OR REPLACE FUNCTION bond_credit_ncen_family_key(answer text, raw text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT CASE WHEN answer = 'Y' AND NOT bond_credit_ncen_is_sentinel(raw) THEN bond_credit_ncen_family_name_key(raw) END
$$;
CREATE OR REPLACE FUNCTION bond_credit_ncen_adviser_tokens(e jsonb) RETURNS text[]
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT ARRAY(SELECT t FROM pg_catalog.unnest(ARRAY[
        'FN:' || bond_credit_ncen_file_number(e ->> 'file_number_raw'),
        'CRD:' || bond_credit_ncen_crd(e ->> 'crd_raw'),
        'LEI:' || bond_credit_ncen_lei(e ->> 'lei_raw')]) t WHERE t IS NOT NULL)
$$;
CREATE OR REPLACE FUNCTION bond_credit_ncen_underwriter_tokens(e jsonb) RETURNS text[]
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT ARRAY(SELECT t FROM pg_catalog.unnest(ARRAY[
        'FN:' || bond_credit_ncen_file_number(e ->> 'file_number_raw'),
        'CRD:' || bond_credit_ncen_crd(e ->> 'crd_raw')]) t WHERE t IS NOT NULL)
$$;
-- NcenFilingEvidence.projection_digest: value encoding of the normalized projection.
CREATE OR REPLACE FUNCTION bond_credit_ncen_projection_digest(f bond_default_ncen_filing) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT bond_credit_encoded_digest(pg_catalog.jsonb_build_array(
        f.accession_number, COALESCE(f.registrant_cik, ''),
        COALESCE(pg_catalog.to_char(f.report_period_end, 'YYYY-MM-DD'), ''), COALESCE(f.family_answer, ''),
        COALESCE(bond_credit_ncen_family_key(f.family_answer, f.family_name_raw), ''),
        pg_catalog.to_jsonb(ARRAY(SELECT s FROM pg_catalog.unnest(f.reported_series_ids) s ORDER BY s COLLATE "C")),
        COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(a1, a2, a3, a4, a5)
                                              ORDER BY a1 COLLATE "C", a2 COLLATE "C", a3 COLLATE "C",
                                                       a4 COLLATE "C", a5 COLLATE "C")
                  FROM (SELECT COALESCE(e ->> 'series_id', '') AS a1, COALESCE(e ->> 'role', '') AS a2,
                               COALESCE(bond_credit_ncen_file_number(e ->> 'file_number_raw'), '') AS a3,
                               COALESCE(bond_credit_ncen_crd(e ->> 'crd_raw'), '') AS a4,
                               COALESCE(bond_credit_ncen_lei(e ->> 'lei_raw'), '') AS a5
                        FROM pg_catalog.jsonb_array_elements(f.adviser_records) e) x), '[]'::jsonb),
        COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(u1, u2, u3)
                                              ORDER BY u1 COLLATE "C", u2 COLLATE "C", u3 COLLATE "C")
                  FROM (SELECT COALESCE(bond_credit_ncen_file_number(e ->> 'file_number_raw'), '') AS u1,
                               COALESCE(bond_credit_ncen_crd(e ->> 'crd_raw'), '') AS u2,
                               COALESCE(bond_credit_ncen_lei(e ->> 'lei_raw'), '') AS u3
                        FROM pg_catalog.jsonb_array_elements(f.underwriter_records) e) y), '[]'::jsonb)))
$$;

-- FE-1a voter: a Y/N N-PORT lot of report date r usable at K (any CUSIP; no flag filter).
CREATE OR REPLACE FUNCTION bond_credit_votes(pkgs uuid[], stale_obs uuid[], k timestamptz, r date)
RETURNS SETOF bond_credit_observation LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT o.* FROM bond_credit_observation o
    WHERE o.package_id = ANY (pkgs) AND o.observation_kind = 'nport_holding' AND o.report_date = r
      AND o.cusip9 IS NOT NULL AND o.registrant_cik IS NOT NULL AND NOT (o.observation_id = ANY (stale_obs))
      AND o.public_available_at <= k AND o.field_presence ->> 'nport_is_default' = 'present'
      AND o.nport_is_default IN ('Y', 'N')
$$;
CREATE OR REPLACE FUNCTION bond_credit_fund_key(o bond_credit_observation) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT COALESCE(o.series_id, 'cik:' || o.registrant_cik::text)
$$;

-- Visible N-CEN rows of one registrant at K with their relied-data public time and the
-- per-accession representative (earliest data time, then id).
CREATE OR REPLACE FUNCTION bond_credit_ncen_visible(pkgs uuid[], stale_filings uuid[], k timestamptz, cik text)
RETURNS TABLE (fid uuid, acc text, period date, accepted timestamptz, pstatus text, form text, pdigest text,
               pkg uuid, data_at timestamptz, is_rep boolean)
LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    WITH vis AS (
        SELECT f.*, LEAST(f.public_available_at,
                          (SELECT pg_catalog.min(v.public_available_at) FROM bond_default_ncen_filing v
                           WHERE v.filing_evidence_id = ANY (f.version_evidence_filing_ids))) AS dpa
        FROM bond_default_ncen_filing f
        WHERE f.package_id = ANY (pkgs) AND f.registrant_cik = cik
          AND NOT (f.filing_evidence_id = ANY (stale_filings)))
    SELECT v.filing_evidence_id, v.accession_number, v.report_period_end, v.acceptance_at, v.parse_status,
           v.form_type, v.projection_digest, v.package_id, v.dpa,
           pg_catalog.row_number() OVER (PARTITION BY v.accession_number
                                         ORDER BY v.dpa, v.filing_evidence_id::text) = 1
    FROM vis v WHERE v.dpa <= k
$$;

-- publication.ncen_selection: {"selected", "blocking", "reason"} (reason null = usable).
CREATE OR REPLACE FUNCTION bond_credit_ncen_selection(pkgs uuid[], stale_filings uuid[], k timestamptz,
                                                      cik text, r date)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    floor_date date := (r - interval '15 months')::date;
    top_period date;
    top text[];
    blockers text[];
    horizon timestamptz;
    pick text;
    reason text;
    rep_id uuid;
    rep_status text;
    rep_form text;
    rep_pkg uuid;
    rep_accepted timestamptz;
BEGIN
    SELECT pg_catalog.max(v.period) INTO top_period FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v
    WHERE v.is_rep AND v.period BETWEEN floor_date AND r;
    top := ARRAY(SELECT v.acc FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v
                 WHERE v.is_rep AND v.period = top_period ORDER BY v.acc COLLATE "C");
    horizon := ((COALESCE(top_period, floor_date) + 1)::timestamp AT TIME ZONE 'America/New_York');
    blockers := ARRAY(SELECT v.acc FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v
                      WHERE v.is_rep AND v.period IS NULL AND v.data_at >= horizon ORDER BY v.acc COLLATE "C");
    IF cardinality(top) = 1 THEN
        pick := top[1];
    ELSIF cardinality(top) > 1 THEN
        IF EXISTS (SELECT 1 FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v
                   WHERE v.is_rep AND v.acc = ANY (top) AND v.accepted IS NULL)
           OR (SELECT pg_catalog.count(DISTINCT v.accepted) FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v
               WHERE v.is_rep AND v.acc = ANY (top)) <> cardinality(top) THEN
            reason := 'selection_order_unresolved';
        ELSE
            SELECT v.acc INTO pick FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v
            WHERE v.is_rep AND v.acc = ANY (top) ORDER BY v.accepted DESC LIMIT 1;
        END IF;
    END IF;
    IF reason IS NULL AND cardinality(blockers) > 0 THEN
        SELECT CASE WHEN v.pstatus = 'index_only' THEN 'filing_not_acquired' ELSE 'filing_period_unknown' END
        INTO reason FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v
        WHERE v.acc = ANY (blockers) ORDER BY v.fid::text LIMIT 1;
    END IF;
    IF reason IS NULL AND pick IS NULL THEN
        reason := CASE WHEN EXISTS (SELECT 1 FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v
                                    WHERE v.is_rep AND v.period < floor_date)
                       THEN 'effective_filing_older_than_15_months' ELSE 'no_effective_filing' END;
    END IF;
    IF pick IS NOT NULL THEN
        SELECT v.fid, v.pstatus, v.form, v.pkg, v.accepted
        INTO rep_id, rep_status, rep_form, rep_pkg, rep_accepted
        FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v WHERE v.is_rep AND v.acc = pick;
    END IF;
    IF reason IS NULL AND pick IS NOT NULL THEN
        IF (SELECT pg_catalog.count(DISTINCT pg_catalog.jsonb_build_array(v.pdigest, v.form, v.period, v.accepted,
                                                                           v.pstatus))
            FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v WHERE v.acc = pick) > 1 THEN
            reason := 'accession_copies_conflict';
        ELSIF rep_status = 'index_only' THEN
            reason := 'filing_not_acquired';
        ELSIF rep_status <> 'parsed' THEN
            reason := 'effective_filing_quarantined';
        ELSIF rep_accepted IS NULL OR rep_accepted > k THEN
            reason := 'filing_acceptance_unattested';
        ELSIF rep_form = 'N-CEN/A' AND NOT EXISTS (
                SELECT 1 FROM bond_default_source_package p
                WHERE p.package_id = rep_pkg AND p.source_family = 'sec_ncen_public_xml'
                  AND p.schema_version = ANY (ARRAY['X0505'])) THEN
            reason := 'amendment_semantics_unknown';
        END IF;
    END IF;
    RETURN pg_catalog.jsonb_build_object(
        'selected', rep_id, 'reason', reason,
        'blocking', pg_catalog.to_jsonb(ARRAY(
            SELECT DISTINCT v.fid FROM bond_credit_ncen_visible(pkgs, stale_filings, k, cik) v
            WHERE v.acc = ANY (top || blockers) AND v.fid IS DISTINCT FROM rep_id ORDER BY v.fid)));
END $$;

-- publication.ncen_profile_reasons: FE-1 completeness and FE-1b series coverage (C-sorted).
CREATE OR REPLACE FUNCTION bond_credit_ncen_profile_reasons(f bond_default_ncen_filing, voting_keys text[])
RETURNS text[] LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    WITH funds AS (
        SELECT s AS series FROM pg_catalog.unnest(f.reported_series_ids) s
        UNION SELECT e ->> 'series_id' FROM pg_catalog.jsonb_array_elements(f.adviser_records) e),
    reasons(reason) AS (
        SELECT 'b5_unanswered' WHERE f.family_answer IS NULL
        UNION ALL SELECT 'b5_family_name_missing'
        WHERE f.family_answer = 'Y' AND bond_credit_ncen_clean(f.family_name_raw) IS NULL
        UNION ALL SELECT 'b5_family_name_sentinel'
        WHERE f.family_answer = 'Y' AND bond_credit_ncen_clean(f.family_name_raw) IS NOT NULL
          AND bond_credit_ncen_is_sentinel(f.family_name_raw)
        UNION ALL SELECT 'b5_family_name_unparseable'
        WHERE f.family_answer = 'Y' AND bond_credit_ncen_clean(f.family_name_raw) IS NOT NULL
          AND NOT bond_credit_ncen_is_sentinel(f.family_name_raw)
          AND bond_credit_ncen_family_name_key(f.family_name_raw) IS NULL
        UNION ALL SELECT 'no_funds' WHERE NOT EXISTS (SELECT 1 FROM funds)
        UNION ALL SELECT 'fund_without_current_adviser'
        WHERE EXISTS (SELECT 1 FROM funds x WHERE NOT EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_array_elements(f.adviser_records) e
            WHERE (e ->> 'series_id') IS NOT DISTINCT FROM x.series AND e ->> 'role' = 'adviser'))
        UNION ALL SELECT 'adviser_without_identifier'
        WHERE EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(f.adviser_records) e
                      WHERE cardinality(bond_credit_ncen_adviser_tokens(e)) = 0)
        UNION ALL SELECT 'underwriter_without_identifier'
        WHERE EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(f.underwriter_records) e
                      WHERE cardinality(bond_credit_ncen_underwriter_tokens(e)) = 0)
        UNION ALL SELECT 'voting_without_series_id'
        WHERE EXISTS (SELECT 1 FROM pg_catalog.unnest(voting_keys) vk WHERE vk !~ '^S[0-9]{9}$')
        UNION ALL SELECT 'voting_series_absent_from_effective_ncen'
        WHERE EXISTS (SELECT 1 FROM pg_catalog.unnest(voting_keys) vk
                      WHERE vk ~ '^S[0-9]{9}$' AND NOT (vk = ANY (f.reported_series_ids))))
    SELECT ARRAY(SELECT DISTINCT reason COLLATE "C" FROM reasons ORDER BY 1)
$$;

-- publication.ncen_components: union-find over adviser, underwriter and B.5 name tokens of the
-- complete profiles; component id = W3 component_id_for(rule, R, K, mode, sorted members).
CREATE OR REPLACE FUNCTION bond_credit_ncen_components(ciks text[], fids uuid[], r date, k timestamptz, mode text)
RETURNS TABLE (cik text, component_id text) LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    WITH RECURSIVE prof AS (SELECT u.c, u.f FROM ROWS FROM (pg_catalog.unnest(ciks), pg_catalog.unnest(fids)) AS u(c, f)),
    tok AS (
        SELECT p.c, 'adv|' || t AS token FROM prof p JOIN bond_default_ncen_filing n ON n.filing_evidence_id = p.f
        CROSS JOIN pg_catalog.jsonb_array_elements(n.adviser_records) e
        CROSS JOIN pg_catalog.unnest(bond_credit_ncen_adviser_tokens(e)) t
        UNION SELECT p.c, 'uw|' || t FROM prof p JOIN bond_default_ncen_filing n ON n.filing_evidence_id = p.f
        CROSS JOIN pg_catalog.jsonb_array_elements(n.underwriter_records) e
        CROSS JOIN pg_catalog.unnest(bond_credit_ncen_underwriter_tokens(e)) t
        UNION SELECT p.c, 'name|' || bond_credit_ncen_family_key(n.family_answer, n.family_name_raw)
        FROM prof p JOIN bond_default_ncen_filing n ON n.filing_evidence_id = p.f
        WHERE bond_credit_ncen_family_key(n.family_answer, n.family_name_raw) IS NOT NULL),
    edge AS (SELECT DISTINCT a.c AS a, b.c AS b FROM tok a JOIN tok b ON a.token = b.token),
    reach(c, other) AS (SELECT prof.c, prof.c FROM prof
                        UNION SELECT reach.c, edge.b FROM reach JOIN edge ON edge.a = reach.other)
    SELECT reach.c, 'ncenfam:' || pg_catalog.substr(bond_credit_text_digest(
        '[' || pg_catalog.to_json((SELECT x.family_rule_version FROM bond_credit_expected_pins() x))::text
        || ',' || pg_catalog.to_json(pg_catalog.to_char(r, 'YYYY-MM-DD'))::text
        || ',' || pg_catalog.to_json(bond_credit_ts_text(k))::text || ',' || pg_catalog.to_json(mode)::text
        || ',' || bond_credit_json_texts(pg_catalog.array_agg(reach.other)) || ']'), 8, 32)
    FROM reach GROUP BY reach.c
$$;

-- publication.expected_context: the full FE-1 context of R at K recomputed from the inventory.
CREATE OR REPLACE FUNCTION bond_credit_family_expected(pkgs uuid[], stale_obs uuid[], stale_filings uuid[],
                                                       r date, k timestamptz, mode text)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    cik text;
    keys text[];
    sel jsonb;
    selected uuid;
    reason text;
    reasons text[];
    blocking uuid[];
    members jsonb := '{}'::jsonb;
    selection uuid[] := '{}';
    prof_ciks text[] := '{}';
    prof_fids uuid[] := '{}';
    universe jsonb := '[]'::jsonb;
    comp record;
    universe_ciks text[];
BEGIN
    IF EXISTS (SELECT 1 FROM bond_credit_votes(pkgs, stale_obs, k, r) o
               GROUP BY o.registrant_cik, bond_credit_fund_key(o) HAVING pg_catalog.count(DISTINCT o.accession_number) > 1) THEN
        RETURN pg_catalog.jsonb_build_object('not_closed', true);
    END IF;
    universe_ciks := ARRAY(SELECT DISTINCT o.registrant_cik::text COLLATE "C" FROM bond_credit_votes(pkgs, stale_obs, k, r) o
                           ORDER BY 1);
    FOREACH cik IN ARRAY universe_ciks LOOP
        keys := ARRAY(SELECT DISTINCT bond_credit_fund_key(o) COLLATE "C" FROM bond_credit_votes(pkgs, stale_obs, k, r) o
                      WHERE o.registrant_cik = cik ORDER BY 1);
        sel := bond_credit_ncen_selection(pkgs, stale_filings, k, cik, r);
        selected := (sel ->> 'selected')::uuid;
        reason := sel ->> 'reason';
        blocking := ARRAY(SELECT x::uuid FROM pg_catalog.jsonb_array_elements_text(sel -> 'blocking') x);
        IF reason IS NULL AND selected IS NOT NULL THEN
            reasons := (SELECT bond_credit_ncen_profile_reasons(f, keys) FROM bond_default_ncen_filing f
                        WHERE f.filing_evidence_id = selected);
        ELSE
            reasons := ARRAY[COALESCE(reason, 'no_effective_filing')];
        END IF;
        IF cardinality(reasons) = 0 THEN
            prof_ciks := prof_ciks || cik;
            prof_fids := prof_fids || selected;
        END IF;
        IF selected IS NOT NULL THEN
            selection := selection || selected || (SELECT f.version_evidence_filing_ids FROM bond_default_ncen_filing f
                                                   WHERE f.filing_evidence_id = selected);
        END IF;
        selection := selection || blocking;
        members := members || pg_catalog.jsonb_build_object(cik, pg_catalog.jsonb_build_object(
            'voting_series_ids', pg_catalog.to_jsonb(keys),
            'vote_observation_ids', pg_catalog.to_jsonb(ARRAY(
                SELECT o.observation_id FROM bond_credit_votes(pkgs, stale_obs, k, r) o
                WHERE o.registrant_cik = cik ORDER BY 1)),
            'selected_filing_id', selected, 'blocking_filing_ids', pg_catalog.to_jsonb(blocking),
            'state', CASE WHEN cardinality(reasons) = 0 THEN 'complete' ELSE 'incomplete' END,
            'reasons', pg_catalog.to_jsonb(reasons), 'component_id', NULL));
        universe := universe || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_array(
            cik, pg_catalog.to_jsonb(keys), pg_catalog.to_jsonb(ARRAY(
                SELECT o.observation_id::text FROM bond_credit_votes(pkgs, stale_obs, k, r) o
                WHERE o.registrant_cik = cik ORDER BY o.observation_id))));
    END LOOP;
    FOR comp IN SELECT * FROM bond_credit_ncen_components(prof_ciks, prof_fids, r, k, mode) LOOP
        members := pg_catalog.jsonb_set(members, ARRAY[comp.cik, 'component_id'], pg_catalog.to_jsonb(comp.component_id));
    END LOOP;
    selection := ARRAY(SELECT DISTINCT s FROM pg_catalog.unnest(selection) s ORDER BY 1);
    RETURN pg_catalog.jsonb_build_object(
        'not_closed', false,
        'vote_observation_ids', pg_catalog.to_jsonb(ARRAY(SELECT o.observation_id FROM bond_credit_votes(pkgs, stale_obs, k, r) o
                                                          ORDER BY 1)),
        'members', members,
        'selection_filing_ids', pg_catalog.to_jsonb(selection),
        'index_package_ids', pg_catalog.to_jsonb(ARRAY(
            SELECT DISTINCT f.package_id FROM bond_default_ncen_filing f
            JOIN bond_default_source_package p ON p.package_id = f.package_id
            WHERE f.package_id = ANY (pkgs) AND f.registrant_cik = ANY (universe_ciks)
              AND p.source_family = 'sec_edgar_index' ORDER BY 1)),
        'universe_digest', bond_credit_encoded_digest(universe),
        'evidence_known_at', (SELECT pg_catalog.max(t) FROM (
            SELECT o.public_available_at AS t FROM bond_credit_votes(pkgs, stale_obs, k, r) o
            UNION ALL
            SELECT LEAST(f.public_available_at, (SELECT pg_catalog.min(v.public_available_at) FROM bond_default_ncen_filing v
                                                 WHERE v.filing_evidence_id = ANY (f.version_evidence_filing_ids)))
            FROM bond_default_ncen_filing f WHERE f.filing_evidence_id = ANY (selection)
            UNION ALL
            SELECT p.first_verified_public_at FROM bond_default_ncen_filing f
            JOIN bond_default_source_package p ON p.package_id = f.package_id
            WHERE f.package_id = ANY (pkgs) AND f.registrant_cik = ANY (universe_ciks)
              AND p.source_family = 'sec_edgar_index') s));
END $$;

-- Effective issue_scope decision admitting a quarantined link (human accepted_evidence head).
CREATE OR REPLACE FUNCTION bond_credit_scope_head(pkgs uuid[], eff_ids uuid[], policy text, lid uuid)
RETURNS uuid LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT a.adjudication_id FROM bond_default_adjudication a
    WHERE a.adjudication_id = ANY (eff_ids) AND a.package_id = ANY (pkgs) AND a.subject_kind = 'issue_scope'
      AND a.subject_id = lid AND a.status = 'accepted_evidence' AND a.reviewer_role = 'human_reviewer'
      AND a.policy_digest = policy
$$;
-- publication._Index.link_time: link knowledge plus an admitting scope decision's support.
CREATE OR REPLACE FUNCTION bond_credit_link_time(pkgs uuid[], eff_ids uuid[], policy text, lid uuid)
RETURNS timestamptz LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT GREATEST(l.link_known_at,
                    (SELECT pg_catalog.max(o.public_available_at) FROM bond_default_adjudication a
                     JOIN bond_credit_observation o ON o.observation_id = ANY (a.evidence_observation_ids)
                     WHERE l.status = 'quarantined' AND a.adjudication_id = bond_credit_scope_head(pkgs, eff_ids, policy, lid)),
                    (SELECT pg_catalog.max(x.link_known_at) FROM bond_default_adjudication a
                     JOIN bond_default_event_link x ON x.link_id = ANY (a.link_ids)
                     WHERE l.status = 'quarantined' AND a.adjudication_id = bond_credit_scope_head(pkgs, eff_ids, policy, lid)))
    FROM bond_default_event_link l WHERE l.link_id = lid
$$;
CREATE OR REPLACE FUNCTION bond_credit_usable_link(pkgs uuid[], stale_links uuid[], eff_ids uuid[], policy text,
                                                   lid uuid)
RETURNS boolean LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT COALESCE((SELECT l.package_id = ANY (pkgs) AND NOT (l.link_id = ANY (stale_links))
                            AND (l.status = 'admitted' OR (l.status = 'quarantined'
                                 AND bond_credit_scope_head(pkgs, eff_ids, policy, lid) IS NOT NULL))
                     FROM bond_default_event_link l WHERE l.link_id = lid), false)
$$;

-- publication._corroboration_ok (day NULL = no date applicability test).
CREATE OR REPLACE FUNCTION bond_credit_corroboration_ok(pkgs uuid[], stale_obs uuid[], stale_links uuid[],
                                                        eff_ids uuid[], policy text, aid uuid, cusip text, day date)
RETURNS boolean LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT COALESCE((
        SELECT a.package_id = ANY (pkgs) AND a.adjudication_id = ANY (eff_ids) AND a.subject_kind = 'corroboration'
           AND a.status = 'accepted_evidence' AND a.reviewer_role = 'human_reviewer' AND a.policy_digest = policy
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(a.link_ids) lid
                           WHERE NOT bond_credit_usable_link(pkgs, stale_links, eff_ids, policy, lid)
                              OR NOT EXISTS (SELECT 1 FROM bond_default_event_link l
                                             WHERE l.link_id = lid AND l.cusip9 = cusip
                                               AND l.affected_scope IN ('issue', 'issuer_affected_obligation')))
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(a.evidence_observation_ids) oid
                           WHERE NOT EXISTS (
                               SELECT 1 FROM bond_credit_observation o
                               JOIN bond_default_source_package p ON p.package_id = o.package_id
                               WHERE o.observation_id = oid AND o.package_id = ANY (pkgs)
                                 AND NOT (oid = ANY (stale_obs)) AND o.observation_kind <> 'nport_holding'
                                 AND (o.observation_kind <> 'agency_action'
                                      OR (p.source_family = 'agency_rocr_xbrl' AND p.rights_state = 'approved'))
                                 AND EXISTS (SELECT 1 FROM bond_default_event_link l
                                             WHERE l.link_id = ANY (a.link_ids) AND l.observation_id = oid)))
           AND (day IS NULL OR (a.support_valid_from <= day AND (a.support_valid_to IS NULL OR day < a.support_valid_to)))
        FROM bond_default_adjudication a WHERE a.adjudication_id = aid), false)
$$;

-- publication.vote_state for (cusip9, R) under the persisted context memberships.
CREATE OR REPLACE FUNCTION bond_credit_vote_state(pkgs uuid[], stale_obs uuid[], k timestamptz, r date, cusip text,
                                                  pub_id uuid, ctx uuid, corroborated boolean)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    keyed jsonb;
    y_ids uuid[];
    n_ids uuid[];
    side text;
    n_keys integer;
    n_series integer;
    n_funds integer;
    n_evidenced integer;
    has_unknown boolean;
    relied text[];
    ok boolean;
    basis text;
BEGIN
    -- One value per (accession, fund key): disputed when both Y and N lots exist.
    keyed := COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('fk', s.fk, 'reg', s.reg, 'val', s.val))
                       FROM (SELECT bond_credit_fund_key(o) AS fk, pg_catalog.max(o.registrant_cik::text) AS reg,
                                    CASE WHEN pg_catalog.bool_or(o.nport_is_default = 'Y')
                                              AND pg_catalog.bool_or(o.nport_is_default = 'N') THEN 'disputed'
                                         WHEN pg_catalog.bool_or(o.nport_is_default = 'Y') THEN 'Y' ELSE 'N' END AS val
                             FROM bond_credit_votes(pkgs, stale_obs, k, r) o WHERE o.cusip9 = cusip
                             GROUP BY o.accession_number, bond_credit_fund_key(o)) s), '[]'::jsonb);
    y_ids := ARRAY(SELECT o.observation_id FROM bond_credit_votes(pkgs, stale_obs, k, r) o
                   WHERE o.cusip9 = cusip AND o.nport_is_default = 'Y' ORDER BY 1);
    n_ids := ARRAY(SELECT o.observation_id FROM bond_credit_votes(pkgs, stale_obs, k, r) o
                   WHERE o.cusip9 = cusip AND o.nport_is_default = 'N' ORDER BY 1);
    IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(keyed) e WHERE e ->> 'val' = 'disputed')
       OR (EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(keyed) e WHERE e ->> 'val' = 'Y')
           AND EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(keyed) e WHERE e ->> 'val' = 'N')) THEN
        RETURN pg_catalog.jsonb_build_object('status', 'conflict', 'y_ids', pg_catalog.to_jsonb(y_ids),
            'n_ids', pg_catalog.to_jsonb(n_ids), 'relied', '[]'::jsonb,
            'basis', CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(keyed) e WHERE e ->> 'val' = 'disputed')
                          THEN 'contradictory_lots' ELSE 'material_y_n_conflict' END);
    END IF;
    IF pg_catalog.jsonb_array_length(keyed) = 0 THEN
        RETURN pg_catalog.jsonb_build_object('status', 'no_informative_vote', 'basis', 'no_y_or_n_vote',
            'y_ids', pg_catalog.to_jsonb(y_ids), 'n_ids', pg_catalog.to_jsonb(n_ids), 'relied', '[]'::jsonb);
    END IF;
    side := CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(keyed) e WHERE e ->> 'val' = 'Y')
                 THEN 'Y' ELSE 'N' END;
    SELECT pg_catalog.count(*), pg_catalog.count(*) FILTER (WHERE v.fk ~ '^S[0-9]{9}$'),
           pg_catalog.count(DISTINCT v.fk) FILTER (WHERE v.fk ~ '^S[0-9]{9}$'),
           pg_catalog.count(DISTINCT m.component_id) FILTER (WHERE v.fk ~ '^S[0-9]{9}$' AND m.state = 'complete'),
           COALESCE(pg_catalog.bool_or(m.component_id IS NULL OR m.state IS DISTINCT FROM 'complete')
                    FILTER (WHERE v.fk ~ '^S[0-9]{9}$'), false),
           ARRAY(SELECT DISTINCT w.reg COLLATE "C"
                 FROM pg_catalog.jsonb_to_recordset(keyed) AS w(fk text, reg text, val text)
                 WHERE w.val = side AND w.fk ~ '^S[0-9]{9}$' ORDER BY 1)
    INTO n_keys, n_series, n_funds, n_evidenced, has_unknown, relied
    FROM pg_catalog.jsonb_to_recordset(keyed) AS v(fk text, reg text, val text)
    LEFT JOIN bond_default_family_evidence_v2 m
      ON m.publication_id = pub_id AND m.context_id = ctx AND m.registrant_cik = v.reg
    WHERE v.val = side;
    IF n_funds < 2 THEN
        ok := false;
        basis := CASE WHEN n_series <> n_keys THEN 'series_id_missing' ELSE 'single_series' END;
    ELSIF n_evidenced >= 2 THEN
        ok := true; basis := 'independent_families';
    ELSIF has_unknown THEN
        ok := false; basis := 'family_independence_unknown';
    ELSIF side = 'Y' AND corroborated THEN
        ok := true; basis := 'same_family_corroborated';
    ELSE
        ok := false; basis := CASE WHEN side = 'Y' THEN 'same_family_uncorroborated' ELSE 'same_family' END;
    END IF;
    RETURN pg_catalog.jsonb_build_object(
        'status', CASE WHEN ok THEN 'consensus_' ELSE 'candidate_' END || pg_catalog.lower(side), 'basis', basis,
        'y_ids', pg_catalog.to_jsonb(y_ids), 'n_ids', pg_catalog.to_jsonb(n_ids), 'relied', pg_catalog.to_jsonb(relied));
END $$;

-- contracts.classify_rating_action: 'rated:<bucket>' / 'withdrawn' / 'unmapped'.
CREATE OR REPLACE FUNCTION bond_credit_rating_action_kind(rac text, symbol text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT CASE
        WHEN bond_credit_ascii_upper(pg_catalog.btrim(COALESCE(rac, ''), E' \t\n\r' || pg_catalog.chr(11) || pg_catalog.chr(12)))
                 = ANY (ARRAY['WD', 'WE', 'WO', 'WR'])
          OR bond_credit_ascii_upper(s) = ANY (ARRAY['WD', 'WR', 'NR']) THEN 'withdrawn'
        WHEN s ~ '^(?:AAA|Aaa)$' THEN 'rated:AAA'
        WHEN s ~ '^(?:AA[+-]?|AA ?\((?:high|low)\)|Aa[1-3]?)$' THEN 'rated:AA'
        WHEN s ~ '^(?:A[+-]?|A ?\((?:high|low)\)|A[1-3])$' THEN 'rated:A'
        WHEN s ~ '^(?:BBB[+-]?|BBB ?\((?:high|low)\)|Baa[1-3]?)$' THEN 'rated:BBB'
        WHEN s ~ '^(?:BB[+-]?|BB ?\((?:high|low)\)|Ba[1-3]?)$' THEN 'rated:BB'
        WHEN s ~ '^(?:B[+-]?|B ?\((?:high|low)\)|B[1-3])$' THEN 'rated:B'
        WHEN s ~ '^(?:CCC[+-]?|CCC ?\((?:high|low)\)|CC|C|Caa[1-3]?|Ca)$' THEN 'rated:CCC'
        WHEN s ~ '^(?:D)$' THEN 'rated:D'
        ELSE 'unmapped' END
    FROM (SELECT pg_catalog.btrim(COALESCE(symbol, ''), E' \t\n\r' || pg_catalog.chr(11) || pg_catalog.chr(12)) AS s) x
$$;

-- contracts.rating_action_input_digest over relied actions, their packages and binding links.
CREATE OR REPLACE FUNCTION bond_credit_rating_action_input_digest(view_kind text, action_ids uuid[], binding uuid[])
RETURNS text LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT bond_credit_text_digest(
        '{"link_row_sha256":' || bond_credit_json_texts(ARRAY(
            SELECT DISTINCT bond_credit_checked_row_sha256('event_links', pg_catalog.to_jsonb(l))
            FROM bond_default_event_link l WHERE l.link_id = ANY (binding)))
        || ',"observation_row_sha256":' || bond_credit_json_texts(ARRAY(
            SELECT DISTINCT bond_credit_checked_row_sha256('observations', pg_catalog.to_jsonb(o))
            FROM bond_credit_observation o WHERE o.observation_id = ANY (action_ids)))
        || ',"package_row_sha256":' || bond_credit_json_texts(ARRAY(
            SELECT DISTINCT bond_credit_checked_row_sha256('source_packages', pg_catalog.to_jsonb(p))
            FROM bond_default_source_package p
            WHERE p.package_id IN (SELECT o.package_id FROM bond_credit_observation o WHERE o.observation_id = ANY (action_ids))))
        || ',"resolver":' || pg_catalog.to_json((SELECT x.rating_resolver_id FROM bond_credit_expected_pins() x))::text
        || ',"view_kind":' || pg_catalog.to_json(view_kind)::text || '}')
$$;

CREATE OR REPLACE FUNCTION bond_credit_validate(target_publication_id uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    pub bond_credit_publications%ROWTYPE;
    k timestamptz;
    current_run boolean;
    bad text;
    view_name text;
    pkgs uuid[];
    eff_ids uuid[];
    stale_obs uuid[];
    stale_links uuid[];
    stale_filings uuid[];
    ctx record;
    prop record;
    prev_date date;
    want jsonb;
    ctx_u uuid;
    ctx_l uuid;
    ctx_d uuid;
    st jsonb;
    st_l jsonb;
    family uuid[];
    lower_ids uuid[];
    cor_obs uuid[];
    day date;
    ok boolean;
    known timestamptz;
    rating_packages_digest text;
BEGIN
    SELECT * INTO pub FROM bond_credit_publications
    WHERE publication_id = target_publication_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'bond_credit_validate:unknown_publication:%', target_publication_id;
    END IF;
    IF pub.lifecycle_state <> 'prepared' THEN
        RAISE EXCEPTION 'bond_credit_validate:already_validated:%', target_publication_id;
    END IF;
    k := pub.knowledge_cutoff;
    current_run := pub.knowledge_mode = 'current_run';

    pkgs := ARRAY(SELECT s.package_id FROM bond_credit_publication_sources s
                  WHERE s.publication_id = pub.publication_id ORDER BY s.package_id);

    -- 0. Contract identity pinned in SQL (generated pins; never writer-selected), then the
    --    fingerprint, receipt digest and UUIDv8 id recomputed from the stored manifest fields.
    IF (pub.contract_version, pub.policy_digest, pub.contract_digest) IS DISTINCT FROM
       (SELECT (x.contract_version, x.policy_digest, x.contract_digest) FROM bond_credit_expected_pins() x) THEN
        RAISE EXCEPTION 'bond_credit_validate:contract_pin_mismatch:%', target_publication_id;
    END IF;
    IF bond_credit_fingerprint_digest(pg_catalog.to_jsonb(pub)) IS DISTINCT FROM pub.fingerprint_digest THEN
        RAISE EXCEPTION 'bond_credit_validate:fingerprint_mismatch:%', target_publication_id;
    END IF;
    IF bond_credit_publication_id_for(pub.fingerprint_digest) IS DISTINCT FROM pub.publication_id THEN
        RAISE EXCEPTION 'bond_credit_validate:publication_id_mismatch:%', target_publication_id;
    END IF;
    IF bond_credit_validation_digest(pg_catalog.to_jsonb(pub)) IS DISTINCT FROM pub.validation_digest THEN
        RAISE EXCEPTION 'bond_credit_validate:validation_digest_mismatch:%', target_publication_id;
    END IF;

    -- 1. Frame row counts and digests over row hashes recomputed from the stored columns
    --    (a stored row_sha256 that differs from its recomputation fails).
    PERFORM bond_credit_assert_output_frames('bond_credit_validate', pub);

    -- 2. Source lineage and ledger inventories.
    SELECT s.package_id::text INTO bad
    FROM bond_credit_publication_sources s JOIN bond_default_source_package p ON p.package_id = s.package_id
    WHERE s.publication_id = pub.publication_id
      AND (p.content_sha256 <> s.package_content_sha256 OR s.role <> bond_credit_family_role(p.source_family)
           OR (current_run AND p.retrieved_at > k))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:source_lineage_mismatch:%', bad;
    END IF;
    SELECT p.package_id::text INTO bad
    FROM bond_default_source_package p JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
    WHERE p.revision_of_package_id IS NOT NULL AND NOT (p.revision_of_package_id = ANY (pkgs))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:source_lineage_mismatch:%', bad;
    END IF;
    SELECT s.package_id::text INTO bad
    FROM bond_credit_publication_sources s
    CROSS JOIN LATERAL (
        SELECT ARRAY(SELECT o.observation_id FROM bond_credit_observation o WHERE o.package_id = s.package_id
                     UNION ALL SELECT l.link_id FROM bond_default_event_link l WHERE l.package_id = s.package_id
                     UNION ALL SELECT a.adjudication_id FROM bond_default_adjudication a WHERE a.package_id = s.package_id
                     UNION ALL SELECT f.filing_evidence_id FROM bond_default_ncen_filing f WHERE f.package_id = s.package_id) AS ids
    ) owned
    WHERE s.publication_id = pub.publication_id
      AND (cardinality(owned.ids) <> s.row_count OR bond_credit_id_digest(owned.ids) <> s.row_inventory_digest)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:source_inventory_mismatch:%', bad;
    END IF;
    IF bond_credit_frame_digest(ARRAY(SELECT bond_credit_checked_row_sha256('source_packages', pg_catalog.to_jsonb(p))
                                      FROM bond_default_source_package p
                                      JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id))) <> pub.package_inventory_digest
       OR bond_credit_frame_digest(ARRAY(SELECT bond_credit_checked_row_sha256('observations', pg_catalog.to_jsonb(o))
                                         FROM bond_credit_observation o
                                         JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id))) <> pub.observation_inventory_digest
       OR bond_credit_frame_digest(ARRAY(SELECT bond_credit_checked_row_sha256('event_links', pg_catalog.to_jsonb(l))
                                         FROM bond_default_event_link l
                                         JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id))) <> pub.link_inventory_digest
       OR bond_credit_frame_digest(ARRAY(SELECT bond_credit_checked_row_sha256('adjudications', pg_catalog.to_jsonb(a))
                                         FROM bond_default_adjudication a
                                         JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id))) <> pub.adjudication_inventory_digest
       OR bond_credit_frame_digest(ARRAY(SELECT bond_credit_checked_row_sha256('ncen_filings', pg_catalog.to_jsonb(f))
                                         FROM bond_default_ncen_filing f
                                         JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id))) <> pub.ncen_filing_inventory_digest THEN
        RAISE EXCEPTION 'bond_credit_validate:inventory_digest_mismatch';
    END IF;
    SELECT CASE WHEN pg_catalog.count(*) = 0 THEN NULL ELSE bond_credit_frame_digest(ARRAY(
               SELECT bond_credit_checked_row_sha256('source_packages', pg_catalog.to_jsonb(p))
               FROM bond_default_source_package p
               JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
               WHERE p.source_family = 'agency_rocr_xbrl' AND p.rights_state = 'approved')) END
    INTO rating_packages_digest
    FROM bond_default_source_package p
    JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
    WHERE p.source_family = 'agency_rocr_xbrl' AND p.rights_state = 'approved';
    IF NOT bond_credit_rating_declarations_valid(pub.rating_declarations)
       OR pub.rating_declarations_digest IS DISTINCT FROM bond_credit_json_digest(pub.rating_declarations) THEN
        RAISE EXCEPTION 'bond_credit_validate:rating_declarations_invalid';
    END IF;
    IF pub.rating_input_digest IS DISTINCT FROM
       bond_credit_rating_input_digest(pub.rating_declarations, rating_packages_digest) THEN
        RAISE EXCEPTION 'bond_credit_validate:rating_input_digest_mismatch';
    END IF;
    IF current_run AND (
        EXISTS (SELECT 1 FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                WHERE o.first_seen_at > k)
        OR EXISTS (SELECT 1 FROM bond_default_adjudication a JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                   WHERE a.adjudicated_at > k)) THEN
        RAISE EXCEPTION 'bond_credit_validate:current_run_input_after_cutoff';
    END IF;
    IF current_run AND pub.validation_issued_at > k THEN
        RAISE EXCEPTION 'bond_credit_validate:validation_receipt_after_cutoff';
    END IF;
    -- Inventory closure: every reference made by an input row stays inside the inventory.
    SELECT o.observation_id::text INTO bad
    FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
    WHERE o.supersedes_observation_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM bond_credit_observation s JOIN pg_catalog.unnest(pkgs) AS w(package_id) USING (package_id)
                      WHERE s.observation_id = o.supersedes_observation_id)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:observation_supersedes_outside_inventory:%', bad;
    END IF;
    SELECT l.link_id::text INTO bad
    FROM bond_default_event_link l JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
    WHERE NOT EXISTS (SELECT 1 FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS w(package_id) USING (package_id)
                      WHERE o.observation_id = l.observation_id)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:link_observation_outside_inventory:%', bad;
    END IF;
    SELECT l.link_id::text INTO bad
    FROM bond_default_event_link l JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
    WHERE l.supersedes_link_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM bond_default_event_link s JOIN pg_catalog.unnest(pkgs) AS w(package_id) USING (package_id)
                      WHERE s.link_id = l.supersedes_link_id)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:link_supersedes_outside_inventory:%', bad;
    END IF;
    SELECT a.adjudication_id::text INTO bad
    FROM bond_default_adjudication a JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
    WHERE EXISTS (SELECT 1 FROM pg_catalog.unnest(a.evidence_observation_ids) oid
                  WHERE NOT EXISTS (SELECT 1 FROM bond_credit_observation o
                                    JOIN pg_catalog.unnest(pkgs) AS w(package_id) USING (package_id)
                                    WHERE o.observation_id = oid))
       OR EXISTS (SELECT 1 FROM pg_catalog.unnest(a.link_ids) lid
                  WHERE NOT EXISTS (SELECT 1 FROM bond_default_event_link l
                                    JOIN pg_catalog.unnest(pkgs) AS w(package_id) USING (package_id)
                                    WHERE l.link_id = lid))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:adjudication_evidence_outside_inventory:%', bad;
    END IF;
    -- Observation/link revision chains resolved as of K. A revision counts only when known by K
    -- (observation public_available_at, link link_known_at); later revisions are ignored. Chains
    -- are acyclic and no record has two counted revisions. Stale = superseded by a counted
    -- revision; a retraction observation is never evidence.
    bad := (SELECT bond_credit_revision_cycle(pg_catalog.array_agg(p.package_id),
                                              pg_catalog.array_agg(p.revision_of_package_id))::text
            FROM bond_default_source_package p JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id));
    IF bad IS NULL THEN
        bad := (SELECT bond_credit_revision_cycle(pg_catalog.array_agg(o.observation_id),
                                              pg_catalog.array_agg(o.supersedes_observation_id))::text
                FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id));
    END IF;
    IF bad IS NULL THEN
        bad := (SELECT bond_credit_revision_cycle(pg_catalog.array_agg(l.link_id),
                                                  pg_catalog.array_agg(l.supersedes_link_id))::text
                FROM bond_default_event_link l JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id));
    END IF;
    IF bad IS NULL THEN
        bad := (SELECT bond_credit_revision_cycle(pg_catalog.array_agg(f.filing_evidence_id),
                                                  pg_catalog.array_agg(f.supersedes_filing_evidence_id))::text
                FROM bond_default_ncen_filing f JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id));
    END IF;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:supersession_cycle:%', bad;
    END IF;
    SELECT o.supersedes_observation_id::text INTO bad
    FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
    WHERE o.supersedes_observation_id IS NOT NULL AND o.public_available_at <= k
      AND (NOT current_run OR o.first_seen_at <= k)
    GROUP BY o.supersedes_observation_id HAVING count(*) > 1 LIMIT 1;
    IF bad IS NULL THEN
        SELECT l.supersedes_link_id::text INTO bad
        FROM bond_default_event_link l JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
        WHERE l.supersedes_link_id IS NOT NULL AND l.link_known_at <= k
        GROUP BY l.supersedes_link_id HAVING count(*) > 1 LIMIT 1;
    END IF;
    IF bad IS NULL THEN
        SELECT f.supersedes_filing_evidence_id::text INTO bad
        FROM bond_default_ncen_filing f JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
        WHERE f.supersedes_filing_evidence_id IS NOT NULL AND f.public_available_at <= k
          AND (NOT current_run OR f.first_seen_at <= k)
        GROUP BY f.supersedes_filing_evidence_id HAVING count(*) > 1 LIMIT 1;
    END IF;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:revision_chain_fork:%', bad;
    END IF;
    stale_obs := ARRAY(
        SELECT o.supersedes_observation_id
        FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
        WHERE o.supersedes_observation_id IS NOT NULL AND o.public_available_at <= k
          AND (NOT current_run OR o.first_seen_at <= k)
        UNION
        SELECT o.observation_id
        FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
        WHERE o.revision_kind = 'retraction');
    stale_links := ARRAY(
        SELECT l.supersedes_link_id
        FROM bond_default_event_link l JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
        WHERE l.supersedes_link_id IS NOT NULL AND l.link_known_at <= k);
    -- N-CEN ledger revisions as of K (mode-aware) plus retractions: never relied on.
    stale_filings := ARRAY(
        SELECT f.supersedes_filing_evidence_id
        FROM bond_default_ncen_filing f JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
        WHERE f.supersedes_filing_evidence_id IS NOT NULL AND f.public_available_at <= k
          AND (NOT current_run OR f.first_seen_at <= k)
        UNION
        SELECT f.filing_evidence_id
        FROM bond_default_ncen_filing f JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
        WHERE f.parse_status = 'retracted');

    -- 3. Adjudication inventory: single effective record per subject, policy binding.
    -- Superseded records must stay inside the inventory (no dangling chain links).
    SELECT a.adjudication_id::text INTO bad
    FROM bond_default_adjudication a JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
    WHERE a.supersedes_adjudication_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM bond_default_adjudication b JOIN pg_catalog.unnest(pkgs) AS w(package_id) USING (package_id)
                      WHERE b.adjudication_id = a.supersedes_adjudication_id AND b.subject_id = a.subject_id
                        AND b.subject_kind = a.subject_kind)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:adjudication_supersedes_outside_inventory:%', bad;
    END IF;
    eff_ids := ARRAY(
        SELECT a.adjudication_id
        FROM bond_default_adjudication a JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
        WHERE NOT EXISTS (SELECT 1 FROM bond_default_adjudication b
                          JOIN pg_catalog.unnest(pkgs) AS w(package_id) USING (package_id)
                          WHERE b.supersedes_adjudication_id = a.adjudication_id));
    SELECT a.subject_id::text INTO bad FROM bond_default_adjudication a
    WHERE a.adjudication_id = ANY (eff_ids) GROUP BY a.subject_kind, a.subject_id HAVING count(*) > 1 LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:adjudication_chain_fork:%', bad;
    END IF;

    -- 3a. N-CEN ledger provenance (publication._check_ncen_filings): row family, acceptance
    --     header, revision parent, equal-projection version proofs and the recomputed
    --     projection digest; current-run rows ingested by K.
    SELECT f.filing_evidence_id::text INTO bad
    FROM bond_default_ncen_filing f
    JOIN bond_default_source_package p ON p.package_id = f.package_id
    WHERE f.package_id = ANY (pkgs) AND NOT (
        (CASE WHEN f.parse_status = 'index_only' THEN p.source_family = 'sec_edgar_index'
              ELSE p.source_family IN ('sec_ncen_dera', 'sec_ncen_public_xml') END)
        -- Never public before its own artifact; acceptance equals the header's attested acceptance.
        AND f.public_available_at >= p.first_verified_public_at
        AND (f.acceptance_at IS NULL OR LEAST(f.public_available_at, (
                SELECT pg_catalog.min(v.public_available_at) FROM bond_default_ncen_filing v
                WHERE v.filing_evidence_id = ANY (f.version_evidence_filing_ids))) >= f.acceptance_at)
        AND (f.header_package_id IS NULL OR EXISTS (
                SELECT 1 FROM bond_default_source_package h
                WHERE h.package_id = f.header_package_id AND h.package_id = ANY (pkgs)
                  AND h.source_family = 'sec_ncen_acceptance_header' AND h.accession_number = f.accession_number
                  AND (f.acceptance_at IS NULL OR (h.public_time_basis = 'edgar_acceptance_datetime'
                                                   AND h.first_verified_public_at = f.acceptance_at))))
        AND (f.supersedes_filing_evidence_id IS NULL OR EXISTS (
                SELECT 1 FROM bond_default_ncen_filing s
                WHERE s.filing_evidence_id = f.supersedes_filing_evidence_id AND s.package_id = ANY (pkgs)
                  AND s.accession_number = f.accession_number AND s.registrant_cik IS NOT DISTINCT FROM f.registrant_cik))
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(f.version_evidence_filing_ids) vid
                        WHERE NOT EXISTS (
                            SELECT 1 FROM bond_default_ncen_filing q
                            WHERE q.filing_evidence_id = vid AND q.package_id = ANY (pkgs)
                              AND q.accession_number = f.accession_number AND q.projection_digest = f.projection_digest
                              AND q.parse_status = 'parsed' AND cardinality(q.version_evidence_filing_ids) = 0))
        AND f.projection_digest = bond_credit_ncen_projection_digest(f))
    ORDER BY f.filing_evidence_id::text LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:ncen_provenance_invalid:%', bad;
    END IF;
    IF current_run AND EXISTS (SELECT 1 FROM bond_default_ncen_filing f
                               WHERE f.package_id = ANY (pkgs) AND f.first_seen_at > k) THEN
        RAISE EXCEPTION 'bond_credit_validate:current_run_input_after_cutoff';
    END IF;

    -- 3b. Effective decisions: cited proposals are publication rows; evidence-only subjects exist.
    SELECT a.adjudication_id::text INTO bad FROM bond_default_adjudication a
    WHERE a.adjudication_id = ANY (eff_ids)
      AND ((a.status = 'accepted_state' AND a.reviewer_role = 'policy_rule_engine'
            AND cardinality(a.proposal_evidence_ids) = 0)
           OR EXISTS (SELECT 1 FROM pg_catalog.unnest(a.proposal_evidence_ids) pid
                      WHERE NOT EXISTS (SELECT 1 FROM bond_default_proposal_evidence_v2 x
                                        WHERE x.publication_id = pub.publication_id AND x.proposal_evidence_id = pid)))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:proposal_evidence_not_closed:%', bad;
    END IF;
    SELECT e.subject_id::text INTO bad FROM bond_default_adjudication e
    WHERE e.adjudication_id = ANY (eff_ids) AND e.status IN ('accepted_state', 'accepted_event')
      AND (e.subject_kind <> 'issue_episode' OR e.policy_digest <> pub.policy_digest
           OR NOT EXISTS (SELECT 1 FROM bond_default_event_v1 ev
                          WHERE ev.publication_id = pub.publication_id AND ev.episode_id = e.subject_id))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:admitted_adjudication_without_event:%', bad;
    END IF;
    SELECT e.subject_id::text INTO bad FROM bond_default_adjudication e
    WHERE e.adjudication_id = ANY (eff_ids) AND e.status = 'accepted_evidence' AND (
        (e.subject_kind = 'issue_scope' AND NOT EXISTS (
            SELECT 1 FROM bond_default_event_link l
            WHERE l.link_id = e.subject_id AND l.package_id = ANY (pkgs) AND l.status = 'quarantined'))
        OR (e.subject_kind = 'exchange_pairing' AND NOT EXISTS (
            SELECT 1 FROM bond_default_exchange_relation_v2 r
            WHERE r.publication_id = pub.publication_id AND r.relation_id = e.subject_id)))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:adjudication_subject_invalid:%', bad;
    END IF;

    -- 3c. Family contexts (FE-1a/b): each context is recomputed from the persisted inventory.
    SELECT m.family_evidence_id::text INTO bad FROM bond_default_family_evidence_v2 m
    WHERE m.publication_id = pub.publication_id
      AND NOT EXISTS (SELECT 1 FROM bond_default_family_context_v2 c
                      WHERE c.publication_id = m.publication_id AND c.context_id = m.context_id)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:family_universe_not_closed:%', bad;
    END IF;
    prev_date := NULL;
    FOR ctx IN SELECT * FROM bond_default_family_context_v2 c
               WHERE c.publication_id = pub.publication_id ORDER BY c.report_date, c.context_id LOOP
        IF ctx.report_date IS NOT DISTINCT FROM prev_date OR ctx.knowledge_cutoff <> k THEN
            RAISE EXCEPTION 'bond_credit_validate:family_selection_invalid:%', ctx.context_id;
        END IF;
        prev_date := ctx.report_date;
        want := bond_credit_family_expected(pkgs, stale_obs, stale_filings, ctx.report_date, k, pub.knowledge_mode);
        IF (want ->> 'not_closed')::boolean
           OR ctx.vote_observation_ids IS DISTINCT FROM ARRAY(
                SELECT x::uuid FROM pg_catalog.jsonb_array_elements_text(want -> 'vote_observation_ids') x)
           OR ARRAY(SELECT m.registrant_cik COLLATE "C" FROM bond_default_family_evidence_v2 m
                    WHERE m.publication_id = pub.publication_id AND m.context_id = ctx.context_id ORDER BY 1)
              IS DISTINCT FROM ARRAY(SELECT x COLLATE "C" FROM pg_catalog.jsonb_object_keys(want -> 'members') x ORDER BY 1)
           OR ctx.universe_digest IS DISTINCT FROM want ->> 'universe_digest'
           OR ctx.membership_digest IS DISTINCT FROM bond_credit_frame_digest(ARRAY(
                SELECT bond_credit_checked_row_sha256('family_evidence', pg_catalog.to_jsonb(m))
                FROM bond_default_family_evidence_v2 m
                WHERE m.publication_id = pub.publication_id AND m.context_id = ctx.context_id)) THEN
            RAISE EXCEPTION 'bond_credit_validate:family_universe_not_closed:%', ctx.context_id;
        END IF;
        SELECT m.family_evidence_id::text INTO bad FROM bond_default_family_evidence_v2 m
        CROSS JOIN LATERAL (SELECT want -> 'members' -> m.registrant_cik AS w) e
        WHERE m.publication_id = pub.publication_id AND m.context_id = ctx.context_id
          AND (m.valid_from <> ctx.report_date
               OR m.voting_series_ids IS DISTINCT FROM ARRAY(SELECT pg_catalog.jsonb_array_elements_text(e.w -> 'voting_series_ids'))
               OR m.vote_observation_ids IS DISTINCT FROM ARRAY(
                    SELECT x::uuid FROM pg_catalog.jsonb_array_elements_text(e.w -> 'vote_observation_ids') x))
        LIMIT 1;
        IF bad IS NOT NULL THEN
            RAISE EXCEPTION 'bond_credit_validate:family_universe_not_closed:%', bad;
        END IF;
        SELECT m.family_evidence_id::text INTO bad FROM bond_default_family_evidence_v2 m
        CROSS JOIN LATERAL (SELECT want -> 'members' -> m.registrant_cik AS w) e
        WHERE m.publication_id = pub.publication_id AND m.context_id = ctx.context_id
          AND (m.selected_filing_id IS DISTINCT FROM (e.w ->> 'selected_filing_id')::uuid
               OR m.blocking_filing_ids IS DISTINCT FROM ARRAY(
                    SELECT x::uuid FROM pg_catalog.jsonb_array_elements_text(e.w -> 'blocking_filing_ids') x)
               OR m.state IS DISTINCT FROM e.w ->> 'state'
               OR m.reasons IS DISTINCT FROM ARRAY(SELECT pg_catalog.jsonb_array_elements_text(e.w -> 'reasons')))
        LIMIT 1;
        IF bad IS NOT NULL THEN
            RAISE EXCEPTION 'bond_credit_validate:family_selection_invalid:%', bad;
        END IF;
        SELECT m.family_evidence_id::text INTO bad FROM bond_default_family_evidence_v2 m
        WHERE m.publication_id = pub.publication_id AND m.context_id = ctx.context_id
          AND m.component_id IS DISTINCT FROM want -> 'members' -> m.registrant_cik ->> 'component_id'
        LIMIT 1;
        IF bad IS NOT NULL THEN
            RAISE EXCEPTION 'bond_credit_validate:family_component_mismatch:%', bad;
        END IF;
        IF ctx.selection_filing_ids IS DISTINCT FROM ARRAY(
                SELECT x::uuid FROM pg_catalog.jsonb_array_elements_text(want -> 'selection_filing_ids') x)
           OR ctx.index_package_ids IS DISTINCT FROM ARRAY(
                SELECT x::uuid FROM pg_catalog.jsonb_array_elements_text(want -> 'index_package_ids') x) THEN
            RAISE EXCEPTION 'bond_credit_validate:family_selection_invalid:%', ctx.context_id;
        END IF;
        IF ctx.evidence_known_at IS DISTINCT FROM (want ->> 'evidence_known_at')::timestamptz
           OR ctx.evidence_known_at > k THEN
            RAISE EXCEPTION 'bond_credit_validate:dependency_knowledge_mismatch:%', ctx.context_id;
        END IF;
    END LOOP;

    -- 3d. Persisted W1 proposals: recomputed from the persisted contexts and votes.
    FOR prop IN SELECT * FROM bond_default_proposal_evidence_v2 x
                WHERE x.publication_id = pub.publication_id ORDER BY x.proposal_evidence_id::text LOOP
        IF prop.policy_digest <> pub.policy_digest
           OR EXISTS (SELECT 1 FROM pg_catalog.unnest(prop.evidence_observation_ids) oid
                      WHERE oid = ANY (stale_obs) OR NOT EXISTS (
                          SELECT 1 FROM bond_credit_observation o WHERE o.observation_id = oid AND o.package_id = ANY (pkgs)))
           OR EXISTS (SELECT 1 FROM pg_catalog.unnest(prop.family_evidence_ids) fid
                      WHERE NOT EXISTS (SELECT 1 FROM bond_default_family_evidence_v2 m
                                        WHERE m.publication_id = pub.publication_id AND m.family_evidence_id = fid)) THEN
            RAISE EXCEPTION 'bond_credit_validate:proposal_evidence_not_closed:%', prop.proposal_evidence_id;
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.unnest(prop.corroboration_adjudication_ids) aid
                   WHERE NOT bond_credit_corroboration_ok(pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest, aid,
                                                          prop.cusip9, prop.onset_upper_inclusive)) THEN
            RAISE EXCEPTION 'bond_credit_validate:corroboration_not_effective:%', prop.proposal_evidence_id;
        END IF;
        cor_obs := ARRAY(SELECT DISTINCT e FROM bond_default_adjudication a
                         CROSS JOIN LATERAL pg_catalog.unnest(a.evidence_observation_ids) e
                         WHERE a.adjudication_id = ANY (prop.corroboration_adjudication_ids));
        IF prop.proposed_status = 'accepted_state' THEN
            ctx_u := (SELECT c.context_id FROM bond_default_family_context_v2 c
                      WHERE c.publication_id = pub.publication_id AND c.report_date = prop.onset_upper_inclusive);
            ctx_l := (SELECT c.context_id FROM bond_default_family_context_v2 c
                      WHERE c.publication_id = pub.publication_id AND c.report_date = prop.onset_lower_exclusive);
            IF ctx_u IS NULL OR (prop.onset_lower_exclusive IS NOT NULL AND ctx_l IS NULL) THEN
                RAISE EXCEPTION 'bond_credit_validate:proposal_evidence_not_closed:%', prop.proposal_evidence_id;
            END IF;
            st := bond_credit_vote_state(pkgs, stale_obs, k, prop.onset_upper_inclusive, prop.cusip9, pub.publication_id,
                                         ctx_u, cardinality(prop.corroboration_adjudication_ids) > 0);
            family := ARRAY(SELECT m.family_evidence_id FROM bond_default_family_evidence_v2 m
                            WHERE m.publication_id = pub.publication_id AND m.context_id = ctx_u
                              AND m.registrant_cik IN (SELECT pg_catalog.jsonb_array_elements_text(st -> 'relied')));
            ok := st ->> 'status' = 'consensus_y' AND st ->> 'basis' = prop.basis
                  AND (cardinality(prop.corroboration_adjudication_ids) > 0) = (prop.basis = 'same_family_corroborated')
                  AND prop.onset_upper_evidence_ids = ARRAY(SELECT x::uuid FROM pg_catalog.jsonb_array_elements_text(st -> 'y_ids') x);
            lower_ids := '{}';
            IF ctx_l IS NOT NULL THEN
                st_l := bond_credit_vote_state(pkgs, stale_obs, k, prop.onset_lower_exclusive, prop.cusip9,
                                               pub.publication_id, ctx_l, false);
                lower_ids := ARRAY(SELECT x::uuid FROM pg_catalog.jsonb_array_elements_text(st_l -> 'n_ids') x);
                ok := ok AND st_l ->> 'status' = 'consensus_n' AND prop.onset_lower_evidence_ids = lower_ids;
                family := family || ARRAY(SELECT m.family_evidence_id FROM bond_default_family_evidence_v2 m
                                          WHERE m.publication_id = pub.publication_id AND m.context_id = ctx_l
                                            AND m.registrant_cik IN (SELECT pg_catalog.jsonb_array_elements_text(st_l -> 'relied')));
            END IF;
            -- First credible Y / last credible N over every earlier voting date of this CUSIP.
            FOR day IN SELECT DISTINCT o.report_date FROM bond_credit_observation o
                       WHERE o.package_id = ANY (pkgs) AND o.observation_kind = 'nport_holding' AND o.cusip9 = prop.cusip9
                         AND o.report_date < prop.onset_upper_inclusive AND o.registrant_cik IS NOT NULL
                         AND NOT (o.observation_id = ANY (stale_obs)) AND o.public_available_at <= k
                         AND o.field_presence ->> 'nport_is_default' = 'present' AND o.nport_is_default IN ('Y', 'N')
                         AND (o.nport_is_default = 'Y' OR prop.onset_lower_exclusive IS NULL
                              OR o.report_date > prop.onset_lower_exclusive)
                       ORDER BY 1 LOOP
                ctx_d := (SELECT c.context_id FROM bond_default_family_context_v2 c
                          WHERE c.publication_id = pub.publication_id AND c.report_date = day);
                IF ctx_d IS NULL THEN
                    ok := false;
                    EXIT;
                END IF;
                st_l := bond_credit_vote_state(pkgs, stale_obs, k, day, prop.cusip9, pub.publication_id, ctx_d,
                    EXISTS (SELECT 1 FROM pg_catalog.unnest(prop.corroboration_adjudication_ids) aid
                            WHERE bond_credit_corroboration_ok(pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest,
                                                               aid, prop.cusip9, day)));
                ok := ok AND st_l ->> 'status' <> 'consensus_y'
                      AND ((prop.onset_lower_exclusive IS NOT NULL AND day <= prop.onset_lower_exclusive)
                           OR st_l ->> 'status' <> 'consensus_n');
            END LOOP;
            ok := ok AND prop.family_evidence_ids = ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(family) x ORDER BY 1)
                  AND prop.evidence_observation_ids = ARRAY(
                      SELECT DISTINCT x FROM pg_catalog.unnest(
                          ARRAY(SELECT y::uuid FROM pg_catalog.jsonb_array_elements_text(st -> 'y_ids') y)
                          || lower_ids || cor_obs) x ORDER BY 1);
            IF NOT ok THEN
                RAISE EXCEPTION 'bond_credit_validate:proposal_evidence_not_closed:%', prop.proposal_evidence_id;
            END IF;
        ELSIF NOT (cor_obs <@ prop.evidence_observation_ids) THEN
            RAISE EXCEPTION 'bond_credit_validate:proposal_evidence_not_closed:%', prop.proposal_evidence_id;
        END IF;
        known := bond_credit_proposal_known_at(pub.publication_id, pkgs, stale_obs, stale_links,
                                               eff_ids, pub.policy_digest, prop.proposal_evidence_id);
        IF known IS NULL OR prop.evidence_known_at IS DISTINCT FROM known OR known > k THEN
            RAISE EXCEPTION 'bond_credit_validate:dependency_knowledge_mismatch:%', prop.proposal_evidence_id;
        END IF;
    END LOOP;

    -- 3e. Directional exchange relations: event binding, side links valid at the exchange date,
    --     documents, the effective human pairing decision, exclusive end and knowledge time.
    SELECT r.relation_id::text INTO bad FROM bond_default_exchange_relation_v2 r
    LEFT JOIN bond_default_event_v1 ev ON ev.publication_id = r.publication_id AND ev.episode_id = r.episode_id
    LEFT JOIN bond_default_event_link ol ON ol.link_id = r.old_link_id
    LEFT JOIN bond_default_event_link nl ON nl.link_id = r.new_link_id
    LEFT JOIN bond_default_adjudication a ON a.adjudication_id = r.pairing_adjudication_id
    WHERE r.publication_id = pub.publication_id AND NOT COALESCE(
        (ev.primary_type = 'distressed_exchange' OR 'distressed_exchange' = ANY (ev.corroboration_flags))
        AND ev.security_id = r.old_security_id AND ev.cusip9 = r.old_cusip9 AND ev.alias_spell_id = r.alias_spell_id
        AND r.relation_id = ANY (ev.exchange_relation_ids)
        AND (CASE WHEN ev.primary_type = 'distressed_exchange' THEN r.exchange_effective_date = ev.onset_upper_inclusive
                  ELSE r.exchange_effective_date >= ev.onset_upper_inclusive END)
        AND NOT EXISTS (
            SELECT 1 FROM (VALUES (r.old_link_id, 'exchange_old', r.old_security_id, r.old_cusip9::text),
                                  (r.new_link_id, 'exchange_new', r.new_security_id, r.new_cusip9::text)) AS s(lid, scope, sec, cus)
            WHERE NOT EXISTS (
                SELECT 1 FROM bond_default_event_link l
                WHERE l.link_id = s.lid AND l.package_id = ANY (pkgs) AND l.status = 'admitted'
                  AND NOT (l.link_id = ANY (stale_links)) AND l.affected_scope = s.scope AND l.security_id = s.sec
                  AND l.cusip9 = s.cus AND l.valid_from <= r.exchange_effective_date
                  AND (l.valid_to IS NULL OR l.valid_to >= r.exchange_effective_date)
                  AND l.observation_id = ANY (r.exchange_document_observation_ids)))
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(r.exchange_document_observation_ids) oid
                        WHERE oid = ANY (stale_obs) OR NOT EXISTS (
                            SELECT 1 FROM bond_credit_observation o
                            WHERE o.observation_id = oid AND o.package_id = ANY (pkgs) AND o.observation_kind <> 'nport_holding'))
        AND nl.valid_to IS DISTINCT FROM DATE '9999-12-31'
        AND ((nl.valid_to IS NULL AND r.valid_to IS NULL AND ol.valid_to IS NULL)
             OR (nl.valid_to IS NOT NULL AND r.valid_to IS NOT NULL
                 AND r.valid_to - 1 = nl.valid_to AND (ol.valid_to IS NULL OR r.valid_to - 1 <= ol.valid_to)))
        AND a.package_id = ANY (pkgs) AND a.adjudicated_at <= k
        AND NOT EXISTS (SELECT 1 FROM bond_default_adjudication newer
                        WHERE newer.package_id = ANY (pkgs)
                          AND newer.supersedes_adjudication_id = a.adjudication_id AND newer.adjudicated_at <= k)
        AND a.subject_kind = 'exchange_pairing'
        AND a.status = 'accepted_evidence' AND a.reviewer_role = 'human_reviewer' AND a.policy_digest = pub.policy_digest
        AND a.subject_id = r.relation_id AND ARRAY[r.old_link_id, r.new_link_id] <@ a.link_ids
        AND r.exchange_document_observation_ids <@ a.evidence_observation_ids
        AND a.support_valid_from = r.exchange_effective_date AND a.support_valid_to IS NOT DISTINCT FROM r.valid_to
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(a.link_ids) lid
                        WHERE NOT EXISTS (SELECT 1 FROM bond_default_event_link l
                                          WHERE l.link_id = lid AND l.package_id = ANY (pkgs)
                                            AND (l.status = 'admitted' OR (l.status = 'quarantined'
                                                 AND bond_credit_scope_head(
                                                     pkgs, eff_ids, pub.policy_digest, l.link_id) IS NOT NULL)))), false)
    ORDER BY r.relation_id::text LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:exchange_pairing_invalid:%', bad;
    END IF;
    SELECT r.relation_id::text INTO bad FROM bond_default_exchange_relation_v2 r
    WHERE r.publication_id = pub.publication_id
      AND (r.evidence_known_at IS DISTINCT FROM bond_credit_relation_known_at(
               pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest, r)
           OR r.evidence_known_at > k)
    ORDER BY r.relation_id::text LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:dependency_knowledge_mismatch:%', bad;
    END IF;
    -- One old origin per new issue over overlapping validity; the old -> new graph is acyclic.
    SELECT b.new_security_id::text INTO bad
    FROM bond_default_exchange_relation_v2 a
    JOIN bond_default_exchange_relation_v2 b
      ON b.publication_id = a.publication_id AND b.new_security_id = a.new_security_id
     AND b.old_security_id <> a.old_security_id AND a.relation_id::text < b.relation_id::text
    WHERE a.publication_id = pub.publication_id
      AND (b.valid_to IS NULL OR a.valid_from < b.valid_to) AND (a.valid_to IS NULL OR b.valid_from < a.valid_to)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:exchange_alias_ambiguous:%', bad;
    END IF;
    WITH RECURSIVE walk(start_id, cur_id) AS (
        SELECT r.old_security_id, r.new_security_id FROM bond_default_exchange_relation_v2 r
        WHERE r.publication_id = pub.publication_id
        UNION
        SELECT w.start_id, r.new_security_id FROM walk w
        JOIN bond_default_exchange_relation_v2 r ON r.publication_id = pub.publication_id AND r.old_security_id = w.cur_id)
    SELECT w.start_id::text INTO bad FROM walk w WHERE w.cur_id = w.start_id ORDER BY 1 LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:dependency_cycle:%', bad;
    END IF;
    SELECT ev.episode_id::text INTO bad FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id
      AND ev.exchange_relation_ids IS DISTINCT FROM ARRAY(
          SELECT r.relation_id FROM bond_default_exchange_relation_v2 r
          WHERE r.publication_id = ev.publication_id AND r.episode_id = ev.episode_id ORDER BY 1)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:exchange_pairing_invalid:%', bad;
    END IF;
    -- Event dependency edges: first bind the exact cited effective issue head, then require its
    -- accepted-state proposals to be publication rows of the same CUSIP and cited identically.
    SELECT ev.episode_id::text INTO bad FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id
      AND NOT EXISTS (
          SELECT 1 FROM bond_default_adjudication a
          WHERE a.package_id = ANY (pkgs)
            AND a.adjudication_id = ANY (ev.adjudication_ids)
            AND a.adjudication_id = ANY (eff_ids)
            AND a.subject_kind = 'issue_episode' AND a.subject_id = ev.episode_id
            AND a.status = ev.admission_status AND a.policy_digest = pub.policy_digest)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:event_evidence_invalid:%', bad;
    END IF;
    SELECT ev.episode_id::text INTO bad FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id AND (
        EXISTS (SELECT 1 FROM pg_catalog.unnest(ev.proposal_evidence_ids) pid
                WHERE NOT EXISTS (SELECT 1 FROM bond_default_proposal_evidence_v2 x
                                  WHERE x.publication_id = ev.publication_id AND x.proposal_evidence_id = pid
                                    AND x.proposed_status = 'accepted_state' AND x.cusip9 = ev.cusip9))
        OR EXISTS (SELECT 1 FROM bond_default_adjudication a
                   WHERE a.adjudication_id = ANY (ev.adjudication_ids) AND a.adjudication_id = ANY (eff_ids)
                     AND a.package_id = ANY (pkgs)
                     AND a.subject_kind = 'issue_episode' AND a.subject_id = ev.episode_id
                     AND a.status = ev.admission_status AND a.policy_digest = pub.policy_digest
                     AND a.proposal_evidence_ids <> ev.proposal_evidence_ids))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:proposal_evidence_not_closed:%', bad;
    END IF;

    -- Refuse an event whose effective accepting decision cites support outside its direct arrays
    -- before comparing the transitive knowledge time, preserving the typed closure reason.
    SELECT ev.episode_id::text INTO bad
    FROM bond_default_event_v1 ev
    JOIN bond_default_adjudication a
      ON a.adjudication_id = ANY (ev.adjudication_ids) AND a.adjudication_id = ANY (eff_ids)
     AND a.status = ev.admission_status
    WHERE ev.publication_id = pub.publication_id
      AND NOT (a.evidence_observation_ids <@ ev.evidence_observation_ids AND a.link_ids <@ ev.link_ids)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:event_evidence_not_closed:%', bad;
    END IF;

    -- 4. Events: inventory membership, admitted links, effective adjudication, temporal admission.
    SELECT ev.episode_id::text INTO bad
    FROM bond_default_event_v1 ev
    CROSS JOIN LATERAL (
        SELECT count(*) AS n, max(o.public_available_at) AS public_at,
               bool_and(v.package_id IS NOT NULL) AS in_inventory
        FROM pg_catalog.unnest(ev.evidence_observation_ids) oid
        JOIN bond_credit_observation o ON o.observation_id = oid
        LEFT JOIN pg_catalog.unnest(pkgs) AS v(package_id) ON v.package_id = o.package_id
    ) obs
    CROSS JOIN LATERAL (
        SELECT count(*) AS n, max(l.link_known_at) AS known_at,
               bool_and(v.package_id IS NOT NULL
                        AND (l.status = 'admitted' OR (l.status = 'quarantined'
                             AND bond_credit_scope_head(pkgs, eff_ids, pub.policy_digest, l.link_id) IS NOT NULL))
                        AND l.security_id = ev.security_id AND l.cusip9 = ev.cusip9
                        AND l.observation_id = ANY (ev.evidence_observation_ids)
                        AND l.valid_from <= ev.onset_upper_inclusive
                        AND (l.valid_to IS NULL OR l.valid_to >= ev.onset_upper_inclusive)) AS ok
        FROM pg_catalog.unnest(ev.link_ids) lid
        JOIN bond_default_event_link l ON l.link_id = lid
        LEFT JOIN pg_catalog.unnest(pkgs) AS v(package_id) ON v.package_id = l.package_id
    ) lnk
    CROSS JOIN LATERAL (
        SELECT count(*) AS n,
               bool_and(v.package_id IS NOT NULL AND a.subject_kind = 'issue_episode' AND a.subject_id = ev.episode_id
                        AND a.policy_digest = pub.policy_digest) AS ok,
               count(*) FILTER (WHERE a.adjudication_id = ANY (eff_ids)
                                AND a.status = ev.admission_status) AS effective_admitting
        FROM pg_catalog.unnest(ev.adjudication_ids) aid
        JOIN bond_default_adjudication a ON a.adjudication_id = aid
        LEFT JOIN pg_catalog.unnest(pkgs) AS v(package_id) ON v.package_id = a.package_id
    ) adj
    WHERE ev.publication_id = pub.publication_id
      AND (obs.n <> cardinality(ev.evidence_observation_ids) OR NOT obs.in_inventory
           OR lnk.n <> cardinality(ev.link_ids) OR NOT lnk.ok
           OR adj.n <> cardinality(ev.adjudication_ids) OR NOT adj.ok OR adj.effective_admitting <> 1
           OR EXISTS (SELECT 1 FROM pg_catalog.unnest(ev.evidence_observation_ids) oid
                      WHERE NOT EXISTS (SELECT 1 FROM bond_default_event_link l2
                                        WHERE l2.link_id = ANY (ev.link_ids) AND l2.observation_id = oid))
           -- Knowledge times over the typed closure (scope decisions, relations, proposals).
            OR ev.link_known_at IS DISTINCT FROM bond_credit_event_link_known_at(
                   pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest, ev)
            OR ev.evidence_known_at IS DISTINCT FROM bond_credit_event_evidence_known_at(
                   pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest, ev)
           OR ev.evidence_known_at > k)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:event_evidence_invalid:%', bad;
    END IF;
    -- Typed dependency-closure digest recomputed from the persisted rows (never trusted).
    SELECT ev.episode_id::text INTO bad FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id
      AND ev.dependency_digest IS DISTINCT FROM bond_credit_event_dependency_digest(
              pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest, ev)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:dependency_digest_mismatch:%', bad;
    END IF;
    -- A non-null onset lower bound needs explicit provenance cited by the accepting
    -- adjudication: a credible prior N-PORT N of the same obligation reported at the bound,
    -- or independent dating evidence (day date = bound + 1, or interval lower = bound).
    SELECT ev.episode_id::text INTO bad
    FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id AND ev.onset_lower_exclusive IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM pg_catalog.unnest(ev.onset_lower_evidence_ids) lid
          WHERE NOT EXISTS (
              SELECT 1 FROM bond_default_adjudication a
              JOIN bond_credit_observation o ON o.observation_id = lid
              WHERE a.adjudication_id = ANY (ev.adjudication_ids) AND a.adjudication_id = ANY (eff_ids)
                AND a.status = ev.admission_status AND lid = ANY (a.evidence_observation_ids)
                AND CASE WHEN o.observation_kind = 'nport_holding'
                              THEN o.nport_is_default = 'N' AND o.report_date = ev.onset_lower_exclusive
                                   AND o.cusip9 = ev.cusip9
                         WHEN o.date_precision = 'day' THEN o.effective_date - 1 = ev.onset_lower_exclusive
                         WHEN o.date_precision = 'interval' THEN o.effective_lower_exclusive = ev.onset_lower_exclusive
                         ELSE false END))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:event_onset_lower_unsupported:%', bad;
    END IF;
    -- A proposal-based state event takes both onset bounds from a relied proposal.
    SELECT ev.episode_id::text INTO bad FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id AND ev.primary_type = 'default_state'
      AND cardinality(ev.proposal_evidence_ids) > 0
      AND NOT EXISTS (SELECT 1 FROM bond_default_proposal_evidence_v2 x
                      WHERE x.publication_id = ev.publication_id AND x.proposal_evidence_id = ANY (ev.proposal_evidence_ids)
                        AND x.onset_upper_inclusive = ev.onset_upper_inclusive)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:proposal_evidence_not_closed:%', bad;
    END IF;
    SELECT ev.episode_id::text INTO bad FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id AND ev.primary_type = 'default_state'
      AND cardinality(ev.proposal_evidence_ids) > 0
      AND NOT EXISTS (SELECT 1 FROM bond_default_proposal_evidence_v2 x
                      WHERE x.publication_id = ev.publication_id AND x.proposal_evidence_id = ANY (ev.proposal_evidence_ids)
                        AND x.onset_upper_inclusive = ev.onset_upper_inclusive
                        AND x.onset_lower_exclusive IS NOT DISTINCT FROM ev.onset_lower_exclusive
                        AND x.onset_lower_evidence_ids = ev.onset_lower_evidence_ids)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:event_onset_lower_unsupported:%', bad;
    END IF;
    SELECT ev.episode_id::text INTO bad FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id AND (
        EXISTS (SELECT 1 FROM pg_catalog.unnest(ev.resolution_refs) rid
                WHERE NOT EXISTS (SELECT 1 FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                                  WHERE o.observation_id = rid)
                  AND NOT EXISTS (SELECT 1 FROM bond_default_adjudication a JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                                  WHERE a.adjudication_id = rid)))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:resolution_ref_not_in_inventory:%', bad;
    END IF;
    PERFORM bond_credit_support_known_at(
        ev.publication_id, pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest,
        ARRAY(SELECT rid FROM pg_catalog.unnest(ev.resolution_refs) rid
              WHERE EXISTS (SELECT 1 FROM bond_credit_observation o
                            WHERE o.package_id = ANY (pkgs) AND o.observation_id = rid)),
        ARRAY(SELECT rid FROM pg_catalog.unnest(ev.resolution_refs) rid
              WHERE EXISTS (SELECT 1 FROM bond_default_adjudication a
                            WHERE a.package_id = ANY (pkgs) AND a.adjudication_id = rid)))
    FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id AND cardinality(ev.resolution_refs) > 0;
    SELECT ev.episode_id::text INTO bad FROM bond_default_event_v1 ev
    WHERE ev.publication_id = pub.publication_id
      AND EXISTS (
          SELECT 1 FROM pg_catalog.unnest(ev.resolution_refs) rid
          JOIN bond_default_adjudication a ON a.adjudication_id = rid
          WHERE a.package_id = ANY (pkgs) AND NOT (
              a.subject_kind = 'issue_episode' AND a.subject_id = ev.episode_id
              AND a.adjudication_id = ANY (eff_ids)
              AND a.adjudication_id = ANY (ev.adjudication_ids)
              AND a.status IN ('accepted_state', 'accepted_event')
              AND a.status = ev.admission_status AND a.policy_digest = pub.policy_digest))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:resolution_adjudication_invalid:%', bad;
    END IF;
    -- Resolution keeps its own knowledge time: the maximum public/link-known time of the
    -- referenced observations and of everything the referenced adjudications cite, <= K.
    SELECT ev.episode_id::text INTO bad
    FROM bond_default_event_v1 ev
    CROSS JOIN LATERAL (
        SELECT bond_credit_support_known_at(
            ev.publication_id, pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest,
            ARRAY(SELECT rid FROM pg_catalog.unnest(ev.resolution_refs) rid
                  WHERE EXISTS (SELECT 1 FROM bond_credit_observation o
                                WHERE o.package_id = ANY (pkgs) AND o.observation_id = rid)),
            ARRAY(SELECT rid FROM pg_catalog.unnest(ev.resolution_refs) rid
                  WHERE EXISTS (SELECT 1 FROM bond_default_adjudication a
                                WHERE a.package_id = ANY (pkgs) AND a.adjudication_id = rid))) AS known_at
    ) res
    WHERE ev.publication_id = pub.publication_id AND cardinality(ev.resolution_refs) > 0
      AND (res.known_at IS NULL OR ev.resolution_known_at IS DISTINCT FROM res.known_at OR res.known_at > k)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:resolution_knowledge_invalid:%', bad;
    END IF;
    -- Disjoint episodes per security: earlier episode resolved before the next onset bound.
    SELECT e2.episode_id::text INTO bad
    FROM bond_default_event_v1 e1
    JOIN bond_default_event_v1 e2
      ON e2.publication_id = e1.publication_id AND e2.security_id = e1.security_id
     AND (e1.onset_upper_inclusive, e1.episode_id) < (e2.onset_upper_inclusive, e2.episode_id)
    WHERE e1.publication_id = pub.publication_id
      AND NOT (e1.resolution_date IS NOT NULL AND e2.onset_lower_exclusive IS NOT NULL
               AND e2.onset_lower_exclusive >= e1.resolution_date)
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:overlapping_episodes:%', bad;
    END IF;

    -- 5. Follow-up segments.
    SELECT f.segment_id::text INTO bad FROM bond_default_followup_v1 f
    WHERE f.publication_id = pub.publication_id AND (
        f.known_at > k
        OR EXISTS (SELECT 1 FROM pg_catalog.unnest(f.evidence_observation_ids) oid
                   WHERE NOT EXISTS (SELECT 1 FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                                     WHERE o.observation_id = oid))
        OR EXISTS (SELECT 1 FROM pg_catalog.unnest(f.adjudication_ids) aid
                   WHERE NOT EXISTS (SELECT 1 FROM bond_default_adjudication a JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                                      WHERE a.adjudication_id = aid)))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:followup_invalid:%', bad;
    END IF;
    -- Resolve the typed dependency graph first so a missing nested row retains its established
    -- dependency reason. The support-time comparison remains after the authority check below.
    PERFORM bond_credit_support_known_at(
        f.publication_id, pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest,
        f.evidence_observation_ids, f.adjudication_ids)
    FROM bond_default_followup_v1 f
    WHERE f.publication_id = pub.publication_id;
    SELECT f.segment_id::text INTO bad FROM bond_default_followup_v1 f
    WHERE f.publication_id = pub.publication_id
      AND EXISTS (
          SELECT 1 FROM pg_catalog.unnest(f.adjudication_ids) aid
          WHERE NOT EXISTS (
              SELECT 1 FROM bond_default_event_v1 ev
              JOIN bond_default_adjudication a ON a.adjudication_id = aid
              WHERE ev.publication_id = f.publication_id
                AND ev.security_id = f.security_id AND ev.cusip9 = f.cusip9
                AND a.package_id = ANY (pkgs)
                AND a.subject_kind = 'issue_episode' AND a.subject_id = ev.episode_id
                AND a.adjudication_id = ANY (eff_ids)
                AND a.adjudication_id = ANY (ev.adjudication_ids)
                AND a.status IN ('accepted_state', 'accepted_event')
                AND a.status = ev.admission_status AND a.policy_digest = pub.policy_digest))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:followup_adjudication_invalid:%', bad;
    END IF;
    SELECT f.segment_id::text INTO bad FROM bond_default_followup_v1 f
    WHERE f.publication_id = pub.publication_id AND (
        -- Every supporting observation/link (including those its adjudications cite) is
        -- public by the segment's own known_at.
        bond_credit_support_known_at(
               f.publication_id, pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest,
               f.evidence_observation_ids, f.adjudication_ids) > f.known_at
        -- Surveillance needs a qualified receipt issued by known_at whose structured scope
        -- (panel-grid CUSIPs over (start, end]) covers the segment.
        OR (f.status = 'nondefault_continuous' AND NOT (
                COALESCE(f.completeness_basis = 'surveillance_receipt'
                         AND pub.validation_verdict = 'qualified'
                         AND pub.validation_issued_at <= f.known_at
                         AND pub.validation_surveillance_start_exclusive <= f.interval_start_exclusive
                         AND f.interval_end_inclusive <= pub.validation_surveillance_end_inclusive
                         AND EXISTS (SELECT 1 FROM bond_rating_history_public_v1 r
                                     WHERE r.publication_id = pub.publication_id AND r.cusip_id = f.cusip9),
                         false)
                OR EXISTS (SELECT 1 FROM pg_catalog.unnest(f.evidence_observation_ids) oid
                           JOIN bond_credit_observation o ON o.observation_id = oid
                           WHERE o.observation_kind <> 'nport_holding')))
        OR EXISTS (SELECT 1 FROM bond_default_followup_v1 g
                   WHERE g.publication_id = f.publication_id AND g.security_id = f.security_id
                     AND g.spell_id = f.spell_id AND g.segment_id > f.segment_id
                     AND g.interval_start_exclusive < f.interval_end_inclusive
                     AND f.interval_start_exclusive < g.interval_end_inclusive))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:followup_invalid:%', bad;
    END IF;

    -- 6. Exit evidence.
    SELECT x.security_id::text INTO bad FROM bond_default_exit_evidence_v1 x
    WHERE x.publication_id = pub.publication_id AND (
        x.known_at > k
        OR EXISTS (SELECT 1 FROM pg_catalog.unnest(x.evidence_observation_ids) oid
                   WHERE NOT EXISTS (SELECT 1 FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                                     WHERE o.observation_id = oid))
        -- Exit/repayment evidence is public by the row's own known_at.
        OR EXISTS (SELECT 1 FROM pg_catalog.unnest(x.evidence_observation_ids) oid
                   JOIN bond_credit_observation o ON o.observation_id = oid
                   WHERE o.public_available_at > x.known_at))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:exit_evidence_invalid:%', bad;
    END IF;

    -- 7. Coverage cells bind the publication's validation receipt.
    SELECT c.period_label INTO bad FROM bond_default_coverage_v1 c
    WHERE c.publication_id = pub.publication_id
      AND c.validation_receipt_digest IS DISTINCT FROM NULL
      AND c.validation_receipt_digest IS DISTINCT FROM pub.validation_digest
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:coverage_receipt_mismatch:%', bad;
    END IF;

    -- 8. Full-grid rating resolution: both views match the pinned panel grid exactly.
    FOREACH view_name IN ARRAY ARRAY['public_pit', 'effective_audit'] LOOP
        IF (SELECT count(*) FROM bond_rating_history_public_v1 r
            WHERE r.publication_id = pub.publication_id AND r.view_kind = view_name) <> pub.panel_grid_count
           OR bond_credit_frame_digest(ARRAY(
                SELECT r.cusip_id || '|' || pg_catalog.to_char(r.month, 'YYYY-MM-DD')
                FROM bond_rating_history_public_v1 r
                WHERE r.publication_id = pub.publication_id AND r.view_kind = view_name)) <> pub.panel_grid_digest THEN
            RAISE EXCEPTION 'bond_credit_validate:rating_grid_mismatch:%', view_name;
        END IF;
    END LOOP;
    SELECT r.cusip_id || '|' || r.month::text || '|' || r.view_kind INTO bad
    FROM bond_rating_history_public_v1 r
    WHERE r.publication_id = pub.publication_id AND (
        (r.public_known_at IS NOT NULL AND r.public_known_at > k)
        OR EXISTS (SELECT 1 FROM pg_catalog.unnest(r.agency_source_ids) oid
                   WHERE NOT EXISTS (SELECT 1 FROM bond_credit_observation o JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                                     WHERE o.observation_id = oid AND o.observation_kind = 'agency_action'))
        OR EXISTS (SELECT 1 FROM pg_catalog.unnest(r.binding_link_ids) lid
                   WHERE NOT EXISTS (SELECT 1 FROM bond_default_event_link l JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                                     WHERE l.link_id = lid))
        OR (r.default_overlay_episode_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM bond_default_event_v1 ev
                WHERE ev.publication_id = pub.publication_id AND ev.episode_id = r.default_overlay_episode_id
                  AND ev.cusip9 = r.cusip_id)))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:rating_row_invalid:%', bad;
    END IF;
    SELECT r.cusip_id || '|' || r.month::text || '|' || r.view_kind INTO bad
    FROM bond_rating_history_public_v1 r
    WHERE r.publication_id = pub.publication_id AND (
        EXISTS (
            SELECT 1 FROM pg_catalog.unnest(r.agency_source_ids) s(oid)
            JOIN bond_credit_observation o ON o.observation_id = s.oid
            WHERE NOT bond_credit_rating_scope_declared(
                pub.rating_declarations, o.agency_name, o.agency_rating_type, o.agency_scale))
        OR (cardinality(r.agency_source_ids) = 0 AND r.state IN ('missing', 'rights_unverified')
            AND (r.state = 'rights_unverified') IS DISTINCT FROM (
                (bond_credit_rating_uncleared_covers(pub.rating_declarations, r.month)
                 OR EXISTS (
                     SELECT 1 FROM bond_credit_observation o
                     JOIN bond_default_source_package p ON p.package_id = o.package_id
                     JOIN pg_catalog.unnest(pkgs) AS v(package_id) ON v.package_id = p.package_id
                     WHERE o.observation_kind = 'agency_action' AND o.cusip9 = r.cusip_id
                       AND NOT (p.source_family = 'agency_rocr_xbrl' AND p.rights_state = 'approved')))
                AND NOT EXISTS (
                    SELECT 1 FROM bond_credit_observation o
                    JOIN bond_default_source_package p ON p.package_id = o.package_id
                    JOIN pg_catalog.unnest(pkgs) AS v(package_id) ON v.package_id = p.package_id
                    WHERE o.observation_kind = 'agency_action'
                      AND o.agency_subject_kind = 'instrument'
                      AND p.source_family = 'agency_rocr_xbrl' AND p.rights_state = 'approved'
                      AND NOT (o.observation_id = ANY (stale_obs))
                      AND bond_credit_rating_scope_declared(
                          pub.rating_declarations, o.agency_name, o.agency_rating_type, o.agency_scale)
                      AND o.agency_action_date <= ((r.month + interval '1 month')::date - 1)
                      AND o.public_available_at <= k
                      AND (NOT current_run OR o.first_seen_at <= k)
                      AND (r.view_kind <> 'public_pit'
                           OR o.public_available_at < ((r.month + interval '1 month') AT TIME ZONE 'UTC'))
                      AND (o.cusip9 = r.cusip_id OR (o.cusip9 IS NULL AND EXISTS (
                          SELECT 1 FROM bond_default_event_link l
                          WHERE l.observation_id = o.observation_id
                            AND l.package_id = ANY (pkgs) AND NOT (l.link_id = ANY (stale_links))
                            AND l.status = 'admitted' AND l.affected_scope = 'issue'
                            AND l.cusip9 = r.cusip_id
                            AND l.valid_from <= ((r.month + interval '1 month')::date - 1)
                            AND (l.valid_to IS NULL
                                 OR l.valid_to >= ((r.month + interval '1 month')::date - 1))
                            AND l.link_known_at <= k
                            AND (r.view_kind <> 'public_pit'
                                 OR l.link_known_at < ((r.month + interval '1 month') AT TIME ZONE 'UTC')))))))))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:rating_scope_invalid:%', bad;
    END IF;
    -- Rated-row derivation (W2b): each relied action names the row CUSIP or is bound to it by a
    -- persisted binding link (admitted, current at K, scope 'issue', valid at the month-end
    -- snapshot, known by K and, for public_pit, before the next month boundary). Action date,
    -- public time, coverage frontier, bucket (recomputed from the actions) and
    -- action_input_digest (recomputed from the relied observation/package/link rows) must match.
    SELECT r.cusip_id || '|' || r.month::text || '|' || r.view_kind INTO bad
    FROM bond_rating_history_public_v1 r
    CROSS JOIN LATERAL (SELECT ((r.month + interval '1 month')::date - 1) AS month_end,
                               ((r.month + interval '1 month') AT TIME ZONE 'UTC') AS boundary) mb
    CROSS JOIN LATERAL (
        SELECT pg_catalog.max(o.agency_action_date) AS action_date,
               GREATEST(
                   pg_catalog.max(o.public_available_at),
                   (SELECT pg_catalog.max(l.link_known_at) FROM bond_default_event_link l
                    WHERE l.link_id = ANY (r.binding_link_ids))) AS known,
               CASE WHEN pg_catalog.bool_or(f.frontier IS NULL) THEN NULL
                    ELSE pg_catalog.max(f.frontier) END AS frontier,
               pg_catalog.bool_and(o.cusip9 IS NOT DISTINCT FROM r.cusip_id OR EXISTS (
                   SELECT 1 FROM bond_default_event_link l
                   WHERE l.link_id = ANY (r.binding_link_ids) AND l.observation_id = o.observation_id)) AS bound,
               ARRAY(SELECT DISTINCT bond_credit_rating_action_kind(x.agency_action_classification, x.agency_rating_symbol)
                     FROM bond_credit_observation x WHERE x.observation_id = ANY (r.agency_source_ids) ORDER BY 1) AS kinds
        FROM pg_catalog.unnest(r.agency_source_ids) AS s(oid)
        JOIN bond_credit_observation o ON o.observation_id = s.oid
        JOIN bond_default_source_package p ON p.package_id = o.package_id
        CROSS JOIN LATERAL (SELECT CASE WHEN r.view_kind = 'public_pit' THEN p.public_coverage_end
                                        ELSE p.effective_coverage_end END AS frontier) f
    ) rel
    WHERE r.publication_id = pub.publication_id AND (
        (cardinality(r.agency_source_ids) = 0
         AND (cardinality(r.binding_link_ids) > 0 OR r.action_input_digest IS NOT NULL))
        OR (cardinality(r.agency_source_ids) > 0 AND (
            EXISTS (SELECT 1 FROM pg_catalog.unnest(r.binding_link_ids) lid
                    WHERE NOT EXISTS (
                        SELECT 1 FROM bond_default_event_link l
                        JOIN bond_credit_observation lo ON lo.observation_id = l.observation_id
                        WHERE l.link_id = lid AND l.package_id = ANY (pkgs) AND NOT (lid = ANY (stale_links))
                          AND l.status = 'admitted' AND l.affected_scope = 'issue' AND l.cusip9 = r.cusip_id
                          AND l.observation_id = ANY (r.agency_source_ids) AND lo.cusip9 IS DISTINCT FROM r.cusip_id
                          AND l.valid_from <= mb.month_end AND (l.valid_to IS NULL OR l.valid_to >= mb.month_end)
                          AND l.link_known_at <= k
                          AND (r.view_kind <> 'public_pit' OR l.link_known_at < mb.boundary)))
            OR NOT rel.bound
            OR r.action_date IS DISTINCT FROM rel.action_date
            OR rel.action_date > mb.month_end
            OR r.public_known_at IS DISTINCT FROM rel.known
            OR (r.view_kind = 'public_pit' AND rel.known >= mb.boundary)
            OR (r.coverage_frontier IS NOT NULL AND r.coverage_frontier IS DISTINCT FROM rel.frontier)
            OR (r.state = 'carried_verified' AND (r.coverage_frontier IS NULL OR r.coverage_frontier < mb.month_end))
            OR (r.state IN ('observed', 'carried_verified') AND rel.kinds IS DISTINCT FROM ARRAY['rated:' || r.bucket])
            OR (r.state = 'withdrawn' AND rel.kinds IS DISTINCT FROM ARRAY['withdrawn'])
            OR r.action_input_digest IS DISTINCT FROM
               bond_credit_rating_action_input_digest(r.view_kind, r.agency_source_ids, r.binding_link_ids))))
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:rating_row_invalid:%', bad;
    END IF;
    -- No output row relies on an observation/link superseded or retracted as of K. Events and
    -- follow-ups use the same transitive closure as their knowledge/digest checks.
    SELECT x.label INTO bad FROM (
        SELECT ev.episode_id::text AS label,
               ARRAY(SELECT v::uuid FROM pg_catalog.jsonb_array_elements_text(dep.c -> 'observations') v) AS obs_ids,
               ARRAY(SELECT v::uuid FROM pg_catalog.jsonb_array_elements_text(dep.c -> 'event_links') v) AS link_ids
        FROM bond_default_event_v1 ev
        CROSS JOIN LATERAL (SELECT bond_credit_dependency_closure(
            ev.publication_id, pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest,
            ev.evidence_observation_ids || ev.onset_lower_evidence_ids || ARRAY(
                SELECT rid FROM pg_catalog.unnest(ev.resolution_refs) rid
                WHERE EXISTS (SELECT 1 FROM bond_credit_observation o
                              WHERE o.package_id = ANY (pkgs) AND o.observation_id = rid)),
            ev.link_ids,
            ev.adjudication_ids || ARRAY(
                SELECT rid FROM pg_catalog.unnest(ev.resolution_refs) rid
                WHERE EXISTS (SELECT 1 FROM bond_default_adjudication a
                              WHERE a.package_id = ANY (pkgs) AND a.adjudication_id = rid)),
            ev.proposal_evidence_ids, ev.exchange_relation_ids) AS c) dep
        WHERE ev.publication_id = pub.publication_id
        UNION ALL
        SELECT f.segment_id::text,
               ARRAY(SELECT v::uuid FROM pg_catalog.jsonb_array_elements_text(dep.c -> 'observations') v),
               ARRAY(SELECT v::uuid FROM pg_catalog.jsonb_array_elements_text(dep.c -> 'event_links') v)
        FROM bond_default_followup_v1 f
        CROSS JOIN LATERAL (SELECT bond_credit_dependency_closure(
            f.publication_id, pkgs, stale_obs, stale_links, eff_ids, pub.policy_digest,
            f.evidence_observation_ids, '{}', f.adjudication_ids, '{}', '{}') AS c) dep
        WHERE f.publication_id = pub.publication_id
        UNION ALL
        SELECT xe.security_id::text, xe.evidence_observation_ids, ARRAY[]::uuid[]
        FROM bond_default_exit_evidence_v1 xe WHERE xe.publication_id = pub.publication_id
        UNION ALL
        SELECT r.cusip_id || '|' || r.month::text || '|' || r.view_kind, r.agency_source_ids, r.binding_link_ids
        FROM bond_rating_history_public_v1 r WHERE r.publication_id = pub.publication_id
    ) x
    WHERE x.obs_ids && stale_obs OR x.link_ids && stale_links
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:evidence_superseded:%', bad;
    END IF;

    -- 9. Quality: qualified needs positive receipt, qualified coverage and PIT-verified ratings.
    IF pub.quality_state = 'qualified' AND (
        NOT EXISTS (SELECT 1 FROM bond_default_coverage_v1 c
                    WHERE c.publication_id = pub.publication_id AND c.state = 'qualified')
        OR EXISTS (SELECT 1 FROM bond_default_coverage_v1 c
                   WHERE c.publication_id = pub.publication_id AND c.state IN ('partial', 'unavailable'))
        OR EXISTS (SELECT 1 FROM bond_rating_history_public_v1 r
                   WHERE r.publication_id = pub.publication_id AND r.view_kind = 'public_pit'
                     AND r.state IN ('pit_unverified', 'rights_unverified'))
        -- Qualified rating input: approved agency packages in the inventory, a receipt bound to the
        -- manifest rating_input_digest and to those packages, and every grid month x view inside a
        -- package's verified coverage (missing = no action under qualified input, never no input).
        OR NOT EXISTS (SELECT 1 FROM bond_default_source_package p
                       JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                       WHERE p.source_family = 'agency_rocr_xbrl' AND p.rights_state = 'approved')
        OR pub.validation_rating_input_digest IS DISTINCT FROM pub.rating_input_digest
        OR pub.validation_rating_package_digest IS DISTINCT FROM bond_credit_frame_digest(ARRAY(
               SELECT bond_credit_checked_row_sha256('source_packages', pg_catalog.to_jsonb(p))
               FROM bond_default_source_package p JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
               WHERE p.source_family = 'agency_rocr_xbrl' AND p.rights_state = 'approved'))
        OR EXISTS (SELECT 1 FROM (SELECT DISTINCT r.month, r.view_kind FROM bond_rating_history_public_v1 r
                                  WHERE r.publication_id = pub.publication_id) g
                   WHERE NOT EXISTS (
                       SELECT 1 FROM bond_default_source_package p
                       JOIN pg_catalog.unnest(pkgs) AS v(package_id) USING (package_id)
                       CROSS JOIN LATERAL (
                           SELECT CASE WHEN g.view_kind = 'public_pit' THEN p.public_coverage_start
                                       ELSE p.effective_coverage_start END AS start_date,
                                  CASE WHEN g.view_kind = 'public_pit' THEN p.public_coverage_end
                                       ELSE p.effective_coverage_end END AS frontier) cov
                       WHERE p.source_family = 'agency_rocr_xbrl' AND p.rights_state = 'approved'
                         AND cov.start_date <= g.month
                         AND cov.frontier >= ((g.month + interval '1 month')::date - 1)))) THEN
        RAISE EXCEPTION 'bond_credit_validate:qualified_state_unsupported';
    END IF;

    INSERT INTO bond_credit_lifecycle_tokens (publication_id, backend_pid, xact_id)
    VALUES (pub.publication_id, pg_catalog.pg_backend_pid(), pg_catalog.txid_current());
    UPDATE bond_credit_publications SET lifecycle_state = 'validated', validated_at = pg_catalog.clock_timestamp()
    WHERE publication_id = pub.publication_id;
    DELETE FROM bond_credit_lifecycle_tokens WHERE publication_id = pub.publication_id;
END $$;

-- ---------------------------------------------------------------------------
-- Promotion (compare-and-set, non-regressing T/K, qualified complete only)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_credit_is_revoked(target_publication_id uuid) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
    SELECT EXISTS (SELECT 1 FROM bond_credit_publication_revocations r
                   WHERE r.publication_id = target_publication_id)
$$;

CREATE OR REPLACE FUNCTION bond_credit_promote(target_publication_id uuid, expected_pointer uuid)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    pub bond_credit_publications%ROWTYPE;
    cur bond_credit_publications%ROWTYPE;
    current_id uuid;
BEGIN
    -- Product advisory lock also protects first-pointer creation (absent row).
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bond_credit_evidence_v1', 0));
    SELECT c.publication_id INTO current_id FROM bond_credit_current_pointer c
    WHERE c.product = 'bond_credit_evidence_v1' FOR UPDATE;
    IF current_id IS DISTINCT FROM expected_pointer THEN
        RAISE EXCEPTION 'bond_credit_promote:cas_mismatch:expected=% current=%', expected_pointer, current_id;
    END IF;
    SELECT * INTO pub FROM bond_credit_publications WHERE publication_id = target_publication_id FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'bond_credit_promote:unknown_publication:%', target_publication_id;
    END IF;
    IF pub.product <> 'bond_credit_evidence_v1' THEN
        RAISE EXCEPTION 'bond_credit_promote:product_mismatch';
    END IF;
    IF pub.lifecycle_state <> 'validated' THEN
        RAISE EXCEPTION 'bond_credit_promote:not_validated';
    END IF;
    IF pub.build_scope <> 'complete' OR pub.quality_state <> 'qualified' THEN
        RAISE EXCEPTION 'bond_credit_promote:not_qualified_complete:%/%', pub.build_scope, pub.quality_state;
    END IF;
    IF bond_credit_is_revoked(target_publication_id) THEN
        RAISE EXCEPTION 'bond_credit_promote:revoked';
    END IF;
    -- Re-verify the stored output frames against the manifest immediately before promotion.
    PERFORM bond_credit_assert_output_frames('bond_credit_promote', pub);
    IF current_id = target_publication_id THEN
        RETURN target_publication_id;
    END IF;
    IF current_id IS NOT NULL THEN
        SELECT * INTO cur FROM bond_credit_publications WHERE publication_id = current_id;
        IF pub.target_month < cur.target_month OR pub.knowledge_cutoff < cur.knowledge_cutoff THEN
            RAISE EXCEPTION 'bond_credit_promote:tk_regression:T % -> %, K % -> %',
                cur.target_month, pub.target_month, cur.knowledge_cutoff, pub.knowledge_cutoff;
        END IF;
        IF pub.target_month = cur.target_month AND pub.knowledge_cutoff = cur.knowledge_cutoff
           AND NOT bond_credit_is_revoked(current_id) THEN
            RAISE EXCEPTION 'bond_credit_promote:tk_not_advanced';
        END IF;
    END IF;
    INSERT INTO bond_credit_pointer_tokens (product, backend_pid, xact_id)
    VALUES ('bond_credit_evidence_v1', pg_catalog.pg_backend_pid(), pg_catalog.txid_current())
    ON CONFLICT (product) DO UPDATE SET backend_pid = EXCLUDED.backend_pid, xact_id = EXCLUDED.xact_id;
    INSERT INTO bond_credit_current_pointer (product, publication_id)
    VALUES ('bond_credit_evidence_v1', target_publication_id)
    ON CONFLICT (product) DO UPDATE SET publication_id = EXCLUDED.publication_id,
                                        changed_at = pg_catalog.clock_timestamp();
    DELETE FROM bond_credit_pointer_tokens WHERE product = 'bond_credit_evidence_v1';
    RETURN target_publication_id;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_revoke(target_publication_id uuid, revoke_reason text,
                                              revoke_evidence_digest text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM bond_credit_publications p
                   WHERE p.publication_id = target_publication_id AND p.lifecycle_state = 'validated') THEN
        RAISE EXCEPTION 'bond_credit_revoke:unknown_or_unvalidated:%', target_publication_id;
    END IF;
    INSERT INTO bond_credit_publication_revocations (publication_id, reason, evidence_digest)
    VALUES (target_publication_id, revoke_reason, revoke_evidence_digest);
END $$;

-- ---------------------------------------------------------------------------
-- Readers: the serving pointer and explicit (shadow) build reads refuse revoked builds.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_credit_current_publication(
    expected_target_month date DEFAULT NULL,
    expected_panel_publication_id uuid DEFAULT NULL
) RETURNS bond_credit_publications
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    pub bond_credit_publications%ROWTYPE;
BEGIN
    SELECT p.* INTO pub FROM bond_credit_current_pointer c
    JOIN bond_credit_publications p ON p.publication_id = c.publication_id
    WHERE c.product = 'bond_credit_evidence_v1';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'bond_credit_current_publication:no_current_publication';
    END IF;
    IF bond_credit_is_revoked(pub.publication_id) THEN
        RAISE EXCEPTION 'bond_credit_current_publication:revoked:%', pub.publication_id;
    END IF;
    IF pub.lifecycle_state <> 'validated' OR pub.quality_state <> 'qualified' OR pub.build_scope <> 'complete' THEN
        RAISE EXCEPTION 'bond_credit_current_publication:not_serving_eligible:%', pub.publication_id;
    END IF;
    IF expected_target_month IS NOT NULL AND pub.target_month <> expected_target_month THEN
        RAISE EXCEPTION 'bond_credit_current_publication:target_month_mismatch:% <> %',
            pub.target_month, expected_target_month;
    END IF;
    IF expected_panel_publication_id IS NOT NULL AND pub.panel_publication_id <> expected_panel_publication_id THEN
        RAISE EXCEPTION 'bond_credit_current_publication:panel_mismatch';
    END IF;
    RETURN pub;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_publication(target_publication_id uuid,
                                                        allow_shadow boolean DEFAULT false)
RETURNS bond_credit_publications
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    pub bond_credit_publications%ROWTYPE;
BEGIN
    SELECT * INTO pub FROM bond_credit_publications WHERE publication_id = target_publication_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'bond_credit_read_publication:unknown_publication:%', target_publication_id;
    END IF;
    IF pub.lifecycle_state <> 'validated' THEN
        RAISE EXCEPTION 'bond_credit_read_publication:not_validated:%', target_publication_id;
    END IF;
    IF bond_credit_is_revoked(target_publication_id) THEN
        RAISE EXCEPTION 'bond_credit_read_publication:revoked:%', target_publication_id;
    END IF;
    IF NOT allow_shadow AND (pub.quality_state <> 'qualified' OR pub.build_scope <> 'complete') THEN
        RAISE EXCEPTION 'bond_credit_read_publication:shadow_build_requires_allow_shadow:%', target_publication_id;
    END IF;
    RETURN pub;
END $$;

-- Guarded frame readers: the serving reader has no table privilege and reads each frame of one
-- publication only through these functions, which re-apply bond_credit_read_publication (unknown,
-- unvalidated, revoked and, unless allow_shadow, shadow publications are refused). Output-frame
-- readers live with their tables (bond_default_events_v1.sql, bond_rating_history_public_v1.sql).
CREATE OR REPLACE FUNCTION bond_credit_read_publication_sources(target_publication_id uuid,
                                                                allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_credit_publication_sources
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_credit_publication_sources r WHERE r.publication_id = target_publication_id;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_source_packages(target_publication_id uuid,
                                                            allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_source_package
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_source_package r
    WHERE r.package_id IN (SELECT s.package_id FROM bond_credit_publication_sources s
                           WHERE s.publication_id = target_publication_id);
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_observations(target_publication_id uuid,
                                                         allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_credit_observation
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_credit_observation r
    WHERE r.package_id IN (SELECT s.package_id FROM bond_credit_publication_sources s
                           WHERE s.publication_id = target_publication_id);
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_event_links(target_publication_id uuid,
                                                        allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_event_link
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_event_link r
    WHERE r.package_id IN (SELECT s.package_id FROM bond_credit_publication_sources s
                           WHERE s.publication_id = target_publication_id);
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_adjudications(target_publication_id uuid,
                                                          allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_adjudication
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_adjudication r
    WHERE r.package_id IN (SELECT s.package_id FROM bond_credit_publication_sources s
                           WHERE s.publication_id = target_publication_id);
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_ncen_filings(target_publication_id uuid,
                                                         allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_ncen_filing
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_ncen_filing r
    WHERE r.package_id IN (SELECT s.package_id FROM bond_credit_publication_sources s
                           WHERE s.publication_id = target_publication_id);
END $$;

-- The serving reader has no table privilege (revoked explicitly for re-installs): it reads through
-- bond_credit_current_publication / bond_credit_read_publication / bond_credit_read_* only.
REVOKE ALL ON bond_credit_publications, bond_credit_publication_sources, bond_credit_current_pointer,
    bond_credit_publication_revocations FROM PUBLIC, bond_credit_reader;
GRANT SELECT ON bond_credit_publications, bond_credit_publication_sources, bond_credit_current_pointer,
    bond_credit_publication_revocations TO bond_credit_auditor;
GRANT SELECT, INSERT ON bond_credit_publications, bond_credit_publication_sources TO bond_credit_writer;
GRANT SELECT ON bond_credit_current_pointer, bond_credit_publication_revocations TO bond_credit_writer;
REVOKE ALL ON FUNCTION bond_credit_frame_digest(text[]), bond_credit_id_digest(uuid[]),
    bond_credit_publication_insert_guard(), bond_credit_publication_update_guard(),
    bond_credit_child_insert_guard(), bond_credit_pointer_guard(), bond_credit_publication_source_seal(),
    bond_credit_assert_frame(text, text, integer, text, text[]),
    bond_credit_assert_output_frames(text, bond_credit_publications), bond_credit_validate(uuid),
    bond_credit_revision_cycle(uuid[], uuid[]), bond_credit_is_revoked(uuid), bond_credit_promote(uuid, uuid), bond_credit_revoke(uuid, text, text),
    bond_credit_current_publication(date, uuid), bond_credit_read_publication(uuid, boolean),
    bond_credit_encoded_scalar(text, text), bond_credit_value_encoding(jsonb),
    bond_credit_canonical_json(jsonb), bond_credit_json_digest(jsonb),
    bond_credit_rating_declarations_valid(jsonb),
    bond_credit_rating_scope_declared(jsonb, text, text, text),
    bond_credit_rating_uncleared_covers(jsonb, date),
    bond_credit_rating_input_digest(jsonb, text), bond_credit_row_sha256(jsonb, text[]),
    bond_credit_frame_spec(text), bond_credit_checked_row_sha256(text, jsonb), bond_credit_fingerprint_digest(jsonb),
    bond_credit_validation_digest(jsonb), bond_credit_publication_id_for(text),
    bond_credit_read_publication_sources(uuid, boolean), bond_credit_read_source_packages(uuid, boolean),
    bond_credit_read_observations(uuid, boolean), bond_credit_read_event_links(uuid, boolean),
    bond_credit_read_adjudications(uuid, boolean), bond_credit_read_ncen_filings(uuid, boolean),
    bond_credit_expected_pins(), bond_credit_ts_text(timestamptz), bond_credit_text_digest(text),
    bond_credit_encoded_digest(jsonb), bond_credit_json_texts(text[]), bond_credit_ascii_upper(text),
    bond_credit_ncen_clean(text), bond_credit_ncen_file_number(text), bond_credit_ncen_crd(text),
    bond_credit_ncen_lei(text), bond_credit_ncen_is_sentinel(text), bond_credit_ncen_family_name_key(text),
    bond_credit_ncen_family_key(text, text), bond_credit_ncen_adviser_tokens(jsonb),
    bond_credit_ncen_underwriter_tokens(jsonb), bond_credit_ncen_projection_digest(bond_default_ncen_filing),
    bond_credit_votes(uuid[], uuid[], timestamptz, date), bond_credit_fund_key(bond_credit_observation),
    bond_credit_ncen_visible(uuid[], uuid[], timestamptz, text),
    bond_credit_ncen_selection(uuid[], uuid[], timestamptz, text, date),
    bond_credit_ncen_profile_reasons(bond_default_ncen_filing, text[]),
    bond_credit_ncen_components(text[], uuid[], date, timestamptz, text),
    bond_credit_family_expected(uuid[], uuid[], uuid[], date, timestamptz, text),
    bond_credit_scope_head(uuid[], uuid[], text, uuid), bond_credit_link_time(uuid[], uuid[], text, uuid),
    bond_credit_usable_link(uuid[], uuid[], uuid[], text, uuid),
    bond_credit_corroboration_ok(uuid[], uuid[], uuid[], uuid[], text, uuid, text, date),
    bond_credit_vote_state(uuid[], uuid[], timestamptz, date, text, uuid, uuid, boolean),
    bond_credit_rating_action_kind(text, text), bond_credit_rating_action_input_digest(text, uuid[], uuid[])
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bond_credit_read_publication_sources(uuid, boolean),
    bond_credit_read_source_packages(uuid, boolean), bond_credit_read_observations(uuid, boolean),
    bond_credit_read_event_links(uuid, boolean), bond_credit_read_adjudications(uuid, boolean),
    bond_credit_read_ncen_filings(uuid, boolean)
    TO bond_credit_reader;
GRANT EXECUTE ON FUNCTION bond_credit_frame_digest(text[]), bond_credit_id_digest(uuid[]),
    bond_credit_is_revoked(uuid), bond_credit_current_publication(date, uuid),
    bond_credit_read_publication(uuid, boolean),
    bond_credit_encoded_scalar(text, text), bond_credit_value_encoding(jsonb), bond_credit_row_sha256(jsonb, text[]),
    bond_credit_frame_spec(text), bond_credit_checked_row_sha256(text, jsonb), bond_credit_fingerprint_digest(jsonb),
    bond_credit_validation_digest(jsonb), bond_credit_publication_id_for(text), bond_credit_expected_pins(),
    bond_credit_ts_text(timestamptz), bond_credit_text_digest(text), bond_credit_encoded_digest(jsonb),
    bond_credit_rating_action_kind(text, text) TO bond_credit_reader, bond_credit_writer;
GRANT EXECUTE ON FUNCTION bond_credit_validate(uuid), bond_credit_promote(uuid, uuid),
    bond_credit_revoke(uuid, text, text) TO bond_credit_writer;

COMMIT;
