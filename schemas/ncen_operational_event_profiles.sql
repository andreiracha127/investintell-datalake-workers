-- Immutable N-CEN operational-event profiles.  Grain is one row per
-- (publication, source run, accession, registrant CIK): a registrant-level
-- filing.  The families are disclosure signals only -- counts and tri-state
-- flags, with NO inference of misconduct (a disclosure is not evidence of a
-- failure).  Registrant-level events come from REGISTRANT/SUBMISSION; fund-level
-- provider/adviser changes are rolled up across the filing's funds; CCO and
-- valuation-method-change children are pre-aggregated at their own grain.

-- OR-style tri-state rollup: any 'true' wins; else any reported 'false' wins;
-- otherwise 'not_reported'.  NULL and absent elements are ignored.
CREATE OR REPLACE FUNCTION ncen_tristate_or(vals text[])
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN 'true' = ANY(vals) THEN 'true'
        WHEN 'false' = ANY(vals) THEN 'false'
        ELSE 'not_reported'
    END
$$;

CREATE TABLE IF NOT EXISTS ncen_operational_event_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    effective_date date NOT NULL,
    form text NOT NULL,
    is_amendment integer NOT NULL CHECK (is_amendment IN (0, 1)),
    registrant_cik text NOT NULL,
    methodology_version text NOT NULL DEFAULT 'ncen_operational_event_v1',
    measured_at date NOT NULL,
    operational_event_state text NOT NULL CHECK (operational_event_state IN ('available','unavailable','not_applicable')),
    operational_event_reason_code text,
    operational_events jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, registrant_cik),
    CHECK (methodology_version = 'ncen_operational_event_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((operational_event_state = 'available') = (operational_events IS NOT NULL AND operational_event_reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS ncen_operational_event_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ncen_operational_event_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN operational-event build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_operational_event_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN operational-event build identity requires a prepared operational-event publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_operational_event_build_guard ON ncen_operational_event_builds;
CREATE TRIGGER ncen_operational_event_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_operational_event_builds
FOR EACH ROW EXECUTE FUNCTION ncen_operational_event_build_guard();

CREATE OR REPLACE FUNCTION ncen_operational_event_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN operational event profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_operational_event_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN operational event profile requires a prepared operational-event publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM ncen_operational_event_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-CEN operational event profile requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_operational_event_write_guard ON ncen_operational_event_profiles;
CREATE TRIGGER ncen_operational_event_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_operational_event_profiles
FOR EACH ROW EXECUTE FUNCTION ncen_operational_event_write_guard();

CREATE OR REPLACE FUNCTION build_ncen_operational_event_profiles(
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
        RAISE EXCEPTION 'N-CEN operational-event build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'ncen_operational_event_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN operational-event build requires a prepared operational-event publication';
    END IF;

    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || effective_date::text || ':' || form,
                '|' ORDER BY ingestion_run_id, accession_number, effective_date, form
            ), '')) || md5('ncen_operational_event_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM ncen_effective_filings
    WHERE effective_date <= as_of_date;

    -- Each effective filing exposes at most one REGISTRANT disclosure surface.
    IF EXISTS (
        SELECT 1
        FROM ncen_effective_filings e
        JOIN ncen_raw_v2_rows r ON r.ingestion_run_id=e.ingestion_run_id
                               AND r.source_table='REGISTRANT.tsv'
                               AND r.parse_status='typed'
                               AND r.accession_number=e.accession_number
        WHERE e.effective_date <= as_of_date
        GROUP BY e.ingestion_run_id, e.accession_number
        HAVING count(*) <> 1
    ) THEN
        RAISE EXCEPTION 'ambiguous N-CEN registrant identity for effective filing';
    END IF;

    INSERT INTO ncen_operational_event_builds
        (publication_id, input_fingerprint, as_of_date, effective_input_count)
    VALUES (target_publication_id, computed_fingerprint, as_of_date, selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM ncen_operational_event_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-CEN operational-event publication is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'N-CEN operational-event publication is already pinned to effective-input fingerprint %', pinned_fingerprint;
    END IF;

    WITH selected AS (
        SELECT e.ingestion_run_id AS source_run_id, e.accession_number, e.effective_date, e.form,
               e.is_amendment, e.registrant_cik, e.raw_row_id AS submission_raw_row_id,
               reg.typed_projection AS reg_evidence, sub.typed_projection AS sub_evidence,
               reg.raw_row_id AS registrant_raw_row_id
        FROM ncen_effective_filings e
        JOIN ncen_raw_v2_rows sub ON sub.raw_row_id=e.raw_row_id
        LEFT JOIN ncen_raw_v2_rows reg ON reg.ingestion_run_id=e.ingestion_run_id
                                      AND reg.source_table='REGISTRANT.tsv'
                                      AND reg.parse_status='typed'
                                      AND reg.accession_number=e.accession_number
        WHERE e.effective_date <= as_of_date
    ), assembled AS (
        SELECT s.*,
            cco.change_tristate AS cco_change,
            vmc.detail_count, vmc.detail_evidence,
            fr.provider_change_funds, fr.adviser_change_funds, fr.funds_total,
            fr.provider_change_fund_count, fr.adviser_change_fund_count,
            jsonb_build_object(
                'adviser_change', fr.adviser_change_funds,
                'provider_change', ncen_tristate_or(ARRAY[
                    ncen_tristate_flag(s.reg_evidence->>'IS_UNDERWRITER_HIRED_OR_FIRED'),
                    ncen_tristate_flag(s.reg_evidence->>'IS_PUB_ACCOUNTANT_CHANGED'),
                    fr.provider_change_funds]),
                'cco_change', cco.change_tristate,
                'valuation_method_change', ncen_tristate_or(ARRAY[
                    ncen_tristate_flag(s.reg_evidence->>'IS_VALUE_METHOD_CHANGED'),
                    CASE WHEN vmc.detail_count > 0 THEN 'true' ELSE 'not_reported' END]),
                'nav_error_correction', ncen_tristate_flag(s.reg_evidence->>'IS_NAV_ERROR_CORRECTED'),
                'material_weakness', ncen_tristate_flag(s.reg_evidence->>'IS_MATERIAL_WEAKNESS_NOTED'),
                'qualified_audit_opinion', ncen_tristate_flag(s.reg_evidence->>'IS_ACCT_OPINION_QUALIFIED'),
                'accounting_principle_change', ncen_tristate_or(ARRAY[
                    ncen_tristate_flag(s.reg_evidence->>'IS_ACCT_PRINCIPLE_CHANGED'),
                    ncen_tristate_flag(s.sub_evidence->>'IS_CHANGE_ACC_PRINCIPLES')]),
                'legal_proceedings', ncen_tristate_or(ARRAY[
                    ncen_tristate_flag(s.reg_evidence->>'HAS_LEGAL_PROCEEDING'),
                    ncen_tristate_flag(s.sub_evidence->>'IS_LEGAL_PROCEEDINGS')]),
                'financial_support', ncen_tristate_or(ARRAY[
                    ncen_tristate_flag(s.reg_evidence->>'FINANCIAL_SUPPORT_2REGISTRANT'),
                    ncen_tristate_flag(s.sub_evidence->>'IS_PROVISION_FINANCIAL_SUPPORT')])
            ) AS normalized
        FROM selected s
        CROSS JOIN LATERAL (
            SELECT ncen_tristate_or(COALESCE(array_agg(ncen_tristate_flag(c.typed_projection->>'IS_CHANGED_SINCE_LAST_FILING')), ARRAY[]::text[])) AS change_tristate
            FROM ncen_raw_v2_rows c
            WHERE c.ingestion_run_id=s.source_run_id AND c.accession_number=s.accession_number
              AND c.source_table='CHIEF_COMPLIANCE_OFFICER.tsv' AND c.parse_status='typed'
        ) cco
        CROSS JOIN LATERAL (
            SELECT count(*) AS detail_count,
                   COALESCE(jsonb_agg(jsonb_build_object('raw_row_id',v.raw_row_id,'evidence',v.typed_projection) ORDER BY v.raw_row_id),'[]'::jsonb) AS detail_evidence
            FROM ncen_raw_v2_rows v
            WHERE v.ingestion_run_id=s.source_run_id AND v.accession_number=s.accession_number
              AND v.source_table='VALUATION_METHOD_CHANGE.tsv' AND v.parse_status='typed'
        ) vmc
        CROSS JOIN LATERAL (
            SELECT
                ncen_tristate_or(COALESCE(array_agg(pf.fund_pc), ARRAY[]::text[])) AS provider_change_funds,
                ncen_tristate_or(COALESCE(array_agg(pf.fund_adv), ARRAY[]::text[])) AS adviser_change_funds,
                count(*) AS funds_total,
                count(*) FILTER (WHERE pf.fund_pc='true') AS provider_change_fund_count,
                count(*) FILTER (WHERE pf.fund_adv='true') AS adviser_change_fund_count
            FROM (
                SELECT fri.fund_id,
                    ncen_tristate_or(ARRAY[
                        ncen_tristate_flag(fri.typed_projection->>'HAS_XAGENT_HIRED_FIRED_MI'),
                        ncen_tristate_flag(fri.typed_projection->>'HAS_PRICING_SRVC_HIRED_FIRED'),
                        ncen_tristate_flag(fri.typed_projection->>'HAS_CUSTODIAN_HIRED_FIRED_MI'),
                        ncen_tristate_flag(fri.typed_projection->>'HAS_SH_SRVC_HIRED_FIRED'),
                        ncen_tristate_flag(fri.typed_projection->>'HAS_ADMIN_HIRED_FIRED'),
                        ncen_tristate_flag(fri.typed_projection->>'HAS_XAGENT_HIRED_FIRED_CE'),
                        ncen_tristate_flag(fri.typed_projection->>'HAS_CUSTODIAN_HIRED_FIRED_CE')
                    ]) AS fund_pc,
                    (SELECT ncen_tristate_or(COALESCE(array_agg(
                        CASE WHEN NULLIF(btrim(a.typed_projection->>'ADVISOR_TERMINATED_DATE'),'') IS NOT NULL THEN 'true'
                             ELSE ncen_tristate_flag(a.typed_projection->>'IS_ADVISOR_HIRED') END), ARRAY[]::text[]))
                     FROM ncen_raw_v2_rows a
                     WHERE a.ingestion_run_id=s.source_run_id AND a.fund_id=fri.fund_id
                       AND a.source_table='ADVISER.tsv' AND a.parse_status='typed') AS fund_adv
                FROM ncen_raw_v2_rows fri
                WHERE fri.ingestion_run_id=s.source_run_id AND fri.accession_number=s.accession_number
                  AND fri.source_table='FUND_REPORTED_INFO.tsv' AND fri.parse_status='typed'
            ) pf
        ) fr
    )
    INSERT INTO ncen_operational_event_profiles(
        publication_id,source_run_id,accession_number,effective_date,form,is_amendment,registrant_cik,
        measured_at,operational_event_state,operational_event_reason_code,operational_events,provenance,coverage
    )
    SELECT target_publication_id,a.source_run_id,a.accession_number,a.effective_date,a.form,a.is_amendment,a.registrant_cik,
           as_of_date,
           CASE WHEN a.reg_evidence IS NOT NULL THEN 'available' ELSE 'unavailable' END,
           CASE WHEN a.reg_evidence IS NOT NULL THEN NULL ELSE 'registrant_disclosure_not_reported' END,
           CASE WHEN a.reg_evidence IS NOT NULL THEN jsonb_build_object(
               'events', jsonb_build_object(
                    'normalized', a.normalized,
                    'source_lexical', jsonb_build_object(
                        'registrant', jsonb_strip_nulls(jsonb_build_object(
                            'IS_UNDERWRITER_HIRED_OR_FIRED', a.reg_evidence->'IS_UNDERWRITER_HIRED_OR_FIRED',
                            'IS_PUB_ACCOUNTANT_CHANGED', a.reg_evidence->'IS_PUB_ACCOUNTANT_CHANGED',
                            'IS_VALUE_METHOD_CHANGED', a.reg_evidence->'IS_VALUE_METHOD_CHANGED',
                            'IS_NAV_ERROR_CORRECTED', a.reg_evidence->'IS_NAV_ERROR_CORRECTED',
                            'IS_MATERIAL_WEAKNESS_NOTED', a.reg_evidence->'IS_MATERIAL_WEAKNESS_NOTED',
                            'IS_ACCT_OPINION_QUALIFIED', a.reg_evidence->'IS_ACCT_OPINION_QUALIFIED',
                            'IS_ACCT_PRINCIPLE_CHANGED', a.reg_evidence->'IS_ACCT_PRINCIPLE_CHANGED',
                            'HAS_LEGAL_PROCEEDING', a.reg_evidence->'HAS_LEGAL_PROCEEDING',
                            'FINANCIAL_SUPPORT_2REGISTRANT', a.reg_evidence->'FINANCIAL_SUPPORT_2REGISTRANT')),
                        'submission', jsonb_strip_nulls(jsonb_build_object(
                            'IS_LEGAL_PROCEEDINGS', a.sub_evidence->'IS_LEGAL_PROCEEDINGS',
                            'IS_PROVISION_FINANCIAL_SUPPORT', a.sub_evidence->'IS_PROVISION_FINANCIAL_SUPPORT',
                            'IS_CHANGE_ACC_PRINCIPLES', a.sub_evidence->'IS_CHANGE_ACC_PRINCIPLES')))
               ),
               'valuation_method_changes', a.detail_evidence,
               'fund_change_rollup', jsonb_build_object(
                    'funds_total', a.funds_total,
                    'provider_change_fund_count', a.provider_change_fund_count,
                    'adviser_change_fund_count', a.adviser_change_fund_count),
               'derived', jsonb_build_object(
                    'operational_change_count', (SELECT count(*) FROM jsonb_each_text(a.normalized) WHERE value = 'true'),
                    'valuation_control_risk_indicator',
                        (a.normalized->>'material_weakness' = 'true'
                         OR a.normalized->>'qualified_audit_opinion' = 'true'
                         OR a.normalized->>'nav_error_correction' = 'true'
                         OR a.normalized->>'valuation_method_change' = 'true'))
           ) END,
           jsonb_build_object('effective_selection_view','ncen_effective_filings','submission_raw_row_id',a.submission_raw_row_id,
                              'registrant_raw_row_id',a.registrant_raw_row_id,'registrant_cik',a.registrant_cik,
                              'source_run_id',a.source_run_id,'accession_number',a.accession_number,
                              'registrant_source_table','REGISTRANT.tsv'),
           jsonb_build_object('event_families_total', (SELECT count(*) FROM jsonb_each_text(a.normalized)),
                              'event_families_reported', (SELECT count(*) FROM jsonb_each_text(a.normalized) WHERE value <> 'not_reported'),
                              'valuation_method_change_detail_count', a.detail_count,
                              'funds_total', a.funds_total)
    FROM assembled a
    ON CONFLICT (publication_id,source_run_id,accession_number,registrant_cik) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_ncen_operational_event_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN ncen_operational_event_profiles p ON p.publication_id=c.publication_id
WHERE c.product='ncen_operational_event_v1';
