-- Immutable N-CEN structure & regulatory-reliance profiles.  Grain is one row
-- per (publication, source run, accession, FUND_ID): the fund reported inside an
-- amendment-aware effective filing.  Structure and reliance flags are tri-state
-- (true/false/not_reported); an empty conditional flag is NEVER coerced to the
-- negative.  The current view never reaches back into raw rows.

CREATE TABLE IF NOT EXISTS ncen_structure_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    effective_date date NOT NULL,
    form text NOT NULL,
    is_amendment integer NOT NULL CHECK (is_amendment IN (0, 1)),
    registrant_cik text NOT NULL,
    fund_id text NOT NULL,
    series_id text,
    methodology_version text NOT NULL DEFAULT 'ncen_structure_profile_v1',
    measured_at date NOT NULL,
    report_period_lt_12month text NOT NULL CHECK (report_period_lt_12month IN ('true','false','not_reported')),
    structure_state text NOT NULL CHECK (structure_state IN ('available','unavailable','not_applicable')),
    structure_reason_code text,
    structure_flags jsonb NOT NULL,
    reliance_state text NOT NULL CHECK (reliance_state IN ('available','unavailable','not_applicable')),
    reliance_reason_code text,
    regulatory_reliance jsonb NOT NULL,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, fund_id),
    CHECK (methodology_version = 'ncen_structure_profile_v1'),
    CHECK (jsonb_typeof(structure_flags) = 'object'),
    CHECK (jsonb_typeof(regulatory_reliance) = 'object'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    -- The tri-state payload is always retained (empty flags are evidence, not a
    -- gap); the family state only reports whether anything was disclosed.
    CHECK ((structure_state = 'available') = (structure_reason_code IS NULL)),
    CHECK ((reliance_state = 'available') = (reliance_reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS ncen_structure_profile_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ncen_structure_profile_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN structure-profile build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_structure_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN structure-profile build identity requires a prepared structure-profile publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_structure_profile_build_guard ON ncen_structure_profile_builds;
CREATE TRIGGER ncen_structure_profile_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_structure_profile_builds
FOR EACH ROW EXECUTE FUNCTION ncen_structure_profile_build_guard();

CREATE OR REPLACE FUNCTION ncen_structure_profile_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN structure profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_structure_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN structure profile requires a prepared structure-profile publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM ncen_structure_profile_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-CEN structure profile requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_structure_profile_write_guard ON ncen_structure_profiles;
CREATE TRIGGER ncen_structure_profile_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_structure_profiles
FOR EACH ROW EXECUTE FUNCTION ncen_structure_profile_write_guard();

CREATE OR REPLACE FUNCTION build_ncen_structure_profiles(
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
        RAISE EXCEPTION 'N-CEN structure-profile build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'ncen_structure_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN structure-profile build requires a prepared structure-profile publication';
    END IF;

    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || effective_date::text || ':' || form,
                '|' ORDER BY ingestion_run_id, accession_number, effective_date, form
            ), '')) || md5('ncen_structure_profile_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM ncen_effective_filings
    WHERE effective_date <= as_of_date;

    PERFORM ncen_assert_effective_fund_identity(as_of_date);

    INSERT INTO ncen_structure_profile_builds
        (publication_id, input_fingerprint, as_of_date, effective_input_count)
    VALUES (target_publication_id, computed_fingerprint, as_of_date, selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM ncen_structure_profile_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-CEN structure-profile publication is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'N-CEN structure-profile publication is already pinned to effective-input fingerprint %', pinned_fingerprint;
    END IF;

    WITH selected_funds AS (
        SELECT e.ingestion_run_id AS source_run_id, e.accession_number, e.effective_date, e.form, e.is_amendment,
               e.registrant_cik, e.raw_row_id AS submission_raw_row_id,
               f.fund_id, f.raw_row_id AS fund_raw_row_id, f.typed_projection AS fund_evidence,
               sub.typed_projection AS submission_evidence
        FROM ncen_effective_filings e
        JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                               AND f.source_table='FUND_REPORTED_INFO.tsv'
                               AND f.parse_status='typed'
                               AND f.accession_number=e.accession_number
        JOIN ncen_raw_v2_rows sub ON sub.raw_row_id=e.raw_row_id
        WHERE e.effective_date <= as_of_date
    ), assembled AS (
        SELECT s.*,
            jsonb_build_object(
                'etf', ncen_tristate_flag(s.fund_evidence->>'IS_ETF'),
                'etmf', ncen_tristate_flag(s.fund_evidence->>'IS_ETMF'),
                'index', ncen_tristate_flag(s.fund_evidence->>'IS_INDEX'),
                'inverse', ncen_tristate_flag(s.fund_evidence->>'IS_MULTI_INVERSE_INDEX'),
                'interval', ncen_tristate_flag(s.fund_evidence->>'IS_INTERVAL'),
                'fund_of_funds', ncen_tristate_flag(s.fund_evidence->>'IS_FUND_OF_FUND'),
                'master_feeder', ncen_tristate_flag(s.fund_evidence->>'IS_MASTER_FEEDER'),
                'money_market', ncen_tristate_flag(s.fund_evidence->>'IS_MONEY_MARKET'),
                'target_date', ncen_tristate_flag(s.fund_evidence->>'IS_TARGET_DATE'),
                'non_diversified', ncen_tristate_flag(s.fund_evidence->>'IS_NON_DIVERSIFIED'),
                'foreign_subsidiary', ncen_tristate_flag(s.fund_evidence->>'IS_FOREIGN_SUBSIDIARY'),
                'underlying_fund', ncen_tristate_flag(s.fund_evidence->>'IS_UNDERLYING_FUND')
            ) AS structure_normalized,
            jsonb_strip_nulls(jsonb_build_object(
                'etf', s.fund_evidence->'IS_ETF', 'etmf', s.fund_evidence->'IS_ETMF',
                'index', s.fund_evidence->'IS_INDEX', 'inverse', s.fund_evidence->'IS_MULTI_INVERSE_INDEX',
                'interval', s.fund_evidence->'IS_INTERVAL', 'fund_of_funds', s.fund_evidence->'IS_FUND_OF_FUND',
                'master_feeder', s.fund_evidence->'IS_MASTER_FEEDER', 'money_market', s.fund_evidence->'IS_MONEY_MARKET',
                'target_date', s.fund_evidence->'IS_TARGET_DATE', 'non_diversified', s.fund_evidence->'IS_NON_DIVERSIFIED',
                'foreign_subsidiary', s.fund_evidence->'IS_FOREIGN_SUBSIDIARY', 'underlying_fund', s.fund_evidence->'IS_UNDERLYING_FUND'
            )) AS structure_lexical,
            jsonb_build_object(
                'rule_6c_11', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_6C_11'),
                'rule_12d1_4', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_12D1_4'),
                'rule_18f_4', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_18F_4'),
                'rule_18f_4c2', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_18F_4C2'),
                'rule_18f_4c4', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_18F_4C4'),
                'rule_18f_4di', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_18F_4DI'),
                'rule_18f_4dii', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_18F_4DII'),
                'rule_18f_4e', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_18F_4E'),
                'rule_18f_4f', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_18F_4F'),
                'rule_17a_7', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_17A_7'),
                'rule_23c_1', ncen_tristate_flag(s.fund_evidence->>'IS_RELYON_RULE_23C_1')
            ) AS reliance_normalized,
            jsonb_strip_nulls(jsonb_build_object(
                'rule_6c_11', s.fund_evidence->'IS_RELYON_RULE_6C_11', 'rule_12d1_4', s.fund_evidence->'IS_RELYON_RULE_12D1_4',
                'rule_18f_4', s.fund_evidence->'IS_RELYON_RULE_18F_4', 'rule_18f_4c2', s.fund_evidence->'IS_RELYON_RULE_18F_4C2',
                'rule_18f_4c4', s.fund_evidence->'IS_RELYON_RULE_18F_4C4', 'rule_18f_4di', s.fund_evidence->'IS_RELYON_RULE_18F_4DI',
                'rule_18f_4dii', s.fund_evidence->'IS_RELYON_RULE_18F_4DII', 'rule_18f_4e', s.fund_evidence->'IS_RELYON_RULE_18F_4E',
                'rule_18f_4f', s.fund_evidence->'IS_RELYON_RULE_18F_4F', 'rule_17a_7', s.fund_evidence->'IS_RELYON_RULE_17A_7',
                'rule_23c_1', s.fund_evidence->'IS_RELYON_RULE_23C_1'
            )) AS reliance_lexical
        FROM selected_funds s
    )
    INSERT INTO ncen_structure_profiles(
        publication_id,source_run_id,accession_number,effective_date,form,is_amendment,registrant_cik,fund_id,series_id,
        measured_at,report_period_lt_12month,
        structure_state,structure_reason_code,structure_flags,
        reliance_state,reliance_reason_code,regulatory_reliance,provenance,coverage
    )
    SELECT target_publication_id,a.source_run_id,a.accession_number,a.effective_date,a.form,a.is_amendment,
           a.registrant_cik,a.fund_id,NULLIF(btrim(a.fund_evidence->>'SERIES_ID'),''),as_of_date,
           ncen_tristate_flag(a.submission_evidence->>'IS_REPORT_PERIOD_LT_12MONTH'),
           CASE WHEN sc.reported > 0 THEN 'available' ELSE 'unavailable' END,
           CASE WHEN sc.reported > 0 THEN NULL ELSE 'fund_structure_not_reported' END,
           jsonb_build_object('normalized',a.structure_normalized,'source_lexical',a.structure_lexical),
           CASE WHEN rc.reported > 0 THEN 'available' ELSE 'unavailable' END,
           CASE WHEN rc.reported > 0 THEN NULL ELSE 'regulatory_reliance_not_reported' END,
           jsonb_build_object('normalized',a.reliance_normalized,'source_lexical',a.reliance_lexical),
           jsonb_build_object('effective_selection_view','ncen_effective_filings',
                              'submission_raw_row_id',a.submission_raw_row_id,'fund_raw_row_id',a.fund_raw_row_id,
                              'registrant_cik',a.registrant_cik,'source_run_id',a.source_run_id,
                              'accession_number',a.accession_number,'fund_source_table','FUND_REPORTED_INFO.tsv'),
           jsonb_build_object('structure_flags_total',jsonb_array_length(to_jsonb(ARRAY(SELECT jsonb_object_keys(a.structure_normalized)))),
                              'structure_flags_reported',sc.reported,
                              'reliance_rules_total',jsonb_array_length(to_jsonb(ARRAY(SELECT jsonb_object_keys(a.reliance_normalized)))),
                              'reliance_rules_reported',rc.reported)
    FROM assembled a
    CROSS JOIN LATERAL (
        SELECT count(*) FILTER (WHERE value <> 'not_reported') AS reported
        FROM jsonb_each_text(a.structure_normalized)
    ) sc
    CROSS JOIN LATERAL (
        SELECT count(*) FILTER (WHERE value <> 'not_reported') AS reported
        FROM jsonb_each_text(a.reliance_normalized)
    ) rc
    ON CONFLICT (publication_id,source_run_id,accession_number,fund_id) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_ncen_structure_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN ncen_structure_profiles p ON p.publication_id=c.publication_id
WHERE c.product='ncen_structure_profile_v1';
