-- Immutable RR1 portfolio-turnover snapshot.  Grain is one row per turnover
-- context (publication, source run, accession, series, class, and the preserved
-- measure/document/dimensions/occurrence): the numeric portfolio-turnover rate
-- and the co-presence of the narrative turnover disclosure.
--
-- The numeric rate is the ``uom=pure`` fraction PortfolioTurnoverRate carried in
-- num.tsv; the narrative is PortfolioTurnoverTextBlock carried in txt.tsv.  The
-- FULL narrative text is never copied into this snapshot -- only its presence,
-- an internal digest + length (INTERNAL provenance), and a mechanical
-- number<->text corroboration flag (public) are kept.  Corroboration is an
-- observation, not a judgement.  The builder consumes only the amendment-aware
-- effective selection; it never re-opens raw RR1.

-- Frozen RR taxonomy local names, single authority for this snapshot.
CREATE OR REPLACE FUNCTION rr1_turnover_concept_map()
RETURNS TABLE(canonical_concept text, original_tag text, source_table text)
LANGUAGE sql IMMUTABLE AS $$
    VALUES ('turnover_rate', 'PortfolioTurnoverRate', 'num.tsv'),
           ('turnover_narrative', 'PortfolioTurnoverTextBlock', 'txt.tsv')
$$;

CREATE TABLE IF NOT EXISTS rr1_turnover_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    series_id text NOT NULL,
    class_id text NOT NULL DEFAULT '',
    data_date date NOT NULL,
    measure_id text NOT NULL DEFAULT '',
    document_id text NOT NULL DEFAULT '',
    dimensions text NOT NULL DEFAULT '',
    occurrence text NOT NULL DEFAULT '',
    effective_date date NOT NULL,
    filed_date date NOT NULL,
    form text NOT NULL,
    turnover_rate numeric,
    declared_unit text,
    original_tag text,
    original_version text,
    turnover_numeric_present boolean NOT NULL,
    turnover_text_present boolean NOT NULL,
    narrative_consistency text NOT NULL CHECK (narrative_consistency IN (
        'corroborated', 'unreconciled', 'number_only', 'text_only'
    )),
    status text NOT NULL CHECK (status IN ('available', 'degraded', 'unavailable', 'not_applicable')),
    reason_code text,
    methodology_version text NOT NULL DEFAULT 'rr1_turnover_profile_v1',
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, series_id, class_id, data_date,
                 measure_id, document_id, dimensions, occurrence),
    CHECK (methodology_version = 'rr1_turnover_profile_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    -- A turnover context exists only if at least one leg was disclosed.
    CHECK (turnover_numeric_present OR turnover_text_present),
    -- The numeric rate is available iff it parsed to a number; the rate can only
    -- exist when the numeric leg is present (never fabricated from the narrative).
    CHECK ((status = 'available') = (turnover_rate IS NOT NULL)),
    CHECK ((status = 'available') = (reason_code IS NULL)),
    CHECK (turnover_rate IS NULL OR turnover_numeric_present),
    CHECK (declared_unit IS NULL OR turnover_numeric_present),
    CHECK ((original_tag IS NOT NULL) = turnover_numeric_present),
    -- Consistency states are a total function of leg co-presence.
    CHECK ((narrative_consistency = 'number_only') = (turnover_numeric_present AND NOT turnover_text_present)),
    CHECK ((narrative_consistency = 'text_only') = (turnover_text_present AND NOT turnover_numeric_present)),
    CHECK ((narrative_consistency IN ('corroborated', 'unreconciled')) = (turnover_numeric_present AND turnover_text_present))
);

CREATE TABLE IF NOT EXISTS rr1_turnover_profile_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION rr1_turnover_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 turnover build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_turnover_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 turnover build identity requires a prepared turnover publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_turnover_build_guard ON rr1_turnover_profile_builds;
CREATE TRIGGER rr1_turnover_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_turnover_profile_builds
FOR EACH ROW EXECUTE FUNCTION rr1_turnover_build_guard();

CREATE OR REPLACE FUNCTION rr1_turnover_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 turnover profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_turnover_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 turnover profile requires a prepared turnover publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM rr1_turnover_profile_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS NULL THEN
        RAISE EXCEPTION 'RR1 turnover profile requires pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_turnover_write_guard ON rr1_turnover_profiles;
CREATE TRIGGER rr1_turnover_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_turnover_profiles
FOR EACH ROW EXECUTE FUNCTION rr1_turnover_write_guard();

CREATE OR REPLACE FUNCTION build_rr1_turnover_profiles(
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
        RAISE EXCEPTION 'RR1 turnover build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'rr1_turnover_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 turnover build requires a prepared turnover publication';
    END IF;

    -- Canonical inputs: only RR-namespaced (``rr/%``) facts whose exact table+tag
    -- is in the frozen concept map.  A custom-namespaced or unmapped tag (version =
    -- accession, not ``rr/%``) can never match, so it never enters the snapshot
    -- (Global Constraint 5).
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_turnover_concept_map() m
          ON m.original_tag = f.tag AND m.source_table = f.source_table
        WHERE f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    )
    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || canonical_concept || ':' || tag || ':' || version || ':' ||
                data_date::text || ':' || COALESCE(series_id,'') || ':' || COALESCE(class_id,'') || ':' || COALESCE(measure_id,'') || ':' ||
                COALESCE(document_id,'') || ':' || COALESCE(dimensions,'') || ':' || COALESCE(occurrence,'') || ':' ||
                effective_date::text || ':' || filed_date::text || ':' || fact_typed_projection::text,
                '|' ORDER BY ingestion_run_id, accession_number, canonical_concept, tag, version, data_date,
                             series_id, class_id, measure_id, document_id, dimensions, occurrence, effective_date, filed_date, raw_row_id
           ), '')) || md5('rr1_turnover_profile_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM selected;

    INSERT INTO rr1_turnover_profile_builds(publication_id,input_fingerprint,as_of_date,effective_input_count)
    VALUES(target_publication_id,computed_fingerprint,as_of_date,selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM rr1_turnover_profile_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'RR1 turnover build is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'RR1 turnover build is already pinned to effective-input fingerprint';
    END IF;

    -- Fan-out guard: each leg (numeric rate, narrative) is pre-aggregated to at
    -- most one fact per preserved context grain BEFORE the legs are combined.  A
    -- duplicated leg would multiply the turnover row, so it is a hard failure.
    -- Turnover is a series/prospectus-level fact, so class identity is optional
    -- here (unlike the class-grain snapshots); series identity is mandatory.
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_turnover_concept_map() m
          ON m.original_tag = f.tag AND m.source_table = f.source_table
        WHERE f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    )
    SELECT CASE
        WHEN EXISTS (SELECT 1 FROM selected WHERE NULLIF(btrim(series_id),'') IS NULL)
            THEN 'missing RR1 series identity'
        WHEN EXISTS (
            SELECT 1 FROM selected
            GROUP BY ingestion_run_id,accession_number,series_id,COALESCE(class_id,''),data_date,COALESCE(measure_id,''),
                     COALESCE(document_id,''),COALESCE(dimensions,''),COALESCE(occurrence,''),effective_date,filed_date,canonical_concept
            HAVING count(*) > 1
        ) THEN 'conflicting RR1 turnover facts'
    END INTO parent_state;
    IF parent_state IS NOT NULL THEN RAISE EXCEPTION '%', parent_state; END IF;

    WITH selected AS (
        SELECT f.*, m.canonical_concept,
               NULLIF(btrim(f.fact_typed_projection->>'value'),'') AS raw_value,
               NULLIF(btrim(f.fact_typed_projection->>'uom'),'') AS declared_unit
        FROM rr1_effective_facts f JOIN rr1_turnover_concept_map() m
          ON m.original_tag = f.tag AND m.source_table = f.source_table
        WHERE f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    ), rate_leg AS (
        SELECT ingestion_run_id,accession_number,series_id,COALESCE(class_id,'') AS class_id,data_date,
               COALESCE(measure_id,'') AS measure_id,COALESCE(document_id,'') AS document_id,
               COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
               effective_date,filed_date,form,tag,version,raw_value,declared_unit,raw_row_id
        FROM selected WHERE canonical_concept='turnover_rate'
    ), text_leg AS (
        SELECT ingestion_run_id,accession_number,series_id,COALESCE(class_id,'') AS class_id,data_date,
               COALESCE(measure_id,'') AS measure_id,COALESCE(document_id,'') AS document_id,
               COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
               effective_date,filed_date,form,tag,version,raw_value AS text_value,raw_row_id
        FROM selected WHERE canonical_concept='turnover_narrative'
    ), combined AS (
        SELECT
            COALESCE(r.ingestion_run_id,t.ingestion_run_id) AS ingestion_run_id,
            COALESCE(r.accession_number,t.accession_number) AS accession_number,
            COALESCE(r.series_id,t.series_id) AS series_id,
            COALESCE(r.class_id,t.class_id) AS class_id,
            COALESCE(r.data_date,t.data_date) AS data_date,
            COALESCE(r.measure_id,t.measure_id) AS measure_id,
            COALESCE(r.document_id,t.document_id) AS document_id,
            COALESCE(r.dimensions,t.dimensions) AS dimensions,
            COALESCE(r.occurrence,t.occurrence) AS occurrence,
            COALESCE(r.effective_date,t.effective_date) AS effective_date,
            COALESCE(r.filed_date,t.filed_date) AS filed_date,
            COALESCE(r.form,t.form) AS form,
            r.tag AS rate_tag, r.version AS rate_version, r.raw_value AS rate_raw_value,
            r.declared_unit AS declared_unit, r.raw_row_id AS rate_raw_row_id,
            (r.raw_row_id IS NOT NULL) AS numeric_present,
            rr1_numeric_value(r.raw_value) AS rate_value,
            t.tag AS text_tag, t.version AS text_version, t.raw_row_id AS text_raw_row_id,
            (t.raw_row_id IS NOT NULL) AS text_present,
            t.text_value AS text_value,
            char_length(t.text_value) AS text_length,
            md5(COALESCE(t.text_value,'')) AS text_md5,
            rr1_number_in_text(r.raw_value, t.text_value) AS number_in_text
        FROM rate_leg r
        FULL OUTER JOIN text_leg t USING
            (ingestion_run_id,accession_number,series_id,class_id,data_date,measure_id,document_id,dimensions,occurrence,effective_date,filed_date,form)
    )
    INSERT INTO rr1_turnover_profiles(
        publication_id,source_run_id,accession_number,series_id,class_id,data_date,measure_id,document_id,dimensions,occurrence,
        effective_date,filed_date,form,turnover_rate,declared_unit,original_tag,original_version,
        turnover_numeric_present,turnover_text_present,narrative_consistency,status,reason_code,provenance,coverage
    )
    SELECT target_publication_id,c.ingestion_run_id,c.accession_number,c.series_id,c.class_id,c.data_date,c.measure_id,
           c.document_id,c.dimensions,c.occurrence,c.effective_date,c.filed_date,c.form,c.rate_value,
           CASE WHEN c.numeric_present THEN c.declared_unit END,
           c.rate_tag, c.rate_version, c.numeric_present, c.text_present,
           CASE WHEN c.numeric_present AND c.text_present AND COALESCE(c.number_in_text,false) THEN 'corroborated'
                WHEN c.numeric_present AND c.text_present THEN 'unreconciled'
                WHEN c.numeric_present THEN 'number_only'
                ELSE 'text_only' END,
           CASE WHEN c.rate_value IS NOT NULL THEN 'available' ELSE 'degraded' END,
           CASE WHEN c.rate_value IS NOT NULL THEN NULL
                WHEN c.numeric_present THEN 'turnover_rate_non_numeric'
                ELSE 'turnover_rate_not_reported' END,
           jsonb_build_object('effective_selection_view','rr1_effective_facts',
                              'source_run_id',c.ingestion_run_id,'accession_number',c.accession_number,
                              'rate_raw_row_id',c.rate_raw_row_id,'rate_tag',c.rate_tag,'rate_version',c.rate_version,
                              'text_raw_row_id',c.text_raw_row_id,'text_tag',c.text_tag,'text_version',c.text_version,
                              'text_block_md5',CASE WHEN c.text_present THEN c.text_md5 END,
                              'text_block_length',CASE WHEN c.text_present THEN c.text_length END,
                              'number_in_text',c.number_in_text),
           jsonb_build_object('source_applicable',true,'as_of_date',as_of_date,'unit_class','fraction',
                              'turnover_numeric_present',c.numeric_present,'turnover_text_present',c.text_present,
                              'context_preserved',true)
    FROM combined c
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_rr1_turnover_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN rr1_turnover_profiles p ON p.publication_id = c.publication_id
WHERE c.product = 'rr1_turnover_profile_v1';
