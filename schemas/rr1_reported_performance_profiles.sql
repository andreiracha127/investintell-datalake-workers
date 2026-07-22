-- Immutable RR1 reported-performance snapshot.  This is the standardised
-- *prospectus* performance a fund discloses in its Risk/Return Summary -- the
-- Average Annual Total Return per class / horizon / tax-load treatment, the
-- since-inception date, the best / worst calendar quarter, and the year-to-date
-- return.  It is a STANDALONE product: it is never merged with realized NAV /
-- price-return performance (a different measurement object built elsewhere).
--
-- Grain is one row per selected effective fact at its preserved context
-- (publication, source run, accession, series, class, measure = the Performance
-- Measure / tax-load axis, document, dimensions = otherdims carrying the horizon
-- and any declared benchmark member, occurrence).  The horizon and benchmark
-- dimensions are preserved VERBATIM in ``dimensions``; the tax/load treatment is
-- additionally classified (rr1_performance_measure_treatment) while ``measure_id``
-- is kept verbatim.  Absence is open-world: a concept a fund did not disclose
-- simply has no row -- never a synthetic zero.  The builder consumes only the
-- amendment-aware effective selection; it never re-opens raw RR1.

-- Frozen taxonomy local names for the reported-performance family.  ``AvgAnnlRtrPct``
-- is the mandated Average Annual Total Return element; the companions are the RR
-- best/worst-quarter, year-to-date and since-inception elements.  ``value_kind``
-- fixes how each concept's value leg is typed.  A custom/unmapped tag can never
-- resolve here (Global Constraint 5).
CREATE OR REPLACE FUNCTION rr1_reported_performance_concept_map()
RETURNS TABLE(canonical_concept text, original_tag text, source_table text, value_kind text)
LANGUAGE sql IMMUTABLE AS $$
    VALUES ('avg_annual_return', 'AvgAnnlRtrPct', 'num.tsv', 'numeric'),
           ('best_quarter_return', 'HighestQuarterlyReturn', 'num.tsv', 'numeric'),
           ('worst_quarter_return', 'LowestQuarterlyReturn', 'num.tsv', 'numeric'),
           ('year_to_date_return', 'YearToDateReturn', 'num.tsv', 'numeric'),
           ('since_inception_date', 'AverageAnnualReturnInceptionDate', 'txt.tsv', 'date'),
           ('best_quarter_period', 'BarChartHighestQuarterlyReturnLabel', 'txt.tsv', 'label'),
           ('worst_quarter_period', 'BarChartLowestQuarterlyReturnLabel', 'txt.tsv', 'label'),
           ('year_to_date_period', 'BarChartYearToDateReturnLabel', 'txt.tsv', 'label')
$$;

CREATE TABLE IF NOT EXISTS rr1_reported_performance_profiles (
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
    canonical_concept text NOT NULL CHECK (canonical_concept IN (
        'avg_annual_return', 'best_quarter_return', 'worst_quarter_return', 'year_to_date_return',
        'since_inception_date', 'best_quarter_period', 'worst_quarter_period', 'year_to_date_period'
    )),
    value_kind text NOT NULL CHECK (value_kind IN ('numeric', 'date', 'label')),
    original_tag text NOT NULL,
    original_version text NOT NULL,
    value_numeric numeric,
    value_date date,
    value_label text,
    declared_unit text,
    treatment text NOT NULL CHECK (treatment IN (
        'before_taxes', 'after_tax_distributions', 'after_tax_distributions_and_sales',
        'broad_market_index', 'unclassified'
    )),
    status text NOT NULL CHECK (status IN ('available', 'degraded', 'unavailable', 'not_applicable')),
    reason_code text,
    methodology_version text NOT NULL DEFAULT 'rr1_reported_performance_profile_v1',
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, series_id, class_id, data_date,
                 measure_id, document_id, dimensions, occurrence, canonical_concept),
    CHECK (methodology_version = 'rr1_reported_performance_profile_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    -- Exactly the value leg for the concept's kind may be populated.
    CHECK (value_kind <> 'numeric' OR (value_date IS NULL AND value_label IS NULL)),
    CHECK (value_kind <> 'date' OR (value_numeric IS NULL AND value_label IS NULL)),
    CHECK (value_kind <> 'label' OR (value_numeric IS NULL AND value_date IS NULL)),
    -- A declared unit only accompanies a numeric leg.
    CHECK (declared_unit IS NULL OR value_kind = 'numeric'),
    -- Availability is the concept's typed value being present; a missing/untyped
    -- value is degraded, never a fabricated value.
    CHECK ((status = 'available') = (
        (value_kind = 'numeric' AND value_numeric IS NOT NULL) OR
        (value_kind = 'date' AND value_date IS NOT NULL) OR
        (value_kind = 'label' AND value_label IS NOT NULL)
    )),
    CHECK ((status = 'available') = (reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS rr1_reported_performance_profile_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION rr1_reported_performance_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 reported-performance build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_reported_performance_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 reported-performance build identity requires a prepared reported-performance publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_reported_performance_build_guard ON rr1_reported_performance_profile_builds;
CREATE TRIGGER rr1_reported_performance_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_reported_performance_profile_builds
FOR EACH ROW EXECUTE FUNCTION rr1_reported_performance_build_guard();

CREATE OR REPLACE FUNCTION rr1_reported_performance_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 reported-performance profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_reported_performance_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 reported-performance profile requires a prepared reported-performance publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM rr1_reported_performance_profile_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS NULL THEN
        RAISE EXCEPTION 'RR1 reported-performance profile requires pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_reported_performance_write_guard ON rr1_reported_performance_profiles;
CREATE TRIGGER rr1_reported_performance_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_reported_performance_profiles
FOR EACH ROW EXECUTE FUNCTION rr1_reported_performance_write_guard();

CREATE OR REPLACE FUNCTION build_rr1_reported_performance_profiles(
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
        RAISE EXCEPTION 'RR1 reported-performance build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'rr1_reported_performance_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 reported-performance build requires a prepared reported-performance publication';
    END IF;

    -- Canonical inputs: only RR-namespaced (``rr/%``) facts whose exact table+tag
    -- is in the frozen concept map.  A custom/unmapped tag can never match.
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_reported_performance_concept_map() m
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
           ), '')) || md5('rr1_reported_performance_profile_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM selected;

    INSERT INTO rr1_reported_performance_profile_builds(publication_id,input_fingerprint,as_of_date,effective_input_count)
    VALUES(target_publication_id,computed_fingerprint,as_of_date,selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM rr1_reported_performance_profile_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'RR1 reported-performance build is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'RR1 reported-performance build is already pinned to effective-input fingerprint';
    END IF;

    -- Fan-out guard: one concept resolves to at most one fact per preserved context
    -- grain.  Series identity is mandatory; class may be empty (a benchmark index
    -- return is disclosed at series level).  A duplicated fact is a hard failure.
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_reported_performance_concept_map() m
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
        ) THEN 'conflicting RR1 reported-performance facts'
    END INTO parent_state;
    IF parent_state IS NOT NULL THEN RAISE EXCEPTION '%', parent_state; END IF;

    WITH selected AS (
        SELECT f.*, m.canonical_concept, m.value_kind,
               NULLIF(btrim(f.fact_typed_projection->>'value'),'') AS raw_value,
               NULLIF(btrim(f.fact_typed_projection->>'uom'),'') AS declared_unit
        FROM rr1_effective_facts f JOIN rr1_reported_performance_concept_map() m
          ON m.original_tag = f.tag AND m.source_table = f.source_table
        WHERE f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    ), typed AS (
        SELECT s.*,
               CASE WHEN s.value_kind = 'numeric' THEN rr1_numeric_value(s.raw_value) END AS value_numeric,
               CASE WHEN s.value_kind = 'date' THEN rr1_safe_iso_date(s.raw_value) END AS value_date,
               CASE WHEN s.value_kind = 'label' THEN s.raw_value END AS value_label
        FROM selected s
    )
    INSERT INTO rr1_reported_performance_profiles(
        publication_id,source_run_id,accession_number,series_id,class_id,data_date,measure_id,document_id,dimensions,occurrence,
        effective_date,filed_date,form,canonical_concept,value_kind,original_tag,original_version,value_numeric,value_date,value_label,
        declared_unit,treatment,status,reason_code,provenance,coverage
    )
    SELECT target_publication_id,t.ingestion_run_id,t.accession_number,t.series_id,COALESCE(t.class_id,''),t.data_date,
           COALESCE(t.measure_id,''),COALESCE(t.document_id,''),COALESCE(t.dimensions,''),COALESCE(t.occurrence,''),
           t.effective_date,t.filed_date,t.form,t.canonical_concept,t.value_kind,t.tag,t.version,
           t.value_numeric,t.value_date,t.value_label,
           CASE WHEN t.value_kind = 'numeric' THEN t.declared_unit END,
           rr1_performance_measure_treatment(t.measure_id),
           CASE WHEN (t.value_kind = 'numeric' AND t.value_numeric IS NOT NULL)
                  OR (t.value_kind = 'date' AND t.value_date IS NOT NULL)
                  OR (t.value_kind = 'label' AND t.value_label IS NOT NULL)
                THEN 'available' ELSE 'degraded' END,
           CASE WHEN (t.value_kind = 'numeric' AND t.value_numeric IS NOT NULL)
                  OR (t.value_kind = 'date' AND t.value_date IS NOT NULL)
                  OR (t.value_kind = 'label' AND t.value_label IS NOT NULL)
                THEN NULL
                WHEN t.value_kind = 'numeric' THEN 'value_non_numeric'
                WHEN t.value_kind = 'date' THEN 'value_unparseable_date'
                ELSE 'value_empty' END,
           jsonb_build_object('effective_selection_view','rr1_effective_facts','effective_raw_row_id',t.raw_row_id,
                              'source_run_id',t.ingestion_run_id,'accession_number',t.accession_number,
                              'original_tag',t.tag,'original_version',t.version),
           jsonb_build_object('source_applicable',true,'as_of_date',as_of_date,'canonical_concept',t.canonical_concept,
                              'value_kind',t.value_kind,'treatment',rr1_performance_measure_treatment(t.measure_id),
                              'measure_member',COALESCE(t.measure_id,''),'dimensions',COALESCE(t.dimensions,''),
                              'context_preserved',true)
    FROM typed t
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_rr1_reported_performance_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN rr1_reported_performance_profiles p ON p.publication_id = c.publication_id
WHERE c.product = 'rr1_reported_performance_profile_v1';
