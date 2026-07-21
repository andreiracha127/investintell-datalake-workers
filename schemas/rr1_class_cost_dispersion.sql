-- Immutable RR1 share-class cost-dispersion snapshot.  Grain is one row per
-- series context (publication, source run, accession, series, data_date): the
-- min / max / spread of the net annual operating expense across the classes of
-- the same series, with per-class evidence retained.  Net expense is the
-- ``uom=pure`` fraction NetExpensesOverAssets; scaling is display-side (Global
-- Constraint 5).  Each class's net fact is pre-aggregated at its own class grain
-- BEFORE the series roll-up, with a row-multiplication hard-failure guard so a
-- duplicated class fact can never double-count a class (Global Constraint 4).
-- The builder consumes only the effective selection; it never re-opens raw RR1.

CREATE TABLE IF NOT EXISTS rr1_class_cost_dispersion (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    series_id text NOT NULL,
    data_date date NOT NULL,
    effective_date date NOT NULL,
    filed_date date NOT NULL,
    form text NOT NULL,
    class_count integer NOT NULL CHECK (class_count >= 0),
    net_min numeric,
    net_max numeric,
    net_spread numeric,
    net_min_class_id text,
    net_max_class_id text,
    status text NOT NULL CHECK (status IN ('available', 'unavailable', 'not_applicable')),
    reason_code text,
    methodology_version text NOT NULL DEFAULT 'rr1_class_cost_dispersion_v1',
    per_class_evidence jsonb NOT NULL,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, series_id, data_date),
    CHECK (methodology_version = 'rr1_class_cost_dispersion_v1'),
    CHECK (jsonb_typeof(per_class_evidence) = 'array'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    -- A dispersion exists only with >=2 numeric classes: available iff a spread
    -- was computed; reason is present exactly when it is not available.
    CHECK ((status = 'available') = (net_spread IS NOT NULL)),
    CHECK ((status = 'available') = (reason_code IS NULL)),
    CHECK ((status = 'unavailable') = (class_count = 0)),
    -- The extrema exist iff at least one class reported a numeric net; a single
    -- class keeps min = max but the spread stays NULL, never a synthetic 0.
    CHECK ((net_min IS NULL) = (class_count = 0)),
    CHECK ((net_min IS NULL) = (net_max IS NULL)),
    CHECK ((net_min_class_id IS NULL) = (net_min IS NULL)),
    CHECK ((net_max_class_id IS NULL) = (net_max IS NULL))
);

CREATE TABLE IF NOT EXISTS rr1_class_cost_dispersion_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION rr1_class_cost_dispersion_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 class-dispersion build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_class_cost_dispersion_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 class-dispersion build identity requires a prepared class-dispersion publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_class_cost_dispersion_build_guard ON rr1_class_cost_dispersion_builds;
CREATE TRIGGER rr1_class_cost_dispersion_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_class_cost_dispersion_builds
FOR EACH ROW EXECUTE FUNCTION rr1_class_cost_dispersion_build_guard();

CREATE OR REPLACE FUNCTION rr1_class_cost_dispersion_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 class-dispersion profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_class_cost_dispersion_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 class-dispersion profile requires a prepared class-dispersion publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM rr1_class_cost_dispersion_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS NULL THEN
        RAISE EXCEPTION 'RR1 class-dispersion profile requires pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_class_cost_dispersion_write_guard ON rr1_class_cost_dispersion;
CREATE TRIGGER rr1_class_cost_dispersion_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_class_cost_dispersion
FOR EACH ROW EXECUTE FUNCTION rr1_class_cost_dispersion_write_guard();

CREATE OR REPLACE FUNCTION build_rr1_class_cost_dispersion(
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
        RAISE EXCEPTION 'RR1 class-dispersion build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'rr1_class_cost_dispersion_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 class-dispersion build requires a prepared class-dispersion publication';
    END IF;

    -- Canonical input: RR-namespaced NetExpensesOverAssets only.
    WITH selected AS (
        SELECT f.*
        FROM rr1_effective_facts f
        WHERE f.source_table = 'num.tsv' AND f.tag = 'NetExpensesOverAssets'
          AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    )
    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || tag || ':' || version || ':' ||
                data_date::text || ':' || COALESCE(series_id,'') || ':' || COALESCE(class_id,'') || ':' || COALESCE(measure_id,'') || ':' ||
                COALESCE(document_id,'') || ':' || COALESCE(dimensions,'') || ':' || COALESCE(occurrence,'') || ':' ||
                effective_date::text || ':' || filed_date::text || ':' || fact_typed_projection::text,
                '|' ORDER BY ingestion_run_id, accession_number, tag, version, data_date,
                             series_id, class_id, measure_id, document_id, dimensions, occurrence, effective_date, filed_date, raw_row_id
           ), '')) || md5('rr1_class_cost_dispersion_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM selected;

    INSERT INTO rr1_class_cost_dispersion_builds(publication_id,input_fingerprint,as_of_date,effective_input_count)
    VALUES(target_publication_id,computed_fingerprint,as_of_date,selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM rr1_class_cost_dispersion_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'RR1 class-dispersion build is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'RR1 class-dispersion build is already pinned to effective-input fingerprint';
    END IF;

    -- Class-grain fan-out guard: at most one net fact per (series, class,
    -- data_date) context, and every net fact must carry series+class identity.
    -- A class reported twice (e.g. under two documents) is a hard failure, never
    -- a double-counted class in the roll-up.
    WITH selected AS (
        SELECT f.*
        FROM rr1_effective_facts f
        WHERE f.source_table = 'num.tsv' AND f.tag = 'NetExpensesOverAssets'
          AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    )
    SELECT CASE
        WHEN EXISTS (SELECT 1 FROM selected WHERE NULLIF(btrim(series_id),'') IS NULL OR NULLIF(btrim(class_id),'') IS NULL)
            THEN 'missing RR1 series or class identity'
        WHEN EXISTS (
            SELECT 1 FROM selected
            GROUP BY ingestion_run_id,accession_number,series_id,class_id,data_date
            HAVING count(*) > 1
        ) THEN 'conflicting RR1 class net-expense facts'
    END INTO parent_state;
    IF parent_state IS NOT NULL THEN RAISE EXCEPTION '%', parent_state; END IF;

    WITH selected AS (
        SELECT f.*, rr1_numeric_value(NULLIF(btrim(f.fact_typed_projection->>'value'),'')) AS net_value
        FROM rr1_effective_facts f
        WHERE f.source_table = 'num.tsv' AND f.tag = 'NetExpensesOverAssets'
          AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    ), series_agg AS (
        SELECT ingestion_run_id, accession_number, series_id, data_date, effective_date, filed_date, form,
               count(net_value)::integer AS numeric_count,
               min(net_value) AS net_min,
               max(net_value) AS net_max,
               (array_agg(class_id ORDER BY net_value ASC, class_id) FILTER (WHERE net_value IS NOT NULL))[1] AS net_min_class_id,
               (array_agg(class_id ORDER BY net_value DESC, class_id) FILTER (WHERE net_value IS NOT NULL))[1] AS net_max_class_id,
               jsonb_agg(jsonb_build_object(
                   'class_id', class_id,
                   'measure_id', COALESCE(measure_id,''),
                   'document_id', COALESCE(document_id,''),
                   'dimensions', COALESCE(dimensions,''),
                   'occurrence', COALESCE(occurrence,''),
                   'net_expense', CASE WHEN net_value IS NOT NULL THEN net_value::text END,
                   'net_state', CASE WHEN net_value IS NOT NULL THEN 'available' ELSE 'degraded' END,
                   'original_tag', tag,
                   'original_version', version,
                   'effective_raw_row_id', raw_row_id
               ) ORDER BY class_id) AS per_class_evidence,
               count(*)::integer AS class_fact_count
        FROM selected
        GROUP BY ingestion_run_id, accession_number, series_id, data_date, effective_date, filed_date, form
    )
    INSERT INTO rr1_class_cost_dispersion(
        publication_id,source_run_id,accession_number,series_id,data_date,effective_date,filed_date,form,
        class_count,net_min,net_max,net_spread,net_min_class_id,net_max_class_id,status,reason_code,
        per_class_evidence,provenance,coverage
    )
    SELECT target_publication_id,a.ingestion_run_id,a.accession_number,a.series_id,a.data_date,a.effective_date,a.filed_date,a.form,
           a.numeric_count,a.net_min,a.net_max,
           CASE WHEN a.numeric_count >= 2 THEN rr1_safe_diff(a.net_max, a.net_min) END,
           a.net_min_class_id,a.net_max_class_id,
           CASE WHEN a.numeric_count >= 2 THEN 'available'
                WHEN a.numeric_count = 1 THEN 'not_applicable'
                ELSE 'unavailable' END,
           CASE WHEN a.numeric_count >= 2 THEN NULL
                WHEN a.numeric_count = 1 THEN 'single_class_series'
                ELSE 'no_numeric_net_expense' END,
           a.per_class_evidence,
           jsonb_build_object('effective_selection_view','rr1_effective_facts','canonical_concept','net_expense',
                              'original_tag','NetExpensesOverAssets','source_run_id',a.ingestion_run_id,
                              'accession_number',a.accession_number),
           jsonb_build_object('source_applicable',true,'as_of_date',as_of_date,'unit_class','fraction',
                              'class_fact_count',a.class_fact_count,'numeric_class_count',a.numeric_count,
                              'context_preserved',true)
    FROM series_agg a
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_rr1_class_cost_dispersion AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN rr1_class_cost_dispersion p ON p.publication_id = c.publication_id
WHERE c.product = 'rr1_class_cost_dispersion_v1';
