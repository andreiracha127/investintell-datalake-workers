-- Immutable RR1 declared-benchmark snapshot.  Grain is one row per (series,
-- class) that declares a benchmark index inside the amendment-aware effective
-- selection; in practice ``class_id`` is always empty because the declaration is
-- a series-level disclosure (see below).
--
-- HOW A BENCHMARK IS IDENTIFIED IN THE SOURCE (verified against the live corpus,
-- not assumed).  The RR taxonomy carries the Average Annual Total Return in the
-- four ``AverageAnnualReturn*`` num.tsv elements (Year01 / Year05 / Year10 /
-- SinceInception).  Per the frozen source-table contract the Performance Measure
-- axis (num.tsv ``measure``) distinguishes Before Taxes (the EMPTY member), After
-- Taxes on Distributions, After Taxes on Distributions and Sales, or "a pre-tax
-- measure of returns based on a broadly available market index".  The RR taxonomy
-- defines NO standard member for that last state: the preparer NAMES the index on
-- the axis itself, so the member IS the benchmark identifier and it is preserved
-- VERBATIM (e.g. ``SP500Index``, ``IndexLB001``, ``Russell``).  The fund's own
-- return carries an empty member; the index leg is disclosed at SERIES level (the
-- benchmark is a property of the series, not of a share class), which is exactly
-- where the axis member is observed to be free of non-index members.
--
-- The snapshot reports WHICH benchmark(s) a series declares and whether the SAME
-- benchmark is declared across the different documents / periods of that series --
-- a consistency FLAG with NO judgement (a fund is free to change or carry multiple
-- benchmarks; we only report the fact).  The member is NEVER normalised to a
-- canonical index name here: resolving 1.7k filer-defined member spellings to an
-- index identity is a governed crosswalk concern, not a snapshot concern.  The
-- builder consumes only the effective selection; it never re-opens raw RR1.

-- Frozen RR taxonomy local names carrying an Average Annual Total Return.  A
-- custom/unmapped tag can never resolve here (Global Constraint 5).  NOTE:
-- ``AvgAnnlRtrPct`` is the OEF (Tailored Shareholder Report) element and lives
-- only under the ``oef/*`` namespace -- it is NOT the RR element and must never
-- be selected under an ``rr/%`` version filter.
CREATE OR REPLACE FUNCTION rr1_benchmark_concept_map()
RETURNS TABLE(original_tag text, source_table text)
LANGUAGE sql IMMUTABLE AS $$
    VALUES ('AverageAnnualReturnYear01', 'num.tsv'),
           ('AverageAnnualReturnYear05', 'num.tsv'),
           ('AverageAnnualReturnYear10', 'num.tsv'),
           ('AverageAnnualReturnSinceInception', 'num.tsv')
$$;

CREATE TABLE IF NOT EXISTS rr1_benchmark_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    series_id text NOT NULL,
    class_id text NOT NULL DEFAULT '',
    latest_accession_number text NOT NULL,
    latest_effective_date date NOT NULL,
    latest_filed_date date NOT NULL,
    form text NOT NULL,
    declared_benchmark_count integer NOT NULL CHECK (declared_benchmark_count >= 0),
    observation_count integer NOT NULL CHECK (observation_count >= 1),
    context_count integer NOT NULL CHECK (context_count >= 1),
    document_count integer NOT NULL CHECK (document_count >= 1),
    period_count integer NOT NULL CHECK (period_count >= 1),
    primary_benchmark text,
    benchmark_consistency text NOT NULL CHECK (benchmark_consistency IN (
        'consistent', 'multiple_declared', 'single_observation'
    )),
    status text NOT NULL CHECK (status IN ('available', 'degraded', 'unavailable', 'not_applicable')),
    reason_code text,
    methodology_version text NOT NULL DEFAULT 'rr1_benchmark_profile_v1',
    per_benchmark_evidence jsonb NOT NULL,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, series_id, class_id),
    CHECK (methodology_version = 'rr1_benchmark_profile_v1'),
    CHECK (jsonb_typeof(per_benchmark_evidence) = 'array'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    -- A named benchmark exists iff a naming dimension was declared; a primary
    -- benchmark is set exactly when a single named benchmark was declared.
    CHECK ((primary_benchmark IS NOT NULL) = (declared_benchmark_count = 1)),
    -- available iff at least one NAMED benchmark; an unnamed-only declaration is
    -- degraded (present but the dimension names no index) -- never fabricated.
    CHECK ((status = 'available') = (declared_benchmark_count >= 1)),
    CHECK ((status = 'available') = (reason_code IS NULL)),
    -- Consistency is a total function of the distinct-benchmark count and the
    -- number of distinct (document, period) contexts.
    CHECK ((benchmark_consistency = 'multiple_declared') = (declared_benchmark_count >= 2)),
    CHECK ((benchmark_consistency = 'consistent') = (declared_benchmark_count = 1 AND context_count >= 2)),
    CHECK ((benchmark_consistency = 'single_observation') =
           (declared_benchmark_count = 0 OR (declared_benchmark_count = 1 AND context_count = 1)))
);

CREATE TABLE IF NOT EXISTS rr1_benchmark_profile_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION rr1_benchmark_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 benchmark build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_benchmark_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 benchmark build identity requires a prepared benchmark publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_benchmark_build_guard ON rr1_benchmark_profile_builds;
CREATE TRIGGER rr1_benchmark_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_benchmark_profile_builds
FOR EACH ROW EXECUTE FUNCTION rr1_benchmark_build_guard();

CREATE OR REPLACE FUNCTION rr1_benchmark_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 benchmark profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_benchmark_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 benchmark profile requires a prepared benchmark publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM rr1_benchmark_profile_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS NULL THEN
        RAISE EXCEPTION 'RR1 benchmark profile requires pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_benchmark_write_guard ON rr1_benchmark_profiles;
CREATE TRIGGER rr1_benchmark_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_benchmark_profiles
FOR EACH ROW EXECUTE FUNCTION rr1_benchmark_write_guard();

CREATE OR REPLACE FUNCTION build_rr1_benchmark_profiles(
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
        RAISE EXCEPTION 'RR1 benchmark build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'rr1_benchmark_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 benchmark build requires a prepared benchmark publication';
    END IF;

    -- Canonical inputs: RR-namespaced Average Annual Total Return facts whose
    -- Performance Measure axis NAMES an index (a non-empty member) at SERIES level.
    -- A custom/unmapped tag can never match (Global Constraint 5).
    WITH selected AS (
        SELECT f.*
        FROM rr1_effective_facts f
        JOIN rr1_benchmark_concept_map() m
          ON m.original_tag = f.tag AND m.source_table = f.source_table
        WHERE f.version LIKE 'rr/%'
          AND f.effective_date <= as_of_date
          AND NULLIF(btrim(f.class_id), '') IS NULL
          AND NULLIF(btrim(f.measure_id), '') IS NOT NULL
    )
    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || tag || ':' || version || ':' ||
                data_date::text || ':' || COALESCE(series_id,'') || ':' || COALESCE(class_id,'') || ':' || COALESCE(measure_id,'') || ':' ||
                COALESCE(document_id,'') || ':' || COALESCE(dimensions,'') || ':' || COALESCE(occurrence,'') || ':' ||
                effective_date::text || ':' || filed_date::text || ':' || fact_typed_projection::text,
                '|' ORDER BY ingestion_run_id, accession_number, tag, version, data_date,
                             series_id, class_id, measure_id, document_id, dimensions, occurrence, effective_date, filed_date, raw_row_id
           ), '')) || md5('rr1_benchmark_profile_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM selected;

    INSERT INTO rr1_benchmark_profile_builds(publication_id,input_fingerprint,as_of_date,effective_input_count)
    VALUES(target_publication_id,computed_fingerprint,as_of_date,selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM rr1_benchmark_profile_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'RR1 benchmark build is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'RR1 benchmark build is already pinned to effective-input fingerprint';
    END IF;

    -- Series identity is mandatory (a benchmark is a property of the SERIES, so the
    -- index leg is disclosed with an empty class).  The per-(series,class) roll-up
    -- itself prevents row multiplication: multiple horizons of one benchmark are
    -- counted, never fanned out into duplicate rows.
    WITH selected AS (
        SELECT f.* FROM rr1_effective_facts f
        JOIN rr1_benchmark_concept_map() m
          ON m.original_tag = f.tag AND m.source_table = f.source_table
        WHERE f.version LIKE 'rr/%'
          AND f.effective_date <= as_of_date
          AND NULLIF(btrim(f.class_id), '') IS NULL
          AND NULLIF(btrim(f.measure_id), '') IS NOT NULL
    )
    SELECT CASE WHEN EXISTS (SELECT 1 FROM selected WHERE NULLIF(btrim(series_id),'') IS NULL)
                THEN 'missing RR1 series identity' END
      INTO parent_state;
    IF parent_state IS NOT NULL THEN RAISE EXCEPTION '%', parent_state; END IF;

    WITH selected AS (
        -- The Performance Measure member IS the benchmark identifier, preserved
        -- verbatim: the RR taxonomy has no standard index member, the preparer
        -- names the index on the axis (``otherdims`` carries no index name -- it is
        -- populated on ~0.1% of these facts and never with an index).
        SELECT f.*, NULLIF(btrim(f.measure_id),'') AS benchmark_identifier
        FROM rr1_effective_facts f
        JOIN rr1_benchmark_concept_map() m
          ON m.original_tag = f.tag AND m.source_table = f.source_table
        WHERE f.version LIKE 'rr/%'
          AND f.effective_date <= as_of_date
          AND NULLIF(btrim(f.class_id), '') IS NULL
          AND NULLIF(btrim(f.measure_id), '') IS NOT NULL
    ), per_bench AS (
        SELECT ingestion_run_id, series_id, COALESCE(class_id,'') AS class_id, benchmark_identifier,
               count(*)::integer AS observation_count,
               count(DISTINCT COALESCE(document_id,''))::integer AS document_count,
               count(DISTINCT data_date)::integer AS period_count
        FROM selected
        WHERE benchmark_identifier IS NOT NULL
        GROUP BY ingestion_run_id, series_id, COALESCE(class_id,''), benchmark_identifier
    ), per_bench_agg AS (
        SELECT ingestion_run_id, series_id, class_id,
               jsonb_agg(jsonb_build_object(
                   'benchmark_identifier', benchmark_identifier,
                   'observation_count', observation_count,
                   'document_count', document_count,
                   'period_count', period_count
               ) ORDER BY benchmark_identifier) AS evidence
        FROM per_bench
        GROUP BY ingestion_run_id, series_id, class_id
    ), latest AS (
        SELECT DISTINCT ON (ingestion_run_id, series_id, COALESCE(class_id,''))
               ingestion_run_id, series_id, COALESCE(class_id,'') AS class_id,
               accession_number, effective_date, filed_date, form
        FROM selected
        ORDER BY ingestion_run_id, series_id, COALESCE(class_id,''),
                 effective_date DESC, filed_date DESC, accession_number DESC
    ), sc AS (
        SELECT ingestion_run_id, series_id, COALESCE(class_id,'') AS class_id,
               count(*)::integer AS observation_count,
               count(DISTINCT benchmark_identifier) FILTER (WHERE benchmark_identifier IS NOT NULL)::integer AS declared_benchmark_count,
               count(DISTINCT (COALESCE(document_id,''), data_date))::integer AS context_count,
               count(DISTINCT COALESCE(document_id,''))::integer AS document_count,
               count(DISTINCT data_date)::integer AS period_count,
               min(benchmark_identifier) FILTER (WHERE benchmark_identifier IS NOT NULL) AS any_named_identifier
        FROM selected
        GROUP BY ingestion_run_id, series_id, COALESCE(class_id,'')
    )
    INSERT INTO rr1_benchmark_profiles(
        publication_id,source_run_id,series_id,class_id,latest_accession_number,latest_effective_date,latest_filed_date,form,
        declared_benchmark_count,observation_count,context_count,document_count,period_count,primary_benchmark,
        benchmark_consistency,status,reason_code,per_benchmark_evidence,provenance,coverage
    )
    SELECT target_publication_id, sc.ingestion_run_id, sc.series_id, sc.class_id,
           l.accession_number, l.effective_date, l.filed_date, l.form,
           sc.declared_benchmark_count, sc.observation_count, sc.context_count, sc.document_count, sc.period_count,
           CASE WHEN sc.declared_benchmark_count = 1 THEN sc.any_named_identifier END,
           CASE WHEN sc.declared_benchmark_count >= 2 THEN 'multiple_declared'
                WHEN sc.declared_benchmark_count = 1 AND sc.context_count >= 2 THEN 'consistent'
                ELSE 'single_observation' END,
           CASE WHEN sc.declared_benchmark_count >= 1 THEN 'available' ELSE 'degraded' END,
           CASE WHEN sc.declared_benchmark_count >= 1 THEN NULL ELSE 'benchmark_dimension_unnamed' END,
           COALESCE(pba.evidence, '[]'::jsonb),
           jsonb_build_object('effective_selection_view','rr1_effective_facts',
                              'original_tags',(SELECT jsonb_agg(m.original_tag ORDER BY m.original_tag)
                                               FROM rr1_benchmark_concept_map() m),
                              'benchmark_signal','performance_measure_named_member_series_level',
                              'source_run_id',sc.ingestion_run_id,'latest_accession_number',l.accession_number),
           jsonb_build_object('source_applicable',true,'as_of_date',as_of_date,
                              'declared_benchmark_count',sc.declared_benchmark_count,'context_count',sc.context_count,
                              'document_count',sc.document_count,'period_count',sc.period_count,'context_preserved',true)
    FROM sc
    JOIN latest l USING (ingestion_run_id, series_id, class_id)
    LEFT JOIN per_bench_agg pba USING (ingestion_run_id, series_id, class_id)
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_rr1_benchmark_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN rr1_benchmark_profiles p ON p.publication_id = c.publication_id
WHERE c.product = 'rr1_benchmark_profile_v1';
