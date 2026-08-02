-- Immutable RR1 share-class fee facts.  This builder consumes only the
-- amendment-aware effective selection; it deliberately does not re-open raw RR1.
--
-- Input scope.  A share-class fee fact is only a share-class fee fact if the
-- filer asserted BOTH identities: rr1_fee_profiles is keyed by (series_id,
-- class_id) and both columns are in its primary key, so a fact that carries
-- neither, or only one of them, has no share-class to be a profile of.  RR1
-- num.tsv legitimately also carries fee tags at series (fund) grain -- the same
-- ManagementFeesOverAssets reported once for a single-class fund, with no class
-- dimension.  Those rows are real data at a DIFFERENT grain; they stay in
-- rr1_effective_facts and are simply out of scope for this product rather than
-- being admitted under an invented blank identity.
--
-- The scope predicate therefore lives HERE, in the product's own selection, and
-- is repeated verbatim in every `selected` CTE of build_rr1_fee_profiles and of
-- rr1_fee_profile_build_is_closed.  It must not live only in a caller's session
-- (see src/workers/rr1_derived_profiles.py): a session-local pre-filter makes
-- the builder and the closure guard read two different input sets, and the guard
-- then can never certify from any session but the one that built.

-- Frozen RR taxonomy local names.  This single mapping is used by fingerprinting,
-- conflict detection, profile materialization, and lifecycle validation.
CREATE OR REPLACE FUNCTION rr1_fee_profile_concept_map()
RETURNS TABLE(canonical_concept text, original_tag text)
LANGUAGE sql IMMUTABLE AS $$
    VALUES ('management_fee', 'ManagementFeesOverAssets'),
           ('distribution_12b1', 'DistributionAndService12b1FeesOverAssets'),
           ('acquired_fund_expense', 'AcquiredFundFeesAndExpensesOverAssets'),
           ('other_expense', 'OtherExpensesOverAssets'),
           ('gross_expense', 'ExpensesOverAssets'),
           ('waiver_reimbursement', 'FeeWaiverOrReimbursementOverAssets'),
           ('net_expense', 'NetExpensesOverAssets')
$$;

CREATE TABLE IF NOT EXISTS rr1_fee_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    series_id text NOT NULL,
    class_id text NOT NULL,
    data_date date NOT NULL,
    measure_id text NOT NULL DEFAULT '',
    document_id text NOT NULL DEFAULT '',
    dimensions text NOT NULL DEFAULT '',
    occurrence text NOT NULL DEFAULT '',
    effective_date date NOT NULL,
    filed_date date NOT NULL,
    form text NOT NULL,
    canonical_concept text NOT NULL CHECK (canonical_concept IN (
        'management_fee', 'distribution_12b1', 'acquired_fund_expense', 'other_expense',
        'gross_expense', 'waiver_reimbursement', 'net_expense'
    )),
    original_tag text,
    original_version text,
    value_numeric numeric,
    status text NOT NULL CHECK (status IN ('available', 'degraded', 'unavailable', 'not_applicable')),
    reason_code text,
    methodology_version text NOT NULL DEFAULT 'rr1_fee_profile_v1',
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, series_id, class_id, data_date,
                 measure_id, document_id, dimensions, occurrence, canonical_concept),
    CHECK (methodology_version = 'rr1_fee_profile_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((status = 'available') = (value_numeric IS NOT NULL AND original_tag IS NOT NULL AND original_version IS NOT NULL)),
    CHECK ((status = 'available') = (reason_code IS NULL)),
    CHECK ((status = 'unavailable') = (original_tag IS NULL AND original_version IS NULL AND value_numeric IS NULL))
);

-- Pre-e838e80 installations persisted RR1 iprx as an integer.  iprx is lexical
-- in the effective contract, so upgrade it without collapsing representations
-- such as "01" in all future writes.
DO $$
DECLARE occurrence_type regtype;
BEGIN
    SELECT a.atttypid::regtype INTO occurrence_type
    FROM pg_attribute a
    WHERE a.attrelid = 'rr1_fee_profiles'::regclass
      AND a.attname = 'occurrence'
      AND NOT a.attisdropped;
    IF occurrence_type = 'integer'::regtype THEN
        ALTER TABLE rr1_fee_profiles ALTER COLUMN occurrence DROP DEFAULT;
        ALTER TABLE rr1_fee_profiles ALTER COLUMN occurrence TYPE text USING occurrence::text;
    ELSIF occurrence_type IS DISTINCT FROM 'text'::regtype THEN
        RAISE EXCEPTION 'RR1 fee-profile occurrence must be text, found %', occurrence_type;
    END IF;
END $$;
ALTER TABLE rr1_fee_profiles ALTER COLUMN occurrence SET NOT NULL;
ALTER TABLE rr1_fee_profiles ALTER COLUMN occurrence SET DEFAULT ''::text;

CREATE TABLE IF NOT EXISTS rr1_fee_profile_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION rr1_fee_profile_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 fee-profile build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_fee_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 fee-profile build identity requires a prepared RR1 fee-profile publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_fee_profile_build_guard ON rr1_fee_profile_builds;
CREATE TRIGGER rr1_fee_profile_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_fee_profile_builds
FOR EACH ROW EXECUTE FUNCTION rr1_fee_profile_build_guard();

CREATE OR REPLACE FUNCTION rr1_fee_profile_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 fee profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_fee_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 fee profile requires a prepared RR1 fee-profile publication';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM rr1_fee_profile_builds b WHERE b.publication_id = NEW.publication_id) THEN
        RAISE EXCEPTION 'RR1 fee profile requires pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_fee_profile_write_guard ON rr1_fee_profiles;
CREATE TRIGGER rr1_fee_profile_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_fee_profiles
FOR EACH ROW EXECUTE FUNCTION rr1_fee_profile_write_guard();

CREATE OR REPLACE FUNCTION rr1_fee_profile_build_is_closed(target_publication_id uuid)
RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE
    pinned_fingerprint char(64);
    pinned_as_of date;
    pinned_input_count integer;
    computed_fingerprint char(64);
    computed_input_count integer;
    expected_profile_count integer;
    actual_profile_count integer;
BEGIN
    SELECT b.input_fingerprint,b.as_of_date,b.effective_input_count
      INTO pinned_fingerprint,pinned_as_of,pinned_input_count
    FROM rr1_fee_profile_builds b WHERE b.publication_id=target_publication_id;
    IF NOT FOUND THEN RETURN false; END IF;

    WITH selected AS (
        SELECT f.*,m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_fee_profile_concept_map() m ON m.original_tag=f.tag
        WHERE f.source_table='num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date<=pinned_as_of
          AND NULLIF(btrim(f.series_id),'') IS NOT NULL AND NULLIF(btrim(f.class_id),'') IS NOT NULL
    )
    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || canonical_concept || ':' || tag || ':' || version || ':' ||
                data_date::text || ':' || COALESCE(series_id,'') || ':' || COALESCE(class_id,'') || ':' || COALESCE(measure_id,'') || ':' ||
                COALESCE(document_id,'') || ':' || COALESCE(dimensions,'') || ':' || COALESCE(occurrence,'') || ':' ||
                effective_date::text || ':' || filed_date::text || ':' || fact_typed_projection::text,
                '|' ORDER BY ingestion_run_id,accession_number,canonical_concept,tag,version,data_date,
                             series_id,class_id,measure_id,document_id,dimensions,occurrence,effective_date,filed_date,raw_row_id
           ),'')) || md5('rr1_fee_profile_v1:' || pinned_as_of::text))::char(64)
      INTO computed_input_count,computed_fingerprint
    FROM selected;
    IF computed_input_count IS DISTINCT FROM pinned_input_count OR computed_fingerprint IS DISTINCT FROM pinned_fingerprint THEN
        RETURN false;
    END IF;

    IF EXISTS (
        WITH selected AS (
            SELECT f.*,m.canonical_concept
            FROM rr1_effective_facts f JOIN rr1_fee_profile_concept_map() m ON m.original_tag=f.tag
            WHERE f.source_table='num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date<=pinned_as_of
              AND NULLIF(btrim(f.series_id),'') IS NOT NULL AND NULLIF(btrim(f.class_id),'') IS NOT NULL
        )
        SELECT 1 FROM selected
        GROUP BY ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,''),COALESCE(document_id,''),
                 COALESCE(dimensions,''),COALESCE(occurrence,''),effective_date,filed_date,canonical_concept
        HAVING count(*)>1
    ) THEN RETURN false; END IF;

    WITH selected AS (
        SELECT f.*,m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_fee_profile_concept_map() m ON m.original_tag=f.tag
        WHERE f.source_table='num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date<=pinned_as_of
          AND NULLIF(btrim(f.series_id),'') IS NOT NULL AND NULLIF(btrim(f.class_id),'') IS NOT NULL
    ), contexts AS (
        SELECT DISTINCT ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,'') AS measure_id,
               COALESCE(document_id,'') AS document_id,COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
               effective_date,filed_date,form
        FROM selected
    )
    SELECT count(*)::integer INTO expected_profile_count
    FROM contexts CROSS JOIN rr1_fee_profile_concept_map();
    SELECT count(*)::integer INTO actual_profile_count
    FROM rr1_fee_profiles p WHERE p.publication_id=target_publication_id;
    IF expected_profile_count IS DISTINCT FROM actual_profile_count THEN RETURN false; END IF;

    IF EXISTS (
        WITH selected AS (
            SELECT f.*,m.canonical_concept
            FROM rr1_effective_facts f JOIN rr1_fee_profile_concept_map() m ON m.original_tag=f.tag
            WHERE f.source_table='num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date<=pinned_as_of
              AND NULLIF(btrim(f.series_id),'') IS NOT NULL AND NULLIF(btrim(f.class_id),'') IS NOT NULL
        ), contexts AS (
            SELECT DISTINCT ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,'') AS measure_id,
                   COALESCE(document_id,'') AS document_id,COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
                   effective_date,filed_date,form
            FROM selected
        ), expected AS (
            SELECT c.*,m.canonical_concept FROM contexts c CROSS JOIN rr1_fee_profile_concept_map() m
        ), actual AS (
            SELECT p.source_run_id AS ingestion_run_id,p.accession_number,p.series_id,p.class_id,p.data_date,p.measure_id,p.document_id,
                   p.dimensions,p.occurrence,p.effective_date,p.filed_date,p.form,p.canonical_concept
            FROM rr1_fee_profiles p WHERE p.publication_id=target_publication_id
        )
        (SELECT * FROM expected EXCEPT SELECT * FROM actual)
        UNION ALL
        (SELECT * FROM actual EXCEPT SELECT * FROM expected)
    ) THEN RETURN false; END IF;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION rr1_fee_profile_publication_validation_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.product='rr1_fee_profile_v1' AND OLD.lifecycle_state='prepared' AND NEW.lifecycle_state='validated'
       AND NOT rr1_fee_profile_build_is_closed(NEW.publication_id) THEN
        RAISE EXCEPTION 'RR1 fee-profile validation requires a pinned build with closed invariants';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_fee_profile_publication_validation_guard ON sec_derived_publications;
CREATE TRIGGER rr1_fee_profile_publication_validation_guard
BEFORE UPDATE ON sec_derived_publications
FOR EACH ROW EXECUTE FUNCTION rr1_fee_profile_publication_validation_guard();

CREATE OR REPLACE FUNCTION rr1_fee_profile_current_pointer_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'DELETE' AND NEW.product='rr1_fee_profile_v1'
       AND NOT rr1_fee_profile_build_is_closed(NEW.publication_id) THEN
        RAISE EXCEPTION 'RR1 fee-profile current pointer requires a closed pinned build';
    END IF;
    RETURN COALESCE(NEW,OLD);
END $$;

DROP TRIGGER IF EXISTS rr1_fee_profile_current_pointer_guard ON sec_derived_current_pointers;
CREATE TRIGGER rr1_fee_profile_current_pointer_guard
BEFORE INSERT OR UPDATE OR DELETE ON sec_derived_current_pointers
FOR EACH ROW EXECUTE FUNCTION rr1_fee_profile_current_pointer_guard();

CREATE OR REPLACE FUNCTION build_rr1_fee_profiles(
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
        RAISE EXCEPTION 'RR1 fee-profile build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'rr1_fee_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 fee-profile build requires a prepared RR1 fee-profile publication';
    END IF;

    -- These are RR taxonomy local names, not sample-derived aliases.  The original
    -- tag and taxonomy version remain sidecar evidence on every produced fact.
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_fee_profile_concept_map() m ON m.original_tag = f.tag
        WHERE f.source_table = 'num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
          AND NULLIF(btrim(f.series_id),'') IS NOT NULL AND NULLIF(btrim(f.class_id),'') IS NOT NULL
    )
    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || canonical_concept || ':' || tag || ':' || version || ':' ||
                data_date::text || ':' || COALESCE(series_id,'') || ':' || COALESCE(class_id,'') || ':' || COALESCE(measure_id,'') || ':' ||
                COALESCE(document_id,'') || ':' || COALESCE(dimensions,'') || ':' || COALESCE(occurrence,'') || ':' ||
                effective_date::text || ':' || filed_date::text || ':' || fact_typed_projection::text,
                '|' ORDER BY ingestion_run_id, accession_number, canonical_concept, tag, version, data_date,
                             series_id, class_id, measure_id, document_id, dimensions, occurrence, effective_date, filed_date, raw_row_id
           ), '')) || md5('rr1_fee_profile_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM selected;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM rr1_fee_profile_builds b WHERE b.publication_id = target_publication_id;
    IF FOUND THEN
        IF pinned_as_of IS DISTINCT FROM as_of_date THEN
            RAISE EXCEPTION 'RR1 fee-profile build is already pinned to as_of_date %', pinned_as_of;
        END IF;
        IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
            RAISE EXCEPTION 'RR1 fee-profile build is already pinned to effective-input fingerprint';
        END IF;
    ELSE
        INSERT INTO rr1_fee_profile_builds(publication_id,input_fingerprint,as_of_date,effective_input_count)
        VALUES(target_publication_id,computed_fingerprint,as_of_date,selected_count);
    END IF;

    -- Missing series/class identity is not an error to fail closed on: it is the
    -- boundary of this product's grain and is excluded by `selected` below.  What
    -- remains fail-closed is a genuine contradiction: two effective facts for the
    -- same share-class context and canonical concept.
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_fee_profile_concept_map() m ON m.original_tag = f.tag
        WHERE f.source_table = 'num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
          AND NULLIF(btrim(f.series_id),'') IS NOT NULL AND NULLIF(btrim(f.class_id),'') IS NOT NULL
    )
    SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM selected
            GROUP BY ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,''),COALESCE(document_id,''),
                     COALESCE(dimensions,''),COALESCE(occurrence,''),effective_date,filed_date,canonical_concept
            HAVING count(*) > 1
        ) THEN 'conflicting RR1 fee facts'
    END INTO parent_state;
    IF parent_state IS NOT NULL THEN RAISE EXCEPTION '%', parent_state; END IF;

    WITH mapped AS (
        SELECT canonical_concept, original_tag FROM rr1_fee_profile_concept_map()
    ), selected AS (
        SELECT f.*, m.canonical_concept, NULLIF(btrim(f.fact_typed_projection->>'value'),'') AS raw_value
        FROM rr1_effective_facts f JOIN mapped m ON m.original_tag = f.tag
        WHERE f.source_table = 'num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
          AND NULLIF(btrim(f.series_id),'') IS NOT NULL AND NULLIF(btrim(f.class_id),'') IS NOT NULL
    ), contexts AS (
        SELECT DISTINCT ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,'') AS measure_id,
               COALESCE(document_id,'') AS document_id,COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
               effective_date,filed_date,form
        FROM selected
    )
    INSERT INTO rr1_fee_profiles(
        publication_id,source_run_id,accession_number,series_id,class_id,data_date,measure_id,document_id,dimensions,occurrence,
        effective_date,filed_date,form,canonical_concept,original_tag,original_version,value_numeric,status,reason_code,
        provenance,coverage
    )
    SELECT target_publication_id,c.ingestion_run_id,c.accession_number,c.series_id,c.class_id,c.data_date,c.measure_id,c.document_id,
           c.dimensions,c.occurrence,c.effective_date,c.filed_date,c.form,m.canonical_concept,
           s.tag,s.version,
           CASE WHEN s.raw_value ~ '^[-+]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN s.raw_value::numeric END,
           CASE WHEN s.tag IS NULL THEN 'unavailable'
                WHEN s.raw_value ~ '^[-+]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN 'available'
                ELSE 'degraded' END,
           CASE WHEN s.tag IS NULL THEN 'concept_not_reported'
                WHEN s.raw_value IS NULL OR s.raw_value !~ '^[-+]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$' THEN 'selected_fact_has_no_numeric_value'
                ELSE NULL END,
           jsonb_build_object('effective_selection_view','rr1_effective_facts','effective_raw_row_id',s.raw_row_id,
                              'source_run_id',c.ingestion_run_id,'accession_number',c.accession_number,
                              'original_tag',s.tag,'original_version',s.version),
           jsonb_build_object('source_applicable',true,'as_of_date',as_of_date,'canonical_concept',m.canonical_concept,
                              'context_preserved',true)
    FROM contexts c CROSS JOIN mapped m
    LEFT JOIN selected s ON s.canonical_concept=m.canonical_concept
                       AND s.ingestion_run_id=c.ingestion_run_id AND s.accession_number=c.accession_number
                       AND s.series_id=c.series_id AND s.class_id=c.class_id AND s.data_date=c.data_date
                       AND COALESCE(s.measure_id,'')=c.measure_id AND COALESCE(s.document_id,'')=c.document_id
                       AND COALESCE(s.dimensions,'')=c.dimensions AND COALESCE(s.occurrence,'')=c.occurrence
                       AND s.effective_date=c.effective_date AND s.filed_date=c.filed_date
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_rr1_fee_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN rr1_fee_profiles p ON p.publication_id = c.publication_id
WHERE c.product = 'rr1_fee_profile_v1';

-- ---------------------------------------------------------------------------
-- CONTEXT-GRAIN SEMANTICS (measured, 2026-08-02)
-- ---------------------------------------------------------------------------
-- The product's grain is the RR1 fact CONTEXT, not the share class: the primary
-- key carries measure_id / document_id / dimensions / occurrence, and the builder
-- writes the full 7-concept grid for EVERY context (a concept absent from a
-- context becomes an `unavailable` row with reason `concept_not_reported`).
--
-- A consumer that wants "the management fee of this class" therefore sees SEVERAL
-- rows for one (series, class, concept) and has to choose.  These comments say
-- what each context column means so the choice is made on semantics rather than
-- on an arbitrary ORDER BY.  Measured on the mirror over the current publication
-- (2 313 955 rows = 330 565 contexts x 7 concepts); of the 12 271 groups where one
-- (accession, series, class, concept, effective_date, filed_date) carries more
-- than one row, the varying column is:
--
--   document_id  7 154 groups   162 disagree on the value
--   occurrence   2 821 groups     0 disagree on the value
--   data_date    2 191 groups    50 disagree on the value
--   dimensions      98 groups    42 disagree on the value
--   measure_id       7 groups     3 disagree on the value
--   (no group varies on more than one of them)
--
-- The recommended consumer rule, in this order: latest data_date, then
-- status IN ('available','degraded') before 'unavailable', then the LOWEST
-- occurrence.  Ranking on occurrence FIRST is the documented trap: at a context
-- whose occurrence exists only because a DIFFERENT concept was repeated, every
-- other concept has nothing but its `unavailable` filler, so preferring the
-- highest occurrence turns available slots into unavailable ones.
-- See docs/rr1-fee-context-grain.md for the evidence and the app-side rule.
-- ---------------------------------------------------------------------------

COMMENT ON COLUMN rr1_fee_profiles.occurrence IS
'RR1 num.tsv `iprx`: the SEC''s own sequence number for facts that would OTHERWISE '
'BE IDENTICAL within a submission. It is not a semantic dimension -- measured over '
'the current publication, 2 821 (series, class, concept) groups vary only by '
'occurrence and ZERO of them disagree on the value; the repeated facts are '
'byte-identical apart from iprx. Consumers must NOT rank on it: prefer a row whose '
'status is available/degraded, then the LOWEST occurrence. Because the builder '
'writes the full 7-concept grid per context, a context that exists only because '
'ANOTHER concept was repeated carries `unavailable` fillers for every concept it '
'did not report -- ranking occurrence DESC serves those fillers instead of the '
'reported value.';

COMMENT ON COLUMN rr1_fee_profiles.document_id IS
'RR1 num.tsv `document`: which prospectus document inside the submission reported '
'the fact. A REAL grain -- one filing can carry several prospectuses for the same '
'class, and 162 of the 7 154 document-varying groups disagree on the value. A '
'consumer that needs one number per class must choose a document deliberately; '
'there is no producer-side ranking of documents.';

COMMENT ON COLUMN rr1_fee_profiles.dimensions IS
'RR1 num.tsv `otherdims`: the remaining XBRL dimensions of the fact. A REAL grain '
'(42 of the 98 dimension-varying groups disagree on the value); the empty string '
'is the undimensioned fact.';

COMMENT ON COLUMN rr1_fee_profiles.measure_id IS
'RR1 num.tsv `measure`. A REAL grain (3 of the 7 measure-varying groups disagree on '
'the value); the empty string is the unmeasured fact.';

COMMENT ON COLUMN rr1_fee_profiles.data_date IS
'RR1 num.tsv `ddate`: the fiscal period the fee applies to. A REAL grain (50 of the '
'2 191 date-varying groups disagree on the value) and the FIRST tie-break a '
'consumer should apply: the latest data_date is the current fee schedule.';

COMMENT ON COLUMN rr1_fee_profiles.status IS
'4-state serving vocabulary, passed through to sec_regulatory_serving_facts. '
'`unavailable` with reason_code `concept_not_reported` means the concept was not '
'reported IN THIS CONTEXT -- it does NOT mean the class never reported it: the same '
'accession may carry it at another occurrence, document or dimension set.';
