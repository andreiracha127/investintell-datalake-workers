-- Immutable RR1 fee-waiver durability snapshot.  Grain is one row per waiver
-- context (publication, source run, accession, series, class, and the preserved
-- measure/document/dimensions/occurrence): the share class that discloses a fee
-- waiver or expense reimbursement inside an amendment-aware effective filing.
--
-- It answers waiver-durability questions the long-form fee tables cannot: the
-- waiver size, its disclosed operative window (termination vs effective date),
-- cliff behaviour (an active waiver approaching termination), and the gross->net
-- reconstruction (gross - waiver ~= net).  A reconstruction divergence beyond
-- tolerance is a typed QUALITY FLAG, never a synthetic value; a missing leg
-- yields a derived NULL with the legs nullable in the payload.  The builder
-- consumes only the effective selection; it never re-opens raw RR1.

-- Frozen RR taxonomy local names, single authority for this snapshot.  The size
-- of the waiver and the reconstruction legs are ``uom=pure`` fractions carried in
-- num.tsv.  The termination date is a date-typed disclosure carried in txt.tsv;
-- RR filers report the operative-through date of an expense-limitation agreement
-- there.  When it is absent the row flags it honestly and the waiver size still
-- stands.  A custom/unmapped tag never resolves to any of these concepts
-- (Global Constraint 5).
CREATE OR REPLACE FUNCTION rr1_waiver_concept_map()
RETURNS TABLE(canonical_concept text, original_tag text, source_table text)
LANGUAGE sql IMMUTABLE AS $$
    VALUES ('waiver_over_assets', 'FeeWaiverOrReimbursementOverAssets', 'num.tsv'),
           ('gross_expense', 'ExpensesOverAssets', 'num.tsv'),
           ('net_expense', 'NetExpensesOverAssets', 'num.tsv'),
           ('waiver_termination', 'FeeWaiverOrReimbursementOverAssetsDateOfTermination', 'txt.tsv')
$$;

CREATE TABLE IF NOT EXISTS rr1_waiver_profiles (
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
    waiver_over_assets numeric,
    gross_expense_over_assets numeric,
    net_expense_over_assets numeric,
    declared_unit text,
    termination_date date,
    term_days integer,
    remaining_days integer,
    gross_minus_waiver numeric,
    net_reconstruction_gap numeric,
    reconciliation_status text NOT NULL CHECK (reconciliation_status IN ('reconciled', 'divergent', 'incomplete')),
    reconciliation_tolerance numeric NOT NULL,
    cliff_horizon_days integer NOT NULL,
    cliff_flag boolean,
    status text NOT NULL CHECK (status IN ('available', 'degraded', 'unavailable', 'not_applicable')),
    reason_code text,
    termination_reason_code text CHECK (termination_reason_code IN ('termination_date_not_reported', 'termination_date_unparseable')),
    methodology_version text NOT NULL DEFAULT 'rr1_waiver_profile_v1',
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, series_id, class_id, data_date,
                 measure_id, document_id, dimensions, occurrence),
    CHECK (methodology_version = 'rr1_waiver_profile_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    -- The row is anchored on a disclosed waiver fact: available iff the waiver
    -- size is numeric; degraded iff the waiver fact is present but non-numeric.
    CHECK ((status = 'available') = (waiver_over_assets IS NOT NULL)),
    CHECK ((status = 'available') = (reason_code IS NULL)),
    -- Termination-dependent metrics are honest about absence.
    CHECK ((termination_date IS NULL) = (termination_reason_code IS NOT NULL)),
    CHECK (cliff_flag IS NULL OR termination_date IS NOT NULL),
    -- A missing reconstruction leg is 'incomplete'; it never masks a value.
    CHECK ((reconciliation_status = 'incomplete') = (net_reconstruction_gap IS NULL))
);

CREATE TABLE IF NOT EXISTS rr1_waiver_profile_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION rr1_waiver_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 waiver build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_waiver_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 waiver build identity requires a prepared waiver publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_waiver_build_guard ON rr1_waiver_profile_builds;
CREATE TRIGGER rr1_waiver_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_waiver_profile_builds
FOR EACH ROW EXECUTE FUNCTION rr1_waiver_build_guard();

CREATE OR REPLACE FUNCTION rr1_waiver_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 waiver profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_waiver_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 waiver profile requires a prepared waiver publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM rr1_waiver_profile_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS NULL THEN
        RAISE EXCEPTION 'RR1 waiver profile requires pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_waiver_write_guard ON rr1_waiver_profiles;
CREATE TRIGGER rr1_waiver_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_waiver_profiles
FOR EACH ROW EXECUTE FUNCTION rr1_waiver_write_guard();

CREATE OR REPLACE FUNCTION build_rr1_waiver_profiles(
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
    reconciliation_tolerance constant numeric := 0.0001;
    cliff_horizon_days constant integer := 180;
BEGIN
    IF as_of_date IS NULL THEN
        RAISE EXCEPTION 'RR1 waiver build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'rr1_waiver_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 waiver build requires a prepared waiver publication';
    END IF;

    -- Canonical inputs: RR-namespaced facts on the concept map's exact table+tag.
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_waiver_concept_map() m
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
           ), '')) || md5('rr1_waiver_profile_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM selected;

    INSERT INTO rr1_waiver_profile_builds(publication_id,input_fingerprint,as_of_date,effective_input_count)
    VALUES(target_publication_id,computed_fingerprint,as_of_date,selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM rr1_waiver_profile_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'RR1 waiver build is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'RR1 waiver build is already pinned to effective-input fingerprint';
    END IF;

    -- Fan-out guard: every leg (waiver, gross, net, termination) is pre-aggregated
    -- to at most one fact per preserved context grain BEFORE the legs are combined.
    -- A duplicated leg would multiply the waiver row, so it is a hard failure.
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_waiver_concept_map() m
          ON m.original_tag = f.tag AND m.source_table = f.source_table
        WHERE f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    )
    SELECT CASE
        WHEN EXISTS (SELECT 1 FROM selected WHERE NULLIF(btrim(series_id),'') IS NULL OR NULLIF(btrim(class_id),'') IS NULL)
            THEN 'missing RR1 series or class identity'
        WHEN EXISTS (
            SELECT 1 FROM selected
            GROUP BY ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,''),COALESCE(document_id,''),
                     COALESCE(dimensions,''),COALESCE(occurrence,''),effective_date,filed_date,canonical_concept
            HAVING count(*) > 1
        ) THEN 'conflicting RR1 waiver facts'
    END INTO parent_state;
    IF parent_state IS NOT NULL THEN RAISE EXCEPTION '%', parent_state; END IF;

    WITH selected AS (
        SELECT f.*, m.canonical_concept,
               NULLIF(btrim(f.fact_typed_projection->>'value'),'') AS raw_value,
               NULLIF(btrim(f.fact_typed_projection->>'uom'),'') AS declared_unit
        FROM rr1_effective_facts f JOIN rr1_waiver_concept_map() m
          ON m.original_tag = f.tag AND m.source_table = f.source_table
        WHERE f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    ), waiver_leg AS (
        SELECT ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,'') AS measure_id,
               COALESCE(document_id,'') AS document_id,COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
               effective_date,filed_date,form,tag,version,raw_value,declared_unit,raw_row_id
        FROM selected WHERE canonical_concept='waiver_over_assets'
    ), gross_leg AS (
        SELECT ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,'') AS measure_id,
               COALESCE(document_id,'') AS document_id,COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
               effective_date,filed_date,tag,version,raw_value,raw_row_id
        FROM selected WHERE canonical_concept='gross_expense'
    ), net_leg AS (
        SELECT ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,'') AS measure_id,
               COALESCE(document_id,'') AS document_id,COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
               effective_date,filed_date,tag,version,raw_value,raw_row_id
        FROM selected WHERE canonical_concept='net_expense'
    ), termination_leg AS (
        SELECT ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,'') AS measure_id,
               COALESCE(document_id,'') AS document_id,COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
               effective_date,filed_date,tag,version,raw_value,raw_row_id
        FROM selected WHERE canonical_concept='waiver_termination'
    ), combined AS (
        SELECT w.*,
               rr1_numeric_value(w.raw_value) AS waiver_value,
               rr1_numeric_value(g.raw_value) AS gross_value,
               rr1_numeric_value(n.raw_value) AS net_value,
               g.tag AS gross_tag, g.version AS gross_version, g.raw_row_id AS gross_raw_row_id,
               n.tag AS net_tag, n.version AS net_version, n.raw_row_id AS net_raw_row_id,
               t.tag AS termination_tag, t.version AS termination_version, t.raw_value AS termination_raw_value,
               t.raw_row_id AS termination_raw_row_id,
               rr1_safe_iso_date(t.raw_value) AS termination_date,
               (t.raw_row_id IS NOT NULL) AS termination_present
        FROM waiver_leg w
        LEFT JOIN gross_leg g USING (ingestion_run_id,accession_number,series_id,class_id,data_date,measure_id,document_id,dimensions,occurrence,effective_date,filed_date)
        LEFT JOIN net_leg n USING (ingestion_run_id,accession_number,series_id,class_id,data_date,measure_id,document_id,dimensions,occurrence,effective_date,filed_date)
        LEFT JOIN termination_leg t USING (ingestion_run_id,accession_number,series_id,class_id,data_date,measure_id,document_id,dimensions,occurrence,effective_date,filed_date)
    ), derived AS (
        SELECT c.*,
               rr1_safe_diff(c.gross_value, c.waiver_value) AS gross_minus_waiver,
               rr1_safe_day_span(c.effective_date, c.termination_date) AS term_days,
               rr1_safe_day_span(as_of_date, c.termination_date) AS remaining_days
        FROM combined c
    )
    INSERT INTO rr1_waiver_profiles(
        publication_id,source_run_id,accession_number,series_id,class_id,data_date,measure_id,document_id,dimensions,occurrence,
        effective_date,filed_date,form,waiver_over_assets,gross_expense_over_assets,net_expense_over_assets,declared_unit,
        termination_date,term_days,remaining_days,gross_minus_waiver,net_reconstruction_gap,reconciliation_status,
        reconciliation_tolerance,cliff_horizon_days,cliff_flag,status,reason_code,termination_reason_code,provenance,coverage
    )
    SELECT target_publication_id,d.ingestion_run_id,d.accession_number,d.series_id,d.class_id,d.data_date,d.measure_id,d.document_id,
           d.dimensions,d.occurrence,d.effective_date,d.filed_date,d.form,d.waiver_value,d.gross_value,d.net_value,d.declared_unit,
           d.termination_date,d.term_days,d.remaining_days,d.gross_minus_waiver,
           rr1_safe_diff(d.gross_minus_waiver, d.net_value),
           CASE WHEN rr1_safe_diff(d.gross_minus_waiver, d.net_value) IS NULL THEN 'incomplete'
                WHEN abs(rr1_safe_diff(d.gross_minus_waiver, d.net_value)) <= reconciliation_tolerance THEN 'reconciled'
                ELSE 'divergent' END,
           reconciliation_tolerance, cliff_horizon_days,
           CASE WHEN d.waiver_value IS NULL OR d.waiver_value <= 0 OR d.termination_date IS NULL THEN NULL
                ELSE (d.remaining_days >= 0 AND d.remaining_days <= cliff_horizon_days) END,
           CASE WHEN d.waiver_value IS NOT NULL THEN 'available' ELSE 'degraded' END,
           CASE WHEN d.waiver_value IS NOT NULL THEN NULL ELSE 'waiver_value_non_numeric' END,
           CASE WHEN d.termination_date IS NOT NULL THEN NULL
                WHEN d.termination_present THEN 'termination_date_unparseable'
                ELSE 'termination_date_not_reported' END,
           jsonb_build_object('effective_selection_view','rr1_effective_facts',
                              'source_run_id',d.ingestion_run_id,'accession_number',d.accession_number,
                              'waiver_raw_row_id',d.raw_row_id,'waiver_tag',d.tag,'waiver_version',d.version,
                              'gross_raw_row_id',d.gross_raw_row_id,'gross_tag',d.gross_tag,'gross_version',d.gross_version,
                              'net_raw_row_id',d.net_raw_row_id,'net_tag',d.net_tag,'net_version',d.net_version,
                              'termination_raw_row_id',d.termination_raw_row_id,'termination_tag',d.termination_tag,
                              'termination_version',d.termination_version,'termination_raw_value',d.termination_raw_value),
           jsonb_build_object('source_applicable',true,'as_of_date',as_of_date,
                              'reconciliation_tolerance',reconciliation_tolerance,'cliff_horizon_days',cliff_horizon_days,
                              'gross_leg_present',(d.gross_tag IS NOT NULL),'net_leg_present',(d.net_tag IS NOT NULL),
                              'termination_leg_present',d.termination_present,'context_preserved',true)
    FROM derived d
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_rr1_waiver_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN rr1_waiver_profiles p ON p.publication_id = c.publication_id
WHERE c.product = 'rr1_waiver_profile_v1';
