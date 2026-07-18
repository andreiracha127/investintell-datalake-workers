-- Immutable RR1 share-class fee facts.  This builder consumes only the
-- amendment-aware effective selection; it deliberately does not re-open raw RR1.

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
    occurrence integer NOT NULL DEFAULT 0,
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
    WITH mapped(canonical_concept, original_tag) AS (
        VALUES ('management_fee', 'ManagementFees'),
               ('distribution_12b1', 'DistributionAndService12b1Fees'),
               ('acquired_fund_expense', 'AcquiredFundFeesAndExpenses'),
               ('other_expense', 'OtherExpenses'),
               ('gross_expense', 'TotalAnnualFundOperatingExpenses'),
               ('waiver_reimbursement', 'FeeWaiversAndExpenseReimbursements'),
               ('net_expense', 'TotalAnnualFundOperatingExpensesAfterFeeWaiversAndExpenseReimbursements')
    ), selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN mapped m ON m.original_tag = f.tag
        WHERE f.source_table = 'num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    )
    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || canonical_concept || ':' || tag || ':' || version || ':' ||
                data_date::text || ':' || COALESCE(series_id,'') || ':' || COALESCE(class_id,'') || ':' || COALESCE(measure_id,'') || ':' ||
                COALESCE(document_id,'') || ':' || COALESCE(dimensions,'') || ':' || COALESCE(occurrence,0)::text || ':' ||
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

    WITH mapped(canonical_concept, original_tag) AS (
        VALUES ('management_fee', 'ManagementFees'),
               ('distribution_12b1', 'DistributionAndService12b1Fees'),
               ('acquired_fund_expense', 'AcquiredFundFeesAndExpenses'),
               ('other_expense', 'OtherExpenses'),
               ('gross_expense', 'TotalAnnualFundOperatingExpenses'),
               ('waiver_reimbursement', 'FeeWaiversAndExpenseReimbursements'),
               ('net_expense', 'TotalAnnualFundOperatingExpensesAfterFeeWaiversAndExpenseReimbursements')
    ), selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN mapped m ON m.original_tag = f.tag
        WHERE f.source_table = 'num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    )
    SELECT CASE
        WHEN EXISTS (SELECT 1 FROM selected WHERE NULLIF(btrim(series_id),'') IS NULL OR NULLIF(btrim(class_id),'') IS NULL)
            THEN 'missing RR1 series or class identity'
        WHEN EXISTS (
            SELECT 1 FROM selected
            GROUP BY ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,''),COALESCE(document_id,''),
                     COALESCE(dimensions,''),COALESCE(occurrence,0),effective_date,filed_date,canonical_concept
            HAVING count(*) > 1
        ) THEN 'conflicting RR1 fee facts'
    END INTO parent_state;
    IF parent_state IS NOT NULL THEN RAISE EXCEPTION '%', parent_state; END IF;

    WITH mapped(canonical_concept, original_tag) AS (
        VALUES ('management_fee', 'ManagementFees'),
               ('distribution_12b1', 'DistributionAndService12b1Fees'),
               ('acquired_fund_expense', 'AcquiredFundFeesAndExpenses'),
               ('other_expense', 'OtherExpenses'),
               ('gross_expense', 'TotalAnnualFundOperatingExpenses'),
               ('waiver_reimbursement', 'FeeWaiversAndExpenseReimbursements'),
               ('net_expense', 'TotalAnnualFundOperatingExpensesAfterFeeWaiversAndExpenseReimbursements')
    ), selected AS (
        SELECT f.*, m.canonical_concept, NULLIF(btrim(f.fact_typed_projection->>'value'),'') AS raw_value
        FROM rr1_effective_facts f JOIN mapped m ON m.original_tag = f.tag
        WHERE f.source_table = 'num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    ), contexts AS (
        SELECT DISTINCT ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,'') AS measure_id,
               COALESCE(document_id,'') AS document_id,COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,0) AS occurrence,
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
                       AND COALESCE(s.dimensions,'')=c.dimensions AND COALESCE(s.occurrence,0)=c.occurrence
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
