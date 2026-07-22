-- Immutable RR1 share-class shareholder-cost facts: the directly-paid
-- shareholder fees (front-end / deferred sales charges, redemption, exchange,
-- account fees) and the standardised expense examples (1/3/5/10 year).  This
-- completes the fee waterfall on the shareholder-cost side; the annual
-- operating-expense over-assets waterfall (management/12b-1/AFFE/other/gross/
-- waiver/net) is owned by rr1_fee_profiles and is deliberately NOT duplicated
-- here.  The builder consumes only the amendment-aware effective selection; it
-- never re-opens raw RR1.

-- Frozen RR taxonomy local names.  These are RR taxonomy standard element names,
-- not sample-derived aliases: shareholder fees are ``uom=pure`` fractions and the
-- expense examples are USD currency amounts.  The fraction/amount is stored
-- verbatim and the declared unit is retained; display-side scaling (fraction
-- x100 to percentage points) is a presentation concern (Global Constraint 5).
CREATE OR REPLACE FUNCTION rr1_shareholder_cost_concept_map()
RETURNS TABLE(canonical_concept text, original_tag text, cost_group text, unit_class text)
LANGUAGE sql IMMUTABLE AS $$
    VALUES ('sales_charge_purchase', 'MaximumSalesChargeImposedOnPurchasesOverOfferingPrice', 'shareholder_fee', 'fraction'),
           ('deferred_sales_charge', 'MaximumDeferredSalesChargeOverOther', 'shareholder_fee', 'fraction'),
           ('redemption_fee', 'RedemptionFeeOverRedemption', 'shareholder_fee', 'fraction'),
           ('exchange_fee', 'ExchangeFeeOverRedemption', 'shareholder_fee', 'fraction'),
           ('account_fee', 'MaximumAccountFee', 'shareholder_fee', 'currency'),
           ('expense_example_1y', 'ExpenseExampleYear01', 'expense_example', 'currency'),
           ('expense_example_3y', 'ExpenseExampleYear03', 'expense_example', 'currency'),
           ('expense_example_5y', 'ExpenseExampleYear05', 'expense_example', 'currency'),
           ('expense_example_10y', 'ExpenseExampleYear10', 'expense_example', 'currency')
$$;

CREATE TABLE IF NOT EXISTS rr1_shareholder_cost_profiles (
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
        'sales_charge_purchase', 'deferred_sales_charge', 'redemption_fee', 'exchange_fee', 'account_fee',
        'expense_example_1y', 'expense_example_3y', 'expense_example_5y', 'expense_example_10y'
    )),
    cost_group text NOT NULL CHECK (cost_group IN ('shareholder_fee', 'expense_example')),
    unit_class text NOT NULL CHECK (unit_class IN ('fraction', 'currency')),
    original_tag text,
    original_version text,
    value_numeric numeric,
    declared_unit text,
    status text NOT NULL CHECK (status IN ('available', 'degraded', 'unavailable', 'not_applicable')),
    reason_code text,
    methodology_version text NOT NULL DEFAULT 'rr1_shareholder_cost_profile_v1',
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, series_id, class_id, data_date,
                 measure_id, document_id, dimensions, occurrence, canonical_concept),
    CHECK (methodology_version = 'rr1_shareholder_cost_profile_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((status = 'available') = (value_numeric IS NOT NULL AND original_tag IS NOT NULL AND original_version IS NOT NULL)),
    CHECK ((status = 'available') = (reason_code IS NULL)),
    CHECK ((status = 'unavailable') = (original_tag IS NULL AND original_version IS NULL AND value_numeric IS NULL)),
    -- A declared unit accompanies every fact that produced a tag; an unreported
    -- concept keeps declared_unit NULL but still records its expected unit_class.
    CHECK ((declared_unit IS NOT NULL) = (original_tag IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS rr1_shareholder_cost_profile_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION rr1_shareholder_cost_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 shareholder-cost build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_shareholder_cost_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 shareholder-cost build identity requires a prepared shareholder-cost publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_shareholder_cost_build_guard ON rr1_shareholder_cost_profile_builds;
CREATE TRIGGER rr1_shareholder_cost_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_shareholder_cost_profile_builds
FOR EACH ROW EXECUTE FUNCTION rr1_shareholder_cost_build_guard();

CREATE OR REPLACE FUNCTION rr1_shareholder_cost_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'RR1 shareholder-cost profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'rr1_shareholder_cost_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 shareholder-cost profile requires a prepared shareholder-cost publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM rr1_shareholder_cost_profile_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS NULL THEN
        RAISE EXCEPTION 'RR1 shareholder-cost profile requires pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_shareholder_cost_write_guard ON rr1_shareholder_cost_profiles;
CREATE TRIGGER rr1_shareholder_cost_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON rr1_shareholder_cost_profiles
FOR EACH ROW EXECUTE FUNCTION rr1_shareholder_cost_write_guard();

CREATE OR REPLACE FUNCTION build_rr1_shareholder_cost_profiles(
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
        RAISE EXCEPTION 'RR1 shareholder-cost build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'rr1_shareholder_cost_profile_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'RR1 shareholder-cost build requires a prepared shareholder-cost publication';
    END IF;

    -- Canonical inputs: only RR-namespaced (``rr/%``) facts whose exact tag is in
    -- the frozen concept map.  A custom-namespaced or unmapped tag can never match,
    -- so it never populates a canonical metric (Global Constraint 5).
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_shareholder_cost_concept_map() m ON m.original_tag = f.tag
        WHERE f.source_table = 'num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    )
    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || canonical_concept || ':' || tag || ':' || version || ':' ||
                data_date::text || ':' || COALESCE(series_id,'') || ':' || COALESCE(class_id,'') || ':' || COALESCE(measure_id,'') || ':' ||
                COALESCE(document_id,'') || ':' || COALESCE(dimensions,'') || ':' || COALESCE(occurrence,'') || ':' ||
                effective_date::text || ':' || filed_date::text || ':' || fact_typed_projection::text,
                '|' ORDER BY ingestion_run_id, accession_number, canonical_concept, tag, version, data_date,
                             series_id, class_id, measure_id, document_id, dimensions, occurrence, effective_date, filed_date, raw_row_id
           ), '')) || md5('rr1_shareholder_cost_profile_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM selected;

    INSERT INTO rr1_shareholder_cost_profile_builds(publication_id,input_fingerprint,as_of_date,effective_input_count)
    VALUES(target_publication_id,computed_fingerprint,as_of_date,selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM rr1_shareholder_cost_profile_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'RR1 shareholder-cost build is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'RR1 shareholder-cost build is already pinned to effective-input fingerprint';
    END IF;

    -- Fan-out guard: one canonical concept must resolve to at most one fact per
    -- preserved context grain.  A duplicated selected fact is a hard failure, not
    -- an arbitrary winner (Global Constraint 3/4).
    WITH selected AS (
        SELECT f.*, m.canonical_concept
        FROM rr1_effective_facts f JOIN rr1_shareholder_cost_concept_map() m ON m.original_tag = f.tag
        WHERE f.source_table = 'num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    )
    SELECT CASE
        WHEN EXISTS (SELECT 1 FROM selected WHERE NULLIF(btrim(series_id),'') IS NULL OR NULLIF(btrim(class_id),'') IS NULL)
            THEN 'missing RR1 series or class identity'
        WHEN EXISTS (
            SELECT 1 FROM selected
            GROUP BY ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,''),COALESCE(document_id,''),
                     COALESCE(dimensions,''),COALESCE(occurrence,''),effective_date,filed_date,canonical_concept
            HAVING count(*) > 1
        ) THEN 'conflicting RR1 shareholder-cost facts'
    END INTO parent_state;
    IF parent_state IS NOT NULL THEN RAISE EXCEPTION '%', parent_state; END IF;

    WITH mapped AS (
        SELECT canonical_concept, original_tag, cost_group, unit_class FROM rr1_shareholder_cost_concept_map()
    ), selected AS (
        SELECT f.*, m.canonical_concept,
               NULLIF(btrim(f.fact_typed_projection->>'value'),'') AS raw_value,
               NULLIF(btrim(f.fact_typed_projection->>'uom'),'') AS declared_unit
        FROM rr1_effective_facts f JOIN mapped m ON m.original_tag = f.tag
        WHERE f.source_table = 'num.tsv' AND f.version LIKE 'rr/%' AND f.effective_date <= as_of_date
    ), contexts AS (
        SELECT DISTINCT ingestion_run_id,accession_number,series_id,class_id,data_date,COALESCE(measure_id,'') AS measure_id,
               COALESCE(document_id,'') AS document_id,COALESCE(dimensions,'') AS dimensions,COALESCE(occurrence,'') AS occurrence,
               effective_date,filed_date,form
        FROM selected
    )
    INSERT INTO rr1_shareholder_cost_profiles(
        publication_id,source_run_id,accession_number,series_id,class_id,data_date,measure_id,document_id,dimensions,occurrence,
        effective_date,filed_date,form,canonical_concept,cost_group,unit_class,original_tag,original_version,value_numeric,
        declared_unit,status,reason_code,provenance,coverage
    )
    SELECT target_publication_id,c.ingestion_run_id,c.accession_number,c.series_id,c.class_id,c.data_date,c.measure_id,c.document_id,
           c.dimensions,c.occurrence,c.effective_date,c.filed_date,c.form,m.canonical_concept,m.cost_group,m.unit_class,
           s.tag,s.version,rr1_numeric_value(s.raw_value),
           CASE WHEN s.tag IS NULL THEN NULL ELSE s.declared_unit END,
           CASE WHEN s.tag IS NULL THEN 'unavailable'
                WHEN rr1_numeric_value(s.raw_value) IS NOT NULL THEN 'available'
                ELSE 'degraded' END,
           CASE WHEN s.tag IS NULL THEN 'concept_not_reported'
                WHEN rr1_numeric_value(s.raw_value) IS NULL THEN 'selected_fact_has_no_numeric_value'
                ELSE NULL END,
           jsonb_build_object('effective_selection_view','rr1_effective_facts','effective_raw_row_id',s.raw_row_id,
                              'source_run_id',c.ingestion_run_id,'accession_number',c.accession_number,
                              'original_tag',s.tag,'original_version',s.version),
           jsonb_build_object('source_applicable',true,'as_of_date',as_of_date,'canonical_concept',m.canonical_concept,
                              'cost_group',m.cost_group,'unit_class',m.unit_class,'context_preserved',true)
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

CREATE OR REPLACE VIEW sec_current_rr1_shareholder_cost_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN rr1_shareholder_cost_profiles p ON p.publication_id = c.publication_id
WHERE c.product = 'rr1_shareholder_cost_profile_v1';
