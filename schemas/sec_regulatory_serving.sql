-- Public-only serving projection over the current N-CEN/RR1 derived snapshots.
--
-- Task 7 (workers half).  A single derived product, `sec_regulatory_serving_v1`,
-- materialises ONE row-set per publication that projects only PUBLIC columns from
-- the family snapshots.  It never copies the internal `provenance` blob, the
-- `source_run_id`, `registrant_cik`, raw row ids, `source_table`, or narrative
-- hashes into the serving surface.  `sec_serving_scrub` recursively strips those
-- keys from every family payload so the leak property holds by construction, and
-- the app reads this product BY EXACT publication_id (never `sec_current_*`).
--
-- The serving publication rides the same `sec_derived_publications` protocol as
-- every other product (prepare -> validate -> current pointer), so a complete
-- serving version is promoted atomically and the app can pin `publication_version`.

-- ---------------------------------------------------------------------------
-- Recursive public scrubber: drop internal-identifier KEYS anywhere in a payload
-- and neutralise the `row:<raw_row_id>` entity-key fallback VALUE.  Immutable so
-- it is safe inside set-based INSERT ... SELECT projections.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sec_serving_scrub(data jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    result jsonb;
    item_key text;
    item_val jsonb;
    blocked text[] := ARRAY[
        'raw_row_id', 'source_run_id', 'ingestion_run_id', 'source_table',
        'submission_raw_row_id', 'registrant_raw_row_id', 'fund_raw_row_id',
        'etf_detail_raw_row_id', 'effective_raw_row_id', 'text_block_md5',
        'registrant_cik', 'custom_tag', 'original_tag', 'original_version'
    ];
BEGIN
    IF data IS NULL THEN
        RETURN NULL;
    END IF;
    CASE jsonb_typeof(data)
        WHEN 'object' THEN
            result := '{}'::jsonb;
            FOR item_key, item_val IN SELECT * FROM jsonb_each(data) LOOP
                CONTINUE WHEN item_key = ANY(blocked);
                CONTINUE WHEN item_key ~ '_raw_row_id$';
                CONTINUE WHEN item_key ~ '_md5$';
                CONTINUE WHEN item_key ~ 'sha256';
                result := result || jsonb_build_object(item_key, sec_serving_scrub(item_val));
            END LOOP;
            RETURN result;
        WHEN 'array' THEN
            RETURN COALESCE(
                (SELECT jsonb_agg(sec_serving_scrub(elem)) FROM jsonb_array_elements(data) elem),
                '[]'::jsonb
            );
        WHEN 'string' THEN
            IF (data #>> '{}') ~ '^row:' THEN
                RETURN to_jsonb('unavailable'::text);
            END IF;
            RETURN data;
        ELSE
            RETURN data;
    END CASE;
END $$;

-- ---------------------------------------------------------------------------
-- Serving facts: one immutable public row per (publication, family, grain, fact).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sec_regulatory_serving_facts (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    family text NOT NULL CHECK (family IN (
        'ncen_structure', 'ncen_provider_network', 'ncen_operational_event',
        'ncen_liquidity_backstop', 'ncen_securities_lending', 'ncen_etf_primary_market',
        'ncen_closed_end', 'ncen_expense_brokerage',
        'rr1_fee', 'rr1_shareholder_cost', 'rr1_waiver', 'rr1_class_cost_dispersion',
        'rr1_turnover', 'rr1_reported_performance', 'rr1_benchmark', 'rr1_custom_tag_crosswalk'
    )),
    series_id text NOT NULL DEFAULT '',
    class_id text NOT NULL DEFAULT '',
    fund_id text NOT NULL DEFAULT '',
    fact_key text NOT NULL DEFAULT '',
    grain_origin text NOT NULL CHECK (grain_origin IN ('fund', 'series', 'class', 'registrant', 'crosswalk')),
    state text NOT NULL CHECK (state IN ('available', 'degraded', 'unavailable', 'not_applicable')),
    reason_code text CHECK (reason_code IS NULL OR reason_code IN (
        'asset_family_not_applicable', 'class_context_ambiguous',
        'coverage_below_certified_threshold', 'publication_not_ready',
        'source_filing_unavailable', 'source_stale'
    )),
    snapshot_reason_code text,
    coverage_pct numeric CHECK (coverage_pct IS NULL OR (coverage_pct >= 0 AND coverage_pct <= 100)),
    source_date date,
    accession_number text NOT NULL DEFAULT '',
    document_id text NOT NULL DEFAULT '',
    filing_date date,
    effective_date date,
    payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, family, series_id, class_id, fund_id, fact_key),
    -- available/degraded carry a public payload; unavailable/not_applicable never do.
    CHECK ((state IN ('available', 'degraded')) = (payload IS NOT NULL)),
    -- the public surface never carries an internal provenance object.
    CHECK (payload IS NULL OR NOT (payload ? 'provenance')),
    CHECK (payload IS NULL OR NOT (payload ? 'source_run_id')),
    CHECK (payload IS NULL OR NOT (payload ? 'raw_row_id'))
);

CREATE INDEX IF NOT EXISTS sec_regulatory_serving_facts_family_idx
    ON sec_regulatory_serving_facts (publication_id, family, series_id, class_id, fund_id);

-- Build metadata records exactly which family current-publications were consumed
-- so the true multi-family lineage is auditable (the serving publication's own
-- lineage anchors to the N-CEN base run for the shared derived-publication FK).
CREATE TABLE IF NOT EXISTS sec_regulatory_serving_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    as_of_date date NOT NULL,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    consumed_family_publications jsonb NOT NULL CHECK (jsonb_typeof(consumed_family_publications) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Write guard: serving rows/builds are insert-only while their publication is
-- prepared, immutable afterwards (mirrors the family snapshot guards).
CREATE OR REPLACE FUNCTION sec_regulatory_serving_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'regulatory serving row is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'sec_regulatory_serving_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'regulatory serving row requires a prepared serving publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS sec_regulatory_serving_facts_write_guard ON sec_regulatory_serving_facts;
CREATE TRIGGER sec_regulatory_serving_facts_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON sec_regulatory_serving_facts
FOR EACH ROW EXECUTE FUNCTION sec_regulatory_serving_write_guard();

DROP TRIGGER IF EXISTS sec_regulatory_serving_builds_write_guard ON sec_regulatory_serving_builds;
CREATE TRIGGER sec_regulatory_serving_builds_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON sec_regulatory_serving_builds
FOR EACH ROW EXECUTE FUNCTION sec_regulatory_serving_write_guard();

-- Current-pointer view for workers verification (the APP pins by exact
-- publication_id via its own artifact, and MUST NOT read this view).
CREATE OR REPLACE VIEW sec_current_regulatory_serving_facts AS
SELECT f.*
FROM sec_derived_current_pointers c
JOIN sec_regulatory_serving_facts f ON f.publication_id = c.publication_id
WHERE c.product = 'sec_regulatory_serving_v1';
