-- Public-only serving projection for Bonds v1 (bond_serving_v1).
--
-- Increment 2 Task 5 (workers half). A SIBLING derived product (independent of
-- sec_regulatory_serving_v1) materialises ONE row-set per publication that
-- projects only PUBLIC columns across the four bond serving surfaces:
--   * catalog       -- search-ready identity + summary terms + data state (grain: security)
--   * detail        -- full terms incl. call/put schedule + 144A + PIT aliases + neutral
--                      identity ambiguity evidence (grain: security)
--   * observations  -- price/trade observations WITH a mandatory `lane` discriminator +
--                      freshness/ambiguity states (grain: security_observation)
--   * fund_exposure -- N-PORT point-in-time reverse lookup by security, pre-aggregated at
--                      fund (series) grain (grain: security_fund)
--
-- Bond DTOs carry NO source/vendor literals and only NEUTRAL product dates
-- (`as_of`, `observation_date`). `bond_serving_scrub` recursively strips internal
-- identifier keys from every payload and neutralises `cik:`/`row:` entity-key
-- fallback VALUES, so the leak property holds by construction, and the app reads
-- this product BY EXACT publication_id (never a `sec_current_*` view).
--
-- The serving publication rides the same `sec_derived_publications` protocol as
-- every other product (prepare -> validate -> current pointer), so a complete
-- serving version is promoted atomically and the app can pin `publication_version`.
--
-- The DDL is idempotent (CREATE ... IF NOT EXISTS, CREATE OR REPLACE) so the
-- worker's install_schema step may apply it repeatedly.

-- ---------------------------------------------------------------------------
-- Recursive public scrubber: drop internal-identifier KEYS anywhere in a payload
-- and neutralise the `row:`/`cik:` entity-key fallback VALUE. Immutable so it is
-- safe inside set-based INSERT ... SELECT projections. The blocklist MUST equal
-- ``serving_contract.SCRUB_BLOCKLIST`` (both hashed into the digest).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_serving_scrub(data jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    result jsonb;
    item_key text;
    item_val jsonb;
    blocked text[] := ARRAY[
        'accession_number', 'contributing_observation_ids', 'holding_id',
        'identity_key', 'ingestion_run_id', 'observation_id', 'observation_ids',
        'provenance', 'raw_row_id', 'registrant_cik', 'source_lineage',
        'source_run_id', 'source_table', 'source_typed_projection', 'text_block_md5'
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
                result := result || jsonb_build_object(item_key, bond_serving_scrub(item_val));
            END LOOP;
            RETURN result;
        WHEN 'array' THEN
            RETURN COALESCE(
                (SELECT jsonb_agg(bond_serving_scrub(elem)) FROM jsonb_array_elements(data) elem),
                '[]'::jsonb
            );
        WHEN 'string' THEN
            -- Neutralise internal entity-key fallbacks surviving under a public
            -- (non-blocklisted) key position: any ``row:<...>`` or ``cik:<...>``
            -- identifier value must never reach the serving surface verbatim.
            IF (data #>> '{}') ~ '^(row|cik):' THEN
                RETURN to_jsonb('unavailable'::text);
            END IF;
            RETURN data;
        ELSE
            RETURN data;
    END CASE;
END $$;

-- ---------------------------------------------------------------------------
-- Serving facts: one immutable public row per (publication, surface, grain key).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_serving_facts (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    surface text NOT NULL CHECK (surface IN ('catalog', 'detail', 'observations', 'fund_exposure')),
    security_id uuid NOT NULL,
    -- Mandatory lane discriminator for observations; forbidden elsewhere.
    lane text NOT NULL DEFAULT '' CHECK (lane IN ('', 'latest', 'fund_asof')),
    fund_key text NOT NULL DEFAULT '',
    fact_key text NOT NULL DEFAULT '',
    state text NOT NULL CHECK (state IN ('available', 'degraded', 'unavailable', 'not_applicable')),
    reason_code text CHECK (reason_code IS NULL OR reason_code IN (
        'identity_ambiguous', 'not_applicable', 'observation_ambiguous',
        'observation_stale', 'source_unavailable', 'terms_incomplete'
    )),
    identity_state text CHECK (identity_state IS NULL OR identity_state IN ('resolved', 'ambiguous')),
    ambiguity_state text CHECK (ambiguity_state IS NULL OR ambiguity_state IN ('resolved', 'ambiguous')),
    as_of date,
    observation_date date,
    coverage_pct numeric CHECK (coverage_pct IS NULL OR (coverage_pct >= 0 AND coverage_pct <= 100)),
    payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, surface, security_id, lane, fund_key, fact_key),
    -- observations carry a lane exactly; every other surface carries none.
    CHECK ((surface = 'observations') = (lane <> '')),
    -- available/degraded carry a public payload; unavailable/not_applicable never do.
    CHECK ((state IN ('available', 'degraded')) = (payload IS NOT NULL)),
    -- Global Constraint 3 / plan #3: an observations payload MUST carry its lane.
    CHECK (surface <> 'observations' OR payload IS NULL OR (payload ? 'lane')),
    -- the public surface never carries an internal provenance/lineage object.
    CHECK (payload IS NULL OR NOT (payload ? 'provenance')),
    CHECK (payload IS NULL OR NOT (payload ? 'source_lineage')),
    CHECK (payload IS NULL OR NOT (payload ? 'source_run_id')),
    CHECK (payload IS NULL OR NOT (payload ? 'raw_row_id')),
    CHECK (payload IS NULL OR NOT (payload ? 'source_typed_projection'))
);

CREATE INDEX IF NOT EXISTS bond_serving_facts_surface_idx
    ON bond_serving_facts (publication_id, surface, security_id);

-- Build metadata records exactly which source current-publications were consumed
-- so the true multi-source lineage is auditable (the serving publication's own
-- lineage anchors to the base validated source run for the shared FK).
CREATE TABLE IF NOT EXISTS bond_serving_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    as_of_date date NOT NULL,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    consumed_source_publications jsonb NOT NULL CHECK (jsonb_typeof(consumed_source_publications) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Write guard: serving rows/builds are insert-only while their publication is
-- prepared, immutable afterwards (mirrors the snapshot guards).
CREATE OR REPLACE FUNCTION bond_serving_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond serving row is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_serving_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond serving row requires a prepared bond_serving_v1 publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_serving_facts_write_guard ON bond_serving_facts;
CREATE TRIGGER bond_serving_facts_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_serving_facts
FOR EACH ROW EXECUTE FUNCTION bond_serving_write_guard();

DROP TRIGGER IF EXISTS bond_serving_builds_write_guard ON bond_serving_builds;
CREATE TRIGGER bond_serving_builds_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_serving_builds
FOR EACH ROW EXECUTE FUNCTION bond_serving_write_guard();

-- Current-pointer view for workers verification (the APP pins by exact
-- publication_id via its own pin row, and MUST NOT read this view).
CREATE OR REPLACE VIEW sec_current_bond_serving_facts AS
SELECT f.*
FROM sec_derived_current_pointers c
JOIN bond_serving_facts f ON f.publication_id = c.publication_id
WHERE c.product = 'bond_serving_v1';
