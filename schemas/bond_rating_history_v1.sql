-- Point-in-time bond credit-rating history (bond_rating_history_v1) with an
-- immutable observation layer, one published snapshot through the shared
-- sec_derived_publications protocol, and an EXPLICIT LICENSE GATE.
--
-- License gate (spec — two layers, fail-closed):
--   1. INPUT layer: every observation MUST carry a non-empty licensed_source_ref
--      (NOT NULL) — you literally cannot land a rating observation without a
--      license reference.
--   2. PRODUCT layer: a product-level license_verified flag on the build pin.
--      SEM licença verificada (license_verified=false), the materializer publishes
--      the WHOLE product with product_state='not_applicable' and
--      reason_code='no_licensed_source' and ZERO rating rows — never data without a
--      verified license.  Rating snapshot rows exist ONLY under a verified license
--      (the snapshot's own license_verified column is CHECK-forced to true).
--
-- Agency opacity (spec): the agency is carried as an OPAQUE internal code
-- (agency_code) with an internal-only mapping table (bond_rating_agency_map).  No
-- agency NAME ever lands in a snapshot column — a future public serving projection
-- must never see a vendor/agency literal.  The mapping table is revoked from PUBLIC.
--
-- Layers (mirroring the Task 3/4 conventions):
--   * bond_rating_observation — immutable PIT rating observations, one row per
--     observed (subject, agency, rating action) with half-open [valid_from,
--     valid_to) windows.  Insert-only (immutability trigger).
--   * bond_rating_history_v1 (+ _builds) — one published snapshot promoted
--     atomically (prepared -> validated -> current).  Child rows written only while
--     'prepared', pinned to a product-salted fingerprint.  The build pin also
--     carries the product_state / reason_code / license_verified triple.
--
-- The DDL is idempotent so the worker's install_schema step may re-apply it.

-- ---------------------------------------------------------------------------
-- Internal opaque-agency mapping (datalake-internal ONLY; never serving-bound).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_rating_agency_map (
    agency_code   text PRIMARY KEY CHECK (agency_code <> ''),
    agency_label  text NOT NULL CHECK (agency_label <> ''),
    created_at    timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON bond_rating_agency_map FROM PUBLIC;
-- NOTE: REVOKE FROM PUBLIC does NOT restrict the table owner or a superuser;
-- effective opacity depends on downstream reads running under a NON-OWNER role
-- (the same posture as the sibling sec_derived_*_tokens token tables).

-- ---------------------------------------------------------------------------
-- Immutable rating observations (publication inputs). licensed_source_ref is the
-- per-observation input license gate (NOT NULL).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_rating_observation (
    observation_id       uuid PRIMARY KEY,
    as_of                date NOT NULL,
    source_run_id        uuid NOT NULL,
    subject_kind         text NOT NULL CHECK (subject_kind IN ('security', 'issuer')),
    security_id          uuid,
    issuer_id            text,
    agency_code text NOT NULL REFERENCES bond_rating_agency_map(agency_code) ON DELETE RESTRICT,
    rating               text NOT NULL CHECK (rating <> ''),
    watch                text,
    outlook              text,
    valid_from           date NOT NULL,
    valid_to             date,
    licensed_source_ref text NOT NULL CHECK (licensed_source_ref <> ''),
    source_lineage       jsonb NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    -- Exactly one subject identity, matching the subject_kind.
    CHECK ((subject_kind = 'security') = (security_id IS NOT NULL)),
    CHECK ((subject_kind = 'issuer') = (issuer_id IS NOT NULL)),
    -- Half-open [valid_from, valid_to) PIT window (Task-3 convention): valid_to is
    -- EXCLUSIVE and open when NULL; a non-open window is strictly non-empty.
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

CREATE OR REPLACE FUNCTION bond_rating_observation_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bond_rating_observation is immutable';
END $$;

DROP TRIGGER IF EXISTS bond_rating_observation_immutable ON bond_rating_observation;
CREATE TRIGGER bond_rating_observation_immutable
BEFORE UPDATE OR DELETE ON bond_rating_observation
FOR EACH ROW EXECUTE FUNCTION bond_rating_observation_immutable();

-- ---------------------------------------------------------------------------
-- Product-salted build pin + PRODUCT-LEVEL license state (one row per publication).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_rating_history_v1_builds (
    publication_id           uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint        char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date               date NOT NULL,
    observation_input_count  integer NOT NULL CHECK (observation_input_count >= 0),
    -- Product-level license gate. When the license is NOT verified the WHOLE product
    -- is 'not_applicable' with reason 'no_licensed_source' and zero snapshot rows.
    product_state text NOT NULL CHECK (product_state IN ('active', 'not_applicable')),
    reason_code              text,
    license_verified boolean NOT NULL,
    created_at               timestamptz NOT NULL DEFAULT now(),
    -- active iff the license is verified; a reason is present iff NOT active.
    CHECK ((product_state = 'active') = (license_verified)),
    CHECK ((reason_code IS NULL) = (product_state = 'active'))
);

CREATE OR REPLACE FUNCTION bond_rating_history_v1_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_rating_history_v1 build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_rating_history_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_rating_history_v1 build requires a prepared bond_rating_history_v1 publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_rating_history_v1_build_guard ON bond_rating_history_v1_builds;
CREATE TRIGGER bond_rating_history_v1_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_rating_history_v1_builds
FOR EACH ROW EXECUTE FUNCTION bond_rating_history_v1_build_guard();

-- ---------------------------------------------------------------------------
-- Published snapshot: PIT rating rows. Rows exist ONLY under a verified license
-- (license_verified is CHECK-forced true); agency stays opaque (agency_code only).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_rating_history_v1 (
    publication_id       uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id        uuid NOT NULL,
    subject_kind         text NOT NULL CHECK (subject_kind IN ('security', 'issuer')),
    subject_ref          text NOT NULL CHECK (subject_ref <> ''),
    security_id          uuid,
    issuer_id            text,
    agency_code text NOT NULL,
    rating               text NOT NULL CHECK (rating <> ''),
    watch                text,
    outlook              text,
    valid_from           date NOT NULL,
    valid_to             date,
    license_verified boolean NOT NULL CHECK (license_verified),
    licensed_source_ref  text NOT NULL CHECK (licensed_source_ref <> ''),
    measured_at          date NOT NULL,
    methodology_version  text NOT NULL DEFAULT 'bond_rating_history_v1',
    provenance           jsonb NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, subject_kind, subject_ref, agency_code, valid_from),
    CHECK (methodology_version = 'bond_rating_history_v1'),
    CHECK ((subject_kind = 'security') = (security_id IS NOT NULL)),
    CHECK ((subject_kind = 'issuer') = (issuer_id IS NOT NULL)),
    CHECK (subject_ref = COALESCE(security_id::text, issuer_id)),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (jsonb_typeof(provenance) = 'object')
);

-- Shared write guard: insert-only, only while the parent publication is 'prepared',
-- and pinned to the build as_of (measured_at).
CREATE OR REPLACE FUNCTION bond_rating_history_v1_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
        pinned_license boolean;
        pinned_product_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_rating_history_v1 snapshot is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_rating_history_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_rating_history_v1 snapshot requires a prepared bond_rating_history_v1 publication';
    END IF;
    SELECT as_of_date, license_verified, product_state
      INTO pinned_as_of, pinned_license, pinned_product_state
    FROM bond_rating_history_v1_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'bond_rating_history_v1 snapshot requires matching pinned build metadata';
    END IF;
    -- LICENSE GATE (DB-level, cross-table): a rating snapshot row can exist ONLY
    -- under a build whose license is verified (product_state='active').  This
    -- forbids rows from coexisting with a not_applicable / license_verified=false
    -- build, so the "no verified license => zero rating rows" invariant is enforced
    -- by the DB, not only by the Python materializer.  The row's own license_verified
    -- CHECK forces the flag true; this trigger ties the row to its build so the flag
    -- can never be forged past an unlicensed product.
    IF pinned_license IS DISTINCT FROM true OR pinned_product_state IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'bond_rating_history_v1 snapshot requires a license-verified build';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_rating_history_v1_write_guard ON bond_rating_history_v1;
CREATE TRIGGER bond_rating_history_v1_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_rating_history_v1
FOR EACH ROW EXECUTE FUNCTION bond_rating_history_v1_write_guard();

-- ---------------------------------------------------------------------------
-- Current-pointer views (never reach back into raw/observation tables).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW sec_current_bond_rating_history_v1 AS
SELECT s.*
FROM sec_derived_current_pointers c
JOIN bond_rating_history_v1 s ON s.publication_id = c.publication_id
WHERE c.product = 'bond_rating_history_v1';

-- Product-state view: exposes the license gate outcome (active / not_applicable +
-- reason 'no_licensed_source') for the CURRENT publication, so a consumer (and the
-- Task-6 gate) can see WHY the product is empty when it is not_applicable.
CREATE OR REPLACE VIEW sec_current_bond_rating_history_v1_status AS
SELECT b.product_state, b.license_verified, b.reason_code, b.publication_id,
       b.observation_input_count
FROM sec_derived_current_pointers c
JOIN bond_rating_history_v1_builds b ON b.publication_id = c.publication_id
WHERE c.product = 'bond_rating_history_v1';
