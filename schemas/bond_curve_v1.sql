-- Point-in-time bond spot/par curve observations (bond_curve_v1) with an
-- immutable observation layer + one published snapshot promoted atomically
-- through the shared sec_derived_publications protocol.
--
-- Three layers (mirroring the Task 3/4 bond_security_v1 / bond_price_observation_v1
-- conventions):
--   * bond_curve_observation — immutable observed curves, one row per observed
--     (currency, curve_date, curve_type) curve.  The node set is carried as a raw
--     JSONB array EXACTLY as observed; qualification/validation happens downstream.
--     The row is insert-only (an immutability trigger rejects UPDATE/DELETE),
--     mirroring the *_observation inputs a publication is built from.  Absence is
--     honest; nothing is fabricated.
--   * bond_curve_v1 (+ bond_curve_node_v1) — one published snapshot promoted
--     atomically (prepared -> validated -> current pointer).  Child rows may only
--     be written while the parent publication is 'prepared', pinned to a
--     product-salted input fingerprint (bond_curve_v1_builds), so reruns are
--     idempotent and a partial build can never become current.  Only NON-DEGENERATE
--     curves are published: a degenerate observed curve (fewer than 2 nodes, a
--     non-increasing tenor, a non-finite rate, a non-positive tenor, or an
--     unsupported interpolation) carries a typed curve_state and is NEVER published
--     (materialized by src/bonds/curves.py).
--
-- Curve identity, materialized by src/bonds/curves.py:
--     curve_id = uuid5(NAMESPACE_BOND_CURVE, curve_key)
--   where curve_key = currency || '|' || curve_date (ISO) || '|' || curve_type.
--   The key never depends on the as_of, source run, or observed rates.
--
-- Interpolation is a DECLARED ATTRIBUTE of the snapshot ('linear' only for now).
-- The published nodes feed src.bonds.pricing.SpotCurve WITHOUT adaptation: SpotCurve
-- interpolates linearly in the zero rate between nodes and is flat outside the node
-- range (ACT/365F time is the consumer's convention, not the curve's).
--
-- The DDL is idempotent (CREATE ... IF NOT EXISTS, CREATE OR REPLACE, guarded
-- DO-block for the domain) so the worker's install_schema step may re-apply it.

-- ---------------------------------------------------------------------------
-- Curve-type discriminator domain (typed CHECK on the curve_type column).
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE DOMAIN bond_curve_type AS text CHECK (VALUE IN ('spot', 'par'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ---------------------------------------------------------------------------
-- Immutable curve observations (publication inputs).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_curve_observation (
    observation_id        uuid PRIMARY KEY,
    as_of                 date NOT NULL,
    curve_date            date NOT NULL,
    source_run_id         uuid NOT NULL,
    currency              text NOT NULL CHECK (currency <> ''),
    curve_type            text NOT NULL CHECK (curve_type IN ('spot', 'par')),
    -- Interpolation is a declared attribute; only 'linear' is supported for now.
    interpolation         text NOT NULL CHECK (interpolation <> ''),
    -- Node set exactly as observed: a JSONB array of [tenor_years, rate] pairs.
    -- Validation (increasing tenor, finite rate, >= 2 nodes, supported
    -- interpolation) happens downstream; a degenerate curve is typed, not repaired.
    nodes                 jsonb NOT NULL,
    source_lineage        jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    CHECK (jsonb_typeof(nodes) = 'array' AND nodes <> '[]'::jsonb),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

CREATE OR REPLACE FUNCTION bond_curve_observation_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bond_curve_observation is immutable';
END $$;

DROP TRIGGER IF EXISTS bond_curve_observation_immutable ON bond_curve_observation;
CREATE TRIGGER bond_curve_observation_immutable
BEFORE UPDATE OR DELETE ON bond_curve_observation
FOR EACH ROW EXECUTE FUNCTION bond_curve_observation_immutable();

-- ---------------------------------------------------------------------------
-- Product-salted build pin (one row per publication).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_curve_v1_builds (
    publication_id           uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint        char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date               date NOT NULL,
    observation_input_count  integer NOT NULL CHECK (observation_input_count >= 0),
    created_at               timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION bond_curve_v1_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_curve_v1 build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_curve_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_curve_v1 build requires a prepared bond_curve_v1 publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_curve_v1_build_guard ON bond_curve_v1_builds;
CREATE TRIGGER bond_curve_v1_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_curve_v1_builds
FOR EACH ROW EXECUTE FUNCTION bond_curve_v1_build_guard();

-- ---------------------------------------------------------------------------
-- Published snapshot: curves (parent) + typed nodes (child).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_curve_v1 (
    publication_id        uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id         uuid NOT NULL,
    curve_id              uuid NOT NULL,
    curve_key             text NOT NULL CHECK (curve_key <> ''),
    currency              text NOT NULL CHECK (currency <> ''),
    curve_date            date NOT NULL,
    curve_type            text NOT NULL CHECK (curve_type IN ('spot', 'par')),
    -- Declared interpolation attribute of the snapshot ('linear' only for now).
    interpolation         text NOT NULL CHECK (interpolation IN ('linear')),
    -- A published curve is never degenerate: at least two strictly increasing nodes.
    node_count            integer NOT NULL CHECK (node_count >= 2),
    measured_at           date NOT NULL,
    methodology_version   text NOT NULL DEFAULT 'bond_curve_v1',
    provenance            jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, curve_id),
    CHECK (methodology_version = 'bond_curve_v1'),
    CHECK (jsonb_typeof(provenance) = 'object')
);

CREATE TABLE IF NOT EXISTS bond_curve_node_v1 (
    publication_id        uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    curve_id              uuid NOT NULL,
    node_ordinal          integer NOT NULL CHECK (node_ordinal >= 0),
    tenor_years           numeric NOT NULL,
    rate                  numeric NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, curve_id, tenor_years),
    FOREIGN KEY (publication_id, curve_id) REFERENCES bond_curve_v1(publication_id, curve_id) ON DELETE RESTRICT,
    -- Typed node: strictly positive tenor and a finite rate (never NaN/±Infinity).
    CHECK (tenor_years > 0),
    CHECK (rate <> 'NaN'::numeric AND rate <> 'Infinity'::numeric AND rate <> '-Infinity'::numeric)
);

-- Shared write guard for the snapshot tables: insert-only, only while the parent
-- publication is 'prepared', and (for the curve parent row) pinned to the build as_of.
CREATE OR REPLACE FUNCTION bond_curve_v1_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_curve_v1 snapshot is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_curve_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_curve_v1 snapshot requires a prepared bond_curve_v1 publication';
    END IF;
    IF TG_TABLE_NAME = 'bond_curve_v1' THEN
        SELECT as_of_date INTO pinned_as_of FROM bond_curve_v1_builds WHERE publication_id = NEW.publication_id;
        IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
            RAISE EXCEPTION 'bond_curve_v1 snapshot requires matching pinned build metadata';
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_curve_v1_write_guard ON bond_curve_v1;
CREATE TRIGGER bond_curve_v1_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_curve_v1
FOR EACH ROW EXECUTE FUNCTION bond_curve_v1_write_guard();

DROP TRIGGER IF EXISTS bond_curve_node_v1_write_guard ON bond_curve_node_v1;
CREATE TRIGGER bond_curve_node_v1_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_curve_node_v1
FOR EACH ROW EXECUTE FUNCTION bond_curve_v1_write_guard();

-- ---------------------------------------------------------------------------
-- Current-pointer views (never reach back into raw/observation tables).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW sec_current_bond_curve_v1 AS
SELECT s.*
FROM sec_derived_current_pointers c
JOIN bond_curve_v1 s ON s.publication_id = c.publication_id
WHERE c.product = 'bond_curve_v1';

CREATE OR REPLACE VIEW sec_current_bond_curve_node_v1 AS
SELECT n.*
FROM sec_derived_current_pointers c
JOIN bond_curve_node_v1 n ON n.publication_id = c.publication_id
WHERE c.product = 'bond_curve_v1';
