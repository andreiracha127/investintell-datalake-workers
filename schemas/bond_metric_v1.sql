-- Per-security Wave-1 metric product (bond_metric_v1).
--
-- One published snapshot of independently RECOMPUTED per-security metrics,
-- promoted atomically through the shared sec_derived_publications protocol
-- (prepared -> validated -> current pointer), mirroring the sibling
-- bond_price_observations_v1 builds/current/guard structure:
--   * bond_metric_v1_builds — the product-salted build pin (one row per
--     publication) so reruns are idempotent and a partial build can never
--     become current;
--   * bond_metric_v1 — one row per (publication_id, security_id, metric_id).
--     The metric vocabulary is CLOSED to the Wave-1 set enforced by the CHECK
--     below; nothing outside it can ever be written to this product.
--   * sec_current_bond_metric_v1 — the current-pointer view (never reaches
--     back into raw/observation tables).
--
-- Honesty invariants (program Constraint 4 — no silent NaN, no synthetic zero):
--   * a value exists exactly when status='available' (CHECK-enforced);
--   * a typed reason code exists exactly when the status is a typed
--     engine/terms refusal (CHECK-enforced);
--   * rows are insert-only while the parent publication is 'prepared' and
--     pinned to the build as_of — the published snapshot is immutable.
--
-- The DDL is idempotent (CREATE ... IF NOT EXISTS, CREATE OR REPLACE) so the
-- worker's install_schema step may re-apply it.

-- ---------------------------------------------------------------------------
-- Product-salted build pin (one row per publication).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_metric_v1_builds (
    publication_id        uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint     char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date            date NOT NULL,
    security_input_count  integer NOT NULL CHECK (security_input_count >= 0),
    metric_row_count      integer NOT NULL CHECK (metric_row_count >= 0),
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION bond_metric_v1_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_metric_v1 build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_metric_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_metric_v1 build requires a prepared bond_metric_v1 publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_metric_v1_build_guard ON bond_metric_v1_builds;
CREATE TRIGGER bond_metric_v1_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_metric_v1_builds
FOR EACH ROW EXECUTE FUNCTION bond_metric_v1_build_guard();

-- ---------------------------------------------------------------------------
-- Published snapshot: one typed row per (publication, security, metric).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_metric_v1 (
    publication_id       uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    security_id          uuid NOT NULL,
    -- The CLOSED metric vocabulary. Extended 2026-08-07 with
    -- security_effective_duration + latest_price_pct (see the ALTER below, which
    -- is what actually widens an ALREADY-CREATED table: the inline CHECK in a
    -- CREATE TABLE IF NOT EXISTS is a no-op on every re-apply).
    metric_id            text NOT NULL CHECK (metric_id IN (
        'security_ytm', 'security_ytw', 'current_yield', 'wal',
        'security_effective_duration', 'latest_price_pct')),
    -- Yields are decimal fractions per annum; wal is years. NULL unless the
    -- row is 'available' (CHECK below) — absence is honest, never a zero.
    value                numeric,
    status               text NOT NULL CHECK (status IN (
        'available', 'no_eligible_price', 'terms_insufficient',
        'engine_typed_error', 'gate_not_passed')),
    -- The typed engine BondError code (engine_typed_error) or the typed terms
    -- reason code (terms_insufficient); NULL for every other status.
    engine_error_code    text,
    as_of                date NOT NULL,
    methodology_version  text NOT NULL DEFAULT 'bond_metric_v1',
    provenance           jsonb NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, security_id, metric_id),
    CHECK (methodology_version = 'bond_metric_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    -- A value exists exactly when the metric is available (no silent NaN/zero).
    CHECK ((status = 'available') = (value IS NOT NULL)),
    -- A typed reason code exists exactly when the status carries one.
    CHECK ((status IN ('engine_typed_error', 'terms_insufficient'))
           = (engine_error_code IS NOT NULL))
);

-- ---------------------------------------------------------------------------
-- Vocabulary migration (2026-08-07): widen the CLOSED metric_id set in place.
--
-- CREATE TABLE IF NOT EXISTS never revisits an existing table's constraints, so
-- editing the inline CHECK above only governs a table created from scratch. This
-- block is what widens the live table, and it is idempotent by construction:
-- the constraint is dropped by name and re-added every time the DDL is applied.
-- The vocabulary stays CLOSED — the two additions are named explicitly and
-- nothing outside the six ids can ever be written.
--   security_effective_duration — modified duration (years), analytic closed
--     form over the published coupon/maturity and the observed price-date yield.
--   latest_price_pct            — the eligible latest clean price, % of par.
-- OAS, z-spread and a callable YTW remain DELIBERATELY absent: no validated
-- model publishes them, and an unmodelled number would be a guess.
-- ---------------------------------------------------------------------------
DO $$
DECLARE constraint_name text;
BEGIN
    IF to_regclass('bond_metric_v1') IS NULL THEN
        RETURN;
    END IF;
    FOR constraint_name IN
        SELECT con.conname
        FROM pg_constraint con
        WHERE con.conrelid = 'bond_metric_v1'::regclass
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) LIKE '%metric_id%'
    LOOP
        EXECUTE format('ALTER TABLE bond_metric_v1 DROP CONSTRAINT %I', constraint_name);
    END LOOP;
    ALTER TABLE bond_metric_v1 ADD CONSTRAINT bond_metric_v1_metric_id_check
        CHECK (metric_id IN (
            'security_ytm', 'security_ytw', 'current_yield', 'wal',
            'security_effective_duration', 'latest_price_pct'));
END $$;

-- Shared write guard: insert-only, only while the parent publication is
-- 'prepared', and pinned to the build as_of.
CREATE OR REPLACE FUNCTION bond_metric_v1_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_metric_v1 snapshot is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_metric_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_metric_v1 snapshot requires a prepared bond_metric_v1 publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM bond_metric_v1_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.as_of THEN
        RAISE EXCEPTION 'bond_metric_v1 snapshot requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_metric_v1_write_guard ON bond_metric_v1;
CREATE TRIGGER bond_metric_v1_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_metric_v1
FOR EACH ROW EXECUTE FUNCTION bond_metric_v1_write_guard();

-- ---------------------------------------------------------------------------
-- Current-pointer view (never reaches back into raw/observation tables).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW sec_current_bond_metric_v1 AS
SELECT s.*
FROM sec_derived_current_pointers c
JOIN bond_metric_v1 s ON s.publication_id = c.publication_id
WHERE c.product = 'bond_metric_v1';
