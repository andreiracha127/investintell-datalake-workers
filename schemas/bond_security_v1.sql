-- Point-in-time bond security master (bond_security_v1).
--
-- Two layers:
--   * bond_security_observation — immutable identity/terms observations, one row
--     per observed security-lot as seen in an N-PORT-shaped source. Missing terms
--     are NULL / 'not_reported'; a term is NEVER fabricated. The row is insert-only
--     (an immutability trigger rejects UPDATE/DELETE), mirroring the mixed_quant
--     *_observation convention (immutable inputs a publication is built from).
--   * bond_security_v1 + bond_security_alias_v1 — one published snapshot promoted
--     atomically through the shared sec_derived_publications protocol
--     (prepared -> validated -> current pointer). Child rows may only be written
--     while the parent publication is 'prepared', pinned to a product-salted input
--     fingerprint (bond_security_v1_builds), so reruns are idempotent and a partial
--     build can never become current.
--
-- Identity key (spec §1), materialized by src/bonds/security_master.py:
--     security_id = uuid5(NAMESPACE_BOND_SECURITY, identity_key)
--   where identity_key is the first qualified identifier in this precedence:
--     1. 'cusip9:' || <directly observed lossless normalized CUSIP9>
--     2. 'cusip9:' || <CUSIP9 anchored from a US/CA ISIN: positional NSIN
--                      (chars 3-11) re-qualified through normalize_cusip9>
--     3. 'isin:'   || <qualified ISIN>   (non-US/CA ISIN, or a US/CA ISIN whose
--                      embedded NSIN normalize_cusip9 rejects)
--   Because a US/CA ISIN embeds its CUSIP9, anchoring makes the key cusip9-based
--   regardless of which field carried the identifier, so security_id is stable
--   across the ISIN-only -> CUSIP9 arrival transition (one instrument never
--   splits into two securities). A security identified only by a non-US/CA ISIN
--   keeps a stable 'isin:' key. The key never depends on as_of, source run, or
--   terms. Placeholders/synthetic prefixes are rejected upstream by
--   normalize_cusip9 and never become an identity or an alias. Conflicting
--   evidence for one identity (e.g. one CUSIP9 with two ISINs at a point in time)
--   is published as an explicit 'ambiguous' state with the conflicting values in
--   identity_evidence — never an arbitrary winner.
--
-- The DDL is idempotent (CREATE ... IF NOT EXISTS, CREATE OR REPLACE) so the
-- worker's install_schema step may apply it repeatedly.

-- ---------------------------------------------------------------------------
-- Immutable identity/terms observations (publication inputs).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_security_observation (
    observation_id        uuid PRIMARY KEY,
    as_of                 date NOT NULL,
    observation_date      date NOT NULL,
    source_run_id         uuid NOT NULL,
    -- Identity inputs exactly as observed (raw); qualification happens downstream.
    cusip9_input          text,
    isin_input            text,
    -- Terms best-effort (N-PORT debt extension shape). Absent term = NULL /
    -- 'not_reported'; never fabricated.
    issuer_name           text,
    currency              text,
    coupon_type           text,
    coupon_rate           numeric,
    coupon_schedule       jsonb,
    maturity_date         date,
    seniority             text,
    secured               text CHECK (secured IS NULL OR secured IN ('secured','unsecured','not_reported')),
    call_schedule         jsonb,
    put_schedule          jsonb,
    is_144a               text CHECK (is_144a IS NULL OR is_144a IN ('true','false','not_reported')),
    day_count             text,
    settlement_convention text,
    source_lineage        jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    CHECK (coupon_schedule IS NULL OR jsonb_typeof(coupon_schedule) = 'array'),
    CHECK (call_schedule IS NULL OR jsonb_typeof(call_schedule) = 'array'),
    CHECK (put_schedule IS NULL OR jsonb_typeof(put_schedule) = 'array'),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

CREATE OR REPLACE FUNCTION bond_security_observation_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bond_security_observation is immutable';
END $$;

DROP TRIGGER IF EXISTS bond_security_observation_immutable ON bond_security_observation;
CREATE TRIGGER bond_security_observation_immutable
BEFORE UPDATE OR DELETE ON bond_security_observation
FOR EACH ROW EXECUTE FUNCTION bond_security_observation_immutable();

-- ---------------------------------------------------------------------------
-- Product-salted build pin (one row per publication).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_security_v1_builds (
    publication_id           uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint        char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date               date NOT NULL,
    observation_input_count  integer NOT NULL CHECK (observation_input_count >= 0),
    created_at               timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION bond_security_v1_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_security_v1 build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_security_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_security_v1 build requires a prepared bond_security_v1 publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_security_v1_build_guard ON bond_security_v1_builds;
CREATE TRIGGER bond_security_v1_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_security_v1_builds
FOR EACH ROW EXECUTE FUNCTION bond_security_v1_build_guard();

-- ---------------------------------------------------------------------------
-- Published snapshot: securities.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_security_v1 (
    publication_id        uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id         uuid NOT NULL,
    security_id           uuid NOT NULL,
    identity_key          text NOT NULL CHECK (identity_key <> ''),
    identity_state        text NOT NULL CHECK (identity_state IN ('resolved','ambiguous')),
    identity_reason_code  text,
    issuer_name           text,
    currency              text,
    coupon_type           text,
    coupon_rate           numeric,
    maturity_date         date,
    seniority             text,
    secured               text CHECK (secured IS NULL OR secured IN ('secured','unsecured','not_reported')),
    is_144a               text CHECK (is_144a IS NULL OR is_144a IN ('true','false','not_reported')),
    day_count             text,
    settlement_convention text,
    terms                 jsonb NOT NULL,
    identity_evidence     jsonb NOT NULL,
    measured_at           date NOT NULL,
    methodology_version   text NOT NULL DEFAULT 'bond_security_v1',
    provenance            jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, security_id),
    CHECK (methodology_version = 'bond_security_v1'),
    CHECK (jsonb_typeof(terms) = 'object'),
    CHECK (jsonb_typeof(identity_evidence) = 'object'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    -- A reason code is present exactly when the identity is not resolved.
    CHECK ((identity_state = 'resolved') = (identity_reason_code IS NULL))
);

-- ---------------------------------------------------------------------------
-- Published snapshot: point-in-time aliases (CUSIP9/ISIN).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_security_alias_v1 (
    publication_id  uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    security_id     uuid NOT NULL,
    alias_kind      text NOT NULL CHECK (alias_kind IN ('cusip9','isin')),
    alias_value     text NOT NULL CHECK (alias_value <> ''),
    valid_from      date NOT NULL,
    valid_to        date,
    source_lineage  jsonb NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, security_id, alias_kind, alias_value, valid_from),
    FOREIGN KEY (publication_id, security_id) REFERENCES bond_security_v1(publication_id, security_id) ON DELETE RESTRICT,
    -- PIT window convention: HALF-OPEN [valid_from, valid_to). valid_from is
    -- INCLUSIVE; valid_to is EXCLUSIVE and open when NULL. A window closes at the
    -- successor's valid_from, i.e. a superseded alias's valid_to EQUALS the next
    -- alias's valid_from (the boundary date belongs to the successor). Windows are
    -- therefore non-empty: valid_to (when present) is strictly after valid_from.
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

-- Shared write guard for the snapshot tables: insert-only, only while the parent
-- publication is 'prepared', and (for the security row) pinned to the build as_of.
CREATE OR REPLACE FUNCTION bond_security_v1_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_security_v1 snapshot is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_security_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_security_v1 snapshot requires a prepared bond_security_v1 publication';
    END IF;
    IF TG_TABLE_NAME = 'bond_security_v1' THEN
        SELECT as_of_date INTO pinned_as_of FROM bond_security_v1_builds WHERE publication_id = NEW.publication_id;
        IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
            RAISE EXCEPTION 'bond_security_v1 snapshot requires matching pinned build metadata';
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_security_v1_write_guard ON bond_security_v1;
CREATE TRIGGER bond_security_v1_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_security_v1
FOR EACH ROW EXECUTE FUNCTION bond_security_v1_write_guard();

DROP TRIGGER IF EXISTS bond_security_alias_v1_write_guard ON bond_security_alias_v1;
CREATE TRIGGER bond_security_alias_v1_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_security_alias_v1
FOR EACH ROW EXECUTE FUNCTION bond_security_v1_write_guard();

-- ---------------------------------------------------------------------------
-- Current-pointer views (never reach back into raw/observation tables).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW sec_current_bond_security_v1 AS
SELECT s.*
FROM sec_derived_current_pointers c
JOIN bond_security_v1 s ON s.publication_id = c.publication_id
WHERE c.product = 'bond_security_v1';

CREATE OR REPLACE VIEW sec_current_bond_security_alias_v1 AS
SELECT a.*
FROM sec_derived_current_pointers c
JOIN bond_security_alias_v1 a ON a.publication_id = c.publication_id
WHERE c.product = 'bond_security_v1';
