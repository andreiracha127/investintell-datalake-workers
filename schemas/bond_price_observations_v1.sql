-- Point-in-time bond price/trade observations (bond_price_observation_v1) with
-- two structurally isolated published lanes.
--
-- Three layers (mirroring the Task 3 bond_security_v1 conventions):
--   * bond_price_observation — immutable price/trade observations, one row per
--     observed (security-lot, observation_date, source row). Prices absent from a
--     source are NULL with a typed price_state; accrued-interest treatment and
--     price_type are typed ('not_reported' when unknown). A row is insert-only (an
--     immutability trigger rejects UPDATE/DELETE), exactly like the *_observation
--     inputs a publication is built from. Identity is resolved to a security_id via
--     the Task 3 identity (uuid5(NAMESPACE_BOND_SECURITY, 'cusip9:'||<normalized>))
--     when the CUSIP normalizes losslessly; an observation whose identifier cannot
--     be qualified carries no fabricated security_id — it keeps its raw identifier
--     and an 'unresolved' identity_state with the IdentifierState reason, and is
--     never published to a lane.
--   * bond_price_observation_v1 — one published snapshot promoted atomically through
--     the shared sec_derived_publications protocol (prepared -> validated -> current
--     pointer). Child rows may only be written while the parent publication is
--     'prepared', pinned to a product-salted input fingerprint
--     (bond_price_observation_v1_builds), so reruns are idempotent and a partial
--     build can never become current. Duplicate (security_id, observation_date) rows
--     from distinct source rows are ALL retained and marked
--     daily_key_state='duplicate_in_matching_cohort' (panel_states semantics) — no
--     arbitrary winner is chosen.
--   * Two published lanes, NEVER interchangeable (spec §2):
--       - latest lane  (bond_price_latest_v1): the most-recent observation per
--         security. INFORMATIVE only. Every row is stamped lane='latest'.
--       - fund_asof lane (bond_price_fund_asof_v1(fund_as_of date)): point-in-time
--         access. For each security it returns the most-recent observation with
--         observation_date <= fund_as_of (NO look-ahead), stamped lane='fund_asof',
--         with observation_age_days and an is_stale flag (age >= 31 days, consistent
--         with MatchState.STALE). The lane discriminator is typed by the
--         bond_price_lane domain (CHECK VALUE IN ('latest','fund_asof')) and each
--         lane hardcodes its literal, so the fund_asof access path can NEVER emit a
--         'latest'-stamped row (structural refusal, mirrored in Python by
--         price_observations.build_pit_index, which refuses any non-fund_asof lane).
--
-- The DDL is idempotent (CREATE ... IF NOT EXISTS, CREATE OR REPLACE, guarded
-- DO-block for the domain) so the worker's install_schema step may re-apply it.

-- ---------------------------------------------------------------------------
-- Lane discriminator domain (mandatory, typed CHECK on the lane column).
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE DOMAIN bond_price_lane AS text CHECK (VALUE IN ('latest', 'fund_asof'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ---------------------------------------------------------------------------
-- Immutable price/trade observations (publication inputs).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_price_observation (
    observation_id        uuid PRIMARY KEY,
    as_of                 date NOT NULL,
    observation_date      date NOT NULL,
    source_run_id         uuid NOT NULL,
    -- Identity: security_id resolved via Task 3 identity when the CUSIP qualifies;
    -- an unresolvable identity carries no fabricated security_id (raw + state kept).
    security_id           uuid,
    cusip9_input          text,
    normalized_cusip9     text,
    identity_state        text NOT NULL CHECK (identity_state IN ('resolved', 'unresolved')),
    identity_reason_code  text,
    -- Price value(s). Absence is honest (NULL + typed state); never fabricated.
    price                 numeric,
    price_state           text NOT NULL CHECK (price_state IN ('present', 'null', 'invalid')),
    price_type            text NOT NULL CHECK (price_type IN ('trade', 'evaluated', 'model', 'not_reported')),
    -- Accrued-interest treatment: clean excludes accrued, dirty includes it,
    -- not_reported when the source does not state it. Typed; never fabricated.
    accrued_treatment     text NOT NULL CHECK (accrued_treatment IN ('clean', 'dirty', 'not_reported')),
    ytm                   numeric,
    db_type               numeric,
    db_type_state         text NOT NULL CHECK (db_type_state IN ('present', 'null', 'invalid')),
    -- Quality/ambiguity: panel_states daily-key state over the (security, date)
    -- cohort. Duplicate keys stay 'duplicate_in_matching_cohort' (ambiguous).
    daily_key_state       text NOT NULL CHECK (daily_key_state IN (
        'unique_in_matching_cohort', 'duplicate_in_matching_cohort',
        'not_in_matching_cohort', 'invalid_key')),
    source_row_number     integer NOT NULL,
    source_lineage        jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    -- Identity invariants: a security_id and normalized CUSIP exist exactly when
    -- resolved; a reason code exists exactly when NOT resolved (Task 3 discipline).
    CHECK ((identity_state = 'resolved') = (security_id IS NOT NULL)),
    CHECK ((identity_state = 'resolved') = (normalized_cusip9 IS NOT NULL)),
    CHECK ((identity_state = 'resolved') = (identity_reason_code IS NULL)),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

CREATE OR REPLACE FUNCTION bond_price_observation_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bond_price_observation is immutable';
END $$;

DROP TRIGGER IF EXISTS bond_price_observation_immutable ON bond_price_observation;
CREATE TRIGGER bond_price_observation_immutable
BEFORE UPDATE OR DELETE ON bond_price_observation
FOR EACH ROW EXECUTE FUNCTION bond_price_observation_immutable();

-- ---------------------------------------------------------------------------
-- Product-salted build pin (one row per publication).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_price_observation_v1_builds (
    publication_id           uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint        char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date               date NOT NULL,
    observation_input_count  integer NOT NULL CHECK (observation_input_count >= 0),
    created_at               timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION bond_price_observation_v1_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_price_observation_v1 build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_price_observation_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_price_observation_v1 build requires a prepared bond_price_observation_v1 publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_price_observation_v1_build_guard ON bond_price_observation_v1_builds;
CREATE TRIGGER bond_price_observation_v1_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_price_observation_v1_builds
FOR EACH ROW EXECUTE FUNCTION bond_price_observation_v1_build_guard();

-- ---------------------------------------------------------------------------
-- Published snapshot: the full PIT price panel (resolved observations only).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_price_observation_v1 (
    publication_id        uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id         uuid NOT NULL,
    security_id           uuid NOT NULL,
    observation_date      date NOT NULL,
    source_row_number     integer NOT NULL,
    price                 numeric,
    price_state           text NOT NULL CHECK (price_state IN ('present', 'null', 'invalid')),
    price_type            text NOT NULL CHECK (price_type IN ('trade', 'evaluated', 'model', 'not_reported')),
    accrued_treatment     text NOT NULL CHECK (accrued_treatment IN ('clean', 'dirty', 'not_reported')),
    ytm                   numeric,
    db_type               numeric,
    db_type_state         text NOT NULL CHECK (db_type_state IN ('present', 'null', 'invalid')),
    -- is_144a derived from db_type == 3 when db_type is present & integral, else NULL
    -- (matching._is_144a semantics); never fabricated.
    is_144a               boolean,
    daily_key_state       text NOT NULL CHECK (daily_key_state IN (
        'unique_in_matching_cohort', 'duplicate_in_matching_cohort',
        'not_in_matching_cohort', 'invalid_key')),
    measured_at           date NOT NULL,
    methodology_version   text NOT NULL DEFAULT 'bond_price_observation_v1',
    provenance            jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    -- All distinct source rows for a (security, date) are retained: the source row
    -- number is part of the key so duplicates coexist and neither is dropped.
    PRIMARY KEY (publication_id, security_id, observation_date, source_row_number),
    CHECK (methodology_version = 'bond_price_observation_v1'),
    CHECK (jsonb_typeof(provenance) = 'object')
);

-- Shared write guard: insert-only, only while the parent publication is 'prepared',
-- and pinned to the build as_of (measured_at).
CREATE OR REPLACE FUNCTION bond_price_observation_v1_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_price_observation_v1 snapshot is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'bond_price_observation_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_price_observation_v1 snapshot requires a prepared bond_price_observation_v1 publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM bond_price_observation_v1_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'bond_price_observation_v1 snapshot requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_price_observation_v1_write_guard ON bond_price_observation_v1;
CREATE TRIGGER bond_price_observation_v1_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_price_observation_v1
FOR EACH ROW EXECUTE FUNCTION bond_price_observation_v1_write_guard();

-- ---------------------------------------------------------------------------
-- Lane 1: latest (INFORMATIVE). Most-recent observation per security. The lane
-- literal is hardcoded and typed by bond_price_lane; this view can only ever emit
-- 'latest'-stamped rows. It is NEVER a point-in-time source (see build_pit_index).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW bond_price_latest_v1 AS
SELECT
    'latest'::bond_price_lane AS lane,
    s.publication_id,
    s.security_id,
    s.observation_date,
    s.source_row_number,
    s.price,
    s.price_state,
    s.price_type,
    s.accrued_treatment,
    s.ytm,
    s.db_type,
    s.db_type_state,
    s.is_144a,
    s.daily_key_state
FROM sec_derived_current_pointers c
JOIN bond_price_observation_v1 s ON s.publication_id = c.publication_id
JOIN (
    SELECT s2.security_id, max(s2.observation_date) AS latest_date
    FROM sec_derived_current_pointers c2
    JOIN bond_price_observation_v1 s2 ON s2.publication_id = c2.publication_id
    WHERE c2.product = 'bond_price_observation_v1'
    GROUP BY s2.security_id
) latest ON latest.security_id = s.security_id AND latest.latest_date = s.observation_date
WHERE c.product = 'bond_price_observation_v1';

-- ---------------------------------------------------------------------------
-- Lane 2: fund_asof (POINT-IN-TIME, no look-ahead). Parameterized by the fund's
-- as-of date. For each security it returns the most-recent observation with
-- observation_date <= fund_as_of; all duplicate rows on that date are retained.
-- The lane literal is hardcoded 'fund_asof' and typed by bond_price_lane: the PIT
-- access path can NEVER return a 'latest'-stamped row. is_stale marks age >= 31d.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_price_fund_asof_v1(fund_as_of date)
RETURNS TABLE (
    lane                  bond_price_lane,
    publication_id        uuid,
    security_id           uuid,
    observation_date      date,
    source_row_number     integer,
    price                 numeric,
    price_state           text,
    price_type            text,
    accrued_treatment     text,
    ytm                   numeric,
    db_type               numeric,
    db_type_state         text,
    is_144a               boolean,
    daily_key_state       text,
    observation_age_days  integer,
    is_stale              boolean
) LANGUAGE sql STABLE AS $$
    SELECT
        'fund_asof'::bond_price_lane AS lane,
        s.publication_id,
        s.security_id,
        s.observation_date,
        s.source_row_number,
        s.price,
        s.price_state,
        s.price_type,
        s.accrued_treatment,
        s.ytm,
        s.db_type,
        s.db_type_state,
        s.is_144a,
        s.daily_key_state,
        (fund_as_of - s.observation_date) AS observation_age_days,
        ((fund_as_of - s.observation_date) >= 31) AS is_stale
    FROM sec_derived_current_pointers c
    JOIN bond_price_observation_v1 s ON s.publication_id = c.publication_id
    JOIN (
        SELECT s2.security_id, max(s2.observation_date) AS asof_date
        FROM sec_derived_current_pointers c2
        JOIN bond_price_observation_v1 s2 ON s2.publication_id = c2.publication_id
        WHERE c2.product = 'bond_price_observation_v1'
          AND s2.observation_date <= fund_as_of
        GROUP BY s2.security_id
    ) asof ON asof.security_id = s.security_id AND asof.asof_date = s.observation_date
    WHERE c.product = 'bond_price_observation_v1'
      AND s.observation_date <= fund_as_of;
$$;
