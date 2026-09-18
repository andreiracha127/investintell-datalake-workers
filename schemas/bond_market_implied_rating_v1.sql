-- Market-implied bond rating (bond_market_implied_rating_v1).
--
-- A FULL-REBUILD, monthly point-in-time classification of every candidate on
-- the served bond-panel snapshot grid: the market's own bucket (AAA..CCC), the
-- confirmed default state D, the withdrawn/never-rated terminal states, and the
-- default-event evidence the Light app's exposure estimator reads.
--
-- PROTOCOL -- the shared derived-publication ledger, exactly like every sibling
-- bond product (bond_security_v1, bond_metric_v1, bond_serving_v1):
--   * `sec_derived_publications` carries the identity, version, prepared ->
--     validated lifecycle and immutability, and `sec_derived_current_pointers`
--     carries the current pointer, advanced only through
--     `sec_set_current_derived_publication` (which also refuses an as_of
--     regression: the pointer never moves backward in panel month).
--   * `bond_market_implied_rating_v1_builds` pins THIS product's build: the
--     consumed panel publication, the frozen policy digest, the input
--     fingerprint/digests and the D counts. Rows may only be written while the
--     publication is `prepared` and the pin exists, so a partial build can
--     never become current.
--
-- The plan (docs/calibration/bond_market_implied_rating_round_declaration.md)
-- sketched bespoke `*_publications`/`*_app_pointer` TABLES. The real sibling
-- pattern is the shared ledger above, and mirroring it is deliberate: the
-- anchor run/package lineage, the immutability and delete guards, and the
-- monotonic pointer are enforced by one implementation the fleet already
-- trusts. The plan's read contract is preserved as the read-only VIEWS at the
-- bottom (`bond_market_implied_rating_publications`,
-- `bond_market_implied_rating_app_pointer`) so the consumer SQL in plan §5.1
-- works unchanged against either shape.
--
-- The worker's run() adds a compare-and-set check around the pointer: the
-- current pointer is read before the build and re-checked FOR UPDATE before
-- promotion, so a concurrent publication that moved it fails this run loudly
-- instead of silently overwriting.
--
-- The DDL is idempotent (CREATE ... IF NOT EXISTS / CREATE OR REPLACE) so the
-- worker's install_schema step may apply it repeatedly. It does NOT create the
-- service: the operational creation of a Railway service for replays is an
-- operator step (see railway.toml).

-- ---------------------------------------------------------------------------
-- Product-salted build pin (one row per publication).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_market_implied_rating_v1_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    panel_publication_id uuid NOT NULL REFERENCES bond_panel_publications(publication_id) ON DELETE RESTRICT,
    policy_version text NOT NULL CHECK (policy_version = 'bond_market_implied_rating_policy_v1'),
    policy_digest char(64) NOT NULL CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    code_revision text NOT NULL CHECK (btrim(code_revision) <> ''),
    panel_last_closed_month date NOT NULL,
    as_of_date date NOT NULL,
    first_month date NOT NULL,
    last_month date NOT NULL,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    -- The frozen calibration anchor this publication was built against. Pinned
    -- per publication so a drifted re-resolution is visible and refusable.
    l_anchor double precision NOT NULL CHECK (l_anchor = l_anchor),
    row_count integer NOT NULL CHECK (row_count > 0),
    rows_digest char(64) NOT NULL CHECK (rows_digest ~ '^[0-9a-f]{64}$'),
    d_confirmed_count integer NOT NULL CHECK (d_confirmed_count >= 0),
    d_candidate_count integer NOT NULL CHECK (d_candidate_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (policy_digest, input_fingerprint, code_revision),
    CHECK (first_month <= last_month AND last_month = panel_last_closed_month),
    CHECK (as_of_date = panel_last_closed_month)
);

-- ---------------------------------------------------------------------------
-- Published rows: one per (publication, month, cusip).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_market_implied_rating_v1 (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    month date NOT NULL,
    cusip_id text NOT NULL CHECK (btrim(cusip_id) <> ''),
    implied_bucket text NOT NULL CHECK (implied_bucket IN
        ('AAA','AA','A','BBB','BB','B','CCC','D','WITHDRAWN','NOT_RATED')),
    -- The two score layers, both NULL exactly when unwitnessed:
    --   spread_norm_log  = s = log(winsor(spread)) - b*(log(mod_dur) - log(d_ref))
    --   neutralized_score = x = s - beta*(L_t - L_anchor), what the state
    --                       machine consumes. Publishing both lets an audit
    --                       recompute either one from market_level_l + anchor.
    spread_norm_log double precision,
    neutralized_score double precision,
    market_level_l double precision,
    witnessed boolean NOT NULL,
    carry_months integer NOT NULL CHECK (carry_months >= 0),
    spell_id integer NOT NULL CHECK (spell_id >= 1),
    d_candidate boolean NOT NULL,
    d_confirmed boolean NOT NULL,
    d_event_month date,
    recovery_observed double precision,
    censoring text NOT NULL CHECK (censoring IN
        ('none','source_exit','withdrawal_absorbing','default_absorbing')),
    policy_version text NOT NULL CHECK (policy_version = 'bond_market_implied_rating_policy_v1'),
    policy_digest char(64) NOT NULL CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, month, cusip_id),
    -- The event annotation is present exactly on the rows that are in D state.
    CHECK ((d_confirmed AND d_event_month IS NOT NULL) OR (NOT d_confirmed AND d_event_month IS NULL)),
    CHECK ((implied_bucket = 'D') = d_confirmed),
    -- A RATED row is witnessed or carried; the terminal buckets are their own.
    CHECK (witnessed = (carry_months = 0) OR implied_bucket IN ('WITHDRAWN','NOT_RATED','D')),
    -- A witnessed month always carries both computable scores, NULL otherwise.
    CHECK ((spread_norm_log IS NULL) = (NOT witnessed)),
    CHECK ((neutralized_score IS NULL) = (NOT witnessed))
);

-- ---------------------------------------------------------------------------
-- Score-layer migration (2026-09-18): `neutralized_score` was added after the
-- first cut of this DDL. CREATE TABLE IF NOT EXISTS never revisits an existing
-- table, so this block is what reaches a table created before the change; the
-- column and its CHECK are added by name and the block is idempotent.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    column_was_missing boolean;
BEGIN
    IF to_regclass('bond_market_implied_rating_v1') IS NULL THEN
        RETURN;
    END IF;
    SELECT NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'bond_market_implied_rating_v1'
          AND column_name = 'neutralized_score'
    ) INTO column_was_missing;
    ALTER TABLE bond_market_implied_rating_v1
        ADD COLUMN IF NOT EXISTS neutralized_score double precision;
    -- The named CHECK is only needed by a table created before this change:
    -- a freshly created table already carries the inline CHECK above, and
    -- adding the named one too would duplicate it.
    IF column_was_missing AND NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bond_market_implied_rating_v1'::regclass
          AND conname = 'bond_market_implied_rating_v1_neutralized_witnessed'
    ) THEN
        ALTER TABLE bond_market_implied_rating_v1
            ADD CONSTRAINT bond_market_implied_rating_v1_neutralized_witnessed
            CHECK ((neutralized_score IS NULL) = (NOT witnessed));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS bond_market_implied_rating_v1_pub_month_idx
    ON bond_market_implied_rating_v1 (publication_id, month);
CREATE INDEX IF NOT EXISTS bond_market_implied_rating_v1_cusip_month_idx
    ON bond_market_implied_rating_v1 (cusip_id, month);

-- ---------------------------------------------------------------------------
-- Write guards: insert-only, only while the parent publication is prepared,
-- and (for rows) only when the product's build pin already exists.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_market_implied_rating_v1_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_market_implied_rating_v1 is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id
      AND product = 'bond_market_implied_rating_v1'
    FOR UPDATE;
    IF parent_state IS DISTINCT FROM 'prepared' THEN
        RAISE EXCEPTION 'bond_market_implied_rating_v1 write requires a prepared publication';
    END IF;
    IF TG_TABLE_NAME = 'bond_market_implied_rating_v1' THEN
        IF NOT EXISTS (
            SELECT 1 FROM bond_market_implied_rating_v1_builds
            WHERE publication_id = NEW.publication_id
        ) THEN
            RAISE EXCEPTION 'bond_market_implied_rating_v1 rows require the pinned build';
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_market_implied_rating_v1_builds_write_guard
    ON bond_market_implied_rating_v1_builds;
CREATE TRIGGER bond_market_implied_rating_v1_builds_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_market_implied_rating_v1_builds
FOR EACH ROW EXECUTE FUNCTION bond_market_implied_rating_v1_write_guard();

DROP TRIGGER IF EXISTS bond_market_implied_rating_v1_rows_write_guard
    ON bond_market_implied_rating_v1;
CREATE TRIGGER bond_market_implied_rating_v1_rows_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_market_implied_rating_v1
FOR EACH ROW EXECUTE FUNCTION bond_market_implied_rating_v1_write_guard();

-- ---------------------------------------------------------------------------
-- Read surfaces (never reach back into the raw panel tables).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW bond_market_implied_rating_v1_current AS
SELECT r.*
FROM sec_derived_current_pointers pointer
JOIN bond_market_implied_rating_v1 r ON r.publication_id = pointer.publication_id
WHERE pointer.product = 'bond_market_implied_rating_v1';

-- Plan §5.1 identity read: the product's own header, joined on the pointer.
CREATE OR REPLACE VIEW bond_market_implied_rating_publications AS
SELECT s.publication_id,
       s.product,
       s.lifecycle_state AS publication_status,
       NULL::text AS failure_reason,
       b.policy_version,
       b.policy_digest,
       b.code_revision,
       b.panel_publication_id,
       b.panel_last_closed_month,
       b.first_month,
       b.last_month,
       b.input_fingerprint,
       b.row_count,
       b.rows_digest,
       b.d_confirmed_count,
       b.d_candidate_count,
       s.prepared_at AS built_at,
       s.validated_at
FROM sec_derived_publications s
JOIN bond_market_implied_rating_v1_builds b USING (publication_id)
WHERE s.product = 'bond_market_implied_rating_v1';

CREATE OR REPLACE VIEW bond_market_implied_rating_app_pointer AS
SELECT product, publication_id, set_at AS changed_at
FROM sec_derived_current_pointers
WHERE product = 'bond_market_implied_rating_v1';

-- ---------------------------------------------------------------------------
-- Ownership and grants (the app_runtime SELECT grant is applied operationally,
-- like every product relation above it).
-- ---------------------------------------------------------------------------
ALTER TABLE bond_market_implied_rating_v1_builds OWNER TO worker_writer;
ALTER TABLE bond_market_implied_rating_v1 OWNER TO worker_writer;
ALTER FUNCTION bond_market_implied_rating_v1_write_guard() OWNER TO worker_writer;
ALTER VIEW bond_market_implied_rating_v1_current OWNER TO worker_writer;
ALTER VIEW bond_market_implied_rating_publications OWNER TO worker_writer;
ALTER VIEW bond_market_implied_rating_app_pointer OWNER TO worker_writer;
REVOKE ALL ON TABLE bond_market_implied_rating_v1_builds FROM PUBLIC;
REVOKE ALL ON TABLE bond_market_implied_rating_v1 FROM PUBLIC;
REVOKE ALL ON TABLE bond_market_implied_rating_v1_current FROM PUBLIC;
REVOKE ALL ON TABLE bond_market_implied_rating_publications FROM PUBLIC;
REVOKE ALL ON TABLE bond_market_implied_rating_app_pointer FROM PUBLIC;
