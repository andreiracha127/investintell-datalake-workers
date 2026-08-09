-- Neutral bond reference terms (bond_reference_terms).
--
-- WHAT THIS IS
--   A flat, replaceable reference table of instrument terms keyed by CUSIP9,
--   used ONLY to fill terms the publication chain does not carry. Measured on
--   the live curated cohort (2026-08-07): the chain reports coupon and maturity
--   almost completely (8 missing coupons, 0 missing maturities of 10,073) but
--   reports seniority, the secured flag, callability and amount outstanding for
--   ZERO securities. Those four are what this table exists to supply.
--
-- WHAT THIS IS NOT
--   * NOT a publication. It carries no publication_id, no lifecycle and no
--     pointer: it is an INPUT the security-master build reads, exactly like the
--     N-PORT holdings it already reads. The published, promoted artefact stays
--     bond_security_v1.
--   * NOT authoritative over the chain. ``apply_reference_terms`` fills gaps
--     only; a term the observations resolved is never overwritten.
--   * NOT a source name. The published basis token is the neutral
--     ``vendor_reference``; no serving payload ever names where terms came from.
--
-- TRANSPORT (decided 2026-08-07, documented so the next operator does not guess)
--   The reference bodies are local JSON profiles on the owner's workstation; the
--   pipeline runs on Railway with no access to that filesystem. So the profiles
--   are flattened to a CSV by ``scripts/load_bond_reference_terms.py`` and
--   \copy'd into this table over ``railway ssh``. The build then reads the TABLE
--   and never a file, which keeps the worker deployable and the load auditable
--   (``loaded_at`` + ``batch_label``). The table is optional: a build with the
--   relation absent or empty simply enriches nothing and says so.
--
-- Idempotent DDL (CREATE ... IF NOT EXISTS) so install_schema may re-apply it.
--
-- OWNERSHIP (measured the hard way, 2026-08-07). The worker re-applies this file
-- on every build and the COMMENT below is owner-only DDL, so this table MUST be
-- owned by the role the worker connects as (``worker_writer`` in production,
-- matching every other bond product table). Creating it by hand as a superuser
-- and leaving it there makes install_schema fail closed with "must be owner of
-- table bond_reference_terms" and kills the pit_update stage instantly. The
-- guarded ownership normalizer below keeps this and its Finnhub ledgers on
-- ``worker_writer`` when the role exists.

CREATE TABLE IF NOT EXISTS bond_reference_terms (
    cusip9                text PRIMARY KEY CHECK (cusip9 ~ '^[0-9A-Z]{9}$'),
    isin                  text,
    coupon_rate           numeric,          -- percent of par per annum
    coupon_type           text,
    maturity_date         date,
    issue_date            date,
    seniority             text,
    secured               text CHECK (secured IS NULL OR secured IN ('secured','unsecured','not_reported')),
    day_count             text,
    payment_frequency     text,
    callable              boolean,
    amount_outstanding_mm numeric,          -- millions of the issue currency
    batch_label           text NOT NULL,
    loaded_at             timestamptz NOT NULL DEFAULT now(),
    -- Finnhub numeric fields retain the vendor's raw magnitude when its unit is
    -- not proven. The governed loader also calls that same raw magnitude
    -- ``amount_outstanding_mm``; the direct path keeps that compatibility.
    amount_outstanding_vendor numeric,
    asset                 text,
    asset_type            text,
    bond_type             text,
    dated_date            date,
    debt_type             text,
    figi                  text,
    first_coupon_date     date,
    industry_group        text,
    industry_sub_group    text,
    offering_price_vendor numeric,
    original_offering_vendor numeric,
    finnhub_run_id        uuid,
    finnhub_profile_state text CHECK (finnhub_profile_state IS NULL OR finnhub_profile_state = 'success'),
    finnhub_reason_code   text,
    finnhub_fetched_at    timestamptz,
    finnhub_source_lineage jsonb
);

-- Re-applicable additive migration for the 2026-08-07 relation. Production
-- applies it as postgres or worker_writer, then normalizes all owners to the
-- runtime worker role below.
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS amount_outstanding_vendor numeric;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS asset text;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS asset_type text;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS bond_type text;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS dated_date date;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS debt_type text;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS figi text;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS first_coupon_date date;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS industry_group text;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS industry_sub_group text;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS offering_price_vendor numeric;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS original_offering_vendor numeric;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS finnhub_run_id uuid;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS finnhub_profile_state text;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS finnhub_reason_code text;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS finnhub_fetched_at timestamptz;
ALTER TABLE bond_reference_terms ADD COLUMN IF NOT EXISTS finnhub_source_lineage jsonb;

CREATE TABLE IF NOT EXISTS bond_reference_terms_finnhub_run (
    run_id               uuid PRIMARY KEY,
    batch_label          text NOT NULL,
    resume_cursor        text,
    source_lineage       jsonb NOT NULL CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb),
    started_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bond_reference_terms_finnhub_attempt (
    run_id               uuid NOT NULL REFERENCES bond_reference_terms_finnhub_run(run_id) ON DELETE RESTRICT,
    cusip9               text NOT NULL CHECK (cusip9 ~ '^[0-9A-Z]{9}$'),
    fetched_at           timestamptz NOT NULL,
    profile_state        text NOT NULL CHECK (profile_state IN ('success','empty','refused','transient','config_error')),
    reason_code          text NOT NULL,
    source_lineage       jsonb NOT NULL CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb),
    CONSTRAINT bond_reference_terms_finnhub_attempt_reason_state_ck CHECK (
        (profile_state = 'success' AND reason_code IN ('returned_cusip','isin_embedded_cusip9')) OR
        (profile_state = 'empty' AND reason_code = 'empty_profile') OR
        (profile_state = 'refused' AND reason_code IN ('invalid_requested_cusip','cusip_mismatch','isin_cusip_mismatch','missing_identity_evidence')) OR
        (profile_state = 'transient' AND reason_code IN ('transient_error','unexpected_error','provider_error')) OR
        (profile_state = 'config_error' AND reason_code = 'config_error')
    ),
    PRIMARY KEY (run_id, cusip9, fetched_at)
);

CREATE INDEX IF NOT EXISTS bond_reference_terms_finnhub_attempt_cusip_idx
    ON bond_reference_terms_finnhub_attempt (cusip9, fetched_at DESC);

-- Existing attempt ledgers predate the named reason/state constraint. Replace
-- only that composite CHECK so the runtime's retryable provider_error reason
-- remains valid without broadening any other profile_state taxonomy.
DO $$
DECLARE
    legacy_constraint record;
BEGIN
    FOR legacy_constraint IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'bond_reference_terms_finnhub_attempt'::regclass
          AND contype = 'c'
          AND conname <> 'bond_reference_terms_finnhub_attempt_reason_state_ck'
          AND pg_get_constraintdef(oid) LIKE '%profile_state%'
          AND pg_get_constraintdef(oid) LIKE '%reason_code%'
    LOOP
        EXECUTE format(
            'ALTER TABLE bond_reference_terms_finnhub_attempt DROP CONSTRAINT %I',
            legacy_constraint.conname
        );
    END LOOP;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bond_reference_terms_finnhub_attempt'::regclass
          AND conname = 'bond_reference_terms_finnhub_attempt_reason_state_ck'
    ) THEN
        ALTER TABLE bond_reference_terms_finnhub_attempt
            ADD CONSTRAINT bond_reference_terms_finnhub_attempt_reason_state_ck CHECK (
                (profile_state = 'success' AND reason_code IN ('returned_cusip','isin_embedded_cusip9')) OR
                (profile_state = 'empty' AND reason_code = 'empty_profile') OR
                (profile_state = 'refused' AND reason_code IN ('invalid_requested_cusip','cusip_mismatch','isin_cusip_mismatch','missing_identity_evidence')) OR
                (profile_state = 'transient' AND reason_code IN ('transient_error','unexpected_error','provider_error')) OR
                (profile_state = 'config_error' AND reason_code = 'config_error')
            );
    END IF;
END $$;

-- Inline CHECKs above cover fresh relations; existing production tables need
-- additive constraints too.  Named constraints make this re-applicable.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'bond_reference_terms_finnhub_profile_state_ck') THEN
        ALTER TABLE bond_reference_terms ADD CONSTRAINT bond_reference_terms_finnhub_profile_state_ck
            CHECK (finnhub_profile_state IS NULL OR finnhub_profile_state = 'success');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'bond_reference_terms_finnhub_reason_ck') THEN
        ALTER TABLE bond_reference_terms ADD CONSTRAINT bond_reference_terms_finnhub_reason_ck
            CHECK ((finnhub_profile_state IS NULL AND finnhub_reason_code IS NULL) OR
                   (finnhub_profile_state = 'success' AND finnhub_reason_code IN ('returned_cusip','isin_embedded_cusip9')));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'bond_reference_terms_finnhub_lineage_ck') THEN
        ALTER TABLE bond_reference_terms ADD CONSTRAINT bond_reference_terms_finnhub_lineage_ck
            CHECK (finnhub_source_lineage IS NULL OR
                   (jsonb_typeof(finnhub_source_lineage) = 'object' AND finnhub_source_lineage <> '{}'::jsonb));
    END IF;
END $$;

-- The target owner is explicit by contract.  A worker_writer session can keep
-- ownership of its own relation; postgres can hand newly created relations to
-- it.  If a dev database has no worker_writer role, schema application remains
-- usable and the ownership check is simply inapplicable there.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker_writer') THEN
        ALTER TABLE bond_reference_terms OWNER TO worker_writer;
        ALTER TABLE bond_reference_terms_finnhub_run OWNER TO worker_writer;
        ALTER TABLE bond_reference_terms_finnhub_attempt OWNER TO worker_writer;
    END IF;
END $$;

COMMENT ON TABLE bond_reference_terms IS
    'Neutral CUSIP9-keyed reference terms; gap-fill input to bond_security_v1 (basis vendor_reference). Not a publication.';
