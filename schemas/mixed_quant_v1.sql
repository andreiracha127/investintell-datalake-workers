-- Point-in-time mixed-universe quant publication (mixed_quant_v1).
--
-- The workers repo owns ingestion/materialization; it publishes one immutable
-- point-in-time snapshot (funds, equities, bonds) that the app pins per solve.
-- A publication is built inactive, materialized from immutable point-in-time
-- observations, then promoted atomically through active_quant_publication_v1.
--
-- Binding constraints enforced here:
--   * Non-active writes: child rows may only be written while the parent
--     publication is 'building' or 'ready'. Once 'active'/'superseded' the
--     snapshot is frozen.
--   * Atomic pointer promotion: promote_quant_publication() swaps the active
--     pointer under a per-product advisory lock in a single transaction.
--   * No inferred yield: quant_income_v1 stores only observed cash events; it
--     has no YTM/YTW/OAS/Z-spread/price/yield columns and rejects them.
--   * Lineage: every published value carries source_lineage resolving to its
--     observation, and every row resolves to its publication by FK.
--
-- The DDL is idempotent (CREATE ... IF NOT EXISTS, CREATE OR REPLACE) so it can
-- be applied repeatedly by the worker's install_schema step.

-- ---------------------------------------------------------------------------
-- Point-in-time observations (immutable inputs the publication is built from).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mixed_quant_identity_observation (
    observation_id   uuid PRIMARY KEY,
    as_of            date NOT NULL,
    instrument_type  text NOT NULL CHECK (instrument_type IN ('fund', 'equity', 'bond')),
    currency         text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    issuer_id        text,
    security_id      text,
    alias_type       text NOT NULL CHECK (alias_type IN ('cusip', 'isin', 'ticker')),
    alias_value      text NOT NULL CHECK (alias_value <> ''),
    -- deterministic_key groups observations that provably share one identity.
    -- When absent the observation stays unresolved (never merged on alias alone).
    deterministic_key text,
    observed_at      timestamptz NOT NULL,
    valid_from       date NOT NULL,
    valid_to         date,
    source_lineage   jsonb NOT NULL,
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

CREATE TABLE IF NOT EXISTS mixed_quant_return_observation (
    observation_id   uuid PRIMARY KEY,
    as_of            date NOT NULL,
    alias_type       text NOT NULL CHECK (alias_type IN ('cusip', 'isin', 'ticker')),
    alias_value      text NOT NULL CHECK (alias_value <> ''),
    period_end       date NOT NULL,
    frequency        text NOT NULL CHECK (frequency IN ('daily', 'monthly', 'quarterly', 'annual')),
    total_return     double precision NOT NULL,
    observed_at      timestamptz NOT NULL,
    source_lineage   jsonb NOT NULL,
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

CREATE TABLE IF NOT EXISTS mixed_quant_income_observation (
    observation_id   uuid PRIMARY KEY,
    as_of            date NOT NULL,
    alias_type       text NOT NULL CHECK (alias_type IN ('cusip', 'isin', 'ticker')),
    alias_value      text NOT NULL CHECK (alias_value <> ''),
    event_date       date NOT NULL,
    cash_amount      numeric NOT NULL,
    currency         text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    event_type       text NOT NULL CHECK (event_type IN ('dividend', 'coupon', 'distribution', 'return_of_capital')),
    observed_at      timestamptz NOT NULL,
    source_lineage   jsonb NOT NULL,
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

-- N-PORT-style holdings observation feeding the pure look-through engine.
-- holdings is a JSON array of holding rows consumed by expand_series.
CREATE TABLE IF NOT EXISTS mixed_quant_holding_observation (
    observation_id   uuid PRIMARY KEY,
    as_of            date NOT NULL,
    series_id        text NOT NULL CHECK (series_id <> ''),
    report_date      date NOT NULL,
    holdings         jsonb NOT NULL,
    source_lineage   jsonb NOT NULL,
    CHECK (jsonb_typeof(holdings) = 'array'),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb),
    UNIQUE (as_of, series_id)
);

-- Observed, return-estimated class-factor evidence (fund/equity). Produced by
-- the governed offline class-factor runner (sec_class_factors); this table
-- carries its per-factor results into the publication. measurement_type keeps
-- the app's observed-vs-estimated distinction; evidence preserves the model
-- fit/version (e.g. instrumented_pca / latent factor evidence).
CREATE TABLE IF NOT EXISTS mixed_quant_class_factor_observation (
    observation_id   uuid PRIMARY KEY,
    as_of            date NOT NULL,
    alias_type       text NOT NULL CHECK (alias_type IN ('cusip', 'isin', 'ticker')),
    alias_value      text NOT NULL CHECK (alias_value <> ''),
    factor           text NOT NULL CHECK (factor <> ''),
    value            double precision NOT NULL,
    method           text NOT NULL CHECK (method <> ''),
    measurement_type text NOT NULL CHECK (measurement_type IN ('observed', 'estimated')),
    quality_status   text NOT NULL CHECK (quality_status <> ''),
    quality_flags    jsonb NOT NULL DEFAULT '[]'::jsonb,
    evidence         jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_at      timestamptz NOT NULL,
    source_lineage   jsonb NOT NULL,
    CHECK (jsonb_typeof(quality_flags) = 'array'),
    CHECK (jsonb_typeof(evidence) = 'object'),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

-- Observed named bond factor inputs. The vocabulary is fixed to the five named
-- factors; the publication publishes only observed factors and declares the
-- rest ABSENT via instrument coverage. No inferred analytics are stored here.
CREATE TABLE IF NOT EXISTS mixed_quant_bond_factor_observation (
    observation_id   uuid PRIMARY KEY,
    as_of            date NOT NULL,
    alias_type       text NOT NULL CHECK (alias_type IN ('cusip', 'isin', 'ticker')),
    alias_value      text NOT NULL CHECK (alias_value <> ''),
    factor           text NOT NULL CHECK (factor IN ('curve', 'duration', 'credit', 'inflation', 'liquidity')),
    value            double precision NOT NULL,
    method           text NOT NULL CHECK (method <> ''),
    observed_at      timestamptz NOT NULL,
    source_lineage   jsonb NOT NULL,
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

-- ---------------------------------------------------------------------------
-- Publication snapshot.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS quant_publication_v1 (
    publication_id    uuid PRIMARY KEY,
    product           text NOT NULL CHECK (product <> ''),
    as_of             date NOT NULL,
    code_revision     text NOT NULL CHECK (code_revision <> ''),
    input_watermarks  jsonb NOT NULL DEFAULT '{}'::jsonb,
    config_version    text NOT NULL CHECK (config_version <> ''),
    status            text NOT NULL DEFAULT 'building'
                          CHECK (status IN ('building', 'ready', 'active', 'superseded')),
    counts            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at        timestamptz NOT NULL DEFAULT now(),
    activated_at      timestamptz,
    CHECK ((status = 'active') = (activated_at IS NOT NULL) OR status = 'superseded'),
    -- One in-flight (non-active) build per identity keeps reruns idempotent.
    UNIQUE (product, as_of, code_revision, config_version)
);

CREATE TABLE IF NOT EXISTS quant_publication_checkpoint_v1 (
    publication_id  uuid NOT NULL REFERENCES quant_publication_v1(publication_id) ON DELETE CASCADE,
    stage           text NOT NULL CHECK (stage <> ''),
    cursor          jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, stage)
);

CREATE TABLE IF NOT EXISTS quant_instrument_v1 (
    publication_id   uuid NOT NULL REFERENCES quant_publication_v1(publication_id) ON DELETE CASCADE,
    instrument_id    uuid NOT NULL,
    instrument_type  text NOT NULL CHECK (instrument_type IN ('fund', 'equity', 'bond')),
    currency         text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    issuer_id        text,
    security_id      text,
    validity         daterange NOT NULL DEFAULT daterange(NULL, NULL),
    coverage         jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (publication_id, instrument_id)
);

CREATE TABLE IF NOT EXISTS quant_instrument_alias_v1 (
    publication_id   uuid NOT NULL,
    instrument_id    uuid NOT NULL,
    alias_type       text NOT NULL CHECK (alias_type IN ('cusip', 'isin', 'ticker')),
    alias_value      text NOT NULL CHECK (alias_value <> ''),
    valid_from       date NOT NULL,
    valid_to         date,
    source_lineage   jsonb NOT NULL,
    PRIMARY KEY (publication_id, instrument_id, alias_type, alias_value, valid_from),
    FOREIGN KEY (publication_id, instrument_id)
        REFERENCES quant_instrument_v1(publication_id, instrument_id) ON DELETE CASCADE,
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);
-- The same alias may resolve to more than one unresolved instrument (a collision
-- preserved as separate records), so aliases are NOT globally unique per value.

CREATE TABLE IF NOT EXISTS quant_return_v1 (
    publication_id   uuid NOT NULL,
    instrument_id    uuid NOT NULL,
    period_end       date NOT NULL,
    frequency        text NOT NULL CHECK (frequency IN ('daily', 'monthly', 'quarterly', 'annual')),
    total_return     double precision NOT NULL,
    observed_at      timestamptz NOT NULL,
    source_lineage   jsonb NOT NULL,
    PRIMARY KEY (publication_id, instrument_id, period_end, frequency),
    FOREIGN KEY (publication_id, instrument_id)
        REFERENCES quant_instrument_v1(publication_id, instrument_id) ON DELETE CASCADE,
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

CREATE TABLE IF NOT EXISTS quant_exposure_v1 (
    publication_id   uuid NOT NULL,
    instrument_id    uuid NOT NULL,
    factor           text NOT NULL CHECK (factor <> ''),
    value            double precision NOT NULL,
    method           text NOT NULL CHECK (method <> ''),
    coverage         jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_lineage   jsonb NOT NULL,
    PRIMARY KEY (publication_id, instrument_id, factor, method),
    FOREIGN KEY (publication_id, instrument_id)
        REFERENCES quant_instrument_v1(publication_id, instrument_id) ON DELETE CASCADE,
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

-- Observed income cash events only. No inferred yield metrics (YTM/YTW/OAS/
-- Z-spread/price/yield) are ever stored here.
CREATE TABLE IF NOT EXISTS quant_income_v1 (
    publication_id   uuid NOT NULL,
    instrument_id    uuid NOT NULL,
    event_date       date NOT NULL,
    cash_amount      numeric NOT NULL,
    currency         text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    event_type       text NOT NULL CHECK (event_type IN ('dividend', 'coupon', 'distribution', 'return_of_capital')),
    source_lineage   jsonb NOT NULL,
    PRIMARY KEY (publication_id, instrument_id, event_date, event_type),
    FOREIGN KEY (publication_id, instrument_id)
        REFERENCES quant_instrument_v1(publication_id, instrument_id) ON DELETE CASCADE,
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

-- Canonical direct-security linkage: a fund's direct holding resolved (through
-- the alias/identity machinery) to a security identity that is itself an
-- instrument in this publication. Only unambiguous resolutions are linked;
-- collisions/unresolved holdings are intentionally left unlinked.
CREATE TABLE IF NOT EXISTS quant_holding_link_v1 (
    publication_id         uuid NOT NULL,
    instrument_id          uuid NOT NULL,   -- the holding fund
    security_instrument_id uuid NOT NULL,   -- the resolved direct security
    alias_type             text NOT NULL CHECK (alias_type IN ('cusip', 'isin', 'ticker')),
    alias_value            text NOT NULL CHECK (alias_value <> ''),
    weight_pct             double precision NOT NULL,
    coverage               jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_lineage         jsonb NOT NULL,
    PRIMARY KEY (publication_id, instrument_id, security_instrument_id),
    FOREIGN KEY (publication_id, instrument_id)
        REFERENCES quant_instrument_v1(publication_id, instrument_id) ON DELETE CASCADE,
    FOREIGN KEY (publication_id, security_instrument_id)
        REFERENCES quant_instrument_v1(publication_id, instrument_id) ON DELETE CASCADE,
    CHECK (instrument_id <> security_instrument_id),
    CHECK (jsonb_typeof(source_lineage) = 'object' AND source_lineage <> '{}'::jsonb)
);

CREATE TABLE IF NOT EXISTS active_quant_publication_v1 (
    product         text PRIMARY KEY CHECK (product <> ''),
    publication_id  uuid NOT NULL UNIQUE REFERENCES quant_publication_v1(publication_id) ON DELETE RESTRICT,
    activated_at    timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Non-active write guard.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION quant_reject_active_publication_write() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target uuid := COALESCE(NEW.publication_id, OLD.publication_id);
    state text;
BEGIN
    SELECT status INTO state FROM quant_publication_v1 WHERE publication_id = target;
    IF state IS NULL THEN
        RAISE EXCEPTION 'unknown publication %', target;
    END IF;
    IF state NOT IN ('building', 'ready') THEN
        RAISE EXCEPTION 'publication % is % and no longer writable', target, state;
    END IF;
    RETURN COALESCE(NEW, OLD);
END $$;

DROP TRIGGER IF EXISTS quant_instrument_v1_guard ON quant_instrument_v1;
CREATE TRIGGER quant_instrument_v1_guard BEFORE INSERT OR UPDATE OR DELETE ON quant_instrument_v1
FOR EACH ROW EXECUTE FUNCTION quant_reject_active_publication_write();
DROP TRIGGER IF EXISTS quant_instrument_alias_v1_guard ON quant_instrument_alias_v1;
CREATE TRIGGER quant_instrument_alias_v1_guard BEFORE INSERT OR UPDATE OR DELETE ON quant_instrument_alias_v1
FOR EACH ROW EXECUTE FUNCTION quant_reject_active_publication_write();
DROP TRIGGER IF EXISTS quant_return_v1_guard ON quant_return_v1;
CREATE TRIGGER quant_return_v1_guard BEFORE INSERT OR UPDATE OR DELETE ON quant_return_v1
FOR EACH ROW EXECUTE FUNCTION quant_reject_active_publication_write();
DROP TRIGGER IF EXISTS quant_exposure_v1_guard ON quant_exposure_v1;
CREATE TRIGGER quant_exposure_v1_guard BEFORE INSERT OR UPDATE OR DELETE ON quant_exposure_v1
FOR EACH ROW EXECUTE FUNCTION quant_reject_active_publication_write();
DROP TRIGGER IF EXISTS quant_income_v1_guard ON quant_income_v1;
CREATE TRIGGER quant_income_v1_guard BEFORE INSERT OR UPDATE OR DELETE ON quant_income_v1
FOR EACH ROW EXECUTE FUNCTION quant_reject_active_publication_write();
DROP TRIGGER IF EXISTS quant_holding_link_v1_guard ON quant_holding_link_v1;
CREATE TRIGGER quant_holding_link_v1_guard BEFORE INSERT OR UPDATE OR DELETE ON quant_holding_link_v1
FOR EACH ROW EXECUTE FUNCTION quant_reject_active_publication_write();

-- ---------------------------------------------------------------------------
-- Atomic pointer promotion.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION promote_quant_publication(target_product text, target_publication_id uuid)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE publication quant_publication_v1%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(target_product, 0));
    SELECT * INTO publication FROM quant_publication_v1
        WHERE publication_id = target_publication_id FOR UPDATE;
    IF publication.publication_id IS NULL OR publication.product <> target_product THEN
        RAISE EXCEPTION 'publication % is not a % publication', target_publication_id, target_product;
    END IF;
    IF publication.status NOT IN ('ready', 'active') THEN
        RAISE EXCEPTION 'publication % must be ready before promotion (is %)',
            target_publication_id, publication.status;
    END IF;
    -- Retire the incumbent for this product.
    UPDATE quant_publication_v1 SET status = 'superseded'
        WHERE product = target_product AND status = 'active'
          AND publication_id <> target_publication_id;
    UPDATE quant_publication_v1 SET status = 'active', activated_at = now()
        WHERE publication_id = target_publication_id AND status <> 'active';
    INSERT INTO active_quant_publication_v1 (product, publication_id, activated_at)
        VALUES (target_product, target_publication_id, now())
        ON CONFLICT (product) DO UPDATE
            SET publication_id = EXCLUDED.publication_id, activated_at = EXCLUDED.activated_at;
END $$;
