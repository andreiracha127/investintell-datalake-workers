-- Bond panel v1 is a worker-owned, immutable research publication protocol.
--
-- Install this file through ``src.bonds.panel_materializer.install_schema``.
-- It deliberately contains no BEGIN/COMMIT: the installer owns the transaction
-- boundary, so a failed install cannot leave a partially upgraded protocol.

CREATE TABLE IF NOT EXISTS bond_panel_publications (
    publication_id uuid PRIMARY KEY,
    product text NOT NULL DEFAULT 'bond_panel_v1' CHECK (product = 'bond_panel_v1'),
    parent_publication_id uuid REFERENCES bond_panel_publications(publication_id) ON DELETE RESTRICT,
    publication_status text NOT NULL CHECK (publication_status IN ('prepared', 'validated', 'failed')),
    failure_reason text,
    config_hash char(16) NOT NULL CONSTRAINT bond_panel_publications_config_hash_check
        CHECK (config_hash IN ('0c0d78a866bc1090', '180a82b3f1413d43', '1863d3d5fa3a0edf')),
    input_fingerprint char(64) NOT NULL,
    code_revision text NOT NULL,
    first_month date NOT NULL,
    last_closed_month date NOT NULL,
    open_month date,
    snapshot_rows integer NOT NULL CHECK (snapshot_rows > 0),
    rv_signal_rows integer NOT NULL CHECK (rv_signal_rows > 0),
    returns_rows integer NOT NULL CHECK (returns_rows > 0),
    ratings_pit_rows integer NOT NULL CHECK (ratings_pit_rows > 0),
    source_lineage jsonb NOT NULL CHECK (jsonb_typeof(source_lineage) = 'object'),
    gate_evidence jsonb NOT NULL CHECK (jsonb_typeof(gate_evidence) = 'object'),
    built_at timestamptz NOT NULL DEFAULT now(),
    computed_at timestamptz NOT NULL DEFAULT now(),
    validated_at timestamptz,
    UNIQUE (input_fingerprint),
    CHECK ((publication_status = 'failed') = (failure_reason IS NOT NULL)),
    CHECK ((publication_status = 'validated') = (validated_at IS NOT NULL)),
    CHECK (first_month <= last_closed_month),
    CHECK (
        (parent_publication_id IS NULL AND open_month IS NULL)
        OR (parent_publication_id IS NOT NULL AND open_month > last_closed_month)
    )
);

-- Re-applying the DDL must admit both the controlled legacy history seeder and
-- the active Reg S configuration. The worker itself still mints only the active
-- hash; the legacy value is required by backfill_bond_panel_history.py on a fresh
-- database and remains governed by the publication/pointer transition triggers.
-- NOT VALID preserves pre-constraint immutable history on upgrades.
ALTER TABLE bond_panel_publications
    DROP CONSTRAINT IF EXISTS bond_panel_publications_config_hash_check;
ALTER TABLE bond_panel_publications
    ADD CONSTRAINT bond_panel_publications_config_hash_check CHECK (config_hash IN ('0c0d78a866bc1090', '180a82b3f1413d43', '1863d3d5fa3a0edf')) NOT VALID;

CREATE TABLE IF NOT EXISTS bond_panel_app_pointer (
    product text PRIMARY KEY DEFAULT 'bond_panel_v1' CHECK (product = 'bond_panel_v1'),
    publication_id uuid NOT NULL REFERENCES bond_panel_publications(publication_id) ON DELETE RESTRICT,
    changed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (product)
);

DO $retired_reg_s_pointer_preflight$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM bond_panel_app_pointer pointer
        JOIN bond_panel_publications publication USING (publication_id)
        WHERE pointer.product = 'bond_panel_v1'
          AND btrim(publication.config_hash::text) = '180a82b3f1413d43'
    ) THEN
        RAISE EXCEPTION 'schema install refuses an active retired Reg S-only pointer';
    END IF;
END
$retired_reg_s_pointer_preflight$;

-- This table retains every candidate; exclusion never causes a CUSIP to vanish.
CREATE TABLE IF NOT EXISTS bond_panel_snapshot (
    publication_id uuid NOT NULL REFERENCES bond_panel_publications(publication_id) ON DELETE RESTRICT,
    month date NOT NULL,
    cusip_id text NOT NULL,
    distribution_rule text CHECK (distribution_rule IS NULL OR distribution_rule IN ('rule_144a', 'reg_s')),
    reference_cusip9 text,
    distribution_decision_id text,
    issuer_id text,
    issuer_identity_state text NOT NULL DEFAULT 'unresolved'
        CHECK (issuer_identity_state <> ''),
    ff17num integer,
    eligibility_state text NOT NULL CHECK (eligibility_state IN ('included', 'excluded')),
    eligibility_reason text NOT NULL CHECK (eligibility_reason <> ''),
    currency text,
    asset_class text,
    amount_outstanding_k numeric,
    maturity_date date,
    maturity_years numeric,
    coupon_pct numeric,
    price numeric,
    price_source text,
    db_type integer,
    ytm numeric,
    ytm_basis text,
    mod_dur numeric,
    mod_dur_source text,
    spread_final numeric,
    spread_final_bps numeric,
    spread_definition text NOT NULL DEFAULT 'ytm_minus_interpolated_dgs',
    spread_source text,
    rating_bucket text NOT NULL DEFAULT 'NR'
        CHECK (rating_bucket IN ('AAA','AA','A','BBB','BB','B','CCC','D','NR')),
    rating_state text NOT NULL DEFAULT 'missing' CHECK (rating_state <> ''),
    traded_days integer,
    trade_count integer,
    dollar_volume numeric,
    rel_bid_ask_bps numeric,
    quoted_days integer,
    terms_source text,
    source_lineage jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload jsonb NOT NULL,
    PRIMARY KEY (publication_id, month, cusip_id),
    CHECK (spread_definition = 'ytm_minus_interpolated_dgs'),
    CHECK (jsonb_typeof(source_lineage) = 'object')
);

-- Model outputs are only published for eligible snapshot rows.  The copied
-- snapshot covariates make the residual diagnosable without an app join.
CREATE TABLE IF NOT EXISTS bond_panel_rv_signal (
    publication_id uuid NOT NULL REFERENCES bond_panel_publications(publication_id) ON DELETE RESTRICT,
    month date NOT NULL,
    cusip_id text NOT NULL,
    distribution_rule text CHECK (distribution_rule IS NULL OR distribution_rule IN ('rule_144a', 'reg_s')),
    reference_cusip9 text,
    distribution_decision_id text,
    issuer_id text,
    ff17num integer,
    eligibility_state text NOT NULL CHECK (eligibility_state = 'included'),
    eligibility_reason text NOT NULL CHECK (eligibility_reason = 'eligible'),
    price numeric,
    amount_outstanding_k numeric,
    maturity_years numeric,
    traded_days integer,
    trade_count integer,
    dollar_volume numeric,
    rel_bid_ask_bps numeric,
    quoted_days integer,
    ytm numeric,
    ytm_basis text,
    mod_dur numeric,
    mod_dur_source text,
    spread_final_bps numeric,
    spread_definition text NOT NULL DEFAULT 'ytm_minus_interpolated_dgs',
    residual_bps numeric,
    rv_signal numeric,
    price_source text,
    flags jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_lineage jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload jsonb NOT NULL,
    PRIMARY KEY (publication_id, month, cusip_id),
    CHECK (spread_definition = 'ytm_minus_interpolated_dgs'),
    CHECK (jsonb_typeof(flags) = 'object'),
    CHECK (jsonb_typeof(source_lineage) = 'object')
);

CREATE TABLE IF NOT EXISTS bond_panel_returns (
    publication_id uuid NOT NULL REFERENCES bond_panel_publications(publication_id) ON DELETE RESTRICT,
    month date NOT NULL,
    cusip_id text NOT NULL,
    distribution_rule text CHECK (distribution_rule IS NULL OR distribution_rule IN ('rule_144a', 'reg_s')),
    reference_cusip9 text,
    distribution_decision_id text,
    total_return numeric NOT NULL,
    price_return numeric,
    carry_return numeric,
    exit_basis text NOT NULL CHECK (exit_basis IN ('observed', 'matured', 'distressed', 'unexplained')),
    exit_reason text,
    suspect boolean NOT NULL DEFAULT false,
    payload jsonb NOT NULL,
    PRIMARY KEY (publication_id, month, cusip_id),
    CHECK ((exit_basis = 'observed') = (exit_reason IS NULL))
);

-- One generic static rating mapping row per CUSIP/month.
CREATE TABLE IF NOT EXISTS bond_panel_rating_pit (
    publication_id uuid NOT NULL REFERENCES bond_panel_publications(publication_id) ON DELETE RESTRICT,
    month date NOT NULL,
    cusip_id text NOT NULL,
    distribution_rule text CHECK (distribution_rule IS NULL OR distribution_rule IN ('rule_144a', 'reg_s')),
    reference_cusip9 text,
    distribution_decision_id text,
    rating_bucket text NOT NULL
        CHECK (rating_bucket IN ('AAA','AA','A','BBB','BB','B','CCC','D','NR')),
    rating_as_of_month date,
    rating_state text NOT NULL CHECK (rating_state IN (
        'historical_pit', 'historical_missing',
        'static_current', 'static_carry_forward', 'static_missing'
    )),
    rating_reason text NOT NULL CHECK (rating_reason <> ''),
    rating_staleness_months integer
        CHECK (rating_staleness_months IS NULL OR rating_staleness_months >= 0),
    source_lineage jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload jsonb NOT NULL,
    PRIMARY KEY (publication_id, month, cusip_id),
    CHECK (jsonb_typeof(source_lineage) = 'object'),
    CHECK (
        rating_state NOT IN ('static_missing', 'historical_missing')
        OR rating_bucket = 'NR'
    )
);

-- Historical immutable publications predate distribution-series resolution and
-- retain NULL identity fields.  New tables receive the columns above; these
-- rerunnable additions upgrade existing installations without rewriting history.
ALTER TABLE bond_panel_snapshot
    ADD COLUMN IF NOT EXISTS distribution_rule text CHECK (distribution_rule IS NULL OR distribution_rule IN ('rule_144a', 'reg_s')),
    ADD COLUMN IF NOT EXISTS reference_cusip9 text,
    ADD COLUMN IF NOT EXISTS distribution_decision_id text;
ALTER TABLE bond_panel_rv_signal
    ADD COLUMN IF NOT EXISTS distribution_rule text CHECK (distribution_rule IS NULL OR distribution_rule IN ('rule_144a', 'reg_s')),
    ADD COLUMN IF NOT EXISTS reference_cusip9 text,
    ADD COLUMN IF NOT EXISTS distribution_decision_id text;
ALTER TABLE bond_panel_returns
    ADD COLUMN IF NOT EXISTS distribution_rule text CHECK (distribution_rule IS NULL OR distribution_rule IN ('rule_144a', 'reg_s')),
    ADD COLUMN IF NOT EXISTS reference_cusip9 text,
    ADD COLUMN IF NOT EXISTS distribution_decision_id text;
ALTER TABLE bond_panel_rating_pit
    ADD COLUMN IF NOT EXISTS distribution_rule text CHECK (distribution_rule IS NULL OR distribution_rule IN ('rule_144a', 'reg_s')),
    ADD COLUMN IF NOT EXISTS reference_cusip9 text,
    ADD COLUMN IF NOT EXISTS distribution_decision_id text;

CREATE OR REPLACE FUNCTION bond_panel_assert_publication_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.publication_status <> 'prepared' OR NEW.failure_reason IS NOT NULL THEN
            RAISE EXCEPTION 'bond panel publications start prepared';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.publication_status = 'prepared' AND NEW.publication_status IN ('validated', 'failed') THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'illegal bond panel lifecycle transition: % -> %', OLD.publication_status, NEW.publication_status;
END;
$$;

CREATE OR REPLACE FUNCTION bond_panel_assert_parent()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.parent_publication_id IS NULL THEN RETURN NEW; END IF;
    IF NEW.parent_publication_id = NEW.publication_id THEN RAISE EXCEPTION 'bond panel ancestry self cycle'; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications parent
        WHERE parent.publication_id = NEW.parent_publication_id AND parent.publication_status = 'validated'
    ) THEN RAISE EXCEPTION 'bond panel parent must be validated'; END IF;
    IF EXISTS (
        WITH RECURSIVE ancestry(publication_id, parent_publication_id) AS (
            SELECT publication_id, parent_publication_id FROM bond_panel_publications WHERE publication_id = NEW.parent_publication_id
            UNION ALL
            SELECT p.publication_id, p.parent_publication_id FROM bond_panel_publications p JOIN ancestry a ON p.publication_id = a.parent_publication_id
        ) SELECT 1 FROM ancestry WHERE publication_id = NEW.publication_id
    ) THEN RAISE EXCEPTION 'bond panel ancestry cycle'; END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION bond_panel_assert_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'immutable bond panel facts'; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications p
        WHERE p.publication_id = NEW.publication_id AND p.publication_status = 'prepared'
    ) THEN RAISE EXCEPTION 'facts only write during prepared lifecycle'; END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION bond_panel_assert_pointer_validated()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        WHERE candidate.publication_id = NEW.publication_id
          AND btrim(candidate.config_hash::text) = '180a82b3f1413d43'
    ) THEN RAISE EXCEPTION 'pointer rejects retired Reg S-only activation target'; END IF;
    IF TG_OP = 'INSERT' AND EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        WHERE candidate.publication_id = NEW.publication_id
          AND btrim(candidate.config_hash::text) = '1863d3d5fa3a0edf'
    ) AND EXISTS (
        SELECT 1 FROM bond_panel_publications legacy
        WHERE legacy.publication_status = 'validated'
          AND btrim(legacy.config_hash::text) = '0c0d78a866bc1090'
    ) THEN RAISE EXCEPTION 'initial dual pointer requires a fresh database without legacy history'; END IF;
    IF EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        WHERE candidate.publication_id = NEW.publication_id
          AND btrim(candidate.config_hash::text) = '1863d3d5fa3a0edf'
    ) AND NOT EXISTS (
        SELECT 1
        FROM bond_panel_snapshot s
        WHERE s.publication_id = NEW.publication_id
          AND s.distribution_rule = 'rule_144a'
          AND s.cusip_id = s.reference_cusip9
          AND s.distribution_decision_id IS NULL
    ) THEN RAISE EXCEPTION 'pointer requires valid dual-series fact identity'; END IF;
    IF EXISTS (
        SELECT 1
        FROM bond_panel_publications candidate
        WHERE candidate.publication_id = NEW.publication_id
          AND btrim(candidate.config_hash::text) = '1863d3d5fa3a0edf'
          AND EXISTS (
              SELECT 1 FROM (
                  SELECT month, cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM bond_panel_snapshot WHERE publication_id = NEW.publication_id
                  UNION ALL SELECT month, cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM bond_panel_rv_signal WHERE publication_id = NEW.publication_id
                  UNION ALL SELECT month, cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM bond_panel_returns WHERE publication_id = NEW.publication_id
                  UNION ALL SELECT month, cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM bond_panel_rating_pit WHERE publication_id = NEW.publication_id
              ) rows
              WHERE rows.distribution_rule IS NULL OR rows.reference_cusip9 IS NULL
                 OR btrim(rows.reference_cusip9) = ''
                 OR (rows.distribution_rule = 'rule_144a' AND (rows.cusip_id <> rows.reference_cusip9 OR rows.distribution_decision_id IS NOT NULL))
                 OR (rows.distribution_rule = 'reg_s' AND nullif(rows.distribution_decision_id, '') IS NULL)
          )
    ) THEN RAISE EXCEPTION 'pointer requires valid dual-series fact identity'; END IF;
    IF EXISTS (
        SELECT 1 FROM bond_panel_publications candidate
        WHERE candidate.publication_id = NEW.publication_id
          AND btrim(candidate.config_hash::text) = '1863d3d5fa3a0edf'
          AND EXISTS (
              SELECT 1 FROM bond_panel_rv_signal f JOIN bond_panel_snapshot s USING (publication_id, month, cusip_id)
              WHERE f.publication_id = NEW.publication_id
                AND (f.distribution_rule, f.reference_cusip9, f.distribution_decision_id) IS DISTINCT FROM (s.distribution_rule, s.reference_cusip9, s.distribution_decision_id)
              UNION ALL
              SELECT 1 FROM bond_panel_returns f JOIN bond_panel_snapshot s USING (publication_id, month, cusip_id)
              WHERE f.publication_id = NEW.publication_id
                AND (f.distribution_rule, f.reference_cusip9, f.distribution_decision_id) IS DISTINCT FROM (s.distribution_rule, s.reference_cusip9, s.distribution_decision_id)
              UNION ALL
              SELECT 1 FROM bond_panel_rating_pit f JOIN bond_panel_snapshot s USING (publication_id, month, cusip_id)
              WHERE f.publication_id = NEW.publication_id
                AND (f.distribution_rule, f.reference_cusip9, f.distribution_decision_id) IS DISTINCT FROM (s.distribution_rule, s.reference_cusip9, s.distribution_decision_id)
          )
    ) THEN RAISE EXCEPTION 'pointer requires matching dual-series surface identity'; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications p
        WHERE p.publication_id = NEW.publication_id
          AND p.publication_status = 'validated'
          AND p.snapshot_rows > 0 AND p.rv_signal_rows > 0
          AND p.returns_rows > 0 AND p.ratings_pit_rows > 0
    ) THEN RAISE EXCEPTION 'pointer requires non-empty validated publication'; END IF;
    IF TG_OP = 'UPDATE' AND EXISTS (
        SELECT 1
        FROM bond_panel_publications prior, bond_panel_publications candidate
        WHERE prior.publication_id = OLD.publication_id
          AND candidate.publication_id = NEW.publication_id
          AND (candidate.last_closed_month < prior.last_closed_month
               OR COALESCE(candidate.open_month, candidate.last_closed_month) < COALESCE(prior.open_month, prior.last_closed_month))
    ) THEN RAISE EXCEPTION 'pointer rejects config or month regression'; END IF;
    IF TG_OP = 'UPDATE' AND EXISTS (
        SELECT 1
        FROM bond_panel_publications prior, bond_panel_publications candidate
        WHERE prior.publication_id = OLD.publication_id
          AND candidate.publication_id = NEW.publication_id
          AND candidate.config_hash <> prior.config_hash
    ) AND NOT EXISTS (
        SELECT 1
        FROM bond_panel_publications prior, bond_panel_publications candidate
        WHERE prior.publication_id = OLD.publication_id
          AND candidate.publication_id = NEW.publication_id
          AND btrim(prior.config_hash::text) = '0c0d78a866bc1090'
          AND btrim(candidate.config_hash::text) = '1863d3d5fa3a0edf'
          AND candidate.parent_publication_id = OLD.publication_id
          AND candidate.first_month = prior.first_month
          AND candidate.last_closed_month = COALESCE(prior.open_month, prior.last_closed_month + INTERVAL '1 month')::date
          AND candidate.open_month = (candidate.last_closed_month + INTERVAL '1 month')::date
          AND candidate.source_lineage->>'distribution_rule' = 'rule_144a_and_reg_s'
          AND nullif(candidate.source_lineage->>'distribution_mapping_snapshot_id', '') IS NOT NULL
          AND candidate.gate_evidence @> jsonb_build_object(
              'config_transition', jsonb_build_object(
                  'contract', 'rule_144a_to_dual_series_delta_v1',
                  'from_publication_id', OLD.publication_id::text,
                  'from_config_hash', btrim(prior.config_hash::text),
                  'to_config_hash', btrim(candidate.config_hash::text),
                  'authorized_code_revision', candidate.code_revision
              )
          )
          AND (SELECT count(*) FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id) = candidate.snapshot_rows
          AND (SELECT count(*) FROM bond_panel_rv_signal f WHERE f.publication_id = candidate.publication_id) = candidate.rv_signal_rows
          AND (SELECT count(*) FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id) = candidate.returns_rows
          AND (SELECT count(*) FROM bond_panel_rating_pit f WHERE f.publication_id = candidate.publication_id) = candidate.ratings_pit_rows
          AND (SELECT min(f.month) FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id) = candidate.last_closed_month
          AND (SELECT max(f.month) FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id) = candidate.open_month
          AND (SELECT min(f.month) FROM bond_panel_rv_signal f WHERE f.publication_id = candidate.publication_id) = candidate.last_closed_month
          AND (SELECT max(f.month) FROM bond_panel_rv_signal f WHERE f.publication_id = candidate.publication_id) = candidate.last_closed_month
          AND (SELECT min(f.month) FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id) = candidate.last_closed_month
          AND (SELECT max(f.month) FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id) = candidate.last_closed_month
          AND NOT EXISTS (
              SELECT f.month, f.cusip_id FROM bond_panel_rv_signal f WHERE f.publication_id = candidate.publication_id
              EXCEPT SELECT f.month, f.cusip_id FROM bond_panel_snapshot f
              WHERE f.publication_id = candidate.publication_id AND f.eligibility_state = 'included'
          )
          AND NOT EXISTS (
              SELECT f.month, f.cusip_id FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id
              EXCEPT SELECT f.month, f.cusip_id FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id
          )
          AND NOT EXISTS (
              SELECT f.month, f.cusip_id FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id
              EXCEPT SELECT f.month, f.cusip_id FROM bond_panel_rating_pit f WHERE f.publication_id = candidate.publication_id
          )
          AND NOT EXISTS (
              SELECT f.month, f.cusip_id FROM bond_panel_rating_pit f WHERE f.publication_id = candidate.publication_id
              EXCEPT SELECT f.month, f.cusip_id FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM (
                  SELECT cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM bond_panel_snapshot WHERE publication_id = candidate.publication_id
                  UNION ALL
                  SELECT cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM bond_panel_rv_signal WHERE publication_id = candidate.publication_id
                  UNION ALL
                  SELECT cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM bond_panel_returns WHERE publication_id = candidate.publication_id
                  UNION ALL
                  SELECT cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM bond_panel_rating_pit WHERE publication_id = candidate.publication_id
              ) identity_rows
              WHERE identity_rows.distribution_rule IS NULL
                 OR identity_rows.reference_cusip9 IS NULL
                 OR btrim(identity_rows.reference_cusip9) = ''
                 OR (identity_rows.distribution_rule = 'rule_144a' AND (
                       identity_rows.cusip_id <> identity_rows.reference_cusip9
                       OR identity_rows.distribution_decision_id IS NOT NULL
                 ))
                 OR (identity_rows.distribution_rule = 'reg_s' AND nullif(identity_rows.distribution_decision_id, '') IS NULL)
          )
          AND NOT EXISTS (
              SELECT 1 FROM bond_panel_rv_signal f
              JOIN bond_panel_snapshot s USING (publication_id, month, cusip_id)
              WHERE f.publication_id = candidate.publication_id
                AND (f.distribution_rule, f.reference_cusip9, f.distribution_decision_id)
                    IS DISTINCT FROM (s.distribution_rule, s.reference_cusip9, s.distribution_decision_id)
          )
          AND NOT EXISTS (
              SELECT 1 FROM bond_panel_returns f
              JOIN bond_panel_snapshot s USING (publication_id, month, cusip_id)
              WHERE f.publication_id = candidate.publication_id
                AND (f.distribution_rule, f.reference_cusip9, f.distribution_decision_id)
                    IS DISTINCT FROM (s.distribution_rule, s.reference_cusip9, s.distribution_decision_id)
          )
          AND NOT EXISTS (
              SELECT 1 FROM bond_panel_rating_pit f
              JOIN bond_panel_snapshot s USING (publication_id, month, cusip_id)
              WHERE f.publication_id = candidate.publication_id
                AND (f.distribution_rule, f.reference_cusip9, f.distribution_decision_id)
                    IS DISTINCT FROM (s.distribution_rule, s.reference_cusip9, s.distribution_decision_id)
          )
          AND EXISTS (
              SELECT 1 FROM bond_panel_snapshot f
              WHERE f.publication_id = candidate.publication_id
                AND f.distribution_rule IN ('rule_144a', 'reg_s')
                AND f.distribution_rule = 'rule_144a' AND f.cusip_id = f.reference_cusip9
          )
          AND (
              EXISTS (
                  SELECT 1 FROM bond_panel_snapshot f
                  WHERE f.publication_id = candidate.publication_id
                    AND f.distribution_rule = 'reg_s'
                    AND nullif(f.distribution_decision_id, '') IS NOT NULL
              )
              OR (
                candidate.source_lineage->>'distribution_mapping_count' = '0'
                AND candidate.source_lineage->>'distribution_mapping_open_count' = '0'
                AND candidate.source_lineage->>'distribution_mapping_closed_count' = '0'
                AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_each_text(candidate.source_lineage) AS omission(key, value)
                    WHERE omission.key LIKE 'distribution_mapping_omission:%'
                      AND (omission.value IS NULL OR omission.value !~ '^[1-9][0-9]*$')
                )
                AND COALESCE((
                    SELECT sum(CASE
                        WHEN omission.value ~ '^[1-9][0-9]*$' THEN omission.value::numeric
                        ELSE 0
                    END)
                    FROM jsonb_each_text(candidate.source_lineage) AS omission(key, value)
                    WHERE omission.key LIKE 'distribution_mapping_omission:%'
                ), 0) = (
                    SELECT count(*) FROM bond_panel_snapshot f
                    WHERE f.publication_id = candidate.publication_id
                      AND f.month = candidate.open_month
                      AND f.distribution_rule = 'rule_144a'
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_each_text(candidate.source_lineage) AS omission(key, value)
                    WHERE omission.key LIKE 'distribution_mapping_closed_omission:%'
                      AND (omission.value IS NULL OR omission.value !~ '^[1-9][0-9]*$')
                )
                AND COALESCE((
                    SELECT sum(CASE
                        WHEN omission.value ~ '^[1-9][0-9]*$' THEN omission.value::numeric
                        ELSE 0
                    END)
                    FROM jsonb_each_text(candidate.source_lineage) AS omission(key, value)
                    WHERE omission.key LIKE 'distribution_mapping_closed_omission:%'
                ), 0) = (
                    SELECT count(*) FROM bond_panel_snapshot f
                    WHERE f.publication_id = candidate.publication_id
                      AND f.month = candidate.last_closed_month
                      AND f.distribution_rule = 'rule_144a'
                )
              )
          )
    ) THEN RAISE EXCEPTION 'pointer config transition requires an authorized dual-series delta child'; END IF;
    -- One deliberately narrow replacement is allowed for the incomplete T3
    -- legacy root.  It is not an ancestry shortcut: the old root remains
    -- immutable and the candidate must prove the frozen-artifact repair
    -- contract, complete direct facts, and exact source identity.
    IF TG_OP = 'UPDATE'
       AND NEW.publication_id IS DISTINCT FROM OLD.publication_id
       AND EXISTS (
           SELECT 1
           FROM bond_panel_publications prior, bond_panel_publications candidate
           WHERE prior.publication_id = OLD.publication_id
             AND candidate.publication_id = NEW.publication_id
             AND prior.publication_id = '92740098-1571-559d-9fb3-119de8321754'::uuid
             AND btrim(prior.config_hash::text) = '0c0d78a866bc1090'
             AND btrim(candidate.config_hash::text) = '0c0d78a866bc1090'
             AND prior.parent_publication_id IS NULL
             AND candidate.parent_publication_id IS NULL
             AND candidate.first_month = prior.first_month
             AND candidate.last_closed_month = prior.last_closed_month
             AND candidate.open_month IS NULL
             AND candidate.snapshot_rows = 3417683
             AND candidate.rv_signal_rows = 1687524
             AND candidate.returns_rows = 2801208
             AND candidate.ratings_pit_rows = 3417683
             AND candidate.gate_evidence @> jsonb_build_object('base_repair', jsonb_build_object(
                 'contract', 'legacy_parentless_return_coverage_repair_v1',
                 'from_publication_id', OLD.publication_id::text,
                 'from_config_hash', btrim(prior.config_hash::text),
                 'from_input_fingerprint', prior.input_fingerprint,
                 'from_artifact_fingerprint', 'e963304af08c1f513d048e1e7eee9fbe334fc3fe01b1c80f3cd5b7f8acb19581',
                 'authorized_code_revision', candidate.code_revision
             ))
             AND prior.gate_evidence @> jsonb_build_object('input_fingerprint', prior.input_fingerprint)
             AND candidate.source_lineage->'source_sha256' = prior.source_lineage->'source_sha256'
             AND (SELECT count(*) FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id) = candidate.snapshot_rows
             AND (SELECT count(*) FROM bond_panel_rv_signal f WHERE f.publication_id = candidate.publication_id) = candidate.rv_signal_rows
             AND (SELECT count(*) FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id) = candidate.returns_rows
             AND (SELECT count(*) FROM bond_panel_rating_pit f WHERE f.publication_id = candidate.publication_id) = candidate.ratings_pit_rows
             AND (SELECT max(f.month) FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id) = candidate.last_closed_month
             AND (SELECT max(f.month) FROM bond_panel_rv_signal f WHERE f.publication_id = candidate.publication_id) = candidate.last_closed_month
             AND (SELECT max(f.month) FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id) = candidate.last_closed_month
             AND (SELECT min(f.month) FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id) = (candidate.first_month + INTERVAL '1 month')::date
             AND (SELECT min(f.month) FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id) = (SELECT min(f.month) FROM bond_panel_returns f WHERE f.publication_id = prior.publication_id)
             AND (SELECT max(f.month) FROM bond_panel_rating_pit f WHERE f.publication_id = candidate.publication_id) = candidate.last_closed_month
             AND NOT EXISTS (SELECT 1 FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id AND (f.month < candidate.first_month OR f.month > candidate.last_closed_month))
             AND NOT EXISTS (
                 SELECT 1 FROM generate_series((candidate.first_month + INTERVAL '1 month')::date, candidate.last_closed_month, INTERVAL '1 month') expected(month)
                 WHERE NOT EXISTS (SELECT 1 FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id AND f.month = expected.month::date)
             )
             AND NOT EXISTS (
                 SELECT f.month, f.cusip_id FROM bond_panel_rv_signal f WHERE f.publication_id = candidate.publication_id
                 EXCEPT SELECT f.month, f.cusip_id FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id AND f.eligibility_state = 'included'
             )
             AND NOT EXISTS (
                 SELECT f.month, f.cusip_id FROM bond_panel_returns f WHERE f.publication_id = candidate.publication_id
                 EXCEPT SELECT f.month, f.cusip_id FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id
             )
             AND NOT EXISTS (
                 SELECT f.month, f.cusip_id FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id
                 EXCEPT SELECT f.month, f.cusip_id FROM bond_panel_rating_pit f WHERE f.publication_id = candidate.publication_id
             )
             AND NOT EXISTS (
                 SELECT f.month, f.cusip_id FROM bond_panel_rating_pit f WHERE f.publication_id = candidate.publication_id
                 EXCEPT SELECT f.month, f.cusip_id FROM bond_panel_snapshot f WHERE f.publication_id = candidate.publication_id
             )
       ) THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE'
       AND NEW.publication_id IS DISTINCT FROM OLD.publication_id
       AND NOT EXISTS (
           SELECT 1 FROM bond_panel_publications candidate
           WHERE candidate.publication_id = NEW.publication_id
             AND candidate.parent_publication_id = OLD.publication_id
       )
    THEN RAISE EXCEPTION 'pointer candidate must directly extend the current publication'; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS bond_panel_publication_transition ON bond_panel_publications;
CREATE TRIGGER bond_panel_publication_transition BEFORE INSERT OR UPDATE ON bond_panel_publications
FOR EACH ROW EXECUTE FUNCTION bond_panel_assert_publication_transition();
DROP TRIGGER IF EXISTS bond_panel_publication_parent ON bond_panel_publications;
CREATE TRIGGER bond_panel_publication_parent BEFORE INSERT OR UPDATE OF parent_publication_id ON bond_panel_publications
FOR EACH ROW EXECUTE FUNCTION bond_panel_assert_parent();
DROP TRIGGER IF EXISTS bond_panel_snapshot_immutable ON bond_panel_snapshot;
CREATE TRIGGER bond_panel_snapshot_immutable BEFORE INSERT OR UPDATE OR DELETE ON bond_panel_snapshot
FOR EACH ROW EXECUTE FUNCTION bond_panel_assert_immutable();
DROP TRIGGER IF EXISTS bond_panel_rv_signal_immutable ON bond_panel_rv_signal;
CREATE TRIGGER bond_panel_rv_signal_immutable BEFORE INSERT OR UPDATE OR DELETE ON bond_panel_rv_signal
FOR EACH ROW EXECUTE FUNCTION bond_panel_assert_immutable();
DROP TRIGGER IF EXISTS bond_panel_returns_immutable ON bond_panel_returns;
CREATE TRIGGER bond_panel_returns_immutable BEFORE INSERT OR UPDATE OR DELETE ON bond_panel_returns
FOR EACH ROW EXECUTE FUNCTION bond_panel_assert_immutable();
DROP TRIGGER IF EXISTS bond_panel_rating_pit_immutable ON bond_panel_rating_pit;
CREATE TRIGGER bond_panel_rating_pit_immutable BEFORE INSERT OR UPDATE OR DELETE ON bond_panel_rating_pit
FOR EACH ROW EXECUTE FUNCTION bond_panel_assert_immutable();
DROP TRIGGER IF EXISTS bond_panel_pointer_validated ON bond_panel_app_pointer;
CREATE TRIGGER bond_panel_pointer_validated BEFORE INSERT OR UPDATE ON bond_panel_app_pointer
FOR EACH ROW EXECUTE FUNCTION bond_panel_assert_pointer_validated();

-- Latest publication depth wins for a rebuilt month/CUSIP while all untouched
-- parent months remain visible through the one-row current pointer.
CREATE OR REPLACE VIEW bond_panel_current_snapshot_v1 AS
WITH RECURSIVE ancestry(publication_id, parent_publication_id, depth, path, config_hash) AS (
    SELECT p.publication_id, p.parent_publication_id, 0 AS depth, ARRAY[p.publication_id], p.config_hash
    FROM bond_panel_app_pointer pointer JOIN bond_panel_publications p ON p.publication_id = pointer.publication_id
    WHERE pointer.product = 'bond_panel_v1' AND p.publication_status = 'validated' AND p.config_hash IN ('0c0d78a866bc1090', '1863d3d5fa3a0edf')
    UNION ALL
    SELECT p.publication_id, p.parent_publication_id, a.depth + 1, a.path || p.publication_id, p.config_hash AS config_hash
    FROM ancestry a JOIN bond_panel_publications p ON p.publication_id = a.parent_publication_id
    WHERE NOT p.publication_id = ANY(a.path) AND p.publication_status = 'validated'
      AND (p.config_hash = a.config_hash OR (
          btrim(a.config_hash::text) = '1863d3d5fa3a0edf'
          AND btrim(p.config_hash::text) = '0c0d78a866bc1090'
      ))
)
SELECT DISTINCT ON (f.month, f.cusip_id) f.* FROM ancestry a JOIN bond_panel_snapshot f USING (publication_id)
ORDER BY f.month, f.cusip_id, a.depth;

CREATE OR REPLACE VIEW bond_panel_current_rv_signal_v1 AS
WITH RECURSIVE ancestry(publication_id, parent_publication_id, depth, path, config_hash) AS (
    SELECT p.publication_id, p.parent_publication_id, 0, ARRAY[p.publication_id], p.config_hash FROM bond_panel_app_pointer pointer JOIN bond_panel_publications p ON p.publication_id = pointer.publication_id WHERE pointer.product = 'bond_panel_v1' AND p.publication_status = 'validated' AND p.config_hash IN ('0c0d78a866bc1090', '1863d3d5fa3a0edf')
    UNION ALL SELECT p.publication_id, p.parent_publication_id, a.depth + 1, a.path || p.publication_id, p.config_hash AS config_hash FROM ancestry a JOIN bond_panel_publications p ON p.publication_id = a.parent_publication_id WHERE NOT p.publication_id = ANY(a.path) AND p.publication_status = 'validated' AND (p.config_hash = a.config_hash OR (btrim(a.config_hash::text) = '1863d3d5fa3a0edf' AND btrim(p.config_hash::text) = '0c0d78a866bc1090'))
)
SELECT DISTINCT ON (f.month, f.cusip_id) f.* FROM ancestry a JOIN bond_panel_rv_signal f USING (publication_id)
ORDER BY f.month, f.cusip_id, a.depth;

CREATE OR REPLACE VIEW bond_panel_current_returns_v1 AS
WITH RECURSIVE ancestry(publication_id, parent_publication_id, depth, path, config_hash) AS (
    SELECT p.publication_id, p.parent_publication_id, 0, ARRAY[p.publication_id], p.config_hash FROM bond_panel_app_pointer pointer JOIN bond_panel_publications p ON p.publication_id = pointer.publication_id WHERE pointer.product = 'bond_panel_v1' AND p.publication_status = 'validated' AND p.config_hash IN ('0c0d78a866bc1090', '1863d3d5fa3a0edf')
    UNION ALL SELECT p.publication_id, p.parent_publication_id, a.depth + 1, a.path || p.publication_id, p.config_hash AS config_hash FROM ancestry a JOIN bond_panel_publications p ON p.publication_id = a.parent_publication_id WHERE NOT p.publication_id = ANY(a.path) AND p.publication_status = 'validated' AND (p.config_hash = a.config_hash OR (btrim(a.config_hash::text) = '1863d3d5fa3a0edf' AND btrim(p.config_hash::text) = '0c0d78a866bc1090'))
)
SELECT DISTINCT ON (f.month, f.cusip_id) f.* FROM ancestry a JOIN bond_panel_returns f USING (publication_id)
ORDER BY f.month, f.cusip_id, a.depth;

CREATE OR REPLACE VIEW bond_panel_current_rating_pit_v1 AS
WITH RECURSIVE ancestry(publication_id, parent_publication_id, depth, path, config_hash) AS (
    SELECT p.publication_id, p.parent_publication_id, 0, ARRAY[p.publication_id], p.config_hash FROM bond_panel_app_pointer pointer JOIN bond_panel_publications p ON p.publication_id = pointer.publication_id WHERE pointer.product = 'bond_panel_v1' AND p.publication_status = 'validated' AND p.config_hash IN ('0c0d78a866bc1090', '1863d3d5fa3a0edf')
    UNION ALL SELECT p.publication_id, p.parent_publication_id, a.depth + 1, a.path || p.publication_id, p.config_hash AS config_hash FROM ancestry a JOIN bond_panel_publications p ON p.publication_id = a.parent_publication_id WHERE NOT p.publication_id = ANY(a.path) AND p.publication_status = 'validated' AND (p.config_hash = a.config_hash OR (btrim(a.config_hash::text) = '1863d3d5fa3a0edf' AND btrim(p.config_hash::text) = '0c0d78a866bc1090'))
)
SELECT DISTINCT ON (f.month, f.cusip_id) f.* FROM ancestry a JOIN bond_panel_rating_pit f USING (publication_id)
ORDER BY f.month, f.cusip_id, a.depth;

ALTER TABLE bond_panel_publications OWNER TO worker_writer;
ALTER TABLE bond_panel_app_pointer OWNER TO worker_writer;
ALTER TABLE bond_panel_snapshot OWNER TO worker_writer;
ALTER TABLE bond_panel_rv_signal OWNER TO worker_writer;
ALTER TABLE bond_panel_returns OWNER TO worker_writer;
ALTER TABLE bond_panel_rating_pit OWNER TO worker_writer;
ALTER FUNCTION bond_panel_assert_publication_transition() OWNER TO worker_writer;
ALTER FUNCTION bond_panel_assert_parent() OWNER TO worker_writer;
ALTER FUNCTION bond_panel_assert_immutable() OWNER TO worker_writer;
ALTER FUNCTION bond_panel_assert_pointer_validated() OWNER TO worker_writer;
ALTER VIEW bond_panel_current_snapshot_v1 OWNER TO worker_writer;
ALTER VIEW bond_panel_current_rv_signal_v1 OWNER TO worker_writer;
ALTER VIEW bond_panel_current_returns_v1 OWNER TO worker_writer;
ALTER VIEW bond_panel_current_rating_pit_v1 OWNER TO worker_writer;
REVOKE ALL ON TABLE bond_panel_publications FROM PUBLIC;
REVOKE ALL ON TABLE bond_panel_app_pointer FROM PUBLIC;
REVOKE ALL ON TABLE bond_panel_snapshot FROM PUBLIC;
REVOKE ALL ON TABLE bond_panel_rv_signal FROM PUBLIC;
REVOKE ALL ON TABLE bond_panel_returns FROM PUBLIC;
REVOKE ALL ON TABLE bond_panel_rating_pit FROM PUBLIC;
REVOKE ALL ON TABLE bond_panel_current_snapshot_v1 FROM PUBLIC;
REVOKE ALL ON TABLE bond_panel_current_rv_signal_v1 FROM PUBLIC;
REVOKE ALL ON TABLE bond_panel_current_returns_v1 FROM PUBLIC;
REVOKE ALL ON TABLE bond_panel_current_rating_pit_v1 FROM PUBLIC;
