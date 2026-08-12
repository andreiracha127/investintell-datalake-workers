-- Open Macro v04 point-in-time evidence.
--
-- Numeric observations and timing/lineage are private.  The public relations
-- deliberately expose only a fixed, categorical 13-item evidence surface plus
-- the categorical taxonomy projection consumed by Light.

CREATE TABLE IF NOT EXISTS open_macro_v04_pit_evidence (
    decision_month             char(7)     NOT NULL CHECK (decision_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    decision_as_of             date        NOT NULL,
    decision_run_id            text        NOT NULL CHECK (decision_run_id <> ''),
    decision_input_digest_sha256 char(64)  NOT NULL CHECK (decision_input_digest_sha256 ~ '^[0-9a-f]{64}$'),
    decision_basis             text        NOT NULL CHECK (decision_basis IN ('live', 'bootstrap_replay')),
    series_key                 text        NOT NULL CHECK (series_key IN (
        'INDPRO', 'PCEC96', 'PAYEMS', 'ACOGNO', 'CPILFESL', 'PPIFIS', 'AHETPI',
        'MICH', 'SPY', 'MTSDS133FMS', 'GDP', 'SUBLPDCILSLGNQ', 'M2SL'
    )),
    value                      numeric,
    unit                       text        CHECK (unit IS NULL OR unit <> ''),
    observation_period         date,
    release_at                 timestamptz,
    ingested_at                timestamptz,
    vintage                    text,
    source                     text        NOT NULL CHECK (source <> ''),
    source_health              text        NOT NULL CHECK (source_health IN ('verified', 'unverified', 'unavailable', 'invalid')),
    fingerprint                char(64)    NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    cutoff_at                  timestamptz NOT NULL,
    carry_seed_decision_month  char(7)     CHECK (carry_seed_decision_month IS NULL OR carry_seed_decision_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    carry_seed_fingerprint     char(64)    CHECK (carry_seed_fingerprint IS NULL OR carry_seed_fingerprint ~ '^[0-9a-f]{64}$'),
    materialization_run_id     text        NOT NULL CHECK (materialization_run_id <> ''),
    materialized_at            timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT open_macro_v04_pit_evidence_pkey PRIMARY KEY
        (decision_month, series_key),
    CONSTRAINT open_macro_v04_pit_evidence_month_matches_decision CHECK (
        decision_month = to_char(decision_as_of, 'YYYY-MM')
    ),
    CONSTRAINT open_macro_v04_pit_evidence_known_by_cutoff CHECK (
        (release_at IS NULL OR release_at <= cutoff_at)
        AND (ingested_at IS NULL OR ingested_at <= cutoff_at)
    ),
    CONSTRAINT open_macro_v04_pit_evidence_carry_seed_complete CHECK (
        (carry_seed_decision_month IS NULL) = (carry_seed_fingerprint IS NULL)
    )
);

CREATE OR REPLACE FUNCTION open_macro_v04_pit_evidence_reject_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'open_macro_v04_pit_evidence is append-only';
END;
$$;

DROP TRIGGER IF EXISTS open_macro_v04_pit_evidence_reject_mutation ON open_macro_v04_pit_evidence;
CREATE TRIGGER open_macro_v04_pit_evidence_reject_mutation
BEFORE UPDATE OR DELETE ON open_macro_v04_pit_evidence
FOR EACH ROW EXECUTE FUNCTION open_macro_v04_pit_evidence_reject_mutation();

CREATE TABLE IF NOT EXISTS open_macro_v04_evidence_snapshots (
    decision_month     char(7) NOT NULL CHECK (decision_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    publication_status text    NOT NULL CHECK (publication_status = 'open'),
    coverage_state     text    NOT NULL CHECK (coverage_state IN ('complete', 'partial', 'unavailable')),
    CONSTRAINT open_macro_v04_evidence_snapshots_pkey PRIMARY KEY (decision_month)
);

CREATE TABLE IF NOT EXISTS open_macro_v04_evidence_items (
    decision_month     char(7) NOT NULL,
    group_key          text    NOT NULL CHECK (group_key IN ('growth', 'inflation', 'market', 'fiscal_liquidity', 'allocation_guard')),
    group_label        text    NOT NULL CHECK (group_label <> ''),
    group_role         text    NOT NULL CHECK (group_role IN ('regime_inputs', 'allocation_evidence')),
    series_key         text    NOT NULL CHECK (series_key IN (
        'INDPRO', 'PCEC96', 'PAYEMS', 'ACOGNO', 'CPILFESL', 'PPIFIS', 'AHETPI',
        'MICH', 'SPY', 'MTSDS133FMS', 'GDP', 'SUBLPDCILSLGNQ', 'M2SL'
    )),
    series_label       text    NOT NULL CHECK (series_label <> ''),
    role               text    NOT NULL CHECK (role IN ('regime_input', 'allocation_guard')),
    display_state      text    NOT NULL CHECK (display_state IN ('ready', 'limited', 'unavailable')),
    availability_state text    NOT NULL CHECK (availability_state IN ('available', 'not_available', 'unknown')),
    evidence_state     text    NOT NULL CHECK (evidence_state IN ('observed', 'carried', 'missing', 'invalid')),
    freshness_state    text    NOT NULL CHECK (freshness_state IN ('current', 'stale', 'unknown')),
    pit_state          text    NOT NULL CHECK (pit_state IN ('verified', 'unverified', 'unavailable')),
    CONSTRAINT open_macro_v04_evidence_items_pkey PRIMARY KEY (decision_month, series_key),
    CONSTRAINT open_macro_v04_evidence_items_snapshot_fk FOREIGN KEY (decision_month)
        REFERENCES open_macro_v04_evidence_snapshots (decision_month) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT open_macro_v04_evidence_items_catalog_coherence CHECK (
        (series_key = 'INDPRO' AND series_label = 'Industrial Production'
            AND group_key = 'growth' AND group_label = 'Growth'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'PCEC96' AND series_label = 'Real Personal Consumption Expenditures'
            AND group_key = 'growth' AND group_label = 'Growth'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'PAYEMS' AND series_label = 'Total Nonfarm Payrolls'
            AND group_key = 'growth' AND group_label = 'Growth'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'ACOGNO' AND series_label = 'Manufacturers’ New Orders for Consumer Goods'
            AND group_key = 'growth' AND group_label = 'Growth'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'CPILFESL' AND series_label = 'Core Consumer Price Index'
            AND group_key = 'inflation' AND group_label = 'Inflation'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'PPIFIS' AND series_label = 'Producer Price Index: Final Demand Intermediate Services'
            AND group_key = 'inflation' AND group_label = 'Inflation'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'AHETPI' AND series_label = 'Average Hourly Earnings'
            AND group_key = 'inflation' AND group_label = 'Inflation'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'MICH' AND series_label = 'University of Michigan Inflation Expectations'
            AND group_key = 'inflation' AND group_label = 'Inflation'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'SPY' AND series_label = 'Cycle Market Leg'
            AND group_key = 'market' AND group_label = 'Market'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'MTSDS133FMS' AND series_label = 'Federal Surplus or Deficit'
            AND group_key = 'fiscal_liquidity' AND group_label = 'Fiscal and liquidity'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'GDP' AND series_label = 'Nominal GDP'
            AND group_key = 'fiscal_liquidity' AND group_label = 'Fiscal and liquidity'
            AND group_role = 'regime_inputs' AND role = 'regime_input')
        OR (series_key = 'SUBLPDCILSLGNQ' AND series_label = 'Bank Lending Standards'
            AND group_key = 'allocation_guard' AND group_label = 'Allocation guard'
            AND group_role = 'allocation_evidence' AND role = 'allocation_guard')
        OR (series_key = 'M2SL' AND series_label = 'M2 Money Stock'
            AND group_key = 'allocation_guard' AND group_label = 'Allocation guard'
            AND group_role = 'allocation_evidence' AND role = 'allocation_guard')
    )
);

-- This is the only worker-owned taxonomy projection Light may read.  It is
-- intentionally categorical: there are no scores, weights, timestamps, IDs,
-- hashes, values, units, or provenance fields on this public relation.
CREATE TABLE IF NOT EXISTS open_macro_v04_categorical_taxonomy (
    decision_month     char(7)  NOT NULL CHECK (decision_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    taxonomy_state     text     NOT NULL CHECK (taxonomy_state IN (
        'aligned_expansion', 'liquidity_led', 'cycle_led', 'aligned_contraction',
        'liquidity_unread', 'no_signal'
    )),
    fiscal_state       text     NOT NULL CHECK (fiscal_state IN ('dominance', 'contained')),
    fiscal_boundary    boolean  NOT NULL,
    guard_level        text     NOT NULL CHECK (guard_level IN ('off', 'alert', 'severe')),
    guard_coverage     text     NOT NULL CHECK (guard_coverage IN ('full', 'partial_a', 'partial_b', 'blind')),
    quadrant           text     NOT NULL CHECK (quadrant IN (
        'recovery', 'expansion', 'slowdown', 'contraction', 'unavailable'
    )),
    cycle_direction    text     NOT NULL CHECK (cycle_direction IN ('up', 'down', 'unavailable')),
    decision_validity  text     NOT NULL CHECK (decision_validity IN (
        'fresh', 'carried', 'dominance_baseline', 'guard_blind', 'no_signal'
    )),
    decision_basis     text     NOT NULL CHECK (decision_basis IN ('live', 'bootstrap_replay')),
    quadrant_source    text     NOT NULL CHECK (quadrant_source IN (
        'chain_fresh', 'chain_carry', 'no_signal', 'proxy', 'proxy_missing'
    )),
    book               text     NOT NULL CHECK (book IN (
        'defensive', 'moderated', 'expansionary_baseline', 'center'
    )),
    CONSTRAINT open_macro_v04_categorical_taxonomy_pkey PRIMARY KEY (decision_month),
    CONSTRAINT open_macro_v04_categorical_taxonomy_snapshot_fk FOREIGN KEY (decision_month)
        REFERENCES open_macro_v04_evidence_snapshots (decision_month) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT open_macro_v04_categorical_taxonomy_quadrant_source_consistent CHECK (
        (quadrant = 'unavailable') = (quadrant_source IN ('no_signal', 'proxy_missing'))
    ),
    CONSTRAINT open_macro_v04_categorical_taxonomy_cycle_consistent CHECK (
        (cycle_direction = 'up' AND quadrant IN ('recovery', 'expansion'))
        OR (cycle_direction = 'down' AND quadrant IN ('slowdown', 'contraction'))
        OR (cycle_direction = 'unavailable' AND quadrant = 'unavailable')
    ),
    CONSTRAINT open_macro_v04_categorical_taxonomy_validity_consistent CHECK (
        decision_validity = CASE
            WHEN guard_coverage = 'blind' THEN 'guard_blind'
            WHEN fiscal_state = 'dominance' THEN 'dominance_baseline'
            WHEN quadrant_source = 'chain_fresh' THEN 'fresh'
            WHEN quadrant_source = 'chain_carry' THEN 'carried'
            ELSE 'no_signal' END
    )
);

CREATE OR REPLACE FUNCTION open_macro_v04_categorical_taxonomy_reject_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'published categorical taxonomy is append-only';
END;
$$;

DROP TRIGGER IF EXISTS open_macro_v04_categorical_taxonomy_reject_mutation ON open_macro_v04_categorical_taxonomy;
CREATE TRIGGER open_macro_v04_categorical_taxonomy_reject_mutation
BEFORE UPDATE OR DELETE ON open_macro_v04_categorical_taxonomy
FOR EACH ROW EXECUTE FUNCTION open_macro_v04_categorical_taxonomy_reject_mutation();

CREATE OR REPLACE FUNCTION open_macro_v04_evidence_snapshots_insert_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    item_count integer;
    taxonomy_count integer;
    derived_coverage text;
BEGIN
    SELECT count(*) INTO item_count
    FROM open_macro_v04_evidence_items
    WHERE decision_month = NEW.decision_month;
    IF item_count <> 13 THEN
        RAISE EXCEPTION 'evidence snapshot % requires exactly 13 fixed catalog items', NEW.decision_month;
    END IF;
    SELECT count(*) INTO taxonomy_count
    FROM open_macro_v04_categorical_taxonomy
    WHERE decision_month = NEW.decision_month;
    IF taxonomy_count <> 1 THEN
        RAISE EXCEPTION 'evidence snapshot % requires exactly one categorical taxonomy row', NEW.decision_month;
    END IF;
    SELECT CASE
        WHEN bool_and(display_state = 'ready'
                      AND availability_state = 'available'
                      AND pit_state = 'verified') THEN 'complete'
        WHEN bool_and(display_state = 'unavailable') THEN 'unavailable'
        ELSE 'partial'
    END INTO derived_coverage
    FROM open_macro_v04_evidence_items
    WHERE decision_month = NEW.decision_month;
    IF NEW.coverage_state <> derived_coverage THEN
        RAISE EXCEPTION 'evidence snapshot % coverage does not match its item states', NEW.decision_month;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION open_macro_v04_evidence_items_insert_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION open_macro_v04_evidence_items_reject_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'published evidence items are append-only';
END;
$$;

CREATE OR REPLACE FUNCTION open_macro_v04_evidence_snapshots_reject_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'published evidence snapshots are append-only';
END;
$$;

DROP TRIGGER IF EXISTS open_macro_v04_evidence_snapshots_insert_guard ON open_macro_v04_evidence_snapshots;
CREATE TRIGGER open_macro_v04_evidence_snapshots_insert_guard
BEFORE INSERT ON open_macro_v04_evidence_snapshots
FOR EACH ROW EXECUTE FUNCTION open_macro_v04_evidence_snapshots_insert_guard();

DROP TRIGGER IF EXISTS open_macro_v04_evidence_snapshots_reject_mutation ON open_macro_v04_evidence_snapshots;
CREATE TRIGGER open_macro_v04_evidence_snapshots_reject_mutation
BEFORE UPDATE OR DELETE ON open_macro_v04_evidence_snapshots
FOR EACH ROW EXECUTE FUNCTION open_macro_v04_evidence_snapshots_reject_mutation();

DROP TRIGGER IF EXISTS open_macro_v04_evidence_items_insert_guard ON open_macro_v04_evidence_items;
CREATE TRIGGER open_macro_v04_evidence_items_insert_guard
BEFORE INSERT ON open_macro_v04_evidence_items
FOR EACH ROW EXECUTE FUNCTION open_macro_v04_evidence_items_insert_guard();

DROP TRIGGER IF EXISTS open_macro_v04_evidence_items_reject_mutation ON open_macro_v04_evidence_items;
CREATE TRIGGER open_macro_v04_evidence_items_reject_mutation
BEFORE UPDATE OR DELETE ON open_macro_v04_evidence_items
FOR EACH ROW EXECUTE FUNCTION open_macro_v04_evidence_items_reject_mutation();

REVOKE ALL ON TABLE open_macro_v04_pit_evidence FROM PUBLIC;
REVOKE ALL ON TABLE open_macro_v04_evidence_snapshots FROM PUBLIC;
REVOKE ALL ON TABLE open_macro_v04_evidence_items FROM PUBLIC;
REVOKE ALL ON TABLE open_macro_v04_categorical_taxonomy FROM PUBLIC;
REVOKE ALL ON FUNCTION open_macro_v04_pit_evidence_reject_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION open_macro_v04_evidence_snapshots_insert_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION open_macro_v04_evidence_items_insert_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION open_macro_v04_evidence_items_reject_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION open_macro_v04_evidence_snapshots_reject_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION open_macro_v04_categorical_taxonomy_reject_mutation() FROM PUBLIC;

DO $$
DECLARE
    can_manage_producer_acl boolean;
BEGIN
    -- Decisions and allocations belong to their producer.  A worker_writer
    -- reapply owns the evidence objects but not these relations, so PostgreSQL
    -- otherwise emits owner-only ACL warnings for a perfectly valid rerun.
    SELECT COALESCE((
        SELECT rolsuper FROM pg_roles WHERE rolname = current_user
    ), false) OR NOT EXISTS (
        SELECT 1
        FROM pg_class
        WHERE oid IN (
            'public.open_macro_v04_decisions'::regclass,
            'public.open_macro_v04_allocations'::regclass
        )
          AND relowner <> (SELECT oid FROM pg_roles WHERE rolname = current_user)
    ) INTO can_manage_producer_acl;

    IF NOT can_manage_producer_acl AND EXISTS (
        SELECT 1
        FROM pg_roles AS application_role
        CROSS JOIN (VALUES
            ('public.open_macro_v04_decisions'::text),
            ('public.open_macro_v04_allocations'::text)
        ) AS producer_relation(name)
        WHERE application_role.rolname IN ('app_runtime', 'app_analytics_ro')
          AND (
              has_table_privilege(application_role.oid, producer_relation.name, 'SELECT')
              OR has_any_column_privilege(
                  application_role.oid, producer_relation.name, 'SELECT'
              )
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'open_macro_v04 producer ACLs are unsafe; owner bootstrap required';
    END IF;

    IF can_manage_producer_acl THEN
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_decisions FROM PUBLIC';
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_allocations FROM PUBLIC';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker_writer') THEN
        IF can_manage_producer_acl THEN
            EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_decisions FROM worker_writer';
            EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_allocations FROM worker_writer';
            EXECUTE 'GRANT SELECT ON TABLE open_macro_v04_decisions TO worker_writer';
            EXECUTE 'GRANT SELECT ON TABLE open_macro_v04_allocations TO worker_writer';
        END IF;
        EXECUTE 'GRANT SELECT, INSERT ON TABLE open_macro_v04_pit_evidence TO worker_writer';
        EXECUTE 'GRANT SELECT, INSERT ON TABLE open_macro_v04_evidence_snapshots TO worker_writer';
        EXECUTE 'GRANT SELECT, INSERT ON TABLE open_macro_v04_evidence_items TO worker_writer';
        EXECUTE 'GRANT SELECT, INSERT ON TABLE open_macro_v04_categorical_taxonomy TO worker_writer';
        -- An administrator may perform the first bootstrap, but runtime
        -- ensure_schema is deliberately re-runnable as worker_writer.  Move
        -- only the worker-owned append-only relations and their trigger
        -- functions; decisions/allocations remain owned by their producer.
        EXECUTE 'ALTER TABLE open_macro_v04_pit_evidence OWNER TO worker_writer';
        EXECUTE 'ALTER TABLE open_macro_v04_evidence_snapshots OWNER TO worker_writer';
        EXECUTE 'ALTER TABLE open_macro_v04_evidence_items OWNER TO worker_writer';
        EXECUTE 'ALTER TABLE open_macro_v04_categorical_taxonomy OWNER TO worker_writer';
        EXECUTE 'ALTER FUNCTION open_macro_v04_pit_evidence_reject_mutation() OWNER TO worker_writer';
        EXECUTE 'ALTER FUNCTION open_macro_v04_evidence_snapshots_insert_guard() OWNER TO worker_writer';
        EXECUTE 'ALTER FUNCTION open_macro_v04_evidence_items_insert_guard() OWNER TO worker_writer';
        EXECUTE 'ALTER FUNCTION open_macro_v04_evidence_items_reject_mutation() OWNER TO worker_writer';
        EXECUTE 'ALTER FUNCTION open_macro_v04_evidence_snapshots_reject_mutation() OWNER TO worker_writer';
        EXECUTE 'ALTER FUNCTION open_macro_v04_categorical_taxonomy_reject_mutation() OWNER TO worker_writer';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_runtime') THEN
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_pit_evidence FROM app_runtime';
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_evidence_snapshots FROM app_runtime';
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_evidence_items FROM app_runtime';
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_categorical_taxonomy FROM app_runtime';
        IF can_manage_producer_acl THEN
            EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_decisions FROM app_runtime';
            EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_allocations FROM app_runtime';
        END IF;
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_pit_evidence_reject_mutation() FROM app_runtime';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_evidence_snapshots_insert_guard() FROM app_runtime';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_evidence_items_insert_guard() FROM app_runtime';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_evidence_items_reject_mutation() FROM app_runtime';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_evidence_snapshots_reject_mutation() FROM app_runtime';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_categorical_taxonomy_reject_mutation() FROM app_runtime';
        EXECUTE 'GRANT SELECT ON TABLE open_macro_v04_evidence_snapshots TO app_runtime';
        EXECUTE 'GRANT SELECT ON TABLE open_macro_v04_evidence_items TO app_runtime';
        EXECUTE 'GRANT SELECT ON TABLE open_macro_v04_categorical_taxonomy TO app_runtime';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_analytics_ro') THEN
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_pit_evidence FROM app_analytics_ro';
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_evidence_snapshots FROM app_analytics_ro';
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_evidence_items FROM app_analytics_ro';
        EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_categorical_taxonomy FROM app_analytics_ro';
        IF can_manage_producer_acl THEN
            EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_decisions FROM app_analytics_ro';
            EXECUTE 'REVOKE ALL ON TABLE open_macro_v04_allocations FROM app_analytics_ro';
        END IF;
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_pit_evidence_reject_mutation() FROM app_analytics_ro';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_evidence_snapshots_insert_guard() FROM app_analytics_ro';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_evidence_items_insert_guard() FROM app_analytics_ro';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_evidence_items_reject_mutation() FROM app_analytics_ro';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_evidence_snapshots_reject_mutation() FROM app_analytics_ro';
        EXECUTE 'REVOKE ALL ON FUNCTION open_macro_v04_categorical_taxonomy_reject_mutation() FROM app_analytics_ro';
    END IF;
END;
$$;
